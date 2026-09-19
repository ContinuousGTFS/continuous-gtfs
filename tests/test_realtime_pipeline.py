"""Integration tests for the realtime pipeline."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2 as gtfs_rt

from continuous_gtfs import PipelineContext, resolve_dag, scan_pipeline
from continuous_gtfs.builtins.realtime import (
    CombineFeeds,
    ConvertScheduledToNew,
    ExpireCancelledTrips,
    FilterStopsByID,
    InsertMissingCancellations,
    PassThrough,
    RenameVehicles,
    TransformTripId,
    UpdateFeedHeader,
)
from continuous_gtfs.pipelines.realtime import run_realtime_pipeline


def _make_test_feed(n_entities=3):
    """Create a minimal test FeedMessage."""
    feed = gtfs_rt.FeedMessage()
    feed.header.gtfs_realtime_version = "1.0"
    feed.header.timestamp = 1000000
    for i in range(n_entities):
        entity = feed.entity.add()
        entity.id = str(i)
        vp = entity.vehicle
        vp.trip.trip_id = f"trip_{i}"
        vp.vehicle.id = f"v{i}"
        vp.vehicle.label = f"Vehicle {i}"
        vp.stop_id = f"S{i}"
        vp.current_stop_sequence = i
        vp.position.latitude = 47.6 + i * 0.01
        vp.position.longitude = -122.3 + i * 0.01
    return feed


def _read_feed(pb_bytes: bytes) -> gtfs_rt.FeedMessage:
    feed = gtfs_rt.FeedMessage()
    feed.ParseFromString(pb_bytes)
    return feed


def _mirror_inputs_init_step():
    """Shared init step: mirror each RT input to ctx.output under the same key.

    Tests that don't care about output-naming use this to keep their
    focus on the transform under test. Real pipelines would pick
    canonical names and/or merge inputs.
    """
    from continuous_gtfs import step

    @step(before="*")
    def init_output(ctx):
        for name, value in ctx.inputs.items():
            ctx.output[name] = value

    init_output.name = "init_output"
    return init_output


# --- Unit tests for RT builtins ---


def test_passthrough():
    feed = _make_test_feed()
    original_bytes = feed.SerializeToString()
    ctx = PipelineContext(output={"feed": feed})
    step = PassThrough()
    step.name = "passthrough"
    step.apply(ctx)
    # Feed should be unchanged
    assert ctx.output["feed"].SerializeToString() == original_bytes


def _make_tu_feed(trip_id: str, stop_ids: list[str]) -> gtfs_rt.FeedMessage:
    """Build a TripUpdates FeedMessage with one entity containing the given STUs.

    Each STU keeps the same `stop_sequence` it would have in a contiguous trip
    (1-indexed) so tests can assert that gaps survive filtering.
    """
    feed = gtfs_rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = trip_id
    entity.trip_update.trip.trip_id = trip_id
    for i, sid in enumerate(stop_ids, start=1):
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_id = sid
        stu.stop_sequence = i
        stu.arrival.time = 1_700_000_000 + i * 60
    return feed


def _apply_filter(blocked: set[str], **outputs) -> dict:
    ctx = PipelineContext(output=dict(outputs))
    step = FilterStopsByID(blocked_stop_ids=blocked)
    step.name = "filter"
    step.apply(ctx)
    return ctx.output


def test_filter_stops_by_id_vp_clears_status_trio():
    """VP entity referencing a blocked stop has all three
    VehicleStopStatus fields cleared."""
    feed = _make_test_feed()
    feed.entity[1].vehicle.current_status = gtfs_rt.VehiclePosition.STOPPED_AT
    out = _apply_filter({"S1"}, vehicle_positions=feed)

    vp = out["vehicle_positions"].entity[1].vehicle
    assert vp.stop_id == ""
    assert vp.current_stop_sequence == 0
    assert not vp.HasField("current_status")
    # Reads still return the proto-declared default
    assert vp.current_status == gtfs_rt.VehiclePosition.IN_TRANSIT_TO


def test_filter_stops_by_id_vp_preserves_unrelated_fields():
    """Position, vehicle ID, and trip reference survive the clear."""
    feed = _make_test_feed()
    feed.entity[1].vehicle.trip.trip_id = "trip_X"
    out = _apply_filter({"S1"}, vehicle_positions=feed)

    vp = out["vehicle_positions"].entity[1].vehicle
    assert vp.vehicle.id == "v1"
    assert vp.trip.trip_id == "trip_X"
    assert vp.position.latitude == pytest.approx(47.61)


def test_filter_stops_by_id_vp_unaffected_when_not_matched():
    """Entities whose stop_id isn't blocked are byte-for-byte unchanged."""
    feed = _make_test_feed()
    before = feed.SerializeToString()
    out = _apply_filter({"DOES_NOT_EXIST"}, vehicle_positions=feed)
    assert out["vehicle_positions"].SerializeToString() == before


def test_filter_stops_by_id_tu_drops_only_matching_stus():
    """Only STUs whose stop_id is in the blocklist are removed;
    the rest survive intact."""
    feed = _make_tu_feed("T1", ["A", "B", "BAD", "C", "BAD"])
    out = _apply_filter({"BAD"}, trip_updates=feed)

    stus = out["trip_updates"].entity[0].trip_update.stop_time_update
    assert [stu.stop_id for stu in stus] == ["A", "B", "C"]


def test_filter_stops_by_id_tu_preserves_stop_sequence_gaps():
    """Surviving STUs keep their original stop_sequence — gaps are legal per GTFS."""
    feed = _make_tu_feed("T1", ["A", "BAD", "C", "BAD", "E"])
    out = _apply_filter({"BAD"}, trip_updates=feed)

    seqs = [
        stu.stop_sequence
        for stu in out["trip_updates"].entity[0].trip_update.stop_time_update
    ]
    assert seqs == [1, 3, 5]


def test_filter_stops_by_id_tu_all_stops_blocked_keeps_entity():
    """A trip where every STU is blocked yields an empty
    stop_time_update list — the entity is not removed."""
    feed = _make_tu_feed("T1", ["BAD", "BAD"])
    out = _apply_filter({"BAD"}, trip_updates=feed)

    assert len(out["trip_updates"].entity) == 1
    assert len(out["trip_updates"].entity[0].trip_update.stop_time_update) == 0


def test_filter_stops_by_id_tu_no_match_is_byte_identical():
    feed = _make_tu_feed("T1", ["A", "B", "C"])
    before = feed.SerializeToString()
    out = _apply_filter({"X"}, trip_updates=feed)
    assert out["trip_updates"].SerializeToString() == before


def test_filter_stops_by_id_empty_blocklist_is_noop():
    """An empty blocklist must not alter any input."""
    vp = _make_test_feed()
    tu = _make_tu_feed("T1", ["A", "B"])
    vp_before, tu_before = vp.SerializeToString(), tu.SerializeToString()
    out = _apply_filter(set(), vehicle_positions=vp, trip_updates=tu)
    assert out["vehicle_positions"].SerializeToString() == vp_before
    assert out["trip_updates"].SerializeToString() == tu_before


def test_filter_stops_by_id_applies_to_every_rt_output():
    """When ctx.output holds multiple FeedMessages, the filter touches each."""
    vp = _make_test_feed()  # S0/S1/S2 — S1 will be cleared
    tu = _make_tu_feed("T1", ["A", "S1", "C"])  # S1 STU removed
    out = _apply_filter({"S1"}, vehicle_positions=vp, trip_updates=tu)

    assert out["vehicle_positions"].entity[1].vehicle.stop_id == ""
    stus = [
        s.stop_id for s in out["trip_updates"].entity[0].trip_update.stop_time_update
    ]
    assert stus == ["A", "C"]


def test_filter_stops_by_id_ignores_non_vp_non_tu_entities():
    """Entities carrying neither trip_update nor vehicle
    (e.g. alert-only) are untouched."""
    feed = gtfs_rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    e = feed.entity.add()
    e.id = "alert-1"
    e.alert.header_text.translation.add().text = "Test alert"
    before = feed.SerializeToString()

    out = _apply_filter({"S1"}, vehicle_positions=feed)
    assert out["vehicle_positions"].SerializeToString() == before


def test_filter_stops_by_id_requires_set_or_regex():
    """At least one of blocked_stop_ids / blocked_stop_id_regex must be supplied."""
    with pytest.raises(ValueError, match="blocked_stop_ids or blocked_stop_id_regex"):
        FilterStopsByID()


def test_filter_stops_by_id_regex_matches_prefix_variants():
    """A regex like `^E01(-|$)` filters the bare ID and all child-platform variants."""
    feed = _make_tu_feed("T1", ["E01", "E01-T1", "E01-NB", "E02", "E010"])
    step = FilterStopsByID(blocked_stop_id_regex=r"^E01(-|$)")
    step.name = "filter"
    ctx = PipelineContext(output={"trip_updates": feed})
    step.apply(ctx)

    surviving = [
        stu.stop_id
        for stu in ctx.output["trip_updates"].entity[0].trip_update.stop_time_update
    ]
    assert surviving == ["E02", "E010"]


def test_filter_stops_by_id_regex_clears_vp_status():
    """Regex match on VehiclePosition.stop_id clears the VehicleStopStatus trio."""
    feed = _make_test_feed()
    feed.entity[1].vehicle.stop_id = "E01-T1"
    feed.entity[1].vehicle.current_status = gtfs_rt.VehiclePosition.STOPPED_AT
    step = FilterStopsByID(blocked_stop_id_regex=r"^E01")
    step.name = "filter"
    ctx = PipelineContext(output={"vehicle_positions": feed})
    step.apply(ctx)

    vp = ctx.output["vehicle_positions"].entity[1].vehicle
    assert vp.stop_id == ""
    assert vp.current_stop_sequence == 0
    assert not vp.HasField("current_status")


def test_filter_stops_by_id_set_and_regex_union():
    """Supplying both unions the matches; either source can drop a stop."""
    feed = _make_tu_feed("T1", ["E01", "X99", "E01-T1", "Y22"])
    step = FilterStopsByID(
        blocked_stop_ids={"X99"},
        blocked_stop_id_regex=r"^E01",
    )
    step.name = "filter"
    ctx = PipelineContext(output={"trip_updates": feed})
    step.apply(ctx)

    surviving = [
        stu.stop_id
        for stu in ctx.output["trip_updates"].entity[0].trip_update.stop_time_update
    ]
    assert surviving == ["Y22"]


def test_filter_stops_by_id_description_reflects_inputs():
    """Description includes set size and/or regex pattern
    depending on what's configured."""
    assert (
        FilterStopsByID(blocked_stop_ids={"E01", "E07"}).description
        == "Filter 2 blocked stops"
    )
    assert (
        FilterStopsByID(blocked_stop_id_regex=r"^E01").description
        == "Filter regex '^E01'"
    )
    s = FilterStopsByID(blocked_stop_ids={"X"}, blocked_stop_id_regex=r"^E01")
    assert s.description == "Filter 1 blocked stops + regex '^E01'"


def test_rename_vehicles():
    feed = _make_test_feed()
    ctx = PipelineContext(output={"vehicle_positions": feed})
    step = RenameVehicles(id_prefix="DEMO-")
    step.name = "rename"
    step.apply(ctx)

    for entity in ctx.output["vehicle_positions"].entity:
        assert entity.vehicle.vehicle.id.startswith("DEMO-")
        assert entity.vehicle.vehicle.label.startswith("DEMO-")


def test_rename_vehicles_idempotent():
    feed = _make_test_feed()
    ctx = PipelineContext(output={"vehicle_positions": feed})
    step = RenameVehicles(id_prefix="DEMO-")
    step.name = "rename"
    step.apply(ctx)
    step.apply(ctx)  # apply again
    # Should not double-prefix
    assert ctx.output["vehicle_positions"].entity[0].vehicle.vehicle.id == "DEMO-v0"


def test_update_feed_header_applies_to_every_rt_output():
    vp = _make_test_feed()
    tu = _make_test_feed()
    assert vp.header.gtfs_realtime_version == "1.0"
    ctx = PipelineContext(output={"vehicle_positions": vp, "trip_updates": tu})
    step = UpdateFeedHeader()
    step.name = "update_header"
    step.apply(ctx)
    assert ctx.output["vehicle_positions"].header.gtfs_realtime_version == "2.0"
    assert ctx.output["trip_updates"].header.gtfs_realtime_version == "2.0"


def test_transform_trip_id():
    feed = _make_test_feed()
    ctx = PipelineContext(output={"vehicle_positions": feed})
    step = TransformTripId(pattern=r"trip_(\d+)", replacement=r"T-\1")
    step.name = "transform_trip"
    step.apply(ctx)

    for entity in ctx.output["vehicle_positions"].entity:
        assert entity.vehicle.trip.trip_id.startswith("T-")


def test_combine_feeds_merges_sources_into_target():
    """CombineFeeds pulls entities from ctx.inputs sources into ctx.output[target]."""
    feed1 = _make_test_feed(2)
    feed2 = gtfs_rt.FeedMessage()
    feed2.header.gtfs_realtime_version = "2.0"
    entity = feed2.entity.add()
    entity.id = "new_entity"
    entity.vehicle.trip.trip_id = "new_trip"

    ctx = PipelineContext(
        inputs={"vendor_a": feed1, "vendor_b": feed2},
        output={"vehicle_positions": _make_test_feed(2)},
    )
    step = CombineFeeds(target="vehicle_positions", sources=["vendor_b"])
    step.name = "combine"
    step.apply(ctx)

    assert len(ctx.output["vehicle_positions"].entity) == 3
    ids = [e.id for e in ctx.output["vehicle_positions"].entity]
    assert "new_entity" in ids


def test_combine_feeds_dedup():
    """Duplicate entity IDs from sources should be skipped."""
    target = _make_test_feed(2)
    source = gtfs_rt.FeedMessage()
    source.header.gtfs_realtime_version = "2.0"
    entity = source.entity.add()
    entity.id = "0"  # same as target's first entity

    ctx = PipelineContext(
        inputs={"vendor_b": source},
        output={"vehicle_positions": target},
    )
    step = CombineFeeds(target="vehicle_positions", sources=["vendor_b"])
    step.name = "combine"
    step.apply(ctx)

    assert len(ctx.output["vehicle_positions"].entity) == 2


# --- RT pipeline runner tests ---


# --- Schedule-dependent transforms ---


def _schedule_for(
    stop_times_rows: list[dict],
    trips_rows: list[dict] | None = None,
    *,
    agency_tz: str | None = "UTC",
    calendar_rows: list[dict] | None = None,
    calendar_dates_rows: list[dict] | None = None,
) -> dict:
    """Build a minimal `gtfs_schedule_zip` parse output for tests.

    Defaults `agency.txt` to UTC and seeds a permissive `calendar.txt`
    that runs every day of the week from 20260101–20261231 for service
    `S1`. When `trips_rows` is provided without a `service_id` column,
    every trip is assumed to be on `S1`. Callers needing different
    behavior can pass explicit `calendar_rows` or set `agency_tz=None`
    to omit `agency.txt`.
    """
    import polars as pl

    schedule: dict = {
        "stop_times.txt": pl.DataFrame(
            stop_times_rows or [{"trip_id": "", "departure_time": ""}]
        ),
    }
    if agency_tz is not None:
        schedule["agency.txt"] = pl.DataFrame(
            {
                "agency_id": ["a1"],
                "agency_name": ["test"],
                "agency_timezone": [agency_tz],
            }
        )

    if trips_rows is not None:
        # Default service_id to S1 if not specified per row.
        defaulted = [
            {**row, "service_id": row.get("service_id", "S1")} for row in trips_rows
        ]
        schedule["trips.txt"] = pl.DataFrame(defaulted)

    schedule["calendar.txt"] = pl.DataFrame(
        calendar_rows
        or [
            {
                "service_id": "S1",
                "monday": 1,
                "tuesday": 1,
                "wednesday": 1,
                "thursday": 1,
                "friday": 1,
                "saturday": 1,
                "sunday": 1,
                "start_date": "20260101",
                "end_date": "20261231",
            }
        ]
    )
    if calendar_dates_rows is not None:
        schedule["calendar_dates.txt"] = pl.DataFrame(calendar_dates_rows)
    return schedule


# Reference "now" used across the cancellation tests — UTC, 10:00 → 36_000 s
# into the service day. Tests overriding the agency timezone should construct
# their own datetime.
_NOW_UTC_1000 = datetime(2026, 5, 20, 10, 0, tzinfo=UTC)


def _tu_feed_with(entries: list[tuple[str, int]]) -> gtfs_rt.FeedMessage:
    """Build a trip_updates FeedMessage.
    `entries` is `(trip_id, schedule_relationship)`."""
    feed = gtfs_rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    for trip_id, rel in entries:
        entity = feed.entity.add()
        entity.id = trip_id
        entity.trip_update.trip.trip_id = trip_id
        entity.trip_update.trip.schedule_relationship = rel
    return feed


def test_expire_cancelled_trips_drops_stale():
    """Cancellations whose scheduled end has passed are removed;
    in-progress cancellations stay."""
    feed = _tu_feed_with(
        [
            ("T_stale", gtfs_rt.TripDescriptor.CANCELED),
            ("T_active", gtfs_rt.TripDescriptor.CANCELED),
            ("T_running", gtfs_rt.TripDescriptor.SCHEDULED),
        ]
    )
    schedule = _schedule_for(
        [
            # T_stale: 06:00–07:00 (ended before 10:00)
            {
                "trip_id": "T_stale",
                "departure_time": "06:00:00",
                "arrival_time": "06:00:00",
            },
            {
                "trip_id": "T_stale",
                "departure_time": "07:00:00",
                "arrival_time": "07:00:00",
            },
            # T_active: 09:00–11:00 (still running at 10:00)
            {
                "trip_id": "T_active",
                "departure_time": "09:00:00",
                "arrival_time": "09:00:00",
            },
            {
                "trip_id": "T_active",
                "departure_time": "11:00:00",
                "arrival_time": "11:00:00",
            },
            # T_running: 08:00–12:00
            {
                "trip_id": "T_running",
                "departure_time": "08:00:00",
                "arrival_time": "08:00:00",
            },
            {
                "trip_id": "T_running",
                "departure_time": "12:00:00",
                "arrival_time": "12:00:00",
            },
        ]
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    ExpireCancelledTrips(now=_NOW_UTC_1000).apply(ctx)

    trip_ids = [e.trip_update.trip.trip_id for e in ctx.output["trip_updates"].entity]
    assert "T_stale" not in trip_ids
    assert "T_active" in trip_ids
    assert "T_running" in trip_ids


def test_expire_cancelled_trips_noop_without_schedule():
    """No schedule input → no-op; never crashes a misconfigured pipeline."""
    feed = _tu_feed_with([("T1", gtfs_rt.TripDescriptor.CANCELED)])
    before = feed.SerializeToString()
    ctx = PipelineContext(inputs={}, output={"trip_updates": feed})
    ExpireCancelledTrips(now=_NOW_UTC_1000).apply(ctx)
    assert ctx.output["trip_updates"].SerializeToString() == before


def test_expire_cancelled_trips_carries_forward_from_previous_output():
    """`previous_output_input` resurrects in-progress cancellations
    missing from current input."""
    # Current input has no cancellations at all.
    current = _tu_feed_with([("T_running", gtfs_rt.TripDescriptor.SCHEDULED)])
    # Previous output had a cancellation for T_carried (still in service window)
    # and a cancellation for T_stale (already finished — should NOT be carried).
    previous = _tu_feed_with(
        [
            ("T_carried", gtfs_rt.TripDescriptor.CANCELED),
            ("T_stale", gtfs_rt.TripDescriptor.CANCELED),
        ]
    )
    schedule = _schedule_for(
        [
            {"trip_id": "T_carried", "departure_time": "09:00:00"},
            {"trip_id": "T_carried", "departure_time": "11:00:00"},
            {"trip_id": "T_stale", "departure_time": "06:00:00"},
            {"trip_id": "T_stale", "departure_time": "07:00:00"},
            {"trip_id": "T_running", "departure_time": "08:00:00"},
            {"trip_id": "T_running", "departure_time": "12:00:00"},
        ]
    )
    ctx = PipelineContext(
        inputs={"schedule": schedule, "previous_trip_updates": previous},
        output={"trip_updates": current},
    )

    ExpireCancelledTrips(
        previous_output_input="previous_trip_updates",
        now=_NOW_UTC_1000,
    ).apply(ctx)

    out = ctx.output["trip_updates"]
    trip_ids = [e.trip_update.trip.trip_id for e in out.entity]
    assert "T_carried" in trip_ids
    assert "T_running" in trip_ids
    assert "T_stale" not in trip_ids


def test_expire_cancelled_trips_carry_forward_does_not_leak_into_other_feeds():
    """Regression for #101: carry-forward touches only the target (trip_updates)
    feed — it must never inject trip_update entities into vehicle_positions.

    Asserts the structural invariant (trip_update-shaped entities land only in
    the trip_updates feed) rather than a single scenario, so the rule stays
    explicit for future refactors of ExpireCancelledTrips.
    """
    # Current trip_updates lacks the cancellation; previous output has it,
    # still within its service window so it qualifies for carry-forward.
    current_tu = _tu_feed_with([("T_running", gtfs_rt.TripDescriptor.SCHEDULED)])
    previous = _tu_feed_with([("T_carried", gtfs_rt.TripDescriptor.CANCELED)])

    # A separate vehicle_positions feed that does NOT reference T_carried —
    # pre-fix, the carry-forward fanned out over every feed and injected the
    # CANCELED trip_update here too.
    vp = gtfs_rt.FeedMessage()
    vp.header.gtfs_realtime_version = "2.0"
    ve = vp.entity.add()
    ve.id = "veh1"
    ve.vehicle.trip.trip_id = "OTHER"

    schedule = _schedule_for(
        [
            {"trip_id": "T_carried", "departure_time": "09:00:00"},
            {"trip_id": "T_carried", "departure_time": "11:00:00"},
            {"trip_id": "T_running", "departure_time": "08:00:00"},
            {"trip_id": "T_running", "departure_time": "12:00:00"},
        ]
    )
    ctx = PipelineContext(
        inputs={"schedule": schedule, "previous_trip_updates": previous},
        output={"trip_updates": current_tu, "vehicle_positions": vp},
    )

    ExpireCancelledTrips(
        previous_output_input="previous_trip_updates",
        now=_NOW_UTC_1000,
    ).apply(ctx)

    # Carry-forward landed in the trip_updates feed.
    tu_ids = [e.trip_update.trip.trip_id for e in ctx.output["trip_updates"].entity]
    assert "T_carried" in tu_ids

    # vehicle_positions is untouched: same single vehicle entity, and no
    # trip_update-shaped entity leaked in.
    vp_out = ctx.output["vehicle_positions"]
    assert [e.id for e in vp_out.entity] == ["veh1"]
    assert all(not e.HasField("trip_update") for e in vp_out.entity)


def test_expire_cancelled_trips_does_not_duplicate_when_carrying_forward():
    """If current input already has the cancellation,
    the previous-output entry is ignored."""
    current = _tu_feed_with([("T_carried", gtfs_rt.TripDescriptor.CANCELED)])
    previous = _tu_feed_with([("T_carried", gtfs_rt.TripDescriptor.CANCELED)])
    schedule = _schedule_for(
        [
            {"trip_id": "T_carried", "departure_time": "09:00:00"},
            {"trip_id": "T_carried", "departure_time": "11:00:00"},
        ]
    )
    ctx = PipelineContext(
        inputs={"schedule": schedule, "previous_trip_updates": previous},
        output={"trip_updates": current},
    )

    ExpireCancelledTrips(
        previous_output_input="previous_trip_updates",
        now=_NOW_UTC_1000,
    ).apply(ctx)

    out = ctx.output["trip_updates"]
    trip_ids = [e.trip_update.trip.trip_id for e in out.entity]
    assert trip_ids == ["T_carried"]


def test_expire_cancelled_trips_previous_input_missing_is_ok():
    """`previous_output_input` set but the slot has no value →
    behave like stateless mode."""
    current = _tu_feed_with([("T_running", gtfs_rt.TripDescriptor.SCHEDULED)])
    schedule = _schedule_for(
        [
            {"trip_id": "T_running", "departure_time": "08:00:00"},
            {"trip_id": "T_running", "departure_time": "12:00:00"},
        ]
    )
    ctx = PipelineContext(
        inputs={"schedule": schedule},  # no previous_trip_updates
        output={"trip_updates": current},
    )

    ExpireCancelledTrips(
        previous_output_input="previous_trip_updates",
        now=_NOW_UTC_1000,
    ).apply(ctx)

    assert [e.trip_update.trip.trip_id for e in ctx.output["trip_updates"].entity] == [
        "T_running"
    ]


def test_expire_cancelled_trips_resolves_agency_timezone():
    """Service-day clock honors agency.txt's timezone, not UTC."""
    feed = _tu_feed_with([("T_active", gtfs_rt.TripDescriptor.CANCELED)])
    # America/Los_Angeles trip running 09:00–11:00 local.
    schedule = _schedule_for(
        [
            {"trip_id": "T_active", "departure_time": "09:00:00"},
            {"trip_id": "T_active", "departure_time": "11:00:00"},
        ],
        agency_tz="America/Los_Angeles",
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    # Now is 10:00 LA local = 17:00 UTC. The cancellation must be kept; if the
    # builtin treated this as 17:00 service-day local, the trip would look
    # finished and the cancellation would be dropped.
    now = datetime(2026, 5, 20, 17, 0, tzinfo=UTC)
    ExpireCancelledTrips(now=now).apply(ctx)

    assert [e.trip_update.trip.trip_id for e in ctx.output["trip_updates"].entity] == [
        "T_active"
    ]


def test_insert_missing_cancellations_adds_for_overdue_trips():
    """Active scheduled trips past start+delay that aren't in input
    get appended as CANCELED."""
    feed = _tu_feed_with([("T_present", gtfs_rt.TripDescriptor.SCHEDULED)])
    schedule = _schedule_for(
        [
            # T_present: 08:00–09:00 — already in input, must NOT be cancelled
            {"trip_id": "T_present", "departure_time": "08:00:00"},
            {"trip_id": "T_present", "departure_time": "09:00:00"},
            # T_missing: 09:30–10:30 — overdue past delay, must be cancelled
            {"trip_id": "T_missing", "departure_time": "09:30:00"},
            {"trip_id": "T_missing", "departure_time": "10:30:00"},
            # T_upcoming: 11:00–12:00 — not yet past start+delay, must NOT be cancelled
            {"trip_id": "T_upcoming", "departure_time": "11:00:00"},
            {"trip_id": "T_upcoming", "departure_time": "12:00:00"},
            # T_old: 04:00–05:00 — trip already over, must NOT be cancelled
            {"trip_id": "T_old", "departure_time": "04:00:00"},
            {"trip_id": "T_old", "departure_time": "05:00:00"},
        ],
        trips_rows=[
            {"trip_id": "T_present", "route_id": "R1"},
            {"trip_id": "T_missing", "route_id": "R2"},
            {"trip_id": "T_upcoming", "route_id": "R3"},
            {"trip_id": "T_old", "route_id": "R4"},
        ],
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    InsertMissingCancellations(delay_seconds=300, now=_NOW_UTC_1000).apply(ctx)

    new_entities = {
        e.trip_update.trip.trip_id: e for e in ctx.output["trip_updates"].entity
    }
    assert (
        new_entities["T_present"].trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.SCHEDULED
    )
    missing = new_entities["T_missing"]
    assert (
        missing.trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.CANCELED
    )
    assert missing.trip_update.trip.route_id == "R2"
    assert missing.trip_update.trip.start_date == "20260520"
    assert "T_upcoming" not in new_entities
    assert "T_old" not in new_entities


def test_insert_missing_cancellations_skips_inactive_services():
    """Trips on a service_id that's not active today are NOT cancelled."""
    feed = _tu_feed_with([])
    # Two trips: T_active on S1 (default — all-day), T_inactive on S2 (no calendar row).
    schedule = _schedule_for(
        [
            {"trip_id": "T_active", "departure_time": "09:30:00"},
            {"trip_id": "T_active", "departure_time": "10:30:00"},
            {"trip_id": "T_inactive", "departure_time": "09:30:00"},
            {"trip_id": "T_inactive", "departure_time": "10:30:00"},
        ],
        trips_rows=[
            {"trip_id": "T_active", "route_id": "R1", "service_id": "S1"},
            {"trip_id": "T_inactive", "route_id": "R2", "service_id": "S_NEVER"},
        ],
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    InsertMissingCancellations(delay_seconds=300, now=_NOW_UTC_1000).apply(ctx)

    trip_ids = [e.trip_update.trip.trip_id for e in ctx.output["trip_updates"].entity]
    assert trip_ids == ["T_active"]


def test_insert_missing_cancellations_honors_calendar_dates_exceptions():
    """calendar_dates type 2 removes a service for the day; type 1 adds one."""
    feed = _tu_feed_with([])
    schedule = _schedule_for(
        [
            {"trip_id": "T_normal", "departure_time": "09:30:00"},
            {"trip_id": "T_normal", "departure_time": "10:30:00"},
            {"trip_id": "T_holiday", "departure_time": "09:30:00"},
            {"trip_id": "T_holiday", "departure_time": "10:30:00"},
        ],
        trips_rows=[
            {
                "trip_id": "T_normal",
                "route_id": "R1",
                "service_id": "S1",
            },  # default S1 calendar
            {"trip_id": "T_holiday", "route_id": "R2", "service_id": "S_HOL"},
        ],
        calendar_dates_rows=[
            {
                "service_id": "S1",
                "date": "20260520",
                "exception_type": 2,
            },  # remove S1 today
            {
                "service_id": "S_HOL",
                "date": "20260520",
                "exception_type": 1,
            },  # add S_HOL today
        ],
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    InsertMissingCancellations(delay_seconds=300, now=_NOW_UTC_1000).apply(ctx)

    trip_ids = [e.trip_update.trip.trip_id for e in ctx.output["trip_updates"].entity]
    assert trip_ids == ["T_holiday"]


def test_insert_missing_cancellations_skips_trips_in_vehicle_too():
    """Trips referenced only by a VehiclePosition in the target feed
    shouldn't get duplicated cancellations."""
    feed = gtfs_rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    e = feed.entity.add()
    e.id = "vp-1"
    e.vehicle.trip.trip_id = "T_running"

    schedule = _schedule_for(
        [
            {"trip_id": "T_running", "departure_time": "08:00:00"},
            {"trip_id": "T_running", "departure_time": "12:00:00"},
        ],
        trips_rows=[{"trip_id": "T_running", "route_id": "R1"}],
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    InsertMissingCancellations(delay_seconds=300, now=_NOW_UTC_1000).apply(ctx)

    # Vehicle entity counts as "present" so no cancellation appended.
    assert len(ctx.output["trip_updates"].entity) == 1


def test_insert_missing_cancellations_resolves_agency_timezone():
    """Active-service date is computed in agency local time, not UTC."""
    feed = _tu_feed_with([])
    schedule = _schedule_for(
        [
            {"trip_id": "T_overdue", "departure_time": "09:00:00"},
            {"trip_id": "T_overdue", "departure_time": "10:00:00"},
        ],
        trips_rows=[{"trip_id": "T_overdue", "route_id": "R1"}],
        agency_tz="America/Los_Angeles",
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    # 10:00 LA local = 17:00 UTC. T_overdue scheduled 09:00–10:00 LA, so it's
    # past end. start_s (09:00 = 32_400) <= threshold (35_700) so it qualifies
    # as overdue; end_s (36_000) == now (36_000) so it's still in-window.
    now = datetime(2026, 5, 20, 17, 0, tzinfo=UTC)
    InsertMissingCancellations(delay_seconds=300, now=now).apply(ctx)

    trip_ids = [e.trip_update.trip.trip_id for e in ctx.output["trip_updates"].entity]
    assert trip_ids == ["T_overdue"]
    assert (
        ctx.output["trip_updates"].entity[0].trip_update.trip.start_date == "20260520"
    )


def test_convert_scheduled_to_new_rewrites_trip_id_and_relationship():
    """SCHEDULED entities become ADDED with a new trip_id and route_id populated."""
    feed = _tu_feed_with(
        [
            ("orig_trip_1", gtfs_rt.TripDescriptor.SCHEDULED),
            ("orig_trip_2", gtfs_rt.TripDescriptor.SCHEDULED),
            (
                "orig_trip_added",
                gtfs_rt.TripDescriptor.ADDED,
            ),  # already added — leave alone
        ]
    )
    schedule = _schedule_for(
        [{"trip_id": "x", "departure_time": "08:00:00"}],
        trips_rows=[
            {"trip_id": "orig_trip_1", "route_id": "R1", "direction_id": "0"},
            {"trip_id": "orig_trip_2", "route_id": "R2", "direction_id": "1"},
        ],
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    step = ConvertScheduledToNew(trip_id_prefix="ADDED_")
    step.name = "convert"
    step.apply(ctx)

    entries = list(ctx.output["trip_updates"].entity)
    assert (
        entries[0].trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.ADDED
    )
    assert entries[0].trip_update.trip.trip_id.startswith("ADDED_orig_trip_1_")
    assert entries[0].trip_update.trip.route_id == "R1"
    assert entries[0].trip_update.trip.direction_id == 0

    assert (
        entries[1].trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.ADDED
    )
    assert entries[1].trip_update.trip.trip_id.startswith("ADDED_orig_trip_2_")
    assert entries[1].trip_update.trip.route_id == "R2"
    assert entries[1].trip_update.trip.direction_id == 1

    # Pre-existing ADDED entry is untouched
    assert entries[2].trip_update.trip.trip_id == "orig_trip_added"


def test_convert_scheduled_to_new_consistent_id_across_feeds():
    """A trip referenced by both a TripUpdate and a VehiclePosition
    gets the same new trip_id."""
    tu = _tu_feed_with([("orig", gtfs_rt.TripDescriptor.SCHEDULED)])
    vp = gtfs_rt.FeedMessage()
    vp.header.gtfs_realtime_version = "2.0"
    ve = vp.entity.add()
    ve.id = "v1"
    ve.vehicle.trip.trip_id = "orig"
    ve.vehicle.trip.schedule_relationship = gtfs_rt.TripDescriptor.SCHEDULED

    schedule = _schedule_for(
        [{"trip_id": "orig", "departure_time": "08:00:00"}],
        trips_rows=[{"trip_id": "orig", "route_id": "R1"}],
    )
    ctx = PipelineContext(
        inputs={"schedule": schedule},
        output={"trip_updates": tu, "vehicle_positions": vp},
    )
    ConvertScheduledToNew().apply(ctx)

    tu_new = ctx.output["trip_updates"].entity[0].trip_update.trip.trip_id
    vp_new = ctx.output["vehicle_positions"].entity[0].vehicle.trip.trip_id
    assert tu_new == vp_new
    assert tu_new != "orig"


def test_convert_scheduled_to_new_skips_unknown_trip_ids():
    """A SCHEDULED entity whose trip_id isn't in the schedule is left as-is."""
    feed = _tu_feed_with([("unknown_trip", gtfs_rt.TripDescriptor.SCHEDULED)])
    schedule = _schedule_for(
        [{"trip_id": "other", "departure_time": "08:00:00"}],
        trips_rows=[{"trip_id": "other", "route_id": "R"}],
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})
    ConvertScheduledToNew().apply(ctx)

    assert (
        ctx.output["trip_updates"].entity[0].trip_update.trip.trip_id == "unknown_trip"
    )
    assert (
        ctx.output["trip_updates"].entity[0].trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.SCHEDULED
    )


def test_convert_scheduled_to_new_route_ids_filter_only_converts_matching_routes():
    """With route_ids set, only trips scheduled on those routes are
    converted; others stay SCHEDULED."""
    feed = _tu_feed_with(
        [
            ("trip_on_R1", gtfs_rt.TripDescriptor.SCHEDULED),
            ("trip_on_R2", gtfs_rt.TripDescriptor.SCHEDULED),
            ("trip_on_R3", gtfs_rt.TripDescriptor.SCHEDULED),
            ("trip_no_route", gtfs_rt.TripDescriptor.SCHEDULED),
        ]
    )
    schedule = _schedule_for(
        [{"trip_id": "x", "departure_time": "08:00:00"}],
        trips_rows=[
            {"trip_id": "trip_on_R1", "route_id": "R1", "direction_id": "0"},
            {"trip_id": "trip_on_R2", "route_id": "R2", "direction_id": "1"},
            {"trip_id": "trip_on_R3", "route_id": "R3", "direction_id": "0"},
            {"trip_id": "trip_no_route", "route_id": "", "direction_id": "0"},
        ],
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    # Pass a set of routes — R1 and R3 convert, R2 and the route-less trip don't.
    ConvertScheduledToNew(route_ids={"R1", "R3"}).apply(ctx)

    entries = list(ctx.output["trip_updates"].entity)
    assert (
        entries[0].trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.ADDED
    )
    assert entries[0].trip_update.trip.trip_id.startswith("new_trip_on_R1_")
    assert entries[1].trip_update.trip.trip_id == "trip_on_R2"
    assert (
        entries[1].trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.SCHEDULED
    )
    assert (
        entries[2].trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.ADDED
    )
    assert entries[2].trip_update.trip.trip_id.startswith("new_trip_on_R3_")
    assert entries[3].trip_update.trip.trip_id == "trip_no_route"
    assert (
        entries[3].trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.SCHEDULED
    )


def test_convert_scheduled_to_new_route_ids_single_route_set():
    """A single-route set converts only that route's trips."""
    feed = _tu_feed_with(
        [
            ("trip_on_R1", gtfs_rt.TripDescriptor.SCHEDULED),
            ("trip_on_R2", gtfs_rt.TripDescriptor.SCHEDULED),
        ]
    )
    schedule = _schedule_for(
        [{"trip_id": "x", "departure_time": "08:00:00"}],
        trips_rows=[
            {"trip_id": "trip_on_R1", "route_id": "R1"},
            {"trip_id": "trip_on_R2", "route_id": "R2"},
        ],
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    ConvertScheduledToNew(route_ids={"R1"}).apply(ctx)

    entries = list(ctx.output["trip_updates"].entity)
    assert (
        entries[0].trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.ADDED
    )
    assert (
        entries[1].trip_update.trip.schedule_relationship
        == gtfs_rt.TripDescriptor.SCHEDULED
    )


def test_convert_scheduled_to_new_default_converts_all_routes():
    """Without route_id, every scheduled trip present in the schedule
    is converted (regression guard)."""
    feed = _tu_feed_with(
        [
            ("trip_on_R1", gtfs_rt.TripDescriptor.SCHEDULED),
            ("trip_on_R2", gtfs_rt.TripDescriptor.SCHEDULED),
        ]
    )
    schedule = _schedule_for(
        [{"trip_id": "x", "departure_time": "08:00:00"}],
        trips_rows=[
            {"trip_id": "trip_on_R1", "route_id": "R1"},
            {"trip_id": "trip_on_R2", "route_id": "R2"},
        ],
    )
    ctx = PipelineContext(inputs={"schedule": schedule}, output={"trip_updates": feed})

    ConvertScheduledToNew().apply(ctx)

    rels = [
        e.trip_update.trip.schedule_relationship
        for e in ctx.output["trip_updates"].entity
    ]
    assert rels == [gtfs_rt.TripDescriptor.ADDED, gtfs_rt.TripDescriptor.ADDED]


def test_rt_pipeline_with_synthetic():
    """Pipeline explicitly seeds ctx.output from the `feed` input."""
    from continuous_gtfs import step

    feed = _make_test_feed()

    @step(before="*")
    def init_output(ctx):
        ctx.output["feed"] = ctx.inputs["feed"]

    init_output.name = "init_output"

    rename = RenameVehicles(id_prefix="DEMO-")
    rename.name = "rename"
    header = UpdateFeedHeader(after=[rename])
    header.name = "header"

    result = run_realtime_pipeline({"feed": feed}, [init_output, rename, header])

    assert result.input_entities == 3
    assert result.output_entities == 3
    assert len(result.feeds) == 1
    assert result.feeds[0].name == "feed"
    assert len(result.feeds[0].pb) > 0
    assert len(result.feeds[0].json) > 0


def test_rt_pipeline_emits_per_feed_outputs():
    """Each named RT input becomes its own output when the init step seeds it."""
    from continuous_gtfs import step

    vp = _make_test_feed(2)
    tu = _make_test_feed(3)

    @step(before="*")
    def init_output(ctx):
        # Mirror each RT input by name — pipelines that want canonical
        # naming (or merges) would name their outputs differently here.
        for name, value in ctx.inputs.items():
            ctx.output[name] = value

    init_output.name = "init_output"

    rename = RenameVehicles(id_prefix="DEMO-")
    rename.name = "rename"

    result = run_realtime_pipeline(
        {"vehicle_positions": vp, "trip_updates": tu},
        [init_output, rename],
    )
    names = {f.name for f in result.feeds}
    assert names == {"vehicle_positions", "trip_updates"}
    assert all(f.entities > 0 for f in result.feeds)


def test_rt_pipeline_without_init_step_emits_no_feeds():
    """No auto-seed: if the pipeline doesn't populate ctx.output,
    result.feeds is empty."""
    feed = _make_test_feed()
    result = run_realtime_pipeline({"feed": feed}, [])
    assert result.feeds == []
    assert result.output_entities == 0
    # input_entities still counts what was supplied, for reporting
    assert result.input_entities == 3


def test_demo_realtime_fixture_dag_resolves_every_transform():
    """transforms.py declares exactly 3 steps (filter, header, passthrough) —
    ungated, so a transform dropped from the scan fails without needing the
    downloaded data."""
    fixture_dir = Path(__file__).parent / "fixtures" / "pipelines" / "demo_realtime"
    assert len(resolve_dag(scan_pipeline(fixture_dir))) == 3
