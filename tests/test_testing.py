"""Tests for the continuous_gtfs.testing public helper module.

Covers every exported symbol and validates the all-text-typing guarantee
that is the module's primary correctness invariant.
"""

import polars as pl
import pytest
from google.transit import gtfs_realtime_pb2 as gtfs_rt

from continuous_gtfs import PipelineContext
from continuous_gtfs.builtins.schedule import MatchCondition, RemoveRows, UpdateFields
from continuous_gtfs.testing import (
    assert_unchanged,
    gtfs_df,
    realtime_context,
    schedule_context,
    trip_updates_feed,
    vehicle_positions_feed,
)

# ---------------------------------------------------------------------------
# gtfs_df
# ---------------------------------------------------------------------------


def test_gtfs_df_returns_dataframe():
    df = gtfs_df("col_a,col_b\nfoo,bar\n")
    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["col_a", "col_b"]
    assert df.shape == (1, 2)


def test_gtfs_df_all_columns_are_string():
    """Every column must be Utf8 — the all-text-typing guarantee."""
    df = gtfs_df(
        """
        service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday
        LIVE,1,0,0,0,0,0,0
        DEAD,0,0,0,0,0,0,0
        """
    )
    for col in df.columns:
        assert df[col].dtype == pl.String, (
            f"Column '{col}' has dtype {df[col].dtype}, expected pl.String. "
            "gtfs_df must force all columns to Utf8 so MatchCondition comparisons work."
        )


def test_gtfs_df_strips_leading_indentation():
    """Indented CSV blocks must dedent cleanly without leaking whitespace."""
    df = gtfs_df(
        """
        stop_id,stop_name
        S1,Main St
        S2,Oak Ave
        """
    )
    assert df["stop_id"].to_list() == ["S1", "S2"]
    assert df["stop_name"].to_list() == ["Main St", "Oak Ave"]


def test_gtfs_df_all_text_prevents_silent_match_failure():
    """MatchCondition(value='0') must match when columns are text, not int.

    This is the core motivation for gtfs_df: without infer_schema_length=0,
    polars infers '0'/'1' flags as integers and the MatchCondition never fires.
    """
    calendar = gtfs_df(
        """
        service_id,monday,tuesday
        DEAD,0,0
        LIVE,1,0
        """
    )

    ctx = schedule_context(**{"calendar.txt": calendar})
    step_inst = RemoveRows(
        "calendar.txt",
        [MatchCondition("monday", value="0")],
    )
    step_inst.name = "remove_dead"
    step_inst.apply(ctx)

    result = ctx.output["calendar.txt"]["service_id"].to_list()
    assert result == ["LIVE"], (
        f"Expected ['LIVE'] but got {result!r}. "
        "This usually means columns are not Utf8 — "
        "check gtfs_df forces infer_schema_length=0."
    )


# ---------------------------------------------------------------------------
# schedule_context
# ---------------------------------------------------------------------------


def test_schedule_context_returns_pipeline_context():
    stops = gtfs_df("stop_id,stop_name\nS1,Main\n")
    ctx = schedule_context(**{"stops.txt": stops})
    assert isinstance(ctx, PipelineContext)


def test_schedule_context_seeds_output():
    stops = gtfs_df("stop_id,stop_name\nS1,Main\n")
    routes = gtfs_df("route_id,route_type\nR1,3\n")
    ctx = schedule_context(**{"stops.txt": stops, "routes.txt": routes})
    assert set(ctx.output.keys()) == {"stops.txt", "routes.txt"}
    assert ctx.output["stops.txt"].shape == (1, 2)


def test_schedule_context_step_can_mutate_output():
    """A builtin step applied to the context mutates ctx.output correctly."""
    routes = gtfs_df(
        """
        route_id,route_short_name
        R1,Bus 1
        R2,Bus 2
        """
    )
    ctx = schedule_context(**{"routes.txt": routes})
    step_inst = UpdateFields(
        "routes.txt",
        [MatchCondition("route_id", value="R1")],
        {"route_short_name": "Shuttle 1"},
    )
    step_inst.name = "rename_r1"
    step_inst.apply(ctx)

    result = ctx.output["routes.txt"]
    assert (
        result.filter(pl.col("route_id") == "R1")["route_short_name"][0] == "Shuttle 1"
    )
    assert result.filter(pl.col("route_id") == "R2")["route_short_name"][0] == "Bus 2"


# ---------------------------------------------------------------------------
# realtime_context
# ---------------------------------------------------------------------------


def test_realtime_context_returns_pipeline_context():
    feed = trip_updates_feed(trip_id="T1", stop_ids=["S1"])
    ctx = realtime_context(trip_updates=feed)
    assert isinstance(ctx, PipelineContext)


def test_realtime_context_seeds_output():
    tu = trip_updates_feed(trip_id="T1", stop_ids=["S1", "S2"])
    vp = vehicle_positions_feed(vehicle_ids=["V1"])
    ctx = realtime_context(trip_updates=tu, vehicle_positions=vp)
    assert set(ctx.output.keys()) == {"trip_updates", "vehicle_positions"}
    assert ctx.output["trip_updates"] is tu
    assert ctx.output["vehicle_positions"] is vp


# ---------------------------------------------------------------------------
# trip_updates_feed
# ---------------------------------------------------------------------------


def test_trip_updates_feed_returns_feed_message():
    feed = trip_updates_feed(trip_id="T1", stop_ids=["S1", "S2"])
    assert isinstance(feed, gtfs_rt.FeedMessage)


def test_trip_updates_feed_single_entity():
    feed = trip_updates_feed(trip_id="trip_42", stop_ids=["S1", "S2", "S3"])
    assert len(feed.entity) == 1
    assert feed.entity[0].id == "trip_42"
    assert feed.entity[0].trip_update.trip.trip_id == "trip_42"


def test_trip_updates_feed_stop_time_updates():
    feed = trip_updates_feed(trip_id="T1", stop_ids=["S1", "S2", "S3"])
    stus = feed.entity[0].trip_update.stop_time_update
    assert len(stus) == 3
    assert stus[0].stop_id == "S1"
    assert stus[0].stop_sequence == 1
    assert stus[1].stop_id == "S2"
    assert stus[1].stop_sequence == 2
    assert stus[2].stop_id == "S3"
    assert stus[2].stop_sequence == 3


def test_trip_updates_feed_empty_stops():
    feed = trip_updates_feed(trip_id="T1", stop_ids=[])
    assert len(feed.entity) == 1
    assert len(feed.entity[0].trip_update.stop_time_update) == 0


def test_trip_updates_feed_header():
    feed = trip_updates_feed(trip_id="T1", stop_ids=["S1"])
    assert feed.header.gtfs_realtime_version == "2.0"
    assert feed.header.timestamp == 1_712_345_678


# ---------------------------------------------------------------------------
# vehicle_positions_feed
# ---------------------------------------------------------------------------


def test_vehicle_positions_feed_returns_feed_message():
    feed = vehicle_positions_feed(vehicle_ids=["V1", "V2"])
    assert isinstance(feed, gtfs_rt.FeedMessage)


def test_vehicle_positions_feed_entity_count():
    feed = vehicle_positions_feed(vehicle_ids=["V1", "V2", "V3"])
    assert len(feed.entity) == 3


def test_vehicle_positions_feed_default_trip_ids():
    feed = vehicle_positions_feed(vehicle_ids=["V1", "V2"])
    assert feed.entity[0].vehicle.trip.trip_id == "trip_V1"
    assert feed.entity[1].vehicle.trip.trip_id == "trip_V2"


def test_vehicle_positions_feed_default_stop_ids():
    feed = vehicle_positions_feed(vehicle_ids=["V1"])
    assert feed.entity[0].vehicle.stop_id == "S0"


def test_vehicle_positions_feed_explicit_trip_and_stop_ids():
    feed = vehicle_positions_feed(
        vehicle_ids=["V1", "V2"],
        trip_ids=["T10", "T20"],
        stop_ids=["S10", "S20"],
    )
    assert feed.entity[0].vehicle.trip.trip_id == "T10"
    assert feed.entity[0].vehicle.stop_id == "S10"
    assert feed.entity[1].vehicle.trip.trip_id == "T20"
    assert feed.entity[1].vehicle.stop_id == "S20"


def test_vehicle_positions_feed_vehicle_ids_set():
    feed = vehicle_positions_feed(vehicle_ids=["V1", "V2"])
    assert feed.entity[0].id == "V1"
    assert feed.entity[0].vehicle.vehicle.id == "V1"
    assert feed.entity[1].id == "V2"
    assert feed.entity[1].vehicle.vehicle.id == "V2"


def test_vehicle_positions_feed_header():
    feed = vehicle_positions_feed(vehicle_ids=["V1"])
    assert feed.header.gtfs_realtime_version == "2.0"
    assert feed.header.timestamp == 1_712_345_678


# ---------------------------------------------------------------------------
# assert_unchanged
# ---------------------------------------------------------------------------


def test_assert_unchanged_passes_when_only_excluded_row_changes():
    before = gtfs_df(
        """
        stop_id,stop_name
        S1,Main St
        S2,Oak Ave
        S3,Park Blvd
        """
    )
    # Simulate a transform that only renames S1.
    after = before.with_columns(
        pl.when(pl.col("stop_id") == "S1")
        .then(pl.lit("Concert Hall"))
        .otherwise(pl.col("stop_name"))
        .alias("stop_name")
    )
    # Should not raise: only S1 changed, S2 and S3 are untouched.
    assert_unchanged(before, after, excluding=pl.col("stop_id") == "S1")


def test_assert_unchanged_fails_when_non_excluded_row_changes():
    before = gtfs_df(
        """
        stop_id,stop_name
        S1,Main St
        S2,Oak Ave
        """
    )
    # Simulates a bug: S2 was also modified unexpectedly.
    after = before.with_columns(
        pl.when(pl.col("stop_id") == "S2")
        .then(pl.lit("CHANGED"))
        .otherwise(pl.col("stop_name"))
        .alias("stop_name")
    )
    with pytest.raises(AssertionError, match="row"):
        assert_unchanged(before, after, excluding=pl.col("stop_id") == "S1")


def test_assert_unchanged_fails_on_unexpected_row_removal():
    before = gtfs_df(
        """
        stop_id,stop_name
        S1,Main St
        S2,Oak Ave
        S3,Park Blvd
        """
    )
    # Simulate a transform that removes S3 in addition to S1 — a bug.
    after = before.filter(pl.col("stop_id") != "S3")
    with pytest.raises(AssertionError, match="count changed"):
        assert_unchanged(before, after, excluding=pl.col("stop_id") == "S1")


def test_assert_unchanged_passes_when_all_rows_excluded():
    before = gtfs_df("stop_id,stop_name\nS1,Main\n")
    after = before.with_columns(pl.lit("Changed").alias("stop_name"))
    # Excluding everything — nothing to check; should pass.
    assert_unchanged(before, after, excluding=pl.col("stop_id") == "S1")


def test_assert_unchanged_passes_on_identical_frames():
    df = gtfs_df("route_id,route_type\nR1,3\nR2,3\n")
    assert_unchanged(df, df, excluding=pl.col("route_id") == "NONEXISTENT")


def test_assert_unchanged_tolerates_row_reordering_in_unchanged_set():
    """Row order changes within the unchanged set must not cause false failures."""
    before = gtfs_df(
        """
        stop_id,stop_name
        S1,Main St
        S2,Oak Ave
        S3,Park Blvd
        """
    )
    # Simulate a transform that removes S1 and returns rows in reversed order.
    after = before.filter(pl.col("stop_id") != "S1").reverse()
    # S3 and S2 are the unchanged rows — verify they're identical despite reordering.
    assert_unchanged(before, after, excluding=pl.col("stop_id") == "S1")


# ---------------------------------------------------------------------------
# End-to-end: build context, apply real builtin, assert
# ---------------------------------------------------------------------------


def test_end_to_end_schedule_step_with_helpers():
    """Demonstrate the full pattern: gtfs_df → schedule_context → step → assert.

    Uses a regex MatchCondition to remove multiple expansion stops at once.
    (Conditions within one RemoveRows use AND logic — to OR across stop IDs,
    use either a regex or separate RemoveRows steps.)
    """
    stops = gtfs_df(
        """
        stop_id,stop_name,stop_lat,stop_lon
        E01,Expansion Stop 1,47.6,122.3
        E07,Expansion Stop 7,47.7,122.4
        455,Old Town,47.608,122.337
        """
    )
    ctx = schedule_context(**{"stops.txt": stops})

    # Regex matches E01 and E07; 455 is unaffected.
    step_inst = RemoveRows(
        "stops.txt",
        [MatchCondition("stop_id", regex=r"^E\d+$")],
    )
    step_inst.name = "remove_expansion_stops"
    step_inst.apply(ctx)

    result = ctx.output["stops.txt"]
    remaining_ids = result["stop_id"].to_list()
    assert "E01" not in remaining_ids
    assert "E07" not in remaining_ids
    assert "455" in remaining_ids
    assert len(result) == 1


def test_end_to_end_realtime_step_with_helpers():
    """Demonstrate the full RT pattern: feed builders → realtime_context → step."""
    from continuous_gtfs.builtins.realtime import FilterStopsByID

    feed = trip_updates_feed(trip_id="T1", stop_ids=["E01", "S2", "E07"])
    ctx = realtime_context(trip_updates=feed)

    step_inst = FilterStopsByID(blocked_stop_ids={"E01", "E07"})
    step_inst.name = "filter_expansion_stops"
    step_inst.apply(ctx)

    output_feed = ctx.output["trip_updates"]
    assert len(output_feed.entity) == 1
    stus = output_feed.entity[0].trip_update.stop_time_update
    remaining_stop_ids = [stu.stop_id for stu in stus]
    assert "E01" not in remaining_stop_ids
    assert "E07" not in remaining_stop_ids
    assert "S2" in remaining_stop_ids


def test_end_to_end_with_assert_unchanged():
    """assert_unchanged catches unintended side-effects in a full step invocation."""
    stops = gtfs_df(
        """
        stop_id,stop_name
        E01,Expansion 1
        S1,Main St
        S2,Oak Ave
        """
    )
    ctx = schedule_context(**{"stops.txt": stops})
    before_snapshot = ctx.output["stops.txt"].clone()

    step_inst = RemoveRows(
        "stops.txt",
        [MatchCondition("stop_id", value="E01")],
    )
    step_inst.name = "remove_e01"
    step_inst.apply(ctx)

    # Verify the step only touched E01.
    assert_unchanged(
        before_snapshot,
        ctx.output["stops.txt"],
        excluding=pl.col("stop_id") == "E01",
    )
    # Also confirm E01 is gone.
    assert "E01" not in ctx.output["stops.txt"]["stop_id"].to_list()
