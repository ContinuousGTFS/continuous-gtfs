"""Unit + oracle tests for the schedule-semantics derivation
(specs/schedule-semantics.md).

Rule tests build small canonical-shaped polars DataFrames by hand (the same
shape `tables_from_archive`/`tables_from_digest` produce — string columns,
"" for blank, per schedule-canonical-form.md) and call `derive_semantics`
directly, one rule per test.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from continuous_gtfs.schedule_semantics import (
    SEMANTICS_REV,
    derive_semantics,
    identity_key_of,
    pattern_id_of,
    profile_id_of,
    tables_from_digest,
    write_semantics,
)

# --- Small canonical-shaped table builders ---------------------------------


def _trips(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _stop_times(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _calendar(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _calendar_dates(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _stops(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _feed_info(row: dict) -> pl.DataFrame:
    return pl.DataFrame([row])


_WEEKDAY_ALL = {
    "monday": "1",
    "tuesday": "1",
    "wednesday": "1",
    "thursday": "1",
    "friday": "1",
    "saturday": "1",
    "sunday": "1",
}


# --- Hash preimage test vectors (pin the byte-exact preimages the module
# docstring documents — a change to either preimage bumps SEMANTICS_REV) ---


def test_pattern_id_hash_vector():
    # preimage: "R1" + US + "0" + US + RS.join(["S1","S2"])
    assert (
        pattern_id_of("R1", "0", ["S1", "S2"])
        == "9104cd8465934b7005e5908889dc6e737a8c323f1db25ec7422de18cd08d91a4"
    )


def test_pattern_id_unstated_direction_uses_sentinel():
    # unstated direction hashes as "-", not "" — distinct from a real
    # (hypothetical) blank-string direction and from "0"/"1".
    assert (
        pattern_id_of("R1", "", ["S1", "S2"])
        == "da755a06913858d0d1a14d061c08669562c5c11e633f14b624f9edba44073211"
    )
    assert pattern_id_of("R1", "", ["S1", "S2"]) != pattern_id_of(
        "R1", "0", ["S1", "S2"]
    )


def test_profile_id_hash_vector():
    # preimage: "0" US "5" RS "60" US ""  (a null dwell serializes as "")
    assert (
        profile_id_of([0, 60], [5, None])
        == "e471fa158a56ad64c2326422704ae3c370f4b19c2b53f75cc7c5c402039484ec"
    )


def test_identity_key_format_matches_trip_matching_ts():
    # Byte-for-byte the same format as trip-matching.ts's identityKey():
    # route_id|first_departure_seconds|direction|station_ids joined by ">".
    assert identity_key_of("R1", 32400, "0", ["S1", "S2"]) == "R1|32400|0|S1>S2"
    assert identity_key_of("R1", 32400, "", ["S1", "S2"]) == "R1|32400|-|S1>S2"


# --- Service dates ----------------------------------------------------------


def test_calendar_dates_only_service_is_valid():
    trips = _trips([{"route_id": "R1", "service_id": "SVC-EXC", "trip_id": "T1"}])
    stop_times = _stop_times(
        [
            {
                "trip_id": "T1",
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "S1",
                "stop_sequence": "1",
            },
            {
                "trip_id": "T1",
                "arrival_time": "09:10:00",
                "departure_time": "09:10:00",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
        ]
    )
    calendar_dates = _calendar_dates(
        [{"service_id": "SVC-EXC", "date": "20260115", "exception_type": "1"}]
    )
    out = derive_semantics(
        {"trips": trips, "stop_times": stop_times, "calendar_dates": calendar_dates}
    )
    dates = set(
        zip(
            out["service_dates"]["service_id"].to_list(),
            out["service_dates"]["date"].to_list(),
            strict=False,
        )
    )
    assert dates == {("SVC-EXC", "20260115")}


def test_exception_removes_a_rule_date():
    trips = _trips([{"route_id": "R1", "service_id": "SVC1", "trip_id": "T1"}])
    stop_times = _stop_times(
        [
            {
                "trip_id": "T1",
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "S1",
                "stop_sequence": "1",
            },
            {
                "trip_id": "T1",
                "arrival_time": "09:10:00",
                "departure_time": "09:10:00",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
        ]
    )
    calendar = _calendar(
        [
            {
                "service_id": "SVC1",
                "start_date": "20260105",
                "end_date": "20260109",
                **_WEEKDAY_ALL,
            }
        ]
    )
    calendar_dates = _calendar_dates(
        [{"service_id": "SVC1", "date": "20260107", "exception_type": "2"}]
    )
    out = derive_semantics(
        {
            "trips": trips,
            "stop_times": stop_times,
            "calendar": calendar,
            "calendar_dates": calendar_dates,
        }
    )
    dates = set(
        out["service_dates"].filter(pl.col("service_id") == "SVC1")["date"].to_list()
    )
    assert dates == {"20260105", "20260106", "20260108", "20260109"}
    assert "20260107" not in dates


def test_window_clips_calendar_to_feed_info():
    trips = _trips([{"route_id": "R1", "service_id": "SVC1", "trip_id": "T1"}])
    stop_times = _stop_times(
        [
            {
                "trip_id": "T1",
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "S1",
                "stop_sequence": "1",
            },
            {
                "trip_id": "T1",
                "arrival_time": "09:10:00",
                "departure_time": "09:10:00",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
        ]
    )
    calendar = _calendar(
        [
            {
                "service_id": "SVC1",
                "start_date": "20260101",
                "end_date": "20260131",
                **_WEEKDAY_ALL,
            }
        ]
    )
    feed_info = _feed_info(
        {"feed_start_date": "20260110", "feed_end_date": "20260112", "feed_id": "x"}
    )
    out = derive_semantics(
        {
            "trips": trips,
            "stop_times": stop_times,
            "calendar": calendar,
            "feed_info": feed_info,
        }
    )
    dates = sorted(out["service_dates"]["date"].to_list())
    assert dates == ["20260110", "20260111", "20260112"]


# --- Clocks / profiles -------------------------------------------------------


def test_late_night_departure_unwraps_past_midnight():
    trips = _trips([{"route_id": "R1", "service_id": "SVC1", "trip_id": "T1"}])
    stop_times = _stop_times(
        [
            {
                "trip_id": "T1",
                "arrival_time": "25:30:00",
                "departure_time": "25:30:00",
                "stop_id": "S1",
                "stop_sequence": "1",
            },
            {
                "trip_id": "T1",
                "arrival_time": "25:45:00",
                "departure_time": "25:45:00",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
        ]
    )
    out = derive_semantics({"trips": trips, "stop_times": stop_times})
    row = out["trips"].row(0, named=True)
    assert row["first_departure_seconds"] == 91_800
    assert row["start_seconds"] == 91_800


def test_blank_intermediate_stop_time_yields_null_offset_but_keeps_profile():
    trips = _trips([{"route_id": "R1", "service_id": "SVC1", "trip_id": "T1"}])
    stop_times = _stop_times(
        [
            {
                "trip_id": "T1",
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "S1",
                "stop_sequence": "1",
            },
            {
                "trip_id": "T1",
                "arrival_time": "",
                "departure_time": "",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
            {
                "trip_id": "T1",
                "arrival_time": "09:20:00",
                "departure_time": "09:20:00",
                "stop_id": "S3",
                "stop_sequence": "3",
            },
        ]
    )
    out = derive_semantics({"trips": trips, "stop_times": stop_times})
    row = out["trips"].row(0, named=True)
    assert row["profile_id"] is not None and row["profile_id"] != ""
    profile = (
        out["time_profiles"]
        .filter(pl.col("profile_id") == row["profile_id"])
        .row(0, named=True)
    )
    assert profile["offsets"] == [0, None, 1200]
    assert profile["dwells"] == [0, None, 0]


# --- Patterns / identity ------------------------------------------------------


def test_undirected_trip_direction_is_its_own_value():
    trips = _trips(
        [{"route_id": "R1", "service_id": "SVC1", "trip_id": "T1"}]
    )  # no direction_id
    stop_times = _stop_times(
        [
            {
                "trip_id": "T1",
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "S1",
                "stop_sequence": "1",
            },
            {
                "trip_id": "T1",
                "arrival_time": "09:10:00",
                "departure_time": "09:10:00",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
        ]
    )
    out = derive_semantics({"trips": trips, "stop_times": stop_times})
    pattern = out["patterns"].row(0, named=True)
    assert pattern["direction_id"] == ""
    trip_row = out["trips"].row(0, named=True)
    assert trip_row["identity_key"] == "R1|32400|-|S1>S2"


def test_child_stop_with_and_without_parent_station():
    trips = _trips([{"route_id": "R1", "service_id": "SVC1", "trip_id": "T1"}])
    stop_times = _stop_times(
        [
            {
                "trip_id": "T1",
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "PLATFORM_A",
                "stop_sequence": "1",
            },
            {
                "trip_id": "T1",
                "arrival_time": "09:10:00",
                "departure_time": "09:10:00",
                "stop_id": "STANDALONE_B",
                "stop_sequence": "2",
            },
        ]
    )
    stops = _stops(
        [
            {"stop_id": "PLATFORM_A", "parent_station": "STATION_P"},
            {"stop_id": "STANDALONE_B", "parent_station": ""},
        ]
    )
    out = derive_semantics({"trips": trips, "stop_times": stop_times, "stops": stops})
    pattern = out["patterns"].row(0, named=True)
    assert pattern["stop_ids"] == ["PLATFORM_A", "STANDALONE_B"]
    assert pattern["station_ids"] == ["STATION_P", "STANDALONE_B"]


def test_two_trips_same_stops_and_timing_share_pattern_and_profile():
    trips = _trips(
        [
            {
                "route_id": "R1",
                "service_id": "SVC1",
                "trip_id": "T1",
                "direction_id": "0",
            },
            {
                "route_id": "R1",
                "service_id": "SVC2",
                "trip_id": "T2",
                "direction_id": "0",
            },
        ]
    )
    stop_times_rows = []
    for trip_id in ("T1", "T2"):
        stop_times_rows += [
            {
                "trip_id": trip_id,
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "S1",
                "stop_sequence": "1",
            },
            {
                "trip_id": trip_id,
                "arrival_time": "09:10:00",
                "departure_time": "09:10:00",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
        ]
    stop_times = _stop_times(stop_times_rows)
    out = derive_semantics({"trips": trips, "stop_times": stop_times})
    t1 = out["trips"].filter(pl.col("trip_id") == "T1").row(0, named=True)
    t2 = out["trips"].filter(pl.col("trip_id") == "T2").row(0, named=True)
    assert t1["pattern_id"] == t2["pattern_id"]
    assert t1["profile_id"] == t2["profile_id"]
    assert out["patterns"].height == 1
    assert out["time_profiles"].height == 1


def test_child_stop_swap_changes_pattern_not_station_or_identity():
    # Two trips on the same route/direction/departure, both serving station
    # STATION_P at position 0 (via different child platforms) then the same
    # stop S2 at position 1.
    trips = _trips(
        [
            {
                "route_id": "R1",
                "service_id": "SVC1",
                "trip_id": "T1",
                "direction_id": "0",
            },
            {
                "route_id": "R1",
                "service_id": "SVC2",
                "trip_id": "T2",
                "direction_id": "0",
            },
        ]
    )
    stop_times = _stop_times(
        [
            {
                "trip_id": "T1",
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "PLATFORM_A",
                "stop_sequence": "1",
            },
            {
                "trip_id": "T1",
                "arrival_time": "09:10:00",
                "departure_time": "09:10:00",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
            {
                "trip_id": "T2",
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "PLATFORM_B",
                "stop_sequence": "1",
            },
            {
                "trip_id": "T2",
                "arrival_time": "09:10:00",
                "departure_time": "09:10:00",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
        ]
    )
    stops = _stops(
        [
            {"stop_id": "PLATFORM_A", "parent_station": "STATION_P"},
            {"stop_id": "PLATFORM_B", "parent_station": "STATION_P"},
            {"stop_id": "S2", "parent_station": ""},
        ]
    )
    out = derive_semantics({"trips": trips, "stop_times": stop_times, "stops": stops})
    t1 = out["trips"].filter(pl.col("trip_id") == "T1").row(0, named=True)
    t2 = out["trips"].filter(pl.col("trip_id") == "T2").row(0, named=True)

    assert t1["pattern_id"] != t2["pattern_id"]  # different child stop_ids
    assert t1["identity_key"] == t2["identity_key"]  # same station-level identity

    p1 = (
        out["patterns"]
        .filter(pl.col("pattern_id") == t1["pattern_id"])
        .row(0, named=True)
    )
    p2 = (
        out["patterns"]
        .filter(pl.col("pattern_id") == t2["pattern_id"])
        .row(0, named=True)
    )
    assert p1["stop_ids"] != p2["stop_ids"]
    assert p1["station_ids"] == p2["station_ids"] == ["STATION_P", "S2"]


# --- Determinism --------------------------------------------------------------


def test_derivation_and_parquet_write_are_byte_identical_across_runs(tmp_path: Path):
    trips = _trips(
        [
            {
                "route_id": "R1",
                "service_id": "SVC1",
                "trip_id": "T2",
                "direction_id": "0",
            },
            {
                "route_id": "R1",
                "service_id": "SVC1",
                "trip_id": "T1",
                "direction_id": "0",
            },
        ]
    )
    stop_times_rows = []
    for trip_id in ("T2", "T1"):  # deliberately out of trip_id order
        stop_times_rows += [
            {
                "trip_id": trip_id,
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "S1",
                "stop_sequence": "1",
            },
            {
                "trip_id": trip_id,
                "arrival_time": "09:10:00",
                "departure_time": "09:10:00",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
        ]
    stop_times = _stop_times(stop_times_rows)
    calendar = _calendar(
        [
            {
                "service_id": "SVC1",
                "start_date": "20260101",
                "end_date": "20260103",
                **_WEEKDAY_ALL,
            }
        ]
    )
    tables = {"trips": trips, "stop_times": stop_times, "calendar": calendar}

    derived_a = derive_semantics(tables)
    derived_b = derive_semantics(tables)

    for name in ("service_dates", "patterns", "time_profiles", "trips"):
        assert derived_a[name].equals(derived_b[name])

    digest = "v1:testdigest"
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    write_semantics(derived_a, str(dir_a), digest)
    write_semantics(derived_b, str(dir_b), digest)

    for name in ("service_dates", "patterns", "time_profiles", "trips"):
        bytes_a = (dir_a / f"_feed_digest={digest}" / f"{name}.parquet").read_bytes()
        bytes_b = (dir_b / f"_feed_digest={digest}" / f"{name}.parquet").read_bytes()
        assert bytes_a == bytes_b


def test_write_semantics_skip_if_exists_and_round_trip(tmp_path: Path):
    trips = _trips([{"route_id": "R1", "service_id": "SVC1", "trip_id": "T1"}])
    stop_times = _stop_times(
        [
            {
                "trip_id": "T1",
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "S1",
                "stop_sequence": "1",
            },
            {
                "trip_id": "T1",
                "arrival_time": "09:10:00",
                "departure_time": "09:10:00",
                "stop_id": "S2",
                "stop_sequence": "2",
            },
        ]
    )
    derived = derive_semantics({"trips": trips, "stop_times": stop_times})
    digest = "v1:round-trip-test"
    base = str(tmp_path)

    assert write_semantics(derived, base, digest) is True
    assert write_semantics(derived, base, digest) is False  # skip-if-exists

    marker = tmp_path / f"_feed_digest={digest}" / "metadata.json"
    assert marker.exists()
    metadata = json.loads(marker.read_text())
    assert metadata["feed_digest"] == digest
    assert metadata["rev"] == SEMANTICS_REV

    read_back = tables_from_digest(base, digest)
    # tables_from_digest reads CANONICAL table names (trips, stop_times, ...);
    # this directory holds the semantic tables instead, so only the
    # coincidentally-named "trips.parquet" resolves — round-trip the
    # semantic trips table explicitly via read_table-equivalent columns.
    assert "trips" in read_back
    assert set(read_back["trips"].columns) == set(derived["trips"].columns)
