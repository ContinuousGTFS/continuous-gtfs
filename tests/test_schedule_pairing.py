"""Unit tests for schedule trip pairing (specs/schedule-semantics.md
§Trip Pairing).

The "pairing pass" and "carriers and the counterpart tie-break" sections
port a sibling TypeScript suite's cases, case for case (case names kept
so the two suites stay alignable), building each
side's semantic tables directly from trip specs shaped like the browser
suite's `trip()` helper (`_side_tables` below) rather than going through a
full canonical GTFS feed — the same shortcut the browser tests take by
constructing `SideTrip` objects directly.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from continuous_gtfs.schedule_helpers import gtfs_time_to_seconds
from continuous_gtfs.schedule_pairing import (
    PAIRING_REV,
    is_strict_subsequence,
    pair_trips,
    write_trip_pairs,
)
from continuous_gtfs.schedule_semantics import (
    identity_key_of,
    pattern_id_of,
    profile_id_of,
)

# --- Building a side's semantic tables from trip specs ---------------------
#
# Mirrors trip-matching.test.ts's `trip()` helper and its fixture timing
# arrays, so each ported case reads the same shape as its TS counterpart.

# Two platforms of one station; every other stop is its own station.
PARENTS: dict[str, str] = {"P1": "PX", "P2": "PX"}


def _parent_of(stop_id: str) -> str:
    return PARENTS.get(stop_id, stop_id)


MAIN = [(0, 0), (420, 60), (1080, 0)]
MAIN_SLOW = [(0, 0), (480, 60), (1200, 0)]
LOOP = [(0, 0), (360, 60), (900, 60), (1560, 0)]
# The loop without its X call, keeping the loop's clock at the common stops.
LOOP_SKIP = [(0, 0), (900, 60), (1560, 0)]

_TRIPS_SCHEMA = {
    "trip_id": pl.Utf8,
    "pattern_id": pl.Utf8,
    "profile_id": pl.Utf8,
    "start_seconds": pl.Int64,
    "first_departure_seconds": pl.Int64,
    "last_arrival_seconds": pl.Int64,
    "stop_events": pl.Int64,
    "identity_key": pl.Utf8,
}
_PATTERNS_SCHEMA = {
    "pattern_id": pl.Utf8,
    "route_id": pl.Utf8,
    "direction_id": pl.Utf8,
    "stop_ids": pl.List(pl.Utf8),
    "station_ids": pl.List(pl.Utf8),
}
_TIME_PROFILES_SCHEMA = {
    "profile_id": pl.Utf8,
    "offsets": pl.List(pl.Int64),
    "dwells": pl.List(pl.Int64),
}
_CANONICAL_TRIPS_SCHEMA = {"trip_id": pl.Utf8, "service_id": pl.Utf8}


def trip(
    trip_id: str,
    *,
    route_id: str = "R1",
    service_id: str = "WK",
    direction_id: str = "0",
    start: str = "07:15:00",
    stops: list[str] | None = None,
    times: list[tuple[int, int]] | None = None,
) -> dict:
    """One trip spec, defaulted like trip-matching.test.ts's `trip()`."""
    return {
        "trip_id": trip_id,
        "route_id": route_id,
        "service_id": service_id,
        "direction_id": direction_id,
        "start": start,
        "stops": stops if stops is not None else ["A", "P1", "C"],
        "times": times if times is not None else MAIN,
    }


def _side_tables(specs: list[dict]) -> tuple[dict[str, pl.DataFrame], pl.DataFrame]:
    """Build (semantic tables, canonical trips) for a list of trip specs —
    the same three semantic tables `schedule_semantics.derive_semantics`
    would produce, computed directly from the specs so tests don't need a
    full canonical GTFS feed."""
    trip_rows: list[dict] = []
    patterns_map: dict[str, dict] = {}
    profiles_map: dict[str, dict] = {}
    canonical_rows: list[dict] = []

    for spec in specs:
        stop_ids = list(spec["stops"])
        station_ids = [_parent_of(s) for s in stop_ids]
        offsets = [t[0] for t in spec["times"]]
        dwells = [t[1] for t in spec["times"]]
        start_seconds = gtfs_time_to_seconds(spec["start"])
        assert start_seconds is not None
        first_departure_seconds = start_seconds + dwells[0]
        last_arrival_seconds = start_seconds + offsets[-1]
        direction_id = spec["direction_id"]
        pattern_id = pattern_id_of(spec["route_id"], direction_id, stop_ids)
        profile_id = profile_id_of(offsets, dwells)
        key = identity_key_of(
            spec["route_id"], first_departure_seconds, direction_id, station_ids
        )

        trip_rows.append(
            {
                "trip_id": spec["trip_id"],
                "pattern_id": pattern_id,
                "profile_id": profile_id,
                "start_seconds": start_seconds,
                "first_departure_seconds": first_departure_seconds,
                "last_arrival_seconds": last_arrival_seconds,
                "stop_events": len(stop_ids),
                "identity_key": key,
            }
        )
        patterns_map.setdefault(
            pattern_id,
            {
                "pattern_id": pattern_id,
                "route_id": spec["route_id"],
                "direction_id": direction_id,
                "stop_ids": stop_ids,
                "station_ids": station_ids,
            },
        )
        profiles_map.setdefault(
            profile_id,
            {"profile_id": profile_id, "offsets": offsets, "dwells": dwells},
        )
        canonical_rows.append(
            {"trip_id": spec["trip_id"], "service_id": spec["service_id"]}
        )

    trips_df = (
        pl.DataFrame(trip_rows, schema=_TRIPS_SCHEMA)
        if trip_rows
        else pl.DataFrame(schema=_TRIPS_SCHEMA)
    )
    patterns_df = (
        pl.DataFrame(list(patterns_map.values()), schema=_PATTERNS_SCHEMA)
        if patterns_map
        else pl.DataFrame(schema=_PATTERNS_SCHEMA)
    )
    profiles_df = (
        pl.DataFrame(list(profiles_map.values()), schema=_TIME_PROFILES_SCHEMA)
        if profiles_map
        else pl.DataFrame(schema=_TIME_PROFILES_SCHEMA)
    )
    canonical_df = (
        pl.DataFrame(canonical_rows, schema=_CANONICAL_TRIPS_SCHEMA)
        if canonical_rows
        else pl.DataFrame(schema=_CANONICAL_TRIPS_SCHEMA)
    )

    semantic = {
        "trips": trips_df,
        "patterns": patterns_df,
        "time_profiles": profiles_df,
    }
    return semantic, canonical_df


def _pair(target_specs: list[dict], candidate_specs: list[dict]) -> pl.DataFrame:
    target_sem, target_canon = _side_tables(target_specs)
    candidate_sem, candidate_canon = _side_tables(candidate_specs)
    return pair_trips(target_sem, target_canon, candidate_sem, candidate_canon)


def _row_for(pairs: pl.DataFrame, candidate_trip_id: str) -> dict:
    matches = pairs.filter(pl.col("candidate_trip_id") == candidate_trip_id)
    assert matches.height == 1, (
        f"expected exactly one pair row for candidate {candidate_trip_id!r}, "
        f"got {matches.height}"
    )
    return matches.row(0, named=True)


def _removed_target_ids(pairs: pl.DataFrame) -> set[str]:
    return set(pairs.filter(pl.col("change") == "removed")["target_trip_id"].to_list())


# --- Identity ----------------------------------------------------------------


def test_the_identity_key_is_route_first_departure_direction_and_parent_stations():
    t = trip(
        "x",
        times=[(0, 30), (420, 60), (1080, 0)],
    )
    start_seconds = gtfs_time_to_seconds(t["start"])
    first_departure = start_seconds + t["times"][0][1]
    assert first_departure == 7 * 3600 + 15 * 60 + 30
    assert (
        identity_key_of("R1", first_departure, "0", ["A", "PX", "C"])
        == "R1|26130|0|A>PX>C"
    )
    # An unstated direction is its own value, not zero.
    assert identity_key_of("R1", 0, "", ["A"]) == "R1|0|-|A"


def test_a_strict_subsequence_keeps_order_and_drops_something_not_equal_or_reordered():
    assert is_strict_subsequence(["A", "C"], ["A", "B", "C"]) is True
    assert is_strict_subsequence(["A", "B", "C"], ["A", "B", "C"]) is False
    assert is_strict_subsequence(["C", "A"], ["A", "B", "C"]) is False
    assert is_strict_subsequence(["A", "D"], ["A", "B", "C"]) is False


# --- The pairing pass, one case each ----------------------------------------


def test_the_same_trip_under_the_same_id_is_unchanged_and_carried_by_itself():
    pairs = _pair([trip("T1")], [trip("T1")])
    row = _row_for(pairs, "T1")
    assert row["change"] == "unchanged"
    assert row["modifications"] == []
    assert row["id_changed"] is False
    assert row["target_trip_id"] == "T1"
    assert _removed_target_ids(pairs) == set()


def test_stop_time_changes_within_the_pattern_are_a_retime_of_the_same_trip():
    pairs = _pair(
        [trip("T3", times=MAIN_SLOW)],
        [trip("T3")],
    )
    row = _row_for(pairs, "T3")
    assert row["change"] == "modified"
    assert row["modifications"] == ["retimed"]
    assert row["id_changed"] is False


def test_another_platform_is_a_platform_change_another_station_is_a_new_trip():
    platform = _pair(
        [trip("T16", stops=["A", "P1", "C"])],
        [trip("T16", stops=["A", "P2", "C"])],
    )
    row = _row_for(platform, "T16")
    assert row["change"] == "modified"
    assert row["modifications"] == ["platform"]
    assert _removed_target_ids(platform) == set()

    # The counterfactual: a stop with a DIFFERENT parent at the same
    # position is a station the match never served — added, and the old
    # trip removed.
    station = _pair(
        [trip("T18", stops=["A", "P1", "C"])],
        [trip("T19", stops=["A", "X", "C"])],
    )
    row = _row_for(station, "T19")
    assert row["change"] == "added"
    assert row["identity_key"] is None
    assert _removed_target_ids(station) == {"T18"}


def test_a_strict_subsequence_of_targets_stations_pairs_as_skipped_stops():
    pairs = _pair(
        [trip("T17", stops=["A", "X", "P1", "C"], times=LOOP)],
        [trip("T17", stops=["A", "P1", "C"], times=LOOP_SKIP)],
    )
    row = _row_for(pairs, "T17")
    assert row["change"] == "modified"
    assert row["modifications"] == ["skipped_stops"]
    assert _removed_target_ids(pairs) == set()

    # Retimed at the common stops as well: both subtypes, skipped last.
    retimed_too = _pair(
        [trip("T17", stops=["A", "X", "P1", "C"], times=LOOP)],
        [trip("T17", stops=["A", "P1", "C"], times=MAIN)],
    )
    row2 = _row_for(retimed_too, "T17")
    assert row2["modifications"] == ["retimed", "skipped_stops"]


def test_the_skipped_stops_fallback_does_not_fire_when_kept_or_when_ambiguous():
    kept = _pair(
        [trip("L", stops=["A", "X", "P1", "C"], times=LOOP)],
        [
            trip("L", stops=["A", "X", "P1", "C"], times=LOOP),
            trip("S", service_id="SAT", stops=["A", "P1", "C"], times=LOOP_SKIP),
        ],
    )
    row = _row_for(kept, "S")
    assert row["change"] == "added"

    ambiguous = _pair(
        [
            trip("L1", stops=["A", "X", "P1", "C"], times=LOOP),
            trip("L2", service_id="SAT", stops=["A", "Y", "P1", "C"], times=LOOP),
        ],
        [trip("S", stops=["A", "P1", "C"], times=LOOP_SKIP)],
    )
    row2 = _row_for(ambiguous, "S")
    assert row2["change"] == "added"
    assert _removed_target_ids(ambiguous) == {"L1", "L2"}


def test_a_trip_whose_only_difference_is_its_trip_id_is_unchanged_with_id_changed_set():
    pairs = _pair([trip("T20")], [trip("T21")])
    row = _row_for(pairs, "T21")
    assert row["change"] == "unchanged"
    assert row["modifications"] == []
    assert row["id_changed"] is True
    assert _removed_target_ids(pairs) == set()


def test_start_time_is_exact_a_minutes_shift_is_a_different_trip():
    pairs = _pair(
        [trip("T5", start="12:03:00")],
        [trip("T5", start="12:05:00")],
    )
    row = _row_for(pairs, "T5")
    assert row["change"] == "added"
    assert _removed_target_ids(pairs) == {"T5"}


# --- Carriers and the counterpart tie-break ---------------------------------


def test_one_identity_many_carriers_counterpart_is_the_identical_carrier_first():
    # The identical carrier loses every OTHER rule on purpose: it rides a
    # different service from the candidate and sorts last by id, while the
    # retimed carrier rides the candidate's own service and sorts first —
    # so only the identical-content rule can pick it.
    target = [
        trip("A-SLOW", service_id="FALL", times=MAIN_SLOW),
        trip("Z-SAME", service_id="HOLIDAY"),
    ]
    candidate = [
        trip("A-SLOW", service_id="FALL", times=MAIN_SLOW),
        trip("NEW", service_id="FALL"),
    ]
    pairs = _pair(target, candidate)
    a_slow = _row_for(pairs, "A-SLOW")
    new = _row_for(pairs, "NEW")
    assert a_slow["identity_key"] == new["identity_key"]
    # The re-issue under a new id reads unchanged + id-changed, because a
    # carrier with identical content exists — not "retimed" against the
    # same-service carrier the later rules would have chosen.
    assert new["change"] == "unchanged"
    assert new["id_changed"] is True
    assert new["target_trip_id"] == "Z-SAME"

    # Counterfactual: with only the retimed carrier on the target, the
    # same candidate IS a retime.
    only = _pair([target[0]], [candidate[1]])
    row = _row_for(only, "NEW")
    assert row["change"] == "modified"
    assert row["modifications"] == ["retimed"]


def test_after_identical_content_prefers_trip_id_then_service_id_then_first():
    target = [
        trip("B", service_id="S2", times=MAIN_SLOW),
        trip("A", service_id="S1", times=LOOP_SKIP),
    ]
    # Same id as B: compared to B (retimed), not to A.
    same_id = _pair(target, [trip("B", service_id="S9")])
    assert _row_for(same_id, "B")["target_trip_id"] == "B"

    # No id in common: the same service wins.
    same_service = _pair(target, [trip("Z", service_id="S1")])
    assert _row_for(same_service, "Z")["target_trip_id"] == "A"

    # Nothing in common: the first by trip_id ("A").
    neither = _pair(target, [trip("Z", service_id="S9")])
    assert _row_for(neither, "Z")["target_trip_id"] == "A"


# --- The spec's own cases (schedule-trip-pairing.md Validation) ------------


def test_pairing_a_digest_with_itself_yields_every_row_unchanged():
    specs = [
        trip("T1"),
        trip(
            "T2",
            route_id="R2",
            start="08:00:00",
            stops=["D", "E"],
            times=[(0, 0), (300, 0)],
        ),
        trip("T3", stops=["A", "X", "P1", "C"], times=LOOP),
    ]
    semantic, canonical = _side_tables(specs)
    pairs = pair_trips(semantic, canonical, semantic, canonical)
    assert pairs.height == len(specs)
    assert set(pairs["change"].to_list()) == {"unchanged"}
    assert all(m == [] for m in pairs["modifications"].to_list())
    assert all(v is False for v in pairs["id_changed"].to_list())


def test_deterministic_output_under_shuffled_input_order():
    target = [trip("T1"), trip("T2", service_id="SAT"), trip("T3", start="09:00:00")]
    candidate = [
        trip("T3", start="09:00:00"),
        trip("T1"),
        trip("NEW", start="11:00:00"),
    ]
    forward = _pair(target, candidate)
    shuffled = _pair(list(reversed(target)), list(reversed(candidate)))
    assert forward.equals(shuffled)


# --- Writer -------------------------------------------------------------


def test_write_trip_pairs_skip_if_exists_and_round_trip(tmp_path: Path):
    pairs = _pair([trip("T1")], [trip("T1")])
    base = str(tmp_path)

    assert write_trip_pairs(pairs, base, "target-digest", "candidate-digest") is True
    assert (
        write_trip_pairs(pairs, base, "target-digest", "candidate-digest") is False
    )  # skip-if-exists

    pair_dir = (
        tmp_path / "_target_digest=target-digest" / "_candidate_digest=candidate-digest"
    )
    marker = pair_dir / "metadata.json"
    assert marker.exists()
    metadata = json.loads(marker.read_text())
    assert metadata["target_digest"] == "target-digest"
    assert metadata["candidate_digest"] == "candidate-digest"
    assert metadata["rev"] == PAIRING_REV
    assert metadata["row_count"] == pairs.height

    read_back = pl.read_parquet(pair_dir / "trip_pairs.parquet")
    assert read_back.equals(pairs)


def test_write_trip_pairs_marker_written_last(tmp_path: Path):
    # The marker is the commit signal (skip-if-exists checks it); the
    # parquet must already be on disk by the time it lands.
    pairs = _pair([trip("T1")], [trip("T1")])
    base = str(tmp_path)
    write_trip_pairs(pairs, base, "t", "c")
    pair_dir = tmp_path / "_target_digest=t" / "_candidate_digest=c"
    assert (pair_dir / "trip_pairs.parquet").exists()
    assert (pair_dir / "metadata.json").exists()
