"""GTFS Schedule trip pairing — the cross-feed relation between a target and
a candidate release's trips.

Implements specs/schedule-semantics.md §Trip Pairing and §Persisted Units
(per pair): the `trip_pairs` table, one row per trip on either side. Pure
computation (`pair_trips`) over both sides' semantic tables
(`schedule_semantics.derive_semantics`'s output — `trips`, `patterns`,
`time_profiles`) plus each side's canonical `trips` table (for `service_id`,
which the semantic `trips` table does not carry) — no I/O in `pair_trips`
itself. This is a Python port of the hosted platform's TypeScript
`pairSides` implementation: same identity rule, same skipped-stops
fallback, same counterpart tie-break, same verdict — ported case for case
by `tests/test_schedule_pairing.py`.

`pair_and_write` is the single storage-writing entry point.

## Content equality (the "identical content" counterpart tie-break)

The browser implementation compares a candidate and a carrier stop-by-stop
(`sameContent`: same length, same child `stop_id`s, same per-stop offset and
dwell). This module instead compares `pattern_id` and `profile_id` equality
from the already-derived semantic tables — cheaper, and equivalent: `pattern_id`
is a content hash of (`route_id`, `direction_id`, child `stop_ids`) and
`profile_id` a content hash of (`offsets`, `dwells`), so two trips share both
ids iff they share the same child-stop sequence and the same per-stop timing
(barring a hash collision) — exactly what `sameContent` checks.

## Storage layout (schedule-semantics.md §Storage layout)

    <base>/schedule-pairing/<rev>/_target_digest=<t>/_candidate_digest=<c>/
      trip_pairs.parquet
      metadata.json                       # commit marker, written last

`PAIRING_REV` names this module's revision segment. It starts at the same
value as `schedule_semantics.SEMANTICS_REV` but is tracked independently
(per the spec: "the pairing tree's revision bumps when the pairing rules
change *or* when the semantic revision it reads bumps") — bump it whenever
this module's derivation rules change, and also whenever `SEMANTICS_REV`
bumps, even if this module's own rules did not change.
"""

from __future__ import annotations

import io
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import polars as pl

from .schedule_semantics import identity_key_of

if TYPE_CHECKING:
    import fsspec

# The revision segment of the storage layout (see module docstring).
PAIRING_REV = "v1"

_TRIP_PAIRS_SCHEMA = {
    "candidate_trip_id": pl.Utf8,
    "target_trip_id": pl.Utf8,
    "identity_key": pl.Utf8,
    "change": pl.Utf8,
    "modifications": pl.List(pl.Utf8),
    "id_changed": pl.Boolean,
}


# --- Indexing one side -------------------------------------------------


def _service_ids(trips_txt: pl.DataFrame | None) -> pl.DataFrame:
    """`trip_id` -> `service_id` from a side's canonical `trips` table
    (the semantic `trips` table carries derived facts only, per
    schedule-semantics.md §Persisted Units — `service_id` lives on the
    canonical row and joins by `trip_id`). Degrades to an empty/blank
    mapping when the canonical table is absent or lacks the column,
    matching `schedule_semantics.derive_semantics`'s "missing tables
    degrade gracefully" contract."""
    if trips_txt is None or "trip_id" not in trips_txt.columns:
        return pl.DataFrame(schema={"trip_id": pl.Utf8, "service_id": pl.Utf8})
    if "service_id" not in trips_txt.columns:
        return trips_txt.select("trip_id").with_columns(pl.lit("").alias("service_id"))
    return trips_txt.select(["trip_id", "service_id"])


def _index_side(
    semantic: Mapping[str, pl.DataFrame], trips_txt: pl.DataFrame | None
) -> list[dict]:
    """One side's trips, enriched for pairing: the semantic `trips` row's
    own columns, plus (via `pattern_id`) `route_id`, `direction_id`,
    `stop_ids`, `station_ids` from `patterns`, plus (via `profile_id`)
    `offsets`, `dwells` from `time_profiles`, plus (via `trip_id`)
    `service_id` from the canonical `trips` table. Sorted by `trip_id` —
    the semantic table's own canonical order (schedule_semantics.py sorts
    `trips` by `trip_id`), which is also the order §Trip Pairing's
    "candidate's `trips` order" refers to, pinned by a determinism test."""
    trips = semantic.get("trips")
    patterns = semantic.get("patterns")
    profiles = semantic.get("time_profiles")
    if trips is None or trips.height == 0:
        return []

    joined = trips
    if patterns is not None:
        joined = joined.join(
            patterns.select(
                ["pattern_id", "route_id", "direction_id", "stop_ids", "station_ids"]
            ),
            on="pattern_id",
            how="left",
        )
    if profiles is not None:
        joined = joined.join(
            profiles.select(["profile_id", "offsets", "dwells"]),
            on="profile_id",
            how="left",
        )
    joined = joined.join(_service_ids(trips_txt), on="trip_id", how="left")
    joined = joined.sort("trip_id")
    return joined.to_dicts()


def _loose_key(row: Mapping) -> str:
    """The route/first-departure/direction part of the identity key — what
    the skipped-stops fallback groups target identities on (§Trip Pairing
    "Matching" step 2)."""
    return identity_key_of(
        row.get("route_id") or "",
        row.get("first_departure_seconds"),
        row.get("direction_id") or "",
        [],
    )


# --- Matching helpers (ported from trip-matching.ts) --------------------


def is_strict_subsequence(small: list[str], big: list[str]) -> bool:
    """Whether `small` is a strict subsequence of `big`: same order, fewer
    entries, nothing in `small` absent from `big`. Port of
    trip-matching.ts's `isStrictSubsequence`."""
    if len(small) >= len(big):
        return False
    i = 0
    for s in big:
        if i < len(small) and s == small[i]:
            i += 1
    return i == len(small)


def _align_subsequence(small: list[str], big: list[str]) -> list[int]:
    """The positions in `big` each entry of `small` aligns to (first match
    in order) — port of trip-matching.ts's `alignSubsequence`."""
    positions: list[int] = []
    i = 0
    for j, s in enumerate(big):
        if i >= len(small):
            break
        if s == small[i]:
            positions.append(j)
            i += 1
    return positions


def _choose_counterpart(candidate: Mapping, carriers: list[tuple[int, dict]]) -> int:
    """The one carrier a matched candidate's content is judged against
    (§Trip Pairing "Counterpart"), over carriers sorted by `trip_id`:
    identical content (same `pattern_id` AND `profile_id`), then the same
    `trip_id`, then the same `service_id`, then the first. Returns the
    target row's index. Port of trip-matching.ts's `chooseCounterpart`."""
    ordered = sorted(carriers, key=lambda pair: pair[1]["trip_id"])
    for idx, row in ordered:
        if (
            row["pattern_id"] == candidate["pattern_id"]
            and row["profile_id"] == candidate["profile_id"]
        ):
            return idx
    for idx, row in ordered:
        if row["trip_id"] == candidate["trip_id"]:
            return idx
    for idx, row in ordered:
        if row["service_id"] == candidate["service_id"]:
            return idx
    return ordered[0][0]


def _modifications_of(
    candidate: Mapping, counterpart: Mapping, skipped: bool
) -> list[str]:
    """Stop-by-stop comparison of a candidate against its counterpart
    (§Trip Pairing "Verdict"), the candidate's stops aligned to the
    counterpart's by station (position for an exact match; the
    subsequence alignment for a skipped match). Port of trip-matching.ts's
    `modificationsOf`."""
    cand_stations = candidate.get("station_ids") or []
    cp_stations = counterpart.get("station_ids") or []
    if skipped:
        positions = _align_subsequence(cand_stations, cp_stations)
    else:
        positions = list(range(len(cand_stations)))

    cand_stops = candidate.get("stop_ids") or []
    cp_stops = counterpart.get("stop_ids") or []
    cand_offsets = candidate.get("offsets") or []
    cand_dwells = candidate.get("dwells") or []
    cp_offsets = counterpart.get("offsets") or []
    cp_dwells = counterpart.get("dwells") or []

    retimed = False
    platform = False
    for i, j in enumerate(positions):
        if i >= len(cand_stops) or j >= len(cp_stops):
            continue
        if cp_stops[j] != cand_stops[i]:
            platform = True
        a_off = cand_offsets[i] if i < len(cand_offsets) else None
        b_off = cp_offsets[j] if j < len(cp_offsets) else None
        a_dwell = cand_dwells[i] if i < len(cand_dwells) else None
        b_dwell = cp_dwells[j] if j < len(cp_dwells) else None
        if a_off != b_off or a_dwell != b_dwell:
            retimed = True

    out: list[str] = []
    if retimed:
        out.append("retimed")
    if platform:
        out.append("platform")
    if skipped:
        out.append("skipped_stops")
    return out


# --- The pairing pass ----------------------------------------------------


def pair_trips(
    target_semantic: Mapping[str, pl.DataFrame],
    target_trips_txt: pl.DataFrame | None,
    candidate_semantic: Mapping[str, pl.DataFrame],
    candidate_trips_txt: pl.DataFrame | None,
) -> pl.DataFrame:
    """Derive the `trip_pairs` table (specs/schedule-semantics.md §Trip
    Pairing) relating `target_semantic`'s trips to `candidate_semantic`'s.

    `target_semantic`/`candidate_semantic` are `schedule_semantics.
    derive_semantics`'s output (or an equivalently-shaped mapping with at
    least "trips"; "patterns" and "time_profiles" as available — missing
    tables degrade gracefully, same convention as `derive_semantics`).
    `*_trips_txt` are each side's canonical `trips` table (only `trip_id`
    and `service_id` are read).

    Pure: no I/O. Deterministic: candidate trips are walked in the
    candidate `trips` table's own order (sorted by `trip_id`), carriers
    are chosen by `trip_id`-sorted tie-break, and `removed` rows are
    appended in the target `trips` table's `trip_id` order — so the same
    two inputs always derive byte-identical parquet regardless of the
    order rows arrived in upstream.
    """
    target_rows = _index_side(target_semantic, target_trips_txt)
    candidate_rows = _index_side(candidate_semantic, candidate_trips_txt)

    target_by_key: dict[str, list[int]] = {}
    target_by_loose: dict[str, set[str]] = {}
    for i, row in enumerate(target_rows):
        target_by_key.setdefault(row["identity_key"], []).append(i)
        loose = _loose_key(row)
        target_by_loose.setdefault(loose, set()).add(row["identity_key"])

    candidate_keys = {row["identity_key"] for row in candidate_rows}

    claimed: set[int] = set()
    pair_rows: list[dict] = []

    for cand in candidate_rows:
        key = cand["identity_key"]
        carrier_indices = target_by_key.get(key)
        skipped = False

        if carrier_indices is None:
            # Skipped-stops fallback (§Trip Pairing "Matching" step 2):
            # among target identities sharing this departure's loose key,
            # those whose station_ids strictly contain the candidate's as
            # a subsequence, excluding any identity the candidate feed
            # ALSO carries exactly (a kept full-length trip is not being
            # replaced by its short-turn). Fires only when exactly one
            # such identity exists.
            loose = _loose_key(cand)
            cand_stations = cand.get("station_ids") or []
            supersets = []
            for candidate_key in target_by_loose.get(loose, ()):
                if candidate_key in candidate_keys:
                    continue
                sample_idx = target_by_key[candidate_key][0]
                sample_stations = target_rows[sample_idx].get("station_ids") or []
                if is_strict_subsequence(cand_stations, sample_stations):
                    supersets.append(candidate_key)
            if len(supersets) == 1:
                key = supersets[0]
                carrier_indices = target_by_key[key]
                skipped = True

        if not carrier_indices:
            pair_rows.append(
                {
                    "candidate_trip_id": cand["trip_id"],
                    "target_trip_id": None,
                    "identity_key": None,
                    "change": "added",
                    "modifications": [],
                    "id_changed": False,
                }
            )
            continue

        for i in carrier_indices:
            claimed.add(i)

        counterpart_idx = _choose_counterpart(
            cand, [(i, target_rows[i]) for i in carrier_indices]
        )
        counterpart = target_rows[counterpart_idx]
        modifications = _modifications_of(cand, counterpart, skipped)
        id_changed = not any(
            target_rows[i]["trip_id"] == cand["trip_id"] for i in carrier_indices
        )
        pair_rows.append(
            {
                "candidate_trip_id": cand["trip_id"],
                "target_trip_id": counterpart["trip_id"],
                "identity_key": key,
                "change": "modified" if modifications else "unchanged",
                "modifications": modifications,
                "id_changed": id_changed,
            }
        )

    for i, row in enumerate(target_rows):
        if i in claimed:
            continue
        pair_rows.append(
            {
                "candidate_trip_id": None,
                "target_trip_id": row["trip_id"],
                "identity_key": None,
                "change": "removed",
                "modifications": [],
                "id_changed": False,
            }
        )

    if not pair_rows:
        return pl.DataFrame(schema=_TRIP_PAIRS_SCHEMA)
    return pl.DataFrame(pair_rows, schema=_TRIP_PAIRS_SCHEMA)


# --- Writing ---------------------------------------------------------------


def write_trip_pairs(
    pairs: pl.DataFrame,
    base_path: str,
    target_digest: str,
    candidate_digest: str,
    filesystem: fsspec.AbstractFileSystem | None = None,
) -> bool:
    """Write `trip_pairs.parquet` plus `metadata.json` (commit marker,
    written last) under
    `{base_path}/_target_digest={target_digest}/_candidate_digest={candidate_digest}/`.

    `base_path` should already include the revision segment (e.g.
    "gs://bucket/schedule-pairing/v1" — schedule-semantics.md §Storage
    layout). `filesystem`, when given, should already be resolved (and,
    on the worker, already wrapped for `customTime` stamping via
    `analysis_storage.stamped_filesystem` — every object here, marker
    included, is stamped at creation, matching the create-only IAM the
    pipeline SA holds on the analysis bucket).

    Skip-if-exists: returns False without writing anything when
    `metadata.json` already exists for this ordered pair (content-
    addressed; re-materializing an existing pair is a no-op). Returns
    True when it wrote a fresh directory.
    """
    if filesystem is None:
        import fsspec as fsspec_runtime

        filesystem, base_path = fsspec_runtime.core.url_to_fs(base_path)

    pair_dir = (
        f"{base_path}/_target_digest={target_digest}"
        f"/_candidate_digest={candidate_digest}"
    )
    marker_path = f"{pair_dir}/metadata.json"
    if filesystem.exists(marker_path):
        return False

    filesystem.mkdirs(pair_dir, exist_ok=True)

    buf = io.BytesIO()
    pairs.write_parquet(buf, compression="zstd")
    with filesystem.open(f"{pair_dir}/trip_pairs.parquet", "wb") as f:
        f.write(buf.getvalue())

    metadata = {
        "target_digest": target_digest,
        "candidate_digest": candidate_digest,
        "rev": PAIRING_REV,
        "written_at": datetime.now(UTC).isoformat(),
        "row_count": pairs.height,
    }
    with filesystem.open(marker_path, "wb") as f:
        f.write(json.dumps(metadata, indent=2).encode("utf-8"))

    return True


def pair_and_write(
    target_digest: str,
    candidate_digest: str,
    bucket: str,
    deadline: str | None,
) -> None:
    """The on-demand materialization entry point (specs/schedule-semantics.md
    §Materialization): derive and write `trip_pairs` for an ordered digest
    pair whose sides' canonical form already exists.

    A platform worker's `"pair-trips"` task handler calls this function
    with the dispatch's `target_digest`, `candidate_digest`, analysis
    bucket, and the EARLIER of the two sides' retention deadlines (the
    caller computes which is earlier; this function stamps whatever
    deadline it is given, matching
    `schedule_semantics.derive_and_write_semantics`'s contract).

    `deadline` is the ISO 8601 GCS `customTime` deadline (or None when
    both sides are permanent/null-window), the same contract
    `derive_and_write_semantics` already uses.
    """
    from .analysis_storage import stamped_filesystem
    from .schedule_semantics import derive_semantics, tables_from_digest

    canonical_base = f"gs://{bucket}/schedule"
    pairing_base = f"gs://{bucket}/schedule-pairing/{PAIRING_REV}"

    target_canon = tables_from_digest(canonical_base, target_digest)
    candidate_canon = tables_from_digest(canonical_base, candidate_digest)
    target_semantic = derive_semantics(target_canon)
    candidate_semantic = derive_semantics(candidate_canon)

    pairs = pair_trips(
        target_semantic,
        target_canon.get("trips"),
        candidate_semantic,
        candidate_canon.get("trips"),
    )

    deadline_dt = datetime.fromisoformat(deadline) if deadline else None
    filesystem, resolved_base_path = stamped_filesystem(pairing_base, deadline_dt)
    write_trip_pairs(
        pairs,
        resolved_base_path,
        target_digest,
        candidate_digest,
        filesystem=filesystem,
    )
