"""GTFS Schedule semantic facts — the per-version derivation.

Implements specs/schedule-semantics.md §Semantic Rules and §Persisted Units
(per version): `service_dates`, `patterns`, `time_profiles`, `trips`. Pure
computation over a digest's canonical tables (polars DataFrames) — no I/O in
`derive_semantics` itself; callers supply the tables however sourced
(`tables_from_archive` for a local zip, `tables_from_digest` for the analysis
bucket's canonical parquet).

`derive_and_write_semantics` is the single storage-writing entry point.

## Hash preimages (revision v1 — specs/schedule-semantics.md §Revisions)

`pattern_id` and `profile_id` are BLAKE3 hex digests (unversioned — any
change to a preimage below is a rule change and bumps the storage `<rev>`,
never a silent reinterpretation of an existing one):

- `pattern_id = blake3(route_id + US + direction + US + RS.join(stop_ids))`
  where `US` is `"\x1f"` (ASCII unit separator), `RS` is `"\x1e"` (ASCII
  record separator), and `direction` is `"0"`, `"1"`, or `"-"` for unstated.
  Keyed on the CHILD `stop_id` sequence (not station), so a same-station
  platform swap changes `pattern_id` and `stop_ids` while leaving
  `station_ids` — and therefore `identity_key` — untouched.
- `profile_id = blake3(RS.join(f"{offset}{US}{dwell}" for each stop))`,
  where a null offset or dwell serializes as the empty string between the
  separators (so a blank stop time is distinguishable from a real `0`).

`identity_key` is NOT a hash — it is the identity tuple
(`route_id`, `first_departure_seconds`, `direction_id`, `station_ids`)
serialized as one string, byte-for-byte the same format as the hosted
platform's TypeScript `identityKey()`:

    f"{route_id}|{first_departure_seconds}|{direction}|{'>'.join(station_ids)}"

(`direction` is the same `"0"`/`"1"`/`"-"` encoding as above.) Matching the
TS format exactly means a future cross-language reader of this column never
has to reconcile two encodings of the same tuple. Both preimages are pinned
by a hand-computed test vector in test_schedule_semantics.py.
"""

from __future__ import annotations

import io
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import blake3
import polars as pl

from .schedule_helpers import gtfs_time_to_seconds

if TYPE_CHECKING:
    import fsspec
    import gtfs_digester as dg

# The revision segment of the storage layout
# (<base>/schedule-semantics/<rev>/_feed_digest=<d>/ — schedule-semantics.md
# §Storage layout). Bump alongside any change to a rule or hash preimage in
# this module's docstring.
SEMANTICS_REV = "v1"

# The four persisted units, in the order they're written (metadata.json
# always last, regardless of this order — see write_semantics).
TABLE_NAMES = ("service_dates", "patterns", "time_profiles", "trips")

# Canonical tables this derivation reads. agency.txt/routes.txt aren't
# needed — patterns/trips carry route_id as a bare id, not a joined name.
_INPUT_TABLES = (
    "stops",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates",
    "feed_info",
)

_GTFS_WEEKDAYS = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)

_UNIT_SEP = "\x1f"  # separates fields within one item
_RECORD_SEP = "\x1e"  # separates items in a sequence


# --- Hashing / identity -------------------------------------------------


def _direction_str(raw: str | None) -> str:
    """Canonical `direction_id` column value: "0", "1", or "" for unstated
    (blank, missing column, or any other value) — matches the canonical
    form's empty-string-for-blank convention."""
    if not raw:
        return ""
    raw = raw.strip()
    return raw if raw in ("0", "1") else ""


def _direction_sentinel(direction: str) -> str:
    """The hash/identity-key encoding of a direction: "-" for unstated,
    else the value itself. Matches trip-matching.ts's `identityKey()`."""
    return direction if direction != "" else "-"


def pattern_id_of(route_id: str, direction_id: str, stop_ids: list[str]) -> str:
    """BLAKE3 hex digest of the pattern preimage — see module docstring."""
    preimage = (
        route_id
        + _UNIT_SEP
        + _direction_sentinel(direction_id)
        + _UNIT_SEP
        + _RECORD_SEP.join(stop_ids)
    )
    return blake3.blake3(preimage.encode("utf-8")).hexdigest()


def profile_id_of(offsets: list[int | None], dwells: list[int | None]) -> str:
    """BLAKE3 hex digest of the time-profile preimage — see module docstring."""
    parts = [
        ("" if offset is None else str(offset))
        + _UNIT_SEP
        + ("" if dwell is None else str(dwell))
        for offset, dwell in zip(offsets, dwells, strict=True)
    ]
    return blake3.blake3(_RECORD_SEP.join(parts).encode("utf-8")).hexdigest()


def identity_key_of(
    route_id: str,
    first_departure_seconds: int,
    direction_id: str,
    station_ids: list[str],
) -> str:
    """The identity tuple as one string — see module docstring."""
    return (
        f"{route_id}|{first_departure_seconds}|"
        f"{_direction_sentinel(direction_id)}|{'>'.join(station_ids)}"
    )


# --- Service dates -------------------------------------------------------


def _expand_calendar(calendar: pl.DataFrame | None) -> dict[str, set[str]]:
    """`service_id` -> set of `YYYYMMDD` dates from calendar.txt's weekday
    ranges, UNCLAMPED (before calendar_dates overlay, before the declared
    window)."""
    out: dict[str, set[str]] = {}
    if calendar is None or calendar.height == 0:
        return out
    for row in calendar.iter_rows(named=True):
        service_id = row.get("service_id") or ""
        start_raw = row.get("start_date") or ""
        end_raw = row.get("end_date") or ""
        if not start_raw or not end_raw:
            continue
        try:
            start = datetime.strptime(start_raw, "%Y%m%d").date()
            end = datetime.strptime(end_raw, "%Y%m%d").date()
        except ValueError:
            continue
        if end < start:
            continue
        flags = [row.get(day) == "1" for day in _GTFS_WEEKDAYS]
        if not any(flags):
            continue
        dates = out.setdefault(service_id, set())
        d = start
        while d <= end:
            if flags[d.weekday()]:  # Monday=0 ... Sunday=6, matches _GTFS_WEEKDAYS
                dates.add(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
    return out


def _apply_calendar_dates(
    expanded: dict[str, set[str]], calendar_dates: pl.DataFrame | None
) -> dict[str, set[str]]:
    """Overlay calendar_dates.txt exceptions onto `expanded` (mutated in
    place, also returned): `exception_type=1` adds the date, `=2` removes
    it. A service_id seen only here (calendar_dates-only) gets its own
    entry."""
    if calendar_dates is None or calendar_dates.height == 0:
        return expanded
    for row in calendar_dates.iter_rows(named=True):
        service_id = row.get("service_id") or ""
        raw_date = row.get("date") or ""
        exception_type = row.get("exception_type") or ""
        if not raw_date:
            continue
        dates = expanded.setdefault(service_id, set())
        if exception_type == "1":
            dates.add(raw_date)
        elif exception_type == "2":
            dates.discard(raw_date)
    return expanded


def _declared_window(
    feed_info: pl.DataFrame | None,
    expanded: dict[str, set[str]],
    trip_service_ids: set[str],
) -> tuple[str, str] | None:
    """feed_info.txt's `feed_start_date`/`feed_end_date` when both are
    present; else the first through last date on which any trip is active
    (a service_id trips.txt actually references)."""
    if feed_info is not None and feed_info.height > 0:
        row = feed_info.row(0, named=True)
        start = row.get("feed_start_date") or ""
        end = row.get("feed_end_date") or ""
        if start and end:
            return start, end
    all_dates = sorted(
        date
        for service_id, dates in expanded.items()
        if service_id in trip_service_ids
        for date in dates
    )
    if not all_dates:
        return None
    return all_dates[0], all_dates[-1]


def _clip(
    expanded: dict[str, set[str]], window: tuple[str, str]
) -> list[tuple[str, str]]:
    start, end = window
    rows = [
        (service_id, date)
        for service_id, dates in expanded.items()
        for date in dates
        if start <= date <= end
    ]
    rows.sort()
    return rows


_SERVICE_DATES_SCHEMA = {"service_id": pl.Utf8, "date": pl.Utf8}
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


def derive_semantics(tables: Mapping[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    """Derive the four per-version semantic tables from a digest's canonical
    tables. `tables` is keyed by table name without ".txt" (as
    `gtfs_digester.read_table`'s `table_name` and `tables_from_archive`
    below both use) — at minimum "trips" and "stop_times" for anything
    useful; "stops", "calendar", "calendar_dates", "feed_info" as available.
    Missing tables degrade gracefully (empty results), never raise.

    Pure: no I/O. Deterministic: output row order is fixed (sorted by each
    table's natural key) regardless of input row order, so the same input
    always derives byte-identical parquet.
    """
    stops_txt = tables.get("stops")
    trips_txt = tables.get("trips")
    stop_times_txt = tables.get("stop_times")
    calendar_txt = tables.get("calendar")
    calendar_dates_txt = tables.get("calendar_dates")
    feed_info_txt = tables.get("feed_info")

    # --- service_dates ---
    expanded = _apply_calendar_dates(_expand_calendar(calendar_txt), calendar_dates_txt)
    trip_service_ids: set[str] = set()
    if trips_txt is not None and "service_id" in trips_txt.columns:
        trip_service_ids = set(trips_txt.get_column("service_id").to_list())
    window = _declared_window(feed_info_txt, expanded, trip_service_ids)
    service_dates_rows = _clip(expanded, window) if window else []
    service_dates_df = (
        pl.DataFrame(service_dates_rows, schema=_SERVICE_DATES_SCHEMA, orient="row")
        if service_dates_rows
        else pl.DataFrame(schema=_SERVICE_DATES_SCHEMA)
    )

    # --- per-trip derivation over stop_times ---
    parent_of: dict[str, str] = {}
    if (
        stops_txt is not None
        and "stop_id" in stops_txt.columns
        and "parent_station" in stops_txt.columns
    ):
        for stop_id, parent in stops_txt.select(
            ["stop_id", "parent_station"]
        ).iter_rows():
            if parent:
                parent_of[stop_id] = parent

    trip_meta: dict[str, dict[str, str]] = {}
    if trips_txt is not None:
        for row in trips_txt.iter_rows(named=True):
            trip_id = row.get("trip_id") or ""
            trip_meta[trip_id] = {
                "route_id": row.get("route_id") or "",
                "direction_id": _direction_str(row.get("direction_id")),
            }

    patterns_map: dict[str, dict] = {}
    profiles_map: dict[str, dict] = {}
    trip_rows: list[dict] = []

    if (
        stop_times_txt is not None
        and stop_times_txt.height > 0
        and {"trip_id", "stop_sequence", "stop_id"} <= set(stop_times_txt.columns)
    ):
        st = stop_times_txt.with_columns(
            pl.col("stop_sequence").cast(pl.Int64, strict=False)
        ).sort(["trip_id", "stop_sequence"])
        grouped = st.group_by("trip_id", maintain_order=True).agg(
            [
                pl.col("stop_id").alias("stop_ids"),
                pl.col("arrival_time").alias("arrivals_raw"),
                pl.col("departure_time").alias("departures_raw"),
            ]
        )
        for row in grouped.iter_rows(named=True):
            trip_id = row["trip_id"]
            meta = trip_meta.get(trip_id)
            stop_ids: list[str] = row["stop_ids"]
            if meta is None or not stop_ids:
                continue  # stop_times with no matching trips.txt row (malformed feed)

            arrivals = [gtfs_time_to_seconds(v) for v in row["arrivals_raw"]]
            departures = [gtfs_time_to_seconds(v) for v in row["departures_raw"]]
            start_seconds = arrivals[0]
            if start_seconds is None:
                continue  # GTFS requires the first stop's arrival; skip malformed

            first_departure_seconds = departures[0]
            last_arrival_seconds = arrivals[-1]
            offsets = [None if a is None else a - start_seconds for a in arrivals]
            dwells = [
                None if (a is None or d is None) else d - a
                for a, d in zip(arrivals, departures, strict=True)
            ]
            station_ids = [parent_of.get(sid, sid) for sid in stop_ids]

            route_id = meta["route_id"]
            direction_id = meta["direction_id"]
            pattern_id = pattern_id_of(route_id, direction_id, stop_ids)
            profile_id = profile_id_of(offsets, dwells)

            patterns_map.setdefault(
                pattern_id,
                {
                    "pattern_id": pattern_id,
                    "route_id": route_id,
                    "direction_id": direction_id,
                    "stop_ids": stop_ids,
                    "station_ids": station_ids,
                },
            )
            profiles_map.setdefault(
                profile_id,
                {"profile_id": profile_id, "offsets": offsets, "dwells": dwells},
            )
            trip_rows.append(
                {
                    "trip_id": trip_id,
                    "pattern_id": pattern_id,
                    "profile_id": profile_id,
                    "start_seconds": start_seconds,
                    "first_departure_seconds": first_departure_seconds,
                    "last_arrival_seconds": last_arrival_seconds,
                    "stop_events": len(stop_ids),
                    "identity_key": identity_key_of(
                        route_id, first_departure_seconds, direction_id, station_ids
                    ),
                }
            )

    patterns_df = (
        pl.DataFrame(list(patterns_map.values()), schema=_PATTERNS_SCHEMA).sort(
            "pattern_id"
        )
        if patterns_map
        else pl.DataFrame(schema=_PATTERNS_SCHEMA)
    )
    time_profiles_df = (
        pl.DataFrame(list(profiles_map.values()), schema=_TIME_PROFILES_SCHEMA).sort(
            "profile_id"
        )
        if profiles_map
        else pl.DataFrame(schema=_TIME_PROFILES_SCHEMA)
    )
    trips_df = (
        pl.DataFrame(trip_rows, schema=_TRIPS_SCHEMA).sort("trip_id")
        if trip_rows
        else pl.DataFrame(schema=_TRIPS_SCHEMA)
    )

    return {
        "service_dates": service_dates_df,
        "patterns": patterns_df,
        "time_profiles": time_profiles_df,
        "trips": trips_df,
    }


# --- Sourcing tables (local zip / canonical parquet) ---------------------


def tables_from_archive(archive: dg.GTFSArchive) -> dict[str, pl.DataFrame]:
    """Build the `derive_semantics` input dict from a loaded
    `gtfs_digester.GTFSArchive` (a local zip, dev/CLI path). Files absent
    from the archive are simply absent from the returned dict."""
    tables: dict[str, pl.DataFrame] = {}
    for name in _INPUT_TABLES:
        filename = f"{name}.txt"
        if filename in archive:
            tables[name] = pl.from_arrow(archive.arrow_table(filename))
    return tables


def tables_from_digest(
    base_path: str,
    fingerprint: str,
    filesystem: fsspec.AbstractFileSystem | None = None,
) -> dict[str, pl.DataFrame]:
    """Build the `derive_semantics` input dict by reading a digest's
    canonical parquet tables (`base_path` is the canonical form's base,
    e.g. "gs://bucket/schedule" — schedule-canonical-form.md's layout).
    Tables the digest doesn't carry are simply absent."""
    import gtfs_digester as dg_runtime

    tables: dict[str, pl.DataFrame] = {}
    for name in _INPUT_TABLES:
        try:
            arrow_table = dg_runtime.read_table(
                base_path, fingerprint, name, filesystem=filesystem
            )
        except (FileNotFoundError, OSError):
            continue
        tables[name] = pl.from_arrow(arrow_table)
    return tables


# --- Writing ---------------------------------------------------------------


def write_semantics(
    tables: Mapping[str, pl.DataFrame],
    base_path: str,
    feed_digest: str,
    filesystem: fsspec.AbstractFileSystem | None = None,
) -> bool:
    """Write the four semantic parquet tables plus `metadata.json` (commit
    marker, written last) under `{base_path}/_feed_digest={feed_digest}/`.

    `base_path` should already include the revision segment (e.g.
    "gs://bucket/schedule-semantics/v1" — schedule-semantics.md §Storage
    layout); pass the same `<rev>` at read time (`tables_from_digest`
    reads canonical form, a different tree entirely). `filesystem`, when
    given, should already be resolved (and, on the worker, already wrapped
    for `customTime` stamping via `analysis_storage.stamped_filesystem` —
    every object here, marker included, is stamped at creation, matching
    the create-only IAM the pipeline SA holds on the analysis bucket).

    Skip-if-exists: returns False without writing anything when
    `metadata.json` already exists for this digest (content-addressed,
    re-materializing an existing digest is a no-op). Returns True when it
    wrote a fresh directory.
    """
    import gtfs_digester as dg_runtime

    if filesystem is None:
        import fsspec as fsspec_runtime

        filesystem, base_path = fsspec_runtime.core.url_to_fs(base_path)

    if dg_runtime.version_exists(base_path, feed_digest, filesystem=filesystem):
        return False

    version_dir = f"{base_path}/_feed_digest={feed_digest}"
    filesystem.mkdirs(version_dir, exist_ok=True)

    row_counts: dict[str, int] = {}
    for name in TABLE_NAMES:
        df = tables[name]
        buf = io.BytesIO()
        df.write_parquet(buf, compression="zstd")
        with filesystem.open(f"{version_dir}/{name}.parquet", "wb") as f:
            f.write(buf.getvalue())
        row_counts[name] = df.height

    metadata = {
        "feed_digest": feed_digest,
        "rev": SEMANTICS_REV,
        "written_at": datetime.now(UTC).isoformat(),
        "row_counts": row_counts,
    }
    with filesystem.open(f"{version_dir}/metadata.json", "wb") as f:
        f.write(json.dumps(metadata, indent=2).encode("utf-8"))

    return True


def derive_and_write_semantics(
    feed_digest: str, bucket: str, deadline: str | None
) -> None:
    """The on-demand materialization entry point (specs/schedule-semantics.md
    §Materialization): derive and write the semantic tables for a digest
    whose canonical form already exists but whose semantic tables don't.

    A platform worker's `"derive-semantics"` task handler calls this
    function with the dispatch's `feed_digest`, analysis bucket, and
    carried retention `deadline`.

    `deadline` is the ISO 8601 GCS `customTime` deadline (or None for a
    permanent/null-window version), the same contract the platform worker's
    `deadline_str` already threads through `_publish_schedule_to_analysis`.
    """
    from .analysis_storage import stamped_filesystem

    canonical_base = f"gs://{bucket}/schedule"
    semantics_base = f"gs://{bucket}/schedule-semantics/{SEMANTICS_REV}"

    tables = tables_from_digest(canonical_base, feed_digest)
    derived = derive_semantics(tables)

    deadline_dt = datetime.fromisoformat(deadline) if deadline else None
    filesystem, resolved_base_path = stamped_filesystem(semantics_base, deadline_dt)
    write_semantics(derived, resolved_base_path, feed_digest, filesystem=filesystem)
