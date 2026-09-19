"""GTFS record registry and the traversal engine behind the semantic removals.

A mechanical ``RemoveRows`` edits one file by a mask. The semantic removal
builtins (``RemoveRoutes``, ``RemoveTrips``, ``RemoveStops``,
``RemoveServices`` — see ``continuous_gtfs.builtins.schedule``) remove a
*record* with everything the GTFS model says belongs to it, then tidy what
the removal left empty. This module is what they share: one declarative
registry of record types and one engine that walks it, so each step is a
thin configuration rather than a hand-written cascade. Custom ``@step``
functions may call :func:`remove_records` directly.

See specs/transform-framework.md §Semantic removals.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Literal

import polars as pl

#: What a step does with a parent record the removal left with no activity.
Policy = Literal["remove", "warn", "keep"]
POLICY_VALUES: tuple[Policy, ...] = ("remove", "warn", "keep")

#: How ``RemoveStops`` treats rows whose ``parent_station`` names a removed stop.
ChildrenMode = Literal["remove", "detach"]
CHILDREN_MODES: tuple[ChildrenMode, ...] = ("remove", "detach")

#: ``location_type`` values that require a parent — always removed with it.
_REQUIRES_PARENT = ("2", "3", "4")


@dataclass(frozen=True)
class Reference:
    """A column in ``file`` that names records of the owning type."""

    file: str
    column: str


@dataclass(frozen=True)
class EmptiedBy:
    """The reference whose disappearance leaves a record of this type empty.

    A route is emptied when no ``trips.txt`` row names it; a trip when no
    ``stop_times.txt`` row does.
    """

    file: str
    column: str


@dataclass(frozen=True)
class RecordType:
    name: str
    #: Home file(s). The first is canonical — ``id_mappings`` are recorded under it.
    files: tuple[str, ...]
    key: str
    references: tuple[Reference, ...] = ()
    emptied_by: EmptiedBy | None = None


RECORD_TYPES: dict[str, RecordType] = {
    "route": RecordType(
        "route",
        ("routes.txt",),
        "route_id",
        references=(
            Reference("trips.txt", "route_id"),
            Reference("fare_rules.txt", "route_id"),
            Reference("route_networks.txt", "route_id"),
            Reference("attributions.txt", "route_id"),
            Reference("transfers.txt", "from_route_id"),
            Reference("transfers.txt", "to_route_id"),
        ),
        emptied_by=EmptiedBy("trips.txt", "route_id"),
    ),
    "trip": RecordType(
        "trip",
        ("trips.txt",),
        "trip_id",
        references=(
            Reference("stop_times.txt", "trip_id"),
            Reference("frequencies.txt", "trip_id"),
            Reference("attributions.txt", "trip_id"),
            Reference("transfers.txt", "from_trip_id"),
            Reference("transfers.txt", "to_trip_id"),
        ),
        emptied_by=EmptiedBy("stop_times.txt", "trip_id"),
    ),
    "stop": RecordType(
        "stop",
        ("stops.txt",),
        "stop_id",
        references=(
            Reference("stop_times.txt", "stop_id"),
            Reference("transfers.txt", "from_stop_id"),
            Reference("transfers.txt", "to_stop_id"),
            Reference("pathways.txt", "from_stop_id"),
            Reference("pathways.txt", "to_stop_id"),
            Reference("stop_areas.txt", "stop_id"),
            # The one self-reference; handled by the ``children`` mode.
            Reference("stops.txt", "parent_station"),
        ),
    ),
    "service": RecordType(
        "service",
        ("calendar.txt", "calendar_dates.txt"),
        "service_id",
        references=(Reference("trips.txt", "service_id"),),
        emptied_by=EmptiedBy("trips.txt", "service_id"),
    ),
    "shape": RecordType(
        "shape",
        ("shapes.txt",),
        "shape_id",
        # trips.shape_id is optional; a shape is never cascaded into, only
        # pruned when the trips that used it are gone.
        emptied_by=EmptiedBy("trips.txt", "shape_id"),
    ),
}

#: Home file → the record type whose rows it holds.
_HOME_TYPE: dict[str, str] = {
    f: rt.name for rt in RECORD_TYPES.values() for f in rt.files
}


@dataclass
class RemovalReport:
    """What one :func:`remove_records` call did — the source of every finding."""

    #: file → rows removed (only files with at least one row removed).
    rows_removed: dict[str, int] = field(default_factory=dict)
    #: file → ``root`` | ``cascade`` | ``prune``.
    roles: dict[str, str] = field(default_factory=dict)
    #: record type → every id removed (root, cascaded, or pruned).
    removed_ids: dict[str, set[str]] = field(default_factory=dict)
    #: record type → ids removed by reverse cleanup (subset of ``removed_ids``).
    pruned: dict[str, set[str]] = field(default_factory=dict)
    #: record type → ids left empty and kept under the ``warn`` policy.
    emptied_kept: dict[str, list[str]] = field(default_factory=dict)
    #: stops whose ``parent_station`` was cleared under ``children="detach"``.
    detached: int = 0


def affected_files(record_type: str, policy_types: Iterable[str] = ()) -> list[str]:
    """Every file a removal of ``record_type`` can touch, for a step's ``files``.

    Follows the registry's references transitively and adds the home files
    of every type the step's reverse-cleanup policy names.
    """
    seen: dict[str, None] = {}
    stack = [record_type, *policy_types]
    visited: set[str] = set()
    while stack:
        name = stack.pop()
        if name in visited:
            continue
        visited.add(name)
        rt = RECORD_TYPES[name]
        for f in rt.files:
            seen.setdefault(f, None)
        for ref in rt.references:
            seen.setdefault(ref.file, None)
            child = _HOME_TYPE.get(ref.file)
            if child is not None and child != name:
                stack.append(child)
    return list(seen)


def remove_records(
    output: dict[str, pl.DataFrame],
    record_type: str,
    ids: Iterable[str],
    *,
    policy: Mapping[str, Policy] | None = None,
    children: ChildrenMode = "remove",
) -> RemovalReport:
    """Remove the ``record_type`` records named by ``ids`` and everything that
    belongs to them, then apply reverse cleanup under ``policy``.

    Mutates ``output`` in place. ``policy`` maps a record type to what to do
    when this call leaves one of its records with no activity: ``remove``
    (and cascade in turn), ``warn`` (keep; report it), or ``keep``. A type
    not in ``policy`` is left alone silently. Only records **this call**
    emptied qualify — a route with no trips before the call is never
    considered. Ids that name no existing record are ignored.

    Cascade runs to a fixpoint and a row reached twice is removed once, so
    the result does not depend on traversal order.
    """
    policy = dict(policy or {})
    for name, action in policy.items():
        if name not in RECORD_TYPES:
            raise ValueError(f"unknown record type in policy: {name!r}")
        if action not in POLICY_VALUES:
            raise ValueError(
                f"policy for {name!r} must be one of {POLICY_VALUES}, got {action!r}"
            )
        if RECORD_TYPES[name].emptied_by is None:
            raise ValueError(f"record type {name!r} has no notion of being emptied")
    if children not in CHILDREN_MODES:
        raise ValueError(f"children must be one of {CHILDREN_MODES}, got {children!r}")
    if record_type not in RECORD_TYPES:
        raise ValueError(f"unknown record type: {record_type!r}")

    before_counts = {
        f: df.height for f, df in output.items() if isinstance(df, pl.DataFrame)
    }
    referenced_before = {
        name: _referenced(output, RECORD_TYPES[name].emptied_by) for name in policy
    }

    report = RemovalReport()
    removed: dict[str, set[str]] = defaultdict(set)
    pruned: dict[str, set[str]] = defaultdict(set)
    emptied_kept: dict[str, set[str]] = defaultdict(set)
    worklist: list[tuple[str, set[str]]] = [
        (record_type, set(ids) & _existing_keys(output, RECORD_TYPES[record_type]))
    ]

    def drain() -> None:
        while worklist:
            name, batch = worklist.pop()
            batch = batch - removed[name]
            if not batch:
                continue
            removed[name] |= batch
            rt = RECORD_TYPES[name]
            for f in rt.files:
                _drop_rows(output, f, rt.key, batch)
            for ref in rt.references:
                df = output.get(ref.file)
                if not isinstance(df, pl.DataFrame) or ref.column not in df.columns:
                    continue
                mask = pl.col(ref.column).cast(pl.Utf8).is_in(sorted(batch))
                if ref.file == "stops.txt" and ref.column == "parent_station":
                    report.detached += _handle_children(
                        output, df, mask, children, worklist
                    )
                    continue
                child = _HOME_TYPE.get(ref.file)
                if child is not None and child != name:
                    keys = _column_values(df.filter(mask), RECORD_TYPES[child].key)
                    worklist.append((child, keys))
                else:
                    output[ref.file] = df.filter(~mask)

    drain()

    while True:
        grew = False
        for name, action in policy.items():
            if action == "keep":
                continue
            rt = RECORD_TYPES[name]
            emptied = (
                referenced_before[name]
                - _referenced(output, rt.emptied_by)
                - removed[name]
            ) & _existing_keys(output, rt)
            if not emptied:
                continue
            if action == "warn":
                emptied_kept[name] |= emptied
            else:
                pruned[name] |= emptied
                worklist.append((name, emptied))
                grew = True
        if not grew:
            break
        drain()

    root_files = set(RECORD_TYPES[record_type].files)
    prune_files = {
        f for name, s in pruned.items() if s for f in RECORD_TYPES[name].files
    }
    for f, before in before_counts.items():
        after = output[f].height
        if after < before:
            report.rows_removed[f] = before - after
            report.roles[f] = (
                "root"
                if f in root_files
                else "prune"
                if f in prune_files
                else "cascade"
            )
    report.removed_ids = {k: v for k, v in removed.items() if v}
    report.pruned = {k: v for k, v in pruned.items() if v}
    report.emptied_kept = {k: sorted(v) for k, v in emptied_kept.items() if v}
    return report


def _handle_children(
    output: dict[str, pl.DataFrame],
    stops: pl.DataFrame,
    mask: pl.Expr,
    children: ChildrenMode,
    worklist: list[tuple[str, set[str]]],
) -> int:
    """Apply the ``children`` mode to stops whose parent is being removed.

    Returns the number of stops detached. Children that require a parent
    (entrances, generic nodes, boarding areas) are removed in either mode.
    """
    if "location_type" in stops.columns:
        requires_parent = (
            pl.col("location_type").cast(pl.Utf8).fill_null("").is_in(_REQUIRES_PARENT)
        )
    else:
        requires_parent = pl.lit(False)

    if children == "remove":
        keys = _column_values(stops.filter(mask), "stop_id")
        worklist.append(("stop", keys))
        return 0

    keys = _column_values(stops.filter(mask & requires_parent), "stop_id")
    worklist.append(("stop", keys))
    detach = mask & ~requires_parent
    detached = int(stops.select(detach.sum()).item() or 0)
    if detached:
        output["stops.txt"] = stops.with_columns(
            pl.when(detach)
            .then(pl.lit(""))
            .otherwise(pl.col("parent_station"))
            .alias("parent_station")
        )
    return detached


def _drop_rows(
    output: dict[str, pl.DataFrame], file: str, key: str, ids: set[str]
) -> None:
    df = output.get(file)
    if not isinstance(df, pl.DataFrame) or key not in df.columns:
        return
    output[file] = df.filter(~pl.col(key).cast(pl.Utf8).is_in(sorted(ids)))


def _column_values(df: pl.DataFrame, column: str) -> set[str]:
    """Non-empty distinct values of ``column`` as strings (empty if absent)."""
    if column not in df.columns:
        return set()
    values = df.get_column(column).cast(pl.Utf8).drop_nulls().unique().to_list()
    return {v for v in values if v != ""}


def _referenced(output: dict[str, pl.DataFrame], via: EmptiedBy | None) -> set[str]:
    if via is None:
        return set()
    df = output.get(via.file)
    if not isinstance(df, pl.DataFrame):
        return set()
    return _column_values(df, via.column)


def _existing_keys(output: dict[str, pl.DataFrame], rt: RecordType) -> set[str]:
    keys: set[str] = set()
    for f in rt.files:
        df = output.get(f)
        if isinstance(df, pl.DataFrame):
            keys |= _column_values(df, rt.key)
    return keys


# --- Service dates ---------------------------------------------------------
#
# The shared date-resolution primitive behind RemoveServices' selectors:
# `never_active` (services with no active date) now, `expired_before`
# (services whose last active date precedes a cutoff — issue #642) next.

_WEEKDAYS = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)


def parse_gtfs_date(value: object) -> date | None:
    """``YYYYMMDD`` → ``date``; ``None`` for anything else."""
    if not isinstance(value, str) or len(value) != 8:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        return None


def service_active_dates(
    schedule: Mapping[str, pl.DataFrame],
) -> dict[str, list[date]]:
    """Every service id the feed defines → its active dates, sorted.

    Expands each ``calendar.txt`` row's weekday flags over its
    ``start_date``..``end_date`` range, then overlays ``calendar_dates.txt``
    (``exception_type`` ``1`` adds, ``2`` removes). A service defined in
    either file appears in the result even when it has no active date. A
    calendar row whose dates fail to parse contributes no days.
    """
    active: dict[str, set[date]] = defaultdict(set)

    calendar = schedule.get("calendar.txt")
    if isinstance(calendar, pl.DataFrame) and "service_id" in calendar.columns:
        for row in calendar.iter_rows(named=True):
            sid = row["service_id"]
            if sid is None or sid == "":
                continue
            days = active[str(sid)]
            start = parse_gtfs_date(row.get("start_date"))
            end = parse_gtfs_date(row.get("end_date"))
            if start is None or end is None or end < start:
                continue
            flags = [str(row.get(d) or "") == "1" for d in _WEEKDAYS]
            if not any(flags):
                continue
            current = start
            while current <= end:
                if flags[current.weekday()]:
                    days.add(current)
                current += timedelta(days=1)

    exceptions = schedule.get("calendar_dates.txt")
    if isinstance(exceptions, pl.DataFrame) and "service_id" in exceptions.columns:
        for row in exceptions.iter_rows(named=True):
            sid = row["service_id"]
            if sid is None or sid == "":
                continue
            days = active[str(sid)]
            day = parse_gtfs_date(row.get("date"))
            if day is None:
                continue
            kind = str(row.get("exception_type") or "")
            if kind == "1":
                days.add(day)
            elif kind == "2":
                days.discard(day)

    return {sid: sorted(days) for sid, days in active.items()}


def calendar_all_zero_services(schedule: Mapping[str, pl.DataFrame]) -> set[str]:
    """Service ids whose ``calendar.txt`` row has every weekday flag off."""
    calendar = schedule.get("calendar.txt")
    if not isinstance(calendar, pl.DataFrame) or "service_id" not in calendar.columns:
        return set()
    result: set[str] = set()
    for row in calendar.iter_rows(named=True):
        sid = row["service_id"]
        if sid is None or sid == "":
            continue
        if not any(str(row.get(d) or "") == "1" for d in _WEEKDAYS):
            result.add(str(sid))
    return result


def feed_start_date(schedule: Mapping[str, pl.DataFrame]) -> date | None:
    """``feed_info.txt``'s ``feed_start_date`` as a date, or ``None`` when the
    file, column, or a parseable value is absent."""
    info = schedule.get("feed_info.txt")
    if (
        not isinstance(info, pl.DataFrame)
        or info.height == 0
        or "feed_start_date" not in info.columns
    ):
        return None
    return parse_gtfs_date(info.get_column("feed_start_date").cast(pl.Utf8)[0])
