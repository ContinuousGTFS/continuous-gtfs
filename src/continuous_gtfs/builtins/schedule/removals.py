"""Semantic removal builtins — one step per GTFS record type.

`RemoveRows` is mechanical: one file, one mask. These steps remove a
*record* — a route, a trip, a stop, a service — with everything the GTFS
model says belongs to it, then tidy what the removal left empty. They are
thin configurations over `continuous_gtfs.schedule_records.remove_records`;
the record model lives there, not here.

See specs/transform-framework.md §Semantic removals.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

import polars as pl

from ...schedule_records import (
    CHILDREN_MODES,
    POLICY_VALUES,
    RECORD_TYPES,
    ChildrenMode,
    Policy,
    RemovalReport,
    affected_files,
    calendar_all_zero_services,
    feed_start_date,
    parse_gtfs_date,
    remove_records,
    service_active_dates,
)
from ...step import Step
from .transforms import (
    _COLUMN_MISSING_DECL,
    MatchCondition,
    _build_group_mask,
    _missing_fields,
    _normalize_groups,
    _warn_missing_columns,
)

if TYPE_CHECKING:
    from ...context import PipelineContext

ROWS_REMOVED = "rows_removed"
ROUTE_WITHOUT_TRIPS = "route_without_trips"
SHAPE_WITHOUT_TRIPS = "shape_without_trips"
TRIPS_REMOVED_WITH_STOPS = "trips_removed_with_stops"

_ROWS_REMOVED_DECL = (ROWS_REMOVED, {"subject": ["file"]})
_ROUTE_WITHOUT_TRIPS_DECL = (ROUTE_WITHOUT_TRIPS, {"subject": ["route_id"]})
_SHAPE_WITHOUT_TRIPS_DECL = (SHAPE_WITHOUT_TRIPS, {"subject": ["shape_id"]})

#: Which warning names an emptied-but-kept record of each type.
_EMPTIED_WARNINGS = {
    "route": (ROUTE_WITHOUT_TRIPS, "route_id"),
    "shape": (SHAPE_WITHOUT_TRIPS, "shape_id"),
}

Groups = list[MatchCondition] | list[list[MatchCondition]] | None


def _check_policy(name: str, value: str) -> Policy:
    if value not in POLICY_VALUES:
        raise ValueError(f"{name} must be one of {POLICY_VALUES}, got {value!r}")
    return value  # type: ignore[return-value]


class _SemanticRemoval(Step):
    """Shared machinery: resolve root ids, run the engine, report."""

    #: The registry key of the records this step removes.
    record_type: str = ""

    def __init__(
        self,
        conditions: Groups = None,
        *,
        ids: list[str] | None = None,
        exclude: list[list[MatchCondition]] | None = None,
        policy: dict[str, Policy],
        children: ChildrenMode = "remove",
        description: str = "",
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._groups = _normalize_groups(conditions or [])
        self._exclude = _normalize_groups(exclude or [], param="exclude")
        self._ids = [str(i) for i in (ids or [])]
        self._policy = policy
        self._children = children
        self.files = affected_files(self.record_type, policy)
        self.description = description or self._default_description()

    # -- selection -------------------------------------------------------

    @property
    def _type(self):
        return RECORD_TYPES[self.record_type]

    def _has_selection(self) -> bool:
        return bool(self._groups or self._ids)

    def _default_description(self) -> str:
        return f"Remove {self.record_type}s"

    def _select_root_ids(self, ctx: PipelineContext) -> set[str] | None:
        """Ids of the root records to remove, or None when the step must
        stand down (home file absent, or a condition on a missing column)."""
        home = self._type.files[0]
        df = ctx.output.get(home)
        if not isinstance(df, pl.DataFrame):
            return None
        return self._select_in_frame(ctx, home, df)

    def _select_in_frame(
        self, ctx: PipelineContext, file: str, df: pl.DataFrame
    ) -> set[str] | None:
        key = self._type.key
        if key not in df.columns:
            return None
        missing = _missing_fields(df, self._groups + self._exclude)
        if missing:
            _warn_missing_columns(ctx, file, missing)
            return None
        mask = pl.lit(False)
        if self._groups:
            mask = mask | _build_group_mask(df, self._groups)
        if self._ids:
            mask = mask | pl.col(key).cast(pl.Utf8).is_in(self._ids)
        if self._exclude:
            mask = mask & ~_build_group_mask(df, self._exclude)
        selected = df.filter(mask).get_column(key).cast(pl.Utf8).drop_nulls()
        return {v for v in selected.unique().to_list() if v != ""}

    # -- execution -------------------------------------------------------

    def apply(self, ctx: PipelineContext) -> None:
        ids = self._select_root_ids(ctx)
        if not ids:
            return
        report = remove_records(
            ctx.output,
            self.record_type,
            ids,
            policy=self._policy,
            children=self._children,
        )
        self._report(ctx, report)

    def _report(self, ctx: PipelineContext, report: RemovalReport) -> None:
        for file in sorted(report.rows_removed):
            count = report.rows_removed[file]
            role = report.roles[file]
            ctx.emit_finding(
                ROWS_REMOVED,
                severity="info",
                message=f"{self.name}: removed {count} row(s) from {file} ({role})",
                context={"file": file, "record_type": self.record_type, "role": role},
                occurrence_count=count,
            )
        for kind, ids in report.emptied_kept.items():
            code, key = _EMPTIED_WARNINGS[kind]
            for record_id in ids:
                ctx.emit_finding(
                    code,
                    severity="warning",
                    message=(
                        f"{kind} {record_id} has no trips after {self.name}; "
                        f"kept (empty_{kind}s='warn')"
                    ),
                    context={key: record_id},
                )
        pruned_trips = report.pruned.get("trip")
        if pruned_trips:
            ctx.emit_finding(
                TRIPS_REMOVED_WITH_STOPS,
                severity="warning",
                message=(
                    f"{self.name}: removing stops left {len(pruned_trips)} trip(s) "
                    "with no stop_times; the trips were removed too"
                ),
                context={"count": str(len(pruned_trips))},
                occurrence_count=len(pruned_trips),
            )
        for kind, ids in report.removed_ids.items():
            rt = RECORD_TYPES[kind]
            for record_id in sorted(ids):
                ctx.add_id_mapping(rt.files[0], rt.key, record_id, None)


class RemoveRoutes(_SemanticRemoval):
    """Remove routes with their trips, stop_times, fare rules, network rows,
    attributions and transfers.

    Select routes with `conditions` (the `RemoveRows` group shape, evaluated
    on `routes.txt`), `ids=[...]`, or both; `exclude` protects rows. A
    service left with no trips is removed (`empty_services`); a shape left
    with no trips is kept and warned about (`empty_shapes`).

    Usage:

        remove_link = RemoveRoutes(ids=["100479", "2LINE"])
    """

    record_type = "route"
    findings = [_ROWS_REMOVED_DECL, _COLUMN_MISSING_DECL, _SHAPE_WITHOUT_TRIPS_DECL]

    def __init__(
        self,
        conditions: Groups = None,
        *,
        ids: list[str] | None = None,
        exclude: list[list[MatchCondition]] | None = None,
        empty_services: Policy = "remove",
        empty_shapes: Policy = "warn",
        description: str = "",
        **kwargs: Any,
    ):
        super().__init__(
            conditions,
            ids=ids,
            exclude=exclude,
            policy={
                "service": _check_policy("empty_services", empty_services),
                "shape": _check_policy("empty_shapes", empty_shapes),
            },
            description=description,
            **kwargs,
        )
        if not self._has_selection():
            raise ValueError(f"{type(self).__name__} needs `conditions` or `ids`")


class RemoveTrips(_SemanticRemoval):
    """Remove trips with their stop_times, frequencies, attributions and
    transfers.

    A service left with no trips is removed (`empty_services`); a route or
    shape left with no trips is kept and warned about (`empty_routes`,
    `empty_shapes`) — agencies keep dormant routes on purpose.
    """

    record_type = "trip"
    findings = [
        _ROWS_REMOVED_DECL,
        _COLUMN_MISSING_DECL,
        _ROUTE_WITHOUT_TRIPS_DECL,
        _SHAPE_WITHOUT_TRIPS_DECL,
    ]

    def __init__(
        self,
        conditions: Groups = None,
        *,
        ids: list[str] | None = None,
        exclude: list[list[MatchCondition]] | None = None,
        empty_services: Policy = "remove",
        empty_routes: Policy = "warn",
        empty_shapes: Policy = "warn",
        description: str = "",
        **kwargs: Any,
    ):
        super().__init__(
            conditions,
            ids=ids,
            exclude=exclude,
            policy={
                "service": _check_policy("empty_services", empty_services),
                "route": _check_policy("empty_routes", empty_routes),
                "shape": _check_policy("empty_shapes", empty_shapes),
            },
            description=description,
            **kwargs,
        )
        if not self._has_selection():
            raise ValueError(f"{type(self).__name__} needs `conditions` or `ids`")


class RemoveStops(_SemanticRemoval):
    """Remove stops — stations, platforms, entrances — with their stop_times,
    transfers, pathways and stop_areas rows.

    `children` decides what happens to rows whose `parent_station` names a
    removed stop: `"remove"` (default) takes them too, recursively — a
    station means the station; `"detach"` clears `parent_station` on
    platforms and still removes entrances, generic nodes and boarding areas,
    which cannot exist without a parent.

    A trip left with no stop_times is removed (`empty_trips`) and a warning
    says so; whatever those trips emptied then follows the `RemoveTrips`
    defaults.

    Usage:

        remove_n15 = RemoveStops(ids=["N15"])   # the station and everything in it
    """

    record_type = "stop"
    findings = [
        _ROWS_REMOVED_DECL,
        _COLUMN_MISSING_DECL,
        TRIPS_REMOVED_WITH_STOPS,
        _ROUTE_WITHOUT_TRIPS_DECL,
        _SHAPE_WITHOUT_TRIPS_DECL,
    ]

    def __init__(
        self,
        conditions: Groups = None,
        *,
        ids: list[str] | None = None,
        exclude: list[list[MatchCondition]] | None = None,
        children: ChildrenMode = "remove",
        empty_trips: Policy = "remove",
        empty_services: Policy = "remove",
        empty_routes: Policy = "warn",
        empty_shapes: Policy = "warn",
        description: str = "",
        **kwargs: Any,
    ):
        if children not in CHILDREN_MODES:
            raise ValueError(
                f"children must be one of {CHILDREN_MODES}, got {children!r}"
            )
        super().__init__(
            conditions,
            ids=ids,
            exclude=exclude,
            policy={
                "trip": _check_policy("empty_trips", empty_trips),
                "service": _check_policy("empty_services", empty_services),
                "route": _check_policy("empty_routes", empty_routes),
                "shape": _check_policy("empty_shapes", empty_shapes),
            },
            children=children,
            description=description,
            **kwargs,
        )
        if not self._has_selection():
            raise ValueError(f"{type(self).__name__} needs `conditions` or `ids`")


class RemoveServices(_SemanticRemoval):
    """Remove services from both calendar files with their trips.

    Selection is by id only: `ids=[...]`, and/or condition groups whose
    every condition is on `service_id` (value or regex), evaluated over the
    union of service ids in `calendar.txt` and `calendar_dates.txt`, so a
    regex reaches a service defined only by exceptions. A condition on any
    other column raises at construction.

    Selectors:

    - `never_active=True` selects every service with no active date at
      all once its calendar row and its exceptions are resolved together.
      With `simplify_calendar=True` (default), a service kept alive only by
      `calendar_dates.txt` adds loses its all-zero `calendar.txt` row.
    - `expired_before` selects every service whose last active date is
      strictly before a cutoff: an explicit `YYYYMMDD` string or `date`, or
      `True` for the feed's own `feed_info.txt` `feed_start_date`. With no
      explicit date and no `feed_start_date` the step fails — never a
      silent no-op, and never the wall clock, so output stays a function
      of the run's inputs and configuration. Exception rows dated before
      the cutoff on services that survive are removed as well, counted in
      `calendar_dates.txt`'s `rows_removed`. A service with no active date
      belongs to `never_active`; combine the two in one step.

    Selected sets union; `exclude` protects a service from every selector.
    A route or shape left with no trips is kept and warned about.

    Usage:

        drop_dead_services = RemoveServices(never_active=True)
        drop_llr = RemoveServices([MatchCondition("service_id", regex=r"^LLR")])
        drop_expired = RemoveServices(expired_before=True)      # feed_start_date
        drop_spring = RemoveServices(expired_before="20260601")  # explicit
    """

    record_type = "service"
    findings = [
        _ROWS_REMOVED_DECL,
        _ROUTE_WITHOUT_TRIPS_DECL,
        _SHAPE_WITHOUT_TRIPS_DECL,
    ]

    def __init__(
        self,
        conditions: Groups = None,
        *,
        ids: list[str] | None = None,
        exclude: list[list[MatchCondition]] | None = None,
        never_active: bool = False,
        simplify_calendar: bool = True,
        expired_before: bool | str | date = False,
        empty_routes: Policy = "warn",
        empty_shapes: Policy = "warn",
        description: str = "",
        **kwargs: Any,
    ):
        # Selector state first: `_has_selection` and the default description
        # read it, and the base constructor calls both.
        self._never_active = never_active
        self._simplify_calendar = simplify_calendar
        self._expired_before = _parse_expired_before(expired_before)
        # Per-run scratch, reset by apply().
        self._cutoff: date | None = None
        self._simplified = 0
        super().__init__(
            conditions,
            ids=ids,
            exclude=exclude,
            policy={
                "route": _check_policy("empty_routes", empty_routes),
                "shape": _check_policy("empty_shapes", empty_shapes),
            },
            description=description,
            **kwargs,
        )
        for group in self._groups + self._exclude:
            for cond in group:
                if cond.field != "service_id":
                    raise ValueError(
                        f"{type(self).__name__} selects services by service_id "
                        f"only; a condition on {cond.field!r} is not allowed"
                    )
        if not self._has_selection():
            raise ValueError(
                f"{type(self).__name__} needs `conditions`, `ids`, "
                "`never_active=True`, or `expired_before`"
            )

    def _has_selection(self) -> bool:
        return bool(
            self._groups
            or self._ids
            or self._never_active
            or self._expired_before is not None
        )

    def _default_description(self) -> str:
        if self._expired_before is None:
            return "Remove services"
        if self._expired_before is True:
            return "Remove services expired before feed_start_date"
        return f"Remove services expired before {self._expired_before:%Y%m%d}"

    def _resolve_cutoff(self, output: dict[str, Any]) -> date | None:
        """The expiry cutoff, or None when the selector is off. Raises when
        the selector is on and no source can supply a date."""
        if self._expired_before is None:
            return None
        if self._expired_before is not True:
            return self._expired_before
        cutoff = feed_start_date(output)
        if cutoff is None:
            raise ValueError(
                f"{type(self).__name__}: expired_before=True needs "
                "feed_info.txt with a feed_start_date, and the feed has none; "
                "pass an explicit expired_before='YYYYMMDD' cutoff instead"
            )
        return cutoff

    def _select_root_ids(self, ctx: PipelineContext) -> set[str] | None:
        output = ctx.output
        defined: set[str] = set()
        for file in self._type.files:
            df = output.get(file)
            if isinstance(df, pl.DataFrame) and "service_id" in df.columns:
                values = df.get_column("service_id").cast(pl.Utf8).drop_nulls()
                defined |= {v for v in values.unique().to_list() if v != ""}
        if not defined:
            return None

        selected: set[str] = set()
        if self._groups or self._ids:
            universe = pl.DataFrame({"service_id": sorted(defined)})
            selected |= self._select_in_frame(ctx, "calendar.txt", universe) or set()

        active = (
            service_active_dates(output)
            if self._never_active or self._cutoff is not None
            else {}
        )

        if self._never_active:
            never = {sid for sid, dates in active.items() if not dates}
            selected |= never
            if self._simplify_calendar:
                # Alive through adds alone: drop the dead calendar row, keep
                # the exceptions. Not a removal of the service.
                simplify = calendar_all_zero_services(output) - never
                self._simplified = self._drop_calendar_rows(output, simplify)

        if self._cutoff is not None:
            cutoff = self._cutoff
            selected |= {
                sid for sid, dates in active.items() if dates and dates[-1] < cutoff
            }

        if self._exclude and (self._never_active or self._cutoff is not None):
            universe = pl.DataFrame({"service_id": sorted(selected)})
            protected = universe.filter(_build_group_mask(universe, self._exclude))
            selected -= set(protected.get_column("service_id").to_list())
        return selected

    @staticmethod
    def _drop_calendar_rows(output: dict[str, Any], service_ids: set[str]) -> int:
        calendar = output.get("calendar.txt")
        if not service_ids or not isinstance(calendar, pl.DataFrame):
            return 0
        kept = calendar.filter(
            ~pl.col("service_id").cast(pl.Utf8).is_in(sorted(service_ids))
        )
        output["calendar.txt"] = kept
        return calendar.height - kept.height

    @staticmethod
    def _drop_stale_exceptions(output: dict[str, Any], cutoff: date) -> int:
        """Drop `calendar_dates.txt` rows dated before `cutoff`; unparseable
        dates are kept. Returns the rows removed."""
        exceptions = output.get("calendar_dates.txt")
        if not isinstance(exceptions, pl.DataFrame) or "date" not in exceptions.columns:
            return 0
        parsed = pl.col("date").cast(pl.Utf8).str.to_date("%Y%m%d", strict=False)
        stale = (parsed < pl.lit(cutoff)).fill_null(False)
        kept = exceptions.filter(~stale)
        output["calendar_dates.txt"] = kept
        return exceptions.height - kept.height

    def apply(self, ctx: PipelineContext) -> None:
        self._simplified = 0
        # Resolve before selecting: a missing cutoff fails regardless of
        # which calendar files the feed carries.
        self._cutoff = self._resolve_cutoff(ctx.output)
        ids = self._select_root_ids(ctx)
        report = RemovalReport()
        if ids:
            report = remove_records(
                ctx.output, self.record_type, ids, policy=self._policy
            )
        if self._simplified:
            report.rows_removed["calendar.txt"] = (
                report.rows_removed.get("calendar.txt", 0) + self._simplified
            )
            report.roles["calendar.txt"] = "root"
        if self._cutoff is not None:
            stale = self._drop_stale_exceptions(ctx.output, self._cutoff)
            if stale:
                report.rows_removed["calendar_dates.txt"] = (
                    report.rows_removed.get("calendar_dates.txt", 0) + stale
                )
                report.roles["calendar_dates.txt"] = "root"
        if report.rows_removed:
            self._report(ctx, report)


def _parse_expired_before(value: bool | str | date) -> date | bool | None:
    """Normalize the `expired_before` parameter: None (off), True (use the
    feed's `feed_start_date`), or the explicit date."""
    if value is False or value is None:
        return None
    if value is True:
        return True
    if isinstance(value, date):
        return value
    parsed = parse_gtfs_date(value) if isinstance(value, str) else None
    if parsed is None:
        raise ValueError(
            f"expired_before must be True, a date, or a YYYYMMDD string, got {value!r}"
        )
    return parsed
