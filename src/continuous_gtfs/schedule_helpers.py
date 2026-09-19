"""Public helpers for transforms that consume a parsed `gtfs_schedule_zip` input.

These were extracted from framework internals to a stable public API so that
agency `@step` functions and schedule-dependent RT builtins (
`ExpireCancelledTrips`, `InsertMissingCancellations`, `ConvertScheduledToNew`)
can compose them without reimplementing. Each helper takes the dict-of-
DataFrames shape that the framework produces when parsing
`content_kind=gtfs_schedule_zip` — typically `ctx.inputs["schedule"]`.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    import polars as pl


# Identity-keyed cache for trip_durations. The same `schedule` mapping
# is passed to multiple builtins within one dispatch (`InsertMissingCancellations`,
# `ExpireCancelledTrips`, agency `@step` functions), and computing the
# durations dominates per-dispatch latency on large feeds. Cache by
# `id(schedule)` so the second-and-later calls within one dispatch reuse
# the work.
#
# id() can be reused by Python after gc, so an old key could in
# principle alias a new mapping. Mitigated by:
#   - capping the cache at a few entries (LRU eviction)
#   - the cache holds a strong reference to `schedule` itself in the
#     value tuple so the id stays valid for as long as the cache entry
#     does (no id reuse while the entry is live)
_TRIP_DURATIONS_CACHE: OrderedDict[
    int, tuple[Mapping[str, pl.DataFrame], dict[str, tuple[int, int]]]
] = OrderedDict()
_TRIP_DURATIONS_CACHE_MAX = 4


def gtfs_time_to_seconds(value: str | None) -> int | None:
    """Parse a GTFS `HH:MM:SS` time string into seconds since service-day start.

    GTFS allows hours `>= 24` to encode trips that cross midnight; this
    helper handles those without complaint. Returns `None` if the input
    is empty or malformed so callers can skip rows defensively rather
    than catching exceptions in a hot loop.
    """
    if value is None or value == "":
        return None
    parts = value.split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        return None
    return h * 3600 + m * 60 + s


def trip_durations(
    schedule: Mapping[str, pl.DataFrame],
) -> dict[str, tuple[int, int]]:
    """Build `{trip_id: (start_seconds, end_seconds)}` from `stop_times.txt`.

    Seconds are relative to service-day start, so end values can exceed
    `86_400` for late-night trips. Trips with no parsable times are
    omitted; rows whose `departure_time` is empty fall back to
    `arrival_time`.

    Vectorized via Polars — on a ~380K-row `stop_times.txt` this
    runs in ~125ms vs the equivalent Python-iter loop's ~2.6s.
    Additionally cached by `id(schedule)`, so the second and later
    calls within one dispatch return without recomputing.
    """
    cache_key = id(schedule)
    entry = _TRIP_DURATIONS_CACHE.get(cache_key)
    if entry is not None and entry[0] is schedule:
        _TRIP_DURATIONS_CACHE.move_to_end(cache_key)
        return entry[1]

    result = _compute_trip_durations(schedule)
    _TRIP_DURATIONS_CACHE[cache_key] = (schedule, result)
    if len(_TRIP_DURATIONS_CACHE) > _TRIP_DURATIONS_CACHE_MAX:
        _TRIP_DURATIONS_CACHE.popitem(last=False)
    return result


def _compute_trip_durations(
    schedule: Mapping[str, pl.DataFrame],
) -> dict[str, tuple[int, int]]:
    import polars as pl

    stop_times = schedule.get("stop_times.txt")
    if stop_times is None:
        return {}

    # Choose the non-empty time per row: prefer departure_time, fall back
    # to arrival_time. `coalesce` treats empty string as a value not null,
    # so we map empties to null first.
    cols = stop_times.columns
    if "departure_time" not in cols and "arrival_time" not in cols:
        return {}

    def _empty_to_null(col_name: str) -> pl.Expr:
        if col_name not in cols:
            return pl.lit(None, dtype=pl.Utf8)
        return (
            pl.when(pl.col(col_name).cast(pl.Utf8).str.len_chars() > 0)
            .then(pl.col(col_name).cast(pl.Utf8))
            .otherwise(None)
        )

    time_expr = pl.coalesce(
        _empty_to_null("departure_time"),
        _empty_to_null("arrival_time"),
    )
    # Parse HH:MM:SS (allowing >24h) into seconds with a single str.extract_groups
    # call on the regex `^(\d+):(\d+):(\d+)$`. Non-matching rows yield null and
    # get filtered out before the group_by.
    pattern = r"^(\d+):(\d+):(\d+)$"
    parts = time_expr.str.extract_groups(pattern)
    seconds_expr = (
        parts.struct.field("1").cast(pl.Int64, strict=False) * 3600
        + parts.struct.field("2").cast(pl.Int64, strict=False) * 60
        + parts.struct.field("3").cast(pl.Int64, strict=False)
    ).alias("_seconds")

    grouped = (
        stop_times.lazy()
        .with_columns(seconds_expr)
        .filter(
            pl.col("trip_id").is_not_null()
            & (pl.col("trip_id").cast(pl.Utf8) != "")
            & pl.col("_seconds").is_not_null()
        )
        .group_by("trip_id")
        .agg(
            [
                pl.col("_seconds").min().alias("start_s"),
                pl.col("_seconds").max().alias("end_s"),
            ]
        )
        .collect()
    )

    return {row[0]: (int(row[1]), int(row[2])) for row in grouped.iter_rows()}


def agency_timezone(
    schedule: Mapping[str, pl.DataFrame], default: str = "UTC"
) -> ZoneInfo:
    """Return `ZoneInfo` for the schedule's `agency.txt` timezone.

    GTFS requires every agency in a feed to share a timezone, so the
    first row's `agency_timezone` field is canonical. Falls back to
    `ZoneInfo(default)` if `agency.txt` is missing, empty, or the value
    isn't a recognized IANA name.
    """
    agency = schedule.get("agency.txt")
    if agency is None or agency.height == 0:
        return ZoneInfo(default)
    try:
        tz_name = agency.select("agency_timezone").row(0)[0]
        if not tz_name:
            return ZoneInfo(default)
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo(default)


def service_day_start(now: datetime, threshold_hour: int = 4) -> datetime:
    """Return the local-midnight start of the GTFS service day containing `now`.

    Per GTFS convention, service days are anchored at the agency's local
    midnight but extend past 24:00 for late-night trips. A `now` earlier
    than `threshold_hour` (default 4 AM) local time still belongs to the
    *previous* service day. `now` must be tz-aware — typically built via
    `datetime.fromtimestamp(t, tz=agency_timezone(schedule))`.
    """
    if now.tzinfo is None:
        raise ValueError("service_day_start requires a tz-aware datetime")
    if now.hour < threshold_hour:
        service_date = (now - timedelta(days=1)).date()
    else:
        service_date = now.date()
    return datetime.combine(service_date, datetime.min.time(), tzinfo=now.tzinfo)


def active_service_ids(
    schedule: Mapping[str, pl.DataFrame], service_date: datetime
) -> set[str]:
    """Set of `service_id`s active on the given service date.

    Applies `calendar.txt` (start_date / end_date range + the per-weekday
    column for the requested date) and then overlays `calendar_dates.txt`
    exceptions (type 1 adds, type 2 removes). `service_date` should be
    tz-aware at the service-day-start (build via `service_day_start(now)`);
    only the date part is used.
    """
    import polars as pl

    service_date_str = service_date.strftime("%Y%m%d")
    weekday_col = service_date.strftime("%A").lower()

    active: set[str] = set()

    calendar = schedule.get("calendar.txt")
    if calendar is not None and weekday_col in calendar.columns:
        active_calendar = calendar.filter(
            (pl.col("start_date") <= service_date_str)
            & (pl.col("end_date") >= service_date_str)
            & (pl.col(weekday_col).cast(pl.Utf8) == "1")
        )
        active.update(active_calendar.select("service_id").to_series().to_list())

    calendar_dates = schedule.get("calendar_dates.txt")
    if calendar_dates is not None:
        exceptions = calendar_dates.filter(pl.col("date") == service_date_str)
        added = (
            exceptions.filter(pl.col("exception_type").cast(pl.Utf8) == "1")
            .select("service_id")
            .to_series()
            .to_list()
        )
        removed = (
            exceptions.filter(pl.col("exception_type").cast(pl.Utf8) == "2")
            .select("service_id")
            .to_series()
            .to_list()
        )
        active.update(added)
        active.difference_update(removed)

    return active


def active_trips(
    schedule: Mapping[str, pl.DataFrame], service_date: datetime
) -> pl.DataFrame:
    """Return the rows of `trips.txt` whose `service_id` is active on the date.

    Returns an empty DataFrame (preserving `trips.txt`'s schema where
    possible) if `trips.txt` is missing or no services are active.
    """
    import polars as pl

    trips = schedule.get("trips.txt")
    if trips is None:
        return pl.DataFrame()

    active_ids = active_service_ids(schedule, service_date)
    if not active_ids:
        return trips.clear()
    return trips.filter(pl.col("service_id").is_in(list(active_ids)))
