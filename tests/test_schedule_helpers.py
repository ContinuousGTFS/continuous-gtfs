"""Unit tests for the public schedule-helpers module."""

from __future__ import annotations

from datetime import datetime
from types import MappingProxyType
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from continuous_gtfs.schedule_helpers import (
    _TRIP_DURATIONS_CACHE,
    _TRIP_DURATIONS_CACHE_MAX,
    active_service_ids,
    active_trips,
    agency_timezone,
    gtfs_time_to_seconds,
    service_day_start,
    trip_durations,
)


@pytest.fixture(autouse=True)
def _clear_trip_durations_cache():
    """Reset the module-level cache between tests for deterministic state."""
    _TRIP_DURATIONS_CACHE.clear()
    yield
    _TRIP_DURATIONS_CACHE.clear()


# --- gtfs_time_to_seconds ---


@pytest.mark.parametrize(
    "value, expected",
    [
        ("00:00:00", 0),
        ("00:00:30", 30),
        ("01:02:03", 3723),
        ("10:00:00", 36_000),
        ("23:59:59", 86_399),
        ("24:00:00", 86_400),
        ("25:30:00", 91_800),  # late-night, crosses midnight
        ("", None),
        (None, None),
        ("garbage", None),
        ("10:00", None),  # missing seconds
        ("aa:bb:cc", None),
    ],
)
def test_gtfs_time_to_seconds(value, expected):
    assert gtfs_time_to_seconds(value) == expected


# --- trip_durations ---


def test_trip_durations_picks_min_and_max_per_trip():
    schedule = {
        "stop_times.txt": pl.DataFrame(
            [
                {
                    "trip_id": "T1",
                    "departure_time": "09:00:00",
                    "arrival_time": "09:00:00",
                },
                {
                    "trip_id": "T1",
                    "departure_time": "10:30:00",
                    "arrival_time": "10:30:00",
                },
                {
                    "trip_id": "T2",
                    "departure_time": "23:50:00",
                    "arrival_time": "23:50:00",
                },
                {
                    "trip_id": "T2",
                    "departure_time": "25:10:00",
                    "arrival_time": "25:10:00",
                },
            ]
        )
    }
    assert trip_durations(schedule) == {
        "T1": (32_400, 37_800),
        "T2": (85_800, 90_600),
    }


def test_trip_durations_falls_back_to_arrival_time_when_departure_missing():
    schedule = {
        "stop_times.txt": pl.DataFrame(
            [
                {"trip_id": "T1", "departure_time": "", "arrival_time": "08:00:00"},
                {"trip_id": "T1", "departure_time": "09:00:00", "arrival_time": ""},
            ]
        )
    }
    assert trip_durations(schedule) == {"T1": (28_800, 32_400)}


def test_trip_durations_empty_schedule_returns_empty_dict():
    assert trip_durations({}) == {}


# --- agency_timezone ---


def test_agency_timezone_reads_from_agency_txt():
    schedule = {
        "agency.txt": pl.DataFrame({"agency_timezone": ["America/Los_Angeles"]})
    }
    assert agency_timezone(schedule) == ZoneInfo("America/Los_Angeles")


def test_agency_timezone_defaults_to_utc_when_missing():
    assert agency_timezone({}) == ZoneInfo("UTC")


def test_agency_timezone_defaults_to_utc_when_empty():
    schedule = {"agency.txt": pl.DataFrame(schema={"agency_timezone": pl.Utf8})}
    assert agency_timezone(schedule) == ZoneInfo("UTC")


def test_agency_timezone_default_overridable():
    assert agency_timezone({}, default="America/New_York") == ZoneInfo(
        "America/New_York"
    )


# --- service_day_start ---


def test_service_day_start_returns_local_midnight_for_afternoon():
    now = datetime(2026, 5, 20, 15, 30, tzinfo=ZoneInfo("America/Los_Angeles"))
    assert service_day_start(now) == datetime(
        2026, 5, 20, 0, 0, tzinfo=ZoneInfo("America/Los_Angeles")
    )


def test_service_day_start_treats_early_morning_as_previous_service_day():
    # 03:00 local on May 21 still belongs to the May 20 service day under
    # the default 4 AM threshold.
    now = datetime(2026, 5, 21, 3, 0, tzinfo=ZoneInfo("America/Los_Angeles"))
    assert service_day_start(now) == datetime(
        2026, 5, 20, 0, 0, tzinfo=ZoneInfo("America/Los_Angeles")
    )


def test_service_day_start_threshold_hour_is_configurable():
    # 05:00 local with a 6 AM threshold → still previous service day.
    now = datetime(2026, 5, 21, 5, 0, tzinfo=ZoneInfo("America/Los_Angeles"))
    assert service_day_start(now, threshold_hour=6) == datetime(
        2026, 5, 20, 0, 0, tzinfo=ZoneInfo("America/Los_Angeles")
    )


def test_service_day_start_rejects_naive_datetime():
    with pytest.raises(ValueError, match="tz-aware"):
        service_day_start(datetime(2026, 5, 20, 12, 0))


# --- active_service_ids ---


def _calendar(service_ids: list[str], **flags) -> pl.DataFrame:
    """Build a calendar.txt with every-day flags by default."""
    rows = []
    days = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    for sid in service_ids:
        rows.append(
            {
                "service_id": sid,
                **{d: flags.get(d, 1) for d in days},
                "start_date": flags.get("start_date", "20260101"),
                "end_date": flags.get("end_date", "20261231"),
            }
        )
    return pl.DataFrame(rows)


def test_active_service_ids_resolves_calendar_weekday_and_date_range():
    # Mixed-day calendar: S_weekday active Mon–Fri, S_weekend active Sat–Sun.
    schedule = {
        "calendar.txt": pl.DataFrame(
            [
                {
                    "service_id": "S_weekday",
                    "monday": 1,
                    "tuesday": 1,
                    "wednesday": 1,
                    "thursday": 1,
                    "friday": 1,
                    "saturday": 0,
                    "sunday": 0,
                    "start_date": "20260101",
                    "end_date": "20261231",
                },
                {
                    "service_id": "S_weekend",
                    "monday": 0,
                    "tuesday": 0,
                    "wednesday": 0,
                    "thursday": 0,
                    "friday": 0,
                    "saturday": 1,
                    "sunday": 1,
                    "start_date": "20260101",
                    "end_date": "20261231",
                },
            ]
        )
    }
    # May 20 2026 is a Wednesday — only S_weekday should match.
    sid_set = active_service_ids(
        schedule, datetime(2026, 5, 20, tzinfo=ZoneInfo("UTC"))
    )
    assert sid_set == {"S_weekday"}


def test_active_service_ids_filters_by_date_range():
    schedule = {
        "calendar.txt": _calendar(["S1"], start_date="20260601", end_date="20261231")
    }
    # May 20 is before start_date.
    assert (
        active_service_ids(schedule, datetime(2026, 5, 20, tzinfo=ZoneInfo("UTC")))
        == set()
    )


def test_active_service_ids_calendar_dates_add_and_remove():
    schedule = {
        "calendar.txt": _calendar(["S_base"]),
        "calendar_dates.txt": pl.DataFrame(
            [
                {"service_id": "S_base", "date": "20260520", "exception_type": 2},
                {"service_id": "S_special", "date": "20260520", "exception_type": 1},
            ]
        ),
    }
    sid_set = active_service_ids(
        schedule, datetime(2026, 5, 20, tzinfo=ZoneInfo("UTC"))
    )
    assert sid_set == {"S_special"}


def test_active_service_ids_no_calendar_returns_empty():
    assert (
        active_service_ids({}, datetime(2026, 5, 20, tzinfo=ZoneInfo("UTC"))) == set()
    )


# --- active_trips ---


def test_active_trips_filters_trips_by_active_service():
    schedule = {
        "calendar.txt": _calendar(["S_active"]),
        "trips.txt": pl.DataFrame(
            [
                {"trip_id": "T1", "service_id": "S_active", "route_id": "R1"},
                {"trip_id": "T2", "service_id": "S_inactive", "route_id": "R2"},
            ]
        ),
    }
    df = active_trips(schedule, datetime(2026, 5, 20, tzinfo=ZoneInfo("UTC")))
    assert df.select("trip_id").to_series().to_list() == ["T1"]


def test_active_trips_empty_when_no_trips_txt():
    schedule = {"calendar.txt": _calendar(["S1"])}
    df = active_trips(schedule, datetime(2026, 5, 20, tzinfo=ZoneInfo("UTC")))
    assert df.height == 0


def test_active_trips_empty_when_no_active_services():
    schedule = {
        "calendar.txt": _calendar(
            ["S1"],
            monday=0,
            tuesday=0,
            wednesday=0,
            thursday=0,
            friday=0,
            saturday=0,
            sunday=0,
        ),
        "trips.txt": pl.DataFrame(
            [{"trip_id": "T1", "service_id": "S1", "route_id": "R1"}]
        ),
    }
    df = active_trips(schedule, datetime(2026, 5, 20, tzinfo=ZoneInfo("UTC")))
    assert df.height == 0


# --- trip_durations cache invariants ---
#
# The cache speeds up cancellation builtins that re-derive trip windows
# per dispatch. The tests below lock in the correctness guarantees:
# different schedule content must not see each other's cached results,
# the cache must hit on the same object across calls, eviction must be
# bounded, and the strong-ref on the cached schedule prevents id reuse
# while the entry is live.


def _schedule_with_times(trip_times: dict[str, list[str]]) -> dict:
    """Build a minimal schedule dict whose stop_times yields known durations."""
    rows = [
        {"trip_id": tid, "departure_time": t, "arrival_time": t}
        for tid, times in trip_times.items()
        for t in times
    ]
    return {"stop_times.txt": pl.DataFrame(rows)}


def test_trip_durations_cache_hit_returns_same_object():
    schedule = _schedule_with_times({"T1": ["08:00:00", "09:00:00"]})

    first = trip_durations(schedule)
    second = trip_durations(schedule)

    assert second is first, "second call on same dict must return the cached object"


def test_trip_durations_cache_distinguishes_different_versions():
    """Different content → different parse → different id → fresh compute."""
    v1 = _schedule_with_times({"T1": ["08:00:00", "09:00:00"]})  # 08:00–09:00
    v2 = _schedule_with_times({"T1": ["12:00:00", "14:00:00"]})  # 12:00–14:00

    d1 = trip_durations(v1)
    d2 = trip_durations(v2)

    assert d1["T1"] == (28_800, 32_400)
    assert d2["T1"] == (43_200, 50_400), (
        "v2 returned v1's result — cache leaked across schedule versions"
    )


def test_trip_durations_cache_holds_strong_ref_to_schedule():
    """The cache entry IS (schedule, result) — the strong ref that makes
    id()-keyed caching safe. If the entry ever stopped retaining the object
    (e.g. a weakref refactor), a GC'd schedule could hand its id to a new
    dict and the `entry[0] is schedule` guard would compare against garbage.
    Deterministic replacement for the old 2000-allocation probabilistic test."""
    schedule = _schedule_with_times({"T1": ["08:00:00", "09:00:00"]})
    result = trip_durations(schedule)
    entry = _TRIP_DURATIONS_CACHE[id(schedule)]
    assert entry[0] is schedule
    assert entry[1] is result


def test_trip_durations_cache_respects_lru_max():
    """Cache size never exceeds _TRIP_DURATIONS_CACHE_MAX."""
    # Push 2× the cap of distinct schedules. Hold strong refs locally so
    # they don't get GC'd mid-loop and confuse id() reuse.
    schedules = [
        _schedule_with_times({f"T{i}": ["08:00:00", "09:00:00"]})
        for i in range(_TRIP_DURATIONS_CACHE_MAX * 2)
    ]
    for s in schedules:
        trip_durations(s)
    assert len(_TRIP_DURATIONS_CACHE) == _TRIP_DURATIONS_CACHE_MAX


def test_trip_durations_cache_evicts_lru_oldest_first():
    """After overflow, the earliest-inserted entry is gone."""
    schedules = [
        _schedule_with_times({f"T{i}": ["08:00:00", "09:00:00"]})
        for i in range(_TRIP_DURATIONS_CACHE_MAX + 1)
    ]
    for s in schedules:
        trip_durations(s)

    # First schedule should have been evicted.
    assert id(schedules[0]) not in _TRIP_DURATIONS_CACHE
    # Latest insertion should be present.
    assert id(schedules[-1]) in _TRIP_DURATIONS_CACHE


def test_trip_durations_cache_handles_mapping_proxy_wrapping():
    """ctx.inputs wraps schedule in MappingProxyType —
    cache must work on the wrapper."""
    raw = _schedule_with_times({"T1": ["08:00:00", "09:00:00"]})
    proxy = MappingProxyType(raw)

    first = trip_durations(proxy)
    second = trip_durations(proxy)

    # Same wrapper instance → cache hit identity preserved.
    assert second is first
    # The cache key is the wrapper's id, not the inner dict's.
    assert id(proxy) in _TRIP_DURATIONS_CACHE
    # A raw-dict call would be a separate cache entry (different id).
    raw_durations = trip_durations(raw)
    assert raw_durations == first  # equal content
    assert id(raw) in _TRIP_DURATIONS_CACHE
