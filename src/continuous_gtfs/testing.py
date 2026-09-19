"""Public test-helper API for unit-testing transform steps.

A transform step — a ``continuous_gtfs`` builtin instance like ``RemoveRows``
or ``UpdateFields``, or a custom ``@step`` function — exposes ``.apply(ctx)``,
which mutates a :class:`~continuous_gtfs.PipelineContext` in place. To unit-test
one step in isolation:

1. Build a ``PipelineContext`` whose ``.output`` holds the GTFS table(s) or
   GTFS-RT feed(s) the step reads — these helpers do that.
2. Call ``step.apply(ctx)``.
3. Assert on the mutated ``ctx.output``.

No orchestrator, no network, no real feed — just the step's logic against a
tiny in-memory fixture.

Example::

    from continuous_gtfs.testing import gtfs_df, schedule_context

    def test_remove_inactive_calendars():
        calendar = gtfs_df(
            \"\"\"
            service_id,monday,tuesday
            DEAD,0,0
            LIVE,1,0
            \"\"\"
        )
        ctx = schedule_context(**{"calendar.txt": calendar})
        remove_inactive_calendars.apply(ctx)
        assert ctx.output["calendar.txt"]["service_id"].to_list() == ["LIVE"]
"""

from __future__ import annotations

import io
import textwrap

import polars as pl
from google.transit import gtfs_realtime_pb2 as gtfs_rt

from continuous_gtfs import PipelineContext

__all__ = [
    "gtfs_df",
    "schedule_context",
    "realtime_context",
    "trip_updates_feed",
    "vehicle_positions_feed",
    "assert_unchanged",
]


def gtfs_df(csv: str) -> pl.DataFrame:
    """Parse a CSV string into an all-text polars DataFrame.

    GTFS columns are text — a ``route_type`` of ``3`` and a ``stop_id`` of
    ``E01`` are both strings. :class:`~continuous_gtfs.builtins.schedule.MatchCondition`
    compares against string values. Forcing every column to ``Utf8``
    (``infer_schema_length=0``) keeps fixtures faithful to how the worker
    parses a real feed — otherwise polars would infer ``0``/``1``
    service-day flags as integers and conditions like
    ``MatchCondition("monday", value="0")`` would silently never match.

    Leading indentation is stripped via :func:`textwrap.dedent`, so the CSV
    block can be indented to match the surrounding test code without that
    indentation leaking into column names or values::

        calendar = gtfs_df(
            \"\"\"
            service_id,monday
            DEAD,0
            LIVE,1
            \"\"\"
        )
        # → polars.DataFrame with Utf8 columns; "DEAD" and "LIVE" as service_id

    Args:
        csv: A CSV-formatted string, optionally indented.

    Returns:
        A polars DataFrame with all columns typed ``Utf8``.
    """
    cleaned = textwrap.dedent(csv).strip() + "\n"
    return pl.read_csv(io.StringIO(cleaned), infer_schema_length=0)


def schedule_context(**tables: pl.DataFrame) -> PipelineContext:
    """Return a :class:`~continuous_gtfs.PipelineContext` seeded for schedule testing.

    ``ctx.output`` is populated with the supplied GTFS tables; keys should be
    GTFS filenames. Schedule steps read and mutate tables in ``ctx.output``.

    Prefer the explicit-filename dict form so the keys match what steps expect::

        ctx = schedule_context(**{"stops.txt": stops_df, "routes.txt": routes_df})

    Keyword arguments whose names don't end in ``.txt`` still work (the dict is
    passed through as-is), but the step will only find them under the key you
    provide, so be deliberate about the naming.

    Args:
        **tables: Keyword arguments mapping GTFS filenames to polars DataFrames.

    Returns:
        A :class:`~continuous_gtfs.PipelineContext` ready to pass to
        ``step.apply(ctx)``.
    """
    return PipelineContext(output=dict(tables))


def realtime_context(**feeds: gtfs_rt.FeedMessage) -> PipelineContext:
    """Return a :class:`~continuous_gtfs.PipelineContext` seeded for realtime testing.

    ``ctx.output`` is populated with the supplied GTFS-RT feeds; keys are feed
    names (``"trip_updates"``, ``"vehicle_positions"``, etc.). Realtime steps
    iterate every :class:`~google.transit.gtfs_realtime_pb2.FeedMessage` in
    ``ctx.output``::

        feed = trip_updates_feed(trip_id="T1", stop_ids=["S1", "S2"])
        ctx = realtime_context(trip_updates=feed)
        some_rt_step.apply(ctx)

    Args:
        **feeds: Keyword arguments mapping feed names to ``FeedMessage`` objects.

    Returns:
        A :class:`~continuous_gtfs.PipelineContext` ready to pass to
        ``step.apply(ctx)``.
    """
    return PipelineContext(output=dict(feeds))


def trip_updates_feed(
    *,
    trip_id: str,
    stop_ids: list[str],
) -> gtfs_rt.FeedMessage:
    """Return a minimal GTFS-RT TripUpdate feed.

    Creates a :class:`~google.transit.gtfs_realtime_pb2.FeedMessage` with a
    single ``TripUpdate`` entity containing one ``StopTimeUpdate`` per entry in
    ``stop_ids``::

        feed = trip_updates_feed(trip_id="trip_42", stop_ids=["S1", "S2", "S3"])
        ctx = realtime_context(trip_updates=feed)
        some_step.apply(ctx)
        assert len(ctx.output["trip_updates"].entity) == 1

    Args:
        trip_id: The trip identifier for the single ``TripUpdate`` entity.
        stop_ids: Ordered list of stop identifiers. Each becomes a
            ``StopTimeUpdate`` with ``stop_sequence`` starting at 1.

    Returns:
        A populated :class:`~google.transit.gtfs_realtime_pb2.FeedMessage`.
    """
    feed = gtfs_rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_712_345_678
    entity = feed.entity.add()
    entity.id = trip_id
    entity.trip_update.trip.trip_id = trip_id
    for seq, stop_id in enumerate(stop_ids, start=1):
        stu = entity.trip_update.stop_time_update.add()
        stu.stop_sequence = seq
        stu.stop_id = stop_id
    return feed


def vehicle_positions_feed(
    *,
    vehicle_ids: list[str],
    trip_ids: list[str] | None = None,
    stop_ids: list[str] | None = None,
) -> gtfs_rt.FeedMessage:
    """Return a minimal GTFS-RT VehiclePositions feed.

    Creates a :class:`~google.transit.gtfs_realtime_pb2.FeedMessage` with one
    ``VehiclePosition`` entity per entry in ``vehicle_ids``::

        feed = vehicle_positions_feed(
            vehicle_ids=["v1", "v2"],
            trip_ids=["T1", "T2"],
            stop_ids=["S10", "S20"],
        )
        ctx = realtime_context(vehicle_positions=feed)
        some_step.apply(ctx)

    When ``trip_ids`` or ``stop_ids`` are omitted, defaults are derived from the
    vehicle ID: ``trip_id`` becomes ``"trip_{vehicle_id}"`` and ``stop_id``
    becomes ``"S0"``.

    Args:
        vehicle_ids: One entry per vehicle; each becomes a separate entity.
        trip_ids: Optional; zipped with ``vehicle_ids``. Defaults to
            ``"trip_{vehicle_id}"`` per vehicle when omitted.
        stop_ids: Optional; zipped with ``vehicle_ids``. Defaults to ``"S0"``
            per vehicle when omitted.

    Returns:
        A populated :class:`~google.transit.gtfs_realtime_pb2.FeedMessage`.
    """
    resolved_trip_ids = (
        trip_ids if trip_ids is not None else [f"trip_{v}" for v in vehicle_ids]
    )
    resolved_stop_ids = stop_ids if stop_ids is not None else ["S0"] * len(vehicle_ids)

    feed = gtfs_rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_712_345_678
    for vehicle_id, trip_id, stop_id in zip(
        vehicle_ids, resolved_trip_ids, resolved_stop_ids, strict=True
    ):
        entity = feed.entity.add()
        entity.id = vehicle_id
        vp = entity.vehicle
        vp.trip.trip_id = trip_id
        vp.vehicle.id = vehicle_id
        vp.stop_id = stop_id
    return feed


def assert_unchanged(
    before: pl.DataFrame,
    after: pl.DataFrame,
    *,
    excluding: pl.Expr,
) -> None:
    """Assert that rows *not* matched by ``excluding`` are unchanged.

    Checks that ``before`` and ``after`` are identical for all rows outside
    ``excluding``. Useful for verifying that a transform modifies only the rows it was
    intended to touch. The complement check — that unintended rows are
    untouched — is easy to miss when writing assertions, and this helper
    makes it explicit::

        before = ctx.output["stops.txt"].clone()
        remove_expansion_stops.apply(ctx)
        assert_unchanged(
            before,
            ctx.output["stops.txt"],
            excluding=pl.col("stop_id").is_in(["E01", "E07"]),
        )

    Both frames are filtered to ``~excluding`` and sorted by all columns before
    comparison, so row-order differences within the unchanged set do not produce
    false failures.

    Raises :exc:`AssertionError` with a human-readable message on failure. The
    message includes the before/after row counts and a sample of up to 5 changed
    rows when the counts match but values differ.

    **Precondition:** ``before`` and ``after`` must share the same schema
    (same columns and types). Steps that add or remove columns are not
    compatible with this helper for the affected columns — use direct polars
    assertions instead.

    Args:
        before: Snapshot of the DataFrame *before* the step ran (use ``.clone()``
            to capture it before calling ``step.apply(ctx)``).
        after: The DataFrame from ``ctx.output`` *after* the step ran.
        excluding: A polars expression that identifies the rows the step was
            *expected* to modify. All other rows are asserted unchanged.

    Raises:
        AssertionError: If any row not matched by ``excluding`` differs between
            ``before`` and ``after``.
    """
    unchanged_before = before.filter(~excluding).sort(before.columns)
    unchanged_after = after.filter(~excluding).sort(after.columns)

    if unchanged_before.equals(unchanged_after):
        return

    n_before = len(unchanged_before)
    n_after = len(unchanged_after)

    if n_before != n_after:
        raise AssertionError(
            f"Unchanged-row count changed: {n_before} → {n_after}\n"
            f"  before: {len(before)} total, {n_before} after ~excluding\n"
            f"  after:  {len(after)} total, {n_after} after ~excluding"
        )

    # Same count but values differ — find the first few changed rows.
    diff_lines: list[str] = []
    for i in range(min(n_before, 5)):
        r_before = unchanged_before.row(i, named=True)
        r_after = unchanged_after.row(i, named=True)
        if r_before != r_after:
            diff_lines.append(
                f"  row {i}:\n    before: {r_before!r}\n    after:  {r_after!r}"
            )

    if diff_lines:
        raise AssertionError(
            f"Unchanged rows were modified ({len(diff_lines)} sample(s) shown):\n"
            + "\n".join(diff_lines)
        )
    # Fallback: frames differ but we couldn't pinpoint a row in the first 5.
    raise AssertionError(
        f"Unchanged rows differ (first 5 sorted rows match, but frames are not equal; "
        f"total unchanged rows: {n_before})"
    )
