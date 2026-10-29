"""Realtime transform builtins operating on GTFS-RT FeedMessage protobuf.

Each builtin iterates every FeedMessage in ctx.output — RT pipelines
emit one output per feed type (vehicle_positions, trip_updates,
service_alerts) so a transform applies to each in turn. Transforms
use HasField() to check which oneof variant (trip_update, vehicle,
alert) is set on each entity — entities of the wrong type become
no-ops in a given iteration, which is correct: a RenameVehicles
step run against a trip_updates output simply has no VehiclePosition
entities to touch.

This replaces the older single-combined-FeedMessage model where the
worker merged all inputs into ctx.datasets["feed"] and transforms
used HasField() as entity-type discrimination inside that mixed
feed. See realtime-pipeline.md §Per-Feed Outputs.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

from google.transit import gtfs_realtime_pb2 as gtfs_rt

from ...schedule_helpers import (
    active_trips as _active_trips,
)
from ...schedule_helpers import (
    agency_timezone,
    service_day_start,
    trip_durations,
)
from ...step import Step

if TYPE_CHECKING:
    import polars as pl

    from ...context import PipelineContext


def _trip_id_set(feed: gtfs_rt.FeedMessage) -> set[str]:
    """Trip IDs referenced by trip_update or vehicle entities in `feed`."""
    out: set[str] = set()
    for e in feed.entity:
        if e.HasField("trip_update") and e.trip_update.trip.trip_id:
            out.add(e.trip_update.trip.trip_id)
        if e.HasField("vehicle") and e.vehicle.trip.trip_id:
            out.add(e.vehicle.trip.trip_id)
    return out


def _rt_feeds(ctx: PipelineContext) -> Iterator[tuple[str, gtfs_rt.FeedMessage]]:
    """Yield (name, feed) for every FeedMessage in ctx.output.

    Non-FeedMessage entries are skipped so transforms don't accidentally
    trip over schedule outputs or other non-RT values sharing the
    output dict.
    """
    for name, value in ctx.output.items():
        if isinstance(value, gtfs_rt.FeedMessage):
            yield name, value


class PassThrough(Step):
    """Do nothing; a baseline for equivalence tests and measurement.

    Args:
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Examples:
        ```python
        passthrough = PassThrough()
        ```
    """

    category = "Testing"

    description = "Pass-through (no-op)"

    def apply(self, ctx: PipelineContext) -> None:
        pass


class FilterStopsByID(Step):
    """Drop references to blocklisted stops from trip updates and vehicle positions.

    On a `TripUpdate`, the `stop_time_update` entries whose `stop_id` is
    blocked are removed. On a `VehiclePosition` at a blocked stop,
    `stop_id`, `current_stop_sequence` and `current_status` are cleared.
    Alerts are left alone. Use it to hide stations that are not yet open
    during simulated service.

    Args:
        blocked_stop_ids: Stop ids to block, matched exactly.
        blocked_stop_id_regex: A pattern a stop id must contain a match
            for to be blocked (Python `re.search`; anchor with `^` and `$`
            to match the whole id). Give one of the two or both; they
            union.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Examples:
        ```python
        filter_stops = FilterStopsByID(blocked_stop_ids={"STN_8", "STN_9"})

        filter_expansion = FilterStopsByID(blocked_stop_id_regex=r"^EXT_")
        ```
    """

    category = "Filtering"

    def __init__(
        self,
        blocked_stop_ids: set[str] | None = None,
        *,
        blocked_stop_id_regex: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        if blocked_stop_ids is None and blocked_stop_id_regex is None:
            raise ValueError(
                "FilterStopsByID needs blocked_stop_ids or blocked_stop_id_regex"
            )
        self._blocked: frozenset[str] = frozenset(blocked_stop_ids or ())
        self._regex: re.Pattern[str] | None = (
            re.compile(blocked_stop_id_regex) if blocked_stop_id_regex else None
        )
        bits = []
        if self._blocked:
            bits.append(f"{len(self._blocked)} blocked stops")
        if self._regex is not None:
            bits.append(f"regex {blocked_stop_id_regex!r}")
        self.description = f"Filter {' + '.join(bits)}"

    def _matches(self, stop_id: str) -> bool:
        if stop_id in self._blocked:
            return True
        if self._regex is not None and self._regex.search(stop_id):
            return True
        return False

    def apply(self, ctx: PipelineContext) -> None:
        for _, feed in _rt_feeds(ctx):
            for entity in feed.entity:
                if entity.HasField("trip_update"):
                    tu = entity.trip_update
                    filtered = [
                        stu
                        for stu in tu.stop_time_update
                        if not self._matches(stu.stop_id)
                    ]
                    if len(filtered) != len(tu.stop_time_update):
                        del tu.stop_time_update[:]
                        tu.stop_time_update.extend(filtered)

                if entity.HasField("vehicle"):
                    vp = entity.vehicle
                    if self._matches(vp.stop_id):
                        vp.ClearField("stop_id")
                        vp.ClearField("current_stop_sequence")
                        vp.ClearField("current_status")


class CombineFeeds(Step):
    """Merge entities from additional realtime inputs into one output feed.

    Entities are added by id; an id already present in the target wins, so
    the primary feed takes precedence over the sources. Merging is within
    one feed type (two vehicle-positions sources into one vehicle-positions
    output); realtime outputs are per feed type, so there is no combined
    feed to merge across types.

    Args:
        target: The output feed to merge into (a `ctx.output` key such as
            `"vehicle_positions"`). An absent target is seeded as an empty
            feed, so this step can also act as the init step for an output
            that exists only as inputs.
        sources: Input names (`ctx.inputs` keys, as declared in the
            pipeline's `INPUTS` manifest) whose entities are merged in. An
            unsupplied or non-realtime input is skipped.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Examples:
        ```python
        combine = CombineFeeds(
            target="vehicle_positions",
            sources=["vendor_b_positions", "vendor_c_positions"],
        )
        ```
    """

    category = "Merging"

    def __init__(
        self,
        *,
        target: str,
        sources: list[str],
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.files = [target] + sources
        self.description = (
            f"Combine {len(sources)} input(s) into ctx.output[{target!r}]"
        )
        self._target = target
        self._sources = sources

    def apply(self, ctx: PipelineContext) -> None:
        feed = ctx.output.get(self._target)
        if not isinstance(feed, gtfs_rt.FeedMessage):
            # Seed an empty FeedMessage if the target hasn't been populated
            # yet — lets CombineFeeds act as an init step when the target
            # only exists as inputs.
            feed = gtfs_rt.FeedMessage()
            feed.header.gtfs_realtime_version = "2.0"
            ctx.output[self._target] = feed

        existing_ids = {e.id for e in feed.entity}
        for name in self._sources:
            source = ctx.inputs.get(name)
            if not isinstance(source, gtfs_rt.FeedMessage):
                continue
            for entity in source.entity:
                if entity.id not in existing_ids:
                    new_entity = feed.entity.add()
                    new_entity.CopyFrom(entity)
                    existing_ids.add(entity.id)


class RenameVehicles(Step):
    """Prefix vehicle ids and labels with a string.

    Applies to entities with a `vehicle.vehicle` descriptor. Idempotent: an
    id or label that already starts with the prefix is left alone. Use it
    to namespace vehicles when combining feeds from several vendors.

    Args:
        id_prefix: The prefix to add, such as `"AGENCY-"`.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Examples:
        ```python
        rename = RenameVehicles(id_prefix="AGENCY-")
        ```
    """

    category = "Identifiers"

    def __init__(self, id_prefix: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.description = f"Rename vehicles with prefix '{id_prefix}'"
        self._prefix = id_prefix

    def apply(self, ctx: PipelineContext) -> None:
        for _, feed in _rt_feeds(ctx):
            for entity in feed.entity:
                if entity.HasField("vehicle") and entity.vehicle.HasField("vehicle"):
                    vid = entity.vehicle.vehicle
                    if vid.id and not vid.id.startswith(self._prefix):
                        vid.id = f"{self._prefix}{vid.id}"
                    if vid.label and not vid.label.startswith(self._prefix):
                        vid.label = f"{self._prefix}{vid.label}"


class UpdateFeedHeader(Step):
    """Set the GTFS-RT version on every output feed's header.

    Args:
        version: The `gtfs_realtime_version` to write.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Examples:
        ```python
        header = UpdateFeedHeader(version="2.0")
        ```
    """

    category = "Feed header"

    description = "Update feed header metadata"

    def __init__(self, version: str = "2.0", **kwargs: Any):
        super().__init__(**kwargs)
        self._version = version

    def apply(self, ctx: PipelineContext) -> None:
        for _, feed in _rt_feeds(ctx):
            feed.header.gtfs_realtime_version = self._version


class TransformTripId(Step):
    """Rewrite trip ids on trip updates and vehicle positions with a regex substitution.

    Applies to `trip_update.trip.trip_id` and `vehicle.trip.trip_id` on
    every output feed. Alert informed entities are not rewritten. Mirrors
    the schedule
    [`TransformTripId`][continuous_gtfs.builtins.schedule.TransformTripId]
    so an agency can rewrite trip ids consistently across both outputs.

    Args:
        pattern: The regex to find (Python `re` syntax).
        replacement: The replacement text; capture groups are `\\1`, `\\2`.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Examples:
        ```python
        transform_trip = TransformTripId(
            pattern=r"^LONG_PREFIX_(\\d+)$",
            replacement=r"T-\\1",
        )
        ```
    """

    category = "Identifiers"

    def __init__(self, pattern: str, replacement: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.description = f"Transform trip_id: s/{pattern}/{replacement}/"
        self._pattern = re.compile(pattern)
        self._replacement = replacement

    def apply(self, ctx: PipelineContext) -> None:
        for _, feed in _rt_feeds(ctx):
            for entity in feed.entity:
                if entity.HasField("trip_update"):
                    trip = entity.trip_update.trip
                    if trip.trip_id:
                        trip.trip_id = self._pattern.sub(
                            self._replacement, trip.trip_id
                        )

                if entity.HasField("vehicle"):
                    trip = entity.vehicle.trip
                    if trip.trip_id:
                        trip.trip_id = self._pattern.sub(
                            self._replacement, trip.trip_id
                        )

                if entity.HasField("alert"):
                    for ie in entity.alert.informed_entity:
                        if ie.HasField("trip") and ie.trip.trip_id:
                            ie.trip.trip_id = self._pattern.sub(
                                self._replacement, ie.trip.trip_id
                            )


# ============================================================
# Schedule-dependent transforms
# ------------------------------------------------------------
# All three read `ctx.inputs[<schedule_input>]` (default: "schedule")
# as a parsed `gtfs_schedule_zip` (dict of filename → polars.DataFrame).
# Pipelines using these builtins declare the schedule input in their
# INPUTS manifest and bind it to the schedule pipeline's output asset.
# When the schedule input is absent, each transform degrades gracefully
# to a no-op so a misconfigured pipeline still runs (validation
# surfaces the binding error elsewhere).
#
# Time handling: the two cancellation builtins resolve "now relative to
# service-day start" via `agency.txt`'s `agency_timezone` field. The
# service-day boundary follows the GTFS convention of local midnight,
# with an early-morning grace window (default 04:00) treating the small
# hours of the calendar morning as the previous service day. Tests and
# operators can inject an explicit `now: datetime` for determinism; the
# default is the context clock `ctx.now` — the run's evaluation moment —
# never the system clock (specs/transform-framework.md §PipelineContext).
# ============================================================


def _now_seconds_for_schedule(
    schedule: Mapping[str, pl.DataFrame],
    now: datetime,
    threshold_hour: int,
) -> tuple[int, datetime, datetime]:
    """Resolve the agency-tz "now" → (seconds_into_service_day, service_start, now_tz).

    `now` is interpreted in the agency's local timezone (read from
    `agency.txt`); callers pass any tz-aware datetime — the step's injected
    `now`, else `ctx.now`. `service_start` is the service-day-start in the agency
    timezone; `now_tz` is `now` converted to that timezone.
    """
    tz = agency_timezone(schedule)
    if now.tzinfo is None:
        raise ValueError(
            "InsertMissingCancellations / ExpireCancelledTrips: "
            "`now` must be a tz-aware datetime"
        )
    else:
        now_tz = now.astimezone(tz)
    service_start = service_day_start(now_tz, threshold_hour=threshold_hour)
    seconds_into_day = int((now_tz - service_start).total_seconds())
    return seconds_into_day, service_start, now_tz


class ExpireCancelledTrips(Step):
    """Drop cancellations whose trip has already ended per the schedule.

    A `CANCELED` entity in the target feed is kept only while its trip's
    scheduled end has not passed; stale cancellations are dropped. Time is
    resolved in the agency's local timezone from `agency.txt`. This is the
    stateless half of the "preserve cancelled trips" contract; see
    `previous_output_input` for the rest.

    Args:
        target: The feed to operate on. Cancellations are a trip-updates
            concept, so other feeds are never touched.
        schedule_input: The `gtfs_schedule_zip` input the trip times are
            read from, as declared in the pipeline's `INPUTS` manifest.
        previous_output_input: Name of an input bound to this pipeline's
            own previous output. When set, `CANCELED` entities from it
            that are still within their scheduled window and are not
            referenced in the current input are carried forward before
            the stale pass runs, so a cancellation survives upstream
            dropping it. The orchestrator resolves "latest at run start",
            so the first run sees an empty previous output.
        service_day_threshold_hour: Local hour at which the service day
            rolls over; trips before it belong to the previous day.
        now: The evaluation moment. Defaults to `ctx.now`; set only in
            tests.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Examples:
        ```python
        expire_cancellations = ExpireCancelledTrips()

        # Carry in-progress cancellations across runs
        expire_cancellations = ExpireCancelledTrips(
            previous_output_input="previous_trip_updates",
        )
        ```
    """

    category = "Schedule-aware"

    description = "Drop stale cancellations; optionally carry forward in-progress ones"

    def __init__(
        self,
        *,
        target: str = "trip_updates",
        schedule_input: str = "schedule",
        previous_output_input: str | None = None,
        service_day_threshold_hour: int = 4,
        now: datetime | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._target = target
        self._schedule_input = schedule_input
        self._previous_output_input = previous_output_input
        self._threshold_hour = service_day_threshold_hour
        self._now = now

    def apply(self, ctx: PipelineContext) -> None:
        # Both steps operate on a single target feed (default trip_updates),
        # mirroring InsertMissingCancellations. Cancellations are a
        # trip_updates concept, so this must NOT fan out across every feed in
        # ctx.output — doing so injects trip_update-shaped entities into
        # vehicle_positions (and any other feed). See issue #101.
        feed = ctx.output.get(self._target)
        if not isinstance(feed, gtfs_rt.FeedMessage):
            return
        schedule = ctx.inputs.get(self._schedule_input)
        if not isinstance(schedule, Mapping):
            return
        durations = trip_durations(schedule)
        if not durations:
            return
        now_s, _, _ = _now_seconds_for_schedule(
            schedule, self._now or ctx.now, self._threshold_hour
        )

        # Step 1: optionally merge in CANCELED entities from previous output.
        if self._previous_output_input is not None:
            prev = ctx.inputs.get(self._previous_output_input)
            if isinstance(prev, gtfs_rt.FeedMessage):
                present = _trip_id_set(feed)
                for prev_entity in prev.entity:
                    if not prev_entity.HasField("trip_update"):
                        continue
                    prev_trip = prev_entity.trip_update.trip
                    if (
                        prev_trip.schedule_relationship
                        != gtfs_rt.TripDescriptor.CANCELED
                    ):
                        continue
                    if not prev_trip.trip_id or prev_trip.trip_id in present:
                        continue
                    duration = durations.get(prev_trip.trip_id)
                    if duration is None or duration[1] < now_s:
                        continue  # trip is over → don't resurrect
                    new_entity = feed.entity.add()
                    new_entity.CopyFrom(prev_entity)
                    present.add(prev_trip.trip_id)

        # Step 2: expire-stale pass over the (possibly merged) feed. The
        # HasField("trip_update") guard is redundant defense now that the feed
        # is scoped to the target, but kept so the pass stays correct if a
        # non-trip_updates target ever carries mixed entities.
        kept = []
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                kept.append(entity)
                continue
            trip = entity.trip_update.trip
            if trip.schedule_relationship != gtfs_rt.TripDescriptor.CANCELED:
                kept.append(entity)
                continue
            duration = durations.get(trip.trip_id)
            if duration is None or duration[1] >= now_s:
                kept.append(entity)
            # else: scheduled end has passed → drop stale cancellation
        if len(kept) != len(feed.entity):
            del feed.entity[:]
            feed.entity.extend(kept)


class InsertMissingCancellations(Step):
    """Cancel scheduled trips that should be running but are absent from the feed.

    For every trip whose service is active today per `calendar.txt` and
    `calendar_dates.txt`: if its scheduled start passed at least
    `delay_seconds` ago, its scheduled end is still ahead, and no entity in
    the target feed references it (on `trip_update.trip.trip_id` or
    `vehicle.trip.trip_id`), a `CANCELED` trip update is appended with
    `trip_id`, `route_id`, `start_date` and `schedule_relationship`, under
    entity id `cancel_<trip_id>`. Time is resolved in the agency's local
    timezone from `agency.txt`.

    Args:
        target: The feed to operate on.
        schedule_input: The `gtfs_schedule_zip` input the trips are read
            from, as declared in the pipeline's `INPUTS` manifest.
        delay_seconds: How long after its scheduled start a missing trip
            is tolerated before it is cancelled.
        service_day_threshold_hour: Local hour at which the service day
            rolls over.
        now: The evaluation moment. Defaults to `ctx.now`; set only in
            tests.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Examples:
        ```python
        insert_cancellations = InsertMissingCancellations(delay_seconds=600)
        ```
    """

    category = "Schedule-aware"

    description = "Insert cancellations for scheduled-but-missing trips"

    def __init__(
        self,
        *,
        target: str = "trip_updates",
        schedule_input: str = "schedule",
        delay_seconds: int = 600,
        service_day_threshold_hour: int = 4,
        now: datetime | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._target = target
        self._schedule_input = schedule_input
        self._delay_seconds = delay_seconds
        self._threshold_hour = service_day_threshold_hour
        self._now = now

    def apply(self, ctx: PipelineContext) -> None:
        feed = ctx.output.get(self._target)
        if not isinstance(feed, gtfs_rt.FeedMessage):
            return
        schedule = ctx.inputs.get(self._schedule_input)
        if not isinstance(schedule, Mapping):
            return
        now_s, service_start, _ = _now_seconds_for_schedule(
            schedule, self._now or ctx.now, self._threshold_hour
        )
        threshold = now_s - self._delay_seconds

        # Restrict to trips whose service is active today; build a fast
        # trip_id → route_id map from the same DataFrame.
        active = _active_trips(schedule, service_start)
        if active.height == 0:
            return
        active_route_by_trip: dict[str, str] = {}
        if "trip_id" in active.columns and "route_id" in active.columns:
            for row in active.iter_rows(named=True):
                tid = row.get("trip_id")
                if tid:
                    active_route_by_trip[tid] = row.get("route_id") or ""
        if not active_route_by_trip:
            return

        durations = trip_durations(schedule)
        if not durations:
            return

        service_date_str = service_start.strftime("%Y%m%d")
        present = _trip_id_set(feed)
        for trip_id, route_id in active_route_by_trip.items():
            if trip_id in present:
                continue
            duration = durations.get(trip_id)
            if duration is None:
                continue
            start_s, end_s = duration
            if start_s > threshold:
                continue  # not yet past start + delay
            if end_s < now_s:
                continue  # trip already over; nothing to cancel
            entity = feed.entity.add()
            entity.id = f"cancel_{trip_id}"
            entity.trip_update.trip.trip_id = trip_id
            if route_id:
                entity.trip_update.trip.route_id = route_id
            entity.trip_update.trip.start_date = service_date_str
            entity.trip_update.trip.schedule_relationship = (
                gtfs_rt.TripDescriptor.CANCELED
            )


def _trip_descriptors(entity: gtfs_rt.FeedEntity) -> Iterator[gtfs_rt.TripDescriptor]:
    """Yield every TripDescriptor present on an entity.

    Covers both trip_update.trip and vehicle.trip.
    """
    if entity.HasField("trip_update"):
        yield entity.trip_update.trip
    if entity.HasField("vehicle"):
        yield entity.vehicle.trip


class ConvertScheduledToNew(Step):
    """Rewrite `SCHEDULED` trips as `ADDED` with fresh trip ids.

    For every entity whose `trip.schedule_relationship` is `SCHEDULED` and
    whose `trip_id` is in the schedule: the trip id is replaced with
    `<prefix><original>_<uuid8>`, the relationship becomes `ADDED`, and
    `route_id` and `direction_id` are filled from `trips.txt` when present.
    Downstream consumers then treat the trip as added rather than as a
    known scheduled trip, which is the point: it breaks the identity link
    to a schedule the agency has not published yet. `stop_time_update` is
    left as is; a pipeline that needs it filled from `stop_times.txt`
    layers its own step.

    Args:
        schedule_input: The `gtfs_schedule_zip` input the trips are read
            from, as declared in the pipeline's `INPUTS` manifest.
        trip_id_prefix: Prefix of the generated trip ids.
        route_ids: Convert only trips scheduled on these routes (per
            `route_id` in `trips.txt`); a trip on another route, or with no
            `route_id` in the schedule, stays `SCHEDULED`. `None` converts
            every scheduled trip in the schedule.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Examples:
        ```python
        unpublished_mode = ConvertScheduledToNew(route_ids={"ROUTE_A"})
        ```
    """

    category = "Schedule-aware"

    description = "Convert SCHEDULED entities to ADDED with new trip_ids"

    def __init__(
        self,
        *,
        schedule_input: str = "schedule",
        trip_id_prefix: str = "new_",
        route_ids: set[str] | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._schedule_input = schedule_input
        self._prefix = trip_id_prefix
        # Normalize to a frozenset for O(1) membership, mirroring
        # FilterStopsByID's blocked_stop_ids handling. None means "no route
        # filter" (convert every scheduled trip), distinct from an empty set.
        self._route_ids: frozenset[str] | None = (
            None if route_ids is None else frozenset(route_ids)
        )

    def apply(self, ctx: PipelineContext) -> None:
        schedule = ctx.inputs.get(self._schedule_input)
        if not isinstance(schedule, Mapping):
            return
        trips_df = schedule.get("trips.txt")
        if (
            trips_df is None
            or trips_df.height == 0
            or "trip_id" not in trips_df.columns
        ):
            return

        # Build a `trip_id -> (route_id, direction_id)` lookup by extracting
        # the columns we actually need via `.to_list()` and zipping in pure
        # Python. Much faster than `iter_rows(named=True)` which builds a
        # per-row dict — on an 18.5K-trip feed this drops from ~110ms to
        # ~5ms. None when the column is absent or the cell is empty.
        trip_ids = trips_df["trip_id"].to_list()
        route_ids = (
            trips_df["route_id"].to_list()
            if "route_id" in trips_df.columns
            else [None] * len(trip_ids)
        )
        direction_ids = (
            trips_df["direction_id"].to_list()
            if "direction_id" in trips_df.columns
            else [None] * len(trip_ids)
        )
        # When route_ids is set, restrict the lookup to trips scheduled on
        # those routes — trips off the routes never enter the lookup, so the
        # conversion loop below skips them via the `not in lookup` guard.
        lookup: dict[str, tuple[str | None, str | None]] = {}
        for tid, rid, did in zip(trip_ids, route_ids, direction_ids, strict=False):
            if not tid:
                continue
            if self._route_ids is not None and rid not in self._route_ids:
                continue
            lookup[tid] = (rid, did)
        if not lookup:
            return

        # New trip_ids are rewritten consistently across all RT outputs in
        # this pipeline run: a TripUpdate and a VehiclePosition referencing
        # the same scheduled trip end up pointing at the same new ID.
        rewrites: dict[str, str] = {}

        for _, feed in _rt_feeds(ctx):
            for entity in feed.entity:
                for descriptor in _trip_descriptors(entity):
                    if (
                        descriptor.schedule_relationship
                        != gtfs_rt.TripDescriptor.SCHEDULED
                    ):
                        continue
                    orig_trip_id = descriptor.trip_id
                    if not orig_trip_id or orig_trip_id not in lookup:
                        continue
                    new_trip_id = rewrites.get(orig_trip_id)
                    if new_trip_id is None:
                        new_trip_id = (
                            f"{self._prefix}{orig_trip_id}_{uuid.uuid4().hex[:8]}"
                        )
                        rewrites[orig_trip_id] = new_trip_id
                    route_id, direction_id = lookup[orig_trip_id]
                    descriptor.trip_id = new_trip_id
                    descriptor.schedule_relationship = gtfs_rt.TripDescriptor.ADDED
                    if route_id:
                        descriptor.route_id = route_id
                    if direction_id not in (None, ""):
                        try:
                            descriptor.direction_id = int(direction_id)
                        except (ValueError, TypeError):
                            pass
