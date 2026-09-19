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
from datetime import UTC, datetime
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
    """No-op transform for baseline measurement."""

    description = "Pass-through (no-op)"

    def apply(self, ctx: PipelineContext) -> None:
        pass


class FilterStopsByID(Step):
    """Remove stop_time_updates and vehicle stop references for blocklisted stops.

    A stop_id matches the blocklist if it's in the explicit `blocked_stop_ids`
    set OR its `blocked_stop_id_regex` finds a match (Python `re.search`
    semantics — anchor with `^` / `$` for whole-string matching). At least
    one of the two must be supplied; supplying both unions them.
    """

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
    """Merge entities from additional RT inputs into a target output feed.

    Scoped within a single feed type (e.g. merging two vehicle_positions
    sources from different vendors into one output). Across-type merging
    is deliberately not supported — RT outputs are per-feed-type, not a
    single combined FeedMessage.
    """

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
    """Prefix vehicle IDs and labels with a configurable string."""

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
    """Set GTFS-RT header version on every RT output."""

    description = "Update feed header metadata"

    def __init__(self, version: str = "2.0", **kwargs: Any):
        super().__init__(**kwargs)
        self._version = version

    def apply(self, ctx: PipelineContext) -> None:
        for _, feed in _rt_feeds(ctx):
            feed.header.gtfs_realtime_version = self._version


class TransformTripId(Step):
    """Regex substitution on trip_id across all entities in every RT output."""

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
# default reads `datetime.now(timezone.utc)`.
# ============================================================


def _now_seconds_for_schedule(
    schedule: Mapping[str, pl.DataFrame],
    now: datetime | None,
    threshold_hour: int,
) -> tuple[int, datetime, datetime]:
    """Resolve the agency-tz "now" → (seconds_into_service_day, service_start, now_tz).

    `now` is interpreted in the agency's local timezone (read from
    `agency.txt`); callers may pass any tz-aware datetime or None to use
    system time. `service_start` is the service-day-start in the agency
    timezone; `now_tz` is `now` converted to that timezone.
    """
    tz = agency_timezone(schedule)
    if now is None:
        now_tz = datetime.now(UTC).astimezone(tz)
    elif now.tzinfo is None:
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
    """Drop cancellation entities whose trip has finished per schedule.

    Cancellations currently in the input are kept *if and only if* the
    trip's scheduled end has not yet passed. Stale cancellations are
    dropped. Time is resolved in the agency's local timezone via
    `agency.txt` — see the module-level note.

    Operates on a single `target` feed (default `trip_updates`), like
    `InsertMissingCancellations`. Cancellations are a trip_updates
    concept; the step deliberately does not touch other feeds such as
    `vehicle_positions`.

    Optional cross-run preservation
    -------------------------------
    By itself this builtin is the stateless half of a "preserve
    cancelled trips" contract. The full intent —
    keeping a cancellation in output across runs even after upstream
    drops it — additionally requires reading the prior run's output.

    Set `previous_output_input` to the name of an input slot bound to
    the pipeline's own previous output (a `gtfs_rt_protobuf` asset).
    When set, CANCELED entities from the previous output that are
    still within their scheduled service window AND aren't already
    referenced in the current input feed are merged into the working
    FeedMessage before the expire-stale pass runs. The agency wires
    that input by adding a `pipeline_slots` row pointing to its own
    output asset; the orchestrator resolves "latest at run start" so
    the first run sees an empty previous output.
    """

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
            schedule, self._now, self._threshold_hour
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
    """Emit cancellations for scheduled-but-missing trips after a grace delay.

    For every trip whose `service_id` is active
    today per `calendar.txt` / `calendar_dates.txt`, if (a) the scheduled
    start has passed by at least `delay_seconds`, (b) the scheduled end
    is still in the future, and (c) no entity in the target feed
    references the trip (checked across both `trip_update.trip.trip_id`
    and `vehicle.trip.trip_id`), append a CANCELED `TripUpdate`
    populated with `trip_id`, `route_id`, `start_date`, and
    `schedule_relationship`. Entity ID is `cancel_<trip_id>`.

    Time is resolved in the agency's local timezone via `agency.txt` —
    see the module-level note.
    """

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
            schedule, self._now, self._threshold_hour
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
    """Rewrite SCHEDULED entities to ADDED, generating new unique trip_ids.

    Supports an "unpublished mode" where SCHEDULED trips are
    republished as NEW/ADDED trips. For any entity whose
    `trip.schedule_relationship == SCHEDULED` and whose `trip_id` is
    present in the schedule, this step:

    1. Replaces `trip.trip_id` with a freshly generated unique value
       (`<prefix><original>_<uuid8>`), so downstream consumers treat
       the trip as added rather than a known scheduled trip.
    2. Sets `trip.schedule_relationship = ADDED` (GTFS-RT's
       enum-equivalent of "NEW" — the test case wording predates the
       GTFS-RT v2 enum name).
    3. Fills `route_id` and `direction_id` from the schedule's
       `trips.txt` row when available — the GTFS-RT spec recommends
       both on ADDED entities.

    Stop sequence and other recommended ADDED fields are intentionally
    left untouched in v1 — the transform's role is to break the
    consumer's identity link to the published schedule. Agencies that
    need to also fill `stop_time_update` from `stop_times.txt` can
    layer a separate step.

    Optional route scoping
    ----------------------
    Set `route_ids` to restrict conversion to trips scheduled on those
    routes (per the trip's `route_id` in `trips.txt`). Entities whose
    trip is on a route not in the set — or whose trip has no `route_id`
    in the schedule — are left as SCHEDULED. When `route_ids` is None
    (default) every scheduled trip present in the schedule is converted.
    """

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
