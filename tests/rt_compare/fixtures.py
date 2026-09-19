"""Synthetic GTFS-RT test feed generators for all 10 test fixtures."""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2 as gtfs_rt

BASE_TIMESTAMP = 1711000000


def _make_header(timestamp: int = BASE_TIMESTAMP) -> gtfs_rt.FeedHeader:
    header = gtfs_rt.FeedHeader()
    header.gtfs_realtime_version = "2.0"
    header.incrementality = gtfs_rt.FeedHeader.FULL_DATASET
    header.timestamp = timestamp
    return header


def _make_vehicle_entity(
    entity_id: str,
    trip_id: str,
    route_id: str,
    lat: float,
    lon: float,
    bearing: float = 90.0,
    speed: float = 10.0,
    timestamp: int = BASE_TIMESTAMP,
    congestion_level: int | None = None,
) -> gtfs_rt.FeedEntity:
    entity = gtfs_rt.FeedEntity()
    entity.id = entity_id

    vp = entity.vehicle
    vp.trip.trip_id = trip_id
    vp.trip.route_id = route_id
    vp.position.latitude = lat
    vp.position.longitude = lon
    vp.position.bearing = bearing
    vp.position.speed = speed
    vp.timestamp = timestamp
    vp.vehicle.id = entity_id
    vp.vehicle.label = f"Vehicle {entity_id}"

    if congestion_level is not None:
        vp.congestion_level = congestion_level

    return entity


def _make_trip_update_entity(
    entity_id: str,
    trip_id: str,
    route_id: str,
    stop_updates: list[tuple[int, int, int]] | None = None,
    timestamp: int = BASE_TIMESTAMP,
) -> gtfs_rt.FeedEntity:
    """Create a TripUpdate entity.

    stop_updates: list of (stop_sequence, arrival_delay, departure_delay)
    """
    entity = gtfs_rt.FeedEntity()
    entity.id = entity_id

    tu = entity.trip_update
    tu.trip.trip_id = trip_id
    tu.trip.route_id = route_id
    tu.timestamp = timestamp

    if stop_updates:
        for seq, arr_delay, dep_delay in stop_updates:
            stu = tu.stop_time_update.add()
            stu.stop_sequence = seq
            stu.stop_id = f"stop_{seq}"
            stu.arrival.delay = arr_delay
            stu.departure.delay = dep_delay

    return entity


def _make_alert_entity(
    entity_id: str,
    header_text: str,
    description_text: str,
    informed_entities: list[tuple[str, str, str, str]] | None = None,
    active_periods: list[tuple[int, int]] | None = None,
) -> gtfs_rt.FeedEntity:
    """Create an Alert entity.

    informed_entities: list of (agency_id, route_id, trip_id, stop_id)
    active_periods: list of (start, end) timestamps
    """
    entity = gtfs_rt.FeedEntity()
    entity.id = entity_id

    alert = entity.alert
    ts = alert.header_text.translation.add()
    ts.language = "en"
    ts.text = header_text
    ds = alert.description_text.translation.add()
    ds.language = "en"
    ds.text = description_text

    if informed_entities:
        for agency_id, route_id, trip_id, stop_id in informed_entities:
            ie = alert.informed_entity.add()
            if agency_id:
                ie.agency_id = agency_id
            if route_id:
                ie.route_id = route_id
            if trip_id:
                ie.trip.trip_id = trip_id
            if stop_id:
                ie.stop_id = stop_id

    if active_periods:
        for start, end in active_periods:
            ap = alert.active_period.add()
            ap.start = start
            ap.end = end

    return entity


def _base_vehicle_entities() -> list[gtfs_rt.FeedEntity]:
    """Create a set of base vehicle position entities."""
    return [
        _make_vehicle_entity("v1", "trip_1", "route_A", 47.6062, -122.3321),
        _make_vehicle_entity("v2", "trip_2", "route_B", 47.6101, -122.3420),
        _make_vehicle_entity("v3", "trip_3", "route_A", 47.6205, -122.3493),
    ]


# ── Fixture 1: identical-reordered ──────────────────────────────────────


def make_identical_reordered() -> tuple[bytes, bytes]:
    """Same entities, different order. Should be Level 2 equivalent."""
    entities = _base_vehicle_entities()

    feed_a = gtfs_rt.FeedMessage()
    feed_a.header.CopyFrom(_make_header())
    for e in entities:
        feed_a.entity.append(e)

    feed_b = gtfs_rt.FeedMessage()
    feed_b.header.CopyFrom(_make_header())
    # Reverse order
    for e in reversed(entities):
        feed_b.entity.append(e)

    return feed_a.SerializeToString(), feed_b.SerializeToString()


# ── Fixture 2: identical-timestamps ─────────────────────────────────────


def make_identical_timestamps() -> tuple[bytes, bytes]:
    """Same entities, different header.timestamp. Should be Level 2 equivalent."""
    entities = _base_vehicle_entities()

    feed_a = gtfs_rt.FeedMessage()
    feed_a.header.CopyFrom(_make_header(BASE_TIMESTAMP))
    for e in entities:
        feed_a.entity.append(e)

    feed_b = gtfs_rt.FeedMessage()
    feed_b.header.CopyFrom(_make_header(BASE_TIMESTAMP + 5))
    for e in entities:
        feed_b.entity.append(e)

    return feed_a.SerializeToString(), feed_b.SerializeToString()


# ── Fixture 3: added-entity ─────────────────────────────────────────────


def make_added_entity() -> tuple[bytes, bytes]:
    """Feed B has one extra VehiclePosition. Should report 1 added."""
    entities = _base_vehicle_entities()

    feed_a = gtfs_rt.FeedMessage()
    feed_a.header.CopyFrom(_make_header())
    for e in entities:
        feed_a.entity.append(e)

    feed_b = gtfs_rt.FeedMessage()
    feed_b.header.CopyFrom(_make_header())
    for e in entities:
        feed_b.entity.append(e)
    # Add extra entity
    extra = _make_vehicle_entity("v4", "trip_4", "route_C", 47.6500, -122.3100)
    feed_b.entity.append(extra)

    return feed_a.SerializeToString(), feed_b.SerializeToString()


# ── Fixture 4: removed-entity ───────────────────────────────────────────


def make_removed_entity() -> tuple[bytes, bytes]:
    """Feed B missing one TripUpdate. Should report 1 removed."""
    entities_a = [
        _make_trip_update_entity("t1", "trip_1", "route_A", [(1, 30, 30), (2, 60, 60)]),
        _make_trip_update_entity("t2", "trip_2", "route_B", [(1, 0, 0), (2, 15, 15)]),
        _make_trip_update_entity("t3", "trip_3", "route_A", [(1, -10, -10)]),
    ]

    feed_a = gtfs_rt.FeedMessage()
    feed_a.header.CopyFrom(_make_header())
    for e in entities_a:
        feed_a.entity.append(e)

    feed_b = gtfs_rt.FeedMessage()
    feed_b.header.CopyFrom(_make_header())
    # Only include first two
    for e in entities_a[:2]:
        feed_b.entity.append(e)

    return feed_a.SerializeToString(), feed_b.SerializeToString()


# ── Fixture 5: modified-position ────────────────────────────────────────


def make_modified_position() -> tuple[bytes, bytes]:
    """One vehicle's lat/lon differs by 0.0001 deg. Should report 1 modified."""
    feed_a = gtfs_rt.FeedMessage()
    feed_a.header.CopyFrom(_make_header())
    for e in _base_vehicle_entities():
        feed_a.entity.append(e)

    feed_b = gtfs_rt.FeedMessage()
    feed_b.header.CopyFrom(_make_header())
    entities_b = _base_vehicle_entities()
    # Modify v2's position by 0.0001 degrees (significant at 5 decimal places)
    entities_b[1].vehicle.position.latitude = 47.6102  # was 47.6101
    for e in entities_b:
        feed_b.entity.append(e)

    return feed_a.SerializeToString(), feed_b.SerializeToString()


# ── Fixture 6: modified-position-noise ──────────────────────────────────


def make_modified_position_noise() -> tuple[bytes, bytes]:
    """One vehicle's lat/lon differs by 0.000001 deg. Equivalent at 5 dp."""
    feed_a = gtfs_rt.FeedMessage()
    feed_a.header.CopyFrom(_make_header())
    for e in _base_vehicle_entities():
        feed_a.entity.append(e)

    feed_b = gtfs_rt.FeedMessage()
    feed_b.header.CopyFrom(_make_header())
    entities_b = _base_vehicle_entities()
    # Modify v2's position by 0.000003 degrees — produces different float32 bytes
    # but rounds to the same value at 5 decimal places
    entities_b[1].vehicle.position.latitude = 47.610100 + 0.000003
    for e in entities_b:
        feed_b.entity.append(e)

    return feed_a.SerializeToString(), feed_b.SerializeToString()


# ── Fixture 7: stale-entity ─────────────────────────────────────────────


def make_stale_entity() -> tuple[bytes, bytes]:
    """Feed A has entity with timestamp 10 minutes old."""
    stale_ts = BASE_TIMESTAMP - 600  # 10 minutes old

    feed_a = gtfs_rt.FeedMessage()
    feed_a.header.CopyFrom(_make_header())
    feed_a.entity.append(
        _make_vehicle_entity(
            "v1", "trip_1", "route_A", 47.6062, -122.3321, timestamp=stale_ts
        )
    )
    feed_a.entity.append(
        _make_vehicle_entity("v2", "trip_2", "route_B", 47.6101, -122.3420)
    )

    feed_b = gtfs_rt.FeedMessage()
    feed_b.header.CopyFrom(_make_header())
    # Feed B only has the non-stale entity
    feed_b.entity.append(
        _make_vehicle_entity("v2", "trip_2", "route_B", 47.6101, -122.3420)
    )

    return feed_a.SerializeToString(), feed_b.SerializeToString()


# ── Fixture 8: stop-time-update-reorder ─────────────────────────────────


def make_stop_time_update_reorder() -> tuple[bytes, bytes]:
    """Same TripUpdate, stop_time_updates in different order. Level 2 equivalent."""
    feed_a = gtfs_rt.FeedMessage()
    feed_a.header.CopyFrom(_make_header())
    feed_a.entity.append(
        _make_trip_update_entity(
            "t1", "trip_1", "route_A", [(1, 30, 30), (2, 60, 60), (3, 90, 90)]
        )
    )

    # Build feed_b with reversed stop_time_update order
    feed_b = gtfs_rt.FeedMessage()
    feed_b.header.CopyFrom(_make_header())
    feed_b.entity.append(
        _make_trip_update_entity(
            "t1", "trip_1", "route_A", [(3, 90, 90), (2, 60, 60), (1, 30, 30)]
        )
    )

    return feed_a.SerializeToString(), feed_b.SerializeToString()


# ── Fixture 9: default-value-presence ───────────────────────────────────


def make_default_value_presence() -> tuple[bytes, bytes]:
    """One feed has congestion_level: 0 explicitly, other omits it. Level 2."""
    feed_a = gtfs_rt.FeedMessage()
    feed_a.header.CopyFrom(_make_header())
    # Explicitly set congestion_level to 0 (UNKNOWN_CONGESTION_LEVEL)
    feed_a.entity.append(
        _make_vehicle_entity(
            "v1", "trip_1", "route_A", 47.6062, -122.3321, congestion_level=0
        )
    )

    feed_b = gtfs_rt.FeedMessage()
    feed_b.header.CopyFrom(_make_header())
    # Don't set congestion_level (defaults to 0)
    feed_b.entity.append(
        _make_vehicle_entity("v1", "trip_1", "route_A", 47.6062, -122.3321)
    )

    return feed_a.SerializeToString(), feed_b.SerializeToString()


# ── Fixture 10: alert-time-overlap ──────────────────────────────────────


def make_alert_time_overlap() -> tuple[bytes, bytes]:
    """Alerts with same content but active_period expressed differently."""
    feed_a = gtfs_rt.FeedMessage()
    feed_a.header.CopyFrom(_make_header())
    feed_a.entity.append(
        _make_alert_entity(
            "a1",
            "Service Alert",
            "Route A delayed",
            informed_entities=[("DMO", "route_A", "", "")],
            active_periods=[(BASE_TIMESTAMP, BASE_TIMESTAMP + 3600)],
        )
    )

    feed_b = gtfs_rt.FeedMessage()
    feed_b.header.CopyFrom(_make_header())
    # Different active period (split into two adjacent periods)
    feed_b.entity.append(
        _make_alert_entity(
            "a1",
            "Service Alert",
            "Route A delayed",
            informed_entities=[("DMO", "route_A", "", "")],
            active_periods=[
                (BASE_TIMESTAMP, BASE_TIMESTAMP + 1800),
                (BASE_TIMESTAMP + 1800, BASE_TIMESTAMP + 3600),
            ],
        )
    )

    return feed_a.SerializeToString(), feed_b.SerializeToString()


# ── All fixtures registry ───────────────────────────────────────────────

ALL_FIXTURES = {
    "identical-reordered": make_identical_reordered,
    "identical-timestamps": make_identical_timestamps,
    "added-entity": make_added_entity,
    "removed-entity": make_removed_entity,
    "modified-position": make_modified_position,
    "modified-position-noise": make_modified_position_noise,
    "stale-entity": make_stale_entity,
    "stop-time-update-reorder": make_stop_time_update_reorder,
    "default-value-presence": make_default_value_presence,
    "alert-time-overlap": make_alert_time_overlap,
}
