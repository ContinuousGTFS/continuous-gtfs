"""Tests for each equivalence level detection."""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2 as gtfs_rt

from continuous_gtfs.rt_compare.compare import compare_feeds
from continuous_gtfs.rt_compare.config import ComparisonConfig


def _make_simple_feed(
    timestamp: int = 1711000000,
    entities: list[gtfs_rt.FeedEntity] | None = None,
) -> gtfs_rt.FeedMessage:
    feed = gtfs_rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_rt.FeedHeader.FULL_DATASET
    feed.header.timestamp = timestamp
    if entities:
        for e in entities:
            feed.entity.append(e)
    return feed


def _make_vehicle_entity(
    eid: str, lat: float = 47.6, lon: float = -122.3
) -> gtfs_rt.FeedEntity:
    entity = gtfs_rt.FeedEntity()
    entity.id = eid
    entity.vehicle.trip.trip_id = f"trip_{eid}"
    entity.vehicle.position.latitude = lat
    entity.vehicle.position.longitude = lon
    entity.vehicle.timestamp = 1711000000
    entity.vehicle.vehicle.id = eid
    return entity


class TestLevel0ByteIdentical:
    """Level 0: serialized bytes match exactly."""

    def test_identical_bytes(self):
        e = _make_vehicle_entity("v1")
        feed = _make_simple_feed(entities=[e])
        data = feed.SerializeToString()
        report = compare_feeds(data, data)
        assert report.equivalence_level == 0

    def test_different_bytes_not_level_0(self):
        e = _make_vehicle_entity("v1")
        feed_a = _make_simple_feed(entities=[e])
        feed_b = _make_simple_feed(timestamp=1711000001, entities=[e])
        report = compare_feeds(feed_a.SerializeToString(), feed_b.SerializeToString())
        assert report.equivalence_level != 0


class TestLevel1StructurallyIdentical:
    """Level 1: parsed protobuf trees match field-by-field."""

    def test_same_content_same_order(self):
        e1 = _make_vehicle_entity("v1")
        e2 = _make_vehicle_entity("v2")
        feed_a = _make_simple_feed(entities=[e1, e2])
        # Serialize and re-parse to get potentially different bytes
        data = feed_a.SerializeToString()
        feed_b = gtfs_rt.FeedMessage()
        feed_b.ParseFromString(data)
        # Re-serialize may produce identical bytes, so use same bytes
        report = compare_feeds(data, feed_b.SerializeToString())
        assert report.equivalence_level <= 1


class TestLevel2SemanticallyEquivalent:
    """Level 2: same entities ignoring order, header timestamp tolerance, defaults."""

    def test_reordered_entities(self):
        e1 = _make_vehicle_entity("v1")
        e2 = _make_vehicle_entity("v2")
        feed_a = _make_simple_feed(entities=[e1, e2])
        feed_b = _make_simple_feed(entities=[e2, e1])
        report = compare_feeds(feed_a.SerializeToString(), feed_b.SerializeToString())
        assert report.equivalence_level == 2

    def test_header_timestamp_within_tolerance(self):
        e = _make_vehicle_entity("v1")
        feed_a = _make_simple_feed(timestamp=1711000000, entities=[e])
        feed_b = _make_simple_feed(timestamp=1711000010, entities=[e])
        config = ComparisonConfig(timestamp_tolerance_seconds=30)
        report = compare_feeds(
            feed_a.SerializeToString(), feed_b.SerializeToString(), config
        )
        assert report.equivalence_level == 2

    def test_header_timestamp_beyond_tolerance(self):
        e = _make_vehicle_entity("v1")
        feed_a = _make_simple_feed(timestamp=1711000000, entities=[e])
        feed_b = _make_simple_feed(timestamp=1711000060, entities=[e])
        config = ComparisonConfig(timestamp_tolerance_seconds=30)
        report = compare_feeds(
            feed_a.SerializeToString(), feed_b.SerializeToString(), config
        )
        # Beyond header tolerance, not Level 2
        assert report.equivalence_level != 2 or report.equivalence_level > 2


class TestLevel3FunctionallyEquivalent:
    """Level 3: allows entity timestamp diffs within tolerance."""

    def test_entity_timestamp_within_tolerance(self):
        e1_a = gtfs_rt.FeedEntity()
        e1_a.id = "v1"
        e1_a.vehicle.trip.trip_id = "trip_1"
        e1_a.vehicle.position.latitude = 47.6
        e1_a.vehicle.position.longitude = -122.3
        e1_a.vehicle.timestamp = 1711000000
        e1_a.vehicle.vehicle.id = "v1"

        e1_b = gtfs_rt.FeedEntity()
        e1_b.id = "v1"
        e1_b.vehicle.trip.trip_id = "trip_1"
        e1_b.vehicle.position.latitude = 47.6
        e1_b.vehicle.position.longitude = -122.3
        e1_b.vehicle.timestamp = 1711000010  # 10s later
        e1_b.vehicle.vehicle.id = "v1"

        feed_a = _make_simple_feed(timestamp=1711000000, entities=[e1_a])
        feed_b = _make_simple_feed(timestamp=1711000050, entities=[e1_b])

        config = ComparisonConfig(
            timestamp_tolerance_seconds=30,
            entity_timestamp_tolerance_seconds=30,
        )
        report = compare_feeds(
            feed_a.SerializeToString(), feed_b.SerializeToString(), config
        )
        # Header timestamp exceeds tolerance (50s > 30s), but entity timestamp
        # is within tolerance. Should be Level 3 (functionally equivalent).
        assert report.equivalence_level == 3


class TestNotEquivalent:
    """Feeds that are not equivalent at any level."""

    def test_added_entity(self):
        e1 = _make_vehicle_entity("v1")
        e2 = _make_vehicle_entity("v2")
        feed_a = _make_simple_feed(entities=[e1])
        feed_b = _make_simple_feed(entities=[e1, e2])
        report = compare_feeds(feed_a.SerializeToString(), feed_b.SerializeToString())
        assert report.equivalence_level == -1

    def test_modified_position(self):
        e_a = _make_vehicle_entity("v1", lat=47.6)
        e_b = _make_vehicle_entity("v1", lat=48.0)
        feed_a = _make_simple_feed(entities=[e_a])
        feed_b = _make_simple_feed(entities=[e_b])
        report = compare_feeds(feed_a.SerializeToString(), feed_b.SerializeToString())
        assert report.equivalence_level == -1
