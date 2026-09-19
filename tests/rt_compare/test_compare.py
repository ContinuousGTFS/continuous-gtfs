"""Tests for all 10 fixtures producing expected comparison results."""

from __future__ import annotations

from continuous_gtfs.rt_compare.compare import compare_feeds
from continuous_gtfs.rt_compare.config import ComparisonConfig


class TestFixtureIdenticalReordered:
    """Fixture 1: same entities, different order."""

    def test_equivalence_level(self, identical_reordered, default_config):
        feed_a, feed_b = identical_reordered
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.equivalence_level == 2

    def test_no_differences(self, identical_reordered, default_config):
        feed_a, feed_b = identical_reordered
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.summary.added == 0
        assert report.summary.removed == 0
        assert report.summary.modified == 0
        assert report.summary.matched == 3


class TestFixtureIdenticalTimestamps:
    """Fixture 2: same entities, different header timestamp."""

    def test_equivalence_level(self, identical_timestamps, default_config):
        feed_a, feed_b = identical_timestamps
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.equivalence_level == 2

    def test_header_delta(self, identical_timestamps, default_config):
        feed_a, feed_b = identical_timestamps
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.header.timestamp_delta_seconds == 5
        assert report.header.version_match is True
        assert report.header.incrementality_match is True


class TestFixtureAddedEntity:
    """Fixture 3: feed B has one extra VehiclePosition."""

    def test_reports_added(self, added_entity, default_config):
        feed_a, feed_b = added_entity
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.summary.added == 1
        assert report.summary.matched == 3
        assert report.equivalence_level == -1

    def test_added_entity_id(self, added_entity, default_config):
        feed_a, feed_b = added_entity
        report = compare_feeds(feed_a, feed_b, default_config)
        added_diffs = [d for d in report.differences if d.status == "added"]
        assert len(added_diffs) == 1
        assert added_diffs[0].entity_id == "v4"


class TestFixtureRemovedEntity:
    """Fixture 4: feed B missing one TripUpdate."""

    def test_reports_removed(self, removed_entity, default_config):
        feed_a, feed_b = removed_entity
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.summary.removed == 1
        assert report.summary.matched == 2
        assert report.equivalence_level == -1

    def test_removed_entity_id(self, removed_entity, default_config):
        feed_a, feed_b = removed_entity
        report = compare_feeds(feed_a, feed_b, default_config)
        removed_diffs = [d for d in report.differences if d.status == "removed"]
        assert len(removed_diffs) == 1
        assert removed_diffs[0].entity_id == "t3"

    def test_feed_type(self, removed_entity, default_config):
        feed_a, feed_b = removed_entity
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.feed_type == "trip_updates"


class TestFixtureModifiedPosition:
    """Fixture 5: one vehicle's lat/lon differs by 0.0001 deg."""

    def test_reports_modified(self, modified_position, default_config):
        feed_a, feed_b = modified_position
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.summary.modified == 1
        assert report.equivalence_level == -1

    def test_modified_entity_id(self, modified_position, default_config):
        feed_a, feed_b = modified_position
        report = compare_feeds(feed_a, feed_b, default_config)
        modified_diffs = [d for d in report.differences if d.status == "modified"]
        assert len(modified_diffs) == 1
        assert modified_diffs[0].entity_id == "v2"


class TestFixtureModifiedPositionNoise:
    """Fixture 6: one vehicle's lat/lon differs by 0.000001 deg."""

    def test_equivalent_at_5dp(self, modified_position_noise, default_config):
        feed_a, feed_b = modified_position_noise
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.summary.modified == 0
        assert report.equivalence_level == 2

    def test_not_byte_identical(self, modified_position_noise):
        feed_a, feed_b = modified_position_noise
        assert feed_a != feed_b


class TestFixtureStaleEntity:
    """Fixture 7: feed A has entity with timestamp 10 minutes old."""

    def test_stale_filtered_default_threshold(self, stale_entity, default_config):
        """At default stale_threshold (300s), the stale entity IS filtered."""
        feed_a, feed_b = stale_entity
        report = compare_feeds(feed_a, feed_b, default_config)
        # v1 is stale (600s old > 300s threshold), so it's classified as stale
        assert report.summary.stale == 1
        assert report.summary.removed == 0
        assert report.equivalence_level == 3

    def test_stale_entity_id(self, stale_entity, default_config):
        feed_a, feed_b = stale_entity
        report = compare_feeds(feed_a, feed_b, default_config)
        stale_diffs = [d for d in report.differences if d.status == "stale"]
        assert len(stale_diffs) == 1
        assert stale_diffs[0].entity_id == "v1"

    def test_stale_disabled_reports_removed(self, stale_entity):
        """With stale_threshold=0, the stale entity is treated as removed."""
        feed_a, feed_b = stale_entity
        config = ComparisonConfig(stale_threshold_seconds=0)
        report = compare_feeds(feed_a, feed_b, config)
        assert report.summary.removed == 1
        assert report.summary.stale == 0
        assert report.equivalence_level == -1
        removed_diffs = [d for d in report.differences if d.status == "removed"]
        assert len(removed_diffs) == 1
        assert removed_diffs[0].entity_id == "v1"


class TestFixtureStopTimeUpdateReorder:
    """Fixture 8: same TripUpdate, stop_time_updates in different order."""

    def test_equivalence_level(self, stop_time_update_reorder, default_config):
        feed_a, feed_b = stop_time_update_reorder
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.equivalence_level == 2

    def test_no_differences(self, stop_time_update_reorder, default_config):
        feed_a, feed_b = stop_time_update_reorder
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.summary.modified == 0
        assert report.summary.matched == 1


class TestFixtureDefaultValuePresence:
    """Fixture 9: one feed has congestion_level: 0, other omits it."""

    def test_equivalence_level(self, default_value_presence, default_config):
        feed_a, feed_b = default_value_presence
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.equivalence_level == 2

    def test_no_modifications(self, default_value_presence, default_config):
        feed_a, feed_b = default_value_presence
        report = compare_feeds(feed_a, feed_b, default_config)
        assert report.summary.modified == 0
        assert report.summary.matched == 1


class TestFixtureAlertTimeOverlap:
    """Fixture 10: alerts with same content but different active_periods."""

    def test_reports_modified(self, alert_time_overlap, default_config):
        feed_a, feed_b = alert_time_overlap
        report = compare_feeds(feed_a, feed_b, default_config)
        # Different active_period structure should be detected
        assert report.summary.modified == 1
        assert report.feed_type == "alerts"

    def test_not_equivalent(self, alert_time_overlap, default_config):
        feed_a, feed_b = alert_time_overlap
        report = compare_feeds(feed_a, feed_b, default_config)
        # They have structurally different active_periods
        assert report.equivalence_level == -1
