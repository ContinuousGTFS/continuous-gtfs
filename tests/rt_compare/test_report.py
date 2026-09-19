"""Tests for report generation formats."""

from __future__ import annotations

import json

from continuous_gtfs.rt_compare.report import (
    ComparisonReport,
    EntityDifference,
    EntitySummary,
    HeaderComparison,
)


def _make_sample_report() -> ComparisonReport:
    return ComparisonReport(
        equivalence_level=2,
        feed_type="vehicle_positions",
        header=HeaderComparison(
            timestamp_a=1711000000,
            timestamp_b=1711000003,
            timestamp_delta_seconds=3,
            version_match=True,
            incrementality_match=True,
        ),
        summary=EntitySummary(
            entities_a=127,
            entities_b=127,
            matched=125,
            added=1,
            removed=1,
        ),
        differences=[
            EntityDifference(
                entity_id="v_1234",
                status="added",
                detail="Entity present only in feed B",
            ),
            EntityDifference(
                entity_id="v_5678",
                status="removed",
                detail="Entity present only in feed A",
            ),
        ],
    )


class TestToJson:
    """Test JSON output format."""

    def test_valid_json(self):
        report = _make_sample_report()
        result = report.to_json()
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_equivalence_level(self):
        report = _make_sample_report()
        parsed = json.loads(report.to_json())
        assert parsed["equivalence_level"] == 2

    def test_feed_type(self):
        report = _make_sample_report()
        parsed = json.loads(report.to_json())
        assert parsed["feed_type"] == "vehicle_positions"

    def test_header_fields(self):
        report = _make_sample_report()
        parsed = json.loads(report.to_json())
        header = parsed["header"]
        assert header["timestamp_a"] == 1711000000
        assert header["timestamp_b"] == 1711000003
        assert header["timestamp_delta_seconds"] == 3
        assert header["version_match"] is True
        assert header["incrementality_match"] is True

    def test_summary_fields(self):
        report = _make_sample_report()
        parsed = json.loads(report.to_json())
        summary = parsed["summary"]
        assert summary["entities_a"] == 127
        assert summary["entities_b"] == 127
        assert summary["matched"] == 125
        assert summary["added"] == 1
        assert summary["removed"] == 1

    def test_differences(self):
        report = _make_sample_report()
        parsed = json.loads(report.to_json())
        diffs = parsed["differences"]
        assert len(diffs) == 2
        assert diffs[0]["entity_id"] == "v_1234"
        assert diffs[0]["status"] == "added"
        assert diffs[1]["entity_id"] == "v_5678"
        assert diffs[1]["status"] == "removed"

    def test_empty_differences(self):
        report = ComparisonReport(
            equivalence_level=0,
            feed_type="vehicle_positions",
            header=HeaderComparison(
                timestamp_a=1711000000,
                timestamp_b=1711000000,
                timestamp_delta_seconds=0,
                version_match=True,
                incrementality_match=True,
            ),
            summary=EntitySummary(entities_a=3, entities_b=3, matched=3),
        )
        parsed = json.loads(report.to_json())
        assert parsed["differences"] == []


class TestToText:
    """Test human-readable text output format."""

    def test_contains_feed_type(self):
        report = _make_sample_report()
        text = report.to_text()
        assert "vehicle_positions" in text

    def test_contains_equivalence_level(self):
        report = _make_sample_report()
        text = report.to_text()
        assert "Level 2" in text
        assert "semantically equivalent" in text

    def test_contains_header_info(self):
        report = _make_sample_report()
        text = report.to_text()
        assert "timestamp delta 3s" in text

    def test_contains_entity_counts(self):
        report = _make_sample_report()
        text = report.to_text()
        assert "127 vs 127" in text
        assert "125" in text

    def test_contains_differences(self):
        report = _make_sample_report()
        text = report.to_text()
        assert "[+] v_1234" in text
        assert "[-] v_5678" in text

    def test_not_equivalent_text(self):
        report = ComparisonReport(
            equivalence_level=-1,
            feed_type="trip_updates",
            header=HeaderComparison(
                timestamp_a=1711000000,
                timestamp_b=1711000000,
                timestamp_delta_seconds=0,
                version_match=True,
                incrementality_match=True,
            ),
            summary=EntitySummary(
                entities_a=3,
                entities_b=4,
                matched=3,
                added=1,
            ),
            differences=[
                EntityDifference(
                    entity_id="t1",
                    status="added",
                    detail="Entity present only in feed B",
                )
            ],
        )
        text = report.to_text()
        assert "NOT equivalent" in text

    def test_to_dict_returns_dict(self):
        report = _make_sample_report()
        d = report.to_dict()
        assert isinstance(d, dict)
        assert d["equivalence_level"] == 2
