"""Tests for the content-kind parser registry."""

from __future__ import annotations

import io
import zipfile

import polars as pl
import pytest
from google.transit import gtfs_realtime_pb2 as gtfs_rt

from continuous_gtfs.parsers import known_kinds, parse, register_parser


def _make_schedule_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


class TestOpaqueBytes:
    def test_passes_through_untouched(self):
        raw = b"\x00\x01\x02 whatever"
        assert parse("opaque_bytes", raw) is raw


class TestScheduleZip:
    def test_parses_to_dict_of_dataframes(self):
        raw = _make_schedule_zip(
            {
                "stops.txt": "stop_id,stop_name\nA,Foo\nB,Bar\n",
                "routes.txt": "route_id,route_type\nR1,3\n",
            }
        )
        result = parse("gtfs_schedule_zip", raw)
        assert isinstance(result, dict)
        assert set(result.keys()) == {"stops.txt", "routes.txt"}
        assert isinstance(result["stops.txt"], pl.DataFrame)
        assert result["stops.txt"]["stop_id"].to_list() == ["A", "B"]


class TestCsvTable:
    def test_parses_to_dataframe(self):
        raw = b"stop_id,stop_desc\nS1,hello\nS2,world\n"
        df = parse("csv_table", raw)
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (2, 2)
        assert df["stop_id"].to_list() == ["S1", "S2"]

    def test_all_columns_typed_as_string(self):
        """Guard against leading-zero loss on numeric-looking IDs."""
        raw = b"stop_id,count\nS1,00123\n"
        df = parse("csv_table", raw)
        assert df["stop_id"].dtype == pl.Utf8
        assert df["count"].dtype == pl.Utf8
        assert df["count"].to_list() == ["00123"]


class TestRtProtobuf:
    def test_parses_to_feed_message(self):
        feed = gtfs_rt.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = 1712345678
        entity = feed.entity.add()
        entity.id = "v1"
        entity.vehicle.vehicle.id = "DEMO-100"
        raw = feed.SerializeToString()

        parsed = parse("gtfs_rt_protobuf", raw)
        assert isinstance(parsed, gtfs_rt.FeedMessage)
        assert parsed.header.gtfs_realtime_version == "2.0"
        assert len(parsed.entity) == 1
        assert parsed.entity[0].vehicle.vehicle.id == "DEMO-100"


class TestRegistry:
    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError, match="No parser registered"):
            parse("not_a_real_kind", b"anything")

    def test_known_kinds_includes_opaque_bytes(self):
        kinds = known_kinds()
        assert "opaque_bytes" in kinds
        assert "gtfs_schedule_zip" in kinds
        assert "csv_table" in kinds
        assert "gtfs_rt_protobuf" in kinds

    def test_register_custom_kind(self):
        @register_parser("_test_only")
        def _parse(raw: bytes) -> str:
            return raw.decode("utf-8").upper()

        try:
            assert parse("_test_only", b"hello") == "HELLO"
        finally:
            from continuous_gtfs import parsers

            parsers._parsers.pop("_test_only", None)
