"""Tests for CLI helpers: input parsing, events serialization, diff wrapper."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl
import pytest

from continuous_gtfs import load_inputs
from continuous_gtfs.cli import (
    _build_schedule_events,
    _color,
    _field_diffs,
    _format_row,
    _install_warning_formatter,
    _prepare_run_or_exit,
    _print_archive_diff_rich,
)

# --- load_inputs (public library API) ---


def test_load_inputs_csv_via_manifest(tmp_path):
    """Manifest kind drives parsing — file extension is irrelevant."""
    csv = tmp_path / "overrides.csv"
    csv.write_text("stop_id,stop_desc\nS1,hello\nS2,world\n")

    manifest = {"overrides": "csv_table"}
    inputs = load_inputs([f"overrides={csv}"], manifest)
    assert "overrides" in inputs
    assert inputs["overrides"].shape[0] == 2
    assert inputs["overrides"]["stop_desc"].to_list() == ["hello", "world"]


def test_load_inputs_schedule_zip_via_manifest(tmp_path):
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("test.txt", "k,v\n1,a\n")

    zip_path = tmp_path / "feed.zip"
    zip_path.write_bytes(buf.getvalue())

    manifest = {"schedule": "gtfs_schedule_zip"}
    inputs = load_inputs([f"schedule={zip_path}"], manifest)
    assert isinstance(inputs["schedule"], dict)
    assert "test.txt" in inputs["schedule"]
    assert isinstance(inputs["schedule"]["test.txt"], pl.DataFrame)
    assert inputs["schedule"]["test.txt"]["v"].to_list() == ["a"]


def test_load_inputs_empty_list():
    assert load_inputs([], {"schedule": "gtfs_schedule_zip"}) == {}


def test_load_inputs_malformed_arg_missing_equals():
    with pytest.raises(ValueError, match="Expected NAME"):
        load_inputs(["nopath"], {})


def test_load_inputs_empty_name():
    with pytest.raises(ValueError, match="Name cannot be empty"):
        load_inputs(["=path.csv"], {})


def test_load_inputs_missing_file(tmp_path):
    manifest = {"x": "csv_table"}
    with pytest.raises(FileNotFoundError, match="Input file not found"):
        load_inputs([f"x={tmp_path}/does-not-exist.csv"], manifest)


def test_load_inputs_name_not_in_manifest_fails(tmp_path):
    csv = tmp_path / "data.csv"
    csv.write_text("k,v\n1,a\n")
    with pytest.raises(ValueError, match="not declared in the pipeline's INPUTS"):
        load_inputs([f"unknown={csv}"], {"schedule": "gtfs_schedule_zip"})


def test_load_inputs_multiple(tmp_path):
    a = tmp_path / "a.csv"
    a.write_text("k,v\n1,A\n")
    b = tmp_path / "b.csv"
    b.write_text("k,v\n2,B\n")
    manifest = {"alpha": "csv_table", "beta": "csv_table"}
    inputs = load_inputs([f"alpha={a}", f"beta={b}"], manifest)
    assert set(inputs.keys()) == {"alpha", "beta"}


def test_load_inputs_explicit_kind_override(tmp_path):
    """:content_kind suffix overrides the manifest kind for that flag."""
    blob = tmp_path / "weird.dat"
    blob.write_bytes(b"\x01\x02\x03")
    manifest = {"weird": "csv_table"}
    with pytest.warns(UserWarning, match="overrides manifest kind"):
        inputs = load_inputs([f"weird:opaque_bytes={blob}"], manifest)
    assert inputs["weird"] == b"\x01\x02\x03"


def test_load_inputs_unknown_explicit_kind_fails(tmp_path):
    csv = tmp_path / "x.csv"
    csv.write_text("k,v\n1,a\n")
    with pytest.raises(ValueError, match="Unknown content_kind"):
        load_inputs([f"x:not_a_kind={csv}"], {"x": "csv_table"})


def test_cli_prepare_run_translates_library_errors_to_exit_2(tmp_path):
    """CLI wrapper around prepare_pipeline_run catches the library's
    exceptions and converts them to sys.exit(2) with a printed message."""
    import argparse

    pipeline_dir = tmp_path / "noop_pipeline"
    pipeline_dir.mkdir()
    (pipeline_dir / "__init__.py").write_text('INPUTS = {"x": "csv_table"}\n')
    (pipeline_dir / "transforms.py").write_text(
        "from continuous_gtfs.builtins.realtime import PassThrough\n"
        "noop = PassThrough()\n"
    )

    args = argparse.Namespace(
        pipeline_dir=pipeline_dir,
        select=None,
        inputs=[f"x={tmp_path}/missing.csv"],
        disabled_steps=[],
    )
    with pytest.raises(SystemExit) as exc:
        _prepare_run_or_exit(args)
    assert exc.value.code == 2


# --- _build_schedule_events ---


class _Args:
    """Minimal argparse-ish holder for _build_schedule_events."""

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


def _make_schedule_result():
    """Build a SchedulePipelineResult by running the real pipeline on a minimal zip."""
    from continuous_gtfs import step
    from continuous_gtfs.pipelines.schedule import extract_zip, run_schedule_pipeline

    @step(before="*")
    def init_output_from_schedule(ctx):
        ctx.output = dict(ctx.inputs["schedule"])

    init_output_from_schedule.name = "init_output_from_schedule"

    @step(files=["stops.txt"])
    def noop(ctx):
        pass

    noop.name = "noop"

    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "agency.txt",
            "agency_name,agency_url,agency_timezone\nDMO,http://demo.example,America/Los_Angeles\n",
        )
        zf.writestr("routes.txt", "route_id,route_type\nA,3\n")
        zf.writestr("trips.txt", "route_id,service_id,trip_id\nA,s1,t1\n")
        zf.writestr("stops.txt", "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,0,0\n")
        zf.writestr("stop_times.txt", "trip_id,stop_id,stop_sequence\nt1,S1,1\n")
    return run_schedule_pipeline(
        {"schedule": extract_zip(buf.getvalue())},
        [init_output_from_schedule, noop],
    )


def test_build_schedule_events_shape(tmp_path):
    result = _make_schedule_result()
    args = _Args(
        env="production",
        pipeline_dir=Path("pipelines/schedule"),
        inputs=["schedule=input.zip"],
        output=tmp_path / "out.zip",
    )
    events = _build_schedule_events(args, result, diff=None, baseline=None)

    assert events["pipeline"] == "schedule"
    assert events["environment"] == "production"
    assert events["success"] is True
    assert events["quick"] is False
    assert "input" not in events  # auto-seed / ingest-stage input reporting removed
    assert "output" in events and events["output"]["files"] == 5
    assert isinstance(events["stages"], list)
    # transform + validate_output + package (ingest and validate_input removed
    # with the auto-seed refactor)
    assert [s["name"] for s in events["stages"]] == [
        "transform",
        "validate_output",
        "package",
    ]
    assert isinstance(events["steps"], list)
    assert [s["name"] for s in events["steps"]] == ["init_output_from_schedule", "noop"]
    assert all(s["status"] == "success" for s in events["steps"])
    assert "diff" not in events  # diff only present when --diff-against is used


def test_build_schedule_events_records_quick_flag(tmp_path):
    """When --quick is passed, events record reflects it and omits validation stages."""
    from continuous_gtfs import step
    from continuous_gtfs.pipelines.schedule import extract_zip, run_schedule_pipeline

    @step(before="*")
    def init_output_from_schedule(ctx):
        ctx.output = dict(ctx.inputs["schedule"])

    init_output_from_schedule.name = "init_output_from_schedule"

    @step(files=["stops.txt"])
    def noop(ctx):
        pass

    noop.name = "noop"

    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "agency.txt",
            "agency_name,agency_url,agency_timezone\nDMO,http://demo.example,America/Los_Angeles\n",
        )
        zf.writestr("routes.txt", "route_id,route_type\nA,3\n")
        zf.writestr("trips.txt", "route_id,service_id,trip_id\nA,s1,t1\n")
        zf.writestr("stops.txt", "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,0,0\n")
        zf.writestr("stop_times.txt", "trip_id,stop_id,stop_sequence\nt1,S1,1\n")
    result = run_schedule_pipeline(
        {"schedule": extract_zip(buf.getvalue())},
        [init_output_from_schedule, noop],
        quick=True,
    )

    args = _Args(
        env="production",
        pipeline_dir=Path("pipelines/schedule"),
        inputs=["schedule=input.zip"],
        output=None,
        quick=True,
    )
    events = _build_schedule_events(args, result, diff=None, baseline=None)

    assert events["quick"] is True
    stage_names = [s["name"] for s in events["stages"]]
    # --quick drops validate_output and package; only transform remains.
    assert stage_names == ["transform"]


def test_build_schedule_events_json_serializable(tmp_path):
    """Events dict must round-trip through json.dumps with default=str."""
    result = _make_schedule_result()
    args = _Args(
        env="production",
        pipeline_dir=Path("pipelines/schedule"),
        inputs=["schedule=input.zip"],
        output=None,
    )
    events = _build_schedule_events(args, result, diff=None, baseline=None)
    blob = json.dumps(events, default=str)
    parsed = json.loads(blob)
    assert parsed["pipeline"] == "schedule"
    assert parsed["steps"][0]["started_at"]  # ISO string, not None


def test_build_schedule_events_includes_diff():
    """When a diff is passed, events include the diff structure."""
    result = _make_schedule_result()
    args = _Args(
        env="production",
        pipeline_dir=Path("pipelines/schedule"),
        inputs=["schedule=input.zip"],
        output=None,
    )

    class _FileDiff:
        added_count = 1
        removed_count = 2
        modified_count = 3
        added_columns: list[str] = []
        removed_columns: list[str] = []

    class _Diff:
        is_identical = False
        added_files = {"new.txt"}
        removed_files = set()
        unchanged_files = {"agency.txt"}
        modified_files = {"stops.txt"}

        def file_diff(self, name):
            return _FileDiff()

    events = _build_schedule_events(
        args, result, diff=_Diff(), baseline=Path("baseline.zip")
    )
    assert "diff" in events
    assert events["diff"]["identical"] is False
    assert events["diff"]["modified_files"]["stops.txt"]["added"] == 1
    assert events["diff"]["added_files"] == ["new.txt"]
    assert events["diff"]["baseline"] == "baseline.zip"


# --- Rich diff helpers ---


def test_color_disabled_returns_plain():
    assert _color("hello", "red", enabled=False) == "hello"


def test_color_enabled_wraps_in_ansi():
    result = _color("hello", "red", enabled=True)
    assert "hello" in result
    assert "\033[31m" in result
    assert "\033[0m" in result


def test_color_unknown_style_returns_plain():
    assert _color("hello", "magenta", enabled=True) == "hello"


def test_format_row_puts_primary_key_first():
    row = {"stop_name": "Main St", "stop_id": "S1", "stop_lat": "47.6"}
    out = _format_row(row, primary_key=["stop_id"])
    assert out.startswith("stop_id=S1")
    assert 'stop_name="Main St"' in out
    assert 'stop_lat="47.6"' in out


def test_format_row_omits_empty_non_pk_fields():
    row = {"stop_id": "S1", "stop_name": "", "stop_desc": None, "stop_lat": "47.6"}
    out = _format_row(row, primary_key=["stop_id"])
    assert "stop_name" not in out
    assert "stop_desc" not in out
    assert 'stop_lat="47.6"' in out


def test_field_diffs_skips_pk_and_unchanged():
    old = {"stop_id": "S1", "stop_name": "Old", "stop_lat": "47.6"}
    new = {"stop_id": "S1", "stop_name": "New", "stop_lat": "47.6"}
    diffs = _field_diffs(old, new, primary_key=["stop_id"])
    assert diffs == [("stop_name", "Old", "New")]


def test_field_diffs_handles_none_values():
    old = {"stop_id": "S1", "note": None}
    new = {"stop_id": "S1", "note": "hello"}
    diffs = _field_diffs(old, new, primary_key=["stop_id"])
    assert diffs == [("note", "", "hello")]


# --- Rich diff end-to-end via minimal archives ---


def _make_test_archive_zip(stops_csv: str, tmp_path: Path, name: str) -> Path:
    """Build a minimal but schema-valid GTFS zip with custom stops.txt content."""
    import zipfile

    zip_path = tmp_path / name
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(
            "agency.txt", "agency_name,agency_url,agency_timezone\nA,http://a,UTC\n"
        )
        zf.writestr("routes.txt", "route_id,route_type\nR1,3\n")
        zf.writestr("trips.txt", "route_id,service_id,trip_id\nR1,s1,t1\n")
        zf.writestr("stops.txt", stops_csv)
        zf.writestr("stop_times.txt", "trip_id,stop_id,stop_sequence\nt1,S1,1\n")
    return zip_path


def test_print_archive_diff_rich_shows_modifications(tmp_path, capsys):
    import gtfs_digester as digester

    base_csv = (
        "stop_id,stop_name,stop_lat,stop_lon\n"
        "S1,Old Name,38.6,-90.3\n"
        "S2,Unchanged,38.7,-90.4\n"
    )
    cand_csv = (
        "stop_id,stop_name,stop_lat,stop_lon\n"
        "S1,New Name,38.6,-90.3\n"
        "S2,Unchanged,38.7,-90.4\n"
    )
    base = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(base_csv, tmp_path, "base.zip")
    )
    cand = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(cand_csv, tmp_path, "cand.zip")
    )
    diff = base.diff(cand)

    _print_archive_diff_rich(
        diff,
        base,
        cand,
        "base.zip",
        "cand.zip",
        limit=50,
        use_color=False,
    )
    out = capsys.readouterr().out

    assert "~ stops.txt" in out
    assert "stop_id=S1" in out
    assert "stop_name:" in out
    assert '"Old Name"' in out
    assert '"New Name"' in out
    # PK column itself should not appear as a field-diff line
    assert "stop_id: " not in out  # stop_id doesn't have its own field-diff line


def test_print_archive_diff_rich_shows_schema_drift(tmp_path, capsys):
    """Regression: a candidate-added
    column should surface as a schema-drift note, not crash or be silently
    hidden."""
    import gtfs_digester as digester

    base_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,38.6,-90.3\n"
    cand_csv = (
        "stop_id,stop_name,stop_lat,stop_lon,stop_code\nS1,Main,38.6,-90.3,CODE1\n"
    )
    base = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(base_csv, tmp_path, "base.zip")
    )
    cand = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(cand_csv, tmp_path, "cand.zip")
    )
    diff = base.diff(cand)

    _print_archive_diff_rich(
        diff,
        base,
        cand,
        "base.zip",
        "cand.zip",
        limit=50,
        use_color=False,
    )
    out = capsys.readouterr().out

    assert "~ stops.txt" in out
    assert "schema:" in out
    assert "+stop_code" in out


def test_print_archive_diff_rich_shows_removed_and_added(tmp_path, capsys):
    import gtfs_digester as digester

    base_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Only in base,38.6,-90.3\n"
    cand_csv = "stop_id,stop_name,stop_lat,stop_lon\nS2,Only in candidate,38.7,-90.4\n"
    base = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(base_csv, tmp_path, "base.zip")
    )
    cand = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(cand_csv, tmp_path, "cand.zip")
    )
    diff = base.diff(cand)

    _print_archive_diff_rich(
        diff,
        base,
        cand,
        "base.zip",
        "cand.zip",
        limit=50,
        use_color=False,
    )
    out = capsys.readouterr().out

    assert "- stop_id=S1" in out
    assert "+ stop_id=S2" in out
    assert "Only in base" in out
    assert "Only in candidate" in out


def test_print_archive_diff_rich_no_ansi_when_color_disabled(tmp_path, capsys):
    import gtfs_digester as digester

    base_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Old,38.6,-90.3\n"
    cand_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,New,38.6,-90.3\n"
    base = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(base_csv, tmp_path, "base.zip")
    )
    cand = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(cand_csv, tmp_path, "cand.zip")
    )
    diff = base.diff(cand)

    _print_archive_diff_rich(
        diff,
        base,
        cand,
        "base.zip",
        "cand.zip",
        limit=50,
        use_color=False,
    )
    out = capsys.readouterr().out
    assert "\033[" not in out


def test_print_archive_diff_rich_ansi_when_color_enabled(tmp_path, capsys):
    import gtfs_digester as digester

    base_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Old,38.6,-90.3\n"
    cand_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,New,38.6,-90.3\n"
    base = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(base_csv, tmp_path, "base.zip")
    )
    cand = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(cand_csv, tmp_path, "cand.zip")
    )
    diff = base.diff(cand)

    _print_archive_diff_rich(
        diff,
        base,
        cand,
        "base.zip",
        "cand.zip",
        limit=50,
        use_color=True,
    )
    out = capsys.readouterr().out
    assert "\033[31m" in out  # red for old value
    assert "\033[32m" in out  # green for new value


def test_print_archive_diff_rich_respects_limit(tmp_path, capsys):
    import gtfs_digester as digester

    # 5 rows in base, all removed in candidate
    base_csv = "stop_id,stop_name,stop_lat,stop_lon\n" + "".join(
        f"S{i},Name {i},47.{i},-122.{i}\n" for i in range(1, 6)
    )
    cand_csv = "stop_id,stop_name,stop_lat,stop_lon\n"  # empty stops
    base = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(base_csv, tmp_path, "base.zip")
    )
    cand = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(cand_csv, tmp_path, "cand.zip")
    )
    diff = base.diff(cand)

    _print_archive_diff_rich(
        diff,
        base,
        cand,
        "base.zip",
        "cand.zip",
        limit=2,
        use_color=False,
    )
    out = capsys.readouterr().out

    assert "Removed (5):" in out
    # First two rows shown, remaining three truncated
    assert "stop_id=S1" in out
    assert "stop_id=S2" in out
    assert "stop_id=S3" not in out
    assert "... and 3 more" in out


def test_warning_formatter_is_concise(capsys, monkeypatch):
    """Custom warnings formatter drops the default's
    filename/lineno/source-line clutter."""
    import warnings

    # Force non-TTY so we can assert exact text without ANSI codes
    monkeypatch.setattr(sys.stderr, "isatty", lambda: False)
    # Save and restore the global showwarning so this test doesn't leak state
    original = warnings.showwarning
    try:
        _install_warning_formatter()
        warnings.warn("something unusual happened", stacklevel=2)
        captured = capsys.readouterr()
        assert captured.err.strip() == "warning: something unusual happened"
        # No default formatter artifacts
        assert "UserWarning" not in captured.err
        assert "warnings.warn" not in captured.err
    finally:
        warnings.showwarning = original


def test_warning_formatter_colorizes_on_tty(capsys, monkeypatch):
    import warnings

    monkeypatch.setattr(sys.stderr, "isatty", lambda: True)
    original = warnings.showwarning
    try:
        _install_warning_formatter()
        warnings.warn("watch out", stacklevel=2)
        captured = capsys.readouterr()
        assert "\033[33m" in captured.err  # yellow
        assert "\033[0m" in captured.err  # reset
        assert "watch out" in captured.err
    finally:
        warnings.showwarning = original


def test_print_archive_diff_rich_identical(tmp_path, capsys):
    import gtfs_digester as digester

    csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Same,38.6,-90.3\n"
    base = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(csv, tmp_path, "base.zip")
    )
    cand = digester.GTFSArchive.from_zip(
        _make_test_archive_zip(csv, tmp_path, "cand.zip")
    )
    diff = base.diff(cand)

    _print_archive_diff_rich(
        diff,
        base,
        cand,
        "base.zip",
        "cand.zip",
        limit=50,
        use_color=False,
    )
    out = capsys.readouterr().out
    assert "Identical: yes" in out
    assert "Modified" not in out
    assert "Removed" not in out


# --- required vs optional --input handling at the CLI boundary ---


def _cli_pipeline_with_optional(tmp_path: Path) -> Path:
    pipeline_dir = tmp_path / "opt_pipeline"
    pipeline_dir.mkdir()
    (pipeline_dir / "__init__.py").write_text(
        "INPUTS = {\n"
        '    "x": "csv_table",\n'
        '    "extra": {"content_kind": "csv_table", "optional": True},\n'
        "}\n"
    )
    (pipeline_dir / "transforms.py").write_text(
        "from continuous_gtfs.builtins.realtime import PassThrough\n"
        "noop = PassThrough()\n"
    )
    return pipeline_dir


def test_cli_prepare_run_optional_input_may_be_omitted(tmp_path):
    import argparse

    pipeline_dir = _cli_pipeline_with_optional(tmp_path)
    csv = tmp_path / "x.csv"
    csv.write_text("a,b\n1,2\n")

    prep = _prepare_run_or_exit(
        argparse.Namespace(
            pipeline_dir=pipeline_dir,
            select=None,
            inputs=[f"x={csv}"],
            disabled_steps=[],
        )
    )
    assert "x" in prep.inputs
    assert "extra" not in prep.inputs


def test_cli_prepare_run_required_input_omitted_exits_2(tmp_path, capsys):
    import argparse

    pipeline_dir = _cli_pipeline_with_optional(tmp_path)

    with pytest.raises(SystemExit) as exc:
        _prepare_run_or_exit(
            argparse.Namespace(
                pipeline_dir=pipeline_dir, select=None, inputs=[], disabled_steps=[]
            )
        )
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "required input(s) not supplied" in err
    assert "x" in err


def test_cli_prepare_run_required_input_omitted_with_select_warns(tmp_path):
    import argparse

    pipeline_dir = _cli_pipeline_with_optional(tmp_path)

    with pytest.warns(UserWarning, match="required input\\(s\\) not supplied"):
        prep = _prepare_run_or_exit(
            argparse.Namespace(
                pipeline_dir=pipeline_dir,
                select="noop",
                inputs=[],
                disabled_steps=[],
            )
        )
    assert prep.missing_required_inputs == ["x"]


def test_load_inputs_accepts_normalized_manifest(tmp_path):
    csv = tmp_path / "x.csv"
    csv.write_text("a,b\n1,2\n")
    normalized = {"x": {"content_kind": "csv_table", "optional": True}}
    inputs = load_inputs([f"x={csv}"], normalized)
    assert isinstance(inputs["x"], pl.DataFrame)
