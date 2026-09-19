"""Tests for `continuous_gtfs.prep` — the shared pipeline-run preparation API."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from continuous_gtfs import PreparedRun, prepare_pipeline_run


def _make_pipeline_dir(
    tmp_path: Path, *, inputs_manifest: str, transforms: str
) -> Path:
    pipeline_dir = tmp_path / "p"
    pipeline_dir.mkdir()
    (pipeline_dir / "__init__.py").write_text(inputs_manifest)
    (pipeline_dir / "transforms.py").write_text(textwrap.dedent(transforms))
    return pipeline_dir


# These tests exercise DAG narrowing / --disable bookkeeping, not inputs, so
# the manifest's one input is optional: a full-DAG run with no --input must
# still be allowed to start (a *required* input would refuse — see the
# required-vs-optional tests at the bottom of this file).
_OPTIONAL_VP_MANIFEST = (
    'INPUTS = {"vp": {"content_kind": "gtfs_rt_protobuf", "optional": True}}\n'
)

_RT_PIPELINE = """
from continuous_gtfs.builtins.realtime import (
    PassThrough,
    FilterStopsByID,
    UpdateFeedHeader,
)

filt = FilterStopsByID(blocked_stop_ids={"S1"})
pass1 = PassThrough(after=[filt])
update = UpdateFeedHeader(after=[pass1])
"""


def test_prepare_minimal_returns_full_dag(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path,
        inputs_manifest=_OPTIONAL_VP_MANIFEST,
        transforms=_RT_PIPELINE,
    )
    prep = prepare_pipeline_run(pipeline_dir)

    assert isinstance(prep, PreparedRun)
    assert prep.pipeline_dir == pipeline_dir
    assert {s.name for s in prep.all_steps} == {"filt", "pass1", "update"}
    assert prep.steps == prep.all_steps  # no --select narrowing
    assert prep.manifest == {"vp": "gtfs_rt_protobuf"}
    assert prep.inputs == {}
    assert prep.disabled_steps == []
    assert prep.unknown_disabled_steps == []
    assert prep.selection is None
    assert prep.selection_summary is None


def test_prepare_select_narrows_to_step_plus_ancestors(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path,
        inputs_manifest=_OPTIONAL_VP_MANIFEST,
        transforms=_RT_PIPELINE,
    )
    prep = prepare_pipeline_run(pipeline_dir, select="pass1")

    assert {s.name for s in prep.steps} == {"filt", "pass1"}
    assert prep.selection == "pass1"
    assert prep.selection_summary == (
        "Selected: pass1 + 1 ancestor(s) — 1 step(s) skipped"
    )
    # full DAG still available
    assert {s.name for s in prep.all_steps} == {"filt", "pass1", "update"}


def test_prepare_select_unknown_step_raises_value_error(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path,
        inputs_manifest=_OPTIONAL_VP_MANIFEST,
        transforms=_RT_PIPELINE,
    )
    with pytest.raises(ValueError):
        prepare_pipeline_run(pipeline_dir, select="not_a_real_step")


def test_prepare_loads_inputs_through_manifest(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path,
        inputs_manifest='INPUTS = {"overrides": "csv_table"}\n',
        transforms=_RT_PIPELINE,
    )
    csv = tmp_path / "overrides.csv"
    csv.write_text("k,v\n1,a\n2,b\n")

    prep = prepare_pipeline_run(pipeline_dir, inputs=[f"overrides={csv}"])
    assert "overrides" in prep.inputs
    assert prep.inputs["overrides"].shape == (2, 2)


def test_prepare_disabled_splits_known_from_unknown(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path,
        inputs_manifest=_OPTIONAL_VP_MANIFEST,
        transforms=_RT_PIPELINE,
    )
    prep = prepare_pipeline_run(
        pipeline_dir,
        disabled_steps=["pass1", "bogus_step", "filt"],
    )

    # disabled_steps passes through unchanged — executor will silently
    # ignore unknowns; surface them via unknown_disabled_steps so callers
    # can warn.
    assert prep.disabled_steps == ["pass1", "bogus_step", "filt"]
    assert prep.unknown_disabled_steps == ["bogus_step"]


def test_prepare_disabled_unknown_judged_against_selected_subset(tmp_path):
    """When --select narrows the DAG, --disable names are validated against
    the narrowed step set, not the full DAG."""
    pipeline_dir = _make_pipeline_dir(
        tmp_path,
        inputs_manifest=_OPTIONAL_VP_MANIFEST,
        transforms=_RT_PIPELINE,
    )
    # 'update' isn't an ancestor of 'pass1', so --select pass1 drops it.
    # --disable update should then surface as unknown.
    prep = prepare_pipeline_run(
        pipeline_dir,
        select="pass1",
        disabled_steps=["update"],
    )
    assert prep.unknown_disabled_steps == ["update"]
    # selected steps do not include 'update'
    assert "update" not in {s.name for s in prep.steps}


def test_prepare_bad_input_path_raises_file_not_found(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path,
        inputs_manifest='INPUTS = {"overrides": "csv_table"}\n',
        transforms=_RT_PIPELINE,
    )
    with pytest.raises(FileNotFoundError):
        prepare_pipeline_run(
            pipeline_dir,
            inputs=[f"overrides={tmp_path}/no-such-file.csv"],
        )


def test_prepare_undeclared_input_name_raises_value_error(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path,
        inputs_manifest='INPUTS = {"overrides": "csv_table"}\n',
        transforms=_RT_PIPELINE,
    )
    csv = tmp_path / "anything.csv"
    csv.write_text("k,v\n1,a\n")
    with pytest.raises(ValueError, match="not declared"):
        prepare_pipeline_run(pipeline_dir, inputs=[f"not_overrides={csv}"])


# --- required vs optional inputs (specs/transform-framework.md §INPUTS manifest) ---

_MIXED_MANIFEST = """\
INPUTS = {
    "vp": "gtfs_rt_protobuf",
    "overrides": {"content_kind": "csv_table", "optional": True},
}
"""


def _rt_feed_file(tmp_path: Path) -> Path:
    from google.transit import gtfs_realtime_pb2 as gtfs_rt

    feed = gtfs_rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    path = tmp_path / "vp.pb"
    path.write_bytes(feed.SerializeToString())
    return path


def test_prepare_exposes_input_specs_alongside_flat_manifest(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path, inputs_manifest=_MIXED_MANIFEST, transforms=_RT_PIPELINE
    )
    vp = _rt_feed_file(tmp_path)

    prep = prepare_pipeline_run(pipeline_dir, inputs=[f"vp={vp}"])
    assert prep.manifest == {"vp": "gtfs_rt_protobuf", "overrides": "csv_table"}
    assert prep.input_specs == {
        "vp": {"content_kind": "gtfs_rt_protobuf", "optional": False},
        "overrides": {"content_kind": "csv_table", "optional": True},
    }


def test_prepare_omitting_optional_input_is_allowed(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path, inputs_manifest=_MIXED_MANIFEST, transforms=_RT_PIPELINE
    )
    vp = _rt_feed_file(tmp_path)

    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # any warning here is a failure
        prep = prepare_pipeline_run(pipeline_dir, inputs=[f"vp={vp}"])

    assert "vp" in prep.inputs
    assert "overrides" not in prep.inputs  # absent, not None
    assert prep.missing_required_inputs == []


def test_prepare_omitting_required_input_on_full_run_raises(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path, inputs_manifest=_MIXED_MANIFEST, transforms=_RT_PIPELINE
    )
    csv = tmp_path / "overrides.csv"
    csv.write_text("k,v\n1,a\n")

    # Only the optional input supplied; the required `vp` is missing.
    with pytest.raises(ValueError, match="required input\\(s\\) not supplied") as exc:
        prepare_pipeline_run(pipeline_dir, inputs=[f"overrides={csv}"])
    assert "vp" in str(exc.value)
    assert "overrides" not in str(exc.value).split("not supplied via --input:")[1]


def test_prepare_omitting_required_input_with_nothing_supplied_raises(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path, inputs_manifest=_MIXED_MANIFEST, transforms=_RT_PIPELINE
    )
    with pytest.raises(ValueError, match="vp"):
        prepare_pipeline_run(pipeline_dir)


def test_prepare_omitting_required_input_with_select_warns_and_proceeds(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path, inputs_manifest=_MIXED_MANIFEST, transforms=_RT_PIPELINE
    )

    with pytest.warns(UserWarning, match="required input\\(s\\) not supplied") as rec:
        prep = prepare_pipeline_run(pipeline_dir, select="filt")

    assert prep.missing_required_inputs == ["vp"]
    assert [s.name for s in prep.steps] == ["filt"]
    assert prep.inputs == {}
    messages = [str(w.message) for w in rec]
    assert any("vp" in m and "--select filt" in m for m in messages)


def test_prepare_select_with_all_required_supplied_does_not_warn(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path, inputs_manifest=_MIXED_MANIFEST, transforms=_RT_PIPELINE
    )
    vp = _rt_feed_file(tmp_path)

    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        prep = prepare_pipeline_run(pipeline_dir, select="filt", inputs=[f"vp={vp}"])
    assert prep.missing_required_inputs == []


def test_prepare_unknown_input_name_still_fails_even_when_optional_exists(tmp_path):
    pipeline_dir = _make_pipeline_dir(
        tmp_path, inputs_manifest=_MIXED_MANIFEST, transforms=_RT_PIPELINE
    )
    vp = _rt_feed_file(tmp_path)
    with pytest.raises(ValueError, match="not declared in the pipeline's INPUTS"):
        prepare_pipeline_run(pipeline_dir, inputs=[f"vp={vp}", f"bogus={vp}"])
