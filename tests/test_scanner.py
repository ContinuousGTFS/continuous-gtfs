"""Tests for the pipeline folder scanner."""

import sys
import textwrap
from pathlib import Path

import pytest

from continuous_gtfs import resolve_dag, scan_pipeline
from continuous_gtfs.scanner import select_with_ancestors


@pytest.fixture(autouse=True)
def _clean_sys():
    """Restore sys.path and sys.modules after each test
    to avoid leaking scanner state."""
    path_copy = sys.path[:]
    mods_copy = set(sys.modules)
    yield
    sys.path[:] = path_copy
    for key in set(sys.modules) - mods_copy:
        del sys.modules[key]


def _write_file(folder: Path, name: str, content: str) -> Path:
    p = folder / name
    p.write_text(textwrap.dedent(content))
    return p


def test_scan_discovers_steps(tmp_path):
    _write_file(
        tmp_path,
        "my_step.py",
        """\
        from continuous_gtfs import Step
        a = Step()
        b = Step(after=[a])
    """,
    )
    steps = scan_pipeline(tmp_path)
    names = {s.name for s in steps}
    assert "a" in names
    assert "b" in names


def test_scan_skips_underscore_files(tmp_path):
    _write_file(
        tmp_path,
        "_private.py",
        """\
        from continuous_gtfs import Step
        hidden = Step()
    """,
    )
    _write_file(
        tmp_path,
        "public.py",
        """\
        from continuous_gtfs import Step
        visible = Step()
    """,
    )
    steps = scan_pipeline(tmp_path)
    names = {s.name for s in steps}
    assert "visible" in names
    assert "hidden" not in names


def test_scan_skips_underscore_attributes(tmp_path):
    _write_file(
        tmp_path,
        "mod.py",
        """\
        from continuous_gtfs import Step
        public_step = Step()
        _private_step = Step()
    """,
    )
    steps = scan_pipeline(tmp_path)
    names = {s.name for s in steps}
    assert "public_step" in names
    assert "_private_step" not in names


def test_scan_duplicate_name_raises(tmp_path):
    _write_file(
        tmp_path,
        "a.py",
        """\
        from continuous_gtfs import Step
        dup = Step()
    """,
    )
    _write_file(
        tmp_path,
        "b.py",
        """\
        from continuous_gtfs import Step
        dup = Step()
    """,
    )
    with pytest.raises(ValueError, match="Duplicate step name"):
        scan_pipeline(tmp_path)


def test_scan_cross_import_dedup(tmp_path):
    """Steps imported from another module in the same folder are not doubled."""
    _write_file(
        tmp_path,
        "first.py",
        """\
        from continuous_gtfs import Step
        shared = Step()
    """,
    )
    _write_file(
        tmp_path,
        "second.py",
        """\
        from continuous_gtfs import Step, step
        from .first import shared
        dependent = Step(after=[shared])
    """,
    )
    steps = scan_pipeline(tmp_path)
    names = [s.name for s in steps]
    assert names.count("shared") == 1
    assert "dependent" in names


def test_scan_with_init_py(tmp_path):
    (tmp_path / "__init__.py").write_text("")
    _write_file(
        tmp_path,
        "mod.py",
        """\
        from continuous_gtfs import Step
        x = Step()
    """,
    )
    steps = scan_pipeline(tmp_path)
    assert len(steps) == 1


def test_scan_sets_source_file(tmp_path):
    _write_file(
        tmp_path,
        "mod.py",
        """\
        from continuous_gtfs import Step
        my_step = Step()
    """,
    )
    steps = scan_pipeline(tmp_path)
    assert steps[0].source_file is not None
    assert "mod.py" in steps[0].source_file


def test_scan_empty_folder(tmp_path):
    steps = scan_pipeline(tmp_path)
    assert steps == []


def test_scan_and_resolve_dag(tmp_path):
    _write_file(
        tmp_path,
        "pipeline.py",
        """\
        from continuous_gtfs import Step
        a = Step(priority=10)
        b = Step(after=[a])
        c = Step(after=[b])
    """,
    )
    steps = scan_pipeline(tmp_path)
    dag = resolve_dag(steps)
    names = [s.name for s in dag]
    assert names.index("a") < names.index("b") < names.index("c")


# --- select_with_ancestors ---


def test_select_with_ancestors_leaf_has_no_deps(tmp_path):
    _write_file(
        tmp_path,
        "p.py",
        """\
        from continuous_gtfs import Step
        a = Step()
        b = Step()
    """,
    )
    dag = resolve_dag(scan_pipeline(tmp_path))
    selected = select_with_ancestors(dag, "a")
    assert [s.name for s in selected] == ["a"]


def test_select_with_ancestors_after_chain(tmp_path):
    _write_file(
        tmp_path,
        "p.py",
        """\
        from continuous_gtfs import Step
        a = Step()
        b = Step(after=[a])
        c = Step(after=[b])
        d = Step()
    """,
    )
    dag = resolve_dag(scan_pipeline(tmp_path))
    selected = select_with_ancestors(dag, "c")
    names = [s.name for s in selected]
    assert names == ["a", "b", "c"]
    # DAG order preserved — a before b before c
    # d (unrelated) excluded
    assert "d" not in names


def test_select_with_ancestors_before_edge(tmp_path):
    _write_file(
        tmp_path,
        "p.py",
        """\
        from continuous_gtfs import Step
        setup = Step()
        main = Step()
        # setup.before says setup must run before main, so main depends on setup
        setup.before = [main]
    """,
    )
    dag = resolve_dag(scan_pipeline(tmp_path))
    selected = select_with_ancestors(dag, "main")
    names = [s.name for s in selected]
    assert "setup" in names
    assert names.index("setup") < names.index("main")


def test_select_with_ancestors_unknown_name(tmp_path):
    _write_file(
        tmp_path,
        "p.py",
        """\
        from continuous_gtfs import Step
        a = Step()
        b = Step()
    """,
    )
    dag = resolve_dag(scan_pipeline(tmp_path))
    with pytest.raises(ValueError, match="No step named 'nonexistent'"):
        select_with_ancestors(dag, "nonexistent")


def test_select_with_ancestors_error_lists_available(tmp_path):
    _write_file(
        tmp_path,
        "p.py",
        """\
        from continuous_gtfs import Step
        alpha = Step()
        beta = Step()
    """,
    )
    dag = resolve_dag(scan_pipeline(tmp_path))
    with pytest.raises(ValueError, match="Available: alpha, beta"):
        select_with_ancestors(dag, "missing")


def test_select_with_ancestors_pulls_in_before_wildcards(tmp_path):
    """A `before='*'` step is an implicit ancestor of every selected step."""
    _write_file(
        tmp_path,
        "p.py",
        """\
        from continuous_gtfs import Step
        init = Step(before="*")
        a = Step()
        b = Step()
    """,
    )
    dag = resolve_dag(scan_pipeline(tmp_path))
    selected = select_with_ancestors(dag, "a")
    names = [s.name for s in selected]
    assert "init" in names
    assert "a" in names
    assert "b" not in names
    assert names.index("init") < names.index("a")


def test_select_with_ancestors_pulls_in_multiple_before_wildcards(tmp_path):
    """Every before='*' step is included, not just one."""
    _write_file(
        tmp_path,
        "p.py",
        """\
        from continuous_gtfs import Step
        init_a = Step(before="*")
        init_b = Step(before="*")
        target = Step()
        bystander = Step()
    """,
    )
    dag = resolve_dag(scan_pipeline(tmp_path))
    selected = select_with_ancestors(dag, "target")
    names = [s.name for s in selected]
    assert set(names) == {"init_a", "init_b", "target"}


def test_select_with_ancestors_on_before_wildcard_does_not_pull_in_peers(tmp_path):
    """Selecting a before='*' step itself doesn't pull in OTHER before='*' steps.

    before='*' steps don't edge to each other in the resolver (that would
    cycle), and by the same logic they aren't ancestors of each other —
    they're peers.
    """
    _write_file(
        tmp_path,
        "p.py",
        """\
        from continuous_gtfs import Step
        init_a = Step(before="*")
        init_b = Step(before="*")
        work = Step()
    """,
    )
    dag = resolve_dag(scan_pipeline(tmp_path))
    selected = select_with_ancestors(dag, "init_a")
    names = [s.name for s in selected]
    assert names == ["init_a"]


def test_select_with_ancestors_does_not_pull_in_after_wildcards(tmp_path):
    """after='*' steps are downstream of the target, not ancestors."""
    _write_file(
        tmp_path,
        "p.py",
        """\
        from continuous_gtfs import Step
        init = Step(before="*")
        target = Step()
        finalize = Step(after="*")
    """,
    )
    dag = resolve_dag(scan_pipeline(tmp_path))
    selected = select_with_ancestors(dag, "target")
    names = [s.name for s in selected]
    assert set(names) == {"init", "target"}


# --- INPUTS manifest loading ---


def test_load_inputs_manifest_reads_init_dict(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(
        tmp_path,
        "__init__.py",
        """\
        INPUTS = {
            "schedule": "gtfs_schedule_zip",
            "stop_overrides": "csv_table",
        }
    """,
    )
    manifest = load_inputs_manifest(tmp_path)
    # Bare strings normalize to required entries.
    assert manifest == {
        "schedule": {"content_kind": "gtfs_schedule_zip", "optional": False},
        "stop_overrides": {"content_kind": "csv_table", "optional": False},
    }


def test_load_inputs_manifest_mapping_form_sets_optional(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(
        tmp_path,
        "__init__.py",
        """\
        INPUTS = {
            "schedule": "gtfs_schedule_zip",
            "shuttle": {"content_kind": "gtfs_schedule_zip", "optional": True},
            "overrides": {"content_kind": "csv_table", "optional": False},
            "notes": {"content_kind": "csv_table"},
        }
    """,
    )
    manifest = load_inputs_manifest(tmp_path)
    assert manifest["schedule"] == {
        "content_kind": "gtfs_schedule_zip",
        "optional": False,
    }
    assert manifest["shuttle"] == {
        "content_kind": "gtfs_schedule_zip",
        "optional": True,
    }
    # Explicit optional=False and an omitted optional key both mean required.
    assert manifest["overrides"]["optional"] is False
    assert manifest["notes"]["optional"] is False
    # Manifest (insertion) order is preserved — it's the console's display order.
    assert list(manifest) == ["schedule", "shuttle", "overrides", "notes"]


def test_load_inputs_manifest_mapping_missing_content_kind_raises(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(tmp_path, "__init__.py", 'INPUTS = {"x": {"optional": True}}\n')
    with pytest.raises(ValueError, match="missing a 'content_kind'") as exc:
        load_inputs_manifest(tmp_path)
    assert "specs/transform-framework.md §INPUTS manifest" in str(exc.value)
    assert "'x'" in str(exc.value)


def test_load_inputs_manifest_mapping_non_bool_optional_raises(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(
        tmp_path,
        "__init__.py",
        'INPUTS = {"x": {"content_kind": "csv_table", "optional": "yes"}}\n',
    )
    with pytest.raises(ValueError, match="'optional' must be a bool") as exc:
        load_inputs_manifest(tmp_path)
    assert "specs/transform-framework.md §INPUTS manifest" in str(exc.value)


def test_load_inputs_manifest_mapping_unknown_key_raises(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(
        tmp_path,
        "__init__.py",
        'INPUTS = {"x": {"content_kind": "csv_table", "required": True}}\n',
    )
    with pytest.raises(ValueError, match="unknown key"):
        load_inputs_manifest(tmp_path)


def test_load_inputs_manifest_mapping_unknown_kind_raises(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(
        tmp_path,
        "__init__.py",
        'INPUTS = {"x": {"content_kind": "nope", "optional": True}}\n',
    )
    with pytest.raises(ValueError, match="not a known content_kind"):
        load_inputs_manifest(tmp_path)


@pytest.mark.parametrize("bad_value", ["42", "None", "['csv_table']", "True"])
def test_load_inputs_manifest_other_value_shapes_raise(tmp_path, bad_value):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(tmp_path, "__init__.py", f'INPUTS = {{"x": {bad_value}}}\n')
    with pytest.raises(ValueError, match="content_kind string or a") as exc:
        load_inputs_manifest(tmp_path)
    assert "specs/transform-framework.md §INPUTS manifest" in str(exc.value)


def test_load_inputs_manifest_non_str_key_raises(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(tmp_path, "__init__.py", 'INPUTS = {1: "csv_table"}\n')
    with pytest.raises(ValueError, match="keys must be input names"):
        load_inputs_manifest(tmp_path)


def test_manifest_view_helpers(tmp_path):
    from continuous_gtfs.scanner import (
        load_inputs_manifest,
        manifest_kinds,
        required_input_names,
    )

    _write_file(
        tmp_path,
        "__init__.py",
        """\
        INPUTS = {
            "a": "csv_table",
            "b": {"content_kind": "gtfs_schedule_zip", "optional": True},
            "c": "gtfs_rt_protobuf",
        }
    """,
    )
    manifest = load_inputs_manifest(tmp_path)
    assert manifest_kinds(manifest) == {
        "a": "csv_table",
        "b": "gtfs_schedule_zip",
        "c": "gtfs_rt_protobuf",
    }
    assert required_input_names(manifest) == ["a", "c"]


def test_load_inputs_manifest_missing_init_raises(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    with pytest.raises(FileNotFoundError, match="missing __init__.py"):
        load_inputs_manifest(tmp_path)


def test_load_inputs_manifest_missing_inputs_dict_raises(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(tmp_path, "__init__.py", "# no INPUTS here\n")
    with pytest.raises(ValueError, match="must define a top-level INPUTS"):
        load_inputs_manifest(tmp_path)


def test_load_inputs_manifest_unknown_kind_raises(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(
        tmp_path,
        "__init__.py",
        """\
        INPUTS = {"x": "not_a_real_kind"}
    """,
    )
    with pytest.raises(ValueError, match="not a\\s+known content_kind"):
        load_inputs_manifest(tmp_path)


def test_load_inputs_manifest_wrong_type_raises(tmp_path):
    from continuous_gtfs.scanner import load_inputs_manifest

    _write_file(tmp_path, "__init__.py", "INPUTS = ['not', 'a', 'dict']\n")
    with pytest.raises(ValueError, match="must be a dict"):
        load_inputs_manifest(tmp_path)


def test_dag_edges_expands_wildcards(tmp_path):
    """dag_edges materializes after='*' edges instead of crashing on the
    sentinel (#683) — and export_reactflow includes them instead of
    silently dropping them."""
    _write_file(
        tmp_path,
        "steps.py",
        """\
        from continuous_gtfs import Step
        a = Step()
        b = Step(after=[a])
        summary = Step(after="*")
    """,
    )
    steps = scan_pipeline(tmp_path)
    from continuous_gtfs import dag_edges
    from continuous_gtfs.scanner import export_reactflow

    edge_names = {(x.name, y.name) for x, y in dag_edges(steps)}
    assert ("a", "summary") in edge_names
    assert ("b", "summary") in edge_names
    assert ("a", "b") in edge_names

    rf = export_reactflow(resolve_dag(steps))
    rf_edges = {(e["source"], e["target"]) for e in rf["edges"]}
    assert ("a", "summary") in rf_edges
    assert ("b", "summary") in rf_edges
