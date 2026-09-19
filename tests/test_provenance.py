"""Step definition provenance — specs/transform-framework.md §Step Snapshot
at Registration, "Step identity across images"."""

import json
import sys
import textwrap
from pathlib import Path

import pytest

from continuous_gtfs import Step, scan_pipeline, step
from continuous_gtfs.builtins.schedule import MatchCondition, RemoveRows
from continuous_gtfs.scanner import export_reactflow
from continuous_gtfs.step import definition_hash, source_text_hash


@pytest.fixture(autouse=True)
def _clean_sys():
    path_copy = sys.path[:]
    mods_copy = set(sys.modules)
    yield
    sys.path[:] = path_copy
    for key in set(sys.modules) - mods_copy:
        del sys.modules[key]


def _write(folder: Path, name: str, content: str) -> Path:
    p = folder / name
    p.write_text(textwrap.dedent(content))
    return p


# --- @step functions -------------------------------------------------------


def test_step_function_hash_is_stable_across_reindent_and_trailing_space():
    a = source_text_hash("def f(ctx):\n    return 1\n")
    b = source_text_hash("    def f(ctx):\n        return 1   \n")
    assert a == b


def test_step_function_hash_changes_with_the_body():
    a = source_text_hash("def f(ctx):\n    return 1\n")
    b = source_text_hash("def f(ctx):\n    return 2\n")
    assert a != b


def test_decorator_sets_source_hash():
    @step(files=["stops.txt"])
    def rename(ctx):
        pass

    assert rename.source_hash is not None
    assert rename.source_line is not None


# --- builtin instances -----------------------------------------------------


def test_identical_builtins_hash_the_same_and_different_conditions_differ():
    a = RemoveRows("stops.txt", [MatchCondition("stop_id", value="N13")])
    b = RemoveRows("stops.txt", [MatchCondition("stop_id", value="N13")])
    c = RemoveRows("stops.txt", [MatchCondition("stop_id", value="N14")])
    a.name, b.name = "first", "second"
    assert definition_hash(a) == definition_hash(b)
    assert definition_hash(a) != definition_hash(c)


def test_builtin_description_is_part_of_the_definition():
    a = RemoveRows(
        "stops.txt", [MatchCondition("stop_id", value="N13")], description="x"
    )
    b = RemoveRows(
        "stops.txt", [MatchCondition("stop_id", value="N13")], description="y"
    )
    assert definition_hash(a) != definition_hash(b)


def test_definition_hash_never_raises_on_unserializable_state():
    class Odd(Step):
        def __init__(self):
            super().__init__()
            self.thing = object()

    h1 = definition_hash(Odd())
    h2 = definition_hash(Odd())
    assert h1 == h2  # a bare object reduces to its type name, not its address


# --- scanner stamping ------------------------------------------------------


def _two_module_pipeline(root: Path) -> Path:
    folder = root / "pipelines" / "schedule"
    folder.mkdir(parents=True)
    _write(
        folder,
        "__init__.py",
        "FEED_TYPE = 'schedule'\nINPUTS = {'schedule': 'gtfs_schedule_zip'}\n",
    )
    _write(
        folder,
        "removals.py",
        """\
        from continuous_gtfs.builtins.schedule import MatchCondition, RemoveRows
        LIMIT = 3
        remove_a = RemoveRows("stops.txt", [MatchCondition("stop_id", value="A")])
        remove_b = RemoveRows("stops.txt", [MatchCondition("stop_id", value="B")])
        """,
    )
    _write(
        folder,
        "checks.py",
        """\
        from continuous_gtfs import step

        @step(files=["stops.txt"])
        def check_names(ctx):
            pass
        """,
    )
    return folder


def test_scan_stamps_root_relative_source_file_and_hashes(tmp_path):
    folder = _two_module_pipeline(tmp_path)
    steps = {s.name: s for s in scan_pipeline(folder)}
    assert steps["remove_a"].source_file == "pipelines/schedule/removals.py"
    assert steps["check_names"].source_file == "pipelines/schedule/checks.py"
    # A builtin's line is where it was constructed; a function's is its def.
    assert steps["remove_a"].source_line == 3
    assert steps["remove_b"].source_line == 4
    assert steps["check_names"].source_line == 3
    for s in steps.values():
        assert s.source_hash and s.module_hash


def test_steps_in_one_module_share_module_hash_which_tracks_helpers(tmp_path):
    folder = _two_module_pipeline(tmp_path)
    before = {s.name: s for s in scan_pipeline(folder)}
    assert before["remove_a"].module_hash == before["remove_b"].module_hash
    assert before["remove_a"].module_hash != before["check_names"].module_hash
    assert before["remove_a"].source_hash != before["remove_b"].source_hash

    # Edit only a module-level constant: definitions unchanged, module changed.
    removals = folder / "removals.py"
    removals.write_text(removals.read_text().replace("LIMIT = 3", "LIMIT = 4"))
    for key in [k for k in sys.modules if k.startswith("_pipeline_")]:
        del sys.modules[key]
    after = {s.name: s for s in scan_pipeline(folder)}
    assert after["remove_a"].source_hash == before["remove_a"].source_hash
    assert after["remove_a"].module_hash != before["remove_a"].module_hash


def test_explicit_root_wins(tmp_path):
    folder = _two_module_pipeline(tmp_path)
    steps = {s.name: s for s in scan_pipeline(folder, root=tmp_path / "pipelines")}
    assert steps["remove_a"].source_file == "schedule/removals.py"


# --- emission --------------------------------------------------------------


def test_dag_json_carries_provenance(tmp_path):
    folder = _two_module_pipeline(tmp_path)
    steps = scan_pipeline(folder)
    by_name = {s.name: s for s in steps}
    builtin, custom = by_name["remove_a"], by_name["check_names"]
    assert builtin.source_file == "pipelines/schedule/removals.py"
    assert builtin.source_line == 3
    assert custom.source_line == 3
    assert len(builtin.source_hash) == 64 and len(builtin.module_hash) == 64

    nodes = {n["id"]: n for n in export_reactflow(steps)["nodes"]}
    assert nodes["remove_a"]["source_line"] == 3
    assert nodes["remove_a"]["source_hash"] == builtin.source_hash
    assert nodes["check_names"]["module_hash"] == custom.module_hash
    json.dumps(export_reactflow(steps))  # still serializable


# --- review round (PR #672): scan order, re-scans, unavailable source ------


def test_rescanning_cached_modules_keeps_the_hashes(tmp_path):
    folder = _two_module_pipeline(tmp_path)
    first = {s.name: (s.source_file, s.module_hash) for s in scan_pipeline(folder)}
    # No sys.modules reset: the second scan meets the SAME Step objects, whose
    # source_file is now root-relative. The hash must come from the absolute
    # location the framework kept, not from a path resolved against cwd.
    second = {s.name: (s.source_file, s.module_hash) for s in scan_pipeline(folder)}
    assert second == first
    assert all(h for _, h in second.values())


def test_an_imported_builtin_keeps_its_defining_module(tmp_path):
    folder = tmp_path / "pipelines" / "schedule"
    folder.mkdir(parents=True)
    _write(folder, "__init__.py", "FEED_TYPE = 'schedule'\nINPUTS = {}\n")
    # Sorted scan order meets a_consumer.py first, where remove_x is only
    # imported; its definition lives in z_rules.py.
    _write(
        folder,
        "z_rules.py",
        """\
        from continuous_gtfs.builtins.schedule import MatchCondition, RemoveRows
        remove_x = RemoveRows("stops.txt", [MatchCondition("stop_id", value="X")])
        """,
    )
    _write(
        folder,
        "a_consumer.py",
        """\
        from continuous_gtfs import step
        from .z_rules import remove_x

        @step(files=["stops.txt"], after=[remove_x])
        def consume(ctx):
            pass
        """,
    )
    steps = {s.name: s for s in scan_pipeline(folder)}
    z_bytes = (folder / "z_rules.py").read_bytes()
    import hashlib

    assert steps["remove_x"].source_file == "pipelines/schedule/z_rules.py"
    assert steps["remove_x"].source_line == 2
    assert steps["remove_x"].module_hash == hashlib.sha256(z_bytes).hexdigest()
    assert steps["consume"].source_file == "pipelines/schedule/a_consumer.py"


def test_a_function_without_source_stays_unknown_rather_than_hashed(tmp_path):
    folder = tmp_path / "pipelines" / "schedule"
    folder.mkdir(parents=True)
    _write(folder, "__init__.py", "FEED_TYPE = 'schedule'\nINPUTS = {}\n")
    _write(
        folder,
        "generated.py",
        """\
        from continuous_gtfs import step

        def _make(body):
            ns = {}
            exec("def f(ctx):\\n    " + body + "\\n", ns)
            return step(files=["stops.txt"])(ns["f"])

        one = _make("return 1")
        two = _make("return 2")
        """,
    )
    steps = {s.name: s for s in scan_pipeline(folder)}
    # inspect.getsource fails for exec'd code: two different bodies must not
    # collapse into one non-empty hash.
    assert steps["one"].source_hash is None
    assert steps["two"].source_hash is None
    # The module they were built in is still known and hashed.
    assert (
        steps["one"].module_hash
        and steps["one"].module_hash == steps["two"].module_hash
    )
