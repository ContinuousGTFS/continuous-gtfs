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


# --- definition text travels with the image


def _sha(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode()).hexdigest()


def test_a_step_functions_source_is_exactly_the_text_its_hash_hashes():
    @step(files=["stops.txt"])
    def rename(ctx):
        x = 1  # trailing spaces below are normalized away
        return x

    assert rename.source is not None
    assert _sha(rename.source) == rename.source_hash
    assert rename.source.startswith("@step(files=")
    assert "def rename(ctx):" in rename.source
    # Dedented (the def sat inside this test), each line right-stripped.
    assert all(line == line.rstrip() for line in rename.source.splitlines())
    assert not rename.source.startswith(" ")


def test_a_builtins_source_compacts_to_exactly_the_hashed_payload():
    a = RemoveRows("stops.txt", [MatchCondition("stop_id", value="N13")])
    from continuous_gtfs.step import definition_text

    text = definition_text(a)
    compact = json.dumps(json.loads(text), sort_keys=True, default=str)
    assert _sha(compact) == definition_hash(a)
    # One key per line, so a changed parameter diffs as its own line.
    assert len(text.splitlines()) > 3
    assert '"value": "N13"' in text


def test_builtin_definition_text_is_canonical():
    from continuous_gtfs.step import definition_text

    a = RemoveRows("stops.txt", [MatchCondition("stop_id", value="N13")])
    b = RemoveRows("stops.txt", [MatchCondition("stop_id", value="N13")])
    c = RemoveRows("stops.txt", [MatchCondition("stop_id", value="N14")])
    a.name, b.name = "first", "second"
    assert definition_text(a) == definition_text(b)
    assert definition_text(a) != definition_text(c)
    # Only the changed parameter's line differs.
    diff = set(definition_text(a).splitlines()) ^ set(definition_text(c).splitlines())
    assert diff == {'        "value": "N13"', '        "value": "N14"'}


def test_the_texts_are_never_part_of_a_definition():
    a = RemoveRows("stops.txt", [MatchCondition("stop_id", value="N13")])
    before = definition_hash(a)
    a.source = "anything"
    a.module_source = "anything else"
    assert "source" not in a.definition_params()
    assert "module_source" not in a.definition_params()
    assert definition_hash(a) == before


def test_scan_stamps_source_and_module_source_beside_their_hashes(tmp_path):
    folder = _two_module_pipeline(tmp_path)
    steps = {s.name: s for s in scan_pipeline(folder)}
    removals = (folder / "removals.py").read_text()
    checks = (folder / "checks.py").read_text()
    assert steps["remove_a"].module_source == removals
    assert steps["remove_b"].module_source == removals
    assert steps["check_names"].module_source == checks
    for s in steps.values():
        assert s.source is not None and s.module_source is not None
        assert _sha(s.module_source) == s.module_hash
    assert _sha(steps["check_names"].source) == steps["check_names"].source_hash
    compact = json.dumps(json.loads(steps["remove_a"].source), sort_keys=True)
    assert _sha(compact) == steps["remove_a"].source_hash
    assert '"value": "A"' in steps["remove_a"].source

    # Re-scanning cached modules keeps the texts with their hashes.
    again = {s.name: s for s in scan_pipeline(folder)}
    assert again["remove_a"].source == steps["remove_a"].source
    assert again["check_names"].module_source == checks


def test_a_step_without_source_carries_neither_text_nor_hash(tmp_path):
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
        """,
    )
    steps = {s.name: s for s in scan_pipeline(folder)}
    assert steps["one"].source_hash is None and steps["one"].source is None
    # Its module is still known: text present exactly with its hash.
    assert steps["one"].module_source == (folder / "generated.py").read_text()
    assert _sha(steps["one"].module_source) == steps["one"].module_hash


def _bytes_pipeline(root: Path, module: bytes) -> Path:
    folder = root / "pipelines" / "schedule"
    folder.mkdir(parents=True)
    _write(folder, "__init__.py", "FEED_TYPE = 'schedule'\nINPUTS = {}\n")
    (folder / "rules.py").write_bytes(module)
    return folder


def test_module_source_decodes_in_the_modules_declared_encoding(tmp_path):
    import hashlib

    raw = (
        "# -*- coding: latin-1 -*-\n"
        "from continuous_gtfs.builtins.schedule import MatchCondition, RemoveRows\n"
        "NAME = 'Montréal'\n"
        "remove_x = RemoveRows('stops.txt', [MatchCondition('stop_id', value='X')])\n"
    ).encode("latin-1")
    folder = _bytes_pipeline(tmp_path, raw)
    s = {s.name: s for s in scan_pipeline(folder)}["remove_x"]
    assert s.module_hash == hashlib.sha256(raw).hexdigest()
    assert s.module_source == raw.decode("latin-1")
    assert "Montréal" in s.module_source


def test_undecodable_module_bytes_leave_text_and_hash_both_absent(tmp_path):
    from continuous_gtfs.scanner import _decode_module

    # Declared UTF-8 (the default) but not valid UTF-8: Python would refuse
    # to import it too, so the decoder is exercised directly.
    with pytest.raises(UnicodeDecodeError):
        _decode_module(b"X = 1\nY = 2\nZ = '\xff\xfe'\n")
    with pytest.raises(SyntaxError):
        _decode_module(b"X = '\xff\xfe'\n")
    with pytest.raises(SyntaxError):
        _decode_module(b"# -*- coding: no-such-codec -*-\nX = 1\n")


def test_builtin_source_escapes_non_ascii_and_still_compacts_to_the_hash():
    from continuous_gtfs.step import definition_text

    a = RemoveRows("stops.txt", [MatchCondition("stop_name", value="Montréal")])
    text = definition_text(a)
    assert "\\u00e9" in text and "é" not in text
    compact = json.dumps(
        json.loads(text), sort_keys=True, separators=(", ", ": "), ensure_ascii=True
    )
    assert _sha(compact) == definition_hash(a)


def test_a_step_in_an_undecodable_module_reports_neither_module_text_nor_hash(tmp_path):
    from continuous_gtfs.scanner import _stamp_provenance

    bad = tmp_path / "bad.py"
    bad.write_bytes(b"X = 1\nY = 2\nZ = '\xff'\n")
    s = RemoveRows("stops.txt", [MatchCondition("stop_id", value="X")])
    s.name = "remove_x"
    s.source_path = str(bad)
    _stamp_provenance(s, tmp_path, {})
    assert s.module_hash is None and s.module_source is None
    # Its own definition is unaffected.
    assert s.source_hash is not None and s.source is not None
