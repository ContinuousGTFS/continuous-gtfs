"""Checks: the step that reads the output and emits findings and cannot
change the data (specs/transform-framework.md §Checks, §Checks phase,
§Read-only by construction, §Dev-time mutation detection)."""

from __future__ import annotations

import logging
import sys
import textwrap
import threading
import time
from pathlib import Path

import polars as pl
import pytest

from continuous_gtfs import (
    Check,
    PipelineContext,
    Step,
    check,
    resolve_dag,
    scan_pipeline,
    step,
)
from continuous_gtfs.executor import (
    CHECK_GUARD_ENV,
    CHECK_MUTATED_VIEW,
    CHECK_WORKERS_ENV,
    PipelineExecutor,
    default_check_workers,
)
from continuous_gtfs.scanner import select_with_ancestors
from continuous_gtfs.testing import gtfs_df, trip_updates_feed


@pytest.fixture(autouse=True)
def _clean_sys():
    path_copy = sys.path[:]
    mods_copy = set(sys.modules)
    yield
    sys.path[:] = path_copy
    for key in set(sys.modules) - mods_copy:
        del sys.modules[key]


def _named(**steps: Step) -> list[Step]:
    for name, s in steps.items():
        s.name = name
    return list(steps.values())


def _stops() -> pl.DataFrame:
    return gtfs_df(
        """\
        stop_id,stop_name
        S1,One
        S2,Two
        """
    )


# --- Declaration ----------------------------------------------------------


def test_roles_follow_the_declaration_form():
    @step()
    def transform(ctx):
        pass

    @check()
    def a_check(ctx):
        pass

    @check
    def bare_check(ctx):
        pass

    class MyCheck(Check):
        def apply(self, ctx):
            pass

    assert transform.role == "transform"
    assert Step().role == "transform"
    assert a_check.role == "check"
    assert bare_check.role == "check"
    assert MyCheck().role == "check"
    assert a_check.is_check and not transform.is_check
    # A @check function is custom code, a Check subclass a builtin.
    assert not a_check.is_builtin
    assert MyCheck().is_builtin
    assert MyCheck().builtin_name == "MyCheck"


@pytest.mark.parametrize("kw", ["after", "before"])
def test_check_decorator_rejects_ordering(kw):
    anchor = Step()
    with pytest.raises(ValueError, match=rf"Check positioned declares {kw}="):

        @check(**{kw: [anchor]})
        def positioned(ctx):
            pass


@pytest.mark.parametrize("value", ["*", []])
def test_check_decorator_rejects_wildcards_but_not_empty(value):
    if value == "*":
        with pytest.raises(ValueError, match="takes no before/after"):

            @check(after=value)
            def wild(ctx):
                pass
    else:

        @check(after=value)
        def fine(ctx):
            pass

        assert fine.role == "check"


@pytest.mark.parametrize("kw", ["after", "before"])
def test_check_class_rejects_ordering(kw):
    class MyCheck(Check):
        pass

    with pytest.raises(ValueError, match=rf"Check MyCheck .*declares {kw}="):
        MyCheck(**{kw: "*"})


def test_scan_fails_when_a_check_declares_after(tmp_path):
    (tmp_path / "checks.py").write_text(
        textwrap.dedent(
            """\
            from continuous_gtfs import check, step

            @step()
            def t(ctx):
                pass

            @check(after="*")
            def summary(ctx):
                pass
            """
        )
    )
    with pytest.raises(ValueError, match="Check summary declares after="):
        scan_pipeline(tmp_path)


def test_scan_fails_when_a_transform_orders_against_a_check(tmp_path):
    (tmp_path / "steps.py").write_text(
        textwrap.dedent(
            """\
            from continuous_gtfs import check, step

            @check()
            def verify(ctx):
                pass

            @step(after=[verify])
            def late(ctx):
                pass
            """
        )
    )
    with pytest.raises(ValueError, match=r"Step late names check verify in its after"):
        scan_pipeline(tmp_path)


def test_resolve_rejects_a_transform_ordering_against_a_check():
    @check()
    def verify(ctx):
        pass

    @step(before=[verify])
    def early(ctx):
        pass

    with pytest.raises(ValueError, match=r"names check verify in its before"):
        resolve_dag(_named(verify=verify, early=early))


def _write_pipeline(tmp_path: Path, body: str) -> Path:
    (tmp_path / "steps.py").write_text(textwrap.dedent(body))
    return tmp_path


def test_cli_suggests_check_for_findings_only_file_less_step(tmp_path, caplog, capsys):
    _write_pipeline(
        tmp_path,
        """\
        from continuous_gtfs import check, step

        @step(findings=["looks_off"])
        def guard(ctx):
            pass

        @step(files=["stops.txt"], findings=["count_guard"])
        def with_files(ctx):
            pass

        @step()
        def plain(ctx):
            pass

        @check(findings=["already"])
        def real_check(ctx):
            pass
        """,
    )
    from continuous_gtfs.cli import _warn_likely_checks
    from continuous_gtfs.scanner import likely_checks

    # The scan itself — what worker startup runs — stays quiet.
    with caplog.at_level(logging.DEBUG):
        steps = scan_pipeline(tmp_path)
    assert [r.getMessage() for r in caplog.records if "@check" in r.getMessage()] == []
    assert likely_checks(steps) == ["guard"]
    # It still loads and runs as a transform.
    assert {s.name: s.role for s in steps}["guard"] == "transform"

    # The CLI prints the nudge.
    _warn_likely_checks(steps)
    err = capsys.readouterr().err.strip().splitlines()
    assert err == [
        "warning: step guard declares findings and operates on no files; if it "
        "only reads the output and reports findings, declare it with @check so "
        "it runs after every transform against a read-only view."
    ]


# --- Resolution -----------------------------------------------------------


def test_checks_resolve_after_every_transform_by_name():
    @step(before="*")
    def init(ctx):
        pass

    @step()
    def middle(ctx):
        pass

    @step(after="*")
    def finalize(ctx):
        pass

    @check(priority=1)
    def zeta(ctx):
        pass

    @check(priority=999)
    def alpha(ctx):
        pass

    order = resolve_dag(
        _named(zeta=zeta, finalize=finalize, alpha=alpha, middle=middle, init=init)
    )
    # after="*" ranges over transforms; checks follow it, by name — priority
    # has no effect on a check.
    assert [s.name for s in order] == ["init", "middle", "finalize", "alpha", "zeta"]


def test_selecting_a_check_selects_every_transform():
    @step()
    def t1(ctx):
        pass

    @step(after=[t1])
    def t2(ctx):
        pass

    @step()
    def t3(ctx):
        pass

    @check()
    def c1(ctx):
        pass

    @check()
    def c2(ctx):
        pass

    order = resolve_dag(_named(t1=t1, t2=t2, t3=t3, c1=c1, c2=c2))
    assert [s.name for s in select_with_ancestors(order, "c2")] == [
        "t1",
        "t2",
        "t3",
        "c2",
    ]
    assert [s.name for s in select_with_ancestors(order, "t2")] == ["t1", "t2"]


# --- Checks phase ---------------------------------------------------------


def test_checks_run_concurrently_and_record_in_name_order():
    """Both checks must be inside apply() at once to pass the barrier — a
    serial executor times out — and the one that finishes first (zz_fast)
    is still recorded after aa_slow."""
    barrier = threading.Barrier(2, timeout=5)
    finished: list[str] = []

    @check(findings=["seen"])
    def aa_slow(ctx):
        barrier.wait()
        time.sleep(0.05)
        ctx.emit_finding("seen", message="slow-1")
        ctx.emit_finding("seen", message="slow-2")
        finished.append("aa_slow")

    @check(findings=["seen"])
    def zz_fast(ctx):
        barrier.wait()
        ctx.emit_finding("seen", message="fast")
        finished.append("zz_fast")

    ctx = PipelineContext(output={"stops.txt": _stops()})
    result = PipelineExecutor(check_workers=2).execute(
        resolve_dag(_named(zz_fast=zz_fast, aa_slow=aa_slow)), ctx
    )
    assert finished == ["zz_fast", "aa_slow"]
    assert [r.name for r in result.steps] == ["aa_slow", "zz_fast"]
    assert all(r.status == "success" for r in result.steps)
    assert [f["message"] for f in ctx.findings] == ["slow-1", "slow-2", "fast"]
    assert [f["step"] for f in ctx.findings] == ["aa_slow", "aa_slow", "zz_fast"]
    for r in result.steps:
        assert r.started_at is not None and r.completed_at is not None
        assert r.duration_ms > 0
        assert r.input_rows == r.output_rows == 2


def test_check_workers_knob(monkeypatch):
    monkeypatch.setenv(CHECK_WORKERS_ENV, "3")
    assert default_check_workers() == 3
    for bad in ("0", "-2", "many"):
        monkeypatch.setenv(CHECK_WORKERS_ENV, bad)
        assert default_check_workers() >= 1
        assert default_check_workers() <= 8
    monkeypatch.delenv(CHECK_WORKERS_ENV)
    import os

    assert default_check_workers() == min(8, os.cpu_count() or 1)


def test_one_worker_runs_checks_serially_in_name_order():
    seen: list[str] = []

    @check()
    def b(ctx):
        seen.append("b")

    @check()
    def a(ctx):
        seen.append("a")

    PipelineExecutor(check_workers=1).execute(
        resolve_dag(_named(b=b, a=a)), PipelineContext()
    )
    assert seen == ["a", "b"]


def test_checks_run_after_the_last_transform():
    seen: list[str] = []

    @check()
    def verify(ctx):
        seen.append(f"check sees {ctx.output['stops.txt'].height}")

    @step(after="*")
    def add_stop(ctx):
        seen.append("transform")
        ctx.output["stops.txt"] = pl.concat(
            [ctx.output["stops.txt"], gtfs_df("stop_id,stop_name\nS3,Three\n")]
        )

    ctx = PipelineContext(output={"stops.txt": _stops()})
    PipelineExecutor().execute(resolve_dag(_named(verify=verify, add=add_stop)), ctx)
    assert seen == ["transform", "check sees 3"]


def test_a_failing_check_does_not_cancel_the_others():
    @check(findings=["before_boom"])
    def boom(ctx):
        ctx.emit_finding("before_boom")
        raise KeyError("missing")

    @check(findings=["ok"])
    def other(ctx):
        ctx.emit_finding("ok")

    errors: list[str] = []
    executor = PipelineExecutor(fail_fast=True)
    executor.add_hook("on_error", lambda ctx, s, exc: errors.append(s.name))
    result = executor.execute(
        resolve_dag(_named(boom=boom, other=other)), PipelineContext()
    )
    by_name = {r.name: r for r in result.steps}
    assert by_name["boom"].status == "error"
    assert by_name["boom"].error_type == "KeyError"
    assert by_name["boom"].role == "check"
    assert [f["code"] for f in by_name["boom"].findings] == [
        "before_boom",
        "check_failed",
    ]
    failed = by_name["boom"].findings[1]
    assert failed["severity"] == "error"
    assert failed["context"] == {"error_type": "KeyError"}
    assert failed["message"] == "Check boom failed: KeyError: 'missing'"
    assert failed["undeclared"] is False
    assert by_name["other"].status == "success"
    assert errors == ["boom"]
    # A check failure never fails the run (specs/transform-framework.md
    # §Checks phase).
    assert result.success is True


def test_a_failing_transform_still_fails_the_run():
    @step()
    def broken(ctx):
        raise RuntimeError("nope")

    result = PipelineExecutor(fail_fast=False).execute(
        resolve_dag(_named(broken=broken)), PipelineContext()
    )
    assert result.success is False


def test_fail_fast_transform_failure_skips_the_checks_phase():
    ran: list[str] = []

    @step()
    def broken(ctx):
        raise RuntimeError("nope")

    @check()
    def verify(ctx):
        ran.append("verify")

    result = PipelineExecutor(fail_fast=True).execute(
        resolve_dag(_named(broken=broken, verify=verify)), PipelineContext()
    )
    assert ran == []
    assert [r.name for r in result.steps] == ["broken"]

    PipelineExecutor(fail_fast=False).execute(
        resolve_dag(_named(broken=broken, verify=verify)), PipelineContext()
    )
    assert ran == ["verify"]


def test_disabled_checks_are_skipped_in_order():
    @check()
    def a(ctx):
        pass

    @check(enabled=False)
    def b(ctx):
        pass

    @check()
    def c(ctx):
        pass

    after: list[tuple[str, str]] = []
    executor = PipelineExecutor()
    executor.add_hook("after_step", lambda ctx, s, r: after.append((s.name, r.status)))
    executor.execute(
        resolve_dag(_named(c=c, b=b, a=a)), PipelineContext(), disabled_steps=["c"]
    )
    assert after == [("a", "success"), ("b", "skipped"), ("c", "skipped")]


def test_check_hooks_fire_on_the_executor_thread():
    main = threading.get_ident()
    calls: list[tuple[str, str, bool]] = []

    @check()
    def a(ctx):
        pass

    @check()
    def b(ctx):
        pass

    executor = PipelineExecutor(check_workers=2)
    executor.add_hook(
        "before_step",
        lambda ctx, s: calls.append(("before", s.name, threading.get_ident() == main)),
    )
    executor.add_hook(
        "after_step",
        lambda ctx, s, r: calls.append(
            ("after", s.name, threading.get_ident() == main)
        ),
    )
    executor.execute(resolve_dag(_named(b=b, a=a)), PipelineContext())
    assert calls == [
        ("before", "a", True),
        ("before", "b", True),
        ("after", "a", True),
        ("after", "b", True),
    ]


def test_logs_are_captured_per_check_under_concurrency():
    barrier = threading.Barrier(2, timeout=5)
    log = logging.getLogger("agency.checks")

    @check()
    def a(ctx):
        barrier.wait()
        for i in range(20):
            log.warning("a-%d", i)

    @check()
    def b(ctx):
        barrier.wait()
        for i in range(20):
            log.warning("b-%d", i)

    result = PipelineExecutor(check_workers=2).execute(
        resolve_dag(_named(a=a, b=b)), PipelineContext()
    )
    by_name = {r.name: [e["message"] for e in r.logs] for r in result.steps}
    assert by_name["a"] == [f"a-{i}" for i in range(20)]
    assert by_name["b"] == [f"b-{i}" for i in range(20)]


def test_a_checks_return_value_is_ignored():
    @check()
    def returns(ctx):
        return {"stops.txt": None}

    ctx = PipelineContext(output={"stops.txt": _stops()})
    PipelineExecutor().execute(resolve_dag(_named(returns=returns)), ctx)
    assert ctx.output["stops.txt"].height == 2


# --- Read-only by construction ---------------------------------------------


def test_a_check_cannot_reach_the_real_output():
    """Every route a check could take to the data: the mapping, an in-place
    DataFrame method, a FeedMessage edit, reassigning ctx.output, and the
    other mutable context fields. None of it reaches the run."""
    feed = trip_updates_feed(trip_id="T1", stop_ids=["S1", "S2"])
    stops = _stops()
    attempts: dict[str, str] = {}

    @check()
    def vandal(ctx):
        try:
            ctx.output["stops.txt"] = None
        except TypeError:
            attempts["setitem"] = "TypeError"
        try:
            del ctx.output["stops.txt"]
        except TypeError:
            attempts["delitem"] = "TypeError"
        ctx.output["stops.txt"].drop_in_place("stop_name")
        ctx.output["trip_updates"].entity[0].trip_update.trip.trip_id = "HACKED"
        del ctx.output["trip_updates"].entity[:]
        ctx.metadata["leak"] = True
        ctx.config["leak"] = True
        ctx.id_mappings.setdefault("stops.txt.stop_id", {})["S1"] = None
        ctx.output = {}

    @check()
    def witness(ctx):
        attempts["witness"] = ",".join(ctx.output["stops.txt"].columns)

    ctx = PipelineContext(
        output={"stops.txt": stops, "trip_updates": feed}, config={"k": "v"}
    )
    result = PipelineExecutor(check_guard=False, check_workers=1).execute(
        resolve_dag(_named(vandal=vandal, witness=witness)), ctx
    )
    assert all(r.status == "success" for r in result.steps)
    assert attempts == {
        "setitem": "TypeError",
        "delitem": "TypeError",
        # vandal ran first (name order) and the next check still sees both
        # columns: each check's view is its own.
        "witness": "stop_id,stop_name",
    }
    assert ctx.output["stops.txt"] is stops
    assert stops.columns == ["stop_id", "stop_name"]
    assert ctx.output["trip_updates"] is feed
    assert len(feed.entity) == 1
    assert feed.entity[0].trip_update.trip.trip_id == "T1"
    assert ctx.metadata == {}
    assert ctx.config == {"k": "v"}
    assert ctx.id_mappings == {}


def test_check_context_shares_inputs_and_environment():
    seen: dict[str, object] = {}

    @check()
    def peek(ctx):
        seen["env"] = ctx.environment
        seen["inputs"] = ctx.inputs

    ctx = PipelineContext(inputs={"x": b"raw"}, environment="staging")
    PipelineExecutor().execute(resolve_dag(_named(peek=peek)), ctx)
    assert seen["env"] == "staging"
    # The check's inputs are its own read-only copy; immutable values shared.
    assert seen["inputs"] is not ctx.inputs
    assert dict(seen["inputs"]) == {"x": b"raw"}


def _aliasing_pipeline():
    """The init-step aliasing the review found: the output is seeded from the
    input, so output and input hold the same DataFrame / FeedMessage."""

    @step(before="*")
    def init(ctx):
        ctx.output = dict(ctx.inputs["sched"])
        ctx.output["trip_updates"] = ctx.inputs["rt"]

    @check()
    def aa_vandal(ctx):
        ctx.inputs["sched"]["stops.txt"].drop_in_place("stop_name")
        del ctx.inputs["rt"].entity[:]

    @check()
    def zz_innocent(ctx):
        ctx.output["stops.txt"].height  # noqa: B018 — a read

    return _named(init=init, aa_vandal=aa_vandal, zz_innocent=zz_innocent)


def _aliasing_ctx():
    return PipelineContext(
        inputs={
            "sched": {"stops.txt": _stops()},
            "rt": trip_updates_feed(trip_id="T1", stop_ids=["S1"]),
        }
    )


@pytest.mark.parametrize("workers", [1, 2])
def test_a_check_cannot_reach_the_output_through_aliased_inputs(workers):
    ctx = _aliasing_ctx()
    result = PipelineExecutor(check_guard=False, check_workers=workers).execute(
        resolve_dag(_aliasing_pipeline()), ctx
    )
    assert all(r.status == "success" for r in result.steps)
    assert ctx.output["stops.txt"].columns == ["stop_id", "stop_name"]
    assert len(ctx.output["trip_updates"].entity) == 1
    assert ctx.inputs["sched"]["stops.txt"].columns == ["stop_id", "stop_name"]


@pytest.mark.parametrize("workers", [1, 2])
def test_the_guard_never_blames_an_innocent_sibling(workers):
    ctx = _aliasing_ctx()
    result = PipelineExecutor(check_guard=True, check_workers=workers).execute(
        resolve_dag(_aliasing_pipeline()), ctx
    )
    by_name = {r.name: r for r in result.steps}
    assert by_name["zz_innocent"].findings == []
    # The vandal edited only its own input copies: nothing to report either.
    assert by_name["aa_vandal"].findings == []


def test_the_guard_compares_against_a_snapshot_taken_before_the_phase():
    """Even if something did reach the real output mid-phase, a check whose
    own view is unchanged is not blamed: verdicts read the pre-phase
    snapshot, not the live output."""
    real_stops = _stops()

    @check()
    def aa_reaches_real(ctx):
        # Stand-in for any leak the views don't cover.
        real_stops.drop_in_place("stop_name")

    @check()
    def zz_innocent(ctx):
        pass

    ctx = PipelineContext(output={"stops.txt": real_stops})
    result = PipelineExecutor(check_guard=True, check_workers=1).execute(
        resolve_dag(_named(aa_reaches_real=aa_reaches_real, zz_innocent=zz_innocent)),
        ctx,
    )
    assert {r.name: r.findings for r in result.steps}["zz_innocent"] == []


def test_a_view_that_cannot_be_built_is_that_checks_error():
    class Uncopyable:
        def __deepcopy__(self, memo):
            raise RuntimeError("no deep copy")

        def __copy__(self):
            raise RuntimeError("no copy")

    @check()
    def verify(ctx):
        pass

    before: list[str] = []
    after: list[tuple[str, str]] = []
    executor = PipelineExecutor(check_guard=False)
    executor.add_hook("before_step", lambda ctx, s: before.append(s.name))
    executor.add_hook("after_step", lambda ctx, s, r: after.append((s.name, r.status)))
    result = executor.execute(
        resolve_dag(_named(verify=verify)), PipelineContext(output={"x": Uncopyable()})
    )
    assert before == ["verify"]
    assert after == [("verify", "error")]
    (sr,) = result.steps
    assert sr.error_type == "RuntimeError"
    assert [f["code"] for f in sr.findings] == ["check_failed"]
    assert result.success is True


def test_guard_equality_recurses_into_mappings_and_series():
    from continuous_gtfs.executor import _changed_keys

    @check()
    def honest(ctx):
        pass

    ctx = PipelineContext(
        output={
            "tables": {"stops.txt": _stops(), "nested": {"routes.txt": _stops()}},
            "series": pl.Series("x", [1, 2]),
            "rows": [_stops()],
        }
    )
    result = PipelineExecutor(check_guard=True).execute(
        resolve_dag(_named(honest=honest)), ctx
    )
    assert result.steps[0].findings == []
    # And real differences inside them are still seen.
    changed = {
        "tables": {
            "stops.txt": _stops().drop("stop_name"),
            "nested": {"routes.txt": _stops()},
        },
        "series": pl.Series("x", [1, 3]),
        "rows": [_stops()],
    }
    assert _changed_keys(changed, ctx.output) == ["series", "tables"]


def test_reserved_codes_cannot_be_declared():
    with pytest.warns(UserWarning, match="reserves for checks"):

        @check(
            findings=[
                "check_mutated_view",
                "real_code",
                ("check_failed", {"subject": []}),
            ]
        )
        def verify(ctx):
            pass

    assert verify.declared_findings == {"real_code": []}


# --- Dev-time mutation detection -----------------------------------------


def _mutating_checks():
    @check()
    def edits_stops(ctx):
        ctx.output["stops.txt"].drop_in_place("stop_name")

    @check()
    def edits_feed(ctx):
        ctx.output["trip_updates"].entity[0].id = "changed"

    @check()
    def reassigns(ctx):
        ctx.output = {"stops.txt": ctx.output["stops.txt"]}

    @check()
    def honest(ctx):
        ctx.output["stops.txt"].height  # noqa: B018 — a read

    return _named(
        edits_stops=edits_stops,
        edits_feed=edits_feed,
        reassigns=reassigns,
        honest=honest,
    )


def _guarded_run(**executor_kw):  # noqa: D401
    ctx = PipelineContext(
        output={
            "stops.txt": _stops(),
            "trip_updates": trip_updates_feed(trip_id="T1", stop_ids=["S1"]),
        }
    )
    result = PipelineExecutor(**executor_kw).execute(
        resolve_dag(_mutating_checks()), ctx
    )
    return ctx, {r.name: r for r in result.steps}


def test_guard_reports_a_check_that_changed_its_view():
    ctx, by_name = _guarded_run(check_guard=True)
    expected = {
        "edits_stops": "stops.txt",
        "edits_feed": "trip_updates",
        "reassigns": "trip_updates",
    }
    for name, files in expected.items():
        (finding,) = by_name[name].findings
        assert finding["code"] == CHECK_MUTATED_VIEW
        assert finding["severity"] == "error"
        assert finding["context"] == {"files": files}
        assert finding["step"] == name
        assert finding["undeclared"] is False
        assert finding["message"] == (
            f"Check {name} changed its view of the output ({files}). A check "
            "cannot change the data, so the run's output is unaffected; if the "
            "step needs to change the output, make it a transform."
        )
        # Findings never gate a run.
        assert by_name[name].status == "success"
    assert by_name["honest"].findings == []
    assert [f["step"] for f in ctx.findings] == [
        "edits_feed",
        "edits_stops",
        "reassigns",
    ]


def test_guard_is_off_by_default_and_on_with_the_env_var(monkeypatch):
    monkeypatch.delenv(CHECK_GUARD_ENV, raising=False)
    _, by_name = _guarded_run()
    assert all(r.findings == [] for r in by_name.values())

    monkeypatch.setenv(CHECK_GUARD_ENV, "1")
    _, by_name = _guarded_run()
    assert by_name["edits_stops"].findings[0]["code"] == CHECK_MUTATED_VIEW

    # An explicit False beats the env var (a library caller opting out).
    _, by_name = _guarded_run(check_guard=False)
    assert by_name["edits_stops"].findings == []


def test_cli_runs_enable_the_guard(tmp_path, monkeypatch, capsys):
    """`continuous-gtfs realtime` reports a mutating check with no env var."""
    from continuous_gtfs.cli import main

    monkeypatch.delenv(CHECK_GUARD_ENV, raising=False)
    pipeline = tmp_path / "rt"
    pipeline.mkdir()
    (pipeline / "__init__.py").write_text(
        'FEED_TYPE = "realtime"\nINPUTS = {"tu": "gtfs_rt_protobuf"}\n'
    )
    (pipeline / "steps.py").write_text(
        textwrap.dedent(
            """\
            from continuous_gtfs import check, step

            @step(before="*")
            def init(ctx):
                ctx.output["trip_updates"] = ctx.inputs["tu"]

            @check()
            def sneaky(ctx):
                del ctx.output["trip_updates"].entity[:]
            """
        )
    )
    pb = tmp_path / "tu.pb"
    pb.write_bytes(trip_updates_feed(trip_id="T1", stop_ids=["S1"]).SerializeToString())
    monkeypatch.setattr(
        sys,
        "argv",
        ["continuous-gtfs", "realtime", str(pipeline), "--input", f"tu={pb}"],
    )
    main()
    out = capsys.readouterr().out
    assert "check_mutated_view (sneaky)" in out
    assert "trip_updates: 1 entities" in out


def test_verbose_cli_names_each_result_and_labels_checks(capsys):
    from continuous_gtfs.cli import _build_verbose_cli_executor, _print_step_result

    @step()
    def t(ctx):
        pass

    @check()
    def b_check(ctx):
        pass

    @check()
    def a_check(ctx):
        pass

    steps = resolve_dag(_named(t=t, b_check=b_check, a_check=a_check))
    result = _build_verbose_cli_executor().execute(steps, PipelineContext())
    lines = [line.strip() for line in capsys.readouterr().out.splitlines()]
    assert lines[0] == "▸ t [@step]"
    assert lines[1].startswith("t: ") and lines[1].endswith("[ok]")
    # Both checks start together, then each result names its check.
    assert lines[2:4] == ["▸ a_check [@check]", "▸ b_check [@check]"]
    assert lines[4].startswith("a_check: ") and lines[5].startswith("b_check: ")

    _print_step_result(result.steps[1], steps)
    assert "a_check [@check]" in capsys.readouterr().out
