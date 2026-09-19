"""Tests for the step-side findings surface.

Covers the emission contract from specs/transform-framework.md §Findings: the
declaration vocabulary, `ctx.emit_finding`'s never-raise / never-gate
guarantees, the executor draining findings into StepResult, and `error_type`
capture.
"""

from __future__ import annotations

import pytest

from continuous_gtfs import PipelineContext, Step, execute_pipeline, step
from continuous_gtfs.executor import PipelineExecutor
from continuous_gtfs.step import normalize_findings
from continuous_gtfs.testing import gtfs_df, schedule_context

# --- Declaration ---


def test_bare_code_declares_no_subject():
    @step(findings=["schedule_realtime_drift"])
    def s(ctx):
        pass

    assert s.declared_findings == {"schedule_realtime_drift": []}


def test_pair_form_declares_subject_keys():
    @step(
        files=["trips.txt"],
        findings=[
            ("trip_missing_from_realtime", {"subject": ["trip_id"]}),
            "schedule_realtime_drift",
        ],
    )
    def s(ctx):
        pass

    assert s.declared_findings == {
        "trip_missing_from_realtime": ["trip_id"],
        "schedule_realtime_drift": [],
    }


def test_builtin_class_attribute_declaration():
    class Checker(Step):
        files = ["stops.txt"]
        findings = [("stop_outside_service_area", {"subject": ["stop_id"]})]

        def apply(self, ctx):
            pass

    assert Checker().declared_findings == {"stop_outside_service_area": ["stop_id"]}


def test_malformed_declaration_degrades_to_no_subject():
    with pytest.warns(UserWarning, match="Malformed findings declaration"):
        assert normalize_findings([("code", ["not", "a", "dict"])]) == {"code": []}
    with pytest.warns(UserWarning):
        assert normalize_findings([("code", {"subject": [1, 2]})]) == {"code": []}
    with pytest.warns(UserWarning):
        assert normalize_findings([123]) == {}


def test_string_subject_is_accepted_as_one_key():
    assert normalize_findings([("code", {"subject": "stop_id"})]) == {
        "code": ["stop_id"]
    }


def test_declaration_rides_the_reactflow_export():
    from continuous_gtfs import export_reactflow, resolve_dag

    @step(findings=[("a", {"subject": ["stop_id"]})])
    def declares(ctx):
        pass

    declares.name = "declares"
    graph = export_reactflow(resolve_dag([declares]))
    assert graph["nodes"][0]["findings"] == [{"code": "a", "subject": ["stop_id"]}]


# --- Emission ---


def test_emit_finding_collects_on_the_context():
    ctx = schedule_context(**{"stops.txt": gtfs_df("stop_id\nS1\n")})
    ctx.emit_finding(
        "stop_outside_service_area",
        severity="warning",
        message="Stop S1 is outside the service area",
        context={"stop_id": "S1", "lat": 47.6},
    )
    assert len(ctx.findings) == 1
    f = ctx.findings[0]
    assert f["code"] == "stop_outside_service_area"
    assert f["severity"] == "warning"
    # Non-string context values are stringified rather than rejected, and the
    # coercion is recorded.
    assert f["context"] == {"stop_id": "S1", "lat": "47.6"}
    assert any("stringified" in e for e in f["emit_errors"])
    assert f["occurrence_count"] == 1


def test_emit_finding_defaults_to_warning():
    ctx = PipelineContext()
    ctx.emit_finding("x")
    assert ctx.findings[0]["severity"] == "warning"


def test_invalid_severity_is_coerced_not_raised():
    ctx = PipelineContext()
    ctx.emit_finding("x", severity="critical", message="m")
    f = ctx.findings[0]
    assert f["severity"] == "warning"
    assert any("critical" in e for e in f["emit_errors"])


def test_emit_finding_never_raises_on_garbage():
    ctx = PipelineContext()
    # Bad code, bad occurrence count, unhashable-ish context keys — all
    # normalized, nothing raised.
    ctx.emit_finding(  # type: ignore[arg-type]
        None,
        severity=object(),
        message=42,
        context={1: None},
        occurrence_count="x",
    )
    f = ctx.findings[0]
    assert f["code"] == "invalid_finding_code"
    assert f["severity"] == "warning"
    assert f["message"] == "42"
    assert f["context"] == {"1": ""}
    assert f["occurrence_count"] == 1
    assert len(f["emit_errors"]) >= 3


def test_repeat_emission_is_two_occurrences_not_deduped_locally():
    ctx = PipelineContext()
    ctx.emit_finding("x", context={"stop_id": "S1"})
    ctx.emit_finding("x", context={"stop_id": "S1"})
    assert len(ctx.findings) == 2


def test_occurrence_count_carries_a_precomputed_total():
    ctx = PipelineContext()
    ctx.emit_finding("x", occurrence_count=4000)
    assert ctx.findings[0]["occurrence_count"] == 4000


# --- Executor integration ---


def test_executor_drains_findings_into_step_result():
    @step(findings=[("bad_stop", {"subject": ["stop_id"]})])
    def emitter(ctx):
        ctx.emit_finding("bad_stop", message="S1 is bad", context={"stop_id": "S1"})

    @step()
    def quiet(ctx):
        pass

    emitter.name = "emitter"
    quiet.name = "quiet"
    ctx = PipelineContext()
    result = execute_pipeline([emitter, quiet], ctx)

    assert [len(sr.findings) for sr in result.steps] == [1, 0]
    assert result.steps[0].findings[0]["step"] == "emitter"
    assert result.steps[0].findings[0]["undeclared"] is False
    # A step that emits nothing carries an empty list — no allocation beyond it.
    assert result.steps[1].findings == []
    # And the context keeps the cumulative record for local rendering.
    assert len(ctx.findings) == 1


def test_undeclared_code_is_flagged_but_still_emitted():
    @step(findings=["declared"])
    def emitter(ctx):
        ctx.emit_finding("not_declared", message="oops")

    emitter.name = "emitter"
    result = execute_pipeline([emitter], PipelineContext())
    f = result.steps[0].findings[0]
    assert f["code"] == "not_declared"
    assert f["undeclared"] is True
    # …and the run is unaffected.
    assert result.success


def test_emission_never_changes_the_run_status():
    @step(findings=["ok"])
    def many(ctx):
        for i in range(10_000):
            ctx.emit_finding("ok", context={"i": str(i)})

    @step()
    def invalid_severity(ctx):
        ctx.emit_finding("x", severity="nope")

    @step()
    def undeclared(ctx):
        ctx.emit_finding("never_declared")

    for s, name in ((many, "many"), (invalid_severity, "sev"), (undeclared, "und")):
        s.name = name
        result = execute_pipeline([s], PipelineContext())
        assert result.success, name

    result = execute_pipeline([many], PipelineContext())
    assert len(result.steps[0].findings) == 10_000


def test_error_type_is_captured_separately_from_the_message():
    class TariffError(ValueError):
        pass

    @step()
    def boom(ctx):
        raise TariffError("row 41821 of stop_times.txt at 2026-07-30T00:00:02Z")

    boom.name = "boom"
    result = execute_pipeline([boom], PipelineContext())
    sr = result.steps[0]
    assert sr.status == "error"
    assert sr.error_type == "TariffError"
    assert "41821" in (sr.error or "")


def test_findings_emitted_before_a_raise_are_still_reported():
    @step(findings=["partial"])
    def emit_then_raise(ctx):
        ctx.emit_finding("partial", message="saw it")
        raise RuntimeError("then died")

    emit_then_raise.name = "emit_then_raise"
    result = execute_pipeline([emit_then_raise], PipelineContext())
    assert result.steps[0].status == "error"
    assert [f["code"] for f in result.steps[0].findings] == ["partial"]


def test_step_scope_is_per_step_not_cumulative():
    @step(findings=["a"])
    def first(ctx):
        ctx.emit_finding("a")

    @step(findings=["b"])
    def second(ctx):
        ctx.emit_finding("b")

    first.name, second.name = "first", "second"
    result = execute_pipeline([first, second], PipelineContext())
    assert [f["code"] for f in result.steps[0].findings] == ["a"]
    assert [f["code"] for f in result.steps[1].findings] == ["b"]


def test_skipped_step_emits_nothing():
    @step(findings=["a"])
    def skipped(ctx):
        ctx.emit_finding("a")

    skipped.name = "skipped"
    executor = PipelineExecutor(fail_fast=False)
    result = executor.execute([skipped], PipelineContext(), disabled_steps=["skipped"])
    assert result.steps[0].status == "skipped"
    assert result.steps[0].findings == []


# --- Local rendering ---


def test_local_run_prints_findings_grouped_by_code(capsys):
    """`continuous-gtfs schedule …` renders a run's findings for reading.

    Per-run rendering only — no fingerprinting, no issues, no triage; those are
    cross-run constructs and therefore platform features
    (specs/principles.md §The local dev kit lives and dies by the run).
    """
    from continuous_gtfs.cli import _print_findings

    @step(findings=[("bad_stop", {"subject": ["stop_id"]})])
    def emitter(ctx):
        ctx.emit_finding(
            "bad_stop", severity="error", message="S1 is bad", context={"stop_id": "S1"}
        )
        ctx.emit_finding(
            "bad_stop", severity="error", message="S2 is bad", context={"stop_id": "S2"}
        )
        ctx.emit_finding("undeclared_one", message="hmm")
        ctx.emit_finding("bad_severity", severity="critical", message="x")

    emitter.name = "emitter"
    result = execute_pipeline([emitter], PipelineContext())
    _print_findings(result)

    out, err = capsys.readouterr()
    assert "Findings: 3 code(s), 4 occurrence(s)" in out
    # Grouped by code, worst severity first, with a representative message.
    assert "[error] bad_stop (emitter): 2 occurrence(s)" in out
    assert "S1 is bad" in out
    # Undeclared codes and normalized emissions are surfaced as warnings so the
    # gap is visible in the dev loop rather than only on the platform.
    assert "undeclared_one" in err
    assert "bad_severity" in err


def test_local_run_prints_nothing_when_no_findings(capsys):
    from continuous_gtfs.cli import _print_findings

    @step()
    def quiet(ctx):
        pass

    quiet.name = "quiet"
    _print_findings(execute_pipeline([quiet], PipelineContext()))
    assert capsys.readouterr().out == ""
