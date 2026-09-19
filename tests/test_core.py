"""Tests for core framework: Step, DAG, Context, Executor."""

import pytest

from continuous_gtfs import (
    PipelineContext,
    Step,
    execute_pipeline,
    export_reactflow,
    resolve_dag,
    step,
)
from continuous_gtfs.executor import PipelineExecutor

# --- Step and @step decorator ---


class DummyBuiltin(Step):
    files = ["stops.txt"]
    description = "A test builtin"

    def apply(self, ctx):
        ctx.metadata["builtin_ran"] = True


def test_step_decorator_creates_step_instance():
    @step(files=["routes.txt"])
    def my_step(ctx):
        pass

    assert isinstance(my_step, Step)
    assert my_step.files == ["routes.txt"]


def test_step_decorator_preserves_docstring():
    @step(files=["stops.txt"])
    def documented(ctx):
        """My docstring."""
        pass

    assert documented.description == "My docstring."


def test_builtin_detection():
    b = DummyBuiltin()
    b.name = "dummy"
    assert b.is_builtin is True
    assert b.builtin_name == "DummyBuiltin"

    @step(files=["a.txt"])
    def custom(ctx):
        pass

    custom.name = "custom"
    assert custom.is_builtin is False
    assert custom.builtin_name is None


def test_step_label_fallback():
    @step(files=["a.txt"])
    def my_cool_step(ctx):
        pass

    my_cool_step.name = "my_cool_step"
    assert my_cool_step.label == "My Cool Step"

    b = DummyBuiltin()
    b.name = "dummy"
    assert b.label == "A test builtin"


def test_step_default_priority():
    s = Step()
    assert s.priority == 100


def test_step_after_before_defaults():
    s = Step()
    assert s.after == []
    assert s.before == []


# --- DAG resolution ---


def test_resolve_dag_simple_chain():
    a = Step()
    a.name = "a"
    b = Step(after=[a])
    b.name = "b"
    c = Step(after=[b])
    c.name = "c"
    result = resolve_dag([c, a, b])
    assert [s.name for s in result] == ["a", "b", "c"]


def test_resolve_dag_before_edges():
    a = Step()
    a.name = "a"
    b = Step(before=[a])
    b.name = "b"
    result = resolve_dag([a, b])
    assert [s.name for s in result] == ["b", "a"]


def test_resolve_dag_deterministic_sort():
    """Steps at the same level sort by (priority, name)."""
    a = Step()
    a.name = "alpha"
    b = Step()
    b.name = "beta"
    c = Step()
    c.name = "gamma"
    result = resolve_dag([c, a, b])
    assert [s.name for s in result] == ["alpha", "beta", "gamma"]


def test_resolve_dag_priority_tiebreak():
    a = Step(priority=50)
    a.name = "z_last"
    b = Step(priority=10)
    b.name = "a_first"
    result = resolve_dag([a, b])
    assert [s.name for s in result] == ["a_first", "z_last"]


def test_resolve_dag_cycle_detection():
    a = Step()
    a.name = "a"
    b = Step(after=[a])
    b.name = "b"
    a.after = [b]
    with pytest.raises(ValueError, match="Cycle detected"):
        resolve_dag([a, b])


def test_resolve_dag_ignores_external_deps():
    """Dependencies on steps not in the list are silently ignored."""
    external = Step()
    external.name = "external"
    a = Step(after=[external])
    a.name = "a"
    result = resolve_dag([a])
    assert [s.name for s in result] == ["a"]


def test_resolve_dag_before_wildcard_runs_first():
    """`before='*'` forces a step to run before every other step."""
    init = Step(before="*")
    init.name = "init_output"
    a = Step()
    a.name = "a"
    b = Step()
    b.name = "b"
    c = Step()
    c.name = "c"

    result = resolve_dag([a, b, c, init])
    assert result[0].name == "init_output"
    # Remaining order is still alphabetical by name (default priority tie-break)
    assert [s.name for s in result[1:]] == ["a", "b", "c"]


def test_resolve_dag_before_wildcard_ignores_input_order():
    """Step order in the input list doesn't change where a before='*' step lands."""
    init = Step(before="*")
    init.name = "z_init"  # alphabetically last — still runs first
    a = Step()
    a.name = "a"

    # Place the init step at the end of the input
    result = resolve_dag([a, init])
    assert [s.name for s in result] == ["z_init", "a"]


def test_resolve_dag_multiple_before_wildcards():
    """Multiple before='*' steps run in (priority, name) order among
    themselves, before everything else."""
    init_b = Step(before="*")
    init_b.name = "init_b"
    init_a = Step(before="*", priority=10)  # lower priority → runs first
    init_a.name = "init_a"
    work = Step()
    work.name = "work"

    result = resolve_dag([work, init_b, init_a])
    # Both before="*" steps queue at in_degree=0 (they don't edge to each
    # other), and sort by (priority, name): init_a (priority 10) first,
    # then init_b (priority 100). work runs last because both init steps
    # have implicit edges to it.
    assert [s.name for s in result] == ["init_a", "init_b", "work"]


def test_resolve_dag_before_wildcard_with_single_step():
    """A before='*' step on its own — no other steps to edge to — just runs."""
    init = Step(before="*")
    init.name = "init"
    result = resolve_dag([init])
    assert [s.name for s in result] == ["init"]


def test_resolve_dag_after_wildcard_runs_last():
    """`after='*'` forces a step to run after every other step."""
    finalize = Step(after="*")
    finalize.name = "finalize"
    a = Step()
    a.name = "a"
    b = Step()
    b.name = "b"
    c = Step()
    c.name = "c"

    # finalize is alphabetically in the middle — wildcard still pushes it last
    result = resolve_dag([finalize, a, b, c])
    assert [s.name for s in result] == ["a", "b", "c", "finalize"]


def test_resolve_dag_multiple_after_wildcards():
    """Multiple after='*' steps all run after non-wildcards,
    ordered by (priority, name)."""
    finalize_b = Step(after="*")
    finalize_b.name = "finalize_b"
    finalize_a = Step(after="*", priority=10)
    finalize_a.name = "finalize_a"
    work = Step()
    work.name = "work"

    result = resolve_dag([finalize_b, work, finalize_a])
    # work first; then both wildcards ordered by (priority, name):
    # finalize_a (priority 10) before finalize_b (priority 100)
    assert [s.name for s in result] == ["work", "finalize_a", "finalize_b"]


def test_resolve_dag_before_and_after_wildcards_coexist():
    """A `before='*'` step runs first; an `after='*'` step runs last;
    regular steps in between."""
    init = Step(before="*")
    init.name = "init"
    finalize = Step(after="*")
    finalize.name = "finalize"
    work_a = Step()
    work_a.name = "work_a"
    work_b = Step()
    work_b.name = "work_b"

    result = resolve_dag([finalize, work_b, init, work_a])
    assert result[0].name == "init"
    assert result[-1].name == "finalize"
    # Middle preserves alphabetical-by-name default
    assert [s.name for s in result[1:-1]] == ["work_a", "work_b"]


def test_step_rejects_before_and_after_both_wildcard():
    """A step with before='*' AND after='*' is a contradiction; reject at init."""
    with pytest.raises(ValueError, match="both before='\\*' and after='\\*'"):
        Step(before="*", after="*")


# --- ReactFlow export ---


def test_export_reactflow():
    a = DummyBuiltin()
    a.name = "a"

    @step(files=["routes.txt"], after=[a])
    def b(ctx):
        pass

    b.name = "b"
    dag = resolve_dag([a, b])
    rf = export_reactflow(dag)
    assert len(rf["nodes"]) == 2
    assert len(rf["edges"]) == 1
    assert rf["edges"][0] == {"source": "a", "target": "b"}
    assert rf["nodes"][0]["type"] == "builtin"
    assert rf["nodes"][1]["type"] == "custom"


# --- PipelineContext with id_mappings ---


def test_context_id_mappings():
    ctx = PipelineContext()
    ctx.add_id_mapping("routes.txt", "route_id", "100", "1")
    ctx.add_id_mapping("routes.txt", "route_id", "200", None)

    mappings = ctx.get_id_mappings("routes.txt", "route_id")
    assert mappings == {"100": "1", "200": None}


def test_context_id_mappings_empty():
    ctx = PipelineContext()
    assert ctx.get_id_mappings("routes.txt", "route_id") == {}


# --- Executor ---


def test_execute_pipeline_success():
    @step(files=["a.txt"])
    def s1(ctx):
        ctx.output["a.txt"] = "modified"

    s1.name = "s1"
    ctx = PipelineContext(output={"a.txt": "original"})
    result = execute_pipeline([s1], ctx)
    assert result.success
    assert len(result.steps) == 1
    assert result.steps[0].status == "success"
    assert ctx.output["a.txt"] == "modified"


def test_execute_pipeline_skips_disabled():
    @step(files=["a.txt"], enabled=False)
    def disabled_step(ctx):
        raise RuntimeError("Should not run")

    disabled_step.name = "disabled_step"
    result = execute_pipeline([disabled_step], PipelineContext())
    assert result.success
    assert result.steps[0].status == "skipped"


def test_execute_pipeline_fail_fast():
    @step(files=["a.txt"])
    def bad(ctx):
        raise ValueError("broken")

    bad.name = "bad"

    @step(files=["b.txt"])
    def good(ctx):
        ctx.metadata["ran"] = True

    good.name = "good"

    ctx = PipelineContext()
    result = execute_pipeline([bad, good], ctx, fail_fast=True)
    assert not result.success
    assert len(result.steps) == 1  # stopped after first error
    assert "ran" not in ctx.metadata


def test_execute_pipeline_continue_mode():
    @step(files=["a.txt"])
    def bad(ctx):
        raise ValueError("broken")

    bad.name = "bad"

    @step(files=["b.txt"])
    def good(ctx):
        ctx.metadata["ran"] = True

    good.name = "good"

    ctx = PipelineContext()
    result = execute_pipeline([bad, good], ctx, fail_fast=False)
    assert not result.success
    assert len(result.steps) == 2  # continued past error
    assert result.steps[0].status == "error"
    assert result.steps[1].status == "success"
    assert ctx.metadata["ran"] is True


def test_executor_hooks_fire():
    events = []

    executor = PipelineExecutor(fail_fast=True)
    executor.add_hook("before_pipeline", lambda ctx: events.append("before_pipeline"))
    executor.add_hook("before_step", lambda ctx, s: events.append(f"before:{s.name}"))
    executor.add_hook(
        "after_step", lambda ctx, s, r: events.append(f"after:{s.name}:{r.status}")
    )
    executor.add_hook("after_pipeline", lambda ctx, r: events.append("after_pipeline"))

    @step(files=["a.txt"])
    def my_step(ctx):
        pass

    my_step.name = "my_step"
    executor.execute([my_step], PipelineContext())

    assert events == [
        "before_pipeline",
        "before:my_step",
        "after:my_step:success",
        "after_pipeline",
    ]


def test_executor_on_error_hook():
    errors = []
    executor = PipelineExecutor(fail_fast=True)
    executor.add_hook("on_error", lambda ctx, s, e: errors.append(str(e)))

    @step(files=["a.txt"])
    def bad(ctx):
        raise ValueError("boom")

    bad.name = "bad"
    executor.execute([bad], PipelineContext())
    assert errors == ["boom"]


# --- disabled_steps (DB-driven step skip) ---


def test_executor_disabled_steps_skips_matching_step():
    """Steps named in `disabled_steps` are recorded as skipped
    without calling apply()."""
    ran: list[str] = []

    @step(files=["a.txt"])
    def step_a(ctx):
        ran.append("a")

    @step(files=["b.txt"])
    def step_b(ctx):
        ran.append("b")

    step_a.name = "step_a"
    step_b.name = "step_b"

    result = PipelineExecutor().execute(
        [step_a, step_b], PipelineContext(), disabled_steps=["step_b"]
    )

    assert ran == ["a"]
    statuses = [(s.name, s.status) for s in result.steps]
    assert statuses == [("step_a", "success"), ("step_b", "skipped")]


def test_executor_disabled_steps_fires_after_step_hook():
    """The skip path fires after_step so streaming consumers see it."""
    seen: list[tuple[str, str]] = []

    @step(files=["a.txt"])
    def step_a(ctx):
        pass

    step_a.name = "step_a"

    executor = PipelineExecutor()
    executor.add_hook(
        "after_step",
        lambda ctx, s, r: seen.append((s.name, r.status)),
    )
    executor.execute([step_a], PipelineContext(), disabled_steps=["step_a"])

    assert seen == [("step_a", "skipped")]


def test_executor_disabled_steps_does_not_short_circuit_other_steps():
    """Disabling step B must not block step C that comes after it."""
    ran: list[str] = []

    @step(files=["a.txt"])
    def step_a(ctx):
        ran.append("a")

    @step(files=["b.txt"])
    def step_b(ctx):
        ran.append("b")

    @step(files=["c.txt"])
    def step_c(ctx):
        ran.append("c")

    step_a.name = "step_a"
    step_b.name = "step_b"
    step_c.name = "step_c"

    PipelineExecutor().execute(
        [step_a, step_b, step_c],
        PipelineContext(),
        disabled_steps=["step_b"],
    )

    assert ran == ["a", "c"]


def test_executor_disabled_steps_none_runs_all():
    """Omitting disabled_steps runs every step normally."""
    ran: list[str] = []

    @step(files=["a.txt"])
    def step_a(ctx):
        ran.append("a")

    @step(files=["b.txt"])
    def step_b(ctx):
        ran.append("b")

    step_a.name = "step_a"
    step_b.name = "step_b"

    PipelineExecutor().execute([step_a, step_b], PipelineContext())
    assert ran == ["a", "b"]


def test_executor_disabled_steps_combines_with_step_enabled_flag():
    """A step is skipped if disabled_steps includes it OR step.enabled is False."""
    ran: list[str] = []

    @step(files=["a.txt"])
    def step_a(ctx):
        ran.append("a")

    @step(files=["b.txt"])
    def step_b(ctx):
        ran.append("b")

    step_a.name = "step_a"
    step_a.enabled = False  # disabled via the existing attribute
    step_b.name = "step_b"

    result = PipelineExecutor().execute(
        [step_a, step_b], PipelineContext(), disabled_steps=["step_b"]
    )

    assert ran == []
    assert [s.status for s in result.steps] == ["skipped", "skipped"]


def test_executor_disabled_step_name_not_in_dag_is_silent_no_op():
    """Unknown names in disabled_steps don't error — they just don't match anything."""
    ran: list[str] = []

    @step(files=["a.txt"])
    def step_a(ctx):
        ran.append("a")

    step_a.name = "step_a"

    PipelineExecutor().execute(
        [step_a], PipelineContext(), disabled_steps=["nonexistent_step"]
    )
    assert ran == ["a"]


def test_executor_hook_failure_doesnt_break_pipeline():
    def bad_hook(ctx):
        raise RuntimeError("hook crash")

    executor = PipelineExecutor()
    executor.add_hook("before_pipeline", bad_hook)

    @step(files=["a.txt"])
    def my_step(ctx):
        ctx.metadata["ran"] = True

    my_step.name = "my_step"
    ctx = PipelineContext()
    result = executor.execute([my_step], ctx)
    assert result.success
    assert ctx.metadata["ran"] is True


def test_executor_captures_logs():
    import logging

    @step(files=["a.txt"])
    def logging_step(ctx):
        logging.getLogger(__name__).info("hello from step")

    logging_step.name = "logging_step"

    # Ensure root logger propagates at INFO level
    root = logging.getLogger()
    original_level = root.level
    root.setLevel(logging.DEBUG)
    try:
        result = execute_pipeline([logging_step], PipelineContext())
    finally:
        root.setLevel(original_level)
    assert result.success
    logs = result.steps[0].logs
    assert any("hello from step" in entry["message"] for entry in logs)


def test_step_result_has_timestamps():
    @step(files=["a.txt"])
    def my_step(ctx):
        pass

    my_step.name = "my_step"
    result = execute_pipeline([my_step], PipelineContext())
    sr = result.steps[0]
    assert sr.started_at is not None
    assert sr.completed_at is not None
    assert sr.completed_at >= sr.started_at


def test_invalid_hook_event_raises():
    executor = PipelineExecutor()
    with pytest.raises(ValueError, match="Unknown hook event"):
        executor.add_hook("nonexistent", lambda: None)


# --- Inputs / Output ---


def test_context_inputs_and_output_default_empty():
    ctx = PipelineContext()
    assert ctx.inputs == {}
    assert ctx.output == {}


def test_context_inputs_preserved():
    ctx = PipelineContext(inputs={"overrides": [1, 2, 3]})
    assert ctx.inputs["overrides"] == [1, 2, 3]


def test_context_output_is_mutable():
    ctx = PipelineContext()
    ctx.output["routes.txt"] = "placeholder"
    assert ctx.output["routes.txt"] == "placeholder"


def test_context_inputs_dict_is_readonly():
    """ctx.inputs is a MappingProxyType — transforms can't
    add / remove / replace entries."""
    ctx = PipelineContext(inputs={"schedule": "data"})
    # Reads work
    assert ctx.inputs["schedule"] == "data"
    # Writes raise TypeError (the standard MappingProxyType behavior)
    with pytest.raises(TypeError):
        ctx.inputs["schedule"] = "different"
    with pytest.raises(TypeError):
        ctx.inputs["new_key"] = "x"
    with pytest.raises(TypeError):
        del ctx.inputs["schedule"]
    with pytest.raises((TypeError, AttributeError)):
        ctx.inputs.update({"schedule": "x"})
    with pytest.raises((TypeError, AttributeError)):
        ctx.inputs.pop("schedule")


def test_context_inputs_isolated_from_caller_mutations():
    """The caller's input dict is shallow-copied, so
    post-construction mutations don't leak in."""
    raw = {"schedule": "v1"}
    ctx = PipelineContext(inputs=raw)
    raw["schedule"] = "v2"
    raw["new_key"] = "added"
    assert ctx.inputs["schedule"] == "v1"
    assert "new_key" not in ctx.inputs


def test_context_inputs_inner_dicts_are_readonly():
    """gtfs_schedule_zip-shaped inputs (dict[filename → DataFrame])
    also reject mutation."""
    schedule = {"stops.txt": "df_placeholder", "routes.txt": "df_placeholder"}
    ctx = PipelineContext(inputs={"schedule": schedule})
    # Reads work
    assert ctx.inputs["schedule"]["stops.txt"] == "df_placeholder"
    # Writes raise — at the inner dict level
    with pytest.raises(TypeError):
        ctx.inputs["schedule"]["stops.txt"] = "modified"
    with pytest.raises(TypeError):
        ctx.inputs["schedule"]["new_file.txt"] = "x"
    with pytest.raises(TypeError):
        del ctx.inputs["schedule"]["stops.txt"]


def test_context_inputs_inner_dict_caller_mutations_dont_leak():
    """Mutations to the caller's inner dict after construction
    don't leak into ctx.inputs."""
    schedule = {"stops.txt": "v1"}
    ctx = PipelineContext(inputs={"schedule": schedule})
    schedule["stops.txt"] = "v2"
    schedule["new_file.txt"] = "added"
    assert ctx.inputs["schedule"]["stops.txt"] == "v1"
    assert "new_file.txt" not in ctx.inputs["schedule"]


def test_context_inputs_inner_dict_copies_cleanly_to_output():
    """`dict(ctx.inputs[name])` produces a fresh mutable dict —
    the init-step copy pattern."""
    schedule = {"stops.txt": "stops_df", "routes.txt": "routes_df"}
    ctx = PipelineContext(inputs={"schedule": schedule})

    # The init-step pattern: copy ctx.inputs[name] into a mutable ctx.output
    ctx.output = dict(ctx.inputs["schedule"])

    # The copy is a regular mutable dict — not a MappingProxyType
    assert ctx.output["stops.txt"] == "stops_df"
    ctx.output["stops.txt"] = "filtered_df"  # mutating output works
    ctx.output["new_file.txt"] = "added"
    assert ctx.output["new_file.txt"] == "added"

    # Mutating the output copy didn't leak back into ctx.inputs
    assert ctx.inputs["schedule"]["stops.txt"] == "stops_df"
    assert "new_file.txt" not in ctx.inputs["schedule"]


def test_context_inputs_non_dict_values_not_wrapped():
    """csv_table / FeedMessage / bytes values pass through untouched."""
    ctx = PipelineContext(
        inputs={
            "csv": "stand_in_for_dataframe",
            "rt_feed": "stand_in_for_FeedMessage",
            "blob": b"\x00\x01\x02",
        }
    )
    # Values come through as-is; no MappingProxy wrap on non-dict values
    assert ctx.inputs["csv"] == "stand_in_for_dataframe"
    assert ctx.inputs["rt_feed"] == "stand_in_for_FeedMessage"
    assert ctx.inputs["blob"] == b"\x00\x01\x02"


# --- Executor row counting ---


def test_executor_populates_row_counts():
    import polars as pl

    @step(files=["a.txt"])
    def remove_one(ctx):
        df = ctx.output["a.txt"]
        ctx.output["a.txt"] = df.head(df.height - 1)

    remove_one.name = "remove_one"

    df = pl.DataFrame({"x": ["1", "2", "3"]})
    result = execute_pipeline([remove_one], PipelineContext(output={"a.txt": df}))
    sr = result.steps[0]
    assert sr.input_rows == 3
    assert sr.output_rows == 2


def test_executor_row_counts_none_for_noncountable_datasets():
    @step(files=[])
    def opaque(ctx):
        pass

    opaque.name = "opaque"

    # Use a non-iterable, non-len-able value
    class Opaque:
        pass

    result = execute_pipeline([opaque], PipelineContext(output={"x": Opaque()}))
    sr = result.steps[0]
    assert sr.input_rows is None
    assert sr.output_rows is None
