"""Pipeline executor — runs steps in DAG order with hooks and log capture."""

from __future__ import annotations

import copy
import dataclasses
import logging
import os
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

import polars as pl
from google.transit import gtfs_realtime_pb2 as gtfs_rt

from .clock import system_now
from .context import PipelineContext
from .results import ExecutionResult, StepResult
from .step import CHECK_FAILED, CHECK_MUTATED_VIEW, Step

logger = logging.getLogger(__name__)

#: Size of the checks-phase thread pool (specs/transform-framework.md
#: §Checks phase). A positive integer; anything else falls back to the
#: default, min(8, cpu count).
CHECK_WORKERS_ENV = "CONTINUOUS_GTFS_CHECK_WORKERS"
#: "1" turns on dev-time mutation detection for checks outside the CLI
#: (specs/transform-framework.md §Dev-time mutation detection).
CHECK_GUARD_ENV = "CONTINUOUS_GTFS_CHECK_GUARD"


def default_check_workers() -> int:
    """The checks-phase pool size: CONTINUOUS_GTFS_CHECK_WORKERS when it is a
    positive integer, else min(8, cpu count)."""
    fallback = min(8, os.cpu_count() or 1)
    raw = os.environ.get(CHECK_WORKERS_ENV)
    if raw is None:
        return fallback
    try:
        n = int(raw)
    except ValueError:
        return fallback
    return n if n >= 1 else fallback


def check_guard_from_env() -> bool:
    return os.environ.get(CHECK_GUARD_ENV) == "1"


class _StepLogHandler(logging.Handler):
    """Captures log records emitted during a single step's execution.

    With `thread` set, only records emitted on that thread are captured — so
    checks running concurrently each keep their own logs."""

    def __init__(self, thread: int | None = None) -> None:
        super().__init__()
        self.records: list[dict[str, Any]] = []
        self.thread = thread

    def emit(self, record: logging.LogRecord) -> None:
        if self.thread is not None and record.thread != self.thread:
            return
        self.records.append(
            {
                "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
                "level": record.levelname.lower(),
                "message": self.format(record),
            }
        )


class PipelineExecutor:
    """Executes pipeline steps with lifecycle hooks and per-step log capture."""

    def __init__(
        self,
        fail_fast: bool = True,
        *,
        check_guard: bool | None = None,
        check_workers: int | None = None,
    ) -> None:
        """
        Args:
            fail_fast: Stop at the first failing transform (the checks phase
                then does not run). A failing check never stops the others.
            check_guard: Compare each check's view with the real output after
                it runs and report a change as `check_mutated_view`. None
                reads CONTINUOUS_GTFS_CHECK_GUARD; the CLI passes True.
            check_workers: Checks-phase pool size. None reads
                CONTINUOUS_GTFS_CHECK_WORKERS (default min(8, cpu count)).
        """
        self.fail_fast = fail_fast
        self.check_guard = check_guard
        self.check_workers = check_workers
        self._hooks: dict[str, list[Callable]] = {
            "before_pipeline": [],
            "before_step": [],
            "after_step": [],
            "on_error": [],
            "after_pipeline": [],
        }

    def add_hook(self, event: str, callback: Callable) -> None:
        if event not in self._hooks:
            raise ValueError(
                f"Unknown hook event '{event}'. "
                f"Valid events: {list(self._hooks.keys())}"
            )
        self._hooks[event].append(callback)

    def _fire_hooks(self, event: str, *args: Any) -> None:
        for hook in self._hooks[event]:
            try:
                hook(*args)
            except Exception:
                logger.warning("Hook %s failed", event, exc_info=True)

    def execute(
        self,
        steps: list[Step],
        ctx: PipelineContext,
        *,
        disabled_steps: Iterable[str] | None = None,
    ) -> ExecutionResult:
        """Execute steps in order (must already be DAG-resolved).

        Args:
            steps: Steps in execution order (from resolve_dag).
            ctx: Pipeline context with datasets.
            disabled_steps: Optional set of step names to skip for this
                run. Combined with each step's own `.enabled` flag — a
                step is skipped if its name is in this set OR
                `not step.enabled`. Skipped steps stay in the DAG
                (so any downstream `after=[…]` dependencies are still
                satisfied) but contribute a `skipped` StepResult and
                fire the `after_step` hook with that status.
                Sourced from `pipelines.disabled_steps` at the
                orchestrator → carried through DispatchRequest →
                handed to the worker → passed here. See
                specs/configuration.md §Step-level ops controls.
        """
        result = ExecutionResult()
        start = time.monotonic()

        disabled_set: set[str] = set(disabled_steps or ())

        self._fire_hooks("before_pipeline", ctx)

        # Transforms run in DAG order; checks run after every transform,
        # concurrently (specs/transform-framework.md §Checks phase). The
        # resolver already orders checks last, by name; partitioning here
        # keeps that true for a caller-built list too.
        indexed = list(enumerate(steps))
        transforms = [(i, s) for i, s in indexed if not s.is_check]
        checks = sorted(
            ((i, s) for i, s in indexed if s.is_check), key=lambda p: p[1].name
        )

        stopped = False
        for idx, s in transforms:
            if s.name in disabled_set or not s.enabled:
                self._record_skip(result, s, idx, ctx)
                continue

            step_result = self._execute_step(s, idx, ctx)
            result.steps.append(step_result)

            if step_result.status == "error" and self.fail_fast:
                stopped = True
                break

        if checks and not stopped:
            self._execute_checks(checks, ctx, disabled_set, result)

        result.total_duration_ms = (time.monotonic() - start) * 1000
        self._fire_hooks("after_pipeline", ctx, result)
        return result

    def _record_skip(
        self, result: ExecutionResult, s: Step, idx: int, ctx: PipelineContext
    ) -> None:
        skipped = StepResult(name=s.name, step_index=idx, status="skipped", role=s.role)
        result.steps.append(skipped)
        # Fire after_step so the orchestrator's event stream records the
        # skip alongside real steps — without this the run's stepResults
        # reflects the skip only via the final ExecutionResult, not the
        # streaming events.
        self._fire_hooks("after_step", ctx, s, skipped)

    # --- Checks phase (specs/transform-framework.md §Checks phase) ---

    def _execute_checks(
        self,
        checks: list[tuple[int, Step]],
        ctx: PipelineContext,
        disabled_set: set[str],
        result: ExecutionResult,
    ) -> None:
        """Run every check concurrently against its own read-only view, then
        record results, findings and hooks in check-name order.

        Hooks fire only on this (the executor's) thread, so no two hook calls
        overlap: every runnable check's before_step as the phase starts, then
        on_error / after_step per check in name order as each is collected.
        """
        guard = (
            self.check_guard if self.check_guard is not None else check_guard_from_env()
        )
        workers = self.check_workers or default_check_workers()
        rows = self._count_rows(ctx)
        # One snapshot of the real output, taken before any check runs. Every
        # check's view is built from it and every guard verdict compares
        # against it, so what a check sees and whether it is blamed never
        # depend on scheduling (specs/transform-framework.md §Dev-time
        # mutation detection). Cheap: DataFrame clones are zero-copy. If it
        # cannot be taken, each check builds its view from the live output
        # and records its own copy failure.
        snapshot: Mapping[str, Any] | None
        try:
            snapshot = {k: _disposable_copy(v) for k, v in ctx.output.items()}
        except Exception:  # noqa: BLE001 — reported per check below
            snapshot = None

        runnable = [
            (idx, s) for idx, s in checks if s.name not in disabled_set and s.enabled
        ]
        for _idx, s in runnable:
            self._fire_hooks("before_step", ctx, s)

        futures: dict[int, Future[tuple[StepResult, Exception | None]]] = {}
        pool = ThreadPoolExecutor(
            max_workers=max(1, min(workers, len(runnable) or 1)),
            thread_name_prefix="cgtfs-check",
        )
        try:
            for idx, s in runnable:
                futures[id(s)] = pool.submit(
                    self._run_check, s, idx, ctx, snapshot, guard, rows
                )
            for idx, s in checks:
                future = futures.get(id(s))
                if future is None:
                    self._record_skip(result, s, idx, ctx)
                    continue
                step_result, exc = future.result()
                result.steps.append(step_result)
                # The run's cumulative record, in check-name order then each
                # check's own emission order — independent of scheduling.
                ctx.findings.extend(step_result.findings)
                if exc is not None:
                    self._fire_hooks("on_error", ctx, s, exc)
                self._fire_hooks("after_step", ctx, s, step_result)
        finally:
            pool.shutdown(wait=True)

    def _run_check(
        self,
        s: Step,
        idx: int,
        ctx: PipelineContext,
        snapshot: Mapping[str, Any] | None,
        guard: bool,
        rows: int | None,
    ) -> tuple[StepResult, Exception | None]:
        """One check on a pool thread. Fires no hooks (the caller does, on
        its own thread) and never raises an Exception: a failure — in the
        check, or while building its view — is recorded on the StepResult,
        reported as `check_failed`, and handed back for on_error. The view
        is built from `snapshot` (the pre-phase output); with the guard on,
        the check's final view is compared with it."""
        step_result = StepResult(
            name=s.name,
            step_index=idx,
            status="running",
            started_at=system_now(),
            # A check cannot change the output, so its row counts are the
            # output's, before and after.
            input_rows=rows,
            output_rows=rows,
            role=s.role,
        )
        log_handler = _StepLogHandler(thread=threading.get_ident())
        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)
        check_ctx: PipelineContext | None = None
        exc: Exception | None = None
        step_start = time.monotonic()
        try:
            # Built here, inside the try, so a copy that fails is this
            # check's error rather than an escape from the phase.
            check_ctx = _check_context(
                ctx, ctx.output if snapshot is None else snapshot
            )
            check_ctx._begin_step(s)
            # The return value is ignored: findings are a check's only
            # channel back to the run.
            s.apply(check_ctx)
            step_result.status = "success"
        except Exception as e:
            exc = e
            step_result.status = "error"
            step_result.error = str(e)
            step_result.error_type = type(e).__name__
        finally:
            step_result.duration_ms = (time.monotonic() - step_start) * 1000
            root_logger.removeHandler(log_handler)
            step_result.logs = log_handler.records
            step_result.findings = (
                check_ctx._drain_step_findings() if check_ctx is not None else []
            )
            step_result.completed_at = system_now()
        if exc is not None:
            step_result.findings.append(_check_failed_finding(s.name, exc))
        if guard and snapshot is not None and check_ctx is not None:
            changed = _changed_keys(check_ctx.output, snapshot)
            if changed:
                step_result.findings.append(_mutated_view_finding(s.name, changed))
        return step_result, exc

    @staticmethod
    def _count_rows(ctx: PipelineContext) -> int | None:
        """Count total rows across all DataFrame-shaped output values.

        Returns None if ctx.output has no sized values to count — for
        RT runs that hold FeedMessage objects the count is meaningless
        pre-serialization and we skip it.
        """
        total = 0
        found = False
        for v in ctx.output.values():
            try:
                total += len(v)
                found = True
            except TypeError:
                continue
        return total if found else None

    def _execute_step(self, s: Step, idx: int, ctx: PipelineContext) -> StepResult:
        step_result = StepResult(
            name=s.name,
            step_index=idx,
            status="running",
            started_at=system_now(),
        )

        step_result.input_rows = self._count_rows(ctx)

        self._fire_hooks("before_step", ctx, s)

        # Install per-step log capture
        log_handler = _StepLogHandler()
        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)

        # Open the step's finding-emission scope so ctx.emit_finding knows
        # which step (and which declared vocabulary) it is emitting under.
        ctx._begin_step(s)

        step_start = time.monotonic()
        try:
            s.apply(ctx)
            duration = (time.monotonic() - step_start) * 1000
            step_result.status = "success"
            step_result.duration_ms = duration
        except Exception as e:
            duration = (time.monotonic() - step_start) * 1000
            step_result.status = "error"
            step_result.duration_ms = duration
            step_result.error = str(e)
            # The exception's type name is captured separately from its
            # message: crash grouping reads the type, never the interpolated
            # message (specs/issues.md §Crash capture).
            step_result.error_type = type(e).__name__
            self._fire_hooks("on_error", ctx, s, e)
        finally:
            root_logger.removeHandler(log_handler)
            step_result.logs = log_handler.records
            # Drain even on the error path: a step that emitted findings and
            # then raised still reported them.
            step_result.findings = ctx._drain_step_findings()
            step_result.completed_at = system_now()

        step_result.output_rows = self._count_rows(ctx)

        self._fire_hooks("after_step", ctx, s, step_result)
        return step_result


def _disposable_copy(value: Any) -> Any:
    """A check's own copy of one output value (specs/transform-framework.md
    §Read-only by construction): a DataFrame clone (zero-copy — the Arrow
    buffers are shared and immutable, so an in-place method changes the
    clone only), a fresh FeedMessage filled by CopyFrom, a deep copy of
    anything else."""
    if isinstance(value, pl.DataFrame):
        return value.clone()
    if isinstance(value, gtfs_rt.FeedMessage):
        return _copy_feed(value)
    if isinstance(value, pl.Series):
        return value.clone()
    if isinstance(value, Mapping):
        return {k: _disposable_copy(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_disposable_copy(v) for v in value]
    return _deep_copy(value)


def _copy_feed(value: gtfs_rt.FeedMessage) -> gtfs_rt.FeedMessage:
    fresh = gtfs_rt.FeedMessage()
    fresh.CopyFrom(value)
    return fresh


def _input_copy(value: Any) -> Any:
    """A check's own copy of one input value. An init step that seeds the
    output from an input aliases the input's DataFrames / FeedMessages into
    the output, so inputs are copied like the output: DataFrames cloned,
    FeedMessages copied, an inner mapping (a gtfs_schedule_zip's files)
    re-wrapped read-only over copies. Immutable values (bytes, a FileTree)
    are shared."""
    if isinstance(value, pl.DataFrame):
        return value.clone()
    if isinstance(value, gtfs_rt.FeedMessage):
        return _copy_feed(value)
    if isinstance(value, Mapping):
        return MappingProxyType({k: _input_copy(v) for k, v in value.items()})
    return value


def _deep_copy(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:  # noqa: BLE001 — an uncopyable value is shared, not lost
        return copy.copy(value)


def _check_context(ctx: PipelineContext, output: Mapping[str, Any]) -> PipelineContext:
    """A check's own context: the run's inputs and environment, a read-only
    mapping of disposable copies as its output, copies of the mutable
    fields, and its own findings list. `dataclasses.replace` carries every
    other field over, so a field added to PipelineContext later reaches
    checks without an edit here."""
    view = MappingProxyType({k: _disposable_copy(v) for k, v in output.items()})
    inputs = MappingProxyType({k: _input_copy(v) for k, v in ctx.inputs.items()})
    return dataclasses.replace(
        ctx,
        inputs=inputs,
        output=view,  # type: ignore[arg-type]
        config=_deep_copy(ctx.config),
        metadata=_deep_copy(ctx.metadata),
        id_mappings={k: dict(v) for k, v in ctx.id_mappings.items()},
        findings=[],
    )


def _same_value(a: Any, b: Any) -> bool:
    if isinstance(a, pl.DataFrame) or isinstance(b, pl.DataFrame):
        return (
            isinstance(a, pl.DataFrame)
            and isinstance(b, pl.DataFrame)
            and a.schema == b.schema
            and a.equals(b)
        )
    if isinstance(a, pl.Series) or isinstance(b, pl.Series):
        return isinstance(a, pl.Series) and isinstance(b, pl.Series) and a.equals(b)
    if isinstance(a, gtfs_rt.FeedMessage) and isinstance(b, gtfs_rt.FeedMessage):
        return a.SerializeToString(deterministic=True) == b.SerializeToString(
            deterministic=True
        )
    if isinstance(a, Mapping) or isinstance(b, Mapping):
        return (
            isinstance(a, Mapping)
            and isinstance(b, Mapping)
            and set(a) == set(b)
            and all(_same_value(a[k], b[k]) for k in a)
        )
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return (
            type(a) is type(b)
            and len(a) == len(b)
            and all(_same_value(x, y) for x, y in zip(a, b, strict=True))
        )
    try:
        return bool(a == b)
    except Exception:  # noqa: BLE001 — incomparable reads as changed
        return False


def _changed_keys(view: Mapping[str, Any], real: Mapping[str, Any]) -> list[str]:
    """The output keys whose value differs between a check's final view and
    the real output; a key on one side only differs."""
    keys = set(view) | set(real)
    return sorted(
        str(k)
        for k in keys
        if k not in view or k not in real or not _same_value(view[k], real[k])
    )


def _mutated_view_finding(step_name: str, changed: list[str]) -> dict[str, Any]:
    """The framework-reserved `check_mutated_view` finding, in the shape
    PipelineContext.emit_finding records. Declared on every check by the
    platform, so never flagged undeclared."""
    files = ", ".join(changed)
    return {
        "code": CHECK_MUTATED_VIEW,
        "severity": "error",
        "message": (
            f"Check {step_name} changed its view of the output ({files}). A "
            "check cannot change the data, so the run's output is unaffected; "
            "if the step needs to change the output, make it a transform."
        ),
        "context": {"files": files},
        "occurrence_count": 1,
        "step": step_name,
        "undeclared": False,
        "emit_errors": [],
    }


def _check_failed_finding(step_name: str, exc: Exception) -> dict[str, Any]:
    """The framework-reserved `check_failed` finding: a check raised. The run
    still completes and publishes (specs/transform-framework.md §Checks
    phase); this is how the failure reaches review."""
    error_type = type(exc).__name__
    return {
        "code": CHECK_FAILED,
        "severity": "error",
        "message": f"Check {step_name} failed: {error_type}: {exc}",
        "context": {"error_type": error_type},
        "occurrence_count": 1,
        "step": step_name,
        "undeclared": False,
        "emit_errors": [],
    }


def execute_pipeline(
    steps: list[Step],
    ctx: PipelineContext,
    fail_fast: bool = True,
) -> ExecutionResult:
    """Convenience function: execute steps with default executor (no hooks)."""
    executor = PipelineExecutor(fail_fast=fail_fast)
    return executor.execute(steps, ctx)
