"""Pipeline executor — runs steps in DAG order with hooks and log capture."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from .context import PipelineContext
from .results import ExecutionResult, StepResult
from .step import Step

logger = logging.getLogger(__name__)


class _StepLogHandler(logging.Handler):
    """Captures log records emitted during a single step's execution."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(
            {
                "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
                "level": record.levelname.lower(),
                "message": self.format(record),
            }
        )


class PipelineExecutor:
    """Executes pipeline steps with lifecycle hooks and per-step log capture."""

    def __init__(self, fail_fast: bool = True) -> None:
        self.fail_fast = fail_fast
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

        for idx, s in enumerate(steps):
            if s.name in disabled_set or not s.enabled:
                skipped = StepResult(
                    name=s.name,
                    step_index=idx,
                    status="skipped",
                )
                result.steps.append(skipped)
                # Fire after_step so the orchestrator's event stream
                # records the skip alongside real steps — without this
                # the run's stepResults reflects the skip only via the
                # final ExecutionResult, not the streaming events.
                self._fire_hooks("after_step", ctx, s, skipped)
                continue

            step_result = self._execute_step(s, idx, ctx)
            result.steps.append(step_result)

            if step_result.status == "error" and self.fail_fast:
                break

        result.total_duration_ms = (time.monotonic() - start) * 1000
        self._fire_hooks("after_pipeline", ctx, result)
        return result

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
            started_at=datetime.now(UTC),
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
            step_result.completed_at = datetime.now(UTC)

        step_result.output_rows = self._count_rows(ctx)

        self._fire_hooks("after_step", ctx, s, step_result)
        return step_result


def execute_pipeline(
    steps: list[Step],
    ctx: PipelineContext,
    fail_fast: bool = True,
) -> ExecutionResult:
    """Convenience function: execute steps with default executor (no hooks)."""
    executor = PipelineExecutor(fail_fast=fail_fast)
    return executor.execute(steps, ctx)
