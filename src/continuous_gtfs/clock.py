"""The framework's single system-clock seam.

A step never reads the system clock: it reads ``ctx.now``, the evaluation
moment the framework sets from the dispatch (specs/transform-framework.md
§PipelineContext). The framework itself still needs wall-clock time for
its own bookkeeping — a dispatch's receipt time when the orchestrator sent
no evaluation moment, the CLI's invocation time, step start/finish
telemetry, an analysis marker's ``written_at``. Every such read goes
through :func:`system_now` so the no-system-clock guard
(tests/test_context_clock.py) can hold every other module to zero
direct reads.
"""

from __future__ import annotations

from datetime import UTC, datetime


def system_now() -> datetime:
    """The current wall-clock moment, tz-aware UTC."""
    return datetime.now(UTC)
