"""Result types for pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class StepResult:
    """Result of executing a single pipeline step."""

    name: str
    step_index: int
    status: str  # "success", "error", "skipped"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: float = 0
    input_rows: int | None = None
    output_rows: int | None = None
    error: str | None = None
    #: Raised exception's type name, separate from `error`'s message. The
    #: message interpolates run-specific values, so it is exactly what a
    #: crash's fingerprint must not read, while the type name is the stable
    #: part (specs/issues.md §Crash capture). Both are reported; the platform
    #: chooses which to group on.
    error_type: str | None = None
    logs: list[dict] = field(default_factory=list)
    metadata: dict | None = None
    #: Findings the step emitted, drained from the context by the executor
    #: (specs/transform-framework.md §Findings).
    findings: list[dict] = field(default_factory=list)


@dataclass
class ExecutionResult:
    """Result of executing the full pipeline."""

    steps: list[StepResult] = field(default_factory=list)
    total_duration_ms: float = 0

    @property
    def success(self) -> bool:
        return all(r.status in ("success", "skipped") for r in self.steps)
