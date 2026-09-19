"""PipelineContext — shared state passed to each step."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .step import Step

logger = logging.getLogger(__name__)

#: The severities a finding may claim. Anything else is coerced to "warning"
#: and recorded as an emission problem — never raised
#: (specs/transform-framework.md §Emitting).
FINDING_SEVERITIES = ("error", "warning", "info")
DEFAULT_FINDING_SEVERITY = "warning"


@dataclass
class PipelineContext:
    """Context passed to each step's apply() method.

    Attributes:
        inputs: Named inputs populated by the framework before the first
            step runs. The shape of each value depends on the source
            asset's content_kind (see
            specs/asset-registry.md §Content Kinds):
              gtfs_schedule_zip  → dict[str, polars.DataFrame]
              csv_table          → polars.DataFrame
              gtfs_rt_protobuf   → gtfs_realtime_pb2.FeedMessage
              opaque_bytes       → bytes
            Transforms read ctx.inputs[name] by the name declared in the
            pipeline's INPUTS manifest.

            The dict itself is read-only (wrapped in
            types.MappingProxyType) — transforms can't add, remove, or
            replace entries; attempts raise TypeError. Inner dicts
            (the dict[filename → DataFrame] for a gtfs_schedule_zip
            input) are also wrapped, so
            `ctx.inputs["schedule"]["stops.txt"] = ...` raises too.
            Polars DataFrame values are NOT deep-frozen: a transform
            that calls a polars in-place method (drop_in_place, etc.)
            on a value could still mutate the underlying DataFrame.
            Polars' API is mostly functional/copy-on-assignment, so
            idiomatic usage is self-enforcing — the runtime guarantee
            stops at the dict layer.
        output: Mutable working set that transforms populate. The final
            state of ctx.output is what the worker serializes back as
            the run's artifact(s). Schedule pipelines seed it from an
            input (e.g. ctx.output = dict(ctx.inputs["schedule"])).
            RT pipelines populate it keyed by canonical feed name
            (vehicle_positions, trip_updates, service_alerts).
        environment: Current environment name (production, staging, etc.)
        config: Arbitrary pipeline configuration.
        metadata: Mutable dict for steps to share non-data state.
        id_mappings: Cross-file ID mapping for cascade operations
            (schedule only). Key: "{file}.{field}" (e.g.
            "routes.txt.route_id"); value: {old_id: new_id} or
            {removed_id: None}.
        findings: Every finding emitted during the run, in emission order —
            the cumulative record local runs and tests read. The executor
            additionally drains a per-step buffer into each
            StepResult.findings, which is what the worker transports. See
            specs/transform-framework.md §Findings.
    """

    inputs: Mapping[str, Any] = field(default_factory=dict)
    output: dict[str, Any] = field(default_factory=dict)
    environment: str = "development"
    config: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    id_mappings: dict[str, dict[str, str | None]] = field(default_factory=dict)
    findings: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        # Per-step emission state. Plain attributes, not dataclass fields:
        # they are framework bookkeeping, never something a caller passes in.
        self._step_findings: list[dict[str, Any]] = []
        self._current_step: Step | None = None
        # Defensive shallow copy + MappingProxyType wrap so transforms
        # can't mutate ctx.inputs at the dict level. Inner dicts (the
        # parsed shape of a gtfs_schedule_zip input is dict[filename →
        # DataFrame]) are also wrapped so a transform doing
        # `ctx.inputs["schedule"]["stops.txt"] = df` fails loudly too.
        # `dict(ctx.inputs["schedule"])` still works for the init-step
        # copy-into-output pattern — dict() on any Mapping produces a
        # fresh mutable dict, so InitScheduleOutput's
        # `ctx.output = dict(ctx.inputs[name])` is unaffected.
        if isinstance(self.inputs, MappingProxyType):
            return
        wrapped: dict[str, Any] = {}
        for k, v in self.inputs.items():
            wrapped[k] = MappingProxyType(dict(v)) if isinstance(v, dict) else v
        self.inputs = MappingProxyType(wrapped)

    # --- Findings (specs/transform-framework.md §Findings) ---

    def emit_finding(
        self,
        code: str,
        *,
        severity: str = DEFAULT_FINDING_SEVERITY,
        message: str = "",
        context: Mapping[str, Any] | None = None,
        occurrence_count: int = 1,
    ) -> None:
        """Report a structured problem this step observed.

        Performs no I/O, mutates no data, and **cannot fail the run**: every
        malformed argument is normalized and recorded on the finding's
        ``emit_errors`` rather than raised, and any unexpected internal error
        is swallowed with a log line. Emitting the same code with the same
        subject values twice is two *occurrences* of one finding, not two
        findings — the platform counts them per run
        (specs/issues.md §Finding counts).

        The emitter passes a flat context map and does NOT split grouping
        keys from detail: the platform selects the code's declared subject
        keys out of ``context`` and treats the rest as non-grouping detail,
        so one place decides grouping.

        Args:
            code: The finding code, matching a declared entry on the step.
                An undeclared code is still ingested (at class grain) and
                additionally reported — declaring is discoverability, not
                enforcement.
            severity: "error" | "warning" | "info"; anything else is coerced
                to "warning" and recorded.
            message: Human line. Free to interpolate run-specific values —
                the message never participates in grouping.
            context: Flat context map. Non-string values are stringified.
            occurrence_count: Occurrences this single call stands for
                (default 1), for emitters that already know their own total.
        """
        try:
            self._append_finding(code, severity, message, context, occurrence_count)
        except Exception:  # pragma: no cover - defensive: never reach the step
            logger.warning(
                "emit_finding(%r) failed and was dropped", code, exc_info=True
            )

    def _append_finding(
        self,
        code: Any,
        severity: Any,
        message: Any,
        context: Mapping[str, Any] | None,
        occurrence_count: Any,
    ) -> None:
        emit_errors: list[str] = []

        if not isinstance(code, str) or not code:
            emit_errors.append(f"code must be a non-empty string, got {code!r}")
            code = "invalid_finding_code"

        if severity not in FINDING_SEVERITIES:
            emit_errors.append(
                f"severity {severity!r} is not one of {list(FINDING_SEVERITIES)}; "
                f"recorded as {DEFAULT_FINDING_SEVERITY}"
            )
            severity = DEFAULT_FINDING_SEVERITY

        if not isinstance(message, str):
            message = str(message)

        flat: dict[str, str] = {}
        coerced: list[str] = []
        for key, value in (context or {}).items():
            name = key if isinstance(key, str) else str(key)
            if isinstance(value, str):
                flat[name] = value
            else:
                flat[name] = "" if value is None else str(value)
                coerced.append(name)
        if coerced:
            emit_errors.append(
                "non-string context value(s) stringified: " + ", ".join(sorted(coerced))
            )

        try:
            count = int(occurrence_count)
        except (TypeError, ValueError):
            emit_errors.append(
                f"occurrence_count {occurrence_count!r} is not an integer; "
                "recorded as 1"
            )
            count = 1
        if count < 1:
            emit_errors.append(f"occurrence_count {count} is below 1; recorded as 1")
            count = 1

        step = self._current_step
        declared = getattr(step, "declared_findings", None) if step else None
        undeclared = declared is not None and code not in declared

        finding: dict[str, Any] = {
            "code": code,
            "severity": severity,
            "message": message,
            "context": flat,
            "occurrence_count": count,
            "step": getattr(step, "name", "") if step else "",
            "undeclared": undeclared,
            "emit_errors": emit_errors,
        }
        self._step_findings.append(finding)
        self.findings.append(finding)

    def _begin_step(self, step: Step | None) -> None:
        """Framework hook: open a step's emission scope. Not for transforms."""
        self._current_step = step
        self._step_findings = []

    def _drain_step_findings(self) -> list[dict[str, Any]]:
        """Framework hook: take the current step's findings. Not for transforms."""
        drained = self._step_findings
        self._step_findings = []
        self._current_step = None
        return drained

    def add_id_mapping(
        self, file: str, field_name: str, old_id: str, new_id: str | None
    ) -> None:
        """Record an ID rename or removal for cross-file cascade.

        Args:
            file: GTFS filename (e.g. "routes.txt")
            field_name: Field name (e.g. "route_id")
            old_id: The original ID value
            new_id: The new ID value, or None if removed
        """
        key = f"{file}.{field_name}"
        if key not in self.id_mappings:
            self.id_mappings[key] = {}
        self.id_mappings[key][old_id] = new_id

    def get_id_mappings(self, file: str, field_name: str) -> dict[str, str | None]:
        """Get all ID mappings for a given file and field.

        Returns empty dict if no mappings exist.
        """
        return self.id_mappings.get(f"{file}.{field_name}", {})
