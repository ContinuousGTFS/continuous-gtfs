"""Shared pipeline-run preparation: scan, resolve, narrow, parse inputs.

The schedule and realtime CLI subcommands and embedders (test rigs,
ad-hoc runners) all do the same up-front sequence before handing off
to a pipeline runner:

  1. scan the pipeline directory + resolve the DAG
  2. (optionally) narrow the resolved DAG to one step + its ancestors
     via `--select`
  3. load the INPUTS manifest + parse supplied --input flags through
     the parser registry
  4. check every *required* declared input was supplied — a full-DAG
     run refuses to start without one; a `--select`-narrowed run only
     warns, because steps don't declare which inputs they read
  5. note which `--disable` names don't match any step in the
     (possibly narrowed) DAG so the caller can warn

`prepare_pipeline_run()` is the single entry point that returns a
`PreparedRun` bundle; callers pass it straight to
`run_schedule_pipeline` / `run_realtime_pipeline` (and surface the
unknown-disable list and select summary to the user however they
like). Pure function — raises `ValueError` / `FileNotFoundError` on
bad inputs rather than printing or exiting, so non-CLI callers stay
in control of error formatting.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .scanner import (
    InputSpec,
    load_inputs_manifest,
    manifest_kinds,
    required_input_names,
    resolve_dag,
    scan_pipeline,
    select_with_ancestors,
)
from .step import Step


@dataclass
class PreparedRun:
    """Everything a pipeline runner needs, computed once.

    Produced by `prepare_pipeline_run`. Embedders typically pass
    `.steps`, `.inputs`, and `.disabled_steps` straight to
    `run_schedule_pipeline` / `run_realtime_pipeline`. The CLI
    additionally uses `.selection_summary` and `.unknown_disabled_steps`
    to emit user-facing warnings.
    """

    pipeline_dir: Path
    all_steps: list[Step]
    steps: list[Step]
    # Flat name → content_kind view of the INPUTS manifest (what the
    # parser registry consumes). `input_specs` carries the full
    # normalized form including the optional flag.
    manifest: dict[str, str]
    inputs: dict[str, Any]
    disabled_steps: list[str]
    unknown_disabled_steps: list[str] = field(default_factory=list)
    selection: str | None = None
    input_specs: dict[str, InputSpec] = field(default_factory=dict)
    # Required inputs the caller didn't supply. Only ever non-empty on a
    # `--select`-narrowed run — a full-DAG run raises instead
    # (specs/transform-framework.md §INPUTS manifest).
    missing_required_inputs: list[str] = field(default_factory=list)

    @property
    def selection_summary(self) -> str | None:
        """One-line human summary of what `--select` narrowed to, or None."""
        if self.selection is None:
            return None
        dropped = len(self.all_steps) - len(self.steps)
        ancestors = len(self.steps) - 1
        return (
            f"Selected: {self.selection} + {ancestors} ancestor(s) — "
            f"{dropped} step(s) skipped"
        )


def prepare_pipeline_run(
    pipeline_dir: Path,
    *,
    select: str | None = None,
    inputs: Iterable[str] = (),
    disabled_steps: Iterable[str] = (),
) -> PreparedRun:
    """Scan + resolve + (optionally) narrow + parse, return a `PreparedRun`.

    Args:
        pipeline_dir: Path to the agency pipeline folder (must contain an
            `__init__.py` with an `INPUTS` manifest and at least one Step).
        select: Optional step name. When set, the resolved DAG is narrowed
            to that step plus its transitive ancestors via
            `select_with_ancestors`. Raises `ValueError` if the name
            doesn't match any step.
        inputs: Iterable of `NAME[:KIND]=PATH` strings, same shape as the
            CLI's `--input` flag. Parsed through the parser registry so the
            shape downstream transforms see matches what the worker produces.
        disabled_steps: Iterable of step names to disable. Forwarded
            verbatim — invalid names land in
            `PreparedRun.unknown_disabled_steps` for the caller to surface;
            they're still passed through to the executor, which treats
            unknowns as silent no-ops.

    Required inputs: every manifest entry not marked `optional` must be
    supplied. On a full-DAG run (`select is None`) a missing required
    input raises `ValueError` before anything runs. When `select` narrows
    the DAG, the missing names are recorded in
    `PreparedRun.missing_required_inputs` and a `UserWarning` is issued
    instead — steps don't declare which inputs they read, so the selected
    subset may not need it. Omitting an optional input is never an error;
    its name is simply absent from `.inputs`.

    Raises:
        ValueError: bad `--input` syntax, unknown input name, unknown
            content_kind override, unknown `--select` target, or a
            required input omitted on a full-DAG run.
        FileNotFoundError: a supplied --input path doesn't exist.
    """
    all_steps = resolve_dag(scan_pipeline(pipeline_dir))
    input_specs = load_inputs_manifest(pipeline_dir)
    manifest = manifest_kinds(input_specs)

    steps = all_steps
    if select is not None:
        steps = select_with_ancestors(all_steps, select)

    parsed_inputs = load_inputs(inputs, manifest)

    missing_required = [
        name for name in required_input_names(input_specs) if name not in parsed_inputs
    ]
    if missing_required:
        detail = (
            f"required input(s) not supplied via --input: {', '.join(missing_required)}"
        )
        if select is None:
            raise ValueError(
                f"Pipeline {pipeline_dir} {detail}. Mark an input optional in "
                f"INPUTS to allow omitting it "
                f"(specs/transform-framework.md §INPUTS manifest)."
            )
        warnings.warn(
            f"--select {select}: {detail} — proceeding because the run is "
            f"narrowed; the selected steps may not read them.",
            stacklevel=2,
        )

    disabled_list = list(disabled_steps)
    step_names = {s.name for s in steps}
    unknown = [s for s in disabled_list if s not in step_names]

    return PreparedRun(
        pipeline_dir=pipeline_dir,
        all_steps=all_steps,
        steps=steps,
        manifest=manifest,
        inputs=parsed_inputs,
        disabled_steps=disabled_list,
        unknown_disabled_steps=unknown,
        selection=select,
        input_specs=input_specs,
        missing_required_inputs=missing_required,
    )


def load_inputs(
    input_args: Iterable[str],
    manifest: Mapping[str, str | InputSpec],
) -> dict[str, Any]:
    """Parse `NAME[:KIND]=PATH` strings into a `{name: parsed_value}` dict.

    `manifest` may be the flat `{name: content_kind}` view or the
    normalized `{name: InputSpec}` form `load_inputs_manifest` returns —
    only the content_kind is read here. Required-ness is *not* enforced
    by this function (it can't know whether the run is narrowed); see
    `prepare_pipeline_run`.

    Resolution order per flag (first match wins):

    1. Explicit `:content_kind` suffix — escape hatch for handing a
       declared input data of a different shape than the manifest
       declares. Triggers `warnings.warn` so the override is visible.
    2. Manifest lookup — `content_kind` comes from `INPUTS[name]`.

    Every NAME must appear in `manifest`; unknown names raise
    `ValueError`. Each input's bytes run through the parser registry so
    the shape matches what the worker produces in production. See
    specs/transform-framework.md §Running Pipelines Locally.

    Raises:
        ValueError: malformed flag, empty name, unknown input name, or
            unknown content_kind override.
        FileNotFoundError: the input path doesn't exist.
    """
    from .parsers import known_kinds, parse

    valid_kinds = known_kinds()
    inputs: dict[str, Any] = {}
    for raw in input_args:
        if "=" not in raw:
            raise ValueError(f"Invalid --input: {raw!r}. Expected NAME[:KIND]=PATH.")
        spec, path_str = raw.split("=", 1)
        name, _, override_kind = spec.partition(":")
        name = name.strip()
        override_kind = override_kind.strip() or None
        path = Path(path_str)
        if not name:
            raise ValueError(f"Invalid --input: {raw!r}. Name cannot be empty.")
        if name not in manifest:
            raise ValueError(
                f"--input {name!r} is not declared in the pipeline's INPUTS "
                f"manifest. Declared inputs: {sorted(manifest)}"
            )
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        if override_kind and override_kind not in valid_kinds:
            raise ValueError(
                f"Unknown content_kind {override_kind!r} on --input {name}. "
                f"Known kinds: {sorted(valid_kinds)}"
            )

        declared = manifest[name]
        manifest_kind = (
            declared if isinstance(declared, str) else declared["content_kind"]
        )
        kind = override_kind or manifest_kind
        if override_kind and override_kind != manifest_kind:
            warnings.warn(
                f"--input {name}:{override_kind} overrides manifest kind "
                f"{manifest_kind!r} — the pipeline's transforms may fail if "
                f"they expect the declared shape.",
                stacklevel=2,
            )
        inputs[name] = parse(kind, path.read_bytes())
    return inputs
