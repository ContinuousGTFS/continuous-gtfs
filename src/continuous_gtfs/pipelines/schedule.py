"""Schedule pipeline.

Seed output from schedule input → validate → transform (DAG) → validate
→ package.
"""

from __future__ import annotations

import io
import logging
import time
import zipfile
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from ..context import PipelineContext
from ..executor import PipelineExecutor
from ..results import ExecutionResult
from ..step import Step

logger = logging.getLogger(__name__)


@dataclass
class StageResult:
    name: str
    duration_ms: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SchedulePipelineResult:
    stages: list[StageResult] = field(default_factory=list)
    execution_result: ExecutionResult | None = None
    output: dict[str, pl.DataFrame] = field(default_factory=dict)
    total_ms: float = 0.0
    output_files: int = 0
    output_rows: int = 0
    output_zip: bytes = b""


def run_schedule_pipeline(
    inputs: dict[str, Any],
    steps: list[Step],
    *,
    environment: str = "production",
    executor: PipelineExecutor | None = None,
    quick: bool = False,
    disabled_steps: list[str] | None = None,
) -> SchedulePipelineResult:
    """Run the full schedule pipeline against already-parsed inputs.

    `inputs` is the already-parsed map of named inputs the pipeline
    declares in its INPUTS manifest — worker (production) and CLI (local)
    both run inputs through the parser registry before handing them off.

    `ctx.output` starts empty. The pipeline is expected to seed it in an
    init step — typically one using `before="*"` to guarantee ordering —
    that picks the right input(s) to seed from. The framework does not
    auto-seed from a convention-based guess: with multiple
    gtfs_schedule_zip inputs the right choice isn't unambiguous, and
    picking silently would produce wrong outputs rather than a clear
    failure. If `ctx.output` is still empty after the DAG runs,
    validate_output and package surface it as an explicit "missing
    required file" failure instead of a silent success with empty
    artifacts.

    Args:
        inputs: Named inputs, values already parsed per each asset's
            content_kind. gtfs_schedule_zip inputs are dict[str,
            DataFrame]; csv_table inputs are polars DataFrame; etc.
        steps: DAG-resolved steps to execute.
        environment: Pipeline environment name.
        executor: Optional pre-configured executor (for hooks). Defaults
            to continue mode.
        quick: Dev-loop mode. Skips validate_output and the output-zip
            packaging step. Caller produces a zip or diffs from
            result.output directly as needed.
    """
    if executor is None:
        executor = PipelineExecutor(fail_fast=False)

    result = SchedulePipelineResult()
    pipeline_start = time.perf_counter()

    # --- Transform (DAG execution) ---
    # ctx.output starts empty by design — the pipeline's own init step
    # (typically @step(before="*")) is responsible for seeding it from
    # whichever input(s) the pipeline cares about. No convention-based
    # auto-seed; see run_schedule_pipeline docstring.
    ctx = PipelineContext(
        inputs=dict(inputs),
        output={},
        environment=environment,
    )
    t0 = time.perf_counter()
    exec_result = executor.execute(steps, ctx, disabled_steps=disabled_steps)
    result.execution_result = exec_result
    output = {k: v for k, v in ctx.output.items() if isinstance(v, pl.DataFrame)}
    result.stages.append(
        StageResult(
            "transform",
            _ms(t0),
            {
                "steps_run": len(exec_result.steps),
                "success": exec_result.success,
                "total_ms": exec_result.total_duration_ms,
            },
        )
    )

    # --- Validate Output ---
    # Input validation was dropped along with auto-seed: there's no
    # single "input" to validate pre-DAG anymore, and validating the
    # post-init ctx.output is subsumed by validate_output.
    if not quick:
        t0 = time.perf_counter()
        output_issues = validate_gtfs(output)
        result.stages.append(
            StageResult(
                "validate_output",
                _ms(t0),
                {
                    "errors": output_issues["errors"],
                    "warnings": output_issues["warnings"],
                },
            )
        )

    # --- Package ---
    result.output_files = len(output)
    result.output_rows = sum(len(df) for df in output.values())
    if not quick:
        t0 = time.perf_counter()
        output_zip = package_zip(output)
        result.output_zip = output_zip
        result.stages.append(
            StageResult(
                "package",
                _ms(t0),
                {
                    "zip_size": len(output_zip),
                    "files": len(output),
                    "rows": result.output_rows,
                },
            )
        )

    result.output = output
    result.total_ms = _ms(pipeline_start)
    return result


def extract_zip(zip_bytes: bytes) -> dict[str, pl.DataFrame]:
    """Extract GTFS zip to dict of filename -> DataFrame (all string columns)."""
    datasets: dict[str, pl.DataFrame] = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if name.endswith(".txt"):
                with zf.open(name) as f:
                    try:
                        df = pl.read_csv(f, infer_schema_length=0)
                        datasets[name] = df
                    except Exception as e:
                        logger.warning("Skipping unparseable %s: %s", name, e)
    return datasets


def package_zip(datasets: dict[str, pl.DataFrame], *, compress: bool = True) -> bytes:
    """Package datasets back into a GTFS zip.

    Args:
        datasets: filename -> DataFrame map.
        compress: If True (default) use DEFLATE. If False, use ZIP_STORED
            (no compression) — faster, used for --quick dev runs when the
            zip is only produced for inspection, not for distribution.
    """
    buf = io.BytesIO()
    mode = zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED
    with zipfile.ZipFile(buf, "w", mode) as zf:
        for filename, df in sorted(datasets.items()):
            csv_bytes = df.write_csv().encode("utf-8")
            zf.writestr(filename, csv_bytes)
    return buf.getvalue()


def validate_gtfs(datasets: dict[str, pl.DataFrame]) -> dict[str, list[str]]:
    """Basic GTFS structural validation."""
    errors: list[str] = []
    warnings: list[str] = []

    required = {"agency.txt", "routes.txt", "trips.txt", "stops.txt", "stop_times.txt"}
    missing = required - set(datasets.keys())
    for f in sorted(missing):
        errors.append(f"Missing required file: {f}")

    required_fields = {
        "agency.txt": ["agency_name", "agency_url", "agency_timezone"],
        "routes.txt": ["route_id", "route_type"],
        "trips.txt": ["route_id", "service_id", "trip_id"],
        "stops.txt": ["stop_id"],
        "stop_times.txt": ["trip_id", "stop_id", "stop_sequence"],
    }
    for filename, fields in required_fields.items():
        if filename in datasets:
            for f in fields:
                if f not in datasets[filename].columns:
                    errors.append(f"{filename}: missing required field '{f}'")

    if "trips.txt" in datasets and "routes.txt" in datasets:
        trip_routes = set(datasets["trips.txt"]["route_id"].unique().to_list())
        valid_routes = set(datasets["routes.txt"]["route_id"].unique().to_list())
        orphan = trip_routes - valid_routes
        for r in sorted(orphan)[:5]:
            warnings.append(f"trips.txt references missing route_id: {r}")
        if len(orphan) > 5:
            warnings.append(f"... and {len(orphan) - 5} more orphan route_ids")

    if "stop_times.txt" in datasets and "trips.txt" in datasets:
        st_trips = set(datasets["stop_times.txt"]["trip_id"].unique().to_list())
        valid_trips = set(datasets["trips.txt"]["trip_id"].unique().to_list())
        orphan = st_trips - valid_trips
        if orphan:
            warnings.append(f"stop_times.txt references {len(orphan)} missing trip_ids")

    return {"errors": errors, "warnings": warnings}


def _ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000
