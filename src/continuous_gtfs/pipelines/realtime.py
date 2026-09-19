"""Realtime pipeline.

Seed per-feed outputs → transform (DAG) → encode PB + JSON per output.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from google.protobuf.json_format import MessageToJson
from google.transit import gtfs_realtime_pb2 as gtfs_rt

from ..context import PipelineContext
from ..executor import PipelineExecutor
from ..results import ExecutionResult
from ..step import Step


@dataclass
class RealtimeFeedArtifact:
    name: str
    entities: int
    pb: bytes
    json: str


@dataclass
class RealtimePipelineResult:
    input_entities: int = 0
    output_entities: int = 0
    input_size: int = 0
    feeds: list[RealtimeFeedArtifact] = field(default_factory=list)
    execution_result: ExecutionResult | None = None
    total_ms: float = 0.0


def run_realtime_pipeline(
    inputs: dict[str, Any],
    steps: list[Step],
    *,
    environment: str = "production",
    executor: PipelineExecutor | None = None,
    disabled_steps: list[str] | None = None,
) -> RealtimePipelineResult:
    """Run the RT pipeline against already-parsed inputs.

    Each input declared in the pipeline's INPUTS manifest as
    `gtfs_rt_protobuf` arrives as a parsed `FeedMessage`. `ctx.output`
    starts empty; the pipeline is expected to seed it in an init step
    (typically `@step(before="*")`), picking which inputs become
    outputs and under what names — e.g. canonical GTFS-RT names like
    `vehicle_positions` / `trip_updates`, or a multi-vendor merge into
    one output. The framework does not auto-mirror: silently emitting
    outputs keyed by whatever the agency named its inputs produces
    confusing artifact URLs and hides pipeline intent.

    Each output FeedMessage in `ctx.output` is encoded to both PB and
    JSON at the end — there is no combined feed. See
    realtime-pipeline.md §Per-Feed Outputs.
    """
    if executor is None:
        executor = PipelineExecutor(fail_fast=True)

    result = RealtimePipelineResult()
    pipeline_start = time.perf_counter()

    # Count input entities / bytes for reporting, but do not seed
    # ctx.output from them — the pipeline's init step owns that.
    for value in inputs.values():
        if isinstance(value, gtfs_rt.FeedMessage):
            result.input_entities += len(value.entity)
            result.input_size += value.ByteSize()

    # --- Transform (DAG execution) ---
    ctx = PipelineContext(
        inputs=dict(inputs),
        output={},
        environment=environment,
    )
    exec_result = executor.execute(steps, ctx, disabled_steps=disabled_steps)
    result.execution_result = exec_result

    # --- Encode every RT FeedMessage in ctx.output ---
    for name, value in ctx.output.items():
        if not isinstance(value, gtfs_rt.FeedMessage):
            continue
        pb = value.SerializeToString()
        json_str = MessageToJson(value, preserving_proto_field_name=True)
        result.feeds.append(
            RealtimeFeedArtifact(
                name=name,
                entities=len(value.entity),
                pb=pb,
                json=json_str,
            )
        )
        result.output_entities += len(value.entity)

    result.total_ms = _ms(pipeline_start)
    return result


def _ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000
