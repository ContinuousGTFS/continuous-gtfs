"""ctx.now — the context clock (specs/transform-framework.md §PipelineContext).

Covers the plumbing end to end: the PipelineContext field, the worker
reading the dispatch's ``evaluated_at`` (receipt time when absent), the
CLI's ``--now``, the cancellation builtins defaulting to ``ctx.now``, and
the guard that no module under ``continuous_gtfs`` reads the system clock
except the one framework seam (``continuous_gtfs.clock``).
"""

from __future__ import annotations

import argparse
import ast
import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from google.transit import gtfs_realtime_pb2 as gtfs_rt

from continuous_gtfs import PipelineContext, cli
from continuous_gtfs.builtins.realtime import (
    ExpireCancelledTrips,
    InsertMissingCancellations,
)
from continuous_gtfs.pipelines.realtime import run_realtime_pipeline
from continuous_gtfs.pipelines.schedule import run_schedule_pipeline
from continuous_gtfs.step import step

from .test_realtime_pipeline import _NOW_UTC_1000, _schedule_for, _tu_feed_with

SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "continuous_gtfs"

# ---------------------------------------------------------------------------
# PipelineContext.now
# ---------------------------------------------------------------------------


def test_ctx_now_defaults_to_tz_aware_utc_construction_time():
    before = datetime.now(UTC)
    ctx = PipelineContext()
    after = datetime.now(UTC)
    assert ctx.now.tzinfo is not None
    assert ctx.now.utcoffset() == timedelta(0)
    assert before <= ctx.now <= after


def test_ctx_now_normalizes_an_offset_moment_to_utc():
    pacific = timezone(timedelta(hours=-7))
    ctx = PipelineContext(now=datetime(2026, 9, 21, 1, 30, tzinfo=pacific))
    assert ctx.now == datetime(2026, 9, 21, 8, 30, tzinfo=UTC)
    assert ctx.now.utcoffset() == timedelta(0)


def test_ctx_now_rejects_a_naive_datetime():
    with pytest.raises(ValueError, match="tz-aware"):
        PipelineContext(now=datetime(2026, 9, 21, 1, 30))


# ---------------------------------------------------------------------------
# Cancellation builtins default to ctx.now
# ---------------------------------------------------------------------------


def _expire_fixture():
    feed = _tu_feed_with(
        [
            ("T_stale", gtfs_rt.TripDescriptor.CANCELED),
            ("T_active", gtfs_rt.TripDescriptor.CANCELED),
        ]
    )
    schedule = _schedule_for(
        [
            {"trip_id": "T_stale", "departure_time": "06:00:00"},
            {"trip_id": "T_stale", "departure_time": "07:00:00"},
            {"trip_id": "T_active", "departure_time": "09:00:00"},
            {"trip_id": "T_active", "departure_time": "11:00:00"},
        ]
    )
    return feed, schedule


def _expire_with_ctx_now(now: datetime) -> list[str]:
    feed, schedule = _expire_fixture()
    ctx = PipelineContext(
        inputs={"schedule": schedule}, output={"trip_updates": feed}, now=now
    )
    ExpireCancelledTrips().apply(ctx)
    return sorted(e.trip_update.trip.trip_id for e in ctx.output["trip_updates"].entity)


def test_expire_cancelled_trips_defaults_to_ctx_now():
    # At 10:00 agency-local T_stale (ended 07:00) is dropped, T_active kept;
    # at 06:30 both are still in their service window. Only ctx.now differs,
    # so the outcome proves the step read it (not the system clock).
    assert _expire_with_ctx_now(_NOW_UTC_1000) == ["T_active"]
    assert _expire_with_ctx_now(_NOW_UTC_1000.replace(hour=6, minute=30)) == [
        "T_active",
        "T_stale",
    ]


def test_expire_cancelled_trips_injected_now_beats_ctx_now():
    feed, schedule = _expire_fixture()
    ctx = PipelineContext(
        inputs={"schedule": schedule},
        output={"trip_updates": feed},
        now=_NOW_UTC_1000.replace(hour=6, minute=30),
    )
    ExpireCancelledTrips(now=_NOW_UTC_1000).apply(ctx)
    ids = [e.trip_update.trip.trip_id for e in ctx.output["trip_updates"].entity]
    assert ids == ["T_active"]


def _insert_with_ctx_now(now: datetime) -> set[str]:
    feed = _tu_feed_with([])
    schedule = _schedule_for(
        [
            {"trip_id": "T_missing", "departure_time": "09:30:00"},
            {"trip_id": "T_missing", "departure_time": "10:30:00"},
        ],
        trips_rows=[{"trip_id": "T_missing", "route_id": "R2"}],
    )
    ctx = PipelineContext(
        inputs={"schedule": schedule}, output={"trip_updates": feed}, now=now
    )
    InsertMissingCancellations(delay_seconds=300).apply(ctx)
    return {
        e.trip_update.trip.trip_id
        for e in ctx.output["trip_updates"].entity
        if e.trip_update.trip.schedule_relationship == gtfs_rt.TripDescriptor.CANCELED
    }


def test_insert_missing_cancellations_defaults_to_ctx_now():
    # 10:00: T_missing started 09:30, past the 5-minute delay → cancelled.
    # 09:00: it has not started yet → nothing inserted.
    assert _insert_with_ctx_now(_NOW_UTC_1000) == {"T_missing"}
    assert _insert_with_ctx_now(_NOW_UTC_1000.replace(hour=9)) == set()


# ---------------------------------------------------------------------------
# Runners and the CLI
# ---------------------------------------------------------------------------


def _clock_probe():
    seen: dict[str, datetime] = {}

    @step()
    def probe(ctx):
        seen["now"] = ctx.now

    return probe, seen


def test_run_realtime_pipeline_threads_now_into_ctx():
    probe, seen = _clock_probe()
    moment = datetime(2026, 9, 21, 8, 30, tzinfo=UTC)
    run_realtime_pipeline({}, [probe], now=moment)
    assert seen["now"] == moment


def test_run_schedule_pipeline_threads_now_into_ctx():
    probe, seen = _clock_probe()
    moment = datetime(2026, 9, 21, 8, 30, tzinfo=UTC)
    run_schedule_pipeline({}, [probe], now=moment, quick=True)
    assert seen["now"] == moment


def test_runner_without_now_uses_invocation_time():
    probe, seen = _clock_probe()
    before = datetime.now(UTC)
    run_realtime_pipeline({}, [probe])
    assert before <= seen["now"] <= datetime.now(UTC)


def test_parse_now_accepts_offsets_and_normalizes_to_utc():
    assert cli._parse_now("2026-09-21T01:30:00Z") == datetime(
        2026, 9, 21, 1, 30, tzinfo=UTC
    )
    assert cli._parse_now("2026-09-20T18:30:00-07:00") == datetime(
        2026, 9, 21, 1, 30, tzinfo=UTC
    )


@pytest.mark.parametrize("value", ["2026-09-21T01:30:00", "yesterday"])
def test_parse_now_refuses_naive_or_garbage(value):
    with pytest.raises(argparse.ArgumentTypeError):
        cli._parse_now(value)


@pytest.mark.parametrize("command", ["realtime", "schedule"])
def test_cli_now_flag_reaches_the_runner(monkeypatch, tmp_path, command):
    captured: dict[str, object] = {}

    def fake_runner(inputs, steps, **kw):
        captured.update(kw)
        raise SystemExit(0)  # stop before result rendering

    prep = SimpleNamespace(
        inputs={},
        steps=[],
        disabled_steps=[],
        selection_summary=None,
        unknown_disabled_steps=[],
    )
    monkeypatch.setattr(cli, "_prepare_run_or_exit", lambda args: prep)
    import continuous_gtfs.pipelines.realtime as rt_mod
    import continuous_gtfs.pipelines.schedule as sched_mod

    monkeypatch.setattr(rt_mod, "run_realtime_pipeline", fake_runner)
    monkeypatch.setattr(sched_mod, "run_schedule_pipeline", fake_runner)

    argv = ["cg", command, str(tmp_path), "--now", "2026-09-21T01:30:00Z"]
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit):
        cli.main()
    assert captured["now"] == datetime(2026, 9, 21, 1, 30, tzinfo=UTC)

    captured.clear()
    monkeypatch.setattr(sys, "argv", argv[:3])
    with pytest.raises(SystemExit):
        cli.main()
    assert captured["now"] is None  # runner falls back to invocation time


# ---------------------------------------------------------------------------
# Worker: dispatch evaluated_at → ctx.now
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# No module reads the system clock directly
# ---------------------------------------------------------------------------

#: The single framework seam allowed to read the wall clock
#: (see continuous_gtfs/clock.py for why the framework needs one).
_CLOCK_SEAM = "clock.py"

#: Framework modules allowed to import the seam — bookkeeping only (receipt
#: time, the runners' and the context's default clock, step telemetry,
#: analysis markers' written_at). Steps and builtins read ctx.now.
_SYSTEM_NOW_IMPORTERS = {
    "context.py",
    "executor.py",
    "pipelines/realtime.py",
    "pipelines/schedule.py",
    "schedule_pairing.py",
    "schedule_semantics.py",
}

#: Wall-clock reads. Duration timers (time.monotonic / perf_counter) measure
#: elapsed time, not the moment, and stay allowed.
_CLASS_CLOCK_METHODS = {"now", "utcnow", "today"}
_TIME_CLOCK_FUNCS = {"time", "time_ns", "localtime", "gmtime", "ctime", "asctime"}


def _bindings(tree: ast.AST) -> dict[str, str]:
    """Local name → what it is: "mod:datetime", "mod:time", "cls:datetime",
    "cls:date", or "fn:time.<name>" — import aliases resolved."""
    names: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in ("datetime", "time"):
                    names[alias.asname or alias.name] = f"mod:{alias.name}"
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            for alias in node.names:
                local = alias.asname or alias.name
                if node.module == "datetime" and alias.name in ("datetime", "date"):
                    names[local] = f"cls:{alias.name}"
                elif node.module == "time" and alias.name in _TIME_CLOCK_FUNCS:
                    names[local] = f"fn:time.{alias.name}"
    return names


def _resolve(expr: ast.expr, names: dict[str, str]) -> str | None:
    if isinstance(expr, ast.Name):
        return names.get(expr.id)
    if isinstance(expr, ast.Attribute):
        owner = _resolve(expr.value, names)
        if owner == "mod:datetime" and expr.attr in ("datetime", "date"):
            return f"cls:{expr.attr}"
    return None


def _clock_reads(source: str, label: str) -> list[str]:
    tree = ast.parse(source, filename=label)
    names = _bindings(tree)
    hits: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        hit = None
        if isinstance(func, ast.Name):
            # A bare call to a clock function imported from `time`.
            kind = names.get(func.id, "")
            if kind.startswith("fn:"):
                hit = f"{func.id}() [{kind[3:]}]"
        elif isinstance(func, ast.Attribute):
            owner = _resolve(func.value, names)
            if (
                owner in ("cls:datetime", "cls:date")
                and func.attr in _CLASS_CLOCK_METHODS
            ):
                hit = f"{owner[4:]}.{func.attr}()"
            elif owner == "mod:time" and func.attr in _TIME_CLOCK_FUNCS:
                hit = f"time.{func.attr}()"
            elif owner is None:
                # Unresolved owner spelled like the stdlib names: still a read.
                spelled = (
                    func.value.id
                    if isinstance(func.value, ast.Name)
                    else func.value.attr
                    if isinstance(func.value, ast.Attribute)
                    else None
                )
                if (
                    spelled in ("datetime", "date")
                    and func.attr in _CLASS_CLOCK_METHODS
                ) or (spelled == "time" and func.attr in _TIME_CLOCK_FUNCS):
                    hit = f"{spelled}.{func.attr}()"
        if hit:
            hits.append(f"{label}:{node.lineno} {hit}")
    return hits


def _imports_system_now(source: str) -> bool:
    """Whether a module imports the clock seam (``system_now`` or the
    ``clock`` module itself), by any spelling."""
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if (
                module == "clock"
                or module.endswith(".clock")
                or module == "continuous_gtfs.clock"
            ):
                return True
            if any(alias.name == "clock" for alias in node.names):
                return True
        elif isinstance(node, ast.Import):
            if any(alias.name.endswith(".clock") for alias in node.names):
                return True
    return False


def _framework_modules():
    for path in sorted(SRC_ROOT.rglob("*.py")):
        rel = path.relative_to(SRC_ROOT).as_posix()
        if rel.endswith("_pb2.py") or rel.endswith("_pb2_grpc.py"):
            continue
        yield rel, path.read_text()


def test_no_module_reads_the_system_clock_directly():
    hits: list[str] = []
    for rel, source in _framework_modules():
        if rel == _CLOCK_SEAM:
            continue
        hits.extend(_clock_reads(source, rel))
    assert hits == [], (
        "steps read ctx.now and framework bookkeeping goes through "
        "continuous_gtfs.clock.system_now — direct system-clock reads found:\n"
        + "\n".join(hits)
    )


def test_only_framework_modules_import_the_clock_seam():
    offenders = [
        rel
        for rel, source in _framework_modules()
        if rel != _CLOCK_SEAM
        and _imports_system_now(source)
        and rel not in _SYSTEM_NOW_IMPORTERS
    ]
    assert offenders == [], (
        "only framework bookkeeping may import continuous_gtfs.clock; steps and "
        f"builtins read ctx.now: {offenders}"
    )


def test_clock_guard_can_fail():
    """Counterfactual: the scanner flags each forbidden shape, aliases and
    bare imported calls included, and nothing else."""
    hits = _clock_reads(
        "import datetime as dt, time\n"
        "import time as clock_mod\n"
        "from datetime import date, datetime\n"
        "from datetime import datetime as DT\n"
        "from time import time as now, time_ns\n"
        "a = datetime.now()\n"
        "b = dt.datetime.now()\n"
        "c = date.today()\n"
        "d = time.time()\n"
        "e = DT.utcnow()\n"
        "f = now()\n"
        "g = time_ns()\n"
        "h = clock_mod.localtime()\n"
        "i = dt.date.today()\n"
        "ok1 = time.monotonic()\n"
        "ok2 = clock_mod.perf_counter()\n"
        "ok3 = DT.fromisoformat('2026-01-01T00:00:00+00:00')\n",
        "probe.py",
    )
    assert len(hits) == 9, hits
    assert not any(
        "monotonic" in h or "perf_counter" in h or "fromisoformat" in h for h in hits
    )


def test_clock_seam_import_guard_can_fail():
    for spelling in (
        "from ...clock import system_now\n",
        "from continuous_gtfs.clock import system_now as sn\n",
        "from .. import clock\n",
        "import continuous_gtfs.clock\n",
    ):
        assert _imports_system_now(spelling), spelling
    assert not _imports_system_now("from ...context import PipelineContext\n")


def test_a_check_reads_the_same_ctx_now_as_the_steps():
    """The checks phase builds each check's own context from the run's; the
    evaluation moment must carry over (specs/transform-framework.md
    §PipelineContext)."""
    from continuous_gtfs import check, resolve_dag
    from continuous_gtfs.executor import PipelineExecutor

    seen: dict[str, datetime] = {}

    @check()
    def clocked(ctx):
        seen["check"] = ctx.now

    @step()
    def stepped(ctx):
        seen["step"] = ctx.now

    clocked.name, stepped.name = "clocked", "stepped"
    moment = datetime(2026, 9, 21, 8, 30, tzinfo=UTC)
    PipelineExecutor().execute(
        resolve_dag([clocked, stepped]), PipelineContext(now=moment)
    )
    assert seen == {"check": moment, "step": moment}
