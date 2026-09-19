"""CLI entrypoint for local pipeline execution."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

_LEVEL_COLORS = {
    "DEBUG": "\033[2m",  # dim
    "INFO": "",  # no color
    "WARNING": "\033[33m",  # yellow — matches warnings formatter
    "ERROR": "\033[31m",  # red
    "CRITICAL": "\033[1;31m",  # bold red
}
_RESET = "\033[0m"


def _install_warning_formatter() -> None:
    """Replace Python's default warnings formatter with a concise, colorized one.

    Default output:
        /path/to/file.py:123: UserWarning: some message
          warnings.warn(

    Our output:
        warning: some message          (yellow on TTY)
    """
    import warnings as _warnings

    use_color = sys.stderr.isatty()

    def _show(message, category, filename, lineno, file=None, line=None):
        target = file if file is not None else sys.stderr
        text = f"warning: {message}"
        if use_color:
            text = f"\033[33m{text}\033[0m"
        print(text, file=target)

    _warnings.showwarning = _show


def _install_logging_formatter(level: int | None = None) -> None:
    """Normalize Python logging output to match the warnings formatter.

    Output shape:
        <level>: <message>                  (color per level on TTY)

    Matches _install_warning_formatter's `warning: <message>` so stderr
    output across the CLI reads consistently regardless of whether it
    came from a `warnings.warn` call or a `logger.xxx()` call.

    Installs a single StreamHandler on the root logger (replacing any
    existing handlers). Safe to call multiple times.
    """
    import logging as _logging

    use_color = sys.stderr.isatty()

    class _ConciseFormatter(_logging.Formatter):
        def format(self, record: _logging.LogRecord) -> str:
            prefix = record.levelname.lower()
            text = f"{prefix}: {record.getMessage()}"
            if use_color:
                color = _LEVEL_COLORS.get(record.levelname, "")
                if color:
                    text = f"{color}{text}{_RESET}"
            if record.exc_info:
                text += "\n" + self.formatException(record.exc_info)
            return text

    root = _logging.getLogger()
    # Remove existing handlers so repeated installs don't double-emit.
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = _logging.StreamHandler(sys.stderr)
    handler.setFormatter(_ConciseFormatter())
    root.addHandler(handler)
    if level is not None:
        root.setLevel(level)


def main() -> None:
    _install_warning_formatter()
    _install_logging_formatter()

    parser = argparse.ArgumentParser(description="continuous-gtfs pipeline CLI")
    sub = parser.add_subparsers(dest="command")

    # schedule sub-command
    sched = sub.add_parser("schedule", help="Run schedule pipeline")
    sched.add_argument(
        "pipeline_dir", type=Path, help="Pipeline folder with Step definitions"
    )
    sched.add_argument("-o", "--output", type=Path, help="Output zip file")
    sched.add_argument("--env", default="production", help="Environment name")
    sched.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Stream step progress as pipeline runs",
    )
    sched.add_argument(
        "--diff-against",
        type=Path,
        metavar="BASELINE",
        help="After running, diff the output against this baseline zip",
    )
    sched.add_argument(
        "--diff-detail",
        action="store_true",
        help=(
            "Show per-row content changes in the diff output (requires --diff-against)"
        ),
    )
    sched.add_argument(
        "--diff-limit",
        type=int,
        default=50,
        metavar="N",
        help=(
            "Max rows shown per category per file in diff detail "
            "(default: 50, 0 = unlimited)"
        ),
    )
    sched.add_argument(
        "--json-events",
        type=Path,
        metavar="PATH",
        help=(
            "Write structured run events (stages, steps, validation, diff) "
            "to a JSON file"
        ),
    )
    sched.add_argument(
        "--input",
        action="append",
        default=[],
        dest="inputs",
        metavar="NAME[:KIND]=PATH",
        help=(
            "Supply a named input to the pipeline. NAME must be declared in "
            "the pipeline's INPUTS manifest; content_kind is resolved from the "
            "manifest by default, or overridden with an optional ':KIND' "
            "suffix. Repeatable."
        ),
    )
    sched.add_argument(
        "--select",
        metavar="STEP",
        help=(
            "Run only the named step plus its transitive dependencies "
            "(useful for isolating diff impact of a single step)"
        ),
    )
    sched.add_argument(
        "--quick",
        action="store_true",
        help=(
            "Dev-loop mode: skip input/output validation, skip zip "
            "compression on -o, and diff directly from in-memory Arrow "
            "tables. Not for verifying final correctness."
        ),
    )
    sched.add_argument(
        "--disable",
        action="append",
        default=[],
        dest="disabled_steps",
        metavar="STEP",
        help=(
            "Skip the named step (no-op its apply() but leave it in the DAG "
            "so dependency chains still satisfy). Mirrors the production "
            "`pipelines.disabled_steps` ops control — use locally to preview "
            "the behavior an operator-side disable would produce. Repeatable."
        ),
    )

    # diff sub-command
    diff = sub.add_parser("diff", help="Diff two GTFS zip archives")
    diff.add_argument("baseline", type=Path, help="Baseline GTFS zip")
    diff.add_argument("candidate", type=Path, help="Candidate GTFS zip to compare")
    diff.add_argument("--json", action="store_true", help="Output JSON")
    diff.add_argument(
        "--detail",
        action="store_true",
        help="Show per-row content changes with colorized field diffs",
    )
    diff.add_argument(
        "--limit",
        type=int,
        default=50,
        metavar="N",
        help="Max rows shown per category per file (default: 50, 0 = unlimited)",
    )

    # rt-compare sub-command
    rtcmp = sub.add_parser(
        "rt-compare", help="Semantically compare two GTFS-RT protobuf feeds"
    )
    rtcmp.add_argument("baseline", type=Path, help="Baseline GTFS-RT .pb file")
    rtcmp.add_argument("candidate", type=Path, help="Candidate GTFS-RT .pb file")
    rtcmp.add_argument("--json", action="store_true", help="Output JSON")
    rtcmp.add_argument(
        "--limit",
        type=int,
        default=50,
        metavar="N",
        help=(
            "Max per-entity differences shown in text output "
            "(default: 50, 0 = unlimited)"
        ),
    )
    rtcmp.add_argument(
        "--timestamp-tolerance",
        type=int,
        default=30,
        metavar="S",
        help="Header timestamp tolerance in seconds (default: 30)",
    )
    rtcmp.add_argument(
        "--entity-timestamp-tolerance",
        type=int,
        default=30,
        metavar="S",
        help="Entity-level timestamp tolerance in seconds (default: 30)",
    )
    rtcmp.add_argument(
        "--position-decimal-places",
        type=int,
        default=5,
        metavar="N",
        help=(
            "Latitude/longitude decimal places for comparison "
            "(default: 5 = ~1m precision)"
        ),
    )
    rtcmp.add_argument(
        "--stale-threshold",
        type=int,
        default=300,
        metavar="S",
        help="Entity-staleness threshold in seconds for Level 3 (default: 300)",
    )

    # realtime sub-command
    rt = sub.add_parser("realtime", help="Run realtime pipeline")
    rt.add_argument(
        "pipeline_dir", type=Path, help="Pipeline folder with Step definitions"
    )
    rt.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        help=(
            "Output directory; each ctx.output[name] is written as "
            "<name>.pb and <name>.json"
        ),
    )
    rt.add_argument("--env", default="production", help="Environment name")
    rt.add_argument(
        "--input",
        action="append",
        default=[],
        dest="inputs",
        metavar="NAME[:KIND]=PATH",
        help=(
            "Supply a named input to the pipeline. See schedule --input for "
            "resolution rules. Repeatable."
        ),
    )
    rt.add_argument(
        "--select",
        metavar="STEP",
        help=(
            "Run only the named step plus its transitive dependencies "
            "(useful for isolating diff impact of a single step)"
        ),
    )
    rt.add_argument(
        "--disable",
        action="append",
        default=[],
        dest="disabled_steps",
        metavar="STEP",
        help=(
            "Skip the named step (no-op its apply() but leave it in the DAG "
            "so dependency chains still satisfy). Mirrors the production "
            "`pipelines.disabled_steps` ops control — use locally to preview "
            "the behavior an operator-side disable would produce. Repeatable."
        ),
    )

    # dag sub-command
    dag = sub.add_parser("dag", help="Show DAG for a pipeline folder")
    dag.add_argument("pipeline_dir", type=Path, help="Pipeline folder")
    dag.add_argument("--json", action="store_true", help="Output ReactFlow JSON")
    dag.add_argument("--mermaid", action="store_true", help="Output Mermaid flowchart")

    # semantics sub-command
    sem = sub.add_parser(
        "semantics",
        help=(
            "Derive schedule-semantics tables (service_dates/patterns/"
            "time_profiles/trips) for a local zip or a digest"
        ),
    )
    sem.add_argument(
        "source",
        nargs="?",
        type=Path,
        help="Local GTFS zip to derive from (omit when using --digest)",
    )
    sem.add_argument(
        "--digest",
        metavar="FINGERPRINT",
        help=(
            "Feed digest to read canonical form for, instead of a local zip "
            "(requires --base-path)"
        ),
    )
    sem.add_argument(
        "--base-path",
        metavar="PATH",
        help=(
            "Canonical-form base path to read from with --digest "
            "(e.g. gs://bucket/schedule or a local exploded directory)"
        ),
    )
    sem.add_argument(
        "-o",
        "--output",
        type=Path,
        metavar="DIR",
        help=(
            "Directory to write the four semantic parquet files + "
            "metadata.json (skip-if-exists, per specs/schedule-semantics.md)"
        ),
    )
    sem.add_argument(
        "--json", action="store_true", help="Print row-count summary as JSON"
    )

    # pair-trips sub-command
    pair = sub.add_parser(
        "pair-trips",
        help=(
            "Pair a target and candidate feed's trips (schedule-semantics.md "
            "§Trip Pairing) into a trip_pairs table"
        ),
    )
    pair.add_argument(
        "target",
        nargs="?",
        type=Path,
        help="Target GTFS zip (omit when using --target-digest)",
    )
    pair.add_argument(
        "candidate",
        nargs="?",
        type=Path,
        help="Candidate GTFS zip (omit when using --candidate-digest)",
    )
    pair.add_argument(
        "--target-digest",
        metavar="FINGERPRINT",
        help="Target feed digest to read canonical form for, instead of a local zip",
    )
    pair.add_argument(
        "--candidate-digest",
        metavar="FINGERPRINT",
        help="Candidate feed digest to read canonical form for, instead of a local zip",
    )
    pair.add_argument(
        "--base-path",
        metavar="PATH",
        help=(
            "Canonical-form base path to read from with --target-digest/"
            "--candidate-digest (e.g. gs://bucket/schedule or a local exploded "
            "directory)"
        ),
    )
    pair.add_argument(
        "-o",
        "--output",
        type=Path,
        metavar="DIR",
        help=(
            "Directory to write trip_pairs.parquet + metadata.json "
            "(skip-if-exists, per specs/schedule-semantics.md)"
        ),
    )
    pair.add_argument(
        "--json", action="store_true", help="Print per-verdict row counts as JSON"
    )

    args = parser.parse_args()

    if args.command == "schedule":
        _run_schedule(args)
    elif args.command == "realtime":
        _run_realtime(args)
    elif args.command == "dag":
        _show_dag(args)
    elif args.command == "diff":
        _run_diff(args)
    elif args.command == "rt-compare":
        _run_rt_compare(args)
    elif args.command == "semantics":
        _run_semantics(args)
    elif args.command == "pair-trips":
        _run_pair_trips(args)
    else:
        parser.print_help()
        sys.exit(1)


def _run_schedule(args: argparse.Namespace) -> None:
    from .pipelines.schedule import run_schedule_pipeline

    prep = _prepare_run_or_exit(args)
    if prep.selection_summary:
        print(f"{prep.selection_summary}\n")
    _warn_unknown_disabled_steps(prep.unknown_disabled_steps)

    executor = (
        _build_verbose_cli_executor() if getattr(args, "verbose", False) else None
    )
    quick = getattr(args, "quick", False)

    result = run_schedule_pipeline(
        prep.inputs,
        prep.steps,
        environment=args.env,
        executor=executor,
        quick=quick,
        disabled_steps=prep.disabled_steps,
    )

    # --- Header ---
    suffix = " (quick)" if quick else ""
    print(f"\nSchedule pipeline{suffix}: {result.total_ms:.1f}ms")
    if prep.disabled_steps:
        print(f"  disabled steps: {', '.join(prep.disabled_steps)}")
    print(f"  Output: {result.output_files} files, {result.output_rows:,} rows")
    print()

    # --- Stage details ---
    for stage in result.stages:
        meta = stage.metadata

        if stage.name in ("validate_input", "validate_output"):
            errors = meta.get("errors", [])
            warnings = meta.get("warnings", [])
            label = (
                "Validate input"
                if stage.name == "validate_input"
                else "Validate output"
            )
            status = "pass" if not errors else f"{len(errors)} error(s)"
            print(f"  {label}: {stage.duration_ms:.1f}ms [{status}]")
            for msg in errors:
                print(f"    ERROR: {msg}")
            for msg in warnings:
                print(f"    WARN:  {msg}")

        elif stage.name == "transform":
            print(
                f"  Transform: {stage.duration_ms:.1f}ms "
                f"[{meta.get('steps_run', 0)} steps]"
            )
            if not getattr(args, "verbose", False):
                exec_result = result.execution_result
                if exec_result:
                    for sr in exec_result.steps:
                        _print_step_result(sr, prep.steps)

        elif stage.name == "ingest":
            file_list = meta.get("file_list", [])
            print(
                f"  Ingest: {stage.duration_ms:.1f}ms "
                f"[{meta.get('files', 0)} files, {meta.get('rows', 0):,} rows]"
            )
            if file_list:
                print(f"    Files: {', '.join(file_list)}")

        elif stage.name == "package":
            zip_kb = meta.get("zip_size", 0) / 1024
            print(f"  Package: {stage.duration_ms:.1f}ms [{zip_kb:.0f} KB]")

        else:
            print(f"  {stage.name}: {stage.duration_ms:.1f}ms")

    print()

    # --- Findings ---
    _print_findings(result.execution_result)

    # --- Output ---
    if args.output:
        # In quick mode, run_schedule_pipeline skipped packaging — create the
        # zip on demand now (uncompressed for speed since the user asked for
        # --quick). Otherwise use the result's pre-built zip.
        if quick:
            from .pipelines.schedule import package_zip

            output_bytes = package_zip(result.output, compress=False)
        else:
            output_bytes = result.output_zip

        if output_bytes:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_bytes(output_bytes)
            print(f"  Written to {args.output}")
    elif not quick:
        print("  Output not written (use -o/--output to save)")

    # --- Diff against baseline ---
    diff = None
    baseline = getattr(args, "diff_against", None)
    candidate_archive = None
    if baseline:
        print()
        print(f"  Loading baseline {baseline}...", end="", flush=True)
        diff_t0 = time.perf_counter()
        digester = _load_digester()
        baseline_archive = digester.GTFSArchive.from_zip(baseline)
        print(f" {(time.perf_counter() - diff_t0) * 1000:.0f}ms")

        print("  Building candidate archive...", end="", flush=True)
        t0 = time.perf_counter()
        if quick:
            # Arrow-native path for known GTFS files (skip CSV + zip round-trip).
            # For unknown files (e.g. modifications.txt, readme-style .txt
            # archives), the pyarrow CSV parser drops empty-valued rows that
            # polars' Arrow view preserves — that would surface as spurious
            # diffs vs. the baseline. Route those through from_csv_bytes so
            # the candidate matches the parse semantics used on the baseline.
            candidate_files = {}
            for name, df in result.output.items():
                schema = digester.get_schema(name)
                if schema is not None:
                    candidate_files[name] = digester.GTFSFile.from_arrow_table(
                        name,
                        df.to_arrow(),
                        schema,
                    )
                else:
                    candidate_files[name] = digester.GTFSFile.from_csv_bytes(
                        name,
                        df.write_csv().encode("utf-8"),
                        None,
                    )
            candidate_archive = digester.GTFSArchive(candidate_files)
        elif result.output_zip:
            candidate_archive = digester.GTFSArchive.from_zip(result.output_zip)
        print(f" {(time.perf_counter() - t0) * 1000:.0f}ms")

        if candidate_archive is not None:
            print("  Computing diff...", end="", flush=True)
            t0 = time.perf_counter()
            diff = baseline_archive.diff(candidate_archive)
            print(f" {(time.perf_counter() - t0) * 1000:.0f}ms")
            print()
            if getattr(args, "diff_detail", False):
                _print_archive_diff_rich(
                    diff,
                    baseline_archive,
                    candidate_archive,
                    baseline,
                    "(pipeline output)",
                    limit=args.diff_limit,
                    use_color=sys.stdout.isatty(),
                )
            else:
                _print_archive_diff(diff, baseline, "(pipeline output)")

    # --- Quick-mode nothing-to-show warning ---
    if quick and not baseline and not args.output:
        print(
            "  quick mode ran without --diff-against or -o "
            "— no diff or output produced",
            file=sys.stderr,
        )

    # --- JSON events dump ---
    events_path = getattr(args, "json_events", None)
    if events_path:
        events = _build_schedule_events(args, result, diff, baseline)
        events_path.parent.mkdir(parents=True, exist_ok=True)
        events_path.write_text(json.dumps(events, indent=2, default=str))
        print(f"  Events written to {events_path}")

    # --- Failure summary ---
    failed_steps = [
        sr
        for sr in (result.execution_result.steps if result.execution_result else [])
        if sr.status == "error"
    ]
    if failed_steps:
        bar = "━" * 60
        print(f"\n{bar}", file=sys.stderr)
        print(f"PIPELINE FAILED: {len(failed_steps)} step(s) errored", file=sys.stderr)
        print(bar, file=sys.stderr)
        for sr in failed_steps:
            print(f"  ✗ {sr.name}: {sr.error}", file=sys.stderr)
        print()

    # --- Exit code ---
    if not result.execution_result or not result.execution_result.success:
        sys.exit(1)
    if diff is not None and not diff.is_identical:
        sys.exit(1)


def _print_step_result(sr, steps) -> None:
    """Print a single step result with detail."""
    # Find the matching Step object for metadata
    step_obj = next((s for s in steps if s.name == sr.name), None)

    # Status indicator
    if sr.status == "success":
        indicator = "ok"
    elif sr.status == "skipped":
        indicator = "skip"
    else:
        indicator = "FAIL"

    # Row delta
    delta_str = ""
    if sr.input_rows is not None and sr.output_rows is not None:
        delta = sr.output_rows - sr.input_rows
        if delta != 0:
            sign = "+" if delta > 0 else ""
            delta_str = f" ({sign}{delta:,} rows)"

    # Step kind and description
    kind = ""
    if step_obj:
        kind = f" [{step_obj.builtin_name or '@step'}]"
        if step_obj.files:
            kind += f" {', '.join(step_obj.files)}"

    desc = ""
    if step_obj and step_obj.description:
        desc = f" — {step_obj.description.split(chr(10))[0]}"

    print(
        f"      {sr.step_index + 1}. {sr.name}{kind}: "
        f"{sr.duration_ms:.1f}ms [{indicator}]{delta_str}{desc}"
    )

    # Error detail
    if sr.error:
        print(f"         ERROR: {sr.error}")

    # Log messages
    for entry in sr.logs:
        print(f"         [{entry['level']}] {entry['message']}")


def _print_findings(exec_result) -> None:
    """Print the run's emitted findings grouped by code.

    Local runs render findings per run and stop there: no fingerprinting, no
    issues, no triage — those are cross-run constructs and therefore platform
    features (specs/principles.md §The local dev kit lives and dies by the
    run; specs/issues.md §Parking Lot). Grouping here is for legibility only,
    and it is also the early-warning surface for a step whose context churns
    every run.
    """
    if not exec_result:
        return
    findings = [f for sr in exec_result.steps for f in (sr.findings or [])]
    if not findings:
        return

    severity_rank = {"error": 0, "warning": 1, "info": 2}
    grouped: dict[tuple[str, str], list[dict]] = {}
    for f in findings:
        grouped.setdefault((f.get("code", ""), f.get("step", "")), []).append(f)

    total_occurrences = sum(f.get("occurrence_count", 1) for f in findings)
    print(f"  Findings: {len(grouped)} code(s), {total_occurrences} occurrence(s)")
    for code, step_name in sorted(
        grouped,
        key=lambda k: (
            min(severity_rank.get(f.get("severity", ""), 3) for f in grouped[k]),
            k[0],
        ),
    ):
        group = grouped[(code, step_name)]
        occurrences = sum(f.get("occurrence_count", 1) for f in group)
        worst = min(group, key=lambda f: severity_rank.get(f.get("severity", ""), 3))
        where = f" ({step_name})" if step_name else ""
        print(
            f"    [{worst.get('severity', 'warning')}] {code}{where}: "
            f"{occurrences} occurrence(s)"
        )
        for f in group[:3]:
            if f.get("message"):
                print(f"        {f['message']}")
        if len(group) > 3:
            print(f"        … {len(group) - 3} more")

    undeclared = sorted({f.get("code", "") for f in findings if f.get("undeclared")})
    if undeclared:
        print(
            "  warning: findings emitted for codes this step never declared "
            f"(add them to the step's findings=[…]): {', '.join(undeclared)}",
            file=sys.stderr,
        )
    emit_errors = [
        (f.get("code", ""), msg)
        for f in findings
        for msg in (f.get("emit_errors") or [])
    ]
    for code, msg in emit_errors[:10]:
        print(f"  warning: emit_finding({code!r}): {msg}", file=sys.stderr)
    print()


def _prepare_run_or_exit(args: argparse.Namespace):
    """Wrap `prepare_pipeline_run` for the CLI: catch the library's
    `ValueError` / `FileNotFoundError` and convert them to a printed
    message + exit code 2 instead of letting them propagate.
    """
    from .prep import prepare_pipeline_run

    try:
        return prepare_pipeline_run(
            args.pipeline_dir,
            select=getattr(args, "select", None),
            inputs=getattr(args, "inputs", []),
            disabled_steps=getattr(args, "disabled_steps", []) or [],
        )
    except (ValueError, FileNotFoundError) as e:
        print(str(e), file=sys.stderr)
        sys.exit(2)


def _warn_unknown_disabled_steps(unknown: list[str]) -> None:
    if unknown:
        print(
            "warning: --disable named steps not in this pipeline "
            "(will be silent no-ops): "
            f"{', '.join(unknown)}",
            file=sys.stderr,
        )


def _build_verbose_cli_executor():
    """Build a PipelineExecutor whose `before_step` / `after_step` hooks
    stream per-step progress to stdout — the shape used by `--verbose`."""
    from .context import PipelineContext
    from .executor import PipelineExecutor
    from .results import StepResult
    from .step import Step

    executor = PipelineExecutor(fail_fast=False)

    def _on_before_step(ctx: PipelineContext, step: Step) -> None:
        kind = step.builtin_name or "@step"
        files = ", ".join(step.files) if step.files else ""
        desc = f" — {step.description.split(chr(10))[0]}" if step.description else ""
        print(f"    ▸ {step.name} [{kind}] {files}{desc}", flush=True)

    def _on_after_step(ctx: PipelineContext, step: Step, sr: StepResult) -> None:
        delta_str = ""
        if sr.input_rows is not None and sr.output_rows is not None:
            delta = sr.output_rows - sr.input_rows
            if delta != 0:
                sign = "+" if delta > 0 else ""
                delta_str = f" ({sign}{delta:,} rows)"
        indicator = (
            "ok"
            if sr.status == "success"
            else "FAIL"
            if sr.status == "error"
            else "skip"
        )
        print(f"      {sr.duration_ms:.1f}ms [{indicator}]{delta_str}", flush=True)
        if sr.error:
            print(f"      ERROR: {sr.error}", flush=True)
        for entry in sr.logs:
            print(f"      [{entry['level']}] {entry['message']}", flush=True)

    executor.add_hook("before_step", _on_before_step)
    executor.add_hook("after_step", _on_after_step)
    return executor


def _build_schedule_events(args, result, diff, baseline) -> dict:
    """Assemble a structured JSON-serializable record of a schedule pipeline run."""
    exec_result = result.execution_result
    events = {
        "pipeline": "schedule",
        "environment": args.env,
        "pipeline_dir": str(args.pipeline_dir),
        "inputs": list(getattr(args, "inputs", [])),
        "output_zip": str(args.output) if args.output else None,
        "quick": bool(getattr(args, "quick", False)),
        "total_ms": result.total_ms,
        "success": exec_result.success if exec_result else None,
        "output": {
            "files": result.output_files,
            "rows": result.output_rows,
            "zip_bytes": len(result.output_zip) if result.output_zip else 0,
        },
        "stages": [
            {
                "name": s.name,
                "duration_ms": s.duration_ms,
                "metadata": s.metadata,
            }
            for s in result.stages
        ],
        "steps": [
            {
                "name": sr.name,
                "step_index": sr.step_index,
                "status": sr.status,
                "started_at": sr.started_at.isoformat() if sr.started_at else None,
                "completed_at": sr.completed_at.isoformat()
                if sr.completed_at
                else None,
                "duration_ms": sr.duration_ms,
                "input_rows": sr.input_rows,
                "output_rows": sr.output_rows,
                "error": sr.error,
                "error_type": sr.error_type,
                "logs": sr.logs,
                "metadata": sr.metadata,
                "findings": sr.findings,
            }
            for sr in (exec_result.steps if exec_result else [])
        ],
    }
    if diff is not None:
        events["diff"] = {
            "baseline": str(baseline),
            "identical": diff.is_identical,
            "added_files": sorted(diff.added_files),
            "removed_files": sorted(diff.removed_files),
            "unchanged_files": sorted(diff.unchanged_files),
            "modified_files": {
                name: {
                    "added": diff.file_diff(name).added_count,
                    "removed": diff.file_diff(name).removed_count,
                    "modified": diff.file_diff(name).modified_count,
                    "added_columns": list(diff.file_diff(name).added_columns),
                    "removed_columns": list(diff.file_diff(name).removed_columns),
                }
                for name in sorted(diff.modified_files)
            },
        }
    return events


def _load_digester():
    """Lazy import of gtfs_digester with a helpful error if missing."""
    try:
        import gtfs_digester
    except ImportError:
        print(
            "gtfs-digester is not installed. Install the diff extra:\n"
            "  uv pip install 'continuous-gtfs[dev]'\n"
            "  # or in a client pyproject.toml:\n"
            '  continuous-gtfs = { ..., extras = ["dev"] }',
            file=sys.stderr,
        )
        sys.exit(2)

    # Suppress the repetitive "Preserving unknown columns" UserWarnings —
    # they're structural notes about non-spec GTFS columns that fire once
    # per load call and add noise without helping the user reason about
    # a diff. Other gtfs_digester warnings are still shown.
    import warnings

    warnings.filterwarnings(
        "ignore",
        message="Preserving unknown columns",
        category=UserWarning,
    )
    return gtfs_digester


def _run_diff(args: argparse.Namespace) -> None:
    if args.json and args.detail:
        print("--json and --detail are mutually exclusive.", file=sys.stderr)
        sys.exit(2)

    digester = _load_digester()

    # JSON mode: emit only structured output — no progress lines on stdout.
    # Human mode: print progress on stderr so it doesn't mingle with the
    # archive's diff output on stdout (and so piping still produces clean output).
    show_progress = not args.json

    def _progress(label: str, fn):
        if show_progress:
            print(f"{label}...", end="", file=sys.stderr, flush=True)
        t0 = time.perf_counter()
        result = fn()
        if show_progress:
            print(f" {(time.perf_counter() - t0) * 1000:.0f}ms", file=sys.stderr)
        return result

    baseline = _progress(
        f"Loading baseline {args.baseline}",
        lambda: digester.GTFSArchive.from_zip(args.baseline),
    )
    candidate = _progress(
        f"Loading candidate {args.candidate}",
        lambda: digester.GTFSArchive.from_zip(args.candidate),
    )
    diff = _progress("Computing diff", lambda: baseline.diff(candidate))
    if show_progress:
        print("", file=sys.stderr)

    if args.json:
        payload = {
            "identical": diff.is_identical,
            "added_files": sorted(diff.added_files),
            "removed_files": sorted(diff.removed_files),
            "unchanged_files": sorted(diff.unchanged_files),
            "modified_files": {
                name: {
                    "added": diff.file_diff(name).added_count,
                    "removed": diff.file_diff(name).removed_count,
                    "modified": diff.file_diff(name).modified_count,
                    "added_columns": list(diff.file_diff(name).added_columns),
                    "removed_columns": list(diff.file_diff(name).removed_columns),
                }
                for name in sorted(diff.modified_files)
            },
        }
        print(json.dumps(payload, indent=2))
    elif args.detail:
        _print_archive_diff_rich(
            diff,
            baseline,
            candidate,
            args.baseline,
            args.candidate,
            limit=args.limit,
            use_color=sys.stdout.isatty(),
        )
    else:
        _print_archive_diff(diff, args.baseline, args.candidate)

    if not diff.is_identical:
        sys.exit(1)


def _print_archive_diff(diff, baseline_label, candidate_label) -> None:
    """Print an ArchiveDiff in human-readable form."""
    print(f"Diff: {baseline_label} vs {candidate_label}")

    if diff.is_identical:
        print("  Identical: yes")
        return

    print("  Identical: no")

    if diff.added_files:
        print(f"  Added files ({len(diff.added_files)}):")
        for name in sorted(diff.added_files):
            print(f"    + {name}")

    if diff.removed_files:
        print(f"  Removed files ({len(diff.removed_files)}):")
        for name in sorted(diff.removed_files):
            print(f"    - {name}")

    if diff.modified_files:
        print(f"  Modified files ({len(diff.modified_files)}):")
        for name in sorted(diff.modified_files):
            fd = diff.file_diff(name)
            print(f"    ~ {name}: {fd.summary()}")


# --- ANSI helpers ---

_ANSI = {
    "reset": "\033[0m",
    "bold": "\033[1m",
    "dim": "\033[2m",
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
}


def _color(text: str, style: str, enabled: bool) -> str:
    if not enabled:
        return text
    code = _ANSI.get(style)
    if not code:
        return text
    return f"{code}{text}{_ANSI['reset']}"


def _format_row(row: dict, primary_key: list[str]) -> str:
    """One-line row representation: PK fields first (bare), then others (quoted)."""
    pk_parts = [f"{k}={row.get(k, '')}" for k in primary_key]
    other_parts = [
        f'{k}="{v}"'
        for k, v in row.items()
        if k not in primary_key and v not in (None, "")
    ]
    return "  ".join(pk_parts + other_parts)


def _field_diffs(
    old_row: dict, new_row: dict, primary_key: list[str]
) -> list[tuple[str, str, str]]:
    """Fields whose values differ between old and new, excluding PK columns."""
    diffs: list[tuple[str, str, str]] = []
    pk_set = set(primary_key)
    all_cols = set(old_row.keys()) | set(new_row.keys())
    for col in sorted(all_cols):
        if col in pk_set:
            continue
        old_val = old_row.get(col)
        new_val = new_row.get(col)
        if old_val != new_val:
            diffs.append(
                (
                    col,
                    "" if old_val is None else str(old_val),
                    "" if new_val is None else str(new_val),
                )
            )
    return diffs


def _print_archive_diff_rich(
    diff,
    baseline_archive,
    candidate_archive,
    baseline_label,
    candidate_label,
    *,
    limit: int,
    use_color: bool,
) -> None:
    """Print an ArchiveDiff with per-row content detail and optional ANSI color."""
    print(f"Diff: {baseline_label} vs {candidate_label}")

    if diff.is_identical:
        print("  " + _color("Identical: yes", "green", use_color))
        return

    print("  Identical: no")

    # Files added/removed entirely — terse one-line treatment
    if diff.added_files:
        print()
        print(_color(f"Added files ({len(diff.added_files)}):", "bold", use_color))
        for name in sorted(diff.added_files):
            print(_color(f"  + {name}", "green", use_color))

    if diff.removed_files:
        print()
        print(_color(f"Removed files ({len(diff.removed_files)}):", "bold", use_color))
        for name in sorted(diff.removed_files):
            print(_color(f"  - {name}", "red", use_color))

    # Modified files — expanded per-row detail
    for name in sorted(diff.modified_files):
        fd = diff.file_diff(name)
        print()
        header = (
            f"~ {name}  "
            f"(+{fd.added_count} / -{fd.removed_count} / ~{fd.modified_count})"
        )
        print(_color(header, "bold", use_color))

        if fd.added_columns or fd.removed_columns:
            schema_parts = []
            if fd.added_columns:
                schema_parts.append(
                    _color(f"+{', +'.join(fd.added_columns)}", "green", use_color)
                )
            if fd.removed_columns:
                schema_parts.append(
                    _color(f"-{', -'.join(fd.removed_columns)}", "red", use_color)
                )
            print("  " + _color("schema: ", "dim", use_color) + " ".join(schema_parts))

        try:
            primary_key = candidate_archive[name].schema.primary_key
        except (KeyError, AttributeError):
            primary_key = []
        if primary_key:
            print(
                "  "
                + _color(f"primary key: {', '.join(primary_key)}", "dim", use_color)
            )

        _render_category(
            "Removed",
            fd.removed,
            primary_key,
            limit,
            bullet="-",
            color="red",
            use_color=use_color,
        )
        _render_category(
            "Added",
            fd.added,
            primary_key,
            limit,
            bullet="+",
            color="green",
            use_color=use_color,
        )

        if fd.modified_count > 0:
            _render_modified(
                name,
                fd.modified,
                baseline_archive,
                primary_key,
                limit,
                use_color,
            )


def _render_category(
    label, table, primary_key, limit, *, bullet, color, use_color
) -> None:
    total = table.num_rows
    if total == 0:
        return
    shown = total if limit == 0 else min(total, limit)
    rows = table.slice(0, shown).to_pylist()
    print()
    print(f"  {label} ({total}):")
    for row in rows:
        line = f"    {bullet} {_format_row(row, primary_key)}"
        print(_color(line, color, use_color))
    if shown < total:
        remaining = total - shown
        print(
            _color(
                f"    ... and {remaining:,} more (use --limit 0 to see all)",
                "dim",
                use_color,
            )
        )


def _render_modified(
    filename, modified_table, baseline_archive, primary_key, limit, use_color
) -> None:
    total = modified_table.num_rows
    if total == 0:
        return

    # Build lookup by PK tuple from the baseline's canonical table
    old_rows_by_key: dict[tuple, dict] = {}
    if primary_key:
        try:
            old_table = baseline_archive.arrow_table(filename)
            for row in old_table.to_pylist():
                key = tuple(row.get(k) for k in primary_key)
                old_rows_by_key[key] = row
        except (KeyError, ValueError):
            pass

    shown = total if limit == 0 else min(total, limit)
    rows = modified_table.slice(0, shown).to_pylist()
    print()
    print(f"  Modified ({total}):")
    for new_row in rows:
        key = tuple(new_row.get(k) for k in primary_key) if primary_key else ()
        pk_label = (
            ", ".join(f"{k}={new_row.get(k, '')}" for k in primary_key)
            if primary_key
            else "(no pk)"
        )
        print(_color(f"    ~ {pk_label}", "yellow", use_color))

        old_row = old_rows_by_key.get(key)
        if old_row is None:
            # Baseline row not found — fall back to printing the new row compactly
            print(
                _color(
                    f"        (old row not found) {_format_row(new_row, primary_key)}",
                    "dim",
                    use_color,
                )
            )
            continue

        for field, old_val, new_val in _field_diffs(old_row, new_row, primary_key):
            old_s = _color(f'"{old_val}"', "red", use_color)
            new_s = _color(f'"{new_val}"', "green", use_color)
            arrow = _color("→", "dim", use_color)
            print(f"        {field}: {old_s} {arrow} {new_s}")

    if shown < total:
        remaining = total - shown
        print(
            _color(
                f"    ... and {remaining:,} more (use --limit 0 to see all)",
                "dim",
                use_color,
            )
        )


def _run_realtime(args: argparse.Namespace) -> None:
    from .pipelines.realtime import run_realtime_pipeline

    prep = _prepare_run_or_exit(args)
    if prep.selection_summary:
        print(f"{prep.selection_summary}\n")
    _warn_unknown_disabled_steps(prep.unknown_disabled_steps)

    result = run_realtime_pipeline(
        prep.inputs,
        prep.steps,
        environment=args.env,
        disabled_steps=prep.disabled_steps,
    )

    print(f"RT pipeline: {result.total_ms:.1f}ms")
    if prep.disabled_steps:
        print(f"  disabled steps: {', '.join(prep.disabled_steps)}")
    print(f"  Input entities:  {result.input_entities}")
    print(f"  Output entities: {result.output_entities}")
    for feed in result.feeds:
        print(
            f"    {feed.name}: {feed.entities} entities  "
            f"({len(feed.pb)}B pb / {len(feed.json)}B json)"
        )

    output_dir = getattr(args, "output_dir", None)
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        for feed in result.feeds:
            pb_path = output_dir / f"{feed.name}.pb"
            json_path = output_dir / f"{feed.name}.json"
            pb_path.write_bytes(feed.pb)
            json_path.write_text(feed.json)
            print(f"  Written {pb_path} + {json_path}")

    # --- Findings ---
    _print_findings(result.execution_result)

    # --- Failure summary ---
    failed_steps = [
        sr
        for sr in (result.execution_result.steps if result.execution_result else [])
        if sr.status == "error"
    ]
    if failed_steps:
        bar = "━" * 60
        print(f"\n{bar}", file=sys.stderr)
        print(f"PIPELINE FAILED: {len(failed_steps)} step(s) errored", file=sys.stderr)
        print(bar, file=sys.stderr)
        for sr in failed_steps:
            print(f"  ✗ {sr.name}: {sr.error}", file=sys.stderr)
        print()

    # --- Exit code ---
    if not result.execution_result or not result.execution_result.success:
        sys.exit(1)


def _show_dag(args: argparse.Namespace) -> None:
    from .scanner import dag_edges, export_reactflow, resolve_dag, scan_pipeline

    steps = resolve_dag(scan_pipeline(args.pipeline_dir))

    if args.json:
        rf = export_reactflow(steps)
        print(json.dumps(rf, indent=2))
    elif args.mermaid:
        print("graph TD")
        for s in steps:
            label = s.description.split("\n")[0] if s.description else s.name
            node_id = s.name
            print(f'    {node_id}["{label}"]')
        # The effective edge list — wildcard after/before declarations
        # expanded exactly as execution orders them (#683: iterating a
        # literal "*" here crashed the listing).
        for a, b in dag_edges(steps):
            if a.name and b.name:
                print(f"    {a.name} --> {b.name}")
    else:
        print(f"DAG: {len(steps)} steps")
        for i, s in enumerate(steps):
            kind = "builtin" if s.is_builtin else "custom"
            deps = (
                "*" if s.after == "*" else ", ".join(d.name for d in s.after if d.name)
            )
            dep_str = f" (after: {deps})" if deps else ""
            print(f"  {i + 1}. {s.name} [{kind}] {s.files}{dep_str}")


def _run_rt_compare(args: argparse.Namespace) -> None:
    """Semantic comparison of two GTFS-RT protobuf feeds.

    Exit code: 0 if functionally equivalent (level >= 0), 1 if differences
    fall outside any of the four equivalence levels. Mirrors the
    schedule `diff` subcommand's exit convention. See
    specs/realtime-pipeline.md §Equivalence Validation.
    """
    from .rt_compare import ComparisonConfig, compare_feeds

    if not args.baseline.exists():
        print(f"baseline not found: {args.baseline}", file=sys.stderr)
        sys.exit(2)
    if not args.candidate.exists():
        print(f"candidate not found: {args.candidate}", file=sys.stderr)
        sys.exit(2)

    config = ComparisonConfig(
        timestamp_tolerance_seconds=args.timestamp_tolerance,
        entity_timestamp_tolerance_seconds=args.entity_timestamp_tolerance,
        position_decimal_places=args.position_decimal_places,
        stale_threshold_seconds=args.stale_threshold,
    )

    baseline_bytes = args.baseline.read_bytes()
    candidate_bytes = args.candidate.read_bytes()
    report = compare_feeds(baseline_bytes, candidate_bytes, config)

    if args.json:
        print(report.to_json())
    else:
        text = report.to_text()
        if args.limit > 0 and len(report.differences) > args.limit:
            # Trim differences list before printing — to_text iterates the
            # full list. Override the in-memory list with a slice; print a
            # trailing "… N more" line.
            full = report.differences
            report.differences = full[: args.limit]
            print(report.to_text())
            print(f"  … and {len(full) - args.limit} more (use --limit 0 to see all)")
        else:
            print(text)

    # Exit code: 0 if any of the 4 equivalence levels held; 1 otherwise.
    # Mirrors the schedule diff CLI semantics (0 on identical, 1 on diffs).
    sys.exit(0 if report.equivalence_level >= 0 else 1)


def _run_semantics(args: argparse.Namespace) -> None:
    from .schedule_semantics import (
        SEMANTICS_REV,
        derive_semantics,
        tables_from_archive,
        tables_from_digest,
        write_semantics,
    )

    if args.digest:
        if not args.base_path:
            print("--digest requires --base-path", file=sys.stderr)
            sys.exit(2)
        feed_digest = args.digest
        tables = tables_from_digest(args.base_path, feed_digest)
    else:
        if not args.source:
            print("Provide a local zip, or --digest with --base-path.", file=sys.stderr)
            sys.exit(2)
        digester = _load_digester()
        archive = digester.GTFSArchive.from_zip(args.source.read_bytes())
        feed_digest = archive.fingerprint.root_hash
        tables = tables_from_archive(archive)

    derived = derive_semantics(tables)

    written = None
    if args.output:
        written = write_semantics(derived, str(args.output), feed_digest)

    if args.json:
        payload = {
            "feed_digest": feed_digest,
            "rev": SEMANTICS_REV,
            "row_counts": {name: df.height for name, df in derived.items()},
            "written": written,
        }
        print(json.dumps(payload, indent=2))
        return

    print(f"Feed digest: {feed_digest} (rev {SEMANTICS_REV})")
    for name, df in derived.items():
        print(f"  {name}: {df.height:,} rows")
    if args.output:
        if written:
            print(f"  Written to {args.output}")
        else:
            print(f"  {args.output}: digest already present, skipped (skip-if-exists)")


def _run_pair_trips(args: argparse.Namespace) -> None:
    from .schedule_pairing import PAIRING_REV, pair_trips, write_trip_pairs
    from .schedule_semantics import (
        derive_semantics,
        tables_from_archive,
        tables_from_digest,
    )

    def _load(source: Path | None, digest: str | None, label: str):
        if digest:
            if not args.base_path:
                print(f"--{label}-digest requires --base-path", file=sys.stderr)
                sys.exit(2)
            return digest, tables_from_digest(args.base_path, digest)
        if not source:
            print(
                f"Provide a local {label} zip, or --{label}-digest with --base-path.",
                file=sys.stderr,
            )
            sys.exit(2)
        digester = _load_digester()
        archive = digester.GTFSArchive.from_zip(source.read_bytes())
        return archive.fingerprint.root_hash, tables_from_archive(archive)

    target_digest, target_canon = _load(args.target, args.target_digest, "target")
    candidate_digest, candidate_canon = _load(
        args.candidate, args.candidate_digest, "candidate"
    )

    target_semantic = derive_semantics(target_canon)
    candidate_semantic = derive_semantics(candidate_canon)
    pairs = pair_trips(
        target_semantic,
        target_canon.get("trips"),
        candidate_semantic,
        candidate_canon.get("trips"),
    )

    written = None
    if args.output:
        written = write_trip_pairs(
            pairs, str(args.output), target_digest, candidate_digest
        )

    counts = {
        row["change"]: row["len"] for row in pairs.group_by("change").len().to_dicts()
    }

    if args.json:
        payload = {
            "target_digest": target_digest,
            "candidate_digest": candidate_digest,
            "rev": PAIRING_REV,
            "row_count": pairs.height,
            "counts": counts,
            "written": written,
        }
        print(json.dumps(payload, indent=2))
        return

    print(
        f"Target digest: {target_digest}\n"
        f"Candidate digest: {candidate_digest} (rev {PAIRING_REV})"
    )
    print(f"  trip_pairs: {pairs.height:,} rows")
    for kind in ("added", "removed", "modified", "unchanged"):
        print(f"    {kind}: {counts.get(kind, 0):,}")
    if args.output:
        if written:
            print(f"  Written to {args.output}")
        else:
            print(f"  {args.output}: pair already present, skipped (skip-if-exists)")


