# Writing Transforms

This walks through building a schedule pipeline from scratch. The same patterns apply to realtime — see the [realtime builtins](../reference/builtins-realtime.md) for the protobuf-specific API.

## Create a pipeline folder

A pipeline is a folder of `.py` files. No config file, no registry — the folder IS the pipeline definition.

```
my-agency/
  __init__.py          # FEED_TYPE and the INPUTS manifest (required)
  init.py              # seeds ctx.output from the schedule input
  calendar_cleanup.py
  route_metadata.py
  station_renames.py
```

Files are scanned alphabetically. The framework discovers all Step instances defined at module level.

## Seed the output

`ctx.output` starts empty. The framework does not decide which input the pipeline edits; an init step does, and every other step works on what it put there. For a schedule pipeline it is one builtin:

```python
# init.py
from continuous_gtfs.builtins.schedule import InitScheduleOutput

init = InitScheduleOutput("schedule")   # the input name from INPUTS
```

`InitScheduleOutput` always runs before every other step. A realtime pipeline writes its own init step with `@step(before="*")`, choosing which inputs become outputs and under what names (see [Concepts: Realtime](concepts.md#realtime)). Without an init step the output stays empty and the run fails output validation. The [minimal example](https://github.com/ContinuousGTFS/continuous-gtfs/tree/develop/examples/minimal-schedule) has the complete shape.

## Declare the pipeline's inputs

Every pipeline must declare its named inputs in `__init__.py` — the manifest is the pipeline's input contract:

```python
# my-agency/__init__.py
FEED_TYPE = "schedule"

INPUTS = {
    "schedule": {"content_kind": "gtfs_schedule_zip", "role": "primary"},
    "stop_overrides": "csv_table",
    "shuttle_schedule": {"content_kind": "gtfs_schedule_zip", "optional": True, "role": "supplemental"},
}
```

An entry's value is either a bare `content_kind` string or a mapping with `content_kind` and, optionally, `optional` and `role`:

- A **bare string declares a required input.** Every run must supply it (a local run fails before it starts without it, and so does a run on the platform), so the transform can read `ctx.inputs[name]` unconditionally.
- A **mapping with `optional: True` declares an optional input.** It may be left unbound, in which case its name is simply absent from `ctx.inputs` — there is no `None` placeholder. Any transform that reads it must tolerate the absence (`if "shuttle_schedule" in ctx.inputs:`, or iterate over whichever supplemental inputs are present).

The bare string is the common form. Mark an input optional only when the pipeline genuinely produces correct output without it.

A mapping may also declare a **`role`**, saying what the input is to the pipeline's output. Transforms never see it and the framework never changes how it runs because of it; it is there for tools that display the pipeline, which cannot tell a merged-in feed from a reference feed by its name or kind:

- **`"primary"`** — the input the output is seeded from (the schedule a schedule pipeline transforms). At most one per pipeline, and it must be required.
- **`"supplemental"`** — agency data merged into the output alongside the primary input, like a stations or shuttle feed. Required or optional.
- **No role** — a reference input the transforms read without merging it: a published feed a cross-release check compares against, a stop-overrides table. This is the default, and a bare string always means it.

A manifest with two primary inputs, an optional primary, or any other `role` value fails at scan time, naming the input.

Each `content_kind` determines the shape transforms see when they read the input:

| content_kind | `ctx.inputs[name]` shape |
|--------------|--------------------------|
| `gtfs_schedule_zip` | `dict[str, polars.DataFrame]` keyed by GTFS filename |
| `csv_table` | `polars.DataFrame` (all string columns) |
| `gtfs_rt_protobuf` | `gtfs_realtime_pb2.FeedMessage` |
| `opaque_bytes` | raw `bytes` |
| `file_tree` | `FileTree` — a read-only directory-tree root over the archived zip (stdlib `Traversable`: `is_dir()`/`is_file()`, `/` descends, `iterdir()` lists name-sorted, `rglob()` walks, `read_bytes()`/`read_text()` per file) |

Every run is validated against this manifest — every required input supplied, no undeclared inputs, each input's `content_kind` matching its declaration — and fails the run before any transform executes if a check fails. The CLI uses the same manifest to resolve `--input` flags in local runs (see [CLI: Inputs](cli.md#inputs)). Pipelines without an `INPUTS` manifest fail at scan time.

When the pipeline runs on the Continuous GTFS platform, each declared input is a **slot** that an operator binds to a feed source, so adding an input to the manifest is how a new slot comes into existence; see [Concepts: Declared inputs and slots](concepts.md#declared-inputs-and-slots).

!!! tip
    Organize files however makes sense for your team — by concern, by GTFS file, by who owns the rules. The framework doesn't care about file names or structure within the folder.

## Using builtins

Most transforms are parameterized builtins. Import and instantiate them as module-level variables:

```python
# calendar_cleanup.py
from continuous_gtfs.builtins.schedule import RemoveRows, MatchCondition

# Remove calendar entries with no active service days
remove_inactive = RemoveRows(
    "calendar.txt",
    [
        MatchCondition("monday", value="0"),
        MatchCondition("tuesday", value="0"),
        MatchCondition("wednesday", value="0"),
        MatchCondition("thursday", value="0"),
        MatchCondition("friday", value="0"),
        MatchCondition("saturday", value="0"),
        MatchCondition("sunday", value="0"),
    ],
    description="Remove calendar records with no active service days",
)

# Remove shuttle service IDs (regex match)
remove_shuttle = RemoveRows(
    "calendar.txt",
    [MatchCondition("service_id", regex=r"^SHUTTLE.*")],
    after=[remove_inactive],
)
```

```python
# route_metadata.py
from continuous_gtfs.builtins.schedule import MatchCondition, UpdateFields

# Set the green line's color
update_green_line = UpdateFields(
    "routes.txt",
    [MatchCondition("route_id", value="GREEN")],
    {"route_color": "00A651"},
)
```

The variable names (`remove_inactive`, `remove_shuttle`, `update_green_line`) become the step names in the DAG.

## Writing custom transforms

When builtins don't cover your use case, use the `@step` decorator:

```python
# station_renames.py
import polars as pl
from continuous_gtfs import step
from .route_metadata import update_green_line  # cross-file import for DAG ordering

@step(files=["stops.txt"], after=[update_green_line])
def normalize_stop_names(ctx):
    """Strip whitespace and normalize capitalization."""
    df = ctx.output["stops.txt"]
    ctx.output["stops.txt"] = df.with_columns(
        pl.col("stop_name").str.strip_chars().str.to_titlecase()
    )
```

The `@step` decorator returns a Step instance (not a function). The framework treats it identically to builtins.

### What your function receives

Your function gets a `PipelineContext` with two separate data handles:

- **`ctx.inputs`** — read-only, populated by the framework before the first step runs. Each entry is the already-parsed value for a named input declared in `INPUTS`. For a schedule pipeline whose `INPUTS["schedule"] = "gtfs_schedule_zip"`, `ctx.inputs["schedule"]` is a `dict[str, DataFrame]`. Required inputs are always present; an optional input that wasn't supplied has no entry, so check `name in ctx.inputs` (or use `ctx.inputs.get(name)`) before reading one.
- **`ctx.output`** — the mutable working set transforms populate and mutate. It starts empty, and the pipeline's init step fills it before any other step runs (see [Seed the output](#seed-the-output)).

Typical access patterns for a schedule transform:

- Read GTFS data: `df = ctx.output["stops.txt"]`
- Write GTFS data: `ctx.output["stops.txt"] = modified_df`
- Read reference inputs: `overrides = ctx.inputs["stop_overrides"]` (already parsed as a DataFrame)
- Share state across steps: `ctx.metadata["my_key"] = value`
- Record ID changes: `ctx.add_id_mapping("routes.txt", "route_id", "OLD", "NEW")`

DataFrames are Polars with all string columns. Use Polars operations for filtering and transformation.

## Declaring dependencies

Dependencies use Python object references, not string names:

```python
a = RemoveRows("routes.txt", [...])
b = UpdateFields("routes.txt", [...], after=[a])  # b runs after a
```

You can import steps from other files in the same pipeline folder using relative imports:

```python
# station_renames.py
from .calendar_cleanup import remove_inactive

@step(files=["stops.txt"], after=[remove_inactive])
def my_step(ctx):
    ...
```

The `before` parameter works in the opposite direction — "run me before these steps":

```python
@step(files=["stops.txt"], before=[main_transform])
def setup(ctx):   # runs before main_transform
    ...
```

!!! note
    Dependencies on steps outside the current pipeline folder are silently ignored. This means you can safely reference steps that may or may not be present.

## Reference-data inputs

Some transforms need reference data that isn't part of the GTFS feed — a CSV of stop description overrides exported from a spreadsheet, route metadata, etc. Declare them in the `INPUTS` manifest alongside the schedule, and they'll arrive parsed as `ctx.inputs[name]` for transforms to consume.

Declare in `__init__.py`:

```python
INPUTS = {
    "schedule": "gtfs_schedule_zip",
    "stop_overrides": "csv_table",
}
```

Supply on the command line:

```bash
continuous-gtfs schedule pipelines/schedule/ \
  --input schedule=feed.zip \
  --input stop_overrides=data/stop_overrides.csv \
  -o output.zip
```

Access from your transform:

```python
import polars as pl
from continuous_gtfs import step

@step(files=["stops.txt"])
def apply_stop_overrides(ctx):
    # Already parsed as a DataFrame per INPUTS[stop_overrides] = "csv_table"
    overrides = ctx.inputs["stop_overrides"]

    stops = ctx.output["stops.txt"]
    ctx.output["stops.txt"] = (
        stops
        .join(overrides, on="stop_id", how="left", suffix="_override")
        .with_columns([
            pl.coalesce(["stop_desc_override", "stop_desc"]).alias("stop_desc"),
        ])
        .drop("stop_desc_override")
    )
```

`stop_overrides` is declared with a bare string, so it is required: a run that omits it fails before any transform runs, locally or on the platform — no per-step guard needed. Had it been declared `{"content_kind": "csv_table", "optional": True}`, a run without it would proceed and the transform would have to check `"stop_overrides" in ctx.inputs` before reading it.

## Reporting problems: findings

A step can report a structured problem it observes — a referential-integrity
violation, a threshold breach, a realtime feed that doesn't line up with the
schedule — without mutating data and without failing the run. These are
**findings**.

Declare the codes a step can emit next to the files it operates on, then emit
through the context. A step whose only job is reporting findings is a
**check**: declare it with `@check` instead of `@step` (both import from
`continuous_gtfs`):

```python
@check(
    files=["stops.txt"],
    findings=[("stop_outside_service_area", {"subject": ["stop_id"]})],
)
def check_stop_bounds(ctx):
    for stop_id, lat, lon in offending_stops(ctx.output["stops.txt"]):
        ctx.emit_finding(
            "stop_outside_service_area",
            severity="warning",                       # "error" | "warning" | "info"
            message=f"Stop {stop_id} is outside the service area",
            context={"stop_id": stop_id, "lat": lat, "lon": lon},
        )
```

`subject` names the context keys that identify **what the finding is about**.
Name one when the finding is durably about a specific record the feed itself
names — a stop, a trip, a route — so the problem an operator tracks is *that
stop's* problem across runs. Omit it (a bare `"my_code"` entry) when the finding
describes a condition rather than a record, or when its subject is a pair or a
position (a stop-pair along a trip, a sequence index): those identities churn
with the data and multiply without bound.

Everything else in `context` travels as non-grouping detail. You pass one flat
map; the platform decides what groups.

Things worth knowing:

- **Emission can't fail your run.** No I/O, no exception path into the transform
  — a bad severity or a non-string value is normalized and reported, never
  raised.
- **A step that emits nothing costs nothing.**
- **Repeat emission is counting, not duplication.** Emitting the same code with
  the same subject twice in one run is two occurrences of one finding, so loop
  over offending records freely.
- **Findings are not the log channel.** `logging` output stays per-step
  diagnostic text; a finding is a claim about the data or the run worth tracking
  across runs.
- **Thresholds are your step's own parameters** — a builtin check takes its
  tolerances as constructor arguments, a custom step reads them from its own
  module. There is no platform-side per-check configuration.
- **Declaring is discoverability, not enforcement.** An undeclared code is still
  reported; the local CLI just warns that the declaration is missing.

A local run prints what was emitted, grouped by code, after the step table:

```
  Findings: 2 code(s), 47 occurrence(s)
    [warning] stop_outside_service_area (check_stop_bounds): 46 occurrence(s)
        Stop S01 is outside the service area
        …
```

On the platform the same emissions are aggregated into issues you can triage;
locally they are a per-run report and nothing more.

### Checks

A check is a kind of step: the one that reads the output, reports findings, and
cannot change the data. What `@check` changes compared with `@step`:

- **It runs after every transform**, concurrently with the other checks. It
  takes no `before` or `after` — passing either fails the pipeline load — and
  no transform may name a check in its own `before` / `after`.
- **It gets its own read-only copy of the output and the inputs.** `ctx.output["stops.txt"] =
  ...` raises; an in-place DataFrame or FeedMessage edit changes only the
  check's copy. The run's output never sees it. A local CLI run reports a
  check that changed its copy as a `check_mutated_view` error finding, so you
  notice before relying on a change that never happened.
- **Its return value is ignored.** `ctx.emit_finding` is the only way back.
- **A check that crashes doesn't fail the run.** The output still publishes;
  the crash is reported as a `check_failed` error finding on that check, so a
  review has to acknowledge it.

A step that needs the output *partway* through the pipeline — a count guard
before and after a removal, say — is a transform that emits findings: keep it a
`@step`. The scanner warns about a `@step` that declares findings and names no
files, since that is usually a check.

To convert an existing findings-only step, replace `@step` with `@check` and
drop `after="*"`. Its name, findings declaration and any `disabled_steps` entry
carry over. Builtin checks subclass `Check` the way builtin transforms subclass
`Step`.

## Testing locally

```bash
# Show the DAG
continuous-gtfs dag my-pipeline/

# Run against real data
continuous-gtfs schedule my-pipeline/ --input schedule=data/schedule.zip -o output.zip

# Check the output
unzip -l output.zip
```

For programmatic testing of a whole pipeline run:

```python
from pathlib import Path

from continuous_gtfs import resolve_dag, scan_pipeline
from continuous_gtfs.pipelines.schedule import extract_zip, run_schedule_pipeline

# my-pipeline/ includes its init step, so the output is seeded
steps = resolve_dag(scan_pipeline("my-pipeline/"))
inputs = {"schedule": extract_zip(Path("data/schedule.zip").read_bytes())}
result = run_schedule_pipeline(inputs, steps)
assert result.execution_result.success
```

To unit-test individual steps in isolation — the fast, hermetic path with no real feed — see [Testing Transforms](testing-transforms.md) and the `continuous_gtfs.testing` helpers.

## Mixing builtins and custom code

A pipeline folder commonly has a mix — builtins for the standard operations, custom `@step` functions for anything agency-specific:

```python
# transforms.py
from continuous_gtfs import step
from continuous_gtfs.builtins.schedule import RemoveRows, UpdateFields, MatchCondition

# Builtin: remove specific routes
remove_route = RemoveRows("routes.txt", [MatchCondition("route_id", value="OLD_ROUTE")])

# Builtin: update metadata
update_meta = UpdateFields(
    "routes.txt",
    [MatchCondition("route_id", value="NEW_ROUTE")],
    {"route_color": "FF0000"},
    after=[remove_route],
)

# Custom: complex logic that builtins can't express
@step(files=["stops.txt", "stop_times.txt"], after=[update_meta])
def remove_orphaned_stops(ctx):
    """Remove stops not referenced by any stop_time."""
    stops = ctx.output["stops.txt"]
    stop_times = ctx.output["stop_times.txt"]
    referenced = set(stop_times["stop_id"].unique().to_list())
    ctx.output["stops.txt"] = stops.filter(
        pl.col("stop_id").is_in(referenced)
    )
```
