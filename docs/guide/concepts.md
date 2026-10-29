# Concepts

## Steps

A **Step** is the fundamental unit of pipeline work. Every transform — whether a parameterized builtin or a custom function — is a Step instance.

Steps have a name (inferred from the Python variable name), a list of files they operate on, and optional dependency declarations:

```python
from continuous_gtfs import Step

class MyBuiltin(Step):
    files = ["routes.txt"]
    description = "Does something to routes"

    def apply(self, ctx):
        df = ctx.output["routes.txt"]
        # ... modify df ...
        ctx.output["routes.txt"] = df
```

Most of the time you'll use **builtins** (pre-built Step subclasses) or the **`@step` decorator** rather than subclassing Step directly.

## Builtins vs custom code

**Builtins** are parameterized Step subclasses for common operations:

```python
from continuous_gtfs.builtins.schedule import RemoveRows, MatchCondition

remove_inactive = RemoveRows(
    "calendar.txt",
    [MatchCondition("monday", value="0"), MatchCondition("tuesday", value="0"), ...],
)
```

**Custom code** uses the `@step` decorator for anything builtins don't cover:

```python
from continuous_gtfs import step

@step(files=["stops.txt"])
def normalize_stop_names(ctx):
    df = ctx.output["stops.txt"]
    ctx.output["stops.txt"] = df.with_columns(
        pl.col("stop_name").str.strip_chars()
    )
```

Both produce Step instances. The framework treats them identically — they're indistinguishable in the DAG.

## The DAG

Steps declare dependencies using `after` and `before` lists, which reference other Step objects directly:

```python
update_feed = UpdateFeedInfo(publisher_name="My Agency")

remove_stops = RemoveRows("stops.txt", [...], after=[update_feed])
```

The framework resolves these into a directed acyclic graph (DAG) and executes steps in topological order. Within a dependency level, steps are sorted by `(priority, name)` for determinism.

A step with no declared dependencies is ordered among the other independent steps by `priority` (lower runs earlier, default `100`) and then by name. That is usually enough, since most transforms do not depend on each other.

Circular dependencies are detected at scan time and raise an error.

## PipelineContext

Every step receives a `PipelineContext` with:

| Field | Type | Description |
|-------|------|-------------|
| `inputs` | `dict[str, Any]` | Read-only, already-parsed named inputs keyed by the pipeline's `INPUTS` manifest. Shape depends on each input's `content_kind`: `gtfs_schedule_zip` → `dict[str, DataFrame]`, `csv_table` → `DataFrame`, `gtfs_rt_protobuf` → `FeedMessage`, `opaque_bytes` → raw `bytes`, `file_tree` → `FileTree` (read-only `Traversable` root over the archived zip). Required inputs are always present; an optional input that wasn't supplied has no entry (see [Declared inputs and slots](#declared-inputs-and-slots)). |
| `output` | `dict[str, Any]` | Mutable working set transforms populate and mutate. It **starts empty**: the pipeline seeds it in an init step that runs before every other step, [`InitScheduleOutput`](../reference/builtins-schedule.md#init) for a schedule pipeline or an `@step(before="*")` for a realtime one (see [Pipeline types](#pipeline-types)). The final state of `ctx.output` is what the run serializes as its output. |
| `environment` | `str` | Name of the environment the run belongs to (`production` by default in local runs; set with `--env`) |
| `config` | `dict` | Arbitrary pipeline configuration |
| `metadata` | `dict` | Mutable dict for steps to share non-data state |
| `id_mappings` | `dict` | Cross-file ID mappings for cascade operations (see [ID Mappings](id-mappings.md)) |
| `findings` | `list[dict]` | Every finding emitted so far in the run, in emission order. Steps add to it with `ctx.emit_finding(...)` (see [Writing Transforms: Reporting problems](writing-transforms.md#reporting-problems-findings)) |
| `now` | `datetime` | The evaluation moment, timezone-aware UTC. Steps that reason about time read `ctx.now`, never the system clock, so a run can be repeated as of a past moment (`--now` on the CLI; invocation time by default) |

Transforms read inputs and mutate output — never the other way around. Reference-data inputs (CSVs, secondary schedules, etc.) stay in `ctx.inputs[name]`; the GTFS files a schedule pipeline is editing live in `ctx.output["stops.txt"]` etc.

The context also provides helpers:

- `ctx.add_id_mapping(file, field, old, new)` / `ctx.get_id_mappings(file, field)` — see [ID Mappings](id-mappings.md)

## Declared inputs and slots

The `INPUTS` manifest in a pipeline's `__init__.py` names each input and its `content_kind`. The value is either a bare string or a mapping:

```python
INPUTS = {
    "schedule": "gtfs_schedule_zip",                                          # required
    "stop_overrides": {"content_kind": "csv_table", "optional": True},       # optional
}
```

A bare string declares a **required** input — the run does not start without it, so transforms read it unconditionally. A mapping with `optional: True` declares an **optional** input — the run proceeds without it, and its name is then absent from `ctx.inputs`, so transforms that read it must check first. Optional is the exception; the bare string is the common form. See [Writing Transforms: Declare the pipeline's inputs](writing-transforms.md#declare-the-pipelines-inputs) for the details and [CLI: Inputs](cli.md#inputs) for how local runs treat a missing input.

When the pipeline runs on the Continuous GTFS platform, each declared input is a **slot** an operator connects to a feed source. Adding an input to the manifest is how a new slot comes into existence, with no platform change: the slot set is whatever the pipeline declares. A required slot left unconnected, or connected to a source of a different `content_kind`, is a configuration error and the pipeline does not run until it is fixed; an optional slot may stay empty. How slots are bound is platform documentation, not part of this SDK.

## Pipeline types

### Schedule

Processes GTFS schedule feeds (zip of CSV files):

```
parse inputs → transform (DAG) → validate output → package
```

Transforms operate on Polars DataFrames via `ctx.output["filename.txt"]`. The framework does not seed `ctx.output`: an init step does, and for a schedule pipeline that is one line, `init = InitScheduleOutput("schedule")`, which copies the named `gtfs_schedule_zip` input into the output and always runs first. Without it the output is empty and validation fails with "missing required file" errors. The [minimal example](https://github.com/ContinuousGTFS/continuous-gtfs/tree/develop/examples/minimal-schedule) shows the whole shape.

### Realtime

Processes GTFS-RT protobuf feeds:

```
parse inputs → transform (DAG) → encode <name>.pb + <name>.json per output
```

`ctx.output` starts empty here too. An `@step(before="*")` init step chooses which inputs become outputs and under what names:

```python
from continuous_gtfs import step

@step(before="*")
def init_outputs(ctx):
    ctx.output["vehicle_positions"] = ctx.inputs["vehicle_positions"]
    ctx.output["trip_updates"] = ctx.inputs["trip_updates"]
```

Transforms mutate the per-feed `FeedMessage`s in place. The framework emits one `.pb` + `.json` pair per entry in `ctx.output` — there is no combined feed.

## Error modes

The executor supports two modes:

- **Fail-fast** (default for realtime): Stop on first step failure. A partial RT result is worse than no result.
- **Continue** (default for schedule): Log the error, skip the failed step, continue remaining steps. Collects all errors for review.
