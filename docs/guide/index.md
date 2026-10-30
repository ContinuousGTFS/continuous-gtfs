# Getting Started

Continuous GTFS provides a Python framework for transforming GTFS schedule and realtime feeds. You define transforms as Python code in a folder, and the framework discovers them, resolves their dependencies, and executes them as a pipeline.

## Install

```bash
pip install continuous-gtfs
# or, in a uv-managed project
uv add continuous-gtfs
```

Requires Python 3.12 or newer. [Local Development](local-development.md) sets up a pipeline repository from scratch.

## Quick start

The [minimal example](https://github.com/ContinuousGTFS/continuous-gtfs/tree/develop/examples/minimal-schedule) in the repository is this quick start, finished and runnable.

### 1. Create a pipeline folder

```
my-pipeline/
  __init__.py     # FEED_TYPE and the INPUTS manifest (required)
  feed_info.py
```

Each `.py` file defines one or more **Steps**. The framework scans the folder and finds them. The `__init__.py` declares what kind of feed the pipeline processes and its named inputs.

### 2. Declare inputs

```python
# my-pipeline/__init__.py
FEED_TYPE = "schedule"   # or "realtime"

INPUTS = {
    "schedule": "gtfs_schedule_zip",
}
```

Every input the pipeline consumes is named here with its `content_kind`. The framework checks every run against this manifest and uses it to resolve `--input` flags in local CLI runs.

### 3. Seed the output and define a transform

```python
# my-pipeline/feed_info.py
from continuous_gtfs.builtins.schedule import InitScheduleOutput, UpdateFeedInfo

# ctx.output starts empty. This step copies the "schedule" input into it,
# and always runs before every other step.
init = InitScheduleOutput("schedule")

update_feed = UpdateFeedInfo(
    publisher_name="My Transit Agency",
    publisher_url="https://example.com",
)
```

`init` and `update_feed` are Step instances, found by the scanner under their variable names. Every schedule pipeline needs the `init` line. The framework never guesses which input to edit, and a pipeline without it produces an empty output and fails validation with "missing required file" errors.

### 4. Run it

```bash
# See what the pipeline contains
continuous-gtfs dag my-pipeline/

# Run against a GTFS zip; the --input name must match the INPUTS manifest
continuous-gtfs schedule my-pipeline/ --input schedule=input.zip -o output.zip
```

### 5. Inspect the output

```
$ continuous-gtfs dag my-pipeline/
DAG: 2 steps
  1. init [builtin] []
  2. update_feed [builtin] ['feed_info.txt']

$ continuous-gtfs schedule my-pipeline/ --input schedule=input.zip -o output.zip

Schedule pipeline: 11.8ms
  Output: 7 files, 11 rows

  Transform: 3.2ms [2 steps]
      1. init [InitScheduleOutput]: 0.0ms [ok] — Seed ctx.output from ctx.inputs['schedule']
      2. update_feed [UpdateFeedInfo] feed_info.txt: 2.9ms [ok] — Update feed publisher info
  Validate output: 0.9ms [pass]
  Package: 7.6ms [1 KB]

  Written to output.zip
```

Each transform step shows its duration, its status, its description and, when it changed the number of rows, the row delta. Add `-v` to stream steps as they run, or `--diff-against baseline.zip` to diff the output in the same command.

A realtime pipeline has the same shape with `FEED_TYPE = "realtime"`, `gtfs_rt_protobuf` inputs, and an `@step(before="*")` init step that puts the feeds into `ctx.output` (see [Concepts: Realtime](concepts.md#realtime)). It runs with `continuous-gtfs realtime`, which writes a `.pb` and a `.json` file per output feed.

## What to read next

- **[Concepts](concepts.md)** — Steps, the DAG, PipelineContext, builtins vs custom code
- **[Writing Transforms](writing-transforms.md)** — full walkthrough of building a pipeline
- **[Builtins](builtins.md)** — how builtins select rows and share parameters; the full catalog is in the [Reference](../reference/builtins-schedule.md)
- **[Local Development](local-development.md)** — the edit, run, compare loop on your own machine
- **[Testing Transforms](testing-transforms.md)** — unit-test one step against a tiny fixture
- **[CLI Guide](cli.md)** — what each command is for, with examples; the exhaustive option list is the [CLI reference](../reference/cli.md)
