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

### 1. Create a pipeline folder

```
my-pipeline/
  __init__.py     # FEED_TYPE and the INPUTS manifest (required)
  feed_info.py
  stop_cleanup.py
```

Each `.py` file defines one or more **Steps** — the framework scans the folder and discovers them automatically. The `__init__.py` declares what kind of feed the pipeline processes and its named inputs.

### 2. Declare inputs

```python
# my-pipeline/__init__.py
FEED_TYPE = "schedule"   # or "realtime"

INPUTS = {
    "schedule": "gtfs_schedule_zip",
}
```

Every input the pipeline consumes is named here with its `content_kind`. The framework validates every run against this manifest and uses it to resolve `--input` flags in local CLI runs.

### 3. Define a transform

```python
# my-pipeline/feed_info.py
from continuous_gtfs.builtins.schedule import UpdateFeedInfo

update_feed = UpdateFeedInfo(
    publisher_name="My Transit Agency",
    publisher_url="https://example.com",
)
```

That's it. `update_feed` is a Step instance that the scanner will find by variable name.

### 4. Run it

```bash
# See what the pipeline contains
continuous-gtfs dag my-pipeline/

# Run against a GTFS zip — --input name must match the INPUTS manifest
continuous-gtfs schedule my-pipeline/ --input schedule=input.zip -o output.zip

# Run against realtime protobuf feeds — each declared RT input is its own output
continuous-gtfs realtime my-pipeline/ \
  --input vehicle_positions=vehicle_positions.pb \
  --input trip_updates=trip_updates.pb \
  -o out/
```

### 5. Inspect the output

```
Schedule pipeline: 592.0ms
  Input:  22 files, 178,485 rows
  Output: 22 files, 178,461 rows
  Delta:  -24 rows

  Ingest: 92.6ms [22 files, 178,485 rows]
  Validate input: 22.1ms [pass]
  Transform: 33.8ms [1 steps]
      1. update_feed [UpdateFeedInfo] feed_info.txt: 0.5ms [ok] — Update feed publisher info
  Validate output: 10.0ms [pass]
  Package: 433.3ms [1273 KB]

  Written to output.zip
```

Each transform step shows duration, row delta, status, and description. Add `-v` to stream steps as they run, or `--diff-against baseline.zip` to diff the output in one shot.

## What to read next

- **[Concepts](concepts.md)** — Steps, the DAG, PipelineContext, builtins vs custom code
- **[Writing Transforms](writing-transforms.md)** — full walkthrough of building a pipeline
- **[Builtins](builtins.md)** — how builtins select rows and share parameters; the full catalog is in the [Reference](../reference/builtins-schedule.md)
- **[Local Development](local-development.md)** — the edit, run, compare loop on your own machine
- **[Testing Transforms](testing-transforms.md)** — unit-test one step against a tiny fixture
- **[CLI Guide](cli.md)** — what each command is for, with examples; the exhaustive option list is the [CLI reference](../reference/cli.md)
