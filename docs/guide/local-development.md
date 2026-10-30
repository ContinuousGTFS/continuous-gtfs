# Local Development

How to set up a repository for your agency's pipelines and iterate on a change with real data, entirely on your machine. Nothing here needs network access beyond installing the package, and nothing needs the platform.

## Set up a pipeline repository

Pipelines live in your own repository, next to their tests. With [uv](https://docs.astral.sh/uv/):

```bash
uv init --bare my-agency-pipelines
cd my-agency-pipelines
uv add continuous-gtfs
uv add --dev pytest
```

A layout that keeps each pipeline a folder and shared code beside them:

```
my-agency-pipelines/
  pyproject.toml
  pipelines/
    schedule/
      __init__.py        # FEED_TYPE = "schedule", INPUTS = {...}
      calendar.py
      stops.py
    realtime/
      __init__.py        # FEED_TYPE = "realtime", INPUTS = {...}
      trip_ids.py
    shared.py            # importable from both pipelines
  tests/
    test_calendar.py
  data/                  # sample feeds; keep out of git
```

Modules next to the pipeline folders (`shared.py` above) are importable from every pipeline, which is how a schedule and a realtime pipeline share an id transformation. The [cross-pipeline example](https://github.com/ContinuousGTFS/continuous-gtfs/tree/develop/examples/cross-pipeline-trip-id) shows the pattern, and the [minimal example](https://github.com/ContinuousGTFS/continuous-gtfs/tree/develop/examples/minimal-schedule) is the smallest working pipeline.

Add `data/` to `.gitignore`. Sample feeds are large, change often, and may carry data you should not publish.

## Get sample data

Each input in the pipeline's `INPUTS` manifest needs a local file for a run:

- a schedule input (`gtfs_schedule_zip`): the GTFS zip your source publishes, or the one the pipeline consumed most recently;
- a realtime input (`gtfs_rt_protobuf`): a captured `.pb` from each feed, fetched once with `curl -o data/rt/trip_updates.pb <feed url>`;
- reference data (`csv_table`, `file_tree`): the file or folder itself. A `file_tree` input may be a plain directory; the CLI packs it the way the platform does.

Keep a copy of today's **published output** too. Comparing against it is how you see what your change does.

## The edit, run, compare loop

See the steps and their order first:

```bash
uv run continuous-gtfs dag pipelines/schedule/
```

Run the whole pipeline and compare its output with the published feed:

```bash
uv run continuous-gtfs schedule pipelines/schedule/ \
  --input schedule=data/schedule.zip \
  -o out/schedule.zip \
  --diff-against data/published.zip --diff-detail
```

The run prints one line per step with its status and row delta, then the diff. Exit code `1` means a step failed or the output differs from the baseline. Output validation errors (the `Validate output` line) are printed but leave the exit code at `0`, so a CI guard built on this command also has to check them, for example in the `--json-events` file: `jq '.stages[] | select(.name == "validate_output") | .metadata.errors | length' run.json` must print `0`.

While working on one step, run only that step and what it depends on:

```bash
uv run continuous-gtfs schedule pipelines/schedule/ \
  --input schedule=data/schedule.zip \
  --select remove_shuttle_calendar \
  --diff-against data/schedule.zip --diff-detail
```

Diffing against the *input* shows exactly what the selected steps changed. Add `-v` to watch steps as they run on a large feed, and `--json-events run.json` to keep a structured record of the run.

For a realtime pipeline the loop is the same with `realtime` and one output pair per feed:

```bash
uv run continuous-gtfs realtime pipelines/realtime/ \
  --input trip_updates=data/rt/trip_updates.pb \
  --input vehicle_positions=data/rt/vehicle_positions.pb \
  -o out/rt/
uv run continuous-gtfs rt-compare data/rt/trip_updates.pb out/rt/trip_updates.pb
```

Each output is written as both `.pb` and `.json`; the JSON is the quickest way to read what a step did to a single entity.

## Findings

A step or check that emits findings prints them after the step table, grouped by code. Locally they are a per-run report. Treat a new finding code in a run as something to explain before you push: either the change caused it, or it was already there and the change is a chance to fix it.

## Lock it in with tests

Every step worth writing is worth a unit test against a tiny fixture. Such a test is fast and hermetic, and it does not depend on whatever the sample data happens to contain. [Testing Transforms](testing-transforms.md) covers the pattern and the `continuous_gtfs.testing` helpers. Put the pipelines directory on pytest's path so tests can import steps by folder and file name:

```toml
# pyproject.toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["pipelines"]
```

```bash
uv run pytest
```

Run the tests, and a full pipeline run on a committed small fixture if you have one, in CI on every pull request.

## Before you push

- `continuous-gtfs dag` shows the step where you expect it in the order.
- The full run passes, and the diff against the published output contains the change you meant and nothing else.
- A test pins the change and asserts the rest of the table is untouched.
- Any new input is declared in `INPUTS`, with `optional: True` only if the pipeline produces correct output without it.
