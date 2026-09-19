# Continuous GTFS

Define, test, and run GTFS data pipelines as Python steps.

`continuous-gtfs` is a framework for transforming [GTFS](https://gtfs.org) schedule feeds and GTFS-Realtime protobuf feeds with small, declarative, dependency-ordered steps — plus a CLI to run pipelines locally, diff feeds, and inspect the step DAG.

```python
# pipelines/my_agency/transforms.py
from continuous_gtfs.builtins.schedule import (
    InitScheduleOutput,
    MatchCondition,
    RemoveRows,
)

init = InitScheduleOutput()

remove_inactive_calendars = RemoveRows(
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
```

```console
continuous-gtfs schedule pipelines/my_agency --input schedule=./gtfs.zip -o ./out
```

Steps declare what they touch and what they run after; the framework scans the pipeline folder, resolves the DAG, and executes it. Custom logic is a decorated function away:

```python
from continuous_gtfs import step

@step(files=["stops.txt"])
def drop_test_stops(ctx):
    stops = ctx.output["stops.txt"]
    ctx.output["stops.txt"] = stops.filter(~stops["stop_id"].str.starts_with("TEST_"))
```

## Install

```console
pip install continuous-gtfs
```

> The first PyPI release (v0.1.0) is landing shortly. Until it does, install
> straight from this repository:
>
> ```console
> pip install git+https://github.com/continuousgtfs/continuous-gtfs
> ```

Or work from a clone (uses [uv](https://docs.astral.sh/uv/)):

```console
git clone https://github.com/continuousgtfs/continuous-gtfs
cd continuous-gtfs
uv sync
uv run continuous-gtfs --help
```

Requires Python 3.12+.

## What's in the box

- **Step framework** — `@step` functions and `Step` classes with `files=`, `after=`, `before=` ordering; `scan_pipeline()` / `resolve_dag()` discover and order a pipeline folder's steps.
- **Schedule builtins** — `RemoveRows`, `UpdateFields`, `ClearField`, `SortRows`, `TransformTripId`, `UpdateFeedInfo`, semantic removals (`RemoveRoutes` / `RemoveTrips` / `RemoveStops` / `RemoveServices` with reference-following), and more, all operating on [Polars](https://pola.rs) DataFrames.
- **Realtime builtins** — `FilterStopsByID`, `RenameVehicles`, `TransformTripId`, `CombineFeeds`, `ExpireCancelledTrips`, `InsertMissingCancellations`, `ConvertScheduledToNew`, and more, operating on GTFS-RT `FeedMessage` protobufs.
- **Testing kit** — `continuous_gtfs.testing` gives you `gtfs_df`, `schedule_context`, `realtime_context`, feed builders, and `assert_unchanged` for unit-testing a single step against a tiny in-memory fixture. No network, no real feed.
- **Schedule semantics** — derivation of service-date / stop-pattern / time-profile tables and semantic trip pairing between two feed versions (`semantics`, `pair-trips` commands).
- **CLI** — see below.

## CLI

The package installs a `continuous-gtfs` command:

| Command | What it does |
| --- | --- |
| `continuous-gtfs schedule <pipeline-dir>` | Run a schedule pipeline on a GTFS zip, write the transformed zip |
| `continuous-gtfs realtime <pipeline-dir>` | Run a realtime pipeline on GTFS-RT protobuf file(s) |
| `continuous-gtfs dag <pipeline-dir>` | Show a pipeline's resolved step DAG |
| `continuous-gtfs diff <a.zip> <b.zip>` | Diff two GTFS zips table-by-table |
| `continuous-gtfs rt-compare <a.pb> <b.pb>` | Semantically compare two GTFS-RT protobuf feeds |
| `continuous-gtfs semantics <feed.zip>` | Derive schedule-semantics tables for a feed |
| `continuous-gtfs pair-trips <target> <candidate>` | Pair two feeds' trips into a `trip_pairs` table |

Each command supports `--help` for its full options.

## Defining a pipeline

A pipeline is a directory:

```
pipelines/
└── my_agency/
    ├── __init__.py      # FEED_TYPE = "schedule", INPUTS = {"schedule": "gtfs_schedule_zip"}
    └── transforms.py    # step definitions (any module name works; all are scanned)
```

`FEED_TYPE` is `"schedule"` or `"realtime"`. `INPUTS` declares the named input slots the pipeline consumes and their content kinds. Every module in the folder is scanned for steps; sibling modules next to the pipeline folder are importable for sharing code between pipelines (see [`examples/cross-pipeline-trip-id`](examples/cross-pipeline-trip-id)).

Start from [`examples/minimal-schedule`](examples/minimal-schedule) for the smallest working pipeline.

## Testing your pipeline

```python
from continuous_gtfs.testing import gtfs_df, schedule_context

from pipelines.my_agency.transforms import remove_inactive_calendars

def test_remove_inactive_calendars():
    ctx = schedule_context(**{
        "calendar.txt": gtfs_df("""
            service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday
            DEAD,0,0,0,0,0,0,0
            LIVE,1,1,1,1,1,0,0
        """)
    })
    remove_inactive_calendars.apply(ctx)
    assert ctx.output["calendar.txt"]["service_id"].to_list() == ["LIVE"]
```

## Relationship to the Continuous GTFS platform

This framework is the open-source core of [Continuous GTFS](https://continuousgtfs.com), a hosted platform that runs these same pipelines continuously against live agency feeds — with orchestration, versioned feed history, review workflows, validation, and CDN publishing on top. Pipelines you define and test with this package run unchanged on the platform, and the framework as published here powers a production deployment serving a major US transit agency today.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Run the test suite with `uv run pytest`.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
Copyright 2026 Jarvus Innovations.
