# Testing Transforms

The `continuous_gtfs.testing` module is the supported way to unit-test the transform steps in your pipeline — fast, hermetic, no platform, no network, no real feed. A test builds a tiny in-memory `PipelineContext`, applies one step, and asserts on the result.

```python
from continuous_gtfs.testing import gtfs_df, schedule_context

def test_remove_inactive_calendars():
    calendar = gtfs_df(
        """
        service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday
        DEAD,0,0,0,0,0,0,0
        LIVE,1,0,0,0,0,0,0
        """
    )
    ctx = schedule_context(**{"calendar.txt": calendar})

    remove_inactive_calendars.apply(ctx)

    assert ctx.output["calendar.txt"]["service_id"].to_list() == ["LIVE"]
```

The pattern is always three steps:

1. Build a `PipelineContext` whose `.output` holds the GTFS table(s) or GTFS-RT feed(s) the step reads.
2. Call `step.apply(ctx)`.
3. Assert on the mutated `ctx.output`.

A step — whether a builtin instance like `RemoveRows` or a custom `@step` function — exposes `.apply(ctx)`, which mutates the context in place. That's the entire contract a test needs.

## The all-text typing guarantee

GTFS columns are text: a `route_type` of `3` and a `stop_id` of `S01` are both strings in a real feed, and `MatchCondition(value=…)` compares against string values. If a fixture lets Polars infer `0`/`1` service-day flags as integers, a condition like `MatchCondition("monday", value="0")` **silently never matches** and your test passes for the wrong reason.

`gtfs_df` removes that trap: it forces every column to `Utf8`, keeping fixtures faithful to how a real run parses the feed. It also `dedent`s the string, so you can indent the CSV block to match the surrounding code.

## Public API

| Symbol | Signature | Use |
|--------|-----------|-----|
| `gtfs_df` | `(csv: str) → DataFrame` | Parse an (indentable) CSV string into an all-text DataFrame. |
| `schedule_context` | `(**tables: DataFrame) → PipelineContext` | Context seeded with GTFS tables as `ctx.output` (keys are filenames, e.g. `**{"trips.txt": df}`). |
| `realtime_context` | `(**feeds: FeedMessage) → PipelineContext` | Context seeded with GTFS-RT feeds (keys are feed names, e.g. `trip_updates=…`). |
| `trip_updates_feed` | `(*, trip_id, stop_ids) → FeedMessage` | A minimal one-trip `trip_updates` feed. |
| `vehicle_positions_feed` | `(*, vehicle_ids, trip_ids=None, stop_ids=None) → FeedMessage` | A minimal `vehicle_positions` feed. |
| `assert_unchanged` | `(before, after, *, excluding) → None` | Assert that rows *not* matched by `excluding` are untouched. |

## Realtime transforms

Realtime steps read every GTFS-RT `FeedMessage` in `ctx.output` and mutate it in place. Build a feed, apply the step, assert:

```python
from continuous_gtfs.testing import realtime_context, trip_updates_feed

def test_filter_drops_blocklisted_stops():
    feed = trip_updates_feed(trip_id="t1", stop_ids=["S01-A", "GOOD", "S07-T2"])
    ctx = realtime_context(trip_updates=feed)

    filter_stops.apply(ctx)

    remaining = [
        stu.stop_id
        for stu in ctx.output["trip_updates"].entity[0].trip_update.stop_time_update
    ]
    assert remaining == ["GOOD"]
```

## Asserting the rest is untouched

The easy half of a transform test is "the right rows changed." The half that's easy to forget is "everything else stayed the same" — a too-broad `MatchCondition` or regex is exactly the kind of bug a unit test should catch. `assert_unchanged` makes that explicit:

```python
from continuous_gtfs.testing import assert_unchanged
import polars as pl

def test_only_expansion_stops_removed():
    ctx = schedule_context(**{"stops.txt": gtfs_df(...)})
    before = ctx.output["stops.txt"].clone()      # snapshot before applying

    remove_expansion_stops.apply(ctx)

    assert_unchanged(
        before,
        ctx.output["stops.txt"],
        excluding=pl.col("stop_id").is_in(["S01", "S07"]),
    )
```

It filters both frames to `~excluding`, sorts them (so row reordering within the unchanged set doesn't trip a false failure), and on failure reports the row-count change plus a sample of changed rows.

!!! note
    `assert_unchanged` requires `before` and `after` to share the same schema. For steps that add or drop columns, assert on the affected columns directly with Polars.

## Wiring it into your repo

Point pytest at your pipeline folders so the transforms import under the same top-level names the scanner loads them under:

```toml
# pyproject.toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["pipelines"]
```

Then a test imports the step instance and applies it:

```python
from schedule.cleanup import remove_inactive_calendars   # pipelines/schedule/cleanup.py
```

Run with `uv run pytest`, and gate it in CI on every change to your pipeline code.
