# ID Mappings

GTFS files are heavily cross-referenced — `trips.route_id` references `routes.route_id`, `stop_times.trip_id` references `trips.trip_id`, and so on. When a transform removes or renames an ID in one file, dependent files need to hear about it.

The `PipelineContext` provides an `id_mappings` dict for this: a communication channel between steps. One step records what it removed or renamed; a later step reads it and reacts. **The framework never acts on a mapping by itself.**

## When to use this

To *remove* a route, trip, stop, or service with everything that belongs to it, don't use mappings — use the [semantic removal builtins](../reference/builtins-schedule.md#semantic-removals) (`RemoveRoutes`, `RemoveTrips`, `RemoveStops`, `RemoveServices`). They know the GTFS record model, cascade for you, and **write** every id they remove into `id_mappings`, so a custom step downstream can still read what went away:

```python
@step(files=["stop_times.txt"], after=[remove_s15])
def note_removed_stops(ctx):
    gone = ctx.get_id_mappings("stops.txt", "stop_id")   # {"S15": None, "S15A": None, ...}
```

Write mappings yourself when your own `@step`:

- renames an ID and other files need the new value
- consolidates multiple IDs into one
- removes records in a way the builtins don't cover and another step must react

If your transform only operates within a single file, you don't need this — just modify the DataFrame directly.

## Writing mappings

The step that removes or renames the ID records the change:

```python
@step(files=["routes.txt"])
def consolidate_routes(ctx):
    df = ctx.output["routes.txt"]

    # Merge route OLD into route NEW
    ctx.add_id_mapping("routes.txt", "route_id", "OLD", "NEW")
    ctx.output["routes.txt"] = df.filter(pl.col("route_id") != "OLD")
```

For removals, use `None` as the new ID:

```python
ctx.add_id_mapping("routes.txt", "route_id", "DEPRECATED", None)
```

## Reading mappings

A dependent step reads the mappings and applies cascading changes:

```python
@step(files=["trips.txt"], after=[consolidate_routes])
def cascade_to_trips(ctx):
    mappings = ctx.get_id_mappings("routes.txt", "route_id")
    if not mappings:
        return

    df = ctx.output["trips.txt"]
    for old_id, new_id in mappings.items():
        if new_id is None:
            # Cascade delete
            df = df.filter(pl.col("route_id") != old_id)
        else:
            # Cascade rename
            df = df.with_columns(
                pl.when(pl.col("route_id") == old_id)
                .then(pl.lit(new_id))
                .otherwise(pl.col("route_id"))
                .alias("route_id")
            )
    ctx.output["trips.txt"] = df
```

## API

### `ctx.add_id_mapping(file, field, old_id, new_id)`

| Parameter | Type | Description |
|-----------|------|-------------|
| `file` | `str` | GTFS filename (e.g. `"routes.txt"`) |
| `field` | `str` | Field name (e.g. `"route_id"`) |
| `old_id` | `str` | The original ID value |
| `new_id` | `str | None` | New value, or `None` if removed |

Mappings are additive — multiple steps can write to the same file/field combination.

### `ctx.get_id_mappings(file, field) -> dict[str, str | None]`

Returns all mappings for the given file and field. Empty dict if none exist.

## Key properties

- **Loose coupling:** The writing step doesn't need to know which steps will read the mappings.
- **DAG enforcement:** Reading steps declare `after=[writing_step]` — the DAG guarantees ordering.
- **Additive:** Multiple steps can contribute mappings to the same file/field.
- **Channel, not trigger:** Nothing in the framework acts on a mapping. The semantic removal builtins write mappings for the records they remove; a custom step that wants to react reads them. Cascading itself lives in those builtins, not in the dict.
