# Builtins

Builtins are parameterized steps for the common operations: you instantiate one with configuration instead of writing a function. Every builtin, with its parameters, example, and the findings it can emit, is catalogued in the reference, rendered from the code:

- **[Schedule builtins](../reference/builtins-schedule.md)** — init, row operations, semantic removals, ordering, identifiers, feed metadata.
- **[Realtime builtins](../reference/builtins-realtime.md)** — filtering, merging, identifiers, schedule-aware transforms, feed header.

This page covers what no single builtin owns: how schedule builtins select rows, how a missing column is reported, how realtime outputs are keyed, and the parameters every step shares.

## Schedule builtins

Schedule builtins operate on Polars DataFrames in `ctx.output["filename.txt"]`, seeded from a `gtfs_schedule_zip` input by [`InitScheduleOutput`](../reference/builtins-schedule.md#init). Each step is self-contained: it performs all its modifications in its own run, with no cross-step side effects.

### Selecting rows

The row operations (`RemoveRows`, `UpdateFields`, `ClearField`) and the semantic removals select rows with conditions. A `MatchCondition` is one test on one field, an exact `value` or a `regex`:

```python
from continuous_gtfs.builtins.schedule import MatchCondition

MatchCondition("route_id", value="R10")
MatchCondition("service_id", regex=r"^SHUTTLE")
```

Conditions combine as **groups**:

- A **flat list** is one group: every condition must match (all-of).
- A **list of lists** is several groups: every condition within a group must match, and a row is selected if it matches *any* group (any-of).

```python
# all-of: calendars with no weekday service
[
    MatchCondition("monday", value="0"),
    MatchCondition("tuesday", value="0"),
]

# any-of: three stops in one step instead of three steps
[
    [MatchCondition("stop_id", value="S13")],
    [MatchCondition("stop_id", value="S13A")],
    [MatchCondition("stop_id", value="S13B")],
]

# (A and B) or C
[
    [MatchCondition("route_id", value="BLUE"), MatchCondition("shape_id", value="BLUE_EAST")],
    [MatchCondition("route_id", value="GREEN")],
]
```

A single condition means the same thing flat or as a one-element group. Don't mix the two shapes in one list, and don't pass an empty group; both raise `ValueError` when the step is constructed. `exclude` takes the same shape and protects any row matching any of its groups; include and exclude are symmetric.

### Missing columns

If any condition names a column the file doesn't have, the step leaves that file untouched and emits a `column_missing` warning finding, one per missing column with the file and column in its context, so the misconfiguration surfaces in the run's issues instead of passing silently. A file absent from the output, or an empty conditions list, is a plain no-op with no finding.

### Rows versus records

`RemoveRows` edits one file and knows nothing about GTFS: remove a station's row from `stops.txt` and its platforms still say `parent_station=S15`, and `stop_times.txt` still calls at them. The [semantic removals](../reference/builtins-schedule.md#semantic-removals) (`RemoveRoutes`, `RemoveTrips`, `RemoveStops`, `RemoveServices`) remove a *record* with everything the GTFS model says belongs to it, tidy what the removal left empty, report every file they touched, and write every removed id into `ctx.id_mappings` so a custom step downstream can read what went away (see [ID Mappings](id-mappings.md)). Use `RemoveRows` for a row that is not a record: a stop_times row, a shape point, a fare rule.

## Realtime builtins

Realtime builtins operate on protobuf `FeedMessage` objects in `ctx.output`, keyed by feed name (`ctx.output["vehicle_positions"]`, `ctx.output["trip_updates"]`), and mutate them in place. A realtime pipeline produces one output per feed type, with no combined feed; its init step (typically `@step(before="*")`) seeds `ctx.output` from the inputs declared in the [INPUTS manifest](pipeline-structure.md).

Each entity type within a `FeedMessage` (`TripUpdate`, `VehiclePosition`, `Alert`) is inspected via protobuf's `HasField()`, so a single builtin handles any mix of entity types and leaves the types it does not know alone.

The [schedule-aware builtins](../reference/builtins-realtime.md#schedule-aware) read the schedule through a `gtfs_schedule_zip` input declared in the manifest, and resolve time in the agency's local timezone from `agency.txt` against `ctx.now`, never the wall clock.

## Parameters every step shares

Every builtin, and every `@step` function, accepts these keyword arguments. The catalog entries do not repeat them.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `after` | `list[Step]` or `"*"` | `[]` | Steps that must run before this one; `"*"` for every other step |
| `before` | `list[Step]` or `"*"` | `[]` | Steps that must run after this one; `"*"` for every other step |
| `priority` | `int` | `100` | Tiebreak for steps at the same DAG level (lower runs earlier) |
| `enabled` | `bool` | `True` | `False` skips the step but leaves it in the DAG |
| `tags` | `list[str]` | `[]` | Free labels for grouping |
| `data_owner` | `str` | `None` | Email of the responsible staff member |
| `description` | `str` | auto-generated | Human-readable label shown in CLI output and on the platform (on the builtins that take it) |

The full signature is on [`Step`](../reference/step.md) in the reference.
