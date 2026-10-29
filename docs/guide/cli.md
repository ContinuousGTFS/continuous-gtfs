# CLI Reference

The `continuous-gtfs` CLI runs pipelines locally for development and testing.

This guide explains what each command is for, with worked examples. The exhaustive option list, rendered from the command's own parser, is the [CLI reference](../reference/cli.md).

## `continuous-gtfs dag`

Show the resolved DAG for a pipeline folder.

```bash
continuous-gtfs dag <pipeline_dir>
continuous-gtfs dag <pipeline_dir> --json
continuous-gtfs dag <pipeline_dir> --mermaid
```

| Option | Description |
|--------|-------------|
| `--json` | Output ReactFlow-compatible JSON (nodes + edges) for visualization |
| `--mermaid` | Output a Mermaid flowchart — renders natively in GitHub and MkDocs |

**Example (text):**

```
$ continuous-gtfs dag my-agency/
DAG: 12 steps
  1. clear_blue_short_name [builtin] ['trips.txt']
  2. clear_green_short_name [builtin] ['trips.txt']
  3. remove_inactive_calendars [builtin] ['calendar.txt']
  4. remove_shuttle_calendar [builtin] ['calendar.txt'] (after: remove_inactive_calendars)
  ...
```

**Render as SVG with the Mermaid CLI:**

```bash
continuous-gtfs dag --mermaid my-agency/ \
  | npx --yes @mermaid-js/mermaid-cli -i - -o dag.svg
```

## `continuous-gtfs schedule`

Run the schedule pipeline, with its input files supplied via `--input` flags.

```bash
continuous-gtfs schedule <pipeline_dir> --input NAME[:KIND]=PATH [options]
```

| Option | Description |
|--------|-------------|
| `--input NAME[:KIND]=PATH` | Supply a named input to the pipeline. `NAME` must be declared in the pipeline's `INPUTS` manifest; `content_kind` resolves from the manifest by default, or is overridden by an optional `:KIND` suffix. Repeatable. Inputs the manifest marks optional may be omitted; omitting a required one fails before the run starts (a warning when `--select` narrows the run). See [Inputs](#inputs). |
| `-o, --output FILE` | Write output GTFS zip to file |
| `-v, --verbose` | Stream per-step progress as the pipeline runs |
| `--env NAME` | Environment name (default: `production`) |
| `--diff-against BASELINE` | After running, diff the output zip against this baseline using gtfs-digester |
| `--diff-detail` | Show per-row content changes in the diff output (requires `--diff-against`) |
| `--diff-limit N` | Max rows shown per category per file in diff detail. Default `50`. `0` = unlimited. |
| `--json-events PATH` | Write structured run events (stages, steps, validation, diff) to a JSON file |
| `--select STEP` | Run only the named step plus its transitive dependencies. Useful for isolating the diff impact of a single step while iterating. Relaxes the required-input check to a warning (see [Inputs](#inputs)). |

**Example:**

```
$ continuous-gtfs schedule my-agency/ --input schedule=data/schedule.zip -o output.zip

Schedule pipeline: 592.0ms
  Input:  23 files, 148,406 rows
  Output: 23 files, 148,385 rows
  Delta:  -21 rows

  Ingest: 92.6ms [23 files, 148,406 rows]
    Files: agency.txt, calendar.txt, ...
  Validate input: 22.1ms [pass]
  Transform: 33.8ms [12 steps]
      1. remove_shuttle_calendar [RemoveRows] calendar.txt: 3.1ms [ok] (-5 rows) — Remove shuttle service IDs
      2. rename_central_station [UpdateFields] stops.txt: 1.3ms [ok] — Rename Main Street to Central
      ...
  Validate output: 10.0ms [pass]
  Package: 433.3ms [1273 KB]

  Written to output.zip
```

Each transform step shows its ordinal, name, builtin class (or `@step`), target files, duration, row delta, status, and description.

### Pipeline stages

| Stage | Description |
|-------|-------------|
| **ingest** | Extract zip to Polars DataFrames (all string columns) |
| **validate_input** | Check required files, required fields, referential integrity |
| **transform** | Execute the step DAG |
| **validate_output** | Re-validate after transforms |
| **package** | Re-zip DataFrames to GTFS zip |

### Verbose mode

With `-v`, step events stream live as each one executes (useful for long pipelines to see progress):

```
    ▸ remove_shuttle_calendar [RemoveRows] calendar.txt — Remove shuttle service IDs
      1.7ms [ok] (-5 rows)
    ▸ rename_central_station [UpdateFields] stops.txt — Rename Main Street to Central
      1.5ms [ok]
```

### Exit codes

| Code | Meaning |
|------|---------|
| `0` | All steps succeeded, diff (if any) was identical |
| `1` | One or more steps errored, OR diff found changes |
| `2` | CLI argument or input-loading error |

On step failure the CLI prints a separated banner listing failed steps and their error messages at the end of output (to stderr), so errors are impossible to miss.

### Inputs

The `--input NAME=PATH` flag supplies a declared input to the pipeline. Every `NAME` must appear in the pipeline's `INPUTS` manifest (`__init__.py`) — unknown names fail before the run starts. Use repeated `--input` flags for each named input the pipeline consumes:

```bash
continuous-gtfs schedule pipelines/schedule/ \
  --input schedule=data/schedule.zip \
  --input stop_overrides=data/stop_overrides.csv \
  -o output.zip
```

Each input's bytes are parsed by the framework's parser registry according to the content_kind the pipeline declared for that name — no extension sniffing. Inputs are exposed to transforms as already-parsed values via `ctx.inputs[NAME]`:

| content_kind | Value shape |
|--------------|-------------|
| `gtfs_schedule_zip` | `dict[str, polars.DataFrame]` keyed by GTFS filename |
| `csv_table` | `polars.DataFrame` (all string columns) |
| `gtfs_rt_protobuf` | `gtfs_realtime_pb2.FeedMessage` |
| `opaque_bytes` | raw `bytes` (escape hatch) |
| `file_tree` | `FileTree` — read-only directory-tree root over the archive |

**Required vs optional inputs.** An input declared with a bare `content_kind` string is required; one declared as `{"content_kind": ..., "optional": True}` may be omitted from the command line, in which case it is simply absent from `ctx.inputs` (see [Writing Transforms: Declare the pipeline's inputs](writing-transforms.md#declare-the-pipelines-inputs)). What happens when a **required** input is omitted depends on the run's scope:

- **Full-DAG run** — fails before the run starts, naming the missing input. A run on the platform refuses the same omission.
- **Narrowed with `--select`** — a warning, and the run proceeds. Steps don't declare which inputs they read, so the selected steps may not need the input at all; the CLI leaves it to the steps to fail if one does.

A `file_tree` input's `PATH` may also be a **directory**: the CLI packs it with the platform's canonical packer (the same deterministic encoding the fetcher produces for a git tree path), so fixtures stay loose files on disk. A `.zip` path passes through as the archive itself. No other kind accepts a directory.

The optional `:KIND` suffix (`--input NAME:KIND=PATH`) overrides the manifest's declared kind for that flag — a dev escape hatch for feeding a declared input something other than its declared shape (e.g. a parser-behavior fixture). The CLI logs a warning so the override is visible.

Missing files, unknown names, malformed args, or a missing required input on a full run exit with code `2`. See [Writing Transforms: Reference-data inputs](writing-transforms.md#reference-data-inputs) for how to access inputs in transforms.

### Diff integration

With `--diff-against BASELINE`, after the pipeline completes the output zip is compared to the baseline using `gtfs-digester` canonical diffing (`gtfs-digester` installs with the package).

Default diff output is a per-file summary. Add `--diff-detail` (optionally with `--diff-limit N`) to expand modified files to per-row content changes with colorized field diffs — the same format produced by the standalone `diff --detail` subcommand:

```bash
continuous-gtfs schedule pipelines/schedule/ \
  --input schedule=feed.zip \
  -o out.zip \
  --diff-against baseline.zip \
  --diff-detail --diff-limit 20
```

Exit code is non-zero if diffs are found — useful for CI that should fail when pipeline output unexpectedly changes.

## `continuous-gtfs diff`

Compare two GTFS zip archives using `gtfs-digester` canonical diffing.

```bash
continuous-gtfs diff <baseline.zip> <candidate.zip> [options]
```

| Option | Description |
|--------|-------------|
| `--json` | Output structured JSON instead of text (mutually exclusive with `--detail`) |
| `--detail` | Expand modified files to show per-row content changes with colorized field diffs (mutually exclusive with `--json`) |
| `--limit N` | Max rows shown per category (added / removed / modified) per file in detail mode. Default `50`. `0` = unlimited. |

**Default (summary) output:**

```
$ continuous-gtfs diff baseline.zip candidate.zip
Diff: baseline.zip vs candidate.zip
  Identical: no
  Modified files (4):
    ~ calendar.txt: +0 added, -8 removed, ~0 modified
    ~ routes.txt: +0 added, -0 removed, ~2 modified
    ~ stops.txt: +0 added, -2 removed, ~5 modified
```

**Detail output** (`--detail`) expands modified files to show exactly what changed — full rows for added/removed, field-level old → new diffs for modified rows, skipping unchanged fields:

```
$ continuous-gtfs diff baseline.zip candidate.zip --detail
Diff: baseline.zip vs candidate.zip
  Identical: no

~ routes.txt  (+0 / -0 / ~2)
  primary key: route_id

  Modified (2):
    ~ route_id=R10
        route_long_name: "Northgate - Airport" → "Northgate - Harbor Point"
    ~ route_id=BLUE
        route_long_name: "Northgate - Eastside" → "Downtown - Eastside"

~ stops.txt  (+0 / -2 / ~5)
  primary key: stop_id

  Removed (2):
    - stop_id=S01  stop_name="Lakeview"  stop_desc="Lakeview Station..."  ...
    - stop_id=S07  stop_name="Riverside"  stop_desc="Riverside Station..."  ...

  Modified (5):
    ~ stop_id=C20
        stop_desc: "Central to Airport" → "Central to Harbor Point"
    ...
```

ANSI colorization auto-enables when stdout is a TTY and auto-disables when piped or redirected. No `--color` flag needed.

Use `--limit N` to cap per-category output per file. Truncated output ends with `... and M more (use --limit 0 to see all)`.

Exit code `0` if identical, `1` if different, `2` if arguments are invalid or `gtfs-digester` is not installed.

## `continuous-gtfs realtime`

Run the realtime pipeline, with its input feeds supplied via `--input` flags. RT pipelines emit one artifact pair (`.pb` + `.json`) per declared output feed — there is no combined feed.

```bash
continuous-gtfs realtime <pipeline_dir> --input NAME[:KIND]=PATH [options]
```

| Option | Description |
|--------|-------------|
| `--input NAME[:KIND]=PATH` | Supply a named RT input. Same resolution rules as `schedule --input`. Repeatable. |
| `-o, --output-dir DIR` | Output directory; each `ctx.output[name]` is written as `<name>.pb` and `<name>.json` |
| `--env NAME` | Environment name (default: `production`) |

**Example:**

```
$ continuous-gtfs realtime my-agency-rt/ \
    --input vehicle_positions=data/rt/vehicle_positions.pb \
    --input trip_updates=data/rt/trip_updates.pb \
    -o out/
RT pipeline: 5.5ms
  Input entities:  32
  Output entities: 32
    vehicle_positions: 16 entities  (4705B pb / 23620B json)
    trip_updates: 16 entities  (6502B pb / 42712B json)
  Written out/vehicle_positions.pb + out/vehicle_positions.json
  Written out/trip_updates.pb + out/trip_updates.json
```

### Pipeline stages

| Stage | Description |
|-------|-------------|
| **parse** | Framework parses each input's bytes per its declared `content_kind` |
| **seed** | `ctx.output` is mirrored from each `FeedMessage` input (pipelines can override in an init step) |
| **transform** | Execute the step DAG; transforms mutate the per-feed `FeedMessage`s in `ctx.output` |
| **encode** | Serialize each `ctx.output[name]` to both `<name>.pb` and `<name>.json` |

## `continuous-gtfs rt-compare`

Compare two GTFS-RT protobuf files semantically rather than byte for byte: entities are matched by identity, timestamps and positions compare within tolerances, and the report says at which level the feeds stop agreeing. Use it to check that a realtime pipeline change did not alter output it was not meant to touch.

```bash
continuous-gtfs rt-compare baseline/trip_updates.pb out/trip_updates.pb
continuous-gtfs rt-compare baseline/vehicle_positions.pb out/vehicle_positions.pb --json
```

The tolerances (`--timestamp-tolerance`, `--entity-timestamp-tolerance`, `--position-decimal-places`, `--stale-threshold`) are listed with their defaults in the [CLI reference](../reference/cli.md#continuous-gtfs-rt-compare).

## `continuous-gtfs semantics` and `continuous-gtfs pair-trips`

Derive the schedule-semantics tables for a feed (service dates, stop patterns, time profiles), and pair the trips of two feed versions into a `trip_pairs` table that says which trip in one version corresponds to which in the other. Both read local GTFS zips and write Parquet files to `-o DIR`; `--json` prints a row-count summary instead.

```bash
continuous-gtfs semantics feed.zip -o semantics/
continuous-gtfs pair-trips current.zip candidate.zip -o pairs/
```

They answer "what did this change do to service?" questions that a row-level `diff` cannot, such as a trip that kept its stops but changed its id. Every option is in the [CLI reference](../reference/cli.md#continuous-gtfs-semantics).
