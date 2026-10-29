# Pipeline Structure

## The folder is the pipeline

A pipeline is a directory of `.py` files. The framework scans the folder, imports each file, and discovers all module-level `Step` instances. No config file or registry needed.

```
my-agency/
  calendar.py        # calendar cleanup rules
  routes.py          # route metadata updates
  stops.py           # station renames, stop removals
  trips.py           # trip field cleanup
```

### What gets scanned

- All `.py` files in the folder (non-recursive, sorted alphabetically)
- Files starting with `_` are skipped (`__init__.py`, `_helpers.py`, etc.)
- Attributes starting with `_` are skipped
- Only module-level objects that are `isinstance(obj, Step)` are discovered

### Names

The **variable name** becomes the step name. Choose clear, descriptive names — they appear in the DAG, in CLI output, and wherever the pipeline runs:

```python
# Good — the variable name IS the step identity
remove_inactive_calendars = RemoveRows(...)
update_blue_line_metadata = UpdateFields(...)

# Avoid — generic names make the DAG hard to read
step1 = RemoveRows(...)
x = UpdateFields(...)
```

## Cross-file imports

Files in the same pipeline folder can import from each other using relative imports:

```python
# stops.py
from .routes import update_green_line  # import for DAG dependency

@step(files=["stops.txt"], after=[update_green_line])
def rename_stations(ctx):
    ...
```

The scanner handles this automatically — it registers the folder as a Python package so relative imports work.

!!! warning
    Imported Step instances are deduplicated by object identity. If `stops.py` imports `update_green_line` from `routes.py`, it appears once in the DAG (not twice).

## `__init__.py`

Required. It declares the pipeline's `FEED_TYPE` (`"schedule"` or `"realtime"`) and its `INPUTS` manifest (see [Writing Transforms: Declare the pipeline's inputs](writing-transforms.md#declare-the-pipelines-inputs)), and it runs when the folder is registered as a package, so it can also hold shared imports. It is never scanned for steps.

## Helper files

Prefix helper files with `_` to exclude them from scanning:

```
my-agency/
  _constants.py      # shared constants, not scanned
  _utils.py           # helper functions, not scanned
  calendar.py         # scanned — defines steps
  routes.py           # scanned — defines steps
```

Or put helpers in a subdirectory — the scanner doesn't recurse:

```
my-agency/
  helpers/
    constants.py      # not scanned (in subdirectory)
  calendar.py         # scanned
```

## Organizing a large pipeline

There's no prescribed structure. Some options:

**By GTFS file:**

```
my-agency/
  calendar_cleanup.py
  route_metadata.py
  station_renames.py
  trip_cleanup.py
```

**By concern:**

```
my-agency/
  data_quality.py      # validation-oriented transforms
  branding.py          # route names, colors, URLs
  expansion.py         # pre-opening station adjustments
```

**By team:**

```
my-agency/
  rail_team.py
  bus_team.py
  operations.py
```

The folder structure also maps naturally to `CODEOWNERS` — different teams can own different transform files with PR review enforced by path.
