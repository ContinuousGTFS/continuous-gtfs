# Example — cross-pipeline trip-id rewrite

Demonstrates the cross-pipeline shared-module convention: a single `pipelines/shared.py` module owns the `(pattern, replacement)` pair, and both the schedule and realtime pipelines import it. This keeps the `trip_id` rewrite identical across the two outputs — essential when downstream consumers join the realtime feed against the transformed schedule.

```
pipelines/
├── shared.py            # source of truth: TRIP_ID_REWRITE = {...}
├── schedule/
│   ├── __init__.py      # FEED_TYPE = "schedule", INPUTS = {...}
│   └── transforms.py    # from shared import TRIP_ID_REWRITE
└── realtime/
    ├── __init__.py      # FEED_TYPE = "realtime", INPUTS = {...}
    ├── init_output.py
    └── transforms.py    # from shared import TRIP_ID_REWRITE
```

## Running locally

```bash
# Schedule — rewrites trip_id in trips.txt, stop_times.txt, transfers.txt, etc.
continuous-gtfs schedule examples/cross-pipeline-trip-id/pipelines/schedule \
  --input schedule=./path/to/gtfs.zip \
  -o /tmp/schedule-out

# Realtime — rewrites trip_id on entities in every output FeedMessage
continuous-gtfs realtime examples/cross-pipeline-trip-id/pipelines/realtime \
  --input vehicle_positions:gtfs_rt_protobuf=./path/to/vp.pb \
  --input trip_updates:gtfs_rt_protobuf=./path/to/tu.pb \
  -o /tmp/rt-out
```

Both runs apply the same regex from `pipelines/shared.py`. To change the pattern, edit `shared.py` only.

## Why this works

When the scanner loads a pipeline folder at `<repo>/pipelines/<name>/`, it inserts the parent directory (`<repo>/pipelines/`) onto `sys.path`. That makes any sibling module (here `shared.py`) directly importable as `from shared import …` from inside any pipeline package — no `__init__.py` on `pipelines/`, no package-prefix in the import.

The scanner skips sibling entries that aren't pipeline directories (anything that isn't a directory, plus directories starting with `_` or `.`), so a regular Python module is invisible to pipeline discovery while still being importable.
