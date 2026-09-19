# Example — minimal schedule pipeline

The smallest working pipeline: an init step, one builtin transform, and one custom `@step` function.

```
pipelines/
└── demo/
    ├── __init__.py      # FEED_TYPE = "schedule", INPUTS = {"schedule": "gtfs_schedule_zip"}
    └── transforms.py    # init + RemoveRows builtin + custom @step
```

## Inspect the DAG

```bash
continuous-gtfs dag examples/minimal-schedule/pipelines/demo
```

## Run it on a feed

```bash
continuous-gtfs schedule examples/minimal-schedule/pipelines/demo \
  --input schedule=./path/to/gtfs.zip \
  -o /tmp/demo-out
```

The transformed zip lands in `/tmp/demo-out`, along with per-step results.
