"""Source of truth for cross-pipeline rewrites.

Both the schedule and realtime pipelines in this example import
TRIP_ID_REWRITE and pass it to their respective `TransformTripId`
builtins, so the same regex is applied to both feeds.
"""

TRIP_ID_REWRITE = {
    # Strip the legacy "_LR_" infix from light-rail trip IDs.
    # Backref-free so it works identically under both Polars/Rust regex
    # (schedule) and Python re (realtime).
    "pattern": r"_LR_",
    "replacement": r"LR-",
}
