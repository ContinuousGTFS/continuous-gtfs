"""Example schedule pipeline — rewrites trip_id consistently with the
realtime pipeline."""

FEED_TYPE = "schedule"

INPUTS = {
    "schedule": "gtfs_schedule_zip",
}
