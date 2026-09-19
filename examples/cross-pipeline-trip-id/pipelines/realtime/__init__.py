"""Example realtime pipeline — rewrites trip_id consistently with the
schedule pipeline."""

FEED_TYPE = "realtime"

INPUTS = {
    "vehicle_positions": "gtfs_rt_protobuf",
    "trip_updates": "gtfs_rt_protobuf",
}
