"""Realtime transforms: rewrite trip_id using the same regex as the
schedule pipeline."""

from shared import TRIP_ID_REWRITE  # see ../shared.py

from continuous_gtfs.builtins.realtime import TransformTripId

rewrite_trip_id = TransformTripId(**TRIP_ID_REWRITE)
