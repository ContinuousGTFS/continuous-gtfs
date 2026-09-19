"""Schedule transforms: seed ctx.output, then rewrite trip_id."""

from shared import TRIP_ID_REWRITE  # see ../shared.py

from continuous_gtfs.builtins.schedule import (
    InitScheduleOutput,
    TransformTripId,
)

init = InitScheduleOutput("schedule")
rewrite_trip_id = TransformTripId(**TRIP_ID_REWRITE)
