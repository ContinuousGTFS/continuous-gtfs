"""Demo agency RT transforms for testing."""

from continuous_gtfs.builtins.realtime import (
    FilterStopsByID,
    PassThrough,
    UpdateFeedHeader,
)

# Filter non-revenue expansion stations
filter_stops = FilterStopsByID(
    blocked_stop_ids={"E01", "E07"},
    description="Filter non-revenue expansion stations",
)

# Update feed header
update_header = UpdateFeedHeader(
    after=[filter_stops],
)

# No-op for baseline comparison
passthrough = PassThrough()
