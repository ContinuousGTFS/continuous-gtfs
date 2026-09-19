"""Three steps: seed the output, clean calendars with a builtin, and one
custom @step — enough to show both ways of defining a transform."""

from continuous_gtfs import step
from continuous_gtfs.builtins.schedule import (
    InitScheduleOutput,
    MatchCondition,
    RemoveRows,
)

# Seed ctx.output from the "schedule" input before any transform runs
# (before="*" is built into the step).
init = InitScheduleOutput()

# A builtin step: drop calendar rows with no active service days.
remove_inactive_calendars = RemoveRows(
    "calendar.txt",
    [
        MatchCondition("monday", value="0"),
        MatchCondition("tuesday", value="0"),
        MatchCondition("wednesday", value="0"),
        MatchCondition("thursday", value="0"),
        MatchCondition("friday", value="0"),
        MatchCondition("saturday", value="0"),
        MatchCondition("sunday", value="0"),
    ],
    description="Remove calendar records with no active service days",
)


# A custom step: anything you can express over a Polars DataFrame.
@step(files=["stops.txt"])
def drop_test_stops(ctx):
    """Remove stops whose id carries a TEST_ prefix."""
    stops = ctx.output["stops.txt"]
    ctx.output["stops.txt"] = stops.filter(
        ~stops["stop_id"].str.starts_with("TEST_")
    )
