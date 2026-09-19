"""Demo agency schedule transforms — single canonical pipeline.

A representative agency pipeline: removes inactive service, removes
pre-opening expansion stops, updates route metadata, renames stations,
and clears unnecessary fields. Environment variance is handled at the
pipeline version level if ever needed.
"""

from continuous_gtfs.builtins.schedule import (
    ClearField,
    InitScheduleOutput,
    MatchCondition,
    RemoveRows,
    UpdateFields,
)

# --- Init ---
#
# Seeds ctx.output from the "schedule" input before any transform runs
# (before="*" is built into the step). The worker loads pipelines with
# resolve_dag(scan_pipeline(dir)) and injects nothing, so the pipeline
# module itself must declare its init — this fixture was missing it,
# which made every full-pipeline run of it start on an empty output.

init_schedule_output = InitScheduleOutput()

# --- Calendar cleanup ---

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

remove_llr_calendar = RemoveRows(
    "calendar.txt",
    [MatchCondition("service_id", regex=r"^XLR.*")],
    description="Remove XLR service IDs from calendar",
    after=[remove_inactive_calendars],
)

remove_tline_calendar = RemoveRows(
    "calendar.txt",
    [MatchCondition("service_id", regex=r"^YLINE.*")],
    description="Remove YLINE service IDs from calendar",
    after=[remove_inactive_calendars],
)

remove_llr_calendar_dates = RemoveRows(
    "calendar_dates.txt",
    [MatchCondition("service_id", regex=r"^XLR.*")],
    description="Remove XLR service IDs from calendar_dates",
)

remove_tline_calendar_dates = RemoveRows(
    "calendar_dates.txt",
    [MatchCondition("service_id", regex=r"^YLINE.*")],
    description="Remove YLINE service IDs from calendar_dates",
)

# --- Stop removal (pre-opening expansion stations) ---

remove_stop_e01 = RemoveRows(
    "stops.txt",
    [MatchCondition("stop_id", value="E01")],
    description="Remove non-revenue expansion stop E01",
)

remove_stop_e07 = RemoveRows(
    "stops.txt",
    [MatchCondition("stop_id", value="E07")],
    description="Remove non-revenue expansion stop E07",
)

# --- Route metadata updates ---

update_red_line = UpdateFields(
    "routes.txt",
    [MatchCondition("route_id", value="RED")],
    {"route_long_name": "Northbrook - Lakeside"},
    description="Update Red Line route metadata",
)

update_blue_line = UpdateFields(
    "routes.txt",
    [MatchCondition("route_id", value="BLUE")],
    {
        "route_long_name": "Easton - Riverview",
        "route_color": "0055AA",
        "route_text_color": "FFFFFF",
    },
    description="Update Blue Line route metadata",
)

# --- Station renames ---

rename_concert_hall_455 = UpdateFields(
    "stops.txt",
    [MatchCondition("stop_id", value="455")],
    {
        "stop_name": "Concert Hall",
        "stop_desc": "Concert Hall to Lakeside",
    },
    description="Rename Old Town (455) to Concert Hall",
)

rename_concert_hall_565 = UpdateFields(
    "stops.txt",
    [MatchCondition("stop_id", value="565")],
    {
        "stop_name": "Concert Hall",
        "stop_desc": "Concert Hall to Midtown",
    },
    description="Rename Old Town (565) to Concert Hall",
)

rename_concert_hall_c05 = UpdateFields(
    "stops.txt",
    [MatchCondition("stop_id", value="C05")],
    {"stop_name": "Concert Hall"},
    description="Rename Old Town (C05) to Concert Hall",
)

# --- Trip cleanup ---

clear_sndr_tl_blocks = ClearField(
    "trips.txt",
    "block_id",
    [MatchCondition("route_id", value="CR_S")],
    description="Clear block_id for South Commuter Rail",
)

clear_sndr_ev_blocks = ClearField(
    "trips.txt",
    "block_id",
    [MatchCondition("route_id", value="CR_N")],
    description="Clear block_id for North Commuter Rail",
)

clear_red_line_short_name = ClearField(
    "trips.txt",
    "trip_short_name",
    [MatchCondition("route_id", value="RED")],
    description="Clear trip_short_name for Red Line",
)

clear_blue_line_short_name = ClearField(
    "trips.txt",
    "trip_short_name",
    [MatchCondition("route_id", value="BLUE")],
    description="Clear trip_short_name for Blue Line",
)
