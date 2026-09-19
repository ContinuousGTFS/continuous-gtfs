"""Semantic removals — RemoveRoutes / RemoveTrips / RemoveStops / RemoveServices.

Fixtures model a realistic mid-size rail agency feed (ids, names, times,
shape points), trimmed to the columns the tests read. Where a row had to be
composed — the base feed carries no transfers, pathways, frequencies, or
never-active calendars — the test says so.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from continuous_gtfs.builtins.schedule import (
    MatchCondition,
    RemoveRoutes,
    RemoveServices,
    RemoveStops,
    RemoveTrips,
)
from continuous_gtfs.schedule_records import (
    feed_start_date,
    remove_records,
    service_active_dates,
)
from continuous_gtfs.testing import gtfs_df, schedule_context

SAT = "XLR_2026-03-29_Spring26_Saturday"
CRR_WK = "CRR_2026-04-15_Spring26_Weekday"
EMW = "XLR_2026-04-14_AprEMW_Weekday"  # calendar_dates-only
T1001 = f"{SAT}_RED_1001"
T1002 = f"{SAT}_RED_1002"
T3001 = f"{SAT}_BLUE_3001"
TS1500 = f"{CRR_WK}_CR_S_1500"


def _feed() -> dict[str, pl.DataFrame]:
    return {
        "routes.txt": gtfs_df("""
            agency_id,route_id,route_short_name,route_long_name,route_type
            40,RED,Red Line,Northbrook - Fairview,0
            40,BLUE,Blue Line,Northbrook - Riverview,0
            40,CR_S,S Line,Central City - Harborton/Lakeview,2
            40,CR_N,N Line,Northport - Central City,2
        """),
        # CR_N has no trips here: the pre-existing-empty control.
        "trips.txt": gtfs_df(f"""
            route_id,trip_id,service_id,shape_id
            RED,{T1001},{SAT},C15:N23
            RED,{T1002},{SAT},S07:N23
            BLUE,{T3001},{SAT},E31:N23
            CR_S,{TS1500},{CRR_WK},CR_S_NBLW_shp
        """),
        # T1001 is excerpted down to its N15 call only, so removing N15
        # empties it; T1002 keeps its Fairview call. T3001's row uses
        # the real times at a Riverview platform.
        "stop_times.txt": gtfs_df(f"""
            trip_id,stop_id,arrival_time,departure_time,stop_sequence
            {T1001},N15-T1,05:03:30,05:04:00,12
            {T1002},S07-T2,04:34:30,04:35:00,1
            {T1002},N15-T1,05:58:30,05:59:00,23
            {T3001},E31-T1,04:09:30,04:10:00,1
            {TS1500},S_LW,04:35:30,04:36:00,1
        """),
        "stops.txt": gtfs_df("""
            stop_id,stop_name,location_type,parent_station
            N15,Cedar Grove/148th,1,
            N15-E-00001,Cedar Grove/148th Station Entrance A,2,N15
            N15-E-00002,Cedar Grove/148th Station Entrance B,2,N15
            N15-T1,Cedar Grove/148th,,N15
            N15-T2,Cedar Grove/148th,,N15
            S07,Fairview Downtown,1,
            S07-T2,Fairview Downtown,,S07
            E31,Riverview,1,
            E31-T1,Riverview,,E31
            S_LW,Lakeview Station,,
        """),
        "calendar.txt": gtfs_df(f"""
            service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
            {SAT},0,0,0,0,0,1,0,20260329,20260828
            {CRR_WK},1,1,1,1,1,0,0,20260415,20260828
        """),
        "calendar_dates.txt": gtfs_df(f"""
            service_id,date,exception_type
            {SAT},20260425,2
            {SAT},20260502,2
            {EMW},20260414,1
        """),
        "shapes.txt": gtfs_df("""
            shape_id,shape_pt_sequence,shape_pt_lat,shape_pt_lon
            C15:N23,4159,38.580586,-90.327362
            C15:N23,4160,38.587892,-90.327357
            S07:N23,7960,38.316181,-90.303554
            S07:N23,7961,38.316566,-90.303311
            E31:N23,4774,38.671460,-90.117960
            E31:N23,4775,38.669680,-90.110710
            CR_S_NBLW_shp,3837,38.153189,-90.499103
            CR_S_NBLW_shp,3838,38.155073,-90.496039
        """),
        "route_networks.txt": gtfs_df("""
            network_id,route_id
            rail_network,RED
            rail_network,BLUE
        """),
        "fare_rules.txt": gtfs_df("""
            fare_id,route_id,origin_id,destination_id
            12,CR_S,S_LW,S_ST
            13,CR_S,S_LW,S_TD
        """),
        "feed_info.txt": gtfs_df("""
            feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date
            Demo Transit,https://www.demotransit.example,en,20260414,20260828
        """),
    }


def _ctx(feed=None):
    return schedule_context(**(feed or _feed()))


def _col(ctx, file, column):
    return ctx.output[file][column].to_list()


def _findings(ctx, code):
    return [f for f in ctx.findings if f["code"] == code]


def _rows_removed(ctx) -> dict[str, tuple[int, str]]:
    return {
        f["context"]["file"]: (f["occurrence_count"], f["context"]["role"])
        for f in _findings(ctx, "rows_removed")
    }


def _apply(step, ctx):
    step.name = type(step).__name__.lower()
    step.apply(ctx)
    return ctx


# --- RemoveRoutes -----------------------------------------------------------


def test_remove_routes_cascades_to_trips_stop_times_and_network_rows():
    ctx = _apply(RemoveRoutes(ids=["RED"]), _ctx())

    assert _col(ctx, "routes.txt", "route_id") == ["BLUE", "CR_S", "CR_N"]
    assert _col(ctx, "trips.txt", "trip_id") == [T3001, TS1500]
    assert _col(ctx, "stop_times.txt", "trip_id") == [T3001, TS1500]
    assert _col(ctx, "route_networks.txt", "route_id") == ["BLUE"]
    # The Saturday service still carries the Blue Line trip: not emptied, kept.
    assert SAT in _col(ctx, "calendar.txt", "service_id")
    assert _rows_removed(ctx) == {
        "routes.txt": (1, "root"),
        "trips.txt": (2, "cascade"),
        "stop_times.txt": (3, "cascade"),
        "route_networks.txt": (1, "cascade"),
    }


def test_remove_routes_warns_about_emptied_shapes_but_keeps_them():
    ctx = _apply(RemoveRoutes(ids=["RED"]), _ctx())

    shapes = set(_col(ctx, "shapes.txt", "shape_id"))
    assert {"C15:N23", "S07:N23"} <= shapes
    warned = sorted(
        f["context"]["shape_id"] for f in _findings(ctx, "shape_without_trips")
    )
    assert warned == ["C15:N23", "S07:N23"]
    assert all(
        f["severity"] == "warning" for f in _findings(ctx, "shape_without_trips")
    )
    assert RemoveRoutes(ids=["x"]).declared_findings["shape_without_trips"] == [
        "shape_id"
    ]


def test_remove_routes_emptying_a_service_removes_it_from_both_calendar_files():
    ctx = _apply(RemoveRoutes(ids=["CR_S"]), _ctx())

    assert CRR_WK not in _col(ctx, "calendar.txt", "service_id")
    assert _col(ctx, "fare_rules.txt", "route_id") == []
    assert _rows_removed(ctx)["calendar.txt"] == (1, "prune")
    assert _col(ctx, "trips.txt", "trip_id") == [T1001, T1002, T3001]


def test_remove_routes_empty_services_keep_leaves_the_calendar_row():
    ctx = _apply(RemoveRoutes(ids=["CR_S"], empty_services="keep"), _ctx())

    assert CRR_WK in _col(ctx, "calendar.txt", "service_id")
    assert "calendar.txt" not in _rows_removed(ctx)


def test_remove_routes_by_condition_with_exclude():
    step = RemoveRoutes(
        [MatchCondition("route_type", value="2")],
        exclude=[[MatchCondition("route_id", value="CR_N")]],
    )
    ctx = _apply(step, _ctx())
    assert _col(ctx, "routes.txt", "route_id") == ["RED", "BLUE", "CR_N"]


# --- RemoveTrips ------------------------------------------------------------


def test_remove_trips_warns_about_the_route_it_emptied_and_only_that_route():
    ctx = _apply(RemoveTrips(ids=[TS1500]), _ctx())

    assert "CR_S" in _col(ctx, "routes.txt", "route_id")
    warned = [f["context"]["route_id"] for f in _findings(ctx, "route_without_trips")]
    # CR_N had no trips before the step ran — not this step's doing.
    assert warned == ["CR_S"]
    # The commuter rail weekday service lost its last trip: removed by default.
    assert CRR_WK not in _col(ctx, "calendar.txt", "service_id")
    assert _rows_removed(ctx)["calendar.txt"] == (1, "prune")
    assert RemoveTrips(ids=["x"]).declared_findings["route_without_trips"] == [
        "route_id"
    ]


def test_remove_trips_empty_routes_remove_cascades_into_the_route():
    ctx = _apply(RemoveTrips(ids=[TS1500], empty_routes="remove"), _ctx())

    assert "CR_S" not in _col(ctx, "routes.txt", "route_id")
    assert _col(ctx, "fare_rules.txt", "route_id") == []
    assert _findings(ctx, "route_without_trips") == []
    assert _rows_removed(ctx)["routes.txt"] == (1, "prune")


def test_remove_trips_empty_routes_keep_is_silent():
    ctx = _apply(RemoveTrips(ids=[TS1500], empty_routes="keep"), _ctx())
    assert "CR_S" in _col(ctx, "routes.txt", "route_id")
    assert _findings(ctx, "route_without_trips") == []


def test_remove_trips_shape_warn_default_and_remove_override():
    ctx = _apply(RemoveTrips(ids=[T1001]), _ctx())
    assert "C15:N23" in _col(ctx, "shapes.txt", "shape_id")
    assert [
        f["context"]["shape_id"] for f in _findings(ctx, "shape_without_trips")
    ] == ["C15:N23"]
    # Service and route both still have trips: nothing else pruned or warned.
    assert SAT in _col(ctx, "calendar.txt", "service_id")
    assert _findings(ctx, "route_without_trips") == []

    ctx = _apply(RemoveTrips(ids=[T1001], empty_shapes="remove"), _ctx())
    assert "C15:N23" not in _col(ctx, "shapes.txt", "shape_id")
    assert _findings(ctx, "shape_without_trips") == []
    assert _rows_removed(ctx)["shapes.txt"] == (2, "prune")


# --- RemoveStops ------------------------------------------------------------


def test_remove_stops_station_takes_platforms_entrances_stop_times_and_emptied_trip():
    ctx = _apply(RemoveStops(ids=["N15"]), _ctx())

    stops = _col(ctx, "stops.txt", "stop_id")
    assert not any(s.startswith("N15") for s in stops)
    assert {"S07", "S07-T2", "E31", "E31-T1", "S_LW"} <= set(stops)
    # T1001 called only at N15 → emptied → removed; T1002 keeps Fairview.
    assert _col(ctx, "trips.txt", "trip_id") == [T1002, T3001, TS1500]
    assert _col(ctx, "stop_times.txt", "stop_id") == ["S07-T2", "E31-T1", "S_LW"]
    took = _findings(ctx, "trips_removed_with_stops")
    assert len(took) == 1 and took[0]["occurrence_count"] == 1
    assert took[0]["severity"] == "warning"
    assert RemoveStops(ids=["x"]).declared_findings["trips_removed_with_stops"] == []
    # What T1001 emptied follows the trip defaults: its shape is warned.
    assert [
        f["context"]["shape_id"] for f in _findings(ctx, "shape_without_trips")
    ] == ["C15:N23"]
    assert _findings(ctx, "route_without_trips") == []
    assert SAT in _col(ctx, "calendar.txt", "service_id")
    assert _rows_removed(ctx) == {
        "stops.txt": (5, "root"),
        "stop_times.txt": (2, "cascade"),
        "trips.txt": (1, "prune"),
    }


def test_remove_stops_detach_keeps_platforms_standalone_and_removes_entrances():
    ctx = _apply(RemoveStops(ids=["N15"], children="detach"), _ctx())

    stops = ctx.output["stops.txt"]
    assert "N15" not in stops["stop_id"].to_list()
    assert not any(s.startswith("N15-E") for s in stops["stop_id"].to_list())
    platforms = stops.filter(pl.col("stop_id").is_in(["N15-T1", "N15-T2"]))
    assert platforms.height == 2
    assert platforms["parent_station"].to_list() == ["", ""]
    # Platforms stayed, so their stop_times and trips are untouched.
    assert _col(ctx, "stop_times.txt", "stop_id").count("N15-T1") == 2
    assert _col(ctx, "trips.txt", "trip_id") == [T1001, T1002, T3001, TS1500]
    assert _findings(ctx, "trips_removed_with_stops") == []
    assert _rows_removed(ctx) == {"stops.txt": (3, "root")}


def test_remove_stops_empty_trips_keep_leaves_the_stopless_trip():
    ctx = _apply(RemoveStops(ids=["N15"], empty_trips="keep"), _ctx())
    assert T1001 in _col(ctx, "trips.txt", "trip_id")
    assert _findings(ctx, "trips_removed_with_stops") == []


def test_remove_stops_platform_alone_leaves_station_and_sibling():
    ctx = _apply(RemoveStops(ids=["N15-T2"]), _ctx())
    assert {"N15", "N15-T1", "N15-E-00001"} <= set(_col(ctx, "stops.txt", "stop_id"))
    assert "N15-T2" not in _col(ctx, "stops.txt", "stop_id")
    assert _col(ctx, "trips.txt", "trip_id") == [T1001, T1002, T3001, TS1500]


def test_remove_stops_follows_transfers_pathways_and_stop_areas():
    # Composed rows: the base feed has no transfers or pathways.
    feed = _feed()
    feed["transfers.txt"] = gtfs_df("""
        from_stop_id,to_stop_id,transfer_type
        N15-T1,N15-T2,0
        S07-T2,S_LW,0
    """)
    feed["pathways.txt"] = gtfs_df("""
        pathway_id,from_stop_id,to_stop_id,pathway_mode
        p1,N15-E-00001,N15-T1,1
        p2,E31,E31-T1,1
    """)
    feed["stop_areas.txt"] = gtfs_df("""
        area_id,stop_id
        cedargrove,N15
        fairview,S07
    """)
    ctx = _apply(RemoveStops(ids=["N15"]), _ctx(feed))
    assert _col(ctx, "transfers.txt", "from_stop_id") == ["S07-T2"]
    assert _col(ctx, "pathways.txt", "pathway_id") == ["p2"]
    assert _col(ctx, "stop_areas.txt", "area_id") == ["fairview"]


def test_remove_trips_follows_frequencies():
    # Composed rows: the base feed has no frequencies.
    feed = _feed()
    feed["frequencies.txt"] = gtfs_df(f"""
        trip_id,start_time,end_time,headway_secs
        {T3001},06:00:00,09:00:00,600
        {TS1500},06:00:00,09:00:00,1800
    """)
    ctx = _apply(RemoveTrips(ids=[T3001]), _ctx(feed))
    assert _col(ctx, "frequencies.txt", "trip_id") == [TS1500]


# --- RemoveServices ---------------------------------------------------------


def test_remove_services_by_id_removes_both_calendar_files_and_cascades_to_trips():
    ctx = _apply(RemoveServices(ids=[SAT]), _ctx())

    assert _col(ctx, "calendar.txt", "service_id") == [CRR_WK]
    assert _col(ctx, "calendar_dates.txt", "service_id") == [EMW]
    assert _col(ctx, "trips.txt", "trip_id") == [TS1500]
    assert _col(ctx, "stop_times.txt", "trip_id") == [TS1500]
    # Both light-rail routes lost every trip: warned, kept.
    warned = sorted(
        f["context"]["route_id"] for f in _findings(ctx, "route_without_trips")
    )
    assert warned == ["BLUE", "RED"]
    assert {"RED", "BLUE"} <= set(_col(ctx, "routes.txt", "route_id"))
    assert _rows_removed(ctx) == {
        "calendar.txt": (1, "root"),
        "calendar_dates.txt": (2, "root"),
        "trips.txt": (3, "cascade"),
        "stop_times.txt": (4, "cascade"),
    }


def test_remove_services_regex_reaches_a_calendar_dates_only_service():
    before = _feed()
    ctx = _apply(
        RemoveServices([MatchCondition("service_id", regex=r"AprEMW")]), _ctx()
    )
    assert _col(ctx, "calendar_dates.txt", "service_id") == [SAT, SAT]
    assert ctx.output["calendar.txt"].equals(before["calendar.txt"])
    assert ctx.output["trips.txt"].equals(before["trips.txt"])
    assert _rows_removed(ctx) == {"calendar_dates.txt": (1, "root")}


def test_remove_services_exclude_protects_a_service():
    step = RemoveServices(
        [MatchCondition("service_id", regex=r"^XLR")],
        exclude=[[MatchCondition("service_id", value=SAT)]],
    )
    ctx = _apply(step, _ctx())
    assert SAT in _col(ctx, "calendar.txt", "service_id")
    assert EMW not in _col(ctx, "calendar_dates.txt", "service_id")


def test_remove_services_rejects_conditions_on_other_columns():
    with pytest.raises(ValueError, match="service_id"):
        RemoveServices([MatchCondition("monday", value="0")])
    with pytest.raises(ValueError, match="service_id"):
        RemoveServices(ids=["x"], exclude=[[MatchCondition("end_date", value="1")]])


def _never_active_feed() -> dict[str, pl.DataFrame]:
    """Composed calendars in the agency's id style: the Spring 2026 feed
    itself has no never-active service."""
    return {
        "calendar.txt": gtfs_df("""
            service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
            XLR_DEAD_AllZero_Weekday,0,0,0,0,0,0,0,20260401,20260430
            XLR_Cancelled_Saturday,0,0,0,0,0,1,0,20260404,20260411
            XLR_AddsOnly_Weekday,0,0,0,0,0,0,0,20260401,20260430
            XLR_Live_Weekday,1,1,1,1,1,0,0,20260401,20260430
        """),
        "calendar_dates.txt": gtfs_df("""
            service_id,date,exception_type
            XLR_Cancelled_Saturday,20260404,2
            XLR_Cancelled_Saturday,20260411,2
            XLR_AddsOnly_Weekday,20260501,1
            XLR_Live_Weekday,20260403,2
        """),
        "trips.txt": gtfs_df("""
            route_id,trip_id,service_id,shape_id
            RED,dead_1,XLR_DEAD_AllZero_Weekday,C15:N23
            RED,live_1,XLR_Live_Weekday,C15:N23
            RED,adds_1,XLR_AddsOnly_Weekday,C15:N23
        """),
        "routes.txt": gtfs_df("""
            route_id,route_type
            RED,0
        """),
    }


def test_never_active_removes_all_zero_and_fully_cancelled_services():
    ctx = _apply(RemoveServices(never_active=True), _ctx(_never_active_feed()))

    assert _col(ctx, "calendar.txt", "service_id") == ["XLR_Live_Weekday"]
    # The cancelled service's exception rows go with it; the adds-only
    # service's add stays; the live service's removal stays.
    assert _col(ctx, "calendar_dates.txt", "service_id") == [
        "XLR_AddsOnly_Weekday",
        "XLR_Live_Weekday",
    ]
    assert _col(ctx, "trips.txt", "trip_id") == ["live_1", "adds_1"]
    assert _rows_removed(ctx) == {
        # 2 never-active rows + the simplified all-zero row of the adds-only service
        "calendar.txt": (3, "root"),
        "calendar_dates.txt": (2, "root"),
        "trips.txt": (1, "cascade"),
    }


def test_never_active_without_simplify_leaves_the_all_zero_row_of_an_active_service():
    ctx = _apply(
        RemoveServices(never_active=True, simplify_calendar=False),
        _ctx(_never_active_feed()),
    )
    assert _col(ctx, "calendar.txt", "service_id") == [
        "XLR_AddsOnly_Weekday",
        "XLR_Live_Weekday",
    ]
    assert "adds_1" in _col(ctx, "trips.txt", "trip_id")
    assert _rows_removed(ctx)["calendar.txt"] == (2, "root")


def test_never_active_on_a_feed_with_nothing_expired_is_a_silent_noop():
    feed = _feed()
    ctx = _apply(RemoveServices(never_active=True), _ctx(feed))
    assert ctx.findings == []
    for name, df in feed.items():
        assert ctx.output[name].equals(df), name


def test_never_active_exclude_protects_a_dead_service():
    step = RemoveServices(
        never_active=True,
        exclude=[[MatchCondition("service_id", value="XLR_DEAD_AllZero_Weekday")]],
    )
    ctx = _apply(step, _ctx(_never_active_feed()))
    assert "XLR_DEAD_AllZero_Weekday" in _col(ctx, "calendar.txt", "service_id")
    assert "XLR_Cancelled_Saturday" not in _col(ctx, "calendar.txt", "service_id")


# --- Shared behavior ----------------------------------------------------------


def test_ids_shortcut_selects_exactly_the_any_of_groups():
    by_ids = _apply(RemoveStops(ids=["N15-T1", "N15-T2"]), _ctx())
    by_groups = _apply(
        RemoveStops(
            [
                [MatchCondition("stop_id", value="N15-T1")],
                [MatchCondition("stop_id", value="N15-T2")],
            ]
        ),
        _ctx(),
    )
    for name in _feed():
        assert by_ids.output[name].equals(by_groups.output[name]), name
    assert [f["code"] for f in by_ids.findings] == [
        f["code"] for f in by_groups.findings
    ]
    # And the selection actually did something.
    assert "N15-T1" not in _col(by_ids, "stops.txt", "stop_id")


def test_condition_on_a_missing_column_warns_and_leaves_the_feed_untouched():
    feed = _feed()
    step = RemoveRoutes([MatchCondition("network_id", value="rail_network")])
    ctx = _apply(step, _ctx(feed))
    assert [f["code"] for f in ctx.findings] == ["column_missing"]
    assert ctx.findings[0]["context"] == {"file": "routes.txt", "column": "network_id"}
    for name, df in feed.items():
        assert ctx.output[name].equals(df), name


def test_removed_records_are_recorded_into_id_mappings():
    ctx = _apply(RemoveStops(ids=["N15"]), _ctx())
    stops = ctx.get_id_mappings("stops.txt", "stop_id")
    assert set(stops) == {"N15", "N15-E-00001", "N15-E-00002", "N15-T1", "N15-T2"}
    assert set(stops.values()) == {None}
    assert ctx.get_id_mappings("trips.txt", "trip_id") == {T1001: None}

    ctx = _apply(RemoveServices(ids=[CRR_WK]), _ctx())
    assert ctx.get_id_mappings("calendar.txt", "service_id") == {CRR_WK: None}
    assert ctx.get_id_mappings("trips.txt", "trip_id") == {TS1500: None}


def test_rows_removed_finding_shape():
    ctx = _apply(RemoveRoutes(ids=["RED"]), _ctx())
    finding = next(
        f for f in _findings(ctx, "rows_removed") if f["context"]["file"] == "trips.txt"
    )
    assert finding["severity"] == "info"
    assert finding["occurrence_count"] == 2
    assert finding["context"] == {
        "file": "trips.txt",
        "record_type": "route",
        "role": "cascade",
    }
    assert RemoveRoutes(ids=["x"]).declared_findings["rows_removed"] == ["file"]
    assert not finding["undeclared"]


def test_steps_declare_every_file_they_can_touch():
    assert set(RemoveStops(ids=["x"]).files) >= {
        "stops.txt",
        "stop_times.txt",
        "transfers.txt",
        "pathways.txt",
        "stop_areas.txt",
        "trips.txt",
        "frequencies.txt",
        "calendar.txt",
        "calendar_dates.txt",
        "routes.txt",
        "shapes.txt",
    }
    assert set(RemoveServices(ids=["x"]).files) >= {
        "calendar.txt",
        "calendar_dates.txt",
        "trips.txt",
        "stop_times.txt",
        "routes.txt",
        "shapes.txt",
    }
    assert "stops.txt" not in RemoveServices(ids=["x"]).files


def test_construction_errors():
    with pytest.raises(ValueError, match="conditions"):
        RemoveRoutes()
    with pytest.raises(ValueError, match="never_active"):
        RemoveServices()
    with pytest.raises(ValueError, match="empty_routes"):
        RemoveTrips(ids=["x"], empty_routes="delete")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="children"):
        RemoveStops(ids=["x"], children="orphan")  # type: ignore[arg-type]


def test_missing_home_file_and_unknown_ids_are_silent_noops():
    feed = _feed()
    del feed["routes.txt"]
    ctx = _apply(RemoveRoutes(ids=["RED"]), _ctx(feed))
    assert ctx.findings == []
    assert ctx.output["trips.txt"].equals(feed["trips.txt"])

    feed = _feed()
    ctx = _apply(RemoveStops(ids=["NOT_A_STOP"]), _ctx(feed))
    assert ctx.findings == []
    assert ctx.id_mappings == {}
    for name, df in feed.items():
        assert ctx.output[name].equals(df), name


# --- Engine and date helpers ---------------------------------------------------


def test_remove_records_only_counts_records_this_call_emptied():
    output = _feed()
    # CR_N is already empty; CR_S becomes empty here.
    report = remove_records(output, "trip", [TS1500], policy={"route": "warn"})
    assert report.emptied_kept == {"route": ["CR_S"]}
    assert report.removed_ids == {"trip": {TS1500}}


def test_remove_records_result_is_independent_of_id_order_and_repeats():
    a, b = _feed(), _feed()
    remove_records(a, "stop", ["N15", "N15-T1"], policy={"trip": "remove"})
    remove_records(b, "stop", ["N15-T1", "N15", "N15"], policy={"trip": "remove"})
    for name in a:
        assert a[name].equals(b[name]), name


def test_service_active_dates_expands_calendar_and_applies_exceptions():
    active = service_active_dates(_feed())
    saturdays = active[SAT]
    assert saturdays[0] == date(2026, 4, 4)  # 2026-03-29 itself is a Sunday
    assert date(2026, 4, 25) not in saturdays and date(2026, 5, 2) not in saturdays
    assert date(2026, 4, 18) in saturdays
    assert active[EMW] == [date(2026, 4, 14)]
    assert all(d.weekday() < 5 for d in active[CRR_WK])


def test_feed_start_date_reads_feed_info_or_returns_none():
    assert feed_start_date(_feed()) == date(2026, 4, 14)
    feed = _feed()
    del feed["feed_info.txt"]
    assert feed_start_date(feed) is None


# --- RemoveServices(expired_before=...) ----------------------------------------
#
# Spring 2026 rows: the April/May weekday calendars, the
# calendar_dates-only AprEMW service, the YLINE weekday service with its
# cancellation dates, a commuter rail control. Composed and marked as such:
# the MAYFIELD type-1 add on 20260615 (keeps an expired calendar row alive),
# the all-zero NEVER row, and feed_info's feed_start_date of 20260601.

APR13 = "XLR_2026-04-13_Spring26_Weekday"  # weekdays to 20260501
MAYFIELD = "XLR_2026-05-04_MayfieldNB_Weekday"  # to 20260515
SHUTTLE = "Shuttle_May_26-28"  # weekdays 20260526..20260528
YLINE = "YLINE_2026-01-26_YLink_Weekday_12mins"  # to 20260828
NEVER = "XLR_composed_never_active"
CUTOFF = "20260601"

T_APR13 = f"{APR13}_RED_1001"
T_MAYFIELD = f"{MAYFIELD}_RED_1001"
T_EMW = f"{EMW}_RED_1001"
T_SHUTTLE = "Shuttle_May_26-28-Weekday-RED-SHUTTLE-ALS-FWD-NB-2240-a8f23e3"
T_YLINE = f"{YLINE}_YLINE_10"


def _expiry_feed() -> dict[str, pl.DataFrame]:
    return {
        "routes.txt": gtfs_df("""
            agency_id,route_id,route_short_name,route_long_name,route_type
            40,RED,Red Line,Northbrook - Fairview,0
            40,RED-SHUTTLE,Shuttle,Red Line Shuttle Bus,3
            40,YLINE,Y Line,Harborton Dome - Hillside,0
            40,CR_S,S Line,Central City - Harborton/Lakeview,2
        """),
        "trips.txt": gtfs_df(f"""
            route_id,trip_id,service_id,shape_id
            RED,{T_APR13},{APR13},C15:N23
            RED,{T_MAYFIELD},{MAYFIELD},C15:N23
            RED,{T_EMW},{EMW},C15:N23
            RED-SHUTTLE,{T_SHUTTLE},{SHUTTLE},RED-SHUTTLE-ALS-FWD-NB
            YLINE,{T_YLINE},{YLINE},T01:T25
            CR_S,{TS1500},{CRR_WK},CR_S_NBLW_shp
        """),
        "stop_times.txt": gtfs_df(f"""
            trip_id,stop_id,arrival_time,departure_time,stop_sequence
            {T_APR13},99256,04:19:30,04:20:00,1
            {T_MAYFIELD},99256,04:19:30,04:20:00,1
            {T_EMW},99256,04:19:30,04:20:00,1
            {T_SHUTTLE},LS_S07_T1,22:40:00,22:40:00,1
            {T_YLINE},T01,07:47:30,07:48:00,1
            {TS1500},S_LW,04:35:30,04:36:00,1
        """),
        "calendar.txt": gtfs_df(f"""
            service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
            {APR13},1,1,1,1,1,0,0,20260413,20260501
            {MAYFIELD},1,1,1,1,1,0,0,20260504,20260515
            {SHUTTLE},1,1,1,1,1,0,0,20260526,20260528
            {YLINE},1,1,1,1,1,0,0,20260126,20260828
            {CRR_WK},1,1,1,1,1,0,0,20260415,20260828
            {NEVER},0,0,0,0,0,0,0,20260413,20260828
        """),
        "calendar_dates.txt": gtfs_df(f"""
            service_id,date,exception_type
            {EMW},20260414,1
            {YLINE},20260216,2
            {YLINE},20260406,2
            {YLINE},20260525,2
            {YLINE},20260703,2
            {MAYFIELD},20260615,1
        """),
        "feed_info.txt": gtfs_df(f"""
            feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date
            Demo Transit,https://www.demotransit.example,en,{CUTOFF},20260828
        """),
    }


def _services(ctx) -> tuple[set[str], set[str]]:
    return (
        set(_col(ctx, "calendar.txt", "service_id")),
        set(_col(ctx, "calendar_dates.txt", "service_id")),
    )


def test_expired_before_explicit_cutoff_removes_expired_services_and_cascades():
    ctx = _apply(RemoveServices(expired_before=CUTOFF), _ctx(_expiry_feed()))
    calendar, exceptions = _services(ctx)
    # Calendar-only services that ended before June are gone ...
    assert APR13 not in calendar and SHUTTLE not in calendar
    # ... the live ones stay, the never-active row is not this selector's.
    assert {YLINE, CRR_WK, NEVER} <= calendar
    assert set(_col(ctx, "trips.txt", "trip_id")) == {
        T_MAYFIELD,
        T_YLINE,
        TS1500,
    }
    assert set(_col(ctx, "stop_times.txt", "trip_id")) == {
        T_MAYFIELD,
        T_YLINE,
        TS1500,
    }
    assert EMW not in exceptions
    assert ctx.get_id_mappings("calendar.txt", "service_id") == {
        APR13: None,
        SHUTTLE: None,
        EMW: None,
    }


def test_expired_before_removes_a_calendar_dates_only_service_with_its_trip():
    ctx = _apply(RemoveServices(expired_before=CUTOFF), _ctx(_expiry_feed()))
    assert EMW not in _col(ctx, "calendar_dates.txt", "service_id")
    assert T_EMW not in _col(ctx, "trips.txt", "trip_id")
    assert T_EMW not in _col(ctx, "stop_times.txt", "trip_id")


def test_expired_before_keeps_a_service_an_added_exception_keeps_alive():
    ctx = _apply(RemoveServices(expired_before=CUTOFF), _ctx(_expiry_feed()))
    calendar, exceptions = _services(ctx)
    # MAYFIELD's calendar row ended 20260515, but its 20260615 add is on or
    # after the cutoff: the service, its calendar row, and its trip stay.
    assert MAYFIELD in calendar and MAYFIELD in exceptions
    assert T_MAYFIELD in _col(ctx, "trips.txt", "trip_id")
    # Counterfactual: without the add, the same cutoff removes it.
    feed = _expiry_feed()
    feed["calendar_dates.txt"] = feed["calendar_dates.txt"].filter(
        pl.col("service_id") != MAYFIELD
    )
    ctx = _apply(RemoveServices(expired_before=CUTOFF), _ctx(feed))
    assert MAYFIELD not in _col(ctx, "calendar.txt", "service_id")
    assert T_MAYFIELD not in _col(ctx, "trips.txt", "trip_id")


def test_expired_before_drops_past_exception_rows_on_a_surviving_service():
    ctx = _apply(RemoveServices(expired_before=CUTOFF), _ctx(_expiry_feed()))
    tline = ctx.output["calendar_dates.txt"].filter(pl.col("service_id") == YLINE)
    # 20260216 and 20260406 are before the cutoff; 20260525 is too. 20260703 stays.
    assert tline["date"].to_list() == ["20260703"]
    assert YLINE in _col(ctx, "calendar.txt", "service_id")
    assert T_YLINE in _col(ctx, "trips.txt", "trip_id")
    # Counted in calendar_dates.txt: EMW's one row + YLINE's three stale rows.
    assert _rows_removed(ctx)["calendar_dates.txt"] == (4, "root")


def test_expired_before_comparison_is_strict():
    # APR13's last active date is Friday 2026-05-01.
    ctx = _apply(RemoveServices(expired_before="20260501"), _ctx(_expiry_feed()))
    assert APR13 in _col(ctx, "calendar.txt", "service_id")
    ctx = _apply(RemoveServices(expired_before="20260502"), _ctx(_expiry_feed()))
    assert APR13 not in _col(ctx, "calendar.txt", "service_id")


def test_expired_before_true_resolves_to_feed_start_date():
    by_default = _apply(RemoveServices(expired_before=True), _ctx(_expiry_feed()))
    explicit = _apply(RemoveServices(expired_before=CUTOFF), _ctx(_expiry_feed()))
    for name in _expiry_feed():
        assert by_default.output[name].equals(explicit.output[name]), name
    assert [f["code"] for f in by_default.findings] == [
        f["code"] for f in explicit.findings
    ]
    # And the default actually reads the feed: an earlier feed_start_date
    # keeps APR13 (last active 20260501) and still removes EMW (20260414).
    feed = _expiry_feed()
    feed["feed_info.txt"] = feed["feed_info.txt"].with_columns(
        pl.lit("20260501").alias("feed_start_date")
    )
    ctx = _apply(RemoveServices(expired_before=True), _ctx(feed))
    assert APR13 in _col(ctx, "calendar.txt", "service_id")
    assert EMW not in _col(ctx, "calendar_dates.txt", "service_id")


def test_expired_before_true_without_feed_start_date_fails():
    feed = _expiry_feed()
    del feed["feed_info.txt"]
    with pytest.raises(ValueError, match="feed_start_date"):
        _apply(RemoveServices(expired_before=True), _ctx(feed))

    feed = _expiry_feed()
    feed["feed_info.txt"] = feed["feed_info.txt"].drop("feed_start_date")
    with pytest.raises(ValueError, match="feed_start_date"):
        _apply(RemoveServices(expired_before=True), _ctx(feed))

    # It fails before looking at the calendars, even with none present.
    feed = {"feed_info.txt": _expiry_feed()["feed_info.txt"].drop("feed_start_date")}
    with pytest.raises(ValueError, match="expired_before"):
        _apply(RemoveServices(expired_before=True), _ctx(feed))

    # An explicit cutoff needs no feed_info.txt at all.
    feed = _expiry_feed()
    del feed["feed_info.txt"]
    ctx = _apply(RemoveServices(expired_before=CUTOFF), _ctx(feed))
    assert APR13 not in _col(ctx, "calendar.txt", "service_id")


def test_expired_before_rejects_a_malformed_date_at_construction():
    with pytest.raises(ValueError, match="expired_before"):
        RemoveServices(expired_before="2026-06-01")
    with pytest.raises(ValueError, match="expired_before"):
        RemoveServices(expired_before="20261301")
    assert RemoveServices(expired_before=date(2026, 6, 1)).description == (
        "Remove services expired before 20260601"
    )
    assert RemoveServices(expired_before=True).description == (
        "Remove services expired before feed_start_date"
    )


def test_expired_before_with_nothing_expired_is_a_silent_noop():
    feed = _expiry_feed()
    # Before anything in the feed: no service has ended, no exception is past.
    ctx = _apply(RemoveServices(expired_before="20260101"), _ctx(feed))
    assert ctx.findings == []
    assert ctx.id_mappings == {}
    for name, df in feed.items():
        assert ctx.output[name].equals(df), name


def test_expired_before_combines_with_never_active_and_exclude():
    ctx = _apply(
        RemoveServices(expired_before=CUTOFF, never_active=True),
        _ctx(_expiry_feed()),
    )
    calendar = set(_col(ctx, "calendar.txt", "service_id"))
    assert NEVER not in calendar and APR13 not in calendar
    assert {MAYFIELD, YLINE, CRR_WK} == calendar

    step = RemoveServices(
        expired_before=CUTOFF,
        exclude=[[MatchCondition("service_id", value=SHUTTLE)]],
    )
    ctx = _apply(step, _ctx(_expiry_feed()))
    calendar = set(_col(ctx, "calendar.txt", "service_id"))
    assert SHUTTLE in calendar and APR13 not in calendar
    assert T_SHUTTLE in _col(ctx, "trips.txt", "trip_id")


def test_expired_before_findings_per_file_and_emptied_route_warning():
    ctx = _apply(RemoveServices(expired_before=CUTOFF), _ctx(_expiry_feed()))
    assert _rows_removed(ctx) == {
        "calendar.txt": (2, "root"),
        "calendar_dates.txt": (4, "root"),
        "trips.txt": (3, "cascade"),
        "stop_times.txt": (3, "cascade"),
    }
    # The shuttle route lost its only trip; Red Line kept MAYFIELD's.
    warned = [f["context"]["route_id"] for f in _findings(ctx, "route_without_trips")]
    assert warned == ["RED-SHUTTLE"]
    assert all(not f["undeclared"] for f in ctx.findings)


def test_expired_before_alone_is_a_selection():
    assert RemoveServices(expired_before=True)._has_selection()
    with pytest.raises(ValueError, match="expired_before"):
        RemoveServices()
