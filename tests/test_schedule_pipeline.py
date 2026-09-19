"""Integration tests for the schedule pipeline."""

import io
from pathlib import Path

import polars as pl
import pytest

from continuous_gtfs import (
    PipelineContext,
    execute_pipeline,
    resolve_dag,
    scan_pipeline,
)
from continuous_gtfs.builtins.schedule import (
    ClearField,
    InitScheduleOutput,
    MatchCondition,
    RemoveRows,
    SortKey,
    SortRows,
    TransformTripId,
    UpdateFeedInfo,
    UpdateFields,
)
from continuous_gtfs.pipelines.schedule import (
    extract_zip,
    package_zip,
    run_schedule_pipeline,
    validate_gtfs,
)
from continuous_gtfs.testing import gtfs_df, schedule_context

# --- Unit tests for builtins ---


def test_remove_rows_exact():
    df = pl.DataFrame({"route_id": ["A", "B", "C"], "name": ["a", "b", "c"]})
    ctx = PipelineContext(output={"routes.txt": df})

    step = RemoveRows(
        "routes.txt",
        [MatchCondition("route_id", value="B")],
    )
    step.name = "remove_b"
    step.apply(ctx)

    assert len(ctx.output["routes.txt"]) == 2
    assert "B" not in ctx.output["routes.txt"]["route_id"].to_list()


def test_remove_rows_regex():
    df = pl.DataFrame({"service_id": ["XLR_1", "XLR_2", "REG_1"]})
    ctx = PipelineContext(output={"calendar.txt": df})

    step = RemoveRows(
        "calendar.txt",
        [MatchCondition("service_id", regex=r"^XLR.*")],
    )
    step.name = "remove_llr"
    step.apply(ctx)

    result = ctx.output["calendar.txt"]
    assert len(result) == 1
    assert result["service_id"][0] == "REG_1"


def test_remove_rows_with_exclude():
    df = pl.DataFrame({"route_id": ["A", "B", "C"]})
    ctx = PipelineContext(output={"routes.txt": df})

    step = RemoveRows(
        "routes.txt",
        [MatchCondition("route_id", value="A")],
        exclude=[[MatchCondition("route_id", value="A")]],
    )
    step.name = "remove_with_exclude"
    step.apply(ctx)

    # Exclude overrides the remove — row A should still be there
    assert len(ctx.output["routes.txt"]) == 3


def test_update_fields():
    df = pl.DataFrame({"route_id": ["A", "B"], "name": ["old_a", "old_b"]})
    ctx = PipelineContext(output={"routes.txt": df})

    step = UpdateFields(
        "routes.txt",
        [MatchCondition("route_id", value="A")],
        {"name": "new_a"},
    )
    step.name = "update_a"
    step.apply(ctx)

    result = ctx.output["routes.txt"]
    assert result.filter(pl.col("route_id") == "A")["name"][0] == "new_a"
    assert result.filter(pl.col("route_id") == "B")["name"][0] == "old_b"


def test_clear_field():
    df = pl.DataFrame({"route_id": ["A", "B"], "block_id": ["x", "y"]})
    ctx = PipelineContext(output={"trips.txt": df})

    step = ClearField(
        "trips.txt",
        "block_id",
        [MatchCondition("route_id", value="A")],
    )
    step.name = "clear_a"
    step.apply(ctx)

    result = ctx.output["trips.txt"]
    assert result.filter(pl.col("route_id") == "A")["block_id"][0] == ""
    assert result.filter(pl.col("route_id") == "B")["block_id"][0] == "y"


def test_update_feed_info():
    df = pl.DataFrame(
        {
            "feed_publisher_name": ["Old"],
            "feed_publisher_url": ["https://old.com"],
        }
    )
    ctx = PipelineContext(output={"feed_info.txt": df})

    step = UpdateFeedInfo(publisher_name="Demo Transit")
    step.name = "update_feed"
    step.apply(ctx)

    assert ctx.output["feed_info.txt"]["feed_publisher_name"][0] == "Demo Transit"


def _apply_transform_trip_id(pattern: str, replacement: str, **files) -> dict:
    ctx = PipelineContext(output=dict(files))
    step = TransformTripId(pattern, replacement)
    step.name = "transform_trip_id"
    step.apply(ctx)
    return ctx.output


def test_transform_trip_id_rewrites_trips_and_stop_times():
    """Every trip_id column in scanned GTFS files is rewritten in lockstep."""
    out = _apply_transform_trip_id(
        r"_LR_",
        r"LR-",
        **{
            "trips.txt": pl.DataFrame(
                {"trip_id": ["100_LR_A", "200_LR_B"], "route_id": ["r1", "r2"]}
            ),
            "stop_times.txt": pl.DataFrame(
                {
                    "trip_id": ["100_LR_A", "100_LR_A", "200_LR_B"],
                    "stop_sequence": [1, 2, 1],
                }
            ),
        },
    )

    assert out["trips.txt"]["trip_id"].to_list() == ["100LR-A", "200LR-B"]
    assert out["stop_times.txt"]["trip_id"].to_list() == [
        "100LR-A",
        "100LR-A",
        "200LR-B",
    ]


def test_transform_trip_id_handles_transfers_from_to():
    """transfers.txt's from_trip_id and to_trip_id are both rewritten."""
    out = _apply_transform_trip_id(
        r"_LR_",
        r"LR-",
        **{
            "transfers.txt": pl.DataFrame(
                {
                    "from_trip_id": ["100_LR_A"],
                    "to_trip_id": ["200_LR_B"],
                    "transfer_type": [1],
                }
            ),
        },
    )

    row = out["transfers.txt"].row(0, named=True)
    assert row["from_trip_id"] == "100LR-A"
    assert row["to_trip_id"] == "200LR-B"


def test_transform_trip_id_ignores_files_without_trip_id_columns():
    """Files that don't carry any trip-id column are left unchanged."""
    routes_before = pl.DataFrame({"route_id": ["R1", "R2"], "name": ["X", "Y"]})
    out = _apply_transform_trip_id(r"_LR_", r"LR-", **{"routes.txt": routes_before})
    assert out["routes.txt"].equals(routes_before)


def test_transform_trip_id_supports_capture_group_replacement():
    """Rust-regex `$1` backref syntax substitutes captured groups."""
    out = _apply_transform_trip_id(
        r"^trip_(\d+)$",
        r"T-$1",
        **{"trips.txt": pl.DataFrame({"trip_id": ["trip_42", "trip_99", "other"]})},
    )
    assert out["trips.txt"]["trip_id"].to_list() == ["T-42", "T-99", "other"]


def test_transform_trip_id_noop_when_pattern_matches_nothing():
    """A pattern that matches no rows leaves every column byte-for-byte identical."""
    before = pl.DataFrame({"trip_id": ["A", "B", "C"]})
    out = _apply_transform_trip_id(r"NEVER_MATCH", r"X", **{"trips.txt": before})
    assert out["trips.txt"]["trip_id"].to_list() == ["A", "B", "C"]


def test_cross_pipeline_shared_module_example_loads():
    """The cross-pipeline trip-id example loads both pipelines
    from a sibling shared module."""
    from continuous_gtfs.scanner import discover_pipelines

    example_root = (
        Path(__file__).parent.parent / "examples" / "cross-pipeline-trip-id"
    )
    if not example_root.is_dir():
        pytest.skip("cross-pipeline-trip-id example not present")

    pipelines = discover_pipelines(example_root)
    by_name = {p.name: p for p in pipelines}
    assert set(by_name) == {"schedule", "realtime"}, (
        "Expected both example pipelines to be discovered; shared.py must be ignored"
    )

    sched_steps = scan_pipeline(by_name["schedule"])
    rt_steps = scan_pipeline(by_name["realtime"])
    sched_rewrite = next(s for s in sched_steps if s.name == "rewrite_trip_id")
    rt_rewrite = next(s for s in rt_steps if s.name == "rewrite_trip_id")

    # Both pipelines pulled (pattern, replacement) from the same `shared.py` constant —
    # so if the shared module changes, both rewrites change together. RT pre-compiles
    # the pattern via re.compile; schedule keeps the raw string for Polars.
    assert sched_rewrite._pattern == r"_LR_"
    assert rt_rewrite._pattern.pattern == r"_LR_"
    assert sched_rewrite._replacement == rt_rewrite._replacement == r"LR-"


def test_transform_trip_id_skips_non_dataframe_output_values():
    """Sentinels or other non-DataFrame entries in ctx.output are left alone."""
    out = _apply_transform_trip_id(
        r"_LR_",
        r"LR-",
        **{
            "trips.txt": pl.DataFrame({"trip_id": ["x_LR_y"]}),
            "_marker": "not a dataframe",
        },
    )
    assert out["trips.txt"]["trip_id"].to_list() == ["xLR-y"]
    assert out["_marker"] == "not a dataframe"


def test_init_schedule_output_seeds_from_named_input():
    """InitScheduleOutput copies ctx.inputs[name] into ctx.output."""
    schedule = {
        "stops.txt": pl.DataFrame({"stop_id": ["S1", "S2"]}),
        "routes.txt": pl.DataFrame({"route_id": ["R1"]}),
    }
    ctx = PipelineContext(inputs={"schedule": schedule})
    step = InitScheduleOutput()  # defaults to input_name="schedule"
    step.name = "init"
    step.apply(ctx)
    assert set(ctx.output.keys()) == {"stops.txt", "routes.txt"}
    assert ctx.output["stops.txt"]["stop_id"].to_list() == ["S1", "S2"]


def test_init_schedule_output_uses_custom_input_name():
    schedule = {"stops.txt": pl.DataFrame({"stop_id": ["S1"]})}
    ctx = PipelineContext(inputs={"vendor_schedule": schedule})
    step = InitScheduleOutput("vendor_schedule")
    step.name = "init"
    step.apply(ctx)
    assert ctx.output["stops.txt"]["stop_id"].to_list() == ["S1"]


def test_init_schedule_output_fails_when_input_missing():
    ctx = PipelineContext(inputs={"other": {"foo.txt": pl.DataFrame({"x": [1]})}})
    step = InitScheduleOutput("schedule")
    step.name = "init"
    with pytest.raises(ValueError, match="input 'schedule' not provided"):
        step.apply(ctx)


def test_init_schedule_output_fails_on_wrong_shape():
    """A csv_table input (single DataFrame, not dict-of-DataFrames) is rejected."""
    ctx = PipelineContext(inputs={"schedule": pl.DataFrame({"x": [1]})})
    step = InitScheduleOutput("schedule")
    step.name = "init"
    with pytest.raises(TypeError, match="not a gtfs_schedule_zip"):
        step.apply(ctx)


def test_init_schedule_output_runs_first_via_before_wildcard():
    """The builtin bakes in before='*' so it runs before any other step."""
    from continuous_gtfs.scanner import resolve_dag

    init = InitScheduleOutput("schedule")
    init.name = "init"
    other = RemoveRows("stops.txt", [MatchCondition("stop_id", value="X")])
    other.name = "other"

    order = resolve_dag([other, init])
    assert [s.name for s in order] == ["init", "other"]


def test_init_schedule_output_ignores_user_supplied_before():
    """Passing `before=[other_step]` can't override the wildcard — safety rail."""
    from continuous_gtfs.scanner import resolve_dag

    other = RemoveRows("stops.txt", [MatchCondition("stop_id", value="X")])
    other.name = "other"
    # User tries to weaken the init guarantee; builtin should still run first
    init = InitScheduleOutput("schedule", before=[other])
    init.name = "init"

    order = resolve_dag([other, init])
    assert [s.name for s in order] == ["init", "other"]
    assert init.before == "*"  # wildcard preserved


def test_remove_rows_missing_file():
    ctx = PipelineContext(output={})
    step = RemoveRows("nonexistent.txt", [MatchCondition("x", value="y")])
    step.name = "noop"
    step.apply(ctx)  # should not raise


def test_remove_rows_missing_column():
    df = pl.DataFrame({"route_id": ["A"]})
    ctx = PipelineContext(output={"routes.txt": df})
    step = RemoveRows("routes.txt", [MatchCondition("nonexistent", value="x")])
    step.name = "noop"
    step.apply(ctx)
    assert len(ctx.output["routes.txt"]) == 1  # unchanged


# --- Condition groups (specs/transform-framework.md §Selecting rows) ---


def _stops_ctx() -> PipelineContext:
    df = pl.DataFrame(
        {
            "stop_id": ["N13", "N13-T1", "N13-T2", "C05"],
            "kind": ["station", "platform", "platform", "station"],
        }
    )
    return PipelineContext(output={"stops.txt": df})


def test_remove_rows_any_of_groups_collapses_repeated_steps():
    # An agency pipeline can burn a separate step per stop id; any_of groups
    # collapse N13 / N13-T1 / N13-T2 into one.
    ctx = _stops_ctx()
    step = RemoveRows(
        "stops.txt",
        [
            [MatchCondition("stop_id", value="N13")],
            [MatchCondition("stop_id", value="N13-T1")],
            [MatchCondition("stop_id", value="N13-T2")],
        ],
    )
    step.name = "remove_n13_family"
    step.apply(ctx)

    assert ctx.output["stops.txt"]["stop_id"].to_list() == ["C05"]
    assert ctx.findings == []


def test_remove_rows_groups_express_and_within_or_across():
    # (kind == platform AND stop_id ~ ^N13) OR (stop_id == C05)
    ctx = _stops_ctx()
    step = RemoveRows(
        "stops.txt",
        [
            [
                MatchCondition("kind", value="platform"),
                MatchCondition("stop_id", regex=r"^N13"),
            ],
            [MatchCondition("stop_id", value="C05")],
        ],
    )
    step.name = "remove_mixed"
    step.apply(ctx)

    # N13 survives: it matches only one condition of the first group.
    assert ctx.output["stops.txt"]["stop_id"].to_list() == ["N13"]


def test_remove_rows_flat_list_is_still_all_of():
    # A flat list ANDs; if it OR'd, N13 (a station) would also go.
    ctx = _stops_ctx()
    step = RemoveRows(
        "stops.txt",
        [
            MatchCondition("kind", value="platform"),
            MatchCondition("stop_id", value="N13-T1"),
        ],
    )
    step.name = "remove_flat"
    step.apply(ctx)

    assert ctx.output["stops.txt"]["stop_id"].to_list() == ["N13", "N13-T2", "C05"]


def test_update_fields_any_of_groups():
    ctx = _stops_ctx()
    step = UpdateFields(
        "stops.txt",
        [
            [MatchCondition("stop_id", value="N13-T1")],
            [MatchCondition("stop_id", value="C05")],
        ],
        {"kind": "renamed"},
    )
    step.name = "update_groups"
    step.apply(ctx)

    result = ctx.output["stops.txt"]
    assert result["kind"].to_list() == ["station", "renamed", "platform", "renamed"]


def test_clear_field_any_of_groups():
    ctx = _stops_ctx()
    step = ClearField(
        "stops.txt",
        "kind",
        [
            [MatchCondition("stop_id", value="N13")],
            [MatchCondition("stop_id", value="N13-T2")],
        ],
    )
    step.name = "clear_groups"
    step.apply(ctx)

    result = ctx.output["stops.txt"]
    assert result["kind"].to_list() == ["", "platform", "", "station"]


def test_single_condition_flat_and_grouped_agree():
    cond = MatchCondition("stop_id", value="N13-T1")

    flat_ctx = _stops_ctx()
    flat = RemoveRows("stops.txt", [cond])
    flat.name = "flat"
    flat.apply(flat_ctx)

    grouped_ctx = _stops_ctx()
    grouped = RemoveRows("stops.txt", [[cond]])
    grouped.name = "grouped"
    grouped.apply(grouped_ctx)

    assert flat_ctx.output["stops.txt"].equals(grouped_ctx.output["stops.txt"])
    # Both actually removed the row — the equality above is not vacuous.
    assert len(flat_ctx.output["stops.txt"]) == 3


def test_remove_rows_groups_respect_exclude():
    ctx = _stops_ctx()
    step = RemoveRows(
        "stops.txt",
        [
            [MatchCondition("stop_id", value="N13")],
            [MatchCondition("stop_id", value="N13-T1")],
        ],
        exclude=[[MatchCondition("stop_id", value="N13-T1")]],
    )
    step.name = "remove_with_exclude_groups"
    step.apply(ctx)

    assert ctx.output["stops.txt"]["stop_id"].to_list() == ["N13-T1", "N13-T2", "C05"]


@pytest.mark.parametrize(
    "make_step",
    [
        pytest.param(lambda conds: RemoveRows("stops.txt", conds), id="RemoveRows"),
        pytest.param(
            lambda conds: UpdateFields("stops.txt", conds, {"kind": "x"}),
            id="UpdateFields",
        ),
        pytest.param(
            lambda conds: ClearField("stops.txt", "kind", conds), id="ClearField"
        ),
    ],
)
def test_missing_column_warns_and_leaves_file_untouched(make_step):
    # Counterfactual first: with the column present the step changes the
    # file and emits nothing, so the assertions below cannot pass vacuously.
    live_ctx = _stops_ctx()
    live = make_step([MatchCondition("stop_id", value="N13")])
    live.name = "live"
    live.apply(live_ctx)
    assert not live_ctx.output["stops.txt"].equals(_stops_ctx().output["stops.txt"])
    assert live_ctx.findings == []

    ctx = _stops_ctx()
    before = ctx.output["stops.txt"].clone()
    step = make_step(
        [
            [MatchCondition("stop_id", value="N13")],
            [MatchCondition("zone_id", value="1")],
        ]
    )
    step.name = "typo"
    step.apply(ctx)

    assert ctx.output["stops.txt"].equals(before)
    assert len(ctx.findings) == 1
    finding = ctx.findings[0]
    assert finding["code"] == "column_missing"
    assert finding["severity"] == "warning"
    assert finding["context"] == {"file": "stops.txt", "column": "zone_id"}
    assert "zone_id" in finding["message"]
    # Declared subject [file, column]: one issue per missing column per file.
    assert step.declared_findings["column_missing"] == ["file", "column"]


def test_missing_column_in_exclude_warns_and_leaves_file_untouched():
    ctx = _stops_ctx()
    before = ctx.output["stops.txt"].clone()
    step = RemoveRows(
        "stops.txt",
        [MatchCondition("stop_id", value="N13")],
        exclude=[[MatchCondition("zone_id", value="1")]],
    )
    step.name = "typo_exclude"
    step.apply(ctx)

    assert ctx.output["stops.txt"].equals(before)
    assert [f["context"]["column"] for f in ctx.findings] == ["zone_id"]


def test_missing_columns_warn_once_each():
    ctx = _stops_ctx()
    step = RemoveRows(
        "stops.txt",
        [
            [
                MatchCondition("zone_id", value="1"),
                MatchCondition("zone_id", value="2"),
            ],
            [MatchCondition("parent_station", value="P")],
        ],
    )
    step.name = "two_typos"
    step.apply(ctx)

    assert [f["context"]["column"] for f in ctx.findings] == [
        "zone_id",
        "parent_station",
    ]


def test_empty_conditions_is_silent_noop():
    ctx = _stops_ctx()
    before = ctx.output["stops.txt"].clone()
    step = RemoveRows("stops.txt", [])
    step.name = "empty"
    step.apply(ctx)

    assert ctx.output["stops.txt"].equals(before)
    assert ctx.findings == []


def test_mixed_condition_shapes_are_rejected_at_construction():
    with pytest.raises(ValueError, match="mix"):
        RemoveRows(
            "stops.txt",
            [
                MatchCondition("stop_id", value="N13"),
                [MatchCondition("kind", value="x")],
            ],  # type: ignore[list-item]
        )
    with pytest.raises(ValueError, match="empty group"):
        UpdateFields("stops.txt", [[MatchCondition("stop_id", value="N13")], []], {})
    with pytest.raises(ValueError, match="exclude"):
        RemoveRows("stops.txt", [MatchCondition("stop_id", value="N13")], exclude=[[]])


# --- SortRows (specs/transform-framework.md §Sort) ---


def _stop_times(rows: str) -> pl.DataFrame:
    return gtfs_df("trip_id,stop_sequence,stop_id\n" + rows)


def test_sort_rows_multi_field_orders_by_first_then_second_key():
    ctx = schedule_context(
        **{"stop_times.txt": _stop_times("T2,2,B\nT1,2,B\nT2,1,A\nT1,1,A\n")}
    )
    SortRows("stop_times.txt", ["trip_id", "stop_sequence"]).apply(ctx)
    out = ctx.output["stop_times.txt"]
    assert out["trip_id"].to_list() == ["T1", "T1", "T2", "T2"]
    assert out["stop_sequence"].to_list() == ["1", "2", "1", "2"]


def test_sort_rows_mixed_direction_is_honored_per_key():
    ctx = schedule_context(
        **{"stop_times.txt": _stop_times("T2,1,A\nT1,1,A\nT2,2,B\nT1,2,B\n")}
    )
    SortRows(
        "stop_times.txt",
        ["trip_id", SortKey("stop_sequence", descending=True)],
    ).apply(ctx)
    out = ctx.output["stop_times.txt"]
    assert out["trip_id"].to_list() == ["T1", "T1", "T2", "T2"]
    assert out["stop_sequence"].to_list() == ["2", "1", "2", "1"]


def test_sort_rows_numeric_key_orders_nine_before_ten_text_does_not():
    rows = "T,10,D\nT,9,C\nT,1,A\nT,2,B\n"
    text_ctx = schedule_context(**{"stop_times.txt": _stop_times(rows)})
    SortRows("stop_times.txt", ["stop_sequence"]).apply(text_ctx)
    assert text_ctx.output["stop_times.txt"]["stop_sequence"].to_list() == [
        "1",
        "10",
        "2",
        "9",
    ]

    num_ctx = schedule_context(**{"stop_times.txt": _stop_times(rows)})
    SortRows("stop_times.txt", [SortKey("stop_sequence", numeric=True)]).apply(num_ctx)
    out = num_ctx.output["stop_times.txt"]
    assert out["stop_sequence"].to_list() == ["1", "2", "9", "10"]
    # Comparison-only cast: the written values are still text, unchanged.
    assert out["stop_sequence"].dtype == pl.Utf8
    assert num_ctx.findings == []


def test_sort_rows_output_is_independent_of_upstream_order_when_keys_tie():
    """Every row shares the sort key; only the full-column tiebreak can order them."""
    a = _stop_times("T,1,C\nT,1,A\nT,1,B\n")
    b = a.reverse()
    assert not a.equals(b)  # the two upstream orders really differ

    ctx_a = schedule_context(**{"stop_times.txt": a})
    ctx_b = schedule_context(**{"stop_times.txt": b})
    step = SortRows("stop_times.txt", ["trip_id"])
    step.apply(ctx_a)
    step.apply(ctx_b)

    out_a, out_b = ctx_a.output["stop_times.txt"], ctx_b.output["stop_times.txt"]
    assert out_a.equals(out_b)
    assert out_a["stop_id"].to_list() == ["A", "B", "C"]
    assert out_a.columns == a.columns  # header order untouched, helpers dropped


def test_sort_rows_empty_and_null_sort_last_in_both_directions():
    df = pl.DataFrame(
        {"stop_id": ["", "b", None, "a"], "stop_name": ["w", "x", "y", "z"]}
    )
    asc = schedule_context(**{"stops.txt": df})
    SortRows("stops.txt", ["stop_id"]).apply(asc)
    assert asc.output["stops.txt"]["stop_name"].to_list()[:2] == ["z", "x"]
    assert set(asc.output["stops.txt"]["stop_id"].to_list()[2:]) == {"", None}

    desc = schedule_context(**{"stops.txt": df})
    SortRows("stops.txt", [SortKey("stop_id", descending=True)]).apply(desc)
    assert desc.output["stops.txt"]["stop_name"].to_list()[:2] == ["x", "z"]
    assert set(desc.output["stops.txt"]["stop_id"].to_list()[2:]) == {"", None}

    # Key-only normalization: the blank cells keep their original form.
    assert sorted(
        asc.output["stops.txt"]["stop_id"].to_list(), key=lambda v: (v is None, v or "")
    ) == ["", "a", "b", None]


def test_sort_rows_missing_field_leaves_file_unsorted_and_warns():
    df = _stop_times("T,2,B\nT,1,A\n")
    ctx = schedule_context(**{"stop_times.txt": df})
    step = SortRows("stop_times.txt", ["trip_id", "nonexistent"])
    step.name = "sort_stop_times"
    step.apply(ctx)

    assert ctx.output["stop_times.txt"].equals(df)  # untouched, not even by trip_id
    assert [f["code"] for f in ctx.findings] == ["column_missing"]
    finding = ctx.findings[0]
    assert finding["severity"] == "warning"
    assert finding["context"] == {"file": "stop_times.txt", "column": "nonexistent"}
    assert step.declared_findings["column_missing"] == ["file", "column"]


def test_sort_rows_unparseable_numeric_values_sort_last_and_are_counted():
    ctx = schedule_context(
        **{"stop_times.txt": _stop_times("T,y,D\nT,3,C\nT,x,B\nT,1,A\n")}
    )
    SortRows("stop_times.txt", [SortKey("stop_sequence", numeric=True)]).apply(ctx)
    out = ctx.output["stop_times.txt"]
    assert out["stop_sequence"].to_list() == ["1", "3", "x", "y"]

    assert [f["code"] for f in ctx.findings] == ["sort_key_not_numeric"]
    finding = ctx.findings[0]
    assert finding["occurrence_count"] == 2
    assert finding["context"]["file"] == "stop_times.txt"
    assert finding["context"]["column"] == "stop_sequence"
    assert finding["emit_errors"] == []


def test_sort_rows_missing_file_is_a_silent_noop():
    ctx = PipelineContext(output={})
    SortRows("nonexistent.txt", ["x"]).apply(ctx)
    assert ctx.output == {}
    assert ctx.findings == []


def test_sort_rows_requires_a_key():
    with pytest.raises(ValueError, match="at least one key"):
        SortRows("stops.txt", [])


def test_sort_rows_runs_after_other_steps_touching_the_file():
    sort = SortRows("stops.txt", ["stop_id"])
    sort.name = "a_sort"  # alphabetically first: only the wildcard can push it last
    other = RemoveRows("stops.txt", [MatchCondition("stop_id", value="X")])
    other.name = "b_remove"

    assert sort.after == "*"
    assert [s.name for s in resolve_dag([sort, other])] == ["b_remove", "a_sort"]


def test_sort_rows_explicit_after_overrides_the_wildcard():
    first = RemoveRows("stops.txt", [MatchCondition("stop_id", value="X")])
    first.name = "b_first"
    later = RemoveRows("stops.txt", [MatchCondition("stop_id", value="Y")])
    later.name = "c_later"
    sort = SortRows("stops.txt", ["stop_id"], after=[first])
    sort.name = "a_sort"

    assert sort.after == [first]
    # Pinned after `first` only; with the wildcard gone it no longer trails `later`.
    assert [s.name for s in resolve_dag([later, sort, first])] == [
        "b_first",
        "a_sort",
        "c_later",
    ]


def test_sort_rows_description_names_the_keys():
    step = SortRows(
        "stop_times.txt",
        ["trip_id", SortKey("stop_sequence", numeric=True, descending=True)],
    )
    expected = "Sort stop_times.txt by trip_id, stop_sequence numeric desc"
    assert step.description == expected
    assert step.files == ["stop_times.txt"]


# --- ID mapping cascade tests ---


def test_id_mappings_cascade_delete():
    routes = pl.DataFrame({"route_id": ["A", "B", "C"]})
    trips = pl.DataFrame({"trip_id": ["t1", "t2", "t3"], "route_id": ["A", "B", "C"]})
    ctx = PipelineContext(output={"routes.txt": routes, "trips.txt": trips})

    # Step 1: remove route B, record mapping
    from continuous_gtfs import step as step_decorator

    @step_decorator(files=["routes.txt"])
    def remove_route(ctx):
        ctx.add_id_mapping("routes.txt", "route_id", "B", None)
        ctx.output["routes.txt"] = ctx.output["routes.txt"].filter(
            pl.col("route_id") != "B"
        )

    @step_decorator(files=["trips.txt"], after=[remove_route])
    def cascade_to_trips(ctx):
        mappings = ctx.get_id_mappings("routes.txt", "route_id")
        df = ctx.output["trips.txt"]
        for old_id, new_id in mappings.items():
            if new_id is None:
                df = df.filter(pl.col("route_id") != old_id)
        ctx.output["trips.txt"] = df

    remove_route.name = "remove_route"
    cascade_to_trips.name = "cascade_to_trips"

    dag = resolve_dag([remove_route, cascade_to_trips])
    execute_pipeline(dag, ctx)

    assert len(ctx.output["routes.txt"]) == 2
    assert len(ctx.output["trips.txt"]) == 2
    assert "B" not in ctx.output["trips.txt"]["route_id"].to_list()


# --- Zip I/O tests ---


def test_extract_and_repackage():
    """Extract a minimal zip, repackage, and verify roundtrip."""
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "agency.txt",
            "agency_name,agency_url,agency_timezone\nDMO,http://demo.example,America/Los_Angeles\n",
        )
        zf.writestr("routes.txt", "route_id,route_type\nA,3\nB,3\n")

    datasets = extract_zip(buf.getvalue())
    assert len(datasets) == 2
    assert len(datasets["routes.txt"]) == 2

    repackaged = package_zip(datasets)
    re_extracted = extract_zip(repackaged)
    assert len(re_extracted) == 2
    assert re_extracted["routes.txt"]["route_id"].to_list() == ["A", "B"]


def test_validate_gtfs_missing_file():
    issues = validate_gtfs(
        {
            "agency.txt": pl.DataFrame(
                {"agency_name": ["x"], "agency_url": ["y"], "agency_timezone": ["z"]}
            )
        }
    )
    assert len(issues["errors"]) > 0
    assert any("Missing required file" in e for e in issues["errors"])


# --- Reference-data inputs on run_schedule_pipeline ---


def _minimal_zip_bytes() -> bytes:
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "agency.txt",
            "agency_name,agency_url,agency_timezone\nDMO,http://demo.example,America/Los_Angeles\n",
        )
        zf.writestr("routes.txt", "route_id,route_type\nA,3\nB,3\n")
        zf.writestr("trips.txt", "route_id,service_id,trip_id\nA,s1,t1\n")
        zf.writestr("stops.txt", "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,0,0\n")
        zf.writestr("stop_times.txt", "trip_id,stop_id,stop_sequence\nt1,S1,1\n")
    return buf.getvalue()


def test_run_schedule_pipeline_exposes_named_inputs():
    """Non-schedule inputs are reachable via ctx.inputs by name."""
    from continuous_gtfs import step

    captured = {}

    @step(files=["stops.txt"])
    def capture_input(ctx):
        captured["overrides"] = ctx.inputs["overrides"]

    capture_input.name = "capture_input"

    overrides = pl.DataFrame({"stop_id": ["S1"], "label": ["hello"]})
    inputs = {
        "schedule": extract_zip(_minimal_zip_bytes()),
        "overrides": overrides,
    }
    result = run_schedule_pipeline(inputs, [capture_input])
    assert result.execution_result.success
    assert captured["overrides"].shape[0] == 1
    assert captured["overrides"]["label"].to_list() == ["hello"]


def test_run_schedule_pipeline_without_extra_inputs_has_only_schedule():
    from continuous_gtfs import step

    captured = {}

    @step(files=["stops.txt"])
    def check_inputs(ctx):
        captured["input_names"] = set(ctx.inputs.keys())

    check_inputs.name = "check_inputs"

    result = run_schedule_pipeline(
        {"schedule": extract_zip(_minimal_zip_bytes())}, [check_inputs]
    )
    assert result.execution_result.success
    assert captured["input_names"] == {"schedule"}


def test_run_schedule_pipeline_missing_input_surfaces_as_keyerror():
    """Missing-input access is a plain KeyError in the failing step."""
    from continuous_gtfs import step

    @step(files=["stops.txt"])
    def needs_input(ctx):
        _ = ctx.inputs["missing_input"]

    needs_input.name = "needs_input"

    result = run_schedule_pipeline(
        {"schedule": extract_zip(_minimal_zip_bytes())}, [needs_input]
    )
    # fail_fast=False in continue mode — step errors but pipeline completes
    step_result = result.execution_result.steps[0]
    assert step_result.status == "error"
    assert "missing_input" in step_result.error


# --- Validation messages in stage metadata ---


def test_validate_stage_metadata_has_full_messages():
    """Validation stages should store full error/warning strings, not just counts."""
    from continuous_gtfs import step

    # Drop required files to force validation errors
    @step(files=[])
    def drop_required(ctx):
        ctx.output.pop("stops.txt", None)
        ctx.output.pop("stop_times.txt", None)

    drop_required.name = "drop_required"

    result = run_schedule_pipeline(
        {"schedule": extract_zip(_minimal_zip_bytes())}, [drop_required]
    )
    output_stage = next(s for s in result.stages if s.name == "validate_output")
    errors = output_stage.metadata["errors"]
    assert isinstance(errors, list)
    assert any("Missing required file: stops.txt" in e for e in errors)


# --- The demo_schedule fixture pipeline on a synthetic feed ---
#
# A hand-built feed with one row per transform target plus untouched
# controls, so every transform assertion runs unconditionally on every
# pytest run.


def _demo_fixture_zip_bytes() -> bytes:
    """A small feed containing one row per demo_schedule transform target."""
    import io
    import zipfile

    files = {
        "agency.txt": (
            "agency_name,agency_url,agency_timezone\n"
            "DMO,http://demo.example,America/Los_Angeles\n"
        ),
        "routes.txt": (
            "route_id,route_type,route_long_name,route_color,route_text_color\n"
            "RED,1,Midtown - Lakeside,AA0033,FFFFFF\n"
            "BLUE,1,Blue Starter Line,000000,000000\n"
            "CR_S,2,Commuter South,,\n"
            "CR_N,2,Commuter North,,\n"
            "BUS9,3,Control Bus,112233,FFFFFF\n"
        ),
        "trips.txt": (
            "route_id,service_id,trip_id,trip_short_name,block_id\n"
            "RED,WKDY,t1,501,blk1\n"
            "BLUE,WKDY,t2,502,blk2\n"
            "CR_S,WKDY,t3,1503,blk3\n"
            "CR_N,WKDY,t5,1505,blk5\n"
            "BUS9,WKDY,t4,504,blk4\n"
        ),
        "stops.txt": (
            "stop_id,stop_name,stop_desc,stop_lat,stop_lon\n"
            "E01,Expansion One,,38.0,-90.0\n"
            "E07,Expansion Seven,,38.1,-90.1\n"
            "455,Old Town,Old Town to Lakeside,38.6,-90.3\n"
            "565,Old Town,Old Town to Midtown,38.6,-90.3\n"
            "C05,Old Town,,38.6,-90.3\n"
            "S1,Control Stop,,38.5,-90.2\n"
        ),
        "stop_times.txt": (
            "trip_id,stop_id,stop_sequence\n"
            "t1,S1,1\nt2,S1,1\nt3,S1,1\nt4,S1,1\nt5,S1,1\n"
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WKDY,1,1,1,1,1,0,0,20260101,20261231\n"
            "DEAD,0,0,0,0,0,0,0,20260101,20261231\n"
            "XLR1,1,1,1,1,1,0,0,20260101,20261231\n"
            "YLINE1,0,0,0,0,0,1,1,20260101,20261231\n"
        ),
        "calendar_dates.txt": (
            "service_id,date,exception_type\n"
            "WKDY,20260704,2\n"
            "XLR1,20260704,1\n"
            "YLINE1,20260704,1\n"
        ),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _demo_fixture_steps():
    """The fixture pipeline exactly as a production worker would load it —
    scan and resolve only, no caller-side injection."""
    fixture_dir = Path(__file__).parent / "fixtures" / "pipelines" / "demo_schedule"
    return resolve_dag(scan_pipeline(fixture_dir))


def test_demo_schedule_fixture_dag_resolves_every_transform():
    """transforms.py declares 16 transforms plus the init step — a step
    silently dropped from the scan should fail here, not inside a skipped
    real-data run."""
    steps = _demo_fixture_steps()
    assert len(steps) == 17
    # The init step's before="*" must actually order it first, or every
    # transform runs against an empty output.
    assert steps[0].name == "init_schedule_output"


def test_demo_schedule_transforms_on_synthetic_feed():
    result = run_schedule_pipeline(
        {"schedule": extract_zip(_demo_fixture_zip_bytes())}, _demo_fixture_steps()
    )
    assert result.execution_result.success

    # Calendar cleanup: dead/XLR/YLINE service rows removed, active kept.
    assert result.output["calendar.txt"]["service_id"].to_list() == ["WKDY"]
    assert result.output["calendar_dates.txt"]["service_id"].to_list() == ["WKDY"]

    # Pre-opening expansion stops removed; every other stop kept.
    stops = result.output["stops.txt"]
    assert stops["stop_id"].to_list() == ["455", "565", "C05", "S1"]

    # Station renames: all three Old Town ids, each keeping its own
    # desc treatment — and the control stop stayed the same.
    by_stop = {r["stop_id"]: r for r in stops.iter_rows(named=True)}
    assert by_stop["455"]["stop_name"] == "Concert Hall"
    assert by_stop["455"]["stop_desc"] == "Concert Hall to Lakeside"
    assert by_stop["565"]["stop_name"] == "Concert Hall"
    assert by_stop["565"]["stop_desc"] == "Concert Hall to Midtown"
    assert by_stop["C05"]["stop_name"] == "Concert Hall"
    assert by_stop["S1"]["stop_name"] == "Control Stop"

    # Route metadata updates, with an untouched control row.
    routes = result.output["routes.txt"]
    by_route = {r["route_id"]: r for r in routes.iter_rows(named=True)}
    assert by_route["RED"]["route_long_name"] == "Northbrook - Lakeside"
    assert by_route["BLUE"]["route_long_name"] == "Easton - Riverview"
    assert by_route["BLUE"]["route_color"] == "0055AA"
    assert by_route["BLUE"]["route_text_color"] == "FFFFFF"
    assert by_route["BUS9"]["route_long_name"] == "Control Bus"
    assert by_route["BUS9"]["route_color"] == "112233"

    # Trip cleanup: short names cleared for light-rail routes only, block_id
    # cleared for BOTH commuter rail routes (separate transforms, each with its
    # own match condition) — each clear leaves the other column alone.
    trips = result.output["trips.txt"]
    by_trip = {r["trip_id"]: r for r in trips.iter_rows(named=True)}
    assert by_trip["t1"]["trip_short_name"] == ""
    assert by_trip["t1"]["block_id"] == "blk1"
    assert by_trip["t2"]["trip_short_name"] == ""
    assert by_trip["t3"]["block_id"] == ""
    assert by_trip["t3"]["trip_short_name"] == "1503"
    assert by_trip["t5"]["block_id"] == ""
    assert by_trip["t5"]["trip_short_name"] == "1505"
    assert by_trip["t4"]["trip_short_name"] == "504"
    assert by_trip["t4"]["block_id"] == "blk4"


def test_schedule_pipeline_packages_valid_zip():
    """package → re-extract → validate, on the packaged BYTES.

    The pipeline already validates its in-memory output as a stage; this is
    the one place the written zip itself is re-read and validated."""
    result = run_schedule_pipeline(
        {"schedule": extract_zip(_demo_fixture_zip_bytes())}, _demo_fixture_steps()
    )
    assert result.execution_result.success
    assert len(result.output_zip) > 0
    issues = validate_gtfs(extract_zip(result.output_zip))
    assert issues["errors"] == []


# --- Quick mode ---


def test_run_schedule_pipeline_quick_skips_validations_and_package():
    from continuous_gtfs import step

    @step(before="*")
    def init_output(ctx):
        ctx.output = dict(ctx.inputs["schedule"])

    init_output.name = "init_output"

    @step(files=["stops.txt"])
    def noop(ctx):
        pass

    noop.name = "noop"

    result = run_schedule_pipeline(
        {"schedule": extract_zip(_minimal_zip_bytes())},
        [init_output, noop],
        quick=True,
    )
    stage_names = [s.name for s in result.stages]

    # --quick drops validate_output and package; transform remains.
    assert stage_names == ["transform"]
    # No output zip produced
    assert result.output_zip == b""


def test_run_schedule_pipeline_quick_still_runs_transforms():
    from continuous_gtfs import step

    captured = {}

    @step(before="*")
    def init_output(ctx):
        ctx.output = dict(ctx.inputs["schedule"])

    init_output.name = "init_output"

    @step(files=["stops.txt"])
    def mark(ctx):
        captured["ran"] = True
        ctx.output["stops.txt"] = ctx.output["stops.txt"].head(0)

    mark.name = "mark"

    result = run_schedule_pipeline(
        {"schedule": extract_zip(_minimal_zip_bytes())},
        [init_output, mark],
        quick=True,
    )
    assert captured["ran"] is True
    assert result.execution_result.success
    assert result.output["stops.txt"].height == 0


def test_package_zip_uncompressed(tmp_path):
    """compress=False produces a valid zip readable by zipfile (ZIP_STORED)."""
    import zipfile

    from continuous_gtfs.pipelines.schedule import package_zip

    datasets = {
        "agency.txt": pl.DataFrame(
            {"agency_name": ["A"], "agency_url": ["u"], "agency_timezone": ["UTC"]}
        ),
        "routes.txt": pl.DataFrame({"route_id": ["R1"], "route_type": ["3"]}),
    }

    compressed = package_zip(datasets, compress=True)
    uncompressed = package_zip(datasets, compress=False)

    # Both should be valid zips
    with zipfile.ZipFile(io.BytesIO(compressed)) as zf:
        assert set(zf.namelist()) == {"agency.txt", "routes.txt"}
        assert zf.getinfo("agency.txt").compress_type == zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(io.BytesIO(uncompressed)) as zf:
        assert set(zf.namelist()) == {"agency.txt", "routes.txt"}
        assert zf.getinfo("agency.txt").compress_type == zipfile.ZIP_STORED
