"""Data-driven schedule transform builtins.

Each builtin is a Step subclass with parameters. All use Polars for
DataFrame operations. Transforms are self-contained — each performs all
its modifications during its own lifecycle.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import polars as pl

from ...step import Step

if TYPE_CHECKING:
    from ...context import PipelineContext


@dataclass
class MatchCondition:
    """One test on one field: an exact `value` or a `regex` pattern.

    Conditions select the rows a row-operation builtin acts on. They
    combine as groups: a flat list is one all-of group; a list of lists is
    any-of across groups. See the [builtins guide](../guide/builtins.md).

    Args:
        field: The column the test reads.
        value: Match rows whose field equals this string exactly.
        regex: Match rows whose field contains a match for this pattern
            (Polars `str.contains`, Rust regex syntax; anchor with `^`
            and `$` to match the whole value). Give `value` or `regex`,
            not both.

    Examples:
        ```python
        MatchCondition("route_id", value="R2")
        MatchCondition("service_id", regex=r"^RAIL")
        ```
    """

    field: str
    value: str | None = None
    regex: str | None = None

    def filter_expr(self) -> pl.Expr:
        if self.regex is not None:
            return pl.col(self.field).cast(pl.Utf8).str.contains(self.regex)
        if self.value is not None:
            return pl.col(self.field).cast(pl.Utf8) == self.value
        raise ValueError("MatchCondition needs value or regex")


#: Finding emitted when a step references a column the file lacks
#: (specs/transform-framework.md §Selecting rows). Shared across schedule
#: builtins so an operator sees one issue per missing column per file.
COLUMN_MISSING = "column_missing"
_COLUMN_MISSING_DECL = (
    COLUMN_MISSING,
    {
        "subject": ["file", "column"],
        "description": (
            "A condition or sort key named a column the file lacks; "
            "the step left the file untouched."
        ),
    },
)


def _normalize_groups(
    conditions: list[MatchCondition] | list[list[MatchCondition]],
    *,
    param: str = "conditions",
) -> list[list[MatchCondition]]:
    """Coerce a flat or grouped conditions list into groups.

    A flat list becomes one group; a list of lists is taken as-is. Mixing
    the two shapes or passing an empty group is a configuration error and
    raises at construction so a misconfigured step fails at scan time
    rather than silently selecting nothing.
    """
    if not conditions:
        return []
    if all(isinstance(c, MatchCondition) for c in conditions):
        return [list(conditions)]  # type: ignore[arg-type]
    if not all(isinstance(g, list) for g in conditions):
        raise ValueError(
            f"{param} must be a list of MatchCondition or a list of "
            "MatchCondition groups, not a mix"
        )
    groups: list[list[MatchCondition]] = []
    for group in conditions:
        if not group:
            raise ValueError(f"{param} contains an empty group")
        if not all(isinstance(c, MatchCondition) for c in group):
            raise ValueError(f"{param} group must contain only MatchCondition")
        groups.append(list(group))  # type: ignore[arg-type]
    return groups


def _missing_fields(df: pl.DataFrame, groups: list[list[MatchCondition]]) -> list[str]:
    """Columns referenced by any condition that the frame lacks, in first-seen order."""
    seen: dict[str, None] = {}
    for group in groups:
        for cond in group:
            if cond.field not in df.columns:
                seen.setdefault(cond.field, None)
    return list(seen)


def _warn_missing_columns(ctx: PipelineContext, file: str, fields: list[str]) -> None:
    """Emit one column_missing warning per absent column."""
    for field_name in fields:
        ctx.emit_finding(
            COLUMN_MISSING,
            severity="warning",
            message=(
                f"{file} has no column {field_name!r}; the step left the file untouched"
            ),
            context={"file": file, "column": field_name},
        )


def _build_group_mask(
    df: pl.DataFrame, groups: list[list[MatchCondition]]
) -> pl.Series:
    """Boolean mask: rows matching ANY group, where a group matches when ALL
    its conditions hold. Caller guarantees every referenced column exists and
    `groups` is non-empty."""
    group_exprs = []
    for group in groups:
        expr = group[0].filter_expr()
        for cond in group[1:]:
            expr = expr & cond.filter_expr()
        group_exprs.append(expr)
    combined = group_exprs[0]
    for expr in group_exprs[1:]:
        combined = combined | expr
    return df.select(combined).to_series()


class RemoveRows(Step):
    """Remove the rows of one file that match conditions.

    Mechanical: one file, one mask, no knowledge of what the rows mean. To
    remove a route, trip, stop, or service with everything that belongs to
    it, use a semantic removal instead.

    Args:
        file: The GTFS file to edit, such as `"routes.txt"`.
        conditions: Which rows to remove: a flat list of
            [`MatchCondition`][continuous_gtfs.builtins.schedule.MatchCondition]
            (all must hold) or a list of lists (any group may hold).
        exclude: Groups in the same shape; a row matching any of them is
            protected from removal.
        description: Label shown in the Console. Defaults to
            `Remove rows from <file>`.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Note:
        A file absent from the output, or an empty `conditions` list, is a
        silent no-op. A condition naming a column the file lacks leaves the
        file untouched and emits `column_missing`, one per missing column.

    Examples:
        ```python
        remove_route = RemoveRows(
            "routes.txt",
            [MatchCondition("route_id", value="OLD_ROUTE")],
            description="Remove deprecated route",
        )

        # Any-of: three stops in one step instead of three steps
        remove_station_family = RemoveRows(
            "stops.txt",
            [
                [MatchCondition("stop_id", value="S100")],
                [MatchCondition("stop_id", value="S100-T1")],
                [MatchCondition("stop_id", value="S100-T2")],
            ],
        )

        # Every RAIL service except the Spring 2026 one
        remove_rail = RemoveRows(
            "calendar.txt",
            [MatchCondition("service_id", regex=r"^RAIL")],
            exclude=[[MatchCondition("service_id", value="RAIL_SP26")]],
        )
        ```
    """

    category = "Row operations"

    findings = [_COLUMN_MISSING_DECL]

    def __init__(
        self,
        file: str,
        conditions: list[MatchCondition] | list[list[MatchCondition]],
        *,
        exclude: list[list[MatchCondition]] | None = None,
        description: str = "",
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.files = [file]
        self.description = description or f"Remove rows from {file}"
        self._file = file
        self._groups = _normalize_groups(conditions)
        self._exclude = _normalize_groups(exclude or [], param="exclude")

    def apply(self, ctx: PipelineContext) -> None:
        df = ctx.output.get(self._file)
        if df is None or not self._groups:
            return

        missing = _missing_fields(df, self._groups + self._exclude)
        if missing:
            _warn_missing_columns(ctx, self._file, missing)
            return
        match_mask = _build_group_mask(df, self._groups)

        if self._exclude:
            match_mask = match_mask & ~_build_group_mask(df, self._exclude)

        ctx.output[self._file] = df.filter(~match_mask)


class UpdateFields(Step):
    """Set field values on the rows of one file that match conditions.

    Args:
        file: The GTFS file to edit.
        conditions: Which rows to update, in the
            [`RemoveRows`][continuous_gtfs.builtins.schedule.RemoveRows]
            group shape.
        updates: Column name to new value. A column the file does not have
            is ignored.
        description: Label shown in the Console. Defaults to
            `Update fields in <file>`.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Note:
        A file absent from the output, or an empty `conditions` list, is a
        silent no-op. A condition naming a column the file lacks leaves the
        file untouched and emits `column_missing`, one per missing column.

    Examples:
        ```python
        update_route = UpdateFields(
            "routes.txt",
            [MatchCondition("route_id", value="R1")],
            {
                "route_long_name": "Northside - Downtown",
                "route_color": "007CAD",
                "route_text_color": "FFFFFF",
            },
            description="Update route R1 metadata",
        )
        ```
    """

    category = "Row operations"

    findings = [_COLUMN_MISSING_DECL]

    def __init__(
        self,
        file: str,
        conditions: list[MatchCondition] | list[list[MatchCondition]],
        updates: dict[str, str],
        *,
        description: str = "",
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.files = [file]
        self.description = description or f"Update fields in {file}"
        self._file = file
        self._groups = _normalize_groups(conditions)
        self._updates = updates

    def apply(self, ctx: PipelineContext) -> None:
        df = ctx.output.get(self._file)
        if df is None or not self._groups:
            return

        missing = _missing_fields(df, self._groups)
        if missing:
            _warn_missing_columns(ctx, self._file, missing)
            return
        match_mask = _build_group_mask(df, self._groups)

        for col, val in self._updates.items():
            if col in df.columns:
                df = df.with_columns(
                    pl.when(match_mask)
                    .then(pl.lit(val))
                    .otherwise(pl.col(col))
                    .alias(col)
                )

        ctx.output[self._file] = df


class ClearField(Step):
    """Set one field to the empty string on the rows that match conditions.

    Args:
        file: The GTFS file to edit.
        field: The column to clear. A column the file does not have is
            ignored.
        conditions: Which rows to clear, in the
            [`RemoveRows`][continuous_gtfs.builtins.schedule.RemoveRows]
            group shape.
        description: Label shown in the Console. Defaults to
            `Clear <field> in <file>`.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Note:
        A file absent from the output, or an empty `conditions` list, is a
        silent no-op. A condition naming a column the file lacks leaves the
        file untouched and emits `column_missing`, one per missing column.

    Examples:
        ```python
        clear_blocks = ClearField(
            "trips.txt",
            "block_id",
            [MatchCondition("route_id", value="COMMUTER_RAIL")],
            description="Clear block_id for the commuter rail route",
        )
        ```
    """

    category = "Row operations"

    findings = [_COLUMN_MISSING_DECL]

    def __init__(
        self,
        file: str,
        field: str,
        conditions: list[MatchCondition] | list[list[MatchCondition]],
        *,
        description: str = "",
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.files = [file]
        self.description = description or f"Clear {field} in {file}"
        self._file = file
        self._field = field
        self._groups = _normalize_groups(conditions)

    def apply(self, ctx: PipelineContext) -> None:
        df = ctx.output.get(self._file)
        if df is None or not self._groups:
            return

        missing = _missing_fields(df, self._groups)
        if missing:
            _warn_missing_columns(ctx, self._file, missing)
            return
        match_mask = _build_group_mask(df, self._groups)

        if self._field in df.columns:
            ctx.output[self._file] = df.with_columns(
                pl.when(match_mask)
                .then(pl.lit(""))
                .otherwise(pl.col(self._field))
                .alias(self._field)
            )


class UpdateFeedInfo(Step):
    """Set publisher fields on every row of `feed_info.txt`.

    Args:
        publisher_name: New `feed_publisher_name`.
        publisher_url: New `feed_publisher_url`.
        feed_lang: New `feed_lang`.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Note:
        Only the fields given are written; a column the file lacks is
        ignored, and a feed with no `feed_info.txt` is a silent no-op.

    Examples:
        ```python
        update_feed = UpdateFeedInfo(
            publisher_name="Example Transit",
            publisher_url="https://transit.example.org",
            feed_lang="en",
        )
        ```
    """

    category = "Feed metadata"

    files = ["feed_info.txt"]

    def __init__(
        self,
        *,
        publisher_name: str | None = None,
        publisher_url: str | None = None,
        feed_lang: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.description = "Update feed publisher info"
        self._updates: dict[str, str] = {}
        if publisher_name is not None:
            self._updates["feed_publisher_name"] = publisher_name
        if publisher_url is not None:
            self._updates["feed_publisher_url"] = publisher_url
        if feed_lang is not None:
            self._updates["feed_lang"] = feed_lang

    def apply(self, ctx: PipelineContext) -> None:
        df = ctx.output.get("feed_info.txt")
        if df is None:
            return
        for col, val in self._updates.items():
            if col in df.columns:
                df = df.with_columns(pl.lit(val).alias(col))
        ctx.output["feed_info.txt"] = df


class TransformTripId(Step):
    """Rewrite every trip-id column across the schedule with a regex substitution.

    Operates on the three column names that carry trip references:
    `trip_id` (`trips.txt`, `stop_times.txt`, `frequencies.txt`,
    `attributions.txt`) and `from_trip_id` / `to_trip_id`
    (`transfers.txt`). Any other column is left untouched.

    Mirrors the realtime
    [`TransformTripId`][continuous_gtfs.builtins.realtime.TransformTripId]:
    both take the same `(pattern, replacement)` pair so an agency can
    rewrite trip ids consistently across schedule and realtime outputs
    (specs/configuration.md §Cross-pipeline shared modules).

    Args:
        pattern: The regex to find, compiled by Polars (Rust `regex`
            syntax).
        replacement: The replacement text; capture groups are `$1`, `$2`.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Note:
        The realtime counterpart uses Python's `re`, where capture
        references are `\\1`. For a pattern shared between the two
        pipelines, stay within the common subset (character classes,
        alternation, anchors, quantifiers, `()` groups) and keep two
        replacement strings, or avoid backreferences.

    Examples:
        ```python
        shorten_trip_ids = TransformTripId(
            pattern=r"^LONG_PREFIX_(\\d+)$",
            replacement=r"T-$1",
        )
        ```
    """

    category = "Identifiers"

    TRIP_ID_COLUMNS = ("trip_id", "from_trip_id", "to_trip_id")

    def __init__(self, pattern: str, replacement: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.description = f"Transform trip_id: s/{pattern}/{replacement}/"
        self._pattern = pattern
        self._replacement = replacement

    def apply(self, ctx: PipelineContext) -> None:
        for filename in list(ctx.output.keys()):
            df = ctx.output[filename]
            if not isinstance(df, pl.DataFrame):
                continue
            cols_present = [c for c in self.TRIP_ID_COLUMNS if c in df.columns]
            if not cols_present:
                continue
            ctx.output[filename] = df.with_columns(
                [
                    pl.col(c)
                    .cast(pl.Utf8)
                    .str.replace_all(self._pattern, self._replacement)
                    .alias(c)
                    for c in cols_present
                ]
            )


class InitScheduleOutput(Step):
    """Seed `ctx.output` from a schedule input before any other step runs.

    `ctx.output` starts empty; this step copies the named
    `gtfs_schedule_zip` input into it so transforms have something to
    mutate. `ctx.inputs[input_name]` stays as it was, so a step that wants
    the pristine schedule (a supplemental merge, a diff) can still read it.
    One of these replaces the `@step(before="*")` boilerplate every schedule
    pipeline would otherwise carry.

    Args:
        input_name: The input to seed from, as declared in the pipeline's
            `INPUTS` manifest.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step]. A `before=` argument is ignored:
            this step always runs first.

    Note:
        Fails the run with a clear error when the named input was not
        supplied, or is not a `gtfs_schedule_zip` (a mapping of file name
        to DataFrame).

    Examples:
        ```python
        # pipelines/schedule/init.py
        from continuous_gtfs.builtins.schedule import InitScheduleOutput

        init = InitScheduleOutput("schedule")
        ```
    """

    category = "Init"

    def __init__(self, input_name: str = "schedule", **kwargs: Any):
        # before="*" is the whole point of this builtin — silently
        # ignore any override so `InitScheduleOutput(before=[x])` can't
        # accidentally defeat the init-step guarantee.
        kwargs.pop("before", None)
        super().__init__(before="*", **kwargs)
        self._input_name = input_name
        self.description = f"Seed ctx.output from ctx.inputs[{input_name!r}]"

    def apply(self, ctx: PipelineContext) -> None:
        from collections.abc import Mapping

        if self._input_name not in ctx.inputs:
            raise ValueError(
                f"{type(self).__name__}: input {self._input_name!r} not "
                f"provided. Declared inputs: {sorted(ctx.inputs.keys())}"
            )
        value = ctx.inputs[self._input_name]
        # `Mapping` rather than `dict` — gtfs_schedule_zip inputs are
        # wrapped in MappingProxyType for immutability. `dict(value)`
        # below normalizes back to a plain mutable dict for ctx.output.
        if not isinstance(value, Mapping) or not all(
            isinstance(v, pl.DataFrame) for v in value.values()
        ):
            raise TypeError(
                f"{type(self).__name__}: input {self._input_name!r} is not "
                f"a gtfs_schedule_zip (expected dict of polars.DataFrame, "
                f"got {type(value).__name__})."
            )
        ctx.output = dict(value)


@dataclass
class SortKey:
    """One `SortRows` key: which field, which direction, how values compare.

    Args:
        field: The column to sort by.
        descending: Sort this key high to low.
        numeric: Compare the values as numbers (`9` before `10`) instead of
            as text (`10` before `9`). The cast is for comparison only; the
            written values are never touched. A value that does not parse
            sorts last and is counted in a `sort_key_not_numeric` finding.

    Examples:
        ```python
        SortKey("stop_sequence", numeric=True)
        SortKey("start_date", descending=True)
        ```
    """

    field: str
    descending: bool = False
    numeric: bool = False


def _sortable_text(field: str) -> pl.Expr:
    """The column as text with "" mapped to null — key-only normalization.

    Empty string and null are one GTFS value; treating them alike (and
    letting `nulls_last` place both) keeps the sort order a function of
    content rather than of which upstream step produced the blank.
    """
    col = pl.col(field).cast(pl.Utf8)
    return (
        pl.when(col.str.len_chars() == 0)
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(col)
    )


class SortRows(Step):
    """Order a file's rows so their order is a pure function of content.

    Sorts by the given keys, then breaks every remaining tie by every
    column of the file in header order (ascending, as text). Two runs
    whose upstream steps hand the file over in different row orders
    therefore write byte-identical files, which is what keeps content-hash
    output versions stable. Use it on any file a merge or dedup step
    leaves in an order that varies between runs.

    Args:
        file: The GTFS file to reorder.
        by: The sort keys, in precedence order: a
            [`SortKey`][continuous_gtfs.builtins.schedule.SortKey], or a
            bare column name as shorthand for an ascending text key.
        description: Label shown in the Console. Defaults to
            `Sort <file> by <keys>`.
        **kwargs: Shared step parameters (`after`, `before`, `priority`,
            `enabled`, `tags`, `data_owner`); see
            [`Step`][continuous_gtfs.step.Step].

    Note:
        Defaults to `after="*"` so it runs after every other step that
        touches the file; an explicit `after=[...]` is honored. Empty string
        and null are one value and sort last in either direction. Header
        order is untouched. A missing file is a silent no-op; a missing sort
        field leaves the file unsorted and emits `column_missing`.

    Examples:
        ```python
        sort_stop_times = SortRows(
            "stop_times.txt",
            ["trip_id", SortKey("stop_sequence", numeric=True)],
        )
        ```
    """

    category = "Ordering"

    findings = [
        _COLUMN_MISSING_DECL,
        (
            "sort_key_not_numeric",
            {
                "subject": ["file", "column"],
                "description": (
                    "Values under a `numeric` sort key did not parse as "
                    "numbers; they sorted last. The count is the number "
                    "of such values."
                ),
            },
        ),
    ]

    def __init__(
        self,
        file: str,
        by: Sequence[SortKey | str],
        *,
        description: str = "",
        **kwargs: Any,
    ):
        if kwargs.get("after") is None:
            kwargs["after"] = "*"
        super().__init__(**kwargs)
        keys = [SortKey(k) if isinstance(k, str) else k for k in by]
        if not keys:
            raise ValueError(f"{type(self).__name__}: `by` needs at least one key")
        self.files = [file]
        self._file = file
        self._keys = keys
        self.description = description or (
            f"Sort {file} by " + ", ".join(self._describe(k) for k in keys)
        )

    @staticmethod
    def _describe(key: SortKey) -> str:
        parts = [key.field]
        if key.numeric:
            parts.append("numeric")
        if key.descending:
            parts.append("desc")
        return " ".join(parts)

    def apply(self, ctx: PipelineContext) -> None:
        df = ctx.output.get(self._file)
        if df is None:
            return

        missing = [k.field for k in self._keys if k.field not in df.columns]
        if missing:
            for column in missing:
                ctx.emit_finding(
                    COLUMN_MISSING,
                    severity="warning",
                    message=f"{self._file} has no column {column!r}; left unsorted",
                    context={"file": self._file, "column": column},
                )
            return

        exprs: list[pl.Expr] = []
        names: list[str] = []
        descending: list[bool] = []

        for i, key in enumerate(self._keys):
            text = _sortable_text(key.field)
            expr = text
            if key.numeric:
                expr = text.cast(pl.Float64, strict=False)
                unparseable = df.select(
                    (text.is_not_null() & expr.is_null()).sum()
                ).item()
                if unparseable:
                    ctx.emit_finding(
                        "sort_key_not_numeric",
                        severity="warning",
                        message=(
                            f"{unparseable} value(s) of {key.field} in "
                            f"{self._file} are not numeric; sorted last"
                        ),
                        context={
                            "file": self._file,
                            "column": key.field,
                            "count": str(unparseable),
                        },
                        occurrence_count=unparseable,
                    )
            name = f"__sort_key_{i}"
            exprs.append(expr.alias(name))
            names.append(name)
            descending.append(key.descending)

        # Full-column tiebreak: every column in header order, ascending as
        # text. Key columns reappear here so numeric keys that tie across
        # spellings ("9" vs "09") still resolve deterministically.
        for i, column in enumerate(df.columns):
            name = f"__sort_tie_{i}"
            exprs.append(_sortable_text(column).alias(name))
            names.append(name)
            descending.append(False)

        ctx.output[self._file] = (
            df.with_columns(exprs)
            .sort(names, descending=descending, nulls_last=True, maintain_order=True)
            .drop(names)
        )
