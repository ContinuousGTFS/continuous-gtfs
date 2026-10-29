"""Schedule transform builtins — parameterized Steps for GTFS schedule data."""

from .removals import (
    RemoveRoutes,
    RemoveServices,
    RemoveStops,
    RemoveTrips,
)
from .transforms import (
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

#: Catalog sections in display order (specs/transform-framework.md §The
#: builtin catalog). Every builtin here names one as its `category`.
CATEGORIES = (
    "Init",
    "Row operations",
    "Semantic removals",
    "Ordering",
    "Identifiers",
    "Feed metadata",
)

__all__ = [
    "ClearField",
    "InitScheduleOutput",
    "MatchCondition",
    "RemoveRoutes",
    "RemoveRows",
    "RemoveServices",
    "RemoveStops",
    "RemoveTrips",
    "SortKey",
    "SortRows",
    "TransformTripId",
    "UpdateFeedInfo",
    "UpdateFields",
]
