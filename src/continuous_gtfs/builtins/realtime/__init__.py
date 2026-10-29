"""Realtime transform builtins — parameterized Steps for GTFS-RT protobuf data."""

from .transforms import (
    CombineFeeds,
    ConvertScheduledToNew,
    ExpireCancelledTrips,
    FilterStopsByID,
    InsertMissingCancellations,
    PassThrough,
    RenameVehicles,
    TransformTripId,
    UpdateFeedHeader,
)

#: Catalog sections in display order (specs/transform-framework.md §The
#: builtin catalog). Every builtin here names one as its `category`.
CATEGORIES = (
    "Filtering",
    "Merging",
    "Identifiers",
    "Schedule-aware",
    "Feed header",
    "Testing",
)

__all__ = [
    "PassThrough",
    "FilterStopsByID",
    "CombineFeeds",
    "RenameVehicles",
    "UpdateFeedHeader",
    "TransformTripId",
    "ExpireCancelledTrips",
    "InsertMissingCancellations",
    "ConvertScheduledToNew",
]
