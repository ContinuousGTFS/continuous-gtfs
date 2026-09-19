"""Core comparison logic for GTFS-RT feeds."""

from __future__ import annotations

from google.protobuf import descriptor as _descriptor
from google.transit import gtfs_realtime_pb2 as gtfs_rt

from .config import ComparisonConfig
from .match import (
    match_active_periods,
    match_entities,
    match_informed_entities,
    match_stop_time_updates,
)
from .report import (
    ComparisonReport,
    EntityDifference,
    EntitySummary,
    HeaderComparison,
)


def _get_entity_timestamp(entity: gtfs_rt.FeedEntity) -> int | None:
    """Extract the entity-level timestamp from a FeedEntity."""
    if entity.HasField("vehicle") and entity.vehicle.timestamp:
        return entity.vehicle.timestamp
    if entity.HasField("trip_update") and entity.trip_update.timestamp:
        return entity.trip_update.timestamp
    # Alerts don't have entity-level timestamps
    return None


def _is_entity_stale(
    entity: gtfs_rt.FeedEntity,
    header_timestamp: int,
    stale_threshold_seconds: int,
) -> bool:
    """Check if an entity is stale relative to the feed header timestamp."""
    if stale_threshold_seconds <= 0:
        return False
    entity_ts = _get_entity_timestamp(entity)
    if entity_ts is None:
        return False
    return (header_timestamp - entity_ts) > stale_threshold_seconds


def _detect_feed_type(feed: gtfs_rt.FeedMessage) -> str:
    """Detect the feed type from the first entity."""
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            return "trip_updates"
        if entity.HasField("vehicle"):
            return "vehicle_positions"
        if entity.HasField("alert"):
            return "alerts"
    return "unknown"


def _is_timestamp_field(field_desc) -> bool:
    """Check if a field is a timestamp field by name convention."""
    return field_desc.name == "timestamp"


def _is_position_field(field_desc) -> bool:
    """Check if a field is a geographic position message."""
    return (
        field_desc.message_type is not None
        and field_desc.message_type.name == "Position"
    )


def _approx_equal(a: float, b: float, tolerance: float) -> bool:
    return abs(a - b) <= tolerance


def _round_coord(value: float, decimal_places: int) -> float:
    return round(value, decimal_places)


def _compare_position(pos_a, pos_b, config: ComparisonConfig) -> list[dict]:
    """Compare two Position messages with tolerance."""
    diffs = []

    lat_a = _round_coord(pos_a.latitude, config.position_decimal_places)
    lat_b = _round_coord(pos_b.latitude, config.position_decimal_places)
    if lat_a != lat_b:
        diffs.append(
            {
                "field": "position.latitude",
                "a": pos_a.latitude,
                "b": pos_b.latitude,
            }
        )

    lon_a = _round_coord(pos_a.longitude, config.position_decimal_places)
    lon_b = _round_coord(pos_b.longitude, config.position_decimal_places)
    if lon_a != lon_b:
        diffs.append(
            {
                "field": "position.longitude",
                "a": pos_a.longitude,
                "b": pos_b.longitude,
            }
        )

    if not _approx_equal(pos_a.bearing, pos_b.bearing, config.bearing_tolerance):
        diffs.append(
            {
                "field": "position.bearing",
                "a": pos_a.bearing,
                "b": pos_b.bearing,
            }
        )

    if not _approx_equal(pos_a.speed, pos_b.speed, config.speed_tolerance):
        diffs.append(
            {
                "field": "position.speed",
                "a": pos_a.speed,
                "b": pos_b.speed,
            }
        )

    return diffs


def _get_default_value(field_desc):
    """Get the default value for a protobuf field."""
    if field_desc.cpp_type == _descriptor.FieldDescriptor.CPPTYPE_STRING:
        return ""
    if (
        field_desc.cpp_type == _descriptor.FieldDescriptor.CPPTYPE_STRING
        and field_desc.name.endswith("_bytes")
    ):
        return b""
    if field_desc.cpp_type == _descriptor.FieldDescriptor.CPPTYPE_BOOL:
        return False
    if field_desc.type in (
        _descriptor.FieldDescriptor.TYPE_FLOAT,
        _descriptor.FieldDescriptor.TYPE_DOUBLE,
    ):
        return 0.0
    if field_desc.cpp_type == _descriptor.FieldDescriptor.CPPTYPE_ENUM:
        return 0
    if field_desc.type in (
        _descriptor.FieldDescriptor.TYPE_INT32,
        _descriptor.FieldDescriptor.TYPE_INT64,
        _descriptor.FieldDescriptor.TYPE_UINT32,
        _descriptor.FieldDescriptor.TYPE_UINT64,
        _descriptor.FieldDescriptor.TYPE_SINT32,
        _descriptor.FieldDescriptor.TYPE_SINT64,
        _descriptor.FieldDescriptor.TYPE_FIXED32,
        _descriptor.FieldDescriptor.TYPE_FIXED64,
        _descriptor.FieldDescriptor.TYPE_SFIXED32,
        _descriptor.FieldDescriptor.TYPE_SFIXED64,
    ):
        return 0
    return None


def _get_field_value_with_default(msg, field_desc):
    """Get a field value, treating absent fields as their default value."""
    # For message-type fields, check HasField
    if field_desc.message_type is not None:
        if field_desc.is_repeated:
            return list(getattr(msg, field_desc.name))
        if msg.HasField(field_desc.name):
            return getattr(msg, field_desc.name)
        return None
    # For scalar fields, just get the value (protobuf returns defaults)
    return getattr(msg, field_desc.name)


def _compare_messages_semantic(
    msg_a,
    msg_b,
    config: ComparisonConfig,
    path: str = "",
    skip_timestamps: bool = False,
) -> tuple[list[dict], bool]:
    """Compare two protobuf messages semantically.

    Returns:
        (field_diffs, timestamp_only) - list of differences, and whether
        only timestamp fields differ.
    """
    diffs = []
    has_non_timestamp_diff = False

    if msg_a is None and msg_b is None:
        return [], False

    # Both should be the same message type
    if msg_a is None or msg_b is None:
        # One is absent - check if the present one is all defaults
        present = msg_a if msg_b is None else msg_b
        label = "a" if msg_b is None else "b"
        # Check if the present message has any non-default fields set
        if present.ListFields():
            diffs.append(
                {
                    "field": path or "message",
                    "detail": f"present only in feed {'A' if label == 'a' else 'B'}",
                }
            )
            has_non_timestamp_diff = True
        return diffs, not has_non_timestamp_diff

    descriptor = msg_a.DESCRIPTOR

    for field_desc in descriptor.fields:
        field_path = f"{path}.{field_desc.name}" if path else field_desc.name

        # Handle repeated message fields specially
        if field_desc.is_repeated and field_desc.message_type is not None:
            items_a = list(getattr(msg_a, field_desc.name))
            items_b = list(getattr(msg_b, field_desc.name))

            # Special handling for stop_time_update
            if field_desc.name == "stop_time_update":
                matched, added, removed = match_stop_time_updates(items_a, items_b)
                if added or removed:
                    diffs.append(
                        {
                            "field": field_path,
                            "detail": f"{len(added)} added, {len(removed)} removed",
                        }
                    )
                    has_non_timestamp_diff = True
                for stu_a, stu_b in matched:
                    sub_diffs, sub_ts_only = _compare_messages_semantic(
                        stu_a, stu_b, config, field_path, skip_timestamps
                    )
                    diffs.extend(sub_diffs)
                    if sub_diffs and not sub_ts_only:
                        has_non_timestamp_diff = True
                continue

            # Special handling for informed_entity
            if field_desc.name == "informed_entity":
                matched, added, removed = match_informed_entities(items_a, items_b)
                if added or removed:
                    diffs.append(
                        {
                            "field": field_path,
                            "detail": f"{len(added)} added, {len(removed)} removed",
                        }
                    )
                    has_non_timestamp_diff = True
                for ie_a, ie_b in matched:
                    sub_diffs, sub_ts_only = _compare_messages_semantic(
                        ie_a, ie_b, config, field_path, skip_timestamps
                    )
                    diffs.extend(sub_diffs)
                    if sub_diffs and not sub_ts_only:
                        has_non_timestamp_diff = True
                continue

            # Special handling for active_period
            if field_desc.name == "active_period":
                matched, added, removed = match_active_periods(items_a, items_b)
                if added or removed:
                    diffs.append(
                        {
                            "field": field_path,
                            "detail": f"{len(added)} added, {len(removed)} removed",
                        }
                    )
                    has_non_timestamp_diff = True
                continue

            # Generic repeated message: compare by index (order matters)
            if len(items_a) != len(items_b):
                diffs.append(
                    {
                        "field": field_path,
                        "a_count": len(items_a),
                        "b_count": len(items_b),
                    }
                )
                has_non_timestamp_diff = True
            else:
                for i, (ia, ib) in enumerate(zip(items_a, items_b, strict=False)):
                    sub_diffs, sub_ts_only = _compare_messages_semantic(
                        ia, ib, config, f"{field_path}[{i}]", skip_timestamps
                    )
                    diffs.extend(sub_diffs)
                    if sub_diffs and not sub_ts_only:
                        has_non_timestamp_diff = True
            continue

        # Handle repeated scalar fields
        if field_desc.is_repeated:
            val_a = list(getattr(msg_a, field_desc.name))
            val_b = list(getattr(msg_b, field_desc.name))
            if val_a != val_b:
                diffs.append({"field": field_path, "a": val_a, "b": val_b})
                has_non_timestamp_diff = True
            continue

        # Handle singular message fields
        if field_desc.message_type is not None:
            # Position gets special tolerance-based comparison
            if _is_position_field(field_desc):
                has_a = msg_a.HasField(field_desc.name)
                has_b = msg_b.HasField(field_desc.name)
                if has_a and has_b:
                    pos_diffs = _compare_position(
                        getattr(msg_a, field_desc.name),
                        getattr(msg_b, field_desc.name),
                        config,
                    )
                    if pos_diffs:
                        diffs.extend(pos_diffs)
                        has_non_timestamp_diff = True
                elif has_a != has_b:
                    diffs.append(
                        {
                            "field": field_path,
                            "detail": "present in one feed but not the other",
                        }
                    )
                    has_non_timestamp_diff = True
                continue

            has_a = msg_a.HasField(field_desc.name)
            has_b = msg_b.HasField(field_desc.name)

            if has_a and has_b:
                sub_diffs, sub_ts_only = _compare_messages_semantic(
                    getattr(msg_a, field_desc.name),
                    getattr(msg_b, field_desc.name),
                    config,
                    field_path,
                    skip_timestamps,
                )
                diffs.extend(sub_diffs)
                if sub_diffs and not sub_ts_only:
                    has_non_timestamp_diff = True
            elif has_a != has_b:
                # One has the sub-message, the other doesn't
                present = getattr(
                    msg_a if has_a else msg_b,
                    field_desc.name,
                )
                # Check if the present sub-message is effectively empty
                if present.ListFields():
                    diffs.append(
                        {
                            "field": field_path,
                            "detail": "present in one feed but not the other",
                        }
                    )
                    has_non_timestamp_diff = True
            continue

        # Scalar fields
        val_a = _get_field_value_with_default(msg_a, field_desc)
        val_b = _get_field_value_with_default(msg_b, field_desc)

        # Handle default value equivalence (Level 2)
        # If one has default value and the other is the same default, they match
        default_val = _get_default_value(field_desc)
        if val_a == default_val and val_b == default_val:
            continue

        # Timestamp fields get tolerance-based comparison
        if _is_timestamp_field(field_desc):
            if skip_timestamps:
                continue
            if isinstance(val_a, (int, float)) and isinstance(val_b, (int, float)):
                if not _approx_equal(
                    float(val_a),
                    float(val_b),
                    float(config.entity_timestamp_tolerance_seconds),
                ):
                    diffs.append(
                        {
                            "field": field_path,
                            "a": val_a,
                            "b": val_b,
                            "type": "timestamp",
                        }
                    )
                    # Timestamp diff is NOT a non-timestamp diff
                else:
                    # Within tolerance - still note it as a diff but timestamp-only
                    if val_a != val_b:
                        diffs.append(
                            {
                                "field": field_path,
                                "a": val_a,
                                "b": val_b,
                                "type": "timestamp",
                                "within_tolerance": True,
                            }
                        )
                continue

        if val_a != val_b:
            diffs.append({"field": field_path, "a": val_a, "b": val_b})
            has_non_timestamp_diff = True

    return diffs, not has_non_timestamp_diff if diffs else False


def _compare_entities_structural(entity_a, entity_b) -> bool:
    """Check if two entities are structurally identical (field-by-field exact match)."""
    return entity_a == entity_b


def compare_feeds(
    feed_a_bytes: bytes,
    feed_b_bytes: bytes,
    config: ComparisonConfig | None = None,
) -> ComparisonReport:
    """Compare two GTFS-RT feeds and produce a comparison report.

    Args:
        feed_a_bytes: Serialized FeedMessage bytes for feed A
        feed_b_bytes: Serialized FeedMessage bytes for feed B
        config: Comparison configuration (uses defaults if None)

    Returns:
        ComparisonReport with equivalence level and differences
    """
    if config is None:
        config = ComparisonConfig()

    # Level 0: Byte-identical check
    byte_identical = feed_a_bytes == feed_b_bytes

    # Parse feeds
    feed_a = gtfs_rt.FeedMessage()
    feed_a.ParseFromString(feed_a_bytes)

    feed_b = gtfs_rt.FeedMessage()
    feed_b.ParseFromString(feed_b_bytes)

    # Detect feed type
    feed_type = _detect_feed_type(feed_a)
    if feed_type == "unknown":
        feed_type = _detect_feed_type(feed_b)

    # Compare headers
    header_a = feed_a.header
    header_b = feed_b.header
    ts_delta = abs(header_a.timestamp - header_b.timestamp)

    header_cmp = HeaderComparison(
        timestamp_a=header_a.timestamp,
        timestamp_b=header_b.timestamp,
        timestamp_delta_seconds=ts_delta,
        version_match=(
            header_a.gtfs_realtime_version == header_b.gtfs_realtime_version
        ),
        incrementality_match=(header_a.incrementality == header_b.incrementality),
    )

    # Match entities
    matched_pairs, added_entities, removed_entities = match_entities(
        list(feed_a.entity), list(feed_b.entity)
    )

    summary = EntitySummary(
        entities_a=len(feed_a.entity),
        entities_b=len(feed_b.entity),
    )

    differences: list[EntityDifference] = []

    # Build stale entity sets using each feed's header timestamp
    stale_ids_a = {
        entity.id
        for entity in feed_a.entity
        if _is_entity_stale(entity, header_a.timestamp, config.stale_threshold_seconds)
    }
    stale_ids_b = {
        entity.id
        for entity in feed_b.entity
        if _is_entity_stale(entity, header_b.timestamp, config.stale_threshold_seconds)
    }

    # Process added entities (present in B but not A)
    for entity in added_entities:
        if entity.id in stale_ids_b:
            differences.append(
                EntityDifference(
                    entity_id=entity.id,
                    status="stale",
                    detail="Entity present only in feed B (stale)",
                )
            )
            summary.stale += 1
        else:
            differences.append(
                EntityDifference(
                    entity_id=entity.id,
                    status="added",
                    detail="Entity present only in feed B",
                )
            )
            summary.added += 1

    # Process removed entities (present in A but not B)
    for entity in removed_entities:
        if entity.id in stale_ids_a:
            differences.append(
                EntityDifference(
                    entity_id=entity.id,
                    status="stale",
                    detail="Entity present only in feed A (stale)",
                )
            )
            summary.stale += 1
        else:
            differences.append(
                EntityDifference(
                    entity_id=entity.id,
                    status="removed",
                    detail="Entity present only in feed A",
                )
            )
            summary.removed += 1

    # Process matched entities
    structurally_identical = True

    for entity_a, entity_b in matched_pairs:
        if not _compare_entities_structural(entity_a, entity_b):
            structurally_identical = False

        # Get the relevant sub-message for comparison
        sub_a = None
        sub_b = None
        entity_type = ""

        if entity_a.HasField("vehicle"):
            sub_a = entity_a.vehicle
            sub_b = entity_b.vehicle
            entity_type = "VehiclePosition"
        elif entity_a.HasField("trip_update"):
            sub_a = entity_a.trip_update
            sub_b = entity_b.trip_update
            entity_type = "TripUpdate"
        elif entity_a.HasField("alert"):
            sub_a = entity_a.alert
            sub_b = entity_b.alert
            entity_type = "Alert"

        if sub_a is not None and sub_b is not None:
            field_diffs, timestamp_only = _compare_messages_semantic(
                sub_a, sub_b, config
            )

            if field_diffs:
                # Filter out within-tolerance timestamp diffs for classification
                meaningful_diffs = [
                    d for d in field_diffs if not d.get("within_tolerance", False)
                ]
                ts_diffs = [d for d in field_diffs if d.get("within_tolerance", False)]

                if meaningful_diffs:
                    # Has real differences beyond timestamps
                    non_ts_diffs = [
                        d for d in meaningful_diffs if d.get("type") != "timestamp"
                    ]
                    if non_ts_diffs:
                        differences.append(
                            EntityDifference(
                                entity_id=entity_a.id,
                                status="modified",
                                detail=f"{entity_type} differs: "
                                + ", ".join(
                                    d.get("field", "unknown") for d in non_ts_diffs
                                ),
                                field_diffs=non_ts_diffs,
                            )
                        )
                        summary.modified += 1
                    else:
                        # Only out-of-tolerance timestamp diffs
                        differences.append(
                            EntityDifference(
                                entity_id=entity_a.id,
                                status="modified",
                                detail=(
                                    f"{entity_type} timestamp differs beyond tolerance"
                                ),
                                field_diffs=meaningful_diffs,
                            )
                        )
                        summary.modified += 1
                elif ts_diffs:
                    # Only within-tolerance timestamp diffs
                    differences.append(
                        EntityDifference(
                            entity_id=entity_a.id,
                            status="timestamp_only",
                            detail=f"{entity_type} timestamps differ within tolerance",
                            field_diffs=ts_diffs,
                        )
                    )
                    summary.timestamp_only += 1
                else:
                    summary.matched += 1
            else:
                summary.matched += 1
        else:
            summary.matched += 1

    # Determine equivalence level
    if byte_identical:
        equivalence_level = 0
    elif (
        structurally_identical
        and not added_entities
        and not removed_entities
        and feed_a.header == feed_b.header
        and len(feed_a.entity) == len(feed_b.entity)
        and all(
            feed_a.entity[i].id == feed_b.entity[i].id
            for i in range(len(feed_a.entity))
        )
        and all(_compare_entities_structural(a, b) for a, b in matched_pairs)
    ):
        equivalence_level = 1
    elif (
        not added_entities
        and not removed_entities
        and summary.modified == 0
        and header_cmp.version_match
        and header_cmp.incrementality_match
        and ts_delta <= config.timestamp_tolerance_seconds
    ):
        equivalence_level = 2
    elif (
        summary.added == 0
        and summary.removed == 0
        and summary.modified == 0
        and header_cmp.version_match
        and header_cmp.incrementality_match
    ):
        # Level 3: functionally equivalent (allows entity timestamp diffs
        # within tolerance, stale entity differences)
        equivalence_level = 3
    else:
        equivalence_level = -1

    return ComparisonReport(
        equivalence_level=equivalence_level,
        feed_type=feed_type,
        header=header_cmp,
        summary=summary,
        differences=differences,
    )
