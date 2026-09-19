"""Entity matching logic for GTFS-RT feed comparison."""

from __future__ import annotations

import warnings
from collections import Counter

from google.transit import gtfs_realtime_pb2 as gtfs_rt


def _build_entity_map(
    entities: list[gtfs_rt.FeedEntity], label: str
) -> dict[str, gtfs_rt.FeedEntity]:
    """Build entity map, warning on duplicate IDs (keeps last occurrence)."""
    id_counts = Counter(e.id for e in entities)
    dupes = {eid: count for eid, count in id_counts.items() if count > 1}
    if dupes:
        warnings.warn(
            f"Feed {label} has {len(dupes)} duplicate entity ID(s): "
            + ", ".join(f"{eid} (x{count})" for eid, count in list(dupes.items())[:5]),
            stacklevel=2,
        )
    return {e.id: e for e in entities}


def match_entities(
    entities_a: list[gtfs_rt.FeedEntity],
    entities_b: list[gtfs_rt.FeedEntity],
) -> tuple[
    list[tuple[gtfs_rt.FeedEntity, gtfs_rt.FeedEntity]],
    list[gtfs_rt.FeedEntity],
    list[gtfs_rt.FeedEntity],
]:
    """Match entities by entity.id.

    Returns:
        (matched_pairs, added_entities, removed_entities)
        - matched_pairs: list of (entity_a, entity_b) tuples
        - added_entities: entities only in B
        - removed_entities: entities only in A

    Note: duplicate entity IDs within a feed are warned about. The last
    occurrence is used for matching (consistent with protobuf list semantics).
    """
    map_a = _build_entity_map(entities_a, "A")
    map_b = _build_entity_map(entities_b, "B")

    ids_a = set(map_a.keys())
    ids_b = set(map_b.keys())

    matched_ids = ids_a & ids_b
    added_ids = ids_b - ids_a
    removed_ids = ids_a - ids_b

    matched = [(map_a[eid], map_b[eid]) for eid in sorted(matched_ids)]
    added = [map_b[eid] for eid in sorted(added_ids)]
    removed = [map_a[eid] for eid in sorted(removed_ids)]

    return matched, added, removed


def match_stop_time_updates(
    stus_a: list, stus_b: list
) -> tuple[list[tuple], list, list]:
    """Match stop_time_updates by stop_sequence (or stop_id if absent).

    Returns:
        (matched_pairs, added, removed)
    """

    def key_fn(stu):
        if stu.stop_sequence != 0:
            return ("seq", stu.stop_sequence)
        return ("id", stu.stop_id)

    map_a = {key_fn(s): s for s in stus_a}
    map_b = {key_fn(s): s for s in stus_b}

    keys_a = set(map_a.keys())
    keys_b = set(map_b.keys())

    matched_keys = keys_a & keys_b
    added_keys = keys_b - keys_a
    removed_keys = keys_a - keys_b

    matched = [(map_a[k], map_b[k]) for k in sorted(matched_keys)]
    added = [map_b[k] for k in sorted(added_keys)]
    removed = [map_a[k] for k in sorted(removed_keys)]

    return matched, added, removed


def match_informed_entities(ies_a: list, ies_b: list) -> tuple[list[tuple], list, list]:
    """Match informed_entity entries by composite key.

    Composite key: (agency_id, route_id, trip.trip_id, stop_id)
    """

    def key_fn(ie):
        trip_id = ie.trip.trip_id if ie.HasField("trip") else ""
        return (ie.agency_id, ie.route_id, trip_id, ie.stop_id)

    map_a = {}
    for ie in ies_a:
        map_a[key_fn(ie)] = ie
    map_b = {}
    for ie in ies_b:
        map_b[key_fn(ie)] = ie

    keys_a = set(map_a.keys())
    keys_b = set(map_b.keys())

    matched_keys = keys_a & keys_b
    added_keys = keys_b - keys_a
    removed_keys = keys_a - keys_b

    matched = [(map_a[k], map_b[k]) for k in sorted(matched_keys)]
    added = [map_b[k] for k in sorted(added_keys)]
    removed = [map_a[k] for k in sorted(removed_keys)]

    return matched, added, removed


def match_active_periods(aps_a: list, aps_b: list) -> tuple[list[tuple], list, list]:
    """Match active_period entries by (start, end) pair."""

    def key_fn(ap):
        return (ap.start, ap.end)

    map_a = {key_fn(ap): ap for ap in aps_a}
    map_b = {key_fn(ap): ap for ap in aps_b}

    keys_a = set(map_a.keys())
    keys_b = set(map_b.keys())

    matched_keys = keys_a & keys_b
    added_keys = keys_b - keys_a
    removed_keys = keys_a - keys_b

    matched = [(map_a[k], map_b[k]) for k in sorted(matched_keys)]
    added = [map_b[k] for k in sorted(added_keys)]
    removed = [map_a[k] for k in sorted(removed_keys)]

    return matched, added, removed
