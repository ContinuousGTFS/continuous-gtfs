"""Parser registry for asset content kinds.

Every asset declares a content_kind (see specs/asset-registry.md
§Content Kinds). This module owns the closed set of kinds and the
functions that turn raw bytes into the Python objects transforms
consume. The worker and the CLI both go through `parse()` so
`ctx.inputs[name]` has the same shape regardless of whether it came
from an orchestrator dispatch or a local file.

Adding a new kind: register a parser here, add the kind to the
`assets.content_kind` CHECK constraint in a new migration.
"""

from __future__ import annotations

import io
from collections.abc import Callable
from typing import Any

_parsers: dict[str, Callable[[bytes], Any]] = {}


def register_parser(
    content_kind: str,
) -> Callable[[Callable[[bytes], Any]], Callable[[bytes], Any]]:
    """Register a parser for a content kind.

    Decorator form:
        @register_parser("csv_table")
        def _parse_csv(raw: bytes) -> pl.DataFrame:
            ...
    """

    def deco(fn: Callable[[bytes], Any]) -> Callable[[bytes], Any]:
        _parsers[content_kind] = fn
        return fn

    return deco


def parse(content_kind: str, raw: bytes) -> Any:
    """Parse raw bytes according to content_kind.

    `opaque_bytes` is the escape hatch — bytes pass through untouched so
    transforms that want raw access can have it. Unknown kinds raise
    (a kind is unknown when no parser has been registered for it;
    `assets.content_kind` CHECK constraint keeps this in sync with the
    DB).
    """
    if content_kind == "opaque_bytes":
        return raw
    if content_kind not in _parsers:
        raise ValueError(
            f"No parser registered for content_kind={content_kind!r}. "
            f"Known kinds: {sorted(_parsers) + ['opaque_bytes']}"
        )
    return _parsers[content_kind](raw)


def known_kinds() -> set[str]:
    """Return the set of content kinds with registered parsers, plus opaque_bytes."""
    return set(_parsers) | {"opaque_bytes"}


# --- Built-in parsers ---


@register_parser("gtfs_schedule_zip")
def _parse_schedule_zip(raw: bytes) -> dict[str, Any]:
    """GTFS schedule zip → dict of filename → polars.DataFrame."""
    from .pipelines.schedule import extract_zip

    return extract_zip(raw)


@register_parser("csv_table")
def _parse_csv_table(raw: bytes) -> Any:
    """CSV/TSV bytes → polars.DataFrame (string-typed columns).

    All columns typed as string matches GTFS ingest semantics — transforms
    stay in control of type coercion and avoid losing leading zeros on
    stop_ids, numeric-looking IDs, etc.
    """
    import polars as pl

    return pl.read_csv(io.BytesIO(raw), infer_schema_length=0)


@register_parser("gtfs_rt_protobuf")
def _parse_rt_protobuf(raw: bytes) -> Any:
    """GTFS-RT protobuf bytes → FeedMessage."""
    from google.transit import gtfs_realtime_pb2 as gtfs_rt

    feed = gtfs_rt.FeedMessage()
    feed.ParseFromString(raw)
    return feed
