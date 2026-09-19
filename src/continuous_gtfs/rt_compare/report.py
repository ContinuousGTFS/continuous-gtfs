"""Report generation for GTFS-RT feed comparison."""

from __future__ import annotations

import json
from dataclasses import dataclass, field


@dataclass
class EntityDifference:
    """A single entity-level difference."""

    entity_id: str
    status: str  # matched, added, removed, modified, timestamp_only, stale
    detail: str
    field_diffs: list[dict] = field(default_factory=list)


@dataclass
class HeaderComparison:
    """Header comparison results."""

    timestamp_a: int
    timestamp_b: int
    timestamp_delta_seconds: int
    version_match: bool
    incrementality_match: bool


@dataclass
class EntitySummary:
    """Summary counts of entity comparison."""

    entities_a: int
    entities_b: int
    matched: int = 0
    added: int = 0
    removed: int = 0
    modified: int = 0
    timestamp_only: int = 0
    stale: int = 0


@dataclass
class ComparisonReport:
    """Full comparison report."""

    equivalence_level: int
    feed_type: str
    header: HeaderComparison
    summary: EntitySummary
    differences: list[EntityDifference] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert report to dictionary."""
        return {
            "equivalence_level": self.equivalence_level,
            "feed_type": self.feed_type,
            "header": {
                "timestamp_a": self.header.timestamp_a,
                "timestamp_b": self.header.timestamp_b,
                "timestamp_delta_seconds": self.header.timestamp_delta_seconds,
                "version_match": self.header.version_match,
                "incrementality_match": self.header.incrementality_match,
            },
            "summary": {
                "entities_a": self.summary.entities_a,
                "entities_b": self.summary.entities_b,
                "matched": self.summary.matched,
                "added": self.summary.added,
                "removed": self.summary.removed,
                "modified": self.summary.modified,
                "timestamp_only": self.summary.timestamp_only,
                "stale": self.summary.stale,
            },
            "differences": [
                {
                    "entity_id": d.entity_id,
                    "status": d.status,
                    "detail": d.detail,
                    **({"field_diffs": d.field_diffs} if d.field_diffs else {}),
                }
                for d in self.differences
            ],
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert report to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def to_text(self) -> str:
        """Convert report to human-readable text."""
        level_names = {
            0: "byte-identical",
            1: "structurally identical",
            2: "semantically equivalent",
            3: "functionally equivalent",
        }
        level_name = level_names.get(self.equivalence_level, "not equivalent")

        lines = []
        lines.append(f"GTFS-RT Comparison: {self.feed_type}")

        if self.equivalence_level >= 0:
            lines.append(f"Equivalence: Level {self.equivalence_level} ({level_name})")
        else:
            lines.append("Equivalence: NOT equivalent")

        # Header info
        h = self.header
        version_mark = "match" if h.version_match else "MISMATCH"
        lines.append(
            f"Header: timestamp delta {h.timestamp_delta_seconds}s "
            f"(within tolerance), version {version_mark}"
        )

        lines.append("")

        # Entity summary
        s = self.summary
        lines.append(f"Entities: {s.entities_a} vs {s.entities_b}")
        lines.append(f"  matched: {s.matched}")
        if s.added > 0:
            lines.append(f"  + {s.added} added")
        if s.removed > 0:
            lines.append(f"  - {s.removed} removed")
        if s.modified > 0:
            lines.append(f"  ~ {s.modified} modified")
        if s.timestamp_only > 0:
            lines.append(f"  ~ {s.timestamp_only} timestamp_only")
        if s.stale > 0:
            lines.append(f"  s {s.stale} stale")

        if self.differences:
            lines.append("")
            lines.append("Differences:")
            for d in self.differences:
                prefix = {
                    "added": "[+]",
                    "removed": "[-]",
                    "modified": "[~]",
                    "timestamp_only": "[t]",
                    "stale": "[s]",
                }.get(d.status, "[?]")
                lines.append(f"  {prefix} {d.entity_id}: {d.detail}")

        return "\n".join(lines)
