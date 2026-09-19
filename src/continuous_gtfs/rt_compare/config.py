"""Comparison configuration with configurable tolerances."""

from dataclasses import dataclass


@dataclass
class ComparisonConfig:
    """Configuration for GTFS-RT feed comparison tolerances."""

    # Header timestamp tolerance in seconds
    timestamp_tolerance_seconds: int = 30

    # Number of decimal places for lat/lon comparison (~1m at 5 places)
    position_decimal_places: int = 5

    # Bearing tolerance in degrees
    bearing_tolerance: float = 1.0

    # Speed tolerance in m/s
    speed_tolerance: float = 0.1

    # Entity-level timestamp tolerance in seconds (for Level 3)
    entity_timestamp_tolerance_seconds: int = 30

    # Stale entity threshold in seconds (for Level 3)
    stale_threshold_seconds: int = 300
