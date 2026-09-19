"""Pytest configuration and shared fixtures for rt_compare tests."""

from __future__ import annotations

import pytest

from continuous_gtfs.rt_compare.config import ComparisonConfig

from .fixtures import (
    make_added_entity,
    make_alert_time_overlap,
    make_default_value_presence,
    make_identical_reordered,
    make_identical_timestamps,
    make_modified_position,
    make_modified_position_noise,
    make_removed_entity,
    make_stale_entity,
    make_stop_time_update_reorder,
)


@pytest.fixture
def default_config():
    return ComparisonConfig()


@pytest.fixture
def identical_reordered():
    return make_identical_reordered()


@pytest.fixture
def identical_timestamps():
    return make_identical_timestamps()


@pytest.fixture
def added_entity():
    return make_added_entity()


@pytest.fixture
def removed_entity():
    return make_removed_entity()


@pytest.fixture
def modified_position():
    return make_modified_position()


@pytest.fixture
def modified_position_noise():
    return make_modified_position_noise()


@pytest.fixture
def stale_entity():
    return make_stale_entity()


@pytest.fixture
def stop_time_update_reorder():
    return make_stop_time_update_reorder()


@pytest.fixture
def default_value_presence():
    return make_default_value_presence()


@pytest.fixture
def alert_time_overlap():
    return make_alert_time_overlap()
