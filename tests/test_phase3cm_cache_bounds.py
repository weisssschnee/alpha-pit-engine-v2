from __future__ import annotations

import pandas as pd

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    _BoundedSeriesCache,
    _feature_matrix_value_bytes,
    _store_bounded_feature_matrix,
)


def test_series_cache_enforces_byte_budget_with_eviction() -> None:
    stats: dict[str, int] = {}
    cache = _BoundedSeriesCache(
        max_entries=10,
        max_bytes=160,
        stats=stats,
        prefix="operator_cache",
    )

    cache["a"] = pd.Series(range(10), dtype=float)
    cache["b"] = pd.Series(range(10), dtype=float)
    cache["c"] = pd.Series(range(10), dtype=float)

    assert "a" not in cache
    assert "b" in cache
    assert "c" in cache
    assert cache.current_bytes <= 160
    assert stats["operator_cache_memory_evictions"] == 1


def test_series_cache_does_not_store_single_value_larger_than_budget() -> None:
    stats: dict[str, int] = {}
    cache = _BoundedSeriesCache(
        max_entries=10,
        max_bytes=64,
        stats=stats,
        prefix="operator_cache",
    )

    cache["too-large"] = pd.Series(range(10), dtype=float)

    assert len(cache) == 0
    assert stats["operator_cache_skipped_byte_capacity"] == 1


def test_feature_matrix_cache_enforces_combined_byte_budget() -> None:
    stats: dict[str, int] = {}
    cache: dict[int, tuple[pd.DataFrame, pd.Series]] = {}
    value = (pd.DataFrame({"x": range(10)}), pd.Series([True] * 10))
    value_bytes = _feature_matrix_value_bytes(value)

    for key in (1, 2, 3):
        assert _store_bounded_feature_matrix(
            cache,
            key,
            value,
            stats=stats,
            max_windows=4,
            max_bytes=value_bytes * 2,
        )

    assert list(cache) == [2, 3]
    assert stats["feature_matrix_cache_memory_evictions"] == 1
    assert stats["feature_matrix_cache_memory_bytes_current"] <= value_bytes * 2
