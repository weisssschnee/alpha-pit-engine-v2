from __future__ import annotations

import numpy as np
import pytest

from our_system_phase2.services.phase3cm_streaming_cache import BoundedBlockCache, CacheBudgetError


def test_bounded_block_cache_evicts_deterministically_by_lru() -> None:
    cache = BoundedBlockCache(max_bytes=32, max_entries=2)
    cache.put("a", np.arange(2, dtype=np.float64))
    cache.put("b", np.arange(2, dtype=np.float64))
    assert cache.get("a") is not None

    cache.put("c", np.arange(2, dtype=np.float64))

    assert cache.get("a") is not None
    assert cache.get("b") is None
    assert cache.get("c") is not None
    assert cache.stats["evictions"] == 1
    assert cache.current_bytes <= cache.max_bytes


def test_single_value_larger_than_budget_fails_closed() -> None:
    cache = BoundedBlockCache(max_bytes=8, max_entries=4)
    with pytest.raises(CacheBudgetError, match="single cache value exceeds"):
        cache.put("too-large", np.arange(2, dtype=np.float64))


def test_consumer_release_removes_value_early() -> None:
    cache = BoundedBlockCache(max_bytes=1024, max_entries=4)
    cache.put("node", np.arange(10, dtype=np.float64), remaining_consumers=2)

    assert cache.release_consumer("node") == 1
    assert "node" in cache
    assert cache.release_consumer("node") == 0
    assert "node" not in cache
    assert cache.stats["consumer_releases"] == 1
