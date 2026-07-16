"""Byte-bounded deterministic caches for streaming Phase3CM blocks."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Hashable


class CacheBudgetError(MemoryError):
    """Raised when one value cannot fit without violating the cache contract."""


def _value_bytes(value: Any) -> int:
    if hasattr(value, "nbytes"):
        return int(value.nbytes)
    if hasattr(value, "memory_usage"):
        usage = value.memory_usage(deep=True)
        return int(usage.sum() if hasattr(usage, "sum") else usage)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    raise TypeError(f"cache value does not expose a deterministic byte size: {type(value)!r}")


@dataclass(slots=True)
class _Entry:
    value: Any
    bytes: int
    remaining_consumers: int | None


class BoundedBlockCache:
    """LRU cache whose byte and entry limits are hard contracts."""

    def __init__(self, *, max_bytes: int, max_entries: int) -> None:
        self.max_bytes = int(max_bytes)
        self.max_entries = int(max_entries)
        if self.max_bytes <= 0 or self.max_entries <= 0:
            raise ValueError("cache limits must be positive")
        self._entries: OrderedDict[Hashable, _Entry] = OrderedDict()
        self.current_bytes = 0
        self.peak_bytes = 0
        self.stats = {
            "hits": 0,
            "misses": 0,
            "stores": 0,
            "replacements": 0,
            "evictions": 0,
            "consumer_releases": 0,
            "clear_count": 0,
        }

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, key: Hashable) -> bool:
        return key in self._entries

    def get(self, key: Hashable) -> Any | None:
        entry = self._entries.get(key)
        if entry is None:
            self.stats["misses"] += 1
            return None
        self._entries.move_to_end(key)
        self.stats["hits"] += 1
        return entry.value

    def put(
        self,
        key: Hashable,
        value: Any,
        *,
        remaining_consumers: int | None = None,
    ) -> None:
        size = _value_bytes(value)
        if size > self.max_bytes:
            raise CacheBudgetError(
                f"single cache value exceeds byte budget: {size} > {self.max_bytes}"
            )
        if remaining_consumers is not None and int(remaining_consumers) < 0:
            raise ValueError("remaining_consumers cannot be negative")
        previous = self._entries.pop(key, None)
        if previous is not None:
            self.current_bytes -= previous.bytes
            self.stats["replacements"] += 1
        while self._entries and (
            len(self._entries) >= self.max_entries or self.current_bytes + size > self.max_bytes
        ):
            _, evicted = self._entries.popitem(last=False)
            self.current_bytes -= evicted.bytes
            self.stats["evictions"] += 1
        self._entries[key] = _Entry(
            value=value,
            bytes=size,
            remaining_consumers=None if remaining_consumers is None else int(remaining_consumers),
        )
        self.current_bytes += size
        self.peak_bytes = max(self.peak_bytes, self.current_bytes)
        self.stats["stores"] += 1

    def release_consumer(self, key: Hashable) -> int | None:
        entry = self._entries.get(key)
        if entry is None or entry.remaining_consumers is None:
            return None
        if entry.remaining_consumers <= 0:
            raise RuntimeError(f"consumer count already exhausted for {key!r}")
        entry.remaining_consumers -= 1
        if entry.remaining_consumers == 0:
            self.current_bytes -= entry.bytes
            del self._entries[key]
            self.stats["consumer_releases"] += 1
        return entry.remaining_consumers

    def clear(self) -> None:
        self._entries.clear()
        self.current_bytes = 0
        self.stats["clear_count"] += 1

    def audit(self) -> dict[str, int]:
        return {
            **self.stats,
            "current_bytes": self.current_bytes,
            "peak_bytes": self.peak_bytes,
            "entry_count": len(self._entries),
            "max_bytes": self.max_bytes,
            "max_entries": self.max_entries,
        }
