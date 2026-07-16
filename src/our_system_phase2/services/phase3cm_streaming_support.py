"""Block-composable common-support identities for matched candidate pairs."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence

import numpy as np


class PairSupportAccumulator:
    """Maintain two independent uint64 digests without retaining coordinates."""

    def __init__(self, *, pair_ids: Sequence[str]) -> None:
        self.pair_ids = tuple(str(value) for value in pair_ids)
        if not self.pair_ids or len(set(self.pair_ids)) != len(self.pair_ids):
            raise ValueError("pair IDs must be non-empty and unique")
        count = len(self.pair_ids)
        self.counts = np.zeros(count, dtype=np.uint64)
        self.sum1 = np.zeros(count, dtype=np.uint64)
        self.xor1 = np.zeros(count, dtype=np.uint64)
        self.sum2 = np.zeros(count, dtype=np.uint64)
        self.xor2 = np.zeros(count, dtype=np.uint64)

    def update(
        self,
        *,
        common_masks: np.ndarray,
        trade_times_ns: np.ndarray,
        code_ids: np.ndarray,
        source_shards: np.ndarray,
        source_row_identity: np.ndarray,
        duplicate_ordinal: np.ndarray,
        pair_indices: Sequence[int] | None = None,
    ) -> None:
        masks = np.asarray(common_masks, dtype=np.bool_)
        indices = tuple(range(len(self.pair_ids))) if pair_indices is None else tuple(int(value) for value in pair_indices)
        if not indices or len(set(indices)) != len(indices):
            raise ValueError("pair indices must be non-empty and unique")
        if min(indices) < 0 or max(indices) >= len(self.pair_ids):
            raise ValueError("pair index outside frozen support accumulator")
        coordinates = tuple(
            np.asarray(value)
            for value in (
                trade_times_ns,
                code_ids,
                source_shards,
                source_row_identity,
                duplicate_ordinal,
            )
        )
        row_count = len(coordinates[0])
        if masks.shape != (len(indices), row_count):
            raise ValueError("common support mask shape drift")
        if any(len(value) != row_count for value in coordinates):
            raise ValueError("support coordinate shape drift")
        time = coordinates[0].astype(np.uint64, copy=False)
        code = coordinates[1].astype(np.uint64, copy=False)
        shard = coordinates[2].astype(np.uint64, copy=False)
        row = coordinates[3].astype(np.uint64, copy=False)
        duplicate = coordinates[4].astype(np.uint64, copy=False)
        token1 = (
            time * np.uint64(0x9E3779B185EBCA87)
            ^ code * np.uint64(0xC2B2AE3D27D4EB4F)
            ^ shard * np.uint64(0x165667B19E3779F9)
            ^ row * np.uint64(0x85EBCA77C2B2AE63)
            ^ duplicate * np.uint64(0x27D4EB2F165667C5)
        )
        token2 = (
            time * np.uint64(0xD6E8FEB86659FD93)
            ^ code * np.uint64(0xA5A3564E27F8862D)
            ^ shard * np.uint64(0x8D58AC26AFE12E47)
            ^ row * np.uint64(0x9E6C63D0676A9A99)
            ^ duplicate * np.uint64(0xC6BC279692B5CC83)
        )
        for local_index, pair_index in enumerate(indices):
            active = masks[local_index]
            first = token1[active]
            second = token2[active]
            self.counts[pair_index] += np.uint64(len(first))
            if len(first):
                self.sum1[pair_index] = np.uint64(
                    (int(self.sum1[pair_index]) + int(np.sum(first, dtype=np.uint64))) & ((1 << 64) - 1)
                )
                self.xor1[pair_index] ^= np.bitwise_xor.reduce(first)
                self.sum2[pair_index] = np.uint64(
                    (int(self.sum2[pair_index]) + int(np.sum(second, dtype=np.uint64))) & ((1 << 64) - 1)
                )
                self.xor2[pair_index] ^= np.bitwise_xor.reduce(second)

    def identities(self) -> dict[str, dict[str, Any]]:
        output: dict[str, dict[str, Any]] = {}
        for index, pair_id in enumerate(self.pair_ids):
            payload = {
                "schema_version": "cn_pair_common_support_digest_v1",
                "pair_id": pair_id,
                "count": int(self.counts[index]),
                "sum1": int(self.sum1[index]),
                "xor1": int(self.xor1[index]),
                "sum2": int(self.sum2[index]),
                "xor2": int(self.xor2[index]),
            }
            payload["support_identity"] = hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            output[pair_id] = payload
        return output

    def continuation_payload(self) -> dict[str, Any]:
        return {
            "pair_ids": list(self.pair_ids),
            "counts": self.counts.copy(),
            "sum1": self.sum1.copy(),
            "xor1": self.xor1.copy(),
            "sum2": self.sum2.copy(),
            "xor2": self.xor2.copy(),
        }

    def restore_continuation_payload(self, payload: Mapping[str, Any]) -> None:
        if tuple(str(value) for value in payload.get("pair_ids") or ()) != self.pair_ids:
            raise ValueError("pair support accumulator identity drift")
        for name in ("counts", "sum1", "xor1", "sum2", "xor2"):
            observed = np.asarray(payload[name], dtype=np.uint64)
            target = getattr(self, name)
            if observed.shape != target.shape:
                raise ValueError(f"pair support accumulator shape drift: {name}")
            target[...] = observed
