"""Block-composable common-support identities for matched candidate pairs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

try:  # pragma: no cover - native path is exercised on 77o.
    from numba import njit, prange
except Exception:  # pragma: no cover
    njit = None
    prange = range


if njit is not None:

    @njit(cache=True, parallel=True)
    def _digest_mask_chunks(
        masks: np.ndarray,
        token1: np.ndarray,
        token2: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        pair_count, row_count = masks.shape
        chunk_size = 262_144
        chunk_count = max(1, min(32, (row_count + chunk_size - 1) // chunk_size))
        counts = np.zeros((pair_count, chunk_count), dtype=np.uint64)
        sum1 = np.zeros((pair_count, chunk_count), dtype=np.uint64)
        xor1 = np.zeros((pair_count, chunk_count), dtype=np.uint64)
        sum2 = np.zeros((pair_count, chunk_count), dtype=np.uint64)
        xor2 = np.zeros((pair_count, chunk_count), dtype=np.uint64)
        for work_index in prange(pair_count * chunk_count):
            pair_index = work_index // chunk_count
            chunk_index = work_index - pair_index * chunk_count
            start = chunk_index * row_count // chunk_count
            end = (chunk_index + 1) * row_count // chunk_count
            local_count = np.uint64(0)
            local_sum1 = np.uint64(0)
            local_xor1 = np.uint64(0)
            local_sum2 = np.uint64(0)
            local_xor2 = np.uint64(0)
            for row_index in range(start, end):
                if masks[pair_index, row_index]:
                    first = token1[row_index]
                    second = token2[row_index]
                    local_count += np.uint64(1)
                    local_sum1 += first
                    local_xor1 ^= first
                    local_sum2 += second
                    local_xor2 ^= second
            counts[pair_index, chunk_index] = local_count
            sum1[pair_index, chunk_index] = local_sum1
            xor1[pair_index, chunk_index] = local_xor1
            sum2[pair_index, chunk_index] = local_sum2
            xor2[pair_index, chunk_index] = local_xor2
        output_count = np.zeros(pair_count, dtype=np.uint64)
        output_sum1 = np.zeros(pair_count, dtype=np.uint64)
        output_xor1 = np.zeros(pair_count, dtype=np.uint64)
        output_sum2 = np.zeros(pair_count, dtype=np.uint64)
        output_xor2 = np.zeros(pair_count, dtype=np.uint64)
        for pair_index in range(pair_count):
            for chunk_index in range(chunk_count):
                output_count[pair_index] += counts[pair_index, chunk_index]
                output_sum1[pair_index] += sum1[pair_index, chunk_index]
                output_xor1[pair_index] ^= xor1[pair_index, chunk_index]
                output_sum2[pair_index] += sum2[pair_index, chunk_index]
                output_xor2[pair_index] ^= xor2[pair_index, chunk_index]
        return output_count, output_sum1, output_xor1, output_sum2, output_xor2

else:  # pragma: no cover
    _digest_mask_chunks = None


@dataclass(frozen=True, slots=True)
class PairSupportBlockTokens:
    token1: np.ndarray
    token2: np.ndarray


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
        trade_times_ns: np.ndarray | None = None,
        code_ids: np.ndarray | None = None,
        source_shards: np.ndarray | None = None,
        source_row_identity: np.ndarray | None = None,
        duplicate_ordinal: np.ndarray | None = None,
        pair_indices: Sequence[int] | None = None,
        block_tokens: PairSupportBlockTokens | None = None,
    ) -> None:
        masks = np.asarray(common_masks, dtype=np.bool_)
        indices = tuple(range(len(self.pair_ids))) if pair_indices is None else tuple(int(value) for value in pair_indices)
        if not indices or len(set(indices)) != len(indices):
            raise ValueError("pair indices must be non-empty and unique")
        if min(indices) < 0 or max(indices) >= len(self.pair_ids):
            raise ValueError("pair index outside frozen support accumulator")
        if block_tokens is None:
            if any(
                value is None
                for value in (
                    trade_times_ns,
                    code_ids,
                    source_shards,
                    source_row_identity,
                    duplicate_ordinal,
                )
            ):
                raise ValueError("support coordinates are required when block tokens are absent")
            block_tokens = self.prepare_block_tokens(
                trade_times_ns=np.asarray(trade_times_ns),
                code_ids=np.asarray(code_ids),
                source_shards=np.asarray(source_shards),
                source_row_identity=np.asarray(source_row_identity),
                duplicate_ordinal=np.asarray(duplicate_ordinal),
            )
        token1 = np.asarray(block_tokens.token1, dtype=np.uint64)
        token2 = np.asarray(block_tokens.token2, dtype=np.uint64)
        row_count = len(token1)
        if masks.shape != (len(indices), row_count):
            raise ValueError("common support mask shape drift")
        if len(token2) != row_count:
            raise ValueError("support token shape drift")
        if _digest_mask_chunks is None:
            raise RuntimeError("Numba is required for native pair support digests")
        counts, sum1, xor1, sum2, xor2 = _digest_mask_chunks(masks, token1, token2)
        for local_index, pair_index in enumerate(indices):
            self.counts[pair_index] += counts[local_index]
            self.sum1[pair_index] = np.uint64(
                (int(self.sum1[pair_index]) + int(sum1[local_index])) & ((1 << 64) - 1)
            )
            self.xor1[pair_index] ^= xor1[local_index]
            self.sum2[pair_index] = np.uint64(
                (int(self.sum2[pair_index]) + int(sum2[local_index])) & ((1 << 64) - 1)
            )
            self.xor2[pair_index] ^= xor2[local_index]

    @staticmethod
    def prepare_block_tokens(
        *,
        trade_times_ns: np.ndarray,
        code_ids: np.ndarray,
        source_shards: np.ndarray,
        source_row_identity: np.ndarray,
        duplicate_ordinal: np.ndarray,
    ) -> PairSupportBlockTokens:
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
        return PairSupportBlockTokens(token1=token1, token2=token2)

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
