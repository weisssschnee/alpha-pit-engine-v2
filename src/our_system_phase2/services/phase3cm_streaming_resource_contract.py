"""Frozen Phase3CM execution plans, thread contracts, and RSS gates."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, replace
from typing import Any, Callable, Mapping, Sequence

from our_system_phase2.services.phase3cm_streaming_telemetry import (
    ResourceSnapshot,
    freeze_thread_budget,
)


class FrozenPlanDriftError(RuntimeError):
    """The runtime no longer matches its content-addressed qualification plan."""


class SoftRSSGateStop(RuntimeError):
    """A recoverable stop after a complete checkpoint at the soft RSS gate."""


class HardRSSGateError(MemoryError):
    """An immediate fail-closed hard RSS breach."""


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class FrozenExecutionPlan:
    schema_version: str
    phase: str
    block_size: int
    block_boundaries: tuple[tuple[str, str], ...]
    pair_batches: tuple[tuple[str, ...], ...]
    heavy_processes: int
    compute_threads: int
    primary_thread_pool: str
    thread_environment: dict[str, str]
    cache_caps: dict[str, int]
    checkpoint_every_blocks: int
    rss_soft_bytes: int
    rss_hard_bytes: int
    global_rss_hard_bytes: int
    execution_plan_hash: str

    @classmethod
    def create(
        cls,
        *,
        phase: str,
        block_size: int,
        block_boundaries: Sequence[Sequence[str]],
        pair_batches: Sequence[Sequence[str]],
        heavy_processes: int,
        compute_threads: int,
        primary_thread_pool: str,
        cache_caps: Mapping[str, int],
        checkpoint_every_blocks: int,
        rss_soft_bytes: int,
        rss_hard_bytes: int,
        global_rss_hard_bytes: int,
    ) -> "FrozenExecutionPlan":
        phase_name = str(phase).upper()
        if phase_name not in {"C", "D", "E"}:
            raise ValueError("execution phase must be C, D, or E")
        if int(block_size) <= 0 or int(checkpoint_every_blocks) <= 0:
            raise ValueError("block size and checkpoint cadence must be positive")
        if not (0 < int(rss_soft_bytes) < int(rss_hard_bytes) <= int(global_rss_hard_bytes)):
            raise ValueError("RSS gates must satisfy 0 < soft < worker hard <= global hard")
        thread_budget = freeze_thread_budget(
            heavy_processes=int(heavy_processes),
            compute_threads_per_process=int(compute_threads),
            primary_pool=str(primary_thread_pool),
        )
        payload = {
            "schema_version": "cn_phase3cm_frozen_execution_plan_v1",
            "phase": phase_name,
            "block_size": int(block_size),
            "block_boundaries": tuple(tuple(str(value) for value in row) for row in block_boundaries),
            "pair_batches": tuple(tuple(str(value) for value in row) for row in pair_batches),
            "heavy_processes": int(heavy_processes),
            "compute_threads": int(compute_threads),
            "primary_thread_pool": str(primary_thread_pool),
            "thread_environment": dict(thread_budget["environment"]),
            "cache_caps": {str(key): int(value) for key, value in sorted(cache_caps.items())},
            "checkpoint_every_blocks": int(checkpoint_every_blocks),
            "rss_soft_bytes": int(rss_soft_bytes),
            "rss_hard_bytes": int(rss_hard_bytes),
            "global_rss_hard_bytes": int(global_rss_hard_bytes),
        }
        return cls(**payload, execution_plan_hash=_stable_hash(payload))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def with_runtime_adjustment(self, **changes: Any) -> "FrozenExecutionPlan":
        if self.phase == "E":
            raise FrozenPlanDriftError("Phase E qualification parameters are immutable")
        allowed = {
            "block_size",
            "block_boundaries",
            "pair_batches",
            "heavy_processes",
            "compute_threads",
            "cache_caps",
            "checkpoint_every_blocks",
        }
        unexpected = sorted(set(changes) - allowed)
        if unexpected:
            raise FrozenPlanDriftError(f"unsupported runtime adjustment: {unexpected}")
        payload = self.to_dict()
        payload.pop("execution_plan_hash", None)
        payload.update(changes)
        payload.pop("schema_version", None)
        payload.pop("thread_environment", None)
        payload.pop("primary_thread_pool", None)
        return FrozenExecutionPlan.create(
            primary_thread_pool=self.primary_thread_pool,
            **payload,
        )


def validate_frozen_thread_environment(plan: FrozenExecutionPlan) -> dict[str, Any]:
    observed = {key: os.environ.get(key) for key in plan.thread_environment}
    drift = {
        key: {"expected": expected, "observed": observed.get(key)}
        for key, expected in plan.thread_environment.items()
        if observed.get(key) != expected
    }
    if drift:
        raise FrozenPlanDriftError(f"native thread environment drift: {drift}")
    active = [key for key, value in plan.thread_environment.items() if int(value) > 1]
    if active != [
        {
            "numba": "NUMBA_NUM_THREADS",
            "arrow": "ARROW_NUM_THREADS",
            "omp": "OMP_NUM_THREADS",
            "mkl": "MKL_NUM_THREADS",
            "openblas": "OPENBLAS_NUM_THREADS",
            "numexpr": "NUMEXPR_MAX_THREADS",
            "polars": "POLARS_MAX_THREADS",
        }[plan.primary_thread_pool]
    ]:
        raise FrozenPlanDriftError("exactly one primary native thread pool may exceed one thread")
    return {
        "status": "FROZEN_THREAD_ENVIRONMENT_MATCH",
        "primary_thread_pool": plan.primary_thread_pool,
        "heavy_processes": plan.heavy_processes,
        "global_active_native_compute_threads": plan.heavy_processes * plan.compute_threads,
        "environment": observed,
        "execution_plan_hash": plan.execution_plan_hash,
    }


class RSSGate:
    def __init__(
        self,
        *,
        soft_bytes: int,
        hard_bytes: int,
        global_hard_bytes: int,
        checkpoint: Callable[[str], None],
    ) -> None:
        if not (0 < int(soft_bytes) < int(hard_bytes) <= int(global_hard_bytes)):
            raise ValueError("invalid RSS gate ordering")
        self.soft_bytes = int(soft_bytes)
        self.hard_bytes = int(hard_bytes)
        self.global_hard_bytes = int(global_hard_bytes)
        self.checkpoint = checkpoint

    def periodic_checkpoint(self, *, block_ordinal: int, cadence: int) -> None:
        if int(cadence) <= 0:
            raise ValueError("checkpoint cadence must be positive")
        if int(block_ordinal) % int(cadence) == 0:
            self.checkpoint(f"PERIODIC_BLOCK_{int(block_ordinal):06d}")

    def check(self, snapshot: ResourceSnapshot, *, global_rss_bytes: int) -> None:
        worker = max(int(snapshot.rss_bytes), int(snapshot.peak_rss_bytes))
        global_rss = int(global_rss_bytes)
        if worker >= self.hard_bytes or global_rss >= self.global_hard_bytes:
            raise HardRSSGateError(
                f"hard RSS gate breached: worker={worker}, global={global_rss}"
            )
        if worker >= self.soft_bytes:
            self.checkpoint("SOFT_RSS_GATE")
            raise SoftRSSGateStop(f"soft RSS gate reached after checkpoint: worker={worker}")
