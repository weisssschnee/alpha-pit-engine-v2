from __future__ import annotations

import os

import pytest

from our_system_phase2.services.phase3cm_streaming_resource_contract import (
    FrozenExecutionPlan,
    FrozenPlanDriftError,
    HardRSSGateError,
    RSSGate,
    SoftRSSGateStop,
    validate_frozen_thread_environment,
)
from our_system_phase2.services.phase3cm_streaming_telemetry import ResourceSnapshot


def _plan() -> FrozenExecutionPlan:
    return FrozenExecutionPlan.create(
        phase="E",
        block_size=5,
        block_boundaries=(("2024-01-02", "2024-01-08"),),
        pair_batches=(("p0", "p1"),),
        heavy_processes=1,
        compute_threads=16,
        primary_thread_pool="numba",
        cache_caps={"dag": 1024, "raw": 2048},
        checkpoint_every_blocks=1,
        rss_soft_bytes=100,
        rss_hard_bytes=200,
        global_rss_hard_bytes=300,
    )


def test_phase_e_plan_is_content_addressed_and_rejects_adaptation() -> None:
    plan = _plan()
    assert plan.execution_plan_hash == _plan().execution_plan_hash
    with pytest.raises(FrozenPlanDriftError):
        plan.with_runtime_adjustment(block_size=4)


def test_thread_environment_must_match_single_primary_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan()
    for key, value in plan.thread_environment.items():
        monkeypatch.setenv(key, value)
    observed = validate_frozen_thread_environment(plan)
    assert observed["status"] == "FROZEN_THREAD_ENVIRONMENT_MATCH"
    monkeypatch.setenv("OMP_NUM_THREADS", "8")
    with pytest.raises(FrozenPlanDriftError):
        validate_frozen_thread_environment(plan)


def test_rss_gate_checkpoints_at_soft_and_fails_hard() -> None:
    checkpoints: list[str] = []
    gate = RSSGate(
        soft_bytes=100,
        hard_bytes=200,
        global_hard_bytes=300,
        checkpoint=lambda reason: checkpoints.append(reason),
    )
    gate.check(ResourceSnapshot(0, 0, 99, 99, 0), global_rss_bytes=99)
    with pytest.raises(SoftRSSGateStop):
        gate.check(ResourceSnapshot(0, 0, 100, 100, 0), global_rss_bytes=100)
    assert checkpoints == ["SOFT_RSS_GATE"]
    with pytest.raises(HardRSSGateError):
        gate.check(ResourceSnapshot(0, 0, 201, 201, 0), global_rss_bytes=201)
    assert checkpoints == ["SOFT_RSS_GATE"]


def test_periodic_checkpoint_occurs_before_rss_gate() -> None:
    checkpoints: list[str] = []
    gate = RSSGate(
        soft_bytes=100,
        hard_bytes=200,
        global_hard_bytes=300,
        checkpoint=lambda reason: checkpoints.append(reason),
    )
    gate.periodic_checkpoint(block_ordinal=1, cadence=1)
    assert checkpoints == ["PERIODIC_BLOCK_000001"]
