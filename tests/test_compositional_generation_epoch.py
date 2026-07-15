from __future__ import annotations

from pathlib import Path

from our_system_phase2.services.compositional_generation_epoch import (
    build_compositional_generation_epoch,
    select_structural_preadmission,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry"
    / "unified_capability_registry.json"
)
POLICIES = (
    "stratified_typed_random",
    "diversity_preserving_cem",
    "uct_ucb_structural_search",
    "evolutionary_mutation",
)
SEEDS = (1729, 2718, 31415, 65537)


def test_generation_epoch_is_globally_exact_deduplicated_and_feedback_dark() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    quotas = {
        route_id: 64
        for route_id in registry.route_contracts
        if route_id != "BROAD_EVENT_FROZEN_ENTRY"
    }
    result = build_compositional_generation_epoch(
        registry,
        route_attempt_quotas=quotas,
        policies=POLICIES,
        seeds=SEEDS,
    )

    assert result.summary["proposal_attempts"] == 64 * 7
    assert result.summary["economic_evaluator_accessed"] is False
    assert result.summary["access_roles"] == []
    assert result.summary["broad_event_new_search_attempts"] == 0
    assert len({row["exact_identity"] for row in result.unique_pairs}) == len(
        result.unique_pairs
    )
    assert all(row["first_visit"] in {True, False} for row in result.ledger)
    assert all(row["strict_call"] is False for row in result.ledger)
    assert all(row["policy_state_hash"] for row in result.ledger)
    assert all(row["control"]["vote_policy"] == "CONTROL_NO_SEPARATE_VOTE" for row in result.unique_pairs)


def test_structural_preadmission_is_deterministic_and_performance_blind() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    quotas = {
        route_id: 128
        for route_id in registry.route_contracts
        if route_id != "BROAD_EVENT_FROZEN_ENTRY"
    }
    result = build_compositional_generation_epoch(
        registry,
        route_attempt_quotas=quotas,
        policies=POLICIES,
        seeds=SEEDS,
    )

    selected_a = select_structural_preadmission(result.unique_pairs, maximum_pairs=200)
    selected_b = select_structural_preadmission(list(reversed(result.unique_pairs)), maximum_pairs=200)

    assert [row["exact_identity"] for row in selected_a] == [
        row["exact_identity"] for row in selected_b
    ]
    assert len(selected_a) <= 200
    assert all(row["admission_reward_accessed"] is False for row in selected_a)
    assert len({row["route_id"] for row in selected_a}) == 7
    assert len({row["policy_id"] for row in selected_a}) == 4
    assert len({row["seed"] for row in selected_a}) == 4


def test_exact_identity_owner_is_independent_of_policy_and_seed_iteration_order() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    quotas = {
        route_id: 64
        for route_id in registry.route_contracts
        if route_id != "BROAD_EVENT_FROZEN_ENTRY"
    }
    forward = build_compositional_generation_epoch(
        registry,
        route_attempt_quotas=quotas,
        policies=POLICIES,
        seeds=SEEDS,
    )
    reversed_order = build_compositional_generation_epoch(
        registry,
        route_attempt_quotas=quotas,
        policies=tuple(reversed(POLICIES)),
        seeds=tuple(reversed(SEEDS)),
    )

    def owners(result: object) -> dict[str, tuple[str, int]]:
        return {
            row["exact_identity"]: (row["policy_id"], row["seed"])
            for row in getattr(result, "unique_pairs")
        }

    assert owners(forward) == owners(reversed_order)
