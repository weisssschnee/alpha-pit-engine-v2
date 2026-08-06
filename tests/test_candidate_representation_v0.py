from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from our_system_phase2.services.candidate_representation_v0 import (
    BROAD_EVENT_TEMPLATE_ID,
    CANDIDATE_REPRESENTATION_VERSION,
    TEMPLATE_CONTRACTS_V0,
    CandidateSpecV0,
)
from our_system_phase2.services.fixed_stratified_candidate_sampling import (
    FIXED_STRATIFIED_POLICY_ID,
    build_fixed_stratified_plan_v0,
    generate_fixed_stratified_epoch_v0,
    iter_fixed_stratified_attempts_v0,
    verify_fixed_stratified_plan_v0,
)
from our_system_phase2.services.compositional_grammar import (
    CompositionalGrammarV2,
)
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    stable_hash,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = (
    REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"
)


def _registry_and_allowlists() -> tuple[
    UnifiedCapabilityRegistry, dict[str, list[str]]
]:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    allowlists = json.loads(ROOT_CONTRACT.read_text(encoding="utf-8"))[
        "route_root_allowlists"
    ]
    return registry, allowlists


def _broad_inventory_hash(registry: UnifiedCapabilityRegistry) -> str:
    return stable_hash(
        [
            row.to_dict()
            for row in registry.fields_for_route(BROAD_EVENT_TEMPLATE_ID)
        ]
    )


def test_v0_representation_covers_eight_registry_templates_without_new_authority() -> None:
    assert tuple(TEMPLATE_CONTRACTS_V0) == ROUTE_IDS
    assert len(TEMPLATE_CONTRACTS_V0) == 8
    assert all(
        contract.template_id == contract.route_id
        for contract in TEMPLATE_CONTRACTS_V0.values()
    )
    assert TEMPLATE_CONTRACTS_V0[BROAD_EVENT_TEMPLATE_ID].sampling_kind == (
        "FROZEN_REPLAY_INVENTORY"
    )
    assert not hasattr(
        TEMPLATE_CONTRACTS_V0[BROAD_EVENT_TEMPLATE_ID],
        "new_generation_allowed",
    )


def test_candidate_spec_hash_is_reward_free_and_lineage_is_separate() -> None:
    registry, allowlists = _registry_and_allowlists()
    pair = CompositionalGrammarV2(
        registry, route_root_allowlist=allowlists
    ).propose("MINUTE_STATIC", attempt_index=0, seed=1729)
    source = dict(pair.primary)
    source.update(
        {
            "train_reward": 10.0,
            "validation_rank_ic_mean": 0.99,
            "optimizer_reward": 12.0,
            "portfolio_behavior_family_id": "post-evaluation-only",
        }
    )
    first = CandidateSpecV0.from_candidate_row(
        source,
        attempt_id="attempt-a",
        route_attempt_index=0,
        sampler_id=FIXED_STRATIFIED_POLICY_ID,
    )
    second = CandidateSpecV0.from_candidate_row(
        {
            **source,
            "train_reward": -999.0,
            "validation_rank_ic_mean": -0.99,
            "optimizer_reward": None,
            "portfolio_behavior_family_id": "different-post-evaluation-row",
        },
        attempt_id="attempt-b",
        route_attempt_index=100,
        sampler_id="different-lineage",
    )

    assert first.spec_hash == second.spec_hash
    assert first.proposal_hash != second.proposal_hash
    assert first.schema_version == CANDIDATE_REPRESENTATION_VERSION
    record = first.to_record()
    assert record["template_id"] == record["route_id"] == "MINUTE_STATIC"
    assert "train_reward" not in json.dumps(record, sort_keys=True)
    assert "validation_rank_ic_mean" not in json.dumps(record, sort_keys=True)
    assert "portfolio_behavior_family_id" not in json.dumps(
        record, sort_keys=True
    )
    reconstructed = CandidateSpecV0.from_record(record)
    assert reconstructed.spec_hash == first.spec_hash
    assert reconstructed.proposal_hash == first.proposal_hash


def test_fixed_plan_is_balanced_self_hashed_and_has_no_credit_or_spillover() -> None:
    registry, _ = _registry_and_allowlists()
    quotas = {template_id: 4 for template_id in ROUTE_IDS}
    plan = build_fixed_stratified_plan_v0(
        template_attempt_quotas=quotas,
        seeds=(1729, 2718),
        registry_hash=registry.registry_hash,
        root_scope_hash="a" * 64,
        frozen_inventory_hashes={BROAD_EVENT_TEMPLATE_ID: "b" * 64},
    )
    attempts = list(iter_fixed_stratified_attempts_v0(plan))

    assert plan["policy_id"] == FIXED_STRATIFIED_POLICY_ID
    assert plan["shared_tpe_study"] is False
    assert plan["optimizer_feedback_accessed"] is False
    assert plan["dynamic_budget_reallocation_allowed"] is False
    assert plan["underfill_spillover_allowed"] is False
    assert len(attempts) == 32
    assert {
        template_id: sum(
            row["template_id"] == template_id for row in attempts
        )
        for template_id in ROUTE_IDS
    } == quotas
    assert all(row["template_id"] == row["route_id"] for row in attempts)
    assert len({row["attempt_id"] for row in attempts}) == len(attempts)


def test_rehashed_plan_cannot_drift_seed_or_template_contract() -> None:
    registry, _ = _registry_and_allowlists()
    plan = build_fixed_stratified_plan_v0(
        template_attempt_quotas={template_id: 2 for template_id in ROUTE_IDS},
        seeds=(1729, 2718),
        registry_hash=registry.registry_hash,
        root_scope_hash="a" * 64,
        frozen_inventory_hashes={
            BROAD_EVENT_TEMPLATE_ID: _broad_inventory_hash(registry)
        },
    )
    plan["seeds"] = [1729, 1729]
    plan["plan_hash"] = stable_hash(
        {key: value for key, value in plan.items() if key != "plan_hash"}
    )

    with pytest.raises(ValueError, match="seeds are not frozen uniquely"):
        verify_fixed_stratified_plan_v0(plan)


def test_broad_event_positive_quota_requires_frozen_inventory_binding() -> None:
    registry, _ = _registry_and_allowlists()
    quotas = {template_id: 1 for template_id in ROUTE_IDS}
    with pytest.raises(ValueError, match="frozen inventory"):
        build_fixed_stratified_plan_v0(
            template_attempt_quotas=quotas,
            seeds=(1729,),
            registry_hash=registry.registry_hash,
            root_scope_hash="a" * 64,
        )


def test_zero_financial_v0_epoch_materializes_all_eight_templates() -> None:
    registry, allowlists = _registry_and_allowlists()
    plan = build_fixed_stratified_plan_v0(
        template_attempt_quotas={template_id: 1 for template_id in ROUTE_IDS},
        seeds=(1729,),
        registry_hash=registry.registry_hash,
        root_scope_hash="a" * 64,
        frozen_inventory_hashes={
            BROAD_EVENT_TEMPLATE_ID: _broad_inventory_hash(registry)
        },
    )
    result = generate_fixed_stratified_epoch_v0(
        registry,
        plan=plan,
        route_root_allowlist=allowlists,
    )

    assert result.summary["scheduled_attempts"] == 8
    assert result.summary["attempted"] == 8
    assert result.summary["economic_evaluator_accessed"] is False
    assert result.summary["optimizer_feedback_accessed"] is False
    assert result.summary["shared_tpe_credit"] is False
    assert result.summary["dynamic_budget_reallocation_allowed"] is False
    assert result.summary["sealed_data_read_count"] == 0
    assert {row["template_id"] for row in result.waterfall} == set(ROUTE_IDS)
    assert len(result.candidate_specs) == 16
    assert len(result.candidate_rows) == 16
    assert all(
        row["template_id"] == row["route_id"]
        for row in result.candidate_specs
    )
    assert all(
        row["schema_version"] == CANDIDATE_REPRESENTATION_VERSION
        for row in result.candidate_specs
    )
    assert all(row["spec_hash"] for row in result.candidate_specs)
    assert all(row["proposal_hash"] for row in result.candidate_specs)
    assert all(
        row["candidate_spec_v0_hash"]
        and row["optimizer_feedback_eligible"] is False
        and row["dynamic_budget_reallocation_eligible"] is False
        for row in result.candidate_rows
    )
    assert all(row["control_valid"] for row in result.waterfall)
    assert result.summary["legal_control_valid_attempts"] == sum(
        row["legal_control_valid"] for row in result.waterfall
    )
    assert all(
        row["production_evidence_state"]
        == "NOT_EVALUATED_ZERO_FINANCIAL_PREFLIGHT"
        and row["pair_evaluated"] is None
        and row["development_productive"] is None
        for row in result.waterfall
    )


def test_epoch_rejects_a_tampered_broad_event_inventory_binding() -> None:
    registry, allowlists = _registry_and_allowlists()
    plan = build_fixed_stratified_plan_v0(
        template_attempt_quotas={template_id: 1 for template_id in ROUTE_IDS},
        seeds=(1729,),
        registry_hash=registry.registry_hash,
        root_scope_hash="a" * 64,
        frozen_inventory_hashes={BROAD_EVENT_TEMPLATE_ID: "b" * 64},
    )

    with pytest.raises(ValueError, match="inventory binding mismatch"):
        generate_fixed_stratified_epoch_v0(
            registry,
            plan=plan,
            route_root_allowlist=allowlists,
        )


def test_joint_legal_control_counter_is_the_attempt_intersection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, allowlists = _registry_and_allowlists()

    class CrossFailureGrammar:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def propose(
            self, route_id: str, *, attempt_index: int, seed: int
        ) -> SimpleNamespace:
            del attempt_index, seed
            primary_legal = ROUTE_IDS.index(route_id) < 4
            return SimpleNamespace(
                primary={
                    "candidate_id": f"primary-{route_id}",
                    "pair_id": f"pair-{route_id}",
                    "legal": primary_legal,
                },
                control={"legal": not primary_legal},
            )

    monkeypatch.setattr(
        "our_system_phase2.services.fixed_stratified_candidate_sampling."
        "CompositionalGrammarV2",
        CrossFailureGrammar,
    )
    plan = build_fixed_stratified_plan_v0(
        template_attempt_quotas={template_id: 1 for template_id in ROUTE_IDS},
        seeds=(1729,),
        registry_hash=registry.registry_hash,
        root_scope_hash="a" * 64,
        frozen_inventory_hashes={
            BROAD_EVENT_TEMPLATE_ID: _broad_inventory_hash(registry)
        },
    )
    result = generate_fixed_stratified_epoch_v0(
        registry,
        plan=plan,
        route_root_allowlist=allowlists,
    )

    assert sum(row["primary_legal"] for row in result.waterfall) == 4
    assert sum(row["control_valid"] for row in result.waterfall) == 4
    assert result.summary["legal_control_valid_attempts"] == 0
