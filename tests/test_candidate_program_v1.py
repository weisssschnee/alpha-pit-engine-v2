from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from our_system_phase2.services.candidate_program_v1 import (
    PROGRAM_SCHEMA_VERSION,
    CandidateProgramSpecV1,
    ProgramCompilerV1,
    ProposalLineageV1,
    TypedNodeSpec,
    legacy_candidate_program_v1,
)
from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"
PORTFOLIO = {
    "decoder_id": "TOPK_10_EQUAL",
    "execution": "PRIOR_CLOSE_SIGNAL_NEXT_OPEN_T_PLUS_1",
    "fees": "EXISTING_FROZEN_A_SHARE_FEES",
    "ledger": "EXISTING_CONTINUOUS_BOOK_FINAL_CLOSE_MTM",
}


@pytest.fixture(scope="module")
def legacy_candidate() -> tuple[UnifiedCapabilityRegistry, dict]:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    allowlists = json.loads(ROOT_CONTRACT.read_text(encoding="utf-8"))[
        "route_root_allowlists"
    ]
    pair = CompositionalGrammarV2(
        registry, route_root_allowlist=allowlists
    ).propose("MINUTE_STATIC", attempt_index=0, seed=1729)
    return registry, dict(pair.primary)


def test_legacy_program_reuses_compiler_and_shared_dag_without_identity_drift(
    legacy_candidate: tuple[UnifiedCapabilityRegistry, dict],
) -> None:
    registry, candidate = legacy_candidate
    spec = legacy_candidate_program_v1(candidate, portfolio_contract=PORTFOLIO)
    compiled = ProgramCompilerV1(registry).compile(spec)

    assert spec.schema_version == PROGRAM_SCHEMA_VERSION
    assert compiled.legacy_component_verdicts[0]["candidate_id"] == candidate[
        "candidate_id"
    ]
    assert compiled.legacy_component_verdicts[0]["exact_identity"] == candidate[
        "exact_identity"
    ]
    assert compiled.legacy_component_verdicts[0]["canonical_identity"] == candidate[
        "canonical_identity"
    ]
    assert compiled.legacy_component_verdicts[0]["canonical_expression"] == candidate[
        "canonical_expression"
    ]
    assert compiled.shared_dag_plan_hash


def test_semantic_identity_is_order_invariant_and_lineage_reward_free(
    legacy_candidate: tuple[UnifiedCapabilityRegistry, dict],
) -> None:
    _, candidate = legacy_candidate
    first = legacy_candidate_program_v1(
        {**candidate, "seed": 1, "train_reward": 999.0},
        portfolio_contract=PORTFOLIO,
    )
    second = replace(
        legacy_candidate_program_v1(
            {**candidate, "seed": 999, "train_reward": -10.0},
            portfolio_contract=PORTFOLIO,
        ),
        nodes=tuple(reversed(first.nodes)),
    )

    assert first.semantic_program_hash == second.semantic_program_hash
    assert first.program_id == second.program_id
    assert "train_reward" not in json.dumps(first.to_record(), sort_keys=True)
    assert "\"seed\"" not in json.dumps(first.to_record(), sort_keys=True)
    restored = CandidateProgramSpecV1.from_record(first.to_record())
    assert restored.semantic_program_hash == first.semantic_program_hash
    lineage_a = ProposalLineageV1(
        template_stratum_id="MINUTE_STATIC",
        seed=1,
        attempt=2,
        sampler="FIXED_STRATIFIED_V0",
    ).to_record(semantic_program_hash=first.semantic_program_hash)
    lineage_b = ProposalLineageV1(
        template_stratum_id="MINUTE_STATIC",
        seed=2,
        attempt=3,
        sampler="FIXED_STRATIFIED_V0",
    ).to_record(semantic_program_hash=first.semantic_program_hash)
    assert lineage_a["semantic_program_hash"] == lineage_b["semantic_program_hash"]
    assert lineage_a["proposal_instance_hash"] != lineage_b["proposal_instance_hash"]


def test_clock_or_output_change_changes_program_identity(
    legacy_candidate: tuple[UnifiedCapabilityRegistry, dict],
) -> None:
    _, candidate = legacy_candidate
    baseline = legacy_candidate_program_v1(candidate, portfolio_contract=PORTFOLIO)
    changed_clock = replace(
        baseline,
        joint_clock_contract=replace(
            baseline.joint_clock_contract,
            action_session_policy="DIFFERENT_ACTION_SESSION",
        ),
    )
    changed_output = replace(
        baseline,
        outputs=replace(
            baseline.outputs,
            veto_mask_node_id="eligibility_identity",
        ),
    )
    assert baseline.semantic_program_hash != changed_clock.semantic_program_hash
    assert baseline.semantic_program_hash != changed_output.semantic_program_hash


def test_cycle_and_output_type_mismatch_fail_closed(
    legacy_candidate: tuple[UnifiedCapabilityRegistry, dict],
) -> None:
    registry, candidate = legacy_candidate
    baseline = legacy_candidate_program_v1(candidate, portfolio_contract=PORTFOLIO)
    score = next(node for node in baseline.nodes if node.node_id == "legacy_score")
    eligibility = next(
        node for node in baseline.nodes if node.node_id == "eligibility_identity"
    )
    cyclic = replace(
        baseline,
        nodes=tuple(
            replace(node, input_node_ids=("eligibility_identity",))
            if node.node_id == "legacy_score"
            else replace(node, input_node_ids=("legacy_score",))
            if node.node_id == "eligibility_identity"
            else node
            for node in baseline.nodes
        ),
    )
    with pytest.raises(ValueError, match="cycle"):
        ProgramCompilerV1(registry).compile(cyclic)

    wrong_type = replace(
        baseline,
        nodes=tuple(
            replace(eligibility, output_semantic_type="STOCK_VALUE")
            if node.node_id == eligibility.node_id
            else node
            for node in baseline.nodes
        ),
    )
    with pytest.raises(ValueError, match="incompatible semantic type"):
        ProgramCompilerV1(registry).compile(wrong_type)


def test_unsupported_node_or_unknown_route_provenance_fails_closed() -> None:
    kwargs = {
        "node_id": "bad",
        "input_node_ids": (),
        "parameters": {},
        "output_semantic_type": "STOCK_VALUE",
        "entity_scope": "STOCK",
        "temporal_semantics": {"kind": "POINT_IN_TIME"},
        "observable_clock": "session_close",
        "maturity": "session_close",
        "unit_signature": "dimensionless",
        "support_unit": "stock-session",
    }
    with pytest.raises(ValueError, match="unsupported program node type"):
        TypedNodeSpec(node_type="ARBITRARY_PYTHON", **kwargs)
    with pytest.raises(ValueError, match="unknown component route"):
        TypedNodeSpec(
            node_type="STOCK_FIELD",
            component_route_provenance=("NEW_ROUTE",),
            **kwargs,
        )
