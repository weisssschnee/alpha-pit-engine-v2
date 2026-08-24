from __future__ import annotations

import json
from pathlib import Path

import pytest

from our_system_phase2.services.candidate_program_proposal_v0 import (
    CandidateProgramProposalAdapterV0,
    ProgramSourceComponentV0,
)
from our_system_phase2.services.candidate_program_v1 import ProgramCompilerV1
from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.program_search_state_jump_generator_v2 import (
    SEMANTIC_STATE_JUMP_GENERATOR_V2,
    ProgramStateJumpSearchMemoryV2,
    SemanticStateJumpProgramGeneratorV2,
)
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"


@pytest.fixture(scope="module")
def generator_context():
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    allowlists = json.loads(ROOT_CONTRACT.read_text(encoding="utf-8"))["route_root_allowlists"]
    grammar = CompositionalGrammarV2(registry, route_root_allowlist=allowlists)
    routes = {
        "base": "SLOW_CROSS_SECTIONAL_LEVEL",
        "temporal": "SLOW_TEMPORAL_CHANGE",
        "market": "MARKET_REGIME_CONDITION",
        "event": "DISCLOSURE_EVENT",
    }
    pools = {}
    for role, route_id in routes.items():
        rows = []
        seen = set()
        for attempt in range(64):
            pair = grammar.propose(route_id, attempt_index=attempt, seed=1729 + 17 * attempt)
            component = ProgramSourceComponentV0(
                role=role,
                primary=dict(pair.primary),
                control=dict(pair.control),
                proposal_id=f"proposal-{role}-{attempt}",
                trial_number=attempt,
                sampling_phase="STARTUP_RANDOM",
            )
            if component.component_id in seen:
                continue
            rows.append(component)
            seen.add(component.component_id)
            if len(rows) == 5:
                break
        assert len(rows) >= 3, role
        pools[role] = rows
    return registry, pools


def test_generator_v2_emits_new_legal_compiled_programs(generator_context):
    registry, pools = generator_context
    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    generator = SemanticStateJumpProgramGeneratorV2(
        adapter=adapter,
        components_by_role=pools,
        seed=29,
    )
    hashes = set()
    for ordinal in range(18):
        generated = generator.propose(
            batch_id="state-jump-smoke",
            ask_ordinal=ordinal,
            template_id="BASE_TEMPORAL_MARKET_EVENT",
        )
        assert generated.receipt.generation_arm == SEMANTIC_STATE_JUMP_GENERATOR_V2
        assert generated.program.semantic_program_hash not in hashes
        hashes.add(generated.program.semantic_program_hash)
        compiled = compiler.compile(generated.program)
        assert compiled.shared_dag_plan_hash
    assert len(hashes) == 18


def test_exploration_tie_break_spreads_across_least_observed_components(generator_context):
    registry, pools = generator_context
    generator = SemanticStateJumpProgramGeneratorV2(
        adapter=CandidateProgramProposalAdapterV0(registry),
        components_by_role=pools,
        seed=20260824,
    )
    chosen = {
        generator._choose_component("base", exploration=True).component_id
        for _ in range(16)
    }
    assert len(chosen) >= 2


def test_generator_can_jump_from_productive_elite(generator_context):
    registry, pools = generator_context
    adapter = CandidateProgramProposalAdapterV0(registry)
    generator = SemanticStateJumpProgramGeneratorV2(
        adapter=adapter,
        components_by_role=pools,
        seed=7,
        operation_priors={
            "FRESH_RECOMPOSE": 0.01,
            "ROLE_REPLACE": 0.01,
            "MULTI_ROLE_JUMP": 0.96,
            "POLICY_JUMP": 0.01,
            "HOMOLOGOUS_RECOMBINE": 0.01,
        },
    )
    first = generator.propose(batch_id="elite", ask_ordinal=0, template_id="BASE_EVENT")
    generator.observe(
        first,
        admitted=True,
        productive=True,
        matched_return_increment=0.4,
        matched_reward_increment=1.2,
        behavior_identity="behavior-a",
    )
    second = generator.propose(batch_id="elite", ask_ordinal=1, template_id="BASE_EVENT")
    assert second.parent_semantic_hashes == (first.program.semantic_program_hash,)
    assert second.operation == "MULTI_ROLE_JUMP"
    assert len(second.changed_slots) == 2
    assert second.program.semantic_program_hash != first.program.semantic_program_hash


def test_search_memory_marks_repeated_dead_region_and_roundtrips(generator_context):
    registry, pools = generator_context
    adapter = CandidateProgramProposalAdapterV0(registry)
    generator = SemanticStateJumpProgramGeneratorV2(
        adapter=adapter,
        components_by_role=pools,
        seed=11,
    )
    generated = generator.propose(batch_id="dead", ask_ordinal=0, template_id="BASE_MARKET")
    # Repeated development-only failures at the same structural region create a
    # deny prior without reading any sealed domain.
    for _ in range(4):
        generator.memory.observe(
            generated,
            admitted=False,
            productive=False,
            blocked=True,
            redundant=False,
        )
    assert generator.memory.region_dead(generated.region_key)
    snapshot = generator.memory.snapshot()
    restored = ProgramStateJumpSearchMemoryV2.restore(snapshot)
    assert restored.snapshot() == snapshot
    assert restored.region_dead(generated.region_key)


def test_generator_diagnostics_never_claim_sealed_feedback(generator_context):
    registry, pools = generator_context
    generator = SemanticStateJumpProgramGeneratorV2(
        adapter=CandidateProgramProposalAdapterV0(registry),
        components_by_role=pools,
        seed=31,
    )
    generated = generator.propose(batch_id="diag", ask_ordinal=0, template_id="BASE_TEMPORAL")
    generator.observe(
        generated,
        admitted=True,
        productive=False,
        matched_return_increment=-0.1,
        matched_reward_increment=-0.2,
    )
    snapshot = generator.memory.snapshot()
    assert snapshot["validation_feedback_allowed"] is False
    assert snapshot["holdout_feedback_allowed"] is False
    assert snapshot["forward_feedback_allowed"] is False
    assert generator.diagnostics()["memory_observations"] == 1


def test_generator_full_snapshot_restore_is_exact(generator_context):
    registry, pools = generator_context
    adapter = CandidateProgramProposalAdapterV0(registry)
    generator = SemanticStateJumpProgramGeneratorV2(
        adapter=adapter,
        components_by_role=pools,
        seed=47,
    )
    first = generator.propose(batch_id="snap", ask_ordinal=0, template_id="BASE_EVENT")
    generator.observe(
        first,
        admitted=True,
        productive=True,
        matched_return_increment=0.2,
        matched_reward_increment=0.7,
        behavior_identity="snap-behavior",
    )
    generator.propose(batch_id="snap", ask_ordinal=1, template_id="BASE_EVENT")
    snapshot = generator.snapshot()
    restored = SemanticStateJumpProgramGeneratorV2.restore(
        adapter=adapter,
        components_by_role=pools,
        snapshot=snapshot,
    )
    assert restored.snapshot() == snapshot
    assert restored.diagnostics() == generator.diagnostics()
