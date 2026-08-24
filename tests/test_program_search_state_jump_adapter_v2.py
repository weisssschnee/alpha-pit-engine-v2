from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from our_system_phase2.services.candidate_program_controls_v1 import (
    construct_matched_control_program_v1,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    CandidateProgramProposalAdapterV0,
    ProgramSourceComponentV0,
)
from our_system_phase2.services.candidate_program_v1 import ProgramCompilerV1
from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.program_search_optimizer_v1 import (
    program_structural_genes_v1,
)
from our_system_phase2.services.program_search_state_jump_adapter_v2 import (
    StateJumpProgramSearchAdapterV2,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"


@pytest.fixture(scope="module")
def adapter_context():
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
        for attempt in range(80):
            pair = grammar.propose(route_id, attempt_index=attempt, seed=8800 + attempt * 13)
            component = ProgramSourceComponentV0(
                role=role,
                primary=dict(pair.primary),
                control=dict(pair.control),
                proposal_id=f"state-jump-adapter-{role}-{attempt}",
                trial_number=attempt,
                sampling_phase="STARTUP_RANDOM",
            )
            if component.component_id in seen:
                continue
            rows.append(component)
            seen.add(component.component_id)
            if len(rows) == 6:
                break
        assert len(rows) >= 4
        pools[role] = rows
    composer = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    sample_components = {"base": pools["base"][0], "event": pools["event"][0]}
    sample_program = composer.compose(
        "BASE_EVENT",
        sample_components["base"],
        event_component=sample_components["event"],
    )
    sample_genes = program_structural_genes_v1(
        program_template_id="BASE_EVENT",
        components=sample_components,
        combination_policy=None,
        program=sample_program,
        compiled=compiler.compile(sample_program),
    )
    # The production availability space uses the sorted union of structural slots.
    ordered_slots = tuple(sorted(sample_genes))
    return registry, composer, compiler, pools, ordered_slots


def test_state_jump_adapter_asks_fresh_normalized_exact_programs(adapter_context):
    _, composer, compiler, pools, slots = adapter_context
    adapter = StateJumpProgramSearchAdapterV2(
        adapter=composer,
        compiler=compiler,
        components_by_role=pools,
        ordered_gene_slots=slots,
        seen_exact_identities=(),
        seed=101,
    )
    asks = adapter.ask(
        checkpoint_id="cp0",
        count=8,
        required_program_template_id="BASE_EVENT",
    )
    assert len(asks) == 8
    assert len({row["exact_identity"] for row in asks}) == 8
    assert all(row["optimizer_arm"] == "SEMANTIC_STATE_JUMP_GENERATOR_V2" for row in asks)
    assert all(row["availability"]["mode"] == "GENERATED_TYPED_PROGRAM_NOT_PREENUMERATED" for row in asks)


def test_state_jump_adapter_materializes_existing_program_contract(adapter_context):
    _, composer, compiler, pools, slots = adapter_context
    adapter = StateJumpProgramSearchAdapterV2(
        adapter=composer,
        compiler=compiler,
        components_by_role=pools,
        ordered_gene_slots=slots,
        seen_exact_identities=(),
        seed=103,
    )
    optimizer_ask = adapter.ask(
        checkpoint_id="cp1",
        count=1,
        required_program_template_id="BASE_EVENT",
    )[0]
    generated = adapter.generated_for_proposal(optimizer_ask["proposal_id"])
    # Recompose from the component bindings exactly as the existing Phase-C
    # schedule builder does; this must reproduce the same semantic identity.
    rebuilt = composer.compose(
        generated.template_id,
        generated.components["base"],
        event_component=generated.components["event"],
        combination_policy=generated.combination_policy,
    )
    assert rebuilt.semantic_program_hash == generated.program.semantic_program_hash
    primary_compiled = compiler.compile(rebuilt)
    matched = construct_matched_control_program_v1(rebuilt)
    control_compiled = compiler.compile(matched.control)
    receipt = composer.build_receipt(
        program_template_id=generated.template_id,
        program=rebuilt,
        components=(generated.components["base"], generated.components["event"]),
        combination_policy=generated.combination_policy,
        batch_id="cp1",
        ask_ordinal=0,
        generation_arm="SEMANTIC_STATE_JUMP_GENERATOR_V2",
    )
    assert receipt.semantic_program_hash == rebuilt.semantic_program_hash
    assert receipt.generation_arm == "SEMANTIC_STATE_JUMP_GENERATOR_V2"
    assert primary_compiled.shared_dag_plan_hash
    assert control_compiled.shared_dag_plan_hash
    assert matched.primary.semantic_program_hash != matched.control.semantic_program_hash


def test_state_jump_adapter_tell_and_snapshot_restore(adapter_context):
    _, composer, compiler, pools, slots = adapter_context
    adapter = StateJumpProgramSearchAdapterV2(
        adapter=composer,
        compiler=compiler,
        components_by_role=pools,
        ordered_gene_slots=slots,
        seen_exact_identities=(),
        seed=107,
    )
    asks = adapter.ask(
        checkpoint_id="cp2",
        count=3,
        required_program_template_id="BASE_EVENT",
    )
    observations = []
    for index, row in enumerate(asks):
        observations.append(
            SimpleNamespace(
                proposal_id=row["proposal_id"],
                exact_identity=row["exact_identity"],
                admission=SimpleNamespace(admitted=True),
                uplift=SimpleNamespace(
                    program_credit={
                        "matched_cumulative_net_return_increment": 0.2 if index < 2 else -0.1,
                        "matched_net_reward_increment": 0.8 if index < 2 else -0.2,
                    }
                ),
            )
        )
    receipt = adapter.tell(observations)
    assert receipt["productive_count"] == 2
    assert receipt["sealed_feedback_used"] is False
    snapshot = adapter.snapshot()
    restored = StateJumpProgramSearchAdapterV2.restore(
        snapshot=snapshot,
        adapter=composer,
        compiler=compiler,
        components_by_role=pools,
    )
    assert restored.snapshot() == snapshot
