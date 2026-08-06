from __future__ import annotations

import json
from collections import Counter, OrderedDict
from dataclasses import replace
from pathlib import Path

import pytest

from our_system_phase2.services.candidate_program_controls_v1 import (
    construct_matched_control_program_v1,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    PROGRAM_TEMPLATE_COMPONENTS,
    CandidateProgramProposalAdapterV0,
    ProgramProposalReceiptV0,
    ProgramSourceComponentV0,
    source_sampling_phase_v0,
)
from our_system_phase2.services.candidate_program_v1 import (
    ProgramCompilerV1,
    legacy_candidate_program_v1,
)
from our_system_phase2.services.compositional_grammar import (
    CompositionalGrammarV2,
)
from our_system_phase2.services.program_factorized_bandit_v0 import (
    ProgramFactorizedBanditV0,
    generation_arm_v0,
)
from our_system_phase2.services.optuna_tpe_search_adapter import (
    RouteConditionalTPESearchAdapter,
)
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


@pytest.fixture(scope="module")
def program_context():
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    allowlists = json.loads(ROOT_CONTRACT.read_text(encoding="utf-8"))[
        "route_root_allowlists"
    ]
    grammar = CompositionalGrammarV2(
        registry, route_root_allowlist=allowlists
    )
    routes = {
        "base": "SLOW_CROSS_SECTIONAL_LEVEL",
        "temporal": "SLOW_TEMPORAL_CHANGE",
        "market": "MARKET_REGIME_CONDITION",
        "event": "DISCLOSURE_EVENT",
    }
    components = {}
    for role, route_id in routes.items():
        pair = grammar.propose(route_id, attempt_index=0, seed=1729)
        components[role] = ProgramSourceComponentV0(
            role=role,
            primary=dict(pair.primary),
            control=dict(pair.control),
            proposal_id=f"proposal-{role}",
            trial_number=0,
            sampling_phase="STARTUP_RANDOM",
        )
    return registry, components


def _compose(adapter, components, template_id):
    roles = PROGRAM_TEMPLATE_COMPONENTS[template_id]
    return adapter.compose(
        template_id,
        components["base"],
        **{
            f"{role}_component": components[role]
            for role in roles
            if role != "base"
        },
    )


def test_all_eight_templates_compile_and_enhanced_controls_remove_semantics(
    program_context,
) -> None:
    registry, components = program_context
    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    for template_id in PROGRAM_TEMPLATE_COMPONENTS:
        program = _compose(adapter, components, template_id)
        compiled = compiler.compile(program)
        assert compiled.shared_dag_plan_hash
        if template_id == "BASE":
            legacy = legacy_candidate_program_v1(
                components["base"].primary,
                portfolio_contract=adapter.portfolio_contract,
            )
            assert program.to_record() == legacy.to_record()
            continue
        pair = construct_matched_control_program_v1(program)
        control = compiler.compile(pair.control)
        assert pair.primary.semantic_program_hash != pair.control.semantic_program_hash
        assert control.shared_dag_plan_hash
        assert set(control.component_clock_requirements) == {
            f"component_{role}"
            for role in PROGRAM_TEMPLATE_COMPONENTS[template_id]
        }


def test_receipt_is_replayable_and_provenance_does_not_change_program_identity(
    program_context,
) -> None:
    registry, components = program_context
    adapter = CandidateProgramProposalAdapterV0(registry)
    program = _compose(adapter, components, "BASE_TEMPORAL_MARKET_EVENT")
    roles = PROGRAM_TEMPLATE_COMPONENTS["BASE_TEMPORAL_MARKET_EVENT"]
    first = adapter.build_receipt(
        program_template_id="BASE_TEMPORAL_MARKET_EVENT",
        program=program,
        components=[components[role] for role in roles],
        combination_policy=None,
        batch_id="batch_001",
        ask_ordinal=7,
        generation_arm="UNIFORM_FRESH",
    )
    changed_components = dict(components)
    changed_components["temporal"] = replace(
        components["temporal"],
        proposal_id="another-proposal-path",
        trial_number=9,
    )
    second_program = _compose(
        adapter, changed_components, "BASE_TEMPORAL_MARKET_EVENT"
    )
    second = adapter.build_receipt(
        program_template_id="BASE_TEMPORAL_MARKET_EVENT",
        program=second_program,
        components=[changed_components[role] for role in roles],
        combination_policy=None,
        batch_id="batch_001",
        ask_ordinal=8,
        generation_arm="NOVELTY_RESERVE",
    )
    assert first.semantic_program_hash == second.semantic_program_hash
    assert first.to_record()["proposal_receipt_sha256"] != second.to_record()[
        "proposal_receipt_sha256"
    ]
    assert ProgramProposalReceiptV0.from_record(first.to_record()) == first


def test_component_forgery_and_template_role_drift_fail_closed(program_context) -> None:
    _, components = program_context
    base = components["base"]
    forged_control = {**base.control, "candidate_id": "forged-control"}
    with pytest.raises(ValueError, match="matched-control identity mismatch"):
        replace(base, control=forged_control)
    forged_pair = {**base.control, "pair_id": "forged-pair"}
    with pytest.raises(ValueError, match="pair mismatch"):
        replace(base, control=forged_pair)
    with pytest.raises(ValueError, match="not legal for temporal"):
        ProgramSourceComponentV0(
            role="temporal",
            primary=base.primary,
            control=base.control,
            proposal_id="forged-role",
            trial_number=0,
            sampling_phase="STARTUP_RANDOM",
        )


def test_source_sampling_phase_preserves_existing_startup_threshold() -> None:
    assert source_sampling_phase_v0(
        optimizer_ask_kind="AVAILABILITY_FIXED_ENQUEUED",
        trial_number=999,
        n_startup_trials=512,
    ) == "AVAILABILITY_FIXED"
    assert source_sampling_phase_v0(
        optimizer_ask_kind="TPE_NATIVE_DRAW",
        trial_number=511,
        n_startup_trials=512,
    ) == "STARTUP_RANDOM"
    assert source_sampling_phase_v0(
        optimizer_ask_kind="TPE_NATIVE_DRAW",
        trial_number=512,
        n_startup_trials=512,
    ) == "TPE_GUIDED"

    adapter = RouteConditionalTPESearchAdapter(
        route_id="TEST_ROUTE",
        lane_spaces=OrderedDict(
            {
                "test.single": {
                    "ordered_categories_by_slot": OrderedDict(
                        [
                            ("skeleton_id", ["test.single"]),
                            ("primary_field_id", ["a", "b"]),
                        ]
                    )
                }
            }
        ),
        seed=11,
        n_startup_trials=2,
        n_ei_candidates=8,
        multivariate=False,
        group=False,
    )
    asked = adapter.ask_population(checkpoint_id="checkpoint_001", count=3)
    assert [row["source_sampling_phase"] for row in asked] == [
        "STARTUP_RANDOM",
        "STARTUP_RANDOM",
        "TPE_GUIDED",
    ]


def test_factorized_bandit_uses_fixed_40_40_20_arms_and_exact_replay(
    program_context,
) -> None:
    registry, components = program_context
    adapter = CandidateProgramProposalAdapterV0(registry)
    program = _compose(adapter, components, "BASE_TEMPORAL")
    receipt = adapter.build_receipt(
        program_template_id="BASE_TEMPORAL",
        program=program,
        components=[components["base"], components["temporal"]],
        combination_policy=None,
        batch_id="batch_001",
        ask_ordinal=0,
        generation_arm="UNIFORM_FRESH",
    )
    assert Counter(generation_arm_v0(index) for index in range(10)) == {
        "UNIFORM_FRESH": 4,
        "FACTORIZED_EXPLOIT": 4,
        "NOVELTY_RESERVE": 2,
    }
    bandit = ProgramFactorizedBanditV0("campaign-1")
    for index in range(3):
        bandit.observe(receipt, matched_increment=0.1 + index * 0.01)
    factor = f"template:{receipt.program_template_id}"
    assert not bandit.factor_score(factor)["exploit_eligible"]
    bandit.observe(receipt, matched_increment=-0.1)
    assert bandit.factor_score(factor)["exploit_eligible"]
    snapshot = bandit.snapshot()
    restored = ProgramFactorizedBanditV0.restore(snapshot)
    assert restored.snapshot() == snapshot
    with pytest.raises(ValueError, match="not bandit-reward eligible"):
        base_program = _compose(adapter, components, "BASE")
        base_receipt = adapter.build_receipt(
            program_template_id="BASE",
            program=base_program,
            components=[components["base"]],
            combination_policy=None,
            batch_id="batch_001",
            ask_ordinal=0,
            generation_arm="UNIFORM_FRESH",
        )
        bandit.observe(base_receipt, matched_increment=1.0)
