from __future__ import annotations

import json
from pathlib import Path

from scripts.run_cn_program_optimizer_large_fresh_v3 import _checkpoint_arm_v3
from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import ENHANCED_TEMPLATES
from our_system_phase2.services.program_optimizer_large_fresh_v3 import (
    ARMS,
    LargeFreshProgramBanditV3,
)
from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    UNIFORM_CONTROL,
    ProgramOptimizerObservationV1,
    program_availability_entries_v1,
)
from our_system_phase2.services.program_search_primitive_credit_v1 import (
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import ProgramUpliftCredit


REPO = Path(__file__).resolve().parents[1]
STATS = REPO / "runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json"
STAGE_D = REPO / "runtime/run_plans/cn_program_stage_d_primitive_confirmation_prefreeze_v1.json"


def _read(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _fixture():
    stats = _read(STATS)
    stage_d = _read(STAGE_D)
    candidates = list(stage_d["cohort"]["candidates"])
    entries = program_availability_entries_v1(
        [{"genes": dict(row["program_genes"])} for row in candidates]
    )
    metadata = {}
    for candidate, entry in zip(candidates, entries, strict=True):
        components = {}
        for role, raw in sorted(dict(candidate["components"]).items()):
            binding = dict(raw)
            components[role] = {
                "component_id": str(binding["component_id"]),
                "route_id": str(binding["route_id"]),
                "skeleton_id": str(entry.genes[f"{role}__skeleton_id"]),
            }
        metadata[entry.exact_identity] = {
            "template_id": str(candidate["template_id"]),
            "components": components,
            "tie_break_identity": str(candidate["exact_identity"]),
        }
    primitive_config = {
        "metadata_by_exact_identity": metadata,
        "primitive_stats": stats,
        "primitive_stats_payload_sha256": stats["stats_payload_sha256"],
    }
    evolution_config = {
        "warmup": 32,
        "tournament_size": 4,
        "population_limit": 256,
        "template_cell_limit": 64,
        "gene_mutation_probability": 0.55,
        "skeleton_mutation_probability": 0.25,
        "crossover_probability": 0.20,
        "minimum_mutated_factors": 1,
        "maximum_mutated_factors": 3,
        "duplicate_resample_limit": 64,
    }
    seeds = {
        UNIFORM_CONTROL: 1,
        PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1: 2,
        CATALOG_TYPED_EVOLUTION_PROGRAM_V2: 3,
    }
    return candidates, entries, primitive_config, evolution_config, seeds


def _positive_observation(ask: dict) -> ProgramOptimizerObservationV1:
    exact = str(ask["exact_identity"])
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=f"record::{exact}",
        pair_id=f"pair::{exact}",
        program_id=f"program::{exact}",
        control_program_id=f"control::{exact}",
        admitted=True,
        failure_reasons=(),
        metrics={},
    )
    uplift = ProgramUpliftCredit(
        record_payload_sha256=f"record::{exact}",
        pair_id=f"pair::{exact}",
        program_id=f"program::{exact}",
        control_program_id=f"control::{exact}",
        program_credit={
            "matched_cumulative_net_return_increment": 1.0,
            "matched_net_reward_increment": 1.0,
        },
    )
    return ProgramOptimizerObservationV1(
        proposal_id=str(ask["proposal_id"]),
        exact_identity=exact,
        admission=admission,
        uplift=uplift,
    )


def test_v3_scheduler_is_five_primitive_one_uniform_one_evolution() -> None:
    for macro in range(14):
        arms = [
            _checkpoint_arm_v3(macro, template_index)
            for template_index in range(len(ENHANCED_TEMPLATES))
        ]
        assert arms.count(PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1) == 5
        assert arms.count(UNIFORM_CONTROL) == 1
        assert arms.count(CATALOG_TYPED_EVOLUTION_PROGRAM_V2) == 1
        assert arms[macro % len(ENHANCED_TEMPLATES)] == UNIFORM_CONTROL
        assert arms[(macro + 3) % len(ENHANCED_TEMPLATES)] == CATALOG_TYPED_EVOLUTION_PROGRAM_V2


def test_v3_bandit_has_confirmed_primary_and_replays_exactly() -> None:
    _candidates, entries, primitive_config, evolution_config, seeds = _fixture()
    state = LargeFreshProgramBanditV3(
        campaign_id="V3_TEST",
        entries_by_arm={arm: entries for arm in ARMS},
        seeds=seeds,
        primitive_config=primitive_config,
        evolution_config=evolution_config,
    )
    assert set(state.adapters) == set(ARMS)
    snapshot = state.snapshot()
    assert snapshot["primary_arm"] == PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
    assert snapshot["uniform_reserve_retained"] is True
    assert snapshot["typed_evolution_challenger_retained"] is True
    assert snapshot["stage_c_or_stage_d_financial_observations_imported"] is False
    restored = LargeFreshProgramBanditV3.restore(
        snapshot,
        entries_by_arm={arm: entries for arm in ARMS},
        primitive_config=primitive_config,
        evolution_config=evolution_config,
        expected_campaign_id="V3_TEST",
    )
    assert restored.snapshot() == snapshot


def test_v3_primitive_preview_commit_tell_keeps_prior_frozen() -> None:
    _candidates, entries, primitive_config, evolution_config, seeds = _fixture()
    state = LargeFreshProgramBanditV3(
        campaign_id="V3_TEST",
        entries_by_arm={arm: entries for arm in ARMS},
        seeds=seeds,
        primitive_config=primitive_config,
        evolution_config=evolution_config,
    )
    template = "BASE_EVENT"
    eligible = [
        entry.exact_identity
        for entry in entries
        if str(entry.genes["program_template_id"]) == template
    ]
    kwargs = dict(
        arm=PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
        checkpoint_id="C1",
        count=24,
        required_program_template_id=template,
        eligible_exact_identities=eligible,
        batch_group_constraint=None,
    )
    before = state.snapshot()
    preview = state.ask(**kwargs)
    assert state.snapshot() == before
    state.commit_ask(expected_asks=preview, **kwargs)
    receipt = state.tell(
        arm=PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
        observations=[_positive_observation(row) for row in preview],
    )
    assert receipt["optimizer_feedback_applied"] is False
    assert receipt["prior_stats_mutated"] is False
    after = state.snapshot()
    primitive_metadata = after["arms"][PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1][
        "optimizer_metadata"
    ]
    assert primitive_metadata["online_feedback_applied"] is False
    assert primitive_metadata["stage_c_or_stage_d_feedback_imported"] is False
