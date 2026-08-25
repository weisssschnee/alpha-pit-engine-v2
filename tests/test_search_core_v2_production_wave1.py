from pathlib import Path

from scripts import build_cn_search_core_v2_production_wave1_prefreeze as builder
from scripts import run_cn_search_core_v2_production_wave1_v1 as runner
from our_system_phase2.services.program_search_state_jump_generator_v2 import SEMANTIC_STATE_JUMP_GENERATOR_V2

REPO = Path(__file__).resolve().parents[1]


def test_production_wave1_prefreeze_is_single_arm_and_zero_financial():
    payload = builder.build(REPO, source_repo_sha="a" * 40)
    assert payload["status"] == builder.STATUS
    assert payload["production_wave1"]["arm"] == SEMANTIC_STATE_JUMP_GENERATOR_V2
    assert payload["production_wave1"]["total_financial_evaluations"] == 336
    assert payload["production_wave1"]["total_checkpoint_count"] == 14
    assert payload["mature_state"]["source_development_observations"] == 1008
    assert payload["mature_state"]["expected_final_development_observations"] == 1344
    assert payload["financial_labels_read_by_builder"] is False
    assert all(value == 0 for value in payload["restricted_reads"].values())


def test_production_wave1_execution_plan_exact():
    plan = runner._execution_plan()
    assert len(plan) == 14
    assert sum(row["batch_size"] for row in plan) == 336
    assert {row["production_round_index"] for row in plan} == {0, 1}
    assert all(row["batch_size"] == 24 for row in plan)
