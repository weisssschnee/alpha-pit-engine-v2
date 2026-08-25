from pathlib import Path

from scripts import build_cn_search_core_v2_production_wave2_prefreeze as builder
from scripts import run_cn_search_core_v2_production_wave2_v1 as runner
from our_system_phase2.services.program_search_state_jump_generator_v2 import SEMANTIC_STATE_JUMP_GENERATOR_V2

REPO = Path(__file__).resolve().parents[1]


def test_production_wave2_prefreeze_is_single_arm_pair_annotated_and_zero_financial():
    payload = builder.build(REPO, source_repo_sha="a" * 40)
    assert payload["status"] == builder.STATUS
    assert payload["production_wave2"]["arm"] == SEMANTIC_STATE_JUMP_GENERATOR_V2
    assert payload["production_wave2"]["total_financial_evaluations"] == 336
    assert payload["production_wave2"]["total_checkpoint_count"] == 14
    assert payload["mature_state"]["source_development_observations"] == 1344
    assert payload["mature_state"]["expected_final_development_observations"] == 1680
    pair = payload["pair_native_annotation_contract"]
    assert pair["rule_id"] == "PAIR_NATIVE_PRIMARY_3OF3_CONTROL_2OF3_POSITIVE_V1"
    assert pair["application_scope"] == "DEVELOPMENT_RESULT_ANNOTATION_AND_ARCHIVE_ONLY"
    assert pair["optimizer_feedback_write"] is False
    assert pair["generator_ask_order_changed"] is False
    assert pair["candidate_admission_changed"] is False
    assert payload["financial_labels_read_by_builder"] is False
    assert all(value == 0 for value in payload["restricted_reads"].values())


def test_production_wave2_execution_plan_exact():
    plan = runner._execution_plan()
    assert len(plan) == 14
    assert sum(row["batch_size"] for row in plan) == 336
    assert {row["production_round_index"] for row in plan} == {0, 1}
    assert all(row["batch_size"] == 24 for row in plan)


def test_pair_native_annotation_is_observation_only():
    contract = {
        "rule_id": "PAIR_NATIVE_PRIMARY_3OF3_CONTROL_2OF3_POSITIVE_V1",
        "rule_contract": {
            "primary_positive_development_window_count_required": 3,
            "control_positive_development_window_count_minimum": 2,
        },
    }
    record = {
        "primary": {
            "cumulative_net_return": 0.20,
            "development_subwindows": [
                {"window_id": "development_1", "cumulative_net_return": 0.01},
                {"window_id": "development_2", "cumulative_net_return": 0.02},
                {"window_id": "development_3", "cumulative_net_return": 0.03},
            ],
        },
        "base_control": {
            "cumulative_net_return": 0.10,
            "development_subwindows": [
                {"window_id": "development_1", "cumulative_net_return": 0.01},
                {"window_id": "development_2", "cumulative_net_return": -0.01},
                {"window_id": "development_3", "cumulative_net_return": 0.02},
            ],
        },
    }
    annotation = runner._pair_native_annotation(record, contract)
    assert annotation["available"] is True
    assert annotation["primary_positive_window_count"] == 3
    assert annotation["control_positive_window_count"] == 2
    assert annotation["challenger_hit"] is True
    assert annotation["annotation_only"] is True
    assert annotation["optimizer_feedback_used"] is False


def test_pair_native_annotation_handles_blocked_pair_without_adoption():
    annotation = runner._pair_native_annotation(
        {"primary": None, "base_control": None},
        {
            "rule_id": "PAIR_NATIVE_PRIMARY_3OF3_CONTROL_2OF3_POSITIVE_V1",
            "rule_contract": {
                "primary_positive_development_window_count_required": 3,
                "control_positive_development_window_count_minimum": 2,
            },
        },
    )
    assert annotation["available"] is False
    assert annotation["challenger_hit"] is False
    assert annotation["optimizer_feedback_used"] is False
