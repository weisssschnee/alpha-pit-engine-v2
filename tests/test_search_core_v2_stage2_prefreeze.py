from pathlib import Path
from scripts import build_cn_search_core_v2_stage2_prefreeze as builder

REPO = Path(__file__).resolve().parents[1]


def test_stage2_prefreeze_completes_original_1008_per_arm_target():
    payload = builder.build(REPO, source_repo_sha="a" * 40)
    assert payload["status"] == builder.STATUS
    assert payload["stage2"]["total_budget_per_arm"] == 504
    assert payload["stage2"]["total_financial_evaluations"] == 1008
    assert payload["stage2"]["cumulative_prior_budget_per_arm"] == 504
    assert payload["stage2"]["target_cumulative_budget_per_arm"] == 1008
    arm_a = payload["arm_a_primitive"]
    assert arm_a["selected_count"] == 504
    assert len(set(arm_a["selected_exact_identities"])) == 504
    assert all(len(rows) == 72 for rows in arm_a["selected_by_template"].values())
    arm_b = payload["arm_b_state_jump"]
    assert arm_b["source_history_count"] == 21
    assert arm_b["source_generated_exact_count"] == 504
    assert arm_b["source_memory_observations"] == 504
    assert arm_b["sealed_feedback_allowed"] is False
    assert payload["financial_labels_read_by_builder"] is False
    assert not any(payload["restricted_reads"].values())
    assert payload["automatic_policy_change_authorized"] is False


def test_stage2_primitive_is_exact_72_to_143_slice_without_prior_reuse():
    payload = builder.build(REPO, source_repo_sha="b" * 40)
    stage1 = builder._read(REPO / builder.STAGE1_PREFREEZE)
    stage15 = builder._read(REPO / builder.STAGE15_PREFREEZE)
    prior15 = set(stage15["arm_a_primitive"]["selected_exact_identities"])
    for template in builder.TEMPLATES:
        order = list(stage1["arm_a_primitive"]["eligible_orders_by_template"][template])
        selected = payload["arm_a_primitive"]["selected_by_template"][template]
        assert selected == order[72:144]
        assert not set(order[:72]).intersection(selected)
        assert not prior15.intersection(selected)