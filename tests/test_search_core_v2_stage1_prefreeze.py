from __future__ import annotations

from pathlib import Path

from scripts import build_cn_search_core_v2_stage1_prefreeze as builder


REPO = Path(__file__).resolve().parents[1]


def test_stage1_prefreeze_freezes_unspent_baseline_and_generator_contract():
    payload = builder.build(REPO, source_repo_sha="a" * 40)
    assert payload["status"] == builder.STATUS
    assert payload["stage1"]["total_budget_per_arm"] == 336
    assert payload["stage1"]["total_financial_evaluations"] == 672
    assert payload["stage1"]["automatic_stage2_launch"] is False
    baseline = payload["arm_a_primitive"]
    assert baseline["unspent_candidate_count"] == 1568
    assert baseline["unspent_per_template"] == 224
    assert baseline["selected_count"] == 336
    assert len(set(baseline["selected_exact_identities"])) == 336
    for template, order in baseline["eligible_orders_by_template"].items():
        assert template in builder.TEMPLATES
        assert len(order) == 224
    state_jump = payload["arm_b_state_jump"]
    assert state_jump["within_campaign_ask_tell_adaptation"] is True
    assert state_jump["cross_campaign_reward_memory"] is False
    assert state_jump["zero_financial_supply_audit_required_before_project_control"] is True
    assert payload["financial_labels_read_by_builder"] is False
    assert payload["validation_reads"] == 0
    assert payload["holdout_reads"] == 0
    assert payload["forward_2026_reads"] == 0


def test_stage1_prefreeze_historical_sanity_band_is_frozen_not_selection_input():
    payload = builder.build(REPO, source_repo_sha="b" * 40)
    assert payload["historical_336_sanity_band"] == {
        "stage_c_336": {
            "productive": 140,
            "distinct_behavior_pair_count": 251,
        },
        "stage_d_336": {
            "productive": 139,
            "distinct_behavior_pair_count": 247,
        },
    }
    assert payload["arm_a_primitive"]["candidate_labels_read_during_selection"] is False
    assert payload["stage1_decision_contract"]["stage2_requires_separate_project_control_review"] is True
