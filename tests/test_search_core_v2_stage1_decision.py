from __future__ import annotations

from scripts.run_cn_search_core_v2_stage1_v1 import _decision


def _metric(productive: int, behaviors: int) -> dict[str, int]:
    return {
        "productive": productive,
        "distinct_behavior_pair_count": behaviors,
    }


def test_stage1_decision_fails_closed_when_historical_baseline_sanity_misses_even_on_clear_win():
    decision = _decision(_metric(126, 261), _metric(130, 310))
    assert decision["baseline_historical_sanity_pass"] is False
    assert decision["same_run_comparator_status"] == "CLEAR_GENERATOR_V2_STAGE1_WIN_STAGE2_REVIEW_ELIGIBLE"
    assert decision["status"] == "BASELINE_HISTORICAL_SANITY_AUDIT_REQUIRED_NO_STAGE2"
    assert decision["automatic_stage2_authorized"] is False


def test_stage1_decision_preserves_clear_win_when_historical_baseline_is_sane():
    decision = _decision(_metric(139, 249), _metric(147, 248))
    assert decision["baseline_historical_sanity_pass"] is True
    assert decision["status"] == "CLEAR_GENERATOR_V2_STAGE1_WIN_STAGE2_REVIEW_ELIGIBLE"


def test_stage1_decision_preserves_clear_loss_when_historical_baseline_is_sane():
    decision = _decision(_metric(139, 249), _metric(120, 240))
    assert decision["baseline_historical_sanity_pass"] is True
    assert decision["status"] == "CLEAR_GENERATOR_V2_STAGE1_LOSS_STOP"


def test_stage1_actual_terminal_metrics_require_baseline_audit_and_keep_ambiguous_comparator():
    decision = _decision(_metric(126, 261), _metric(105, 309))
    assert decision["baseline_historical_sanity_pass"] is False
    assert decision["same_run_comparator_status"] == "AMBIGUOUS_STAGE1_REVIEW_NO_AUTOMATIC_STAGE2"
    assert decision["status"] == "BASELINE_HISTORICAL_SANITY_AUDIT_REQUIRED_NO_STAGE2"
