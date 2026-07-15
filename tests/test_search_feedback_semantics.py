from __future__ import annotations

from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import _family_tables, _is_clean
from our_system_phase2.runtime.phase3bs_adaptive_ucb_cem_practice import _feedback_eligible
from our_system_phase2.services.search_feedback import clean_optimizer_feedback_rows
from our_system_phase2.services.matched_control_pairs import (
    MATCHED_OPTIMIZER_REWARD_METRIC,
    MATCHED_OPTIMIZER_REWARD_SOURCE,
)


def _reward_row(expression: str) -> dict[str, object]:
    return {
        "candidate_id": expression,
        "expression_hash": expression,
        "expression": expression,
        "family_id": "family-a",
        "generator_arm": "cem_exploit",
        "optimizer_reward": 0.25,
        "optimizer_reward_split": "train",
        "optimizer_reward_source": MATCHED_OPTIMIZER_REWARD_SOURCE,
        "optimizer_reward_metric": MATCHED_OPTIMIZER_REWARD_METRIC,
        "pair_evaluation_status": "PAIR_EVALUATED",
        "pair_member_role": "PRIMARY",
        "primary_receipt_hash": "p" * 64,
        "control_receipt_hash": "c" * 64,
        "pair_receipt_hash": "r" * 64,
        "feedback_data_role": "development",
        "evaluation_access_guard": "evalreset_feedback_guard_v1",
        "train_reward": 0.25,
        "matched_train_increment": 0.25,
        "pair_train_reward": 0.25,
        "pair_train_reward_decision": "PAIR_TRAIN_FEEDBACK_READY",
        "pair_train_reward_blockers": "",
        "pair_turnover_metric": 0.2,
        "pair_support_metric": 1.0,
        "pair_rank_ic_metric": 0.01,
        "primary_evaluator_invocation_count": 1,
        "control_evaluator_invocation_count": 1,
        "primary_standalone_train_reward_blockers": "",
        "primary_standalone_train_reward_decision": "TRAIN_REWARD_FOLLOWUP_READY",
    }


def test_degenerate_expression_cannot_enter_optimizer_feedback() -> None:
    degenerate = _reward_row("Sign(CSRank($x))")
    valid = _reward_row("Sign(ZScore($x))")

    clean = clean_optimizer_feedback_rows([degenerate, valid], arm_id="cem_exploit")

    assert [row["expression"] for row in clean] == ["Sign(ZScore($x))"]
    assert _is_clean(degenerate, train_threshold=0.0, validation_floor=0.0, max_turnover=0.75) is False
    assert _is_clean(valid, train_threshold=0.0, validation_floor=0.0, max_turnover=0.75) is True

    materialization_metrics = {
        "mean_one_way_turnover": 0.2,
        "abs_aligned_ic_mean": 0.05,
        "positive_horizon_count": 2,
    }
    assert _feedback_eligible({**degenerate, **materialization_metrics}) is False
    assert _feedback_eligible({**valid, **materialization_metrics}) is True


def test_family_with_many_semantic_degeneracies_is_frozen_not_exploited() -> None:
    rows = [_reward_row("Sign(CSRank($x))"), _reward_row("Sign(ZScore($x))")]

    family_rows, blocked_rows, exploit_rows = _family_tables(
        rows,
        train_threshold=0.0,
        validation_floor=0.0,
        max_turnover=0.75,
        max_family_share=1.0,
    )

    assert family_rows[0]["semantic_degeneracy_count"] == 1
    assert family_rows[0]["family_status"] == "freeze"
    assert blocked_rows
    assert exploit_rows == []
