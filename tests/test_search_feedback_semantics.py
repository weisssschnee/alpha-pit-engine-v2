from __future__ import annotations

from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import _family_tables, _is_clean
from our_system_phase2.runtime.phase3bs_adaptive_ucb_cem_practice import _feedback_eligible
from our_system_phase2.services.search_feedback import clean_optimizer_feedback_rows


def _reward_row(expression: str) -> dict[str, object]:
    return {
        "candidate_id": expression,
        "expression_hash": expression,
        "expression": expression,
        "family_id": "family-a",
        "generator_arm": "cem_exploit",
        "optimizer_reward": 0.25,
        "train_reward": 0.25,
        "train_mean_one_way_turnover": 0.2,
        "train_reward_blockers": "",
        "train_reward_decision": "TRAIN_REWARD_FOLLOWUP_READY",
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
