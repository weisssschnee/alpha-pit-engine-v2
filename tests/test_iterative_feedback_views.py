from __future__ import annotations

from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import (
    build_iterative_feedback_views,
)
from our_system_phase2.services.evaluation_access_guard import GUARD_VERSION


def _row(**values: object) -> dict[str, object]:
    return {
        "pair_id": "pair-a",
        "route_id": "MINUTE_STATIC",
        "pair_evaluation_status": "PAIR_EVALUATED",
        "matched_gross_increment": 0.1,
        "matched_net_increment": 0.05,
        "matched_trading_cost_difference": 0.05,
        "matched_train_increment": 0.05,
        "pair_turnover_metric": 0.2,
        "pair_train_reward_blockers": "",
        "feedback_data_role": "development",
        "evaluation_access_guard": GUARD_VERSION,
        **values,
    }


def test_feedback_views_use_matched_increments() -> None:
    ledger, positive, negative, run_health = build_iterative_feedback_views([_row()])
    assert ledger[0]["outcome_labels"] == ["GROSS_POSITIVE", "NET_POSITIVE"]
    assert len(positive) == 1
    assert negative == []
    assert run_health == []


def test_cost_killed_requires_matched_direction_flip() -> None:
    row = _row(
        matched_gross_increment=0.03,
        matched_net_increment=-0.01,
        matched_trading_cost_difference=0.04,
        matched_train_increment=-0.01,
    )
    ledger, positive, negative, _ = build_iterative_feedback_views([row])
    assert "COST_KILLED" in ledger[0]["outcome_labels"]
    assert positive == []
    assert negative[0]["negative_labels"] == ["COST_KILLED"]


def test_infrastructure_failure_only_enters_run_health() -> None:
    ledger, positive, negative, run_health = build_iterative_feedback_views(
        [_row(pair_evaluation_status="INFRASTRUCTURE_FAILURE")]
    )
    assert ledger == []
    assert positive == []
    assert negative == []
    assert run_health[0]["run_health_status"] == "INFRASTRUCTURE_FAILURE"


def test_evaluated_zero_matched_increment_is_auditable_no_increment() -> None:
    ledger, positive, negative, run_health = build_iterative_feedback_views(
        [
            _row(
                matched_gross_increment=0.0,
                matched_net_increment=0.0,
                matched_trading_cost_difference=0.0,
                matched_train_increment=0.0,
            )
        ]
    )
    assert ledger[0]["outcome_labels"] == ["NO_INCREMENT"]
    assert positive == []
    assert negative[0]["negative_labels"] == ["NO_INCREMENT"]
    assert run_health == []
