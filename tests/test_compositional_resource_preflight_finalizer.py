from __future__ import annotations

from scripts.finalize_cn_compositional_resource_preflight import (
    linear_capacity_projection,
    safe_worker_count,
    summarize_pair_rows,
)


def test_safe_worker_count_reserves_memory_and_obeys_caps() -> None:
    assert safe_worker_count(
        total_memory_bytes=100 * 1024**3,
        observed_peak_bytes=14 * 1024**3,
        logical_processors=32,
    ) == 5


def test_linear_capacity_projection_is_explicit_not_alpha_evidence() -> None:
    result = linear_capacity_projection(one_pair_wall_seconds=600, active_pairs=1200, workers=5)

    assert result["linear_wall_hours"] == 40.0
    assert result["active_pairs"] == 1200


def test_pair_summary_keeps_diagnostic_positive_separate_from_execution_status() -> None:
    result = summarize_pair_rows(
        [
            {
                "pair_id": "a",
                "route_id": "MINUTE_STATIC",
                "pair_evaluation_status": "PAIR_EVALUATED",
                "pair_train_reward_decision": "PAIR_TRAIN_FEEDBACK_READY",
                "primary_behavior_identity": "cluster-a",
            },
            {
                "pair_id": "b",
                "route_id": "MINUTE_STATIC",
                "pair_evaluation_status": "PAIR_BLOCKED",
                "pair_train_reward_decision": "PAIR_TRAIN_FEEDBACK_BLOCKED",
            },
        ]
    )

    assert result["evaluated_pairs"] == 1
    assert result["blocked_pairs"] == 1
    assert result["diagnostic_matched_positive_behavior_identities"] == 1
