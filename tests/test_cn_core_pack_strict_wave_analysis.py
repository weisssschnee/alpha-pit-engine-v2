from __future__ import annotations

import pytest

from scripts.analyze_cn_core_pack_strict_wave import analyze_strict_wave


def _binding() -> dict:
    return {
        "binding_hash": "binding-1",
        "pair_count": 2,
        "data_role": "development_train_only",
        "promotion": "FORBIDDEN",
        "cross_sprint_memory": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "pairs": [
            {
                "pair_id": "p1",
                "route_id": "MINUTE_STATIC",
                "clock_namespace": "active_bar",
                "support_unit": "minute",
                "primary_expression": "Rank(x)",
                "control_expression": "x",
            },
            {
                "pair_id": "p2",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "clock_namespace": "stock_session",
                "support_unit": "stock_session",
                "primary_expression": "Delta(y, 5)",
                "control_expression": "y",
            },
        ],
        "candidate_members": [
            {"candidate_id": "a", "receipt_hash": "ra"},
            {"candidate_id": "ac", "receipt_hash": "rac"},
            {"candidate_id": "b", "receipt_hash": "rb"},
            {"candidate_id": "bc", "receipt_hash": "rbc"},
        ],
    }


def _result(backend: str, pair_id: str, primary: str, control: str, increment: float) -> dict:
    return {
        "backend": backend,
        "status": "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED",
        "input_binding_hash": "binding-1",
        "pair_count": 1,
        "wall_seconds": 10.0,
        "peak_rss_bytes": 123,
        "parallelism_status": "PARALLELISM_ENGAGED",
        "hot_path_bottleneck": "BATCHED_PORTFOLIO_KERNEL_BOTTLENECK",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "candidate_rewards": [
            {
                "candidate_id": primary,
                "train_rank_ic_mean": 0.03,
                "train_mean_one_way_turnover": 0.2,
            },
            {
                "candidate_id": control,
                "train_rank_ic_mean": 0.01,
                "train_mean_one_way_turnover": 0.1,
            },
        ],
        "pair_results": [
            {
                "pair_id": pair_id,
                "primary_candidate_id": primary,
                "control_candidate_id": control,
                "pair_evaluation_status": "PAIR_EVALUATED",
                "pair_evaluation_blockers": "",
                "primary_train_reward": 0.2 + increment,
                "control_train_reward": 0.2,
                "pair_train_reward": increment,
                "pair_support_count": 100,
                "pair_support_overlap": 1.0,
            }
        ],
    }


def test_analysis_joins_backends_to_routes_and_keeps_access_closed() -> None:
    summary, rows = analyze_strict_wave(
        binding=_binding(),
        backend_results=[
            ("active_bar", _result("active_bar", "p1", "a", "ac", 0.05)),
            ("stock_session", _result("stock_session", "p2", "b", "bc", -0.02)),
        ],
    )

    assert summary["status"] == "CN_CORE_PACK_STRICT_WAVE_ANALYZED"
    assert summary["overall"]["positive_matched_reward_count"] == 1
    assert summary["gates"]["new_matched_mechanism_observed"] is True
    assert summary["gates"]["no_access_or_pit_violation"] is True
    assert summary["maximum_route_pair_share"] == 0.5
    assert rows[0]["matched_rank_ic_increment"] == pytest.approx(0.02)
    assert {row["route_id"] for row in rows} == {
        "MINUTE_STATIC",
        "SLOW_TEMPORAL_CHANGE",
    }


def test_analysis_fails_closed_on_binding_hash_mismatch() -> None:
    result = _result("active_bar", "p1", "a", "ac", 0.05)
    result["input_binding_hash"] = "wrong"

    with pytest.raises(ValueError, match="binding hash mismatch"):
        analyze_strict_wave(binding=_binding(), backend_results=[("active_bar", result)])


def test_semantic_pair_blocker_is_not_mislabeled_as_infrastructure_failure() -> None:
    result = _result("active_bar", "p1", "a", "ac", 0.0)
    result["pair_results"][0]["pair_evaluation_status"] = "PAIR_EVALUATION_BLOCKED"
    result["pair_results"][0]["pair_evaluation_blockers"] = "control_signal_empty_or_constant"

    summary, _ = analyze_strict_wave(
        binding={
            **_binding(),
            "pair_count": 1,
            "pairs": _binding()["pairs"][:1],
            "candidate_members": _binding()["candidate_members"][:2],
        },
        backend_results=[("active_bar", result)],
    )

    assert summary["gates"]["no_infrastructure_failed_pairs"] is True
    assert summary["overall"]["blocked_pair_count"] == 1
