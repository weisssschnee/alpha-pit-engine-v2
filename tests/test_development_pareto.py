from __future__ import annotations

from our_system_phase2.services.development_pareto import (
    dominates,
    prepare_objectives,
    select_pareto,
)


def _row(candidate: str, lane: str, cluster: int, reward: float, quality: float) -> dict:
    return {
        "candidate_id": candidate,
        "lane_id": lane,
        "exact_identity": candidate,
        "signal_cluster_id": cluster,
        "family_id": f"{lane}:family",
        "parent_id": "",
        "legal": True,
        "survivor": True,
        "proxy_reward": reward,
        "proxy_ic_mean": quality,
        "proxy_turnover": 0.2,
        "proxy_worst_time_block_abs_ic": quality * 0.7,
        "proxy_time_block_stability": 0.8,
        "proxy_signal_concentration": 0.1,
        "complexity": 6,
    }


def test_pareto_hard_gate_prevents_novelty_from_compensating_negative_benchmark_increment() -> None:
    rows = [
        _row("good", "temporal_program", 1, 0.12, 0.10),
        _row("novel_bad", "orthogonal_exile", 2, 0.05, 0.20),
    ]
    prepared = prepare_objectives(rows, benchmark_median=0.08)
    indexed = {row["candidate_id"]: row for row in prepared}
    assert indexed["good"]["objective_gate_allowed"] is True
    assert indexed["novel_bad"]["objective_gate_allowed"] is False
    assert "NEGATIVE_BENCHMARK_INCREMENT" in indexed["novel_bad"]["objective_gate_reasons"]


def test_pareto_selection_uses_one_identity_one_cluster_and_lane_floors() -> None:
    rows = [
        _row("a1", "static_cross_sectional", 1, 0.12, 0.10),
        _row("a2", "static_cross_sectional", 1, 0.13, 0.11),
        _row("a3", "static_cross_sectional", 2, 0.11, 0.09),
        _row("b1", "temporal_program", 3, 0.14, 0.12),
    ]
    prepared = prepare_objectives(rows, benchmark_median=0.08)
    selected = select_pareto(prepared, cap=3, lane_floor=1, family_cap=3, parent_cap=2)
    assert {row["lane_id"] for row in selected} == {"static_cross_sectional", "temporal_program"}
    assert len({row["signal_cluster_id"] for row in selected}) == len(selected)
    assert len({row["exact_identity"] for row in selected}) == len(selected)


def test_dominance_requires_no_worse_in_every_objective() -> None:
    left = {
        "signal_quality": 0.2, "cost_adjusted_quality": 0.19, "worst_time_block_quality": 0.1,
        "stability": 0.8, "benchmark_increment": 0.05, "behaviour_novelty": 1.0,
        "turnover": 0.2, "concentration": 0.1, "complexity": 5.0,
    }
    right = dict(left, signal_quality=0.1)
    assert dominates(left, right)
    assert not dominates(right, left)
