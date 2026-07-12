from __future__ import annotations

import pandas as pd
import pytest

from our_system_phase2.services.generator_funnel_diagnostics import analyze_generator_funnel


def test_generator_funnel_diagnosis_measures_duplicates_overlap_and_strict_quality() -> None:
    proposals = pd.DataFrame(
        [
            {"candidate_id": "s1", "lane_id": "static", "exact_identity": "e1", "canonical_identity": "c1", "semantic_bucket": "s:a", "family_id": "s", "motif": "level", "expression": "CSRank($close)", "legal": True, "materialized": True, "survivor": True, "signal_cluster_id": 1, "proxy_reward": 0.10},
            {"candidate_id": "s2", "lane_id": "static", "exact_identity": "e1", "canonical_identity": "c1", "semantic_bucket": "s:a", "family_id": "s", "motif": "level", "expression": "CSRank($close)", "legal": True, "materialized": True, "survivor": True, "signal_cluster_id": 1, "proxy_reward": 0.10},
            {"candidate_id": "t1", "lane_id": "temporal", "exact_identity": "e2", "canonical_identity": "c2", "semantic_bucket": "t:d", "family_id": "t", "motif": "delta", "expression": "CSRank(Delta($close,5))", "legal": True, "materialized": True, "survivor": True, "signal_cluster_id": 1, "proxy_reward": 0.08},
            {"candidate_id": "t2", "lane_id": "temporal", "exact_identity": "e3", "canonical_identity": "c3", "semantic_bucket": "t:s", "family_id": "t", "motif": "slope", "expression": "CSRank(Slope($close,5))", "legal": True, "materialized": True, "survivor": True, "signal_cluster_id": 2, "proxy_reward": 0.12},
        ]
    )
    strict = pd.DataFrame(
        [
            {"candidate_id": candidate, "lane_id": lane, "signal_cluster_id": cluster, "horizon_bars": horizon, "ic_mean": ic, "ic_abs_mean": abs(ic), "ic_count": 100, "mean_one_way_turnover": turnover}
            for candidate, lane, cluster, turnover in (("s1", "static", 1, 0.7), ("t2", "temporal", 2, 0.3))
            for horizon, ic in ((1, 0.20), (5, 0.15), (15, 0.10), (30, 0.05))
        ]
    )
    funnel = pd.DataFrame(
        [
            {"lane_id": "static", "n_eff": 1.0, "top1_cluster_share": 1.0},
            {"lane_id": "temporal", "n_eff": 2.0, "top1_cluster_share": 0.5},
        ]
    )
    result = analyze_generator_funnel(
        proposals,
        strict,
        funnel,
        {"benchmark_median_reward": 0.09},
        admissions={
            "stratified": proposals.iloc[[0, 2]],
            "global_top_k": proposals.iloc[[0, 1]],
            "hybrid": proposals.iloc[[0, 3]],
        },
        adaptive_vs_control=[
            {"lane_id": "rx_ucb", "adaptive_outperformed_matched_control": True},
            {"lane_id": "cem", "adaptive_outperformed_matched_control": False},
        ],
        bottleneck_diagnosis={"primary_bottleneck": "generator"},
    )
    lanes = {row["lane_id"]: row for row in result["per_lane"]}

    assert result["funnel_totals"]["proposal_count"] == 4
    assert result["funnel_totals"]["proposal_exact_identity_count"] == 3
    assert result["funnel_totals"]["legal_exact_identity_count"] == 3
    assert result["funnel_totals"]["survivor_exact_identity_count"] == 3
    assert lanes["static"]["proposal_exact_duplicate_rate"] == 0.5
    assert lanes["temporal"]["materialized_unique_vs_other_lane_cluster_count"] == 1
    assert lanes["temporal"]["survivor_unique_vs_other_lane_cluster_count"] == 1
    assert lanes["temporal"]["median_turnover"] == 0.3
    assert lanes["temporal"]["proxy_increment_vs_benchmark_median"] == pytest.approx(0.01)
    assert lanes["temporal"]["ic_confidence_bound_available"] is False
    assert lanes["temporal"]["admission_counts"]["stratified"] == 1
    assert result["admission_strategies"]["global_top_k"]["exact_identity_count"] == 1
    assert result["adaptive_vs_matched_control"]["winners"] == ["rx_ucb"]
    assert result["recorded_bottleneck_diagnosis"]["primary_bottleneck"] == "generator"
    assert any(
        row["materialized_shared_cluster_count"] == 1
        for row in result["cross_lane_overlap"]
    )
