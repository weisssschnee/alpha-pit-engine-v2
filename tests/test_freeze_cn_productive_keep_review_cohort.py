from __future__ import annotations

import math

import pandas as pd

from scripts.freeze_cn_productive_keep_review_cohort import (
    HIGHER_IS_BETTER,
    LOWER_IS_BETTER,
    _payload_sha256,
    _rank_candidates,
    _screen_reason,
    _select_with_caps,
)


def _ranking_frame() -> pd.DataFrame:
    rows = []
    groups = [
        ("ROUTE_A", "STRUCT_A", "SIGNAL_A", 60),
        ("ROUTE_B", "STRUCT_B", "SIGNAL_B", 20),
        ("ROUTE_C", "STRUCT_C", "SIGNAL_C", 20),
    ]
    ordinal = 0
    for route_id, structural_id, signal_id, count in groups:
        for _ in range(count):
            ordinal += 1
            row = {
                "pair_id": f"pair-{ordinal:03d}",
                "route_id": route_id,
                "structural_family_id": structural_id,
                "signal_cluster_id": signal_id,
            }
            for offset, column in enumerate(HIGHER_IS_BETTER):
                row[column] = 1000.0 - ordinal - offset * 0.01
            for offset, column in enumerate(LOWER_IS_BETTER):
                row[column] = ordinal + offset * 0.01
            rows.append(row)
    return pd.DataFrame(rows)


def test_cap_constrained_selection_is_deterministic_and_diverse() -> None:
    ranked = _rank_candidates(_ranking_frame())
    first = _select_with_caps(ranked)
    second = _select_with_caps(ranked)
    assert first == second
    assert len(first) == 64
    selected = ranked[ranked["pair_id"].isin(first)]
    assert selected["route_id"].value_counts().max() <= 48
    assert selected["structural_family_id"].value_counts().max() <= 32
    assert selected["signal_cluster_id"].value_counts().max() <= 48
    assert set(selected["structural_family_id"]) == {
        "STRUCT_A",
        "STRUCT_B",
        "STRUCT_C",
    }
    assert set(selected["signal_cluster_id"]) == {
        "SIGNAL_A",
        "SIGNAL_B",
        "SIGNAL_C",
    }


def test_train_stability_screen_is_fail_closed() -> None:
    clean = {
        "search_score": 0.1,
        "primary_composite_reward": 0.2,
        "matched_train_increment": 0.1,
        "matched_gross_increment": 1.0,
        "matched_net_increment": 0.9,
        "matched_trading_cost_difference": 0.1,
        "train_day_sortino": 0.4,
        "train_worst_horizon_day_sortino": 0.04,
        "train_median_horizon_day_sortino": 0.2,
        "train_horizon_sortino_stdev": 0.1,
        "train_day_mcmc_p25": 0.2,
        "train_day_mcmc_prob_gt_0": 1.0,
        "train_regime_positive_share": 0.5,
        "train_regime_stability_score": -0.5,
        "train_rank_ic_mean": 0.01,
        "train_rank_ic_hit_rate": 0.6,
        "train_mean_one_way_turnover": 0.02,
    }
    assert _screen_reason(clean) == ""

    broken = dict(clean)
    broken["matched_net_increment"] = -0.01
    assert _screen_reason(broken) == "NONPOSITIVE_MATCHED_NET_INCREMENT"

    broken = dict(clean)
    broken["train_day_mcmc_p25"] = float("nan")
    assert _screen_reason(broken).startswith(
        "MISSING_FINITE_TRAIN_METRICS:"
    )


def test_payload_hash_is_canonical() -> None:
    left = {"b": 2, "a": {"x": 1}}
    right = {"a": {"x": 1}, "b": 2}
    assert _payload_sha256(left) == _payload_sha256(right)
