from __future__ import annotations

import math
import json
import hashlib
from pathlib import Path

import pandas as pd

from scripts.freeze_cn_productive_keep_review_cohort import (
    HIGHER_IS_BETTER,
    LOWER_IS_BETTER,
    _deduplicate_behavior_candidates,
    _load_excluded_cohort_pairs,
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


def test_small_cohort_does_not_overshoot_diversity_anchors() -> None:
    ranked = _rank_candidates(_ranking_frame())
    selected_ids = _select_with_caps(ranked, cohort_pairs=2)
    assert len(selected_ids) == 2
    assert selected_ids == _select_with_caps(ranked, cohort_pairs=2)


def test_behavior_dedup_keeps_highest_ranked_representative() -> None:
    frame = _ranking_frame().head(4).copy()
    frame["portfolio_behavior_family_id"] = [
        "FAMILY_DUPLICATE",
        "FAMILY_DUPLICATE",
        "FAMILY_3",
        "FAMILY_4",
    ]
    frame["portfolio_behavior_signature_id"] = [
        "SIGNATURE_1",
        "SIGNATURE_2",
        "SIGNATURE_3",
        "SIGNATURE_4",
    ]
    ranked = _rank_candidates(frame)
    deduped, rejected = _deduplicate_behavior_candidates(ranked)
    assert deduped["pair_id"].tolist() == [
        "pair-001",
        "pair-003",
        "pair-004",
    ]
    assert rejected == {
        "pair-002": "BEHAVIOR_FAMILY_DUPLICATE_LOWER_RANK"
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


def test_excluded_cohort_is_hash_verified_and_loaded(
    tmp_path: Path,
) -> None:
    root = tmp_path / "prior"
    root.mkdir()
    pairs = root / "keep_review_pairs.parquet"
    pd.DataFrame({"pair_id": ["pair-old-1", "pair-old-2"]}).to_parquet(
        pairs, index=False
    )
    artifact = {
        "path": pairs.name,
        "bytes": pairs.stat().st_size,
        "sha256": hashlib.sha256(pairs.read_bytes()).hexdigest(),
    }
    manifest = {
        "status": "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE",
        "selection_payload_sha256": "selection",
        "artifacts": [artifact],
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    (root / "keep_review_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    pair_ids, bindings = _load_excluded_cohort_pairs((root,))
    assert pair_ids == {"pair-old-1", "pair-old-2"}
    assert bindings[0]["excluded_pair_count"] == 2
