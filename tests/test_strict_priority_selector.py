from __future__ import annotations

import numpy as np
import pandas as pd

from our_system_phase2.runtime.cn_sprint2_offline_selector import _strict_evidence
from our_system_phase2.services.strict_priority_selector import (
    StrictPriorityModel,
    apply_development_eligibility,
    balanced_group_folds,
    leakage_components,
    selector_comparison,
)


def _row(index: int) -> dict:
    quality = index / 119.0
    return {
        "candidate_id": f"candidate_{index}",
        "exact_identity": f"exact_{index}",
        "family_id": f"family_{index // 3}",
        "signal_cluster_id": index // 2,
        "run_id": f"run_{index % 4}",
        "seed_set": f"seed_{index % 4}",
        "lane_id": ("temporal_program" if index % 2 else "static_cross_sectional"),
        "primitive_family": f"primitive_{index % 7}",
        "hypothesis_arm": f"arm_{index % 5}",
        "proposal_stage": "fixed",
        "field_family": "raw_1min",
        "proxy_ic_abs_lcb95": quality,
        "proxy_reward": quality,
        "proxy_worst_time_block_abs_ic": quality * 0.8,
        "proxy_time_block_stability": 0.4 + quality * 0.6,
        "proxy_turnover": 1.0 - quality * 0.7,
        "proxy_signal_concentration": 0.5 - quality * 0.3,
        "proxy_signal_unique": 8 + index,
        "proxy_finite_ratio": 1.0,
        "control_increment_vs_benchmark": quality - 0.5,
        "information_gain": quality,
        "complexity": 5 + index % 4,
        "limited_scalar_tiebreak": quality,
    }


def test_development_eligible_is_evaluability_not_quality_survivor() -> None:
    contract = {
        "minimum_finite_ratio": 0.25,
        "minimum_signal_unique": 8,
        "minimum_eligible_cross_sections": 40,
        "minimum_proxy_reward": 0.0,
    }
    source = {
        "legal": "True", "materialized": "True", "survivor": "True",
        "proxy_finite_ratio": 1.0, "proxy_signal_unique": 12,
        "proxy_ic_count": 60, "proxy_reward": 0.0,
    }
    accepted = apply_development_eligibility([source], contract)[0]
    assert accepted["development_eligible"] is True
    assert accepted["legacy_survivor"] is True
    assert accepted["development_eligibility_reasons"] == []

    rejected = dict(source, proxy_signal_unique=2, survivor=False)
    audited = apply_development_eligibility([rejected], contract)[0]
    assert audited["development_eligible"] is False
    assert audited["development_eligibility_reasons"] == ["SIGNAL_UNIQUE"]


def test_leakage_components_keep_exact_family_and_cluster_together() -> None:
    rows = [_row(index) for index in range(12)]
    rows[7]["exact_identity"] = rows[0]["exact_identity"]
    rows[8]["family_id"] = rows[1]["family_id"]
    rows[9]["run_id"] = rows[2]["run_id"]
    rows[9]["signal_cluster_id"] = rows[2]["signal_cluster_id"]
    groups = leakage_components(rows)
    assert groups[7] == groups[0]
    assert groups[8] == groups[1]
    assert groups[9] == groups[2]
    folds = balanced_group_folds(groups, n_splits=4)
    for group in set(groups):
        assert len(set(folds[index] for index, value in enumerate(groups) if value == group)) == 1


def test_selector_cross_fit_is_deterministic_and_exports_model() -> None:
    rows = [_row(index) for index in range(120)]
    labels = [int(index >= 72) for index in range(120)]
    first = selector_comparison(rows, labels)
    second = selector_comparison(rows, labels)
    assert first["metrics"] == second["metrics"]
    assert first["folds"]["seed_fold_count"] == 4
    assert first["folds"]["leakage_component_count"] < len(rows)
    assert set(first["metrics"]) == {
        "random", "current_scalar", "hard_gate_rank_rule",
        "model_component_oof", "model_seed_oof",
    }
    model = StrictPriorityModel.from_artifact(first["full_model"])
    scores = model.score(rows[:5])
    assert np.isfinite(scores).all()
    assert ((scores >= 0) & (scores <= 1)).all()


def test_strict_evidence_uses_h5_benchmark_and_all_horizon_stability() -> None:
    proposals = pd.DataFrame(
        [
            {**_row(0), "candidate_id": "bench", "lane_id": "benchmark_competitor"},
            {**_row(100), "candidate_id": "alpha", "lane_id": "rx_ucb"},
        ]
    )
    strict_rows = []
    for candidate, lane, quality in (("bench", "benchmark_competitor", 0.10), ("alpha", "rx_ucb", 0.20)):
        for horizon in (1, 5, 15, 30):
            strict_rows.append(
                {
                    "candidate_id": candidate,
                    "lane_id": lane,
                    "horizon_bars": horizon,
                    "ic_mean": quality,
                    "ic_abs_mean": quality,
                    "ic_abs_lcb95": quality - 0.02,
                    "cost_adjusted_abs_ic": quality - 0.01,
                    "mean_one_way_turnover": 0.3,
                    "proxy_worst_time_block_abs_ic": 0.05,
                    "proxy_time_block_stability": 0.8,
                }
            )
    evidence = _strict_evidence(
        proposals, pd.DataFrame(strict_rows), run_id="run", seed_set="seed"
    )
    alpha = evidence[evidence["candidate_id"].eq("alpha")].iloc[0]
    assert alpha["strict_benchmark_increment"] > 0
    assert alpha["strict_stable"]
    assert alpha["strict_priority_hit"] == 1


def test_strict_evidence_derives_legacy_lcb_and_cost_without_faking_time_blocks() -> None:
    proposals = pd.DataFrame(
        [{
            key: value
            for key, value in {**_row(0), "candidate_id": "bench", "lane_id": "benchmark_competitor"}.items()
            if key not in {"family_id", "primitive_family"}
        }]
    )
    strict = pd.DataFrame(
        [
            {
                "candidate_id": "bench", "lane_id": "benchmark_competitor",
                "horizon_bars": horizon, "ic_mean": 0.1, "ic_abs_mean": 0.1,
                "ic_standard_error": 0.01, "mean_one_way_turnover": 0.2,
            }
            for horizon in (1, 5, 15, 30)
        ]
    )
    evidence = _strict_evidence(proposals, strict, run_id="legacy", seed_set="legacy")
    row = evidence.iloc[0]
    assert row["strict_schema_version"] == "strict_v1_derived_lcb_cost_no_time_blocks"
    assert not row["time_block_evidence_available"]
    assert np.isclose(row["ic_abs_lcb95"], 0.0804)
    assert np.isclose(row["cost_adjusted_abs_ic"], 0.0995)
    assert row["family_id"] == "benchmark_competitor:unknown"
