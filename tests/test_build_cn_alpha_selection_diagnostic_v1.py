from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from scripts import build_cn_alpha_selection_diagnostic_v1 as subject
from scripts import audit_cn_alpha_selection_diagnostic_v1 as independent_audit


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8"
    )


def _synthetic_inputs(root: Path) -> tuple[Path, Path]:
    input_root = root / "input"
    for name in ("decoder", "freeze", "oos"):
        (input_root / name).mkdir(parents=True, exist_ok=True)
    pair_ids = [f"pair-{index:02d}" for index in range(32)]
    numeric_features = [
        "search_score",
        "train_rank_ic_mean",
        "train_rank_ic_hit_rate",
        "train_regime_stability_score",
        "primary_continuous_book_net_reward",
        "primary_cumulative_net_return",
        "primary_cumulative_realized_trade_pnl_cny",
        "primary_ending_unrealized_pnl_cny",
        "primary_net_return_per_turnover",
        "matched_continuous_book_net_reward_increment",
        "matched_cumulative_net_return_increment",
    ]
    review_rows: list[dict[str, object]] = []
    transition_rows: list[dict[str, object]] = []
    for index, pair_id in enumerate(pair_ids):
        base = float(index + 1)
        review_rows.append(
            {
                "pair_id": pair_id,
                "primary_candidate_id": f"primary-{index:02d}",
                "control_candidate_id": f"control-{index:02d}",
                "source_pair_order": index + 1,
                "finalist_outcome": (
                    "FROZEN_DECODER_V2_TRAIN_ONLY_FINALIST"
                    if index < 22
                    else "ECONOMIC_ADMISSION_BLOCKED_NO_BACKFILL"
                ),
                "decoder_id": "TOPK_10_EQUAL",
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "search_score": base / 100,
                "train_rank_ic_mean": base / 10000,
                "train_rank_ic_hit_rate": 0.5 + base / 1000,
                "train_regime_stability_score": -1 + base / 100,
                "primary_continuous_book_net_reward": base / 10,
                "primary_cumulative_net_return": base / 100,
                "primary_cumulative_realized_trade_pnl_cny": base * 1000,
                "primary_ending_unrealized_pnl_cny": base * 10,
                "primary_net_return_per_turnover": base / 20,
                "matched_continuous_book_net_reward_increment": base / 50,
                "matched_cumulative_net_return_increment": base / 1000,
            }
        )
        transition_rows.append(
            {
                "pair_id": pair_id,
                "candidate_id": f"primary-{index:02d}",
                "signal_rank_ic_mean": base / 10000,
            }
        )
    review_path = input_root / "freeze" / "finalist_review_ledger.parquet"
    transition_path = input_root / "decoder" / "candidate_transition_matrix.parquet"
    pd.DataFrame(review_rows).to_parquet(review_path, index=False)
    pd.DataFrame(transition_rows).to_parquet(transition_path, index=False)
    oos_rows: list[dict[str, object]] = []
    for index, pair_id in enumerate(pair_ids[:22]):
        survivor = index < 10
        oos_rows.append(
            {
                "pair_id": pair_id,
                "finalist_order": index + 1,
                "absolute_reward_positive": True,
                "absolute_return_positive": True,
                "matched_reward_increment_positive": survivor,
                "matched_return_increment_positive": survivor,
                "all_four_economic_gates_positive": survivor,
                "interstage_filter_applied": False,
                "promotion_authorized": False,
                "primary_continuous_book_net_reward": 1.0 + index,
                "primary_cumulative_net_return": 0.01 + index / 100,
                "matched_continuous_book_net_reward_increment": (
                    0.1 if survivor else -0.1
                ),
                "matched_cumulative_net_return_increment": (
                    0.01 if survivor else -0.01
                ),
                "primary_mean_one_way_turnover": 0.2,
                "primary_net_return_per_turnover": 0.5,
                "primary_daily_net_return_p10": -0.01,
                "primary_daily_net_return_worst": -0.02,
                "primary_quarterly_regime_positive_share": 0.5,
                "primary_quarterly_regime_worst_return": -0.03,
            }
        )
    oos_path = input_root / "oos" / "oos_pair_metrics.parquet"
    pd.DataFrame(oos_rows).to_parquet(oos_path, index=False)
    contract = {
        "schema_version": "cn_alpha_selection_diagnostic_v1_contract",
        "status": "ACTIVE_ZERO_FINANCIAL_READ_DIAGNOSTIC_CONTRACT",
        "source_artifacts": [
            {
                "scope": "freeze",
                "name": review_path.name,
                "sha256": _sha256(review_path),
            },
            {
                "scope": "decoder",
                "name": transition_path.name,
                "sha256": _sha256(transition_path),
            },
            {
                "scope": "oos",
                "name": oos_path.name,
                "sha256": _sha256(oos_path),
            },
        ],
        "allowed_train_visible_numeric_features": numeric_features,
        "allowed_train_visible_categorical_features": ["route_id"],
        "allowed_derived_train_features": {
            "realized_pnl_share": "fixed",
            "train_signal_to_decoder_rank_displacement": "fixed",
            "decoder_absolute_and_matched_margin_floor": "fixed",
            "decoder_reward_absolute_and_matched_margin_floor": "fixed",
        },
        "diagnostic_plan": {
            "fixed_bootstrap_seed": 7,
            "bootstrap_iterations": 50,
            "fixed_precision_k": [5, 10, 15, 20],
        },
        "predeclared_feature_directions": {
            "higher_is_better": numeric_features
            + [
                "realized_pnl_share",
                "decoder_absolute_and_matched_margin_floor",
                "decoder_reward_absolute_and_matched_margin_floor",
            ],
            "lower_is_better": ["train_signal_to_decoder_rank_displacement"],
        },
        "predeclared_ranking_heuristics": [
            {"ranker_id": "CURRENT_SEARCH_SCORE_BASELINE"},
            {"ranker_id": "TRAIN_DECODER_ECONOMIC_FLOOR"},
            {"ranker_id": "THREE_AXIS_EQUAL_RANK"},
            {"ranker_id": "CONSERVATIVE_THREE_AXIS_FLOOR"},
        ],
        "experimental_ranker_freeze_gate": {
            "minimum_top10_survivor_count": 6,
            "must_strictly_beat_current_search_score_top10": True,
            "leave_one_out_top10_survivor_count_minimum": 5,
        },
    }
    contract_path = root / "contract.json"
    _write_json(contract_path, contract)
    return input_root, contract_path


def test_bh_fdr_is_monotone_and_bounded() -> None:
    adjusted = subject._bh_fdr([0.01, 0.02, 0.5, None])
    assert adjusted[0] <= adjusted[1] <= adjusted[2] <= 1.0
    assert adjusted[3] is None


def test_build_and_verify_never_imputes_unlabeled_oos(tmp_path: Path) -> None:
    input_root, contract_path = _synthetic_inputs(tmp_path)
    output_root = tmp_path / "output"
    result = subject.build_diagnostic(
        contract_path=contract_path,
        input_root=input_root,
        output_root=output_root,
        builder_commit_sha="test-sha",
    )
    assert result["status"].endswith("HOLD_RESEARCH")
    audit = subject.verify_diagnostic(
        output_root=output_root,
        contract_path=contract_path,
        input_root=input_root,
    )
    assert audit["status"] == "PASS"
    diagnosis = pd.read_parquet(output_root / "candidate_diagnosis.parquet")
    assert diagnosis["diagnostic_group"].value_counts().to_dict() == {
        "ADAPTIVE_VALIDATION_ABSOLUTE_ONLY_RELATIVE_INCOMPLETE": 12,
        "ADAPTIVE_VALIDATION_SURVIVOR": 10,
        "TRAIN_SCREEN_REJECT_OOS_LABEL_MISSING": 10,
    }
    unlabeled = diagnosis.loc[~diagnosis["oos_label_observed"]]
    oos_value_columns = [
        column
        for column in diagnosis
        if column.startswith("oos_") and column != "oos_label_observed"
    ]
    assert unlabeled[oos_value_columns].isna().all().all()
    ceiling = json.loads(
        (output_root / "selection_ceiling.json").read_text(encoding="utf-8")
    )
    assert ceiling["scope"] == "22_LABELED_ADAPTIVE_VALIDATION_PAIRS_ONLY"
    assert ceiling["full_32_known_survivor_rate_lower_bound"] == 10 / 32
    assert ceiling["full_32_logical_survivor_rate_upper_bound_if_all_unlabeled_survive"] == 20 / 32
    audit = independent_audit.audit(
        contract_path=contract_path,
        input_root=input_root,
        output_root=output_root,
        audit_root=tmp_path / "independent_audit",
        auditor_commit_sha="test-auditor-sha",
    )
    assert audit["status"] == "PASS"


def test_quality_percentile_fails_closed_on_missing() -> None:
    values = pd.Series([1.0, np.nan], name="feature")
    try:
        subject._quality_percentile(values, higher_is_better=True)
    except RuntimeError as exc:
        assert "missing values" in str(exc)
    else:
        raise AssertionError("missing ranker feature did not fail closed")
