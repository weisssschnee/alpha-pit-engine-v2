from __future__ import annotations

import math
import hashlib
import json
from pathlib import Path

import pandas as pd

from scripts.build_cn_alpha_autopsy_v1 import (
    MISSING,
    _portfolio_status,
    _signal_status,
    _stable_hash,
    _transition,
    _verify_manifest,
    aggregate_signal_quality,
    build_tables,
    rank_preservation_report,
)


def _prepared() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "pair_id": "p1",
                "primary_candidate_id": "c1",
                "control_candidate_id": "k1",
                "route_id": "Slow Temporal",
                "train_rank_ic_mean": 0.04,
                "train_rank_ic_hit_rate": 0.6,
            },
            {
                "pair_id": "p2",
                "primary_candidate_id": "c2",
                "control_candidate_id": "k2",
                "route_id": "Slow Cross-sectional",
                "train_rank_ic_mean": 0.03,
                "train_rank_ic_hit_rate": 0.55,
            },
        ]
    )


def _atoms() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for candidate_id, sign in (("c1", 1.0), ("c2", -1.0)):
        for horizon in ("all", "1", "5", "15", "30"):
            for day, multiplier in (("2025-01-02", 1.0), ("2025-01-03", 2.0)):
                rows.append(
                    {
                        "candidate_id": candidate_id,
                        "split": "validation",
                        "horizon_min": horizon,
                        "trade_date": day,
                        "curve_count": 10,
                        "net_return_sum": sign * multiplier,
                        "raw_return_sum": sign * multiplier + 0.1,
                        "turnover_sum": 2.0,
                        "turnover_count": 10,
                        "rank_ic_sum": sign * multiplier * 0.2,
                        "rank_ic_count": 10,
                        "rank_ic_positive_count": 10 if sign > 0 else 0,
                    }
                )
    return pd.DataFrame(rows)


def test_signal_quality_uses_all_horizon_without_double_counting() -> None:
    result = aggregate_signal_quality(_atoms(), _prepared()).set_index("candidate_id")
    assert math.isclose(result.loc["c1", "oos_rank_ic_mean"], 0.03)
    assert math.isclose(result.loc["c2", "oos_rank_ic_mean"], -0.03)
    assert result.loc["c1", "oos_rank_ic_hit_rate"] == 1.0
    assert result.loc["c2", "oos_rank_ic_hit_rate"] == 0.0
    assert result.loc["c1", "oos_daily_observation_count"] == 2
    assert result.loc["c1", "oos_sample_grade"] == "WEAK"
    assert result.loc["c1", "train_icir_availability"] == MISSING
    assert result.loc["c1", "oos_top_bottom_spread_availability"] == MISSING
    assert math.isclose(result.loc["c1", "oos_30m_rank_ic_mean"], 0.03)


def test_rank_transition_reports_spearman_and_retention() -> None:
    frame = pd.DataFrame(
        {
            "pair_id": [f"p{value:02d}" for value in range(10)],
            "signal": list(range(10)),
            "portfolio": list(reversed(range(10))),
        }
    )
    result = _transition(frame, "signal", "portfolio")
    assert result["n_valid"] == 10
    assert math.isclose(result["spearman"], -1.0)
    assert result["top_decile_k"] == 1
    assert result["top_decile_retention"] == 0.0
    assert result["top5_retention"] == 0.0


def test_multilabel_stage_classifiers_do_not_collapse_dimensions() -> None:
    assert _signal_status(0.1, 0.2) == "STABLE"
    assert _signal_status(0.1, -0.1) == "OOS_DECAY"
    assert _signal_status(-0.1, 0.1) == "OOS_DIVERGENT"
    assert _signal_status(-0.1, -0.2) == "FAILURE"
    assert _portfolio_status(0.1, -0.01) == "FAILED"
    assert _portfolio_status(0.1, 0.01) == "PASS"
    assert _portfolio_status(-0.1, 0.01) == "NOT_SIGNAL_STABLE"


def test_manifest_verification_accepts_declared_zero_byte_artifact(
    tmp_path: Path,
) -> None:
    empty = tmp_path / "empty.jsonl"
    empty.write_bytes(b"")
    manifest = {
        "status": "TEST_CLOSED_IMMUTABLE",
        "selection_payload_sha256": "selection",
        "artifacts": [
            {
                "path": str(empty),
                "bytes": 0,
                "sha256": hashlib.sha256(b"").hexdigest(),
            }
        ],
    }
    manifest["manifest_body_sha256"] = _stable_hash(manifest)
    path = tmp_path / "closure.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    verified = _verify_manifest(
        path,
        allowed_statuses=("TEST_CLOSED_IMMUTABLE",),
    )
    assert verified["artifact_count_verified"] == 1


def test_build_tables_keeps_multilabel_route_summary_columns() -> None:
    prepared = _prepared().assign(
        primary_composite_reward=[0.5, 0.4],
        matched_train_increment=[0.2, 0.1],
        search_score=[0.2, 0.1],
        train_mean_one_way_turnover=[0.3, 0.2],
    )
    signal = aggregate_signal_quality(_atoms(), prepared)
    identity = prepared[
        ["pair_id", "primary_candidate_id", "control_candidate_id", "route_id"]
    ]
    oos = identity.assign(
        primary_validation_report_metric=[0.3, -0.2],
        pair_validation_report_metric=[0.2, -0.3],
        validation_search_score=[0.2, -0.3],
    )
    mtm_pairs = identity.assign(
        primary_mark_to_market_net_reward=[-0.2, -0.3],
        mark_to_market_net_increment=[0.1, -0.1],
        primary_cumulative_net_return=[-0.02, -0.03],
        cumulative_net_return_increment=[0.01, -0.01],
        primary_mean_one_way_turnover=[0.4, 0.5],
    )
    mtm_candidates = pd.DataFrame(
        [
            {
                "pair_id": row.pair_id,
                "candidate_id": row.primary_candidate_id,
                "route_id": row.route_id,
                "initial_cash_cny": 1_000_000.0,
                "ending_nav_cny": 990_000.0,
                "cumulative_net_return": -0.01,
                "total_fees_cny": 100.0,
                "trade_count": 10,
                "fill_count": 10,
                "blocked_buy_count": 0,
                "blocked_sell_count": 1,
                "ending_holding_count": 1,
                "ending_holdings_weight": 0.4,
                "ending_holdings": [{"market_value_cny": 400_000.0}],
                "a_share_mean_one_way_turnover": 0.4,
            }
            for row in prepared.itertuples(index=False)
        ]
    )
    tables = build_tables(prepared, oos, mtm_pairs, mtm_candidates, signal)
    assert len(tables["candidate_diagnosis"]) == 2
    assert len(tables["route_summary"]) == 2
    assert set(tables["route_summary"]["route_id"]) == {
        "Slow Temporal",
        "Slow Cross-sectional",
    }
    rank_report = rank_preservation_report(tables)
    assert len(
        rank_report["validation_predictive_waterfall"]["adjacent_transitions"]
    ) == 3
    execution = tables["execution_cost_attribution"]
    assert execution["fee_only_reconciliation_status"].eq("PASS").all()
    without_initial = build_tables(
        prepared,
        oos,
        mtm_pairs,
        mtm_candidates.drop(columns=["initial_cash_cny"]),
        signal,
    )["execution_cost_attribution"]
    assert without_initial["initial_cash_cny_availability"].eq(MISSING).all()
    assert without_initial["fee_only_reconciliation_status"].eq(
        "UNAVAILABLE_INITIAL_CASH_NOT_PERSISTED"
    ).all()
