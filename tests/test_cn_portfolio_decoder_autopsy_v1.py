from __future__ import annotations

import math

import numpy as np
import pandas as pd

from scripts.build_cn_portfolio_decoder_autopsy_v1 import (
    DECODERS,
    build_pair_metrics,
    build_rank_preservation,
    decoder_weights,
    evaluate_candidate_decoder_matrix,
    forward_open_to_close_returns,
)


def _decoder(decoder_id: str) -> dict[str, object]:
    return next(dict(row) for row in DECODERS if row["decoder_id"] == decoder_id)


def test_decoder_weights_are_deterministic_and_normalized() -> None:
    signal = np.asarray([2.0, 2.0, 1.0, np.nan])
    codes = np.asarray(["000002", "000001", "000003", "000004"])
    selected, weights = decoder_weights(signal, codes, _decoder("TOPK_10_EQUAL"))
    assert selected.tolist() == [1, 0, 2]
    assert math.isclose(float(weights.sum()), 1.0)
    assert weights.tolist() == [1 / 3, 1 / 3, 1 / 3]


def test_rank_and_softmax_decoders_preserve_long_only_budget() -> None:
    signal = np.arange(60, dtype=float)
    codes = np.asarray([f"{value:06d}" for value in range(60)])
    for decoder_id in ("TOPK_20_RANK", "SOFTMAX_TOP50_TAU_0.25"):
        selected, weights = decoder_weights(signal, codes, _decoder(decoder_id))
        assert len(selected) in {20, 50}
        assert np.all(weights > 0)
        assert math.isclose(float(weights.sum()), 1.0, abs_tol=1e-12)
        assert np.all(np.diff(weights) <= 0)


def test_forward_returns_are_next_open_to_hth_session_close() -> None:
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "code": ["000001"] * 3,
            "open": [10.0, 11.0, 12.0],
            "close": [10.5, 11.5, 13.0],
        }
    )
    result = forward_open_to_close_returns(frame, horizons=(1, 2))
    assert math.isclose(result[1][0], 11.5 / 11.0 - 1.0)
    assert math.isclose(result[2][0], 13.0 / 11.0 - 1.0)
    assert np.isnan(result[2][1])


def test_candidate_matrix_and_pair_metrics_keep_session_semantics() -> None:
    dates = np.repeat(pd.to_datetime(["2024-01-01", "2024-01-02"]), 4)
    codes = np.tile(np.asarray(["1", "2", "3", "4"]), 2)
    signal = np.tile(np.asarray([4.0, 3.0, 2.0, 1.0]), 2)
    returns = {1: np.tile(np.asarray([0.04, 0.03, -0.01, -0.02]), 2)}
    groups = [np.arange(0, 4), np.arange(4, 8)]
    rows = evaluate_candidate_decoder_matrix(
        candidate={
            "pair_id": "p1",
            "candidate_id": "c1",
            "pair_member_role": "PRIMARY",
            "route_id": "R",
            "exact_identity": "x1",
        },
        signal=signal,
        codes=codes,
        dates=dates.to_numpy(),
        eligible=np.ones(8, dtype=bool),
        future_returns=returns,
        date_groups=groups,
        decoders=(_decoder("TOPK_10_EQUAL"),),
    )
    assert len(rows) == 1
    assert rows[0]["horizon_semantics"] == "STOCK_SESSION_SHIFT_NOT_MINUTE"
    assert rows[0]["signal_rank_ic_mean"] > 0.99
    control = dict(rows[0])
    control["candidate_id"] = "c0"
    control["pair_member_role"] = "CONTROL"
    control["gross_forward_return_mean"] = -0.01
    pair = build_pair_metrics(pd.DataFrame([rows[0], control]))
    assert len(pair) == 1
    assert pair.iloc[0]["matched_gross_forward_return_increment"] > 0


def test_rank_preservation_reports_retention_and_mtm_alignment() -> None:
    metrics = pd.DataFrame(
        [
            {
                "pair_id": f"p{i}",
                "candidate_id": f"c{i}",
                "pair_member_role": "PRIMARY",
                "route_id": "R",
                "decoder_id": "D",
                "forward_holding_sessions": 1,
                "signal_rank_ic_mean": float(i),
                "gross_forward_return_mean": float(i),
            }
            for i in range(10)
        ]
    )
    mtm = pd.DataFrame(
        [
            {
                "candidate_id": f"c{i}",
                "pair_member_role": "PRIMARY",
                "mark_to_market_net_reward": float(i),
                "ending_nav_cny": float(i),
            }
            for i in range(10)
        ]
    )
    result = build_rank_preservation(metrics, mtm).iloc[0]
    assert math.isclose(result["signal_to_gross_spearman"], 1.0)
    assert math.isclose(result["top_decile_retention"], 1.0)
    assert math.isclose(result["top5_retention"], 1.0)
    assert math.isclose(result["gross_to_existing_mtm_reward_spearman"], 1.0)
