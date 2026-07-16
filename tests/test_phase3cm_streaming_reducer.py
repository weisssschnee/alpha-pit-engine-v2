from __future__ import annotations

import numpy as np
import pandas as pd

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    _build_eval_time_index,
    _candidate_portfolio_rows_from_precomputed_time_groups,
    _rank_by_eval_time_index,
    _reward_atoms_for_candidate,
)
from our_system_phase2.services.phase3cm_streaming_portfolio import BatchedPortfolioKernel
from our_system_phase2.services.phase3cm_streaming_reducer import StreamingPortfolioReducer


def _fixture() -> tuple[pd.DataFrame, np.ndarray, dict[int, np.ndarray]]:
    rows: list[dict[str, object]] = []
    for minute in range(6):
        for code_index, code in enumerate(("A", "B", "C", "D", "E", "F")):
            rows.append(
                {
                    "trade_time": pd.Timestamp("2025-01-02 09:30") + pd.Timedelta(minutes=minute),
                    "code": code,
                    "signal": float((code_index + 2 * minute) % 5),
                    "ret1": (code_index - 2.0 + minute * 0.1) / 100.0,
                    "ret2": (code_index * 0.5 + minute * 0.2) / 100.0,
                }
            )
    frame = pd.DataFrame(rows)
    signals = np.vstack((frame["signal"].to_numpy(dtype=float), -frame["signal"].to_numpy(dtype=float)))
    return frame, signals, {1: frame["ret1"].to_numpy(dtype=float), 5: frame["ret2"].to_numpy(dtype=float)}


def _legacy_rows(frame: pd.DataFrame, signal: np.ndarray, labels: dict[int, np.ndarray], direction: str) -> list[dict[str, object]]:
    index = _build_eval_time_index(frame)
    signal_series = pd.Series(signal)
    return _candidate_portfolio_rows_from_precomputed_time_groups(
        candidate={"candidate_id": "c", "expression_hash": "h", "open_direction": direction},
        eval_time_index=index,
        labels=pd.DataFrame({f"fwd_ret_{h}m": value for h, value in labels.items()}),
        signal=signal_series,
        signal_rank=_rank_by_eval_time_index(signal_series, index),
        split_by_time={pd.Timestamp(value): "train" for value in frame["trade_time"].unique()},
        shard_index=0,
        horizons=tuple(labels),
        min_obs=5,
        cost_bps=5.0,
        top_quantile=0.2,
        portfolio_mode="long_only_top",
    )


def test_streaming_reducer_matches_coordinate_reference_atoms() -> None:
    frame, signals, labels = _fixture()
    time_ids = pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64)
    code_ids = pd.factorize(frame["code"], sort=True)[0].astype(np.int32)
    kernel = BatchedPortfolioKernel(
        candidate_count=2,
        code_count=6,
        horizons=(1, 5),
        compute_threads=2,
        min_obs=5,
        top_quantile=0.2,
        cost_bps=5.0,
        portfolio_mode="long_only_top",
    )
    result = kernel.evaluate_block(
        signals=signals,
        labels=labels,
        time_ids=time_ids,
        code_ids=code_ids,
        day_ids=np.zeros(len(frame), dtype=np.int32),
        directions=np.array([1.0, -1.0]),
        day_count=1,
    )
    candidates = [
        {"candidate_id": "c0", "expression_hash": "h0"},
        {"candidate_id": "c1", "expression_hash": "h1"},
    ]
    reducer = StreamingPortfolioReducer(candidates=candidates, horizons=(1, 5))
    reducer.update(result, day_labels=("2025-01-02",))
    observed = reducer.reward_atoms()
    for candidate_index, direction in enumerate(("long_top", "long_bottom")):
        legacy_rows = _legacy_rows(frame, signals[candidate_index], labels, direction)
        expected = _reward_atoms_for_candidate(candidates[candidate_index], legacy_rows, (1, 5))
        expected = sorted(
            [row for row in expected if row["split"] == "train"],
            key=lambda row: (str(row["horizon_min"]), str(row["trade_date"])),
        )
        actual = sorted(
            [row for row in observed if row["candidate_id"] == f"c{candidate_index}"],
            key=lambda row: (str(row["horizon_min"]), str(row["trade_date"])),
        )
        assert len(actual) == len(expected)
        for left, right in zip(actual, expected):
            for key in (
                "curve_count",
                "net_return_sum",
                "raw_return_sum",
                "net_positive_count",
                "downside_square_sum",
                "market_mean_return_sum",
                "market_mean_return_count",
                "turnover_sum",
                "turnover_count",
                "rank_ic_sum",
                "rank_ic_count",
                "rank_ic_positive_count",
            ):
                np.testing.assert_allclose(float(left[key]), float(right[key]), rtol=1e-12, atol=1e-12)
    assert reducer.coordinate_rows_retained == 0


def test_reducer_payload_round_trip_is_exact() -> None:
    reducer = StreamingPortfolioReducer(
        candidates=[{"candidate_id": "c", "expression_hash": "h"}],
        horizons=(1,),
    )
    payload = reducer.continuation_payload()
    restored = StreamingPortfolioReducer(
        candidates=[{"candidate_id": "c", "expression_hash": "h"}],
        horizons=(1,),
    )
    restored.restore_continuation_payload(payload)
    assert restored.continuation_payload()["candidate_identity"] == payload["candidate_identity"]
    assert restored.coordinate_rows_retained == 0


def test_reducer_pair_batches_match_single_full_candidate_update() -> None:
    frame, signals, labels = _fixture()
    time_ids = pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64)
    code_ids = pd.factorize(frame["code"], sort=True)[0].astype(np.int32)
    candidates = [
        {"candidate_id": f"c{index}", "expression_hash": f"h{index}"}
        for index in range(4)
    ]
    four_signals = np.vstack((signals, signals * 0.5))
    common = dict(
        code_count=6,
        horizons=(1, 5),
        compute_threads=2,
        min_obs=5,
        top_quantile=0.2,
        cost_bps=5.0,
        portfolio_mode="long_only_top",
    )
    full = BatchedPortfolioKernel(candidate_count=4, **common).evaluate_block(
        signals=four_signals,
        labels=labels,
        time_ids=time_ids,
        code_ids=code_ids,
        day_ids=np.zeros(len(frame), dtype=np.int32),
        directions=np.array([1.0, -1.0, 1.0, -1.0]),
        day_count=1,
    )
    full_reducer = StreamingPortfolioReducer(candidates=candidates, horizons=(1, 5))
    full_reducer.update(full, day_labels=("2025-01-02",))

    batched_reducer = StreamingPortfolioReducer(candidates=candidates, horizons=(1, 5))
    for batch_index, indices in enumerate(((0, 1), (2, 3))):
        result = BatchedPortfolioKernel(candidate_count=2, **common).evaluate_block(
            signals=four_signals[list(indices)],
            labels=labels,
            time_ids=time_ids,
            code_ids=code_ids,
            day_ids=np.zeros(len(frame), dtype=np.int32),
            directions=np.array([1.0, -1.0]),
            day_count=1,
        )
        batched_reducer.update(
            result,
            day_labels=("2025-01-02",),
            candidate_indices=indices,
            complete_block=batch_index == 1,
        )

    np.testing.assert_allclose(batched_reducer.stats, full_reducer.stats, rtol=0.0, atol=0.0)
    np.testing.assert_allclose(
        batched_reducer.daily["2025-01-02"],
        full_reducer.daily["2025-01-02"],
        rtol=0.0,
        atol=0.0,
    )
    assert batched_reducer.completed_block_count == 1
    assert batched_reducer.behavior_blocks == full_reducer.behavior_blocks
