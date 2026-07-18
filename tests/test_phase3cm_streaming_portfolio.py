from __future__ import annotations

import numpy as np
import pandas as pd

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    _build_eval_time_index,
    _candidate_portfolio_rows_from_precomputed_time_groups,
    _rank_by_eval_time_index,
)
from our_system_phase2.services.phase3cm_streaming_portfolio import (
    BatchedPortfolioKernel,
    _linear_quantile,
    _linear_quantile_pair,
    _pearson,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


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
    signals = np.vstack(
        (
            frame["signal"].to_numpy(dtype=float),
            -frame["signal"].to_numpy(dtype=float),
        )
    )
    return frame, signals, {1: frame["ret1"].to_numpy(dtype=float), 5: frame["ret2"].to_numpy(dtype=float)}


def _legacy_rows(frame: pd.DataFrame, signal: np.ndarray, labels: dict[int, np.ndarray], direction: str) -> list[dict[str, object]]:
    index = _build_eval_time_index(frame)
    signal_series = pd.Series(signal)
    rank = _rank_by_eval_time_index(signal_series, index)
    label_frame = pd.DataFrame({f"fwd_ret_{h}m": value for h, value in labels.items()})
    return _candidate_portfolio_rows_from_precomputed_time_groups(
        candidate={"candidate_id": "c", "expression_hash": "h", "open_direction": direction},
        eval_time_index=index,
        labels=label_frame,
        signal=signal_series,
        signal_rank=rank,
        split_by_time={pd.Timestamp(value): "train" for value in frame["trade_time"].unique()},
        shard_index=0,
        horizons=tuple(labels),
        min_obs=5,
        cost_bps=5.0,
        top_quantile=0.2,
        portfolio_mode="long_only_top",
    )


def test_batched_native_portfolio_matches_merged_legacy_reference() -> None:
    frame, signals, labels = _fixture()
    time_ids = pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64)
    code_ids = pd.factorize(frame["code"], sort=True)[0].astype(np.int32)
    day_ids = np.zeros(len(frame), dtype=np.int32)
    kernel = BatchedPortfolioKernel(
        candidate_count=2,
        code_count=frame["code"].nunique(),
        horizons=(1, 5),
        compute_threads=2,
        min_obs=5,
        top_quantile=0.2,
        cost_bps=5.0,
        portfolio_mode="long_only_top",
    )
    observed = kernel.evaluate_block(
        signals=signals,
        labels=labels,
        time_ids=time_ids,
        code_ids=code_ids,
        day_ids=day_ids,
        directions=np.array([1.0, -1.0]),
        day_count=1,
        audit_coordinate_arrays=True,
    )
    for candidate_index, direction in enumerate(("long_top", "long_bottom")):
        rows = _legacy_rows(frame, signals[candidate_index], labels, direction)
        for horizon_index, horizon in enumerate((1, 5)):
            selected = [row for row in rows if int(row["horizon_min"]) == horizon]
            expected = {
                "curve_count": len(selected),
                "net_sum": sum(float(row["net_return"]) for row in selected),
                "raw_sum": sum(float(row["raw_return"]) for row in selected),
                "turnover_sum": sum(float(row["one_way_turnover"]) for row in selected),
                "rank_ic_sum": sum(float(row["rank_ic"]) for row in selected),
            }
            stats = observed.stats[candidate_index, horizon_index]
            assert int(stats[0]) == expected["curve_count"]
            np.testing.assert_allclose(stats[1], expected["net_sum"], rtol=1e-12, atol=1e-12)
            np.testing.assert_allclose(stats[2], expected["raw_sum"], rtol=1e-12, atol=1e-12)
            np.testing.assert_allclose(stats[7], expected["turnover_sum"], rtol=1e-12, atol=1e-12)
            np.testing.assert_allclose(stats[9], expected["rank_ic_sum"], rtol=1e-12, atol=1e-12)
        for row in rows:
            horizon_index = (1, 5).index(int(row["horizon_min"]))
            time_index = int(
                (pd.Timestamp(row["trade_time"]) - pd.Timestamp("2025-01-02 09:30"))
                / pd.Timedelta(minutes=1)
            )
            positions = np.flatnonzero(time_ids == time_index)
            chosen = positions[observed.audit_selected[candidate_index, horizon_index, positions]]
            selected_codes = sorted(frame.iloc[chosen]["code"].astype(str).tolist())
            assert row["selected_code_identity"] == stable_hash(
                {"long": selected_codes, "short": []}
            )
            assert row["portfolio_weight_identity"] == stable_hash(
                {
                    "long": [(code, 1.0 / len(selected_codes)) for code in selected_codes],
                    "short": [],
                    "portfolio_mode": "long_only_top",
                }
            )
            metrics = observed.audit_coordinate_metrics[candidate_index, horizon_index, time_index]
            np.testing.assert_allclose(metrics[0], float(row["raw_return"]), rtol=1e-12, atol=1e-12)
            np.testing.assert_allclose(metrics[1], float(row["net_return"]), rtol=1e-12, atol=1e-12)
            np.testing.assert_allclose(metrics[2], float(row["one_way_turnover"]), rtol=1e-12, atol=1e-12)
            np.testing.assert_allclose(metrics[3], float(row["rank_ic"]), rtol=1e-12, atol=1e-12)
    assert observed.coordinate_rows_retained == 0
    assert observed.audit["native_portfolio_kernel_called"] is True


def test_linear_quantile_does_not_move_an_equal_cutoff_tie_by_one_ulp() -> None:
    tied_value = np.float64(0.8659217877094972)
    values = np.array([0.1, 0.2, tied_value, tied_value], dtype=np.float64)

    cutoff = _linear_quantile(values, 0.8)

    assert cutoff == tied_value
    assert int((values >= cutoff).sum()) == 2


def _legacy_linear_quantile(values: np.ndarray, quantile: float) -> np.float64:
    ordered = np.sort(values)
    if ordered.shape[0] == 1:
        return ordered[0]
    position = (ordered.shape[0] - 1) * quantile
    lower = int(np.floor(position))
    upper = int(np.ceil(position))
    if lower == upper or ordered[lower] == ordered[upper]:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _float64_bits(value: float) -> np.uint64:
    return np.asarray(value, dtype=np.float64).view(np.uint64).item()


def test_linear_quantile_pair_is_bit_and_selection_mask_exact() -> None:
    tied_value = np.float64(0.8659217877094972)
    adjacent = np.nextafter(tied_value, np.inf)
    cases = (
        (np.array([0.25], dtype=np.float64), 0.0),
        (np.array([1.0, -1.0], dtype=np.float64), 0.25),
        (np.array([3.0, 1.0, 2.0], dtype=np.float64), 0.2),
        (np.array([4.0, 1.0, 3.0, 2.0], dtype=np.float64), 0.2),
        (np.array([0.1, 0.2, tied_value, tied_value], dtype=np.float64), 0.2),
        (np.array([0.0, -0.0], dtype=np.float64), 0.0),
        (np.array([0.1, tied_value, adjacent, 0.9], dtype=np.float64), 0.8),
        (np.array([-2.0, 0.0, 3.0], dtype=np.float64), 0.0),
        (np.array([-2.0, 0.0, 3.0], dtype=np.float64), 1.0),
    )

    for values, quantile in cases:
        expected_low = _legacy_linear_quantile(values, quantile)
        expected_high = _legacy_linear_quantile(values, 1.0 - quantile)
        observed_low, observed_high = _linear_quantile_pair(values, quantile)

        assert _float64_bits(observed_low) == _float64_bits(expected_low)
        assert _float64_bits(observed_high) == _float64_bits(expected_high)
        np.testing.assert_array_equal(values <= observed_low, values <= expected_low)
        np.testing.assert_array_equal(values >= observed_high, values >= expected_high)


def test_native_pearson_returns_nan_for_constant_rank_vector() -> None:
    observed = _pearson(
        np.full(64, np.float64(0.5)),
        np.linspace(-1.0, 1.0, 64, dtype=np.float64),
    )

    assert np.isnan(observed)


def test_portfolio_turnover_state_matches_across_block_boundary() -> None:
    frame, signals, labels = _fixture()
    time_ids = pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64)
    code_ids = pd.factorize(frame["code"], sort=True)[0].astype(np.int32)
    kwargs = dict(
        candidate_count=2,
        code_count=frame["code"].nunique(),
        horizons=(1, 5),
        compute_threads=2,
        min_obs=5,
        top_quantile=0.2,
        cost_bps=5.0,
        portfolio_mode="long_only_top",
    )
    whole = BatchedPortfolioKernel(**kwargs).evaluate_block(
        signals=signals,
        labels=labels,
        time_ids=time_ids,
        code_ids=code_ids,
        day_ids=np.zeros(len(frame), dtype=np.int32),
        directions=np.array([1.0, -1.0]),
        day_count=1,
    )
    split = 18
    streamed_kernel = BatchedPortfolioKernel(**kwargs)
    parts = []
    for positions in (slice(0, split), slice(split, None)):
        part_time = time_ids[positions]
        part_time = part_time - part_time[0]
        parts.append(
            streamed_kernel.evaluate_block(
                signals=signals[:, positions],
                labels={h: value[positions] for h, value in labels.items()},
                time_ids=part_time,
                code_ids=code_ids[positions],
                day_ids=np.zeros(len(code_ids[positions]), dtype=np.int32),
                directions=np.array([1.0, -1.0]),
                day_count=1,
            )
        )
    combined_stats = parts[0].stats + parts[1].stats
    combined_daily = parts[0].daily + parts[1].daily
    combined_stats[:, :, -1] = np.maximum(parts[0].stats[:, :, -1], parts[1].stats[:, :, -1])
    combined_daily[:, :, :, -1] = np.maximum(parts[0].daily[:, :, :, -1], parts[1].daily[:, :, :, -1])
    np.testing.assert_allclose(combined_stats, whole.stats, rtol=1e-12, atol=1e-12)
    np.testing.assert_allclose(combined_daily, whole.daily, rtol=1e-12, atol=1e-12)
    payload = streamed_kernel.continuation_payload()
    assert payload["selection_epoch"].shape == (2, 2, 6)


def test_full_market_barrier_differs_from_shard_local_mapping() -> None:
    frame, signals, labels = _fixture()
    time_ids = pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64)
    code_ids = pd.factorize(frame["code"], sort=True)[0].astype(np.int32)
    kernel = BatchedPortfolioKernel(
        candidate_count=1,
        code_count=6,
        horizons=(1,),
        compute_threads=2,
        min_obs=2,
        top_quantile=0.2,
        cost_bps=5.0,
        portfolio_mode="long_only_top",
    )
    merged = kernel.evaluate_block(
        signals=signals[:1],
        labels={1: labels[1]},
        time_ids=time_ids,
        code_ids=code_ids,
        day_ids=np.zeros(len(frame), dtype=np.int32),
        directions=np.array([1.0]),
        day_count=1,
    )
    shard_sum = 0.0
    for code_mask in (code_ids < 3, code_ids >= 3):
        local_frame = frame.loc[code_mask].reset_index(drop=True)
        local_rows = _legacy_rows(local_frame, signals[0, code_mask], {1: labels[1][code_mask]}, "long_top")
        shard_sum += sum(float(row["net_return"]) for row in local_rows)
    assert not np.isclose(float(merged.stats[0, 0, 1]), shard_sum)


def test_independent_pair_batch_kernels_match_one_full_candidate_kernel() -> None:
    frame, signals, labels = _fixture()
    time_ids = pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64)
    code_ids = pd.factorize(frame["code"], sort=True)[0].astype(np.int32)
    four_signals = np.vstack((signals, signals * 0.5))
    kwargs = dict(
        code_count=6,
        horizons=(1, 5),
        compute_threads=2,
        min_obs=5,
        top_quantile=0.2,
        cost_bps=5.0,
        portfolio_mode="long_only_top",
    )
    full_kernel = BatchedPortfolioKernel(candidate_count=4, **kwargs)
    full = full_kernel.evaluate_block(
        signals=four_signals,
        labels=labels,
        time_ids=time_ids,
        code_ids=code_ids,
        day_ids=np.zeros(len(frame), dtype=np.int32),
        directions=np.array([1.0, -1.0, 1.0, -1.0]),
        day_count=1,
    )

    parts = []
    batch_kernels = []
    for indices in ((0, 1), (2, 3)):
        kernel = BatchedPortfolioKernel(candidate_count=2, **kwargs)
        batch_kernels.append(kernel)
        parts.append(
            kernel.evaluate_block(
                signals=four_signals[list(indices)],
                labels=labels,
                time_ids=time_ids,
                code_ids=code_ids,
                day_ids=np.zeros(len(frame), dtype=np.int32),
                directions=np.array([1.0, -1.0]),
                day_count=1,
            )
        )

    np.testing.assert_allclose(
        np.concatenate([part.stats for part in parts], axis=0),
        full.stats,
        rtol=0.0,
        atol=0.0,
    )
    np.testing.assert_array_equal(
        np.concatenate([kernel.selection_epoch for kernel in batch_kernels], axis=0),
        full_kernel.selection_epoch,
    )
    np.testing.assert_array_equal(
        np.concatenate([kernel.epoch_counter for kernel in batch_kernels], axis=0),
        full_kernel.epoch_counter,
    )
