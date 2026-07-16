"""Batched native full-market portfolio mapping for Phase3CM streaming."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

try:  # pragma: no cover - native path is exercised on 77o.
    from numba import njit, prange, set_num_threads
except Exception:  # pragma: no cover
    njit = None
    prange = range
    set_num_threads = None


STAT_FIELDS = (
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
    "support_count",
    "selected_count",
)

DAILY_FIELDS = STAT_FIELDS


def _boundaries(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(values) == 0:
        empty = np.empty(0, dtype=np.int64)
        return empty, empty
    cuts = np.flatnonzero(np.r_[True, values[1:] != values[:-1], True]).astype(np.int64)
    return cuts[:-1], cuts[1:]


if njit is not None:

    @njit(cache=True)
    def _rank_average(values: np.ndarray) -> np.ndarray:
        out = np.empty(values.shape[0], dtype=np.float64)
        out[:] = np.nan
        count = 0
        for index in range(values.shape[0]):
            if np.isfinite(values[index]):
                count += 1
        if count == 0:
            return out
        positions = np.empty(count, dtype=np.int64)
        valid_values = np.empty(count, dtype=np.float64)
        cursor = 0
        for index in range(values.shape[0]):
            if np.isfinite(values[index]):
                positions[cursor] = index
                valid_values[cursor] = values[index]
                cursor += 1
        order = np.argsort(valid_values)
        tie_start = 0
        while tie_start < count:
            tie_end = tie_start + 1
            while tie_end < count and valid_values[order[tie_end]] == valid_values[order[tie_start]]:
                tie_end += 1
            rank = (((tie_start + 1) + tie_end) / 2.0) / count
            for index in range(tie_start, tie_end):
                out[positions[order[index]]] = rank
            tie_start = tie_end
        return out


    @njit(cache=True)
    def _linear_quantile(values: np.ndarray, quantile: float) -> float:
        ordered = np.sort(values)
        if ordered.shape[0] == 1:
            return ordered[0]
        position = (ordered.shape[0] - 1) * quantile
        lower = int(np.floor(position))
        upper = int(np.ceil(position))
        if lower == upper:
            return ordered[lower]
        weight = position - lower
        return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


    @njit(cache=True)
    def _pearson(left: np.ndarray, right: np.ndarray) -> float:
        count = left.shape[0]
        if count < 2:
            return np.nan
        left_centered = left - np.mean(left)
        right_centered = right - np.mean(right)
        numerator = np.dot(left_centered, right_centered)
        left_scale = np.dot(left_centered, left_centered)
        right_scale = np.dot(right_centered, right_centered)
        denominator = np.sqrt(left_scale * right_scale)
        return numerator / denominator if denominator > 0.0 else np.nan


    @njit(cache=True, parallel=True)
    def _portfolio_kernel(
        signals: np.ndarray,
        labels: np.ndarray,
        starts: np.ndarray,
        ends: np.ndarray,
        code_ids: np.ndarray,
        day_ids: np.ndarray,
        directions: np.ndarray,
        selection_epoch: np.ndarray,
        epoch_counter: np.ndarray,
        min_obs: int,
        top_quantile: float,
        one_way_cost: float,
        day_count: int,
        excess_market: bool,
    ) -> tuple[np.ndarray, np.ndarray]:
        candidate_count = signals.shape[0]
        horizon_count = labels.shape[0]
        stats = np.zeros((candidate_count, horizon_count + 1, len(STAT_FIELDS)), dtype=np.float64)
        daily = np.zeros((candidate_count, horizon_count + 1, day_count, len(DAILY_FIELDS)), dtype=np.float64)
        for candidate in prange(candidate_count):
            direction = directions[candidate]
            for group in range(starts.shape[0]):
                start = starts[group]
                end = ends[group]
                signal_part = signals[candidate, start:end]
                signal_rank = _rank_average(signal_part)
                all_net = 0.0
                all_raw = 0.0
                all_market = 0.0
                all_turnover = 0.0
                all_rank_ic = 0.0
                all_support = 0.0
                all_selected = 0.0
                sleeve_count = 0
                rank_ic_sleeves = 0
                day = int(day_ids[start])
                for horizon in range(horizon_count):
                    ret_part = labels[horizon, start:end]
                    valid_count = 0
                    for local in range(end - start):
                        if np.isfinite(signal_rank[local]) and np.isfinite(ret_part[local]):
                            valid_count += 1
                    if valid_count < min_obs:
                        continue
                    ranks = np.empty(valid_count, dtype=np.float64)
                    returns = np.empty(valid_count, dtype=np.float64)
                    codes = np.empty(valid_count, dtype=np.int32)
                    cursor = 0
                    for local in range(end - start):
                        if np.isfinite(signal_rank[local]) and np.isfinite(ret_part[local]):
                            ranks[cursor] = signal_rank[local]
                            returns[cursor] = ret_part[local]
                            codes[cursor] = code_ids[start + local]
                            cursor += 1
                    low = _linear_quantile(ranks, top_quantile)
                    high = _linear_quantile(ranks, 1.0 - top_quantile)
                    selected_count = 0
                    selected_return_sum = 0.0
                    market_sum = 0.0
                    top_sum = 0.0
                    top_count = 0
                    bottom_sum = 0.0
                    bottom_count = 0
                    current_epoch = epoch_counter[candidate, horizon] + 1
                    previous_epoch = epoch_counter[candidate, horizon]
                    intersection = 0
                    for index in range(valid_count):
                        market_sum += returns[index]
                        if ranks[index] >= high:
                            top_sum += returns[index]
                            top_count += 1
                        if ranks[index] <= low:
                            bottom_sum += returns[index]
                            bottom_count += 1
                        chosen = ranks[index] >= high if direction > 0.0 else ranks[index] <= low
                        if chosen:
                            code = codes[index]
                            if previous_epoch > 0 and selection_epoch[candidate, horizon, code] == previous_epoch:
                                intersection += 1
                            selection_epoch[candidate, horizon, code] = current_epoch
                            selected_count += 1
                            selected_return_sum += returns[index]
                    if selected_count == 0 or top_count == 0 or bottom_count == 0:
                        continue
                    epoch_counter[candidate, horizon] = current_epoch
                    turnover = 1.0 if previous_epoch == 0 else 1.0 - intersection / selected_count
                    market_mean = market_sum / valid_count
                    selected_mean = selected_return_sum / selected_count
                    raw_return = selected_mean - market_mean if excess_market else selected_mean
                    net_return = raw_return - one_way_cost * turnover
                    return_rank = _rank_average(returns)
                    rank_ic_raw = _pearson(ranks, return_rank)
                    rank_ic = rank_ic_raw * direction if np.isfinite(rank_ic_raw) else np.nan

                    stats[candidate, horizon, 0] += 1.0
                    stats[candidate, horizon, 1] += net_return
                    stats[candidate, horizon, 2] += raw_return
                    stats[candidate, horizon, 3] += 1.0 if net_return > 0.0 else 0.0
                    stats[candidate, horizon, 4] += min(0.0, net_return) ** 2
                    stats[candidate, horizon, 5] += market_mean
                    stats[candidate, horizon, 6] += 1.0
                    stats[candidate, horizon, 7] += turnover
                    stats[candidate, horizon, 8] += 1.0
                    if np.isfinite(rank_ic):
                        stats[candidate, horizon, 9] += rank_ic
                        stats[candidate, horizon, 10] += 1.0
                        stats[candidate, horizon, 11] += 1.0 if rank_ic > 0.0 else 0.0
                    stats[candidate, horizon, 12] += valid_count
                    stats[candidate, horizon, 13] += selected_count
                    daily[candidate, horizon, day, 0] += 1.0
                    daily[candidate, horizon, day, 1] += net_return
                    daily[candidate, horizon, day, 2] += raw_return
                    daily[candidate, horizon, day, 3] += 1.0 if net_return > 0.0 else 0.0
                    daily[candidate, horizon, day, 4] += min(0.0, net_return) ** 2
                    daily[candidate, horizon, day, 5] += market_mean
                    daily[candidate, horizon, day, 6] += 1.0
                    daily[candidate, horizon, day, 7] += turnover
                    daily[candidate, horizon, day, 8] += 1.0
                    if np.isfinite(rank_ic):
                        daily[candidate, horizon, day, 9] += rank_ic
                        daily[candidate, horizon, day, 10] += 1.0
                        daily[candidate, horizon, day, 11] += 1.0 if rank_ic > 0.0 else 0.0
                    daily[candidate, horizon, day, 12] += valid_count
                    daily[candidate, horizon, day, 13] += selected_count

                    all_net += net_return
                    all_raw += raw_return
                    all_market += market_mean
                    all_turnover += turnover
                    all_support += valid_count
                    all_selected += selected_count
                    sleeve_count += 1
                    if np.isfinite(rank_ic):
                        all_rank_ic += rank_ic
                        rank_ic_sleeves += 1

                if sleeve_count > 0:
                    slot = horizon_count
                    net_return = all_net / sleeve_count
                    raw_return = all_raw / sleeve_count
                    market_mean = all_market / sleeve_count
                    turnover = all_turnover / sleeve_count
                    stats[candidate, slot, 0] += 1.0
                    stats[candidate, slot, 1] += net_return
                    stats[candidate, slot, 2] += raw_return
                    stats[candidate, slot, 3] += 1.0 if net_return > 0.0 else 0.0
                    stats[candidate, slot, 4] += min(0.0, net_return) ** 2
                    stats[candidate, slot, 5] += market_mean
                    stats[candidate, slot, 6] += 1.0
                    stats[candidate, slot, 7] += turnover
                    stats[candidate, slot, 8] += 1.0
                    if rank_ic_sleeves > 0:
                        rank_ic = all_rank_ic / rank_ic_sleeves
                        stats[candidate, slot, 9] += rank_ic
                        stats[candidate, slot, 10] += 1.0
                        stats[candidate, slot, 11] += 1.0 if rank_ic > 0.0 else 0.0
                    stats[candidate, slot, 12] += all_support / sleeve_count
                    stats[candidate, slot, 13] += all_selected / sleeve_count
                    daily[candidate, slot, day, 0] += 1.0
                    daily[candidate, slot, day, 1] += net_return
                    daily[candidate, slot, day, 2] += raw_return
                    daily[candidate, slot, day, 3] += 1.0 if net_return > 0.0 else 0.0
                    daily[candidate, slot, day, 4] += min(0.0, net_return) ** 2
                    daily[candidate, slot, day, 5] += market_mean
                    daily[candidate, slot, day, 6] += 1.0
                    daily[candidate, slot, day, 7] += turnover
                    daily[candidate, slot, day, 8] += 1.0
                    if rank_ic_sleeves > 0:
                        daily[candidate, slot, day, 9] += all_rank_ic / rank_ic_sleeves
                        daily[candidate, slot, day, 10] += 1.0
                        daily[candidate, slot, day, 11] += 1.0 if all_rank_ic / rank_ic_sleeves > 0.0 else 0.0
                    daily[candidate, slot, day, 12] += all_support / sleeve_count
                    daily[candidate, slot, day, 13] += all_selected / sleeve_count
        return stats, daily

else:  # pragma: no cover
    _portfolio_kernel = None


@dataclass(slots=True)
class PortfolioBlockResult:
    stats: np.ndarray
    daily: np.ndarray
    audit: dict[str, Any]
    coordinate_rows_retained: int = 0


class BatchedPortfolioKernel:
    """One native call maps a complete block for all candidate members."""

    def __init__(
        self,
        *,
        candidate_count: int,
        code_count: int,
        horizons: Sequence[int],
        compute_threads: int,
        min_obs: int,
        top_quantile: float,
        cost_bps: float,
        portfolio_mode: str,
    ) -> None:
        if candidate_count <= 0 or code_count <= 0:
            raise ValueError("candidate_count and code_count must be positive")
        if compute_threads <= 0 or compute_threads > 24:
            raise ValueError("compute_threads must be between 1 and 24")
        if not horizons:
            raise ValueError("at least one horizon is required")
        if portfolio_mode not in {"long_only_top", "long_only_excess_market"}:
            raise ValueError("streaming qualification only supports the frozen long-only portfolio modes")
        self.candidate_count = int(candidate_count)
        self.code_count = int(code_count)
        self.horizons = tuple(int(value) for value in horizons)
        self.compute_threads = int(compute_threads)
        self.min_obs = int(min_obs)
        self.top_quantile = float(top_quantile)
        self.cost_bps = float(cost_bps)
        self.portfolio_mode = str(portfolio_mode)
        self.selection_epoch = np.zeros(
            (self.candidate_count, len(self.horizons), self.code_count),
            dtype=np.int32,
        )
        self.epoch_counter = np.zeros((self.candidate_count, len(self.horizons)), dtype=np.int32)

    def evaluate_block(
        self,
        *,
        signals: np.ndarray,
        labels: Mapping[int, np.ndarray],
        time_ids: np.ndarray,
        code_ids: np.ndarray,
        day_ids: np.ndarray,
        directions: np.ndarray,
        day_count: int,
    ) -> PortfolioBlockResult:
        if _portfolio_kernel is None:
            raise RuntimeError("Numba is required for the batched portfolio hot path")
        signal_array = np.asarray(signals, dtype=np.float64)
        time_ids = np.asarray(time_ids, dtype=np.int64)
        code_ids = np.asarray(code_ids, dtype=np.int32)
        day_ids = np.asarray(day_ids, dtype=np.int32)
        directions = np.asarray(directions, dtype=np.float64)
        if signal_array.shape != (self.candidate_count, len(time_ids)):
            raise ValueError("signal block shape does not match frozen candidate count")
        if code_ids.shape != time_ids.shape or day_ids.shape != time_ids.shape:
            raise ValueError("coordinate array shape mismatch")
        if directions.shape != (self.candidate_count,):
            raise ValueError("direction array shape mismatch")
        if len(time_ids) and bool(np.any(time_ids[1:] < time_ids[:-1])):
            raise ValueError("portfolio block must contain a complete time-major global barrier")
        if len(code_ids) and (int(code_ids.min()) < 0 or int(code_ids.max()) >= self.code_count):
            raise ValueError("code id outside frozen symbol universe")
        label_array = np.vstack(
            [np.asarray(labels[horizon], dtype=np.float64) for horizon in self.horizons]
        )
        if label_array.shape != (len(self.horizons), len(time_ids)):
            raise ValueError("label block shape mismatch")
        starts, ends = _boundaries(time_ids)
        if set_num_threads is not None:
            set_num_threads(self.compute_threads)
        started_wall = time.perf_counter()
        started_cpu = time.process_time()
        stats, daily = _portfolio_kernel(
            signal_array,
            label_array,
            starts,
            ends,
            code_ids,
            day_ids,
            directions,
            self.selection_epoch,
            self.epoch_counter,
            self.min_obs,
            self.top_quantile,
            self.cost_bps / 10000.0,
            int(day_count),
            self.portfolio_mode == "long_only_excess_market",
        )
        wall = time.perf_counter() - started_wall
        cpu = time.process_time() - started_cpu
        audit = {
            "native_portfolio_kernel_called": True,
            "candidate_count": self.candidate_count,
            "horizon_count": len(self.horizons),
            "row_count": int(len(time_ids)),
            "global_trade_time_count": int(len(starts)),
            "wall_seconds": wall,
            "cpu_seconds": cpu,
            "effective_cores": cpu / wall if wall > 0.0 else 0.0,
            "allocated_compute_threads": self.compute_threads,
            "parallelism_status": (
                "PARALLELISM_ENGAGED"
                if wall > 0.0 and cpu / wall >= 0.5 * self.compute_threads
                else "PARALLELISM_NOT_ENGAGED"
            ),
            "coordinate_rows_retained": 0,
            "python_pandas_hot_path_calls": 0,
            "native_thread_environment": {
                key: os.environ.get(key)
                for key in (
                    "NUMBA_NUM_THREADS",
                    "ARROW_NUM_THREADS",
                    "OMP_NUM_THREADS",
                    "MKL_NUM_THREADS",
                    "OPENBLAS_NUM_THREADS",
                    "NUMEXPR_MAX_THREADS",
                )
            },
        }
        return PortfolioBlockResult(stats=stats, daily=daily, audit=audit)

    def continuation_payload(self) -> dict[str, np.ndarray]:
        return {
            "selection_epoch": self.selection_epoch.copy(),
            "epoch_counter": self.epoch_counter.copy(),
        }

    def restore_continuation_payload(self, payload: Mapping[str, Any]) -> None:
        selection = np.asarray(payload["selection_epoch"], dtype=np.int32)
        counters = np.asarray(payload["epoch_counter"], dtype=np.int32)
        if selection.shape != self.selection_epoch.shape or counters.shape != self.epoch_counter.shape:
            raise ValueError("portfolio continuation shape drift")
        self.selection_epoch[...] = selection
        self.epoch_counter[...] = counters
