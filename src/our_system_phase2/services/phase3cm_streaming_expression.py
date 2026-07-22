"""NumPy/Numba block executor for the frozen Phase3CM expression surface.

The executor intentionally separates numeric value transforms from complete
trade-time cross-sectional mapping.  It never constructs a Pandas object in
the hot path and keeps only small, explicit per-symbol continuation payloads
between blocks.
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import numpy as np

from our_system_phase2.services.expression_semantics import ExpressionNode, parse_expression
from our_system_phase2.services.phase3cm_streaming_cache import CacheBudgetError

try:  # pragma: no cover - exercised on the 77o qualification host.
    from numba import njit, prange, set_num_threads
except Exception:  # pragma: no cover
    njit = None
    prange = range
    set_num_threads = None


MAPPING_OPERATORS = {"csrank", "rank", "zscore", "csresidual", "maskedzscore", "winsorize"}
ROLLING_OPERATORS = {
    "acceleration",
    "delta",
    "eventcount",
    "firsthit",
    "lasthit",
    "maskedzscore",
    "multiscalerelation",
    "pathshape",
    "persistence",
    "slope",
    "transition",
}
STREAMING_OPERATOR_SURFACE = frozenset(
    {
        "abs", "acceleration", "add", "csrank", "csresidual", "delta", "div",
        "duration", "eventage", "eventcount", "firsthit", "lasthit", "maskedzscore",
        "mul", "multiscalerelation", "pathshape", "persistence", "positive", "rank",
        "safediv", "sign", "sincelastevent", "slope", "stateage", "sub", "timesince",
        "transition", "winsorize", "zscore",
    }
)


def _contains_mapping(node: ExpressionNode) -> bool:
    return node.token.lower() in MAPPING_OPERATORS or any(_contains_mapping(child) for child in node.args)


def unsupported_streaming_operators(expressions: Iterable[str]) -> tuple[str, ...]:
    observed: set[str] = set()

    def visit(node: ExpressionNode) -> None:
        if node.args:
            observed.add(node.token.lower())
        for child in node.args:
            visit(child)

    for expression in expressions:
        visit(parse_expression(str(expression)))
    return tuple(sorted(observed - STREAMING_OPERATOR_SURFACE))


def _boundaries(sorted_ids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(sorted_ids) == 0:
        empty = np.empty(0, dtype=np.int64)
        return empty, empty
    cuts = np.flatnonzero(np.r_[True, sorted_ids[1:] != sorted_ids[:-1], True]).astype(np.int64)
    return cuts[:-1], cuts[1:]


if njit is not None:

    @njit(cache=True, parallel=True)
    def _rolling_kernel(
        values: np.ndarray,
        order: np.ndarray,
        starts: np.ndarray,
        ends: np.ndarray,
        sorted_codes: np.ndarray,
        history: np.ndarray,
        history_counts: np.ndarray,
        window: int,
        operation: int,
        parameter: float,
    ) -> np.ndarray:
        out = np.empty(values.shape[0], dtype=np.float64)
        out[:] = np.nan
        for group in prange(starts.shape[0]):
            start = starts[group]
            end = ends[group]
            code = sorted_codes[start]
            old_count = int(history_counts[code])
            length = end - start
            combined = np.empty(old_count + length, dtype=np.float64)
            for j in range(old_count):
                combined[j] = history[code, j]
            for j in range(length):
                combined[old_count + j] = values[order[start + j]]

            for j in range(length):
                absolute = old_count + j
                result = np.nan
                if operation == 1:  # Delta
                    if absolute >= window:
                        left = combined[absolute]
                        right = combined[absolute - window]
                        if np.isfinite(left) and np.isfinite(right):
                            result = left - right
                elif operation in (2, 3, 5, 6, 7, 8, 9):
                    window_start = absolute - window + 1
                    if operation == 8:  # rolling valid-ratio gate permits a partial first window.
                        window_start = 0 if window_start < 0 else window_start
                        count = absolute - window_start + 1
                        valid = 0
                        for k in range(window_start, absolute + 1):
                            if np.isfinite(combined[k]):
                                valid += 1
                        current = combined[absolute]
                        if np.isfinite(current) and valid / count >= parameter:
                            result = current
                    elif window_start >= 0:
                        finite = True
                        for k in range(window_start, absolute + 1):
                            if not np.isfinite(combined[k]):
                                finite = False
                                break
                        if operation == 7:  # EventCount treats missing as no event.
                            total = 0.0
                            for k in range(window_start, absolute + 1):
                                value = combined[k]
                                if np.isfinite(value) and value != 0.0:
                                    total += 1.0
                            result = total
                        elif finite:
                            if operation == 2:  # Persistence
                                total = 0.0
                                for k in range(window_start, absolute + 1):
                                    total += combined[k]
                                result = total / window
                            elif operation == 3:  # Slope
                                x_mean = (window - 1.0) / 2.0
                                numerator = 0.0
                                denominator = 0.0
                                value_mean = 0.0
                                for k in range(window_start, absolute + 1):
                                    value_mean += combined[k]
                                value_mean /= window
                                for local in range(window):
                                    centered = local - x_mean
                                    numerator += centered * (combined[window_start + local] - value_mean)
                                    denominator += centered * centered
                                result = numerator / denominator if denominator else 0.0
                            elif operation == 5:  # PathShape
                                distance = 0.0
                                for k in range(window_start + 1, absolute + 1):
                                    distance += abs(combined[k] - combined[k - 1])
                                result = ((combined[absolute] - combined[window_start]) / distance) if distance > 0 else 0.0
                            elif operation in (6, 9):  # FirstHit / LastHit
                                hit = -1
                                if operation == 6:
                                    for local in range(window):
                                        if combined[window_start + local] > 0.0:
                                            hit = local
                                            break
                                    result = hit + 1.0 if hit >= 0 else np.nan
                                else:
                                    for local in range(window - 1, -1, -1):
                                        if combined[window_start + local] > 0.0:
                                            hit = local
                                            break
                                    result = window - 1.0 - hit if hit >= 0 else np.nan
                elif operation == 4:  # Acceleration: current rolling slope - prior rolling slope.
                    if absolute >= window:
                        finite = True
                        for k in range(absolute - window, absolute + 1):
                            if not np.isfinite(combined[k]):
                                finite = False
                                break
                        if finite:
                            x_mean = (window - 1.0) / 2.0
                            denominator = 0.0
                            for local in range(window):
                                centered = local - x_mean
                                denominator += centered * centered
                            slopes = np.empty(2, dtype=np.float64)
                            for which in range(2):
                                w_start = absolute - window + which
                                mean = 0.0
                                for local in range(window):
                                    mean += combined[w_start + local]
                                mean /= window
                                numerator = 0.0
                                for local in range(window):
                                    numerator += (local - x_mean) * (combined[w_start + local] - mean)
                                slopes[which] = numerator / denominator if denominator else 0.0
                            result = slopes[1] - slopes[0]
                out[order[start + j]] = result

            keep = window if window < combined.shape[0] else combined.shape[0]
            source_start = combined.shape[0] - keep
            for j in range(keep):
                history[code, j] = combined[source_start + j]
            history_counts[code] = keep
        return out


    @njit(cache=True, parallel=True)
    def _state_kernel(
        values: np.ndarray,
        order: np.ndarray,
        starts: np.ndarray,
        ends: np.ndarray,
        sorted_codes: np.ndarray,
        previous: np.ndarray,
        age: np.ndarray,
        has_previous: np.ndarray,
        operation: int,
        source: float,
        target: float,
    ) -> np.ndarray:
        out = np.empty(values.shape[0], dtype=np.float64)
        out[:] = np.nan
        for group in prange(starts.shape[0]):
            start = starts[group]
            end = ends[group]
            code = sorted_codes[start]
            prev = previous[code]
            counter = age[code]
            seen = has_previous[code]
            for pos in range(start, end):
                row = order[pos]
                value = values[row]
                if operation == 1:  # Duration / StateAge
                    if not np.isfinite(value):
                        counter = 0.0
                        seen = False
                    else:
                        counter = counter + 1.0 if seen and value == prev else 1.0
                        prev = value
                        seen = True
                        out[row] = counter
                elif operation == 2:  # TimeSince
                    if np.isfinite(value):
                        if value > 0.0:
                            counter = 0.0
                            seen = True
                        elif seen:
                            counter += 1.0
                        if seen:
                            out[row] = counter
                    elif seen:
                        counter += 1.0
                else:  # Transition; missing previous/current state is false.
                    matched = (
                        seen
                        and np.isfinite(prev)
                        and np.isfinite(value)
                        and prev == source
                        and value == target
                    )
                    out[row] = 1.0 if matched else 0.0
                    prev = value
                    seen = True
            previous[code] = prev
            age[code] = counter
            has_previous[code] = seen
        return out


    @njit(cache=True, parallel=True)
    def _winsorize_kernel(values: np.ndarray, starts: np.ndarray, ends: np.ndarray) -> np.ndarray:
        out = values.copy()
        for group in prange(starts.shape[0]):
            start = starts[group]
            end = ends[group]
            count = 0
            for pos in range(start, end):
                if np.isfinite(values[pos]):
                    count += 1
            if count == 0:
                continue
            valid = np.empty(count, dtype=np.float64)
            cursor = 0
            for pos in range(start, end):
                if np.isfinite(values[pos]):
                    valid[cursor] = values[pos]
                    cursor += 1
            valid.sort()
            lower_position = (count - 1) * 0.01
            upper_position = (count - 1) * 0.99
            lower_index = int(math.floor(lower_position))
            upper_index = int(math.floor(upper_position))
            lower_fraction = lower_position - lower_index
            upper_fraction = upper_position - upper_index
            lower_next = lower_index + 1 if lower_index + 1 < count else lower_index
            upper_next = upper_index + 1 if upper_index + 1 < count else upper_index
            lower = valid[lower_index] + lower_fraction * (valid[lower_next] - valid[lower_index])
            upper = valid[upper_index] + upper_fraction * (valid[upper_next] - valid[upper_index])
            for pos in range(start, end):
                value = values[pos]
                if np.isfinite(value):
                    out[pos] = lower if value < lower else upper if value > upper else value
        return out


    @njit(cache=True)
    def _correlation(values_left: np.ndarray, values_right: np.ndarray, start: int, end: int) -> float:
        count = end - start
        if count < 2:
            return np.nan
        left_mean = 0.0
        right_mean = 0.0
        for pos in range(start, end):
            left = values_left[pos]
            right = values_right[pos]
            if not np.isfinite(left) or not np.isfinite(right):
                return np.nan
            left_mean += left
            right_mean += right
        left_mean /= count
        right_mean /= count
        covariance = 0.0
        left_variance = 0.0
        right_variance = 0.0
        for pos in range(start, end):
            left_delta = values_left[pos] - left_mean
            right_delta = values_right[pos] - right_mean
            covariance += left_delta * right_delta
            left_variance += left_delta * left_delta
            right_variance += right_delta * right_delta
        denominator = math.sqrt(left_variance * right_variance)
        if denominator <= 0.0 or not np.isfinite(denominator):
            return np.nan
        return covariance / denominator


    @njit(cache=True, parallel=True)
    def _multiscale_relation_kernel(
        left: np.ndarray,
        right: np.ndarray,
        order: np.ndarray,
        starts: np.ndarray,
        ends: np.ndarray,
        sorted_codes: np.ndarray,
        history_left: np.ndarray,
        history_right: np.ndarray,
        history_counts: np.ndarray,
        short: int,
        long: int,
    ) -> np.ndarray:
        out = np.empty(left.shape[0], dtype=np.float64)
        out[:] = np.nan
        for group in prange(starts.shape[0]):
            start = starts[group]
            end = ends[group]
            code = sorted_codes[start]
            old_count = int(history_counts[code])
            length = end - start
            combined_left = np.empty(old_count + length, dtype=np.float64)
            combined_right = np.empty(old_count + length, dtype=np.float64)
            for pos in range(old_count):
                combined_left[pos] = history_left[code, pos]
                combined_right[pos] = history_right[code, pos]
            for pos in range(length):
                row = order[start + pos]
                combined_left[old_count + pos] = left[row]
                combined_right[old_count + pos] = right[row]
            for pos in range(length):
                absolute = old_count + pos
                long_start = absolute - long + 1
                if long_start < 0:
                    continue
                short_start = absolute - short + 1
                short_corr = _correlation(combined_left, combined_right, short_start, absolute + 1)
                long_corr = _correlation(combined_left, combined_right, long_start, absolute + 1)
                if np.isfinite(short_corr) and np.isfinite(long_corr):
                    out[order[start + pos]] = short_corr - long_corr
            keep = long if long < combined_left.shape[0] else combined_left.shape[0]
            source_start = combined_left.shape[0] - keep
            for pos in range(keep):
                history_left[code, pos] = combined_left[source_start + pos]
                history_right[code, pos] = combined_right[source_start + pos]
            history_counts[code] = keep
        return out


    @njit(cache=True, parallel=True)
    def _rank_kernel(values: np.ndarray, starts: np.ndarray, ends: np.ndarray) -> np.ndarray:
        out = np.empty(values.shape[0], dtype=np.float64)
        out[:] = np.nan
        for group in prange(starts.shape[0]):
            start = starts[group]
            end = ends[group]
            valid_count = 0
            for pos in range(start, end):
                if np.isfinite(values[pos]):
                    valid_count += 1
            if valid_count == 0:
                continue
            valid_pos = np.empty(valid_count, dtype=np.int64)
            valid_values = np.empty(valid_count, dtype=np.float64)
            cursor = 0
            for pos in range(start, end):
                if np.isfinite(values[pos]):
                    valid_pos[cursor] = pos
                    valid_values[cursor] = values[pos]
                    cursor += 1
            order = np.argsort(valid_values)
            tie_start = 0
            while tie_start < valid_count:
                tie_end = tie_start + 1
                while tie_end < valid_count and valid_values[order[tie_end]] == valid_values[order[tie_start]]:
                    tie_end += 1
                pct = (((tie_start + 1) + tie_end) / 2.0) / valid_count
                for index in range(tie_start, tie_end):
                    out[valid_pos[order[index]]] = pct
                tie_start = tie_end
        return out


    @njit(cache=True, parallel=True)
    def _zscore_kernel(values: np.ndarray, starts: np.ndarray, ends: np.ndarray) -> np.ndarray:
        out = np.empty(values.shape[0], dtype=np.float64)
        out[:] = np.nan
        for group in prange(starts.shape[0]):
            start = starts[group]
            end = ends[group]
            count = 0
            total = 0.0
            for pos in range(start, end):
                value = values[pos]
                if np.isfinite(value):
                    count += 1
                    total += value
            if count < 2:
                continue
            mean = total / count
            squared = 0.0
            for pos in range(start, end):
                value = values[pos]
                if np.isfinite(value):
                    squared += (value - mean) * (value - mean)
            std = math.sqrt(squared / (count - 1))
            if not np.isfinite(std) or std == 0.0:
                continue
            for pos in range(start, end):
                value = values[pos]
                if np.isfinite(value):
                    out[pos] = (value - mean) / std
        return out


    @njit(cache=True, parallel=True)
    def _residual_kernel(left: np.ndarray, right: np.ndarray, starts: np.ndarray, ends: np.ndarray) -> np.ndarray:
        out = np.empty(left.shape[0], dtype=np.float64)
        out[:] = np.nan
        for group in prange(starts.shape[0]):
            start = starts[group]
            end = ends[group]
            count = 0
            x_total = 0.0
            y_total = 0.0
            for pos in range(start, end):
                if np.isfinite(left[pos]) and np.isfinite(right[pos]):
                    count += 1
                    y_total += left[pos]
                    x_total += right[pos]
            if count < 5:
                continue
            x_mean = x_total / count
            y_mean = y_total / count
            variance = 0.0
            covariance = 0.0
            for pos in range(start, end):
                if np.isfinite(left[pos]) and np.isfinite(right[pos]):
                    x_delta = right[pos] - x_mean
                    variance += x_delta * x_delta
                    covariance += x_delta * (left[pos] - y_mean)
            if variance <= 0.0 or not np.isfinite(variance):
                continue
            beta = covariance / variance
            intercept = y_mean - beta * x_mean
            for pos in range(start, end):
                if np.isfinite(left[pos]) and np.isfinite(right[pos]):
                    out[pos] = left[pos] - intercept - beta * right[pos]
        return out

else:  # pragma: no cover - development environment has Numba.
    _rolling_kernel = _state_kernel = _rank_kernel = _zscore_kernel = _residual_kernel = None
    _winsorize_kernel = _multiscale_relation_kernel = None


@dataclass(slots=True)
class _RollingState:
    history: np.ndarray
    counts: np.ndarray


@dataclass(slots=True)
class _StateState:
    previous: np.ndarray
    age: np.ndarray
    has_previous: np.ndarray


@dataclass(slots=True)
class _BivariateRollingState:
    history_left: np.ndarray
    history_right: np.ndarray
    counts: np.ndarray


class StreamingExpressionExecutor:
    """Evaluate canonical expressions on one complete-market time block."""

    def __init__(
        self,
        *,
        code_count: int,
        compute_threads: int,
        cache_max_bytes: int = 8 * 1024**3,
        cache_max_entries: int = 2048,
    ) -> None:
        if code_count <= 0:
            raise ValueError("code_count must be positive")
        if compute_threads <= 0 or compute_threads > 32:
            raise ValueError("compute_threads must be between 1 and 32")
        self.code_count = int(code_count)
        self.compute_threads = int(compute_threads)
        self.cache_max_bytes = int(cache_max_bytes)
        self.cache_max_entries = int(cache_max_entries)
        if self.cache_max_bytes <= 0 or self.cache_max_entries <= 0:
            raise ValueError("cache byte and entry caps must be positive")
        self._rolling_states: dict[str, _RollingState] = {}
        self._state_states: dict[str, _StateState] = {}
        self._multiscale_states: dict[str, _BivariateRollingState] = {}
        self.raw_fields: dict[str, np.ndarray] = {}
        self.code_ids = np.empty(0, dtype=np.int32)
        self.time_ids = np.empty(0, dtype=np.int64)
        self._code_order = np.empty(0, dtype=np.int64)
        self._code_starts = np.empty(0, dtype=np.int64)
        self._code_ends = np.empty(0, dtype=np.int64)
        self._sorted_codes = np.empty(0, dtype=np.int32)
        self._time_starts = np.empty(0, dtype=np.int64)
        self._time_ends = np.empty(0, dtype=np.int64)
        self._cache: dict[str, np.ndarray] = {}
        self._cache_owned_bytes: dict[str, int] = {}
        self._cache_bytes = 0
        self.audit: dict[str, Any] = {}

    def bind_block(
        self,
        *,
        raw_fields: Mapping[str, np.ndarray],
        code_ids: np.ndarray,
        time_ids: np.ndarray,
    ) -> "StreamingExpressionExecutor":
        code_ids = np.asarray(code_ids, dtype=np.int32)
        time_ids = np.asarray(time_ids, dtype=np.int64)
        if code_ids.shape != time_ids.shape:
            raise ValueError("code_ids/time_ids shape mismatch")
        if len(time_ids) and bool(np.any(time_ids[1:] < time_ids[:-1])):
            raise ValueError("global barrier block must be time-major")
        if len(code_ids) and (int(code_ids.min()) < 0 or int(code_ids.max()) >= self.code_count):
            raise ValueError("code id outside frozen symbol universe")
        normalized: dict[str, np.ndarray] = {}
        for name, values in raw_fields.items():
            array = np.asarray(values, dtype=np.float64)
            if array.shape != code_ids.shape:
                raise ValueError(f"raw field length mismatch: {name}")
            normalized[str(name)] = array
        self.raw_fields = normalized
        self.code_ids = code_ids
        self.time_ids = time_ids
        self._code_order = np.lexsort((time_ids, code_ids)).astype(np.int64)
        self._sorted_codes = code_ids[self._code_order]
        self._code_starts, self._code_ends = _boundaries(self._sorted_codes)
        self._time_starts, self._time_ends = _boundaries(time_ids)
        self._cache = {}
        self._cache_owned_bytes = {}
        self._cache_bytes = 0
        self.audit = {
            "rows": int(len(code_ids)),
            "raw_field_count": len(normalized),
            "value_node_evaluations": 0,
            "mapping_node_evaluations": 0,
            "cache_hits": 0,
            "native_kernel_calls": 0,
            "compute_threads": self.compute_threads,
            "python_pandas_hot_path_calls": 0,
            "cache_max_bytes": self.cache_max_bytes,
            "cache_max_entries": self.cache_max_entries,
        }
        if set_num_threads is not None:
            set_num_threads(self.compute_threads)
        return self

    def _store_cache(self, key: str, value: np.ndarray, *, owned: bool) -> np.ndarray:
        array = np.asarray(value, dtype=np.float64)
        if key in self._cache:
            return self._cache[key]
        incremental = int(array.nbytes) if owned else 0
        if len(self._cache) + 1 > self.cache_max_entries:
            raise CacheBudgetError("DAG block cache entry cap reached")
        if self._cache_bytes + incremental > self.cache_max_bytes:
            raise CacheBudgetError(
                f"DAG block cache byte cap reached: {self._cache_bytes + incremental} > {self.cache_max_bytes}"
            )
        self._cache[key] = array
        self._cache_owned_bytes[key] = incremental
        self._cache_bytes += incremental
        self.audit["cache_current_bytes"] = self._cache_bytes
        self.audit["cache_peak_bytes"] = max(int(self.audit.get("cache_peak_bytes") or 0), self._cache_bytes)
        self.audit["cache_entry_count"] = len(self._cache)
        self.audit["cache_peak_entries"] = max(
            int(self.audit.get("cache_peak_entries") or 0), len(self._cache)
        )
        return array

    @staticmethod
    def cache_key(
        canonical_expression: str,
        *,
        value_namespace: str,
        mapping_namespace: str | None,
    ) -> str:
        canonical = parse_expression(str(canonical_expression)).render()
        return (
            f"mapping:{mapping_namespace}:{canonical}"
            if mapping_namespace is not None
            else f"value:{value_namespace}:{canonical}"
        )

    def release_cache_keys(self, keys: Iterable[str]) -> dict[str, int]:
        released_entries = 0
        released_bytes = 0
        for raw_key in keys:
            key = str(raw_key)
            if key not in self._cache:
                continue
            self._cache.pop(key)
            released_bytes += int(self._cache_owned_bytes.pop(key, 0))
            released_entries += 1
        self._cache_bytes -= released_bytes
        if self._cache_bytes < 0:
            raise RuntimeError("expression cache byte accounting underflow")
        self.audit["cache_current_bytes"] = self._cache_bytes
        self.audit["cache_entry_count"] = len(self._cache)
        self.audit["cache_released_entries"] = int(
            self.audit.get("cache_released_entries") or 0
        ) + released_entries
        self.audit["cache_released_bytes"] = int(
            self.audit.get("cache_released_bytes") or 0
        ) + released_bytes
        return {"released_entries": released_entries, "released_bytes": released_bytes}

    def _rolling(self, key: str, values: np.ndarray, window: int, operation: int, parameter: float = 0.0) -> np.ndarray:
        if _rolling_kernel is None:
            raise RuntimeError("Numba is required for the streaming rolling hot path")
        state = self._rolling_states.get(key)
        if state is None:
            state = _RollingState(
                history=np.full((self.code_count, max(1, window)), np.nan, dtype=np.float64),
                counts=np.zeros(self.code_count, dtype=np.int32),
            )
            self._rolling_states[key] = state
        self.audit["native_kernel_calls"] += 1
        return _rolling_kernel(
            values,
            self._code_order,
            self._code_starts,
            self._code_ends,
            self._sorted_codes,
            state.history,
            state.counts,
            int(window),
            int(operation),
            float(parameter),
        )

    def _state(
        self,
        key: str,
        values: np.ndarray,
        operation: int,
        source: float = 0.0,
        target: float = 0.0,
    ) -> np.ndarray:
        if _state_kernel is None:
            raise RuntimeError("Numba is required for the streaming state hot path")
        state = self._state_states.get(key)
        if state is None:
            state = _StateState(
                previous=np.full(self.code_count, np.nan, dtype=np.float64),
                age=np.zeros(self.code_count, dtype=np.float64),
                has_previous=np.zeros(self.code_count, dtype=np.bool_),
            )
            self._state_states[key] = state
        self.audit["native_kernel_calls"] += 1
        return _state_kernel(
            values,
            self._code_order,
            self._code_starts,
            self._code_ends,
            self._sorted_codes,
            state.previous,
            state.age,
            state.has_previous,
            int(operation),
            float(source),
            float(target),
        )

    def _multiscale(
        self,
        key: str,
        left: np.ndarray,
        right: np.ndarray,
        short: int,
        long: int,
    ) -> np.ndarray:
        if _multiscale_relation_kernel is None:
            raise RuntimeError("Numba is required for the streaming multiscale hot path")
        if short <= 1 or long <= short:
            raise ValueError("MultiScaleRelation requires 1 < short < long")
        state = self._multiscale_states.get(key)
        if state is None:
            state = _BivariateRollingState(
                history_left=np.full((self.code_count, long), np.nan, dtype=np.float64),
                history_right=np.full((self.code_count, long), np.nan, dtype=np.float64),
                counts=np.zeros(self.code_count, dtype=np.int32),
            )
            self._multiscale_states[key] = state
        self.audit["native_kernel_calls"] += 1
        return _multiscale_relation_kernel(
            left,
            right,
            self._code_order,
            self._code_starts,
            self._code_ends,
            self._sorted_codes,
            state.history_left,
            state.history_right,
            state.counts,
            int(short),
            int(long),
        )

    @staticmethod
    def _number(node: ExpressionNode) -> float:
        if node.args:
            raise ValueError(f"numeric atom required: {node.render()}")
        return float(node.token)

    def _evaluate(
        self,
        node: ExpressionNode,
        value_namespace: str,
        mapping_namespace: str,
        *,
        cache_result: bool = True,
    ) -> np.ndarray:
        canonical = node.render()
        key = self.cache_key(
            canonical,
            value_namespace=value_namespace,
            mapping_namespace=mapping_namespace if _contains_mapping(node) else None,
        )
        cached = self._cache.get(key)
        if cached is not None:
            self.audit["cache_hits"] += 1
            return cached
        if not node.args:
            if node.token.startswith("$"):
                field = node.token[1:]
                if field not in self.raw_fields:
                    raise KeyError(f"missing streaming raw field: {field}")
                result = self.raw_fields[field]
            else:
                result = np.full(len(self.code_ids), float(node.token), dtype=np.float64)
            self.audit["value_node_evaluations"] += 1
            return (
                self._store_cache(key, result, owned=not node.token.startswith("$"))
                if cache_result
                else np.asarray(result, dtype=np.float64)
            )

        name = node.token.lower()
        args = [
            self._evaluate(child, value_namespace, mapping_namespace)
            for child in node.args
            if not (not child.args and not child.token.startswith("$") and name in ROLLING_OPERATORS)
        ]
        if name == "abs":
            result = np.abs(args[0])
        elif name == "sign":
            result = np.sign(args[0])
        elif name == "positive":
            result = np.where(np.isfinite(args[0]), (args[0] > 0.0).astype(np.float64), np.nan)
        elif name in {"add", "sub", "mul", "div"}:
            if name == "add":
                result = args[0] + args[1]
            elif name == "sub":
                result = args[0] - args[1]
            elif name == "mul":
                result = args[0] * args[1]
            else:
                denominator = np.where(args[1] == 0.0, np.nan, args[1])
                result = args[0] / denominator
        elif name == "safediv":
            floor = self._number(node.args[2])
            denominator = args[1].copy()
            finite = np.isfinite(denominator)
            small = finite & (np.abs(denominator) < floor)
            denominator[small] = np.where(denominator[small] < 0.0, -floor, floor)
            result = args[0] / denominator
        elif name in {"delta", "persistence", "slope", "acceleration", "pathshape", "firsthit", "lasthit", "eventcount"}:
            window = int(self._number(node.args[1]))
            operation = {
                "delta": 1,
                "persistence": 2,
                "slope": 3,
                "acceleration": 4,
                "pathshape": 5,
                "firsthit": 6,
                "eventcount": 7,
                "lasthit": 9,
            }[name]
            result = self._rolling(key, args[0], window, operation)
        elif name in {"duration", "stateage"}:
            result = self._state(key, args[0], 1)
        elif name in {"timesince", "eventage", "sincelastevent"}:
            result = self._state(key, args[0], 2)
        elif name == "transition":
            result = self._state(
                key,
                args[0],
                3,
                self._number(node.args[1]),
                self._number(node.args[2]),
            )
        elif name == "multiscalerelation":
            result = self._multiscale(
                key,
                args[0],
                args[1],
                int(self._number(node.args[2])),
                int(self._number(node.args[3])),
            )
        elif name in {"csrank", "rank"}:
            if _rank_kernel is None:
                raise RuntimeError("Numba is required for cross-sectional mapping")
            result = _rank_kernel(args[0], self._time_starts, self._time_ends)
            self.audit["native_kernel_calls"] += 1
        elif name == "zscore":
            if _zscore_kernel is None:
                raise RuntimeError("Numba is required for cross-sectional mapping")
            result = _zscore_kernel(args[0], self._time_starts, self._time_ends)
            self.audit["native_kernel_calls"] += 1
        elif name == "winsorize":
            if _winsorize_kernel is None:
                raise RuntimeError("Numba is required for cross-sectional winsorization")
            result = _winsorize_kernel(args[0], self._time_starts, self._time_ends)
            self.audit["native_kernel_calls"] += 1
        elif name == "csresidual":
            if _residual_kernel is None:
                raise RuntimeError("Numba is required for cross-sectional mapping")
            result = _residual_kernel(args[0], args[1], self._time_starts, self._time_ends)
            self.audit["native_kernel_calls"] += 1
        elif name == "maskedzscore":
            window = int(self._number(node.args[1]))
            ratio = self._number(node.args[2])
            gated = self._rolling(key + ":valid_ratio", args[0], window, 8, ratio)
            if _zscore_kernel is None:
                raise RuntimeError("Numba is required for cross-sectional mapping")
            result = _zscore_kernel(gated, self._time_starts, self._time_ends)
            self.audit["native_kernel_calls"] += 1
        else:
            raise ValueError(f"unsupported streaming operator: {node.token}")
        if name in MAPPING_OPERATORS:
            self.audit["mapping_node_evaluations"] += 1
        else:
            self.audit["value_node_evaluations"] += 1
        if cache_result:
            return self._store_cache(key, result, owned=True)
        self.audit["root_cache_bypass_count"] = int(
            self.audit.get("root_cache_bypass_count") or 0
        ) + 1
        self.audit["root_cache_bypass_bytes"] = int(
            self.audit.get("root_cache_bypass_bytes") or 0
        ) + int(np.asarray(result).nbytes)
        return np.asarray(result, dtype=np.float64)

    def evaluate_ordered(
        self,
        expressions: Iterable[str],
        *,
        value_namespaces: Iterable[str] | None = None,
        mapping_namespaces: Iterable[str] | None = None,
    ) -> tuple[np.ndarray, ...]:
        expression_rows = tuple(str(value) for value in expressions)
        value_rows = (
            tuple("default" for _ in expression_rows)
            if value_namespaces is None
            else tuple(str(value) for value in value_namespaces)
        )
        mapping_rows = (
            tuple("default" for _ in expression_rows)
            if mapping_namespaces is None
            else tuple(str(value) for value in mapping_namespaces)
        )
        if len(value_rows) != len(expression_rows) or len(mapping_rows) != len(expression_rows):
            raise ValueError("expression namespace count drift")
        started_wall = time.perf_counter()
        started_cpu = time.process_time()
        result = tuple(
            self._evaluate(
                parse_expression(parse_expression(expression).render()),
                value_namespace,
                mapping_namespace,
            )
            for expression, value_namespace, mapping_namespace in zip(
                expression_rows,
                value_rows,
                mapping_rows,
            )
        )
        wall = time.perf_counter() - started_wall
        cpu = time.process_time() - started_cpu
        self.audit["last_evaluate_wall_seconds"] = wall
        self.audit["last_evaluate_cpu_seconds"] = cpu
        self.audit["last_evaluate_effective_cores"] = cpu / wall if wall > 0.0 else 0.0
        self.audit["native_thread_environment"] = {
            key: os.environ.get(key)
            for key in (
                "NUMBA_NUM_THREADS",
                "ARROW_NUM_THREADS",
                "OMP_NUM_THREADS",
                "MKL_NUM_THREADS",
                "OPENBLAS_NUM_THREADS",
                "NUMEXPR_MAX_THREADS",
            )
        }
        return result

    def evaluate_ordered_into(
        self,
        expressions: Iterable[str],
        *,
        value_namespaces: Iterable[str] | None = None,
        mapping_namespaces: Iterable[str] | None = None,
        release_keys_after_each: Iterable[Iterable[str]] | None = None,
    ) -> np.ndarray:
        """Materialize roots into one matrix while releasing nodes after their last consumer."""
        expression_rows = tuple(str(value) for value in expressions)
        value_rows = (
            tuple("default" for _ in expression_rows)
            if value_namespaces is None
            else tuple(str(value) for value in value_namespaces)
        )
        mapping_rows = (
            tuple("default" for _ in expression_rows)
            if mapping_namespaces is None
            else tuple(str(value) for value in mapping_namespaces)
        )
        release_rows = (
            tuple(() for _ in expression_rows)
            if release_keys_after_each is None
            else tuple(tuple(str(key) for key in keys) for keys in release_keys_after_each)
        )
        if not (
            len(value_rows)
            == len(mapping_rows)
            == len(release_rows)
            == len(expression_rows)
        ):
            raise ValueError("expression namespace/release count drift")

        started_wall = time.perf_counter()
        started_cpu = time.process_time()
        result = np.empty((len(expression_rows), len(self.code_ids)), dtype=np.float64)
        released_entries = 0
        released_bytes = 0
        for index, (expression, value_namespace, mapping_namespace, release_keys) in enumerate(
            zip(expression_rows, value_rows, mapping_rows, release_rows)
        ):
            root_node = parse_expression(parse_expression(expression).render())
            root_key = self.cache_key(
                root_node.render(),
                value_namespace=value_namespace,
                mapping_namespace=(
                    mapping_namespace if _contains_mapping(root_node) else None
                ),
            )
            values = self._evaluate(
                root_node,
                value_namespace,
                mapping_namespace,
                cache_result=root_key not in release_keys,
            )
            result[index] = values
            released = self.release_cache_keys(release_keys)
            released_entries += int(released["released_entries"])
            released_bytes += int(released["released_bytes"])
            del values

        wall = time.perf_counter() - started_wall
        cpu = time.process_time() - started_cpu
        self.audit["last_evaluate_wall_seconds"] = wall
        self.audit["last_evaluate_cpu_seconds"] = cpu
        self.audit["last_evaluate_effective_cores"] = cpu / wall if wall > 0.0 else 0.0
        self.audit["intra_batch_released_entries"] = int(
            self.audit.get("intra_batch_released_entries") or 0
        ) + released_entries
        self.audit["intra_batch_released_bytes"] = int(
            self.audit.get("intra_batch_released_bytes") or 0
        ) + released_bytes
        self.audit["native_thread_environment"] = {
            key: os.environ.get(key)
            for key in (
                "NUMBA_NUM_THREADS",
                "ARROW_NUM_THREADS",
                "OMP_NUM_THREADS",
                "MKL_NUM_THREADS",
                "OPENBLAS_NUM_THREADS",
                "NUMEXPR_MAX_THREADS",
            )
        }
        return result

    def evaluate_many(
        self,
        expressions: Iterable[str],
        *,
        mapping_masks: Mapping[str, np.ndarray] | None = None,
        value_namespaces: Mapping[str, str] | None = None,
        mapping_namespaces: Mapping[str, str] | None = None,
    ) -> dict[str, np.ndarray]:
        expression_rows = tuple(str(expression) for expression in expressions)
        evaluated = self.evaluate_ordered(
            expression_rows,
            value_namespaces=tuple(
                str((value_namespaces or {}).get(expression, "default"))
                for expression in expression_rows
            ),
            mapping_namespaces=tuple(
                str((mapping_namespaces or {}).get(expression, "default"))
                for expression in expression_rows
            ),
        )
        result: dict[str, np.ndarray] = {}
        for expression, evaluated_values in zip(expression_rows, evaluated):
            values = evaluated_values
            mask = (mapping_masks or {}).get(expression)
            if mask is not None:
                mask_array = np.asarray(mask, dtype=bool)
                if mask_array.shape != values.shape:
                    raise ValueError("mapping mask shape mismatch")
                values = np.where(mask_array, values, np.nan)
            result[expression] = values
        return result

    def continuation_payload(self) -> dict[str, Any]:
        return {
            "rolling": {
                key: {"history": value.history.copy(), "counts": value.counts.copy()}
                for key, value in sorted(self._rolling_states.items())
            },
            "state": {
                key: {
                    "previous": value.previous.copy(),
                    "age": value.age.copy(),
                    "has_previous": value.has_previous.copy(),
                }
                for key, value in sorted(self._state_states.items())
            },
            "multiscale": {
                key: {
                    "history_left": value.history_left.copy(),
                    "history_right": value.history_right.copy(),
                    "counts": value.counts.copy(),
                }
                for key, value in sorted(self._multiscale_states.items())
            },
        }

    def restore_continuation_payload(self, payload: Mapping[str, Any]) -> None:
        self._rolling_states = {
            str(key): _RollingState(
                history=np.asarray(value["history"], dtype=np.float64).copy(),
                counts=np.asarray(value["counts"], dtype=np.int32).copy(),
            )
            for key, value in dict(payload.get("rolling") or {}).items()
        }
        self._state_states = {
            str(key): _StateState(
                previous=np.asarray(value["previous"], dtype=np.float64).copy(),
                age=np.asarray(value["age"], dtype=np.float64).copy(),
                has_previous=np.asarray(value["has_previous"], dtype=np.bool_).copy(),
            )
            for key, value in dict(payload.get("state") or {}).items()
        }
        self._multiscale_states = {
            str(key): _BivariateRollingState(
                history_left=np.asarray(value["history_left"], dtype=np.float64).copy(),
                history_right=np.asarray(value["history_right"], dtype=np.float64).copy(),
                counts=np.asarray(value["counts"], dtype=np.int32).copy(),
            )
            for key, value in dict(payload.get("multiscale") or {}).items()
        }
