from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import json
import math
import re
from math import sqrt
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from our_system_phase2.services.feature_algebra import WINDOW_PRIOR, expand_derived_fields
from our_system_phase2.services.field_encoder import FIELD_ALIASES
from our_system_phase2.services.event_derived_features import (
    EVENT_DERIVED_FEATURE_FIELDS,
    EVENT_DERIVED_FULL_DAY_FIELDS,
    EVENT_DERIVED_OPEN_PRINT_FIELDS,
    attach_event_derived_features,
)
from our_system_phase2.services.market_regime_state import (
    PIT_TREND_STATE_FEATURE_FIELDS,
    attach_pit_trend_state_features,
    trend_state_feature_contract,
)
from our_system_phase2.services.real_market_data import (
    DEFAULT_REAL_MARKET_DATASET_PATH,
    REAL_MARKET_VALIDATION_PERIOD_MONTHS,
    panel_header,
)


DEFAULT_RELATION_WINDOW_DAYS = 20
DEFAULT_VALIDATION_HORIZON_DAYS = tuple(WINDOW_PRIOR)
VALIDATION_HORIZON_POLICY = "feature_algebra_window_prior"
DEFAULT_RECENT_LOOKBACK_DAYS = 92
DEFAULT_RECENT_WARMUP_DAYS = 90
DEFAULT_EXECUTION_LAG_DAYS = 1
MARKET_PANEL_CHUNKSIZE = 200_000
RELATION_OPERATORS = {"corr", "cov"}
ROLLING_OPERATORS = {"mean", "mom", "std", "delay", "delta", "wma", "med", "kurt", "skew", "corr", "cov"}
TYPED_PRIMITIVE_OPERATORS = {
    "eventage",
    "sincelastevent",
    "eventcount",
    "stateage",
    "statedwell",
    "windowstatecount",
    "validratiogate",
    "maskedzscore",
    "maskedcorr",
    "safecsresidual",
}
OPTIONAL_TRADABILITY_COLUMNS = (
    "is_limit_up",
    "is_limit_down",
    "tdxgp_limit_status",
    "tdxgp_limit_status_value2",
    "limitup",
    "limitdown",
    "limit_up",
    "limit_down",
    "up_limit",
    "down_limit",
    "rt_change_pct",
    "susp",
)
OPTIONAL_FEATURE_COLUMNS = (
    "sector",
    "daily_ret",
    "return_1d",
    "return_5d",
    "return_20d",
    "rps_rank",
    "rps_score",
    "rps_slope_3d",
    "money_flow",
    "turnover_ratio",
    "turnover_ratio_real",
    "seal_money",
    "seal_rate",
    "seal_circulation_rate",
    "f9_quantile_250d",
    "crowding",
    "overnight",
    "low_20",
    "high_20",
    "price_pos",
    "rps_enhanced",
    "rps_rank_enhanced",
    "limit_up_event",
    "limit_down_event",
    "limit_up_streak",
    "limit_down_streak",
    "limit_up_break",
    "limit_down_repair",
    "limit_flip_up_to_down",
    "limit_flip_down_to_up",
    *EVENT_DERIVED_FEATURE_FIELDS,
    "float_share",
    "total_share",
    "after_float_share_10k",
    "after_total_share_10k",
    "is_capital_applicable",
    "market_cap",
    "float_market_cap",
    "market_cap_billion",
    "float_market_cap_billion",
    "gbbq_has_capital",
    "tdxgp_total_mv_10k_cny",
    "tdxgp_total_market_cap",
    "tdxgp_total_market_cap_billion",
    "final_total_market_cap",
    "final_total_market_cap_billion",
    "final_float_market_cap",
    "final_float_market_cap_billion",
    "market_cap_conflict_gt5pct",
    "actual_circulation_value",
    *PIT_TREND_STATE_FEATURE_FIELDS,
)
MARKET_PANEL_METADATA_COLUMNS = ("instrument_type", "market")
A_SHARE_NON_STOCK_CODE_PATTERN = re.compile(r"^(?:880|sh000|sz399|bj899)", re.IGNORECASE)
TDXGP_LIMIT_STATUS_TYPE_ID = 15
TDXGP_LIMIT_UP_STATUS = 2.0
TDXGP_LIMIT_DOWN_STATUS = -2.0
TDXGP_LIMIT_STATUS_VALUES = {-2.0, -1.0, 0.0, 1.0, 2.0}
TDXGP_LIMIT_STATUS_SOURCE = "tdxgp_gpjvalue_15_status"
_TDXGP_LIMIT_STATUS_CACHE: dict[Path, pd.DataFrame] = {}
SIGNAL_CLOCK_AFTER_CLOSE = "after_close"
SIGNAL_CLOCK_PRE_OPEN = "pre_open"
SIGNAL_CLOCK_AFTER_OPEN = "after_open"
VALID_SIGNAL_CLOCKS = {SIGNAL_CLOCK_AFTER_CLOSE, SIGNAL_CLOCK_PRE_OPEN, SIGNAL_CLOCK_AFTER_OPEN}
OPEN_PRINT_FIELDS = {"open", "overnight", *EVENT_DERIVED_OPEN_PRINT_FIELDS}
FULL_DAY_BAR_FIELDS = {
    "high",
    "low",
    "close",
    "amount",
    "volume",
    "vwap",
    "turnover_rate",
    "turnover_ratio",
    "turnover_ratio_real",
    "ret",
    "daily_ret",
    "amtm",
    "reta",
    "retb",
    "retc",
    "retd",
    "rete",
    "retf",
    "return_1d",
    "return_5d",
    "return_20d",
    "rps_rank",
    "rps_score",
    "rps_slope_3d",
    "money_flow",
    "seal_money",
    "seal_rate",
    "seal_circulation_rate",
    "f9_quantile_250d",
    "crowding",
    "low_20",
    "high_20",
    "price_pos",
    "rps_enhanced",
    "rps_rank_enhanced",
    "limit_up_event",
    "limit_down_event",
    "limit_up_streak",
    "limit_down_streak",
    "limit_up_break",
    "limit_down_repair",
    "limit_flip_up_to_down",
    "limit_flip_down_to_up",
    *EVENT_DERIVED_FULL_DAY_FIELDS,
    "float_share",
    "total_share",
    "after_float_share_10k",
    "after_total_share_10k",
    "is_capital_applicable",
    "market_cap",
    "float_market_cap",
    "market_cap_billion",
    "float_market_cap_billion",
    "gbbq_has_capital",
    "tdxgp_total_mv_10k_cny",
    "tdxgp_total_market_cap",
    "tdxgp_total_market_cap_billion",
    "final_total_market_cap",
    "final_total_market_cap_billion",
    "final_float_market_cap",
    "final_float_market_cap_billion",
    "market_cap_conflict_gt5pct",
    "actual_circulation_value",
    *PIT_TREND_STATE_FEATURE_FIELDS,
}
LONG_SELECTION_DIAGNOSTIC_FIELDS = {
    "amount": "long_selected_amount",
    "turnover_rate": "long_selected_turnover_rate",
    "final_float_market_cap": "long_selected_final_float_market_cap",
    "final_total_market_cap": "long_selected_final_total_market_cap",
    "final_float_market_cap_billion": "long_selected_final_float_market_cap_billion",
    "final_total_market_cap_billion": "long_selected_final_total_market_cap_billion",
    "market_cap_conflict_gt5pct": "long_selected_market_cap_conflict_rate",
}


class UnsupportedExpressionError(ValueError):
    pass


def expression_validation_cost_report(expression: str) -> dict[str, Any]:
    normalized = expand_derived_fields(expression.strip())
    operators = [item.lower() for item in re.findall(r"\b([A-Za-z][A-Za-z0-9_]*)\s*\(", normalized)]
    relation_count = sum(1 for item in operators if item in RELATION_OPERATORS)
    rolling_count = sum(1 for item in operators if item in ROLLING_OPERATORS)
    max_depth = 0
    depth = 0
    for char in normalized:
        if char == "(":
            depth += 1
            max_depth = max(max_depth, depth)
        elif char == ")":
            depth = max(0, depth - 1)
    cost_score = round((len(normalized) / 50.0) + (rolling_count * 2.0) + (relation_count * 8.0) + (max_depth * 1.5), 3)
    binary_output = bool(re.match(r"^\s*Sign\s*\(", normalized))
    if relation_count == 0 and rolling_count <= 4:
        validation_lane = "cheap_fast_path"
    elif relation_count <= 2 and rolling_count <= 8:
        validation_lane = "moderate_fast_path"
    elif relation_count <= 8:
        validation_lane = "slow_relation_path"
    else:
        validation_lane = "very_slow_nested_relation_path"
    validation_role = "group_spread_regime_shadow" if binary_output else "cross_sectional_rank_validation"
    return {
        "expression": expression,
        "normalized_expression": normalized,
        "operator_count": len(operators),
        "relation_operator_count": relation_count,
        "rolling_operator_count": rolling_count,
        "max_parenthesis_depth": max_depth,
        "expression_length": len(normalized),
        "binary_output": binary_output,
        "validation_lane": validation_lane,
        "validation_role": validation_role,
        "estimated_validation_cost_score": cost_score,
    }


def _split_args(payload: str) -> list[str]:
    args: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(payload):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            args.append(payload[start:index].strip())
            start = index + 1
    args.append(payload[start:].strip())
    return args


def _parse_call(expression: str) -> tuple[str, list[str]] | None:
    expression = expression.strip()
    if not expression.endswith(")") or "(" not in expression:
        return None
    name, rest = expression.split("(", 1)
    payload = rest[:-1]
    if not name.strip():
        return None
    return name.strip(), _split_args(payload)


def _safe_log(series: pd.Series) -> pd.Series:
    return np.log(series.abs().replace(0, np.nan))


def _rolling_wma(series: pd.Series, window: int) -> pd.Series:
    weights = np.arange(1, window + 1, dtype=float)

    def weighted(values: np.ndarray) -> float:
        return float(np.dot(values, weights) / weights.sum())

    return series.rolling(window, min_periods=window).apply(weighted, raw=True)


def _rolling_relation(
    frame: pd.DataFrame,
    left: pd.Series,
    right: pd.Series,
    *,
    window: int,
    operator: str,
) -> pd.Series:
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    for index in frame.groupby("code", sort=False).groups.values():
        left_part = left.loc[index]
        right_part = right.loc[index]
        rolling = left_part.rolling(window, min_periods=min(5, window))
        if operator == "corr":
            result.loc[index] = rolling.corr(right_part)
        else:
            result.loc[index] = rolling.cov(right_part)
    return result


def _cross_section_key(frame: pd.DataFrame) -> pd.Series:
    if "trade_time" in frame.columns:
        return pd.to_datetime(frame["trade_time"], errors="coerce")
    return frame["date"]


def fast_rank_pct_by_group(values: pd.Series, group: pd.Series) -> pd.Series:
    """Memory-stable equivalent of groupby(...).rank(pct=True)."""
    numeric = pd.to_numeric(values, errors="coerce")
    arr = numeric.to_numpy(dtype=float, copy=False)
    codes, _ = pd.factorize(group, sort=False)
    out = np.full(len(arr), np.nan, dtype=float)
    valid_code_mask = codes >= 0
    if not bool(valid_code_mask.any()):
        return pd.Series(out, index=values.index)

    order = np.argsort(codes, kind="stable")
    sorted_codes = codes[order]
    boundaries = np.flatnonzero(np.r_[True, sorted_codes[1:] != sorted_codes[:-1], True])
    for start, end in zip(boundaries[:-1], boundaries[1:]):
        if sorted_codes[start] < 0:
            continue
        idx = order[start:end]
        part = arr[idx]
        valid = np.isfinite(part)
        n = int(valid.sum())
        if n == 0:
            continue
        valid_pos = np.flatnonzero(valid)
        valid_values = part[valid_pos]
        value_order = np.argsort(valid_values, kind="mergesort")
        sorted_values = valid_values[value_order]
        tie_bounds = np.flatnonzero(np.r_[True, sorted_values[1:] != sorted_values[:-1], True])
        ranks = np.empty(n, dtype=float)
        for tie_start, tie_end in zip(tie_bounds[:-1], tie_bounds[1:]):
            # pandas rank(method="average", pct=True): average 1-based rank / group valid count.
            avg_rank = ((tie_start + 1) + tie_end) / 2.0
            ranks[value_order[tie_start:tie_end]] = avg_rank / n
        out[idx[valid_pos]] = ranks
    return pd.Series(out, index=values.index)


def fast_zscore_by_group(values: pd.Series, group: pd.Series) -> pd.Series:
    """Memory-stable equivalent of groupby(...).transform zscore with ddof=1 std."""
    numeric = pd.to_numeric(values, errors="coerce")
    arr = numeric.to_numpy(dtype=float, copy=False)
    codes, _ = pd.factorize(group, sort=False)
    out = np.full(len(arr), np.nan, dtype=float)
    valid_code_mask = codes >= 0
    if not bool(valid_code_mask.any()):
        return pd.Series(out, index=values.index)

    order = np.argsort(codes, kind="stable")
    sorted_codes = codes[order]
    boundaries = np.flatnonzero(np.r_[True, sorted_codes[1:] != sorted_codes[:-1], True])
    for start, end in zip(boundaries[:-1], boundaries[1:]):
        if sorted_codes[start] < 0:
            continue
        idx = order[start:end]
        part = arr[idx]
        valid = np.isfinite(part)
        n = int(valid.sum())
        if n < 2:
            continue
        mean = float(np.nanmean(part))
        std = float(np.nanstd(part, ddof=1))
        if not math.isfinite(std) or std == 0.0:
            continue
        out[idx[valid]] = (part[valid] - mean) / std
    return pd.Series(out, index=values.index)


def _cross_sectional_residual(frame: pd.DataFrame, left: pd.Series, right: pd.Series) -> pd.Series:
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    for index in frame.groupby(_cross_section_key(frame), sort=False).groups.values():
        y = pd.to_numeric(left.loc[index], errors="coerce")
        x = pd.to_numeric(right.loc[index], errors="coerce")
        valid = y.notna() & x.notna()
        if int(valid.sum()) < 5:
            continue
        x_valid = x[valid]
        y_valid = y[valid]
        x_var = float(x_valid.var(ddof=0))
        if not math.isfinite(x_var) or x_var <= 0.0:
            continue
        beta = float(((x_valid - x_valid.mean()) * (y_valid - y_valid.mean())).mean() / x_var)
        intercept = float(y_valid.mean() - beta * x_valid.mean())
        result.loc[y_valid.index] = y_valid - intercept - beta * x_valid
    return result


def _safe_cross_sectional_residual(
    frame: pd.DataFrame,
    left: pd.Series,
    right: pd.Series,
    *,
    min_n: int,
    min_x_unique: int,
    min_valid_ratio: float,
) -> pd.Series:
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    min_n = max(2, int(min_n))
    min_x_unique = max(2, int(min_x_unique))
    min_valid_ratio = max(0.0, min(1.0, float(min_valid_ratio)))
    for index in frame.groupby(_cross_section_key(frame), sort=False).groups.values():
        y = pd.to_numeric(left.loc[index], errors="coerce")
        x = pd.to_numeric(right.loc[index], errors="coerce")
        valid = y.notna() & x.notna()
        if int(valid.sum()) < min_n:
            continue
        if float(valid.mean()) < min_valid_ratio:
            continue
        x_valid = x[valid]
        y_valid = y[valid]
        if int(x_valid.nunique(dropna=True)) < min_x_unique:
            continue
        x_var = float(x_valid.var(ddof=0))
        if not math.isfinite(x_var) or x_var <= 0.0:
            continue
        beta = float(((x_valid - x_valid.mean()) * (y_valid - y_valid.mean())).mean() / x_var)
        intercept = float(y_valid.mean() - beta * x_valid.mean())
        result.loc[y_valid.index] = y_valid - intercept - beta * x_valid
    return result


def _rolling_valid_ratio(frame: pd.DataFrame, value: pd.Series, *, window: int) -> pd.Series:
    valid = pd.to_numeric(value, errors="coerce").notna().astype(float)
    return valid.groupby(frame["code"], sort=False).transform(
        lambda item: item.rolling(window, min_periods=1).mean()
    )


def _event_mask(value: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(value, errors="coerce")
    return numeric.notna() & (numeric != 0.0)


def _rolling_event_count(frame: pd.DataFrame, value: pd.Series, *, window: int) -> pd.Series:
    numeric = pd.to_numeric(value, errors="coerce")
    valid = numeric.notna().astype(float)
    event = (numeric.notna() & (numeric != 0.0)).astype(float)
    grouped_event = event.groupby(frame["code"], sort=False)
    grouped_valid = valid.groupby(frame["code"], sort=False)
    counts = grouped_event.transform(lambda item: item.rolling(window, min_periods=window).sum())
    valid_counts = grouped_valid.transform(lambda item: item.rolling(window, min_periods=window).sum())
    return counts.where(valid_counts >= window)


def _event_age(frame: pd.DataFrame, value: pd.Series) -> pd.Series:
    event = _event_mask(value)
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    for index in frame.groupby("code", sort=False).groups.values():
        seen = False
        age = 0
        for idx in index:
            if bool(event.loc[idx]):
                seen = True
                age = 0
                result.loc[idx] = 0.0
            elif seen:
                age += 1
                result.loc[idx] = float(age)
    return result


def _state_dwell(frame: pd.DataFrame, value: pd.Series, *, window: int | None = None) -> pd.Series:
    numeric = pd.to_numeric(value, errors="coerce")
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    cap = None if window is None or int(window) <= 0 else int(window)
    for index in frame.groupby("code", sort=False).groups.values():
        previous: float | None = None
        dwell = 0
        for idx in index:
            current = numeric.loc[idx]
            if pd.isna(current):
                previous = None
                dwell = 0
                continue
            current_f = float(current)
            if previous is not None and current_f == previous:
                dwell += 1
            else:
                previous = current_f
                dwell = 1
            result.loc[idx] = float(min(dwell, cap)) if cap is not None else float(dwell)
    return result


def _masked_zscore(frame: pd.DataFrame, value: pd.Series, *, window: int, min_ratio: float) -> pd.Series:
    gated = pd.to_numeric(value, errors="coerce").where(_rolling_valid_ratio(frame, value, window=window) >= min_ratio)
    return fast_zscore_by_group(gated, _cross_section_key(frame))


def _masked_relation(
    frame: pd.DataFrame,
    left: pd.Series,
    right: pd.Series,
    *,
    window: int,
    min_ratio: float,
    operator: str,
) -> pd.Series:
    min_periods = max(2, int(math.ceil(max(0.0, min(1.0, float(min_ratio))) * int(window))))
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    for index in frame.groupby("code", sort=False).groups.values():
        left_part = pd.to_numeric(left.loc[index], errors="coerce")
        right_part = pd.to_numeric(right.loc[index], errors="coerce")
        joint_valid = left_part.notna() & right_part.notna()
        valid_ratio = joint_valid.astype(float).rolling(window, min_periods=1).mean()
        rolling = left_part.rolling(window, min_periods=min_periods)
        if operator == "corr":
            values = rolling.corr(right_part)
        else:
            values = rolling.cov(right_part)
        result.loc[index] = values.where(valid_ratio >= min_ratio)
    return result


def _expression_cache_key(expression: str, field_lags: dict[str, int] | None) -> str:
    active_lags = tuple(sorted((field, int(lag)) for field, lag in (field_lags or {}).items() if int(lag) != 0))
    if not active_lags:
        return expression
    return f"{expression}||field_lags={active_lags!r}"


def evaluate_panel_expression(
    frame: pd.DataFrame,
    expression: str,
    *,
    cache: dict[str, pd.Series] | None = None,
    field_lags: dict[str, int] | None = None,
) -> pd.Series:
    expression = expand_derived_fields(expression.strip())
    cache_key = _expression_cache_key(expression, field_lags)
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    def store(series: pd.Series) -> pd.Series:
        if cache is not None:
            cache[cache_key] = series
        return series

    if expression.startswith("$"):
        column = expression[1:]
        column = FIELD_ALIASES.get(column, column)
        if column not in frame.columns:
            raise UnsupportedExpressionError(f"missing_field:{column}")
        series = pd.to_numeric(frame[column], errors="coerce")
        lag = int((field_lags or {}).get(column, 0))
        if lag > 0:
            series = series.groupby(frame["code"], sort=False).shift(lag)
        return store(series)
    try:
        return store(pd.Series(float(expression), index=frame.index))
    except ValueError:
        pass

    call = _parse_call(expression)
    if call is None:
        raise UnsupportedExpressionError(f"unsupported_expression:{expression}")
    name, args = call
    name_lower = name.lower()

    if name_lower in {"csrank", "rank"} and len(args) == 1:
        value = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        return store(fast_rank_pct_by_group(value, _cross_section_key(frame)))
    if name_lower == "abs" and len(args) == 1:
        return store(evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags).abs())
    if name_lower == "sign" and len(args) == 1:
        return store(np.sign(evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)))
    if name_lower == "log" and len(args) == 1:
        return store(_safe_log(evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)))
    if name_lower == "neg" and len(args) == 1:
        return store(-evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags))
    if name_lower == "zscore" and len(args) == 1:
        value = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        return store(fast_zscore_by_group(value, _cross_section_key(frame)))
    if name_lower == "csresidual" and len(args) == 2:
        left = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        right = evaluate_panel_expression(frame, args[1], cache=cache, field_lags=field_lags)
        return store(_cross_sectional_residual(frame, left, right))
    if name_lower in {"eventage", "sincelastevent"} and len(args) == 1:
        value = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        return store(_event_age(frame, value))
    if name_lower in {"stateage"} and len(args) == 1:
        value = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        return store(_state_dwell(frame, value))
    if name_lower == "eventcount" and len(args) == 2:
        value = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        window = int(float(args[1]))
        return store(_rolling_event_count(frame, value, window=window))
    if name_lower in {"statedwell", "windowstatecount"} and len(args) == 2:
        value = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        window = int(float(args[1]))
        if name_lower == "statedwell":
            return store(_state_dwell(frame, value, window=window))
        return store(_rolling_event_count(frame, value, window=window))
    if name_lower == "validratiogate" and len(args) == 3:
        value = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        window = int(float(args[1]))
        min_ratio = float(args[2])
        return store(pd.to_numeric(value, errors="coerce").where(_rolling_valid_ratio(frame, value, window=window) >= min_ratio))
    if name_lower == "maskedzscore" and len(args) == 3:
        value = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        window = int(float(args[1]))
        min_ratio = float(args[2])
        return store(_masked_zscore(frame, value, window=window, min_ratio=min_ratio))
    if name_lower == "maskedcorr" and len(args) == 4:
        left = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        right = evaluate_panel_expression(frame, args[1], cache=cache, field_lags=field_lags)
        window = int(float(args[2]))
        min_ratio = float(args[3])
        return store(_masked_relation(frame, left, right, window=window, min_ratio=min_ratio, operator="corr"))
    if name_lower == "safecsresidual" and len(args) == 5:
        left = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        right = evaluate_panel_expression(frame, args[1], cache=cache, field_lags=field_lags)
        min_n = int(float(args[2]))
        min_x_unique = int(float(args[3]))
        min_valid_ratio = float(args[4])
        return store(
            _safe_cross_sectional_residual(
                frame,
                left,
                right,
                min_n=min_n,
                min_x_unique=min_x_unique,
                min_valid_ratio=min_valid_ratio,
            )
        )

    if name_lower in {"mean", "mom", "std", "delay", "delta", "wma", "med", "kurt", "skew"} and len(args) == 2:
        window = int(float(args[1]))
        if name_lower == "delay" and args[0].strip().startswith("$"):
            column = FIELD_ALIASES.get(args[0].strip()[1:], args[0].strip()[1:])
            if column not in frame.columns:
                raise UnsupportedExpressionError(f"missing_field:{column}")
            lag = max(window, int((field_lags or {}).get(column, 0)))
            return store(pd.to_numeric(frame[column], errors="coerce").groupby(frame["code"], sort=False).shift(lag))
        value = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        grouped = value.groupby(frame["code"], sort=False)
        if name_lower == "mean":
            return store(grouped.transform(lambda item: item.rolling(window, min_periods=window).mean()))
        if name_lower == "mom":
            return store(grouped.transform(lambda item: item / item.shift(window) - 1.0))
        if name_lower == "std":
            return store(grouped.transform(lambda item: item.rolling(window, min_periods=window).std()))
        if name_lower == "delay":
            return store(grouped.shift(window))
        if name_lower == "delta":
            return store(value - grouped.shift(window))
        if name_lower == "wma":
            return store(grouped.transform(lambda item: _rolling_wma(item, window)))
        if name_lower == "med":
            return store(grouped.transform(lambda item: item.rolling(window, min_periods=window).median()))
        if name_lower == "kurt":
            return store(grouped.transform(lambda item: item.rolling(window, min_periods=window).kurt()))
        if name_lower == "skew":
            return store(grouped.transform(lambda item: item.rolling(window, min_periods=window).skew()))

    if name_lower in {"add", "sub", "mul", "div"} and len(args) == 2:
        left = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        right = evaluate_panel_expression(frame, args[1], cache=cache, field_lags=field_lags)
        if name_lower == "add":
            return store(left + right)
        if name_lower == "sub":
            return store(left - right)
        if name_lower == "mul":
            return store(left * right)
        denominator = right.replace(0, np.nan)
        return store(left / denominator)

    if name_lower in {"corr", "cov"} and len(args) in {2, 3}:
        left = evaluate_panel_expression(frame, args[0], cache=cache, field_lags=field_lags)
        right = evaluate_panel_expression(frame, args[1], cache=cache, field_lags=field_lags)
        window = int(float(args[2])) if len(args) == 3 else DEFAULT_RELATION_WINDOW_DAYS
        return store(_rolling_relation(frame, left, right, window=window, operator=name_lower))

    raise UnsupportedExpressionError(f"unsupported_operator:{name}")


def _augment_market_fields(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    close = pd.to_numeric(frame["close"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    amount = pd.to_numeric(frame["amount"], errors="coerce")
    denominator = volume.replace(0, np.nan)
    if "vwap" not in frame.columns:
        frame["vwap"] = amount / denominator
    if "ret" not in frame.columns:
        frame["ret"] = close.groupby(frame["code"], sort=False).pct_change()
    cfg_return_windows = {
        "reta": 1,
        "retb": 2,
        "retc": 3,
        "retd": 5,
        "rete": 10,
        "retf": 20,
    }
    for column, window in cfg_return_windows.items():
        if column not in frame.columns:
            frame[column] = close.groupby(frame["code"], sort=False).pct_change(window)
    if "amtm" not in frame.columns:
        frame["amtm"] = close.groupby(frame["code"], sort=False).pct_change(20)
    if "susp" not in frame.columns:
        frame["susp"] = 0.0
    if "turnover_rate" not in frame.columns:
        if "float_share" in frame.columns:
            float_share = pd.to_numeric(frame["float_share"], errors="coerce")
            frame["turnover_rate"] = volume / float_share.replace(0, np.nan)
        else:
            volume_base = volume.groupby(frame["code"], sort=False).transform(
                lambda item: item.rolling(20, min_periods=5).mean()
            )
            frame["turnover_rate"] = volume / volume_base.replace(0, np.nan)
    tdxgp_masks = _tdxgp_limit_status_masks(frame)
    if tdxgp_masks is not None:
        limit_up, limit_down = tdxgp_masks
    else:
        limit_up, limit_up_source = _flag_mask(frame, ("is_limit_up", "limitup", "limit_up", "up_limit"))
        limit_down, limit_down_source = _flag_mask(frame, ("is_limit_down", "limitdown", "limit_down", "down_limit"))
        if limit_up_source is None and limit_down_source is None and "rt_change_pct" in frame.columns:
            change = pd.to_numeric(frame["rt_change_pct"], errors="coerce")
            limit_up = change >= 9.8
            limit_down = change <= -9.8
    up_event = limit_up.fillna(False).astype(float)
    down_event = limit_down.fillna(False).astype(float)
    grouped_up = up_event.groupby(frame["code"], sort=False)
    grouped_down = down_event.groupby(frame["code"], sort=False)
    if "limit_up_event" not in frame.columns:
        frame["limit_up_event"] = up_event
    if "limit_down_event" not in frame.columns:
        frame["limit_down_event"] = down_event
    if "limit_up_streak" not in frame.columns:
        frame["limit_up_streak"] = grouped_up.transform(
            lambda item: item.groupby((item <= 0.0).cumsum()).cumsum()
        )
    if "limit_down_streak" not in frame.columns:
        frame["limit_down_streak"] = grouped_down.transform(
            lambda item: item.groupby((item <= 0.0).cumsum()).cumsum()
        )
    previous_up = grouped_up.shift(1).fillna(0.0) > 0.0
    previous_down = grouped_down.shift(1).fillna(0.0) > 0.0
    current_up = up_event > 0.0
    current_down = down_event > 0.0
    if "limit_up_break" not in frame.columns:
        frame["limit_up_break"] = (previous_up & ~current_up).astype(float)
    if "limit_down_repair" not in frame.columns:
        frame["limit_down_repair"] = (previous_down & ~current_down).astype(float)
    if "limit_flip_up_to_down" not in frame.columns:
        frame["limit_flip_up_to_down"] = (previous_up & current_down).astype(float)
    if "limit_flip_down_to_up" not in frame.columns:
        frame["limit_flip_down_to_up"] = (previous_down & current_up).astype(float)
    frame = attach_event_derived_features(frame)
    return frame


def _signal_clock_field_lags(signal_clock: str) -> dict[str, int]:
    if signal_clock not in VALID_SIGNAL_CLOCKS:
        raise ValueError(f"unsupported_signal_clock:{signal_clock}")
    if signal_clock == SIGNAL_CLOCK_AFTER_CLOSE:
        return {}
    if signal_clock == SIGNAL_CLOCK_PRE_OPEN:
        return {field: 1 for field in OPEN_PRINT_FIELDS | FULL_DAY_BAR_FIELDS}
    return {field: 1 for field in FULL_DAY_BAR_FIELDS}


def _signal_evaluation_frame(frame: pd.DataFrame, *, signal_clock: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    field_lags = _signal_clock_field_lags(signal_clock)
    if not field_lags:
        return frame, {
            "signal_clock": signal_clock,
            "field_lag_policy": "after_close_raw_daily_bar_available_before_next_session",
            "field_lags": {},
        }
    applied: dict[str, int] = {}
    for column, lag in sorted(field_lags.items()):
        if column not in frame.columns:
            continue
        applied[column] = lag
    policy = (
        "pre_open_no_current_daily_bar_fields_available"
        if signal_clock == SIGNAL_CLOCK_PRE_OPEN
        else "after_open_open_print_available_full_day_bar_fields_prior_day"
    )
    return frame, {
        "signal_clock": signal_clock,
        "field_lag_policy": policy,
        "field_lags": applied,
    }


def _feature_timestamp_policy(signal_clock: str, feature_lag_days: int) -> str:
    if signal_clock == SIGNAL_CLOCK_AFTER_CLOSE and feature_lag_days == 0:
        return "after_close_raw_daily_bar_available_before_next_session"
    if feature_lag_days > 0:
        return "signal_clock_field_lags_then_optional_whole_expression_lag"
    return "signal_clock_field_lags_without_whole_expression_lag"


def _available_market_panel_usecols(path: Path | str) -> list[str]:
    base = ["date", "open", "high", "low", "close", "amount", "volume", "code"]
    columns = set(panel_header(path))
    optional = [
        column
        for column in (*OPTIONAL_TRADABILITY_COLUMNS, *OPTIONAL_FEATURE_COLUMNS)
        if column in columns and column not in base
    ]
    fundamental = sorted(column for column in columns if column.startswith("fund_") and column not in base)
    integrated = sorted(
        column
        for column in columns
        if (
            column.startswith("ctx_")
            or column.startswith("m1_")
            or column.startswith("evt_")
            or column.startswith("mkt_")
            or column.startswith("plcov_")
            or column.startswith("plshuf_")
            or column.startswith("plrand_")
            or column.startswith("plmatch_")
            or column == "vwap"
        )
        and not column.startswith("meta_")
        and not column.startswith("label_")
        and column not in base
    )
    metadata = [column for column in MARKET_PANEL_METADATA_COLUMNS if column in columns and column not in base]
    out: list[str] = []
    seen: set[str] = set()
    for column in [*base, *metadata, *optional, *fundamental, *integrated]:
        if column in seen:
            continue
        seen.add(column)
        out.append(column)
    return out


def _has_effective_numeric_column(frame: pd.DataFrame, column: str) -> bool:
    if column not in frame.columns:
        return False
    values = pd.to_numeric(frame[column], errors="coerce")
    return bool(values.notna().any())


def _has_effective_limit_flag_columns(frame: pd.DataFrame) -> bool:
    names = ("is_limit_up", "is_limit_down", "limitup", "limitdown", "limit_up", "limit_down", "up_limit", "down_limit")
    return any(_has_effective_numeric_column(frame, name) for name in names)


def _tdxgp_limit_status_paths(source_path: Path | None) -> list[Path]:
    if source_path is None:
        return []
    parent = source_path.parent
    if not parent.exists():
        return []
    candidates = [
        item
        for item in parent.glob("tdxgp_gpjvalue_types_*_since_*.parquet")
        if item.is_file() and "15" in item.name
    ]
    return sorted(candidates, key=lambda item: (item.stat().st_mtime, item.stat().st_size), reverse=True)


def _load_tdxgp_limit_status_events(source_path: Path | None) -> pd.DataFrame:
    paths = _tdxgp_limit_status_paths(source_path)
    if not paths:
        return pd.DataFrame(columns=["date", "symbol", "tdxgp_limit_status", "tdxgp_limit_status_value2"])
    for path in paths:
        resolved = path.resolve()
        if resolved in _TDXGP_LIMIT_STATUS_CACHE:
            return _TDXGP_LIMIT_STATUS_CACHE[resolved]
        try:
            columns = set(panel_header(path))
        except Exception:
            continue
        required = {"date", "symbol", "type_id", "value1", "value2"}
        if not required.issubset(columns):
            continue
        events = pd.read_parquet(path, columns=sorted(required))
        events = events[pd.to_numeric(events["type_id"], errors="coerce").eq(TDXGP_LIMIT_STATUS_TYPE_ID)].copy()
        if events.empty:
            continue
        events["tdxgp_limit_status"] = pd.to_numeric(events["value1"], errors="coerce")
        events = events[events["tdxgp_limit_status"].isin(TDXGP_LIMIT_STATUS_VALUES)].copy()
        if events.empty:
            continue
        events["date"] = pd.to_datetime(events["date"], errors="coerce")
        events["symbol"] = events["symbol"].astype(str).str.lower()
        events["tdxgp_limit_status_value2"] = pd.to_numeric(events["value2"], errors="coerce")
        events["_status_priority"] = events["tdxgp_limit_status"].abs()
        events = events.sort_values(["symbol", "date", "_status_priority"])
        events = events.groupby(["symbol", "date"], as_index=False).tail(1)
        events = events[["date", "symbol", "tdxgp_limit_status", "tdxgp_limit_status_value2"]].reset_index(drop=True)
        _TDXGP_LIMIT_STATUS_CACHE[resolved] = events
        return events
    return pd.DataFrame(columns=["date", "symbol", "tdxgp_limit_status", "tdxgp_limit_status_value2"])


def _normalized_symbol(frame: pd.DataFrame) -> pd.Series:
    symbol = frame["code"].astype(str).str.lower()
    if "market" in frame.columns:
        has_prefix = symbol.str.match(r"^(?:sh|sz|bj)\d{6}$", na=False)
        symbol = symbol.where(has_prefix, frame["market"].astype(str).str.lower() + symbol.str[-6:])
    return symbol


def _attach_tdxgp_limit_status(frame: pd.DataFrame, source_path: Path | None) -> pd.DataFrame:
    if _has_effective_limit_flag_columns(frame) or _has_effective_numeric_column(frame, "tdxgp_limit_status"):
        return frame
    events = _load_tdxgp_limit_status_events(source_path)
    if events.empty:
        return frame
    work = frame.copy()
    work["_tdxgp_symbol"] = _normalized_symbol(work)
    events = events.rename(columns={"symbol": "_tdxgp_symbol"})
    merged = work.merge(
        events,
        how="left",
        left_on=["date", "_tdxgp_symbol"],
        right_on=["date", "_tdxgp_symbol"],
    )
    status = pd.to_numeric(merged["tdxgp_limit_status"], errors="coerce")
    merged["is_limit_up"] = status.eq(TDXGP_LIMIT_UP_STATUS).astype(float)
    merged["is_limit_down"] = status.eq(TDXGP_LIMIT_DOWN_STATUS).astype(float)
    merged = merged.drop(columns=[column for column in ("_tdxgp_symbol",) if column in merged.columns])
    return merged


def _prepare_market_panel(
    frame: pd.DataFrame,
    *,
    enable_trend_state_features: bool = False,
    source_path: Path | None = None,
) -> pd.DataFrame:
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date", "code", "close"]).copy()
    frame["code"] = frame["code"].astype(str)
    if "instrument_type" in frame.columns:
        instrument_type = frame["instrument_type"].astype(str).str.lower().str.strip()
        frame = frame[instrument_type.eq("stock")].copy()
    if "instrument_type" in frame.columns or "market" in frame.columns:
        non_stock_code = frame["code"].str.match(A_SHARE_NON_STOCK_CODE_PATTERN)
        if non_stock_code.any():
            frame = frame[~non_stock_code].copy()
    frame = _attach_tdxgp_limit_status(frame, source_path)
    frame = frame.sort_values(["code", "date"]).reset_index(drop=True)
    for column in ("open", "high", "low", "close", "amount", "volume", *OPTIONAL_TRADABILITY_COLUMNS, *OPTIONAL_FEATURE_COLUMNS):
        if column in frame.columns:
            if column == "sector":
                continue
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in frame.columns:
        if column.startswith("ctx_") or column.startswith("m1_") or column == "vwap":
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = _augment_market_fields(frame)
    if enable_trend_state_features:
        frame = attach_pit_trend_state_features(frame)
    return frame


def _load_market_panel(
    path: Path | str,
    *,
    max_rows: int | None = None,
    enable_trend_state_features: bool = False,
) -> pd.DataFrame:
    path_obj = Path(path)
    usecols = _available_market_panel_usecols(path_obj)
    if path_obj.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path_obj, columns=usecols)
        if max_rows is not None:
            frame = frame.head(max_rows)
    else:
        frame = pd.read_csv(path_obj, usecols=usecols, nrows=max_rows)
    return _prepare_market_panel(frame, enable_trend_state_features=enable_trend_state_features, source_path=path_obj)


def _latest_market_date(path: Path | str, *, max_rows: int | None = None) -> pd.Timestamp:
    path_obj = Path(path)
    if path_obj.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path_obj, columns=["date"])
        if max_rows is not None:
            frame = frame.head(max_rows)
        dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
        if dates.empty:
            raise ValueError(f"no_valid_dates:{path}")
        return dates.max()
    latest: pd.Timestamp | None = None
    remaining = max_rows
    for chunk in pd.read_csv(path_obj, usecols=["date"], chunksize=MARKET_PANEL_CHUNKSIZE):
        if remaining is not None:
            if remaining <= 0:
                break
            chunk = chunk.head(remaining)
            remaining -= len(chunk)
        dates = pd.to_datetime(chunk["date"], errors="coerce").dropna()
        if dates.empty:
            continue
        chunk_latest = dates.max()
        if latest is None or chunk_latest > latest:
            latest = chunk_latest
    if latest is None:
        raise ValueError(f"no_valid_dates:{path}")
    return latest


def _load_recent_market_panel(
    path: Path | str,
    *,
    lookback_days: int = DEFAULT_RECENT_LOOKBACK_DAYS,
    warmup_days: int = DEFAULT_RECENT_WARMUP_DAYS,
    max_rows: int | None = None,
    enable_trend_state_features: bool = False,
) -> tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    latest = _latest_market_date(path, max_rows=max_rows)
    evaluation_start = latest - pd.Timedelta(days=lookback_days)
    load_start = evaluation_start - pd.Timedelta(days=warmup_days)
    path_obj = Path(path)
    usecols = _available_market_panel_usecols(path_obj)
    if path_obj.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path_obj, columns=usecols)
        if max_rows is not None:
            frame = frame.head(max_rows)
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame = frame[frame["date"] >= load_start]
        return (
            _prepare_market_panel(frame, enable_trend_state_features=enable_trend_state_features, source_path=path_obj),
            evaluation_start,
            latest,
        )
    chunks: list[pd.DataFrame] = []
    remaining = max_rows
    for chunk in pd.read_csv(path_obj, usecols=usecols, chunksize=MARKET_PANEL_CHUNKSIZE):
        if remaining is not None:
            if remaining <= 0:
                break
            chunk = chunk.head(remaining)
            remaining -= len(chunk)
        chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce")
        chunk = chunk[chunk["date"] >= load_start]
        if not chunk.empty:
            chunks.append(chunk)
    if not chunks:
        return _prepare_market_panel(
            pd.DataFrame(columns=usecols),
            enable_trend_state_features=enable_trend_state_features,
            source_path=path_obj,
        ), evaluation_start, latest
    return _prepare_market_panel(
        pd.concat(chunks, ignore_index=True),
        enable_trend_state_features=enable_trend_state_features,
        source_path=path_obj,
    ), evaluation_start, latest


def _quarter_start(date: pd.Timestamp) -> pd.Timestamp:
    month = (((int(date.month) - 1) // REAL_MARKET_VALIDATION_PERIOD_MONTHS) * REAL_MARKET_VALIDATION_PERIOD_MONTHS) + 1
    return pd.Timestamp(year=int(date.year), month=month, day=1)


def _load_recent_quarter_market_panel(
    path: Path | str,
    *,
    quarter_window_count: int,
    warmup_days: int = DEFAULT_RECENT_WARMUP_DAYS,
    max_rows: int | None = None,
    enable_trend_state_features: bool = False,
) -> tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    if quarter_window_count <= 0:
        raise ValueError("quarter_window_count_must_be_positive")
    latest = _latest_market_date(path, max_rows=max_rows)
    latest_quarter_start = _quarter_start(latest)
    evaluation_start = latest_quarter_start - pd.DateOffset(
        months=REAL_MARKET_VALIDATION_PERIOD_MONTHS * (quarter_window_count - 1)
    )
    load_start = evaluation_start - pd.Timedelta(days=warmup_days)
    path_obj = Path(path)
    usecols = _available_market_panel_usecols(path_obj)
    if path_obj.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path_obj, columns=usecols)
        if max_rows is not None:
            frame = frame.head(max_rows)
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame = frame[frame["date"] >= load_start]
        return (
            _prepare_market_panel(frame, enable_trend_state_features=enable_trend_state_features, source_path=path_obj),
            evaluation_start,
            latest,
        )
    chunks: list[pd.DataFrame] = []
    remaining = max_rows
    for chunk in pd.read_csv(path_obj, usecols=usecols, chunksize=MARKET_PANEL_CHUNKSIZE):
        if remaining is not None:
            if remaining <= 0:
                break
            chunk = chunk.head(remaining)
            remaining -= len(chunk)
        chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce")
        chunk = chunk[chunk["date"] >= load_start]
        if not chunk.empty:
            chunks.append(chunk)
    if not chunks:
        return _prepare_market_panel(
            pd.DataFrame(columns=usecols),
            enable_trend_state_features=enable_trend_state_features,
            source_path=path_obj,
        ), evaluation_start, latest
    return _prepare_market_panel(
        pd.concat(chunks, ignore_index=True),
        enable_trend_state_features=enable_trend_state_features,
        source_path=path_obj,
    ), evaluation_start, latest


def _forward_return(frame: pd.DataFrame, horizon_days: int, *, execution_lag_days: int = 0) -> pd.Series:
    close = pd.to_numeric(frame["close"], errors="coerce")
    grouped = close.groupby(frame["code"], sort=False)
    entry = grouped.shift(-execution_lag_days) if execution_lag_days > 0 else close
    exit_ = grouped.shift(-(execution_lag_days + horizon_days))
    return exit_ / entry - 1.0


def _shift_mask_to_signal_date(frame: pd.DataFrame, mask: pd.Series, *, days_ahead: int) -> pd.Series:
    if days_ahead <= 0:
        return mask.fillna(False).astype(bool)
    shifted = mask.groupby(frame["code"], sort=False).shift(-days_ahead)
    return shifted.eq(True)


def _lag_signal_to_decision_date(frame: pd.DataFrame, signal: pd.Series, *, feature_lag_days: int) -> pd.Series:
    if feature_lag_days <= 0:
        return signal
    return signal.groupby(frame["code"], sort=False).shift(feature_lag_days)


def _attach_signal_safe_long_selection_diagnostics(
    frame: pd.DataFrame,
    work: pd.DataFrame,
    field_lags: dict[str, int],
) -> pd.DataFrame:
    """Attach capacity diagnostics using the same field clock as expressions."""

    for column in LONG_SELECTION_DIAGNOSTIC_FIELDS:
        if column not in frame.columns:
            continue
        value = pd.to_numeric(frame[column], errors="coerce")
        lag = int(field_lags.get(column, 0))
        if lag > 0:
            value = value.groupby(frame["code"], sort=False).shift(lag)
        work[f"diagnostic_{column}"] = value.loc[work.index]
    return work


def _long_selection_diagnostics(top: pd.DataFrame) -> dict[str, float]:
    diagnostics: dict[str, float] = {}
    for column, metric_name in LONG_SELECTION_DIAGNOSTIC_FIELDS.items():
        diagnostic_column = f"diagnostic_{column}"
        if diagnostic_column not in top.columns:
            continue
        values = pd.to_numeric(top[diagnostic_column], errors="coerce").dropna()
        if values.empty:
            continue
        diagnostics[metric_name] = float(values.mean())
    return diagnostics


def _mean_diagnostic_windows(windows: list[dict[str, Any]], metric_name: str) -> float | None:
    return _mean_or_none([item[f"mean_{metric_name}"] for item in windows if item.get(f"mean_{metric_name}") is not None])


def _sortino(values: pd.Series) -> float | None:
    clean = values.dropna()
    if clean.empty:
        return None
    downside = clean[clean < 0]
    if downside.empty:
        return round(float(clean.mean()), 6)
    downside_std = float(downside.std(ddof=0))
    if downside_std <= 0:
        return None
    return round(float(clean.mean() / downside_std * sqrt(len(clean))), 6)


def _mean_or_none(values: list[float]) -> float | None:
    clean = [float(value) for value in values if pd.notna(value)]
    return round(float(np.mean(clean)), 6) if clean else None


def _quarterly_window_label(date: pd.Timestamp) -> str:
    quarter = ((int(date.month) - 1) // REAL_MARKET_VALIDATION_PERIOD_MONTHS) + 1
    return f"{int(date.year)}Q{quarter}"


def _daily_signal_frame(
    frame: pd.DataFrame,
    signal: pd.Series,
    *,
    horizon_days: int,
    execution_lag_days: int = DEFAULT_EXECUTION_LAG_DAYS,
    feature_lag_days: int = 0,
    evaluation_start_date: pd.Timestamp | None = None,
    evaluation_end_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    work = frame[["date", "code"]].copy()
    work["signal"] = _lag_signal_to_decision_date(frame, signal, feature_lag_days=feature_lag_days)
    work["forward_return"] = _forward_return(frame, horizon_days, execution_lag_days=execution_lag_days)
    work = work.dropna(subset=["signal", "forward_return"])
    if evaluation_start_date is not None:
        work = work[work["date"] >= evaluation_start_date]
    if evaluation_end_date is not None:
        work = work[work["date"] <= evaluation_end_date]
    work["window"] = work["date"].map(_quarterly_window_label)
    return work


def _bucket_turnover_by_date(work: pd.DataFrame, *, top_bottom_quantile: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    previous_top: set[str] | None = None
    previous_bottom: set[str] | None = None
    for date, day in work.groupby("date", sort=True):
        if len(day) < 5 or day["signal"].nunique(dropna=True) < 2:
            continue
        side_count = max(1, int(math.ceil(len(day) * top_bottom_quantile)))
        top_codes = set(
            day.sort_values(["signal", "code"], ascending=[False, True]).head(side_count)["code"].astype(str)
        )
        bottom_codes = set(
            day.sort_values(["signal", "code"], ascending=[True, True]).head(side_count)["code"].astype(str)
        )
        if previous_top is None or previous_bottom is None:
            top_turnover = None
            bottom_turnover = None
        else:
            top_turnover = 1.0 - (len(top_codes & previous_top) / max(1, len(top_codes)))
            bottom_turnover = 1.0 - (len(bottom_codes & previous_bottom) / max(1, len(bottom_codes)))
        previous_top = top_codes
        previous_bottom = bottom_codes
        rows.append(
            {
                "date": date,
                "window": _quarterly_window_label(date),
                "side_count": side_count,
                "top_turnover": top_turnover,
                "bottom_turnover": bottom_turnover,
                "average_one_way_turnover": None
                if top_turnover is None or bottom_turnover is None
                else (top_turnover + bottom_turnover) / 2.0,
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "date",
            "window",
            "side_count",
            "top_turnover",
            "bottom_turnover",
            "average_one_way_turnover",
        ],
    )


def _daily_ic_spread_frame(work: pd.DataFrame, *, top_bottom_quantile: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for date, day in work.groupby("date", sort=True):
        if len(day) < 5 or day["signal"].nunique(dropna=True) < 2:
            continue
        ic = day["signal"].rank().corr(day["forward_return"].rank())
        low_cut = day["signal"].quantile(top_bottom_quantile)
        high_cut = day["signal"].quantile(1.0 - top_bottom_quantile)
        long_ret = day.loc[day["signal"] >= high_cut, "forward_return"].mean()
        short_ret = day.loc[day["signal"] <= low_cut, "forward_return"].mean()
        rows.append(
            {
                "date": date,
                "window": _quarterly_window_label(date),
                "rank_ic": float(ic) if pd.notna(ic) else None,
                "long_short_return": float(long_ret - short_ret) if pd.notna(long_ret) and pd.notna(short_ret) else None,
            }
        )
    return pd.DataFrame(rows, columns=["date", "window", "rank_ic", "long_short_return"])


def _tradable_signal_work_frame(
    frame: pd.DataFrame,
    signal: pd.Series,
    *,
    horizon_days: int,
    execution_lag_days: int = DEFAULT_EXECUTION_LAG_DAYS,
    feature_lag_days: int = 0,
    evaluation_start_date: pd.Timestamp | None = None,
    evaluation_end_date: pd.Timestamp | None = None,
    field_lags: dict[str, int] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    forward = _forward_return(frame, horizon_days, execution_lag_days=execution_lag_days)
    tradability_masks = _limit_state_masks(frame)
    work = frame[["date", "code"]].copy()
    work["signal"] = _lag_signal_to_decision_date(frame, signal, feature_lag_days=feature_lag_days)
    work["forward_return"] = forward
    work["entry_limit_up"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_up"], days_ahead=execution_lag_days
    )
    work["entry_limit_down"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_down"], days_ahead=execution_lag_days
    )
    work["entry_suspended"] = _shift_mask_to_signal_date(
        frame, tradability_masks["suspended"], days_ahead=execution_lag_days
    )
    work["exit_limit_up"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_up"], days_ahead=execution_lag_days + horizon_days
    )
    work["exit_limit_down"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_down"], days_ahead=execution_lag_days + horizon_days
    )
    work["exit_suspended"] = _shift_mask_to_signal_date(
        frame, tradability_masks["suspended"], days_ahead=execution_lag_days + horizon_days
    )
    work = _attach_signal_safe_long_selection_diagnostics(frame, work, field_lags or {})
    work = work.dropna(subset=["signal", "forward_return"])
    if evaluation_start_date is not None:
        work = work[work["date"] >= evaluation_start_date]
    if evaluation_end_date is not None:
        work = work[work["date"] <= evaluation_end_date]
    work["window"] = work["date"].map(_quarterly_window_label)
    return work, tradability_masks


def _tradable_daily_ic_spread_turnover_frame(work: pd.DataFrame, *, top_bottom_quantile: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    previous_top: set[str] | None = None
    previous_bottom: set[str] | None = None
    for date, day in work.groupby("date", sort=True):
        if len(day) < 5 or day["signal"].nunique(dropna=True) < 2:
            continue
        # Do not use exit-day limit states to decide whether a signal-day row
        # exists in the IC universe. Exit-day filtering is ex-post information
        # and can silently remove bad outcomes.
        ic_blocked = day["entry_limit_up"] | day["entry_limit_down"] | day["entry_suspended"]
        ic_day = day[~ic_blocked]
        ic = None
        if len(ic_day) >= 5 and ic_day["signal"].nunique(dropna=True) >= 2:
            raw_ic = ic_day["signal"].rank().corr(ic_day["forward_return"].rank())
            ic = float(raw_ic) if pd.notna(raw_ic) else None

        long_blocked = day["entry_limit_up"] | day["entry_suspended"]
        short_blocked = day["entry_limit_down"] | day["entry_suspended"]
        long_pool = day[~long_blocked]
        short_pool = day[~short_blocked]
        long_ret = None
        short_ret = None
        top_codes: set[str] | None = None
        bottom_codes: set[str] | None = None
        if len(long_pool) >= 5 and long_pool["signal"].nunique(dropna=True) >= 2:
            long_count = max(1, int(math.ceil(len(long_pool) * top_bottom_quantile)))
            top = long_pool.sort_values(["signal", "code"], ascending=[False, True]).head(long_count)
            long_ret = float(top["forward_return"].mean()) if pd.notna(top["forward_return"].mean()) else None
            top_codes = set(top["code"].astype(str))
        if len(short_pool) >= 5 and short_pool["signal"].nunique(dropna=True) >= 2:
            short_count = max(1, int(math.ceil(len(short_pool) * top_bottom_quantile)))
            bottom = short_pool.sort_values(["signal", "code"], ascending=[True, True]).head(short_count)
            short_ret = float(bottom["forward_return"].mean()) if pd.notna(bottom["forward_return"].mean()) else None
            bottom_codes = set(bottom["code"].astype(str))

        if previous_top is None or previous_bottom is None or top_codes is None or bottom_codes is None:
            top_turnover = None
            bottom_turnover = None
        else:
            top_turnover = 1.0 - (len(top_codes & previous_top) / max(1, len(top_codes)))
            bottom_turnover = 1.0 - (len(bottom_codes & previous_bottom) / max(1, len(bottom_codes)))
        if top_codes is not None:
            previous_top = top_codes
        if bottom_codes is not None:
            previous_bottom = bottom_codes

        rows.append(
            {
                "date": date,
                "window": _quarterly_window_label(date),
                "rank_ic": ic,
                "long_short_return": long_ret - short_ret
                if long_ret is not None and short_ret is not None
                else None,
                "average_one_way_turnover": None
                if top_turnover is None or bottom_turnover is None
                else (top_turnover + bottom_turnover) / 2.0,
            }
        )
    return pd.DataFrame(
        rows,
        columns=["date", "window", "rank_ic", "long_short_return", "average_one_way_turnover"],
    )


def _flag_mask(frame: pd.DataFrame, names: tuple[str, ...]) -> tuple[pd.Series, str | None]:
    for name in names:
        if name in frame.columns:
            values = pd.to_numeric(frame[name], errors="coerce")
            if values.notna().any():
                return values.fillna(0.0) > 0.0, name
    return pd.Series(False, index=frame.index), None


def _tdxgp_limit_status_masks(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series] | None:
    if "tdxgp_limit_status" not in frame.columns:
        return None
    status = pd.to_numeric(frame["tdxgp_limit_status"], errors="coerce")
    if not status.notna().any():
        return None
    return status.eq(TDXGP_LIMIT_UP_STATUS), status.eq(TDXGP_LIMIT_DOWN_STATUS)


def _limit_state_masks(frame: pd.DataFrame) -> dict[str, Any]:
    tdxgp_masks = _tdxgp_limit_status_masks(frame)
    if tdxgp_masks is not None:
        limit_up, limit_down = tdxgp_masks
        limit_up_source = f"{TDXGP_LIMIT_STATUS_SOURCE}==2"
        limit_down_source = f"{TDXGP_LIMIT_STATUS_SOURCE}==-2"
    else:
        limit_up, limit_up_source = _flag_mask(frame, ("is_limit_up", "limitup", "limit_up", "up_limit"))
        limit_down, limit_down_source = _flag_mask(frame, ("is_limit_down", "limitdown", "limit_down", "down_limit"))
    derived_from_rt_change = False
    if limit_up_source is None and limit_down_source is None and "rt_change_pct" in frame.columns:
        change = pd.to_numeric(frame["rt_change_pct"], errors="coerce")
        limit_up = change >= 9.8
        limit_down = change <= -9.8
        limit_up_source = "rt_change_pct>=9.8"
        limit_down_source = "rt_change_pct<=-9.8"
        derived_from_rt_change = True
    suspended = pd.to_numeric(frame["susp"], errors="coerce").fillna(0.0) > 0.0 if "susp" in frame.columns else pd.Series(False, index=frame.index)
    return {
        "limit_up": limit_up.fillna(False),
        "limit_down": limit_down.fillna(False),
        "suspended": suspended.fillna(False),
        "limit_up_source": limit_up_source,
        "limit_down_source": limit_down_source,
        "derived_from_rt_change": derived_from_rt_change,
        "available": limit_up_source is not None or limit_down_source is not None or bool(suspended.any()),
    }


def _tradability_summary(work: pd.DataFrame, masks: dict[str, Any]) -> dict[str, Any]:
    limit_up_count = int(work["entry_limit_up"].sum()) if "entry_limit_up" in work.columns else 0
    limit_down_count = int(work["entry_limit_down"].sum()) if "entry_limit_down" in work.columns else 0
    suspended_count = int(work["entry_suspended"].sum()) if "entry_suspended" in work.columns else 0
    exit_limit_up_count = int(work["exit_limit_up"].sum()) if "exit_limit_up" in work.columns else 0
    exit_limit_down_count = int(work["exit_limit_down"].sum()) if "exit_limit_down" in work.columns else 0
    exit_suspended_count = int(work["exit_suspended"].sum()) if "exit_suspended" in work.columns else 0
    return {
        "tradability_filter_available": bool(masks["available"]),
        "tradability_limit_up_source": masks["limit_up_source"],
        "tradability_limit_down_source": masks["limit_down_source"],
        "tradability_derived_from_rt_change": bool(masks["derived_from_rt_change"]),
        "tradability_entry_limit_up_row_count": limit_up_count,
        "tradability_entry_limit_down_row_count": limit_down_count,
        "tradability_entry_suspended_row_count": suspended_count,
        "tradability_exit_limit_up_row_count": exit_limit_up_count,
        "tradability_exit_limit_down_row_count": exit_limit_down_count,
        "tradability_exit_suspended_row_count": exit_suspended_count,
        "tradability_limit_up_row_count": limit_up_count,
        "tradability_limit_down_row_count": limit_down_count,
        "tradability_suspended_row_count": suspended_count,
        "tradability_ic_excluded_row_count": 0,
    }


@dataclass(frozen=True)
class ValidationWorkContext:
    signal_frame: pd.DataFrame
    signal_clock_report: dict[str, Any]
    base_work: pd.DataFrame
    tradability_masks: dict[str, Any]
    evaluation_start_date: pd.Timestamp | None
    evaluation_end_date: pd.Timestamp | None


def prepare_validation_work_context(
    frame: pd.DataFrame,
    *,
    horizon_days: int = 1,
    execution_lag_days: int = DEFAULT_EXECUTION_LAG_DAYS,
    signal_clock: str = SIGNAL_CLOCK_AFTER_CLOSE,
    evaluation_start_date: pd.Timestamp | None = None,
    evaluation_end_date: pd.Timestamp | None = None,
) -> ValidationWorkContext:
    """Precompute expression-independent validation columns for a loaded panel.

    This is an opt-in sidecar acceleration path. It deliberately preserves the
    same timestamp and tradability shifts used by validate_expression_on_loaded_panel.
    """

    signal_frame, signal_clock_report = _signal_evaluation_frame(frame, signal_clock=signal_clock)
    forward = _forward_return(frame, horizon_days, execution_lag_days=execution_lag_days)
    tradability_masks = _limit_state_masks(frame)
    base_work = frame[["date", "code"]].copy()
    base_work["forward_return"] = forward
    base_work["entry_limit_up"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_up"], days_ahead=execution_lag_days
    )
    base_work["entry_limit_down"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_down"], days_ahead=execution_lag_days
    )
    base_work["entry_suspended"] = _shift_mask_to_signal_date(
        frame, tradability_masks["suspended"], days_ahead=execution_lag_days
    )
    base_work["exit_limit_up"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_up"], days_ahead=execution_lag_days + horizon_days
    )
    base_work["exit_limit_down"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_down"], days_ahead=execution_lag_days + horizon_days
    )
    base_work["exit_suspended"] = _shift_mask_to_signal_date(
        frame, tradability_masks["suspended"], days_ahead=execution_lag_days + horizon_days
    )
    base_work = _attach_signal_safe_long_selection_diagnostics(
        frame,
        base_work,
        signal_clock_report["field_lags"],
    )
    if evaluation_start_date is not None:
        base_work = base_work[base_work["date"] >= evaluation_start_date].copy()
    if evaluation_end_date is not None:
        base_work = base_work[base_work["date"] <= evaluation_end_date].copy()
    base_work["window"] = base_work["date"].map(_quarterly_window_label)
    return ValidationWorkContext(
        signal_frame=signal_frame,
        signal_clock_report=signal_clock_report,
        base_work=base_work,
        tradability_masks=tradability_masks,
        evaluation_start_date=evaluation_start_date,
        evaluation_end_date=evaluation_end_date,
    )


def validate_expression_on_loaded_panel_fast_context(
    expression: str,
    frame: pd.DataFrame,
    context: ValidationWorkContext,
    *,
    dataset_path: Path | str,
    horizon_days: int = 1,
    execution_lag_days: int = DEFAULT_EXECUTION_LAG_DAYS,
    feature_lag_days: int = 0,
    top_bottom_quantile: float = 0.2,
    expression_cache: dict[str, pd.Series] | None = None,
) -> dict[str, Any]:
    signal = evaluate_panel_expression(
        context.signal_frame,
        expression,
        cache=expression_cache,
        field_lags=context.signal_clock_report["field_lags"],
    )
    work = context.base_work.copy()
    aligned_signal = _lag_signal_to_decision_date(frame, signal, feature_lag_days=feature_lag_days)
    work["signal"] = aligned_signal.loc[work.index]
    work = work.dropna(subset=["signal", "forward_return"])

    windows: list[dict[str, Any]] = []
    for window, window_frame in work.groupby("window", sort=True):
        daily_ics: list[float] = []
        daily_spreads: list[float] = []
        daily_longs: list[float] = []
        daily_diagnostics: dict[str, list[float]] = {
            metric_name: [] for metric_name in LONG_SELECTION_DIAGNOSTIC_FIELDS.values()
        }
        ic_excluded_rows = 0
        long_excluded_rows = 0
        short_excluded_rows = 0
        for _date, day in window_frame.groupby("date", sort=True):
            ic_blocked = day["entry_limit_up"] | day["entry_limit_down"] | day["entry_suspended"]
            ic_day = day[~ic_blocked]
            ic_excluded_rows += int(len(day) - len(ic_day))
            if len(ic_day) < 5 or ic_day["signal"].nunique(dropna=True) < 2:
                continue
            ic = ic_day["signal"].rank().corr(ic_day["forward_return"].rank())
            if pd.notna(ic):
                daily_ics.append(float(ic))
            long_blocked = day["entry_limit_up"] | day["entry_suspended"]
            short_blocked = day["entry_limit_down"] | day["entry_suspended"]
            long_pool = day[~long_blocked]
            short_pool = day[~short_blocked]
            long_excluded_rows += int(len(day) - len(long_pool))
            short_excluded_rows += int(len(day) - len(short_pool))
            if len(long_pool) < 5 or len(short_pool) < 5:
                continue
            long_count = max(1, int(math.ceil(len(long_pool) * top_bottom_quantile)))
            short_count = max(1, int(math.ceil(len(short_pool) * top_bottom_quantile)))
            top = long_pool.sort_values(["signal", "code"], ascending=[False, True]).head(long_count)
            long_ret = top["forward_return"].mean()
            short_ret = short_pool.sort_values(["signal", "code"], ascending=[True, True]).head(short_count)[
                "forward_return"
            ].mean()
            if pd.notna(long_ret):
                daily_longs.append(float(long_ret))
                for metric_name, value in _long_selection_diagnostics(top).items():
                    daily_diagnostics[metric_name].append(value)
            if pd.notna(long_ret) and pd.notna(short_ret):
                daily_spreads.append(float(long_ret - short_ret))
        ic_series = pd.Series(daily_ics, dtype=float)
        spread_series = pd.Series(daily_spreads, dtype=float)
        long_series = pd.Series(daily_longs, dtype=float)
        windows.append(
            {
                "window": str(window),
                "trading_day_count": int(len(ic_series)),
                "mean_rank_ic": round(float(ic_series.mean()), 6) if not ic_series.empty else None,
                "rank_ic_hit_rate": round(float((ic_series > 0).mean()), 6) if not ic_series.empty else None,
                "mean_long_return": round(float(long_series.mean()), 6) if not long_series.empty else None,
                "long_sortino": _sortino(long_series),
                "mean_long_short_return": round(float(spread_series.mean()), 6) if not spread_series.empty else None,
                "long_short_sortino": _sortino(spread_series),
                "tradability_ic_excluded_row_count": ic_excluded_rows,
                "tradability_long_excluded_unbuyable_or_unsellable_row_count": long_excluded_rows,
                "tradability_short_excluded_unsellable_or_unbuyable_row_count": short_excluded_rows,
                **{
                    f"mean_{metric_name}": _mean_or_none(values)
                    for metric_name, values in daily_diagnostics.items()
                    if values
                },
            }
        )

    valid_ics = [item["mean_rank_ic"] for item in windows if item["mean_rank_ic"] is not None]
    valid_long_returns = [item["mean_long_return"] for item in windows if item["mean_long_return"] is not None]
    valid_long_sortinos = [item["long_sortino"] for item in windows if item["long_sortino"] is not None]
    valid_sortinos = [item["long_short_sortino"] for item in windows if item["long_short_sortino"] is not None]
    diagnostic_summary = {
        f"mean_window_{metric_name}": _mean_diagnostic_windows(windows, metric_name)
        for metric_name in LONG_SELECTION_DIAGNOSTIC_FIELDS.values()
    }
    tradability_summary = _tradability_summary(work, context.tradability_masks)
    tradability_summary["tradability_ic_excluded_row_count"] = int(
        sum(item.get("tradability_ic_excluded_row_count", 0) for item in windows)
    )
    return {
        "expression": expression,
        "dataset_path": str(dataset_path),
        "horizon_days": horizon_days,
        "execution_lag_days": execution_lag_days,
        **context.signal_clock_report,
        "feature_lag_days": feature_lag_days,
        "feature_timestamp_policy": _feature_timestamp_policy(context.signal_clock_report["signal_clock"], feature_lag_days),
        "execution_policy": "signal_t_execute_t_plus_1_exit_t_plus_2_close_to_close"
        if execution_lag_days == 1 and horizon_days == 1
        else "custom_close_to_close_execution_lag",
        "validation_period_months": REAL_MARKET_VALIDATION_PERIOD_MONTHS,
        "validation_period_policy": "quarterly_3_month_windows",
        "evaluation_start_date": context.evaluation_start_date.date().isoformat()
        if context.evaluation_start_date is not None
        else None,
        "evaluation_end_date": context.evaluation_end_date.date().isoformat()
        if context.evaluation_end_date is not None
        else None,
        "row_count_after_signal_and_target": int(len(work)),
        "window_count": len(windows),
        "mean_window_rank_ic": round(float(np.mean(valid_ics)), 6) if valid_ics else None,
        "mean_window_long_return": round(float(np.mean(valid_long_returns)), 6) if valid_long_returns else None,
        "mean_window_long_sortino": round(float(np.mean(valid_long_sortinos)), 6) if valid_long_sortinos else None,
        "mean_window_sortino": round(float(np.mean(valid_sortinos)), 6) if valid_sortinos else None,
        **diagnostic_summary,
        **tradability_summary,
        "windows": windows,
        "validation_acceleration_mode": "precomputed_work_context",
    }


def validate_expression_on_loaded_panel(
    expression: str,
    frame: pd.DataFrame,
    *,
    dataset_path: Path | str,
    horizon_days: int = 1,
    execution_lag_days: int = DEFAULT_EXECUTION_LAG_DAYS,
    signal_clock: str = SIGNAL_CLOCK_AFTER_CLOSE,
    feature_lag_days: int = 0,
    top_bottom_quantile: float = 0.2,
    evaluation_start_date: pd.Timestamp | None = None,
    evaluation_end_date: pd.Timestamp | None = None,
    expression_cache: dict[str, pd.Series] | None = None,
) -> dict[str, Any]:
    signal_frame, signal_clock_report = _signal_evaluation_frame(frame, signal_clock=signal_clock)
    signal = evaluate_panel_expression(
        signal_frame,
        expression,
        cache=expression_cache,
        field_lags=signal_clock_report["field_lags"],
    )
    forward = _forward_return(frame, horizon_days, execution_lag_days=execution_lag_days)
    tradability_masks = _limit_state_masks(frame)
    work = frame[["date", "code"]].copy()
    work["signal"] = _lag_signal_to_decision_date(frame, signal, feature_lag_days=feature_lag_days)
    work["forward_return"] = forward
    work["entry_limit_up"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_up"], days_ahead=execution_lag_days
    )
    work["entry_limit_down"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_down"], days_ahead=execution_lag_days
    )
    work["entry_suspended"] = _shift_mask_to_signal_date(
        frame, tradability_masks["suspended"], days_ahead=execution_lag_days
    )
    work["exit_limit_up"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_up"], days_ahead=execution_lag_days + horizon_days
    )
    work["exit_limit_down"] = _shift_mask_to_signal_date(
        frame, tradability_masks["limit_down"], days_ahead=execution_lag_days + horizon_days
    )
    work["exit_suspended"] = _shift_mask_to_signal_date(
        frame, tradability_masks["suspended"], days_ahead=execution_lag_days + horizon_days
    )
    work = _attach_signal_safe_long_selection_diagnostics(frame, work, signal_clock_report["field_lags"])
    work = work.dropna(subset=["signal", "forward_return"])
    if evaluation_start_date is not None:
        work = work[work["date"] >= evaluation_start_date]
    if evaluation_end_date is not None:
        work = work[work["date"] <= evaluation_end_date]
    work["window"] = work["date"].map(_quarterly_window_label)

    windows: list[dict[str, Any]] = []
    for window, window_frame in work.groupby("window", sort=True):
        daily_ics: list[float] = []
        daily_spreads: list[float] = []
        daily_longs: list[float] = []
        daily_diagnostics: dict[str, list[float]] = {
            metric_name: [] for metric_name in LONG_SELECTION_DIAGNOSTIC_FIELDS.values()
        }
        ic_excluded_rows = 0
        long_excluded_rows = 0
        short_excluded_rows = 0
        for _date, day in window_frame.groupby("date", sort=True):
            # Entry tradability is knowable at the assumed execution point.
            # Exit-day limit states are future information relative to signal t
            # and must not be used to drop rows from IC or spread evaluation.
            ic_blocked = day["entry_limit_up"] | day["entry_limit_down"] | day["entry_suspended"]
            ic_day = day[~ic_blocked]
            ic_excluded_rows += int(len(day) - len(ic_day))
            if len(ic_day) < 5 or ic_day["signal"].nunique(dropna=True) < 2:
                continue
            ic = ic_day["signal"].rank().corr(ic_day["forward_return"].rank())
            if pd.notna(ic):
                daily_ics.append(float(ic))
            long_blocked = day["entry_limit_up"] | day["entry_suspended"]
            short_blocked = day["entry_limit_down"] | day["entry_suspended"]
            long_pool = day[~long_blocked]
            short_pool = day[~short_blocked]
            long_excluded_rows += int(len(day) - len(long_pool))
            short_excluded_rows += int(len(day) - len(short_pool))
            if len(long_pool) < 5 or len(short_pool) < 5:
                continue
            long_count = max(1, int(math.ceil(len(long_pool) * top_bottom_quantile)))
            short_count = max(1, int(math.ceil(len(short_pool) * top_bottom_quantile)))
            top = long_pool.sort_values(["signal", "code"], ascending=[False, True]).head(long_count)
            long_ret = top["forward_return"].mean()
            short_ret = short_pool.sort_values(["signal", "code"], ascending=[True, True]).head(short_count)[
                "forward_return"
            ].mean()
            if pd.notna(long_ret):
                daily_longs.append(float(long_ret))
                for metric_name, value in _long_selection_diagnostics(top).items():
                    daily_diagnostics[metric_name].append(value)
            if pd.notna(long_ret) and pd.notna(short_ret):
                daily_spreads.append(float(long_ret - short_ret))
        ic_series = pd.Series(daily_ics, dtype=float)
        spread_series = pd.Series(daily_spreads, dtype=float)
        long_series = pd.Series(daily_longs, dtype=float)
        windows.append(
            {
                "window": str(window),
                "trading_day_count": int(len(ic_series)),
                "mean_rank_ic": round(float(ic_series.mean()), 6) if not ic_series.empty else None,
                "rank_ic_hit_rate": round(float((ic_series > 0).mean()), 6) if not ic_series.empty else None,
                "mean_long_return": round(float(long_series.mean()), 6) if not long_series.empty else None,
                "long_sortino": _sortino(long_series),
                "mean_long_short_return": round(float(spread_series.mean()), 6) if not spread_series.empty else None,
                "long_short_sortino": _sortino(spread_series),
                "tradability_ic_excluded_row_count": ic_excluded_rows,
                "tradability_long_excluded_unbuyable_or_unsellable_row_count": long_excluded_rows,
                "tradability_short_excluded_unsellable_or_unbuyable_row_count": short_excluded_rows,
                **{
                    f"mean_{metric_name}": _mean_or_none(values)
                    for metric_name, values in daily_diagnostics.items()
                    if values
                },
            }
        )

    valid_ics = [item["mean_rank_ic"] for item in windows if item["mean_rank_ic"] is not None]
    valid_long_returns = [item["mean_long_return"] for item in windows if item["mean_long_return"] is not None]
    valid_long_sortinos = [item["long_sortino"] for item in windows if item["long_sortino"] is not None]
    valid_sortinos = [item["long_short_sortino"] for item in windows if item["long_short_sortino"] is not None]
    diagnostic_summary = {
        f"mean_window_{metric_name}": _mean_diagnostic_windows(windows, metric_name)
        for metric_name in LONG_SELECTION_DIAGNOSTIC_FIELDS.values()
    }
    tradability_summary = _tradability_summary(work, tradability_masks)
    tradability_summary["tradability_ic_excluded_row_count"] = int(
        sum(item.get("tradability_ic_excluded_row_count", 0) for item in windows)
    )
    return {
        "expression": expression,
        "dataset_path": str(dataset_path),
        "horizon_days": horizon_days,
        "execution_lag_days": execution_lag_days,
        **signal_clock_report,
        "feature_lag_days": feature_lag_days,
        "feature_timestamp_policy": _feature_timestamp_policy(signal_clock, feature_lag_days),
        "execution_policy": "signal_t_execute_t_plus_1_exit_t_plus_2_close_to_close"
        if execution_lag_days == 1 and horizon_days == 1
        else "custom_close_to_close_execution_lag",
        "validation_period_months": REAL_MARKET_VALIDATION_PERIOD_MONTHS,
        "validation_period_policy": "quarterly_3_month_windows",
        "evaluation_start_date": evaluation_start_date.date().isoformat() if evaluation_start_date is not None else None,
        "evaluation_end_date": evaluation_end_date.date().isoformat() if evaluation_end_date is not None else None,
        "row_count_after_signal_and_target": int(len(work)),
        "window_count": len(windows),
        "mean_window_rank_ic": round(float(np.mean(valid_ics)), 6) if valid_ics else None,
        "mean_window_long_return": round(float(np.mean(valid_long_returns)), 6) if valid_long_returns else None,
        "mean_window_long_sortino": round(float(np.mean(valid_long_sortinos)), 6) if valid_long_sortinos else None,
        "mean_window_sortino": round(float(np.mean(valid_sortinos)), 6) if valid_sortinos else None,
        **diagnostic_summary,
        **tradability_summary,
        "windows": windows,
    }


def validate_expression_on_real_market_panel(
    expression: str,
    *,
    path: Path | str = DEFAULT_REAL_MARKET_DATASET_PATH,
    horizon_days: int = 1,
    execution_lag_days: int = DEFAULT_EXECUTION_LAG_DAYS,
    signal_clock: str = SIGNAL_CLOCK_AFTER_CLOSE,
    feature_lag_days: int = 0,
    top_bottom_quantile: float = 0.2,
    max_rows: int | None = None,
    enable_trend_state_features: bool = False,
) -> dict[str, Any]:
    frame = _load_market_panel(path, max_rows=max_rows, enable_trend_state_features=enable_trend_state_features)
    return validate_expression_on_loaded_panel(
        expression,
        frame,
        dataset_path=path,
        horizon_days=horizon_days,
        execution_lag_days=execution_lag_days,
        signal_clock=signal_clock,
        feature_lag_days=feature_lag_days,
        top_bottom_quantile=top_bottom_quantile,
    )


def _recent_window_summary(windows: list[dict[str, Any]], *, recent_window_count: int) -> dict[str, Any]:
    recent = windows[-recent_window_count:]
    recent_ics = [item["mean_rank_ic"] for item in recent if item["mean_rank_ic"] is not None]
    recent_sortinos = [item["long_short_sortino"] for item in recent if item["long_short_sortino"] is not None]
    return {
        "recent_window_count": len(recent),
        "recent_mean_rank_ic": round(float(np.mean(recent_ics)), 6) if recent_ics else None,
        "recent_positive_rank_ic_ratio": round(float(np.mean([value > 0 for value in recent_ics])), 6)
        if recent_ics
        else None,
        "recent_mean_sortino": round(float(np.mean(recent_sortinos)), 6) if recent_sortinos else None,
        "recent_windows": recent,
    }


def _smoke_flags(report: dict[str, Any], recent_summary: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    mean_ic = report.get("mean_window_rank_ic")
    recent_ic = recent_summary.get("recent_mean_rank_ic")
    positive_ratio = recent_summary.get("recent_positive_rank_ic_ratio")
    if mean_ic is None:
        flags.append("no_valid_quarterly_rank_ic")
    elif mean_ic < 0.01:
        flags.append("weak_mean_rank_ic_below_0_01")
    if recent_ic is None:
        flags.append("no_recent_quarterly_rank_ic")
    elif recent_ic <= 0:
        flags.append("non_positive_recent_mean_rank_ic")
    if positive_ratio is not None and positive_ratio < 0.5:
        flags.append("recent_positive_quarter_ratio_below_0_5")
    if report.get("window_count", 0) < 4:
        flags.append("insufficient_quarterly_windows")
    return flags


def _fast_screen_decision(report: dict[str, Any]) -> str:
    mean_ic = report.get("mean_window_rank_ic")
    if mean_ic is None:
        return "reject_no_valid_recent_ic"
    if mean_ic >= 0.01:
        return "needs_full_history_review"
    if mean_ic > 0:
        return "watchlist_weak_positive_recent_ic"
    return "reject_non_positive_recent_ic"


def _validation_evaluation_record(
    *,
    record: dict[str, Any],
    report: dict[str, Any],
    recent_summary: dict[str, Any],
    flags: list[str],
    fast_screen_decision: str | None,
    cost_report: dict[str, Any],
) -> dict[str, Any]:
    return {
        "candidate_id": str(record.get("candidate_id", "")),
        "expression": str(record.get("expression", "")),
        "retained": bool(record.get("retained")),
        "source_mode": record.get("source_mode"),
        "frontier_lane": record.get("frontier_lane"),
        "archive_cell": record.get("archive_cell"),
        "primitive_family": record.get("primitive_family"),
        "direction": record.get("direction"),
        "window": record.get("window"),
        "proposal_kind": record.get("proposal_kind"),
        "posterior_mean": record.get("posterior_mean"),
        "posterior_std": record.get("posterior_std"),
        "canonical_rank_validation_expression": record.get("canonical_rank_validation_expression"),
        "family_mass": record.get("family_mass"),
        "momentum_weight": record.get("momentum_weight"),
        "gap_weight": record.get("gap_weight"),
        "momentum_window": record.get("momentum_window"),
        "gap_window": record.get("gap_window"),
        "short_window": record.get("short_window"),
        "long_window": record.get("long_window"),
        "smoothing_window": record.get("smoothing_window"),
        "slope_lag": record.get("slope_lag"),
        "volatility_window": record.get("volatility_window"),
        "base_transform": record.get("base_transform"),
        "numerator_window": record.get("numerator_window"),
        "denominator_window": record.get("denominator_window"),
        "denominator_family": record.get("denominator_family"),
        "research_track": record.get("research_track"),
        "quarter_floor_required": record.get("quarter_floor_required"),
        "regime_conditional_audit": record.get("regime_conditional_audit"),
        "turnover_cost_shadow_required": record.get("turnover_cost_shadow_required"),
        "numerator_transform": record.get("numerator_transform"),
        "denominator_transform": record.get("denominator_transform"),
        "numerator_smoothing_window": record.get("numerator_smoothing_window"),
        "denominator_smoothing_window": record.get("denominator_smoothing_window"),
        "center_overlap_audit_required": record.get("center_overlap_audit_required"),
        "center_signal_expression": record.get("center_signal_expression"),
        "orthogonalization_mode": record.get("orthogonalization_mode"),
        "denominator_kernel_id": record.get("denominator_kernel_id"),
        "denominator_kernel_expression": record.get("denominator_kernel_expression"),
        "denominator_kernel_shape": record.get("denominator_kernel_shape"),
        "denominator_kernel_weights": record.get("denominator_kernel_weights"),
        "effective_denominator_horizon": record.get("effective_denominator_horizon"),
        "mean_window_rank_ic": report["mean_window_rank_ic"],
        "mean_window_long_return": report["mean_window_long_return"],
        "mean_window_long_sortino": report["mean_window_long_sortino"],
        "mean_window_sortino": report["mean_window_sortino"],
        "mean_window_long_selected_amount": report.get("mean_window_long_selected_amount"),
        "mean_window_long_selected_turnover_rate": report.get("mean_window_long_selected_turnover_rate"),
        "mean_window_long_selected_final_float_market_cap": report.get(
            "mean_window_long_selected_final_float_market_cap"
        ),
        "mean_window_long_selected_final_total_market_cap": report.get(
            "mean_window_long_selected_final_total_market_cap"
        ),
        "mean_window_long_selected_final_float_market_cap_billion": report.get(
            "mean_window_long_selected_final_float_market_cap_billion"
        ),
        "mean_window_long_selected_final_total_market_cap_billion": report.get(
            "mean_window_long_selected_final_total_market_cap_billion"
        ),
        "mean_window_long_selected_market_cap_conflict_rate": report.get(
            "mean_window_long_selected_market_cap_conflict_rate"
        ),
        "row_count_after_signal_and_target": report["row_count_after_signal_and_target"],
        "window_count": report["window_count"],
        "execution_lag_days": report["execution_lag_days"],
        "signal_clock": report["signal_clock"],
        "field_lag_policy": report["field_lag_policy"],
        "field_lags": report["field_lags"],
        "feature_lag_days": report["feature_lag_days"],
        "feature_timestamp_policy": report["feature_timestamp_policy"],
        "execution_policy": report["execution_policy"],
        "tradability_filter_available": report["tradability_filter_available"],
        "tradability_limit_up_source": report["tradability_limit_up_source"],
        "tradability_limit_down_source": report["tradability_limit_down_source"],
        "tradability_derived_from_rt_change": report["tradability_derived_from_rt_change"],
        "tradability_entry_limit_up_row_count": report["tradability_entry_limit_up_row_count"],
        "tradability_entry_limit_down_row_count": report["tradability_entry_limit_down_row_count"],
        "tradability_entry_suspended_row_count": report["tradability_entry_suspended_row_count"],
        "tradability_exit_limit_up_row_count": report["tradability_exit_limit_up_row_count"],
        "tradability_exit_limit_down_row_count": report["tradability_exit_limit_down_row_count"],
        "tradability_exit_suspended_row_count": report["tradability_exit_suspended_row_count"],
        "tradability_limit_up_row_count": report["tradability_limit_up_row_count"],
        "tradability_limit_down_row_count": report["tradability_limit_down_row_count"],
        "tradability_suspended_row_count": report["tradability_suspended_row_count"],
        "tradability_ic_excluded_row_count": report["tradability_ic_excluded_row_count"],
        "evaluation_start_date": report["evaluation_start_date"],
        "evaluation_end_date": report["evaluation_end_date"],
        "smoke_flags": flags,
        "passes_real_market_smoke": not flags,
        "fast_screen_decision": fast_screen_decision,
        "promoted_to_full_history_review": fast_screen_decision == "needs_full_history_review",
        "validation_lane": cost_report["validation_lane"],
        "validation_role": cost_report["validation_role"],
        "estimated_validation_cost_score": cost_report["estimated_validation_cost_score"],
        "relation_operator_count": cost_report["relation_operator_count"],
        "rolling_operator_count": cost_report["rolling_operator_count"],
        **recent_summary,
    }


def _validate_record_on_loaded_panel(
    record: dict[str, Any],
    *,
    frame: pd.DataFrame,
    path: Path | str,
    horizon_days: int,
    execution_lag_days: int,
    signal_clock: str,
    feature_lag_days: int,
    top_bottom_quantile: float,
    evaluation_start_date: pd.Timestamp | None,
    evaluation_end_date: pd.Timestamp | None,
    recent_window_count: int,
    fast_recent_window_only: bool,
    enable_trend_state_features: bool,
) -> dict[str, Any]:
    expression = str(record.get("expression", ""))
    candidate_id = str(record.get("candidate_id", ""))
    local_cache: dict[str, pd.Series] = {}
    try:
        report = validate_expression_on_loaded_panel(
            expression,
            frame,
            dataset_path=path,
            horizon_days=horizon_days,
            execution_lag_days=execution_lag_days,
            signal_clock=signal_clock,
            feature_lag_days=feature_lag_days,
            top_bottom_quantile=top_bottom_quantile,
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
            expression_cache=local_cache,
        )
    except UnsupportedExpressionError as error:
        return {
            "kind": "unsupported",
            "cache_count": len(local_cache),
            "item": {
                "candidate_id": candidate_id,
                "expression": expression,
                "error": str(error),
                "source_mode": record.get("source_mode"),
                "archive_cell": record.get("archive_cell"),
            },
        }
    recent_summary = _recent_window_summary(report["windows"], recent_window_count=recent_window_count)
    flags = _smoke_flags(report, recent_summary)
    fast_screen_decision = _fast_screen_decision(report) if fast_recent_window_only else None
    cost_report = expression_validation_cost_report(expression)
    return {
        "kind": "evaluation",
        "cache_count": len(local_cache),
        "validation_cache_key": (
            expand_derived_fields(expression.strip()),
            int(horizon_days),
            int(execution_lag_days),
            str(signal_clock),
            int(feature_lag_days),
            float(top_bottom_quantile),
            evaluation_start_date.date().isoformat() if evaluation_start_date is not None else None,
            evaluation_end_date.date().isoformat() if evaluation_end_date is not None else None,
            bool(enable_trend_state_features),
        ),
        "item": _validation_evaluation_record(
            record=record,
            report=report,
            recent_summary=recent_summary,
            flags=flags,
            fast_screen_decision=fast_screen_decision,
            cost_report=cost_report,
        ),
    }


def batch_validate_candidate_ledger(
    ledger_path: Path | str,
    *,
    path: Path | str = DEFAULT_REAL_MARKET_DATASET_PATH,
    retained_only: bool = True,
    max_candidates: int | None = None,
    horizon_days: int = 1,
    execution_lag_days: int | None = None,
    signal_clock: str | None = None,
    feature_lag_days: int | None = None,
    top_bottom_quantile: float = 0.2,
    max_rows: int | None = None,
    recent_window_count: int = 4,
    fast_recent_window_only: bool = False,
    recent_quarter_window_count: int | None = None,
    recent_lookback_days: int = DEFAULT_RECENT_LOOKBACK_DAYS,
    recent_warmup_days: int = DEFAULT_RECENT_WARMUP_DAYS,
    parallel_workers: int = 1,
    enable_trend_state_features: bool | None = None,
    use_fast_context: bool = False,
) -> dict[str, Any]:
    ledger = json.loads(Path(ledger_path).read_text(encoding="utf-8"))
    recommended_validation_kwargs = ledger.get("recommended_validation_kwargs", {})
    if not isinstance(recommended_validation_kwargs, dict):
        recommended_validation_kwargs = {}
    validation_defaults_source: dict[str, str] = {}
    if execution_lag_days is None:
        execution_lag_days = int(recommended_validation_kwargs.get("execution_lag_days", DEFAULT_EXECUTION_LAG_DAYS))
        validation_defaults_source["execution_lag_days"] = (
            "ledger_recommended_validation_kwargs"
            if "execution_lag_days" in recommended_validation_kwargs
            else "function_default"
        )
    else:
        validation_defaults_source["execution_lag_days"] = "explicit_argument"
    if signal_clock is None:
        signal_clock = str(recommended_validation_kwargs.get("signal_clock", SIGNAL_CLOCK_AFTER_CLOSE))
        validation_defaults_source["signal_clock"] = (
            "ledger_recommended_validation_kwargs"
            if "signal_clock" in recommended_validation_kwargs
            else "function_default"
        )
    else:
        validation_defaults_source["signal_clock"] = "explicit_argument"
    if feature_lag_days is None:
        feature_lag_days = int(recommended_validation_kwargs.get("feature_lag_days", 0))
        validation_defaults_source["feature_lag_days"] = (
            "ledger_recommended_validation_kwargs"
            if "feature_lag_days" in recommended_validation_kwargs
            else "function_default"
        )
    else:
        validation_defaults_source["feature_lag_days"] = "explicit_argument"
    if enable_trend_state_features is None:
        enable_trend_state_features = bool(recommended_validation_kwargs.get("enable_trend_state_features", False))
        validation_defaults_source["enable_trend_state_features"] = (
            "ledger_recommended_validation_kwargs"
            if "enable_trend_state_features" in recommended_validation_kwargs
            else "function_default"
        )
    else:
        validation_defaults_source["enable_trend_state_features"] = "explicit_argument"

    records = list(ledger.get("records", []))
    if retained_only:
        records = [record for record in records if record.get("retained")]
    if max_candidates is not None:
        records = records[:max_candidates]

    evaluation_start_date: pd.Timestamp | None = None
    evaluation_end_date: pd.Timestamp | None = None
    if fast_recent_window_only:
        frame, evaluation_start_date, evaluation_end_date = _load_recent_market_panel(
            path,
            lookback_days=recent_lookback_days,
            warmup_days=recent_warmup_days,
            max_rows=max_rows,
            enable_trend_state_features=enable_trend_state_features,
        )
    elif recent_quarter_window_count is not None:
        frame, evaluation_start_date, evaluation_end_date = _load_recent_quarter_market_panel(
            path,
            quarter_window_count=recent_quarter_window_count,
            warmup_days=recent_warmup_days,
            max_rows=max_rows,
            enable_trend_state_features=enable_trend_state_features,
        )
    else:
        frame = _load_market_panel(path, max_rows=max_rows, enable_trend_state_features=enable_trend_state_features)
    evaluations: list[dict[str, Any]] = []
    unsupported: list[dict[str, Any]] = []
    expression_cache: dict[str, pd.Series] = {}
    fast_context = (
        prepare_validation_work_context(
            frame,
            horizon_days=horizon_days,
            execution_lag_days=execution_lag_days,
            signal_clock=signal_clock,
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
        )
        if use_fast_context and parallel_workers <= 1
        else None
    )
    validation_report_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    unsupported_validation_cache: dict[tuple[Any, ...], str] = {}
    validation_report_cache_hit_count = 0
    unsupported_validation_cache_hit_count = 0
    effective_parallel_workers = max(1, int(parallel_workers))
    if effective_parallel_workers > 1 and len(records) > 1:
        parallel_cached_expression_count = 0
        with ThreadPoolExecutor(max_workers=min(effective_parallel_workers, len(records))) as executor:
            futures = [
                executor.submit(
                    _validate_record_on_loaded_panel,
                    record,
                    frame=frame,
                    path=path,
                    horizon_days=horizon_days,
                    execution_lag_days=execution_lag_days,
                    signal_clock=signal_clock,
                    feature_lag_days=feature_lag_days,
                    top_bottom_quantile=top_bottom_quantile,
                    evaluation_start_date=evaluation_start_date,
                    evaluation_end_date=evaluation_end_date,
                    recent_window_count=recent_window_count,
                    fast_recent_window_only=fast_recent_window_only,
                    enable_trend_state_features=enable_trend_state_features,
                )
                for record in records
            ]
            validation_cache_keys: set[tuple[Any, ...]] = set()
            for future in as_completed(futures):
                result = future.result()
                parallel_cached_expression_count += int(result.get("cache_count", 0))
                if result["kind"] == "unsupported":
                    unsupported.append(result["item"])
                else:
                    evaluations.append(result["item"])
                    validation_cache_keys.add(result["validation_cache_key"])

        evaluations.sort(
            key=lambda item: (
                item["mean_window_rank_ic"] is not None,
                item["mean_window_rank_ic"] or -999.0,
                item["recent_mean_rank_ic"] or -999.0,
            ),
            reverse=True,
        )
        passed = [item for item in evaluations if item["passes_real_market_smoke"]]
        promoted = [item for item in evaluations if item["promoted_to_full_history_review"]]
        return {
            "ledger_path": str(ledger_path),
            "source_run_id": ledger.get("run_id"),
            "dataset_path": str(path),
            "retained_only": retained_only,
            "requested_candidate_count": len(records),
            "evaluated_count": len(evaluations),
            "unsupported_count": len(unsupported),
            "passed_smoke_count": len(passed),
            "promoted_to_full_history_review_count": len(promoted),
            "validation_period_months": REAL_MARKET_VALIDATION_PERIOD_MONTHS,
            "validation_period_policy": "quarterly_3_month_windows",
            "horizon_days": horizon_days,
            "execution_lag_days": execution_lag_days,
            "signal_clock": signal_clock,
            "signal_clock_field_lags": _signal_clock_field_lags(signal_clock),
            "feature_lag_days": feature_lag_days,
            "enable_trend_state_features": bool(enable_trend_state_features),
            "trend_state_feature_contract": trend_state_feature_contract() if enable_trend_state_features else None,
            "validation_defaults_source": validation_defaults_source,
            "ledger_recommended_validation_kwargs": recommended_validation_kwargs,
            "feature_timestamp_policy": _feature_timestamp_policy(signal_clock, feature_lag_days),
            "execution_policy": "signal_t_execute_t_plus_1_exit_t_plus_2_close_to_close"
            if execution_lag_days == 1 and horizon_days == 1
            else "custom_close_to_close_execution_lag",
            "top_bottom_quantile": top_bottom_quantile,
            "screening_mode": "recent_3_month_fast_screen"
            if fast_recent_window_only
            else (
                f"recent_{recent_quarter_window_count}_quarter_multi_cycle_smoke"
                if recent_quarter_window_count is not None
                else "full_history_quarterly_smoke"
            ),
            "recent_lookback_days": recent_lookback_days if fast_recent_window_only else None,
            "recent_quarter_window_count": recent_quarter_window_count,
            "recent_warmup_days": recent_warmup_days
            if fast_recent_window_only or recent_quarter_window_count is not None
            else None,
            "evaluation_start_date": evaluation_start_date.date().isoformat()
            if evaluation_start_date is not None
            else None,
            "evaluation_end_date": evaluation_end_date.date().isoformat()
            if evaluation_end_date is not None
            else None,
            "loaded_panel_rows": int(len(frame)),
            "parallel_workers": effective_parallel_workers,
            "parallel_validation_mode": "threaded_shared_loaded_panel",
            "validation_acceleration_mode": "baseline_parallel",
            "cached_expression_count": parallel_cached_expression_count,
            "unique_validated_expression_count": len(validation_cache_keys),
            "validation_report_cache_hit_count": validation_report_cache_hit_count,
            "unsupported_validation_cache_hit_count": unsupported_validation_cache_hit_count,
            "validation_report_cache_key_policy": "expanded_stripped_expression_plus_validation_contract",
            "real_edge_claim_allowed": False,
            "real_edge_claim_blockers": [
                "smoke_validation_only_not_purged_walk_forward_oos",
                "transaction_cost_slippage_capacity_not_applied",
                "factor_exposure_and_crowding_not_audited",
                "forward_shadow_validation_not_run",
            ],
            "evaluations": evaluations,
            "unsupported": unsupported,
        }

    for record in records:
        expression = str(record.get("expression", ""))
        candidate_id = str(record.get("candidate_id", ""))
        canonical_expression = expand_derived_fields(expression.strip())
        validation_cache_key = (
            canonical_expression,
            int(horizon_days),
            int(execution_lag_days),
            str(signal_clock),
            int(feature_lag_days),
            float(top_bottom_quantile),
            evaluation_start_date.date().isoformat() if evaluation_start_date is not None else None,
            evaluation_end_date.date().isoformat() if evaluation_end_date is not None else None,
            bool(enable_trend_state_features),
        )
        try:
            if validation_cache_key in validation_report_cache:
                validation_report_cache_hit_count += 1
                report = validation_report_cache[validation_cache_key]
            elif validation_cache_key in unsupported_validation_cache:
                unsupported_validation_cache_hit_count += 1
                raise UnsupportedExpressionError(unsupported_validation_cache[validation_cache_key])
            else:
                if fast_context is not None:
                    report = validate_expression_on_loaded_panel_fast_context(
                        expression,
                        frame,
                        fast_context,
                        dataset_path=path,
                        horizon_days=horizon_days,
                        execution_lag_days=execution_lag_days,
                        feature_lag_days=feature_lag_days,
                        top_bottom_quantile=top_bottom_quantile,
                        expression_cache=expression_cache,
                    )
                else:
                    report = validate_expression_on_loaded_panel(
                        expression,
                        frame,
                        dataset_path=path,
                        horizon_days=horizon_days,
                        execution_lag_days=execution_lag_days,
                        signal_clock=signal_clock,
                        feature_lag_days=feature_lag_days,
                        top_bottom_quantile=top_bottom_quantile,
                        evaluation_start_date=evaluation_start_date,
                        evaluation_end_date=evaluation_end_date,
                        expression_cache=expression_cache,
                    )
                validation_report_cache[validation_cache_key] = report
        except UnsupportedExpressionError as error:
            unsupported_validation_cache[validation_cache_key] = str(error)
            unsupported.append(
                {
                    "candidate_id": candidate_id,
                    "expression": expression,
                    "error": str(error),
                    "source_mode": record.get("source_mode"),
                    "archive_cell": record.get("archive_cell"),
                }
            )
            continue
        recent_summary = _recent_window_summary(report["windows"], recent_window_count=recent_window_count)
        flags = _smoke_flags(report, recent_summary)
        fast_screen_decision = _fast_screen_decision(report) if fast_recent_window_only else None
        cost_report = expression_validation_cost_report(expression)
        evaluations.append(
            _validation_evaluation_record(
                record=record,
                report=report,
                recent_summary=recent_summary,
                flags=flags,
                fast_screen_decision=fast_screen_decision,
                cost_report=cost_report,
            )
        )

    evaluations.sort(
        key=lambda item: (
            item["mean_window_rank_ic"] is not None,
            item["mean_window_rank_ic"] or -999.0,
            item["recent_mean_rank_ic"] or -999.0,
        ),
        reverse=True,
    )
    passed = [item for item in evaluations if item["passes_real_market_smoke"]]
    promoted = [item for item in evaluations if item["promoted_to_full_history_review"]]
    return {
        "ledger_path": str(ledger_path),
        "source_run_id": ledger.get("run_id"),
        "dataset_path": str(path),
        "retained_only": retained_only,
        "requested_candidate_count": len(records),
        "evaluated_count": len(evaluations),
        "unsupported_count": len(unsupported),
        "passed_smoke_count": len(passed),
        "promoted_to_full_history_review_count": len(promoted),
        "validation_period_months": REAL_MARKET_VALIDATION_PERIOD_MONTHS,
        "validation_period_policy": "quarterly_3_month_windows",
        "horizon_days": horizon_days,
        "execution_lag_days": execution_lag_days,
        "signal_clock": signal_clock,
        "signal_clock_field_lags": _signal_clock_field_lags(signal_clock),
        "feature_lag_days": feature_lag_days,
        "enable_trend_state_features": bool(enable_trend_state_features),
        "trend_state_feature_contract": trend_state_feature_contract() if enable_trend_state_features else None,
        "validation_defaults_source": validation_defaults_source,
        "ledger_recommended_validation_kwargs": recommended_validation_kwargs,
        "feature_timestamp_policy": _feature_timestamp_policy(signal_clock, feature_lag_days),
        "execution_policy": "signal_t_execute_t_plus_1_exit_t_plus_2_close_to_close"
        if execution_lag_days == 1 and horizon_days == 1
        else "custom_close_to_close_execution_lag",
        "top_bottom_quantile": top_bottom_quantile,
        "screening_mode": "recent_3_month_fast_screen"
        if fast_recent_window_only
        else (
            f"recent_{recent_quarter_window_count}_quarter_multi_cycle_smoke"
            if recent_quarter_window_count is not None
            else "full_history_quarterly_smoke"
        ),
        "recent_lookback_days": recent_lookback_days if fast_recent_window_only else None,
        "recent_quarter_window_count": recent_quarter_window_count,
        "recent_warmup_days": recent_warmup_days if fast_recent_window_only or recent_quarter_window_count is not None else None,
        "evaluation_start_date": evaluation_start_date.date().isoformat() if evaluation_start_date is not None else None,
        "evaluation_end_date": evaluation_end_date.date().isoformat() if evaluation_end_date is not None else None,
        "loaded_panel_rows": int(len(frame)),
        "parallel_workers": effective_parallel_workers,
        "parallel_validation_mode": "serial_shared_expression_cache",
        "validation_acceleration_mode": "precomputed_work_context" if fast_context is not None else "baseline",
        "cached_expression_count": len(expression_cache),
        "unique_validated_expression_count": len(validation_report_cache),
        "validation_report_cache_hit_count": validation_report_cache_hit_count,
        "unsupported_validation_cache_hit_count": unsupported_validation_cache_hit_count,
        "validation_report_cache_key_policy": "expanded_stripped_expression_plus_validation_contract",
        "real_edge_claim_allowed": False,
        "real_edge_claim_blockers": [
            "smoke_validation_only_not_purged_walk_forward_oos",
            "transaction_cost_slippage_capacity_not_applied",
            "factor_exposure_and_crowding_not_audited",
            "forward_shadow_validation_not_run",
        ],
        "evaluations": evaluations,
        "unsupported": unsupported,
    }


def build_validation_cost_report_from_ledger(
    ledger_path: Path | str,
    *,
    retained_only: bool = True,
    max_candidates: int | None = None,
) -> dict[str, Any]:
    ledger = json.loads(Path(ledger_path).read_text(encoding="utf-8"))
    records = list(ledger.get("records", []))
    if retained_only:
        records = [record for record in records if record.get("retained")]
    if max_candidates is not None:
        records = records[:max_candidates]

    candidates: list[dict[str, Any]] = []
    lane_counts: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    for record in records:
        cost_report = expression_validation_cost_report(str(record.get("expression", "")))
        lane = cost_report["validation_lane"]
        role = cost_report["validation_role"]
        lane_counts[lane] = lane_counts.get(lane, 0) + 1
        role_counts[role] = role_counts.get(role, 0) + 1
        candidates.append(
            {
                "candidate_id": str(record.get("candidate_id", "")),
                "retained": bool(record.get("retained")),
                "source_mode": record.get("source_mode"),
                "frontier_lane": record.get("frontier_lane"),
                "archive_cell": record.get("archive_cell"),
                **cost_report,
            }
        )

    candidates.sort(key=lambda item: (item["estimated_validation_cost_score"], item["relation_operator_count"], item["expression_length"]))
    return {
        "ledger_path": str(ledger_path),
        "source_run_id": ledger.get("run_id"),
        "retained_only": retained_only,
        "candidate_count": len(candidates),
        "lane_counts": lane_counts,
        "role_counts": role_counts,
        "recommended_execution_order": [
            "cheap_fast_path",
            "moderate_fast_path",
            "slow_relation_path",
            "very_slow_nested_relation_path",
        ],
        "cost_policy": {
            "fast_screen_before_full_history": True,
            "binary_output_role": "monitor_group_spread_not_fine_rank",
            "very_slow_nested_relation_path": "sample_or_shadow_only_until_vectorized",
        },
        "candidates": candidates,
    }


def _mean_numeric(values: list[Any]) -> float | None:
    numbers: list[float] = []
    for value in values:
        if value is None:
            continue
        number = float(value)
        if math.isfinite(number):
            numbers.append(number)
    if not numbers:
        return None
    return round(float(sum(numbers) / len(numbers)), 6)


def _field_mentions(expression: str) -> list[str]:
    return sorted(set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", expression)))


def build_real_replay_feedback_objective(report: Path | str | dict[str, Any]) -> dict[str, Any]:
    """Convert real-market replay into soft priors for the next search.

    This is deliberately not a promotion gate and not a formula-space lock. It
    only turns corrected real replay into auditable hints so Phase2 does not
    keep spending synthetic budget on motifs that fail under the A-share
    timestamp, T+1, and tradability contract.
    """

    payload = json.loads(Path(report).read_text(encoding="utf-8")) if not isinstance(report, dict) else report
    evaluations = list(payload.get("evaluations", []))
    supported = [item for item in evaluations if item.get("mean_window_rank_ic") is not None]
    passed = [item for item in evaluations if item.get("passes_real_market_smoke")]
    weak_positive = [
        item
        for item in supported
        if (float(item.get("mean_window_rank_ic") or 0.0) > 0.0 and not item.get("passes_real_market_smoke"))
    ]
    negative_or_zero = [item for item in supported if float(item.get("mean_window_rank_ic") or 0.0) <= 0.0]

    grouped: dict[str, dict[str, Any]] = {}

    def add_group(group: str, item: dict[str, Any]) -> None:
        current = grouped.setdefault(
            group,
            {
                "group": group,
                "count": 0,
                "passed_count": 0,
                "mean_rank_ic_values": [],
                "candidate_ids": [],
            },
        )
        current["count"] += 1
        current["passed_count"] += int(bool(item.get("passes_real_market_smoke")))
        current["mean_rank_ic_values"].append(item.get("mean_window_rank_ic"))
        current["candidate_ids"].append(str(item.get("candidate_id", "")))

    for item in evaluations:
        for key in ("frontier_lane", "validation_lane", "source_mode", "archive_cell"):
            add_group(f"{key}:{item.get(key) or 'unknown'}", item)
        for field in _field_mentions(str(item.get("expression", ""))):
            add_group(f"field:${field}", item)

    group_diagnostics: list[dict[str, Any]] = []
    for current in grouped.values():
        mean_ic = _mean_numeric(current["mean_rank_ic_values"])
        group_diagnostics.append(
            {
                "group": current["group"],
                "count": current["count"],
                "passed_count": current["passed_count"],
                "pass_rate": round(current["passed_count"] / max(1, current["count"]), 6),
                "mean_rank_ic": mean_ic,
                "candidate_ids": current["candidate_ids"][:8],
            }
        )
    group_diagnostics.sort(
        key=lambda item: (
            item["pass_rate"],
            item["mean_rank_ic"] if item["mean_rank_ic"] is not None else -999.0,
            -item["count"],
        )
    )

    flag_counts: dict[str, int] = {}
    for item in evaluations:
        for flag in item.get("smoke_flags") or []:
            flag_counts[str(flag)] = flag_counts.get(str(flag), 0) + 1

    weak_positive_candidates = sorted(
        weak_positive,
        key=lambda item: (
            float(item.get("mean_window_rank_ic") or -999.0),
            float(item.get("mean_window_sortino") or -999.0),
        ),
        reverse=True,
    )[:8]
    watched_groups = [
        item
        for item in sorted(
            group_diagnostics,
            key=lambda group: group["mean_rank_ic"] if group["mean_rank_ic"] is not None else -999.0,
            reverse=True,
        )
        if item["passed_count"] == 0 and (item["mean_rank_ic"] or 0.0) > 0.0
    ][:8]
    demoted_groups = [
        item
        for item in group_diagnostics
        if item["count"] >= 2
        and item["passed_count"] == 0
        and (item["mean_rank_ic"] is None or item["mean_rank_ic"] <= 0.0)
    ][:12]

    if passed:
        decision = "PROMOTE_REAL_REPLAY_PRIORS_TO_CANDIDATE_REVIEW"
        next_action = "run_cost_exposure_and_multi_window_review_for_passed_replay_candidates"
    elif weak_positive_candidates:
        decision = "USE_WEAK_REAL_REPLAY_PRIORS_FOR_NEXT_SEARCH"
        next_action = "bias_next_operator_family_toward_weak_positive_motifs_without_claiming_edge"
    else:
        decision = "REJECT_CURRENT_SYNTHETIC_MOTIFS_FOR_REAL_REPLAY"
        next_action = "change_math_operator_family_or_objective_before_more_synthetic_scale"

    return {
        "source_report": str(report) if not isinstance(report, dict) else payload.get("experiment_id"),
        "dataset_path": payload.get("dataset_path"),
        "screening_mode": payload.get("screening_mode"),
        "evaluation_start_date": payload.get("evaluation_start_date"),
        "evaluation_end_date": payload.get("evaluation_end_date"),
        "evaluated_count": len(evaluations),
        "supported_count": len(supported),
        "passed_smoke_count": len(passed),
        "weak_positive_count": len(weak_positive),
        "negative_or_zero_count": len(negative_or_zero),
        "mean_rank_ic_all_supported": _mean_numeric([item.get("mean_window_rank_ic") for item in supported]),
        "smoke_flag_counts": dict(sorted(flag_counts.items(), key=lambda item: (-item[1], item[0]))),
        "decision": decision,
        "next_action": next_action,
        "soft_prior_policy": "feedback_biases_search_routing_but_does_not_lock_or_prune_formula_space",
        "real_edge_claim_allowed": False,
        "saturated_positive_candidates": [
            {
                "candidate_id": item.get("candidate_id"),
                "expression": item.get("expression"),
                "mean_window_rank_ic": item.get("mean_window_rank_ic"),
                "mean_window_sortino": item.get("mean_window_sortino"),
                "frontier_lane": item.get("frontier_lane"),
                "archive_cell": item.get("archive_cell"),
            }
            for item in sorted(
                passed,
                key=lambda item: (
                    float(item.get("mean_window_rank_ic") or -999.0),
                    float(item.get("mean_window_sortino") or -999.0),
                ),
                reverse=True,
            )[:8]
        ],
        "weak_positive_candidates": [
            {
                "candidate_id": item.get("candidate_id"),
                "expression": item.get("expression"),
                "mean_window_rank_ic": item.get("mean_window_rank_ic"),
                "mean_window_sortino": item.get("mean_window_sortino"),
                "frontier_lane": item.get("frontier_lane"),
                "archive_cell": item.get("archive_cell"),
            }
            for item in weak_positive_candidates
        ],
        "watched_soft_prior_groups": watched_groups,
        "demoted_soft_prior_groups": demoted_groups,
        "search_objective_adjustment": {
            "increase_weight": [
                "real_replay_rank_ic",
                "real_replay_positive_window_ratio",
                "tradability_filtered_top_bottom_spread",
            ],
            "decrease_weight": [
                "synthetic_ic_without_real_replay_support",
                "coverage_only_growth_after_retained_yield_floor_break",
            ],
            "hard_blocks": [
                "lookahead_fields_at_signal_clock",
                "entry_limit_up_long_or_entry_limit_down_short",
            ],
        },
    }


def build_forward_shadow_watchlist(
    expression: str,
    *,
    candidate_id: str | None = None,
    path: Path | str = DEFAULT_REAL_MARKET_DATASET_PATH,
    lookback_days: int = DEFAULT_RECENT_LOOKBACK_DAYS,
    warmup_days: int = DEFAULT_RECENT_WARMUP_DAYS,
    top_bottom_quantile: float = 0.2,
    max_rows: int | None = None,
) -> dict[str, Any]:
    frame, evaluation_start, latest = _load_recent_market_panel(
        path,
        lookback_days=lookback_days,
        warmup_days=warmup_days,
        max_rows=max_rows,
    )
    cache: dict[str, pd.Series] = {}
    signal = evaluate_panel_expression(frame, expression, cache=cache)
    latest_frame = frame[["date", "code"]].copy()
    latest_frame["signal"] = signal
    latest_frame = latest_frame.dropna(subset=["signal"])
    latest_frame = latest_frame[latest_frame["date"] == latest_frame["date"].max()].copy()
    if latest_frame.empty:
        return {
            "candidate_id": candidate_id,
            "expression": expression,
            "dataset_path": str(path),
            "as_of_date": None,
            "status": "no_latest_signal",
            "real_edge_claim_allowed": False,
            "not_a_backtest": True,
            "monitoring_horizon": "next_3_months_forward_shadow",
            "top_watchlist": [],
            "bottom_watchlist": [],
        }

    latest_frame["signal_rank_pct"] = latest_frame["signal"].rank(pct=True)
    side_count = max(1, int(math.ceil(len(latest_frame) * top_bottom_quantile)))
    top = latest_frame.sort_values(["signal", "signal_rank_pct", "code"], ascending=[False, False, True]).head(side_count)
    bottom = latest_frame.sort_values(["signal", "signal_rank_pct", "code"], ascending=[True, True, True]).head(side_count)

    def serialize_rows(rows: pd.DataFrame) -> list[dict[str, Any]]:
        return [
            {
                "code": str(row.code),
                "signal": round(float(row.signal), 8),
                "signal_rank_pct": round(float(row.signal_rank_pct), 6),
            }
            for row in rows.itertuples(index=False)
        ]

    return {
        "candidate_id": candidate_id,
        "expression": expression,
        "dataset_path": str(path),
        "as_of_date": latest_frame["date"].max().date().isoformat(),
        "loaded_panel_rows": int(len(frame)),
        "evaluation_start_date": evaluation_start.date().isoformat(),
        "evaluation_end_date": latest.date().isoformat(),
        "status": "regime_local_forward_watchlist",
        "real_edge_claim_allowed": False,
        "not_a_backtest": True,
        "cannot_guarantee_next_3_months": True,
        "monitoring_horizon": "next_3_months_forward_shadow",
        "top_bottom_quantile": top_bottom_quantile,
        "side_count": side_count,
        "unique_signal_count": int(latest_frame["signal"].nunique(dropna=True)),
        "instrument_count": int(len(latest_frame)),
        "cached_expression_count": len(cache),
        "top_watchlist": serialize_rows(top),
        "bottom_watchlist": serialize_rows(bottom),
    }


def strict_audit_expression_on_real_market_panel(
    expression: str,
    *,
    candidate_id: str | None = None,
    path: Path | str = DEFAULT_REAL_MARKET_DATASET_PATH,
    horizons: tuple[int, ...] = DEFAULT_VALIDATION_HORIZON_DAYS,
    signal_clock: str = SIGNAL_CLOCK_AFTER_CLOSE,
    feature_lag_days: int = 0,
    top_bottom_quantile: float = 0.2,
    cost_bps: float = 10.0,
    max_rows: int | None = None,
    recent_quarter_window_count: int | None = None,
    recent_warmup_days: int = DEFAULT_RECENT_WARMUP_DAYS,
) -> dict[str, Any]:
    horizon_policy = VALIDATION_HORIZON_POLICY if tuple(horizons) == DEFAULT_VALIDATION_HORIZON_DAYS else "explicit_override"
    evaluation_start_date: pd.Timestamp | None = None
    evaluation_end_date: pd.Timestamp | None = None
    if recent_quarter_window_count is not None:
        frame, evaluation_start_date, evaluation_end_date = _load_recent_quarter_market_panel(
            path,
            quarter_window_count=recent_quarter_window_count,
            warmup_days=recent_warmup_days,
            max_rows=max_rows,
        )
    else:
        frame = _load_market_panel(path, max_rows=max_rows)
    cache: dict[str, pd.Series] = {}
    signal_frame, signal_clock_report = _signal_evaluation_frame(frame, signal_clock=signal_clock)
    signal = evaluate_panel_expression(
        signal_frame,
        expression,
        cache=cache,
        field_lags=signal_clock_report["field_lags"],
    )

    horizon_reports: list[dict[str, Any]] = []
    tradability_reference: dict[str, Any] | None = None
    for horizon in horizons:
        work, tradability_masks = _tradable_signal_work_frame(
            frame,
            signal,
            horizon_days=horizon,
            feature_lag_days=feature_lag_days,
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
            field_lags=signal_clock_report["field_lags"],
        )
        if tradability_reference is None:
            tradability_reference = _tradability_summary(work, tradability_masks)
        merged = _tradable_daily_ic_spread_turnover_frame(work, top_bottom_quantile=top_bottom_quantile)
        cost_per_turnover = float(cost_bps) / 10_000.0
        merged["cost_adjusted_long_short_return"] = merged["long_short_return"] - (
            merged["average_one_way_turnover"].fillna(0.0) * cost_per_turnover
        )
        windows: list[dict[str, Any]] = []
        for window, window_frame in merged.groupby("window", sort=True):
            ic_values = [float(value) for value in window_frame["rank_ic"].dropna()]
            spread = pd.to_numeric(window_frame["long_short_return"], errors="coerce")
            net_spread = pd.to_numeric(window_frame["cost_adjusted_long_short_return"], errors="coerce")
            turnover_values = [float(value) for value in window_frame["average_one_way_turnover"].dropna()]
            windows.append(
                {
                    "window": str(window),
                    "trading_day_count": int(len(window_frame)),
                    "mean_rank_ic": _mean_or_none(ic_values),
                    "rank_ic_hit_rate": round(float(np.mean([value > 0 for value in ic_values])), 6) if ic_values else None,
                    "mean_long_short_return": round(float(spread.mean()), 6) if not spread.dropna().empty else None,
                    "mean_cost_adjusted_long_short_return": round(float(net_spread.mean()), 6)
                    if not net_spread.dropna().empty
                    else None,
                    "long_short_sortino": _sortino(spread),
                    "cost_adjusted_sortino": _sortino(net_spread),
                    "mean_one_way_turnover": _mean_or_none(turnover_values),
                }
            )
        valid_ic = [item["mean_rank_ic"] for item in windows if item["mean_rank_ic"] is not None]
        valid_net = [item["mean_cost_adjusted_long_short_return"] for item in windows if item["mean_cost_adjusted_long_short_return"] is not None]
        valid_turnover = [item["mean_one_way_turnover"] for item in windows if item["mean_one_way_turnover"] is not None]
        horizon_reports.append(
            {
                "horizon_days": horizon,
                "row_count_after_signal_and_target": int(len(work)),
                "daily_observation_count": int(len(merged)),
                "window_count": len(windows),
                "mean_window_rank_ic": _mean_or_none(valid_ic),
                "positive_window_rank_ic_ratio": round(float(np.mean([value > 0 for value in valid_ic])), 6)
                if valid_ic
                else None,
                "mean_cost_adjusted_window_spread": _mean_or_none(valid_net),
                "mean_one_way_turnover": _mean_or_none(valid_turnover),
                "windows": windows,
            }
        )

    exposure_columns = ["amount", "volume", "close", "turnover_rate"]
    exposure_summary: dict[str, Any] = {}
    exposure_frame = frame[["date", "code", *[col for col in exposure_columns if col in frame.columns]]].copy()
    exposure_frame["signal"] = signal
    exposure_frame = exposure_frame.dropna(subset=["signal"])
    for column in exposure_columns:
        if column not in exposure_frame.columns:
            continue
        daily_corrs: list[float] = []
        for _date, day in exposure_frame.dropna(subset=[column]).groupby("date", sort=True):
            if len(day) < 5 or day["signal"].nunique(dropna=True) < 2 or day[column].nunique(dropna=True) < 2:
                continue
            corr = day["signal"].rank().corr(pd.to_numeric(day[column], errors="coerce").rank())
            if pd.notna(corr):
                daily_corrs.append(float(corr))
        exposure_summary[column] = {
            "mean_daily_rank_corr": _mean_or_none(daily_corrs),
            "abs_mean_daily_rank_corr": round(abs(float(np.mean(daily_corrs))), 6) if daily_corrs else None,
            "observation_day_count": len(daily_corrs),
        }

    blocker_flags: list[str] = []
    primary = horizon_reports[0] if horizon_reports else {}
    if primary.get("mean_window_rank_ic") is None or primary.get("mean_window_rank_ic", 0.0) < 0.01:
        blocker_flags.append("weak_primary_horizon_ic")
    if primary.get("mean_cost_adjusted_window_spread") is None or primary.get("mean_cost_adjusted_window_spread", 0.0) <= 0:
        blocker_flags.append("non_positive_cost_adjusted_primary_spread")
    amount_exposure = exposure_summary.get("amount", {}).get("abs_mean_daily_rank_corr")
    if amount_exposure is not None and amount_exposure > 0.8:
        blocker_flags.append("very_high_amount_self_exposure")
    blocker_flags.extend(
        [
            "sector_neutralization_not_run",
            "capacity_model_not_run",
            "survivorship_and_universe_policy_not_promotion_grade",
        ]
    )

    return {
        "candidate_id": candidate_id,
        "expression": expression,
        "dataset_path": str(path),
        "audit_type": "strict_real_market_smoke_audit",
        "real_edge_claim_allowed": False,
        "validation_period_months": REAL_MARKET_VALIDATION_PERIOD_MONTHS,
        "validation_period_policy": "quarterly_3_month_windows",
        "screening_mode": f"recent_{recent_quarter_window_count}_quarter_strict_audit"
        if recent_quarter_window_count is not None
        else "full_history_strict_audit",
        "recent_quarter_window_count": recent_quarter_window_count,
        "recent_warmup_days": recent_warmup_days if recent_quarter_window_count is not None else None,
        "evaluation_start_date": evaluation_start_date.date().isoformat() if evaluation_start_date is not None else None,
        "evaluation_end_date": evaluation_end_date.date().isoformat() if evaluation_end_date is not None else None,
        "operator_window_prior": list(WINDOW_PRIOR),
        "default_validation_horizon_days": list(DEFAULT_VALIDATION_HORIZON_DAYS),
        "horizon_days": list(horizons),
        "horizon_policy": horizon_policy,
        **signal_clock_report,
        "feature_lag_days": feature_lag_days,
        "feature_timestamp_policy": _feature_timestamp_policy(signal_clock, feature_lag_days),
        "top_bottom_quantile": top_bottom_quantile,
        "cost_bps": cost_bps,
        "loaded_panel_rows": int(len(frame)),
        "cached_expression_count": len(cache),
        "horizon_reports": horizon_reports,
        "turnover_reference_horizon_days": horizons[0] if horizons else None,
        "turnover_cost_shadow_tradability_filtered": True,
        **(tradability_reference or {}),
        "exposure_summary": exposure_summary,
        "blocker_flags": blocker_flags,
        "gatekeeper_decision": "HOLD_RESEARCH" if blocker_flags else "ALLOW_KEEP_REVIEW",
    }


def _summarize_tradable_signal_metrics(
    frame: pd.DataFrame,
    signal: pd.Series,
    *,
    horizon_days: int,
    execution_lag_days: int,
    feature_lag_days: int,
    evaluation_start_date: pd.Timestamp | None,
    evaluation_end_date: pd.Timestamp | None,
    top_bottom_quantile: float,
    field_lags: dict[str, int] | None = None,
) -> dict[str, Any]:
    work, tradability_masks = _tradable_signal_work_frame(
        frame,
        signal,
        horizon_days=horizon_days,
        execution_lag_days=execution_lag_days,
        feature_lag_days=feature_lag_days,
        evaluation_start_date=evaluation_start_date,
        evaluation_end_date=evaluation_end_date,
        field_lags=field_lags,
    )
    daily = _tradable_daily_ic_spread_turnover_frame(work, top_bottom_quantile=top_bottom_quantile)
    windows: list[dict[str, Any]] = []
    for window, window_frame in daily.groupby("window", sort=True):
        ic_values = [float(value) for value in window_frame["rank_ic"].dropna()]
        spread = pd.to_numeric(window_frame["long_short_return"], errors="coerce")
        turnover_values = [float(value) for value in window_frame["average_one_way_turnover"].dropna()]
        windows.append(
            {
                "window": str(window),
                "trading_day_count": int(len(window_frame)),
                "mean_rank_ic": _mean_or_none(ic_values),
                "rank_ic_hit_rate": round(float(np.mean([value > 0 for value in ic_values])), 6) if ic_values else None,
                "mean_long_short_return": round(float(spread.mean()), 6) if not spread.dropna().empty else None,
                "long_short_sortino": _sortino(spread),
                "mean_one_way_turnover": _mean_or_none(turnover_values),
            }
        )
    valid_ic = [item["mean_rank_ic"] for item in windows if item["mean_rank_ic"] is not None]
    valid_spread = [item["mean_long_short_return"] for item in windows if item["mean_long_short_return"] is not None]
    valid_turnover = [item["mean_one_way_turnover"] for item in windows if item["mean_one_way_turnover"] is not None]
    return {
        "horizon_days": horizon_days,
        "row_count_after_signal_and_target": int(len(work)),
        "daily_observation_count": int(len(daily)),
        "window_count": len(windows),
        "mean_window_rank_ic": _mean_or_none(valid_ic),
        "positive_window_rank_ic_ratio": round(float(np.mean([value > 0 for value in valid_ic])), 6)
        if valid_ic
        else None,
        "mean_window_long_short_return": _mean_or_none(valid_spread),
        "mean_one_way_turnover": _mean_or_none(valid_turnover),
        "windows": windows,
        **_tradability_summary(work, tradability_masks),
    }


def _daily_residualize_signal_against_controls(
    frame: pd.DataFrame,
    signal: pd.Series,
    controls: list[pd.Series],
) -> pd.Series:
    residual = signal.copy()
    for control in controls:
        residual = _cross_sectional_residual(frame, residual, control)
    return residual


def _daily_group_demean_signal(
    frame: pd.DataFrame,
    signal: pd.Series,
    *,
    group_column: str,
    min_group_size: int,
) -> pd.Series:
    group = frame[group_column].astype(str)
    work = pd.DataFrame({"date": frame["date"], "group": group, "signal": signal})
    counts = work.groupby(["date", "group"], sort=False)["signal"].transform("count")
    means = work.groupby(["date", "group"], sort=False)["signal"].transform("mean")
    demeaned = work["signal"] - means
    return demeaned.where(counts >= min_group_size)


def audit_expression_panel_exposure_neutrality(
    expression: str,
    *,
    candidate_id: str | None = None,
    path: Path | str = DEFAULT_REAL_MARKET_DATASET_PATH,
    horizon_days: int = DEFAULT_VALIDATION_HORIZON_DAYS[0],
    execution_lag_days: int = DEFAULT_EXECUTION_LAG_DAYS,
    signal_clock: str = SIGNAL_CLOCK_AFTER_OPEN,
    feature_lag_days: int = 0,
    top_bottom_quantile: float = 0.2,
    exposure_controls: tuple[str, ...] = ("amount", "volume", "turnover_rate", "crowding", "rps_score", "money_flow"),
    group_column: str = "sector",
    min_group_size: int = 3,
    recent_quarter_window_count: int | None = 4,
    recent_warmup_days: int = DEFAULT_RECENT_WARMUP_DAYS,
    max_rows: int | None = None,
) -> dict[str, Any]:
    evaluation_start_date: pd.Timestamp | None = None
    evaluation_end_date: pd.Timestamp | None = None
    if recent_quarter_window_count is not None:
        frame, evaluation_start_date, evaluation_end_date = _load_recent_quarter_market_panel(
            path,
            quarter_window_count=recent_quarter_window_count,
            warmup_days=recent_warmup_days,
            max_rows=max_rows,
        )
    else:
        frame = _load_market_panel(path, max_rows=max_rows)
    cache: dict[str, pd.Series] = {}
    signal_frame, signal_clock_report = _signal_evaluation_frame(frame, signal_clock=signal_clock)
    signal = evaluate_panel_expression(
        signal_frame,
        expression,
        cache=cache,
        field_lags=signal_clock_report["field_lags"],
    )
    raw_metrics = _summarize_tradable_signal_metrics(
        frame,
        signal,
        horizon_days=horizon_days,
        execution_lag_days=execution_lag_days,
        feature_lag_days=feature_lag_days,
        evaluation_start_date=evaluation_start_date,
        evaluation_end_date=evaluation_end_date,
        top_bottom_quantile=top_bottom_quantile,
        field_lags=signal_clock_report["field_lags"],
    )

    control_series: list[pd.Series] = []
    available_controls: list[str] = []
    control_exposure: dict[str, Any] = {}
    for control in exposure_controls:
        if control not in frame.columns:
            continue
        control_signal = evaluate_panel_expression(
            signal_frame,
            f"${control}",
            cache=cache,
            field_lags=signal_clock_report["field_lags"],
        )
        available_controls.append(control)
        control_series.append(control_signal)
        daily_corrs: list[float] = []
        probe = pd.DataFrame({"date": frame["date"], "signal": signal, "control": control_signal}).dropna()
        for _date, day in probe.groupby("date", sort=True):
            if len(day) < 5 or day["signal"].nunique(dropna=True) < 2 or day["control"].nunique(dropna=True) < 2:
                continue
            corr = day["signal"].rank().corr(day["control"].rank())
            if pd.notna(corr):
                daily_corrs.append(float(corr))
        control_exposure[control] = {
            "mean_daily_rank_corr": _mean_or_none(daily_corrs),
            "abs_mean_daily_rank_corr": round(abs(float(np.mean(daily_corrs))), 6) if daily_corrs else None,
            "observation_day_count": len(daily_corrs),
        }

    residualized_metrics: dict[str, Any] | None = None
    if control_series:
        residualized_signal = _daily_residualize_signal_against_controls(frame, signal, control_series)
        residualized_metrics = _summarize_tradable_signal_metrics(
            frame,
            residualized_signal,
            horizon_days=horizon_days,
            execution_lag_days=execution_lag_days,
            feature_lag_days=feature_lag_days,
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
            top_bottom_quantile=top_bottom_quantile,
            field_lags=signal_clock_report["field_lags"],
        )

    group_neutral_metrics: dict[str, Any] | None = None
    group_diagnostics: dict[str, Any] = {
        "group_column": group_column,
        "available": group_column in frame.columns,
        "neutralization_viable": False,
        "reason": "group_column_missing",
    }
    if group_column in frame.columns:
        group_counts = frame.groupby(group_column)["code"].nunique()
        code_group_counts = frame.groupby("code")[group_column].nunique()
        viable_group_count = int((group_counts >= min_group_size).sum())
        group_diagnostics = {
            "group_column": group_column,
            "available": True,
            "unique_groups": int(group_counts.shape[0]),
            "unique_codes": int(frame["code"].nunique()),
            "codes_per_group_min": int(group_counts.min()) if not group_counts.empty else 0,
            "codes_per_group_median": float(group_counts.median()) if not group_counts.empty else None,
            "codes_per_group_max": int(group_counts.max()) if not group_counts.empty else 0,
            "groups_meeting_min_size": viable_group_count,
            "sectors_per_code_median": float(code_group_counts.median()) if not code_group_counts.empty else None,
            "min_group_size": min_group_size,
            "neutralization_viable": viable_group_count > 0,
            "reason": "ok" if viable_group_count > 0 else "insufficient_codes_per_group",
        }
        if viable_group_count > 0:
            group_signal = _daily_group_demean_signal(
                frame,
                signal,
                group_column=group_column,
                min_group_size=min_group_size,
            )
            group_neutral_metrics = _summarize_tradable_signal_metrics(
                frame,
                group_signal,
                horizon_days=horizon_days,
                execution_lag_days=execution_lag_days,
                feature_lag_days=feature_lag_days,
                evaluation_start_date=evaluation_start_date,
                evaluation_end_date=evaluation_end_date,
                top_bottom_quantile=top_bottom_quantile,
                field_lags=signal_clock_report["field_lags"],
            )

    raw_ic = raw_metrics.get("mean_window_rank_ic")
    residual_ic = (residualized_metrics or {}).get("mean_window_rank_ic")
    residual_delta = None
    if raw_ic is not None and residual_ic is not None:
        residual_delta = round(float(residual_ic) - float(raw_ic), 6)
    blockers = []
    if not group_diagnostics.get("neutralization_viable"):
        blockers.append("true_group_neutralization_not_available_on_current_panel")
    if residual_delta is not None and residual_delta < -0.005:
        blockers.append("ic_materially_weakens_after_exposure_residualization")
    blockers.extend(
        [
            "stock_level_pit_industry_join_not_run",
            "capacity_model_not_run",
            "survivorship_and_universe_policy_not_promotion_grade",
        ]
    )
    return {
        "candidate_id": candidate_id,
        "expression": expression,
        "dataset_path": str(path),
        "audit_type": "panel_exposure_neutrality_probe",
        "real_edge_claim_allowed": False,
        "screening_mode": f"recent_{recent_quarter_window_count}_quarter_panel_exposure_probe"
        if recent_quarter_window_count is not None
        else "full_history_panel_exposure_probe",
        "evaluation_start_date": evaluation_start_date.date().isoformat() if evaluation_start_date is not None else None,
        "evaluation_end_date": evaluation_end_date.date().isoformat() if evaluation_end_date is not None else None,
        **signal_clock_report,
        "feature_lag_days": feature_lag_days,
        "feature_timestamp_policy": _feature_timestamp_policy(signal_clock, feature_lag_days),
        "horizon_days": horizon_days,
        "execution_lag_days": execution_lag_days,
        "top_bottom_quantile": top_bottom_quantile,
        "exposure_controls_requested": list(exposure_controls),
        "exposure_controls_available": available_controls,
        "control_exposure": control_exposure,
        "raw_metrics": raw_metrics,
        "exposure_residualized_metrics": residualized_metrics,
        "residualized_mean_ic_delta": residual_delta,
        "group_neutrality_diagnostics": group_diagnostics,
        "group_neutral_metrics": group_neutral_metrics,
        "blocker_flags": blockers,
        "gatekeeper_decision": "HOLD_RESEARCH",
    }
