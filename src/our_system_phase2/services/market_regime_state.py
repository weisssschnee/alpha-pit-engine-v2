from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


PIT_TREND_STATE_FEATURE_FIELDS = (
    "market_trend_eff",
    "market_trend_state",
    "market_breadth_state",
    "market_vol_state",
    "stock_trend_eff",
    "stock_trend_state",
    "stock_trend_slope",
    "stock_price_position_state",
)


@dataclass(frozen=True, slots=True)
class MarketRegimeThresholds:
    strong_uptrend_min_trend: float = 0.0015
    drawdown_max_trend: float = -0.0010
    high_vol_min_vol: float = 0.018
    liquidity_contraction_max_ratio: float = 0.75
    high_limit_density_min_ratio: float = 0.001


DEFAULT_MARKET_REGIME_THRESHOLDS = MarketRegimeThresholds()


@dataclass(frozen=True, slots=True)
class TrendStateFeatureConfig:
    market_trend_window: int = 20
    market_min_periods: int = 8
    stock_trend_window: int = 20
    stock_short_window: int = 5
    stock_min_periods: int = 8
    stock_price_position_window: int = 20
    market_uptrend_min: float = 0.0015
    market_downtrend_max: float = -0.0010
    stock_uptrend_min: float = 0.0015
    stock_downtrend_max: float = -0.0010


DEFAULT_TREND_STATE_FEATURE_CONFIG = TrendStateFeatureConfig()


def _threshold_state(values: pd.Series, *, upper: float, lower: float) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    state = pd.Series(0.0, index=numeric.index, dtype=float)
    state[numeric >= upper] = 1.0
    state[numeric <= lower] = -1.0
    state[numeric.isna()] = pd.NA
    return state


def trend_state_feature_contract() -> dict[str, Any]:
    return {
        "adapter": "pit_trend_state_features",
        "model_type": "deterministic_rolling_state_proxy",
        "uses_hmm": False,
        "uses_training": False,
        "feature_fields": list(PIT_TREND_STATE_FEATURE_FIELDS),
        "timestamp_policy": (
            "columns are computed from same-date completed daily bars only; "
            "after_open/pre_open validation must treat them as full-day fields and apply signal_clock lags"
        ),
        "provenance": {
            "market_trend_eff": "equal_weight_stock_return rolling mean by date",
            "market_trend_state": "thresholded market_trend_eff",
            "market_breadth_state": "rolling mean of daily positive-return stock ratio",
            "market_vol_state": "rolling std of equal_weight_stock_return",
            "stock_trend_eff": "per-stock rolling mean close-to-close return",
            "stock_trend_state": "thresholded stock_trend_eff",
            "stock_trend_slope": "per-stock short rolling return mean minus long rolling return mean",
            "stock_price_position_state": "per-stock close location inside rolling high-low range",
        },
    }


def attach_pit_trend_state_features(
    panel: pd.DataFrame,
    *,
    date_column: str = "date",
    code_column: str = "code",
    close_column: str = "close",
    high_column: str = "high",
    low_column: str = "low",
    config: TrendStateFeatureConfig = DEFAULT_TREND_STATE_FEATURE_CONFIG,
) -> pd.DataFrame:
    """Attach deterministic trend-state proxy columns without using future rows.

    The generated columns are full-day state values for row date ``t`` and use
    only data up to and including date ``t``. The real-market evaluator owns the
    trading-clock lag: under ``after_open`` these fields are shifted by one
    trading day before formula evaluation.
    """

    required = {date_column, code_column, close_column}
    missing = sorted(required.difference(panel.columns))
    if missing:
        raise ValueError(f"missing_required_columns:{missing}")

    out = panel.copy()
    if out.empty:
        for field in PIT_TREND_STATE_FEATURE_FIELDS:
            if field not in out.columns:
                out[field] = pd.Series(dtype=float)
        return out

    work = out[[date_column, code_column, close_column]].copy()
    if high_column in out.columns:
        work[high_column] = out[high_column]
    if low_column in out.columns:
        work[low_column] = out[low_column]
    work["_row_id"] = np.arange(len(work))
    work[date_column] = pd.to_datetime(work[date_column], errors="coerce")
    work[close_column] = pd.to_numeric(work[close_column], errors="coerce")
    if high_column in work.columns:
        work[high_column] = pd.to_numeric(work[high_column], errors="coerce")
    if low_column in work.columns:
        work[low_column] = pd.to_numeric(work[low_column], errors="coerce")
    work = work.dropna(subset=[date_column, code_column, close_column]).sort_values([code_column, date_column])
    grouped_close = work.groupby(code_column, sort=False)[close_column]
    work["_stock_ret"] = grouped_close.pct_change()

    stock_ret = work["_stock_ret"].groupby(work[code_column], sort=False)
    long_window = max(1, int(config.stock_trend_window))
    short_window = max(1, int(config.stock_short_window))
    long_min = max(1, min(int(config.stock_min_periods), long_window))
    short_min = max(1, min(int(config.stock_min_periods), short_window))
    work["stock_trend_eff"] = stock_ret.transform(lambda item: item.rolling(long_window, min_periods=long_min).mean())
    stock_short = stock_ret.transform(lambda item: item.rolling(short_window, min_periods=short_min).mean())
    work["stock_trend_slope"] = stock_short - work["stock_trend_eff"]
    work["stock_trend_state"] = _threshold_state(
        work["stock_trend_eff"],
        upper=config.stock_uptrend_min,
        lower=config.stock_downtrend_max,
    )

    if high_column in work.columns and low_column in work.columns:
        high = work[high_column].where(work[high_column].notna(), work[close_column])
        low = work[low_column].where(work[low_column].notna(), work[close_column])
        rolling_high = high.groupby(work[code_column], sort=False).transform(
            lambda item: item.rolling(config.stock_price_position_window, min_periods=long_min).max()
        )
        rolling_low = low.groupby(work[code_column], sort=False).transform(
            lambda item: item.rolling(config.stock_price_position_window, min_periods=long_min).min()
        )
        denominator = (rolling_high - rolling_low).replace(0, pd.NA)
        work["stock_price_position_state"] = (work[close_column] - rolling_low) / denominator
    else:
        work["stock_price_position_state"] = pd.NA

    daily = (
        work.groupby(date_column, sort=True)
        .agg(
            ew_return=("_stock_ret", "mean"),
            up_ratio=("_stock_ret", lambda item: float((pd.to_numeric(item, errors="coerce") > 0).mean())),
        )
        .reset_index()
    )
    market_window = max(1, int(config.market_trend_window))
    market_min = max(1, min(int(config.market_min_periods), market_window))
    daily["market_trend_eff"] = daily["ew_return"].rolling(market_window, min_periods=market_min).mean()
    daily["market_trend_state"] = _threshold_state(
        daily["market_trend_eff"],
        upper=config.market_uptrend_min,
        lower=config.market_downtrend_max,
    )
    daily["market_breadth_state"] = daily["up_ratio"].rolling(market_window, min_periods=market_min).mean()
    daily["market_vol_state"] = daily["ew_return"].rolling(market_window, min_periods=market_min).std(ddof=0)
    market_features = daily[
        [date_column, "market_trend_eff", "market_trend_state", "market_breadth_state", "market_vol_state"]
    ]
    work = work.merge(market_features, on=date_column, how="left")

    by_row = work.set_index("_row_id")
    for field in PIT_TREND_STATE_FEATURE_FIELDS:
        if field in by_row.columns:
            out[field] = pd.to_numeric(by_row[field].reindex(np.arange(len(out))), errors="coerce").to_numpy()
        elif field not in out.columns:
            out[field] = pd.NA
    return out


def _mean_positive_ratio(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return None
    return float((clean > 0).mean())


def _limit_ratio(values: pd.Series, *, direction: str) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return None
    if direction == "up":
        return float((clean >= 9.8).mean())
    if direction == "down":
        return float((clean <= -9.8).mean())
    raise ValueError(f"unsupported_limit_direction:{direction}")


def _label_regime(row: pd.Series, thresholds: MarketRegimeThresholds) -> str:
    trend = row.get("trend_mean_lag1")
    volatility = row.get("volatility_lag1")
    liquidity_ratio = row.get("liquidity_ratio_lag1")
    limit_density = row.get("limit_density_lag1")
    if pd.isna(trend) or pd.isna(volatility):
        return "unknown_warmup"
    if pd.notna(limit_density) and float(limit_density) >= thresholds.high_limit_density_min_ratio:
        return "limit_density_high"
    if pd.notna(liquidity_ratio) and float(liquidity_ratio) < thresholds.liquidity_contraction_max_ratio:
        return "liquidity_contraction"
    if float(trend) >= thresholds.strong_uptrend_min_trend:
        return "strong_uptrend"
    if float(trend) <= thresholds.drawdown_max_trend:
        return "drawdown"
    if float(volatility) >= thresholds.high_vol_min_vol:
        return "high_vol_sideways"
    return "sideways_or_rotation"


def build_pit_market_regime_state_frame(
    panel: pd.DataFrame,
    *,
    date_column: str = "date",
    code_column: str = "code",
    close_column: str = "close",
    amount_column: str = "amount",
    limit_change_column: str = "rt_change_pct",
    trend_window: int = 20,
    trend_min_periods: int = 8,
    liquidity_short_window: int = 20,
    liquidity_long_window: int = 60,
    liquidity_min_periods: int = 20,
    limit_density_window: int = 5,
    limit_density_min_periods: int = 2,
    thresholds: MarketRegimeThresholds = DEFAULT_MARKET_REGIME_THRESHOLDS,
) -> pd.DataFrame:
    """Build PIT-ish market-state labels from lagged panel aggregates.

    The label assigned to date ``t`` uses only rolling aggregates shifted by one
    trading day. This is intentionally a deterministic state proxy, not a
    trained hidden Markov model.
    """

    required = {date_column, code_column, close_column}
    missing = sorted(required.difference(panel.columns))
    if missing:
        raise ValueError(f"missing_required_columns:{missing}")

    work_columns = [date_column, code_column, close_column]
    for optional in (amount_column, limit_change_column):
        if optional in panel.columns:
            work_columns.append(optional)
    work = panel[work_columns].copy()
    work[date_column] = pd.to_datetime(work[date_column], errors="coerce")
    work[close_column] = pd.to_numeric(work[close_column], errors="coerce")
    if amount_column in work.columns:
        work[amount_column] = pd.to_numeric(work[amount_column], errors="coerce")
    if limit_change_column in work.columns:
        work[limit_change_column] = pd.to_numeric(work[limit_change_column], errors="coerce")
    work = work.dropna(subset=[date_column, code_column, close_column]).sort_values([code_column, date_column])
    work["panel_return"] = work.groupby(code_column, sort=False)[close_column].pct_change()

    aggregations: dict[str, Any] = {
        "ew_return": ("panel_return", "mean"),
        "up_ratio": ("panel_return", _mean_positive_ratio),
        "instrument_count": (code_column, "nunique"),
    }
    if amount_column in work.columns:
        aggregations["amount_sum"] = (amount_column, "sum")
    if limit_change_column in work.columns:
        aggregations["limit_up_ratio"] = (limit_change_column, lambda values: _limit_ratio(values, direction="up"))
        aggregations["limit_down_ratio"] = (limit_change_column, lambda values: _limit_ratio(values, direction="down"))

    daily = work.groupby(date_column, sort=True).agg(**aggregations).reset_index()
    daily = daily.rename(columns={date_column: "date"})
    daily["trend_mean_lag1"] = daily["ew_return"].rolling(trend_window, min_periods=trend_min_periods).mean().shift(1)
    daily["volatility_lag1"] = daily["ew_return"].rolling(trend_window, min_periods=trend_min_periods).std(ddof=0).shift(1)

    if "amount_sum" in daily.columns:
        short_liquidity = daily["amount_sum"].rolling(liquidity_short_window, min_periods=trend_min_periods).mean().shift(1)
        long_liquidity = daily["amount_sum"].rolling(liquidity_long_window, min_periods=liquidity_min_periods).mean().shift(1)
        daily["liquidity_ratio_lag1"] = short_liquidity / long_liquidity
    else:
        daily["liquidity_ratio_lag1"] = pd.NA

    if {"limit_up_ratio", "limit_down_ratio"}.issubset(daily.columns):
        limit_density = daily["limit_up_ratio"].fillna(0.0) + daily["limit_down_ratio"].fillna(0.0)
        daily["limit_density_lag1"] = (
            limit_density.rolling(limit_density_window, min_periods=limit_density_min_periods).mean().shift(1)
        )
    else:
        daily["limit_density_lag1"] = pd.NA

    daily["pit_regime_label"] = daily.apply(lambda row: _label_regime(row, thresholds), axis=1)
    return daily


def summarize_regime_coverage(regime_frame: pd.DataFrame) -> list[dict[str, Any]]:
    required = {"pit_regime_label", "ew_return", "up_ratio"}
    missing = sorted(required.difference(regime_frame.columns))
    if missing:
        raise ValueError(f"missing_required_columns:{missing}")
    rows: list[dict[str, Any]] = []
    for label, group in regime_frame.groupby("pit_regime_label", sort=True):
        rows.append(
            {
                "pit_regime_label": str(label),
                "days": int(len(group)),
                "mean_ew_return": round(float(pd.to_numeric(group["ew_return"], errors="coerce").mean()), 6),
                "mean_up_ratio": round(float(pd.to_numeric(group["up_ratio"], errors="coerce").mean()), 6),
                "mean_liquidity_ratio": round(
                    float(pd.to_numeric(group.get("liquidity_ratio_lag1"), errors="coerce").mean()), 6
                )
                if "liquidity_ratio_lag1" in group
                else None,
                "mean_limit_density": round(
                    float(pd.to_numeric(group.get("limit_density_lag1"), errors="coerce").mean()), 6
                )
                if "limit_density_lag1" in group
                else None,
            }
        )
    return rows
