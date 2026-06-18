from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_LIMIT_UP_PCT = 0.098
DEFAULT_LIMIT_DOWN_PCT = -0.098
DEFAULT_MAX_STREAK_N = 10
DEFAULT_POST_HIGH_BOARD_DAYS = (1, 2, 3, 5)


@dataclass(frozen=True, slots=True)
class EventDerivedFeatureSpec:
    field_name: str
    family: str
    source_fields: tuple[str, ...]
    lag_rule: str
    availability_clock: str
    tradability_rule: str
    leakage_flag: str
    description: str


BASE_EVENT_DERIVED_FEATURE_SPECS: dict[str, EventDerivedFeatureSpec] = {
    "limit_up_close_event": EventDerivedFeatureSpec(
        "limit_up_close_event",
        "limit_close",
        ("tdxgp_limit_status", "is_limit_up", "rt_change_pct", "close"),
        "same_day_close_lock; after_open must lag by one session",
        "after_close",
        "entry_buy_blocked_if_next_session_limit_up",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Close-locked limit-up event.",
    ),
    "limit_down_close_event": EventDerivedFeatureSpec(
        "limit_down_close_event",
        "limit_close",
        ("tdxgp_limit_status", "is_limit_down", "rt_change_pct", "close"),
        "same_day_close_lock; after_open must lag by one session",
        "after_close",
        "entry_sell_blocked_if_next_session_limit_down",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Close-locked limit-down event.",
    ),
    "limit_up_open_event": EventDerivedFeatureSpec(
        "limit_up_open_event",
        "limit_open",
        ("open", "prev_close", "up_limit_price"),
        "same_day_open_print; pre_open must lag by one session",
        "after_open",
        "buy_at_open_may_be_unfilled_when_open_locked_limit_up",
        "safe_after_open_only_if_open_print_is_available",
        "Open printed at or above the limit-up proxy/price.",
    ),
    "limit_down_open_event": EventDerivedFeatureSpec(
        "limit_down_open_event",
        "limit_open",
        ("open", "prev_close", "down_limit_price"),
        "same_day_open_print; pre_open must lag by one session",
        "after_open",
        "sell_at_open_may_be_unfilled_when_open_locked_limit_down",
        "safe_after_open_only_if_open_print_is_available",
        "Open printed at or below the limit-down proxy/price.",
    ),
    "limit_up_touch_event": EventDerivedFeatureSpec(
        "limit_up_touch_event",
        "limit_touch",
        ("high", "prev_close", "up_limit_price", "tdxgp_limit_status"),
        "same_day_intraday_high; after_open must lag by one session",
        "after_close",
        "intraday_touch_not_assumed_fillable",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Intraday touched limit-up using high/limit proxy or TDXGP touch state.",
    ),
    "limit_down_touch_event": EventDerivedFeatureSpec(
        "limit_down_touch_event",
        "limit_touch",
        ("low", "prev_close", "down_limit_price", "tdxgp_limit_status"),
        "same_day_intraday_low; after_open must lag by one session",
        "after_close",
        "intraday_touch_not_assumed_fillable",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Intraday touched limit-down using low/limit proxy or TDXGP touch state.",
    ),
    "limit_up_touch_not_close": EventDerivedFeatureSpec(
        "limit_up_touch_not_close",
        "limit_break",
        ("limit_up_touch_event", "limit_up_close_event"),
        "derived_from_same_day_touch_and_close; after_open must lag by one session",
        "after_close",
        "touch_then_break_not_assumed_fillable",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Touched limit-up but did not close locked limit-up.",
    ),
    "limit_down_touch_not_close": EventDerivedFeatureSpec(
        "limit_down_touch_not_close",
        "limit_break",
        ("limit_down_touch_event", "limit_down_close_event"),
        "derived_from_same_day_touch_and_close; after_open must lag by one session",
        "after_close",
        "touch_then_repair_not_assumed_fillable",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Touched limit-down but did not close locked limit-down.",
    ),
    "limit_up_open_not_close": EventDerivedFeatureSpec(
        "limit_up_open_not_close",
        "limit_open_break",
        ("limit_up_open_event", "limit_up_close_event"),
        "same_day_open_known_after_open; close component must lag for after_open signals",
        "after_close",
        "open_locked_then_break_not_assumed_fillable",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Opened limit-up but did not close locked limit-up.",
    ),
    "limit_down_open_not_close": EventDerivedFeatureSpec(
        "limit_down_open_not_close",
        "limit_open_break",
        ("limit_down_open_event", "limit_down_close_event"),
        "same_day_open_known_after_open; close component must lag for after_open signals",
        "after_close",
        "open_locked_then_repair_not_assumed_fillable",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Opened limit-down but did not close locked limit-down.",
    ),
    "limit_up_close_not_open": EventDerivedFeatureSpec(
        "limit_up_close_not_open",
        "limit_close_without_open",
        ("limit_up_close_event", "limit_up_open_event"),
        "same_day_close_lock; after_open must lag by one session",
        "after_close",
        "close_locked_entry_next_session_may_be_unfilled",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Closed limit-up but was not locked limit-up at open.",
    ),
    "limit_down_close_not_open": EventDerivedFeatureSpec(
        "limit_down_close_not_open",
        "limit_close_without_open",
        ("limit_down_close_event", "limit_down_open_event"),
        "same_day_close_lock; after_open must lag by one session",
        "after_close",
        "close_locked_exit_next_session_may_be_unfilled",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Closed limit-down but was not locked limit-down at open.",
    ),
    "reason_record_exists": EventDerivedFeatureSpec(
        "reason_record_exists",
        "limit_reason_record",
        ("review_uplimit_reason.reason",),
        "vendor event record; daily use must lag by one session",
        "after_close",
        "reason_record_is_not_entry_fillability",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Vendor limit-reason record exists for the stock-date.",
    ),
    "open_board_record": EventDerivedFeatureSpec(
        "open_board_record",
        "open_board_record",
        ("review_uplimit_reason_open.reason",),
        "vendor event record; daily use must lag by one session",
        "after_close",
        "open_board_record_is_not_entry_fillability",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Vendor open-board record exists for the stock-date.",
    ),
    "close_limit_without_reason_record": EventDerivedFeatureSpec(
        "close_limit_without_reason_record",
        "limit_data_quality",
        ("limit_up_close_event", "reason_record_exists"),
        "data-quality diagnostic; must not drive promotion without manual review",
        "after_close",
        "data_quality_diagnostic_not_trade_signal",
        "diagnostic_only_until_source_mismatch_review",
        "Close-limit event without matching vendor reason record.",
    ),
    "reason_record_without_close_limit": EventDerivedFeatureSpec(
        "reason_record_without_close_limit",
        "limit_data_quality",
        ("reason_record_exists", "limit_up_close_event"),
        "data-quality diagnostic; must not drive promotion without manual review",
        "after_close",
        "data_quality_diagnostic_not_trade_signal",
        "diagnostic_only_until_source_mismatch_review",
        "Vendor reason record exists without close-limit event in daily panel.",
    ),
    "limit_up_keep_times_vendor": EventDerivedFeatureSpec(
        "limit_up_keep_times_vendor",
        "limit_streak_vendor",
        ("review_uplimit_reason.up_limit_keep_times",),
        "vendor event field; daily use must lag by one session",
        "after_close",
        "vendor_streak_may_indicate_unfillable_limit_queue",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Vendor-reported limit-up keep/streak count.",
    ),
    "seal_money": EventDerivedFeatureSpec(
        "seal_money",
        "limit_seal_flow",
        ("review_uplimit_reason.fengdan_money",),
        "vendor event field; daily use must lag by one session",
        "after_close",
        "seal_size_is_not_fillability",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Limit-up seal/order money proxy.",
    ),
    "seal_rate": EventDerivedFeatureSpec(
        "seal_rate",
        "limit_seal_flow",
        ("review_uplimit_reason.fengdan_rate",),
        "vendor event field; daily use must lag by one session",
        "after_close",
        "seal_rate_is_not_fillability",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Limit-up seal/order rate proxy.",
    ),
    "seal_circulation_rate": EventDerivedFeatureSpec(
        "seal_circulation_rate",
        "limit_seal_flow",
        ("review_uplimit_reason.feng_circulation_rate",),
        "vendor event field; daily use must lag by one session",
        "after_close",
        "seal_circulation_rate_is_not_fillability",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Limit-up seal amount relative to circulation proxy.",
    ),
    "actual_circulation_value": EventDerivedFeatureSpec(
        "actual_circulation_value",
        "event_capacity",
        ("review_uplimit_reason.actualcirculation_value",),
        "vendor event field; daily use must lag by one session",
        "after_close",
        "capacity_proxy_not_alpha_by_itself",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Actual circulation value proxy attached to limit event.",
    ),
    "turnover_ratio_real": EventDerivedFeatureSpec(
        "turnover_ratio_real",
        "event_liquidity",
        ("review_uplimit_reason.turnover_ration_real",),
        "vendor event field; daily use must lag by one session",
        "after_close",
        "liquidity_proxy_not_alpha_by_itself",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Real turnover ratio proxy attached to limit event.",
    ),
    "plate_score": EventDerivedFeatureSpec(
        "plate_score",
        "theme_plate",
        ("review_uplimit_reason.plate_score",),
        "vendor event/theme field; daily use must lag by one session",
        "after_close",
        "theme_score_not_entry_fillability",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Limit-event theme/plate score.",
    ),
    "high_board_rank": EventDerivedFeatureSpec(
        "high_board_rank",
        "high_board",
        ("limit_up_streak_close",),
        "same_day_close_streak; after_open must lag by one session",
        "after_close",
        "not_a_tradeability_filter",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Cross-sectional dense rank of close-lock limit-up streak, descending.",
    ),
    "market_high_board": EventDerivedFeatureSpec(
        "market_high_board",
        "high_board",
        ("limit_up_streak_close",),
        "same_day_market_max_streak; after_open must lag by one session",
        "after_close",
        "market_state_only",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Market-wide maximum close-lock limit-up streak for the date.",
    ),
    "is_market_high_board": EventDerivedFeatureSpec(
        "is_market_high_board",
        "high_board",
        ("limit_up_streak_close", "market_high_board"),
        "same_day_close_streak; after_open must lag by one session",
        "after_close",
        "high_board_member_may_be_untradeable",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Stock is tied for the market high-board streak on the date.",
    ),
    "streak_gap_to_market_high": EventDerivedFeatureSpec(
        "streak_gap_to_market_high",
        "high_board",
        ("limit_up_streak_close", "market_high_board"),
        "same_day_close_streak; after_open must lag by one session",
        "after_close",
        "not_a_tradeability_filter",
        "unsafe_same_day_for_after_open_without_field_lag",
        "Market high-board streak minus stock close-lock streak.",
    ),
}


EVENT_DERIVED_OPEN_PRINT_FIELDS = frozenset(
    {
        "limit_up_open_event",
        "limit_down_open_event",
    }
)


def _parametric_feature_names(max_streak_n: int = DEFAULT_MAX_STREAK_N) -> set[str]:
    names: set[str] = {
        "limit_up_event",
        "limit_down_event",
        "limit_up_streak",
        "limit_down_streak",
        "limit_up_streak_close",
        "limit_down_streak_close",
        "limit_up_streak_touch",
        "limit_down_streak_touch",
        "limit_up_break",
        "limit_down_repair",
        "limit_flip_up_to_down",
        "limit_flip_down_to_up",
    }
    for n in range(1, int(max_streak_n) + 1):
        names.update(
            {
                f"limit_up_streak_ge_{n}",
                f"limit_down_streak_ge_{n}",
                f"limit_up_touch_streak_ge_{n}",
                f"limit_down_touch_streak_ge_{n}",
                f"break_board_after_streak_ge_{n}",
                f"limit_down_rebound_after_streak_ge_{n}",
            }
        )
    for n in range(2, int(max_streak_n) + 1):
        names.update(
            {
                f"limit_up_close_count_t{n}",
                f"limit_up_open_count_t{n}",
                f"limit_up_touch_count_t{n}",
                f"limit_up_open_not_close_count_t{n}",
                f"limit_up_touch_not_close_count_t{n}",
                f"limit_up_close_not_open_count_t{n}",
                f"limit_up_any_open_not_close_in_t{n}",
                f"limit_up_any_close_not_open_in_t{n}",
            }
        )
    for days in DEFAULT_POST_HIGH_BOARD_DAYS:
        names.update(
            {
                f"post_market_high_board_tplus_{days}",
                f"break_after_high_board_tplus_{days}",
            }
        )
    return names


EVENT_DERIVED_FEATURE_FIELDS = frozenset(
    set(BASE_EVENT_DERIVED_FEATURE_SPECS)
    | _parametric_feature_names(DEFAULT_MAX_STREAK_N)
)
EVENT_DERIVED_FULL_DAY_FIELDS = frozenset(EVENT_DERIVED_FEATURE_FIELDS - EVENT_DERIVED_OPEN_PRINT_FIELDS)


def event_feature_spec(field_name: str) -> EventDerivedFeatureSpec | None:
    name = field_name.lower().lstrip("$")
    if name in BASE_EVENT_DERIVED_FEATURE_SPECS:
        return BASE_EVENT_DERIVED_FEATURE_SPECS[name]
    if name in EVENT_DERIVED_FEATURE_FIELDS:
        return EventDerivedFeatureSpec(
            field_name=name,
            family=_infer_event_family(name),
            source_fields=_infer_source_fields(name),
            lag_rule="derived_event_field; see family source fields",
            availability_clock="after_open" if name in EVENT_DERIVED_OPEN_PRINT_FIELDS else "after_close",
            tradability_rule=_infer_tradability_rule(name),
            leakage_flag=(
                "safe_after_open_only_if_open_print_is_available"
                if name in EVENT_DERIVED_OPEN_PRINT_FIELDS
                else "unsafe_same_day_for_after_open_without_field_lag"
            ),
            description=f"Parametric event-derived feature: {name}",
        )
    return None


def canonical_event_feature_name(field_name: str) -> str | None:
    spec = event_feature_spec(field_name)
    return spec.field_name if spec else None


def event_feature_type(field_name: str) -> str | None:
    spec = event_feature_spec(field_name)
    if spec is None:
        return None
    if spec.family in {"high_board", "limit_density"}:
        return "event_crosssec"
    return "event_ts"


def event_feature_behavior_profile(field_name: str) -> dict[str, float] | None:
    spec = event_feature_spec(field_name)
    if spec is None:
        return None
    family = spec.family
    if family in {"limit_close", "limit_touch", "limit_open", "limit_reason_record"}:
        return {"momentum": 0.78, "size": 0.05, "value": 0.05, "volatility": 0.78, "turnover": 0.38}
    if family in {"limit_break", "limit_open_break", "limit_close_without_open", "open_board_record"}:
        return {"momentum": 0.42, "size": 0.05, "value": 0.18, "volatility": 0.92, "turnover": 0.50}
    if family in {"limit_streak", "limit_streak_vendor"}:
        return {"momentum": 0.86, "size": 0.05, "value": 0.04, "volatility": 0.82, "turnover": 0.30}
    if family == "limit_seal_flow":
        return {"momentum": 0.68, "size": 0.55, "value": 0.04, "volatility": 0.80, "turnover": 0.50}
    if family == "event_capacity":
        return {"momentum": 0.08, "size": 0.92, "value": 0.25, "volatility": 0.15, "turnover": 0.25}
    if family == "event_liquidity":
        return {"momentum": 0.25, "size": 0.65, "value": 0.05, "volatility": 0.55, "turnover": 0.85}
    if family == "theme_plate":
        return {"momentum": 0.62, "size": 0.15, "value": 0.04, "volatility": 0.65, "turnover": 0.42}
    if family == "limit_data_quality":
        return {"momentum": 0.05, "size": 0.05, "value": 0.05, "volatility": 0.40, "turnover": 0.05}
    if family == "high_board":
        return {"momentum": 0.88, "size": 0.08, "value": 0.02, "volatility": 0.86, "turnover": 0.34}
    if family == "limit_down_repair":
        return {"momentum": 0.55, "size": 0.05, "value": 0.45, "volatility": 0.90, "turnover": 0.42}
    return {"momentum": 0.55, "size": 0.05, "value": 0.12, "volatility": 0.80, "turnover": 0.40}


def event_derived_feature_contract(max_streak_n: int = DEFAULT_MAX_STREAK_N) -> dict[str, Any]:
    fields = sorted(set(BASE_EVENT_DERIVED_FEATURE_SPECS) | _parametric_feature_names(max_streak_n))
    return {
        "adapter": "event_derived_feature_layer",
        "version": "event-derived-feature-layer-v1-2026-05-28",
        "status": "feature_adapter_diagnostic_ready",
        "fields": fields,
        "open_print_fields": sorted(EVENT_DERIVED_OPEN_PRINT_FIELDS),
        "full_day_fields": sorted(set(fields) - set(EVENT_DERIVED_OPEN_PRINT_FIELDS)),
        "default_max_streak_n": int(max_streak_n),
        "default_post_high_board_days": list(DEFAULT_POST_HIGH_BOARD_DAYS),
        "strict_rules": {
            "same_day_close_or_touch_fields": "must_be_lagged_for_after_open_selection",
            "open_fields": "available_after_open_but_not_pre_open",
            "tradability": "open/close locked limit-up/down may be unfillable; use entry limit masks in replay",
            "promotion": "diagnostic until strict replay, global clustering, OOS/regime/marginal audit, and chain-lock update",
        },
        "specs": {
            name: {
                "family": spec.family,
                "source_fields": list(spec.source_fields),
                "lag_rule": spec.lag_rule,
                "availability_clock": spec.availability_clock,
                "tradability_rule": spec.tradability_rule,
                "leakage_flag": spec.leakage_flag,
                "description": spec.description,
            }
            for name, spec in sorted(BASE_EVENT_DERIVED_FEATURE_SPECS.items())
        },
    }


def attach_event_derived_features(
    panel: pd.DataFrame,
    *,
    max_streak_n: int = DEFAULT_MAX_STREAK_N,
    up_limit_pct: float = DEFAULT_LIMIT_UP_PCT,
    down_limit_pct: float = DEFAULT_LIMIT_DOWN_PCT,
    tolerance: float = 1e-6,
) -> pd.DataFrame:
    required = {"date", "code", "open", "high", "low", "close"}
    missing = sorted(required.difference(panel.columns))
    if missing:
        raise ValueError(f"missing_event_feature_columns:{missing}")

    frame = panel.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["code"] = frame["code"].astype(str)
    frame = frame.sort_values(["code", "date"]).reset_index(drop=True)

    for column in ("open", "high", "low", "close", "rt_change_pct", "tdxgp_limit_status"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    grouped = frame.groupby("code", sort=False)
    prev_close = grouped["close"].shift(1)
    frame["event_prev_close"] = prev_close

    close_up = _close_limit_mask(frame, "up", up_limit_pct=up_limit_pct, down_limit_pct=down_limit_pct)
    close_down = _close_limit_mask(frame, "down", up_limit_pct=up_limit_pct, down_limit_pct=down_limit_pct)
    open_up = _open_touch_mask(frame, "open", "up", up_limit_pct=up_limit_pct, down_limit_pct=down_limit_pct, tolerance=tolerance)
    open_down = _open_touch_mask(frame, "open", "down", up_limit_pct=up_limit_pct, down_limit_pct=down_limit_pct, tolerance=tolerance)
    touch_up = _open_touch_mask(frame, "high", "up", up_limit_pct=up_limit_pct, down_limit_pct=down_limit_pct, tolerance=tolerance)
    touch_down = _open_touch_mask(frame, "low", "down", up_limit_pct=up_limit_pct, down_limit_pct=down_limit_pct, tolerance=tolerance)
    if "tdxgp_limit_status" in frame.columns:
        status = pd.to_numeric(frame["tdxgp_limit_status"], errors="coerce")
        touch_up = touch_up | status.ge(1.0).fillna(False)
        touch_down = touch_down | status.le(-1.0).fillna(False)

    frame["limit_up_close_event"] = close_up.astype(float)
    frame["limit_down_close_event"] = close_down.astype(float)
    frame["limit_up_event"] = frame["limit_up_close_event"]
    frame["limit_down_event"] = frame["limit_down_close_event"]
    frame["limit_up_open_event"] = open_up.astype(float)
    frame["limit_down_open_event"] = open_down.astype(float)
    frame["limit_up_touch_event"] = touch_up.astype(float)
    frame["limit_down_touch_event"] = touch_down.astype(float)
    frame["limit_up_touch_not_close"] = (touch_up & ~close_up).astype(float)
    frame["limit_down_touch_not_close"] = (touch_down & ~close_down).astype(float)
    frame["limit_up_open_not_close"] = (open_up & ~close_up).astype(float)
    frame["limit_down_open_not_close"] = (open_down & ~close_down).astype(float)
    frame["limit_up_close_not_open"] = (close_up & ~open_up).astype(float)
    frame["limit_down_close_not_open"] = (close_down & ~open_down).astype(float)

    up_close_count = _streak_count(frame, close_up)
    down_close_count = _streak_count(frame, close_down)
    up_touch_count = _streak_count(frame, touch_up)
    down_touch_count = _streak_count(frame, touch_down)
    frame["limit_up_streak_close"] = up_close_count
    frame["limit_down_streak_close"] = down_close_count
    frame["limit_up_streak"] = up_close_count
    frame["limit_down_streak"] = down_close_count
    frame["limit_up_streak_touch"] = up_touch_count
    frame["limit_down_streak_touch"] = down_touch_count

    prev_up_count = up_close_count.groupby(frame["code"], sort=False).shift(1).fillna(0.0)
    prev_down_count = down_close_count.groupby(frame["code"], sort=False).shift(1).fillna(0.0)
    current_up = close_up.fillna(False)
    current_down = close_down.fillna(False)
    frame["limit_up_break"] = ((prev_up_count > 0.0) & ~current_up).astype(float)
    frame["limit_down_repair"] = ((prev_down_count > 0.0) & ~current_down).astype(float)
    frame["limit_flip_up_to_down"] = ((prev_up_count > 0.0) & current_down).astype(float)
    frame["limit_flip_down_to_up"] = ((prev_down_count > 0.0) & current_up).astype(float)

    generated_columns: dict[str, pd.Series] = {}
    for n in range(1, int(max_streak_n) + 1):
        generated_columns[f"limit_up_streak_ge_{n}"] = up_close_count.ge(n).astype(float)
        generated_columns[f"limit_down_streak_ge_{n}"] = down_close_count.ge(n).astype(float)
        generated_columns[f"limit_up_touch_streak_ge_{n}"] = up_touch_count.ge(n).astype(float)
        generated_columns[f"limit_down_touch_streak_ge_{n}"] = down_touch_count.ge(n).astype(float)
        generated_columns[f"break_board_after_streak_ge_{n}"] = ((prev_up_count >= n) & ~current_up).astype(float)
        generated_columns[f"limit_down_rebound_after_streak_ge_{n}"] = ((prev_down_count >= n) & ~current_down).astype(float)

    market_high = up_close_count.groupby(frame["date"], sort=False).transform("max").fillna(0.0)
    frame["market_high_board"] = market_high.astype(float)
    frame["is_market_high_board"] = ((up_close_count > 0.0) & up_close_count.eq(market_high)).astype(float)
    frame["streak_gap_to_market_high"] = (market_high - up_close_count).clip(lower=0.0)
    frame["high_board_rank"] = up_close_count.groupby(frame["date"], sort=False).rank(method="dense", ascending=False)

    high_board_flag = frame["is_market_high_board"].astype(float)
    for days in DEFAULT_POST_HIGH_BOARD_DAYS:
        shifted_high_board = high_board_flag.groupby(frame["code"], sort=False).shift(days).fillna(0.0) > 0.0
        generated_columns[f"post_market_high_board_tplus_{days}"] = shifted_high_board.astype(float)
        generated_columns[f"break_after_high_board_tplus_{days}"] = (shifted_high_board & frame["limit_up_break"].gt(0.0)).astype(float)

    if generated_columns:
        frame = pd.concat([frame, pd.DataFrame(generated_columns, index=frame.index)], axis=1)

    frame.attrs["event_derived_feature_contract"] = event_derived_feature_contract(max_streak_n=max_streak_n)
    frame.attrs["event_derived_feature_sources"] = _source_report(frame)
    return frame


def event_derived_feature_coverage_report(frame: pd.DataFrame, *, max_streak_n: int = DEFAULT_MAX_STREAK_N) -> dict[str, Any]:
    fields = sorted(set(BASE_EVENT_DERIVED_FEATURE_SPECS) | _parametric_feature_names(max_streak_n))
    rows = int(len(frame))
    coverage: dict[str, Any] = {}
    for field in fields:
        if field not in frame.columns:
            coverage[field] = {"present": False, "non_null_ratio": 0.0, "positive_ratio": 0.0}
            continue
        values = pd.to_numeric(frame[field], errors="coerce")
        non_null = values.notna()
        positive = values.fillna(0.0).gt(0.0)
        coverage[field] = {
            "present": True,
            "non_null_ratio": round(float(non_null.mean()) if rows else 0.0, 6),
            "positive_ratio": round(float(positive.mean()) if rows else 0.0, 6),
        }
    return {
        "adapter": "event_derived_feature_layer",
        "version": event_derived_feature_contract(max_streak_n=max_streak_n)["version"],
        "rows": rows,
        "date_min": str(pd.to_datetime(frame["date"], errors="coerce").min().date()) if rows and "date" in frame else None,
        "date_max": str(pd.to_datetime(frame["date"], errors="coerce").max().date()) if rows and "date" in frame else None,
        "code_count": int(frame["code"].nunique()) if "code" in frame else 0,
        "source_report": frame.attrs.get("event_derived_feature_sources", _source_report(frame)),
        "coverage": coverage,
        "contract": event_derived_feature_contract(max_streak_n=max_streak_n),
    }


def _close_limit_mask(frame: pd.DataFrame, direction: str, *, up_limit_pct: float, down_limit_pct: float) -> pd.Series:
    if "tdxgp_limit_status" in frame.columns:
        status = pd.to_numeric(frame["tdxgp_limit_status"], errors="coerce")
        if direction == "up":
            return status.eq(2.0).fillna(False)
        return status.eq(-2.0).fillna(False)
    names = ("is_limit_up", "limitup", "limit_up", "up_limit") if direction == "up" else ("is_limit_down", "limitdown", "limit_down", "down_limit")
    for name in names:
        if name in frame.columns and pd.to_numeric(frame[name], errors="coerce").notna().any():
            return pd.to_numeric(frame[name], errors="coerce").fillna(0.0).ne(0.0)
    if "rt_change_pct" in frame.columns:
        change = pd.to_numeric(frame["rt_change_pct"], errors="coerce")
        return change.ge(up_limit_pct * 100.0) if direction == "up" else change.le(down_limit_pct * 100.0)
    prev_close = pd.to_numeric(frame.get("event_prev_close"), errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    ratio = close / prev_close.replace(0, np.nan) - 1.0
    return ratio.ge(up_limit_pct) if direction == "up" else ratio.le(down_limit_pct)


def _open_touch_mask(
    frame: pd.DataFrame,
    price_column: str,
    direction: str,
    *,
    up_limit_pct: float,
    down_limit_pct: float,
    tolerance: float,
) -> pd.Series:
    price = pd.to_numeric(frame[price_column], errors="coerce")
    if direction == "up":
        for column in ("up_limit_price", "limit_up_price"):
            if column in frame.columns:
                limit_price = pd.to_numeric(frame[column], errors="coerce")
                return price.ge(limit_price - tolerance).fillna(False)
        prev_close = pd.to_numeric(frame["event_prev_close"], errors="coerce")
        return (price / prev_close.replace(0, np.nan) - 1.0).ge(up_limit_pct - tolerance).fillna(False)
    for column in ("down_limit_price", "limit_down_price"):
        if column in frame.columns:
            limit_price = pd.to_numeric(frame[column], errors="coerce")
            return price.le(limit_price + tolerance).fillna(False)
    prev_close = pd.to_numeric(frame["event_prev_close"], errors="coerce")
    return (price / prev_close.replace(0, np.nan) - 1.0).le(down_limit_pct + tolerance).fillna(False)


def _streak_count(frame: pd.DataFrame, mask: pd.Series) -> pd.Series:
    event = mask.fillna(False).astype(float)
    return event.groupby(frame["code"], sort=False).transform(
        lambda item: item.groupby((item <= 0.0).cumsum()).cumsum()
    )


def _source_report(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "has_tdxgp_limit_status": bool("tdxgp_limit_status" in frame.columns and pd.to_numeric(frame["tdxgp_limit_status"], errors="coerce").notna().any()),
        "has_exact_up_limit_price": bool(any(column in frame.columns for column in ("up_limit_price", "limit_up_price"))),
        "has_exact_down_limit_price": bool(any(column in frame.columns for column in ("down_limit_price", "limit_down_price"))),
        "has_open_high_low": bool({"open", "high", "low"}.issubset(frame.columns)),
        "has_rt_change_pct": bool("rt_change_pct" in frame.columns),
        "price_touch_mode": "exact_limit_price_if_available_else_prev_close_pct_proxy",
    }


def _infer_event_family(name: str) -> str:
    if "count_t" in name or "any_" in name:
        return "limit_event_count"
    if "close_not_open" in name:
        return "limit_close_without_open"
    if "reason_record" in name:
        return "limit_reason_record" if "without" not in name else "limit_data_quality"
    if "open_board_record" in name:
        return "open_board_record"
    if "seal_" in name:
        return "limit_seal_flow"
    if "circulation" in name or "capacity" in name:
        return "event_capacity"
    if "turnover_ratio_real" in name:
        return "event_liquidity"
    if "plate_score" in name:
        return "theme_plate"
    if "high_board" in name or "market_high" in name or "streak_gap" in name:
        return "high_board"
    if "down_rebound" in name or "down_repair" in name:
        return "limit_down_repair"
    if "streak" in name:
        return "limit_streak"
    if "open_not_close" in name:
        return "limit_open_break"
    if "touch_not_close" in name or "break" in name:
        return "limit_break"
    if "open" in name:
        return "limit_open"
    if "touch" in name:
        return "limit_touch"
    return "limit_close"


def _infer_source_fields(name: str) -> tuple[str, ...]:
    if "touch" in name:
        return ("high", "low", "prev_close", "tdxgp_limit_status")
    if "open" in name:
        return ("open", "prev_close")
    if "high_board" in name or "streak_gap" in name:
        return ("limit_up_streak_close",)
    return ("limit_up_close_event", "limit_down_close_event")


def _infer_tradability_rule(name: str) -> str:
    if "up" in name:
        return "buy_side_may_be_unfilled_on_limit_up; enforce entry limit mask"
    if "down" in name:
        return "sell_side_may_be_unfilled_on_limit_down; enforce entry limit mask"
    return "not_a_direct_tradability_filter"
