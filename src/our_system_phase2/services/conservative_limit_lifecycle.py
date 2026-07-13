"""Conservative A-share limit lifecycle reconstruction from one-minute bars."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from typing import Any

import numpy as np
import pandas as pd


LIFECYCLE_VERSION = "cn_conservative_limit_lifecycle_v2"


@dataclass(frozen=True, slots=True)
class LimitLifecycleConfig:
    price_tick: float = 0.01
    comparison_tolerance: float = 1e-6
    conservative_listing_exclusion_sessions: int = 5
    require_pit_st_context_for_derived_limits: bool = True
    allow_vendor_confirmed_up_limit_when_st_missing: bool = True

    def validate(self) -> None:
        if self.price_tick <= 0:
            raise ValueError("price_tick must be positive")
        if self.comparison_tolerance < 0:
            raise ValueError("comparison_tolerance must be nonnegative")
        if self.conservative_listing_exclusion_sessions < 1:
            raise ValueError("listing exclusion must be positive")


def _round_tick(values: pd.Series, tick: float) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    rounded = np.floor(numeric / tick + 0.5 + 1e-10) * tick
    return pd.Series(rounded, index=values.index, dtype=float)


def _board_limit_pct(code: pd.Series) -> pd.Series:
    text = code.astype(str).str.extract(r"(\d{6})", expand=False).fillna("")
    pct = pd.Series(np.nan, index=code.index, dtype=float)
    pct.loc[text.str.startswith(("300", "301", "688", "689"))] = 0.20
    pct.loc[text.str.startswith(("4", "8", "920"))] = 0.30
    pct.loc[text.str.startswith(("0", "1", "2", "5", "6")) & pct.isna()] = 0.10
    return pct


def _resolve_session_limits(frame: pd.DataFrame, cfg: LimitLifecycleConfig) -> pd.DataFrame:
    keys = ["code", "session"]
    aggregations: dict[str, tuple[str, str]] = {
        "session_close": ("close", "last"),
        "session_high": ("high", "max"),
        "session_low": ("low", "min"),
    }
    for column in ("up_limit_price", "down_limit_price", "ctx_hfq_is_st"):
        if column in frame.columns:
            aggregations[column] = (column, "first")
    if "evt_uplimit_active" in frame.columns:
        aggregations["vendor_uplimit_observed"] = ("evt_uplimit_active", "max")
    sessions = frame.groupby(keys, as_index=False, sort=False).agg(**aggregations)
    sessions = sessions.sort_values(keys, kind="mergesort").reset_index(drop=True)
    sessions["previous_session_close"] = sessions.groupby("code", sort=False)["session_close"].shift(1)
    sessions["observed_session_number"] = sessions.groupby("code", sort=False).cumcount() + 1
    sessions["base_limit_pct"] = _board_limit_pct(sessions["code"])
    st = pd.to_numeric(sessions.get("ctx_hfq_is_st"), errors="coerce")
    if not isinstance(st, pd.Series):
        st = pd.Series(np.nan, index=sessions.index, dtype=float)
    exact_up = pd.to_numeric(sessions.get("up_limit_price"), errors="coerce")
    exact_down = pd.to_numeric(sessions.get("down_limit_price"), errors="coerce")
    if not isinstance(exact_up, pd.Series):
        exact_up = pd.Series(np.nan, index=sessions.index, dtype=float)
    if not isinstance(exact_down, pd.Series):
        exact_down = pd.Series(np.nan, index=sessions.index, dtype=float)
    exact_up_available = exact_up.gt(0)
    exact_down_available = exact_down.gt(0)
    listing_ok = sessions["observed_session_number"].gt(cfg.conservative_listing_exclusion_sessions)
    board_ok = sessions["base_limit_pct"].notna()
    previous_ok = pd.to_numeric(sessions["previous_session_close"], errors="coerce").gt(0)
    st_ok = st.notna() if cfg.require_pit_st_context_for_derived_limits else pd.Series(True, index=sessions.index)
    derived_up_eligible = ~exact_up_available & listing_ok & board_ok & previous_ok & st_ok
    derived_down_eligible = ~exact_down_available & listing_ok & board_ok & previous_ok & st_ok
    pct = sessions["base_limit_pct"].where(~st.eq(1.0), 0.05)
    derived_up = _round_tick(sessions["previous_session_close"] * (1.0 + pct), cfg.price_tick)
    derived_down = _round_tick(sessions["previous_session_close"] * (1.0 - pct), cfg.price_tick)
    standard_up = _round_tick(
        sessions["previous_session_close"] * (1.0 + sessions["base_limit_pct"]), cfg.price_tick
    )
    tolerance = cfg.price_tick / 2.0 + cfg.comparison_tolerance
    vendor_observed_raw = sessions.get("vendor_uplimit_observed")
    vendor_observed = (
        pd.to_numeric(vendor_observed_raw, errors="coerce").eq(1.0)
        if isinstance(vendor_observed_raw, pd.Series)
        else pd.Series(False, index=sessions.index)
    )
    vendor_price_consistent = pd.to_numeric(sessions["session_high"], errors="coerce").sub(standard_up).abs().le(tolerance)
    vendor_confirmed_up = (
        cfg.allow_vendor_confirmed_up_limit_when_st_missing
        & ~exact_up_available
        & ~st.notna()
        & listing_ok
        & board_ok
        & previous_ok
        & vendor_observed
        & vendor_price_consistent
    )
    sessions["resolved_up_limit_price"] = exact_up.where(
        exact_up_available, derived_up.where(derived_up_eligible, standard_up.where(vendor_confirmed_up))
    )
    sessions["resolved_down_limit_price"] = exact_down.where(
        exact_down_available, derived_down.where(derived_down_eligible)
    )
    sessions["up_limit_price_source"] = np.select(
        [exact_up_available, derived_up_eligible, vendor_confirmed_up],
        ["PIT_EXACT_LIMIT_FIELD", "CONSERVATIVE_RULE_DERIVATION", "VENDOR_CONFIRMED_RULE_DERIVATION"],
        default="EXCLUDED",
    )
    sessions["down_limit_price_source"] = np.select(
        [exact_down_available, derived_down_eligible],
        ["PIT_EXACT_LIMIT_FIELD", "CONSERVATIVE_RULE_DERIVATION"],
        default="EXCLUDED",
    )
    # Compatibility view: the historical column described the up-limit path.
    sessions["limit_price_source"] = sessions["up_limit_price_source"]
    reason = np.select(
        [exact_up_available, ~listing_ok, ~board_ok, ~previous_ok, ~st_ok & ~vendor_confirmed_up],
        ["", "IPO_OR_INSUFFICIENT_LISTING_HISTORY", "UNSUPPORTED_SECURITY_RULE", "MISSING_PREVIOUS_SESSION_CLOSE", "MISSING_PIT_ST_CONTEXT"],
        default="RULE_UNCERTAIN",
    )
    sessions["limit_rule_exclusion_reason"] = reason
    sessions["up_limit_rule_eligible"] = exact_up_available | derived_up_eligible | vendor_confirmed_up
    sessions["down_limit_rule_eligible"] = exact_down_available | derived_down_eligible
    sessions["limit_rule_eligible"] = sessions["up_limit_rule_eligible"] | sessions["down_limit_rule_eligible"]
    return sessions


def _direction_features(out: pd.DataFrame, direction: str, cfg: LimitLifecycleConfig) -> None:
    is_up = direction == "UP"
    limit = pd.to_numeric(
        out["resolved_up_limit_price" if is_up else "resolved_down_limit_price"], errors="coerce"
    )
    open_price = pd.to_numeric(out["open"], errors="coerce")
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    close = pd.to_numeric(out["close"], errors="coerce")
    tolerance = cfg.price_tick / 2.0 + cfg.comparison_tolerance
    eligible = out[f"{direction.lower()}_limit_rule_eligible"].fillna(False) & limit.notna()
    if is_up:
        vendor_confirmed = out["up_limit_price_source"].eq("VENDOR_CONFIRMED_RULE_DERIVATION")
        vendor_raw = out.get("evt_uplimit_active")
        vendor_visible = (
            pd.to_numeric(vendor_raw, errors="coerce").eq(1.0)
            if isinstance(vendor_raw, pd.Series)
            else pd.Series(False, index=out.index)
        )
        eligible = eligible & (~vendor_confirmed | vendor_visible)
    touched = (high.ge(limit - tolerance) if is_up else low.le(limit + tolerance)) & eligible
    opened = open_price.sub(limit).abs().le(tolerance) & eligible
    closed = close.sub(limit).abs().le(tolerance) & eligible
    session_key = [out["code"], out["session"]]
    previous_closed = closed.groupby(session_key, sort=False).shift(1, fill_value=False)
    left = previous_closed & ~closed & eligible
    had_left = left.groupby(session_key, sort=False).transform(lambda x: x.cummax().shift(1, fill_value=False))
    closed_entry = closed & ~previous_closed
    resealed = closed_entry & had_left
    first_touch = touched & ~touched.groupby(session_key, sort=False).transform(lambda x: x.cummax().shift(1, fill_value=False))
    other_side = low.lt(limit - tolerance) if is_up else high.gt(limit + tolerance)
    intrabar_ambiguous = touched & other_side
    values = {
        "TOUCHED_LIMIT": touched,
        "OPENED_AT_LIMIT": opened,
        "CLOSED_AT_LIMIT": closed,
        "FIRST_TOUCH": first_touch,
        "CLOSED_AT_LIMIT_ENTRY": closed_entry,
        "LEFT_LIMIT_BETWEEN_BARS": left,
        "RESEALED_BETWEEN_BARS": resealed,
        "INTRABAR_ORDER_AMBIGUOUS": intrabar_ambiguous,
    }
    for name, value in values.items():
        out[f"{direction}_{name}"] = value.where(eligible, pd.NA).astype("boolean")


def _episode_rows(out: pd.DataFrame, direction: str) -> list[dict[str, Any]]:
    touched_column = f"{direction}_TOUCHED_LIMIT"
    rows: list[dict[str, Any]] = []
    for (code, session), group in out.loc[out[touched_column].fillna(False)].groupby(
        ["code", "session"], sort=False
    ):
        all_bars = out.loc[out["code"].eq(code) & out["session"].eq(session)]
        first_index = group.index[0]
        first_time = out.at[first_index, "trade_time"]
        later = all_bars.loc[all_bars["trade_time"].gt(first_time)]
        eligible_time = later["trade_time"].iloc[0] if not later.empty else pd.NaT
        pulses = {
            "first_touch": int(group[f"{direction}_FIRST_TOUCH"].fillna(False).sum()),
            "closed_entries": int(group[f"{direction}_CLOSED_AT_LIMIT_ENTRY"].fillna(False).sum()),
            "left_between_bars": int(group[f"{direction}_LEFT_LIMIT_BETWEEN_BARS"].fillna(False).sum()),
            "resealed_between_bars": int(group[f"{direction}_RESEALED_BETWEEN_BARS"].fillna(False).sum()),
        }
        identity = f"{code}|{pd.Timestamp(session).date()}|{direction}|{pd.Timestamp(first_time).isoformat()}"
        rows.append(
            {
                "episode_id": hashlib.sha256(identity.encode()).hexdigest()[:24],
                "event_source": "minute_limit_lifecycle",
                "event_class": f"LIMIT_{direction}_LIFECYCLE",
                "event_start": first_time,
                "event_end": group["trade_time"].iloc[-1],
                "maturity_time": first_time,
                "eligible_action_time": eligible_time,
                "entity_scope": "SYMBOL",
                "entity_id": str(code),
                "session": pd.Timestamp(session),
                "direction": 1 if direction == "UP" else -1,
                "inference_unit": "lifecycle_episode",
                "admission_vote": 1,
                "intrabar_order_ambiguous": bool(group[f"{direction}_INTRABAR_ORDER_AMBIGUOUS"].fillna(False).any()),
                **pulses,
            }
        )
    return rows


def materialize_conservative_limit_lifecycle(
    frame: pd.DataFrame, *, config: LimitLifecycleConfig | None = None
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    cfg = config or LimitLifecycleConfig()
    cfg.validate()
    required = {"code", "trade_time", "open", "high", "low", "close"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"limit lifecycle frame missing columns: {missing}")
    out = frame.copy()
    out["trade_time"] = pd.to_datetime(out["trade_time"], errors="raise")
    out["session"] = out["trade_time"].dt.normalize()
    if out.duplicated(["code", "trade_time"]).any():
        raise ValueError("duplicate lifecycle coordinates")
    out = out.sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
    sessions = _resolve_session_limits(out, cfg)
    original_limits = [column for column in ("up_limit_price", "down_limit_price") if column in out]
    out = out.drop(columns=original_limits).merge(
        sessions.drop(columns=["session_close", "ctx_hfq_is_st", *original_limits], errors="ignore"),
        on=["code", "session"], how="left", validate="many_to_one",
    )
    _direction_features(out, "UP", cfg)
    _direction_features(out, "DOWN", cfg)
    out["TOUCHED_LIMIT"] = out["UP_TOUCHED_LIMIT"] | out["DOWN_TOUCHED_LIMIT"]
    out["OPENED_AT_LIMIT"] = out["UP_OPENED_AT_LIMIT"] | out["DOWN_OPENED_AT_LIMIT"]
    out["CLOSED_AT_LIMIT"] = out["UP_CLOSED_AT_LIMIT"] | out["DOWN_CLOSED_AT_LIMIT"]
    out["LEFT_LIMIT_BETWEEN_BARS"] = out["UP_LEFT_LIMIT_BETWEEN_BARS"] | out["DOWN_LEFT_LIMIT_BETWEEN_BARS"]
    out["RESEALED_BETWEEN_BARS"] = out["UP_RESEALED_BETWEEN_BARS"] | out["DOWN_RESEALED_BETWEEN_BARS"]
    out["INTRABAR_ORDER_AMBIGUOUS"] = out["UP_INTRABAR_ORDER_AMBIGUOUS"] | out["DOWN_INTRABAR_ORDER_AMBIGUOUS"]
    episodes = pd.DataFrame(_episode_rows(out, "UP") + _episode_rows(out, "DOWN"))
    if not episodes.empty and episodes["episode_id"].duplicated().any():
        raise RuntimeError("lifecycle episode identity collision")
    manifest = {
        "version": LIFECYCLE_VERSION,
        "config": asdict(cfg),
        "row_count": len(out),
        "session_count": int(sessions.shape[0]),
        "eligible_session_count": int(sessions["limit_rule_eligible"].sum()),
        "exact_limit_session_count": int(
            (sessions["up_limit_price_source"].eq("PIT_EXACT_LIMIT_FIELD") | sessions["down_limit_price_source"].eq("PIT_EXACT_LIMIT_FIELD")).sum()
        ),
        "derived_limit_session_count": int(sessions["limit_price_source"].eq("CONSERVATIVE_RULE_DERIVATION").sum()),
        "vendor_confirmed_limit_session_count": int(
            sessions["up_limit_price_source"].eq("VENDOR_CONFIRMED_RULE_DERIVATION").sum()
        ),
        "excluded_session_count": int(sessions["limit_price_source"].eq("EXCLUDED").sum()),
        "episode_count": len(episodes),
        "episode_inference_unit": "lifecycle_episode",
        "one_episode_one_admission_vote": True,
        "intrabar_ambiguity_negative_control_allowed": False,
        "claims_not_made": ["true order-book seal", "intrabar transition order", "intrabar transition count"],
    }
    return out, episodes, manifest


def validate_derived_limits_against_vendor_occurrence(features: pd.DataFrame) -> dict[str, Any]:
    if "evt_uplimit_active" not in features:
        return {"decision": "NOT_AVAILABLE", "vendor_episode_count": 0}
    active = pd.to_numeric(features["evt_uplimit_active"], errors="coerce").eq(1.0)
    key = [features["code"], features["session"]]
    entry = active & ~active.groupby(key, sort=False).shift(1, fill_value=False)
    entries = features.loc[entry, ["code", "session", "trade_time"]].rename(columns={"trade_time": "vendor_time"})
    derived = features.loc[features["up_limit_price_source"].isin(
        ["CONSERVATIVE_RULE_DERIVATION", "VENDOR_CONFIRMED_RULE_DERIVATION"]
    )]
    first_touch = derived.loc[derived["UP_FIRST_TOUCH"].fillna(False), ["code", "session", "trade_time"]].rename(columns={"trade_time": "derived_touch_time"})
    compared = entries.merge(first_touch, on=["code", "session"], how="left", validate="one_to_one")
    if compared.empty:
        return {"decision": "INSUFFICIENT_SUPPORT", "vendor_episode_count": 0}
    delta = (compared["derived_touch_time"] - compared["vendor_time"]).dt.total_seconds().abs() / 60.0
    matched = compared["derived_touch_time"].notna() & delta.le(2.0)
    match_rate = float(matched.mean())
    return {
        "decision": "CONSERVATIVE_LIMIT_VENDOR_CONSISTENCY_PASS" if len(compared) >= 30 and match_rate >= 0.95 else "CONSERVATIVE_LIMIT_VENDOR_CONSISTENCY_FAIL",
        "vendor_episode_count": len(compared),
        "matched_within_two_minutes": int(matched.sum()),
        "match_rate": match_rate,
        "median_absolute_minute_delta": float(delta.dropna().median()) if delta.notna().any() else None,
    }
