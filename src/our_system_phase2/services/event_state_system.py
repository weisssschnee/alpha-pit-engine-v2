"""PIT-safe event/state detection and materialization for NEXTGEN-DARK."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

from our_system_phase2.services.pit_group_sidecar import PLACEHOLDER_GROUPS
from our_system_phase2.services.typed_temporal_program import evaluate_temporal_primitive


EVENT_STATE_VERSION = "nextgen_dark_event_state_v1"


@dataclass(frozen=True, slots=True)
class EventStateFeatureSpec:
    name: str
    inputs: tuple[str, ...]
    output_type: str
    observable_time: str
    maturity: str
    missing_semantics: str
    overlap_policy: str
    source_lag: str
    canonicalization: str = "event_name_plus_versioned_parameters"

    def canonical(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["inputs"] = list(self.inputs)
        return payload


def _event_spec(name: str, inputs: tuple[str, ...], output: str, maturity: str, missing: str, overlap: str = "configured") -> EventStateFeatureSpec:
    return EventStateFeatureSpec(
        name, inputs, output, "max_input_observable_time_plus_maturity", maturity,
        missing, overlap, "max_input_source_lag",
    )


EVENT_STATE_SPECS = {
    row.name: row
    for row in (
        _event_spec("limit_up_touch", ("high", "up_limit_price"), "event", "0 bars", "missing limit price blocks detection"),
        _event_spec("limit_down_touch", ("low", "down_limit_price"), "event", "0 bars", "missing limit price blocks detection"),
        _event_spec("seal", ("close", "up_limit_price"), "event", "0 bars", "missing close/limit blocks detection"),
        _event_spec("break_board", ("seal_state", "close"), "event", "0 bars", "missing state blocks transition"),
        _event_spec("reseal", ("break_board", "seal_state"), "event", "0 bars", "missing state blocks transition"),
        _event_spec("event_age", ("event",), "count", "0 bars", "missing before first event"),
        _event_spec("event_count", ("event", "window"), "count", "window-1 bars", "full window required"),
        _event_spec("event_intensity", ("high", "low", "close", "limit_price"), "ratio", "0 bars", "missing range yields missing"),
        _event_spec("state_duration", ("state",), "count", "0 bars", "missing state yields missing"),
        _event_spec("pre_event_path", ("close", "event", "window"), "ratio", "0 bars", "full pre-event window required"),
        _event_spec("post_event_path", ("close", "event", "window"), "numeric", "window bars after event", "output delayed until post window matures"),
        _event_spec("continuation_reversal", ("event_direction", "path_return"), "state", "0 bars", "missing anchor yields missing"),
        _event_spec("firstN_path", ("firstN_return",), "numeric", "firstN maturity", "missing firstN stays missing"),
        _event_spec("daily_context_lag", ("context", "context_observed_at"), "numeric", "0 bars", "context unavailable before observed_at"),
        _event_spec("state_transition", ("state",), "event", "0 bars", "missing previous state yields no transition"),
        _event_spec("cross_asset_plate_confirmation", ("event_direction", "pit_peer_value"), "numeric", "0 bars", "requires versioned PIT group sidecar"),
    )
}


def event_state_contract() -> dict[str, Any]:
    rows = [EVENT_STATE_SPECS[name].canonical() for name in sorted(EVENT_STATE_SPECS)]
    payload = {
        "version": EVENT_STATE_VERSION,
        "feature_count": len(rows),
        "features": rows,
        "winner_or_performance_directed": False,
    }
    payload["contract_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return payload


@dataclass(frozen=True, slots=True)
class EventStateConfig:
    limit_tolerance: float = 1e-6
    event_count_window: int = 10
    pre_path_window: int = 5
    post_path_window: int = 3
    overlap_policy: str = "latest"

    def validate(self) -> None:
        if self.limit_tolerance < 0:
            raise ValueError("limit_tolerance must be non-negative")
        if min(self.event_count_window, self.pre_path_window, self.post_path_window) <= 0:
            raise ValueError("event/path windows must be positive")
        if self.overlap_policy not in {"latest", "merge", "reject"}:
            raise ValueError(f"unsupported overlap policy: {self.overlap_policy}")


def _rolling_sum(frame: pd.DataFrame, values: pd.Series, window: int) -> pd.Series:
    return values.groupby(frame["code"], sort=False).transform(
        lambda item: item.rolling(window, min_periods=window).sum()
    )


def _session_keys(frame: pd.DataFrame) -> pd.Series:
    if "date" in frame.columns:
        return pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    return pd.to_datetime(frame["trade_time"], errors="coerce").dt.normalize()


def _merge_group_confirmation(frame: pd.DataFrame, group_confirmation: pd.DataFrame | None) -> pd.DataFrame:
    if group_confirmation is None:
        return frame
    required = {"code", "trade_time", "group_id", "source_version", "peer_value_mean"}
    missing = sorted(required - set(group_confirmation.columns))
    if missing:
        raise ValueError(f"group confirmation missing PIT contract columns: {missing}")
    groups = group_confirmation.copy()
    bad = groups["group_id"].astype(str).str.lower().isin(PLACEHOLDER_GROUPS)
    if bad.any() or groups["source_version"].astype(str).str.strip().eq("").any():
        raise ValueError("group confirmation cannot use placeholder IDs or empty source version")
    groups["trade_time"] = pd.to_datetime(groups["trade_time"], errors="coerce")
    aggregated = groups.groupby(["code", "trade_time"], as_index=False).agg(
        pit_peer_value_mean=("peer_value_mean", "mean"),
        pit_group_count=("group_id", "nunique"),
        pit_source_versions=("source_version", lambda values: "|".join(sorted(set(map(str, values))))),
    )
    return frame.merge(aggregated, on=["code", "trade_time"], how="left", validate="many_to_one")


def build_event_state_features(
    frame: pd.DataFrame,
    *,
    config: EventStateConfig | None = None,
    group_confirmation: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    cfg = config or EventStateConfig()
    cfg.validate()
    required = {"code", "trade_time", "high", "low", "close", "up_limit_price", "down_limit_price"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"event frame missing columns: {missing}")
    out = frame.copy()
    out["trade_time"] = pd.to_datetime(out["trade_time"], errors="coerce")
    out = out.sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
    out = _merge_group_confirmation(out, group_confirmation)

    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    close = pd.to_numeric(out["close"], errors="coerce")
    up = pd.to_numeric(out["up_limit_price"], errors="coerce")
    down = pd.to_numeric(out["down_limit_price"], errors="coerce")
    tolerance = cfg.limit_tolerance
    up_touch = high.ge(up * (1.0 - tolerance)) & up.notna()
    down_touch = low.le(down * (1.0 + tolerance)) & down.notna()
    at_up = close.ge(up * (1.0 - tolerance)) & up.notna()
    at_down = close.le(down * (1.0 + tolerance)) & down.notna()
    overlap = up_touch & down_touch
    if cfg.overlap_policy == "reject" and overlap.any():
        raise ValueError("overlapping up/down events are forbidden by config")
    if cfg.overlap_policy == "latest":
        # With contradictory limit inputs, close proximity decides. Ties merge.
        up_distance = (close - up).abs()
        down_distance = (close - down).abs()
        choose_up = overlap & up_distance.lt(down_distance)
        choose_down = overlap & down_distance.lt(up_distance)
        up_touch = (up_touch & ~overlap) | choose_up
        down_touch = (down_touch & ~overlap) | choose_down
        at_up = at_up & up_touch
        at_down = at_down & down_touch
    elif cfg.overlap_policy == "merge":
        at_up = at_up & ~overlap
        at_down = at_down & ~overlap

    session = _session_keys(out)
    group_keys = [out["code"], session]
    previous_up = at_up.groupby(group_keys, sort=False).shift(1, fill_value=False)
    previous_down = at_down.groupby(group_keys, sort=False).shift(1, fill_value=False)
    seal = at_up & ~previous_up
    down_seal = at_down & ~previous_down
    break_board = previous_up & ~at_up
    down_break = previous_down & ~at_down
    had_break = break_board.groupby(group_keys, sort=False).transform(
        lambda values: values.cummax().shift(1, fill_value=False)
    )
    had_down_break = down_break.groupby(group_keys, sort=False).transform(
        lambda values: values.cummax().shift(1, fill_value=False)
    )
    reseal = at_up & had_break & ~previous_up
    down_reseal = at_down & had_down_break & ~previous_down

    out["event_limit_up_touch"] = up_touch.astype(float)
    out["event_limit_down_touch"] = down_touch.astype(float)
    out["event_seal"] = seal.astype(float)
    out["event_break_board"] = break_board.astype(float)
    out["event_reseal"] = reseal.astype(float)
    out["event_down_seal"] = down_seal.astype(float)
    out["event_down_break"] = down_break.astype(float)
    out["event_down_reseal"] = down_reseal.astype(float)
    active = seal | break_board | reseal | down_seal | down_break | down_reseal
    direction = pd.Series(
        np.select(
            [seal | reseal, break_board, down_seal | down_reseal, down_break],
            [1.0, -1.0, -1.0, 1.0],
            default=0.0,
        ),
        index=out.index,
    )
    out["event_active"] = active.astype(float)
    out["event_direction"] = direction
    out["event_age"] = evaluate_temporal_primitive(out, "TimeSince", [out["event_active"]])
    out["event_count"] = _rolling_sum(out, out["event_active"], cfg.event_count_window)
    price_range = (up - down).replace(0, np.nan)
    out["event_intensity"] = np.where(direction >= 0, (close - down) / price_range, (up - close) / price_range)
    out["event_intensity"] = pd.to_numeric(out["event_intensity"], errors="coerce").clip(0.0, 1.0)
    state = pd.Series(np.select([at_up, at_down], [1.0, -1.0], default=0.0), index=out.index)
    out["state_limit"] = state
    out["state_duration"] = evaluate_temporal_primitive(out, "StateAge", [state])
    previous_state = state.groupby(out["code"], sort=False).shift(1)
    out["state_transition"] = (state - previous_state).where(previous_state.notna(), 0.0)

    pre_shape = evaluate_temporal_primitive(out, "PathShape", [close], [cfg.pre_path_window])
    out["event_pre_path_shape"] = pre_shape.where(active)
    out["event_post_window_mean"] = evaluate_temporal_primitive(
        out, "EventWindow", [close, out["event_active"]], [0, cfg.post_path_window]
    )
    anchor = close.where(active).groupby(out["code"], sort=False).ffill()
    anchor_direction = direction.where(active).groupby(out["code"], sort=False).ffill()
    path_return = close / anchor.replace(0, np.nan) - 1.0
    out["event_path_return"] = path_return
    signed_path = anchor_direction * path_return
    out["event_continuation"] = signed_path.gt(0).where(anchor.notna()).astype(float)
    out["event_reversal"] = signed_path.lt(0).where(anchor.notna()).astype(float)

    firstn_candidates = [
        "m1_first30_last_return_vs_open",
        "m1_first15_last_return_vs_open",
        "m1_first5_last_return_vs_open",
    ]
    firstn = next((column for column in firstn_candidates if column in out.columns), None)
    out["event_firstn_path"] = pd.to_numeric(out[firstn], errors="coerce") if firstn else np.nan

    if "daily_context" in out.columns:
        if "daily_context_observed_at" not in out.columns:
            raise ValueError("daily_context requires daily_context_observed_at")
        observed = pd.to_datetime(out["daily_context_observed_at"], errors="coerce")
        out["event_daily_context_lagged"] = pd.to_numeric(out["daily_context"], errors="coerce").where(
            observed.notna() & (observed <= out["trade_time"])
        )
    else:
        out["event_daily_context_lagged"] = np.nan

    if "pit_peer_value_mean" in out.columns:
        out["event_cross_asset_confirmation"] = direction * pd.to_numeric(out["pit_peer_value_mean"], errors="coerce")
    else:
        out["event_cross_asset_confirmation"] = np.nan

    produced = [column for column in out.columns if column.startswith(("event_", "state_"))]
    manifest = {
        "version": EVENT_STATE_VERSION,
        "contract_hash": event_state_contract()["contract_hash"],
        "row_count": len(out),
        "produced_fields": produced,
        "overlap_policy": cfg.overlap_policy,
        "event_count_window": cfg.event_count_window,
        "pre_path_window": cfg.pre_path_window,
        "post_path_window": cfg.post_path_window,
        "post_event_observable_delay_bars": cfg.post_path_window,
        "pit_group_confirmation_attached": group_confirmation is not None,
        "winner_or_performance_directed": False,
    }
    return out, manifest
