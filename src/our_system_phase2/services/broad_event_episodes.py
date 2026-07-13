"""Episode-level materialization and support audit for Broad Event sources."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from typing import Any, Iterable

import numpy as np
import pandas as pd


EPISODE_SYSTEM_VERSION = "cn_broad_event_episode_system_v1"


STOCK_CONTEXT_SOURCES: dict[str, tuple[str, ...]] = {
    "BILLBOARD_CHANGE": ("ctx_billboard_",),
    "MARGIN_FINANCING_CHANGE": ("ctx_rzrq_",),
    "HOLDER_STRUCTURE_CHANGE": ("ctx_holder_",),
    "HOTNESS_CHANGE": ("ctx_ths_hot_",),
}
MARKET_CONTEXT_PREFIXES = ("ctx_sent_", "ctx_zls_")


@dataclass(frozen=True, slots=True)
class EpisodeSupportThresholds:
    min_symbol_episode_count: int = 30
    min_symbol_date_count: int = 10
    min_symbol_count: int = 10
    min_market_block_count: int = 10
    max_single_date_share: float = 0.25
    max_single_symbol_share: float = 0.20


def _identity(*parts: object) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:24]


def _first_bar_map(frame: pd.DataFrame) -> pd.DataFrame:
    ordered = frame.sort_values(["code", "trade_time"], kind="mergesort").copy()
    ordered["session"] = pd.to_datetime(ordered["trade_time"], errors="raise").dt.normalize()
    first = ordered.groupby(["code", "session"], as_index=False, sort=False).first()
    return first


def _relative_change(current: pd.DataFrame, previous: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    valid = current.notna() & previous.notna()
    scale = previous.abs().clip(lower=1e-6)
    relative = (current - previous).abs().div(scale).where(valid)
    intensity = relative.mean(axis=1, skipna=True)
    signed = (current - previous).div(scale).where(valid).mean(axis=1, skipna=True)
    changed = ((current - previous).abs().where(valid) > 1e-12).any(axis=1)
    return intensity, signed, changed


def _stock_context_episodes(first: pd.DataFrame, source_id: str, prefixes: tuple[str, ...]) -> list[dict[str, Any]]:
    columns = sorted(column for column in first if column.startswith(prefixes))
    if not columns:
        return []
    values = first[columns].apply(pd.to_numeric, errors="coerce")
    previous = values.groupby(first["code"], sort=False).shift(1)
    intensity, signed, changed = _relative_change(values, previous)
    if source_id == "MARGIN_FINANCING_CHANGE":
        changed &= intensity.ge(0.02)
    elif source_id == "HOTNESS_CHANGE" and "ctx_ths_hot_rank_diff" in first:
        rank_shock = pd.to_numeric(first["ctx_ths_hot_rank_diff"], errors="coerce").abs().ge(10)
        changed &= intensity.ge(0.05) | rank_shock
    rows = []
    for index in first.index[changed.fillna(False)]:
        code = str(first.at[index, "code"])
        session = pd.Timestamp(first.at[index, "session"])
        event_time = pd.Timestamp(first.at[index, "trade_time"])
        rows.append(
            {
                "episode_id": _identity(source_id, code, session.date()),
                "event_source": source_id,
                "event_class": "DAILY_DISCLOSURE_OR_CONTEXT_CHANGE",
                "event_start": event_time,
                "event_end": event_time,
                "maturity_time": event_time,
                "eligible_action_time": event_time,
                "entity_scope": "SYMBOL",
                "entity_id": code,
                "session": session,
                "direction": float(np.sign(signed.at[index])) if np.isfinite(signed.at[index]) else 0.0,
                "trigger_intensity": float(intensity.at[index]) if np.isfinite(intensity.at[index]) else np.nan,
                "inference_unit": "symbol-day",
                "admission_vote": 1,
                "intrabar_order_ambiguous": False,
                "source_fields": columns,
            }
        )
    return rows


def _vendor_occurrence_episodes(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if "evt_uplimit_active" not in frame:
        return []
    active = pd.to_numeric(frame["evt_uplimit_active"], errors="coerce").eq(1.0)
    session = pd.to_datetime(frame["trade_time"], errors="raise").dt.normalize()
    entry = active & ~active.groupby([frame["code"], session], sort=False).shift(1, fill_value=False)
    rows = []
    for index in frame.index[entry]:
        code = str(frame.at[index, "code"])
        event_time = pd.Timestamp(frame.at[index, "trade_time"])
        event_session = event_time.normalize()
        amount = pd.to_numeric(pd.Series([frame.at[index, "evt_uplimit_amount"]]), errors="coerce").iloc[0] if "evt_uplimit_amount" in frame else np.nan
        rows.append(
            {
                "episode_id": _identity("VENDOR_LIMIT_OCCURRENCE", code, event_session.date()),
                "event_source": "VENDOR_LIMIT_OCCURRENCE",
                "event_class": "LATCHED_VENDOR_OCCURRENCE_ENTRY",
                "event_start": event_time,
                "event_end": event_time,
                "maturity_time": event_time,
                "eligible_action_time": event_time,
                "entity_scope": "SYMBOL",
                "entity_id": code,
                "session": event_session,
                "direction": 1.0,
                "trigger_intensity": float(np.log1p(max(amount, 0.0))) if np.isfinite(amount) else np.nan,
                "inference_unit": "symbol-day occurrence episode",
                "admission_vote": 1,
                "intrabar_order_ambiguous": False,
                "source_fields": ["evt_uplimit_active", "evt_uplimit_amount"],
            }
        )
    return rows


def _market_episodes(first: pd.DataFrame) -> list[dict[str, Any]]:
    columns = sorted(column for column in first if column.startswith(MARKET_CONTEXT_PREFIXES))
    if not columns:
        return []
    market = first.groupby("session", as_index=False, sort=True).first()
    values = market[columns].apply(pd.to_numeric, errors="coerce")
    previous = values.shift(1)
    intensity, signed, changed = _relative_change(values, previous)
    rows = []
    for index in market.index[changed.fillna(False)]:
        session = pd.Timestamp(market.at[index, "session"])
        event_time = pd.Timestamp(market.at[index, "trade_time"])
        rows.append(
            {
                "episode_id": _identity("MARKET_ECOLOGY_TRANSITION", session.date()),
                "event_source": "MARKET_ECOLOGY_TRANSITION",
                "event_class": "REGIME_TRANSITION",
                "event_start": event_time,
                "event_end": event_time,
                "maturity_time": event_time,
                "eligible_action_time": event_time,
                "entity_scope": "MARKET",
                "entity_id": "CN_MARKET",
                "session": session,
                "direction": float(np.sign(signed.at[index])) if np.isfinite(signed.at[index]) else 0.0,
                "trigger_intensity": float(intensity.at[index]) if np.isfinite(intensity.at[index]) else np.nan,
                "inference_unit": "market-time block",
                "admission_vote": 1,
                "intrabar_order_ambiguous": False,
                "source_fields": columns,
                "cross_sectional_rank_allowed": False,
            }
        )
    return rows


def _chip_episodes(chip_context: pd.DataFrame | None) -> list[dict[str, Any]]:
    if chip_context is None or chip_context.empty:
        return []
    frame = chip_context.copy()
    frame["source_observed_at"] = pd.to_datetime(frame["source_observed_at"], errors="raise")
    frame["session"] = frame["source_observed_at"].dt.normalize()
    frame = frame.sort_values(["code", "source_observed_at"], kind="mergesort").reset_index(drop=True)
    columns = sorted(column for column in frame if column.startswith("chip_") and column not in {"chip_source_session"})
    values = frame[columns].apply(pd.to_numeric, errors="coerce")
    previous = values.groupby(frame["code"], sort=False).shift(1)
    intensity, signed, changed = _relative_change(values, previous)
    changed &= intensity.ge(0.02)
    rows = []
    for index in frame.index[changed.fillna(False)]:
        code = str(frame.at[index, "code"])
        event_time = pd.Timestamp(frame.at[index, "source_observed_at"])
        session = event_time.normalize()
        rows.append(
            {
                "episode_id": _identity("CHIP_STRUCTURE_CHANGE", code, session.date()),
                "event_source": "CHIP_STRUCTURE_CHANGE",
                "event_class": "DAILY_CONTEXT_CHANGE",
                "event_start": event_time,
                "event_end": event_time,
                "maturity_time": event_time,
                "eligible_action_time": event_time,
                "entity_scope": "SYMBOL",
                "entity_id": code,
                "session": session,
                "direction": float(np.sign(signed.at[index])) if np.isfinite(signed.at[index]) else 0.0,
                "trigger_intensity": float(intensity.at[index]),
                "inference_unit": "symbol-day",
                "admission_vote": 1,
                "intrabar_order_ambiguous": False,
                "source_fields": columns,
            }
        )
    return rows


def materialize_broad_event_episodes(
    frame: pd.DataFrame,
    *,
    lifecycle_episodes: pd.DataFrame | None = None,
    chip_context: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    ordered = frame.sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
    first = _first_bar_map(ordered)
    rows: list[dict[str, Any]] = []
    if lifecycle_episodes is not None and not lifecycle_episodes.empty:
        life = lifecycle_episodes.copy()
        life["event_source"] = np.where(
            pd.to_numeric(life["direction"], errors="coerce").ge(0),
            "LIMIT_UP_LIFECYCLE",
            "LIMIT_DOWN_LIFECYCLE",
        )
        life["trigger_intensity"] = (
            pd.to_numeric(life.get("closed_entries"), errors="coerce").fillna(0)
            + pd.to_numeric(life.get("resealed_between_bars"), errors="coerce").fillna(0)
            + 1.0
        )
        life["source_fields"] = [["open", "high", "low", "close"] for _ in range(len(life))]
        rows.extend(life.to_dict("records"))
    rows.extend(_vendor_occurrence_episodes(ordered))
    for source_id, prefixes in STOCK_CONTEXT_SOURCES.items():
        rows.extend(_stock_context_episodes(first, source_id, prefixes))
    rows.extend(_market_episodes(first))
    rows.extend(_chip_episodes(chip_context))
    episodes = pd.DataFrame(rows)
    if episodes.empty:
        episodes = pd.DataFrame(columns=[
            "episode_id", "event_source", "event_class", "event_start", "event_end",
            "maturity_time", "eligible_action_time", "entity_scope", "entity_id",
            "session", "direction", "trigger_intensity", "inference_unit", "admission_vote",
            "intrabar_order_ambiguous", "source_fields",
        ])
    if episodes["episode_id"].duplicated().any():
        duplicate = episodes.loc[episodes["episode_id"].duplicated(), "episode_id"].iloc[0]
        raise RuntimeError(f"duplicate Broad Event episode ID: {duplicate}")
    if not episodes["admission_vote"].eq(1).all():
        raise RuntimeError("each event episode must own exactly one admission vote")
    manifest = {
        "version": EPISODE_SYSTEM_VERSION,
        "episode_count": len(episodes),
        "event_sources": sorted(episodes["event_source"].unique().tolist()),
        "source_counts": {str(k): int(v) for k, v in episodes["event_source"].value_counts().sort_index().items()},
        "inference_units": sorted(episodes["inference_unit"].unique().tolist()),
        "one_episode_one_admission_vote": True,
        "minute_rows_used_as_inference_units": False,
    }
    return episodes.sort_values(["session", "event_source", "entity_id"], kind="mergesort").reset_index(drop=True), manifest


def audit_episode_support(
    episodes: pd.DataFrame, *, thresholds: EpisodeSupportThresholds | None = None
) -> dict[str, Any]:
    cfg = thresholds or EpisodeSupportThresholds()
    reports: dict[str, dict[str, Any]] = {}
    for source, group in episodes.groupby("event_source", sort=True):
        date_counts = group["session"].dt.normalize().value_counts()
        symbol = group.loc[group["entity_scope"].eq("SYMBOL"), "entity_id"]
        symbol_counts = symbol.value_counts()
        market = group["entity_scope"].eq("MARKET").all()
        checks = {
            "episode_count": len(group) >= (cfg.min_market_block_count if market else cfg.min_symbol_episode_count),
            "date_count": group["session"].nunique() >= (cfg.min_market_block_count if market else cfg.min_symbol_date_count),
            "symbol_count": True if market else symbol.nunique() >= cfg.min_symbol_count,
            "single_date_concentration": date_counts.iloc[0] / len(group) <= cfg.max_single_date_share,
            "single_symbol_concentration": True if market else symbol_counts.iloc[0] / len(group) <= cfg.max_single_symbol_share,
            "one_episode_one_vote": group["admission_vote"].eq(1).all(),
        }
        reports[str(source)] = {
            "decision": "SUPPORT_PASS" if all(checks.values()) else "INSUFFICIENT_SUPPORT",
            "checks": {key: bool(value) for key, value in checks.items()},
            "episode_count": len(group),
            "date_count": int(group["session"].nunique()),
            "symbol_count": int(symbol.nunique()),
            "top_date_share": float(date_counts.iloc[0] / len(group)),
            "top_symbol_share": None if market else float(symbol_counts.iloc[0] / len(group)),
            "inference_unit": str(group["inference_unit"].iloc[0]),
        }
    return {
        "version": EPISODE_SYSTEM_VERSION,
        "thresholds": asdict(cfg),
        "episode_count": len(episodes),
        "source_reports": reports,
        "operational_sources": sorted(source for source, report in reports.items() if report["decision"] == "SUPPORT_PASS"),
        "row_count_used_as_effective_sample_size": False,
    }


def matched_control_contract() -> dict[str, Any]:
    return {
        "version": "cn_broad_event_matched_controls_v1",
        "structural_control": {
            "same_fields": True,
            "same_windows": True,
            "same_complexity": True,
            "same_maturity": True,
            "same_episode_support_times": True,
            "difference": "remove event-derived trigger attributes while retaining preregistered path feature",
        },
        "episode_placebo_control": {
            "same_symbol": True,
            "same_date_structure": True,
            "same_episode_count": True,
            "same_windows_and_maturity": True,
            "difference": "deterministically shift trigger time within eligible development session",
            "ambiguous_rows_allowed_as_negative": False,
        },
        "one_episode_one_admission_vote": True,
        "multiple_preregistered_horizons_allowed": True,
    }
