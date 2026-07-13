"""Performance-blind support audit for Sprint-2 event and state generators."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
import math
from typing import Iterable, Mapping

import numpy as np
import pandas as pd


AUDIT_VERSION = "cn_sprint2_event_state_support_audit_v1"
STATE_SOURCE_VERSION = "cn_sprint2_operational_raw_states_v1"


@dataclass(frozen=True, slots=True)
class EventSupportThresholds:
    min_active_rows: int = 100
    min_active_dates: int = 10
    min_active_symbols: int = 20
    min_enter_transitions: int = 20
    min_exit_transitions: int = 20
    min_episode_count: int = 20
    min_activation_rate: float = 0.00001
    max_activation_rate: float = 0.10
    min_competitive_cross_sections: int = 20
    min_competitive_active_row_share: float = 0.10


@dataclass(frozen=True, slots=True)
class StateSupportThresholds:
    min_valid_rows: int = 10_000
    min_dates: int = 20
    min_symbols: int = 100
    min_categories: int = 2
    min_transitions: int = 1_000
    min_entropy_bits: float = 0.50
    max_category_share: float = 0.98


EVENT_COLUMNS = ("evt_uplimit_active", "evt_uplimit_type_code")
RAW_STATE_COLUMNS = (
    "intraday_ret_from_open", "ret_1m", "close", "high", "low",
)
REQUIRED_COLUMNS = ("code", "trade_time", *EVENT_COLUMNS, *RAW_STATE_COLUMNS)


def _quantile_from_histogram(histogram: Mapping[int, int], quantile: float) -> float | None:
    total = sum(int(count) for count in histogram.values())
    if total <= 0:
        return None
    target = max(1, int(math.ceil(float(quantile) * total)))
    seen = 0
    for value, count in sorted((int(value), int(count)) for value, count in histogram.items()):
        seen += count
        if seen >= target:
            return float(value)
    return float(max(histogram))


def _entropy_bits(counts: Mapping[int, int]) -> float:
    total = sum(int(count) for count in counts.values())
    if total <= 0:
        return 0.0
    probabilities = [count / total for count in counts.values() if count > 0]
    return float(-sum(probability * math.log2(probability) for probability in probabilities))


def _duration_histogram(group_key: pd.Series, values: pd.Series, valid: pd.Series) -> Counter[int]:
    previous = values.groupby(group_key, sort=False).shift(1)
    changed = (~valid) | previous.isna() | values.ne(previous)
    run_id = changed.groupby(group_key, sort=False).cumsum()
    runs = (
        pd.DataFrame({"group_key": group_key, "run_id": run_id, "valid": valid})
        .loc[valid]
        .groupby(["group_key", "run_id"], sort=False)
        .size()
    )
    return Counter(int(value) for value in runs.to_numpy())


def _state_sources(frame: pd.DataFrame) -> dict[str, pd.Series]:
    close = pd.to_numeric(frame["close"], errors="coerce")
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    return {
        "intraday_return_sign": np.sign(
            pd.to_numeric(frame["intraday_ret_from_open"], errors="coerce")
        ),
        "one_minute_return_sign": np.sign(pd.to_numeric(frame["ret_1m"], errors="coerce")),
        "candle_position_sign": np.sign(2.0 * close - high - low),
    }


def _new_accumulator() -> dict:
    return {
        "rows": 0,
        "codes": set(),
        "dates": set(),
        "event": {
            "valid_rows": 0,
            "active_rows": 0,
            "active_dates": set(),
            "active_symbols": set(),
            "enter_transitions": 0,
            "exit_transitions": 0,
            "episode_durations": Counter(),
            "type_counts": Counter(),
            "cross_section_active_counts": Counter(),
        },
        "states": {
            name: {
                "valid_rows": 0,
                "dates": set(),
                "symbols": set(),
                "category_counts": Counter(),
                "transitions": 0,
                "entry_counts": Counter(),
                "duration_histogram": Counter(),
            }
            for name in (
                "intraday_return_sign", "one_minute_return_sign", "candle_position_sign"
            )
        },
    }


def _consume_frame(accumulator: dict, source: pd.DataFrame) -> None:
    missing = sorted(set(REQUIRED_COLUMNS) - set(source.columns))
    if missing:
        raise ValueError(f"event/state support frame missing columns: {missing}")
    frame = source.loc[:, REQUIRED_COLUMNS].copy()
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="raise", format="mixed")
    if frame.duplicated(["code", "trade_time"]).any():
        raise ValueError("duplicate event/state audit coordinates")
    frame = frame.sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
    dates = frame["trade_time"].dt.normalize()
    codes = frame["code"].astype(str)
    session_key = codes + "|" + dates.astype(str)
    accumulator["rows"] += len(frame)
    accumulator["codes"].update(codes.unique())
    accumulator["dates"].update(str(value.date()) for value in dates.unique())

    event_numeric = pd.to_numeric(frame["evt_uplimit_active"], errors="coerce")
    event_valid = event_numeric.notna()
    event = event_numeric.eq(1.0)
    event_previous = event_numeric.groupby(session_key, sort=False).shift(1)
    event_acc = accumulator["event"]
    event_acc["valid_rows"] += int(event_valid.sum())
    event_acc["active_rows"] += int(event.sum())
    event_acc["active_dates"].update(str(value.date()) for value in dates[event].unique())
    event_acc["active_symbols"].update(codes[event].unique())
    event_acc["enter_transitions"] += int((event & event_previous.eq(0.0)).sum())
    event_acc["exit_transitions"] += int((~event & event_valid & event_previous.eq(1.0)).sum())
    # Only retain durations from active episodes, not inactive stretches.
    run_changed = event_numeric.groupby(session_key, sort=False).shift(1).ne(event_numeric)
    run_id = run_changed.groupby(session_key, sort=False).cumsum()
    active_runs = (
        pd.DataFrame({"session_key": session_key, "run_id": run_id, "active": event})
        .loc[event]
        .groupby(["session_key", "run_id"], sort=False)
        .size()
    )
    event_acc["episode_durations"].update(int(value) for value in active_runs.to_numpy())
    event_acc["type_counts"].update(
        int(value)
        for value in pd.to_numeric(frame.loc[event, "evt_uplimit_type_code"], errors="coerce")
        .dropna()
        .to_numpy()
    )
    cross_counts = event.groupby(frame["trade_time"], sort=False).sum().astype(int)
    event_acc["cross_section_active_counts"].update(
        {int(active_count): int(count) for active_count, count in cross_counts.value_counts().items()}
    )

    for name, values in _state_sources(frame).items():
        values = pd.Series(values, index=frame.index, dtype=float)
        valid = values.notna()
        previous = values.groupby(session_key, sort=False).shift(1)
        transitions = valid & previous.notna() & values.ne(previous)
        state_acc = accumulator["states"][name]
        state_acc["valid_rows"] += int(valid.sum())
        state_acc["dates"].update(str(value.date()) for value in dates[valid].unique())
        state_acc["symbols"].update(codes[valid].unique())
        state_acc["category_counts"].update(
            int(value) for value in values.loc[valid].to_numpy()
        )
        state_acc["transitions"] += int(transitions.sum())
        state_acc["entry_counts"].update(
            int(value) for value in values.loc[transitions].to_numpy()
        )
        state_acc["duration_histogram"].update(_duration_histogram(session_key, values, valid))


def audit_event_state_support(
    frames: Iterable[pd.DataFrame],
    *,
    event_thresholds: EventSupportThresholds | None = None,
    state_thresholds: StateSupportThresholds | None = None,
) -> dict:
    """Audit observable support only; no label or performance column is accepted or used."""

    event_thresholds = event_thresholds or EventSupportThresholds()
    state_thresholds = state_thresholds or StateSupportThresholds()
    accumulator = _new_accumulator()
    seen_codes: set[str] = set()
    frame_count = 0
    for frame in frames:
        incoming_codes = set(frame["code"].astype(str).unique())
        overlap = seen_codes & incoming_codes
        if overlap:
            raise ValueError(f"codes span multiple audit chunks: {sorted(overlap)[:5]}")
        seen_codes.update(incoming_codes)
        _consume_frame(accumulator, frame)
        frame_count += 1
    if frame_count == 0:
        raise ValueError("event/state support audit received no frames")

    event = accumulator["event"]
    valid_rows = int(event["valid_rows"])
    active_rows = int(event["active_rows"])
    activation_rate = active_rows / valid_rows if valid_rows else 0.0
    competitive_sections = sum(
        int(count) for active_count, count in event["cross_section_active_counts"].items()
        if int(active_count) >= 2
    )
    competitive_active_rows = sum(
        int(active_count) * int(count)
        for active_count, count in event["cross_section_active_counts"].items()
        if int(active_count) >= 2
    )
    competitive_share = competitive_active_rows / active_rows if active_rows else 0.0
    event_checks = {
        "active_rows": active_rows >= event_thresholds.min_active_rows,
        "active_dates": len(event["active_dates"]) >= event_thresholds.min_active_dates,
        "active_symbols": len(event["active_symbols"]) >= event_thresholds.min_active_symbols,
        "enter_transitions": int(event["enter_transitions"]) >= event_thresholds.min_enter_transitions,
        "exit_transitions": int(event["exit_transitions"]) >= event_thresholds.min_exit_transitions,
        "episode_count": sum(event["episode_durations"].values()) >= event_thresholds.min_episode_count,
        "activation_rate_floor": activation_rate >= event_thresholds.min_activation_rate,
        "activation_rate_ceiling": activation_rate <= event_thresholds.max_activation_rate,
        "competitive_cross_sections": competitive_sections >= event_thresholds.min_competitive_cross_sections,
        "competitive_active_row_share": competitive_share >= event_thresholds.min_competitive_active_row_share,
    }
    entry_checks = {
        key: value for key, value in event_checks.items() if key != "exit_transitions"
    }
    entry_operational = all(entry_checks.values())
    exit_reseal_operational = bool(event_checks["exit_transitions"])
    event_report = {
        "decision": (
            "EVENT_SUPPORT_OPERATIONAL"
            if entry_operational and exit_reseal_operational
            else "EVENT_SUPPORT_PARTIAL_ENTRY_ONLY"
            if entry_operational
            else "EVENT_SUPPORT_NOT_OPERATIONAL"
        ),
        "entry_path_decision": (
            "EVENT_ENTRY_PATH_OPERATIONAL" if entry_operational else "EVENT_ENTRY_PATH_NOT_OPERATIONAL"
        ),
        "exit_reseal_decision": (
            "EVENT_EXIT_RESEAL_OPERATIONAL"
            if exit_reseal_operational
            else "EVENT_EXIT_RESEAL_NOT_OPERATIONAL"
        ),
        "checks": event_checks,
        "valid_rows": valid_rows,
        "active_rows": active_rows,
        "activation_rate": activation_rate,
        "active_date_count": len(event["active_dates"]),
        "active_symbol_count": len(event["active_symbols"]),
        "enter_transition_count": int(event["enter_transitions"]),
        "exit_transition_count": int(event["exit_transitions"]),
        "episode_count": int(sum(event["episode_durations"].values())),
        "episode_duration_p50": _quantile_from_histogram(event["episode_durations"], 0.50),
        "episode_duration_p95": _quantile_from_histogram(event["episode_durations"], 0.95),
        "episode_duration_max": max(event["episode_durations"], default=None),
        "competitive_cross_section_count": competitive_sections,
        "competitive_active_row_share": competitive_share,
        "max_active_in_cross_section": max(event["cross_section_active_counts"], default=0),
        "active_type_counts": {str(key): int(value) for key, value in sorted(event["type_counts"].items())},
    }

    state_reports: dict[str, dict] = {}
    for name, state in accumulator["states"].items():
        counts = state["category_counts"]
        total = sum(counts.values())
        max_share = max(counts.values(), default=0) / total if total else 1.0
        entropy = _entropy_bits(counts)
        checks = {
            "valid_rows": int(state["valid_rows"]) >= state_thresholds.min_valid_rows,
            "dates": len(state["dates"]) >= state_thresholds.min_dates,
            "symbols": len(state["symbols"]) >= state_thresholds.min_symbols,
            "categories": len(counts) >= state_thresholds.min_categories,
            "transitions": int(state["transitions"]) >= state_thresholds.min_transitions,
            "entropy": entropy >= state_thresholds.min_entropy_bits,
            "category_concentration": max_share <= state_thresholds.max_category_share,
        }
        state_reports[name] = {
            "decision": "STATE_SOURCE_OPERATIONAL" if all(checks.values()) else "STATE_SOURCE_NOT_OPERATIONAL",
            "checks": checks,
            "valid_rows": int(state["valid_rows"]),
            "date_count": len(state["dates"]),
            "symbol_count": len(state["symbols"]),
            "category_counts": {str(key): int(value) for key, value in sorted(counts.items())},
            "category_entropy_bits": entropy,
            "top_category_share": max_share,
            "transition_count": int(state["transitions"]),
            "transition_entry_counts": {
                str(key): int(value) for key, value in sorted(state["entry_counts"].items())
            },
            "duration_p50": _quantile_from_histogram(state["duration_histogram"], 0.50),
            "duration_p95": _quantile_from_histogram(state["duration_histogram"], 0.95),
            "duration_max": max(state["duration_histogram"], default=None),
        }
    operational_states = [
        name for name, report in state_reports.items()
        if report["decision"] == "STATE_SOURCE_OPERATIONAL"
    ]
    return {
        "audit_version": AUDIT_VERSION,
        "state_source_version": STATE_SOURCE_VERSION,
        "data_role": "development",
        "performance_columns_used": [],
        "frame_count": frame_count,
        "row_count": int(accumulator["rows"]),
        "date_count": len(accumulator["dates"]),
        "symbol_count": len(accumulator["codes"]),
        "thresholds": {
            "event": asdict(event_thresholds),
            "state": asdict(state_thresholds),
        },
        "event": event_report,
        "states": state_reports,
        "state_system_decision": (
            "STATE_GENERATOR_OPERATIONAL" if operational_states else "STATE_GENERATOR_NOT_OPERATIONAL"
        ),
        "operational_state_sources": operational_states,
    }
