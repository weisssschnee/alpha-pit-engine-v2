"""Deterministic helpers for the compositional session/PIT sketch panel."""

from __future__ import annotations

import hashlib
from typing import Any, Iterable, Mapping

import pandas as pd


SESSION_PANEL_VERSION = "cn_compositional_session_signal_panel_v1"
SESSION_COORDINATE_VERSION = "cn_compositional_session_coordinates_v2_all_development"


def field_partition(field_id: str, partition_count: int) -> int:
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    digest = hashlib.sha256(
        f"{SESSION_PANEL_VERSION}|{field_id}".encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "big") % partition_count


def build_session_coordinate_rows(
    active_rows: Iterable[Mapping[str, Any]],
    *,
    available_keys: set[tuple[str, pd.Timestamp]],
) -> list[dict[str, Any]]:
    """Collapse intraday A/B coordinates to one stock-session coordinate."""

    deduped: dict[tuple[str, str, pd.Timestamp], dict[str, Any]] = {}
    for raw in active_rows:
        coordinate_set = str(raw.get("coordinate_set") or "")
        if coordinate_set not in {"A", "B"}:
            continue
        code = str(raw.get("code") or "")
        session = pd.Timestamp(raw.get("trade_date") or raw.get("trade_time")).normalize()
        if not code or pd.isna(session):
            raise ValueError("session coordinates require code and trade date")
        key = (coordinate_set, code, session)
        row = deduped.setdefault(
            key,
            {
                "coordinate_set": coordinate_set,
                "code": code,
                "trade_date": session.strftime("%Y-%m-%d"),
                "trade_month": session.strftime("%Y-%m"),
                "stock_coverage_interval": str(raw.get("stock_coverage_interval") or ""),
                "listing_age_bucket": str(raw.get("listing_age_bucket") or ""),
                "activation_density_bucket": str(raw.get("activation_density_bucket") or ""),
                "data_role": "development",
                "selection_uses_labels_or_performance": False,
            },
        )
        row["trade_time"] = (session + pd.Timedelta(hours=15)).isoformat()

    rows: list[dict[str, Any]] = []
    for (coordinate_set, code, session), row in sorted(deduped.items()):
        row["coordinate_id"] = hashlib.sha256(
            f"{SESSION_PANEL_VERSION}|{coordinate_set}|{code}|{session.date()}".encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        row["coordinate_version"] = SESSION_PANEL_VERSION
        row["row_index"] = -1 if (code, session) not in available_keys else 0
        rows.append(row)
    return rows


def build_full_session_coordinate_rows(
    active_rows: Iterable[Mapping[str, Any]],
    *,
    sessions: pd.DatetimeIndex,
    available_keys: set[tuple[str, pd.Timestamp]],
) -> list[dict[str, Any]]:
    """Use every development session, alternating A/B within each month."""

    metadata: dict[str, dict[str, str]] = {}
    for raw in active_rows:
        code = str(raw.get("code") or "")
        if code:
            metadata.setdefault(
                code,
                {
                    "stock_coverage_interval": str(raw.get("stock_coverage_interval") or ""),
                    "activation_density_bucket": str(raw.get("activation_density_bucket") or ""),
                },
            )
    normalized_sessions = pd.DatetimeIndex(pd.to_datetime(sessions)).normalize().sort_values()
    if not metadata or normalized_sessions.empty:
        raise ValueError("full session coordinates require stocks and development sessions")
    assignment: dict[pd.Timestamp, str] = {}
    session_frame = pd.DataFrame({"session": normalized_sessions})
    session_frame["month"] = session_frame["session"].dt.strftime("%Y-%m")
    for month, group in session_frame.groupby("month", sort=True):
        phase = int(hashlib.sha256(f"{SESSION_COORDINATE_VERSION}|{month}".encode()).hexdigest(), 16) % 2
        for index, session in enumerate(group["session"]):
            assignment[pd.Timestamp(session)] = "A" if (index + phase) % 2 == 0 else "B"
    available_by_code: dict[str, list[pd.Timestamp]] = {}
    for code, session in available_keys:
        available_by_code.setdefault(code, []).append(pd.Timestamp(session).normalize())
    data_start = normalized_sessions.min()
    rows: list[dict[str, Any]] = []
    for code in sorted(metadata):
        first_available = min(available_by_code.get(code, [data_start]))
        for session in normalized_sessions:
            coordinate_set = assignment[pd.Timestamp(session)]
            age = int((pd.Timestamp(session) - first_available).days)
            if first_available <= data_start + pd.Timedelta(days=7):
                listing_bucket = "left_censored_252plus"
            elif age <= 60:
                listing_bucket = "age_0_60d"
            elif age <= 252:
                listing_bucket = "age_61_252d"
            else:
                listing_bucket = "age_253plus_d"
            row = {
                "coordinate_id": hashlib.sha256(
                    f"{SESSION_COORDINATE_VERSION}|{coordinate_set}|{code}|{session.date()}".encode()
                ).hexdigest()[:24],
                "coordinate_version": SESSION_COORDINATE_VERSION,
                "coordinate_set": coordinate_set,
                "code": code,
                "trade_time": (pd.Timestamp(session) + pd.Timedelta(hours=15)).isoformat(),
                "trade_date": pd.Timestamp(session).strftime("%Y-%m-%d"),
                "trade_month": pd.Timestamp(session).strftime("%Y-%m"),
                "intraday_period": "session_close",
                "stock_coverage_interval": metadata[code]["stock_coverage_interval"],
                "listing_age_bucket": listing_bucket,
                "activation_density_bucket": metadata[code]["activation_density_bucket"],
                "row_index": 0 if (code, pd.Timestamp(session)) in available_keys else -1,
                "data_role": "development",
                "selection_uses_labels_or_performance": False,
                "coordinate_selection_policy": "ALL_DEVELOPMENT_SESSIONS_MONTHLY_ALTERNATING_AB",
            }
            rows.append(row)
    return sorted(rows, key=lambda row: (row["coordinate_set"], row["coordinate_id"]))


def attach_coordinate_row_indices(
    rows: Iterable[Mapping[str, Any]], panel: pd.DataFrame
) -> list[dict[str, Any]]:
    lookup = {
        (str(code), pd.Timestamp(trade_time).normalize()): index
        for index, (code, trade_time) in enumerate(
            zip(panel["code"], panel["trade_time"], strict=True)
        )
    }
    output: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        key = (str(row["code"]), pd.Timestamp(row["trade_time"]).normalize())
        row["row_index"] = int(lookup.get(key, -1))
        output.append(row)
    return output
