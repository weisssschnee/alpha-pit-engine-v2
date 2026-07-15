"""Deterministic helpers for the compositional session/PIT sketch panel."""

from __future__ import annotations

import hashlib
from typing import Any, Iterable, Mapping

import pandas as pd


SESSION_PANEL_VERSION = "cn_compositional_session_signal_panel_v1"


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
