"""Augment true-1min shards with compact sidecar context fields.

This route runs on the machine that owns the true-1min shard root. It reads a
Phase3CS compact sidecar pack and writes a new shard root with the same panel
layout expected by the true1min searcher.
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


REPO = Path(__file__).resolve().parents[3]
DEFAULT_INPUT_ROOT = Path("runtime/phase3au_aq_only_true1min_sharded_20260611")
DEFAULT_SIDECAR_ROOT = Path("runtime/phase3cs_true1min_sidecar_pack_20260624")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cs_true1min_sidecar_augmented_shards_20260624")
DEFAULT_REPORT_ROOT = Path("reports/phase3cs_true1min_sidecar_augmented_shards_20260624")
PANEL_REL = Path("phase3aq_wide_true1min/canary/phase3aq_true_1min_formula_canary.parquet")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_pack(sidecar_root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    stock = pd.read_parquet(sidecar_root / "phase3cs_stock_lagged_context.parquet")
    market = pd.read_parquet(sidecar_root / "phase3cs_market_lagged_context.parquet")
    event = pd.read_parquet(sidecar_root / "phase3cs_stock_event_context.parquet")
    for frame in (stock, market, event):
        if "source_date" in frame:
            frame["source_dt"] = pd.to_datetime(frame["source_date"], errors="coerce")
        if "code" in frame:
            frame["code"] = frame["code"].astype(str)
    stock = stock.dropna(subset=["code", "source_dt"]) if {"code", "source_dt"}.issubset(stock.columns) else stock
    market = market.dropna(subset=["source_dt"]) if "source_dt" in market.columns else market
    event = event.dropna(subset=["code", "source_dt"]) if {"code", "source_dt"}.issubset(event.columns) else event
    return stock, market, event


def _discover_panels(input_root: Path, max_shards: int | None) -> list[Path]:
    panels = sorted(input_root.glob(f"shard_*/{PANEL_REL.as_posix()}"))
    if max_shards is not None and max_shards > 0:
        panels = panels[:max_shards]
    if not panels:
        raise FileNotFoundError(f"no true1min panels under {input_root}")
    return panels


def _merge_stock_asof(keys: pd.DataFrame, stock: pd.DataFrame) -> pd.DataFrame:
    if stock.empty:
        return keys.copy()
    value_cols = [col for col in stock.columns if col not in {"code", "source_date", "source_dt"}]
    if not value_cols:
        return keys.copy()
    out_parts: list[pd.DataFrame] = []
    stock = stock[["code", "source_dt", *value_cols]].sort_values(["code", "source_dt"])
    for code, left in keys.sort_values(["code", "exec_dt"]).groupby("code", sort=False):
        right = stock[stock["code"].eq(code)].sort_values("source_dt")
        if right.empty:
            part = left.copy()
            for col in value_cols:
                part[col] = np.nan
        else:
            part = pd.merge_asof(
                left.sort_values("exec_dt"),
                right.drop(columns=["code"]),
                left_on="exec_dt",
                right_on="source_dt",
                direction="backward",
                allow_exact_matches=False,
            )
            part["code"] = code
        out_parts.append(part)
    merged = pd.concat(out_parts, ignore_index=True) if out_parts else keys.copy()
    return merged.drop(columns=["source_dt"], errors="ignore")


def _merge_market_asof(keys: pd.DataFrame, market: pd.DataFrame) -> pd.DataFrame:
    if market.empty:
        return keys.copy()
    value_cols = [col for col in market.columns if col not in {"source_date", "source_dt"}]
    if not value_cols:
        return keys.copy()
    return pd.merge_asof(
        keys.sort_values("exec_dt"),
        market[["source_dt", *value_cols]].sort_values("source_dt"),
        left_on="exec_dt",
        right_on="source_dt",
        direction="backward",
        allow_exact_matches=False,
    ).drop(columns=["source_dt"], errors="ignore")


def _trade_minute(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return (dt.dt.hour * 60 + dt.dt.minute).astype("float32")


def _augment_one_panel(panel: Path, input_root: Path, output_root: Path, stock: pd.DataFrame, market: pd.DataFrame, event: pd.DataFrame) -> dict[str, Any]:
    frame = pd.read_parquet(panel)
    frame["code"] = frame["code"].astype(str)
    frame["exec_date"] = frame["exec_date"].astype(str)
    frame["exec_dt"] = pd.to_datetime(frame["exec_date"], errors="coerce")
    keys = frame[["code", "exec_date", "exec_dt"]].drop_duplicates().reset_index(drop=True)

    stock_join = _merge_stock_asof(keys, stock)
    market_keys = keys[["exec_date", "exec_dt"]].drop_duplicates().reset_index(drop=True)
    market_join = _merge_market_asof(market_keys, market)
    add_cols_stock = [col for col in stock_join.columns if col not in {"code", "exec_date", "exec_dt"}]
    add_cols_market = [col for col in market_join.columns if col not in {"exec_date", "exec_dt"}]
    if add_cols_stock:
        frame = frame.merge(stock_join[["code", "exec_date", *add_cols_stock]], on=["code", "exec_date"], how="left")
    if add_cols_market:
        frame = frame.merge(market_join[["exec_date", *add_cols_market]], on="exec_date", how="left")

    event_cols = [col for col in event.columns if col not in {"code", "source_date", "source_dt"}]
    if event_cols:
        event_join = event.copy()
        event_join["exec_date"] = event_join["source_dt"].dt.strftime("%Y-%m-%d")
        event_join = event_join[["code", "exec_date", *event_cols]].drop_duplicates(["code", "exec_date"], keep="first")
        frame = frame.merge(event_join, on=["code", "exec_date"], how="left")
        trade_minute = _trade_minute(frame["trade_time"])
        cutoff = pd.to_numeric(frame.get("evt_uplimit_cutoff_minute"), errors="coerce")
        active = cutoff.notna() & (trade_minute >= cutoff)
        frame["evt_uplimit_active"] = active.astype("float32")
        frame["evt_uplimit_age_min"] = np.where(active, trade_minute - cutoff, np.nan).astype("float32")
        for col in event_cols:
            if col == "evt_uplimit_cutoff_minute":
                continue
            if col in frame.columns:
                frame.loc[~active, col] = np.nan

    frame = frame.drop(columns=["exec_dt"], errors="ignore")
    rel = panel.relative_to(input_root)
    out_path = output_root / rel
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), out_path, compression="zstd")
    added_cols = [col for col in frame.columns if col not in pq.ParquetFile(panel).schema_arrow.names]
    return {
        "input_panel": str(panel),
        "output_panel": str(out_path),
        "rows": int(len(frame)),
        "columns": int(len(frame.columns)),
        "added_column_count": int(len(added_cols)),
        "added_columns": added_cols,
        "event_active_rows": int(frame.get("evt_uplimit_active", pd.Series(dtype=float)).fillna(0).sum()) if "evt_uplimit_active" in frame else 0,
    }


def augment(
    *,
    input_root: Path,
    sidecar_root: Path,
    output_root: Path,
    report_root: Path,
    max_shards: int | None,
    overwrite: bool,
) -> dict[str, Any]:
    input_root = _resolve(input_root)
    sidecar_root = _resolve(sidecar_root)
    output_root = _resolve(output_root)
    report_root = _resolve(report_root)
    if output_root.exists() and overwrite:
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    stock, market, event = _load_pack(sidecar_root)
    panels = _discover_panels(input_root, max_shards)
    shard_rows = [_augment_one_panel(panel, input_root, output_root, stock, market, event) for panel in panels]
    all_added = sorted({col for row in shard_rows for col in row["added_columns"]})
    manifest = pd.DataFrame(shard_rows)
    manifest_path = output_root / "phase3cs_augmented_shard_manifest.csv"
    manifest.to_csv(manifest_path, index=False)
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3CS_TRUE1MIN_SHARDS_AUGMENTED_WITH_SIDECARS_DIAGNOSTIC_ONLY",
        "input_root": str(input_root),
        "sidecar_root": str(sidecar_root),
        "output_root": str(output_root),
        "panel_rel": str(PANEL_REL),
        "shard_count": int(len(shard_rows)),
        "added_column_count": int(len(all_added)),
        "added_columns": all_added,
        "event_active_rows": int(sum(row["event_active_rows"] for row in shard_rows)),
        "stock_context_rows": int(len(stock)),
        "market_context_rows": int(len(market)),
        "event_context_rows": int(len(event)),
        "manifest": str(manifest_path),
        "hard_rules": [
            "ctx_* sidecars are previous-available only via source_date < exec_date",
            "evt_uplimit_* sidecars are same-day but hidden until trade_time >= cutoff minute",
            "original shard root is not modified",
            "output layout is compatible with Phase3BP _discover_panels",
        ],
    }
    _write_json(output_root / "phase3cs_augmented_shard_summary.json", summary)
    _write_json(report_root / "phase3cs_augmented_shard_summary.json", summary)
    report_lines = [
        "# Phase3CS True1min Sidecar Augmented Shards 2026-06-24",
        "",
        f"decision: `{summary['decision']}`",
        "",
        f"- shard_count: `{summary['shard_count']}`",
        f"- added_column_count: `{summary['added_column_count']}`",
        f"- event_active_rows: `{summary['event_active_rows']}`",
        "",
        "## Hard Rules",
        "",
    ]
    report_lines.extend(f"- {rule}" for rule in summary["hard_rules"])
    (report_root / "PHASE3CS_TRUE1MIN_SIDECAR_AUGMENTED_SHARDS_20260624.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--sidecar-root", type=Path, default=DEFAULT_SIDECAR_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-shards", type=int, default=4)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    augment(
        input_root=args.input_root,
        sidecar_root=args.sidecar_root,
        output_root=args.output_root,
        report_root=args.report_root,
        max_shards=args.max_shards,
        overwrite=args.overwrite,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
