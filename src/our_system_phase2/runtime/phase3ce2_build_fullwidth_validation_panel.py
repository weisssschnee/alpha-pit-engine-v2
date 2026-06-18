from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SHARD_MANIFEST = REPO_ROOT / "runtime/phase3au_aq_only_true1min_sharded_20260611/phase3au_aq_only_shard_manifest.csv"
DEFAULT_EVENT_DERIVED_PANEL = REPO_ROOT / "runtime/phase3ce2_event_derived_daily_panel_202406_202512/phase3ce2_event_derived_daily_panel.parquet"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "runtime/phase3ce2_fullwidth_validation_panel_20260618"
DEFAULT_REPORT_ROOT = REPO_ROOT / "reports/phase3ce2_fullwidth_validation_panel_20260618"


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def _select_event_dates(event_panel: Path, *, date_count: int, start_date: str, end_date: str) -> tuple[list[str], list[str], pd.DataFrame]:
    fields = [
        "date",
        "is_market_high_board",
        "limit_up_any_close_not_open_in_t2",
        "limit_up_any_close_not_open_in_t10",
        "limit_up_any_open_not_close_in_t2",
        "limit_up_any_open_not_close_in_t10",
    ]
    df = pd.read_parquet(event_panel, columns=fields)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()
    for field in fields[1:]:
        df[field] = pd.to_numeric(df[field], errors="coerce").fillna(0.0)
    grouped = df.groupby("date", as_index=False)[fields[1:]].sum()
    grouped["event_score"] = (
        grouped["is_market_high_board"] * 25.0
        + grouped["limit_up_any_open_not_close_in_t2"] * 5.0
        + grouped["limit_up_any_open_not_close_in_t10"] * 2.0
        + grouped["limit_up_any_close_not_open_in_t2"] * 3.0
        + grouped["limit_up_any_close_not_open_in_t10"]
    )
    grouped = grouped.sort_values(["event_score", "date"], ascending=[False, True]).reset_index(drop=True)
    selected = sorted(grouped.head(date_count)["date"].tolist())
    calendar = sorted(df["date"].dropna().unique().tolist())
    previous: set[str] = set()
    calendar_index = {date: idx for idx, date in enumerate(calendar)}
    for date in selected:
        idx = calendar_index.get(date)
        if idx is not None and idx > 0:
            previous.add(calendar[idx - 1])
    read_dates = sorted(set(selected) | previous)
    return selected, read_dates, grouped


def _read_shard(panel_path: Path, read_dates: list[str]) -> pd.DataFrame:
    table = pq.read_table(
        panel_path,
        filters=[("exec_date", "in", read_dates)],
    )
    frame = table.to_pandas()
    if frame.empty:
        return frame
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["exec_date"] = frame["exec_date"].astype(str)
    frame["code"] = frame["code"].astype(str)
    return frame


def build(
    *,
    shard_manifest: Path,
    event_derived_panel: Path,
    output_root: Path,
    report_root: Path,
    date_count: int,
    start_date: str,
    end_date: str,
    max_shards: int | None,
) -> dict[str, Any]:
    shard_manifest = _resolve(shard_manifest)
    event_derived_panel = _resolve(event_derived_panel)
    output_root = _resolve(output_root)
    report_root = _resolve(report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    selected_dates, read_dates, event_rank = _select_event_dates(
        event_derived_panel,
        date_count=date_count,
        start_date=start_date,
        end_date=end_date,
    )

    manifest = pd.read_csv(shard_manifest)
    manifest = manifest[manifest["exists"].astype(str).str.lower().eq("true")].copy()
    manifest["shard_index"] = pd.to_numeric(manifest["shard_index"], errors="coerce").astype(int)
    manifest = manifest.sort_values("shard_index")
    if max_shards is not None:
        manifest = manifest.head(max_shards)

    frames: list[pd.DataFrame] = []
    shard_rows: list[dict[str, Any]] = []
    for row in manifest.to_dict("records"):
        panel_path = Path(row["panel_path"])
        part = _read_shard(panel_path, read_dates)
        frames.append(part)
        shard_rows.append(
            {
                "shard": row["shard"],
                "shard_index": int(row["shard_index"]),
                "panel_path": str(panel_path),
                "rows": int(len(part)),
                "code_count": int(part["code"].nunique()) if not part.empty else 0,
                "trade_time_count": int(part["trade_time"].nunique()) if not part.empty else 0,
            }
        )

    if frames:
        out = pd.concat(frames, ignore_index=True)
    else:
        out = pd.DataFrame()
    if not out.empty:
        out = out.sort_values(["trade_time", "code"]).reset_index(drop=True)

    output_path = output_root / "phase3ce2_fullwidth_true1min_validation_panel.parquet"
    table = pa.Table.from_pandas(out, preserve_index=False)
    pq.write_table(table, output_path, compression="zstd")

    top_dates_path = report_root / "phase3ce2_fullwidth_validation_event_date_ranking.csv"
    event_rank.head(max(date_count * 5, 50)).to_csv(top_dates_path, index=False)
    shard_report_path = report_root / "phase3ce2_fullwidth_validation_shard_rows.csv"
    pd.DataFrame(shard_rows).to_csv(shard_report_path, index=False)

    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3CE2_FULLWIDTH_TRUE1MIN_VALIDATION_PANEL_BUILT",
        "input_grain": "trade_time_1min",
        "note": "full-width selected-date true-1min rows; no 1D fallback and no minute resampling",
        "shard_manifest": str(shard_manifest),
        "event_derived_panel": str(event_derived_panel),
        "selected_event_dates": selected_dates,
        "read_dates_including_previous_close_dates": read_dates,
        "shard_count": int(len(manifest)),
        "rows": int(len(out)),
        "code_count": int(out["code"].nunique()) if not out.empty else 0,
        "trade_time_count": int(out["trade_time"].nunique()) if not out.empty else 0,
        "date_min": None if out.empty else str(out["exec_date"].min()),
        "date_max": None if out.empty else str(out["exec_date"].max()),
        "output_path": str(output_path),
        "reports": {
            "event_date_ranking": str(top_dates_path),
            "shard_rows": str(shard_report_path),
        },
    }
    summary_path = report_root / "phase3ce2_fullwidth_validation_panel_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a full-width selected-date true-1min panel for Phase3CE2 validation.")
    parser.add_argument("--shard-manifest", type=Path, default=DEFAULT_SHARD_MANIFEST)
    parser.add_argument("--event-derived-panel", type=Path, default=DEFAULT_EVENT_DERIVED_PANEL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--date-count", type=int, default=3)
    parser.add_argument("--start-date", default="2025-01-01")
    parser.add_argument("--end-date", default="2025-12-31")
    parser.add_argument("--max-shards", type=int, default=None)
    args = parser.parse_args()
    build(
        shard_manifest=args.shard_manifest,
        event_derived_panel=args.event_derived_panel,
        output_root=args.output_root,
        report_root=args.report_root,
        date_count=args.date_count,
        start_date=args.start_date,
        end_date=args.end_date,
        max_shards=args.max_shards,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
