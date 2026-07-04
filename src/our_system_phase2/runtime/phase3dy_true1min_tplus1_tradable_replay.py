"""Replay Phase3DW follow-ups on true-1min sidecar shards with T+1 tradability.

This route is intentionally narrow:

* evaluate expressions on true minute rows, not on the old daily panel;
* collapse each code/date to an after-close daily signal;
* run long-only top-bucket T+1 replay with entry limit-up blocking;
* report open/open and close/close variants separately.

It is a research audit, not a promotion gate. The current default shard root is
the partial Phase3CY sidecar-augmented canary root.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.real_market_validation import (
    UnsupportedExpressionError,
    evaluate_panel_expression,
)


REPO = Path(__file__).resolve().parents[3]
DEFAULT_INPUT = Path(r"G:\Chengbo\runtime\phase3dw_recovery_20260702\phase3cm_train_reward_recovered_6144.csv")
DEFAULT_SHARD_ROOT = Path("runtime/phase3cy_true1min_sidecar_augmented_shards_20260626")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3dy_true1min_tplus1_tradable_replay_20260702")
DEFAULT_REPORT_ROOT = Path("reports/phase3dy_true1min_tplus1_tradable_replay_20260702")
PANEL_REL = Path("phase3aq_wide_true1min/canary/phase3aq_true_1min_formula_canary.parquet")
FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")

BASE_COLUMNS = [
    "code",
    "trade_time",
    "date",
    "exec_date",
    "open",
    "high",
    "low",
    "close",
    "amount",
    "volume",
    "vol",
    "vwap",
    "evt_uplimit_active",
]


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sortino(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return None
    downside = clean[clean < 0]
    if downside.empty:
        return round(float(clean.mean()), 6)
    downside_std = float(downside.std(ddof=0))
    if downside_std <= 0:
        return None
    return round(float(clean.mean() / downside_std * math.sqrt(len(clean))), 6)


def _quarter_label(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return "unknown"
    quarter = ((int(dt.month) - 1) // 3) + 1
    return f"{dt.year}Q{quarter}"


def _extract_fields(expression: str) -> list[str]:
    return sorted(set(FIELD_RE.findall(str(expression or ""))))


def _load_followups(path: Path, decision: str, limit: int | None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"input not found: {path}")
    frame = pd.read_csv(path)
    if "train_reward_decision" in frame.columns:
        frame = frame[frame["train_reward_decision"].astype(str).eq(decision)].copy()
    else:
        frame = frame.iloc[0:0].copy()
    if "train_reward" in frame.columns:
        frame["_sort_reward"] = pd.to_numeric(frame["train_reward"], errors="coerce").fillna(-1e12)
        frame = frame.sort_values("_sort_reward", ascending=False).drop(columns=["_sort_reward"])
    if limit is not None and limit > 0:
        frame = frame.head(limit)
    if "candidate_id" not in frame.columns:
        frame["candidate_id"] = [f"candidate_{idx:05d}" for idx in range(len(frame))]
    return frame.reset_index(drop=True)


def _discover_panels(root: Path, max_shards: int | None) -> list[Path]:
    panels = sorted(root.glob(f"shard_*/{PANEL_REL.as_posix()}"))
    if max_shards is not None and max_shards > 0:
        panels = panels[:max_shards]
    if not panels:
        raise FileNotFoundError(f"no true1min sidecar panels under {root}")
    return panels


def _panel_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _read_panel(path: Path, required_fields: set[str]) -> pd.DataFrame:
    available = set(_panel_columns(path))
    columns = [col for col in BASE_COLUMNS if col in available]
    columns.extend(sorted(field for field in required_fields if field in available and field not in columns))
    missing_base = [col for col in ("code", "trade_time", "open", "high", "low", "close") if col not in columns]
    if missing_base:
        raise ValueError(f"panel {path} missing required base columns: {missing_base}")
    frame = pd.read_parquet(path, columns=columns)
    if "exec_date" not in frame.columns:
        frame["exec_date"] = frame.get("date")
    if "volume" not in frame.columns and "vol" in frame.columns:
        frame["volume"] = frame["vol"]
    if "amount" not in frame.columns:
        frame["amount"] = np.nan
    if "vwap" not in frame.columns:
        amount = pd.to_numeric(frame["amount"], errors="coerce")
        volume = pd.to_numeric(frame["volume"], errors="coerce").replace(0, np.nan)
        frame["vwap"] = amount / volume
    if "evt_uplimit_active" not in frame.columns:
        frame["evt_uplimit_active"] = 0.0
    frame["code"] = frame["code"].astype(str)
    frame["exec_date"] = frame["exec_date"].astype(str)
    frame["_trade_dt"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    frame = frame.sort_values(["code", "_trade_dt"], kind="mergesort").reset_index(drop=True)
    return frame


def _daily_from_minute_frame(frame: pd.DataFrame, signal_columns: list[str]) -> pd.DataFrame:
    group_cols = ["code", "exec_date"]
    aggregations: dict[str, tuple[str, str]] = {
        "open": ("open", "first"),
        "high": ("high", "max"),
        "low": ("low", "min"),
        "close": ("close", "last"),
        "amount": ("amount", "sum"),
        "volume": ("volume", "sum"),
        "trade_time_last": ("trade_time", "last"),
        "is_limit_up": ("evt_uplimit_active", "max"),
        "minute_row_count": ("trade_time", "count"),
    }
    for col in signal_columns:
        aggregations[col] = (col, "last")
    daily = frame.groupby(group_cols, sort=False).agg(**aggregations).reset_index()
    daily["date"] = pd.to_datetime(daily["exec_date"], errors="coerce")
    daily["amount"] = pd.to_numeric(daily["amount"], errors="coerce")
    daily["volume"] = pd.to_numeric(daily["volume"], errors="coerce")
    daily["vwap"] = daily["amount"] / daily["volume"].replace(0, np.nan)
    daily["is_limit_up"] = pd.to_numeric(daily["is_limit_up"], errors="coerce").fillna(0.0) > 0.0
    return daily


def _evaluate_shard(
    panel_path: Path,
    candidates: pd.DataFrame,
    required_fields: set[str],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    frame = _read_panel(panel_path, required_fields)
    cache: dict[str, pd.Series] = {}
    signal_columns: list[str] = []
    errors: list[dict[str, Any]] = []
    for _, row in candidates.iterrows():
        candidate_id = str(row["candidate_id"])
        expression = str(row.get("expression") or "")
        signal_col = f"signal__{candidate_id}"
        try:
            frame[signal_col] = evaluate_panel_expression(frame, expression, cache=cache)
            signal_columns.append(signal_col)
        except (UnsupportedExpressionError, ValueError, ZeroDivisionError) as exc:
            errors.append(
                {
                    "candidate_id": candidate_id,
                    "panel_path": str(panel_path),
                    "error": str(exc),
                    "expression": expression,
                }
            )
    daily = _daily_from_minute_frame(frame, signal_columns)
    return daily, errors


def _future_columns(daily: pd.DataFrame, *, execution_lag_days: int, horizon_days: int) -> pd.DataFrame:
    daily = daily.sort_values(["code", "date"], kind="mergesort").reset_index(drop=True)
    grouped = daily.groupby("code", sort=False)
    daily["entry_close"] = grouped["close"].shift(-execution_lag_days)
    daily["exit_close"] = grouped["close"].shift(-(execution_lag_days + horizon_days))
    daily["entry_open"] = grouped["open"].shift(-execution_lag_days)
    daily["exit_open"] = grouped["open"].shift(-(execution_lag_days + horizon_days))
    daily["entry_limit_up"] = grouped["is_limit_up"].shift(-execution_lag_days).fillna(False).astype(bool)
    daily["ret_close_to_close"] = daily["exit_close"] / daily["entry_close"] - 1.0
    daily["ret_open_to_open"] = daily["exit_open"] / daily["entry_open"] - 1.0
    daily["ret_open_to_exit_close"] = daily["exit_close"] / daily["entry_open"] - 1.0
    return daily


def _candidate_metrics(
    daily: pd.DataFrame,
    *,
    candidate_id: str,
    expression: str,
    top_quantile: float,
    return_column: str,
) -> dict[str, Any]:
    signal_col = f"signal__{candidate_id}"
    if signal_col not in daily.columns:
        return {
            "candidate_id": candidate_id,
            "expression": expression,
            "return_column": return_column,
            "decision": "EVAL_ERROR_NO_SIGNAL_COLUMN",
        }
    work = daily[["date", "code", signal_col, return_column, "entry_limit_up"]].copy()
    work = work.rename(columns={signal_col: "signal", return_column: "forward_return"})
    work["signal"] = pd.to_numeric(work["signal"], errors="coerce")
    work["forward_return"] = pd.to_numeric(work["forward_return"], errors="coerce")
    work = work.dropna(subset=["date", "code", "signal", "forward_return"])
    raw_rows = int(len(work))
    work = work[~work["entry_limit_up"].astype(bool)].copy()

    rows: list[dict[str, Any]] = []
    prev_top: set[str] | None = None
    for date, day in work.groupby("date", sort=True):
        if len(day) < 5 or day["signal"].nunique(dropna=True) < 2:
            continue
        rank_ic = day["signal"].rank().corr(day["forward_return"].rank())
        top_n = max(1, int(math.ceil(len(day) * top_quantile)))
        top = day.sort_values(["signal", "code"], ascending=[False, True]).head(top_n)
        top_codes = set(top["code"].astype(str))
        turnover = None if prev_top is None else 1.0 - (len(top_codes & prev_top) / max(1, len(top_codes)))
        prev_top = top_codes
        rows.append(
            {
                "date": date,
                "window": _quarter_label(date),
                "rank_ic": float(rank_ic) if pd.notna(rank_ic) else None,
                "long_only_return": float(top["forward_return"].mean()),
                "selected_count": int(len(top)),
                "universe_count": int(len(day)),
                "one_way_turnover": turnover,
            }
        )
    daily_metrics = pd.DataFrame(rows)
    windows: list[dict[str, Any]] = []
    if not daily_metrics.empty:
        for window, part in daily_metrics.groupby("window", sort=True):
            rank_ic_values = pd.to_numeric(part["rank_ic"], errors="coerce").dropna()
            ret_values = pd.to_numeric(part["long_only_return"], errors="coerce").dropna()
            turnover_values = pd.to_numeric(part["one_way_turnover"], errors="coerce").dropna()
            windows.append(
                {
                    "window": str(window),
                    "trading_day_count": int(len(part)),
                    "mean_rank_ic": round(float(rank_ic_values.mean()), 6) if not rank_ic_values.empty else None,
                    "rank_ic_hit_rate": round(float((rank_ic_values > 0).mean()), 6) if not rank_ic_values.empty else None,
                    "mean_long_only_return": round(float(ret_values.mean()), 6) if not ret_values.empty else None,
                    "long_only_sortino": _sortino(ret_values),
                    "mean_one_way_turnover": round(float(turnover_values.mean()), 6) if not turnover_values.empty else None,
                    "mean_selected_count": round(float(part["selected_count"].mean()), 2),
                    "mean_universe_count": round(float(part["universe_count"].mean()), 2),
                }
            )
    ret_all = pd.to_numeric(daily_metrics.get("long_only_return", pd.Series(dtype=float)), errors="coerce").dropna()
    ic_all = pd.to_numeric(daily_metrics.get("rank_ic", pd.Series(dtype=float)), errors="coerce").dropna()
    turnover_all = pd.to_numeric(daily_metrics.get("one_way_turnover", pd.Series(dtype=float)), errors="coerce").dropna()
    blockers: list[str] = []
    if len(daily_metrics) < 20:
        blockers.append("low_daily_observation_count")
    if ret_all.empty or _sortino(ret_all) is None:
        blockers.append("missing_long_only_sortino")
    elif float(_sortino(ret_all) or 0.0) <= 0.0:
        blockers.append("non_positive_long_only_sortino")
    if turnover_all.notna().any() and float(turnover_all.mean()) > 0.8:
        blockers.append("very_high_turnover")
    return {
        "candidate_id": candidate_id,
        "expression": expression,
        "return_column": return_column,
        "decision": "HOLD_RESEARCH" if blockers else "FOLLOWUP_REPLAY_READY",
        "blockers": "|".join(blockers),
        "raw_signal_return_rows": raw_rows,
        "tradable_rows_after_entry_limit_filter": int(len(work)),
        "daily_observation_count": int(len(daily_metrics)),
        "date_min": daily_metrics["date"].min().date().isoformat() if not daily_metrics.empty else None,
        "date_max": daily_metrics["date"].max().date().isoformat() if not daily_metrics.empty else None,
        "mean_rank_ic": round(float(ic_all.mean()), 6) if not ic_all.empty else None,
        "rank_ic_hit_rate": round(float((ic_all > 0).mean()), 6) if not ic_all.empty else None,
        "mean_long_only_return": round(float(ret_all.mean()), 6) if not ret_all.empty else None,
        "long_only_sortino": _sortino(ret_all),
        "mean_one_way_turnover": round(float(turnover_all.mean()), 6) if not turnover_all.empty else None,
        "window_count": int(len(windows)),
        "windows_json": json.dumps(windows, ensure_ascii=False),
    }


def _write_markdown(path: Path, summary: dict[str, Any], metrics: pd.DataFrame) -> None:
    lines = [
        "# Phase3DY True1min T+1 Tradable Replay",
        "",
        f"decision: `{summary['decision']}`",
        "",
        "This is a research replay on true minute sidecar shards. It does not promote any candidate.",
        "",
        "## Scope",
        "",
        f"- input: `{summary['input_path']}`",
        f"- shard_root: `{summary['shard_root']}`",
        f"- panel_count: `{summary['panel_count']}`",
        f"- candidate_count: `{summary['candidate_count']}`",
        f"- daily_rows: `{summary['daily_rows']}`",
        f"- date_range: `{summary['date_min']} .. {summary['date_max']}`",
        "",
        "## Trading Contract",
        "",
    ]
    lines.extend(f"- {item}" for item in summary["trading_contract"])
    lines.extend(["", "## Best Rows", "", "| candidate | return | decision | sortino | mean ret | rank IC | turnover | blockers | expression |", "|---|---|---|---:|---:|---:|---:|---|---|"])
    top = metrics.sort_values(["long_only_sortino", "mean_long_only_return"], ascending=[False, False], na_position="last").head(30)
    for _, row in top.iterrows():
        expr = str(row.get("expression") or "").replace("|", "\\|")
        lines.append(
            f"| `{row.get('candidate_id')}` | `{row.get('return_column')}` | `{row.get('decision')}` | "
            f"{row.get('long_only_sortino')} | {row.get('mean_long_only_return')} | {row.get('mean_rank_ic')} | "
            f"{row.get('mean_one_way_turnover')} | `{row.get('blockers')}` | `{expr}` |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    input_path = Path(args.input)
    shard_root = _resolve(Path(args.shard_root))
    output_root = _resolve(Path(args.output_root))
    report_root = _resolve(Path(args.report_root))
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    candidates = _load_followups(input_path, args.decision, args.limit)
    required_fields = {field for expr in candidates.get("expression", pd.Series(dtype=str)).astype(str) for field in _extract_fields(expr)}
    panels = _discover_panels(shard_root, args.max_shards)
    daily_parts: list[pd.DataFrame] = []
    errors: list[dict[str, Any]] = []
    shard_rows: list[dict[str, Any]] = []
    for panel in panels:
        daily, panel_errors = _evaluate_shard(panel, candidates, required_fields)
        daily_parts.append(daily)
        errors.extend(panel_errors)
        shard_rows.append(
            {
                "panel_path": str(panel),
                "daily_rows": int(len(daily)),
                "date_min": daily["date"].min().date().isoformat() if not daily.empty else None,
                "date_max": daily["date"].max().date().isoformat() if not daily.empty else None,
                "missing_candidate_count": len(panel_errors),
            }
        )
    daily_all = pd.concat(daily_parts, ignore_index=True) if daily_parts else pd.DataFrame()
    if not daily_all.empty:
        daily_all = daily_all.sort_values(["code", "date", "trade_time_last"]).drop_duplicates(["code", "date"], keep="last")
        daily_all = _future_columns(daily_all, execution_lag_days=args.execution_lag_days, horizon_days=args.horizon_days)
    daily_path = output_root / "phase3dy_daily_signal_panel.parquet"
    daily_all.to_parquet(daily_path, index=False)
    pd.DataFrame(shard_rows).to_csv(output_root / "phase3dy_shard_manifest.csv", index=False)
    pd.DataFrame(errors).to_csv(output_root / "phase3dy_eval_errors.csv", index=False)

    metric_rows: list[dict[str, Any]] = []
    for _, row in candidates.iterrows():
        for return_column in ["ret_close_to_close", "ret_open_to_open", "ret_open_to_exit_close"]:
            metric_rows.append(
                _candidate_metrics(
                    daily_all,
                    candidate_id=str(row["candidate_id"]),
                    expression=str(row.get("expression") or ""),
                    top_quantile=args.top_quantile,
                    return_column=return_column,
                )
            )
    metrics = pd.DataFrame(metric_rows)
    metrics_path = output_root / "phase3dy_candidate_tradable_metrics.csv"
    metrics.to_csv(metrics_path, index=False)

    decision_counts = Counter(metrics.get("decision", pd.Series(dtype=str)).astype(str))
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3DY_TRUE1MIN_TPLUS1_TRADABLE_REPLAY_RESEARCH_ONLY",
        "input_path": str(input_path),
        "shard_root": str(shard_root),
        "output_root": str(output_root),
        "report_root": str(report_root),
        "panel_count": int(len(panels)),
        "candidate_count": int(len(candidates)),
        "required_fields": sorted(required_fields),
        "daily_rows": int(len(daily_all)),
        "date_min": daily_all["date"].min().date().isoformat() if not daily_all.empty else None,
        "date_max": daily_all["date"].max().date().isoformat() if not daily_all.empty else None,
        "decision_counts": dict(decision_counts),
        "eval_error_count": int(len(errors)),
        "daily_panel_path": str(daily_path),
        "metrics_path": str(metrics_path),
        "trading_contract": [
            "expressions are evaluated on true minute rows before daily collapse",
            "daily signal is the last available minute signal for each code/exec_date",
            "long-only top quantile, no short leg",
            "entry rows with next-session limit-up event are excluded",
            "ret_close_to_close uses T+1 close entry and T+2 close exit",
            "ret_open_to_open uses T+1 open entry and T+2 open exit",
            "ret_open_to_exit_close uses T+1 open entry and T+2 close exit",
            "current shard root is partial canary coverage, not final full-OOS proof",
        ],
    }
    _write_json(output_root / "phase3dy_true1min_tplus1_tradable_replay_summary.json", summary)
    _write_json(report_root / "phase3dy_true1min_tplus1_tradable_replay_summary.json", summary)
    _write_markdown(report_root / "PHASE3DY_TRUE1MIN_TPLUS1_TRADABLE_REPLAY_20260702.md", summary, metrics)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--shard-root", default=str(DEFAULT_SHARD_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--report-root", default=str(DEFAULT_REPORT_ROOT))
    parser.add_argument("--decision", default="TRAIN_REWARD_FOLLOWUP_READY")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-shards", type=int, default=1)
    parser.add_argument("--top-quantile", type=float, default=0.2)
    parser.add_argument("--execution-lag-days", type=int, default=1)
    parser.add_argument("--horizon-days", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
