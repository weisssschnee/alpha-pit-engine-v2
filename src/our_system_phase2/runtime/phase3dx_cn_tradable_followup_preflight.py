from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from our_system_phase2.services.real_market_data import (
    DEFAULT_REAL_MARKET_DATASET_PATH,
    build_real_market_data_contract,
    panel_header,
)
from our_system_phase2.services.real_market_validation import batch_validate_candidate_ledger


DEFAULT_INPUT = Path(r"G:\Chengbo\runtime\phase3dw_recovery_20260702\phase3cm_train_reward_recovered_6144.csv")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3dx_cn_tradable_followup_preflight_20260702")
DEFAULT_REPORT_ROOT = Path("reports/phase3dx_cn_tradable_followup_preflight_20260702")

FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if pd.notna(out) else None


def _field_class(field: str) -> str:
    if field.startswith("evt_"):
        return "event_state_or_event_lag"
    if field.startswith("ctx_"):
        return "lagged_daily_context"
    if field.startswith("m1_first"):
        return "first_minutes_intraday_summary"
    if field.startswith("m1_"):
        return "minute_summary"
    if field in {"open", "high", "low", "close", "amount", "volume", "vwap"}:
        return "base_price_volume"
    return "unknown_or_panel_specific"


def _extract_fields(expression: str) -> list[str]:
    return sorted(set(FIELD_RE.findall(str(expression or ""))))


def _load_followups(input_path: Path, decision: str, limit: int | None) -> pd.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"input not found: {input_path}")
    frame = pd.read_csv(input_path)
    if "train_reward_decision" in frame.columns:
        frame = frame[frame["train_reward_decision"].astype(str).eq(decision)].copy()
    else:
        frame = frame.iloc[0:0].copy()
    if "train_reward" in frame.columns:
        frame["_sort_reward"] = pd.to_numeric(frame["train_reward"], errors="coerce").fillna(-999999.0)
        frame = frame.sort_values("_sort_reward", ascending=False).drop(columns=["_sort_reward"])
    if limit is not None:
        frame = frame.head(limit)
    return frame.reset_index(drop=True)


def _compatibility_rows(followups: pd.DataFrame, available_fields: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, row in followups.iterrows():
        expression = str(row.get("expression") or "")
        fields = _extract_fields(expression)
        missing = [field for field in fields if field not in available_fields]
        classes = sorted(set(_field_class(field) for field in fields))
        missing_classes = sorted(set(_field_class(field) for field in missing))
        needs_true1min_panel = any(cls in {"event_state_or_event_lag", "lagged_daily_context", "first_minutes_intraday_summary", "minute_summary"} for cls in missing_classes)
        if not fields:
            decision = "REJECT_NO_EXPRESSION_FIELDS"
        elif not missing:
            decision = "RUN_EXISTING_REAL_MARKET_LONG_ONLY_TPLUS1_VALIDATION"
        elif needs_true1min_panel:
            decision = "NEEDS_TRUE1MIN_TO_TPLUS1_TRADABLE_PANEL"
        else:
            decision = "MISSING_FIELDS_ON_REAL_MARKET_PANEL"
        rows.append(
            {
                "rank": idx + 1,
                "candidate_id": row.get("candidate_id"),
                "train_reward": row.get("train_reward"),
                "train_day_sortino": row.get("train_day_sortino"),
                "validation_day_sortino": row.get("validation_day_sortino"),
                "holdout_day_sortino": row.get("holdout_day_sortino"),
                "train_rank_ic_mean": row.get("train_rank_ic_mean"),
                "train_mean_one_way_turnover": row.get("train_mean_one_way_turnover"),
                "field_count": len(fields),
                "fields": "|".join(fields),
                "field_classes": "|".join(classes),
                "missing_field_count": len(missing),
                "missing_fields": "|".join(missing),
                "missing_field_classes": "|".join(missing_classes),
                "compatibility_decision": decision,
                "expression": expression,
            }
        )
    return rows


def _ledger_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in rows:
        if row["compatibility_decision"] != "RUN_EXISTING_REAL_MARKET_LONG_ONLY_TPLUS1_VALIDATION":
            continue
        records.append(
            {
                "candidate_id": row["candidate_id"],
                "expression": row["expression"],
                "retained": True,
                "source_mode": "phase3dw_followup_ready",
                "frontier_lane": "phase3dx_cn_tradable_preflight",
                "archive_cell": "dw_followup_existing_panel_compatible",
                "validation_usage": "report_only",
            }
        )
    return records


def _write_markdown(path: Path, summary: dict[str, Any], top_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Phase3DX CN Tradable Follow-up Preflight",
        "",
        "Purpose: check whether Phase3DW follow-up expressions can enter existing long-only T+1 real-market validation.",
        "",
        "This is not a promotion report. Unsupported expressions are routed to true1min-to-T+1 panel construction.",
        "",
        "## Summary",
        "",
    ]
    for key in [
        "input_path",
        "real_market_dataset_path",
        "followup_count",
        "existing_validation_ready_count",
        "needs_true1min_to_tplus1_panel_count",
        "missing_field_candidate_count",
        "validation_evaluated_count",
        "validation_passed_smoke_count",
    ]:
        lines.append(f"- {key}: `{summary.get(key)}`")
    lines.extend(["", "## Top Field Gaps", ""])
    for item in summary.get("top_missing_fields", []):
        lines.append(f"- `{item['field']}`: {item['count']}")
    lines.extend(["", "## Top Follow-ups", "", "| candidate | decision | train reward | val sortino | holdout sortino | missing fields | expression |", "|---|---|---:|---:|---:|---|---|"])
    for row in top_rows[:20]:
        expr = str(row.get("expression") or "").replace("|", "\\|")
        missing = str(row.get("missing_fields") or "").replace("|", ", ")
        lines.append(
            f"| `{row.get('candidate_id')}` | `{row.get('compatibility_decision')}` | {row.get('train_reward')} | "
            f"{row.get('validation_day_sortino')} | {row.get('holdout_day_sortino')} | `{missing}` | `{expr}` |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_root = Path(args.output_root)
    report_root = Path(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    input_path = Path(args.input)
    dataset_path = Path(args.real_market_dataset)
    followups = _load_followups(input_path, args.decision, args.limit)
    available_fields = set(panel_header(dataset_path)) if dataset_path.exists() else set()
    data_contract = build_real_market_data_contract(dataset_path, full_scan=False)
    rows = _compatibility_rows(followups, available_fields)

    compatibility_path = output_root / "phase3dx_followup_compatibility.csv"
    pd.DataFrame(rows).to_csv(compatibility_path, index=False, encoding="utf-8")

    records = _ledger_records(rows)
    ledger = {
        "run_id": "phase3dx_cn_tradable_followup_preflight_20260702",
        "source_input": str(input_path),
        "records": records,
        "recommended_validation_kwargs": {
            "execution_lag_days": 1,
            "horizon_days": 1,
            "signal_clock": "after_close",
            "feature_lag_days": 0,
            "enable_trend_state_features": False,
        },
        "boundary": "existing real_market_validation compatible records only; other records require true1min-to-T+1 panel",
    }
    ledger_path = output_root / "phase3dx_existing_validation_ledger.json"
    ledger_path.write_text(json.dumps(ledger, ensure_ascii=False, indent=2), encoding="utf-8")

    validation_report: dict[str, Any] | None = None
    if args.run_existing_validation and records:
        validation_report = batch_validate_candidate_ledger(
            ledger_path,
            path=dataset_path,
            retained_only=True,
            horizon_days=1,
            execution_lag_days=1,
            signal_clock="after_close",
            feature_lag_days=0,
            top_bottom_quantile=args.top_quantile,
            recent_quarter_window_count=args.recent_quarter_window_count,
            parallel_workers=args.parallel_workers,
            use_fast_context=args.use_fast_context,
        )
        (output_root / "phase3dx_existing_real_validation_report.json").write_text(
            json.dumps(validation_report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    missing_counter: Counter[str] = Counter()
    for row in rows:
        for field in str(row.get("missing_fields") or "").split("|"):
            if field:
                missing_counter[field] += 1
    decisions = Counter(row["compatibility_decision"] for row in rows)
    summary = {
        "input_path": str(input_path),
        "output_root": str(output_root),
        "report_root": str(report_root),
        "real_market_dataset_path": str(dataset_path),
        "data_contract": data_contract,
        "followup_count": len(rows),
        "decision_counts": dict(decisions),
        "existing_validation_ready_count": decisions.get("RUN_EXISTING_REAL_MARKET_LONG_ONLY_TPLUS1_VALIDATION", 0),
        "needs_true1min_to_tplus1_panel_count": decisions.get("NEEDS_TRUE1MIN_TO_TPLUS1_TRADABLE_PANEL", 0),
        "missing_field_candidate_count": sum(1 for row in rows if int(row.get("missing_field_count") or 0) > 0),
        "top_missing_fields": [{"field": field, "count": count} for field, count in missing_counter.most_common(30)],
        "ledger_path": str(ledger_path),
        "compatibility_path": str(compatibility_path),
        "validation_report_path": str(output_root / "phase3dx_existing_real_validation_report.json") if validation_report else None,
        "validation_evaluated_count": (validation_report or {}).get("evaluated_count", 0),
        "validation_passed_smoke_count": (validation_report or {}).get("passed_smoke_count", 0),
        "required_next_action": (
            "build_true1min_to_tplus1_tradable_panel_for_dw_followups"
            if decisions.get("NEEDS_TRUE1MIN_TO_TPLUS1_TRADABLE_PANEL", 0)
            else "review_existing_real_validation_report"
        ),
    }
    summary_path = report_root / "phase3dx_cn_tradable_followup_preflight_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown(report_root / "PHASE3DX_CN_TRADABLE_FOLLOWUP_PREFLIGHT_20260702.md", summary, rows)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--report-root", default=str(DEFAULT_REPORT_ROOT))
    parser.add_argument("--real-market-dataset", default=str(DEFAULT_REAL_MARKET_DATASET_PATH))
    parser.add_argument("--decision", default="TRAIN_REWARD_FOLLOWUP_READY")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--top-quantile", type=float, default=0.2)
    parser.add_argument("--recent-quarter-window-count", type=int, default=4)
    parser.add_argument("--parallel-workers", type=int, default=1)
    parser.add_argument("--use-fast-context", action="store_true")
    parser.add_argument("--no-existing-validation", dest="run_existing_validation", action="store_false")
    parser.set_defaults(run_existing_validation=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
