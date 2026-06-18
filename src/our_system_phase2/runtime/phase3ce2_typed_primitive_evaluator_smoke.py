"""Smoke-test typed primitive evaluator semantics on a synthetic true-1min panel."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from our_system_phase2.domain.models import utc_now_iso
from our_system_phase2.services.artifact_schema import write_json_artifact
from our_system_phase2.services.real_market_validation import evaluate_panel_expression


DEFAULT_OUTPUT = Path("reports/phase3ce2_typed_primitive_evaluator_smoke_20260618")
SMOKE_VERSION = "phase3ce2-typed-primitive-evaluator-smoke-v1-2026-06-18"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _synthetic_panel() -> pd.DataFrame:
    times = pd.date_range("2026-06-01 09:31:00", periods=16, freq="min")
    rows: list[dict[str, Any]] = []
    for code_idx, code in enumerate(("000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ")):
        for t_idx, trade_time in enumerate(times):
            event = 1.0 if (t_idx in {3 + code_idx, 9} and code_idx != 3) else 0.0
            high_board_rank = float((t_idx // 3 + code_idx) % 4)
            turnover = float(0.5 + code_idx * 0.2 + t_idx * 0.03)
            if code == "000004.SZ" and t_idx < 8:
                turnover = np.nan
            amount = float(1000 + code_idx * 250 + t_idx * 17)
            rows.append(
                {
                    "code": code,
                    "date": trade_time,
                    "trade_time": trade_time,
                    "close": 10.0 + code_idx + t_idx * 0.01,
                    "amount": amount,
                    "limit_up_any_close_not_open_in_t10": event,
                    "high_board_rank": high_board_rank,
                    "turnover_ratio": turnover,
                }
            )
    return pd.DataFrame(rows).sort_values(["code", "trade_time"]).reset_index(drop=True)


def _expression_cases() -> list[dict[str, str]]:
    return [
        {
            "primitive": "EventCount",
            "expression": "CSRank(EventCount($limit_up_any_close_not_open_in_t10,5))",
            "expectation": "rolling event count becomes non-null after full window",
        },
        {
            "primitive": "EventAge",
            "expression": "CSRank(EventAge($limit_up_any_close_not_open_in_t10))",
            "expectation": "age is NaN before first event and grows after event",
        },
        {
            "primitive": "StateDwell",
            "expression": "CSRank(StateDwell($high_board_rank,5))",
            "expectation": "state dwell tracks consecutive same finite state",
        },
        {
            "primitive": "WindowStateCount",
            "expression": "CSRank(WindowStateCount($high_board_rank,10))",
            "expectation": "rolling nonzero finite-state count",
        },
        {
            "primitive": "ValidRatioGate",
            "expression": "CSRank(ValidRatioGate($turnover_ratio,6,0.8))",
            "expectation": "coverage-poor early rows are masked",
        },
        {
            "primitive": "MaskedZScore",
            "expression": "CSRank(MaskedZScore($turnover_ratio,6,0.8))",
            "expectation": "coverage-gated cross-sectional zscore",
        },
        {
            "primitive": "MaskedCorr",
            "expression": "CSRank(MaskedCorr($turnover_ratio,$amount,6,0.5))",
            "expectation": "coverage-gated rolling relation",
        },
        {
            "primitive": "SafeCSResidual",
            "expression": "CSRank(SafeCSResidual($turnover_ratio,$amount,2,2,0.5))",
            "expectation": "cross-sectional residual only when regression geometry is valid",
        },
    ]


def _series_stats(frame: pd.DataFrame, expression: str) -> dict[str, Any]:
    raw = pd.to_numeric(evaluate_panel_expression(frame, expression, cache={}), errors="coerce")
    return {
        "signal_nonnull": int(raw.notna().sum()),
        "signal_unique": int(raw.nunique(dropna=True)),
        "signal_min": None if raw.dropna().empty else float(raw.min()),
        "signal_max": None if raw.dropna().empty else float(raw.max()),
        "first_valid_trade_time": None
        if raw.dropna().empty
        else str(frame.loc[raw.dropna().index[0], "trade_time"]),
    }


def _direct_semantic_checks(frame: pd.DataFrame) -> list[dict[str, Any]]:
    event_age = pd.to_numeric(
        evaluate_panel_expression(frame, "EventAge($limit_up_any_close_not_open_in_t10)", cache={}),
        errors="coerce",
    )
    event_count = pd.to_numeric(
        evaluate_panel_expression(frame, "EventCount($limit_up_any_close_not_open_in_t10,5)", cache={}),
        errors="coerce",
    )
    gated = pd.to_numeric(
        evaluate_panel_expression(frame, "ValidRatioGate($turnover_ratio,6,0.8)", cache={}),
        errors="coerce",
    )
    code1 = frame["code"].eq("000001.SZ")
    code4 = frame["code"].eq("000004.SZ")
    return [
        {
            "check_id": "event_age_before_first_event_nan",
            "status": "pass" if bool(event_age[code1].iloc[:3].isna().all()) else "fail",
            "evidence": f"pre_event_nonnull={int(event_age[code1].iloc[:3].notna().sum())}",
        },
        {
            "check_id": "event_age_resets_on_event",
            "status": "pass" if float(event_age[code1].iloc[3]) == 0.0 else "fail",
            "evidence": f"event_bar_age={event_age[code1].iloc[3]}",
        },
        {
            "check_id": "event_count_requires_full_window",
            "status": "pass" if bool(event_count[code1].iloc[:4].isna().all()) else "fail",
            "evidence": f"first4_nonnull={int(event_count[code1].iloc[:4].notna().sum())}",
        },
        {
            "check_id": "valid_ratio_gate_masks_low_coverage",
            "status": "pass" if bool(gated[code4].iloc[:8].isna().all()) else "fail",
            "evidence": f"low_coverage_nonnull={int(gated[code4].iloc[:8].notna().sum())}",
        },
    ]


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Phase3CE2 Typed Primitive Evaluator Smoke",
        "",
        f"- created_at: {summary['created_at']}",
        f"- panel_rows: {summary['panel_rows']}",
        f"- expression_count: {summary['expression_count']}",
        f"- error_count: {summary['error_count']}",
        f"- decision: {summary['decision']}",
        "",
        "## Semantic Checks",
        "",
    ]
    for row in summary["semantic_checks"]:
        lines.append(f"- {row['check_id']}: {row['status']} - {row['evidence']}")
    lines.extend(
        [
            "",
            "This is evaluator plumbing and semantic smoke only; it is not alpha proof.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)
    frame = _synthetic_panel()
    frame.to_parquet(output_root / "synthetic_true1min_typed_primitive_panel.parquet", index=False)

    result_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for case in _expression_cases():
        try:
            stats = _series_stats(frame, case["expression"])
            result_rows.append({**case, **stats, "status": "pass" if stats["signal_nonnull"] > 0 else "fail"})
        except Exception as exc:
            errors.append({**case, "error": f"{type(exc).__name__}: {str(exc)[:300]}"})
    semantic_checks = _direct_semantic_checks(frame)
    _write_csv(output_root / "typed_primitive_evaluator_smoke_rows.csv", result_rows)
    _write_csv(output_root / "typed_primitive_evaluator_smoke_errors.csv", errors)
    _write_csv(output_root / "typed_primitive_evaluator_semantic_checks.csv", semantic_checks)
    failed = [row for row in result_rows if row.get("status") != "pass"] + [
        row for row in semantic_checks if row.get("status") != "pass"
    ]
    summary = {
        "phase3_version": SMOKE_VERSION,
        "created_at": utc_now_iso(),
        "output_root": str(output_root),
        "panel_rows": int(len(frame)),
        "code_count": int(frame["code"].nunique()),
        "trade_time_count": int(frame["trade_time"].nunique()),
        "expression_count": len(_expression_cases()),
        "evaluated_expression_count": len(result_rows),
        "error_count": len(errors),
        "failed_check_count": len(failed),
        "semantic_checks": semantic_checks,
        "decision": "PASS_TYPED_PRIMITIVE_EVALUATOR_SMOKE" if not errors and not failed else "HOLD_TYPED_PRIMITIVE_EVALUATOR_SMOKE",
    }
    write_json_artifact(output_root / "typed_primitive_evaluator_smoke_summary.json", summary)
    _write_markdown(output_root / "PHASE3CE2_TYPED_PRIMITIVE_EVALUATOR_SMOKE_20260618.md", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
