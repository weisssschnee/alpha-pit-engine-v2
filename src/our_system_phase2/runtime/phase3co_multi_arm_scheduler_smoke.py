"""Phase3CO multi-arm scheduler smoke.

This route reads Phase3CN feedback memory and emits the arm/family budget plan
needed before Phase3CP can run. It does not generate candidates or run search.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import _write_csv, _write_json
from our_system_phase2.services.multi_arm_scheduler import build_arm_schedule, read_csv_rows


REPO = Path(__file__).resolve().parents[3]
DEFAULT_FEEDBACK_ROOT = Path("reports/phase3cn_integrated_feedback_smoke_20260623/phase3cn_feedback_memory")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3co_multi_arm_scheduler_smoke_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3co_multi_arm_scheduler_smoke_20260623")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _render_md(summary: dict[str, Any], arm_rows: list[dict[str, Any]], family_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3CO Multi-Arm Scheduler Smoke 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Result",
        "",
        "```text",
        f"total_budget: {summary['scheduler_summary']['total_budget']}",
        f"allocated_budget: {summary['scheduler_summary']['allocated_budget']}",
        f"fresh_share: {summary['scheduler_summary']['fresh_share']}",
        f"cem_exploit_budget: {summary['scheduler_summary']['cem_exploit_budget']}",
        f"cem_probe_cap_budget: {summary['scheduler_summary']['cem_probe_cap_budget']}",
        f"exploit_allowed_family_count: {summary['scheduler_summary']['exploit_allowed_family_count']}",
        f"blocked_or_frozen_family_count: {summary['scheduler_summary']['blocked_or_frozen_family_count']}",
        "```",
        "",
        "## Arm Budgets",
        "",
        "| arm | budget | share | action | reason |",
        "|---|---:|---:|---|---|",
    ]
    for row in arm_rows:
        lines.append(
            f"| `{row['arm_id']}` | {row['candidate_budget']} | {row['target_share']} | "
            f"`{row['scheduler_action']}` | {row['scheduler_reason']} |"
        )
    lines.extend(["", "## Family Actions", "", "| family | action | cap | reason |", "|---|---|---:|---|"])
    for row in family_rows[:12]:
        lines.append(f"| `{row['family_id']}` | `{row['scheduler_action']}` | {row['candidate_budget_cap']} | {row['scheduler_reason']} |")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- No search generation.",
            "- No true1min portfolio evaluation.",
            "- Holdout is not an input to scheduler scoring.",
            "- CEM exploit remains capped when CN feedback_update_allowed=false.",
        ]
    )
    return "\n".join(lines) + "\n"


def _inputs_from_root(root: Path) -> dict[str, Path]:
    return {
        "feedback_table": root / "phase3cn_search_feedback_memory.csv",
        "arm_score_table": root / "phase3cn_arm_score_table.csv",
        "family_memory": root / "phase3cn_family_score_table.csv",
        "blocked_family_table": root / "phase3cn_blocked_family_table.csv",
        "exploit_allowed_family_table": root / "phase3cn_exploit_allowed_family_table.csv",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feedback-root", type=Path, default=DEFAULT_FEEDBACK_ROOT)
    parser.add_argument("--feedback-table", type=Path, default=None)
    parser.add_argument("--arm-score-table", type=Path, default=None)
    parser.add_argument("--family-memory", type=Path, default=None)
    parser.add_argument("--blocked-family-table", type=Path, default=None)
    parser.add_argument("--exploit-allowed-family-table", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--total-budget", type=int, default=512)
    parser.add_argument("--fresh-floor-share", type=float, default=0.45)
    parser.add_argument("--cem-probe-cap-share", type=float, default=0.06)
    parser.add_argument("--max-family-share", type=float, default=0.25)
    args = parser.parse_args(argv)

    feedback_root = _resolve(args.feedback_root)
    default_inputs = _inputs_from_root(feedback_root)
    inputs = {
        "feedback_table": _resolve(args.feedback_table) if args.feedback_table else default_inputs["feedback_table"],
        "arm_score_table": _resolve(args.arm_score_table) if args.arm_score_table else default_inputs["arm_score_table"],
        "family_memory": _resolve(args.family_memory) if args.family_memory else default_inputs["family_memory"],
        "blocked_family_table": _resolve(args.blocked_family_table) if args.blocked_family_table else default_inputs["blocked_family_table"],
        "exploit_allowed_family_table": _resolve(args.exploit_allowed_family_table) if args.exploit_allowed_family_table else default_inputs["exploit_allowed_family_table"],
    }
    feedback_rows = read_csv_rows(inputs["feedback_table"])
    arm_score_rows = read_csv_rows(inputs["arm_score_table"])
    family_rows = read_csv_rows(inputs["family_memory"])
    blocked_rows = read_csv_rows(inputs["blocked_family_table"])
    exploit_rows = read_csv_rows(inputs["exploit_allowed_family_table"])
    arm_budget_rows, family_action_rows, scheduler_summary = build_arm_schedule(
        arm_score_rows,
        family_rows,
        blocked_rows,
        exploit_rows,
        total_budget=args.total_budget,
        fresh_floor_share=args.fresh_floor_share,
        cem_probe_cap_share=args.cem_probe_cap_share,
        max_family_share=args.max_family_share,
    )
    cem_row = next(row for row in arm_budget_rows if row["arm_id"] == "cem_exploit")
    fresh_ok = float(scheduler_summary["fresh_share"]) >= args.fresh_floor_share
    cem_cap_ok = int(cem_row["candidate_budget"]) <= int(scheduler_summary["cem_probe_cap_budget"])
    family_block_ok = any(row["scheduler_action"] in {"block", "freeze"} and int(row["candidate_budget_cap"]) == 0 for row in family_action_rows)
    exploit_family_ok = any(row["scheduler_action"] == "allow_followup" and int(row["candidate_budget_cap"]) > 0 for row in family_action_rows)
    total_ok = int(scheduler_summary["allocated_budget"]) == int(args.total_budget)
    passed = fresh_ok and cem_cap_ok and family_block_ok and exploit_family_ok and total_ok
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3co_multi_arm_scheduler_smoke",
        "decision": "PHASE3CO_MULTI_ARM_SCHEDULER_SMOKE_PASS_DIAGNOSTIC_ONLY" if passed else "PHASE3CO_MULTI_ARM_SCHEDULER_SMOKE_FAIL",
        "search_generation": False,
        "true1min_portfolio_eval": False,
        "input_row_counts": {
            "feedback": len(feedback_rows),
            "arm_score": len(arm_score_rows),
            "family": len(family_rows),
            "blocked_family": len(blocked_rows),
            "exploit_allowed_family": len(exploit_rows),
        },
        "checks": {
            "fresh_floor_ok": fresh_ok,
            "cem_probe_cap_ok": cem_cap_ok,
            "family_block_ok": family_block_ok,
            "exploit_family_ok": exploit_family_ok,
            "total_budget_ok": total_ok,
        },
        "input_paths": {key: str(value) for key, value in inputs.items()},
        "scheduler_summary": scheduler_summary,
        "metric_boundary": "scheduler smoke only; no candidate generation and no holdout optimization",
    }
    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "phase3co_arm_budget_table.csv", arm_budget_rows)
    _write_csv(output_root / "phase3co_family_action_table.csv", family_action_rows)
    _write_json(output_root / "phase3co_scheduler_summary.json", summary)
    _write_csv(report_root / "phase3co_arm_budget_table.csv", arm_budget_rows)
    _write_csv(report_root / "phase3co_family_action_table.csv", family_action_rows)
    _write_json(report_root / "phase3co_scheduler_summary.json", summary)
    (report_root / "PHASE3CO_MULTI_ARM_SCHEDULER_SMOKE_20260623.md").write_text(
        _render_md(summary, arm_budget_rows, family_action_rows),
        encoding="utf-8",
    )
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
