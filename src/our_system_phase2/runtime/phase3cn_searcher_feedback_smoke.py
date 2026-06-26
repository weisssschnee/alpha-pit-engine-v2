"""Phase3CN searcher feedback guard smoke.

This route does not run search. It proves the BS/BT/BU searchers expose the
Phase3CN feedback inputs and that sparse or blocked feedback cannot mutate the
CEM/UCB policy.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import _write_csv, _write_json
from our_system_phase2.runtime.phase3bs_adaptive_ucb_cem_practice import _policy_with_train_reward_feedback
from our_system_phase2.services.search_feedback import (
    build_search_feedback_context,
    load_search_feedback_context,
    policy_blocked_by_external_feedback,
)


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cn_searcher_feedback_smoke_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3cn_searcher_feedback_smoke_20260623")
SEARCHER_FILES = {
    "phase3bs-adaptive-ucb-cem-practice": Path("src/our_system_phase2/runtime/phase3bs_adaptive_ucb_cem_practice.py"),
    "phase3bt-ast-algorithm-bakeoff": Path("src/our_system_phase2/runtime/phase3bt_ast_algorithm_bakeoff.py"),
    "phase3bu-ast-fresh-winner-variants": Path("src/our_system_phase2/runtime/phase3bu_ast_fresh_winner_variants.py"),
}
REQUIRED_ARGS = [
    "--feedback-table",
    "--arm-score-table",
    "--family-memory",
    "--blocked-family-table",
    "--exploit-allowed-family-table",
    "--arm-id",
]


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _synthetic_low_feedback_context(min_clean_feedback: int, arm_id: str):
    feedback_rows = [
        {
            "candidate_id": "synthetic_proxy_winner",
            "expression_hash": "synthetic_proxy_winner_hash",
            "expression": "Rank($m1_first_ret) * ZScore($range_location)",
            "generator_arm": arm_id,
            "generator_route": "phase3bs-adaptive-ucb-cem-practice",
            "family_id": "synthetic_rewardhack_family",
            "phase3ca_proxy_quality": "0.93",
            "train_reward": "-0.75",
            "validation_day_sortino": "-0.20",
            "validation_mcmc_prob_gt_0": "0.20",
            "holdout_day_sortino": "1.25",
            "holdout_mcmc_prob_gt_0": "0.90",
            "mean_one_way_turnover": "0.92",
            "blocker_flags": "",
        }
    ]
    arm_rows = [
        {
            "generator_arm": arm_id,
            "candidate_count": "1",
            "clean_feedback_count": "0",
            "min_clean_feedback": str(min_clean_feedback),
            "feedback_update_allowed": "false",
            "arm_score": "-1.0",
        }
    ]
    family_rows = [
        {
            "family_id": "synthetic_rewardhack_family",
            "family_status": "freeze",
            "family_reasons": "proxy_high_cm_negative|high_turnover",
        }
    ]
    blocked_rows = list(family_rows)
    return build_search_feedback_context(
        feedback_rows=feedback_rows,
        arm_rows=arm_rows,
        family_rows=family_rows,
        blocked_rows=blocked_rows,
        exploit_rows=[],
        arm_id=arm_id,
        min_clean_feedback=min_clean_feedback,
        source_tables={"synthetic": "phase3cn_searcher_feedback_smoke_low_feedback"},
    )


def _synthetic_allowed_train_feedback(min_clean_feedback: int, arm_id: str):
    feedback_rows = [
        {
            "candidate_id": "synthetic_train_reward_winner",
            "expression_hash": "synthetic_train_reward_winner_hash",
            "expression": "Rank(Mean($m1_first_ret,5)) - Rank(Std($range_location,10))",
            "generator_arm": arm_id,
            "generator_route": "phase3bs-adaptive-ucb-cem-practice",
            "family_id": "synthetic_train_reward_family",
            "optimizer_reward": "0.42",
            "optimizer_reward_source": "train_only_phase3cm",
            "optimizer_reward_metric": "train_portfolio_sortino_reward",
            "optimizer_reward_split": "train",
            "train_reward": "0.42",
            "train_reward_decision": "TRAIN_REWARD_FOLLOWUP_READY",
            "train_reward_blockers": "",
            "validation_day_sortino": "-0.33",
            "validation_mcmc_prob_gt_0": "0.12",
            "holdout_day_sortino": "-0.77",
            "holdout_mcmc_prob_gt_0": "0.08",
            "mean_one_way_turnover": "0.34",
            "blocker_flags": "",
            "validation_usage": "report_only",
            "holdout_usage": "report_only",
        }
    ]
    family_rows = [
        {
            "family_id": "synthetic_train_reward_family",
            "family_status": "exploit_allowed",
            "family_reasons": "train_optimizer_reward_positive",
        }
    ]
    context = build_search_feedback_context(
        feedback_rows=feedback_rows,
        arm_rows=[],
        family_rows=family_rows,
        blocked_rows=[],
        exploit_rows=family_rows,
        arm_id=arm_id,
        min_clean_feedback=min_clean_feedback,
        source_tables={"synthetic": "phase3cn_searcher_feedback_smoke_allowed_train_reward"},
    )
    return context, feedback_rows


def _arg_checks() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for route, rel_path in SEARCHER_FILES.items():
        path = REPO / rel_path
        text = path.read_text(encoding="utf-8")
        missing = [arg for arg in REQUIRED_ARGS if arg not in text]
        rows.append(
            {
                "route": route,
                "file": str(rel_path).replace("\\", "/"),
                "required_args": "|".join(REQUIRED_ARGS),
                "missing_args": "|".join(missing),
                "pass": str(not missing).lower(),
            }
        )
    return rows


def _render_md(summary: dict[str, Any], checks: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3CN Searcher Feedback Smoke 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Result",
        "",
        "```text",
        f"feedback_update_allowed: {summary['feedback_context']['feedback_update_allowed']}",
        f"clean_feedback_count: {summary['feedback_context']['clean_feedback_count']}",
        f"min_clean_feedback: {summary['feedback_context']['min_clean_feedback']}",
        f"holdout_columns_present: {summary['feedback_context']['holdout_columns_present']}",
        f"holdout_used_for_score: {summary['feedback_context']['holdout_used_for_score']}",
        f"policy_scores_unchanged: {summary['policy_scores_unchanged']}",
        f"allowed_train_reward_policy_updated: {summary['allowed_train_reward_policy_updated']}",
        f"allowed_train_reward_source: {summary['allowed_train_reward_policy']['feedback'].get('optimizer_reward_source')}",
        "```",
        "",
        "## Searcher Args",
        "",
        "| route | pass | missing_args |",
        "|---|---:|---|",
    ]
    for row in checks:
        lines.append(f"| `{row['route']}` | `{row['pass']}` | `{row['missing_args']}` |")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This smoke does not run search.",
            "- Holdout columns are carried for audit only.",
            "- Sparse or blocked external CN feedback leaves CEM/UCB policy scores unchanged.",
            "- Clean external train reward feedback can update policy without using validation/holdout.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feedback-table", type=Path, default=None)
    parser.add_argument("--arm-score-table", type=Path, default=None)
    parser.add_argument("--family-memory", type=Path, default=None)
    parser.add_argument("--blocked-family-table", type=Path, default=None)
    parser.add_argument("--exploit-allowed-family-table", type=Path, default=None)
    parser.add_argument("--arm-id", default="cem_exploit")
    parser.add_argument("--min-clean-feedback", type=int, default=8)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    args = parser.parse_args(argv)

    if args.feedback_table or args.arm_score_table or args.family_memory or args.blocked_family_table or args.exploit_allowed_family_table:
        context = load_search_feedback_context(
            feedback_table=_resolve(args.feedback_table) if args.feedback_table else None,
            arm_score_table=_resolve(args.arm_score_table) if args.arm_score_table else None,
            family_memory=_resolve(args.family_memory) if args.family_memory else None,
            blocked_family_table=_resolve(args.blocked_family_table) if args.blocked_family_table else None,
            exploit_allowed_family_table=_resolve(args.exploit_allowed_family_table) if args.exploit_allowed_family_table else None,
            arm_id=args.arm_id,
            min_clean_feedback=args.min_clean_feedback,
        )
    else:
        context = _synthetic_low_feedback_context(args.min_clean_feedback, args.arm_id)

    base_policy = {
        "policy_version": "phase3cn_smoke_seed_policy",
        "scores": {
            "field": {"m1_first_ret": 1.0, "range_location": 0.8},
            "lane": {"fresh": 0.7, "cem": 0.2},
        },
    }
    guarded_policy = policy_blocked_by_external_feedback(base_policy, context)
    allowed_context, allowed_rows = _synthetic_allowed_train_feedback(1, args.arm_id)
    allowed_policy = _policy_with_train_reward_feedback(
        base_policy,
        allowed_rows,
        context=allowed_context,
        learning_rate=0.50,
        entropy_floor=0.01,
        min_eligible=1,
    )
    arg_checks = _arg_checks()
    all_args_present = all(row["pass"] == "true" for row in arg_checks)
    policy_scores_unchanged = guarded_policy.get("scores") == base_policy.get("scores")
    guard_blocks_update = context.provided and not context.feedback_update_allowed and guarded_policy.get("feedback", {}).get("updated") is False
    holdout_read_only = context.holdout_columns_present and not context.holdout_used_for_score
    allowed_train_reward_policy_updated = allowed_policy.get("feedback", {}).get("updated") is True
    allowed_train_reward_train_only = (
        allowed_policy.get("feedback", {}).get("optimizer_reward_source") == "train_only_phase3cm"
        and allowed_policy.get("feedback", {}).get("validation_used_for_score") is False
        and allowed_policy.get("feedback", {}).get("holdout_used_for_score") is False
    )
    passed = (
        all_args_present
        and policy_scores_unchanged
        and guard_blocks_update
        and holdout_read_only
        and allowed_train_reward_policy_updated
        and allowed_train_reward_train_only
    )
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cn_searcher_feedback_smoke",
        "decision": "PHASE3CN_SEARCHER_FEEDBACK_GUARD_PASS_DIAGNOSTIC_ONLY" if passed else "PHASE3CN_SEARCHER_FEEDBACK_GUARD_FAIL",
        "search_generation": False,
        "feedback_context": context.to_dict(),
        "all_searchers_have_feedback_args": all_args_present,
        "policy_scores_unchanged": policy_scores_unchanged,
        "guard_blocks_update": guard_blocks_update,
        "holdout_read_only": holdout_read_only,
        "allowed_train_reward_context": allowed_context.to_dict(),
        "allowed_train_reward_policy": allowed_policy,
        "allowed_train_reward_policy_updated": allowed_train_reward_policy_updated,
        "allowed_train_reward_train_only": allowed_train_reward_train_only,
        "required_args": REQUIRED_ARGS,
    }

    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "phase3cn_searcher_feedback_smoke_summary.json", summary)
    _write_json(report_root / "phase3cn_searcher_feedback_smoke_summary.json", summary)
    _write_csv(report_root / "phase3cn_searcher_feedback_smoke_arg_checks.csv", arg_checks)
    (report_root / "PHASE3CN_SEARCHER_FEEDBACK_SMOKE_20260623.md").write_text(_render_md(summary, arg_checks), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
