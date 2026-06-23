"""Phase3CN integrated feedback contract smoke.

This route does not run search or true1min portfolio evaluation. It exercises
the interface chain with a controlled fixture:

synthetic search top_decisions -> CA bridge -> CM reward fixture -> CN feedback
memory -> BS/BT/BU feedback guard.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.runtime.phase3ca_build_bz_candidate_audit import build_candidate_table
from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import build_feedback_memory
from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import _write_csv, _write_json
from our_system_phase2.services.candidate_schema import normalize_candidate_schema
from our_system_phase2.services.search_feedback import load_search_feedback_context, policy_blocked_by_external_feedback


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cn_integrated_feedback_smoke_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3cn_integrated_feedback_smoke_20260623")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _synthetic_search_rows() -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": "cn4_clean_reward_fixture",
            "expression_hash": "cn4_clean_reward_hash",
            "expression": "Rank(Mean($m1_first_ret,5)) - Rank(Std($range_location,10))",
            "generator_arm": "cem_exploit",
            "round_id": "round1_synthetic_fresh",
            "factor_lane": "true1min_opening_range",
            "aligned_ic_mean": "0.051",
            "aligned_spread_mean": "0.0012",
            "spread_hit_rate": "0.57",
            "positive_horizon_count": "3",
            "mean_one_way_turnover": "0.34",
            "blocker_flags": "",
        },
        {
            "candidate_id": "cn4_proxy_rewardhack_fixture",
            "expression_hash": "cn4_proxy_rewardhack_hash",
            "expression": "Rank($m1_first_volume) * ZScore($range_location)",
            "generator_arm": "cem_exploit",
            "round_id": "round1_synthetic_fresh",
            "factor_lane": "true1min_proxy_control",
            "aligned_ic_mean": "0.044",
            "aligned_spread_mean": "0.0010",
            "spread_hit_rate": "0.55",
            "positive_horizon_count": "2",
            "mean_one_way_turnover": "0.92",
            "blocker_flags": "",
        },
    ]


def _write_synthetic_search_fixture(search_root: Path) -> Path:
    out = search_root / "round1_synthetic_fresh" / "cn4_top_decisions.csv"
    _write_csv(out, _synthetic_search_rows())
    return out


def _cm_reward_fixture_from_ca(ca_table: Path, cm_root: Path) -> Path:
    rows: list[dict[str, Any]] = []
    for row in _read_csv(ca_table):
        out = dict(row)
        if row.get("candidate_id") == "cn4_clean_reward_fixture":
            out.update(
                {
                    "train_reward": "0.38",
                    "train_reward_decision": "TRAIN_REWARD_FOLLOWUP_READY",
                    "train_reward_blockers": "",
                    "validation_day_sortino": "0.24",
                    "validation_mcmc_prob_gt_0": "0.62",
                    "holdout_day_sortino": "-0.11",
                    "holdout_mcmc_prob_gt_0": "0.44",
                    "train_mean_one_way_turnover": "0.34",
                    "mean_one_way_turnover": "0.34",
                }
            )
        else:
            out.update(
                {
                    "train_reward": "-0.61",
                    "train_reward_decision": "TRAIN_REWARD_REJECT",
                    "train_reward_blockers": "proxy_high_cm_negative|high_turnover",
                    "validation_day_sortino": "-0.18",
                    "validation_mcmc_prob_gt_0": "0.21",
                    "holdout_day_sortino": "0.88",
                    "holdout_mcmc_prob_gt_0": "0.83",
                    "train_mean_one_way_turnover": "0.92",
                    "mean_one_way_turnover": "0.92",
                }
            )
        out.update(normalize_candidate_schema(out))
        rows.append(out)
    path = cm_root / "phase3cm_train_reward.csv"
    _write_csv(path, rows)
    _write_csv(cm_root / "phase3cm_candidate_train_reward_summary.csv", rows)
    _write_json(
        cm_root / "phase3cm_train_reward_audit_summary.json",
        {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "decision": "PHASE3CN_INTEGRATED_SMOKE_CM_FIXTURE_ONLY",
            "candidate_count": len(rows),
            "metric_boundary": "controlled CM reward fixture; no true1min portfolio evaluation run",
        },
    )
    return path


def _copy_report_files(source_root: Path, report_root: Path) -> None:
    report_root.mkdir(parents=True, exist_ok=True)
    for path in sorted(source_root.glob("*")):
        if path.is_file():
            shutil.copy2(path, report_root / path.name)


def _render_md(summary: dict[str, Any]) -> str:
    checks = summary["checks"]
    lines = [
        "# Phase3CN Integrated Feedback Smoke 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Chain",
        "",
        "```text",
        "synthetic search top_decisions",
        "  -> phase3ca-build-bz-candidate-audit bridge",
        "  -> controlled Phase3CM reward fixture",
        "  -> phase3cn-feedback-memory-smoke builder",
        "  -> search_feedback guard context",
        "```",
        "",
        "## Checks",
        "",
        "```text",
        f"ca_candidate_count: {checks['ca_candidate_count']}",
        f"cn_candidate_count: {checks['cn_candidate_count']}",
        f"clean_feedback_count: {checks['clean_feedback_count']}",
        f"strict_update_allowed: {checks['strict_update_allowed']}",
        f"loose_update_allowed: {checks['loose_update_allowed']}",
        f"strict_policy_scores_unchanged: {checks['strict_policy_scores_unchanged']}",
        f"holdout_used_for_score: {checks['holdout_used_for_score']}",
        "```",
        "",
        "## Boundary",
        "",
        "- This route does not launch search.",
        "- This route does not run true1min portfolio reward evaluation.",
        "- It validates schema handoff and feedback safety gates before Phase3CP.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--min-clean-feedback", type=int, default=2)
    parser.add_argument("--loose-min-clean-feedback", type=int, default=1)
    parser.add_argument("--arm-id", default="cem_exploit")
    args = parser.parse_args(argv)

    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    search_root = output_root / "synthetic_search"
    ca_root = output_root / "phase3ca_bridge"
    cm_root = output_root / "phase3cm_reward_fixture"
    cn_output_root = output_root / "phase3cn_feedback_memory"
    ca_report_root = report_root / "phase3ca_bridge"
    cm_report_root = report_root / "phase3cm_reward_fixture"
    cn_report_root = report_root / "phase3cn_feedback_memory"
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    search_file = _write_synthetic_search_fixture(search_root)
    ca_summary = build_candidate_table([search_root], ca_root, top_n=2, allow_high_corr=False)
    ca_table = ca_root / "phase3ca_bz_candidate_audit.csv"
    cm_table = _cm_reward_fixture_from_ca(ca_table, cm_root)
    _copy_report_files(ca_root, ca_report_root)
    _copy_report_files(cm_root, cm_report_root)
    cn_summary = build_feedback_memory(
        cm_tables=[cm_table],
        cm_roots=[],
        output_root=cn_output_root,
        report_root=cn_report_root,
        train_threshold=0.0,
        validation_floor=0.0,
        max_turnover=0.75,
        max_family_share=0.80,
        min_clean_feedback=args.min_clean_feedback,
    )

    strict_context = load_search_feedback_context(
        feedback_table=cn_output_root / "phase3cn_search_feedback_memory.csv",
        arm_score_table=cn_output_root / "phase3cn_arm_score_table.csv",
        family_memory=cn_output_root / "phase3cn_family_score_table.csv",
        blocked_family_table=cn_output_root / "phase3cn_blocked_family_table.csv",
        exploit_allowed_family_table=cn_output_root / "phase3cn_exploit_allowed_family_table.csv",
        arm_id=args.arm_id,
        min_clean_feedback=args.min_clean_feedback,
    )
    loose_context = load_search_feedback_context(
        feedback_table=cn_output_root / "phase3cn_search_feedback_memory.csv",
        arm_score_table=None,
        family_memory=cn_output_root / "phase3cn_family_score_table.csv",
        blocked_family_table=cn_output_root / "phase3cn_blocked_family_table.csv",
        exploit_allowed_family_table=cn_output_root / "phase3cn_exploit_allowed_family_table.csv",
        arm_id=args.arm_id,
        min_clean_feedback=args.loose_min_clean_feedback,
    )
    base_policy = {"policy_version": "phase3cn_integrated_seed", "scores": {"field": {"m1_first_ret": 1.0}, "lane": {"cem": 0.4}}}
    guarded_policy = policy_blocked_by_external_feedback(base_policy, strict_context)
    checks = {
        "ca_candidate_count": int(ca_summary["candidate_count"]),
        "cn_candidate_count": int(cn_summary["candidate_count"]),
        "clean_feedback_count": strict_context.clean_feedback_count,
        "strict_update_allowed": strict_context.feedback_update_allowed,
        "loose_update_allowed": loose_context.feedback_update_allowed,
        "strict_policy_scores_unchanged": guarded_policy.get("scores") == base_policy.get("scores"),
        "holdout_used_for_score": strict_context.holdout_used_for_score or loose_context.holdout_used_for_score,
    }
    passed = (
        checks["ca_candidate_count"] == 2
        and checks["cn_candidate_count"] == 2
        and checks["clean_feedback_count"] == 1
        and checks["strict_update_allowed"] is False
        and checks["loose_update_allowed"] is True
        and checks["strict_policy_scores_unchanged"] is True
        and checks["holdout_used_for_score"] is False
    )
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cn_integrated_feedback_smoke",
        "decision": "PHASE3CN_INTEGRATED_FEEDBACK_SMOKE_PASS_DIAGNOSTIC_ONLY" if passed else "PHASE3CN_INTEGRATED_FEEDBACK_SMOKE_FAIL",
        "search_generation": False,
        "true1min_portfolio_eval": False,
        "paths": {
            "synthetic_search_top_decisions": str(search_file),
            "ca_candidate_audit": str(ca_table),
            "cm_reward_fixture": str(cm_table),
            "cn_feedback_memory": str(cn_output_root / "phase3cn_search_feedback_memory.csv"),
            "report_ca_candidate_audit": str(ca_report_root / "phase3ca_bz_candidate_audit.csv"),
            "report_cm_reward_fixture": str(cm_report_root / "phase3cm_train_reward.csv"),
            "report_cn_feedback_memory": str(cn_report_root / "phase3cn_search_feedback_memory.csv"),
        },
        "checks": checks,
        "strict_feedback_context": strict_context.to_dict(),
        "loose_feedback_context": loose_context.to_dict(),
        "ca_summary": ca_summary,
        "cn_summary": cn_summary,
    }
    _write_json(output_root / "phase3cn_integrated_feedback_smoke_summary.json", summary)
    _write_json(report_root / "phase3cn_integrated_feedback_smoke_summary.json", summary)
    _write_csv(report_root / "phase3cn_integrated_feedback_smoke_checks.csv", [checks])
    (report_root / "PHASE3CN_INTEGRATED_FEEDBACK_SMOKE_20260623.md").write_text(_render_md(summary), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
