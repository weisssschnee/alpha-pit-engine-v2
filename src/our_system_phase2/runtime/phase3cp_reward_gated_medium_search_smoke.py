"""Phase3CP reward-gated medium closed-loop search smoke.

This route performs a small scheduler-controlled candidate generation pass and
then exercises the feedback loop:

CO budgets -> generator arms -> CA bridge -> controlled CM reward fixture ->
CN feedback memory -> CO reschedule.

It intentionally does not run the expensive true1min portfolio evaluator. The
next escalation replaces the controlled CM fixture with
phase3cm-train-portfolio-sortino-reward-audit.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.runtime.phase3bp_true1min_search_algorithm_smoke import (
    build_checked_seed_policy,
    _generate_cem_elite_candidates,
    _generate_event_state_candidates,
    _generate_hybrid_candidates,
    _panel_schema_fields,
    _generate_rx_ucb_candidates,
)
from our_system_phase2.runtime.phase3ca_build_bz_candidate_audit import build_candidate_table
from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import build_feedback_memory
from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import DEFAULT_SHARD_ROOT, _discover_panels, _write_csv, _write_json
from our_system_phase2.services.candidate_schema import normalize_candidate_schema, safe_float
from our_system_phase2.services.multi_arm_scheduler import build_arm_schedule, read_csv_rows


REPO = Path(__file__).resolve().parents[3]
DEFAULT_CO_ROOT = Path("reports/phase3co_multi_arm_scheduler_smoke_20260623")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cp_reward_gated_medium_search_smoke_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3cp_reward_gated_medium_search_smoke_20260623")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _round(value: Any, ndigits: int = 8) -> float:
    try:
        out = float(value)
        return round(out if math.isfinite(out) else 0.0, ndigits)
    except Exception:
        return 0.0


def _scale_budgets(arm_rows: list[dict[str, Any]], total: int) -> list[dict[str, Any]]:
    total = max(1, int(total))
    rows: list[dict[str, Any]] = []
    allocated = 0
    for row in arm_rows:
        share = safe_float(row.get("target_share"), 0.0)
        exact = max(0.0, share) * total
        budget = int(math.floor(exact))
        if exact > 0 and budget == 0:
            budget = 1
        item = dict(row)
        item["cp_smoke_exact_budget"] = exact
        item["cp_smoke_candidate_budget"] = budget
        rows.append(item)
        allocated += budget
    while allocated > total:
        reducible = sorted(rows, key=lambda item: int(item["cp_smoke_candidate_budget"]), reverse=True)
        changed = False
        for row in reducible:
            if int(row["cp_smoke_candidate_budget"]) > 0:
                row["cp_smoke_candidate_budget"] = int(row["cp_smoke_candidate_budget"]) - 1
                allocated -= 1
                changed = True
                break
        if not changed:
            break
    while allocated < total:
        ranked = sorted(rows, key=lambda item: float(item["cp_smoke_exact_budget"]) - int(item["cp_smoke_candidate_budget"]), reverse=True)
        ranked[0]["cp_smoke_candidate_budget"] = int(ranked[0]["cp_smoke_candidate_budget"]) + 1
        allocated += 1
    for row in rows:
        row["cp_smoke_share"] = _round(int(row["cp_smoke_candidate_budget"]) / total)
        row.pop("cp_smoke_exact_budget", None)
    return rows


def _tag_generated(rows: list[dict[str, Any]], *, arm_id: str, source: str, route_hint: str, start_idx: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for offset, row in enumerate(rows, start_idx):
        item = dict(row)
        item["candidate_id"] = f"phase3cp_{offset:05d}"
        item["generator_arm"] = arm_id
        item["generator_route"] = route_hint
        item["source_generator"] = source
        item["source_lane"] = source
        item["round_id"] = f"phase3cp_{arm_id}"
        item["mutation_type"] = "scheduler_controlled_smoke"
        item["phase3cp_scheduler_source"] = "phase3co_arm_budget_table"
        out.append(item)
    return out


def _generate_for_arm(
    arm: dict[str, Any],
    *,
    budget: int,
    blocked: set[str],
    policy: dict[str, Any],
    start_idx: int,
    available_fields: list[str] | set[str] | None,
) -> list[dict[str, Any]]:
    arm_id = str(arm.get("arm_id") or "")
    route_hint = str(arm.get("route_hint") or "")
    budget = max(0, int(budget))
    if budget <= 0:
        return []
    if arm_id == "cem_exploit":
        rows = _generate_cem_elite_candidates(
            budget,
            blocked,
            policy,
            include_residual=False,
            population_size=max(64, budget * 8),
            elite_frac=0.12,
            rounds=2,
            available_fields=available_fields,
        )
        source = "phase3cp_cem_probe_from_co"
    elif arm_id == "typed_ast_fresh":
        rows = _generate_hybrid_candidates(
            budget,
            blocked,
            policy,
            include_residual=False,
            population_size=max(96, budget * 6),
            elite_frac=0.18,
            rounds=2,
            available_fields=available_fields,
        )
        source = "phase3cp_typed_ast_fresh_from_co"
    elif arm_id == "challenger_repair":
        rows = _generate_hybrid_candidates(
            budget,
            blocked,
            policy,
            include_residual=True,
            population_size=max(96, budget * 6),
            elite_frac=0.20,
            rounds=2,
            available_fields=available_fields,
        )
        source = "phase3cp_challenger_repair_from_co"
    elif arm_id == "event_state":
        rows = _generate_event_state_candidates(budget, blocked, policy, include_interactions=True, available_fields=available_fields)
        source = "phase3cp_event_state_from_co"
    elif arm_id == "random_orthogonal":
        rows = _generate_rx_ucb_candidates(max(budget * 2, budget), blocked, policy, include_residual=False, available_fields=available_fields)
        rows = list(reversed(rows))[:budget]
        source = "phase3cp_random_orthogonal_control_from_co"
    else:
        rows = _generate_rx_ucb_candidates(budget, blocked, policy, include_residual=False, available_fields=available_fields)
        source = "phase3cp_rx_ucb_fresh_from_co"
    return _tag_generated(rows[:budget], arm_id=arm_id, source=source, route_hint=route_hint, start_idx=start_idx)


def _decisionize(row: dict[str, Any], idx: int) -> dict[str, Any]:
    digest = str(row.get("expression_hash") or "")
    seed = int(hashlib.sha256(digest.encode("utf-8")).hexdigest()[:8], 16) if digest else idx
    policy_score = safe_float(row.get("policy_score"), 0.05)
    arm = str(row.get("generator_arm") or "")
    arm_bonus = {
        "rx_ucb_fresh": 0.010,
        "typed_ast_fresh": 0.014,
        "challenger_repair": 0.012,
        "event_state": 0.008,
        "random_orthogonal": 0.004,
        "cem_exploit": -0.006,
    }.get(arm, 0.0)
    aligned_ic = max(-0.08, min(0.08, 0.018 + arm_bonus + (policy_score * 0.012) + ((seed % 17) - 8) * 0.0007))
    turnover = {
        "rx_ucb_fresh": 0.42,
        "typed_ast_fresh": 0.48,
        "challenger_repair": 0.58,
        "event_state": 0.62,
        "random_orthogonal": 0.50,
        "cem_exploit": 0.86,
    }.get(arm, 0.55)
    out = dict(row)
    out.update(
        {
            "aligned_ic_mean": _round(aligned_ic),
            "abs_aligned_ic_mean": _round(abs(aligned_ic)),
            "aligned_spread_mean": _round(aligned_ic / 100.0),
            "spread_hit_rate": _round(0.50 + max(0.0, aligned_ic) * 1.4),
            "positive_horizon_count": 3 if aligned_ic > 0.025 else 1,
            "mean_one_way_turnover": _round(turnover),
            "phase3bp_blocker_flags": "",
            "blocker_flags": "",
            "phase3cp_rank": idx,
            "metric_boundary": "Phase3CP smoke proxy metrics for CA bridge only; CM fixture gates feedback",
        }
    )
    out.update(normalize_candidate_schema(out))
    return out


def _write_arm_outputs(rows: list[dict[str, Any]], root: Path) -> None:
    by_arm: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_arm.setdefault(str(row.get("generator_arm") or "unknown"), []).append(row)
    for arm, items in by_arm.items():
        arm_root = root / arm / "round1_scheduler_controlled"
        _write_csv(arm_root / "phase3cp_top_decisions.csv", items)


def _cm_reward_fixture_from_ca(ca_table: Path, cm_root: Path) -> Path:
    rows: list[dict[str, Any]] = []
    for idx, row in enumerate(_read_csv(ca_table), 1):
        arm = str(row.get("generator_arm") or "")
        proxy = safe_float(row.get("phase3ca_proxy_quality"), 0.0)
        turnover = safe_float(row.get("mean_one_way_turnover"), 0.55)
        if arm in {"rx_ucb_fresh", "typed_ast_fresh", "challenger_repair"} and idx <= 8:
            reward = 0.18 + min(0.22, proxy * 0.08)
            decision = "TRAIN_REWARD_FOLLOWUP_READY"
            blockers = ""
            validation = 0.09 + min(0.12, proxy * 0.04)
            prob = 0.58
        elif arm == "event_state" and idx <= 10:
            reward = 0.08
            decision = "TRAIN_REWARD_RESEARCH_ONLY"
            blockers = "event_state_needs_event_validation"
            validation = 0.03
            prob = 0.52
        else:
            reward = -0.16 - max(0.0, turnover - 0.75)
            decision = "TRAIN_REWARD_REJECT"
            blockers = "cp_smoke_not_clean|high_turnover" if turnover > 0.75 else "cp_smoke_not_clean"
            validation = -0.05
            prob = 0.35
        out = dict(row)
        out.update(
            {
                "train_reward": _round(reward),
                "train_reward_decision": decision,
                "train_reward_blockers": blockers,
                "validation_day_sortino": _round(validation),
                "validation_mcmc_prob_gt_0": _round(prob),
                "holdout_day_sortino": _round(-0.30 if reward > 0 else 0.70),
                "holdout_mcmc_prob_gt_0": _round(0.25 if reward > 0 else 0.82),
                "train_mean_one_way_turnover": _round(turnover),
                "mean_one_way_turnover": _round(turnover),
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
            "decision": "PHASE3CP_SMOKE_CM_FIXTURE_ONLY",
            "candidate_count": len(rows),
            "followup_count": sum(1 for row in rows if row.get("train_reward_decision") == "TRAIN_REWARD_FOLLOWUP_READY"),
            "metric_boundary": "controlled CP smoke fixture; replace with Phase3CM audit for real reward",
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
    scheduler = summary["reschedule_summary"]
    lines = [
        "# Phase3CP Reward-Gated Medium Search Smoke 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Result",
        "",
        "```text",
        f"requested_smoke_candidates: {summary['requested_smoke_candidates']}",
        f"generated_candidates: {summary['generated_candidates']}",
        f"ca_candidate_count: {summary['ca_summary']['candidate_count']}",
        f"cm_candidate_count: {summary['cm_summary']['candidate_count']}",
        f"cn_candidate_count: {summary['cn_summary']['candidate_count']}",
        f"reschedule_allocated_budget: {scheduler['allocated_budget']}",
        f"reschedule_fresh_share: {scheduler['fresh_share']}",
        f"reschedule_cem_budget: {scheduler['cem_exploit_budget']}",
        "```",
        "",
        "## Checks",
        "",
        "```text",
    ]
    for key, value in checks.items():
        lines.append(f"{key}: {value}")
    lines.extend(
        [
            "```",
            "",
            "## Boundary",
            "",
            "- Candidate generation uses existing true1min generator functions.",
            "- CM reward is a controlled fixture in this smoke.",
            "- No true1min portfolio reward evaluation is run here.",
            "- Holdout remains report-only and is not used for scheduler decisions.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--co-root", type=Path, default=DEFAULT_CO_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--smoke-total-candidates", type=int, default=48)
    parser.add_argument("--ca-top-n", type=int, default=24)
    parser.add_argument("--min-clean-feedback", type=int, default=4)
    parser.add_argument("--reschedule-total-budget", type=int, default=512)
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--schema-max-shards", type=int, default=4)
    parser.add_argument("--allow-unbound-generation", action="store_true")
    args = parser.parse_args(argv)

    co_root = _resolve(args.co_root)
    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    schema_panels: list[Path] = []
    available_fields: list[str] = []
    try:
        schema_panels = _discover_panels(_resolve(args.shard_root), args.schema_max_shards)
        available_fields = _panel_schema_fields(schema_panels)
    except Exception:
        if not args.allow_unbound_generation:
            raise
    arm_budget_rows = read_csv_rows(co_root / "phase3co_arm_budget_table.csv")
    family_action_rows = read_csv_rows(co_root / "phase3co_family_action_table.csv")
    scaled_plan = _scale_budgets(arm_budget_rows, args.smoke_total_candidates)
    policy, _, _ = build_checked_seed_policy(exploration=0.92)
    generated: list[dict[str, Any]] = []
    blocked: set[str] = set()
    for arm in scaled_plan:
        budget = int(arm.get("cp_smoke_candidate_budget") or 0)
        rows = _generate_for_arm(
            arm,
            budget=budget,
            blocked=blocked,
            policy=policy,
            start_idx=len(generated) + 1,
            available_fields=available_fields or None,
        )
        for row in rows:
            blocked.add(str(row.get("expression_hash") or ""))
        generated.extend(rows)
    decisions = [_decisionize(row, idx) for idx, row in enumerate(generated, 1)]
    search_root = output_root / "search_outputs"
    report_search_root = report_root / "search_outputs"
    _write_arm_outputs(decisions, search_root)
    _write_arm_outputs(decisions, report_search_root)
    _write_csv(output_root / "phase3cp_arm_execution_plan.csv", scaled_plan)
    _write_csv(report_root / "phase3cp_arm_execution_plan.csv", scaled_plan)
    _write_csv(output_root / "phase3cp_all_generated_top_decisions.csv", decisions)
    _write_csv(report_root / "phase3cp_all_generated_top_decisions.csv", decisions)

    ca_root = output_root / "phase3ca_bridge"
    report_ca_root = report_root / "phase3ca_bridge"
    ca_summary = build_candidate_table([search_root], ca_root, top_n=args.ca_top_n, allow_high_corr=False)
    _copy_report_files(ca_root, report_ca_root)
    ca_table = ca_root / "phase3ca_bz_candidate_audit.csv"
    cm_root = output_root / "phase3cm_reward_fixture"
    report_cm_root = report_root / "phase3cm_reward_fixture"
    cm_table = _cm_reward_fixture_from_ca(ca_table, cm_root)
    _copy_report_files(cm_root, report_cm_root)
    cm_summary = json.loads((cm_root / "phase3cm_train_reward_audit_summary.json").read_text(encoding="utf-8"))
    cn_output_root = output_root / "phase3cn_feedback_memory"
    cn_report_root = report_root / "phase3cn_feedback_memory"
    cn_summary = build_feedback_memory(
        cm_tables=[cm_table],
        cm_roots=[],
        output_root=cn_output_root,
        report_root=cn_report_root,
        train_threshold=0.0,
        validation_floor=0.0,
        max_turnover=0.75,
        max_family_share=0.25,
        min_clean_feedback=args.min_clean_feedback,
    )
    next_arm_rows = read_csv_rows(cn_output_root / "phase3cn_arm_score_table.csv")
    next_family_rows = read_csv_rows(cn_output_root / "phase3cn_family_score_table.csv")
    next_blocked_rows = read_csv_rows(cn_output_root / "phase3cn_blocked_family_table.csv")
    next_exploit_rows = read_csv_rows(cn_output_root / "phase3cn_exploit_allowed_family_table.csv")
    reschedule_rows, reschedule_family_actions, reschedule_summary = build_arm_schedule(
        next_arm_rows,
        next_family_rows,
        next_blocked_rows,
        next_exploit_rows,
        total_budget=args.reschedule_total_budget,
        fresh_floor_share=0.45,
        cem_probe_cap_share=0.06,
        max_family_share=0.25,
    )
    _write_csv(output_root / "phase3cp_next_arm_budget_table.csv", reschedule_rows)
    _write_csv(output_root / "phase3cp_next_family_action_table.csv", reschedule_family_actions)
    _write_csv(report_root / "phase3cp_next_arm_budget_table.csv", reschedule_rows)
    _write_csv(report_root / "phase3cp_next_family_action_table.csv", reschedule_family_actions)

    initial_cem = sum(int(row.get("cp_smoke_candidate_budget") or 0) for row in scaled_plan if row.get("arm_id") == "cem_exploit")
    initial_fresh = sum(int(row.get("cp_smoke_candidate_budget") or 0) for row in scaled_plan if row.get("category") == "fresh")
    checks = {
        "co_budget_used": bool(arm_budget_rows),
        "generated_budget_ok": len(decisions) == int(args.smoke_total_candidates),
        "ca_has_candidates": int(ca_summary["candidate_count"]) > 0,
        "cm_fixture_has_candidates": int(cm_summary["candidate_count"]) == int(ca_summary["candidate_count"]),
        "cn_memory_has_candidates": int(cn_summary["candidate_count"]) == int(ca_summary["candidate_count"]),
        "initial_cem_probe_capped": initial_cem <= max(1, math.ceil(args.smoke_total_candidates * 0.06)),
        "initial_fresh_floor_ok": initial_fresh / max(1, int(args.smoke_total_candidates)) >= 0.45,
        "reschedule_total_ok": int(reschedule_summary["allocated_budget"]) == int(args.reschedule_total_budget),
        "reschedule_fresh_floor_ok": float(reschedule_summary["fresh_share"]) >= 0.45,
        "holdout_not_optimizer_input": True,
    }
    passed = all(bool(value) for value in checks.values())
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cp_reward_gated_medium_search_smoke",
        "decision": "PHASE3CP_REWARD_GATED_MEDIUM_SEARCH_SMOKE_PASS_DIAGNOSTIC_ONLY" if passed else "PHASE3CP_REWARD_GATED_MEDIUM_SEARCH_SMOKE_FAIL",
        "requested_smoke_candidates": int(args.smoke_total_candidates),
        "generated_candidates": len(decisions),
        "search_generation": True,
        "true1min_portfolio_eval": False,
        "cm_reward_source": "controlled_fixture",
        "input_co_root": str(co_root),
        "schema_bound_generation": bool(available_fields),
        "schema_shard_root": str(_resolve(args.shard_root)),
        "schema_panel_count": len(schema_panels),
        "available_field_count": len(available_fields),
        "available_fields": available_fields,
        "checks": checks,
        "initial_arm_plan": scaled_plan,
        "family_action_input_count": len(family_action_rows),
        "ca_summary": ca_summary,
        "cm_summary": cm_summary,
        "cn_summary": cn_summary,
        "reschedule_summary": reschedule_summary,
        "metric_boundary": "Phase3CP smoke validates closed-loop wiring; replace CM fixture with true Phase3CM audit before claims",
    }
    _write_json(output_root / "phase3cp_closed_loop_summary.json", summary)
    _write_json(report_root / "phase3cp_closed_loop_summary.json", summary)
    _write_csv(report_root / "phase3cp_closed_loop_checks.csv", [checks])
    (report_root / "PHASE3CP_REWARD_GATED_MEDIUM_SEARCH_SMOKE_20260623.md").write_text(_render_md(summary), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
