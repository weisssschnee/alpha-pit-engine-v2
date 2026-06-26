"""Adaptive true-1min UCB-CEM practice.

Phase3BS tests whether CEM improves when it is updated from real true-1min
evaluation feedback instead of using a one-shot prior. It runs:

1. RX/UCB high-exploration seed.
2. Adaptive CEM resampling from seed feedback.
3. Adaptive RX-CEM hybrid from the updated policy.

This is algorithm practice only. It does not promote alpha and does not modify
X0/R3.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _discover_panels,
    _f,
    _run_materialization,
    _write_csv,
    _write_json,
)
from our_system_phase2.runtime.phase3bn_open_diversified_true1min_canary import (
    DEFAULT_MEMORY_ROOT,
    _load_memory_hashes,
    _prior_hashes,
)
from our_system_phase2.runtime.phase3bp_true1min_search_algorithm_smoke import (
    PRIOR_HASH_FILES,
    _aggregate_decisions,
    _ast_variables,
    _fields,
    _generate_cem_elite_candidates,
    _generate_hybrid_candidates,
    _panel_schema_fields,
    _generate_rx_ucb_candidates,
    _operators,
    _summarize_by,
    _windows,
    build_checked_seed_policy,
)
from our_system_phase2.runtime.phase3bq_compute_allocation_benchmark import (
    _fmt,
    _hot_path_scan,
    _package_versions,
)
from our_system_phase2.services.search_feedback import (
    annotate_policy_with_external_feedback,
    clean_optimizer_feedback_rows,
    load_search_feedback_context,
    load_search_feedback_rows,
    policy_blocked_by_external_feedback,
)


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3bs_adaptive_ucb_cem_practice_ast_v3_20260615")
DEFAULT_REPORT_ROOT = Path("reports/phase3bs_adaptive_ucb_cem_practice_ast_v3_20260615")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _quality(row: dict[str, Any]) -> float:
    blockers = str(row.get("phase3bp_blocker_flags") or row.get("blocker_flags") or "")
    future = "future_signal_wrong_lag_too_strong" in blockers
    crowded = "signal_corr_abs" in blockers
    turnover = _f(row.get("mean_one_way_turnover"), 0.0)
    abs_ic = abs(_f(row.get("abs_aligned_ic_mean") or row.get("aligned_ic_mean"), 0.0))
    stable = int(_f(row.get("positive_horizon_count"), 0.0))
    score = (0.75 * min(0.25, abs_ic)) + (0.025 * min(4, stable))
    if not future:
        score += 0.04
    if future:
        score -= 0.22
    if crowded:
        score -= 0.07
    if turnover > 0.95:
        score -= 0.04
    if turnover < 0.25:
        score -= 0.015
    return float(max(-0.35, min(0.35, score)))


def _feedback_eligible(row: dict[str, Any]) -> bool:
    blockers = str(row.get("phase3bp_blocker_flags") or row.get("blocker_flags") or "")
    if "future_signal_wrong_lag_too_strong" in blockers:
        return False
    if "signal_corr_abs" in blockers:
        return False
    turnover = _f(row.get("mean_one_way_turnover"), 0.0)
    abs_ic = abs(_f(row.get("abs_aligned_ic_mean") or row.get("aligned_ic_mean"), 0.0))
    stable = int(_f(row.get("positive_horizon_count"), 0.0))
    return turnover <= 0.98 and abs_ic >= 0.025 and stable >= 1


def _policy_with_feedback(
    base_policy: dict[str, Any],
    decisions: list[dict[str, Any]],
    *,
    learning_rate: float,
    entropy_floor: float,
    min_eligible: int = 32,
) -> dict[str, Any]:
    policy = copy.deepcopy(base_policy)
    policy["policy_version"] = "phase3bs_adaptive_ucb_cem_feedback_v1"
    policy["scope"] = "true1min_feedback_updated_generator_policy_not_production_reward"
    scores = policy.setdefault("scores", {})
    credits: dict[str, dict[str, list[float]]] = {
        "field": defaultdict(list),
        "operator": defaultdict(list),
        "window": defaultdict(list),
        "lane": defaultdict(list),
        "fieldset": defaultdict(list),
        "ast_skeleton": defaultdict(list),
        "ast_operator_sequence": defaultdict(list),
        "ast_operator_multiset": defaultdict(list),
        "ast_root_operator": defaultdict(list),
        "ast_depth_bin": defaultdict(list),
        "ast_operator_count_bin": defaultdict(list),
        "ast_field_count_bin": defaultdict(list),
        "ast_window_count_bin": defaultdict(list),
        "ast_max_window_bin": defaultdict(list),
        "ast_complexity_bin": defaultdict(list),
    }
    eligible_decisions = [row for row in decisions if _feedback_eligible(row)]
    if len(eligible_decisions) < max(1, int(min_eligible)):
        policy["feedback"] = {
            "decision_count": len(decisions),
            "eligible_decision_count": len(eligible_decisions),
            "min_eligible_decision_count": int(min_eligible),
            "learning_rate": learning_rate,
            "entropy_floor": entropy_floor,
            "top_feedback": {},
            "guardrail": "insufficient clean decisions; CEM/UCB feedback left unchanged",
        }
        policy["top_keys"] = {
            kind: sorted(values.items(), key=lambda item: item[1], reverse=True)[:12]
            for kind, values in scores.items()
        }
        return policy
    for row in eligible_decisions:
        expression = str(row.get("expression") or "")
        fields = _fields(expression)
        fieldset = "|".join(fields)
        lane = str(row.get("factor_lane") or row.get("source_lane") or "unknown")
        ast = _ast_variables(expression)
        quality = _quality(row)
        credits["lane"][lane].append(quality)
        credits["fieldset"][fieldset].append(quality)
        for key in (
            "ast_skeleton",
            "ast_operator_sequence",
            "ast_operator_multiset",
            "ast_root_operator",
            "ast_depth_bin",
            "ast_operator_count_bin",
            "ast_field_count_bin",
            "ast_window_count_bin",
            "ast_max_window_bin",
            "ast_complexity_bin",
        ):
            credits[key][str(ast[key])].append(quality)
        for field in fields:
            credits["field"][field].append(quality)
        for op in _operators(expression):
            credits["operator"][op].append(quality)
        for win in _windows(expression):
            credits["window"][win].append(quality)

    feedback_summary: dict[str, list[tuple[str, float]]] = {}
    for kind, items in credits.items():
        table = scores.setdefault(kind, {})
        ranked: list[tuple[str, float]] = []
        for key, values in items.items():
            mean_credit = sum(values) / max(1, len(values))
            old = float(table.get(key, entropy_floor))
            new = ((1.0 - learning_rate) * old) + (learning_rate * mean_credit)
            table[key] = round(max(entropy_floor, min(1.25, new)), 6)
            ranked.append((key, round(mean_credit, 6)))
        ranked.sort(key=lambda item: item[1], reverse=True)
        feedback_summary[kind] = ranked[:16]
    policy["feedback"] = {
        "decision_count": len(decisions),
        "eligible_decision_count": len(eligible_decisions),
        "min_eligible_decision_count": int(min_eligible),
        "learning_rate": learning_rate,
        "entropy_floor": entropy_floor,
        "top_feedback": feedback_summary,
        "guardrail": "future wrong-lag and signal-crowded rows excluded from policy feedback",
    }
    policy["top_keys"] = {
        kind: sorted(values.items(), key=lambda item: item[1], reverse=True)[:12]
        for kind, values in scores.items()
    }
    return policy


def _policy_with_train_reward_feedback(
    base_policy: dict[str, Any],
    feedback_rows: list[dict[str, Any]],
    *,
    context: Any,
    learning_rate: float,
    entropy_floor: float,
    min_eligible: int = 32,
) -> dict[str, Any]:
    policy = copy.deepcopy(base_policy)
    policy["policy_version"] = "phase3bs_external_cm_train_reward_feedback_v1"
    policy["scope"] = "external_phase3cm_train_reward_updates_generator_policy_validation_holdout_report_only"
    scores = policy.setdefault("scores", {})
    eligible_rows = clean_optimizer_feedback_rows(
        feedback_rows,
        arm_id=getattr(context, "arm_id", ""),
        train_threshold=0.0,
        max_turnover=0.75,
    )
    if len(eligible_rows) < max(1, int(min_eligible)):
        policy["feedback"] = {
            "decision_count": len(feedback_rows),
            "eligible_decision_count": len(eligible_rows),
            "min_eligible_decision_count": int(min_eligible),
            "learning_rate": 0.0,
            "entropy_floor": entropy_floor,
            "top_feedback": {},
            "updated": False,
            "guardrail": "external CM train reward feedback below threshold; CEM/UCB policy scores unchanged",
            "optimizer_reward_source": "train_only_phase3cm",
            "validation_used_for_score": False,
            "holdout_used_for_score": False,
            "phase3cn_external_feedback": context.to_dict() if hasattr(context, "to_dict") else {},
        }
        policy["top_keys"] = {
            kind: sorted(values.items(), key=lambda item: item[1], reverse=True)[:12]
            for kind, values in scores.items()
            if isinstance(values, dict)
        }
        return policy

    credits: dict[str, dict[str, list[float]]] = {
        "field": defaultdict(list),
        "operator": defaultdict(list),
        "window": defaultdict(list),
        "lane": defaultdict(list),
        "fieldset": defaultdict(list),
        "ast_skeleton": defaultdict(list),
        "ast_operator_sequence": defaultdict(list),
        "ast_operator_multiset": defaultdict(list),
        "ast_root_operator": defaultdict(list),
        "ast_depth_bin": defaultdict(list),
        "ast_operator_count_bin": defaultdict(list),
        "ast_field_count_bin": defaultdict(list),
        "ast_window_count_bin": defaultdict(list),
        "ast_max_window_bin": defaultdict(list),
        "ast_complexity_bin": defaultdict(list),
    }
    for row in eligible_rows:
        expression = str(row.get("expression") or "")
        fields = _fields(expression)
        fieldset = "|".join(fields)
        lane = str(row.get("factor_lane") or row.get("source_lane") or row.get("generator_arm") or "unknown")
        ast = _ast_variables(expression)
        reward = _f(row.get("optimizer_reward") or row.get("train_reward"), 0.0)
        quality = float(max(-0.35, min(0.35, reward)))
        credits["lane"][lane].append(quality)
        credits["fieldset"][fieldset].append(quality)
        for key in (
            "ast_skeleton",
            "ast_operator_sequence",
            "ast_operator_multiset",
            "ast_root_operator",
            "ast_depth_bin",
            "ast_operator_count_bin",
            "ast_field_count_bin",
            "ast_window_count_bin",
            "ast_max_window_bin",
            "ast_complexity_bin",
        ):
            credits[key][str(ast[key])].append(quality)
        for field in fields:
            credits["field"][field].append(quality)
        for op in _operators(expression):
            credits["operator"][op].append(quality)
        for win in _windows(expression):
            credits["window"][win].append(quality)

    feedback_summary: dict[str, list[tuple[str, float]]] = {}
    for kind, items in credits.items():
        table = scores.setdefault(kind, {})
        ranked: list[tuple[str, float]] = []
        for key, values in items.items():
            mean_credit = sum(values) / max(1, len(values))
            old = float(table.get(key, entropy_floor))
            new = ((1.0 - learning_rate) * old) + (learning_rate * mean_credit)
            table[key] = round(max(entropy_floor, min(1.25, new)), 6)
            ranked.append((key, round(mean_credit, 6)))
        ranked.sort(key=lambda item: item[1], reverse=True)
        feedback_summary[kind] = ranked[:16]
    policy["feedback"] = {
        "decision_count": len(feedback_rows),
        "eligible_decision_count": len(eligible_rows),
        "min_eligible_decision_count": int(min_eligible),
        "learning_rate": learning_rate,
        "entropy_floor": entropy_floor,
        "top_feedback": feedback_summary,
        "updated": True,
        "guardrail": "policy updated from external Phase3CM train reward only; validation/holdout report-only",
        "optimizer_reward_source": "train_only_phase3cm",
        "validation_used_for_score": False,
        "holdout_used_for_score": False,
        "phase3cn_external_feedback": context.to_dict() if hasattr(context, "to_dict") else {},
    }
    policy["top_keys"] = {
        kind: sorted(values.items(), key=lambda item: item[1], reverse=True)[:12]
        for kind, values in scores.items()
        if isinstance(values, dict)
    }
    return policy


def _round_metrics(round_id: str, candidates: list[dict[str, Any]], decisions: list[dict[str, Any]], meta: dict[str, Any], elapsed: float) -> dict[str, Any]:
    hard_blocked = []
    non_future = []
    research_pool = []
    for row in decisions:
        blockers = str(row.get("phase3bp_blocker_flags") or row.get("blocker_flags") or "")
        future = "future_signal_wrong_lag_too_strong" in blockers
        crowded = "signal_corr_abs" in blockers
        turnover = _f(row.get("mean_one_way_turnover"), 0.0)
        abs_ic = abs(_f(row.get("abs_aligned_ic_mean") or row.get("aligned_ic_mean"), 0.0))
        stable = int(_f(row.get("positive_horizon_count"), 0.0))
        if future or crowded:
            hard_blocked.append(row)
        if not future:
            non_future.append(row)
        if (not future) and turnover <= 0.98 and abs_ic >= 0.025 and stable >= 1:
            research_pool.append(row)
    fields = {str(row.get("fields") or "") for row in decisions if row.get("fields")}
    lanes = {str(row.get("factor_lane") or "") for row in decisions if row.get("factor_lane")}
    ast_skeletons = {str(_ast_variables(str(row.get("expression") or "")).get("ast_skeleton")) for row in decisions if row.get("expression")}
    ast_shapes = {
        "|".join(
            str(_ast_variables(str(row.get("expression") or "")).get(key))
            for key in ("ast_depth_bin", "ast_operator_count_bin", "ast_field_count_bin", "ast_complexity_bin")
        )
        for row in decisions
        if row.get("expression")
    }
    top_abs = sorted([abs(_f(row.get("abs_aligned_ic_mean") or row.get("aligned_ic_mean"), 0.0)) for row in decisions], reverse=True)
    top10_mean = sum(top_abs[:10]) / max(1, min(10, len(top_abs)))
    total_eval_rows = sum(int(shard.get("eval_rows") or 0) for shard in meta.get("shards", []))
    score = (
        (0.45 * len(research_pool) / max(1, len(decisions)))
        + (0.20 * len(non_future) / max(1, len(decisions)))
        + (0.15 * min(1.0, len(lanes) / 18.0))
        + (0.12 * min(1.0, len(fields) / 30.0))
        + (0.08 * min(1.0, len(ast_shapes) / 24.0))
        + (0.08 * min(1.0, top10_mean / 0.08))
    )
    return {
        "round_id": round_id,
        "elapsed_seconds": round(elapsed, 3),
        "candidate_count": len(candidates),
        "decision_rows": len(decisions),
        "panel_count": len(meta.get("shards", [])),
        "sample_trade_times_per_shard": meta.get("sample_trade_times_per_shard"),
        "total_eval_rows": total_eval_rows,
        "rows_per_second": round(total_eval_rows / elapsed, 3) if elapsed > 0 else 0.0,
        "legacy_followup_count": sum(1 for row in decisions if row.get("phase3bp_decision") == "bp_followup_priority"),
        "hard_blocked_count": len(hard_blocked),
        "hard_blocked_ratio": round(len(hard_blocked) / max(1, len(decisions)), 6),
        "non_future_count": len(non_future),
        "research_pool_count": len(research_pool),
        "research_pool_ratio": round(len(research_pool) / max(1, len(decisions)), 6),
        "unique_lane_count": len(lanes),
        "unique_fieldset_count": len(fields),
        "unique_ast_skeleton_count": len(ast_skeletons),
        "unique_ast_shape_count": len(ast_shapes),
        "top10_abs_ic_mean": round(top10_mean, 10),
        "best_abs_ic": round(max(top_abs, default=0.0), 10),
        "research_quality_score": round(score, 8),
    }


def _evaluate_round(
    *,
    round_id: str,
    candidates: list[dict[str, Any]],
    panels: list[Path],
    horizons: tuple[int, ...],
    sample_trade_times_per_shard: int,
    min_obs_per_time: int,
    output_root: Path,
    report_root: Path,
    top_decisions: int,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    import time

    started = time.perf_counter()
    metric_rows, aggregate_rows, meta = _run_materialization(
        candidates=candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=sample_trade_times_per_shard,
        min_obs_per_time=min_obs_per_time,
    )
    pairwise_rows = meta.pop("pairwise_rows")
    decisions = _aggregate_decisions(aggregate_rows, pairwise_rows, candidates, top_decisions)
    elapsed = time.perf_counter() - started
    round_output = output_root / round_id
    round_report = report_root / round_id
    round_output.mkdir(parents=True, exist_ok=True)
    round_report.mkdir(parents=True, exist_ok=True)
    generator_rows = _summarize_by(decisions, "source_generator")
    lane_rows = _summarize_by(decisions, "factor_lane")
    metrics = _round_metrics(round_id, candidates, decisions, meta, elapsed)
    _write_csv(round_output / "phase3bs_candidate_pack.csv", candidates)
    _write_csv(round_output / "phase3bs_candidate_horizon_shard_metrics.csv", metric_rows)
    _write_csv(round_output / "phase3bs_candidate_horizon_aggregate.csv", aggregate_rows)
    _write_csv(round_output / "phase3bs_pairwise_signal_rank_corr.csv", pairwise_rows)
    _write_csv(round_output / "phase3bs_top_decisions.csv", decisions)
    _write_json(round_output / "phase3bs_round_summary.json", {**metrics, **meta})
    _write_csv(round_report / "phase3bs_candidate_pack.csv", candidates)
    _write_csv(round_report / "phase3bs_top_decisions.csv", decisions)
    _write_csv(round_report / "phase3bs_generator_summary.csv", generator_rows)
    _write_csv(round_report / "phase3bs_lane_summary.csv", lane_rows)
    _write_json(round_report / "phase3bs_round_summary.json", {**metrics, **meta, "top_decisions": decisions[:12]})
    return decisions, metrics, meta


def _tag_candidates(rows: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    tagged: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, 1):
        item = dict(row)
        item["candidate_id"] = f"phase3bs_{idx:05d}"
        item["source_generator"] = source
        item["source_lane"] = source
        item["note"] = f"{source}: {row.get('note')}"
        tagged.append(item)
    return tagged


def _mix_unique(primary: list[dict[str, Any]], secondary: list[dict[str, Any]], *, max_candidates: int, source: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in [*primary, *secondary]:
        digest = str(row.get("expression_hash"))
        if digest in seen:
            continue
        seen.add(digest)
        rows.append(row)
        if len(rows) >= max_candidates:
            break
    return _tag_candidates(rows, source)


def _policy_with_entropy_boost(policy: dict[str, Any], *, boost: float, floor: float) -> dict[str, Any]:
    out = copy.deepcopy(policy)
    out["policy_version"] = f"{policy.get('policy_version')}_entropy_boost"
    for table in (out.get("scores") or {}).values():
        if isinstance(table, dict):
            for key, value in list(table.items()):
                table[key] = round(max(floor, (float(value) * boost) + floor), 6)
    out.setdefault("feedback", {})["entropy_boost"] = {"boost": boost, "floor": floor}
    return out


def _render_md(summary: dict[str, Any]) -> str:
    lines = [
        "# Phase3BS Adaptive UCB-CEM Practice 2026-06-15",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Purpose",
        "",
        "Test AST-aware adaptive UCB-CEM as a multi-round generator: RX/UCB seed, feedback-updated CEM, adaptive hybrid, and two CEM-dominant variants.",
        "The winner metric is research-pool quality, not first clean followup.",
        "",
        "## Round Results",
        "",
        "| round | candidates | sec | rows | rows/sec | legacy followup | hard-blocked | research pool | lanes | fieldsets | ast shapes | top10 abs IC | score |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary["rounds"]:
        lines.append(
            f"| `{row['round_id']}` | {row['candidate_count']} | {_fmt(row['elapsed_seconds'])} | {row['total_eval_rows']} | "
            f"{_fmt(row['rows_per_second'])} | {row['legacy_followup_count']} | {_fmt(row['hard_blocked_ratio'])} | "
            f"{row['research_pool_count']} | {row['unique_lane_count']} | {row['unique_fieldset_count']} | {row['unique_ast_shape_count']} | "
            f"{_fmt(row['top10_abs_ic_mean'])} | {_fmt(row['research_quality_score'])} |"
        )
    rec = summary["recommendation"]
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            f"- best round: `{rec['best_round']}`",
            f"- decision: `{rec['decision']}`",
            f"- interpretation: {rec['interpretation']}",
            "",
            "## Feedback",
            "",
            f"- seed feedback updated policy: `{summary['adaptive_policy']['policy_version']}`",
            f"- top feedback lanes: `{summary['adaptive_policy']['feedback']['top_feedback'].get('lane')}`",
            f"- top feedback fields: `{summary['adaptive_policy']['feedback']['top_feedback'].get('field')}`",
            f"- top feedback AST shapes: `{summary['adaptive_policy']['feedback']['top_feedback'].get('ast_complexity_bin')}`",
            f"- top feedback AST operator sequences: `{summary['adaptive_policy']['feedback']['top_feedback'].get('ast_operator_sequence')}`",
            "",
            "## Boundary",
            "",
            "- True `trade_time` 1min panels only.",
            "- No old daily stock-PIT default panel.",
            "- No X0/R3 modification.",
            "- `research pool` excludes future-lag but is not deployable; crowded members still require orthogonalization/rejection.",
            "",
            "## Reproducibility",
            "",
            f"- python executable: `{summary['python_executable']}`",
            f"- package matrix: `{summary['package_versions']}`",
            f"- hot path scan: `{summary['hot_path_scan']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--memory-root", type=Path, default=DEFAULT_MEMORY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--seed-candidates", type=int, default=128)
    parser.add_argument("--adaptive-cem-candidates", type=int, default=128)
    parser.add_argument("--adaptive-hybrid-candidates", type=int, default=160)
    parser.add_argument("--cem-dominant-ucb-candidates", type=int, default=160)
    parser.add_argument("--cem-dominant-rx-candidates", type=int, default=160)
    parser.add_argument("--max-shards", type=int, default=4)
    parser.add_argument("--sample-trade-times-per-shard", type=int, default=40)
    parser.add_argument("--top-decisions", type=int, default=96)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs-per-time", type=int, default=20)
    parser.add_argument("--seed-exploration", type=float, default=0.85)
    parser.add_argument("--learning-rate", type=float, default=0.55)
    parser.add_argument("--entropy-floor", type=float, default=0.015)
    parser.add_argument("--min-feedback-eligible", type=int, default=32)
    parser.add_argument("--feedback-table", type=Path, default=None)
    parser.add_argument("--arm-score-table", type=Path, default=None)
    parser.add_argument("--family-memory", type=Path, default=None)
    parser.add_argument("--blocked-family-table", type=Path, default=None)
    parser.add_argument("--exploit-allowed-family-table", type=Path, default=None)
    parser.add_argument("--arm-id", default="cem_exploit")
    args = parser.parse_args(argv)

    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    panels = _discover_panels(_resolve(args.shard_root), args.max_shards)
    available_fields = _panel_schema_fields(panels)
    horizons = tuple(int(item.strip()) for item in str(args.horizons).split(",") if item.strip())
    blocked = _load_memory_hashes(args.memory_root) | _prior_hashes(PRIOR_HASH_FILES)

    seed_policy, _, _ = build_checked_seed_policy(exploration=args.seed_exploration)
    external_feedback = load_search_feedback_context(
        feedback_table=_resolve(args.feedback_table) if args.feedback_table else None,
        arm_score_table=_resolve(args.arm_score_table) if args.arm_score_table else None,
        family_memory=_resolve(args.family_memory) if args.family_memory else None,
        blocked_family_table=_resolve(args.blocked_family_table) if args.blocked_family_table else None,
        exploit_allowed_family_table=_resolve(args.exploit_allowed_family_table) if args.exploit_allowed_family_table else None,
        arm_id=args.arm_id,
        min_clean_feedback=args.min_feedback_eligible,
    )
    external_feedback_rows = load_search_feedback_rows(_resolve(args.feedback_table)) if args.feedback_table else []
    seed_candidates = _tag_candidates(
        _generate_rx_ucb_candidates(args.seed_candidates, blocked, seed_policy, include_residual=False, available_fields=available_fields),
        "phase3bs_seed_rx_ucb_fresh",
    )
    seed_decisions, seed_metrics, seed_meta = _evaluate_round(
        round_id="round1_seed_rx_ucb_fresh",
        candidates=seed_candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
        output_root=output_root,
        report_root=report_root,
        top_decisions=args.top_decisions,
    )

    if external_feedback.provided and not external_feedback.feedback_update_allowed:
        adaptive_policy = policy_blocked_by_external_feedback(seed_policy, external_feedback)
    elif external_feedback.provided:
        adaptive_policy = _policy_with_train_reward_feedback(
            seed_policy,
            external_feedback_rows,
            context=external_feedback,
            learning_rate=args.learning_rate,
            entropy_floor=args.entropy_floor,
            min_eligible=args.min_feedback_eligible,
        )
    else:
        adaptive_policy = _policy_with_feedback(
            seed_policy,
            seed_decisions,
            learning_rate=args.learning_rate,
            entropy_floor=args.entropy_floor,
            min_eligible=args.min_feedback_eligible,
        )
        adaptive_policy = annotate_policy_with_external_feedback(adaptive_policy, external_feedback)
    used_hashes = blocked | {str(row.get("expression_hash")) for row in seed_candidates}
    cem_candidates = _tag_candidates(
        _generate_cem_elite_candidates(
            args.adaptive_cem_candidates,
            used_hashes,
            adaptive_policy,
            include_residual=False,
            population_size=max(1024, args.adaptive_cem_candidates * 8),
            elite_frac=0.14,
            rounds=4,
            available_fields=available_fields,
        ),
        "phase3bs_adaptive_cem_feedback",
    )
    cem_decisions, cem_metrics, cem_meta = _evaluate_round(
        round_id="round2_adaptive_cem_feedback",
        candidates=cem_candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
        output_root=output_root,
        report_root=report_root,
        top_decisions=args.top_decisions,
    )

    used_hashes = used_hashes | {str(row.get("expression_hash")) for row in cem_candidates}
    hybrid_candidates = _tag_candidates(
        _generate_hybrid_candidates(
            args.adaptive_hybrid_candidates,
            used_hashes,
            adaptive_policy,
            include_residual=False,
            population_size=max(1280, args.adaptive_hybrid_candidates * 8),
            elite_frac=0.16,
            rounds=4,
            available_fields=available_fields,
        ),
        "phase3bs_adaptive_hybrid_rx_cem",
    )
    hybrid_decisions, hybrid_metrics, hybrid_meta = _evaluate_round(
        round_id="round3_adaptive_hybrid_rx_cem",
        candidates=hybrid_candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
        output_root=output_root,
        report_root=report_root,
        top_decisions=args.top_decisions,
    )

    used_hashes = used_hashes | {str(row.get("expression_hash")) for row in hybrid_candidates}
    cem_dominant_ucb_primary = _generate_cem_elite_candidates(
        max(1, int(args.cem_dominant_ucb_candidates * 0.85)),
        used_hashes,
        adaptive_policy,
        include_residual=False,
        population_size=max(2048, args.cem_dominant_ucb_candidates * 12),
        elite_frac=0.10,
        rounds=6,
        available_fields=available_fields,
    )
    cem_dominant_ucb_entropy = _generate_rx_ucb_candidates(
        max(8, args.cem_dominant_ucb_candidates - len(cem_dominant_ucb_primary)),
        used_hashes | {str(row.get("expression_hash")) for row in cem_dominant_ucb_primary},
        adaptive_policy,
        include_residual=False,
        available_fields=available_fields,
    )
    cem_dominant_ucb_candidates = _mix_unique(
        cem_dominant_ucb_primary,
        cem_dominant_ucb_entropy,
        max_candidates=args.cem_dominant_ucb_candidates,
        source="phase3bs_cem_dominant_ucb_feedback",
    )
    cem_dominant_ucb_decisions, cem_dominant_ucb_metrics, cem_dominant_ucb_meta = _evaluate_round(
        round_id="round4_cem_dominant_ucb",
        candidates=cem_dominant_ucb_candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
        output_root=output_root,
        report_root=report_root,
        top_decisions=args.top_decisions,
    )

    used_hashes = used_hashes | {str(row.get("expression_hash")) for row in cem_dominant_ucb_candidates}
    rx_entropy_policy = _policy_with_entropy_boost(seed_policy, boost=0.65, floor=max(args.entropy_floor, 0.035))
    cem_dominant_rx_primary = _generate_cem_elite_candidates(
        max(1, int(args.cem_dominant_rx_candidates * 0.80)),
        used_hashes,
        rx_entropy_policy,
        include_residual=False,
        population_size=max(2048, args.cem_dominant_rx_candidates * 12),
        elite_frac=0.12,
        rounds=5,
        available_fields=available_fields,
    )
    cem_dominant_rx_entropy = _generate_rx_ucb_candidates(
        max(12, args.cem_dominant_rx_candidates - len(cem_dominant_rx_primary)),
        used_hashes | {str(row.get("expression_hash")) for row in cem_dominant_rx_primary},
        rx_entropy_policy,
        include_residual=False,
        available_fields=available_fields,
    )
    cem_dominant_rx_candidates = _mix_unique(
        cem_dominant_rx_primary,
        cem_dominant_rx_entropy,
        max_candidates=args.cem_dominant_rx_candidates,
        source="phase3bs_cem_dominant_rx_entropy",
    )
    cem_dominant_rx_decisions, cem_dominant_rx_metrics, cem_dominant_rx_meta = _evaluate_round(
        round_id="round5_cem_dominant_rx",
        candidates=cem_dominant_rx_candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
        output_root=output_root,
        report_root=report_root,
        top_decisions=args.top_decisions,
    )

    rounds = [seed_metrics, cem_metrics, hybrid_metrics, cem_dominant_ucb_metrics, cem_dominant_rx_metrics]
    best = max(rounds, key=lambda row: (float(row["research_quality_score"]), int(row["research_pool_count"])))
    improvement = float(best["research_quality_score"]) - float(seed_metrics["research_quality_score"])
    if best["round_id"] != seed_metrics["round_id"] and improvement > 0.03:
        rec = {
            "decision": "ADAPTIVE_UCB_CEM_SHOWS_INCREMENTAL_SEARCH_VALUE_DIAGNOSTIC_ONLY",
            "best_round": best["round_id"],
            "improvement_vs_seed": round(improvement, 8),
            "interpretation": "feedback-updated CEM/hybrid improved research-pool score enough to justify a larger adaptive run",
        }
    else:
        rec = {
            "decision": "ADAPTIVE_UCB_CEM_HOLD_KEEP_RX_FRESH_PRIMARY",
            "best_round": best["round_id"],
            "improvement_vs_seed": round(improvement, 8),
            "interpretation": "adaptive feedback ran correctly, but did not beat RX/UCB seed by enough under this budget",
        }

    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260615_phase3bs_adaptive_ucb_cem_practice",
        "decision": "PHASE3BS_ADAPTIVE_UCB_CEM_PRACTICE_COMPLETE_DIAGNOSTIC_ONLY",
        "objective": "test multi-round feedback-updated UCB-CEM on true1min panels",
        "parameters": {
            "seed_candidates": args.seed_candidates,
            "adaptive_cem_candidates": args.adaptive_cem_candidates,
            "adaptive_hybrid_candidates": args.adaptive_hybrid_candidates,
            "cem_dominant_ucb_candidates": args.cem_dominant_ucb_candidates,
            "cem_dominant_rx_candidates": args.cem_dominant_rx_candidates,
            "max_shards": args.max_shards,
            "sample_trade_times_per_shard": args.sample_trade_times_per_shard,
            "horizons": list(horizons),
            "learning_rate": args.learning_rate,
            "entropy_floor": args.entropy_floor,
            "min_feedback_eligible": args.min_feedback_eligible,
            "seed_exploration": args.seed_exploration,
            "phase3cn_feedback": external_feedback.to_dict(),
            "schema_bound_generation": True,
            "available_field_count": len(available_fields),
            "available_fields": available_fields,
        },
        "python_executable": __import__("sys").executable,
        "package_versions": _package_versions(),
        "hot_path_scan": _hot_path_scan(),
        "rounds": rounds,
        "round_meta": {
            "seed": seed_meta,
            "adaptive_cem": cem_meta,
            "adaptive_hybrid": hybrid_meta,
            "cem_dominant_ucb": cem_dominant_ucb_meta,
            "cem_dominant_rx": cem_dominant_rx_meta,
        },
        "adaptive_policy": adaptive_policy,
        "recommendation": rec,
        "hard_boundary": [
            "true trade_time minute panels only",
            "no old daily stock-PIT default panel",
            "no X0/R3 modification",
            "algorithm practice only, not alpha promotion",
        ],
    }
    _write_json(output_root / "phase3bs_adaptive_ucb_cem_summary.json", summary)
    _write_json(report_root / "phase3bs_adaptive_ucb_cem_summary.json", summary)
    _write_csv(report_root / "phase3bs_round_results.csv", rounds)
    (report_root / "PHASE3BS_ADAPTIVE_UCB_CEM_PRACTICE_20260615.md").write_text(_render_md(summary), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
