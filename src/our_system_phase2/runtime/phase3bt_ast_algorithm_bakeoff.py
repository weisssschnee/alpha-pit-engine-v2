"""AST-aware true-1min algorithm bakeoff.

Phase3BT is the pre-large-search algorithm lock trial. It reuses the true-1min
Phase3BP/BS evaluator and compares AST-aware RX/UCB, feedback CEM, hybrids, and
fresh-preserving variants under one budget.

This is diagnostic only. It does not promote alpha and does not modify X0/R3.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _discover_panels,
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
    _generate_cem_elite_candidates,
    _generate_hybrid_candidates,
    _panel_schema_fields,
    _generate_rx_ucb_candidates,
    _summarize_by,
    build_checked_seed_policy,
)
from our_system_phase2.runtime.phase3bq_compute_allocation_benchmark import (
    _fmt,
    _hot_path_scan,
    _package_versions,
)
from our_system_phase2.runtime.phase3bs_adaptive_ucb_cem_practice import (
    _external_feedback_seed_candidates,
    _policy_with_entropy_boost,
    _policy_with_feedback,
    _policy_with_train_reward_feedback,
    _round_metrics,
)
from our_system_phase2.services.search_feedback import (
    annotate_policy_with_external_feedback,
    load_search_feedback_context,
    load_search_feedback_rows,
    policy_blocked_by_external_feedback,
)


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3bt_ast_algorithm_bakeoff_20260615")
DEFAULT_REPORT_ROOT = Path("reports/phase3bt_ast_algorithm_bakeoff_20260615")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _average_policies(primary: dict[str, Any], secondary: dict[str, Any], *, primary_weight: float) -> dict[str, Any]:
    out = copy.deepcopy(primary)
    out["policy_version"] = "phase3bt_fresh_preserving_ast_policy_v1"
    out["scope"] = "blend_feedback_with_high_entropy_seed_for_freshness"
    scores = out.setdefault("scores", {})
    other_scores = secondary.get("scores") or {}
    for kind, table in list(scores.items()):
        if not isinstance(table, dict):
            continue
        other = other_scores.get(kind) or {}
        keys = set(table) | set(other)
        blended: dict[str, float] = {}
        for key in keys:
            left = float(table.get(key, 0.0))
            right = float(other.get(key, 0.0))
            blended[key] = round((primary_weight * left) + ((1.0 - primary_weight) * right), 6)
        scores[kind] = blended
    out["feedback"] = {
        "blend": {
            "primary_policy": primary.get("policy_version"),
            "secondary_policy": secondary.get("policy_version"),
            "primary_weight": primary_weight,
        }
    }
    return out


def _tag_bt_candidates(rows: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    tagged: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, 1):
        item = dict(row)
        item["candidate_id"] = f"phase3bt_{idx:05d}"
        item["source_generator"] = source
        item["source_lane"] = source
        item["note"] = f"{source}: {row.get('note')}"
        tagged.append(item)
    return tagged


def _mix_bt_unique(primary: list[dict[str, Any]], secondary: list[dict[str, Any]], *, max_candidates: int, source: str) -> list[dict[str, Any]]:
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
    return _tag_bt_candidates(rows, source)


def _write_round_outputs(
    *,
    round_id: str,
    candidates: list[dict[str, Any]],
    metric_rows: list[dict[str, Any]],
    aggregate_rows: list[dict[str, Any]],
    pairwise_rows: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    meta: dict[str, Any],
    metrics: dict[str, Any],
    output_root: Path,
    report_root: Path,
) -> None:
    round_output = output_root / round_id
    round_report = report_root / round_id
    round_output.mkdir(parents=True, exist_ok=True)
    round_report.mkdir(parents=True, exist_ok=True)
    _write_csv(round_output / "phase3bt_candidate_pack.csv", candidates)
    _write_csv(round_output / "phase3bt_candidate_horizon_shard_metrics.csv", metric_rows)
    _write_csv(round_output / "phase3bt_candidate_horizon_aggregate.csv", aggregate_rows)
    _write_csv(round_output / "phase3bt_pairwise_signal_rank_corr.csv", pairwise_rows)
    _write_csv(round_output / "phase3bt_top_decisions.csv", decisions)
    _write_json(round_output / "phase3bt_round_summary.json", {**metrics, **meta})
    _write_csv(round_report / "phase3bt_candidate_pack.csv", candidates)
    _write_csv(round_report / "phase3bt_top_decisions.csv", decisions)
    _write_csv(round_report / "phase3bt_generator_summary.csv", _summarize_by(decisions, "source_generator"))
    _write_csv(round_report / "phase3bt_lane_summary.csv", _summarize_by(decisions, "factor_lane"))
    _write_json(round_report / "phase3bt_round_summary.json", {**metrics, **meta, "top_decisions": decisions[:12]})


def _evaluate_bt_round(
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

    round_output = output_root / round_id
    round_report = report_root / round_id
    round_output.mkdir(parents=True, exist_ok=True)
    round_report.mkdir(parents=True, exist_ok=True)
    _write_csv(round_output / "phase3bt_candidate_pack.pre_eval.csv", candidates)
    _write_json(
        round_output / "phase3bt_round_progress.json",
        {
            "round_id": round_id,
            "status": "materialization_started",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "candidate_count": len(candidates),
            "panel_count": len(panels),
            "sample_trade_times_per_shard": sample_trade_times_per_shard,
            "horizons": list(horizons),
        },
    )
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
    metrics = _round_metrics(round_id, candidates, decisions, meta, elapsed)
    _write_round_outputs(
        round_id=round_id,
        candidates=candidates,
        metric_rows=metric_rows,
        aggregate_rows=aggregate_rows,
        pairwise_rows=pairwise_rows,
        decisions=decisions,
        meta=meta,
        metrics=metrics,
        output_root=output_root,
        report_root=report_root,
    )
    return decisions, metrics, meta


def _render_md(summary: dict[str, Any]) -> str:
    lines = [
        "# Phase3BT AST Algorithm Bakeoff 2026-06-15",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Purpose",
        "",
        "Lock the next true-1min search-core allocation before any broad large search.",
        "All arms use AST-aware generator variables and the same true `trade_time` minute panels.",
        "",
        "## Arm Results",
        "",
        "| arm | candidates | sec | rows | rows/sec | hard-blocked | research pool | lanes | fieldsets | ast shapes | top10 abs IC | score |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary["rounds"]:
        lines.append(
            f"| `{row['round_id']}` | {row['candidate_count']} | {_fmt(row['elapsed_seconds'])} | {row['total_eval_rows']} | "
            f"{_fmt(row['rows_per_second'])} | {_fmt(row['hard_blocked_ratio'])} | {row['research_pool_count']} | "
            f"{row['unique_lane_count']} | {row['unique_fieldset_count']} | {row['unique_ast_shape_count']} | "
            f"{_fmt(row['top10_abs_ic_mean'])} | {_fmt(row['research_quality_score'])} |"
        )
    rec = summary["recommendation"]
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            f"- primary arm: `{rec['primary_arm']}`",
            f"- secondary arm: `{rec['secondary_arm']}`",
            f"- exploration arm: `{rec['exploration_arm']}`",
            f"- decision: `{rec['decision']}`",
            f"- reason: {rec['reason']}",
            "",
            "## Next Large-search Allocation",
            "",
        ]
    )
    for key, value in rec["next_allocation"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Experiment Record",
            "",
            f"- experiment_id: `{summary['experiment_id']}`",
            f"- python executable: `{summary['python_executable']}`",
            f"- package matrix: `{summary['package_versions']}`",
            f"- hot path scan: `{summary['hot_path_scan']}`",
            "- status: completed",
            "- decision discipline: diagnostic only; no X0/R3 modification; no alpha promotion.",
            "",
            "## Boundary",
            "",
            "- True `trade_time` 1min panels only.",
            "- Search memory and prior hashes are blocked.",
            "- `research pool` is a deeper-review queue, not deployable proof.",
            "- Freshness is kept as a first-class allocation arm even when feedback CEM wins.",
        ]
    )
    return "\n".join(lines) + "\n"


def _recommend(rounds: list[dict[str, Any]]) -> dict[str, Any]:
    ranked = sorted(rounds, key=lambda row: (float(row["research_quality_score"]), int(row["research_pool_count"])), reverse=True)
    primary = ranked[0]
    secondary = next((row for row in ranked if "fresh" not in row["round_id"] and row["round_id"] != primary["round_id"]), ranked[min(1, len(ranked) - 1)])
    exploration = next(
        (row for row in ranked if "fresh" in row["round_id"] and row["round_id"] != primary["round_id"]),
        next((row for row in ranked if row["round_id"] != primary["round_id"]), ranked[min(1, len(ranked) - 1)]),
    )
    return {
        "decision": "LOCK_AST_AWARE_ADAPTIVE_SEARCH_CORE_FOR_NEXT_LARGE_SEARCH_DIAGNOSTIC_ONLY",
        "primary_arm": primary["round_id"],
        "secondary_arm": secondary["round_id"],
        "exploration_arm": exploration["round_id"],
        "next_allocation": {
            "primary_best_arm": "45%",
            "secondary_adaptive_or_hybrid": "25%",
            "fresh_high_exploration": "20%",
            "control_or_residual_probe": "10%",
        },
        "reason": "ranking uses research-pool quality, AST diversity, fieldset/lane coverage, hard-block ratio, and throughput; not legacy first-up.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--memory-root", type=Path, default=DEFAULT_MEMORY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-shards", type=int, default=4)
    parser.add_argument("--sample-trade-times-per-shard", type=int, default=48)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs-per-time", type=int, default=20)
    parser.add_argument("--top-decisions", type=int, default=112)
    parser.add_argument("--seed-candidates", type=int, default=160)
    parser.add_argument("--cem-candidates", type=int, default=160)
    parser.add_argument("--hybrid-candidates", type=int, default=192)
    parser.add_argument("--dominant-candidates", type=int, default=192)
    parser.add_argument("--fresh-hybrid-candidates", type=int, default=192)
    parser.add_argument("--seed-exploration", type=float, default=0.9)
    parser.add_argument("--learning-rate", type=float, default=0.55)
    parser.add_argument("--entropy-floor", type=float, default=0.02)
    parser.add_argument("--min-feedback-eligible", type=int, default=32)
    parser.add_argument("--feedback-table", type=Path, default=None)
    parser.add_argument("--arm-score-table", type=Path, default=None)
    parser.add_argument("--family-memory", type=Path, default=None)
    parser.add_argument("--blocked-family-table", type=Path, default=None)
    parser.add_argument("--exploit-allowed-family-table", type=Path, default=None)
    parser.add_argument("--arm-id", default="typed_ast_fresh")
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
    seed_candidates = _tag_bt_candidates(
        _generate_rx_ucb_candidates(args.seed_candidates, blocked, seed_policy, include_residual=False, available_fields=available_fields),
        "phase3bt_seed_ast_rx_ucb_fresh",
    )
    seed_decisions, seed_metrics, seed_meta = _evaluate_bt_round(
        round_id="round1_seed_ast_rx_ucb_fresh",
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
    used = blocked | {str(row.get("expression_hash")) for row in seed_candidates}
    feedback_seed_budget = max(0, min(16, int(math.ceil(args.cem_candidates * 0.25))))
    feedback_seeds = _external_feedback_seed_candidates(
        external_feedback_rows,
        context=external_feedback,
        policy=adaptive_policy,
        available_fields=available_fields,
        blocked=used,
        max_count=feedback_seed_budget,
        source="phase3bt_external_train_feedback_seed",
    )
    cem_generated = _generate_cem_elite_candidates(
        args.cem_candidates,
        used | {str(row.get("expression_hash")) for row in feedback_seeds},
        adaptive_policy,
        include_residual=False,
        population_size=max(1600, args.cem_candidates * 10),
        elite_frac=0.12,
        rounds=5,
        available_fields=available_fields,
    )
    cem_candidates = _mix_bt_unique(
        feedback_seeds,
        cem_generated,
        max_candidates=args.cem_candidates,
        source="phase3bt_ast_feedback_cem",
    )
    cem_decisions, cem_metrics, cem_meta = _evaluate_bt_round(
        round_id="round2_ast_feedback_cem",
        candidates=cem_candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
        output_root=output_root,
        report_root=report_root,
        top_decisions=args.top_decisions,
    )

    used |= {str(row.get("expression_hash")) for row in cem_candidates}
    hybrid_feedback_seeds = _external_feedback_seed_candidates(
        external_feedback_rows,
        context=external_feedback,
        policy=adaptive_policy,
        available_fields=available_fields,
        blocked=used,
        max_count=max(0, min(16, int(math.ceil(args.hybrid_candidates * 0.20)))),
        source="phase3bt_external_train_feedback_seed",
    )
    hybrid_generated = _generate_hybrid_candidates(
        args.hybrid_candidates,
        used | {str(row.get("expression_hash")) for row in hybrid_feedback_seeds},
        adaptive_policy,
        include_residual=False,
        population_size=max(1920, args.hybrid_candidates * 10),
        elite_frac=0.14,
        rounds=5,
        available_fields=available_fields,
    )
    hybrid_candidates = _mix_bt_unique(
        hybrid_feedback_seeds,
        hybrid_generated,
        max_candidates=args.hybrid_candidates,
        source="phase3bt_ast_feedback_hybrid",
    )
    hybrid_decisions, hybrid_metrics, hybrid_meta = _evaluate_bt_round(
        round_id="round3_ast_feedback_hybrid",
        candidates=hybrid_candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
        output_root=output_root,
        report_root=report_root,
        top_decisions=args.top_decisions,
    )

    used |= {str(row.get("expression_hash")) for row in hybrid_candidates}
    dominant_primary = _generate_cem_elite_candidates(
        int(args.dominant_candidates * 0.85),
        used,
        adaptive_policy,
        include_residual=False,
        population_size=max(2304, args.dominant_candidates * 12),
        elite_frac=0.10,
        rounds=6,
        available_fields=available_fields,
    )
    dominant_secondary = _generate_rx_ucb_candidates(
        max(12, args.dominant_candidates - len(dominant_primary)),
        used | {str(row.get("expression_hash")) for row in dominant_primary},
        adaptive_policy,
        include_residual=False,
        available_fields=available_fields,
    )
    dominant_candidates = _mix_bt_unique(dominant_primary, dominant_secondary, max_candidates=args.dominant_candidates, source="phase3bt_ast_cem_dominant_ucb")
    dominant_decisions, dominant_metrics, dominant_meta = _evaluate_bt_round(
        round_id="round4_ast_cem_dominant_ucb",
        candidates=dominant_candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
        output_root=output_root,
        report_root=report_root,
        top_decisions=args.top_decisions,
    )

    used |= {str(row.get("expression_hash")) for row in dominant_candidates}
    entropy_policy = _policy_with_entropy_boost(seed_policy, boost=0.75, floor=0.04)
    fresh_policy = _average_policies(adaptive_policy, entropy_policy, primary_weight=0.55)
    fresh_primary = _generate_hybrid_candidates(
        int(args.fresh_hybrid_candidates * 0.65),
        used,
        fresh_policy,
        include_residual=False,
        population_size=max(2048, args.fresh_hybrid_candidates * 10),
        elite_frac=0.18,
        rounds=4,
        available_fields=available_fields,
    )
    fresh_secondary = _generate_rx_ucb_candidates(
        max(24, args.fresh_hybrid_candidates - len(fresh_primary)),
        used | {str(row.get("expression_hash")) for row in fresh_primary},
        entropy_policy,
        include_residual=False,
        available_fields=available_fields,
    )
    fresh_candidates = _mix_bt_unique(fresh_primary, fresh_secondary, max_candidates=args.fresh_hybrid_candidates, source="phase3bt_ast_fresh_preserving_hybrid")
    fresh_decisions, fresh_metrics, fresh_meta = _evaluate_bt_round(
        round_id="round5_ast_fresh_preserving_hybrid",
        candidates=fresh_candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
        output_root=output_root,
        report_root=report_root,
        top_decisions=args.top_decisions,
    )

    rounds = [seed_metrics, cem_metrics, hybrid_metrics, dominant_metrics, fresh_metrics]
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260615_phase3bt_ast_algorithm_bakeoff",
        "decision": "PHASE3BT_AST_ALGORITHM_BAKEOFF_COMPLETE_DIAGNOSTIC_ONLY",
        "objective": "choose the next true1min large-search core among AST-aware RX/UCB, CEM, hybrid, and fresh-preserving arms",
        "parameters": {
            "max_shards": args.max_shards,
            "sample_trade_times_per_shard": args.sample_trade_times_per_shard,
            "horizons": list(horizons),
            "seed_candidates": args.seed_candidates,
            "cem_candidates": args.cem_candidates,
            "hybrid_candidates": args.hybrid_candidates,
            "dominant_candidates": args.dominant_candidates,
            "fresh_hybrid_candidates": args.fresh_hybrid_candidates,
            "seed_exploration": args.seed_exploration,
            "learning_rate": args.learning_rate,
            "entropy_floor": args.entropy_floor,
            "min_feedback_eligible": args.min_feedback_eligible,
            "phase3cn_feedback": external_feedback.to_dict(),
            "schema_bound_generation": True,
            "available_field_count": len(available_fields),
            "available_fields": available_fields,
        },
        "python_executable": sys.executable,
        "package_versions": _package_versions(),
        "hot_path_scan": _hot_path_scan(),
        "rounds": rounds,
        "round_meta": {
            "seed": seed_meta,
            "cem": cem_meta,
            "hybrid": hybrid_meta,
            "dominant": dominant_meta,
            "fresh_preserving": fresh_meta,
        },
        "recommendation": _recommend(rounds),
        "adaptive_policy": adaptive_policy,
        "hard_boundary": [
            "true trade_time minute panels only",
            "no old daily stock-PIT default panel",
            "no X0/R3 modification",
            "algorithm bakeoff only, not alpha promotion",
        ],
    }
    _write_json(output_root / "phase3bt_ast_algorithm_bakeoff_summary.json", summary)
    _write_json(report_root / "phase3bt_ast_algorithm_bakeoff_summary.json", summary)
    _write_csv(report_root / "phase3bt_round_results.csv", rounds)
    (report_root / "PHASE3BT_AST_ALGORITHM_BAKEOFF_20260615.md").write_text(_render_md(summary), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
