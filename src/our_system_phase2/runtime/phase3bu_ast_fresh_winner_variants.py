"""Fresh-preserving AST winner variants on true-1min panels.

Phase3BU takes the Phase3BT winning idea, feedback CEM plus fresh exploration
entropy, and tests nearby variants before the next broad search.

This is diagnostic only. It does not promote alpha and does not modify X0/R3.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import DEFAULT_SHARD_ROOT, _discover_panels, _write_csv, _write_json
from our_system_phase2.runtime.phase3bn_open_diversified_true1min_canary import DEFAULT_MEMORY_ROOT, _load_memory_hashes, _prior_hashes
from our_system_phase2.runtime.phase3bp_true1min_search_algorithm_smoke import (
    PRIOR_DECISION_FILES,
    PRIOR_HASH_FILES,
    _build_policy,
    _generate_cem_elite_candidates,
    _generate_hybrid_candidates,
    _generate_rx_ucb_candidates,
)
from our_system_phase2.runtime.phase3bq_compute_allocation_benchmark import _fmt, _hot_path_scan, _package_versions
from our_system_phase2.runtime.phase3bs_adaptive_ucb_cem_practice import _policy_with_entropy_boost, _policy_with_feedback
from our_system_phase2.runtime.phase3bt_ast_algorithm_bakeoff import _average_policies, _evaluate_bt_round, _mix_bt_unique, _tag_bt_candidates
from our_system_phase2.services.search_feedback import (
    annotate_policy_with_external_feedback,
    load_search_feedback_context,
    policy_blocked_by_external_feedback,
)


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3bu_ast_fresh_winner_variants_20260615")
DEFAULT_REPORT_ROOT = Path("reports/phase3bu_ast_fresh_winner_variants_20260615")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _variant_specs(args: argparse.Namespace) -> list[dict[str, Any]]:
    return [
        {
            "round_id": "round1_bt_winner_replay",
            "kind": "fresh_hybrid",
            "count": args.winner_replay_candidates,
            "feedback_weight": 0.55,
            "entropy_boost": 0.75,
            "entropy_floor": 0.04,
            "primary_frac": 0.65,
            "population_mult": 10,
            "elite_frac": 0.18,
            "cem_rounds": 4,
            "description": "BT winner replay: feedback policy plus entropy-preserving fresh RX tail.",
        },
        {
            "round_id": "round2_more_fresh_entropy",
            "kind": "fresh_hybrid",
            "count": args.variant_candidates,
            "feedback_weight": 0.35,
            "entropy_boost": 0.90,
            "entropy_floor": 0.06,
            "primary_frac": 0.58,
            "population_mult": 12,
            "elite_frac": 0.22,
            "cem_rounds": 4,
            "description": "More fresh: deliberately weaken feedback lock-in and widen entropy floor.",
        },
        {
            "round_id": "round3_feedback_heavier_fresh_tail",
            "kind": "fresh_hybrid",
            "count": args.variant_candidates,
            "feedback_weight": 0.72,
            "entropy_boost": 0.65,
            "entropy_floor": 0.035,
            "primary_frac": 0.70,
            "population_mult": 12,
            "elite_frac": 0.16,
            "cem_rounds": 5,
            "description": "Feedback-heavy winner variant with a smaller but explicit fresh tail.",
        },
        {
            "round_id": "round4_high_elite_fresh_mixer",
            "kind": "fresh_hybrid",
            "count": args.variant_candidates,
            "feedback_weight": 0.50,
            "entropy_boost": 1.05,
            "entropy_floor": 0.05,
            "primary_frac": 0.62,
            "population_mult": 14,
            "elite_frac": 0.24,
            "cem_rounds": 3,
            "description": "High-elite broad mixer: less iterative pressure, more elite breadth.",
        },
        {
            "round_id": "round5_rx_fresh_control_wide",
            "kind": "rx_control",
            "count": args.variant_candidates,
            "entropy_boost": 1.10,
            "entropy_floor": 0.06,
            "description": "Pure fresh RX/UCB control with AST policy and very high entropy.",
        },
        {
            "round_id": "round6_cem_feedback_control_wide",
            "kind": "cem_control",
            "count": args.variant_candidates,
            "population_mult": 14,
            "elite_frac": 0.12,
            "cem_rounds": 6,
            "description": "Feedback CEM control: tests whether fresh tail is still required.",
        },
    ]


def _build_variant_candidates(
    spec: dict[str, Any],
    *,
    seed_policy: dict[str, Any],
    adaptive_policy: dict[str, Any],
    blocked: set[str],
    used: set[str],
) -> list[dict[str, Any]]:
    count = int(spec["count"])
    if spec["kind"] == "rx_control":
        entropy_policy = _policy_with_entropy_boost(seed_policy, boost=float(spec["entropy_boost"]), floor=float(spec["entropy_floor"]))
        rows = _generate_rx_ucb_candidates(count, blocked | used, entropy_policy, include_residual=False)
        return _tag_bt_candidates(rows, f"phase3bu_{spec['round_id']}")
    if spec["kind"] == "cem_control":
        rows = _generate_cem_elite_candidates(
            count,
            blocked | used,
            adaptive_policy,
            include_residual=False,
            population_size=max(2048, count * int(spec["population_mult"])),
            elite_frac=float(spec["elite_frac"]),
            rounds=int(spec["cem_rounds"]),
        )
        return _tag_bt_candidates(rows, f"phase3bu_{spec['round_id']}")

    entropy_policy = _policy_with_entropy_boost(seed_policy, boost=float(spec["entropy_boost"]), floor=float(spec["entropy_floor"]))
    fresh_policy = _average_policies(adaptive_policy, entropy_policy, primary_weight=float(spec["feedback_weight"]))
    primary_count = int(count * float(spec["primary_frac"]))
    primary = _generate_hybrid_candidates(
        primary_count,
        blocked | used,
        fresh_policy,
        include_residual=False,
        population_size=max(2048, count * int(spec["population_mult"])),
        elite_frac=float(spec["elite_frac"]),
        rounds=int(spec["cem_rounds"]),
    )
    secondary = _generate_rx_ucb_candidates(
        max(24, count - len(primary)),
        blocked | used | {str(row.get("expression_hash")) for row in primary},
        entropy_policy,
        include_residual=False,
    )
    return _mix_bt_unique(primary, secondary, max_candidates=count, source=f"phase3bu_{spec['round_id']}")


def _render_md(summary: dict[str, Any]) -> str:
    lines = [
        "# Phase3BU AST Fresh Winner Variants 2026-06-15",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Purpose",
        "",
        "Stress-test the Phase3BT winner by making it fresher, feedback-heavier, elite-broader, and comparing against pure RX/CEM controls.",
        "All variants use true `trade_time` 1min panels and search memory blocking.",
        "",
        "## Variant Results",
        "",
        "| variant | candidates | sec | rows/sec | hard-blocked | research pool | lanes | fieldsets | ast shapes | top10 abs IC | score |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary["rounds"]:
        lines.append(
            f"| `{row['round_id']}` | {row['candidate_count']} | {_fmt(row['elapsed_seconds'])} | {_fmt(row['rows_per_second'])} | "
            f"{_fmt(row['hard_blocked_ratio'])} | {row['research_pool_count']} | {row['unique_lane_count']} | "
            f"{row['unique_fieldset_count']} | {row['unique_ast_shape_count']} | {_fmt(row['top10_abs_ic_mean'])} | "
            f"{_fmt(row['research_quality_score'])} |"
        )
    rec = summary["recommendation"]
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            f"- best variant: `{rec['best_variant']}`",
            f"- second variant: `{rec['second_variant']}`",
            f"- decision: `{rec['decision']}`",
            f"- interpretation: {rec['interpretation']}",
            "",
            "## Boundary",
            "",
            "- True `trade_time` 1min panels only.",
            "- No old 1D stock-PIT panel.",
            "- No X0/R3 modification.",
            "- Diagnostic search-core experiment only; no candidate promotion.",
            "",
            "## Reproducibility",
            "",
            f"- python executable: `{summary['python_executable']}`",
            f"- package matrix: `{summary['package_versions']}`",
            f"- hot path scan: `{summary['hot_path_scan']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def _recommend(rounds: list[dict[str, Any]]) -> dict[str, Any]:
    ranked = sorted(rounds, key=lambda row: (float(row["research_quality_score"]), int(row["research_pool_count"])), reverse=True)
    best = ranked[0]
    second = ranked[1] if len(ranked) > 1 else ranked[0]
    if "rx_fresh_control" in best["round_id"]:
        decision = "FRESH_EXPLORATION_DOMINATES_KEEP_FEEDBACK_AS_SECONDARY_DIAGNOSTIC_ONLY"
    elif "cem_feedback_control" in best["round_id"]:
        decision = "PURE_CEM_CONTROL_WINS_RECHECK_FRESH_TAIL_BEFORE_SCALE_DIAGNOSTIC_ONLY"
    else:
        decision = "FRESH_PRESERVING_WINNER_VARIANT_CONFIRMED_DIAGNOSTIC_ONLY"
    return {
        "decision": decision,
        "best_variant": best["round_id"],
        "second_variant": second["round_id"],
        "ranked_variants": [row["round_id"] for row in ranked],
        "interpretation": "Use the best fresh-preserving or fresh-control variant as the primary arm for the next larger company-machine search; keep pure CEM capped unless it clearly wins.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--memory-root", type=Path, default=DEFAULT_MEMORY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-shards", type=int, default=4)
    parser.add_argument("--sample-trade-times-per-shard", type=int, default=56)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs-per-time", type=int, default=20)
    parser.add_argument("--top-decisions", type=int, default=128)
    parser.add_argument("--seed-candidates", type=int, default=160)
    parser.add_argument("--winner-replay-candidates", type=int, default=192)
    parser.add_argument("--variant-candidates", type=int, default=224)
    parser.add_argument("--seed-exploration", type=float, default=0.92)
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
    horizons = tuple(int(item.strip()) for item in str(args.horizons).split(",") if item.strip())
    blocked = _load_memory_hashes(args.memory_root) | _prior_hashes(PRIOR_HASH_FILES)

    seed_policy = _build_policy(PRIOR_DECISION_FILES, exploration=args.seed_exploration)
    external_feedback = load_search_feedback_context(
        feedback_table=_resolve(args.feedback_table) if args.feedback_table else None,
        arm_score_table=_resolve(args.arm_score_table) if args.arm_score_table else None,
        family_memory=_resolve(args.family_memory) if args.family_memory else None,
        blocked_family_table=_resolve(args.blocked_family_table) if args.blocked_family_table else None,
        exploit_allowed_family_table=_resolve(args.exploit_allowed_family_table) if args.exploit_allowed_family_table else None,
        arm_id=args.arm_id,
        min_clean_feedback=args.min_feedback_eligible,
    )
    seed_candidates = _tag_bt_candidates(
        _generate_rx_ucb_candidates(args.seed_candidates, blocked, seed_policy, include_residual=False),
        "phase3bu_seed_for_feedback",
    )
    seed_decisions, seed_metrics, seed_meta = _evaluate_bt_round(
        round_id="round0_seed_for_feedback",
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
    else:
        adaptive_policy = _policy_with_feedback(
            seed_policy,
            seed_decisions,
            learning_rate=args.learning_rate,
            entropy_floor=args.entropy_floor,
            min_eligible=args.min_feedback_eligible,
        )
        adaptive_policy = annotate_policy_with_external_feedback(adaptive_policy, external_feedback)

    used: set[str] = {str(row.get("expression_hash")) for row in seed_candidates}
    round_metrics = []
    round_meta: dict[str, Any] = {"seed": seed_meta}
    specs = _variant_specs(args)
    for spec in specs:
        candidates = _build_variant_candidates(spec, seed_policy=seed_policy, adaptive_policy=adaptive_policy, blocked=blocked, used=used)
        used |= {str(row.get("expression_hash")) for row in candidates}
        _, metrics, meta = _evaluate_bt_round(
            round_id=str(spec["round_id"]),
            candidates=candidates,
            panels=panels,
            horizons=horizons,
            sample_trade_times_per_shard=args.sample_trade_times_per_shard,
            min_obs_per_time=args.min_obs_per_time,
            output_root=output_root,
            report_root=report_root,
            top_decisions=args.top_decisions,
        )
        metrics["variant_description"] = spec["description"]
        round_metrics.append(metrics)
        round_meta[str(spec["round_id"])] = meta

    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260615_phase3bu_ast_fresh_winner_variants",
        "decision": "PHASE3BU_AST_FRESH_WINNER_VARIANTS_COMPLETE_DIAGNOSTIC_ONLY",
        "objective": "try fresher and more varied variants around the Phase3BT winning AST fresh-preserving hybrid",
        "parameters": {
            "max_shards": args.max_shards,
            "sample_trade_times_per_shard": args.sample_trade_times_per_shard,
            "horizons": list(horizons),
            "seed_candidates": args.seed_candidates,
            "winner_replay_candidates": args.winner_replay_candidates,
            "variant_candidates": args.variant_candidates,
            "seed_exploration": args.seed_exploration,
            "learning_rate": args.learning_rate,
            "entropy_floor": args.entropy_floor,
            "min_feedback_eligible": args.min_feedback_eligible,
            "phase3cn_feedback": external_feedback.to_dict(),
        },
        "python_executable": sys.executable,
        "package_versions": _package_versions(),
        "hot_path_scan": _hot_path_scan(),
        "seed_metrics": seed_metrics,
        "rounds": round_metrics,
        "round_meta": round_meta,
        "recommendation": _recommend(round_metrics),
        "hard_boundary": [
            "true trade_time minute panels only",
            "no old daily stock-PIT default panel",
            "no X0/R3 modification",
            "algorithm variant test only, not alpha promotion",
        ],
    }
    _write_json(output_root / "phase3bu_ast_fresh_winner_variants_summary.json", summary)
    _write_json(report_root / "phase3bu_ast_fresh_winner_variants_summary.json", summary)
    _write_csv(report_root / "phase3bu_round_results.csv", round_metrics)
    (report_root / "PHASE3BU_AST_FRESH_WINNER_VARIANTS_20260615.md").write_text(_render_md(summary), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
