"""Phase3CP low-turnover event/opening-state targeted probe.

The Phase3CP arm-balanced real-CM run found one near-pass event_state row:
positive train/validation/holdout Sortino, but rejected by the turnover gate.
This route generates bounded low-turnover variants around that motif and sends
them through CA -> true1min field gate -> real Phase3CM -> CN feedback.

Diagnostic-only. It does not launch Phase3CQ and does not touch X0/R3.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _write_csv,
    _write_json,
)
from our_system_phase2.runtime.phase3ca_build_bz_candidate_audit import build_candidate_table
from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import main as phase3cm_main
from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import build_feedback_memory
from our_system_phase2.runtime.phase3cp_real_cm_small_loop import (
    _audit_cm_lineage_consistency,
    _filter_cm_feasible_candidates,
)
from our_system_phase2.runtime.phase3cp_reward_gated_medium_search_smoke import _copy_report_files
from our_system_phase2.services.candidate_schema import normalize_candidate_schema


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cp_low_turnover_event_state_probe_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3cp_low_turnover_event_state_probe_20260623")
EPS = "0.000001"


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def _round(value: Any, ndigits: int = 8) -> float | None:
    try:
        out = float(value)
        return round(out, ndigits) if math.isfinite(out) else None
    except Exception:
        return None


def _safe_div(left: str, right: str) -> str:
    return f"Div({left},Add(Abs({right}),{EPS}))"


def _range_location() -> str:
    return _safe_div("Sub($close,$low)", "Sub($high,$low)")


def _range_width() -> str:
    return _safe_div("Sub($high,$low)", "$open")


def _flow_ratio(field: str) -> str:
    denominator = "$amount" if "amount" in field else "$volume"
    return _safe_div(f"${field}", denominator)


def _smooth(expr: str, mode: str, window: int) -> str:
    if mode == "identity":
        return expr
    if mode == "mean":
        return f"Mean({expr},{window})"
    if mode == "wma":
        return f"Wma({expr},{window})"
    if mode == "delay":
        return f"Delay({expr},{window})"
    if mode == "mean_rank":
        return f"Mean(CSRank({expr}),{window})"
    return expr


def _candidate_rows(limit: int) -> list[dict[str, Any]]:
    range_loc = _range_location()
    range_width = _range_width()
    flow_fields = ["m1_first30_vol", "m1_first30_amount", "m1_first15_vol", "m1_first15_amount"]
    trend_windows = [15, 30, 45, 60]
    smooth_specs = [
        ("identity", 1),
        ("mean", 3),
        ("mean", 5),
        ("mean", 8),
        ("wma", 5),
        ("wma", 8),
        ("delay", 1),
        ("mean_rank", 3),
        ("mean_rank", 5),
    ]
    expressions: list[tuple[str, str]] = []
    for flow in flow_fields:
        flow_expr = _flow_ratio(flow)
        for trend_window in trend_windows:
            range_mom = f"Sub({range_loc},Mean({range_loc},{trend_window}))"
            product = f"Mul(ZScore({range_mom}),ZScore({flow_expr}))"
            for smooth_mode, smooth_window in smooth_specs:
                smoothed = _smooth(product, smooth_mode, smooth_window)
                expressions.append((f"CSRank({smoothed})", f"range_mom{trend_window}_{flow}_{smooth_mode}{smooth_window}"))

            volatility_gap = f"Sub(ZScore({flow_expr}),ZScore(Std({range_width},{trend_window})))"
            for smooth_mode, smooth_window in smooth_specs[:6]:
                smoothed = _smooth(volatility_gap, smooth_mode, smooth_window)
                expressions.append((f"CSRank({smoothed})", f"flow_vs_rangevol{trend_window}_{flow}_{smooth_mode}{smooth_window}"))

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for idx, (expression, motif) in enumerate(expressions, 1):
        digest = _hash(expression)
        if digest in seen:
            continue
        seen.add(digest)
        smooth_bonus = 0.015 if "mean" in motif or "wma" in motif else 0.0
        long_window_bonus = 0.010 if "45" in motif or "60" in motif else 0.0
        proxy = 0.17 + smooth_bonus + long_window_bonus - (0.012 if "identity" in motif else 0.0)
        turnover_proxy = 0.48 if ("mean" in motif or "wma" in motif) else 0.62
        row: dict[str, Any] = {
            "candidate_id": f"phase3cp_lt_event_{idx:05d}",
            "expression_hash": digest,
            "expression": expression,
            "generator_arm": "event_state",
            "generator_route": "phase3cp-low-turnover-event-state-probe",
            "source_generator": "phase3cp_low_turnover_event_state_probe",
            "source_lane": "phase3cp_low_turnover_event_state_probe",
            "round_id": "low_turnover_event_state/round1_targeted",
            "mutation_type": "low_turnover_variant",
            "factor_lane": "targeted_low_turnover_event_opening_state",
            "aligned_ic_mean": _round(proxy / 7.0),
            "abs_aligned_ic_mean": _round(proxy / 7.0),
            "aligned_spread_mean": _round(proxy / 1000.0),
            "spread_hit_rate": _round(0.52 + proxy * 0.06),
            "positive_horizon_count": 3,
            "mean_one_way_turnover": _round(turnover_proxy),
            "phase3ca_proxy_quality": _round(proxy),
            "proxy_quality": _round(proxy),
            "blocker_flags": "",
            "phase3bp_blocker_flags": "",
            "motif_label": motif,
            "metric_boundary": "targeted low-turnover proxy; real CM train_reward is required",
        }
        row.update(normalize_candidate_schema(row))
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _run_cm(
    *,
    candidate_table: Path,
    shard_root: Path,
    output_root: Path,
    report_root: Path,
    candidate_limit: int,
    max_shards: int,
    sample_trade_times: int,
    horizons: str,
    numexpr_threads: int,
) -> dict[str, Any]:
    argv = [
        "--candidate-audit",
        str(candidate_table),
        "--shard-root",
        str(shard_root),
        "--output-root",
        str(output_root / "phase3cm_train_reward"),
        "--report-root",
        str(report_root / "phase3cm_train_reward"),
        "--candidate-limit",
        str(candidate_limit),
        "--max-shards",
        str(max_shards),
        "--sample-trade-times-per-shard",
        str(sample_trade_times),
        "--horizons",
        horizons,
        "--numexpr-threads",
        str(numexpr_threads),
        "--fast-mode",
    ]
    result = phase3cm_main(argv)
    if int(result or 0) != 0:
        raise RuntimeError(f"Phase3CM failed with exit code {result}")
    return json.loads((output_root / "phase3cm_train_reward" / "phase3cm_train_reward_audit_summary.json").read_text(encoding="utf-8"))


def _render_md(summary: dict[str, Any], top_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3CP Low-Turnover Event-State Probe 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Result",
        "",
        "```text",
        f"generated_candidates: {summary['generated_candidates']}",
        f"ca_candidate_count: {summary['ca_summary']['candidate_count']}",
        f"cm_candidate_count: {summary['cm_summary']['candidate_count']}",
        f"cm_followup_count: {summary['cm_summary']['followup_count']}",
        f"lineage_consistent: {summary['lineage_consistency_summary']['lineage_consistent']}",
        "```",
        "",
        "## Top Train Reward Rows",
        "",
        "```text",
    ]
    for row in top_rows[:8]:
        lines.append(
            f"{row.get('candidate_id')} arm={row.get('generator_arm')} "
            f"train_reward={row.get('train_reward')} train_sortino={row.get('train_day_sortino')} "
            f"val_sortino={row.get('validation_day_sortino')} turnover={row.get('train_mean_one_way_turnover')} "
            f"decision={row.get('train_reward_decision')}"
        )
    lines.extend(
        [
            "```",
            "",
            "## Boundary",
            "",
            "- Targeted diagnostic route only.",
            "- No X0/R3 modification.",
            "- No Phase3CQ launch authority unless CM followup families appear.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--candidate-budget", type=int, default=64)
    parser.add_argument("--ca-top-n", type=int, default=48)
    parser.add_argument("--cm-candidate-limit", type=int, default=24)
    parser.add_argument("--cm-max-shards", type=int, default=1)
    parser.add_argument("--cm-sample-trade-times-per-shard", type=int, default=48)
    parser.add_argument("--cm-horizons", default="1,5,15")
    parser.add_argument("--numexpr-threads", type=int, default=4)
    parser.add_argument("--min-clean-feedback", type=int, default=2)
    args = parser.parse_args(argv)

    shard_root = _resolve(args.shard_root)
    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    if not shard_root.exists():
        raise FileNotFoundError(shard_root)

    rows = _candidate_rows(args.candidate_budget)
    search_root = output_root / "search_outputs" / "low_turnover_event_state" / "round1_targeted"
    report_search_root = report_root / "search_outputs" / "low_turnover_event_state" / "round1_targeted"
    _write_csv(search_root / "phase3cp_top_decisions.csv", rows)
    _write_csv(report_search_root / "phase3cp_top_decisions.csv", rows)
    _write_csv(output_root / "phase3cp_low_turnover_generated_candidates.csv", rows)
    _write_csv(report_root / "phase3cp_low_turnover_generated_candidates.csv", rows)

    ca_root = output_root / "phase3ca_bridge"
    report_ca_root = report_root / "phase3ca_bridge"
    ca_summary = build_candidate_table([output_root / "search_outputs"], ca_root, top_n=args.ca_top_n, allow_high_corr=False)
    _copy_report_files(ca_root, report_ca_root)
    candidate_table, field_gate_summary = _filter_cm_feasible_candidates(
        ca_table=ca_root / "phase3ca_bz_candidate_audit.csv",
        shard_root=shard_root,
        max_shards=args.cm_max_shards,
        limit=args.cm_candidate_limit,
        selection_mode="ca_ranked",
        output_root=output_root,
        report_root=report_root,
    )
    cm_summary = _run_cm(
        candidate_table=candidate_table,
        shard_root=shard_root,
        output_root=output_root,
        report_root=report_root,
        candidate_limit=args.cm_candidate_limit,
        max_shards=args.cm_max_shards,
        sample_trade_times=args.cm_sample_trade_times_per_shard,
        horizons=args.cm_horizons,
        numexpr_threads=args.numexpr_threads,
    )
    cm_table = output_root / "phase3cm_train_reward" / "phase3cm_train_reward.csv"
    lineage_summary = _audit_cm_lineage_consistency(
        candidate_table=candidate_table,
        cm_table=cm_table,
        output_root=output_root,
        report_root=report_root,
    )
    cn_summary = build_feedback_memory(
        cm_tables=[cm_table],
        cm_roots=[],
        output_root=output_root / "phase3cn_feedback_memory",
        report_root=report_root / "phase3cn_feedback_memory",
        train_threshold=0.0,
        validation_floor=0.0,
        max_turnover=0.75,
        max_family_share=0.25,
        min_clean_feedback=args.min_clean_feedback,
    )
    with (report_root / "phase3cm_train_reward" / "phase3cm_train_reward.csv").open("r", encoding="utf-8-sig", newline="") as handle:
        reward_rows = sorted(
            [dict(row) for row in csv.DictReader(handle)],
            key=lambda row: float(row.get("train_reward") or -999.0),
            reverse=True,
        )
    checks = {
        "generated_candidates_ok": len(rows) == int(args.candidate_budget),
        "ca_has_candidates": int(ca_summary["candidate_count"]) > 0,
        "field_gate_has_candidates": int(field_gate_summary["candidate_count"]) > 0,
        "real_cm_eval_used": str(cm_summary.get("experiment_id")) == "20260623_phase3cm_train_portfolio_sortino_reward_audit",
        "lineage_consistent": bool(lineage_summary.get("lineage_consistent")),
        "cn_memory_matches_cm": int(cn_summary["candidate_count"]) == int(cm_summary["candidate_count"]),
    }
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cp_low_turnover_event_state_probe",
        "decision": "PHASE3CP_LOW_TURNOVER_EVENT_STATE_PROBE_READY_DIAGNOSTIC_ONLY" if all(checks.values()) else "PHASE3CP_LOW_TURNOVER_EVENT_STATE_PROBE_FAIL",
        "generated_candidates": len(rows),
        "shard_root": str(shard_root),
        "checks": checks,
        "ca_summary": ca_summary,
        "field_gate_summary": field_gate_summary,
        "cm_summary": cm_summary,
        "lineage_consistency_summary": lineage_summary,
        "cn_summary": cn_summary,
        "top_train_reward_rows": reward_rows[:8],
        "metric_boundary": "targeted low-turnover event/opening-state diagnostic; not proof",
    }
    _write_json(output_root / "phase3cp_low_turnover_event_state_probe_summary.json", summary)
    _write_json(report_root / "phase3cp_low_turnover_event_state_probe_summary.json", summary)
    _write_csv(report_root / "phase3cp_low_turnover_event_state_probe_checks.csv", [checks])
    (report_root / "PHASE3CP_LOW_TURNOVER_EVENT_STATE_PROBE_20260623.md").write_text(_render_md(summary, reward_rows), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0 if all(checks.values()) else 2


if __name__ == "__main__":
    raise SystemExit(main())
