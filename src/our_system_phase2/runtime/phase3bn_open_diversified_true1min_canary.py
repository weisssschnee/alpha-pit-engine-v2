"""Open diversified true-1min canary after Phase3BM crowding.

BN deliberately steps away from the crowded `close/vwap/m1_first30_vwap` sibling
family. It builds a broader minute pack across opening amount/range, intraday
return, range pressure, volume acceleration, and price-location lanes, then runs
a bounded true-1min materialization canary.

This is diagnostic-only and keeps X0/R3 read-only.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _discover_panels,
    _f,
    _fields,
    _fmt,
    _max_expression_window,
    _run_materialization,
    _write_csv,
    _write_json,
)


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3bn_open_diversified_true1min_canary_20260615")
DEFAULT_REPORT_ROOT = Path("reports/phase3bn_open_diversified_true1min_canary_20260615")
DEFAULT_MEMORY_ROOT = Path("runtime/search_memory")
DEFAULT_CE1_BLOCKED_VIEW_PATHS = (
    Path("reports/phase3ce1_search_memory_blocked_view_20260618/search_memory_blocked_keys.csv"),
    Path("runtime/phase3ce1_search_memory_blocked_view_20260618/search_memory_blocked_keys.csv"),
)
EPS = "0.000001"


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _hash(text: str, length: int = 24) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:length]


def _load_memory_hashes(memory_root: Path) -> set[str]:
    root = _resolve(memory_root)
    blocked: set[str] = set()
    paths: list[Path] = []
    if root.exists():
        paths.extend(list(root.rglob("phase3aj_search_memory_ledger.json")) if root.is_dir() else [root])
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        entries = payload.get("memory_entries") if isinstance(payload, dict) else payload
        if not isinstance(entries, list):
            continue
        for row in entries:
            if not isinstance(row, dict):
                continue
            for key in ("expression_hash", "search_memory_key"):
                value = str(row.get(key) or "").strip()
                if value:
                    blocked.add(value)
    ce1_paths = [path for path in (_resolve(item) for item in DEFAULT_CE1_BLOCKED_VIEW_PATHS) if path.exists()]
    if root.exists() and root.is_dir():
        ce1_paths.extend(root.rglob("search_memory_blocked_keys.csv"))
    for path in dict.fromkeys(ce1_paths):
        try:
            rows = _read_csv(path)
        except Exception:
            continue
        for row in rows:
            if str(row.get("memory_block_policy") or "").strip() not in {"preserve_key_for_duplicate_block", ""}:
                continue
            for key in ("expression_hash", "expression_key", "search_memory_key"):
                value = str(row.get(key) or "").strip()
                if value:
                    blocked.add(value)
    return blocked


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _prior_hashes(paths: list[Path]) -> set[str]:
    out: set[str] = set()
    for path in paths:
        for row in _read_csv(_resolve(path)):
            expr = str(row.get("expression") or "").strip()
            expr_hash = str(row.get("expression_hash") or "").strip()
            if expr:
                out.add(_hash(expr))
            if expr_hash:
                out.add(expr_hash)
    return out


def _add(
    rows: list[dict[str, Any]],
    seen: set[str],
    blocked: set[str],
    expression: str,
    *,
    factor_lane: str,
    source_lane: str,
    note: str,
) -> None:
    expression = expression.strip()
    digest = _hash(expression)
    fields = _fields(expression)
    memory_key = f"phase3bn:{digest}"
    if digest in seen or digest in blocked or memory_key in blocked:
        return
    seen.add(digest)
    rows.append(
        {
            "candidate_id": f"phase3bn_open_diversified_{len(rows) + 1:05d}",
            "expression_hash": digest,
            "expression": expression,
            "factor_lane": factor_lane,
            "source_lane": source_lane,
            "source_generator": "phase3bn_open_diversified_true1min_canary_v1",
            "fields": "|".join(fields),
            "fields_list": fields,
            "max_window": _max_expression_window(expression),
            "search_memory_key": memory_key,
            "fresh_search_intent": True,
            "x0_r3_role": "read_only_research_candidate",
            "note": note,
        }
    )


def _candidate_templates(max_candidates: int, blocked: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    windows = [2, 3, 5, 8, 10, 15, 20, 30]
    opening_prefixes = ["m1_first5", "m1_first15", "m1_first30"]

    for prefix in opening_prefixes:
        for window in windows:
            _add(
                rows,
                seen,
                blocked,
                f"CSRank(Div(${prefix}_amount,Add(Abs(Mean($amount,{window})),{EPS})))",
                factor_lane="opening_amount_pressure",
                source_lane="phase3bn_open_fresh",
                note="opening auction/session amount pressure against rolling amount",
            )
            _add(
                rows,
                seen,
                blocked,
                f"CSRank(Sub(ZScore(Div(${prefix}_vol,Add(Abs(Mean($volume,{window})),{EPS}))),ZScore(Div(${prefix}_range,Add(Abs($open),{EPS})))))",
                factor_lane="opening_volume_range_imbalance",
                source_lane="phase3bn_open_fresh",
                note="opening volume pressure net of opening range",
            )
            _add(
                rows,
                seen,
                blocked,
                f"CSRank(Div(Sub(${prefix}_last_close,$open),Add(Abs($open),{EPS})))",
                factor_lane="opening_directional_return",
                source_lane="phase3bn_open_fresh",
                note="opening-window directional move from open",
            )
            _add(
                rows,
                seen,
                blocked,
                f"CSRank(Sub(ZScore(${prefix}_vwap_return_vs_open),ZScore(Delta($ret_1m,{window}))))",
                factor_lane="opening_vs_intraday_return_divergence",
                source_lane="phase3bn_open_fresh",
                note="opening VWAP move contrasted with recent minute return",
            )

    for window in windows:
        range_norm = f"Div(Sub($high,$low),Add(Abs($open),{EPS}))"
        loc = f"Div(Sub($close,$low),Add(Abs(Sub($high,$low)),{EPS}))"
        _add(
            rows,
            seen,
            blocked,
            f"CSRank(Div(Std({range_norm},{window}),Add(Abs(Mean({range_norm},{window})),{EPS})))",
            factor_lane="range_pressure",
            source_lane="phase3bn_open_fresh",
            note="minute range pressure independent of vwap residual family",
        )
        _add(
            rows,
            seen,
            blocked,
            f"CSRank(Sub(ZScore({loc}),ZScore(Mean({loc},{window}))))",
            factor_lane="price_location_shift",
            source_lane="phase3bn_open_fresh",
            note="close location inside high-low bar range",
        )
        _add(
            rows,
            seen,
            blocked,
            f"CSRank(Sub(ZScore(Delta($amount,{window})),ZScore(Delta($volume,{window}))))",
            factor_lane="amount_volume_acceleration_spread",
            source_lane="phase3bn_open_fresh",
            note="amount acceleration vs share volume acceleration",
        )
        _add(
            rows,
            seen,
            blocked,
            f"CSRank(Div(ZScore(Delta($intraday_ret_from_open,{window})),Add(Abs(ZScore(Std($ret_1m,{window}))),{EPS})))",
            factor_lane="intraday_return_efficiency",
            source_lane="phase3bn_open_fresh",
            note="intraday trend efficiency adjusted by minute return volatility",
        )
        _add(
            rows,
            seen,
            blocked,
            f"CSRank(Sub(ZScore(Delta($pct_chg,{window})),ZScore(Delta($intraday_ret_from_open,{window}))))",
            factor_lane="pct_chg_intraday_divergence",
            source_lane="phase3bn_open_fresh",
            note="vendor pct_chg movement vs intraday return from open",
        )

    for prefix in opening_prefixes:
        for window in [5, 10, 20, 30]:
            _add(
                rows,
                seen,
                blocked,
                f"CSRank(Add(ZScore(Div(${prefix}_range,Add(Abs($open),{EPS}))),ZScore(Div(Delta($amount,{window}),Add(Abs(Mean($amount,{window})),{EPS})))))",
                factor_lane="opening_range_amount_impulse",
                source_lane="phase3bn_open_fresh",
                note="opening range joined with later amount impulse",
            )
            _add(
                rows,
                seen,
                blocked,
                f"CSRank(Sub(ZScore(Div(${prefix}_amount,Add(Abs(Mean($amount,{window})),{EPS}))),ZScore(Std($ret_1m,{window}))))",
                factor_lane="opening_amount_volatility_residual",
                source_lane="phase3bn_open_fresh",
                note="opening amount after removing minute volatility",
            )

    # Small allowance for vwap, but only when mixed with non-vwap fields. This is
    # intentionally capped so the new canary does not collapse back into BM.
    for window in [5, 10, 20, 30]:
        _add(
            rows,
            seen,
            blocked,
            f"CSRank(Sub(ZScore(Div($m1_first5_amount,Add(Abs(Mean($amount,{window})),{EPS}))),ZScore(Delta($vwap,{window}))))",
            factor_lane="bounded_vwap_mixed_opening_amount",
            source_lane="phase3bn_open_fresh",
            note="bounded vwap exposure, mixed with opening amount",
        )

    rows = _cap_crowded_vwap(rows, max_share=0.20, max_candidates=max_candidates)
    return rows[:max_candidates]


def _cap_crowded_vwap(rows: list[dict[str, Any]], *, max_share: float, max_candidates: int) -> list[dict[str, Any]]:
    max_vwap = int(max_candidates * max_share)
    out: list[dict[str, Any]] = []
    vwap_count = 0
    for row in rows:
        fields = set(str(row.get("fields") or "").split("|"))
        is_crowded = fields <= {"close", "vwap", "m1_first30_vwap", "m1_first15_vwap", "m1_first5_vwap"}
        has_vwap = "vwap" in fields or any(field.endswith("_vwap") for field in fields)
        if is_crowded:
            continue
        if has_vwap:
            if vwap_count >= max_vwap:
                continue
            vwap_count += 1
        out.append(row)
    return out


def _aggregate_decisions(aggregate_rows: list[dict[str, Any]], pairwise_rows: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    by_hash: dict[str, list[dict[str, Any]]] = {}
    for row in aggregate_rows:
        by_hash.setdefault(str(row.get("expression_hash")), []).append(row)
    crowded: set[str] = set()
    for row in pairwise_rows:
        if abs(_f(row.get("signal_rank_corr"), 0.0)) >= 0.8:
            crowded.add(str(row.get("left_expression_hash")))
            crowded.add(str(row.get("right_expression_hash")))
    decisions: list[dict[str, Any]] = []
    for expr_hash, rows in by_hash.items():
        rows = sorted(rows, key=lambda item: abs(_f(item.get("aligned_ic_mean"), 0.0)), reverse=True)
        best = dict(rows[0])
        stable = sum(1 for row in rows if abs(_f(row.get("aligned_ic_mean"), 0.0)) > 0.02)
        blockers: list[str] = []
        if str(expr_hash) in crowded:
            blockers.append("signal_corr_abs_ge_0.80")
        if stable < 2:
            blockers.append("too_few_positive_horizons")
        if _f(best.get("mean_one_way_turnover"), 0.0) > 0.95:
            blockers.append("extreme_turnover")
        aligned_ic = _f(best.get("aligned_ic_mean"), float("nan"))
        abs_ic = abs(aligned_ic) if math.isfinite(aligned_ic) else float("nan")
        if not math.isfinite(abs_ic) or abs_ic <= 0.03:
            blockers.append("weak_dense_primary_abs_ic")
        best["open_direction"] = "long_top" if aligned_ic >= 0 else "short_top"
        best["abs_aligned_ic_mean"] = abs_ic
        best["positive_horizon_count"] = stable
        best["phase3bn_blocker_flags"] = "|".join(blockers)
        best["phase3bn_decision"] = "bn_followup_priority" if not blockers and abs_ic > 0.035 else "bn_watch_or_reject"
        decisions.append(best)
    decisions.sort(key=lambda item: (_f(item.get("abs_aligned_ic_mean"), -999.0), int(item.get("positive_horizon_count") or 0)), reverse=True)
    return decisions[:top_n]


def _render_md(summary: dict[str, Any], decisions: list[dict[str, Any]], lane_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3BN Open Diversified True-1min Canary 2026-06-15",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Scope",
        "",
        f"- candidates generated: `{summary['candidate_count']}`",
        f"- true-1min shard panels: `{summary['panel_count']}`",
        f"- sampled signal trade_times per shard: `{summary['sample_trade_times_per_shard']}`",
        f"- total eval rows: `{summary['total_eval_rows']}`",
        f"- followup priority: `{summary['followup_priority_count']}`",
        "",
        "## Lane Counts",
        "",
        "| lane | count | best aligned IC | followup |",
        "|---|---:|---:|---:|",
    ]
    for row in lane_rows:
        lines.append(f"| `{row['factor_lane']}` | {row['count']} | {_fmt(row.get('best_aligned_ic'))} | {row['followup_count']} |")
    lines.extend(["", "## Top Decisions", "", "| rank | lane | h | fields | abs IC | direction | turnover | decision | blockers |", "|---:|---|---:|---|---:|---|---:|---|---|"])
    for idx, row in enumerate(decisions[:20], 1):
        lines.append(
            f"| {idx} | `{row['factor_lane']}` | {row['horizon_min']} | `{row['fields']}` | "
            f"{_fmt(row.get('abs_aligned_ic_mean'))} | `{row.get('open_direction')}` | {_fmt(row.get('mean_one_way_turnover'))} | "
            f"`{row['phase3bn_decision']}` | `{row.get('phase3bn_blocker_flags') or ''}` |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- True `trade_time` 1min panels only.",
            "- This is open diversified canary, not promotion evidence.",
            "- Crowded pure `close/vwap` residual family is capped and not allowed to dominate.",
            "- X0/R3 remains read-only.",
        ]
    )
    return "\n".join(lines) + "\n"


def _lane_summary(decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_lane: dict[str, list[dict[str, Any]]] = {}
    for row in decisions:
        by_lane.setdefault(str(row.get("factor_lane")), []).append(row)
    rows: list[dict[str, Any]] = []
    for lane, items in by_lane.items():
        rows.append(
            {
                "factor_lane": lane,
                "count": len(items),
                "best_aligned_ic": max((abs(_f(row.get("aligned_ic_mean"), 0.0)) for row in items), default=None),
                "followup_count": sum(1 for row in items if row.get("phase3bn_decision") == "bn_followup_priority"),
            }
        )
    rows.sort(key=lambda row: (_f(row.get("best_aligned_ic"), -999.0), int(row.get("followup_count") or 0)), reverse=True)
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--memory-root", type=Path, default=DEFAULT_MEMORY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-candidates", type=int, default=96)
    parser.add_argument("--top-decisions", type=int, default=32)
    parser.add_argument("--max-shards", type=int, default=8)
    parser.add_argument("--sample-trade-times-per-shard", type=int, default=80)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs-per-time", type=int, default=20)
    args = parser.parse_args(argv)

    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    blocked = _load_memory_hashes(args.memory_root) | _prior_hashes(
        [
            Path("reports/phase3bk_bj_top64_strict_audit_20260615/phase3bk_bj_top64_candidate_audit.csv"),
            Path("reports/phase3bl_bk_priority_signal_materialization_20260615/phase3bl_candidate_horizon_aggregate.csv"),
            Path("reports/phase3bm_bl_pass_focused_replay_20260615/phase3bm_candidate_decisions.csv"),
        ]
    )
    candidates = _candidate_templates(args.max_candidates, blocked)
    horizons = tuple(int(item.strip()) for item in str(args.horizons).split(",") if item.strip())
    panels = _discover_panels(_resolve(args.shard_root), args.max_shards)
    metric_rows, aggregate_rows, meta = _run_materialization(
        candidates=candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
    )
    pairwise_rows = meta.pop("pairwise_rows")
    decisions = _aggregate_decisions(aggregate_rows, pairwise_rows, args.top_decisions)
    lane_rows = _lane_summary(decisions)
    total_eval_rows = sum(int(shard.get("eval_rows") or 0) for shard in meta["shards"])
    followup_count = sum(1 for row in decisions if row.get("phase3bn_decision") == "bn_followup_priority")
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3BN_OPEN_DIVERSIFIED_CANARY_COMPLETE_DIAGNOSTIC_ONLY",
        "candidate_count": len(candidates),
        "blocked_hash_count": len(blocked),
        "panel_count": len(panels),
        "sample_trade_times_per_shard": args.sample_trade_times_per_shard,
        "horizons_min": list(horizons),
        "total_eval_rows": total_eval_rows,
        "followup_priority_count": followup_count,
        "output_root": str(output_root),
        "report_root": str(report_root),
        "hard_boundary": [
            "true trade_time minute panels only",
            "open diversified canary, not production proof",
            "pure close/vwap residual family capped",
            "X0/R3 read-only",
        ],
        **meta,
    }
    _write_csv(output_root / "phase3bn_candidate_pack.csv", candidates)
    _write_csv(output_root / "phase3bn_candidate_horizon_shard_metrics.csv", metric_rows)
    _write_csv(output_root / "phase3bn_candidate_horizon_aggregate.csv", aggregate_rows)
    _write_csv(output_root / "phase3bn_pairwise_signal_rank_corr.csv", pairwise_rows)
    _write_csv(output_root / "phase3bn_top_decisions.csv", decisions)
    _write_json(output_root / "phase3bn_open_diversified_summary.json", summary)
    _write_csv(report_root / "phase3bn_candidate_pack.csv", candidates)
    _write_csv(report_root / "phase3bn_top_decisions.csv", decisions)
    _write_csv(report_root / "phase3bn_lane_summary.csv", lane_rows)
    _write_json(report_root / "phase3bn_open_diversified_summary.json", {**summary, "top_decisions": decisions[:10]})
    (report_root / "PHASE3BN_OPEN_DIVERSIFIED_TRUE1MIN_CANARY_20260615.md").write_text(
        _render_md(summary, decisions, lane_rows),
        encoding="utf-8",
    )
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
