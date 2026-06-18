"""Phase3BX reward audit for Phase3BV top true-1min candidates.

This is a repair audit for BV/BU algorithm-search outputs. It does not rerun
signals and it is not a promotion replay. It computes proxy Sortino and
bootstrap stability from already-written per-shard/horizon spread metrics.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = Path("reports/phase3bx_bv_sortino_mcmc_audit_20260616")


RUNS = {
    "company_bv": {
        "report": Path("reports/phase3bv_company_large_ast_fresh_search_20260616"),
        "runtime": Path("runtime/phase3bv_company_large_ast_fresh_search_20260616"),
    },
    "local_bv": {
        "report": Path("reports/phase3bv_local_large_ast_fresh_search_20260616"),
        "runtime": Path("runtime/phase3bv_local_large_ast_fresh_search_20260616"),
    },
    "local_wide_partial": {
        "report": Path("reports/phase3bv_local_wide_fresh2_20260616"),
        "runtime": Path("runtime/phase3bv_local_wide_fresh2_20260616"),
    },
}


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        out = float(value)
        if not math.isfinite(out):
            return default
        return out
    except Exception:
        return default


def _round(value: Any, ndigits: int = 8) -> float | None:
    val = _float(value)
    if val is None:
        return None
    return round(val, ndigits)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _sortino(values: list[float], annualizer: float = 1.0) -> float | None:
    clean = [v for v in values if math.isfinite(v)]
    if not clean:
        return None
    mean = statistics.fmean(clean)
    downside = [min(0.0, v) for v in clean]
    downside_var = statistics.fmean([v * v for v in downside])
    if downside_var <= 1e-18:
        return None
    return mean / math.sqrt(downside_var) * math.sqrt(annualizer)


def _max_drawdown(values: list[float]) -> float | None:
    clean = [v for v in values if math.isfinite(v)]
    if not clean:
        return None
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for value in clean:
        equity *= 1.0 + value
        peak = max(peak, equity)
        if peak > 0:
            max_dd = min(max_dd, equity / peak - 1.0)
    return max_dd


def _quantile(values: list[float], q: float) -> float | None:
    clean = sorted(v for v in values if math.isfinite(v))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    pos = (len(clean) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return clean[lo]
    return clean[lo] * (hi - pos) + clean[hi] * (pos - lo)


def _bootstrap(values: list[float], *, iterations: int, seed: int) -> dict[str, Any]:
    clean = [v for v in values if math.isfinite(v)]
    if not clean:
        return {"iterations": 0}
    rng = random.Random(seed)
    draws: list[float] = []
    positive = 0
    for _ in range(iterations):
        sample = [clean[rng.randrange(len(clean))] for _ in range(len(clean))]
        val = _sortino(sample)
        if val is None:
            continue
        draws.append(val)
        if val > 0:
            positive += 1
    return {
        "iterations": len(draws),
        "sortino_p05": _round(_quantile(draws, 0.05)),
        "sortino_p25": _round(_quantile(draws, 0.25)),
        "sortino_median": _round(_quantile(draws, 0.50)),
        "sortino_p75": _round(_quantile(draws, 0.75)),
        "sortino_p95": _round(_quantile(draws, 0.95)),
        "prob_sortino_gt_0": _round(positive / len(draws) if draws else None),
    }


def _decision(row: dict[str, Any]) -> str:
    if (row.get("phase3bp_blocker_flags") or "").strip():
        return "HOLD_BLOCKED"
    if (row.get("mcmc_prob_sortino_gt_0") or 0) < 0.80:
        return "HOLD_WEAK_MCMC"
    if (row.get("proxy_sortino") or -999) <= 0:
        return "HOLD_NEGATIVE_SORTINO"
    if (row.get("mean_one_way_turnover") or 999) > 0.75:
        return "HOLD_EXTREME_TURNOVER"
    return "REWARD_AUDIT_FOLLOWUP"


def _collect_run(run_name: str, report_root: Path, runtime_root: Path, *, top_per_round: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not report_root.exists():
        return out
    for round_report in sorted(path for path in report_root.iterdir() if path.is_dir()):
        round_id = round_report.name
        top_rows = _read_csv(round_report / "phase3bt_top_decisions.csv")[:top_per_round]
        metric_rows = _read_csv(runtime_root / round_id / "phase3bt_candidate_horizon_shard_metrics.csv")
        metrics_by_hash: dict[str, list[dict[str, str]]] = defaultdict(list)
        for metric in metric_rows:
            metrics_by_hash[str(metric.get("expression_hash") or "")].append(metric)
        for rank, top in enumerate(top_rows, 1):
            expr_hash = str(top.get("expression_hash") or "")
            aligned_ic = _float(top.get("aligned_ic_mean"), _float(top.get("ic_mean"), 0.0)) or 0.0
            sign = 1.0 if aligned_ic >= 0 else -1.0
            signed_spreads: list[float] = []
            signed_ics: list[float] = []
            horizons: set[str] = set()
            shards: set[str] = set()
            for metric in metrics_by_hash.get(expr_hash, []):
                spread = _float(metric.get("spread_mean"))
                ic = _float(metric.get("ic_mean"))
                if spread is not None:
                    signed_spreads.append(sign * spread)
                if ic is not None:
                    signed_ics.append(sign * ic)
                if metric.get("horizon_min") not in (None, ""):
                    horizons.add(str(metric.get("horizon_min")))
                if metric.get("shard_index") not in (None, ""):
                    shards.add(str(metric.get("shard_index")))
            boot = _bootstrap(signed_spreads, iterations=2000, seed=20260616 + rank)
            row: dict[str, Any] = {
                "run": run_name,
                "round_id": round_id,
                "rank_in_round": rank,
                "candidate_id": top.get("candidate_id"),
                "expression_hash": expr_hash,
                "factor_lane": top.get("factor_lane"),
                "fields": top.get("fields"),
                "expression": top.get("expression"),
                "open_direction": top.get("open_direction"),
                "phase3bp_decision": top.get("phase3bp_decision"),
                "phase3bp_blocker_flags": top.get("phase3bp_blocker_flags"),
                "horizon_count": len(horizons),
                "shard_count": len(shards),
                "proxy_obs": len(signed_spreads),
                "proxy_mean_spread": _round(statistics.fmean(signed_spreads) if signed_spreads else None, 10),
                "proxy_hit_rate": _round(sum(1 for v in signed_spreads if v > 0) / len(signed_spreads) if signed_spreads else None),
                "proxy_sortino": _round(_sortino(signed_spreads)),
                "proxy_max_drawdown": _round(_max_drawdown(signed_spreads)),
                "proxy_mean_signed_ic": _round(statistics.fmean(signed_ics) if signed_ics else None),
                "abs_aligned_ic_mean": _round(top.get("abs_aligned_ic_mean")),
                "positive_horizon_count": int(_float(top.get("positive_horizon_count"), 0) or 0),
                "mean_one_way_turnover": _round(top.get("mean_one_way_turnover")),
                "wrong_lag_prev_ic_mean": _round(top.get("wrong_lag_prev_ic_mean")),
                "wrong_lag_future_ic_mean": _round(top.get("wrong_lag_future_ic_mean")),
                "mcmc_sortino_p05": boot.get("sortino_p05"),
                "mcmc_sortino_median": boot.get("sortino_median"),
                "mcmc_sortino_p95": boot.get("sortino_p95"),
                "mcmc_prob_sortino_gt_0": boot.get("prob_sortino_gt_0"),
                "mcmc_iterations": boot.get("iterations"),
            }
            row["reward_audit_decision"] = _decision(row)
            out.append(row)
    return out


def _render_md(summary: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3BX BV Sortino MCMC Audit 2026-06-16",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Scope",
        "",
        "This audit repairs the Phase3BV/BU reporting gap. It evaluates already-generated true-1min top candidates with direction-adjusted spread-return proxy Sortino and bootstrap/MCMC stability.",
        "",
        "It is not a full execution replay, not a production promotion gate, and not X0/R3 modification.",
        "",
        "## Summary",
        "",
        f"- candidates audited: `{summary['candidate_count']}`",
        f"- followup candidates: `{summary['followup_count']}`",
        f"- blocked or weak candidates: `{summary['hold_count']}`",
        f"- best proxy Sortino: `{summary['best_proxy_sortino']}`",
        f"- best MCMC median Sortino: `{summary['best_mcmc_median_sortino']}`",
        "",
        "## Top Followup Queue",
        "",
        "| rank | run | round | candidate | proxy sortino | mcmc p05 | mcmc median | prob > 0 | turnover | decision | expression |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    follow = [row for row in rows if row.get("reward_audit_decision") == "REWARD_AUDIT_FOLLOWUP"]
    follow = sorted(
        follow,
        key=lambda row: (
            _float(row.get("mcmc_prob_sortino_gt_0"), -1) or -1,
            _float(row.get("mcmc_sortino_median"), -999) or -999,
            _float(row.get("proxy_sortino"), -999) or -999,
        ),
        reverse=True,
    )
    for idx, row in enumerate(follow[:25], 1):
        expr = str(row.get("expression") or "").replace("|", "/")[:120]
        lines.append(
            f"| {idx} | `{row.get('run')}` | `{row.get('round_id')}` | `{row.get('candidate_id')}` | "
            f"{row.get('proxy_sortino')} | {row.get('mcmc_sortino_p05')} | {row.get('mcmc_sortino_median')} | "
            f"{row.get('mcmc_prob_sortino_gt_0')} | {row.get('mean_one_way_turnover')} | "
            f"`{row.get('reward_audit_decision')}` | `{expr}` |"
        )
    lines.extend(
        [
            "",
            "## Bias Boundary",
            "",
            "- Uses true `trade_time` 1min BV/BU materialization outputs.",
            "- Uses per-shard/per-horizon aggregate spread metrics, not tick-level fills.",
            "- Bootstrap/MCMC is over shard-horizon proxy observations; it is a stability smoke, not a full posterior model.",
            "- Candidates with existing BP blocker flags remain HOLD.",
            "- Next valid step is strict replay / cost / execution calibration for the followup queue.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--top-per-round", type=int, default=80)
    args = parser.parse_args(argv)

    output_root = _resolve(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    for run_name, spec in RUNS.items():
        rows.extend(
            _collect_run(
                run_name,
                _resolve(spec["report"]),
                _resolve(spec["runtime"]),
                top_per_round=args.top_per_round,
            )
        )
    rows = sorted(
        rows,
        key=lambda row: (
            str(row.get("reward_audit_decision") == "REWARD_AUDIT_FOLLOWUP"),
            _float(row.get("mcmc_prob_sortino_gt_0"), -1) or -1,
            _float(row.get("mcmc_sortino_median"), -999) or -999,
            _float(row.get("proxy_sortino"), -999) or -999,
        ),
        reverse=True,
    )
    follow = [row for row in rows if row.get("reward_audit_decision") == "REWARD_AUDIT_FOLLOWUP"]
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260616_phase3bx_bv_sortino_mcmc_audit",
        "decision": "HOLD_RESEARCH_PROXY_REWARD_AUDIT_COMPLETE",
        "candidate_count": len(rows),
        "followup_count": len(follow),
        "hold_count": len(rows) - len(follow),
        "best_proxy_sortino": _round(max((_float(row.get("proxy_sortino"), -999) or -999) for row in rows), 6) if rows else None,
        "best_mcmc_median_sortino": _round(max((_float(row.get("mcmc_sortino_median"), -999) or -999) for row in rows), 6) if rows else None,
        "inputs": {key: {k: str(_resolve(v)) for k, v in spec.items()} for key, spec in RUNS.items()},
        "top_per_round": args.top_per_round,
        "metric_boundary": "proxy Sortino over per-shard/per-horizon spread_mean observations; not full execution replay",
    }
    _write_csv(output_root / "phase3bx_bv_sortino_mcmc_audit.csv", rows)
    _write_json(output_root / "phase3bx_bv_sortino_mcmc_summary.json", summary)
    (output_root / "PHASE3BX_BV_SORTINO_MCMC_AUDIT_20260616.md").write_text(_render_md(summary, rows), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
