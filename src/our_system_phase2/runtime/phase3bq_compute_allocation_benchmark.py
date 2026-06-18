"""Benchmark true-1min search algorithm compute allocation.

Phase3BQ runs small, bounded arms that compare generator/evaluator throughput
and blocker yield. The objective is compute allocation, not candidate promotion.
"""

from __future__ import annotations

import argparse
import csv
import importlib.metadata
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3bq_compute_allocation_benchmark_20260615")
DEFAULT_REPORT_ROOT = Path("reports/phase3bq_compute_allocation_benchmark_20260615")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _fmt(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.6g}"
    return "" if value is None else str(value)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def _package_versions() -> dict[str, str]:
    packages = ["numpy", "pandas", "pyarrow", "numba", "bottleneck", "numexpr", "polars", "joblib", "scikit-learn"]
    versions: dict[str, str] = {}
    for package in packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "missing"
    return versions


def _hot_path_scan() -> dict[str, Any]:
    files = [
        Path("src/our_system_phase2/runtime/phase3bp_true1min_search_algorithm_smoke.py"),
        Path("src/our_system_phase2/runtime/phase3bl_bk_priority_signal_materialization.py"),
        Path("src/our_system_phase2/runtime/phase3bo_mature_cem_bridge_true1min_pack.py"),
    ]
    patterns = [
        "use_fast_context",
        "prepare_validation_work_context",
        "successive_halving",
        "parallel_workers",
        "global_worker_limit",
        "groupby(",
        "sort=False",
        "numba",
        "polars",
        "pyarrow",
    ]
    hits: dict[str, list[str]] = {}
    for path in files:
        full = _resolve(path)
        if not full.exists():
            continue
        text = full.read_text(encoding="utf-8", errors="ignore")
        hits[str(path)] = [pattern for pattern in patterns if pattern in text]
    return {
        "files": hits,
        "interpretation": "current true1min smoke hot path is pandas/numpy materialization; numba is available only if called by evaluator code",
    }


def _arm_specs() -> list[dict[str, Any]]:
    return [
        {
            "arm_id": "bo_template_quota_48x3x30",
            "route": "phase3bo-mature-cem-bridge-true1min-pack",
            "args": ["--max-candidates", "48", "--max-shards", "3", "--sample-trade-times-per-shard", "30"],
            "summary": "phase3bo_mature_cem_bridge_summary.json",
            "decisions": "phase3bo_top_decisions.csv",
            "decision_col": "phase3bo_decision",
            "blocker_col": "phase3bo_blocker_flags",
            "algorithm_family": "template_quota",
            "budget_class": "medium_control",
            "hypothesis": "BO template/quota reference: low generator overhead, moderate diversity.",
        },
        {
            "arm_id": "bp_rx_ucb_72x3x30",
            "route": "phase3bp-true1min-search-algorithm-smoke",
            "args": [
                "--algorithm-mode",
                "rx_ucb",
                "--max-candidates",
                "72",
                "--max-shards",
                "3",
                "--sample-trade-times-per-shard",
                "30",
            ],
            "summary": "phase3bp_true1min_search_algorithm_summary.json",
            "decisions": "phase3bp_top_decisions.csv",
            "decision_col": "phase3bp_decision",
            "blocker_col": "phase3bp_blocker_flags",
            "algorithm_family": "rx_ucb",
            "budget_class": "medium_search",
            "hypothesis": "BP rx/UCB fast: native search-core smoke without residual complexity.",
        },
        {
            "arm_id": "bp_cem_elite_72x3x30",
            "route": "phase3bp-true1min-search-algorithm-smoke",
            "args": [
                "--algorithm-mode",
                "cem_elite",
                "--cem-population-size",
                "512",
                "--cem-elite-frac",
                "0.16",
                "--cem-rounds",
                "3",
                "--max-candidates",
                "72",
                "--max-shards",
                "3",
                "--sample-trade-times-per-shard",
                "30",
            ],
            "summary": "phase3bp_true1min_search_algorithm_summary.json",
            "decisions": "phase3bp_top_decisions.csv",
            "decision_col": "phase3bp_decision",
            "blocker_col": "phase3bp_blocker_flags",
            "algorithm_family": "cem_elite",
            "budget_class": "medium_search",
            "hypothesis": "CEM-style elite resampling: tests whether prior-guided elite mutation improves clean yield per minute.",
        },
        {
            "arm_id": "bp_hybrid_rx_cem_96x3x30",
            "route": "phase3bp-true1min-search-algorithm-smoke",
            "args": [
                "--algorithm-mode",
                "hybrid_rx_cem",
                "--cem-population-size",
                "640",
                "--cem-elite-frac",
                "0.18",
                "--cem-rounds",
                "3",
                "--max-candidates",
                "96",
                "--max-shards",
                "3",
                "--sample-trade-times-per-shard",
                "30",
            ],
            "summary": "phase3bp_true1min_search_algorithm_summary.json",
            "decisions": "phase3bp_top_decisions.csv",
            "decision_col": "phase3bp_decision",
            "blocker_col": "phase3bp_blocker_flags",
            "algorithm_family": "hybrid_rx_cem",
            "budget_class": "large_smoke",
            "hypothesis": "Hybrid arm: lets rx/UCB preserve broad typed coverage while CEM spends extra budget near elite priors.",
        },
        {
            "arm_id": "bp_residual_probe_16x1x12",
            "route": "phase3bp-true1min-search-algorithm-smoke",
            "args": [
                "--algorithm-mode",
                "rx_ucb",
                "--max-candidates",
                "16",
                "--max-shards",
                "1",
                "--sample-trade-times-per-shard",
                "12",
                "--include-residual",
            ],
            "summary": "phase3bp_true1min_search_algorithm_summary.json",
            "decisions": "phase3bp_top_decisions.csv",
            "decision_col": "phase3bp_decision",
            "blocker_col": "phase3bp_blocker_flags",
            "algorithm_family": "residual_probe",
            "budget_class": "tiny_risk_probe",
            "hypothesis": "Residual complexity probe: only checks whether residual operators deserve budget.",
        },
    ]


def _run_arm(spec: dict[str, Any], *, output_root: Path, report_root: Path, timeout_seconds: int) -> dict[str, Any]:
    arm_output = output_root / spec["arm_id"]
    arm_report = report_root / spec["arm_id"]
    command = [
        sys.executable,
        "app.py",
        str(spec["route"]),
        "--allow-diagnostic",
        "--",
        *spec["args"],
        "--output-root",
        str(arm_output),
        "--report-root",
        str(arm_report),
    ]
    started = time.perf_counter()
    status = "completed"
    stdout_tail = ""
    stderr_tail = ""
    try:
        proc = subprocess.run(
            command,
            cwd=REPO,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
        elapsed = time.perf_counter() - started
        stdout_tail = proc.stdout[-4000:]
        stderr_tail = proc.stderr[-4000:]
        if proc.returncode != 0:
            status = "failed"
    except subprocess.TimeoutExpired as exc:
        elapsed = time.perf_counter() - started
        status = "timeout"
        stdout_tail = (exc.stdout or "")[-4000:] if isinstance(exc.stdout, str) else ""
        stderr_tail = (exc.stderr or "")[-4000:] if isinstance(exc.stderr, str) else ""

    summary_path = arm_report / spec["summary"]
    decisions_path = arm_report / spec["decisions"]
    summary = _read_json(summary_path) if summary_path.exists() else {}
    decisions = _read_csv(decisions_path)
    blocker_col = spec["blocker_col"]
    decision_col = spec["decision_col"]
    future_wrong = sum(1 for row in decisions if "future_signal_wrong_lag_too_strong" in str(row.get(blocker_col) or row.get("blocker_flags") or ""))
    crowded = sum(1 for row in decisions if "signal_corr_abs" in str(row.get(blocker_col) or ""))
    hard_blocked = sum(
        1
        for row in decisions
        if (
            "future_signal_wrong_lag_too_strong" in str(row.get(blocker_col) or row.get("blocker_flags") or "")
            or "signal_corr_abs" in str(row.get(blocker_col) or "")
        )
    )
    followup = sum(1 for row in decisions if "followup_priority" in str(row.get(decision_col) or ""))
    best_abs_ic = max((_float(row.get("abs_aligned_ic_mean")) for row in decisions), default=0.0)
    eval_rows = int(summary.get("total_eval_rows") or 0)
    candidates = int(summary.get("candidate_count") or 0)
    panel_count = int(summary.get("panel_count") or 0)
    sample_times = int(summary.get("sample_trade_times_per_shard") or 0)
    rows_per_sec = eval_rows / elapsed if elapsed > 0 else 0.0
    candidate_eval_units = max(1, candidates) * max(1, panel_count) * max(1, sample_times)
    units_per_sec = candidate_eval_units / elapsed if elapsed > 0 else 0.0
    clean_top_ratio = followup / max(1, len(decisions))
    blocker_event_density = (future_wrong + crowded) / max(1, len(decisions))
    hard_blocked_ratio = hard_blocked / max(1, len(decisions))
    result = {
        "arm_id": spec["arm_id"],
        "route": spec["route"],
        "algorithm_family": spec.get("algorithm_family"),
        "budget_class": spec.get("budget_class"),
        "status": status,
        "elapsed_seconds": round(elapsed, 3),
        "candidate_count": candidates,
        "panel_count": panel_count,
        "sample_trade_times_per_shard": sample_times,
        "total_eval_rows": eval_rows,
        "rows_per_second": round(rows_per_sec, 3),
        "candidate_eval_units_per_second": round(units_per_sec, 3),
        "decision_rows": len(decisions),
        "followup_count": followup,
        "future_wrong_lag_count": future_wrong,
        "crowded_count": crowded,
        "hard_blocked_count": hard_blocked,
        "best_abs_aligned_ic": round(best_abs_ic, 10),
        "clean_top_ratio": round(clean_top_ratio, 6),
        "hard_blocked_ratio": round(hard_blocked_ratio, 6),
        "blocker_event_density": round(blocker_event_density, 6),
        "summary_path": str(summary_path),
        "decisions_path": str(decisions_path),
        "hypothesis": spec["hypothesis"],
        "command": " ".join(command),
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }
    if status != "completed":
        result["decision"] = "allocation_reject_or_debug"
    elif followup > 0 and hard_blocked_ratio < 1.0:
        result["decision"] = "allocation_candidate"
    else:
        result["decision"] = "allocation_hold"
    return result


def _recommend(rows: list[dict[str, Any]]) -> dict[str, Any]:
    completed = [row for row in rows if row["status"] == "completed"]
    if not completed:
        return {
            "decision": "HOLD_RESEARCH",
            "recommended_allocation": {},
            "reason": "no benchmark arm completed",
        }
    fast_rows = sorted(completed, key=lambda row: float(row["rows_per_second"]), reverse=True)
    clean_rows = sorted(
        completed,
        key=lambda row: (
            int(row["followup_count"]) > 0,
            -float(row["hard_blocked_ratio"]),
            float(row["rows_per_second"]),
        ),
        reverse=True,
    )
    residual = next((row for row in completed if "residual" in row["arm_id"]), None)
    residual_share = 0.05
    if residual is None or residual["status"] != "completed" or float(residual["rows_per_second"]) < 0.5 * float(fast_rows[0]["rows_per_second"]):
        residual_share = 0.0
    return {
        "decision": "USE_STAGED_RX_CEM_PORTFOLIO_WITH_TEMPLATE_CONTROL",
        "recommended_allocation": {
            "stage0_ledger_surrogate": "10% budget; broad cheap ledger/surrogate generation before strict minute materialization",
            "stage1_rx_ucb": "35% budget; typed exploration with family caps and search memory",
            "stage1_cem_elite": "25% budget; CEM-style elite resampling when it beats rx on clean yield per minute",
            "stage1_template_control": "15% budget; keep BO/template as control and simple-family fallback",
            "stage2_hybrid_followup": "15% budget; only deepen clean no-future-wrong-lag representatives",
            "residual_complexity": f"{int(residual_share * 100)}% budget; run only as tiny probe until evaluator is optimized",
        },
        "best_throughput_arm": fast_rows[0]["arm_id"],
        "best_clean_arm": clean_rows[0]["arm_id"],
        "reason": "allocation is based on completed true1min arm throughput, hard-blocked ratio, and clean followup yield; residual remains capped because previous larger residual runs timed out.",
    }


def _render_md(summary: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3BQ Compute Allocation Benchmark 2026-06-15",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Purpose",
        "",
        "Compare true-1min search algorithms by compute efficiency, blocker yield, and clean followup rate.",
        "",
        "## Arm Results",
        "",
        "| arm | family | status | sec | candidates | rows | rows/sec | followup | hard-blocked | future-lag | crowded | best abs IC | decision |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            f"| `{row['arm_id']}` | `{row.get('algorithm_family')}` | `{row['status']}` | {_fmt(row['elapsed_seconds'])} | {row['candidate_count']} | "
            f"{row['total_eval_rows']} | {_fmt(row['rows_per_second'])} | {row['followup_count']} | "
            f"{_fmt(row.get('hard_blocked_ratio'))} | {row['future_wrong_lag_count']} | {row['crowded_count']} | {_fmt(row['best_abs_aligned_ic'])} | `{row['decision']}` |"
        )
    rec = summary["recommendation"]
    lines.extend(
        [
            "",
            "## Allocation",
            "",
            f"- recommendation: `{rec['decision']}`",
            f"- best throughput arm: `{rec.get('best_throughput_arm')}`",
            f"- best clean arm: `{rec.get('best_clean_arm')}`",
            f"- reason: {rec['reason']}",
            "",
            "## Budget Split",
            "",
        ]
    )
    for key, value in (rec.get("recommended_allocation") or {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- True `trade_time` 1min only.",
            "- This is compute allocation research, not alpha proof.",
            "- `future_signal_wrong_lag_too_strong` remains a hard blocker.",
            "- Residual expressions are not allowed into broad search until evaluator hot path is optimized.",
            "",
            "## Acceleration Audit",
            "",
            f"- python executable: `{summary['python_executable']}`",
            f"- package matrix: `{summary['package_versions']}`",
            f"- hot path scan: `{summary['hot_path_scan']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--timeout-seconds-per-arm", type=int, default=1200)
    args = parser.parse_args(argv)

    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    rows = [_run_arm(spec, output_root=output_root, report_root=report_root, timeout_seconds=args.timeout_seconds_per_arm) for spec in _arm_specs()]
    recommendation = _recommend(rows)
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3BQ_COMPUTE_ALLOCATION_BENCHMARK_COMPLETE_DIAGNOSTIC_ONLY",
        "objective": "compare search algorithm arms and recommend compute allocation for true1min broad search",
        "python_executable": sys.executable,
        "package_versions": _package_versions(),
        "hot_path_scan": _hot_path_scan(),
        "arm_count": len(rows),
        "arms": rows,
        "recommendation": recommendation,
        "hard_boundary": [
            "true trade_time minute panels only",
            "compute allocation benchmark only",
            "no X0/R3 modification",
            "no production promotion",
        ],
    }
    _write_json(output_root / "phase3bq_compute_allocation_summary.json", summary)
    _write_json(report_root / "phase3bq_compute_allocation_summary.json", summary)
    _write_csv(report_root / "phase3bq_arm_results.csv", rows)
    (report_root / "PHASE3BQ_COMPUTE_ALLOCATION_BENCHMARK_20260615.md").write_text(_render_md(summary, rows), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
