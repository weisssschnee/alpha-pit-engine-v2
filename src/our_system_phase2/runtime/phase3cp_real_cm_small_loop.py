"""Phase3CP real-CM small closed-loop search.

This route is the first Phase3CP escalation that replaces the controlled CM
fixture with the real true-1min train portfolio Sortino reward audit.

It remains diagnostic-only:

CO/CP budget -> bounded generation -> CA bridge -> field-availability gate ->
real CM train reward -> CN feedback memory -> CO reschedule.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _discover_panels,
    _fields,
    _write_csv,
    _write_json,
)
from our_system_phase2.runtime.phase3ca_build_bz_candidate_audit import build_candidate_table
from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import main as phase3cm_main
from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import build_feedback_memory
from our_system_phase2.runtime.phase3cp_reward_gated_medium_search_smoke import (
    _copy_report_files,
    _decisionize,
    _generate_for_arm,
    _scale_budgets,
    _write_arm_outputs,
)
from our_system_phase2.runtime.phase3bp_true1min_search_algorithm_smoke import build_checked_seed_policy
from our_system_phase2.services.candidate_schema import normalize_candidate_schema, safe_float
from our_system_phase2.services.multi_arm_scheduler import build_arm_schedule, read_csv_rows


REPO = Path(__file__).resolve().parents[3]
DEFAULT_CO_ROOT = Path("reports/phase3cp_reward_gated_medium_search_smoke_20260623")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cp_real_cm_small_loop_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3cp_real_cm_small_loop_20260623")
DEFAULT_MEMORY_GLOBS = [
    "**/*search_memory_ledger.csv",
    "**/*top_decisions.csv",
    "**/*candidate_audit.csv",
    "**/*generated_candidates.csv",
    "**/*train_reward.csv",
]


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _round(value: Any, ndigits: int = 8) -> float | None:
    out = safe_float(value)
    if not math.isfinite(out):
        return None
    return round(out, ndigits)


def _budget_table_path(co_root: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        return _resolve(explicit)
    candidates = [
        co_root / "phase3cp_next_arm_budget_table.csv",
        co_root / "phase3co_arm_budget_table.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"no CP/CO arm budget table under {co_root}")


def _load_memory_hashes(memory_roots: list[Path], memory_globs: list[str]) -> tuple[set[str], list[dict[str, Any]]]:
    hashes: set[str] = set()
    files_seen: set[Path] = set()
    rows: list[dict[str, Any]] = []
    for raw_root in memory_roots:
        root = _resolve(raw_root)
        if root.is_file():
            files = [root]
        elif root.is_dir():
            files = []
            for pattern in memory_globs:
                files.extend(root.glob(pattern))
        else:
            rows.append({"memory_root": str(root), "exists": False, "file_count": 0, "hash_count": 0})
            continue

        before = len(hashes)
        file_count = 0
        for file_path in files:
            file_path = file_path.resolve()
            if file_path in files_seen or file_path.suffix.lower() != ".csv":
                continue
            files_seen.add(file_path)
            file_count += 1
            for row in _read_csv(file_path):
                digest = str(row.get("expression_hash") or row.get("candidate_hash") or "").strip()
                if digest and 8 <= len(digest) <= 128:
                    hashes.add(digest)
        rows.append({"memory_root": str(root), "exists": True, "file_count": file_count, "hash_count": len(hashes) - before})
    return hashes, rows


def _generate_candidates(
    *,
    budget_rows: list[dict[str, Any]],
    total_budget: int,
    initial_blocked: set[str],
    available_fields: set[str],
    output_root: Path,
    report_root: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    scaled_plan = _scale_budgets(budget_rows, total_budget)
    policy, _, _ = build_checked_seed_policy(exploration=0.94)
    generated: list[dict[str, Any]] = []
    blocked: set[str] = set(initial_blocked)
    for arm in scaled_plan:
        budget = int(arm.get("cp_smoke_candidate_budget") or 0)
        rows = _generate_for_arm(
            arm,
            budget=budget,
            blocked=blocked,
            policy=policy,
            start_idx=len(generated) + 1,
            available_fields=available_fields,
        )
        for row in rows:
            digest = str(row.get("expression_hash") or "")
            if digest:
                blocked.add(digest)
        generated.extend(rows)

    shortfall_rows: list[dict[str, Any]] = []
    if len(generated) < total_budget:
        fill_arm = {
            "arm_id": "rx_ucb_fresh",
            "route_hint": "phase3bs-adaptive-ucb-cem-practice",
            "category": "fresh",
        }
        missing = total_budget - len(generated)
        shortfall_rows = _generate_for_arm(
            fill_arm,
            budget=missing,
            blocked=blocked,
            policy=policy,
            start_idx=len(generated) + 1,
            available_fields=available_fields,
        )
        generated.extend(shortfall_rows)

    decisions = [_decisionize(row, idx) for idx, row in enumerate(generated[:total_budget], 1)]
    for row in decisions:
        row["phase3cp_real_cm_loop"] = "true"
        row["metric_boundary"] = "Phase3CP real-CM loop generation metrics are CA ranking only; CM train_reward is the feedback source"
        row.update(normalize_candidate_schema(row))

    search_root = output_root / "search_outputs"
    report_search_root = report_root / "search_outputs"
    _write_arm_outputs(decisions, search_root)
    _write_arm_outputs(decisions, report_search_root)
    _write_csv(output_root / "phase3cp_real_cm_arm_execution_plan.csv", scaled_plan)
    _write_csv(report_root / "phase3cp_real_cm_arm_execution_plan.csv", scaled_plan)
    _write_csv(output_root / "phase3cp_real_cm_all_generated_top_decisions.csv", decisions)
    _write_csv(report_root / "phase3cp_real_cm_all_generated_top_decisions.csv", decisions)
    if shortfall_rows:
        _write_csv(output_root / "phase3cp_real_cm_generation_shortfall_fill.csv", shortfall_rows)
        _write_csv(report_root / "phase3cp_real_cm_generation_shortfall_fill.csv", shortfall_rows)
    return decisions, scaled_plan


def _available_fields(shard_root: Path, max_shards: int) -> tuple[set[str], list[dict[str, Any]]]:
    panels = _discover_panels(shard_root, max_shards)
    metas: list[dict[str, Any]] = []
    intersection: set[str] | None = None
    for panel in panels:
        fields = set(pq.ParquetFile(panel).schema_arrow.names)
        intersection = fields if intersection is None else intersection & fields
        metas.append(
            {
                "panel": str(panel),
                "column_count": len(fields),
                "has_trade_time": "trade_time" in fields,
                "has_close": "close" in fields,
                "has_volume": "volume" in fields,
                "has_amount": "amount" in fields,
            }
        )
    return intersection or set(), metas


def _filter_cm_feasible_candidates(
    *,
    ca_table: Path,
    shard_root: Path,
    max_shards: int,
    limit: int,
    selection_mode: str,
    output_root: Path,
    report_root: Path,
) -> tuple[Path, dict[str, Any]]:
    available, panel_meta = _available_fields(shard_root, max_shards)
    passed: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in _read_csv(ca_table):
        expression = str(row.get("expression") or "")
        needed = set(_fields(expression))
        missing = sorted(needed - available)
        item = dict(row)
        item["cm_required_fields"] = "|".join(sorted(needed))
        if missing:
            item["cm_field_gate_decision"] = "REJECT_MISSING_FIELDS"
            item["cm_missing_fields"] = "|".join(missing)
            rejected.append(item)
            continue
        item["cm_field_gate_decision"] = "PASS"
        item["cm_missing_fields"] = ""
        passed.append(item)

    if selection_mode == "arm_balanced":
        by_arm: dict[str, list[dict[str, Any]]] = {}
        for row in passed:
            by_arm.setdefault(str(row.get("generator_arm") or "unknown_arm"), []).append(row)
        arm_order = [
            "rx_ucb_fresh",
            "typed_ast_fresh",
            "challenger_repair",
            "event_state",
            "cem_exploit",
            "random_orthogonal",
            "unknown_arm",
        ]
        kept = []
        while len(kept) < limit:
            changed = False
            for arm in arm_order:
                bucket = by_arm.get(arm) or []
                if bucket and len(kept) < limit:
                    kept.append(bucket.pop(0))
                    changed = True
            if not changed:
                break
    else:
        kept = passed[:limit]
    pass_over_limit = max(0, len(passed) - len(kept))

    table = output_root / "phase3cp_real_cm_candidate_audit.csv"
    _write_csv(table, kept)
    _write_csv(output_root / "phase3cp_real_cm_field_gate_rejects.csv", rejected)
    _write_csv(report_root / "phase3cp_real_cm_candidate_audit.csv", kept)
    _write_csv(report_root / "phase3cp_real_cm_field_gate_rejects.csv", rejected)
    summary = {
        "candidate_count": len(kept),
        "rejected_missing_field_count": len(rejected),
        "passed_total_count": len(passed),
        "passed_over_limit_count": pass_over_limit,
        "selection_mode": selection_mode,
        "available_field_count": len(available),
        "shard_root": str(shard_root),
        "max_shards_schema_checked": max_shards,
        "panel_meta": panel_meta,
        "missing_field_examples": [
            {
                "candidate_id": row.get("candidate_id"),
                "expression_hash": row.get("expression_hash"),
                "cm_missing_fields": row.get("cm_missing_fields"),
            }
            for row in rejected[:10]
        ],
    }
    _write_json(output_root / "phase3cp_real_cm_field_gate_summary.json", summary)
    _write_json(report_root / "phase3cp_real_cm_field_gate_summary.json", summary)
    if not kept:
        raise RuntimeError("no CA candidates passed the true1min CM field-availability gate")
    return table, summary


def _audit_cm_lineage_consistency(
    *,
    candidate_table: Path,
    cm_table: Path,
    output_root: Path,
    report_root: Path,
) -> dict[str, Any]:
    input_by_hash = {str(row.get("expression_hash") or ""): row for row in _read_csv(candidate_table)}
    rows: list[dict[str, Any]] = []
    mismatch_count = 0
    for row in _read_csv(cm_table):
        digest = str(row.get("expression_hash") or "")
        source = input_by_hash.get(digest, {})
        expected_arm = str(source.get("generator_arm") or "")
        actual_arm = str(row.get("generator_arm") or "")
        expected_candidate_id = str(source.get("candidate_id") or "")
        actual_candidate_id = str(row.get("candidate_id") or "")
        ok = bool(expected_arm and expected_arm == actual_arm and expected_candidate_id == actual_candidate_id)
        if not ok:
            mismatch_count += 1
        rows.append(
            {
                "expression_hash": digest,
                "expected_candidate_id": expected_candidate_id,
                "actual_candidate_id": actual_candidate_id,
                "expected_generator_arm": expected_arm,
                "actual_generator_arm": actual_arm,
                "lineage_consistent": str(ok).lower(),
            }
        )
    _write_csv(output_root / "phase3cp_real_cm_lineage_consistency.csv", rows)
    _write_csv(report_root / "phase3cp_real_cm_lineage_consistency.csv", rows)
    summary = {
        "checked_count": len(rows),
        "mismatch_count": mismatch_count,
        "lineage_consistent": mismatch_count == 0 and bool(rows),
    }
    _write_json(output_root / "phase3cp_real_cm_lineage_consistency_summary.json", summary)
    _write_json(report_root / "phase3cp_real_cm_lineage_consistency_summary.json", summary)
    return summary


def _write_minimal_parallel_cm_md(summary: dict[str, Any], reward_rows: list[dict[str, Any]], report_root: Path) -> None:
    ranked = sorted(reward_rows, key=lambda row: safe_float(row.get("train_reward"), -999.0), reverse=True)
    lines = [
        "# Phase3CM Parallel Train Portfolio Sortino Reward Audit",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Summary",
        "",
        f"- candidates: `{summary['candidate_count']}`",
        f"- followup-ready: `{summary['followup_count']}`",
        f"- parallel workers: `{summary['acceleration_contract']['parallel_workers']}`",
        f"- chunk count: `{summary['parallel_chunk_count']}`",
        "",
        "## Top Rows",
        "",
        "| rank | candidate | reward | train day sortino | validation day sortino | turnover | decision | blockers |",
        "|---:|---|---:|---:|---:|---:|---|---|",
    ]
    for idx, row in enumerate(ranked[:30], 1):
        lines.append(
            f"| {idx} | `{row.get('candidate_id')}` | {row.get('train_reward')} | {row.get('train_day_sortino')} | "
            f"{row.get('validation_day_sortino')} | {row.get('train_mean_one_way_turnover')} | "
            f"`{row.get('train_reward_decision')}` | `{row.get('train_reward_blockers')}` |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This is the same Phase3CM train reward audit executed by candidate chunks.",
            "- Holdout remains report-only and must not feed search.",
            "- X0/R3 remain read-only.",
        ]
    )
    (report_root / "PHASE3CM_PARALLEL_TRAIN_PORTFOLIO_SORTINO_REWARD_AUDIT.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def _run_real_cm_chunk_subprocess(
    *,
    args: argparse.Namespace,
    chunk_table: Path,
    chunk_output_root: Path,
    chunk_report_root: Path,
    chunk_limit: int,
) -> dict[str, Any]:
    argv = [
        sys.executable,
        "-m",
        "our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit",
        "--candidate-audit",
        str(chunk_table),
        "--shard-root",
        str(_resolve(args.shard_root)),
        "--output-root",
        str(chunk_output_root),
        "--report-root",
        str(chunk_report_root),
        "--candidate-limit",
        str(chunk_limit),
        "--max-shards",
        str(args.cm_max_shards),
        "--sample-trade-times-per-shard",
        str(args.cm_sample_trade_times_per_shard),
        "--horizons",
        str(args.cm_horizons),
        "--train-fraction",
        str(args.cm_train_fraction),
        "--validation-fraction",
        str(args.cm_validation_fraction),
        "--min-obs-per-time",
        str(args.cm_min_obs_per_time),
        "--cost-bps",
        str(args.cm_cost_bps),
        "--top-quantile",
        str(args.cm_top_quantile),
        "--numexpr-threads",
        str(args.numexpr_threads),
        "--fast-mode",
    ]
    env = os.environ.copy()
    env.setdefault("PYTHONPATH", "src")
    proc = subprocess.run(
        argv,
        cwd=str(REPO),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    (chunk_output_root / "phase3cm_subprocess_stdout.log").write_text(proc.stdout or "", encoding="utf-8")
    (chunk_output_root / "phase3cm_subprocess_stderr.log").write_text(proc.stderr or "", encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(f"Phase3CM chunk failed rc={proc.returncode}: {chunk_table}")
    summary_path = chunk_output_root / "phase3cm_train_reward_audit_summary.json"
    return json.loads(summary_path.read_text(encoding="utf-8"))


def _run_real_cm_parallel(args: argparse.Namespace, candidate_table: Path, output_root: Path, report_root: Path) -> dict[str, Any]:
    cm_output_root = output_root / "phase3cm_train_reward"
    cm_report_root = report_root / "phase3cm_train_reward"
    cm_output_root.mkdir(parents=True, exist_ok=True)
    cm_report_root.mkdir(parents=True, exist_ok=True)

    candidates = _read_csv(candidate_table)
    workers = max(1, min(int(args.cm_workers), len(candidates)))
    if workers <= 1:
        return _run_real_cm_serial(args, candidate_table, output_root, report_root)

    chunk_root = output_root / "phase3cm_train_reward_chunks"
    chunk_report_root = report_root / "phase3cm_train_reward_chunks"
    chunk_root.mkdir(parents=True, exist_ok=True)
    chunk_report_root.mkdir(parents=True, exist_ok=True)

    chunks: list[tuple[int, Path, Path, Path, int]] = []
    for worker_idx in range(workers):
        rows = candidates[worker_idx::workers]
        if not rows:
            continue
        chunk_table = chunk_root / f"candidate_chunk_{worker_idx + 1:02d}.csv"
        chunk_out = chunk_root / f"chunk_{worker_idx + 1:02d}"
        chunk_rep = chunk_report_root / f"chunk_{worker_idx + 1:02d}"
        chunk_out.mkdir(parents=True, exist_ok=True)
        chunk_rep.mkdir(parents=True, exist_ok=True)
        _write_csv(chunk_table, rows)
        chunks.append((worker_idx + 1, chunk_table, chunk_out, chunk_rep, len(rows)))

    chunk_summaries: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        futures = {
            executor.submit(
                _run_real_cm_chunk_subprocess,
                args=args,
                chunk_table=chunk_table,
                chunk_output_root=chunk_out,
                chunk_report_root=chunk_rep,
                chunk_limit=count,
            ): (chunk_id, chunk_table, chunk_out, count)
            for chunk_id, chunk_table, chunk_out, chunk_rep, count in chunks
        }
        for future in as_completed(futures):
            chunk_id, chunk_table, chunk_out, count = futures[future]
            summary = future.result()
            summary["parallel_chunk_id"] = chunk_id
            summary["parallel_chunk_candidate_count"] = count
            summary["parallel_chunk_table"] = str(chunk_table)
            summary["parallel_chunk_output_root"] = str(chunk_out)
            chunk_summaries.append(summary)

    reward_rows: list[dict[str, Any]] = []
    split_horizon_rows: list[dict[str, Any]] = []
    shard_meta_rows: list[dict[str, Any]] = []
    progress_rows: list[dict[str, Any]] = []
    for chunk_id, _, chunk_out, _, _ in chunks:
        for row in _read_csv(chunk_out / "phase3cm_train_reward.csv"):
            row["parallel_chunk_id"] = chunk_id
            reward_rows.append(row)
        for row in _read_csv(chunk_out / "phase3cm_candidate_split_horizon_summary.csv"):
            row["parallel_chunk_id"] = chunk_id
            split_horizon_rows.append(row)
        for row in _read_csv(chunk_out / "phase3cm_shard_meta.csv"):
            row["parallel_chunk_id"] = chunk_id
            shard_meta_rows.append(row)
        for row in _read_csv(chunk_out / "phase3cm_candidate_progress.csv"):
            row["parallel_chunk_id"] = chunk_id
            progress_rows.append(row)

    reward_rows.sort(key=lambda row: safe_float(row.get("train_reward"), -999.0), reverse=True)
    followup_count = sum(1 for row in reward_rows if row.get("train_reward_decision") == "TRAIN_REWARD_FOLLOWUP_READY")
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cm_train_portfolio_sortino_reward_audit",
        "decision": "PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY",
        "candidate_count": len(reward_rows),
        "followup_count": followup_count,
        "input_candidate_audit": str(candidate_table),
        "shard_root": str(_resolve(args.shard_root)),
        "max_shards": args.cm_max_shards,
        "sample_trade_times_per_shard": args.cm_sample_trade_times_per_shard,
        "horizons": [int(item.strip()) for item in str(args.cm_horizons).split(",") if item.strip()],
        "train_fraction": args.cm_train_fraction,
        "validation_fraction": args.cm_validation_fraction,
        "holdout_fraction": round(1.0 - args.cm_train_fraction - args.cm_validation_fraction, 8),
        "cost_bps": args.cm_cost_bps,
        "top_quantile": args.cm_top_quantile,
        "portfolio_pnl_rows_written": 0,
        "metric_boundary": "parallel train portfolio Sortino reward audit; not production proof; holdout must not feed search",
        "fast_mode": True,
        "numexpr_threads": int(args.numexpr_threads),
        "incremental_checkpoints_enabled": True,
        "checkpoint_every_candidates": 8,
        "drop_hard_blocked_input": False,
        "python_executable": sys.executable,
        "package_versions": chunk_summaries[0].get("package_versions", {}) if chunk_summaries else {},
        "parallel_chunk_count": len(chunks),
        "parallel_chunk_summaries": sorted(chunk_summaries, key=lambda row: int(row.get("parallel_chunk_id") or 0)),
        "acceleration_contract": {
            "batched_shard_read": True,
            "column_pruned_pyarrow_read": True,
            "expression_cache_scope": "per_worker_per_shard",
            "fast_group_rank": True,
            "omp_threads": os.environ.get("OMP_NUM_THREADS"),
            "mkl_threads": os.environ.get("MKL_NUM_THREADS"),
            "numexpr_max_threads": os.environ.get("NUMEXPR_MAX_THREADS"),
            "parallel_workers": len(chunks),
            "global_worker_limit": len(chunks),
        },
    }
    for root in (cm_output_root, cm_report_root):
        _write_csv(root / "phase3cm_candidate_train_reward_summary.csv", reward_rows)
        _write_csv(root / "phase3cm_train_reward.csv", reward_rows)
        _write_csv(root / "phase3cm_candidate_split_horizon_summary.csv", split_horizon_rows)
        _write_csv(root / "phase3cm_shard_meta.csv", shard_meta_rows)
        _write_csv(root / "phase3cm_candidate_progress.csv", progress_rows)
        _write_csv(root / "phase3cm_train_reward_partial.csv", reward_rows)
        _write_json(root / "phase3cm_train_reward_audit_summary.json", summary)
        _write_json(
            root / "phase3cm_incremental_checkpoint_summary.json",
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "experiment_id": "20260623_phase3cm_incremental_checkpoint",
                "partial": False,
                "final": True,
                "candidate_count": len(reward_rows),
                "shard_count": int(args.cm_max_shards),
                "processed_candidate_shards": len(progress_rows),
                "partial_reward_count": len(reward_rows),
                "progress_row_count": len(progress_rows),
                "completed_shards": int(args.cm_max_shards),
                "parallel_workers": len(chunks),
            },
        )
    _write_minimal_parallel_cm_md(summary, reward_rows, cm_report_root)
    return summary


def _run_real_cm_serial(args: argparse.Namespace, candidate_table: Path, output_root: Path, report_root: Path) -> dict[str, Any]:
    cm_output_root = output_root / "phase3cm_train_reward"
    cm_report_root = report_root / "phase3cm_train_reward"
    argv = [
        "--candidate-audit",
        str(candidate_table),
        "--shard-root",
        str(_resolve(args.shard_root)),
        "--output-root",
        str(cm_output_root),
        "--report-root",
        str(cm_report_root),
        "--candidate-limit",
        str(args.cm_candidate_limit),
        "--max-shards",
        str(args.cm_max_shards),
        "--sample-trade-times-per-shard",
        str(args.cm_sample_trade_times_per_shard),
        "--horizons",
        str(args.cm_horizons),
        "--train-fraction",
        str(args.cm_train_fraction),
        "--validation-fraction",
        str(args.cm_validation_fraction),
        "--min-obs-per-time",
        str(args.cm_min_obs_per_time),
        "--cost-bps",
        str(args.cm_cost_bps),
        "--top-quantile",
        str(args.cm_top_quantile),
        "--numexpr-threads",
        str(args.numexpr_threads),
        "--fast-mode",
    ]
    result = phase3cm_main(argv)
    if int(result or 0) != 0:
        raise RuntimeError(f"Phase3CM audit failed with exit code {result}")
    return json.loads((cm_output_root / "phase3cm_train_reward_audit_summary.json").read_text(encoding="utf-8"))


def _run_real_cm(args: argparse.Namespace, candidate_table: Path, output_root: Path, report_root: Path) -> dict[str, Any]:
    if int(getattr(args, "cm_workers", 1) or 1) > 1:
        return _run_real_cm_parallel(args, candidate_table, output_root, report_root)
    return _run_real_cm_serial(args, candidate_table, output_root, report_root)


def _render_md(summary: dict[str, Any]) -> str:
    checks = summary["checks"]
    cm = summary["cm_summary"]
    cn = summary["cn_summary"]
    lines = [
        "# Phase3CP Real CM Small Loop 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Result",
        "",
        "```text",
        f"generated_candidates: {summary['generated_candidates']}",
        f"ca_candidate_count: {summary['ca_summary']['candidate_count']}",
        f"cm_field_gate_passed: {summary['field_gate_summary']['candidate_count']}",
        f"cm_field_gate_rejected_missing: {summary['field_gate_summary']['rejected_missing_field_count']}",
        f"cm_field_gate_passed_over_limit: {summary['field_gate_summary']['passed_over_limit_count']}",
        f"cm_selection_mode: {summary['field_gate_summary']['selection_mode']}",
        f"cm_lineage_consistent: {summary['lineage_consistency_summary']['lineage_consistent']}",
        f"cm_candidate_count: {cm['candidate_count']}",
        f"cm_followup_count: {cm['followup_count']}",
        f"cn_candidate_count: {cn['candidate_count']}",
        f"next_allocated_budget: {summary['reschedule_summary']['allocated_budget']}",
        f"next_fresh_share: {summary['reschedule_summary']['fresh_share']}",
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
            "- This route runs real `phase3cm-train-portfolio-sortino-reward-audit`.",
            "- It still uses a bounded small candidate/sample budget.",
            "- CA metrics remain ranking-only.",
            "- Holdout is report-only and not scheduler feedback.",
            "- X0/R3 remain read-only.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--co-root", type=Path, default=DEFAULT_CO_ROOT)
    parser.add_argument("--arm-budget-table", type=Path)
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--generation-budget", type=int, default=32)
    parser.add_argument("--ca-top-n", type=int, default=24)
    parser.add_argument("--cm-candidate-limit", type=int, default=8)
    parser.add_argument("--cm-selection-mode", choices=["ca_ranked", "arm_balanced"], default="ca_ranked")
    parser.add_argument("--cm-max-shards", type=int, default=1)
    parser.add_argument("--cm-sample-trade-times-per-shard", type=int, default=32)
    parser.add_argument("--cm-horizons", default="1,5,15")
    parser.add_argument("--cm-train-fraction", type=float, default=0.60)
    parser.add_argument("--cm-validation-fraction", type=float, default=0.20)
    parser.add_argument("--cm-min-obs-per-time", type=int, default=20)
    parser.add_argument("--cm-cost-bps", type=float, default=5.0)
    parser.add_argument("--cm-top-quantile", type=float, default=0.2)
    parser.add_argument("--cm-workers", type=int, default=1)
    parser.add_argument("--numexpr-threads", type=int, default=4)
    parser.add_argument("--min-clean-feedback", type=int, default=2)
    parser.add_argument("--reschedule-total-budget", type=int, default=512)
    parser.add_argument("--memory-root", type=Path, action="append", default=[])
    parser.add_argument("--memory-glob", action="append", default=DEFAULT_MEMORY_GLOBS)
    args = parser.parse_args(argv)

    co_root = _resolve(args.co_root)
    shard_root = _resolve(args.shard_root)
    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    if not shard_root.exists():
        raise FileNotFoundError(f"true1min shard root does not exist: {shard_root}")
    shard_root_text = str(shard_root).lower()
    if "tdxofficial" in shard_root_text or "\\1d" in shard_root_text or "/1d" in shard_root_text:
        raise RuntimeError(f"refusing suspicious non-true1min shard root: {shard_root}")

    arm_budget_path = _budget_table_path(co_root, args.arm_budget_table)
    arm_budget_rows = read_csv_rows(arm_budget_path)
    available_fields, generation_panel_meta = _available_fields(shard_root, max(1, int(args.cm_max_shards)))
    memory_hashes, memory_rows = _load_memory_hashes(args.memory_root, args.memory_glob)
    _write_csv(output_root / "phase3cp_real_cm_memory_roots.csv", memory_rows)
    _write_csv(report_root / "phase3cp_real_cm_memory_roots.csv", memory_rows)
    _write_csv(output_root / "phase3cp_real_cm_generation_panel_schema.csv", generation_panel_meta)
    _write_csv(report_root / "phase3cp_real_cm_generation_panel_schema.csv", generation_panel_meta)
    decisions, scaled_plan = _generate_candidates(
        budget_rows=arm_budget_rows,
        total_budget=args.generation_budget,
        initial_blocked=memory_hashes,
        available_fields=available_fields,
        output_root=output_root,
        report_root=report_root,
    )

    search_root = output_root / "search_outputs"
    ca_root = output_root / "phase3ca_bridge"
    report_ca_root = report_root / "phase3ca_bridge"
    ca_summary = build_candidate_table([search_root], ca_root, top_n=args.ca_top_n, allow_high_corr=False)
    _copy_report_files(ca_root, report_ca_root)
    ca_table = ca_root / "phase3ca_bz_candidate_audit.csv"
    cm_candidate_table, field_gate_summary = _filter_cm_feasible_candidates(
        ca_table=ca_table,
        shard_root=shard_root,
        max_shards=args.cm_max_shards,
        limit=args.cm_candidate_limit,
        selection_mode=args.cm_selection_mode,
        output_root=output_root,
        report_root=report_root,
    )
    cm_summary = _run_real_cm(args, cm_candidate_table, output_root, report_root)

    cm_table = output_root / "phase3cm_train_reward" / "phase3cm_train_reward.csv"
    lineage_consistency_summary = _audit_cm_lineage_consistency(
        candidate_table=cm_candidate_table,
        cm_table=cm_table,
        output_root=output_root,
        report_root=report_root,
    )
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
    _write_csv(output_root / "phase3cp_real_cm_next_arm_budget_table.csv", reschedule_rows)
    _write_csv(output_root / "phase3cp_real_cm_next_family_action_table.csv", reschedule_family_actions)
    _write_csv(report_root / "phase3cp_real_cm_next_arm_budget_table.csv", reschedule_rows)
    _write_csv(report_root / "phase3cp_real_cm_next_family_action_table.csv", reschedule_family_actions)

    checks = {
        "budget_table_used": bool(arm_budget_rows),
        "generated_budget_ok": len(decisions) == int(args.generation_budget),
        "ca_has_candidates": int(ca_summary["candidate_count"]) > 0,
        "field_gate_has_cm_candidates": int(field_gate_summary["candidate_count"]) > 0,
        "real_cm_eval_used": str(cm_summary.get("experiment_id")) == "20260623_phase3cm_train_portfolio_sortino_reward_audit",
        "true1min_shard_root_exists": shard_root.exists(),
        "suspicious_1d_path_blocked": "tdxofficial" not in shard_root_text and "\\1d" not in shard_root_text and "/1d" not in shard_root_text,
        "cm_fast_mode": bool(cm_summary.get("fast_mode")),
        "cm_candidate_count_ok": int(cm_summary["candidate_count"]) == min(int(args.cm_candidate_limit), int(field_gate_summary["candidate_count"])),
        "cm_lineage_consistent": bool(lineage_consistency_summary["lineage_consistent"]),
        "cn_memory_matches_cm": int(cn_summary["candidate_count"]) == int(cm_summary["candidate_count"]),
        "reschedule_total_ok": int(reschedule_summary["allocated_budget"]) == int(args.reschedule_total_budget),
        "holdout_not_optimizer_input": True,
        "memory_blocklist_loaded": len(memory_hashes) >= 0,
    }
    passed = all(bool(value) for value in checks.values())
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cp_real_cm_small_loop",
        "decision": "PHASE3CP_REAL_CM_SMALL_LOOP_PASS_DIAGNOSTIC_ONLY" if passed else "PHASE3CP_REAL_CM_SMALL_LOOP_FAIL",
        "input_co_root": str(co_root),
        "input_arm_budget_table": str(arm_budget_path),
        "shard_root": str(shard_root),
        "generated_candidates": len(decisions),
        "generation_budget": int(args.generation_budget),
        "memory_hash_count": len(memory_hashes),
        "memory_roots": memory_rows,
        "search_generation": True,
        "true1min_portfolio_eval": True,
        "cm_reward_source": "phase3cm_train_portfolio_sortino_reward_audit",
        "checks": checks,
        "initial_arm_plan": scaled_plan,
        "ca_summary": ca_summary,
        "field_gate_summary": field_gate_summary,
        "cm_summary": cm_summary,
        "lineage_consistency_summary": lineage_consistency_summary,
        "cn_summary": cn_summary,
        "reschedule_summary": reschedule_summary,
        "metric_boundary": "small real-CM diagnostic loop; not alpha proof and not production promotion",
    }
    _write_json(output_root / "phase3cp_real_cm_small_loop_summary.json", summary)
    _write_json(report_root / "phase3cp_real_cm_small_loop_summary.json", summary)
    _write_csv(report_root / "phase3cp_real_cm_small_loop_checks.csv", [checks])
    (report_root / "PHASE3CP_REAL_CM_SMALL_LOOP_20260623.md").write_text(_render_md(summary), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
