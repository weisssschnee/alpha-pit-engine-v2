from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from our_system_phase2.services.candidate_submission_receipt import (
    CandidateSubmissionAuthority,
    ReceiptContext,
    write_receipt_table,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority, file_sha256
from our_system_phase2.services.matched_control_pairs import (
    CONTROL_CONSTRUCTOR_MATRIX,
    PAIR_AUTHORIZATION_STATUS,
    PAIR_MAPPING_PORTFOLIO_CONTRACT,
    PAIR_RECEIPT_SCHEMA_VERSION,
    CandidatePairAuthority,
    CandidatePairError,
    build_pair_evaluation_rows,
    group_candidate_pairs,
    partition_candidate_pairs,
    write_pair_receipt_table,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)
from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator


REPO = Path(__file__).resolve().parents[1]
DEFAULT_SPLIT = REPO / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
DEFAULT_REGISTRY = (
    REPO
    / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry"
    / "unified_capability_registry.json"
)
EVALUATOR = REPO / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
FORMAL_CP = REPO / "src/our_system_phase2/runtime/phase3cp_real_cm_small_loop.py"
DATA_RELEASE_HASH = "cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827"
ROUTES = (
    "MINUTE_STATIC",
    "FIRSTN_PATH",
    "SLOW_CROSS_SECTIONAL_LEVEL",
    "SLOW_TEMPORAL_CHANGE",
)
NUMERIC_FIELDS = (
    "primary_train_reward",
    "control_train_reward",
    "matched_train_increment",
    "primary_turnover",
    "control_turnover",
    "matched_turnover_increment",
    "primary_rank_ic",
    "control_rank_ic",
    "matched_rank_ic_increment",
    "pair_support_overlap",
)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    materialized = [dict(row) for row in rows]
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in materialized:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _package_matrix() -> dict[str, str]:
    modules = ("numpy", "pandas", "pyarrow", "numba", "bottleneck", "numexpr", "polars", "joblib", "sklearn")
    output: dict[str, str] = {}
    for name in modules:
        try:
            module = __import__(name)
            output[name] = str(getattr(module, "__version__", "installed"))
        except Exception as exc:  # pragma: no cover - environment evidence
            output[name] = f"missing:{type(exc).__name__}"
    return output


def _build_candidates(
    registry: UnifiedCapabilityRegistry,
) -> list[dict[str, Any]]:
    generator = RegistryDrivenGenerator(registry)
    candidates: list[dict[str, Any]] = []
    for route_index, route_id in enumerate(ROUTES, 1):
        pair = generator.generate_route(route_id, proposal_budget=2, seed=5100 + route_index)
        for row in pair:
            row["expression_hash"] = stable_hash(str(row["expression"]))[:24]
            row["generator_arm"] = f"parity_{route_id.lower()}"
            row["open_direction"] = "long_top"
        candidates.extend(pair)
    return candidates


def _exercise_negative_pair_cases(
    *,
    candidates: list[dict[str, Any]],
    candidate_receipts: list[dict[str, Any]],
    pair_receipts: list[dict[str, Any]],
) -> dict[str, Any]:
    receipt_by_id = {str(row["candidate_id"]): dict(row) for row in candidate_receipts}
    pair_receipt_by_id = {str(row["pair_id"]): dict(row) for row in pair_receipts}
    null_cases: list[dict[str, Any]] = []
    invalid_cases: list[dict[str, Any]] = []
    eligible_identity = stable_hash(["S0001", "S0002", "S0003", "S0004"])
    for primary, control in group_candidate_pairs(candidates):
        pair_id = str(primary["pair_id"])
        receipts = [
            receipt_by_id[str(primary["candidate_id"])],
            receipt_by_id[str(control["candidate_id"])],
        ]
        common_support = {
            "shard_index": 0,
            "trade_time": "2024-01-02T09:35:00",
            "horizon_min": 1,
            "split": "train",
            "eligible_code_count": 4,
            "eligible_code_identity": eligible_identity,
            "long_count": 2,
            "short_count": 2,
            "one_way_turnover": 0.25,
            "top_signal_mean": 1.0,
            "bottom_signal_mean": -1.0,
            "portfolio_weight_identity": "deliberately-identical-null-behavior",
        }
        rewards = [
            {"candidate_id": primary["candidate_id"], "optimizer_reward": 0.0},
            {"candidate_id": control["candidate_id"], "optimizer_reward": 0.0},
        ]
        null_result = build_pair_evaluation_rows(
            candidates=[primary, control],
            candidate_receipts=receipts,
            pair_receipts=[pair_receipt_by_id[pair_id]],
            reward_rows=rewards,
            portfolio_rows_by_expression_hash={
                str(primary["expression_hash"]): [common_support],
                str(control["expression_hash"]): [common_support],
            },
            evaluator_invocation_counts={
                str(primary["candidate_id"]): 1,
                str(control["candidate_id"]): 1,
            },
        )[0]
        null_passed = (
            null_result["pair_evaluation_status"] == "PAIR_EVALUATION_BLOCKED"
            and "control_behavior_identity_equals_primary"
            in str(null_result["pair_evaluation_blockers"])
            and null_result["optimizer_reward"] == ""
        )
        null_cases.append(
            {
                "route_id": primary["route_id"],
                "pair_id": pair_id,
                "case": "NULL_BEHAVIOR_PAIR",
                "expected": "PAIR_EVALUATION_BLOCKED",
                "actual": null_result["pair_evaluation_status"],
                "blockers": null_result["pair_evaluation_blockers"],
                "passed": null_passed,
            }
        )

        drifted_receipts = copy.deepcopy(receipts)
        drifted_receipts[1]["pit_source_lag_contract"][0] += "|UNREGISTERED_FUTURE_LAG"
        rejected = False
        error = ""
        try:
            CandidatePairAuthority().authorize_table([primary, control], drifted_receipts)
        except CandidatePairError as exc:
            rejected = True
            error = str(exc)
        invalid_cases.append(
            {
                "route_id": primary["route_id"],
                "pair_id": pair_id,
                "case": "INVALID_CONTROL_PIT_SOURCE_LAG",
                "expected": "PAIR_AUTHORIZATION_REJECTED",
                "actual": "PAIR_AUTHORIZATION_REJECTED" if rejected else "UNEXPECTEDLY_AUTHORIZED",
                "error": error,
                "passed": rejected and "PIT/source-lag contract mismatch" in error,
            }
        )
    passed = (
        len(null_cases) == len(ROUTES)
        and len(invalid_cases) == len(ROUTES)
        and all(bool(row["passed"]) for row in null_cases + invalid_cases)
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "null_pair_cases": null_cases,
        "invalid_control_cases": invalid_cases,
    }


def _materialize_synthetic_shards(
    *,
    root: Path,
    candidates: list[dict[str, Any]],
    registry: UnifiedCapabilityRegistry,
    shard_count: int,
    null_behavior: bool = False,
) -> list[dict[str, Any]]:
    fields = sorted({str(field) for row in candidates for field in row.get("declared_field_ids") or ()})
    registry_by_id = {field.field_id: field for field in registry.fields}
    manifests: list[dict[str, Any]] = []
    dates = ("2024-01-02", "2024-01-03", "2024-01-04")
    for shard_index in range(shard_count):
        rows: list[dict[str, Any]] = []
        for day_index, date in enumerate(dates):
            for minute in range(12):
                trade_time = pd.Timestamp(date) + pd.Timedelta(hours=9, minutes=30 + minute)
                for symbol_index in range(32):
                    global_symbol = shard_index * 32 + symbol_index
                    row: dict[str, Any] = {
                        "code": f"S{global_symbol:04d}",
                        "trade_time": trade_time,
                        "date": pd.Timestamp(date),
                        "close": 20.0
                        + global_symbol * 0.003
                        + minute * (0.006 if global_symbol % 3 else -0.005)
                        + day_index * (0.012 if global_symbol % 2 else -0.009),
                    }
                    for field_index, field_id in enumerate(fields, 1):
                        if field_id == "close":
                            continue
                        field = registry_by_id[field_id]
                        time_term = 0.0 if field.source_family != "raw_1min" else minute * 0.17
                        day_term = day_index * 0.29
                        cross_sectional_path_term = (
                            0.21 * np.sin((minute + 1) * ((global_symbol % 11) + 1) * 0.19)
                            if field.source_family == "raw_1min"
                            else 0.0
                        )
                        row[field_id] = (
                            1.0 if global_symbol % 2 else -1.0
                        ) if null_behavior else float(
                            np.sin((global_symbol + 1) * (field_index + 1) * 0.173 + day_term)
                            + np.cos((field_index + 2) * 0.193 + time_term)
                            + cross_sectional_path_term
                        )
                    rows.append(row)
        panel_dir = root / f"shard_{shard_index:02d}" / "phase3aq_wide_true1min" / "canary"
        panel_dir.mkdir(parents=True, exist_ok=True)
        path = panel_dir / "phase3aq_true_1min_formula_canary.parquet"
        frame = pd.DataFrame(rows)
        frame.to_parquet(path, index=False)
        manifests.append(
            {
                "shard_index": shard_index,
                "path": str(path),
                "row_count": len(frame),
                "column_count": len(frame.columns),
                "sha256": file_sha256(path),
                "date_min": dates[0],
                "date_max": dates[-1],
            }
        )
    return manifests


def _synthetic_release_hash(
    legal_manifest: list[dict[str, Any]],
    null_manifest: list[dict[str, Any]],
) -> str:
    def portable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {key: value for key, value in row.items() if key != "path"}
            for row in sorted(rows, key=lambda row: int(row["shard_index"]))
        ]

    return stable_hash(
        {
            "release_contract": "CN_MATCHED_CONTROL_SYNTHETIC_PARITY_RELEASE_V1",
            "legal_shards": portable(legal_manifest),
            "null_behavior_shards": portable(null_manifest),
        }
    )


def _run_chunk(
    *,
    candidate_table: Path,
    pair_count: int,
    shard_root: Path,
    output_root: Path,
    report_root: Path,
    split: Path,
    registry: Path,
    candidate_receipts: Path,
    pair_receipts: Path,
    data_release_hash: str,
    shard_count: int,
    expect_failure: bool = False,
) -> dict[str, Any]:
    command = [
        sys.executable,
        "app.py",
        "phase3cm-train-portfolio-sortino-reward-audit",
        "--",
        "--candidate-audit",
        str(candidate_table),
        "--shard-root",
        str(shard_root),
        "--output-root",
        str(output_root),
        "--report-root",
        str(report_root),
        "--candidate-limit",
        str(pair_count),
        "--max-shards",
        str(shard_count),
        "--sample-trade-times-per-shard",
        "36",
        "--horizons",
        "1",
        "--split-manifest",
        str(split),
        "--candidate-receipt-table",
        str(candidate_receipts),
        "--candidate-pair-receipt-table",
        str(pair_receipts),
        "--unified-registry",
        str(registry),
        "--data-release-hash",
        data_release_hash,
        "--min-obs-per-time",
        "12",
        "--cost-bps",
        "5",
        "--top-quantile",
        "0.2",
        "--write-pnl-rows",
        "--write-reward-atoms",
        "--write-semantic-sketches",
        "--semantic-sketch-size",
        "128",
        "--checkpoint-every-candidates",
        "1",
        "--checkpoint-bootstrap-iterations",
        "0",
        "--persistent-cache-mode",
        "off",
        "--fast-mode",
        "--numexpr-threads",
        "2",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    env["OMP_NUM_THREADS"] = "1"
    env["MKL_NUM_THREADS"] = "1"
    env["NUMEXPR_MAX_THREADS"] = "2"
    started = time.perf_counter()
    proc = subprocess.run(command, cwd=REPO, env=env, text=True, capture_output=True, check=False)
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "stdout.log").write_text(proc.stdout or "", encoding="utf-8")
    (output_root / "stderr.log").write_text(proc.stderr or "", encoding="utf-8")
    rejection_text = f"{proc.stdout or ''}\n{proc.stderr or ''}"
    formal_rejection_observed = any(
        token in rejection_text.lower()
        for token in (
            "control does not point to its primary",
            "matched control does not point back to primary",
            "candidate receipt authority drift",
            "fail_closed_no_matched_control_constructor",
        )
    )
    if proc.returncode and not expect_failure:
        raise RuntimeError(f"candidate-parallel parity chunk failed rc={proc.returncode}: {candidate_table}")
    if not proc.returncode and expect_failure:
        raise RuntimeError(f"invalid-control chunk was unexpectedly accepted: {candidate_table}")
    return {
        "command": command,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "candidate_table": str(candidate_table),
        "pair_count": pair_count,
        "output_root": str(output_root),
        "return_code": int(proc.returncode),
        "expected_failure": bool(expect_failure),
        "formal_rejection_observed": formal_rejection_observed,
        "rejection_excerpt": rejection_text[-1200:] if expect_failure else "",
    }


def _key(row: Mapping[str, Any], fields: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(str(row.get(field) or "") for field in fields)


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _compare_rows(
    baseline: list[dict[str, str]],
    observed: list[dict[str, str]],
    *,
    keys: tuple[str, ...],
    numeric_fields: tuple[str, ...],
    exact_fields: tuple[str, ...],
) -> dict[str, Any]:
    left = {_key(row, keys): row for row in baseline}
    right = {_key(row, keys): row for row in observed}
    missing = sorted(set(left) - set(right))
    extra = sorted(set(right) - set(left))
    max_error = 0.0
    numeric_mismatch_count = 0
    exact_mismatch_count = 0
    for identity in sorted(set(left) & set(right)):
        for field in numeric_fields:
            a = _finite(left[identity].get(field))
            b = _finite(right[identity].get(field))
            if a is None and b is None:
                continue
            if a is None or b is None:
                numeric_mismatch_count += 1
                max_error = float("inf")
                continue
            error = abs(a - b)
            max_error = max(max_error, error)
            if error > 1e-12:
                numeric_mismatch_count += 1
        for field in exact_fields:
            if str(left[identity].get(field) or "") != str(right[identity].get(field) or ""):
                exact_mismatch_count += 1
    return {
        "baseline_count": len(baseline),
        "observed_count": len(observed),
        "missing_key_count": len(missing),
        "extra_key_count": len(extra),
        "numeric_mismatch_count": numeric_mismatch_count,
        "exact_mismatch_count": exact_mismatch_count,
        "max_numeric_error": max_error,
        "passed": not missing and not extra and not numeric_mismatch_count and not exact_mismatch_count,
    }


def _merge_worker_outputs(worker_roots: list[Path]) -> dict[str, list[dict[str, str]]]:
    files = {
        "pair": "phase3cm_candidate_pair_evaluation.csv",
        "atoms": "phase3cm_reward_atoms.csv",
        "pnl": "phase3cm_portfolio_pnl_rows.csv",
        "progress": "phase3cm_candidate_progress.csv",
        "reward": "phase3cm_train_reward.csv",
    }
    return {
        name: [row for root in worker_roots for row in _read_csv(root / filename)]
        for name, filename in files.items()
    }


def _run_worker_count(
    *,
    worker_count: int,
    candidates: list[dict[str, Any]],
    run_root: Path,
    shard_root: Path,
    split: Path,
    registry: Path,
    candidate_receipts: Path,
    pair_receipts: Path,
    data_release_hash: str,
    shard_count: int,
    run_namespace: str,
) -> dict[str, Any]:
    root = run_root / run_namespace / f"workers_{worker_count}"
    chunks = partition_candidate_pairs(candidates, worker_count=worker_count)
    jobs: list[dict[str, Any]] = []
    for index, rows in enumerate(chunks, 1):
        table = root / "inputs" / f"candidate_pairs_{index:02d}.csv"
        _write_csv(table, rows)
        jobs.append(
            {
                "candidate_table": table,
                "pair_count": len(rows) // 2,
                "output_root": root / f"worker_{index:02d}",
                "report_root": root / f"worker_{index:02d}_report",
            }
        )
    summaries: list[dict[str, Any]] = []
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        futures = {
            executor.submit(
                _run_chunk,
                **job,
                shard_root=shard_root,
                split=split,
                registry=registry,
                candidate_receipts=candidate_receipts,
                pair_receipts=pair_receipts,
                data_release_hash=data_release_hash,
                shard_count=shard_count,
            ): job
            for job in jobs
        }
        for future in as_completed(futures):
            summaries.append(future.result())
    worker_roots = [job["output_root"] for job in jobs]
    merged = _merge_worker_outputs(worker_roots)
    pair_ids = [str(row.get("pair_id") or "") for row in merged["pair"]]
    return {
        "worker_count": worker_count,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "chunk_summaries": summaries,
        "full_shard_universe_per_worker": True,
        "shared_cache_writes": False,
        "pair_count": len(merged["pair"]),
        "pair_ids_unique": len(pair_ids) == len(set(pair_ids)),
        "reward_atom_count": len(merged["atoms"]),
        "merged": merged,
    }


def _run_invalid_control_worker_count(
    *,
    worker_count: int,
    candidates: list[dict[str, Any]],
    run_root: Path,
    shard_root: Path,
    split: Path,
    registry: Path,
    candidate_receipts: Path,
    pair_receipts: Path,
    data_release_hash: str,
    shard_count: int,
) -> dict[str, Any]:
    root = run_root / "invalid_controls" / f"workers_{worker_count}"
    legal_chunks = partition_candidate_pairs(candidates, worker_count=worker_count)
    jobs: list[dict[str, Any]] = []
    covered_routes: set[str] = set()
    for index, legal_rows in enumerate(legal_chunks, 1):
        invalid_rows = copy.deepcopy(legal_rows)
        for row in invalid_rows:
            covered_routes.add(str(row.get("route_id") or ""))
            if bool(row.get("is_matched_control")):
                row["matched_control_id"] = "INVALID_PRIMARY_POINTER"
        table = root / "inputs" / f"invalid_candidate_pairs_{index:02d}.csv"
        _write_csv(table, invalid_rows)
        jobs.append(
            {
                "candidate_table": table,
                "pair_count": len(invalid_rows) // 2,
                "output_root": root / f"worker_{index:02d}",
                "report_root": root / f"worker_{index:02d}_report",
            }
        )
    summaries: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        futures = {
            executor.submit(
                _run_chunk,
                **job,
                shard_root=shard_root,
                split=split,
                registry=registry,
                candidate_receipts=candidate_receipts,
                pair_receipts=pair_receipts,
                data_release_hash=data_release_hash,
                shard_count=shard_count,
                expect_failure=True,
            ): job
            for job in jobs
        }
        for future in as_completed(futures):
            summaries.append(future.result())
    return {
        "worker_count": worker_count,
        "status": "PASS"
        if len(covered_routes) == len(ROUTES)
        and all(
            summary["return_code"] != 0 and summary["formal_rejection_observed"]
            for summary in summaries
        )
        else "FAIL",
        "route_count": len(covered_routes),
        "routes": sorted(covered_routes),
        "chunk_count": len(summaries),
        "all_formal_app_invocations_rejected": all(
            summary["return_code"] != 0 for summary in summaries
        ),
        "all_rejections_have_formal_contract_evidence": all(
            summary["formal_rejection_observed"] for summary in summaries
        ),
        "chunk_summaries": sorted(summaries, key=lambda row: row["candidate_table"]),
    }


def _build_parity(runs: dict[int, dict[str, Any]]) -> dict[str, Any]:
    baseline = runs[1]["merged"]
    results: dict[str, Any] = {}
    max_error = 0.0
    for worker_count in sorted(runs):
        observed = runs[worker_count]["merged"]
        pair_compare = _compare_rows(
            baseline["pair"],
            observed["pair"],
            keys=("pair_id",),
            numeric_fields=NUMERIC_FIELDS,
            exact_fields=(
                "primary_candidate_id",
                "control_candidate_id",
                "primary_expression",
                "control_expression",
                "primary_expression_hash",
                "control_expression_hash",
                "primary_behavior_identity",
                "control_behavior_identity",
                "primary_signal_value_identity",
                "control_signal_value_identity",
                "primary_receipt_hash",
                "control_receipt_hash",
                "pair_receipt_hash",
                "pair_evaluation_status",
                "pair_evaluation_blockers",
            ),
        )
        pnl_compare = _compare_rows(
            baseline["pnl"],
            observed["pnl"],
            keys=("candidate_id", "shard_index", "trade_time", "horizon_min", "split"),
            numeric_fields=(
                "top_signal_mean",
                "bottom_signal_mean",
                "one_way_turnover",
                "trading_cost",
                "raw_return",
                "net_return",
                "rank_ic",
                "eligible_code_count",
            ),
            exact_fields=(
                "eligible_code_identity",
                "selected_code_identity",
                "portfolio_weight_identity",
                "portfolio_mode",
            ),
        )
        atom_compare = _compare_rows(
            baseline["atoms"],
            observed["atoms"],
            keys=("candidate_id", "split", "horizon_min", "trade_date"),
            numeric_fields=(
                "curve_count",
                "net_return_sum",
                "raw_return_sum",
                "turnover_sum",
                "rank_ic_sum",
                "rank_ic_count",
            ),
            exact_fields=("expression_hash",),
        )
        progress_compare = _compare_rows(
            baseline["progress"],
            observed["progress"],
            keys=("candidate_id", "shard_index"),
            numeric_fields=("signal_finite_count", "signal_nonzero_count", "signal_std"),
            exact_fields=("signal_rank_sketch", "signal_value_sketch", "signal_missingness_sketch"),
        )
        numeric_errors = [
            value
            for value in (
                pair_compare["max_numeric_error"],
                pnl_compare["max_numeric_error"],
                atom_compare["max_numeric_error"],
                progress_compare["max_numeric_error"],
            )
            if math.isfinite(float(value))
        ]
        local_max = max(numeric_errors, default=0.0)
        max_error = max(max_error, local_max)
        results[str(worker_count)] = {
            "elapsed_seconds": runs[worker_count]["elapsed_seconds"],
            "pair_count": runs[worker_count]["pair_count"],
            "pair_ids_unique": runs[worker_count]["pair_ids_unique"],
            "reward_atom_count": runs[worker_count]["reward_atom_count"],
            "pair": pair_compare,
            "pnl_signal_weight_cost": pnl_compare,
            "reward_atoms": atom_compare,
            "signal_sketches": progress_compare,
        }
    baseline_atom_count = runs[1]["reward_atom_count"]
    passed = all(
        result["pair_ids_unique"]
        and result["reward_atom_count"] == baseline_atom_count
        and result["pair"]["passed"]
        and result["pnl_signal_weight_cost"]["passed"]
        and result["reward_atoms"]["passed"]
        and result["signal_sketches"]["passed"]
        for result in results.values()
    )
    return {
        "status": "PASS" if passed and max_error <= 1e-12 else "FAIL",
        "worker_counts": results,
        "max_numeric_error": max_error,
        "tolerance": 1e-12,
        "reward_atom_count_worker_invariant": all(
            run["reward_atom_count"] == baseline_atom_count for run in runs.values()
        ),
        "pair_evaluated_exactly_once": all(run["pair_ids_unique"] for run in runs.values()),
        "primary_control_never_split": True,
        "full_shard_universe_per_worker": True,
        "shared_cache_writes": False,
    }


def _artifact_index(report_root: Path, producer: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for path in sorted(report_root.iterdir()):
        if not path.is_file():
            continue
        output.append(
            {
                "path": str(path),
                "sha256": file_sha256(path),
                "size_bytes": path.stat().st_size,
                "producer": producer,
                "stage": "final",
            }
        )
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--split-manifest", type=Path, default=DEFAULT_SPLIT)
    parser.add_argument("--data-release-hash", default=DATA_RELEASE_HASH)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--shard-count", type=int, default=4)
    parser.add_argument("--worker-counts", default="1,2,4")
    args = parser.parse_args(argv)

    report_root = args.report_root.resolve()
    run_root = args.run_root.resolve()
    report_root.mkdir(parents=True, exist_ok=True)
    run_root.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    split_authority = FixedSplitAuthority.read(args.split_manifest.resolve(), require_official=True)
    candidates = _build_candidates(registry)
    candidate_table = run_root / "candidate_pairs.csv"
    _write_csv(candidate_table, candidates)
    shard_root = run_root / "synthetic_true1min_shards"
    shard_manifest = _materialize_synthetic_shards(
        root=shard_root,
        candidates=candidates,
        registry=registry,
        shard_count=max(1, int(args.shard_count)),
    )
    null_shard_root = run_root / "null_behavior_synthetic_true1min_shards"
    null_shard_manifest = _materialize_synthetic_shards(
        root=null_shard_root,
        candidates=candidates,
        registry=registry,
        shard_count=max(1, int(args.shard_count)),
        null_behavior=True,
    )
    parity_data_release_hash = _synthetic_release_hash(shard_manifest, null_shard_manifest)
    context = ReceiptContext.build(
        registry=registry,
        split_authority=split_authority,
        data_release_hash=parity_data_release_hash,
        evaluator_paths=[EVALUATOR],
    )
    candidate_receipts = CandidateSubmissionAuthority(registry, context).authorize_table(candidates)
    pair_receipts = CandidatePairAuthority().authorize_table(candidates, candidate_receipts)
    negative_cases = _exercise_negative_pair_cases(
        candidates=candidates,
        candidate_receipts=candidate_receipts,
        pair_receipts=pair_receipts,
    )
    candidate_receipt_path = run_root / "candidate_receipts.jsonl"
    pair_receipt_path = run_root / "pair_receipts.jsonl"
    write_receipt_table(candidate_receipt_path, candidate_receipts)
    write_pair_receipt_table(pair_receipt_path, pair_receipts)
    worker_counts = tuple(sorted({int(value) for value in args.worker_counts.split(",") if value.strip()}))
    if worker_counts != (1, 2, 4):
        raise ValueError("formal parity requires worker counts exactly 1,2,4")
    runs = {
        worker_count: _run_worker_count(
            worker_count=worker_count,
            candidates=candidates,
            run_root=run_root,
            shard_root=shard_root,
            split=args.split_manifest.resolve(),
            registry=args.registry.resolve(),
            candidate_receipts=candidate_receipt_path,
            pair_receipts=pair_receipt_path,
            data_release_hash=parity_data_release_hash,
            shard_count=max(1, int(args.shard_count)),
            run_namespace="legal_pairs",
        )
        for worker_count in worker_counts
    }
    parity = _build_parity(runs)
    null_runs = {
        worker_count: _run_worker_count(
            worker_count=worker_count,
            candidates=candidates,
            run_root=run_root,
            shard_root=null_shard_root,
            split=args.split_manifest.resolve(),
            registry=args.registry.resolve(),
            candidate_receipts=candidate_receipt_path,
            pair_receipts=pair_receipt_path,
            data_release_hash=parity_data_release_hash,
            shard_count=max(1, int(args.shard_count)),
            run_namespace="null_behavior_pairs",
        )
        for worker_count in worker_counts
    }
    null_parity = _build_parity(null_runs)
    null_formal_pass = (
        null_parity["status"] == "PASS"
        and all(
            len(run["merged"]["pair"]) == len(ROUTES)
            and all(
                str(row.get("pair_evaluation_status") or "") == "PAIR_EVALUATION_BLOCKED"
                and "control_behavior_identity_equals_primary"
                in str(row.get("pair_evaluation_blockers") or "")
                and int(float(row.get("control_evaluator_invocation_count") or 0)) > 0
                for row in run["merged"]["pair"]
            )
            for run in null_runs.values()
        )
    )
    invalid_runs = {
        worker_count: _run_invalid_control_worker_count(
            worker_count=worker_count,
            candidates=candidates,
            run_root=run_root,
            shard_root=shard_root,
            split=args.split_manifest.resolve(),
            registry=args.registry.resolve(),
            candidate_receipts=candidate_receipt_path,
            pair_receipts=pair_receipt_path,
            data_release_hash=parity_data_release_hash,
            shard_count=max(1, int(args.shard_count)),
        )
        for worker_count in worker_counts
    }
    invalid_formal_pass = all(run["status"] == "PASS" for run in invalid_runs.values())
    formal_negative_parity = {
        "status": "PASS" if null_formal_pass and invalid_formal_pass else "FAIL",
        "null_behavior": {
            "status": "PASS" if null_formal_pass else "FAIL",
            "parity": null_parity,
            "pair_rows_by_worker_count": {
                str(worker_count): run["merged"]["pair"]
                for worker_count, run in null_runs.items()
            },
        },
        "invalid_control": {
            "status": "PASS" if invalid_formal_pass else "FAIL",
            "worker_counts": {str(key): value for key, value in invalid_runs.items()},
        },
    }

    source = FORMAL_CP.read_text(encoding="utf-8")
    removal = {
        "status": "PASS",
        "formal_module": str(FORMAL_CP),
        "formal_module_sha256": file_sha256(FORMAL_CP),
        "deprecated_state": "DEPRECATED_DIAGNOSTIC_ONLY",
        "formal_shard_parallel_entry_absent": "_run_real_cm_shard_subprocess" not in source
        and "_run_real_cm_parallel_by_shard" not in source,
        "mean_of_shard_reward_fallback_absent": "shard_chunk_reward_fallback" not in source
        and "mean_of_shard_chunk_reward_rows" not in source,
        "formal_cli_candidate_only": 'choices=("candidate",)' in source,
        "phase3cn_matched_source_required": True,
        "global_cross_section_merge_implemented": False,
    }
    removal["status"] = (
        "PASS"
        if all(
            removal[key]
            for key in (
                "formal_shard_parallel_entry_absent",
                "mean_of_shard_reward_fallback_absent",
                "formal_cli_candidate_only",
            )
        )
        else "FAIL"
    )
    contract = {
        "contract_id": "CN_MATCHED_CONTROL_CONTRACT_V1",
        "authority_status": PAIR_AUTHORIZATION_STATUS,
        "formal_object": "candidate_pair=primary+matched_control",
        "candidate_limit_unit": "primary_pair_count",
        "mapping_portfolio_contract": PAIR_MAPPING_PORTFOLIO_CONTRACT,
        "primary_controls_selection": True,
        "control_independent_vote": False,
        "control_independent_quota": False,
        "control_survivor_eligible": False,
        "control_memory_eligible": False,
        "feedback_metric": "matched_train_increment",
        "feedback_split": "train",
        "forbidden_data_roles": ["validation", "holdout", "sealed", "forward_2026"],
        "route_constructor_count": len(CONTROL_CONSTRUCTOR_MATRIX),
        "negative_case_matrix_status": negative_cases["status"],
        "formal_negative_1_2_4_parity_status": formal_negative_parity["status"],
    }
    receipt_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": PAIR_RECEIPT_SCHEMA_VERSION,
        "type": "object",
        "required": [
            "pair_id",
            "primary_candidate_id",
            "control_candidate_id",
            "primary_receipt_hash",
            "control_receipt_hash",
            "pair_receipt_hash",
            "route_id",
            "support_unit",
            "pair_clock_alignment_policy",
            "primary_observable_time_contract",
            "control_observable_time_contract",
            "primary_pit_source_lag_contract",
            "control_pit_source_lag_contract",
            "mapping_portfolio_contract",
            "split_manifest_hash",
            "data_release_hash",
            "evaluator_code_hash",
            "pair_authority_code_hash",
            "control_constructor_id",
        ],
        "additionalProperties": True,
    }
    acceleration = {
        "python_executable": sys.executable,
        "package_matrix": _package_matrix(),
        "hot_path": "pyarrow pruned shard reads + expression/subtree cache + numba group rank + numpy portfolio loop",
        "runtime_worker_counts": list(worker_counts),
        "safe_concurrency_limit_for_parity": 4,
        "full_shard_universe_per_worker": True,
        "parallel_axis": "candidate_pair",
        "shared_cache_writes": False,
        "persistent_cache_mode": "off",
        "semantic_change": "tie-aware quantile cutoffs for coarse controls; covered by serial/parallel parity",
        "successive_halving": False,
        "use_fast_context": "Phase3CM --fast-mode",
        "global_worker_limit": 4,
    }

    _write_json(report_root / "CN_MATCHED_CONTROL_CONTRACT.json", contract)
    matrix_rows = [{"route_id": route_id, **row} for route_id, row in CONTROL_CONSTRUCTOR_MATRIX.items()]
    _write_csv(report_root / "CN_ROUTE_CONTROL_CONSTRUCTOR_MATRIX.csv", matrix_rows)
    _write_json(report_root / "CN_CANDIDATE_PAIR_RECEIPT_SCHEMA.json", receipt_schema)
    _write_json(
        report_root / "CN_PAIR_EVALUATION_RESULTS.json",
        {
            "status": "PASS"
            if all(str(row.get("pair_evaluation_status") or "") == "PAIR_EVALUATED" for row in runs[1]["merged"]["pair"])
            and negative_cases["status"] == "PASS"
            and formal_negative_parity["status"] == "PASS"
            else "FAIL",
            "legal_pair_evaluations": runs[1]["merged"]["pair"],
            "negative_case_status": negative_cases["status"],
            "null_pair_cases": negative_cases["null_pair_cases"],
            "invalid_control_cases": negative_cases["invalid_control_cases"],
            "formal_negative_1_2_4_parity": formal_negative_parity,
        },
    )
    _write_json(report_root / "CN_CANDIDATE_PARALLEL_PAIR_PARITY.json", parity)
    _write_json(report_root / "CN_SHARD_PARALLEL_FORMAL_REMOVAL_AUDIT.json", removal)
    _write_json(report_root / "CN_BACKTEST_ACCELERATION_AUDIT.json", acceleration)

    qualified = (
        parity["status"] == "PASS"
        and removal["status"] == "PASS"
        and negative_cases["status"] == "PASS"
        and formal_negative_parity["status"] == "PASS"
        and all(
            str(row.get("pair_evaluation_status") or "") == "PAIR_EVALUATED"
            for row in runs[1]["merged"]["pair"]
        )
        and len(runs[1]["merged"]["pair"]) == len(ROUTES)
    )
    status = (
        "CN_MATCHED_CONTROL_AND_CANDIDATE_PARALLEL_QUALIFIED"
        if qualified
        else "CN_MATCHED_CONTROL_AND_CANDIDATE_PARALLEL_PARTIALLY_QUALIFIED"
    )
    report = f"""# CN Matched-Control and Candidate-Parallel Formalization

Status: `{status}`

## Outcome

- route-specific control constructors: `{len(CONTROL_CONSTRUCTOR_MATRIX)}`
- evaluated primary/control pairs: `{len(runs[1]['merged']['pair'])}`
- null-pair fail-closed cases: `{len(negative_cases['null_pair_cases'])}`
- invalid-control fail-closed cases: `{len(negative_cases['invalid_control_cases'])}`
- null/invalid formal 1/2/4 parity: `{formal_negative_parity['status']}`
- candidate workers checked: `1 / 2 / 4`
- maximum numerical error: `{parity['max_numeric_error']}`
- reward atoms per worker configuration: `{runs[1]['reward_atom_count']}`
- shard-parallel formal entry: `REMOVED`
- mean-of-shard reward fallback: `FORBIDDEN_FROM_FEEDBACK`
- validation / holdout / 2026 reads: `0` (synthetic development-only coordinates)

No formal search, candidate promotion, forward opening, or cross-sprint memory update was performed.
"""
    (report_root / "CN_MATCHED_CONTROL_AND_CANDIDATE_PARALLEL_REPORT.md").write_text(report, encoding="utf-8")
    manifest = {
        "experiment_id": "20260715_cn_matched_control_candidate_parallel_001",
        "objective": "prove route-specific matched evaluation and candidate-pair parallel parity",
        "status": "completed" if qualified else "partial",
        "mode": "research_synthetic_development_only",
        "repo_sha": args.repo_sha,
        "python_executable": sys.executable,
        "inputs": {
            "registry": str(args.registry.resolve()),
            "registry_sha256": file_sha256(args.registry.resolve()),
            "split_manifest": str(args.split_manifest.resolve()),
            "split_manifest_sha256": file_sha256(args.split_manifest.resolve()),
            "source_reference_data_release_hash": args.data_release_hash,
            "parity_data_release_hash": parity_data_release_hash,
            "candidate_receipt_table_sha256": file_sha256(candidate_receipt_path),
            "pair_receipt_table_sha256": file_sha256(pair_receipt_path),
            "synthetic_shards": shard_manifest,
            "null_behavior_synthetic_shards": null_shard_manifest,
        },
        "parameters": {
            "routes": list(ROUTES),
            "seeds": {route_id: 5100 + index for index, route_id in enumerate(ROUTES, 1)},
            "worker_counts": list(worker_counts),
            "shard_count": args.shard_count,
            "horizon": 1,
            "cost_bps": 5,
            "top_quantile": 0.2,
            "cache_mode": "off",
            "negative_case_routes": list(ROUTES),
            "negative_formal_worker_counts": list(worker_counts),
        },
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "actual_elapsed_seconds": round(time.perf_counter() - started, 6),
        "reproducible": parity["status"] == "PASS",
        "continuation": "formal search remains frozen; use only matched pair train feedback",
        "failure": "" if qualified else "inspect parity or blocked pair artifacts",
        "decision": "N/A_NO_PERFORMANCE_SEARCH",
    }
    _write_json(report_root / "CN_MATCHED_CONTROL_RUN_MANIFEST.json", manifest)
    index = _artifact_index(report_root, str(Path(__file__).resolve()))
    _write_json(report_root / "CN_MATCHED_CONTROL_ARTIFACT_INDEX.json", index)
    print(json.dumps({"status": status, "parity": parity["status"], "report_root": str(report_root)}))
    return 0 if qualified else 2


if __name__ == "__main__":
    raise SystemExit(main())
