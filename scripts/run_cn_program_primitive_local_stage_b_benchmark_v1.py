"""Prospective Stage-B benchmark for transferring primitive-local search credit."""
from __future__ import annotations

import argparse
import json
import statistics
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_disclosure_timing_mechanism_successor_v1 as mechanism
from scripts import run_cn_program_optimizer_large_fresh_v1 as large_fresh
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as successor
from our_system_phase2.services.unified_capability_registry import stable_hash

CHECKPOINT_SIZE = 24
PRIMARY_EXECUTOR_WORKERS = 24
RESOURCE_CANARY_PROBE_SECONDS = 30.0
POLICY_ID = "PRIMITIVE_LOCAL_BETA_TRANSFER_V1"
FAMILY_POLICY_ID = "FAMILY_ONLY_BACKOFF_V1"
UNIFORM_POLICY_ID = "UNIFORM_HASH_V1"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def verify_policy(path: Path) -> dict[str, Any]:
    payload = _read(path)
    body = dict(payload)
    claimed = str(body.pop("policy_payload_sha256", ""))
    if claimed != stable_hash(body):
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_POLICY_HASH_DRIFT")
    if payload.get("status") != "PRIMITIVE_LOCAL_STAGE_B_POLICY_FROZEN_BEFORE_STAGE_B_READ":
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_POLICY_STATUS_DRIFT")
    if payload.get("stage_b_financial_labels_read_during_policy_freeze") is not False:
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_POLICY_LABEL_LEAK")
    spent = dict(payload["spent_freeze"])
    if int(spent["combined_spent_exact_count"]) != 2438 or int(spent["stage_b_overlap_count"]) != 0:
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_SPENT_DRIFT")
    policies = dict(payload["policies"])
    primitive = dict(policies[POLICY_ID])["orders_by_temporal"]
    family = dict(policies[FAMILY_POLICY_ID])["orders_by_temporal"]
    uniforms = dict(policies[UNIFORM_POLICY_ID])["orders_by_seed_and_temporal"]
    if len(primitive) != 6 or len(family) != 6 or len(uniforms) != 8:
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_ORDER_COVERAGE_DRIFT")
    expected = set()
    for temporal, order in primitive.items():
        if len(order) != 44 or len(set(order)) != 44:
            raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_PRIMITIVE_ORDER_DRIFT")
        expected.update(map(str, order))
        if set(map(str, family[temporal])) != set(map(str, order)):
            raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_FAMILY_ORDER_DRIFT")
        for seed_orders in uniforms.values():
            if set(map(str, seed_orders[temporal])) != set(map(str, order)):
                raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_UNIFORM_ORDER_DRIFT")
    if len(expected) != 264:
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_EXACT_COVERAGE_DRIFT")
    return payload


def _result_metric(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(rows)
    n = len(rows)
    productive = sum(mechanism._productive(row) for row in rows)
    admitted = sum(bool(dict(row.get("admission") or {}).get("admitted")) for row in rows)
    credits = [
        dict(dict(row.get("uplift") or {}).get("program_credit") or {})
        for row in rows
        if row.get("uplift") is not None
    ]
    returns = [float(row["matched_cumulative_net_return_increment"]) for row in credits if row.get("matched_cumulative_net_return_increment") is not None]
    rewards = [float(row["matched_net_reward_increment"]) for row in credits if row.get("matched_net_reward_increment") is not None]
    return {
        "evaluated": n,
        "admitted": admitted,
        "productive": productive,
        "productive_rate": productive / n if n else 0.0,
        "median_matched_cumulative_net_return_increment": statistics.median(returns) if returns else None,
        "median_matched_net_reward_increment": statistics.median(rewards) if rewards else None,
    }


def _select(order_by_temporal: Mapping[str, Sequence[str]], k: int, result_by_exact: Mapping[str, Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    selected = []
    per_temporal = {}
    for temporal, order in sorted(order_by_temporal.items()):
        rows = [dict(result_by_exact[str(exact)]) for exact in list(order)[: int(k)]]
        selected.extend(rows)
        per_temporal[str(temporal)] = _result_metric(rows)
    return selected, per_temporal


def _policy_curves(policy: Mapping[str, Any], result_by_exact: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    evaluation = dict(policy["evaluation"])
    budgets = list(map(int, evaluation["budgets_per_temporal"]))
    policies = dict(policy["policies"])
    primitive_orders = dict(policies[POLICY_ID]["orders_by_temporal"])
    family_orders = dict(policies[FAMILY_POLICY_ID]["orders_by_temporal"])
    uniform_orders = dict(policies[UNIFORM_POLICY_ID]["orders_by_seed_and_temporal"])
    output: dict[str, Any] = {"budgets": {}}
    for k in budgets:
        primitive_rows, primitive_temporal = _select(primitive_orders, k, result_by_exact)
        family_rows, family_temporal = _select(family_orders, k, result_by_exact)
        uniform_seed_metrics = {}
        uniform_seed_temporal = {}
        for seed, seed_orders in sorted(uniform_orders.items()):
            rows, per_temporal = _select(seed_orders, k, result_by_exact)
            uniform_seed_metrics[str(seed)] = _result_metric(rows)
            uniform_seed_temporal[str(seed)] = per_temporal
        uniform_counts = [float(row["productive"]) for row in uniform_seed_metrics.values()]
        uniform_rates = [float(row["productive_rate"]) for row in uniform_seed_metrics.values()]
        temporal_wins = 0
        for temporal in primitive_temporal:
            primitive_count = float(primitive_temporal[temporal]["productive"])
            baseline_mean = statistics.mean(
                float(uniform_seed_temporal[seed][temporal]["productive"])
                for seed in uniform_seed_temporal
            )
            if primitive_count > baseline_mean:
                temporal_wins += 1
        primitive_metric = _result_metric(primitive_rows)
        family_metric = _result_metric(family_rows)
        uniform_mean_count = statistics.mean(uniform_counts)
        output["budgets"][str(6 * k)] = {
            "per_temporal_budget": k,
            "primitive_local": primitive_metric,
            "family_only": family_metric,
            "uniform_seed_metrics": uniform_seed_metrics,
            "uniform_seed_mean_productive_count": uniform_mean_count,
            "uniform_seed_mean_productive_rate": statistics.mean(uniform_rates),
            "primitive_vs_uniform_absolute_productive_delta": float(primitive_metric["productive"]) - uniform_mean_count,
            "primitive_vs_uniform_relative_ratio": (
                float(primitive_metric["productive"]) / uniform_mean_count
                if uniform_mean_count > 0 else None
            ),
            "primitive_temporal_strata_won_vs_uniform_seed_mean": temporal_wins,
            "primitive_per_temporal": primitive_temporal,
            "family_per_temporal": family_temporal,
        }
    gate = dict(evaluation["primary_system_search_victory_gate"])
    primary = output["budgets"][str(int(evaluation["primary_total_budget"]))]
    budget_144 = output["budgets"]["144"]
    checks = {
        "primary_relative_ratio": float(primary["primitive_vs_uniform_relative_ratio"] or 0.0)
        >= float(gate["primitive_local_productive_count_vs_uniform_seed_mean_minimum_relative_ratio"]),
        "primary_absolute_delta": float(primary["primitive_vs_uniform_absolute_productive_delta"])
        >= float(gate["primitive_local_productive_count_vs_uniform_seed_mean_minimum_absolute_delta"]),
        "temporal_strata_wins": int(primary["primitive_temporal_strata_won_vs_uniform_seed_mean"])
        >= int(gate["minimum_temporal_strata_won_vs_uniform_seed_mean"]),
        "budget_144_relative_ratio": float(budget_144["primitive_vs_uniform_relative_ratio"] or 0.0)
        >= float(gate["budget_144_minimum_relative_ratio_vs_uniform_seed_mean"]),
    }
    output["victory_checks"] = checks
    output["status"] = (
        "PRIMITIVE_LOCAL_SEARCH_TRANSFER_PASS_STAGE_C_LARGE_SCALE_AUTHORIZATION_ELIGIBLE"
        if all(checks.values())
        else "PRIMITIVE_LOCAL_SEARCH_TRANSFER_FAIL_NO_LARGE_SCALE_CLAIM"
    )
    return output


def _close_checkpoint(inflight: Path, closed: Path, previous_manifest_sha: str, checkpoint_ordinal: int) -> str:
    artifacts = [successor._artifact(path, inflight) for path in sorted(inflight.rglob("*")) if path.is_file() and path.name != "checkpoint_manifest.json"]
    manifest = engine._self_hashed({
        "schema_version": "cn_program_primitive_local_stage_b_checkpoint_manifest_v1",
        "status": "PRIMITIVE_LOCAL_STAGE_B_CHECKPOINT_CLOSED_IMMUTABLE",
        "checkpoint_ordinal": int(checkpoint_ordinal),
        "previous_checkpoint_manifest_file_sha256": str(previous_manifest_sha),
        "artifacts": artifacts,
    }, "manifest_payload_sha256")
    engine._write_json(inflight / "checkpoint_manifest.json", manifest)
    if closed.exists():
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_CHECKPOINT_ALREADY_EXISTS")
    inflight.replace(closed)
    return engine._sha256(closed / "checkpoint_manifest.json")


def run(args: argparse.Namespace, *, admission: Mapping[str, Any], authorization: Mapping[str, Any]) -> dict[str, Any]:
    root = args.output_root.resolve()
    if not root.is_dir() or not (root / ".project_control_execution").is_dir():
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_ADMITTED_ROOT_MISSING")
    if {path.name for path in root.iterdir()} != {".project_control_execution"}:
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_ADMITTED_ROOT_NOT_CLEAN")
    policy = verify_policy(args.search_policy)
    prefreeze = mechanism.verify_prefreeze(args.mechanism_prefreeze)
    stage_b = [dict(row) for row in prefreeze["candidates"]["stage_b"]]
    stage_b_ids = [str(row["exact_identity"]) for row in stage_b]
    spent_ids = set(map(str, policy["spent_freeze"]["combined_spent_exact_identities"]))
    if len(stage_b_ids) != 264 or len(set(stage_b_ids)) != 264 or set(stage_b_ids).intersection(spent_ids):
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_FRESHNESS_DRIFT")

    authority = mechanism._load_authority(args, authorization=authorization, repo_sha=str(admission["repo_sha"]))
    schedules = [
        mechanism._fixed_schedule(
            candidate,
            authority=authority,
            global_ordinal=index,
            checkpoint_ordinal=index // CHECKPOINT_SIZE,
        )
        for index, candidate in enumerate(stage_b)
    ]
    if [str(row["mechanism_exact_identity"]) for row in schedules] != stage_b_ids:
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_SCHEDULE_EXACT_DRIFT")

    engine._write_json(root / "input_binding.json", authority["input_binding"])
    engine._write_json(root / "policy_binding.json", {
        "schema_version": "cn_program_primitive_local_stage_b_policy_binding_v1",
        "policy_payload_sha256": str(policy["policy_payload_sha256"]),
        "combined_spent_exact_count": 2438,
        "stage_b_exact_count": 264,
        "stage_b_financial_labels_read_during_policy_freeze": False,
        "optimizer_selection_used": False,
    })
    input_hash = str(authority["input_binding"]["input_binding_sha256"])
    fields = tuple(map(str, prefreeze["resource_preview"]["union_field_columns"]))
    old_probe = large_fresh.RESOURCE_CANARY_PROBE_SECONDS
    try:
        large_fresh.RESOURCE_CANARY_PROBE_SECONDS = RESOURCE_CANARY_PROBE_SECONDS
        canary = large_fresh._resource_canary(authority, input_hash, PRIMARY_EXECUTOR_WORKERS, fields)
    finally:
        large_fresh.RESOURCE_CANARY_PROBE_SECONDS = old_probe
    if (
        canary.get("status") != "PASS_ZERO_CANDIDATE_EVALUATION_RESOURCE_CANARY"
        or int(canary.get("requested_workers") or 0) != PRIMARY_EXECUTOR_WORKERS
        or int(canary.get("field_column_count") or 0) != len(fields)
        or int(canary.get("pagefile_pages_in_delta_bytes", -1)) != 0
        or int(canary.get("pagefile_pages_out_delta_bytes", -1)) != 0
        or canary.get("candidate_evaluation_executed") is not False
    ):
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_RESOURCE_CANARY_FAIL")
    engine._write_json(root / "resource_canary.json", canary)

    started = time.perf_counter()
    previous_manifest = "GENESIS"
    all_results: list[dict[str, Any]] = []
    for checkpoint in range(11):
        batch = schedules[checkpoint * CHECKPOINT_SIZE : (checkpoint + 1) * CHECKPOINT_SIZE]
        inflight = root / f"checkpoint_{checkpoint:04d}.inflight"
        closed = root / f"checkpoint_{checkpoint:04d}"
        inflight.mkdir(parents=False, exist_ok=False)
        engine._write_jsonl(inflight / "selected_schedule.jsonl", batch)
        records = successor._evaluate_schedules(
            batch,
            record_root=inflight / "records",
            authority=authority,
            input_hash=input_hash,
            executor_workers=PRIMARY_EXECUTOR_WORKERS,
        )
        by_ordinal = {int(row["main_record_ordinal"]): row for row in batch}
        results = [
            mechanism._result_record(record, by_ordinal[int(record["main_record_ordinal"])])
            for record in records
        ]
        engine._write_jsonl(inflight / "candidate_results.jsonl", results)
        previous_manifest = _close_checkpoint(inflight, closed, previous_manifest, checkpoint)
        all_results.extend(results)

    result_by_exact = {str(row["exact_identity"]): row for row in all_results}
    if len(all_results) != 264 or len(result_by_exact) != 264:
        raise RuntimeError("PRIMITIVE_LOCAL_STAGE_B_RESULT_COVERAGE_DRIFT")
    benchmark = _policy_curves(policy, result_by_exact)
    engine._write_json(root / "search_policy_benchmark.json", benchmark)
    closure = engine._self_hashed({
        "schema_version": "cn_program_primitive_local_stage_b_benchmark_complete_v1",
        "status": "PRIMITIVE_LOCAL_STAGE_B_BENCHMARK_COMPLETE",
        "repo_sha": str(admission["repo_sha"]),
        "authorization_payload_sha256": str(authorization["authorization_payload_sha256"]),
        "policy_payload_sha256": str(policy["policy_payload_sha256"]),
        "stage_b_evaluated": 264,
        "checkpoint_count": 11,
        "last_checkpoint_manifest_file_sha256": previous_manifest,
        "search_transfer_status": str(benchmark["status"]),
        "search_policy_benchmark": benchmark,
        "full_stage_b_metric": _result_metric(all_results),
        "wall_seconds": time.perf_counter() - started,
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
        "validation_feedback_used": False,
        "promotion_authorized": False,
        "oos_authority": "NONE",
        "stage_c_execution_authorized": False,
    }, "closure_payload_sha256")
    engine._write_json(root / "CN_PROGRAM_PRIMITIVE_LOCAL_STAGE_B_BENCHMARK_COMPLETE.json", closure)
    return closure


__all__ = ["verify_policy", "_policy_curves", "run"]
