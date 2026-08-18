"""Execute the frozen large-fresh Hybrid-TPE Program development campaign."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import json
import os
from pathlib import Path
import time
from typing import Any, Mapping, Sequence

import psutil

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as shared
from scripts import run_cn_program_optimizer_tournament_v1 as tournament
from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import (
    CAMPAIGN_ID,
    CAMPAIGN_PROFILE,
    CHECKPOINT_BATCH_SIZE,
    ENHANCED_TEMPLATES,
    MINIMUM_MACROS_BEFORE_VALUE_COLLAPSE_TEST,
    NEW_BEHAVIOR_PAIR_RATE_COLLAPSE_MAX,
    MINIMUM_PAIRS_PER_HOUR_AFTER_FIRST_MACRO,
    PRIMARY_EXECUTOR_WORKERS,
    PRIOR_EXACT_COUNT,
    PRIOR_EXACT_IDENTITIES_SHA256,
    PRIOR_FREEZE_PAYLOAD_SHA256,
    PRODUCTIVE_EFFICIENCY_COLLAPSE_MAX,
    RESOURCE_FALLBACK_EXECUTOR_WORKERS,
    RESOURCE_PROFILE,
    VALUE_COLLAPSE_LOOKBACK_MACROS,
)
from our_system_phase2.runtime.cn_program_optimizer_tournament_v1 import (
    SEEDS,
    SURROGATE_CONFIG,
    TPE_CONFIG,
)
from our_system_phase2.services.program_optimizer_tournament_v1 import (
    ProgramOptimizerTournamentV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    PROGRAM_OPTIMIZER_ARMS,
    UNIFORM_CONTROL,
)
from our_system_phase2.services.search_v2_conditional_uplift import (
    MATCHED_CONTROL_CONTRACT_ID,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS_COMPLETE = "LARGE_FRESH_DEVELOPMENT_SEARCH_COMPLETE"
CLOSURE_NAME = "CN_PROGRAM_OPTIMIZER_LARGE_FRESH_DEVELOPMENT_COMPLETE.json"
FORMAL_OPTIMIZER_ARM = HYBRID_TPE_PROGRAM
FORMAL_SEARCH_AUTHORITY = "HYBRID_TPE_AVAILABILITY"
CLOSURE_SCHEMA_VERSION = "cn_program_optimizer_large_fresh_development_complete_v1"
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    engine._write_json(path, dict(payload))


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    engine._write_jsonl(path, [dict(row) for row in rows])


def _selection_state_record(state: Mapping[str, Any]) -> dict[str, Any]:
    base_counts: dict[str, dict[str, int]] = {}
    for (template, base_id), count in state["base_counts"].items():
        base_counts.setdefault(str(template), {})[str(base_id)] = int(count)
    return {
        "program_ids": sorted(map(str, state["program_ids"])),
        "reservoir_ids": sorted(map(str, state["reservoir_ids"])),
        "component_ids": sorted(map(str, state["component_ids"])),
        "combination_ids": sorted(map(str, state["combination_ids"])),
        "base_counts": {
            key: dict(sorted(value.items()))
            for key, value in sorted(base_counts.items())
        },
    }


def _checkpoint_arm(macro_index: int, template_index: int) -> str:
    return (
        UNIFORM_CONTROL
        if template_index == macro_index % len(ENHANCED_TEMPLATES)
        else FORMAL_OPTIMIZER_ARM
    )


def _ask_rows(*, macro_index: int, template_index: int, checkpoint_ordinal: int,
              start_ordinal: int, arm: str) -> list[dict[str, Any]]:
    template = ENHANCED_TEMPLATES[template_index]
    output = []
    for local in range(CHECKPOINT_BATCH_SIZE):
        row = {
            "schema_version": "cn_program_optimizer_large_fresh_ask_v1",
            "macro_index": int(macro_index),
            "optimizer_arm": str(arm),
            "generation_arm": str(arm),
            "template_id": str(template),
            "template_record_ordinal": int(macro_index * CHECKPOINT_BATCH_SIZE + local),
            "main_record_ordinal": int(start_ordinal + local),
            "checkpoint_ordinal": int(checkpoint_ordinal),
            "campaign_profile": CAMPAIGN_PROFILE,
            "absolute_admission_head_eligible": True,
            "conditional_uplift_head_eligible": True,
            "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
            "program_level_credit_only": True,
            "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
        }
        row["ask_record_sha256"] = stable_hash(row)
        output.append(row)
    return output


def _resource_probe(delay: float) -> dict[str, int]:
    time.sleep(float(delay))
    process = psutil.Process()
    return {
        "pid": os.getpid(),
        "rss_bytes": int(process.memory_info().rss),
        "available_memory_bytes": int(psutil.virtual_memory().available),
    }


def _resource_canary(authority: Mapping[str, Any], input_hash: str, workers: int) -> dict[str, Any]:
    before_children = {child.pid for child in psutil.Process().children(recursive=True)}
    before = engine._runtime_resource_snapshot()
    engine._require_runtime_resource_safety(before)
    initargs = (
        str(authority["execution_contract_path"]),
        str(authority["train_field_root"]),
        str(authority["train_price_root"]),
        authority["price_manifest"],
        str(authority["price_manifest_path"]),
        str(authority["registry_path"]),
        str(input_hash),
        authority["windows"],
        authority["field_manifest_file_sha"],
        authority["field_manifest_payload_sha"],
        None,
    )
    results: list[dict[str, int]] = []
    started = time.perf_counter()
    with ProcessPoolExecutor(
        max_workers=int(workers),
        initializer=engine._initialize_worker,
        initargs=initargs,
    ) as executor:
        futures = [executor.submit(_resource_probe, 1.0) for _ in range(int(workers) * 2)]
        results = [future.result() for future in futures]
    after = engine._runtime_resource_snapshot()
    engine._require_runtime_resource_safety(after)
    orphans = engine._new_child_process_ids(before_children)
    if orphans:
        raise RuntimeError(
            "LARGE_FRESH_RESOURCE_CANARY_LEFT_ORPHANS:" + ",".join(map(str, orphans))
        )
    pids = sorted({int(row["pid"]) for row in results})
    minimum_free = min(
        [int(row["available_memory_bytes"]) for row in results]
        + [int(after["available_physical_bytes"])]
    )
    if len(pids) != int(workers):
        raise RuntimeError(
            f"LARGE_FRESH_RESOURCE_CANARY_WORKER_CARDINALITY:{len(pids)}!={workers}"
        )
    if minimum_free < MINIMUM_FREE_MEMORY_BYTES:
        raise RuntimeError("LARGE_FRESH_RESOURCE_CANARY_MEMORY_HEADROOM")
    return {
        "schema_version": "cn_program_optimizer_large_fresh_resource_canary_v1",
        "status": "PASS_ZERO_CANDIDATE_EVALUATION_RESOURCE_CANARY",
        "requested_workers": int(workers),
        "distinct_worker_pids": pids,
        "minimum_free_memory_bytes": minimum_free,
        "maximum_worker_rss_bytes": max(int(row["rss_bytes"]) for row in results),
        "wall_seconds": float(time.perf_counter() - started),
        "candidate_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def _choose_executor_workers(authority: Mapping[str, Any], input_hash: str) -> tuple[int, dict[str, Any]]:
    attempts = []
    for workers in (PRIMARY_EXECUTOR_WORKERS, RESOURCE_FALLBACK_EXECUTOR_WORKERS):
        try:
            receipt = _resource_canary(authority, input_hash, workers)
            attempts.append(receipt)
            return int(workers), {
                "schema_version": "cn_program_optimizer_large_fresh_resource_decision_v1",
                "status": "PASS",
                "selected_executor_workers": int(workers),
                "attempts": attempts,
                "fallback_applied": int(workers) != PRIMARY_EXECUTOR_WORKERS,
                "fallback_reason": (
                    None if int(workers) == PRIMARY_EXECUTOR_WORKERS
                    else "PRIMARY_ZERO_EVALUATION_RESOURCE_CANARY_FAILED"
                ),
            }
        except Exception as exc:
            attempts.append(
                {
                    "status": "FAIL_ZERO_CANDIDATE_EVALUATION_RESOURCE_CANARY",
                    "requested_workers": int(workers),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "candidate_evaluation_executed": False,
                }
            )
    raise RuntimeError("LARGE_FRESH_RESOURCE_CANARY_ALL_PROFILES_FAILED")


def _productive_feedback(row: Mapping[str, Any]) -> bool:
    admission = dict(row.get("absolute_admission") or {})
    credit = dict(dict(row.get("enhancer_credit") or {}).get("program_credit") or {})
    return bool(admission.get("admitted")) and (
        float(credit.get("matched_cumulative_net_return_increment") or 0.0) > 0.0
        and float(credit.get("matched_net_reward_increment") or 0.0) > 0.0
    )


def _behavior_pair_identity(record: Mapping[str, Any]) -> str | None:
    primary = dict(record.get("primary") or {})
    control = dict(record.get("base_control") or {})
    left = str(primary.get("behavior_identity") or "")
    right = str(control.get("behavior_identity") or "")
    if not left or not right or left == right:
        return None
    return stable_hash({"primary": left, "base_control": right})


def _metric_block(feedback_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(row) for row in feedback_rows]
    evaluated = len(rows)
    admitted = sum(bool(dict(row.get("absolute_admission") or {}).get("admitted")) for row in rows)
    productive = sum(_productive_feedback(row) for row in rows)
    return {
        "evaluated": evaluated,
        "admitted": admitted,
        "productive": productive,
        "admission_rate": admitted / evaluated if evaluated else 0.0,
        "productive_efficiency": productive / evaluated if evaluated else 0.0,
    }


def _macro_metrics(
    feedback_rows: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
    *,
    seen_behavior_pairs: set[str],
) -> dict[str, Any]:
    block = _metric_block(feedback_rows)
    new_pairs = []
    observed_pairs = []
    for record in records:
        identity = _behavior_pair_identity(record)
        if identity is None:
            continue
        observed_pairs.append(identity)
        if identity not in seen_behavior_pairs:
            new_pairs.append(identity)
            seen_behavior_pairs.add(identity)
    per_arm = {}
    for arm in (FORMAL_OPTIMIZER_ARM, UNIFORM_CONTROL):
        subset = [row for row in feedback_rows if str(row.get("generation_arm")) == arm]
        per_arm[arm] = _metric_block(subset)
    per_template = {}
    for template in ENHANCED_TEMPLATES:
        subset = [row for row in feedback_rows if str(row.get("template_id")) == template]
        per_template[str(template)] = _metric_block(subset)
    return {
        **block,
        "behavior_pair_observations": len(observed_pairs),
        "new_behavior_pair_count": len(new_pairs),
        "new_behavior_pair_rate": len(new_pairs) / block["evaluated"] if block["evaluated"] else 0.0,
        "cumulative_behavior_pair_count": len(seen_behavior_pairs),
        "per_arm": per_arm,
        "per_template": per_template,
    }


def _value_collapse(macros: Sequence[Mapping[str, Any]]) -> bool:
    if len(macros) < MINIMUM_MACROS_BEFORE_VALUE_COLLAPSE_TEST:
        return False
    recent = list(macros[-VALUE_COLLAPSE_LOOKBACK_MACROS:])
    return len(recent) == VALUE_COLLAPSE_LOOKBACK_MACROS and all(
        float(row["productive_efficiency"]) <= PRODUCTIVE_EFFICIENCY_COLLAPSE_MAX
        and float(row["new_behavior_pair_rate"]) <= NEW_BEHAVIOR_PAIR_RATE_COLLAPSE_MAX
        for row in recent
    )


def _close_checkpoint(
    *,
    inflight: Path,
    closed: Path,
    previous_manifest_sha256: str,
    macro_index: int,
    template_id: str,
    optimizer_arm: str,
) -> str:
    artifacts = [
        shared._artifact(path, inflight)
        for path in sorted(inflight.rglob("*"))
        if path.is_file() and path.name != "checkpoint_manifest.json"
    ]
    manifest = engine._self_hashed(
        {
            "schema_version": "cn_program_optimizer_large_fresh_checkpoint_manifest_v1",
            "status": "LARGE_FRESH_CHECKPOINT_CLOSED_IMMUTABLE",
            "macro_index": int(macro_index),
            "template_id": str(template_id),
            "optimizer_arm": str(optimizer_arm),
            "previous_checkpoint_manifest_sha256": str(previous_manifest_sha256),
            "artifacts": artifacts,
        },
        "manifest_payload_sha256",
    )
    _write_json(inflight / "checkpoint_manifest.json", manifest)
    if closed.exists():
        raise RuntimeError("LARGE_FRESH_CLOSED_CHECKPOINT_ALREADY_EXISTS")
    inflight.replace(closed)
    return engine._sha256(closed / "checkpoint_manifest.json")


def _formal_optimizer_closure_fields(bandit: Any) -> dict[str, Any]:
    adapter = bandit.adapters[FORMAL_OPTIMIZER_ARM]
    if hasattr(adapter, "projection_statistics"):
        return {"projection_statistics": adapter.projection_statistics()}
    return {"formal_optimizer_metadata": adapter.optimizer_metadata()}


def _filtered_bandit(authority: Mapping[str, Any]) -> ProgramOptimizerTournamentV1:
    prior = set(map(str, authority["prior_ids"]))
    entries = tuple(
        entry for entry in authority["entries"]
        if entry.exact_identity not in prior
        and str(entry.genes["program_template_id"]) in set(ENHANCED_TEMPLATES)
    )
    entries_by_arm = {arm: entries for arm in PROGRAM_OPTIMIZER_ARMS}
    tpe_config = {
        key: value for key, value in TPE_CONFIG.items()
        if key in {"n_startup_trials", "n_ei_candidates"}
    }
    surrogate_config = {
        key: value for key, value in SURROGATE_CONFIG.items()
        if key in {"cold_start_asks", "candidate_pool_size", "n_estimators", "min_samples_leaf", "exploration_beta"}
    }
    return ProgramOptimizerTournamentV1.fresh(
        campaign_id=CAMPAIGN_ID,
        entries_by_arm=entries_by_arm,
        seeds=SEEDS,
        tpe_config=tpe_config,
        surrogate_config=surrogate_config,
    )


def run(
    args: argparse.Namespace,
    *,
    admission: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    repo_sha = str(admission["repo_sha"])
    local_args = argparse.Namespace(**vars(args))
    local_args.executor_workers = PRIMARY_EXECUTOR_WORKERS
    authority = shared._load_authority(
        local_args,
        authorization=authorization,
        repo_sha=repo_sha,
        campaign_id=CAMPAIGN_ID,
        campaign_profile=CAMPAIGN_PROFILE,
        prior_freeze_payload_sha256=PRIOR_FREEZE_PAYLOAD_SHA256,
        prior_exact_count=PRIOR_EXACT_COUNT,
        prior_exact_identities_sha256=PRIOR_EXACT_IDENTITIES_SHA256,
        prior_identity_field="combined_prior_exact_identities",
        input_binding_schema_version="cn_program_optimizer_large_fresh_input_binding_v1",
        resource_profile_id=RESOURCE_PROFILE,
        resource_profile_role="SEARCH",
        maximum_executor_workers=PRIMARY_EXECUTOR_WORKERS,
    )
    root = args.output_root.resolve()
    if not root.is_dir() or not (root / ".project_control_execution").is_dir():
        raise RuntimeError("LARGE_FRESH_ADMITTED_OUTPUT_ROOT_MISSING")
    unexpected = {path.name for path in root.iterdir() if path.name != ".project_control_execution"}
    if unexpected:
        raise RuntimeError("LARGE_FRESH_ADMITTED_OUTPUT_ROOT_NOT_CLEAN")

    input_binding = dict(authority["input_binding"])
    input_binding.update(
        {
            "schema_version": "cn_program_optimizer_large_fresh_input_binding_v1",
            "campaign_id": CAMPAIGN_ID,
            "campaign_profile": CAMPAIGN_PROFILE,
            "authorization_payload_sha256": authorization["authorization_payload_sha256"],
            "node_resource_profile": RESOURCE_PROFILE,
            "node_resource_lease_receipt": str(args.node_resource_lease_receipt.resolve()),
            "validation_feedback_used": False,
        }
    )
    input_binding.pop("input_binding_sha256", None)
    input_binding["input_binding_sha256"] = stable_hash(input_binding)
    input_hash = str(input_binding["input_binding_sha256"])
    _write_json(root / "input_binding.json", input_binding)

    workers, resource_decision = _choose_executor_workers(authority, input_hash)
    _write_json(root / "resource_canary.json", resource_decision)
    bandit = _filtered_bandit(authority)
    state = engine._selection_state()
    _write_json(root / "optimizer_state_genesis.json", bandit.snapshot())
    _write_json(root / "selection_state_genesis.json", _selection_state_record(state))

    design = dict(authorization["search_design"])
    macro_cap = int(design["macro_count"])
    prior_ids = tuple(map(str, authority["prior_ids"]))
    prior_set = set(prior_ids)
    previous_manifest_sha = "GENESIS"
    checkpoint_ordinal = 0
    next_record_ordinal = 0
    selected_exacts: set[str] = set()
    seen_behavior_pairs: set[str] = set()
    macro_rows: list[dict[str, Any]] = []
    all_feedback: list[dict[str, Any]] = []
    all_records: list[dict[str, Any]] = []
    wall_start = time.perf_counter()
    stop_reason = "HARD_CAP_REACHED"

    for macro_index in range(macro_cap):
        macro_feedback: list[dict[str, Any]] = []
        macro_records: list[dict[str, Any]] = []
        macro_manifests: list[str] = []
        for template_index, template_id in enumerate(ENHANCED_TEMPLATES):
            arm = _checkpoint_arm(macro_index, template_index)
            asks = _ask_rows(
                macro_index=macro_index,
                template_index=template_index,
                checkpoint_ordinal=checkpoint_ordinal,
                start_ordinal=next_record_ordinal,
                arm=arm,
            )
            inflight = root / f"checkpoint_{checkpoint_ordinal:04d}.inflight"
            closed = root / f"checkpoint_{checkpoint_ordinal:04d}"
            if inflight.exists() or closed.exists():
                raise RuntimeError("LARGE_FRESH_CHECKPOINT_PATH_NOT_FRESH")
            inflight.mkdir(parents=False, exist_ok=False)
            _write_jsonl(inflight / "logical_asks.jsonl", asks)
            _write_json(inflight / "optimizer_state_before.json", bandit.snapshot())
            _write_json(inflight / "selection_state_before.json", _selection_state_record(state))
            schedules, decisions = tournament._select_checkpoint(
                asks,
                catalog=authority["catalog"],
                bandit=bandit,
                state=state,
                components_by_id=authority["components_by_id"],
                adapter=authority["adapter"],
                compiler=authority["compiler"],
                prior_exact_identities=prior_ids,
            )
            exacts = [str(dict(row["optimizer_ask"])["exact_identity"]) for row in schedules]
            if len(exacts) != CHECKPOINT_BATCH_SIZE or len(set(exacts)) != CHECKPOINT_BATCH_SIZE:
                raise RuntimeError("LARGE_FRESH_CHECKPOINT_EXACT_CARDINALITY_DRIFT")
            if set(exacts).intersection(prior_set) or set(exacts).intersection(selected_exacts):
                raise RuntimeError("LARGE_FRESH_EXACT_REUSE")
            selected_exacts.update(exacts)
            _write_jsonl(inflight / "selected_schedule.jsonl", schedules)
            _write_jsonl(inflight / "selection_ledger.jsonl", decisions)
            records = shared._evaluate_schedules(
                schedules,
                record_root=inflight / "records",
                authority=authority,
                input_hash=input_hash,
                executor_workers=workers,
            )
            for record in records:
                if any(
                    int(record.get(key) or 0) != 0
                    for key in (
                        "validation_reads",
                        "holdout_reads",
                        "historical_2023_reads",
                        "forward_b_reads",
                        "forward_2026_reads",
                    )
                ):
                    raise RuntimeError("LARGE_FRESH_RESTRICTED_READ_DRIFT")
            feedback = tournament._feedback_update(
                records,
                schedules,
                bandit=bandit,
                behavior_counts=Counter(),
            )
            _write_jsonl(inflight / "feedback.jsonl", feedback)
            _write_json(inflight / "optimizer_state_after.json", bandit.snapshot())
            _write_json(inflight / "selection_state_after.json", _selection_state_record(state))
            manifest_sha = _close_checkpoint(
                inflight=inflight,
                closed=closed,
                previous_manifest_sha256=previous_manifest_sha,
                macro_index=macro_index,
                template_id=str(template_id),
                optimizer_arm=arm,
            )
            previous_manifest_sha = manifest_sha
            macro_manifests.append(manifest_sha)
            macro_feedback.extend(feedback)
            macro_records.extend(records)
            all_feedback.extend(feedback)
            all_records.extend(records)
            checkpoint_ordinal += 1
            next_record_ordinal += CHECKPOINT_BATCH_SIZE

        macro_metric = _macro_metrics(
            macro_feedback,
            macro_records,
            seen_behavior_pairs=seen_behavior_pairs,
        )
        elapsed = max(time.perf_counter() - wall_start, 1e-9)
        macro_metric["campaign_pairs_per_hour"] = len(all_feedback) * 3600.0 / elapsed
        macro_receipt = engine._self_hashed(
            {
                "schema_version": "cn_program_optimizer_large_fresh_macro_receipt_v1",
                "status": "LARGE_FRESH_MACRO_CLOSED_IMMUTABLE",
                "macro_index": int(macro_index),
                "evaluated": len(macro_feedback),
                "checkpoint_manifest_sha256": macro_manifests,
                "metrics": macro_metric,
                "optimizer_state_sha256": str(bandit.snapshot()["bandit_state_sha256"]),
                "selected_exact_count_cumulative": len(selected_exacts),
                "prior_overlap_count": len(selected_exacts.intersection(prior_set)),
            },
            "macro_payload_sha256",
        )
        _write_json(root / f"macro_{macro_index:02d}_receipt.json", macro_receipt)
        macro_rows.append({"macro_index": int(macro_index), **macro_metric})
        _write_json(root / "macro_metrics.json", {"macros": macro_rows})
        if (
            macro_index == 0
            and float(macro_metric["campaign_pairs_per_hour"])
            < MINIMUM_PAIRS_PER_HOUR_AFTER_FIRST_MACRO
        ):
            stop_reason = "THROUGHPUT_BELOW_FROZEN_MINIMUM"
            break
        if _value_collapse(macro_rows):
            stop_reason = "SUSTAINED_DEVELOPMENT_VALUE_COLLAPSE"
            break

    if len(selected_exacts) != len(all_feedback) or len(all_feedback) != len(all_records):
        raise RuntimeError("LARGE_FRESH_TERMINAL_CARDINALITY_DRIFT")
    if selected_exacts.intersection(prior_set):
        raise RuntimeError("LARGE_FRESH_TERMINAL_PRIOR_OVERLAP")
    total_metrics = _metric_block(all_feedback)
    per_arm = {
        arm: _metric_block([row for row in all_feedback if str(row.get("generation_arm")) == arm])
        for arm in (FORMAL_OPTIMIZER_ARM, UNIFORM_CONTROL)
    }
    per_template = {
        str(template): _metric_block(
            [row for row in all_feedback if str(row.get("template_id")) == str(template)]
        )
        for template in ENHANCED_TEMPLATES
    }
    closure = engine._self_hashed(
        {
            "schema_version": CLOSURE_SCHEMA_VERSION,
            "status": STATUS_COMPLETE,
            "campaign_id": CAMPAIGN_ID,
            "campaign_profile": CAMPAIGN_PROFILE,
            "repo_sha": repo_sha,
            "authorization_payload_sha256": authorization["authorization_payload_sha256"],
            "input_binding_sha256": input_hash,
            "formal_search_authority": FORMAL_SEARCH_AUTHORITY,
            "resource_profile": RESOURCE_PROFILE,
            "executor_workers": workers,
            "logical_records": len(all_feedback),
            "unique_exact_count": len(selected_exacts),
            "prior_exact_count": PRIOR_EXACT_COUNT,
            "prior_exact_identities_sha256": PRIOR_EXACT_IDENTITIES_SHA256,
            "prior_overlap_count": 0,
            "closed_checkpoints": checkpoint_ordinal,
            "closed_macros": len(macro_rows),
            "stop_reason": stop_reason,
            "total_metrics": total_metrics,
            "per_arm": per_arm,
            "per_template": per_template,
            "macro_metrics": macro_rows,
            "behavior_pair_count": len(seen_behavior_pairs),
            "optimizer_final_state_sha256": str(bandit.snapshot()["bandit_state_sha256"]),
            **_formal_optimizer_closure_fields(bandit),
            "wall_seconds": float(time.perf_counter() - wall_start),
            "resource_final": engine._runtime_resource_snapshot(),
            "restricted_reads": {
                "validation": 0,
                "holdout": 0,
                "historical_2023": 0,
                "forward_b": 0,
                "forward_2026": 0,
            },
            "validation_feedback_used": False,
            "oos_authority": "NONE",
            "promotion_authorized": False,
            "automatic_successor_authorized": False,
        },
        "closure_payload_sha256",
    )
    _write_json(root / CLOSURE_NAME, closure)
    return closure


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--node-resource-lease-receipt", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.error("run through the Project-Control route, not this helper directly")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
