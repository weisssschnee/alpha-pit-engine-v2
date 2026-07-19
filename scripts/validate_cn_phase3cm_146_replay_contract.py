from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping


SHA256_RE = re.compile(r"[0-9a-f]{64}")
GIT_SHA_RE = re.compile(r"[0-9a-f]{40}")


def contract_hash(payload: Mapping[str, Any]) -> str:
    canonical = copy.deepcopy(dict(payload))
    canonical.pop("contract_hash", None)
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and SHA256_RE.fullmatch(value) is not None


def validate_contract(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    _require(
        payload.get("schema_version")
        == "cn_phase3cm_current_kernel_146_parity_replay_v1",
        "schema_version drift",
        errors,
    )
    _require(
        payload.get("authorization_status")
        == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_REPLAY_AUTHORIZED",
        "authorization drift",
        errors,
    )

    authority = payload.get("authority_statuses") or {}
    expected_authority = {
        "kernel_qualification": "PARTIALLY_QUALIFIED",
        "backend_authority": "EXPERIMENTAL_BACKEND",
        "formal_evaluator_authority": "FORMAL_EVALUATOR_AUTHORITY_UNCHANGED",
        "kernel_change_policy": "FROZEN_NO_FURTHER_KERNEL_CHANGES",
    }
    _require(authority == expected_authority, "authority statuses drift", errors)

    source = payload.get("source_binding") or {}
    _require(
        isinstance(source.get("frozen_kernel_base_repo_sha"), str)
        and GIT_SHA_RE.fullmatch(source["frozen_kernel_base_repo_sha"]) is not None,
        "frozen kernel base repo SHA is invalid",
        errors,
    )
    _require(
        source.get("portfolio_source_sha256")
        == "5b665d33d74eb4b5f451f06b4ad7e4b352f779c3e2e4836bb2d76950c413d110",
        "portfolio source SHA drift",
        errors,
    )
    _require(source.get("historical_assets_immutable") is True, "historical assets not immutable", errors)

    historical = payload.get("historical_input_binding") or {}
    global_pairs = historical.get("expected_pair_ids") or []
    global_candidates = historical.get("expected_candidate_ids") or []
    _require(historical.get("expected_pair_count") == 146, "global pair count is not 146", errors)
    _require(historical.get("expected_candidate_count") == 292, "global candidate count is not 292", errors)
    _require(len(global_pairs) == 146 and len(set(global_pairs)) == 146, "global pair IDs are not 146 unique IDs", errors)
    _require(
        len(global_candidates) == 292 and len(set(global_candidates)) == 292,
        "global candidate IDs are not 292 unique IDs",
        errors,
    )
    _require(global_pairs == sorted(global_pairs), "global pair IDs are not canonical-sorted", errors)
    _require(global_candidates == sorted(global_candidates), "global candidate IDs are not canonical-sorted", errors)
    for key in ("partition_contract", "input_binding", "full_active_candidate_table"):
        _require(_is_sha256((historical.get(key) or {}).get("sha256")), f"{key} SHA is invalid", errors)

    partitions = payload.get("partitions") or []
    _require(len(partitions) == 2, "partition count is not two", errors)
    partition_pairs: list[str] = []
    partition_candidates: list[str] = []
    for index, partition in enumerate(partitions):
        prefix = f"partition[{index}]"
        pairs = partition.get("expected_pair_ids") or []
        candidates = partition.get("expected_candidate_ids") or []
        _require(partition.get("pair_count") == 73, f"{prefix} pair count is not 73", errors)
        _require(partition.get("candidate_count") == 146, f"{prefix} candidate count is not 146", errors)
        _require(len(pairs) == 73 and len(set(pairs)) == 73, f"{prefix} pair IDs drift", errors)
        _require(len(candidates) == 146 and len(set(candidates)) == 146, f"{prefix} candidate IDs drift", errors)
        for offset in range(0, len(candidates), 2):
            if offset + 1 >= len(candidates):
                errors.append(f"{prefix} candidate pair is incomplete")
                break
            primary, control = candidates[offset : offset + 2]
            _require(
                not primary.endswith(".control") and control == primary + ".control",
                f"{prefix} primary/control adjacency drift at candidate offset {offset}",
                errors,
            )
        partition_pairs.extend(pairs)
        partition_candidates.extend(candidates)
        _require(_is_sha256((partition.get("candidate_table") or {}).get("sha256")), f"{prefix} candidate SHA invalid", errors)
        plan = partition.get("historical_execution_plan") or {}
        _require(plan.get("reuse_scope") == "EXACT_PHASE_E_PLAN_REPLAY", f"{prefix} plan reuse scope drift", errors)
        _require(_is_sha256(plan.get("sha256")), f"{prefix} plan SHA invalid", errors)
        _require(_is_sha256(plan.get("execution_plan_hash")), f"{prefix} plan hash invalid", errors)
        _require(plan.get("frozen_checkpoint_every_blocks") == 1, f"{prefix} checkpoint cadence drift", errors)
        command = partition.get("historical_backend_command") or {}
        _require(_is_sha256(command.get("sha256")), f"{prefix} command SHA invalid", errors)
        receipt = partition.get("capacity_receipt") or {}
        _require(_is_sha256(receipt.get("sha256")), f"{prefix} capacity receipt SHA invalid", errors)
        _require(_is_sha256(receipt.get("receipt_hash")), f"{prefix} capacity receipt hash invalid", errors)
        _require(receipt.get("receipt_hash") == partition.get("capacity_receipt_hash"), f"{prefix} capacity receipt binding drift", errors)
        _require(receipt.get("max_block_rows") == partition.get("max_block_rows") == 12_389_087, f"{prefix} max block rows drift", errors)

    _require(set(partition_pairs) == set(global_pairs), "partition pair union differs from global pair IDs", errors)
    _require(set(partition_candidates) == set(global_candidates), "partition candidate union differs from global candidate IDs", errors)
    _require(set(partitions[0].get("expected_pair_ids") or []).isdisjoint(partitions[1].get("expected_pair_ids") or []) if len(partitions) == 2 else False, "partition pair IDs overlap", errors)

    execution = payload.get("execution_contract") or {}
    _require(execution.get("phase") == "E", "replay is not bound to exact Phase E plan mode", errors)
    _require(execution.get("authority_scope") == "PARITY_REPLAY_ONLY_NOT_FORMAL_EVALUATOR", "replay authority scope drift", errors)
    _require(execution.get("heavy_processes") == 2, "heavy process count drift", errors)
    _require(execution.get("compute_threads_per_process") == 11, "per-process thread count drift", errors)
    _require(execution.get("active_native_compute_threads_total") == 22, "active native thread total drift", errors)
    _require(execution.get("active_native_compute_threads_total", 999) <= execution.get("global_native_compute_threads_max", 0) <= 24, "global native thread budget exceeded", errors)
    expected_env = {
        "NUMBA_NUM_THREADS": "11",
        "ARROW_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "NUMEXPR_MAX_THREADS": "1",
        "POLARS_MAX_THREADS": "1",
    }
    _require(execution.get("thread_environment") == expected_env, "thread environment drift", errors)
    _require(execution.get("adaptation") == "FORBIDDEN", "execution adaptation is not forbidden", errors)

    checkpoint = payload.get("checkpoint_contract") or {}
    _require(
        checkpoint.get("required_payload_sections")
        == ["temporal", "state", "support", "portfolio", "reducer"],
        "checkpoint payload sections drift",
        errors,
    )
    _require(checkpoint.get("hash_is_verification_not_payload") is True, "checkpoint payload contract drift", errors)

    resources = payload.get("resource_gates") or {}
    _require(resources.get("wall_seconds_hard") == 21_600, "wall cap drift", errors)
    _require(resources.get("global_rss_hard_bytes") == 60 * 1024**3, "global RSS cap drift", errors)
    _require(resources.get("output_bytes_hard") == 64 * 1024**3, "output cap drift", errors)
    _require(resources.get("fail_closed") is True and resources.get("plan_change_after_start") == "FORBIDDEN", "resource fail-closed contract drift", errors)

    access = payload.get("data_access_contract") or {}
    _require(access.get("development_train_2024_2025") == "ALLOWED", "development access drift", errors)
    for key in ("validation", "holdout"):
        _require(access.get(key) == "FORBIDDEN_ZERO_READS_REQUIRED", f"{key} boundary drift", errors)
    _require(access.get("forward_2026") == "SEALED_ZERO_READS_REQUIRED", "forward boundary drift", errors)
    _require(access.get("candidate_promotion") == "FORBIDDEN", "promotion boundary drift", errors)

    parity = payload.get("parity_gate") or {}
    required_dimensions = {
        "candidate_identity",
        "support",
        "rank",
        "weights",
        "turnover",
        "cost",
        "reward",
        "rank_ic",
        "blocker",
        "checkpoint",
    }
    _require(set((parity.get("required_dimensions") or {}).keys()) == required_dimensions, "parity dimensions drift", errors)
    _require(parity.get("raw_rank_arrays_retained") is False, "raw rank arrays falsely claimed", errors)
    _require(parity.get("raw_weight_arrays_retained") is False, "raw weight arrays falsely claimed", errors)
    _require(parity.get("minimum_speedup_required") is False, "speedup remains a parity gate", errors)
    _require(parity.get("two_point_zero_speedup_gate") == "REMOVED", "2.000x gate was not removed", errors)

    wave = payload.get("wave_1024_gate") or {}
    _require(wave.get("status") == "CONDITIONALLY_AUTHORIZED_NOT_EXECUTABLE_UNTIL_GATE_RECEIPT", "1024 authorization gate drift", errors)
    _require(wave.get("speedup_threshold") == "NONE", "1024 speedup threshold was reintroduced", errors)
    _require("146_PAIR_EXACT_SEMANTIC_PARITY_PASS" in (wave.get("required_conditions") or []), "1024 gate lacks 146 parity", errors)

    expected_hash = contract_hash(payload)
    _require(payload.get("contract_hash") == expected_hash, "contract hash mismatch", errors)
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contract", type=Path)
    parser.add_argument("--print-contract-hash", action="store_true")
    args = parser.parse_args()
    payload = json.loads(args.contract.read_text(encoding="utf-8-sig"))
    expected_hash = contract_hash(payload)
    if args.print_contract_hash:
        print(expected_hash)
    errors = validate_contract(payload)
    result = {
        "status": "CN_PHASE3CM_CURRENT_KERNEL_146_REPLAY_CONTRACT_VALID"
        if not errors
        else "CN_PHASE3CM_CURRENT_KERNEL_146_REPLAY_CONTRACT_INVALID",
        "contract_hash": expected_hash,
        "errors": errors,
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())

