"""Prospective development-only Phase D freeze for the joint-program allocator."""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import json
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from our_system_phase2.runtime import (
    cn_joint_program_allocator_repair_canary_v0 as canary,
)
from our_system_phase2.runtime import cn_joint_program_phase_c_v0 as phase_c
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import (
    CACHE_CAP_BYTES,
    ENTITLEMENT_THREADS,
    ENHANCED_TEMPLATE_ORDER,
    EXECUTOR_WORKERS,
    NATIVE_THREADS_PER_WORKER,
    TEMPLATE_ORDER,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


FREEZE_SCHEMA = "cn_joint_program_phase_d_prefinancial_freeze_v0"
FREEZE_STATUS = "CN_JOINT_PROGRAM_PHASE_D_PREFINANCIAL_FREEZE_COMPLETE"
FREEZE_CLOSURE_NAME = "CN_JOINT_PROGRAM_PHASE_D_PREFINANCIAL_FREEZE_COMPLETE.json"
RUN_CONTRACT_SCHEMA = "cn_joint_program_phase_d_run_contract_v0"
ACCESS_SCHEMA = "cn_joint_program_phase_d_access_v0"
SUMMARY_SCHEMA = "cn_joint_program_phase_d_prefinancial_summary_v0"
SUMMARY_STATUS = "PHASE_D_PREFINANCIAL_READY"
ARTIFACT_MANIFEST_SCHEMA = "cn_joint_program_phase_d_prefinancial_artifacts_v0"
CAMPAIGN_ID = "CN_JOINT_PROGRAM_PHASE_D_V0"
BATCH_ID = "PHASE_D_PROSPECTIVE_CONFIRMATION_512_V0"
EXPECTED_RECORDS = 512
BASE_RECORDS = 64
ENHANCED_RECORDS_PER_TEMPLATE = 64
RAW_RESERVOIR_PER_ENHANCED_TEMPLATE = 512
RECORDS_PER_CHECKPOINT = 8
CHECKPOINT_COUNT = EXPECTED_RECORDS // RECORDS_PER_CHECKPOINT
MAX_VARIANTS_PER_BASE_PER_TEMPLATE = 4
MIN_BASE_IDENTITIES_PER_TEMPLATE = 16
RESOURCE_PROFILE = "VALIDATION_EXCLUSIVE_32_PHASE_D_TELEMETRY"
MINIMUM_FREE_MEMORY_BYTES = 1
FREE_MEMORY_ENFORCEMENT = "TELEMETRY_ONLY_NO_FIXED_24_GIB_HARD_FAIL"
IMPROVED_TEMPLATES = (
    "BASE_EVENT",
    "BASE_MARKET_EVENT",
    "BASE_TEMPORAL",
    "BASE_TEMPORAL_EVENT",
)
WEAK_TEMPLATES = (
    "BASE_MARKET",
    "BASE_TEMPORAL_MARKET",
    "BASE_TEMPORAL_MARKET_EVENT",
)
IMPROVED_ARM_PATTERN = (
    "UNIFORM_FRESH",
    "UNIFORM_FRESH",
    "UNIFORM_FRESH",
    "REVISED_EXPLOIT",
    "REVISED_EXPLOIT",
    "REVISED_EXPLOIT",
    "NOVELTY_RESERVE",
    "NOVELTY_RESERVE",
)
WEAK_ARM_PATTERN = (
    "UNIFORM_FRESH",
    "UNIFORM_FRESH",
    "UNIFORM_FRESH",
    "UNIFORM_FRESH",
    "UNIFORM_FRESH",
    "UNIFORM_FRESH",
    "NOVELTY_RESERVE",
    "NOVELTY_RESERVE",
)
EXPERIMENT_SEED_SHA256 = stable_hash(
    {"experiment_id": CAMPAIGN_ID, "batch_id": BATCH_ID, "version": 0}
)
RAW_ORDINAL_OFFSET = int(EXPERIMENT_SEED_SHA256[:8], 16)
DECISION_GATES = {
    "productive_rate_delta_vs_uniform_minimum": 0.05,
    "all_four_positive_rate_delta_vs_uniform_minimum": 1e-12,
    "primary_reward_positive_rate_delta_vs_uniform_minimum": 0.0,
    "primary_return_positive_rate_delta_vs_uniform_minimum": 0.0,
    "three_window_positive_rate_delta_vs_uniform_minimum": 0.0,
    "median_return_per_turnover_delta_vs_uniform_minimum": 0.0,
    "blocked_rate_delta_vs_uniform_maximum": 0.0,
    "matched_return_increment_mean_delta_vs_uniform_minimum": 0.0,
    "matched_return_increment_median_delta_vs_uniform_minimum": 0.0,
    "minimum_improved_revised_templates": 3,
    "per_template_blocked_rate_worsening_allowed": False,
    "revised_exploit_allowed_templates": list(IMPROVED_TEMPLATES),
    "revised_exploit_prohibited_templates": list(WEAK_TEMPLATES),
}


def generation_arm_v0(template_id: str, template_record_ordinal: int) -> str:
    ordinal = int(template_record_ordinal)
    if ordinal not in range(64):
        raise ValueError("Phase D template ordinal is out of range")
    if template_id == "BASE":
        return "UNIFORM_FRESH"
    if template_id in IMPROVED_TEMPLATES:
        return IMPROVED_ARM_PATTERN[ordinal % RECORDS_PER_CHECKPOINT]
    if template_id in WEAK_TEMPLATES:
        return WEAK_ARM_PATTERN[ordinal % RECORDS_PER_CHECKPOINT]
    raise ValueError(f"unknown Phase D template: {template_id}")


def build_ask_plan_v0() -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    for template_id in TEMPLATE_ORDER:
        for template_ordinal in range(64):
            ordinal = len(rows)
            arm = generation_arm_v0(template_id, template_ordinal)
            row = {
                "schema_version": "cn_joint_program_phase_d_ask_v0",
                "main_record_ordinal": ordinal,
                "checkpoint_ordinal": ordinal // RECORDS_PER_CHECKPOINT,
                "template_id": template_id,
                "template_record_ordinal": template_ordinal,
                "generation_arm": arm,
                "baseline_comparison_member": bool(
                    template_id in IMPROVED_TEMPLATES and arm == "UNIFORM_FRESH"
                ),
                "bandit_feedback_eligible": template_id != "BASE",
                "early_template_cancellation_allowed": False,
                "adaptive_budget_reallocation_allowed": False,
                "maximum_variants_per_base_per_template": (
                    MAX_VARIANTS_PER_BASE_PER_TEMPLATE
                ),
                "phase_d_allocator_scope": (
                    "REVISED_EXPLOIT_CONFIRMATION"
                    if template_id in IMPROVED_TEMPLATES
                    else "UNIFORM_NOVELTY_DIAGNOSTIC"
                    if template_id in WEAK_TEMPLATES
                    else "BASE_PARITY"
                ),
            }
            row["ask_record_sha256"] = stable_hash(row)
            rows.append(row)
    if len(rows) != EXPECTED_RECORDS:
        raise RuntimeError("Phase D ask-plan cardinality drift")
    return tuple(rows)


def _verify_allocator_canary_outcome(path: Path) -> dict[str, Any]:
    outcome = phase_c._read_json(path)
    phase_c._verify_self_hash(
        outcome, "outcome_payload_sha256", "allocator-repair canary outcome"
    )
    next_action = dict(outcome.get("next_action") or {})
    if (
        outcome.get("status")
        != "ALLOCATOR_REPAIR_CANARY_CLOSED_PHASE_D_ELIGIBLE_NO_LAUNCH"
        or outcome.get("decision") != "PHASE_D_ELIGIBLE_NO_LAUNCH"
        or next_action.get("authorization")
        != "REQUIRES_SEPARATE_PHASE_D_FREEZE_AND_EXPLICIT_LAUNCH"
        or bool(outcome.get("phase_d_launched"))
    ):
        raise PermissionError("allocator canary outcome does not authorize Phase D freeze")
    closure_path = (
        Path(str(outcome["output_root"]))
        / "CN_JOINT_PROGRAM_ALLOCATOR_REPAIR_CANARY_COMPLETE.json"
    )
    audit_path = Path(str(outcome["final_audit_root"])) / "audit.json"
    if (
        not closure_path.is_file()
        or phase_c._sha256(closure_path) != str(outcome["closure_file_sha256"])
        or not audit_path.is_file()
        or phase_c._sha256(audit_path) != str(outcome["final_audit_file_sha256"])
    ):
        raise ValueError("allocator canary outcome evidence binding drift")
    return outcome


def _verify_capacity(path: Path) -> dict[str, Any]:
    payload = phase_c._read_json(path)
    body = dict(payload)
    expected = str(body.pop("capacity_manifest_sha256", ""))
    if expected != stable_hash(body):
        raise ValueError("Phase D capacity manifest self-hash drift")
    profile = dict(payload.get("profiles", {}).get(RESOURCE_PROFILE) or {})
    if (
        payload.get("free_memory_enforcement") != FREE_MEMORY_ENFORCEMENT
        or int(profile.get("cpu_threads") or 0) != ENTITLEMENT_THREADS
        or int(profile.get("memory_claim_bytes") or 0) != 1
        or int(profile.get("minimum_free_memory_bytes") or 0)
        != MINIMUM_FREE_MEMORY_BYTES
        or profile.get("concurrency_contract") != "EXCLUSIVE_HEAVY_LANE"
    ):
        raise ValueError("Phase D capacity contract drift")
    return payload


@contextmanager
def _configured_canary_builder() -> Iterator[None]:
    settings = {
        "FREEZE_SCHEMA": FREEZE_SCHEMA,
        "FREEZE_STATUS": FREEZE_STATUS,
        "FREEZE_CLOSURE_NAME": FREEZE_CLOSURE_NAME,
        "RUN_CONTRACT_SCHEMA": RUN_CONTRACT_SCHEMA,
        "ACCESS_SCHEMA": ACCESS_SCHEMA,
        "SUMMARY_SCHEMA": SUMMARY_SCHEMA,
        "SUMMARY_STATUS": SUMMARY_STATUS,
        "ARTIFACT_MANIFEST_SCHEMA": ARTIFACT_MANIFEST_SCHEMA,
        "BANDIT_FEEDBACK_SOURCE": "PHASE_B_DEVELOPMENT_ONLY_NO_CANARY_OR_PHASE_C_FINANCIAL_IMPORT",
        "CAMPAIGN_ID": CAMPAIGN_ID,
        "BATCH_ID": BATCH_ID,
        "EXPECTED_RECORDS": EXPECTED_RECORDS,
        "BASE_RECORDS": BASE_RECORDS,
        "ENHANCED_RECORDS_PER_TEMPLATE": ENHANCED_RECORDS_PER_TEMPLATE,
        "RAW_RESERVOIR_PER_ENHANCED_TEMPLATE": RAW_RESERVOIR_PER_ENHANCED_TEMPLATE,
        "RECORDS_PER_CHECKPOINT": RECORDS_PER_CHECKPOINT,
        "CHECKPOINT_COUNT": CHECKPOINT_COUNT,
        "MAX_VARIANTS_PER_BASE_PER_TEMPLATE": MAX_VARIANTS_PER_BASE_PER_TEMPLATE,
        "MIN_BASE_IDENTITIES_PER_TEMPLATE": MIN_BASE_IDENTITIES_PER_TEMPLATE,
        "EXPERIMENT_SEED_SHA256": EXPERIMENT_SEED_SHA256,
        "RAW_ORDINAL_OFFSET": RAW_ORDINAL_OFFSET,
        "DECISION_GATES": DECISION_GATES,
        "RESOURCE_PROFILE": RESOURCE_PROFILE,
        "MINIMUM_FREE_MEMORY_BYTES": MINIMUM_FREE_MEMORY_BYTES,
        "build_ask_plan_v0": build_ask_plan_v0,
    }
    original = {name: getattr(canary, name) for name in settings}
    try:
        for name, value in settings.items():
            setattr(canary, name, value)
        yield
    finally:
        for name, value in original.items():
            setattr(canary, name, value)


def build_prefinancial_freeze_v0(
    *,
    output_root: Path,
    phase_b_freeze_root: Path,
    phase_b_result_root: Path,
    phase_b_outcome_path: Path,
    phase_c_outcome_path: Path,
    allocator_canary_outcome_path: Path,
    registry_path: Path,
    accepted_field_manifest_path: Path,
    node_resource_capacity_path: Path,
    repo_sha: str,
) -> dict[str, Any]:
    canary_outcome = _verify_allocator_canary_outcome(
        allocator_canary_outcome_path.resolve()
    )
    _verify_capacity(node_resource_capacity_path.resolve())
    parent_evidence = {
        "allocator_canary_outcome_path": str(allocator_canary_outcome_path.resolve()),
        "allocator_canary_outcome_file_sha256": phase_c._sha256(
            allocator_canary_outcome_path.resolve()
        ),
        "allocator_canary_outcome_payload_sha256": str(
            canary_outcome["outcome_payload_sha256"]
        ),
        "allocator_canary_financial_records_imported": False,
        "phase_c_financial_records_imported_into_phase_d": False,
        "node_resource_capacity_path": str(node_resource_capacity_path.resolve()),
        "node_resource_capacity_file_sha256": phase_c._sha256(
            node_resource_capacity_path.resolve()
        ),
        "node_resource_capacity_payload_sha256": str(
            _verify_capacity(node_resource_capacity_path.resolve())[
                "capacity_manifest_sha256"
            ]
        ),
        "free_memory_enforcement": FREE_MEMORY_ENFORCEMENT,
        "revised_exploit_allowed_templates": list(IMPROVED_TEMPLATES),
        "revised_exploit_prohibited_templates": list(WEAK_TEMPLATES),
        "automatic_phase_e_launch": False,
    }
    with _configured_canary_builder():
        return canary.build_prefinancial_freeze_v0(
            output_root=output_root,
            phase_b_freeze_root=phase_b_freeze_root,
            phase_b_result_root=phase_b_result_root,
            phase_b_outcome_path=phase_b_outcome_path,
            phase_c_outcome_path=phase_c_outcome_path,
            registry_path=registry_path,
            accepted_field_manifest_path=accepted_field_manifest_path,
            repo_sha=repo_sha,
            additional_parent_evidence=parent_evidence,
        )


def verify_prefinancial_freeze_v0(root: Path) -> dict[str, Any]:
    with _configured_canary_builder():
        closure = canary.verify_prefinancial_freeze_v0(root)
    asks = phase_c._read_jsonl(root.resolve() / "phase_c_ask_plan.jsonl")
    counts = Counter(str(row["generation_arm"]) for row in asks)
    if counts != Counter(
        {"UNIFORM_FRESH": 304, "REVISED_EXPLOIT": 96, "NOVELTY_RESERVE": 112}
    ):
        raise ValueError("Phase D arm totals drift")
    for checkpoint in range(CHECKPOINT_COUNT):
        rows = asks[checkpoint * 8 : (checkpoint + 1) * 8]
        if len({str(row["template_id"]) for row in rows}) != 1:
            raise ValueError("Phase D checkpoint crosses templates")
    if any(
        row["generation_arm"] == "REVISED_EXPLOIT"
        and row["template_id"] not in IMPROVED_TEMPLATES
        for row in asks
    ):
        raise PermissionError("Phase D revised exploit escaped qualified templates")
    contract = phase_c._read_json(root.resolve() / "phase_c_run_contract.json")
    phase_c._verify_self_hash(contract, "run_contract_sha256", "Phase D contract")
    if (
        contract.get("free_memory_enforcement") != FREE_MEMORY_ENFORCEMENT
        or int(contract.get("minimum_free_memory_bytes") or 0) != 1
        or contract.get("resource_profile") != RESOURCE_PROFILE
        or bool(contract.get("allocator_canary_financial_records_imported"))
        or bool(contract.get("phase_c_financial_records_imported_into_phase_d"))
        or contract.get("decision_gates") != DECISION_GATES
    ):
        raise ValueError("Phase D frozen contract drift")
    capacity_path = Path(str(contract["node_resource_capacity_path"]))
    _verify_capacity(capacity_path)
    if phase_c._sha256(capacity_path) != str(
        contract["node_resource_capacity_file_sha256"]
    ):
        raise ValueError("Phase D capacity file binding drift")
    return closure


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--output-root", required=True, type=Path)
    freeze.add_argument("--phase-b-freeze-root", required=True, type=Path)
    freeze.add_argument("--phase-b-result-root", required=True, type=Path)
    freeze.add_argument("--phase-b-outcome", required=True, type=Path)
    freeze.add_argument("--phase-c-outcome", required=True, type=Path)
    freeze.add_argument("--allocator-canary-outcome", required=True, type=Path)
    freeze.add_argument("--registry", required=True, type=Path)
    freeze.add_argument("--accepted-field-manifest", required=True, type=Path)
    freeze.add_argument("--node-resource-capacity", required=True, type=Path)
    freeze.add_argument("--repo-sha", required=True)
    verify = subparsers.add_parser("verify-freeze")
    verify.add_argument("--freeze-root", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        closure = build_prefinancial_freeze_v0(
            output_root=args.output_root,
            phase_b_freeze_root=args.phase_b_freeze_root,
            phase_b_result_root=args.phase_b_result_root,
            phase_b_outcome_path=args.phase_b_outcome,
            phase_c_outcome_path=args.phase_c_outcome,
            allocator_canary_outcome_path=args.allocator_canary_outcome,
            registry_path=args.registry,
            accepted_field_manifest_path=args.accepted_field_manifest,
            node_resource_capacity_path=args.node_resource_capacity,
            repo_sha=str(args.repo_sha),
        )
    else:
        closure = verify_prefinancial_freeze_v0(args.freeze_root)
    print(json.dumps(closure, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
