"""Zero-financial freeze for the prospective joint-program allocator repair canary."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.runtime import cn_joint_program_phase_c_v0 as phase_c
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import (
    CACHE_CAP_BYTES,
    ENTITLEMENT_THREADS,
    ENHANCED_TEMPLATE_ORDER,
    EXECUTOR_WORKERS,
    MINIMUM_FREE_MEMORY_BYTES,
    NATIVE_THREADS_PER_WORKER,
    RESOURCE_PROFILE,
    TEMPLATE_ORDER,
    verify_phase_b_prefinancial_freeze_v0,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    ProgramProposalReceiptV0,
)
from our_system_phase2.services.program_allocator_repair_bandit_v0 import (
    BANDIT_POLICY_ID,
    BANDIT_VERSION,
    OUTCOME_FIELDS,
    ProgramAllocatorRepairBanditV0,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


FREEZE_SCHEMA = "cn_joint_program_allocator_repair_canary_prefinancial_freeze_v0"
FREEZE_STATUS = "CN_JOINT_PROGRAM_ALLOCATOR_REPAIR_CANARY_PREFINANCIAL_FREEZE_COMPLETE"
FREEZE_CLOSURE_NAME = (
    "CN_JOINT_PROGRAM_ALLOCATOR_REPAIR_CANARY_PREFINANCIAL_FREEZE_COMPLETE.json"
)
CAMPAIGN_ID = "CN_JOINT_PROGRAM_ALLOCATOR_REPAIR_CANARY_V0"
BATCH_ID = "ALLOCATOR_REPAIR_PROSPECTIVE_256_V0"
EXPECTED_RECORDS = 256
BASE_RECORDS = 32
ENHANCED_RECORDS_PER_TEMPLATE = 32
RAW_RESERVOIR_PER_ENHANCED_TEMPLATE = 256
RECORDS_PER_CHECKPOINT = 8
CHECKPOINT_COUNT = EXPECTED_RECORDS // RECORDS_PER_CHECKPOINT
MAX_VARIANTS_PER_BASE_PER_TEMPLATE = 4
MIN_BASE_IDENTITIES_PER_TEMPLATE = 16
ARM_PATTERN = (
    "UNIFORM_FRESH",
    "UNIFORM_FRESH",
    "UNIFORM_FRESH",
    "REVISED_EXPLOIT",
    "REVISED_EXPLOIT",
    "REVISED_EXPLOIT",
    "NOVELTY_RESERVE",
    "NOVELTY_RESERVE",
)
EXPERIMENT_SEED_SHA256 = stable_hash(
    {"experiment_id": CAMPAIGN_ID, "batch_id": BATCH_ID, "version": 0}
)
RAW_ORDINAL_OFFSET = int(EXPERIMENT_SEED_SHA256[:8], 16)


def generation_arm_v0(template_id: str, template_record_ordinal: int) -> str:
    ordinal = int(template_record_ordinal)
    if template_id == "BASE":
        if ordinal not in range(BASE_RECORDS):
            raise ValueError("allocator-repair BASE ordinal is out of range")
        return "UNIFORM_FRESH"
    if template_id not in ENHANCED_TEMPLATE_ORDER or ordinal not in range(
        ENHANCED_RECORDS_PER_TEMPLATE
    ):
        raise ValueError("allocator-repair enhanced template/ordinal is out of range")
    return ARM_PATTERN[ordinal % len(ARM_PATTERN)]


def build_ask_plan_v0() -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    global_ordinal = 0
    for template_id in TEMPLATE_ORDER:
        quota = BASE_RECORDS if template_id == "BASE" else ENHANCED_RECORDS_PER_TEMPLATE
        for template_ordinal in range(quota):
            arm = generation_arm_v0(template_id, template_ordinal)
            row = {
                "schema_version": "cn_joint_program_allocator_repair_canary_ask_v0",
                "main_record_ordinal": global_ordinal,
                "checkpoint_ordinal": global_ordinal // RECORDS_PER_CHECKPOINT,
                "template_id": template_id,
                "template_record_ordinal": template_ordinal,
                "generation_arm": arm,
                "baseline_comparison_member": bool(
                    template_id != "BASE" and arm == "UNIFORM_FRESH"
                ),
                "bandit_feedback_eligible": template_id != "BASE",
                "early_template_cancellation_allowed": False,
                "adaptive_budget_reallocation_allowed": False,
                "maximum_variants_per_base_per_template": (
                    MAX_VARIANTS_PER_BASE_PER_TEMPLATE
                ),
            }
            row["ask_record_sha256"] = stable_hash(row)
            rows.append(row)
            global_ordinal += 1
    if len(rows) != EXPECTED_RECORDS:
        raise RuntimeError("allocator-repair ask-plan cardinality drift")
    return tuple(rows)


def _verify_phase_c_parent_outcome(path: Path) -> dict[str, Any]:
    outcome = phase_c._read_json(path)
    phase_c._verify_self_hash(outcome, "outcome_payload_sha256", "Phase C outcome")
    next_action = dict(outcome.get("next_action") or {})
    if (
        outcome.get("status") != "PHASE_C_CLOSED_PHASE_D_HELD"
        or outcome.get("decision") != "HOLD_PHASE_D_NO_LAUNCH"
        or next_action.get("authorization") != "REQUIRES_SEPARATE_FRESH_FREEZE"
        or bool(outcome.get("phase_d_launched"))
    ):
        raise PermissionError("Phase C outcome does not authorize a fresh allocator repair")
    closure_path = Path(str(outcome["output_root"])) / "CN_JOINT_PROGRAM_PHASE_C_COMPLETE.json"
    audit_path = Path(str(outcome["final_audit_root"])) / "audit.json"
    if (
        not closure_path.is_file()
        or phase_c._sha256(closure_path) != str(outcome["closure_file_sha256"])
        or not audit_path.is_file()
        or phase_c._sha256(audit_path) != str(outcome["final_audit_file_sha256"])
    ):
        raise ValueError("Phase C parent outcome evidence binding drift")
    return outcome


def build_initial_bandit_v0(
    *,
    phase_b_schedule: Sequence[Mapping[str, Any]],
    phase_b_results: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    schedule_by_ordinal = {
        int(row["main_record_ordinal"]): dict(row) for row in phase_b_schedule
    }
    bandit = ProgramAllocatorRepairBanditV0(CAMPAIGN_ID)
    ledger: list[dict[str, Any]] = []
    for row in sorted(phase_b_results, key=lambda item: int(item["main_record_ordinal"])):
        ordinal = int(row["main_record_ordinal"])
        template_id = str(row["template_id"])
        observed = template_id != "BASE"
        outcome = None
        if observed:
            receipt = ProgramProposalReceiptV0.from_record(
                dict(schedule_by_ordinal[ordinal]["proposal_receipt"])
            )
            outcome = bandit.observe_record(receipt, row)
        ledger_row = {
            "schema_version": "cn_joint_program_allocator_repair_phase_b_seed_v0",
            "phase_b_main_record_ordinal": ordinal,
            "template_id": template_id,
            "phase_b_record_payload_sha256": str(row["record_payload_sha256"]),
            "bandit_observation_applied": observed,
            "positive_credit_applied": bool(
                observed and outcome and outcome["replay_admissible"] > 0.0
            ),
            "reason": (
                "BASE_PARITY_NOT_CREDIT_ELIGIBLE"
                if not observed
                else "ABSOLUTE_FIRST_OUTCOME_OBSERVED"
                if outcome and outcome["replay_admissible"] > 0.0
                else "REPLAY_BLOCKED_ZERO_CREDIT_RISK_OBSERVATION"
            ),
            "allocator_outcome": outcome,
            "phase_c_financial_result_used": False,
            "validation_feedback_used": False,
            "cross_campaign_state_imported": False,
        }
        ledger_row["feedback_seed_record_sha256"] = stable_hash(ledger_row)
        ledger.append(ledger_row)
    return bandit.snapshot(), tuple(ledger)


def build_prefinancial_freeze_v0(
    *,
    output_root: Path,
    phase_b_freeze_root: Path,
    phase_b_result_root: Path,
    phase_b_outcome_path: Path,
    phase_c_outcome_path: Path,
    registry_path: Path,
    accepted_field_manifest_path: Path,
    repo_sha: str,
) -> dict[str, Any]:
    root = output_root.resolve()
    if root.exists():
        raise FileExistsError(f"allocator-repair freeze root is not fresh: {root}")
    root.mkdir(parents=True)
    phase_b_freeze_root = phase_b_freeze_root.resolve()
    phase_b_result_root = phase_b_result_root.resolve()
    phase_b_outcome_path = phase_b_outcome_path.resolve()
    phase_c_outcome_path = phase_c_outcome_path.resolve()
    registry_path = registry_path.resolve()
    accepted_field_manifest_path = accepted_field_manifest_path.resolve()

    phase_b_freeze = verify_phase_b_prefinancial_freeze_v0(phase_b_freeze_root)
    phase_b_outcome, phase_b_closure = phase_c._verify_phase_b_outcome(
        phase_b_outcome_path, phase_b_result_root=phase_b_result_root
    )
    phase_c_outcome = _verify_phase_c_parent_outcome(phase_c_outcome_path)
    input_binding = phase_c._read_json(phase_b_result_root / "input_binding.json")
    phase_c._verify_self_hash(input_binding, "input_binding_sha256", "Phase B input")
    if (
        str(input_binding["phase_b_prefinancial_closure_payload_sha256"])
        != str(phase_b_freeze["closure_sha256"])
    ):
        raise ValueError("allocator-repair Phase B result/freeze binding drift")

    registry = UnifiedCapabilityRegistry.read(registry_path)
    source_rows = phase_c._read_jsonl(
        phase_b_freeze_root / "source_component_pool.jsonl"
    )
    component_rows = phase_c._session_executable_component_rows(source_rows)
    pools = phase_c._pool_by_role(component_rows)
    asks = list(build_ask_plan_v0())
    reservoir, fixtures = phase_c._build_reservoir(
        registry=registry,
        pools=pools,
        base_record_count=BASE_RECORDS,
        enhanced_record_count=RAW_RESERVOIR_PER_ENHANCED_TEMPLATE,
        raw_ordinal_offset=RAW_ORDINAL_OFFSET,
    )
    field_manifest, available_fields = phase_c._manifest_available_fields(
        accepted_field_manifest_path
    )
    materialization = phase_c.build_phase_c_component_materialization_plan_v0(
        component_rows=component_rows,
        registry=registry,
        available_fields=available_fields,
    )
    phase_c.verify_phase_c_component_materialization_plan_v0(materialization)
    if materialization["missing_required_field_ids"]:
        raise ValueError("allocator-repair freeze has unresolved materialization fields")

    phase_b_schedule = phase_c._read_jsonl(
        phase_b_freeze_root / "phase_b_uniform_schedule.jsonl"
    )
    phase_b_results = phase_c._read_jsonl(
        phase_b_result_root / "phase_b_record_results.jsonl"
    )
    initial_bandit, feedback_seed = build_initial_bandit_v0(
        phase_b_schedule=phase_b_schedule,
        phase_b_results=phase_b_results,
    )
    ProgramAllocatorRepairBanditV0.restore(initial_bandit)

    component_path = phase_c._write_jsonl(
        root / "phase_c_session_executable_component_pool.jsonl", component_rows
    )
    ask_path = phase_c._write_jsonl(root / "phase_c_ask_plan.jsonl", asks)
    reservoir_path = phase_c._write_jsonl(
        root / "phase_c_raw_program_reservoir.jsonl", reservoir
    )
    fixture_path = phase_c._write_jsonl(
        root / "phase_c_template_compile_fixtures.jsonl", fixtures
    )
    seed_path = phase_c._write_jsonl(root / "phase_b_feedback_seed.jsonl", feedback_seed)
    bandit_path = phase_c._write_json(root / "initial_bandit_state.json", initial_bandit)
    materialization_path = phase_c._write_json(
        root / "phase_c_materialization_plan.json", materialization
    )
    contract = phase_c._self_hashed(
        {
            "schema_version": "cn_joint_program_allocator_repair_run_contract_v0",
            "status": "ALLOCATOR_REPAIR_INPUTS_FROZEN_BEFORE_FINANCIAL_READ",
            "repo_sha": str(repo_sha),
            "campaign_id": CAMPAIGN_ID,
            "batch_id": BATCH_ID,
            "phase_b_outcome_path": str(phase_b_outcome_path),
            "phase_b_outcome_file_sha256": phase_c._sha256(phase_b_outcome_path),
            "phase_b_outcome_payload_sha256": str(
                phase_b_outcome["receipt_payload_sha256"]
            ),
            "phase_b_result_root": str(phase_b_result_root),
            "phase_b_result_closure_payload_sha256": str(
                phase_b_closure["closure_payload_sha256"]
            ),
            "phase_b_freeze_root": str(phase_b_freeze_root),
            "phase_b_freeze_closure_payload_sha256": str(
                phase_b_freeze["closure_sha256"]
            ),
            "phase_c_parent_outcome_path": str(phase_c_outcome_path),
            "phase_c_parent_outcome_file_sha256": phase_c._sha256(phase_c_outcome_path),
            "phase_c_parent_outcome_payload_sha256": str(
                phase_c_outcome["outcome_payload_sha256"]
            ),
            "phase_c_financial_records_imported": False,
            "registry_path": str(registry_path),
            "registry_file_sha256": phase_c._sha256(registry_path),
            "accepted_field_manifest_path": str(accepted_field_manifest_path),
            "accepted_field_manifest_file_sha256": phase_c._sha256(
                accepted_field_manifest_path
            ),
            "accepted_field_manifest_payload_sha256": str(
                field_manifest["manifest_hash"]
            ),
            "session_executable_component_pool_file_sha256": phase_c._sha256(
                component_path
            ),
            "session_executable_component_count": len(component_rows),
            "ask_plan_file_sha256": phase_c._sha256(ask_path),
            "raw_program_reservoir_file_sha256": phase_c._sha256(reservoir_path),
            "template_compile_fixtures_file_sha256": phase_c._sha256(fixture_path),
            "phase_b_feedback_seed_file_sha256": phase_c._sha256(seed_path),
            "initial_bandit_state_file_sha256": phase_c._sha256(bandit_path),
            "initial_bandit_state_payload_sha256": str(
                initial_bandit["bandit_state_sha256"]
            ),
            "materialization_plan_file_sha256": phase_c._sha256(materialization_path),
            "materialization_plan_payload_sha256": str(materialization["plan_sha256"]),
            "experiment_seed_sha256": EXPERIMENT_SEED_SHA256,
            "raw_ordinal_offset": RAW_ORDINAL_OFFSET,
            "main_record_count": EXPECTED_RECORDS,
            "base_parity_record_count": BASE_RECORDS,
            "enhanced_template_record_count": ENHANCED_RECORDS_PER_TEMPLATE,
            "template_order": list(TEMPLATE_ORDER),
            "template_quotas": {template_id: 32 for template_id in TEMPLATE_ORDER},
            "enhanced_arm_quotas": {
                "UNIFORM_FRESH": 12,
                "REVISED_EXPLOIT": 12,
                "NOVELTY_RESERVE": 8,
            },
            "initial_uniform_baseline_per_enhanced_template": 12,
            "raw_reservoir_per_enhanced_template": RAW_RESERVOIR_PER_ENHANCED_TEMPLATE,
            "maximum_variants_per_base_per_template": (
                MAX_VARIANTS_PER_BASE_PER_TEMPLATE
            ),
            "minimum_base_identities_per_template": MIN_BASE_IDENTITIES_PER_TEMPLATE,
            "no_early_template_cancellation": True,
            "adaptive_budget_reallocation": False,
            "fixed_arm_floors": True,
            "bandit_version": BANDIT_VERSION,
            "bandit_policy_id": BANDIT_POLICY_ID,
            "selection_hierarchy": list(OUTCOME_FIELDS),
            "blocked_positive_credit": False,
            "blocked_risk_observation": True,
            "bandit_feedback_source": "PHASE_B_AND_CURRENT_CANARY_DEVELOPMENT_ONLY",
            "cross_campaign_optimizer_state_import": False,
            "route_local_tpe_authority_unchanged": True,
            "portfolio_decoder_id": "TOPK_10_EQUAL",
            "resource_profile": RESOURCE_PROFILE,
            "entitlement_threads": ENTITLEMENT_THREADS,
            "executor_backend": "PROCESS_POOL",
            "executor_workers": EXECUTOR_WORKERS,
            "executor_lifecycle": "CHECKPOINT_SCOPED_RECYCLE",
            "native_threads_per_worker": NATIVE_THREADS_PER_WORKER,
            "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
            "cache_cap_bytes": CACHE_CAP_BYTES,
            "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
            "checkpoint_count": CHECKPOINT_COUNT,
            "decision_gates": {
                "productive_rate_delta_vs_uniform_minimum": 0.05,
                "all_four_positive_rate_strictly_above_uniform": True,
                "primary_positive_rates_materially_worse_allowed": False,
                "three_window_stability_worse_allowed": False,
                "turnover_efficiency_worse_allowed": False,
                "blocker_rate_worse_allowed": False,
                "minimum_improved_enhanced_templates": 4,
            },
            "incomplete_financial_result_reuse": False,
            "financial_evaluation_executed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "formal_optimizer_authority_write": False,
            "formal_scheduler_authority_write": False,
            "archive_write": False,
            "promotion_write": False,
            "automatic_phase_d_launch": False,
        },
        "run_contract_sha256",
    )
    contract_path = phase_c._write_json(root / "phase_c_run_contract.json", contract)
    access = phase_c._self_hashed(
        {
            "schema_version": "cn_joint_program_allocator_repair_access_v0",
            "financial_evaluation_executed": False,
            "market_price_rows_read": 0,
            "label_rows_read": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "phase_b_development_feedback_seeded": True,
            "phase_c_financial_feedback_seeded": False,
            "cross_campaign_optimizer_state_imported": False,
        },
        "access_ledger_sha256",
    )
    access_path = phase_c._write_json(root / "access_ledger.json", access)
    summary = phase_c._self_hashed(
        {
            "schema_version": "cn_joint_program_allocator_repair_summary_v0",
            "status": "ALLOCATOR_REPAIR_PREFINANCIAL_READY",
            "main_record_count": EXPECTED_RECORDS,
            "checkpoint_count": CHECKPOINT_COUNT,
            "reservoir_record_count": len(reservoir),
            "initial_bandit_observations": int(initial_bandit["observations"]),
            "required_physical_leaf_count": int(
                materialization["required_physical_leaf_count"]
            ),
            "missing_required_field_ids": list(
                materialization["missing_required_field_ids"]
            ),
            "zero_financial_reads": True,
            "sealed_reads": 0,
        },
        "summary_payload_sha256",
    )
    summary_path = phase_c._write_json(root / "summary.json", summary)
    artifacts = [
        component_path,
        ask_path,
        reservoir_path,
        fixture_path,
        seed_path,
        bandit_path,
        materialization_path,
        contract_path,
        access_path,
        summary_path,
    ]
    manifest = phase_c._self_hashed(
        {
            "schema_version": "cn_joint_program_allocator_repair_artifacts_v0",
            "artifacts": [phase_c._artifact(path, root=root) for path in artifacts],
        },
        "artifact_manifest_sha256",
    )
    manifest_path = phase_c._write_json(root / "ARTIFACT_MANIFEST.json", manifest)
    closure = phase_c._self_hashed(
        {
            "schema_version": FREEZE_SCHEMA,
            "status": FREEZE_STATUS,
            "repo_sha": str(repo_sha),
            "output_root": str(root),
            "main_record_count": EXPECTED_RECORDS,
            "checkpoint_count": CHECKPOINT_COUNT,
            "reservoir_record_count": len(reservoir),
            "initial_bandit_observations": int(initial_bandit["observations"]),
            "phase_c_financial_records_imported": False,
            "artifact_manifest": phase_c._artifact(manifest_path, root=root),
            "artifact_count": len(manifest["artifacts"]),
            "financial_evaluation_executed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "closure_sha256",
    )
    return phase_c._read_json(
        phase_c._write_json(root / FREEZE_CLOSURE_NAME, closure)
    )


def verify_prefinancial_freeze_v0(root: Path) -> dict[str, Any]:
    root = root.resolve()
    closure = phase_c._read_json(root / FREEZE_CLOSURE_NAME)
    phase_c._verify_self_hash(closure, "closure_sha256", "allocator-repair closure")
    if closure.get("schema_version") != FREEZE_SCHEMA or closure.get("status") != FREEZE_STATUS:
        raise ValueError("allocator-repair freeze authority drift")
    manifest_path = root / str(closure["artifact_manifest"]["relative_path"])
    if phase_c._sha256(manifest_path) != str(closure["artifact_manifest"]["sha256"]):
        raise ValueError("allocator-repair artifact manifest drift")
    manifest = phase_c._read_json(manifest_path)
    phase_c._verify_self_hash(
        manifest, "artifact_manifest_sha256", "allocator-repair artifacts"
    )
    for artifact in manifest["artifacts"]:
        path = root / str(artifact["relative_path"])
        if (
            not path.is_file()
            or path.stat().st_size != int(artifact["size_bytes"])
            or phase_c._sha256(path) != str(artifact["sha256"])
        ):
            raise ValueError(f"allocator-repair artifact drift: {path}")
    asks = phase_c._read_jsonl(root / "phase_c_ask_plan.jsonl")
    if asks != list(build_ask_plan_v0()):
        raise ValueError("allocator-repair ask-plan canonical replay drift")
    counts = Counter(
        (str(row["template_id"]), str(row["generation_arm"])) for row in asks
    )
    for template_id in ENHANCED_TEMPLATE_ORDER:
        if {
            arm: counts[(template_id, arm)]
            for arm in ("UNIFORM_FRESH", "REVISED_EXPLOIT", "NOVELTY_RESERVE")
        } != {"UNIFORM_FRESH": 12, "REVISED_EXPLOIT": 12, "NOVELTY_RESERVE": 8}:
            raise ValueError("allocator-repair enhanced arm quota drift")
    reservoir = phase_c._read_jsonl(root / "phase_c_raw_program_reservoir.jsonl")
    reservoir_counts = Counter(str(row["template_id"]) for row in reservoir)
    expected_reservoir_counts = {
        "BASE": BASE_RECORDS,
        **{
            template_id: RAW_RESERVOIR_PER_ENHANCED_TEMPLATE
            for template_id in ENHANCED_TEMPLATE_ORDER
        },
    }
    if dict(reservoir_counts) != expected_reservoir_counts:
        raise ValueError("allocator-repair reservoir quota drift")
    for row in reservoir:
        body = dict(row)
        claimed = str(body.pop("reservoir_record_sha256", ""))
        if stable_hash(body) != claimed:
            raise ValueError("allocator-repair reservoir self-hash drift")
    components = phase_c._read_jsonl(
        root / "phase_c_session_executable_component_pool.jsonl"
    )
    if components != phase_c._session_executable_component_rows(components):
        raise ValueError("allocator-repair component-pool drift")
    pools = phase_c._pool_by_role(components)
    base_counts = Counter(
        str(row["components"]["base"]["component_id"])
        for row in reservoir
        if row["template_id"] == "BASE"
    )
    if (
        len(base_counts) < MIN_BASE_IDENTITIES_PER_TEMPLATE
        or max(base_counts.values()) > math.ceil(BASE_RECORDS / len(pools["base"]))
    ):
        raise ValueError("allocator-repair BASE reservoir diversity drift")
    ProgramAllocatorRepairBanditV0.restore(
        phase_c._read_json(root / "initial_bandit_state.json")
    )
    materialization = phase_c._read_json(root / "phase_c_materialization_plan.json")
    phase_c.verify_phase_c_component_materialization_plan_v0(materialization)
    if materialization["missing_required_field_ids"]:
        raise ValueError("allocator-repair materialization coverage drift")
    contract = phase_c._read_json(root / "phase_c_run_contract.json")
    phase_c._verify_self_hash(contract, "run_contract_sha256", "allocator-repair contract")
    if (
        contract.get("bandit_policy_id") != BANDIT_POLICY_ID
        or bool(contract.get("phase_c_financial_records_imported"))
        or bool(contract.get("adaptive_budget_reallocation"))
        or not bool(contract.get("fixed_arm_floors"))
    ):
        raise ValueError("allocator-repair contract policy drift")
    access = phase_c._read_json(root / "access_ledger.json")
    phase_c._verify_self_hash(access, "access_ledger_sha256", "allocator-repair access")
    if bool(access["financial_evaluation_executed"]) or any(
        int(access[key])
        for key in (
            "market_price_rows_read",
            "label_rows_read",
            "validation_reads",
            "holdout_reads",
            "historical_2023_reads",
            "forward_b_reads",
            "forward_2026_reads",
        )
    ):
        raise PermissionError("allocator-repair freeze records prohibited reads")
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
    freeze.add_argument("--registry", required=True, type=Path)
    freeze.add_argument("--accepted-field-manifest", required=True, type=Path)
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
            registry_path=args.registry,
            accepted_field_manifest_path=args.accepted_field_manifest,
            repo_sha=str(args.repo_sha),
        )
    else:
        closure = verify_prefinancial_freeze_v0(args.freeze_root)
    print(json.dumps(closure, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
