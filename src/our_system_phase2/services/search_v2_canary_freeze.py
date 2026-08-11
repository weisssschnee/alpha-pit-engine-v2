"""Zero-financial pre-run freeze for the Search Engine V2 canary."""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.runtime import cn_joint_program_phase_c_v0 as phase_c
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import (
    CACHE_CAP_BYTES,
    ENTITLEMENT_THREADS,
    EXECUTOR_WORKERS,
    FREEZE_CLOSURE_NAME as PHASE_B_FREEZE_CLOSURE_NAME,
    MINIMUM_FREE_MEMORY_BYTES,
    NATIVE_THREADS_PER_WORKER,
    RESOURCE_PROFILE,
    TEMPLATE_ORDER,
    verify_phase_b_prefinancial_freeze_v0,
)
from our_system_phase2.runtime.cn_joint_program_search_v2_canary import (
    ARM_CONDITIONAL_UPLIFT,
    ARM_NOVELTY,
    ARM_UNIFORM,
    BATCH_ID,
    CAMPAIGN_ID,
    CANARY_PROFILE,
    CHECKPOINT_COUNT,
    EXPECTED_RECORDS,
    MAX_VARIANTS_PER_BASE_PER_TEMPLATE,
    MIN_BASE_IDENTITIES_PER_TEMPLATE,
    PROSPECTIVE_SUCCESS_GATES,
    RAW_RESERVOIR_PER_ENHANCED_TEMPLATE,
    RECORDS_PER_CHECKPOINT,
    build_ask_plan_v1,
    canary_provenance_v1,
)
from our_system_phase2.services.search_v2_scheduler import (
    SCHEDULER_POLICY_ID,
    SCHEDULER_VERSION,
    SearchV2SchedulerV1,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


FREEZE_SCHEMA = "cn_search_engine_v2_canary_prefinancial_freeze_v1"
FREEZE_STATUS = "SEARCH_ENGINE_V2_CANARY_PREFINANCIAL_FREEZE_COMPLETE"
FREEZE_CLOSURE_NAME = "SEARCH_ENGINE_V2_CANARY_PREFINANCIAL_FREEZE_COMPLETE.json"
RUN_CONTRACT_SCHEMA = "cn_search_engine_v2_canary_run_contract_v1"
ACCESS_SCHEMA = "cn_search_engine_v2_canary_access_v1"
ARTIFACT_MANIFEST_SCHEMA = "cn_search_engine_v2_canary_artifacts_v1"
PHASE_B_RESULT_CLOSURE_NAME = "CN_JOINT_PROGRAM_PHASE_B_COMPLETE.json"


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    expected = str(body.pop(field, ""))
    if not expected or stable_hash(body) != expected:
        raise ValueError(f"{label} self-hash drift")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify_phase_b_field_manifest_binding(
    *,
    accepted_field_manifest_path: Path,
    field_manifest: Mapping[str, Any],
    phase_b_input: Mapping[str, Any],
) -> None:
    if (
        phase_c._sha256(accepted_field_manifest_path)
        != str(phase_b_input["train_field_manifest_sha256"])
        or str(field_manifest["manifest_hash"])
        != str(phase_b_input["train_field_manifest_payload_sha256"])
    ):
        raise ValueError(
            "Search V2 field manifest differs from accepted Phase B input"
        )


def _verify_phase_b_source_authority(
    *,
    phase_b_freeze_root: Path,
    phase_b_result_root: Path,
    phase_b_outcome_path: Path,
    authorization: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    phase_b_freeze = verify_phase_b_prefinancial_freeze_v0(phase_b_freeze_root)
    outcome = _read_json(phase_b_outcome_path)
    _verify_self_hash(outcome, "receipt_payload_sha256", "Phase B compact outcome")
    closure_binding = dict(outcome.get("closure") or {})
    if (
        str(outcome.get("status") or "")
        != "CN_JOINT_PROGRAM_ROLLING_SEARCH_V0_PHASE_B_ACCEPTED"
        or Path(str(outcome.get("phase_b_root") or "")).resolve()
        != phase_b_result_root
        or str(closure_binding.get("file_sha256") or "")
        != str(authorization["source_phase_b_closure_file_sha256"])
        or str(closure_binding.get("payload_sha256") or "")
        != str(authorization["source_phase_b_closure_payload_sha256"])
    ):
        raise ValueError("Search V2 Phase B compact outcome binding drift")
    result_closure_path = phase_b_result_root / PHASE_B_RESULT_CLOSURE_NAME
    if (
        not result_closure_path.is_file()
        or phase_c._sha256(result_closure_path)
        != str(authorization["source_phase_b_closure_file_sha256"])
    ):
        raise ValueError("Search V2 Phase B result closure binding drift")
    result_closure = _read_json(result_closure_path)
    _verify_self_hash(result_closure, "closure_payload_sha256", "Phase B closure")
    if str(result_closure["closure_payload_sha256"]) != str(
        authorization["source_phase_b_closure_payload_sha256"]
    ):
        raise ValueError("Search V2 Phase B closure payload drift")

    input_binding_path = phase_b_result_root / "input_binding.json"
    input_binding = _read_json(input_binding_path)
    _verify_self_hash(input_binding, "input_binding_sha256", "Phase B input binding")
    freeze_closure_path = phase_b_freeze_root / PHASE_B_FREEZE_CLOSURE_NAME
    if (
        phase_c._sha256(freeze_closure_path)
        != str(input_binding["phase_b_prefinancial_closure_file_sha256"])
        or str(phase_b_freeze["closure_sha256"])
        != str(input_binding["phase_b_prefinancial_closure_payload_sha256"])
    ):
        raise ValueError("Search V2 Phase B source freeze/result binding drift")
    return phase_b_freeze, outcome, input_binding


def build_prefinancial_freeze_v1(
    *,
    output_root: Path,
    phase_b_freeze_root: Path,
    phase_b_result_root: Path,
    phase_b_outcome_path: Path,
    registry_path: Path,
    accepted_field_manifest_path: Path,
    node_resource_capacity_path: Path,
    repo_sha: str,
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    root = output_root.resolve()
    if root.exists():
        raise FileExistsError(f"Search V2 prefinancial freeze root exists: {root}")
    root.mkdir(parents=True)
    phase_b_freeze_root = phase_b_freeze_root.resolve()
    phase_b_result_root = phase_b_result_root.resolve()
    phase_b_outcome_path = phase_b_outcome_path.resolve()
    registry_path = registry_path.resolve()
    accepted_field_manifest_path = accepted_field_manifest_path.resolve()
    node_resource_capacity_path = node_resource_capacity_path.resolve()

    phase_b_freeze, phase_b_outcome, phase_b_input = _verify_phase_b_source_authority(
        phase_b_freeze_root=phase_b_freeze_root,
        phase_b_result_root=phase_b_result_root,
        phase_b_outcome_path=phase_b_outcome_path,
        authorization=authorization,
    )
    if phase_c._sha256(registry_path) != str(phase_b_input["registry_sha256"]):
        raise ValueError("Search V2 registry differs from accepted Phase B input")
    if phase_c._sha256(node_resource_capacity_path) != str(
        phase_b_input["node_resource_capacity_sha256"]
    ):
        raise ValueError("Search V2 resource capacity differs from Phase B input")
    capacity = _read_json(node_resource_capacity_path)
    _verify_self_hash(capacity, "capacity_manifest_sha256", "node resource capacity")
    profile = dict(capacity.get("profiles", {}).get(RESOURCE_PROFILE) or {})
    if (
        int(profile.get("cpu_threads") or 0) != ENTITLEMENT_THREADS
        or int(profile.get("minimum_free_memory_bytes") or 0)
        != MINIMUM_FREE_MEMORY_BYTES
    ):
        raise ValueError("Search V2 resource profile drift")

    registry = UnifiedCapabilityRegistry.read(registry_path)
    source_rows = phase_c._read_jsonl(
        phase_b_freeze_root / "source_component_pool.jsonl"
    )
    component_rows = phase_c._session_executable_component_rows(source_rows)
    pools = phase_c._pool_by_role(component_rows)
    seed_sha = stable_hash(
        {"campaign_id": CAMPAIGN_ID, "batch_id": BATCH_ID, "version": 1}
    )
    reservoir, fixtures = phase_c._build_reservoir(
        registry=registry,
        pools=pools,
        base_record_count=64,
        enhanced_record_count=RAW_RESERVOIR_PER_ENHANCED_TEMPLATE,
        raw_ordinal_offset=int(seed_sha[:8], 16),
    )
    field_manifest, available_fields = phase_c._manifest_available_fields(
        accepted_field_manifest_path
    )
    _verify_phase_b_field_manifest_binding(
        accepted_field_manifest_path=accepted_field_manifest_path,
        field_manifest=field_manifest,
        phase_b_input=phase_b_input,
    )
    materialization = phase_c.build_phase_c_component_materialization_plan_v0(
        component_rows=component_rows,
        registry=registry,
        available_fields=available_fields,
    )
    phase_c.verify_phase_c_component_materialization_plan_v0(materialization)
    if materialization["missing_required_field_ids"]:
        raise ValueError("Search V2 freeze has unresolved materialization fields")

    asks = list(build_ask_plan_v1())
    initial_scheduler = SearchV2SchedulerV1.fresh(
        CAMPAIGN_ID, canary_provenance_v1()
    ).snapshot()
    feedback_seed: list[dict[str, Any]] = []
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
    feedback_path = phase_c._write_jsonl(
        root / "development_feedback_seed.jsonl", feedback_seed
    )
    state_path = phase_c._write_json(
        root / "initial_bandit_state.json", initial_scheduler
    )
    materialization_path = phase_c._write_json(
        root / "phase_c_materialization_plan.json", materialization
    )
    template_quotas = Counter(str(row["template_id"]) for row in asks)
    arm_quotas = Counter(str(row["generation_arm"]) for row in asks)
    contract = phase_c._self_hashed(
        {
            "schema_version": RUN_CONTRACT_SCHEMA,
            "status": "SEARCH_V2_INPUTS_FROZEN_BEFORE_FINANCIAL_READ",
            "repo_sha": str(repo_sha),
            "campaign_id": CAMPAIGN_ID,
            "campaign_profile": CANARY_PROFILE,
            "batch_id": BATCH_ID,
            "campaign_authorization_payload_sha256": str(
                authorization["authorization_payload_sha256"]
            ),
            "ask_plan_sha256": str(authorization["ask_plan_sha256"]),
            "phase_b_outcome_path": str(phase_b_outcome_path),
            "phase_b_outcome_file_sha256": phase_c._sha256(phase_b_outcome_path),
            "phase_b_outcome_payload_sha256": str(
                phase_b_outcome["receipt_payload_sha256"]
            ),
            "phase_b_result_root": str(phase_b_result_root),
            "phase_b_result_closure_file_sha256": str(
                authorization["source_phase_b_closure_file_sha256"]
            ),
            "phase_b_result_closure_payload_sha256": str(
                authorization["source_phase_b_closure_payload_sha256"]
            ),
            "phase_b_input_binding_payload_sha256": str(
                phase_b_input["input_binding_sha256"]
            ),
            "phase_b_freeze_root": str(phase_b_freeze_root),
            "phase_b_freeze_closure_payload_sha256": str(
                phase_b_freeze["closure_sha256"]
            ),
            "registry_path": str(registry_path),
            "registry_file_sha256": phase_c._sha256(registry_path),
            "accepted_field_manifest_path": str(accepted_field_manifest_path),
            "accepted_field_manifest_file_sha256": phase_c._sha256(
                accepted_field_manifest_path
            ),
            "accepted_field_manifest_payload_sha256": str(
                field_manifest["manifest_hash"]
            ),
            "node_resource_capacity_path": str(node_resource_capacity_path),
            "node_resource_capacity_file_sha256": phase_c._sha256(
                node_resource_capacity_path
            ),
            "session_executable_component_pool_file_sha256": phase_c._sha256(
                component_path
            ),
            "ask_plan_file_sha256": phase_c._sha256(ask_path),
            "raw_program_reservoir_file_sha256": phase_c._sha256(reservoir_path),
            "template_compile_fixtures_file_sha256": phase_c._sha256(fixture_path),
            "development_feedback_seed_file_sha256": phase_c._sha256(feedback_path),
            "initial_bandit_state_file_sha256": phase_c._sha256(state_path),
            "initial_bandit_state_payload_sha256": str(
                initial_scheduler["bandit_state_sha256"]
            ),
            "materialization_plan_file_sha256": phase_c._sha256(
                materialization_path
            ),
            "materialization_plan_payload_sha256": str(
                materialization["plan_sha256"]
            ),
            "main_record_count": EXPECTED_RECORDS,
            "checkpoint_count": CHECKPOINT_COUNT,
            "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
            "template_order": list(TEMPLATE_ORDER),
            "template_quotas": dict(sorted(template_quotas.items())),
            "arm_quotas": dict(sorted(arm_quotas.items())),
            "enhanced_arm_quotas": {
                ARM_UNIFORM: 32,
                ARM_CONDITIONAL_UPLIFT: 24,
                ARM_NOVELTY: 8,
            },
            "initial_uniform_baseline_per_enhanced_template": 32,
            "raw_reservoir_per_enhanced_template": (
                RAW_RESERVOIR_PER_ENHANCED_TEMPLATE
            ),
            "maximum_variants_per_base_per_template": (
                MAX_VARIANTS_PER_BASE_PER_TEMPLATE
            ),
            "minimum_base_identities_per_template": MIN_BASE_IDENTITIES_PER_TEMPLATE,
            "no_early_template_cancellation": True,
            "adaptive_budget_reallocation": False,
            "fixed_arm_floors": True,
            "bandit_version": SCHEDULER_VERSION,
            "bandit_policy_id": SCHEDULER_POLICY_ID,
            "selection_hierarchy": list(initial_scheduler["selection_hierarchy"]),
            "absolute_admission_and_conditional_uplift_separate": True,
            "scalar_absolute_plus_uplift_reward": None,
            "component_credit_authoritative": False,
            "development_feedback_provenance": canary_provenance_v1(),
            "serialized_optimizer_state_imported": False,
            "development_financial_observations_imported": False,
            "development_observation_count": 0,
            "candidate_results_imported": False,
            "factor_statistics_imported": False,
            "behavior_statistics_imported": False,
            "template_classification_imported": False,
            "route_local_tpe_authority_unchanged": True,
            "formal_development_search_authority_replaced": False,
            "portfolio_decoder_id": "TOPK_10_EQUAL",
            "resource_profile": RESOURCE_PROFILE,
            "entitlement_threads": ENTITLEMENT_THREADS,
            "executor_backend": "PROCESS_POOL",
            "executor_workers": EXECUTOR_WORKERS,
            "executor_lifecycle": "CHECKPOINT_SCOPED_RECYCLE",
            "native_threads_per_worker": NATIVE_THREADS_PER_WORKER,
            "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
            "cache_cap_bytes": CACHE_CAP_BYTES,
            "decision_gates": PROSPECTIVE_SUCCESS_GATES,
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
            "automatic_successor_launch": False,
        },
        "run_contract_sha256",
    )
    contract_path = phase_c._write_json(root / "phase_c_run_contract.json", contract)
    access = phase_c._self_hashed(
        {
            "schema_version": ACCESS_SCHEMA,
            "financial_evaluation_executed": False,
            "market_price_rows_read": 0,
            "label_rows_read": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "phase_b_development_financial_observations_imported": False,
            "phase_c_development_financial_observations_imported": False,
            "phase_d_development_financial_observations_imported": False,
            "serialized_optimizer_state_imported": False,
            "development_observation_count": 0,
            "development_feedback_provenance": canary_provenance_v1(),
        },
        "access_ledger_sha256",
    )
    access_path = phase_c._write_json(root / "access_ledger.json", access)
    artifacts = [
        component_path,
        ask_path,
        reservoir_path,
        fixture_path,
        feedback_path,
        state_path,
        materialization_path,
        contract_path,
        access_path,
    ]
    manifest = phase_c._self_hashed(
        {
            "schema_version": ARTIFACT_MANIFEST_SCHEMA,
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
            "campaign_id": CAMPAIGN_ID,
            "campaign_profile": CANARY_PROFILE,
            "main_record_count": EXPECTED_RECORDS,
            "checkpoint_count": CHECKPOINT_COUNT,
            "reservoir_record_count": len(reservoir),
            "initial_scheduler_observations": 0,
            "initial_conditional_uplift_observations": 0,
            "artifact_manifest": phase_c._artifact(manifest_path, root=root),
            "artifact_count": len(manifest["artifacts"]),
            "financial_evaluation_executed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "canary_run": False,
        },
        "closure_sha256",
    )
    return phase_c._read_json(
        phase_c._write_json(root / FREEZE_CLOSURE_NAME, closure)
    )


def verify_prefinancial_freeze_v1(root: Path) -> dict[str, Any]:
    root = root.resolve()
    closure = _read_json(root / FREEZE_CLOSURE_NAME)
    _verify_self_hash(closure, "closure_sha256", "Search V2 freeze closure")
    if (
        closure.get("schema_version") != FREEZE_SCHEMA
        or closure.get("status") != FREEZE_STATUS
        or int(closure.get("main_record_count") or 0) != EXPECTED_RECORDS
        or int(closure.get("initial_scheduler_observations") or -1) != 0
        or bool(closure.get("financial_evaluation_executed"))
        or bool(closure.get("canary_run"))
    ):
        raise ValueError("Search V2 freeze closure contract drift")
    manifest_path = root / str(closure["artifact_manifest"]["relative_path"])
    if phase_c._sha256(manifest_path) != str(
        closure["artifact_manifest"]["sha256"]
    ):
        raise ValueError("Search V2 freeze artifact manifest drift")
    manifest = _read_json(manifest_path)
    _verify_self_hash(manifest, "artifact_manifest_sha256", "Search V2 manifest")
    for artifact in manifest["artifacts"]:
        path = root / str(artifact["relative_path"])
        if (
            not path.is_file()
            or path.stat().st_size != int(artifact["size_bytes"])
            or phase_c._sha256(path) != str(artifact["sha256"])
        ):
            raise ValueError(f"Search V2 freeze artifact drift: {path}")
    asks = phase_c._read_jsonl(root / "phase_c_ask_plan.jsonl")
    if asks != list(build_ask_plan_v1()):
        raise ValueError("Search V2 ask-plan canonical replay drift")
    scheduler = SearchV2SchedulerV1.restore(
        _read_json(root / "initial_bandit_state.json"),
        expected_campaign_id=CAMPAIGN_ID,
    )
    if scheduler.observations != 0 or scheduler.conditional_uplift_observations != 0:
        raise ValueError("Search V2 initial scheduler is not fresh")
    if phase_c._read_jsonl(root / "development_feedback_seed.jsonl"):
        raise ValueError("Search V2 freeze imported development feedback")
    contract = _read_json(root / "phase_c_run_contract.json")
    _verify_self_hash(contract, "run_contract_sha256", "Search V2 run contract")
    if (
        contract.get("decision_gates") != PROSPECTIVE_SUCCESS_GATES
        or int(contract.get("development_observation_count") or -1) != 0
        or bool(contract.get("serialized_optimizer_state_imported"))
        or bool(contract.get("development_financial_observations_imported"))
        or contract.get("scalar_absolute_plus_uplift_reward") is not None
        or not bool(contract.get("route_local_tpe_authority_unchanged"))
        or bool(contract.get("formal_development_search_authority_replaced"))
    ):
        raise ValueError("Search V2 run-contract search-control drift")
    access = _read_json(root / "access_ledger.json")
    _verify_self_hash(access, "access_ledger_sha256", "Search V2 access ledger")
    if bool(access.get("financial_evaluation_executed")) or any(
        int(access.get(key) or 0)
        for key in (
            "market_price_rows_read",
            "label_rows_read",
            "validation_reads",
            "holdout_reads",
            "historical_2023_reads",
            "forward_b_reads",
            "forward_2026_reads",
            "development_observation_count",
        )
    ):
        raise PermissionError("Search V2 prefinancial freeze records prohibited reads")
    materialization = _read_json(root / "phase_c_materialization_plan.json")
    phase_c.verify_phase_c_component_materialization_plan_v0(materialization)
    if materialization["missing_required_field_ids"]:
        raise ValueError("Search V2 materialization coverage drift")
    return closure
