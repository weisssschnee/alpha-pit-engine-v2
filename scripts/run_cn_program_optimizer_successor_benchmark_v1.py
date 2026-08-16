from __future__ import annotations

import argparse
import copy
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
import gc
import json
import math
from pathlib import Path
import platform
import statistics
import time
from typing import Any, Mapping, Sequence

import psutil

PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_joint_program_phase_b_v0 as phase_b
from our_system_phase2.runtime.cn_program_optimizer_successor_benchmark_v1 import (
    AUTHORIZED_HOST,
    CAMPAIGN_ID,
    CAMPAIGN_PROFILE,
    FROZEN_BASE_PROGRAM_COUNT,
    FROZEN_ENHANCED_PROGRAM_COUNT,
    FROZEN_PROGRAM_SPACE_COUNT,
    FROZEN_PROGRAM_SPACE_SHA256,
    PRIOR_EXACT_COUNT,
    PRIOR_EXACT_IDENTITIES_SHA256,
    PRIOR_FREEZE_PAYLOAD_SHA256,
    SEEDS,
    SOURCE_COMPONENT_POOL_SHA256,
    SOURCE_EXECUTION_CONTRACT_PATH,
    SOURCE_EXECUTION_CONTRACT_SHA256,
    SOURCE_FREEZE_CLOSURE,
    SOURCE_FREEZE_CLOSURE_FILE_SHA256,
    SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256,
    SOURCE_FREEZE_ROOT,
    SOURCE_NODE_CAPACITY_PATH,
    SOURCE_NODE_CAPACITY_SHA256,
    SOURCE_RAW_RESERVOIR_SHA256,
    SOURCE_REGISTRY_PATH,
    SOURCE_REGISTRY_SHA256,
    SOURCE_RUN_CONTRACT_FILE_SHA256,
    SOURCE_RUN_CONTRACT_PAYLOAD_SHA256,
    SOURCE_TRAIN_FIELD_ROOT,
    SOURCE_TRAIN_PRICE_ROOT,
    SURROGATE_CONFIG,
    TPE_CONFIG,
)
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import TEMPLATE_ORDER
from our_system_phase2.services.candidate_program_proposal_v0 import (
    CandidateProgramProposalAdapterV0,
)
from our_system_phase2.services.candidate_program_v1 import ProgramCompilerV1
from our_system_phase2.services.program_optimizer_successor_benchmark_v1 import (
    BOOTSTRAP_WAVES,
    POLICIES,
    POLICY_D1,
    POLICY_D2,
    POLICY_TPE,
    POLICY_UNIFORM,
    POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY_DENOMINATOR,
    POST_BOOTSTRAP_EFFICIENCY_DENOMINATOR,
    TOTAL_POLICY_EFFICIENCY_DENOMINATOR,
    TOTAL_WAVES,
    UNIFORM_FLOOR_WAVES,
    PhysicalProgramResultV1,
    ProgramOptimizerSuccessorBenchmarkV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    normalized_program_gene_identity_v1,
)
from our_system_phase2.services.program_tournament_freeze_v1 import (
    verify_phase_freeze_v1,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import (
    MATCHED_CONTROL_CONTRACT_ID,
    conditional_uplift_credit,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


STATUS = "CN_PROGRAM_OPTIMIZER_SUCCESSOR_BENCHMARK_COMPLETE"
CLOSURE_NAME = "CN_PROGRAM_OPTIMIZER_SUCCESSOR_BENCHMARK_COMPLETE.json"
ENHANCED_TEMPLATES = tuple(template for template in TEMPLATE_ORDER if template != "BASE")


def _norm(path: Path | str) -> str:
    return str(Path(path).resolve()).replace("/", "\\").lower()


def _artifact(path: Path, root: Path) -> dict[str, Any]:
    resolved = path.resolve()
    return {
        "path": str(resolved.relative_to(root.resolve())).replace("\\", "/"),
        "bytes": resolved.stat().st_size,
        "sha256": engine._sha256(resolved),
    }


def _read_prior_freeze(
    path: Path,
    *,
    expected_payload_sha256: str = PRIOR_FREEZE_PAYLOAD_SHA256,
    expected_count: int = PRIOR_EXACT_COUNT,
    expected_exact_identities_sha256: str = PRIOR_EXACT_IDENTITIES_SHA256,
    identity_field: str = "prior_exact_identities",
) -> dict[str, Any]:
    payload = engine._read_json(path.resolve())
    body = dict(payload)
    claimed = str(body.pop("freeze_payload_sha256", ""))
    if claimed != stable_hash(body) or claimed != str(expected_payload_sha256):
        raise RuntimeError("SUCCESSOR_PRIOR_FREEZE_HASH_DRIFT")
    ids = tuple(map(str, payload.get(str(identity_field)) or ()))
    if (
        len(ids) != int(expected_count)
        or len(set(ids)) != int(expected_count)
        or stable_hash(list(ids)) != str(expected_exact_identities_sha256)
    ):
        raise RuntimeError("SUCCESSOR_PRIOR_FREEZE_IDENTITY_DRIFT")
    normalized = dict(payload)
    normalized["prior_exact_identities"] = list(ids)
    return normalized


def _program_entries(catalog: Mapping[str, Sequence[Mapping[str, Any]]]):
    from scripts.run_cn_program_optimizer_tournament_v1 import _program_entries as build
    return build(catalog)


def _load_authority(
    args: argparse.Namespace,
    *,
    authorization: Mapping[str, Any],
    repo_sha: str,
    campaign_id: str = CAMPAIGN_ID,
    campaign_profile: str = CAMPAIGN_PROFILE,
    prior_freeze_payload_sha256: str = PRIOR_FREEZE_PAYLOAD_SHA256,
    prior_exact_count: int = PRIOR_EXACT_COUNT,
    prior_exact_identities_sha256: str = PRIOR_EXACT_IDENTITIES_SHA256,
    prior_identity_field: str = "prior_exact_identities",
    input_binding_schema_version: str = "cn_program_optimizer_successor_input_binding_v1",
) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError("SUCCESSOR_BENCHMARK_UNAUTHORIZED_HOST")
    expected_paths = {
        "source_freeze_root": SOURCE_FREEZE_ROOT,
        "execution_contract": SOURCE_EXECUTION_CONTRACT_PATH,
        "train_field_root": SOURCE_TRAIN_FIELD_ROOT,
        "train_price_root": SOURCE_TRAIN_PRICE_ROOT,
        "registry": SOURCE_REGISTRY_PATH,
        "node_resource_capacity": SOURCE_NODE_CAPACITY_PATH,
    }
    observed_paths = {
        "source_freeze_root": args.source_freeze_root,
        "execution_contract": args.execution_contract,
        "train_field_root": args.train_field_root,
        "train_price_root": args.train_price_root,
        "registry": args.registry,
        "node_resource_capacity": args.node_resource_capacity,
    }
    for key, expected in expected_paths.items():
        if _norm(observed_paths[key]) != _norm(expected):
            raise RuntimeError(f"SUCCESSOR_{key.upper()}_PATH_DRIFT")

    prior = _read_prior_freeze(
        args.prior_exact_freeze,
        expected_payload_sha256=prior_freeze_payload_sha256,
        expected_count=prior_exact_count,
        expected_exact_identities_sha256=prior_exact_identities_sha256,
        identity_field=prior_identity_field,
    )
    prior_ids = tuple(map(str, prior["prior_exact_identities"]))

    freeze_root = args.source_freeze_root.resolve()
    closure = verify_phase_freeze_v1(freeze_root)
    closure_path = freeze_root / SOURCE_FREEZE_CLOSURE
    contract_path = freeze_root / "phase_c_run_contract.json"
    reservoir_path = freeze_root / "phase_c_raw_program_reservoir.jsonl"
    component_path = freeze_root / "phase_c_session_executable_component_pool.jsonl"
    if (
        engine._sha256(closure_path) != SOURCE_FREEZE_CLOSURE_FILE_SHA256
        or str(closure.get("closure_sha256") or "")
        != SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256
        or engine._sha256(contract_path) != SOURCE_RUN_CONTRACT_FILE_SHA256
        or engine._sha256(reservoir_path) != SOURCE_RAW_RESERVOIR_SHA256
        or engine._sha256(component_path) != SOURCE_COMPONENT_POOL_SHA256
    ):
        raise RuntimeError("SUCCESSOR_SOURCE_FREEZE_FILE_DRIFT")
    contract = engine._read_json(contract_path)
    contract_body = dict(contract)
    contract_hash = str(contract_body.pop("run_contract_sha256", ""))
    if (
        contract_hash != stable_hash(contract_body)
        or contract_hash != SOURCE_RUN_CONTRACT_PAYLOAD_SHA256
    ):
        raise RuntimeError("SUCCESSOR_SOURCE_RUN_CONTRACT_DRIFT")

    execution_contract_path = args.execution_contract.resolve()
    registry_path = args.registry.resolve()
    capacity_path = args.node_resource_capacity.resolve()
    train_field_root = args.train_field_root.resolve()
    train_price_root = args.train_price_root.resolve()
    if engine._sha256(execution_contract_path) != SOURCE_EXECUTION_CONTRACT_SHA256:
        raise RuntimeError("SUCCESSOR_EXECUTION_CONTRACT_DRIFT")
    if engine._sha256(registry_path) != SOURCE_REGISTRY_SHA256:
        raise RuntimeError("SUCCESSOR_REGISTRY_DRIFT")
    if engine._sha256(capacity_path) != SOURCE_NODE_CAPACITY_SHA256:
        raise RuntimeError("SUCCESSOR_NODE_CAPACITY_DRIFT")
    for root in (train_field_root, train_price_root):
        if not root.is_dir():
            raise FileNotFoundError(root)
        normalized = _norm(root)
        if any(
            token in normalized
            for token in (
                "validation",
                "holdout",
                "historical_challenge_2023",
                "forward_b",
                "forward_2026",
            )
        ):
            raise RuntimeError("SUCCESSOR_NON_DEVELOPMENT_INPUT_ROOT")

    phase_b_result_root = Path(str(contract["phase_b_result_root"])).resolve()
    phase_b_input = engine._read_json(phase_b_result_root / "input_binding.json")
    phase_b_body = dict(phase_b_input)
    phase_b_hash = str(phase_b_body.pop("input_binding_sha256", ""))
    if phase_b_hash != stable_hash(phase_b_body):
        raise RuntimeError("SUCCESSOR_PHASE_B_INPUT_HASH_DRIFT")
    if (
        str(phase_b_input["execution_contract_sha256"])
        != SOURCE_EXECUTION_CONTRACT_SHA256
        or _norm(phase_b_input["train_price_root"]) != _norm(train_price_root)
    ):
        raise RuntimeError("SUCCESSOR_PHASE_B_AUTHORITY_DRIFT")

    execution_contract = engine._read_json(execution_contract_path)
    engine.decoder_v2.base._verify_payload_hash(
        execution_contract,
        field="contract_payload_sha256",
        label="successor benchmark execution contract",
    )
    accepted_field_manifest = Path(str(contract["accepted_field_manifest_path"])).resolve()
    if accepted_field_manifest.parent != train_field_root:
        raise RuntimeError("SUCCESSOR_TRAIN_FIELD_ROOT_DRIFT")
    field_manifest_file_sha = str(contract["accepted_field_manifest_file_sha256"])
    field_manifest_payload_sha = str(contract["accepted_field_manifest_payload_sha256"])
    field_manifest, field_manifest_path = phase_b._validate_phase_b_materialized_sidecar(
        train_field_root,
        split_manifest_sha256=str(execution_contract["split_manifest_sha256"]),
        verify_shards=True,
        expected_manifest_file_sha256=field_manifest_file_sha,
        expected_manifest_payload_sha256=field_manifest_payload_sha,
    )
    price_manifest, price_manifest_path = phase_b._validate_phase_b_execution_price_sidecar(
        train_price_root,
        split_manifest_sha256=str(execution_contract["split_manifest_sha256"]),
        expected_manifest_file_sha256=str(phase_b_input["train_price_manifest_sha256"]),
    )
    if int(field_manifest["sidecar_rows"]) != int(price_manifest["sidecar_rows"]):
        raise RuntimeError("SUCCESSOR_FEATURE_PRICE_ROW_COUNT_DRIFT")

    capacity = engine._read_json(capacity_path)
    capacity_body = dict(capacity)
    capacity_hash = str(capacity_body.pop("capacity_manifest_sha256", ""))
    if capacity_hash != stable_hash(capacity_body):
        raise RuntimeError("SUCCESSOR_NODE_CAPACITY_SELF_HASH_DRIFT")
    profile = dict(capacity.get("profiles", {}).get("VALIDATION_EXCLUSIVE_32") or {})
    if (
        int(profile.get("cpu_threads") or 0) != 32
        or int(profile.get("minimum_free_memory_bytes") or 0)
        != engine.MINIMUM_FREE_MEMORY_BYTES
        or not 1 <= int(args.executor_workers) <= 8
    ):
        raise RuntimeError("SUCCESSOR_RESOURCE_PROFILE_DRIFT")

    reservoir = engine._read_jsonl(reservoir_path)
    component_rows = engine._read_jsonl(component_path)
    registry = UnifiedCapabilityRegistry.read(registry_path)
    catalog, catalog_report = engine._build_catalog(
        reservoir=reservoir,
        component_rows=component_rows,
        registry=registry,
    )
    entries = _program_entries(catalog)
    program_space_hash = stable_hash([entry.to_dict() for entry in entries])
    if len(entries) != FROZEN_PROGRAM_SPACE_COUNT or program_space_hash != FROZEN_PROGRAM_SPACE_SHA256:
        raise RuntimeError("SUCCESSOR_PROGRAM_SPACE_DRIFT")
    entry_ids = {entry.exact_identity for entry in entries}
    if not set(prior_ids).issubset(entry_ids):
        raise RuntimeError("SUCCESSOR_PRIOR_EXACT_OUTSIDE_PROGRAM_SPACE")

    ordered_slots = tuple(entries[0].genes)
    catalog_by_exact: dict[str, dict[str, Any]] = {}
    group_by_exact: dict[str, str] = {}
    catalog_source_rows = 0
    catalog_alias_collapses = 0
    for template_id in sorted(catalog):
        for source in catalog[template_id]:
            catalog_source_rows += 1
            exact = normalized_program_gene_identity_v1(
                dict(source["program_genes"]), ordered_slots=ordered_slots
            )
            if exact in catalog_by_exact:
                catalog_alias_collapses += 1
            # Mirror the V1 exact-space builder: normalized Program identity is
            # authoritative and later source aliases deterministically replace
            # earlier aliases in catalog order.
            catalog_by_exact[exact] = dict(source)
            group_by_exact[exact] = str(source["base_component_id"])
    if set(catalog_by_exact) != entry_ids:
        raise RuntimeError("SUCCESSOR_CATALOG_ENTRY_IDENTITY_DRIFT")

    components_by_id = {
        component.component_id: component
        for component in (engine._component_from_row(row) for row in component_rows)
    }
    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    windows = tuple(
        dict(row)
        for row in engine._read_json(
            Path(str(contract["phase_b_freeze_root"])) / "phase_b_run_contract.json"
        )["development_subwindows"]
    )
    input_binding = engine._self_hashed(
        {
            "schema_version": str(input_binding_schema_version),
            "campaign_id": str(campaign_id),
            "campaign_profile": str(campaign_profile),
            "runner_repo_sha": str(repo_sha),
            "authorization_payload_sha256": str(
                authorization["authorization_payload_sha256"]
            ),
            "prior_freeze_payload_sha256": str(prior_freeze_payload_sha256),
            "prior_exact_identities_sha256": str(prior_exact_identities_sha256),
            "source_freeze_closure_file_sha256": SOURCE_FREEZE_CLOSURE_FILE_SHA256,
            "source_freeze_closure_payload_sha256": SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256,
            "source_run_contract_file_sha256": SOURCE_RUN_CONTRACT_FILE_SHA256,
            "source_run_contract_payload_sha256": SOURCE_RUN_CONTRACT_PAYLOAD_SHA256,
            "program_space_sha256": program_space_hash,
            "catalog_source_row_count": catalog_source_rows,
            "catalog_exact_alias_collapse_count": catalog_alias_collapses,
            "execution_contract_sha256": SOURCE_EXECUTION_CONTRACT_SHA256,
            "registry_sha256": SOURCE_REGISTRY_SHA256,
            "node_capacity_sha256": SOURCE_NODE_CAPACITY_SHA256,
            "field_manifest_file_sha256": engine._sha256(field_manifest_path),
            "field_manifest_payload_sha256": str(field_manifest["manifest_hash"]),
            "price_manifest_file_sha256": engine._sha256(price_manifest_path),
            "price_manifest_payload_sha256": stable_hash(price_manifest),
            "train_field_root": str(train_field_root),
            "train_price_root": str(train_price_root),
            "evaluation_data_role": "DEVELOPMENT_ONLY",
            "restricted_reads": {
                "validation": 0,
                "holdout": 0,
                "historical_2023": 0,
                "forward_b": 0,
                "forward_2026": 0,
            },
        },
        "input_binding_sha256",
    )
    return {
        "prior": prior,
        "prior_ids": prior_ids,
        "contract": contract,
        "catalog": catalog,
        "catalog_report": catalog_report,
        "catalog_source_row_count": catalog_source_rows,
        "catalog_exact_alias_collapse_count": catalog_alias_collapses,
        "catalog_by_exact": catalog_by_exact,
        "group_by_exact": group_by_exact,
        "entries": entries,
        "components_by_id": components_by_id,
        "adapter": adapter,
        "compiler": compiler,
        "execution_contract_path": execution_contract_path,
        "train_field_root": train_field_root,
        "train_price_root": train_price_root,
        "registry_path": registry_path,
        "price_manifest": price_manifest,
        "price_manifest_path": price_manifest_path,
        "field_manifest_file_sha": field_manifest_file_sha,
        "field_manifest_payload_sha": field_manifest_payload_sha,
        "windows": windows,
        "input_binding": input_binding,
    }


def _benchmark(authority: Mapping[str, Any]) -> ProgramOptimizerSuccessorBenchmarkV1:
    return ProgramOptimizerSuccessorBenchmarkV1(
        entries=authority["entries"],
        template_ids=ENHANCED_TEMPLATES,
        group_by_exact_identity=authority["group_by_exact"],
        uniform_seed=SEEDS["UNIFORM"],
        tpe_seed=SEEDS["TPE_CONTROL"],
        d1_surrogate_seed=SEEDS["TPE_TO_SURROGATE"],
        d2_surrogate_seed=SEEDS["FEASIBILITY_GATED_TPE"],
        tpe_config=TPE_CONFIG,
        surrogate_config=SURROGATE_CONFIG,
        prior_exact_identities=authority["prior_ids"],
        minimum_distinct_groups=16,
        maximum_per_group=4,
    )


def _logical_row(row: Any) -> dict[str, Any]:
    return {
        "policy": row.policy,
        "wave_index": row.wave_index,
        "template_id": row.template_id,
        "exact_identity": row.exact_identity,
        "logical_proposal_id": row.logical_proposal_id,
        "optimizer_proposal_id": row.optimizer_proposal_id,
        "selection_kind": row.selection_kind,
        "optimizer_ask": copy.deepcopy(row.optimizer_ask),
    }


def _build_physical_schedules(
    *,
    prepared: Any,
    authority: Mapping[str, Any],
    start_ordinal: int,
) -> list[dict[str, Any]]:
    schedules = []
    logical_by_exact: dict[str, list[Any]] = {}
    for logical in prepared.asks:
        logical_by_exact.setdefault(logical.exact_identity, []).append(logical)
    template_counts: dict[str, int] = {}
    for offset, exact in enumerate(prepared.physical_exact_identities):
        logicals = logical_by_exact[exact]
        template_id = str(logicals[0].template_id)
        if any(str(row.template_id) != template_id for row in logicals):
            raise RuntimeError("SUCCESSOR_PHYSICAL_EXACT_TEMPLATE_DRIFT")
        entry = dict(authority["catalog_by_exact"][exact])
        template_ordinal = template_counts.get(template_id, 0)
        template_counts[template_id] = template_ordinal + 1
        ask_body = {
            "schema_version": "cn_program_optimizer_successor_physical_ask_v1",
            "main_record_ordinal": int(start_ordinal + offset),
            "checkpoint_ordinal": int(prepared.wave_index),
            "template_id": template_id,
            "template_record_ordinal": int(prepared.wave_index * 4 + template_ordinal),
            "generation_arm": "SUCCESSOR_PHYSICAL_DEDUP",
            "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
            "canary_profile": CAMPAIGN_PROFILE,
            "absolute_admission_head_eligible": True,
            "conditional_uplift_head_eligible": True,
        }
        ask = {**ask_body, "ask_record_sha256": stable_hash(ask_body)}
        decision = engine._self_hashed(
            {
                "schema_version": "cn_program_optimizer_successor_physical_selection_v1",
                "wave_index": int(prepared.wave_index),
                "main_record_ordinal": int(start_ordinal + offset),
                "template_id": template_id,
                "exact_identity": exact,
                "logical_policies": sorted(row.policy for row in logicals),
                "logical_proposal_ids": sorted(row.logical_proposal_id for row in logicals),
                "adaptive_template_credit_used": False,
            },
            "selection_decision_sha256",
        )
        schedule = engine._schedule_record(
            ask,
            entry,
            decision,
            components_by_id=authority["components_by_id"],
            adapter=authority["adapter"],
            compiler=authority["compiler"],
        )
        schedule.update(
            {
                "successor_wave_index": int(prepared.wave_index),
                "successor_exact_identity": exact,
                "successor_logical_policies": sorted(row.policy for row in logicals),
                "successor_logical_proposal_ids": sorted(
                    row.logical_proposal_id for row in logicals
                ),
            }
        )
        schedule["schedule_record_sha256"] = stable_hash(
            {
                key: value
                for key, value in schedule.items()
                if key != "schedule_record_sha256"
            }
        )
        schedules.append(schedule)
    return schedules


def _evaluate_schedules(
    schedules: Sequence[Mapping[str, Any]],
    *,
    record_root: Path,
    authority: Mapping[str, Any],
    input_hash: str,
    executor_workers: int,
) -> list[dict[str, Any]]:
    if not schedules:
        return []
    record_root.mkdir(parents=True, exist_ok=False)
    fields = engine._checkpoint_field_columns(schedules)
    before_children = {child.pid for child in psutil.Process().children(recursive=True)}
    before = engine._runtime_resource_snapshot()
    engine._require_runtime_resource_safety(before)
    futures = {}
    completed: set[int] = set()
    executor_options = {
        "max_workers": min(int(executor_workers), len(schedules)),
        "initializer": engine._initialize_worker,
        "initargs": (
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
            fields,
        ),
    }
    with ProcessPoolExecutor(**executor_options) as executor:
        for schedule in schedules:
            ordinal = int(schedule["main_record_ordinal"])
            target = record_root / f"record_{ordinal:04d}.json"
            futures[executor.submit(engine._evaluate_record, schedule, str(target))] = ordinal
        pending = set(futures)
        while pending:
            done, pending = wait(pending, timeout=2.0, return_when=FIRST_COMPLETED)
            for future in done:
                payload = future.result()
                completed.add(int(payload["main_record_ordinal"]))
            engine._require_runtime_resource_safety(engine._runtime_resource_snapshot())
    gc.collect()
    engine._require_runtime_resource_safety(engine._runtime_resource_snapshot())
    orphans = engine._new_child_process_ids(before_children)
    if orphans:
        raise RuntimeError(
            "SUCCESSOR_EVALUATOR_LEFT_ORPHAN_WORKERS:" + ",".join(map(str, orphans))
        )
    expected = {int(row["main_record_ordinal"]) for row in schedules}
    if completed != expected:
        raise RuntimeError("SUCCESSOR_PHYSICAL_EVALUATION_COVERAGE_DRIFT")
    schedule_by_ordinal = {int(row["main_record_ordinal"]): row for row in schedules}
    records = []
    for path in sorted(record_root.glob("record_*.json")):
        ordinal = int(path.stem.split("_")[-1])
        records.append(
            engine._verify_record(
                path,
                expected_input_hash=input_hash,
                schedule=schedule_by_ordinal[ordinal],
            )
        )
    if len(records) != len(schedules):
        raise RuntimeError("SUCCESSOR_VERIFIED_RECORD_COUNT_DRIFT")
    return records


def _physical_result(
    record: Mapping[str, Any], schedule: Mapping[str, Any]
) -> PhysicalProgramResultV1:
    exact = str(schedule["successor_exact_identity"])
    admission = AbsoluteEconomicAdmission.evaluate(
        record,
        expected_pair_id=str(schedule["pair_id"]),
        expected_program_id=str(schedule["primary_program"]["program_id"]),
        expected_control_program_id=str(schedule["control_program"]["program_id"]),
    )
    uplift = conditional_uplift_credit(record, admission)
    return PhysicalProgramResultV1.create(
        exact_identity=exact,
        admission=admission,
        uplift=uplift,
    )


def _physical_result_record(
    result: PhysicalProgramResultV1,
    *,
    source_wave: int,
    source_record_sha256: str,
    cache_hit: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "cn_program_optimizer_successor_physical_result_receipt_v1",
        "exact_identity": result.exact_identity,
        "physical_result_hash": result.physical_result_hash,
        "admission": result.admission.to_record(),
        "uplift": None if result.uplift is None else result.uplift.to_record(),
        "source_wave": int(source_wave),
        "source_record_sha256": str(source_record_sha256),
        "cache_hit": bool(cache_hit),
    }


def _close_wave(
    *,
    inflight: Path,
    closed: Path,
    root: Path,
    previous_manifest_sha256: str,
    wave_index: int,
) -> Path:
    artifacts = [
        _artifact(path, inflight)
        for path in sorted(inflight.rglob("*"))
        if path.is_file() and path.name != "wave_manifest.json"
    ]
    manifest = engine._self_hashed(
        {
            "schema_version": "cn_program_optimizer_successor_wave_manifest_v1",
            "status": "SUCCESSOR_WAVE_CLOSED_IMMUTABLE",
            "wave_index": int(wave_index),
            "previous_wave_manifest_sha256": str(previous_manifest_sha256),
            "artifacts": artifacts,
        },
        "manifest_payload_sha256",
    )
    engine._write_json(inflight / "wave_manifest.json", manifest)
    if closed.exists():
        raise RuntimeError("SUCCESSOR_CLOSED_WAVE_ALREADY_EXISTS")
    inflight.replace(closed)
    return closed / "wave_manifest.json"


def _productive(row: Mapping[str, Any]) -> bool:
    if not bool(row.get("admitted")):
        return False
    uplift = dict(row.get("uplift_record") or {})
    credit = dict(uplift.get("program_credit") or {})
    return (
        float(credit.get("matched_cumulative_net_return_increment") or 0.0) > 0.0
        and float(credit.get("matched_net_reward_increment") or 0.0) > 0.0
    )


def _wilson(successes: int, total: int) -> tuple[float, float]:
    if total <= 0:
        return (0.0, 1.0)
    z = 1.959963984540054
    rate = successes / total
    denom = 1.0 + z * z / total
    center = rate + z * z / (2.0 * total)
    radius = z * math.sqrt(rate * (1.0 - rate) / total + z * z / (4.0 * total * total))
    return ((center - radius) / denom, (center + radius) / denom)


def _metric_block(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    admitted = sum(bool(row.get("admitted")) for row in rows)
    productive = sum(_productive(row) for row in rows)
    low, high = _wilson(productive, total)
    return {
        "evaluated": total,
        "admitted": admitted,
        "productive": productive,
        "admission_rate": admitted / total if total else 0.0,
        "productive_efficiency": productive / total if total else 0.0,
        "productive_wilson_95": {"lower": low, "upper": high},
    }


def _final_metrics(benchmark: ProgramOptimizerSuccessorBenchmarkV1) -> dict[str, Any]:
    completed = benchmark.snapshot()["completed_rows"]
    output = {}
    for policy in POLICIES:
        rows = list(completed[policy])
        post = [row for row in rows if int(row["wave_index"]) >= BOOTSTRAP_WAVES]
        specific = [
            row
            for row in post
            if int(row["wave_index"]) not in set(UNIFORM_FLOOR_WAVES)
        ]
        if (
            len(rows) != TOTAL_POLICY_EFFICIENCY_DENOMINATOR
            or len(post) != POST_BOOTSTRAP_EFFICIENCY_DENOMINATOR
            or len(specific) != POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY_DENOMINATOR
        ):
            raise RuntimeError("SUCCESSOR_FINAL_METRIC_DENOMINATOR_DRIFT")
        output[policy] = {
            "TOTAL_POLICY_EFFICIENCY": _metric_block(rows),
            "POST_BOOTSTRAP_EFFICIENCY": _metric_block(post),
            "POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY": _metric_block(specific),
            "per_template": {
                template: _metric_block(
                    [row for row in rows if str(row["template_id"]) == template]
                )
                for template in ENHANCED_TEMPLATES
            },
            "per_wave": {
                str(wave): _metric_block(
                    [row for row in rows if int(row["wave_index"]) == wave]
                )
                for wave in range(TOTAL_WAVES)
            },
        }
    leader = max(
        POLICIES,
        key=lambda policy: (
            output[policy]["POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY"][
                "productive_efficiency"
            ],
            policy,
        ),
    )
    return {
        "policies": output,
        "development_point_estimate_leader": leader,
        "development_leader_is_promotion_authority": False,
    }


def _run_prefinancial(
    args: argparse.Namespace,
    *,
    authorization: Mapping[str, Any],
    repo_sha: str,
) -> dict[str, Any]:
    authority = _load_authority(args, authorization=authorization, repo_sha=repo_sha)
    benchmark = _benchmark(authority)
    prepared = benchmark.prepare_wave()
    if len(prepared.asks) != 28:
        raise RuntimeError("SUCCESSOR_PREFINANCIAL_LOGICAL_WAVE_DRIFT")
    if any(exact in set(authority["prior_ids"]) for exact in prepared.physical_exact_identities):
        raise RuntimeError("SUCCESSOR_PREFINANCIAL_PRIOR_EXACT_REUSE")
    schedules = _build_physical_schedules(
        prepared=prepared,
        authority=authority,
        start_ordinal=0,
    )
    if len(schedules) != len(prepared.physical_exact_identities):
        raise RuntimeError("SUCCESSOR_PREFINANCIAL_PHYSICAL_SCHEDULE_COUNT_DRIFT")
    proposal_arms = {
        str(dict(row["proposal_receipt"])["generation_arm"])
        for row in schedules
    }
    if proposal_arms != {"SUCCESSOR_PHYSICAL_DEDUP"}:
        raise RuntimeError("SUCCESSOR_PREFINANCIAL_PHYSICAL_PROVENANCE_DRIFT")
    return {
        "status": "SUCCESSOR_PREFINANCIAL_READY",
        "program_space_count": len(authority["entries"]),
        "catalog_source_row_count": authority["catalog_source_row_count"],
        "catalog_exact_alias_collapse_count": (
            authority["catalog_exact_alias_collapse_count"]
        ),
        "prior_exact_count": len(authority["prior_ids"]),
        "base_program_count": FROZEN_BASE_PROGRAM_COUNT,
        "enhanced_program_count": FROZEN_ENHANCED_PROGRAM_COUNT,
        "prospective_enhanced_available_count": (
            FROZEN_ENHANCED_PROGRAM_COUNT - len(authority["prior_ids"])
        ),
        "wave0_logical_count": len(prepared.asks),
        "wave0_physical_unique_count": len(prepared.physical_exact_identities),
        "wave0_schedule_build_count": len(schedules),
        "wave0_physical_generation_arms": sorted(proposal_arms),
        "wave0_overlap_map": prepared.overlap_map(),
        "gate_statistics": prepared.gate_statistics,
        "restricted_reads": 0,
    }


def run_authorized_successor_benchmark(
    args: argparse.Namespace,
    *,
    admission: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    repo_sha = str(admission["repo_sha"])
    if bool(getattr(args, "prefinancial_only", False)):
        raise RuntimeError("SUCCESSOR_PREFINANCIAL_ONLY_FORBIDDEN_AFTER_ADMISSION")
    authority = _load_authority(args, authorization=authorization, repo_sha=repo_sha)
    root = args.output_root.resolve()
    if not root.is_dir() or not (root / ".project_control_execution").is_dir():
        raise RuntimeError("SUCCESSOR_ADMITTED_OUTPUT_ROOT_MISSING")
    unexpected = {
        path.name for path in root.iterdir()
        if path.name != ".project_control_execution"
    }
    if unexpected:
        raise RuntimeError("SUCCESSOR_ADMITTED_OUTPUT_ROOT_NOT_CLEAN")

    engine._write_json(root / "input_binding.json", authority["input_binding"])
    input_hash = str(authority["input_binding"]["input_binding_sha256"])
    benchmark = _benchmark(authority)
    engine._write_json(root / "benchmark_state_genesis.json", benchmark.snapshot())
    engine._write_json(
        root / "prior_exact_binding.json",
        {
            "schema_version": "cn_program_optimizer_successor_prior_binding_v1",
            "prior_exact_count": len(authority["prior_ids"]),
            "prior_exact_identities_sha256": PRIOR_EXACT_IDENTITIES_SHA256,
            "prior_freeze_payload_sha256": PRIOR_FREEZE_PAYLOAD_SHA256,
            "prior_results_imported_as_optimizer_feedback": False,
        },
    )

    physical_cache: dict[str, PhysicalProgramResultV1] = {}
    cache_source_wave: dict[str, int] = {}
    cache_source_record_sha: dict[str, str] = {}
    previous_manifest_sha = "GENESIS"
    next_physical_ordinal = 0
    wave_manifests = []
    aggregate_gate = {
        "gate_raw_ask_count": 0,
        "gate_rejection_count": 0,
        "economic_ask_count": 0,
        "ordinary_projection_count": 0,
    }
    physical_evaluations = 0
    cache_hits = 0
    wall_start = time.perf_counter()

    for wave in range(TOTAL_WAVES):
        if benchmark.wave_index != wave:
            raise RuntimeError("SUCCESSOR_WAVE_STATE_INDEX_DRIFT")
        state_before = benchmark.snapshot()
        prepared = benchmark.prepare_wave()
        inflight = root / f"wave_{wave:03d}.inflight"
        closed = root / f"wave_{wave:03d}"
        if inflight.exists() or closed.exists():
            raise RuntimeError("SUCCESSOR_WAVE_PATH_NOT_FRESH")
        inflight.mkdir(parents=False, exist_ok=False)
        engine._write_json(inflight / "benchmark_state_before.json", state_before)
        engine._write_jsonl(
            inflight / "logical_asks.jsonl",
            [_logical_row(row) for row in prepared.asks],
        )
        engine._write_jsonl(
            inflight / "gate_rejections.jsonl",
            list(prepared.gate_rejections),
        )
        engine._write_json(
            inflight / "physical_union.json",
            {
                "schema_version": "cn_program_optimizer_successor_physical_union_v1",
                "wave_index": wave,
                "logical_count": len(prepared.asks),
                "physical_exact_identities": list(prepared.physical_exact_identities),
                "overlap_map": prepared.overlap_map(),
                "selection_frozen_before_cache_lookup": True,
            },
        )

        needed = [
            exact
            for exact in prepared.physical_exact_identities
            if exact not in physical_cache
        ]
        schedules = _build_physical_schedules(
            prepared=prepared,
            authority=authority,
            start_ordinal=next_physical_ordinal,
        )
        schedules = [
            row for row in schedules if row["successor_exact_identity"] in set(needed)
        ]
        engine._write_jsonl(inflight / "physical_schedules.jsonl", schedules)
        records = _evaluate_schedules(
            schedules,
            record_root=inflight / "records",
            authority=authority,
            input_hash=input_hash,
            executor_workers=int(args.executor_workers),
        ) if schedules else []
        next_physical_ordinal += len(schedules)
        physical_evaluations += len(schedules)

        schedule_by_ordinal = {
            int(row["main_record_ordinal"]): row for row in schedules
        }
        new_result_rows = []
        for record in records:
            ordinal = int(record["main_record_ordinal"])
            schedule = schedule_by_ordinal[ordinal]
            result = _physical_result(record, schedule)
            exact = result.exact_identity
            if exact in physical_cache:
                raise RuntimeError("SUCCESSOR_NEW_PHYSICAL_RESULT_DUPLICATE")
            physical_cache[exact] = result
            cache_source_wave[exact] = wave
            cache_source_record_sha[exact] = str(record["record_payload_sha256"])
            new_result_rows.append(
                _physical_result_record(
                    result,
                    source_wave=wave,
                    source_record_sha256=str(record["record_payload_sha256"]),
                    cache_hit=False,
                )
            )
        engine._write_jsonl(inflight / "new_physical_results.jsonl", new_result_rows)

        physical_results = {
            exact: physical_cache[exact] for exact in prepared.physical_exact_identities
        }
        wave_result_rows = []
        for exact in prepared.physical_exact_identities:
            hit = cache_source_wave[exact] != wave
            cache_hits += int(hit)
            wave_result_rows.append(
                _physical_result_record(
                    physical_cache[exact],
                    source_wave=cache_source_wave[exact],
                    source_record_sha256=cache_source_record_sha[exact],
                    cache_hit=hit,
                )
            )
        engine._write_jsonl(inflight / "wave_physical_results.jsonl", wave_result_rows)
        commit = benchmark.commit_wave(prepared, physical_results)
        engine._write_json(inflight / "wave_commit_receipt.json", commit)
        engine._write_json(inflight / "benchmark_state_after.json", benchmark.snapshot())
        for key in aggregate_gate:
            aggregate_gate[key] += int(prepared.gate_statistics[key])
        manifest_path = _close_wave(
            inflight=inflight,
            closed=closed,
            root=root,
            previous_manifest_sha256=previous_manifest_sha,
            wave_index=wave,
        )
        previous_manifest_sha = engine._sha256(manifest_path)
        wave_manifests.append(previous_manifest_sha)

    if benchmark.wave_index != TOTAL_WAVES:
        raise RuntimeError("SUCCESSOR_TERMINAL_WAVE_COUNT_DRIFT")
    final_state = benchmark.snapshot()
    engine._write_json(root / "benchmark_state_final.json", final_state)
    metrics = _final_metrics(benchmark)
    engine._write_json(root / "policy_metrics.json", metrics)
    physical_unique = len(physical_cache)
    wall_seconds = float(time.perf_counter() - wall_start)
    resource = engine._runtime_resource_snapshot()
    closure = engine._self_hashed(
        {
            "schema_version": "cn_program_optimizer_successor_benchmark_complete_v1",
            "status": STATUS,
            "campaign_id": CAMPAIGN_ID,
            "campaign_profile": CAMPAIGN_PROFILE,
            "repo_sha": repo_sha,
            "authorization_payload_sha256": str(
                authorization["authorization_payload_sha256"]
            ),
            "input_binding_sha256": input_hash,
            "prior_exact_count": PRIOR_EXACT_COUNT,
            "prior_exact_identities_sha256": PRIOR_EXACT_IDENTITIES_SHA256,
            "program_space_count": FROZEN_PROGRAM_SPACE_COUNT,
            "program_space_sha256": FROZEN_PROGRAM_SPACE_SHA256,
            "logical_records": TOTAL_POLICY_EFFICIENCY_DENOMINATOR * len(POLICIES),
            "logical_records_per_policy": TOTAL_POLICY_EFFICIENCY_DENOMINATOR,
            "closed_waves": TOTAL_WAVES,
            "physical_unique_evaluations": physical_unique,
            "physical_evaluation_calls": physical_evaluations,
            "cross_wave_cache_hits": cache_hits,
            "aggregate_gate_statistics": aggregate_gate,
            "policy_metrics": metrics,
            "wave_manifest_sha256": wave_manifests,
            "wall_seconds": wall_seconds,
            "resource_final": resource,
            "restricted_reads": {
                "validation": 0,
                "holdout": 0,
                "historical_2023": 0,
                "forward_b": 0,
                "forward_2026": 0,
            },
            "oos_authority": "NONE",
            "promotion_authorized": False,
            "automatic_successor_authorized": False,
        },
        "closure_payload_sha256",
    )
    engine._write_json(root / CLOSURE_NAME, closure)
    return closure


def _prefinancial_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--executor-workers", type=int, default=8)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--prefinancial-only", action="store_true", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _prefinancial_parser().parse_args(argv)
    from our_system_phase2.runtime.cn_program_optimizer_successor_benchmark_v1 import (
        verify_authorization,
    )
    authorization = verify_authorization(args.campaign_authorization)
    result = _run_prefinancial(
        args,
        authorization=authorization,
        repo_sha=str(args.repo_sha),
    )
    root = args.output_root.resolve()
    if root.exists():
        raise FileExistsError(root)
    root.mkdir(parents=True, exist_ok=False)
    engine._write_json(root / "SUCCESSOR_PREFINANCIAL_READY.json", result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
