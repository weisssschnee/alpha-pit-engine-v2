"""Frozen Phase-B contract for CN joint candidate-program evaluation.

The freeze is deliberately zero-financial.  It regenerates the already
accepted fixed-stratified route proposals, keeps only pairs that the accepted
development-sidecar materialization screen declared compatible, freezes honest
component pools, and composes the exact 64-record uniform Phase-B schedule.

Financial execution is a later command/phase.  Nothing in this module opens a
field sidecar, price table, label table, validation asset, historical challenge,
Forward-B asset, or 2026 asset.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from our_system_phase2.runtime.cn_candidate_representation_v0_preflight import (
    verify_candidate_representation_v0_preflight,
)
from our_system_phase2.runtime.cn_joint_program_phase_a_v0 import verify_phase_a_v0
from our_system_phase2.services.candidate_program_controls_v1 import (
    construct_matched_control_program_v1,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    DEFAULT_COMBINATION_POLICY,
    PROGRAM_TEMPLATE_COMPONENTS,
    CandidateProgramProposalAdapterV0,
    ProgramProposalReceiptV0,
    ProgramSourceComponentV0,
)
from our_system_phase2.services.candidate_program_v1 import (
    ProgramCompilerV1,
    legacy_candidate_program_v1,
)
from our_system_phase2.services.fixed_stratified_candidate_sampling import (
    generate_fixed_stratified_epoch_v0,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)
from our_system_phase2.services.unified_discovery_generators import (
    load_development_discovery_root_authority,
)


FREEZE_SCHEMA = "cn_joint_program_phase_b_prefinancial_freeze_v0"
FREEZE_STATUS = "CN_JOINT_PROGRAM_PHASE_B_PREFINANCIAL_FREEZE_COMPLETE"
FREEZE_CLOSURE_NAME = "CN_JOINT_PROGRAM_PHASE_B_PREFINANCIAL_FREEZE_COMPLETE.json"

TEMPLATE_ORDER = tuple(PROGRAM_TEMPLATE_COMPONENTS)
ENHANCED_TEMPLATE_ORDER = TEMPLATE_ORDER[1:]
ROLE_TARGETS = {"base": 128, "temporal": 96, "market": 48, "event": 48}
ROUTE_ROLE = {
    "MINUTE_STATIC": "base",
    "FIRSTN_PATH": "base",
    "SLOW_CROSS_SECTIONAL_LEVEL": "base",
    "INTRADAY_STATE_TRANSITION": "base",
    "SLOW_TEMPORAL_CHANGE": "temporal",
    "MARKET_REGIME_CONDITION": "market",
    "DISCLOSURE_EVENT": "event",
    "BROAD_EVENT_FROZEN_ENTRY": "event",
}
SESSION_EXECUTABLE_ROUTES = frozenset(
    {
        "SLOW_CROSS_SECTIONAL_LEVEL",
        "SLOW_TEMPORAL_CHANGE",
        "MARKET_REGIME_CONDITION",
        "DISCLOSURE_EVENT",
    }
)
TEMPORAL_POLICIES = ("ADD", "SUBTRACT", "MIN", "MAX")
MARKET_POLICIES = ("GATE", "FILTER", "VETO", "MODULATE")
EVENT_POLICIES = ("GATE", "FILTER")

MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
CACHE_CAP_BYTES = 8 * 1024**3
RESOURCE_PROFILE = "VALIDATION_EXCLUSIVE_32"
ENTITLEMENT_THREADS = 32
EXECUTOR_WORKERS = 10
NATIVE_THREADS_PER_WORKER = 1

# Reuse the accepted decoder-process qualification.  This is an execution
# topology qualification, not economic input and not a new infrastructure run.
ACCELERATION_BASIS = {
    "qualification_root": (
        "D:\\ChengboRemote\\runtime\\"
        "cn_portfolio_decoder_v2_process_qualification_32_20260804_7051ae6"
    ),
    "closure_file_sha256": (
        "063cdc0040516bd1910adb633ea6818c13536353798f4ad44e116c063eb75537"
    ),
    "audit_file_sha256": (
        "a9f35020e77738259129c299c47aef1edade0fa04a4062480e2a05ccdc005105"
    ),
    "historical_candidates_per_hour": 16.53051836148971,
    "qualified_candidates_per_hour": 136.59507764334006,
    "qualified_speedup": 8.263205947706915,
    "minimum_observed_free_memory_bytes": 53552918528,
    "maximum_process_tree_rss_bytes": 31133659136,
    "exact_shape_memory_failure_incident": (
        "D:\\ChengboRemote\\runtime\\run_health_incidents\\"
        "20260807T044736_joint_program_phase_b_exact_shape_memory_gate_failure"
    ),
    "exact_shape_memory_failure_incident_sha256": (
        "2849e6c4203bba647d4d43d84afda99a5a26549e0f46d145d1154de46a628963"
    ),
    "failed_executor_workers": 12,
    "recovery_executor_workers": EXECUTOR_WORKERS,
    "failed_financial_results_reused": False,
    "memory_threshold_weakened": False,
}
FIXED_V0_PRODUCTION_CLOSURE_FILE_SHA256 = (
    "bda072cc965db1f915fd7495b47e91b862917d061534a396831d3ed7c287fc9f"
)
FIXED_V0_PRODUCTION_CLOSURE_PAYLOAD_SHA256 = (
    "bc81007a9ee8842ad1d9fcb844332c723cb6499a344bb61ede200e2030d4964e"
)


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    return path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _self_hashed(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    body = dict(payload)
    body[key] = stable_hash(body)
    return body


def _artifact(path: Path, *, root: Path) -> dict[str, Any]:
    return {
        "relative_path": path.relative_to(root).as_posix(),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _component_record(component: ProgramSourceComponentV0) -> dict[str, Any]:
    return {
        "role": component.role,
        "component_id": component.component_id,
        "route_id": component.route_id,
        "proposal_id": component.proposal_id,
        "trial_number": component.trial_number,
        "sampling_phase": component.sampling_phase,
        "generation_receipt_hash": component.generation_receipt_hash,
        "credit_eligible": component.credit_eligible,
        "primary": dict(component.primary),
        "control": dict(component.control),
    }


def _component_from_record(record: Mapping[str, Any]) -> ProgramSourceComponentV0:
    component = ProgramSourceComponentV0(
        role=str(record["role"]),
        primary=dict(record["primary"]),
        control=dict(record["control"]),
        proposal_id=str(record["proposal_id"]),
        trial_number=(
            None if record.get("trial_number") is None else int(record["trial_number"])
        ),
        sampling_phase=str(record["sampling_phase"]),
    )
    expected = {
        "component_id": component.component_id,
        "route_id": component.route_id,
        "generation_receipt_hash": component.generation_receipt_hash,
        "credit_eligible": component.credit_eligible,
    }
    drift = [key for key, value in expected.items() if record.get(key) != value]
    if drift:
        raise ValueError(f"component-pool record drift: {drift}")
    return component


def _validate_execution_contract_snapshot(payload: Mapping[str, Any]) -> None:
    body = dict(payload)
    expected = str(body.pop("contract_payload_sha256", ""))
    if expected != stable_hash(body):
        raise ValueError("execution contract snapshot self-hash mismatch")
    required = {
        "split_manifest_sha256",
        "session_authority_manifest",
        "session_authority_manifest_sha256",
        "session_authority_path",
        "universe_policy",
        "fee_schedule",
        "execution_policy",
        "corporate_action_policy",
    }
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"execution contract snapshot lacks fields: {missing}")
    def string_values(value: Any) -> Iterable[str]:
        if isinstance(value, Mapping):
            for item in value.values():
                yield from string_values(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                yield from string_values(item)
        elif isinstance(value, str):
            yield value.lower()

    serialized_values = tuple(string_values(payload))
    forbidden = ("historical_challenge_2023", "forward_b", "forward_2026")
    if any(
        marker in value
        for value in serialized_values
        for marker in forbidden
    ):
        raise ValueError("execution contract snapshot binds a prohibited asset")


def _development_windows(split_manifest: Path) -> dict[str, Any]:
    with split_manifest.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row.get("split") == "train"]
    if not rows:
        raise ValueError("split manifest has no development/train rows")
    dates = [str(row["trade_date"]) for row in rows]
    if dates != sorted(set(dates)):
        raise ValueError("development dates are not strictly ordered and unique")
    quotient = len(dates) // 3
    sizes = (quotient, quotient, len(dates) - 2 * quotient)
    windows = []
    offset = 0
    for ordinal, size in enumerate(sizes, start=1):
        segment = dates[offset : offset + size]
        windows.append(
            {
                "window_id": f"development_{ordinal}",
                "start_date": segment[0],
                "end_date": segment[-1],
                "session_count": len(segment),
            }
        )
        offset += size
    return {
        "development_start_date": dates[0],
        "development_end_date": dates[-1],
        "development_session_count": len(dates),
        "development_subwindows": windows,
    }


def _materialized_pair_ids(
    *,
    production_closure_path: Path,
    materialized_schema_path: Path,
    materialization_screen_path: Path,
) -> tuple[set[str], dict[str, Any], dict[str, Any]]:
    if _sha256(production_closure_path) != FIXED_V0_PRODUCTION_CLOSURE_FILE_SHA256:
        raise ValueError("fixed-V0 production closure file hash drift")
    closure = _read_json(production_closure_path)
    closure_body = dict(closure)
    closure_expected = str(closure_body.pop("closure_payload_sha256", ""))
    if (
        closure_expected != stable_hash(closure_body)
        or closure_expected != FIXED_V0_PRODUCTION_CLOSURE_PAYLOAD_SHA256
        or str(closure.get("status") or "")
        != "FIXED_STRATIFIED_PRODUCTION_V0_COMPLETE"
    ):
        raise ValueError("fixed-V0 production closure canonical drift")
    bound_schema = dict(closure.get("materialized_schema_binding") or {})
    bound_screen = dict(closure.get("materialization_screen") or {})
    if (
        _sha256(materialized_schema_path) != str(bound_schema.get("sha256") or "")
        or materialized_schema_path.stat().st_size
        != int(bound_schema.get("bytes") or 0)
    ):
        raise ValueError("fixed-V0 materialized schema snapshot drift")
    if (
        _sha256(materialization_screen_path)
        != str(bound_screen.get("sha256") or "")
        or materialization_screen_path.stat().st_size
        != int(bound_screen.get("bytes") or 0)
    ):
        raise ValueError("fixed-V0 materialization screen snapshot drift")
    schema = _read_json(materialized_schema_path)
    if (
        str(schema.get("status") or "")
        != "SCHEMA_FIRST_COMPATIBLE_POOLS_BOUND"
        or int(schema.get("validation_reads") or 0)
        or int(schema.get("holdout_reads") or 0)
        or int(schema.get("forward_2026_reads") or 0)
    ):
        raise ValueError("fixed-V0 materialized schema authority drift")
    screen = _read_json(materialization_screen_path)
    screen_body = dict(screen)
    screen_expected = str(screen_body.pop("screen_payload_sha256", ""))
    if (
        screen_expected != stable_hash(screen_body)
        or str(screen.get("status") or "")
        != "FIXED_COHORT_MATERIALIZATION_SCREEN_COMPLETE"
        or int(screen.get("materialized_pair_count") or 0) != 180
        or int(screen.get("frozen_pair_count") or 0) != 229
        or int(screen.get("validation_reads") or 0)
        or int(screen.get("holdout_reads") or 0)
        or int(screen.get("forward_2026_reads") or 0)
    ):
        raise ValueError("fixed-V0 materialization screen canonical drift")
    compatible = {
        str(row["pair_id"])
        for row in screen["pair_screen"]
        if str(row.get("status") or "") == "MATERIALIZED_PAIR_COMPATIBLE"
    }
    if len(compatible) != 180:
        raise ValueError("fixed-V0 compatible pair identity count drift")
    return compatible, screen, schema


def _regenerate_compatible_components(
    *,
    registry: UnifiedCapabilityRegistry,
    source_preflight_root: Path,
    root_contract_path: Path,
    compatible_pair_ids: set[str],
) -> tuple[list[ProgramSourceComponentV0], dict[str, Any]]:
    closure = verify_candidate_representation_v0_preflight(source_preflight_root)
    plan = _read_json(source_preflight_root / "fixed_stratified_plan_v0.json")
    root_authority = load_development_discovery_root_authority(
        root_contract_path.resolve(), registry=registry
    )
    regenerated = generate_fixed_stratified_epoch_v0(
        registry,
        plan=plan,
        route_root_allowlist=root_authority["route_root_allowlists"],
    )
    source_rows = _read_jsonl(
        source_preflight_root / "compatible_candidate_rows_v0.jsonl"
    )
    source_by_id = {
        str(row["candidate_id"]): row for row in source_rows
    }
    raw_by_id = {
        str(row["candidate_id"]): dict(row) for row in regenerated.candidate_rows
    }
    if set(source_by_id) - set(raw_by_id):
        raise ValueError("source preflight rows are absent from regenerated supply")
    regenerated_pair_ids = {str(row["pair_id"]) for row in regenerated.candidate_rows}
    if compatible_pair_ids - regenerated_pair_ids:
        raise ValueError("materialization screen references absent regenerated pairs")
    components: list[ProgramSourceComponentV0] = []
    seen_component_ids: set[str] = set()
    primary_rows = [
        row for row in regenerated.candidate_rows if not bool(row["is_matched_control"])
    ]
    for primary in primary_rows:
        candidate_id = str(primary["candidate_id"])
        control_id = str(primary["matched_control_id"])
        if str(primary["pair_id"]) not in compatible_pair_ids:
            continue
        control = raw_by_id[control_id]
        for raw, accepted in (
            (primary, source_by_id[candidate_id]),
            (control, source_by_id[control_id]),
        ):
            protected = (
                "candidate_id",
                "matched_control_id",
                "pair_id",
                "route_id",
                "exact_identity",
                "canonical_identity",
                "canonical_expression",
            )
            if any(str(raw.get(key)) != str(accepted.get(key)) for key in protected):
                raise ValueError("compatible component identity drift")
        role = ROUTE_ROLE[str(primary["route_id"])]
        proposal_id = str(
            source_by_id[candidate_id].get("candidate_proposal_v0_hash")
            or source_by_id[candidate_id].get("candidate_spec_v0_hash")
            or candidate_id
        )
        component = ProgramSourceComponentV0(
            role=role,
            primary=primary,
            control=control,
            proposal_id=proposal_id,
            trial_number=None,
            sampling_phase="AVAILABILITY_FIXED",
        )
        if component.component_id in seen_component_ids:
            continue
        seen_component_ids.add(component.component_id)
        components.append(component)
    components.sort(key=lambda row: (row.role, row.route_id, row.component_id))
    return components, closure


def _policy_for_ordinal(ordinal: int) -> dict[str, str]:
    return {
        **DEFAULT_COMBINATION_POLICY,
        "temporal": TEMPORAL_POLICIES[ordinal % len(TEMPORAL_POLICIES)],
        "market": MARKET_POLICIES[ordinal % len(MARKET_POLICIES)],
        "event_application": EVENT_POLICIES[ordinal % len(EVENT_POLICIES)],
    }


def _components_for_template(
    template_id: str,
    pools: Mapping[str, Sequence[ProgramSourceComponentV0]],
    *,
    template_ordinal: int,
    record_ordinal: int,
) -> dict[str, ProgramSourceComponentV0]:
    selected: dict[str, ProgramSourceComponentV0] = {}
    for role in PROGRAM_TEMPLATE_COMPONENTS[template_id]:
        pool = list(pools[role])
        if not pool:
            raise ValueError(f"Phase B has no executable {role} components")
        index = (template_ordinal * 8 + record_ordinal) % len(pool)
        selected[role] = pool[index]
    return selected


def _schedule_record(
    *,
    adapter: CandidateProgramProposalAdapterV0,
    compiler: ProgramCompilerV1,
    template_id: str,
    template_ordinal: int,
    record_ordinal: int,
    components: Mapping[str, ProgramSourceComponentV0],
    global_ordinal: int,
) -> dict[str, Any]:
    policy = _policy_for_ordinal(record_ordinal)
    program = adapter.compose(
        template_id,
        base_component=components["base"],
        temporal_component=components.get("temporal"),
        market_component=components.get("market"),
        event_component=components.get("event"),
        combination_policy=policy,
    )
    receipt = adapter.build_receipt(
        program_template_id=template_id,
        program=program,
        components=tuple(components[role] for role in PROGRAM_TEMPLATE_COMPONENTS[template_id]),
        combination_policy=policy,
        batch_id="PHASE_B_UNIFORM_64_V0",
        ask_ordinal=global_ordinal,
        generation_arm="UNIFORM_FRESH",
    )
    receipt_record = receipt.to_record()
    ProgramProposalReceiptV0.from_record(receipt_record)
    compiled = compiler.compile(program)
    record: dict[str, Any] = {
        "schema_version": "cn_joint_program_phase_b_schedule_record_v0",
        "main_record_ordinal": global_ordinal,
        "template_id": template_id,
        "template_record_ordinal": record_ordinal,
        "generation_arm": "UNIFORM_FRESH",
        "adaptive_template_credit_used": False,
        "raw_composition_attempt": 1,
        "maximum_raw_composition_attempts": 64,
        "combination_policy": policy,
        "components": {
            role: {
                "component_id": component.component_id,
                "route_id": component.route_id,
                "proposal_id": component.proposal_id,
                "generation_receipt_hash": component.generation_receipt_hash,
            }
            for role, component in components.items()
        },
        "proposal_receipt": receipt_record,
        "primary_program": program.to_record(),
        "primary_compiled": compiled.to_record(),
        "semantic_noop": False,
    }
    if template_id == "BASE":
        legacy_control = legacy_candidate_program_v1(
            components["base"].control,
            portfolio_contract=adapter.portfolio_contract,
        )
        record.update(
            {
                "record_kind": "BASE_WRAPPER_PARITY",
                "legacy_primary_candidate": dict(components["base"].primary),
                "legacy_control_candidate": dict(components["base"].control),
                "control_program": legacy_control.to_record(),
                "control_compiled": compiler.compile(legacy_control).to_record(),
                "pair_id": str(components["base"].primary["pair_id"]),
            }
        )
    else:
        matched = construct_matched_control_program_v1(program)
        control_compiled = compiler.compile(matched.control)
        if matched.primary.semantic_program_hash == matched.control.semantic_program_hash:
            raise ValueError("enhanced Phase B control is a semantic no-op")
        record.update(
            {
                "record_kind": "ENHANCED_FULL_BASE_PAIR",
                "control_program": matched.control.to_record(),
                "control_compiled": control_compiled.to_record(),
                "pair_id": matched.pair_id,
                "diagnostic_only": matched.diagnostic_only,
            }
        )
    record["schedule_record_sha256"] = stable_hash(record)
    return record


def build_phase_b_prefinancial_freeze_v0(
    *,
    output_root: Path,
    phase_a_root: Path,
    source_preflight_root: Path,
    registry_path: Path,
    root_contract_path: Path,
    split_manifest_path: Path,
    execution_contract_snapshot_path: Path,
    fixed_v0_production_closure_snapshot_path: Path,
    materialized_schema_snapshot_path: Path,
    materialization_screen_snapshot_path: Path,
    node_resource_profiles_path: Path,
    repo_sha: str,
    remote_train_session_field_root: str,
) -> dict[str, Any]:
    root = output_root.resolve()
    if root.exists() and any(root.iterdir()):
        raise FileExistsError(f"Phase B freeze root is not fresh: {root}")
    root.mkdir(parents=True, exist_ok=True)
    phase_a_audit_path = root / "phase_a_reverification.json"
    phase_a = verify_phase_a_v0(
        phase_a_root=phase_a_root.resolve(),
        registry_path=registry_path.resolve(),
        root_contract_path=root_contract_path.resolve(),
        audit_output=phase_a_audit_path,
    )
    registry = UnifiedCapabilityRegistry.read(registry_path.resolve())
    execution_contract = _read_json(execution_contract_snapshot_path.resolve())
    _validate_execution_contract_snapshot(execution_contract)
    if str(execution_contract["split_manifest_sha256"]) != _sha256(
        split_manifest_path.resolve()
    ):
        raise ValueError("execution contract/split manifest binding drift")
    normalized_remote_field_root = remote_train_session_field_root.replace(
        "/", "\\"
    ).lower()
    if (
        not normalized_remote_field_root.startswith("d:\\chengboremote\\runtime\\")
        or not normalized_remote_field_root.endswith(
            "\\session_time_major_train_v1"
        )
        or any(
            marker in normalized_remote_field_root
            for marker in (
                "validation",
                "holdout",
                "historical_challenge_2023",
                "forward_b",
                "forward_2026",
            )
        )
    ):
        raise ValueError("remote train session field root is not development-only")
    node_profiles = _read_json(node_resource_profiles_path.resolve())
    node_profile_body = dict(node_profiles)
    node_profile_expected = str(
        node_profile_body.pop("capacity_manifest_sha256", "")
    )
    if node_profile_expected != stable_hash(node_profile_body):
        raise ValueError("node-resource capacity manifest self-hash mismatch")
    profile = dict(node_profiles.get("profiles", {}).get(RESOURCE_PROFILE) or {})
    if (
        int(profile.get("cpu_threads") or 0) != ENTITLEMENT_THREADS
        or int(profile.get("minimum_free_memory_bytes") or 0)
        != MINIMUM_FREE_MEMORY_BYTES
        or str(profile.get("concurrency_contract") or "") != "EXCLUSIVE_HEAVY_LANE"
    ):
        raise ValueError("Phase B node-resource profile drift")
    if int(ACCELERATION_BASIS["minimum_observed_free_memory_bytes"]) < MINIMUM_FREE_MEMORY_BYTES:
        raise ValueError("accepted acceleration basis violates the memory gate")
    if float(ACCELERATION_BASIS["qualified_speedup"]) < 1.5:
        raise ValueError("accepted acceleration basis lacks minimum speedup")

    (
        compatible_pair_ids,
        accepted_materialization_screen,
        accepted_materialized_schema,
    ) = _materialized_pair_ids(
        production_closure_path=fixed_v0_production_closure_snapshot_path.resolve(),
        materialized_schema_path=materialized_schema_snapshot_path.resolve(),
        materialization_screen_path=materialization_screen_snapshot_path.resolve(),
    )
    accepted_remote_session_root = str(
        (
            accepted_materialized_schema.get("backends", {}).get(
                "stock_session", {}
            )
        ).get("root")
        or ""
    )
    if (
        accepted_remote_session_root.replace("/", "\\").lower()
        != remote_train_session_field_root.replace("/", "\\").lower()
    ):
        raise ValueError(
            "remote train session field root is not the accepted materialized root"
        )
    components, source_closure = _regenerate_compatible_components(
        registry=registry,
        source_preflight_root=source_preflight_root.resolve(),
        root_contract_path=root_contract_path.resolve(),
        compatible_pair_ids=compatible_pair_ids,
    )
    pool_records = [_component_record(component) for component in components]
    role_counts = {
        role: sum(1 for component in components if component.role == role)
        for role in ROLE_TARGETS
    }
    route_counts = {
        route: sum(1 for component in components if component.route_id == route)
        for route in ROUTE_ROLE
    }
    pool_manifest = _self_hashed(
        {
            "schema_version": "cn_joint_program_component_pool_manifest_v0",
            "status": "JOINT_PROGRAM_COMPONENT_POOLS_FROZEN",
            "source_preflight_root": str(source_preflight_root.resolve()),
            "source_preflight_closure_sha256": _sha256(
                source_preflight_root.resolve()
                / "CANDIDATE_REPRESENTATION_V0_PREFLIGHT_COMPLETE.json"
            ),
            "source_preflight_closure_payload_sha256": str(
                source_closure["closure_payload_sha256"]
            ),
            "fixed_v0_production_closure_snapshot_file_sha256": _sha256(
                fixed_v0_production_closure_snapshot_path.resolve()
            ),
            "materialized_schema_snapshot_file_sha256": _sha256(
                materialized_schema_snapshot_path.resolve()
            ),
            "materialization_screen_snapshot_file_sha256": _sha256(
                materialization_screen_snapshot_path.resolve()
            ),
            "accepted_materialized_pair_count": len(compatible_pair_ids),
            "accepted_stock_session_field_root": accepted_remote_session_root,
            "accepted_materialization_screen_payload_sha256": str(
                accepted_materialization_screen["screen_payload_sha256"]
            ),
            "registry_hash": registry.registry_hash,
            "role_targets": ROLE_TARGETS,
            "role_counts": role_counts,
            "role_underfill": {
                role: max(0, ROLE_TARGETS[role] - role_counts[role])
                for role in ROLE_TARGETS
            },
            "route_counts": route_counts,
            "component_count": len(components),
            "component_identity_unique": (
                len({component.component_id for component in components})
                == len(components)
            ),
            "joint_economic_feedback_used": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "component_pool_manifest_sha256",
    )
    pool_path = _write_jsonl(root / "source_component_pool.jsonl", pool_records)
    pool_manifest_path = _write_json(root / "component_pool_manifest.json", pool_manifest)

    executable_pools = {
        role: tuple(
            component
            for component in components
            if component.role == role and component.route_id in SESSION_EXECUTABLE_ROUTES
        )
        for role in ROLE_TARGETS
    }
    if any(len(pool) < 8 for pool in executable_pools.values()):
        raise ValueError(
            "Phase B needs at least eight all-session executable components per role"
        )
    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    schedule: list[dict[str, Any]] = []
    global_ordinal = 0
    for template_ordinal, template_id in enumerate(TEMPLATE_ORDER):
        for record_ordinal in range(8):
            selected = _components_for_template(
                template_id,
                executable_pools,
                template_ordinal=template_ordinal,
                record_ordinal=record_ordinal,
            )
            schedule.append(
                _schedule_record(
                    adapter=adapter,
                    compiler=compiler,
                    template_id=template_id,
                    template_ordinal=template_ordinal,
                    record_ordinal=record_ordinal,
                    components=selected,
                    global_ordinal=global_ordinal,
                )
            )
            global_ordinal += 1
    if len(schedule) != 64:
        raise ValueError("Phase B schedule must contain exactly 64 main records")
    schedule_by_template = {
        template_id: sum(1 for row in schedule if row["template_id"] == template_id)
        for template_id in TEMPLATE_ORDER
    }
    if any(count != 8 for count in schedule_by_template.values()):
        raise ValueError("Phase B template quota drift")
    primary_semantics = [
        str(row["primary_program"]["semantic_program_hash"]) for row in schedule
    ]
    if len(set(primary_semantics)) != len(primary_semantics):
        raise ValueError("Phase B schedule contains duplicate full-program semantics")
    schedule_path = _write_jsonl(root / "phase_b_uniform_schedule.jsonl", schedule)
    proposal_receipt_path = _write_jsonl(
        root / "program_proposal_receipts.jsonl",
        (row["proposal_receipt"] for row in schedule),
    )

    windows = _development_windows(split_manifest_path.resolve())
    prefinancial_contract = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_run_contract_v0",
            "status": "PHASE_B_INPUTS_FROZEN_BEFORE_FINANCIAL_READ",
            "repo_sha": repo_sha,
            **windows,
            "development_subwindow_rule": "EQUAL_SESSION_COUNT_121_121_122",
            "split_manifest_path": str(split_manifest_path.resolve()),
            "split_manifest_file_sha256": _sha256(split_manifest_path.resolve()),
            "execution_contract_snapshot_path": str(
                execution_contract_snapshot_path.resolve()
            ),
            "execution_contract_snapshot_file_sha256": _sha256(
                execution_contract_snapshot_path.resolve()
            ),
            "execution_contract_payload_sha256": str(
                execution_contract["contract_payload_sha256"]
            ),
            "remote_train_session_field_root": remote_train_session_field_root,
            "session_authority_manifest": str(
                execution_contract["session_authority_manifest"]
            ),
            "session_authority_manifest_sha256": str(
                execution_contract["session_authority_manifest_sha256"]
            ),
            "session_authority_path": str(execution_contract["session_authority_path"]),
            "universe_policy": dict(execution_contract["universe_policy"]),
            "fee_schedule": dict(execution_contract["fee_schedule"]),
            "execution_policy": dict(execution_contract["execution_policy"]),
            "corporate_action_policy": dict(
                execution_contract["corporate_action_policy"]
            ),
            "portfolio_decoder_id": "TOPK_10_EQUAL",
            "ending_book_policy": "FINAL_CLOSE_MARK_TO_MARKET_NO_FORCED_SALE",
            "component_pool_manifest_sha256": pool_manifest[
                "component_pool_manifest_sha256"
            ],
            "component_pool_file_sha256": _sha256(pool_path),
            "program_schedule_file_sha256": _sha256(schedule_path),
            "program_proposal_receipt_file_sha256": _sha256(proposal_receipt_path),
            "program_template_order": list(TEMPLATE_ORDER),
            "template_quotas": schedule_by_template,
            "seed": 1729,
            "generation_arm": "UNIFORM_FRESH_ONLY_NO_ADAPTIVE_ALLOCATION",
            "raw_composition_attempt_cap_per_template": 64,
            "reward_formula": (
                "min(primary_continuous_book_net_reward,matched_net_reward_increment)"
            ),
            "blocked_noop_tell_rule": "RECORD_BLOCKER_NO_BANDIT_UPDATE",
            "semantic_noop_quota_rule": "DO_NOT_COUNT_TOWARD_TEMPLATE_QUOTA",
            "source_route_sampling_phases": (
                "AVAILABILITY_FIXED|STARTUP_RANDOM|TPE_GUIDED"
            ),
            "resource_profile": RESOURCE_PROFILE,
            "entitlement_threads": ENTITLEMENT_THREADS,
            "executor_backend": "PROCESS_POOL",
            "executor_workers": EXECUTOR_WORKERS,
            "native_threads_per_worker": NATIVE_THREADS_PER_WORKER,
            "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
            "cache_cap_bytes": CACHE_CAP_BYTES,
            "node_resource_capacity_manifest_sha256": str(
                node_profiles["capacity_manifest_sha256"]
            ),
            "acceleration_basis": ACCELERATION_BASIS,
            "minimum_qualified_speedup": 1.5,
            "checkpoint_policy": "IMMUTABLE_RECORD_FILES_AND_ROOT_CLOSURE",
            "resume_policy": "SAME_ROOT_ONLY_FROM_VERIFIED_COMPLETE_RECORDS",
            "incomplete_financial_result_reuse": False,
            "campaign_local_route_ask_tell_allowed": True,
            "campaign_local_route_snapshots_allowed": True,
            "campaign_local_program_bandit_state_allowed": True,
            "cross_campaign_optimizer_state_import": False,
            "formal_optimizer_authority_write": False,
            "formal_scheduler_authority_write": False,
            "archive_write": False,
            "promotion_write": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "run_contract_sha256",
    )
    contract_path = _write_json(root / "phase_b_run_contract.json", prefinancial_contract)
    access_ledger = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_prefinancial_access_ledger_v0",
            "financial_evaluation_executed": False,
            "market_price_rows_read": 0,
            "label_rows_read": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "optimizer_feedback_consumed": False,
            "bandit_feedback_consumed": False,
        },
        "access_ledger_sha256",
    )
    access_path = _write_json(root / "access_ledger.json", access_ledger)
    summary = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_prefinancial_summary_v0",
            "status": "PHASE_B_PREFINANCIAL_READY",
            "repo_sha": repo_sha,
            "phase_a_status": str(phase_a["status"]),
            "component_role_counts": role_counts,
            "component_role_underfill": pool_manifest["role_underfill"],
            "session_executable_component_counts": {
                role: len(pool) for role, pool in executable_pools.items()
            },
            "main_record_count": len(schedule),
            "base_parity_record_count": sum(
                row["record_kind"] == "BASE_WRAPPER_PARITY" for row in schedule
            ),
            "enhanced_full_base_pair_count": sum(
                row["record_kind"] == "ENHANCED_FULL_BASE_PAIR" for row in schedule
            ),
            "template_quotas": schedule_by_template,
            "all_programs_compile": True,
            "all_controls_compile": True,
            "all_receipts_replay": True,
            "all_selected_components_stock_session": True,
            "financial_evaluation_executed": False,
            "sealed_reads": 0,
        },
        "summary_sha256",
    )
    summary_path = _write_json(root / "summary.json", summary)
    artifacts = sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.name != FREEZE_CLOSURE_NAME
    )
    manifest = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_prefinancial_artifact_manifest_v0",
            "artifacts": [_artifact(path, root=root) for path in artifacts],
        },
        "artifact_manifest_sha256",
    )
    manifest_path = _write_json(root / "ARTIFACT_MANIFEST.json", manifest)
    # Include the manifest itself in closure declarations, not in its own body.
    closure = _self_hashed(
        {
            "schema_version": FREEZE_SCHEMA,
            "status": FREEZE_STATUS,
            "repo_sha": repo_sha,
            "output_root": str(root),
            "artifact_manifest": _artifact(manifest_path, root=root),
            "component_pool_manifest": _artifact(pool_manifest_path, root=root),
            "source_component_pool": _artifact(pool_path, root=root),
            "phase_b_run_contract": _artifact(contract_path, root=root),
            "phase_b_uniform_schedule": _artifact(schedule_path, root=root),
            "program_proposal_receipts": _artifact(proposal_receipt_path, root=root),
            "summary": _artifact(summary_path, root=root),
            "access_ledger": _artifact(access_path, root=root),
            "artifact_count": len(manifest["artifacts"]),
            "main_record_count": 64,
            "base_parity_record_count": 8,
            "enhanced_full_base_pair_count": 56,
            "financial_evaluation_executed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "closure_sha256",
    )
    _write_json(root / FREEZE_CLOSURE_NAME, closure)
    return closure


def verify_phase_b_prefinancial_freeze_v0(root: Path) -> dict[str, Any]:
    root = root.resolve()
    closure_path = root / FREEZE_CLOSURE_NAME
    closure = _read_json(closure_path)
    body = dict(closure)
    expected = str(body.pop("closure_sha256", ""))
    if expected != stable_hash(body):
        raise ValueError("Phase B freeze closure self-hash mismatch")
    if closure.get("schema_version") != FREEZE_SCHEMA or closure.get("status") != FREEZE_STATUS:
        raise ValueError("Phase B freeze closure status/version mismatch")
    manifest_path = root / str(closure["artifact_manifest"]["relative_path"])
    if _sha256(manifest_path) != str(closure["artifact_manifest"]["sha256"]):
        raise ValueError("Phase B freeze artifact-manifest file drift")
    manifest = _read_json(manifest_path)
    manifest_body = dict(manifest)
    manifest_expected = str(manifest_body.pop("artifact_manifest_sha256", ""))
    if manifest_expected != stable_hash(manifest_body):
        raise ValueError("Phase B freeze artifact-manifest self-hash mismatch")
    for artifact in manifest["artifacts"]:
        path = root / str(artifact["relative_path"])
        if not path.is_file() or path.stat().st_size != int(artifact["size_bytes"]):
            raise ValueError(f"Phase B freeze artifact absent/size drift: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise ValueError(f"Phase B freeze artifact hash drift: {path}")
    components = [
        _component_from_record(row)
        for row in _read_jsonl(root / "source_component_pool.jsonl")
    ]
    component_by_id = {component.component_id: component for component in components}
    if len(component_by_id) != len(components):
        raise ValueError("Phase B component pool identity duplication")
    schedule = _read_jsonl(root / "phase_b_uniform_schedule.jsonl")
    if len(schedule) != 64:
        raise ValueError("Phase B schedule record count drift")
    for ordinal, row in enumerate(schedule):
        row_body = dict(row)
        row_expected = str(row_body.pop("schedule_record_sha256", ""))
        if row_expected != stable_hash(row_body):
            raise ValueError("Phase B schedule record self-hash mismatch")
        if int(row["main_record_ordinal"]) != ordinal:
            raise ValueError("Phase B schedule order drift")
        receipt = ProgramProposalReceiptV0.from_record(row["proposal_receipt"])
        if receipt.program_id != str(row["primary_program"]["program_id"]):
            raise ValueError("Phase B receipt/program identity drift")
        for component in row["components"].values():
            component_id = str(component["component_id"])
            if component_id not in component_by_id:
                raise ValueError("Phase B schedule references absent component")
            frozen = component_by_id[component_id]
            if (
                frozen.route_id != str(component["route_id"])
                or frozen.generation_receipt_hash
                != str(component["generation_receipt_hash"])
            ):
                raise ValueError("Phase B schedule component binding drift")
    contract = _read_json(root / "phase_b_run_contract.json")
    contract_body = dict(contract)
    contract_expected = str(contract_body.pop("run_contract_sha256", ""))
    if contract_expected != stable_hash(contract_body):
        raise ValueError("Phase B run contract self-hash mismatch")
    if (
        int(contract["validation_reads"])
        or int(contract["holdout_reads"])
        or int(contract["historical_2023_reads"])
        or int(contract["forward_b_reads"])
        or int(contract["forward_2026_reads"])
        or bool(contract["incomplete_financial_result_reuse"])
    ):
        raise ValueError("Phase B run contract enables prohibited access/reuse")
    access = _read_json(root / "access_ledger.json")
    access_body = dict(access)
    access_expected = str(access_body.pop("access_ledger_sha256", ""))
    if access_expected != stable_hash(access_body) or any(
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
        raise ValueError("Phase B prefinancial access ledger drift")
    return closure


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--output-root", type=Path, required=True)
    freeze.add_argument("--phase-a-root", type=Path, required=True)
    freeze.add_argument("--source-preflight-root", type=Path, required=True)
    freeze.add_argument("--registry", type=Path, required=True)
    freeze.add_argument("--root-contract", type=Path, required=True)
    freeze.add_argument("--split-manifest", type=Path, required=True)
    freeze.add_argument("--execution-contract-snapshot", type=Path, required=True)
    freeze.add_argument(
        "--fixed-v0-production-closure-snapshot", type=Path, required=True
    )
    freeze.add_argument("--materialized-schema-snapshot", type=Path, required=True)
    freeze.add_argument(
        "--materialization-screen-snapshot", type=Path, required=True
    )
    freeze.add_argument("--node-resource-profiles", type=Path, required=True)
    freeze.add_argument("--repo-sha", required=True)
    freeze.add_argument("--remote-train-session-field-root", required=True)
    verify = subparsers.add_parser("verify-freeze")
    verify.add_argument("--phase-b-freeze-root", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        closure = build_phase_b_prefinancial_freeze_v0(
            output_root=args.output_root,
            phase_a_root=args.phase_a_root,
            source_preflight_root=args.source_preflight_root,
            registry_path=args.registry,
            root_contract_path=args.root_contract,
            split_manifest_path=args.split_manifest,
            execution_contract_snapshot_path=args.execution_contract_snapshot,
            fixed_v0_production_closure_snapshot_path=(
                args.fixed_v0_production_closure_snapshot
            ),
            materialized_schema_snapshot_path=args.materialized_schema_snapshot,
            materialization_screen_snapshot_path=(
                args.materialization_screen_snapshot
            ),
            node_resource_profiles_path=args.node_resource_profiles,
            repo_sha=args.repo_sha,
            remote_train_session_field_root=args.remote_train_session_field_root,
        )
    else:
        closure = verify_phase_b_prefinancial_freeze_v0(args.phase_b_freeze_root)
    print(json.dumps(closure, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
