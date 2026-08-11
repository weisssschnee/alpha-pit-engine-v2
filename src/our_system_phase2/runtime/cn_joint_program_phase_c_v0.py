"""Zero-financial Phase-C freeze for CN joint candidate programs.

This module binds the independently accepted Phase-B result, reuses its frozen
component pools and materialization authority, derives a campaign-local bandit
seed only from complete development records, and freezes the exact 512-record
Phase-C ask budget plus a deterministic raw-program reservoir.  It never opens
market prices, labels, validation, holdout, historical challenge, Forward-B or
2026 assets.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import (
    CACHE_CAP_BYTES,
    ENTITLEMENT_THREADS,
    ENHANCED_TEMPLATE_ORDER,
    EXECUTOR_WORKERS,
    FREEZE_CLOSURE_NAME as PHASE_B_FREEZE_CLOSURE_NAME,
    MINIMUM_FREE_MEMORY_BYTES,
    NATIVE_THREADS_PER_WORKER,
    RESOURCE_PROFILE,
    SESSION_EXECUTABLE_ROUTES,
    TEMPLATE_ORDER,
    _component_from_record,
    verify_phase_b_prefinancial_freeze_v0,
)
from our_system_phase2.services.candidate_program_controls_v1 import (
    construct_matched_control_program_v1,
)
from our_system_phase2.services.candidate_materialization_requirements import (
    resolve_required_physical_leaves,
)
from our_system_phase2.services.candidate_program_materialization_v1 import (
    ADAPTER_ALREADY_MATERIALIZED,
    materialization_adapter_v1,
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
from our_system_phase2.services.development_feedback_provenance import (
    build_development_feedback_provenance,
)
from our_system_phase2.services.program_factorized_bandit_v0 import (
    BANDIT_POLICY_ID,
    BANDIT_VERSION,
    ProgramFactorizedBanditV0,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


FREEZE_SCHEMA = "cn_joint_program_phase_c_prefinancial_freeze_v0"
FREEZE_STATUS = "CN_JOINT_PROGRAM_PHASE_C_PREFINANCIAL_FREEZE_COMPLETE"
FREEZE_CLOSURE_NAME = "CN_JOINT_PROGRAM_PHASE_C_PREFINANCIAL_FREEZE_COMPLETE.json"
PHASE_B_RESULT_CLOSURE_NAME = "CN_JOINT_PROGRAM_PHASE_B_COMPLETE.json"

PHASE_C_CAMPAIGN_ID = "CN_JOINT_PROGRAM_ROLLING_SEARCH_V0"
PHASE_C_BATCH_ID = "PHASE_C_FACTORIZED_COMPARISON_512_V0"
EXPECTED_RECORDS = 512
BASE_RECORDS = 64
ENHANCED_RECORDS_PER_TEMPLATE = 64
RAW_RESERVOIR_PER_ENHANCED_TEMPLATE = 512
RECORDS_PER_CHECKPOINT = 8
CHECKPOINT_COUNT = EXPECTED_RECORDS // RECORDS_PER_CHECKPOINT
MAX_VARIANTS_PER_BASE_PER_TEMPLATE = 4
MIN_BASE_IDENTITIES_PER_TEMPLATE = 16
MATERIALIZATION_RECORDS_PER_TEMPLATE = 32
MATERIALIZATION_SCHEDULE_RECORDS = (
    len(TEMPLATE_ORDER) * MATERIALIZATION_RECORDS_PER_TEMPLATE
)

TEMPORAL_POLICIES = ("ADD", "SUBTRACT", "MIN", "MAX")
MARKET_POLICIES = ("GATE", "FILTER", "VETO", "MODULATE")
EVENT_POLICIES = ("GATE", "FILTER")


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


def _verify_self_hash(payload: Mapping[str, Any], key: str, label: str) -> None:
    body = dict(payload)
    claimed = str(body.pop(key, ""))
    if len(claimed) != 64 or stable_hash(body) != claimed:
        raise ValueError(f"{label} self-hash drift")


def _artifact_binding_path(binding: Mapping[str, Any]) -> str:
    value = str(binding.get("relative_path") or binding.get("path") or "")
    if not value:
        raise ValueError("artifact binding lacks a relative path")
    return value


def _artifact_binding_size(binding: Mapping[str, Any]) -> int:
    value = binding.get("size_bytes")
    if value is None:
        value = binding.get("bytes")
    if value is None or int(value) < 0:
        raise ValueError("artifact binding lacks a valid size")
    return int(value)


def phase_c_generation_arm_v0(
    template_id: str, template_record_ordinal: int
) -> str:
    ordinal = int(template_record_ordinal)
    if template_id == "BASE":
        if ordinal not in range(BASE_RECORDS):
            raise ValueError("Phase C BASE ordinal is out of range")
        return "UNIFORM_FRESH"
    if template_id not in ENHANCED_TEMPLATE_ORDER or ordinal not in range(
        ENHANCED_RECORDS_PER_TEMPLATE
    ):
        raise ValueError("Phase C enhanced template/ordinal is out of range")
    if ordinal < 16:
        return "UNIFORM_FRESH"
    if ordinal < 40:
        return "FACTORIZED_EXPLOIT"
    if ordinal < 52:
        return "UNIFORM_FRESH"
    return "NOVELTY_RESERVE"


def build_phase_c_ask_plan_v0() -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    global_ordinal = 0
    for template_id in TEMPLATE_ORDER:
        quota = BASE_RECORDS if template_id == "BASE" else ENHANCED_RECORDS_PER_TEMPLATE
        for template_record_ordinal in range(quota):
            arm = phase_c_generation_arm_v0(template_id, template_record_ordinal)
            row = {
                "schema_version": "cn_joint_program_phase_c_ask_v0",
                "main_record_ordinal": global_ordinal,
                "checkpoint_ordinal": global_ordinal // RECORDS_PER_CHECKPOINT,
                "template_id": template_id,
                "template_record_ordinal": template_record_ordinal,
                "generation_arm": arm,
                "baseline_comparison_member": bool(
                    template_id != "BASE" and template_record_ordinal < 16
                ),
                "bandit_feedback_eligible": template_id != "BASE",
                "early_template_cancellation_allowed": False,
                "maximum_variants_per_base_per_template": (
                    MAX_VARIANTS_PER_BASE_PER_TEMPLATE
                ),
            }
            row["ask_record_sha256"] = stable_hash(row)
            rows.append(row)
            global_ordinal += 1
    if len(rows) != EXPECTED_RECORDS:
        raise RuntimeError("Phase C ask-plan cardinality drift")
    return tuple(rows)


def _raw_cursor_after_policy_axes(
    template_id: str, raw_ordinal: int, base_count: int
) -> tuple[dict[str, str], int]:
    roles = PROGRAM_TEMPLATE_COMPONENTS[template_id]
    cursor = int(raw_ordinal) // max(int(base_count), 1)
    policy = dict(DEFAULT_COMBINATION_POLICY)
    if "temporal" in roles:
        policy["temporal"] = TEMPORAL_POLICIES[cursor % len(TEMPORAL_POLICIES)]
        cursor //= len(TEMPORAL_POLICIES)
    if "market" in roles:
        policy["market"] = MARKET_POLICIES[cursor % len(MARKET_POLICIES)]
        cursor //= len(MARKET_POLICIES)
    if "event" in roles:
        policy["event_application"] = EVENT_POLICIES[
            cursor % len(EVENT_POLICIES)
        ]
        cursor //= len(EVENT_POLICIES)
    return policy, cursor


def _pool_by_role(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, tuple[ProgramSourceComponentV0, ...]]:
    pools: dict[str, list[ProgramSourceComponentV0]] = {
        role: [] for role in ("base", "temporal", "market", "event")
    }
    for row in rows:
        if str(row.get("route_id") or "") not in SESSION_EXECUTABLE_ROUTES:
            continue
        component = _component_from_record(row)
        pools[component.role].append(component)
    output = {
        role: tuple(sorted(values, key=lambda value: value.component_id))
        for role, values in pools.items()
    }
    if len(output["base"]) < MIN_BASE_IDENTITIES_PER_TEMPLATE:
        raise ValueError("Phase C needs at least 16 executable base components")
    if any(len(output[role]) < 8 for role in ("temporal", "market", "event")):
        raise ValueError("Phase C enhancer component supply is underfilled")
    return output


def _session_executable_component_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    accepted = [
        dict(row)
        for row in rows
        if str(row.get("route_id") or "") in SESSION_EXECUTABLE_ROUTES
    ]
    pools = _pool_by_role(accepted)
    accepted_ids = [
        str(_component_from_record(row).component_id) for row in accepted
    ]
    if len(set(accepted_ids)) != len(accepted_ids):
        raise ValueError("Phase C executable component identity duplication")
    expected_count = sum(len(pool) for pool in pools.values())
    if len(accepted) != expected_count:
        raise ValueError("Phase C executable component role coverage drift")
    return accepted


def _components_for_raw_ordinal(
    template_id: str,
    pools: Mapping[str, Sequence[ProgramSourceComponentV0]],
    raw_ordinal: int,
) -> dict[str, ProgramSourceComponentV0]:
    roles = PROGRAM_TEMPLATE_COMPONENTS[template_id]
    ordinal = int(raw_ordinal)
    base_count = len(tuple(pools["base"]))
    _, cursor = _raw_cursor_after_policy_axes(template_id, ordinal, base_count)
    selected: dict[str, ProgramSourceComponentV0] = {}
    for role in roles:
        pool = tuple(pools[role])
        if role == "base":
            index = ordinal % len(pool)
        else:
            index = cursor % len(pool)
            cursor //= len(pool)
        selected[role] = pool[index]
    return selected


def _reservoir_record(
    *,
    template_id: str,
    raw_ordinal: int,
    components: Mapping[str, ProgramSourceComponentV0],
    combination_policy: Mapping[str, str],
) -> dict[str, Any]:
    policy = dict(combination_policy)
    record: dict[str, Any] = {
        "schema_version": "cn_joint_program_phase_c_reservoir_record_v0",
        "template_id": template_id,
        "raw_reservoir_ordinal": int(raw_ordinal),
        "components": {
            role: {
                "component_id": component.component_id,
                "route_id": component.route_id,
                "proposal_id": component.proposal_id,
                "generation_receipt_hash": component.generation_receipt_hash,
            }
            for role, component in components.items()
        },
        "combination_policy": policy,
        "semantic_compile_required_at_selection": True,
        "semantic_noop_does_not_count_toward_quota": True,
        "financial_evaluation_executed": False,
    }
    record["raw_combination_sha256"] = stable_hash(
        {
            "template_id": template_id,
            "component_ids": {
                role: component.component_id
                for role, component in sorted(components.items())
            },
            "combination_policy": policy,
        }
    )
    record["reservoir_record_sha256"] = stable_hash(record)
    return record


def _build_reservoir(
    *,
    registry: UnifiedCapabilityRegistry,
    pools: Mapping[str, Sequence[ProgramSourceComponentV0]],
    base_record_count: int = BASE_RECORDS,
    enhanced_record_count: int = RAW_RESERVOIR_PER_ENHANCED_TEMPLATE,
    raw_ordinal_offset: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if int(base_record_count) <= 0 or int(enhanced_record_count) <= 0:
        raise ValueError("reservoir record counts must be positive")
    if int(raw_ordinal_offset) < 0:
        raise ValueError("reservoir raw ordinal offset must be non-negative")
    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    records: list[dict[str, Any]] = []
    compile_fixtures: list[dict[str, Any]] = []
    for template_id in TEMPLATE_ORDER:
        target = (
            int(base_record_count)
            if template_id == "BASE"
            else int(enhanced_record_count)
        )
        seen: set[str] = set()
        raw_cursor = int(raw_ordinal_offset)
        attempts = 0
        template_record_count = 0
        maximum_attempts = target * 16
        while (
            (
                template_record_count < target
                if template_id == "BASE"
                else len(seen) < target
            )
            and attempts < maximum_attempts
        ):
            components = _components_for_raw_ordinal(
                template_id, pools, raw_cursor
            )
            policy, _ = _raw_cursor_after_policy_axes(
                template_id, raw_cursor, len(tuple(pools["base"]))
            )
            record = _reservoir_record(
                template_id=template_id,
                raw_ordinal=raw_cursor,
                components=components,
                combination_policy=policy,
            )
            combination_hash = str(record["raw_combination_sha256"])
            raw_cursor += 1
            attempts += 1
            if template_id != "BASE" and combination_hash in seen:
                continue
            seen.add(combination_hash)
            record["template_reservoir_ordinal"] = template_record_count
            if template_id == "BASE":
                record["base_parity_repeat_ordinal"] = (
                    template_record_count // len(tuple(pools["base"]))
                )
            body = dict(record)
            body.pop("reservoir_record_sha256")
            record["reservoir_record_sha256"] = stable_hash(body)
            records.append(record)
            template_record_count += 1
            if len(seen) == 1:
                program = adapter.compose(
                    template_id,
                    base_component=components["base"],
                    temporal_component=components.get("temporal"),
                    market_component=components.get("market"),
                    event_component=components.get("event"),
                    combination_policy=record["combination_policy"],
                )
                if template_id == "BASE":
                    control = legacy_candidate_program_v1(
                        components["base"].control,
                        portfolio_contract=adapter.portfolio_contract,
                    )
                else:
                    control = construct_matched_control_program_v1(program).control
                fixture = {
                    "schema_version": "cn_joint_program_phase_c_compile_fixture_v0",
                    "template_id": template_id,
                    "primary_semantic_program_hash": program.semantic_program_hash,
                    "control_semantic_program_hash": control.semantic_program_hash,
                    "primary_compiled": compiler.compile(program).to_record(),
                    "control_compiled": compiler.compile(control).to_record(),
                    "financial_evaluation_executed": False,
                }
                fixture["compile_fixture_sha256"] = stable_hash(fixture)
                compile_fixtures.append(fixture)
        observed = template_record_count
        if observed != target:
            raise ValueError(
                f"Phase C raw reservoir underfilled for {template_id}: {observed}/{target}"
            )
    return records, compile_fixtures


def build_phase_c_materialization_schedule_v0(
    *,
    registry: UnifiedCapabilityRegistry,
    pools: Mapping[str, Sequence[ProgramSourceComponentV0]],
) -> tuple[dict[str, Any], ...]:
    """Compile an exact zero-financial schedule covering every executable component."""

    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    rows: list[dict[str, Any]] = []
    covered: dict[str, set[str]] = {
        role: set() for role in ("base", "temporal", "market", "event")
    }
    for template_id in TEMPLATE_ORDER:
        for template_ordinal in range(MATERIALIZATION_RECORDS_PER_TEMPLATE):
            components = {
                role: tuple(pools[role])[template_ordinal % len(tuple(pools[role]))]
                for role in PROGRAM_TEMPLATE_COMPONENTS[template_id]
            }
            policy, _ = _raw_cursor_after_policy_axes(
                template_id,
                template_ordinal,
                len(tuple(pools["base"])),
            )
            program = adapter.compose(
                template_id,
                base_component=components["base"],
                temporal_component=components.get("temporal"),
                market_component=components.get("market"),
                event_component=components.get("event"),
                combination_policy=policy,
            )
            if template_id == "BASE":
                control = legacy_candidate_program_v1(
                    components["base"].control,
                    portfolio_contract=adapter.portfolio_contract,
                )
                pair_id = str(components["base"].primary["pair_id"])
            else:
                matched = construct_matched_control_program_v1(program)
                control = matched.control
                pair_id = matched.pair_id
            for role, component in components.items():
                covered[role].add(component.component_id)
            record = {
                "schema_version": "cn_joint_program_phase_c_materialization_schedule_record_v0",
                "main_record_ordinal": len(rows),
                "template_id": template_id,
                "template_record_ordinal": template_ordinal,
                "pair_id": pair_id,
                "component_ids": {
                    role: component.component_id
                    for role, component in sorted(components.items())
                },
                "combination_policy": dict(policy),
                "primary_program": program.to_record(),
                "control_program": control.to_record(),
                "primary_compiled": compiler.compile(program).to_record(),
                "control_compiled": compiler.compile(control).to_record(),
                "financial_evaluation_executed": False,
                "validation_reads": 0,
                "holdout_reads": 0,
                "historical_2023_reads": 0,
                "forward_b_reads": 0,
                "forward_2026_reads": 0,
            }
            if template_id == "BASE":
                record.update(
                    {
                        "legacy_primary_candidate": dict(
                            components["base"].primary
                        ),
                        "legacy_control_candidate": dict(
                            components["base"].control
                        ),
                    }
                )
            record["schedule_record_sha256"] = stable_hash(record)
            rows.append(record)
    if len(rows) != MATERIALIZATION_SCHEDULE_RECORDS:
        raise RuntimeError("Phase C materialization schedule cardinality drift")
    for role, pool in pools.items():
        expected = {component.component_id for component in pool}
        if covered[role] != expected:
            raise ValueError(
                f"Phase C materialization schedule misses {role} components: "
                f"{sorted(expected - covered[role])}"
            )
    return tuple(rows)


def write_phase_c_materialization_schedule_v0(
    *,
    output_root: Path,
    phase_b_freeze_root: Path,
    registry_path: Path,
    repo_sha: str,
) -> dict[str, Any]:
    root = output_root.resolve()
    if root.exists():
        raise FileExistsError(f"Phase C materialization schedule root exists: {root}")
    root.mkdir(parents=True)
    phase_b_freeze_root = phase_b_freeze_root.resolve()
    verify_phase_b_prefinancial_freeze_v0(phase_b_freeze_root)
    registry = UnifiedCapabilityRegistry.read(registry_path.resolve())
    component_rows = _session_executable_component_rows(
        _read_jsonl(phase_b_freeze_root / "source_component_pool.jsonl")
    )
    pools = _pool_by_role(component_rows)
    schedule = build_phase_c_materialization_schedule_v0(
        registry=registry,
        pools=pools,
    )
    schedule_path = _write_jsonl(
        root / "phase_c_materialization_schedule.jsonl", schedule
    )
    closure = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_materialization_schedule_v0",
            "status": "PHASE_C_MATERIALIZATION_SCHEDULE_COMPLETE",
            "repo_sha": str(repo_sha),
            "schedule": _artifact(schedule_path, root=root),
            "record_count": len(schedule),
            "records_per_template": MATERIALIZATION_RECORDS_PER_TEMPLATE,
            "template_counts": {
                template_id: MATERIALIZATION_RECORDS_PER_TEMPLATE
                for template_id in TEMPLATE_ORDER
            },
            "component_count": len(component_rows),
            "component_role_counts": {
                role: len(pool) for role, pool in pools.items()
            },
            "all_session_executable_components_covered": True,
            "financial_evaluation_executed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "closure_sha256",
    )
    return _read_json(
        _write_json(root / "PHASE_C_MATERIALIZATION_SCHEDULE_COMPLETE.json", closure)
    )


def build_phase_c_component_materialization_plan_v0(
    *,
    component_rows: Sequence[Mapping[str, Any]],
    registry: UnifiedCapabilityRegistry,
    available_fields: Sequence[str],
) -> dict[str, Any]:
    available = {str(value) for value in available_fields}
    required: set[str] = set()
    external: set[str] = set()
    rows: list[dict[str, Any]] = []
    for component_row in component_rows:
        component = _component_from_record(component_row)
        member_leaves: dict[str, list[str]] = {}
        member_external: dict[str, list[str]] = {}
        for member, candidate in (
            ("PRIMARY", component.primary),
            ("CONTROL", component.control),
        ):
            resolution = resolve_required_physical_leaves(candidate)
            required.update(resolution.physical_leaf_ids)
            external.update(resolution.external_adapter_requirements)
            member_leaves[member] = list(resolution.physical_leaf_ids)
            member_external[member] = list(resolution.external_adapter_requirements)
        rows.append(
            {
                "component_id": component.component_id,
                "role": component.role,
                "route_id": component.route_id,
                "member_physical_leaf_ids": member_leaves,
                "member_external_adapter_requirements": member_external,
            }
        )
    bindings: list[dict[str, Any]] = []
    unsupported: list[dict[str, str]] = []
    for field_id in sorted(required):
        capability = registry.resolve(field_id)
        if field_id in available:
            adapter_id = ADAPTER_ALREADY_MATERIALIZED
        else:
            try:
                adapter_id = materialization_adapter_v1(capability)
            except ValueError as exc:
                unsupported.append({"field_id": field_id, "reason": str(exc)})
                continue
        bindings.append(
            {
                "field_id": field_id,
                "adapter_id": adapter_id,
                "already_materialized": field_id in available,
                "entity_scope": capability.entity_scope,
                "observable_clock": capability.observable_clock,
                "source_lag": capability.source_lag,
                "source_lag_unit": capability.source_lag_unit,
            }
        )
    if unsupported:
        raise ValueError(f"Phase C component pool has unmaterializable leaves: {unsupported}")
    materializable = {str(row["field_id"]) for row in bindings}
    if materializable != required:
        raise RuntimeError("Phase C component required/materializable coverage drift")
    payload = {
        "schema_version": "cn_joint_program_phase_c_component_materialization_plan_v0",
        "status": "PHASE_C_COMPONENT_MATERIALIZATION_PLAN_COMPLETE",
        "registry_hash": registry.registry_hash,
        "component_count": len(rows),
        "component_records": rows,
        "required_physical_leaf_ids": sorted(required),
        "required_physical_leaf_count": len(required),
        "available_required_field_ids": sorted(required & available),
        "missing_required_field_ids": sorted(required - available),
        "materializable_required_field_ids": sorted(materializable),
        "external_adapter_requirements": sorted(external),
        "field_bindings": bindings,
        "coverage_checks": {
            "required_equals_materializable": True,
            "unsupported_required_fields": [],
            "wrapper_introduces_no_new_physical_leaf": True,
        },
        "financial_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["plan_sha256"] = stable_hash(payload)
    return payload


def verify_phase_c_component_materialization_plan_v0(
    plan: Mapping[str, Any]
) -> None:
    body = dict(plan)
    claimed = str(body.pop("plan_sha256", ""))
    if len(claimed) != 64 or stable_hash(body) != claimed:
        raise ValueError("Phase C materialization plan self-hash drift")
    if (
        body.get("schema_version")
        != "cn_joint_program_phase_c_component_materialization_plan_v0"
        or body.get("status") != "PHASE_C_COMPONENT_MATERIALIZATION_PLAN_COMPLETE"
        or not bool(body["coverage_checks"]["required_equals_materializable"])
        or body["coverage_checks"]["unsupported_required_fields"]
    ):
        raise ValueError("Phase C materialization plan authority drift")
    required = set(body["required_physical_leaf_ids"])
    if required != set(body["materializable_required_field_ids"]):
        raise ValueError("Phase C materialization plan coverage drift")
    if any(
        int(body.get(key) or 0)
        for key in (
            "validation_reads",
            "holdout_reads",
            "historical_2023_reads",
            "forward_b_reads",
            "forward_2026_reads",
        )
    ) or bool(body.get("financial_evaluation_executed")):
        raise PermissionError("Phase C materialization plan records prohibited reads")


def _feedback_quality_inputs(row: Mapping[str, Any]) -> tuple[int, float, float]:
    primary = dict(row.get("primary") or {})
    control = dict(row.get("base_control") or {})
    primary_turnover = float(primary.get("mean_one_way_turnover") or 0.0)
    control_turnover = float(control.get("mean_one_way_turnover") or 0.0)
    turnover_excess = max(primary_turnover - control_turnover, 0.0) / max(
        control_turnover, 1e-12
    )
    primary_windows = {
        str(item["window_id"]): float(item["cumulative_net_return"])
        for item in primary.get("development_subwindows") or ()
    }
    control_windows = {
        str(item["window_id"]): float(item["cumulative_net_return"])
        for item in control.get("development_subwindows") or ()
    }
    increments = [
        primary_windows[key] - control_windows[key]
        for key in sorted(set(primary_windows) & set(control_windows))
    ]
    absolute_total = sum(abs(value) for value in increments)
    concentration = (
        max((abs(value) for value in increments), default=0.0) / absolute_total
        if absolute_total > 0.0
        else 0.0
    )
    return 0, float(turnover_excess), float(concentration)


def build_phase_c_initial_bandit_v0(
    *,
    phase_b_schedule: Sequence[Mapping[str, Any]],
    phase_b_results: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    schedule_by_ordinal = {
        int(row["main_record_ordinal"]): dict(row) for row in phase_b_schedule
    }
    bandit = ProgramFactorizedBanditV0(PHASE_C_CAMPAIGN_ID)
    behavior_counts: Counter[str] = Counter()
    ledger: list[dict[str, Any]] = []
    for row in sorted(phase_b_results, key=lambda item: int(item["main_record_ordinal"])):
        ordinal = int(row["main_record_ordinal"])
        schedule = schedule_by_ordinal[ordinal]
        template_id = str(row["template_id"])
        behavior_identity = str((row.get("primary") or {}).get("behavior_identity") or "")
        reason = "UPDATED"
        updated = True
        if template_id == "BASE":
            reason = "BASE_PARITY_NOT_REWARD_ELIGIBLE"
            updated = False
        elif str(row.get("replay_status") or "") != "PAIR_REPLAY_COMPLETE":
            reason = "REPLAY_BLOCKED_NO_UPDATE"
            updated = False
        elif row.get("blockers"):
            reason = "BLOCKER_NO_UPDATE"
            updated = False
        elif row.get("matched_net_reward_increment") is None:
            reason = "NONFINITE_INCREMENT_NO_UPDATE"
            updated = False
        if updated:
            receipt = ProgramProposalReceiptV0.from_record(
                dict(schedule["proposal_receipt"])
            )
            _, turnover_excess, concentration = _feedback_quality_inputs(row)
            bandit.observe(
                receipt,
                matched_increment=float(row["matched_net_reward_increment"]),
                behavior_cluster_repeat_count=behavior_counts[behavior_identity],
                turnover_excess_ratio=turnover_excess,
                single_window_concentration=concentration,
            )
        if behavior_identity:
            behavior_counts[behavior_identity] += 1
        ledger_row = {
            "schema_version": "cn_joint_program_phase_c_phase_b_feedback_seed_v0",
            "phase_b_main_record_ordinal": ordinal,
            "template_id": template_id,
            "phase_b_record_payload_sha256": str(row["record_payload_sha256"]),
            "bandit_update_applied": updated,
            "reason": reason,
            "validation_feedback_used": False,
            "cross_campaign_state_imported": False,
            "development_feedback_provenance": (
                build_development_feedback_provenance(
                    serialized_optimizer_state_imported=False,
                    development_financial_observations_imported=updated,
                    development_observation_count=int(updated),
                    candidate_results_imported=True,
                    factor_statistics_imported=False,
                    behavior_statistics_imported=bool(behavior_identity),
                    template_classification_imported=True,
                    manual_diagnosis_imported=False,
                    objective_designed_after_parent_results=False,
                )
            ),
        }
        ledger_row["feedback_seed_record_sha256"] = stable_hash(ledger_row)
        ledger.append(ledger_row)
    return bandit.snapshot(), tuple(ledger)


def _verify_phase_b_outcome(
    outcome_path: Path,
    *,
    phase_b_result_root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    outcome = _read_json(outcome_path)
    _verify_self_hash(outcome, "receipt_payload_sha256", "Phase B outcome")
    if (
        outcome.get("status")
        != "CN_JOINT_PROGRAM_ROLLING_SEARCH_V0_PHASE_B_ACCEPTED"
        or outcome.get("promotion") != "HOLD_PROMOTION"
        or dict(outcome.get("phase_c_decision") or {}).get("decision")
        != "AUTHORIZE_BOUNDED_DEVELOPMENT_ONLY_PHASE_C_NO_LAUNCH_IN_THIS_WORKFLOW"
    ):
        raise PermissionError("Phase B outcome does not authorize bounded Phase C")
    closure_path = phase_b_result_root / PHASE_B_RESULT_CLOSURE_NAME
    if _sha256(closure_path) != str(outcome["closure"]["file_sha256"]):
        raise ValueError("Phase B result closure file hash drift")
    closure = _read_json(closure_path)
    _verify_self_hash(closure, "closure_payload_sha256", "Phase B result closure")
    if (
        closure.get("status") != "CN_JOINT_PROGRAM_PHASE_B_COMPLETE"
        or int(closure.get("record_count") or 0) != 64
        or int(closure.get("checkpoint_count") or 0) != 8
        or str(closure.get("closure_payload_sha256") or "")
        != str(outcome["closure"]["payload_sha256"])
    ):
        raise ValueError("Phase B accepted result binding drift")
    manifest_path = phase_b_result_root / _artifact_binding_path(
        closure["artifact_manifest"]
    )
    if _sha256(manifest_path) != str(closure["artifact_manifest"]["sha256"]):
        raise ValueError("Phase B result artifact manifest file drift")
    manifest = _read_json(manifest_path)
    _verify_self_hash(manifest, "artifact_manifest_sha256", "Phase B artifact manifest")
    for artifact in manifest["artifacts"]:
        path = phase_b_result_root / _artifact_binding_path(artifact)
        if (
            not path.is_file()
            or path.stat().st_size != _artifact_binding_size(artifact)
            or _sha256(path) != str(artifact["sha256"])
        ):
            raise ValueError(f"Phase B accepted artifact drift: {path}")
    audit = dict(outcome["independent_audit"])
    audit_path = Path(str(audit["path"]))
    if not audit_path.is_file() or _sha256(audit_path) != str(audit["file_sha256"]):
        raise ValueError("Phase B independent audit binding drift")
    return outcome, closure


def _manifest_available_fields(path: Path) -> tuple[dict[str, Any], tuple[str, ...]]:
    manifest = _read_json(path)
    body = dict(manifest)
    claimed = str(body.pop("manifest_hash", ""))
    if len(claimed) != 64 or stable_hash(body) != claimed:
        raise ValueError("accepted development field manifest self-hash drift")
    fields = tuple(str(value) for value in manifest.get("fields") or ())
    if not {"trade_time", "code", "close"}.issubset(fields):
        raise ValueError("accepted development field manifest lacks replay fields")
    if any(
        int(manifest.get(key) or 0)
        for key in ("validation_reads", "holdout_reads", "forward_2026_reads")
    ):
        raise PermissionError("accepted development field manifest records sealed reads")
    return manifest, fields


def build_phase_c_prefinancial_freeze_v0(
    *,
    output_root: Path,
    phase_b_freeze_root: Path,
    phase_b_result_root: Path,
    phase_b_outcome_path: Path,
    registry_path: Path,
    accepted_field_manifest_path: Path,
    repo_sha: str,
) -> dict[str, Any]:
    root = output_root.resolve()
    if root.exists():
        raise FileExistsError(f"Phase C freeze root is not fresh: {root}")
    root.mkdir(parents=True)
    phase_b_freeze_root = phase_b_freeze_root.resolve()
    phase_b_result_root = phase_b_result_root.resolve()
    phase_b_outcome_path = phase_b_outcome_path.resolve()
    registry_path = registry_path.resolve()
    accepted_field_manifest_path = accepted_field_manifest_path.resolve()

    phase_b_freeze = verify_phase_b_prefinancial_freeze_v0(phase_b_freeze_root)
    phase_b_outcome, phase_b_closure = _verify_phase_b_outcome(
        phase_b_outcome_path,
        phase_b_result_root=phase_b_result_root,
    )
    input_binding = _read_json(phase_b_result_root / "input_binding.json")
    _verify_self_hash(input_binding, "input_binding_sha256", "Phase B input binding")
    if (
        str(input_binding["phase_b_prefinancial_closure_file_sha256"])
        != _sha256(phase_b_freeze_root / PHASE_B_FREEZE_CLOSURE_NAME)
        or str(input_binding["phase_b_prefinancial_closure_payload_sha256"])
        != str(phase_b_freeze["closure_sha256"])
    ):
        raise ValueError("Phase B result/freeze binding drift")

    registry = UnifiedCapabilityRegistry.read(registry_path)
    source_component_rows = _read_jsonl(
        phase_b_freeze_root / "source_component_pool.jsonl"
    )
    component_rows = _session_executable_component_rows(source_component_rows)
    pools = _pool_by_role(component_rows)
    ask_plan = list(build_phase_c_ask_plan_v0())
    reservoir, compile_fixtures = _build_reservoir(
        registry=registry,
        pools=pools,
    )
    field_manifest, available_fields = _manifest_available_fields(
        accepted_field_manifest_path
    )
    materialization_plan = build_phase_c_component_materialization_plan_v0(
        component_rows=component_rows,
        registry=registry,
        available_fields=available_fields,
    )
    verify_phase_c_component_materialization_plan_v0(materialization_plan)
    if materialization_plan["missing_required_field_ids"]:
        raise ValueError(
            "Phase C reservoir requires additional materialization before freeze: "
            f"{materialization_plan['missing_required_field_ids']}"
        )

    phase_b_schedule = _read_jsonl(
        phase_b_freeze_root / "phase_b_uniform_schedule.jsonl"
    )
    phase_b_results = _read_jsonl(
        phase_b_result_root / "phase_b_record_results.jsonl"
    )
    initial_bandit, feedback_seed = build_phase_c_initial_bandit_v0(
        phase_b_schedule=phase_b_schedule,
        phase_b_results=phase_b_results,
    )
    ProgramFactorizedBanditV0.restore(initial_bandit)

    component_pool_path = _write_jsonl(
        root / "phase_c_session_executable_component_pool.jsonl",
        component_rows,
    )
    ask_path = _write_jsonl(root / "phase_c_ask_plan.jsonl", ask_plan)
    reservoir_path = _write_jsonl(
        root / "phase_c_raw_program_reservoir.jsonl", reservoir
    )
    fixture_path = _write_jsonl(
        root / "phase_c_template_compile_fixtures.jsonl", compile_fixtures
    )
    feedback_path = _write_jsonl(
        root / "phase_b_feedback_seed.jsonl", feedback_seed
    )
    bandit_path = _write_json(root / "initial_bandit_state.json", initial_bandit)
    materialization_path = _write_json(
        root / "phase_c_materialization_plan.json", materialization_plan
    )
    development_feedback_provenance = build_development_feedback_provenance(
        serialized_optimizer_state_imported=False,
        development_financial_observations_imported=(
            int(initial_bandit["observations"]) > 0
        ),
        development_observation_count=int(initial_bandit["observations"]),
        candidate_results_imported=True,
        factor_statistics_imported=False,
        behavior_statistics_imported=True,
        template_classification_imported=True,
        manual_diagnosis_imported=False,
        objective_designed_after_parent_results=False,
    )
    contract = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_run_contract_v0",
            "status": "PHASE_C_INPUTS_FROZEN_BEFORE_FINANCIAL_READ",
            "repo_sha": str(repo_sha),
            "campaign_id": PHASE_C_CAMPAIGN_ID,
            "batch_id": PHASE_C_BATCH_ID,
            "phase_b_outcome_path": str(phase_b_outcome_path),
            "phase_b_outcome_file_sha256": _sha256(phase_b_outcome_path),
            "phase_b_outcome_payload_sha256": str(
                phase_b_outcome["receipt_payload_sha256"]
            ),
            "phase_b_result_root": str(phase_b_result_root),
            "phase_b_result_closure_file_sha256": _sha256(
                phase_b_result_root / PHASE_B_RESULT_CLOSURE_NAME
            ),
            "phase_b_result_closure_payload_sha256": str(
                phase_b_closure["closure_payload_sha256"]
            ),
            "phase_b_freeze_root": str(phase_b_freeze_root),
            "phase_b_freeze_closure_file_sha256": _sha256(
                phase_b_freeze_root / PHASE_B_FREEZE_CLOSURE_NAME
            ),
            "phase_b_freeze_closure_payload_sha256": str(
                phase_b_freeze["closure_sha256"]
            ),
            "registry_path": str(registry_path),
            "registry_file_sha256": _sha256(registry_path),
            "accepted_field_manifest_path": str(accepted_field_manifest_path),
            "accepted_field_manifest_file_sha256": _sha256(
                accepted_field_manifest_path
            ),
            "accepted_field_manifest_payload_sha256": str(
                field_manifest["manifest_hash"]
            ),
            "source_component_pool_file_sha256": _sha256(
                phase_b_freeze_root / "source_component_pool.jsonl"
            ),
            "session_executable_component_pool_file_sha256": _sha256(
                component_pool_path
            ),
            "session_executable_component_count": len(component_rows),
            "session_executable_component_role_counts": {
                role: len(pool) for role, pool in pools.items()
            },
            "session_executable_routes": sorted(SESSION_EXECUTABLE_ROUTES),
            "ask_plan_file_sha256": _sha256(ask_path),
            "raw_program_reservoir_file_sha256": _sha256(reservoir_path),
            "template_compile_fixtures_file_sha256": _sha256(fixture_path),
            "phase_b_feedback_seed_file_sha256": _sha256(feedback_path),
            "initial_bandit_state_file_sha256": _sha256(bandit_path),
            "initial_bandit_state_payload_sha256": str(
                initial_bandit["bandit_state_sha256"]
            ),
            "materialization_plan_file_sha256": _sha256(materialization_path),
            "materialization_plan_payload_sha256": str(
                materialization_plan["plan_sha256"]
            ),
            "main_record_count": EXPECTED_RECORDS,
            "base_parity_record_count": BASE_RECORDS,
            "enhanced_template_record_count": ENHANCED_RECORDS_PER_TEMPLATE,
            "template_order": list(TEMPLATE_ORDER),
            "template_quotas": {
                "BASE": BASE_RECORDS,
                **{
                    template_id: ENHANCED_RECORDS_PER_TEMPLATE
                    for template_id in ENHANCED_TEMPLATE_ORDER
                },
            },
            "enhanced_arm_quotas": {
                "UNIFORM_FRESH": 28,
                "FACTORIZED_EXPLOIT": 24,
                "NOVELTY_RESERVE": 12,
            },
            "initial_uniform_baseline_per_enhanced_template": 16,
            "raw_reservoir_per_enhanced_template": (
                RAW_RESERVOIR_PER_ENHANCED_TEMPLATE
            ),
            "maximum_variants_per_base_per_template": (
                MAX_VARIANTS_PER_BASE_PER_TEMPLATE
            ),
            "minimum_base_identities_per_template": MIN_BASE_IDENTITIES_PER_TEMPLATE,
            "no_early_template_cancellation": True,
            "bandit_version": BANDIT_VERSION,
            "bandit_policy_id": BANDIT_POLICY_ID,
            "bandit_feedback_source": "COMPLETE_DEVELOPMENT_FULL_PROGRAMS_ONLY",
            "blocked_noop_tell_rule": "RECORD_BLOCKER_NO_BANDIT_UPDATE",
            "cross_campaign_optimizer_state_import": False,
            "development_feedback_provenance": development_feedback_provenance,
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
    contract_path = _write_json(root / "phase_c_run_contract.json", contract)
    access = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_prefinancial_access_v0",
            "financial_evaluation_executed": False,
            "market_price_rows_read": 0,
            "label_rows_read": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "optimizer_feedback_consumed": False,
            "phase_b_development_feedback_seeded": True,
            "cross_campaign_optimizer_state_imported": False,
            "development_feedback_provenance": development_feedback_provenance,
        },
        "access_ledger_sha256",
    )
    access_path = _write_json(root / "access_ledger.json", access)
    summary = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_prefinancial_summary_v0",
            "status": "PHASE_C_PREFINANCIAL_READY",
            "main_record_count": EXPECTED_RECORDS,
            "checkpoint_count": CHECKPOINT_COUNT,
            "reservoir_record_count": len(reservoir),
            "initial_bandit_observations": int(initial_bandit["observations"]),
            "required_physical_leaf_count": int(
                materialization_plan["required_physical_leaf_count"]
            ),
            "session_executable_component_count": len(component_rows),
            "session_executable_component_role_counts": {
                role: len(pool) for role, pool in pools.items()
            },
            "missing_required_field_ids": list(
                materialization_plan["missing_required_field_ids"]
            ),
            "zero_financial_reads": True,
            "sealed_reads": 0,
        },
        "summary_payload_sha256",
    )
    summary_path = _write_json(root / "summary.json", summary)
    artifacts = [
        component_pool_path,
        ask_path,
        reservoir_path,
        fixture_path,
        feedback_path,
        bandit_path,
        materialization_path,
        contract_path,
        access_path,
        summary_path,
    ]
    manifest = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_prefinancial_artifacts_v0",
            "artifacts": [_artifact(path, root=root) for path in artifacts],
        },
        "artifact_manifest_sha256",
    )
    manifest_path = _write_json(root / "ARTIFACT_MANIFEST.json", manifest)
    closure = _self_hashed(
        {
            "schema_version": FREEZE_SCHEMA,
            "status": FREEZE_STATUS,
            "repo_sha": str(repo_sha),
            "output_root": str(root),
            "main_record_count": EXPECTED_RECORDS,
            "checkpoint_count": CHECKPOINT_COUNT,
            "reservoir_record_count": len(reservoir),
            "initial_bandit_observations": int(initial_bandit["observations"]),
            "required_physical_leaf_count": int(
                materialization_plan["required_physical_leaf_count"]
            ),
            "session_executable_component_count": len(component_rows),
            "session_executable_component_role_counts": {
                role: len(pool) for role, pool in pools.items()
            },
            "missing_required_field_count": len(
                materialization_plan["missing_required_field_ids"]
            ),
            "artifact_manifest": _artifact(manifest_path, root=root),
            "financial_evaluation_executed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
        },
        "closure_sha256",
    )
    return _read_json(_write_json(root / FREEZE_CLOSURE_NAME, closure))


def verify_phase_c_prefinancial_freeze_v0(root: Path) -> dict[str, Any]:
    root = root.resolve()
    closure = _read_json(root / FREEZE_CLOSURE_NAME)
    _verify_self_hash(closure, "closure_sha256", "Phase C freeze closure")
    if closure.get("schema_version") != FREEZE_SCHEMA or closure.get("status") != FREEZE_STATUS:
        raise ValueError("Phase C freeze authority drift")
    manifest_path = root / str(closure["artifact_manifest"]["relative_path"])
    if _sha256(manifest_path) != str(closure["artifact_manifest"]["sha256"]):
        raise ValueError("Phase C artifact manifest file drift")
    manifest = _read_json(manifest_path)
    _verify_self_hash(manifest, "artifact_manifest_sha256", "Phase C artifact manifest")
    for artifact in manifest["artifacts"]:
        path = root / str(artifact["relative_path"])
        if (
            not path.is_file()
            or path.stat().st_size != int(artifact["size_bytes"])
            or _sha256(path) != str(artifact["sha256"])
        ):
            raise ValueError(f"Phase C freeze artifact drift: {path}")
    asks = _read_jsonl(root / "phase_c_ask_plan.jsonl")
    if asks != list(build_phase_c_ask_plan_v0()):
        raise ValueError("Phase C ask-plan canonical replay drift")
    reservoir = _read_jsonl(root / "phase_c_raw_program_reservoir.jsonl")
    component_rows = _read_jsonl(
        root / "phase_c_session_executable_component_pool.jsonl"
    )
    if component_rows != _session_executable_component_rows(component_rows):
        raise ValueError("Phase C executable component-pool route drift")
    pools = _pool_by_role(component_rows)
    for row in reservoir:
        body = dict(row)
        claimed = str(body.pop("reservoir_record_sha256", ""))
        if stable_hash(body) != claimed:
            raise ValueError("Phase C reservoir record self-hash drift")
    counts = Counter(str(row["template_id"]) for row in reservoir)
    expected_counts = {
        "BASE": BASE_RECORDS,
        **{
            template_id: RAW_RESERVOIR_PER_ENHANCED_TEMPLATE
            for template_id in ENHANCED_TEMPLATE_ORDER
        },
    }
    if dict(counts) != expected_counts:
        raise ValueError("Phase C reservoir template quota drift")
    if any(
        len(
            {
                str(row["raw_combination_sha256"])
                for row in reservoir
                if row["template_id"] == template_id
            }
        )
        != expected
        for template_id, expected in expected_counts.items()
        if template_id != "BASE"
    ):
        raise ValueError("Phase C reservoir contains duplicate raw combinations")
    base_component_counts = Counter(
        str(row["components"]["base"]["component_id"])
        for row in reservoir
        if row["template_id"] == "BASE"
    )
    if (
        len(base_component_counts) < MIN_BASE_IDENTITIES_PER_TEMPLATE
        or max(base_component_counts.values())
        > math.ceil(BASE_RECORDS / len(pools["base"]))
    ):
        raise ValueError("Phase C BASE parity repetition contract drift")
    ProgramFactorizedBanditV0.restore(_read_json(root / "initial_bandit_state.json"))
    fixtures = _read_jsonl(root / "phase_c_template_compile_fixtures.jsonl")
    if [row["template_id"] for row in fixtures] != list(TEMPLATE_ORDER):
        raise ValueError("Phase C compile fixture template coverage drift")
    for row in fixtures:
        body = dict(row)
        claimed = str(body.pop("compile_fixture_sha256", ""))
        if stable_hash(body) != claimed:
            raise ValueError("Phase C compile fixture self-hash drift")
    plan = _read_json(root / "phase_c_materialization_plan.json")
    verify_phase_c_component_materialization_plan_v0(plan)
    if int(plan["component_count"]) != len(component_rows):
        raise ValueError("Phase C materialization/component-pool binding drift")
    if plan["missing_required_field_ids"]:
        raise ValueError("Phase C freeze has unresolved materialization fields")
    contract = _read_json(root / "phase_c_run_contract.json")
    _verify_self_hash(contract, "run_contract_sha256", "Phase C run contract")
    access = _read_json(root / "access_ledger.json")
    _verify_self_hash(access, "access_ledger_sha256", "Phase C access ledger")
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
        raise PermissionError("Phase C prefinancial freeze records prohibited reads")
    return closure


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--output-root", type=Path, required=True)
    freeze.add_argument("--phase-b-freeze-root", type=Path, required=True)
    freeze.add_argument("--phase-b-result-root", type=Path, required=True)
    freeze.add_argument("--phase-b-outcome", type=Path, required=True)
    freeze.add_argument("--registry", type=Path, required=True)
    freeze.add_argument("--accepted-field-manifest", type=Path, required=True)
    freeze.add_argument("--repo-sha", required=True)
    materialization = subparsers.add_parser("materialization-schedule")
    materialization.add_argument("--output-root", type=Path, required=True)
    materialization.add_argument("--phase-b-freeze-root", type=Path, required=True)
    materialization.add_argument("--registry", type=Path, required=True)
    materialization.add_argument("--repo-sha", required=True)
    verify = subparsers.add_parser("verify-freeze")
    verify.add_argument("--phase-c-freeze-root", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        closure = build_phase_c_prefinancial_freeze_v0(
            output_root=args.output_root,
            phase_b_freeze_root=args.phase_b_freeze_root,
            phase_b_result_root=args.phase_b_result_root,
            phase_b_outcome_path=args.phase_b_outcome,
            registry_path=args.registry,
            accepted_field_manifest_path=args.accepted_field_manifest,
            repo_sha=str(args.repo_sha),
        )
    elif args.command == "materialization-schedule":
        closure = write_phase_c_materialization_schedule_v0(
            output_root=args.output_root,
            phase_b_freeze_root=args.phase_b_freeze_root,
            registry_path=args.registry,
            repo_sha=str(args.repo_sha),
        )
    else:
        closure = verify_phase_c_prefinancial_freeze_v0(args.phase_c_freeze_root)
    print(json.dumps(closure, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
