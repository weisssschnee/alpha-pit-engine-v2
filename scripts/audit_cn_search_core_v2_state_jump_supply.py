"""Zero-financial audit of Generator V2 against the real frozen Program component pool.

Run on the authorized 77o host after this branch is deployed.  The audit reads
only frozen structural artifacts (registry, component pool, Program reservoir,
and committed zero-financial Stage-C/D supply manifests).  It never opens the
train evaluator, validation, holdout, historical challenge, or forward assets.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import platform
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts.run_cn_program_optimizer_successor_benchmark_v1 import _program_entries
from our_system_phase2.runtime.cn_program_optimizer_successor_benchmark_v1 import (
    AUTHORIZED_HOST,
    FROZEN_PROGRAM_SPACE_COUNT,
    FROZEN_PROGRAM_SPACE_SHA256,
    SOURCE_COMPONENT_POOL_SHA256,
    SOURCE_FREEZE_CLOSURE,
    SOURCE_FREEZE_CLOSURE_FILE_SHA256,
    SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256,
    SOURCE_FREEZE_ROOT,
    SOURCE_RAW_RESERVOIR_SHA256,
    SOURCE_REGISTRY_PATH,
    SOURCE_REGISTRY_SHA256,
    SOURCE_RUN_CONTRACT_FILE_SHA256,
    SOURCE_RUN_CONTRACT_PAYLOAD_SHA256,
)
from our_system_phase2.services.candidate_program_controls_v1 import (
    construct_matched_control_program_v1,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    CandidateProgramProposalAdapterV0,
)
from our_system_phase2.services.candidate_program_v1 import ProgramCompilerV1
from our_system_phase2.services.program_search_optimizer_v1 import (
    normalized_program_gene_identity_v1,
    program_availability_entries_v1,
    program_structural_genes_v1,
)
from our_system_phase2.services.program_search_state_jump_generator_v2 import (
    SemanticStateJumpProgramGeneratorV2,
)
from our_system_phase2.services.program_tournament_freeze_v1 import (
    verify_phase_freeze_v1,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


SCHEMA = "cn_search_core_v2_state_jump_supply_audit_v1"
STATUS_PASS = "PASS_ZERO_FINANCIAL_STATE_JUMP_REAL_SUPPLY_AUDIT"
STATUS_FAIL = "FAIL_ZERO_FINANCIAL_STATE_JUMP_REAL_SUPPLY_AUDIT"
TEMPLATES = (
    "BASE_EVENT",
    "BASE_MARKET",
    "BASE_MARKET_EVENT",
    "BASE_TEMPORAL",
    "BASE_TEMPORAL_EVENT",
    "BASE_TEMPORAL_MARKET",
    "BASE_TEMPORAL_MARKET_EVENT",
)
DEFAULT_PROBES_PER_TEMPLATE = 96


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _verify_self(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _normalized_supply_exact(rows: list[dict[str, Any]]) -> set[str]:
    entries = program_availability_entries_v1(
        [{"genes": dict(row["program_genes"])} for row in rows]
    )
    return {entry.exact_identity for entry in entries}


def audit(
    repo: Path,
    *,
    source_freeze_root: Path,
    registry_path: Path,
    stage1_prefreeze_path: Path,
    probes_per_template: int,
) -> dict[str, Any]:
    repo = repo.resolve()
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError("STATE_JUMP_SUPPLY_AUDIT_UNAUTHORIZED_HOST")
    stage1_prefreeze_path = stage1_prefreeze_path.resolve()
    prefreeze = _read(stage1_prefreeze_path)
    prefreeze_hash = _verify_self(
        prefreeze, "prefreeze_payload_sha256", "Search Core V2 Stage-1 prefreeze"
    )
    if (
        prefreeze.get("status")
        != "SEARCH_CORE_V2_STAGE1_PREFROZEN_BEFORE_FINANCIAL_READ"
        or bool(prefreeze.get("financial_labels_read_by_builder"))
    ):
        raise RuntimeError("STATE_JUMP_SUPPLY_STAGE1_PREFREEZE_DRIFT")
    generator_contract = dict(prefreeze["arm_b_state_jump"])
    source_freeze_root = source_freeze_root.resolve()
    registry_path = registry_path.resolve()
    closure_path = source_freeze_root / SOURCE_FREEZE_CLOSURE
    contract_path = source_freeze_root / "phase_c_run_contract.json"
    reservoir_path = source_freeze_root / "phase_c_raw_program_reservoir.jsonl"
    component_path = source_freeze_root / "phase_c_session_executable_component_pool.jsonl"
    closure = verify_phase_freeze_v1(source_freeze_root)
    if (
        _sha(closure_path) != SOURCE_FREEZE_CLOSURE_FILE_SHA256
        or str(closure.get("closure_sha256") or "") != SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256
        or _sha(contract_path) != SOURCE_RUN_CONTRACT_FILE_SHA256
        or _sha(reservoir_path) != SOURCE_RAW_RESERVOIR_SHA256
        or _sha(component_path) != SOURCE_COMPONENT_POOL_SHA256
        or _sha(registry_path) != SOURCE_REGISTRY_SHA256
    ):
        raise RuntimeError("STATE_JUMP_SUPPLY_SOURCE_AUTHORITY_DRIFT")
    contract = _read(contract_path)
    contract_body = dict(contract)
    claimed_contract = str(contract_body.pop("run_contract_sha256", ""))
    if claimed_contract != stable_hash(contract_body) or claimed_contract != SOURCE_RUN_CONTRACT_PAYLOAD_SHA256:
        raise RuntimeError("STATE_JUMP_SUPPLY_SOURCE_CONTRACT_DRIFT")

    reservoir = _read_jsonl(reservoir_path)
    component_rows = _read_jsonl(component_path)
    registry = UnifiedCapabilityRegistry.read(registry_path)
    catalog, catalog_report = engine._build_catalog(
        reservoir=reservoir,
        component_rows=component_rows,
        registry=registry,
    )
    entries = _program_entries(catalog)
    old_program_space_hash = stable_hash([entry.to_dict() for entry in entries])
    if len(entries) != FROZEN_PROGRAM_SPACE_COUNT or old_program_space_hash != FROZEN_PROGRAM_SPACE_SHA256:
        raise RuntimeError("STATE_JUMP_SUPPLY_PROGRAM_SPACE_DRIFT")

    components_by_id = {
        component.component_id: component
        for component in (engine._component_from_row(row) for row in component_rows)
    }
    if len(components_by_id) != len(component_rows):
        raise RuntimeError("STATE_JUMP_SUPPLY_COMPONENT_DUPLICATE")
    components_by_role: dict[str, list[Any]] = {}
    component_field_columns: set[str] = set()
    for component in components_by_id.values():
        components_by_role.setdefault(component.role, []).append(component)
        for candidate in (component.primary, component.control):
            for field_key in ("field_ids", "declared_field_ids", "condition_field_ids"):
                component_field_columns.update(
                    str(field_id)
                    for field_id in (candidate.get(field_key) or ())
                    if str(field_id)
                )
    if not component_field_columns:
        raise RuntimeError("STATE_JUMP_SUPPLY_COMPONENT_FIELD_SURFACE_EMPTY")
    for role in components_by_role:
        components_by_role[role] = sorted(
            components_by_role[role], key=lambda component: component.component_id
        )

    stage_c_supply_path = repo / "runtime/run_plans/cn_stage_c_expanded_supply_audit_d0160c6_20260820.json"
    stage_d_supply_path = repo / "runtime/run_plans/cn_stage_d_expanded_supply_audit_cbaaaee_20260820.json"
    stage_c_supply = _read(stage_c_supply_path)
    stage_d_supply = _read(stage_d_supply_path)
    stage_c_payload = _verify_self(stage_c_supply, "audit_payload_sha256", "Stage-C supply")
    stage_d_payload = _verify_self(stage_d_supply, "audit_payload_sha256", "Stage-D supply")
    if (
        bool(stage_c_supply.get("candidate_evaluation_executed"))
        or bool(stage_d_supply.get("candidate_evaluation_executed"))
        or int(stage_c_supply.get("validation_reads") or 0)
        or int(stage_d_supply.get("validation_reads") or 0)
        or int(stage_c_supply.get("holdout_reads") or 0)
        or int(stage_d_supply.get("holdout_reads") or 0)
    ):
        raise RuntimeError("STATE_JUMP_SUPPLY_COMMITTED_SUPPLY_ROLE_DRIFT")

    known_exact = {entry.exact_identity for entry in entries}
    known_exact.update(_normalized_supply_exact(list(stage_c_supply["fresh_entries"])))
    known_exact.update(_normalized_supply_exact(list(stage_d_supply["fresh_entries"])))
    known_exact_hash = stable_hash(sorted(known_exact))

    composer = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    generator = SemanticStateJumpProgramGeneratorV2(
        adapter=composer,
        components_by_role=components_by_role,
        seed=int(generator_contract["seed"]),
        operation_priors=dict(generator_contract["operation_priors"]),
        maximum_attempts=int(generator_contract["maximum_attempts"]),
    )
    ordered_slots = tuple(entries[0].genes)
    observed_exact: set[str] = set()
    observed_semantic: set[str] = set()
    generated_field_columns: set[str] = set()
    per_template: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, Any]] = []

    for template in TEMPLATES:
        exacts: list[str] = []
        semantics: list[str] = []
        bases: set[str] = set()
        raw_fields: list[int] = []
        for ordinal in range(int(probes_per_template)):
            try:
                generated = generator.propose(
                    batch_id="ZERO_FINANCIAL_REAL_SUPPLY_AUDIT",
                    ask_ordinal=ordinal,
                    template_id=template,
                )
                compiled = compiler.compile(generated.program)
                genes = program_structural_genes_v1(
                    program_template_id=template,
                    components=generated.components,
                    combination_policy=generated.combination_policy,
                    program=generated.program,
                    compiled=compiled,
                )
                exact = normalized_program_gene_identity_v1(
                    genes, ordered_slots=ordered_slots
                )
                matched = construct_matched_control_program_v1(generated.program)
                if matched.primary.semantic_program_hash == matched.control.semantic_program_hash:
                    raise RuntimeError("MATCHED_CONTROL_SEMANTIC_NOOP")
                control_compiled = compiler.compile(matched.control)
                observed_fields = set(map(str, compiled.field_lags)) | set(
                    map(str, control_compiled.field_lags)
                )
                if not observed_fields.issubset(component_field_columns):
                    raise RuntimeError("GENERATED_COMPILED_FIELD_OUTSIDE_COMPONENT_POOL_SURFACE")
                generated_field_columns.update(observed_fields)
                receipt = composer.build_receipt(
                    program_template_id=template,
                    program=generated.program,
                    components=tuple(
                        generated.components[role]
                        for role in generated.components
                    ),
                    combination_policy=generated.combination_policy,
                    batch_id="ZERO_FINANCIAL_REAL_SUPPLY_AUDIT",
                    ask_ordinal=ordinal,
                    generation_arm="SEMANTIC_STATE_JUMP_GENERATOR_V2",
                )
                if receipt.semantic_program_hash != generated.program.semantic_program_hash:
                    raise RuntimeError("RECEIPT_SEMANTIC_IDENTITY_DRIFT")
                if exact in known_exact:
                    raise RuntimeError("KNOWN_NORMALIZED_EXACT_OVERLAP")
                if exact in observed_exact:
                    raise RuntimeError("AUDIT_NORMALIZED_EXACT_DUPLICATE")
                if generated.program.semantic_program_hash in observed_semantic:
                    raise RuntimeError("AUDIT_SEMANTIC_DUPLICATE")
                observed_exact.add(exact)
                observed_semantic.add(generated.program.semantic_program_hash)
                exacts.append(exact)
                semantics.append(generated.program.semantic_program_hash)
                bases.add(generated.components["base"].component_id)
                raw_fields.append(int(genes["raw_field_count"]))
            except Exception as exc:
                failures.append(
                    {
                        "template_id": template,
                        "ordinal": ordinal,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
                break
        per_template[template] = {
            "requested": int(probes_per_template),
            "generated": len(exacts),
            "unique_normalized_exact": len(set(exacts)),
            "unique_semantic_program": len(set(semantics)),
            "distinct_base_components": len(bases),
            "raw_field_count_min": min(raw_fields) if raw_fields else None,
            "raw_field_count_max": max(raw_fields) if raw_fields else None,
            "normalized_exact_sha256": stable_hash(exacts),
        }

    required = max(48, int(probes_per_template) // 2)
    checks = {
        "source_authority_verified": True,
        "original_program_space_verified": len(entries) == FROZEN_PROGRAM_SPACE_COUNT,
        "known_exact_union_nonempty": bool(known_exact),
        "no_generation_failures": not failures,
        "all_templates_have_stage1_capacity": all(
            int(row["generated"]) >= required for row in per_template.values()
        ),
        "all_generated_normalized_exacts_unique": len(observed_exact)
        == sum(int(row["generated"]) for row in per_template.values()),
        "all_generated_semantics_unique": len(observed_semantic)
        == sum(int(row["generated"]) for row in per_template.values()),
        "all_templates_use_multiple_base_components": all(
            int(row["distinct_base_components"]) >= 4 for row in per_template.values()
        ),
        "component_pool_field_surface_nonempty": bool(component_field_columns),
        "generated_compiled_fields_subset_component_pool_surface": generated_field_columns.issubset(
            component_field_columns
        ),
        "financial_evaluation_executed": False,
        "restricted_reads_zero": True,
    }
    status = STATUS_PASS if all(checks.values()) else STATUS_FAIL
    payload = {
        "schema_version": SCHEMA,
        "status": status,
        "host": platform.node(),
        "source_authority": {
            "source_freeze_root": str(source_freeze_root),
            "closure_file_sha256": _sha(closure_path),
            "closure_payload_sha256": str(closure["closure_sha256"]),
            "raw_reservoir_sha256": _sha(reservoir_path),
            "component_pool_sha256": _sha(component_path),
            "registry_sha256": _sha(registry_path),
            "program_space_count": len(entries),
            "program_space_sha256": old_program_space_hash,
            "catalog_report": catalog_report,
        },
        "committed_zero_financial_supply": {
            "stage_c_file_sha256": _sha(stage_c_supply_path),
            "stage_c_payload_sha256": stage_c_payload,
            "stage_d_file_sha256": _sha(stage_d_supply_path),
            "stage_d_payload_sha256": stage_d_payload,
            "known_normalized_exact_count": len(known_exact),
            "known_normalized_exact_sha256": known_exact_hash,
        },
        "component_pool": {
            "total": len(components_by_id),
            "per_role": {
                role: len(rows) for role, rows in sorted(components_by_role.items())
            },
        },
        "resource_field_surface": {
            "field_column_count": len(component_field_columns),
            "field_columns": sorted(component_field_columns),
            "field_columns_sha256": stable_hash(sorted(component_field_columns)),
            "authority": "FULL_FROZEN_COMPONENT_POOL_FIELD_UNION",
            "derived_from_component_primary_and_control_field_receipts": True,
            "generated_probe_field_column_count": len(generated_field_columns),
            "generated_probe_field_columns_sha256": stable_hash(
                sorted(generated_field_columns)
            ),
            "generated_probe_fields_subset": generated_field_columns.issubset(
                component_field_columns
            ),
        },
        "generator": {
            "prefreeze_payload_sha256": prefreeze_hash,
            "seed": int(generator_contract["seed"]),
            "operation_priors": dict(generator_contract["operation_priors"]),
            "maximum_attempts": int(generator_contract["maximum_attempts"]),
            "probes_per_template": int(probes_per_template),
            "requested_total": len(TEMPLATES) * int(probes_per_template),
            "generated_total": len(observed_exact),
            "per_template": per_template,
            "diagnostics": generator.diagnostics(),
        },
        "failures": failures,
        "checks": checks,
        "candidate_evaluation_executed": False,
        "financial_sidecar_read": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--source-freeze-root", type=Path, default=Path(SOURCE_FREEZE_ROOT))
    parser.add_argument("--registry", type=Path, default=Path(SOURCE_REGISTRY_PATH))
    parser.add_argument("--stage1-prefreeze", type=Path, required=True)
    parser.add_argument("--probes-per-template", type=int, default=DEFAULT_PROBES_PER_TEMPLATE)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_search_core_v2_state_jump_real_supply_audit_20260824.json"),
    )
    args = parser.parse_args(argv)
    payload = audit(
        args.repo_root,
        source_freeze_root=args.source_freeze_root,
        registry_path=args.registry,
        stage1_prefreeze_path=args.stage1_prefreeze,
        probes_per_template=args.probes_per_template,
    )
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "status": payload["status"],
                "generated_total": payload["generator"]["generated_total"],
                "known_exact_count": payload["committed_zero_financial_supply"]["known_normalized_exact_count"],
                "failures": len(payload["failures"]),
                "payload": payload["audit_payload_sha256"],
                "output": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0 if payload["status"] == STATUS_PASS else 2


if __name__ == "__main__":
    raise SystemExit(main())
