"""Official zero-financial resource canary for Search Core V2 Stage-1.

This script is run on the authorized 77o node after the real state-jump supply
audit and before campaign authorization.  It reconstructs the frozen Primitive
A-arm schedules, unions their physical field surface with the Generator V2
primary+matched-control field surface from the zero-financial supply audit, and
starts the existing SEARCH_DUAL_24 worker initializer only.  It never evaluates
any candidate.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_large_fresh_v1 as large_fresh
from scripts import run_cn_search_core_v2_stage1_v1 as stage1
from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as stage_d_runtime
from our_system_phase2.runtime import cn_search_core_v2_stage1_v1 as runtime
from our_system_phase2.services.project_control_admission import sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash


PROBE_SECONDS = 30.0


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify_self(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo = PROJECT_ROOT.resolve()
    source_auth = stage_d_runtime.verify_authorization(
        args.source_stage_d_authorization.resolve(), repo_root=repo
    )
    prefreeze = stage1.verify_prefreeze(args.stage1_prefreeze.resolve())
    supply_audit = _read(args.state_jump_supply_audit.resolve())
    supply_hash = _verify_self(
        supply_audit, "audit_payload_sha256", "Search Core V2 state-jump supply audit"
    )
    supply_generator = dict(supply_audit["generator"])
    frozen_generator = dict(prefreeze["arm_b_state_jump"])
    if (
        supply_audit.get("status")
        != "PASS_ZERO_FINANCIAL_STATE_JUMP_REAL_SUPPLY_AUDIT"
        or bool(supply_audit.get("candidate_evaluation_executed"))
        or bool(supply_audit.get("financial_sidecar_read"))
        or str(supply_generator.get("prefreeze_payload_sha256") or "")
        != str(prefreeze["prefreeze_payload_sha256"])
        or int(supply_generator.get("seed") or -1) != int(frozen_generator["seed"])
        or dict(supply_generator.get("operation_priors") or {})
        != dict(frozen_generator["operation_priors"])
        or int(supply_generator.get("maximum_attempts") or -1)
        != int(frozen_generator["maximum_attempts"])
    ):
        raise RuntimeError("SEARCH_CORE_V2_RESOURCE_CANARY_SUPPLY_AUDIT_NOT_PASS")

    # _load_authority needs the target campaign identifiers and the already
    # authorized development prior.  No target campaign authorization exists yet;
    # this provisional binding is used only to reopen frozen authority and start
    # zero-candidate worker initialization.
    provisional = {
        "authorization_payload_sha256": stable_hash(
            {
                "role": "SEARCH_CORE_V2_STAGE1_ZERO_FINANCIAL_RESOURCE_CANARY",
                "prefreeze_payload_sha256": prefreeze["prefreeze_payload_sha256"],
                "state_jump_supply_audit_payload_sha256": supply_hash,
            }
        ),
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "source_prior_exact": dict(source_auth["source_prior_exact"]),
    }
    args.executor_workers = runtime.PRIMARY_EXECUTOR_WORKERS
    authority = stage1._load_authority(
        args,
        authorization=provisional,
        repo_sha=str(args.repo_sha),
    )
    stage_d_supply = stage1._stage_d_supply(repo, prefreeze)
    supply_by_exact = {
        str(row["exact_identity"]): dict(row)
        for row in stage_d_supply["fresh_entries"]
    }

    schedules = []
    global_ordinal = 0
    for template in stage1.TEMPLATES:
        exacts = list(
            map(
                str,
                prefreeze["arm_a_primitive"]["eligible_orders_by_template"][template],
            )
        )[:48]
        if len(exacts) != 48:
            raise RuntimeError("SEARCH_CORE_V2_RESOURCE_CANARY_ARM_A_UNDERFILL")
        for template_ordinal, exact in enumerate(exacts):
            schedules.append(
                stage1._static_schedule(
                    supply_by_exact[exact],
                    authority=authority,
                    global_ordinal=global_ordinal,
                    checkpoint_ordinal=global_ordinal // runtime.CHECKPOINT_SIZE,
                    template_ordinal=template_ordinal,
                )
            )
            global_ordinal += 1
    if len(schedules) != runtime.TOTAL_PER_ARM:
        raise RuntimeError("SEARCH_CORE_V2_RESOURCE_CANARY_ARM_A_COUNT_DRIFT")

    arm_a_fields = set(map(str, engine._checkpoint_field_columns(schedules)))
    arm_b_fields = set(
        map(str, supply_audit["resource_field_surface"]["field_columns"])
    )
    fields = tuple(sorted(arm_a_fields | arm_b_fields))
    if not fields:
        raise RuntimeError("SEARCH_CORE_V2_RESOURCE_CANARY_FIELD_SURFACE_EMPTY")

    old_probe = large_fresh.RESOURCE_CANARY_PROBE_SECONDS
    try:
        large_fresh.RESOURCE_CANARY_PROBE_SECONDS = PROBE_SECONDS
        canary = large_fresh._resource_canary(
            authority,
            str(authority["input_binding"]["input_binding_sha256"]),
            runtime.PRIMARY_EXECUTOR_WORKERS,
            fields,
        )
    finally:
        large_fresh.RESOURCE_CANARY_PROBE_SECONDS = old_probe
    if (
        canary.get("status") != "PASS_ZERO_CANDIDATE_EVALUATION_RESOURCE_CANARY"
        or int(canary.get("requested_workers") or 0)
        != runtime.PRIMARY_EXECUTOR_WORKERS
        or int(canary.get("pagefile_pages_in_delta_bytes", -1)) != 0
        or int(canary.get("pagefile_pages_out_delta_bytes", -1)) != 0
        or canary.get("candidate_evaluation_executed") is not False
    ):
        raise RuntimeError("SEARCH_CORE_V2_OFFICIAL_RESOURCE_CANARY_FAIL")

    runner = repo / "scripts/run_cn_search_core_v2_stage1_v1.py"
    payload = {
        "schema_version": "cn_search_core_v2_stage1_official_resource_canary_v1",
        "status": "PASS",
        "repo_sha": str(args.repo_sha),
        "runner_source_file_sha256": sha256_file(runner),
        "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
        "state_jump_supply_audit_payload_sha256": supply_hash,
        "source_stage_d_authorization_payload_sha256": str(
            source_auth["authorization_payload_sha256"]
        ),
        "resource_profile": "SEARCH_DUAL_24",
        "requested_workers": runtime.PRIMARY_EXECUTOR_WORKERS,
        "field_column_count": len(fields),
        "field_columns": list(fields),
        "field_columns_sha256": stable_hash(list(fields)),
        "arm_a_field_column_count": len(arm_a_fields),
        "arm_a_field_columns_sha256": stable_hash(sorted(arm_a_fields)),
        "arm_b_field_column_count": len(arm_b_fields),
        "arm_b_field_columns_sha256": stable_hash(sorted(arm_b_fields)),
        "resource_probe": dict(canary),
        "candidate_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["official_canary_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-stage-d-authorization", type=Path, required=True)
    parser.add_argument("--stage1-prefreeze", type=Path, required=True)
    parser.add_argument("--state-jump-supply-audit", type=Path, required=True)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = run(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    engine._write_json(args.output, payload)
    print(
        json.dumps(
            {
                "status": payload["status"],
                "field_column_count": payload["field_column_count"],
                "payload": payload["official_canary_payload_sha256"],
                "output": str(args.output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
