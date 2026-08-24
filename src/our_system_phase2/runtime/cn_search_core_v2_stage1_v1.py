"""Project-Control route for the two-arm Search Core V2 Stage-1 tournament."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as source_runtime
from our_system_phase2.services.node_resource_governor import validate_node_resource_lease_receipt
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    ProjectControlDenied,
    consume_active_admission,
    sha256_file,
    verify_campaign_authorization_binding,
    verify_consumed_admission_target,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


ROUTE_ID = "cn-search-core-v2-stage1-v1"
CAMPAIGN_ID = "CN_SEARCH_CORE_V2_STAGE1_V1"
CAMPAIGN_PROFILE = "cn_search_core_v2_stage1_v1"
AUTHORIZATION_SCHEMA = "cn_search_core_v2_stage1_authorization_v1"
AUTHORIZATION_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_search_core_v2_stage1_authorization_v1.json"
)
PREFREEZE_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_search_core_v2_stage1_prefreeze_20260824.json"
)
SUPPLY_AUDIT_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_search_core_v2_state_jump_real_supply_audit_20260824.json"
)
PRIMARY_EXECUTOR_WORKERS = 24
CHECKPOINT_SIZE = 24
TOTAL_PER_ARM = 336
TOTAL_EVALUATIONS = 672


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _bound(
    root: Path,
    binding: Mapping[str, Any],
    *,
    payload_field: str,
    label: str,
) -> dict[str, Any]:
    path = (root / Path(str(binding["relative_path"]))).resolve()
    if (
        not path.is_relative_to(root)
        or not path.is_file()
        or sha256_file(path) != str(binding["file_sha256"])
    ):
        raise ValueError(f"{label} file drift")
    payload = _read(path)
    body = dict(payload)
    claimed = str(body.pop(payload_field, ""))
    if claimed != str(binding["payload_sha256"]) or stable_hash(body) != claimed:
        raise ValueError(f"{label} payload drift")
    return payload


def verify_authorization(
    path: Path, *, repo_root: Path | None = None
) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = _read(path.resolve())
    body = dict(payload)
    claimed = str(body.pop("authorization_payload_sha256", ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError("Search Core V2 Stage-1 authorization self-hash drift")
    if (
        payload.get("schema_version") != AUTHORIZATION_SCHEMA
        or payload.get("status") != "SEARCH_CORE_V2_STAGE1_AUTHORIZED_NOT_RUN"
        or payload.get("campaign_id") != CAMPAIGN_ID
        or payload.get("campaign_profile") != CAMPAIGN_PROFILE
        or payload.get("project_control_route_id") != ROUTE_ID
        or list(payload.get("permitted_project_control_actions") or ())
        != [ACTION_LAUNCH, ACTION_RETRY]
        or payload.get("evaluation_data_role") != "DEVELOPMENT_ONLY"
        or payload.get("execution_authorized") is not True
        or payload.get("validation_feedback_used") is not False
        or payload.get("promotion_authorized") is not False
        or payload.get("oos_authority") != "NONE"
        or payload.get("automatic_stage2_authorized") is not False
    ):
        raise ValueError("Search Core V2 Stage-1 authorization contract drift")

    source = dict(payload["source_stage_d_authorization"])
    source_path = (root / Path(str(source["relative_path"]))).resolve()
    if sha256_file(source_path) != str(source["file_sha256"]):
        raise ValueError("Search Core V2 source Stage-D authorization file drift")
    source_auth = source_runtime.verify_authorization(source_path, repo_root=root)
    if source_auth.get("authorization_payload_sha256") != source["payload_sha256"]:
        raise ValueError("Search Core V2 source Stage-D authorization payload drift")

    prefreeze = _bound(
        root,
        payload["stage1_prefreeze"],
        payload_field="prefreeze_payload_sha256",
        label="Search Core V2 Stage-1 prefreeze",
    )
    if (
        prefreeze.get("status")
        != "SEARCH_CORE_V2_STAGE1_PREFROZEN_BEFORE_FINANCIAL_READ"
        or int(prefreeze["stage1"]["total_budget_per_arm"]) != TOTAL_PER_ARM
        or int(prefreeze["stage1"]["total_financial_evaluations"])
        != TOTAL_EVALUATIONS
        or bool(prefreeze.get("financial_labels_read_by_builder"))
        or bool(prefreeze["stage1"].get("automatic_stage2_launch"))
    ):
        raise ValueError("Search Core V2 Stage-1 prefreeze binding drift")

    supply = _bound(
        root,
        payload["state_jump_supply_audit"],
        payload_field="audit_payload_sha256",
        label="Search Core V2 real state-jump supply audit",
    )
    supply_generator = dict(supply["generator"])
    frozen_generator = dict(prefreeze["arm_b_state_jump"])
    if (
        supply.get("status")
        != "PASS_ZERO_FINANCIAL_STATE_JUMP_REAL_SUPPLY_AUDIT"
        or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("financial_sidecar_read"))
        or int(supply_generator["generated_total"]) < TOTAL_EVALUATIONS
        or any(
            int(row.get("generated") or 0) < 48
            for row in dict(supply_generator["per_template"]).values()
        )
        or str(supply_generator.get("prefreeze_payload_sha256") or "")
        != str(prefreeze["prefreeze_payload_sha256"])
        or int(supply_generator.get("seed") or -1) != int(frozen_generator["seed"])
        or dict(supply_generator.get("operation_priors") or {})
        != dict(frozen_generator["operation_priors"])
        or int(supply_generator.get("maximum_attempts") or -1)
        != int(frozen_generator["maximum_attempts"])
    ):
        raise ValueError("Search Core V2 state-jump real supply evidence drift")

    resource = dict(payload["resource_contract"])
    canary_binding = dict(payload["official_resource_canary"])
    if (
        resource.get("profile") != "SEARCH_DUAL_24"
        or int(resource.get("cpu_threads") or 0) != 24
        or int(resource.get("executor_workers") or 0) != PRIMARY_EXECUTOR_WORKERS
        or int(resource.get("checkpoint_size") or 0) != CHECKPOINT_SIZE
        or resource.get("candidate_evaluation_during_canary") is not False
        or int(resource.get("field_column_count") or 0)
        != int(canary_binding.get("field_column_count") or 0)
        or str(resource.get("field_columns_sha256") or "")
        != str(canary_binding.get("field_columns_sha256") or "")
    ):
        raise ValueError("Search Core V2 resource contract drift")
    if any(int(value) != 0 for value in dict(payload["restricted_reads"]).values()):
        raise ValueError("Search Core V2 restricted-read authorization drift")
    return payload


def verify_official_canary(
    path: Path,
    authorization: Mapping[str, Any],
    *,
    repo_root: Path,
) -> dict[str, Any]:
    raw = path.resolve().read_bytes()
    observed = hashlib.sha256(raw).hexdigest()
    binding = dict(authorization["official_resource_canary"])
    if observed != str(binding["file_sha256"]):
        raise ProjectControlDenied("Search Core V2 official canary file drift")
    payload = json.loads(raw.decode("utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop("official_canary_payload_sha256", ""))
    if (
        claimed != str(binding["payload_sha256"])
        or stable_hash(body) != claimed
        or payload.get("status") != "PASS"
        or payload.get("candidate_evaluation_executed") is not False
        or int(payload.get("field_column_count") or 0)
        != int(binding["field_column_count"])
        or str(payload.get("field_columns_sha256") or "")
        != str(binding["field_columns_sha256"])
    ):
        raise ProjectControlDenied("Search Core V2 official canary payload drift")
    runner = repo_root / "scripts/run_cn_search_core_v2_stage1_v1.py"
    if sha256_file(runner) != str(binding["runner_source_file_sha256"]):
        raise ProjectControlDenied("Search Core V2 runner changed after official canary")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(ROUTE_ID, {ACTION_LAUNCH, ACTION_RETRY})
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--stage1-prefreeze", type=Path, required=True)
    parser.add_argument("--state-jump-supply-audit", type=Path, required=True)
    parser.add_argument("--official-resource-canary", type=Path, required=True)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--node-resource-lease-receipt", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    args.executor_workers = PRIMARY_EXECUTOR_WORKERS

    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(
        admission, args.campaign_authorization
    )
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("Search Core V2 authorization payload drift")
    root = Path(__file__).resolve().parents[3]
    expected_prefreeze = (root / Path(str(authorization["stage1_prefreeze"]["relative_path"]))).resolve()
    expected_supply = (root / Path(str(authorization["state_jump_supply_audit"]["relative_path"]))).resolve()
    if args.stage1_prefreeze.resolve() != expected_prefreeze:
        raise ProjectControlDenied("Search Core V2 prefreeze path outside authorization")
    if args.state_jump_supply_audit.resolve() != expected_supply:
        raise ProjectControlDenied("Search Core V2 supply-audit path outside authorization")
    verify_official_canary(
        args.official_resource_canary, authorization, repo_root=root
    )
    validate_node_resource_lease_receipt(
        args.node_resource_lease_receipt.resolve(),
        expected_role="SEARCH",
        expected_cpu_threads=24,
    )
    from scripts.run_cn_search_core_v2_stage1_v1 import run

    result = run(args, admission=admission, authorization=authorization)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
