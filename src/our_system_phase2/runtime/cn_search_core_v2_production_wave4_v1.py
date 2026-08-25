"""Project-Control route for Search Core V2 Production Wave 4."""
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

ROUTE_ID = "cn-search-core-v2-production-wave4-v1"
CAMPAIGN_ID = "CN_SEARCH_CORE_V2_PRODUCTION_WAVE4_V1"
CAMPAIGN_PROFILE = "cn_search_core_v2_production_wave4_v1"
AUTHORIZATION_SCHEMA = "cn_search_core_v2_production_wave4_authorization_v1"
AUTHORIZATION_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave4_authorization_v1.json")
PREFREEZE_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave4_prefreeze_20260826.json")
SUPPLY_AUDIT_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave4_supply_audit_20260826.json")
PRIMARY_EXECUTOR_WORKERS = 24
CHECKPOINT_SIZE = 24
CHECKPOINT_COUNT = 8
TOTAL_EVALUATIONS = 192
SUPPLY_PROBE_TOTAL = 96


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _bound(root: Path, binding: Mapping[str, Any], *, field: str, label: str) -> dict[str, Any]:
    path = (root / Path(str(binding["relative_path"]))).resolve()
    if not path.is_relative_to(root) or not path.is_file() or sha256_file(path) != str(binding["file_sha256"]):
        raise ValueError(f"{label} file drift")
    payload = _read(path)
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if claimed != str(binding["payload_sha256"]) or stable_hash(body) != claimed:
        raise ValueError(f"{label} payload drift")
    return payload


def verify_authorization(path: Path, *, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = _read(path.resolve())
    body = dict(payload)
    claimed = str(body.pop("authorization_payload_sha256", ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError("Production Wave 4 authorization self-hash drift")
    if (
        payload.get("schema_version") != AUTHORIZATION_SCHEMA
        or payload.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE4_AUTHORIZED_NOT_RUN"
        or payload.get("campaign_id") != CAMPAIGN_ID
        or payload.get("campaign_profile") != CAMPAIGN_PROFILE
        or payload.get("project_control_route_id") != ROUTE_ID
        or list(payload.get("permitted_project_control_actions") or ()) != [ACTION_LAUNCH, ACTION_RETRY]
        or payload.get("execution_authorized") is not True
        or payload.get("evaluation_data_role") != "DEVELOPMENT_ONLY"
        or payload.get("validation_feedback_used") is not False
        or payload.get("promotion_authorized") is not False
        or payload.get("oos_authority") != "NONE"
        or payload.get("capital_action_authorized") is not False
        or payload.get("automatic_followon_authorized") is not False
    ):
        raise ValueError("Production Wave 4 authorization contract drift")
    source = dict(payload["source_stage_d_authorization"])
    source_path = (root / Path(str(source["relative_path"]))).resolve()
    if sha256_file(source_path) != str(source["file_sha256"]):
        raise ValueError("Production Wave 4 source Stage-D file drift")
    source_auth = source_runtime.verify_authorization(source_path, repo_root=root)
    if source_auth.get("authorization_payload_sha256") != source["payload_sha256"]:
        raise ValueError("Production Wave 4 source Stage-D payload drift")
    policy = _bound(root, payload["source_policy_review"], field="policy_review_payload_sha256", label="Production Wave 4 policy review")
    prefreeze = _bound(root, payload["production_wave4_prefreeze"], field="prefreeze_payload_sha256", label="Production Wave 4 prefreeze")
    supply = _bound(root, payload["production_wave4_supply_audit"], field="audit_payload_sha256", label="Production Wave 4 supply audit")
    pair_contract = dict(payload.get("pair_native_annotation_contract") or {})
    if pair_contract != dict(prefreeze.get("pair_native_annotation_contract") or {}):
        raise ValueError("Production Wave 4 pair-native authorization binding drift")
    if (
        pair_contract.get("application_scope") != "DEVELOPMENT_RESULT_ANNOTATION_AND_ARCHIVE_ONLY"
        or pair_contract.get("optimizer_feedback_write") is not False
        or pair_contract.get("generator_ask_order_changed") is not False
        or pair_contract.get("candidate_admission_changed") is not False
        or pair_contract.get("automatic_policy_adoption") is not False
        or pair_contract.get("validation_label_read_at_application") is not False
    ):
        raise ValueError("Production Wave 4 pair-native authorization contract drift")
    if (
        policy.get("status") != "SEARCH_CORE_V2_POLICY_REVIEW_COMPLETE_MATURE_STATE_JUMP_PRIMARY"
        or policy["project_control_decision"].get("search_core_policy_change_authorized") is not True
        or int(prefreeze["production_wave4"].get("total_financial_evaluations") or 0) != TOTAL_EVALUATIONS
        or int(prefreeze["production_wave4"].get("total_checkpoint_count") or 0) != CHECKPOINT_COUNT
        or supply.get("status") != "PASS_ZERO_FINANCIAL_PRODUCTION_WAVE4_CHECKPOINT_SUPPLY_AUDIT"
        or int(supply.get("generated_total") or 0) != SUPPLY_PROBE_TOTAL
        or int(supply.get("prior_overlap_count", -1)) != 0
        or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("financial_sidecar_read"))
    ):
        raise ValueError("Production Wave 4 authority evidence drift")
    resource = dict(payload["resource_contract"])
    canary = dict(payload["official_resource_canary"])
    if (
        resource.get("profile") != "SEARCH_DUAL_24"
        or int(resource.get("cpu_threads") or 0) != 24
        or int(resource.get("executor_workers") or 0) != 24
        or int(resource.get("checkpoint_count") or 0) != CHECKPOINT_COUNT
        or resource.get("candidate_evaluation_during_canary") is not False
        or int(resource.get("field_column_count") or 0) != int(canary.get("field_column_count") or 0)
        or str(resource.get("field_columns_sha256") or "") != str(canary.get("field_columns_sha256") or "")
    ):
        raise ValueError("Production Wave 4 resource contract drift")
    if any(int(value) != 0 for value in dict(payload["restricted_reads"]).values()):
        raise ValueError("Production Wave 4 restricted-read drift")
    return payload


def verify_official_canary(path: Path, authorization: Mapping[str, Any], *, repo_root: Path) -> dict[str, Any]:
    raw = path.resolve().read_bytes()
    observed = hashlib.sha256(raw).hexdigest()
    binding = dict(authorization["official_resource_canary"])
    if observed != str(binding["file_sha256"]):
        raise ProjectControlDenied("Production Wave 4 canary file drift")
    payload = json.loads(raw.decode("utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop("official_canary_payload_sha256", ""))
    if (
        claimed != str(binding["payload_sha256"])
        or stable_hash(body) != claimed
        or payload.get("status") != "PASS"
        or payload.get("candidate_evaluation_executed") is not False
        or int(payload.get("field_column_count") or 0) != int(binding["field_column_count"])
        or str(payload.get("field_columns_sha256") or "") != str(binding["field_columns_sha256"])
    ):
        raise ProjectControlDenied("Production Wave 4 canary payload drift")
    runner = repo_root / "scripts/run_cn_search_core_v2_production_wave4_v1.py"
    if sha256_file(runner) != str(binding["runner_source_file_sha256"]):
        raise ProjectControlDenied("Production Wave 4 runner changed after canary")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(ROUTE_ID, {ACTION_LAUNCH, ACTION_RETRY})
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--production-wave4-prefreeze", type=Path, required=True)
    parser.add_argument("--production-wave4-supply-audit", type=Path, required=True)
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
    verified = verify_campaign_authorization_binding(admission, args.campaign_authorization)
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("Production Wave 4 authorization payload drift")
    root = Path(__file__).resolve().parents[3]
    expected_pre = (root / Path(str(authorization["production_wave4_prefreeze"]["relative_path"]))).resolve()
    expected_supply = (root / Path(str(authorization["production_wave4_supply_audit"]["relative_path"]))).resolve()
    if args.production_wave4_prefreeze.resolve() != expected_pre or args.production_wave4_supply_audit.resolve() != expected_supply:
        raise ProjectControlDenied("Production Wave 4 frozen path outside authorization")
    verify_official_canary(args.official_resource_canary, authorization, repo_root=root)
    validate_node_resource_lease_receipt(
        args.node_resource_lease_receipt.resolve(), expected_role="SEARCH", expected_cpu_threads=24
    )
    from scripts.run_cn_search_core_v2_production_wave4_v1 import run

    result = run(args, admission=admission, authorization=authorization)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
