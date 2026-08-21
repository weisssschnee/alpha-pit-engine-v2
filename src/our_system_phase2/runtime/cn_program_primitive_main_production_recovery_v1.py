from __future__ import annotations

import argparse, hashlib, json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.runtime import cn_program_primitive_main_production_v1 as source_runtime
from our_system_phase2.services.node_resource_governor import validate_node_resource_lease_receipt
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH, ACTION_RETRY, ProjectControlDenied, consume_active_admission,
    sha256_file, verify_campaign_authorization_binding, verify_consumed_admission_target,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

ROUTE_ID = "cn-program-primitive-main-production-recovery-v1"
CAMPAIGN_ID = "CN_PROGRAM_PRIMITIVE_MAIN_PRODUCTION_RECOVERY_V1"
CAMPAIGN_PROFILE = "cn_program_primitive_main_production_recovery_v1"
AUTHORIZATION_SCHEMA = "cn_program_primitive_main_production_recovery_authorization_v1"
AUTHORIZATION_RELATIVE_PATH = Path("runtime/run_plans/cn_program_primitive_main_production_recovery_authorization_v1.json")
RECOVERY_RELATIVE_PATH = Path("runtime/run_plans/cn_program_primitive_main_production_recovery_prefix_be111a2_20260821.json")


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _self_hashed_bound(root: Path, binding: Mapping[str, Any], field: str, label: str) -> dict[str, Any]:
    path = (root / Path(str(binding["relative_path"]))).resolve()
    if not path.is_relative_to(root) or not path.is_file() or sha256_file(path) != str(binding["file_sha256"]):
        raise ValueError(f"{label} file drift")
    row = _read(path); body = dict(row); claim = str(body.pop(field, ""))
    if claim != str(binding["payload_sha256"]) or stable_hash(body) != claim:
        raise ValueError(f"{label} payload drift")
    return row


def verify_authorization(path: Path, *, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = _read(path.resolve()); body = dict(payload); claim = str(body.pop("authorization_payload_sha256", ""))
    if not claim or stable_hash(body) != claim:
        raise ValueError("primitive recovery authorization self-hash drift")
    if (
        payload.get("schema_version") != AUTHORIZATION_SCHEMA
        or payload.get("status") != "PRIMITIVE_MAIN_PRODUCTION_RECOVERY_AUTHORIZED_NOT_RUN"
        or payload.get("execution_authorized") is not True
        or payload.get("campaign_id") != CAMPAIGN_ID
        or payload.get("campaign_profile") != CAMPAIGN_PROFILE
        or payload.get("project_control_route_id") != ROUTE_ID
        or list(payload.get("permitted_project_control_actions") or ()) != [ACTION_LAUNCH, ACTION_RETRY]
        or payload.get("evaluation_data_role") != "DEVELOPMENT_ONLY"
        or payload.get("financial_evaluator_reexecution_authorized") is not False
        or payload.get("validation_feedback_used") is not False
        or payload.get("promotion_authorized") is not False
        or payload.get("oos_authority") != "NONE"
        or payload.get("automatic_successor_authorized") is not False
    ):
        raise ValueError("primitive recovery authorization contract drift")
    source_binding = dict(payload["source_production_authorization"])
    source_path = (root / Path(source_binding["relative_path"])).resolve()
    if sha256_file(source_path) != str(source_binding["file_sha256"]):
        raise ValueError("source production authorization file drift")
    source = source_runtime.verify_authorization(source_path, repo_root=root)
    if source["authorization_payload_sha256"] != source_binding["payload_sha256"]:
        raise ValueError("source production authorization payload drift")
    recovery = _self_hashed_bound(root, payload["recovery_prefix"], "recovery_prefix_payload_sha256", "recovery prefix")
    if (
        recovery.get("status") != "PRIMITIVE_MAIN_RECOVERY_PREFIX_FROZEN"
        or int(recovery.get("completed_logical_records") or 0) != 24
        or int(recovery.get("remaining_new_evaluations") or 0) != 816
        or int(recovery.get("final_total_logical_records") or 0) != 840
        or recovery.get("financial_evaluator_reexecution_authorized") is not False
        or recovery.get("financial_evaluator_reexecution_performed") is not False
    ):
        raise ValueError("primitive recovery prefix contract drift")
    resource = dict(payload["resource_contract"])
    if (
        resource.get("profile") != "SEARCH_DUAL_24"
        or int(resource.get("cpu_threads") or 0) != 24
        or int(resource.get("primary_executor_workers") or 0) != 24
        or int(resource.get("checkpoint_size") or 0) != 24
        or int(resource.get("recovered_records") or 0) != 24
        or int(resource.get("remaining_new_evaluations") or 0) != 816
        or int(resource.get("final_hard_cap") or 0) != 840
    ):
        raise ValueError("primitive recovery resource contract drift")
    if any(int(v) != 0 for v in dict(payload["restricted_reads"]).values()):
        raise ValueError("primitive recovery restricted-read drift")
    runner = root / "scripts/run_cn_program_primitive_main_production_recovery_v1.py"
    proposal = root / "src/our_system_phase2/services/candidate_program_proposal_v0.py"
    if sha256_file(runner) != str(payload["implementation"]["recovery_runner_source_file_sha256"]) or sha256_file(proposal) != str(payload["implementation"]["proposal_registry_source_file_sha256"]):
        raise ValueError("primitive recovery implementation drift")
    return payload


def verify_official_canary(path: Path, authorization: Mapping[str, Any], *, repo_root: Path) -> dict[str, Any]:
    raw = path.resolve().read_bytes(); binding = dict(authorization["official_resource_canary"])
    if hashlib.sha256(raw).hexdigest() != str(binding["file_sha256"]):
        raise ProjectControlDenied("primitive recovery canary file drift")
    payload = json.loads(raw.decode("utf-8-sig")); body = dict(payload); claim = str(body.pop("official_canary_payload_sha256", ""))
    if (
        claim != str(binding["payload_sha256"])
        or stable_hash(body) != claim
        or payload.get("status") != "PASS"
        or payload.get("candidate_evaluation_executed") is not False
        or payload.get("financial_evaluator_reexecution_performed") is not False
        or int(payload.get("field_column_count") or 0) != int(binding["field_column_count"])
        or payload.get("field_columns_sha256") != binding["field_columns_sha256"]
        or int(payload.get("recovered_logical_records") or 0) != 24
        or int(payload.get("remaining_new_evaluations") or 0) != 816
    ):
        raise ProjectControlDenied("primitive recovery canary payload drift")
    if sha256_file(repo_root / "scripts/run_cn_program_primitive_main_production_recovery_v1.py") != str(binding["recovery_runner_source_file_sha256"]):
        raise ProjectControlDenied("primitive recovery runner changed after canary")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(ROUTE_ID, {ACTION_LAUNCH, ACTION_RETRY})
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--recovery-prefix", type=Path, required=True)
    parser.add_argument("--production-plan", type=Path, required=True)
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
    args = parser.parse_args(argv); args.executor_workers = 24
    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(admission, args.campaign_authorization)
    auth = verify_authorization(verified.path)
    if dict(verified.payload) != auth:
        raise ProjectControlDenied("primitive recovery authorization payload drift")
    root = Path(__file__).resolve().parents[3]
    if args.recovery_prefix.resolve() != (root / RECOVERY_RELATIVE_PATH).resolve():
        raise ProjectControlDenied("primitive recovery prefix path outside authorization")
    if args.production_plan.resolve() != (root / Path(auth["source_production_authorization"]["production_plan_relative_path"])).resolve():
        raise ProjectControlDenied("primitive recovery production plan path outside authorization")
    verify_official_canary(args.official_resource_canary, auth, repo_root=root)
    validate_node_resource_lease_receipt(args.node_resource_lease_receipt.resolve(), expected_role="SEARCH", expected_cpu_threads=24)
    from scripts.run_cn_program_primitive_main_production_recovery_v1 import run
    result = run(args, admission=admission, authorization=auth)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
