from __future__ import annotations

import argparse, hashlib, json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.runtime import cn_program_primitive_main_production_v1 as source_runtime
from our_system_phase2.services.node_resource_governor import validate_node_resource_lease_receipt
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH, ACTION_RETRY, ProjectControlDenied, consume_active_admission, sha256_file, verify_campaign_authorization_binding, verify_consumed_admission_target
from our_system_phase2.services.unified_capability_registry import stable_hash

ROUTE_ID = "cn-program-base-event-replication-study-v1"
CAMPAIGN_ID = "CN_PROGRAM_BASE_EVENT_REPLICATION_STUDY_V1"
CAMPAIGN_PROFILE = "cn_program_base_event_replication_study_v1"
AUTHORIZATION_SCHEMA = "cn_program_base_event_replication_study_authorization_v1"
AUTHORIZATION_RELATIVE_PATH = Path("runtime/run_plans/cn_program_base_event_replication_study_authorization_v1.json")
PLAN_RELATIVE_PATH = Path("runtime/run_plans/cn_program_base_event_replication_study_plan.json")


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _self_bound(root: Path, binding: Mapping[str, Any], field: str, label: str) -> dict[str, Any]:
    path = (root / Path(str(binding["relative_path"]))).resolve()
    if not path.is_relative_to(root) or not path.is_file() or sha256_file(path) != str(binding["file_sha256"]):
        raise ValueError(f"{label} file drift")
    row = _read(path)
    body = dict(row); claim = str(body.pop(field, ""))
    if claim != str(binding["payload_sha256"]) or stable_hash(body) != claim:
        raise ValueError(f"{label} payload drift")
    return row


def verify_authorization(path: Path, *, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = _read(path.resolve()); body = dict(payload); claim = str(body.pop("authorization_payload_sha256", ""))
    if not claim or stable_hash(body) != claim:
        raise ValueError("base-event replication authorization self-hash drift")
    if payload.get("schema_version") != AUTHORIZATION_SCHEMA or payload.get("status") != "BASE_EVENT_REPLICATION_STUDY_AUTHORIZED_NOT_RUN" or payload.get("execution_authorized") is not True or payload.get("campaign_id") != CAMPAIGN_ID or payload.get("campaign_profile") != CAMPAIGN_PROFILE or payload.get("project_control_route_id") != ROUTE_ID or list(payload.get("permitted_project_control_actions") or ()) != [ACTION_LAUNCH, ACTION_RETRY] or payload.get("evaluation_data_role") != "DEVELOPMENT_ONLY" or payload.get("validation_feedback_used") is not False or payload.get("oos_authority") != "NONE" or payload.get("promotion_authorized") is not False or payload.get("automatic_successor_authorized") is not False:
        raise ValueError("base-event replication authorization contract drift")
    source_binding = dict(payload["source_production_authorization"]); source_path = (root / Path(str(source_binding["relative_path"]))).resolve()
    if sha256_file(source_path) != str(source_binding["file_sha256"]):
        raise ValueError("source production authorization file drift")
    source = source_runtime.verify_authorization(source_path, repo_root=root)
    if source["authorization_payload_sha256"] != source_binding["payload_sha256"]:
        raise ValueError("source production authorization payload drift")
    plan = _self_bound(root, payload["replication_plan"], "plan_payload_sha256", "replication plan")
    if int(plan["budget"]["hard_cap_logical_records"]) != 240 or plan.get("anchor_v3_used_in_prospective_gate") is not False:
        raise ValueError("base-event replication plan contract drift")
    pool = _self_bound(root, payload["pool_review"], "review_payload_sha256", "pool review")
    if int(pool.get("effective_spent_exact_count") or 0) != 8654 or len(pool.get("replicate_pools") or ()) != 2 or pool.get("candidate_evaluation_executed") is not False or pool.get("financial_labels_read") is not False:
        raise ValueError("base-event replication pool review drift")
    review = _self_bound(root, payload["study_review"], "review_payload_sha256", "study review")
    if review.get("status") != "BASE_EVENT_REPLICATION_STUDY_REVIEW_FROZEN" or review.get("new_candidate_evaluation_executed") is not False:
        raise ValueError("base-event replication study review drift")
    v3audit = _self_bound(root, payload["source_v3_terminal_audit"], "audit_payload_sha256", "v3 terminal audit")
    if v3audit.get("status") != "PASS_INDEPENDENT_TERMINAL_AUDIT" or v3audit.get("gate_status") != "PRIMITIVE_FRONTIER_CONTROLLED_V3_FAIL":
        raise ValueError("base-event replication v3 audit drift")
    v3outcome = _self_bound(root, payload["source_v3_postrun_outcome"], "outcome_payload_sha256", "v3 outcome")
    if v3outcome.get("post_batch_recommendation") != "REDIRECT" or v3outcome.get("redirect_target") != "BASE_EVENT_ONLY_SYSTEM_SCALE_DEVELOPMENT_SEARCH_REVIEW":
        raise ValueError("base-event replication v3 outcome drift")
    accel = _self_bound(root, payload["acceleration_accuracy_audit"], "audit_payload_sha256", "acceleration accuracy audit")
    if accel.get("status") != "PASS_SUCCESSOR_REAUTH_ELIGIBLE":
        raise ValueError("base-event replication acceleration audit drift")
    if any(payload.get(key) is not False for key in ("production_feedback_imported_into_primitive_stats", "v1_successor_feedback_imported_into_primitive_stats", "v2_successor_feedback_imported_into_primitive_stats", "v3_successor_feedback_imported_into_primitive_stats", "diversity_contract_changed")):
        raise ValueError("base-event replication search-authority drift")
    rc = dict(payload["resource_contract"]); canary = dict(payload["official_resource_canary"])
    if rc.get("profile") != "SEARCH_DUAL_24" or int(rc.get("cpu_threads") or 0) != 24 or int(rc.get("primary_executor_workers") or 0) != 24 or int(rc.get("fallback_executor_workers") or 0) != 16 or int(rc.get("hard_cap_logical_records") or 0) != 240 or int(rc.get("field_column_count") or 0) <= 0 or int(rc.get("field_column_count") or 0) != int(canary.get("field_column_count") or 0) or rc.get("field_columns_sha256") != canary.get("field_columns_sha256") or rc.get("evaluator_pool_lifetime") != "PERSISTENT_RUN_SCOPE" or float(rc.get("minimum_records_per_hour_after_first_checkpoint") or 0) < 650.0 or rc.get("persistent_pool_record_hash_parity_required") is not True:
        raise ValueError("base-event replication resource contract drift")
    if any(int(v) != 0 for v in dict(payload["restricted_reads"]).values()):
        raise ValueError("base-event replication restricted-read drift")
    runner = root / "scripts/run_cn_program_base_event_replication_study_v1.py"
    if sha256_file(runner) != str(payload["implementation"]["runner_source_file_sha256"]):
        raise ValueError("base-event replication runner drift")
    return payload


def verify_official_canary(path: Path, authorization: Mapping[str, Any], *, repo_root: Path) -> dict[str, Any]:
    raw = path.resolve().read_bytes(); binding = dict(authorization["official_resource_canary"])
    if hashlib.sha256(raw).hexdigest() != str(binding["file_sha256"]):
        raise ProjectControlDenied("base-event replication canary file drift")
    payload = json.loads(raw.decode("utf-8-sig")); body = dict(payload); claim = str(body.pop("official_canary_payload_sha256", ""))
    if claim != str(binding["payload_sha256"]) or stable_hash(body) != claim or payload.get("status") != "PASS" or payload.get("candidate_evaluation_executed") is not False or int(payload.get("preview_selected_count") or 0) != 240 or int(payload.get("field_column_count") or 0) <= 0 or int(payload.get("field_column_count") or 0) != int(binding.get("field_column_count") or 0) or payload.get("field_columns_sha256") != binding["field_columns_sha256"] or payload.get("replication_plan_payload_sha256") != binding["replication_plan_payload_sha256"] or payload.get("pool_review_payload_sha256") != binding["pool_review_payload_sha256"] or payload.get("study_review_payload_sha256") != binding["study_review_payload_sha256"] or payload.get("source_v3_terminal_audit_payload_sha256") != binding["source_v3_terminal_audit_payload_sha256"] or payload.get("source_v3_postrun_outcome_payload_sha256") != binding["source_v3_postrun_outcome_payload_sha256"] or payload.get("acceleration_accuracy_audit_payload_sha256") != binding["acceleration_accuracy_audit_payload_sha256"] or payload.get("evaluator_pool_lifetime") != "PERSISTENT_RUN_SCOPE":
        raise ProjectControlDenied("base-event replication canary payload drift")
    if sha256_file(repo_root / "scripts/run_cn_program_base_event_replication_study_v1.py") != str(binding["runner_source_file_sha256"]):
        raise ProjectControlDenied("base-event replication runner changed after canary")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(ROUTE_ID, {ACTION_LAUNCH, ACTION_RETRY})
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--replication-plan", type=Path, required=True)
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
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("base-event replication authorization payload drift")
    root = Path(__file__).resolve().parents[3]
    if args.replication_plan.resolve() != (root / PLAN_RELATIVE_PATH).resolve():
        raise ProjectControlDenied("base-event replication plan path outside authorization")
    verify_official_canary(args.official_resource_canary, authorization, repo_root=root)
    validate_node_resource_lease_receipt(args.node_resource_lease_receipt.resolve(), expected_role="SEARCH", expected_cpu_threads=24)
    from scripts.run_cn_program_base_event_replication_study_v1 import run
    result = run(args, admission=admission, authorization=authorization)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
