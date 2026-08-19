"""Project-Control entry for the prospective primitive-local Stage-B benchmark."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.runtime import cn_program_disclosure_timing_mechanism_successor_v1 as source_runtime
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

ROUTE_ID = "cn-program-primitive-local-stage-b-benchmark-v1"
CAMPAIGN_ID = "CN_PROGRAM_PRIMITIVE_LOCAL_STAGE_B_BENCHMARK_V1"
CAMPAIGN_PROFILE = "cn_program_primitive_local_stage_b_benchmark_v1"
AUTHORIZATION_SCHEMA = "cn_program_primitive_local_stage_b_authorization_v1"
AUTHORIZATION_RELATIVE_PATH = Path("runtime/run_plans/cn_program_primitive_local_stage_b_authorization_v1.json")


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _bound(root: Path, binding: Mapping[str, Any], *, field: str, label: str) -> Path:
    path = (root / Path(str(binding["relative_path"]))).resolve()
    if not path.is_relative_to(root) or not path.is_file() or sha256_file(path) != str(binding["file_sha256"]):
        raise ValueError(f"{label} file drift")
    payload = _read(path)
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if claimed != str(binding["payload_sha256"]) or stable_hash(body) != claimed:
        raise ValueError(f"{label} payload drift")
    return path


def verify_authorization(path: Path, *, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = _read(path.resolve())
    body = dict(payload)
    claimed = str(body.pop("authorization_payload_sha256", ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError("primitive-local Stage-B authorization self-hash drift")
    if (
        payload.get("schema_version") != AUTHORIZATION_SCHEMA
        or payload.get("status") != "PRIMITIVE_LOCAL_STAGE_B_BENCHMARK_AUTHORIZED_NOT_RUN"
        or payload.get("campaign_id") != CAMPAIGN_ID
        or payload.get("campaign_profile") != CAMPAIGN_PROFILE
        or payload.get("project_control_route_id") != ROUTE_ID
        or list(payload.get("permitted_project_control_actions") or ()) != [ACTION_LAUNCH, ACTION_RETRY]
        or payload.get("evaluation_data_role") != "DEVELOPMENT_ONLY"
        or payload.get("execution_authorized") is not True
        or payload.get("validation_feedback_used") is not False
        or payload.get("promotion_authorized") is not False
        or payload.get("oos_authority") != "NONE"
        or payload.get("automatic_successor_authorized") is not False
    ):
        raise ValueError("primitive-local Stage-B authorization contract drift")

    source_binding = dict(payload["source_mechanism_authorization"])
    source_path = _bound(root, source_binding, field="authorization_payload_sha256", label="source mechanism authorization")
    source_authorization = source_runtime.verify_authorization(source_path, repo_root=root)
    if source_authorization.get("authorization_payload_sha256") != source_binding["payload_sha256"]:
        raise ValueError("source mechanism authorization replay drift")

    from scripts.run_cn_program_primitive_local_stage_b_benchmark_v1 import verify_policy
    from scripts.run_cn_program_disclosure_timing_mechanism_successor_v1 import verify_prefreeze

    policy_binding = dict(payload["search_policy"])
    policy_path = _bound(root, policy_binding, field="policy_payload_sha256", label="Stage B search policy")
    policy = verify_policy(policy_path)
    if (
        policy_binding.get("stage_b_labels_read_during_policy_freeze") is not False
        or int(policy_binding.get("primary_budget") or 0) != 72
        or int(policy_binding.get("stage_b_field_column_count") or 0) != 38
        or policy_binding.get("stage_b_field_columns_sha256")
        != policy["stage_b_resource_preview"]["field_columns_sha256"]
        or policy.get("stage_b_financial_labels_read_during_policy_freeze") is not False
    ):
        raise ValueError("Stage B policy authorization drift")

    prefreeze_binding = dict(payload["mechanism_prefreeze"])
    prefreeze_path = _bound(root, prefreeze_binding, field="prefreeze_payload_sha256", label="mechanism prefreeze")
    prefreeze = verify_prefreeze(prefreeze_path)
    if (
        int(prefreeze_binding.get("stage_b_records") or 0) != 264
        or int(prefreeze_binding.get("field_column_count") or 0) != 42
        or prefreeze_binding.get("field_columns_sha256") != prefreeze["resource_preview"]["union_field_columns_sha256"]
    ):
        raise ValueError("Stage B prefreeze authorization drift")

    audit_binding = dict(payload["source_stage_a_audit"])
    audit_path = _bound(root, audit_binding, field="audit_payload_sha256", label="Stage A terminal audit")
    audit = _read(audit_path)
    if (
        audit.get("status") != "PASS_INDEPENDENT_TERMINAL_AUDIT"
        or audit.get("classification") != "LOCAL_PRIMITIVE_WIN_NOT_SYSTEMATIC_EVENT_FAMILY"
        or audit.get("stage_b_never_started") is not True
    ):
        raise ValueError("Stage A audit authorization drift")

    spent = dict(payload["spent_contract"])
    policy_spent = dict(policy["spent_freeze"])
    if (
        int(spent.get("combined_spent_exact_count") or 0) != 2438
        or int(spent.get("stage_b_overlap_count", -1)) != 0
        or spent.get("combined_spent_exact_identities_sha256") != policy_spent.get("combined_spent_exact_identities_sha256")
    ):
        raise ValueError("Stage B spent authorization drift")

    resource = dict(payload["resource_contract"])
    if (
        resource.get("profile") != "SEARCH_DUAL_24"
        or int(resource.get("cpu_threads") or 0) != 24
        or int(resource.get("executor_workers") or 0) != 24
        or float(resource.get("resource_canary_probe_seconds") or 0.0) != 30.0
        or int(resource.get("resource_canary_field_column_count") or 0) != 38
        or resource.get("candidate_evaluation_during_resource_canary") is not False
    ):
        raise ValueError("Stage B resource authorization drift")
    benchmark = dict(payload["benchmark_contract"])
    if (
        int(benchmark.get("physical_stage_b_evaluations") or 0) != 264
        or benchmark.get("search_policy_orders_frozen_before_stage_b_labels") is not True
        or benchmark.get("stage_c_execution_authorized_by_this_campaign") is not False
    ):
        raise ValueError("Stage B benchmark contract drift")
    if any(int(value) != 0 for value in dict(payload["restricted_reads"]).values()):
        raise ValueError("Stage B restricted-read authorization drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(ROUTE_ID, {ACTION_LAUNCH, ACTION_RETRY})
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--search-policy", type=Path, required=True)
    parser.add_argument("--mechanism-prefreeze", type=Path, required=True)
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
    args.executor_workers = 24

    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(admission, args.campaign_authorization)
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("primitive-local Stage-B authorization payload drift")
    repo_root = Path(__file__).resolve().parents[3]
    if args.search_policy.resolve() != (repo_root / authorization["search_policy"]["relative_path"]).resolve():
        raise ProjectControlDenied("Stage B search policy path outside authorization")
    if args.mechanism_prefreeze.resolve() != (repo_root / authorization["mechanism_prefreeze"]["relative_path"]).resolve():
        raise ProjectControlDenied("Stage B prefreeze path outside authorization")
    validate_node_resource_lease_receipt(
        args.node_resource_lease_receipt.resolve(),
        expected_role="SEARCH",
        expected_cpu_threads=24,
    )
    from scripts.run_cn_program_primitive_local_stage_b_benchmark_v1 import run

    result = run(args, admission=admission, authorization=authorization)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
