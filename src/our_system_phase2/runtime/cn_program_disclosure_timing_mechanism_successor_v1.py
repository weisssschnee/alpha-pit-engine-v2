"""Project-Control entry for the disclosure-timing mechanism falsification successor."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

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

ROUTE_ID = "cn-program-disclosure-timing-mechanism-successor-v1"
CAMPAIGN_ID = "CN_PROGRAM_DISCLOSURE_TIMING_MECHANISM_SUCCESSOR_V1"
CAMPAIGN_PROFILE = "cn_program_disclosure_timing_mechanism_successor_v1"
AUTHORIZATION_SCHEMA = "cn_program_disclosure_timing_mechanism_successor_authorization_v1"
AUTHORIZATION_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_disclosure_timing_mechanism_successor_v1.json"
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _bound_path(root: Path, binding: Mapping[str, Any], *, label: str) -> Path:
    path = (root / Path(str(binding.get("relative_path") or ""))).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError(f"{label} path drift")
    if sha256_file(path) != str(binding.get("file_sha256") or ""):
        raise ValueError(f"{label} file hash drift")
    return path


def _verify_self_hash(path: Path, *, field: str, expected: str, label: str) -> dict[str, Any]:
    payload = _read_json(path)
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if claimed != expected or stable_hash(body) != claimed:
        raise ValueError(f"{label} payload hash drift")
    return payload


def verify_authorization(path: Path, *, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = _read_json(path.resolve())
    body = dict(payload)
    claimed = str(body.pop("authorization_payload_sha256", ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError("mechanism successor authorization self-hash drift")
    if (
        payload.get("schema_version") != AUTHORIZATION_SCHEMA
        or payload.get("status") != "DISCLOSURE_TIMING_MECHANISM_SUCCESSOR_FROZEN_NOT_RUN"
        or payload.get("campaign_id") != CAMPAIGN_ID
        or payload.get("campaign_profile") != CAMPAIGN_PROFILE
        or payload.get("project_control_route_id") != ROUTE_ID
        or list(payload.get("permitted_project_control_actions") or ())
        != [ACTION_LAUNCH, ACTION_RETRY]
        or payload.get("evaluation_data_role") != "DEVELOPMENT_ONLY"
        or payload.get("execution_authorized") is not True
        or payload.get("validation_feedback_used") is not False
        or payload.get("oos_authority") != "NONE"
        or payload.get("promotion_authorized") is not False
        or payload.get("automatic_successor_authorized") is not False
    ):
        raise ValueError("mechanism successor authorization contract drift")

    prefreeze_binding = dict(payload["mechanism_prefreeze"])
    prefreeze_path = _bound_path(root, prefreeze_binding, label="mechanism prefreeze")
    prefreeze = _verify_self_hash(
        prefreeze_path,
        field="prefreeze_payload_sha256",
        expected=str(prefreeze_binding["payload_sha256"]),
        label="mechanism prefreeze",
    )
    if (
        prefreeze.get("status") != "PREFINANCIAL_MECHANISM_FALSIFICATION_DESIGN_FROZEN"
        or int(prefreeze_binding.get("stage_a_records") or 0) != 288
        or int(prefreeze_binding.get("stage_b_records") or 0) != 264
        or int(prefreeze_binding.get("combined_records") or 0) != 552
        or int(prefreeze_binding.get("field_column_count") or 0) != 42
        or prefreeze_binding.get("field_columns_sha256")
        != prefreeze["resource_preview"]["union_field_columns_sha256"]
        or prefreeze_binding.get("stage_b_candidate_set_frozen_before_stage_a") is not True
    ):
        raise ValueError("mechanism prefreeze binding drift")

    spent_binding = dict(payload["spent_exact_freeze"])
    spent_path = _bound_path(root, spent_binding, label="spent exact freeze")
    spent = _verify_self_hash(
        spent_path,
        field="freeze_payload_sha256",
        expected=str(spent_binding["payload_sha256"]),
        label="spent exact freeze",
    )
    if (
        spent.get("status") != "SPENT_EXACT_DENY_SET_FROZEN"
        or int(spent_binding.get("combined_spent_exact_count") or 0) != 2150
        or spent_binding.get("combined_spent_exact_identities_sha256")
        != spent.get("combined_spent_exact_identities_sha256")
    ):
        raise ValueError("spent exact freeze binding drift")

    audit_binding = dict(payload["systematicity_supply_audit"])
    audit_path = _bound_path(root, audit_binding, label="systematicity supply audit")
    audit = _verify_self_hash(
        audit_path,
        field="audit_payload_sha256",
        expected=str(audit_binding["payload_sha256"]),
        label="systematicity supply audit",
    )
    if audit.get("status") != "ZERO_FINANCIAL_SYSTEMATICITY_AND_SUPPLY_AUDIT_COMPLETE":
        raise ValueError("systematicity supply audit status drift")

    source = dict(payload["source_large_fresh_v2"])
    base_auth_path = (
        root / Path(str(source["authorization_relative_path"]))
    ).resolve()
    if (
        not base_auth_path.is_file()
        or sha256_file(base_auth_path) != str(source["authorization_file_sha256"])
    ):
        raise ValueError("source Large Fresh authorization file drift")
    base_auth = _read_json(base_auth_path)
    if base_auth.get("authorization_payload_sha256") != source.get(
        "authorization_payload_sha256"
    ):
        raise ValueError("source Large Fresh authorization payload drift")
    large_audit_path = (
        root / Path(str(source["independent_audit_relative_path"]))
    ).resolve()
    if (
        not large_audit_path.is_file()
        or sha256_file(large_audit_path) != str(source["independent_audit_file_sha256"])
    ):
        raise ValueError("source Large Fresh audit file drift")
    large_audit = _verify_self_hash(
        large_audit_path,
        field="audit_payload_sha256",
        expected=str(source["independent_audit_payload_sha256"]),
        label="source Large Fresh audit",
    )
    if large_audit.get("status") != "PASS_INDEPENDENT_TERMINAL_AUDIT":
        raise ValueError("source Large Fresh audit status drift")

    resource = dict(payload["resource_contract"])
    if (
        resource.get("profile") != "SEARCH_DUAL_24"
        or int(resource.get("cpu_threads") or 0) != 24
        or int(resource.get("executor_workers") or 0) != 24
        or resource.get("resource_canary_required_before_first_candidate_evaluation") is not True
        or float(resource.get("resource_canary_probe_seconds") or 0.0) != 30.0
        or int(resource.get("resource_canary_field_column_count") or 0) != 42
        or resource.get("resource_canary_field_columns_sha256")
        != prefreeze_binding.get("field_columns_sha256")
        or resource.get("candidate_evaluation_during_resource_canary") is not False
    ):
        raise ValueError("mechanism successor resource contract drift")
    source_resource_path = (
        root / Path(str(resource["source_53_field_resource_evidence_relative_path"]))
    ).resolve()
    if (
        not source_resource_path.is_file()
        or sha256_file(source_resource_path)
        != str(resource["source_53_field_resource_evidence_file_sha256"])
    ):
        raise ValueError("source resource evidence file drift")
    resource_payload = _read_json(source_resource_path)
    resource_body = dict(resource_payload)
    resource_claimed = str(resource_body.pop("resource_evidence_payload_sha256", ""))
    if (
        resource_claimed != resource.get("source_53_field_resource_evidence_payload_sha256")
        or stable_hash(resource_body) != resource_claimed
        or resource_payload.get("financial_candidate_evaluation_performed") is not False
    ):
        raise ValueError("source resource evidence payload drift")

    design = dict(payload["falsification_design"])
    if (
        design.get("optimizer_selection_authorized") is not False
        or design.get("dynamic_candidate_reallocation_authorized") is not False
        or design.get("stage_b_mutation_after_stage_a_authorized") is not False
        or design.get("classification") != prefreeze["gates"]["classification"]
    ):
        raise ValueError("mechanism falsification design drift")
    if any(int(value) != 0 for value in dict(payload["restricted_reads"]).values()):
        raise ValueError("mechanism restricted-read authorization drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(ROUTE_ID, {ACTION_LAUNCH, ACTION_RETRY})
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--mechanism-prefreeze", type=Path, required=True)
    parser.add_argument("--spent-exact-freeze", type=Path, required=True)
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

    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(admission, args.campaign_authorization)
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("mechanism successor authorization payload drift")
    repo_root = Path(__file__).resolve().parents[3]
    expected_prefreeze = (
        repo_root / authorization["mechanism_prefreeze"]["relative_path"]
    ).resolve()
    expected_spent = (
        repo_root / authorization["spent_exact_freeze"]["relative_path"]
    ).resolve()
    if args.mechanism_prefreeze.resolve() != expected_prefreeze:
        raise ProjectControlDenied("mechanism prefreeze path outside authorization")
    if args.spent_exact_freeze.resolve() != expected_spent:
        raise ProjectControlDenied("spent exact freeze path outside authorization")
    validate_node_resource_lease_receipt(
        args.node_resource_lease_receipt.resolve(),
        expected_role="SEARCH",
        expected_cpu_threads=24,
    )
    from scripts.run_cn_program_disclosure_timing_mechanism_successor_v1 import run

    result = run(args, admission=admission, authorization=authorization)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
