from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.project_control_admission import ACTION_LAUNCH, ACTION_RETRY
from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA = "cn_program_primitive_local_stage_b_authorization_v1"
ROUTE_ID = "cn-program-primitive-local-stage-b-benchmark-v1"
CAMPAIGN_ID = "CN_PROGRAM_PRIMITIVE_LOCAL_STAGE_B_BENCHMARK_V1"
CAMPAIGN_PROFILE = "cn_program_primitive_local_stage_b_benchmark_v1"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError(f"{label} self-hash drift")
    return claimed


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    source_auth_path = repo / "runtime/run_plans/cn_program_disclosure_timing_mechanism_successor_v1.json"
    policy_path = repo / "runtime/run_plans/cn_program_primitive_local_stage_b_policy_v1.json"
    prefreeze_path = repo / "runtime/run_plans/cn_disclosure_timing_mechanism_successor_prefreeze_20260819.json"
    audit_path = repo / "runtime/run_plans/cn_program_disclosure_timing_mechanism_successor_retry3_cbf191f_independent_audit_20260819.json"
    source = _read(source_auth_path)
    policy = _read(policy_path)
    prefreeze = _read(prefreeze_path)
    audit = _read(audit_path)
    source_hash = _verify(source, "authorization_payload_sha256", "source mechanism authorization")
    policy_hash = _verify(policy, "policy_payload_sha256", "Stage B search policy")
    prefreeze_hash = _verify(prefreeze, "prefreeze_payload_sha256", "mechanism prefreeze")
    audit_hash = _verify(audit, "audit_payload_sha256", "Stage A audit")
    if source.get("campaign_id") != "CN_PROGRAM_DISCLOSURE_TIMING_MECHANISM_SUCCESSOR_V1":
        raise ValueError("source mechanism authorization drift")
    if policy.get("status") != "PRIMITIVE_LOCAL_STAGE_B_POLICY_FROZEN_BEFORE_STAGE_B_READ":
        raise ValueError("Stage B policy not frozen")
    if audit.get("stage_b_never_started") is not True:
        raise ValueError("Stage B already started in source campaign")
    spent = dict(policy["spent_freeze"])
    resource_source = dict(source["resource_contract"])
    payload: dict[str, Any] = {
        "schema_version": SCHEMA,
        "status": "PRIMITIVE_LOCAL_STAGE_B_BENCHMARK_AUTHORIZED_NOT_RUN",
        "execution_authorized": True,
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "project_control_route_id": ROUTE_ID,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "source_mechanism_authorization": {
            "relative_path": "runtime/run_plans/cn_program_disclosure_timing_mechanism_successor_v1.json",
            "file_sha256": _sha(source_auth_path),
            "payload_sha256": source_hash,
        },
        "source_stage_a_audit": {
            "relative_path": "runtime/run_plans/cn_program_disclosure_timing_mechanism_successor_retry3_cbf191f_independent_audit_20260819.json",
            "file_sha256": _sha(audit_path),
            "payload_sha256": audit_hash,
            "classification": str(audit["classification"]),
            "stage_b_never_started": True,
        },
        "search_policy": {
            "relative_path": "runtime/run_plans/cn_program_primitive_local_stage_b_policy_v1.json",
            "file_sha256": _sha(policy_path),
            "payload_sha256": policy_hash,
            "stage_b_labels_read_during_policy_freeze": False,
            "primary_budget": int(policy["evaluation"]["primary_total_budget"]),
        },
        "mechanism_prefreeze": {
            "relative_path": "runtime/run_plans/cn_disclosure_timing_mechanism_successor_prefreeze_20260819.json",
            "file_sha256": _sha(prefreeze_path),
            "payload_sha256": prefreeze_hash,
            "stage_b_records": 264,
            "field_column_count": int(prefreeze["resource_preview"]["union_field_count"]),
            "field_columns_sha256": str(prefreeze["resource_preview"]["union_field_columns_sha256"]),
        },
        "source_prior_exact": dict(source["source_prior_exact"]),
        "spent_contract": {
            "combined_spent_exact_count": int(spent["combined_spent_exact_count"]),
            "combined_spent_exact_identities_sha256": str(spent["combined_spent_exact_identities_sha256"]),
            "stage_b_overlap_count": int(spent["stage_b_overlap_count"]),
        },
        "resource_contract": {
            "profile": "SEARCH_DUAL_24",
            "cpu_threads": 24,
            "executor_workers": 24,
            "resource_canary_required_before_first_candidate_evaluation": True,
            "resource_canary_probe_seconds": 30.0,
            "resource_canary_field_column_count": int(prefreeze["resource_preview"]["union_field_count"]),
            "resource_canary_field_columns_sha256": str(prefreeze["resource_preview"]["union_field_columns_sha256"]),
            "source_53_field_resource_evidence_relative_path": str(resource_source["source_53_field_resource_evidence_relative_path"]),
            "source_53_field_resource_evidence_file_sha256": str(resource_source["source_53_field_resource_evidence_file_sha256"]),
            "source_53_field_resource_evidence_payload_sha256": str(resource_source["source_53_field_resource_evidence_payload_sha256"]),
            "candidate_evaluation_during_resource_canary": False,
        },
        "benchmark_contract": {
            "physical_stage_b_evaluations": 264,
            "physical_evaluation_order_is_not_search_policy_order": True,
            "search_policy_orders_frozen_before_stage_b_labels": True,
            "all_policy_curves_are_counterfactual_reveal_orders_over_same_frozen_264_physical_results": True,
            "primitive_local_policy_id": "PRIMITIVE_LOCAL_BETA_TRANSFER_V1",
            "uniform_policy_id": "UNIFORM_HASH_V1",
            "system_search_victory_gate": dict(policy["evaluation"]["primary_system_search_victory_gate"]),
            "stage_c_execution_authorized_by_this_campaign": False,
        },
        "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
        "validation_feedback_used": False,
        "promotion_authorized": False,
        "oos_authority": "NONE",
        "automatic_successor_authorized": False,
    }
    payload["authorization_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_program_primitive_local_stage_b_authorization_v1.json"),
    )
    args = parser.parse_args(argv)
    payload = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "authorization_payload_sha256": payload["authorization_payload_sha256"], "output": str(output.resolve())}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
