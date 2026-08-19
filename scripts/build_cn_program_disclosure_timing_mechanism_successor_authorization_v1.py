"""Freeze authorization for the disclosure-timing mechanism falsification successor."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.project_control_admission import ACTION_LAUNCH, ACTION_RETRY
from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA = "cn_program_disclosure_timing_mechanism_successor_authorization_v1"
ROUTE_ID = "cn-program-disclosure-timing-mechanism-successor-v1"
CAMPAIGN_ID = "CN_PROGRAM_DISCLOSURE_TIMING_MECHANISM_SUCCESSOR_V1"
CAMPAIGN_PROFILE = "cn_program_disclosure_timing_mechanism_successor_v1"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError(f"{label} self-hash drift")
    return claimed


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    base_path = repo / "runtime/run_plans/cn_program_optimizer_large_fresh_development_v2.json"
    prefreeze_path = repo / "runtime/run_plans/cn_disclosure_timing_mechanism_successor_prefreeze_20260819.json"
    spent_path = repo / "runtime/run_plans/cn_disclosure_timing_mechanism_spent_exact_freeze_20260819.json"
    audit_path = repo / "runtime/run_plans/cn_large_fresh_systematicity_supply_audit_20260819.json"
    large_audit_path = repo / "runtime/run_plans/cn_program_optimizer_large_fresh_v2_retry_5b58c29_independent_audit_20260819.json"
    base = _read(base_path)
    prefreeze = _read(prefreeze_path)
    spent = _read(spent_path)
    audit = _read(audit_path)
    large_audit = _read(large_audit_path)
    prefreeze_hash = _verify_self_hash(prefreeze, "prefreeze_payload_sha256", "prefreeze")
    spent_hash = _verify_self_hash(spent, "freeze_payload_sha256", "spent freeze")
    audit_hash = _verify_self_hash(audit, "audit_payload_sha256", "systematicity audit")
    large_audit_hash = _verify_self_hash(large_audit, "audit_payload_sha256", "large fresh audit")
    if base.get("authorization_payload_sha256") != "7988660dd053e9cc5429893acd878454815b907a1b259c9295de3e1dfcf57774":
        raise ValueError("source Large Fresh authorization drift")
    if large_audit.get("status") != "PASS_INDEPENDENT_TERMINAL_AUDIT":
        raise ValueError("source Large Fresh independent audit not PASS")
    if prefreeze.get("status") != "PREFINANCIAL_MECHANISM_FALSIFICATION_DESIGN_FROZEN":
        raise ValueError("mechanism prefreeze not frozen")
    if spent.get("status") != "SPENT_EXACT_DENY_SET_FROZEN":
        raise ValueError("mechanism spent freeze not frozen")
    if audit.get("status") != "ZERO_FINANCIAL_SYSTEMATICITY_AND_SUPPLY_AUDIT_COMPLETE":
        raise ValueError("systematicity supply audit not complete")
    program_space = dict(base["program_space"])
    resource = dict(base["resource_evidence"])
    payload: dict[str, Any] = {
        "schema_version": SCHEMA,
        "status": "DISCLOSURE_TIMING_MECHANISM_SUCCESSOR_FROZEN_NOT_RUN",
        "execution_authorized": True,
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "project_control_route_id": ROUTE_ID,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "source_large_fresh_v2": {
            "authorization_relative_path": "runtime/run_plans/cn_program_optimizer_large_fresh_development_v2.json",
            "authorization_file_sha256": _sha(base_path),
            "authorization_payload_sha256": base["authorization_payload_sha256"],
            "independent_audit_relative_path": "runtime/run_plans/cn_program_optimizer_large_fresh_v2_retry_5b58c29_independent_audit_20260819.json",
            "independent_audit_file_sha256": _sha(large_audit_path),
            "independent_audit_payload_sha256": large_audit_hash,
        },
        "source_prior_exact": {
            "relative_path": str(program_space["prior_freeze_relative_path"]),
            "payload_sha256": str(program_space["prior_freeze_payload_sha256"]),
            "count": int(program_space["prior_exact_count"]),
            "exact_identities_sha256": str(program_space["prior_exact_identities_sha256"]),
            "identity_field": "combined_prior_exact_identities",
        },
        "mechanism_prefreeze": {
            "relative_path": "runtime/run_plans/cn_disclosure_timing_mechanism_successor_prefreeze_20260819.json",
            "file_sha256": _sha(prefreeze_path),
            "payload_sha256": prefreeze_hash,
            "stage_a_records": 288,
            "stage_b_records": 264,
            "combined_records": 552,
            "field_column_count": int(prefreeze["resource_preview"]["union_field_count"]),
            "field_columns_sha256": str(prefreeze["resource_preview"]["union_field_columns_sha256"]),
            "stage_b_candidate_set_frozen_before_stage_a": True,
        },
        "spent_exact_freeze": {
            "relative_path": "runtime/run_plans/cn_disclosure_timing_mechanism_spent_exact_freeze_20260819.json",
            "file_sha256": _sha(spent_path),
            "payload_sha256": spent_hash,
            "combined_spent_exact_count": int(spent["combined_spent_exact_count"]),
            "combined_spent_exact_identities_sha256": str(spent["combined_spent_exact_identities_sha256"]),
        },
        "systematicity_supply_audit": {
            "relative_path": "runtime/run_plans/cn_large_fresh_systematicity_supply_audit_20260819.json",
            "file_sha256": _sha(audit_path),
            "payload_sha256": audit_hash,
        },
        "resource_contract": {
            "profile": "SEARCH_DUAL_24",
            "cpu_threads": 24,
            "executor_workers": 24,
            "resource_canary_required_before_first_candidate_evaluation": True,
            "resource_canary_probe_seconds": 30.0,
            "resource_canary_field_column_count": int(prefreeze["resource_preview"]["union_field_count"]),
            "resource_canary_field_columns_sha256": str(prefreeze["resource_preview"]["union_field_columns_sha256"]),
            "source_53_field_resource_evidence_relative_path": str(resource["relative_path"]),
            "source_53_field_resource_evidence_file_sha256": str(resource["file_sha256"]),
            "source_53_field_resource_evidence_payload_sha256": str(resource["payload_sha256"]),
            "candidate_evaluation_during_resource_canary": False,
        },
        "falsification_design": {
            "optimizer_selection_authorized": False,
            "dynamic_candidate_reallocation_authorized": False,
            "stage_b_mutation_after_stage_a_authorized": False,
            "stage_a_gate": prefreeze["gates"]["stage_a_systematic_event_family_gate"],
            "stage_a_anchor_gate": prefreeze["gates"]["stage_a_anchor_gate"],
            "stage_b_gate": prefreeze["gates"]["stage_b_joint_transfer_gate"],
            "classification": prefreeze["gates"]["classification"],
        },
        "source_evaluator_authority": dict(base["source_evaluator_authority"]),
        "restricted_reads": {
            "validation": 0,
            "holdout": 0,
            "historical_2023": 0,
            "forward_b": 0,
            "forward_2026": 0,
        },
        "validation_feedback_used": False,
        "oos_authority": "NONE",
        "promotion_authorized": False,
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
        default=Path("runtime/run_plans/cn_program_disclosure_timing_mechanism_successor_v1.json"),
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
