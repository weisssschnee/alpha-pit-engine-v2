"""Project-Control route for frozen Wave1 report-only validation."""
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

ROUTE_ID = "cn-search-core-v2-production-wave1-report-only-validation-v1"
CAMPAIGN_ID = "CN_SEARCH_CORE_V2_PRODUCTION_WAVE1_REPORT_ONLY_VALIDATION_V1"
CAMPAIGN_PROFILE = "cn_search_core_v2_production_wave1_report_only_validation_v1"
AUTHORIZATION_SCHEMA = "cn_search_core_v2_production_wave1_report_only_validation_authorization_v1"
AUTHORIZATION_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave1_report_only_validation_authorization_v1.json")
PREPARED_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_prepared_binding_20260825.json")
FREEZE_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_shortlist_freeze_20260825.json")
MEMBERS_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_shortlist_members_20260825.jsonl")
RESOLUTION_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_schedule_resolution_20260825.json")
SCHEDULES_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_resolved_schedules_20260825.jsonl")
CANDIDATE_COUNT = 42
EVALUATOR_WORKERS = 4


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify_self(path: Path, field: str, label: str) -> dict[str, Any]:
    payload = _read(path)
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError(f"{label} self-hash drift")
    return payload


def _bound_file(root: Path, binding: Mapping[str, Any], *, label: str) -> Path:
    path = (root / Path(str(binding["relative_path"]))).resolve()
    if not path.is_relative_to(root) or not path.is_file() or sha256_file(path) != str(binding["file_sha256"]):
        raise ValueError(f"{label} file drift")
    return path


def verify_authorization(path: Path, *, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = _verify_self(path.resolve(), "authorization_payload_sha256", "Wave1 report-only validation authorization")
    if (
        payload.get("schema_version") != AUTHORIZATION_SCHEMA
        or payload.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_REPORT_ONLY_VALIDATION_AUTHORIZED_NOT_RUN"
        or payload.get("execution_authorized") is not True
        or payload.get("campaign_id") != CAMPAIGN_ID
        or payload.get("campaign_profile") != CAMPAIGN_PROFILE
        or payload.get("project_control_route_id") != ROUTE_ID
        or list(payload.get("permitted_project_control_actions") or ()) != [ACTION_LAUNCH, ACTION_RETRY]
        or payload.get("evaluation_role") != "validation"
        or payload.get("usage") != "REPORT_ONLY_FROZEN_WAVE1_SHORTLIST"
        or int(payload.get("candidate_count") or 0) != CANDIDATE_COUNT
        or payload.get("candidate_generation_authorized") is not False
        or payload.get("threshold_tuning_allowed") is not False
        or payload.get("same_slice_reselection_allowed") is not False
        or payload.get("optimizer_feedback_write") != "FORBIDDEN"
        or payload.get("policy_memory_write") != "FORBIDDEN"
        or payload.get("scheduler_write") != "FORBIDDEN"
        or payload.get("archive_write") != "FORBIDDEN"
        or payload.get("promotion_authorized") is not False
        or payload.get("automatic_followon_authorized") is not False
        or payload.get("oos_authority") != "VALIDATION_REPORT_ONLY_EVIDENCE_ONLY"
    ):
        raise ValueError("Wave1 report-only validation authorization contract drift")
    if any(int(payload.get(key) or 0) != 0 for key in ("holdout_reads", "forward_b_reads", "forward_2026_reads")):
        raise ValueError("Wave1 report-only validation authority boundary drift")

    prepared_path = _bound_file(root, payload["prepared_binding"], label="Wave1 validation prepared binding")
    prepared = _verify_self(prepared_path, "prepared_binding_payload_sha256", "Wave1 validation prepared binding")
    if (
        prepared.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_PREFINANCIAL_READY"
        or int(prepared.get("candidate_count") or 0) != CANDIDATE_COUNT
        or prepared.get("candidate_evaluation_executed") is not False
        or prepared.get("candidate_results_generated") is not False
        or int(prepared.get("holdout_reads") or 0) != 0
        or int(prepared.get("forward_reads") or 0) != 0
        or str(prepared["prepared_binding_payload_sha256"]) != str(payload["prepared_binding"]["payload_sha256"])
    ):
        raise ValueError("Wave1 report-only validation prepared binding drift")

    freeze_path = _bound_file(root, payload["shortlist_freeze"], label="Wave1 validation shortlist freeze")
    freeze = _verify_self(freeze_path, "freeze_payload_sha256", "Wave1 validation shortlist freeze")
    members_path = _bound_file(root, payload["shortlist_members"], label="Wave1 validation shortlist members")
    resolution_path = _bound_file(root, payload["schedule_resolution"], label="Wave1 validation schedule resolution")
    resolution = _verify_self(resolution_path, "resolution_payload_sha256", "Wave1 validation schedule resolution")
    schedules_path = _bound_file(root, payload["resolved_schedules"], label="Wave1 validation resolved schedules")
    if (
        int(freeze.get("candidate_count") or 0) != CANDIDATE_COUNT
        or freeze.get("membership_frozen_before_validation") is not True
        or freeze.get("same_slice_reselection_allowed") is not False
        or freeze.get("threshold_tuning_allowed") is not False
        or str(freeze["candidate_exact_identities_sha256"]) != str(payload["candidate_exact_identities_sha256"])
        or int(resolution.get("candidate_count") or 0) != CANDIDATE_COUNT
        or str(resolution.get("resolution_payload_sha256") or "") != str(payload["schedule_resolution"]["payload_sha256"])
        or sha256_file(members_path) != str(payload["shortlist_members"]["file_sha256"])
        or sha256_file(schedules_path) != str(prepared["resolved_schedule_file_sha256"])
    ):
        raise ValueError("Wave1 report-only validation frozen membership/schedule drift")

    source_contract = Path(str(payload["source_contract"]["path"])).resolve()
    registry = Path(str(payload["registry"]["path"])).resolve()
    if (
        not source_contract.is_file()
        or sha256_file(source_contract) != str(payload["source_contract"]["sha256"])
        or not registry.is_file()
        or sha256_file(registry) != str(payload["registry"]["sha256"])
    ):
        raise ValueError("Wave1 report-only validation source file drift")

    impl = dict(payload["implementation"])
    runner = root / "scripts/run_cn_search_core_v2_production_wave1_report_only_validation_v1.py"
    runtime = Path(__file__).resolve()
    if sha256_file(runner) != str(impl["runner_source_file_sha256"]) or sha256_file(runtime) != str(impl["runtime_source_file_sha256"]):
        raise ValueError("Wave1 report-only validation implementation drift")
    if dict(payload["resource_contract"]) != {
        "profile": "VALIDATION_DUAL_8",
        "cpu_threads": 8,
        "evaluator_workers": EVALUATOR_WORKERS,
        "candidate_count": CANDIDATE_COUNT,
    }:
        raise ValueError("Wave1 report-only validation resource contract drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(ROUTE_ID, {ACTION_LAUNCH, ACTION_RETRY})
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--prepared-binding", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-lease-receipt", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=EVALUATOR_WORKERS)
    args = parser.parse_args(argv)
    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(admission, args.campaign_authorization)
    root = Path(__file__).resolve().parents[3]
    authorization = verify_authorization(verified.path, repo_root=root)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("Wave1 report-only validation authorization payload drift")
    if args.prepared_binding.resolve() != (root / PREPARED_RELATIVE_PATH).resolve():
        raise ProjectControlDenied("Wave1 report-only validation prepared path outside authorization")
    if args.source_contract.resolve() != Path(str(authorization["source_contract"]["path"])).resolve():
        raise ProjectControlDenied("Wave1 report-only validation source contract path drift")
    if args.registry.resolve() != Path(str(authorization["registry"]["path"])).resolve():
        raise ProjectControlDenied("Wave1 report-only validation registry path drift")
    if int(args.workers) != EVALUATOR_WORKERS:
        raise ProjectControlDenied("Wave1 report-only validation worker count drift")
    validate_node_resource_lease_receipt(
        args.node_resource_lease_receipt.resolve(),
        expected_role="VALIDATION",
        expected_cpu_threads=8,
    )
    from scripts.run_cn_search_core_v2_production_wave1_report_only_validation_v1 import run

    result = run(
        argparse.Namespace(
            repo_root=root,
            prepared_binding=args.prepared_binding,
            source_contract=args.source_contract,
            registry=args.registry,
            output_root=args.output_root,
            workers=EVALUATOR_WORKERS,
        ),
        admission=admission,
        authorization=authorization,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
