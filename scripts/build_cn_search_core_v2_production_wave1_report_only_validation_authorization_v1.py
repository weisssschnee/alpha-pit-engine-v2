"""Build execution authorization for frozen Wave1 report-only validation."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.runtime import cn_search_core_v2_production_wave1_report_only_validation_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH, ACTION_RETRY, sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash

PREP_AUTH = Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_prep_authorization_v1.json")
PREPARED = runtime.PREPARED_RELATIVE_PATH
FREEZE = runtime.FREEZE_RELATIVE_PATH
MEMBERS = runtime.MEMBERS_RELATIVE_PATH
RESOLUTION = runtime.RESOLUTION_RELATIVE_PATH
SCHEDULES = runtime.SCHEDULES_RELATIVE_PATH


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _binding(repo: Path, relative: Path, *, field: str | None = None) -> dict[str, Any]:
    path = repo / relative
    row: dict[str, Any] = {
        "relative_path": str(relative).replace("\\", "/"),
        "file_sha256": sha256_file(path),
    }
    if field:
        payload = _read(path)
        row["payload_sha256"] = _verify(payload, field, str(relative))
    return row


def build(repo: Path, *, source_repo_sha: str) -> dict[str, Any]:
    repo = repo.resolve()
    prep_auth = _read(repo / PREP_AUTH)
    prep_auth_hash = _verify(prep_auth, "authorization_payload_sha256", "Wave1 validation prep authorization")
    prepared_path = repo / PREPARED
    prepared = _read(prepared_path)
    prepared_hash = _verify(prepared, "prepared_binding_payload_sha256", "Wave1 validation prepared binding")
    freeze = _read(repo / FREEZE)
    freeze_hash = _verify(freeze, "freeze_payload_sha256", "Wave1 validation shortlist freeze")
    resolution = _read(repo / RESOLUTION)
    resolution_hash = _verify(resolution, "resolution_payload_sha256", "Wave1 validation schedule resolution")
    if (
        prep_auth.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_PREP_AUTHORIZED_NOT_RUN"
        or prep_auth.get("candidate_evaluation_authorized") is not False
        or prep_auth.get("threshold_tuning_allowed") is not False
        or prep_auth.get("same_slice_reselection_allowed") is not False
        or prepared.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_PREFINANCIAL_READY"
        or prepared.get("candidate_evaluation_executed") is not False
        or int(prepared.get("candidate_count") or 0) != runtime.CANDIDATE_COUNT
        or freeze.get("membership_frozen_before_validation") is not True
        or int(freeze.get("candidate_count") or 0) != runtime.CANDIDATE_COUNT
        or int(resolution.get("candidate_count") or 0) != runtime.CANDIDATE_COUNT
        or str(prepared["candidate_exact_identities_sha256"]) != str(freeze["candidate_exact_identities_sha256"])
        or str(resolution["candidate_exact_identities_sha256"]) != str(freeze["candidate_exact_identities_sha256"])
        or sha256_file(repo / MEMBERS) != str(prep_auth["shortlist_freeze"]["members_file_sha256"])
        or sha256_file(repo / SCHEDULES) != str(prep_auth["schedule_resolution"]["schedules_file_sha256"])
        or str(prepared["resolved_schedule_file_sha256"]) != sha256_file(repo / SCHEDULES)
    ):
        raise RuntimeError("Wave1 report-only validation frozen authority drift")
    source = dict(prep_auth["source_validation_authority_plan"]["source_data"])
    source_contract = dict(source["source_contract"])
    registry = dict(source["registry"])
    runner = repo / "scripts/run_cn_search_core_v2_production_wave1_report_only_validation_v1.py"
    runtime_path = repo / "src/our_system_phase2/runtime/cn_search_core_v2_production_wave1_report_only_validation_v1.py"
    payload = {
        "schema_version": runtime.AUTHORIZATION_SCHEMA,
        "status": "SEARCH_CORE_V2_PRODUCTION_WAVE1_REPORT_ONLY_VALIDATION_AUTHORIZED_NOT_RUN",
        "source_repo_sha": str(source_repo_sha),
        "execution_authorized": True,
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "project_control_route_id": runtime.ROUTE_ID,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "evaluation_role": "validation",
        "usage": "REPORT_ONLY_FROZEN_WAVE1_SHORTLIST",
        "candidate_count": runtime.CANDIDATE_COUNT,
        "candidate_exact_identities_sha256": str(freeze["candidate_exact_identities_sha256"]),
        "candidate_generation_authorized": False,
        "prepared_binding": _binding(repo, PREPARED, field="prepared_binding_payload_sha256"),
        "shortlist_freeze": _binding(repo, FREEZE, field="freeze_payload_sha256"),
        "shortlist_members": _binding(repo, MEMBERS),
        "schedule_resolution": _binding(repo, RESOLUTION, field="resolution_payload_sha256"),
        "resolved_schedules": _binding(repo, SCHEDULES),
        "prep_authorization_payload_sha256": prep_auth_hash,
        "prepared_binding_payload_sha256": prepared_hash,
        "shortlist_freeze_payload_sha256": freeze_hash,
        "schedule_resolution_payload_sha256": resolution_hash,
        "source_contract": {"path": str(source_contract["path"]), "sha256": str(source_contract["sha256"])},
        "registry": {"path": str(registry["path"]), "sha256": str(registry["sha256"])},
        "validation_windows": list(prepared["validation_windows"]),
        "implementation": {
            "runner_source_file_sha256": sha256_file(runner),
            "runtime_source_file_sha256": sha256_file(runtime_path),
        },
        "resource_contract": {
            "profile": "VALIDATION_DUAL_8",
            "cpu_threads": 8,
            "evaluator_workers": runtime.EVALUATOR_WORKERS,
            "candidate_count": runtime.CANDIDATE_COUNT,
        },
        "threshold_tuning_allowed": False,
        "same_slice_reselection_allowed": False,
        "optimizer_feedback_write": "FORBIDDEN",
        "policy_memory_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion_authorized": False,
        "automatic_followon_authorized": False,
        "holdout_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
        "oos_authority": "VALIDATION_REPORT_ONLY_EVIDENCE_ONLY",
    }
    payload["authorization_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source-repo-sha", required=True)
    parser.add_argument("--output", type=Path, default=runtime.AUTHORIZATION_RELATIVE_PATH)
    args = parser.parse_args(argv)
    payload = build(args.repo_root, source_repo_sha=args.source_repo_sha)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "payload": payload["authorization_payload_sha256"], "candidates": payload["candidate_count"], "output": str(output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
