"""Project-Control entry for frozen D1 report-only validation.

Candidate selection is frozen before validation access.  This route may read only
the prepared validation authority and emits report-only transfer evidence; it
has no optimizer feedback, holdout, Forward-2026, or promotion authority.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

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


ROUTE_ID = "cn-program-optimizer-d1-report-only-validation-v1"
CAMPAIGN_ID = "CN_PROGRAM_OPTIMIZER_D1_REPORT_ONLY_VALIDATION_V1"
CAMPAIGN_PROFILE = "cn_program_optimizer_d1_report_only_validation_v1"
AUTHORIZATION_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_d1_report_only_validation_v1.json"
)
PREPARED_BINDING_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_d1_validation_prefinancial_20260816.json"
)
CANDIDATE_FREEZE_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_d1_validation_candidate_freeze_20260816.json"
)
CANDIDATE_MEMBERS_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_d1_validation_candidate_members_20260816.jsonl"
)
EXPECTED_CANDIDATE_COUNT = 120
EXPECTED_CANDIDATE_EXACT_SHA256 = (
    "ec419271b7bc2a3fb5d039c6e1441c489961601fbd788ccd84c2d80231cc3687"
)
EXPECTED_CANDIDATE_FREEZE_PAYLOAD_SHA256 = (
    "ddde0c64667bef8a12e6261dab8a3998e5591085bf0fb1f0851fd0cfa0f85a8a"
)
EXPECTED_CANDIDATE_MEMBERS_FILE_SHA256 = (
    "4f0befede84762a81b1a458178c7e9a294821c3cb8a61f512e7a3bf990ef19da"
)
VALIDATION_WINDOWS = (
    {"window_id": "validation_1", "start_date": "2025-07-08", "end_date": "2025-08-08", "session_count": 24},
    {"window_id": "validation_2", "start_date": "2025-08-11", "end_date": "2025-09-11", "session_count": 24},
    {"window_id": "validation_3", "start_date": "2025-09-12", "end_date": "2025-10-24", "session_count": 25},
)


def _read_self_hashed(path: Path, field: str, label: str) -> dict[str, Any]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError(f"{label} self-hash drift")
    return payload


def verify_authorization(path: Path, *, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = _read_self_hashed(path, "authorization_payload_sha256", "D1 validation authorization")
    if (
        str(payload.get("schema_version")) != "cn_program_optimizer_d1_report_only_validation_authorization_v1"
        or str(payload.get("status")) != "D1_REPORT_ONLY_VALIDATION_FROZEN_READY"
        or str(payload.get("campaign_id")) != CAMPAIGN_ID
        or str(payload.get("campaign_profile")) != CAMPAIGN_PROFILE
        or str(payload.get("project_control_route_id")) != ROUTE_ID
        or not bool(payload.get("execution_authorized"))
        or list(payload.get("permitted_project_control_actions") or ()) != [ACTION_LAUNCH, ACTION_RETRY]
        or str(payload.get("evaluation_role")) != "validation"
        or str(payload.get("usage")) != "REPORT_ONLY_CANDIDATE_TRANSFER"
        or bool(payload.get("promotion_authorized"))
        or str(payload.get("optimizer_feedback_write")) != "FORBIDDEN"
        or str(payload.get("scheduler_write")) != "FORBIDDEN"
        or str(payload.get("archive_write")) != "FORBIDDEN"
        or int(payload.get("holdout_reads") or 0) != 0
        or int(payload.get("forward_2026_reads") or 0) != 0
        or list(payload.get("validation_windows") or ()) != list(VALIDATION_WINDOWS)
    ):
        raise ValueError("D1 validation authorization contract drift")
    candidate = dict(payload.get("candidate_freeze") or {})
    if (
        int(candidate.get("candidate_count") or 0) != EXPECTED_CANDIDATE_COUNT
        or str(candidate.get("candidate_exact_identities_sha256") or "")
        != EXPECTED_CANDIDATE_EXACT_SHA256
        or str(candidate.get("freeze_payload_sha256") or "")
        != EXPECTED_CANDIDATE_FREEZE_PAYLOAD_SHA256
        or str(candidate.get("members_file_sha256") or "")
        != EXPECTED_CANDIDATE_MEMBERS_FILE_SHA256
    ):
        raise ValueError("D1 validation authorization candidate freeze drift")
    freeze_path = (root / CANDIDATE_FREEZE_RELATIVE_PATH).resolve()
    freeze = _read_self_hashed(freeze_path, "freeze_payload_sha256", "D1 validation candidate freeze")
    if str(freeze["freeze_payload_sha256"]) != EXPECTED_CANDIDATE_FREEZE_PAYLOAD_SHA256:
        raise ValueError("D1 validation candidate freeze payload drift")
    members_path = (root / CANDIDATE_MEMBERS_RELATIVE_PATH).resolve()
    if sha256_file(members_path) != EXPECTED_CANDIDATE_MEMBERS_FILE_SHA256:
        raise ValueError("D1 validation member file drift")
    prepared = dict(payload.get("prepared_binding") or {})
    prepared_path = (root / PREPARED_BINDING_RELATIVE_PATH).resolve()
    if (
        str(prepared.get("relative_path") or "").replace("\\", "/")
        != str(PREPARED_BINDING_RELATIVE_PATH).replace("\\", "/")
        or sha256_file(prepared_path) != str(prepared.get("file_sha256") or "")
    ):
        raise ValueError("D1 validation prepared binding file drift")
    prepared_payload = _read_self_hashed(
        prepared_path,
        "prepared_binding_payload_sha256",
        "D1 validation prepared binding",
    )
    if (
        str(prepared_payload.get("prepared_binding_payload_sha256") or "")
        != str(prepared.get("payload_sha256") or "")
        or str(prepared_payload.get("status")) != "D1_VALIDATION_PREFINANCIAL_READY"
        or int(prepared_payload.get("candidate_count") or 0) != EXPECTED_CANDIDATE_COUNT
        or str(prepared_payload.get("candidate_exact_identities_sha256") or "")
        != EXPECTED_CANDIDATE_EXACT_SHA256
        or bool(prepared_payload.get("candidate_evaluation_executed"))
        or int(prepared_payload.get("holdout_reads") or 0) != 0
        or int(prepared_payload.get("forward_2026_reads") or 0) != 0
    ):
        raise ValueError("D1 validation prepared binding semantic drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(
        "cn-program-optimizer-d1-report-only-validation-v1",
        {ACTION_LAUNCH, ACTION_RETRY},
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)

    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(admission, args.campaign_authorization)
    repo_root = Path(__file__).resolve().parents[3]
    authorization = verify_authorization(verified.path, repo_root=repo_root)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("D1 validation campaign authorization payload drift")
    if str(admission.get("requested_action") or "") not in {ACTION_LAUNCH, ACTION_RETRY}:
        raise ProjectControlDenied("D1 validation requested action drift")
    source = dict(authorization.get("source_binding") or {})
    if sha256_file(args.source_contract.resolve()) != str(source.get("source_contract_sha256") or ""):
        raise ProjectControlDenied("D1 validation source contract drift")
    if sha256_file(args.registry.resolve()) != str(source.get("registry_sha256") or ""):
        raise ProjectControlDenied("D1 validation registry drift")

    from scripts.run_cn_program_optimizer_d1_report_only_validation_v1 import run

    prepared_path = (repo_root / PREPARED_BINDING_RELATIVE_PATH).resolve()
    result = run(
        argparse.Namespace(
            repo_root=repo_root,
            prepared_binding=prepared_path,
            source_contract=args.source_contract,
            registry=args.registry,
            output_root=args.output_root,
            workers=args.workers,
        )
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
