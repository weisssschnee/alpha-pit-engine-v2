"""Freeze report-only validation authorization from a completed D1 validation prep."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from our_system_phase2.runtime.cn_program_optimizer_d1_report_only_validation_v1 import (
    AUTHORIZATION_RELATIVE_PATH,
    CAMPAIGN_ID,
    CAMPAIGN_PROFILE,
    CANDIDATE_FREEZE_RELATIVE_PATH,
    CANDIDATE_MEMBERS_RELATIVE_PATH,
    EXPECTED_CANDIDATE_COUNT,
    EXPECTED_CANDIDATE_EXACT_SHA256,
    EXPECTED_CANDIDATE_FREEZE_PAYLOAD_SHA256,
    EXPECTED_CANDIDATE_MEMBERS_FILE_SHA256,
    PREPARED_BINDING_RELATIVE_PATH,
    ROUTE_ID,
    VALIDATION_WINDOWS,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    sha256_file,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


def _read_self_hashed(path: Path, field: str, label: str) -> dict[str, Any]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError(f"{label} self-hash drift")
    return payload


def build(
    *,
    repo_root: Path,
    source_contract: Path,
    registry: Path,
) -> dict[str, Any]:
    root = repo_root.resolve()
    freeze_path = (root / CANDIDATE_FREEZE_RELATIVE_PATH).resolve()
    members_path = (root / CANDIDATE_MEMBERS_RELATIVE_PATH).resolve()
    prepared_path = (root / PREPARED_BINDING_RELATIVE_PATH).resolve()
    freeze = _read_self_hashed(
        freeze_path, "freeze_payload_sha256", "D1 validation candidate freeze"
    )
    prepared = _read_self_hashed(
        prepared_path,
        "prepared_binding_payload_sha256",
        "D1 validation prepared binding",
    )
    if (
        str(freeze["freeze_payload_sha256"])
        != EXPECTED_CANDIDATE_FREEZE_PAYLOAD_SHA256
        or int(freeze["frozen_candidate_count"]) != EXPECTED_CANDIDATE_COUNT
        or str(freeze["frozen_candidate_exact_identities_sha256"])
        != EXPECTED_CANDIDATE_EXACT_SHA256
        or sha256_file(members_path) != EXPECTED_CANDIDATE_MEMBERS_FILE_SHA256
    ):
        raise ValueError("D1 validation candidate freeze contract drift")
    if (
        str(prepared.get("status")) != "D1_VALIDATION_PREFINANCIAL_READY"
        or int(prepared.get("candidate_count") or 0) != EXPECTED_CANDIDATE_COUNT
        or str(prepared.get("candidate_exact_identities_sha256") or "")
        != EXPECTED_CANDIDATE_EXACT_SHA256
        or bool(prepared.get("candidate_evaluation_executed"))
        or list(prepared.get("validation_windows") or ()) != list(VALIDATION_WINDOWS)
        or int(prepared.get("holdout_reads") or 0) != 0
        or int(prepared.get("forward_2026_reads") or 0) != 0
        or str(prepared.get("optimizer_feedback_write")) != "FORBIDDEN"
        or str(prepared.get("scheduler_write")) != "FORBIDDEN"
        or str(prepared.get("archive_write")) != "FORBIDDEN"
        or str(prepared.get("promotion")) != "FORBIDDEN"
    ):
        raise ValueError("D1 validation prepared binding contract drift")
    payload = {
        "schema_version": "cn_program_optimizer_d1_report_only_validation_authorization_v1",
        "status": "D1_REPORT_ONLY_VALIDATION_FROZEN_READY",
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "project_control_route_id": ROUTE_ID,
        "execution_authorized": True,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "authorized_host": "DESKTOP-77OPJ6F",
        "evaluation_role": "validation",
        "usage": "REPORT_ONLY_CANDIDATE_TRANSFER",
        "candidate_freeze": {
            "relative_path": str(CANDIDATE_FREEZE_RELATIVE_PATH).replace("\\", "/"),
            "freeze_payload_sha256": EXPECTED_CANDIDATE_FREEZE_PAYLOAD_SHA256,
            "candidate_count": EXPECTED_CANDIDATE_COUNT,
            "candidate_exact_identities_sha256": EXPECTED_CANDIDATE_EXACT_SHA256,
            "members_relative_path": str(CANDIDATE_MEMBERS_RELATIVE_PATH).replace("\\", "/"),
            "members_file_sha256": EXPECTED_CANDIDATE_MEMBERS_FILE_SHA256,
        },
        "prepared_binding": {
            "relative_path": str(PREPARED_BINDING_RELATIVE_PATH).replace("\\", "/"),
            "file_sha256": sha256_file(prepared_path),
            "payload_sha256": str(prepared["prepared_binding_payload_sha256"]),
            "prep_repo_sha": str(prepared["repo_sha"]),
            "required_physical_leaf_count": int(prepared["required_physical_leaf_count"]),
            "validation_field_manifest_sha256": str(prepared["validation_field_manifest_sha256"]),
            "validation_session_authority_manifest_sha256": str(
                prepared["validation_session_authority_manifest_sha256"]
            ),
            "validation_session_authority_audit_sha256": str(
                prepared["validation_session_authority_audit_sha256"]
            ),
        },
        "source_binding": {
            "source_contract_path": str(source_contract.resolve()),
            "source_contract_sha256": sha256_file(source_contract.resolve()),
            "registry_path": str(registry.resolve()),
            "registry_sha256": sha256_file(registry.resolve()),
        },
        "validation_windows": list(VALIDATION_WINDOWS),
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "development_search_visibility": "FORBIDDEN",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "oos_authority": "VALIDATION_REPORT_ONLY_EVIDENCE_ONLY",
        "promotion_authorized": False,
        "automatic_successor_authorized": False,
    }
    payload["authorization_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    payload = build(
        repo_root=args.repo_root,
        source_contract=args.source_contract,
        registry=args.registry,
    )
    output = (
        args.output.resolve()
        if args.output is not None
        else (args.repo_root.resolve() / AUTHORIZATION_RELATIVE_PATH).resolve()
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"path": str(output), **payload}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
