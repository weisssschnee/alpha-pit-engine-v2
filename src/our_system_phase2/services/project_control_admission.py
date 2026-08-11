"""Thin physical consumer for Project Control execution decisions."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Collection


PROJECT_ID = "alpha_pit_true1min_engine_evalreset_20260711"
ADMISSION_SCHEMA_VERSION = "cn_project_control_execution_admission_v1"

ACTION_FREEZE = "FREEZE_HIGH_COST_CAMPAIGN"
ACTION_LAUNCH = "LAUNCH_HIGH_COST_CAMPAIGN"
ACTION_SUCCESSOR = "SUCCESSOR_CAMPAIGN"
ACTION_RETRY = "RETRY"
ACTION_RECOVERY = "RECOVERY"
ALLOWED_ACTIONS = {
    ACTION_FREEZE,
    ACTION_LAUNCH,
    ACTION_SUCCESSOR,
    ACTION_RETRY,
    ACTION_RECOVERY,
}
TECHNICAL_RECOVERY = "TECHNICAL_RECOVERY_OF_ALREADY_AUTHORIZED_RUN"


class ProjectControlDenied(PermissionError):
    """Raised before a high-cost route can import or execute."""


def _stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def sha256_file(path: Path) -> str:
    resolved = path.expanduser().resolve()
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, label: str) -> tuple[Path, dict[str, Any]]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise ProjectControlDenied(f"project-control receipt path missing: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProjectControlDenied(f"invalid {label}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise ProjectControlDenied(f"invalid {label}: {resolved}")
    return resolved, payload


def _selected_receipt(record: dict[str, Any], phase: str) -> dict[str, Any]:
    matches = [
        dict(item)
        for item in list(record.get("project_control") or ())
        if isinstance(item, dict) and str(item.get("phase") or "") == phase
    ]
    if len(matches) != 1:
        raise ProjectControlDenied(f"exactly one {phase} receipt is required")
    return matches[0]


def _receipt_binding(record: dict[str, Any], phase: str) -> dict[str, Any]:
    receipt = _selected_receipt(record, phase)
    automatic_field = (
        "automatic_execution_allowed"
        if phase == "PREFLIGHT"
        else "automatic_continuation_allowed"
    )
    return {
        "run_id": str(record.get("run_id") or ""),
        "task_id": str(record.get("task_id") or ""),
        "project_id": str(record.get("project_id") or ""),
        "code_base_sha": str(record.get("code_base_sha") or ""),
        "phase": phase,
        "automatic_field": automatic_field,
        "automatic_allowed": record.get(automatic_field),
        "receipt": receipt,
    }


def project_control_receipt_sha256(path: Path, phase: str) -> str:
    _, record = _read_json(path, "project-control run record")
    return _stable_hash(_receipt_binding(record, str(phase).upper()))


def _validate_source(
    path: Path,
    *,
    phase: str,
    expected_receipt_sha256: str,
    expected_project_id: str,
    expected_repo_sha: str | None,
) -> dict[str, Any]:
    resolved, record = _read_json(path, "project-control run record")
    binding = _receipt_binding(record, phase)
    observed_hash = _stable_hash(binding)
    if observed_hash != str(expected_receipt_sha256).lower():
        raise ProjectControlDenied("project-control receipt hash drift")
    if binding["project_id"] != expected_project_id:
        raise ProjectControlDenied("project identity drift")
    if not binding["run_id"]:
        raise ProjectControlDenied("project-control run identity missing")
    if expected_repo_sha is not None and binding["code_base_sha"] != expected_repo_sha:
        raise ProjectControlDenied("project-control repo SHA drift")
    receipt = dict(binding["receipt"])
    if phase == "PREFLIGHT":
        if receipt.get("verdict") != "PROCEED" or binding["automatic_allowed"] is not True:
            raise ProjectControlDenied("PREFLIGHT_PROCEED_REQUIRED")
    elif (
        receipt.get("verdict") != "CONTINUE"
        or binding["automatic_allowed"] is not True
    ):
        raise ProjectControlDenied("POST_BATCH_CONTINUE_REQUIRED")
    return {
        "run_record_path": str(resolved),
        "receipt_sha256": observed_hash,
        **binding,
    }


def _validate_action_shape(
    *,
    requested_action: str,
    target_campaign_id: str,
    target_run_id: str,
    repo_sha: str,
    recovery_kind: str,
    recovery_of_target_run_id: str,
    incident_id: str,
) -> None:
    if requested_action not in ALLOWED_ACTIONS:
        raise ProjectControlDenied("unsupported requested action")
    if not target_campaign_id or not target_run_id:
        raise ProjectControlDenied("target campaign/run identity missing")
    if not re.fullmatch(r"[0-9a-f]{40}", repo_sha):
        raise ProjectControlDenied("relevant repo SHA invalid")
    if requested_action == ACTION_RECOVERY:
        if recovery_kind != TECHNICAL_RECOVERY:
            raise ProjectControlDenied("RECOVERY_KIND_FORBIDDEN")
        if not incident_id:
            raise ProjectControlDenied("RECOVERY_INCIDENT_BINDING_REQUIRED")
        if not recovery_of_target_run_id or target_run_id != recovery_of_target_run_id:
            raise ProjectControlDenied("RECOVERY_TARGET_DRIFT")
    elif recovery_kind or recovery_of_target_run_id or incident_id:
        raise ProjectControlDenied("recovery fields forbidden for non-recovery action")


def materialize_admission(
    *,
    output_path: Path,
    project_control_run_record_path: Path,
    expected_project_control_receipt_sha256: str,
    requested_action: str,
    target_campaign_id: str,
    target_run_id: str,
    repo_sha: str,
    parent_post_batch_run_record_path: Path | None = None,
    expected_parent_post_batch_receipt_sha256: str = "",
    recovery_kind: str = "",
    recovery_of_target_run_id: str = "",
    incident_id: str = "",
) -> dict[str, Any]:
    """Freeze an action-specific admission from already recorded L1 decisions."""

    action = str(requested_action).upper()
    _validate_action_shape(
        requested_action=action,
        target_campaign_id=str(target_campaign_id),
        target_run_id=str(target_run_id),
        repo_sha=str(repo_sha),
        recovery_kind=str(recovery_kind),
        recovery_of_target_run_id=str(recovery_of_target_run_id),
        incident_id=str(incident_id),
    )
    child = _validate_source(
        project_control_run_record_path,
        phase="PREFLIGHT",
        expected_receipt_sha256=expected_project_control_receipt_sha256,
        expected_project_id=PROJECT_ID,
        expected_repo_sha=repo_sha,
    )
    parent = None
    if action == ACTION_SUCCESSOR:
        if parent_post_batch_run_record_path is None:
            raise ProjectControlDenied("SUCCESSOR_PARENT_POST_BATCH_REQUIRED")
        parent = _validate_source(
            parent_post_batch_run_record_path,
            phase="POST_BATCH",
            expected_receipt_sha256=expected_parent_post_batch_receipt_sha256,
            expected_project_id=PROJECT_ID,
            expected_repo_sha=None,
        )
        if parent["run_id"] == child["run_id"]:
            raise ProjectControlDenied("successor parent and child control runs must differ")
    elif parent_post_batch_run_record_path is not None:
        raise ProjectControlDenied("parent POST_BATCH is valid only for successor")

    payload: dict[str, Any] = {
        "schema_version": ADMISSION_SCHEMA_VERSION,
        "project_id": PROJECT_ID,
        "requested_action": action,
        "target_campaign_id": str(target_campaign_id),
        "target_run_id": str(target_run_id),
        "repo_sha": str(repo_sha),
        "project_control_preflight": child,
        "project_control_parent_post_batch": parent,
        "recovery_kind": str(recovery_kind),
        "recovery_of_target_run_id": str(recovery_of_target_run_id),
        "incident_id": str(incident_id),
        "economic_decision_authority": "FORBIDDEN",
        "alpha_selection_authority": "FORBIDDEN",
    }
    payload["admission_payload_sha256"] = _stable_hash(payload)
    output = output_path.expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"project-control admission already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return payload


def validate_admission(
    path: Path,
    *,
    expected_admission_file_sha256: str,
    expected_project_id: str,
    expected_repo_sha: str,
    expected_actions: Collection[str],
    expected_target_campaign_id: str,
    expected_target_run_id: str,
) -> dict[str, Any]:
    """Consume and revalidate an immutable admission before route import."""

    resolved, payload = _read_json(path, "project-control admission")
    if sha256_file(resolved) != str(expected_admission_file_sha256).lower():
        raise ProjectControlDenied("project-control admission file hash drift")
    body = dict(payload)
    declared = str(body.pop("admission_payload_sha256", ""))
    if declared != _stable_hash(body):
        raise ProjectControlDenied("project-control admission payload hash drift")
    if payload.get("schema_version") != ADMISSION_SCHEMA_VERSION:
        raise ProjectControlDenied("project-control admission schema drift")
    if payload.get("project_id") != expected_project_id:
        raise ProjectControlDenied("project identity drift")
    if payload.get("repo_sha") != expected_repo_sha:
        raise ProjectControlDenied("repo SHA drift")
    action = str(payload.get("requested_action") or "")
    if action not in set(expected_actions):
        raise ProjectControlDenied("requested action drift")
    if payload.get("target_campaign_id") != expected_target_campaign_id:
        raise ProjectControlDenied("target campaign drift")
    if payload.get("target_run_id") != expected_target_run_id:
        raise ProjectControlDenied("target run drift")
    _validate_action_shape(
        requested_action=action,
        target_campaign_id=str(payload.get("target_campaign_id") or ""),
        target_run_id=str(payload.get("target_run_id") or ""),
        repo_sha=str(payload.get("repo_sha") or ""),
        recovery_kind=str(payload.get("recovery_kind") or ""),
        recovery_of_target_run_id=str(
            payload.get("recovery_of_target_run_id") or ""
        ),
        incident_id=str(payload.get("incident_id") or ""),
    )
    child_payload = dict(payload.get("project_control_preflight") or {})
    child = _validate_source(
        Path(str(child_payload.get("run_record_path") or "")),
        phase="PREFLIGHT",
        expected_receipt_sha256=str(child_payload.get("receipt_sha256") or ""),
        expected_project_id=expected_project_id,
        expected_repo_sha=expected_repo_sha,
    )
    if child != child_payload:
        raise ProjectControlDenied("project-control preflight binding drift")
    parent_run_id = None
    if action == ACTION_SUCCESSOR:
        parent_payload = dict(payload.get("project_control_parent_post_batch") or {})
        parent = _validate_source(
            Path(str(parent_payload.get("run_record_path") or "")),
            phase="POST_BATCH",
            expected_receipt_sha256=str(parent_payload.get("receipt_sha256") or ""),
            expected_project_id=expected_project_id,
            expected_repo_sha=None,
        )
        if parent != parent_payload:
            raise ProjectControlDenied("project-control parent binding drift")
        parent_run_id = parent["run_id"]
    elif payload.get("project_control_parent_post_batch") is not None:
        raise ProjectControlDenied("unexpected parent POST_BATCH binding")
    if payload.get("economic_decision_authority") != "FORBIDDEN":
        raise ProjectControlDenied("Project Control cannot decide economics")
    if payload.get("alpha_selection_authority") != "FORBIDDEN":
        raise ProjectControlDenied("Project Control cannot select alpha")
    return {
        "status": "PROJECT_CONTROL_ADMISSION_ELIGIBLE",
        "admission_path": str(resolved),
        "admission_file_sha256": sha256_file(resolved),
        "admission_payload_sha256": declared,
        "project_control_run_id": child["run_id"],
        "parent_run_id": parent_run_id,
        "project_id": expected_project_id,
        "requested_action": action,
        "target_campaign_id": payload["target_campaign_id"],
        "target_run_id": payload["target_run_id"],
        "repo_sha": payload["repo_sha"],
        "recovery_kind": payload.get("recovery_kind") or "",
        "incident_id": payload.get("incident_id") or "",
    }
