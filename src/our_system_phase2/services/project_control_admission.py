"""Fail-closed Project Control admission for canonical high-cost CN routes."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Collection


PROJECT_ID = "alpha_pit_true1min_engine_evalreset_20260711"
ADMISSION_SCHEMA_VERSION = "cn_project_control_execution_admission_v1"
EXECUTION_REQUEST_SCHEMA_VERSION = "cn_project_control_execution_request_v1"
TRUST_SCHEMA_VERSION = "cn_project_control_trust_v1"

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
ORIGINAL_EXECUTION_ACTIONS = {
    ACTION_LAUNCH,
    ACTION_SUCCESSOR,
    ACTION_RETRY,
}
TECHNICAL_RECOVERY = "TECHNICAL_RECOVERY_OF_ALREADY_AUTHORIZED_RUN"

_ACTIVE_ADMISSION: dict[str, Any] | None = None


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
        raise ProjectControlDenied(f"{label} path missing: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProjectControlDenied(f"invalid {label}: {resolved}") from exc
    if not isinstance(payload, dict):
        raise ProjectControlDenied(f"invalid {label}: {resolved}")
    return resolved, payload


def _self_hashed_payload(
    payload: dict[str, Any],
    *,
    hash_field: str,
    schema_field: str,
    expected_schema: str,
    label: str,
) -> dict[str, Any]:
    if payload.get(schema_field) != expected_schema:
        raise ProjectControlDenied(f"{label} schema drift")
    body = dict(payload)
    declared = str(body.pop(hash_field, ""))
    if not re.fullmatch(r"[0-9a-f]{64}", declared) or declared != _stable_hash(body):
        raise ProjectControlDenied(f"{label} payload hash drift")
    return payload


def _slug(value: str) -> str:
    result = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return result or "task"


def load_trust_config(
    path: Path,
    *,
    expected_project_id: str = PROJECT_ID,
) -> dict[str, Any]:
    resolved, payload = _read_json(path, "Project Control trust config")
    _self_hashed_payload(
        payload,
        hash_field="trust_payload_sha256",
        schema_field="schema_version",
        expected_schema=TRUST_SCHEMA_VERSION,
        label="Project Control trust config",
    )
    if payload.get("project_id") != expected_project_id:
        raise ProjectControlDenied("Project Control trust project drift")
    trusted_root = Path(str(payload.get("trusted_harness_runs_root") or "")).resolve()
    repository_path = Path(str(payload.get("repository_path") or "")).resolve()
    if not trusted_root.is_dir():
        raise ProjectControlDenied(f"trusted Harness runs root missing: {trusted_root}")
    if not repository_path.is_dir():
        raise ProjectControlDenied(f"trusted repository path missing: {repository_path}")
    return {
        **payload,
        "trust_config_path": str(resolved),
        "trust_config_file_sha256": sha256_file(resolved),
        "trusted_harness_runs_root": str(trusted_root),
        "repository_path": str(repository_path),
    }


def _parse_expiry(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProjectControlDenied("execution request expiry invalid") from exc
    if parsed.tzinfo is None:
        raise ProjectControlDenied("execution request expiry must include timezone")
    return parsed.astimezone(timezone.utc)


def _validate_request_shape(
    request: dict[str, Any], *, allow_expired: bool = False
) -> dict[str, Any]:
    _self_hashed_payload(
        request,
        hash_field="request_payload_sha256",
        schema_field="schema_version",
        expected_schema=EXECUTION_REQUEST_SCHEMA_VERSION,
        label="Project Control execution request",
    )
    action = str(request.get("requested_action") or "")
    repo_sha = str(request.get("repo_sha") or "")
    if action not in ALLOWED_ACTIONS:
        raise ProjectControlDenied("unsupported requested action")
    if not re.fullmatch(r"[0-9a-f]{40}", repo_sha):
        raise ProjectControlDenied("execution request repo SHA invalid")
    if not str(request.get("target_campaign_id") or "") or not str(
        request.get("target_run_id") or ""
    ):
        raise ProjectControlDenied("target campaign/run identity missing")
    if not allow_expired and _parse_expiry(
        str(request.get("expires_at") or "")
    ) <= datetime.now(timezone.utc):
        raise ProjectControlDenied("STALE_PROJECT_CONTROL_EXECUTION_REQUEST")
    return request


def build_execution_request(
    *,
    requested_action: str,
    target_campaign_id: str,
    target_run_id: str,
    repo_sha: str,
    expires_at: str,
    parent_project_control_run_id: str = "",
    parent_target_campaign_id: str = "",
    parent_target_run_id: str = "",
    recovery_kind: str = "",
    recovery_of_target_run_id: str = "",
    original_admission_path: str = "",
    original_admission_file_sha256: str = "",
    incident_id: str = "",
    incident_path: str = "",
    incident_file_sha256: str = "",
) -> dict[str, Any]:
    """Build the exact request that must be embedded in a Harness task spec."""

    request: dict[str, Any] = {
        "schema_version": EXECUTION_REQUEST_SCHEMA_VERSION,
        "project_id": PROJECT_ID,
        "requested_action": str(requested_action).upper(),
        "target_campaign_id": str(target_campaign_id),
        "target_run_id": str(target_run_id),
        "repo_sha": str(repo_sha),
        "expires_at": str(expires_at),
        "parent_project_control_run_id": str(parent_project_control_run_id),
        "parent_target_campaign_id": str(parent_target_campaign_id),
        "parent_target_run_id": str(parent_target_run_id),
        "recovery_kind": str(recovery_kind),
        "recovery_of_target_run_id": str(recovery_of_target_run_id),
        "original_admission_path": str(original_admission_path),
        "original_admission_file_sha256": str(
            original_admission_file_sha256
        ),
        "incident_id": str(incident_id),
        "incident_path": str(incident_path),
        "incident_file_sha256": str(incident_file_sha256),
    }
    request["request_payload_sha256"] = _stable_hash(request)
    _validate_request_shape(request)
    return {
        "task_id": f"cn-execution-{request['request_payload_sha256'][:24]}",
        "execution_request": request,
    }


def _load_harness_bundle(
    run_record_path: Path,
    *,
    trust: dict[str, Any],
    allow_expired_request: bool = False,
) -> dict[str, Any]:
    resolved, record = _read_json(run_record_path, "Harness run record")
    trusted_root = Path(str(trust["trusted_harness_runs_root"]))
    try:
        relative = resolved.relative_to(trusted_root)
    except ValueError as exc:
        raise ProjectControlDenied("Harness run record is outside trusted runs root") from exc
    if resolved.name != "run_record.json" or len(relative.parts) != 3:
        raise ProjectControlDenied("Harness run record path shape drift")
    run_root = resolved.parent
    task_path, task = _read_json(run_root / "task_spec.json", "Harness task spec")
    profile_path, profile = _read_json(
        run_root / "project_profile.json", "Harness project profile"
    )
    request = _validate_request_shape(
        dict(task.get("execution_request") or {}),
        allow_expired=allow_expired_request,
    )
    request_hash = str(request["request_payload_sha256"])
    expected_task_id = f"cn-execution-{request_hash[:24]}"
    if (
        str(record.get("run_id") or "") != run_root.name
        or str(record.get("task_id") or "") != expected_task_id
        or str(task.get("task_id") or "") != expected_task_id
        or relative.parts[0] != _slug(expected_task_id)
    ):
        raise ProjectControlDenied("Harness task/run path identity drift")
    if (
        record.get("project_id") != trust["project_id"]
        or task.get("project_id") != trust["project_id"]
        or profile.get("project_id") != trust["project_id"]
        or request.get("project_id") != trust["project_id"]
    ):
        raise ProjectControlDenied("project identity drift")
    repository_path = Path(str(trust["repository_path"]))
    source_worktree = Path(
        str(dict(record.get("worktree") or {}).get("source_worktree") or "")
    ).resolve()
    profile_repository = Path(str(profile.get("repository_path") or "")).resolve()
    if source_worktree != repository_path or profile_repository != repository_path:
        raise ProjectControlDenied("Harness repository binding drift")
    if record.get("code_base_sha") != request.get("repo_sha"):
        raise ProjectControlDenied("Harness record/request repo SHA drift")
    return {
        "run_record_path": str(resolved),
        "run_record_file_sha256": sha256_file(resolved),
        "task_spec_path": str(task_path),
        "task_spec_file_sha256": sha256_file(task_path),
        "project_profile_path": str(profile_path),
        "project_profile_file_sha256": sha256_file(profile_path),
        "record": record,
        "request": request,
    }


def _selected_receipt(record: dict[str, Any], phase: str) -> dict[str, Any]:
    matches = [
        dict(item)
        for item in list(record.get("project_control") or ())
        if isinstance(item, dict) and str(item.get("phase") or "") == phase
    ]
    if len(matches) != 1:
        raise ProjectControlDenied(f"exactly one {phase} receipt is required")
    return matches[0]


def _receipt_binding(bundle: dict[str, Any], phase: str) -> dict[str, Any]:
    record = dict(bundle["record"])
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
        "task_spec_file_sha256": bundle["task_spec_file_sha256"],
        "project_profile_file_sha256": bundle["project_profile_file_sha256"],
        "execution_request_payload_sha256": bundle["request"][
            "request_payload_sha256"
        ],
        "phase": phase,
        "automatic_field": automatic_field,
        "automatic_allowed": record.get(automatic_field),
        "receipt": receipt,
    }


def project_control_receipt_sha256(
    path: Path,
    phase: str,
    *,
    trust_config_path: Path,
) -> str:
    trust = load_trust_config(trust_config_path)
    bundle = _load_harness_bundle(path, trust=trust)
    return _stable_hash(_receipt_binding(bundle, str(phase).upper()))


def _validate_source(
    path: Path,
    *,
    trust: dict[str, Any],
    phase: str,
    expected_receipt_sha256: str,
    expected_repo_sha: str | None,
    expected_action: str | None,
    expected_target_campaign_id: str | None,
    expected_target_run_id: str | None,
    allow_expired_request: bool = False,
) -> dict[str, Any]:
    bundle = _load_harness_bundle(
        path,
        trust=trust,
        allow_expired_request=allow_expired_request or phase == "POST_BATCH",
    )
    binding = _receipt_binding(bundle, phase)
    observed_hash = _stable_hash(binding)
    if observed_hash != str(expected_receipt_sha256).lower():
        raise ProjectControlDenied("project-control receipt hash drift")
    request = dict(bundle["request"])
    if expected_repo_sha is not None and request["repo_sha"] != expected_repo_sha:
        raise ProjectControlDenied("project-control repo SHA drift")
    if expected_action is not None and request["requested_action"] != expected_action:
        raise ProjectControlDenied("requested action drift")
    if (
        expected_target_campaign_id is not None
        and request["target_campaign_id"] != expected_target_campaign_id
    ):
        raise ProjectControlDenied("target campaign drift")
    if (
        expected_target_run_id is not None
        and request["target_run_id"] != expected_target_run_id
    ):
        raise ProjectControlDenied("target run drift")
    receipt = dict(binding["receipt"])
    if phase == "PREFLIGHT":
        if receipt.get("verdict") != "PROCEED" or binding["automatic_allowed"] is not True:
            raise ProjectControlDenied("PREFLIGHT_PROCEED_REQUIRED")
    elif receipt.get("verdict") != "CONTINUE" or binding["automatic_allowed"] is not True:
        raise ProjectControlDenied("POST_BATCH_CONTINUE_REQUIRED")
    return {
        "run_record_path": bundle["run_record_path"],
        "run_record_file_sha256": bundle["run_record_file_sha256"],
        "task_spec_path": bundle["task_spec_path"],
        "task_spec_file_sha256": bundle["task_spec_file_sha256"],
        "project_profile_path": bundle["project_profile_path"],
        "project_profile_file_sha256": bundle["project_profile_file_sha256"],
        "receipt_sha256": observed_hash,
        "request": request,
        **binding,
    }


def _validate_file_binding(path_value: str, sha_value: str, label: str) -> Path:
    path = Path(str(path_value)).expanduser().resolve()
    if not path.is_file() or sha256_file(path) != str(sha_value).lower():
        raise ProjectControlDenied(f"{label} binding drift")
    return path


def materialize_admission(
    *,
    output_path: Path,
    trust_config_path: Path,
    project_control_run_record_path: Path,
    expected_project_control_receipt_sha256: str,
    requested_action: str,
    target_campaign_id: str,
    target_run_id: str,
    repo_sha: str,
    parent_post_batch_run_record_path: Path | None = None,
    expected_parent_post_batch_receipt_sha256: str = "",
) -> dict[str, Any]:
    """Freeze an action-specific admission from a trusted, target-bound Run."""

    trust = load_trust_config(trust_config_path)
    action = str(requested_action).upper()
    child = _validate_source(
        project_control_run_record_path,
        trust=trust,
        phase="PREFLIGHT",
        expected_receipt_sha256=expected_project_control_receipt_sha256,
        expected_repo_sha=str(repo_sha),
        expected_action=action,
        expected_target_campaign_id=str(target_campaign_id),
        expected_target_run_id=str(target_run_id),
    )
    request = dict(child["request"])
    parent = None
    if action == ACTION_SUCCESSOR:
        if parent_post_batch_run_record_path is None:
            raise ProjectControlDenied("SUCCESSOR_PARENT_POST_BATCH_REQUIRED")
        parent = _validate_source(
            parent_post_batch_run_record_path,
            trust=trust,
            phase="POST_BATCH",
            expected_receipt_sha256=expected_parent_post_batch_receipt_sha256,
            expected_repo_sha=None,
            expected_action=None,
            expected_target_campaign_id=str(
                request.get("parent_target_campaign_id") or ""
            ),
            expected_target_run_id=str(request.get("parent_target_run_id") or ""),
        )
        if (
            str(request.get("parent_project_control_run_id") or "")
            != parent["run_id"]
            or parent["run_id"] == child["run_id"]
        ):
            raise ProjectControlDenied("successor parent lineage drift")
    elif parent_post_batch_run_record_path is not None:
        raise ProjectControlDenied("parent POST_BATCH is valid only for successor")

    original = None
    incident = None
    if action == ACTION_RECOVERY:
        if request.get("recovery_kind") != TECHNICAL_RECOVERY:
            raise ProjectControlDenied("RECOVERY_KIND_FORBIDDEN")
        if request.get("recovery_of_target_run_id") != target_run_id:
            raise ProjectControlDenied("RECOVERY_TARGET_DRIFT")
        incident_path = _validate_file_binding(
            str(request.get("incident_path") or ""),
            str(request.get("incident_file_sha256") or ""),
            "recovery incident",
        )
        original_path = _validate_file_binding(
            str(request.get("original_admission_path") or ""),
            str(request.get("original_admission_file_sha256") or ""),
            "original execution admission",
        )
        original = validate_admission(
            original_path,
            trust_config_path=trust_config_path,
            expected_admission_file_sha256=str(
                request["original_admission_file_sha256"]
            ),
            expected_project_id=PROJECT_ID,
            expected_repo_sha=str(repo_sha),
            expected_actions=ORIGINAL_EXECUTION_ACTIONS,
            expected_target_campaign_id=str(target_campaign_id),
            expected_target_run_id=str(target_run_id),
            _allow_expired_request=True,
        )
        incident = {
            "incident_id": str(request.get("incident_id") or ""),
            "incident_path": str(incident_path),
            "incident_file_sha256": str(request["incident_file_sha256"]),
        }
        if not incident["incident_id"]:
            raise ProjectControlDenied("RECOVERY_INCIDENT_BINDING_REQUIRED")
    elif any(
        request.get(field)
        for field in (
            "recovery_kind",
            "recovery_of_target_run_id",
            "original_admission_path",
            "original_admission_file_sha256",
            "incident_id",
            "incident_path",
            "incident_file_sha256",
        )
    ):
        raise ProjectControlDenied("recovery fields forbidden for non-recovery action")

    payload: dict[str, Any] = {
        "schema_version": ADMISSION_SCHEMA_VERSION,
        "project_id": PROJECT_ID,
        "requested_action": action,
        "target_campaign_id": str(target_campaign_id),
        "target_run_id": str(target_run_id),
        "repo_sha": str(repo_sha),
        "trust_config_path": str(Path(trust_config_path).resolve()),
        "trust_config_file_sha256": trust["trust_config_file_sha256"],
        "project_control_preflight": child,
        "project_control_parent_post_batch": parent,
        "original_execution_admission": original,
        "recovery_incident": incident,
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
    trust_config_path: Path,
    expected_admission_file_sha256: str,
    expected_project_id: str,
    expected_repo_sha: str,
    expected_actions: Collection[str],
    expected_target_campaign_id: str,
    expected_target_run_id: str,
    _allow_expired_request: bool = False,
) -> dict[str, Any]:
    """Consume and revalidate a trusted, immutable admission before route import."""

    trust = load_trust_config(
        trust_config_path,
        expected_project_id=expected_project_id,
    )
    resolved, payload = _read_json(path, "project-control admission")
    if sha256_file(resolved) != str(expected_admission_file_sha256).lower():
        raise ProjectControlDenied("project-control admission file hash drift")
    _self_hashed_payload(
        payload,
        hash_field="admission_payload_sha256",
        schema_field="schema_version",
        expected_schema=ADMISSION_SCHEMA_VERSION,
        label="project-control admission",
    )
    if (
        payload.get("trust_config_path") != str(Path(trust_config_path).resolve())
        or payload.get("trust_config_file_sha256")
        != trust["trust_config_file_sha256"]
    ):
        raise ProjectControlDenied("Project Control trust binding drift")
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
    child_payload = dict(payload.get("project_control_preflight") or {})
    child = _validate_source(
        Path(str(child_payload.get("run_record_path") or "")),
        trust=trust,
        phase="PREFLIGHT",
        expected_receipt_sha256=str(child_payload.get("receipt_sha256") or ""),
        expected_repo_sha=expected_repo_sha,
        expected_action=action,
        expected_target_campaign_id=expected_target_campaign_id,
        expected_target_run_id=expected_target_run_id,
        allow_expired_request=_allow_expired_request,
    )
    if child != child_payload:
        raise ProjectControlDenied("project-control preflight binding drift")
    parent_run_id = None
    if action == ACTION_SUCCESSOR:
        parent_payload = dict(payload.get("project_control_parent_post_batch") or {})
        child_request = dict(child["request"])
        parent = _validate_source(
            Path(str(parent_payload.get("run_record_path") or "")),
            trust=trust,
            phase="POST_BATCH",
            expected_receipt_sha256=str(parent_payload.get("receipt_sha256") or ""),
            expected_repo_sha=None,
            expected_action=None,
            expected_target_campaign_id=str(
                child_request.get("parent_target_campaign_id") or ""
            ),
            expected_target_run_id=str(
                child_request.get("parent_target_run_id") or ""
            ),
        )
        if (
            parent != parent_payload
            or child_request.get("parent_project_control_run_id") != parent["run_id"]
        ):
            raise ProjectControlDenied("project-control parent lineage drift")
        parent_run_id = parent["run_id"]
    elif payload.get("project_control_parent_post_batch") is not None:
        raise ProjectControlDenied("unexpected parent POST_BATCH binding")
    if action == ACTION_RECOVERY:
        request = dict(child["request"])
        original_payload = dict(payload.get("original_execution_admission") or {})
        original_path = _validate_file_binding(
            str(request.get("original_admission_path") or ""),
            str(request.get("original_admission_file_sha256") or ""),
            "original execution admission",
        )
        original = validate_admission(
            original_path,
            trust_config_path=trust_config_path,
            expected_admission_file_sha256=str(
                request["original_admission_file_sha256"]
            ),
            expected_project_id=expected_project_id,
            expected_repo_sha=expected_repo_sha,
            expected_actions=ORIGINAL_EXECUTION_ACTIONS,
            expected_target_campaign_id=expected_target_campaign_id,
            expected_target_run_id=expected_target_run_id,
            _allow_expired_request=True,
        )
        if original != original_payload:
            raise ProjectControlDenied("original execution admission binding drift")
        incident_payload = dict(payload.get("recovery_incident") or {})
        _validate_file_binding(
            str(incident_payload.get("incident_path") or ""),
            str(incident_payload.get("incident_file_sha256") or ""),
            "recovery incident",
        )
        if (
            request.get("recovery_kind") != TECHNICAL_RECOVERY
            or request.get("recovery_of_target_run_id") != expected_target_run_id
            or incident_payload.get("incident_id") != request.get("incident_id")
        ):
            raise ProjectControlDenied("recovery lineage drift")
    elif payload.get("original_execution_admission") is not None or payload.get(
        "recovery_incident"
    ) is not None:
        raise ProjectControlDenied("unexpected recovery binding")
    if payload.get("economic_decision_authority") != "FORBIDDEN":
        raise ProjectControlDenied("Project Control cannot decide economics")
    if payload.get("alpha_selection_authority") != "FORBIDDEN":
        raise ProjectControlDenied("Project Control cannot select alpha")
    return {
        "status": "PROJECT_CONTROL_ADMISSION_ELIGIBLE",
        "admission_path": str(resolved),
        "admission_file_sha256": sha256_file(resolved),
        "admission_payload_sha256": payload["admission_payload_sha256"],
        "project_control_run_id": child["run_id"],
        "parent_run_id": parent_run_id,
        "project_id": expected_project_id,
        "requested_action": action,
        "target_campaign_id": expected_target_campaign_id,
        "target_run_id": expected_target_run_id,
        "repo_sha": expected_repo_sha,
        "recovery_kind": str(child["request"].get("recovery_kind") or ""),
        "incident_id": str(child["request"].get("incident_id") or ""),
    }


def activate_admission(proof: dict[str, Any]) -> None:
    global _ACTIVE_ADMISSION
    if proof.get("status") != "PROJECT_CONTROL_ADMISSION_ELIGIBLE":
        raise ProjectControlDenied("cannot activate an ineligible admission")
    if _ACTIVE_ADMISSION is not None:
        raise ProjectControlDenied("another high-cost admission is already active")
    _ACTIVE_ADMISSION = dict(proof)


def consume_active_admission(expected_campaign_id: str) -> dict[str, Any]:
    global _ACTIVE_ADMISSION
    proof = _ACTIVE_ADMISSION
    _ACTIVE_ADMISSION = None
    if proof is None:
        raise ProjectControlDenied("DIRECT_HIGH_COST_MODULE_EXECUTION_FORBIDDEN")
    if proof.get("target_campaign_id") != expected_campaign_id:
        raise ProjectControlDenied("active admission campaign drift")
    return proof


def clear_active_admission() -> None:
    global _ACTIVE_ADMISSION
    _ACTIVE_ADMISSION = None
