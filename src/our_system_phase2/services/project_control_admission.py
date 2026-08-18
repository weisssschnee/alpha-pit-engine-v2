"""Fail-closed Project Control admission for canonical high-cost CN routes."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Collection, Mapping


PROJECT_ID = "alpha_pit_true1min_engine_evalreset_20260711"
REPO_ROOT = Path(__file__).resolve().parents[3]
CANONICAL_TRUST_CONFIG = (
    REPO_ROOT / "runtime" / "run_plans" / "cn_project_control_trust_v1.json"
)
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
SOURCE_REPAIR_RECOVERY_SCOPES = {
    ("cn-joint-program-search-v2-canary", "ENGINE_ROOT_FINALIZATION_ONLY"),
    (
        "cn-program-optimizer-tournament-v1",
        "PHASE_C_CHECKPOINT_RECOVERY_AFTER_RESOURCE_FAILURE",
    ),
    (
        "cn-program-optimizer-tournament-v1",
        "PHASE_C_CHECKPOINT_RECOVERY",
    ),
}
PROGRAM_TOURNAMENT_RECORDS_PER_CHECKPOINT = 8

# The qualified 77o wrappers create only these non-financial launch-control
# artifacts before app.py can consume the admission. Business output remains
# forbidden until after the durable control directory is claimed.
PREPARED_OUTPUT_ROOT_ALLOWLISTS: dict[str, frozenset[str]] = {
    "cn-targeted-search-medium-campaign": frozenset(
        {"deployment_binding.json", "campaign.stdout.log", "campaign.stderr.log"}
    ),
    "cn-large-tpe-search-campaign": frozenset(
        {
            "deployment_binding.json",
            "campaign.stdout.log",
            "campaign.stderr.log",
            "qualification_authorization.json",
            "resource_leases",
        }
    ),
    "cn-fixed-stratified-production-v0": frozenset(
        {"deployment_binding.json"}
    ),
}
CAMPAIGN_AUTHORIZATION_BOUND_ROUTES = frozenset(
    {
        "cn-targeted-search-medium-campaign",
        "cn-large-tpe-search-campaign",
        "cn-joint-program-search-v2-canary",
        "cn-program-optimizer-tournament-v1",
        "cn-program-optimizer-successor-benchmark-v1",
        "cn-program-optimizer-d1-development-v1",
        "cn-program-optimizer-large-fresh-development-v1",
        "cn-program-optimizer-large-fresh-development-v2",
        "cn-program-optimizer-d1-report-only-validation-v1",
        "cn-program-optimizer-d1-transfer-prospective-validation-v1",
    }
)

_ACTIVE_ADMISSION: dict[str, Any] | None = None


class ProjectControlDenied(PermissionError):
    """Raised before a high-cost route can import or execute."""


@dataclass(frozen=True)
class VerifiedCampaignAuthorization:
    """One byte-exact campaign authorization read at the admission seam."""

    path: Path
    file_sha256: str
    byte_count: int
    payload: dict[str, Any]


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
    runtime_repository_path: Path | None = None,
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
    if (
        payload.get("trust_model") != "HARNESS_RUNS_ROOT_IS_AUTHORITY_STORE"
        or payload.get("cryptographic_receipt_signature")
        != "UNAVAILABLE_IN_EXISTING_HARNESS"
    ):
        raise ProjectControlDenied("Project Control trust model drift")
    configured = list(payload.get("deployments") or ())
    if not configured:
        configured = [
            {
                "deployment_id": "canonical",
                "repository_path": payload.get("repository_path"),
                "trusted_harness_runs_root": payload.get(
                    "trusted_harness_runs_root"
                ),
            }
        ]
    deployments: list[dict[str, str]] = []
    for item in configured:
        deployment = dict(item or {})
        deployment_id = str(deployment.get("deployment_id") or "")
        trusted_root_value = str(
            deployment.get("trusted_harness_runs_root") or ""
        )
        repository_value = str(deployment.get("repository_path") or "")
        repository_prefix_value = str(
            deployment.get("repository_root_prefix") or ""
        )
        if (
            not deployment_id
            or not Path(trusted_root_value).is_absolute()
            or bool(repository_value) == bool(repository_prefix_value)
        ):
            raise ProjectControlDenied("Project Control deployment trust drift")
        deployments.append(
            {
                "deployment_id": deployment_id,
                "trusted_harness_runs_root": str(
                    Path(trusted_root_value).resolve()
                ),
                "repository_path": (
                    str(Path(repository_value).resolve())
                    if repository_value
                    else ""
                ),
                "repository_root_prefix": (
                    str(Path(repository_prefix_value).resolve())
                    if repository_prefix_value
                    else ""
                ),
            }
        )
    if len({item["deployment_id"] for item in deployments}) != len(deployments):
        raise ProjectControlDenied("Project Control deployment id drift")

    active_deployment: dict[str, str] | None = None
    if runtime_repository_path is not None:
        runtime_repository = Path(runtime_repository_path).resolve()
        for deployment in deployments:
            exact = deployment["repository_path"]
            prefix = deployment["repository_root_prefix"]
            if exact and runtime_repository == Path(exact):
                active_deployment = deployment
                break
            if prefix:
                try:
                    runtime_repository.relative_to(Path(prefix))
                except ValueError:
                    continue
                active_deployment = deployment
                break
        if active_deployment is None:
            raise ProjectControlDenied(
                "executing repository is outside canonical deployment trust"
            )
        active_root = Path(active_deployment["trusted_harness_runs_root"])
        if not active_root.is_dir():
            raise ProjectControlDenied(
                f"trusted Harness runs root missing: {active_root}"
            )
        if not runtime_repository.is_dir():
            raise ProjectControlDenied(
                f"executing repository path missing: {runtime_repository}"
            )
    elif not any(
        Path(item["trusted_harness_runs_root"]).is_dir()
        for item in deployments
    ):
        raise ProjectControlDenied("all trusted Harness runs roots are missing")
    return {
        **payload,
        "trust_config_path": str(resolved),
        "trust_config_file_sha256": sha256_file(resolved),
        "deployments": deployments,
        "source_deployments": (
            [active_deployment]
            if active_deployment is not None
            else deployments
        ),
        "active_deployment": active_deployment,
        "repository_path": (
            str(Path(runtime_repository_path).resolve())
            if runtime_repository_path is not None
            else str(payload.get("repository_path") or "")
        ),
        "trusted_harness_runs_root": (
            active_deployment["trusted_harness_runs_root"]
            if active_deployment is not None
            else str(payload.get("trusted_harness_runs_root") or "")
        ),
    }


def _repository_is_allowlisted(path: Path, trust: dict[str, Any]) -> bool:
    resolved = path.resolve()
    for deployment in list(trust["deployments"]):
        exact = str(deployment.get("repository_path") or "")
        prefix = str(deployment.get("repository_root_prefix") or "")
        if exact and resolved == Path(exact):
            return True
        if prefix:
            try:
                resolved.relative_to(Path(prefix))
            except ValueError:
                continue
            return True
    return False


def _parse_expiry(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProjectControlDenied("execution request expiry invalid") from exc
    if parsed.tzinfo is None:
        raise ProjectControlDenied("execution request expiry must include timezone")
    return parsed.astimezone(timezone.utc)


def _canonical_output_root(value: str | Path) -> str:
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        raise ProjectControlDenied("execution target output root must be absolute")
    return str(path.resolve())


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
    original_repo_sha = str(request.get("original_repo_sha") or "")
    if original_repo_sha and not re.fullmatch(r"[0-9a-f]{40}", original_repo_sha):
        raise ProjectControlDenied("original execution repo SHA invalid")
    if not str(request.get("target_campaign_id") or "") or not str(
        request.get("target_run_id") or ""
    ):
        raise ProjectControlDenied("target campaign/run identity missing")
    if request.get("target_output_root") != _canonical_output_root(
        str(request.get("target_output_root") or "")
    ):
        raise ProjectControlDenied("execution target output root drift")
    authorization_fields = (
        "campaign_authorization_path",
        "campaign_authorization_file_sha256",
        "target_campaign_instance_id",
        "target_campaign_profile",
    )
    authorization_values = [str(request.get(field) or "") for field in authorization_fields]
    if any(authorization_values):
        if not all(authorization_values):
            raise ProjectControlDenied("campaign authorization binding incomplete")
        authorization_path = Path(authorization_values[0])
        if (
            not authorization_path.is_absolute()
            or str(authorization_path.resolve()) != authorization_values[0]
            or not re.fullmatch(r"[0-9a-f]{64}", authorization_values[1])
        ):
            raise ProjectControlDenied("campaign authorization binding invalid")
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
    target_output_root: str | Path,
    repo_sha: str,
    expires_at: str,
    campaign_authorization_path: str | Path = "",
    campaign_authorization_file_sha256: str = "",
    target_campaign_instance_id: str = "",
    target_campaign_profile: str = "",
    parent_project_control_run_id: str = "",
    parent_target_campaign_id: str = "",
    parent_target_run_id: str = "",
    recovery_kind: str = "",
    recovery_of_target_run_id: str = "",
    original_admission_path: str = "",
    original_admission_file_sha256: str = "",
    original_repo_sha: str = "",
    recovery_scope: str = "",
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
        "target_output_root": _canonical_output_root(target_output_root),
        "repo_sha": str(repo_sha),
        "expires_at": str(expires_at),
        "campaign_authorization_path": (
            _canonical_output_root(campaign_authorization_path)
            if str(campaign_authorization_path)
            else ""
        ),
        "campaign_authorization_file_sha256": str(
            campaign_authorization_file_sha256
        ).lower(),
        "target_campaign_instance_id": str(target_campaign_instance_id),
        "target_campaign_profile": str(target_campaign_profile),
        "parent_project_control_run_id": str(parent_project_control_run_id),
        "parent_target_campaign_id": str(parent_target_campaign_id),
        "parent_target_run_id": str(parent_target_run_id),
        "recovery_kind": str(recovery_kind),
        "recovery_of_target_run_id": str(recovery_of_target_run_id),
        "original_admission_path": str(original_admission_path),
        "original_admission_file_sha256": str(
            original_admission_file_sha256
        ),
        "original_repo_sha": str(original_repo_sha),
        "recovery_scope": str(recovery_scope),
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
    relative: Path | None = None
    for deployment in list(trust["source_deployments"]):
        trusted_root = Path(str(deployment["trusted_harness_runs_root"]))
        try:
            relative = resolved.relative_to(trusted_root)
        except ValueError:
            continue
        break
    if relative is None:
        raise ProjectControlDenied("Harness run record is outside trusted runs root")
    if resolved.name != "run_record.json" or len(relative.parts) != 3:
        raise ProjectControlDenied("Harness run record path shape drift")
    run_root = resolved.parent
    task_path, task = _read_json(run_root / "task_spec.json", "Harness task spec")
    profile_path, profile = _read_json(
        run_root / "project_profile.json", "Harness project profile"
    )
    required_record_fields = {
        "schema_version", "run_id", "task_id", "project_id", "session_id",
        "code_base_sha", "worktree", "started_at", "status",
        "commands_executed", "files_changed", "checks_executed",
        "evidence_references", "failure_class", "handoff_reference",
    }
    required_task_fields = {
        "schema_version", "task_id", "project_id", "objective", "background",
        "in_scope", "out_of_scope", "constraints", "expected_artifacts",
        "acceptance_checks", "risk_level", "execution_mode", "stop_conditions",
    }
    required_profile_fields = {
        "schema_version", "project_id", "repository_path", "primary_branch",
        "project_instructions", "architecture_docs", "setup_commands",
        "test_commands", "build_commands", "smoke_test_commands",
        "allowed_paths", "protected_paths", "sensitive_files",
        "environment_requirements", "git_policy", "worktree_policy",
    }
    if (
        record.get("schema_version") != 1
        or not required_record_fields.issubset(record)
        or task.get("schema_version") != 1
        or not required_task_fields.issubset(task)
        or profile.get("schema_version") != 1
        or not required_profile_fields.issubset(profile)
    ):
        raise ProjectControlDenied("Harness bundle schema drift")
    execution_context_path = run_root / "execution_context.md"
    if not execution_context_path.is_file():
        raise ProjectControlDenied("Harness execution context missing")
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
    source_worktree = Path(
        str(dict(record.get("worktree") or {}).get("source_worktree") or "")
    ).resolve()
    profile_repository = Path(str(profile.get("repository_path") or "")).resolve()
    if not _repository_is_allowlisted(
        source_worktree, trust
    ) or not _repository_is_allowlisted(profile_repository, trust):
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
        "execution_context_path": str(execution_context_path.resolve()),
        "execution_context_file_sha256": sha256_file(execution_context_path),
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
        "execution_context_file_sha256": bundle[
            "execution_context_file_sha256"
        ],
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
    expected_target_output_root: str | Path | None,
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
    if (
        expected_target_output_root is not None
        and request["target_output_root"]
        != _canonical_output_root(expected_target_output_root)
    ):
        raise ProjectControlDenied("target output root drift")
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
        "execution_context_path": bundle["execution_context_path"],
        "execution_context_file_sha256": bundle[
            "execution_context_file_sha256"
        ],
        "receipt_sha256": observed_hash,
        "request": request,
        **binding,
    }


def _validate_file_binding(path_value: str, sha_value: str, label: str) -> Path:
    path = Path(str(path_value)).expanduser().resolve()
    if not path.is_file() or sha256_file(path) != str(sha_value).lower():
        raise ProjectControlDenied(f"{label} binding drift")
    return path


def _validate_source_repair_recovery(
    *,
    target_campaign_id: str,
    request: Mapping[str, Any],
    current_repo_sha: str,
    incident_path: Path,
) -> str:
    original_repo_sha = str(request.get("original_repo_sha") or current_repo_sha)
    if original_repo_sha == current_repo_sha:
        return original_repo_sha
    recovery_scope = str(request.get("recovery_scope") or "")
    if (target_campaign_id, recovery_scope) not in SOURCE_REPAIR_RECOVERY_SCOPES:
        raise ProjectControlDenied("cross-SHA recovery scope forbidden")
    _, incident = _read_json(incident_path, "recovery incident")
    if target_campaign_id == "cn-program-optimizer-tournament-v1":
        incident_body = dict(incident)
        incident_hash = str(incident_body.pop("incident_payload_sha256", ""))
        if incident_hash != _stable_hash(incident_body):
            raise ProjectControlDenied("source-repair recovery incident hash drift")
    common_drift = (
        str(incident.get("checkpoint_builder_repo_sha") or "")
        != original_repo_sha
        or str(incident.get("recovery_scope") or "") != recovery_scope
        or bool(incident.get("incomplete_results_reused"))
    )
    if target_campaign_id == "cn-program-optimizer-tournament-v1":
        try:
            closed_checkpoint_count = int(
                incident.get("closed_checkpoint_count") or 0
            )
            closed_record_count = int(incident.get("closed_record_count") or 0)
            first_recovered_checkpoint = int(
                incident.get("first_recovered_checkpoint") or 0
            )
        except (TypeError, ValueError) as exc:
            raise ProjectControlDenied(
                "source-repair recovery incident boundary drift"
            ) from exc
        boundary_drift = (
            not bool(incident.get("checkpoint_recomputation_authorized"))
            or closed_checkpoint_count < 1
            or closed_record_count
            != closed_checkpoint_count
            * PROGRAM_TOURNAMENT_RECORDS_PER_CHECKPOINT
            or first_recovered_checkpoint != closed_checkpoint_count + 1
        )
        if recovery_scope == "PHASE_C_CHECKPOINT_RECOVERY":
            boundary_drift = (
                boundary_drift
                or not str(incident.get("failure_classification") or "")
                or bool(incident.get("financial_results_reusable"))
                or incident.get("optimizer_tell_count", 0) != 0
                or any(
                    incident.get(key, 0) != 0
                    for key in (
                        "validation_reads",
                        "holdout_reads",
                        "historical_2023_reads",
                        "forward_b_reads",
                        "forward_2026_reads",
                    )
                )
            )
    else:
        boundary_drift = (
            bool(incident.get("checkpoint_recomputation_authorized"))
            or int(incident.get("closed_checkpoint_count") or 0) != 64
            or int(incident.get("closed_record_count") or 0) != 512
        )
    if common_drift or boundary_drift:
        raise ProjectControlDenied("source-repair recovery incident drift")
    return original_repo_sha


def _original_admission_trust_config(original_path: Path) -> Path:
    _, payload = _read_json(original_path, "original execution admission")
    trust_path = Path(str(payload.get("trust_config_path") or "")).resolve()
    if not trust_path.is_file():
        raise ProjectControlDenied("original execution trust config missing")
    return trust_path


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
        expected_target_output_root=None,
    )
    request = dict(child["request"])
    if str(target_campaign_id) in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES and not all(
        str(request.get(field) or "")
        for field in (
            "campaign_authorization_path",
            "campaign_authorization_file_sha256",
            "target_campaign_instance_id",
            "target_campaign_profile",
        )
    ):
        raise ProjectControlDenied(
            "target campaign authorization binding is required"
        )
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
            expected_target_output_root=None,
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
        original_repo_sha = _validate_source_repair_recovery(
            target_campaign_id=str(target_campaign_id),
            request=request,
            current_repo_sha=str(repo_sha),
            incident_path=incident_path,
        )
        original = validate_admission(
            original_path,
            trust_config_path=_original_admission_trust_config(original_path),
            expected_admission_file_sha256=str(
                request["original_admission_file_sha256"]
            ),
            expected_project_id=PROJECT_ID,
            expected_repo_sha=original_repo_sha,
            expected_actions=ORIGINAL_EXECUTION_ACTIONS,
            expected_target_campaign_id=str(target_campaign_id),
            expected_target_run_id=str(target_run_id),
            expected_target_output_root=str(request["target_output_root"]),
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
            "original_repo_sha",
            "recovery_scope",
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
        "target_output_root": str(request["target_output_root"]),
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
    expected_target_output_root: str | Path,
    _allow_expired_request: bool = False,
    _runtime_repository_path: Path | None = None,
) -> dict[str, Any]:
    """Consume and revalidate a trusted, immutable admission before route import."""

    trust = load_trust_config(
        trust_config_path,
        expected_project_id=expected_project_id,
        runtime_repository_path=_runtime_repository_path,
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
    canonical_output_root = _canonical_output_root(expected_target_output_root)
    if payload.get("target_output_root") != canonical_output_root:
        raise ProjectControlDenied("target output root drift")
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
        expected_target_output_root=canonical_output_root,
        allow_expired_request=_allow_expired_request,
    )
    if child != child_payload:
        raise ProjectControlDenied("project-control preflight binding drift")
    parent_run_id = None
    execution_lineage_action = action
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
            expected_target_output_root=None,
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
            trust_config_path=_original_admission_trust_config(original_path),
            expected_admission_file_sha256=str(
                request["original_admission_file_sha256"]
            ),
            expected_project_id=expected_project_id,
            expected_repo_sha=str(request.get("original_repo_sha") or expected_repo_sha),
            expected_actions=ORIGINAL_EXECUTION_ACTIONS,
            expected_target_campaign_id=expected_target_campaign_id,
            expected_target_run_id=expected_target_run_id,
            expected_target_output_root=canonical_output_root,
            _allow_expired_request=True,
            _runtime_repository_path=_runtime_repository_path,
        )
        if original != original_payload:
            raise ProjectControlDenied("original execution admission binding drift")
        original_repo_sha = _validate_source_repair_recovery(
            target_campaign_id=expected_target_campaign_id,
            request=request,
            current_repo_sha=expected_repo_sha,
            incident_path=Path(str(request["incident_path"])),
        )
        if original_repo_sha != str(original.get("repo_sha") or ""):
            raise ProjectControlDenied("original execution repo SHA drift")
        execution_lineage_action = str(
            original.get("execution_lineage_action")
            or original.get("requested_action")
            or ""
        )
        if any(
            str(request.get(field) or "") != str(original.get(field) or "")
            for field in (
                "campaign_authorization_path",
                "campaign_authorization_file_sha256",
                "target_campaign_instance_id",
                "target_campaign_profile",
            )
        ):
            raise ProjectControlDenied("recovery campaign authorization drift")
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
        "execution_lineage_action": execution_lineage_action,
        "target_campaign_id": expected_target_campaign_id,
        "target_run_id": expected_target_run_id,
        "target_output_root": canonical_output_root,
        "repo_sha": expected_repo_sha,
        "original_repo_sha": (
            str(child["request"].get("original_repo_sha") or expected_repo_sha)
            if action == ACTION_RECOVERY
            else expected_repo_sha
        ),
        "recovery_scope": str(child["request"].get("recovery_scope") or ""),
        "recovery_kind": str(child["request"].get("recovery_kind") or ""),
        "incident_id": str(child["request"].get("incident_id") or ""),
        "incident_path": str(child["request"].get("incident_path") or ""),
        "incident_file_sha256": str(
            child["request"].get("incident_file_sha256") or ""
        ),
        "original_admission_file_sha256": str(
            child["request"].get("original_admission_file_sha256") or ""
        ),
        "campaign_authorization_path": str(
            child["request"].get("campaign_authorization_path") or ""
        ),
        "campaign_authorization_file_sha256": str(
            child["request"].get("campaign_authorization_file_sha256") or ""
        ),
        "target_campaign_instance_id": str(
            child["request"].get("target_campaign_instance_id") or ""
        ),
        "target_campaign_profile": str(
            child["request"].get("target_campaign_profile") or ""
        ),
    }


def _clean_repository_head(repository_path: Path) -> str:
    status = subprocess.run(
        ["git", "-C", str(repository_path), "status", "--porcelain", "--untracked-files=all"],
        check=True,
        capture_output=True,
        text=True,
    )
    if status.stdout.strip():
        raise ProjectControlDenied(
            "high-cost route requires the canonical repository to be clean"
        )
    completed = subprocess.run(
        ["git", "-C", str(repository_path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _write_exclusive_json(path: Path, payload: dict[str, Any], label: str) -> None:
    try:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    except FileExistsError as exc:
        raise ProjectControlDenied(f"{label} already consumed") from exc


def _durable_control_root(output_root: Path) -> Path:
    return output_root.resolve() / ".project_control_execution"


def _consume_admission_durably(proof: dict[str, Any]) -> None:
    output_root = Path(str(proof["target_output_root"]))
    control_root = _durable_control_root(output_root)
    identity_path = control_root / "execution_identity.json"
    action = str(proof["requested_action"])
    if action == ACTION_RECOVERY:
        if not output_root.is_dir() or not identity_path.is_file():
            raise ProjectControlDenied("recovery target has no original execution identity")
        _, identity = _read_json(identity_path, "original execution identity")
        if (
            identity.get("project_id") != proof["project_id"]
            or identity.get("repo_sha") != proof["original_repo_sha"]
            or identity.get("target_campaign_id") != proof["target_campaign_id"]
            or identity.get("target_run_id") != proof["target_run_id"]
            or identity.get("root_admission_file_sha256")
            != proof["original_admission_file_sha256"]
            or identity.get("root_action") != proof["execution_lineage_action"]
            or identity.get("campaign_authorization_file_sha256")
            != proof["campaign_authorization_file_sha256"]
            or identity.get("target_campaign_instance_id")
            != proof["target_campaign_instance_id"]
            or identity.get("target_campaign_profile")
            != proof["target_campaign_profile"]
        ):
            raise ProjectControlDenied("recovery original execution identity drift")
    else:
        campaign_id = str(proof["target_campaign_id"])
        prepared_allowlist = PREPARED_OUTPUT_ROOT_ALLOWLISTS.get(campaign_id)
        if output_root.exists():
            observed = (
                {path.name for path in output_root.iterdir()}
                if output_root.is_dir()
                else set()
            )
            if (
                prepared_allowlist is None
                or "deployment_binding.json" not in observed
                or not observed.issubset(prepared_allowlist)
            ):
                raise ProjectControlDenied(
                    "new execution requires a fresh or control-metadata-only "
                    "admitted output root"
                )
        else:
            try:
                output_root.mkdir(parents=True, exist_ok=False)
            except FileExistsError as exc:
                raise ProjectControlDenied(
                    "new execution requires a fresh admitted output root"
                ) from exc
        try:
            control_root.mkdir(exist_ok=False)
        except FileExistsError as exc:
            raise ProjectControlDenied(
                "target output identity is already claimed"
            ) from exc
        _write_exclusive_json(
            identity_path,
            {
                "schema_version": "cn_project_control_execution_identity_v1",
                "project_id": proof["project_id"],
                "repo_sha": proof["repo_sha"],
                "target_campaign_id": proof["target_campaign_id"],
                "target_run_id": proof["target_run_id"],
                "target_output_root": proof["target_output_root"],
                "root_action": action,
                "root_admission_file_sha256": proof["admission_file_sha256"],
                "campaign_authorization_file_sha256": proof[
                    "campaign_authorization_file_sha256"
                ],
                "target_campaign_instance_id": proof[
                    "target_campaign_instance_id"
                ],
                "target_campaign_profile": proof["target_campaign_profile"],
            },
            "execution identity",
        )
    consumption_root = control_root / "consumptions"
    consumption_root.mkdir(exist_ok=True)
    _write_exclusive_json(
        consumption_root / f"{proof['admission_payload_sha256']}.json",
        {
            "schema_version": "cn_project_control_admission_consumption_v1",
            "consumed_at": datetime.now(timezone.utc).isoformat(),
            "admission_file_sha256": proof["admission_file_sha256"],
            "admission_payload_sha256": proof["admission_payload_sha256"],
            "requested_action": action,
            "target_campaign_id": proof["target_campaign_id"],
            "target_run_id": proof["target_run_id"],
            "target_output_root": proof["target_output_root"],
            "execution_lineage_action": proof["execution_lineage_action"],
            "repo_sha": proof["repo_sha"],
            "original_repo_sha": proof["original_repo_sha"],
            "recovery_scope": proof["recovery_scope"],
            "campaign_authorization_file_sha256": proof[
                "campaign_authorization_file_sha256"
            ],
            "target_campaign_instance_id": proof["target_campaign_instance_id"],
            "target_campaign_profile": proof["target_campaign_profile"],
        },
        "Project Control admission",
    )


def activate_admission(
    path: Path,
    *,
    expected_admission_file_sha256: str,
    expected_actions: Collection[str],
    expected_target_campaign_id: str,
    expected_target_run_id: str,
    expected_target_output_root: str | Path,
) -> dict[str, Any]:
    """Atomically validate and activate one exact execution admission."""

    global _ACTIVE_ADMISSION
    if _ACTIVE_ADMISSION is not None:
        raise ProjectControlDenied("another high-cost admission is already active")
    repository_path = REPO_ROOT.resolve()
    load_trust_config(
        CANONICAL_TRUST_CONFIG,
        runtime_repository_path=repository_path,
    )
    expected_repo_sha = _clean_repository_head(repository_path)
    proof = validate_admission(
        path,
        trust_config_path=CANONICAL_TRUST_CONFIG,
        expected_admission_file_sha256=expected_admission_file_sha256,
        expected_project_id=PROJECT_ID,
        expected_repo_sha=expected_repo_sha,
        expected_actions=expected_actions,
        expected_target_campaign_id=expected_target_campaign_id,
        expected_target_run_id=expected_target_run_id,
        expected_target_output_root=expected_target_output_root,
        _runtime_repository_path=repository_path,
    )
    _consume_admission_durably(proof)
    _ACTIVE_ADMISSION = dict(proof)
    return proof


def consume_active_admission(
    expected_campaign_id: str,
    expected_actions: Collection[str],
) -> dict[str, Any]:
    global _ACTIVE_ADMISSION
    proof = _ACTIVE_ADMISSION
    _ACTIVE_ADMISSION = None
    if proof is None:
        raise ProjectControlDenied("DIRECT_HIGH_COST_MODULE_EXECUTION_FORBIDDEN")
    if proof.get("target_campaign_id") != expected_campaign_id:
        raise ProjectControlDenied("active admission campaign drift")
    if proof.get("requested_action") not in set(expected_actions):
        raise ProjectControlDenied("active admission action drift")
    return proof


def verify_consumed_admission_target(
    proof: dict[str, Any], *, output_root: str | Path
) -> None:
    """Bind parsed route arguments to the already-consumed execution proof."""

    if proof.get("status") != "PROJECT_CONTROL_ADMISSION_ELIGIBLE":
        raise ProjectControlDenied("consumed admission proof is ineligible")
    if proof.get("target_output_root") != _canonical_output_root(output_root):
        raise ProjectControlDenied("active admission output root drift")


def verify_campaign_authorization_binding(
    proof: Mapping[str, Any], authorization_path: str | Path
) -> VerifiedCampaignAuthorization:
    """Bind the live campaign authorization to the reviewed request payload."""

    resolved = Path(authorization_path).expanduser().resolve()
    try:
        raw = resolved.read_bytes()
        observed_file_sha256 = hashlib.sha256(raw).hexdigest()
        authorization = json.loads(raw.decode("utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProjectControlDenied(
            "campaign authorization is unreadable before financial execution"
        ) from exc
    if not isinstance(authorization, dict):
        raise ProjectControlDenied("campaign authorization is invalid")
    if (
        str(proof.get("campaign_authorization_path") or "") != str(resolved)
        or str(proof.get("campaign_authorization_file_sha256") or "")
        != observed_file_sha256
        or str(proof.get("target_campaign_instance_id") or "")
        != str(authorization.get("campaign_id") or "")
        or str(proof.get("target_campaign_profile") or "")
        != str(authorization.get("campaign_profile") or "")
    ):
        raise ProjectControlDenied(
            "campaign authorization is outside the reviewed request"
        )
    return VerifiedCampaignAuthorization(
        path=resolved,
        file_sha256=observed_file_sha256,
        byte_count=len(raw),
        payload=dict(authorization),
    )


def clear_active_admission() -> None:
    global _ACTIVE_ADMISSION
    _ACTIVE_ADMISSION = None
