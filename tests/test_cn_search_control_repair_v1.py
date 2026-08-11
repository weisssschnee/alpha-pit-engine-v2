from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import pytest

import app as repo_app
from our_system_phase2.runtime.cn_large_tpe_search_campaign import (
    require_project_control_action_for_campaign_authorization,
)
from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    require_project_control_action_for_campaign_authorization
    as require_targeted_project_control_action,
)
from our_system_phase2.services import project_control_admission as project_control
from our_system_phase2.services.development_feedback_provenance import (
    build_development_feedback_provenance,
)
from our_system_phase2.services.evaluation_asset_authority import (
    PERMANENT_DENY_ALREADY_SPENT,
    EvaluationAssetDenied,
    verify_historical_challenge_destructive_use,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_FREEZE,
    ACTION_LAUNCH,
    ACTION_RECOVERY,
    ACTION_RETRY,
    ACTION_SUCCESSOR,
    PROJECT_ID,
    TECHNICAL_RECOVERY,
    TRUST_SCHEMA_VERSION,
    ProjectControlDenied,
    activate_admission,
    build_execution_request,
    consume_active_admission,
    materialize_admission,
    project_control_receipt_sha256,
    sha256_file,
    validate_admission,
    verify_campaign_authorization_binding,
    verify_consumed_admission_target,
)
from scripts.materialize_cn_search_preflight_authorization import (
    authorization_bytes,
    materialize_authorization,
    planned_authorization_binding,
)


REPO = Path(__file__).resolve().parents[1]
REPO_SHA = "a" * 40
PARENT_SHA = "b" * 40


def _stable_hash(payload: dict) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _trust_config(root: Path) -> Path:
    trusted_root = root / "trusted-runs"
    trusted_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": TRUST_SCHEMA_VERSION,
        "project_id": PROJECT_ID,
        "repository_path": str(REPO),
        "trusted_harness_runs_root": str(trusted_root),
        "trust_model": "HARNESS_RUNS_ROOT_IS_AUTHORITY_STORE",
        "cryptographic_receipt_signature": "UNAVAILABLE_IN_EXISTING_HARNESS",
    }
    payload["trust_payload_sha256"] = _stable_hash(payload)
    return _write_json(root / "trust.json", payload)


def _request(
    *,
    action: str,
    campaign_id: str,
    target_run_id: str,
    target_output_root: Path,
    repo_sha: str,
    expires_at: str = "2099-01-01T00:00:00+00:00",
    campaign_authorization_path: str = "",
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
    incident_id: str = "",
    incident_path: str = "",
    incident_file_sha256: str = "",
) -> dict:
    return build_execution_request(
        requested_action=action,
        target_campaign_id=campaign_id,
        target_run_id=target_run_id,
        target_output_root=target_output_root,
        repo_sha=repo_sha,
        expires_at=expires_at,
        campaign_authorization_path=campaign_authorization_path,
        campaign_authorization_file_sha256=(
            campaign_authorization_file_sha256
        ),
        target_campaign_instance_id=target_campaign_instance_id,
        target_campaign_profile=target_campaign_profile,
        parent_project_control_run_id=parent_project_control_run_id,
        parent_target_campaign_id=parent_target_campaign_id,
        parent_target_run_id=parent_target_run_id,
        recovery_kind=recovery_kind,
        recovery_of_target_run_id=recovery_of_target_run_id,
        original_admission_path=original_admission_path,
        original_admission_file_sha256=original_admission_file_sha256,
        incident_id=incident_id,
        incident_path=incident_path,
        incident_file_sha256=incident_file_sha256,
    )["execution_request"]


def _run_record(
    root: Path,
    *,
    trust_config: Path,
    run_id: str,
    action: str = ACTION_LAUNCH,
    campaign_id: str = "cn-large-tpe-search-campaign",
    target_run_id: str = "target-1",
    target_output_root: Path | None = None,
    repo_sha: str = REPO_SHA,
    preflight_verdict: str = "PROCEED",
    execution_allowed: bool = True,
    post_batch_verdict: str | None = None,
    continuation_allowed: bool | None = None,
    expires_at: str = "2099-01-01T00:00:00+00:00",
    **request_fields: str,
) -> Path:
    trust = json.loads(trust_config.read_text(encoding="utf-8"))
    if (
        campaign_id
        in {
            "cn-large-tpe-search-campaign",
            "cn-targeted-search-medium-campaign",
        }
        and "campaign_authorization_path" not in request_fields
    ):
        if campaign_id == "cn-targeted-search-medium-campaign":
            profile = "slow_cross_sectional_evaluated384"
        elif action == ACTION_SUCCESSOR:
            profile = "cn_full_compute_successor_search_v1"
        else:
            profile = "cn_winner_guided_large_search_v1"
        campaign_instance_id = f"synthetic-{target_run_id}"
        authorization_path = _write_json(
            root
            / "synthetic-campaign-authorizations"
            / f"{target_run_id}.json",
            {
                "campaign_id": campaign_instance_id,
                "campaign_profile": profile,
            },
        ).resolve()
        request_fields.update(
            {
                "campaign_authorization_path": str(authorization_path),
                "campaign_authorization_file_sha256": sha256_file(
                    authorization_path
                ),
                "target_campaign_instance_id": campaign_instance_id,
                "target_campaign_profile": profile,
            }
        )
    request = _request(
        action=action,
        campaign_id=campaign_id,
        target_run_id=target_run_id,
        target_output_root=(
            target_output_root or (root / f"output-{target_run_id}")
        ).resolve(),
        repo_sha=repo_sha,
        expires_at=expires_at,
        **request_fields,
    )
    task_id = f"cn-execution-{request['request_payload_sha256'][:24]}"
    run_root = Path(trust["trusted_harness_runs_root"]) / task_id / run_id
    task = {
        "schema_version": 1,
        "task_id": task_id,
        "project_id": PROJECT_ID,
        "objective": "synthetic bound execution request",
        "background": "test",
        "in_scope": [],
        "out_of_scope": [],
        "constraints": [],
        "expected_artifacts": [],
        "acceptance_checks": [],
        "risk_level": "high",
        "execution_mode": "read-only",
        "stop_conditions": [],
        "execution_request": request,
    }
    profile = {
        "schema_version": 1,
        "project_id": PROJECT_ID,
        "repository_path": str(REPO),
        "primary_branch": "feature/a-share-tradability-authority-repair",
        "project_instructions": ["AGENTS.md"],
        "architecture_docs": [],
        "setup_commands": [],
        "test_commands": [],
        "build_commands": [],
        "smoke_test_commands": [],
        "allowed_paths": ["**"],
        "protected_paths": [],
        "sensitive_files": [],
        "environment_requirements": {},
        "git_policy": {},
        "worktree_policy": {},
    }
    receipts = [
        {
            "phase": "PREFLIGHT",
            "verdict": preflight_verdict,
            "current_bottleneck": "search control drift",
            "reason_codes": ["AUTHORITY_REPAIR"],
            "evidence_references": [],
            "next_required_decision": "" if preflight_verdict == "PROCEED" else "stop",
            "recorded_at": "2026-08-11T00:00:00+00:00",
            "addresses_bottleneck": "YES",
            "expected_decision_change": "physical execution admission",
            "stop_condition": "any binding drift",
        }
    ]
    if post_batch_verdict is not None:
        receipts.append(
            {
                "phase": "POST_BATCH",
                "verdict": post_batch_verdict,
                "current_bottleneck": "search control drift",
                "reason_codes": ["BATCH_REVIEW"],
                "evidence_references": ["synthetic-evidence.json"],
                "next_required_decision": (
                    "" if post_batch_verdict == "CONTINUE" else "stop"
                ),
                "recorded_at": "2026-08-11T01:00:00+00:00",
                "deltas": {
                    "decision": "YES",
                    "bottleneck": "YES",
                    "frontier": "NO",
                    "capability": "YES",
                    "information": "HIGH",
                },
                "budget_burn": "synthetic",
            }
        )
    record = {
        "schema_version": 1,
        "run_id": run_id,
        "task_id": task_id,
        "project_id": PROJECT_ID,
        "session_id": "synthetic-control-review",
        "code_base_sha": repo_sha,
        "worktree": {
            "path": str(root / "synthetic-worktree"),
            "branch": "synthetic",
            "source_worktree": str(REPO),
        },
        "automatic_execution_allowed": execution_allowed,
        "automatic_continuation_allowed": continuation_allowed,
        "project_control": receipts,
        "started_at": "2026-08-11T00:00:00+00:00",
        "finished_at": None,
        "status": "ready",
        "commands_executed": [],
        "files_changed": [],
        "checks_executed": [],
        "evidence_references": [],
        "failure_class": None,
        "handoff_reference": None,
    }
    _write_json(run_root / "task_spec.json", task)
    _write_json(run_root / "project_profile.json", profile)
    (run_root / "execution_context.md").write_text(
        "# Synthetic Harness execution context\n", encoding="utf-8"
    )
    return _write_json(run_root / "run_record.json", record)


def _asset_authority(
    root: Path,
    *,
    current_role: str,
    access_role: str,
    reads: int,
) -> tuple[Path, Path, Path]:
    registry = _write_json(
        root / "roles.json",
        {
            "default_deny": True,
            "asset_states": {
                "historical_challenge_2023_b05e2ca0": {
                    "current_role": current_role,
                    "performance_rows_read": reads,
                },
                "forward_b_tdx_lc1_20260413_20260514_f69cc84f": {
                    "current_role": "forward",
                    "performance_rows_read": 0,
                },
            },
        },
    )
    access = _write_json(
        root / "access.json",
        {
            "status": "SPENT" if access_role == "spent" else "UNOPENED",
            "asset_id": "historical_challenge_2023_b05e2ca0",
            "data_role_after_transition": access_role,
        },
    )
    outcome = _write_json(
        root / "outcome.json",
        {
            "access_decision": {
                "historical_challenge_2023_state": "SPENT" if reads else "UNOPENED",
                "forward_b_state": "SEALED",
                "forward_b_access": "NOT_AUTHORIZED",
            },
            "provenance": {"historical_challenge_reads": reads},
        },
    )
    return registry, access, outcome


def _materialize(
    root: Path,
    *,
    trust_config: Path,
    action: str,
    child: Path,
    campaign_id: str = "cn-large-tpe-search-campaign",
    target_run_id: str = "target-1",
    parent: Path | None = None,
) -> Path:
    output = root / f"admission-{action.lower()}.json"
    materialize_admission(
        output_path=output,
        trust_config_path=trust_config,
        project_control_run_record_path=child,
        expected_project_control_receipt_sha256=project_control_receipt_sha256(
            child,
            "PREFLIGHT",
            trust_config_path=trust_config,
        ),
        requested_action=action,
        target_campaign_id=campaign_id,
        target_run_id=target_run_id,
        repo_sha=REPO_SHA,
        parent_post_batch_run_record_path=parent,
        expected_parent_post_batch_receipt_sha256=(
            project_control_receipt_sha256(
                parent,
                "POST_BATCH",
                trust_config_path=trust_config,
            )
            if parent is not None
            else ""
        ),
    )
    return output


def _validate(
    path: Path,
    *,
    trust_config: Path,
    action: str,
    target_run_id: str = "target-1",
) -> dict:
    target_output_root = json.loads(path.read_text(encoding="utf-8"))[
        "target_output_root"
    ]
    return validate_admission(
        path,
        trust_config_path=trust_config,
        expected_admission_file_sha256=sha256_file(path),
        expected_project_id=PROJECT_ID,
        expected_repo_sha=REPO_SHA,
        expected_actions={action},
        expected_target_campaign_id="cn-large-tpe-search-campaign",
        expected_target_run_id=target_run_id,
        expected_target_output_root=target_output_root,
    )


def test_unopened_synthetic_asset_reaches_pre_execution_check(tmp_path: Path) -> None:
    registry, access, outcome = _asset_authority(
        tmp_path, current_role="challenge", access_role="challenge", reads=0
    )
    proof = verify_historical_challenge_destructive_use(
        role_registry_path=registry,
        access_started_path=access,
        outcome_path=outcome,
    )
    assert proof["status"] == "ELIGIBLE_UNOPENED_SYNTHETIC"
    assert proof["forward_b_access"] == "NOT_AUTHORIZED"


def test_spent_asset_is_permanently_denied_with_fresh_output_root(
    tmp_path: Path,
) -> None:
    registry, access, outcome = _asset_authority(
        tmp_path, current_role="spent", access_role="spent", reads=2_383_217
    )
    fresh_output_root = tmp_path / "fresh-output"
    with pytest.raises(EvaluationAssetDenied, match=PERMANENT_DENY_ALREADY_SPENT):
        verify_historical_challenge_destructive_use(
            role_registry_path=registry,
            access_started_path=access,
            outcome_path=outcome,
        )
    assert not fresh_output_root.exists()


def test_missing_untrusted_nonproceed_and_expired_preflight_deny(
    tmp_path: Path,
) -> None:
    trust = _trust_config(tmp_path)
    missing = tmp_path / "missing.json"
    with pytest.raises(ProjectControlDenied, match="path missing"):
        materialize_admission(
            output_path=tmp_path / "missing-admission.json",
            trust_config_path=trust,
            project_control_run_record_path=missing,
            expected_project_control_receipt_sha256="0" * 64,
            requested_action=ACTION_LAUNCH,
            target_campaign_id="cn-large-tpe-search-campaign",
            target_run_id="target-1",
            repo_sha=REPO_SHA,
        )

    outside = _write_json(tmp_path / "outside.json", {"project_control": []})
    with pytest.raises(ProjectControlDenied, match="outside trusted runs root"):
        project_control_receipt_sha256(
            outside,
            "PREFLIGHT",
            trust_config_path=trust,
        )

    paused = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="paused",
        preflight_verdict="PAUSE",
        execution_allowed=False,
    )
    with pytest.raises(ProjectControlDenied, match="PREFLIGHT_PROCEED_REQUIRED"):
        _materialize(
            tmp_path / "paused",
            trust_config=trust,
            action=ACTION_LAUNCH,
            child=paused,
        )

    with pytest.raises(ProjectControlDenied, match="STALE"):
        _run_record(
            tmp_path,
            trust_config=trust,
            run_id="expired",
            expires_at="2020-01-01T00:00:00+00:00",
        )


@pytest.mark.parametrize("action", [ACTION_FREEZE, ACTION_LAUNCH, ACTION_RETRY])
def test_target_bound_preflight_proceed_is_eligible(
    tmp_path: Path, action: str
) -> None:
    trust = _trust_config(tmp_path)
    child = _run_record(
        tmp_path,
        trust_config=trust,
        run_id=f"child-{action}",
        action=action,
    )
    admission = _materialize(
        tmp_path,
        trust_config=trust,
        action=action,
        child=child,
    )
    proof = _validate(admission, trust_config=trust, action=action)
    assert proof["requested_action"] == action


def test_successor_requires_matching_parent_lineage_and_continue(
    tmp_path: Path,
) -> None:
    trust = _trust_config(tmp_path)
    stopped = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="parent-stopped",
        action=ACTION_LAUNCH,
        campaign_id="parent-campaign",
        target_run_id="parent-run",
        repo_sha=PARENT_SHA,
        post_batch_verdict="PAUSE",
        continuation_allowed=False,
    )
    child = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="child-successor",
        action=ACTION_SUCCESSOR,
        parent_project_control_run_id="parent-stopped",
        parent_target_campaign_id="parent-campaign",
        parent_target_run_id="parent-run",
    )
    with pytest.raises(ProjectControlDenied, match="POST_BATCH_CONTINUE_REQUIRED"):
        _materialize(
            tmp_path / "stopped",
            trust_config=trust,
            action=ACTION_SUCCESSOR,
            child=child,
            parent=stopped,
        )

    continued = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="parent-continued",
        action=ACTION_LAUNCH,
        campaign_id="parent-campaign",
        target_run_id="parent-run",
        repo_sha=PARENT_SHA,
        post_batch_verdict="CONTINUE",
        continuation_allowed=True,
    )
    with pytest.raises(ProjectControlDenied, match="parent lineage drift"):
        _materialize(
            tmp_path / "unrelated",
            trust_config=trust,
            action=ACTION_SUCCESSOR,
            child=child,
            parent=continued,
        )

    bound_child = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="bound-child",
        action=ACTION_SUCCESSOR,
        parent_project_control_run_id="parent-continued",
        parent_target_campaign_id="parent-campaign",
        parent_target_run_id="parent-run",
    )
    admission = _materialize(
        tmp_path / "continued",
        trust_config=trust,
        action=ACTION_SUCCESSOR,
        child=bound_child,
        parent=continued,
    )
    assert _validate(
        admission,
        trust_config=trust,
        action=ACTION_SUCCESSOR,
    )["parent_run_id"] == "parent-continued"


def test_recovery_requires_original_admission_same_run_and_incident(
    monkeypatch, tmp_path: Path,
) -> None:
    trust = _trust_config(tmp_path)
    original_control = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="original-control",
        action=ACTION_LAUNCH,
        target_run_id="immutable-run-1",
    )
    original_admission = _materialize(
        tmp_path / "original",
        trust_config=trust,
        action=ACTION_LAUNCH,
        child=original_control,
        target_run_id="immutable-run-1",
    )
    incident = _write_json(
        tmp_path / "incident.json",
        {"incident_id": "incident-checkpoint-055", "status": "OPEN"},
    )
    recovery_control = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="recovery-control",
        action=ACTION_RECOVERY,
        target_run_id="immutable-run-1",
        recovery_kind=TECHNICAL_RECOVERY,
        recovery_of_target_run_id="immutable-run-1",
        original_admission_path=str(original_admission),
        original_admission_file_sha256=sha256_file(original_admission),
        incident_id="incident-checkpoint-055",
        incident_path=str(incident),
        incident_file_sha256=sha256_file(incident),
    )
    recovery = _materialize(
        tmp_path / "recovery",
        trust_config=trust,
        action=ACTION_RECOVERY,
        child=recovery_control,
        target_run_id="immutable-run-1",
    )
    proof = _validate(
        recovery,
        trust_config=trust,
        action=ACTION_RECOVERY,
        target_run_id="immutable-run-1",
    )
    assert proof["incident_id"] == "incident-checkpoint-055"

    drift = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="drift-recovery",
        action=ACTION_RECOVERY,
        target_run_id="new-run",
        recovery_kind=TECHNICAL_RECOVERY,
        recovery_of_target_run_id="new-run",
        original_admission_path=str(original_admission),
        original_admission_file_sha256=sha256_file(original_admission),
        incident_id="incident-checkpoint-055",
        incident_path=str(incident),
        incident_file_sha256=sha256_file(incident),
    )
    with pytest.raises(ProjectControlDenied, match="target run drift"):
        _materialize(
            tmp_path / "drift",
            trust_config=trust,
            action=ACTION_RECOVERY,
            child=drift,
            target_run_id="new-run",
        )

    target_output_root = Path(
        json.loads(original_admission.read_text(encoding="utf-8"))[
            "target_output_root"
        ]
    )
    monkeypatch.setattr(project_control, "CANONICAL_TRUST_CONFIG", trust)
    monkeypatch.setattr(
        project_control, "_clean_repository_head", lambda _path: REPO_SHA
    )
    original_proof = activate_admission(
        original_admission,
        expected_admission_file_sha256=sha256_file(original_admission),
        expected_actions={ACTION_LAUNCH},
        expected_target_campaign_id="cn-large-tpe-search-campaign",
        expected_target_run_id="immutable-run-1",
        expected_target_output_root=target_output_root,
    )
    consume_active_admission(
        "cn-large-tpe-search-campaign", {ACTION_LAUNCH}
    )
    project_control.clear_active_admission()
    assert original_proof["target_output_root"] == str(target_output_root)

    recovery_proof = activate_admission(
        recovery,
        expected_admission_file_sha256=sha256_file(recovery),
        expected_actions={ACTION_RECOVERY},
        expected_target_campaign_id="cn-large-tpe-search-campaign",
        expected_target_run_id="immutable-run-1",
        expected_target_output_root=target_output_root,
    )
    consume_active_admission(
        "cn-large-tpe-search-campaign", {ACTION_RECOVERY}
    )
    project_control.clear_active_admission()
    assert recovery_proof["incident_id"] == "incident-checkpoint-055"
    with pytest.raises(ProjectControlDenied, match="already consumed"):
        activate_admission(
            recovery,
            expected_admission_file_sha256=sha256_file(recovery),
            expected_actions={ACTION_RECOVERY},
            expected_target_campaign_id="cn-large-tpe-search-campaign",
            expected_target_run_id="immutable-run-1",
            expected_target_output_root=target_output_root,
        )
    reformatted_recovery = tmp_path / "recovery-reformatted.json"
    reformatted_recovery.write_text(
        json.dumps(
            json.loads(recovery.read_text(encoding="utf-8")),
            ensure_ascii=False,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    assert sha256_file(reformatted_recovery) != sha256_file(recovery)
    with pytest.raises(ProjectControlDenied, match="already consumed"):
        activate_admission(
            reformatted_recovery,
            expected_admission_file_sha256=sha256_file(reformatted_recovery),
            expected_actions={ACTION_RECOVERY},
            expected_target_campaign_id="cn-large-tpe-search-campaign",
            expected_target_run_id="immutable-run-1",
            expected_target_output_root=target_output_root,
        )


def test_receipt_action_project_run_and_repo_binding_drift_denies(
    tmp_path: Path,
) -> None:
    trust = _trust_config(tmp_path)
    child = _run_record(tmp_path, trust_config=trust, run_id="child")
    with pytest.raises(ProjectControlDenied, match="receipt hash drift"):
        materialize_admission(
            output_path=tmp_path / "bad.json",
            trust_config_path=trust,
            project_control_run_record_path=child,
            expected_project_control_receipt_sha256="f" * 64,
            requested_action=ACTION_LAUNCH,
            target_campaign_id="cn-large-tpe-search-campaign",
            target_run_id="target-1",
            repo_sha=REPO_SHA,
        )
    admission = _materialize(
        tmp_path,
        trust_config=trust,
        action=ACTION_LAUNCH,
        child=child,
    )
    for kwargs, pattern in (
        ({"expected_project_id": "another-project"}, "trust project drift"),
        ({"expected_repo_sha": "c" * 40}, "repo SHA drift"),
        ({"expected_actions": {ACTION_RETRY}}, "requested action drift"),
        ({"expected_target_run_id": "another-run"}, "target run drift"),
    ):
        expected = {
            "trust_config_path": trust,
            "expected_admission_file_sha256": sha256_file(admission),
            "expected_project_id": PROJECT_ID,
            "expected_repo_sha": REPO_SHA,
            "expected_actions": {ACTION_LAUNCH},
            "expected_target_campaign_id": "cn-large-tpe-search-campaign",
            "expected_target_run_id": "target-1",
            "expected_target_output_root": json.loads(
                admission.read_text(encoding="utf-8")
            )["target_output_root"],
        }
        expected.update(kwargs)
        with pytest.raises(ProjectControlDenied, match=pattern):
            validate_admission(admission, **expected)


def test_false_serialized_state_does_not_erase_observation_feedback() -> None:
    provenance = build_development_feedback_provenance(
        serialized_optimizer_state_imported=False,
        development_financial_observations_imported=True,
        development_observation_count=51,
        candidate_results_imported=True,
        factor_statistics_imported=False,
        behavior_statistics_imported=True,
        template_classification_imported=True,
        manual_diagnosis_imported=False,
        objective_designed_after_parent_results=False,
    )
    assert provenance["serialized_optimizer_state_imported"] is False
    assert provenance["development_observation_count"] == 51
    assert provenance["cross_campaign_development_feedback"] is True


def test_no_feedback_provenance_requires_zero_observation_count() -> None:
    with pytest.raises(ValueError, match="observation count"):
        build_development_feedback_provenance(
            serialized_optimizer_state_imported=False,
            development_financial_observations_imported=False,
            development_observation_count=1,
            candidate_results_imported=False,
            factor_statistics_imported=False,
            behavior_statistics_imported=False,
            template_classification_imported=False,
            manual_diagnosis_imported=False,
            objective_designed_after_parent_results=False,
        )


def test_2023_launcher_denies_before_archive_hash_or_output_creation() -> None:
    launcher = (
        REPO / "scripts" / "run_cn_fixed_survivor_historical_challenge_2023_77o.ps1"
    ).read_text(encoding="utf-8")
    gate = launcher.index("verify_cn_historical_challenge_admission.py")
    archive_hash = launcher.index("$archiveHash = Get-SharedReadSha256")
    output_create = launcher.index("New-Item -ItemType Directory -Path $resolvedRoot")
    conversion = launcher.index("convert_cn_yearly_1min_zip_to_session_shards.py")
    assert gate < archive_hash < output_create < conversion


def test_every_high_cost_module_has_in_process_admission_gate() -> None:
    assert set(repo_app.HIGH_COST_ROUTE_ACTIONS) == {
        "phase3cp-real-cm-small-loop",
        "phase3cf-large-search-prelaunch",
        "nextgen-dark-development-canary",
        "cn-b1s-development-canary",
        "cn-iterative-search-v1-canary",
        "cn-targeted-search-medium-campaign",
        "cn-large-tpe-search-campaign",
        "cn-fixed-stratified-production-v0",
        "cn-joint-program-search-v2-canary",
    }
    assert repo_app.HIGH_COST_ROUTE_ACTIONS["phase3cf-large-search-prelaunch"] == {
        ACTION_FREEZE
    }
    assert all(
        ACTION_FREEZE not in actions
        for route, actions in repo_app.HIGH_COST_ROUTE_ACTIONS.items()
        if route != "phase3cf-large-search-prelaunch"
    )
    for route, module_path in repo_app.ROUTES.items():
        if route not in repo_app.HIGH_COST_ROUTE_ACTIONS:
            continue
        source_path = REPO / "src" / (module_path.replace(".", "/") + ".py")
        source = source_path.read_text(encoding="utf-8")
        module = ast.parse(source)
        main = next(
            node
            for node in module.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "main"
        )
        first = (
            main.body[2]
            if route == "cn-iterative-search-v1-canary"
            else main.body[0]
        )
        assert isinstance(first, ast.Assign)
        assert isinstance(first.value, ast.Call)
        assert isinstance(first.value.func, ast.Name)
        assert first.value.func.id == "consume_active_admission"
        assert ast.literal_eval(first.value.args[0]) == route
        parse_index = next(
            index
            for index, statement in enumerate(main.body)
            if isinstance(statement, ast.Assign)
            and isinstance(statement.value, ast.Call)
            and isinstance(statement.value.func, ast.Attribute)
            and statement.value.func.attr == "parse_args"
        )
        target_check = main.body[parse_index + 1]
        assert isinstance(target_check, ast.Expr)
        assert isinstance(target_check.value, ast.Call)
        assert isinstance(target_check.value.func, ast.Name)
        assert target_check.value.func.id == "verify_consumed_admission_target"

    with pytest.raises(
        ProjectControlDenied, match="DIRECT_HIGH_COST_MODULE_EXECUTION_FORBIDDEN"
    ):
        consume_active_admission(
            "cn-large-tpe-search-campaign", {ACTION_LAUNCH}
        )

    with pytest.raises(TypeError):
        activate_admission(  # type: ignore[call-arg]
            {"status": "PROJECT_CONTROL_ADMISSION_ELIGIBLE"}
        )


def test_high_cost_entry_denies_before_route_import(monkeypatch) -> None:
    imported = False

    def forbidden_import(_route: str):
        nonlocal imported
        imported = True
        raise AssertionError("route imported before admission")

    monkeypatch.setattr(repo_app, "_load_main", forbidden_import)
    with pytest.raises(SystemExit) as exc:
        repo_app.main(["phase3cp-real-cm-small-loop"])
    assert exc.value.code == 2
    assert imported is False


def test_activation_binds_code_derived_deployment_repo_and_store(
    monkeypatch, tmp_path: Path
) -> None:
    remote_repo = (tmp_path / "remote" / "workspace" / "deployed-repo").resolve()
    remote_repo.mkdir(parents=True)
    remote_store = (tmp_path / "remote" / "authority-store").resolve()
    remote_store.mkdir(parents=True)
    payload = {
        "schema_version": TRUST_SCHEMA_VERSION,
        "project_id": PROJECT_ID,
        "repository_path": str(REPO),
        "trusted_harness_runs_root": str(remote_store),
        "deployments": [
            {
                "deployment_id": "local-audit",
                "repository_path": str(REPO),
                "trusted_harness_runs_root": str(remote_store),
            },
            {
                "deployment_id": "remote-validation",
                "repository_root_prefix": str(remote_repo.parent),
                "trusted_harness_runs_root": str(remote_store),
            },
        ],
        "trust_model": "HARNESS_RUNS_ROOT_IS_AUTHORITY_STORE",
        "cryptographic_receipt_signature": "UNAVAILABLE_IN_EXISTING_HARNESS",
    }
    payload["trust_payload_sha256"] = _stable_hash(payload)
    trust = _write_json(tmp_path / "deployment-trust.json", payload)
    target_output_root = (tmp_path / "target-output").resolve()
    child = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="deployment-control",
        target_output_root=target_output_root,
    )
    admission = _materialize(
        tmp_path,
        trust_config=trust,
        action=ACTION_LAUNCH,
        child=child,
    )
    observed_repositories: list[Path] = []

    def clean_head(path: Path) -> str:
        observed_repositories.append(path.resolve())
        return REPO_SHA

    monkeypatch.setattr(project_control, "CANONICAL_TRUST_CONFIG", trust)
    monkeypatch.setattr(project_control, "REPO_ROOT", remote_repo)
    monkeypatch.setattr(project_control, "_clean_repository_head", clean_head)
    activate_admission(
        admission,
        expected_admission_file_sha256=sha256_file(admission),
        expected_actions={ACTION_LAUNCH},
        expected_target_campaign_id="cn-large-tpe-search-campaign",
        expected_target_run_id="target-1",
        expected_target_output_root=target_output_root,
    )
    consume_active_admission(
        "cn-large-tpe-search-campaign", {ACTION_LAUNCH}
    )
    project_control.clear_active_admission()

    assert observed_repositories == [remote_repo]


def test_high_cost_entry_consumes_valid_target_bound_admission(
    monkeypatch, tmp_path: Path
) -> None:
    trust = _trust_config(tmp_path)
    target_output_root = (tmp_path / "target-output").resolve()
    child = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="entry-control",
        target_output_root=target_output_root,
    )
    admission = _materialize(
        tmp_path,
        trust_config=trust,
        action=ACTION_LAUNCH,
        child=child,
    )
    loaded: list[str] = []

    def admitted_import(route: str):
        loaded.append(route)

        def admitted_main(_passthrough):
            proof = consume_active_admission(route, {ACTION_LAUNCH})
            assert proof["target_run_id"] == "target-1"
            verify_consumed_admission_target(
                proof, output_root=target_output_root
            )
            return 0

        return admitted_main

    monkeypatch.setattr(project_control, "CANONICAL_TRUST_CONFIG", trust)
    monkeypatch.setattr(
        project_control, "_clean_repository_head", lambda _path: REPO_SHA
    )
    monkeypatch.setattr(repo_app, "_load_main", admitted_import)
    assert (
        repo_app.main(
            [
                "cn-large-tpe-search-campaign",
                "--requested-action",
                ACTION_LAUNCH,
                "--target-run-id",
                "target-1",
                "--project-control-admission",
                str(admission),
                "--project-control-admission-sha256",
                sha256_file(admission),
                "--",
                "--output-root",
                str(target_output_root),
            ]
        )
        == 0
    )
    assert loaded == ["cn-large-tpe-search-campaign"]

    with pytest.raises(SystemExit) as exc:
        repo_app.main(
            [
                "cn-large-tpe-search-campaign",
                "--requested-action",
                ACTION_LAUNCH,
                "--target-run-id",
                "target-1",
                "--project-control-admission",
                str(admission),
                "--project-control-admission-sha256",
                sha256_file(admission),
                "--",
                "--output-root",
                str(tmp_path / "different-output"),
            ]
        )
    assert exc.value.code == 2
    assert loaded == ["cn-large-tpe-search-campaign"]

    with pytest.raises(SystemExit) as exc:
        repo_app.main(
            [
                "cn-large-tpe-search-campaign",
                "--requested-action",
                ACTION_LAUNCH,
                "--target-run-id",
                "target-1",
                "--project-control-admission",
                str(admission),
                "--project-control-admission-sha256",
                sha256_file(admission),
                "--",
                "--output-root",
                str(target_output_root),
            ]
        )
    assert exc.value.code == 2
    assert loaded == ["cn-large-tpe-search-campaign"]
    marker = (
        project_control._durable_control_root(target_output_root)
        / "consumptions"
        / (
            json.loads(admission.read_text(encoding="utf-8"))[
                "admission_payload_sha256"
            ]
            + ".json"
        )
    )
    assert marker.is_file()


def test_fixed_stratified_admission_preserves_binding_only_freshness(
    monkeypatch, tmp_path: Path
) -> None:
    trust = _trust_config(tmp_path)
    target_output_root = (tmp_path / "fixed-output").resolve()
    target_output_root.mkdir()
    (target_output_root / "deployment_binding.json").write_text(
        "{}\n", encoding="utf-8"
    )
    child = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="fixed-entry-control",
        campaign_id="cn-fixed-stratified-production-v0",
        target_output_root=target_output_root,
    )
    admission = _materialize(
        tmp_path,
        trust_config=trust,
        action=ACTION_LAUNCH,
        child=child,
        campaign_id="cn-fixed-stratified-production-v0",
    )
    monkeypatch.setattr(project_control, "CANONICAL_TRUST_CONFIG", trust)
    monkeypatch.setattr(
        project_control, "_clean_repository_head", lambda _path: REPO_SHA
    )

    proof = activate_admission(
        admission,
        expected_admission_file_sha256=sha256_file(admission),
        expected_actions={ACTION_LAUNCH},
        expected_target_campaign_id="cn-fixed-stratified-production-v0",
        expected_target_run_id="target-1",
        expected_target_output_root=target_output_root,
    )
    consume_active_admission(
        "cn-fixed-stratified-production-v0", {ACTION_LAUNCH}
    )
    project_control.clear_active_admission()

    assert {path.name for path in target_output_root.iterdir()} == {
        "deployment_binding.json",
        ".project_control_execution",
    }
    marker = (
        project_control._durable_control_root(target_output_root)
        / "consumptions"
        / f"{proof['admission_payload_sha256']}.json"
    )
    assert marker.is_file()


def test_prepared_77o_metadata_root_is_claimed_but_business_output_denies(
    monkeypatch, tmp_path: Path
) -> None:
    trust = _trust_config(tmp_path)
    monkeypatch.setattr(project_control, "CANONICAL_TRUST_CONFIG", trust)
    monkeypatch.setattr(
        project_control, "_clean_repository_head", lambda _path: REPO_SHA
    )

    prepared_root = (tmp_path / "prepared-output").resolve()
    prepared_root.mkdir()
    (prepared_root / "deployment_binding.json").write_text(
        "{}\n", encoding="utf-8"
    )
    (prepared_root / "campaign.stdout.log").write_text("", encoding="utf-8")
    (prepared_root / "qualification_authorization.json").write_text(
        "{}\n", encoding="utf-8"
    )
    prepared_child = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="prepared-control",
        target_output_root=prepared_root,
    )
    prepared_admission = _materialize(
        tmp_path / "prepared",
        trust_config=trust,
        action=ACTION_LAUNCH,
        child=prepared_child,
    )
    activate_admission(
        prepared_admission,
        expected_admission_file_sha256=sha256_file(prepared_admission),
        expected_actions={ACTION_LAUNCH},
        expected_target_campaign_id="cn-large-tpe-search-campaign",
        expected_target_run_id="target-1",
        expected_target_output_root=prepared_root,
    )
    consume_active_admission(
        "cn-large-tpe-search-campaign", {ACTION_LAUNCH}
    )
    project_control.clear_active_admission()
    assert project_control._durable_control_root(prepared_root).is_dir()

    stale_root = (tmp_path / "stale-output").resolve()
    stale_root.mkdir()
    (stale_root / "deployment_binding.json").write_text(
        "{}\n", encoding="utf-8"
    )
    (stale_root / "candidate_results.json").write_text(
        "{}\n", encoding="utf-8"
    )
    stale_child = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="stale-control",
        target_output_root=stale_root,
        target_run_id="target-stale",
    )
    stale_admission = _materialize(
        tmp_path / "stale",
        trust_config=trust,
        action=ACTION_LAUNCH,
        child=stale_child,
        target_run_id="target-stale",
    )
    with pytest.raises(ProjectControlDenied, match="control-metadata-only"):
        activate_admission(
            stale_admission,
            expected_admission_file_sha256=sha256_file(stale_admission),
            expected_actions={ACTION_LAUNCH},
            expected_target_campaign_id="cn-large-tpe-search-campaign",
            expected_target_run_id="target-stale",
            expected_target_output_root=stale_root,
        )


def test_current_77o_high_cost_wrappers_forward_project_control() -> None:
    wrappers = (
        "run_cn_slow_cross_sectional_evaluated384_77o.ps1",
        "run_cn_hybrid_bounded_large_tranche_77o.ps1",
        "run_cn_hybrid_only_tranche_77o.ps1",
        "run_cn_hybrid_search_productivity_medium_77o.ps1",
        "run_cn_winner_guided_large_search_77o.ps1",
        "run_cn_fixed_stratified_production_v0_77o.ps1",
    )
    for name in wrappers:
        text = (REPO / "scripts" / name).read_text(encoding="utf-8-sig")
        assert "--target-run-id" in text, name
        assert "--project-control-admission" in text, name
        assert "--project-control-admission-sha256" in text, name
        assert "output root must be fresh" in text, name


def test_campaign_bound_route_rejects_unbound_project_control_request(
    tmp_path: Path,
) -> None:
    trust = _trust_config(tmp_path)
    child = _run_record(
        tmp_path,
        trust_config=trust,
        run_id="unbound-campaign",
        campaign_authorization_path="",
    )
    with pytest.raises(
        ProjectControlDenied, match="campaign authorization binding is required"
    ):
        _materialize(
            tmp_path,
            trust_config=trust,
            action=ACTION_LAUNCH,
            child=child,
        )


def test_successor_campaign_authorization_cannot_use_launch_admission(
    tmp_path: Path,
) -> None:
    successor = _write_json(
        tmp_path / "successor-authorization.json",
        {
            "campaign_id": "successor-campaign-1",
            "campaign_profile": "cn_full_compute_successor_search_v1",
        },
    )
    successor_proof = {
        "campaign_authorization_path": str(successor.resolve()),
        "campaign_authorization_file_sha256": sha256_file(successor),
        "target_campaign_instance_id": "successor-campaign-1",
        "target_campaign_profile": "cn_full_compute_successor_search_v1",
    }
    with pytest.raises(ProjectControlDenied, match="lineage drift"):
        require_project_control_action_for_campaign_authorization(
            {"requested_action": ACTION_LAUNCH, **successor_proof}, successor
        )
    with pytest.raises(ProjectControlDenied, match="lineage drift"):
        require_project_control_action_for_campaign_authorization(
            {"requested_action": ACTION_RETRY, **successor_proof}, successor
        )
    require_project_control_action_for_campaign_authorization(
        {"requested_action": ACTION_SUCCESSOR, **successor_proof}, successor
    )
    require_project_control_action_for_campaign_authorization(
        {
            "requested_action": ACTION_RECOVERY,
            "execution_lineage_action": ACTION_SUCCESSOR,
            **successor_proof,
        },
        successor,
    )
    with pytest.raises(ProjectControlDenied, match="lineage drift"):
        require_project_control_action_for_campaign_authorization(
            {
                "requested_action": ACTION_RECOVERY,
                "execution_lineage_action": ACTION_LAUNCH,
                **successor_proof,
            },
            successor,
        )

    other_successor = _write_json(
        tmp_path / "other-successor-authorization.json",
        {
            "campaign_id": "successor-campaign-2",
            "campaign_profile": "cn_full_compute_successor_search_v1",
        },
    )
    with pytest.raises(ProjectControlDenied, match="outside the reviewed request"):
        require_project_control_action_for_campaign_authorization(
            {"requested_action": ACTION_SUCCESSOR, **successor_proof},
            other_successor,
        )

    launch = _write_json(
        tmp_path / "launch-authorization.json",
        {
            "campaign_id": "launch-campaign-1",
            "campaign_profile": "cn_winner_guided_large_search_v1",
        },
    )
    launch_proof = {
        "campaign_authorization_path": str(launch.resolve()),
        "campaign_authorization_file_sha256": sha256_file(launch),
        "target_campaign_instance_id": "launch-campaign-1",
        "target_campaign_profile": "cn_winner_guided_large_search_v1",
    }
    require_project_control_action_for_campaign_authorization(
        {"requested_action": ACTION_LAUNCH, **launch_proof}, launch
    )
    with pytest.raises(ProjectControlDenied, match="lineage drift"):
        require_project_control_action_for_campaign_authorization(
            {"requested_action": ACTION_SUCCESSOR, **launch_proof}, launch
        )


@pytest.mark.parametrize(
    "profile",
    (
        "cn_large_optuna_tpe_availability_v3",
        "cn_hybrid_search_productivity_medium_v1",
        "cn_hybrid_only_tranche_v1",
        "cn_hybrid_bounded_large_tranche_v1",
        "cn_winner_guided_large_search_v1",
    ),
)
def test_current_large_launch_profiles_are_classified(
    tmp_path: Path, profile: str
) -> None:
    authorization = _write_json(
        tmp_path / f"{profile}.json",
        {"campaign_id": f"launch-{profile}", "campaign_profile": profile},
    )
    proof = {
        "requested_action": ACTION_LAUNCH,
        "campaign_authorization_path": str(authorization.resolve()),
        "campaign_authorization_file_sha256": sha256_file(authorization),
        "target_campaign_instance_id": f"launch-{profile}",
        "target_campaign_profile": profile,
    }
    require_project_control_action_for_campaign_authorization(
        proof, authorization
    )


def test_targeted_campaign_authorization_is_exactly_bound_to_launch_lineage(
    tmp_path: Path,
) -> None:
    authorization = _write_json(
        tmp_path / "targeted-authorization.json",
        {
            "campaign_id": "targeted-campaign-1",
            "campaign_profile": "slow_cross_sectional_evaluated384",
        },
    )
    proof = {
        "campaign_authorization_path": str(authorization.resolve()),
        "campaign_authorization_file_sha256": sha256_file(authorization),
        "target_campaign_instance_id": "targeted-campaign-1",
        "target_campaign_profile": "slow_cross_sectional_evaluated384",
    }
    require_targeted_project_control_action(
        {"requested_action": ACTION_LAUNCH, **proof},
        authorization,
        "slow_cross_sectional_evaluated384",
    )
    require_targeted_project_control_action(
        {
            "requested_action": ACTION_RECOVERY,
            "execution_lineage_action": ACTION_LAUNCH,
            **proof,
        },
        authorization,
        "slow_cross_sectional_evaluated384",
    )
    with pytest.raises(ProjectControlDenied, match="lineage drift"):
        require_targeted_project_control_action(
            {"requested_action": ACTION_SUCCESSOR, **proof},
            authorization,
            "slow_cross_sectional_evaluated384",
        )

    swapped = _write_json(
        tmp_path / "swapped-targeted-authorization.json",
        {
            "campaign_id": "targeted-campaign-2",
            "campaign_profile": "slow_cross_sectional_evaluated384",
        },
    )
    with pytest.raises(ProjectControlDenied, match="outside the reviewed request"):
        verify_campaign_authorization_binding(proof, swapped)


def test_preflight_authorization_binding_is_plannable_without_claiming_root(
    tmp_path: Path,
) -> None:
    source_payload = {
        "campaign_id": "planned-campaign-1",
        "campaign_profile": "cn_full_compute_successor_search_v1",
        "execution_authorized": True,
        "financial_campaign_authorized": True,
    }
    source_payload["resource_topology_authorization_sha256"] = _stable_hash(
        source_payload
    )
    source = _write_json(tmp_path / "source-authorization.json", source_payload)
    future = tmp_path / "fresh-root" / "qualification_authorization.json"
    binding = planned_authorization_binding(source, future)
    assert not future.parent.exists()
    assert binding["campaign_authorization_path"] == str(future.resolve())
    assert binding["target_campaign_instance_id"] == "planned-campaign-1"
    assert (
        binding["target_campaign_profile"]
        == "cn_full_compute_successor_search_v1"
    )

    payload = materialize_authorization(source)
    future.parent.mkdir()
    future.write_bytes(authorization_bytes(payload))
    assert sha256_file(future) == binding["campaign_authorization_file_sha256"]


def test_successor_prepare_wrappers_stop_at_external_project_control() -> None:
    for name in (
        "prepare_cn_shared_control_dual_lane_77o.ps1",
        "prepare_cn_terminal_liquidity_search_continuity_77o.ps1",
    ):
        text = (REPO / "scripts" / name).read_text(encoding="utf-8-sig")
        boundary = text.index("AWAITING_EXTERNAL_PROJECT_CONTROL_ADMISSION")
        stop = text.index("\nreturn\n", boundary)
        assert boundary < stop, name
        assert (
            "& (Join-Path $repo 'scripts\\run_cn_winner_guided_large_search_77o.ps1')"
            not in text
        )
        assert not text[stop + len("\nreturn\n") :].strip()
        before_stop = text[:stop]
        assert "--preflight-authorization-source $searchAuthorization" in before_stop
        assert "--action 'SUCCESSOR_CAMPAIGN'" in before_stop
        assert "ProjectControlAdmission" not in before_stop


def test_fixed_stratified_does_not_advertise_unsupported_recovery() -> None:
    text = (
        REPO / "scripts" / "run_cn_fixed_stratified_production_v0_77o.ps1"
    ).read_text(encoding="utf-8-sig")
    assert ACTION_RECOVERY not in repo_app.HIGH_COST_ROUTE_ACTIONS[
        "cn-fixed-stratified-production-v0"
    ]
    assert "'RECOVERY'" not in text


def test_targeted_route_does_not_advertise_unsupported_successor() -> None:
    assert ACTION_SUCCESSOR not in repo_app.HIGH_COST_ROUTE_ACTIONS[
        "cn-targeted-search-medium-campaign"
    ]


def test_winner_wrapper_does_not_advertise_unsafe_recovery() -> None:
    text = (
        REPO / "scripts" / "run_cn_winner_guided_large_search_77o.ps1"
    ).read_text(encoding="utf-8-sig")
    assert "'RECOVERY'" not in text


def test_closed_actual20000_wrapper_is_retired_before_execution() -> None:
    text = (
        REPO / "scripts" / "run_cn_large_optuna_tpe_actual20000_77o.ps1"
    ).read_text(encoding="utf-8-sig")
    retired = text.index("RETIRED_CLOSED_CAMPAIGN_PROVENANCE_ONLY")
    assert retired < text.index("New-Item")
    assert retired < text.index("'cn-large-tpe-search-campaign'")
