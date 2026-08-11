from __future__ import annotations

import json
from pathlib import Path

import pytest

import app as repo_app

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
    ProjectControlDenied,
    materialize_admission,
    project_control_receipt_sha256,
    sha256_file,
    validate_admission,
)


REPO_SHA = "a" * 40
PARENT_SHA = "b" * 40


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _run_record(
    path: Path,
    *,
    run_id: str,
    repo_sha: str = REPO_SHA,
    preflight_verdict: str = "PROCEED",
    execution_allowed: bool = True,
    post_batch_verdict: str | None = None,
    continuation_allowed: bool | None = None,
) -> Path:
    receipts = [
        {
            "phase": "PREFLIGHT",
            "verdict": preflight_verdict,
            "current_bottleneck": "search control drift",
            "reason_codes": ["AUTHORITY_REPAIR"],
            "evidence_references": [],
            "next_required_decision": "",
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
                "next_required_decision": "",
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
    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "task_id": f"task-{run_id}",
        "project_id": PROJECT_ID,
        "code_base_sha": repo_sha,
        "automatic_execution_allowed": execution_allowed,
        "automatic_continuation_allowed": continuation_allowed,
        "project_control": receipts,
    }
    return _write_json(path, payload)


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
                    "status": (
                        "spent_negative_no_retry"
                        if current_role == "spent"
                        else "authorized_unopened_fixed_ten_report_only"
                    ),
                    "performance_rows_read": reads,
                },
                "forward_b_tdx_lc1_20260413_20260514_f69cc84f": {
                    "current_role": "forward",
                    "status": "reserved_sealed_not_authorized_for_performance_access",
                    "performance_rows_read": 0,
                },
            },
        },
    )
    access = _write_json(
        root / "access.json",
        {
            "status": (
                "HISTORICAL_CHALLENGE_2023_ACCESS_STARTED_ASSET_SPENT"
                if access_role == "spent"
                else "HISTORICAL_CHALLENGE_2023_NOT_STARTED"
            ),
            "asset_id": "historical_challenge_2023_b05e2ca0",
            "data_role_after_transition": access_role,
            "forward_b_reads": 0,
        },
    )
    outcome = _write_json(
        root / "outcome.json",
        {
            "status": (
                "INDEPENDENT_AUDIT_PASS_FORWARD_B_REMAINS_SEALED"
                if reads
                else "NOT_RUN"
            ),
            "access_decision": {
                "historical_challenge_2023_state": (
                    "SPENT" if reads else "UNOPENED"
                ),
                "forward_b_state": "SEALED",
                "forward_b_access": "NOT_AUTHORIZED",
            },
            "provenance": {
                "historical_challenge_reads": reads,
                "forward_b_reads": 0,
            },
        },
    )
    return registry, access, outcome


def _materialize(
    root: Path,
    *,
    action: str,
    child: Path,
    target_run_id: str = "target-1",
    parent: Path | None = None,
    recovery_of_target_run_id: str = "",
    recovery_kind: str = "",
    incident_id: str = "",
) -> Path:
    output = root / f"admission-{action.lower()}.json"
    materialize_admission(
        output_path=output,
        project_control_run_record_path=child,
        expected_project_control_receipt_sha256=(
            project_control_receipt_sha256(child, "PREFLIGHT")
        ),
        requested_action=action,
        target_campaign_id="cn-large-tpe-search-campaign",
        target_run_id=target_run_id,
        repo_sha=REPO_SHA,
        parent_post_batch_run_record_path=parent,
        expected_parent_post_batch_receipt_sha256=(
            project_control_receipt_sha256(parent, "POST_BATCH")
            if parent is not None
            else ""
        ),
        recovery_kind=recovery_kind,
        recovery_of_target_run_id=recovery_of_target_run_id,
        incident_id=incident_id,
    )
    return output


def _validate(
    path: Path, *, action: str, target_run_id: str = "target-1"
) -> dict:
    return validate_admission(
        path,
        expected_admission_file_sha256=sha256_file(path),
        expected_project_id=PROJECT_ID,
        expected_repo_sha=REPO_SHA,
        expected_actions={action},
        expected_target_campaign_id="cn-large-tpe-search-campaign",
        expected_target_run_id=target_run_id,
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
    assert proof["forward_b_state"] == "SEALED"


def test_spent_asset_is_permanently_denied_even_with_fresh_output_root(
    tmp_path: Path,
) -> None:
    registry, access, outcome = _asset_authority(
        tmp_path, current_role="spent", access_role="spent", reads=2_383_217
    )
    fresh_output_root = tmp_path / "fresh-output"
    assert not fresh_output_root.exists()
    with pytest.raises(EvaluationAssetDenied, match=PERMANENT_DENY_ALREADY_SPENT):
        verify_historical_challenge_destructive_use(
            role_registry_path=registry,
            access_started_path=access,
            outcome_path=outcome,
        )
    assert not fresh_output_root.exists()


def test_missing_or_non_proceed_preflight_denies(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    with pytest.raises(ProjectControlDenied, match="receipt path"):
        materialize_admission(
            output_path=tmp_path / "admission.json",
            project_control_run_record_path=missing,
            expected_project_control_receipt_sha256="0" * 64,
            requested_action=ACTION_LAUNCH,
            target_campaign_id="cn-large-tpe-search-campaign",
            target_run_id="target-1",
            repo_sha=REPO_SHA,
        )

    paused = _run_record(
        tmp_path / "paused.json",
        run_id="paused",
        preflight_verdict="PAUSE",
        execution_allowed=False,
    )
    with pytest.raises(ProjectControlDenied, match="PREFLIGHT_PROCEED_REQUIRED"):
        _materialize(tmp_path, action=ACTION_LAUNCH, child=paused)


@pytest.mark.parametrize("action", [ACTION_FREEZE, ACTION_LAUNCH, ACTION_RETRY])
def test_preflight_proceed_makes_new_action_eligible(
    tmp_path: Path, action: str
) -> None:
    child = _run_record(tmp_path / f"{action}.json", run_id=f"child-{action}")
    admission = _materialize(tmp_path, action=action, child=child)
    proof = _validate(admission, action=action)
    assert proof["status"] == "PROJECT_CONTROL_ADMISSION_ELIGIBLE"
    assert proof["requested_action"] == action


def test_successor_requires_parent_continue_and_child_preflight(tmp_path: Path) -> None:
    child = _run_record(tmp_path / "child.json", run_id="child")
    stopped = _run_record(
        tmp_path / "stopped.json",
        run_id="parent-stopped",
        repo_sha=PARENT_SHA,
        post_batch_verdict="PAUSE",
        continuation_allowed=False,
    )
    with pytest.raises(ProjectControlDenied, match="POST_BATCH_CONTINUE_REQUIRED"):
        _materialize(
            tmp_path,
            action=ACTION_SUCCESSOR,
            child=child,
            parent=stopped,
        )

    continued = _run_record(
        tmp_path / "continued.json",
        run_id="parent-continued",
        repo_sha=PARENT_SHA,
        post_batch_verdict="CONTINUE",
        continuation_allowed=True,
    )
    admission = _materialize(
        tmp_path,
        action=ACTION_SUCCESSOR,
        child=child,
        parent=continued,
    )
    assert _validate(admission, action=ACTION_SUCCESSOR)["parent_run_id"] == (
        "parent-continued"
    )


def test_technical_recovery_preserves_same_run_and_binds_incident(
    tmp_path: Path,
) -> None:
    child = _run_record(tmp_path / "original.json", run_id="control-original")
    admission = _materialize(
        tmp_path,
        action=ACTION_RECOVERY,
        child=child,
        target_run_id="immutable-run-1",
        recovery_of_target_run_id="immutable-run-1",
        recovery_kind=TECHNICAL_RECOVERY,
        incident_id="incident-checkpoint-055",
    )
    proof = _validate(
        admission,
        action=ACTION_RECOVERY,
        target_run_id="immutable-run-1",
    )
    assert proof["recovery_kind"] == TECHNICAL_RECOVERY

    with pytest.raises(ProjectControlDenied, match="RECOVERY_TARGET_DRIFT"):
        _materialize(
            tmp_path / "drift",
            action=ACTION_RECOVERY,
            child=child,
            target_run_id="new-run",
            recovery_of_target_run_id="immutable-run-1",
            recovery_kind=TECHNICAL_RECOVERY,
            incident_id="incident-checkpoint-055",
        )

    with pytest.raises(ProjectControlDenied, match="RECOVERY_KIND_FORBIDDEN"):
        _materialize(
            tmp_path / "new-campaign",
            action=ACTION_RECOVERY,
            child=child,
            target_run_id="new-run",
            recovery_of_target_run_id="immutable-run-1",
            recovery_kind="NEW_RETRY_OR_NEW_CAMPAIGN",
            incident_id="incident-checkpoint-055",
        )


def test_receipt_project_run_and_repo_binding_drift_denies(tmp_path: Path) -> None:
    child = _run_record(tmp_path / "child.json", run_id="child")
    with pytest.raises(ProjectControlDenied, match="receipt hash drift"):
        materialize_admission(
            output_path=tmp_path / "bad.json",
            project_control_run_record_path=child,
            expected_project_control_receipt_sha256="f" * 64,
            requested_action=ACTION_LAUNCH,
            target_campaign_id="cn-large-tpe-search-campaign",
            target_run_id="target-1",
            repo_sha=REPO_SHA,
        )

    admission = _materialize(tmp_path, action=ACTION_LAUNCH, child=child)
    with pytest.raises(ProjectControlDenied, match="project identity drift"):
        validate_admission(
            admission,
            expected_admission_file_sha256=sha256_file(admission),
            expected_project_id="another-project",
            expected_repo_sha=REPO_SHA,
            expected_actions={ACTION_LAUNCH},
            expected_target_campaign_id="cn-large-tpe-search-campaign",
            expected_target_run_id="target-1",
        )
    with pytest.raises(ProjectControlDenied, match="repo SHA drift"):
        validate_admission(
            admission,
            expected_admission_file_sha256=sha256_file(admission),
            expected_project_id=PROJECT_ID,
            expected_repo_sha="c" * 40,
            expected_actions={ACTION_LAUNCH},
            expected_target_campaign_id="cn-large-tpe-search-campaign",
            expected_target_run_id="target-1",
        )
    with pytest.raises(ProjectControlDenied, match="target run drift"):
        validate_admission(
            admission,
            expected_admission_file_sha256=sha256_file(admission),
            expected_project_id=PROJECT_ID,
            expected_repo_sha=REPO_SHA,
            expected_actions={ACTION_LAUNCH},
            expected_target_campaign_id="cn-large-tpe-search-campaign",
            expected_target_run_id="another-target-run",
        )
    with pytest.raises(ProjectControlDenied, match="requested action drift"):
        validate_admission(
            admission,
            expected_admission_file_sha256=sha256_file(admission),
            expected_project_id=PROJECT_ID,
            expected_repo_sha=REPO_SHA,
            expected_actions={ACTION_RETRY},
            expected_target_campaign_id="cn-large-tpe-search-campaign",
            expected_target_run_id="target-1",
        )


def test_false_serialized_state_does_not_erase_observation_feedback() -> None:
    provenance = build_development_feedback_provenance(
        serialized_optimizer_state_imported=False,
        development_financial_observations_imported=True,
        development_observation_count=56,
        candidate_results_imported=True,
        factor_statistics_imported=False,
        behavior_statistics_imported=False,
        template_classification_imported=True,
        manual_diagnosis_imported=False,
        objective_designed_after_parent_results=False,
    )
    assert provenance["serialized_optimizer_state_imported"] is False
    assert provenance["development_financial_observations_imported"] is True
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
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_cn_fixed_survivor_historical_challenge_2023_77o.ps1"
    ).read_text(encoding="utf-8")
    gate = launcher.index("verify_cn_historical_challenge_admission.py")
    archive_hash = launcher.index("$archiveHash = Get-SharedReadSha256")
    output_create = launcher.index("New-Item -ItemType Directory -Path $resolvedRoot")
    conversion = launcher.index("convert_cn_yearly_1min_zip_to_session_shards.py")
    assert gate < archive_hash < output_create < conversion


def test_high_cost_entry_denies_before_route_import(monkeypatch) -> None:
    imported = False

    def forbidden_import(_route: str):
        nonlocal imported
        imported = True
        raise AssertionError("high-cost route imported before admission")

    monkeypatch.setattr(repo_app, "_load_main", forbidden_import)
    with pytest.raises(SystemExit) as exc:
        repo_app.main(["cn-large-tpe-search-campaign"])
    assert exc.value.code == 2
    assert imported is False


def test_high_cost_entry_consumes_valid_bound_admission(
    monkeypatch, tmp_path: Path
) -> None:
    child = _run_record(tmp_path / "child.json", run_id="entry-control")
    admission = _materialize(tmp_path, action=ACTION_LAUNCH, child=child)
    loaded: list[str] = []

    def admitted_import(route: str):
        loaded.append(route)
        return lambda _passthrough: 0

    monkeypatch.setattr(repo_app, "_git_head", lambda: REPO_SHA)
    monkeypatch.setattr(repo_app, "_load_main", admitted_import)
    result = repo_app.main(
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
        ]
    )
    assert result == 0
    assert loaded == ["cn-large-tpe-search-campaign"]
