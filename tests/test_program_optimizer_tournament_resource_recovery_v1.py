from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import pytest

import app
from scripts import run_cn_finalist_replay_then_oos as replay
from scripts import run_cn_joint_program_phase_c_v0 as phase_c
from scripts import run_cn_program_optimizer_tournament_v1 as tournament_runner
from our_system_phase2.runtime.cn_program_optimizer_tournament_v1 import (
    FROZEN_PROGRAM_SPACE_ENTRY_COUNT,
    FROZEN_PROGRAM_SPACE_SHA256,
    authorization_payload_v1,
    build_maximum_ask_plan_v1,
)
from our_system_phase2.services import project_control_admission as project_control
from our_system_phase2.services.unified_capability_registry import stable_hash


ROOT = Path(__file__).resolve().parents[1]
AUTHORIZATION_SHA = "99430779f9b80651da1928a90e3cfc0df2357a750fb8837da5c5cd0f195685fd"
ASK_PLAN_SHA = "eb66b7b374f7e2550db5bf5d7cee98fd66a8526dabadfbda14af1ecd6cf3dc4b"
PROGRAM_SPACE_SHA = "86d9bce8f7bdc75e55c791b6e101ec4beefe093e346abfc39c76ee1c30f354e0"
SOURCE_BINDING_SHA = "d9cdcc4459590dc769aa9b1f724530cb0d1d18f2ba999e3d790a4707b07d6d94"


def test_checkpoint_projection_uses_compiled_physical_leaves_only() -> None:
    schedules = [
        {
            "primary_compiled": {"physical_leaf_ids": ["field_b", "field_a"]},
            "control_compiled": {"physical_leaf_ids": ["field_c", "field_a"]},
        }
    ]
    assert phase_c._checkpoint_field_columns(schedules) == (
        "close",
        "code",
        "field_a",
        "field_b",
        "field_c",
        "trade_time",
    )


def test_field_loader_forwards_exact_projection(monkeypatch, tmp_path: Path) -> None:
    observed: list[list[str] | None] = []

    def fake_read_parquet(path, *, columns=None):
        del path
        observed.append(columns)
        return pd.DataFrame(
            {
                "trade_time": ["2026-01-05 15:00:00"],
                "code": ["000001.SZ"],
                "close": [10.0],
                "field_a": [1.0],
            }
        )

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)
    manifest = {
        "source_shard_count": 1,
        "shards": [{"output_path": str(tmp_path / "shard.parquet")}],
    }
    frame = replay._load_field_frame(
        tmp_path,
        manifest,
        columns=("trade_time", "code", "close", "field_a"),
    )
    assert observed == [["trade_time", "code", "close", "field_a"]]
    assert list(frame.columns) == ["trade_time", "code", "close", "field_a", "date"]


def test_commit_gate_and_adaptive_worker_cap() -> None:
    with pytest.raises(RuntimeError, match="commit-headroom"):
        phase_c._require_runtime_resource_safety(
            {"commit_headroom_bytes": phase_c.MINIMUM_COMMIT_HEADROOM_BYTES - 1}
        )
    assert phase_c._effective_checkpoint_workers(
        worker_cap=8,
        schedule_count=8,
        commit_headroom_bytes=phase_c.MINIMUM_COMMIT_HEADROOM_BYTES + 80 * 1024**3,
    ) == 8
    assert phase_c._effective_checkpoint_workers(
        worker_cap=8,
        schedule_count=8,
        commit_headroom_bytes=phase_c.MINIMUM_COMMIT_HEADROOM_BYTES + 48 * 1024**3,
    ) == 6
    assert phase_c._effective_checkpoint_workers(
        worker_cap=8,
        schedule_count=8,
        commit_headroom_bytes=phase_c.MINIMUM_COMMIT_HEADROOM_BYTES + 32 * 1024**3,
    ) == 4


def test_recovery_isolates_only_first_recovered_checkpoint() -> None:
    isolated = phase_c._checkpoint_executor_plan(
        checkpoint_index=2,
        recovery_start_checkpoint_index=2,
        checkpoint_recovery_mode=True,
        worker_cap=8,
        schedule_count=8,
        commit_headroom_bytes=phase_c.MINIMUM_COMMIT_HEADROOM_BYTES + 80 * 1024**3,
    )
    continued = phase_c._checkpoint_executor_plan(
        checkpoint_index=3,
        recovery_start_checkpoint_index=2,
        checkpoint_recovery_mode=True,
        worker_cap=8,
        schedule_count=8,
        commit_headroom_bytes=phase_c.MINIMUM_COMMIT_HEADROOM_BYTES + 80 * 1024**3,
    )
    assert isolated == {
        "executor_mode": "RECOVERY_ISOLATED_FIRST_CHECKPOINT",
        "effective_checkpoint_workers": 1,
        "max_tasks_per_child": 1,
        "checkpoint_recovery_provenance": True,
    }
    assert continued == {
        "executor_mode": "NORMAL_ADAPTIVE_AFTER_RECOVERY_ISOLATION",
        "effective_checkpoint_workers": 8,
        "max_tasks_per_child": None,
        "checkpoint_recovery_provenance": True,
    }


@pytest.mark.parametrize("boundary", [1, 7, 45])
def test_arbitrary_partial_recovery_boundary_isolates_exactly_one_checkpoint(
    boundary: int,
) -> None:
    modes = [
        phase_c._checkpoint_executor_plan(
            checkpoint_index=index,
            recovery_start_checkpoint_index=boundary,
            checkpoint_recovery_mode=True,
            worker_cap=8,
            schedule_count=8,
            commit_headroom_bytes=(
                phase_c.MINIMUM_COMMIT_HEADROOM_BYTES + 80 * 1024**3
            ),
        )["executor_mode"]
        for index in (boundary, boundary + 1)
    ]
    assert modes == [
        "RECOVERY_ISOLATED_FIRST_CHECKPOINT",
        "NORMAL_ADAPTIVE_AFTER_RECOVERY_ISOLATION",
    ]


def test_non_recovery_executor_plan_remains_adaptive() -> None:
    plan = phase_c._checkpoint_executor_plan(
        checkpoint_index=0,
        recovery_start_checkpoint_index=0,
        checkpoint_recovery_mode=False,
        worker_cap=8,
        schedule_count=8,
        commit_headroom_bytes=phase_c.MINIMUM_COMMIT_HEADROOM_BYTES + 48 * 1024**3,
    )
    assert plan == {
        "executor_mode": "NORMAL_ADAPTIVE",
        "effective_checkpoint_workers": 6,
        "max_tasks_per_child": None,
        "checkpoint_recovery_provenance": False,
    }


def test_quarantined_results_are_outside_recovery_read_path() -> None:
    source = (
        ROOT / "scripts/run_cn_joint_program_phase_c_v0.py"
    ).read_text(encoding="utf-8")
    quarantine_verifier = source[
        source.index("def _verify_checkpoint_quarantine(") : source.index(
            "def _runtime_resource_snapshot("
        )
    ]
    assert '_sha256(quarantine_path)' in quarantine_verifier
    assert '_read_json(quarantine_path)' not in quarantine_verifier
    assert 'record_root.glob("record_*.json")' in source
    assert 'if inflight_root.exists() and any(inflight_root.iterdir())' in source


def test_checkpoint_quarantine_binds_all_eight_inflight_hashes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "stage01_run"
    quarantine_root = tmp_path / "quarantine" / "checkpoint_012"
    artifacts = []
    for ordinal in range(88, 96):
        quarantined = quarantine_root / f"record_{ordinal:04d}.json"
        quarantined.parent.mkdir(parents=True, exist_ok=True)
        quarantined.write_text(f"record-{ordinal}", encoding="utf-8")
        artifacts.append(
            {
                "main_record_ordinal": ordinal,
                "source_path": str(
                    root
                    / "inflight/checkpoint_012/records"
                    / f"record_{ordinal:04d}.json"
                ),
                "quarantine_path": str(quarantined),
                "file_sha256": phase_c._sha256(quarantined),
            }
        )
    payload = {
        "status": "PASS",
        "accepted_root": str(root),
        "checkpoint_number": 12,
        "record_count": 8,
        "artifacts": artifacts,
        "formal_inflight_empty": True,
        "financial_results_reusable": False,
        "incomplete_results_reused": False,
        "optimizer_tell_count": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["quarantine_payload_sha256"] = stable_hash(payload)
    manifest = tmp_path / "quarantine.json"
    phase_c._write_json(manifest, payload)

    verified, payload_hash = phase_c._verify_checkpoint_quarantine(
        manifest, root=root, checkpoint_number=12
    )

    assert verified == payload
    assert payload_hash == payload["quarantine_payload_sha256"]


def test_tournament_phase_forwards_recovery_only_to_stage01(
    monkeypatch, tmp_path: Path
) -> None:
    freeze_root = tmp_path / "freeze"
    freeze_root.mkdir()
    accepted = tmp_path / "accepted" / "manifest.json"
    captured = []
    monkeypatch.setattr(tournament_runner, "_verify_freeze", lambda _: {})
    monkeypatch.setattr(
        tournament_runner.engine,
        "_read_json",
        lambda _: {"accepted_field_manifest_path": str(accepted)},
    )
    monkeypatch.setattr(
        tournament_runner.engine,
        "run",
        lambda args: captured.append(args) or {"status": "PASS"},
    )
    args = argparse.Namespace(
        checkpoint_recovery_from_repo_sha="5" * 40,
        checkpoint_recovery_incident=tmp_path / "incident.json",
        checkpoint_recovery_diagnostic_audit=tmp_path / "audit.json",
        checkpoint_recovery_deployment_manifest=tmp_path / "deployment.json",
    )
    tournament_runner._run_phase(
        args,
        freeze_root=freeze_root,
        run_root=tmp_path / "stage01_run",
        repo_sha="6" * 40,
        checkpoint_recovery=True,
    )
    tournament_runner._run_phase(
        args,
        freeze_root=freeze_root,
        run_root=tmp_path / "stage02_run",
        repo_sha="6" * 40,
        checkpoint_recovery=False,
    )
    assert captured[0].checkpoint_recovery_from_repo_sha == "5" * 40
    assert captured[1].checkpoint_recovery_from_repo_sha is None


@pytest.mark.parametrize(
    "scope,boundary",
    [
        ("PHASE_C_CHECKPOINT_RECOVERY_AFTER_RESOURCE_FAILURE", 2),
        ("PHASE_C_CHECKPOINT_RECOVERY", 1),
        ("PHASE_C_CHECKPOINT_RECOVERY", 11),
        ("PHASE_C_CHECKPOINT_RECOVERY", 45),
    ],
)
def test_tournament_cross_sha_recovery_boundary_is_exact(
    tmp_path: Path, scope: str, boundary: int
) -> None:
    original_sha = "5" * 40
    current_sha = "6" * 40
    incident = tmp_path / "incident.json"
    payload = {
        "checkpoint_builder_repo_sha": original_sha,
        "recovery_scope": scope,
        "failure_classification": "PROGRAM_CONDITIONAL_OBJECTIVE_KEY_DRIFT",
        "checkpoint_recomputation_authorized": True,
        "closed_checkpoint_count": boundary,
        "closed_record_count": boundary * phase_c.RECORDS_PER_CHECKPOINT,
        "first_recovered_checkpoint": boundary + 1,
        "incomplete_results_reused": False,
    }
    payload["incident_payload_sha256"] = stable_hash(payload)
    incident.write_text(json.dumps(payload), encoding="utf-8")
    request = {
        "original_repo_sha": original_sha,
        "recovery_scope": scope,
    }
    assert project_control._validate_source_repair_recovery(
        target_campaign_id="cn-program-optimizer-tournament-v1",
        request=request,
        current_repo_sha=current_sha,
        incident_path=incident,
    ) == original_sha
    payload = json.loads(incident.read_text(encoding="utf-8"))
    payload["incomplete_results_reused"] = True
    incident.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(project_control.ProjectControlDenied, match="incident.*drift"):
        project_control._validate_source_repair_recovery(
            target_campaign_id="cn-program-optimizer-tournament-v1",
            request=request,
            current_repo_sha=current_sha,
            incident_path=incident,
        )


def test_checkpoint_recovery_history_is_append_only_and_linear(
    tmp_path: Path,
) -> None:
    legacy_path = tmp_path / "checkpoint_recovery_binding.json"
    legacy = phase_c._self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_checkpoint_recovery_v0",
            "status": "CHECKPOINT_RECOVERY_BOUND",
            "closed_checkpoint_count": 2,
        },
        "recovery_binding_sha256",
    )
    phase_c._write_json(legacy_path, legacy)
    continuation = phase_c._self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_checkpoint_recovery_v0",
            "status": "CHECKPOINT_RECOVERY_BOUND",
            "closed_checkpoint_count": 11,
            "previous_recovery_binding": str(legacy_path.resolve()),
            "previous_recovery_binding_file_sha256": phase_c._sha256(legacy_path),
            "previous_recovery_binding_payload_sha256": legacy[
                "recovery_binding_sha256"
            ],
        },
        "recovery_binding_sha256",
    )
    continuation_path = (
        tmp_path
        / "checkpoint_recovery_attempts"
        / f"{continuation['recovery_binding_sha256']}.json"
    )
    phase_c._write_json(continuation_path, continuation)

    history = phase_c._checkpoint_recovery_history(tmp_path)

    assert [path for path, _ in history] == [
        legacy_path.resolve(),
        continuation_path.resolve(),
    ]
    assert history[-1][1]["closed_checkpoint_count"] == 11


def test_checkpoint_recovery_history_rejects_a_fork(tmp_path: Path) -> None:
    legacy_path = tmp_path / "checkpoint_recovery_binding.json"
    legacy = phase_c._self_hashed(
        {"status": "CHECKPOINT_RECOVERY_BOUND"},
        "recovery_binding_sha256",
    )
    phase_c._write_json(legacy_path, legacy)
    for ordinal in (1, 2):
        continuation = phase_c._self_hashed(
            {
                "status": "CHECKPOINT_RECOVERY_BOUND",
                "ordinal": ordinal,
                "previous_recovery_binding": str(legacy_path.resolve()),
                "previous_recovery_binding_file_sha256": phase_c._sha256(
                    legacy_path
                ),
                "previous_recovery_binding_payload_sha256": legacy[
                    "recovery_binding_sha256"
                ],
            },
            "recovery_binding_sha256",
        )
        phase_c._write_json(
            tmp_path
            / "checkpoint_recovery_attempts"
            / f"{continuation['recovery_binding_sha256']}.json",
            continuation,
        )
    with pytest.raises(RuntimeError, match="not linear"):
        phase_c._checkpoint_recovery_history(tmp_path)


def test_recovery_surface_precedes_project_control_and_frozen_ids_hold() -> None:
    wrapper = (
        ROOT / "scripts/run_cn_program_optimizer_tournament_77o.ps1"
    ).read_text(encoding="utf-8-sig")
    assert "'RECOVERY'" in wrapper
    assert wrapper.index("check_cn_program_optimizer_execution_node_cleanliness_v1.py") < wrapper.index(
        "'cn-program-optimizer-tournament-v1'"
    )
    assert "--checkpoint-recovery-from-repo-sha" in wrapper
    assert app.HIGH_COST_ROUTE_ACTIONS["cn-program-optimizer-tournament-v1"] == {
        project_control.ACTION_LAUNCH,
        project_control.ACTION_RETRY,
        project_control.ACTION_RECOVERY,
    }
    authorization = authorization_payload_v1()
    assert authorization["authorization_payload_sha256"] == AUTHORIZATION_SHA
    assert stable_hash(list(build_maximum_ask_plan_v1())) == ASK_PLAN_SHA
    assert FROZEN_PROGRAM_SPACE_ENTRY_COUNT == 3616
    assert FROZEN_PROGRAM_SPACE_SHA256 == PROGRAM_SPACE_SHA
    source_binding = json.loads(
        (
            ROOT
            / "runtime/run_plans/cn_program_optimizer_tournament_source_binding_v1.json"
        ).read_text(encoding="utf-8")
    )
    source_binding_body = dict(source_binding)
    assert source_binding_body.pop("source_binding_payload_sha256") == SOURCE_BINDING_SHA
    assert stable_hash(source_binding_body) == SOURCE_BINDING_SHA
