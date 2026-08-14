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


def test_tournament_cross_sha_recovery_scope_is_exact(tmp_path: Path) -> None:
    original_sha = "5" * 40
    current_sha = "6" * 40
    incident = tmp_path / "incident.json"
    incident.write_text(
        json.dumps(
            {
                "checkpoint_builder_repo_sha": original_sha,
                "recovery_scope": (
                    "PHASE_C_CHECKPOINT_RECOVERY_AFTER_RESOURCE_FAILURE"
                ),
                "checkpoint_recomputation_authorized": True,
                "closed_checkpoint_count": 2,
                "closed_record_count": 16,
                "first_recovered_checkpoint": 3,
                "incomplete_results_reused": False,
            }
        ),
        encoding="utf-8",
    )
    request = {
        "original_repo_sha": original_sha,
        "recovery_scope": "PHASE_C_CHECKPOINT_RECOVERY_AFTER_RESOURCE_FAILURE",
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
    with pytest.raises(project_control.ProjectControlDenied, match="incident drift"):
        project_control._validate_source_repair_recovery(
            target_campaign_id="cn-program-optimizer-tournament-v1",
            request=request,
            current_repo_sha=current_sha,
            incident_path=incident,
        )


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
