from __future__ import annotations

import json
import hashlib
import inspect
from pathlib import Path

import pytest

from our_system_phase2.services.a_share_executable_replay import (
    AShareCorporateActionFractionalSharesError,
)
from scripts import run_cn_joint_program_phase_b_v0 as phase_b_runner

from our_system_phase2.runtime.cn_iterative_search_v1 import _batch_manifest
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import TEMPLATE_ORDER
from scripts.run_cn_joint_program_phase_b_v0 import (
    _parity_differences,
    run,
    _self_hashed,
    _template_summary,
    _validate_phase_b_execution_price_sidecar,
    _validate_phase_b_materialized_sidecar,
    _validate_frozen_execution_contract,
    _verify_checkpoint,
    _write_json,
)


def test_phase_b_recycles_process_pool_at_each_checkpoint_boundary() -> None:
    source = inspect.getsource(run)
    checkpoint_loop = source.index(
        "for checkpoint_index in range(closed_checkpoints, CHECKPOINT_COUNT):"
    )
    process_pool = source.index("with ProcessPoolExecutor(", checkpoint_loop)
    resource_gate = source.index(
        "if available < MINIMUM_FREE_MEMORY_BYTES:", process_pool
    )
    checkpoint_close = source.index(
        "previous_manifest = _close_checkpoint(", resource_gate
    )

    assert checkpoint_loop < process_pool < resource_gate < checkpoint_close
    assert "for row in checkpoint_rows:" in source[process_pool:resource_gate]
    assert "for row in remaining:" not in source
    assert '"executor_lifecycle": "CHECKPOINT_SCOPED_RECYCLE"' in source
    assert '"maximum_inflight_records": RECORDS_PER_CHECKPOINT' in source
    assert '"minimum_observed_free_memory_bytes": minimum_observed_free' in source


def _closed_record(ordinal: int, template_id: str) -> dict:
    return _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_b_record_v0",
            "status": "JOINT_PROGRAM_PHASE_B_RECORD_CLOSED_IMMUTABLE",
            "input_binding_sha256": "a" * 64,
            "main_record_ordinal": ordinal,
            "template_id": template_id,
            "record_kind": (
                "BASE_WRAPPER_PARITY"
                if template_id == "BASE"
                else "ENHANCED_FULL_BASE_PAIR"
            ),
            "schedule_record_sha256": f"{ordinal:064x}",
            "program_id": f"program-{ordinal}",
            "control_program_id": f"control-{ordinal}",
            "control_contract_valid": True,
            "compile_status": "PASS",
            "physical_ready": True,
            "dag_ready": True,
            "semantic_noop": False,
            "replay_status": "PAIR_REPLAY_COMPLETE",
            "replay_blocker": None,
            "primary": {
                "behavior_identity": f"behavior-{ordinal}",
                "continuous_book_net_reward": 0.01,
            },
            "base_control": {
                "behavior_identity": f"control-behavior-{ordinal}",
                "continuous_book_net_reward": 0.0,
            },
            "matched_net_reward_increment": 0.01,
            "search_score": 0.01,
            "productive": True,
            "base_wrapper_parity": (
                {"status": "PASS"} if template_id == "BASE" else None
            ),
        },
        "record_payload_sha256",
    )


def test_phase_b_template_summary_preserves_exact_uniform_quota() -> None:
    records = [
        _closed_record(template_ordinal * 8 + offset, template_id)
        for template_ordinal, template_id in enumerate(TEMPLATE_ORDER)
        for offset in range(8)
    ]
    summary = _template_summary(records)
    assert [row["template_id"] for row in summary] == list(TEMPLATE_ORDER)
    assert all(row["program_proposed"] == 8 for row in summary)
    assert all(row["replay_complete"] == 8 for row in summary)
    assert all(row["economic_rows_complete"] == 8 for row in summary)
    assert all(row["semantic_unique"] == 8 for row in summary)
    assert all(row["behavior_unique"] == 8 for row in summary)


def test_phase_b_template_summary_excludes_candidate_local_blocker_economics() -> None:
    records = [
        _closed_record(template_ordinal * 8 + offset, template_id)
        for template_ordinal, template_id in enumerate(TEMPLATE_ORDER)
        for offset in range(8)
    ]
    blocked = records[29]
    blocked.update(
        {
            "replay_status": "PAIR_REPLAY_BLOCKED",
            "replay_blocker": {
                "leg": "BASE_CONTROL",
                "blocker_code": "CORPORATE_ACTION_FRACTIONAL_SHARES",
            },
            "base_control": None,
            "matched_net_reward_increment": None,
            "search_score": None,
            "productive": False,
            "blockers": ["CORPORATE_ACTION_FRACTIONAL_SHARES"],
        }
    )

    summary = {
        row["template_id"]: row for row in _template_summary(records)
    }["BASE_EVENT"]
    assert summary["program_proposed"] == 8
    assert summary["replay_complete"] == 7
    assert summary["replay_blocked"] == 1
    assert summary["economic_rows_complete"] == 7
    assert summary["productive"] == 7


def test_phase_b_worker_closes_candidate_local_blocker_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(phase_b_runner, "_WORKER_CONTEXT", {})
    monkeypatch.setattr(phase_b_runner, "_WORKER_REGISTRY", object())
    monkeypatch.setattr(phase_b_runner, "_WORKER_INPUT_HASH", "a" * 64)
    monkeypatch.setattr(phase_b_runner, "_WORKER_WINDOWS", ())
    monkeypatch.setattr(
        phase_b_runner,
        "_compiled",
        lambda schedule, *, program_key, compiled_key, registry: program_key,
    )

    def evaluate(compiled, *, context, windows):
        if compiled == "control_program":
            raise AShareCorporateActionFractionalSharesError(
                code="000001.SZ",
                session_date="2024-01-02",
                opening_shares=100,
                multiplier=1.001,
                adjusted_shares=100.1,
            )
        return {
            "behavior_identity": "primary-behavior",
            "continuous_book_net_reward": 0.01,
            "cumulative_net_return": 0.02,
            "fill_count": 3,
        }

    monkeypatch.setattr(phase_b_runner, "_evaluate_compiled", evaluate)
    target = tmp_path / "record_0029.json"
    payload = phase_b_runner._evaluate_record(
        {
            "semantic_noop": False,
            "primary_program": {"program_id": "primary"},
            "primary_compiled": {},
            "control_program": {"program_id": "control"},
            "control_compiled": {},
            "main_record_ordinal": 29,
            "template_id": "BASE_EVENT",
            "record_kind": "ENHANCED_FULL_BASE_PAIR",
            "schedule_record_sha256": "b" * 64,
            "pair_id": "pair-29",
            "proposal_receipt": {"proposal_receipt_sha256": "c" * 64},
        },
        str(target),
    )

    assert target.is_file()
    assert payload["replay_status"] == "PAIR_REPLAY_BLOCKED"
    assert payload["replay_blocker"]["leg"] == "BASE_CONTROL"
    assert (
        payload["replay_blocker"]["blocker_code"]
        == "CORPORATE_ACTION_FRACTIONAL_SHARES"
    )
    assert payload["primary"]["behavior_identity"] == "primary-behavior"
    assert payload["base_control"] is None
    assert payload["matched_net_reward_increment"] is None
    assert payload["search_score"] is None
    assert payload["productive"] is False
    assert payload["blockers"] == ["CORPORATE_ACTION_FRACTIONAL_SHARES"]


def test_phase_b_checkpoint_chain_replays_only_complete_records(
    tmp_path: Path,
) -> None:
    checkpoint = tmp_path / "checkpoint_001"
    record_root = checkpoint / "records"
    record_root.mkdir(parents=True)
    schedules = {}
    paths = []
    for ordinal in range(8):
        row = _closed_record(ordinal, "BASE")
        path = _write_json(record_root / f"record_{ordinal:04d}.json", row)
        paths.append(path)
        schedules[ordinal] = {
            "main_record_ordinal": ordinal,
            "schedule_record_sha256": f"{ordinal:064x}",
        }
    summary = _write_json(
        checkpoint / "checkpoint_summary.json",
        {"status": "JOINT_PROGRAM_PHASE_B_CHECKPOINT_COMPLETE"},
    )
    _batch_manifest(
        batch_root=checkpoint,
        batch_id="checkpoint_001",
        input_hashes={
            "phase_b_input_binding": "a" * 64,
            "prior_checkpoint_manifest": "GENESIS",
        },
        paths=[*paths, summary],
        access_receipts=[],
        evaluation_name="joint candidate-program development continuous-book evaluation",
    )
    manifest, rows = _verify_checkpoint(
        checkpoint,
        checkpoint_id="checkpoint_001",
        previous_manifest=None,
        input_hash="a" * 64,
        schedule_by_ordinal=schedules,
    )
    assert manifest.name == "batch_manifest.json"
    assert len(rows) == 8


def test_phase_b_parity_reports_exact_field_drift() -> None:
    left = {"signal_sha256": "a", "ending_nav_cny": 100.0}
    right = dict(left)
    assert _parity_differences(left, right) == []
    right["ending_nav_cny"] = 99.0
    assert _parity_differences(left, right) == ["ending_nav_cny"]


def test_phase_b_frozen_execution_contract_uses_emitted_executor_key() -> None:
    _validate_frozen_execution_contract(
        {
            "portfolio_decoder_id": "TOPK_10_EQUAL",
            "executor_backend": "PROCESS_POOL",
            "executor_workers": 10,
            "execution_contract_snapshot_file_sha256": "a" * 64,
            "node_resource_capacity_manifest_sha256": "b" * 64,
        },
        execution_contract_sha256="a" * 64,
        capacity_manifest_sha256="b" * 64,
        executor_workers=10,
    )


def test_phase_b_accepts_only_exact_legacy_development_sidecar(
    tmp_path: Path,
) -> None:
    shards = []
    for ordinal in range(16):
        path = tmp_path / f"shard_{ordinal:02d}.parquet"
        path.write_bytes(f"shard-{ordinal}".encode("utf-8"))
        shards.append(
            {
                "status": "TIME_MAJOR_SHARD_READY",
                "split_manifest_hash": "s" * 64,
                "output_path": str(path),
                "output_bytes": path.stat().st_size,
                "output_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "rows": 1,
            }
        )
    manifest = _self_hashed(
        {
            "schema_version": (
                "cn_development_time_major_execution_layout_manifest_v2_train_only"
            ),
            "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
            "data_role": "development_train_only",
            "split_manifest_hash": "s" * 64,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "source_shard_count": 16,
            "sidecar_rows": 16,
            "source_rows": 16,
            "fields": ["trade_time", "code", "close"],
            "shards": shards,
        },
        "manifest_hash",
    )
    manifest_path = _write_json(
        tmp_path / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json",
        manifest,
    )
    observed, observed_path = _validate_phase_b_materialized_sidecar(
        tmp_path,
        split_manifest_sha256="s" * 64,
        verify_shards=True,
        expected_manifest_file_sha256=hashlib.sha256(
            manifest_path.read_bytes()
        ).hexdigest(),
        expected_manifest_payload_sha256=manifest["manifest_hash"],
    )
    assert observed == manifest
    assert observed_path == manifest_path


def test_phase_b_accepts_exact_development_execution_price_sidecar(
    tmp_path: Path,
) -> None:
    shards = []
    for ordinal in range(16):
        path = tmp_path / f"price_{ordinal:02d}.parquet"
        path.write_bytes(f"price-{ordinal}".encode("utf-8"))
        shards.append(
            {
                "output_path": str(path),
                "output_bytes": path.stat().st_size,
                "output_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "rows": 1,
            }
        )
    manifest = {
        "schema_version": "cn_core_pack_report_only_session_sidecar_v2",
        "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
        "evaluation_role": "train",
        "data_role": "development_train_only",
        "split_manifest_hash": "s" * 64,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "source_shard_count": 16,
        "sidecar_rows": 16,
        "source_rows": 16,
        "fields": ["trade_time", "code", "open", "close"],
        "shards": shards,
    }
    manifest_path = _write_json(
        tmp_path / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json",
        manifest,
    )

    observed, observed_path = _validate_phase_b_execution_price_sidecar(
        tmp_path,
        split_manifest_sha256="s" * 64,
        expected_manifest_file_sha256=hashlib.sha256(
            manifest_path.read_bytes()
        ).hexdigest(),
    )

    assert observed == manifest
    assert observed_path == manifest_path
