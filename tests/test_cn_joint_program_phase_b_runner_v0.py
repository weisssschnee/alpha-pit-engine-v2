from __future__ import annotations

import json
import hashlib
from pathlib import Path

from our_system_phase2.runtime.cn_iterative_search_v1 import _batch_manifest
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import TEMPLATE_ORDER
from scripts.run_cn_joint_program_phase_b_v0 import (
    _parity_differences,
    _self_hashed,
    _template_summary,
    _validate_phase_b_execution_price_sidecar,
    _validate_phase_b_materialized_sidecar,
    _validate_frozen_execution_contract,
    _verify_checkpoint,
    _write_json,
)


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
