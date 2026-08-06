from __future__ import annotations

import json
import hashlib
from pathlib import Path

import pytest

import our_system_phase2.runtime.cn_joint_program_phase_b_v0 as phase_b_runtime

from our_system_phase2.runtime.cn_candidate_representation_v0_preflight import (
    build_candidate_representation_v0_preflight,
)
from our_system_phase2.runtime.cn_joint_program_phase_a_v0 import build_phase_a_v0
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import (
    FREEZE_CLOSURE_NAME,
    FREEZE_STATUS,
    build_phase_b_prefinancial_freeze_v0,
    verify_phase_b_prefinancial_freeze_v0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"
SPLIT = REPO / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
NODE_PROFILES = REPO / "runtime/run_plans/cn_alpha_node_resource_profiles_v1.json"


def _execution_contract(path: Path) -> Path:
    body = {
        "schema_version": "test_execution_contract_v1",
        "split_manifest_sha256": hashlib.sha256(SPLIT.read_bytes()).hexdigest(),
        "session_authority_manifest": "D:/development/session_manifest.json",
        "session_authority_manifest_sha256": "2" * 64,
        "session_authority_path": "D:/development/session_authority.parquet",
        "universe_policy": {
            "allowed_exchanges": ["SSE", "SZSE"],
            "long_only": True,
        },
        "fee_schedule": {"sell_stamp_duty_bps": 5.0},
        "execution_policy": {
            "signal_clock": "SESSION_CLOSE_T",
            "execution_clock": "NEXT_SESSION_OPEN_T_PLUS_1",
        },
        "corporate_action_policy": {"mode": "PIT_ONLY"},
    }
    body["contract_payload_sha256"] = stable_hash(body)
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _materialization_test_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: Path
) -> tuple[Path, Path, Path]:
    paths = tuple(
        tmp_path / name
        for name in (
            "fixed_v0_closure.json",
            "materialized_schema.json",
            "materialization_screen.json",
        )
    )
    for path in paths:
        path.write_text("{}\n", encoding="utf-8")
    source_rows = [
        json.loads(line)
        for line in (source / "compatible_candidate_rows_v0.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    compatible_pair_ids = {str(row["pair_id"]) for row in source_rows}
    monkeypatch.setattr(
        phase_b_runtime,
        "_materialized_pair_ids",
        lambda **_: (
            compatible_pair_ids,
            {"screen_payload_sha256": "c" * 64},
        ),
    )
    return paths


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    phase_a = tmp_path / "phase_a"
    build_phase_a_v0(
        output_root=phase_a,
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        repo_sha="a" * 40,
    )
    source = tmp_path / "source_preflight"
    build_candidate_representation_v0_preflight(
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        output_root=source,
        quota_per_template=8,
        seeds=(1729,),
    )
    closure_snapshot, schema_snapshot, screen_snapshot = (
        _materialization_test_inputs(tmp_path, monkeypatch, source)
    )
    output = tmp_path / "phase_b_freeze"
    closure = build_phase_b_prefinancial_freeze_v0(
        output_root=output,
        phase_a_root=phase_a,
        source_preflight_root=source,
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        split_manifest_path=SPLIT,
        execution_contract_snapshot_path=_execution_contract(
            tmp_path / "execution_contract.json"
        ),
        fixed_v0_production_closure_snapshot_path=closure_snapshot,
        materialized_schema_snapshot_path=schema_snapshot,
        materialization_screen_snapshot_path=screen_snapshot,
        node_resource_profiles_path=NODE_PROFILES,
        repo_sha="a" * 40,
        remote_train_session_field_root=(
            "D:/ChengboRemote/runtime/development/session_time_major_train_v1"
        ),
    )
    assert closure["status"] == FREEZE_STATUS
    return output


def test_phase_b_prefinancial_freeze_is_exact_uniform_64_and_replays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _build(tmp_path, monkeypatch)
    closure = verify_phase_b_prefinancial_freeze_v0(root)
    assert closure["main_record_count"] == 64
    assert closure["base_parity_record_count"] == 8
    assert closure["enhanced_full_base_pair_count"] == 56
    assert closure["financial_evaluation_executed"] is False
    assert closure["validation_reads"] == 0
    assert closure["historical_2023_reads"] == 0
    schedule = [
        json.loads(line)
        for line in (root / "phase_b_uniform_schedule.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert len(schedule) == 64
    assert {row["generation_arm"] for row in schedule} == {"UNIFORM_FRESH"}
    assert not any(row["adaptive_template_credit_used"] for row in schedule)
    assert all(
        component["route_id"]
        in {
            "SLOW_CROSS_SECTIONAL_LEVEL",
            "SLOW_TEMPORAL_CHANGE",
            "MARKET_REGIME_CONDITION",
            "DISCLOSURE_EVENT",
        }
        for row in schedule
        for component in row["components"].values()
    )
    contract = json.loads(
        (root / "phase_b_run_contract.json").read_text(encoding="utf-8")
    )
    assert contract["development_session_count"] == 364
    assert [
        row["session_count"] for row in contract["development_subwindows"]
    ] == [121, 121, 122]
    assert contract["executor_workers"] == 12
    assert contract["entitlement_threads"] == 32


def test_phase_b_prefinancial_verifier_rejects_schedule_tampering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _build(tmp_path, monkeypatch)
    schedule = root / "phase_b_uniform_schedule.jsonl"
    rows = schedule.read_text(encoding="utf-8").splitlines()
    first = json.loads(rows[0])
    first["generation_arm"] = "FACTORIZED_EXPLOIT"
    rows[0] = json.dumps(first, sort_keys=True)
    schedule.write_text("\n".join(rows) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="artifact .*drift"):
        verify_phase_b_prefinancial_freeze_v0(root)


def test_phase_b_execution_snapshot_rejects_prohibited_asset_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    phase_a = tmp_path / "phase_a"
    build_phase_a_v0(
        output_root=phase_a,
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        repo_sha="b" * 40,
    )
    source = tmp_path / "source_preflight"
    build_candidate_representation_v0_preflight(
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        output_root=source,
        quota_per_template=8,
        seeds=(1729,),
    )
    execution_path = _execution_contract(tmp_path / "execution_contract.json")
    payload = json.loads(execution_path.read_text(encoding="utf-8"))
    payload.pop("contract_payload_sha256")
    payload["session_authority_path"] = "D:/Forward_B/session.parquet"
    payload["contract_payload_sha256"] = stable_hash(payload)
    execution_path.write_text(json.dumps(payload), encoding="utf-8")
    closure_snapshot, schema_snapshot, screen_snapshot = (
        _materialization_test_inputs(tmp_path, monkeypatch, source)
    )
    with pytest.raises(ValueError, match="prohibited asset"):
        build_phase_b_prefinancial_freeze_v0(
            output_root=tmp_path / "phase_b_freeze",
            phase_a_root=phase_a,
            source_preflight_root=source,
            registry_path=REGISTRY,
            root_contract_path=ROOT_CONTRACT,
            split_manifest_path=SPLIT,
            execution_contract_snapshot_path=execution_path,
            fixed_v0_production_closure_snapshot_path=closure_snapshot,
            materialized_schema_snapshot_path=schema_snapshot,
            materialization_screen_snapshot_path=screen_snapshot,
            node_resource_profiles_path=NODE_PROFILES,
            repo_sha="b" * 40,
            remote_train_session_field_root=(
                "D:/ChengboRemote/runtime/development/"
                "session_time_major_train_v1"
            ),
        )


def test_phase_b_closure_name_is_stable() -> None:
    assert FREEZE_CLOSURE_NAME == (
        "CN_JOINT_PROGRAM_PHASE_B_PREFINANCIAL_FREEZE_COMPLETE.json"
    )
