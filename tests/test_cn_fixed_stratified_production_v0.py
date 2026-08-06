from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from our_system_phase2.runtime.cn_fixed_stratified_production_v0 import (
    _require_execution_authority,
    _require_sidecar_authority,
    build_data_input_inventory_v0,
    build_materialization_underfill_gate_v0,
    build_route_production_metrics_v0,
    require_fresh_output_root_v0,
    require_zero_prohibited_reads_v0,
    screen_materialized_candidate_rows_v0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


def test_route_metrics_distinguish_evaluation_yield_and_economic_yield() -> None:
    outcomes = [
        {
            "pair_id": "pair-a",
            "pair_evaluation_status": "PAIR_EVALUATED",
            "primary_composite_reward": 0.4,
            "matched_train_increment": 0.2,
            "primary_standalone_train_reward_decision": (
                "TRAIN_REWARD_FOLLOWUP_READY"
            ),
        },
        {
            "pair_id": "pair-b",
            "pair_evaluation_status": "PAIR_EVALUATED",
            "primary_composite_reward": 0.1,
            "matched_train_increment": -0.3,
            "primary_standalone_train_reward_decision": (
                "TRAIN_REWARD_FOLLOWUP_READY"
            ),
        },
    ]
    behavior = [
        {"pair_id": "pair-a", "portfolio_behavior_family_id": "family-a"},
        {"pair_id": "pair-b", "portfolio_behavior_family_id": "family-b"},
    ]

    row = build_route_production_metrics_v0(
        route_id="MINUTE_STATIC",
        scheduled_attempts=4,
        unique_pairs=2,
        outcomes=outcomes,
        behavior_rows=behavior,
        wall_seconds=360.0,
        compute_threads=32,
    )

    assert row["production_evidence_state"] == "TRAIN_ONLY_EVALUATED"
    assert row["pair_evaluated"] == 2
    assert row["primary_exact_unique"] == 2
    assert row["materialized_pair_compatible"] == 2
    assert row["materialization_incompatible"] == 0
    assert row["evaluator_fill_ratio"] == 1.0
    assert row["standalone_positive"] == 2
    assert row["matched_positive"] == 1
    assert row["development_productive"] == 1
    assert row["behavior_family_unique"] == 2
    assert row["pair_evaluated_per_wall_hour"] == pytest.approx(20.0)
    assert row["candidate_member_evaluated_per_wall_hour"] == pytest.approx(
        40.0
    )
    assert row["productive_per_entitled_core_hour"] == pytest.approx(1 / 3.2)


def test_materialization_screen_preserves_fixed_underfill_without_spillover() -> None:
    def pair_rows(pair_id: str, route_id: str, field_id: str) -> list[dict]:
        return [
            {
                "pair_id": pair_id,
                "pair_member_role": role,
                "candidate_id": f"{pair_id}-{role.lower()}",
                "route_id": route_id,
                "declared_field_ids": [field_id],
                "field_ids": [field_id],
                "condition_field_ids": [],
            }
            for role in ("PRIMARY", "CONTROL")
        ]

    rows = [
        *pair_rows("pair-session-ok", "SLOW_TEMPORAL_CHANGE", "session_ok"),
        *pair_rows(
            "pair-session-missing",
            "SLOW_TEMPORAL_CHANGE",
            "session_missing",
        ),
    ]
    for route_id in (
        "MINUTE_STATIC",
        "FIRSTN_PATH",
        "SLOW_CROSS_SECTIONAL_LEVEL",
        "MARKET_REGIME_CONDITION",
        "INTRADAY_STATE_TRANSITION",
        "DISCLOSURE_EVENT",
        "BROAD_EVENT_FROZEN_ENTRY",
    ):
        rows.extend(pair_rows(f"pair-{route_id}", route_id, "shared_ok"))

    compatible, screen = screen_materialized_candidate_rows_v0(
        candidate_rows=rows,
        schema_by_backend={
            "active_bar": {"shared_ok"},
            "stock_session": {"shared_ok", "session_ok"},
        },
    )

    assert len(compatible) == len(rows) - 2
    assert all(
        row["pair_id"] != "pair-session-missing" for row in compatible
    )
    slow = next(
        row
        for row in screen["route_waterfall"]
        if row["route_id"] == "SLOW_TEMPORAL_CHANGE"
    )
    assert slow["frozen_pairs"] == 2
    assert slow["materialized_pair_compatible"] == 1
    assert slow["materialization_incompatible"] == 1
    assert screen["replacement_allowed"] is False
    assert screen["cross_template_spillover_allowed"] is False
    assert screen["dynamic_budget_reallocation_allowed"] is False


def test_zero_materialized_stratum_is_reportable_without_backend_launch() -> None:
    row = build_route_production_metrics_v0(
        route_id="BROAD_EVENT_FROZEN_ENTRY",
        scheduled_attempts=32,
        unique_pairs=0,
        frozen_unique_pairs=11,
        materialization_incompatible_pairs=11,
        outcomes=(),
        behavior_rows=(),
        wall_seconds=0.25,
        compute_threads=32,
    )
    gate = build_materialization_underfill_gate_v0(
        route_id="BROAD_EVENT_FROZEN_ENTRY",
        minimum_free_memory_bytes=64 * 1024**3,
    )

    assert row["production_evidence_state"] == (
        "PREFINANCIAL_MATERIALIZATION_UNDERFILL"
    )
    assert row["primary_exact_unique"] == 11
    assert row["materialized_pair_compatible"] == 0
    assert row["materialization_incompatible"] == 11
    assert row["total_prefinancial_underfill"] == 32
    assert gate["semantic_integrity_status"] == "PASS"
    assert gate["backends"] == {}


def test_production_boundary_rejects_any_nested_prohibited_read() -> None:
    require_zero_prohibited_reads_v0(
        {
            "validation_reads": 0,
            "nested": {
                "holdout_read_count": 0,
                "forward_2026_reads": 0,
                "sealed_data_read_count": 0,
            },
        }
    )

    with pytest.raises(RuntimeError, match="prohibited read"):
        require_zero_prohibited_reads_v0(
            {"nested": {"validation_read_count": 1}}
        )


def test_fresh_root_allows_only_runner_deployment_binding(
    tmp_path: Path,
) -> None:
    root = tmp_path / "run"
    root.mkdir()
    (root / "deployment_binding.json").write_text("{}", encoding="utf-8")
    require_fresh_output_root_v0(root)

    (root / "stale.json").write_text("{}", encoding="utf-8")
    with pytest.raises(FileExistsError, match="not fresh"):
        require_fresh_output_root_v0(root)


def test_execution_authority_fails_closed_without_repo_sha(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "our_system_phase2.runtime.cn_fixed_stratified_production_v0."
        "platform.node",
        lambda: "DESKTOP-77OPJ6F",
    )
    monkeypatch.delenv("CN_CAMPAIGN_REPO_SHA", raising=False)

    with pytest.raises(RuntimeError, match="repo SHA missing"):
        _require_execution_authority(output_root=tmp_path, compute_threads=32)


def test_sidecar_authority_binds_roots_split_and_sealed_reads(
    tmp_path: Path,
) -> None:
    active = tmp_path / "active"
    session = tmp_path / "session"
    active.mkdir()
    session.mkdir()
    split = tmp_path / "split.csv"
    split.write_text("trade_date,split\n2024-01-02,train\n", encoding="utf-8")
    split_sha = hashlib.sha256(split.read_bytes()).hexdigest()
    closure = {
        "schema_version": "cn_phase3cm_1024_sidecar_closure_v1",
        "status": "CN_PHASE3CM_1024_SIDECAR_CLOSURE_PASS",
        "data_role": "development_train_only",
        "active_sidecar": {"root": str(active)},
        "session_sidecar": {"root": str(session)},
        "split_manifest": {"path": str(split), "sha256": split_sha},
        "sealed_reads": {
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    closure["closure_hash"] = stable_hash(closure)
    closure_path = tmp_path / "closure.json"
    closure_path.write_text(json.dumps(closure), encoding="utf-8")

    _require_sidecar_authority(
        closure_path=closure_path,
        split_manifest=split,
        active_field_root=active,
        session_field_root=session,
    )
    closure["validation_reads"] = 1
    closure.pop("closure_hash")
    closure["closure_hash"] = stable_hash(closure)
    closure_path.write_text(json.dumps(closure), encoding="utf-8")
    with pytest.raises(RuntimeError, match="prohibited read"):
        _require_sidecar_authority(
            closure_path=closure_path,
            split_manifest=split,
            active_field_root=active,
            session_field_root=session,
        )


def test_data_inventory_requires_every_consumed_shard_declaration(
    tmp_path: Path,
) -> None:
    split = tmp_path / "split.csv"
    split.write_text("trade_date,split\n2024-01-02,train\n", encoding="utf-8")
    split_sha = hashlib.sha256(split.read_bytes()).hexdigest()
    roots = {
        name: tmp_path / name
        for name in (
            "active_field",
            "session_field",
            "active_label",
            "session_label",
        )
    }
    for root in roots.values():
        root.mkdir()
        (root / "shard_00.parquet").write_bytes(b"frozen-shard")
    shard_sha = hashlib.sha256(b"frozen-shard").hexdigest()

    def write_manifest(path: Path, *, label: bool = False) -> str:
        payload = {
            "schema_version": "fixture",
            "status": "LABEL_SIDECARS_READY" if label else "READY",
            "shards": [
                {
                    "output_path": str(path.parent / "shard_00.parquet"),
                    "output_sha256": shard_sha,
                    "output_bytes": len(b"frozen-shard"),
                    "rows": 1,
                }
            ],
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }
        if label:
            payload.update(
                {
                    "data_role": "development_train_only",
                    "split_manifest_hash": split_sha,
                }
            )
        path.write_text(json.dumps(payload), encoding="utf-8")
        return hashlib.sha256(path.read_bytes()).hexdigest()

    active_manifest = roots["active_field"] / "active.json"
    session_augmentation = roots["session_field"] / "augmentation.json"
    session_v2 = roots["session_field"] / "v2.json"
    active_sha = write_manifest(active_manifest)
    augmentation_sha = write_manifest(session_augmentation)
    v2_sha = write_manifest(session_v2)
    write_manifest(
        roots["active_label"] / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json",
        label=True,
    )
    write_manifest(
        roots["session_label"] / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json",
        label=True,
    )
    sidecar = {
        "active_sidecar": {
            "manifest_path": str(active_manifest),
            "manifest_sha256": active_sha,
        },
        "session_sidecar": {
            "augmentation_manifest_path": str(session_augmentation),
            "augmentation_manifest_sha256": augmentation_sha,
            "v2_manifest_path": str(session_v2),
            "v2_manifest_sha256": v2_sha,
        },
    }
    inventory = build_data_input_inventory_v0(
        sidecar_authority=sidecar,
        split_manifest=split,
        field_roots={
            "active_bar": roots["active_field"],
            "stock_session": roots["session_field"],
        },
        label_roots={
            "active_bar": roots["active_label"],
            "stock_session": roots["session_label"],
        },
    )
    assert inventory["status"] == "DEVELOPMENT_TRAIN_INPUTS_BOUND"

    active_shard = roots["active_field"] / "shard_00.parquet"
    active_shard.write_bytes(b"mutant-shard")
    assert active_shard.stat().st_size == len(b"frozen-shard")
    with pytest.raises(RuntimeError, match="shard hash drift"):
        build_data_input_inventory_v0(
            sidecar_authority=sidecar,
            split_manifest=split,
            field_roots={
                "active_bar": roots["active_field"],
                "stock_session": roots["session_field"],
            },
            label_roots={
                "active_bar": roots["active_label"],
                "stock_session": roots["session_label"],
            },
        )
    active_shard.write_bytes(b"frozen-shard")

    (roots["active_label"] / "shard_01.parquet").write_bytes(b"undeclared")
    with pytest.raises(RuntimeError, match="shard inventory drift"):
        build_data_input_inventory_v0(
            sidecar_authority=sidecar,
            split_manifest=split,
            field_roots={
                "active_bar": roots["active_field"],
                "stock_session": roots["session_field"],
            },
            label_roots={
                "active_bar": roots["active_label"],
                "stock_session": roots["session_label"],
            },
        )


def test_remote_wrapper_uses_windows_powershell51_safe_manifest_fallback() -> None:
    wrapper = (
        Path(__file__).parents[1]
        / "scripts"
        / "run_cn_fixed_stratified_production_v0_77o.ps1"
    ).read_text(encoding="utf-8")
    assert "$manifestSha = if ($manifest.head)" in wrapper
    assert "$manifestWorkspace = if ($manifest.workspace)" in wrapper
    assert "$manifestSha = [string](" not in wrapper
    assert "$manifestWorkspace = [string](" not in wrapper
