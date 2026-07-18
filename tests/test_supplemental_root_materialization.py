from __future__ import annotations

import hashlib
import json
from argparse import Namespace
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.services.feature_state_fabric import (
    materialize_registered_close_range_state,
)
from our_system_phase2.services.fundamental_representations import (
    CanonicalFundamentalMaterializer,
)
from our_system_phase2.services.materialization_support_receipt import (
    build_materialization_support_receipt,
    verify_materialization_support_receipt,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from scripts.run_signal_sketch_audit import (
    _verify_gated_panel_receipts,
    materialize_state_authority,
)
from scripts.run_cn_core_pack_supplemental_generation import (
    load_runtime_receipt_bindings,
)
from scripts.prepare_cn_compositional_session_signal_panel import (
    assemble as assemble_session_panel,
    materialize as materialize_session_panel,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY_PATH = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)


def _registry() -> UnifiedCapabilityRegistry:
    return UnifiedCapabilityRegistry.read(REGISTRY_PATH)


def test_registered_close_range_state_materializes_exactly_and_is_order_invariant() -> None:
    registry = _registry()
    capability = registry.resolve("state_close_range_location_sign")
    frame = pd.DataFrame(
        {
            "code": ["B", "A", "A", "B"],
            "trade_time": pd.to_datetime(
                [
                    "2024-01-02 09:31",
                    "2024-01-02 09:31",
                    "2024-01-02 09:32",
                    "2024-01-02 09:32",
                ]
            ),
            "close": [10.0, 10.0, np.nan, 9.0],
            "high": [11.0, 12.0, 12.0, 10.0],
            "low": [8.0, 9.0, 8.0, 8.0],
        }
    )

    first, first_manifest = materialize_registered_close_range_state(
        frame, capability, registry_hash=registry.registry_hash
    )
    second, second_manifest = materialize_registered_close_range_state(
        frame.iloc[[2, 0, 3, 1]], capability, registry_hash=registry.registry_hash
    )

    pd.testing.assert_frame_equal(first, second)
    assert first["state_close_range_location_sign"].iloc[0] == -1.0
    assert np.isnan(first["state_close_range_location_sign"].iloc[1])
    assert first["state_close_range_location_sign"].iloc[2] == 1.0
    assert first["state_close_range_location_sign"].iloc[3] == 0.0
    assert first_manifest["output_fingerprint"] == second_manifest["output_fingerprint"]
    assert first_manifest["unified_registry_hash"] == registry.registry_hash
    assert first_manifest["registered_materialization_expression"] == (
        "Sign(Sub(Mul($close,2),Add($high,$low)))"
    )


def test_registered_close_range_state_rejects_registry_semantic_drift() -> None:
    registry = _registry()
    capability = registry.resolve("state_close_range_location_sign")
    tampered = replace(
        capability,
        metadata={
            **dict(capability.metadata or {}),
            "materialization_expression": "Sign(Sub($close,$open))",
        },
    )
    frame = pd.DataFrame(
        {
            "code": ["A"],
            "trade_time": pd.to_datetime(["2024-01-02 09:31"]),
            "close": [10.0],
            "high": [11.0],
            "low": [9.0],
        }
    )

    with pytest.raises(ValueError, match="materialization contract drift"):
        materialize_registered_close_range_state(
            frame, tampered, registry_hash=registry.registry_hash
        )


def test_full_active_state_authority_has_real_artifact_closure(tmp_path: Path) -> None:
    release = tmp_path / "development_only_release_manifest.json"
    release.write_text(
        json.dumps(
            {
                "release_hash": "a" * 64,
                "forbidden_roles_present": [],
                "forward_2026_present": False,
            }
        ),
        encoding="utf-8",
    )
    split = tmp_path / "split.csv"
    pd.DataFrame(
        {"trade_date": ["2024-01-02", "2024-01-03"], "split": ["train", "train"]}
    ).to_csv(split, index=False)
    source = tmp_path / "full_active.parquet"
    pd.DataFrame(
        {
            "code": ["A", "A", "B", "B"],
            "trade_time": pd.to_datetime(
                [
                    "2024-01-02 09:31",
                    "2024-01-03 09:31",
                    "2024-01-02 09:31",
                    "2024-01-03 09:31",
                ]
            ),
            "close": [10.0, 11.0, 9.0, 10.0],
            "high": [11.0, 12.0, 10.0, 11.0],
            "low": [8.0, 10.0, 8.0, 9.0],
        }
    ).to_parquet(source, index=False)
    output = tmp_path / "authority"

    assert materialize_state_authority(
        Namespace(
            full_active_panel=source,
            release_manifest=release,
            split_manifest=split,
            capability_registry=REGISTRY_PATH,
            output_root=output,
        )
    ) == 0
    receipt_path = output / (
        "state_close_range_location_sign.full_authority_receipt.json"
    )
    bindings = load_runtime_receipt_bindings([receipt_path], registry=_registry())
    assert set(bindings) == {"state_close_range_location_sign"}


def test_state_authority_rejects_forward_release_before_reading_panel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = tmp_path / "release.json"
    release.write_text(
        json.dumps(
            {
                "release_hash": "b" * 64,
                "forbidden_roles_present": [],
                "forward_2026_present": True,
            }
        ),
        encoding="utf-8",
    )
    split = tmp_path / "split.csv"
    pd.DataFrame({"trade_date": ["2024-01-02"], "split": ["train"]}).to_csv(
        split, index=False
    )

    def forbidden_read(*args: object, **kwargs: object) -> pd.DataFrame:
        raise AssertionError("sealed panel must not be opened")

    monkeypatch.setattr(pd, "read_parquet", forbidden_read)
    with pytest.raises(PermissionError, match="not development-only"):
        materialize_state_authority(
            Namespace(
                full_active_panel=tmp_path / "sealed.parquet",
                release_manifest=release,
                split_manifest=split,
                capability_registry=REGISTRY_PATH,
                output_root=tmp_path / "out",
            )
        )


class _AgeFixtureAdapter:
    def __init__(self) -> None:
        self.sessions = pd.DatetimeIndex(
            pd.to_datetime(["2024-04-19", "2024-04-22", "2024-04-23"])
        )

    def materialize_level(self, request: object, coordinates: pd.DataFrame) -> pd.DataFrame:
        output = coordinates.copy()
        output[str(request.output_name)] = 1.0
        output["observable_time"] = pd.to_datetime(
            ["2024-04-19 09:30", "2024-04-19 09:30", "2024-04-24 09:30"]
        )
        return output


@pytest.mark.parametrize(
    "field_id",
    [
        "fund_disclosure_balance_age_sessions",
        "fund_disclosure_cashflow_age_sessions",
        "fund_disclosure_holder_age_sessions",
        "fund_disclosure_profit_age_sessions",
    ],
)
def test_disclosure_age_uses_declared_trading_sessions_and_future_clock_fails_closed(
    field_id: str,
) -> None:
    materializer = CanonicalFundamentalMaterializer(_AgeFixtureAdapter())
    coordinates = pd.DataFrame(
        {
            "code": ["000001", "000001", "000001"],
            "session_time": pd.to_datetime(
                ["2024-04-19 15:00", "2024-04-22 15:00", "2024-04-23 15:00"]
            ),
        }
    )
    capability = _registry().resolve(field_id)
    spec = dict((capability.metadata or {})["canonical_representation"])

    output = materializer.materialize(spec, coordinates)

    assert output[field_id].iloc[0] == 0.0
    assert output[field_id].iloc[1] == 1.0
    assert np.isnan(output[field_id].iloc[2])


def _state_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": ["A", "A", "A", "B"],
            "trade_time": pd.to_datetime(
                [
                    "2024-01-02 09:31",
                    "2024-01-02 09:32",
                    "2024-01-02 09:33",
                    "2024-01-02 09:31",
                ]
            ),
            "state_close_range_location_sign": [-1.0, -1.0, 1.0, 0.0],
        }
    )


def test_materialization_support_receipt_binds_values_support_and_partition() -> None:
    registry = _registry()
    capability = registry.resolve("state_close_range_location_sign")
    frame = _state_frame()
    receipt = build_materialization_support_receipt(
        frame,
        field_id=capability.field_id,
        representation_id=capability.representation_id,
        registry_hash=registry.registry_hash,
        receipt_type="FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
        materializer_authority="FeatureStateFabric",
        materializer_manifest={"output_fingerprint": "fixture"},
        support_unit=capability.support_unit,
        observable_time_contract=capability.observable_clock,
        maturity_contract=capability.maturity_rule,
        row_keys=("code", "trade_time"),
        partition_identity={"partition_index": 0, "partition_count": 2},
    )

    verified = verify_materialization_support_receipt(
        receipt,
        expected_field_id=capability.field_id,
        expected_registry_hash=registry.registry_hash,
        expected_partition_identity={"partition_index": 0, "partition_count": 2},
        required_receipt_type="FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
    )

    assert verified["materialization_status"] == "MATERIALIZED_DEVELOPMENT_ONLY"
    assert verified["support"]["support_count"] == 3
    assert verified["support"]["episode_count"] == 3
    assert verified["access_ledger"]["forward_2026_rows_read"] == 0


def test_materialization_support_receipt_rejects_tamper_partition_and_2026() -> None:
    registry = _registry()
    capability = registry.resolve("state_close_range_location_sign")
    receipt = build_materialization_support_receipt(
        _state_frame(),
        field_id=capability.field_id,
        representation_id=capability.representation_id,
        registry_hash=registry.registry_hash,
        receipt_type="FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
        materializer_authority="FeatureStateFabric",
        materializer_manifest={"output_fingerprint": "fixture"},
        support_unit=capability.support_unit,
        observable_time_contract=capability.observable_clock,
        maturity_contract=capability.maturity_rule,
        row_keys=("code", "trade_time"),
        partition_identity={"partition_index": 0, "partition_count": 2},
    )
    tampered = {**receipt, "support": {**receipt["support"], "support_count": 999}}

    with pytest.raises(ValueError, match="self-hash"):
        verify_materialization_support_receipt(tampered)
    with pytest.raises(ValueError, match="partition identity"):
        verify_materialization_support_receipt(
            receipt,
            expected_partition_identity={"partition_index": 1, "partition_count": 2},
        )

    future = _state_frame()
    future["trade_time"] = pd.to_datetime(
        ["2026-01-02 09:31", "2026-01-02 09:32", "2026-01-02 09:33", "2026-01-02 09:31"]
    )
    with pytest.raises(PermissionError, match="2026"):
        build_materialization_support_receipt(
            future,
            field_id=capability.field_id,
            representation_id=capability.representation_id,
            registry_hash=registry.registry_hash,
            receipt_type="FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
            materializer_authority="FeatureStateFabric",
            materializer_manifest={},
            support_unit=capability.support_unit,
            observable_time_contract=capability.observable_clock,
            maturity_contract=capability.maturity_rule,
            row_keys=("code", "trade_time"),
        )


def test_signal_sketch_worker_gate_requires_and_replays_panel_bound_receipt(
    tmp_path: Path,
) -> None:
    registry = _registry()
    capability = registry.resolve("state_close_range_location_sign")
    frame = _state_frame()
    receipt = build_materialization_support_receipt(
        frame,
        field_id=capability.field_id,
        representation_id=capability.representation_id,
        registry_hash=registry.registry_hash,
        receipt_type="FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
        materializer_authority="FeatureStateFabric",
        materializer_manifest={"output_fingerprint": "fixture"},
        support_unit=capability.support_unit,
        observable_time_contract=capability.observable_clock,
        maturity_contract=capability.maturity_rule,
        row_keys=("code", "trade_time"),
        evidence_contract={
            "evidence_role": "STAGE_MATERIALIZATION",
            "parent_authority_receipt_hash": "a" * 64,
            "stage": "FROZEN_A_B_SIGNAL_SKETCH_PANEL",
        },
    )
    receipt_path = tmp_path / "state_receipt.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    candidates = [
        {
            "candidate_id": "state-transition",
            "expression": (
                "Transition($state_close_range_location_sign,-1,1)"
            ),
            "materialization_support_receipt_hashes": json.dumps(
                {capability.field_id: "a" * 64},
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
    ]

    with pytest.raises(RuntimeError, match="lacks materialization receipts"):
        _verify_gated_panel_receipts(
            frame,
            candidates,
            capability_registry_path=REGISTRY_PATH,
            receipt_paths=[],
        )
    bindings = _verify_gated_panel_receipts(
        frame,
        candidates,
        capability_registry_path=REGISTRY_PATH,
        receipt_paths=[receipt_path],
    )
    assert bindings == {capability.field_id: "a" * 64}

    forged_candidates = [
        {
            **candidates[0],
            "materialization_support_receipt_hashes": json.dumps(
                {capability.field_id: "0" * 64}
            ),
        }
    ]
    with pytest.raises(RuntimeError, match="does not match verified panel"):
        _verify_gated_panel_receipts(
            frame,
            forged_candidates,
            capability_registry_path=REGISTRY_PATH,
            receipt_paths=[receipt_path],
        )

    changed = frame.copy()
    changed.loc[0, capability.field_id] = 1.0
    with pytest.raises(ValueError, match="panel differs"):
        _verify_gated_panel_receipts(
            changed,
            candidates,
            capability_registry_path=REGISTRY_PATH,
            receipt_paths=[receipt_path],
        )


def test_session_panel_materializes_age_and_emits_panel_bound_receipt(
    tmp_path: Path,
) -> None:
    field_id = "fund_disclosure_balance_age_sessions"
    source_root = tmp_path / "fundamental"
    table_root = source_root / "balance_sheet_report_em"
    table_root.mkdir(parents=True)
    pd.DataFrame(
        {
            "source_code6": ["000001"],
            "SECURITY_CODE": ["000001"],
            "REPORT_DATE": pd.to_datetime(["2023-12-31"]),
            "NOTICE_DATE": pd.to_datetime(["2024-04-19"]),
            "UPDATE_DATE": pd.to_datetime(["2024-04-19"]),
            "TOTAL_ASSETS": [100.0],
        }
    ).to_parquet(table_root / "SZ000001.parquet", index=False)
    split = tmp_path / "split.csv"
    pd.DataFrame(
        {
            "trade_date": ["2024-04-19", "2024-04-22", "2024-04-23"],
            "split": ["train", "train", "train"],
        }
    ).to_csv(split, index=False)
    base = pd.DataFrame(
        {
            "code": ["000001"] * 3,
            "trade_time": pd.to_datetime(
                ["2024-04-19 15:00", "2024-04-22 15:00", "2024-04-23 15:00"]
            ),
            "session_time": pd.to_datetime(
                ["2024-04-19 15:00", "2024-04-22 15:00", "2024-04-23 15:00"]
            ),
            "date": pd.to_datetime(["2024-04-19", "2024-04-22", "2024-04-23"]),
        }
    )
    base_path = tmp_path / "base.parquet"
    base.to_parquet(base_path, index=False)
    split_sha256 = hashlib.sha256(split.read_bytes()).hexdigest()
    sidecar_authority = tmp_path / "CN_SESSION_SIDECAR_AUGMENTATION_MANIFEST.json"
    sidecar_payload = {
        "schema_version": "cn_phase3cm_session_sidecar_augmentation_manifest_v1",
        "status": "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS",
        "row_count": len(base),
        "shard_count": 1,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    sidecar_authority.write_text(json.dumps(sidecar_payload), encoding="utf-8")
    base_manifest = tmp_path / "session_signal_base_manifest.json"
    base_manifest_payload = {
        "schema_version": "cn_full_development_session_coordinate_base_v1",
        "status": "FULL_DEVELOPMENT_SESSION_BASE_PREPARED",
        "data_role": "development_train_only",
        "labels_or_returns_read": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "development_session_count": 3,
        "stock_count": 1,
        "base_panel_rows": len(base),
        "coordinate_unique": True,
        "inputs": {
            "session_sidecar_manifest": {
                "path": str(sidecar_authority.resolve()),
                "sha256": hashlib.sha256(sidecar_authority.read_bytes()).hexdigest(),
            },
            "split_manifest": {
                "path": str(split.resolve()),
                "sha256": split_sha256,
            },
        },
        "artifacts": {
            "base_panel": {
                "path": str(base_path.resolve()),
                "sha256": hashlib.sha256(base_path.read_bytes()).hexdigest(),
            }
        },
    }
    base_manifest.write_text(json.dumps(base_manifest_payload), encoding="utf-8")
    registry = _registry()
    input_manifest = tmp_path / "input_manifest.json"
    input_manifest.write_text(
        json.dumps(
            {
                "canonical_fundamental_fields": [field_id],
                "registry_hash": registry.registry_hash,
            }
        ),
        encoding="utf-8",
    )
    fundamental_manifest = tmp_path / "pit_sidecar_manifest.json"
    fundamental_manifest.write_text(
        json.dumps(
            {
                "manifest_version": "fixture",
                "source_root": str(source_root.resolve()),
                "split_manifest_sha256": split_sha256,
                "development_maximum_observable_time": "2024-04-23 15:00",
            }
        ),
        encoding="utf-8",
    )
    cache_root = tmp_path / "cache"
    result = materialize_session_panel(
        Namespace(
            base_panel=base_path,
            base_manifest=base_manifest,
            split_manifest=split,
            registry=REGISTRY_PATH,
            input_manifest=input_manifest,
            fundamental_root=source_root,
            fundamental_manifest=fundamental_manifest,
            maximum_observable_time="2024-04-23 15:00",
            cache_root=cache_root,
            partition_index=0,
            partition_count=1,
        )
    )
    assert result == 0
    materialized = pd.read_parquet(cache_root / f"{field_id}.parquet")
    assert np.isnan(materialized[field_id].iloc[0])
    assert materialized[field_id].iloc[1:].tolist() == [0.0, 1.0]
    cache_receipt = json.loads(
        (
            cache_root / f"{field_id}.materialization_support_receipt.json"
        ).read_text(encoding="utf-8")
    )
    assert cache_receipt["source_binding"]["fundamental_root"] == str(
        source_root.resolve()
    )

    coordinates = tmp_path / "coordinates.csv"
    pd.DataFrame(
        {
            "coordinate_id": ["a", "b", "c"],
            "coordinate_set": ["A", "B", "A"],
            "code": ["000001"] * 3,
            "trade_time": base["trade_time"].astype(str),
        }
    ).to_csv(coordinates, index=False)
    output_root = tmp_path / "assembled"
    assert (
        assemble_session_panel(
            Namespace(
                base_panel=base_path,
                base_manifest=base_manifest,
                coordinate_manifest=coordinates,
                input_manifest=input_manifest,
                cache_root=cache_root,
                output_root=output_root,
                source_release=tmp_path / "source_release.json",
                fundamental_root=source_root,
                fundamental_manifest=fundamental_manifest,
                split_manifest=split,
                maximum_observable_time="2024-04-23 15:00",
            )
        )
        == 0
    )
    panel = pd.read_parquet(output_root / "signal_sketch_compact_panel.parquet")
    panel_receipt = output_root / f"{field_id}.materialization_support_receipt.json"
    panel_receipt_payload = json.loads(panel_receipt.read_text(encoding="utf-8"))
    runtime_bindings = load_runtime_receipt_bindings(
        [panel_receipt],
        registry=registry,
    )
    assert runtime_bindings[field_id]["receipt_hash"] == panel_receipt_payload[
        "receipt_hash"
    ]
    bindings = _verify_gated_panel_receipts(
        panel,
        [
            {
                "candidate_id": "fund-age",
                "expression": f"CSRank(${field_id})",
                "materialization_support_receipt_hashes": json.dumps(
                    {field_id: panel_receipt_payload["receipt_hash"]},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }
        ],
        capability_registry_path=REGISTRY_PATH,
        receipt_paths=[panel_receipt],
    )
    assert bindings[field_id] == json.loads(panel_receipt.read_text())["receipt_hash"]

    full_authority_root = tmp_path / "assembled_full_authority"
    assert (
        assemble_session_panel(
            Namespace(
                base_panel=base_path,
                base_manifest=base_manifest,
                coordinate_manifest=None,
                input_manifest=input_manifest,
                cache_root=cache_root,
                output_root=full_authority_root,
                source_release=tmp_path / "source_release.json",
                fundamental_root=source_root,
                fundamental_manifest=fundamental_manifest,
                split_manifest=split,
                maximum_observable_time="2024-04-23 15:00",
            )
        )
        == 0
    )
    full_manifest = json.loads(
        (full_authority_root / "session_signal_panel_manifest.json").read_text()
    )
    assert full_manifest["coordinate_projection_status"] == "NOT_REQUESTED"
    assert full_manifest["coordinate_count"] == 0
    assert "coordinate_manifest" not in full_manifest["artifacts"]
    assert not (
        full_authority_root / "signal_sketch_coordinate_manifest.csv"
    ).exists()

    mismatched_manifest = tmp_path / "mismatched_pit_sidecar_manifest.json"
    mismatched_manifest.write_text(
        json.dumps(
            {
                "source_root": str((tmp_path / "other_source").resolve()),
                "split_manifest_sha256": split_sha256,
                "development_maximum_observable_time": "2024-04-23 15:00",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="source_root differs"):
        assemble_session_panel(
            Namespace(
                base_panel=base_path,
                base_manifest=base_manifest,
                coordinate_manifest=None,
                input_manifest=input_manifest,
                cache_root=cache_root,
                output_root=tmp_path / "mismatched_assembly",
                source_release=tmp_path / "source_release.json",
                fundamental_root=source_root,
                fundamental_manifest=mismatched_manifest,
                split_manifest=split,
                maximum_observable_time="2024-04-23 15:00",
            )
        )

    sparse_sidecar_authority = tmp_path / "sparse_sidecar_authority.json"
    sparse_sidecar_authority.write_text(
        json.dumps({**sidecar_payload, "row_count": len(base) + 1}),
        encoding="utf-8",
    )
    sparse_base_manifest = tmp_path / "sparse_base_manifest.json"
    sparse_base_manifest.write_text(
        json.dumps(
            {
                **base_manifest_payload,
                "inputs": {
                    **base_manifest_payload["inputs"],
                    "session_sidecar_manifest": {
                        "path": str(sparse_sidecar_authority.resolve()),
                        "sha256": hashlib.sha256(
                            sparse_sidecar_authority.read_bytes()
                        ).hexdigest(),
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="row count differs from sidecar authority"):
        materialize_session_panel(
            Namespace(
                base_panel=base_path,
                base_manifest=sparse_base_manifest,
                split_manifest=split,
                registry=REGISTRY_PATH,
                input_manifest=input_manifest,
                fundamental_root=source_root,
                fundamental_manifest=fundamental_manifest,
                maximum_observable_time="2024-04-23 15:00",
                cache_root=tmp_path / "sparse_cache",
                partition_index=0,
                partition_count=1,
            )
        )
