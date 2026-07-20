from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.finalize_cn_phase3cm_1024_sidecars import (
    OVERALL_CLOSURE_NAME,
    PARITY_CHECKS,
    SESSION_MANIFEST_NAME,
    SESSION_V2_MANIFEST_NAME,
    STABLE_KEY,
    REUSE_CONTRACT,
    REUSE_RECEIPT_NAME,
    EvidenceError,
    _candidate_contract,
    _stable_hash,
    finalize_sidecars,
)
from scripts.preflight_cn_phase3cm_dag_cache import _sidecar_manifest_evidence


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=sorted({key for row in rows for key in row}))
        writer.writeheader()
        writer.writerows(rows)


def _candidate_rows(*, backend: str, field: str, pair_count: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pair_index in range(pair_count):
        pair_id = f"{backend}.pair.{pair_index}"
        for role in ("PRIMARY", "CONTROL"):
            rows.append(
                {
                    "pair_id": pair_id,
                    "candidate_id": f"{pair_id}.{role.lower()}",
                    "pair_member_role": role,
                    "clock_namespace": backend,
                    "expression": f"Add(${field},$close)",
                }
            )
    return rows


def test_candidate_contract_separates_implicit_close_from_expression_fields(
    tmp_path: Path,
) -> None:
    path = tmp_path / "candidates.csv"
    rows = _candidate_rows(backend="stock_session", field="fund_x", pair_count=1)
    for row in rows:
        row["expression"] = "Add($fund_x,1)"
    _write_csv(path, rows)

    contract = _candidate_contract(path, backend="stock_session", expected_pair_count=1)

    assert contract["expression_raw_fields"] == ["fund_x"]
    assert contract["required_raw_fields"] == ["close", "fund_x"]


def _parity(shard_index: int) -> dict[str, Any]:
    return {
        "schema_version": "cn_time_major_sidecar_parity_v2_train_only",
        "status": "SIDECAR_PARITY_PASS",
        "source_shard": shard_index,
        **{check: True for check in PARITY_CHECKS},
    }


def _v2_manifest(
    *,
    root: Path,
    fields: list[str],
    shards: list[dict[str, Any]],
    split_hash: str,
) -> Path:
    path = root / SESSION_V2_MANIFEST_NAME
    _write_json(
        path,
        {
            "schema_version": "cn_development_time_major_execution_layout_manifest_v2_train_only",
            "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
            "data_role": "development_train_only",
            "fields": fields,
            "source_shard_count": 16,
            "source_rows": sum(int(row["rows"]) for row in shards),
            "source_total_rows": sum(int(row["rows"]) for row in shards),
            "sidecar_rows": sum(int(row["rows"]) for row in shards),
            "sidecar_bytes": sum(int(row.get("output_bytes") or 1) for row in shards),
            "split_manifest_hash": split_hash,
            "shards": shards,
            "parity": [_parity(index) for index in range(16)],
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
    )
    return path


@pytest.fixture
def evidence(tmp_path: Path) -> dict[str, Any]:
    split = tmp_path / "split.csv"
    _write_csv(
        split,
        [
            {"trade_date": "2024-01-02", "split": "train"},
            {"trade_date": "2024-01-03", "split": "validation"},
        ],
    )
    split_hash = _sha256(split)

    active_candidates = tmp_path / "active_candidates.csv"
    session_candidates = tmp_path / "session_candidates.csv"
    _write_csv(
        active_candidates,
        _candidate_rows(backend="active_bar", field="active_field", pair_count=584),
    )
    _write_csv(
        session_candidates,
        _candidate_rows(backend="stock_session", field="fund_field", pair_count=440),
    )

    active_root = tmp_path / "active"
    active_root.mkdir()
    active_fields = [*STABLE_KEY, "close", "active_field"]
    active_shards = [
        {
            "source_shard": index,
            "source_path": str(tmp_path / f"active_source_{index:02d}.parquet"),
            "source_sha256": "a" * 64,
            "output_path": str(active_root / f"shard_{index:02d}.parquet"),
            "output_sha256": "b" * 64,
            "output_bytes": 1,
            "rows": 1,
            "fields": active_fields,
            "stable_key": list(STABLE_KEY),
            "split_manifest_hash": split_hash,
        }
        for index in range(16)
    ]
    active_manifest = _v2_manifest(
        root=active_root,
        fields=active_fields,
        shards=active_shards,
        split_hash=split_hash,
    )

    base_root = tmp_path / "session_base"
    base_root.mkdir()
    session_root = tmp_path / "session_augmented"
    session_root.mkdir()
    base_fields = [*STABLE_KEY, "close", "base_field"]
    base_shards: list[dict[str, Any]] = []
    receipt_paths: list[Path] = []
    for shard_index in range(16):
        base_path = base_root / f"shard_{shard_index:02d}.parquet"
        base_table = pa.table(
            {
                "trade_time": pa.array(["2024-01-02T15:00:00"]),
                "code": pa.array([f"{shard_index:06d}.SZ"]),
                "source_shard": pa.array([shard_index], type=pa.uint16()),
                "source_row_identity": pa.array([0], type=pa.uint64()),
                "duplicate_ordinal": pa.array([0], type=pa.uint32()),
                "close": pa.array([10.0]),
                "base_field": pa.array([1.0]),
            }
        )
        pq.write_table(base_table, base_path)
        base_shards.append(
            {
                "source_shard": shard_index,
                "source_path": str(tmp_path / f"raw_session_{shard_index:02d}.parquet"),
                "source_sha256": "c" * 64,
                "output_path": str(base_path.resolve()),
                "output_sha256": _sha256(base_path),
                "output_bytes": base_path.stat().st_size,
                "rows": 1,
                "fields": base_fields,
                "stable_key": list(STABLE_KEY),
                "split_manifest_hash": split_hash,
            }
        )

        output_path = session_root / f"shard_{shard_index:02d}.parquet"
        output_table = base_table.append_column("fund_field", pa.array([2.0]))
        pq.write_table(output_table, output_path)
        receipt_path = session_root / f"shard_{shard_index:02d}.augmentation.json"
        _write_json(
            receipt_path,
            {
                "schema_version": "cn_phase3cm_session_sidecar_augmentation_v1",
                "status": "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS",
                "shard_index": shard_index,
                "data_role": "development_train_only",
                "source": {
                    "path": str(base_path.resolve()),
                    "sha256": _sha256(base_path),
                    "rows": 1,
                },
                "output": {
                    "path": str(output_path.resolve()),
                    "sha256": _sha256(output_path),
                    "bytes": output_path.stat().st_size,
                },
                "source_stable_key_digest": f"{shard_index:064x}",
                "source_payload_digest": f"{shard_index + 16:064x}",
                "required_field_count": 2,
                "already_present_field_count": 1,
                "fundamental_fields": ["fund_field"],
                "chip_fields": [],
                "bar_context_fields": [],
                "bar_context_source": {},
                "coverage": {"fund_field": 1.0},
                "chip_sidecar": None,
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
            },
        )
        receipt_paths.append(receipt_path)
    base_manifest = _v2_manifest(
        root=base_root,
        fields=base_fields,
        shards=base_shards,
        split_hash=split_hash,
    )

    coverage = tmp_path / "coverage.json"
    _write_json(
        coverage,
        {
            "schema_version": "cn_strict_wave_sidecar_coverage_v1",
            "status": "CN_STRICT_WAVE_01024_SIDECAR_COVERAGE_AUDITED",
            "wave_id": "01024",
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "all_required_fields_present": True,
            "backends": {
                "active": {
                    "candidate_path": str(active_candidates.resolve()),
                    "candidate_members": 1168,
                    "required_raw_fields": ["active_field", "close"],
                    "required_raw_field_count": 2,
                    "field_sidecar": {
                        "root": str(active_root.resolve()),
                        "shards": 16,
                        "intersection": active_fields,
                        "union": active_fields,
                    },
                    "missing_raw_fields": [],
                    "label_sidecar": {
                        "root": str(tmp_path / "active_labels"),
                        "shards": 16,
                        "intersection": [
                            "fwd_ret_1m",
                            "fwd_ret_5m",
                            "fwd_ret_15m",
                            "fwd_ret_30m",
                        ],
                        "union": [],
                    },
                    "missing_label_fields": [],
                },
                "session": {
                    "candidate_path": str(session_candidates.resolve()),
                    "candidate_members": 880,
                    "required_raw_fields": ["close", "fund_field"],
                    "required_raw_field_count": 2,
                    "field_sidecar": {
                        "root": str(session_root.resolve()),
                        "shards": 16,
                        "intersection": [*base_fields, "fund_field"],
                        "union": [*base_fields, "fund_field"],
                    },
                    "missing_raw_fields": [],
                    "label_sidecar": {
                        "root": str(tmp_path / "session_labels"),
                        "shards": 16,
                        "intersection": [
                            "fwd_ret_1m",
                            "fwd_ret_5m",
                            "fwd_ret_15m",
                            "fwd_ret_30m",
                        ],
                        "union": [],
                    },
                    "missing_label_fields": [],
                },
            },
        },
    )

    source_index = tmp_path / "source_file_index.csv"
    _write_csv(
        source_index,
        [
            {
                "source_table": "balance_sheet_report_em",
                "relative_path": "balance_sheet_report_em/000001.parquet",
                "bytes": 100,
                "rows_footer": 1,
                "row_groups": 1,
                "schema_sha256": "d" * 64,
                "development_safe_rows": 1,
                "development_safe_content_sha256": "e" * 64,
            }
        ],
    )
    fundamental_manifest = tmp_path / "pit_sidecar_manifest.json"
    fundamental = {
        "manifest_version": "cn_pit_fundamental_sidecar_manifest_v1",
        "fabric_version": "CN_PIT_FUNDAMENTAL_FABRIC_V1",
        "source_root": str((tmp_path / "fundamental_source").resolve()),
        "physical_design": "IMMUTABLE_SYMBOL_PARTITIONED_WIDE_PARQUET_PLUS_LAZY_COLUMN_LOAD",
        "minute_panel_expansion": "FORBIDDEN_NOT_MATERIALIZED",
        "development_maximum_observable_time": "2025-12-31T15:00:00",
        "split_manifest": str(split.resolve()),
        "split_manifest_sha256": split_hash,
        "semantic_registry_hash": "f" * 64,
        "observable_contract_hash": "1" * 64,
        "tables": [
            {
                "source_table": "balance_sheet_report_em",
                "family": "fundamental_balance_sheet",
                "status": "PIT_SAFE_CURRENT_SNAPSHOT_ONLY",
                "partition_root": str(tmp_path / "fundamental_source" / "balance_sheet_report_em"),
                "file_count": 1,
                "bytes": 100,
                "rows_footer": 1,
                "schema_sha256": "2" * 64,
                "development_safe_content_sha256": "e" * 64,
            }
        ],
    }
    fundamental["manifest_hash"] = _stable_hash(fundamental)
    _write_json(fundamental_manifest, fundamental)

    return {
        "active_manifest_path": active_manifest,
        "session_base_manifest_path": base_manifest,
        "augmentation_receipt_paths": receipt_paths,
        "coverage_audit_path": coverage,
        "active_candidate_table": active_candidates,
        "session_candidate_table": session_candidates,
        "split_manifest_path": split,
        "split_manifest_sha256": split_hash,
        "fundamental_manifest_path": fundamental_manifest,
        "fundamental_source_index_path": source_index,
        "session_root": session_root,
        "output_root": tmp_path / "closure",
    }


def test_finalize_writes_capacity_compatible_manifests_and_self_hashed_closure(
    evidence: dict[str, Any],
) -> None:
    closure = finalize_sidecars(**evidence)

    session_root = Path(evidence["session_root"])
    aggregate_path = session_root / SESSION_MANIFEST_NAME
    v2_path = session_root / SESSION_V2_MANIFEST_NAME
    closure_path = Path(evidence["output_root"]) / OVERALL_CLOSURE_NAME
    assert aggregate_path.is_file()
    assert v2_path.is_file()
    assert closure_path.is_file()

    aggregate = json.loads(aggregate_path.read_text(encoding="utf-8"))
    assert aggregate["status"] == "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS"
    assert aggregate["shard_count"] == 16
    assert aggregate["row_count"] == 16
    assert len(aggregate["shards"]) == 16

    capacity = _sidecar_manifest_evidence(session_root, kind="field")
    assert capacity["manifest_status"] == "TIME_MAJOR_LAYOUT_PARITY_PASS"
    assert capacity["shard_count"] == 16
    assert capacity["row_count"] == 16

    stored = json.loads(closure_path.read_text(encoding="utf-8"))
    claimed_hash = stored.pop("closure_hash")
    assert claimed_hash == _stable_hash(stored)
    assert closure["pair_counts"] == {
        "active_bar": 584,
        "stock_session": 440,
        "total": 1024,
    }
    assert closure["formal_evaluator_authority"] == "UNCHANGED"
    assert closure["streaming_backend_authority"] == "EXPERIMENTAL_BACKEND"
    assert closure["sealed_reads"] == {
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def _convert_session_base_to_hash_exact_reuse(evidence: dict[str, Any]) -> Path:
    path = Path(evidence["session_base_manifest_path"])
    manifest = json.loads(path.read_text(encoding="utf-8"))
    source_base = path.parent / "source_base_manifest.json"
    source_augmentation = path.parent / "source_augmentation_manifest.json"
    _write_json(source_base, {"status": "TIME_MAJOR_LAYOUT_PARITY_PASS"})
    _write_json(
        source_augmentation,
        {"status": "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS"},
    )
    manifest["reuse_contract"] = REUSE_CONTRACT
    manifest["source_base_manifest"] = str(source_base.resolve())
    manifest["source_base_manifest_sha256"] = _sha256(source_base)
    manifest["source_augmentation_manifest"] = str(source_augmentation.resolve())
    manifest["source_augmentation_manifest_sha256"] = _sha256(source_augmentation)
    manifest["parity"] = [
        {
            "source_shard": int(row["source_shard"]),
            "status": "SIDECAR_REUSE_EXACT_HASH_PASS",
            "source_sha256": str(row["output_sha256"]),
            "output_sha256": str(row["output_sha256"]),
            "rows": int(row["rows"]),
        }
        for row in manifest["shards"]
    ]
    _write_json(path, manifest)
    receipt = {
        "schema_version": "cn_phase3cm_session_reuse_source_receipt_v1",
        "status": "CN_PHASE3CM_SESSION_REUSE_SOURCE_READY",
        "output_root": str(path.parent.resolve()),
        "layout_manifest": str(path.resolve()),
        "layout_manifest_sha256": _sha256(path),
        "shard_count": 16,
        "row_count": sum(int(row["rows"]) for row in manifest["shards"]),
        "field_count": len(manifest["fields"]),
        "reuse_contract": REUSE_CONTRACT,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    receipt["receipt_hash"] = _stable_hash(receipt)
    receipt_path = path.parent / REUSE_RECEIPT_NAME
    _write_json(receipt_path, receipt)
    return receipt_path


def test_hash_exact_reuse_manifest_is_accepted_without_faking_standard_parity(
    evidence: dict[str, Any],
) -> None:
    _convert_session_base_to_hash_exact_reuse(evidence)

    closure = finalize_sidecars(**evidence)

    assert closure["status"] == "CN_PHASE3CM_1024_SIDECAR_CLOSURE_PASS"
    assert closure["session_sidecar"]["shard_count"] == 16


def test_hash_exact_reuse_receipt_tamper_fails_closed(evidence: dict[str, Any]) -> None:
    receipt_path = _convert_session_base_to_hash_exact_reuse(evidence)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["row_count"] += 1
    _write_json(receipt_path, receipt)

    with pytest.raises(EvidenceError, match="reuse receipt self-hash drift"):
        finalize_sidecars(**evidence)


def test_hash_exact_reuse_parity_hash_drift_fails_closed(evidence: dict[str, Any]) -> None:
    _convert_session_base_to_hash_exact_reuse(evidence)
    path = Path(evidence["session_base_manifest_path"])
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["parity"][0]["output_sha256"] = "0" * 64
    _write_json(path, manifest)
    receipt_path = path.parent / REUSE_RECEIPT_NAME
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["layout_manifest_sha256"] = _sha256(path)
    receipt["receipt_hash"] = _stable_hash(
        {key: value for key, value in receipt.items() if key != "receipt_hash"}
    )
    _write_json(receipt_path, receipt)

    with pytest.raises(EvidenceError, match="reuse parity shard 0 hash drift"):
        finalize_sidecars(**evidence)


def test_output_hash_drift_fails_before_authority_publish(evidence: dict[str, Any]) -> None:
    receipt_path = Path(evidence["augmentation_receipt_paths"][0])
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["output"]["sha256"] = "0" * 64
    _write_json(receipt_path, receipt)

    with pytest.raises(EvidenceError, match="output hash drift"):
        finalize_sidecars(**evidence)
    assert not (Path(evidence["session_root"]) / SESSION_MANIFEST_NAME).exists()
    assert not (Path(evidence["output_root"]) / OVERALL_CLOSURE_NAME).exists()


def test_source_hash_drift_fails_closed(evidence: dict[str, Any]) -> None:
    receipt_path = Path(evidence["augmentation_receipt_paths"][0])
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["source"]["sha256"] = "0" * 64
    _write_json(receipt_path, receipt)

    with pytest.raises(EvidenceError, match="source hash drift"):
        finalize_sidecars(**evidence)


@pytest.mark.parametrize("field", ["source_stable_key_digest", "source_payload_digest"])
def test_missing_stable_payload_evidence_fails_closed(
    evidence: dict[str, Any], field: str
) -> None:
    receipt_path = Path(evidence["augmentation_receipt_paths"][0])
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt[field] = ""
    _write_json(receipt_path, receipt)

    with pytest.raises(EvidenceError, match=field):
        finalize_sidecars(**evidence)


def test_missing_required_session_schema_fails_closed(evidence: dict[str, Any]) -> None:
    output_path = Path(evidence["session_root"]) / "shard_00.parquet"
    table = pq.read_table(output_path).drop(["fund_field"])
    pq.write_table(table, output_path)
    receipt_path = Path(evidence["augmentation_receipt_paths"][0])
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["output"]["sha256"] = _sha256(output_path)
    receipt["output"]["bytes"] = output_path.stat().st_size
    _write_json(receipt_path, receipt)

    with pytest.raises(EvidenceError, match="required fields"):
        finalize_sidecars(**evidence)


def test_coverage_false_fails_closed(evidence: dict[str, Any]) -> None:
    path = Path(evidence["coverage_audit_path"])
    coverage = json.loads(path.read_text(encoding="utf-8"))
    coverage["all_required_fields_present"] = False
    _write_json(path, coverage)

    with pytest.raises(EvidenceError, match="all-required-fields-present"):
        finalize_sidecars(**evidence)


def test_active_parity_failure_fails_closed(evidence: dict[str, Any]) -> None:
    path = Path(evidence["active_manifest_path"])
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["parity"][3]["field_value_digest_match"] = False
    _write_json(path, manifest)

    with pytest.raises(EvidenceError, match="field_value_digest_match"):
        finalize_sidecars(**evidence)


def test_forbidden_receipt_read_fails_closed(evidence: dict[str, Any]) -> None:
    path = Path(evidence["augmentation_receipt_paths"][4])
    receipt = json.loads(path.read_text(encoding="utf-8"))
    receipt["holdout_reads"] = 1
    _write_json(path, receipt)

    with pytest.raises(EvidenceError, match="holdout_reads"):
        finalize_sidecars(**evidence)


def test_fundamental_source_index_drift_fails_closed(evidence: dict[str, Any]) -> None:
    path = Path(evidence["fundamental_source_index_path"])
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows[0]["rows_footer"] = "2"
    _write_csv(path, rows)

    with pytest.raises(EvidenceError, match="row-count drift"):
        finalize_sidecars(**evidence)
