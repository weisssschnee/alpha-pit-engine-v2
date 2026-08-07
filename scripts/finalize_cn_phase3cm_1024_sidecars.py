"""Finalize the development-only sidecar closure for the 1,024-pair wave.

The finalizer does not materialize fields and does not launch evaluation.  It
binds the already-built active V2 authority, aggregates the sixteen session
augmentation receipts, emits a capacity-preflight-compatible session V2
manifest, and writes one self-hashed overall closure.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pyarrow.parquet as pq


SHARD_COUNT = 16
ACTIVE_PAIR_COUNT = 584
SESSION_PAIR_COUNT = 440
FIELD_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
HEX_64 = re.compile(r"[0-9a-f]{64}")
STABLE_KEY = (
    "trade_time",
    "code",
    "source_shard",
    "source_row_identity",
    "duplicate_ordinal",
)
ACCESS_FIELDS = ("validation_reads", "holdout_reads", "forward_2026_reads")
ACTIVE_CLOCK = "active_bar"
SESSION_CLOCK = "stock_session"
V2_SCHEMA_VERSION = "cn_development_time_major_execution_layout_manifest_v2_train_only"
V2_STATUS = "TIME_MAJOR_LAYOUT_PARITY_PASS"
SESSION_MANIFEST_NAME = "CN_SESSION_SIDECAR_AUGMENTATION_MANIFEST.json"
SESSION_V2_MANIFEST_NAME = "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
OVERALL_CLOSURE_NAME = "CN_PHASE3CM_1024_SIDECAR_CLOSURE.json"
REUSE_RECEIPT_NAME = "CN_SESSION_REUSE_SOURCE_RECEIPT.json"
REUSE_CONTRACT = "HASH_EXACT_IMMUTABLE_AUGMENTED_256_SOURCE"


class EvidenceError(ValueError):
    """Raised when an input cannot support the frozen sidecar closure."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"{label} is not readable JSON: {path}") from exc
    if not isinstance(value, dict):
        raise EvidenceError(f"{label} must be a JSON object")
    return value


def _read_csv(path: Path, label: str) -> list[dict[str, str]]:
    try:
        with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
            rows = [dict(row) for row in csv.DictReader(handle)]
    except OSError as exc:
        raise EvidenceError(f"{label} is not readable CSV: {path}") from exc
    if not rows:
        raise EvidenceError(f"{label} is empty")
    return rows


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(
                dict(value),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EvidenceError(f"{label} must be an object")
    return value


def _array(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise EvidenceError(f"{label} must be an array")
    return value


def _integer(value: Any, label: str, *, minimum: int | None = None) -> int:
    if type(value) is not int:
        raise EvidenceError(f"{label} must be an integer")
    result = int(value)
    if minimum is not None and result < minimum:
        raise EvidenceError(f"{label} must be >= {minimum}")
    return result


def _hash(value: Any, label: str) -> str:
    text = str(value or "").lower()
    if HEX_64.fullmatch(text) is None:
        raise EvidenceError(f"{label} must be an exact lowercase SHA-256")
    return text


def _zero_access(payload: Mapping[str, Any], label: str) -> dict[str, int]:
    observed: dict[str, int] = {}
    for field in ACCESS_FIELDS:
        value = payload.get(field)
        if type(value) is not int or int(value) != 0:
            raise EvidenceError(f"{label}.{field} must be the integer zero")
        observed[field] = 0
    return observed


def _resolve_artifact(raw: Any, *, anchor: Path, label: str) -> Path:
    text = str(raw or "").strip()
    if not text:
        raise EvidenceError(f"{label} path is empty")
    path = Path(text)
    if not path.is_absolute():
        path = Path(anchor) / path
    return path.resolve()


def _expression_fields(rows: Iterable[Mapping[str, Any]]) -> list[str]:
    return sorted(
        {
            field
            for row in rows
            for field in FIELD_PATTERN.findall(str(row.get("expression") or ""))
        }
    )


def _required_fields(rows: Iterable[Mapping[str, Any]]) -> list[str]:
    return sorted(set(_expression_fields(rows)) | {"close"})


def _candidate_contract(
    path: Path,
    *,
    backend: str,
    expected_pair_count: int,
) -> dict[str, Any]:
    rows = _read_csv(path, f"{backend} candidate table")
    by_pair: dict[str, list[Mapping[str, Any]]] = {}
    candidate_ids: set[str] = set()
    for ordinal, row in enumerate(rows, start=1):
        pair_id = str(row.get("pair_id") or "").strip()
        candidate_id = str(row.get("candidate_id") or "").strip()
        expression = str(row.get("expression") or "").strip()
        if not pair_id or not candidate_id or not expression:
            raise EvidenceError(f"{backend} candidate row {ordinal} is incomplete")
        if candidate_id in candidate_ids:
            raise EvidenceError(f"{backend} candidate identity is duplicated: {candidate_id}")
        candidate_ids.add(candidate_id)
        observed_clock = str(row.get("clock_namespace") or backend)
        if observed_clock != backend:
            raise EvidenceError(f"{backend} candidate clock drift: {candidate_id}")
        by_pair.setdefault(pair_id, []).append(row)
    if len(by_pair) != int(expected_pair_count):
        raise EvidenceError(
            f"{backend} candidate table must contain exactly {expected_pair_count} pairs; "
            f"observed {len(by_pair)}"
        )
    for pair_id, members in by_pair.items():
        roles = sorted(str(row.get("pair_member_role") or "") for row in members)
        if len(members) != 2 or roles != ["CONTROL", "PRIMARY"]:
            raise EvidenceError(
                f"{backend} pair must contain one PRIMARY and one CONTROL: {pair_id}"
            )
    expression_fields = _expression_fields(rows)
    return {
        "path": str(Path(path).resolve()),
        "sha256": _sha256(path),
        "pair_count": len(by_pair),
        "candidate_member_count": len(rows),
        "required_raw_fields": _required_fields(rows),
        "expression_raw_fields": expression_fields,
    }


PARITY_CHECKS = (
    "row_count_match",
    "stable_key_unique",
    "duplicate_identity_match",
    "missing_identity_match",
    "coordinate_digest_match",
    "null_bitmap_match",
    "dtype_match",
    "observable_time_match",
    "field_value_digest_match",
)


def _v2_manifest(
    path: Path,
    *,
    label: str,
    split_sha256: str,
    required_fields: Sequence[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = _read_json(path, label)
    if payload.get("schema_version") != V2_SCHEMA_VERSION:
        raise EvidenceError(f"{label} schema version drift")
    if payload.get("status") != V2_STATUS:
        raise EvidenceError(f"{label} is not parity PASS")
    if payload.get("data_role") != "development_train_only":
        raise EvidenceError(f"{label} is not development/train-only")
    _zero_access(payload, label)
    if str(payload.get("split_manifest_hash") or "") != split_sha256:
        raise EvidenceError(f"{label} split-manifest hash drift")
    fields = {str(value) for value in _array(payload.get("fields"), f"{label}.fields")}
    missing_fields = sorted(set(required_fields) - fields)
    if missing_fields:
        raise EvidenceError(f"{label} misses candidate fields: {missing_fields}")
    records = [dict(_object(row, f"{label}.shards")) for row in _array(payload.get("shards"), f"{label}.shards")]
    if len(records) != SHARD_COUNT or _integer(
        payload.get("source_shard_count"), f"{label}.source_shard_count"
    ) != SHARD_COUNT:
        raise EvidenceError(f"{label} must bind exactly {SHARD_COUNT} shards")
    by_index: dict[int, dict[str, Any]] = {}
    for record in records:
        shard_index = _integer(record.get("source_shard"), f"{label}.source_shard")
        if shard_index in by_index:
            raise EvidenceError(f"{label} duplicates shard {shard_index}")
        _integer(record.get("rows"), f"{label}.shard[{shard_index}].rows", minimum=1)
        _hash(record.get("output_sha256"), f"{label}.shard[{shard_index}].output_sha256")
        _hash(record.get("source_sha256"), f"{label}.shard[{shard_index}].source_sha256")
        if str(record.get("split_manifest_hash") or "") != split_sha256:
            raise EvidenceError(f"{label} shard {shard_index} split hash drift")
        stable_key = tuple(str(value) for value in record.get("stable_key") or ())
        if stable_key != STABLE_KEY:
            raise EvidenceError(f"{label} shard {shard_index} stable-key contract drift")
        by_index[shard_index] = record
    if set(by_index) != set(range(SHARD_COUNT)):
        raise EvidenceError(f"{label} shard indices are incomplete")

    reuse_mode = payload.get("reuse_contract") == REUSE_CONTRACT
    if reuse_mode:
        receipt_path = Path(path).resolve().parent / REUSE_RECEIPT_NAME
        receipt = _read_json(receipt_path, f"{label} reuse receipt")
        if receipt.get("schema_version") != "cn_phase3cm_session_reuse_source_receipt_v1":
            raise EvidenceError(f"{label} reuse receipt schema version drift")
        if receipt.get("status") != "CN_PHASE3CM_SESSION_REUSE_SOURCE_READY":
            raise EvidenceError(f"{label} reuse receipt is not READY")
        _zero_access(receipt, f"{label} reuse receipt")
        if receipt.get("reuse_contract") != REUSE_CONTRACT:
            raise EvidenceError(f"{label} reuse receipt contract drift")
        claimed_receipt_hash = _hash(
            receipt.get("receipt_hash"), f"{label} reuse receipt.receipt_hash"
        )
        receipt_body = dict(receipt)
        receipt_body.pop("receipt_hash", None)
        if _stable_hash(receipt_body) != claimed_receipt_hash:
            raise EvidenceError(f"{label} reuse receipt self-hash drift")
        receipt_root = _resolve_artifact(
            receipt.get("output_root"),
            anchor=receipt_path.parent,
            label=f"{label} reuse receipt.output_root",
        )
        if receipt_root != Path(path).resolve().parent:
            raise EvidenceError(f"{label} reuse receipt output-root drift")
        layout_path = _resolve_artifact(
            receipt.get("layout_manifest"),
            anchor=receipt_path.parent,
            label=f"{label} reuse receipt.layout_manifest",
        )
        if layout_path != Path(path).resolve():
            raise EvidenceError(f"{label} reuse receipt layout-manifest path drift")
        if _hash(
            receipt.get("layout_manifest_sha256"),
            f"{label} reuse receipt.layout_manifest_sha256",
        ) != _sha256(path):
            raise EvidenceError(f"{label} reuse receipt layout-manifest hash drift")
        if _integer(
            receipt.get("shard_count"), f"{label} reuse receipt.shard_count"
        ) != SHARD_COUNT:
            raise EvidenceError(f"{label} reuse receipt shard-count drift")
        expected_rows = sum(
            _integer(row.get("rows"), f"{label}.shard.rows", minimum=1)
            for row in by_index.values()
        )
        if _integer(
            receipt.get("row_count"), f"{label} reuse receipt.row_count", minimum=1
        ) != expected_rows:
            raise EvidenceError(f"{label} reuse receipt row-count drift")
        if _integer(
            receipt.get("field_count"), f"{label} reuse receipt.field_count", minimum=1
        ) != len(fields):
            raise EvidenceError(f"{label} reuse receipt field-count drift")
        for source_label in ("source_base_manifest", "source_augmentation_manifest"):
            source_path = _resolve_artifact(
                payload.get(source_label),
                anchor=Path(path).resolve().parent,
                label=f"{label}.{source_label}",
            )
            if not source_path.is_file():
                raise EvidenceError(f"{label} {source_label} is missing")
            claimed_source_hash = _hash(
                payload.get(f"{source_label}_sha256"),
                f"{label}.{source_label}_sha256",
            )
            if _sha256(source_path) != claimed_source_hash:
                raise EvidenceError(f"{label} {source_label} hash drift")

    parity = [dict(_object(row, f"{label}.parity")) for row in _array(payload.get("parity"), f"{label}.parity")]
    if len(parity) != SHARD_COUNT:
        raise EvidenceError(f"{label} must contain exactly {SHARD_COUNT} parity receipts")
    parity_indices: set[int] = set()
    for ordinal, receipt in enumerate(parity):
        shard_index = int(receipt.get("source_shard", ordinal))
        if shard_index in parity_indices or shard_index not in by_index:
            raise EvidenceError(f"{label} parity shard identity drift")
        parity_indices.add(shard_index)
        if reuse_mode:
            if receipt.get("status") != "SIDECAR_REUSE_EXACT_HASH_PASS":
                raise EvidenceError(f"{label} reuse parity receipt {ordinal} is not PASS")
            shard = by_index[shard_index]
            source_hash = _hash(
                receipt.get("source_sha256"),
                f"{label} reuse parity shard {shard_index}.source_sha256",
            )
            output_hash = _hash(
                receipt.get("output_sha256"),
                f"{label} reuse parity shard {shard_index}.output_sha256",
            )
            shard_hash = _hash(
                shard.get("output_sha256"),
                f"{label} shard {shard_index}.output_sha256",
            )
            if len({source_hash, output_hash, shard_hash}) != 1:
                raise EvidenceError(f"{label} reuse parity shard {shard_index} hash drift")
            if _integer(
                receipt.get("rows"),
                f"{label} reuse parity shard {shard_index}.rows",
                minimum=1,
            ) != _integer(
                shard.get("rows"), f"{label} shard {shard_index}.rows", minimum=1
            ):
                raise EvidenceError(f"{label} reuse parity shard {shard_index} row drift")
        else:
            if receipt.get("status") != "SIDECAR_PARITY_PASS":
                raise EvidenceError(f"{label} parity receipt {ordinal} is not PASS")
            for check in PARITY_CHECKS:
                if receipt.get(check) is not True:
                    raise EvidenceError(f"{label} parity shard {shard_index} failed {check}")
    if parity_indices != set(range(SHARD_COUNT)):
        raise EvidenceError(f"{label} parity coverage is incomplete")
    return payload, [by_index[index] for index in range(SHARD_COUNT)]


def _coverage_backend(
    payload: Mapping[str, Any],
    *,
    label: str,
    candidate: Mapping[str, Any],
    expected_field_root: Path,
) -> dict[str, Any]:
    if _resolve_artifact(
        payload.get("candidate_path"),
        anchor=Path(candidate["path"]).parent,
        label=f"coverage.{label}.candidate_path",
    ) != Path(candidate["path"]):
        raise EvidenceError(f"coverage {label} candidate path drift")
    if _integer(payload.get("candidate_members"), f"coverage.{label}.candidate_members") != int(
        candidate["candidate_member_count"]
    ):
        raise EvidenceError(f"coverage {label} candidate-member count drift")
    required = sorted(str(value) for value in payload.get("required_raw_fields") or ())
    if required != list(candidate["required_raw_fields"]):
        raise EvidenceError(f"coverage {label} required-field set drift")
    if _integer(payload.get("required_raw_field_count"), f"coverage.{label}.required_raw_field_count") != len(required):
        raise EvidenceError(f"coverage {label} required-field count drift")
    if list(payload.get("missing_raw_fields") or ()) or list(payload.get("missing_label_fields") or ()):
        raise EvidenceError(f"coverage {label} still records missing fields")
    field = _object(payload.get("field_sidecar"), f"coverage.{label}.field_sidecar")
    labels = _object(payload.get("label_sidecar"), f"coverage.{label}.label_sidecar")
    if _integer(field.get("shards"), f"coverage.{label}.field_sidecar.shards") != SHARD_COUNT:
        raise EvidenceError(f"coverage {label} field shard count drift")
    if _integer(labels.get("shards"), f"coverage.{label}.label_sidecar.shards") != SHARD_COUNT:
        raise EvidenceError(f"coverage {label} label shard count drift")
    if not set(required) <= {str(value) for value in field.get("intersection") or ()}:
        raise EvidenceError(f"coverage {label} field schema is incomplete")
    expected_labels = {"fwd_ret_1m", "fwd_ret_5m", "fwd_ret_15m", "fwd_ret_30m"}
    if not expected_labels <= {str(value) for value in labels.get("intersection") or ()}:
        raise EvidenceError(f"coverage {label} label schema is incomplete")
    if _resolve_artifact(
        field.get("root"),
        anchor=expected_field_root,
        label=f"coverage.{label}.field_sidecar.root",
    ) != expected_field_root.resolve():
        raise EvidenceError(f"coverage {label} field-root drift")
    return {
        "candidate_members": int(candidate["candidate_member_count"]),
        "required_raw_fields": required,
        "field_root": str(expected_field_root.resolve()),
        "label_root": str(labels.get("root") or ""),
        "field_shards": SHARD_COUNT,
        "label_shards": SHARD_COUNT,
    }


def _coverage_audit(
    path: Path,
    *,
    active_candidate: Mapping[str, Any],
    session_candidate: Mapping[str, Any],
    active_root: Path,
    session_root: Path,
) -> dict[str, Any]:
    payload = _read_json(path, "coverage audit")
    if payload.get("all_required_fields_present") is not True:
        raise EvidenceError("coverage audit is not all-required-fields-present PASS")
    _zero_access(payload, "coverage audit")
    backends = _object(payload.get("backends"), "coverage audit.backends")
    active = _coverage_backend(
        _object(backends.get("active"), "coverage audit.backends.active"),
        label="active",
        candidate=active_candidate,
        expected_field_root=active_root,
    )
    session = _coverage_backend(
        _object(backends.get("session"), "coverage audit.backends.session"),
        label="session",
        candidate=session_candidate,
        expected_field_root=session_root,
    )
    return {
        "path": str(Path(path).resolve()),
        "sha256": _sha256(path),
        "status": str(payload.get("status") or ""),
        "all_required_fields_present": True,
        "backends": {"active": active, "session": session},
        "sealed_reads": {field: 0 for field in ACCESS_FIELDS},
    }


def _fundamental_authority(
    manifest_path: Path,
    source_index_path: Path,
    *,
    split_sha256: str,
) -> dict[str, Any]:
    manifest = _read_json(manifest_path, "fundamental manifest")
    claimed_manifest_hash = _hash(manifest.get("manifest_hash"), "fundamental manifest.manifest_hash")
    body = dict(manifest)
    body.pop("manifest_hash", None)
    if _stable_hash(body) != claimed_manifest_hash:
        raise EvidenceError("fundamental manifest self-hash drift")
    if manifest.get("manifest_version") != "cn_pit_fundamental_sidecar_manifest_v1":
        raise EvidenceError("fundamental manifest version drift")
    if str(manifest.get("split_manifest_sha256") or "") != split_sha256:
        raise EvidenceError("fundamental manifest split hash drift")
    if manifest.get("minute_panel_expansion") != "FORBIDDEN_NOT_MATERIALIZED":
        raise EvidenceError("fundamental manifest minute-expansion boundary drift")
    source_root = Path(str(manifest.get("source_root") or ""))
    if not source_root.is_absolute():
        raise EvidenceError("fundamental manifest source root must be absolute")

    rows = _read_csv(source_index_path, "fundamental source index")
    required_columns = {
        "source_table",
        "relative_path",
        "bytes",
        "rows_footer",
        "schema_sha256",
        "development_safe_content_sha256",
    }
    if not required_columns <= set(rows[0]):
        raise EvidenceError("fundamental source index schema is incomplete")
    relative_paths: set[str] = set()
    by_table: dict[str, list[Mapping[str, str]]] = {}
    for ordinal, row in enumerate(rows, start=1):
        table = str(row.get("source_table") or "").strip()
        relative = str(row.get("relative_path") or "").strip()
        if not table or not relative or relative in relative_paths:
            raise EvidenceError(f"fundamental source index identity drift at row {ordinal}")
        relative_paths.add(relative)
        try:
            byte_count = int(str(row.get("bytes") or ""))
            row_count = int(str(row.get("rows_footer") or ""))
        except ValueError as exc:
            raise EvidenceError(f"fundamental source index count drift at row {ordinal}") from exc
        if byte_count <= 0 or row_count < 0:
            raise EvidenceError(f"fundamental source index count drift at row {ordinal}")
        _hash(row.get("schema_sha256"), f"fundamental source index row {ordinal} schema")
        safe_hash = str(row.get("development_safe_content_sha256") or "")
        if safe_hash not in {"NOT_SCANNED", "NOT_SCANNED_PIT_UNRESOLVED"}:
            _hash(safe_hash, f"fundamental source index row {ordinal} development-safe content")
        by_table.setdefault(table, []).append(row)

    tables = [dict(_object(row, "fundamental manifest.tables")) for row in _array(manifest.get("tables"), "fundamental manifest.tables")]
    manifest_tables = {str(row.get("source_table") or ""): row for row in tables}
    if "" in manifest_tables or len(manifest_tables) != len(tables) or set(manifest_tables) != set(by_table):
        raise EvidenceError("fundamental manifest/source-index table membership drift")
    for table, table_rows in by_table.items():
        authority = manifest_tables[table]
        if _integer(authority.get("file_count"), f"fundamental table {table}.file_count") != len(table_rows):
            raise EvidenceError(f"fundamental table {table} file-count drift")
        if _integer(authority.get("bytes"), f"fundamental table {table}.bytes") != sum(
            int(str(row["bytes"])) for row in table_rows
        ):
            raise EvidenceError(f"fundamental table {table} byte-count drift")
        if _integer(authority.get("rows_footer"), f"fundamental table {table}.rows_footer") != sum(
            int(str(row["rows_footer"])) for row in table_rows
        ):
            raise EvidenceError(f"fundamental table {table} row-count drift")
        _hash(authority.get("schema_sha256"), f"fundamental table {table}.schema_sha256")

    return {
        "manifest": {
            "path": str(Path(manifest_path).resolve()),
            "sha256": _sha256(manifest_path),
            "manifest_hash": claimed_manifest_hash,
        },
        "source_index": {
            "path": str(Path(source_index_path).resolve()),
            "sha256": _sha256(source_index_path),
            "row_count": len(rows),
            "table_count": len(by_table),
        },
        "source_root": str(source_root),
        "split_manifest_sha256": split_sha256,
    }


def _session_augmentation(
    *,
    base_manifest_path: Path,
    base_manifest: Mapping[str, Any],
    base_shards: Sequence[Mapping[str, Any]],
    receipt_paths: Sequence[Path],
    session_root: Path,
    split_sha256: str,
    train_date_count: int,
    required_fields: Sequence[str],
    fundamental: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(receipt_paths) != SHARD_COUNT:
        raise EvidenceError(f"exactly {SHARD_COUNT} session augmentation receipts are required")
    receipt_by_index: dict[int, tuple[Path, dict[str, Any]]] = {}
    for path in receipt_paths:
        receipt = _read_json(path, "session augmentation receipt")
        shard_index = _integer(receipt.get("shard_index"), "session augmentation receipt.shard_index")
        if shard_index in receipt_by_index:
            raise EvidenceError(f"duplicate session augmentation receipt for shard {shard_index}")
        receipt_by_index[shard_index] = (Path(path).resolve(), receipt)
    if set(receipt_by_index) != set(range(SHARD_COUNT)):
        raise EvidenceError("session augmentation receipt shard coverage is incomplete")

    session_root = Path(session_root).resolve()
    session_root.mkdir(parents=True, exist_ok=True)
    aggregate_records: list[dict[str, Any]] = []
    v2_records: list[dict[str, Any]] = []
    parity_records: list[dict[str, Any]] = []
    schema_intersection: set[str] | None = None
    schema_union: set[str] = set()
    declared_fields = {
        str(value) for value in _array(base_manifest.get("fields"), "session base V2 manifest.fields")
    } | set(required_fields)
    total_rows = 0
    total_bytes = 0

    for shard_index in range(SHARD_COUNT):
        receipt_path, receipt = receipt_by_index[shard_index]
        if receipt.get("status") != "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS":
            raise EvidenceError(f"session augmentation shard {shard_index} is not parity PASS")
        if receipt.get("data_role") != "development_train_only":
            raise EvidenceError(f"session augmentation shard {shard_index} is not development/train-only")
        _zero_access(receipt, f"session augmentation shard {shard_index}")
        source = _object(receipt.get("source"), f"session augmentation shard {shard_index}.source")
        output = _object(receipt.get("output"), f"session augmentation shard {shard_index}.output")
        base = dict(base_shards[shard_index])
        base_source = _resolve_artifact(
            base.get("output_path"),
            anchor=Path(base_manifest_path).parent,
            label=f"session base shard {shard_index}.output_path",
        )
        receipt_source = _resolve_artifact(
            source.get("path"),
            anchor=receipt_path.parent,
            label=f"session augmentation shard {shard_index}.source.path",
        )
        if receipt_source != base_source:
            raise EvidenceError(f"session augmentation shard {shard_index} source path drift")
        if not receipt_source.is_file():
            raise EvidenceError(f"session augmentation shard {shard_index} source is missing")
        base_source_sha = _hash(
            base.get("output_sha256"), f"session base shard {shard_index}.output_sha256"
        )
        receipt_source_sha = _hash(
            source.get("sha256"), f"session augmentation shard {shard_index}.source.sha256"
        )
        observed_source_sha = _sha256(receipt_source)
        if len({base_source_sha, receipt_source_sha, observed_source_sha}) != 1:
            raise EvidenceError(f"session augmentation shard {shard_index} source hash drift")
        source_rows = _integer(
            source.get("rows"), f"session augmentation shard {shard_index}.source.rows", minimum=1
        )
        if source_rows != _integer(base.get("rows"), f"session base shard {shard_index}.rows", minimum=1):
            raise EvidenceError(f"session augmentation shard {shard_index} source row drift")
        source_parquet = pq.ParquetFile(receipt_source)
        if source_parquet.metadata.num_rows != source_rows:
            raise EvidenceError(f"session augmentation shard {shard_index} source footer row drift")
        stable_digest = _hash(
            receipt.get("source_stable_key_digest"),
            f"session augmentation shard {shard_index}.source_stable_key_digest",
        )
        payload_digest = _hash(
            receipt.get("source_payload_digest"),
            f"session augmentation shard {shard_index}.source_payload_digest",
        )

        output_path = _resolve_artifact(
            output.get("path"),
            anchor=session_root,
            label=f"session augmentation shard {shard_index}.output.path",
        )
        expected_output = session_root / f"shard_{shard_index:02d}.parquet"
        if output_path != expected_output or not output_path.is_file():
            raise EvidenceError(f"session augmentation shard {shard_index} output path drift")
        output_sha = _hash(
            output.get("sha256"), f"session augmentation shard {shard_index}.output.sha256"
        )
        if _sha256(output_path) != output_sha:
            raise EvidenceError(f"session augmentation shard {shard_index} output hash drift")
        output_bytes = _integer(
            output.get("bytes"), f"session augmentation shard {shard_index}.output.bytes", minimum=1
        )
        if output_path.stat().st_size != output_bytes:
            raise EvidenceError(f"session augmentation shard {shard_index} output byte-count drift")
        parquet = pq.ParquetFile(output_path)
        if parquet.metadata.num_rows != source_rows:
            raise EvidenceError(f"session augmentation shard {shard_index} output row drift")
        schema = set(parquet.schema_arrow.names)
        source_schema = set(source_parquet.schema_arrow.names)
        if not set(STABLE_KEY) <= schema or not source_schema <= schema:
            raise EvidenceError(f"session augmentation shard {shard_index} did not preserve base schema")
        missing_required = sorted(set(required_fields) - schema)
        if missing_required:
            raise EvidenceError(
                f"session augmentation shard {shard_index} misses required fields: {missing_required}"
            )
        required_count = _integer(
            receipt.get("required_field_count"),
            f"session augmentation shard {shard_index}.required_field_count",
        )
        if required_count != len(required_fields):
            raise EvidenceError(f"session augmentation shard {shard_index} required-field count drift")
        expected_present = len(set(required_fields) & source_schema)
        if _integer(
            receipt.get("already_present_field_count"),
            f"session augmentation shard {shard_index}.already_present_field_count",
        ) != expected_present:
            raise EvidenceError(f"session augmentation shard {shard_index} present-field count drift")
        added_groups = {
            "fundamental": {str(value) for value in receipt.get("fundamental_fields") or ()},
            "chip": {str(value) for value in receipt.get("chip_fields") or ()},
            "bar_context": {str(value) for value in receipt.get("bar_context_fields") or ()},
            "market_context": {
                str(value) for value in receipt.get("market_context_fields") or ()
            },
            "session_close_stock": {
                str(value) for value in receipt.get("session_close_stock_fields") or ()
            },
            "session_close_market": {
                str(value) for value in receipt.get("session_close_market_fields") or ()
            },
        }
        added_flat = set().union(*added_groups.values())
        if sum(len(values) for values in added_groups.values()) != len(added_flat):
            raise EvidenceError(f"session augmentation shard {shard_index} added-field groups overlap")
        if added_flat != set(required_fields) - source_schema:
            raise EvidenceError(f"session augmentation shard {shard_index} added-field set drift")
        coverage = _object(receipt.get("coverage"), f"session augmentation shard {shard_index}.coverage")
        if set(coverage) != added_flat:
            raise EvidenceError(f"session augmentation shard {shard_index} coverage field set drift")
        for field, raw in coverage.items():
            try:
                value = float(raw)
            except (TypeError, ValueError) as exc:
                raise EvidenceError(
                    f"session augmentation shard {shard_index} coverage is invalid: {field}"
                ) from exc
            if not 0.0 <= value <= 1.0:
                raise EvidenceError(
                    f"session augmentation shard {shard_index} coverage is out of bounds: {field}"
                )

        schema_intersection = schema if schema_intersection is None else schema_intersection & schema
        schema_union |= schema
        total_rows += source_rows
        total_bytes += output_bytes
        receipt_record = {
            **receipt,
            "receipt": {"path": str(receipt_path), "sha256": _sha256(receipt_path)},
        }
        aggregate_records.append(receipt_record)
        v2_records.append(
            {
                "schema_version": V2_SCHEMA_VERSION,
                "sidecar_identity": _stable_hash(
                    {
                        "source_sha256": receipt_source_sha,
                        "output_sha256": output_sha,
                        "source_shard": shard_index,
                        "split_manifest_hash": split_sha256,
                    }
                ),
                "source_shard": shard_index,
                "source_path": str(receipt_source),
                "source_sha256": receipt_source_sha,
                "source_bytes": receipt_source.stat().st_size,
                "source_total_rows": source_rows,
                "source_rows": source_rows,
                "output_path": str(output_path),
                "output_sha256": output_sha,
                "output_bytes": output_bytes,
                "rows": source_rows,
                "fields": sorted(declared_fields),
                "eligible_trade_date_count": train_date_count,
                "split_manifest_hash": split_sha256,
                "stable_key": list(STABLE_KEY),
                "source_stable_key_digest": stable_digest,
                "source_payload_digest": payload_digest,
                "augmentation_receipt_path": str(receipt_path),
                "augmentation_receipt_sha256": _sha256(receipt_path),
                "status": "TIME_MAJOR_SHARD_READY",
            }
        )
        parity_records.append(
            {
                "schema_version": "cn_time_major_sidecar_augmentation_parity_v1",
                "status": "SIDECAR_PARITY_PASS",
                "source_shard": shard_index,
                "source_rows": source_rows,
                "sidecar_rows": source_rows,
                "stable_key_unique_count": source_rows,
                "source_stable_key_digest": stable_digest,
                "source_payload_digest": payload_digest,
                **{check: True for check in PARITY_CHECKS},
            }
        )

    if schema_intersection is None or not set(required_fields) <= schema_intersection:
        raise EvidenceError("session augmented schema intersection is incomplete")
    session_manifest: dict[str, Any] = {
        "schema_version": "cn_phase3cm_session_sidecar_augmentation_manifest_v1",
        "status": "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS",
        "output_root": str(session_root),
        "source_root": str(Path(base_manifest_path).parent.resolve()),
        "source_manifest": {
            "path": str(Path(base_manifest_path).resolve()),
            "sha256": _sha256(base_manifest_path),
            "status": str(base_manifest.get("status") or ""),
        },
        "row_count": total_rows,
        "shard_count": SHARD_COUNT,
        "required_fields": list(required_fields),
        "schema_intersection": sorted(schema_intersection),
        "schema_union": sorted(schema_union),
        "shards": aggregate_records,
        "fundamental_authority": fundamental,
        "split_manifest_hash": split_sha256,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    session_manifest["manifest_hash"] = _stable_hash(session_manifest)

    v2_manifest: dict[str, Any] = {
        "schema_version": V2_SCHEMA_VERSION,
        "status": V2_STATUS,
        "data_role": "development_train_only",
        "fields": sorted(declared_fields),
        "source_shard_count": SHARD_COUNT,
        "source_rows": total_rows,
        "source_total_rows": total_rows,
        "sidecar_rows": total_rows,
        "sidecar_bytes": total_bytes,
        "split_manifest_hash": split_sha256,
        "eligible_train_date_count": train_date_count,
        "shards": v2_records,
        "parity": parity_records,
        "session_augmentation_manifest": SESSION_MANIFEST_NAME,
        "fundamental_authority": fundamental,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    v2_manifest["manifest_hash"] = _stable_hash(v2_manifest)
    return session_manifest, v2_manifest


def _development_train_dates(split_manifest: Path, split_sha256: str) -> int:
    if _sha256(split_manifest) != split_sha256:
        raise EvidenceError("split manifest content hash drift")
    rows = _read_csv(split_manifest, "split manifest")
    train_dates = [str(row.get("trade_date") or "") for row in rows if str(row.get("split") or "").lower() == "train"]
    if not train_dates or len(train_dates) != len(set(train_dates)) or any(value[:4] >= "2026" for value in train_dates):
        raise EvidenceError("split manifest development calendar is empty, duplicated, or enters 2026")
    return len(train_dates)


def finalize_sidecars(
    *,
    active_manifest_path: Path,
    session_base_manifest_path: Path,
    augmentation_receipt_paths: Sequence[Path],
    coverage_audit_path: Path,
    active_candidate_table: Path,
    session_candidate_table: Path,
    split_manifest_path: Path,
    split_manifest_sha256: str,
    fundamental_manifest_path: Path,
    fundamental_source_index_path: Path,
    session_root: Path,
    output_root: Path,
    expected_active_pairs: int = ACTIVE_PAIR_COUNT,
    expected_session_pairs: int = SESSION_PAIR_COUNT,
) -> dict[str, Any]:
    split_sha256 = _hash(split_manifest_sha256, "split manifest SHA-256")
    train_date_count = _development_train_dates(split_manifest_path, split_sha256)
    active_candidate = _candidate_contract(
        active_candidate_table,
        backend=ACTIVE_CLOCK,
        expected_pair_count=expected_active_pairs,
    )
    session_candidate = _candidate_contract(
        session_candidate_table,
        backend=SESSION_CLOCK,
        expected_pair_count=expected_session_pairs,
    )
    active_manifest, _active_shards = _v2_manifest(
        active_manifest_path,
        label="active V2 manifest",
        split_sha256=split_sha256,
        required_fields=active_candidate["required_raw_fields"],
    )
    session_base_manifest, session_base_shards = _v2_manifest(
        session_base_manifest_path,
        label="session base V2 manifest",
        split_sha256=split_sha256,
        required_fields=["close"],
    )
    fundamental = _fundamental_authority(
        fundamental_manifest_path,
        fundamental_source_index_path,
        split_sha256=split_sha256,
    )
    session_manifest, session_v2_manifest = _session_augmentation(
        base_manifest_path=session_base_manifest_path,
        base_manifest=session_base_manifest,
        base_shards=session_base_shards,
        receipt_paths=augmentation_receipt_paths,
        session_root=session_root,
        split_sha256=split_sha256,
        train_date_count=train_date_count,
        required_fields=session_candidate["expression_raw_fields"],
        fundamental=fundamental,
    )

    session_root = Path(session_root).resolve()
    coverage = _coverage_audit(
        coverage_audit_path,
        active_candidate=active_candidate,
        session_candidate=session_candidate,
        active_root=Path(active_manifest_path).resolve().parent,
        session_root=session_root,
    )
    session_manifest_path = session_root / SESSION_MANIFEST_NAME
    session_v2_manifest_path = session_root / SESSION_V2_MANIFEST_NAME
    output_root = Path(output_root).resolve()
    closure_path = output_root / OVERALL_CLOSURE_NAME

    # All evidence is validated before the first authority file is published.
    _atomic_json(session_manifest_path, session_manifest)
    _atomic_json(session_v2_manifest_path, session_v2_manifest)
    closure: dict[str, Any] = {
        "schema_version": "cn_phase3cm_1024_sidecar_closure_v1",
        "status": "CN_PHASE3CM_1024_SIDECAR_CLOSURE_PASS",
        "data_role": "development_train_only",
        "pair_counts": {
            ACTIVE_CLOCK: int(active_candidate["pair_count"]),
            SESSION_CLOCK: int(session_candidate["pair_count"]),
            "total": int(active_candidate["pair_count"]) + int(session_candidate["pair_count"]),
        },
        "candidate_member_counts": {
            ACTIVE_CLOCK: int(active_candidate["candidate_member_count"]),
            SESSION_CLOCK: int(session_candidate["candidate_member_count"]),
        },
        "candidate_tables": {
            ACTIVE_CLOCK: active_candidate,
            SESSION_CLOCK: session_candidate,
        },
        "split_manifest": {
            "path": str(Path(split_manifest_path).resolve()),
            "sha256": split_sha256,
            "development_train_date_count": train_date_count,
        },
        "active_sidecar": {
            "root": str(Path(active_manifest_path).resolve().parent),
            "manifest_path": str(Path(active_manifest_path).resolve()),
            "manifest_sha256": _sha256(active_manifest_path),
            "manifest_status": str(active_manifest["status"]),
            "shard_count": SHARD_COUNT,
            "row_count": int(active_manifest.get("sidecar_rows") or active_manifest.get("source_rows") or 0),
            "required_raw_fields": list(active_candidate["required_raw_fields"]),
        },
        "session_sidecar": {
            "root": str(session_root),
            "augmentation_manifest_path": str(session_manifest_path),
            "augmentation_manifest_sha256": _sha256(session_manifest_path),
            "v2_manifest_path": str(session_v2_manifest_path),
            "v2_manifest_sha256": _sha256(session_v2_manifest_path),
            "manifest_status": V2_STATUS,
            "shard_count": SHARD_COUNT,
            "row_count": int(session_v2_manifest["sidecar_rows"]),
            "required_raw_fields": list(session_candidate["required_raw_fields"]),
        },
        "coverage_audit": coverage,
        "fundamental_authority": fundamental,
        "sealed_reads": {field: 0 for field in ACCESS_FIELDS},
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "formal_evaluator_authority": "UNCHANGED",
        "streaming_backend_authority": "EXPERIMENTAL_BACKEND",
    }
    closure["closure_hash"] = _stable_hash(closure)
    _atomic_json(closure_path, closure)
    return closure


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--active-manifest", type=Path, required=True)
    parser.add_argument("--session-base-manifest", type=Path, required=True)
    parser.add_argument("--augmentation-receipt", type=Path, action="append", required=True)
    parser.add_argument("--coverage-audit", type=Path, required=True)
    parser.add_argument("--active-candidate-table", type=Path, required=True)
    parser.add_argument("--session-candidate-table", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest-hash", required=True)
    parser.add_argument("--fundamental-manifest", type=Path, required=True)
    parser.add_argument("--fundamental-source-index", type=Path, required=True)
    parser.add_argument("--session-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--expected-active-pairs", type=int, default=ACTIVE_PAIR_COUNT)
    parser.add_argument("--expected-session-pairs", type=int, default=SESSION_PAIR_COUNT)
    args = parser.parse_args(argv)
    try:
        closure = finalize_sidecars(
            active_manifest_path=args.active_manifest.resolve(),
            session_base_manifest_path=args.session_base_manifest.resolve(),
            augmentation_receipt_paths=[path.resolve() for path in args.augmentation_receipt],
            coverage_audit_path=args.coverage_audit.resolve(),
            active_candidate_table=args.active_candidate_table.resolve(),
            session_candidate_table=args.session_candidate_table.resolve(),
            split_manifest_path=args.split_manifest.resolve(),
            split_manifest_sha256=args.split_manifest_hash,
            fundamental_manifest_path=args.fundamental_manifest.resolve(),
            fundamental_source_index_path=args.fundamental_source_index.resolve(),
            session_root=args.session_root.resolve(),
            output_root=args.output_root.resolve(),
            expected_active_pairs=args.expected_active_pairs,
            expected_session_pairs=args.expected_session_pairs,
        )
    except (EvidenceError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "status": "CN_PHASE3CM_1024_SIDECAR_CLOSURE_FAIL_CLOSED",
                    "errors": [str(exc)],
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(
        json.dumps(
            {
                "status": closure["status"],
                "closure_hash": closure["closure_hash"],
                "pair_counts": closure["pair_counts"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
