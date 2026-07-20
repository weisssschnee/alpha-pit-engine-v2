"""Create an immutable, validated session-sidecar reuse root for a wider wave."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


MANIFEST_NAME = "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
RECEIPT_NAME = "CN_SESSION_REUSE_SOURCE_RECEIPT.json"
STABLE_KEY = ["trade_time", "code", "source_shard", "source_row_identity", "duplicate_ordinal"]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent, suffix=".tmp", mode="w", encoding="utf-8", delete=False
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def _self_hash(payload: dict[str, Any]) -> str:
    canonical = dict(payload)
    canonical.pop("receipt_hash", None)
    return hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def prepare_reuse_source(
    *,
    base_manifest_path: Path,
    augmentation_manifest_path: Path,
    output_root: Path,
) -> dict[str, Any]:
    base = _json(base_manifest_path)
    augmented = _json(augmentation_manifest_path)
    if base.get("status") != "TIME_MAJOR_LAYOUT_PARITY_PASS":
        raise ValueError("base session manifest is not parity-qualified")
    if augmented.get("status") != "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS":
        raise ValueError("augmentation manifest is not parity-qualified")
    if base.get("data_role") != "development_train_only":
        raise PermissionError("base session manifest is not development/train-only")
    for payload in (base, augmented):
        if any(int(payload.get(key) or 0) for key in ("validation_reads", "holdout_reads", "forward_2026_reads")):
            raise PermissionError("session reuse source records forbidden data access")

    base_shards = {int(row["source_shard"]): row for row in base.get("shards") or []}
    augmented_shards = {int(row["shard_index"]): row for row in augmented.get("shards") or []}
    if set(base_shards) != set(range(16)) or set(augmented_shards) != set(range(16)):
        raise ValueError("session reuse source requires exactly 16 shards")

    output_root.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    fields: list[str] | None = None
    for shard_index in range(16):
        base_row = base_shards[shard_index]
        receipt = augmented_shards[shard_index]
        receipt_path = augmentation_manifest_path.parent / f"shard_{shard_index:02d}.augmentation.json"
        if _json(receipt_path) != receipt:
            raise ValueError(f"aggregate/individual augmentation receipt drift for shard {shard_index}")
        if receipt.get("status") != "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS":
            raise ValueError(f"augmentation shard {shard_index} is not parity-qualified")
        if any(int(receipt.get(key) or 0) for key in ("validation_reads", "holdout_reads", "forward_2026_reads")):
            raise PermissionError(f"augmentation shard {shard_index} records forbidden data access")
        source = Path(str(receipt["source"]["path"]))
        augmented_file = Path(str(receipt["output"]["path"]))
        if _sha256(source) != str(receipt["source"]["sha256"]):
            raise ValueError(f"augmentation source hash drift for shard {shard_index}")
        if _sha256(augmented_file) != str(receipt["output"]["sha256"]):
            raise ValueError(f"augmentation output hash drift for shard {shard_index}")
        if source.resolve() != Path(str(base_row["output_path"])).resolve():
            raise ValueError(f"augmentation source path drift for shard {shard_index}")
        if str(receipt["source"]["sha256"]) != str(base_row["output_sha256"]):
            raise ValueError(f"augmentation source authority drift for shard {shard_index}")

        parquet = pq.ParquetFile(augmented_file)
        shard_fields = parquet.schema_arrow.names
        if fields is None:
            fields = list(shard_fields)
        elif list(shard_fields) != fields:
            raise ValueError(f"augmentation schema drift for shard {shard_index}")
        if parquet.metadata.num_rows != int(receipt["source"]["rows"]):
            raise ValueError(f"augmentation row-count drift for shard {shard_index}")
        missing_key = sorted(set(STABLE_KEY) - set(shard_fields))
        if missing_key:
            raise ValueError(f"augmentation stable key missing for shard {shard_index}: {missing_key}")

        target = output_root / f"shard_{shard_index:02d}.parquet"
        if target.exists():
            if _sha256(target) != str(receipt["output"]["sha256"]):
                raise ValueError(f"existing reuse target hash drift for shard {shard_index}")
            link_mode = "EXISTING_EXACT"
        else:
            try:
                os.link(augmented_file, target)
                link_mode = "HARDLINK"
            except OSError:
                shutil.copy2(augmented_file, target)
                link_mode = "COPY_FALLBACK"
        records.append(
            {
                "schema_version": "cn_development_time_major_execution_layout_v2_train_only",
                "status": "TIME_MAJOR_SHARD_READY",
                "source_shard": shard_index,
                "source_path": str(augmented_file),
                "source_sha256": str(receipt["output"]["sha256"]),
                "source_rows": parquet.metadata.num_rows,
                "source_total_rows": parquet.metadata.num_rows,
                "output_path": str(target),
                "output_sha256": str(receipt["output"]["sha256"]),
                "output_bytes": target.stat().st_size,
                "rows": parquet.metadata.num_rows,
                "fields": fields,
                "stable_key": STABLE_KEY,
                "source_stable_key_digest": str(receipt["source_stable_key_digest"]),
                "source_payload_digest": str(receipt["source_payload_digest"]),
                "augmentation_receipt_sha256": _sha256(receipt_path),
                "link_mode": link_mode,
                "split_manifest_hash": str(base["split_manifest_hash"]),
            }
        )

    layout = {
        "schema_version": "cn_development_time_major_execution_layout_manifest_v2_train_only",
        "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
        "data_role": "development_train_only",
        "split_manifest_hash": str(base["split_manifest_hash"]),
        "eligible_train_date_count": int(base["eligible_train_date_count"]),
        "fields": fields or [],
        "source_shard_count": 16,
        "source_rows": sum(int(row["rows"]) for row in records),
        "source_total_rows": sum(int(row["rows"]) for row in records),
        "sidecar_rows": sum(int(row["rows"]) for row in records),
        "sidecar_bytes": sum(int(row["output_bytes"]) for row in records),
        "thread_environment": dict(base.get("thread_environment") or {}),
        "shards": records,
        "parity": [
            {
                "source_shard": row["source_shard"],
                "status": "SIDECAR_REUSE_EXACT_HASH_PASS",
                "source_sha256": row["source_sha256"],
                "output_sha256": row["output_sha256"],
                "rows": row["rows"],
            }
            for row in records
        ],
        "reuse_contract": "HASH_EXACT_IMMUTABLE_AUGMENTED_256_SOURCE",
        "source_base_manifest": str(base_manifest_path),
        "source_base_manifest_sha256": _sha256(base_manifest_path),
        "source_augmentation_manifest": str(augmentation_manifest_path),
        "source_augmentation_manifest_sha256": _sha256(augmentation_manifest_path),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    _atomic_json(output_root / MANIFEST_NAME, layout)
    receipt = {
        "schema_version": "cn_phase3cm_session_reuse_source_receipt_v1",
        "status": "CN_PHASE3CM_SESSION_REUSE_SOURCE_READY",
        "output_root": str(output_root),
        "layout_manifest": str(output_root / MANIFEST_NAME),
        "layout_manifest_sha256": _sha256(output_root / MANIFEST_NAME),
        "shard_count": 16,
        "row_count": layout["sidecar_rows"],
        "field_count": len(layout["fields"]),
        "reuse_contract": layout["reuse_contract"],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    receipt["receipt_hash"] = _self_hash(receipt)
    _atomic_json(output_root / RECEIPT_NAME, receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-manifest", type=Path, required=True)
    parser.add_argument("--augmentation-manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    result = prepare_reuse_source(
        base_manifest_path=args.base_manifest.resolve(),
        augmentation_manifest_path=args.augmentation_manifest.resolve(),
        output_root=args.output_root.resolve(),
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
