"""Add selected PIT fundamental/chip fields to a parity-proven session sidecar."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from our_system_phase2.services.chip_sidecar import (
    CHIP_FIELDS,
    load_chip_context,
    point_in_time_chip_context,
)
from our_system_phase2.services.fundamental_representations import CanonicalFundamentalMaterializer
from our_system_phase2.services.pit_fundamental_fabric import PITFundamentalFabricAdapter, normalize_cn_code
from our_system_phase2.services.typed_primitive_gate import expression_fields
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry


STABLE_KEY = ("trade_time", "code", "source_shard", "source_row_identity", "duplicate_ordinal")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _sessions(path: Path) -> pd.DatetimeIndex:
    values = sorted(
        pd.Timestamp(row["trade_date"])
        for row in _read_csv(path)
        if str(row.get("split") or "").lower() == "train"
    )
    if not values or len(values) != len(set(values)):
        raise ValueError("split manifest must define unique development sessions")
    return pd.DatetimeIndex(values)


def _frame_digest(frame: pd.DataFrame, columns: list[str]) -> str:
    values = pd.util.hash_pandas_object(frame[columns], index=False).to_numpy().tobytes()
    return hashlib.sha256(values).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--candidate-table", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--fundamental-root", type=Path, required=True)
    parser.add_argument("--chip-root", type=Path)
    parser.add_argument("--maximum-observable-time", required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    args = parser.parse_args()

    if not 0 <= int(args.shard_index) < 16:
        raise ValueError("session sidecar shard index must be in [0, 15]")
    source_manifest_path = args.source_root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    if str(source_manifest.get("data_role")) != "development_train_only":
        raise PermissionError("source session sidecar is not development/train-only")
    if any(int(source_manifest.get(key) or 0) for key in ("validation_reads", "holdout_reads", "forward_2026_reads")):
        raise PermissionError("source session sidecar records forbidden data access")

    source = args.source_root / f"shard_{int(args.shard_index):02d}.parquet"
    frame = pd.read_parquet(source)
    missing_key = sorted(set(STABLE_KEY) - set(frame.columns))
    if missing_key:
        raise ValueError(f"source session sidecar stable key is incomplete: {missing_key}")
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="raise")
    if frame["trade_time"].dt.year.ge(2026).any():
        raise PermissionError("source session sidecar enters sealed 2026")
    if frame.duplicated(list(STABLE_KEY)).any():
        raise ValueError("source session sidecar has duplicate stable keys")

    candidates = _read_csv(args.candidate_table)
    required_fields = sorted(
        {field for row in candidates for field in expression_fields(str(row.get("expression") or ""))}
    )
    missing_fields = sorted(set(required_fields) - set(frame.columns))
    chip_fields = sorted(set(missing_fields) & set(CHIP_FIELDS.values()))
    fundamental_fields = sorted(set(missing_fields) - set(chip_fields))
    if chip_fields and args.chip_root is None:
        raise PermissionError("chip fields require --chip-root")

    original_columns = list(frame.columns)
    original_key_digest = _frame_digest(frame, list(STABLE_KEY))
    original_payload_digest = _frame_digest(frame, original_columns)
    sessions = _sessions(args.split_manifest)
    registry = UnifiedCapabilityRegistry.read(args.registry)
    specs: dict[str, dict[str, Any]] = {}
    prefetch_fields: dict[str, set[str]] = {}
    for field_id in fundamental_fields:
        capability = registry.resolve(field_id)
        spec = dict((capability.metadata or {}).get("canonical_representation") or {})
        if not spec or not bool(spec.get("search_eligible")):
            raise PermissionError(f"session field is not PIT search eligible: {field_id}")
        specs[field_id] = spec
        for source_spec in spec.get("source_fields") or []:
            prefetch_fields.setdefault(str(source_spec["source_table"]), set()).add(
                str(source_spec["source_field"])
            )
    adapter = PITFundamentalFabricAdapter(
        source_root=args.fundamental_root,
        sessions=sessions,
        maximum_observable_time=args.maximum_observable_time,
        prefetch_source_fields_by_table=prefetch_fields,
    )
    materializer = CanonicalFundamentalMaterializer(adapter)
    coordinates = frame[["code", "trade_time"]].rename(columns={"trade_time": "session_time"}).copy()
    coordinates["code"] = coordinates["code"].map(normalize_cn_code)
    coordinate_index = pd.MultiIndex.from_frame(coordinates)
    coverage: dict[str, float] = {}
    for field_id in fundamental_fields:
        spec = specs[field_id]
        materialized = materializer.materialize(spec, coordinates)
        values = materialized.set_index(["code", "session_time"])[field_id]
        if values.index.has_duplicates:
            raise ValueError(f"duplicate PIT materialization coordinates: {field_id}")
        frame[field_id] = values.reindex(coordinate_index).to_numpy()
        coverage[field_id] = round(float(frame[field_id].notna().mean()), 8)

    chip_input: dict[str, Any] | None = None
    if chip_fields:
        chip, chip_input = load_chip_context(
            args.chip_root,
            allowed_codes=set(frame["code"].astype(str)),
            fields=chip_fields,
            maximum_observable_time=args.maximum_observable_time,
        )
        frame = point_in_time_chip_context(
            frame,
            chip,
            fields=chip_fields,
            data_role="development",
        )
        for field_id in chip_fields:
            coverage[field_id] = round(float(frame[field_id].notna().mean()), 8)

    if len(frame) != int(source_manifest["shards"][int(args.shard_index)]["rows"]):
        raise RuntimeError("session augmentation row count drift")
    if _frame_digest(frame, list(STABLE_KEY)) != original_key_digest:
        raise RuntimeError("session augmentation stable-key digest drift")
    if _frame_digest(frame, original_columns) != original_payload_digest:
        raise RuntimeError("session augmentation changed source payload columns")

    args.output_root.mkdir(parents=True, exist_ok=True)
    target = args.output_root / f"shard_{int(args.shard_index):02d}.parquet"
    temporary = target.with_suffix(".tmp.parquet")
    frame.to_parquet(temporary, index=False, compression="zstd")
    temporary.replace(target)
    summary = {
        "schema_version": "cn_phase3cm_session_sidecar_augmentation_v1",
        "status": "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "shard_index": int(args.shard_index),
        "data_role": "development_train_only",
        "source": {"path": str(source), "sha256": _sha256(source), "rows": len(frame)},
        "output": {"path": str(target), "sha256": _sha256(target), "bytes": target.stat().st_size},
        "source_stable_key_digest": original_key_digest,
        "source_payload_digest": original_payload_digest,
        "required_field_count": len(required_fields),
        "already_present_field_count": len(set(required_fields) & set(original_columns)),
        "fundamental_fields": fundamental_fields,
        "chip_fields": chip_fields,
        "coverage": coverage,
        "chip_sidecar": chip_input,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    summary_path = args.output_root / f"shard_{int(args.shard_index):02d}.augmentation.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
