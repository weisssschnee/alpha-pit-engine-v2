"""Build one full-universe stock-session shard for strict resource preflight."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.chip_sidecar import (
    CHIP_FIELDS,
    load_chip_context,
    point_in_time_chip_context,
)
from our_system_phase2.services.fundamental_representations import CanonicalFundamentalMaterializer
from our_system_phase2.services.pit_fundamental_fabric import PITFundamentalFabricAdapter, normalize_cn_code
from our_system_phase2.services.typed_primitive_gate import expression_fields
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry


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
    rows = _read_csv(path)
    values = sorted(pd.Timestamp(row["trade_date"]) for row in rows if row.get("split") == "train")
    if not values or len(values) != len(set(values)):
        raise ValueError("split manifest does not define unique development sessions")
    return pd.DatetimeIndex(values)


def _source_panel(release_manifest: dict[str, Any], shard_index: int, release_root: Path) -> Path:
    target = f"shard_{int(shard_index):02d}/"
    matches = [row for row in release_manifest["files"] if str(row["relative_path"]).replace("\\", "/").startswith(target)]
    if len(matches) != 1:
        raise ValueError(f"release manifest does not resolve exactly one shard {shard_index}")
    return release_root / str(matches[0]["relative_path"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-root", type=Path, required=True)
    parser.add_argument("--release-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--candidate-table", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--fundamental-root", type=Path, required=True)
    parser.add_argument("--chip-root", type=Path)
    parser.add_argument("--maximum-observable-time", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    args = parser.parse_args()

    release = json.loads(args.release_manifest.read_text(encoding="utf-8"))
    if release.get("forbidden_roles_present") or bool(release.get("forward_2026_present")):
        raise RuntimeError("session preflight source contains forbidden data roles")
    source = _source_panel(release, args.shard_index, args.release_root)
    candidates = _read_csv(args.candidate_table)
    required_fields = sorted(
        {field for row in candidates for field in expression_fields(str(row["expression"]))}
    )
    parquet = pq.ParquetFile(source)
    schema = set(parquet.schema_arrow.names)
    physical_fields = sorted(set(required_fields) & schema)
    sidecar_fields = sorted(set(required_fields) - schema)
    chip_fields = sorted(set(sidecar_fields) & set(CHIP_FIELDS.values()))
    fundamental_fields = sorted(set(sidecar_fields) - set(chip_fields))
    if chip_fields and args.chip_root is None:
        raise PermissionError("session preflight chip fields require --chip-root")
    base_columns = [
        column
        for column in (
            "code",
            "trade_time",
            "date",
            "close",
            "open",
            "high",
            "low",
            "volume",
            "vol",
            "amount",
            "amount_yuan",
            "vwap",
            *physical_fields,
        )
        if column in schema
    ]
    parts: list[pd.DataFrame] = []
    for row_group in range(parquet.num_row_groups):
        frame = parquet.read_row_group(row_group, columns=base_columns).to_pandas()
        frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
        frame = frame.dropna(subset=["code", "trade_time", "close"])
        frame["_session"] = frame["trade_time"].dt.normalize()
        parts.append(
            frame.sort_values(["code", "trade_time"], kind="mergesort")
            .groupby(["code", "_session"], sort=False, as_index=False)
            .tail(1)
        )
    base = (
        pd.concat(parts, ignore_index=True)
        .sort_values(["code", "trade_time"], kind="mergesort")
        .drop_duplicates(["code", "_session"], keep="last")
        .reset_index(drop=True)
    )
    allowed_sessions = set(_sessions(args.split_manifest))
    base = base.loc[base["_session"].isin(allowed_sessions)].copy().reset_index(drop=True)
    base["session_time"] = base["_session"] + pd.Timedelta(hours=15)
    base["trade_time"] = base["session_time"]
    base["date"] = base["_session"]

    registry = UnifiedCapabilityRegistry.read(args.registry)
    adapter = PITFundamentalFabricAdapter(
        source_root=args.fundamental_root,
        sessions=pd.DatetimeIndex(sorted(allowed_sessions)),
        maximum_observable_time=args.maximum_observable_time,
    )
    materializer = CanonicalFundamentalMaterializer(adapter)
    coordinates = base[["code", "session_time"]].copy()
    coordinates["code"] = coordinates["code"].map(normalize_cn_code)
    coordinate_index = pd.MultiIndex.from_frame(coordinates)
    coverage: dict[str, float] = {}
    for field_id in fundamental_fields:
        capability = registry.resolve(field_id)
        spec = dict((capability.metadata or {}).get("canonical_representation") or {})
        if not spec or not bool(spec.get("search_eligible")):
            raise PermissionError(f"session preflight field is not search eligible: {field_id}")
        materialized = materializer.materialize(spec, coordinates)
        values = materialized.set_index(["code", "session_time"])[field_id]
        if values.index.has_duplicates:
            raise ValueError(f"duplicate PIT materialization coordinates: {field_id}")
        base[field_id] = values.reindex(coordinate_index).to_numpy()
        coverage[field_id] = round(float(base[field_id].notna().mean()), 8)

    chip_input: dict[str, Any] | None = None
    if chip_fields:
        chip, chip_input = load_chip_context(
            args.chip_root,
            allowed_codes=set(base["code"].astype(str)),
            fields=chip_fields,
            maximum_observable_time=args.maximum_observable_time,
        )
        base = point_in_time_chip_context(
            base,
            chip,
            fields=chip_fields,
            data_role="development",
        )
        for field_id in chip_fields:
            coverage[field_id] = round(float(base[field_id].notna().mean()), 8)

    for column in ("open", "high", "low", "vwap"):
        if column not in base:
            base[column] = base["close"]
    for column in ("volume", "vol"):
        if column not in base:
            base[column] = 1.0
    if "amount" not in base:
        base["amount"] = base["close"] * base["volume"]
    if "amount_yuan" not in base:
        base["amount_yuan"] = base["amount"]
    base = base.drop(columns=["_session", "session_time"], errors="ignore")
    base = base.sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)

    target = (
        args.output_root
        / f"shard_{int(args.shard_index):02d}"
        / "phase3aq_wide_true1min"
        / "canary"
        / "phase3aq_true_1min_formula_canary.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(".tmp.parquet")
    base.to_parquet(temp, index=False, compression="zstd")
    temp.replace(target)
    summary = {
        "status": "SESSION_PREFLIGHT_SHARD_READY",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "shard_index": int(args.shard_index),
        "data_role": "development",
        "validation_holdout_forward_read": False,
        "source_panel": str(source),
        "source_rows": int(parquet.metadata.num_rows),
        "session_rows": len(base),
        "session_count": int(base["date"].nunique()),
        "symbol_count": int(base["code"].nunique()),
        "physical_field_count": len(physical_fields),
        "sidecar_field_count": len(sidecar_fields),
        "fundamental_field_count": len(fundamental_fields),
        "chip_field_count": len(chip_fields),
        "chip_sidecar": chip_input,
        "sidecar_coverage": coverage,
        "output": {"path": str(target), "sha256": _sha256(target), "bytes": target.stat().st_size},
    }
    summary_path = args.output_root / f"shard_{int(args.shard_index):02d}" / "session_preflight_shard_manifest.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
