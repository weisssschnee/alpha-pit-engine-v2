"""Build the development-only stock-session panel for compositional sketches."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.compositional_session_signal_panel import (  # noqa: E402
    SESSION_PANEL_VERSION,
    attach_coordinate_row_indices,
    build_full_session_coordinate_rows,
    field_partition,
)
from our_system_phase2.services.chip_sidecar import (  # noqa: E402
    CHIP_FIELDS,
    load_chip_context,
    point_in_time_chip_context,
)
from our_system_phase2.services.fundamental_representations import (  # noqa: E402
    CanonicalFundamentalMaterializer,
)
from our_system_phase2.services.materialization_support_receipt import (  # noqa: E402
    build_materialization_support_receipt,
    materialization_frame_fingerprints,
    verify_materialization_support_receipt,
)
from our_system_phase2.services.pit_fundamental_fabric import (  # noqa: E402
    PITFundamentalFabricAdapter,
    normalize_cn_code,
)
from our_system_phase2.services.unified_capability_registry import (  # noqa: E402
    UnifiedCapabilityRegistry,
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _development_sessions(path: Path) -> pd.DatetimeIndex:
    rows = _read_csv(path)
    values = [pd.Timestamp(row["trade_date"]) for row in rows if row.get("split") == "train"]
    if not values or len(values) != len(set(values)):
        raise ValueError("split manifest must provide unique development/train sessions")
    return pd.DatetimeIndex(sorted(values))


def _context_fields(path: Path) -> list[str]:
    return sorted(
        {
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    )


SESSION_SIDECAR_MANIFEST = "CN_SESSION_SIDECAR_AUGMENTATION_MANIFEST.json"


def _required_zero_access(payload: Mapping[str, Any]) -> None:
    for key in ("validation_reads", "holdout_reads", "forward_2026_reads"):
        value = payload.get(key)
        if type(value) is not int or value != 0:
            raise PermissionError(
                f"session sidecar manifest must record exact integer zero for {key}"
            )


def prepare_sidecar_base(args: argparse.Namespace) -> int:
    """Publish a full-development stock-session coordinate base from sidecars."""

    sidecar_root = args.session_sidecar_root.resolve()
    source_manifest_path = sidecar_root / SESSION_SIDECAR_MANIFEST
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8-sig"))
    if (
        source_manifest.get("schema_version")
        != "cn_phase3cm_session_sidecar_augmentation_manifest_v1"
        or source_manifest.get("status")
        != "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS"
    ):
        raise PermissionError("session sidecar augmentation lacks its parity authority")
    if Path(str(source_manifest.get("output_root") or "")).resolve() != sidecar_root:
        raise ValueError("session sidecar manifest output root drift")
    _required_zero_access(source_manifest)

    sessions = _development_sessions(args.split_manifest)
    session_dates = {value.normalize() for value in sessions}
    split_hash = _sha256(args.split_manifest)

    expected_shards = int(getattr(args, "expected_shard_count", 16))
    discovered_shards = sorted(sidecar_root.glob("shard_*.parquet"))
    expected_names = [f"shard_{index:02d}.parquet" for index in range(expected_shards)]
    if [path.name for path in discovered_shards] != expected_names:
        raise ValueError("session sidecar directory does not contain the exact shard set")
    if type(source_manifest.get("shard_count")) is not int or int(
        source_manifest["shard_count"]
    ) != expected_shards:
        raise ValueError("session sidecar manifest shard count mismatch")
    if type(source_manifest.get("row_count")) is not int or int(
        source_manifest["row_count"]
    ) <= 0:
        raise ValueError("session sidecar manifest row count is invalid")
    shard_records = source_manifest.get("shards")
    if not isinstance(shard_records, list) or len(shard_records) != expected_shards:
        raise ValueError("session sidecar manifest shard inventory mismatch")
    records_by_index = {int(row["shard_index"]): row for row in shard_records}
    if set(records_by_index) != set(range(expected_shards)):
        raise ValueError("session sidecar manifest shard indices are incomplete")

    parts: list[pd.DataFrame] = []
    shard_inputs: list[dict[str, Any]] = []
    actual_row_count = 0
    for shard_index in range(expected_shards):
        record = records_by_index[shard_index]
        if record.get("status") != "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS":
            raise PermissionError(f"session sidecar shard {shard_index} lacks parity PASS")
        if record.get("data_role") != "development_train_only":
            raise PermissionError(
                f"session sidecar shard {shard_index} is not development/train-only"
            )
        _required_zero_access(record)
        output = record.get("output")
        source = record.get("source")
        if not isinstance(output, Mapping) or not isinstance(source, Mapping):
            raise ValueError(f"session sidecar shard {shard_index} binding is incomplete")
        path = Path(str(output.get("path") or "")).resolve()
        if path.parent != sidecar_root or path.name != f"shard_{shard_index:02d}.parquet":
            raise ValueError(f"session sidecar shard {shard_index} output path drift")
        if not path.is_file():
            raise FileNotFoundError(path)
        expected_sha256 = str(output.get("sha256") or "")
        if len(expected_sha256) != 64 or _sha256(path) != expected_sha256:
            raise ValueError(f"session sidecar shard {shard_index} output hash drift")
        if type(output.get("bytes")) is not int or int(output["bytes"]) != path.stat().st_size:
            raise ValueError(f"session sidecar shard {shard_index} output size drift")
        parquet = pq.ParquetFile(path)
        source_rows = source.get("rows")
        if type(source_rows) is not int or int(source_rows) != parquet.metadata.num_rows:
            raise ValueError(f"session sidecar shard {shard_index} row count drift")
        actual_row_count += int(source_rows)
        names = set(parquet.schema_arrow.names)
        if "code" not in names:
            raise ValueError(f"session sidecar shard lacks code: {path}")
        if "session_time" in names:
            source_time_column = "session_time"
        elif "trade_time" in names:
            source_time_column = "trade_time"
        else:
            raise ValueError(f"session sidecar shard lacks a session clock: {path}")
        frame = pd.read_parquet(path, columns=["code", source_time_column]).rename(
            columns={source_time_column: "session_time"}
        )
        if parquet.metadata.num_rows != len(frame):
            raise ValueError(f"session sidecar shard row count mismatch: {path}")
        frame["session_time"] = pd.to_datetime(frame["session_time"], errors="raise")
        if frame["code"].isna().any() or frame["code"].astype(str).str.strip().eq("").any():
            raise ValueError(f"session sidecar shard has invalid codes: {path}")
        if frame["session_time"].isna().any():
            raise ValueError(f"session sidecar shard has invalid session times: {path}")
        parts.append(frame)
        shard_inputs.append(
            {
                "source_shard": shard_index,
                "path": str(path),
                "rows": len(frame),
                "sha256": expected_sha256,
                "source_time_column": source_time_column,
            }
        )

    if actual_row_count != int(source_manifest["row_count"]):
        raise ValueError("session sidecar manifest total row count drift")

    base = pd.concat(parts, ignore_index=True)
    base["code"] = base["code"].astype(str)
    if base.duplicated(["code", "session_time"]).any():
        raise ValueError("session sidecar has duplicate stock-session coordinates")
    base["date"] = base["session_time"].dt.normalize()
    if base.duplicated(["code", "date"]).any():
        raise ValueError("session sidecar has multiple coordinates for one stock-session")
    observed_dates = set(base["date"].drop_duplicates())
    if observed_dates != session_dates:
        missing = sorted(value.strftime("%Y-%m-%d") for value in session_dates - observed_dates)
        extra = sorted(value.strftime("%Y-%m-%d") for value in observed_dates - session_dates)
        raise ValueError(
            f"session sidecar date set differs from frozen train split: missing={missing[:8]} extra={extra[:8]}"
        )
    base["trade_time"] = base["session_time"]
    base = (
        base[["code", "trade_time", "session_time", "date"]]
        .sort_values(["code", "session_time"], kind="mergesort")
        .reset_index(drop=True)
    )

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    base_path = output_root / "session_signal_base_panel.parquet"
    manifest_path = output_root / "session_signal_base_manifest.json"
    if base_path.exists() or manifest_path.exists():
        raise FileExistsError("refusing to replace an existing committed session base")
    token = uuid.uuid4().hex
    temporary_base = output_root / f".{base_path.name}.{token}.tmp"
    temporary_manifest = output_root / f".{manifest_path.name}.{token}.tmp"
    base_installed = False
    try:
        base.to_parquet(temporary_base, index=False, compression="zstd")
        manifest = {
            "schema_version": "cn_full_development_session_coordinate_base_v1",
            "status": "FULL_DEVELOPMENT_SESSION_BASE_PREPARED",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "data_role": "development_train_only",
            "labels_or_returns_read": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "development_session_count": len(sessions),
            "stock_count": int(base["code"].nunique()),
            "base_panel_rows": len(base),
            "coordinate_unique": True,
            "inputs": {
                "session_sidecar_manifest": {
                    "path": str(source_manifest_path),
                    "sha256": _sha256(source_manifest_path),
                },
                "split_manifest": {
                    "path": str(args.split_manifest.resolve()),
                    "sha256": split_hash,
                },
                "shards": shard_inputs,
            },
            "artifacts": {
                "base_panel": {
                    "path": str(base_path),
                    "sha256": _sha256(temporary_base),
                }
            },
        }
        _write_json(temporary_manifest, manifest)
        os.replace(temporary_base, base_path)
        base_installed = True
        os.replace(temporary_manifest, manifest_path)
    except BaseException:
        if base_installed:
            base_path.unlink(missing_ok=True)
        raise
    finally:
        temporary_base.unlink(missing_ok=True)
        temporary_manifest.unlink(missing_ok=True)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


def _load_chip_context(
    root: Path,
    *,
    allowed_codes: set[str],
    fields: list[str],
    maximum_observable_time: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    return load_chip_context(
        root,
        allowed_codes=allowed_codes,
        fields=fields,
        maximum_observable_time=maximum_observable_time,
    )


def prepare(args: argparse.Namespace) -> int:
    sessions = _development_sessions(args.split_manifest)
    session_set = set(sessions)
    selections = _read_csv(args.stock_selection)
    active_coordinates = _read_csv(args.active_coordinate_manifest)
    context_fields = _context_fields(args.session_context_fields)
    chip_field_names = set(CHIP_FIELDS.values())
    chip_fields = sorted(set(context_fields) & chip_field_names)
    panel_context_fields = sorted(set(context_fields) - chip_field_names)
    if chip_fields and (args.chip_root is None or not args.maximum_observable_time):
        raise ValueError(
            "chip context fields require --chip-root and --maximum-observable-time"
        )
    parts: list[pd.DataFrame] = []
    selection_frame = pd.DataFrame(selections)
    if selection_frame["code"].astype(str).duplicated().any():
        raise ValueError("stock selection must contain unique codes")
    for (panel_text, row_group), group in selection_frame.groupby(
        ["panel", "row_group"], sort=True
    ):
        panel = Path(str(panel_text))
        parquet = pq.ParquetFile(panel)
        columns = ["code", "trade_time", "date", *panel_context_fields]
        missing = sorted(set(columns) - set(parquet.schema_arrow.names))
        if missing:
            raise RuntimeError(f"{panel} missing session fields: {missing}")
        table = parquet.read_row_group(int(row_group), columns=columns)
        codes = group["code"].astype(str).tolist()
        table = table.filter(pc.is_in(table["code"], value_set=pa.array(codes)))
        frame = table.to_pandas()
        frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
        frame["_session"] = frame["trade_time"].dt.normalize()
        frame = frame.loc[frame["_session"].isin(session_set)].copy()
        frame = (
            frame.sort_values(["code", "trade_time"], kind="mergesort")
            .groupby(["code", "_session"], sort=False, as_index=False)
            .tail(1)
        )
        parts.append(frame[["code", "_session", *panel_context_fields]])
    base = (
        pd.concat(parts, ignore_index=True)
        .drop_duplicates(["code", "_session"], keep="last")
        .sort_values(["code", "_session"], kind="mergesort")
        .reset_index(drop=True)
    )
    base["trade_time"] = base["_session"] + pd.Timedelta(hours=15)
    base["session_time"] = base["trade_time"]
    base["date"] = base["_session"]
    base = base[["code", "trade_time", "session_time", "date", *panel_context_fields]]
    chip_input: dict[str, Any] | None = None
    if chip_fields:
        chip, chip_input = _load_chip_context(
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
    available = {
        (str(code), pd.Timestamp(trade_time).normalize())
        for code, trade_time in zip(base["code"], base["trade_time"], strict=True)
    }
    coordinates = build_full_session_coordinate_rows(
        active_coordinates, sessions=sessions, available_keys=available
    )
    coordinates = attach_coordinate_row_indices(coordinates, base)
    args.output_root.mkdir(parents=True, exist_ok=True)
    base_path = args.output_root / "session_signal_base_panel.parquet"
    coordinate_path = args.output_root / "signal_sketch_coordinate_manifest.csv"
    base.to_parquet(base_path, index=False, compression="zstd")
    _write_csv(coordinate_path, coordinates)
    manifest = {
        "status": "SESSION_SIGNAL_BASE_PANEL_PREPARED",
        "panel_version": SESSION_PANEL_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_role": "development",
        "labels_or_returns_read": False,
        "validation_holdout_forward_read": False,
        "selected_stock_count": int(base["code"].nunique()),
        "development_session_count": len(sessions),
        "base_panel_rows": len(base),
        "context_field_count": len(context_fields),
        "panel_context_field_count": len(panel_context_fields),
        "chip_context_field_count": len(chip_fields),
        "coordinate_count": len(coordinates),
        "coordinate_count_by_set": dict(
            Counter(row["coordinate_set"] for row in coordinates)
        ),
        "materialized_coordinate_count_by_set": dict(
            Counter(
                row["coordinate_set"]
                for row in coordinates
                if int(row["row_index"]) >= 0
            )
        ),
        "inputs": {
            "split_manifest": {"path": str(args.split_manifest), "sha256": _sha256(args.split_manifest)},
            "stock_selection": {"path": str(args.stock_selection), "sha256": _sha256(args.stock_selection)},
            "active_coordinate_manifest": {"path": str(args.active_coordinate_manifest), "sha256": _sha256(args.active_coordinate_manifest)},
            "shard_root": str(args.shard_root),
            "chip_sidecar": chip_input,
        },
        "artifacts": {
            "base_panel": {"path": str(base_path), "sha256": _sha256(base_path)},
            "coordinate_manifest": {"path": str(coordinate_path), "sha256": _sha256(coordinate_path)},
        },
    }
    _write_json(args.output_root / "session_signal_base_manifest.json", manifest)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


def materialize(args: argparse.Namespace) -> int:
    sessions = _development_sessions(args.split_manifest)
    base = pd.read_parquet(args.base_panel, columns=["code", "session_time"])
    coordinates = base.copy()
    coordinates["code"] = coordinates["code"].map(normalize_cn_code)
    if coordinates["code"].eq("").any() or coordinates.duplicated(
        ["code", "session_time"]
    ).any():
        raise ValueError("fundamental coordinates are invalid after code normalization")
    registry = UnifiedCapabilityRegistry.read(args.registry)
    input_manifest = json.loads(args.input_manifest.read_text(encoding="utf-8"))
    all_fields = sorted(str(value) for value in input_manifest["canonical_fundamental_fields"])
    fields = [
        value
        for value in all_fields
        if field_partition(value, args.partition_count) == args.partition_index
    ]
    adapter = PITFundamentalFabricAdapter(
        source_root=args.fundamental_root,
        sessions=sessions,
        maximum_observable_time=args.maximum_observable_time,
    )
    materializer = CanonicalFundamentalMaterializer(adapter)
    args.cache_root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for field_id in fields:
        path = args.cache_root / f"{field_id}.parquet"
        receipt_path = args.cache_root / f"{field_id}.materialization_support_receipt.json"
        try:
            capability = registry.resolve(field_id)
            spec = dict((capability.metadata or {}).get("canonical_representation") or {})
            if not spec or not bool(spec.get("search_eligible")):
                raise PermissionError("canonical representation is not search eligible")
            partition_identity = {
                "panel_version": SESSION_PANEL_VERSION,
                "partition_index": int(args.partition_index),
                "partition_count": int(args.partition_count),
                "field_partition": int(field_partition(field_id, args.partition_count)),
            }
            if (
                path.exists()
                and receipt_path.exists()
                and field_id in pq.ParquetFile(path).schema_arrow.names
            ):
                frame = pd.read_parquet(
                    path, columns=["code", "session_time", field_id]
                )
                receipt = verify_materialization_support_receipt(
                    json.loads(receipt_path.read_text(encoding="utf-8")),
                    expected_field_id=field_id,
                    expected_registry_hash=registry.registry_hash,
                    expected_partition_identity=partition_identity,
                    required_receipt_type=(
                        "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT"
                    ),
                )
                fingerprints = materialization_frame_fingerprints(
                    frame, field_id=field_id, row_keys=("code", "session_time")
                )
                if (
                    receipt["coordinate_contract"]["coordinate_fingerprint"]
                    != fingerprints["coordinate_fingerprint"]
                    or receipt["materialization"]["value_fingerprint"]
                    != fingerprints["value_fingerprint"]
                ):
                    raise ValueError("cached fundamental field differs from its receipt")
                rows.append(
                    {
                        "field_id": field_id,
                        "status": "RESUMED",
                        "coverage": round(float(frame[field_id].notna().mean()), 8),
                        "sha256": _sha256(path),
                        "materialization_support_receipt_hash": receipt["receipt_hash"],
                        "materialization_support_receipt_sha256": _sha256(receipt_path),
                    }
                )
                continue
            frame = materializer.materialize(spec, coordinates)
            keep = frame[["code", "session_time", field_id]].copy()
            materializer_manifest = {
                "materializer": "CanonicalFundamentalMaterializer",
                "pit_adapter": "PITFundamentalFabricAdapter",
                "canonical_representation": spec,
                "development_maximum_observable_time": str(
                    args.maximum_observable_time
                ),
                "session_calendar_sha256": hashlib.sha256(
                    "|".join(pd.DatetimeIndex(sessions).astype(str)).encode("utf-8")
                ).hexdigest(),
                "split_manifest_sha256": _sha256(args.split_manifest),
                "fundamental_manifest_sha256": _sha256(args.fundamental_manifest),
            }
            receipt = build_materialization_support_receipt(
                keep,
                field_id=field_id,
                representation_id=capability.representation_id,
                registry_hash=registry.registry_hash,
                receipt_type=(
                    "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT"
                ),
                materializer_authority=(
                    "CanonicalFundamentalMaterializer/PITFundamentalFabricAdapter"
                ),
                materializer_manifest=materializer_manifest,
                support_unit=capability.support_unit,
                observable_time_contract=capability.observable_clock,
                maturity_contract=capability.maturity_rule,
                row_keys=("code", "session_time"),
                partition_identity=partition_identity,
                source_binding={
                    "source_field_id": capability.source_field_id,
                    "source_table": capability.source_table,
                    "source_field": capability.source_field,
                    "pit_status": capability.pit_status,
                    "fundamental_manifest": str(args.fundamental_manifest),
                    "fundamental_manifest_sha256": _sha256(
                        args.fundamental_manifest
                    ),
                    "split_manifest_sha256": _sha256(args.split_manifest),
                    "maximum_observable_time": str(args.maximum_observable_time),
                },
            )
            verify_materialization_support_receipt(
                receipt,
                expected_field_id=field_id,
                expected_registry_hash=registry.registry_hash,
                expected_partition_identity=partition_identity,
                required_receipt_type=(
                    "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT"
                ),
            )
            temp = path.with_suffix(".tmp.parquet")
            temp_receipt = receipt_path.with_suffix(".tmp.json")
            keep.to_parquet(temp, index=False, compression="zstd")
            _write_json(temp_receipt, receipt)
            os.replace(temp, path)
            os.replace(temp_receipt, receipt_path)
            rows.append(
                {
                    "field_id": field_id,
                    "status": "MATERIALIZED",
                    "coverage": round(float(keep[field_id].notna().mean()), 8),
                    "sha256": _sha256(path),
                    "materialization_support_receipt_hash": receipt["receipt_hash"],
                    "materialization_support_receipt_sha256": _sha256(receipt_path),
                }
            )
        except Exception as exc:  # field failures are isolated and reported fail-closed.
            failures.append({"field_id": field_id, "error": repr(exc)})
    summary = {
        "status": "SESSION_FUNDAMENTAL_FIELDS_MATERIALIZED" if not failures else "SESSION_FUNDAMENTAL_FIELD_FAILURE",
        "panel_version": SESSION_PANEL_VERSION,
        "partition_index": args.partition_index,
        "partition_count": args.partition_count,
        "assigned_field_count": len(fields),
        "completed_field_count": len(rows),
        "failure_count": len(failures),
        "data_role": "development",
        "labels_or_returns_read": False,
        "validation_holdout_forward_read": False,
        "fields": rows,
        "failures": failures,
    }
    _write_json(args.cache_root / f"worker_{args.partition_index}.summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0 if not failures else 2


def assemble(args: argparse.Namespace) -> int:
    base = pd.read_parquet(args.base_panel).sort_values(
        ["code", "trade_time"], kind="mergesort"
    ).reset_index(drop=True)
    base["_normal_code"] = base["code"].map(normalize_cn_code)
    input_manifest = json.loads(args.input_manifest.read_text(encoding="utf-8"))
    fields = sorted(str(value) for value in input_manifest["canonical_fundamental_fields"])
    key = pd.MultiIndex.from_frame(base[["_normal_code", "session_time"]])
    missing: list[str] = []
    materialized_columns: dict[str, np.ndarray] = {}
    receipt_bindings: dict[str, dict[str, str]] = {}
    cache_receipts: dict[str, dict[str, Any]] = {}
    registry_hash = str(input_manifest.get("registry_hash") or "")
    if len(registry_hash) != 64:
        raise ValueError("session input manifest lacks a bound unified registry hash")
    fundamental_manifest_sha256 = _sha256(args.fundamental_manifest)
    for field_id in fields:
        path = args.cache_root / f"{field_id}.parquet"
        receipt_path = args.cache_root / f"{field_id}.materialization_support_receipt.json"
        if not path.exists() or not receipt_path.exists():
            missing.append(field_id)
            continue
        frame = pd.read_parquet(path, columns=["code", "session_time", field_id])
        series = frame.set_index(["code", "session_time"])[field_id]
        if series.index.has_duplicates:
            raise ValueError(f"duplicate fundamental materialization coordinates: {field_id}")
        if len(series) != len(key) or not series.index.sort_values().equals(key.sort_values()):
            raise ValueError(f"fundamental materialization coordinate mismatch: {field_id}")
        receipt = verify_materialization_support_receipt(
            json.loads(receipt_path.read_text(encoding="utf-8")),
            expected_field_id=field_id,
            expected_registry_hash=registry_hash,
            required_receipt_type=(
                "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT"
            ),
        )
        if receipt["source_binding"].get(
            "fundamental_manifest_sha256"
        ) != fundamental_manifest_sha256:
            raise ValueError(f"fundamental source manifest mismatch: {field_id}")
        fingerprints = materialization_frame_fingerprints(
            frame, field_id=field_id, row_keys=("code", "session_time")
        )
        if (
            receipt["coordinate_contract"]["coordinate_fingerprint"]
            != fingerprints["coordinate_fingerprint"]
            or receipt["materialization"]["value_fingerprint"]
            != fingerprints["value_fingerprint"]
        ):
            raise ValueError(f"fundamental receipt/value mismatch: {field_id}")
        receipt_bindings[field_id] = {
            "receipt_hash": str(receipt["receipt_hash"]),
            "receipt_sha256": _sha256(receipt_path),
        }
        cache_receipts[field_id] = receipt
        materialized_columns[field_id] = series.reindex(key).to_numpy()
    if missing:
        raise RuntimeError(f"missing fundamental materializations: {missing[:8]}")
    base = pd.concat(
        [base, pd.DataFrame(materialized_columns, index=base.index)], axis=1
    )
    args.output_root.mkdir(parents=True, exist_ok=True)
    output_panel = args.output_root / "signal_sketch_compact_panel.parquet"
    output_coordinates = args.output_root / "signal_sketch_coordinate_manifest.csv"
    base = base.drop(columns=["_normal_code", "session_time"])
    base.to_parquet(output_panel, index=False, compression="zstd")
    panel_receipt_bindings: dict[str, dict[str, str]] = {}
    for field_id in fields:
        upstream = cache_receipts[field_id]
        panel_values = base[["code", "trade_time", field_id]].copy()
        panel_receipt = build_materialization_support_receipt(
            panel_values,
            field_id=field_id,
            representation_id=str(upstream["representation_id"]),
            registry_hash=registry_hash,
            receipt_type="PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT",
            materializer_authority="CompositionalSessionSignalPanelAssembler",
            materializer_manifest={
                "panel_version": SESSION_PANEL_VERSION,
                "upstream_receipt_hash": upstream["receipt_hash"],
                "base_panel_sha256": _sha256(args.base_panel),
                "fundamental_manifest_sha256": fundamental_manifest_sha256,
            },
            support_unit=str(upstream["support"]["support_unit"]),
            observable_time_contract=str(upstream["observable_time_contract"]),
            maturity_contract=str(upstream["maturity_contract"]),
            row_keys=("code", "trade_time"),
            partition_identity={
                "panel_version": SESSION_PANEL_VERSION,
                "assembly": "FULL_DEVELOPMENT_SESSION_PANEL",
            },
            source_binding={
                **dict(upstream.get("source_binding") or {}),
                "upstream_receipt_hash": upstream["receipt_hash"],
                "split_manifest_sha256": _sha256(args.split_manifest),
                "maximum_observable_time": str(args.maximum_observable_time),
            },
            evidence_contract={
                "evidence_role": "FULL_DEVELOPMENT_ROOT_AUTHORITY",
                "split_manifest": {
                    "path": str(args.split_manifest.resolve()),
                    "sha256": _sha256(args.split_manifest),
                },
                "coordinate_authority": {
                    "path": str(args.base_panel.resolve()),
                    "sha256": _sha256(args.base_panel),
                },
                "materialized_artifact": {
                    "path": str(output_panel.resolve()),
                    "sha256": _sha256(output_panel),
                },
            },
        )
        verify_materialization_support_receipt(
            panel_receipt,
            expected_field_id=field_id,
            expected_registry_hash=registry_hash,
            expected_partition_identity={
                "panel_version": SESSION_PANEL_VERSION,
                "assembly": "FULL_DEVELOPMENT_SESSION_PANEL",
            },
            required_receipt_type=(
                "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT"
            ),
        )
        panel_receipt_path = (
            args.output_root / f"{field_id}.materialization_support_receipt.json"
        )
        _write_json(panel_receipt_path, panel_receipt)
        panel_receipt_bindings[field_id] = {
            "receipt_hash": str(panel_receipt["receipt_hash"]),
            "receipt_sha256": _sha256(panel_receipt_path),
            "path": str(panel_receipt_path),
            "upstream_receipt_hash": str(upstream["receipt_hash"]),
        }
    coordinate_manifest = getattr(args, "coordinate_manifest", None)
    coordinate_rows: list[dict[str, Any]] = []
    coordinate_projection_status = "NOT_REQUESTED"
    if coordinate_manifest is not None:
        coordinate_rows = attach_coordinate_row_indices(
            _read_csv(coordinate_manifest), base
        )
        _write_csv(output_coordinates, coordinate_rows)
        coordinate_projection_status = "MATERIALIZED"
    else:
        output_coordinates.unlink(missing_ok=True)
    summary_paths = sorted(args.cache_root.glob("worker_*.summary.json"))
    artifacts = {
        "compact_panel": {"path": str(output_panel), "sha256": _sha256(output_panel)},
    }
    if coordinate_projection_status == "MATERIALIZED":
        artifacts["coordinate_manifest"] = {
            "path": str(output_coordinates),
            "sha256": _sha256(output_coordinates),
        }
    summary = {
        "status": "SESSION_SIGNAL_PANEL_ASSEMBLED",
        "panel_version": SESSION_PANEL_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_role": "development",
        "labels_or_returns_read": False,
        "validation_holdout_forward_read": False,
        "panel_rows": len(base),
        "panel_columns": len(base.columns),
        "canonical_fundamental_field_count": len(fields),
        "materialization_support_receipt_count": len(receipt_bindings),
        "cache_materialization_support_receipts": receipt_bindings,
        "materialization_support_receipts": panel_receipt_bindings,
        "coordinate_projection_status": coordinate_projection_status,
        "coordinate_count": len(coordinate_rows),
        "materialized_coordinate_count_by_set": dict(
            Counter(
                row["coordinate_set"]
                for row in coordinate_rows
                if int(row["row_index"]) >= 0
            )
        ),
        "worker_summary_sha256": {path.name: _sha256(path) for path in summary_paths},
        "artifacts": artifacts,
    }
    _write_json(args.output_root / "session_signal_panel_manifest.json", summary)
    _write_json(
        args.output_root / "development_only_access_ledger.json",
        {
            "ledger_version": "cn_compositional_session_sketch_access_v1",
            "requested_role": "development",
            "labels_or_returns_read": False,
            "validation_rows_read": 0,
            "holdout_rows_read": 0,
            "forward_2026_rows_read": 0,
            "source_release": str(args.source_release),
            "fundamental_root": str(args.fundamental_root),
            "maximum_observable_time": args.maximum_observable_time,
        },
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command", required=True)
    sidecar_base = sub.add_parser(
        "prepare-sidecar-base",
        help="build the full-development coordinate base from stock-session sidecars",
    )
    sidecar_base.add_argument("--session-sidecar-root", type=Path, required=True)
    sidecar_base.add_argument("--split-manifest", type=Path, required=True)
    sidecar_base.add_argument("--expected-shard-count", type=int, default=16)
    sidecar_base.add_argument("--output-root", type=Path, required=True)
    sidecar_base.set_defaults(func=prepare_sidecar_base)

    prep = sub.add_parser("prepare")
    prep.add_argument("--shard-root", type=Path, required=True)
    prep.add_argument("--split-manifest", type=Path, required=True)
    prep.add_argument("--stock-selection", type=Path, required=True)
    prep.add_argument("--active-coordinate-manifest", type=Path, required=True)
    prep.add_argument("--session-context-fields", type=Path, required=True)
    prep.add_argument("--chip-root", type=Path)
    prep.add_argument("--maximum-observable-time")
    prep.add_argument("--output-root", type=Path, required=True)
    prep.set_defaults(func=prepare)

    material = sub.add_parser("materialize")
    material.add_argument("--base-panel", type=Path, required=True)
    material.add_argument("--split-manifest", type=Path, required=True)
    material.add_argument("--registry", type=Path, required=True)
    material.add_argument("--input-manifest", type=Path, required=True)
    material.add_argument("--fundamental-root", type=Path, required=True)
    material.add_argument("--fundamental-manifest", type=Path, required=True)
    material.add_argument("--maximum-observable-time", required=True)
    material.add_argument("--cache-root", type=Path, required=True)
    material.add_argument("--partition-index", type=int, required=True)
    material.add_argument("--partition-count", type=int, required=True)
    material.set_defaults(func=materialize)

    assemble_parser = sub.add_parser("assemble")
    assemble_parser.add_argument("--base-panel", type=Path, required=True)
    assemble_parser.add_argument(
        "--coordinate-manifest",
        type=Path,
        help="optional sketch-coordinate projection; the base panel remains authority",
    )
    assemble_parser.add_argument("--input-manifest", type=Path, required=True)
    assemble_parser.add_argument("--cache-root", type=Path, required=True)
    assemble_parser.add_argument("--output-root", type=Path, required=True)
    assemble_parser.add_argument("--source-release", type=Path, required=True)
    assemble_parser.add_argument("--fundamental-root", type=Path, required=True)
    assemble_parser.add_argument("--fundamental-manifest", type=Path, required=True)
    assemble_parser.add_argument("--split-manifest", type=Path, required=True)
    assemble_parser.add_argument("--maximum-observable-time", required=True)
    assemble_parser.set_defaults(func=assemble)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
