"""Build a physical development-only true1min release from the approved mixed 2024-2025 source."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import traceback
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from our_system_phase2.services.atomic_checkpoint import atomic_write_json
from our_system_phase2.services.development_only_data_access import (
    DEVELOPMENT_ROLES,
    PANEL_RELATIVE_PATH,
    RELEASE_MANIFEST_VERSION,
    canonical_json_hash,
    release_hash,
    schema_hash,
    sha256_file,
    split_roles,
    validate_development_release,
)


def _table_hash(table: pa.Table) -> str:
    digest = hashlib.sha256()
    digest.update(schema_hash(table.schema).encode("ascii"))
    digest.update(str(len(table)).encode("ascii"))
    for field in table.schema:
        values = table[field.name].combine_chunks()
        one_column = pa.Table.from_arrays([values], schema=pa.schema([field]))
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, one_column.schema) as writer:
            writer.write_table(one_column, max_chunksize=max(1, len(one_column)))
        digest.update(sink.getvalue().to_pybytes())
    return digest.hexdigest()


def _date_values(table: pa.Table) -> list[str]:
    days = pc.floor_temporal(table["trade_time"].combine_chunks(), unit="day")
    return sorted(str(pd.Timestamp(value).date()) for value in pc.unique(days).to_pylist())


def _source_preflight(paths: list[Path], roles: dict[pd.Timestamp, str]) -> pa.Schema:
    if not paths:
        raise FileNotFoundError("approved source release contains no true1min panels")
    allowed_years = {date.year for date in roles}
    if not allowed_years.issubset({2024, 2025}):
        raise PermissionError("release builder refuses a split manifest outside 2024-2025")
    schema: pa.Schema | None = None
    for path in paths:
        parquet = pq.ParquetFile(path)
        if schema is None:
            schema = parquet.schema_arrow
        elif not schema.equals(parquet.schema_arrow, check_metadata=True):
            raise ValueError(f"source schema drift: {path}")
        index = parquet.schema_arrow.names.index("trade_time")
        for row_group_id in range(parquet.metadata.num_row_groups):
            statistics = parquet.metadata.row_group(row_group_id).column(index).statistics
            if statistics is None or not statistics.has_min_max:
                raise ValueError(f"source lacks trade_time metadata: {path}#{row_group_id}")
            if pd.Timestamp(statistics.min).year < 2024 or pd.Timestamp(statistics.max).year > 2025:
                raise PermissionError(f"source row group escapes approved 2024-2025 window: {path}#{row_group_id}")
    assert schema is not None
    return schema


def _build_shard(
    source: Path,
    destination: Path,
    *,
    allowed_dates: set[pd.Timestamp],
    output_root: Path,
    build_provenance: dict[str, Any],
) -> dict[str, Any]:
    relative = destination.relative_to(output_root).as_posix()
    checkpoint = output_root / "build_checkpoints" / f"{destination.parents[2].name}.json"
    if checkpoint.exists() and destination.exists():
        record = json.loads(checkpoint.read_text(encoding="utf-8"))
        if (
            record.get("sha256") == sha256_file(destination)
            and record.get("relative_path") == relative
            and record.get("build_provenance") == build_provenance
            and record.get("source_path") == str(source.resolve())
            and int(record.get("source_size", -1)) == source.stat().st_size
            and int(record.get("source_mtime_ns", -1)) == source.stat().st_mtime_ns
        ):
            return record

    destination.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".partial", dir=destination.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    temporary.unlink(missing_ok=True)
    parquet = pq.ParquetFile(source)
    allowed_array = pa.array(
        [date.to_datetime64() for date in sorted(allowed_dates)], type=pa.timestamp("ns")
    )
    writer: pq.ParquetWriter | None = None
    row_groups: list[dict[str, Any]] = []
    symbols: set[str] = set()
    dates: set[str] = set()
    source_rows = 0
    development_rows = 0
    excluded_rows = 0
    started = time.perf_counter()
    try:
        writer = pq.ParquetWriter(
            temporary,
            parquet.schema_arrow,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
            version="2.6",
        )
        for source_row_group_id in range(parquet.metadata.num_row_groups):
            table = parquet.read_row_group(source_row_group_id)
            source_rows += len(table)
            days = pc.floor_temporal(table["trade_time"].combine_chunks(), unit="day")
            mask = pc.is_in(days, value_set=allowed_array)
            filtered = table.filter(mask)
            excluded_rows += len(table) - len(filtered)
            if not len(filtered):
                continue
            date_values = _date_values(filtered)
            if any(pd.Timestamp(value) not in allowed_dates for value in date_values):
                raise PermissionError("builder selected a forbidden date")
            content_hash = _table_hash(filtered)
            writer.write_table(filtered, row_group_size=len(filtered))
            output_row_group_id = len(row_groups)
            row_groups.append(
                {
                    "row_group_id": output_row_group_id,
                    "source_row_group_id": source_row_group_id,
                    "rows": len(filtered),
                    "min_trade_date": date_values[0],
                    "max_trade_date": date_values[-1],
                    "date_count": len(date_values),
                    "date_set_sha256": canonical_json_hash(date_values),
                    "source_train_subset_content_sha256": content_hash,
                    "output_content_sha256": content_hash,
                    "data_role": "development",
                }
            )
            development_rows += len(filtered)
            dates.update(date_values)
            symbols.update(str(value) for value in pc.unique(filtered["code"].combine_chunks()).to_pylist())
        writer.close()
        writer = None

        output = pq.ParquetFile(temporary)
        try:
            if not parquet.schema_arrow.equals(output.schema_arrow, check_metadata=True):
                raise ValueError(f"output schema drift: {relative}")
            if output.metadata.num_row_groups != len(row_groups):
                raise ValueError(f"output row-group count drift: {relative}")
            for row_group_id, record in enumerate(row_groups):
                observed = _table_hash(output.read_row_group(row_group_id))
                if observed != record["source_train_subset_content_sha256"]:
                    raise ValueError(f"source/output train subset mismatch: {relative}#{row_group_id}")
                record["output_content_sha256"] = observed
        finally:
            output.close()
        os.replace(temporary, destination)
    finally:
        if writer is not None:
            writer.close()
        parquet.close()
        temporary.unlink(missing_ok=True)

    stat = destination.stat()
    record = {
        "relative_path": relative,
        "source_path": str(source.resolve()),
        "source_size": source.stat().st_size,
        "source_mtime_ns": source.stat().st_mtime_ns,
        "sha256": sha256_file(destination),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "rows": development_rows,
        "source_rows": source_rows,
        "excluded_non_development_rows": excluded_rows,
        "symbol_count": len(symbols),
        "date_count": len(dates),
        "min_trade_date": min(dates),
        "max_trade_date": max(dates),
        "date_set_sha256": canonical_json_hash(sorted(dates)),
        "row_groups": row_groups,
        "data_role": "development",
        "build_seconds": round(time.perf_counter() - started, 3),
        "build_provenance": build_provenance,
    }
    atomic_write_json(checkpoint, record)
    return record


def build_release(
    source_root: Path,
    output_root: Path,
    split_manifest: Path,
    source_release_manifest: Path,
    *,
    release_id: str,
    expected_source_release_manifest_sha256: str,
    expected_shards: int = 16,
) -> dict[str, Any]:
    source_root = source_root.resolve()
    output_root = output_root.resolve()
    split_manifest = split_manifest.resolve()
    source_release_manifest = source_release_manifest.resolve()
    paths = sorted(source_root.glob(f"shard_*/{PANEL_RELATIVE_PATH.as_posix()}"))
    if len(paths) != expected_shards:
        raise ValueError(f"expected {expected_shards} source shards, observed {len(paths)}")
    source_release_manifest_hash = sha256_file(source_release_manifest)
    if source_release_manifest_hash != expected_source_release_manifest_sha256:
        raise ValueError("source release manifest hash is not the approved frozen hash")
    source_release = json.loads(source_release_manifest.read_text(encoding="utf-8"))
    if Path(source_release.get("output_root", "")).resolve() != source_root:
        raise PermissionError("source release manifest is not bound to the requested approved source root")
    if int(source_release.get("shard_count", -1)) != expected_shards:
        raise ValueError("source release manifest shard count mismatch")
    declared_panel_rel = str(source_release.get("panel_rel", "")).replace("\\", "/")
    if declared_panel_rel != PANEL_RELATIVE_PATH.as_posix():
        raise ValueError("source release manifest panel path mismatch")
    required_hard_rules = {
        "ctx_* sidecars are previous-available only via source_date < exec_date",
        "evt_uplimit_* sidecars are same-day but hidden until trade_time >= cutoff minute",
        "original shard root is not modified",
    }
    if not required_hard_rules.issubset(set(source_release.get("hard_rules", []))):
        raise ValueError("source release manifest lacks required PIT/source-lag hard rules")
    source_file_manifest = Path(source_release.get("manifest", "")).resolve()
    if not source_file_manifest.is_file():
        raise FileNotFoundError("source release file manifest is missing")
    source_files = pd.read_csv(source_file_manifest)
    if "output_panel" not in source_files.columns:
        raise ValueError("source release file manifest lacks output_panel")
    declared_source_paths = {
        str(Path(value).resolve()) for value in source_files["output_panel"].astype(str)
    }
    observed_source_paths = {str(path.resolve()) for path in paths}
    if declared_source_paths != observed_source_paths:
        raise PermissionError("source release file manifest does not match the requested source panels")
    roles = split_roles(split_manifest)
    allowed_dates = {date for date, role in roles.items() if role in DEVELOPMENT_ROLES}
    if not allowed_dates:
        raise ValueError("split manifest contains no development dates")
    source_schema = _source_preflight(paths, roles)
    source_file_manifest_hash = sha256_file(source_file_manifest)
    split_manifest_hash = sha256_file(split_manifest)
    allowed_dates_hash = canonical_json_hash(sorted(str(date.date()) for date in allowed_dates))
    build_provenance = {
        "source_release_manifest_sha256": source_release_manifest_hash,
        "source_file_manifest_sha256": source_file_manifest_hash,
        "split_manifest_sha256": split_manifest_hash,
        "allowed_dates_sha256": allowed_dates_hash,
        "schema_sha256": schema_hash(source_schema),
    }
    output_root.mkdir(parents=True, exist_ok=True)
    records = []
    started_at = datetime.now(timezone.utc)
    for source in paths:
        shard = source.parents[2].name
        destination = output_root / shard / PANEL_RELATIVE_PATH
        records.append(
            _build_shard(
                source,
                destination,
                allowed_dates=allowed_dates,
                output_root=output_root,
                build_provenance=build_provenance,
            )
        )

    all_dates = sorted(str(date.date()) for date in allowed_dates)
    manifest: dict[str, Any] = {
        "manifest_version": RELEASE_MANIFEST_VERSION,
        "release_id": release_id,
        "release_root": str(output_root),
        "data_role": "development",
        "created_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "source_root": str(source_root),
        "source_release_manifest": str(source_release_manifest),
        "source_release_manifest_sha256": source_release_manifest_hash,
        "source_file_manifest": str(source_file_manifest),
        "source_file_manifest_sha256": source_file_manifest_hash,
        "split_manifest_path": str(split_manifest),
        "split_manifest_sha256": split_manifest_hash,
        "schema_sha256": schema_hash(source_schema),
        "schema_field_count": len(source_schema),
        "allowed_dates": {
            "count": len(all_dates),
            "min": all_dates[0],
            "max": all_dates[-1],
            "sha256": canonical_json_hash(all_dates),
        },
        "file_count": len(records),
        "files": records,
        "totals": {
            "rows": sum(int(row["rows"]) for row in records),
            "source_rows": sum(int(row["source_rows"]) for row in records),
            "excluded_non_development_rows": sum(int(row["excluded_non_development_rows"]) for row in records),
            "row_groups": sum(len(row["row_groups"]) for row in records),
        },
        "consistency_audit": {
            "method": "full_row_hash_per_output_row_group",
            "source_train_subset_equals_output": all(
                group["source_train_subset_content_sha256"] == group["output_content_sha256"]
                for row in records
                for group in row["row_groups"]
            ),
        },
        "forbidden_roles_present": [],
        "forward_2026_present": False,
    }
    manifest["release_hash"] = release_hash(manifest)
    manifest_path = output_root / "development_only_release_manifest.json"
    atomic_write_json(manifest_path, manifest)
    validate_development_release(
        output_root,
        manifest_path,
        split_manifest,
        expected_release_hash=manifest["release_hash"],
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--source-release-manifest", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--expected-source-release-manifest-sha256", required=True)
    parser.add_argument("--expected-shards", type=int, default=16)
    args = parser.parse_args(argv)
    started = datetime.now(timezone.utc)
    record_path = args.output_root.resolve() / "release_build_run_manifest.json"
    record: dict[str, Any] = {
        "experiment_id": args.release_id,
        "objective": "build and verify a physically isolated development-only true1min release",
        "status": "RUNNING",
        "mode": "heavy_release_build",
        "started_at": started.isoformat(),
        "inputs": {
            "source_root": str(args.source_root.resolve()),
            "source_release_manifest": str(args.source_release_manifest.resolve()),
            "source_release_manifest_sha256": sha256_file(args.source_release_manifest),
            "split_manifest": str(args.split_manifest.resolve()),
            "split_manifest_sha256": sha256_file(args.split_manifest),
        },
        "parameters": {"expected_shards": args.expected_shards, "data_role": "development"},
        "commands": [
            "python app.py build-development-only-true1min-release -- "
            f'--source-root "{args.source_root}" --output-root "{args.output_root}" '
            f'--split-manifest "{args.split_manifest}" '
            f'--source-release-manifest "{args.source_release_manifest}" '
            f'--expected-source-release-manifest-sha256 {args.expected_source_release_manifest_sha256} '
            f'--release-id {args.release_id} --expected-shards {args.expected_shards}'
        ],
        "estimated_runtime": "60-180 minutes on one local worker",
        "outputs": [],
        "reproducibility": "PENDING",
        "continuation": "resume only through provenance-bound per-shard checkpoints; never reuse invalidated CANARY artifacts",
        "failure": None,
    }
    args.output_root.mkdir(parents=True, exist_ok=True)
    atomic_write_json(record_path, record)
    timer = time.perf_counter()
    try:
        manifest = build_release(
            args.source_root,
            args.output_root,
            args.split_manifest,
            args.source_release_manifest,
            release_id=args.release_id,
            expected_source_release_manifest_sha256=args.expected_source_release_manifest_sha256,
            expected_shards=args.expected_shards,
        )
        release_manifest_path = args.output_root.resolve() / "development_only_release_manifest.json"
        record.update(
            {
                "status": "COMPLETED",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_runtime_seconds": round(time.perf_counter() - timer, 3),
                "outputs": [
                    {
                        "path": str(release_manifest_path),
                        "sha256": sha256_file(release_manifest_path),
                        "purpose": "final development-only release manifest",
                        "stage": "final",
                    }
                ],
                "release_hash": manifest["release_hash"],
                "totals": manifest["totals"],
                "reproducibility": "YES_WITH_PROVENANCE_BOUND_CHECKPOINTS",
                "decision": "DEVELOPMENT_ONLY_RELEASE_READY",
            }
        )
        atomic_write_json(record_path, record)
        print(json.dumps({"release_hash": manifest["release_hash"], "totals": manifest["totals"]}, indent=2))
        return 0
    except Exception as exc:
        record.update(
            {
                "status": "FAILED",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_runtime_seconds": round(time.perf_counter() - timer, 3),
                "reproducibility": "PARTIAL",
                "failure": {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                },
                "decision": "FAIL",
            }
        )
        atomic_write_json(record_path, record)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
