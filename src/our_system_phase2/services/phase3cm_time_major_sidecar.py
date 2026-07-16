"""Content-addressed time-major execution sidecars and full-key parity."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from datetime import date
from typing import Any, Sequence

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


SIDECAR_SCHEMA_VERSION = "cn_development_time_major_execution_layout_v2_train_only"
STABLE_KEY = (
    "trade_time",
    "code",
    "source_shard",
    "source_row_identity",
    "duplicate_ordinal",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _identity(
    source_sha: str,
    source_shard: int,
    fields: Sequence[str],
    eligible_trade_dates: Sequence[str] | None,
    split_manifest_hash: str | None,
) -> str:
    payload = {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "source_sha256": source_sha,
        "source_shard": int(source_shard),
        "fields": list(fields),
        "sort_key": list(STABLE_KEY),
        "eligible_trade_dates": list(eligible_trade_dates or ()),
        "split_manifest_hash": str(split_manifest_hash or ""),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def build_time_major_shard(
    *,
    source_path: Path,
    output_path: Path,
    source_shard: int,
    fields: Sequence[str],
    row_group_size: int = 262_144,
    eligible_trade_dates: Sequence[str] | None = None,
    split_manifest_hash: str | None = None,
) -> dict[str, Any]:
    source = Path(source_path)
    output = Path(output_path)
    parquet = pq.ParquetFile(source)
    selected = tuple(dict.fromkeys(str(field) for field in fields))
    missing = sorted(set(selected) - set(parquet.schema_arrow.names))
    if missing:
        raise ValueError(f"source shard is missing sidecar fields: {missing}")
    if not {"trade_time", "code"} <= set(selected):
        raise ValueError("time-major sidecar requires trade_time and code")
    source_sha = _sha256(source)
    eligible = tuple(sorted({date.fromisoformat(str(value)) for value in eligible_trade_dates or ()}))
    eligible_strings = tuple(value.isoformat() for value in eligible)
    identity = _identity(source_sha, source_shard, selected, eligible_strings, split_manifest_hash)
    started = time.perf_counter()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp.parquet")
    lazy = pl.scan_parquet(source, row_index_name="source_row_identity", rechunk=False, low_memory=True)
    if eligible:
        lazy = lazy.filter(pl.col("trade_time").dt.date().is_in(eligible))
    lazy = (
        lazy
        .select(
            *[pl.col(field) for field in selected],
            pl.lit(int(source_shard), dtype=pl.UInt16).alias("source_shard"),
            pl.col("source_row_identity").cast(pl.UInt64),
        )
        .sort(["trade_time", "code", "source_row_identity"], maintain_order=True)
        .with_columns(
            (pl.col("source_row_identity").cum_count().over(["trade_time", "code"]) - 1)
            .cast(pl.UInt32)
            .alias("duplicate_ordinal")
        )
        .select(*STABLE_KEY, *[field for field in selected if field not in {"trade_time", "code"}])
    )
    lazy.sink_parquet(
        temporary,
        compression="zstd",
        compression_level=3,
        statistics=True,
        row_group_size=int(row_group_size),
        maintain_order=True,
        mkdir=True,
        engine="streaming",
    )
    temporary.replace(output)
    elapsed = time.perf_counter() - started
    result = {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "sidecar_identity": identity,
        "source_shard": int(source_shard),
        "source_path": str(source),
        "source_sha256": source_sha,
        "source_bytes": source.stat().st_size,
        "source_total_rows": parquet.metadata.num_rows,
        "output_path": str(output),
        "output_sha256": _sha256(output),
        "output_bytes": output.stat().st_size,
        "rows": pq.ParquetFile(output).metadata.num_rows,
        "fields": list(selected),
        "eligible_trade_date_count": len(eligible_strings),
        "split_manifest_hash": str(split_manifest_hash or ""),
        "stable_key": list(STABLE_KEY),
        "wall_seconds": elapsed,
        "status": "TIME_MAJOR_SHARD_READY",
    }
    result["source_rows"] = result["rows"]
    return result


def _digest(lazy: pl.LazyFrame, columns: Sequence[str], *, prefix: str) -> dict[str, int]:
    expressions = []
    for seed in (0, 0x9E3779B9):
        hashed = pl.struct([pl.col(column) for column in columns]).hash(seed=seed)
        expressions.append(hashed.sum().alias(f"{prefix}_{seed}"))
    row = lazy.select(expressions).collect(engine="streaming").row(0, named=True)
    return {key: int(value or 0) for key, value in row.items()}


def _null_digest(lazy: pl.LazyFrame, fields: Sequence[str], *, prefix: str) -> dict[str, Any]:
    null_names = [f"null__{field}" for field in fields]
    null_columns = [pl.col(field).is_null().alias(name) for field, name in zip(fields, null_names)]
    pattern = lazy.select(*STABLE_KEY, *null_columns)
    counts = lazy.select([pl.col(field).null_count().alias(field) for field in fields]).collect(engine="streaming").row(0, named=True)
    return {
        "counts": {key: int(value) for key, value in counts.items()},
        "digest": _digest(pattern, [*STABLE_KEY, *null_names], prefix=prefix),
    }


def _parity_aggregates(lazy: pl.LazyFrame, fields: Sequence[str]) -> dict[str, Any]:
    value_columns = [*STABLE_KEY, *[field for field in fields if field not in {"trade_time", "code"}]]
    null_struct = pl.struct(
        [pl.col(field).is_null().alias(f"null__{field}") for field in fields]
    )
    expressions: list[pl.Expr] = [
        pl.len().alias("row_count"),
        pl.struct(list(STABLE_KEY)).n_unique().alias("stable_key_unique_count"),
    ]
    for field in fields:
        expressions.append(pl.col(field).null_count().alias(f"null_count__{field}"))
    for seed in (0, 0x9E3779B9):
        expressions.extend(
            (
                pl.struct([pl.col(column) for column in STABLE_KEY]).hash(seed=seed).sum().alias(f"coordinate__{seed}"),
                pl.struct([pl.col(column) for column in value_columns]).hash(seed=seed).sum().alias(f"values__{seed}"),
                pl.struct([*[pl.col(column) for column in STABLE_KEY], null_struct]).hash(seed=seed).sum().alias(f"nulls__{seed}"),
            )
        )
    row = lazy.select(expressions).collect(engine="streaming").row(0, named=True)
    return {
        "row_count": int(row["row_count"]),
        "stable_key_unique_count": int(row["stable_key_unique_count"]),
        "coordinate_digest": {str(seed): int(row[f"coordinate__{seed}"] or 0) for seed in (0, 0x9E3779B9)},
        "field_value_digest": {str(seed): int(row[f"values__{seed}"] or 0) for seed in (0, 0x9E3779B9)},
        "null_bitmap": {
            "counts": {field: int(row[f"null_count__{field}"]) for field in fields},
            "digest": {str(seed): int(row[f"nulls__{seed}"] or 0) for seed in (0, 0x9E3779B9)},
        },
    }


def audit_sidecar_parity(
    *,
    source_path: Path,
    sidecar_path: Path,
    source_shard: int,
    fields: Sequence[str],
    eligible_trade_dates: Sequence[str] | None = None,
    split_manifest_hash: str | None = None,
) -> dict[str, Any]:
    source = Path(source_path)
    sidecar = Path(sidecar_path)
    selected = tuple(dict.fromkeys(str(field) for field in fields))
    eligible = tuple(sorted({date.fromisoformat(str(value)) for value in eligible_trade_dates or ()}))
    source_lazy = pl.scan_parquet(source, row_index_name="source_row_identity", rechunk=False, low_memory=True)
    if eligible:
        source_lazy = source_lazy.filter(pl.col("trade_time").dt.date().is_in(eligible))
    source_lazy = (
        source_lazy
        .select(
            *[pl.col(field) for field in selected],
            pl.lit(int(source_shard), dtype=pl.UInt16).alias("source_shard"),
            pl.col("source_row_identity").cast(pl.UInt64),
        )
        .sort(["trade_time", "code", "source_row_identity"], maintain_order=True)
        .with_columns(
            (pl.col("source_row_identity").cum_count().over(["trade_time", "code"]) - 1)
            .cast(pl.UInt32)
            .alias("duplicate_ordinal")
        )
        .select(*STABLE_KEY, *[field for field in selected if field not in {"trade_time", "code"}])
    )
    sidecar_lazy = pl.scan_parquet(sidecar, rechunk=False, low_memory=True).select(
        *STABLE_KEY, *[field for field in selected if field not in {"trade_time", "code"}]
    )
    source_aggregate = _parity_aggregates(source_lazy, selected)
    sidecar_aggregate = _parity_aggregates(sidecar_lazy, selected)
    source_count = int(source_aggregate["row_count"])
    sidecar_count = int(sidecar_aggregate["row_count"])
    unique_keys = int(sidecar_aggregate["stable_key_unique_count"])
    source_schema = pq.ParquetFile(source).schema_arrow
    sidecar_schema = pq.ParquetFile(sidecar).schema_arrow
    dtype_match = all(source_schema.field(field).type == sidecar_schema.field(field).type for field in selected)
    coordinate_source = source_aggregate["coordinate_digest"]
    coordinate_sidecar = sidecar_aggregate["coordinate_digest"]
    values_source = source_aggregate["field_value_digest"]
    values_sidecar = sidecar_aggregate["field_value_digest"]
    null_source = source_aggregate["null_bitmap"]
    null_sidecar = sidecar_aggregate["null_bitmap"]
    checks = {
        "row_count_match": source_count == sidecar_count,
        "stable_key_unique": unique_keys == sidecar_count,
        "duplicate_identity_match": coordinate_source == coordinate_sidecar,
        "missing_identity_match": coordinate_source == coordinate_sidecar and source_count == sidecar_count,
        "coordinate_digest_match": coordinate_source == coordinate_sidecar,
        "null_bitmap_match": null_source == null_sidecar,
        "dtype_match": dtype_match,
        "observable_time_match": coordinate_source == coordinate_sidecar,
        "field_value_digest_match": values_source == values_sidecar,
    }
    return {
        "schema_version": "cn_time_major_sidecar_parity_v2_train_only",
        "status": "SIDECAR_PARITY_PASS" if all(checks.values()) else "SIDECAR_PARITY_FAIL",
        "source_path": str(source),
        "sidecar_path": str(sidecar),
        "eligible_trade_date_count": len(eligible),
        "split_manifest_hash": str(split_manifest_hash or ""),
        "source_rows": source_count,
        "sidecar_rows": sidecar_count,
        "stable_key_unique_count": unique_keys,
        "source_coordinate_digest": coordinate_source,
        "sidecar_coordinate_digest": coordinate_sidecar,
        "source_null_bitmap": null_source,
        "sidecar_null_bitmap": null_sidecar,
        "source_field_value_digest": values_source,
        "sidecar_field_value_digest": values_sidecar,
        **checks,
    }
