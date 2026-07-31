"""Build label-free candidate field compatibility for stock-session replay."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pyarrow.parquet as pq

from our_system_phase2.services.chip_sidecar import CHIP_FIELDS
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _required_fields(path: Path) -> tuple[str, ...]:
    fields: set[str] = set()
    path = Path(path).resolve()
    if path.suffix.lower() == ".parquet":
        schema = set(pq.ParquetFile(path).schema_arrow.names)
        expression_column = next(
            (
                column
                for column in ("expression", "canonical_expression")
                if column in schema
            ),
            None,
        )
        if expression_column is None:
            raise RuntimeError(
                "candidate parquet has no expression authority column"
            )
        expressions = pq.read_table(
            path, columns=[expression_column]
        ).column(expression_column).to_pylist()
    else:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            expressions = [
                str(
                    row.get("expression")
                    or row.get("canonical_expression")
                    or ""
                )
                for row in csv.DictReader(handle)
            ]
    for expression in expressions:
        fields.update(
            re.findall(
                r"\$([A-Za-z_][A-Za-z0-9_]*)",
                str(expression or ""),
            )
        )
    return tuple(sorted(fields))


def _direct_field_variation(
    *,
    sources: tuple[Path, ...],
    fields: tuple[str, ...],
    eligible_dates: tuple[str, ...],
) -> tuple[dict[str, int], dict[str, int], int]:
    maxima = {field: 0 for field in fields}
    shard_coverage = {field: 0 for field in fields}
    scanned_rows = 0
    date_values = tuple(pd.to_datetime(eligible_dates).date)
    for source in sources:
        schema = set(pq.ParquetFile(source).schema_arrow.names)
        present = tuple(field for field in fields if field in schema)
        for field in present:
            shard_coverage[field] += 1
        if not present:
            continue
        lazy = (
            pl.scan_parquet(source, low_memory=True)
            .filter(pl.col("trade_time").dt.date().is_in(date_values))
            .select("code", "trade_time", *present)
            .with_columns(pl.col("trade_time").dt.date().alias("__trade_date"))
            .group_by("code", "__trade_date")
            .agg(
                pl.len().alias("__rows"),
                *(
                    pl.col(field)
                    .drop_nulls()
                    .n_unique()
                    .alias(f"__nunique_{field}")
                    for field in present
                ),
            )
        )
        frame = lazy.collect(engine="streaming")
        scanned_rows += int(frame["__rows"].sum()) if len(frame) else 0
        for field in present:
            observed = int(frame[f"__nunique_{field}"].max() or 0)
            maxima[field] = max(maxima[field], observed)
    return maxima, shard_coverage, scanned_rows


def build_capability(
    *,
    source_root: Path,
    candidate_table: Path,
    registry_path: Path,
    split_manifest: Path,
    split_manifest_hash: str,
    max_shards: int,
) -> dict[str, Any]:
    split_path = Path(split_manifest).resolve()
    if _sha256(split_path) != str(split_manifest_hash):
        raise RuntimeError("split manifest hash drift")
    split = pd.read_csv(split_path, dtype=str)
    train = split.loc[split["split"] == "train"]
    if train.empty or not train["optimizer_usage"].eq("allowed").all():
        raise RuntimeError("train-only capability calendar is invalid")
    eligible_dates = tuple(sorted(train["trade_date"].tolist()))
    if max(eligible_dates) >= "2026-01-01":
        raise RuntimeError("capability preflight enters sealed 2026")
    fields = _required_fields(candidate_table)
    registry = UnifiedCapabilityRegistry.read(Path(registry_path).resolve())
    sources = tuple(
        sorted(Path(source_root).resolve().rglob("*.parquet"))[: int(max_shards)]
    )
    if not sources:
        raise FileNotFoundError("stock-session capability source shards missing")
    source_schemas = {
        field
        for source in sources
        for field in pq.ParquetFile(source).schema_arrow.names
    }
    chip_field_ids = set(CHIP_FIELDS.values())
    direct_fields = tuple(
        sorted(set(fields) & source_schemas - chip_field_ids)
    )
    variation, shard_coverage, scanned_rows = _direct_field_variation(
        sources=sources,
        fields=direct_fields,
        eligible_dates=eligible_dates,
    )
    rows: dict[str, dict[str, Any]] = {}
    for field_id in fields:
        try:
            capability = registry.resolve(field_id)
        except KeyError:
            rows[field_id] = {
                "status": "UNSUPPORTED",
                "reason": "REGISTRY_FIELD_MISSING",
                "source_kind": "MISSING",
                "source_shard_count": len(sources),
                "source_shard_coverage_count": int(
                    shard_coverage.get(field_id) or 0
                ),
                "maximum_within_session_unique": int(
                    variation.get(field_id) or 0
                ),
                "observable_clock": "UNKNOWN",
                "temporal_semantics": "UNKNOWN",
                "source_field_id": None,
                "representation_id": None,
            }
            continue
        representation = dict(
            (capability.metadata or {}).get("canonical_representation") or {}
        )
        if field_id in chip_field_ids:
            status = "SUPPORTED"
            reason = "PIT_CHIP_SESSION_MATERIALIZATION"
            source_kind = "CHIP_PIT"
        elif representation:
            status = "SUPPORTED"
            reason = "PIT_CANONICAL_SESSION_MATERIALIZATION"
            source_kind = "CANONICAL_REPRESENTATION"
        elif field_id not in source_schemas:
            status = "UNSUPPORTED"
            reason = "FIELD_NOT_MATERIALIZABLE_ON_SESSION_SIDECAR"
            source_kind = "MISSING"
        elif int(shard_coverage.get(field_id) or 0) != len(sources):
            status = "UNSUPPORTED"
            reason = "FIELD_SCHEMA_COVERAGE_INCOMPLETE"
            source_kind = "DIRECT_MINUTE_FIELD"
        elif int(variation.get(field_id) or 0) > 1:
            status = "UNSUPPORTED"
            reason = "VARIES_WITHIN_SESSION"
            source_kind = "DIRECT_MINUTE_FIELD"
        else:
            status = "SUPPORTED"
            reason = "DIRECT_FIELD_CONSTANT_WITHIN_SESSION"
            source_kind = "DIRECT_MINUTE_FIELD"
        rows[field_id] = {
            "status": status,
            "reason": reason,
            "source_kind": source_kind,
            "source_shard_count": len(sources),
            "source_shard_coverage_count": int(
                shard_coverage.get(field_id) or 0
            ),
            "maximum_within_session_unique": int(variation.get(field_id) or 0),
            "observable_clock": capability.observable_clock,
            "temporal_semantics": capability.temporal_semantics,
            "source_field_id": capability.source_field_id,
            "representation_id": capability.representation_id,
        }
    payload = {
        "schema_version": "cn_execution_field_capability_v1",
        "status": "ZERO_FINANCIAL_CAPABILITY_CLOSED",
        "execution_clock": "stock_session",
        "candidate_table": {
            "path": str(Path(candidate_table).resolve()),
            "sha256": _sha256(Path(candidate_table).resolve()),
        },
        "registry": {
            "path": str(Path(registry_path).resolve()),
            "sha256": _sha256(Path(registry_path).resolve()),
        },
        "split_manifest": {
            "path": str(split_path),
            "sha256": _sha256(split_path),
            "role": "train",
        },
        "source_shards": [
            {"path": str(path), "sha256": _sha256(path)} for path in sources
        ],
        "required_field_count": len(fields),
        "supported_field_count": sum(
            row["status"] == "SUPPORTED" for row in rows.values()
        ),
        "unsupported_field_count": sum(
            row["status"] != "SUPPORTED" for row in rows.values()
        ),
        "label_free_source_rows_scanned": scanned_rows,
        "fields": rows,
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_writes": 0,
        "feedback_writes": 0,
        "archive_writes": 0,
        "promotion_writes": 0,
    }
    payload["capability_manifest_sha256"] = _stable_hash(payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--candidate-table", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest-hash", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-shards", type=int, default=16)
    args = parser.parse_args()
    result = build_capability(
        source_root=args.source_root,
        candidate_table=args.candidate_table,
        registry_path=args.registry,
        split_manifest=args.split_manifest,
        split_manifest_hash=args.split_manifest_hash,
        max_shards=args.max_shards,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
