from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pyarrow.parquet as pq

from our_system_phase2.services.chip_sidecar import (
    CHIP_FIELDS,
    load_chip_context,
    point_in_time_chip_context,
    validate_chip_context_role,
)
from our_system_phase2.services.fundamental_representations import (
    CanonicalFundamentalMaterializer,
)
from our_system_phase2.services.pit_fundamental_fabric import (
    PITFundamentalFabricAdapter,
    normalize_cn_code,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


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


def _required_fields(path: Path) -> tuple[str, ...]:
    fields: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            fields.update(
                re.findall(
                    r"\$([A-Za-z_][A-Za-z0-9_]*)",
                    str(row.get("expression") or ""),
                )
            )
    return tuple(sorted(fields))


def _resolve_fundamental_partition_root(
    root: Path,
    *,
    source_tables: set[str],
) -> Path:
    candidates = (root.resolve(), (root / "silver_partitioned").resolve())
    for candidate in candidates:
        if all((candidate / table).is_dir() for table in source_tables):
            return candidate
    missing = {
        str(candidate): sorted(
            table for table in source_tables if not (candidate / table).is_dir()
        )
        for candidate in candidates
    }
    raise FileNotFoundError(
        "fundamental partition root does not expose required source tables: "
        f"{missing}"
    )


def _assert_positive_pit_coverage(
    *,
    specs: dict[str, dict],
    records: list[dict],
) -> dict[str, float]:
    aggregate = {
        field_id: max(
            (float(row["pit_coverage"].get(field_id, 0.0)) for row in records),
            default=0.0,
        )
        for field_id in specs
    }
    empty = sorted(field_id for field_id, coverage in aggregate.items() if coverage <= 0.0)
    if empty:
        raise RuntimeError(
            "canonical PIT fields have zero coverage across all session shards: "
            f"{empty}"
        )
    return aggregate


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--candidate-table", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument(
        "--evaluation-role",
        choices=(
            "train",
            "validation",
            "holdout",
            "forward_2026",
            "historical_challenge",
        ),
        default="validation",
    )
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest-hash", required=True)
    parser.add_argument("--fundamental-root", type=Path, required=True)
    parser.add_argument("--chip-root", type=Path, required=True)
    parser.add_argument("--max-shards", type=int, default=16)
    args = parser.parse_args()

    split_manifest = args.split_manifest.resolve()
    if _sha256(split_manifest) != args.split_manifest_hash:
        raise RuntimeError("split manifest hash drift")
    split = pd.read_csv(split_manifest, dtype=str)
    role_rows = split.loc[split["split"] == args.evaluation_role]
    eligible_dates = tuple(sorted(role_rows["trade_date"].tolist()))
    expected_usage = (
        "allowed" if args.evaluation_role == "train" else "report_only"
    )
    if not eligible_dates or not role_rows["optimizer_usage"].eq(
        expected_usage
    ).all():
        raise RuntimeError(
            f"{args.evaluation_role} calendar usage is not {expected_usage}"
        )
    sessions = pd.DatetimeIndex(pd.to_datetime(eligible_dates))
    maximum_observable_time = str(sessions.max() + pd.Timedelta(hours=15))
    chip_data_role = (
        "forward_2026_report_only"
        if args.evaluation_role == "forward_2026"
        else (
            "historical_challenge_report_only"
            if args.evaluation_role == "historical_challenge"
            else "development"
        )
    )
    validate_chip_context_role(
        data_role=chip_data_role,
        maximum_observable_time=maximum_observable_time,
    )

    required_fields = _required_fields(args.candidate_table.resolve())
    chip_fields = tuple(
        sorted(set(required_fields) & set(CHIP_FIELDS.values()))
    )
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    specs: dict[str, dict] = {}
    prefetch: dict[str, set[str]] = {}
    for field_id in required_fields:
        capability = registry.resolve(field_id)
        spec = dict((capability.metadata or {}).get("canonical_representation") or {})
        if not spec:
            continue
        if not bool(spec.get("search_eligible")):
            raise PermissionError(f"field is not PIT search eligible: {field_id}")
        specs[field_id] = spec
        for source in spec.get("source_fields") or []:
            prefetch.setdefault(str(source["source_table"]), set()).add(
                str(source["source_field"])
            )

    fundamental_partition_root = _resolve_fundamental_partition_root(
        args.fundamental_root,
        source_tables=set(prefetch),
    )
    adapter = PITFundamentalFabricAdapter(
        source_root=fundamental_partition_root,
        sessions=sessions,
        maximum_observable_time=maximum_observable_time,
        prefetch_source_fields_by_table=prefetch,
    )
    materializer = CanonicalFundamentalMaterializer(adapter)
    sources = sorted(args.source_root.resolve().rglob("*.parquet"))[
        : int(args.max_shards)
    ]
    if len(sources) != int(args.max_shards):
        raise FileNotFoundError(
            f"expected exactly {args.max_shards} source shards"
        )

    allowed_codes = set(
        pl.concat(
            [
                pl.scan_parquet(source, low_memory=True)
                .filter(pl.col("trade_time").dt.date().is_in(tuple(sessions.date)))
                .select("code")
                for source in sources
            ]
        )
        .unique()
        .collect(engine="streaming")["code"]
        .to_list()
    )
    if chip_fields:
        chip_context, chip_receipt = load_chip_context(
            args.chip_root.resolve(),
            allowed_codes={normalize_cn_code(value) for value in allowed_codes},
            fields=chip_fields,
            maximum_observable_time=maximum_observable_time,
            data_role=chip_data_role,
        )
    else:
        chip_context = pd.DataFrame()
        chip_receipt = {
            "data_role": chip_data_role,
            "loaded_row_count": 0,
            "requested_fields": [],
            "source_read": "SKIPPED_NO_REQUIRED_CHIP_FIELDS",
        }

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    records = []
    started = time.perf_counter()
    for shard, source in enumerate(sources):
        source_schema = set(pq.ParquetFile(source).schema_arrow.names)
        direct_fields = sorted(
            (set(required_fields) - set(chip_fields)) & source_schema
        )
        missing = sorted(
            set(required_fields)
            - set(direct_fields)
            - set(specs)
            - set(chip_fields)
        )
        if missing:
            raise RuntimeError(f"unmaterializable session fields: {missing}")
        variation_exprs = [
            pl.col(field).drop_nulls().n_unique().alias(f"__nunique_{field}")
            for field in direct_fields
        ]
        value_exprs = [
            pl.col(field).sort_by("trade_time").last().alias(field)
            for field in ("close", *direct_fields)
        ]
        lazy = (
            pl.scan_parquet(source, low_memory=True)
            .filter(pl.col("trade_time").dt.date().is_in(tuple(sessions.date)))
            .select("code", "trade_time", "open", "close", *direct_fields)
            .with_columns(pl.col("trade_time").dt.date().alias("__trade_date"))
            .group_by("code", "__trade_date")
            .agg(
                pl.col("trade_time").max(),
                pl.col("open").sort_by("trade_time").first().alias("open"),
                *value_exprs,
                *variation_exprs,
            )
            .sort("trade_time", "code")
        )
        frame = lazy.collect(engine="streaming").to_pandas()
        direct_field_intraday_variation = {
            field: int(frame[f"__nunique_{field}"].max() or 0)
            for field in direct_fields
        }
        for field in direct_fields:
            frame.drop(columns=f"__nunique_{field}", inplace=True)
        frame.drop(columns="__trade_date", inplace=True)
        frame["code"] = frame["code"].map(normalize_cn_code)
        coordinates = frame[["code", "trade_time"]].rename(
            columns={"trade_time": "session_time"}
        )
        coordinate_index = pd.MultiIndex.from_frame(coordinates)
        coverage: dict[str, float] = {}
        for field_id, spec in specs.items():
            values = materializer.materialize(spec, coordinates).set_index(
                ["code", "session_time"]
            )[field_id]
            if values.index.has_duplicates:
                raise RuntimeError(f"duplicate PIT coordinates: {field_id}")
            frame[field_id] = values.reindex(coordinate_index).to_numpy()
            coverage[field_id] = round(float(frame[field_id].notna().mean()), 8)
        if chip_fields:
            joined = point_in_time_chip_context(
                frame[["code", "trade_time"]],
                chip_context,
                fields=chip_fields,
                data_role=chip_data_role,
            )
            frame["chip_source_session"] = joined[
                "chip_source_session"
            ].to_numpy()
            for field_id in chip_fields:
                frame[field_id] = joined[field_id].to_numpy()
                coverage[field_id] = round(
                    float(frame[field_id].notna().mean()), 8
                )
        frame["source_shard"] = np.uint16(shard)
        frame["source_row_identity"] = np.arange(len(frame), dtype=np.uint64)
        frame["duplicate_ordinal"] = np.uint32(0)
        output_fields = list(
            dict.fromkeys(
                [
                    *STABLE_KEY,
                    "open",
                    "close",
                    *required_fields,
                    *(["chip_source_session"] if chip_fields else []),
                ]
            )
        )
        frame = frame[output_fields]
        if frame.duplicated(list(STABLE_KEY)).any():
            raise RuntimeError(f"duplicate stable keys in shard {shard}")
        target = output_root / f"shard_{shard:02d}.parquet"
        temporary = target.with_suffix(".tmp.parquet")
        frame.to_parquet(temporary, index=False, compression="zstd")
        temporary.replace(target)
        records.append(
            {
                "source_shard": shard,
                "source_path": str(source),
                "source_sha256": _sha256(source),
                "output_path": str(target),
                "output_sha256": _sha256(target),
                "output_bytes": target.stat().st_size,
                "rows": len(frame),
                "fields": output_fields,
                "pit_coverage": coverage,
                "direct_field_intraday_variation": (
                    direct_field_intraday_variation
                ),
                "direct_field_materialization_policy": (
                    "LAST_OBSERVED_VALUE_AT_OR_BEFORE_SESSION_CLOSE_PIT"
                ),
                "status": (
                    f"SESSION_{args.evaluation_role.upper()}_PIT_MATERIALIZATION_PASS"
                ),
            }
        )
        print(json.dumps({"shard": shard, "rows": len(frame), "status": "PASS"}))

    aggregate_pit_coverage = _assert_positive_pit_coverage(
        specs=specs,
        records=records,
    )
    manifest = {
        "schema_version": "cn_core_pack_report_only_session_sidecar_v3",
        "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
        "data_role": (
            "development_train_only"
            if args.evaluation_role == "train"
            else f"{args.evaluation_role}_report_only"
        ),
        "evaluation_role": args.evaluation_role,
        "split_manifest_hash": args.split_manifest_hash,
        "eligible_trade_date_count": len(eligible_dates),
        "eligible_train_date_count": (
            len(eligible_dates) if args.evaluation_role == "train" else 0
        ),
        "eligible_validation_date_count": (
            len(eligible_dates) if args.evaluation_role == "validation" else 0
        ),
        "eligible_holdout_date_count": (
            len(eligible_dates) if args.evaluation_role == "holdout" else 0
        ),
        "eligible_forward_2026_date_count": (
            len(eligible_dates)
            if args.evaluation_role == "forward_2026"
            else 0
        ),
        "eligible_historical_challenge_date_count": (
            len(eligible_dates)
            if args.evaluation_role == "historical_challenge"
            else 0
        ),
        "fields": records[0]["fields"],
        "direct_minute_field_policy": (
            "LAST_OBSERVED_VALUE_AT_OR_BEFORE_SESSION_CLOSE_PIT"
        ),
        "direct_field_intraday_variation_max": {
            field: max(
                int(
                    (row.get("direct_field_intraday_variation") or {}).get(
                        field, 0
                    )
                )
                for row in records
            )
            for field in sorted(
                {
                    field
                    for row in records
                    for field in (
                        row.get("direct_field_intraday_variation") or {}
                    )
                }
            )
        },
        "fundamental_partition_root": str(fundamental_partition_root),
        "canonical_pit_coverage": aggregate_pit_coverage,
        "source_shard_count": len(records),
        "source_rows": sum(int(row["rows"]) for row in records),
        "sidecar_rows": sum(int(row["rows"]) for row in records),
        "sidecar_bytes": sum(int(row["output_bytes"]) for row in records),
        "build_wall_seconds": time.perf_counter() - started,
        "shards": records,
        "chip_receipt": chip_receipt,
        "validation_reads": (
            sum(int(row["rows"]) for row in records)
            if args.evaluation_role == "validation"
            else 0
        ),
        "holdout_reads": (
            sum(int(row["rows"]) for row in records)
            if args.evaluation_role == "holdout"
            else 0
        ),
        "forward_2026_reads": (
            sum(int(row["rows"]) for row in records)
            if args.evaluation_role == "forward_2026"
            else 0
        ),
        "historical_challenge_reads": (
            sum(int(row["rows"]) for row in records)
            if args.evaluation_role == "historical_challenge"
            else 0
        ),
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    manifest_path = output_root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"status": manifest["status"], "rows": manifest["sidecar_rows"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
