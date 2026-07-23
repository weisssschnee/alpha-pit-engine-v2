from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from pathlib import Path

from our_system_phase2.services.phase3cm_streaming_telemetry import _process_snapshot
from our_system_phase2.services.phase3cm_time_major_sidecar import (
    audit_sidecar_parity,
    build_time_major_shard,
)


REQUIRED_THREAD_ENV = {
    "NUMBA_NUM_THREADS": "1",
    "ARROW_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "NUMEXPR_MAX_THREADS": "1",
}


def _candidate_fields(paths: list[Path]) -> tuple[str, ...]:
    fields = {"code", "trade_time", "close"}
    for path in paths:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                fields.update(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", str(row.get("expression") or "")))
    return tuple(sorted(fields))


def _split_dates(
    path: Path,
    expected_sha256: str,
    *,
    evaluation_role: str,
) -> tuple[str, ...]:
    if evaluation_role not in {"train", "validation", "holdout"}:
        raise ValueError(f"unsupported evaluation role: {evaluation_role}")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != str(expected_sha256):
        raise RuntimeError("split manifest hash drift")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]
    dates = tuple(
        str(row["trade_date"])
        for row in rows
        if str(row.get("split")) == evaluation_role
    )
    if not dates or len(dates) != len(set(dates)):
        raise RuntimeError(
            f"split manifest {evaluation_role} calendar is empty or duplicated"
        )
    expected_usage = "allowed" if evaluation_role == "train" else "report_only"
    if any(
        str(row.get("optimizer_usage") or "") != expected_usage
        for row in rows
        if str(row.get("split")) == evaluation_role
    ):
        raise RuntimeError(f"split manifest {evaluation_role} usage drift")
    return dates


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument(
        "--evaluation-role",
        choices=("train", "validation", "holdout"),
        default="train",
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--candidate-table", type=Path, action="append", required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest-hash", required=True)
    parser.add_argument("--max-shards", type=int, default=16)
    parser.add_argument("--row-group-size", type=int, default=262_144)
    parser.add_argument("--polars-threads", type=int, required=True)
    parser.add_argument("--parity", action="store_true")
    args = parser.parse_args()
    expected = {**REQUIRED_THREAD_ENV, "POLARS_MAX_THREADS": str(args.polars_threads)}
    observed = {name: os.environ.get(name) for name in expected}
    if observed != expected:
        raise RuntimeError(f"thread environment is not frozen: observed={observed} expected={expected}")
    if int(args.polars_threads) < 1 or int(args.polars_threads) > 24:
        raise ValueError("POLARS_MAX_THREADS must be between 1 and 24")

    source_files = sorted(args.source_root.resolve().rglob("*.parquet"))[: int(args.max_shards)]
    if not source_files:
        raise FileNotFoundError("no source parquet shards found")
    fields = _candidate_fields([path.resolve() for path in args.candidate_table])
    eligible_dates = _split_dates(
        args.split_manifest.resolve(),
        args.split_manifest_hash,
        evaluation_role=args.evaluation_role,
    )
    root = args.output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    records = []
    parity_records = []
    for source_shard, source in enumerate(source_files):
        output = root / f"shard_{source_shard:02d}.parquet"
        before = _process_snapshot()
        record = build_time_major_shard(
            source_path=source,
            output_path=output,
            source_shard=source_shard,
            fields=fields,
            row_group_size=int(args.row_group_size),
            eligible_trade_dates=eligible_dates,
            split_manifest_hash=args.split_manifest_hash,
        )
        after = _process_snapshot()
        record.update(
            {
                "cpu_seconds": max(0.0, after.cpu_seconds - before.cpu_seconds),
                "effective_cores": (
                    max(0.0, after.cpu_seconds - before.cpu_seconds) / record["wall_seconds"]
                    if record["wall_seconds"] > 0
                    else 0.0
                ),
                "peak_rss_bytes": max(before.peak_rss_bytes, after.peak_rss_bytes),
                "read_transfer_bytes": max(0, after.bytes_read - before.bytes_read),
            }
        )
        records.append(record)
        if args.parity:
            parity_records.append(
                audit_sidecar_parity(
                    source_path=source,
                    sidecar_path=output,
                    source_shard=source_shard,
                    fields=fields,
                    eligible_trade_dates=eligible_dates,
                    split_manifest_hash=args.split_manifest_hash,
                )
            )
    manifest = {
        "schema_version": "cn_development_time_major_execution_layout_manifest_v3_split_role",
        "status": (
            "TIME_MAJOR_LAYOUT_PARITY_PASS"
            if args.parity and all(row["status"] == "SIDECAR_PARITY_PASS" for row in parity_records)
            else "TIME_MAJOR_LAYOUT_READY_PARITY_PENDING"
        ),
        "data_role": (
            "development_train_only"
            if args.evaluation_role == "train"
            else f"{args.evaluation_role}_report_only"
        ),
        "evaluation_role": args.evaluation_role,
        "split_manifest_hash": args.split_manifest_hash,
        "eligible_trade_date_count": len(eligible_dates),
        "eligible_train_date_count": len(eligible_dates) if args.evaluation_role == "train" else 0,
        "eligible_validation_date_count": len(eligible_dates) if args.evaluation_role == "validation" else 0,
        "eligible_holdout_date_count": len(eligible_dates) if args.evaluation_role == "holdout" else 0,
        "fields": list(fields),
        "source_shard_count": len(records),
        "source_rows": sum(int(row["source_rows"]) for row in records),
        "source_total_rows": sum(int(row["source_total_rows"]) for row in records),
        "sidecar_rows": sum(int(row["rows"]) for row in records),
        "sidecar_bytes": sum(int(row["output_bytes"]) for row in records),
        "build_wall_seconds": sum(float(row["wall_seconds"]) for row in records),
        "thread_environment": expected,
        "shards": records,
        "parity": parity_records,
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
        "forward_2026_reads": 0,
    }
    (root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"status": manifest["status"], "rows": manifest["source_rows"], "bytes": manifest["sidecar_bytes"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
