from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
from pathlib import Path

import polars as pl


THREAD_ENV = {
    "NUMBA_NUM_THREADS": "1",
    "ARROW_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "NUMEXPR_MAX_THREADS": "1",
}


def _train_dates(path: Path, expected_sha256: str) -> tuple[str, ...]:
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != str(expected_sha256):
        raise RuntimeError(f"split manifest hash drift: actual={actual}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = tuple(csv.DictReader(handle))
    dates = tuple(str(row["trade_date"]) for row in rows if str(row.get("split")) == "train")
    if not dates or len(dates) != len(set(dates)):
        raise RuntimeError("split manifest train calendar is empty or duplicated")
    return dates


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest-hash", required=True)
    parser.add_argument("--polars-threads", type=int, required=True)
    args = parser.parse_args()

    expected = {**THREAD_ENV, "POLARS_MAX_THREADS": str(int(args.polars_threads))}
    observed = {key: os.environ.get(key) for key in expected}
    if observed != expected:
        raise RuntimeError(f"thread environment is not frozen: observed={observed} expected={expected}")
    if int(args.polars_threads) < 1 or int(args.polars_threads) > 24:
        raise ValueError("POLARS_MAX_THREADS must be between 1 and 24")

    root = args.input_root.resolve()
    files = tuple(sorted(root.glob("shard_*.parquet")))
    if len(files) != 16:
        raise RuntimeError(f"expected 16 sidecar shards, got {len(files)}")
    train_dates = _train_dates(args.split_manifest.resolve(), args.split_manifest_hash)
    train_values = tuple(dt.date.fromisoformat(value) for value in train_dates)
    scan = pl.concat(
        [pl.scan_parquet(path, low_memory=True).select(pl.col("trade_time").dt.date().alias("trade_date")) for path in files],
        rechunk=False,
    )
    aggregate = (
        scan.select(
            pl.len().alias("rows"),
            pl.col("trade_date").n_unique().alias("distinct_trade_dates"),
            pl.col("trade_date").min().alias("min_trade_date"),
            pl.col("trade_date").max().alias("max_trade_date"),
            (~pl.col("trade_date").is_in(train_values)).sum().alias("rows_outside_train"),
            pl.col("trade_date")
            .filter(~pl.col("trade_date").is_in(train_values))
            .n_unique()
            .alias("dates_outside_train"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    observed_dates = set(
        scan.select(pl.col("trade_date").unique())
        .collect(engine="streaming")["trade_date"]
        .to_list()
    )
    missing_train = tuple(value for value in train_values if value not in observed_dates)
    record = {
        "schema_version": "cn_phase3cm_sidecar_calendar_audit_v1",
        "status": "SIDECAR_CALENDAR_TRAIN_ONLY_PASS" if int(aggregate["rows_outside_train"]) == 0 else "SIDECAR_CALENDAR_BOUNDARY_FAIL",
        "input_root": str(root),
        "input_shard_count": len(files),
        "rows": int(aggregate["rows"]),
        "distinct_trade_dates": int(aggregate["distinct_trade_dates"]),
        "min_trade_date": str(aggregate["min_trade_date"]),
        "max_trade_date": str(aggregate["max_trade_date"]),
        "eligible_train_date_count": len(train_dates),
        "observed_train_date_count": int(aggregate["distinct_trade_dates"]) - int(aggregate["dates_outside_train"]),
        "missing_train_date_count": len(missing_train),
        "missing_train_dates": tuple(str(value) for value in missing_train),
        "rows_outside_train": int(aggregate["rows_outside_train"]),
        "dates_outside_train": int(aggregate["dates_outside_train"]),
        "split_manifest_hash": args.split_manifest_hash,
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "thread_environment": expected,
    }
    print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    if record["status"] != "SIDECAR_CALENDAR_TRAIN_ONLY_PASS":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
