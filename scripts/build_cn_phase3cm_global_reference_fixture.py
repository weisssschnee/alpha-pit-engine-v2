from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import polars as pl

from our_system_phase2.services.phase3cm_time_major_sidecar import STABLE_KEY


THREAD_ENV = {
    "NUMBA_NUM_THREADS": "1",
    "ARROW_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "NUMEXPR_MAX_THREADS": "1",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--split-manifest-hash", required=True)
    parser.add_argument("--first-trade-times", type=int, default=0)
    parser.add_argument("--polars-threads", type=int, required=True)
    args = parser.parse_args()

    expected = {**THREAD_ENV, "POLARS_MAX_THREADS": str(int(args.polars_threads))}
    observed = {key: os.environ.get(key) for key in expected}
    if observed != expected:
        raise RuntimeError(f"thread environment is not frozen: observed={observed} expected={expected}")
    inputs = tuple(sorted(args.input_root.resolve().glob("shard_*.parquet")))
    if len(inputs) != 16:
        raise RuntimeError(f"global reference fixture requires 16 train-only shards, got {len(inputs)}")
    manifest_path = args.input_root.resolve() / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "TIME_MAJOR_LAYOUT_PARITY_PASS":
        raise RuntimeError("input train-only sidecar parity is not qualified")
    if str(manifest.get("split_manifest_hash")) != str(args.split_manifest_hash):
        raise RuntimeError("input train-only sidecar split hash drift")

    lazy = pl.concat([pl.scan_parquet(path, rechunk=False, low_memory=True) for path in inputs])
    if "date" not in lazy.collect_schema().names():
        lazy = lazy.with_columns(pl.col("trade_time").dt.date().alias("date"))
    selection = "ALL_TRAIN_COORDINATES"
    if int(args.first_trade_times) > 0:
        times = (
            lazy.select(pl.col("trade_time").unique().sort().head(int(args.first_trade_times)))
            .collect(engine="streaming")["trade_time"]
        )
        lazy = lazy.filter(pl.col("trade_time").is_in(times))
        selection = f"FIRST_{int(args.first_trade_times)}_TRADE_TIMES_SORTED"
    lazy = lazy.sort(list(STABLE_KEY), maintain_order=True)
    root = args.output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    output = root / "shard_00" / "phase3aq_wide_true1min" / "canary" / "phase3aq_true_1min_formula_canary.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    started = time.perf_counter()
    lazy.sink_parquet(
        temporary,
        compression="zstd",
        compression_level=3,
        statistics=True,
        row_group_size=262_144,
        maintain_order=True,
        mkdir=True,
        engine="streaming",
    )
    temporary.replace(output)
    aggregate = (
        pl.scan_parquet(output)
        .select(
            pl.len().alias("rows"),
            pl.struct(list(STABLE_KEY)).n_unique().alias("unique_keys"),
            pl.col("trade_time").n_unique().alias("trade_times"),
            pl.col("trade_time").dt.date().n_unique().alias("trade_dates"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    record = {
        "schema_version": "cn_phase3cm_global_reference_fixture_v1",
        "status": "GLOBAL_REFERENCE_FIXTURE_READY",
        "selection_policy": selection,
        "source_shard_count": 16,
        "source_sidecar_manifest_sha256": _sha256(manifest_path),
        "split_manifest_hash": args.split_manifest_hash,
        "output_path": str(output),
        "output_sha256": _sha256(output),
        "output_bytes": output.stat().st_size,
        "rows": int(aggregate["rows"]),
        "stable_key_unique_count": int(aggregate["unique_keys"]),
        "trade_time_count": int(aggregate["trade_times"]),
        "trade_date_count": int(aggregate["trade_dates"]),
        "wall_seconds": time.perf_counter() - started,
        "thread_environment": expected,
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    if record["rows"] != record["stable_key_unique_count"]:
        raise RuntimeError("global reference fixture stable-key collision")
    (root / "CN_GLOBAL_REFERENCE_FIXTURE_MANIFEST.json").write_text(
        json.dumps(record, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(record, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
