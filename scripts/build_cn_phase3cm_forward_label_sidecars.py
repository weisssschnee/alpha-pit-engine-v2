from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pyarrow.parquet as pq

from our_system_phase2.services.phase3cm_streaming_block_reader import build_forward_label_shard
from our_system_phase2.services.phase3cm_streaming_telemetry import _process_snapshot


REQUIRED_SERIAL_ENV = {
    "NUMBA_NUM_THREADS": "1",
    "ARROW_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "NUMEXPR_MAX_THREADS": "1",
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--pattern", default="*.parquet")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--max-shards", type=int, default=16)
    parser.add_argument("--row-group-size", type=int, default=262_144)
    parser.add_argument("--polars-threads", type=int, required=True)
    args = parser.parse_args()
    expected = {**REQUIRED_SERIAL_ENV, "POLARS_MAX_THREADS": str(int(args.polars_threads))}
    observed = {key: os.environ.get(key) for key in expected}
    if observed != expected:
        raise RuntimeError(f"thread environment is not frozen: observed={observed} expected={expected}")
    if int(args.polars_threads) < 1 or int(args.polars_threads) > 24:
        raise ValueError("POLARS_MAX_THREADS must be between 1 and 24")
    horizons = tuple(int(value) for value in str(args.horizons).split(",") if value)
    sources = sorted(args.source_root.resolve().rglob(str(args.pattern)))[: int(args.max_shards)]
    if not sources:
        raise FileNotFoundError("no source shards matched")
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    before_all = _process_snapshot()
    records = []
    for shard, source in enumerate(sources):
        before = _process_snapshot()
        output = output_root / f"shard_{shard:02d}.parquet"
        record = build_forward_label_shard(
            source_path=source,
            output_path=output,
            source_shard=shard,
            horizons=horizons,
            row_group_size=int(args.row_group_size),
        )
        after = _process_snapshot()
        source_rows = int(pq.ParquetFile(source).metadata.num_rows)
        output_rows = int(pq.ParquetFile(output).metadata.num_rows)
        record.update(
            {
                "source_rows": source_rows,
                "output_rows": output_rows,
                "row_count_match": source_rows == output_rows,
                "cpu_seconds": max(0.0, after.cpu_seconds - before.cpu_seconds),
                "effective_cores": (
                    max(0.0, after.cpu_seconds - before.cpu_seconds) / float(record["wall_seconds"])
                    if float(record["wall_seconds"]) > 0.0
                    else 0.0
                ),
                "peak_rss_bytes": max(before.peak_rss_bytes, after.peak_rss_bytes),
                "read_transfer_bytes": max(0, after.bytes_read - before.bytes_read),
            }
        )
        if not record["row_count_match"]:
            raise RuntimeError(f"label sidecar row drift: {source}")
        records.append(record)
    after_all = _process_snapshot()
    manifest = {
        "schema_version": "cn_phase3cm_forward_label_sidecar_manifest_v1",
        "status": "FORWARD_LABEL_SIDECARS_READY",
        "data_role": "development_train_only",
        "horizons": list(horizons),
        "source_shard_count": len(records),
        "source_rows": sum(int(row["source_rows"]) for row in records),
        "output_rows": sum(int(row["output_rows"]) for row in records),
        "output_bytes": sum(int(row["output_bytes"]) for row in records),
        "build_wall_seconds": max(0.0, after_all.wall_seconds - before_all.wall_seconds),
        "build_cpu_seconds": max(0.0, after_all.cpu_seconds - before_all.cpu_seconds),
        "peak_rss_bytes": max(before_all.peak_rss_bytes, after_all.peak_rss_bytes),
        "read_transfer_bytes": max(0, after_all.bytes_read - before_all.bytes_read),
        "thread_environment": expected,
        "shards": records,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    (output_root / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({key: manifest[key] for key in ("status", "source_rows", "output_bytes", "build_wall_seconds")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
