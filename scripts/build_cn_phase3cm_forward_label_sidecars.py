from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path

from our_system_phase2.services.phase3cm_streaming_block_reader import (
    build_global_continuity_forward_label_sidecars,
)
from our_system_phase2.services.phase3cm_streaming_telemetry import _process_snapshot


REQUIRED_SERIAL_ENV = {
    "NUMBA_NUM_THREADS": "1",
    "ARROW_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "NUMEXPR_MAX_THREADS": "1",
}


def _split_dates(
    path: Path,
    expected_sha256: str,
    *,
    evaluation_role: str,
) -> tuple[str, ...]:
    if evaluation_role not in {
        "train",
        "validation",
        "holdout",
        "forward_2026",
        "historical_challenge",
    }:
        raise ValueError(f"unsupported evaluation role: {evaluation_role}")
    if hashlib.sha256(path.read_bytes()).hexdigest() != str(expected_sha256):
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
        choices=(
            "train",
            "validation",
            "holdout",
            "forward_2026",
            "historical_challenge",
        ),
        default="train",
    )
    parser.add_argument("--pattern", default="*.parquet")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest-hash", required=True)
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
    eligible_dates = _split_dates(
        args.split_manifest.resolve(),
        args.split_manifest_hash,
        evaluation_role=args.evaluation_role,
    )
    source_root = args.source_root.resolve()
    sources = sorted(source_root.glob(str(args.pattern)))[: int(args.max_shards)]
    if len(sources) != int(args.max_shards):
        raise FileNotFoundError(f"global-continuity labels require {args.max_shards} field sidecars")
    field_manifest_path = source_root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    if not field_manifest_path.is_file():
        raise FileNotFoundError("field sidecar manifest is required")
    field_manifest = json.loads(field_manifest_path.read_text(encoding="utf-8"))
    if field_manifest.get("status") != "TIME_MAJOR_LAYOUT_PARITY_PASS":
        raise RuntimeError("field sidecar parity is not qualified")
    if str(field_manifest.get("split_manifest_hash")) != str(args.split_manifest_hash):
        raise RuntimeError("field sidecar split hash drift")
    if str(field_manifest.get("evaluation_role") or "train") != args.evaluation_role:
        raise RuntimeError("field sidecar evaluation role drift")
    if int(field_manifest.get("eligible_trade_date_count") or field_manifest.get("eligible_train_date_count") or 0) != len(eligible_dates):
        raise RuntimeError("field sidecar calendar count drift")
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    before_all = _process_snapshot()
    build = build_global_continuity_forward_label_sidecars(
        field_sidecars=sources,
        output_root=output_root,
        horizons=horizons,
        split_manifest_hash=args.split_manifest_hash,
        row_group_size=int(args.row_group_size),
    )
    after_all = _process_snapshot()
    manifest = {
        **build,
        "schema_version": "cn_phase3cm_forward_label_sidecar_manifest_v3_global_symbol_continuity",
        "status": "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY",
        "evaluation_role": args.evaluation_role,
        "data_role": (
            "development_train_only"
            if args.evaluation_role == "train"
            else f"{args.evaluation_role}_report_only"
        ),
        "eligible_trade_date_count": len(eligible_dates),
        "eligible_train_date_count": len(eligible_dates) if args.evaluation_role == "train" else 0,
        "eligible_validation_date_count": len(eligible_dates) if args.evaluation_role == "validation" else 0,
        "eligible_holdout_date_count": len(eligible_dates) if args.evaluation_role == "holdout" else 0,
        "eligible_forward_2026_date_count": len(eligible_dates) if args.evaluation_role == "forward_2026" else 0,
        "eligible_historical_challenge_date_count": len(eligible_dates) if args.evaluation_role == "historical_challenge" else 0,
        "validation_reads": (
            int(build.get("source_rows") or 0)
            if args.evaluation_role == "validation"
            else 0
        ),
        "holdout_reads": (
            int(build.get("source_rows") or 0)
            if args.evaluation_role == "holdout"
            else 0
        ),
        "forward_2026_reads": (
            int(build.get("source_rows") or 0)
            if args.evaluation_role == "forward_2026"
            else 0
        ),
        "historical_challenge_reads": (
            int(build.get("source_rows") or 0)
            if args.evaluation_role == "historical_challenge"
            else 0
        ),
        "build_wall_seconds": max(0.0, after_all.wall_seconds - before_all.wall_seconds),
        "build_cpu_seconds": max(0.0, after_all.cpu_seconds - before_all.cpu_seconds),
        "peak_rss_bytes": max(before_all.peak_rss_bytes, after_all.peak_rss_bytes),
        "read_transfer_bytes": max(0, after_all.bytes_read - before_all.bytes_read),
        "thread_environment": expected,
    }
    (output_root / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({key: manifest[key] for key in ("status", "source_rows", "output_bytes", "build_wall_seconds")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
