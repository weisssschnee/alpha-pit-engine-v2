"""Zero-financial parity and acceleration qualification for stock-session mapping."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import time
from pathlib import Path
from typing import Any

import numba
import numpy as np

from our_system_phase2.services.phase3cm_streaming_portfolio import (
    _mapping_kernel,
    _mapping_kernel_fused,
    _prepare_label_orders,
    _prepare_signal_ranks,
)


OFFICIAL_CANDIDATE_COUNT = 24
OFFICIAL_ROW_COUNT = 50_352
OFFICIAL_GROUP_COUNT = 10
OFFICIAL_HORIZON_COUNT = 4
OFFICIAL_REPEATS = 40
OFFICIAL_COMPUTE_THREADS = 32
MINIMUM_EFFECTIVE_CORES = 16.0
MINIMUM_WALL_SPEEDUP = 1.05


def _canonical_sha256(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _synthetic_fixture(
    *,
    candidate_count: int,
    row_count: int,
    group_count: int,
    horizon_count: int,
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
]:
    rng = np.random.default_rng(20260802)
    signals = np.round(
        rng.normal(size=(candidate_count, row_count)),
        decimals=2,
    )
    labels = np.round(
        rng.normal(scale=0.01, size=(horizon_count, row_count)),
        decimals=6,
    )
    signal_nan = rng.random(signals.shape) < 0.01
    label_nan = rng.random(labels.shape) < 0.01
    signals[signal_nan] = np.nan
    labels[label_nan] = np.nan
    signal_inf = rng.random(signals.shape) < 0.0005
    label_inf = rng.random(labels.shape) < 0.0005
    signals[signal_inf] = np.inf
    labels[label_inf] = -np.inf
    group_sizes = np.full(group_count, row_count // group_count, dtype=np.int64)
    group_sizes[: row_count % group_count] += 1
    ends = np.cumsum(group_sizes, dtype=np.int64)
    starts = np.r_[np.int64(0), ends[:-1]]
    directions = np.where(
        np.arange(candidate_count) % 2 == 0,
        1.0,
        -1.0,
    ).astype(np.float64)
    return signals, labels, starts, ends, directions


def _scratch(
    *,
    compute_threads: int,
    max_group_size: int,
) -> tuple[np.ndarray, np.ndarray]:
    return (
        np.empty((compute_threads, 3, max_group_size), dtype=np.float64),
        np.empty((compute_threads, 2, max_group_size), dtype=np.int64),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()

    if args.smoke:
        candidate_count = 6
        row_count = 1_024
        group_count = 4
        horizon_count = 4
        repeats = 2
        compute_threads = min(4, int(numba.config.NUMBA_NUM_THREADS))
    else:
        candidate_count = OFFICIAL_CANDIDATE_COUNT
        row_count = OFFICIAL_ROW_COUNT
        group_count = OFFICIAL_GROUP_COUNT
        horizon_count = OFFICIAL_HORIZON_COUNT
        repeats = OFFICIAL_REPEATS
        compute_threads = OFFICIAL_COMPUTE_THREADS
        if int(numba.config.NUMBA_NUM_THREADS) != OFFICIAL_COMPUTE_THREADS:
            raise RuntimeError(
                "official qualification requires NUMBA_NUM_THREADS=32 before "
                "Python starts"
            )

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=False)
    numba.set_num_threads(compute_threads)
    signals, labels, starts, ends, directions = _synthetic_fixture(
        candidate_count=candidate_count,
        row_count=row_count,
        group_count=group_count,
        horizon_count=horizon_count,
    )
    label_orders, label_order_counts = _prepare_label_orders(
        labels,
        starts,
        ends,
    )
    max_group_size = int(np.max(ends - starts))

    legacy_float, legacy_int = _scratch(
        compute_threads=compute_threads,
        max_group_size=max_group_size,
    )
    fused_float, fused_int = _scratch(
        compute_threads=compute_threads,
        max_group_size=max_group_size,
    )
    signal_ranks, signal_orders, signal_order_counts = _prepare_signal_ranks(
        signals,
        starts,
        ends,
    )
    legacy_selected, legacy_metrics = _mapping_kernel(
        signals,
        signal_ranks,
        signal_orders,
        signal_order_counts,
        labels,
        label_orders,
        label_order_counts,
        starts,
        ends,
        directions,
        20,
        0.2,
        True,
        legacy_float,
        legacy_int,
    )
    fused_selected, fused_metrics = _mapping_kernel_fused(
        signals,
        labels,
        label_orders,
        label_order_counts,
        starts,
        ends,
        directions,
        20,
        0.2,
        True,
        fused_float,
        fused_int,
    )
    selected_bit_exact = bool(np.array_equal(fused_selected, legacy_selected))
    metrics_bit_exact = bool(
        np.array_equal(fused_metrics, legacy_metrics, equal_nan=True)
    )
    output_digest = hashlib.sha256(
        np.packbits(fused_selected.reshape(-1), bitorder="little").tobytes()
        + np.nan_to_num(fused_metrics, nan=np.inf).tobytes()
    ).hexdigest()
    del (
        signal_ranks,
        signal_orders,
        signal_order_counts,
        legacy_selected,
        legacy_metrics,
        fused_selected,
        fused_metrics,
    )

    legacy_wall = 0.0
    legacy_cpu = 0.0
    fused_wall = 0.0
    fused_cpu = 0.0
    for repeat in range(repeats):
        modes = ("legacy", "fused") if repeat % 2 == 0 else ("fused", "legacy")
        for mode in modes:
            wall_start = time.perf_counter()
            cpu_start = time.process_time()
            if mode == "legacy":
                signal_ranks, signal_orders, signal_order_counts = (
                    _prepare_signal_ranks(signals, starts, ends)
                )
                selected, metrics = _mapping_kernel(
                    signals,
                    signal_ranks,
                    signal_orders,
                    signal_order_counts,
                    labels,
                    label_orders,
                    label_order_counts,
                    starts,
                    ends,
                    directions,
                    20,
                    0.2,
                    True,
                    legacy_float,
                    legacy_int,
                )
            else:
                selected, metrics = _mapping_kernel_fused(
                    signals,
                    labels,
                    label_orders,
                    label_order_counts,
                    starts,
                    ends,
                    directions,
                    20,
                    0.2,
                    True,
                    fused_float,
                    fused_int,
                )
            cpu_elapsed = time.process_time() - cpu_start
            wall_elapsed = time.perf_counter() - wall_start
            if mode == "legacy":
                legacy_wall += wall_elapsed
                legacy_cpu += cpu_elapsed
                del signal_ranks, signal_orders, signal_order_counts
            else:
                fused_wall += wall_elapsed
                fused_cpu += cpu_elapsed
            del selected, metrics

    legacy_effective = legacy_cpu / legacy_wall if legacy_wall > 0.0 else 0.0
    fused_effective = fused_cpu / fused_wall if fused_wall > 0.0 else 0.0
    wall_speedup = legacy_wall / fused_wall if fused_wall > 0.0 else 0.0
    parity_pass = selected_bit_exact and metrics_bit_exact
    performance_pass = (
        fused_effective >= MINIMUM_EFFECTIVE_CORES
        and wall_speedup >= MINIMUM_WALL_SPEEDUP
    )
    if args.smoke:
        status = "ZERO_FINANCIAL_MAPPING_SMOKE_PASS" if parity_pass else "FAIL"
    else:
        status = (
            "ZERO_FINANCIAL_MAPPING_ACCELERATION_QUALIFIED"
            if parity_pass and performance_pass
            else "FAIL"
        )

    payload: dict[str, Any] = {
        "status": status,
        "qualification_mode": "SMOKE" if args.smoke else "OFFICIAL_77O",
        "contract": {
            "candidate_count": candidate_count,
            "row_count": row_count,
            "group_count": group_count,
            "horizon_count": horizon_count,
            "repeats": repeats,
            "compute_threads": compute_threads,
            "minimum_effective_cores": (
                None if args.smoke else MINIMUM_EFFECTIVE_CORES
            ),
            "minimum_wall_speedup": None if args.smoke else MINIMUM_WALL_SPEEDUP,
            "mapping_kernel_mode": "FUSED_CANDIDATE_GROUP_RANK_AND_HORIZONS",
        },
        "parity": {
            "selected_bit_exact": selected_bit_exact,
            "metrics_bit_exact": metrics_bit_exact,
            "output_digest_sha256": output_digest,
        },
        "performance": {
            "legacy_wall_seconds": legacy_wall,
            "legacy_cpu_seconds": legacy_cpu,
            "legacy_effective_cores": legacy_effective,
            "fused_wall_seconds": fused_wall,
            "fused_cpu_seconds": fused_cpu,
            "fused_effective_cores": fused_effective,
            "wall_speedup": wall_speedup,
            "legacy_signal_rank_surface_bytes": int(
                signals.nbytes
                + signals.size * np.dtype(np.int32).itemsize
                + candidate_count
                * group_count
                * np.dtype(np.int32).itemsize
            ),
        },
        "runtime": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "numba": numba.__version__,
            "numba_threading_layer": numba.threading_layer(),
            "numba_num_threads": numba.get_num_threads(),
            "thread_environment": {
                key: os.environ.get(key)
                for key in (
                    "NUMBA_NUM_THREADS",
                    "ARROW_NUM_THREADS",
                    "OMP_NUM_THREADS",
                    "MKL_NUM_THREADS",
                    "OPENBLAS_NUM_THREADS",
                    "NUMEXPR_MAX_THREADS",
                    "POLARS_MAX_THREADS",
                )
            },
        },
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_writes": 0,
        "feedback_writes": 0,
        "scheduler_writes": 0,
        "archive_writes": 0,
        "promotion_writes": 0,
    }
    payload["canonical_payload_sha256"] = _canonical_sha256(payload)
    receipt_path = output_root / "qualification_receipt.json"
    receipt_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    closure: dict[str, Any] = {
        "status": status,
        "qualification_receipt": {
            "path": receipt_path.name,
            "size": receipt_path.stat().st_size,
            "sha256": _file_sha256(receipt_path),
        },
    }
    closure["canonical_payload_sha256"] = _canonical_sha256(closure)
    (output_root / "ZERO_FINANCIAL_QUALIFICATION_COMPLETE.json").write_text(
        json.dumps(closure, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, sort_keys=True))
    return 0 if status != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
