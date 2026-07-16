from __future__ import annotations

import argparse
import csv
import json
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import _rank_pct_1d_average
from our_system_phase2.services.phase3cm_streaming_block_reader import TimeMajorBlockReader
from our_system_phase2.services.phase3cm_streaming_expression import StreamingExpressionExecutor
from our_system_phase2.services.phase3cm_streaming_portfolio import (
    _linear_quantile,
    _mapping_kernel,
    _rank_average,
)
from our_system_phase2.services.real_market_validation import evaluate_panel_expression
from our_system_phase2.services.unified_capability_registry import stable_hash


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _sidecars(root: Path) -> tuple[Path, ...]:
    paths = tuple(sorted(root.glob("shard_*.parquet")))
    if len(paths) != 16:
        raise RuntimeError(f"expected 16 sidecars, got {len(paths)}")
    return paths


def _top_summary(codes: np.ndarray, signal: np.ndarray, ranks: np.ndarray) -> dict[str, object]:
    valid = np.isfinite(signal) & np.isfinite(ranks)
    cutoff = float(np.nanquantile(ranks[valid], 0.8))
    chosen = valid & (ranks >= cutoff)
    selected = sorted(set(codes[chosen].astype(str)))
    return {
        "valid_count": int(valid.sum()),
        "signal_unique_count": int(np.unique(signal[valid]).size),
        "rank_unique_count": int(np.unique(ranks[valid]).size),
        "rank_cutoff": cutoff,
        "rank_max": float(np.max(ranks[valid])),
        "selected_row_count": int(chosen.sum()),
        "selected_code_identity": stable_hash(selected),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-table", type=Path, required=True)
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--field-sidecar-root", type=Path, required=True)
    parser.add_argument("--label-sidecar-root", type=Path, required=True)
    parser.add_argument("--global-fixture", type=Path, required=True)
    parser.add_argument("--target-time", required=True)
    parser.add_argument("--start-time", required=True)
    parser.add_argument("--compute-threads", type=int, default=4)
    args = parser.parse_args()

    expected = {
        "NUMBA_NUM_THREADS": str(int(args.compute_threads)),
        "ARROW_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "NUMEXPR_MAX_THREADS": "1",
        "POLARS_MAX_THREADS": "1",
    }
    observed = {key: os.environ.get(key) for key in expected}
    if observed != expected:
        raise RuntimeError(f"thread environment is not frozen: observed={observed} expected={expected}")

    rows = _read_csv(args.candidate_table.resolve())
    candidate = next(row for row in rows if str(row["candidate_id"]) == str(args.candidate_id))
    pair_id = str(candidate["pair_id"])
    pair = [row for row in rows if str(row["pair_id"]) == pair_id]
    pair.sort(key=lambda row: 0 if str(row.get("pair_member_role")) == "PRIMARY" else 1)
    fields = sorted(
        {
            match
            for row in pair
            for match in re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", str(row["expression"]))
        }
        | {"close"}
    )
    field_paths = _sidecars(args.field_sidecar_root.resolve())
    label_paths = _sidecars(args.label_sidecar_root.resolve())
    symbols = tuple(
        str(value)
        for value in (
            pl.concat([pl.scan_parquet(path).select(pl.col("code").cast(pl.String)) for path in field_paths])
            .select(pl.col("code").unique().sort())
            .collect(engine="streaming")["code"]
            .to_list()
        )
    )
    target = pd.Timestamp(args.target_time)
    reader = TimeMajorBlockReader(
        field_sidecars=field_paths,
        label_sidecars=label_paths,
        raw_fields=fields,
        horizons=(1,),
        symbol_registry=symbols,
    )
    block = reader.read_block(start_time=pd.Timestamp(args.start_time), end_time=target + pd.Timedelta(days=1))
    executor = StreamingExpressionExecutor(
        code_count=len(symbols), compute_threads=int(args.compute_threads), cache_max_bytes=8 * 1024**3
    ).bind_block(raw_fields=block.raw_fields, code_ids=block.code_ids, time_ids=block.time_ids)
    evaluated = executor.evaluate_many([str(row["expression"]) for row in pair])
    new_primary = evaluated[str(pair[0]["expression"])].copy()
    new_control = evaluated[str(pair[1]["expression"])].copy()
    common = np.isfinite(new_primary) & np.isfinite(new_control)
    new_primary[~common] = np.nan
    target_ns = int(target.value)
    new_mask = block.trade_times_ns == target_ns
    new_codes = np.array([symbols[int(value)] for value in block.code_ids[new_mask]], dtype=object)
    new_signal = new_primary[new_mask]
    new_rank = _rank_pct_1d_average(new_signal)
    native_rank = _rank_average(new_signal)
    native_valid_rank = native_rank[np.isfinite(native_rank)]
    native_cutoff = float(_linear_quantile(native_valid_rank, 0.8))
    native_direct_selected = np.isfinite(native_rank) & (native_rank >= native_cutoff)
    target_label = block.labels[1][new_mask]
    native_selected, native_metrics = _mapping_kernel(
        new_signal.reshape(1, -1),
        target_label.reshape(1, -1),
        np.array([0], dtype=np.int64),
        np.array([len(new_signal)], dtype=np.int64),
        np.array([1.0], dtype=np.float64),
        20,
        0.2,
        False,
    )

    legacy = pl.read_parquet(args.global_fixture.resolve(), columns=["code", "trade_time", "date", *fields]).to_pandas()
    legacy["code"] = legacy["code"].astype(str)
    legacy["trade_time"] = pd.to_datetime(legacy["trade_time"])
    legacy = legacy.sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
    legacy_primary = pd.to_numeric(
        evaluate_panel_expression(legacy, str(pair[0]["expression"]), cache={}, data_role="development"), errors="coerce"
    ).to_numpy(dtype=float, copy=True)
    legacy_control = pd.to_numeric(
        evaluate_panel_expression(legacy, str(pair[1]["expression"]), cache={}, data_role="development"), errors="coerce"
    ).to_numpy(dtype=float, copy=True)
    legacy_common = np.isfinite(legacy_primary) & np.isfinite(legacy_control)
    legacy_primary[~legacy_common] = np.nan
    legacy_mask = legacy["trade_time"].to_numpy(dtype="datetime64[ns]").astype(np.int64) == target_ns
    legacy_codes = legacy.loc[legacy_mask, "code"].to_numpy(dtype=object)
    legacy_signal = legacy_primary[legacy_mask]
    legacy_rank = _rank_pct_1d_average(legacy_signal)

    new_by_code = {str(code): float(value) for code, value in zip(new_codes, new_signal) if np.isfinite(value)}
    legacy_by_code = {str(code): float(value) for code, value in zip(legacy_codes, legacy_signal) if np.isfinite(value)}
    shared_codes = sorted(set(new_by_code) & set(legacy_by_code))
    signal_errors = np.array([new_by_code[code] - legacy_by_code[code] for code in shared_codes], dtype=float)
    report = {
        "candidate_id": args.candidate_id,
        "pair_id": pair_id,
        "target_time": target.isoformat(),
        "new": _top_summary(new_codes, new_signal, new_rank),
        "native_rank_max_abs_error": float(np.nanmax(np.abs(native_rank - new_rank))),
        "native_rank_cutoff": native_cutoff,
        "native_direct_selected_count": int(native_direct_selected.sum()),
        "native_mapping_selected_count": int(native_selected.sum()),
        "native_mapping_metric": native_metrics[0, 0, 0].tolist(),
        "legacy": _top_summary(legacy_codes, legacy_signal, legacy_rank),
        "new_finite_codes": len(new_by_code),
        "legacy_finite_codes": len(legacy_by_code),
        "shared_finite_codes": len(shared_codes),
        "signal_max_abs_error": float(np.max(np.abs(signal_errors))) if len(signal_errors) else None,
        "signal_nonzero_error_count": int(np.count_nonzero(signal_errors)) if len(signal_errors) else 0,
        "signal_correlation": float(np.corrcoef(
            [new_by_code[code] for code in shared_codes], [legacy_by_code[code] for code in shared_codes]
        )[0, 1]) if len(shared_codes) > 1 else None,
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
