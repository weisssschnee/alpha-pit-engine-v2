from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import polars as pl

from our_system_phase2.services.phase3cm_streaming_block_reader import TimeMajorBlockReader
from our_system_phase2.services.phase3cm_streaming_expression import StreamingExpressionExecutor
from our_system_phase2.services.phase3cm_streaming_portfolio import BatchedPortfolioKernel
from our_system_phase2.services.unified_capability_registry import stable_hash


SERIAL_THREAD_ENV = {
    "ARROW_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "NUMEXPR_MAX_THREADS": "1",
    "POLARS_MAX_THREADS": "1",
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _candidate_pairs(rows: Sequence[Mapping[str, Any]], pair_count: int) -> list[dict[str, Any]]:
    pair_order: list[str] = []
    by_pair: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        pair_id = str(row.get("pair_id") or "")
        if pair_id not in by_pair:
            pair_order.append(pair_id)
            by_pair[pair_id] = []
        by_pair[pair_id].append(row)
    selected: list[dict[str, Any]] = []
    for pair_id in pair_order[: int(pair_count)]:
        members = by_pair[pair_id]
        members.sort(key=lambda row: 0 if str(row.get("pair_member_role")) == "PRIMARY" else 1)
        if len(members) != 2 or [str(row.get("pair_member_role")) for row in members] != ["PRIMARY", "CONTROL"]:
            raise RuntimeError(f"candidate pair contract drift: {pair_id}")
        selected.extend(members)
    if len(selected) != int(pair_count) * 2:
        raise RuntimeError("requested pair count exceeds candidate table")
    return selected


def _raw_fields(candidates: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    fields = {"close"}
    for row in candidates:
        fields.update(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", str(row.get("expression") or "")))
    return tuple(sorted(fields))


def _train_dates(path: Path, expected_sha256: str) -> tuple[str, ...]:
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != str(expected_sha256):
        raise RuntimeError(f"split manifest hash drift: actual={actual}")
    rows = _read_csv(path)
    dates = tuple(str(row["trade_date"]) for row in rows if str(row.get("split")) == "train")
    if not dates or len(dates) != len(set(dates)):
        raise RuntimeError("train calendar is empty or duplicated")
    return dates


def _sidecars(root: Path) -> tuple[Path, ...]:
    paths = tuple(sorted(root.glob("shard_*.parquet")))
    if len(paths) != 16:
        raise RuntimeError(f"expected 16 sidecars, got {len(paths)}: {root}")
    return paths


def _symbols(paths: Sequence[Path]) -> tuple[str, ...]:
    values = (
        pl.concat(
            [pl.scan_parquet(path, low_memory=True).select(pl.col("code").cast(pl.String)) for path in paths],
            rechunk=False,
        )
        .select(pl.col("code").unique().sort())
        .collect(engine="streaming")["code"]
        .to_list()
    )
    return tuple(str(value) for value in values)


def _pair_common_support(signals: np.ndarray) -> None:
    if signals.shape[0] % 2:
        raise ValueError("candidate member count must be even")
    for candidate in range(0, signals.shape[0], 2):
        common = np.isfinite(signals[candidate]) & np.isfinite(signals[candidate + 1])
        signals[candidate, ~common] = np.nan
        signals[candidate + 1, ~common] = np.nan


def _optional_float(value: Any) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    converted = float(value)
    return converted if math.isfinite(converted) else None


def _numeric_equal(left: float | None, right: float | None, tolerance: float) -> tuple[bool, float]:
    if left is None or right is None:
        return left is None and right is None, 0.0
    error = abs(float(left) - float(right))
    return error <= tolerance + tolerance * max(abs(float(left)), abs(float(right))), error


def _key(candidate_id: str, horizon: int, trade_time: str) -> tuple[str, int, str]:
    return str(candidate_id), int(horizon), pd.Timestamp(trade_time).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-table", type=Path, required=True)
    parser.add_argument("--field-sidecar-root", type=Path, required=True)
    parser.add_argument("--label-sidecar-root", type=Path, required=True)
    parser.add_argument("--legacy-pnl", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest-hash", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pair-count", type=int, default=14)
    parser.add_argument("--compute-threads", type=int, default=4)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs", type=int, default=20)
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--top-quantile", type=float, default=0.2)
    parser.add_argument("--portfolio-mode", default="long_only_top")
    parser.add_argument("--numeric-tolerance", type=float, default=1e-8)
    parser.add_argument("--signal-tolerance", type=float, default=1e-7)
    args = parser.parse_args()

    expected = {**SERIAL_THREAD_ENV, "NUMBA_NUM_THREADS": str(int(args.compute_threads))}
    observed = {key: os.environ.get(key) for key in expected}
    if observed != expected:
        raise RuntimeError(f"thread environment is not frozen: observed={observed} expected={expected}")
    if int(args.compute_threads) < 1 or int(args.compute_threads) > 24:
        raise ValueError("compute threads must be between 1 and 24")

    candidates = _candidate_pairs(_read_csv(args.candidate_table.resolve()), int(args.pair_count))
    horizons = tuple(int(value) for value in str(args.horizons).split(",") if value)
    train_dates = _train_dates(args.split_manifest.resolve(), args.split_manifest_hash)
    field_paths = _sidecars(args.field_sidecar_root.resolve())
    label_paths = _sidecars(args.label_sidecar_root.resolve())
    symbols = _symbols(field_paths)
    reader = TimeMajorBlockReader(
        field_sidecars=field_paths,
        label_sidecars=label_paths,
        raw_fields=_raw_fields(candidates),
        horizons=horizons,
        symbol_registry=symbols,
        eligible_trade_dates=train_dates,
    )
    block = reader.read_block(
        start_time=pd.Timestamp(train_dates[0]),
        end_time=pd.Timestamp(train_dates[-1]) + pd.Timedelta(days=1),
    )
    expression = StreamingExpressionExecutor(
        code_count=len(symbols),
        compute_threads=int(args.compute_threads),
        cache_max_bytes=8 * 1024**3,
    ).bind_block(raw_fields=block.raw_fields, code_ids=block.code_ids, time_ids=block.time_ids)
    namespaces = {
        str(row["expression"]): "|".join(
            (
                str(row.get("support_unit") or ""),
                str(row.get("outer_mapping") or ""),
                str(args.portfolio_mode),
            )
        )
        for row in candidates
    }
    evaluated = expression.evaluate_many(
        [str(row["expression"]) for row in candidates],
        mapping_namespaces=namespaces,
    )
    signals = np.vstack([evaluated[str(row["expression"])] for row in candidates])
    _pair_common_support(signals)
    directions = np.array(
        [1.0 if str(row.get("open_direction") or "long_top") == "long_top" else -1.0 for row in candidates],
        dtype=np.float64,
    )
    portfolio = BatchedPortfolioKernel(
        candidate_count=len(candidates),
        code_count=len(symbols),
        horizons=horizons,
        compute_threads=int(args.compute_threads),
        min_obs=int(args.min_obs),
        top_quantile=float(args.top_quantile),
        cost_bps=float(args.cost_bps),
        portfolio_mode=str(args.portfolio_mode),
    ).evaluate_block(
        signals=signals,
        labels=block.labels,
        time_ids=block.time_ids,
        code_ids=block.code_ids,
        day_ids=block.day_ids,
        directions=directions,
        day_count=len(block.day_labels),
        audit_coordinate_arrays=True,
    )
    if portfolio.audit_selected is None or portfolio.audit_mapping_metrics is None or portfolio.audit_coordinate_metrics is None:
        raise RuntimeError("coordinate audit arrays were not returned")

    legacy_rows = _read_csv(args.legacy_pnl.resolve())
    legacy = {
        _key(row["candidate_id"], int(row["horizon_min"]), row["trade_time"]): row
        for row in legacy_rows
    }
    if len(legacy) != len(legacy_rows):
        raise RuntimeError("legacy reference contains duplicate candidate/horizon/trade_time keys")

    cuts = np.flatnonzero(np.r_[True, block.time_ids[1:] != block.time_ids[:-1], True]).astype(np.int64)
    starts, ends = cuts[:-1], cuts[1:]
    new_keys: set[tuple[str, int, str]] = set()
    mismatch_counts = {
        "missing_in_legacy": 0,
        "missing_in_streaming": 0,
        "identity": 0,
        "count": 0,
        "numeric": 0,
        "signal_spread": 0,
    }
    mismatch_samples: list[dict[str, Any]] = []
    max_numeric_error: dict[str, float] = {}
    digest = hashlib.sha256()
    coordinate_rows_compared = 0
    numeric_fields = (
        "raw_return",
        "market_mean_return",
        "trading_cost",
        "net_return",
        "one_way_turnover",
        "rank_ic",
        "rank_ic_raw",
    )
    for candidate_index, candidate in enumerate(candidates):
        candidate_id = str(candidate["candidate_id"])
        direction = float(directions[candidate_index])
        for horizon_index, horizon in enumerate(horizons):
            label = block.labels[horizon]
            for group, (start, end) in enumerate(zip(starts, ends)):
                coordinate = portfolio.audit_coordinate_metrics[candidate_index, horizon_index, group]
                if not np.isfinite(coordinate[0]):
                    continue
                trade_time = pd.Timestamp(int(block.trade_times_ns[start]), unit="ns").isoformat()
                key = _key(candidate_id, horizon, trade_time)
                new_keys.add(key)
                expected_row = legacy.get(key)
                if expected_row is None:
                    mismatch_counts["missing_in_legacy"] += 1
                    if len(mismatch_samples) < 20:
                        mismatch_samples.append({"key": key, "reason": "missing_in_legacy"})
                    continue
                local_signal = signals[candidate_index, start:end]
                local_label = label[start:end]
                valid = np.isfinite(local_signal) & np.isfinite(local_label)
                eligible_codes = sorted({symbols[int(value)] for value in block.code_ids[start:end][valid]})
                selected_mask = portfolio.audit_selected[candidate_index, horizon_index, start:end]
                selected_codes = sorted({symbols[int(value)] for value in block.code_ids[start:end][selected_mask]})
                observed_row: dict[str, Any] = {
                    "eligible_code_count": len(eligible_codes),
                    "eligible_code_identity": stable_hash(eligible_codes),
                    "selected_code_identity": stable_hash({"long": selected_codes, "short": []}),
                    "portfolio_weight_identity": stable_hash(
                        {
                            "long": [(code, 1.0 / max(1, len(selected_codes))) for code in selected_codes],
                            "short": [],
                            "portfolio_mode": str(args.portfolio_mode),
                        }
                    ),
                    "long_count": int(selected_mask.sum()),
                    "short_count": 0,
                    "raw_return": float(coordinate[0]),
                    "net_return": float(coordinate[1]),
                    "one_way_turnover": float(coordinate[2]),
                    "rank_ic": float(coordinate[3]) if np.isfinite(coordinate[3]) else None,
                    "rank_ic_raw": float(coordinate[3] / direction) if np.isfinite(coordinate[3]) else None,
                    "market_mean_return": float(portfolio.audit_mapping_metrics[candidate_index, horizon_index, group, 2]),
                    "trading_cost": float(coordinate[0] - coordinate[1]),
                    "signal_spread": float(portfolio.audit_mapping_metrics[candidate_index, horizon_index, group, 6]),
                }
                row_mismatches: list[str] = []
                for field in ("eligible_code_identity", "selected_code_identity", "portfolio_weight_identity"):
                    if str(observed_row[field]) != str(expected_row[field]):
                        mismatch_counts["identity"] += 1
                        row_mismatches.append(field)
                for field in ("eligible_code_count", "long_count", "short_count"):
                    if int(observed_row[field]) != int(expected_row[field]):
                        mismatch_counts["count"] += 1
                        row_mismatches.append(field)
                for field in numeric_fields:
                    matches, error = _numeric_equal(
                        _optional_float(observed_row[field]),
                        _optional_float(expected_row.get(field)),
                        float(args.numeric_tolerance),
                    )
                    max_numeric_error[field] = max(max_numeric_error.get(field, 0.0), error)
                    if not matches:
                        mismatch_counts["numeric"] += 1
                        row_mismatches.append(field)
                expected_spread = abs(float(expected_row["top_signal_mean"]) - float(expected_row["bottom_signal_mean"]))
                spread_matches, spread_error = _numeric_equal(
                    observed_row["signal_spread"], expected_spread, float(args.signal_tolerance)
                )
                max_numeric_error["signal_spread"] = max(max_numeric_error.get("signal_spread", 0.0), spread_error)
                if not spread_matches:
                    mismatch_counts["signal_spread"] += 1
                    row_mismatches.append("signal_spread")
                if row_mismatches and len(mismatch_samples) < 20:
                    mismatch_samples.append(
                        {"key": key, "fields": tuple(row_mismatches), "observed": observed_row, "legacy": expected_row}
                    )
                digest.update(
                    json.dumps(
                        {"key": key, **observed_row},
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                )
                coordinate_rows_compared += 1

    missing_streaming = sorted(set(legacy) - new_keys)
    mismatch_counts["missing_in_streaming"] = len(missing_streaming)
    for key in missing_streaming[: max(0, 20 - len(mismatch_samples))]:
        mismatch_samples.append({"key": key, "reason": "missing_in_streaming"})
    mismatch_total = sum(mismatch_counts.values())
    report = {
        "schema_version": "cn_phase3cm_streaming_full_coordinate_parity_v1",
        "status": "FULL_COORDINATE_PARITY_PASS" if mismatch_total == 0 else "FULL_COORDINATE_PARITY_FAIL",
        "candidate_count": len(candidates),
        "pair_count": int(args.pair_count),
        "horizons": list(horizons),
        "field_sidecar_shards": len(field_paths),
        "label_sidecar_shards": len(label_paths),
        "input_rows": block.row_count,
        "trade_time_count": block.trade_time_count,
        "legacy_coordinate_rows": len(legacy_rows),
        "streaming_coordinate_rows": len(new_keys),
        "coordinate_rows_compared": coordinate_rows_compared,
        "mismatch_counts": mismatch_counts,
        "mismatch_total": mismatch_total,
        "mismatch_samples": mismatch_samples,
        "numeric_tolerance": float(args.numeric_tolerance),
        "signal_tolerance": float(args.signal_tolerance),
        "max_numeric_error": max_numeric_error,
        "streaming_coordinate_digest": digest.hexdigest(),
        "formal_coordinate_rows_retained": 0,
        "audit_coordinate_arrays_ephemeral": True,
        "expression_audit": expression.audit,
        "portfolio_audit": portfolio.audit,
        "data_role": "development_train_only",
        "split_manifest_hash": args.split_manifest_hash,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "thread_environment": expected,
    }
    args.output.resolve().parent.mkdir(parents=True, exist_ok=True)
    args.output.resolve().write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({key: report[key] for key in ("status", "coordinate_rows_compared", "mismatch_total")}, sort_keys=True))
    return 0 if mismatch_total == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
