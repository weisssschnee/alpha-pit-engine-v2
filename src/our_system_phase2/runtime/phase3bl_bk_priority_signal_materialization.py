"""Materialize true-1min signal vectors for Phase3BK replay-priority candidates.

Phase3BK reduced the Phase3BJ broad search top64 to a small priority set, but
only with metric-vector clustering. This stage reads true 1min shards, computes
the selected expressions on contiguous minute windows, and audits actual signal
vector correlation plus wrong-lag diagnostics.

This remains diagnostic-only. It does not compare against or modify X0/R3.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from our_system_phase2.services.real_market_validation import evaluate_panel_expression, fast_rank_pct_by_group


REPO = Path(__file__).resolve().parents[3]
DEFAULT_BK_AUDIT = Path("reports/phase3bk_bj_top64_strict_audit_20260615/phase3bk_bj_top64_candidate_audit.csv")
DEFAULT_SHARD_ROOT = Path("runtime/phase3au_aq_only_true1min_sharded_20260611")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3bl_bk_priority_signal_materialization_20260615")
DEFAULT_REPORT_ROOT = Path("reports/phase3bl_bk_priority_signal_materialization_20260615")
FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
WINDOW_OPS = {"mean", "std", "delta", "delay", "mom", "wma", "med", "kurt", "skew", "corr", "cov"}
DEFAULT_RELATION_WINDOW = 20
TYPED_WINDOW_ARG_INDEX = {
    "eventcount": 1,
    "statedwell": 1,
    "windowstatecount": 1,
    "validratiogate": 1,
    "maskedzscore": 1,
    "maskedcorr": 2,
}
CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
NUMERIC_ARG_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _fmt(row.get(key, "")) for key in fieldnames or []})


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _fmt(value: Any) -> str:
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        return f"{value:.10g}"
    if isinstance(value, bool):
        return "true" if value else "false"
    return "" if value is None else str(value)


def _f(value: Any, default: float = float("nan")) -> float:
    try:
        if value in (None, ""):
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def _fields(expression: str) -> list[str]:
    return sorted(set(FIELD_RE.findall(expression or "")))


def _find_matching_paren(text: str, open_idx: int) -> int:
    depth = 0
    for idx in range(open_idx, len(text)):
        char = text[idx]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return idx
    return -1


def _split_top_level_args(text: str) -> list[str]:
    args: list[str] = []
    start = 0
    depth = 0
    for idx, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            args.append(text[start:idx].strip())
            start = idx + 1
    tail = text[start:].strip()
    if tail or text:
        args.append(tail)
    return args


def _max_expression_window(expression: str) -> int:
    expr = expression or ""
    windows: list[int] = []
    for match in CALL_RE.finditer(expr):
        op = match.group(1).lower()
        if op not in WINDOW_OPS and op not in TYPED_WINDOW_ARG_INDEX:
            continue
        open_idx = match.end() - 1
        close_idx = _find_matching_paren(expr, open_idx)
        if close_idx < 0:
            continue
        args = _split_top_level_args(expr[open_idx + 1 : close_idx])
        if op in {"corr", "cov"} and len(args) == 2:
            windows.append(DEFAULT_RELATION_WINDOW)
            continue
        arg_index = TYPED_WINDOW_ARG_INDEX.get(op)
        if arg_index is None:
            arg_index = len(args) - 1
        if arg_index < 0:
            arg_index = len(args) + arg_index
        if arg_index < 0 or arg_index >= len(args):
            continue
        arg = args[arg_index]
        if NUMERIC_ARG_RE.fullmatch(arg):
            windows.append(int(float(arg)))
    return max(windows, default=0)


def _discover_panels(shard_root: Path, max_shards: int | None) -> list[Path]:
    panels = sorted(shard_root.glob("shard_*/phase3aq_wide_true1min/canary/phase3aq_true_1min_formula_canary.parquet"))
    if max_shards is not None and max_shards > 0:
        panels = panels[:max_shards]
    if not panels:
        raise FileNotFoundError(f"no true-1min shard panels under {shard_root}")
    return panels


def _load_priority_candidates(path: Path, limit: int | None) -> list[dict[str, Any]]:
    rows = [row for row in _read_csv(path) if str(row.get("decision_tier", "")).startswith("bk_replay_priority")]
    rows.sort(key=lambda row: int(float(row.get("phase3bk_rank") or 999999)))
    if limit is not None and limit > 0:
        rows = rows[:limit]
    if not rows:
        raise RuntimeError(f"no bk_replay_priority rows found in {path}")
    out: list[dict[str, Any]] = []
    for row in rows:
        expression = row.get("expression") or ""
        item = dict(row)
        item["expression_hash"] = row.get("expression_hash") or _stable_hash(expression)
        item["fields_list"] = _fields(expression)
        item["max_window"] = _max_expression_window(expression)
        out.append(item)
    return out


def _panel_trade_times(panel_path: Path) -> pd.Series:
    parquet = pq.ParquetFile(panel_path)
    chunks: list[pd.Series] = []
    for row_group in range(parquet.num_row_groups):
        table = parquet.read_row_group(row_group, columns=["trade_time"])
        unique = pc.unique(table["trade_time"].combine_chunks()).to_pandas()
        chunks.append(pd.Series(pd.to_datetime(unique, errors="coerce")).dropna())
    return pd.concat(chunks, ignore_index=True).drop_duplicates().sort_values(ignore_index=True)


def _sample_positions(count: int, sample_count: int | None) -> np.ndarray:
    if sample_count is None or sample_count <= 0 or count <= sample_count:
        return np.arange(count, dtype=int)
    return np.unique(np.linspace(0, count - 1, sample_count).round().astype(int))


def _read_windowed_panel(
    panel_path: Path,
    *,
    columns: list[str],
    signal_time_count: int | None,
    lookback: int,
    max_horizon: int,
) -> tuple[pd.DataFrame, set[pd.Timestamp], int, int]:
    trade_times = _panel_trade_times(panel_path)
    signal_positions = _sample_positions(len(trade_times), signal_time_count)
    read_positions: set[int] = set()
    for pos in signal_positions:
        start = max(0, int(pos) - lookback)
        end = min(len(trade_times) - 1, int(pos) + max_horizon)
        read_positions.update(range(start, end + 1))
    signal_times = set(pd.to_datetime(trade_times.iloc[signal_positions]).tolist())
    read_times = set(pd.to_datetime(trade_times.iloc[sorted(read_positions)]).tolist())

    parquet = pq.ParquetFile(panel_path)
    trade_time_type = parquet.schema_arrow.field("trade_time").type
    value_set = pa.array(pd.to_datetime(sorted(read_times)).to_numpy(dtype="datetime64[ns]"))
    if not value_set.type.equals(trade_time_type):
        value_set = value_set.cast(trade_time_type)
    tables: list[pa.Table] = []
    for row_group in range(parquet.num_row_groups):
        table = parquet.read_row_group(row_group, columns=columns)
        mask = pc.is_in(table["trade_time"], value_set=value_set)
        filtered = table.filter(mask)
        if filtered.num_rows:
            tables.append(filtered)
    if not tables:
        return pd.DataFrame(columns=columns), signal_times, len(signal_times), len(read_times)
    frame = pa.concat_tables(tables, promote_options="default").to_pandas()
    return frame, signal_times, len(signal_times), len(read_times)


def _future_returns(frame: pd.DataFrame, horizons: tuple[int, ...]) -> pd.DataFrame:
    close = pd.to_numeric(frame["close"], errors="coerce")
    grouped = close.groupby(frame["code"], sort=False)
    labels = pd.DataFrame(index=frame.index)
    for horizon in horizons:
        labels[f"fwd_ret_{horizon}m"] = grouped.shift(-horizon) / close.replace(0, np.nan) - 1.0
    return labels


def _rank_by_group(values: pd.Series, group: pd.Series) -> pd.Series:
    return fast_rank_pct_by_group(values, group)


def _pearson(x: pd.Series, y: pd.Series) -> float | None:
    x_arr = pd.to_numeric(x, errors="coerce").to_numpy(dtype=float)
    y_arr = pd.to_numeric(y, errors="coerce").to_numpy(dtype=float)
    mask = np.isfinite(x_arr) & np.isfinite(y_arr)
    if int(mask.sum()) < 5:
        return None
    xv = x_arr[mask]
    yv = y_arr[mask]
    if np.nanstd(xv) == 0.0 or np.nanstd(yv) == 0.0:
        return None
    return float(np.corrcoef(xv, yv)[0, 1])


def _mean_ic(signal_rank: pd.Series, label_rank: pd.Series, group: pd.Series, min_obs: int) -> dict[str, Any]:
    vals: list[float] = []
    for _, idx in group.groupby(group, sort=False).groups.items():
        if len(idx) < min_obs:
            continue
        corr = _pearson(signal_rank.loc[idx], label_rank.loc[idx])
        if corr is not None and math.isfinite(corr):
            vals.append(float(corr))
    if not vals:
        return {"ic_mean": None, "ic_abs_mean": None, "ic_count": 0, "ic_hit_rate": None}
    arr = np.asarray(vals, dtype=float)
    return {
        "ic_mean": float(arr.mean()),
        "ic_abs_mean": float(np.abs(arr).mean()),
        "ic_count": int(len(arr)),
        "ic_hit_rate": float(np.mean(arr > 0.0)),
    }


def _spread(signal_rank: pd.Series, label: pd.Series, group: pd.Series, min_obs: int) -> dict[str, Any]:
    vals: list[float] = []
    for _, idx in group.groupby(group, sort=False).groups.items():
        if len(idx) < min_obs:
            continue
        ranks = pd.to_numeric(signal_rank.loc[idx], errors="coerce")
        y = pd.to_numeric(label.loc[idx], errors="coerce")
        top = y[ranks >= 0.8]
        bottom = y[ranks <= 0.2]
        if top.empty or bottom.empty:
            continue
        spread = float(top.mean() - bottom.mean())
        if math.isfinite(spread):
            vals.append(spread)
    if not vals:
        return {"spread_mean": None, "spread_abs_mean": None, "spread_count": 0, "spread_hit_rate": None}
    arr = np.asarray(vals, dtype=float)
    return {
        "spread_mean": float(arr.mean()),
        "spread_abs_mean": float(np.abs(arr).mean()),
        "spread_count": int(len(arr)),
        "spread_hit_rate": float(np.mean(arr > 0.0)),
    }


def _turnover(signal_rank: pd.Series, frame: pd.DataFrame) -> dict[str, Any]:
    work = pd.DataFrame({"code": frame["code"].astype(str), "trade_time": frame["trade_time"], "rank": signal_rank})
    previous_top: set[str] | None = None
    previous_bottom: set[str] | None = None
    turns: list[float] = []
    for _, block in work.dropna().groupby("trade_time", sort=True):
        top = set(block.loc[block["rank"] >= 0.8, "code"])
        bottom = set(block.loc[block["rank"] <= 0.2, "code"])
        if previous_top is not None and previous_bottom is not None and top and bottom:
            top_turn = 1.0 - (len(top & previous_top) / max(1, len(top)))
            bottom_turn = 1.0 - (len(bottom & previous_bottom) / max(1, len(bottom)))
            turns.append((top_turn + bottom_turn) / 2.0)
        previous_top = top
        previous_bottom = bottom
    if not turns:
        return {"mean_one_way_turnover": None, "turnover_count": 0}
    return {"mean_one_way_turnover": float(np.mean(turns)), "turnover_count": int(len(turns))}


def _candidate_direction(row: dict[str, Any]) -> int:
    ic = _f(row.get("best_ic_mean_median"), 0.0)
    return 1 if ic >= 0 else -1


def _score_row(row: dict[str, Any], *, direction: int, horizon: int) -> dict[str, Any]:
    ic = row.get("ic_mean")
    spread = row.get("spread_mean")
    aligned_ic = None if ic is None else float(ic) * direction
    aligned_spread = None if spread is None else float(spread) * direction
    blocker_flags: list[str] = []
    if aligned_ic is None or aligned_ic <= 0.0:
        blocker_flags.append("primary_direction_not_reproduced")
    if row.get("wrong_lag_future_ic_mean") is not None and aligned_ic is not None:
        if abs(float(row["wrong_lag_future_ic_mean"])) >= abs(aligned_ic) * 0.9:
            blocker_flags.append("future_signal_wrong_lag_too_strong")
    if row.get("mean_one_way_turnover") is not None and float(row["mean_one_way_turnover"]) > 0.95:
        blocker_flags.append("extreme_minute_turnover")
    return {
        **row,
        "expected_direction": direction,
        "primary_horizon_min": horizon,
        "aligned_ic_mean": aligned_ic,
        "aligned_spread_mean": aligned_spread,
        "blocker_flags": "|".join(blocker_flags),
        "phase3bl_decision": "bl_signal_materialized_pass" if not blocker_flags else "bl_signal_materialized_watch",
    }


def _candidate_pairwise(signal_vectors: dict[str, pd.Series]) -> list[dict[str, Any]]:
    keys = sorted(signal_vectors)
    rows: list[dict[str, Any]] = []
    for i, left in enumerate(keys):
        for right in keys[i + 1 :]:
            corr = _pearson(signal_vectors[left], signal_vectors[right])
            rows.append(
                {
                    "left_expression_hash": left,
                    "right_expression_hash": right,
                    "signal_rank_corr": corr,
                    "abs_signal_rank_corr": None if corr is None else abs(float(corr)),
                }
            )
    rows.sort(key=lambda row: _f(row.get("abs_signal_rank_corr"), -1.0), reverse=True)
    return rows


def _run_materialization(
    *,
    candidates: list[dict[str, Any]],
    panels: list[Path],
    horizons: tuple[int, ...],
    sample_trade_times_per_shard: int | None,
    min_obs_per_time: int,
    pairwise_candidate_limit: int = 96,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    fields = sorted({field for row in candidates for field in row["fields_list"]})
    max_window = max((int(row["max_window"]) for row in candidates), default=0)
    max_horizon = max(horizons)
    required = {
        "code",
        "trade_time",
        "date",
        "close",
        "volume",
        "vol",
        "amount",
        "amount_yuan",
        "vwap",
        *fields,
    }

    metric_rows: list[dict[str, Any]] = []
    pairwise_limit = max(0, int(pairwise_candidate_limit))
    pairwise_hashes = {
        str(row["expression_hash"])
        for row in (candidates[:pairwise_limit] if pairwise_limit > 0 else [])
    }
    audit_meta = {
        "panel_count": len(panels),
        "panel_paths": [str(path) for path in panels],
        "read_fields": fields,
        "max_expression_window": max_window,
        "max_horizon": max_horizon,
        "sample_trade_times_per_shard": sample_trade_times_per_shard,
        "pairwise_candidate_limit": pairwise_limit,
        "pairwise_candidate_count": len(pairwise_hashes),
        "shards": [],
    }
    per_candidate_metric_accum: dict[tuple[str, int], list[dict[str, Any]]] = {}
    per_candidate_signal_chunks: dict[str, list[pd.Series]] = {expr_hash: [] for expr_hash in pairwise_hashes}

    def emit_progress(event: dict[str, Any]) -> None:
        if progress_callback is None:
            return
        try:
            progress_callback(event)
        except Exception:
            # Progress reporting must never change materialization semantics.
            pass

    for shard_index, panel in enumerate(panels):
        emit_progress(
            {
                "stage": "read_panel_start",
                "shard_index": shard_index,
                "panel_path": str(panel),
                "candidate_count": len(candidates),
                "completed_candidate_count": 0,
            }
        )
        schema = set(pq.ParquetFile(panel).schema_arrow.names)
        columns = [column for column in sorted(required) if column in schema]
        missing = sorted({"code", "trade_time", "date", "close"} - set(columns))
        if missing:
            raise RuntimeError(f"{panel} missing required columns {missing}")
        missing_fields = sorted(set(fields) - set(columns))
        if missing_fields:
            raise RuntimeError(f"{panel} missing candidate fields {missing_fields}")

        frame, signal_times, signal_time_count, read_time_count = _read_windowed_panel(
            panel,
            columns=columns,
            signal_time_count=sample_trade_times_per_shard,
            lookback=max_window,
            max_horizon=max_horizon,
        )
        frame["code"] = frame["code"].astype(str)
        frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        # The true minute evaluator groups CSRank/ZScore by `date`; these panels
        # intentionally use date as a trade_time-compatible clock column.
        frame = frame.dropna(subset=["code", "trade_time", "date", "close"]).sort_values(["code", "trade_time"]).reset_index(drop=True)
        eval_mask = frame["trade_time"].isin(signal_times)
        eval_frame = frame.loc[eval_mask].copy().reset_index(drop=True)
        labels_all = _future_returns(frame, horizons)
        labels = labels_all.loc[eval_mask].reset_index(drop=True)
        label_ranks = {h: _rank_by_group(labels[f"fwd_ret_{h}m"], eval_frame["trade_time"]) for h in horizons}

        shard_meta = {
            "shard_index": shard_index,
            "panel_path": str(panel),
            "read_rows": int(len(frame)),
            "eval_rows": int(len(eval_frame)),
            "signal_trade_time_count": signal_time_count,
            "read_trade_time_count": read_time_count,
            "code_count": int(eval_frame["code"].nunique()),
        }
        audit_meta["shards"].append(shard_meta)
        emit_progress({"stage": "read_panel_complete", **shard_meta, "candidate_count": len(candidates), "completed_candidate_count": 0})

        for candidate_index, candidate in enumerate(candidates, 1):
            expr_hash = str(candidate["expression_hash"])
            expression = str(candidate.get("expression") or "")
            signal_all = pd.to_numeric(evaluate_panel_expression(frame, expression, cache={}), errors="coerce")
            signal = signal_all.loc[eval_mask].reset_index(drop=True)
            signal_rank = _rank_by_group(signal, eval_frame["trade_time"])
            signal_prev = pd.Series(signal_all.groupby(frame["code"], sort=False).shift(1).loc[eval_mask].to_numpy())
            signal_future = pd.Series(signal_all.groupby(frame["code"], sort=False).shift(-1).loc[eval_mask].to_numpy())
            prev_rank = _rank_by_group(signal_prev, eval_frame["trade_time"])
            future_rank = _rank_by_group(signal_future, eval_frame["trade_time"])

            if expr_hash in per_candidate_signal_chunks:
                per_candidate_signal_chunks[expr_hash].append(pd.Series(signal_rank.to_numpy(dtype=float)))

            for horizon in horizons:
                row = {
                    "expression_hash": expr_hash,
                    "candidate_id": candidate.get("candidate_ids", ""),
                    "phase3bk_rank": candidate.get("phase3bk_rank", ""),
                    "horizon_min": horizon,
                    "shard_index": shard_index,
                    "signal_nonnull": int(signal.notna().sum()),
                    "signal_unique": int(signal.nunique(dropna=True)),
                    "eval_rows": int(len(eval_frame)),
                    "eval_trade_times": int(eval_frame["trade_time"].nunique()),
                }
                row.update(_mean_ic(signal_rank, label_ranks[horizon], eval_frame["trade_time"], min_obs_per_time))
                row.update(_spread(signal_rank, labels[f"fwd_ret_{horizon}m"], eval_frame["trade_time"], min_obs_per_time))
                prev_ic = _mean_ic(prev_rank, label_ranks[horizon], eval_frame["trade_time"], min_obs_per_time)
                future_ic = _mean_ic(future_rank, label_ranks[horizon], eval_frame["trade_time"], min_obs_per_time)
                row["wrong_lag_prev_ic_mean"] = prev_ic.get("ic_mean")
                row["wrong_lag_future_ic_mean"] = future_ic.get("ic_mean")
                row.update(_turnover(signal_rank, eval_frame))
                per_candidate_metric_accum.setdefault((expr_hash, horizon), []).append(row)
                metric_rows.append(row)
            if candidate_index == 1 or candidate_index % 10 == 0 or candidate_index == len(candidates):
                emit_progress(
                    {
                        "stage": "candidate_eval_progress",
                        "shard_index": shard_index,
                        "panel_path": str(panel),
                        "candidate_count": len(candidates),
                        "completed_candidate_count": candidate_index,
                        "metric_rows": len(metric_rows),
                    }
                )

    emit_progress({"stage": "aggregate_start", "candidate_count": len(candidates), "metric_rows": len(metric_rows)})
    aggregate_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        expr_hash = str(candidate["expression_hash"])
        direction = _candidate_direction(candidate)
        primary_horizon = int(float(candidate.get("best_horizon_min") or 0))
        for horizon in horizons:
            rows = per_candidate_metric_accum.get((expr_hash, horizon), [])
            if not rows:
                continue

            def mean_col(name: str) -> float | None:
                vals = [_f(row.get(name), float("nan")) for row in rows]
                vals = [val for val in vals if math.isfinite(val)]
                return float(np.mean(vals)) if vals else None

            out = {
                "expression_hash": expr_hash,
                "candidate_id": candidate.get("candidate_ids", ""),
                "phase3bk_rank": candidate.get("phase3bk_rank", ""),
                "factor_lane": candidate.get("factor_lane", ""),
                "fields": candidate.get("fields", ""),
                "expression": candidate.get("expression", ""),
                "horizon_min": horizon,
                "shard_count": len(rows),
                "ic_mean": mean_col("ic_mean"),
                "ic_abs_mean": mean_col("ic_abs_mean"),
                "ic_hit_rate": mean_col("ic_hit_rate"),
                "spread_mean": mean_col("spread_mean"),
                "spread_abs_mean": mean_col("spread_abs_mean"),
                "spread_hit_rate": mean_col("spread_hit_rate"),
                "wrong_lag_prev_ic_mean": mean_col("wrong_lag_prev_ic_mean"),
                "wrong_lag_future_ic_mean": mean_col("wrong_lag_future_ic_mean"),
                "mean_one_way_turnover": mean_col("mean_one_way_turnover"),
                "signal_unique_min": min(int(row.get("signal_unique") or 0) for row in rows),
                "signal_nonnull_sum": sum(int(row.get("signal_nonnull") or 0) for row in rows),
                "eval_rows_sum": sum(int(row.get("eval_rows") or 0) for row in rows),
            }
            aggregate_rows.append(_score_row(out, direction=direction, horizon=primary_horizon))

    signal_vectors = {
        expr_hash: pd.concat(chunks, ignore_index=True, copy=False)
        for expr_hash, chunks in per_candidate_signal_chunks.items()
        if chunks
    }
    emit_progress({"stage": "pairwise_start", "signal_vector_count": len(signal_vectors), "aggregate_rows": len(aggregate_rows)})
    pairwise_rows = _candidate_pairwise(signal_vectors)
    emit_progress({"stage": "complete", "metric_rows": len(metric_rows), "aggregate_rows": len(aggregate_rows), "pairwise_rows": len(pairwise_rows)})
    return metric_rows, aggregate_rows, {**audit_meta, "pairwise_rows": pairwise_rows}


def _render_md(summary: dict[str, Any], aggregate_rows: list[dict[str, Any]], pairwise_rows: list[dict[str, Any]]) -> str:
    primary = [row for row in aggregate_rows if int(row.get("horizon_min") or -1) == int(row.get("primary_horizon_min") or -2)]
    primary.sort(key=lambda row: int(float(row.get("phase3bk_rank") or 999999)))
    lines = [
        "# Phase3BL BK Priority Signal Materialization 2026-06-15",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Scope",
        "",
        f"- true-1min shard panels: `{summary['panel_count']}`",
        f"- priority candidates: `{summary['candidate_count']}`",
        f"- sampled signal trade_times per shard: `{summary['sample_trade_times_per_shard']}`",
        f"- total eval rows: `{summary['total_eval_rows']}`",
        f"- signal-vector pair count: `{summary['pairwise_count']}`",
        "",
        "## Boundary",
        "",
        "- Uses true `trade_time` 1min panels from Phase3AU AQ shards.",
        "- Reads contiguous minute warmup windows before sampled signal times.",
        "- Does not modify X0/R3 and does not promote candidates.",
        "- Old 149 signal caches are not same-domain minute vectors; this stage only proves candidate-to-candidate minute-vector crowding.",
        "",
        "## Primary Horizon Results",
        "",
        "| bk_rank | h | lane | fields | aligned_ic | spread | turnover | wrong_lag_future_ic | decision | blockers |",
        "|---:|---:|---|---|---:|---:|---:|---:|---|---|",
    ]
    for row in primary:
        lines.append(
            f"| {row['phase3bk_rank']} | {row['horizon_min']} | `{row['factor_lane']}` | `{row['fields']}` | "
            f"{_fmt(row.get('aligned_ic_mean'))} | {_fmt(row.get('aligned_spread_mean'))} | "
            f"{_fmt(row.get('mean_one_way_turnover'))} | {_fmt(row.get('wrong_lag_future_ic_mean'))} | "
            f"`{row['phase3bl_decision']}` | `{row.get('blocker_flags') or ''}` |"
        )
    lines.extend(["", "## Pairwise Signal Rank Correlation", "", "| left | right | corr |", "|---|---|---:|"])
    for row in pairwise_rows:
        lines.append(f"| `{row['left_expression_hash']}` | `{row['right_expression_hash']}` | {_fmt(row.get('signal_rank_corr'))} |")
    lines.extend(
        [
            "",
            "## Next Gate",
            "",
            "If primary-horizon aligned IC survives and pairwise crowding is acceptable, run a wider materialization on more signal times or a full replay shard job. If not, redirect search budget away from this crowded `volume/vwap/close` family.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bk-audit", type=Path, default=DEFAULT_BK_AUDIT)
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-shards", type=int, default=16)
    parser.add_argument("--priority-limit", type=int, default=4)
    parser.add_argument("--sample-trade-times-per-shard", type=int, default=480)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs-per-time", type=int, default=20)
    args = parser.parse_args(argv)

    bk_audit = _resolve(args.bk_audit)
    shard_root = _resolve(args.shard_root)
    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    horizons = tuple(int(item.strip()) for item in str(args.horizons).split(",") if item.strip())
    candidates = _load_priority_candidates(bk_audit, args.priority_limit)
    panels = _discover_panels(shard_root, args.max_shards)
    metric_rows, aggregate_rows, meta = _run_materialization(
        candidates=candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
    )
    pairwise_rows = meta.pop("pairwise_rows")
    total_eval_rows = sum(int(shard.get("eval_rows") or 0) for shard in meta["shards"])
    primary_rows = [row for row in aggregate_rows if int(row.get("horizon_min") or -1) == int(row.get("primary_horizon_min") or -2)]
    pass_count = sum(1 for row in primary_rows if row.get("phase3bl_decision") == "bl_signal_materialized_pass")
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3BL_SIGNAL_MATERIALIZATION_COMPLETE_DIAGNOSTIC_ONLY",
        "bk_audit": str(bk_audit),
        "shard_root": str(shard_root),
        "output_root": str(output_root),
        "report_root": str(report_root),
        "candidate_count": len(candidates),
        "panel_count": len(panels),
        "sample_trade_times_per_shard": args.sample_trade_times_per_shard,
        "horizons_min": list(horizons),
        "total_eval_rows": total_eval_rows,
        "pairwise_count": len(pairwise_rows),
        "primary_pass_count": pass_count,
        "same_domain_149_recluster_status": "NOT_DONE_OLD_149_CACHE_IS_NOT_TRUE_1MIN_TRADE_TIME_DOMAIN",
        "hard_boundary": [
            "true trade_time minute panels only",
            "contiguous warmup windows are read before sampled signal times",
            "candidate-to-candidate signal-vector clustering only",
            "X0/R3 read-only; no promotion decision",
        ],
        **meta,
    }

    _write_csv(output_root / "phase3bl_candidate_horizon_shard_metrics.csv", metric_rows)
    _write_csv(output_root / "phase3bl_candidate_horizon_aggregate.csv", aggregate_rows)
    _write_csv(output_root / "phase3bl_pairwise_signal_rank_corr.csv", pairwise_rows)
    _write_json(output_root / "phase3bl_signal_materialization_summary.json", summary)
    _write_json(report_root / "phase3bl_signal_materialization_summary.json", {**summary, "primary_rows": primary_rows})
    _write_csv(report_root / "phase3bl_candidate_horizon_aggregate.csv", aggregate_rows)
    _write_csv(report_root / "phase3bl_pairwise_signal_rank_corr.csv", pairwise_rows)
    (report_root / "PHASE3BL_BK_PRIORITY_SIGNAL_MATERIALIZATION_20260615.md").write_text(
        _render_md(summary, aggregate_rows, pairwise_rows),
        encoding="utf-8",
    )
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
