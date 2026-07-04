"""Train-set portfolio Sortino reward audit for true-1min candidates.

Phase3BZ fragment replay is useful as a diagnostic slice replay, but it is not
the right optimization target for CEM/UCB. This module builds a continuous
minute portfolio PnL curve on true trade_time shards and reports train /
validation / holdout Sortino-style reward fields for later search feedback.

This route is diagnostic-only. It does not generate candidates, launch search,
or modify X0/R3.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.metadata
import json
import math
import os
import random
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

try:
    from numba import njit
except Exception:  # pragma: no cover - numba is an optional acceleration dependency.
    njit = None

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _discover_panels,
    _fields,
    _future_returns,
    _max_expression_window,
    _panel_trade_times,
    _rank_by_group,
    _read_windowed_panel,
    _write_csv,
    _write_json,
)
from our_system_phase2.services.legacy_field_aliases import rewrite_legacy_field_aliases, rewrite_summary
from our_system_phase2.services.candidate_schema import OPTIMIZER_REWARD_METRIC, normalize_candidate_schema
from our_system_phase2.services.real_market_validation import evaluate_panel_expression


REPO = Path(__file__).resolve().parents[3]
DEFAULT_CANDIDATE_AUDIT = Path("reports/phase3cl_bz_candidate_audit_20260622/phase3ca_bz_candidate_audit.csv")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cm_train_portfolio_sortino_reward_audit_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3cm_train_portfolio_sortino_reward_audit_20260623")
HARD_INPUT_BLOCKER_TOKENS = (
    "future_signal_wrong_lag_too_strong",
    "candidate_uses_blocked_or_future_fields",
    "candidate_missing_schema_fields",
    "missing_schema",
    "blocked_or_future",
)
_NUMBA_RANK_RUNTIME_DISABLED = False


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _sample_positions_for_count(count: int, sample_count: int | None) -> np.ndarray:
    if sample_count is None or int(sample_count) <= 0 or count <= int(sample_count):
        return np.arange(count, dtype=int)
    return np.unique(np.linspace(0, count - 1, int(sample_count)).round().astype(int))


def _read_windowed_panel_positions(
    panel_path: Path,
    *,
    columns: list[str],
    all_trade_times: pd.Series,
    signal_positions: list[int],
    lookback: int,
    max_horizon: int,
) -> tuple[pd.DataFrame, set[pd.Timestamp], int, int]:
    if not signal_positions:
        return pd.DataFrame(columns=columns), set(), 0, 0
    trade_times = pd.to_datetime(all_trade_times)
    read_positions: set[int] = set()
    max_pos = len(trade_times) - 1
    for pos in signal_positions:
        start = max(0, int(pos) - int(lookback))
        end = min(max_pos, int(pos) + int(max_horizon))
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
    return pa.concat_tables(tables, promote_options="default").to_pandas(), signal_times, len(signal_times), len(read_times)


def _f(value: Any, default: float = float("nan")) -> float:
    try:
        if value in (None, ""):
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _round(value: Any, ndigits: int = 8) -> float | None:
    val = _f(value)
    if not math.isfinite(val):
        return None
    return round(val, ndigits)


def _sortino(values: list[float], annualizer: float = 1.0) -> float | None:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if not clean:
        return None
    mean = statistics.fmean(clean)
    downside = [min(0.0, value) for value in clean]
    downside_var = statistics.fmean([value * value for value in downside])
    if downside_var <= 1e-18:
        return None
    return mean / math.sqrt(downside_var) * math.sqrt(annualizer)


def _max_drawdown(values: list[float]) -> float | None:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if not clean:
        return None
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for value in clean:
        equity *= 1.0 + value
        peak = max(peak, equity)
        if peak > 0:
            max_dd = min(max_dd, equity / peak - 1.0)
    return max_dd


def _quantile(values: list[float], q: float) -> float | None:
    clean = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    pos = (len(clean) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return clean[lo]
    return clean[lo] * (hi - pos) + clean[hi] * (pos - lo)


def _safe_stdev(values: list[float]) -> float | None:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if len(clean) < 2:
        return None
    return float(statistics.stdev(clean))


def _bounded(value: float, cap: float) -> float:
    cap = max(0.0, float(cap))
    if not math.isfinite(value):
        return 0.0
    return max(-cap, min(cap, value))


class _BoundedSeriesCache(dict[str, pd.Series]):
    """Small in-memory cache for expression operator subtrees.

    The evaluator stores every recursively evaluated subtree in the provided
    cache. Keeping this bounded is important because each value is a full panel
    series and Phase3CM may run many worker processes in parallel.
    """

    def __init__(self, *, max_entries: int, stats: dict[str, int], prefix: str) -> None:
        super().__init__()
        self.max_entries = max(0, int(max_entries))
        self.stats = stats
        self.prefix = prefix

    def __contains__(self, key: object) -> bool:
        hit = super().__contains__(key)
        self.stats[f"{self.prefix}_hits" if hit else f"{self.prefix}_misses"] = (
            self.stats.get(f"{self.prefix}_hits" if hit else f"{self.prefix}_misses", 0) + 1
        )
        return hit

    def __setitem__(self, key: str, value: pd.Series) -> None:
        exists = super().__contains__(key)
        if not exists and self.max_entries and len(self) >= self.max_entries:
            self.stats[f"{self.prefix}_skipped_capacity"] = self.stats.get(f"{self.prefix}_skipped_capacity", 0) + 1
            return
        if not exists:
            self.stats[f"{self.prefix}_stores"] = self.stats.get(f"{self.prefix}_stores", 0) + 1
        super().__setitem__(key, value)


def _inc(stats: dict[str, int] | None, key: str, amount: int = 1) -> None:
    if stats is not None:
        stats[key] = stats.get(key, 0) + int(amount)


def _package_versions() -> dict[str, str]:
    packages = ["numpy", "pandas", "pyarrow", "numba", "bottleneck", "numexpr", "polars", "joblib", "scikit-learn"]
    versions: dict[str, str] = {}
    for package in packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "missing"
    return versions


def _has_hard_input_blocker(row: dict[str, Any]) -> bool:
    text = " ".join(
        str(row.get(key) or "")
        for key in (
            "phase3bp_blocker_flags",
            "phase3ca_blocker_flags",
            "train_reward_blockers",
            "blocker_flags",
            "hard_reject_reason",
            "blocked_or_future_fields",
            "missing_schema_fields",
        )
    ).lower()
    return any(token in text for token in HARD_INPUT_BLOCKER_TOKENS)


def _load_candidates(
    path: Path,
    limit: int,
    *,
    drop_hard_blocked_input: bool = False,
    enable_legacy_alias_rewrite: bool = True,
    m1_first_ret_replacement: str = "m1_first5_last_return_vs_open",
) -> list[dict[str, Any]]:
    rows = _read_csv(path)
    rows.sort(
        key=lambda row: (
            _f(row.get("phase3ca_proxy_quality"), -999.0),
            _f(row.get("aligned_ic_mean") or row.get("abs_aligned_ic_mean"), -999.0),
        ),
        reverse=True,
    )
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if drop_hard_blocked_input and _has_hard_input_blocker(row):
            continue
        expression = str(row.get("expression") or "").strip()
        source_expression = expression
        rewrites = []
        if enable_legacy_alias_rewrite:
            expression, rewrites = rewrite_legacy_field_aliases(
                expression,
                m1_first_ret_replacement=m1_first_ret_replacement,
            )
        digest = str(row.get("expression_hash") or "").strip()
        if not expression:
            continue
        if rewrites:
            source_digest = digest
            digest = hashlib.sha256(expression.encode("utf-8")).hexdigest()[:24]
        else:
            source_digest = ""
        if not digest:
            digest = hashlib.sha256(expression.encode("utf-8")).hexdigest()[:24]
        if digest in seen:
            continue
        seen.add(digest)
        item = dict(row)
        item["expression"] = expression
        item["expression_hash"] = digest
        item["candidate_id"] = row.get("candidate_id") or digest[:12]
        if rewrites:
            item["legacy_alias_original_expression"] = source_expression
            item["legacy_alias_source_expression_hash"] = source_digest
            item["legacy_alias_rewrites"] = rewrite_summary(rewrites)
            item["legacy_alias_rewrite_policy"] = "|".join(sorted({rewrite.policy for rewrite in rewrites}))
        item["fields_list"] = _fields(expression)
        item["max_window"] = _max_expression_window(expression)
        selected.append(item)
        if len(selected) >= limit:
            break
    if not selected:
        raise RuntimeError(f"no candidates selected from {path}")
    return selected


def _schema_intersection(panels: list[Path]) -> set[str]:
    intersection: set[str] | None = None
    for panel in panels:
        fields = set(pq.ParquetFile(panel).schema_arrow.names)
        intersection = fields if intersection is None else intersection & fields
    return intersection or set()


def _schema_gate_candidates(
    candidates: list[dict[str, Any]],
    available_fields: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    runnable: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    for candidate in candidates:
        missing = sorted(set(candidate.get("fields_list") or ()) - set(available_fields))
        if missing:
            item = dict(candidate)
            item["missing_schema_fields"] = "|".join(missing)
            held.append(item)
        else:
            runnable.append(candidate)
    return runnable, held


def _schema_hold_reward_row(candidate: dict[str, Any], *, portfolio_mode: str) -> dict[str, Any]:
    missing = str(candidate.get("missing_schema_fields") or "")
    blockers = "candidate_missing_schema_fields"
    if missing:
        blockers = f"{blockers}:{missing}"
    row = {
        "candidate_id": candidate.get("candidate_id"),
        "expression_hash": candidate.get("expression_hash"),
        "run": candidate.get("run"),
        "source_round": candidate.get("round_id"),
        "generator_arm": candidate.get("generator_arm"),
        "generator_route": candidate.get("generator_route"),
        "source_generator": candidate.get("source_generator"),
        "source_lane": candidate.get("source_lane"),
        "factor_lane": candidate.get("factor_lane"),
        "field_family": candidate.get("field_family"),
        "primitive_family": candidate.get("primitive_family"),
        "event_state_family": candidate.get("event_state_family"),
        "horizon_bucket": candidate.get("horizon_bucket"),
        "turnover_bucket": candidate.get("turnover_bucket"),
        "family_id": candidate.get("family_id"),
        "motif_id": candidate.get("motif_id"),
        "subtree_hashes": candidate.get("subtree_hashes"),
        "phase3ca_proxy_quality": candidate.get("phase3ca_proxy_quality"),
        "proxy_quality": candidate.get("proxy_quality"),
        "aligned_ic_mean": candidate.get("aligned_ic_mean"),
        "spread_hit_rate": candidate.get("spread_hit_rate"),
        "mean_one_way_turnover": candidate.get("mean_one_way_turnover"),
        "fields": candidate.get("fields") or "|".join(candidate.get("fields_list") or ()),
        "missing_schema_fields": missing,
        "legacy_alias_rewrites": candidate.get("legacy_alias_rewrites"),
        "legacy_alias_rewrite_policy": candidate.get("legacy_alias_rewrite_policy"),
        "legacy_alias_original_expression": candidate.get("legacy_alias_original_expression"),
        "legacy_alias_source_expression_hash": candidate.get("legacy_alias_source_expression_hash"),
        "expression": candidate.get("expression"),
        "portfolio_mode": portfolio_mode,
        "short_allowed": bool(portfolio_mode == "long_short_spread"),
        "train_reward": -2.3,
        "optimizer_reward": -2.3,
        "optimizer_reward_source": "train_only_phase3cm",
        "optimizer_reward_metric": OPTIMIZER_REWARD_METRIC,
        "optimizer_reward_split": "train",
        "validation_usage": "report_only",
        "holdout_usage": "report_only",
        "train_reward_blockers": blockers,
        "train_reward_decision": "HOLD_SCHEMA_MISSING_FIELDS",
    }
    row.update(normalize_candidate_schema(row))
    return row


def _split_map(signal_times: set[pd.Timestamp], train_fraction: float, validation_fraction: float) -> dict[pd.Timestamp, str]:
    times = sorted(pd.to_datetime(list(signal_times)))
    if not times:
        return {}
    n = len(times)
    train_end = max(1, min(n, int(round(n * train_fraction))))
    validation_end = max(train_end, min(n, train_end + int(round(n * validation_fraction))))
    out: dict[pd.Timestamp, str] = {}
    for idx, trade_time in enumerate(times):
        if idx < train_end:
            split = "train"
        elif idx < validation_end:
            split = "validation"
        else:
            split = "holdout"
        out[pd.Timestamp(trade_time)] = split
    return out


def _read_train_shard(
    *,
    candidates: list[dict[str, Any]],
    panel_path: Path,
    horizons: tuple[int, ...],
    sample_trade_times: int | None,
    sample_block_count: int = 1,
    sample_block_index: int = 0,
) -> tuple[
    pd.DataFrame,
    pd.Series,
    pd.DataFrame,
    pd.DataFrame,
    set[pd.Timestamp],
    set[pd.Timestamp],
    dict[int, set[pd.Timestamp]],
    dict[str, Any],
]:
    max_window = max((int(candidate.get("max_window") or 0) for candidate in candidates), default=0)
    max_horizon = max(horizons)
    fields = sorted({field for candidate in candidates for field in candidate["fields_list"]})
    required = {
        "code",
        "trade_time",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vol",
        "amount",
        "amount_yuan",
        "vwap",
        *fields,
    }

    schema = set(pq.ParquetFile(panel_path).schema_arrow.names)
    columns = [column for column in sorted(required) if column in schema]
    missing = sorted({"code", "trade_time", "date", "close"} - set(columns))
    if missing:
        raise RuntimeError(f"{panel_path} missing required columns {missing}")
    missing_fields = sorted(set(fields) - set(columns))
    if missing_fields:
        raise RuntimeError(f"{panel_path} missing expression fields {missing_fields}")

    sample_block_count = max(1, int(sample_block_count))
    sample_block_index = max(0, int(sample_block_index))
    if sample_block_index >= sample_block_count:
        raise ValueError("sample_block_index must be < sample_block_count")

    if sample_block_count <= 1:
        frame, signal_times, signal_time_count, read_time_count = _read_windowed_panel(
            panel_path,
            columns=columns,
            signal_time_count=sample_trade_times,
            lookback=max_window,
            max_horizon=max_horizon,
        )
        full_signal_times = set(signal_times)
        all_trade_times = _panel_trade_times(panel_path)
        position_by_time = {pd.Timestamp(value): idx for idx, value in enumerate(pd.to_datetime(all_trade_times))}
        signal_positions = sorted(position_by_time[pd.Timestamp(value)] for value in signal_times if pd.Timestamp(value) in position_by_time)
    else:
        all_trade_times = _panel_trade_times(panel_path)
        all_signal_positions = _sample_positions_for_count(len(all_trade_times), sample_trade_times)
        signal_blocks = np.array_split(all_signal_positions, sample_block_count)
        signal_positions = [int(value) for value in signal_blocks[sample_block_index].tolist()]
        full_signal_times = set(pd.to_datetime(all_trade_times.iloc[all_signal_positions]).tolist())
        frame, signal_times, signal_time_count, read_time_count = _read_windowed_panel_positions(
            panel_path,
            columns=columns,
            all_trade_times=all_trade_times,
            signal_positions=signal_positions,
            lookback=max_window,
            max_horizon=max_horizon,
        )
    frame["code"] = frame["code"].astype(str)
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["code", "trade_time", "close"]).sort_values(["code", "trade_time"]).reset_index(drop=True)

    candidate_windows = sorted({max(0, int(candidate.get("max_window") or 0)) for candidate in candidates})
    context_times_by_window: dict[int, set[pd.Timestamp]] = {}
    for window in candidate_windows:
        read_positions: set[int] = set()
        for pos in signal_positions:
            start = max(0, int(pos) - int(window))
            read_positions.update(range(start, int(pos) + 1))
        context_times_by_window[int(window)] = set(pd.to_datetime(all_trade_times.iloc[sorted(read_positions)]).tolist())

    eval_mask = frame["trade_time"].isin(signal_times)
    eval_frame = frame.loc[eval_mask].copy().reset_index(drop=True)
    labels = _future_returns(frame, horizons).loc[eval_mask].reset_index(drop=True)
    meta = {
        "panel": str(panel_path),
        "read_rows": int(len(frame)),
        "eval_rows": int(len(eval_frame)),
        "signal_trade_times": int(signal_time_count),
        "full_signal_trade_times": int(len(full_signal_times)),
        "read_trade_times": int(read_time_count),
        "sample_block_count": int(sample_block_count),
        "sample_block_index": int(sample_block_index),
        "context_trade_time_counts": json.dumps({str(key): len(value) for key, value in context_times_by_window.items()}, sort_keys=True),
        "read_column_count": len(columns),
        "candidate_count_in_batch": len(candidates),
    }
    return frame, eval_mask, eval_frame, labels, signal_times, full_signal_times, context_times_by_window, meta


if njit is not None:

    @njit(cache=True)
    def _rank_pct_1d_average_numba(values: np.ndarray) -> np.ndarray:
        out = np.empty(values.shape[0], dtype=np.float64)
        for i in range(out.shape[0]):
            out[i] = np.nan

        valid_pos = np.empty(values.shape[0], dtype=np.int64)
        valid_values = np.empty(values.shape[0], dtype=np.float64)
        n = 0
        for i in range(values.shape[0]):
            value = values[i]
            if np.isfinite(value):
                valid_pos[n] = i
                valid_values[n] = value
                n += 1
        if n == 0:
            return out

        order = np.argsort(valid_values[:n])
        sorted_values = valid_values[:n][order]
        tie_start = 0
        while tie_start < n:
            tie_end = tie_start + 1
            while tie_end < n and sorted_values[tie_end] == sorted_values[tie_start]:
                tie_end += 1
            avg_rank = ((tie_start + 1) + tie_end) / 2.0
            pct_rank = avg_rank / n
            for j in range(tie_start, tie_end):
                out[valid_pos[order[j]]] = pct_rank
            tie_start = tie_end
        return out

else:
    _rank_pct_1d_average_numba = None


def _rank_pct_1d_average(values: np.ndarray, cache_stats: dict[str, int] | None = None) -> np.ndarray:
    global _NUMBA_RANK_RUNTIME_DISABLED
    arr = np.asarray(values, dtype=float)
    if (
        _rank_pct_1d_average_numba is not None
        and not _NUMBA_RANK_RUNTIME_DISABLED
        and os.environ.get("PHASE3CM_DISABLE_NUMBA", "0") != "1"
    ):
        try:
            _inc(cache_stats, "numba_rank_calls")
            return _rank_pct_1d_average_numba(arr)
        except Exception:
            _NUMBA_RANK_RUNTIME_DISABLED = True
            _inc(cache_stats, "numba_rank_runtime_disabled")

    out = np.full(len(values), np.nan, dtype=float)
    valid = np.isfinite(arr)
    n = int(valid.sum())
    if n == 0:
        return out
    valid_pos = np.flatnonzero(valid)
    valid_values = arr[valid_pos]
    value_order = np.argsort(valid_values, kind="mergesort")
    sorted_values = valid_values[value_order]
    tie_bounds = np.flatnonzero(np.r_[True, sorted_values[1:] != sorted_values[:-1], True])
    ranks = np.empty(n, dtype=float)
    for tie_start, tie_end in zip(tie_bounds[:-1], tie_bounds[1:]):
        avg_rank = ((tie_start + 1) + tie_end) / 2.0
        ranks[value_order[tie_start:tie_end]] = avg_rank / n
    out[valid_pos] = ranks
    return out


def _build_eval_time_index(eval_frame: pd.DataFrame) -> dict[str, Any]:
    times = pd.to_datetime(eval_frame["trade_time"], errors="coerce").reset_index(drop=True)
    time_ns = times.to_numpy(dtype="datetime64[ns]").astype("int64")
    codes = eval_frame["code"].astype(str).to_numpy()
    order = np.argsort(time_ns, kind="mergesort")
    sorted_ns = time_ns[order]
    boundaries = np.flatnonzero(np.r_[True, sorted_ns[1:] != sorted_ns[:-1], True])
    groups: list[tuple[pd.Timestamp, np.ndarray]] = []
    for start, end in zip(boundaries[:-1], boundaries[1:]):
        value = sorted_ns[start]
        if value == np.iinfo(np.int64).min:
            continue
        groups.append((pd.Timestamp(value), order[start:end]))
    return {"groups": groups, "codes": codes, "times": times}


def _rank_by_eval_time_index(
    values: pd.Series,
    eval_time_index: dict[str, Any],
    cache_stats: dict[str, int] | None = None,
) -> pd.Series:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float, copy=False)
    out = np.full(len(arr), np.nan, dtype=float)
    for _, idx in eval_time_index["groups"]:
        out[idx] = _rank_pct_1d_average(arr[idx], cache_stats=cache_stats)
    return pd.Series(out, index=values.index)


def _candidate_portfolio_rows_from_precomputed_time_groups(
    *,
    candidate: dict[str, Any],
    eval_time_index: dict[str, Any],
    labels: pd.DataFrame,
    signal: pd.Series,
    signal_rank: pd.Series,
    split_by_time: dict[pd.Timestamp, str],
    shard_index: int,
    horizons: tuple[int, ...],
    min_obs: int,
    cost_bps: float,
    top_quantile: float,
    portfolio_mode: str,
    cache_stats: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    direction = 1.0 if str(candidate.get("open_direction") or "long_top") == "long_top" else -1.0
    one_way_cost = float(cost_bps) / 10000.0
    q_low = float(top_quantile)
    q_high = 1.0 - float(top_quantile)
    codes = eval_time_index["codes"]
    rank_arr = pd.to_numeric(signal_rank, errors="coerce").to_numpy(dtype=float, copy=False)
    signal_arr = pd.to_numeric(signal, errors="coerce").to_numpy(dtype=float, copy=False)
    rows: list[dict[str, Any]] = []

    for horizon in horizons:
        ret_arr = pd.to_numeric(labels[f"fwd_ret_{horizon}m"], errors="coerce").to_numpy(dtype=float, copy=False)
        previous_top: set[str] | None = None
        previous_bottom: set[str] | None = None
        previous_long: set[str] | None = None
        for trade_time, idx in eval_time_index["groups"]:
            rank_part = rank_arr[idx]
            ret_part = ret_arr[idx]
            valid = np.isfinite(rank_part) & np.isfinite(ret_part)
            if int(valid.sum()) < min_obs:
                continue
            rank_valid = rank_part[valid]
            ret_valid = ret_part[valid]
            signal_valid = signal_arr[idx][valid]
            codes_valid = codes[idx][valid]

            rank_ic_raw = float("nan")
            if np.unique(rank_valid).size > 1 and np.unique(ret_valid).size > 1:
                ret_rank = _rank_pct_1d_average(ret_valid, cache_stats=cache_stats)
                ret_rank_valid = ret_rank[np.isfinite(ret_rank)]
                if len(ret_rank_valid) == len(rank_valid) and np.nanstd(rank_valid) > 0.0 and np.nanstd(ret_rank_valid) > 0.0:
                    rank_ic_raw = _f(np.corrcoef(rank_valid, ret_rank_valid)[0, 1])
            rank_ic = rank_ic_raw * direction if math.isfinite(rank_ic_raw) else float("nan")

            top_mask = rank_valid >= q_high
            bottom_mask = rank_valid <= q_low
            if not bool(top_mask.any()) or not bool(bottom_mask.any()):
                continue
            top_codes = set(codes_valid[top_mask].astype(str))
            bottom_codes = set(codes_valid[bottom_mask].astype(str))

            top_returns = ret_valid[top_mask]
            bottom_returns = ret_valid[bottom_mask]
            market_mean_return = float(np.mean(ret_valid))
            if portfolio_mode == "long_short_spread":
                if previous_top is None or previous_bottom is None:
                    one_way_turnover = 1.0
                else:
                    top_turn = 1.0 - (len(top_codes & previous_top) / max(1, len(top_codes)))
                    bottom_turn = 1.0 - (len(bottom_codes & previous_bottom) / max(1, len(bottom_codes)))
                    one_way_turnover = (top_turn + bottom_turn) / 2.0
                previous_top = top_codes
                previous_bottom = bottom_codes
                raw_return = float(np.mean(top_returns) - np.mean(bottom_returns)) * direction
                trading_cost = 2.0 * one_way_cost * one_way_turnover
                long_count = int(top_mask.sum() if direction > 0 else bottom_mask.sum())
                short_count = int(bottom_mask.sum() if direction > 0 else top_mask.sum())
            else:
                long_mask = top_mask if direction > 0 else bottom_mask
                long_codes = set(codes_valid[long_mask].astype(str))
                if previous_long is None:
                    one_way_turnover = 1.0
                else:
                    one_way_turnover = 1.0 - (len(long_codes & previous_long) / max(1, len(long_codes)))
                previous_long = long_codes
                selected_return = float(np.mean(ret_valid[long_mask]))
                raw_return = selected_return - market_mean_return if portfolio_mode == "long_only_excess_market" else selected_return
                trading_cost = one_way_cost * one_way_turnover
                long_count = int(long_mask.sum())
                short_count = 0
            net_return = raw_return - trading_cost
            rows.append(
                {
                    "candidate_id": candidate.get("candidate_id"),
                    "expression_hash": candidate.get("expression_hash"),
                    "run": candidate.get("run"),
                    "source_round": candidate.get("round_id"),
                    "factor_lane": candidate.get("factor_lane"),
                    "shard_index": shard_index,
                    "split": split_by_time.get(trade_time, "unassigned"),
                    "trade_time": trade_time.isoformat(),
                    "trade_date": trade_time.date().isoformat(),
                    "horizon_min": horizon,
                    "portfolio_mode": portfolio_mode,
                    "long_count": long_count,
                    "short_count": short_count,
                    "raw_return": raw_return,
                    "market_mean_return": market_mean_return,
                    "trading_cost": trading_cost,
                    "net_return": net_return,
                    "one_way_turnover": one_way_turnover,
                    "top_mean_return": float(np.mean(top_returns)),
                    "bottom_mean_return": float(np.mean(bottom_returns)),
                    "top_signal_mean": float(np.mean(signal_valid[top_mask])),
                    "bottom_signal_mean": float(np.mean(signal_valid[bottom_mask])),
                    "rank_ic": rank_ic,
                    "rank_ic_raw": rank_ic_raw,
                    "rank_ic_obs": int(len(rank_valid)),
                    "cost_bps": cost_bps,
                }
            )
    return rows


def _candidate_portfolio_rows_from_frame(
    *,
    candidate: dict[str, Any],
    frame: pd.DataFrame,
    eval_mask: pd.Series,
    eval_frame: pd.DataFrame,
    labels: pd.DataFrame,
    eval_time_index: dict[str, Any] | None,
    split_by_time: dict[pd.Timestamp, str],
    context_times_by_window: dict[int, set[pd.Timestamp]],
    shard_index: int,
    horizons: tuple[int, ...],
    min_obs: int,
    cost_bps: float,
    top_quantile: float,
    portfolio_mode: str,
    expression_cache: dict[str, pd.Series],
    feature_matrix_cache: dict[int, tuple[pd.DataFrame, pd.Series]],
    operator_cache_by_window: dict[int, dict[str, pd.Series]],
    cache_stats: dict[str, int],
    operator_cache_max_entries: int,
    feature_matrix_cache_max_windows: int,
) -> list[dict[str, Any]]:
    expression = str(candidate["expression"])
    if expression in expression_cache:
        _inc(cache_stats, "factor_expression_cache_hits")
        signal = expression_cache[expression]
    else:
        _inc(cache_stats, "factor_expression_cache_misses")
        context_window = max(0, int(candidate.get("max_window") or 0))
        context_times = context_times_by_window.get(context_window)
        if context_times:
            cached_context = feature_matrix_cache.get(context_window)
            if cached_context is not None:
                _inc(cache_stats, "feature_matrix_cache_hits")
                context_frame, context_eval_mask = cached_context
            else:
                _inc(cache_stats, "feature_matrix_cache_misses")
                context_mask = frame["trade_time"].isin(context_times)
                context_frame = frame.loc[context_mask].copy().reset_index(drop=True)
                context_eval_mask = context_frame["trade_time"].isin(split_by_time.keys())
                if len(feature_matrix_cache) < max(0, int(feature_matrix_cache_max_windows)):
                    feature_matrix_cache[context_window] = (context_frame, context_eval_mask)
                    _inc(cache_stats, "feature_matrix_cache_stores")
                else:
                    _inc(cache_stats, "feature_matrix_cache_skipped_capacity")
            operator_cache = operator_cache_by_window.get(context_window)
            if operator_cache is None:
                operator_cache = None
                if operator_cache_max_entries >= 0:
                    operator_cache = _BoundedSeriesCache(
                        max_entries=operator_cache_max_entries,
                        stats=cache_stats,
                        prefix="operator_cache",
                    )
                    operator_cache_by_window[context_window] = operator_cache
            signal_all = pd.to_numeric(evaluate_panel_expression(context_frame, expression, cache=operator_cache), errors="coerce")
            signal = pd.Series(signal_all.loc[context_eval_mask].to_numpy(dtype=float))
            if len(signal) != len(eval_frame):
                # Fallback preserves correctness if a sparse context unexpectedly loses signal rows.
                _inc(cache_stats, "feature_matrix_cache_fallback_full_frame")
                full_operator_cache = operator_cache_by_window.get(-1)
                if full_operator_cache is None:
                    full_operator_cache = None
                    if operator_cache_max_entries >= 0:
                        full_operator_cache = _BoundedSeriesCache(
                            max_entries=operator_cache_max_entries,
                            stats=cache_stats,
                            prefix="operator_cache",
                        )
                        operator_cache_by_window[-1] = full_operator_cache
                signal_all = pd.to_numeric(evaluate_panel_expression(frame, expression, cache=full_operator_cache), errors="coerce")
                signal = pd.Series(signal_all.loc[eval_mask].to_numpy(dtype=float))
        else:
            operator_cache = operator_cache_by_window.get(-1)
            if operator_cache is None:
                operator_cache = None
                if operator_cache_max_entries >= 0:
                    operator_cache = _BoundedSeriesCache(
                        max_entries=operator_cache_max_entries,
                        stats=cache_stats,
                        prefix="operator_cache",
                    )
                    operator_cache_by_window[-1] = operator_cache
            signal_all = pd.to_numeric(evaluate_panel_expression(frame, expression, cache=operator_cache), errors="coerce")
            signal = pd.Series(signal_all.loc[eval_mask].to_numpy(dtype=float))
        expression_cache[expression] = signal
        _inc(cache_stats, "factor_expression_cache_stores")
    if eval_time_index is not None:
        signal_rank = _rank_by_eval_time_index(signal, eval_time_index, cache_stats=cache_stats)
        _inc(cache_stats, "fast_portfolio_loop_used")
        return _candidate_portfolio_rows_from_precomputed_time_groups(
            candidate=candidate,
            eval_time_index=eval_time_index,
            labels=labels,
            signal=signal,
            signal_rank=signal_rank,
            split_by_time=split_by_time,
            shard_index=shard_index,
            horizons=horizons,
            min_obs=min_obs,
            cost_bps=cost_bps,
            top_quantile=top_quantile,
            portfolio_mode=portfolio_mode,
            cache_stats=cache_stats,
        )

    signal_rank = _rank_by_group(signal, eval_frame["trade_time"])
    direction = 1.0 if str(candidate.get("open_direction") or "long_top") == "long_top" else -1.0
    one_way_cost = float(cost_bps) / 10000.0
    q_low = float(top_quantile)
    q_high = 1.0 - float(top_quantile)
    rows: list[dict[str, Any]] = []

    for horizon in horizons:
        label = pd.to_numeric(labels[f"fwd_ret_{horizon}m"], errors="coerce")
        work = pd.DataFrame(
            {
                "code": eval_frame["code"].astype(str),
                "trade_time": eval_frame["trade_time"],
                "rank": signal_rank,
                "ret": label,
                "signal": signal,
            }
        ).dropna(subset=["rank", "ret"])
        previous_top: set[str] | None = None
        previous_bottom: set[str] | None = None
        previous_long: set[str] | None = None
        for trade_time, block in work.groupby("trade_time", sort=True):
            if len(block) < min_obs:
                continue
            rank_ic_raw = float("nan")
            if block["rank"].nunique(dropna=True) > 1 and block["ret"].nunique(dropna=True) > 1:
                ret_rank = block["ret"].rank(pct=True, method="average")
                rank_ic_raw = _f(block["rank"].corr(ret_rank))
            rank_ic = rank_ic_raw * direction if math.isfinite(rank_ic_raw) else float("nan")
            top_block = block.loc[block["rank"] >= q_high]
            bottom_block = block.loc[block["rank"] <= q_low]
            if top_block.empty or bottom_block.empty:
                continue
            top_codes = set(top_block["code"].astype(str))
            bottom_codes = set(bottom_block["code"].astype(str))

            market_mean_return = float(block["ret"].mean())
            if portfolio_mode == "long_short_spread":
                if previous_top is None or previous_bottom is None:
                    one_way_turnover = 1.0
                else:
                    top_turn = 1.0 - (len(top_codes & previous_top) / max(1, len(top_codes)))
                    bottom_turn = 1.0 - (len(bottom_codes & previous_bottom) / max(1, len(bottom_codes)))
                    one_way_turnover = (top_turn + bottom_turn) / 2.0
                previous_top = top_codes
                previous_bottom = bottom_codes
                raw_return = float(top_block["ret"].mean() - bottom_block["ret"].mean()) * direction
                trading_cost = 2.0 * one_way_cost * one_way_turnover
                long_count = int(len(top_block) if direction > 0 else len(bottom_block))
                short_count = int(len(bottom_block) if direction > 0 else len(top_block))
            else:
                long_block = top_block if direction > 0 else bottom_block
                long_codes = set(long_block["code"].astype(str))
                if previous_long is None:
                    one_way_turnover = 1.0
                else:
                    one_way_turnover = 1.0 - (len(long_codes & previous_long) / max(1, len(long_codes)))
                previous_long = long_codes
                selected_return = float(long_block["ret"].mean())
                raw_return = selected_return - market_mean_return if portfolio_mode == "long_only_excess_market" else selected_return
                trading_cost = one_way_cost * one_way_turnover
                long_count = int(len(long_block))
                short_count = 0
            net_return = raw_return - trading_cost
            ts = pd.Timestamp(trade_time)
            rows.append(
                {
                    "candidate_id": candidate.get("candidate_id"),
                    "expression_hash": candidate.get("expression_hash"),
                    "run": candidate.get("run"),
                    "source_round": candidate.get("round_id"),
                    "factor_lane": candidate.get("factor_lane"),
                    "shard_index": shard_index,
                    "split": split_by_time.get(ts, "unassigned"),
                    "trade_time": ts.isoformat(),
                    "trade_date": ts.date().isoformat(),
                    "horizon_min": horizon,
                    "portfolio_mode": portfolio_mode,
                    "long_count": long_count,
                    "short_count": short_count,
                    "raw_return": raw_return,
                    "market_mean_return": market_mean_return,
                    "trading_cost": trading_cost,
                    "net_return": net_return,
                    "one_way_turnover": one_way_turnover,
                    "top_mean_return": float(top_block["ret"].mean()),
                    "bottom_mean_return": float(bottom_block["ret"].mean()),
                    "top_signal_mean": float(top_block["signal"].mean()),
                    "bottom_signal_mean": float(bottom_block["signal"].mean()),
                    "rank_ic": rank_ic,
                    "rank_ic_raw": rank_ic_raw,
                    "rank_ic_obs": int(len(block)),
                    "cost_bps": cost_bps,
                }
            )
    return rows


def _curve_rows(rows: list[dict[str, Any]], *, split: str, horizon: int | None = None) -> list[dict[str, Any]]:
    filtered = [
        row
        for row in rows
        if str(row.get("split")) == split and (horizon is None or int(row.get("horizon_min") or -1) == horizon)
    ]
    if not filtered:
        return []
    frame = pd.DataFrame(filtered)
    frame["net_return"] = pd.to_numeric(frame["net_return"], errors="coerce")
    frame["raw_return"] = pd.to_numeric(frame["raw_return"], errors="coerce")
    frame["one_way_turnover"] = pd.to_numeric(frame["one_way_turnover"], errors="coerce")
    if "market_mean_return" not in frame:
        frame["market_mean_return"] = np.nan
    frame["market_mean_return"] = pd.to_numeric(frame["market_mean_return"], errors="coerce")
    if "rank_ic" not in frame:
        frame["rank_ic"] = np.nan
    frame["rank_ic"] = pd.to_numeric(frame["rank_ic"], errors="coerce")
    grouped = (
        frame.groupby(["trade_time", "trade_date"], sort=True)
        .agg(
            net_return=("net_return", "mean"),
            raw_return=("raw_return", "mean"),
            market_mean_return=("market_mean_return", "mean"),
            one_way_turnover=("one_way_turnover", "mean"),
            rank_ic=("rank_ic", "mean"),
            rank_ic_count=("rank_ic", "count"),
            sleeve_count=("net_return", "count"),
        )
        .reset_index()
    )
    return grouped.to_dict("records")


def _daily_returns(curve_rows: list[dict[str, Any]]) -> list[float]:
    if not curve_rows:
        return []
    frame = pd.DataFrame(curve_rows)
    frame["net_return"] = pd.to_numeric(frame["net_return"], errors="coerce")
    daily = frame.groupby("trade_date", sort=True)["net_return"].sum()
    return [float(value) for value in daily.to_numpy(dtype=float) if math.isfinite(float(value))]


def _bootstrap_days(day_values: list[float], *, iterations: int, seed: int) -> dict[str, Any]:
    clean = [float(value) for value in day_values if math.isfinite(float(value))]
    if not clean:
        return {"iterations": 0, "day_count": 0}
    rng = random.Random(seed)
    draws: list[float] = []
    positives = 0
    for _ in range(iterations):
        sample = [clean[rng.randrange(len(clean))] for _ in range(len(clean))]
        value = _sortino(sample)
        if value is None:
            continue
        draws.append(value)
        positives += int(value > 0)
    return {
        "iterations": len(draws),
        "day_count": len(clean),
        "p05": _round(_quantile(draws, 0.05)),
        "p25": _round(_quantile(draws, 0.25)),
        "median": _round(_quantile(draws, 0.50)),
        "p95": _round(_quantile(draws, 0.95)),
        "prob_gt_0": _round(positives / len(draws) if draws else None),
    }


def _summarize_curve(curve_rows: list[dict[str, Any]], *, split: str, horizon: int | str, seed: int) -> dict[str, Any]:
    values = [_f(row.get("net_return")) for row in curve_rows]
    values = [value for value in values if math.isfinite(value)]
    raw = [_f(row.get("raw_return")) for row in curve_rows]
    raw = [value for value in raw if math.isfinite(value)]
    turnover = [_f(row.get("one_way_turnover")) for row in curve_rows]
    turnover = [value for value in turnover if math.isfinite(value)]
    rank_ic = [_f(row.get("rank_ic")) for row in curve_rows]
    rank_ic = [value for value in rank_ic if math.isfinite(value)]
    rank_ic_mean = statistics.fmean(rank_ic) if rank_ic else None
    days = sorted({str(row.get("trade_date")) for row in curve_rows if row.get("trade_date")})
    day_values = _daily_returns(curve_rows)
    boot = _bootstrap_days(day_values, iterations=600, seed=seed)
    return {
        "split": split,
        "horizon_min": horizon,
        "curve_count": len(values),
        "day_count": len(days),
        "net_mean_return": _round(statistics.fmean(values) if values else None, 10),
        "raw_mean_return": _round(statistics.fmean(raw) if raw else None, 10),
        "net_hit_rate": _round(sum(1 for value in values if value > 0) / len(values) if values else None),
        "minute_sortino": _round(_sortino(values)),
        "day_sortino": _round(_sortino(day_values)),
        "day_mcmc_sortino_p25": boot.get("p25"),
        "day_mcmc_sortino_median": boot.get("median"),
        "day_mcmc_prob_sortino_gt_0": boot.get("prob_gt_0"),
        "max_drawdown": _round(_max_drawdown(values)),
        "mean_one_way_turnover": _round(statistics.fmean(turnover) if turnover else None),
        "rank_ic_mean": _round(rank_ic_mean),
        "rank_ic_hit_rate": _round(sum(1 for value in rank_ic if value > 0) / len(rank_ic) if rank_ic else None),
        "rank_ic_loss": _round(-rank_ic_mean if rank_ic_mean is not None else None),
        "rank_ic_obs": len(rank_ic),
    }


def _regime_stability_summary(curve_rows: list[dict[str, Any]], *, min_days_per_regime: int = 3) -> dict[str, Any]:
    """Train-only low-weight stability diagnostic across coarse regimes.

    The primary route uses market-return terciles from the same train split. If
    market-return observations are unavailable, it falls back to chronological
    thirds. Validation/holdout are never used here.
    """

    if not curve_rows:
        return {
            "train_regime_stability_score": None,
            "train_regime_worst_day_sortino": None,
            "train_regime_median_day_sortino": None,
            "train_regime_positive_share": None,
            "train_regime_count": 0,
            "train_regime_method": "none_no_train_curve",
        }
    frame = pd.DataFrame(curve_rows)
    frame["net_return"] = pd.to_numeric(frame["net_return"], errors="coerce")
    if "market_mean_return" not in frame:
        frame["market_mean_return"] = np.nan
    frame["market_mean_return"] = pd.to_numeric(frame["market_mean_return"], errors="coerce")
    daily = (
        frame.groupby("trade_date", sort=True)
        .agg(net_return=("net_return", "sum"), market_mean_return=("market_mean_return", "mean"))
        .reset_index()
        .dropna(subset=["net_return"])
    )
    if len(daily) < max(3, int(min_days_per_regime) * 2):
        return {
            "train_regime_stability_score": 0.0,
            "train_regime_worst_day_sortino": None,
            "train_regime_median_day_sortino": None,
            "train_regime_positive_share": None,
            "train_regime_count": 0,
            "train_regime_method": "insufficient_train_days",
        }

    method = "market_return_tercile"
    if daily["market_mean_return"].notna().sum() >= max(6, int(min_days_per_regime) * 3) and daily["market_mean_return"].nunique(dropna=True) >= 3:
        ranks = daily["market_mean_return"].rank(method="first", pct=True)
        daily["regime"] = np.where(ranks <= 1 / 3, "market_down", np.where(ranks <= 2 / 3, "market_mid", "market_up"))
    else:
        method = "chronological_tercile"
        positions = np.arange(len(daily), dtype=float) / max(1, len(daily) - 1)
        daily["regime"] = np.where(positions <= 1 / 3, "early", np.where(positions <= 2 / 3, "middle", "late"))

    regime_sortinos: list[float] = []
    regime_rows: list[dict[str, Any]] = []
    for regime, block in daily.groupby("regime", sort=True):
        values = [float(value) for value in block["net_return"].to_numpy(dtype=float) if math.isfinite(float(value))]
        if len(values) < int(min_days_per_regime):
            continue
        sortino = _sortino(values)
        if sortino is None or not math.isfinite(sortino):
            continue
        regime_sortinos.append(float(sortino))
        regime_rows.append({"regime": regime, "day_count": len(values), "day_sortino": _round(sortino)})
    if not regime_sortinos:
        return {
            "train_regime_stability_score": 0.0,
            "train_regime_worst_day_sortino": None,
            "train_regime_median_day_sortino": None,
            "train_regime_positive_share": None,
            "train_regime_count": 0,
            "train_regime_method": f"{method}_no_valid_regime_sortino",
        }
    worst = min(regime_sortinos)
    median = float(np.median(regime_sortinos))
    positive_share = sum(1 for value in regime_sortinos if value > 0.0) / len(regime_sortinos)
    clipped_worst = max(-1.0, min(1.0, worst))
    balance = max(-1.0, min(1.0, positive_share * 2.0 - 1.0))
    score = 0.65 * clipped_worst + 0.35 * balance
    return {
        "train_regime_stability_score": _round(score),
        "train_regime_worst_day_sortino": _round(worst),
        "train_regime_median_day_sortino": _round(median),
        "train_regime_positive_share": _round(positive_share),
        "train_regime_count": len(regime_sortinos),
        "train_regime_method": method,
        "train_regime_rows": json.dumps(regime_rows, ensure_ascii=False, sort_keys=True),
    }


def _candidate_summary(
    candidate: dict[str, Any],
    rows: list[dict[str, Any]],
    horizons: tuple[int, ...],
    *,
    seed: int,
    rank_ic_loss_weight: float,
    rank_ic_component_cap: float,
    regime_stability_weight: float,
    regime_component_cap: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    split_rows: list[dict[str, Any]] = []
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    train_all_curve: list[dict[str, Any]] = []
    for split in ("train", "validation", "holdout"):
        all_curve = _curve_rows(rows, split=split)
        if split == "train":
            train_all_curve = all_curve
        summary = _summarize_curve(all_curve, split=split, horizon="equal_weight_horizon_sleeves", seed=seed + len(split))
        split_rows.append(summary)
        by_key[(split, "all")] = summary
        for horizon in horizons:
            curve = _curve_rows(rows, split=split, horizon=horizon)
            horizon_summary = _summarize_curve(curve, split=split, horizon=horizon, seed=seed + horizon)
            split_rows.append(horizon_summary)
            by_key[(split, str(horizon))] = horizon_summary

    train_all = by_key.get(("train", "all"), {})
    validation_all = by_key.get(("validation", "all"), {})
    holdout_all = by_key.get(("holdout", "all"), {})
    train_horizon_sortinos = [
        _f(by_key.get(("train", str(horizon)), {}).get("day_sortino"))
        for horizon in horizons
    ]
    train_horizon_sortinos = [value for value in train_horizon_sortinos if math.isfinite(value)]
    train_worst = min(train_horizon_sortinos) if train_horizon_sortinos else float("nan")
    train_median = float(np.median(train_horizon_sortinos)) if train_horizon_sortinos else float("nan")
    instability = _safe_stdev(train_horizon_sortinos)
    train_turnover = _f(train_all.get("mean_one_way_turnover"), 0.0)
    train_day_sortino = _f(train_all.get("day_sortino"))
    train_day_mcmc_p25 = _f(train_all.get("day_mcmc_sortino_p25"))
    train_rank_ic_mean = _f(train_all.get("rank_ic_mean"))
    train_rank_ic_loss = _f(train_all.get("rank_ic_loss"), 0.05)
    rank_ic_reward_component = _bounded(-float(rank_ic_loss_weight) * train_rank_ic_loss, float(rank_ic_component_cap))
    train_regime = _regime_stability_summary(train_all_curve)
    regime_score = _f(train_regime.get("train_regime_stability_score"), 0.0)
    regime_reward_component = _bounded(float(regime_stability_weight) * regime_score, float(regime_component_cap))
    turnover_penalty = max(0.0, train_turnover - 0.55) * 0.75
    instability_penalty = max(0.0, _f(instability, 0.0) - 0.50) * 0.25
    inherited_blocker_penalty = 0.15 if str(candidate.get("phase3bp_blocker_flags") or candidate.get("blocker_flags") or "") else 0.0
    reward = (
        0.55 * _f(train_day_sortino, -2.0)
        + 0.25 * _f(train_worst, -2.0)
        + 0.20 * _f(train_day_mcmc_p25, -2.0)
        + rank_ic_reward_component
        + regime_reward_component
        - turnover_penalty
        - instability_penalty
        - inherited_blocker_penalty
    )
    blockers: list[str] = []
    if not math.isfinite(reward) or reward <= 0.0:
        blockers.append("non_positive_train_reward")
    if not math.isfinite(train_day_sortino) or train_day_sortino <= 0.0:
        blockers.append("non_positive_train_day_sortino")
    if not math.isfinite(train_worst) or train_worst <= 0.0:
        blockers.append("non_positive_worst_horizon_train_sortino")
    if _f(train_all.get("day_mcmc_prob_sortino_gt_0"), 0.0) < 0.60:
        blockers.append("weak_train_day_mcmc")
    if not math.isfinite(train_rank_ic_mean):
        blockers.append("no_valid_train_rank_ic")
    if train_turnover > 0.75:
        blockers.append("extreme_turnover")
    if str(candidate.get("phase3bp_blocker_flags") or candidate.get("blocker_flags") or ""):
        blockers.append("inherited_search_blocker")
    decision = "TRAIN_REWARD_FOLLOWUP_READY" if not blockers else "HOLD_TRAIN_REWARD"
    portfolio_mode = str(rows[0].get("portfolio_mode") or "unknown") if rows else "unknown"
    reward_row = {
        "candidate_id": candidate.get("candidate_id"),
        "expression_hash": candidate.get("expression_hash"),
        "run": candidate.get("run"),
        "source_round": candidate.get("round_id"),
        "generator_arm": candidate.get("generator_arm"),
        "generator_route": candidate.get("generator_route"),
        "source_generator": candidate.get("source_generator"),
        "source_lane": candidate.get("source_lane"),
        "factor_lane": candidate.get("factor_lane"),
        "field_family": candidate.get("field_family"),
        "primitive_family": candidate.get("primitive_family"),
        "event_state_family": candidate.get("event_state_family"),
        "horizon_bucket": candidate.get("horizon_bucket"),
        "turnover_bucket": candidate.get("turnover_bucket"),
        "family_id": candidate.get("family_id"),
        "motif_id": candidate.get("motif_id"),
        "subtree_hashes": candidate.get("subtree_hashes"),
        "phase3ca_proxy_quality": candidate.get("phase3ca_proxy_quality"),
        "proxy_quality": candidate.get("proxy_quality"),
        "aligned_ic_mean": candidate.get("aligned_ic_mean"),
        "spread_hit_rate": candidate.get("spread_hit_rate"),
        "mean_one_way_turnover": candidate.get("mean_one_way_turnover"),
        "fields": candidate.get("fields"),
        "legacy_alias_rewrites": candidate.get("legacy_alias_rewrites"),
        "legacy_alias_rewrite_policy": candidate.get("legacy_alias_rewrite_policy"),
        "legacy_alias_original_expression": candidate.get("legacy_alias_original_expression"),
        "legacy_alias_source_expression_hash": candidate.get("legacy_alias_source_expression_hash"),
        "expression": candidate.get("expression"),
        "portfolio_mode": portfolio_mode,
        "short_allowed": bool(portfolio_mode == "long_short_spread"),
        "train_reward": _round(reward),
        "optimizer_reward": _round(reward),
        "optimizer_reward_source": "train_only_phase3cm",
        "optimizer_reward_metric": OPTIMIZER_REWARD_METRIC,
        "optimizer_reward_split": "train",
        "validation_usage": "report_only",
        "holdout_usage": "report_only",
        "train_day_sortino": train_all.get("day_sortino"),
        "train_minute_sortino": train_all.get("minute_sortino"),
        "train_worst_horizon_day_sortino": _round(train_worst),
        "train_median_horizon_day_sortino": _round(train_median),
        "train_horizon_sortino_stdev": _round(instability),
        "train_day_mcmc_p25": train_all.get("day_mcmc_sortino_p25"),
        "train_day_mcmc_prob_gt_0": train_all.get("day_mcmc_prob_sortino_gt_0"),
        "train_mean_one_way_turnover": train_all.get("mean_one_way_turnover"),
        "train_rank_ic_mean": train_all.get("rank_ic_mean"),
        "train_rank_ic_hit_rate": train_all.get("rank_ic_hit_rate"),
        "train_rank_ic_loss": train_all.get("rank_ic_loss"),
        "train_rank_ic_reward_component": _round(rank_ic_reward_component),
        "train_rank_ic_obs": train_all.get("rank_ic_obs"),
        "train_regime_stability_score": train_regime.get("train_regime_stability_score"),
        "train_regime_reward_component": _round(regime_reward_component),
        "train_regime_worst_day_sortino": train_regime.get("train_regime_worst_day_sortino"),
        "train_regime_median_day_sortino": train_regime.get("train_regime_median_day_sortino"),
        "train_regime_positive_share": train_regime.get("train_regime_positive_share"),
        "train_regime_count": train_regime.get("train_regime_count"),
        "train_regime_method": train_regime.get("train_regime_method"),
        "train_regime_rows": train_regime.get("train_regime_rows"),
        "validation_day_sortino": validation_all.get("day_sortino"),
        "validation_day_mcmc_prob_gt_0": validation_all.get("day_mcmc_prob_sortino_gt_0"),
        "validation_rank_ic_mean": validation_all.get("rank_ic_mean"),
        "validation_rank_ic_loss": validation_all.get("rank_ic_loss"),
        "holdout_day_sortino": holdout_all.get("day_sortino"),
        "holdout_day_mcmc_prob_gt_0": holdout_all.get("day_mcmc_prob_sortino_gt_0"),
        "holdout_rank_ic_mean": holdout_all.get("rank_ic_mean"),
        "holdout_rank_ic_loss": holdout_all.get("rank_ic_loss"),
        "inherited_blockers": candidate.get("phase3bp_blocker_flags") or candidate.get("blocker_flags"),
        "train_reward_blockers": "|".join(blockers),
        "train_reward_decision": decision,
    }
    reward_row.update(normalize_candidate_schema(reward_row))
    return split_rows, reward_row


def _write_incremental_checkpoint(
    *,
    output_root: Path,
    report_root: Path,
    reward_by_hash: dict[str, dict[str, Any]],
    progress_rows: list[dict[str, Any]],
    shard_meta: list[dict[str, Any]],
    processed_candidate_shards: int,
    candidate_count: int,
    shard_count: int,
    final: bool = False,
) -> None:
    partial_rewards = sorted(
        reward_by_hash.values(),
        key=lambda row: _f(row.get("train_reward"), -999.0),
        reverse=True,
    )
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cm_incremental_checkpoint",
        "partial": not final,
        "final": bool(final),
        "candidate_count": int(candidate_count),
        "shard_count": int(shard_count),
        "processed_candidate_shards": int(processed_candidate_shards),
        "partial_reward_count": len(partial_rewards),
        "progress_row_count": len(progress_rows),
        "completed_shards": len([row for row in shard_meta if row.get("shard_complete")]),
        "latest_shard_index": shard_meta[-1].get("shard_index") if shard_meta else None,
        "metric_boundary": "incremental partial checkpoint; not final reward unless final=true",
    }
    for root in (output_root, report_root):
        _write_csv(root / "phase3cm_candidate_progress.csv", progress_rows)
        _write_csv(root / "phase3cm_train_reward_partial.csv", partial_rewards)
        _write_csv(root / "phase3cm_incremental_shard_meta.csv", shard_meta)
        _write_json(root / "phase3cm_incremental_checkpoint_summary.json", summary)


def _render_md(summary: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    ranked = sorted(rows, key=lambda row: _f(row.get("train_reward"), -999.0), reverse=True)
    lines = [
        "# Phase3CM Train Portfolio Sortino Reward Audit 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Scope",
        "",
        "Computes true1min train / validation / holdout portfolio reward curves for already-generated candidates.",
        "This replaces fragment Sortino as the intended search feedback target. It does not launch search and does not promote candidates.",
        "",
        "## Summary",
        "",
        f"- candidates: `{summary['candidate_count']}`",
        f"- portfolio pnl rows written: `{summary['portfolio_pnl_rows_written']}`",
        f"- portfolio mode: `{summary.get('portfolio_mode')}`; short allowed: `{summary.get('short_allowed')}`",
        f"- followup-ready by train reward only: `{summary['followup_count']}`",
        f"- horizons: `{summary['horizons']}`",
        f"- train/validation/holdout fractions: `{summary['train_fraction']}` / `{summary['validation_fraction']}` / `{summary['holdout_fraction']}`",
        f"- rank IC loss weight/cap: `{summary.get('rank_ic_loss_weight')}` / `{summary.get('rank_ic_component_cap')}`",
        f"- regime stability weight/cap: `{summary.get('regime_stability_weight')}` / `{summary.get('regime_component_cap')}`",
        "",
        "## Top Train Reward Rows",
        "",
        "| rank | candidate | reward | train day sortino | train rank IC | rank IC comp | regime comp | worst h sortino | val sortino | holdout sortino | turnover | decision | blockers | expression |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for idx, row in enumerate(ranked[:30], 1):
        expr = str(row.get("expression") or "").replace("|", "/")[:120]
        lines.append(
            f"| {idx} | `{row.get('candidate_id')}` | {row.get('train_reward')} | {row.get('train_day_sortino')} | "
            f"{row.get('train_rank_ic_mean')} | {row.get('train_rank_ic_reward_component')} | {row.get('train_regime_reward_component')} | "
            f"{row.get('train_worst_horizon_day_sortino')} | {row.get('validation_day_sortino')} | {row.get('holdout_day_sortino')} | "
            f"{row.get('train_mean_one_way_turnover')} | `{row.get('train_reward_decision')}` | `{row.get('train_reward_blockers')}` | `{expr}` |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This is train-set reward evidence, not final alpha proof.",
            "- `rank_ic_loss` is train-side aligned rank IC loss and is included in optimizer reward with a bounded component.",
            "- Regime stability is a low-weight train-only reward component; it is not a promotion gate.",
            "- Validation and holdout columns are reported for leakage control; searchers must not optimize holdout.",
            "- Horizon sleeves are equal-weighted at each trade_time before portfolio Sortino is computed.",
            "- `long_short_spread` is a ranking proxy and is not CN/A-share tradable reward.",
            "- CN/A-share train reward should use a long-only mode; costs are turnover-adjusted and still not a full fill simulator.",
            "- Phase3BZ fragment replay remains available only as diagnostic slice replay.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-audit", type=Path, default=DEFAULT_CANDIDATE_AUDIT)
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--candidate-limit", type=int, default=64)
    parser.add_argument("--max-shards", type=int, default=8)
    parser.add_argument("--sample-trade-times-per-shard", type=int, default=240)
    parser.add_argument("--sample-block-count", type=int, default=1)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--min-obs-per-time", type=int, default=20)
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--top-quantile", type=float, default=0.2)
    parser.add_argument(
        "--portfolio-mode",
        choices=("long_short_spread", "long_only_top", "long_only_excess_market"),
        default="long_only_top",
        help=(
            "Return construction mode. long_short_spread is a ranking proxy; "
            "CN tradable reward should use long_only_top or long_only_excess_market."
        ),
    )
    parser.add_argument("--write-pnl-rows", action="store_true")
    parser.add_argument("--fast-mode", action="store_true")
    parser.add_argument("--numexpr-threads", type=int, default=4)
    parser.add_argument("--checkpoint-every-candidates", type=int, default=8)
    parser.add_argument("--disable-incremental-checkpoints", action="store_true")
    parser.add_argument("--drop-hard-blocked-input", action="store_true")
    parser.add_argument("--rank-ic-loss-weight", type=float, default=6.0)
    parser.add_argument("--rank-ic-component-cap", type=float, default=0.35)
    parser.add_argument("--regime-stability-weight", type=float, default=0.08)
    parser.add_argument("--regime-component-cap", type=float, default=0.10)
    parser.add_argument("--operator-cache-max-entries", type=int, default=512)
    parser.add_argument("--feature-matrix-cache-max-windows", type=int, default=6)
    parser.add_argument("--disable-factor-expression-cache", action="store_true")
    parser.add_argument("--disable-operator-cache", action="store_true")
    parser.add_argument("--disable-feature-matrix-cache", action="store_true")
    parser.add_argument("--disable-fast-portfolio-loop", action="store_true")
    parser.add_argument("--disable-schema-gate", action="store_true")
    parser.add_argument("--disable-legacy-alias-rewrite", action="store_true")
    parser.add_argument("--m1-first-ret-replacement", default="m1_first5_last_return_vs_open")
    args = parser.parse_args(argv)

    if args.fast_mode:
        os.environ.setdefault("NUMEXPR_MAX_THREADS", str(args.numexpr_threads))
        os.environ.setdefault("OMP_NUM_THREADS", "1")
        os.environ.setdefault("MKL_NUM_THREADS", "1")

    if args.train_fraction <= 0 or args.validation_fraction < 0 or args.train_fraction + args.validation_fraction >= 1:
        raise ValueError("train_fraction must be >0 and train_fraction + validation_fraction must be < 1")
    if args.sample_block_count <= 0:
        raise ValueError("sample_block_count must be positive")

    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    horizons = tuple(int(item.strip()) for item in str(args.horizons).split(",") if item.strip())
    candidates = _load_candidates(
        _resolve(args.candidate_audit),
        args.candidate_limit,
        drop_hard_blocked_input=bool(args.drop_hard_blocked_input),
        enable_legacy_alias_rewrite=not bool(args.disable_legacy_alias_rewrite),
        m1_first_ret_replacement=str(args.m1_first_ret_replacement),
    )
    panels = _discover_panels(_resolve(args.shard_root), args.max_shards)
    input_candidate_count = len(candidates)
    schema_held_candidates: list[dict[str, Any]] = []
    schema_field_count: int | None = None
    if not args.disable_schema_gate:
        schema_fields = _schema_intersection(panels)
        schema_field_count = len(schema_fields)
        candidates, schema_held_candidates = _schema_gate_candidates(candidates, schema_fields)

    rows_by_hash: dict[str, list[dict[str, Any]]] = {str(candidate["expression_hash"]): [] for candidate in candidates}
    reward_by_hash: dict[str, dict[str, Any]] = {}
    progress_rows: list[dict[str, Any]] = []
    processed_candidate_shards = 0
    pnl_rows: list[dict[str, Any]] = []
    shard_meta: list[dict[str, Any]] = []
    global_cache_stats: dict[str, int] = {}
    for shard_index, panel in enumerate(panels):
        if not candidates:
            break
        for sample_block_index in range(int(args.sample_block_count)):
            (
                frame,
                eval_mask,
                eval_frame,
                labels,
                signal_times,
                full_signal_times,
                context_times_by_window,
                meta,
            ) = _read_train_shard(
                candidates=candidates,
                panel_path=panel,
                horizons=horizons,
                sample_trade_times=args.sample_trade_times_per_shard,
                sample_block_count=args.sample_block_count,
                sample_block_index=sample_block_index,
            )
            meta["shard_index"] = shard_index
            split_by_time = _split_map(full_signal_times, args.train_fraction, args.validation_fraction)
            eval_time_index = None if args.disable_fast_portfolio_loop else _build_eval_time_index(eval_frame)
            meta["fast_portfolio_loop"] = not bool(args.disable_fast_portfolio_loop)
            meta["eval_time_group_count"] = len(eval_time_index["groups"]) if eval_time_index is not None else None
            expression_cache: dict[str, pd.Series] = {} if not args.disable_factor_expression_cache else {}
            feature_matrix_cache: dict[int, tuple[pd.DataFrame, pd.Series]] = {}
            operator_cache_by_window: dict[int, dict[str, pd.Series]] = {}
            shard_cache_stats: dict[str, int] = {}
            shard_rows = 0
            for candidate_index, candidate in enumerate(candidates, 1):
                rows = _candidate_portfolio_rows_from_frame(
                    candidate=candidate,
                    frame=frame,
                    eval_mask=eval_mask,
                    eval_frame=eval_frame,
                    labels=labels,
                    eval_time_index=eval_time_index,
                    split_by_time=split_by_time,
                    context_times_by_window=context_times_by_window,
                    shard_index=shard_index,
                    horizons=horizons,
                    min_obs=args.min_obs_per_time,
                    cost_bps=args.cost_bps,
                    top_quantile=args.top_quantile,
                    portfolio_mode=args.portfolio_mode,
                    expression_cache=expression_cache if not args.disable_factor_expression_cache else {},
                    feature_matrix_cache=feature_matrix_cache if not args.disable_feature_matrix_cache else {},
                    operator_cache_by_window=operator_cache_by_window if not args.disable_operator_cache else {},
                    cache_stats=shard_cache_stats,
                    operator_cache_max_entries=-1 if args.disable_operator_cache else args.operator_cache_max_entries,
                    feature_matrix_cache_max_windows=0 if args.disable_feature_matrix_cache else args.feature_matrix_cache_max_windows,
                )
                expression_hash = str(candidate["expression_hash"])
                rows_by_hash[expression_hash].extend(rows)
                shard_rows += len(rows)
                processed_candidate_shards += 1
                progress_rows.append(
                    {
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "candidate_id": candidate.get("candidate_id"),
                        "expression_hash": expression_hash,
                        "generator_arm": candidate.get("generator_arm"),
                        "shard_index": shard_index,
                        "sample_block_index": sample_block_index,
                        "sample_block_count": int(args.sample_block_count),
                        "candidate_index": candidate_index,
                        "processed_candidate_shards": processed_candidate_shards,
                        "rows_added": len(rows),
                        "cumulative_rows_for_candidate": len(rows_by_hash[expression_hash]),
                        "expression_cache_size": len(expression_cache),
                        "checkpoint_partial": True,
                    }
                )
                if not args.disable_incremental_checkpoints:
                    _, reward_row = _candidate_summary(
                        candidate,
                        rows_by_hash[expression_hash],
                        horizons,
                        seed=20260623 + candidate_index,
                        rank_ic_loss_weight=args.rank_ic_loss_weight,
                        rank_ic_component_cap=args.rank_ic_component_cap,
                        regime_stability_weight=args.regime_stability_weight,
                        regime_component_cap=args.regime_component_cap,
                    )
                    reward_row["checkpoint_partial"] = True
                    reward_row["checkpoint_shards_seen"] = shard_index + 1
                    reward_row["checkpoint_sample_block_count"] = int(args.sample_block_count)
                    reward_row["checkpoint_processed_candidate_shards"] = processed_candidate_shards
                    reward_by_hash[expression_hash] = reward_row
                    if processed_candidate_shards % max(1, int(args.checkpoint_every_candidates)) == 0:
                        _write_incremental_checkpoint(
                            output_root=output_root,
                            report_root=report_root,
                            reward_by_hash=reward_by_hash,
                            progress_rows=progress_rows,
                            shard_meta=shard_meta,
                            processed_candidate_shards=processed_candidate_shards,
                            candidate_count=len(candidates),
                            shard_count=len(panels),
                        )
                if args.write_pnl_rows:
                    pnl_rows.extend(rows)
            meta["portfolio_pnl_rows"] = shard_rows
            meta["expression_cache_size"] = len(expression_cache)
            meta["feature_matrix_cache_windows"] = len(feature_matrix_cache)
            meta["operator_cache_contexts"] = len(operator_cache_by_window)
            meta["operator_cache_entries"] = sum(len(cache) for cache in operator_cache_by_window.values())
            for key, value in shard_cache_stats.items():
                meta[f"cache_{key}"] = value
                global_cache_stats[key] = global_cache_stats.get(key, 0) + int(value)
            meta["shard_complete"] = sample_block_index == int(args.sample_block_count) - 1
            meta["sample_block_complete"] = True
            meta["processed_candidate_shards"] = processed_candidate_shards
            shard_meta.append(meta)
            if not args.disable_incremental_checkpoints:
                _write_incremental_checkpoint(
                    output_root=output_root,
                    report_root=report_root,
                    reward_by_hash=reward_by_hash,
                    progress_rows=progress_rows,
                    shard_meta=shard_meta,
                    processed_candidate_shards=processed_candidate_shards,
                    candidate_count=len(candidates),
                    shard_count=len(panels),
                )
            del frame, eval_mask, eval_frame, labels, eval_time_index, expression_cache, feature_matrix_cache, operator_cache_by_window

    split_horizon_rows: list[dict[str, Any]] = []
    reward_rows: list[dict[str, Any]] = [
        _schema_hold_reward_row(candidate, portfolio_mode=args.portfolio_mode)
        for candidate in schema_held_candidates
    ]
    for idx, candidate in enumerate(candidates, 1):
        rows = rows_by_hash[str(candidate["expression_hash"])]
        per_split, reward_row = _candidate_summary(
            candidate,
            rows,
            horizons,
            seed=20260623 + idx,
            rank_ic_loss_weight=args.rank_ic_loss_weight,
            rank_ic_component_cap=args.rank_ic_component_cap,
            regime_stability_weight=args.regime_stability_weight,
            regime_component_cap=args.regime_component_cap,
        )
        for row in per_split:
            split_horizon_rows.append(
                {
                    "candidate_id": candidate.get("candidate_id"),
                    "expression_hash": candidate.get("expression_hash"),
                    "generator_arm": candidate.get("generator_arm"),
                    "factor_lane": candidate.get("factor_lane"),
                    "expression": candidate.get("expression"),
                    **row,
                }
            )
        reward_rows.append(reward_row)

    reward_rows.sort(key=lambda row: _f(row.get("train_reward"), -999.0), reverse=True)
    followup_count = sum(1 for row in reward_rows if row.get("train_reward_decision") == "TRAIN_REWARD_FOLLOWUP_READY")
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cm_train_portfolio_sortino_reward_audit",
        "decision": "PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY",
        "candidate_count": input_candidate_count,
        "runnable_candidate_count": len(candidates),
        "schema_gate_enabled": not bool(args.disable_schema_gate),
        "schema_gate_field_count": schema_field_count,
        "schema_gate_held_count": len(schema_held_candidates),
        "followup_count": followup_count,
        "input_candidate_audit": str(_resolve(args.candidate_audit)),
        "shard_root": str(_resolve(args.shard_root)),
        "max_shards": args.max_shards,
        "sample_trade_times_per_shard": args.sample_trade_times_per_shard,
        "sample_block_count": int(args.sample_block_count),
        "horizons": list(horizons),
        "train_fraction": args.train_fraction,
        "validation_fraction": args.validation_fraction,
        "holdout_fraction": round(1.0 - args.train_fraction - args.validation_fraction, 8),
        "cost_bps": args.cost_bps,
        "top_quantile": args.top_quantile,
        "portfolio_mode": args.portfolio_mode,
        "short_allowed": bool(args.portfolio_mode == "long_short_spread"),
        "rank_ic_loss_weight": args.rank_ic_loss_weight,
        "rank_ic_component_cap": args.rank_ic_component_cap,
        "regime_stability_weight": args.regime_stability_weight,
        "regime_component_cap": args.regime_component_cap,
        "optimizer_reward_metric": OPTIMIZER_REWARD_METRIC,
        "portfolio_pnl_rows_written": len(pnl_rows) if args.write_pnl_rows else 0,
        "metric_boundary": "train portfolio Sortino + rankIC loss reward audit; not production proof; validation/holdout must not feed search; long_short_spread is not CN tradable",
        "fast_mode": bool(args.fast_mode),
        "numexpr_threads": int(args.numexpr_threads),
        "incremental_checkpoints_enabled": not bool(args.disable_incremental_checkpoints),
        "checkpoint_every_candidates": int(args.checkpoint_every_candidates),
        "drop_hard_blocked_input": bool(args.drop_hard_blocked_input),
        "legacy_alias_rewrite_enabled": not bool(args.disable_legacy_alias_rewrite),
        "m1_first_ret_replacement": str(args.m1_first_ret_replacement),
        "legacy_alias_rewrite_count": sum(1 for candidate in [*schema_held_candidates, *candidates] if candidate.get("legacy_alias_rewrites")),
        "python_executable": os.sys.executable,
        "package_versions": _package_versions(),
        "cache_stats": global_cache_stats,
        "acceleration_contract": {
            "batched_shard_read": True,
            "sample_blocked_read": int(args.sample_block_count) > 1,
            "column_pruned_pyarrow_read": True,
            "factor_expression_cache": not bool(args.disable_factor_expression_cache),
            "feature_matrix_cache": not bool(args.disable_feature_matrix_cache),
            "operator_subtree_cache": not bool(args.disable_operator_cache),
            "expression_cache_scope": "per_shard",
            "feature_matrix_cache_scope": "per_shard_context_window",
            "operator_cache_scope": "per_shard_context_window",
            "fast_portfolio_loop": not bool(args.disable_fast_portfolio_loop),
            "numba_rank_available": njit is not None,
            "numba_rank_enabled": (
                njit is not None
                and os.environ.get("PHASE3CM_DISABLE_NUMBA", "0") != "1"
                and not _NUMBA_RANK_RUNTIME_DISABLED
            ),
            "operator_cache_max_entries": int(args.operator_cache_max_entries),
            "feature_matrix_cache_max_windows": int(args.feature_matrix_cache_max_windows),
            "fast_group_rank": True,
            "omp_threads": os.environ.get("OMP_NUM_THREADS"),
            "mkl_threads": os.environ.get("MKL_NUM_THREADS"),
            "numexpr_max_threads": os.environ.get("NUMEXPR_MAX_THREADS"),
            "parallel_workers": 1,
            "global_worker_limit": 1,
        },
    }
    if args.write_pnl_rows:
        _write_csv(output_root / "phase3cm_portfolio_pnl_rows.csv", pnl_rows)
    if not args.disable_incremental_checkpoints:
        _write_incremental_checkpoint(
            output_root=output_root,
            report_root=report_root,
            reward_by_hash={str(row.get("expression_hash")): row for row in reward_rows},
            progress_rows=progress_rows,
            shard_meta=shard_meta,
            processed_candidate_shards=processed_candidate_shards,
            candidate_count=len(candidates),
            shard_count=len(panels),
            final=True,
        )
    _write_csv(output_root / "phase3cm_candidate_split_horizon_summary.csv", split_horizon_rows)
    _write_csv(output_root / "phase3cm_candidate_train_reward_summary.csv", reward_rows)
    _write_csv(output_root / "phase3cm_train_reward.csv", reward_rows)
    _write_csv(output_root / "phase3cm_shard_meta.csv", shard_meta)
    _write_json(output_root / "phase3cm_train_reward_audit_summary.json", summary)
    report_root.mkdir(parents=True, exist_ok=True)
    _write_csv(report_root / "phase3cm_candidate_train_reward_summary.csv", reward_rows)
    _write_csv(report_root / "phase3cm_train_reward.csv", reward_rows)
    _write_csv(report_root / "phase3cm_candidate_split_horizon_summary.csv", split_horizon_rows)
    _write_json(report_root / "phase3cm_train_reward_audit_summary.json", summary)
    markdown_path = report_root / "PHASE3CM_TRAIN_PORTFOLIO_SORTINO_REWARD_AUDIT_20260623.md"
    try:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(_render_md(summary, reward_rows), encoding="utf-8")
    except OSError as exc:
        _write_json(
            output_root / "phase3cm_report_markdown_write_warning.json",
            {
                "warning": "report_markdown_write_failed_after_core_outputs",
                "path": str(markdown_path),
                "error": str(exc),
            },
        )
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
