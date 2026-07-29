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
import shutil
import statistics
import time
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
from our_system_phase2.services.a_share_tradability_guard import (
    development_predictive_evidence,
    read_a_share_tradability_receipts,
)
from our_system_phase2.services.candidate_schema import OPTIMIZER_REWARD_METRIC, normalize_candidate_schema
from our_system_phase2.services.candidate_submission_receipt import (
    CandidateSubmissionAuthority,
    ReceiptContext,
    read_receipt_table,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.matched_control_pairs import (
    CandidatePairAuthority,
    PAIR_MATURITY_ALIGNMENT_POLICY,
    PAIR_SUPPORT_ALIGNMENT_POLICY,
    build_pair_evaluation_rows,
    flatten_candidate_pairs,
    group_candidate_pairs,
    read_pair_receipt_table,
)
from our_system_phase2.services.real_market_validation import (
    evaluate_panel_expression,
    frozen_replay_channels,
)
from our_system_phase2.services.signal_vector_semantics import build_signal_semantic_diagnostics
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry, stable_hash


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
PERSISTENT_CACHE_VERSION = "phase3cm_persistent_series_cache_v1"
DEFAULT_PERSISTENT_CACHE_MAX_GB = 120.0
DEFAULT_PERSISTENT_CACHE_TTL_DAYS = 7.0
_PERSISTENT_CACHE_PRUNE_LAST_RUN: dict[str, float] = {}


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


def _candidate_event_fields(candidates: list[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            str(field)
            for candidate in candidates
            for field in candidate.get("fields_list", [])
            if str(field).startswith("evt_")
        }
    )


def _event_signal_positions(
    panel_path: Path,
    *,
    all_trade_times: pd.Series,
    event_fields: list[str],
    max_event_trade_times: int | None,
    forward_bars: int = 20,
) -> list[int]:
    if not event_fields:
        return []
    schema = set(pq.ParquetFile(panel_path).schema_arrow.names)
    fields = [field for field in event_fields if field in schema]
    if not fields or "trade_time" not in schema:
        return []
    table = pq.read_table(panel_path, columns=["trade_time", *fields])
    frame = table.to_pandas()
    if frame.empty:
        return []
    position_by_time = {pd.Timestamp(value): idx for idx, value in enumerate(pd.to_datetime(all_trade_times))}
    max_pos = len(position_by_time) - 1
    cap = int(max_event_trade_times or 0)
    position_set: set[int] = set()
    for field in fields:
        values = pd.to_numeric(frame[field], errors="coerce")
        mask = values.notna() & (values != 0.0)
        if not bool(mask.any()):
            continue
        event_times = pd.to_datetime(frame.loc[mask, "trade_time"], errors="coerce").dropna().drop_duplicates().sort_values()
        positions = sorted({position_by_time[pd.Timestamp(value)] for value in event_times if pd.Timestamp(value) in position_by_time})
        if cap > 0 and len(positions) > cap:
            pick = np.unique(np.linspace(0, len(positions) - 1, cap).round().astype(int))
            positions = [positions[int(idx)] for idx in pick]
        for pos in positions:
            end = min(max_pos, int(pos) + max(0, int(forward_bars)))
            position_set.update(range(int(pos), end + 1))
    return sorted(position_set)


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


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def _hash_items(items: list[str]) -> str:
    digest = hashlib.sha256()
    for item in items:
        digest.update(str(item).encode("utf-8", errors="replace"))
        digest.update(b"\0")
    return digest.hexdigest()


def _path_fingerprint(path: Path) -> str:
    resolved = _resolve(path)
    try:
        stat = resolved.stat()
        payload = f"{resolved}|{stat.st_size}|{stat.st_mtime_ns}"
    except OSError:
        payload = str(resolved)
    return _sha256_text(payload)


def _timestamp_fingerprint(values: set[pd.Timestamp] | list[pd.Timestamp] | pd.Series) -> str:
    if isinstance(values, pd.Series):
        items = pd.to_datetime(values, errors="coerce").dropna().astype("int64").astype(str).tolist()
    else:
        items = [str(pd.Timestamp(value).value) for value in values if pd.notna(value)]
    items.sort()
    return _hash_items(items)


def _persistent_namespace(
    *,
    kind: str,
    panel_fingerprint: str,
    columns_fingerprint: str,
    sample_block_index: int,
    context_window: int,
    context_fingerprint: str,
    eval_fingerprint: str,
) -> str:
    payload = {
        "version": PERSISTENT_CACHE_VERSION,
        "kind": kind,
        "panel": panel_fingerprint,
        "columns": columns_fingerprint,
        "sample_block_index": int(sample_block_index),
        "context_window": int(context_window),
        "context_trade_times": context_fingerprint,
        "eval_trade_times": eval_fingerprint,
    }
    return _sha256_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def _series_to_float64(series: pd.Series) -> np.ndarray:
    return pd.to_numeric(series, errors="coerce").to_numpy(dtype=np.float64, copy=False)


def _read_cached_series(path: Path, expected_length: int | None, stats: dict[str, int], prefix: str) -> pd.Series | None:
    try:
        if not path.exists():
            _inc(stats, f"{prefix}_disk_misses")
            return None
        with path.open("rb") as handle:
            values = np.load(handle, allow_pickle=False)
        if expected_length is not None and len(values) != int(expected_length):
            _inc(stats, f"{prefix}_disk_length_mismatch")
            return None
        _inc(stats, f"{prefix}_disk_hits")
        return pd.Series(values)
    except Exception:
        _inc(stats, f"{prefix}_disk_read_errors")
        return None


def _persistent_cache_write_allowed(
    path: Path,
    *,
    stats: dict[str, int],
    prefix: str,
    min_free_gb: float,
    max_gb: float,
    ttl_days: float,
    estimated_bytes: int = 0,
) -> bool:
    _persistent_cache_maybe_prune(path, stats=stats, prefix=prefix, max_gb=max_gb, ttl_days=ttl_days)
    min_free_bytes = max(0, int(float(min_free_gb or 0.0) * (1024**3)))
    if min_free_bytes <= 0:
        return True
    try:
        parent = path.parent
        parent.mkdir(parents=True, exist_ok=True)
        free_bytes = int(shutil.disk_usage(parent).free)
        required_free = min_free_bytes + max(0, int(estimated_bytes or 0))
        if free_bytes <= required_free:
            _inc(stats, f"{prefix}_disk_write_skipped_low_space")
            return False
        return True
    except Exception:
        _inc(stats, f"{prefix}_disk_space_check_errors")
        return False


def _persistent_cache_budget_root(path: Path) -> Path:
    for parent in path.resolve().parents:
        if parent.name == PERSISTENT_CACHE_VERSION:
            return parent
    return path.parent


def _persistent_cache_maybe_prune(
    path: Path,
    *,
    stats: dict[str, int],
    prefix: str,
    max_gb: float,
    ttl_days: float,
    min_interval_sec: float = 300.0,
) -> None:
    if float(max_gb or 0.0) <= 0 and float(ttl_days or 0.0) <= 0:
        return
    try:
        budget_root = _persistent_cache_budget_root(path)
        key = str(budget_root)
        now = time.time()
        last = _PERSISTENT_CACHE_PRUNE_LAST_RUN.get(key, 0.0)
        if now - last < float(min_interval_sec):
            return
        _PERSISTENT_CACHE_PRUNE_LAST_RUN[key] = now
        _persistent_cache_prune(
            budget_root,
            stats=stats,
            prefix=prefix,
            max_gb=float(max_gb or 0.0),
            ttl_days=float(ttl_days or 0.0),
        )
    except Exception:
        _inc(stats, f"{prefix}_disk_prune_errors")


def _persistent_cache_prune(
    budget_root: Path,
    *,
    stats: dict[str, int],
    prefix: str,
    max_gb: float,
    ttl_days: float,
) -> None:
    if not budget_root.exists():
        return
    files: list[Path] = [path for path in budget_root.rglob("*") if path.is_file()]
    now = time.time()
    cutoff = now - float(ttl_days) * 86400.0 if ttl_days > 0 else None
    total_bytes = 0
    file_rows: list[tuple[float, int, Path]] = []
    for file_path in files:
        try:
            stat = file_path.stat()
        except OSError:
            continue
        total_bytes += int(stat.st_size)
        file_rows.append((float(stat.st_mtime), int(stat.st_size), file_path))
        if cutoff is not None and float(stat.st_mtime) < cutoff:
            try:
                file_path.unlink()
                total_bytes -= int(stat.st_size)
                _inc(stats, f"{prefix}_disk_pruned_ttl")
            except OSError:
                _inc(stats, f"{prefix}_disk_prune_unlink_errors")
    max_bytes = int(max_gb * (1024**3)) if max_gb > 0 else 0
    if max_bytes <= 0 or total_bytes <= max_bytes:
        return
    for _mtime, size, file_path in sorted(file_rows, key=lambda row: row[0]):
        if total_bytes <= max_bytes:
            break
        if not file_path.exists():
            continue
        try:
            file_path.unlink()
            total_bytes -= int(size)
            _inc(stats, f"{prefix}_disk_pruned_budget")
        except OSError:
            _inc(stats, f"{prefix}_disk_prune_unlink_errors")


def _write_cached_series(
    path: Path,
    series: pd.Series,
    stats: dict[str, int],
    prefix: str,
    *,
    min_free_gb: float = 0.0,
    max_gb: float = DEFAULT_PERSISTENT_CACHE_MAX_GB,
    ttl_days: float = DEFAULT_PERSISTENT_CACHE_TTL_DAYS,
) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        estimated_bytes = int(getattr(series, "size", len(series)) or 0) * 8
        if not _persistent_cache_write_allowed(
            path,
            stats=stats,
            prefix=prefix,
            min_free_gb=min_free_gb,
            max_gb=max_gb,
            ttl_days=ttl_days,
            estimated_bytes=estimated_bytes,
        ):
            return
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        with tmp.open("wb") as handle:
            np.save(handle, _series_to_float64(series), allow_pickle=False)
        os.replace(tmp, path)
        _inc(stats, f"{prefix}_disk_stores")
    except Exception:
        _inc(stats, f"{prefix}_disk_write_errors")
        try:
            if "tmp" in locals() and tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _persistent_cache_active(root: Path | None, mode: str) -> bool:
    return root is not None and str(mode).lower() in {"read", "write", "readwrite"}


def _feature_matrix_cache_path(root: Path, namespace: str) -> Path:
    return root / PERSISTENT_CACHE_VERSION / "feature_matrix" / namespace[:2] / f"{namespace}.parquet"


def _read_cached_feature_matrix(
    path: Path,
    *,
    expected_columns: list[str],
    stats: dict[str, int],
    prefix: str,
) -> tuple[pd.DataFrame, pd.Series] | None:
    try:
        if not path.exists():
            _inc(stats, f"{prefix}_disk_misses")
            return None
        frame = pd.read_parquet(path)
        marker = "__phase3cm_eval_mask"
        if marker not in frame.columns:
            _inc(stats, f"{prefix}_disk_schema_mismatch")
            return None
        eval_mask = frame.pop(marker).astype(bool)
        if list(frame.columns) != list(expected_columns):
            _inc(stats, f"{prefix}_disk_column_mismatch")
            return None
        _inc(stats, f"{prefix}_disk_hits")
        return frame, eval_mask
    except Exception:
        _inc(stats, f"{prefix}_disk_read_errors")
        return None


def _write_cached_feature_matrix(
    path: Path,
    *,
    frame: pd.DataFrame,
    eval_mask: pd.Series,
    stats: dict[str, int],
    prefix: str,
    min_free_gb: float = 0.0,
    max_gb: float = DEFAULT_PERSISTENT_CACHE_MAX_GB,
    ttl_days: float = DEFAULT_PERSISTENT_CACHE_TTL_DAYS,
) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        estimated_bytes = int(frame.memory_usage(index=False, deep=False).sum()) + int(len(eval_mask))
        if not _persistent_cache_write_allowed(
            path,
            stats=stats,
            prefix=prefix,
            min_free_gb=min_free_gb,
            max_gb=max_gb,
            ttl_days=ttl_days,
            estimated_bytes=estimated_bytes,
        ):
            return
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        payload = frame.copy()
        payload["__phase3cm_eval_mask"] = eval_mask.to_numpy(dtype=bool)
        payload.to_parquet(tmp, index=False)
        os.replace(tmp, path)
        _inc(stats, f"{prefix}_disk_stores")
    except Exception:
        _inc(stats, f"{prefix}_disk_write_errors")
        try:
            if "tmp" in locals() and tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _feature_matrix_value_bytes(value: tuple[pd.DataFrame, pd.Series]) -> int:
    frame, eval_mask = value
    frame_bytes = int(frame.memory_usage(index=True, deep=False).sum())
    mask_bytes = int(eval_mask.memory_usage(index=True, deep=False))
    return max(0, frame_bytes + mask_bytes)


def _store_bounded_feature_matrix(
    cache: dict[int, tuple[pd.DataFrame, pd.Series]],
    key: int,
    value: tuple[pd.DataFrame, pd.Series],
    *,
    stats: dict[str, int],
    max_windows: int,
    max_bytes: int,
) -> bool:
    max_windows = max(0, int(max_windows))
    max_bytes = max(0, int(max_bytes))
    if max_windows == 0:
        _inc(stats, "feature_matrix_cache_skipped_capacity")
        return False
    value_bytes = _feature_matrix_value_bytes(value)
    if max_bytes and value_bytes > max_bytes:
        _inc(stats, "feature_matrix_cache_skipped_byte_capacity")
        return False
    current_bytes = int(stats.get("feature_matrix_cache_memory_bytes_current", 0))
    while cache and (
        len(cache) >= max_windows
        or (max_bytes and current_bytes + value_bytes > max_bytes)
    ):
        oldest = next(iter(cache))
        evicted = cache.pop(oldest)
        current_bytes = max(0, current_bytes - _feature_matrix_value_bytes(evicted))
        _inc(stats, "feature_matrix_cache_memory_evictions")
    if max_bytes and current_bytes + value_bytes > max_bytes:
        _inc(stats, "feature_matrix_cache_skipped_byte_capacity")
        return False
    cache[key] = value
    current_bytes += value_bytes
    stats["feature_matrix_cache_memory_bytes_current"] = current_bytes
    stats["feature_matrix_cache_memory_bytes_peak"] = max(
        stats.get("feature_matrix_cache_memory_bytes_peak", 0),
        current_bytes,
    )
    _inc(stats, "feature_matrix_cache_stores")
    return True


class _BoundedSeriesCache(dict[str, pd.Series]):
    """Small in-memory cache for expression operator subtrees.

    The evaluator stores every recursively evaluated subtree in the provided
    cache. Keeping this bounded is important because each value is a full panel
    series and Phase3CM may run many worker processes in parallel.
    """

    def __init__(
        self,
        *,
        max_entries: int,
        stats: dict[str, int],
        prefix: str,
        max_bytes: int = 0,
    ) -> None:
        super().__init__()
        self.max_entries = max(0, int(max_entries))
        self.max_bytes = max(0, int(max_bytes))
        self.current_bytes = 0
        self.stats = stats
        self.prefix = prefix

    @staticmethod
    def _value_bytes(value: pd.Series) -> int:
        try:
            return max(0, int(value.memory_usage(index=False, deep=False)))
        except Exception:
            return max(0, int(getattr(value, "nbytes", 0) or 0))

    def _evict_oldest(self) -> bool:
        try:
            oldest = next(iter(self.keys()))
        except StopIteration:
            return False
        value = dict.__getitem__(self, oldest)
        self.current_bytes = max(0, self.current_bytes - self._value_bytes(value))
        dict.__delitem__(self, oldest)
        _inc(self.stats, f"{self.prefix}_memory_evictions")
        return True

    def _store_memory(self, key: str, value: pd.Series) -> bool:
        value_bytes = self._value_bytes(value)
        if self.max_bytes and value_bytes > self.max_bytes:
            _inc(self.stats, f"{self.prefix}_skipped_byte_capacity")
            return False

        exists = dict.__contains__(self, key)
        if exists:
            old_value = dict.__getitem__(self, key)
            self.current_bytes = max(0, self.current_bytes - self._value_bytes(old_value))
            dict.__delitem__(self, key)
        while self and (
            (self.max_entries and len(self) >= self.max_entries)
            or (self.max_bytes and self.current_bytes + value_bytes > self.max_bytes)
        ):
            if not self._evict_oldest():
                break
        if self.max_bytes and self.current_bytes + value_bytes > self.max_bytes:
            _inc(self.stats, f"{self.prefix}_skipped_byte_capacity")
            return False
        if not exists:
            _inc(self.stats, f"{self.prefix}_stores")
        dict.__setitem__(self, key, value)
        self.current_bytes += value_bytes
        self.stats[f"{self.prefix}_memory_bytes_current"] = self.current_bytes
        self.stats[f"{self.prefix}_memory_bytes_peak"] = max(
            self.stats.get(f"{self.prefix}_memory_bytes_peak", 0),
            self.current_bytes,
        )
        return True

    def __contains__(self, key: object) -> bool:
        hit = super().__contains__(key)
        self.stats[f"{self.prefix}_hits" if hit else f"{self.prefix}_misses"] = (
            self.stats.get(f"{self.prefix}_hits" if hit else f"{self.prefix}_misses", 0) + 1
        )
        return hit

    def __setitem__(self, key: str, value: pd.Series) -> None:
        self._store_memory(key, value)

    def __getitem__(self, key: str) -> pd.Series:
        value = dict.__getitem__(self, key)
        dict.__delitem__(self, key)
        dict.__setitem__(self, key, value)
        return value


class _PersistentSeriesCache(_BoundedSeriesCache):
    """Bounded in-memory series cache with a disk backing store."""

    def __init__(
        self,
        *,
        root: Path,
        namespace: str,
        max_entries: int,
        stats: dict[str, int],
        prefix: str,
        mode: str,
        expected_length: int | None,
        min_free_gb: float = 0.0,
        max_gb: float = DEFAULT_PERSISTENT_CACHE_MAX_GB,
        ttl_days: float = DEFAULT_PERSISTENT_CACHE_TTL_DAYS,
        max_bytes: int = 0,
    ) -> None:
        super().__init__(max_entries=max_entries, stats=stats, prefix=prefix, max_bytes=max_bytes)
        self.root = root
        self.namespace = namespace
        self.mode = mode
        self.expected_length = expected_length
        self.min_free_gb = float(min_free_gb or 0.0)
        self.max_gb = float(max_gb or 0.0)
        self.ttl_days = float(ttl_days or 0.0)
        # Keep the namespace in the digest rather than the directory name.
        # Windows workers otherwise hit MAX_PATH on deep remote cache roots.
        self.namespace_root = root / PERSISTENT_CACHE_VERSION / "series" / namespace[:2]

    @property
    def can_read(self) -> bool:
        return self.mode in {"read", "readwrite"}

    @property
    def can_write(self) -> bool:
        return self.mode in {"write", "readwrite"}

    def _path_for_key(self, key: str) -> Path:
        digest = _sha256_text(f"{self.namespace}\0{key}")
        return self.namespace_root / digest[:2] / f"{digest}.npy"

    def _put_memory(self, key: str, value: pd.Series) -> None:
        self._store_memory(key, value)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if dict.__contains__(self, key):
            _inc(self.stats, f"{self.prefix}_hits")
            return True
        if not self.can_read:
            _inc(self.stats, f"{self.prefix}_misses")
            return False
        series = _read_cached_series(self._path_for_key(key), self.expected_length, self.stats, self.prefix)
        if series is None:
            _inc(self.stats, f"{self.prefix}_misses")
            return False
        self._put_memory(key, series)
        _inc(self.stats, f"{self.prefix}_hits")
        return True

    def __getitem__(self, key: str) -> pd.Series:
        if dict.__contains__(self, key):
            return super().__getitem__(key)
        if self.can_read:
            series = _read_cached_series(self._path_for_key(key), self.expected_length, self.stats, self.prefix)
            if series is not None:
                self._put_memory(key, series)
                return series
        raise KeyError(key)

    def __setitem__(self, key: str, value: pd.Series) -> None:
        self._put_memory(key, value)
        if self.can_write:
            _write_cached_series(
                self._path_for_key(key),
                value,
                self.stats,
                self.prefix,
                min_free_gb=self.min_free_gb,
                max_gb=self.max_gb,
                ttl_days=self.ttl_days,
            )


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
    pairs = group_candidate_pairs(rows)
    pairs.sort(
        key=lambda pair: (
            _f(pair[0].get("phase3ca_proxy_quality"), -999.0),
            _f(pair[0].get("aligned_ic_mean") or pair[0].get("abs_aligned_ic_mean"), -999.0),
            str(pair[0].get("pair_id") or ""),
        ),
        reverse=True,
    )
    rows = flatten_candidate_pairs(pairs[: max(1, int(limit))])
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
        item["runtime_fields_list"] = frozen_replay_channels(expression)
        item["max_window"] = _max_expression_window(expression)
        selected.append(item)
    if not selected:
        raise RuntimeError(f"no candidates selected from {path}")
    # Re-validate after legacy alias handling/dedup so a formal worker can
    # never receive a primary without its control.
    return flatten_candidate_pairs(group_candidate_pairs(selected))


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
    for primary, control in group_candidate_pairs(candidates):
        pair = [primary, control]
        missing = sorted(
            {
                field
                for candidate in pair
                for field in (
                    set(candidate.get("fields_list") or ())
                    | set(candidate.get("runtime_fields_list") or ())
                )
                - set(available_fields)
            }
        )
        target = held if missing else runnable
        for candidate in pair:
            item = dict(candidate)
            if missing:
                item["missing_schema_fields"] = "|".join(missing)
            target.append(item)
    return runnable, held


def _schema_hold_reward_row(candidate: dict[str, Any], *, portfolio_mode: str) -> dict[str, Any]:
    missing = str(candidate.get("missing_schema_fields") or "")
    blockers = "candidate_missing_schema_fields"
    if missing:
        blockers = f"{blockers}:{missing}"
    row = {
        "candidate_id": candidate.get("candidate_id"),
        "candidate_submission_receipt_id": candidate.get("candidate_submission_receipt_id"),
        "candidate_submission_receipt_hash": candidate.get("candidate_submission_receipt_hash"),
        "candidate_submission_authorization": candidate.get("candidate_submission_authorization"),
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
        **development_predictive_evidence(),
    }
    row.update(normalize_candidate_schema(row))
    return row


def _row_trade_date(row: dict[str, Any]) -> str:
    value = row.get("trade_date") or row.get("trade_time")
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return pd.Timestamp(parsed).date().isoformat()


def normalize_against_fixed_manifest(
    rows: list[dict[str, Any]],
    *,
    train_fraction: float,
    validation_fraction: float,
    split_manifest: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Apply the required fixed trade-date authority to every formal row."""
    if not split_manifest:
        raise ValueError("formal split normalization requires a non-empty fixed manifest")
    if train_fraction <= 0 or validation_fraction < 0 or train_fraction + validation_fraction >= 1:
        raise ValueError("invalid global date split fractions")

    old_splits_by_date: dict[str, set[str]] = {}
    dates: set[str] = set()
    for row in rows:
        trade_date = _row_trade_date(row)
        if not trade_date:
            continue
        dates.add(trade_date)
        old_split = str(row.get("split") or "")
        if old_split:
            old_splits_by_date.setdefault(trade_date, set()).add(old_split)

    split_by_date: dict[str, str] = {}
    split_policy = "fixed_trade_date_manifest"
    split_order = {"train": 0, "validation": 1, "holdout": 2}
    previous_rank = -1
    for raw in sorted(split_manifest, key=lambda item: str(item.get("trade_date") or "")):
        trade_date = _row_trade_date(raw)
        split = str(raw.get("split") or "")
        if not trade_date or split not in split_order:
            raise ValueError(f"invalid fixed split manifest row: {raw}")
        if trade_date in split_by_date and split_by_date[trade_date] != split:
            raise ValueError(f"conflicting fixed split manifest date: {trade_date}")
        rank = split_order[split]
        if rank < previous_rank:
            raise ValueError("fixed split manifest is not chronologically contiguous")
        previous_rank = rank
        split_by_date[trade_date] = split
    ordered_dates = sorted(split_by_date)
    date_count = len(ordered_dates)
    expected_counts = {
        "train": max(1, min(date_count, int(round(date_count * train_fraction)))) if date_count else 0,
        "validation": int(round(date_count * validation_fraction)) if date_count else 0,
    }
    expected_counts["holdout"] = date_count - expected_counts["train"] - expected_counts["validation"]
    observed_counts = {
        split: sum(value == split for value in split_by_date.values())
        for split in ("train", "validation", "holdout")
    }
    if observed_counts != expected_counts:
        raise ValueError(
            f"fixed split manifest counts {observed_counts} do not match fractions {expected_counts}"
        )

    date_count = len(ordered_dates)
    manifest: list[dict[str, Any]] = []
    for index, trade_date in enumerate(ordered_dates):
        split = split_by_date[trade_date]
        manifest.append(
            {
                "trade_date": trade_date,
                "split": split,
                "date_ordinal": index + 1,
                "date_count": date_count,
                "train_fraction": train_fraction,
                "validation_fraction": validation_fraction,
                "holdout_fraction": round(1.0 - train_fraction - validation_fraction, 8),
                "optimizer_usage": "allowed" if split == "train" else "report_only",
            }
        )

    reassigned = 0
    post_splits_by_date: dict[str, set[str]] = {}
    unassigned = 0
    for row in rows:
        trade_date = _row_trade_date(row)
        split = split_by_date.get(trade_date)
        if split is None:
            unassigned += 1
            row["split"] = "unassigned"
            continue
        if str(row.get("split") or "") != split:
            reassigned += 1
        row["split"] = split
        post_splits_by_date.setdefault(trade_date, set()).add(split)
    if unassigned:
        raise ValueError(f"fixed split manifest does not cover {unassigned} reward rows")

    boundaries: dict[str, str | None] = {}
    for split in ("train", "validation", "holdout"):
        split_dates = [row["trade_date"] for row in manifest if row["split"] == split]
        boundaries[f"{split}_start"] = split_dates[0] if split_dates else None
        boundaries[f"{split}_end"] = split_dates[-1] if split_dates else None

    audit = {
        "split_policy": split_policy,
        "row_count": len(rows),
        "trade_date_count": len(dates),
        "manifest_trade_date_count": date_count,
        "manifest_unused_date_count": len(set(ordered_dates) - dates),
        "manifest_split_counts": {
            split: sum(row["split"] == split for row in manifest)
            for split in ("train", "validation", "holdout")
        },
        "reassigned_row_count": reassigned,
        "unassigned_row_count": unassigned,
        "preexisting_cross_split_date_count": sum(len(values) > 1 for values in old_splits_by_date.values()),
        "post_normalization_cross_split_date_count": sum(len(values) > 1 for values in post_splits_by_date.values()),
        "boundaries": boundaries,
    }
    return manifest, audit


def _read_train_shard(
    *,
    candidates: list[dict[str, Any]],
    panel_path: Path,
    horizons: tuple[int, ...],
    sample_trade_times: int | None,
    sample_block_count: int = 1,
    sample_block_index: int = 0,
    event_aware_sample_times: bool = True,
    event_sample_trade_times: int | None = None,
    prepare_labels: bool = True,
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
    max_horizon = max(horizons) if prepare_labels else 0
    fields = sorted(
        {
            field
            for candidate in candidates
            for field in (
                list(candidate["fields_list"])
                + list(candidate.get("runtime_fields_list") or ())
            )
        }
    )
    required = {
        "code",
        "trade_time",
        "date",
        "close",
        *fields,
    }
    if prepare_labels:
        required.update({"open", "high", "low", "volume", "vol", "amount", "amount_yuan", "vwap"})

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

    all_trade_times = _panel_trade_times(panel_path)
    event_positions: list[int] = []
    if event_aware_sample_times:
        event_positions = _event_signal_positions(
            panel_path,
            all_trade_times=all_trade_times,
            event_fields=_candidate_event_fields(candidates),
            max_event_trade_times=event_sample_trade_times if event_sample_trade_times is not None else sample_trade_times,
        )

    if sample_block_count <= 1 and not event_positions:
        frame, signal_times, signal_time_count, read_time_count = _read_windowed_panel(
            panel_path,
            columns=columns,
            signal_time_count=sample_trade_times,
            lookback=max_window,
            max_horizon=max_horizon,
        )
        full_signal_times = set(signal_times)
        position_by_time = {pd.Timestamp(value): idx for idx, value in enumerate(pd.to_datetime(all_trade_times))}
        signal_positions = sorted(position_by_time[pd.Timestamp(value)] for value in signal_times if pd.Timestamp(value) in position_by_time)
    else:
        all_signal_positions = sorted(set(_sample_positions_for_count(len(all_trade_times), sample_trade_times).tolist()) | set(event_positions))
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
    labels = (
        _future_returns(frame, horizons).loc[eval_mask].reset_index(drop=True)
        if prepare_labels
        else pd.DataFrame(index=eval_frame.index)
    )
    context_time_fingerprints = {
        str(key): _timestamp_fingerprint(value)
        for key, value in sorted(context_times_by_window.items(), key=lambda item: item[0])
    }
    meta = {
        "panel": str(panel_path),
        "panel_file_fingerprint": _path_fingerprint(panel_path),
        "read_columns_fingerprint": _hash_items(columns),
        "read_rows": int(len(frame)),
        "eval_rows": int(len(eval_frame)),
        "signal_trade_times": int(signal_time_count),
        "full_signal_trade_times": int(len(full_signal_times)),
        "event_signal_trade_times": int(len(event_positions)),
        "read_trade_times": int(read_time_count),
        "sample_block_count": int(sample_block_count),
        "sample_block_index": int(sample_block_index),
        "context_trade_time_counts": json.dumps({str(key): len(value) for key, value in context_times_by_window.items()}, sort_keys=True),
        "context_trade_time_fingerprints": json.dumps(context_time_fingerprints, sort_keys=True),
        "signal_trade_time_fingerprint": _timestamp_fingerprint(signal_times),
        "full_signal_trade_time_fingerprint": _timestamp_fingerprint(full_signal_times),
        "eval_trade_time_fingerprint": _timestamp_fingerprint(eval_frame["trade_time"]),
        "read_column_count": len(columns),
        "candidate_count_in_batch": len(candidates),
        "labels_prepared": bool(prepare_labels),
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

            # Tie-aware cutoffs keep coarse matched controls (for example a
            # sign projection) evaluable without changing the registered
            # quantile. A truly constant signal still degenerates because the
            # pair audit observes zero top/bottom signal spread.
            top_mask = rank_valid >= float(np.nanquantile(rank_valid, q_high))
            bottom_mask = rank_valid <= float(np.nanquantile(rank_valid, q_low))
            if not bool(top_mask.any()) or not bool(bottom_mask.any()):
                continue
            top_codes = set(codes_valid[top_mask].astype(str))
            bottom_codes = set(codes_valid[bottom_mask].astype(str))
            eligible_codes = sorted(set(codes_valid.astype(str)))
            eligible_code_identity = stable_hash(eligible_codes)

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
                selected_long_codes = top_codes if direction > 0 else bottom_codes
                selected_short_codes = bottom_codes if direction > 0 else top_codes
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
                selected_long_codes = long_codes
                selected_short_codes = set()
            net_return = raw_return - trading_cost
            selected_code_identity = stable_hash(
                {"long": sorted(selected_long_codes), "short": sorted(selected_short_codes)}
            )
            portfolio_weight_identity = stable_hash(
                {
                    "long": [(code, 1.0 / max(1, len(selected_long_codes))) for code in sorted(selected_long_codes)],
                    "short": [
                        (code, -1.0 / max(1, len(selected_short_codes)))
                        for code in sorted(selected_short_codes)
                    ],
                    "portfolio_mode": portfolio_mode,
                }
            )
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
                    "eligible_code_count": len(eligible_codes),
                    "eligible_code_identity": eligible_code_identity,
                    "selected_code_identity": selected_code_identity,
                    "portfolio_weight_identity": portfolio_weight_identity,
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


def _pair_common_finite_mask(primary: pd.Series, control: pd.Series) -> np.ndarray:
    """Return the only support on which a formal matched pair may be scored."""

    if len(primary) != len(control):
        raise ValueError("primary/control signal lengths differ")
    return np.isfinite(pd.to_numeric(primary, errors="coerce").to_numpy(dtype=float)) & np.isfinite(
        pd.to_numeric(control, errors="coerce").to_numpy(dtype=float)
    )


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
    persistent_cache_root: Path | None,
    persistent_cache_mode: str,
    persistent_expression_cache: bool,
    persistent_operator_cache: bool,
    persistent_feature_matrix_cache: bool,
    persistent_cache_min_free_gb: float,
    persistent_cache_max_gb: float,
    persistent_cache_ttl_days: float,
    persistent_cache_scope: dict[str, Any],
    semantic_only: bool = False,
    semantic_diagnostics: dict[str, Any] | None = None,
    semantic_sketch_size: int = 512,
    operator_cache_max_bytes: int = 0,
    feature_matrix_cache_max_bytes: int = 0,
    eligible_signal_mask: np.ndarray | None = None,
) -> list[dict[str, Any]]:
    expression = str(candidate["expression"])
    expression_data_role = (
        "development"
        if split_by_time and set(str(value) for value in split_by_time.values()) <= {"train"}
        else None
    )
    context_window = max(0, int(candidate.get("max_window") or 0))
    context_fingerprints = persistent_cache_scope.get("context_fingerprints") or {}
    context_fingerprint = str(context_fingerprints.get(str(context_window)) or "")
    eval_fingerprint = str(persistent_cache_scope.get("eval_fingerprint") or "")
    panel_fingerprint = str(persistent_cache_scope.get("panel_fingerprint") or "")
    columns_fingerprint = str(persistent_cache_scope.get("columns_fingerprint") or "")
    sample_block_index = int(persistent_cache_scope.get("sample_block_index") or 0)
    persistent_active = _persistent_cache_active(persistent_cache_root, persistent_cache_mode)
    expression_disk_cache: _PersistentSeriesCache | None = None
    expression_cache_key = expression
    if persistent_active and persistent_expression_cache and context_fingerprint and eval_fingerprint:
        expression_namespace = _persistent_namespace(
            kind="factor_expression",
            panel_fingerprint=panel_fingerprint,
            columns_fingerprint=columns_fingerprint,
            sample_block_index=sample_block_index,
            context_window=context_window,
            context_fingerprint=context_fingerprint,
            eval_fingerprint=eval_fingerprint,
        )
        expression_disk_cache = _PersistentSeriesCache(
            root=persistent_cache_root,
            namespace=expression_namespace,
            max_entries=max(1, len(expression_cache) + 1),
            stats=cache_stats,
            prefix="persistent_factor_expression_cache",
            mode=persistent_cache_mode,
            expected_length=len(eval_frame),
            min_free_gb=persistent_cache_min_free_gb,
            max_gb=persistent_cache_max_gb,
            ttl_days=persistent_cache_ttl_days,
        )
    if expression in expression_cache:
        _inc(cache_stats, "factor_expression_cache_hits")
        signal = expression_cache[expression]
    elif expression_disk_cache is not None and expression_cache_key in expression_disk_cache:
        _inc(cache_stats, "factor_expression_cache_hits")
        signal = expression_disk_cache[expression_cache_key]
        expression_cache[expression] = signal
    else:
        _inc(cache_stats, "factor_expression_cache_misses")
        context_times = context_times_by_window.get(context_window)
        if context_times:
            cached_context = feature_matrix_cache.get(context_window)
            if cached_context is not None:
                _inc(cache_stats, "feature_matrix_cache_hits")
                context_frame, context_eval_mask = cached_context
            else:
                feature_disk_hit = None
                if persistent_active and persistent_feature_matrix_cache and context_fingerprint and eval_fingerprint:
                    feature_namespace = _persistent_namespace(
                        kind="feature_matrix",
                        panel_fingerprint=panel_fingerprint,
                        columns_fingerprint=columns_fingerprint,
                        sample_block_index=sample_block_index,
                        context_window=context_window,
                        context_fingerprint=context_fingerprint,
                        eval_fingerprint=eval_fingerprint,
                    )
                    feature_disk_hit = _read_cached_feature_matrix(
                        _feature_matrix_cache_path(persistent_cache_root, feature_namespace),
                        expected_columns=list(frame.columns),
                        stats=cache_stats,
                        prefix="persistent_feature_matrix_cache",
                    )
                if feature_disk_hit is not None:
                    context_frame, context_eval_mask = feature_disk_hit
                    _inc(cache_stats, "feature_matrix_cache_hits")
                else:
                    _inc(cache_stats, "feature_matrix_cache_misses")
                    context_mask = frame["trade_time"].isin(context_times)
                    context_frame = frame.loc[context_mask].copy().reset_index(drop=True)
                    context_eval_mask = context_frame["trade_time"].isin(split_by_time.keys())
                    if persistent_active and persistent_feature_matrix_cache and context_fingerprint and eval_fingerprint:
                        _write_cached_feature_matrix(
                            _feature_matrix_cache_path(persistent_cache_root, feature_namespace),
                            frame=context_frame,
                            eval_mask=context_eval_mask,
                            stats=cache_stats,
                            prefix="persistent_feature_matrix_cache",
                            min_free_gb=persistent_cache_min_free_gb,
                            max_gb=persistent_cache_max_gb,
                            ttl_days=persistent_cache_ttl_days,
                        )
                _store_bounded_feature_matrix(
                    feature_matrix_cache,
                    context_window,
                    (context_frame, context_eval_mask),
                    stats=cache_stats,
                    max_windows=feature_matrix_cache_max_windows,
                    max_bytes=feature_matrix_cache_max_bytes,
                )
            operator_cache = operator_cache_by_window.get(context_window)
            if operator_cache is None:
                operator_cache = None
                if operator_cache_max_entries >= 0:
                    if persistent_active and persistent_operator_cache and context_fingerprint:
                        operator_namespace = _persistent_namespace(
                            kind="operator_subtree",
                            panel_fingerprint=panel_fingerprint,
                            columns_fingerprint=columns_fingerprint,
                            sample_block_index=sample_block_index,
                            context_window=context_window,
                            context_fingerprint=context_fingerprint,
                            eval_fingerprint=context_fingerprint,
                        )
                        operator_cache = _PersistentSeriesCache(
                            root=persistent_cache_root,
                            namespace=operator_namespace,
                            max_entries=operator_cache_max_entries,
                            stats=cache_stats,
                            prefix="operator_cache",
                            mode=persistent_cache_mode,
                            expected_length=len(context_frame),
                            min_free_gb=persistent_cache_min_free_gb,
                            max_gb=persistent_cache_max_gb,
                            ttl_days=persistent_cache_ttl_days,
                            max_bytes=operator_cache_max_bytes,
                        )
                    else:
                        operator_cache = _BoundedSeriesCache(
                            max_entries=operator_cache_max_entries,
                            stats=cache_stats,
                            prefix="operator_cache",
                            max_bytes=operator_cache_max_bytes,
                        )
                    operator_cache_by_window[context_window] = operator_cache
            signal_all = pd.to_numeric(
                evaluate_panel_expression(
                    context_frame,
                    expression,
                    cache=operator_cache,
                    diagnostics=semantic_diagnostics,
                    data_role=expression_data_role,
                ),
                errors="coerce",
            )
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
                            max_bytes=operator_cache_max_bytes,
                        )
                        operator_cache_by_window[-1] = full_operator_cache
                signal_all = pd.to_numeric(
                    evaluate_panel_expression(
                        frame,
                        expression,
                        cache=full_operator_cache,
                        diagnostics=semantic_diagnostics,
                        data_role=expression_data_role,
                    ),
                    errors="coerce",
                )
                signal = pd.Series(signal_all.loc[eval_mask].to_numpy(dtype=float))
        else:
            operator_cache = operator_cache_by_window.get(-1)
            if operator_cache is None:
                operator_cache = None
                if operator_cache_max_entries >= 0:
                    if persistent_active and persistent_operator_cache and eval_fingerprint:
                        operator_namespace = _persistent_namespace(
                            kind="operator_subtree",
                            panel_fingerprint=panel_fingerprint,
                            columns_fingerprint=columns_fingerprint,
                            sample_block_index=sample_block_index,
                            context_window=-1,
                            context_fingerprint=eval_fingerprint,
                            eval_fingerprint=eval_fingerprint,
                        )
                        operator_cache = _PersistentSeriesCache(
                            root=persistent_cache_root,
                            namespace=operator_namespace,
                            max_entries=operator_cache_max_entries,
                            stats=cache_stats,
                            prefix="operator_cache",
                            mode=persistent_cache_mode,
                            expected_length=len(frame),
                            min_free_gb=persistent_cache_min_free_gb,
                            max_gb=persistent_cache_max_gb,
                            ttl_days=persistent_cache_ttl_days,
                            max_bytes=operator_cache_max_bytes,
                        )
                    else:
                        operator_cache = _BoundedSeriesCache(
                            max_entries=operator_cache_max_entries,
                            stats=cache_stats,
                            prefix="operator_cache",
                            max_bytes=operator_cache_max_bytes,
                        )
                    operator_cache_by_window[-1] = operator_cache
            signal_all = pd.to_numeric(
                evaluate_panel_expression(
                    frame,
                    expression,
                    cache=operator_cache,
                    diagnostics=semantic_diagnostics,
                    data_role=expression_data_role,
                ),
                errors="coerce",
            )
            signal = pd.Series(signal_all.loc[eval_mask].to_numpy(dtype=float))
        expression_cache[expression] = signal
        if expression_disk_cache is not None:
            expression_disk_cache[expression_cache_key] = signal
        _inc(cache_stats, "factor_expression_cache_stores")
    if eligible_signal_mask is not None:
        mask = np.asarray(eligible_signal_mask, dtype=bool)
        if mask.shape != (len(signal),):
            raise ValueError("eligible_signal_mask length does not match candidate signal")
        signal = signal.where(mask, np.nan)
    if eval_time_index is not None:
        signal_rank = _rank_by_eval_time_index(signal, eval_time_index, cache_stats=cache_stats)
        if semantic_diagnostics is not None:
            semantic_diagnostics.update(
                build_signal_semantic_diagnostics(
                    signal.to_numpy(dtype=float, copy=False),
                    signal_rank.to_numpy(dtype=float, copy=False),
                    sketch_size=semantic_sketch_size,
                )
            )
        if semantic_only:
            return []
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
    if semantic_diagnostics is not None:
        semantic_diagnostics.update(
            build_signal_semantic_diagnostics(
                signal.to_numpy(dtype=float, copy=False),
                signal_rank.to_numpy(dtype=float, copy=False),
                sketch_size=semantic_sketch_size,
            )
        )
    if semantic_only:
        return []
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
            top_block = block.loc[block["rank"] >= float(block["rank"].quantile(q_high))]
            bottom_block = block.loc[block["rank"] <= float(block["rank"].quantile(q_low))]
            if top_block.empty or bottom_block.empty:
                continue
            top_codes = set(top_block["code"].astype(str))
            bottom_codes = set(bottom_block["code"].astype(str))
            eligible_codes = sorted(set(block["code"].astype(str)))
            eligible_code_identity = stable_hash(eligible_codes)

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
                selected_long_codes = top_codes if direction > 0 else bottom_codes
                selected_short_codes = bottom_codes if direction > 0 else top_codes
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
                selected_long_codes = long_codes
                selected_short_codes = set()
            net_return = raw_return - trading_cost
            ts = pd.Timestamp(trade_time)
            selected_code_identity = stable_hash(
                {"long": sorted(selected_long_codes), "short": sorted(selected_short_codes)}
            )
            portfolio_weight_identity = stable_hash(
                {
                    "long": [(code, 1.0 / max(1, len(selected_long_codes))) for code in sorted(selected_long_codes)],
                    "short": [
                        (code, -1.0 / max(1, len(selected_short_codes)))
                        for code in sorted(selected_short_codes)
                    ],
                    "portfolio_mode": portfolio_mode,
                }
            )
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
                    "eligible_code_count": len(eligible_codes),
                    "eligible_code_identity": eligible_code_identity,
                    "selected_code_identity": selected_code_identity,
                    "portfolio_weight_identity": portfolio_weight_identity,
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
    clean = np.asarray([float(value) for value in day_values if math.isfinite(float(value))], dtype=np.float64)
    engine = "numpy_vectorized_v1"
    iterations = max(0, int(iterations))
    if len(clean) == 0 or iterations == 0:
        return {"iterations": 0, "day_count": int(len(clean)), "engine": engine}
    rng = np.random.default_rng(int(seed))
    indices = rng.integers(0, len(clean), size=(iterations, len(clean)), dtype=np.int64)
    samples = clean[indices]
    means = np.mean(samples, axis=1)
    downside = np.minimum(samples, 0.0)
    downside_scale = np.sqrt(np.mean(downside * downside, axis=1))
    valid = np.isfinite(means) & np.isfinite(downside_scale) & (downside_scale > 1e-18)
    draws = means[valid] / downside_scale[valid]
    positives = int(np.sum(draws > 0.0))
    return {
        "iterations": int(len(draws)),
        "day_count": int(len(clean)),
        "engine": engine,
        "p05": _round(float(np.quantile(draws, 0.05)) if len(draws) else None),
        "p25": _round(float(np.quantile(draws, 0.25)) if len(draws) else None),
        "median": _round(float(np.quantile(draws, 0.50)) if len(draws) else None),
        "p95": _round(float(np.quantile(draws, 0.95)) if len(draws) else None),
        "prob_gt_0": _round(positives / len(draws) if len(draws) else None),
    }


def _summarize_curve(
    curve_rows: list[dict[str, Any]],
    *,
    split: str,
    horizon: int | str,
    seed: int,
    bootstrap_iterations: int = 600,
) -> dict[str, Any]:
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
    boot = _bootstrap_days(day_values, iterations=bootstrap_iterations, seed=seed)
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
        "day_mcmc_engine": boot.get("engine"),
        "day_mcmc_iterations": boot.get("iterations"),
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


def _reward_atoms_for_curve(
    *,
    candidate: dict[str, Any],
    curve_rows: list[dict[str, Any]],
    split: str,
    horizon: int | str,
) -> list[dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = {}
    for row in curve_rows:
        trade_date = str(row.get("trade_date") or "")
        if not trade_date:
            continue
        item = by_date.setdefault(
            trade_date,
            {
                "candidate_id": candidate.get("candidate_id"),
                "expression_hash": candidate.get("expression_hash"),
                "split": split,
                "horizon_min": str(horizon),
                "trade_date": trade_date,
                "curve_count": 0,
                "net_return_sum": 0.0,
                "raw_return_sum": 0.0,
                "net_positive_count": 0,
                "downside_square_sum": 0.0,
                "daily_net_return": 0.0,
                "market_mean_return_sum": 0.0,
                "market_mean_return_count": 0,
                "turnover_sum": 0.0,
                "turnover_count": 0,
                "rank_ic_sum": 0.0,
                "rank_ic_count": 0,
                "rank_ic_positive_count": 0,
            },
        )
        net = _f(row.get("net_return"))
        if math.isfinite(net):
            item["curve_count"] += 1
            item["net_return_sum"] += net
            item["daily_net_return"] += net
            item["net_positive_count"] += int(net > 0.0)
            item["downside_square_sum"] += min(0.0, net) ** 2
        raw = _f(row.get("raw_return"))
        if math.isfinite(raw):
            item["raw_return_sum"] += raw
        market = _f(row.get("market_mean_return"))
        if math.isfinite(market):
            item["market_mean_return_sum"] += market
            item["market_mean_return_count"] += 1
        turnover = _f(row.get("one_way_turnover"))
        if math.isfinite(turnover):
            item["turnover_sum"] += turnover
            item["turnover_count"] += 1
        rank_ic = _f(row.get("rank_ic"))
        if math.isfinite(rank_ic):
            item["rank_ic_sum"] += rank_ic
            item["rank_ic_count"] += 1
            item["rank_ic_positive_count"] += int(rank_ic > 0.0)
    return list(by_date.values())


def _reward_atoms_for_candidate(
    candidate: dict[str, Any],
    rows: list[dict[str, Any]],
    horizons: tuple[int, ...],
) -> list[dict[str, Any]]:
    atoms: list[dict[str, Any]] = []
    for split in ("train", "validation", "holdout"):
        atoms.extend(
            _reward_atoms_for_curve(
                candidate=candidate,
                curve_rows=_curve_rows(rows, split=split),
                split=split,
                horizon="all",
            )
        )
        for horizon in horizons:
            atoms.extend(
                _reward_atoms_for_curve(
                    candidate=candidate,
                    curve_rows=_curve_rows(rows, split=split, horizon=horizon),
                    split=split,
                    horizon=horizon,
                )
            )
    return atoms


def _summarize_reward_atoms(atom_rows: list[dict[str, Any]], *, split: str, horizon: int | str, seed: int) -> dict[str, Any]:
    by_date: dict[str, dict[str, float]] = {}
    curve_count = 0
    net_return_sum = 0.0
    raw_return_sum = 0.0
    net_positive_count = 0
    downside_square_sum = 0.0
    turnover_sum = 0.0
    turnover_count = 0
    rank_ic_sum = 0.0
    rank_ic_count = 0
    rank_ic_positive_count = 0
    for row in atom_rows:
        trade_date = str(row.get("trade_date") or "")
        if trade_date:
            day = by_date.setdefault(
                trade_date,
                {
                    "daily_net_return": 0.0,
                    "market_mean_return_sum": 0.0,
                    "market_mean_return_count": 0.0,
                },
            )
            day["daily_net_return"] += _f(row.get("daily_net_return"), 0.0)
            market_count = _f(row.get("market_mean_return_count"), 0.0)
            day["market_mean_return_sum"] += _f(row.get("market_mean_return_sum"), 0.0)
            day["market_mean_return_count"] += market_count
        count = int(_f(row.get("curve_count"), 0.0))
        curve_count += count
        net_return_sum += _f(row.get("net_return_sum"), 0.0)
        raw_return_sum += _f(row.get("raw_return_sum"), 0.0)
        net_positive_count += int(_f(row.get("net_positive_count"), 0.0))
        downside_square_sum += _f(row.get("downside_square_sum"), 0.0)
        turnover_sum += _f(row.get("turnover_sum"), 0.0)
        turnover_count += int(_f(row.get("turnover_count"), 0.0))
        rank_ic_sum += _f(row.get("rank_ic_sum"), 0.0)
        rank_ic_count += int(_f(row.get("rank_ic_count"), 0.0))
        rank_ic_positive_count += int(_f(row.get("rank_ic_positive_count"), 0.0))

    day_values = [item["daily_net_return"] for item in by_date.values() if math.isfinite(item["daily_net_return"])]
    boot = _bootstrap_days(day_values, iterations=600, seed=seed)
    net_mean = net_return_sum / curve_count if curve_count else None
    raw_mean = raw_return_sum / curve_count if curve_count else None
    downside_var = downside_square_sum / curve_count if curve_count else None
    minute_sortino = None
    if net_mean is not None and downside_var is not None and downside_var > 1e-18:
        minute_sortino = net_mean / math.sqrt(downside_var)
    rank_ic_mean = rank_ic_sum / rank_ic_count if rank_ic_count else None
    daily_rows = []
    for trade_date, item in by_date.items():
        market_count = item["market_mean_return_count"]
        daily_rows.append(
            {
                "trade_date": trade_date,
                "net_return": item["daily_net_return"],
                "market_mean_return": item["market_mean_return_sum"] / market_count if market_count else float("nan"),
            }
        )
    return {
        "split": split,
        "horizon_min": horizon,
        "curve_count": curve_count,
        "day_count": len(day_values),
        "net_mean_return": _round(net_mean, 10),
        "raw_mean_return": _round(raw_mean, 10),
        "net_hit_rate": _round(net_positive_count / curve_count if curve_count else None),
        "minute_sortino": _round(minute_sortino),
        "day_sortino": _round(_sortino(day_values)),
        "day_mcmc_sortino_p25": boot.get("p25"),
        "day_mcmc_sortino_median": boot.get("median"),
        "day_mcmc_prob_sortino_gt_0": boot.get("prob_gt_0"),
        "day_mcmc_engine": boot.get("engine"),
        "day_mcmc_iterations": boot.get("iterations"),
        "max_drawdown": _round(_max_drawdown(day_values)),
        "mean_one_way_turnover": _round(turnover_sum / turnover_count if turnover_count else None),
        "rank_ic_mean": _round(rank_ic_mean),
        "rank_ic_hit_rate": _round(rank_ic_positive_count / rank_ic_count if rank_ic_count else None),
        "rank_ic_loss": _round(-rank_ic_mean if rank_ic_mean is not None else None),
        "rank_ic_obs": rank_ic_count,
        "_daily_rows": daily_rows,
    }


def _regime_stability_summary_from_daily_rows(daily_rows: list[dict[str, Any]], *, min_days_per_regime: int = 3) -> dict[str, Any]:
    if not daily_rows:
        return {
            "train_regime_stability_score": None,
            "train_regime_worst_day_sortino": None,
            "train_regime_median_day_sortino": None,
            "train_regime_positive_share": None,
            "train_regime_count": 0,
            "train_regime_method": "none_no_train_curve",
        }
    frame = pd.DataFrame(daily_rows)
    frame["net_return"] = pd.to_numeric(frame["net_return"], errors="coerce")
    frame["market_mean_return"] = pd.to_numeric(frame.get("market_mean_return", np.nan), errors="coerce")
    daily = frame.dropna(subset=["net_return"]).sort_values("trade_date").reset_index(drop=True)
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


def _candidate_summary_from_reward_atoms(
    candidate: dict[str, Any],
    atom_rows: list[dict[str, Any]],
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
    for split in ("train", "validation", "holdout"):
        all_atoms = [row for row in atom_rows if str(row.get("split")) == split and str(row.get("horizon_min")) == "all"]
        summary = _summarize_reward_atoms(all_atoms, split=split, horizon="equal_weight_horizon_sleeves", seed=seed + len(split))
        split_rows.append({key: value for key, value in summary.items() if key != "_daily_rows"})
        by_key[(split, "all")] = summary
        for horizon in horizons:
            horizon_atoms = [
                row
                for row in atom_rows
                if str(row.get("split")) == split and str(row.get("horizon_min")) == str(horizon)
            ]
            horizon_summary = _summarize_reward_atoms(horizon_atoms, split=split, horizon=horizon, seed=seed + horizon)
            split_rows.append({key: value for key, value in horizon_summary.items() if key != "_daily_rows"})
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
    train_regime = _regime_stability_summary_from_daily_rows(train_all.get("_daily_rows") or [])
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
    portfolio_mode = str(candidate.get("portfolio_mode") or "long_only_top")
    reward_row = {
        "candidate_id": candidate.get("candidate_id"),
        "candidate_submission_receipt_id": candidate.get("candidate_submission_receipt_id"),
        "candidate_submission_receipt_hash": candidate.get("candidate_submission_receipt_hash"),
        "candidate_submission_authorization": candidate.get("candidate_submission_authorization"),
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
        "reward_atom_mode": "daily_aggregate",
        **development_predictive_evidence(),
    }
    reward_row.update(normalize_candidate_schema(reward_row))
    return split_rows, reward_row


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
    bootstrap_iterations: int = 600,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    split_rows: list[dict[str, Any]] = []
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    train_all_curve: list[dict[str, Any]] = []
    for split in ("train", "validation", "holdout"):
        all_curve = _curve_rows(rows, split=split)
        if split == "train":
            train_all_curve = all_curve
        summary = _summarize_curve(
            all_curve,
            split=split,
            horizon="equal_weight_horizon_sleeves",
            seed=seed + len(split),
            bootstrap_iterations=bootstrap_iterations,
        )
        split_rows.append(summary)
        by_key[(split, "all")] = summary
        for horizon in horizons:
            curve = _curve_rows(rows, split=split, horizon=horizon)
            horizon_summary = _summarize_curve(
                curve,
                split=split,
                horizon=horizon,
                seed=seed + horizon,
                bootstrap_iterations=bootstrap_iterations,
            )
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
        "candidate_submission_receipt_id": candidate.get("candidate_submission_receipt_id"),
        "candidate_submission_receipt_hash": candidate.get("candidate_submission_receipt_hash"),
        "candidate_submission_authorization": candidate.get("candidate_submission_authorization"),
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
        **development_predictive_evidence(),
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
    parser.add_argument("--event-aware-sample-times", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--event-sample-trade-times-per-shard", type=int, default=0)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--train-fraction", type=float, default=0.75)
    parser.add_argument("--validation-fraction", type=float, default=0.15)
    parser.add_argument(
        "--split-manifest",
        type=Path,
        required=True,
        help="Required fixed 485-session trade_date/split authority shared by every formal worker.",
    )
    parser.add_argument("--candidate-receipt-table", type=Path, required=True)
    parser.add_argument("--candidate-pair-receipt-table", type=Path, required=True)
    parser.add_argument(
        "--a-share-replay-receipt-table",
        type=Path,
        default=None,
        help=(
            "Optional immutable train-only executable replay receipts. "
            "Without this table Phase3CM remains predictive evidence only."
        ),
    )
    parser.add_argument("--unified-registry", type=Path, required=True)
    parser.add_argument("--data-release-hash", required=True)
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
    parser.add_argument("--write-reward-atoms", action="store_true")
    parser.add_argument(
        "--semantic-only",
        action="store_true",
        help="Evaluate signal values/ranks only; skip labels, portfolio PnL, and reward construction.",
    )
    parser.add_argument(
        "--write-semantic-sketches",
        action="store_true",
        help="Write bounded rank sketches and numerical signal diagnostics into candidate progress rows.",
    )
    parser.add_argument("--semantic-sketch-size", type=int, default=512)
    parser.add_argument("--fast-mode", action="store_true")
    parser.add_argument("--numexpr-threads", type=int, default=4)
    parser.add_argument("--checkpoint-every-candidates", type=int, default=8)
    parser.add_argument("--checkpoint-bootstrap-iterations", type=int, default=128)
    parser.add_argument("--disable-incremental-checkpoints", action="store_true")
    parser.add_argument("--drop-hard-blocked-input", action="store_true")
    parser.add_argument("--rank-ic-loss-weight", type=float, default=6.0)
    parser.add_argument("--rank-ic-component-cap", type=float, default=0.35)
    parser.add_argument("--regime-stability-weight", type=float, default=0.08)
    parser.add_argument("--regime-component-cap", type=float, default=0.10)
    parser.add_argument("--operator-cache-max-entries", type=int, default=512)
    parser.add_argument(
        "--operator-cache-max-mb",
        type=float,
        default=2048.0,
        help="Per-process in-memory byte cap for full-panel operator Series cache.",
    )
    parser.add_argument("--feature-matrix-cache-max-windows", type=int, default=6)
    parser.add_argument(
        "--feature-matrix-cache-max-mb",
        type=float,
        default=4096.0,
        help="Per-process in-memory byte cap for context-window DataFrame cache.",
    )
    parser.add_argument("--persistent-cache-root", type=Path, default=None)
    parser.add_argument("--persistent-cache-mode", choices=("off", "read", "write", "readwrite"), default="readwrite")
    parser.add_argument(
        "--persistent-cache-min-free-gb",
        type=float,
        default=2.0,
        help="Skip persistent cache writes when the target volume has less than this much free space.",
    )
    parser.add_argument(
        "--persistent-cache-max-gb",
        type=float,
        default=DEFAULT_PERSISTENT_CACHE_MAX_GB,
        help="Prune oldest persistent cache files when the cache version directory exceeds this size. Use <=0 to disable.",
    )
    parser.add_argument(
        "--persistent-cache-ttl-days",
        type=float,
        default=DEFAULT_PERSISTENT_CACHE_TTL_DAYS,
        help="Prune persistent cache files older than this many days. Use <=0 to disable.",
    )
    parser.add_argument("--disable-persistent-expression-cache", action="store_true")
    parser.add_argument("--disable-persistent-operator-cache", action="store_true")
    parser.add_argument("--disable-persistent-feature-matrix-cache", action="store_true")
    parser.add_argument("--disable-factor-expression-cache", action="store_true")
    parser.add_argument("--disable-operator-cache", action="store_true")
    parser.add_argument("--disable-feature-matrix-cache", action="store_true")
    parser.add_argument("--disable-fast-portfolio-loop", action="store_true")
    parser.add_argument(
        "--enforce-pair-shared-support",
        action="store_true",
        help=(
            "Evaluate every primary/control pair only on their finite-signal intersection. "
            "Required by the compositional matched-control contract."
        ),
    )
    parser.add_argument("--disable-schema-gate", action="store_true")
    parser.add_argument("--disable-legacy-alias-rewrite", action="store_true")
    parser.add_argument("--m1-first-ret-replacement", default="m1_first5_last_return_vs_open")
    args = parser.parse_args(argv)
    write_semantic_sketches = bool(args.write_semantic_sketches or args.semantic_only)

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
    split_authority = FixedSplitAuthority.read(_resolve(args.split_manifest), require_official=True)
    fixed_split_manifest_rows = [dict(row) for row in split_authority.rows]
    unified_registry = UnifiedCapabilityRegistry.read(_resolve(args.unified_registry))
    receipt_context = ReceiptContext.build(
        registry=unified_registry,
        split_authority=split_authority,
        data_release_hash=str(args.data_release_hash),
        evaluator_paths=[Path(__file__)],
    )
    submission_authority = CandidateSubmissionAuthority(unified_registry, receipt_context)
    horizons = tuple(int(item.strip()) for item in str(args.horizons).split(",") if item.strip())
    candidates = _load_candidates(
        _resolve(args.candidate_audit),
        args.candidate_limit,
        drop_hard_blocked_input=bool(args.drop_hard_blocked_input),
        enable_legacy_alias_rewrite=not bool(args.disable_legacy_alias_rewrite),
        m1_first_ret_replacement=str(args.m1_first_ret_replacement),
    )
    authorized_candidates = [dict(row) for row in candidates]
    candidate_receipts = read_receipt_table(_resolve(args.candidate_receipt_table))
    validated_receipts = submission_authority.validate_table(candidates, candidate_receipts)
    pair_ids = {str(row.get("pair_id") or "") for row in candidates}
    pair_receipts = [
        row
        for row in read_pair_receipt_table(_resolve(args.candidate_pair_receipt_table))
        if str(row.get("pair_id") or "") in pair_ids
    ]
    validated_pair_receipts = CandidatePairAuthority().validate_table(
        candidates,
        validated_receipts,
        pair_receipts,
    )
    candidate_ids = {
        str(row.get("candidate_id") or "") for row in candidates
    }
    replay_receipts = (
        [
            row
            for row in read_a_share_tradability_receipts(
                _resolve(args.a_share_replay_receipt_table)
            )
            if str(row.get("candidate_id") or "") in candidate_ids
        ]
        if args.a_share_replay_receipt_table is not None
        else []
    )
    receipt_by_candidate = {str(row["candidate_id"]): row for row in validated_receipts}
    pair_receipt_by_id = {str(row["pair_id"]): row for row in validated_pair_receipts}
    for candidate in candidates:
        receipt = receipt_by_candidate[str(candidate.get("candidate_id") or "")]
        pair_receipt = pair_receipt_by_id[str(candidate.get("pair_id") or "")]
        candidate["candidate_submission_receipt_id"] = receipt["receipt_id"]
        candidate["candidate_submission_receipt_hash"] = receipt["receipt_hash"]
        candidate["candidate_submission_authorization"] = receipt["authorization_status"]
        candidate["candidate_pair_receipt_id"] = pair_receipt["pair_receipt_id"]
        candidate["candidate_pair_receipt_hash"] = pair_receipt["pair_receipt_hash"]
    all_panels = _discover_panels(_resolve(args.shard_root), args.max_shards)
    panel_items = list(enumerate(all_panels))
    panels = [panel for _, panel in panel_items]
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
    evaluator_invocation_counts: dict[str, int] = {
        str(candidate.get("candidate_id") or ""): 0 for candidate in authorized_candidates
    }
    processed_candidate_shards = 0
    pnl_rows: list[dict[str, Any]] = []
    shard_meta: list[dict[str, Any]] = []
    global_cache_stats: dict[str, int] = {}
    for shard_index, panel in panel_items:
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
                event_aware_sample_times=bool(args.event_aware_sample_times),
                event_sample_trade_times=(
                    int(args.event_sample_trade_times_per_shard)
                    if int(args.event_sample_trade_times_per_shard or 0) > 0
                    else args.sample_trade_times_per_shard
                ),
                prepare_labels=not bool(args.semantic_only),
            )
            meta["shard_index"] = shard_index
            # The formal path has no shard-local fallback.  Unknown, report-only
            # and sealed dates are resolved exclusively by the frozen authority.
            split_by_time = split_authority.map_times(full_signal_times)
            eval_time_index = None if args.disable_fast_portfolio_loop else _build_eval_time_index(eval_frame)
            meta["fast_portfolio_loop"] = not bool(args.disable_fast_portfolio_loop)
            meta["eval_time_group_count"] = len(eval_time_index["groups"]) if eval_time_index is not None else None
            persistent_scope = {
                "panel_fingerprint": meta.get("panel_file_fingerprint", ""),
                "columns_fingerprint": meta.get("read_columns_fingerprint", ""),
                "sample_block_index": int(meta.get("sample_block_index") or 0),
                "context_fingerprints": json.loads(str(meta.get("context_trade_time_fingerprints") or "{}")),
                "eval_fingerprint": meta.get("eval_trade_time_fingerprint", ""),
            }
            expression_cache: dict[str, pd.Series] = {} if not args.disable_factor_expression_cache else {}
            feature_matrix_cache: dict[int, tuple[pd.DataFrame, pd.Series]] = {}
            operator_cache_by_window: dict[int, dict[str, pd.Series]] = {}
            shard_cache_stats: dict[str, int] = {}
            shard_rows = 0
            evaluation_kwargs = {
                "frame": frame,
                "eval_mask": eval_mask,
                "eval_frame": eval_frame,
                "labels": labels,
                "eval_time_index": eval_time_index,
                "split_by_time": split_by_time,
                "context_times_by_window": context_times_by_window,
                "shard_index": shard_index,
                "horizons": horizons,
                "min_obs": args.min_obs_per_time,
                "cost_bps": args.cost_bps,
                "top_quantile": args.top_quantile,
                "portfolio_mode": args.portfolio_mode,
                "expression_cache": expression_cache,
                "feature_matrix_cache": feature_matrix_cache if not args.disable_feature_matrix_cache else {},
                "operator_cache_by_window": operator_cache_by_window if not args.disable_operator_cache else {},
                "cache_stats": shard_cache_stats,
                "operator_cache_max_entries": -1 if args.disable_operator_cache else args.operator_cache_max_entries,
                "feature_matrix_cache_max_windows": 0 if args.disable_feature_matrix_cache else args.feature_matrix_cache_max_windows,
                "persistent_cache_root": _resolve(args.persistent_cache_root) if args.persistent_cache_root is not None else None,
                "persistent_cache_mode": "off" if args.persistent_cache_mode == "off" else args.persistent_cache_mode,
                "persistent_expression_cache": not bool(args.disable_persistent_expression_cache),
                "persistent_operator_cache": not bool(args.disable_persistent_operator_cache),
                "persistent_feature_matrix_cache": not bool(args.disable_persistent_feature_matrix_cache),
                "persistent_cache_min_free_gb": float(args.persistent_cache_min_free_gb),
                "persistent_cache_max_gb": float(args.persistent_cache_max_gb),
                "persistent_cache_ttl_days": float(args.persistent_cache_ttl_days),
                "persistent_cache_scope": persistent_scope,
                "semantic_sketch_size": max(16, int(args.semantic_sketch_size)),
                "operator_cache_max_bytes": max(0, int(float(args.operator_cache_max_mb) * 1024 * 1024)),
                "feature_matrix_cache_max_bytes": max(0, int(float(args.feature_matrix_cache_max_mb) * 1024 * 1024)),
            }
            evaluation_sequence: list[tuple[dict[str, Any], np.ndarray | None]] = []
            if args.enforce_pair_shared_support:
                if args.disable_factor_expression_cache:
                    raise ValueError("pair shared-support alignment requires the factor expression cache")
                for primary, control in group_candidate_pairs(candidates):
                    for member in (primary, control):
                        if str(member.get("pair_support_alignment_policy") or "") != PAIR_SUPPORT_ALIGNMENT_POLICY:
                            raise ValueError("candidate does not declare the frozen pair support-alignment policy")
                        if str(member.get("pair_maturity_alignment_policy") or "") != PAIR_MATURITY_ALIGNMENT_POLICY:
                            raise ValueError("candidate does not declare the frozen pair maturity-alignment policy")
                        _candidate_portfolio_rows_from_frame(
                            candidate=member,
                            semantic_only=True,
                            semantic_diagnostics=None,
                            eligible_signal_mask=None,
                            **evaluation_kwargs,
                        )
                    common_mask = _pair_common_finite_mask(
                        expression_cache[str(primary["expression"])],
                        expression_cache[str(control["expression"])],
                    )
                    evaluation_sequence.extend(((primary, common_mask), (control, common_mask)))
            else:
                evaluation_sequence = [(candidate, None) for candidate in candidates]
            for candidate_index, (candidate, shared_support_mask) in enumerate(evaluation_sequence, 1):
                semantic_diagnostics: dict[str, Any] | None = {} if write_semantic_sketches else None
                rows = _candidate_portfolio_rows_from_frame(
                    candidate=candidate,
                    semantic_only=bool(args.semantic_only),
                    semantic_diagnostics=semantic_diagnostics,
                    eligible_signal_mask=shared_support_mask,
                    **evaluation_kwargs,
                )
                candidate_id = str(candidate.get("candidate_id") or "")
                evaluator_invocation_counts[candidate_id] = evaluator_invocation_counts.get(candidate_id, 0) + 1
                expression_hash = str(candidate["expression_hash"])
                rows_by_hash[expression_hash].extend(rows)
                shard_rows += len(rows)
                processed_candidate_shards += 1
                progress_row = {
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
                    "semantic_only": bool(args.semantic_only),
                    "actual_evaluator_invocation_count": evaluator_invocation_counts[candidate_id],
                }
                if semantic_diagnostics:
                    progress_row.update(semantic_diagnostics)
                progress_rows.append(progress_row)
                if not args.disable_incremental_checkpoints and not args.semantic_only:
                    _, reward_row = _candidate_summary(
                        candidate,
                        rows_by_hash[expression_hash],
                        horizons,
                        seed=20260623 + candidate_index,
                        rank_ic_loss_weight=args.rank_ic_loss_weight,
                        rank_ic_component_cap=args.rank_ic_component_cap,
                        regime_stability_weight=args.regime_stability_weight,
                        regime_component_cap=args.regime_component_cap,
                        bootstrap_iterations=max(0, int(args.checkpoint_bootstrap_iterations)),
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
    reward_atom_rows: list[dict[str, Any]] = []
    reward_rows: list[dict[str, Any]] = [
        _schema_hold_reward_row(candidate, portfolio_mode=args.portfolio_mode)
        for candidate in schema_held_candidates
    ]
    split_manifest_rows: list[dict[str, Any]] = []
    split_reassignment_audit: dict[str, Any] = {
        "split_policy": "semantic_only_no_reward_split",
        "row_count": 0,
        "trade_date_count": 0,
        "reassigned_row_count": 0,
        "unassigned_row_count": 0,
        "preexisting_cross_split_date_count": 0,
        "post_normalization_cross_split_date_count": 0,
        "boundaries": {},
    }
    if not args.semantic_only:
        all_portfolio_rows = [row for candidate_rows in rows_by_hash.values() for row in candidate_rows]
        split_manifest_rows, split_reassignment_audit = normalize_against_fixed_manifest(
            all_portfolio_rows,
            train_fraction=args.train_fraction,
            validation_fraction=args.validation_fraction,
            split_manifest=fixed_split_manifest_rows,
        )
        for idx, candidate in enumerate(candidates, 1):
            rows = rows_by_hash[str(candidate["expression_hash"])]
            per_split, reward_row = _candidate_summary(
                candidate,
                rows,
                horizons,
                seed=20260623 + int(stable_hash(str(candidate.get("candidate_id") or ""))[:8], 16),
                rank_ic_loss_weight=args.rank_ic_loss_weight,
                rank_ic_component_cap=args.rank_ic_component_cap,
                regime_stability_weight=args.regime_stability_weight,
                regime_component_cap=args.regime_component_cap,
                bootstrap_iterations=600,
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
            if args.write_reward_atoms:
                reward_atom_rows.extend(_reward_atoms_for_candidate(candidate, rows, horizons))
            reward_rows.append(reward_row)

    portfolio_rows_by_expression_hash = {
        str(expression_hash): list(rows)
        for expression_hash, rows in rows_by_hash.items()
    }
    pair_evaluation_rows = (
        []
        if args.semantic_only
        else build_pair_evaluation_rows(
            candidates=authorized_candidates,
            candidate_receipts=validated_receipts,
            pair_receipts=validated_pair_receipts,
            reward_rows=reward_rows,
            portfolio_rows_by_expression_hash=portfolio_rows_by_expression_hash,
            replay_receipt_rows=replay_receipts,
            reward_atom_rows=reward_atom_rows,
            evaluator_invocation_counts=evaluator_invocation_counts,
        )
    )

    reward_rows.sort(key=lambda row: _f(row.get("train_reward"), -999.0), reverse=True)
    followup_count = sum(1 for row in reward_rows if row.get("train_reward_decision") == "TRAIN_REWARD_FOLLOWUP_READY")
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cm_train_portfolio_sortino_reward_audit",
        "decision": (
            "PHASE3CM_SIGNAL_SEMANTIC_AUDIT_READY_DIAGNOSTIC_ONLY"
            if args.semantic_only
            else "PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY"
        ),
        "candidate_count": input_candidate_count,
        "candidate_pair_count": input_candidate_count // 2,
        "pair_evaluated_count": sum(
            row.get("pair_evaluation_status") == "PAIR_EVALUATED" for row in pair_evaluation_rows
        ),
        "pair_blocked_count": sum(
            row.get("pair_evaluation_status") == "PAIR_EVALUATION_BLOCKED" for row in pair_evaluation_rows
        ),
        "runnable_candidate_count": len(candidates),
        "schema_gate_enabled": not bool(args.disable_schema_gate),
        "schema_gate_field_count": schema_field_count,
        "schema_gate_held_count": len(schema_held_candidates),
        "followup_count": followup_count,
        "input_candidate_audit": str(_resolve(args.candidate_audit)),
        "shard_root": str(_resolve(args.shard_root)),
        "max_shards": args.max_shards,
        "selected_shard_indices": [int(idx) for idx, _ in panel_items],
        "selected_shard_count": len(panel_items),
        "sample_trade_times_per_shard": args.sample_trade_times_per_shard,
        "event_aware_sample_times": bool(args.event_aware_sample_times),
        "event_sample_trade_times_per_shard": int(args.event_sample_trade_times_per_shard or 0),
        "sample_block_count": int(args.sample_block_count),
        "horizons": list(horizons),
        "train_fraction": args.train_fraction,
        "validation_fraction": args.validation_fraction,
        "holdout_fraction": round(1.0 - args.train_fraction - args.validation_fraction, 8),
        "split_policy": split_reassignment_audit.get("split_policy"),
        "split_manifest_input": str(_resolve(args.split_manifest)) if args.split_manifest is not None else "",
        "split_audit": split_reassignment_audit,
        "cost_bps": args.cost_bps,
        "top_quantile": args.top_quantile,
        "portfolio_mode": args.portfolio_mode,
        "short_allowed": bool(args.portfolio_mode == "long_short_spread"),
        "pair_shared_support_enforced": bool(args.enforce_pair_shared_support),
        "a_share_replay_receipt_table": (
            str(_resolve(args.a_share_replay_receipt_table))
            if args.a_share_replay_receipt_table is not None
            else ""
        ),
        "a_share_replay_receipt_count": len(replay_receipts),
        "pair_support_alignment_policy": (
            PAIR_SUPPORT_ALIGNMENT_POLICY if args.enforce_pair_shared_support else "LEGACY_INDEPENDENT_SUPPORT"
        ),
        "pair_maturity_alignment_policy": (
            PAIR_MATURITY_ALIGNMENT_POLICY if args.enforce_pair_shared_support else "LEGACY_MEMBER_LOCAL_MATURITY"
        ),
        "rank_ic_loss_weight": args.rank_ic_loss_weight,
        "rank_ic_component_cap": args.rank_ic_component_cap,
        "regime_stability_weight": args.regime_stability_weight,
        "regime_component_cap": args.regime_component_cap,
        "optimizer_reward_metric": None if args.semantic_only else OPTIMIZER_REWARD_METRIC,
        "bootstrap_engine": None if args.semantic_only else "numpy_vectorized_v1",
        "bootstrap_iterations_per_curve": 0 if args.semantic_only else 600,
        "portfolio_pnl_rows_written": len(pnl_rows) if args.write_pnl_rows else 0,
        "reward_atom_rows_written": len(reward_atom_rows) if args.write_reward_atoms else 0,
        "metric_boundary": (
            "signal-value and rank-equivalence diagnostics only; no labels, PnL, reward, validation, or holdout optimization"
            if args.semantic_only
            else "train portfolio Sortino + rankIC loss reward audit; not production proof; validation/holdout must not feed search; long_short_spread is not CN tradable"
        ),
        "semantic_only": bool(args.semantic_only),
        "semantic_sketches_written": bool(write_semantic_sketches),
        "semantic_sketch_size": max(16, int(args.semantic_sketch_size)),
        "fast_mode": bool(args.fast_mode),
        "numexpr_threads": int(args.numexpr_threads),
        "incremental_checkpoints_enabled": not bool(args.disable_incremental_checkpoints),
        "checkpoint_every_candidates": int(args.checkpoint_every_candidates),
        "checkpoint_bootstrap_iterations": max(0, int(args.checkpoint_bootstrap_iterations)),
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
            "persistent_cache": _persistent_cache_active(_resolve(args.persistent_cache_root) if args.persistent_cache_root is not None else None, args.persistent_cache_mode),
            "persistent_cache_root": str(_resolve(args.persistent_cache_root)) if args.persistent_cache_root is not None else "",
            "persistent_cache_mode": str(args.persistent_cache_mode),
            "persistent_cache_min_free_gb": float(args.persistent_cache_min_free_gb),
            "persistent_cache_max_gb": float(args.persistent_cache_max_gb),
            "persistent_cache_ttl_days": float(args.persistent_cache_ttl_days),
            "persistent_factor_expression_cache": not bool(args.disable_persistent_expression_cache),
            "persistent_operator_subtree_cache": not bool(args.disable_persistent_operator_cache),
            "persistent_feature_matrix_cache": not bool(args.disable_persistent_feature_matrix_cache),
            "persistent_cache_version": PERSISTENT_CACHE_VERSION,
            "expression_cache_scope": "per_shard",
            "feature_matrix_cache_scope": "per_shard_context_window",
            "operator_cache_scope": "per_shard_context_window",
            "persistent_cache_scope": "panel_file_fingerprint+columns_fingerprint+sample_block+context_trade_time_fingerprint+eval_trade_time_fingerprint",
            "fast_portfolio_loop": not bool(args.disable_fast_portfolio_loop),
            "numba_rank_available": njit is not None,
            "numba_rank_enabled": (
                njit is not None
                and os.environ.get("PHASE3CM_DISABLE_NUMBA", "0") != "1"
                and not _NUMBA_RANK_RUNTIME_DISABLED
            ),
            "operator_cache_max_entries": int(args.operator_cache_max_entries),
            "operator_cache_max_mb": float(args.operator_cache_max_mb),
            "feature_matrix_cache_max_windows": int(args.feature_matrix_cache_max_windows),
            "feature_matrix_cache_max_mb": float(args.feature_matrix_cache_max_mb),
            "fast_group_rank": True,
            "omp_threads": os.environ.get("OMP_NUM_THREADS"),
            "mkl_threads": os.environ.get("MKL_NUM_THREADS"),
            "numexpr_max_threads": os.environ.get("NUMEXPR_MAX_THREADS"),
            "parallel_workers": 1,
            "global_worker_limit": 1,
            "parallel_axis": "none_serial",
            "event_aware_sample_times": bool(args.event_aware_sample_times),
            "event_sample_trade_times_per_shard": int(args.event_sample_trade_times_per_shard or 0),
            "semantic_only_no_label_construction": bool(args.semantic_only),
        },
    }
    if args.write_pnl_rows:
        _write_csv(output_root / "phase3cm_portfolio_pnl_rows.csv", pnl_rows)
    if args.write_reward_atoms:
        _write_csv(output_root / "phase3cm_reward_atoms.csv", reward_atom_rows)
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
    _write_csv(output_root / "phase3cm_candidate_pair_evaluation.csv", pair_evaluation_rows)
    _write_json(output_root / "phase3cm_candidate_pair_evaluation.json", pair_evaluation_rows)
    _write_csv(output_root / "phase3cm_shard_meta.csv", shard_meta)
    _write_csv(output_root / "phase3cm_split_manifest.csv", split_manifest_rows)
    _write_json(output_root / "phase3cm_split_reassignment_audit.json", split_reassignment_audit)
    _write_json(output_root / "phase3cm_train_reward_audit_summary.json", summary)
    report_root.mkdir(parents=True, exist_ok=True)
    _write_csv(report_root / "phase3cm_candidate_train_reward_summary.csv", reward_rows)
    _write_csv(report_root / "phase3cm_train_reward.csv", reward_rows)
    _write_csv(report_root / "phase3cm_candidate_pair_evaluation.csv", pair_evaluation_rows)
    _write_json(report_root / "phase3cm_candidate_pair_evaluation.json", pair_evaluation_rows)
    _write_csv(report_root / "phase3cm_candidate_split_horizon_summary.csv", split_horizon_rows)
    _write_csv(report_root / "phase3cm_split_manifest.csv", split_manifest_rows)
    _write_json(report_root / "phase3cm_split_reassignment_audit.json", split_reassignment_audit)
    if args.write_reward_atoms:
        _write_csv(report_root / "phase3cm_reward_atoms.csv", reward_atom_rows)
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
