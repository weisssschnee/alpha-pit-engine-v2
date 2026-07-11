"""Two-pass deterministic signal-sketch audit for CN true1min EVALRESET.

The script never reads return labels or validation/holdout/forward performance.
It materializes expressions on a compact, fixed development-coordinate panel,
immediately compresses values into sketches, and supports resumable partitions.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import math
import os
import time
import zlib
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    _fields,
    _max_expression_window,
    _rank_by_group,
)
from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import _BoundedSeriesCache
from our_system_phase2.services.deterministic_signal_sketch import (
    SKETCH_VERSION,
    build_signal_sketch,
    cluster_distribution,
    cluster_sketches,
    decode_i8,
    projection_matrix,
    sketch_similarity,
)
from our_system_phase2.services.atomic_checkpoint import (
    AtomicRecordStore,
    atomic_write_bytes,
    atomic_write_json,
    durable_flush,
)
from our_system_phase2.services.real_market_validation import evaluate_panel_expression


COORDINATE_VERSION = "evalreset_development_coordinates_v1"
PERIOD_TIMES = {
    "A": {"open": "09:35", "morning": "10:15", "afternoon": "13:15", "close": "14:30"},
    "B": {"open": "09:50", "morning": "10:45", "afternoon": "13:45", "close": "14:55"},
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    handle = io.StringIO(newline="")
    writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    atomic_write_bytes(path, handle.getvalue().encode("utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    atomic_write_json(path, payload)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _development_dates(path: Path) -> list[pd.Timestamp]:
    rows = _read_csv(path)
    dates = [pd.Timestamp(row["trade_date"]) for row in rows if row.get("split") == "train"]
    if not dates or len(dates) != len(set(dates)):
        raise RuntimeError("fixed split manifest must contain unique development/train dates")
    return sorted(dates)


def _target_times(development_dates: list[pd.Timestamp], coordinate_set: str) -> list[dict[str, str]]:
    by_month: dict[str, list[pd.Timestamp]] = defaultdict(list)
    for value in development_dates:
        by_month[value.strftime("%Y-%m")].append(value)
    rows: list[dict[str, str]] = []
    for month, dates in sorted(by_month.items()):
        fractions = (1 / 7, 3 / 7, 5 / 7) if coordinate_set == "A" else (2 / 7, 4 / 7, 6 / 7)
        indices = sorted(
            {
                min(len(dates) - 1, max(0, int(round((len(dates) - 1) * fraction))))
                for fraction in fractions
            }
        )
        for date_slot, index in enumerate(indices, 1):
            selected = dates[index]
            for period, clock in PERIOD_TIMES[coordinate_set].items():
                rows.append(
                    {
                        "coordinate_set": coordinate_set,
                        "trade_month": month,
                        "trade_date": selected.strftime("%Y-%m-%d"),
                        "date_slot": str(date_slot),
                        "intraday_period": period,
                        "trade_time": f"{selected.strftime('%Y-%m-%d')} {clock}:00",
                    }
                )
    return rows


def _listing_bucket(first_date: pd.Timestamp, at_date: pd.Timestamp, data_start: pd.Timestamp) -> str:
    if first_date <= data_start + pd.Timedelta(days=7):
        return "left_censored_252plus"
    age = int((at_date.normalize() - first_date.normalize()).days)
    if age <= 60:
        return "age_0_60d"
    if age <= 252:
        return "age_61_252d"
    return "age_253plus_d"


def _profile_row_group(panel: Path, row_group: int, *, target_times: set[pd.Timestamp]) -> pd.DataFrame:
    parquet = pq.ParquetFile(panel)
    table = parquet.read_row_group(row_group, columns=["code", "trade_time", "volume"])
    frame = table.to_pandas()
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame["is_development_target"] = frame["trade_time"].isin(target_times)
    profile = (
        frame.groupby(frame["code"].astype(str), sort=True)
        .agg(
            first_date=("trade_time", "min"),
            last_date=("trade_time", "max"),
            bar_count=("trade_time", "count"),
            active_bar_count=("volume", lambda values: int((values.fillna(0.0) > 0.0).sum())),
            development_target_hits=("is_development_target", "sum"),
        )
        .reset_index(names="code")
    )
    profile["activation_density"] = profile["active_bar_count"] / profile["bar_count"].clip(lower=1)
    return profile


def _choose_codes(profile: pd.DataFrame, *, count: int, seed_text: str) -> list[str]:
    if profile.empty:
        return []
    work = profile.copy()
    work["stable_hash"] = work["code"].map(
        lambda code: hashlib.sha256(f"{COORDINATE_VERSION}|{seed_text}|{code}".encode()).hexdigest()
    )
    work = work.sort_values(["activation_density", "first_date", "stable_hash"], kind="mergesort")
    positions = np.linspace(0, len(work) - 1, num=min(count, len(work)), dtype=int)
    return work.iloc[np.unique(positions)]["code"].astype(str).tolist()


def prepare(args: argparse.Namespace) -> int:
    generation = _read_csv(args.generation_csv)
    expressions = [str(row.get("expression") or "") for row in generation]
    required_fields = sorted({field for expression in expressions for field in _fields(expression)})
    max_window = max((_max_expression_window(expression) for expression in expressions), default=0)
    if max_window > args.max_window:
        raise RuntimeError(f"observed expression window {max_window} exceeds configured {args.max_window}")
    panels = sorted(args.shard_root.rglob("*.parquet"))
    if len(panels) != 16:
        raise RuntimeError(f"expected 16 shard panels, observed {len(panels)}")
    development_dates = _development_dates(args.split_manifest)
    data_start = min(development_dates)
    data_end = max(development_dates)
    targets = {name: _target_times(development_dates, name) for name in ("A", "B")}

    selections: list[dict[str, Any]] = []
    panel_indices = {"A": [0, 4, 8, 12], "B": [2, 6, 10, 14]}
    for coordinate_set, indices in panel_indices.items():
        for interval, panel_index in enumerate(indices):
            panel = panels[panel_index]
            parquet = pq.ParquetFile(panel)
            quantile = interval if coordinate_set == "A" else (interval + 2) % 4
            row_group = min(parquet.num_row_groups - 1, int(round((parquet.num_row_groups - 1) * quantile / 3.0)))
            target_set = {pd.Timestamp(row["trade_time"]) for row in targets[coordinate_set]}
            profile = _profile_row_group(panel, row_group, target_times=target_set)
            profile = profile[profile["development_target_hits"] > 0].copy()
            codes = _choose_codes(
                profile,
                count=args.codes_per_row_group,
                seed_text=f"{coordinate_set}|{panel_index}|{row_group}",
            )
            selected_profile = profile[profile["code"].isin(codes)].copy()
            activity_order = selected_profile["activation_density"].rank(method="first", pct=True)
            for (_, row), pct in zip(selected_profile.iterrows(), activity_order, strict=True):
                selections.append(
                    {
                        "coordinate_set": "SHARED",
                        "source_coordinate_seed_set": coordinate_set,
                        "panel": str(panel),
                        "panel_index": panel_index,
                        "row_group": row_group,
                        "code": str(row["code"]),
                        "stock_coverage_interval": f"q{interval + 1}",
                        "first_observed_date": pd.Timestamp(row["first_date"]).strftime("%Y-%m-%d"),
                        "last_observed_date": pd.Timestamp(row["last_date"]).strftime("%Y-%m-%d"),
                        "activation_density": round(float(row["activation_density"]), 8),
                        "development_target_hits": int(row["development_target_hits"]),
                        "activation_density_bucket": "low" if pct <= 1 / 3 else ("mid" if pct <= 2 / 3 else "high"),
                    }
                )

    columns = ["code", "trade_time", "date", *required_fields]
    compact_parts: list[pd.DataFrame] = []
    selection_lookup = {row["code"]: row for row in selections}
    if len(selection_lookup) != len(selections):
        raise RuntimeError("shared coordinate stock universe contains duplicate codes")
    all_target_times = {
        pd.Timestamp(row["trade_time"])
        for coordinate_set in ("A", "B")
        for row in targets[coordinate_set]
    }
    for group_key, group in pd.DataFrame(selections).groupby(["panel", "row_group"], sort=True):
        panel_text, row_group = group_key
        panel = Path(str(panel_text))
        parquet = pq.ParquetFile(panel)
        available = set(parquet.schema_arrow.names)
        missing = sorted(set(columns) - available)
        if missing:
            raise RuntimeError(f"{panel} missing candidate fields: {missing}")
        table = parquet.read_row_group(int(row_group), columns=columns)
        codes = group["code"].astype(str).tolist()
        filtered = table.filter(pc.is_in(table["code"], value_set=pa.array(codes)))
        frame = filtered.to_pandas().sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
        frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
        keep = np.zeros(len(frame), dtype=bool)
        for _code, index in frame.groupby(frame["code"].astype(str), sort=False).groups.items():
            positions = np.asarray(list(index), dtype=int)
            hits = np.flatnonzero(frame.loc[positions, "trade_time"].isin(all_target_times).to_numpy())
            for hit in hits:
                keep[positions[max(0, hit - args.max_window) : hit + 1]] = True
        compact_parts.append(frame.loc[keep])

    compact = (
        pd.concat(compact_parts, ignore_index=True)
        .drop_duplicates(["code", "trade_time"], keep="first")
        .sort_values(["code", "trade_time"], kind="mergesort")
        .reset_index(drop=True)
    )
    row_lookup = {(str(code), pd.Timestamp(trade_time)): index for index, (code, trade_time) in enumerate(zip(compact["code"], compact["trade_time"], strict=True))}
    coordinate_rows: list[dict[str, Any]] = []
    for coordinate_set in ("A", "B"):
        codes = sorted(row["code"] for row in selections)
        for target in targets[coordinate_set]:
            at_date = pd.Timestamp(target["trade_date"])
            at_time = pd.Timestamp(target["trade_time"])
            for code in codes:
                profile = selection_lookup[code]
                first_date = pd.Timestamp(profile["first_observed_date"])
                coordinate_rows.append(
                    {
                        "coordinate_id": hashlib.sha256(f"{COORDINATE_VERSION}|{coordinate_set}|{code}|{at_time.isoformat()}".encode()).hexdigest()[:24],
                        "coordinate_version": COORDINATE_VERSION,
                        "coordinate_set": coordinate_set,
                        "code": code,
                        "trade_time": at_time.isoformat(),
                        "trade_date": target["trade_date"],
                        "trade_month": target["trade_month"],
                        "intraday_period": target["intraday_period"],
                        "stock_coverage_interval": profile["stock_coverage_interval"],
                        "listing_age_bucket": _listing_bucket(first_date, at_date, data_start),
                        "activation_density_bucket": profile["activation_density_bucket"],
                        "row_index": row_lookup.get((code, at_time), -1),
                        "data_role": "development",
                        "selection_uses_labels_or_performance": False,
                    }
                )
    coordinate_rows.sort(key=lambda row: (row["coordinate_set"], row["coordinate_id"]))

    args.output_root.mkdir(parents=True, exist_ok=True)
    panel_path = args.output_root / "signal_sketch_compact_panel.parquet"
    coordinate_path = args.output_root / "signal_sketch_coordinate_manifest.csv"
    selection_path = args.output_root / "signal_sketch_stock_selection.csv"
    compact.to_parquet(panel_path, index=False, compression="zstd")
    _write_csv(coordinate_path, coordinate_rows)
    _write_csv(selection_path, selections)
    manifest = {
        "run_type": "evalreset_signal_sketch_prepare",
        "coordinate_version": COORDINATE_VERSION,
        "sketch_version": SKETCH_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_role": "development",
        "labels_or_returns_read": False,
        "validation_holdout_forward_read": False,
        "source_shard_count": len(panels),
        "selected_stock_count": len(selections),
        "coordinate_count": len(coordinate_rows),
        "coordinate_count_by_set": dict(Counter(row["coordinate_set"] for row in coordinate_rows)),
        "materialized_coordinate_count_by_set": dict(Counter(row["coordinate_set"] for row in coordinate_rows if int(row["row_index"]) >= 0)),
        "compact_panel_rows": len(compact),
        "candidate_count": len(generation),
        "candidate_field_count": len(required_fields),
        "max_expression_window": max_window,
        "inputs": {
            "generation_csv": {"path": str(args.generation_csv), "sha256": _sha256(args.generation_csv)},
            "split_manifest": {"path": str(args.split_manifest), "sha256": _sha256(args.split_manifest)},
            "shard_root": str(args.shard_root),
        },
        "artifacts": {
            "compact_panel": {"path": str(panel_path), "sha256": _sha256(panel_path)},
            "coordinate_manifest": {"path": str(coordinate_path), "sha256": _sha256(coordinate_path)},
            "stock_selection": {"path": str(selection_path), "sha256": _sha256(selection_path)},
        },
    }
    _write_json(args.output_root / "signal_sketch_prepare_manifest.json", manifest)
    print(json.dumps(manifest, indent=2))
    return 0


def _encode_f4(values: np.ndarray) -> str:
    return base64.b64encode(zlib.compress(np.asarray(values, dtype="<f4").tobytes(), level=6)).decode("ascii")


def _decode_f4(payload: str) -> np.ndarray:
    return np.frombuffer(zlib.decompress(base64.b64decode(payload)), dtype="<f4").copy()


def _window_frame(
    full_frame: pd.DataFrame,
    coordinates: list[dict[str, str]],
    *,
    window: int,
) -> tuple[pd.DataFrame, dict[tuple[str, pd.Timestamp], int]]:
    target_keys = {
        (str(row["code"]), pd.Timestamp(row["trade_time"]))
        for row in coordinates
        if int(row.get("row_index") or -1) >= 0
    }
    keep = np.zeros(len(full_frame), dtype=bool)
    for code, index in full_frame.groupby(full_frame["code"].astype(str), sort=False).groups.items():
        positions = np.asarray(list(index), dtype=int)
        times = full_frame.loc[positions, "trade_time"].tolist()
        hits = [offset for offset, trade_time in enumerate(times) if (str(code), pd.Timestamp(trade_time)) in target_keys]
        for hit in hits:
            keep[positions[max(0, hit - int(window)) : hit + 1]] = True
    frame = full_frame.loc[keep].reset_index(drop=True)
    lookup = {
        (str(code), pd.Timestamp(trade_time)): index
        for index, (code, trade_time) in enumerate(zip(frame["code"], frame["trade_time"], strict=True))
    }
    return frame, lookup


def worker(args: argparse.Namespace) -> int:
    full_frame = pd.read_parquet(args.compact_panel).sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
    full_frame["trade_time"] = pd.to_datetime(full_frame["trade_time"], errors="coerce")
    coordinates = _read_csv(args.coordinate_manifest)
    by_set = {name: sorted([row for row in coordinates if row["coordinate_set"] == name], key=lambda row: row["coordinate_id"]) for name in ("A", "B")}
    projections = {
        name: projection_matrix([row["coordinate_id"] for row in rows], seed=101 if name == "A" else 211)
        for name, rows in by_set.items()
    }
    candidates = _read_csv(args.generation_csv)
    for candidate in candidates:
        candidate["_max_window"] = _max_expression_window(str(candidate.get("expression") or ""))
    candidates.sort(key=lambda row: (int(row["_max_window"]), str(row.get("family_id") or ""), str(row.get("motif_id") or ""), str(row.get("candidate_id") or "")))
    grouped: dict[tuple[int, str], list[dict[str, str]]] = defaultdict(list)
    for candidate in candidates:
        grouped[(int(candidate["_max_window"]), str(candidate.get("family_id") or ""))].append(candidate)
    loads = [0.0] * args.partition_count
    assigned: list[list[dict[str, str]]] = [[] for _ in range(args.partition_count)]
    weighted_groups = sorted(
        grouped.items(),
        key=lambda item: (-(len(item[1]) * (1.0 + item[0][0] / 20.0)), item[0]),
    )
    for (window, _family), rows in weighted_groups:
        owner = min(range(args.partition_count), key=lambda index: (loads[index], index))
        assigned[owner].extend(rows)
        loads[owner] += len(rows) * (1.0 + window / 20.0)
    candidates = sorted(
        assigned[args.partition_index],
        key=lambda row: (int(row["_max_window"]), str(row.get("family_id") or ""), str(row.get("motif_id") or ""), str(row.get("candidate_id") or "")),
    )
    exact_ids = {str(row.get("candidate_id") or "") for row in _read_csv(args.exact_candidate_csv)} if args.exact_candidate_csv else set()
    completed: set[tuple[str, str]] = set()
    resume_paths = [*args.resume_csv, args.output_csv]
    for resume_path in resume_paths:
        if resume_path.exists():
            completed.update(
                (row.get("candidate_id", ""), row.get("coordinate_set", ""))
                for row in _read_csv(resume_path)
                if row.get("candidate_id") and row.get("coordinate_set")
            )
    checkpoint_store = AtomicRecordStore(
        args.checkpoint_root or args.output_csv.with_suffix(".records")
    )
    checkpoint_rows: list[dict[str, Any]] = []
    for _key, payload in checkpoint_store.records():
        rows = list(payload.get("rows", []))
        checkpoint_rows.extend(rows)
        completed.update(
            (str(row.get("candidate_id") or ""), str(row.get("coordinate_set") or ""))
            for row in rows
            if row.get("candidate_id") and row.get("coordinate_set")
        )
    if checkpoint_rows:
        existing_rows = _read_csv(args.output_csv) if args.output_csv.exists() else []
        merged = {
            (str(row.get("candidate_id") or ""), str(row.get("coordinate_set") or "")): row
            for row in [*existing_rows, *checkpoint_rows]
            if row.get("candidate_id") and row.get("coordinate_set")
        }
        _write_csv(args.output_csv, [merged[key] for key in sorted(merged)])
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    cache_stats: dict[str, int] = {}
    current_window = -1
    frame = pd.DataFrame()
    coordinate_lookup: dict[tuple[str, pd.Timestamp], int] = {}
    cache: _BoundedSeriesCache | None = None
    mode = "a" if args.output_csv.exists() and args.output_csv.stat().st_size else "w"
    handle = args.output_csv.open(mode, encoding="utf-8", newline="")
    writer: csv.DictWriter[str] | None = None
    processed = 0
    failures = 0
    fsync_supported = True
    started = time.time()
    try:
        for candidate in candidates:
            candidate_id = str(candidate.get("candidate_id") or "")
            needed_sets = [name for name in ("A", "B") if (candidate_id, name) not in completed]
            if not needed_sets:
                continue
            try:
                candidate_window = int(candidate["_max_window"])
                if candidate_window != current_window:
                    current_window = candidate_window
                    frame, coordinate_lookup = _window_frame(full_frame, coordinates, window=current_window)
                    cache = _BoundedSeriesCache(
                        max_entries=args.cache_entries,
                        max_bytes=int(args.cache_max_mb * 1024 * 1024),
                        stats=cache_stats,
                        prefix=f"signal_sketch_w{current_window}_operator_cache",
                    )
                assert cache is not None
                signal = pd.to_numeric(evaluate_panel_expression(frame, str(candidate["expression"]), cache=cache), errors="coerce")
                materialized_positions = sorted(
                    {
                        coordinate_lookup[(str(row["code"]), pd.Timestamp(row["trade_time"]))]
                        for row in coordinates
                        if (str(row["code"]), pd.Timestamp(row["trade_time"])) in coordinate_lookup
                    }
                )
                coordinate_signal = signal.iloc[materialized_positions]
                coordinate_times = frame["trade_time"].iloc[materialized_positions]
                coordinate_ranks = _rank_by_group(
                    coordinate_signal.reset_index(drop=True),
                    coordinate_times.reset_index(drop=True),
                )
                rank_by_position = dict(zip(materialized_positions, coordinate_ranks.to_numpy(dtype=float), strict=True))
                candidate_outputs: list[dict[str, Any]] = []
                for name in needed_sets:
                    rows = by_set[name]
                    positions = [coordinate_lookup.get((str(row["code"]), pd.Timestamp(row["trade_time"])), -1) for row in rows]
                    values = np.asarray([signal.iloc[position] if position >= 0 else np.nan for position in positions], dtype=float)
                    rank_values = np.asarray([rank_by_position.get(position, np.nan) if position >= 0 else np.nan for position in positions], dtype=float)
                    sketch = build_signal_sketch(values, rank_values, rows, coordinate_set=name, projection=projections[name])
                    output = {
                        "candidate_id": candidate_id,
                        "expression_hash": candidate.get("expression_hash", ""),
                        "generator_arm": candidate.get("generator_arm", ""),
                        "family_id": candidate.get("family_id", ""),
                        "motif_id": candidate.get("motif_id", ""),
                        **sketch,
                        "exact_rank_vector": _encode_f4(rank_values) if candidate_id in exact_ids else "",
                        "exact_value_vector": _encode_f4(values) if candidate_id in exact_ids else "",
                        "worker_partition": args.partition_index,
                    }
                    candidate_outputs.append(output)
                checkpoint_store.put(candidate_id, {"rows": candidate_outputs})
                for output in candidate_outputs:
                    if writer is None:
                        writer = csv.DictWriter(handle, fieldnames=list(output))
                        if mode == "w":
                            writer.writeheader()
                    writer.writerow(output)
                processed += 1
                if processed % args.flush_every == 0:
                    fsync_supported = durable_flush(handle) and fsync_supported
                    print(json.dumps({"partition": args.partition_index, "processed": processed, "failures": failures, "elapsed_sec": round(time.time() - started, 1)}), flush=True)
            except Exception as exc:
                failures += 1
                with args.output_csv.with_suffix(".errors.jsonl").open("a", encoding="utf-8") as error_handle:
                    error_handle.write(json.dumps({"candidate_id": candidate_id, "error": repr(exc)}) + "\n")
    finally:
        try:
            fsync_supported = durable_flush(handle) and fsync_supported
        finally:
            handle.close()
    existing_rows = _read_csv(args.output_csv) if args.output_csv.exists() else []
    checkpoint_rows = [
        row
        for _key, payload in checkpoint_store.records()
        for row in payload.get("rows", [])
    ]
    merged = {
        (str(row.get("candidate_id") or ""), str(row.get("coordinate_set") or "")): row
        for row in [*existing_rows, *checkpoint_rows]
        if row.get("candidate_id") and row.get("coordinate_set")
    }
    _write_csv(args.output_csv, [merged[key] for key in sorted(merged)])
    summary = {
        "run_type": "evalreset_signal_sketch_worker",
        "partition_index": args.partition_index,
        "partition_count": args.partition_count,
        "estimated_partition_loads": [round(value, 3) for value in loads],
        "assigned_candidate_count": len(candidates),
        "processed_candidate_count": processed,
        "failure_count": failures,
        "elapsed_sec": round(time.time() - started, 3),
        "data_role": "development",
        "labels_or_returns_read": False,
        "validation_holdout_forward_read": False,
        "cache_stats": cache_stats,
        "atomic_checkpoint_root": str(checkpoint_store.root),
        "atomic_checkpoint_count": sum(1 for _ in checkpoint_store.records()),
        "fsync_supported": fsync_supported,
        "idempotent_resume": True,
        "output_csv": {"path": str(args.output_csv), "sha256": _sha256(args.output_csv)},
    }
    _write_json(args.output_csv.with_suffix(".summary.json"), summary)
    print(json.dumps(summary, indent=2))
    return 0 if failures == 0 else 2


def _stage_metrics(stage_rows: list[dict[str, str]], cluster_by_candidate: dict[str, int], previous_clusters: set[int]) -> dict[str, Any]:
    candidate_ids = [str(row.get("candidate_id") or "") for row in stage_rows]
    mapped_labels = [cluster_by_candidate[candidate_id] for candidate_id in candidate_ids if candidate_id in cluster_by_candidate]
    labels = [label for label in mapped_labels if label > 0]
    counts = Counter(labels)
    present = set(counts)
    distribution = cluster_distribution(labels)
    top_count = max(counts.values(), default=0)
    top3_count = sum(sorted(counts.values(), reverse=True)[:3])
    return {
        **distribution,
        "row_count": len(stage_rows),
        "mapped_candidate_count": len(mapped_labels),
        "signal_coverage_qualified_count": len(labels),
        "signal_coverage_limited_count": len(mapped_labels) - len(labels),
        "top1_share_all_candidates": round(top_count / max(1, len(mapped_labels)), 8),
        "top3_share_all_candidates": round(top3_count / max(1, len(mapped_labels)), 8),
        "retained_cluster_count": len(present & previous_clusters) if previous_clusters else len(present),
        "dropped_cluster_count": len(previous_clusters - present),
        "new_cluster_count": len(present - previous_clusters) if previous_clusters else len(present),
        "new_cluster_retention": round(len(present & previous_clusters) / max(1, len(previous_clusters)), 8) if previous_clusters else 1.0,
        "cluster_survival_probability": round(sum(1 for cluster in previous_clusters if cluster in present) / max(1, len(previous_clusters)), 8) if previous_clusters else 1.0,
        "per_cluster_trial_count": dict(sorted(counts.items())),
    }


def cluster_and_validate(args: argparse.Namespace) -> int:
    from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score

    sketch_rows: list[dict[str, str]] = []
    for path in args.worker_csv:
        sketch_rows.extend(_read_csv(path))
    deduped: dict[tuple[str, str], dict[str, str]] = {}
    for row in sketch_rows:
        key = (str(row.get("candidate_id") or ""), str(row.get("coordinate_set") or ""))
        if key[0] and key[1] in {"A", "B"} and row.get("rank_quantized_sketch"):
            deduped.setdefault(key, row)
    by_set = {name: [row for (candidate_id, coordinate_set), row in deduped.items() if coordinate_set == name] for name in ("A", "B")}
    generation_count = len(_read_csv(args.generation_csv))
    for name, rows in by_set.items():
        if len(rows) != generation_count:
            raise RuntimeError(f"coordinate set {name} expected {generation_count} sketches, observed {len(rows)}")
    finite_by_set = {
        name: {row["candidate_id"]: int(row.get("finite_count") or 0) for row in rows}
        for name, rows in by_set.items()
    }
    coordinate_count = int(by_set["A"][0].get("coordinate_count") or 0)
    joint_finite_min = max(128, int(math.ceil(0.05 * coordinate_count)))
    all_candidate_ids = sorted(finite_by_set["A"])
    eligible_ids = {
        candidate_id
        for candidate_id in all_candidate_ids
        if min(finite_by_set["A"][candidate_id], finite_by_set["B"][candidate_id]) >= joint_finite_min
    }
    labels_by_set = {
        name: cluster_sketches([row for row in rows if row["candidate_id"] in eligible_ids])
        for name, rows in by_set.items()
    }
    candidate_ids = sorted(eligible_ids)
    tuple_to_consensus: dict[tuple[int, int], int] = {}
    registry: list[dict[str, Any]] = []
    row_a = {row["candidate_id"]: row for row in by_set["A"]}
    for candidate_id in all_candidate_ids:
        if candidate_id in eligible_ids:
            pair = (labels_by_set["A"][candidate_id], labels_by_set["B"][candidate_id])
            consensus = tuple_to_consensus.setdefault(pair, len(tuple_to_consensus) + 1)
        else:
            pair = (0, 0)
            consensus = 0
        registry.append(
            {
                "candidate_id": candidate_id,
                "expression_hash": row_a[candidate_id].get("expression_hash", ""),
                "signal_coverage_status": "qualified" if candidate_id in eligible_ids else "coverage_limited",
                "finite_count_a": finite_by_set["A"][candidate_id],
                "finite_count_b": finite_by_set["B"][candidate_id],
                "cluster_a": pair[0],
                "cluster_b": pair[1],
                "consensus_cluster_id": consensus,
            }
        )
    cluster_a = [row["cluster_a"] for row in registry if row["consensus_cluster_id"] > 0]
    cluster_b = [row["cluster_b"] for row in registry if row["consensus_cluster_id"] > 0]
    stability = {
        "adjusted_rand_index": round(float(adjusted_rand_score(cluster_a, cluster_b)), 8),
        "normalized_mutual_information": round(float(normalized_mutual_info_score(cluster_a, cluster_b)), 8),
        "coordinate_a": cluster_distribution(cluster_a),
        "coordinate_b": cluster_distribution(cluster_b),
        "consensus": cluster_distribution(
            row["consensus_cluster_id"] for row in registry if row["consensus_cluster_id"] > 0
        ),
        "joint_finite_min": joint_finite_min,
        "joint_coverage_qualified_count": len(eligible_ids),
        "joint_coverage_limited_count": len(all_candidate_ids) - len(eligible_ids),
    }
    stability["top1_share_absolute_difference"] = round(
        abs(float(stability["coordinate_a"]["top1_share"]) - float(stability["coordinate_b"]["top1_share"])),
        8,
    )

    exact_rows = [
        row
        for row in deduped.values()
        if row.get("exact_rank_vector") and row.get("candidate_id") in eligible_ids
    ]
    exact_by_set = {name: [row for row in exact_rows if row["coordinate_set"] == name] for name in ("A", "B")}
    fidelity_pairs: list[dict[str, Any]] = []
    for name, rows in exact_by_set.items():
        rows = sorted(rows, key=lambda row: row["candidate_id"])
        for left in range(len(rows)):
            for right in range(left + 1, len(rows)):
                lrank, rrank = _decode_f4(rows[left]["exact_rank_vector"]), _decode_f4(rows[right]["exact_rank_vector"])
                valid = np.isfinite(lrank) & np.isfinite(rrank)
                if int(valid.sum()) < 8 or float(np.std(lrank[valid])) <= 0 or float(np.std(rrank[valid])) <= 0:
                    continue
                exact_corr = float(np.corrcoef(lrank[valid], rrank[valid])[0, 1])
                similarity = sketch_similarity(rows[left], rows[right])
                fidelity_pairs.append(
                    {
                        "coordinate_set": name,
                        "left": rows[left]["candidate_id"],
                        "right": rows[right]["candidate_id"],
                        "exact_correlation": exact_corr,
                        "sketch_rank_correlation": similarity["rank_corr"],
                        "same_sketch_cluster": labels_by_set[name][rows[left]["candidate_id"]] == labels_by_set[name][rows[right]["candidate_id"]],
                        "sketch_pair_equivalent": bool(
                            math.isfinite(similarity["rank_corr"])
                            and abs(similarity["rank_corr"]) >= args.exact_correlation_threshold
                        ),
                        "same_exact_cluster": abs(exact_corr) >= args.exact_correlation_threshold,
                    }
                )
    exact_values = np.asarray([row["exact_correlation"] for row in fidelity_pairs], dtype=float)
    sketch_values = np.asarray([row["sketch_rank_correlation"] for row in fidelity_pairs], dtype=float)
    valid_pair = np.isfinite(exact_values) & np.isfinite(sketch_values)
    same_pred = np.asarray([bool(row["same_sketch_cluster"]) for row in fidelity_pairs])
    pair_pred = np.asarray([bool(row["sketch_pair_equivalent"]) for row in fidelity_pairs])
    same_exact = np.asarray([bool(row["same_exact_cluster"]) for row in fidelity_pairs])
    boundary = valid_pair & (np.abs(np.abs(exact_values) - args.exact_correlation_threshold) <= 0.02)
    fidelity = {
        "candidate_count": len({row["candidate_id"] for row in exact_rows}),
        "pair_count": int(valid_pair.sum()),
        "sketch_distance_exact_correlation_pearson": round(float(np.corrcoef(sketch_values[valid_pair], exact_values[valid_pair])[0, 1]), 8) if int(valid_pair.sum()) >= 3 else None,
        "cluster_pair_purity": round(float(np.mean(same_exact[same_pred])) if bool(same_pred.any()) else 1.0, 8),
        "boundary_pair_count": int(boundary.sum()),
        "boundary_misclassification_rate": round(float(np.mean(pair_pred[boundary] != same_exact[boundary])) if bool(boundary.any()) else 0.0, 8),
    }
    gates = {
        "sketch_vs_exact_correlation": bool(
            fidelity["sketch_distance_exact_correlation_pearson"] is not None
            and float(fidelity["sketch_distance_exact_correlation_pearson"]) >= 0.90
        ),
        "cluster_pair_purity": float(fidelity["cluster_pair_purity"]) >= 0.90,
        "coordinate_cluster_ari": float(stability["adjusted_rand_index"]) >= 0.80,
        "coordinate_cluster_nmi": float(stability["normalized_mutual_information"]) >= 0.80,
        "top1_share_stability": float(stability["top1_share_absolute_difference"]) <= 0.03,
        "boundary_misclassification": float(fidelity["boundary_misclassification_rate"]) <= 0.10,
    }
    gates["all_pass"] = all(gates.values())

    consensus_by_candidate = {row["candidate_id"]: int(row["consensus_cluster_id"]) for row in registry}
    stages = [
        ("generation", _read_csv(args.generation_csv)),
        ("proxy", _read_csv(args.proxy_csv)),
        ("admission", _read_csv(args.admission_csv)),
        ("strict_reward", _read_csv(args.strict_csv)),
    ]
    collapse_audit = json.loads(args.collapse_audit_json.read_text(encoding="utf-8"))
    strict_stage = next(stage for stage in collapse_audit["stages"] if stage["stage"] == "strict_reward")
    qualified_ids = {row["candidate_id"] for row in strict_stage["behaviour"].get("candidate_assignments", [])}
    stages.append(("coverage_qualified", [row for row in stages[-1][1] if row.get("candidate_id") in qualified_ids]))
    funnel: dict[str, Any] = {}
    previous_clusters: set[int] = set()
    previous_counts: Counter[int] = Counter()
    for name, rows in stages:
        metrics = _stage_metrics(rows, consensus_by_candidate, previous_clusters)
        current_counts = Counter(
            consensus_by_candidate[row["candidate_id"]]
            for row in rows
            if row.get("candidate_id") in consensus_by_candidate
            and consensus_by_candidate[row["candidate_id"]] > 0
        )
        metrics["per_cluster_admission_rate"] = {
            str(cluster): round(current_counts[cluster] / max(1, previous_counts[cluster]), 8)
            for cluster in sorted(previous_counts)
        } if previous_counts else {}
        funnel[name] = metrics
        previous_clusters = set(current_counts)
        previous_counts = current_counts

    args.output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(args.output_root / "candidate_signal_cluster_registry.csv", registry)
    _write_csv(args.output_root / "sketch_fidelity_pairs.csv", fidelity_pairs)
    summary = {
        "decision": "EVALRESET_SIGNAL_SKETCH_AUDIT_DIAGNOSTIC_ONLY",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_role": "development",
        "labels_or_returns_used_for_coordinates_or_clustering": False,
        "validation_holdout_forward_used": False,
        "stability": stability,
        "fidelity": fidelity,
        "fidelity_gates": gates,
        "funnel": funnel,
        "first_signal_level_collapse": (
            "not_classified_pending_stagewise_review"
            if gates["all_pass"]
            else "not_classifiable_sketch_fidelity_failed"
        ),
    }
    _write_json(args.output_root / "signal_sketch_audit_summary.json", summary)
    stage_rows = []
    for stage, metrics in funnel.items():
        stage_rows.append(
            {
                "stage": stage,
                **{
                    key: value
                    for key, value in metrics.items()
                    if key not in {"per_cluster_trial_count", "per_cluster_admission_rate"}
                },
                "per_cluster_trial_count": json.dumps(metrics["per_cluster_trial_count"], sort_keys=True),
                "per_cluster_admission_rate": json.dumps(metrics["per_cluster_admission_rate"], sort_keys=True),
            }
        )
    _write_csv(args.output_root / "stage_signal_cluster_metrics.csv", stage_rows)
    lines = [
        "# EVALRESET Signal-Sketch Audit",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "Coordinates and clustering use development data only; labels, returns, validation, holdout, and forward data are excluded.",
        "",
        "## Fidelity and stability",
        "",
        f"- Joint coverage minimum: `{stability['joint_finite_min']}` coordinates.",
        f"- Qualified / limited: `{stability['joint_coverage_qualified_count']}` / `{stability['joint_coverage_limited_count']}`.",
        f"- A/B ARI: `{stability['adjusted_rand_index']}`; NMI: `{stability['normalized_mutual_information']}`.",
        f"- Top-1 share difference: `{stability['top1_share_absolute_difference']}`.",
        f"- Sketch/exact correlation: `{fidelity['sketch_distance_exact_correlation_pearson']}`.",
        f"- Cluster-pair purity: `{fidelity['cluster_pair_purity']}`.",
        f"- Boundary misclassification: `{fidelity['boundary_misclassification_rate']}`.",
        f"- All preregistered gates pass: `{gates['all_pass']}`.",
        "",
        "## Unified stage mapping",
        "",
        "| stage | rows | signal-qualified | limited | clusters | N_eff | top-1 eligible | top-3 eligible | top-1 all | survival |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for stage, metrics in funnel.items():
        lines.append(
            f"| {stage} | {metrics['row_count']} | {metrics['signal_coverage_qualified_count']} | "
            f"{metrics['signal_coverage_limited_count']} | {metrics['cluster_count']} | {metrics['n_eff']} | "
            f"{metrics['top1_share']} | {metrics['top3_share']} | {metrics['top1_share_all_candidates']} | "
            f"{metrics['cluster_survival_probability']} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            "- Coverage-limited candidates are reported separately and are not treated as an economic signal cluster.",
            "- Cluster IDs are fixed once at generation and mapped unchanged through every downstream stage.",
            "- The report does not compare AST-skeleton N_eff directly with signal-cluster N_eff.",
            f"- First signal-level collapse status: `{summary['first_signal_level_collapse']}`.",
        ]
    )
    (args.output_root / "SIGNAL_SKETCH_AUDIT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    prep.add_argument("--generation-csv", type=Path, required=True)
    prep.add_argument("--shard-root", type=Path, required=True)
    prep.add_argument("--split-manifest", type=Path, required=True)
    prep.add_argument("--output-root", type=Path, required=True)
    prep.add_argument("--codes-per-row-group", type=int, default=2)
    prep.add_argument("--max-window", type=int, default=120)
    prep.set_defaults(func=prepare)

    work = sub.add_parser("worker")
    work.add_argument("--compact-panel", type=Path, required=True)
    work.add_argument("--coordinate-manifest", type=Path, required=True)
    work.add_argument("--generation-csv", type=Path, required=True)
    work.add_argument("--exact-candidate-csv", type=Path)
    work.add_argument("--partition-index", type=int, required=True)
    work.add_argument("--partition-count", type=int, required=True)
    work.add_argument("--output-csv", type=Path, required=True)
    work.add_argument("--checkpoint-root", type=Path)
    work.add_argument("--resume-csv", type=Path, action="append", default=[])
    work.add_argument("--cache-entries", type=int, default=1024)
    work.add_argument("--cache-max-mb", type=float, default=1024.0)
    work.add_argument("--flush-every", type=int, default=25)
    work.set_defaults(func=worker)

    cluster = sub.add_parser("cluster")
    cluster.add_argument("--worker-csv", type=Path, action="append", required=True)
    cluster.add_argument("--generation-csv", type=Path, required=True)
    cluster.add_argument("--proxy-csv", type=Path, required=True)
    cluster.add_argument("--admission-csv", type=Path, required=True)
    cluster.add_argument("--strict-csv", type=Path, required=True)
    cluster.add_argument("--collapse-audit-json", type=Path, required=True)
    cluster.add_argument("--exact-correlation-threshold", type=float, default=0.95)
    cluster.add_argument("--output-root", type=Path, required=True)
    cluster.set_defaults(func=cluster_and_validate)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
