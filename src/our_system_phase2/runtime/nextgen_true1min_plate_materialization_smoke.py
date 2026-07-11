"""Development-only structural smoke for sparse true1min plate materialization."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import traceback
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.atomic_checkpoint import atomic_write_json, durable_flush
from our_system_phase2.services.pit_group_release import FORWARD_SEALED_FROM
from our_system_phase2.services.true1min_plate_aggregation import (
    aggregate_true1min_plate,
    true1min_plate_field_specs,
)


EXPERIMENT_ID = "nextgen_true1min_plate_smoke"
PANEL_RELATIVE_PATH = Path(
    "phase3aq_wide_true1min/canary/phase3aq_true_1min_formula_canary.parquet"
)
PANEL_COLUMNS = ["code", "trade_time", "ret_1m", "amount_yuan", "volume"]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _frame_sha256(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update("\n".join(map(str, frame.columns)).encode("utf-8"))
    digest.update(pd.util.hash_pandas_object(frame, index=False).to_numpy().tobytes())
    return digest.hexdigest()


def _atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temporary = Path(name)
    try:
        frame.to_parquet(temporary, index=False)
        with temporary.open("r+b") as handle:
            durable_flush(handle)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _persist_attempt(attempt: Path, latest: Path, record: dict[str, Any]) -> None:
    atomic_write_json(attempt, record)
    atomic_write_json(
        latest,
        {
            "experiment_id": EXPERIMENT_ID,
            "latest_attempt": str(attempt),
            "status": record["status"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def _development_gate(split_manifest: Path, trade_date: pd.Timestamp) -> str:
    split = pd.read_csv(split_manifest, dtype=str)
    if not {"trade_date", "split"} <= set(split.columns):
        raise ValueError("split manifest requires trade_date and split")
    rows = split[pd.to_datetime(split["trade_date"], errors="coerce").dt.normalize().eq(trade_date)]
    if len(rows) != 1:
        raise ValueError(f"trade date is not unique in split manifest: {trade_date.date()}")
    role = str(rows.iloc[0]["split"]).strip().lower()
    if role not in {"train", "development"}:
        raise PermissionError(f"materialization smoke requires development/train, got {role}")
    return role


def _read_sampled_bars(
    panel_root: Path,
    *,
    trade_date: pd.Timestamp,
    row_group_index: int,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    panel_paths = sorted(panel_root.glob(f"shard_*/{PANEL_RELATIVE_PATH.as_posix()}"))
    if not panel_paths:
        raise FileNotFoundError(f"no canonical true1min panels under {panel_root}")
    frames: list[pd.DataFrame] = []
    inputs: list[dict[str, Any]] = []
    for path in panel_paths:
        parquet = pq.ParquetFile(path)
        if row_group_index < 0 or row_group_index >= parquet.metadata.num_row_groups:
            raise IndexError(f"row group {row_group_index} unavailable in {path}")
        missing = sorted(set(PANEL_COLUMNS) - set(parquet.schema_arrow.names))
        if missing:
            raise ValueError(f"true1min panel missing columns {missing}: {path}")
        table = parquet.read_row_group(row_group_index, columns=PANEL_COLUMNS)
        frame = table.to_pandas()
        frame["trade_time"] = pd.to_datetime(
            frame["trade_time"], errors="coerce", format="mixed"
        ).astype("datetime64[ns]")
        selected = frame[frame["trade_time"].dt.normalize().eq(trade_date)]
        if not selected.empty:
            frames.append(selected)
        inputs.append(
            {
                "path": str(path),
                "size": path.stat().st_size,
                "mtime_ns": path.stat().st_mtime_ns,
                "row_group_index": row_group_index,
                "row_group_rows": parquet.metadata.row_group(row_group_index).num_rows,
                "selected_rows": len(selected),
                "selected_rows_sha256": _frame_sha256(selected[PANEL_COLUMNS]),
            }
        )
    if not frames:
        raise ValueError(f"sampled row groups have no rows for {trade_date.date()}")
    return pd.concat(frames, ignore_index=True), inputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel-root", type=Path, required=True)
    parser.add_argument("--membership", type=Path, required=True)
    parser.add_argument("--membership-manifest", type=Path)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--row-group-index", type=int, default=0)
    parser.add_argument("--minimum-active-peers", type=int, default=2)
    args = parser.parse_args(argv)

    trade_date = pd.Timestamp(args.trade_date).normalize()
    if trade_date >= FORWARD_SEALED_FROM:
        raise ValueError("true1min plate smoke cannot enter sealed 2026")
    split_role = _development_gate(args.split_manifest, trade_date)
    membership_manifest_path = args.membership_manifest or args.membership.with_suffix(
        ".manifest.json"
    )
    membership_manifest = json.loads(membership_manifest_path.read_text(encoding="utf-8"))
    started_at = datetime.now(timezone.utc)
    attempt_id = started_at.strftime("%Y%m%dT%H%M%S%fZ")
    attempt_path = (
        args.output_root / "run_records" / "attempts" / f"{EXPERIMENT_ID}_{attempt_id}.json"
    )
    latest_path = args.output_root / "run_records" / f"{EXPERIMENT_ID}.latest.json"
    command = (
        "$env:PYTHONPATH='src'; python app.py "
        "nextgen-true1min-plate-materialization-smoke -- "
        f'--panel-root "{args.panel_root}" --membership "{args.membership}" '
        f'--membership-manifest "{membership_manifest_path}" '
        f'--split-manifest "{args.split_manifest}" --output-root "{args.output_root}" '
        f"--trade-date {args.trade_date} --row-group-index {args.row_group_index} "
        f"--minimum-active-peers {args.minimum_active_peers}"
    )
    record: dict[str, Any] = {
        "experiment_id": EXPERIMENT_ID,
        "attempt_id": attempt_id,
        "objective": "verify sparse PIT true1min plate materialization without performance evaluation",
        "status": "RUNNING",
        "mode": "light_structural_smoke",
        "started_at": started_at.isoformat(),
        "inputs": {
            "panel_root": str(args.panel_root),
            "membership": str(args.membership),
            "membership_size": args.membership.stat().st_size,
            "membership_sha256": _sha256(args.membership),
            "membership_manifest": str(membership_manifest_path),
            "membership_manifest_sha256": _sha256(membership_manifest_path),
            "split_manifest": str(args.split_manifest),
            "split_manifest_sha256": _sha256(args.split_manifest),
        },
        "parameters": {
            "trade_date": trade_date.strftime("%Y-%m-%d"),
            "split_role": split_role,
            "row_group_index": args.row_group_index,
            "minimum_active_peers": args.minimum_active_peers,
            "sampled_code_universe": True,
            "formal_performance_search_allowed": False,
            "forward_2026_allowed": False,
        },
        "commands": [command],
        "estimated_runtime": "1-3 minutes for one row group from each shard",
        "outputs": [],
        "reproducibility": "PENDING",
        "decision": "N/A_NO_PERFORMANCE_EVALUATION",
        "failure": None,
    }
    _persist_attempt(attempt_path, latest_path, record)
    started = time.perf_counter()
    try:
        bars, panel_inputs = _read_sampled_bars(
            args.panel_root,
            trade_date=trade_date,
            row_group_index=args.row_group_index,
        )
        membership = pd.read_parquet(args.membership)
        result = aggregate_true1min_plate(
            bars,
            membership,
            membership_manifest=membership_manifest,
            data_role="development",
            minimum_active_peers=args.minimum_active_peers,
        )
        artifact_root = args.output_root / "artifacts" / attempt_id
        group_path = artifact_root / "group_panel.parquet"
        stock_path = artifact_root / "stock_context.parquet"
        diagnostics_path = artifact_root / "diagnostics.json"
        registry_path = artifact_root / "field_registry.json"
        _atomic_parquet(result.group_panel, group_path)
        _atomic_parquet(result.stock_context, stock_path)
        diagnostics = dict(result.diagnostics)
        diagnostics.update(
            {
                "sampled_code_universe": True,
                "panel_inputs": panel_inputs,
                "split_manifest_role": split_role,
                "production_materialization_executed": False,
            }
        )
        atomic_write_json(diagnostics_path, diagnostics)
        atomic_write_json(
            registry_path,
            {
                "registry_version": "nextgen_dark_true1min_plate_fields_v1",
                "fields": [spec.canonical() for spec in true1min_plate_field_specs()],
                "formal_performance_search_allowed": False,
            },
        )
        for path, producer, purpose, stage in (
            (group_path, "aggregate_true1min_plate", "sampled group-minute panel", "diagnostic"),
            (stock_path, "aggregate_true1min_plate", "sampled leave-one-out stock context", "diagnostic"),
            (diagnostics_path, "materialization_smoke", "structural diagnostics", "diagnostic"),
            (registry_path, "true1min_plate_field_specs", "isolated field contract", "final"),
        ):
            record["outputs"].append(
                {
                    "path": str(path),
                    "producer": producer,
                    "purpose": purpose,
                    "stage": stage,
                    "size": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
        record.update(
            {
                "status": "COMPLETED",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_runtime_seconds": round(time.perf_counter() - started, 3),
                "reproducibility": "YES_FOR_CONSUMED_SAMPLED_ROWS_WITH_CONTENT_HASHES",
                "continuation": (
                    "review structural smoke; full development materialization needs a separately "
                    "authorized heavy scan and must not execute performance search"
                ),
            }
        )
        _persist_attempt(attempt_path, latest_path, record)
        print(
            json.dumps(
                {
                    "status": "COMPLETED",
                    "run_manifest": str(attempt_path),
                    "latest_pointer": str(latest_path),
                },
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        record.update(
            {
                "status": "FAILED",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_runtime_seconds": round(time.perf_counter() - started, 3),
                "reproducibility": "PARTIAL",
                "failure": {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                },
                "continuation": "fix the recorded structural failure and rerun a new append-only attempt",
            }
        )
        _persist_attempt(attempt_path, latest_path, record)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
