"""Run the frozen, performance-blind Sprint-2 event/state support audit."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time
from typing import Iterable

import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.atomic_checkpoint import atomic_write_json
from our_system_phase2.services.development_only_data_access import (
    DEVELOPMENT_ROLES,
    split_roles,
    validate_development_release,
)
from our_system_phase2.services.event_state_support_audit import (
    AUDIT_VERSION,
    REQUIRED_COLUMNS,
    audit_event_state_support,
)
from our_system_phase2.services.development_only_data_access import sha256_file


RUNNER_VERSION = "cn_sprint2_event_state_support_runner_v1"


def _repo_sha() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True, encoding="utf-8"
    ).strip()


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-root", type=Path, required=True)
    parser.add_argument("--release-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--expected-release-hash", required=True)
    parser.add_argument("--expected-repo-sha", required=True)
    parser.add_argument("--row-group-index", type=int, action="append", default=[])
    parser.add_argument("--output-root", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _arguments(argv)
    started = time.perf_counter()
    started_at = datetime.now(timezone.utc).isoformat()
    actual_sha = _repo_sha()
    if actual_sha != args.expected_repo_sha:
        raise PermissionError(
            f"repo SHA differs from frozen support-audit contract: {actual_sha} != {args.expected_repo_sha}"
        )
    row_group_indices = tuple(sorted(set(args.row_group_index or [0])))
    if any(index < 0 for index in row_group_indices):
        raise ValueError("row-group indices must be non-negative")

    release = validate_development_release(
        args.release_root,
        args.release_manifest,
        args.split_manifest,
        expected_release_hash=args.expected_release_hash,
    )
    roles = split_roles(args.split_manifest)
    entries: list[dict] = []

    def frames() -> Iterable[pd.DataFrame]:
        for item in sorted(release.files, key=lambda value: value.relative_path):
            stat = item.path.stat()
            if stat.st_size != item.size or stat.st_mtime_ns != item.mtime_ns:
                raise RuntimeError(f"release file changed after preflight: {item.relative_path}")
            parquet = pq.ParquetFile(item.path)
            missing = sorted(set(REQUIRED_COLUMNS) - set(parquet.schema_arrow.names))
            if missing:
                raise ValueError(f"support audit missing fields {missing}: {item.relative_path}")
            for row_group_index in row_group_indices:
                if row_group_index >= parquet.metadata.num_row_groups:
                    raise IndexError(f"row group unavailable: {item.relative_path}#{row_group_index}")
                group_manifest = item.row_groups[row_group_index]
                if group_manifest.get("data_role") != "development":
                    raise PermissionError(
                        f"support audit refuses non-development row group: {item.relative_path}#{row_group_index}"
                    )
                table = parquet.read_row_group(row_group_index, columns=list(REQUIRED_COLUMNS))
                frame = table.to_pandas()
                times = pd.to_datetime(frame["trade_time"], errors="raise", format="mixed")
                observed_roles = times.dt.normalize().map(roles)
                if observed_roles.isna().any():
                    raise PermissionError(
                        f"support audit read dates absent from split manifest: {item.relative_path}#{row_group_index}"
                    )
                role_counts = Counter(str(value) for value in observed_roles)
                forbidden = sorted(role for role in role_counts if role not in DEVELOPMENT_ROLES)
                if forbidden:
                    raise PermissionError(
                        f"support audit read forbidden roles {forbidden}: {item.relative_path}#{row_group_index}"
                    )
                entries.append(
                    {
                        "file_path": str(item.path),
                        "file_hash": item.sha256,
                        "row_group_id": row_group_index,
                        "assigned_data_role": "development",
                        "observed_rows_by_role": dict(sorted(role_counts.items())),
                        "rows_read": len(frame),
                        "release_hash": release.release_hash,
                        "loader_repo_sha": actual_sha,
                    }
                )
                yield frame

    # The release is sharded by source batch rather than by disjoint symbol.
    # Concatenate the fixed row groups before the audit so a code/session is
    # evaluated exactly once and results are invariant to shard iteration order.
    source_frames = list(frames())
    combined = pd.concat(source_frames, ignore_index=True)
    del source_frames
    report = audit_event_state_support([combined])
    report["source_chunk_count"] = len(entries)
    report["batch_order_invariance_policy"] = "concatenate_then_stable_sort_code_trade_time"
    args.output_root.mkdir(parents=True, exist_ok=True)
    report_path = args.output_root / "event_state_support_audit_v1.json"
    ledger_path = args.output_root / "development_read_ledger_v1.json"
    manifest_path = args.output_root / "run_manifest.json"
    atomic_write_json(report_path, report)

    rows_by_role: Counter[str] = Counter()
    for entry in entries:
        rows_by_role.update(entry["observed_rows_by_role"])
    ledger = {
        "ledger_version": "cn_sprint2_event_state_support_read_ledger_v1",
        "requested_data_role": "development",
        "release_hash": release.release_hash,
        "split_manifest_sha256": release.split_manifest_sha256,
        "repo_sha": actual_sha,
        "sampling_contract": {
            "method": "all_16_shards_fixed_parquet_row_groups",
            "row_group_indices": list(row_group_indices),
            "selection_uses_performance": False,
        },
        "entries": entries,
        "opened_file_count_by_role": {"development": len(entries)},
        "rows_read_by_role": dict(sorted(rows_by_role.items())),
        "validation_rows_read": int(rows_by_role["validation"]),
        "holdout_rows_read": int(rows_by_role["holdout"]),
        "forward_rows_read": int(rows_by_role["forward"]),
        "forbidden_file_open_count": 0,
        "forbidden_row_group_read_count": 0,
        "total_rows_read": sum(int(entry["rows_read"]) for entry in entries),
        "performance_columns_read": [],
    }
    atomic_write_json(ledger_path, ledger)
    outputs = {
        report_path.name: {
            "path": str(report_path.resolve()),
            "size": report_path.stat().st_size,
            "sha256": sha256_file(report_path),
        },
        ledger_path.name: {
            "path": str(ledger_path.resolve()),
            "size": ledger_path.stat().st_size,
            "sha256": sha256_file(ledger_path),
        },
    }
    run_manifest = {
        "manifest_version": "cn_sprint2_event_state_support_run_manifest_v1",
        "runner_version": RUNNER_VERSION,
        "audit_version": AUDIT_VERSION,
        "status": "COMPLETED",
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "wall_seconds": time.perf_counter() - started,
        "experiment_identity": {
            "repo_sha": actual_sha,
            "release_hash": release.release_hash,
            "split_manifest_sha256": release.split_manifest_sha256,
            "row_group_indices": list(row_group_indices),
        },
        "immutable_boundaries": {
            "data_role": "development",
            "performance_columns_allowed": False,
            "validation_allowed": False,
            "holdout_allowed": False,
            "forward_2026_allowed": False,
        },
        "command_arguments": {
            "release_root": str(args.release_root),
            "release_manifest": str(args.release_manifest),
            "split_manifest": str(args.split_manifest),
            "expected_release_hash": args.expected_release_hash,
            "expected_repo_sha": args.expected_repo_sha,
            "row_group_indices": list(row_group_indices),
            "output_root": str(args.output_root),
        },
        "result": {
            "event_decision": report["event"]["decision"],
            "state_system_decision": report["state_system_decision"],
            "zero_forbidden_access": all(
                ledger[key] == 0
                for key in (
                    "validation_rows_read", "holdout_rows_read", "forward_rows_read",
                    "forbidden_file_open_count", "forbidden_row_group_read_count",
                )
            ),
        },
        "outputs": outputs,
        "continuation_state": "READY_FOR_REPAIR_CAPABILITY_CANARY",
        "failure_reason": None,
    }
    atomic_write_json(manifest_path, run_manifest)
    print(json.dumps(run_manifest["result"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
