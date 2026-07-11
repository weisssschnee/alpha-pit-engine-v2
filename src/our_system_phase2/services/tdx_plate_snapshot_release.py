"""Normalize TDX daily plate-component snapshots into an auditable PIT release."""

from __future__ import annotations

import csv
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any
from zipfile import ZipFile

import pandas as pd

from our_system_phase2.services.atomic_checkpoint import atomic_write_json, durable_flush
from our_system_phase2.services.pit_group_release import (
    FORWARD_SEALED_FROM,
    PITGroupReleaseSpec,
    build_interval_release,
    load_verified_release,
    write_release,
)


TDX_PLATE_RELEASE_VERSION = "nextgen_dark_tdx_plate_snapshots_v2"
_HEADER = ("板块代码", "交易日期", "成分股票代码", "成分股票名称")
_DATE_PATTERN = re.compile(r"(20\d{6})")
MAX_SNAPSHOT_MEMBERS = 5_000
MAX_SNAPSHOT_MEMBER_BYTES = 20 * 1024 * 1024
MAX_SNAPSHOT_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
MAX_SNAPSHOT_COMPRESSION_RATIO = 250.0


@dataclass(frozen=True, slots=True)
class TDXSnapshot:
    path: Path
    snapshot_date: pd.Timestamp
    source_observed_at: pd.Timestamp


@dataclass(frozen=True, slots=True)
class TDXSnapshotDiscovery:
    selected: tuple[TDXSnapshot, ...]
    excluded_2026_count: int
    excluded_unobservable_count: int
    excluded_invalid: tuple[dict[str, Any], ...]

    @property
    def excluded_invalid_count(self) -> int:
        return len(self.excluded_invalid)


@dataclass(frozen=True, slots=True)
class TDXPlateReleaseResult:
    membership_path: Path
    manifest_path: Path
    inventory_path: Path


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_date(path: Path) -> pd.Timestamp:
    match = _DATE_PATTERN.search(path.name)
    if match is None:
        raise ValueError(f"TDX snapshot filename has no YYYYMMDD date: {path}")
    return pd.to_datetime(match.group(1), format="%Y%m%d")


def discover_safe_snapshots(
    root: str | Path,
    *,
    cutoff: str = "2025-12-31",
) -> TDXSnapshotDiscovery:
    cutoff_time = pd.Timestamp(cutoff)
    if cutoff_time >= FORWARD_SEALED_FROM:
        raise ValueError("TDX plate cutoff cannot enter sealed 2026")
    selected: list[TDXSnapshot] = []
    excluded_2026 = 0
    excluded_unobservable = 0
    excluded_invalid: list[dict[str, Any]] = []
    for path in sorted(Path(root).rglob("*.zip")):
        try:
            snapshot_date = _snapshot_date(path)
        except ValueError:
            continue
        if snapshot_date >= FORWARD_SEALED_FROM:
            excluded_2026 += 1
            continue
        source_observed_at = snapshot_date + pd.Timedelta(days=1)
        if snapshot_date > cutoff_time or source_observed_at >= FORWARD_SEALED_FROM:
            excluded_unobservable += 1
            continue
        with ZipFile(path) as archive:
            all_members = [info for info in archive.infolist() if not info.is_dir()]
            if len(all_members) > MAX_SNAPSHOT_MEMBERS:
                raise ValueError(f"TDX snapshot member limit exceeded: {path}")
            if any(info.file_size > MAX_SNAPSHOT_MEMBER_BYTES for info in all_members):
                raise ValueError(f"TDX snapshot member size limit exceeded: {path}")
            total_bytes = sum(info.file_size for info in all_members)
            if total_bytes > MAX_SNAPSHOT_TOTAL_BYTES:
                raise ValueError(f"TDX snapshot expansion limit exceeded: {path}")
            compressed = sum(max(1, info.compress_size) for info in all_members)
            if total_bytes / max(1, compressed) > MAX_SNAPSHOT_COMPRESSION_RATIO:
                raise ValueError(f"TDX snapshot compression ratio limit exceeded: {path}")
            csv_members = [
                info
                for info in all_members
                if not info.is_dir() and info.filename.lower().endswith(".csv") and info.file_size > 0
            ]
        if not csv_members:
            excluded_invalid.append(
                {
                    "path": str(path),
                    "snapshot_date": snapshot_date.strftime("%Y-%m-%d"),
                    "size": path.stat().st_size,
                    "reason": "no_nonempty_csv_members",
                }
            )
            continue
        selected.append(TDXSnapshot(path, snapshot_date, source_observed_at))
    selected.sort(key=lambda item: item.snapshot_date)
    dates = [item.snapshot_date for item in selected]
    duplicate_dates = sorted({date for date in dates if dates.count(date) > 1})
    if duplicate_dates:
        raise ValueError(
            "duplicate TDX snapshot dates are forbidden: "
            f"{[date.strftime('%Y-%m-%d') for date in duplicate_dates]}"
        )
    if len(selected) < 2:
        raise ValueError("TDX plate release needs at least two safe historical snapshots")
    return TDXSnapshotDiscovery(
        tuple(selected), excluded_2026, excluded_unobservable, tuple(excluded_invalid)
    )


def _decode(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("TDX snapshot member is not UTF-8/GB18030")


def read_tdx_snapshot(path: str | Path) -> pd.DataFrame:
    snapshot_path = Path(path)
    snapshot_date = _snapshot_date(snapshot_path)
    source_observed_at = snapshot_date + pd.Timedelta(days=1)
    rows: list[tuple[str, str, pd.Timestamp, pd.Timestamp]] = []
    with ZipFile(snapshot_path) as archive:
        for info in sorted(archive.infolist(), key=lambda item: item.filename):
            if info.is_dir() or not info.filename.lower().endswith(".csv"):
                continue
            reader = csv.reader(io.StringIO(_decode(archive.read(info))))
            header = tuple(next(reader, ()))
            if header != _HEADER:
                raise ValueError(f"unexpected TDX snapshot header in {snapshot_path}!{info.filename}: {header}")
            for row in reader:
                if not row:
                    continue
                if len(row) != 4:
                    raise ValueError(f"malformed TDX membership row in {snapshot_path}!{info.filename}")
                group_id, row_date, code, _ = (item.strip() for item in row)
                if pd.to_datetime(row_date, format="%Y%m%d") != snapshot_date:
                    raise ValueError(f"TDX row date disagrees with snapshot filename: {row_date}")
                rows.append((code, group_id, snapshot_date, source_observed_at))
    frame = pd.DataFrame(
        rows,
        columns=["code", "group_id", "snapshot_at", "source_observed_at"],
    )
    if frame.empty:
        raise ValueError(f"TDX snapshot is empty: {snapshot_path}")
    if frame.duplicated(["code", "group_id"]).any():
        raise ValueError(f"duplicate TDX membership within snapshot: {snapshot_path}")
    return frame.sort_values(["code", "group_id"], kind="mergesort").reset_index(drop=True)


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


def _normalized_snapshot(snapshot: TDXSnapshot, output_root: Path, source_sha256: str) -> Path:
    day = snapshot.snapshot_date.strftime("%Y%m%d")
    path = output_root / "snapshots" / f"tdx_plate_{day}.parquet"
    meta_path = output_root / "snapshot_manifests" / f"tdx_plate_{day}.json"
    if path.exists() and meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("source_sha256") == source_sha256:
            return path
    frame = read_tdx_snapshot(snapshot.path)
    _atomic_parquet(frame, path)
    atomic_write_json(
        meta_path,
        {
            "snapshot_date": day,
            "source_path": str(snapshot.path),
            "source_sha256": source_sha256,
            "row_count": len(frame),
            "output_path": str(path),
            "output_sha256": _file_sha256(path),
        },
    )
    return path


def _normalize_snapshot_job(arguments: tuple[TDXSnapshot, Path, str]) -> Path:
    return _normalized_snapshot(*arguments)


def build_tdx_plate_release(
    snapshot_root: str | Path,
    output_root: str | Path,
    *,
    cutoff: str = "2025-12-31",
    workers: int = 1,
    retrieved_at: str = "2026-07-11T00:00:00+08:00",
) -> TDXPlateReleaseResult:
    if workers <= 0:
        raise ValueError("TDX plate workers must be positive")
    output = Path(output_root)
    discovery = discover_safe_snapshots(snapshot_root, cutoff=cutoff)
    source_hashes = {snapshot.path: _file_sha256(snapshot.path) for snapshot in discovery.selected}
    inventory_path = output / "tdx_plate_source_inventory.json"
    membership_path = output / "release" / "pit_group_membership.parquet"
    manifest_path = output / "release" / "pit_group_membership.manifest.json"
    if inventory_path.exists() and membership_path.exists() and manifest_path.exists():
        existing_inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
        existing_sources = [
            (item["path"], item["sha256"]) for item in existing_inventory.get("snapshots", [])
        ]
        current_sources = [
            (str(snapshot.path), source_hashes[snapshot.path]) for snapshot in discovery.selected
        ]
        if (
            existing_sources == current_sources
            and existing_inventory.get("inventory_version") == TDX_PLATE_RELEASE_VERSION
            and existing_inventory.get("excluded_2026_snapshot_count") == discovery.excluded_2026_count
            and existing_inventory.get("excluded_unobservable_snapshot_count")
            == discovery.excluded_unobservable_count
            and existing_inventory.get("excluded_invalid_snapshot_count") == discovery.excluded_invalid_count
        ):
            load_verified_release(
                membership_path,
                manifest_path,
                source_path=inventory_path,
            )
            return TDXPlateReleaseResult(membership_path, manifest_path, inventory_path)
    jobs = [
        (snapshot, output, source_hashes[snapshot.path]) for snapshot in discovery.selected
    ]
    if workers == 1:
        normalized_paths = [_normalize_snapshot_job(job) for job in jobs]
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            normalized_paths = list(executor.map(_normalize_snapshot_job, jobs))
    inventory_rows: list[dict[str, Any]] = []
    active: dict[tuple[str, str], tuple[pd.Timestamp, pd.Timestamp]] = {}
    intervals: list[dict[str, Any]] = []
    input_row_count = 0
    for snapshot, normalized_path in zip(discovery.selected, normalized_paths):
        source_hash = source_hashes[snapshot.path]
        frame = pd.read_parquet(normalized_path)
        input_row_count += len(frame)
        current = set(zip(frame["code"].astype(str), frame["group_id"].astype(str)))
        prior = set(active)
        for key in sorted(prior - current):
            effective_from, observed_from = active.pop(key)
            intervals.append(
                {
                    "code": key[0],
                    "group_id": key[1],
                    "effective_from": effective_from,
                    "effective_to": snapshot.snapshot_date,
                    "source_observed_at": observed_from,
                    "source_observed_to": snapshot.source_observed_at,
                }
            )
        for key in sorted(current - prior):
            active[key] = (snapshot.snapshot_date, snapshot.source_observed_at)
        inventory_rows.append(
            {
                "snapshot_date": snapshot.snapshot_date.strftime("%Y-%m-%d"),
                "source_observed_at": snapshot.source_observed_at.isoformat(),
                "path": str(snapshot.path),
                "size": snapshot.path.stat().st_size,
                "sha256": source_hash,
                "row_count": len(frame),
            }
        )
    for (code, group_id), (effective_from, observed_from) in sorted(active.items()):
        intervals.append(
            {
                "code": code,
                "group_id": group_id,
                "effective_from": effective_from,
                "effective_to": pd.NaT,
                "source_observed_at": observed_from,
                "source_observed_to": pd.NaT,
            }
        )

    inventory = {
        "inventory_version": TDX_PLATE_RELEASE_VERSION,
        "selected_snapshot_count": len(inventory_rows),
        "excluded_2026_snapshot_count": discovery.excluded_2026_count,
        "excluded_unobservable_snapshot_count": discovery.excluded_unobservable_count,
        "excluded_invalid_snapshot_count": discovery.excluded_invalid_count,
        "excluded_invalid_snapshots": list(discovery.excluded_invalid),
        "input_membership_row_count": input_row_count,
        "snapshots": inventory_rows,
        "forward_2026_performance_accessed": False,
        "source_observed_policy": "CONSERVATIVE_ASSUMPTION_NEXT_CALENDAR_DAY_00_NOT_VENDOR_TIMESTAMP",
    }
    atomic_write_json(inventory_path, inventory)
    source_hash = _file_sha256(inventory_path)
    release_spec = PITGroupReleaseSpec(
        source_name="tdx_plate_snapshots_user_supplied",
        source_version=f"{inventory_rows[0]['snapshot_date']}..{inventory_rows[-1]['snapshot_date']}",
        source_uri="baidu-share:user-supplied-tdx-plate-components",
        source_artifact_sha256=source_hash,
        retrieved_at=pd.Timestamp(retrieved_at).isoformat(),
        group_type="plate",
        membership_policy="multi",
        required_start=discovery.selected[0].source_observed_at.isoformat(),
        required_end="2025-12-31 15:00:00",
        maximum_observable_time="2025-12-31 23:59:59",
    )
    membership, manifest = build_interval_release(pd.DataFrame(intervals), release_spec)
    manifest.update(
        {
            "tdx_plate_release_version": TDX_PLATE_RELEASE_VERSION,
            "source_inventory": str(inventory_path),
            "selected_snapshot_count": len(inventory_rows),
            "excluded_2026_snapshot_count": discovery.excluded_2026_count,
            "excluded_invalid_snapshot_count": discovery.excluded_invalid_count,
            "full_2024_2025_coverage": False,
            "coverage_limitation": "historical component snapshots start at 2025-03-28",
            "forward_2026_performance_accessed": False,
            "source_observed_policy": "CONSERVATIVE_ASSUMPTION_NEXT_CALENDAR_DAY_00_NOT_VENDOR_TIMESTAMP",
        }
    )
    paths = write_release(membership, manifest, output / "release", source_path=inventory_path)
    return TDXPlateReleaseResult(paths["membership"], paths["manifest"], inventory_path)
