from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest

import our_system_phase2.services.tdx_plate_snapshot_release as tdx_release
from our_system_phase2.services.pit_group_sidecar import (
    PITGroupContract,
    cross_sectional_group_confirmation,
)
from our_system_phase2.services.tdx_plate_snapshot_release import (
    build_tdx_plate_release,
    discover_safe_snapshots,
    read_tdx_snapshot,
)


HEADER = ["板块代码", "交易日期", "成分股票代码", "成分股票名称"]


def _snapshot(root: Path, day: str, groups: dict[str, list[str]]) -> Path:
    month = root / f"{day[:4]}-{day[4:6]}"
    month.mkdir(parents=True, exist_ok=True)
    path = month / f"plate_TDX_{day}.zip"
    with ZipFile(path, "w") as archive:
        for group_id, codes in groups.items():
            stream = io.StringIO()
            writer = csv.writer(stream)
            writer.writerow(HEADER)
            for code in codes:
                writer.writerow([group_id, day, code, f"name-{code}"])
            archive.writestr(f"{group_id}.csv", stream.getvalue().encode("utf-8-sig"))
    return path


def test_snapshot_discovery_hard_excludes_2026_and_unobservable_year_end(tmp_path: Path) -> None:
    _snapshot(tmp_path, "20250328", {"880001.TDX": ["000001.SZ"]})
    _snapshot(tmp_path, "20251230", {"880001.TDX": ["000001.SZ"]})
    _snapshot(tmp_path, "20251231", {"880001.TDX": ["000001.SZ"]})
    _snapshot(tmp_path, "20260105", {"880001.TDX": ["000001.SZ"]})
    empty = tmp_path / "2025-10" / "plate_TDX_20251024.zip"
    empty.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(empty, "w"):
        pass

    discovery = discover_safe_snapshots(tmp_path)

    assert [item.snapshot_date.strftime("%Y%m%d") for item in discovery.selected] == [
        "20250328",
        "20251230",
    ]
    assert discovery.excluded_unobservable_count == 1
    assert discovery.excluded_2026_count == 1
    assert discovery.excluded_invalid_count == 1
    assert discovery.excluded_invalid[0]["reason"] == "no_nonempty_csv_members"


def test_snapshot_reader_normalizes_membership_and_observation_clock(tmp_path: Path) -> None:
    path = _snapshot(
        tmp_path,
        "20250328",
        {"880001.TDX": ["000001.SZ", "600000.SH"], "880002.TDX": ["000001.SZ"]},
    )

    frame = read_tdx_snapshot(path)

    assert list(frame.columns) == [
        "code",
        "group_id",
        "snapshot_at",
        "source_observed_at",
    ]
    assert len(frame) == 3
    assert frame["snapshot_at"].eq(pd.Timestamp("2025-03-28")).all()
    assert frame["source_observed_at"].eq(pd.Timestamp("2025-03-29")).all()


def test_release_builds_entry_exit_intervals_and_auditable_inventory(tmp_path: Path) -> None:
    source = tmp_path / "source"
    _snapshot(source, "20250328", {"880001.TDX": ["000001.SZ", "600000.SH"]})
    _snapshot(source, "20250331", {"880001.TDX": ["000001.SZ"], "880002.TDX": ["600000.SH"]})
    _snapshot(source, "20251230", {"880001.TDX": ["000001.SZ"], "880002.TDX": ["600000.SH"]})
    output = tmp_path / "output"

    result = build_tdx_plate_release(source, output)
    membership = pd.read_parquet(result.membership_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    inventory = json.loads(result.inventory_path.read_text(encoding="utf-8"))

    exited = membership[(membership["code"] == "600000") & (membership["group_id"] == "880001.TDX")].iloc[0]
    assert exited["effective_to"] == pd.Timestamp("2025-03-31")
    assert exited["source_observed_to"] == pd.Timestamp("2025-04-01")
    assert manifest["source_artifact_verified_at_write"] is True
    assert manifest["forward_2026_performance_accessed"] is False
    assert manifest["full_2024_2025_coverage"] is False
    assert inventory["selected_snapshot_count"] == 3
    assert all(len(item["sha256"]) == 64 for item in inventory["snapshots"])

    second = build_tdx_plate_release(source, output)
    assert second.membership_path == result.membership_path
    assert second.manifest_path == result.manifest_path


def test_snapshot_discovery_rejects_duplicate_dates(tmp_path: Path) -> None:
    _snapshot(tmp_path / "copy-a", "20250328", {"880001.TDX": ["000001.SZ"]})
    _snapshot(tmp_path / "copy-b", "20250328", {"880001.TDX": ["000001.SZ"]})

    with pytest.raises(ValueError, match="duplicate TDX snapshot dates"):
        discover_safe_snapshots(tmp_path)


def test_snapshot_discovery_enforces_expansion_limits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _snapshot(tmp_path, "20250328", {"880001.TDX": ["000001.SZ"]})
    _snapshot(tmp_path, "20250331", {"880001.TDX": ["000001.SZ"]})
    monkeypatch.setattr(tdx_release, "MAX_SNAPSHOT_MEMBER_BYTES", 1)

    with pytest.raises(ValueError, match="member size limit"):
        discover_safe_snapshots(tmp_path)


def test_release_supports_real_pit_cross_sectional_join(tmp_path: Path) -> None:
    source = tmp_path / "source"
    _snapshot(source, "20250328", {"880001.TDX": ["000001.SZ", "600000.SH"]})
    _snapshot(source, "20250331", {"880001.TDX": ["000001.SZ"], "880002.TDX": ["600000.SH"]})
    result = build_tdx_plate_release(source, tmp_path / "output")
    membership = pd.read_parquet(result.membership_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    bars = pd.DataFrame(
        {
            "code": ["000001", "600000"],
            "trade_time": pd.to_datetime(["2025-03-29 09:30", "2025-03-29 09:30"]),
            "signal": [1.0, -1.0],
        }
    )
    contract = PITGroupContract(
        source_name=manifest["source_name"],
        source_version=manifest["source_version"],
        group_type="plate",
        membership_policy="multi",
    )

    confirmation = cross_sectional_group_confirmation(
        bars, membership, contract, value_field="signal"
    )

    assert confirmation["group_id"].eq("880001.TDX").all()
    assert confirmation["group_member_count"].eq(2).all()
    assert confirmation["group_value_mean"].eq(0.0).all()
    assert confirmation["group_up_ratio"].eq(0.5).all()
