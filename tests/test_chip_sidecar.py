from __future__ import annotations

import csv
import hashlib
import io
import json
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest

import our_system_phase2.services.chip_sidecar as chip_sidecar
from our_system_phase2.services.chip_sidecar import (
    CHIP_FIELDS,
    build_chip_sidecar,
    chip_field_specs,
    point_in_time_chip_context,
)
from our_system_phase2.services.feature_state_fabric import FieldRole


HEADER = [
    "股票代码",
    "交易日期",
    "历史最低价",
    "历史最高价",
    "5分位成本",
    "15分位成本",
    "50分位成本",
    "85分位成本",
    "95分位成本",
    "加权平均成本",
    "胜率",
]


def _chip_zip(path: Path) -> Path:
    with ZipFile(path, "w") as archive:
        for code in ("000001.SZ", "600000.SH"):
            stream = io.StringIO()
            writer = csv.writer(stream)
            writer.writerow(HEADER)
            writer.writerow([code, "20260105", 1, 20, 5, 6, 8, 10, 12, 8.2, 60])
            writer.writerow([code, "20251231", 1, 20, 5, 6, 8, 10, 12, 8.2, 55])
            writer.writerow([code, "20251230", 1, 20, 4, 5, 7, 9, 11, 7.2, 100.32])
            writer.writerow([code, "20240102", 0, 10, 2, 3, 4, 5, 6, 4.1, 30])
            archive.writestr(f"{code}.csv", stream.getvalue().encode("utf-8-sig"))
    return path


def test_chip_builder_excludes_2026_and_writes_resumable_shards(tmp_path: Path) -> None:
    archive = _chip_zip(tmp_path / "chip.zip")
    output = tmp_path / "output"

    result = build_chip_sidecar(archive, output, members_per_shard=1)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    frames = [pd.read_parquet(path) for path in sorted((output / "shards").glob("*.parquet"))]
    frame = pd.concat(frames, ignore_index=True)

    assert len(frames) == 2
    assert frame["source_session"].max() == pd.Timestamp("2025-12-30")
    assert not frame["source_session"].dt.year.eq(2026).any()
    assert manifest["excluded_2026_row_count"] == 2
    assert manifest["excluded_unobservable_row_count"] == 2
    assert manifest["clipped_profit_ratio_count"] == 2
    assert manifest["sanitized_nonpositive_historical_low_count"] == 2
    assert manifest["maximum_raw_profit_ratio"] == 100.32
    assert frame.loc[frame["source_session"].eq(pd.Timestamp("2025-12-30")), "chip_profit_ratio"].eq(100.0).all()
    assert frame.loc[
        frame["source_session"].eq(pd.Timestamp("2024-01-02")), "chip_historical_low"
    ].isna().all()
    assert manifest["forward_2026_performance_accessed"] is False
    assert manifest["sealed_2026_rows_scanned_for_date_filter"] == 2
    assert manifest["sealed_2026_values_converted_or_used"] is False
    assert manifest["reward_or_performance_used"] is False
    assert manifest["reproducibility"] == "YES_WITH_PATH_INDEPENDENT_CONTENT_HASHES"
    assert len(manifest["source_archive_sha256"]) == 64
    assert set(CHIP_FIELDS.values()) <= set(frame.columns)


def test_chip_join_is_strictly_previous_session() -> None:
    chip = pd.DataFrame(
        {
            "code": ["000001", "000001"],
            "source_session": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "chip_cost_p50": [10.0, 11.0],
        }
    )
    bars = pd.DataFrame(
        {
            "code": ["000001", "000001"],
            "trade_time": pd.to_datetime(["2025-01-03 09:30", "2025-01-06 09:30"]),
        }
    )

    joined = point_in_time_chip_context(
        bars, chip, fields=["chip_cost_p50"], data_role="development"
    )

    assert joined["chip_cost_p50"].tolist() == [10.0, 11.0]
    assert joined["chip_source_session"].tolist() == [
        pd.Timestamp("2025-01-02"),
        pd.Timestamp("2025-01-03"),
    ]

    sealed = bars.copy()
    sealed.loc[0, "trade_time"] = pd.Timestamp("2026-01-05 09:30")
    try:
        point_in_time_chip_context(
            sealed, chip, fields=["chip_cost_p50"], data_role="development"
        )
    except ValueError as exc:
        assert "sealed 2026" in str(exc)
    else:
        raise AssertionError("sealed 2026 chip join should fail")


def test_chip_fields_are_isolated_from_primary_search() -> None:
    specs = {spec.name: spec for spec in chip_field_specs()}

    assert specs["chip_cost_p50"].role is FieldRole.INTERACTION_ONLY
    assert specs["chip_profit_ratio"].role is FieldRole.CONDITION_ONLY
    assert specs["chip_historical_low"].role is FieldRole.BENCHMARK_ONLY
    assert all(spec.source_session_field == "chip_source_session" for spec in specs.values())


def test_chip_builder_enforces_archive_expansion_limits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = _chip_zip(tmp_path / "chip.zip")
    monkeypatch.setattr(chip_sidecar, "MAX_CHIP_MEMBER_BYTES", 1)

    with pytest.raises(ValueError, match="member size limit"):
        build_chip_sidecar(archive, tmp_path / "output")


def test_chip_builder_migrates_cached_zero_low_sentinel(tmp_path: Path) -> None:
    archive = _chip_zip(tmp_path / "chip.zip")
    output = tmp_path / "output"
    first = build_chip_sidecar(archive, output, members_per_shard=2)
    shard = first.shard_paths[0]
    frame = pd.read_parquet(shard)
    frame.loc[0, "chip_historical_low"] = 0.0
    frame.to_parquet(shard, index=False)
    meta_path = next((output / "shard_manifests").glob("*.json"))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta.pop("sanitized_nonpositive_historical_low_count")
    meta["output_sha256"] = hashlib.sha256(shard.read_bytes()).hexdigest()
    meta_path.write_text(json.dumps(meta), encoding="utf-8")

    second = build_chip_sidecar(archive, output, members_per_shard=2)
    migrated = pd.read_parquet(second.shard_paths[0])
    migrated_meta = json.loads(meta_path.read_text(encoding="utf-8"))

    assert pd.isna(migrated.loc[0, "chip_historical_low"])
    assert migrated_meta["sanitized_nonpositive_historical_low_count"] == 1
