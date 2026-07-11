from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest

import our_system_phase2.services.tdx_plate_market_sidecar as plate_market
from our_system_phase2.services.feature_state_fabric import FieldRole
from our_system_phase2.services.pit_group_sidecar import (
    PITGroupContract,
    point_in_time_membership,
)
from our_system_phase2.services.tdx_plate_market_sidecar import (
    build_tdx_plate_market_sidecar,
    discover_tdx_plate_market_archives,
    plate_market_linkage_report,
    plate_market_field_specs,
    point_in_time_plate_market_context,
)


HEADER = [
    "日期",
    "代码",
    "名称",
    "开盘",
    "收盘",
    "最高",
    "最低",
    "成交量",
    "成交额",
    "上涨家数",
    "下跌家数",
]


def _market_zip(path: Path, code: str = "880301", name: str = "煤炭") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(HEADER)
    writer.writerow(["2026-01-05", code, name, 10, 11, 12, 9, 100, 1000, 8, 2])
    writer.writerow(["2025-12-31", code, name, 9, 10, 11, 8, 90, 900, 7, 3])
    writer.writerow(["2025-12-30", code, name, 8, 9, 10, 7, 80, 800, 6, 4])
    writer.writerow(["2024-01-02", code, name, 4, 5, 6, 3, 40, 400, 5, 5])
    with ZipFile(path, "w") as archive:
        archive.writestr(f"{code}_日k.csv", stream.getvalue().encode("utf-8-sig"))
    return path


def test_archive_discovery_requires_all_four_market_categories(tmp_path: Path) -> None:
    names = {
        "region": "通达信_地区板块_历史行情数据",
        "concept": "通达信_概念板块_历史行情数据",
        "industry": "通达信_行业板块_历史行情数据",
        "style": "通达信_风格板块_历史行情数据",
    }
    for category, directory in names.items():
        _market_zip(tmp_path / directory / f"{category}_日k_K线.zip")

    discovered = discover_tdx_plate_market_archives(tmp_path)

    assert set(discovered) == set(names)


def test_market_builder_filters_sealed_rows_and_writes_content_hashes(tmp_path: Path) -> None:
    archive = _market_zip(tmp_path / "industry.zip")
    result = build_tdx_plate_market_sidecar(
        {"industry": archive}, tmp_path / "output"
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    frame = pd.concat([pd.read_parquet(path) for path in result.shard_paths], ignore_index=True)

    assert frame["source_session"].tolist() == [
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2025-12-30"),
    ]
    assert frame["group_id"].eq("880301.TDX").all()
    assert frame["group_category"].eq("industry").all()
    assert manifest["excluded_2026_row_count"] == 1
    assert manifest["excluded_unobservable_row_count"] == 1
    assert manifest["sealed_2026_values_converted_or_used"] is False
    assert manifest["forward_2026_performance_accessed"] is False
    assert manifest["coverage_by_category"]["industry"]["distinct_group_count"] == 1
    assert manifest["all_archives_span_2024_2025"] is False
    assert len(manifest["archives"][0]["sha256"]) == 64
    assert len(manifest["shards"][0]["output_sha256"]) == 64


def test_market_join_is_strictly_previous_session_and_development_only() -> None:
    market = pd.DataFrame(
        {
            "group_id": ["880301.TDX", "880301.TDX"],
            "source_session": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "source_observed_at": pd.to_datetime(["2025-01-03", "2025-01-04"]),
            "plate_close": [10.0, 11.0],
        }
    )
    market["source_session"] = market["source_session"].astype("datetime64[s]")
    rows = pd.DataFrame(
        {
            "group_id": ["880301.TDX", "880301.TDX"],
            "trade_time": pd.to_datetime(["2025-01-03 09:30", "2025-01-06 09:30"]),
        }
    )
    rows["trade_time"] = rows["trade_time"].astype("datetime64[s]")

    joined = point_in_time_plate_market_context(
        rows, market, fields=["plate_close"], data_role="development"
    )

    assert joined["plate_close"].tolist() == [10.0, 11.0]
    assert joined["plate_market_source_session"].tolist() == [
        pd.Timestamp("2025-01-02"),
        pd.Timestamp("2025-01-03"),
    ]
    assert joined["plate_market_source_observed_at"].tolist() == [
        pd.Timestamp("2025-01-03"),
        pd.Timestamp("2025-01-04"),
    ]
    with pytest.raises(PermissionError, match="development-only"):
        point_in_time_plate_market_context(
            rows, market, fields=["plate_close"], data_role="sealed"
        )


def test_market_join_hides_late_revision_until_observed() -> None:
    market = pd.DataFrame(
        {
            "group_id": ["880301.TDX"],
            "source_session": pd.to_datetime(["2025-01-02"]),
            "source_observed_at": pd.to_datetime(["2025-01-06 12:00"]),
            "plate_close": [10.0],
        }
    )
    rows = pd.DataFrame(
        {
            "group_id": ["880301.TDX", "880301.TDX"],
            "trade_time": pd.to_datetime(["2025-01-03 09:30", "2025-01-06 13:00"]),
        }
    )

    joined = point_in_time_plate_market_context(
        rows, market, fields=["plate_close"], data_role="development"
    )

    assert pd.isna(joined.loc[0, "plate_close"])
    assert joined.loc[1, "plate_close"] == 10.0


def test_stock_membership_to_market_context_end_to_end() -> None:
    membership = pd.DataFrame(
        {
            "code": ["000001", "600000"],
            "group_id": ["880301.TDX", "880301.TDX"],
            "effective_from": pd.to_datetime(["2025-01-01", "2025-01-01"]),
            "effective_to": [pd.NaT, pd.NaT],
            "source_observed_at": pd.to_datetime(["2025-01-02", "2025-01-02"]),
            "source_observed_to": [pd.NaT, pd.NaT],
        }
    )
    bars = pd.DataFrame(
        {
            "code": ["000001", "600000"],
            "trade_time": pd.to_datetime(["2025-01-03 09:30", "2025-01-03 09:30"]),
        }
    )
    market = pd.DataFrame(
        {
            "group_id": ["880301.TDX"],
            "source_session": pd.to_datetime(["2025-01-02"]),
            "source_observed_at": pd.to_datetime(["2025-01-03"]),
            "plate_close": [10.0],
        }
    )
    contract = PITGroupContract("fixture", "v1", "plate", "multi")

    linked = point_in_time_membership(bars, membership, contract)
    joined = point_in_time_plate_market_context(
        linked, market, fields=["plate_close"], data_role="development"
    )

    assert joined["code"].tolist() == ["000001", "600000"]
    assert joined["plate_close"].eq(10.0).all()
    assert joined["plate_market_source_observed_at"].eq(pd.Timestamp("2025-01-03")).all()
    assert joined["source_observed_at"].eq(pd.Timestamp("2025-01-02")).all()


def test_market_fields_cannot_enter_primary_search() -> None:
    specs = {spec.name: spec for spec in plate_market_field_specs()}

    assert specs["plate_close"].role is FieldRole.BENCHMARK_ONLY
    assert specs["plate_up_count"].role is FieldRole.CONDITION_ONLY
    assert specs["plate_down_count"].role is FieldRole.CONDITION_ONLY
    assert all(spec.source_lag == 1 for spec in specs.values())


def test_market_linkage_report_exposes_unmatched_groups() -> None:
    membership = pd.DataFrame({"group_id": ["880301.TDX", "880302.TDX"]})
    market = pd.DataFrame({"group_id": ["880301.TDX", "880303.TDX"]})

    report = plate_market_linkage_report(membership, market)

    assert report["shared_group_count"] == 1
    assert report["membership_group_coverage_ratio"] == 0.5
    assert report["membership_without_market"] == ["880302.TDX"]
    assert report["market_without_membership"] == ["880303.TDX"]
    assert report["reward_or_performance_used"] is False


def test_market_builder_enforces_archive_expansion_limits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = _market_zip(tmp_path / "industry.zip")
    monkeypatch.setattr(plate_market, "MAX_MARKET_MEMBER_BYTES", 1)

    with pytest.raises(ValueError, match="member size limit"):
        build_tdx_plate_market_sidecar({"industry": archive}, tmp_path / "output")
