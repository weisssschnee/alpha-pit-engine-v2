from pathlib import Path

import pytest

from scripts.build_cn_core_pack_validation_session_sidecar import (
    _assert_positive_pit_coverage,
    _resolve_fundamental_partition_root,
)


def test_resolves_packaged_silver_partition_root(tmp_path: Path) -> None:
    packaged = tmp_path / "silver_partitioned"
    (packaged / "balance_sheet_report_em").mkdir(parents=True)
    (packaged / "profit_sheet_report_em").mkdir()

    resolved = _resolve_fundamental_partition_root(
        tmp_path,
        source_tables={"balance_sheet_report_em", "profit_sheet_report_em"},
    )

    assert resolved == packaged.resolve()


def test_rejects_root_without_required_source_tables(tmp_path: Path) -> None:
    (tmp_path / "silver_partitioned" / "balance_sheet_report_em").mkdir(
        parents=True
    )

    with pytest.raises(FileNotFoundError, match="profit_sheet_report_em"):
        _resolve_fundamental_partition_root(
            tmp_path,
            source_tables={"balance_sheet_report_em", "profit_sheet_report_em"},
        )


def test_rejects_canonical_field_with_zero_aggregate_coverage() -> None:
    specs = {"fund_a": {}, "fund_b": {}}
    records = [
        {"pit_coverage": {"fund_a": 0.25, "fund_b": 0.0}},
        {"pit_coverage": {"fund_a": 0.50, "fund_b": 0.0}},
    ]

    with pytest.raises(RuntimeError, match="fund_b"):
        _assert_positive_pit_coverage(specs=specs, records=records)


def test_accepts_positive_coverage_in_any_shard() -> None:
    specs = {"fund_a": {}, "fund_b": {}}
    records = [
        {"pit_coverage": {"fund_a": 0.25, "fund_b": 0.0}},
        {"pit_coverage": {"fund_a": 0.50, "fund_b": 0.75}},
    ]

    assert _assert_positive_pit_coverage(specs=specs, records=records) == {
        "fund_a": 0.5,
        "fund_b": 0.75,
    }
