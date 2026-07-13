from __future__ import annotations

import hashlib
import json
from argparse import Namespace
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from our_system_phase2.services.pit_fundamental_fabric import (
    DeterministicSessionCache,
    FundamentalFieldRequest,
    PITFundamentalFabricAdapter,
    StockSessionAsOfResolver,
    compute_disclosed_change,
    compute_ttm_from_disclosed_ytd,
    conservative_financial_versions,
    conservative_holder_episodes,
    next_session_open,
    source_partition_path,
)
from scripts.build_pit_fundamental_fabric import DATASETS, _parquet_cutoff_value, build


REPO = Path(__file__).resolve().parents[1]
FROZEN_PACK = REPO / "reports/cn_broad_event_recovery_20260713/DISCOVERY_ENTRY_PACK.json"
FROZEN_PACK_SHA256 = "2f5174427525635bb917f613e1215c4fb3dc7793daf07b3dc5a2bb34106333f8"


def _sessions() -> pd.DatetimeIndex:
    return pd.DatetimeIndex(pd.to_datetime(["2024-04-19", "2024-04-22", "2024-04-23", "2025-04-21", "2025-04-22"]))


def test_parquet_cutoff_value_matches_file_local_disclosure_type() -> None:
    maximum = pd.Timestamp("2025-07-07 15:00:00")
    assert _parquet_cutoff_value(pa.field("NOTICE_DATE", pa.string()), maximum) == "2025-07-07 15:00:00"
    assert _parquet_cutoff_value(pa.field("NOTICE_DATE", pa.date32()), maximum) == maximum.date()
    assert _parquet_cutoff_value(pa.field("NOTICE_DATE", pa.timestamp("ns")), maximum) == maximum.to_pydatetime()

    with pytest.raises(TypeError, match="unsupported observable-time physical type"):
        _parquet_cutoff_value(pa.field("NOTICE_DATE", pa.int64()), maximum)


def _finance_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source_code6": ["000001", "000001", "000001"],
            "REPORT_DATE": ["2023-12-31", "2024-03-31", "2024-06-30"],
            "NOTICE_DATE": ["2024-04-19", "2024-04-19", "2024-08-01"],
            "UPDATE_DATE": ["2025-04-21", "2024-04-19", None],
            "TOTAL_ASSETS": [100.0, 110.0, 120.0],
        }
    )


def test_date_only_clock_matures_at_next_declared_session() -> None:
    result = next_session_open(pd.Series(pd.to_datetime(["2024-04-19", "2024-04-22"])), _sessions())

    assert result.iloc[0] == pd.Timestamp("2024-04-22 09:30:00")
    assert result.iloc[1] == pd.Timestamp("2024-04-23 09:30:00")


def test_current_snapshot_never_backfills_before_update_and_missing_update_fails_closed() -> None:
    versions = conservative_financial_versions(
        _finance_frame(),
        table="balance_sheet_report_em",
        sessions=_sessions(),
        maximum_observable_time="2025-04-22 15:00:00",
    )

    annual = versions.loc[versions["report_period"].eq(pd.Timestamp("2023-12-31"))].iloc[0]
    q1 = versions.loc[versions["report_period"].eq(pd.Timestamp("2024-03-31"))].iloc[0]
    q2 = versions.loc[versions["report_period"].eq(pd.Timestamp("2024-06-30"))].iloc[0]
    assert annual.observable_time == pd.Timestamp("2025-04-22 09:30:00")
    assert annual.version_kind == "LATEST_REVISED_SNAPSHOT_ONLY"
    assert not annual.disclosure_event_eligible
    assert q1.observable_time == pd.Timestamp("2024-04-22 09:30:00")
    assert q1.disclosure_event_eligible
    assert q2.pit_status == "PIT_CONTRACT_UNRESOLVED"


def test_asof_resolution_preserves_unknown_until_safe_version_time() -> None:
    versions = conservative_financial_versions(
        _finance_frame().iloc[:2],
        table="balance_sheet_report_em",
        sessions=_sessions(),
        maximum_observable_time="2025-04-22 15:00:00",
    )
    coordinates = pd.DataFrame(
        {
            "code": ["000001", "000001", "000001"],
            "session_time": pd.to_datetime(["2024-04-19 09:30", "2024-04-22 09:30", "2025-04-22 09:30"]),
        }
    )
    output = StockSessionAsOfResolver(maximum_observable_time="2025-04-22 15:00").resolve(
        versions, coordinates, value_fields=["TOTAL_ASSETS"]
    )

    assert np.isnan(output.iloc[0].TOTAL_ASSETS)
    assert output.iloc[1].TOTAL_ASSETS == 110.0
    # A late revision of an older annual report must not replace the newer Q1
    # report as the latest company level.
    assert output.iloc[2].TOTAL_ASSETS == 110.0


def test_asof_resolver_rejects_non_development_coordinate() -> None:
    versions = conservative_financial_versions(
        _finance_frame().iloc[:2], table="balance_sheet_report_em", sessions=_sessions()
    )
    with pytest.raises(PermissionError, match="development-only"):
        StockSessionAsOfResolver(maximum_observable_time="2024-12-31 15:00").resolve(
            versions,
            pd.DataFrame({"code": ["000001"], "session_time": pd.to_datetime(["2025-01-02 09:30"])}),
            value_fields=["TOTAL_ASSETS"],
        )


def test_revision_replay_uses_the_version_visible_at_each_session() -> None:
    frame = pd.DataFrame(
        {
            "source_code6": ["000001", "000001"],
            "REPORT_DATE": ["2023-12-31", "2023-12-31"],
            "NOTICE_DATE": ["2024-04-19", "2024-04-19"],
            "UPDATE_DATE": ["2024-04-19", "2025-04-21"],
            "TOTAL_ASSETS": [100.0, 105.0],
        }
    )
    versions = conservative_financial_versions(
        frame,
        table="balance_sheet_report_em",
        sessions=_sessions(),
        maximum_observable_time="2025-04-22 15:00",
    )
    coordinates = pd.DataFrame(
        {
            "code": ["000001", "000001"],
            "session_time": pd.to_datetime(["2024-04-22 09:30", "2025-04-22 09:30"]),
        }
    )
    output = StockSessionAsOfResolver(maximum_observable_time="2025-04-22 15:00").resolve(
        versions, coordinates, value_fields=["TOTAL_ASSETS"]
    )

    assert output["TOTAL_ASSETS"].tolist() == [100.0, 105.0]


def test_holder_rows_without_announcement_remain_unobservable() -> None:
    frame = pd.DataFrame(
        {
            "source_code6": ["000001", "000001"],
            "截至日期": ["2024-03-31", "2024-06-30"],
            "公告日期": ["2024-04-19", None],
            "股东名称": ["A", "B"],
        }
    )
    output = conservative_holder_episodes(frame, sessions=_sessions())

    assert output.iloc[0].observable_time == pd.Timestamp("2024-04-22 09:30")
    assert output.iloc[1].pit_status == "PIT_CONTRACT_UNRESOLVED"


def test_pre_calendar_disclosure_can_seed_level_but_not_fake_event() -> None:
    finance = pd.DataFrame(
        {
            "source_code6": ["000001"],
            "REPORT_DATE": ["2022-12-31"],
            "NOTICE_DATE": ["2023-03-01"],
            "UPDATE_DATE": ["2023-03-01"],
            "TOTAL_ASSETS": [90.0],
        }
    )
    output = conservative_financial_versions(
        finance, table="balance_sheet_report_em", sessions=_sessions()
    )

    assert output.iloc[0].pit_status == "ELIGIBLE_CURRENT_SNAPSHOT_VERSION"
    assert not output.iloc[0].disclosure_event_eligible


def test_ttm_uses_only_components_observable_at_query_time() -> None:
    frame = pd.DataFrame(
        {
            "code": ["000001"] * 4,
            "report_period": pd.to_datetime(["2023-03-31", "2023-12-31", "2024-03-31", "2024-06-30"]),
            "observable_time": pd.to_datetime(["2023-04-20", "2024-03-20", "2024-04-20", "2024-08-20"]),
            "pit_status": ["ELIGIBLE_CURRENT_SNAPSHOT_VERSION"] * 4,
            "NETPROFIT": [10.0, 100.0, 12.0, 60.0],
        }
    )
    before_q2 = compute_ttm_from_disclosed_ytd(frame, value_field="NETPROFIT", as_of_time="2024-05-01")

    q1 = before_q2.loc[before_q2["report_period"].eq(pd.Timestamp("2024-03-31"))].iloc[0]
    assert q1.NETPROFIT__ttm == 102.0
    assert pd.Timestamp("2024-06-30") not in set(before_q2["report_period"])


def test_change_route_propagates_latest_dependency_clock() -> None:
    versions = pd.DataFrame(
        {
            "code": ["000001", "000001"],
            "report_period": pd.to_datetime(["2023-03-31", "2024-03-31"]),
            "observable_time": pd.to_datetime(["2025-01-20", "2024-04-20"]),
            "pit_status": ["ELIGIBLE_CURRENT_SNAPSHOT_VERSION"] * 2,
            "NETPROFIT": [10.0, 12.0],
            "version_id": ["old", "new"],
        }
    )
    derived = compute_disclosed_change(versions, value_field="NETPROFIT", transform="yoy")
    q1 = derived.loc[derived["report_period"].eq(pd.Timestamp("2024-03-31"))].iloc[0]

    assert q1.NETPROFIT__yoy == 2.0
    assert q1.observable_time == pd.Timestamp("2025-01-20")


def test_symbol_partition_prefix_and_unresolved_family_guard(tmp_path: Path) -> None:
    root = tmp_path / "source"
    table = root / "balance_sheet_report_em"
    table.mkdir(parents=True)
    (table / "SZ000001.parquet").touch()
    assert source_partition_path(root, "balance_sheet_report_em", "000001").name == "SZ000001.parquet"

    adapter = PITFundamentalFabricAdapter(
        source_root=root,
        sessions=_sessions(),
        maximum_observable_time="2025-04-22 15:00",
    )
    with pytest.raises(PermissionError, match="PIT_CONTRACT_UNRESOLVED"):
        adapter.materialize_level(
            FundamentalFieldRequest("zygc_em", "主营收入"),
            pd.DataFrame({"code": ["000001"], "session_time": pd.to_datetime(["2024-04-22 09:30"])}),
        )


def test_non_ascii_source_fields_get_distinct_stable_output_names() -> None:
    holder_count = FundamentalFieldRequest("main_stock_holder_sina", "股东总数")
    average_holding = FundamentalFieldRequest("main_stock_holder_sina", "平均持股数")

    assert holder_count.output_name.startswith("fund_holder_field_")
    assert holder_count.output_name != average_holding.output_name


def test_balance_sheet_ttm_route_is_rejected_before_source_access(tmp_path: Path) -> None:
    adapter = PITFundamentalFabricAdapter(
        source_root=tmp_path,
        sessions=_sessions(),
        maximum_observable_time="2025-04-22 15:00",
    )
    with pytest.raises(ValueError, match="balance-sheet stock levels"):
        adapter.materialize_change(
            FundamentalFieldRequest(
                "balance_sheet_report_em", "TOTAL_ASSETS", route="FUNDAMENTAL_CHANGE", transform="ttm"
            ),
            pd.DataFrame({"code": ["000001"], "session_time": pd.to_datetime(["2024-04-22 09:30"])}),
        )


def test_atomic_session_cache_is_deterministic(tmp_path: Path) -> None:
    cache = DeterministicSessionCache(tmp_path)
    coordinates = pd.DataFrame(
        {"code": ["000002", "000001"], "session_time": pd.to_datetime(["2024-01-02", "2024-01-02"])}
    )
    first = cache.key(registry_hash="r", source_manifest_hash="s", request={"x": 1}, coordinates=coordinates)
    second = cache.key(registry_hash="r", source_manifest_hash="s", request={"x": 1}, coordinates=coordinates.iloc[::-1])
    output, manifest = cache.write(first, coordinates, {"key": first})

    assert first == second
    assert output.exists() and manifest.exists()
    pd.testing.assert_frame_equal(cache.read(first), coordinates)


def _write_fixture_sources(root: Path) -> None:
    finance = pd.DataFrame(
        {
            "source_code6": ["000001"],
            "SECURITY_CODE": ["000001"],
            "REPORT_DATE": pd.to_datetime(["2023-12-31"]),
            "NOTICE_DATE": pd.to_datetime(["2024-03-20"]),
            "UPDATE_DATE": pd.to_datetime(["2024-03-20"]),
            "TOTAL_ASSETS": [100.0],
            "TOTAL_ASSETS_YOY": [0.1],
        }
    )
    for table in (
        "balance_sheet_report_em", "profit_sheet_report_em", "cash_flow_sheet_report_em"
    ):
        (root / table).mkdir(parents=True)
        finance.to_parquet(root / table / "SZ000001.parquet", index=False)
    zygc = pd.DataFrame({"source_code6": ["000001"], "报告日期": pd.to_datetime(["2023-12-31"]), "主营收入": [10.0]})
    (root / "zygc_em").mkdir(parents=True)
    zygc.to_parquet(root / "zygc_em" / "SZ000001.parquet", index=False)
    holder = pd.DataFrame(
        {"source_code6": ["000001"], "截至日期": pd.to_datetime(["2024-03-31"]), "公告日期": pd.to_datetime(["2024-04-19"]), "股东总数": [1000]}
    )
    (root / "main_stock_holder_sina").mkdir(parents=True)
    holder.to_parquet(root / "main_stock_holder_sina" / "SZ000001.parquet", index=False)


def test_builder_emits_required_contracts_without_search(tmp_path: Path) -> None:
    source = tmp_path / "source"
    _write_fixture_sources(source)
    split = tmp_path / "split.csv"
    pd.DataFrame(
        {"trade_date": ["2024-04-19", "2024-04-22", "2024-04-23"], "split": ["train", "train", "validation"]}
    ).to_csv(split, index=False)
    current = tmp_path / "current.json"
    current.write_text(json.dumps({"fields": [{"name": "ctx_holder_holder_num"}]}), encoding="utf-8")
    runtime = tmp_path / "runtime"
    report = tmp_path / "report"
    result = build(
        Namespace(
            repo=REPO,
            source_root=source,
            runtime_root=runtime,
            report_root=report,
            split_manifest=split,
            current_registry=current,
            run_id="fixture",
            repo_sha="fixture_sha",
            execution_host="TEST",
            frozen_broad_event_pack_sha256=FROZEN_PACK_SHA256,
            scan_coverage=True,
        )
    )

    assert result["pit_unresolved_families"] == ["zygc_em"]
    assert (runtime / "fundamental_source_universe.json").exists()
    assert (runtime / "fundamental_semantic_registry.json").exists()
    assert (runtime / "fundamental_observable_time_contract.json").exists()
    assert (runtime / "pit_sidecar_manifest.json").exists()
    assert (runtime / "fundamental_121_gap_report.csv").exists()
    assert (report / "coverage_missingness_report.json").exists()
    assert (report / "development_only_access_ledger.jsonl").exists()
    manifest = json.loads((report / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["boundaries"]["performance_search"] == "NOT_EXECUTED"
    assert manifest["boundaries"]["forward_2026_performance_dataset"] == "SEALED_NOT_READ"


def test_broad_event_discovery_pack_remains_exactly_frozen() -> None:
    assert hashlib.sha256(FROZEN_PACK.read_bytes()).hexdigest() == FROZEN_PACK_SHA256
