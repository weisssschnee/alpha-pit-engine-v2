from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import build_cn_finalist_session_authority as subject


def test_text_pit_st_values_are_not_coerced_to_null() -> None:
    values = pd.Series(["否", "是", 0, 1, False, True])

    observed = subject._normalize_pit_st(values)

    assert observed.tolist() == [False, True, False, True, False, True]
    with pytest.raises(ValueError, match="unknown values"):
        subject._normalize_pit_st(pd.Series(["未知"]))


def test_materialize_cli_requires_and_routes_pit_st_root() -> None:
    args = subject.build_parser().parse_args(
        [
            "materialize",
            "--release-root",
            "release",
            "--source-root",
            "source",
            "--pit-st-root",
            "pit-st",
            "--output-root",
            "output",
        ]
    )

    assert args.pit_st_root == Path("pit-st")
    fetch = subject.build_parser().parse_args(
        [
            "fetch",
            "--release-root",
            "release",
            "--source-root",
            "source",
        ]
    )
    assert not hasattr(fetch, "pit_st_root")


def test_freeze_pit_st_source_is_exact_date_and_hash_bound(
    tmp_path: Path,
) -> None:
    release = tmp_path / "release"
    panel = (
        release
        / "shard_00"
        / "phase3aq_wide_true1min"
        / "canary"
        / "phase3aq_true_1min_formula_canary.parquet"
    )
    panel.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "code": ["600000"],
            "trade_time": [pd.Timestamp("2024-01-02 09:31")],
        }
    ).to_parquet(panel, index=False)
    (release / "development_only_release_manifest.json").write_text(
        json.dumps(
            {
                "data_role": "development",
                "forward_2026_present": False,
                "allowed_dates": {
                    "min": "2024-01-02",
                    "max": "2024-01-03",
                },
            }
        ),
        encoding="utf-8",
    )
    hfq = tmp_path / "hfq.parquet"
    pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
            ),
            "code": ["600000"] * 4,
            "is_st": ["否", "否", "是", "否"],
        }
    ).to_parquet(hfq, index=False)
    output = tmp_path / "pit_st"

    result = subject.freeze_pit_st_source(
        release_root=release,
        hfq_paths=[hfq],
        output_root=output,
    )
    frame, receipt = subject.verify_pit_st_source(
        output,
        release=subject._release_manifest(release),
    )

    assert result["status"] == "PIT_HISTORICAL_ST_SOURCE_CLOSED_IMMUTABLE"
    assert frame["date"].min() == pd.Timestamp("2024-01-02")
    assert frame["date"].max() == pd.Timestamp("2024-01-03")
    assert frame["is_st"].tolist() == [False, True]
    assert receipt["st_true_row_count"] == 1


def test_cninfo_raw_keys_map_to_payment_and_effective_sessions() -> None:
    payload = {
        "code": "600001",
        "records": [
            {
                "F006D": "2024-01-02",
                "F010N": 1.0,
                "F011N": 0.5,
                "F012N": 2.0,
                "F018D": "2024-01-03",
                "F020D": "2024-01-04",
                "F023D": "2024-01-05",
                "F025D": "2024-01-05",
            }
        ],
    }

    actions, blockers = subject.parse_dividend_actions(
        [payload],
        date_min="2024-01-01",
        date_max="2024-01-31",
    )

    assert blockers == []
    cash = actions.loc[actions["date"].eq(pd.Timestamp("2024-01-05"))].iloc[0]
    shares = actions.loc[
        actions["date"].eq(pd.Timestamp("2024-01-04"))
    ].iloc[0]
    assert cash["cash_per_share"] == 0.2
    assert cash["share_multiplier"] == 1.0
    assert shares["cash_per_share"] == 0.0
    assert shares["share_multiplier"] == 1.15


def test_missing_relevant_action_date_fails_closed() -> None:
    payload = {
        "code": "000001",
        "records": [
            {
                "F006D": "2024-01-02",
                "F010N": 0,
                "F011N": 0,
                "F012N": 1.0,
                "F020D": "2024-01-04",
                "F023D": None,
            }
        ],
    }

    _, blockers = subject.parse_dividend_actions(
        [payload],
        date_min="2024-01-01",
        date_max="2024-01-31",
    )

    assert blockers == ["cash_action_missing_payment_date:000001"]


def test_restructuring_transfer_is_not_applied_as_pro_rata_dividend() -> None:
    payload = {
        "code": "002217",
        "records": [
            {
                "F006D": "2024-12-25",
                "F010N": None,
                "F011N": 14.0,
                "F012N": None,
                "F018D": "2024-12-30",
                "F020D": None,
                "F023D": None,
                "F025D": None,
                "F044V": "重整转增",
            }
        ],
    }

    actions, blockers = subject.parse_dividend_actions(
        [payload],
        date_min="2024-01-01",
        date_max="2025-07-07",
    )

    assert blockers == []
    assert actions.empty


def test_session_authority_adds_suspensions_and_zero_recovery_terminal() -> None:
    calendar = pd.Series(pd.bdate_range("2023-12-01", "2024-01-05"))
    observed = pd.DataFrame(
        {
            "code": ["600001", "600001", "000001"],
            "date": pd.to_datetime(
                ["2024-01-02", "2024-01-03", "2024-01-05"]
            ),
            "open": [10.0, 10.5, 8.2],
            "high": [10.2, 10.7, 8.3],
            "low": [9.9, 10.4, 8.1],
            "close": [10.1, 10.6, 8.2],
            "is_st": [0, 0, 0],
        }
    )
    master = pd.DataFrame(
        {
            "code": ["600001", "000001"],
            "exchange": ["SSE", "SZSE"],
            "security_type": ["A_SHARE", "A_SHARE"],
            "name": ["A", "B"],
            "listing_date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "delisting_date": [pd.Timestamp("2024-01-05"), pd.NaT],
            "source_status": ["DELISTED", "ACTIVE"],
        }
    )
    actions = pd.DataFrame(
        {
            "code": ["600001"],
            "date": [pd.Timestamp("2024-01-04")],
            "cash_per_share": [0.2],
            "share_multiplier": [1.1],
        }
    )

    result = subject.materialize_session_authority(
        observed=observed,
        security_master=master,
        trade_calendar=calendar,
        actions=actions,
        date_min="2024-01-02",
        date_max="2024-01-05",
    )

    suspended = result.loc[
        result["code"].eq("600001")
        & result["date"].eq(pd.Timestamp("2024-01-04"))
    ].iloc[0]
    terminal = result.loc[
        result["code"].eq("600001")
        & result["date"].eq(pd.Timestamp("2024-01-05"))
    ].iloc[0]
    assert bool(suspended["suspended"]) is True
    assert suspended["corporate_action_cash_per_share"] == 0.2
    assert suspended["corporate_action_share_multiplier"] == 1.1
    assert suspended["up_limit_price"] > 0
    assert bool(terminal["is_delisting"]) is True
    assert bool(terminal["is_terminal_session"]) is True
    assert terminal["terminal_liquidation_price"] == 0.0
    assert result["listing_age_sessions"].min() > 5
    assert not result.duplicated(["date", "code"]).any()
    leading_unknown = result.loc[
        result["code"].eq("000001")
        & result["date"].lt(pd.Timestamp("2024-01-05"))
    ]
    assert leading_unknown["is_st"].all()
    assert result.attrs["leading_unknown_st_blocked_session_count"] == 3


def test_code_namespace_resolves_supported_exchanges() -> None:
    assert subject.normalize_code("600000.XSHG") == "600000"
    assert subject.normalize_code(1.0) == "000001"
    assert subject.exchange_from_code("600000.XSHG") == "SSE"
    assert subject.exchange_from_code("000001.XSHE") == "SZSE"
