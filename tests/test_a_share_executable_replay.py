from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from our_system_phase2.runtime.phase3dy_true1min_tplus1_tradable_replay import (
    _authority_replay_receipts,
    panel_input_manifest_sha256,
)
from our_system_phase2.services.a_share_executable_replay import (
    AShareCorporateActionFractionalSharesError,
    AShareCorporateActionPolicy,
    AShareExecutionPolicy,
    AShareFeeSchedule,
    AShareUniversePolicy,
    ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    run_a_share_long_only_replay,
)
from our_system_phase2.services.a_share_tradability_guard import (
    a_share_tradability_blockers,
    a_share_tradability_ready,
    build_a_share_tradability_receipt,
)


def _fees() -> AShareFeeSchedule:
    return AShareFeeSchedule(
        commission_bps=2.5,
        minimum_commission_cny=5.0,
        exchange_handling_bps=0.341,
        transfer_fee_bps=0.1,
        sell_stamp_duty_bps=5.0,
        effective_start="2023-08-28",
        effective_end="2025-12-31",
        source_reference="frozen_test_account_and_official_fee_sources",
    )


def _universe() -> AShareUniversePolicy:
    return AShareUniversePolicy(
        minimum_listing_sessions=60,
        source_reference="synthetic_pit_survivorship_free_universe",
    )


def _corporate_actions() -> AShareCorporateActionPolicy:
    return AShareCorporateActionPolicy(
        source_reference="synthetic_pit_corporate_action_and_terminal_policy",
    )


def _frame() -> pd.DataFrame:
    dates = pd.date_range("2024-01-02", periods=5, freq="B")
    signals = {
        dates[0]: {"A": 3.0, "B": 2.0, "C": 1.0},
        dates[1]: {"A": 1.0, "B": 3.0, "C": 2.0},
        dates[2]: {"A": 1.0, "B": 2.0, "C": 3.0},
        dates[3]: {"A": 1.0, "B": 2.0, "C": 3.0},
        dates[4]: {"A": 1.0, "B": 2.0, "C": 3.0},
    }
    rows = []
    for date in dates:
        for code, base in {"A": 10.0, "B": 20.0, "C": 30.0}.items():
            open_price = base
            up_limit = base * 1.1
            down_limit = base * 0.9
            if date == dates[1] and code == "A":
                open_price = up_limit
            if date == dates[3] and code == "B":
                open_price = down_limit
            rows.append(
                {
                    "date": date,
                    "code": code,
                    "open": open_price,
                    "close": base * (1.0 + 0.002 * (date.day % 3)),
                    "signal": signals[date][code],
                    "security_type": "A_SHARE",
                    "exchange": "SSE",
                    "universe_eligible": True,
                    "listing_age_sessions": 500,
                    "is_st": False,
                    "is_delisting": False,
                    "suspended": False,
                    "up_limit_price": up_limit,
                    "down_limit_price": down_limit,
                    "corporate_action_cash_per_share": 0.0,
                    "corporate_action_share_multiplier": 1.0,
                    "is_terminal_session": False,
                    "terminal_liquidation_price": float("nan"),
                    "lot_size": 100,
                }
            )
    return pd.DataFrame(rows)


def test_executable_replay_blocks_limit_fills_carries_sell_and_charges_full_fees() -> None:
    replay = run_a_share_long_only_replay(
        _frame(),
        fee_schedule=_fees(),
        universe_policy=_universe(),
        execution_policy=AShareExecutionPolicy(top_quantile=0.2),
        corporate_action_policy=_corporate_actions(),
    )

    fills = replay["fills"]
    assert replay["blocked_buy_count"] == 1
    assert replay["blocked_sell_count"] == 1
    assert replay["total_fees_cny"] > 0
    assert replay["trade_count"] == len(fills) > 0
    assert replay["a_share_mean_one_way_turnover"] > 0
    assert set(replay["proofs"].values()) == {True}
    b_buy_date = fills.loc[
        (fills["code"] == "B") & (fills["side"] == "BUY"), "date"
    ].iloc[0]
    b_sell_date = fills.loc[
        (fills["code"] == "B") & (fills["side"] == "SELL"), "date"
    ].iloc[0]
    assert b_buy_date < b_sell_date


def test_fee_schedule_is_asymmetric_and_minimum_commission_is_applied() -> None:
    fees = _fees()
    buy = fees.fee(1_000.0, side="BUY")
    sell = fees.fee(1_000.0, side="SELL")

    assert buy >= 5.0
    assert sell > buy
    assert sell - buy == pytest.approx(0.5)
    with pytest.raises(ValueError, match="unsupported fee side"):
        fees.fee(1_000.0, side="UNKNOWN")


def test_replay_receipt_binds_reward_and_survives_outer_row_projection() -> None:
    replay = run_a_share_long_only_replay(
        _frame(),
        fee_schedule=_fees(),
        universe_policy=_universe(),
        execution_policy=AShareExecutionPolicy(top_quantile=0.2),
        corporate_action_policy=_corporate_actions(),
    )
    receipt = build_a_share_tradability_receipt(
        candidate_id="candidate-1",
        candidate_exact_identity="exact-1",
        replay_code_sha256="a" * 64,
        input_data_sha256="b" * 64,
        universe_manifest_sha256="c" * 64,
        fee_schedule_sha256=replay["fee_schedule_sha256"],
        execution_policy_sha256=replay["execution_policy_sha256"],
        corporate_action_policy_sha256=replay[
            "corporate_action_policy_sha256"
        ],
        executable_net_reward=replay["a_share_executable_net_reward"],
        train_read_count=15,
        trade_count=replay["trade_count"],
        fill_count=replay["fill_count"],
        blocked_buy_count=replay["blocked_buy_count"],
        blocked_sell_count=replay["blocked_sell_count"],
        extra={
            "a_share_mean_one_way_turnover": replay[
                "a_share_mean_one_way_turnover"
            ]
        },
    )

    assert a_share_tradability_ready(receipt)
    assert not a_share_tradability_blockers(
        {**receipt, "pair_id": "outer-pair-row"},
        expected_candidate_id="candidate-1",
        expected_exact_identity="exact-1",
    )

    tampered = dict(receipt)
    tampered["replay_receipt_canonical_json"] = (
        tampered["replay_receipt_canonical_json"].replace(
            '"candidate-1"', '"candidate-2"'
        )
    )
    assert not a_share_tradability_ready(tampered)
    assert "replay_receipt_payload_sha256_mismatch" in (
        a_share_tradability_blockers(tampered)
    )


def test_replay_fails_closed_without_promotion_grade_universe_columns() -> None:
    frame = _frame().drop(columns=["is_delisting"])
    with pytest.raises(ValueError, match="missing columns"):
        run_a_share_long_only_replay(
            frame,
            fee_schedule=_fees(),
            universe_policy=_universe(),
            execution_policy=AShareExecutionPolicy(top_quantile=0.2),
            corporate_action_policy=_corporate_actions(),
        )


def test_replay_fails_closed_when_fee_schedule_does_not_cover_dates() -> None:
    fees = AShareFeeSchedule(
        **{
            **asdict(_fees()),
            "effective_end": "2023-12-31",
        }
    )
    with pytest.raises(ValueError, match="does not cover replay dates"):
        run_a_share_long_only_replay(
            _frame(),
            fee_schedule=fees,
            universe_policy=_universe(),
            execution_policy=AShareExecutionPolicy(top_quantile=0.2),
            corporate_action_policy=_corporate_actions(),
        )


def test_replay_fails_closed_when_a_held_security_disappears() -> None:
    frame = _frame()
    dates = sorted(frame["date"].unique())
    frame = frame[
        ~(
            frame["date"].eq(dates[3])
            & frame["code"].eq("B")
        )
    ]
    with pytest.raises(ValueError, match="held code missing"):
        run_a_share_long_only_replay(
            frame,
            fee_schedule=_fees(),
            universe_policy=_universe(),
            execution_policy=AShareExecutionPolicy(top_quantile=0.2),
            corporate_action_policy=_corporate_actions(),
        )


def test_fractional_corporate_action_is_typed_candidate_outcome() -> None:
    frame = _frame()
    dates = sorted(frame["date"].unique())
    frame.loc[
        frame["date"].eq(dates[3]) & frame["code"].eq("B"),
        "corporate_action_share_multiplier",
    ] = 1.00001

    with pytest.raises(
        AShareCorporateActionFractionalSharesError
    ) as captured:
        run_a_share_long_only_replay(
            frame,
            fee_schedule=_fees(),
            universe_policy=_universe(),
            execution_policy=AShareExecutionPolicy(top_quantile=0.2),
            corporate_action_policy=_corporate_actions(),
        )

    assert captured.value.code == "B"
    assert captured.value.session_date == str(pd.Timestamp(dates[3]).date())
    assert captured.value.opening_shares > 0
    assert captured.value.adjusted_shares != round(
        captured.value.adjusted_shares
    )


def test_authority_receipt_verifies_actual_panel_and_universe_hashes(
    tmp_path: Path,
) -> None:
    shard_root = tmp_path / "shards"
    panel = (
        shard_root
        / "shard_00000"
        / "phase3aq_wide_true1min"
        / "canary"
        / "phase3aq_true_1min_formula_canary.parquet"
    )
    panel.parent.mkdir(parents=True)
    panel.write_bytes(b"synthetic-panel-binding")
    universe_manifest = tmp_path / "universe.json"
    universe_manifest.write_text(
        json.dumps(
            {
                "schema_version": "synthetic_pit_universe_v1",
                "survivorship_free": True,
                "delisting_history_included": True,
            }
        ),
        encoding="utf-8",
    )
    split = tmp_path / "split.csv"
    dates = pd.date_range("2024-01-02", periods=5, freq="B")
    pd.DataFrame(
        {"trade_date": dates, "split": ["train"] * len(dates)}
    ).to_csv(split, index=False)
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": "candidate-1",
                "candidate_exact_identity": "exact-1",
            }
        ]
    )
    daily = _frame().rename(columns={"signal": "signal__candidate-1"})
    contract = {
        "schema_version": (
            "a_share_tradability_replay_qualification_contract_v1"
        ),
        "input_data_sha256": panel_input_manifest_sha256(
            [panel],
            shard_root=shard_root,
        ),
        "universe_manifest": str(universe_manifest),
        "universe_manifest_sha256": hashlib.sha256(
            universe_manifest.read_bytes()
        ).hexdigest(),
        "split_manifest": str(split),
        "fee_schedule": asdict(_fees()),
        "universe_policy": asdict(_universe()),
        "execution_policy": asdict(
            AShareExecutionPolicy(top_quantile=0.2)
        ),
        "corporate_action_policy": asdict(_corporate_actions()),
        "candidate_exact_identities": {"candidate-1": "exact-1"},
    }
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    receipts, diagnostics = _authority_replay_receipts(
        daily=daily,
        candidates=candidates,
        contract_path=contract_path,
        panel_paths=[panel],
        shard_root=shard_root,
    )

    assert len(receipts) == 1
    assert diagnostics[0]["decision"] == "A_SHARE_TRADABILITY_READY"
    assert a_share_tradability_ready(receipts[0])

    contract["input_data_sha256"] = "0" * 64
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="panels actually read"):
        _authority_replay_receipts(
            daily=daily,
            candidates=candidates,
            contract_path=contract_path,
            panel_paths=[panel],
            shard_root=shard_root,
        )


def test_corporate_actions_apply_only_to_opening_holdings_and_end_flat() -> None:
    frame = _frame()
    dates = sorted(frame["date"].unique())
    action = frame["date"].eq(dates[3]) & frame["code"].eq("B")
    frame.loc[action, "corporate_action_cash_per_share"] = 0.2
    frame.loc[action, "corporate_action_share_multiplier"] = 1.1

    replay = run_a_share_long_only_replay(
        frame,
        fee_schedule=_fees(),
        universe_policy=_universe(),
        execution_policy=AShareExecutionPolicy(top_quantile=0.2),
        corporate_action_policy=_corporate_actions(),
    )

    assert replay["corporate_action_cash_cny"] > 0
    assert replay["corporate_action_share_delta"] > 0
    assert replay["ending_holding_count"] == 0


def test_final_close_mark_to_market_skips_fabricated_terminal_sale() -> None:
    frame = _frame()
    replay = run_a_share_long_only_replay(
        frame,
        fee_schedule=_fees(),
        universe_policy=_universe(),
        execution_policy=AShareExecutionPolicy(top_quantile=0.2),
        corporate_action_policy=_corporate_actions(),
        ending_book_policy=ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    )

    final_date = pd.Timestamp(frame["date"].max()).date().isoformat()
    assert replay["ending_book_policy"] == (
        ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET
    )
    assert replay["final_close_mark_to_market_diagnostic"] is True
    assert replay["ending_holding_count"] > 0
    assert replay["ending_holdings_market_value_cny"] > 0
    assert 0 < replay["ending_holdings_weight"] <= 1
    assert replay["fills"]["date"].ne(final_date).all()
    assert replay["proofs"]["terminal_liquidation_enforced"] is False
    assert (
        replay["proofs"]["final_close_mark_to_market_enforced"] is True
    )


def test_delisting_terminal_liquidation_is_explicit_and_fee_charged() -> None:
    frame = _frame()
    dates = sorted(frame["date"].unique())
    terminal = frame["date"].eq(dates[3]) & frame["code"].eq("B")
    frame.loc[terminal, "is_delisting"] = True
    frame.loc[terminal, "is_terminal_session"] = True
    frame.loc[terminal, "terminal_liquidation_price"] = 18.0
    frame = frame[
        ~(frame["date"].eq(dates[4]) & frame["code"].eq("B"))
    ].copy()

    replay = run_a_share_long_only_replay(
        frame,
        fee_schedule=_fees(),
        universe_policy=_universe(),
        execution_policy=AShareExecutionPolicy(top_quantile=0.2),
        corporate_action_policy=_corporate_actions(),
    )

    terminal_fills = replay["fills"].loc[
        replay["fills"]["fill_reason"].eq(
            "DELISTING_TERMINAL_LIQUIDATION"
        )
    ]
    assert replay["terminal_liquidation_count"] == 1
    assert len(terminal_fills) == 1
    assert terminal_fills.iloc[0]["fee"] > 0


def test_delisting_zero_recovery_is_explicit_without_fabricated_price() -> None:
    frame = _frame()
    dates = sorted(frame["date"].unique())
    terminal = frame["date"].eq(dates[3]) & frame["code"].eq("B")
    frame.loc[terminal, "is_delisting"] = True
    frame.loc[terminal, "is_terminal_session"] = True
    frame.loc[terminal, "terminal_liquidation_price"] = 0.0
    frame = frame[
        ~(frame["date"].eq(dates[4]) & frame["code"].eq("B"))
    ].copy()

    replay = run_a_share_long_only_replay(
        frame,
        fee_schedule=_fees(),
        universe_policy=_universe(),
        execution_policy=AShareExecutionPolicy(top_quantile=0.2),
        corporate_action_policy=_corporate_actions(),
    )

    terminal_fills = replay["fills"].loc[
        replay["fills"]["fill_reason"].eq(
            "DELISTING_TERMINAL_LIQUIDATION"
        )
    ]
    assert replay["terminal_liquidation_count"] == 1
    assert len(terminal_fills) == 1
    assert terminal_fills.iloc[0]["price"] == 0.0
    assert terminal_fills.iloc[0]["notional"] == 0.0
    assert terminal_fills.iloc[0]["fee"] == 0.0
    assert replay["ending_holding_count"] == 0
