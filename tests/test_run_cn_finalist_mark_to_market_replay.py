from __future__ import annotations

import json

import pandas as pd
import pytest

from scripts import run_cn_finalist_mark_to_market_replay as subject


def _candidates() -> pd.DataFrame:
    rows = []
    for rank in range(1, 25):
        for role in ("PRIMARY", "CONTROL"):
            candidate_id = f"candidate-{rank:02d}-{role.lower()}"
            rows.append(
                {
                    "candidate_id": candidate_id,
                    "pair_id": f"pair-{rank:02d}",
                    "pair_member_role": role,
                    "route_id": "SLOW_TEMPORAL_CHANGE",
                    "exact_identity": f"exact-{candidate_id}",
                }
            )
    return pd.DataFrame(rows)


def test_mark_to_market_binds_size_from_immutable_freeze(tmp_path) -> None:
    base = subject.base
    original_pairs = base.EXPECTED_PAIR_COUNT
    freeze = {
        "status": "FROZEN_UNCHANGED_FIXED_COHORT",
        "pair_count": 32,
        "candidate_member_count": 64,
    }
    freeze["manifest_body_sha256"] = base._stable_hash(freeze)
    path = tmp_path / "freeze.json"
    path.write_text(json.dumps(freeze), encoding="utf-8")

    try:
        loaded = subject._load_freeze_for_mark_to_market(path)
        assert loaded["pair_count"] == 32
        assert base.EXPECTED_PAIR_COUNT == 32
        assert base.EXPECTED_MEMBER_COUNT == 64
    finally:
        base._configure_expected_cohort_size(original_pairs)


def test_strict_replay_closure_is_bound_below_campaign_root() -> None:
    root = subject.Path("campaign")
    assert subject._strict_replay_closure_path(root).parts[-2:] == (
        "replay",
        "REPLAY_COMPLETE.json",
    )


def test_pair_results_include_cumulative_returns_and_diagnostic_no_fill() -> None:
    candidates = _candidates()
    candidate_rows = []
    for row in candidates.to_dict(orient="records"):
        no_fill = row["candidate_id"] == "candidate-01-control"
        cumulative_return = 0.0 if no_fill else 0.05
        candidate_rows.append(
            {
                **row,
                "candidate_mark_to_market_status": subject.CANDIDATE_COMPLETE,
                "mark_to_market_net_reward": 0.0 if no_fill else 0.4,
                "initial_cash_cny": 1_000_000.0,
                "ending_nav_cny": 1_000_000.0 * (1 + cumulative_return),
                "cumulative_net_return": cumulative_return,
                "ending_holdings_weight": 0.8,
                "a_share_mean_one_way_turnover": 0.02,
                "diagnostic_code": "NO_EXECUTABLE_FILLS" if no_fill else None,
                "blocker_code": None,
            }
        )

    pairs = subject._pair_results(candidates, candidate_rows)

    assert len(pairs) == 24
    assert pairs["pair_mark_to_market_status"].eq(
        subject.PAIR_COMPLETE
    ).all()
    assert pairs.iloc[1:]["mark_to_market_net_increment"].eq(0.0).all()
    assert pairs.iloc[0]["primary_cumulative_net_return"] == 0.05
    assert pairs.iloc[0]["control_cumulative_net_return"] == 0.0
    assert pairs.iloc[0]["cumulative_net_return_increment"] == 0.05


def test_candidate_receipt_never_claims_execution_or_promotion() -> None:
    candidate = _candidates().iloc[0].to_dict()
    result = {
        "a_share_executable_net_reward": 0.7,
        "trade_count": 4,
        "fill_count": 4,
        "blocked_buy_count": 1,
        "blocked_sell_count": 2,
        "total_fees_cny": 25.0,
        "ending_nav_cny": 1_050_000.0,
        "ending_cash_cny": 50_000.0,
        "ending_holding_count": 2,
        "ending_holdings": [
            {
                "code": "000001",
                "shares": 100,
                "final_pit_close": 10.0,
                "market_value_cny": 1_000.0,
            }
        ],
        "ending_holdings_market_value_cny": 1_000_000.0,
        "ending_holdings_weight": 0.95238,
        "execution_policy": {"initial_cash_cny": 1_000_000.0},
        "a_share_mean_one_way_turnover": 0.02,
        "accounting_ledger_version": "a_share_lot_cash_pnl_ledger_v1",
        "accounting_ledger_contract": {
            "lot_disposal_method": "FIFO",
        },
        "accounting_invariants": {
            "status": "PASS",
            "maximum_cash_identity_error_cny": 0.0,
            "maximum_nav_identity_error_cny": 0.0,
            "maximum_pnl_identity_error_cny": 0.0,
            "maximum_lot_quantity_error": 0,
        },
        "cumulative_net_pnl_cny": 50_000.0,
        "cumulative_realized_trade_pnl_cny": 10_000.0,
        "cumulative_corporate_action_cash_pnl_cny": 0.0,
        "ending_unrealized_pnl_cny": 40_000.0,
        "share_weighted_average_position_age_sessions": 1.5,
        "maximum_position_age_sessions": 3,
        "ending_lots": [],
        "fee_schedule_sha256": "a" * 64,
        "execution_policy_sha256": "b" * 64,
        "corporate_action_policy_sha256": "c" * 64,
        "proofs": {
            "terminal_liquidation_enforced": False,
            "final_close_mark_to_market_enforced": True,
        },
    }

    receipt = subject._candidate_receipt(
        candidate=candidate,
        result=result,
        input_data_sha256="d" * 64,
        source_code_sha256="e" * 64,
        train_read_count=100,
        selection_payload_sha256="f" * 64,
        strict_replay_closure_sha256="1" * 64,
        blocked=False,
    )

    assert receipt["ending_book_policy"] == (
        "FINAL_CLOSE_MARK_TO_MARKET"
    )
    assert receipt["no_fabricated_terminal_sale"] is True
    assert receipt["terminal_sale_fee_applied"] is False
    assert receipt["initial_cash_cny"] == 1_000_000.0
    assert receipt["cumulative_net_return"] == pytest.approx(0.05)
    assert receipt["train_economic_claim_authorized"] is True
    assert receipt["validation_reads"] == 0
    assert receipt["economic_claim_authorized"] is False
    assert receipt["promotion_authorized"] is False
    assert receipt["successor_search_authorized"] is False
