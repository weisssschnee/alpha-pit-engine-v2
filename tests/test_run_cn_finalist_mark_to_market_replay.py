from __future__ import annotations

import pandas as pd

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


def test_pair_results_keep_no_fill_as_the_only_blocked_pair() -> None:
    candidates = _candidates()
    candidate_rows = []
    for row in candidates.to_dict(orient="records"):
        blocked = row["candidate_id"] == "candidate-01-control"
        candidate_rows.append(
            {
                **row,
                "candidate_mark_to_market_status": (
                    subject.CANDIDATE_BLOCKED
                    if blocked
                    else subject.CANDIDATE_COMPLETE
                ),
                "mark_to_market_net_reward": None if blocked else 0.4,
                "ending_holdings_weight": 0.8,
                "a_share_mean_one_way_turnover": 0.02,
                "blocker_code": (
                    "NO_EXECUTABLE_FILLS" if blocked else None
                ),
            }
        )

    pairs = subject._pair_results(candidates, candidate_rows)

    assert len(pairs) == 24
    assert pairs.iloc[0]["pair_mark_to_market_status"] == (
        subject.PAIR_BLOCKED
    )
    assert pairs.iloc[0]["control_blocker_code"] == (
        "NO_EXECUTABLE_FILLS"
    )
    assert pairs.iloc[1:]["pair_mark_to_market_status"].eq(
        subject.PAIR_COMPLETE
    ).all()
    assert pairs.iloc[1:]["mark_to_market_net_increment"].eq(0.0).all()


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
        "a_share_mean_one_way_turnover": 0.02,
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
    assert receipt["validation_reads"] == 0
    assert receipt["economic_claim_authorized"] is False
    assert receipt["promotion_authorized"] is False
    assert receipt["successor_search_authorized"] is False
