from __future__ import annotations

import pandas as pd
import pytest

from scripts import run_cn_finalist_replay_then_oos as subject


def _candidate_rows() -> pd.DataFrame:
    rows = []
    for rank in range(1, 25):
        pair_id = f"pair-{rank:02d}"
        for role in ("CONTROL", "PRIMARY"):
            candidate_id = f"{pair_id}-{role.lower()}"
            rows.append(
                {
                    "candidate_id": candidate_id,
                    "pair_id": pair_id,
                    "pair_member_role": role,
                    "route_id": "SLOW_TEMPORAL_CHANGE",
                    "expression": "CSRank($x)",
                    "exact_identity": f"exact-{candidate_id}",
                }
            )
    return pd.DataFrame(rows)


def _pair_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "pair_id": f"pair-{rank:02d}",
                "primary_candidate_id": f"pair-{rank:02d}-primary",
                "control_candidate_id": f"pair-{rank:02d}-control",
                "keep_review_rank": rank,
            }
            for rank in range(1, 25)
        ]
    )


def test_ordered_candidates_puts_primary_before_control() -> None:
    ordered = subject._ordered_candidates(_candidate_rows(), _pair_rows())

    assert len(ordered) == 48
    assert ordered.iloc[0]["candidate_id"] == "pair-01-primary"
    assert ordered.iloc[1]["candidate_id"] == "pair-01-control"
    assert ordered["pair_member_role"].tolist()[:4] == [
        "PRIMARY",
        "CONTROL",
        "PRIMARY",
        "CONTROL",
    ]


def test_expected_cohort_size_can_expand_to_64_pairs() -> None:
    original_pairs = subject.EXPECTED_PAIR_COUNT
    try:
        subject._configure_expected_cohort_size(64)
        assert subject.EXPECTED_PAIR_COUNT == 64
        assert subject.EXPECTED_MEMBER_COUNT == 128
    finally:
        subject._configure_expected_cohort_size(original_pairs)


def test_ordered_candidates_rejects_exact_identity_collision() -> None:
    candidates = _candidate_rows()
    candidates.loc[1, "exact_identity"] = candidates.loc[0, "exact_identity"]

    with pytest.raises(RuntimeError, match="exact identities"):
        subject._ordered_candidates(candidates, _pair_rows())


def test_materialize_replay_master_marks_suspension_at_last_close() -> None:
    fields = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-02", "2025-01-06"]),
            "trade_time": pd.to_datetime(
                ["2025-01-02 15:00", "2025-01-06 15:00"]
            ),
            "code": ["000001", "000001"],
            "open": [10.0, 11.0],
            "close": [10.5, 11.5],
            "x": [1.0, 2.0],
        }
    )
    authority = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2025-01-02", "2025-01-03", "2025-01-06"]
            ),
            "code": ["000001", "000001", "000001"],
            "security_type": ["A_SHARE"] * 3,
            "exchange": ["SZSE"] * 3,
            "universe_eligible": [True] * 3,
            "listing_age_sessions": [100, 101, 102],
            "is_st": [False] * 3,
            "is_delisting": [False] * 3,
            "suspended": [False, True, False],
            "up_limit_price": [11.0, 11.55, 12.1],
            "down_limit_price": [9.0, 9.45, 9.9],
            "corporate_action_cash_per_share": [0.0] * 3,
            "corporate_action_share_multiplier": [1.0] * 3,
            "is_terminal_session": [False] * 3,
            "terminal_liquidation_price": [0.0] * 3,
        }
    )

    master, observed, all_sessions = subject._materialize_replay_master(
        fields,
        authority,
    )

    assert len(observed) == 2
    assert len(all_sessions) == 3
    suspended = master.loc[master["suspended"]].iloc[0]
    assert suspended["open"] == 10.5
    assert suspended["close"] == 10.5


def test_oos_rows_keep_every_pair_without_interstage_filter() -> None:
    candidates = _candidate_rows()
    result = {
        "candidate_rewards": [
            {
                "candidate_id": f"pair-{rank:02d}-primary",
                "validation_report_metric": 0.2,
                "validation_day_sortino": 0.3,
            }
            for rank in range(1, 25)
        ],
        "pair_results": [
            {
                "pair_id": f"pair-{rank:02d}",
                "pair_evaluation_status": (
                    "PAIR_EVALUATED" if rank != 24 else "SUPPORT_BLOCKED"
                ),
                "pair_evaluation_blockers": (
                    "" if rank != 24 else "insufficient_support"
                ),
                "pair_support_count": 100,
                "pair_validation_report_metric": 0.1,
                "control_validation_report_metric": -0.1,
            }
            for rank in range(1, 25)
        ],
    }
    replay = pd.DataFrame(
        [
            {
                "pair_id": f"pair-{rank:02d}",
                "a_share_replay_status": (
                    "PAIR_REPLAY_COMPLETE"
                    if rank != 24
                    else "PAIR_REPLAY_BLOCKED"
                ),
                "primary_a_share_executable_net_reward": (
                    0.4 if rank != 24 else None
                ),
                "control_a_share_executable_net_reward": (
                    0.1 if rank != 24 else None
                ),
                "a_share_executable_net_increment": (
                    0.3 if rank != 24 else None
                ),
            }
            for rank in range(1, 25)
        ]
    )

    rows = subject._oos_pair_rows(candidates, result, replay)

    assert len(rows) == 24
    assert rows["interstage_filter_applied"].eq(False).all()
    assert rows["oos_positive_transfer"].sum() == 23
    assert rows.iloc[-1]["validation_pair_status"] == "SUPPORT_BLOCKED"
    assert rows.iloc[-1]["a_share_replay_status"] == "PAIR_REPLAY_BLOCKED"
    assert pd.isna(
        rows.iloc[-1]["primary_a_share_executable_net_reward"]
    )


def test_pair_replay_keeps_terminal_liquidity_blocked_pair() -> None:
    candidates = _candidate_rows()
    candidate_rows = []
    for candidate in candidates.to_dict(orient="records"):
        blocked = candidate["candidate_id"] == "pair-01-primary"
        candidate_rows.append(
            {
                **candidate,
                "candidate_replay_status": (
                    "CANDIDATE_REPLAY_BLOCKED"
                    if blocked
                    else "CANDIDATE_REPLAY_COMPLETE"
                ),
                "a_share_executable_net_reward": None if blocked else 0.2,
                "trade_count": None if blocked else 5,
                "blocked_buy_count": None if blocked else 0,
                "blocked_sell_count": None if blocked else 0,
                "blocker_code": (
                    "FINAL_SESSION_UNLIQUIDATED_HOLDINGS"
                    if blocked
                    else None
                ),
            }
        )

    pairs = subject._replay_pair_results(candidates, candidate_rows)

    assert len(pairs) == 24
    assert pairs.iloc[0]["a_share_replay_status"] == "PAIR_REPLAY_BLOCKED"
    assert pd.isna(
        pairs.iloc[0]["primary_a_share_executable_net_reward"]
    )
    assert pairs.iloc[1:]["a_share_replay_status"].eq(
        "PAIR_REPLAY_COMPLETE"
    ).all()


def test_only_no_fill_receipt_is_candidate_economic_blocker() -> None:
    assert subject._candidate_economic_blocker_code(
        ["replay_has_no_executable_fills"]
    ) == "NO_EXECUTABLE_FILLS"
    assert (
        subject._candidate_economic_blocker_code(
            [
                "replay_has_no_executable_fills",
                "replay_candidate_id_mismatch",
            ]
        )
        is None
    )
