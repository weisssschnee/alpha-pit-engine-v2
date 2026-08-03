from __future__ import annotations

from our_system_phase2.services.finalist_economic_admission import (
    mark_to_market_blockers,
    mark_to_market_eligible,
    strict_replay_eligible,
)


def test_strict_replay_requires_absolute_and_relative_positive() -> None:
    base = {
        "a_share_replay_status": "PAIR_REPLAY_COMPLETE",
        "primary_a_share_executable_net_reward": 0.2,
        "a_share_executable_net_increment": 0.1,
    }
    assert strict_replay_eligible(base)
    assert not strict_replay_eligible(
        {**base, "primary_a_share_executable_net_reward": -0.01}
    )
    assert not strict_replay_eligible(
        {**base, "a_share_executable_net_increment": -0.01}
    )


def test_mtm_requires_both_terminal_weights_within_five_percent() -> None:
    base = {
        "pair_mark_to_market_status": "PAIR_MARK_TO_MARKET_COMPLETE",
        "primary_mark_to_market_net_reward": 0.2,
        "mark_to_market_net_increment": 0.1,
        "primary_ending_holdings_weight": 0.05,
        "control_ending_holdings_weight": 0.01,
    }
    assert mark_to_market_eligible(base)
    blocked = {
        **base,
        "control_ending_holdings_weight": 0.0500001,
    }
    assert not mark_to_market_eligible(blocked)
    assert "CONTROL_TERMINAL_HOLDINGS_WEIGHT_EXCEEDED" in (
        mark_to_market_blockers(blocked)
    )


def test_relative_winner_with_negative_primary_is_not_admitted() -> None:
    row = {
        "pair_mark_to_market_status": "PAIR_MARK_TO_MARKET_COMPLETE",
        "primary_mark_to_market_net_reward": -0.2,
        "mark_to_market_net_increment": 0.4,
        "primary_ending_holdings_weight": 0.001,
        "control_ending_holdings_weight": 0.001,
    }
    assert not mark_to_market_eligible(row)
