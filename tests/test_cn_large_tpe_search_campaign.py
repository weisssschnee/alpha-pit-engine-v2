from __future__ import annotations

from collections import Counter

from our_system_phase2.runtime.cn_large_tpe_search_campaign import (
    ASKS_PER_CHECKPOINT,
    MINIMUM_ACTUAL_EVALUATED_PAIRS,
    ROUTE_EVALUATED_TARGETS,
    ROUTES,
    _allocate_checkpoint_asks,
)


def test_large_contract_is_five_digit_actual_evaluated_not_scheduled() -> None:
    assert MINIMUM_ACTUAL_EVALUATED_PAIRS == 20_000
    assert sum(ROUTE_EVALUATED_TARGETS.values()) == 20_000
    assert set(ROUTE_EVALUATED_TARGETS) == set(ROUTES)
    assert "MINUTE_STATIC" not in ROUTES
    assert "INTRADAY_STATE_TRANSITION" not in ROUTES


def test_checkpoint_ask_allocation_is_registry_route_bounded() -> None:
    allocation = _allocate_checkpoint_asks(
        evaluated_by_route=Counter(),
        asked_by_route=Counter(),
    )

    assert set(allocation) == set(ROUTES)
    assert sum(allocation.values()) == ASKS_PER_CHECKPOINT
    assert all(value >= 8 for value in allocation.values())
    assert allocation["SLOW_TEMPORAL_CHANGE"] == max(
        allocation.values()
    )


def test_completed_route_receives_no_further_asks() -> None:
    completed = Counter(
        {
            "DISCLOSURE_EVENT": ROUTE_EVALUATED_TARGETS[
                "DISCLOSURE_EVENT"
            ]
        }
    )
    allocation = _allocate_checkpoint_asks(
        evaluated_by_route=completed,
        asked_by_route=Counter(),
    )

    assert "DISCLOSURE_EVENT" not in allocation
    assert sum(allocation.values()) == ASKS_PER_CHECKPOINT


def test_observed_low_yield_increases_route_ask_share() -> None:
    evaluated = Counter(
        {
            route_id: 100 for route_id in ROUTES
        }
    )
    asked = Counter(
        {
            route_id: 200 for route_id in ROUTES
        }
    )
    asked["FIRSTN_PATH"] = 500

    allocation = _allocate_checkpoint_asks(
        evaluated_by_route=evaluated,
        asked_by_route=asked,
    )

    assert allocation["FIRSTN_PATH"] > 8
