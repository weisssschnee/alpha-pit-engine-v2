from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.services.event_state_system import (
    EVENT_STATE_SPECS,
    EventStateConfig,
    build_event_state_features,
    event_state_contract,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": ["A"] * 7,
            "trade_time": pd.date_range("2024-01-02 09:31", periods=7, freq="min"),
            "date": ["2024-01-02"] * 7,
            "high": [9.8, 10.0, 10.0, 10.0, 10.0, 9.9, 10.0],
            "low": [9.7, 9.9, 9.8, 9.9, 9.9, 9.7, 9.9],
            "close": [9.8, 10.0, 9.9, 10.0, 10.0, 9.8, 10.0],
            "up_limit_price": [10.0] * 7,
            "down_limit_price": [8.0] * 7,
            "m1_first30_last_return_vs_open": [0.01] * 7,
            "daily_context": [1.0] * 7,
            "daily_context_observed_at": pd.to_datetime(["2024-01-02 09:30"] * 6 + ["2024-01-02 10:00"]),
        }
    )


def test_event_contract_covers_required_system_capabilities() -> None:
    required = {
        "limit_up_touch", "limit_down_touch", "seal", "break_board", "reseal",
        "event_age", "event_count", "event_intensity", "state_duration",
        "pre_event_path", "post_event_path", "continuation_reversal", "firstN_path",
        "daily_context_lag", "state_transition", "cross_asset_plate_confirmation",
    }
    contract = event_state_contract()

    assert required == set(EVENT_STATE_SPECS)
    assert contract["winner_or_performance_directed"] is False
    assert all(row["observable_time"] and row["maturity"] for row in contract["features"])


def test_seal_break_and_reseal_are_detected_without_future_rows() -> None:
    result, manifest = build_event_state_features(
        _frame(), config=EventStateConfig(event_count_window=2, pre_path_window=2, post_path_window=1)
    )

    assert result.loc[1, "event_seal"] == 1.0
    assert result.loc[2, "event_break_board"] == 1.0
    assert result.loc[3, "event_reseal"] == 1.0
    assert result.loc[3, "state_duration"] == 1.0
    assert manifest["post_event_observable_delay_bars"] == 1


def test_post_event_path_only_appears_after_maturity() -> None:
    result, _ = build_event_state_features(
        _frame(), config=EventStateConfig(event_count_window=2, pre_path_window=2, post_path_window=1)
    )

    assert np.isnan(result.loc[1, "event_post_window_mean"])
    assert result.loc[2, "event_post_window_mean"] == pytest.approx((10.0 + 9.9) / 2)


def test_daily_context_is_hidden_until_observed() -> None:
    result, _ = build_event_state_features(
        _frame(), config=EventStateConfig(event_count_window=2, pre_path_window=2, post_path_window=1)
    )

    assert result["event_daily_context_lagged"].iloc[:-1].notna().all()
    assert np.isnan(result["event_daily_context_lagged"].iloc[-1])


def test_group_confirmation_requires_versioned_nonplaceholder_pit_rows() -> None:
    group = pd.DataFrame(
        {
            "code": ["A"] * 7,
            "trade_time": _frame()["trade_time"],
            "group_id": ["P1"] * 7,
            "source_version": ["v1"] * 7,
            "peer_value_mean": [0.2] * 7,
        }
    )
    result, manifest = build_event_state_features(
        _frame(),
        config=EventStateConfig(event_count_window=2, pre_path_window=2, post_path_window=1),
        group_confirmation=group,
    )

    assert manifest["pit_group_confirmation_attached"] is True
    assert result.loc[1, "event_cross_asset_confirmation"] == pytest.approx(0.2)
    group.loc[0, "group_id"] = "0"
    with pytest.raises(ValueError, match="placeholder"):
        build_event_state_features(_frame(), group_confirmation=group)


def test_overlap_policy_rejects_contradictory_limit_inputs() -> None:
    frame = _frame()
    frame["down_limit_price"] = frame["up_limit_price"]
    with pytest.raises(ValueError, match="overlapping"):
        build_event_state_features(frame, config=EventStateConfig(overlap_policy="reject"))
