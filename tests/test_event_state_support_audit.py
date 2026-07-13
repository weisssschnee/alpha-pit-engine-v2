from __future__ import annotations

import pandas as pd

from our_system_phase2.services.event_state_support_audit import (
    EventSupportThresholds,
    StateSupportThresholds,
    audit_event_state_support,
)


def _frame(*, active_codes_per_time: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day in range(3):
        for code in range(12):
            for minute in range(12):
                trade_time = pd.Timestamp("2025-04-01") + pd.Timedelta(days=day, hours=9, minutes=31 + minute)
                active = code < active_codes_per_time and minute in {3, 4, 8}
                state = -1.0 if ((minute + code) // 2) % 2 == 0 else 1.0
                close = 10.0 + code * 0.1 + minute * 0.01
                rows.append(
                    {
                        "code": f"{day:02d}{code:04d}",
                        "trade_time": trade_time,
                        "evt_uplimit_active": float(active),
                        "evt_uplimit_type_code": float(code % 3),
                        "intraday_ret_from_open": state,
                        "ret_1m": -state,
                        "close": close,
                        "high": close + (0.08 if code % 2 else 0.04),
                        "low": close - (0.04 if code % 3 else 0.09),
                    }
                )
    return pd.DataFrame(rows)


def test_support_audit_separates_competitive_event_support_from_sparse_event_support() -> None:
    thresholds = EventSupportThresholds(
        min_active_rows=10,
        min_active_dates=3,
        min_active_symbols=3,
        min_enter_transitions=3,
        min_exit_transitions=3,
        min_episode_count=3,
        min_activation_rate=0.00001,
        max_activation_rate=0.5,
        min_competitive_cross_sections=3,
        min_competitive_active_row_share=0.5,
    )
    state_thresholds = StateSupportThresholds(
        min_valid_rows=100,
        min_dates=3,
        min_symbols=10,
        min_categories=2,
        min_transitions=10,
        min_entropy_bits=0.1,
        max_category_share=0.99,
    )

    operational = audit_event_state_support(
        [_frame(active_codes_per_time=4)],
        event_thresholds=thresholds,
        state_thresholds=state_thresholds,
    )
    sparse = audit_event_state_support(
        [_frame(active_codes_per_time=1)],
        event_thresholds=thresholds,
        state_thresholds=state_thresholds,
    )

    assert operational["event"]["decision"] == "EVENT_SUPPORT_OPERATIONAL"
    assert sparse["event"]["decision"] == "EVENT_SUPPORT_NOT_OPERATIONAL"
    assert sparse["event"]["checks"]["competitive_cross_sections"] is False
    assert operational["state_system_decision"] == "STATE_GENERATOR_OPERATIONAL"
    assert operational["performance_columns_used"] == []


def test_support_audit_rejects_codes_split_across_chunks() -> None:
    frame = _frame(active_codes_per_time=2)
    try:
        audit_event_state_support([frame.iloc[:100], frame.iloc[100:]])
    except ValueError as exc:
        assert "codes span multiple audit chunks" in str(exc)
    else:
        raise AssertionError("audit accepted a code split across chunks")
