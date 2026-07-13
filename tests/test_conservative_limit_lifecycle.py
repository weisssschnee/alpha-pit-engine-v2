from __future__ import annotations

import pandas as pd

from our_system_phase2.services.conservative_limit_lifecycle import (
    LimitLifecycleConfig,
    materialize_conservative_limit_lifecycle,
)


def _exact_frame() -> pd.DataFrame:
    times = pd.date_range("2025-04-01 09:31", periods=6, freq="min")
    return pd.DataFrame(
        {
            "code": ["600000"] * 6,
            "trade_time": times,
            "open": [9.8, 9.9, 10.0, 10.0, 9.9, 10.0],
            "high": [9.9, 10.0, 10.0, 10.0, 10.0, 10.0],
            "low": [9.7, 9.8, 10.0, 9.9, 9.8, 10.0],
            "close": [9.8, 9.9, 10.0, 10.0, 9.9, 10.0],
            "up_limit_price": [10.0] * 6,
            "down_limit_price": [8.0] * 6,
        }
    )


def test_conservative_lifecycle_only_claims_between_bar_transitions() -> None:
    features, episodes, manifest = materialize_conservative_limit_lifecycle(_exact_frame())
    assert bool(features.loc[1, "UP_TOUCHED_LIMIT"])
    assert bool(features.loc[2, "UP_CLOSED_AT_LIMIT_ENTRY"])
    assert bool(features.loc[4, "UP_LEFT_LIMIT_BETWEEN_BARS"])
    assert bool(features.loc[5, "UP_RESEALED_BETWEEN_BARS"])
    assert bool(features.loc[1, "UP_INTRABAR_ORDER_AMBIGUOUS"])
    assert episodes.shape[0] == 1
    assert episodes.loc[0, "admission_vote"] == 1
    assert manifest["one_episode_one_admission_vote"] is True
    assert manifest["intrabar_ambiguity_negative_control_allowed"] is False
    assert "true order-book seal" in manifest["claims_not_made"]


def test_intrabar_ambiguity_is_unknown_not_a_negative_event() -> None:
    features, _, _ = materialize_conservative_limit_lifecycle(_exact_frame())
    ambiguous = features["UP_INTRABAR_ORDER_AMBIGUOUS"].fillna(False)
    assert ambiguous.any()
    assert not features.loc[ambiguous, "UP_CLOSED_AT_LIMIT"].all()


def test_rule_derivation_excludes_initial_observed_sessions_and_uses_pit_st_context() -> None:
    rows = []
    for day in pd.date_range("2025-01-02", periods=7, freq="B"):
        close = 10.0
        rows.append(
            {
                "code": "600001",
                "trade_time": day + pd.Timedelta(hours=9, minutes=31),
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "ctx_hfq_is_st": 0.0,
            }
        )
    features, _, manifest = materialize_conservative_limit_lifecycle(
        pd.DataFrame(rows), config=LimitLifecycleConfig(conservative_listing_exclusion_sessions=5)
    )
    sessions = features.drop_duplicates(["code", "session"])
    assert sessions.iloc[:5]["limit_rule_eligible"].eq(False).all()
    assert sessions.iloc[5:]["limit_price_source"].eq("CONSERVATIVE_RULE_DERIVATION").all()
    assert manifest["derived_limit_session_count"] == 2


def test_missing_st_context_only_allows_vendor_confirmed_up_path_after_observation() -> None:
    rows = []
    sessions = pd.date_range("2025-01-02", periods=7, freq="B")
    closes = [10.0] * 6 + [11.0]
    for day_index, day in enumerate(sessions):
        for minute in range(3):
            active = day_index == 6 and minute >= 1
            price = 11.0 if active else closes[day_index]
            rows.append(
                {
                    "code": "600002",
                    "trade_time": day + pd.Timedelta(hours=9, minutes=31 + minute),
                    "open": price,
                    "high": price,
                    "low": price - (0.1 if active else 0.0),
                    "close": price,
                    "ctx_hfq_is_st": float("nan"),
                    "evt_uplimit_active": float(active),
                }
            )
    features, episodes, manifest = materialize_conservative_limit_lifecycle(pd.DataFrame(rows))
    final = features.loc[features["session"].eq(sessions[-1].normalize())].reset_index(drop=True)
    assert final["up_limit_price_source"].eq("VENDOR_CONFIRMED_RULE_DERIVATION").all()
    assert pd.isna(final.loc[0, "UP_TOUCHED_LIMIT"])
    assert bool(final.loc[1, "UP_TOUCHED_LIMIT"])
    assert final["DOWN_TOUCHED_LIMIT"].isna().all()
    assert len(episodes) == 1
    assert manifest["vendor_confirmed_limit_session_count"] == 1


def test_missing_st_without_vendor_confirmation_remains_unknown() -> None:
    rows = []
    for day in pd.date_range("2025-01-02", periods=7, freq="B"):
        rows.append(
            {
                "code": "600003", "trade_time": day + pd.Timedelta(hours=9, minutes=31),
                "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
                "ctx_hfq_is_st": float("nan"), "evt_uplimit_active": 0.0,
            }
        )
    features, episodes, manifest = materialize_conservative_limit_lifecycle(pd.DataFrame(rows))
    assert features["UP_TOUCHED_LIMIT"].isna().all()
    assert episodes.empty
    assert manifest["vendor_confirmed_limit_session_count"] == 0
