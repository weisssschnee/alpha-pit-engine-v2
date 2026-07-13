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
