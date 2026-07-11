from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.services.feature_state_fabric import (
    DeterministicFeatureCache,
    FeatureStateFabric,
    FieldRegistry,
    FieldRole,
    FieldSpec,
    MissingPolicy,
    ObservableClock,
)


REGISTRY_PATH = Path(__file__).resolve().parents[1] / "runtime/field_registry/nextgen_dark_field_registry_v1.json"


def test_committed_registry_covers_real_121_field_schema() -> None:
    registry = FieldRegistry.read(REGISTRY_PATH)

    assert len(registry.fields) == 121
    assert registry.get("open").role is FieldRole.PRIMARY
    assert registry.get("m1_first30_vwap").observable_clock is ObservableClock.FIRST_N_END
    assert registry.get("ctx_rzrq_rzye").source_lag == 1
    assert registry.get("evt_uplimit_active").role is FieldRole.STATE_ONLY
    assert registry.get("label_horizon").role is FieldRole.BLOCKED


def _registry() -> FieldRegistry:
    return FieldRegistry(
        "fixture_v1",
        [
            FieldSpec("close", "float64", "raw_1min", FieldRole.PRIMARY, ObservableClock.BAR_CLOSE, 0, "bars", MissingPolicy.PROPAGATE, ("close",), "identity"),
            FieldSpec("known", "float64", "context", FieldRole.INTERACTION_ONLY, ObservableClock.SOURCE_EFFECTIVE_TIME, 0, "bars", MissingPolicy.PROPAGATE, ("known",), "identity", observable_time_field="known_at"),
            FieldSpec("blocked", "float64", "metadata", FieldRole.BLOCKED, ObservableClock.METADATA_ONLY, 0, "rows", MissingPolicy.BLOCK, ("blocked",), "identity", blocked_reason="fixture"),
        ],
    )


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": ["B", "A", "A"],
            "trade_time": pd.to_datetime(["2024-01-02 09:31", "2024-01-02 09:32", "2024-01-02 09:31"]),
            "close": [3.0, 2.0, 1.0],
            "known": [30.0, 20.0, 10.0],
            "known_at": pd.to_datetime(["2024-01-02 09:30", "2024-01-02 09:33", "2024-01-02 09:30"]),
            "blocked": [1.0, 1.0, 1.0],
        }
    )


def test_materialization_is_batch_order_invariant_and_cached() -> None:
    cache = DeterministicFeatureCache()
    fabric = FeatureStateFabric(_registry(), cache=cache)
    frame = _frame()

    one, manifest_one = fabric.materialize(frame, ["close", "known"])
    two, manifest_two = fabric.materialize_shards([frame.iloc[1:], frame.iloc[:1]], ["close", "known"])

    pd.testing.assert_frame_equal(one, two)
    assert manifest_one["output_fingerprint"] == manifest_two["output_fingerprint"]
    assert manifest_two["cache_hits"] == 2
    assert np.isnan(one.loc[(one["code"] == "A") & (one["trade_time"].dt.minute == 32), "known"]).all()


def test_blocked_fields_fail_closed() -> None:
    with pytest.raises(PermissionError, match="blocked field"):
        FeatureStateFabric(_registry()).materialize(_frame(), ["blocked"])


def test_firstn_maturity_is_physically_masked_until_nth_bar() -> None:
    registry = FieldRegistry(
        "firstn_fixture_v1",
        [
            FieldSpec(
                "m1_first5_value",
                "float64",
                "firstN",
                FieldRole.PRIMARY,
                ObservableClock.FIRST_N_END,
                5,
                "minutes",
                MissingPolicy.PROPAGATE,
                ("m1_first5_value",),
                "identity",
            )
        ],
    )
    frame = pd.DataFrame(
        {
            "code": ["A"] * 6,
            "trade_time": pd.date_range("2024-01-02 09:30", periods=6, freq="min"),
            "m1_first5_value": [99.0] * 6,
        }
    )

    output, manifest = FeatureStateFabric(registry).materialize(frame, ["m1_first5_value"])

    assert output["m1_first5_value"].iloc[:4].isna().all()
    assert output["m1_first5_value"].iloc[4:].eq(99.0).all()
    assert manifest["temporal_guards"]["m1_first5_value"]["pre_maturity_value_count"] == 4


def test_firstn_maturity_uses_exchange_clock_not_batch_row_count() -> None:
    registry = FieldRegistry(
        "firstn_clock_fixture_v1",
        [
            FieldSpec(
                "m1_first5_value",
                "float64",
                "firstN",
                FieldRole.PRIMARY,
                ObservableClock.FIRST_N_END,
                5,
                "minutes",
                MissingPolicy.PROPAGATE,
                ("m1_first5_value",),
                "identity",
            )
        ],
    )
    frame = pd.DataFrame(
        {
            "code": ["A", "A", "B"],
            "trade_time": pd.to_datetime(
                ["2024-01-02 09:30", "2024-01-02 09:34", "2024-01-02 10:00"]
            ),
            "m1_first5_value": [99.0, 99.0, 88.0],
        }
    )

    output, _ = FeatureStateFabric(registry).materialize(frame, ["m1_first5_value"])

    assert np.isnan(output.iloc[0]["m1_first5_value"])
    assert output.iloc[1]["m1_first5_value"] == 99.0
    assert output.iloc[2]["m1_first5_value"] == 88.0


def test_previous_session_context_rejects_intraday_variation_without_source_clock() -> None:
    registry = FieldRegistry(
        "context_fixture_v1",
        [
            FieldSpec(
                "ctx_value",
                "float64",
                "lagged_daily_context",
                FieldRole.INTERACTION_ONLY,
                ObservableClock.PREVIOUS_SESSION,
                1,
                "sessions",
                MissingPolicy.PROPAGATE,
                ("ctx_value",),
                "identity",
                source_lag=1,
                source_lag_unit="sessions",
            )
        ],
    )
    frame = pd.DataFrame(
        {
            "code": ["A", "A"],
            "trade_time": pd.to_datetime(["2024-01-02 09:30", "2024-01-02 09:31"]),
            "ctx_value": [1.0, 2.0],
        }
    )

    with pytest.raises(ValueError, match="changes within a session"):
        FeatureStateFabric(registry).materialize(frame, ["ctx_value"])


def test_previous_session_context_fails_closed_without_source_clock() -> None:
    registry = FieldRegistry(
        "context_no_source_fixture_v1",
        [
            FieldSpec(
                "ctx_value",
                "float64",
                "lagged_daily_context",
                FieldRole.INTERACTION_ONLY,
                ObservableClock.PREVIOUS_SESSION,
                1,
                "sessions",
                MissingPolicy.PROPAGATE,
                ("ctx_value",),
                "identity",
                source_lag=1,
                source_lag_unit="sessions",
            )
        ],
    )
    frame = pd.DataFrame(
        {
            "code": ["A", "A"],
            "trade_time": pd.to_datetime(["2024-01-02 09:30", "2024-01-02 09:31"]),
            "ctx_value": [1.0, 1.0],
        }
    )

    with pytest.raises(ValueError, match="source-session evidence"):
        FeatureStateFabric(registry).materialize(frame, ["ctx_value"])


def test_previous_session_source_clock_masks_same_session_context() -> None:
    registry = FieldRegistry(
        "context_source_fixture_v1",
        [
            FieldSpec(
                "ctx_value",
                "float64",
                "lagged_daily_context",
                FieldRole.INTERACTION_ONLY,
                ObservableClock.PREVIOUS_SESSION,
                1,
                "sessions",
                MissingPolicy.PROPAGATE,
                ("ctx_value",),
                "identity",
                source_lag=1,
                source_lag_unit="sessions",
                source_session_field="ctx_source_session",
            )
        ],
    )
    frame = pd.DataFrame(
        {
            "code": ["A", "B"],
            "trade_time": pd.to_datetime(["2024-01-02 09:30", "2024-01-02 09:30"]),
            "ctx_source_session": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "ctx_value": [1.0, 2.0],
        }
    )

    output, manifest = FeatureStateFabric(registry).materialize(frame, ["ctx_value"])

    assert output.loc[output["code"] == "A", "ctx_value"].iloc[0] == 1.0
    assert np.isnan(output.loc[output["code"] == "B", "ctx_value"].iloc[0])
    assert manifest["temporal_guards"]["ctx_value"]["source_lag_violation_count"] == 1
    assert manifest["temporal_guards"]["ctx_value"]["source_lag_evidence"] == "SOURCE_SESSION_FIELD"
