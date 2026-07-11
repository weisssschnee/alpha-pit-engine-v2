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
