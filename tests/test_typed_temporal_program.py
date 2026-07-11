from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.services.real_market_validation import evaluate_panel_expression
from our_system_phase2.services.typed_temporal_program import (
    TEMPORAL_PRIMITIVES,
    canonical_temporal_call,
    evaluate_temporal_primitive,
    temporal_equivalence_key,
    temporal_registry_contract,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": ["A"] * 8 + ["B"] * 8,
            "trade_time": pd.date_range("2024-01-02 09:31", periods=8, freq="min").tolist() * 2,
            "x": [1, 2, 3, 2, 1, 2, 3, 4, 4, 3, 2, 1, 2, 3, 2, 1],
            "y": [1, 1, 2, 2, 3, 3, 4, 4, 4, 4, 3, 3, 2, 2, 1, 1],
            "event": [0, 0, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0],
            "state": [0, 0, 1, 1, 1, 0, 0, 1, 1, 1, 0, 0, 2, 2, 1, 1],
        }
    )


def test_all_required_temporal_primitives_have_full_contracts() -> None:
    required = {
        "delta", "slope", "acceleration", "persistence", "duration", "stateage",
        "timesince", "transition", "firsthit", "lasthit", "pathshape",
        "drawdownpath", "recoverypath", "eventwindow", "multiscalerelation",
    }

    assert required == set(TEMPORAL_PRIMITIVES)
    contract = temporal_registry_contract()
    assert contract["primitive_count"] == 15
    for row in contract["primitives"]:
        assert all(row[key] for key in (
            "input_types", "output_type", "observable_time", "maturity",
            "missing_semantics", "canonicalization", "equivalence", "cache_policy",
            "pit_source_lag",
        ))


def test_canonicalization_and_symmetric_relation_dedup() -> None:
    assert canonical_temporal_call("delta", ["$x"], ["2.0"]) == "Delta($x,2)"
    assert temporal_equivalence_key("MultiScaleRelation", ["$x", "$y"], [2, 4]) == temporal_equivalence_key(
        "multiscalerelation", ["$y", "$x"], [2.0, 4.0]
    )


@pytest.mark.parametrize(
    ("name", "inputs", "params"),
    [
        ("Delta", ("x",), (2,)),
        ("Slope", ("x",), (3,)),
        ("Acceleration", ("x",), (3,)),
        ("Persistence", ("event",), (3,)),
        ("Duration", ("state",), ()),
        ("StateAge", ("state",), ()),
        ("TimeSince", ("event",), ()),
        ("Transition", ("state",), (0, 1)),
        ("FirstHit", ("event",), (3,)),
        ("LastHit", ("event",), (3,)),
        ("PathShape", ("x",), (3,)),
        ("DrawdownPath", ("x",), (3,)),
        ("RecoveryPath", ("x",), (3,)),
        ("EventWindow", ("x", "event"), (1, 1)),
        ("MultiScaleRelation", ("x", "y"), (2, 4)),
    ],
)
def test_each_temporal_primitive_is_deterministic(name: str, inputs: tuple[str, ...], params: tuple[int, ...]) -> None:
    frame = _frame()
    series = [frame[column] for column in inputs]
    left = evaluate_temporal_primitive(frame, name, series, params)
    right = evaluate_temporal_primitive(frame, name, series, params)

    pd.testing.assert_series_equal(left, right)
    assert len(left) == len(frame)


def test_event_window_is_delayed_until_post_event_maturity() -> None:
    frame = _frame().iloc[:8].copy()
    result = evaluate_temporal_primitive(frame, "EventWindow", [frame["x"], frame["event"]], [1, 1])

    assert np.isnan(result.iloc[2])
    assert result.iloc[3] == pytest.approx((2 + 3 + 2) / 3)


def test_existing_expression_evaluator_dispatches_new_temporal_calls() -> None:
    frame = _frame()
    slope = evaluate_panel_expression(frame, "Slope($x,3)")
    relation = evaluate_panel_expression(frame, "MultiScaleRelation($x,$y,2,4)")

    assert slope.notna().sum() > 0
    assert relation.notna().sum() > 0
