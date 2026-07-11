from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.services.real_market_validation import evaluate_panel_expression
from our_system_phase2.services.typed_temporal_program import (
    TEMPORAL_PRIMITIVES,
    TemporalInput,
    TemporalProgramCache,
    canonical_temporal_call,
    evaluate_temporal_primitive,
    evaluate_typed_temporal_primitive,
    primitive_contract,
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
    left = evaluate_temporal_primitive(frame, name, series, params, data_role="development")
    right = evaluate_temporal_primitive(frame, name, series, params, data_role="development")

    pd.testing.assert_series_equal(left, right)
    assert len(left) == len(frame)


def test_event_window_is_delayed_until_post_event_maturity() -> None:
    frame = _frame().iloc[:8].copy()
    result = evaluate_temporal_primitive(
        frame,
        "EventWindow",
        [frame["x"], frame["event"]],
        [1, 1],
        data_role="development",
    )

    assert np.isnan(result.iloc[2])
    assert result.iloc[3] == pytest.approx((2 + 3 + 2) / 3)


def test_existing_expression_evaluator_dispatches_new_temporal_calls() -> None:
    frame = _frame()
    slope = evaluate_panel_expression(frame, "Slope($x,3)", data_role="development")
    relation = evaluate_panel_expression(
        frame, "MultiScaleRelation($x,$y,2,4)", data_role="development"
    )

    assert slope.notna().sum() > 0
    assert relation.notna().sum() > 0


@pytest.mark.parametrize(
    ("name", "inputs", "params"),
    [
        ("Delta", ("x",), (-1,)),
        ("Delta", ("x",), (0,)),
        ("Slope", ("x",), (1,)),
        ("Acceleration", ("x",), (1,)),
        ("Persistence", ("event",), (0,)),
        ("FirstHit", ("event",), (0,)),
        ("LastHit", ("event",), (0,)),
        ("PathShape", ("x",), (1,)),
        ("DrawdownPath", ("x",), (1,)),
        ("RecoveryPath", ("x",), (1,)),
        ("EventWindow", ("x", "event"), (-1, 1)),
        ("MultiScaleRelation", ("x", "y"), (4, 2)),
    ],
)
def test_invalid_temporal_parameters_fail_closed(
    name: str,
    inputs: tuple[str, ...],
    params: tuple[int, ...],
) -> None:
    frame = _frame()

    with pytest.raises(ValueError, match="invalid temporal parameters"):
        evaluate_temporal_primitive(
            frame,
            name,
            [frame[column] for column in inputs],
            params,
            data_role="development",
        )


def test_negative_delta_cannot_read_a_future_row() -> None:
    frame = _frame()

    with pytest.raises(ValueError, match="invalid temporal parameters"):
        evaluate_temporal_primitive(
            frame, "Delta", [frame["x"]], [-1], data_role="development"
        )


def test_temporal_evaluation_is_input_order_invariant() -> None:
    frame = _frame()
    expected = frame[["code", "trade_time"]].copy()
    expected["value"] = evaluate_temporal_primitive(
        frame, "Slope", [frame["x"]], [3], data_role="development"
    )
    shuffled = frame.sample(frac=1.0, random_state=17)
    observed = shuffled[["code", "trade_time"]].copy()
    observed["value"] = evaluate_temporal_primitive(
        shuffled, "Slope", [shuffled["x"]], [3], data_role="development"
    )

    expected = expected.sort_values(["code", "trade_time"]).reset_index(drop=True)
    observed = observed.sort_values(["code", "trade_time"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(observed, expected)


def test_temporal_evaluation_rejects_duplicate_coordinates() -> None:
    frame = _frame().iloc[:8].copy()
    frame.loc[1, "trade_time"] = frame.loc[0, "trade_time"]

    with pytest.raises(ValueError, match="duplicate temporal coordinates"):
        evaluate_temporal_primitive(
            frame, "Delta", [frame["x"]], [1], data_role="development"
        )


def test_event_and_boolean_primitives_reject_non_binary_inputs() -> None:
    frame = _frame()
    bad_event = frame["event"].copy()
    bad_event.iloc[2] = 2

    with pytest.raises(ValueError, match="binary event/boolean"):
        evaluate_temporal_primitive(
            frame, "TimeSince", [bad_event], data_role="development"
        )
    with pytest.raises(ValueError, match="binary event/boolean"):
        evaluate_temporal_primitive(
            frame,
            "EventWindow",
            [frame["x"], bad_event],
            [1, 1],
            data_role="development",
        )


@pytest.mark.parametrize(
    ("name", "inputs", "types", "params"),
    [
        ("Delta", ("x",), ("numeric",), (2,)),
        ("Slope", ("x",), ("numeric",), (3,)),
        ("Acceleration", ("x",), ("numeric",), (3,)),
        ("Persistence", ("event",), ("boolean",), (3,)),
        ("Duration", ("state",), ("state",), ()),
        ("StateAge", ("state",), ("state",), ()),
        ("TimeSince", ("event",), ("event",), ()),
        ("Transition", ("state",), ("state",), (0, 1)),
        ("FirstHit", ("event",), ("boolean",), (3,)),
        ("LastHit", ("event",), ("boolean",), (3,)),
        ("PathShape", ("x",), ("numeric",), (3,)),
        ("DrawdownPath", ("x",), ("numeric",), (3,)),
        ("RecoveryPath", ("x",), ("numeric",), (3,)),
        ("EventWindow", ("x", "event"), ("numeric", "event"), (1, 1)),
        ("MultiScaleRelation", ("x", "y"), ("numeric", "numeric"), (2, 4)),
    ],
)
def test_typed_result_propagates_clock_lag_output_type_and_cache(
    name: str,
    inputs: tuple[str, ...],
    types: tuple[str, ...],
    params: tuple[int, ...],
) -> None:
    frame = _frame()
    cache = TemporalProgramCache()
    typed_inputs = [
        TemporalInput(frame[column], value_type, frame["trade_time"], index + 1)
        for index, (column, value_type) in enumerate(zip(inputs, types))
    ]

    first = evaluate_typed_temporal_primitive(
        frame, name, typed_inputs, params, data_role="development", cache=cache
    )
    second = evaluate_typed_temporal_primitive(
        frame, name, typed_inputs, params, data_role="development", cache=cache
    )

    assert first.output_type == primitive_contract(name).output_type
    assert first.source_lag == len(typed_inputs)
    assert first.cache_hit is False
    assert second.cache_hit is True
    assert len(cache) == 1
    assert first.observable_at[first.values.notna()].le(frame.loc[first.values.notna(), "trade_time"]).all()
    pd.testing.assert_series_equal(first.values, second.values)


def test_typed_temporal_masks_inputs_not_yet_observable() -> None:
    frame = _frame().iloc[:8].copy()
    observable_at = frame["trade_time"].copy()
    observable_at.iloc[-1] = observable_at.iloc[-1] + pd.Timedelta(days=1)

    result = evaluate_typed_temporal_primitive(
        frame,
        "Delta",
        [TemporalInput(frame["x"], "numeric", observable_at, 2)],
        [1],
        data_role="development",
    )

    assert np.isnan(result.values.iloc[-1])
    assert result.source_lag == 2


def test_typed_temporal_requires_development_role_and_declared_types() -> None:
    frame = _frame()
    typed = [TemporalInput(frame["x"], "numeric", frame["trade_time"], 0)]

    with pytest.raises(PermissionError, match="development-only"):
        evaluate_typed_temporal_primitive(frame, "Delta", typed, [1], data_role="forward")
    with pytest.raises(TypeError, match="requires boolean"):
        evaluate_typed_temporal_primitive(
            frame,
            "Persistence",
            typed,
            [2],
            data_role="development",
        )


def test_typed_temporal_rejects_empty_code_and_implicit_expression_role() -> None:
    frame = _frame()
    frame.loc[0, "code"] = None
    with pytest.raises(ValueError, match="non-empty code"):
        evaluate_temporal_primitive(
            frame, "Delta", [frame["x"]], [1], data_role="development"
        )

    clean = _frame()
    with pytest.raises(PermissionError, match="explicit development data role"):
        evaluate_panel_expression(clean, "Slope($x,3)")
