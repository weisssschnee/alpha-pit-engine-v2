from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.services.phase3cm_streaming_expression import (
    StreamingExpressionExecutor,
    unsupported_streaming_operators,
)
from our_system_phase2.services.real_market_validation import evaluate_panel_expression


def _frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for minute in range(8):
        for code_index, code in enumerate(("000001.SZ", "000002.SZ", "600000.SH")):
            rows.append(
                {
                    "trade_time": pd.Timestamp("2025-01-02 09:30") + pd.Timedelta(minutes=minute),
                    "code": code,
                    "x": float(minute + code_index + 1),
                    "y": float((minute + 1) ** 2 + code_index * (minute + 1)),
                    "event": float(minute in {1, 5}),
                }
            )
    return pd.DataFrame(rows)


def _executor(frame: pd.DataFrame) -> StreamingExpressionExecutor:
    code_values = sorted(frame["code"].unique())
    code_map = {code: index for index, code in enumerate(code_values)}
    code_ids = frame["code"].map(code_map).to_numpy(dtype=np.int32)
    time_ids = pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64)
    return StreamingExpressionExecutor(
        code_count=len(code_values),
        compute_threads=2,
    ).bind_block(
        raw_fields={
            "x": frame["x"].to_numpy(dtype=np.float64),
            "y": frame["y"].to_numpy(dtype=np.float64),
            "event": frame["event"].to_numpy(dtype=np.float64),
        },
        code_ids=code_ids,
        time_ids=time_ids,
    )


def test_streaming_executor_accepts_full_host_pool_and_rejects_oversubscription() -> None:
    executor = StreamingExpressionExecutor(code_count=3, compute_threads=30)
    assert executor.compute_threads == 30
    with pytest.raises(ValueError, match="between 1 and 32"):
        StreamingExpressionExecutor(code_count=3, compute_threads=33)


def test_streaming_expression_matches_reference_for_frozen_operator_surface() -> None:
    frame = _frame()
    expressions = (
        "CSRank(Mul(ZScore($x),ZScore(Delta($x,3))))",
        "CSResidual(ZScore($x),ZScore(Delta($x,2)))",
        "Mul(Persistence(Positive($x),3),Sign($x))",
        "CSRank(Mul(PathShape($x,3),Sign($x)))",
        "SafeDiv(EventCount($event,3),Add(TimeSince($event),1),1)",
        "MaskedZScore($x,3,0.8)",
        "CSRank(Acceleration($x,3))",
        "CSRank(Slope($x,3))",
        "FirstHit($event,3)",
        "Duration(Sign($x))",
        "Winsorize($x)",
        "Transition($event,0,1)",
        "EventWindow($x,$event,2,1)",
        "EventWindow(Sign($x),$event,5,0)",
        "MultiScaleRelation($x,$y,2,4)",
    )
    executor = _executor(frame)
    observed = executor.evaluate_many(expressions)
    for expression in expressions:
        expected = evaluate_panel_expression(frame, expression, data_role="development").to_numpy(dtype=float)
        np.testing.assert_allclose(observed[expression], expected, rtol=1e-12, atol=1e-12, equal_nan=True)


def test_streaming_expression_continuation_matches_uninterrupted() -> None:
    frame = _frame()
    expressions = (
        "Delta($x,3)",
        "Acceleration($x,3)",
        "Persistence(Positive($event),3)",
        "PathShape($x,3)",
        "Duration(Sign($x))",
        "TimeSince($event)",
        "EventCount($event,3)",
        "Transition($event,0,1)",
        "EventWindow($x,$event,2,1)",
        "EventWindow(Sign($x),$event,5,0)",
        "MultiScaleRelation($x,$y,2,4)",
    )
    expected_executor = _executor(frame)
    expected = expected_executor.evaluate_many(expressions)

    split = 6
    code_values = sorted(frame["code"].unique())
    code_map = {code: index for index, code in enumerate(code_values)}
    streaming = StreamingExpressionExecutor(code_count=len(code_values), compute_threads=2)
    chunks: dict[str, list[np.ndarray]] = {expression: [] for expression in expressions}
    for part in (frame.iloc[:split], frame.iloc[split:]):
        streaming.bind_block(
            raw_fields={
                "x": part["x"].to_numpy(dtype=np.float64),
                "y": part["y"].to_numpy(dtype=np.float64),
                "event": part["event"].to_numpy(dtype=np.float64),
            },
            code_ids=part["code"].map(code_map).to_numpy(dtype=np.int32),
            time_ids=pd.factorize(part["trade_time"], sort=False)[0].astype(np.int64),
        )
        result = streaming.evaluate_many(expressions)
        for expression in expressions:
            chunks[expression].append(result[expression])
    for expression in expressions:
        np.testing.assert_allclose(
            np.concatenate(chunks[expression]),
            expected[expression],
            rtol=1e-12,
            atol=1e-12,
            equal_nan=True,
        )


def test_mapping_mask_isolated_without_changing_value_state() -> None:
    frame = _frame()
    executor = _executor(frame)
    expression = "CSRank(Delta($x,2))"
    base = executor.evaluate_many((expression,))[expression]
    mask = np.ones(len(frame), dtype=bool)
    mask[1::3] = False
    masked = executor.evaluate_many((expression,), mapping_masks={expression: mask})[expression]
    assert np.isnan(masked[~mask]).all()
    assert np.isfinite(base[mask]).any()
    assert executor.audit["value_node_evaluations"] > 0
    assert executor.audit["mapping_node_evaluations"] > 0


def test_two_level_namespaces_share_values_only_within_value_cohort() -> None:
    frame = _frame()
    executor = _executor(frame)
    expression = "CSRank(Delta($x,2))"

    first, second, third = executor.evaluate_ordered(
        (expression, expression, expression),
        value_namespaces=("value.a", "value.a", "value.b"),
        mapping_namespaces=("mapping.a", "mapping.b", "mapping.c"),
    )

    np.testing.assert_allclose(first, second, rtol=0.0, atol=0.0, equal_nan=True)
    np.testing.assert_allclose(first, third, rtol=0.0, atol=0.0, equal_nan=True)
    continuation = executor.continuation_payload()
    assert len(continuation["rolling"]) == 2
    assert executor.audit["mapping_node_evaluations"] == 3


def test_cache_liveness_release_frees_owned_arrays_without_losing_state() -> None:
    frame = _frame()
    executor = _executor(frame)
    expression = "CSRank(Delta($x,2))"
    executor.evaluate_ordered(
        (expression,),
        value_namespaces=("value.a",),
        mapping_namespaces=("mapping.a",),
    )
    before = int(executor.audit["cache_current_bytes"])
    released = executor.release_cache_keys(
        (
            executor.cache_key(
                expression,
                value_namespace="value.a",
                mapping_namespace="mapping.a",
            ),
            executor.cache_key(
                "Delta($x,2)",
                value_namespace="value.a",
                mapping_namespace=None,
            ),
        )
    )

    assert released["released_entries"] == 2
    assert released["released_bytes"] > 0
    assert int(executor.audit["cache_current_bytes"]) < before
    assert executor.continuation_payload()["rolling"]


def test_intra_batch_liveness_materialization_matches_ordered_results() -> None:
    frame = _frame()
    expressions = (
        "CSRank(Delta($x,2))",
        "CSRank(Delta($x,2))",
        "CSRank(Acceleration($y,3))",
    )
    value_namespaces = ("value.a", "value.a", "value.b")
    mapping_namespaces = ("mapping.a", "mapping.a", "mapping.b")
    expected_executor = _executor(frame)
    expected = np.vstack(
        expected_executor.evaluate_ordered(
            expressions,
            value_namespaces=value_namespaces,
            mapping_namespaces=mapping_namespaces,
        )
    )

    observed_executor = _executor(frame)
    first_root = observed_executor.cache_key(
        expressions[0],
        value_namespace=value_namespaces[0],
        mapping_namespace=mapping_namespaces[0],
    )
    observed = observed_executor.evaluate_ordered_into(
        expressions,
        value_namespaces=value_namespaces,
        mapping_namespaces=mapping_namespaces,
        release_keys_after_each=((), (first_root,), ()),
    )

    np.testing.assert_allclose(observed, expected, rtol=0.0, atol=0.0, equal_nan=True)
    assert observed_executor.audit["intra_batch_released_entries"] == 1
    assert observed_executor.audit["intra_batch_released_bytes"] > 0


def test_intra_batch_liveness_stays_within_byte_cap_without_changing_values() -> None:
    frame = _frame()
    code_values = sorted(frame["code"].unique())
    code_map = {code: index for index, code in enumerate(code_values)}
    expressions = ("Add($x,$y)", "Sub($x,$y)", "Mul($x,$y)")
    array_bytes = len(frame) * np.dtype(np.float64).itemsize
    executor = StreamingExpressionExecutor(
        code_count=len(code_values),
        compute_threads=2,
        cache_max_bytes=array_bytes,
    ).bind_block(
        raw_fields={
            "x": frame["x"].to_numpy(dtype=np.float64),
            "y": frame["y"].to_numpy(dtype=np.float64),
        },
        code_ids=frame["code"].map(code_map).to_numpy(dtype=np.int32),
        time_ids=pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64),
    )
    release_keys = tuple(
        (
            executor.cache_key(
                expression,
                value_namespace="default",
                mapping_namespace=None,
            ),
        )
        for expression in expressions
    )

    observed = executor.evaluate_ordered_into(
        expressions,
        release_keys_after_each=release_keys,
    )

    expected = np.vstack(
        [
            evaluate_panel_expression(frame, expression, data_role="development").to_numpy(
                dtype=float
            )
            for expression in expressions
        ]
    )
    np.testing.assert_allclose(observed, expected, rtol=0.0, atol=0.0, equal_nan=True)
    assert executor.audit["cache_peak_bytes"] <= array_bytes
    assert executor.audit["cache_current_bytes"] == 0


def test_last_use_root_bypasses_cache_without_changing_values() -> None:
    frame = _frame()
    code_values = sorted(frame["code"].unique())
    code_map = {code: index for index, code in enumerate(code_values)}
    array_bytes = len(frame) * np.dtype(np.float64).itemsize
    executor = StreamingExpressionExecutor(
        code_count=len(code_values),
        compute_threads=2,
        cache_max_bytes=array_bytes,
    ).bind_block(
        raw_fields={"x": frame["x"].to_numpy(dtype=np.float64)},
        code_ids=frame["code"].map(code_map).to_numpy(dtype=np.int32),
        time_ids=pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64),
    )
    expression = "CSRank(Delta($x,2))"
    root_key = executor.cache_key(
        expression,
        value_namespace="default",
        mapping_namespace="default",
    )
    child_key = executor.cache_key(
        "Delta($x,2)",
        value_namespace="default",
        mapping_namespace=None,
    )

    observed = executor.evaluate_ordered_into(
        (expression,),
        release_keys_after_each=((root_key, child_key),),
    )
    expected = evaluate_panel_expression(
        frame, expression, data_role="development"
    ).to_numpy(dtype=float)

    np.testing.assert_allclose(observed[0], expected, rtol=0.0, atol=0.0, equal_nan=True)
    assert executor.audit["root_cache_bypass_count"] == 1
    assert executor.audit["root_cache_bypass_bytes"] == array_bytes
    assert executor.audit["cache_peak_bytes"] <= array_bytes
    assert executor.audit["cache_current_bytes"] == 0


def test_cache_pressure_bypasses_recomputable_last_use_nodes() -> None:
    frame = _frame()
    code_values = sorted(frame["code"].unique())
    code_map = {code: index for index, code in enumerate(code_values)}
    array_bytes = len(frame) * np.dtype(np.float64).itemsize
    executor = StreamingExpressionExecutor(
        code_count=len(code_values),
        compute_threads=2,
        cache_max_bytes=array_bytes,
    ).bind_block(
        raw_fields={
            "x": frame["x"].to_numpy(dtype=np.float64),
            "y": frame["y"].to_numpy(dtype=np.float64),
        },
        code_ids=frame["code"].map(code_map).to_numpy(dtype=np.int32),
        time_ids=pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64),
    )
    expression = "CSRank(Mul(Add(Delta($x,2),1),Add($y,2)))"
    release_keys = tuple(
        executor.cache_key(
            child,
            value_namespace="default",
            mapping_namespace="default" if child.startswith("CSRank") else None,
        )
        for child in (
            expression,
            "Mul(Add(Delta($x,2),1),Add($y,2))",
            "Add(Delta($x,2),1)",
            "Delta($x,2)",
            "Add($y,2)",
            "1",
            "2",
            "$x",
            "$y",
        )
    )

    observed = executor.evaluate_ordered_into(
        (expression,),
        release_keys_after_each=(release_keys,),
    )
    expected = evaluate_panel_expression(
        frame, expression, data_role="development"
    ).to_numpy(dtype=float)

    np.testing.assert_allclose(observed[0], expected, rtol=0.0, atol=0.0, equal_nan=True)
    assert executor.audit["native_kernel_calls"] == 2
    assert executor.audit["cache_pressure_bypass_count"] >= 1
    assert executor.audit["cache_pressure_bypass_bytes"] >= array_bytes
    assert executor.audit["cache_peak_bytes"] <= array_bytes
    assert executor.audit["cache_current_bytes"] == 0


def test_cache_pressure_bypasses_recomputable_non_last_use_nodes() -> None:
    frame = _frame()
    code_values = sorted(frame["code"].unique())
    code_map = {code: index for index, code in enumerate(code_values)}
    array_bytes = len(frame) * np.dtype(np.float64).itemsize
    executor = StreamingExpressionExecutor(
        code_count=len(code_values),
        compute_threads=2,
        cache_max_bytes=array_bytes,
    ).bind_block(
        raw_fields={
            "x": frame["x"].to_numpy(dtype=np.float64),
            "y": frame["y"].to_numpy(dtype=np.float64),
        },
        code_ids=frame["code"].map(code_map).to_numpy(dtype=np.int32),
        time_ids=pd.factorize(frame["trade_time"], sort=False)[0].astype(np.int64),
    )
    executor._store_cache(
        "stateful:retained",
        np.zeros(len(frame), dtype=np.float64),
        owned=True,
        recomputable=False,
    )
    expression = "Mul(Add($x,1),Add($y,2))"

    observed = executor.evaluate_ordered_into(
        (expression,),
        release_keys_after_each=((),),
    )
    expected = evaluate_panel_expression(
        frame, expression, data_role="development"
    ).to_numpy(dtype=float)

    np.testing.assert_allclose(observed[0], expected, rtol=0.0, atol=0.0, equal_nan=True)
    assert executor.audit["cache_pressure_non_last_use_bypass_count"] >= 1
    assert executor.audit["cache_pressure_non_last_use_bypass_bytes"] >= array_bytes
    assert executor.audit["cache_peak_bytes"] <= array_bytes


def test_new_streaming_operators_resume_from_serialized_continuation() -> None:
    frame = _frame()
    expressions = (
        "Transition($event,0,1)",
        "EventWindow($x,$event,2,1)",
        "EventWindow(Sign($x),$event,5,0)",
        "MultiScaleRelation($x,$y,2,4)",
    )
    expected = _executor(frame).evaluate_many(expressions)
    code_values = sorted(frame["code"].unique())
    code_map = {code: index for index, code in enumerate(code_values)}
    split = 12
    first = StreamingExpressionExecutor(code_count=len(code_values), compute_threads=2)
    first_part = frame.iloc[:split]
    first.bind_block(
        raw_fields={
            "x": first_part["x"].to_numpy(dtype=np.float64),
            "y": first_part["y"].to_numpy(dtype=np.float64),
            "event": first_part["event"].to_numpy(dtype=np.float64),
        },
        code_ids=first_part["code"].map(code_map).to_numpy(dtype=np.int32),
        time_ids=pd.factorize(first_part["trade_time"], sort=False)[0].astype(np.int64),
    )
    first_values = first.evaluate_many(expressions)
    payload = first.continuation_payload()

    second = StreamingExpressionExecutor(code_count=len(code_values), compute_threads=2)
    second.restore_continuation_payload(payload)
    second_part = frame.iloc[split:]
    second.bind_block(
        raw_fields={
            "x": second_part["x"].to_numpy(dtype=np.float64),
            "y": second_part["y"].to_numpy(dtype=np.float64),
            "event": second_part["event"].to_numpy(dtype=np.float64),
        },
        code_ids=second_part["code"].map(code_map).to_numpy(dtype=np.int32),
        time_ids=pd.factorize(second_part["trade_time"], sort=False)[0].astype(np.int64),
    )
    second_values = second.evaluate_many(expressions)
    for expression in expressions:
        observed = np.concatenate((first_values[expression], second_values[expression]))
        np.testing.assert_allclose(
            observed,
            expected[expression],
            rtol=1e-12,
            atol=1e-12,
            equal_nan=True,
        )


def test_operator_surface_preflight_reports_unknown_calls() -> None:
    assert unsupported_streaming_operators(("CSRank(Winsorize($x))",)) == ()
    assert unsupported_streaming_operators(("EventWindow(Sign($x),$event,5,0)",)) == ()
    assert unsupported_streaming_operators(("CSRank(FutureMagic($x))",)) == ("futuremagic",)
