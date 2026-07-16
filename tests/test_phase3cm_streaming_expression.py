from __future__ import annotations

import numpy as np
import pandas as pd

from our_system_phase2.services.phase3cm_streaming_expression import (
    StreamingExpressionExecutor,
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
            "event": frame["event"].to_numpy(dtype=np.float64),
        },
        code_ids=code_ids,
        time_ids=time_ids,
    )


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
    )
    expected_executor = _executor(frame)
    expected = expected_executor.evaluate_many(expressions)

    split = 12
    code_values = sorted(frame["code"].unique())
    code_map = {code: index for index, code in enumerate(code_values)}
    streaming = StreamingExpressionExecutor(code_count=len(code_values), compute_threads=2)
    chunks: dict[str, list[np.ndarray]] = {expression: [] for expression in expressions}
    for part in (frame.iloc[:split], frame.iloc[split:]):
        streaming.bind_block(
            raw_fields={
                "x": part["x"].to_numpy(dtype=np.float64),
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
