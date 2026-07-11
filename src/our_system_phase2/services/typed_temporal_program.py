"""Typed, PIT-safe temporal primitives for NEXTGEN-DARK.

This module contains no reward, label, ranking, or search-policy logic. Every
primitive has an explicit type/clock/maturity/missing/cache contract and a
deterministic evaluator grouped by instrument.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Any, Callable, Sequence

import numpy as np
import pandas as pd


TEMPORAL_REGISTRY_VERSION = "nextgen_dark_typed_temporal_v1"


@dataclass(frozen=True, slots=True)
class TemporalPrimitiveSpec:
    name: str
    input_types: tuple[str, ...]
    output_type: str
    observable_time: str
    maturity: str
    missing_semantics: str
    canonicalization: str
    equivalence: str
    cache_policy: str
    pit_source_lag: str

    def canonical(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["input_types"] = list(self.input_types)
        return payload


def _spec(
    name: str,
    inputs: tuple[str, ...],
    output: str,
    maturity: str,
    missing: str,
    *,
    observable: str = "input_observable_time_plus_maturity",
    canonicalization: str = "lower_name_integer_windows_normalized_numeric_literals",
    equivalence: str = "canonical_name_args_params",
    cache: str = "registry_version_plus_canonical_call_plus_input_hash",
    lag: str = "max_input_source_lag",
) -> TemporalPrimitiveSpec:
    return TemporalPrimitiveSpec(
        name, inputs, output, observable, maturity, missing,
        canonicalization, equivalence, cache, lag,
    )


TEMPORAL_PRIMITIVES: dict[str, TemporalPrimitiveSpec] = {
    row.name.lower(): row
    for row in (
        _spec("Delta", ("numeric", "lag:int"), "numeric", "lag bars", "nan until both endpoints are valid"),
        _spec("Slope", ("numeric", "window:int"), "numeric", "window-1 bars", "full finite window required"),
        _spec("Acceleration", ("numeric", "window:int"), "numeric", "window bars", "full finite slope window required"),
        _spec("Persistence", ("boolean", "window:int"), "ratio", "window-1 bars", "missing input makes window missing"),
        _spec("Duration", ("state",), "count", "0 bars", "missing state yields missing duration"),
        _spec("StateAge", ("state",), "count", "0 bars", "missing state yields missing age"),
        _spec("TimeSince", ("event",), "count", "0 bars", "missing before first observed event"),
        _spec("Transition", ("state", "from", "to"), "event", "0 bars", "missing previous/current state yields false"),
        _spec("FirstHit", ("boolean", "window:int"), "position", "window-1 bars", "missing when no hit or incomplete window"),
        _spec("LastHit", ("boolean", "window:int"), "age", "window-1 bars", "missing when no hit or incomplete window"),
        _spec("PathShape", ("numeric", "window:int"), "ratio", "window-1 bars", "full finite window required"),
        _spec("DrawdownPath", ("numeric", "window:int"), "ratio", "window-1 bars", "full finite window and nonzero peak required"),
        _spec("RecoveryPath", ("numeric", "window:int"), "ratio", "window-1 bars", "full finite window and nonzero range required"),
        _spec("EventWindow", ("numeric", "event", "pre:int", "post:int"), "numeric", "post bars after event", "full event window required; output delayed to event+post"),
        _spec("MultiScaleRelation", ("numeric", "numeric", "short:int", "long:int"), "numeric", "long-1 bars", "jointly finite long window required", equivalence="symmetric_inputs_then_short_long"),
    )
}


def primitive_contract(name: str) -> TemporalPrimitiveSpec:
    try:
        return TEMPORAL_PRIMITIVES[name.lower()]
    except KeyError as exc:
        raise KeyError(f"unknown temporal primitive: {name}") from exc


def temporal_registry_contract() -> dict[str, Any]:
    rows = [TEMPORAL_PRIMITIVES[name].canonical() for name in sorted(TEMPORAL_PRIMITIVES)]
    payload = {
        "registry_version": TEMPORAL_REGISTRY_VERSION,
        "primitive_count": len(rows),
        "primitives": rows,
        "reward_or_performance_used": False,
    }
    payload["registry_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return payload


def _normalize_param(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value).strip().lower()
    if math.isfinite(number) and number.is_integer():
        return str(int(number))
    return format(number, ".12g")


def canonical_temporal_call(name: str, args: Sequence[str], params: Sequence[Any]) -> str:
    spec = primitive_contract(name)
    canonical_args = [str(arg).strip() for arg in args]
    canonical_params = [_normalize_param(value) for value in params]
    if spec.name == "MultiScaleRelation" and len(canonical_args) == 2:
        canonical_args = sorted(canonical_args)
    return f"{spec.name}({','.join([*canonical_args, *canonical_params])})"


def temporal_equivalence_key(name: str, args: Sequence[str], params: Sequence[Any]) -> str:
    return hashlib.sha256(
        f"{TEMPORAL_REGISTRY_VERSION}|{canonical_temporal_call(name, args, params)}".encode()
    ).hexdigest()[:24]


def _group_apply(frame: pd.DataFrame, value: pd.Series, function: Callable[[pd.Series], pd.Series]) -> pd.Series:
    pieces: list[pd.Series] = []
    for indices in frame.groupby("code", sort=False).groups.values():
        pieces.append(function(value.loc[indices]))
    return pd.concat(pieces).reindex(frame.index) if pieces else pd.Series(np.nan, index=frame.index)


def _rolling_apply(value: pd.Series, window: int, function: Callable[[np.ndarray], float]) -> pd.Series:
    return value.rolling(window, min_periods=window).apply(function, raw=True)


def _slope(values: np.ndarray) -> float:
    if not np.isfinite(values).all():
        return float("nan")
    x = np.arange(len(values), dtype=float)
    centered = x - x.mean()
    denominator = float(np.square(centered).sum())
    return float(centered @ (values - values.mean()) / denominator) if denominator else 0.0


def _duration(values: pd.Series) -> pd.Series:
    raw = values.to_numpy()
    out = np.full(len(raw), np.nan, dtype=float)
    age = 0
    has_previous = False
    previous: Any = None
    for index, value in enumerate(raw):
        if pd.isna(value):
            age = 0
            has_previous = False
            continue
        age = age + 1 if has_previous and value == previous else 1
        out[index] = float(age)
        previous = value
        has_previous = True
    return pd.Series(out, index=values.index)


def _time_since(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    out = np.full(len(values), np.nan, dtype=float)
    last: int | None = None
    for index, value in enumerate(numeric.to_numpy()):
        if not np.isfinite(value):
            continue
        if value > 0:
            last = index
        if last is not None:
            out[index] = float(index - last)
    return pd.Series(out, index=values.index)


def _first_hit(values: np.ndarray) -> float:
    if not np.isfinite(values).all():
        return float("nan")
    indices = np.flatnonzero(values > 0)
    return float(indices[0] + 1) if len(indices) else float("nan")


def _last_hit(values: np.ndarray) -> float:
    if not np.isfinite(values).all():
        return float("nan")
    indices = np.flatnonzero(values > 0)
    return float(len(values) - 1 - indices[-1]) if len(indices) else float("nan")


def _path_shape(values: np.ndarray) -> float:
    if not np.isfinite(values).all():
        return float("nan")
    distance = float(np.abs(np.diff(values)).sum())
    return float((values[-1] - values[0]) / distance) if distance > 0 else 0.0


def _drawdown(values: np.ndarray) -> float:
    if not np.isfinite(values).all():
        return float("nan")
    peak = float(np.max(values))
    return float(values[-1] / peak - 1.0) if abs(peak) > 1e-12 else float("nan")


def _recovery(values: np.ndarray) -> float:
    if not np.isfinite(values).all():
        return float("nan")
    low, high = float(np.min(values)), float(np.max(values))
    return float((values[-1] - low) / (high - low)) if high > low else 0.0


def evaluate_temporal_primitive(
    frame: pd.DataFrame,
    name: str,
    inputs: Sequence[pd.Series],
    params: Sequence[Any] = (),
) -> pd.Series:
    spec = primitive_contract(name)
    if "code" not in frame.columns:
        raise ValueError("temporal evaluation requires code")
    values = [pd.to_numeric(value, errors="coerce") for value in inputs]
    lower = spec.name.lower()
    if lower == "delta":
        lag = int(params[0])
        return values[0] - values[0].groupby(frame["code"], sort=False).shift(lag)
    if lower == "slope":
        window = int(params[0])
        return _group_apply(frame, values[0], lambda x: _rolling_apply(x, window, _slope))
    if lower == "acceleration":
        window = int(params[0])
        slope = _group_apply(frame, values[0], lambda x: _rolling_apply(x, window, _slope))
        return slope - slope.groupby(frame["code"], sort=False).shift(1)
    if lower == "persistence":
        window = int(params[0])
        return _group_apply(frame, values[0], lambda x: x.rolling(window, min_periods=window).mean())
    if lower in {"duration", "stateage"}:
        return _group_apply(frame, values[0], _duration)
    if lower == "timesince":
        return _group_apply(frame, values[0], _time_since)
    if lower == "transition":
        source, target = float(params[0]), float(params[1])
        previous = values[0].groupby(frame["code"], sort=False).shift(1)
        return ((previous == source) & (values[0] == target)).astype(float)
    if lower == "firsthit":
        window = int(params[0])
        return _group_apply(frame, values[0], lambda x: _rolling_apply(x, window, _first_hit))
    if lower == "lasthit":
        window = int(params[0])
        return _group_apply(frame, values[0], lambda x: _rolling_apply(x, window, _last_hit))
    if lower == "pathshape":
        window = int(params[0])
        return _group_apply(frame, values[0], lambda x: _rolling_apply(x, window, _path_shape))
    if lower == "drawdownpath":
        window = int(params[0])
        return _group_apply(frame, values[0], lambda x: _rolling_apply(x, window, _drawdown))
    if lower == "recoverypath":
        window = int(params[0])
        return _group_apply(frame, values[0], lambda x: _rolling_apply(x, window, _recovery))
    if lower == "eventwindow":
        pre, post = int(params[0]), int(params[1])
        if pre < 0 or post < 0:
            raise ValueError("EventWindow pre/post must be non-negative")
        out = pd.Series(np.nan, index=frame.index, dtype=float)
        for indices in frame.groupby("code", sort=False).groups.values():
            local_value = values[0].loc[indices].reset_index(drop=True)
            local_event = values[1].loc[indices].fillna(0).to_numpy() > 0
            target = np.full(len(indices), np.nan, dtype=float)
            for event_index in np.flatnonzero(local_event):
                start, end = event_index - pre, event_index + post
                maturity_index = event_index + post
                if start < 0 or end >= len(indices):
                    continue
                window_values = local_value.iloc[start : end + 1]
                if window_values.notna().all():
                    target[maturity_index] = float(window_values.mean())
            out.loc[indices] = target
        return out
    if lower == "multiscalerelation":
        short, long = int(params[0]), int(params[1])
        if short <= 1 or long <= short:
            raise ValueError("MultiScaleRelation requires 1 < short < long")
        out = pd.Series(np.nan, index=frame.index, dtype=float)
        for indices in frame.groupby("code", sort=False).groups.values():
            left = values[0].loc[indices]
            right = values[1].loc[indices]
            short_corr = left.rolling(short, min_periods=short).corr(right)
            long_corr = left.rolling(long, min_periods=long).corr(right)
            out.loc[indices] = (short_corr - long_corr).to_numpy()
        return out
    raise AssertionError(f"unhandled primitive: {spec.name}")


def evaluate_expression_temporal_call(
    frame: pd.DataFrame,
    name: str,
    args: Sequence[str],
    evaluate_child: Callable[[str], pd.Series],
) -> pd.Series | None:
    if name.lower() not in TEMPORAL_PRIMITIVES:
        return None
    lower = name.lower()
    if lower in {"duration", "stateage", "timesince"} and len(args) == 1:
        return evaluate_temporal_primitive(frame, name, [evaluate_child(args[0])])
    if lower in {"delta", "slope", "acceleration", "persistence", "firsthit", "lasthit", "pathshape", "drawdownpath", "recoverypath"} and len(args) == 2:
        return evaluate_temporal_primitive(frame, name, [evaluate_child(args[0])], [int(float(args[1]))])
    if lower == "transition" and len(args) == 3:
        return evaluate_temporal_primitive(frame, name, [evaluate_child(args[0])], [float(args[1]), float(args[2])])
    if lower in {"eventwindow", "multiscalerelation"} and len(args) == 4:
        return evaluate_temporal_primitive(
            frame, name, [evaluate_child(args[0]), evaluate_child(args[1])],
            [int(float(args[2])), int(float(args[3]))],
        )
    raise ValueError(f"invalid typed temporal call: {name}({','.join(args)})")
