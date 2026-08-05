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
_AUTHORIZED_EVALUATION_ROLES = frozenset(
    {
        "development",
        "validation_report_only",
        "forward_2026_report_only",
        "historical_challenge_report_only",
    }
)


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


@dataclass(frozen=True, slots=True)
class TemporalInput:
    values: pd.Series
    value_type: str
    observable_at: pd.Series
    source_lag: int = 0


@dataclass(frozen=True, slots=True)
class TemporalEvaluationResult:
    values: pd.Series
    observable_at: pd.Series
    output_type: str
    source_lag: int
    maturity: str
    cache_key: str
    cache_hit: bool


class TemporalProgramCache:
    def __init__(self) -> None:
        self._values: dict[str, tuple[pd.Series, pd.Series, str, int, str]] = {}

    def get(self, key: str) -> TemporalEvaluationResult | None:
        item = self._values.get(key)
        if item is None:
            return None
        values, observable_at, output_type, source_lag, maturity = item
        return TemporalEvaluationResult(
            values.copy(), observable_at.copy(), output_type, source_lag, maturity, key, True
        )

    def put(self, key: str, result: TemporalEvaluationResult) -> None:
        self._values[key] = (
            result.values.copy(),
            result.observable_at.copy(),
            result.output_type,
            result.source_lag,
            result.maturity,
        )

    def __len__(self) -> int:
        return len(self._values)


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

_INPUT_COUNTS = {
    "delta": 1,
    "slope": 1,
    "acceleration": 1,
    "persistence": 1,
    "duration": 1,
    "stateage": 1,
    "timesince": 1,
    "transition": 1,
    "firsthit": 1,
    "lasthit": 1,
    "pathshape": 1,
    "drawdownpath": 1,
    "recoverypath": 1,
    "eventwindow": 2,
    "multiscalerelation": 2,
}
_PARAM_COUNTS = {
    "delta": 1,
    "slope": 1,
    "acceleration": 1,
    "persistence": 1,
    "duration": 0,
    "stateage": 0,
    "timesince": 0,
    "transition": 2,
    "firsthit": 1,
    "lasthit": 1,
    "pathshape": 1,
    "drawdownpath": 1,
    "recoverypath": 1,
    "eventwindow": 2,
    "multiscalerelation": 2,
}
_VALUE_INPUT_TYPES = {
    "delta": ("numeric",),
    "slope": ("numeric",),
    "acceleration": ("numeric",),
    "persistence": ("boolean",),
    "duration": ("state",),
    "stateage": ("state",),
    "timesince": ("event",),
    "transition": ("state",),
    "firsthit": ("boolean",),
    "lasthit": ("boolean",),
    "pathshape": ("numeric",),
    "drawdownpath": ("numeric",),
    "recoverypath": ("numeric",),
    "eventwindow": ("numeric", "event"),
    "multiscalerelation": ("numeric", "numeric"),
}
_TYPE_COMPATIBILITY = {
    "numeric": {"numeric", "ratio", "count", "position", "age"},
    "boolean": {"boolean", "event"},
    "event": {"event", "boolean"},
    "state": {"state", "count"},
}


def _integer_parameter(value: Any) -> int:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid temporal parameters: {value!r} is not numeric") from exc
    if not math.isfinite(number) or not number.is_integer():
        raise ValueError(f"invalid temporal parameters: {value!r} is not an integer")
    return int(number)


def _validate_temporal_signature(
    spec: TemporalPrimitiveSpec,
    input_count: int,
    params: Sequence[Any],
) -> None:
    lower = spec.name.lower()
    if input_count != _INPUT_COUNTS[lower] or len(params) != _PARAM_COUNTS[lower]:
        raise ValueError(
            "invalid temporal parameters/signature: "
            f"{spec.name} needs {_INPUT_COUNTS[lower]} inputs and {_PARAM_COUNTS[lower]} params"
        )
    if lower == "transition":
        try:
            values = [float(value) for value in params]
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid temporal parameters: Transition states must be numeric") from exc
        if not all(math.isfinite(value) for value in values):
            raise ValueError("invalid temporal parameters: Transition states must be finite")
        return
    integers = [_integer_parameter(value) for value in params]
    if lower == "delta" and integers[0] < 1:
        raise ValueError("invalid temporal parameters: Delta lag must be >= 1")
    if lower in {"slope", "acceleration", "pathshape", "drawdownpath", "recoverypath"} and integers[0] < 2:
        raise ValueError(f"invalid temporal parameters: {spec.name} window must be >= 2")
    if lower in {"persistence", "firsthit", "lasthit"} and integers[0] < 1:
        raise ValueError(f"invalid temporal parameters: {spec.name} window must be >= 1")
    if lower == "eventwindow" and any(value < 0 for value in integers):
        raise ValueError("invalid temporal parameters: EventWindow pre/post must be non-negative")
    if lower == "multiscalerelation" and not (1 < integers[0] < integers[1]):
        raise ValueError("invalid temporal parameters: MultiScaleRelation requires 1 < short < long")


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
    _validate_temporal_signature(spec, len(args), params)
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


def _evaluate_temporal_primitive_canonical(
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
            relation = (short_corr - long_corr).replace([np.inf, -np.inf], np.nan)
            out.loc[indices] = relation.to_numpy()
        return out
    raise AssertionError(f"unhandled primitive: {spec.name}")


def _validate_binary_input(value: pd.Series) -> None:
    numeric = pd.to_numeric(value, errors="coerce")
    observed = set(numeric.dropna().unique().tolist())
    if not observed <= {0.0, 1.0}:
        raise ValueError(f"binary event/boolean input required, observed {sorted(observed)}")


def _series_hash(series: pd.Series) -> bytes:
    normalized = series.reset_index(drop=True)
    return pd.util.hash_pandas_object(normalized, index=False).to_numpy(dtype=np.uint64).tobytes()


def _typed_cache_key(
    spec: TemporalPrimitiveSpec,
    frame: pd.DataFrame,
    inputs: Sequence[TemporalInput],
    params: Sequence[Any],
) -> str:
    digest = hashlib.sha256()
    digest.update(TEMPORAL_REGISTRY_VERSION.encode())
    digest.update(canonical_temporal_call(spec.name, [f"input{index}" for index in range(len(inputs))], params).encode())
    digest.update(_series_hash(frame["code"].astype(str)))
    digest.update(_series_hash(pd.to_datetime(frame["trade_time"], errors="coerce", format="mixed")))
    for item in inputs:
        digest.update(item.value_type.encode())
        digest.update(str(item.source_lag).encode())
        digest.update(_series_hash(item.values))
        digest.update(_series_hash(pd.to_datetime(item.observable_at, errors="coerce", format="mixed")))
    return digest.hexdigest()


def _validate_output_type(values: pd.Series, output_type: str) -> None:
    numeric = pd.to_numeric(values, errors="coerce")
    finite = numeric.dropna().to_numpy(dtype=float)
    if not np.isfinite(finite).all():
        raise ValueError(f"temporal output type {output_type} produced non-finite values")
    if output_type == "event" and not set(finite.tolist()) <= {0.0, 1.0}:
        raise ValueError("temporal event output must be binary")
    if output_type in {"count", "position", "age"}:
        if (finite < 0).any() or not np.equal(finite, np.floor(finite)).all():
            raise ValueError(f"temporal {output_type} output must be non-negative integers")


def evaluate_typed_temporal_primitive(
    frame: pd.DataFrame,
    name: str,
    inputs: Sequence[TemporalInput],
    params: Sequence[Any] = (),
    *,
    data_role: str,
    cache: TemporalProgramCache | None = None,
) -> TemporalEvaluationResult:
    if data_role not in _AUTHORIZED_EVALUATION_ROLES:
        raise PermissionError(
            "typed temporal evaluation is development-only unless explicitly "
            "an explicitly authorized report-only role"
        )
    spec = primitive_contract(name)
    _validate_temporal_signature(spec, len(inputs), params)
    if not {"code", "trade_time"} <= set(frame.columns):
        raise ValueError("temporal evaluation requires code and trade_time")
    if frame["code"].isna().any() or frame["code"].astype(str).str.strip().isin({"", "nan", "None"}).any():
        raise ValueError("temporal coordinates require non-empty code")
    if any(len(item.values) != len(frame) or len(item.observable_at) != len(frame) for item in inputs):
        raise ValueError("temporal input length must equal frame length")
    expected_types = _VALUE_INPUT_TYPES[spec.name.lower()]
    for index, (item, expected) in enumerate(zip(inputs, expected_types)):
        if item.value_type not in _TYPE_COMPATIBILITY[expected]:
            raise TypeError(
                f"temporal input {index} for {spec.name} requires {expected}, observed {item.value_type}"
            )
        if item.source_lag < 0:
            raise ValueError("temporal input source_lag must be non-negative")

    coordinates = pd.DataFrame(
        {
            "code": frame["code"].astype(str).to_numpy(),
            "trade_time": pd.to_datetime(
                frame["trade_time"], errors="coerce", format="mixed"
            ).to_numpy(),
            "_position": np.arange(len(frame)),
        }
    )
    if coordinates["trade_time"].isna().any():
        raise ValueError("temporal coordinates must be valid")
    if coordinates.duplicated(["code", "trade_time"]).any():
        raise ValueError("duplicate temporal coordinates are forbidden")
    order = coordinates.sort_values(["code", "trade_time"], kind="mergesort")["_position"].to_numpy()
    canonical_frame = frame.iloc[order].copy().reset_index(drop=True)
    canonical_frame["code"] = coordinates.iloc[order]["code"].to_numpy()
    canonical_frame["trade_time"] = coordinates.iloc[order]["trade_time"].to_numpy()
    canonical_inputs: list[TemporalInput] = []
    safe_values: list[pd.Series] = []
    row_time = pd.to_datetime(canonical_frame["trade_time"], errors="coerce", format="mixed")
    for item in inputs:
        values = pd.Series(item.values.to_numpy(copy=False)[order], index=canonical_frame.index)
        observable_at = pd.Series(
            pd.to_datetime(item.observable_at, errors="coerce", format="mixed").to_numpy()[order],
            index=canonical_frame.index,
        )
        safe = values.where(observable_at.notna() & observable_at.le(row_time))
        canonical = TemporalInput(safe, item.value_type, observable_at, item.source_lag)
        canonical_inputs.append(canonical)
        safe_values.append(safe)

    binary_inputs = {
        "persistence": (0,),
        "timesince": (0,),
        "firsthit": (0,),
        "lasthit": (0,),
        "eventwindow": (1,),
    }.get(spec.name.lower(), ())
    for index in binary_inputs:
        _validate_binary_input(canonical_inputs[index].values)

    key = _typed_cache_key(spec, canonical_frame, canonical_inputs, params)
    cached = cache.get(key) if cache is not None else None
    if cached is None:
        values = _evaluate_temporal_primitive_canonical(
            canonical_frame, spec.name, safe_values, params
        )
        _validate_output_type(values, spec.output_type)
        observable_at = row_time.where(values.notna())
        canonical_result = TemporalEvaluationResult(
            values.reset_index(drop=True),
            observable_at.reset_index(drop=True),
            spec.output_type,
            max((item.source_lag for item in inputs), default=0),
            spec.maturity,
            key,
            False,
        )
        if cache is not None:
            cache.put(key, canonical_result)
    else:
        canonical_result = cached

    restored_values = np.full(len(frame), np.nan, dtype=float)
    restored_values[order] = pd.to_numeric(canonical_result.values, errors="coerce").to_numpy()
    restored_observable = np.full(len(frame), np.datetime64("NaT"), dtype="datetime64[ns]")
    restored_observable[order] = pd.to_datetime(canonical_result.observable_at).to_numpy()
    return TemporalEvaluationResult(
        pd.Series(restored_values, index=frame.index, dtype=float),
        pd.Series(restored_observable, index=frame.index),
        canonical_result.output_type,
        canonical_result.source_lag,
        canonical_result.maturity,
        canonical_result.cache_key,
        canonical_result.cache_hit,
    )


def evaluate_temporal_primitive(
    frame: pd.DataFrame,
    name: str,
    inputs: Sequence[pd.Series],
    params: Sequence[Any] = (),
    *,
    data_role: str,
    cache: TemporalProgramCache | None = None,
) -> pd.Series:
    spec = primitive_contract(name)
    observable_at = pd.to_datetime(frame["trade_time"], errors="coerce", format="mixed")
    typed_inputs = [
        TemporalInput(value, value_type, observable_at, 0)
        for value, value_type in zip(inputs, _VALUE_INPUT_TYPES[spec.name.lower()])
    ]
    return evaluate_typed_temporal_primitive(
        frame,
        spec.name,
        typed_inputs,
        params,
        data_role=data_role,
        cache=cache,
    ).values


def evaluate_expression_temporal_call(
    frame: pd.DataFrame,
    name: str,
    args: Sequence[str],
    evaluate_child: Callable[[str], pd.Series],
    *,
    data_role: str | None,
) -> pd.Series | None:
    if name.lower() not in TEMPORAL_PRIMITIVES:
        return None
    if data_role is None:
        raise PermissionError("typed temporal expression requires explicit development data role")
    lower = name.lower()
    if lower in {"duration", "stateage", "timesince"} and len(args) == 1:
        return evaluate_temporal_primitive(
            frame, name, [evaluate_child(args[0])], data_role=data_role
        )
    if lower in {"delta", "slope", "acceleration", "persistence", "firsthit", "lasthit", "pathshape", "drawdownpath", "recoverypath"} and len(args) == 2:
        return evaluate_temporal_primitive(
            frame,
            name,
            [evaluate_child(args[0])],
            [int(float(args[1]))],
            data_role=data_role,
        )
    if lower == "transition" and len(args) == 3:
        return evaluate_temporal_primitive(
            frame,
            name,
            [evaluate_child(args[0])],
            [float(args[1]), float(args[2])],
            data_role=data_role,
        )
    if lower in {"eventwindow", "multiscalerelation"} and len(args) == 4:
        return evaluate_temporal_primitive(
            frame, name, [evaluate_child(args[0]), evaluate_child(args[1])],
            [int(float(args[2])), int(float(args[3]))],
            data_role=data_role,
        )
    raise ValueError(f"invalid typed temporal call: {name}({','.join(args)})")
