from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


WINDOW_PRIOR = (2, 5, 10, 20, 60)
MIN_WINDOW = 1
MAX_WINDOW = 252

TIME_SERIES_OPERATORS = {
    "mean": {"canonical": "Mean", "arity": 2, "kind": "smoothing"},
    "ma": {"canonical": "Mean", "arity": 2, "kind": "smoothing"},
    "tsmean": {"canonical": "Mean", "arity": 2, "kind": "smoothing"},
    "mom": {"canonical": "Mom", "arity": 2, "kind": "momentum"},
    "std": {"canonical": "Std", "arity": 2, "kind": "volatility"},
    "delay": {"canonical": "Delay", "arity": 2, "kind": "lag"},
    "delta": {"canonical": "Delta", "arity": 2, "kind": "change"},
    "sub": {"canonical": "Sub", "arity": 2, "kind": "arithmetic"},
    "div": {"canonical": "Div", "arity": 2, "kind": "arithmetic"},
}


@dataclass(frozen=True, slots=True)
class DerivedFeatureSpec:
    field_name: str
    base_field: str
    operator: str
    window: int
    expression: str
    window_from_prior: bool


def normalize_window(value: int) -> int:
    if value < MIN_WINDOW:
        return MIN_WINDOW
    if value > MAX_WINDOW:
        return MAX_WINDOW
    return value


def parse_derived_feature_name(field_name: str) -> DerivedFeatureSpec | None:
    normalized = field_name.lower().lstrip("$")
    patterns = (
        (r"^(?:ma|mean)_?(\d+)$", "close", "Mean"),
        (r"^([a-z][a-z0-9]*)_(?:ma|mean)_?(\d+)$", None, "Mean"),
        (r"^(?:mom|momentum)_?(\d+)$", "close", "Mom"),
        (r"^([a-z][a-z0-9]*)_(?:mom|momentum)_?(\d+)$", None, "Mom"),
        (r"^(?:std|vol)_?(\d+)$", "ret", "Std"),
        (r"^([a-z][a-z0-9]*)_(?:std|vol)_?(\d+)$", None, "Std"),
        (r"^(?:delay|lag)_?(\d+)$", "close", "Delay"),
        (r"^([a-z][a-z0-9]*)_(?:delay|lag)_?(\d+)$", None, "Delay"),
    )
    for pattern, default_base, operator in patterns:
        match = re.match(pattern, normalized)
        if not match:
            continue
        if default_base is None:
            base_field = match.group(1)
            window = normalize_window(int(match.group(2)))
        else:
            base_field = default_base
            window = normalize_window(int(match.group(1)))
        canonical_name = f"{base_field}_{operator.lower()}_{window}"
        return DerivedFeatureSpec(
            field_name=canonical_name,
            base_field=base_field,
            operator=operator,
            window=window,
            expression=f"{operator}(${base_field},{window})",
            window_from_prior=window in WINDOW_PRIOR,
        )
    return None


def expand_derived_fields(expression: str) -> str:
    def replace(match: re.Match[str]) -> str:
        token = match.group(1)
        spec = parse_derived_feature_name(token)
        return spec.expression if spec else f"${token}"

    return re.sub(r"\$([A-Za-z_][A-Za-z0-9_]*)", replace, expression)


def operator_semantic_profile(operator: str, window: int | None = None) -> dict[str, float]:
    name = operator.lower()
    window = normalize_window(window or MIN_WINDOW)
    long_window = min(1.0, window / MAX_WINDOW)
    if name in {"mean", "ma", "tsmean"}:
        return {
            "momentum": 0.10,
            "size": 0.0,
            "value": 0.05,
            "volatility": -0.10,
            "turnover": -0.35 - (0.25 * long_window),
            "decay": 0.35 + (0.45 * long_window),
        }
    if name == "mom":
        return {"momentum": 0.35, "size": 0.0, "value": -0.05, "volatility": 0.05, "turnover": 0.05, "decay": 0.05}
    if name == "std":
        return {"momentum": 0.0, "size": 0.0, "value": 0.0, "volatility": 0.45, "turnover": 0.05, "decay": 0.10}
    if name in {"delay", "delta"}:
        return {"momentum": 0.05, "size": 0.0, "value": 0.0, "volatility": 0.05, "turnover": 0.10, "decay": 0.05}
    return {"momentum": 0.0, "size": 0.0, "value": 0.0, "volatility": 0.0, "turnover": 0.0, "decay": 0.0}


def operator_catalog_report() -> dict[str, Any]:
    return {
        "window_policy": {
            "parameterized": True,
            "min_window": MIN_WINDOW,
            "max_window": MAX_WINDOW,
            "sampling_prior": list(WINDOW_PRIOR),
            "sampling_prior_is_not_a_whitelist": True,
        },
        "operators": {
            key: dict(value)
            for key, value in TIME_SERIES_OPERATORS.items()
        },
        "derived_field_examples": {
            "ma2": "Mean($close,2)",
            "ma_2": "Mean($close,2)",
            "close_ma_20": "Mean($close,20)",
            "close_ma_137": "Mean($close,137)",
            "mom5": "Mom($close,5)",
            "ret_std_20": "Std($ret,20)",
        },
    }
