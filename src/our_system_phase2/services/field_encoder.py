from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from statistics import mean
from typing import Any

from our_system_phase2.services.feature_algebra import (
    expand_derived_fields,
    operator_semantic_profile,
    parse_derived_feature_name,
)
from our_system_phase2.services.event_derived_features import (
    canonical_event_feature_name,
    event_feature_behavior_profile,
    event_feature_type,
)


FIRST_BATCH_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "turnover_rate",
    "amount",
)

FIELD_TYPE_MAP = {
    "open": "price_ts",
    "high": "price_ts",
    "low": "price_ts",
    "close": "price_ts",
    "vwap": "price_ts",
    "volume": "volume_ts",
    "amount": "volume_ts",
    "turnover_rate": "crosssec",
    "turnover_ratio": "crosssec",
    "ret": "price_ts",
    "amtm": "price_ts",
    "return_1d": "price_ts",
    "return_5d": "price_ts",
    "return_20d": "price_ts",
    "rps_rank": "crosssec",
    "rps_score": "crosssec",
    "rps_slope_3d": "crosssec",
    "money_flow": "crosssec",
    "f9_quantile_250d": "crosssec",
    "crowding": "crosssec",
    "overnight": "crosssec",
    "low_20": "price_ts",
    "high_20": "price_ts",
    "price_pos": "crosssec",
    "rps_enhanced": "crosssec",
    "rps_rank_enhanced": "crosssec",
    "limit_up_event": "event_ts",
    "limit_down_event": "event_ts",
    "limit_up_streak": "event_ts",
    "limit_down_streak": "event_ts",
    "limit_up_break": "event_ts",
    "limit_down_repair": "event_ts",
    "limit_flip_up_to_down": "event_ts",
    "limit_flip_down_to_up": "event_ts",
    "market_trend_eff": "market_state_ts",
    "market_trend_state": "market_state_ts",
    "market_breadth_state": "market_state_ts",
    "market_vol_state": "market_state_ts",
    "stock_trend_eff": "state_ts",
    "stock_trend_state": "state_ts",
    "stock_trend_slope": "state_ts",
    "stock_price_position_state": "state_ts",
    "float_share": "capacity_crosssec",
    "total_share": "capacity_crosssec",
    "market_cap": "capacity_crosssec",
    "float_market_cap": "capacity_crosssec",
    "float_market_cap_yuan": "capacity_crosssec",
    "market_cap_billion": "capacity_crosssec",
    "float_market_cap_billion": "capacity_crosssec",
    "final_total_market_cap": "capacity_crosssec",
    "final_total_market_cap_billion": "capacity_crosssec",
    "final_float_market_cap": "capacity_crosssec",
    "final_float_market_cap_billion": "capacity_crosssec",
    "tdxgp_total_market_cap": "capacity_crosssec",
    "tdxgp_total_market_cap_billion": "capacity_crosssec",
    "market_cap_conflict_gt5pct": "data_quality_event",
    "plate_score": "event_crosssec",
}


FIELD_BEHAVIOR_PROFILES = {
    "open": {"momentum": 0.55, "size": 0.10, "value": 0.20, "volatility": 0.20, "turnover": 0.05},
    "high": {"momentum": 0.45, "size": 0.10, "value": 0.15, "volatility": 0.65, "turnover": 0.05},
    "low": {"momentum": 0.20, "size": 0.05, "value": 0.70, "volatility": 0.45, "turnover": 0.05},
    "close": {"momentum": 0.70, "size": 0.10, "value": 0.20, "volatility": 0.25, "turnover": 0.05},
    "vwap": {"momentum": 0.45, "size": 0.35, "value": 0.25, "volatility": 0.25, "turnover": 0.45},
    "volume": {"momentum": 0.10, "size": 0.75, "value": 0.10, "volatility": 0.35, "turnover": 0.85},
    "amount": {"momentum": 0.25, "size": 0.85, "value": 0.15, "volatility": 0.30, "turnover": 0.70},
    "turnover_rate": {"momentum": 0.15, "size": 0.65, "value": 0.10, "volatility": 0.40, "turnover": 0.95},
    "turnover_ratio": {"momentum": 0.15, "size": 0.65, "value": 0.10, "volatility": 0.40, "turnover": 0.95},
    "ret": {"momentum": 0.60, "size": 0.05, "value": 0.10, "volatility": 0.65, "turnover": 0.10},
    "amtm": {"momentum": 0.70, "size": 0.10, "value": 0.20, "volatility": 0.25, "turnover": 0.05},
    "return_1d": {"momentum": 0.55, "size": 0.05, "value": 0.10, "volatility": 0.65, "turnover": 0.10},
    "return_5d": {"momentum": 0.62, "size": 0.05, "value": 0.12, "volatility": 0.55, "turnover": 0.08},
    "return_20d": {"momentum": 0.70, "size": 0.10, "value": 0.18, "volatility": 0.35, "turnover": 0.06},
    "rps_rank": {"momentum": 0.82, "size": 0.10, "value": 0.10, "volatility": 0.20, "turnover": 0.20},
    "rps_score": {"momentum": 0.82, "size": 0.10, "value": 0.10, "volatility": 0.20, "turnover": 0.20},
    "rps_slope_3d": {"momentum": 0.70, "size": 0.05, "value": 0.10, "volatility": 0.45, "turnover": 0.35},
    "money_flow": {"momentum": 0.35, "size": 0.65, "value": 0.10, "volatility": 0.35, "turnover": 0.75},
    "f9_quantile_250d": {"momentum": 0.25, "size": 0.25, "value": 0.15, "volatility": 0.35, "turnover": 0.55},
    "crowding": {"momentum": 0.25, "size": 0.25, "value": 0.15, "volatility": 0.35, "turnover": 0.55},
    "overnight": {"momentum": 0.35, "size": 0.05, "value": 0.10, "volatility": 0.60, "turnover": 0.25},
    "low_20": {"momentum": 0.25, "size": 0.05, "value": 0.70, "volatility": 0.45, "turnover": 0.05},
    "high_20": {"momentum": 0.45, "size": 0.05, "value": 0.15, "volatility": 0.65, "turnover": 0.05},
    "price_pos": {"momentum": 0.65, "size": 0.05, "value": 0.20, "volatility": 0.35, "turnover": 0.10},
    "rps_enhanced": {"momentum": 0.85, "size": 0.12, "value": 0.10, "volatility": 0.22, "turnover": 0.22},
    "rps_rank_enhanced": {"momentum": 0.85, "size": 0.12, "value": 0.10, "volatility": 0.22, "turnover": 0.22},
    "limit_up_event": {"momentum": 0.78, "size": 0.05, "value": 0.05, "volatility": 0.72, "turnover": 0.30},
    "limit_down_event": {"momentum": 0.20, "size": 0.05, "value": 0.45, "volatility": 0.85, "turnover": 0.30},
    "limit_up_streak": {"momentum": 0.86, "size": 0.05, "value": 0.04, "volatility": 0.80, "turnover": 0.28},
    "limit_down_streak": {"momentum": 0.18, "size": 0.05, "value": 0.52, "volatility": 0.92, "turnover": 0.28},
    "limit_up_break": {"momentum": 0.45, "size": 0.05, "value": 0.20, "volatility": 0.90, "turnover": 0.36},
    "limit_down_repair": {"momentum": 0.55, "size": 0.05, "value": 0.42, "volatility": 0.88, "turnover": 0.36},
    "limit_flip_up_to_down": {"momentum": 0.15, "size": 0.05, "value": 0.18, "volatility": 0.95, "turnover": 0.40},
    "limit_flip_down_to_up": {"momentum": 0.70, "size": 0.05, "value": 0.38, "volatility": 0.94, "turnover": 0.40},
    "market_trend_eff": {"momentum": 0.78, "size": 0.05, "value": 0.10, "volatility": 0.25, "turnover": 0.04},
    "market_trend_state": {"momentum": 0.82, "size": 0.05, "value": 0.10, "volatility": 0.20, "turnover": 0.03},
    "market_breadth_state": {"momentum": 0.65, "size": 0.05, "value": 0.12, "volatility": 0.35, "turnover": 0.05},
    "market_vol_state": {"momentum": 0.18, "size": 0.05, "value": 0.18, "volatility": 0.92, "turnover": 0.05},
    "stock_trend_eff": {"momentum": 0.82, "size": 0.05, "value": 0.10, "volatility": 0.25, "turnover": 0.06},
    "stock_trend_state": {"momentum": 0.85, "size": 0.05, "value": 0.10, "volatility": 0.22, "turnover": 0.05},
    "stock_trend_slope": {"momentum": 0.74, "size": 0.05, "value": 0.10, "volatility": 0.42, "turnover": 0.18},
    "stock_price_position_state": {"momentum": 0.62, "size": 0.05, "value": 0.32, "volatility": 0.34, "turnover": 0.08},
    "float_share": {"momentum": 0.05, "size": 0.95, "value": 0.22, "volatility": 0.05, "turnover": 0.30},
    "total_share": {"momentum": 0.05, "size": 0.98, "value": 0.20, "volatility": 0.05, "turnover": 0.25},
    "market_cap": {"momentum": 0.10, "size": 0.98, "value": 0.35, "volatility": 0.08, "turnover": 0.28},
    "float_market_cap": {"momentum": 0.10, "size": 0.96, "value": 0.35, "volatility": 0.08, "turnover": 0.38},
    "float_market_cap_yuan": {"momentum": 0.10, "size": 0.96, "value": 0.35, "volatility": 0.08, "turnover": 0.38},
    "market_cap_billion": {"momentum": 0.10, "size": 0.98, "value": 0.35, "volatility": 0.08, "turnover": 0.28},
    "float_market_cap_billion": {"momentum": 0.10, "size": 0.96, "value": 0.35, "volatility": 0.08, "turnover": 0.38},
    "final_total_market_cap": {"momentum": 0.10, "size": 0.99, "value": 0.35, "volatility": 0.08, "turnover": 0.28},
    "final_total_market_cap_billion": {"momentum": 0.10, "size": 0.99, "value": 0.35, "volatility": 0.08, "turnover": 0.28},
    "final_float_market_cap": {"momentum": 0.10, "size": 0.97, "value": 0.35, "volatility": 0.08, "turnover": 0.40},
    "final_float_market_cap_billion": {"momentum": 0.10, "size": 0.97, "value": 0.35, "volatility": 0.08, "turnover": 0.40},
    "tdxgp_total_market_cap": {"momentum": 0.10, "size": 0.98, "value": 0.35, "volatility": 0.08, "turnover": 0.28},
    "tdxgp_total_market_cap_billion": {"momentum": 0.10, "size": 0.98, "value": 0.35, "volatility": 0.08, "turnover": 0.28},
    "market_cap_conflict_gt5pct": {"momentum": 0.02, "size": 0.45, "value": 0.05, "volatility": 0.50, "turnover": 0.10},
    "plate_score": {"momentum": 0.62, "size": 0.15, "value": 0.04, "volatility": 0.65, "turnover": 0.42},
}

FIELD_ALIASES = {
    "volt": "volume",
    "vrat": "turnover_rate",
    "mbrd": "vwap",
    "arat": "amount",
    "pldn": "low",
}


def _clip(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 6)


def _fundamental_behavior_profile(field_name: str) -> dict[str, float] | None:
    normalized = field_name.lower().lstrip("$")
    if not normalized.startswith("fund_"):
        return None
    if any(token in normalized for token in ("debt", "goodwill", "inventory")):
        return {"momentum": 0.05, "size": 0.15, "value": 0.70, "volatility": 0.45, "turnover": 0.05}
    if any(token in normalized for token in ("total_assets", "total_operate_income", "total_shares")):
        return {"momentum": 0.05, "size": 0.85, "value": 0.25, "volatility": 0.15, "turnover": 0.03}
    if any(token in normalized for token in ("netprofit", "ocf", "cash", "current_ratio", "research", "margin")):
        return {"momentum": 0.20, "size": 0.25, "value": 0.75, "volatility": 0.20, "turnover": 0.04}
    if any(token in normalized for token in ("holder", "float_share", "circulate")):
        return {"momentum": 0.15, "size": 0.35, "value": 0.45, "volatility": 0.25, "turnover": 0.06}
    return {"momentum": 0.10, "size": 0.35, "value": 0.55, "volatility": 0.25, "turnover": 0.05}


@dataclass(slots=True)
class EncodedField:
    field_name: str
    field_type: str
    vector: tuple[float, ...]
    behavior_profile: dict[str, float]


class FieldEncoder:
    """Continuous field encoder for Phase2 Stage B.

    This prototype intentionally encodes raw field identity into continuous
    behavior vectors without introducing a trainable model into the frozen
    runtime. New fields can be inserted by extending FIELD_TYPE_MAP and
    FIELD_BEHAVIOR_PROFILES without retraining existing fields.
    """

    def __init__(self, d_model: int = 8) -> None:
        self.d_model = d_model

    def encode(self, field_name: str, field_data: Any | None = None) -> EncodedField:
        normalized = field_name.lower().lstrip("$")
        event_name = canonical_event_feature_name(normalized)
        if event_name is not None:
            profile = event_feature_behavior_profile(event_name) or {
                "momentum": 0.55,
                "size": 0.05,
                "value": 0.12,
                "volatility": 0.80,
                "turnover": 0.40,
            }
            field_type = event_feature_type(event_name) or "event_ts"
            base = (
                profile["momentum"],
                profile["size"],
                profile["value"],
                profile["volatility"],
                profile["turnover"],
                0.0,
                0.0,
                1.0 if field_type == "event_crosssec" else 0.0,
            )
            return EncodedField(
                field_name=event_name,
                field_type=field_type,
                vector=tuple(round(value, 6) for value in base[: self.d_model]),
                behavior_profile=dict(profile),
            )
        derived = parse_derived_feature_name(normalized)
        if derived is not None:
            base_field = self.encode(derived.base_field)
            operator_profile = operator_semantic_profile(derived.operator, derived.window)
            profile = {
                name: _clip(base_field.behavior_profile[name] + operator_profile[name])
                for name in ("momentum", "size", "value", "volatility", "turnover")
            }
            base = (
                profile["momentum"],
                profile["size"],
                profile["value"],
                profile["volatility"],
                profile["turnover"],
                1.0 if base_field.field_type.endswith("price_ts") else 0.0,
                1.0 if base_field.field_type.endswith("volume_ts") else 0.0,
                1.0 if base_field.field_type.endswith("crosssec") else 0.0,
            )
            return EncodedField(
                field_name=derived.field_name,
                field_type=f"derived_{base_field.field_type}",
                vector=tuple(round(value, 6) for value in base[: self.d_model]),
                behavior_profile=profile,
            )
        fundamental_profile = _fundamental_behavior_profile(normalized)
        if fundamental_profile is not None:
            base = (
                fundamental_profile["momentum"],
                fundamental_profile["size"],
                fundamental_profile["value"],
                fundamental_profile["volatility"],
                fundamental_profile["turnover"],
                0.0,
                0.0,
                1.0,
            )
            return EncodedField(
                field_name=normalized,
                field_type="fundamental_crosssec",
                vector=tuple(round(value, 6) for value in base[: self.d_model]),
                behavior_profile=dict(fundamental_profile),
            )
        if normalized not in FIELD_TYPE_MAP:
            normalized = _closest_known_field(normalized)
        profile = FIELD_BEHAVIOR_PROFILES[normalized]
        field_type = FIELD_TYPE_MAP[normalized]
        base = (
            profile["momentum"],
            profile["size"],
            profile["value"],
            profile["volatility"],
            profile["turnover"],
            1.0 if field_type == "price_ts" else 0.0,
            1.0 if field_type == "volume_ts" else 0.0,
            1.0 if field_type == "crosssec" else 0.0,
        )
        return EncodedField(
            field_name=normalized,
            field_type=field_type,
            vector=tuple(round(value, 6) for value in base[: self.d_model]),
            behavior_profile=dict(profile),
        )


def _closest_known_field(field_name: str) -> str:
    return FIELD_ALIASES.get(field_name, "close")


def canonical_field_name(field_name: str) -> str | None:
    normalized = field_name.lower().lstrip("$")
    event_name = canonical_event_feature_name(normalized)
    if event_name is not None:
        return event_name
    derived = parse_derived_feature_name(normalized)
    if derived is not None:
        base = canonical_field_name(derived.base_field)
        if base is None:
            return None
        return derived.field_name
    if _fundamental_behavior_profile(normalized) is not None:
        return normalized
    if normalized in FIELD_TYPE_MAP:
        return normalized
    if normalized in FIELD_ALIASES:
        return FIELD_ALIASES[normalized]
    return None


def extract_field_names(expression: str) -> list[str]:
    fields: list[str] = []
    token = ""
    in_field = False
    for char in expression:
        if char == "$":
            if token:
                fields.append(token)
            token = ""
            in_field = True
            continue
        if in_field and (char.isalnum() or char == "_"):
            token += char
            continue
        if in_field:
            if token:
                fields.append(token)
            token = ""
            in_field = False
    if in_field and token:
        fields.append(token)
    return fields


def aggregate_field_profile(expression: str, encoder: FieldEncoder | None = None) -> dict[str, float]:
    encoder = encoder or FieldEncoder()
    fields = extract_field_names(expand_derived_fields(expression))
    if not fields:
        fields = ["close"]
    encoded = [encoder.encode(field).behavior_profile for field in fields]
    return {
        name: round(mean(profile[name] for profile in encoded), 6)
        for name in ("momentum", "size", "value", "volatility", "turnover")
    }


def _cosine(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = sqrt(sum(a * a for a in left))
    right_norm = sqrt(sum(b * b for b in right))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return dot / (left_norm * right_norm)


def field_redundancy_report(fields: tuple[str, ...] = FIRST_BATCH_FIELDS) -> dict[str, Any]:
    encoder = FieldEncoder()
    encoded = [encoder.encode(field) for field in fields]
    pairwise = []
    redundant_pairs = 0
    for index, left in enumerate(encoded):
        for right in encoded[index + 1 :]:
            similarity = round(_cosine(left.vector, right.vector), 6)
            is_redundant = similarity >= 0.92
            redundant_pairs += 1 if is_redundant else 0
            pairwise.append(
                {
                    "left": left.field_name,
                    "right": right.field_name,
                    "cosine_similarity": similarity,
                    "redundant": is_redundant,
                }
            )
    redundancy_ratio = round(redundant_pairs / max(1, len(pairwise)), 6)
    return {
        "field_batch": list(fields),
        "field_type_map": {field: encoder.encode(field).field_type for field in fields},
        "embedding_dimension": encoder.d_model,
        "redundancy_metric": "field_pair_cosine_similarity_ge_0.92_ratio",
        "redundancy_ratio": redundancy_ratio,
        "redundancy_threshold": 0.60,
        "redundancy_pass": redundancy_ratio < 0.60,
        "pairwise_similarity": pairwise,
    }
