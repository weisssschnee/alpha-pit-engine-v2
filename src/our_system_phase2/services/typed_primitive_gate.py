"""Typed primitive gate for CN event/state field safety.

This module is a construction-time validator. It is intentionally independent
from search memory: even if memory is empty, unsafe ordinary primitives on
sparse event/state fields must still be blocked.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any


REGISTRY_VERSION = "phase3ce1_typed_primitive_gate_v1_20260618"

FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")

ORDINARY_PRIMITIVES = (
    "Mean",
    "Std",
    "Delta",
    "Delay",
    "Wma",
    "Med",
    "Kurt",
    "Skew",
    "ZScore",
    "Mom",
    "Corr",
    "Cov",
    "CSResidual",
    "Div",
    "Log",
)

TYPED_PRIMITIVES = (
    "EventAge",
    "SinceLastEvent",
    "EventCount",
    "EventTransition",
    "StateAge",
    "StateDwell",
    "StateTransition",
    "WindowStateCount",
    "ValidRatioGate",
    "MaskedCorr",
    "MaskedZScore",
    "SafeCSResidual",
)


@dataclass(frozen=True)
class TypedGateVerdict:
    typed_gate_decision: str
    typed_gate_reason: str
    blocked_fields: str = ""
    blocked_primitives: str = ""
    required_rewrite: str = ""
    registry_version: str = REGISTRY_VERSION
    entry_lineage: str = ""
    materialization_stage: str = ""
    candidate_role: str = ""

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


def expression_fields(expression: str) -> list[str]:
    seen: set[str] = set()
    fields: list[str] = []
    for token in FIELD_RE.findall(expression or ""):
        field = token.lower()
        if field in seen:
            continue
        seen.add(field)
        fields.append(field)
    return fields


def field_category(field: str) -> str:
    name = field.lower().lstrip("$")
    if any(token in name for token in ("future", "label", "next_open", "next_close", "next_return")):
        return "text_or_label"
    if name.endswith("cutoff_minute") or name.endswith("_time_minute"):
        return "timestamp_or_key"
    if name in {"code", "symbol", "date", "trade_date", "trade_time", "notice_date", "report_date", "update_time"}:
        return "timestamp_or_key"
    if any(token in name for token in ("industry", "sector", "plate", "concept", "board_code", "theme")):
        return "membership_or_group_key"
    if name.startswith("ctx_"):
        return "coverage_sensitive"
    if any(token in name for token in ("limit", "uplimit", "open_board", "break_board", "fengdan", "auction", "seal")):
        return "sparse_event"
    if any(token in name for token in ("is_", "streak", "lb_", "max_lb", "hotness", "rank", "dwell", "state")):
        return "discrete_state"
    exact_coverage = {"pe", "pb", "ps", "pe_ttm", "pb_mrq", "ps_ttm", "volume_ratio", "turnover_ratio"}
    if (
        name in exact_coverage
        or name.startswith("fund_")
        or any(
            token in name
            for token in (
                "rzrq",
                "rzye",
                "billboard",
                "holder",
                "market_cap",
                "float_share",
                "total_share",
                "fundamental",
            )
        )
    ):
        return "coverage_sensitive"
    if name.startswith("m1_first"):
        return "firstn_minute_derived"
    if name in {"pct_chg", "amplitude_pct", "change", "pre_close", "vol"}:
        return "continuous_minute"
    if any(token in name for token in ("open", "high", "low", "close", "vwap", "amount", "volume", "ret", "range")):
        return "continuous_minute"
    return "coverage_sensitive"


def _primitive_present(expression: str, primitive: str) -> bool:
    return re.search(rf"\b{re.escape(primitive)}\s*\(", expression or "") is not None


def _ordinary_primitives(expression: str) -> list[str]:
    return [primitive for primitive in ORDINARY_PRIMITIVES if _primitive_present(expression, primitive)]


def _typed_primitives(expression: str) -> list[str]:
    return [primitive for primitive in TYPED_PRIMITIVES if _primitive_present(expression, primitive)]


def _has_field_inside_primitive(expression: str, field: str, primitive: str) -> bool:
    # Conservative text gate: if a blocked field appears after a primitive call
    # in the same expression, require typed rewrite. The evaluator parser is not
    # needed for this construction-time safety check.
    pattern = rf"\b{re.escape(primitive)}\s*\([^)]*\${re.escape(field)}\b"
    return re.search(pattern, expression or "", flags=re.IGNORECASE) is not None


def validate_expression(
    expression: str,
    *,
    entry_lineage: str,
    materialization_stage: str,
    candidate_role: str,
) -> TypedGateVerdict:
    expr = expression or ""
    fields = expression_fields(expr)
    if not fields:
        if any(token in expr.lower() for token in ("future", "label", "next_")):
            return TypedGateVerdict(
                typed_gate_decision="reject_label_or_future_field",
                typed_gate_reason="expression has label/future token without explicit field syntax",
                blocked_fields="",
                blocked_primitives="",
                entry_lineage=entry_lineage,
                materialization_stage=materialization_stage,
                candidate_role=candidate_role,
            )
        return TypedGateVerdict(
            typed_gate_decision="allow",
            typed_gate_reason="no dollar-field formula inputs detected",
            entry_lineage=entry_lineage,
            materialization_stage=materialization_stage,
            candidate_role=candidate_role,
        )

    typed_prims = _typed_primitives(expr)
    ordinary_prims = _ordinary_primitives(expr)
    field_categories = {field: field_category(field) for field in fields}

    label_or_key_fields = [
        field for field, category in field_categories.items() if category in {"timestamp_or_key", "text_or_label"}
    ]
    if label_or_key_fields:
        return TypedGateVerdict(
            typed_gate_decision="reject_label_or_future_field",
            typed_gate_reason="timestamp/key/label fields cannot be formula inputs",
            blocked_fields="|".join(label_or_key_fields),
            blocked_primitives="|".join(ordinary_prims),
            entry_lineage=entry_lineage,
            materialization_stage=materialization_stage,
            candidate_role=candidate_role,
        )

    membership_fields = [
        field for field, category in field_categories.items() if category == "membership_or_group_key"
    ]
    if membership_fields:
        return TypedGateVerdict(
            typed_gate_decision="reject_membership_key_formula_input",
            typed_gate_reason="membership/group fields are context keys until group geometry audit",
            blocked_fields="|".join(membership_fields),
            blocked_primitives="|".join(ordinary_prims),
            entry_lineage=entry_lineage,
            materialization_stage=materialization_stage,
            candidate_role=candidate_role,
        )

    blocked_categories = {"sparse_event", "discrete_state"}
    blocked_fields = [
        field for field, category in field_categories.items() if category in blocked_categories
    ]
    if blocked_fields and ordinary_prims:
        offending_prims = sorted(
            {
                primitive
                for primitive in ordinary_prims
                for field in blocked_fields
                if _has_field_inside_primitive(expr, field, primitive)
            }
        )
        if offending_prims or any(primitive in {"CSResidual", "Corr", "Cov", "Std", "Delta", "Delay"} for primitive in ordinary_prims):
            return TypedGateVerdict(
                typed_gate_decision="blocked_unsafe_known_structure",
                typed_gate_reason="ordinary continuous primitive consumed sparse event or discrete state field",
                blocked_fields="|".join(blocked_fields),
                blocked_primitives="|".join(offending_prims or ordinary_prims),
                required_rewrite="EventCount|EventAge|EventTransition|StateDwell|WindowStateCount",
                entry_lineage=entry_lineage,
                materialization_stage=materialization_stage,
                candidate_role=candidate_role,
            )
        return TypedGateVerdict(
            typed_gate_decision="require_typed_rewrite",
            typed_gate_reason="sparse event or discrete state field requires typed primitive route",
            blocked_fields="|".join(blocked_fields),
            blocked_primitives="|".join(ordinary_prims),
            required_rewrite="EventCount|EventAge|EventTransition|StateDwell|WindowStateCount",
            entry_lineage=entry_lineage,
            materialization_stage=materialization_stage,
            candidate_role=candidate_role,
        )
    if blocked_fields and not typed_prims:
        return TypedGateVerdict(
            typed_gate_decision="require_typed_rewrite",
            typed_gate_reason="sparse event or discrete state field cannot enter raw formula path without typed primitive",
            blocked_fields="|".join(blocked_fields),
            blocked_primitives="|".join(ordinary_prims),
            required_rewrite="EventCount|EventAge|EventTransition|StateDwell|WindowStateCount",
            entry_lineage=entry_lineage,
            materialization_stage=materialization_stage,
            candidate_role=candidate_role,
        )

    coverage_fields = [
        field for field, category in field_categories.items() if category == "coverage_sensitive"
    ]
    coverage_prims = [primitive for primitive in ordinary_prims if primitive in {"Corr", "Cov", "ZScore", "CSResidual"}]
    if coverage_fields and coverage_prims and not typed_prims:
        return TypedGateVerdict(
            typed_gate_decision="require_typed_rewrite",
            typed_gate_reason="coverage-sensitive field requires coverage-aware primitive guard",
            blocked_fields="|".join(coverage_fields),
            blocked_primitives="|".join(coverage_prims),
            required_rewrite="ValidRatioGate|MaskedCorr|MaskedZScore|SafeCSResidual",
            entry_lineage=entry_lineage,
            materialization_stage=materialization_stage,
            candidate_role=candidate_role,
        )

    return TypedGateVerdict(
        typed_gate_decision="allow",
        typed_gate_reason="no blocked field/primitive combination detected",
        registry_version=REGISTRY_VERSION,
        entry_lineage=entry_lineage,
        materialization_stage=materialization_stage,
        candidate_role=candidate_role,
    )


def validate_row(
    row: dict[str, Any],
    *,
    entry_lineage: str,
    materialization_stage: str,
    candidate_role: str,
    expression_key: str = "expression",
) -> dict[str, Any]:
    verdict = validate_expression(
        str(row.get(expression_key) or ""),
        entry_lineage=entry_lineage,
        materialization_stage=materialization_stage,
        candidate_role=candidate_role,
    )
    return {**row, **verdict.to_row()}


def gate_g2_input_row(
    row: dict[str, Any],
    *,
    entry_lineage: str = "g2_selector_input",
    materialization_stage: str = "selector_input",
    candidate_role: str = "g2_candidate",
    expression_key: str = "expression",
) -> dict[str, Any]:
    existing_decision = str(row.get("typed_gate_decision") or "")
    current = validate_expression(
        str(row.get(expression_key) or ""),
        entry_lineage=entry_lineage,
        materialization_stage=materialization_stage,
        candidate_role=candidate_role,
    )
    out = {
        **row,
        "input_typed_gate_decision": existing_decision,
        **current.to_row(),
    }
    if existing_decision and existing_decision != "allow":
        return {
            **out,
            "g2_input_gate_decision": "reject",
            "g2_input_gate_reason": "upstream typed gate already rejected row",
        }
    if current.typed_gate_decision != "allow":
        return {
            **out,
            "g2_input_gate_decision": "reject",
            "g2_input_gate_reason": "current typed primitive validator rejected row",
        }
    return {
        **out,
        "g2_input_gate_decision": "allow",
        "g2_input_gate_reason": "typed primitive validator passed selector input",
    }


def gate_g2_input_rows(
    rows: list[dict[str, Any]],
    *,
    entry_lineage: str = "g2_selector_input",
    materialization_stage: str = "selector_input",
    candidate_role: str = "g2_candidate",
    expression_key: str = "expression",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    allowed: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in rows:
        gated = gate_g2_input_row(
            row,
            entry_lineage=entry_lineage,
            materialization_stage=materialization_stage,
            candidate_role=candidate_role,
            expression_key=expression_key,
        )
        if str(gated.get("g2_input_gate_decision") or "") == "allow":
            allowed.append(gated)
        else:
            rejected.append(gated)
    return allowed, rejected


def is_allowed(verdict_or_row: dict[str, Any] | TypedGateVerdict) -> bool:
    if isinstance(verdict_or_row, TypedGateVerdict):
        return verdict_or_row.typed_gate_decision == "allow"
    return str(verdict_or_row.get("typed_gate_decision") or "") == "allow"
