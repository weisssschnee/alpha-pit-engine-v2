"""Fail-closed compiler for registry-backed CN typed research routes."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from our_system_phase2.services.expression_semantics import (
    ExpressionNode,
    ExpressionParseError,
    analyze_expression,
    parse_expression,
)
from our_system_phase2.services.typed_primitive_gate import expression_fields
from our_system_phase2.services.unified_capability_registry import (
    CapabilityField,
    UnifiedCapabilityRegistry,
    stable_hash,
)


COMPILER_VERSION = "cn_typed_route_compiler_v1"


REJECTION_CODES = {
    "UNKNOWN_FIELD_ID",
    "SOURCE_FIELD_ID_PENDING",
    "PIT_UNQUALIFIED",
    "UNIT_UNQUALIFIED",
    "ENTITY_SCOPE_MISMATCH",
    "MARKET_FIELD_DIRECT_CSRANK",
    "ROUTE_OPERATOR_FORBIDDEN",
    "ROUTE_NOT_ALLOWED",
    "LATCHED_AS_LIFECYCLE",
    "INTRABAR_ORDER_FABRICATED",
    "POST_WINDOW_NOT_MATURE",
    "EPISODE_REPEAT_VOTE",
    "CONTROL_CONTRACT_MISSING",
    "STATE_FIELD_NOT_CONSUMED",
    "EXPOSURE_LEDGER_MISSING",
    "SEALED_DATA_ACCESS",
    "EXPRESSION_PARSE_ERROR",
    "SEMANTIC_GATE_REJECTED",
}


@dataclass(frozen=True, slots=True)
class CompileVerdict:
    legal: bool
    decision: str
    rejection_code: str
    reason: str
    route_id: str
    canonical_expression: str
    canonical_identity: str
    exact_identity: str
    field_ids: tuple[str, ...]
    source_field_ids: tuple[str, ...]
    representation_ids: tuple[str, ...]
    operator_paths: tuple[str, ...]
    support_unit: str
    maturity_rule: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "legal": self.legal,
            "typed_route_decision": self.decision,
            "typed_route_rejection_code": self.rejection_code,
            "typed_route_reason": self.reason,
            "route_id": self.route_id,
            "canonical_expression": self.canonical_expression,
            "canonical_identity": self.canonical_identity,
            "exact_identity": self.exact_identity,
            "field_ids": list(self.field_ids),
            "source_field_ids": list(self.source_field_ids),
            "representation_ids": list(self.representation_ids),
            "operator_paths": list(self.operator_paths),
            "support_unit": self.support_unit,
            "maturity_rule": self.maturity_rule,
        }


def _walk(node: ExpressionNode, path: str = "root") -> Iterable[tuple[str, ExpressionNode]]:
    yield path, node
    for index, child in enumerate(node.args):
        yield from _walk(child, f"{path}.{node.token}[{index}]")


def _subtree_fields(node: ExpressionNode) -> set[str]:
    fields: set[str] = set()
    for _, current in _walk(node):
        if not current.args and current.token.startswith("$"):
            fields.add(current.token[1:])
    return fields


def _operator_paths(node: ExpressionNode) -> tuple[str, ...]:
    return tuple(f"{path}:{current.token}" for path, current in _walk(node) if current.args)


def _operators(node: ExpressionNode) -> set[str]:
    return {current.token for _, current in _walk(node) if current.args}


def _reject(
    *,
    route_id: str,
    code: str,
    reason: str,
    expression: str = "",
    fields: Iterable[CapabilityField] = (),
    support_unit: str = "",
    maturity_rule: str = "",
) -> CompileVerdict:
    if code not in REJECTION_CODES:
        raise ValueError(f"unknown rejection code: {code}")
    rows = tuple(fields)
    return CompileVerdict(
        legal=False,
        decision="REJECT",
        rejection_code=code,
        reason=reason,
        route_id=route_id,
        canonical_expression=expression,
        canonical_identity="",
        exact_identity="",
        field_ids=tuple(row.field_id for row in rows),
        source_field_ids=tuple(row.source_field_id for row in rows),
        representation_ids=tuple(row.representation_id for row in rows),
        operator_paths=(),
        support_unit=support_unit,
        maturity_rule=maturity_rule,
    )


class TypedRouteCompiler:
    def __init__(self, registry: UnifiedCapabilityRegistry) -> None:
        self.registry = registry

    def compile(self, candidate: Mapping[str, Any]) -> CompileVerdict:
        route_id = str(candidate.get("route_id") or "")
        if route_id not in self.registry.route_contracts:
            return _reject(route_id=route_id, code="ROUTE_NOT_ALLOWED", reason="unknown route")
        route = self.registry.route_contracts[route_id]
        expression = str(candidate.get("expression") or "")
        try:
            semantic = analyze_expression(expression)
            parsed = parse_expression(semantic.canonical_expression)
        except (ExpressionParseError, ValueError) as exc:
            return _reject(
                route_id=route_id,
                code="EXPRESSION_PARSE_ERROR",
                reason=str(exc),
                expression=expression,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )
        if semantic.hard_blocked:
            return _reject(
                route_id=route_id,
                code="SEMANTIC_GATE_REJECTED",
                reason="|".join(semantic.issue_codes),
                expression=semantic.canonical_expression,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )

        declared = [str(value) for value in candidate.get("declared_field_ids", ())]
        expression_field_ids = expression_fields(semantic.canonical_expression)
        field_ids = sorted(set(expression_field_ids + declared))
        resolved: list[CapabilityField] = []
        for field_id in field_ids:
            try:
                field = self.registry.resolve(field_id)
            except KeyError:
                return _reject(
                    route_id=route_id,
                    code="UNKNOWN_FIELD_ID",
                    reason=f"unregistered field {field_id}",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )
            resolved.append(field)
            if not field.source_field_id.startswith("cn.sf.") or not field.representation_id.startswith("cn.rep."):
                return _reject(
                    route_id=route_id,
                    code="SOURCE_FIELD_ID_PENDING",
                    reason=f"unresolved identity for {field_id}",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )
            if not field.search_eligible or field.pit_status == "PIT_CONTRACT_UNRESOLVED":
                return _reject(
                    route_id=route_id,
                    code="PIT_UNQUALIFIED",
                    reason=field.blocked_reason or f"{field_id} is not PIT/search qualified",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )
            if field.unit_status == "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED":
                return _reject(
                    route_id=route_id,
                    code="UNIT_UNQUALIFIED",
                    reason=f"unit glossary not asserted for {field_id}",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )
            if route_id not in field.allowed_routes:
                return _reject(
                    route_id=route_id,
                    code="ROUTE_NOT_ALLOWED",
                    reason=f"{field_id} is not allowed on {route_id}",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )

        if not resolved:
            return _reject(
                route_id=route_id,
                code="UNKNOWN_FIELD_ID",
                reason="candidate has no registered field lineage",
                expression=semantic.canonical_expression,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )

        operators = _operators(parsed)
        allowed = set(str(value) for value in route.get("allowed_operator_families", ()))
        forbidden = set(str(value) for value in route.get("forbidden_operator_families", ()))
        explicit_family = str(candidate.get("operator_family") or "")
        if not explicit_family or explicit_family not in allowed:
            return _reject(
                route_id=route_id,
                code="ROUTE_OPERATOR_FORBIDDEN",
                reason=f"operator family {explicit_family or '<missing>'} is not registered on {route_id}",
                expression=semantic.canonical_expression,
                fields=resolved,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )
        if explicit_family in forbidden or operators.intersection(forbidden):
            return _reject(
                route_id=route_id,
                code="ROUTE_OPERATOR_FORBIDDEN",
                reason=f"forbidden operator family on {route_id}",
                expression=semantic.canonical_expression,
                fields=resolved,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )

        by_id = {row.field_id: row for row in resolved}
        market_fields = {row.field_id for row in resolved if row.entity_scope in {"MARKET", "INDUSTRY", "PLATE"}}
        for _, node in _walk(parsed):
            ranked_fields = _subtree_fields(node) if node.token == "CSRank" else set()
            if node.token == "CSRank" and ranked_fields and ranked_fields.issubset(market_fields):
                return _reject(
                    route_id=route_id,
                    code="MARKET_FIELD_DIRECT_CSRANK",
                    reason="market-level fields cannot be directly cross-sectionally ranked",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )

        condition_ids = set(str(value) for value in candidate.get("condition_field_ids", ()))
        if route_id == "MARKET_REGIME_CONDITION":
            if not condition_ids or not condition_ids.issubset(by_id):
                return _reject(
                    route_id=route_id,
                    code="ENTITY_SCOPE_MISMATCH",
                    reason="regime route requires registered condition_field_ids",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )
            if any(by_id[field_id].entity_scope not in {"MARKET", "INDUSTRY", "PLATE"} for field_id in condition_ids):
                return _reject(
                    route_id=route_id,
                    code="ENTITY_SCOPE_MISMATCH",
                    reason="regime condition field is not market/industry/plate scoped",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )
        elif any(row.entity_scope not in set(route["entity_scopes"]) for row in resolved):
            # Stock payload/control fields are allowed on event replay routes;
            # condition scope is checked separately above.
            if route_id not in {"BROAD_EVENT_FROZEN_ENTRY"}:
                return _reject(
                    route_id=route_id,
                    code="ENTITY_SCOPE_MISMATCH",
                    reason="field entity scope is incompatible with route",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )

        if route_id == "INTRADAY_STATE_TRANSITION" and not bool(
            candidate.get("is_matched_control", False)
        ):
            latched = [row.field_id for row in resolved if "LATCHED" in row.temporal_semantics]
            if latched and operators.intersection({"Transition", "StateTransition", "Duration", "StateAge"}):
                return _reject(
                    route_id=route_id,
                    code="LATCHED_AS_LIFECYCLE",
                    reason="latched observation cannot stand in for a lifecycle state",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )
            claimed = str(candidate.get("claimed_state_field_id") or "")
            state_expression = str(candidate.get("state_source_expression") or "")
            if not claimed or claimed not in by_id or not state_expression or state_expression not in semantic.canonical_expression:
                return _reject(
                    route_id=route_id,
                    code="STATE_FIELD_NOT_CONSUMED",
                    reason="state lane did not consume its declared state representation",
                    expression=semantic.canonical_expression,
                    fields=resolved,
                    support_unit=str(route["support_unit"]),
                    maturity_rule=str(route["maturity_rule"]),
                )

        if bool(candidate.get("requires_intrabar_order", False)):
            return _reject(
                route_id=route_id,
                code="INTRABAR_ORDER_FABRICATED",
                reason="one-minute OHLC cannot prove intrabar event order",
                expression=semantic.canonical_expression,
                fields=resolved,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )
        if bool(candidate.get("uses_future_revision", False)):
            return _reject(
                route_id=route_id,
                code="PIT_UNQUALIFIED",
                reason="future revision is forbidden",
                expression=semantic.canonical_expression,
                fields=resolved,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )
        if route_id in {"DISCLOSURE_EVENT", "BROAD_EVENT_FROZEN_ENTRY"} and not bool(
            candidate.get("maturity_contract_registered", False)
        ):
            return _reject(
                route_id=route_id,
                code="POST_WINDOW_NOT_MATURE",
                reason="event candidate lacks a registered maturity contract",
                expression=semantic.canonical_expression,
                fields=resolved,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )
        if str(candidate.get("vote_policy") or "") not in {"ONE_SUPPORT_UNIT_ONE_VOTE", "CONTROL_NO_SEPARATE_VOTE"}:
            return _reject(
                route_id=route_id,
                code="EPISODE_REPEAT_VOTE",
                reason="candidate lacks one-support-unit-one-vote contract",
                expression=semantic.canonical_expression,
                fields=resolved,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )
        if not str(candidate.get("matched_control_id") or ""):
            return _reject(
                route_id=route_id,
                code="CONTROL_CONTRACT_MISSING",
                reason="matched control identity is required before exposure",
                expression=semantic.canonical_expression,
                fields=resolved,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )
        if any(str(role).lower() in {"validation", "holdout", "challenge", "forward", "2026"} for role in candidate.get("access_roles", ())):
            return _reject(
                route_id=route_id,
                code="SEALED_DATA_ACCESS",
                reason="candidate contract requests a forbidden data role",
                expression=semantic.canonical_expression,
                fields=resolved,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )
        if not bool(candidate.get("exposure_ledger_required", False)):
            return _reject(
                route_id=route_id,
                code="EXPOSURE_LEDGER_MISSING",
                reason="candidate generation did not bind the exposure ledger",
                expression=semantic.canonical_expression,
                fields=resolved,
                support_unit=str(route["support_unit"]),
                maturity_rule=str(route["maturity_rule"]),
            )

        canonical_identity = stable_hash(
            {
                "compiler_version": COMPILER_VERSION,
                "route_id": route_id,
                "expression": semantic.canonical_expression,
                "representations": sorted(row.representation_id for row in resolved),
                "support_unit": route["support_unit"],
            }
        )
        exact_identity = hashlib.sha256(
            (route_id + "|" + semantic.canonical_expression + "|" + "|".join(sorted(row.representation_id for row in resolved))).encode("utf-8")
        ).hexdigest()
        return CompileVerdict(
            legal=True,
            decision="ALLOW",
            rejection_code="",
            reason="registry, PIT, unit, route, maturity, control and access contracts passed",
            route_id=route_id,
            canonical_expression=semantic.canonical_expression,
            canonical_identity=canonical_identity,
            exact_identity=exact_identity,
            field_ids=tuple(row.field_id for row in resolved),
            source_field_ids=tuple(row.source_field_id for row in resolved),
            representation_ids=tuple(row.representation_id for row in resolved),
            operator_paths=_operator_paths(parsed),
            support_unit=str(route["support_unit"]),
            maturity_rule=str(route["maturity_rule"]),
        )
