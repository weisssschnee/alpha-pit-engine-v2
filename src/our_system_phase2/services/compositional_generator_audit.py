"""Structural expressivity audit for legacy and compositional CN generators.

This module never opens market returns or invokes an economic evaluator.  It
measures syntax, lineage and compile contracts only; behavior identity remains
explicitly unresolved until a development-only signal sketch is materialized.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from statistics import median
from typing import Any, Iterable, Mapping

from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.expression_semantics import ExpressionNode, parse_expression
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator


AUDIT_VERSION = "cn_generator_expressivity_audit_v2"
_WINDOW_OPERATORS = {
    "Acceleration", "Delta", "Duration", "EventCount", "EventWindow",
    "FirstHit", "LastHit", "MultiScaleRelation", "PathShape", "Persistence",
    "Slope",
}
_FAILURE_CATEGORIES = (
    "GENERATOR_TEMPLATE_COLLAPSE",
    "SEMANTIC_ALIAS_COLLAPSE",
    "FIELD_COVERAGE_BOTTLENECK",
    "UNIT_CONTRACT_REJECTION",
    "PIT_OR_CLOCK_REJECTION",
    "CONTROL_CONSTRUCTION_FAILURE",
    "MATERIALIZATION_FAILURE",
    "SUPPORT_COLLAPSE",
    "TURNOVER_COST_DOMINATED",
    "NO_GROSS_EDGE",
    "CONTROL_NOT_BEATEN",
    "CROSS_SEED_INSTABILITY",
    "SEARCH_POLICY_COLLAPSE",
    "COMPUTE_OR_IO_BOTTLENECK",
)


def _walk(node: ExpressionNode, path: str = "root") -> Iterable[tuple[str, ExpressionNode]]:
    yield path, node
    for index, child in enumerate(node.args):
        yield from _walk(child, f"{path}.{node.token}[{index}]")


def _depth(node: ExpressionNode) -> int:
    if not node.args:
        return 0
    return 1 + max(_depth(child) for child in node.args)


def _structural_expression(expression: str) -> str:
    fields: dict[str, str] = {}

    def normalize(node: ExpressionNode) -> ExpressionNode:
        if not node.args:
            if node.token.startswith("$"):
                fields.setdefault(node.token, f"$F{len(fields) + 1}")
                return ExpressionNode(fields[node.token])
            try:
                value = float(node.token)
            except ValueError:
                return node
            if value == 0.0:
                return ExpressionNode("0")
            return ExpressionNode("-#" if value < 0.0 else "#")
        return ExpressionNode(node.token, tuple(normalize(child) for child in node.args))

    return normalize(parse_expression(expression)).render()


def _operator_signature(node: ExpressionNode) -> str:
    return "|".join(
        f"{path}:{current.token}"
        for path, current in _walk(node)
        if current.args
    )


def _windows(node: ExpressionNode) -> set[str]:
    output: set[str] = set()
    for _, current in _walk(node):
        if current.token not in _WINDOW_OPERATORS:
            continue
        for child in current.args[1:]:
            if child.args:
                continue
            try:
                float(child.token)
            except ValueError:
                continue
            output.add(child.token)
    return output


def _compile_failure_category(row: Mapping[str, Any], *, control: bool) -> str:
    code = str(row.get("typed_route_rejection_code") or "")
    if control or code == "CONTROL_CONTRACT_MISSING":
        return "CONTROL_CONSTRUCTION_FAILURE"
    if code in {"UNKNOWN_FIELD_ID", "SOURCE_FIELD_ID_PENDING", "ROUTE_NOT_ALLOWED"}:
        return "FIELD_COVERAGE_BOTTLENECK"
    if code == "UNIT_UNQUALIFIED":
        return "UNIT_CONTRACT_REJECTION"
    if code in {
        "PIT_UNQUALIFIED", "ENTITY_SCOPE_MISMATCH", "LATCHED_AS_LIFECYCLE",
        "INTRABAR_ORDER_FABRICATED", "POST_WINDOW_NOT_MATURE", "STATE_FIELD_NOT_CONSUMED",
        "SEALED_DATA_ACCESS", "EXPOSURE_LEDGER_MISSING",
    }:
        return "PIT_OR_CLOCK_REJECTION"
    if code in {"EXPRESSION_PARSE_ERROR", "SEMANTIC_GATE_REJECTED", "ROUTE_OPERATOR_FORBIDDEN"}:
        return "SEMANTIC_ALIAS_COLLAPSE"
    return "CONTROL_CONSTRUCTION_FAILURE" if control else "SEMANTIC_ALIAS_COLLAPSE"


def _empty_route_accumulator() -> dict[str, Any]:
    return {
        "attempts": 0,
        "pair_constructed": 0,
        "primary_compile_pass": 0,
        "control_compile_pass": 0,
        "control_valid": 0,
        "skeletons": set(),
        "arities": [],
        "depths": [],
        "operator_signatures": Counter(),
        "primitives": set(),
        "windows": set(),
        "source_families": set(),
        "condition_fields": set(),
        "exact": set(),
        "canonical": set(),
        "field_tuples_by_skeleton": defaultdict(set),
        "failures": Counter(),
        "search_role": "PRIMARY_SEARCH",
    }


def _finalize_route(row: dict[str, Any]) -> dict[str, Any]:
    exact_count = len(row["exact"])
    canonical_count = len(row["canonical"])
    skeleton_count = len(row["skeletons"])
    successful = int(row["control_valid"])
    operator_total = sum(row["operator_signatures"].values())
    top_operator_share = (
        max(row["operator_signatures"].values()) / operator_total
        if operator_total
        else 0.0
    )
    substituted = sum(
        len(field_sets)
        for field_sets in row["field_tuples_by_skeleton"].values()
        if len(field_sets) > 1
    )
    field_assignments = sum(
        len(field_sets) for field_sets in row["field_tuples_by_skeleton"].values()
    )
    failure_counts = {category: int(row["failures"].get(category, 0)) for category in _FAILURE_CATEGORIES}
    accounted_failures = sum(failure_counts.values())
    attempts = int(row["attempts"])
    bottleneck_reasons: list[str] = []
    if row["search_role"] != "FROZEN_REFERENCE_ONLY":
        if skeleton_count < 8:
            bottleneck_reasons.append("DISTINCT_SKELETONS_LT_8")
        if row["arities"] and median(row["arities"]) <= 2:
            bottleneck_reasons.append("MEDIAN_FIELD_ARITY_LE_2")
        if top_operator_share >= 0.95:
            bottleneck_reasons.append("TOP_OPERATOR_PATH_SHARE_GE_0_95")
    return {
        "search_role": row["search_role"],
        "attempts": attempts,
        "pair_constructed": int(row["pair_constructed"]),
        "parse_pass": int(row["primary_compile_pass"]),
        "route_type_pit_unit_pass": int(row["primary_compile_pass"]),
        "control_valid": successful,
        "distinct_expression_skeleton_count": skeleton_count,
        "field_arity_distribution": dict(sorted(Counter(row["arities"]).items())),
        "median_field_arity": float(median(row["arities"])) if row["arities"] else None,
        "maximum_expression_depth": max(row["depths"], default=0),
        "operator_path_coverage_count": len(row["operator_signatures"]),
        "top_operator_path_share": round(top_operator_share, 8),
        "primitive_coverage": sorted(row["primitives"]),
        "window_coverage": sorted(row["windows"], key=lambda value: float(value)),
        "source_family_coverage": sorted(row["source_families"]),
        "condition_field_coverage_count": len(row["condition_fields"]),
        "exact_identity_count": exact_count,
        "canonical_identity_count": canonical_count,
        "behavior_identity_count": None,
        "behavior_identity_status": "NOT_EVALUATED_PENDING_DEVELOPMENT_SIGNAL_SKETCH",
        "numeric_behavior_alias_rate": None,
        "template_level_collision_rate": round(
            1.0 - (canonical_count / successful), 8
        ) if successful else None,
        "field_substitution_rate": round(substituted / field_assignments, 8) if field_assignments else 0.0,
        "expressivity_bottleneck": bool(bottleneck_reasons),
        "expressivity_bottleneck_reasons": bottleneck_reasons,
        "failure_counts": failure_counts,
        "failure_categories_are_mutually_exclusive": accounted_failures == attempts - successful,
    }


def _audit_one_generator(
    registry: UnifiedCapabilityRegistry,
    *,
    generator_name: str,
    attempts: int,
    seed: int,
) -> dict[str, Any]:
    legacy = RegistryDrivenGenerator(registry)
    compositional = CompositionalGrammarV2(registry)
    routes = {route_id: _empty_route_accumulator() for route_id in ROUTE_IDS}
    base, remainder = divmod(int(attempts), len(ROUTE_IDS))
    allocations = {
        route_id: base + (1 if index < remainder else 0)
        for index, route_id in enumerate(ROUTE_IDS)
    }
    for route_index, route_id in enumerate(ROUTE_IDS):
        acc = routes[route_id]
        for index in range(allocations[route_id]):
            acc["attempts"] += 1
            try:
                if generator_name == "legacy_registry_v1":
                    raw_pair = legacy._pair(route_id, index, seed + route_index)  # deterministic audit of current authority
                    primary = {**raw_pair.candidate, **legacy.compiler.compile(raw_pair.candidate).to_dict()}
                    control = {**raw_pair.control, **legacy.compiler.compile(raw_pair.control).to_dict()}
                else:
                    pair = compositional.propose(route_id, attempt_index=index, seed=seed + route_index)
                    primary, control = pair.primary, pair.control
                acc["pair_constructed"] += 1
                acc["search_role"] = str(primary.get("search_role") or "PRIMARY_SEARCH")
            except Exception as exc:  # construction failure is an audited outcome, not silent underfill
                message = str(exc)
                category = (
                    "FIELD_COVERAGE_BOTTLENECK"
                    if "FIELD_COVERAGE_BOTTLENECK" in message or "empty registry pool" in message
                    else "CONTROL_CONSTRUCTION_FAILURE"
                )
                acc["failures"][category] += 1
                continue
            primary_legal = bool(primary.get("legal"))
            control_legal = bool(control.get("legal"))
            if primary_legal:
                acc["primary_compile_pass"] += 1
            if control_legal:
                acc["control_compile_pass"] += 1
            if not primary_legal:
                acc["failures"][_compile_failure_category(primary, control=False)] += 1
                continue
            if not control_legal:
                acc["failures"][_compile_failure_category(control, control=True)] += 1
                continue
            acc["control_valid"] += 1
            expression = str(primary["canonical_expression"])
            node = parse_expression(expression)
            skeleton = _structural_expression(expression)
            fields = tuple(sorted(str(value) for value in primary.get("declared_field_ids", ())))
            acc["skeletons"].add(skeleton)
            acc["arities"].append(len(fields))
            acc["depths"].append(_depth(node))
            signature = _operator_signature(node)
            acc["operator_signatures"][signature] += 1
            acc["primitives"].update(current.token for _, current in _walk(node) if current.args)
            acc["windows"].update(_windows(node))
            acc["condition_fields"].update(str(value) for value in primary.get("condition_field_ids", ()))
            acc["exact"].add(str(primary["exact_identity"]))
            acc["canonical"].add(str(primary["canonical_identity"]))
            acc["field_tuples_by_skeleton"][skeleton].add(fields)
            for field_id in fields:
                acc["source_families"].add(registry.resolve(field_id).source_family)
    finalized = {route_id: _finalize_route(routes[route_id]) for route_id in ROUTE_IDS}
    return {
        "attempts": int(attempts),
        "route_allocation": allocations,
        "routes": finalized,
        "generator_expressivity_bottleneck_observed": any(
            row["expressivity_bottleneck"]
            for route_id, row in finalized.items()
            if route_id != "BROAD_EVENT_FROZEN_ENTRY"
        ),
    }


def audit_generator_expressivity(
    registry: UnifiedCapabilityRegistry,
    *,
    attempts_per_generator: int,
    seed: int,
) -> dict[str, Any]:
    if attempts_per_generator < len(ROUTE_IDS) * 8:
        raise ValueError("attempt budget must expose every declared route skeleton")
    return {
        "audit_version": AUDIT_VERSION,
        "audit_scope": "STRUCTURAL_COMPILE_ONLY",
        "economic_evaluator_accessed": False,
        "data_roles_accessed": [],
        "behavior_identity_status": "NOT_EVALUATED",
        "behavior_identity_claim_ceiling": "AST_AND_COMPILE_RESULTS_CANNOT_PROVE_SIGNAL_BEHAVIOR_IDENTITY",
        "generators": {
            name: _audit_one_generator(
                registry,
                generator_name=name,
                attempts=int(attempts_per_generator),
                seed=int(seed),
            )
            for name in ("legacy_registry_v1", "compositional_v2")
        },
    }
