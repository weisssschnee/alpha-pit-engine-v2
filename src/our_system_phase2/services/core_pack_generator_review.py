"""Non-performance review of Core Pack roots against the typed generator.

The review opens registries and structural expressions only.  It never opens
returns, labels, validation, holdout, or forward data and cannot authorize a
search run.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Any, Iterable, Mapping, Sequence

from our_system_phase2.services.compositional_grammar import (
    CompositionalGrammarV2,
    SUPPLEMENTAL_GRAMMAR_VERSION,
    skeleton_registry,
    supplemental_skeleton_registry,
)
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    CapabilityField,
    UnifiedCapabilityRegistry,
    stable_hash,
)


REVIEW_VERSION = "cn_core_pack_generator_capacity_review_v1"
DISCOVERY_CONTRACT_VERSION = "cn_core_pack_development_discovery_contract_v1"


def _effective_count(counts: Mapping[str, int]) -> float:
    total = float(sum(counts.values()))
    if total <= 0.0:
        return 0.0
    entropy = -sum(
        (count / total) * math.log(count / total)
        for count in counts.values()
        if count > 0
    )
    return math.exp(entropy)


def _top_share(counts: Mapping[str, int]) -> float:
    total = sum(counts.values())
    return max(counts.values(), default=0) / total if total else 0.0


def _state_root_is_constructible(field: CapabilityField) -> bool:
    if field.source_family != "intraday_derived_state":
        return True
    expression = str((field.metadata or {}).get("materialization_expression") or "")
    return bool(expression) and expression.count("(") <= 1


def _compound_state_root_is_supplementally_constructible(
    field: CapabilityField,
) -> bool:
    if (
        field.source_family != "intraday_derived_state"
        or field.field_role != "state-only"
        or field.entity_scope != "STOCK"
        or field.temporal_semantics != "INTRADAY_DERIVED_STATE"
    ):
        return False
    metadata = field.metadata or {}
    expression = str(metadata.get("materialization_expression") or "")
    source_fields = tuple(str(value) for value in metadata.get("source_fields", ()))
    return bool(expression) and expression.count("(") > 1 and bool(source_fields)


def _is_slow_disclosure_age_condition(field: CapabilityField) -> bool:
    return (
        field.field_role == "condition-only"
        and field.entity_scope == "STOCK"
        and field.source_family
        == "canonical_fundamental_disclosure_timing_staleness"
        and field.temporal_semantics == "ASOF_LEVEL"
    )


def _is_previous_session_stock_regime_context(field: CapabilityField) -> bool:
    return (
        field.entity_scope == "STOCK"
        and field.field_role in {"condition-only", "state-only"}
        and field.temporal_semantics == "PREVIOUS_SESSION_STOCK_CONTEXT"
    )


def _root_is_constructible_on_route(
    route_id: str,
    field: CapabilityField,
) -> bool:
    payload = field.field_role in {"primary", "interaction-only"}
    if route_id == "MINUTE_STATIC":
        return payload and field.entity_scope == "STOCK"
    if route_id == "FIRSTN_PATH":
        return payload and field.entity_scope == "STOCK" and field.source_family in {
            "firstN",
            "raw_1min",
        }
    if route_id in {"SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE"}:
        return payload and field.entity_scope == "STOCK"
    if route_id == "DISCLOSURE_EVENT":
        return (
            field.entity_scope == "STOCK"
            and (
                (
                    field.field_role == "condition-only"
                    and field.temporal_semantics == "DISCLOSURE_PULSE"
                )
                or (
                    payload
                    and field.temporal_semantics
                    in {"DISCLOSURE_LEVEL_PAYLOAD", "DISCLOSURE_CHANGE_PAYLOAD"}
                )
            )
        )
    if route_id == "MARKET_REGIME_CONDITION":
        return (payload and field.entity_scope == "STOCK") or field.entity_scope == "MARKET"
    if route_id == "INTRADAY_STATE_TRANSITION":
        return (
            payload
            and field.entity_scope == "STOCK"
            and field.source_family == "raw_1min"
        ) or (
            field.field_role == "state-only"
            and field.entity_scope == "STOCK"
            and field.temporal_semantics == "INTRADAY_DERIVED_STATE"
            and _state_root_is_constructible(field)
        )
    if route_id == "BROAD_EVENT_FROZEN_ENTRY":
        return field.source_family == "broad_event_frozen_entry"
    return False


def audit_generator_capacity(
    registry: UnifiedCapabilityRegistry,
    *,
    core_field_ids: Iterable[str],
    attempts_by_route: Mapping[str, int],
    seeds: Sequence[int],
    checkpoints: Sequence[int],
) -> dict[str, Any]:
    """Measure current compositional-root consumption without market data."""

    if set(attempts_by_route) != set(ROUTE_IDS):
        raise ValueError("attempt allocation must cover every registered route")
    if not seeds:
        raise ValueError("at least one seed is required")
    if not checkpoints:
        raise ValueError("at least one checkpoint is required")
    grammar = CompositionalGrammarV2(registry)
    core = set(str(value) for value in core_field_ids)
    routes: dict[str, Any] = {}
    global_seen_routes: dict[str, set[str]] = {}

    for route_id in ROUTE_IDS:
        attempt_cap = int(attempts_by_route[route_id])
        if attempt_cap < len(skeleton_registry()[route_id]):
            raise ValueError(f"attempt cap does not cover all skeletons on {route_id}")
        route_checkpoints = sorted(
            set(int(value) for value in checkpoints if 0 < int(value) <= attempt_cap)
            | {attempt_cap}
        )
        eligible = registry.fields_for_route(route_id)
        eligible_ids = {field.field_id for field in eligible}
        eligible_core = eligible_ids & core
        field_counts: Counter[str] = Counter()
        family_counts: Counter[str] = Counter()
        operator_counts: Counter[str] = Counter()
        skeleton_counts: Counter[str] = Counter()
        exact_ids: set[str] = set()
        canonical_ids: set[str] = set()
        control_exact_ids: set[str] = set()
        construction_failures: Counter[str] = Counter()
        primary_legal = 0
        control_legal = 0
        valid_pairs = 0
        checkpoint_growth: list[dict[str, Any]] = []

        for seed in seeds:
            seed_exact: set[str] = set()
            seed_canonical: set[str] = set()
            seed_fields: set[str] = set()
            previous_exact = 0
            for attempt in range(attempt_cap):
                try:
                    pair = grammar.propose(
                        route_id,
                        attempt_index=attempt,
                        seed=int(seed),
                    )
                except Exception as exc:  # construction failure is a review result
                    construction_failures[type(exc).__name__ + ":" + str(exc)] += 1
                    continue
                primary = pair.primary
                control = pair.control
                primary_ok = bool(primary.get("legal"))
                control_ok = bool(control.get("legal"))
                primary_legal += int(primary_ok)
                control_legal += int(control_ok)
                if primary_ok and control_ok:
                    valid_pairs += 1
                    exact_id = str(primary["exact_identity"])
                    canonical_id = str(primary["canonical_identity"])
                    seed_exact.add(exact_id)
                    seed_canonical.add(canonical_id)
                    exact_ids.add(exact_id)
                    canonical_ids.add(canonical_id)
                    control_exact_ids.add(str(control["exact_identity"]))
                    operator_counts[str(primary.get("operator_family") or "UNKNOWN")] += 1
                    skeleton_counts[str(primary.get("skeleton_id") or "UNKNOWN")] += 1
                    for field_id in primary.get("declared_field_ids", ()):
                        field_id = str(field_id)
                        if field_id not in eligible_ids:
                            continue
                        field_counts[field_id] += 1
                        seed_fields.add(field_id)
                        global_seen_routes.setdefault(field_id, set()).add(route_id)
                        family_counts[registry.resolve(field_id).source_family] += 1
                completed = attempt + 1
                if completed in route_checkpoints:
                    checkpoint_growth.append(
                        {
                            "route_id": route_id,
                            "seed": int(seed),
                            "attempts": completed,
                            "exact_identity_count": len(seed_exact),
                            "new_exact_since_previous_checkpoint": len(seed_exact) - previous_exact,
                            "canonical_identity_count": len(seed_canonical),
                            "eligible_root_coverage_count": len(seed_fields),
                            "eligible_root_coverage_rate": (
                                len(seed_fields) / len(eligible_ids) if eligible_ids else 0.0
                            ),
                        }
                    )
                    previous_exact = len(seed_exact)

        seen = set(field_counts)
        expected_structural = {
            field.field_id
            for field in eligible
            if _root_is_constructible_on_route(route_id, field)
        }
        total_attempts = attempt_cap * len(seeds)
        routes[route_id] = {
            "route_id": route_id,
            "search_role": (
                "FROZEN_REFERENCE_ONLY"
                if route_id == "BROAD_EVENT_FROZEN_ENTRY"
                else "PRIMARY_SEARCH_PENDING_SEPARATE_AUTHORIZATION"
            ),
            "attempts_per_seed": attempt_cap,
            "seed_count": len(seeds),
            "attempts": total_attempts,
            "pair_construction_failure_count": sum(construction_failures.values()),
            "pair_construction_failures": dict(sorted(construction_failures.items())),
            "primary_legal_count": primary_legal,
            "control_legal_count": control_legal,
            "valid_pair_count": valid_pairs,
            "valid_pair_rate": valid_pairs / total_attempts if total_attempts else 0.0,
            "declared_skeleton_count": len(skeleton_registry()[route_id]),
            "observed_skeleton_count": len(skeleton_counts),
            "skeleton_observation_counts": dict(sorted(skeleton_counts.items())),
            "operator_family_count": len(operator_counts),
            "operator_family_observation_counts": dict(sorted(operator_counts.items())),
            "exact_identity_count": len(exact_ids),
            "control_exact_identity_count": len(control_exact_ids),
            "canonical_identity_count": len(canonical_ids),
            "eligible_root_count": len(eligible_ids),
            "structurally_constructible_root_count": len(expected_structural),
            "observed_eligible_root_count": len(seen),
            "observed_eligible_root_rate": len(seen) / len(eligible_ids) if eligible_ids else 0.0,
            "eligible_core_root_count": len(eligible_core),
            "observed_core_root_count": len(seen & eligible_core),
            "unobserved_core_root_ids": sorted(eligible_core - seen),
            "unobserved_eligible_root_ids": sorted(eligible_ids - seen),
            "structurally_unconstructible_root_ids": sorted(eligible_ids - expected_structural),
            "field_observation_counts": dict(sorted(field_counts.items())),
            "source_family_observation_counts": dict(sorted(family_counts.items())),
            "source_family_n_eff": _effective_count(family_counts),
            "top_source_family_share": _top_share(family_counts),
            "checkpoint_growth": checkpoint_growth,
        }

    exposed_core = core & set(global_seen_routes)
    return {
        "review_version": REVIEW_VERSION,
        "scope": "STRUCTURAL_COMPILE_AND_ROOT_CONSUMPTION_ONLY",
        "registry_hash": registry.registry_hash,
        "seeds": [int(value) for value in seeds],
        "attempts_by_route": {key: int(value) for key, value in attempts_by_route.items()},
        "core_root_count": len(core),
        "core_root_exposed_count": len(exposed_core),
        "core_root_unexposed_ids": sorted(core - exposed_core),
        "routes": routes,
        "economic_evaluator_accessed": False,
        "performance_or_reward_used": False,
        "data_roles_accessed": [],
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
        "behavior_identity_status": "NOT_EVALUATED",
        "claim_ceiling": "STRUCTURAL_CAPACITY_IS_NOT_ALPHA_EVIDENCE",
    }


def audit_supplemental_generator_delta(
    registry: UnifiedCapabilityRegistry,
    *,
    attempts_per_route: int = 64,
    seeds: Sequence[int] = (20260718,),
    route_root_allowlist: Mapping[str, Iterable[str]] | None = None,
) -> dict[str, Any]:
    """Audit only append-only gap constructors without replaying the base pack."""

    if attempts_per_route <= 0:
        raise ValueError("supplemental attempts must be positive")
    if not seeds:
        raise ValueError("at least one supplemental seed is required")
    grammar = CompositionalGrammarV2(
        registry,
        route_root_allowlist=route_root_allowlist,
    )
    routes: dict[str, Any] = {}
    for route_id in supplemental_skeleton_registry():
        eligible = registry.fields_for_route(route_id)
        if route_id == "SLOW_CROSS_SECTIONAL_LEVEL":
            expected = {
                field.field_id
                for field in eligible
                if _is_slow_disclosure_age_condition(field)
            }
        elif route_id == "MARKET_REGIME_CONDITION":
            expected = {
                field.field_id
                for field in eligible
                if _is_previous_session_stock_regime_context(field)
            }
        else:
            expected = {
                field.field_id
                for field in eligible
                if _compound_state_root_is_supplementally_constructible(field)
            }
        allowed = (
            set(str(value) for value in route_root_allowlist.get(route_id, ()))
            if route_root_allowlist is not None
            else None
        )
        if allowed is not None:
            expected &= allowed
        observed: set[str] = set()
        exact_ids: set[str] = set()
        canonical_ids: set[str] = set()
        failures: Counter[str] = Counter()
        valid_pairs = 0
        for seed in seeds:
            for attempt_index in range(int(attempts_per_route)):
                try:
                    pair = grammar.propose_supplemental(
                        route_id,
                        attempt_index=attempt_index,
                        seed=int(seed),
                    )
                except Exception as exc:  # structural failure is the audit result
                    failures[type(exc).__name__ + ":" + str(exc)] += 1
                    continue
                if bool(pair.primary.get("legal")) and bool(pair.control.get("legal")):
                    valid_pairs += 1
                    exact_ids.add(str(pair.primary["exact_identity"]))
                    canonical_ids.add(str(pair.primary["canonical_identity"]))
                    observed.update(
                        str(field_id)
                        for field_id in pair.primary.get("declared_field_ids", ())
                        if str(field_id) in expected
                    )
        total = int(attempts_per_route) * len(seeds)
        routes[route_id] = {
            "supplemental_skeleton_ids": [
                row.skeleton_id for row in supplemental_skeleton_registry()[route_id]
            ],
            "attempts": total,
            "valid_pair_count": valid_pairs,
            "valid_pair_rate": valid_pairs / total if total else 0.0,
            "construction_failures": dict(sorted(failures.items())),
            "exact_identity_count": len(exact_ids),
            "canonical_identity_count": len(canonical_ids),
            "expected_gap_root_ids": sorted(expected),
            "observed_gap_root_ids": sorted(observed),
            "unobserved_gap_root_ids": sorted(expected - observed),
            "all_expected_gap_roots_observed": expected == observed,
        }
    return {
        "generator_version": SUPPLEMENTAL_GRAMMAR_VERSION,
        "scope": "APPEND_ONLY_STRUCTURAL_DELTA_NO_BASE_PACK_REPLAY",
        "attempts_per_route": int(attempts_per_route),
        "seeds": [int(value) for value in seeds],
        "routes": routes,
        "existing_pack_rewrite_required": False,
        "global_exact_dedup_required_before_admission": True,
        "performance_or_reward_used": False,
        "data_roles_accessed": [],
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
        "claim_ceiling": "STRUCTURAL_DELTA_CAPACITY_IS_NOT_ALPHA_EVIDENCE",
    }


def build_field_root_review(
    master_rows: Iterable[Mapping[str, Any]],
    registry: UnifiedCapabilityRegistry,
    *,
    information_metrics: Iterable[Mapping[str, Any]],
    core_pack: Mapping[str, Any],
    capacity_review: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Apply candidate-factor-review discipline to pre-discovery field roots."""

    by_field = {field.field_id: field for field in registry.fields}
    metrics = {str(row["field_id"]): dict(row) for row in information_metrics}
    decisions = {
        str(row["field_id"]): dict(row)
        for row in core_pack.get("decisions", ())
    }
    core = set(str(value) for value in core_pack.get("selected_field_ids", ()))
    exposed_routes: dict[str, set[str]] = {}
    for route_id, route in capacity_review["routes"].items():
        for field_id in route["field_observation_counts"]:
            exposed_routes.setdefault(str(field_id), set()).add(str(route_id))

    output: list[dict[str, Any]] = []
    for source in master_rows:
        field_id = str(source["field_name"])
        capability = by_field.get(field_id)
        metric = metrics.get(field_id, {})
        core_decision = decisions.get(field_id, {})
        eligible_routes = (
            sorted(capability.allowed_routes)
            if capability is not None and capability.search_eligible
            else []
        )
        observed = sorted(exposed_routes.get(field_id, set()))
        selected = field_id in core
        if selected and capability is None:
            outcome = "REJECT_INVALID"
            reason = "CORE_ROOT_MISSING_FROM_UNIFIED_CAPABILITY_REGISTRY"
        elif selected and not capability.search_eligible:
            outcome = "HOLD_RESEARCH"
            reason = capability.blocked_reason or "REGISTRY_SEARCH_INELIGIBLE"
        elif selected and not set(observed).intersection(eligible_routes):
            outcome = "HOLD_RESEARCH"
            reason = "REGISTERED_CORE_ROOT_NOT_CONSUMED_BY_CURRENT_GENERATOR"
        elif selected:
            outcome = "ALLOW_KEEP_REVIEW"
            reason = "INFORMATION_QUALIFIED_AND_STRUCTURALLY_CONSUMED"
        elif capability is not None and observed and capability.field_role in {
            "condition-only",
            "state-only",
            "benchmark-only",
        }:
            outcome = "HOLD_RESEARCH"
            reason = "GENERATOR_SUPPORT_OR_CONTROL_ROOT_NOT_ALPHA_PAYLOAD"
        elif capability is not None and observed:
            outcome = "HOLD_RESEARCH"
            reason = "NON_CORE_ELIGIBLE_ROOT_AVAILABLE_OUTSIDE_FROZEN_CORE_SCOPE"
        else:
            outcome = "HOLD_RESEARCH"
            reason = str(source.get("blocked_reason") or "NOT_IN_CURRENT_DISCOVERY_SCOPE")
        output.append(
            {
                "field_uid": str(source["field_uid"]),
                "field_id": field_id,
                "source_table": str(source["source_table"]),
                "source_field": str(source["source_field"]),
                "data_family": str(source["data_family"]),
                "entity_scope": str(source["entity_scope"]),
                "semantic_role": str(source["semantic_role"]),
                "field_role": str(source["field_role"]),
                "observable_time": str(source["observable_time"]),
                "pit_status": str(source["pit_status"]),
                "materialization_status": str(source["materialization_status"]),
                "registry_present": capability is not None,
                "registry_search_eligible": bool(
                    capability is not None and capability.search_eligible
                ),
                "eligible_routes": eligible_routes,
                "generator_observed_routes": observed,
                "information_status": str(metric.get("information_status") or "NOT_EVALUATED"),
                "information_qualified": bool(metric.get("information_qualified", False)),
                "core_pack_selected": selected,
                "core_pack_decision": str(core_decision.get("decision") or "NOT_SELECTED"),
                "nearest_root_id": str(core_decision.get("representative_field_id") or ""),
                "nearest_root_nmi": float(core_decision.get("representative_nmi") or 0.0),
                "operator_path_status": (
                    "STRUCTURALLY_OBSERVED" if observed else "NOT_OBSERVED"
                ),
                "review_outcome": outcome,
                "review_reason": reason,
                "alpha_or_performance_claim": False,
            }
        )
    return sorted(output, key=lambda row: row["field_uid"])


def _add_support_roots(
    route_id: str,
    eligible: Sequence[CapabilityField],
    roots: set[str],
) -> dict[str, str]:
    reasons: dict[str, str] = {}
    for field in eligible:
        support_root = (
            route_id == "DISCLOSURE_EVENT"
            and field.field_role == "condition-only"
            and field.temporal_semantics == "DISCLOSURE_PULSE"
        ) or (
            route_id == "MARKET_REGIME_CONDITION"
            and field.entity_scope == "MARKET"
        ) or (
            route_id == "INTRADAY_STATE_TRANSITION"
            and field.field_role == "state-only"
            and _state_root_is_constructible(field)
        )
        if support_root:
            roots.add(field.field_id)
            reasons[field.field_id] = "MANDATORY_CONDITION_OR_STATE_ROOT"
        if route_id == "BROAD_EVENT_FROZEN_ENTRY":
            roots.add(field.field_id)
            reasons[field.field_id] = "FROZEN_REFERENCE_ROOT"
    if route_id == "SLOW_CROSS_SECTIONAL_LEVEL" and not any(
        "market_cap" in field_id.lower() for field_id in roots
    ):
        size_roots = sorted(
            field.field_id for field in eligible if "market_cap" in field.field_id.lower()
        )
        if not size_roots:
            raise ValueError("slow-level contract has no registered market-cap support root")
        roots.add(size_roots[0])
        reasons[size_roots[0]] = "GRAMMAR_SIZE_NORMALIZER_ROOT"
    return reasons


def build_frozen_discovery_contract(
    registry: UnifiedCapabilityRegistry,
    *,
    core_field_ids: Iterable[str],
    capacity_review: Mapping[str, Any],
    source_hashes: Mapping[str, str],
) -> dict[str, Any]:
    """Freeze root scope and guards without authorizing or budgeting a run."""

    core = set(str(value) for value in core_field_ids)
    registry_by_field = {field.field_id: field for field in registry.fields}
    route_allowlists: dict[str, list[str]] = {}
    route_root_roles: dict[str, dict[str, str]] = {}
    held_roots: dict[str, list[str]] = {}
    for route_id in ROUTE_IDS:
        eligible = registry.fields_for_route(route_id)
        route_review = capacity_review["routes"][route_id]
        structurally_blocked = set(route_review["structurally_unconstructible_root_ids"])
        roots = {
            field.field_id
            for field in eligible
            if field.field_id in core and field.field_id not in structurally_blocked
        }
        roles = {field_id: "INFORMATION_CORE_PAYLOAD_ROOT" for field_id in roots}
        support = _add_support_roots(route_id, eligible, roots)
        roles.update(support)
        route_allowlists[route_id] = sorted(roots)
        route_root_roles[route_id] = {
            field_id: roles.get(field_id, "INFORMATION_CORE_PAYLOAD_ROOT")
            for field_id in sorted(roots)
        }
        held_roots[route_id] = sorted(
            field.field_id
            for field in eligible
            if field.field_id in core and field.field_id not in roots
        )

    grammar = CompositionalGrammarV2(
        registry,
        route_root_allowlist=route_allowlists,
    )
    smoke: dict[str, Any] = {}
    for route_id in ROUTE_IDS:
        pairs = [
            grammar.propose(route_id, attempt_index=index, seed=20260718)
            for index in range(len(skeleton_registry()[route_id]))
        ]
        smoke[route_id] = {
            "skeleton_count": len(pairs),
            "all_primary_legal": all(bool(pair.primary.get("legal")) for pair in pairs),
            "all_controls_legal": all(bool(pair.control.get("legal")) for pair in pairs),
            "observed_root_ids": sorted(
                {
                    str(field_id)
                    for pair in pairs
                    for field_id in pair.primary.get("declared_field_ids", ())
                    if str(field_id) in set(route_allowlists[route_id])
                }
            ),
        }
        if not smoke[route_id]["all_primary_legal"] or not smoke[route_id]["all_controls_legal"]:
            raise ValueError(f"frozen root contract does not compile on {route_id}")

    body = {
        "contract_version": DISCOVERY_CONTRACT_VERSION,
        "status": "FROZEN_PREPARED_NOT_AUTHORIZED",
        "execution_authorized": False,
        "active_proposal_budget": 0,
        "active_admission_budget": 0,
        "active_strict_evaluation_budget": 0,
        "next_required_authority": "SEPARATE_DEVELOPMENT_ONLY_DISCOVERY_AUTHORIZATION",
        "registry_hash": registry.registry_hash,
        "source_hashes": dict(sorted(source_hashes.items())),
        "root_selection_policy": "INFORMATION_CORE_PLUS_MANDATORY_SUPPORT_FAIL_CLOSED",
        "route_root_allowlists": route_allowlists,
        "route_root_roles": route_root_roles,
        "held_core_roots": held_roots,
        "globally_blocked_core_roots": {
            field_id: (
                registry_by_field[field_id].blocked_reason
                if field_id in registry_by_field
                else "MISSING_FROM_UNIFIED_CAPABILITY_REGISTRY"
            )
            for field_id in sorted(core)
            if field_id not in registry_by_field
            or not registry_by_field[field_id].search_eligible
        },
        "route_smoke_validation": smoke,
        "data_access_contract": {
            "development_2024_2025": "ONLY_IF_SEPARATELY_AUTHORIZED",
            "validation": "FORBIDDEN",
            "holdout": "FORBIDDEN",
            "forward_2026": "SEALED",
            "reward_or_performance_used_to_select_roots": False,
        },
        "feedback_contract": {
            "candidate_promotion": "FORBIDDEN",
            "cross_epoch_memory": "FORBIDDEN",
            "adaptive_reward": "FORBIDDEN",
            "broad_event_pack": "FROZEN_UNMODIFIED",
        },
        "capability_exclusions": {
            "true1min_plate_sparse": "DISABLED_UNTIL_REAL_PIT_MINUTE_MATERIALIZATION_IS_AVAILABLE_ON_EXECUTION_HOST",
            "zygc_em": "PIT_CONTRACT_UNRESOLVED",
            "unqualified_units": "FAIL_CLOSED",
        },
        "claim_ceiling": "ROOT_SCOPE_AND_COMPILE_READINESS_ONLY_NO_ALPHA_CLAIM",
    }
    body["contract_hash"] = stable_hash(body)
    return body


def summarize_field_review(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    records = list(rows)
    core_rows = [row for row in records if row["core_pack_selected"]]
    return {
        "field_record_count": len(records),
        "core_root_count": len(core_rows),
        "core_review_outcomes": dict(
            sorted(Counter(str(row["review_outcome"]) for row in core_rows).items())
        ),
        "core_hold_reasons": dict(
            sorted(
                Counter(
                    str(row["review_reason"])
                    for row in core_rows
                    if row["review_outcome"] != "ALLOW_KEEP_REVIEW"
                ).items()
            )
        ),
        "all_review_outcomes": dict(
            sorted(Counter(str(row["review_outcome"]) for row in records).items())
        ),
        "review_does_not_promote_candidates": True,
        "performance_or_reward_used": False,
    }
