"""Bounded route-supply diagnosis for the next CN development campaign.

This module measures deterministic exact-unique constructor capacity.  It does
not evaluate returns, consume feedback, authorize formal search, or create a
second scheduling authority.  Broad Event remains a frozen reference lane.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from our_system_phase2.services.unified_capability_registry import ROUTE_IDS
from our_system_phase2.services.unified_discovery_generators import (
    RegistryDrivenGenerator,
)


FROZEN_REFERENCE_ONLY = "FROZEN_REFERENCE_ONLY"
PRIMARY_SEARCH = "PRIMARY_SEARCH"
PRIMARY_SEARCH_ROUTES = tuple(
    route_id for route_id in ROUTE_IDS if route_id != "BROAD_EVENT_FROZEN_ENTRY"
)
ROUTE_SEARCH_ROLES = {
    route_id: (
        FROZEN_REFERENCE_ONLY
        if route_id == "BROAD_EVENT_FROZEN_ENTRY"
        else PRIMARY_SEARCH
    )
    for route_id in ROUTE_IDS
}


def _classification(*, final_count: int, required: int, headroom: int) -> str:
    if final_count >= headroom:
        return "SEARCHABLE_SUPPLY_READY_WITH_HEADROOM"
    if final_count >= required:
        return "SEARCHABLE_SUPPLY_MINIMUM_ONLY"
    return "STABLE_EXACT_SUPPLY_BOTTLENECK"


def diagnose_exact_supply(
    generator: RegistryDrivenGenerator,
    *,
    route_ids: Sequence[str] = ROUTE_IDS,
    seeds: Sequence[int],
    attempt_caps: Sequence[int],
    required_pairs: int,
    headroom_multiplier: int = 2,
    historical_exact: set[str] | None = None,
) -> dict[str, Any]:
    """Measure exact supply under a fixed, deterministic attempt-cap ladder."""

    unknown = sorted(set(map(str, route_ids)) - set(ROUTE_IDS))
    if unknown:
        raise ValueError(f"unknown unified registry routes: {unknown}")
    normalized_seeds = tuple(int(seed) for seed in seeds)
    normalized_caps = tuple(sorted({int(cap) for cap in attempt_caps}))
    required = int(required_pairs)
    headroom = required * int(headroom_multiplier)
    if not normalized_seeds or not normalized_caps:
        raise ValueError("supply diagnosis requires seeds and attempt caps")
    if normalized_caps[0] <= 0 or required <= 0 or headroom < required:
        raise ValueError("invalid supply diagnosis budget contract")

    routes: dict[str, dict[str, Any]] = {}
    for route_id in map(str, route_ids):
        role = ROUTE_SEARCH_ROLES[route_id]
        if role == FROZEN_REFERENCE_ONLY:
            routes[route_id] = {
                "route_id": route_id,
                "search_role": role,
                "classification": "FROZEN_REFERENCE_ONLY_NOT_SEARCH_SUPPLY",
                "blocks_next_development_phase": False,
                "required_pairs": required,
                "required_headroom_pairs": headroom,
                "seed_ladders": [],
                "minimum_final_exact_unique_pairs": 0,
            }
            continue

        ladders: list[dict[str, Any]] = []
        for seed in normalized_seeds:
            levels: list[dict[str, Any]] = []
            for cap in normalized_caps:
                _, funnel = generator.generate_route_attempts(
                    route_id,
                    scheduled_pairs=normalized_caps[-1],
                    seed=seed,
                    attempt_limit=cap,
                    existing_exact_identities=set(historical_exact or ()),
                )
                levels.append(
                    {
                        "attempt_cap": cap,
                        "generation_attempts": int(funnel["generation_attempts"]),
                        "legal_pairs": int(funnel["legal_pairs"]),
                        "exact_unique_pairs": int(funnel["exact_unique_pairs"]),
                        "exact_duplicate_pairs": int(
                            funnel["exact_duplicate_pairs"]
                        ),
                        "materialization_unsupported_pairs": int(
                            funnel.get("materialization_unsupported_pairs") or 0
                        ),
                    }
                )
            ladders.append(
                {
                    "seed": seed,
                    "levels": levels,
                    "final_exact_unique_pairs": levels[-1]["exact_unique_pairs"],
                    "last_step_increment": levels[-1]["exact_unique_pairs"]
                    - (levels[-2]["exact_unique_pairs"] if len(levels) > 1 else 0),
                }
            )
        minimum_final = min(row["final_exact_unique_pairs"] for row in ladders)
        classification = _classification(
            final_count=minimum_final,
            required=required,
            headroom=headroom,
        )
        routes[route_id] = {
            "route_id": route_id,
            "search_role": role,
            "classification": classification,
            "blocks_next_development_phase": classification
            == "STABLE_EXACT_SUPPLY_BOTTLENECK",
            "required_pairs": required,
            "required_headroom_pairs": headroom,
            "minimum_final_exact_unique_pairs": minimum_final,
            "all_seeds_saturated_at_final_cap": all(
                int(row["last_step_increment"]) == 0 for row in ladders
            ),
            "seed_ladders": ladders,
        }
    return {
        "schema_version": "cn_route_exact_supply_diagnosis_v1",
        "top_level_scheduling_key": "unified_registry_route_id",
        "generator_authority": "RegistryDrivenGenerator",
        "constructor_profile": generator.constructor_profile,
        "generator_version": generator.generator_version,
        "attempt_caps": list(normalized_caps),
        "seeds": list(normalized_seeds),
        "required_pairs": required,
        "required_headroom_pairs": headroom,
        "historical_exact_identity_count": len(historical_exact or ()),
        "routes": routes,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }


def classify_clamped_route(
    *,
    route_id: str,
    legacy_classification: str,
    upgraded_classification: str,
    prior_feedback_status: str,
) -> dict[str, Any]:
    """Separate a campaign-local clamp from a stable searchable bottleneck."""

    if prior_feedback_status != "ACTIONABLE_FEEDBACK_CLAMPED":
        return {
            "route_id": route_id,
            "prior_feedback_status": prior_feedback_status,
            "root_cause": "NOT_A_CLAMPED_ROUTE",
            "repair_status": "NOT_APPLICABLE",
            "blocks_next_development_phase": False,
        }
    upgraded_ready = upgraded_classification in {
        "SEARCHABLE_SUPPLY_READY_WITH_HEADROOM",
        "SEARCHABLE_SUPPLY_MINIMUM_ONLY",
    }
    legacy_bottleneck = legacy_classification == "STABLE_EXACT_SUPPLY_BOTTLENECK"
    if legacy_bottleneck and upgraded_ready:
        root_cause = "LEGACY_CONSTRUCTOR_CYCLE_EXHAUSTION"
        repair_status = "REPAIRED_BY_REGISTRY_COMPOSITIONAL_PROFILE"
        blocks = False
    elif upgraded_ready:
        root_cause = "CAMPAIGN_LOCAL_ARCHIVE_OR_SMALL_BUDGET_CLAMP"
        repair_status = "NO_STABLE_SEARCHABLE_SUPPLY_BOTTLENECK_OBSERVED"
        blocks = False
    else:
        root_cause = "STABLE_REGISTRY_CONSTRUCTOR_SUPPLY_BOTTLENECK"
        repair_status = "UNRESOLVED"
        blocks = True
    return {
        "route_id": route_id,
        "prior_feedback_status": prior_feedback_status,
        "legacy_classification": legacy_classification,
        "upgraded_classification": upgraded_classification,
        "root_cause": root_cause,
        "repair_status": repair_status,
        "blocks_next_development_phase": blocks,
    }


def compare_clamped_routes(
    *,
    prior_route_comparison: Mapping[str, Mapping[str, Any]],
    legacy_report: Mapping[str, Any],
    upgraded_report: Mapping[str, Any],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    legacy_routes = dict(legacy_report.get("routes") or {})
    upgraded_routes = dict(upgraded_report.get("routes") or {})
    for route_id, prior in sorted(prior_route_comparison.items()):
        status = str(prior.get("feedback_exposure_status") or "")
        if status != "ACTIONABLE_FEEDBACK_CLAMPED":
            continue
        output.append(
            classify_clamped_route(
                route_id=str(route_id),
                legacy_classification=str(
                    (legacy_routes.get(route_id) or {}).get("classification") or ""
                ),
                upgraded_classification=str(
                    (upgraded_routes.get(route_id) or {}).get("classification") or ""
                ),
                prior_feedback_status=status,
            )
        )
    return output
