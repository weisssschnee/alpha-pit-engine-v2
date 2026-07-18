from __future__ import annotations

import json
from pathlib import Path

from our_system_phase2.services.core_pack_generator_review import (
    audit_generator_capacity,
    audit_supplemental_generator_delta,
    build_field_root_review,
    build_frozen_discovery_contract,
    summarize_field_review,
)
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY_PATH = REPO / "runtime/field_registry/cn_unified_capability_registry_v3_20260717/unified_capability_registry.json"
MASTER_PATH = REPO / "runtime/field_registry/cn_field_master_registry_v1/cn_field_master_registry_v1.json"
INFORMATION_ROOT = REPO / "runtime/cn_full_field_information_research_v1_20260717"


def _inputs() -> tuple[UnifiedCapabilityRegistry, dict, dict, list[dict]]:
    registry = UnifiedCapabilityRegistry.read(REGISTRY_PATH)
    master = json.loads(MASTER_PATH.read_text(encoding="utf-8"))
    core = json.loads((INFORMATION_ROOT / "core_pack.json").read_text(encoding="utf-8"))
    metrics = json.loads(
        (INFORMATION_ROOT / "capability_information_metrics.json").read_text(encoding="utf-8")
    )
    return registry, master, core, metrics


def test_capacity_review_separates_base_observation_from_supplemental_reachability() -> None:
    registry, _, core, _ = _inputs()
    review = audit_generator_capacity(
        registry,
        core_field_ids=core["selected_field_ids"],
        attempts_by_route={route_id: 16 for route_id in ROUTE_IDS},
        seeds=[20260718],
        checkpoints=[8, 16],
    )

    assert review["economic_evaluator_accessed"] is False
    assert review["data_roles_accessed"] == []
    assert review["behavior_identity_status"] == "NOT_EVALUATED"
    assert set(review["routes"]) == set(ROUTE_IDS)
    for route_id, field_id in (
        ("INTRADAY_STATE_TRANSITION", "state_close_range_location_sign"),
        ("SLOW_CROSS_SECTIONAL_LEVEL", "fund_disclosure_balance_age_sessions"),
        ("MARKET_REGIME_CONDITION", "ctx_hfq_is_st"),
    ):
        route = review["routes"][route_id]
        assert field_id in route["structurally_unconstructible_root_ids"]

    delta = audit_supplemental_generator_delta(
        registry,
        attempts_per_route=64,
        seeds=(20260718,),
    )
    assert delta["existing_pack_rewrite_required"] is False
    assert delta["global_exact_dedup_required_before_admission"] is True
    assert delta["data_roles_accessed"] == []
    assert all(
        row["all_expected_gap_roots_observed"]
        and row["valid_pair_count"] == row["attempts"]
        for row in delta["routes"].values()
    )
    observed = {
        field_id
        for row in delta["routes"].values()
        for field_id in row["observed_gap_root_ids"]
    }
    assert observed == {
        "state_close_range_location_sign",
        "fund_disclosure_balance_age_sessions",
        "fund_disclosure_profit_age_sessions",
        "fund_disclosure_cashflow_age_sessions",
        "fund_disclosure_holder_age_sessions",
        "ctx_hfq_is_st",
        "ctx_hfq_prev_is_limit_up",
    }


def test_frozen_contract_uses_core_plus_support_and_authorizes_nothing() -> None:
    registry, master, core, metrics = _inputs()
    review = audit_generator_capacity(
        registry,
        core_field_ids=core["selected_field_ids"],
        attempts_by_route={route_id: 16 for route_id in ROUTE_IDS},
        seeds=[20260718],
        checkpoints=[16],
    )
    contract = build_frozen_discovery_contract(
        registry,
        core_field_ids=core["selected_field_ids"],
        capacity_review=review,
        source_hashes={"test": "hash"},
    )

    assert contract["execution_authorized"] is False
    assert contract["active_proposal_budget"] == 0
    assert contract["active_strict_evaluation_budget"] == 0
    assert set(contract["route_root_allowlists"]) == set(ROUTE_IDS)
    assert "state_close_range_location_sign" in contract["held_core_roots"][
        "INTRADAY_STATE_TRANSITION"
    ]
    assert contract["globally_blocked_core_roots"]["vol"] == (
        "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED"
    )
    assert not any(
        field_id.startswith("plate_")
        for field_id in contract["route_root_allowlists"]["MINUTE_STATIC"]
    )
    assert "fund_disclosure_balance_age_sessions" not in contract[
        "route_root_allowlists"
    ]["SLOW_CROSS_SECTIONAL_LEVEL"]
    assert "ctx_hfq_is_st" not in contract["route_root_allowlists"][
        "MARKET_REGIME_CONDITION"
    ]
    assert all(
        row["all_primary_legal"] and row["all_controls_legal"]
        for row in contract["route_smoke_validation"].values()
    )

    rows = build_field_root_review(
        master["rows"],
        registry,
        information_metrics=metrics,
        core_pack=core,
        capacity_review=review,
    )
    summary = summarize_field_review(rows)
    assert len(rows) == 1683
    assert summary["core_root_count"] == 272
    assert summary["review_does_not_promote_candidates"] is True
