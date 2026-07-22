from __future__ import annotations

import app

from scripts.run_cn_core_pack_authority_smoke import (
    PAIR_BUDGET_PER_ROUTE,
    SMOKE_ROUTES,
    SUPPLY_PROBE_PAIRS_PER_ROUTE,
)
from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _causal_route_comparison,
    main as iterative_main,
)
from our_system_phase2.services.multi_arm_scheduler import (
    build_medium_campaign_schedule,
    build_route_schedule,
)
from our_system_phase2.services.unified_capability_registry import ROUTE_IDS


def _row(route_id: str, **values: float) -> dict[str, object]:
    return {
        "route_id": route_id,
        "actionable_support": 12,
        "positive_matched_density": 0.5,
        "median_matched_reward": 0.0,
        "new_signal_cluster_rate": 0.5,
        "new_portfolio_behavior_rate": 0.5,
        "cost_conversion_rate": 0.0,
        "turnover_killed_rate": 0.0,
        "behavior_duplicate_rate": 0.0,
        "semantic_wrong_lag_high_corr_rate": 0.0,
        **values,
    }


def test_route_scheduler_uses_registry_route_as_top_level_key() -> None:
    rows, summary = build_route_schedule(
        [_row(route) for route in ROUTE_IDS],
        total_pairs=48,
        admission_pairs=24,
    )

    assert {row["route_id"] for row in rows} == set(ROUTE_IDS)
    assert sum(int(row["scheduled_pairs"]) for row in rows) == 48
    assert sum(int(row["admission_pair_budget"]) for row in rows) == 24
    assert max(int(row["scheduled_pairs"]) for row in rows) <= 12
    assert summary["top_level_scheduling_key"] == "unified_registry_route_id"
    assert summary["legacy_default_arm_profiles"] == "COMPATIBILITY_ONLY"


def test_medium_campaign_scheduler_holds_route_diversity_bounds() -> None:
    base = {
        "INTRADAY_STATE_TRANSITION": 48,
        "SLOW_CROSS_SECTIONAL_LEVEL": 43,
        "MINUTE_STATIC": 38,
        "SLOW_TEMPORAL_CHANGE": 38,
        "FIRSTN_PATH": 32,
        "MARKET_REGIME_CONDITION": 32,
        "DISCLOSURE_EVENT": 25,
    }
    rows, summary = build_medium_campaign_schedule(
        [_row(route_id, actionable_support=0) for route_id in ROUTE_IDS],
        base_targets=base,
        total_pairs=256,
    )
    budgets = {str(row["route_id"]): int(row["scheduled_pairs"]) for row in rows}

    assert sum(budgets.values()) == 256
    assert budgets["INTRADAY_STATE_TRANSITION"] >= 36
    assert budgets["SLOW_TEMPORAL_CHANGE"] >= 26
    assert budgets["FIRSTN_PATH"] >= 21
    assert budgets["MARKET_REGIME_CONDITION"] >= 21
    assert budgets["DISCLOSURE_EVENT"] >= 16
    assert max(budgets.values()) <= 64
    assert budgets["MINUTE_STATIC"] + budgets["SLOW_CROSS_SECTIONAL_LEVEL"] <= 128
    assert summary["temporal_event_share"] >= 0.5


def test_iterative_canary_is_exposed_through_narrow_app_route(capsys) -> None:
    assert app.ROUTES["cn-iterative-search-v1-canary"] == (
        "our_system_phase2.runtime.cn_iterative_search_v1"
    )
    assert iterative_main(["--synthetic-rules-only"]) == 0
    assert '"status": "PASS"' in capsys.readouterr().out
    assert app.ROUTES["cn-targeted-search-medium-campaign"] == (
        "our_system_phase2.runtime.cn_targeted_search_medium_campaign"
    )
    assert app.ROUTES["cn-core-pack-authority-smoke"] == (
        "scripts.run_cn_core_pack_authority_smoke"
    )
    assert set(SMOKE_ROUTES) == set(ROUTE_IDS) - {"BROAD_EVENT_FROZEN_ENTRY"}
    assert len(SMOKE_ROUTES) * PAIR_BUDGET_PER_ROUTE == 28
    assert SUPPLY_PROBE_PAIRS_PER_ROUTE == 12


def test_causal_gate_requires_actual_route_distribution_to_follow_schedule() -> None:
    on_budget = {route_id: 6 for route_id in ROUTE_IDS}
    off_budget = dict(on_budget)
    on_actual = {route_id: 6 for route_id in ROUTE_IDS}
    off_actual = dict(on_actual)
    on_budget["MINUTE_STATIC"] = 8
    on_actual["MINUTE_STATIC"] = 8
    actions = {route_id: "MAINTAIN" for route_id in ROUTE_IDS}
    actions["MINUTE_STATIC"] = "EXPAND"
    comparison, gates = _causal_route_comparison(
        feedback_on_budgets=on_budget,
        feedback_off_budgets=off_budget,
        feedback_on_actual=on_actual,
        feedback_off_actual=off_actual,
        feedback_on_actions=actions,
        feedback_on_funnel={},
        feedback_off_funnel={},
    )
    assert all(value is True for key, value in gates.items() if key not in {
        "real_positive_direction_status", "real_negative_direction_status"
    })
    assert comparison["MINUTE_STATIC"]["feedback_exposure_status"] == "APPLIED"


def test_causal_gate_excludes_maintain_spillover_and_records_supply_clamp() -> None:
    on_budget = {route_id: 6 for route_id in ROUTE_IDS}
    off_budget = dict(on_budget)
    on_actual = {route_id: 6 for route_id in ROUTE_IDS}
    off_actual = dict(on_actual)
    actions = {route_id: "MAINTAIN" for route_id in ROUTE_IDS}
    actions["INTRADAY_STATE_TRANSITION"] = "EXPAND"
    actions["SLOW_CROSS_SECTIONAL_LEVEL"] = "DOWNWEIGHT"
    on_budget["INTRADAY_STATE_TRANSITION"] = 10
    on_actual["INTRADAY_STATE_TRANSITION"] = 2
    off_actual["INTRADAY_STATE_TRANSITION"] = 2
    on_budget["SLOW_CROSS_SECTIONAL_LEVEL"] = 4
    on_actual["SLOW_CROSS_SECTIONAL_LEVEL"] = 8
    off_actual["SLOW_CROSS_SECTIONAL_LEVEL"] = 12
    on_actual["FIRSTN_PATH"] = 12
    off_actual["FIRSTN_PATH"] = 7
    comparison, gates = _causal_route_comparison(
        feedback_on_budgets=on_budget,
        feedback_off_budgets=off_budget,
        feedback_on_actual=on_actual,
        feedback_off_actual=off_actual,
        feedback_on_actions=actions,
        feedback_on_funnel={
            "INTRADAY_STATE_TRANSITION": {
                "underfill_reason": "EXACT_UNIQUE_STREAM_UNDERFILL"
            }
        },
        feedback_off_funnel={},
    )
    assert comparison["INTRADAY_STATE_TRANSITION"]["feedback_exposure_status"] == (
        "ACTIONABLE_FEEDBACK_CLAMPED"
    )
    assert comparison["SLOW_CROSS_SECTIONAL_LEVEL"]["feedback_exposure_status"] == (
        "APPLIED"
    )
    assert comparison["FIRSTN_PATH"]["feedback_exposure_status"] == (
        "SPILLOVER_ONLY_NOT_FEEDBACK"
    )
    assert gates["real_positive_direction_status"] == "ACTIONABLE_FEEDBACK_CLAMPED"
    assert gates["real_negative_direction_status"] == "APPLIED"
    assert gates["maintain_spillover_excluded_from_feedback"] is True


def test_positive_and_negative_rules_expand_and_contract_synthetically() -> None:
    positive = _row(
        "MINUTE_STATIC",
        positive_matched_density=0.9,
        median_matched_reward=0.2,
        new_signal_cluster_rate=0.9,
        new_portfolio_behavior_rate=0.9,
    )
    negative = _row(
        "FIRSTN_PATH",
        positive_matched_density=0.0,
        median_matched_reward=-0.2,
        cost_conversion_rate=0.7,
        turnover_killed_rate=0.8,
    )
    neutral = [_row(route) for route in ROUTE_IDS if route not in {"MINUTE_STATIC", "FIRSTN_PATH"}]
    rows, _ = build_route_schedule(
        [positive, negative, *neutral],
        total_pairs=48,
        admission_pairs=24,
    )
    by_route = {str(row["route_id"]): row for row in rows}

    assert by_route["MINUTE_STATIC"]["scheduler_action"] == "EXPAND"
    assert by_route["FIRSTN_PATH"]["scheduler_action"] in {"DOWNWEIGHT", "REPAIR"}
    assert int(by_route["MINUTE_STATIC"]["scheduled_pairs"]) > int(
        by_route["FIRSTN_PATH"]["scheduled_pairs"]
    )


def test_infrastructure_failure_is_not_a_route_health_input() -> None:
    baseline, _ = build_route_schedule(
        [_row(route) for route in ROUTE_IDS], total_pairs=48, admission_pairs=24
    )
    with_infrastructure = [
        {**_row(route), "infrastructure_failure_rate": 1.0} for route in ROUTE_IDS
    ]
    observed, _ = build_route_schedule(
        with_infrastructure, total_pairs=48, admission_pairs=24
    )
    assert baseline == observed


def test_freeze_requires_semantic_or_exact_behavior_evidence() -> None:
    rows = [_row(route) for route in ROUTE_IDS]
    rows[0] = _row(
        "MINUTE_STATIC",
        positive_matched_density=0.0,
        median_matched_reward=-1.0,
        turnover_killed_rate=1.0,
    )
    scheduled, _ = build_route_schedule(rows, total_pairs=48, admission_pairs=24)
    minute = next(row for row in scheduled if row["route_id"] == "MINUTE_STATIC")
    assert minute["scheduler_action"] != "FREEZE"
