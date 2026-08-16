from __future__ import annotations

import app

from scripts.run_cn_program_optimizer_d1_transfer_prospective_validation_v1 import _acceptance
from our_system_phase2.runtime.cn_program_optimizer_d1_transfer_prospective_validation_v1 import ROUTE_ID
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
)


def _row(productive: bool, admitted: bool = True) -> dict:
    return {
        "admission": {"admitted": admitted},
        "validation_productive": productive,
    }


def _contract() -> dict:
    return {
        "minimum_development_productive_population": 30,
        "filtered_precision_minimum": 0.35,
        "filtered_minus_unfiltered_precision_minimum": 0.10,
        "filtered_recall_minimum": 0.60,
        "all_conditions_required": True,
        "rule_change_after_B_development_or_validation": "FORBIDDEN",
    }


def test_prospective_transfer_acceptance_passes_only_frozen_joint_gate() -> None:
    all_rows = [_row(i < 8) for i in range(30)]
    selected = [_row(i < 6) for i in range(12)]
    verdict = _acceptance(all_rows=all_rows, selected_rows=selected, contract=_contract())
    assert verdict["status"] == "PASS"
    assert verdict["all_development_positive_precision"] == 8 / 30
    assert verdict["filtered_precision"] == 6 / 12
    assert verdict["filtered_recall"] == 6 / 8
    assert verdict["filtered_minus_unfiltered_precision"] > 0.10
    assert all(verdict["checks"].values())


def test_prospective_transfer_acceptance_fails_when_precision_lift_is_too_small() -> None:
    all_rows = [_row(i < 8) for i in range(30)]
    selected = [_row(i < 4) for i in range(12)]
    verdict = _acceptance(all_rows=all_rows, selected_rows=selected, contract=_contract())
    assert verdict["status"] == "FAIL_PROSPECTIVE_FILTER_TEST"
    assert verdict["checks"]["filtered_precision_minimum"] is False
    assert verdict["checks"]["filtered_minus_unfiltered_precision_minimum"] is False


def test_prospective_transfer_validation_route_is_high_cost_and_campaign_bound() -> None:
    assert app.ROUTES[ROUTE_ID] == (
        "our_system_phase2.runtime.cn_program_optimizer_d1_transfer_prospective_validation_v1"
    )
    assert app.HIGH_COST_ROUTE_ACTIONS[ROUTE_ID] == {ACTION_LAUNCH, ACTION_RETRY}
    assert ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES
