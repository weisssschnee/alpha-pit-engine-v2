from __future__ import annotations

from pathlib import Path

import pytest

import app
from our_system_phase2.runtime import cn_program_primitive_local_stage_b_benchmark_v1 as runtime
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
    ProjectControlDenied,
)
from scripts import run_cn_program_primitive_local_stage_b_benchmark_v1 as runner

REPO = Path(__file__).resolve().parents[1]
AUTH = REPO / runtime.AUTHORIZATION_RELATIVE_PATH
POLICY = REPO / "runtime/run_plans/cn_program_primitive_local_stage_b_policy_v1.json"


def _result(exact: str, productive: bool) -> dict:
    return {
        "exact_identity": exact,
        "admission": {"admitted": productive},
        "uplift": {
            "program_credit": {
                "matched_cumulative_net_return_increment": 1.0 if productive else -1.0,
                "matched_net_reward_increment": 1.0 if productive else -1.0,
            }
        } if productive else None,
    }


def test_route_is_high_cost_and_authorization_bound() -> None:
    assert app.ROUTES[runtime.ROUTE_ID] == "our_system_phase2.runtime.cn_program_primitive_local_stage_b_benchmark_v1"
    assert app.HIGH_COST_ROUTE_ACTIONS[runtime.ROUTE_ID] == {ACTION_LAUNCH, ACTION_RETRY}
    assert runtime.ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_frozen_policy_and_authorization_verify() -> None:
    policy = runner.verify_policy(POLICY)
    authorization = runtime.verify_authorization(AUTH, repo_root=REPO)
    assert policy["spent_freeze"]["combined_spent_exact_count"] == 2438
    assert policy["spent_freeze"]["stage_b_overlap_count"] == 0
    assert authorization["search_policy"]["payload_sha256"] == policy["policy_payload_sha256"]
    assert authorization["benchmark_contract"]["physical_stage_b_evaluations"] == 264


def test_policy_orders_cover_same_264_exact_space() -> None:
    policy = runner.verify_policy(POLICY)
    primitive = policy["policies"][runner.POLICY_ID]["orders_by_temporal"]
    family = policy["policies"][runner.FAMILY_POLICY_ID]["orders_by_temporal"]
    uniforms = policy["policies"][runner.UNIFORM_POLICY_ID]["orders_by_seed_and_temporal"]
    exacts = set()
    for temporal, order in primitive.items():
        assert len(order) == 44
        assert len(set(order)) == 44
        assert set(family[temporal]) == set(order)
        for seed_orders in uniforms.values():
            assert set(seed_orders[temporal]) == set(order)
        exacts.update(order)
    assert len(exacts) == 264


def test_policy_curve_gate_can_distinguish_search_advantage() -> None:
    policy = runner.verify_policy(POLICY)
    primitive = policy["policies"][runner.POLICY_ID]["orders_by_temporal"]
    result_by_exact = {}
    for temporal, order in primitive.items():
        for index, exact in enumerate(order):
            result_by_exact[exact] = _result(exact, productive=index < 14)
    curve = runner._policy_curves(policy, result_by_exact)
    assert curve["budgets"]["72"]["primitive_local"]["productive"] == 72
    assert curve["status"] in {
        "PRIMITIVE_LOCAL_SEARCH_TRANSFER_PASS_STAGE_C_LARGE_SCALE_AUTHORIZATION_ELIGIBLE",
        "PRIMITIVE_LOCAL_SEARCH_TRANSFER_FAIL_NO_LARGE_SCALE_CLAIM",
    }


def test_direct_runtime_invocation_is_denied_before_argument_parsing() -> None:
    with pytest.raises(ProjectControlDenied, match="DIRECT_HIGH_COST_MODULE_EXECUTION_FORBIDDEN"):
        runtime.main([])
