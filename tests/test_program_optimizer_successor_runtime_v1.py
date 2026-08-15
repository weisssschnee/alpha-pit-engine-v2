from pathlib import Path

import app

from our_system_phase2.runtime import cn_program_optimizer_successor_benchmark_v1 as runtime
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
)


ROOT = Path(__file__).resolve().parents[1]


def test_successor_authorization_is_exact_and_development_only() -> None:
    path = ROOT / runtime.AUTHORIZATION_RELATIVE_PATH
    payload = runtime.verify_authorization(path)
    assert payload == runtime.authorization_payload_v1(ROOT)
    assert payload["evaluation_data_role"] == "DEVELOPMENT_ONLY"
    assert payload["program_space"]["full_count"] == 3616
    assert payload["program_space"]["base_count"] == 32
    assert payload["program_space"]["enhanced_count"] == 3584
    assert payload["program_space"]["prior_exact_count"] == 504
    assert payload["program_space"]["prospective_enhanced_available_count"] == 3080
    assert payload["logical_design"]["total_logical_records"] == 560
    assert payload["logical_design"]["logical_records_per_policy"] == 140
    assert payload["logical_design"]["post_bootstrap_denominator"] == 112
    assert payload["logical_design"]["policy_specific_optimized_denominator"] == 84
    assert payload["logical_design"]["cross_policy_virtual_depletion"] is False
    assert payload["logical_design"]["physical_exact_deduplication"] is True
    assert payload["d2"]["gate_outside_action"] == "NON_ECONOMIC_TPE_FAIL_AND_REASK"
    assert payload["d2"]["global_fallback"] is False
    assert payload["oos_authority"] == "NONE"
    assert payload["promotion_authorized"] is False


def test_successor_route_is_high_cost_and_authorization_bound() -> None:
    route = runtime.ROUTE_ID
    assert app.ROUTES[route] == (
        "our_system_phase2.runtime.cn_program_optimizer_successor_benchmark_v1"
    )
    assert app.HIGH_COST_ROUTE_ACTIONS[route] == frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    )
    assert route in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_prior_freeze_is_exact_504_enhanced_programs() -> None:
    payload = runtime._load_prior_freeze(ROOT)
    ids = payload["prior_exact_identities"]
    assert len(ids) == 504
    assert len(set(ids)) == 504
    assert payload["prior_exact_identities_sha256"] == runtime.PRIOR_EXACT_IDENTITIES_SHA256
    assert payload["per_arm"] == {
        "HYBRID_TPE_PROGRAM": 168,
        "STRUCTURED_SURROGATE_PROGRAM": 168,
        "UNIFORM_CONTROL": 168,
    }
    assert set(payload["per_template"].values()) == {72}
