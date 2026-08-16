from __future__ import annotations

from pathlib import Path

import app

from our_system_phase2.runtime.cn_program_optimizer_d1_development_v1 import (
    AUTHORIZATION_RELATIVE_PATH,
    CAMPAIGN_ID,
    CAMPAIGN_PROFILE,
    D1_LOGICAL_RECORDS,
    PRIOR_EXACT_COUNT,
    PRIOR_EXACT_IDENTITIES_SHA256,
    PRIOR_FREEZE_PAYLOAD_SHA256,
    PRIOR_FREEZE_RELATIVE_PATH,
    REMAINING_PROSPECTIVE_ENHANCED,
    ROUTE_ID,
    authorization_payload_v1,
    verify_authorization,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
)
from our_system_phase2.services.program_optimizer_d1_cohort_v1 import (
    D1_SELECTION_SURROGATE,
    D1_SELECTION_TPE,
    D1_SELECTION_UNIFORM,
    D1_SELECTOR_COUNTS,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    STRUCTURED_SURROGATE_PROGRAM,
    UNIFORM_CONTROL,
)
from scripts.run_cn_program_optimizer_d1_development_v1 import _generation_arm
from scripts.run_cn_program_optimizer_successor_benchmark_v1 import _read_prior_freeze


REPO = Path(__file__).resolve().parents[1]


def test_d1_authorization_is_byte_semantically_bound_to_frozen_policy() -> None:
    path = REPO / AUTHORIZATION_RELATIVE_PATH
    observed = verify_authorization(path)
    assert observed == authorization_payload_v1(REPO)
    assert observed["campaign_id"] == CAMPAIGN_ID
    assert observed["campaign_profile"] == CAMPAIGN_PROFILE
    assert observed["project_control_route_id"] == ROUTE_ID
    assert observed["policy_binding"]["logical_records"] == D1_LOGICAL_RECORDS
    assert observed["policy_binding"]["selector_counts"] == D1_SELECTOR_COUNTS
    assert observed["program_space"]["prior_exact_count"] == PRIOR_EXACT_COUNT
    assert (
        observed["program_space"]["prior_exact_identities_sha256"]
        == PRIOR_EXACT_IDENTITIES_SHA256
    )
    assert (
        observed["program_space"]["remaining_prospective_enhanced_exact_count"]
        == REMAINING_PROSPECTIVE_ENHANCED
    )
    assert observed["oos_authority"] == "NONE"
    assert observed["promotion_authorized"] is False


def test_shared_prior_loader_normalizes_d1_combined_identity_field_in_memory() -> None:
    prior = _read_prior_freeze(
        REPO / PRIOR_FREEZE_RELATIVE_PATH,
        expected_payload_sha256=PRIOR_FREEZE_PAYLOAD_SHA256,
        expected_count=PRIOR_EXACT_COUNT,
        expected_exact_identities_sha256=PRIOR_EXACT_IDENTITIES_SHA256,
        identity_field="combined_prior_exact_identities",
    )
    assert len(prior["prior_exact_identities"]) == PRIOR_EXACT_COUNT
    assert prior["prior_exact_identities"] == prior["combined_prior_exact_identities"]


def test_d1_route_is_high_cost_and_campaign_authorization_bound() -> None:
    assert app.ROUTES[ROUTE_ID] == (
        "our_system_phase2.runtime.cn_program_optimizer_d1_development_v1"
    )
    assert app.HIGH_COST_ROUTE_ACTIONS[ROUTE_ID] == {ACTION_LAUNCH, ACTION_RETRY}
    assert ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_d1_physical_provenance_uses_existing_generation_arms() -> None:
    assert _generation_arm(D1_SELECTION_UNIFORM) == UNIFORM_CONTROL
    assert _generation_arm(D1_SELECTION_TPE) == HYBRID_TPE_PROGRAM
    assert _generation_arm(D1_SELECTION_SURROGATE) == STRUCTURED_SURROGATE_PROGRAM
