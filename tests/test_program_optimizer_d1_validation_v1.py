from __future__ import annotations

from types import MappingProxyType
from pathlib import Path

import app

from scripts.prepare_cn_program_optimizer_d1_report_only_validation_v1 import (
    EXPECTED_CANDIDATE_COUNT,
    EXPECTED_CANDIDATE_EXACT_SHA256,
    VALIDATION_WINDOWS,
    _load_frozen_members,
)
from scripts.run_cn_program_optimizer_d1_report_only_validation_v1 import (
    _validation_productive,
)
from our_system_phase2.runtime.cn_program_optimizer_d1_report_only_validation_v1 import (
    AUTHORIZATION_RELATIVE_PATH,
    ROUTE_ID,
    verify_authorization,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import ProgramUpliftCredit


REPO = Path(__file__).resolve().parents[1]


def test_d1_validation_candidate_freeze_is_fixed_before_validation() -> None:
    freeze, members = _load_frozen_members(REPO)
    assert freeze["status"] == "FROZEN_BEFORE_VALIDATION_ACCESS"
    assert len(members) == EXPECTED_CANDIDATE_COUNT == 120
    assert len({row["exact_identity"] for row in members}) == 120
    assert freeze["frozen_candidate_exact_identities_sha256"] == EXPECTED_CANDIDATE_EXACT_SHA256
    assert freeze["selection_rule"]["ranking"] == "NONE_ALL_ELIGIBLE_INCLUDED"
    assert freeze["selection_rule"]["post_hoc_top_k"] == "FORBIDDEN"
    assert freeze["validation_reads_performed_by_freeze"] == 0
    assert freeze["validation_contract"]["optimizer_feedback_write"] == "FORBIDDEN"
    assert freeze["validation_contract"]["promotion"] == "FORBIDDEN"


def test_d1_validation_windows_are_frozen_24_24_25() -> None:
    assert tuple(row["session_count"] for row in VALIDATION_WINDOWS) == (24, 24, 25)
    assert sum(row["session_count"] for row in VALIDATION_WINDOWS) == 73
    assert VALIDATION_WINDOWS[0]["start_date"] == "2025-07-08"
    assert VALIDATION_WINDOWS[-1]["end_date"] == "2025-10-24"
    assert [row["window_id"] for row in VALIDATION_WINDOWS] == [
        "validation_1",
        "validation_2",
        "validation_3",
    ]


def _admission(admitted: bool) -> AbsoluteEconomicAdmission:
    return AbsoluteEconomicAdmission(
        record_payload_sha256="record",
        pair_id="pair",
        program_id="program",
        control_program_id="control",
        admitted=admitted,
        failure_reasons=() if admitted else ("FAIL",),
        metrics={},
    )


def _uplift(return_increment: float, reward_increment: float) -> ProgramUpliftCredit:
    return ProgramUpliftCredit(
        record_payload_sha256="record",
        pair_id="pair",
        program_id="program",
        control_program_id="control",
        program_credit=MappingProxyType(
            {
                "matched_cumulative_net_return_increment": return_increment,
                "matched_net_reward_increment": reward_increment,
            }
        ),
    )


def test_validation_productive_requires_admission_and_both_positive_uplifts() -> None:
    assert _validation_productive(_admission(True), _uplift(0.1, 0.2)) is True
    assert _validation_productive(_admission(False), None) is False
    assert _validation_productive(_admission(True), _uplift(0.0, 0.2)) is False
    assert _validation_productive(_admission(True), _uplift(0.1, 0.0)) is False
    assert _validation_productive(_admission(True), _uplift(-0.1, 0.2)) is False


def test_d1_validation_route_is_high_cost_and_campaign_bound() -> None:
    assert app.ROUTES[ROUTE_ID] == (
        "our_system_phase2.runtime.cn_program_optimizer_d1_report_only_validation_v1"
    )
    assert app.HIGH_COST_ROUTE_ACTIONS[ROUTE_ID] == {ACTION_LAUNCH, ACTION_RETRY}
    assert ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_d1_validation_authorization_roundtrip_is_frozen_and_report_only() -> None:
    payload = verify_authorization(REPO / AUTHORIZATION_RELATIVE_PATH, repo_root=REPO)
    assert payload["status"] == "D1_REPORT_ONLY_VALIDATION_FROZEN_READY"
    assert payload["candidate_freeze"]["candidate_count"] == 120
    assert payload["prepared_binding"]["required_physical_leaf_count"] == 48
    assert payload["evaluation_role"] == "validation"
    assert payload["usage"] == "REPORT_ONLY_CANDIDATE_TRANSFER"
    assert payload["optimizer_feedback_write"] == "FORBIDDEN"
    assert payload["holdout_reads"] == 0
    assert payload["forward_2026_reads"] == 0
    assert payload["promotion_authorized"] is False
