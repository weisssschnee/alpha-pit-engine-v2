from __future__ import annotations

import json
from pathlib import Path

from our_system_phase2.services.broad_event_episodes import matched_control_contract
from our_system_phase2.services.broad_event_preflight import run_preflight, validate_canary_contract


REPO = Path(__file__).resolve().parents[1]


def _inputs() -> tuple[dict, dict]:
    contract = json.loads((REPO / "runtime/run_plans/cn_broad_event_canary_v1.json").read_text(encoding="utf-8"))
    semantics = json.loads((REPO / "runtime/event_registry/cn_broad_event_semantic_registry_v1.json").read_text(encoding="utf-8"))
    return contract, semantics


def test_frozen_broad_event_contract_has_two_seeds_nonzero_budgets_and_sealed_boundaries() -> None:
    contract, _ = _inputs()
    validate_canary_contract(contract)
    assert len(contract["seeds"]) == 2
    assert contract["boundaries"]["forward_2026_allowed"] is False
    assert contract["boundaries"]["candidate_promotion_allowed"] is False
    assert all(value > 0 for lane in contract["budgets"].values() for value in lane.values())


def test_preflight_passes_only_with_episode_support_pit_controls_and_zero_forbidden_reads() -> None:
    contract, semantics = _inputs()
    support = {
        "row_count_used_as_effective_sample_size": False,
        "operational_sources": contract["required_event_sources"],
    }
    report = run_preflight(
        contract=contract,
        semantic_registry=semantics,
        episode_support=support,
        lifecycle_manifest={
            "one_episode_one_admission_vote": True,
            "exact_limit_session_count": 0,
            "derived_limit_session_count": 100,
        },
        limit_validation={"decision": "DERIVED_LIMIT_VENDOR_CONSISTENCY_PASS"},
        access_ledger={
            "data_role": "development_only",
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "forbidden_file_reads": 0,
            "forbidden_row_group_reads": 0,
        },
        controls=matched_control_contract(),
    )
    assert report["decision"] == "BROAD_EVENT_PREFLIGHT_PASS"
    assert report["canary_authorized"] is True


def test_zero_budget_or_missing_support_cannot_be_called_generator_failure() -> None:
    contract, semantics = _inputs()
    support = {"row_count_used_as_effective_sample_size": False, "operational_sources": []}
    report = run_preflight(
        contract=contract,
        semantic_registry=semantics,
        episode_support=support,
        lifecycle_manifest={"one_episode_one_admission_vote": True, "exact_limit_session_count": 1, "derived_limit_session_count": 0},
        limit_validation={"decision": "NOT_REQUIRED"},
        access_ledger={"data_role": "development_only", "validation_reads": 0, "holdout_reads": 0, "forward_2026_reads": 0, "forbidden_file_reads": 0, "forbidden_row_group_reads": 0},
        controls=matched_control_contract(),
    )
    assert report["decision"] == "INSUFFICIENT_SUPPORT"
    assert report["decision"] != "BROAD_EVENT_GENERATOR_NO_INCREMENTAL_VALUE"
