from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scripts.freeze_cn_core_pack_delta_discovery import (
    CHALLENGERS,
    DEFAULT_OUTPUT,
    DEFAULT_SUPPLEMENTAL_SCOPE,
    SUPPLEMENTAL_ROOTS,
    build_contract,
    build_supplemental_scope,
    validate_contract,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


def _resign(contract: dict) -> dict:
    contract["contract_hash"] = stable_hash(
        {key: value for key, value in contract.items() if key != "contract_hash"}
    )
    return contract


def test_frozen_delta_campaign_matches_builder_and_reuses_baseline() -> None:
    supplemental = json.loads(Path(DEFAULT_SUPPLEMENTAL_SCOPE).read_text(encoding="utf-8"))
    assert supplemental == build_supplemental_scope()
    assert supplemental["root_count"] == 7
    assert supplemental["route_count"] == 3
    assert {row["field_id"] for row in supplemental["roots"]} == set(SUPPLEMENTAL_ROOTS)
    blocked = {
        row["field_id"]
        for row in supplemental["roots"]
        if row.get("materialization_status") == "NOT_MATERIALIZED"
    }
    assert blocked == {
        "state_close_range_location_sign",
        "fund_disclosure_balance_age_sessions",
        "fund_disclosure_cashflow_age_sessions",
        "fund_disclosure_holder_age_sessions",
        "fund_disclosure_profit_age_sessions",
    }
    assert all(
        row.get("signal_sketch_allowed") is False
        and row.get("strict_evaluation_allowed") is False
        and row.get("runtime_ready") is False
        and row.get("runtime_activation")
        == "CANONICAL_FULL_DEVELOPMENT_RECEIPT_REQUIRED_PER_ROOT"
        and row.get("required_receipt")
        for row in supplemental["roots"]
        if row["field_id"] in blocked
    )
    assert supplemental["runtime_activation_contract"] == {
        "default": "FROZEN",
        "scope_mutation_allowed": False,
        "activation_unit": "ONE_ROOT_ONE_VERIFIED_RECEIPT",
        "candidate_receipt_hash_injection_required": True,
        "requirements_by_field": {
            "fund_disclosure_balance_age_sessions": {
                "receipt_type": "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT",
                "full_development_assembly": "FULL_DEVELOPMENT_SESSION_PANEL",
            },
            "fund_disclosure_cashflow_age_sessions": {
                "receipt_type": "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT",
                "full_development_assembly": "FULL_DEVELOPMENT_SESSION_PANEL",
            },
            "fund_disclosure_holder_age_sessions": {
                "receipt_type": "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT",
                "full_development_assembly": "FULL_DEVELOPMENT_SESSION_PANEL",
            },
            "fund_disclosure_profit_age_sessions": {
                "receipt_type": "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT",
                "full_development_assembly": "FULL_DEVELOPMENT_SESSION_PANEL",
            },
            "state_close_range_location_sign": {
                "receipt_type": "FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
                "full_development_assembly": "FULL_DEVELOPMENT_ACTIVE_PANEL",
            },
        },
    }
    built = build_contract()
    frozen = json.loads(Path(DEFAULT_OUTPUT).read_text(encoding="utf-8"))
    assert frozen == built
    validate_contract(frozen)
    assert frozen["baseline_contract"]["proposal_attempts"] == 1_900_544
    assert frozen["baseline_contract"]["legal_exact_unique_primaries"] == 69_897
    assert frozen["baseline_contract"]["full_pack_regeneration"] == "FORBIDDEN"
    delta = frozen["root_delta_contract"]
    assert sum(len(values) for values in delta["newly_connected_roots_by_route"].values()) == 7
    assert sum(bool(values) for values in delta["newly_connected_roots_by_route"].values()) == 3
    for route, roots in delta["newly_connected_roots_by_route"].items():
        budget = delta["budgets_by_route"][route]
        assert bool(roots) == bool(budget["proposal_budget"] and budget["admission_budget"])
    assert {row["challenger_id"] for row in frozen["challenger_contract"]["challengers"]} == set(CHALLENGERS)


@pytest.mark.parametrize("role", ["validation_reads", "holdout_reads", "forward_2026_reads"])
def test_forbidden_data_budget_fails_closed(role: str) -> None:
    contract = build_contract()
    contract["data_access_contract"][role] = 1
    with pytest.raises(ValueError, match="read budgets must be zero"):
        validate_contract(_resign(contract))


def test_full_regeneration_and_generator_self_gate_fail_closed() -> None:
    contract = build_contract()
    contract["baseline_contract"]["full_pack_regeneration"] = "ALLOWED"
    contract["challenger_contract"]["generator_may_self_gate"] = True
    with pytest.raises(ValueError, match="full-pack regeneration.*cannot gate itself"):
        validate_contract(_resign(contract))


@pytest.mark.parametrize("field", ["root_scope_blob_sha", "root_scope_hash", "root_scope_path", "repo_sha"])
def test_historical_baseline_scope_identity_is_exact(field: str) -> None:
    contract = build_contract()
    contract["baseline_contract"][field] = "forged"
    with pytest.raises(ValueError, match="baseline .*drift"):
        validate_contract(_resign(contract))


@pytest.mark.parametrize("field", ["role", "path", "sha256", "mode"])
def test_baseline_artifact_tuple_is_exact(field: str) -> None:
    contract = build_contract()
    contract["baseline_contract"]["artifacts"][0][field] = "forged"
    with pytest.raises(ValueError, match="role/path/hash/mode tuple drift"):
        validate_contract(_resign(contract))


def test_challenger_requires_equal_nonadaptive_control_budget() -> None:
    contract = build_contract()
    challenger = contract["challenger_contract"]["challengers"][0]
    challenger["matched_nonadaptive_control"]["proposal_budget_by_route"]["MINUTE_STATIC"] -= 1
    with pytest.raises(ValueError, match="matched budget mismatch"):
        validate_contract(_resign(contract))


def test_unproved_adapter_cannot_be_activated_by_label() -> None:
    contract = build_contract()
    contract["challenger_contract"]["challengers"][1]["activation_state"] = "ACTIVE"
    with pytest.raises(ValueError, match="unproved challenger activated"):
        validate_contract(_resign(contract))


def test_reward_cannot_gate_semantic_saturation_or_rewrite_wave64_scope() -> None:
    contract = build_contract()
    contract["semantic_saturation_contract"]["performance_or_reward_metrics_allowed"] = True
    contract["strict_continuation_contract"]["wave64_reward_may_change_grammar_fields_or_route_budgets"] = True
    with pytest.raises(ValueError, match="performance/reward.*wave64 reward"):
        validate_contract(_resign(contract))


def test_adaptive_challengers_wait_for_256_and_memory_is_epoch_local() -> None:
    contract = build_contract()
    contract["strict_continuation_contract"]["adaptive_challenger_minimum_completed_strict_pairs"] = 64
    contract["epoch_freeze_contract"]["cross_epoch_memory"] = "ALLOWED"
    with pytest.raises(ValueError, match="memory must be forbidden.*cannot start before 256"):
        validate_contract(_resign(contract))


def test_r6_liveness_parity_receipt_is_a_fail_closed_256_gate() -> None:
    contract = build_contract()
    contract["strict_continuation_contract"]["pre_256_engineering_gate"][
        "epoch_freeze_artifact_sha256_required"
    ] = False
    with pytest.raises(ValueError, match="R6 liveness parity evidence must gate 256"):
        validate_contract(_resign(contract))
