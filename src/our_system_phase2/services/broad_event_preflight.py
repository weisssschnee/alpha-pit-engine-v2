"""Fail-closed preflight for the development-only Broad Event CANARY."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from our_system_phase2.services.broad_event_semantics import validate_semantic_registry


PREFLIGHT_VERSION = "cn_broad_event_preflight_v2"


def contract_hash(contract: dict[str, Any]) -> str:
    payload = {key: value for key, value in contract.items() if key != "contract_hash"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_canary_contract(contract: dict[str, Any]) -> None:
    if contract.get("contract_hash") != contract_hash(contract):
        raise ValueError("Broad Event CANARY contract hash mismatch")
    boundaries = contract["boundaries"]
    if boundaries != {
        "allowed_years": [2024, 2025],
        "allowed_data_role": "development_only",
        "validation_allowed": False,
        "holdout_allowed": False,
        "forward_2026_allowed": False,
        "candidate_promotion_allowed": False,
        "cross_sprint_memory_allowed": False,
        "plate_industry_allowed": False,
    }:
        raise ValueError("Broad Event data boundary contract mismatch")
    if len(contract["seeds"]) < 2 or len(set(contract["seeds"])) != len(contract["seeds"]):
        raise ValueError("Broad Event CANARY requires at least two distinct fixed seeds")
    for lane, budgets in contract["budgets"].items():
        if not all(int(value) > 0 for value in budgets.values()):
            raise ValueError(f"Broad Event lane has zero frozen budget: {lane}")
    if contract["time_blocks"] != [
        ["2024-01-01", "2024-06-30"],
        ["2024-07-01", "2024-12-31"],
        ["2025-01-01", "2025-06-30"],
        ["2025-07-01", "2025-12-31"],
    ]:
        raise ValueError("Broad Event development time blocks drifted")


def run_preflight(
    *,
    contract: dict[str, Any],
    semantic_registry: dict[str, Any],
    episode_support: dict[str, Any],
    lifecycle_manifest: dict[str, Any],
    limit_validation: dict[str, Any],
    access_ledger: dict[str, Any],
    controls: dict[str, Any],
) -> dict[str, Any]:
    checks: dict[str, bool] = {}
    errors: list[str] = []
    try:
        validate_canary_contract(contract)
        checks["frozen_contract"] = True
    except (KeyError, TypeError, ValueError) as exc:
        checks["frozen_contract"] = False
        errors.append(str(exc))
    try:
        validate_semantic_registry(semantic_registry)
        checks["semantic_registry"] = True
    except (KeyError, TypeError, ValueError) as exc:
        checks["semantic_registry"] = False
        errors.append(str(exc))
    checks["episode_inference"] = (
        not episode_support.get("row_count_used_as_effective_sample_size", True)
        and lifecycle_manifest.get("one_episode_one_admission_vote") is True
    )
    required_sources = set(contract["required_event_sources"])
    operational = set(episode_support.get("operational_sources", []))
    checks["event_support"] = required_sources <= operational
    if not checks["event_support"]:
        errors.append("insufficient event episode support: " + ",".join(sorted(required_sources - operational)))
    exact = int(lifecycle_manifest.get("exact_limit_session_count", 0))
    derived = int(lifecycle_manifest.get("derived_limit_session_count", 0))
    vendor_confirmed = int(lifecycle_manifest.get("vendor_confirmed_limit_session_count", 0))
    checks["pit_limit_prices"] = exact > 0 or (
        (derived > 0 or vendor_confirmed > 0)
        and limit_validation.get("decision") in {
            "DERIVED_LIMIT_VENDOR_CONSISTENCY_PASS",
            "CONSERVATIVE_LIMIT_VENDOR_CONSISTENCY_PASS",
        }
    )
    if not checks["pit_limit_prices"]:
        errors.append("PIT limit price source or derived-limit validation failed")
    forbidden_counters = (
        "validation_reads", "holdout_reads", "forward_2026_reads",
        "forbidden_file_reads", "forbidden_row_group_reads",
    )
    checks["data_access"] = (
        access_ledger.get("data_role") == "development_only"
        and all(int(access_ledger.get(key, -1)) == 0 for key in forbidden_counters)
    )
    checks["matched_controls"] = (
        controls.get("structural_control", {}).get("same_episode_support_times") is True
        and controls.get("episode_placebo_control", {}).get("same_episode_count") is True
        and controls.get("one_episode_one_admission_vote") is True
    )
    checks["nonzero_budgets"] = all(
        int(value) > 0 for budgets in contract.get("budgets", {}).values() for value in budgets.values()
    )
    if not checks.get("semantic_registry", False) or not checks.get("frozen_contract", False):
        decision = "SEMANTIC_CONTRACT_INVALID"
    elif not checks["event_support"]:
        decision = "INSUFFICIENT_SUPPORT"
    elif all(checks.values()):
        decision = "BROAD_EVENT_PREFLIGHT_PASS"
    else:
        decision = "SEMANTIC_CONTRACT_INVALID"
    return {
        "version": PREFLIGHT_VERSION,
        "decision": decision,
        "checks": checks,
        "errors": errors,
        "required_event_sources": sorted(required_sources),
        "operational_event_sources": sorted(operational),
        "canary_authorized": decision == "BROAD_EVENT_PREFLIGHT_PASS",
    }
