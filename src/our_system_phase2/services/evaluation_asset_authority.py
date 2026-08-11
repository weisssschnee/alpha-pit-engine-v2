"""Project-level destructive-use authority for sealed evaluation assets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


HISTORICAL_ASSET_ID = "historical_challenge_2023_b05e2ca0"
FORWARD_B_ASSET_ID = "forward_b_tdx_lc1_20260413_20260514_f69cc84f"
PERMANENT_DENY_ALREADY_SPENT = "PERMANENT_DENY_ALREADY_SPENT"


class EvaluationAssetDenied(PermissionError):
    """Raised before any destructive asset read when authority denies access."""


def _read_json(path: Path, label: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise EvaluationAssetDenied(f"AUTHORITY_EVIDENCE_MISSING:{label}:{resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvaluationAssetDenied(
            f"AUTHORITY_EVIDENCE_INVALID:{label}:{resolved}"
        ) from exc
    if not isinstance(payload, dict):
        raise EvaluationAssetDenied(f"AUTHORITY_EVIDENCE_INVALID:{label}:{resolved}")
    return payload


def verify_historical_challenge_destructive_use(
    *,
    role_registry_path: Path,
    access_started_path: Path,
    outcome_path: Path,
) -> dict[str, Any]:
    """Deny the 2023 challenge permanently if any authoritative source says spent."""

    registry = _read_json(role_registry_path, "role_registry")
    access = _read_json(access_started_path, "access_transition")
    outcome = _read_json(outcome_path, "outcome")
    if registry.get("default_deny") is not True:
        raise EvaluationAssetDenied("EVALUATION_ROLE_REGISTRY_NOT_DEFAULT_DENY")

    states = dict(registry.get("asset_states") or {})
    historical = dict(states.get(HISTORICAL_ASSET_ID) or {})
    forward_b = dict(states.get(FORWARD_B_ASSET_ID) or {})
    if not historical or not forward_b:
        raise EvaluationAssetDenied("EVALUATION_ASSET_AUTHORITY_MISSING")
    if str(access.get("asset_id") or "") != HISTORICAL_ASSET_ID:
        raise EvaluationAssetDenied("HISTORICAL_ACCESS_ASSET_ID_DRIFT")

    access_decision = dict(outcome.get("access_decision") or {})
    provenance = dict(outcome.get("provenance") or {})
    spent_signals = {
        "registry_role_spent": str(historical.get("current_role") or "").lower()
        == "spent",
        "registry_rows_read": int(historical.get("performance_rows_read") or 0) > 0,
        "access_transition_spent": str(
            access.get("data_role_after_transition") or ""
        ).lower()
        == "spent",
        "access_status_spent": "SPENT" in str(access.get("status") or "").upper(),
        "outcome_state_spent": str(
            access_decision.get("historical_challenge_2023_state") or ""
        ).upper()
        == "SPENT",
        "outcome_rows_read": int(
            provenance.get("historical_challenge_reads") or 0
        )
        > 0,
    }
    if any(spent_signals.values()):
        raise EvaluationAssetDenied(PERMANENT_DENY_ALREADY_SPENT)

    if (
        str(historical.get("current_role") or "") != "challenge"
        or int(historical.get("performance_rows_read") or 0) != 0
        or str(access.get("data_role_after_transition") or "") != "challenge"
        or str(
            access_decision.get("historical_challenge_2023_state") or ""
        ).upper()
        != "UNOPENED"
    ):
        raise EvaluationAssetDenied("HISTORICAL_CHALLENGE_AUTHORITY_AMBIGUOUS")
    if (
        str(forward_b.get("current_role") or "") != "forward"
        or int(forward_b.get("performance_rows_read") or 0) != 0
        or str(access_decision.get("forward_b_state") or "").upper() != "SEALED"
        or str(access_decision.get("forward_b_access") or "").upper()
        != "NOT_AUTHORIZED"
    ):
        raise EvaluationAssetDenied("FORWARD_B_SEAL_DRIFT")
    return {
        "status": "ELIGIBLE_UNOPENED_SYNTHETIC",
        "asset_id": HISTORICAL_ASSET_ID,
        "performance_rows_read": 0,
        "forward_b_state": "SEALED",
        "forward_b_access": "NOT_AUTHORIZED",
    }


def verify_search_feedback_boundary(
    *,
    role_registry_path: Path,
    access_started_path: Path,
    outcome_path: Path,
) -> dict[str, Any]:
    """Verify the fail-closed search boundary without opening an asset.

    Unlike destructive historical use, Search V2 requires the 2023 asset to
    remain spent and Forward-B to remain sealed.  This function reads only the
    committed authority metadata and never reads an evaluation dataset.
    """

    registry = _read_json(role_registry_path, "role_registry")
    access = _read_json(access_started_path, "access_transition")
    outcome = _read_json(outcome_path, "outcome")
    if registry.get("default_deny") is not True:
        raise EvaluationAssetDenied("EVALUATION_ROLE_REGISTRY_NOT_DEFAULT_DENY")
    states = dict(registry.get("asset_states") or {})
    historical = dict(states.get(HISTORICAL_ASSET_ID) or {})
    forward_b = dict(states.get(FORWARD_B_ASSET_ID) or {})
    access_decision = dict(outcome.get("access_decision") or {})
    if (
        str(historical.get("current_role") or "").lower() != "spent"
        or int(historical.get("performance_rows_read") or 0) <= 0
        or str(historical.get("status") or "")
        != "spent_negative_no_retry_no_search_feedback"
        or str(historical.get("result") or "").upper() != "NEGATIVE"
        or str(historical.get("retry") or "").upper() != "FORBIDDEN"
        or str(historical.get("search_feedback") or "").upper() != "FORBIDDEN"
        or str(historical.get("promotion") or "").upper() != "FORBIDDEN"
        or str(historical.get("permanent_deny") or "")
        != PERMANENT_DENY_ALREADY_SPENT
        or str(access.get("asset_id") or "") != HISTORICAL_ASSET_ID
        or str(access.get("data_role_after_transition") or "").lower() != "spent"
        or str(
            access_decision.get("historical_challenge_2023_state") or ""
        ).upper()
        != "SPENT"
    ):
        raise EvaluationAssetDenied("HISTORICAL_2023_SPENT_BOUNDARY_DRIFT")
    if (
        str(forward_b.get("current_role") or "").lower() != "forward"
        or int(forward_b.get("performance_rows_read") or 0) != 0
        or str(access_decision.get("forward_b_state") or "").upper() != "SEALED"
        or str(access_decision.get("forward_b_access") or "").upper()
        != "NOT_AUTHORIZED"
    ):
        raise EvaluationAssetDenied("FORWARD_B_SEAL_DRIFT")
    return {
        "status": "SEARCH_FEEDBACK_BOUNDARY_CLOSED",
        "historical_2023_state": "SPENT",
        "historical_2023_result": "NEGATIVE",
        "historical_2023_retry": "FORBIDDEN",
        "historical_2023_search_feedback": "FORBIDDEN",
        "forward_b_state": "SEALED",
        "forward_b_search_feedback": "FORBIDDEN",
        "validation_search_feedback": "FORBIDDEN_BY_DEFAULT_DENY",
        "holdout_search_feedback": "FORBIDDEN_BY_DEFAULT_DENY",
        "promotion_authority": "NOT_OWNED_BY_SEARCH_V2",
        "financial_data_reads": 0,
    }
