"""Frozen primary-arm policy for Search Core V2 after Stage-2 confirmation."""
from __future__ import annotations

from typing import Any, Mapping

from our_system_phase2.services.program_search_primitive_credit_v1 import (
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
)
from our_system_phase2.services.program_search_state_jump_generator_v2 import (
    SEMANTIC_STATE_JUMP_GENERATOR_V2,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

SEARCH_CORE_V2_PRIMARY_POLICY_ID = "BOOTSTRAP_PRIMITIVE_THEN_MATURE_STATE_JUMP_V1"
MIN_MATURE_DEVELOPMENT_OBSERVATIONS = 168
STATE_JUMP_ADAPTER_SCHEMA = "cn_program_state_jump_optimizer_adapter_v2"


def _validate_snapshot(snapshot: Mapping[str, Any]) -> tuple[int, int]:
    payload = dict(snapshot)
    body = dict(payload)
    claimed = str(body.pop("snapshot_hash", ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError("SEARCH_CORE_V2_POLICY_SNAPSHOT_HASH_DRIFT")
    if payload.get("schema_version") != STATE_JUMP_ADAPTER_SCHEMA:
        raise ValueError("SEARCH_CORE_V2_POLICY_SNAPSHOT_SCHEMA_DRIFT")
    history = list(payload.get("history") or ())
    observations = 0
    for row in history:
        feedback_domain = str(row.get("feedback_domain") or "")
        if feedback_domain != "DEVELOPMENT_ONLY":
            raise ValueError("SEARCH_CORE_V2_POLICY_NON_DEVELOPMENT_FEEDBACK")
        if bool(row.get("sealed_feedback_used")):
            raise ValueError("SEARCH_CORE_V2_POLICY_SEALED_FEEDBACK_FORBIDDEN")
        observations += int(row.get("asked_count") or 0)
    if history:
        diagnostics = dict(history[-1].get("generator_diagnostics") or {})
        if int(diagnostics.get("memory_observations") or -1) != observations:
            raise ValueError("SEARCH_CORE_V2_POLICY_MEMORY_OBSERVATION_DRIFT")
        if int(diagnostics.get("dead_region_count") or 0) != 0:
            raise ValueError("SEARCH_CORE_V2_POLICY_DEAD_REGION_NOT_CLEAN")
    generated = len(set(map(str, payload.get("generated_exact_identities") or ())))
    if generated != observations:
        raise ValueError("SEARCH_CORE_V2_POLICY_GENERATED_EXACT_COUNT_DRIFT")
    return observations, len(history)


def resolve_search_core_v2_primary_arm(
    state_jump_snapshot: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if state_jump_snapshot is None:
        observations = 0
        history_count = 0
        arm = PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
        reason = "NO_VERIFIED_MATURE_STATE"
    else:
        observations, history_count = _validate_snapshot(state_jump_snapshot)
        if observations < MIN_MATURE_DEVELOPMENT_OBSERVATIONS:
            arm = PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
            reason = "MATURE_THRESHOLD_NOT_REACHED"
        else:
            arm = SEMANTIC_STATE_JUMP_GENERATOR_V2
            reason = "MATURE_DEVELOPMENT_STATE_CONFIRMED"
    return {
        "policy_id": SEARCH_CORE_V2_PRIMARY_POLICY_ID,
        "primary_arm": arm,
        "development_observations": observations,
        "history_count": history_count,
        "minimum_mature_development_observations": MIN_MATURE_DEVELOPMENT_OBSERVATIONS,
        "reason": reason,
        "primitive_role": "BOOTSTRAP_AND_CONTROL",
        "mcts_role": "CHALLENGER_DIAGNOSTIC_ONLY",
        "generator_failure_policy": "FAIL_CLOSED_NO_SILENT_PRIMITIVE_FALLBACK",
        "validation_feedback_allowed": False,
        "holdout_feedback_allowed": False,
        "forward_feedback_allowed": False,
    }


__all__ = [
    "MIN_MATURE_DEVELOPMENT_OBSERVATIONS",
    "SEARCH_CORE_V2_PRIMARY_POLICY_ID",
    "resolve_search_core_v2_primary_arm",
]
