"""Fail-closed A-share tradability evidence boundary.

Phase3CM is a development predictive/ranking evaluator. Its scores must not
become optimizer feedback, finalist evidence, or economic claims unless a
separate replay has explicitly proved the execution rules below.
"""

from __future__ import annotations

from typing import Any, Mapping


DEVELOPMENT_PREDICTIVE_EVIDENCE_CLASS = "DEVELOPMENT_PREDICTIVE_SCORE_ONLY"
A_SHARE_TRADABILITY_EVIDENCE_CLASS = "A_SHARE_TRADABILITY_REPLAY_V1"
A_SHARE_TRADABILITY_READY = "A_SHARE_TRADABILITY_READY"
A_SHARE_TRADABILITY_UNPROVEN = "A_SHARE_TRADABILITY_UNPROVEN"

REQUIRED_TRADABILITY_PROOFS = (
    "execution_clock_enforced",
    "same_bar_execution_excluded",
    "t_plus_one_enforced",
    "limit_lock_fill_enforced",
    "suspension_fill_enforced",
    "full_fee_schedule_enforced",
    "promotion_grade_universe_enforced",
)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def development_predictive_evidence() -> dict[str, Any]:
    """Return the explicit non-authoritative evidence declaration for Phase3CM."""

    return {
        "evaluation_evidence_class": DEVELOPMENT_PREDICTIVE_EVIDENCE_CLASS,
        "a_share_tradability_decision": A_SHARE_TRADABILITY_UNPROVEN,
        **{field: False for field in REQUIRED_TRADABILITY_PROOFS},
        "economic_claim_authorized": False,
        "candidate_promotion_authorized": False,
    }


def a_share_tradability_blockers(row: Mapping[str, Any]) -> tuple[str, ...]:
    """Return stable blockers; missing evidence is always blocked."""

    blockers: list[str] = []
    evidence_class = str(row.get("evaluation_evidence_class") or "")
    if evidence_class != A_SHARE_TRADABILITY_EVIDENCE_CLASS:
        blockers.append("a_share_tradability_evidence_class_not_ready")
    decision = str(row.get("a_share_tradability_decision") or "")
    if decision != A_SHARE_TRADABILITY_READY:
        blockers.append("a_share_tradability_decision_not_ready")
    blockers.extend(
        f"{field}_not_proven"
        for field in REQUIRED_TRADABILITY_PROOFS
        if not _truthy(row.get(field))
    )
    return tuple(blockers)


def a_share_tradability_ready(row: Mapping[str, Any]) -> bool:
    """True only for an explicit replay receipt satisfying every hard proof."""

    return not a_share_tradability_blockers(row)


def prefixed_tradability_evidence(
    row: Mapping[str, Any],
    *,
    prefix: str,
) -> dict[str, Any]:
    """Project comparable evidence fields onto a pair outcome."""

    fields = (
        "evaluation_evidence_class",
        "a_share_tradability_decision",
        *REQUIRED_TRADABILITY_PROOFS,
        "economic_claim_authorized",
        "candidate_promotion_authorized",
    )
    return {f"{prefix}{field}": row.get(field) for field in fields}


def pair_tradability_evidence(
    primary: Mapping[str, Any],
    control: Mapping[str, Any],
) -> dict[str, Any]:
    """Combine comparable replay evidence; either missing member blocks the pair."""

    ready = a_share_tradability_ready(primary) and a_share_tradability_ready(
        control
    )
    return {
        "evaluation_evidence_class": (
            A_SHARE_TRADABILITY_EVIDENCE_CLASS
            if ready
            else DEVELOPMENT_PREDICTIVE_EVIDENCE_CLASS
        ),
        "a_share_tradability_decision": (
            A_SHARE_TRADABILITY_READY if ready else A_SHARE_TRADABILITY_UNPROVEN
        ),
        **{
            field: _truthy(primary.get(field)) and _truthy(control.get(field))
            for field in REQUIRED_TRADABILITY_PROOFS
        },
        "economic_claim_authorized": False,
        "candidate_promotion_authorized": False,
    }
