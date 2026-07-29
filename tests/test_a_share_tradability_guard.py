from __future__ import annotations

from our_system_phase2.services.a_share_tradability_guard import (
    A_SHARE_TRADABILITY_EVIDENCE_CLASS,
    A_SHARE_TRADABILITY_READY,
    A_SHARE_TRADABILITY_UNPROVEN,
    REQUIRED_TRADABILITY_PROOFS,
    a_share_tradability_blockers,
    a_share_tradability_ready,
    development_predictive_evidence,
    pair_tradability_evidence,
)


def _ready_evidence() -> dict[str, object]:
    return {
        "evaluation_evidence_class": A_SHARE_TRADABILITY_EVIDENCE_CLASS,
        "a_share_tradability_decision": A_SHARE_TRADABILITY_READY,
        **{field: True for field in REQUIRED_TRADABILITY_PROOFS},
        "economic_claim_authorized": False,
        "candidate_promotion_authorized": False,
    }


def test_phase3cm_development_evidence_is_explicitly_unproven() -> None:
    evidence = development_predictive_evidence()

    assert evidence["a_share_tradability_decision"] == (
        A_SHARE_TRADABILITY_UNPROVEN
    )
    assert a_share_tradability_ready(evidence) is False
    assert "t_plus_one_enforced_not_proven" in a_share_tradability_blockers(
        evidence
    )
    assert evidence["economic_claim_authorized"] is False
    assert evidence["candidate_promotion_authorized"] is False


def test_tradability_ready_requires_every_explicit_proof() -> None:
    evidence = _ready_evidence()
    assert a_share_tradability_ready(evidence) is True

    evidence["same_bar_execution_excluded"] = False
    assert a_share_tradability_ready(evidence) is False
    assert "same_bar_execution_excluded_not_proven" in (
        a_share_tradability_blockers(evidence)
    )


def test_pair_tradability_requires_both_members() -> None:
    pair = pair_tradability_evidence(
        _ready_evidence(),
        development_predictive_evidence(),
    )

    assert pair["a_share_tradability_decision"] == A_SHARE_TRADABILITY_UNPROVEN
    assert a_share_tradability_ready(pair) is False
