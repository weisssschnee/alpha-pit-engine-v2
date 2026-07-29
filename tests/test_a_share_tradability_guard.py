from __future__ import annotations

from our_system_phase2.services.a_share_tradability_guard import (
    A_SHARE_TRADABILITY_EVIDENCE_CLASS,
    A_SHARE_TRADABILITY_READY,
    A_SHARE_TRADABILITY_UNPROVEN,
    REQUIRED_TRADABILITY_PROOFS,
    a_share_tradability_blockers,
    a_share_tradability_ready,
    build_a_share_tradability_receipt,
    development_predictive_evidence,
    pair_row_tradability_blockers,
    pair_tradability_evidence,
    prefixed_tradability_evidence,
)


def _ready_evidence() -> dict[str, object]:
    return build_a_share_tradability_receipt(
        candidate_id="candidate-1",
        candidate_exact_identity="exact-1",
        replay_code_sha256="a" * 64,
        input_data_sha256="b" * 64,
        universe_manifest_sha256="c" * 64,
        fee_schedule_sha256="d" * 64,
        execution_policy_sha256="e" * 64,
        executable_net_reward=0.25,
        train_read_count=10,
        trade_count=2,
        fill_count=2,
        blocked_buy_count=0,
        blocked_sell_count=0,
    )


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

    evidence["replay_receipt_canonical_json"] = (
        evidence["replay_receipt_canonical_json"].replace(
            '"same_bar_execution_excluded":true',
            '"same_bar_execution_excluded":false',
        )
    )
    assert a_share_tradability_ready(evidence) is False
    assert "replay_receipt_payload_sha256_mismatch" in (
        a_share_tradability_blockers(evidence)
    )


def test_pair_tradability_requires_both_members() -> None:
    pair = pair_tradability_evidence(
        _ready_evidence(),
        development_predictive_evidence(),
    )

    assert pair["a_share_tradability_decision"] == A_SHARE_TRADABILITY_UNPROVEN
    assert a_share_tradability_ready(pair) is False


def test_pair_row_revalidates_both_canonical_receipts() -> None:
    primary = _ready_evidence()
    control = build_a_share_tradability_receipt(
        candidate_id="candidate-2",
        candidate_exact_identity="exact-2",
        replay_code_sha256="a" * 64,
        input_data_sha256="b" * 64,
        universe_manifest_sha256="c" * 64,
        fee_schedule_sha256="d" * 64,
        execution_policy_sha256="e" * 64,
        executable_net_reward=0.10,
        train_read_count=10,
        trade_count=2,
        fill_count=2,
        blocked_buy_count=0,
        blocked_sell_count=0,
    )
    row = {
        "primary_candidate_id": "candidate-1",
        "control_candidate_id": "candidate-2",
        **prefixed_tradability_evidence(primary, prefix="primary_"),
        **prefixed_tradability_evidence(control, prefix="control_"),
    }

    assert pair_row_tradability_blockers(row) == ()

    row["control_replay_receipt_canonical_json"] = str(
        row["control_replay_receipt_canonical_json"]
    ).replace('"candidate-2"', '"candidate-X"')
    assert "control_replay_candidate_id_mismatch" in (
        pair_row_tradability_blockers(row)
    )
    assert "control_replay_receipt_payload_sha256_mismatch" in (
        pair_row_tradability_blockers(row)
    )
