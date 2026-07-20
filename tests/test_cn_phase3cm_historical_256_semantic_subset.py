from __future__ import annotations

import copy

import pytest

from scripts.verify_cn_phase3cm_historical_256_semantic_subset import (
    SemanticSubsetError,
    verify_historical_subset,
)


def _pair(index: int) -> dict:
    return {
        "pair_receipt_schema_version": "cn_candidate_pair_receipt_v2",
        "pair_id": f"pair-{index}",
        "primary_candidate_id": f"candidate-{index}-primary",
        "control_candidate_id": f"candidate-{index}-control",
        "pair_receipt_id": f"receipt-{index}",
        "pair_receipt_hash": f"pair-hash-{index}",
        "primary_receipt_hash": f"primary-hash-{index}",
        "control_receipt_hash": f"control-hash-{index}",
        "allow_behavior_equivalence": False,
        "route_id": "SLOW_TEMPORAL_CHANGE",
        "control_constructor_id": "slow_change_sign_projection_v1",
        "control_constructor_contract": {"keeps": ["field", "support"]},
        "primary_field_ids": [f"field-{index}"],
        "control_field_ids": [f"field-{index}"],
        "primary_observable_time_contract": [f"field-{index}|next_session"],
        "control_observable_time_contract": [f"field-{index}|next_session"],
        "primary_pit_source_lag_contract": [f"field-{index}|PIT_SAFE"],
        "control_pit_source_lag_contract": [f"field-{index}|PIT_SAFE"],
        "primary_vote_policy": "ONE_SUPPORT_UNIT_ONE_VOTE",
        "control_vote_policy": "CONTROL_NO_SEPARATE_VOTE",
        "support_unit": "stock-session",
        "pair_clock_alignment_policy": "PRIMARY_CLOCK_AND_EXACT_ELIGIBLE_SUPPORT",
        "mapping_portfolio_contract": "SAME_SUPPORT_COORDINATES",
        "pair_authorization_status": "AUTHORIZED_FOR_FORMAL_PAIR_EVALUATION",
        "representation_ids": [f"rep-{index}"],
        "source_field_ids": [f"source-{index}"],
    }


def _candidate(index: int, role: str) -> dict[str, str]:
    control = role == "CONTROL"
    candidate_id = f"candidate-{index}-{'control' if control else 'primary'}"
    return {
        "candidate_id": candidate_id,
        "pair_id": f"pair-{index}",
        "pair_member_role": role,
        "route_id": "SLOW_TEMPORAL_CHANGE",
        "canonical_expression": f"CSRank(Delta($field_{index},5))" if not control else f"CSRank($field_{index})",
        "canonical_identity": f"canonical-{index}-{role}",
        "exact_identity": f"exact-{index}-{role}",
        "clock_contract": "all_source_versions_observable_before_action",
        "maturity_contract": "latest_required_version maturity",
        "support_unit": "stock-session",
        "seed": "1729",
        "skeleton_id": "slow.temporal.delta",
        "pair_mapping_portfolio_contract": "SAME_SUPPORT_COORDINATES",
        "pair_maturity_alignment_policy": "MAX_PRIMARY_CONTROL_MATURITY_BEFORE_SHARED_SUPPORT",
        "pair_support_alignment_policy": "PRIMARY_CONTROL_FINITE_INTERSECTION_AT_SHARED_COORDINATE",
        "access_roles": '["development"]',
        "field_ids": f'["field-{index}"]',
        "declared_field_ids": f'["field-{index}"]',
        "representation_ids": f'["rep-{index}"]',
        "source_field_ids": f'["source-{index}"]',
        "condition_field_ids": "[]",
        "input_roles": '["PRIMARY"]',
        "operator_paths": '["root:CSRank"]',
        "legal": "True",
        "is_matched_control": str(control),
        "uses_future_revision": "False",
        "matched_control_id": f"candidate-{index}-{'primary' if control else 'control'}",
        "control_constructor_id": "slow_change_sign_projection_v1",
        "control_ablation_rule": "remove_delta",
        "generator_arm": "typed_random",
        "generator_version": "v2",
        "proposal_origin": "typed_compositional_grammar_v2",
        "operator_family": "Delta",
        "outer_mapping": "cross_sectional",
        "vote_policy": "ONE_SUPPORT_UNIT_ONE_VOTE",
        "maturity_rule": "next_session",
        "allow_behavior_equivalence": "False",
        "requires_intrabar_order": "False",
        "maturity_contract_registered": "True",
        "round_id": "historical-256",
        "expression_hash": f"expression-hash-{index}-{role}",
    }


def _packs() -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    historical_pairs = [_pair(index) for index in range(256)]
    historical_candidates = [
        _candidate(index, role)
        for index in range(256)
        for role in ("PRIMARY", "CONTROL")
    ]
    freeze_pairs = [copy.deepcopy(row) for row in historical_pairs]
    freeze_candidates = [copy.deepcopy(row) for row in historical_candidates]
    for row in freeze_pairs:
        row["pair_receipt_hash"] = "refrozen-" + row["pair_receipt_hash"]
        row["pair_receipt_id"] = "refrozen-" + row["pair_receipt_id"]
    for row in freeze_candidates:
        row["round_id"] = "freeze-1024"
        row["expression_hash"] = "refrozen-" + row["expression_hash"]
    freeze_pairs.extend(_pair(index) for index in range(256, 1024))
    freeze_candidates.extend(
        _candidate(index, role)
        for index in range(256, 1024)
        for role in ("PRIMARY", "CONTROL")
    )
    return historical_pairs, freeze_pairs, historical_candidates, freeze_candidates


def _verify(packs: tuple[list[dict], list[dict], list[dict], list[dict]]) -> dict:
    return verify_historical_subset(*packs)


def test_historical_identity_and_semantics_are_exact_subset() -> None:
    result = _verify(_packs())
    assert result["status"] == "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_PASS"
    assert result["pair_identity_subset_exact"] is True
    assert result["candidate_identity_subset_exact"] is True
    assert result["pair_semantics_exact"] is True
    assert result["candidate_semantics_exact"] is True
    assert "expression_hash" in result["ignored_refreeze_fields"]


def test_substituted_pair_identity_fails_closed() -> None:
    historical_pairs, freeze_pairs, historical_candidates, freeze_candidates = _packs()
    freeze_pairs[0]["pair_id"] = "substituted-pair"
    freeze_candidates[0]["pair_id"] = "substituted-pair"
    freeze_candidates[1]["pair_id"] = "substituted-pair"
    result = verify_historical_subset(
        historical_pairs, freeze_pairs, historical_candidates, freeze_candidates
    )
    assert result["status"] == "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_FAIL_CLOSED"
    assert result["pair_identity_subset_exact"] is False


def test_candidate_expression_drift_fails_closed() -> None:
    packs = _packs()
    packs[3][17]["canonical_expression"] = "CSRank($future_field)"
    result = _verify(packs)
    assert result["status"] == "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_FAIL_CLOSED"
    assert result["candidate_semantics_exact"] is False


def test_pair_observable_contract_drift_fails_closed() -> None:
    packs = _packs()
    packs[1][9]["primary_observable_time_contract"] = ["field-9|same_bar"]
    result = _verify(packs)
    assert result["status"] == "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_FAIL_CLOSED"
    assert result["pair_semantics_exact"] is False


def test_duplicate_pair_member_role_fails_closed() -> None:
    packs = _packs()
    packs[3][1]["pair_member_role"] = "PRIMARY"
    packs[3][1]["is_matched_control"] = "False"
    with pytest.raises(SemanticSubsetError, match="one primary and one control"):
        _verify(packs)
