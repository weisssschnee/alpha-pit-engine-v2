from __future__ import annotations

import pytest

from scripts.run_cn_phase3cm_streaming_qualification import (
    _verify_selected_candidate_semantics,
)


def _inputs() -> tuple[list[dict[str, object]], dict[str, object]]:
    candidates: list[dict[str, object]] = []
    members: list[dict[str, object]] = []
    for role, candidate_id, expression in (
        ("PRIMARY", "primary", "CSRank($x)"),
        ("CONTROL", "control", "$x"),
    ):
        row = {
            "candidate_id": candidate_id,
            "pair_id": "pair.1",
            "pair_member_role": role,
            "expression": expression,
            "canonical_expression": expression,
            "route_id": "STATIC_CROSS_SECTIONAL",
            "field_ids": '["x"]',
        }
        candidates.append(row)
        members.append(
            {
                **row,
                "field_ids": ["x"],
                "clock_namespace": "active_bar",
            }
        )
    binding = {
        "candidate_members": members,
        "pairs": [
            {
                "pair_id": "pair.1",
                "candidate_id": "primary",
                "control_candidate_id": "control",
                "clock_namespace": "active_bar",
            }
        ],
    }
    return candidates, binding


def test_selected_candidate_semantics_match_binding_exactly() -> None:
    candidates, binding = _inputs()
    _verify_selected_candidate_semantics(
        candidates,
        binding=binding,
        clock="active_bar",
    )


def test_selected_candidate_semantics_reject_same_id_with_changed_expression() -> None:
    candidates, binding = _inputs()
    candidates[0]["expression"] = "CSRank($future_x)"
    with pytest.raises(RuntimeError, match="candidate semantic drift primary:expression"):
        _verify_selected_candidate_semantics(
            candidates,
            binding=binding,
            clock="active_bar",
        )


def test_selected_candidate_semantics_reject_pair_member_swap() -> None:
    candidates, binding = _inputs()
    candidates[0]["pair_member_role"] = "CONTROL"
    with pytest.raises(RuntimeError, match="pair_member_role"):
        _verify_selected_candidate_semantics(
            candidates,
            binding=binding,
            clock="active_bar",
        )
