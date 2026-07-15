from __future__ import annotations

import hashlib

from scripts.freeze_cn_compositional_resource_preflight import (
    assign_evaluator_expression_identities,
    evaluator_expression_identity,
    representative_route_quotas,
    select_representative_pairs,
)


def test_evaluator_identity_keeps_duplicate_controls_in_distinct_pairs() -> None:
    expression = "CSRank(Sign($x))"

    assert evaluator_expression_identity("control-a", expression) != evaluator_expression_identity(
        "control-b", expression
    )


def test_evaluator_identity_changes_only_duplicate_expressions() -> None:
    rows = [
        {"candidate_id": "primary-a", "expression": "CSRank($x)"},
        {"candidate_id": "control-a", "expression": "CSRank(Sign($y))"},
        {"candidate_id": "control-b", "expression": "CSRank(Sign($y))"},
    ]

    assign_evaluator_expression_identities(rows)

    assert rows[0]["expression_hash"] == hashlib.sha256(b"CSRank($x)").hexdigest()[:24]
    assert rows[1]["expression_hash"] != rows[2]["expression_hash"]


def test_resource_preflight_quotas_cover_every_route_and_preserve_stage_weight_ties() -> None:
    quotas = representative_route_quotas(
        {"A": 592, "B": 576, "C": 592, "D": 576, "E": 592, "F": 576, "G": 592},
        32,
    )

    assert sum(quotas.values()) == 32
    assert {route for route, count in quotas.items() if count == 5} == {"A", "C", "E", "G"}
    assert set(quotas.values()) == {4, 5}


def test_resource_preflight_selection_is_deterministic_and_covers_dimensions() -> None:
    rows = []
    for index in range(12):
        rows.append(
            {
                "candidate_id": f"c{index:02d}",
                "route_id": "A",
                "policy_id": f"p{index % 4}",
                "seed": str(index % 4),
                "skeleton_id": f"s{index % 3}",
                "behavior_cluster_id": str(index),
            }
        )

    first = select_representative_pairs(rows, route_quotas={"A": 4})
    second = select_representative_pairs(reversed(rows), route_quotas={"A": 4})

    assert [row["candidate_id"] for row in first] == [row["candidate_id"] for row in second]
    assert len({row["policy_id"] for row in first}) == 4
    assert len({row["seed"] for row in first}) == 4
    assert len({row["behavior_cluster_id"] for row in first}) == 4
