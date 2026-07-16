from __future__ import annotations

from our_system_phase2.services.phase3cm_streaming_dag import SharedMultiCandidateDAGPlan


def _candidate(
    candidate_id: str,
    *,
    expression: str,
    support: str,
    maturity: str = "bar close",
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "pair_id": "pair." + candidate_id.split(".")[-1],
        "pair_member_role": "PRIMARY",
        "clock_namespace": "active_bar",
        "route_id": "MINUTE_STATIC",
        "expression": expression,
        "declared_field_ids": ["close", "volume"],
        "support_unit": support,
        "maturity_contract": maturity,
        "outer_mapping": "cross_sectional",
        "portfolio_mode": "long_only_top",
    }


def test_value_nodes_share_across_support_but_mapping_nodes_do_not() -> None:
    candidates = [
        _candidate(
            "candidate.a",
            expression="CSRank(Mul(ZScore($close),Delta($volume,5)))",
            support="stock-minute cross-section",
        ),
        _candidate(
            "candidate.b",
            expression="CSRank(Mul(ZScore($close),Delta($volume,5)))",
            support="stock-state episode",
        ),
    ]

    plan = SharedMultiCandidateDAGPlan.build(candidates)

    assert len(plan.value_cohorts) == 1
    assert len(plan.mapping_subcohorts) == 2
    delta_nodes = [node for node in plan.nodes if node.canonical_expression == "Delta($volume,5)"]
    mapping_roots = [node for node in plan.nodes if node.canonical_expression.startswith("CSRank(")]
    assert len(delta_nodes) == 1
    assert delta_nodes[0].layer == "VALUE"
    assert len(mapping_roots) == 2
    assert all(node.layer == "MAPPING" for node in mapping_roots)
    assert mapping_roots[0].node_id != mapping_roots[1].node_id


def test_different_maturity_prevents_value_node_sharing() -> None:
    candidates = [
        _candidate("candidate.a", expression="Delta($close,5)", support="same", maturity="bar close"),
        _candidate("candidate.b", expression="Delta($close,5)", support="same", maturity="next session"),
    ]

    plan = SharedMultiCandidateDAGPlan.build(candidates)

    delta_nodes = [node for node in plan.nodes if node.canonical_expression == "Delta($close,5)"]
    assert len(plan.value_cohorts) == 2
    assert len(delta_nodes) == 2
    assert delta_nodes[0].node_id != delta_nodes[1].node_id


def test_plan_is_deterministic_and_records_consumer_reuse() -> None:
    rows = [
        _candidate("candidate.a", expression="Add(Delta($close,5),1)", support="same"),
        _candidate("candidate.b", expression="Mul(Delta($close,5),2)", support="same"),
    ]

    first = SharedMultiCandidateDAGPlan.build(rows)
    second = SharedMultiCandidateDAGPlan.build(list(reversed(rows)))

    assert first.plan_hash == second.plan_hash
    assert first.to_dict() == second.to_dict()
    delta = next(node for node in first.nodes if node.canonical_expression == "Delta($close,5)")
    assert delta.consumer_count == 2
    assert delta.reuse_count == 1

