from __future__ import annotations

from our_system_phase2.services.program_search_semantic_mcts_v1 import (
    SemanticProgramMCTSTreeV1,
    SemanticProgramStateV1,
    semantic_program_facts_v1,
)
from our_system_phase2.services.route_local_availability import AvailabilityEntry


class _Prior:
    def __init__(self, scores: dict[str, float]):
        self.scores = scores

    def action_prior(self, *, state, slot, value, candidates):
        return max(self.scores[e.exact_identity] for e in candidates)

    def state_value(self, *, state, candidates):
        return max(self.scores[e.exact_identity] for e in candidates)

    def completion_key(self, entry):
        return (-self.scores[entry.exact_identity], entry.exact_identity)

    def completion_details(self, entry):
        return {"score": self.scores[entry.exact_identity]}


def _entry(exact: str, template: str, base_skel: str, enhancer_role: str, enhancer_skel: str, app: str) -> AvailabilityEntry:
    genes = {
        "program_template_id": template,
        "base__route_id": "SLOW_CROSS_SECTIONAL_LEVEL",
        "base__skeleton_id": base_skel,
        f"{enhancer_role}__route_id": {
            "event": "DISCLOSURE_EVENT",
            "market": "MARKET_STATE",
        }[enhancer_role],
        f"{enhancer_role}__skeleton_id": enhancer_skel,
        "combination_temporal": "ADD",
        "combination_market": "FILTER",
        "combination_event_episode": "SOURCE_ROUTE_EPISODE",
        "combination_event_application": app,
        "composition_topology": f"base>{enhancer_role}",
        "lag_class": "SHOULD_NOT_ENTER_TREE",
        "skeleton_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
    }
    return AvailabilityEntry(
        route_id="CN_PROGRAM",
        bucket_key=f"bucket-{exact}",
        exact_identity=exact,
        control_exact_identity=f"control-{exact}",
        genes=genes,
    )


def _pool():
    entries = [
        _entry("a", "BASE_EVENT", "base.A", "event", "event.X", "FILTER"),
        _entry("b", "BASE_EVENT", "base.B", "event", "event.X", "GATE"),
        _entry("c", "BASE_MARKET", "base.A", "market", "market.X", "FILTER"),
        _entry("d", "BASE_MARKET", "base.B", "market", "market.Y", "FILTER"),
    ]
    facts = {e.exact_identity: semantic_program_facts_v1(e) for e in entries}
    scores = {"a": 0.9, "b": 0.6, "c": 0.7, "d": 0.5}
    return entries, facts, _Prior(scores)


def test_semantic_surface_excludes_lag_class():
    entries, facts, _ = _pool()
    assert entries[0].genes["lag_class"] == "SHOULD_NOT_ENTER_TREE"
    assert "lag_class" not in facts["a"]


def test_state_is_order_independent_and_interned_as_transposition():
    _, facts, prior = _pool()
    left = SemanticProgramStateV1((("b", "2"), ("a", "1")))
    right = SemanticProgramStateV1((("a", "1"), ("b", "2")))
    assert left == right
    assert left.key == right.key
    tree = SemanticProgramMCTSTreeV1(facts_by_exact_identity=facts, prior_value=prior)
    n1 = tree.intern_state(left)
    n2 = tree.intern_state(right)
    assert n1 is n2
    assert tree.transposition_hits >= 1


def test_simulation_expands_multiple_depths_and_backpropagates():
    entries, facts, prior = _pool()
    tree = SemanticProgramMCTSTreeV1(
        facts_by_exact_identity=facts,
        prior_value=prior,
        progressive_widening_coefficient=2.0,
        progressive_widening_alpha=0.5,
    )
    tree.run_simulations(entries, count=20)
    diag = tree.diagnostics()
    assert diag["max_semantic_depth"] >= 2
    assert diag["expansion_count"] >= 2
    assert tree.nodes[tree.root_key].visits == 20
    assert tree.nodes[tree.root_key].value_sum > 0.0


def test_progressive_widening_limits_root_then_opens_more_actions():
    entries, facts, prior = _pool()
    tree = SemanticProgramMCTSTreeV1(
        facts_by_exact_identity=facts,
        prior_value=prior,
        progressive_widening_coefficient=1.0,
        progressive_widening_alpha=0.5,
    )
    tree.simulate(entries)
    root = tree.nodes[tree.root_key]
    assert len(root.edges) == 1
    tree.run_simulations(entries, count=8)
    assert len(root.edges) == 2


def test_explicit_observation_backprop_updates_ancestors():
    entries, facts, prior = _pool()
    tree = SemanticProgramMCTSTreeV1(facts_by_exact_identity=facts, prior_value=prior)
    tree.run_simulations(entries, count=6)
    node_keys, edge_path, leaf_pool = tree.principal_variation(entries)
    assert len(node_keys) >= 2
    before = tree.nodes[tree.root_key].visits
    tree.backpropagate_observation(node_keys=node_keys, edge_path=edge_path, value=1.0)
    assert tree.nodes[tree.root_key].visits == before + 1
    assert tree.actual_backpropagations == 1
    assert tree.complete(leaf_pool).exact_identity in {e.exact_identity for e in leaf_pool}
