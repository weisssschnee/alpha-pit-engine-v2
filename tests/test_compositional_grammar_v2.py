from __future__ import annotations

from pathlib import Path

from our_system_phase2.services.compositional_grammar import (
    CompositionalGrammarV2,
    skeleton_registry,
)
from our_system_phase2.services.expression_semantics import parse_expression
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry"
    / "unified_capability_registry.json"
)


def test_searchable_routes_expose_multiple_financially_declared_skeletons() -> None:
    registry = skeleton_registry()

    assert set(registry) == set(ROUTE_IDS)
    for route_id in ROUTE_IDS:
        rows = registry[route_id]
        if route_id == "BROAD_EVENT_FROZEN_ENTRY":
            assert len(rows) == 1
            assert rows[0].search_role == "FROZEN_REFERENCE_ONLY"
            continue
        assert len(rows) >= 8
        assert len({row.skeleton_id for row in rows}) == len(rows)
        assert all(row.financial_hypothesis for row in rows)
        assert all(row.input_roles for row in rows)
        assert all(row.unit_signature for row in rows)
        assert all(row.clock_contract for row in rows)
        assert all(row.maturity_contract for row in rows)
        assert all(row.control_ablation_rule for row in rows)
        assert all(row.allowed_routes == (route_id,) for row in rows)
        assert all(1 <= row.maximum_depth <= 4 for row in rows)


def test_minute_compositions_compile_with_same_field_matched_controls() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))
    pairs = [
        grammar.propose("MINUTE_STATIC", attempt_index=index, seed=1729)
        for index in range(8)
    ]

    assert len({pair.skeleton.skeleton_id for pair in pairs}) == 8
    for pair in pairs:
        assert pair.primary["legal"] is True
        assert pair.control["legal"] is True
        assert set(pair.primary["declared_field_ids"]) == set(
            pair.control["declared_field_ids"]
        )
        assert pair.primary["clock_contract"] == pair.control["clock_contract"]
        assert pair.primary["maturity_contract"] == pair.control["maturity_contract"]
        assert pair.primary["unit_signature"] == pair.control["unit_signature"]
        assert pair.primary["expression"] != pair.control["expression"]
        assert pair.control["expression"] != "CSRank(Sign($first_field))"


def test_firstn_compositions_compile_with_path_clock_preserved() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))
    pairs = [
        grammar.propose("FIRSTN_PATH", attempt_index=index, seed=2718)
        for index in range(8)
    ]

    assert len({pair.skeleton.skeleton_id for pair in pairs}) == 8
    for pair in pairs:
        assert pair.primary["legal"] is True
        assert pair.control["legal"] is True
        assert set(pair.primary["declared_field_ids"]) == set(
            pair.control["declared_field_ids"]
        )
        assert pair.primary["clock_contract"] == "bar_close_and_firstN_clock"
        assert pair.control["clock_contract"] == pair.primary["clock_contract"]
        assert "Mul(0," in pair.control["expression"]


def test_slow_routes_compile_without_unqualified_industry_neutralization() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))

    for route_id in ("SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE"):
        pairs = [
            grammar.propose(route_id, attempt_index=index, seed=31415)
            for index in range(8)
        ]
        assert len({pair.skeleton.skeleton_id for pair in pairs}) == 8
        for pair in pairs:
            assert pair.primary["legal"] is True
            assert pair.control["legal"] is True
            assert set(pair.primary["declared_field_ids"]) == set(
                pair.control["declared_field_ids"]
            )
            assert "IndustryNeutralize" not in pair.primary["expression"]
            assert pair.primary["access_roles"] == ["development"]
            assert pair.control["access_roles"] == ["development"]


def test_disclosure_compositions_are_episode_mature_and_controlled_by_recency() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))
    pairs = [
        grammar.propose("DISCLOSURE_EVENT", attempt_index=index, seed=16180)
        for index in range(8)
    ]

    assert len({pair.skeleton.skeleton_id for pair in pairs}) == 8
    for pair in pairs:
        assert pair.primary["legal"] is True
        assert pair.control["legal"] is True
        assert pair.primary["maturity_contract_registered"] is True
        assert "TimeSince(" in pair.control["expression"]
        assert set(pair.primary["declared_field_ids"]) == set(
            pair.control["declared_field_ids"]
        )


def test_market_regime_compositions_condition_stock_payload_without_direct_market_rank() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))
    pairs = [
        grammar.propose("MARKET_REGIME_CONDITION", attempt_index=index, seed=57721)
        for index in range(8)
    ]

    assert len({pair.skeleton.skeleton_id for pair in pairs}) == 8
    for pair in pairs:
        assert pair.primary["legal"] is True
        assert pair.control["legal"] is True
        assert pair.primary["condition_field_ids"]
        assert pair.primary["condition_field_ids"] == pair.control["condition_field_ids"]
        assert set(pair.primary["declared_field_ids"]) == set(
            pair.control["declared_field_ids"]
        )
        assert "Mul(0," in pair.control["expression"]


def test_intraday_state_compositions_consume_registered_state_and_source_lineage() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))
    pairs = [
        grammar.propose("INTRADAY_STATE_TRANSITION", attempt_index=index, seed=75025)
        for index in range(8)
    ]

    assert len({pair.skeleton.skeleton_id for pair in pairs}) == 8
    for pair in pairs:
        assert pair.primary["legal"] is True
        assert pair.control["legal"] is True
        assert pair.primary["claimed_state_field_id"]
        assert pair.primary["state_source_expression"] in pair.primary["canonical_expression"]
        assert pair.primary["declared_field_ids"] == pair.control["declared_field_ids"]
        assert "Mul(0," in pair.control["expression"]


def test_broad_event_route_is_frozen_reference_only() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))
    pair = grammar.propose("BROAD_EVENT_FROZEN_ENTRY", attempt_index=0, seed=65537)

    assert pair.skeleton.search_role == "FROZEN_REFERENCE_ONLY"
    assert pair.primary["legal"] is True
    assert pair.control["legal"] is True
    assert pair.primary["search_role"] == "FROZEN_REFERENCE_ONLY"
    assert pair.primary["frozen_mechanism_id"]
    assert pair.primary["declared_field_ids"] == pair.control["declared_field_ids"]
    assert pair.primary["expression"].startswith("FrozenMechanismReplay(")
    assert pair.control["expression"].startswith("MatchedControlReplay(")


def _call_depth(expression: str) -> int:
    def visit(node: object) -> int:
        args = getattr(node, "args")
        if not args:
            return 0
        return 1 + max(visit(child) for child in args)

    return visit(parse_expression(expression))


def test_all_compositional_candidates_respect_registered_depth_and_field_volume() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))

    for route_id in ROUTE_IDS:
        for index in range(len(skeleton_registry()[route_id])):
            pair = grammar.propose(route_id, attempt_index=index, seed=99991)
            for candidate in (pair.primary, pair.control):
                assert _call_depth(candidate["canonical_expression"]) <= 4
                assert 1 <= len(candidate["declared_field_ids"]) <= 3


def test_interaction_legs_do_not_collapse_to_the_same_source_field() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))
    expected_two_field_skeletons = {
        "MINUTE_STATIC": set(range(1, 8)),
        "FIRSTN_PATH": set(range(8)),
        "SLOW_CROSS_SECTIONAL_LEVEL": {1, 2, 3, 4, 7},
        "SLOW_TEMPORAL_CHANGE": {4, 7},
        "DISCLOSURE_EVENT": {4},
        "MARKET_REGIME_CONDITION": set(range(8)),
    }
    for route_id, indices in expected_two_field_skeletons.items():
        for index in indices:
            pair = grammar.propose(route_id, attempt_index=index, seed=104729)
            assert len(pair.primary["declared_field_ids"]) >= 2

    for index in range(8):
        pair = grammar.propose("INTRADAY_STATE_TRANSITION", attempt_index=index, seed=104729)
        assert len(pair.primary["declared_field_ids"]) == 3
