from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from our_system_phase2.services.compositional_grammar import (
    CompositionalGrammarV2,
    SUPPLEMENTAL_GRAMMAR_VERSION,
    skeleton_registry,
    supplemental_skeleton_registry,
)
from our_system_phase2.services.expression_semantics import parse_expression
from our_system_phase2.services.real_market_validation import evaluate_panel_expression
from our_system_phase2.services.typed_route_compiler import TypedRouteCompiler
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
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


def test_supplemental_gap_generation_is_append_only_and_covers_seven_roots() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    grammar = CompositionalGrammarV2(registry)
    base_rows: list[list[object]] = []
    base_exact: set[str] = set()
    for route_id in ROUTE_IDS:
        for index in range(len(skeleton_registry()[route_id])):
            pair = grammar.propose(route_id, attempt_index=index, seed=20260718)
            base_rows.append(
                [
                    route_id,
                    index,
                    pair.primary["candidate_id"],
                    pair.primary["exact_identity"],
                    pair.control["exact_identity"],
                ]
            )
            base_exact.update(
                (pair.primary["exact_identity"], pair.control["exact_identity"])
            )
    base_digest = hashlib.sha256(
        json.dumps(
            base_rows,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert base_digest == "8fb0d9cc7d9f9fa6d255596da4145e6b33caf1ea792648ff2fc6989f964c3b4c"

    targets = {
        "fund_disclosure_balance_age_sessions",
        "fund_disclosure_profit_age_sessions",
        "fund_disclosure_cashflow_age_sessions",
        "fund_disclosure_holder_age_sessions",
        "ctx_hfq_is_st",
        "ctx_hfq_prev_is_limit_up",
        "state_close_range_location_sign",
    }
    observed: set[str] = set()
    supplemental_exact: set[str] = set()
    assert set(supplemental_skeleton_registry()) == {
        "SLOW_CROSS_SECTIONAL_LEVEL",
        "MARKET_REGIME_CONDITION",
        "INTRADAY_STATE_TRANSITION",
    }
    for route_id in supplemental_skeleton_registry():
        for index in range(64):
            pair = grammar.propose_supplemental(
                route_id,
                attempt_index=index,
                seed=20260718,
            )
            repeated = grammar.propose_supplemental(
                route_id,
                attempt_index=index,
                seed=20260718,
            )
            assert pair.primary["candidate_id"] == repeated.primary["candidate_id"]
            assert pair.primary["exact_identity"] == repeated.primary["exact_identity"]
            for candidate in (pair.primary, pair.control):
                assert candidate["legal"] is True
                assert candidate["generator_version"] == SUPPLEMENTAL_GRAMMAR_VERSION
                assert candidate["supplemental_delta_only"] is True
                assert candidate["existing_pack_rewrite_allowed"] is False
                assert _call_depth(candidate["canonical_expression"]) <= 4
                assert 1 <= len(candidate["declared_field_ids"]) <= 3
            supplemental_exact.update(
                (pair.primary["exact_identity"], pair.control["exact_identity"])
            )
            observed.update(set(pair.primary["declared_field_ids"]) & targets)
            if route_id == "MARKET_REGIME_CONDITION":
                assert set(pair.primary["condition_field_ids"]) == (
                    set(pair.primary["market_condition_field_ids"])
                    | set(pair.primary["stock_context_field_ids"])
                )
            if route_id == "INTRADAY_STATE_TRANSITION":
                assert pair.primary["state_materialization_required"] is True
                assert pair.primary["state_source_field_ids"] == ["close", "high", "low"]
                assert pair.primary["claimed_state_field_id"] == (
                    "state_close_range_location_sign"
                )

    assert observed == targets
    assert not base_exact.intersection(supplemental_exact)


def test_supplemental_roots_require_exact_independent_authority_envelope() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    grammar = CompositionalGrammarV2(registry)
    compiler = TypedRouteCompiler(registry)
    pair = grammar.propose_supplemental(
        "MARKET_REGIME_CONDITION",
        attempt_index=0,
        seed=20260718,
    )
    forged = dict(pair.primary, supplemental_authority_id="forged-authority")

    verdict = compiler.compile(forged)

    assert verdict.legal is False
    assert verdict.rejection_code == "ROUTE_NOT_ALLOWED"
    assert "append-only authority envelope" in verdict.reason


def test_supplemental_market_receipt_rejects_forged_context_partition() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    grammar = CompositionalGrammarV2(registry)
    compiler = TypedRouteCompiler(registry)
    pair = grammar.propose_supplemental(
        "MARKET_REGIME_CONDITION",
        attempt_index=0,
        seed=20260718,
    )
    forged = dict(pair.primary)
    forged["condition_field_ids"] = list(forged["market_condition_field_ids"])

    verdict = compiler.compile(forged)

    assert verdict.legal is False
    assert verdict.rejection_code == "ENTITY_SCOPE_MISMATCH"
    assert "partition market and stock contexts" in verdict.reason


def test_supplemental_state_receipt_rejects_forged_materialization_lineage() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    grammar = CompositionalGrammarV2(registry)
    compiler = TypedRouteCompiler(registry)
    pair = grammar.propose_supplemental(
        "INTRADAY_STATE_TRANSITION",
        attempt_index=0,
        seed=20260718,
    )
    forged = dict(pair.primary, state_source_field_ids=["close", "low", "high"])

    verdict = compiler.compile(forged)

    assert verdict.legal is False
    assert verdict.rejection_code == "STATE_FIELD_NOT_CONSUMED"
    assert "lineage does not match registry" in verdict.reason


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
        assert pair.primary["condition_field_ids"] == pair.control["condition_field_ids"]
        assert len(pair.primary["condition_field_ids"]) == 1
        if pair.primary["expression"].startswith("EventWindow("):
            arguments = pair.primary["expression"][len("EventWindow(") :].split(",")
            assert arguments[0] != arguments[1]


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


def test_persistence_skeletons_feed_registered_binary_states() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))

    for route_id in (
        "FIRSTN_PATH",
        "SLOW_TEMPORAL_CHANGE",
        "MARKET_REGIME_CONDITION",
        "INTRADAY_STATE_TRANSITION",
    ):
        expressions = [
            grammar.propose(route_id, attempt_index=index, seed=130363).primary[
                "expression"
            ]
            for index in range(8)
        ]
        persistence = [value for value in expressions if "Persistence(" in value]
        assert persistence, route_id
        assert all("Persistence(Positive(" in value for value in persistence)


def test_all_search_skeletons_execute_against_typed_synthetic_inputs() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))
    pairs = {
        route_id: [
            grammar.propose(route_id, attempt_index=index, seed=155921)
            for index in range(8)
        ]
        for route_id in ROUTE_IDS
        if route_id != "BROAD_EVENT_FROZEN_ENTRY"
    }
    times = pd.date_range("2025-01-02 09:31", periods=30, freq="min")
    frame = pd.DataFrame(
        {
            "code": np.repeat([f"S{index}" for index in range(6)], len(times)),
            "trade_time": np.tile(times, 6),
        }
    )
    position = np.arange(len(frame), dtype=float)
    for route_id, route_pairs in pairs.items():
        for pair in route_pairs:
            for field_index, field_id in enumerate(pair.primary["declared_field_ids"]):
                if route_id == "DISCLOSURE_EVENT":
                    frame[field_id] = ((position + field_index) % 11 == 0).astype(float)
                else:
                    frame[field_id] = ((position + 3 * field_index) % 17) - 8.0
            for candidate in (pair.primary, pair.control):
                result = evaluate_panel_expression(
                    frame,
                    candidate["expression"],
                    data_role="development",
                )
                assert len(result) == len(frame)
                assert not np.isinf(result.to_numpy(dtype=float)).any()


def test_all_compositional_pairs_freeze_shared_support_and_maturity_alignment() -> None:
    grammar = CompositionalGrammarV2(UnifiedCapabilityRegistry.read(REGISTRY))

    for route_id in ROUTE_IDS:
        pair = grammar.propose(route_id, attempt_index=3, seed=155921)
        for member in (pair.primary, pair.control):
            assert member["pair_support_alignment_policy"] == (
                "PRIMARY_CONTROL_FINITE_INTERSECTION_AT_SHARED_COORDINATE"
            )
            assert member["pair_maturity_alignment_policy"] == (
                "MAX_PRIMARY_CONTROL_MATURITY_BEFORE_SHARED_SUPPORT"
            )
            assert "SAME_SUPPORT_COORDINATES" in member["pair_mapping_portfolio_contract"]


def test_route_root_allowlist_is_enforced_without_changing_registry_authority() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    grammar = CompositionalGrammarV2(
        registry,
        route_root_allowlist={
            "MINUTE_STATIC": ["amount", "close", "ret_1m", "plate_peer_return_mean"]
        },
        enforce_route_compatibility=True,
    )

    for index in range(32):
        pair = grammar.propose("MINUTE_STATIC", attempt_index=index, seed=20260718)
        assert pair.primary["legal"] is True
        assert pair.control["legal"] is True
        assert set(pair.primary["declared_field_ids"]) <= {
            "amount", "close", "ret_1m", "plate_peer_return_mean"
        }

    try:
        CompositionalGrammarV2(
            registry,
            route_root_allowlist={"MINUTE_STATIC": ["chip_cost_p50"]},
        )
    except ValueError as exc:
        assert "not eligible on route" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("route-ineligible proposal root was accepted")
