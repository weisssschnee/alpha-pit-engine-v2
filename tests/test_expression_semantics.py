from __future__ import annotations

from our_system_phase2.services.expression_semantics import analyze_expression
from our_system_phase2.services.typed_primitive_gate import validate_expression
from our_system_phase2.runtime.phase3bp_true1min_search_algorithm_smoke import (
    _add_candidate,
    _generate_event_state_candidates,
    _generate_orthogonal_candidates,
    begin_generation_accounting,
    end_generation_accounting,
)


def test_sign_of_cross_sectional_rank_is_blocked_as_availability_mask() -> None:
    analysis = analyze_expression("Sign(CSRank($event_count))")

    assert analysis.hard_blocked
    assert "DEGENERATE_SIGN_OF_POSITIVE_RANK" in analysis.issue_codes
    assert analysis.canonical_expression == "Sign(CSRank($event_count))"


def test_redundant_abs_of_rank_is_removed_before_memory_and_search() -> None:
    analysis = analyze_expression(
        "CSRank(Sub(Abs(CSRank($left)),Abs(CSRank($right))))"
    )

    assert not analysis.hard_blocked
    assert analysis.changed
    assert analysis.canonical_expression == "CSRank(Sub(CSRank($left),CSRank($right)))"
    assert analysis.issue_codes.count("REDUNDANT_ABS_OF_NONNEGATIVE") == 2


def test_signed_zscore_remains_a_valid_directional_transform() -> None:
    analysis = analyze_expression("Sign(ZScore($ret_1m))")

    assert not analysis.hard_blocked
    assert not analysis.changed
    assert analysis.issue_codes == ()


def test_sign_of_strictly_negative_rank_expression_is_also_blocked() -> None:
    analysis = analyze_expression("Sign(Neg(CSRank($ret_1m)))")

    assert analysis.hard_blocked
    assert "DEGENERATE_SIGN_OF_NEGATIVE_RANK" in analysis.issue_codes


def test_rank_denominator_is_blocked_but_epsilon_guard_is_audited() -> None:
    rank_denominator = analyze_expression("Div($left,CSRank($right))")
    guarded = analyze_expression("Div($left,Add(Abs($right),0.000001))")

    assert rank_denominator.hard_blocked
    assert "UNBOUNDED_RANK_DENOMINATOR" in rank_denominator.issue_codes
    assert not guarded.hard_blocked
    assert "EPSILON_ONLY_DIV_GUARD" in guarded.issue_codes


def test_shared_typed_gate_consults_semantic_value_domain_rules() -> None:
    verdict = validate_expression(
        "Sign(CSRank($event_count))",
        entry_lineage="test",
        materialization_stage="candidate_construction",
        candidate_role="true1min_search_candidate",
    )

    assert verdict.typed_gate_decision == "blocked_semantic_degeneracy"
    assert verdict.semantic_gate_decision == "REJECT_SEMANTIC_DEGENERACY"
    assert "DEGENERATE_SIGN_OF_POSITIVE_RANK" in verdict.semantic_issue_codes


def test_event_and_orthogonal_generators_emit_semantically_valid_directions() -> None:
    available_fields = {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "vwap",
        "ret_1m",
        "evt_uplimit_active",
        "evt_uplimit_fd_close",
        "evt_uplimit_auction_money",
        "evt_uplimit_auction_turnover",
        "evt_uplimit_auction_pre1max_ratio",
        "evt_uplimit_up_limit_keep_times",
        "ctx_hfq_pb",
        "ctx_hfq_market_cap_yuan",
    }
    policy = {"scores": {}}
    candidates = [
        *_generate_event_state_candidates(
            128,
            set(),
            policy,
            include_interactions=True,
            available_fields=available_fields,
        ),
        *_generate_orthogonal_candidates(
            128,
            set(),
            policy,
            available_fields=available_fields,
        ),
    ]

    assert len(candidates) == 256
    assert all("Sign(CSRank(" not in row["expression"] for row in candidates)
    assert all("Abs(CSRank(" not in row["expression"] for row in candidates)
    assert all(not analyze_expression(row["expression"]).hard_blocked for row in candidates)


def test_candidate_construction_rewrites_before_hashing_and_accounts_blocks() -> None:
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    previous, _ = begin_generation_accounting()
    try:
        _add_candidate(
            rows,
            seen,
            set(),
            "CSRank(Sub(Abs(CSRank($left)),Abs(CSRank($right))))",
            lane="test",
            source_generator="test",
            note="test",
            policy={"scores": {}},
        )
        _add_candidate(
            rows,
            seen,
            set(),
            "Sign(CSRank($left))",
            lane="test",
            source_generator="test",
            note="test",
            policy={"scores": {}},
        )
    finally:
        accounting = end_generation_accounting(previous)

    assert len(rows) == 1
    assert rows[0]["expression"] == "CSRank(Sub(CSRank($left),CSRank($right)))"
    assert rows[0]["semantic_rewritten"] == "true"
    assert accounting["accepted_unique"] == 1
    assert accounting["reject_semantic_degenerate"] == 1
