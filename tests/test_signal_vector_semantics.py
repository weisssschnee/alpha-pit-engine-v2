from __future__ import annotations

import numpy as np

from our_system_phase2.services.signal_vector_semantics import (
    build_signal_semantic_diagnostics,
    classify_candidate_signal_semantics,
)


def _diagnostics(candidate_id: str, signal: list[float], rank: list[float]) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "shard_index": 0,
        **build_signal_semantic_diagnostics(
            np.asarray(signal, dtype=float),
            np.asarray(rank, dtype=float),
            sketch_size=len(signal),
        ),
    }


def test_rank_equivalent_candidates_are_collapsed_before_full_cm() -> None:
    candidates = [{"candidate_id": name} for name in ("a", "b", "c")]
    progress = [
        _diagnostics("a", [1, 2, 3, 4], [0.25, 0.50, 0.75, 1.00]),
        _diagnostics("b", [10, 20, 30, 40], [0.25, 0.50, 0.75, 1.00]),
        _diagnostics("c", [1, 3, 2, 4], [0.25, 0.75, 0.50, 1.00]),
    ]

    decisions = classify_candidate_signal_semantics(candidates, progress)

    assert decisions["a"]["signal_semantic_decision"] == "PASS"
    assert decisions["b"]["signal_semantic_decision"] == "REJECT_SIGNAL_EQUIVALENT"
    assert decisions["b"]["signal_equivalent_to"] == "a"
    assert decisions["c"]["signal_semantic_decision"] == "PASS"


def test_different_availability_masks_are_not_collapsed() -> None:
    candidates = [{"candidate_id": name} for name in ("a", "b")]
    progress = [
        _diagnostics("a", [1, 2, np.nan, np.nan], [0.5, 1.0, np.nan, np.nan]),
        _diagnostics("b", [np.nan, np.nan, 1, 2], [np.nan, np.nan, 0.5, 1.0]),
    ]

    decisions = classify_candidate_signal_semantics(candidates, progress)

    assert decisions["a"]["signal_semantic_decision"] == "PASS"
    assert decisions["b"]["signal_semantic_decision"] == "PASS"


def test_same_portfolio_buckets_are_collapsed_even_when_middle_ranks_differ() -> None:
    rank_a = np.linspace(0.05, 1.0, 20)
    rank_b = np.concatenate([rank_a[:4], rank_a[4:16][::-1], rank_a[16:]])
    candidates = [{"candidate_id": name} for name in ("a", "b")]
    progress = [
        _diagnostics("a", rank_a.tolist(), rank_a.tolist()),
        _diagnostics("b", rank_b.tolist(), rank_b.tolist()),
    ]

    decisions = classify_candidate_signal_semantics(
        candidates,
        progress,
        correlation_threshold=0.9999,
        position_overlap_threshold=0.99,
    )

    assert decisions["b"]["signal_semantic_decision"] == "REJECT_SIGNAL_EQUIVALENT"
    assert decisions["b"]["signal_semantic_reasons"] == "position_equivalent_portfolio_buckets"
    assert decisions["b"]["signal_equivalence_position_overlap"] == 1.0


def test_constant_and_tail_concentrated_signals_are_reported() -> None:
    constant = build_signal_semantic_diagnostics(
        np.ones(100, dtype=float),
        np.ones(100, dtype=float),
        sketch_size=32,
    )
    tail = build_signal_semantic_diagnostics(
        np.asarray([1.0] * 990 + [10_000.0] * 10),
        np.linspace(0.001, 1.0, 1000),
        sketch_size=64,
    )

    assert constant["signal_is_constant"] is True
    assert tail["signal_top1pct_abs_share"] > 0.9
    assert tail["signal_tail_concentration_flag"] is True
