from __future__ import annotations

from dataclasses import replace

import pytest

pytest.importorskip("cmaes")

from our_system_phase2.services.catcma_search_adapter import (
    BEHAVIOR_BLOCKED,
    CONTROL_BLOCKED,
    DETERMINISTIC_INVALID,
    EVALUATED,
    INFRASTRUCTURE_FAILURE,
    SUPPORT_BLOCKED,
    CatCMASearchAdapter,
    ExactGeneSemantics,
    ProposalCategory,
    rank_population_observations,
)
from our_system_phase2.runtime.cn_search_policy_qualification import (
    POPULATION_SIZE,
    build_baseline_population,
    build_final_verdict,
)


def _categories(count: int = 12) -> tuple[ProposalCategory, ...]:
    return tuple(
        ProposalCategory(
            category_id=f"category_{index:03d}",
            route_id="DISCLOSURE_EVENT",
            skeleton_id=f"disclosure.skeleton.{index % 3}",
            pair_id=f"pair_{index:03d}",
            exact_identity=f"exact_{index:03d}",
            primary={
                "candidate_id": f"candidate_{index:03d}",
                "pair_id": f"pair_{index:03d}",
                "expression": f"EventCount($field_{index:03d},5)",
            },
            control={
                "candidate_id": f"candidate_{index:03d}.control",
                "pair_id": f"pair_{index:03d}",
                "expression": f"TimeSince($field_{index:03d})",
            },
        )
        for index in range(count)
    )


def _semantics(
    categories: tuple[ProposalCategory, ...] | None = None,
) -> ExactGeneSemantics:
    return ExactGeneSemantics.freeze(
        route_id="DISCLOSURE_EVENT",
        categories=categories or _categories(),
        registry_hash="registry-hash",
        root_contract_hash="root-contract-hash",
        grammar_hash="grammar-hash",
    )


def _observations(asked: list[dict[str, object]]) -> list[dict[str, object]]:
    outcomes = (
        EVALUATED,
        EVALUATED,
        SUPPORT_BLOCKED,
        CONTROL_BLOCKED,
        BEHAVIOR_BLOCKED,
        DETERMINISTIC_INVALID,
    )
    rows = []
    for index, proposal in enumerate(asked):
        outcome = outcomes[index % len(outcomes)]
        row: dict[str, object] = {
            "proposal_id": proposal["proposal_id"],
            "exact_identity": proposal["exact_identity"],
            "outcome_class": outcome,
        }
        if outcome == EVALUATED:
            row["signed_matched_increment"] = 1.0 - (index * 0.25)
        rows.append(row)
    return rows


def test_exact_gene_semantics_binds_order_and_meaning() -> None:
    categories = _categories()
    original = _semantics(categories)
    reordered = _semantics(tuple(reversed(categories)))
    changed_skeleton = list(categories)
    changed_skeleton[0] = replace(
        changed_skeleton[0],
        skeleton_id="different.skeleton",
    )
    changed = _semantics(tuple(changed_skeleton))

    assert original.exact_gene_semantics_hash != reordered.exact_gene_semantics_hash
    assert original.exact_gene_semantics_hash != changed.exact_gene_semantics_hash
    assert original.ordered_gene_slot_names == ("proposal_category_id",)


def test_population_is_complete_and_state_changes_after_tell() -> None:
    categories = _categories()
    adapter = CatCMASearchAdapter(
        semantics=_semantics(categories),
        categories=categories,
        seed=17,
        population_size=6,
    )
    asked = adapter.ask_population(checkpoint_id="checkpoint_001")

    assert len(asked) == 6
    with pytest.raises(RuntimeError, match="ASK_BEFORE_PENDING"):
        adapter.ask_population(checkpoint_id="checkpoint_001")
    with pytest.raises(ValueError, match="exactly one observation"):
        adapter.tell_population(_observations(asked)[:-1])

    receipt = adapter.tell_population(_observations(asked))
    assert receipt["optimizer_state_changed"] is True
    assert adapter.generation == 1
    assert adapter.has_pending_population is False
    assert len(receipt["observations"]) == 6


def test_infrastructure_failure_invalidates_whole_population() -> None:
    categories = _categories()
    adapter = CatCMASearchAdapter(
        semantics=_semantics(categories),
        categories=categories,
        seed=19,
        population_size=6,
    )
    asked = adapter.ask_population(checkpoint_id="checkpoint_001")
    observations = _observations(asked)
    observations[0] = {
        **observations[0],
        "outcome_class": INFRASTRUCTURE_FAILURE,
    }

    with pytest.raises(RuntimeError, match="INVALIDATES_WHOLE_POPULATION"):
        adapter.tell_population(observations)
    assert adapter.generation == 0
    assert adapter.has_pending_population is True


def test_loss_ranking_keeps_financial_objective_separate_from_novelty() -> None:
    rows = [
        {
            "proposal_id": "positive",
            "exact_identity": "z",
            "outcome_class": EVALUATED,
            "signed_matched_increment": 0.5,
            "behavior_novelty": -999,
        },
        {
            "proposal_id": "negative",
            "exact_identity": "a",
            "outcome_class": EVALUATED,
            "signed_matched_increment": -0.2,
            "behavior_novelty": 999,
        },
        {
            "proposal_id": "support",
            "exact_identity": "b",
            "outcome_class": SUPPORT_BLOCKED,
        },
        {
            "proposal_id": "invalid",
            "exact_identity": "c",
            "outcome_class": DETERMINISTIC_INVALID,
        },
    ]
    ranked = {row["proposal_id"]: row for row in rank_population_observations(rows)}

    assert ranked["positive"]["loss"] == 0.0
    assert ranked["negative"]["loss"] == 1.0
    assert ranked["support"]["loss"] == 2.0
    assert ranked["invalid"]["loss"] == 3.0


def test_genesis_replay_reproduces_next_population_exactly() -> None:
    categories = _categories()
    semantics = _semantics(categories)
    adapter = CatCMASearchAdapter(
        semantics=semantics,
        categories=categories,
        seed=23,
        population_size=6,
    )
    asked = adapter.ask_population(checkpoint_id="checkpoint_001")
    adapter.tell_population(_observations(asked))
    expected = adapter.next_ask_preview(checkpoint_id="checkpoint_002")

    replayed = CatCMASearchAdapter.replay(
        semantics=semantics,
        categories=categories,
        seed=23,
        population_size=6,
        generations=adapter.history,
    )
    actual = replayed.ask_population(checkpoint_id="checkpoint_002")

    assert [row["category_id"] for row in actual] == expected


def test_tell_rejects_unasked_proposals() -> None:
    categories = _categories()
    adapter = CatCMASearchAdapter(
        semantics=_semantics(categories),
        categories=categories,
        seed=29,
        population_size=6,
    )
    asked = adapter.ask_population(checkpoint_id="checkpoint_001")
    observations = _observations(asked)
    observations[0] = {**observations[0], "proposal_id": "not-asked"}

    with pytest.raises(ValueError, match="currently asked population"):
        adapter.tell_population(observations)


def test_registry_baseline_consumes_disjoint_frozen_segments() -> None:
    categories = _categories(POPULATION_SIZE * 2)
    first = build_baseline_population(
        categories=categories,
        route_id="DISCLOSURE_EVENT",
        checkpoint_index=0,
    )
    second = build_baseline_population(
        categories=categories,
        route_id="DISCLOSURE_EVENT",
        checkpoint_index=1,
    )

    assert len(first) == len(second) == POPULATION_SIZE
    assert {row["category_id"] for row in first}.isdisjoint(
        {row["category_id"] for row in second}
    )
    assert [row["category_id"] for row in first] == [
        category.category_id for category in categories[:POPULATION_SIZE]
    ]


def _arm_metrics(
    *,
    positive_per_hour: float,
    median: float,
    behavior_rate: float,
    route_hhi: float,
    skeleton_hhi: float,
    category_share: float,
    pairs_per_hour: float,
    cores: float = 19.5,
    occupancy: float = 0.61,
) -> dict[str, object]:
    return {
        "positive_matched_pairs_per_wall_hour": positive_per_hour,
        "median_signed_matched_increment": median,
        "behavior_discovery_per_evaluated_pair": behavior_rate,
        "evaluated_route_hhi": route_hhi,
        "asked_skeleton_hhi": skeleton_hhi,
        "maximum_category_ask_share": category_share,
        "evaluated_pairs_per_wall_hour": pairs_per_hour,
        "tell_observations_by_route": {
            "INTRADAY_STATE_TRANSITION": 64,
            "DISCLOSURE_EVENT": 64,
            "SLOW_TEMPORAL_CHANGE": 64,
        },
        "active_effective_cores_median": cores,
        "active_host_logical_occupancy_median": occupancy,
        "minimum_free_memory_bytes": 40 * 1024**3,
        "maximum_observed_cache_bytes": 7 * 1024**3,
    }


def test_final_verdict_separates_quality_from_scale_readiness() -> None:
    baseline = _arm_metrics(
        positive_per_hour=10.0,
        median=0.1,
        behavior_rate=0.8,
        route_hhi=0.34,
        skeleton_hhi=0.20,
        category_share=0.02,
        pairs_per_hour=20.0,
    )
    catcma = _arm_metrics(
        positive_per_hour=11.0,
        median=0.1,
        behavior_rate=0.72,
        route_hhi=0.40,
        skeleton_hhi=0.25,
        category_share=0.10,
        pairs_per_hour=21.0,
    )

    passed = build_final_verdict(baseline, catcma)
    assert passed["SEARCH_POLICY_QUALITY"] == "PASS"
    assert passed["LARGE_SEARCH_SCALE_READINESS"] == "PASS"

    low_cpu = {
        **catcma,
        "active_effective_cores_median": 18.0,
        "active_host_logical_occupancy_median": 0.60,
    }
    failed_scale = build_final_verdict(baseline, low_cpu)
    assert failed_scale["SEARCH_POLICY_QUALITY"] == "PASS"
    assert failed_scale["LARGE_SEARCH_SCALE_READINESS"] == "FAIL"
