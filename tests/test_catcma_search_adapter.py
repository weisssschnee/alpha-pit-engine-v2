from __future__ import annotations

from collections import OrderedDict
import importlib.util
from pathlib import Path

import numpy as np
import pytest

CMAES_AVAILABLE = importlib.util.find_spec("cmaes") is not None
requires_cmaes = pytest.mark.skipif(
    not CMAES_AVAILABLE,
    reason="official cmaes package is required for optimizer-state tests",
)

from our_system_phase2.runtime.cn_search_policy_qualification import (
    POPULATION_SIZE,
    build_baseline_population,
    build_final_verdict,
)
from our_system_phase2.services.catcma_search_adapter import (
    BEHAVIOR_BLOCKED,
    CONTROL_BLOCKED,
    DETERMINISTIC_INVALID,
    EVALUATED,
    INFRASTRUCTURE_FAILURE,
    SUPPORT_BLOCKED,
    CatCMASearchAdapter,
    ExactGeneSemantics,
    rank_population_observations,
)
from our_system_phase2.services import catcma_search_adapter as catcma_module
from our_system_phase2.services.compositional_grammar import (
    CompositionalGrammarV2,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)


class _FakeCatCMA:
    def __init__(
        self,
        *,
        c_space: list[int],
        population_size: int,
        seed: int,
    ) -> None:
        self.c_space = tuple(c_space)
        self.population_size = population_size
        self.seed = seed

    def ask(self):
        width = max(self.c_space)
        categorical = np.zeros((len(self.c_space), width), dtype=float)
        for index, category_count in enumerate(self.c_space):
            categorical[index, category_count - 1] = 1.0
        return type("FakeSolution", (), {"c": categorical})()


def _semantics(
    *,
    categories: OrderedDict[str, tuple[str, ...]] | None = None,
    compatibility: dict[str, object] | None = None,
) -> ExactGeneSemantics:
    ordered = categories or OrderedDict(
        (
            ("skeleton_id", ("skeleton_a", "skeleton_b", "skeleton_c")),
            ("field_id", ("field_a", "field_b", "field_c", "field_d")),
            ("window_id", ("3", "5", "10", "20")),
        )
    )
    return ExactGeneSemantics.freeze(
        route_id="DISCLOSURE_EVENT",
        ordered_categories_by_slot=ordered,
        none_semantics={
            slot: "NONE_NOT_PRESENT_SLOT_REQUIRED" for slot in ordered
        },
        skeleton_compatibility=compatibility
        or {
            skeleton: {
                "required_slots": list(ordered),
                "inactive_slots": [],
            }
            for skeleton in ordered["skeleton_id"]
        },
        registry_hash="registry-hash",
        root_contract_hash="root-contract-hash",
        grammar_hash="grammar-hash",
    )


def _observations(
    asked: list[dict[str, object]],
) -> list[dict[str, object]]:
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
            "exact_identity": proposal["proposal_id"],
            "outcome_class": outcome,
        }
        if outcome == EVALUATED:
            row["signed_matched_increment"] = 1.0 - (index * 0.25)
        rows.append(row)
    return rows


def test_exact_gene_semantics_binds_slot_category_order_and_compatibility() -> None:
    original = _semantics()
    reordered_categories = OrderedDict(
        (
            ("skeleton_id", ("skeleton_c", "skeleton_b", "skeleton_a")),
            ("field_id", ("field_a", "field_b", "field_c", "field_d")),
            ("window_id", ("3", "5", "10", "20")),
        )
    )
    reordered = _semantics(categories=reordered_categories)
    changed = _semantics(
        compatibility={
            "skeleton_a": {"required_slots": ["skeleton_id"]},
            "skeleton_b": {"required_slots": ["skeleton_id"]},
            "skeleton_c": {"required_slots": ["skeleton_id"]},
        }
    )

    assert original.exact_gene_semantics_hash != (
        reordered.exact_gene_semantics_hash
    )
    assert original.exact_gene_semantics_hash != changed.exact_gene_semantics_hash
    assert original.ordered_gene_slot_names == (
        "skeleton_id",
        "field_id",
        "window_id",
    )


def test_exact_gene_semantics_supports_one_fixed_skeleton_lane() -> None:
    semantics = ExactGeneSemantics.freeze(
        route_id="MINUTE_STATIC",
        ordered_categories_by_slot=OrderedDict(
            (
                ("skeleton_id", ("cn.comp.v2.minute_static.normalized_level",)),
                ("primary_field_id", ("amount", "ret_1m")),
            )
        ),
        none_semantics={
            "skeleton_id": "FIXED_ROUTE_LOCAL_GENERATION_LANE",
            "primary_field_id": "NONE_NOT_PRESENT_ACTIVE_SLOT",
        },
        skeleton_compatibility={
            "cn.comp.v2.minute_static.normalized_level": {
                "required_slots": ["skeleton_id", "primary_field_id"],
                "inactive_slots": [],
            }
        },
        registry_hash="registry-hash",
        root_contract_hash="root-contract-hash",
        grammar_hash="grammar-hash",
    )

    assert semantics.ordered_category_ids_by_slot[0] == (
        "cn.comp.v2.minute_static.normalized_level",
    )
    assert semantics.ordered_category_ids_by_slot[1] == (
        "amount",
        "ret_1m",
    )


def test_adapter_removes_fixed_slots_from_optimizer_and_rehydrates_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        catcma_module,
        "_load_catcmawm",
        lambda: (_FakeCatCMA, "0.13.0", "fake-cmaes"),
    )
    semantics = ExactGeneSemantics.freeze(
        route_id="MINUTE_STATIC",
        ordered_categories_by_slot=OrderedDict(
            (
                ("skeleton_id", ("fixed-skeleton",)),
                ("gene_surface_id", ("fixed-surface",)),
                ("primary_field_id", ("field-a", "field-b")),
                ("window_id", ("3", "5", "10")),
            )
        ),
        none_semantics={
            "skeleton_id": "FIXED_ROUTE_LOCAL_GENERATION_LANE",
            "gene_surface_id": "FIXED_GENE_SURFACE_VERSION",
            "primary_field_id": "NONE_NOT_PRESENT_ACTIVE_SLOT",
            "window_id": "NONE_NOT_PRESENT_ACTIVE_SLOT",
        },
        skeleton_compatibility={
            "fixed-skeleton": {
                "required_slots": [
                    "skeleton_id",
                    "gene_surface_id",
                    "primary_field_id",
                    "window_id",
                ],
                "inactive_slots": [],
            }
        },
        registry_hash="registry-hash",
        root_contract_hash="root-contract-hash",
        grammar_hash="grammar-hash",
    )

    adapter = CatCMASearchAdapter(
        semantics=semantics,
        seed=17,
        population_size=2,
    )
    asked = adapter.ask_population(checkpoint_id="checkpoint_001")

    assert adapter._optimizer.c_space == (2, 3)
    assert asked[0]["genes"] == {
        "skeleton_id": "fixed-skeleton",
        "gene_surface_id": "fixed-surface",
        "primary_field_id": "field-b",
        "window_id": "10",
    }
    assert adapter.environment_receipt()[
        "fixed_categorical_gene_slot_count"
    ] == 2


def test_equivalent_blockers_receive_equal_optimizer_loss() -> None:
    ranked = {
        row["proposal_id"]: row
        for row in rank_population_observations(
            [
                {
                    "proposal_id": "blocked-z",
                    "exact_identity": "z",
                    "outcome_class": SUPPORT_BLOCKED,
                },
                {
                    "proposal_id": "blocked-a",
                    "exact_identity": "a",
                    "outcome_class": SUPPORT_BLOCKED,
                },
                {
                    "proposal_id": "invalid-z",
                    "exact_identity": "z",
                    "outcome_class": DETERMINISTIC_INVALID,
                },
                {
                    "proposal_id": "invalid-a",
                    "exact_identity": "a",
                    "outcome_class": DETERMINISTIC_INVALID,
                },
            ]
        )
    }

    assert ranked["blocked-z"]["loss"] == ranked["blocked-a"]["loss"]
    assert ranked["invalid-z"]["loss"] == ranked["invalid-a"]["loss"]
    assert ranked["blocked-z"]["loss"] < ranked["invalid-z"]["loss"]


@requires_cmaes
def test_population_is_complete_and_decodes_every_gene_slot() -> None:
    adapter = CatCMASearchAdapter(
        semantics=_semantics(),
        seed=17,
        population_size=6,
    )
    asked = adapter.ask_population(checkpoint_id="checkpoint_001")

    assert len(asked) == 6
    assert all(
        tuple(row["genes"]) == adapter.semantics.ordered_gene_slot_names
        for row in asked
    )
    with pytest.raises(RuntimeError, match="ASK_BEFORE_PENDING"):
        adapter.ask_population(checkpoint_id="checkpoint_001")
    with pytest.raises(ValueError, match="exactly one observation"):
        adapter.tell_population(_observations(asked)[:-1])

    receipt = adapter.tell_population(_observations(asked))
    assert receipt["optimizer_state_changed"] is True
    assert adapter.generation == 1
    assert adapter.has_pending_population is False
    assert len(receipt["observations"]) == 6


@requires_cmaes
def test_infrastructure_failure_invalidates_whole_population() -> None:
    adapter = CatCMASearchAdapter(
        semantics=_semantics(),
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
    ranked = {
        row["proposal_id"]: row for row in rank_population_observations(rows)
    }

    assert ranked["positive"]["loss"] == 0.0
    assert ranked["negative"]["loss"] == 1.0
    assert ranked["support"]["loss"] == 2.0
    assert ranked["invalid"]["loss"] == 3.0


@requires_cmaes
def test_genesis_replay_reproduces_next_population_exactly() -> None:
    semantics = _semantics()
    adapter = CatCMASearchAdapter(
        semantics=semantics,
        seed=23,
        population_size=6,
    )
    asked = adapter.ask_population(checkpoint_id="checkpoint_001")
    adapter.tell_population(_observations(asked))
    expected = adapter.next_ask_preview(checkpoint_id="checkpoint_002")

    replayed = CatCMASearchAdapter.replay(
        semantics=semantics,
        seed=23,
        population_size=6,
        generations=adapter.history,
    )
    actual = replayed.ask_population(checkpoint_id="checkpoint_002")

    assert [row["genes"] for row in actual] == expected


@requires_cmaes
def test_tell_rejects_unasked_proposals() -> None:
    adapter = CatCMASearchAdapter(
        semantics=_semantics(),
        seed=29,
        population_size=6,
    )
    asked = adapter.ask_population(checkpoint_id="checkpoint_001")
    observations = _observations(asked)
    observations[0] = {**observations[0], "proposal_id": "not-asked"}

    with pytest.raises(ValueError, match="currently asked population"):
        adapter.tell_population(observations)


def test_registry_baseline_schedules_attempts_without_searching_past_them() -> None:
    first = build_baseline_population(
        route_id="DISCLOSURE_EVENT",
        checkpoint_index=0,
        seed=101,
    )
    second = build_baseline_population(
        route_id="DISCLOSURE_EVENT",
        checkpoint_index=1,
        seed=101,
    )

    assert len(first) == len(second) == POPULATION_SIZE
    assert {row["generation_attempt_index"] for row in first}.isdisjoint(
        {row["generation_attempt_index"] for row in second}
    )
    assert [row["generation_attempt_index"] for row in first] == list(
        range(POPULATION_SIZE)
    )


def test_authoritative_grammar_materializes_exact_categorical_genes() -> None:
    grammar = CompositionalGrammarV2(
        UnifiedCapabilityRegistry.read(REGISTRY),
        enforce_route_compatibility=True,
    )
    for route_id in (
        "INTRADAY_STATE_TRANSITION",
        "DISCLOSURE_EVENT",
        "SLOW_TEMPORAL_CHANGE",
    ):
        space = grammar.categorical_gene_space(route_id)
        categories = space["ordered_categories_by_slot"]
        candidate_genes = {
            slot: values[0] for slot, values in categories.items()
        }
        pair = None
        # Historical route-wide qualification surfaces remain replayable,
        # including their deterministic-invalid combinations. New large
        # search uses compatibility-qualified skeleton lanes instead.
        for primary in categories[
            "state_field_id"
            if route_id == "INTRADAY_STATE_TRANSITION"
            else (
                "primary_field_id"
                if route_id == "SLOW_TEMPORAL_CHANGE"
                else "event_field_id"
            )
        ]:
            candidate_genes[
                "state_field_id"
                if route_id == "INTRADAY_STATE_TRANSITION"
                else (
                    "primary_field_id"
                    if route_id == "SLOW_TEMPORAL_CHANGE"
                    else "event_field_id"
                )
            ] = primary
            try:
                pair = grammar.propose_from_categorical_genes(
                    route_id, genes=candidate_genes
                )
                break
            except ValueError:
                continue
        assert pair is not None
        assert pair.primary["legal"] is True
        assert pair.control["legal"] is True
        assert pair.primary["categorical_genes"] == candidate_genes
        assert pair.primary["categorical_gene_construction"] == (
            "AUTHORITATIVE_GRAMMAR_V2"
        )
        assert "categorical_gene_surface_version" not in pair.primary
        assert "generation_mode" not in pair.primary
        again = grammar.propose_from_categorical_genes(
            route_id, genes=candidate_genes
        )
        assert again.primary["exact_identity"] == pair.primary["exact_identity"]
        assert again.primary["pair_id"] == pair.primary["pair_id"]


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
