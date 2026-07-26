from __future__ import annotations

from collections import OrderedDict

from our_system_phase2.services.optuna_tpe_search_adapter import (
    EVALUATED,
    RouteConditionalTPESearchAdapter,
)


def _lanes():
    return OrderedDict(
        (
            (
                "route.skeleton.single",
                {
                    "ordered_categories_by_slot": OrderedDict(
                        (
                            (
                                "skeleton_id",
                                ["route.skeleton.single"],
                            ),
                            ("gene_surface_id", ["surface-v1"]),
                            ("primary_field_id", ["a", "b", "c"]),
                        )
                    )
                },
            ),
            (
                "route.skeleton.pair",
                {
                    "ordered_categories_by_slot": OrderedDict(
                        (
                            (
                                "skeleton_id",
                                ["route.skeleton.pair"],
                            ),
                            ("gene_surface_id", ["surface-v1"]),
                            (
                                "field_pair_id",
                                ["a::b", "a::c", "b::c"],
                            ),
                            ("window_id", ["5", "20"]),
                        )
                    )
                },
            ),
        )
    )


def _observations(asked):
    return [
        {
            "proposal_id": row["proposal_id"],
            "outcome_class": EVALUATED,
            "optimizer_reward": float(index),
            "outcome_reason": "",
        }
        for index, row in enumerate(asked)
    ]


def test_tpe_builds_complete_conditional_genes_and_factorizes_pairs() -> None:
    adapter = RouteConditionalTPESearchAdapter(
        route_id="TEST_ROUTE",
        lane_spaces=_lanes(),
        seed=17,
        n_startup_trials=2,
        n_ei_candidates=8,
    )
    asked = adapter.ask_population(
        checkpoint_id="checkpoint_001",
        count=8,
    )

    assert len(asked) == 8
    assert len({row["proposal_id"] for row in asked}) == 8
    for row in asked:
        genes = row["genes"]
        assert genes["gene_surface_id"] == "surface-v1"
        if genes["skeleton_id"].endswith(".pair"):
            assert genes["field_pair_id"] in {"a::b", "a::c", "b::c"}
            assert row["typed_pair_compatible"] is True
            assert genes["window_id"] in {"5", "20"}
        else:
            assert genes["primary_field_id"] in {"a", "b", "c"}

    receipt = adapter.tell_population(_observations(asked))
    assert receipt["completed_count"] == 8
    assert receipt["failed_count"] == 0
    assert adapter.has_pending_population is False


def test_failed_nonfinancial_attempts_do_not_enter_tpe_reward() -> None:
    adapter = RouteConditionalTPESearchAdapter(
        route_id="TEST_ROUTE",
        lane_spaces=_lanes(),
        seed=19,
        n_startup_trials=2,
        n_ei_candidates=8,
    )
    asked = adapter.ask_population(
        checkpoint_id="checkpoint_001",
        count=4,
    )
    observations = _observations(asked)
    observations[0] = {
        "proposal_id": asked[0]["proposal_id"],
        "outcome_class": "BEHAVIOR_BLOCKED",
        "optimizer_reward": None,
        "outcome_reason": "BEHAVIOR_DUPLICATE",
    }

    receipt = adapter.tell_population(observations)

    assert receipt["completed_count"] == 3
    assert receipt["failed_count"] == 1


def test_genesis_transcript_replay_reproduces_next_batch() -> None:
    adapter = RouteConditionalTPESearchAdapter(
        route_id="TEST_ROUTE",
        lane_spaces=_lanes(),
        seed=23,
        n_startup_trials=2,
        n_ei_candidates=8,
    )
    first = adapter.ask_population(
        checkpoint_id="checkpoint_001",
        count=12,
    )
    adapter.tell_population(_observations(first))
    second = adapter.ask_population(
        checkpoint_id="checkpoint_002",
        count=6,
    )
    expected_second_genes = [row["genes"] for row in second]

    replayed = RouteConditionalTPESearchAdapter.replay(
        route_id="TEST_ROUTE",
        lane_spaces=_lanes(),
        seed=23,
        transcripts=adapter.history[:1],
        n_startup_trials=2,
        n_ei_candidates=8,
    )
    actual_second = replayed.ask_population(
        checkpoint_id="checkpoint_002",
        count=6,
    )

    assert [row["genes"] for row in actual_second] == expected_second_genes


def test_environment_receipt_forbids_a_new_optimizer_database() -> None:
    adapter = RouteConditionalTPESearchAdapter(
        route_id="TEST_ROUTE",
        lane_spaces=_lanes(),
        seed=29,
    )

    receipt = adapter.environment_receipt()

    assert receipt["package_version"] == "4.8.0"
    assert receipt["persistent_database"] is False
    assert receipt["restore_authority"] == (
        "IMMUTABLE_ASK_TELL_TRANSCRIPT_REPLAY"
    )
