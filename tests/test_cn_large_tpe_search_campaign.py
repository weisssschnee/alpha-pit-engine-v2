from __future__ import annotations

from collections import Counter
from pathlib import Path

import numpy as np
import pytest

from scripts.run_cn_phase3cm_streaming_qualification import _finalize_pairs
from our_system_phase2.runtime.cn_large_tpe_search_campaign import (
    ASKS_PER_CHECKPOINT,
    CAMPAIGN_PROFILE,
    MAXIMUM_RAW_ASKS,
    MINIMUM_ACTUAL_EVALUATED_PAIRS,
    N_EI_CANDIDATES,
    OPTUNA_ROUTE_WORKERS,
    PAIR_BATCH_SIZES,
    ROUTE_EVALUATED_TARGETS,
    ROUTES,
    TPE_GROUP,
    TPE_MULTIVARIATE,
    TPE_SAMPLER_MODE,
    _allocate_checkpoint_asks,
    _ask_route_populations,
    _conservative_search_score,
    _freeze_gene_lanes,
    _restore_or_import_adapters,
    _select_validation_finalists,
    _validation_eligible,
)


def test_large_contract_is_five_digit_actual_evaluated_not_scheduled() -> None:
    assert CAMPAIGN_PROFILE == "cn_large_optuna_tpe_actual20000_v2"
    assert MINIMUM_ACTUAL_EVALUATED_PAIRS == 20_000
    assert sum(ROUTE_EVALUATED_TARGETS.values()) == 20_000
    assert set(ROUTE_EVALUATED_TARGETS) == set(ROUTES)
    assert "MINUTE_STATIC" not in ROUTES
    assert "INTRADAY_STATE_TRANSITION" not in ROUTES
    assert PAIR_BATCH_SIZES == {
        "active_bar": 12,
        "stock_session": 12,
    }
    assert ASKS_PER_CHECKPOINT == 3_072
    assert MAXIMUM_RAW_ASKS == 73_728
    assert OPTUNA_ROUTE_WORKERS == len(ROUTES)
    assert N_EI_CANDIDATES == 24
    assert TPE_SAMPLER_MODE == (
        "OFFICIAL_DEFAULT_UNIVARIATE_CONSTANT_LIAR"
    )
    assert TPE_MULTIVARIATE is False
    assert TPE_GROUP is False


def _evaluated_outcome(
    *,
    pair_id: str = "pair-1",
    route_id: str = "SLOW_TEMPORAL_CHANGE",
    primary: float,
    control: float,
    decision: str = "TRAIN_REWARD_FOLLOWUP_READY",
) -> dict[str, object]:
    matched = primary - control
    return {
        "pair_id": pair_id,
        "route_id": route_id,
        "pair_evaluation_status": "PAIR_EVALUATED",
        "primary_composite_reward": primary,
        "control_composite_reward": control,
        "matched_train_increment": matched,
        "pair_train_reward": matched,
        "primary_standalone_train_reward_decision": decision,
    }


def test_conservative_search_score_cannot_promote_bad_primary_against_worse_control() -> None:
    bad_primary = _evaluated_outcome(
        primary=-0.3,
        control=-1.3,
        decision="HOLD_TRAIN_REWARD",
    )
    good_primary = _evaluated_outcome(primary=0.7, control=0.5)
    negative_increment = _evaluated_outcome(primary=0.7, control=0.9)

    assert _conservative_search_score(bad_primary) == pytest.approx(-0.3)
    assert _conservative_search_score(good_primary) == pytest.approx(0.2)
    assert _conservative_search_score(negative_increment) == pytest.approx(-0.2)
    assert _validation_eligible(bad_primary) is False
    assert _validation_eligible(good_primary) is True
    assert _validation_eligible(negative_increment) is False


def test_validation_finalists_require_standalone_ready_and_positive_increment() -> None:
    outcomes = []
    candidates = []
    behavior = []
    for index in range(256):
        pair_id = f"pair-{index:03d}"
        route_id = ROUTES[index % len(ROUTES)]
        outcomes.append(
            _evaluated_outcome(
                pair_id=pair_id,
                route_id=route_id,
                primary=1.0 + index / 1000.0,
                control=0.5,
            )
        )
        candidates.extend(
            [
                {"pair_id": pair_id, "pair_member_role": "PRIMARY"},
                {"pair_id": pair_id, "pair_member_role": "CONTROL"},
            ]
        )
        behavior.append(
            {
                "pair_id": pair_id,
                "portfolio_behavior_family_id": f"family-{index:03d}",
            }
        )
    rejected_pair = "pair-rejected"
    outcomes.append(
        _evaluated_outcome(
            pair_id=rejected_pair,
            route_id="MARKET_REGIME_CONDITION",
            primary=9.0,
            control=-9.0,
            decision="HOLD_TRAIN_REWARD",
        )
    )
    candidates.extend(
        [
            {"pair_id": rejected_pair, "pair_member_role": "PRIMARY"},
            {"pair_id": rejected_pair, "pair_member_role": "CONTROL"},
        ]
    )
    behavior.append(
        {
            "pair_id": rejected_pair,
            "portfolio_behavior_family_id": "family-rejected",
        }
    )

    selected = _select_validation_finalists(
        candidates=candidates,
        outcomes=outcomes,
        behavior_rows=behavior,
    )

    assert len(selected) == 512
    assert rejected_pair not in {str(row["pair_id"]) for row in selected}


class _PairReducer:
    stats = np.asarray([[[0.0, 1.0]], [[0.0, 2.0]]], dtype=float)

    @staticmethod
    def behavior_identity(index: int) -> str:
        return f"behavior-{index}"


class _PairSupport:
    @staticmethod
    def identities() -> dict[str, dict[str, object]]:
        return {
            "pair-1": {
                "count": 10,
                "support_identity": "support-1",
            }
        }


def test_streaming_pair_outcome_preserves_primary_control_and_standalone_decision() -> None:
    rows = _finalize_pairs(
        candidates=[
            {"candidate_id": "primary-1", "pair_id": "pair-1"},
            {"candidate_id": "control-1", "pair_id": "pair-1"},
        ],
        reward_rows=[
            {
                "candidate_id": "primary-1",
                "optimizer_reward": 0.7,
                "train_reward_decision": "TRAIN_REWARD_FOLLOWUP_READY",
                "train_reward_blockers": "",
            },
            {
                "candidate_id": "control-1",
                "optimizer_reward": 0.5,
                "train_reward_decision": "TRAIN_REWARD_FOLLOWUP_READY",
                "train_reward_blockers": "",
            },
        ],
        reducer=_PairReducer(),
        support=_PairSupport(),
        binding={
            "candidate_members": [
                {"candidate_id": "primary-1", "receipt_hash": "primary-hash"},
                {"candidate_id": "control-1", "receipt_hash": "control-hash"},
            ],
            "pairs": [
                {"pair_id": "pair-1", "pair_receipt_hash": "pair-hash"}
            ],
        },
    )

    assert rows[0]["primary_composite_reward"] == pytest.approx(0.7)
    assert rows[0]["control_composite_reward"] == pytest.approx(0.5)
    assert rows[0]["matched_train_increment"] == pytest.approx(0.2)
    assert rows[0]["optimizer_reward"] == pytest.approx(0.2)
    assert rows[0]["primary_standalone_train_reward_decision"] == (
        "TRAIN_REWARD_FOLLOWUP_READY"
    )


class _DeterministicRouteAdapter:
    def __init__(self, route_id: str) -> None:
        self.route_id = route_id
        self.received_expected: object = None

    def ask_population(
        self,
        *,
        checkpoint_id: str,
        count: int,
        expected_genes: object = None,
    ) -> list[dict[str, object]]:
        self.received_expected = expected_genes
        return [
            {
                "route_id": self.route_id,
                "checkpoint_id": checkpoint_id,
                "ordinal": ordinal,
            }
            for ordinal in range(count)
        ]


def test_parallel_route_ask_preserves_schedule_order_and_replay_genes() -> None:
    scheduled_routes = [ROUTES[2], ROUTES[0], ROUTES[4]]
    schedule = [
        {"route_id": route_id, "asked_pairs": index + 1}
        for index, route_id in enumerate(scheduled_routes)
    ]
    adapters = {
        route_id: _DeterministicRouteAdapter(route_id)
        for route_id in scheduled_routes
    }
    expected = {
        route_id: [{"slot": f"expected-{route_id}"}]
        for route_id in scheduled_routes
    }

    asked, audit = _ask_route_populations(
        schedule=schedule,
        adapters=adapters,
        checkpoint_id="checkpoint_011",
        expected_by_route=expected,
        replay_existing=True,
    )

    assert [row["route_id"] for row in asked] == [
        route_id
        for index, route_id in enumerate(scheduled_routes)
        for _ in range(index + 1)
    ]
    assert all(
        adapters[route_id].received_expected == expected[route_id]
        for route_id in scheduled_routes
    )
    assert audit["execution"] == (
        "PROCESS_PARALLEL_ROUTE_LOCAL_OPTUNA_STUDIES"
    )
    assert audit["route_worker_count"] == len(scheduled_routes)
    assert audit["asked_pairs"] == sum(range(1, 4))


def test_optimizer_snapshot_restores_without_transcript_resampling(
    tmp_path: Path,
) -> None:
    lanes = {
        route_id: {
            f"{route_id}.single": {
                "ordered_categories_by_slot": {
                    "skeleton_id": [f"{route_id}.single"],
                    "gene_surface_id": ["surface-v1"],
                    "primary_field_id": ["field-a", "field-b"],
                }
            }
        }
        for route_id in ROUTES
    }
    transcripts = {route_id: [] for route_id in ROUTES}

    imported = _restore_or_import_adapters(
        output_root=tmp_path,
        lanes_by_route=lanes,
        seed_base=2026072602,
        transcripts=transcripts,
        checkpoint_count=0,
        prior_manifest=None,
    )
    restored = _restore_or_import_adapters(
        output_root=tmp_path,
        lanes_by_route=lanes,
        seed_base=2026072602,
        transcripts=transcripts,
        checkpoint_count=0,
        prior_manifest=None,
    )

    assert set(imported) == set(ROUTES)
    assert set(restored) == set(ROUTES)
    assert all(
        adapter.n_ei_candidates == N_EI_CANDIDATES
        for adapter in restored.values()
    )
    assert all(
        adapter.multivariate == TPE_MULTIVARIATE
        and adapter.group == TPE_GROUP
        for adapter in restored.values()
    )
    assert (
        tmp_path
        / "optimizer_recovery"
        / (
            "GENESIS_"
            f"{TPE_SAMPLER_MODE.lower()}_post_tell_receipt.json"
        )
    ).is_file()


def test_checkpoint_ask_allocation_is_registry_route_bounded() -> None:
    allocation = _allocate_checkpoint_asks(
        evaluated_by_route=Counter(),
        asked_by_route=Counter(),
    )

    assert set(allocation) == set(ROUTES)
    assert sum(allocation.values()) == ASKS_PER_CHECKPOINT
    assert all(value >= 8 for value in allocation.values())
    assert allocation["SLOW_TEMPORAL_CHANGE"] == max(
        allocation.values()
    )


def test_completed_route_receives_no_further_asks() -> None:
    completed = Counter(
        {
            "DISCLOSURE_EVENT": ROUTE_EVALUATED_TARGETS[
                "DISCLOSURE_EVENT"
            ]
        }
    )
    allocation = _allocate_checkpoint_asks(
        evaluated_by_route=completed,
        asked_by_route=Counter(),
    )

    assert "DISCLOSURE_EVENT" not in allocation
    assert sum(allocation.values()) == ASKS_PER_CHECKPOINT


def test_observed_low_yield_increases_route_ask_share() -> None:
    evaluated = Counter(
        {
            route_id: 100 for route_id in ROUTES
        }
    )
    asked = Counter(
        {
            route_id: 200 for route_id in ROUTES
        }
    )
    asked["FIRSTN_PATH"] = 500

    allocation = _allocate_checkpoint_asks(
        evaluated_by_route=evaluated,
        asked_by_route=asked,
    )

    assert allocation["FIRSTN_PATH"] > 8


class _FrozenLaneGenerator:
    def __init__(self, rule: str = "RULE_A") -> None:
        self.rule = rule

    def categorical_gene_lanes(self, route_id: str) -> dict[str, object]:
        return {
            "route_id": route_id,
            "lanes": {
                "lane_a": {
                    "root_rule": [self.rule],
                    "ordered_categories_by_slot": {
                        "z_slot": ["Z"],
                        "a_slot": ["A"],
                    },
                }
            },
        }


def test_frozen_gene_lanes_survive_runtime_only_contract_change(
    tmp_path: Path,
) -> None:
    generator = _FrozenLaneGenerator()
    initial, manifest_path = _freeze_gene_lanes(
        output_root=tmp_path,
        generator=generator,
        input_hashes={
            "contract": "contract-before-runtime-fix",
            "registry": "registry-frozen",
            "schema": "schema-frozen",
        },
    )

    resumed, resumed_manifest = _freeze_gene_lanes(
        output_root=tmp_path,
        generator=generator,
        input_hashes={
            "contract": "contract-after-runtime-fix",
            "registry": "registry-frozen",
            "schema": "schema-frozen",
        },
    )

    assert resumed == initial
    assert resumed_manifest == manifest_path
    assert list(
        resumed[ROUTES[0]]["lane_a"]["ordered_categories_by_slot"]
    ) == ["z_slot", "a_slot"]


def test_frozen_gene_lanes_reject_projected_formula_space_drift(
    tmp_path: Path,
) -> None:
    generator = _FrozenLaneGenerator()
    _freeze_gene_lanes(
        output_root=tmp_path,
        generator=generator,
        input_hashes={
            "contract": "contract-a",
            "registry": "registry-a",
            "schema": "schema-a",
        },
    )

    with pytest.raises(
        RuntimeError, match="LARGE_TPE_GENE_LANE_INPUT_DRIFT"
    ):
        _freeze_gene_lanes(
            output_root=tmp_path,
            generator=_FrozenLaneGenerator("RULE_B"),
            input_hashes={
                "contract": "contract-b",
                "registry": "registry-a",
                "schema": "schema-a",
            },
        )
