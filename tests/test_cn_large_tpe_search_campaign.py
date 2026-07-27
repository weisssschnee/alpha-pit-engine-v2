from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from our_system_phase2.runtime.cn_large_tpe_search_campaign import (
    ASKS_PER_CHECKPOINT,
    MINIMUM_ACTUAL_EVALUATED_PAIRS,
    N_EI_CANDIDATES,
    OPTUNA_ROUTE_WORKERS,
    PAIR_BATCH_SIZES,
    ROUTE_EVALUATED_TARGETS,
    ROUTES,
    _allocate_checkpoint_asks,
    _ask_route_populations,
    _freeze_gene_lanes,
    _restore_or_import_adapters,
)


def test_large_contract_is_five_digit_actual_evaluated_not_scheduled() -> None:
    assert MINIMUM_ACTUAL_EVALUATED_PAIRS == 20_000
    assert sum(ROUTE_EVALUATED_TARGETS.values()) == 20_000
    assert set(ROUTE_EVALUATED_TARGETS) == set(ROUTES)
    assert "MINUTE_STATIC" not in ROUTES
    assert "INTRADAY_STATE_TRANSITION" not in ROUTES
    assert PAIR_BATCH_SIZES == {
        "active_bar": 8,
        "stock_session": 8,
    }
    assert OPTUNA_ROUTE_WORKERS == len(ROUTES)
    assert N_EI_CANDIDATES == 24


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
    assert (
        tmp_path
        / "optimizer_recovery"
        / "GENESIS_post_tell_receipt.json"
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
