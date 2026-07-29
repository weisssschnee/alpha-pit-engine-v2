from __future__ import annotations

from collections import Counter
from collections import OrderedDict
import json
from pathlib import Path

import numpy as np
import pytest

from scripts.run_cn_phase3cm_streaming_qualification import _finalize_pairs
from our_system_phase2.runtime.cn_large_tpe_search_campaign import (
    ASKS_PER_CHECKPOINT,
    CAMPAIGN_PROFILE,
    HYBRID_BOUNDED_LARGE_TRANCHE_COVERAGE_FLOORS,
    HYBRID_BOUNDED_LARGE_TRANCHE_FLEXIBLE_ALLOCATION,
    HYBRID_BOUNDED_LARGE_TRANCHE_MAXIMUM_CHECKPOINTS,
    HYBRID_BOUNDED_LARGE_TRANCHE_MAXIMUM_RAW_ASKS,
    HYBRID_BOUNDED_LARGE_TRANCHE_PROFILE,
    HYBRID_BOUNDED_LARGE_TRANCHE_ROUTE_CAPS,
    HYBRID_BOUNDED_LARGE_TRANCHE_ROUTE_MIX,
    HYBRID_ONLY_TRANCHE_COVERAGE_FLOORS,
    HYBRID_ONLY_TRANCHE_FLEXIBLE_ALLOCATION,
    HYBRID_ONLY_TRANCHE_MAXIMUM_CHECKPOINTS,
    HYBRID_ONLY_TRANCHE_MAXIMUM_RAW_ASKS,
    HYBRID_ONLY_TRANCHE_PROFILE,
    HYBRID_ONLY_TRANCHE_ROUTE_CAPS,
    HYBRID_ONLY_TRANCHE_ROUTE_MIX,
    MAXIMUM_RAW_ASKS,
    MINIMUM_ACTUAL_EVALUATED_PAIRS,
    N_EI_CANDIDATES,
    OPTUNA_ROUTE_WORKERS,
    PAIR_BATCH_SIZES,
    PRODUCTIVITY_MAXIMUM_CHECKPOINTS,
    PRODUCTIVITY_MAXIMUM_RAW_ASKS,
    PRODUCTIVITY_POLICY_ARMS,
    PRODUCTIVITY_ROUTE_MIX,
    ROUTE_EVALUATED_TARGETS,
    ROUTES,
    TPE_GROUP,
    TPE_MULTIVARIATE,
    TPE_SAMPLER_MODE,
    _allocate_checkpoint_asks,
    _availability_semantic_input_hashes,
    _ask_availability_aware_populations,
    _ask_route_populations,
    _conservative_search_score,
    _campaign_runtime_spec,
    _freeze_availability_index,
    _freeze_gene_lanes,
    _medium_policy_decision,
    _policy_arm_assignments,
    _productive_family_diagnostics,
    _route_budget_feasibility,
    _restore_or_import_adapters,
    _sha256,
    _stable_hash,
    _select_validation_finalists,
    _validation_eligible,
)
from our_system_phase2.runtime.cn_large_tpe_search_campaign import (
    HYBRID_POLICY_ARM,
    UNIFORM_POLICY_ARM,
)
from our_system_phase2.services.optuna_tpe_search_adapter import (
    RouteConditionalTPESearchAdapter,
)
from our_system_phase2.services.route_local_availability import (
    AvailabilityEntry,
    RouteLocalAvailabilityController,
    structural_bucket_key,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_successor_contract_is_formal_exact_bounded_not_financially_authorized() -> None:
    assert CAMPAIGN_PROFILE == "cn_large_optuna_tpe_availability_v3"
    assert MINIMUM_ACTUAL_EVALUATED_PAIRS == 16_880
    assert sum(ROUTE_EVALUATED_TARGETS.values()) == 16_880
    assert set(ROUTE_EVALUATED_TARGETS) == set(ROUTES)
    assert "MINUTE_STATIC" not in ROUTES
    assert "INTRADAY_STATE_TRANSITION" not in ROUTES
    assert PAIR_BATCH_SIZES == {
        "active_bar": 12,
        "stock_session": 12,
    }
    assert ASKS_PER_CHECKPOINT == 384
    assert MAXIMUM_RAW_ASKS == 36_864
    assert OPTUNA_ROUTE_WORKERS == len(ROUTES)
    assert N_EI_CANDIDATES == 24
    assert TPE_SAMPLER_MODE == (
        "OFFICIAL_DEFAULT_UNIVARIATE_CONSTANT_LIAR"
    )
    assert TPE_MULTIVARIATE is False
    assert TPE_GROUP is False
    target_contract = json.loads(
        (
            REPO_ROOT
            / "runtime"
            / "run_plans"
            / "cn_large_tpe_successor_route_targets_v3.json"
        ).read_text(encoding="utf-8")
    )
    assert target_contract["execution_authorized"] is False
    assert target_contract["financial_campaign_authorized"] is False
    assert target_contract["route_actual_evaluated_targets"] == (
        ROUTE_EVALUATED_TARGETS
    )


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


def test_live_runner_replaces_seen_exact_without_pruning_or_formal_budget_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    route = "TEST_ROUTE"
    lanes = OrderedDict(
        {
            "test.single": {
                "ordered_categories_by_slot": OrderedDict(
                    [
                        ("skeleton_id", ["test.single"]),
                        ("gene_surface_id", ["surface-v1"]),
                        ("primary_field_id", ["a", "b"]),
                    ]
                )
            }
        }
    )
    adapter = RouteConditionalTPESearchAdapter(
        route_id=route,
        lane_spaces=lanes,
        seed=2,
        n_startup_trials=2,
        n_ei_candidates=8,
        multivariate=False,
        group=False,
    )

    def genes(field: str) -> dict[str, str]:
        return {
            "skeleton_id": "test.single",
            "gene_surface_id": "surface-v1",
            "primary_field_id": field,
        }

    entries = [
        AvailabilityEntry(
            route_id=route,
            bucket_key=structural_bucket_key(route, genes(field)),
            exact_identity=f"exact-{field}",
            control_exact_identity=f"control-{field}",
            genes=genes(field),
        )
        for field in ("a", "b")
    ]
    controller = RouteLocalAvailabilityController(
        entries=entries,
        seen_exact_identities={"exact-a"},
        emitter_seed=17,
        input_hashes={"registry": "r", "grammar": "g", "compiler": "c"},
    )

    def materialize(*, asked, generator, schema_by_backend):
        del generator, schema_by_backend
        return [
            {
                **row,
                "construction_status": "LEGAL",
                "exact_identity": (
                    "exact-" + row["genes"]["primary_field_id"]
                ),
            }
            for row in asked
        ]

    monkeypatch.setattr(
        "our_system_phase2.runtime.cn_large_tpe_search_campaign."
        "_materialize_population",
        materialize,
    )
    all_asked, formal, internal, audit = (
        _ask_availability_aware_populations(
            schedule=[{"route_id": route, "asked_pairs": 1}],
            adapters={route: adapter},
            controller=controller,
            generator=object(),
            schema_by_backend={},
            checkpoint_id="checkpoint_001",
        )
    )

    assert len(all_asked) == 2
    assert len(formal) == 1
    assert formal[0]["genes"]["primary_field_id"] == "b"
    assert formal[0]["availability_emission_mode"] == (
        "TPE_BUCKET_REPLACEMENT"
    )
    assert audit["formal_fresh_exact_asks"] == 1
    assert audit["optimizer_draw_attempts"] == 2
    assert list(internal.values())[0]["outcome_class"] == (
        "AVAILABILITY_REPLACED"
    )
    receipt = adapter.tell_population(
        [
            *internal.values(),
            {
                "proposal_id": formal[0]["proposal_id"],
                "outcome_class": "BEHAVIOR_BLOCKED",
                "optimizer_reward": None,
                "outcome_reason": "BEHAVIOR_DUPLICATE",
            },
        ]
    )
    assert receipt["pruned_count"] == 0
    assert receipt["failed_count"] == 2
    assert [trial.state.name for trial in adapter._study.trials] == [
        "FAIL",
        "FAIL",
    ]


def test_productivity_arm_assignment_is_exact_balanced_and_seedless_replayable() -> None:
    first = _policy_arm_assignments(
        checkpoint_id="checkpoint_003",
        route_id="SLOW_TEMPORAL_CHANGE",
        count=160,
    )
    second = _policy_arm_assignments(
        checkpoint_id="checkpoint_003",
        route_id="SLOW_TEMPORAL_CHANGE",
        count=160,
    )

    assert first == second
    assert Counter(first) == {
        HYBRID_POLICY_ARM: 80,
        UNIFORM_POLICY_ARM: 80,
    }
    assert sum(PRODUCTIVITY_ROUTE_MIX.values()) == 384
    assert PRODUCTIVITY_MAXIMUM_CHECKPOINTS == 8
    assert PRODUCTIVITY_MAXIMUM_RAW_ASKS == 3_072
    assert PRODUCTIVITY_POLICY_ARMS == (
        HYBRID_POLICY_ARM,
        UNIFORM_POLICY_ARM,
    )


def test_hybrid_only_tranche_is_exact_bounded_and_not_an_unlimited_search() -> None:
    spec = _campaign_runtime_spec(HYBRID_ONLY_TRANCHE_PROFILE)
    final_allocation = {
        route_id: count * HYBRID_ONLY_TRANCHE_MAXIMUM_CHECKPOINTS
        for route_id, count in HYBRID_ONLY_TRANCHE_ROUTE_MIX.items()
    }
    authorization = json.loads(
        (
            REPO_ROOT
            / "runtime"
            / "run_plans"
            / "cn_hybrid_only_tranche_v1_authorization.json"
        ).read_text(encoding="utf-8")
    )

    assert sum(HYBRID_ONLY_TRANCHE_ROUTE_MIX.values()) == 384
    assert HYBRID_ONLY_TRANCHE_MAXIMUM_CHECKPOINTS == 8
    assert HYBRID_ONLY_TRANCHE_MAXIMUM_RAW_ASKS == 3_072
    assert sum(final_allocation.values()) == 3_072
    assert sum(HYBRID_ONLY_TRANCHE_COVERAGE_FLOORS.values()) == 1_728
    assert sum(HYBRID_ONLY_TRANCHE_FLEXIBLE_ALLOCATION.values()) == 1_344
    assert all(
        HYBRID_ONLY_TRANCHE_COVERAGE_FLOORS[route_id]
        <= final_allocation[route_id]
        <= HYBRID_ONLY_TRANCHE_ROUTE_CAPS[route_id]
        for route_id in ROUTES
    )
    assert spec["completion_mode"] == "FIXED_FORMAL_ASK_TRANCHE"
    assert spec["validation"] == "FORBIDDEN_DURING_AND_AFTER_TRANCHE"
    assert spec["fixed_route_mix"] == HYBRID_ONLY_TRANCHE_ROUTE_MIX
    assert authorization["accepted_development_search_policy"] == (
        HYBRID_POLICY_ARM
    )
    assert authorization["policy_reopened"] is False
    assert authorization["uniform_arm"] == "FORBIDDEN"
    assert authorization["final_route_formal_ask_allocation"] == (
        final_allocation
    )
    assert authorization["unlimited_or_20k_search_authorized"] is False


def test_hybrid_bounded_large_tranche_profile_matches_frozen_contract() -> None:
    spec = _campaign_runtime_spec(HYBRID_BOUNDED_LARGE_TRANCHE_PROFILE)
    final_allocation = {
        route_id: count
        * HYBRID_BOUNDED_LARGE_TRANCHE_MAXIMUM_CHECKPOINTS
        for route_id, count in (
            HYBRID_BOUNDED_LARGE_TRANCHE_ROUTE_MIX.items()
        )
    }
    qualification = json.loads(
        (
            REPO_ROOT
            / "runtime"
            / "run_plans"
            / "cn_hybrid_bounded_large_tranche_v1_qualification.json"
        ).read_text(encoding="utf-8")
    )

    assert sum(HYBRID_BOUNDED_LARGE_TRANCHE_ROUTE_MIX.values()) == 768
    assert HYBRID_BOUNDED_LARGE_TRANCHE_MAXIMUM_CHECKPOINTS == 8
    assert HYBRID_BOUNDED_LARGE_TRANCHE_MAXIMUM_RAW_ASKS == 6_144
    assert sum(final_allocation.values()) == 6_144
    assert final_allocation == {
        "SLOW_TEMPORAL_CHANGE": 5_376,
        "FIRSTN_PATH": 64,
        "SLOW_CROSS_SECTIONAL_LEVEL": 192,
        "MARKET_REGIME_CONDITION": 128,
        "DISCLOSURE_EVENT": 384,
    }
    assert all(
        HYBRID_BOUNDED_LARGE_TRANCHE_COVERAGE_FLOORS[route_id]
        <= final_allocation[route_id]
        <= HYBRID_BOUNDED_LARGE_TRANCHE_ROUTE_CAPS[route_id]
        for route_id in ROUTES
    )
    assert sum(
        HYBRID_BOUNDED_LARGE_TRANCHE_FLEXIBLE_ALLOCATION.values()
    ) == 992
    assert spec["fixed_route_mix"] == (
        HYBRID_BOUNDED_LARGE_TRANCHE_ROUTE_MIX
    )
    assert spec["minimum_required_fresh_exact_by_route"] == {
        "SLOW_TEMPORAL_CHANGE": 6_682,
        "FIRSTN_PATH": 154,
        "SLOW_CROSS_SECTIONAL_LEVEL": 231,
        "MARKET_REGIME_CONDITION": 308,
        "DISCLOSURE_EVENT": 615,
    }
    assert qualification["qualification_authorized"] is True
    assert qualification["execution_authorized"] is False
    assert qualification["financial_campaign_authorized"] is False
    assert qualification["fixed_route_formal_asks_per_checkpoint"] == (
        HYBRID_BOUNDED_LARGE_TRANCHE_ROUTE_MIX
    )
    assert qualification["final_route_formal_ask_allocation"] == (
        final_allocation
    )
    assert qualification["unlimited_or_20k_search_authorized"] is False


def test_bounded_large_execution_authority_is_single_tranche_only() -> None:
    authorization = json.loads(
        (
            REPO_ROOT
            / "runtime"
            / "run_plans"
            / "cn_hybrid_bounded_large_tranche_v1_authorization.json"
        ).read_text(encoding="utf-8")
    )

    assert authorization["qualification_authorized"] is True
    assert authorization["execution_authorized"] is True
    assert authorization["financial_campaign_authorized"] is True
    assert authorization["single_bounded_task_authorized"] is True
    assert authorization["authorized_task_count"] == 1
    assert authorization["maximum_raw_asks"] == 6_144
    assert authorization["automatic_validation"] == "FORBIDDEN"
    assert authorization["unlimited_or_20k_search_authorized"] is False
    assert authorization["next_search_tranche_pre_authorized"] is False
    assert authorization["preflight_financial_reads"] == 0
    assert authorization["preflight_validation_reads"] == 0
    assert authorization["preflight_holdout_reads"] == 0
    assert authorization["preflight_forward_2026_reads"] == 0


def test_productive_family_diagnostics_are_diagnostic_only() -> None:
    outcomes = [
        {
            "pair_id": pair_id,
            "pair_evaluation_status": "PAIR_EVALUATED",
            "primary_composite_reward": score,
            "matched_train_increment": score,
            "primary_standalone_train_reward_decision": (
                "TRAIN_REWARD_FOLLOWUP_READY"
            ),
        }
        for pair_id, score in (
            ("p1", 1.0),
            ("p2", 2.0),
            ("p3", 3.0),
            ("p4", 4.0),
            ("p5", -1.0),
        )
    ]
    full_behavior = [
        {
            "pair_id": pair_id,
            "portfolio_behavior_family_id": family_id,
        }
        for pair_id, family_id in (
            ("p1", "family-a"),
            ("p2", "family-a"),
            ("p3", "family-b"),
            ("p4", "family-c"),
            ("p5", "family-d"),
        )
    ]
    candidates = [
        {
            "pair_id": pair_id,
            "pair_member_role": "PRIMARY",
            "exact_identity": f"exact-{pair_id}",
        }
        for pair_id in ("p1", "p2", "p3", "p4", "p5")
    ]

    diagnostics = _productive_family_diagnostics(
        outcomes=outcomes,
        full_behavior=full_behavior,
        candidates=candidates,
        formal_asks=8,
        prior_family_ids=("family-a",),
    )

    assert diagnostics["reporting_role"] == (
        "DIAGNOSTIC_ONLY_NOT_RUNTIME_STOP_GATE"
    )
    assert diagnostics["productive_candidates"] == 4
    assert diagnostics["productive_yield"] == 0.5
    assert diagnostics["productive_exact_unique"] == 4
    assert diagnostics["productive_behavior_family_unique"] == 3
    assert diagnostics["productive_exact_per_behavior_family"] == pytest.approx(
        4 / 3
    )
    assert diagnostics["top_10_productive_family_concentration"] == 1.0
    assert diagnostics["new_productive_behavior_families"] == 2
    assert diagnostics["unresolved_productive_behavior_family_count"] == 0
    assert diagnostics["unresolved_productive_exact_count"] == 0


def test_availability_semantic_hashes_ignore_only_indirect_runtime_paths() -> None:
    authority = {
        "status": "SEARCH_PRODUCTIVITY_MEDIUM_AUTHORIZED",
        "frozen": {"asks_per_checkpoint": 384},
        "authorization": {
            "path": "D:/workspace-a/authorization.json",
            "bytes": 11,
            "sha256": "a",
        },
        "historical_candidate_archive": {
            "path": "D:/runtime/candidates.parquet",
            "bytes": 12,
            "sha256": "b",
        },
        "historical_behavior_archive": {
            "path": "D:/runtime/behavior.parquet",
            "bytes": 13,
            "sha256": "c",
        },
        "historical_archive_manifest": {
            "path": "D:/runtime/manifest.json",
            "bytes": 14,
            "sha256": "d",
        },
    }
    registry = {
        "status": "BOUND",
        "registry_path": "D:/workspace-a/registry.json",
        "repo_sha": "old",
        "registry_file_sha256": "registry",
        "internal_registry_hash": "internal",
    }
    contract = {
        "campaign_profile": "medium",
        "maximum_raw_asks": 3072,
        "input_bindings": {
            "runtime": {"sha256": "dynamic-a"},
            "registry": {"path": "D:/workspace-a/registry.json"},
        },
    }
    first = _availability_semantic_input_hashes(
        authority=authority,
        registry_binding=registry,
        contract=contract,
    )
    authority["authorization"]["path"] = (
        "D:/workspace-b/authorization.json"
    )
    registry["registry_path"] = "D:/workspace-b/registry.json"
    registry["repo_sha"] = "new"
    contract["input_bindings"] = {
        "runtime": {"sha256": "dynamic-b"},
        "registry": {"path": "D:/workspace-b/registry.json"},
    }
    second = _availability_semantic_input_hashes(
        authority=authority,
        registry_binding=registry,
        contract=contract,
    )

    assert first == second
    contract["maximum_raw_asks"] = 3073
    changed = _availability_semantic_input_hashes(
        authority=authority,
        registry_binding=registry,
        contract=contract,
    )
    assert (
        changed["compiler_contract_semantic"]
        != first["compiler_contract_semantic"]
    )


def test_legacy_availability_index_resume_is_checkpoint_anchored_and_structural(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entries = [
        AvailabilityEntry(
            route_id="SLOW_TEMPORAL_CHANGE",
            bucket_key="bucket",
            exact_identity="exact",
            control_exact_identity="control",
            genes={
                "skeleton_id": "test.skeleton",
                "primary_field_id": "field",
            },
        )
    ]
    enumeration = {
        "schema_version": "test_enumeration_v1",
        "routes": {"SLOW_TEMPORAL_CHANGE": 1},
    }
    monkeypatch.setattr(
        "our_system_phase2.runtime.cn_large_tpe_search_campaign."
        "enumerate_authoritative_entries",
        lambda **_: (entries, enumeration),
    )
    legacy_inputs = {
        "registry": "legacy-registry-binding",
        "grammar_lanes": "stable-lanes",
        "compiler_contract": "legacy-runtime-bearing-contract",
        "historical_exact_archive": "stable-archive",
    }
    semantic_inputs = {
        "campaign_authority_semantic": "authority",
        "registry_semantic": "registry",
        "compiler_contract_semantic": "contract",
    }
    frozen_entries, index_path, effective_inputs = (
        _freeze_availability_index(
            output_root=tmp_path,
            generator=object(),
            lanes_by_route={},
            input_hashes=legacy_inputs,
            semantic_input_hashes=semantic_inputs,
        )
    )
    assert frozen_entries == entries
    assert effective_inputs == legacy_inputs

    checkpoint_root = (
        tmp_path / "checkpoints" / "checkpoint_001"
    )
    checkpoint_root.mkdir(parents=True)
    anchor = {
        "status": "BATCH_CLOSED_IMMUTABLE",
        "input_hashes": {
            "availability_index": _sha256(index_path),
            "frozen_contract": legacy_inputs["compiler_contract"],
            "gene_lane_manifest": legacy_inputs["grammar_lanes"],
            "prior_checkpoint_manifest": "GENESIS",
        },
    }
    anchor["manifest_payload_hash"] = _stable_hash(anchor)
    (checkpoint_root / "batch_manifest.json").write_text(
        json.dumps(anchor),
        encoding="utf-8",
    )
    current_indirect_inputs = {
        **legacy_inputs,
        "registry": "new-repo-path-bearing-binding",
        "compiler_contract": "new-runtime-bearing-contract",
    }
    restored, _, restored_inputs = _freeze_availability_index(
        output_root=tmp_path,
        generator=object(),
        lanes_by_route={},
        input_hashes=current_indirect_inputs,
        semantic_input_hashes=semantic_inputs,
    )

    assert restored == entries
    assert restored_inputs == legacy_inputs
    receipt = json.loads(
        (
            tmp_path
            / "route_local_availability_index_resume_receipt.json"
        ).read_text(encoding="utf-8")
    )
    assert receipt["status"] == (
        "LEGACY_INDIRECT_BINDING_RECOVERED_FAIL_CLOSED"
    )
    assert receipt["indirect_drift_keys"] == [
        "compiler_contract",
        "registry",
    ]
    assert receipt["financial_reads"] == 0

    with pytest.raises(
        RuntimeError,
        match="LARGE_TPE_AVAILABILITY_INDEX_DRIFT",
    ):
        _freeze_availability_index(
            output_root=tmp_path,
            generator=object(),
            lanes_by_route={},
            input_hashes={
                **current_indirect_inputs,
                "grammar_lanes": "drifted-lanes",
            },
            semantic_input_hashes=semantic_inputs,
        )


def test_productivity_ask_keeps_uniform_outside_optimizer_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    route = "TEST_ROUTE"
    lanes = OrderedDict(
        {
            "test.single": {
                "ordered_categories_by_slot": OrderedDict(
                    [
                        ("skeleton_id", ["test.single"]),
                        ("gene_surface_id", ["surface-v1"]),
                        ("primary_field_id", ["a", "b", "c", "d"]),
                    ]
                )
            }
        }
    )
    adapter = RouteConditionalTPESearchAdapter(
        route_id=route,
        lane_spaces=lanes,
        seed=3,
        n_startup_trials=2,
        n_ei_candidates=8,
        multivariate=False,
        group=False,
    )

    def genes(field: str) -> dict[str, str]:
        return {
            "skeleton_id": "test.single",
            "gene_surface_id": "surface-v1",
            "primary_field_id": field,
        }

    controller = RouteLocalAvailabilityController(
        entries=[
            AvailabilityEntry(
                route_id=route,
                bucket_key=structural_bucket_key(route, genes(field)),
                exact_identity=f"exact-{field}",
                control_exact_identity=f"control-{field}",
                genes=genes(field),
            )
            for field in ("a", "b", "c", "d")
        ],
        seen_exact_identities=set(),
        emitter_seed=19,
        input_hashes={"registry": "r", "grammar": "g", "compiler": "c"},
    )

    def materialize(*, asked, generator, schema_by_backend):
        del generator, schema_by_backend
        return [
            {
                **row,
                "construction_status": "LEGAL",
                "exact_identity": (
                    "exact-" + row["genes"]["primary_field_id"]
                ),
            }
            for row in asked
        ]

    monkeypatch.setattr(
        "our_system_phase2.runtime.cn_large_tpe_search_campaign."
        "_materialize_population",
        materialize,
    )
    all_asked, formal, internal, audit = (
        _ask_availability_aware_populations(
            schedule=[{"route_id": route, "asked_pairs": 2}],
            adapters={route: adapter},
            controller=controller,
            generator=object(),
            schema_by_backend={},
            checkpoint_id="checkpoint_001",
            productivity_experiment=True,
        )
    )

    assert Counter(row["search_policy_arm"] for row in formal) == {
        HYBRID_POLICY_ARM: 1,
        UNIFORM_POLICY_ARM: 1,
    }
    uniform = next(
        row for row in formal if row["search_policy_arm"] == UNIFORM_POLICY_ARM
    )
    assert uniform["trial_number"] is None
    assert uniform["optimizer_feedback_eligible"] is False
    assert audit["routes"][route]["availability_aware_uniform"] == 1
    optimizer_rows = [
        row
        for row in all_asked
        if bool(row.get("optimizer_feedback_eligible", True))
    ]
    observations = [
        internal.get(
            str(row["proposal_id"]),
            {
                "proposal_id": str(row["proposal_id"]),
                "outcome_class": "BEHAVIOR_BLOCKED",
                "optimizer_reward": None,
                "outcome_reason": "TEST",
            },
        )
        for row in optimizer_rows
    ]
    receipt = adapter.tell_population(observations)
    assert receipt["asked_count"] == len(optimizer_rows)
    assert uniform["proposal_id"] not in {
        str(row["proposal_id"]) for row in observations
    }


def test_productivity_decision_defaults_to_uniform_without_demonstrated_uplift() -> None:
    rows = []
    for checkpoint_index in range(PRODUCTIVITY_MAXIMUM_CHECKPOINTS):
        checkpoint = f"checkpoint_{checkpoint_index + 1:03d}"
        for route_id, route_count in PRODUCTIVITY_ROUTE_MIX.items():
            for arm in PRODUCTIVITY_POLICY_ARMS:
                for ordinal in range(route_count // 2):
                    rows.append(
                        {
                            "checkpoint": checkpoint,
                            "route_id": route_id,
                            "intention_to_treat_arm": arm,
                            "pair_evaluated": True,
                            "productive_candidate": ordinal % 4 == 0,
                            "positive_search_score": ordinal % 3 == 0,
                            "search_score": 0.2,
                            "portfolio_behavior_family_id": (
                                f"{route_id}-{arm}-{ordinal}"
                            ),
                            "availability_emission_mode": arm,
                        }
                    )
    summaries = [
        {"checkpoint_wall_seconds": 600.0}
        for _ in range(PRODUCTIVITY_MAXIMUM_CHECKPOINTS)
    ]

    decision = _medium_policy_decision(
        policy_rows=rows,
        checkpoint_summaries=summaries,
    )

    assert decision["status"] == "PRODUCTIVITY_MEDIUM_COMPLETE"
    assert decision["selected_search_policy"] == UNIFORM_POLICY_ARM
    assert decision["hybrid_acceptance_rule_passed"] is False
    assert decision["statistical_equivalence_claimed"] is False


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


def test_scheduler_preserves_observed_1_4_percent_without_20_percent_floor() -> None:
    evaluated = Counter({route_id: 100 for route_id in ROUTES})
    asked = Counter({route_id: 200 for route_id in ROUTES})
    evaluated["FIRSTN_PATH"] = 14
    asked["FIRSTN_PATH"] = 1_000

    allocation = _allocate_checkpoint_asks(
        evaluated_by_route=evaluated,
        asked_by_route=asked,
    )

    assert 14 / 1_000 == pytest.approx(0.014)
    assert allocation["FIRSTN_PATH"] == max(allocation.values())


def test_budget_feasibility_separates_absolute_failure_from_recent_risk() -> None:
    at_risk = _route_budget_feasibility(
        remaining_evaluated_target=100,
        remaining_formal_ask_budget=1_000,
        remaining_exact_count=1_000,
        recent_checkpoints=[
            {"formal_fresh_exact_asks": 100, "pair_evaluated": 5},
            {"formal_fresh_exact_asks": 100, "pair_evaluated": 6},
        ],
    )
    impossible = _route_budget_feasibility(
        remaining_evaluated_target=101,
        remaining_formal_ask_budget=100,
        remaining_exact_count=1_000,
        recent_checkpoints=[],
    )

    assert at_risk["recent_checkpoint_yields"] == [0.05, 0.06]
    assert at_risk["required_future_yield"] == pytest.approx(0.10)
    assert at_risk["status"] == (
        "AT_RISK_RECENT_YIELD_SHORTFALL_TWO_CHECKPOINTS"
    )
    assert impossible["status"] == "INFEASIBLE_ABSOLUTE_CEILING"


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
