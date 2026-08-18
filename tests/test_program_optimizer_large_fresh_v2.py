from __future__ import annotations

import argparse

import pytest

import app

from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import ENHANCED_TEMPLATES
from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v2 import (
    PRIMARY_EXECUTOR_WORKERS,
    RESOURCE_CANARY_FIELD_COLUMNS,
    RESOURCE_CANARY_PROBE_SECONDS,
    RESOURCE_FALLBACK_EXECUTOR_WORKERS,
    ROUTE_ID,
)
from our_system_phase2.services.program_optimizer_large_fresh_v2 import (
    ARMS,
    LargeFreshProgramBanditV2,
)
from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    ProgramOptimizerObservationV1,
    UNIFORM_CONTROL,
    program_availability_entries_v1,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import ProgramUpliftCredit
from scripts import run_cn_program_optimizer_large_fresh_v1 as base_runner
from scripts.run_cn_program_optimizer_large_fresh_v2 import (
    EVOLUTION_CONFIG,
    SEEDS,
    _BASE_RUNNER_MUTABLE_FIELDS,
    _checkpoint_arm_v2,
    run as run_v2,
)


def _entries(count: int = 64):
    rows = []
    for index in range(count):
        rows.append(
            {
                "genes": {
                    "skeleton_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "gene_surface_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "program_template_id": "BASE_TEMPORAL",
                    "active_component_roles": "base+temporal",
                    "composition_topology": "base>temporal",
                    "combination_temporal": ("ADD", "MAX")[index % 2],
                    "combination_market": "FILTER",
                    "combination_event_episode": "SOURCE_ROUTE_EPISODE",
                    "combination_event_application": "FILTER",
                    "joint_clock_class": "clock",
                    "lag_class": f"lag-{index % 4}",
                    "structural_complexity_class": f"complex-{index % 5}",
                    "raw_field_count": "2",
                    "rolling_node_count": str(index % 2),
                    "interaction_topology": f"interaction-{index % 3}",
                    "base__route_id": ("MINUTE_STATIC", "SLOW_CROSS_SECTIONAL_LEVEL")[index % 2],
                    "base__skeleton_id": f"base.{index % 8}",
                    "base__primary_field_id": f"base_field.{index % 16}",
                    "temporal__route_id": "SLOW_TEMPORAL_CHANGE",
                    "temporal__skeleton_id": f"temporal.{(index // 2) % 8}",
                    "temporal__primary_field_id": f"temporal_field.{(index * 3) % 17}",
                }
            }
        )
    return program_availability_entries_v1(rows)


def _observation(ask: dict, value: float) -> ProgramOptimizerObservationV1:
    exact = str(ask["exact_identity"])
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=f"record-{exact}",
        pair_id=f"pair-{exact}",
        program_id=f"program-{exact}",
        control_program_id=f"control-{exact}",
        admitted=True,
        failure_reasons=(),
        metrics={"synthetic": True},
    )
    uplift = ProgramUpliftCredit(
        record_payload_sha256=admission.record_payload_sha256,
        pair_id=admission.pair_id,
        program_id=admission.program_id,
        control_program_id=admission.control_program_id,
        program_credit={
            "matched_cumulative_net_return_increment": value,
            "matched_net_reward_increment": max(value, 0.01),
        },
    )
    return ProgramOptimizerObservationV1(
        proposal_id=str(ask["proposal_id"]),
        exact_identity=exact,
        admission=admission,
        uplift=uplift,
    )


def test_large_fresh_v2_route_is_high_cost_and_authorization_bound() -> None:
    assert app.ROUTES[ROUTE_ID] == "our_system_phase2.runtime.cn_program_optimizer_large_fresh_v2"
    assert app.HIGH_COST_ROUTE_ACTIONS[ROUTE_ID] == {ACTION_LAUNCH, ACTION_RETRY}
    assert ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_large_fresh_v2_uses_realistic_24_worker_resource_canary() -> None:
    assert PRIMARY_EXECUTOR_WORKERS == 24
    assert RESOURCE_FALLBACK_EXECUTOR_WORKERS == 16
    assert len(RESOURCE_CANARY_FIELD_COLUMNS) == 47
    assert RESOURCE_CANARY_PROBE_SECONDS == 30.0
    assert base_runner.PRIMARY_EXECUTOR_WORKERS == 24
    assert base_runner.RESOURCE_FALLBACK_EXECUTOR_WORKERS == 16
    assert base_runner.RESOURCE_CANARY_FIELD_COLUMNS is None
    assert base_runner.RESOURCE_CANARY_REQUIRE_MINIMUM_FREE_PHYSICAL is True
    assert base_runner.RESOURCE_CANARY_PROBE_SECONDS == 1.0


def test_large_fresh_v2_is_six_evolution_one_rotating_uniform() -> None:
    for macro_index in range(10):
        arms = [
            _checkpoint_arm_v2(macro_index, template_index)
            for template_index in range(len(ENHANCED_TEMPLATES))
        ]
        assert arms.count(UNIFORM_CONTROL) == 1
        assert arms.count(CATALOG_TYPED_EVOLUTION_PROGRAM_V2) == 6
        assert arms[macro_index % len(ENHANCED_TEMPLATES)] == UNIFORM_CONTROL


def test_large_fresh_v2_bandit_has_only_uniform_and_evolution() -> None:
    entries = _entries()
    state = LargeFreshProgramBanditV2(
        campaign_id="TEST_V2",
        entries_by_arm={arm: entries for arm in ARMS},
        seeds=SEEDS,
        evolution_config=EVOLUTION_CONFIG,
    )
    assert set(state.adapters) == {UNIFORM_CONTROL, CATALOG_TYPED_EVOLUTION_PROGRAM_V2}
    assert "HYBRID_TPE_PROGRAM" not in state.adapters
    assert "HIERARCHICAL_CEM_V2" not in state.adapters


def test_large_fresh_v2_preview_commit_tell_and_restore_are_exact() -> None:
    entries = _entries()
    config = dict(
        campaign_id="TEST_V2",
        entries_by_arm={arm: entries for arm in ARMS},
        seeds=SEEDS,
        evolution_config=EVOLUTION_CONFIG,
    )
    state = LargeFreshProgramBanditV2(**config)
    asks = state.ask(
        arm=CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
        checkpoint_id="checkpoint_001",
        count=8,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=[entry.exact_identity for entry in entries],
    )
    state.commit_ask(
        arm=CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
        checkpoint_id="checkpoint_001",
        count=8,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=[entry.exact_identity for entry in entries],
        expected_asks=asks,
    )
    state.tell(
        arm=CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
        observations=[_observation(row, 0.1 + index * 0.01) for index, row in enumerate(asks)],
    )
    snapshot = state.snapshot()
    restored = LargeFreshProgramBanditV2.restore(
        snapshot,
        entries_by_arm=config["entries_by_arm"],
        expected_campaign_id="TEST_V2",
    )
    assert restored.snapshot() == snapshot
    next_kwargs = dict(
        arm=CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
        checkpoint_id="checkpoint_002",
        count=8,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=[entry.exact_identity for entry in entries],
    )
    assert restored.ask(**next_kwargs) == state.ask(**next_kwargs)


def test_large_fresh_v2_restores_v1_runner_globals_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = {
        name: getattr(base_runner, name) for name in _BASE_RUNNER_MUTABLE_FIELDS
    }

    def fail_run(*args, **kwargs):
        assert base_runner.CAMPAIGN_ID == "CN_PROGRAM_OPTIMIZER_LARGE_FRESH_DEVELOPMENT_V2"
        assert base_runner.FORMAL_OPTIMIZER_ARM == CATALOG_TYPED_EVOLUTION_PROGRAM_V2
        assert base_runner.PRIMARY_EXECUTOR_WORKERS == 24
        assert base_runner.RESOURCE_FALLBACK_EXECUTOR_WORKERS == 16
        assert base_runner.RESOURCE_CANARY_FIELD_COLUMNS == RESOURCE_CANARY_FIELD_COLUMNS
        assert base_runner.RESOURCE_CANARY_REQUIRE_MINIMUM_FREE_PHYSICAL is False
        assert base_runner.RESOURCE_CANARY_PROBE_SECONDS == 30.0
        raise RuntimeError("synthetic-v2-failure")

    monkeypatch.setattr(base_runner, "run", fail_run)
    with pytest.raises(RuntimeError, match="synthetic-v2-failure"):
        run_v2(argparse.Namespace(), admission={}, authorization={})
    assert {
        name: getattr(base_runner, name) for name in _BASE_RUNNER_MUTABLE_FIELDS
    } == original
