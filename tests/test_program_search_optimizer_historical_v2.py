from __future__ import annotations

import json
from itertools import product

from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
    EVOLUTION_OPERATIONS,
    HIERARCHICAL_CEM_PROGRAM_V2,
    CatalogTypedEvolutionProgramV2,
    HierarchicalProgramCEMV2,
    program_factor_plan_v2,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    ProgramOptimizerObservationV1,
    program_availability_entries_v1,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import ProgramUpliftCredit


def _rows() -> list[dict[str, object]]:
    rows = []
    for base_route, base_skeleton, temporal_skeleton, combine, base_field in product(
        ("MINUTE_STATIC", "SLOW_CROSS_SECTIONAL_LEVEL"),
        ("base.0", "base.1", "base.2"),
        ("temporal.0", "temporal.1", "temporal.2"),
        ("ADD", "MAX"),
        ("field.0", "field.1"),
    ):
        rows.append(
            {
                "genes": {
                    "skeleton_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "gene_surface_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "program_template_id": "BASE_TEMPORAL",
                    "active_component_roles": "base+temporal",
                    "composition_topology": "base>temporal",
                    "combination_temporal": combine,
                    "combination_market": "FILTER",
                    "combination_event_episode": "SOURCE_ROUTE_EPISODE",
                    "combination_event_application": "FILTER",
                    "joint_clock_class": "derived-clock",
                    "lag_class": f"derived-lag::{base_skeleton}::{temporal_skeleton}",
                    "structural_complexity_class": f"derived-complex::{base_route}",
                    "raw_field_count": "2",
                    "rolling_node_count": "1",
                    "interaction_topology": "derived-interaction",
                    "base__route_id": base_route,
                    "base__skeleton_id": base_skeleton,
                    "base__primary_field_id": base_field,
                    "temporal__route_id": "SLOW_TEMPORAL_CHANGE",
                    "temporal__skeleton_id": temporal_skeleton,
                    "temporal__primary_field_id": "field.temporal",
                }
            }
        )
    return rows


def _observation(
    ask: dict[str, object], *, return_uplift: float, reward_uplift: float
) -> ProgramOptimizerObservationV1:
    identity = str(ask["exact_identity"])
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=f"record-{identity}",
        pair_id=f"pair-{identity}",
        program_id=f"program-{identity}",
        control_program_id=f"control-{identity}",
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
            "matched_cumulative_net_return_increment": return_uplift,
            "matched_net_reward_increment": reward_uplift,
        },
    )
    return ProgramOptimizerObservationV1(
        proposal_id=str(ask["proposal_id"]),
        exact_identity=identity,
        admission=admission,
        uplift=uplift,
    )


def _json_roundtrip(value):
    return json.loads(json.dumps(value, sort_keys=True))


def test_program_factor_plan_uses_active_controls_not_derived_summaries() -> None:
    entries = program_availability_entries_v1(_rows())
    plan = program_factor_plan_v2(entries, "BASE_TEMPORAL")
    assert plan[:2] == ("base__route_id", "base__skeleton_id")
    assert "base__primary_field_id" in plan
    assert "temporal__skeleton_id" in plan
    assert "combination_temporal" in plan
    assert "combination_market" not in plan
    assert "lag_class" not in plan
    assert "structural_complexity_class" not in plan


def test_hierarchical_cem_legal_exact_only_complete_tell_update_and_entropy_floor() -> None:
    entries = program_availability_entries_v1(_rows())
    adapter = HierarchicalProgramCEMV2(
        entries=entries,
        seen_exact_identities=(),
        seed=101,
        minimum_observation_count=1,
        elite_fraction=0.5,
    )
    asks = adapter.ask(
        checkpoint_id="cem_001",
        count=8,
        required_program_template_id="BASE_TEMPORAL",
    )
    assert adapter.update_count == 0
    assert len(asks) == len({row["exact_identity"] for row in asks}) == 8
    legal = {entry.exact_identity for entry in entries}
    assert all(row["exact_identity"] in legal for row in asks)
    adapter.tell(
        [
            _observation(
                ask,
                return_uplift=1.0 - index * 0.05,
                reward_uplift=0.5 if index % 2 == 0 else -0.2,
            )
            for index, ask in enumerate(asks)
        ]
    )
    assert adapter.update_count == 1
    probabilities = adapter._regularized(
        ("a", "b", "c", "d"), {"a": 100.0, "b": 0.0, "c": 0.0, "d": 0.0}
    )
    assert min(probabilities.values()) > 0.0
    entropy = adapter._entropy(list(probabilities.values()))
    assert entropy + 1e-12 >= adapter.entropy_floor_ratio * __import__("math").log(4)
    assert adapter.optimizer_metadata()["optimizer_arm"] == HIERARCHICAL_CEM_PROGRAM_V2


def test_cem_reward_positive_feasibility_and_snapshot_restore() -> None:
    entries = program_availability_entries_v1(_rows())
    config = dict(
        entries=entries,
        seen_exact_identities=(),
        seed=103,
        minimum_observation_count=1,
        elite_fraction=0.5,
    )
    adapter = HierarchicalProgramCEMV2(**config)
    asks = adapter.ask(
        checkpoint_id="cem_reward",
        count=4,
        required_program_template_id="BASE_TEMPORAL",
    )
    receipt = adapter.tell(
        [
            _observation(asks[0], return_uplift=0.9, reward_uplift=-1.0),
            _observation(asks[1], return_uplift=0.8, reward_uplift=0.2),
            _observation(asks[2], return_uplift=0.7, reward_uplift=0.3),
            _observation(asks[3], return_uplift=0.6, reward_uplift=-0.1),
        ]
    )
    assert receipt["productive_feasible_count"] == 2
    persisted = _json_roundtrip(adapter.snapshot())
    restored = HierarchicalProgramCEMV2.restore(snapshot=persisted, **config)
    assert _json_roundtrip(restored.snapshot()) == persisted


def test_catalog_typed_evolution_uses_legal_children_and_operation_receipts() -> None:
    entries = program_availability_entries_v1(_rows())
    adapter = CatalogTypedEvolutionProgramV2(
        entries=entries,
        seen_exact_identities=(),
        seed=107,
        warmup=4,
        tournament_size=2,
    )
    first = adapter.ask(
        checkpoint_id="evo_001",
        count=4,
        required_program_template_id="BASE_TEMPORAL",
    )
    assert all(row["acquisition"]["operation"] == "UNIFORM_WARMUP" for row in first)
    adapter.tell(
        [_observation(row, return_uplift=0.5, reward_uplift=0.5) for row in first]
    )
    second = adapter.ask(
        checkpoint_id="evo_002",
        count=8,
        required_program_template_id="BASE_TEMPORAL",
    )
    legal = {entry.exact_identity for entry in entries}
    assert len(second) == len({row["exact_identity"] for row in second})
    assert all(row["exact_identity"] in legal for row in second)
    operations = {str(row["acquisition"]["operation"]) for row in second}
    assert operations <= set(EVOLUTION_OPERATIONS)
    assert operations
    assert adapter.optimizer_metadata()["optimizer_arm"] == CATALOG_TYPED_EVOLUTION_PROGRAM_V2


def test_catalog_typed_evolution_snapshot_restore_exact() -> None:
    entries = program_availability_entries_v1(_rows())
    config = dict(
        entries=entries,
        seen_exact_identities=(),
        seed=109,
        warmup=4,
        tournament_size=2,
    )
    adapter = CatalogTypedEvolutionProgramV2(**config)
    asks = adapter.ask(
        checkpoint_id="evo_restore",
        count=4,
        required_program_template_id="BASE_TEMPORAL",
    )
    adapter.tell(
        [
            _observation(
                row,
                return_uplift=0.4 - index * 0.05,
                reward_uplift=0.2 if index < 3 else -0.1,
            )
            for index, row in enumerate(asks)
        ]
    )
    persisted = _json_roundtrip(adapter.snapshot())
    restored = CatalogTypedEvolutionProgramV2.restore(snapshot=persisted, **config)
    assert _json_roundtrip(restored.snapshot()) == persisted
