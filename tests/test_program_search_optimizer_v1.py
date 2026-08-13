from __future__ import annotations

from dataclasses import replace

import pytest

from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    PROGRAM_ROUTE_ID,
    STRUCTURED_SURROGATE_PROGRAM,
    UNIFORM_CONTROL,
    HybridTPEProgramSearchAdapter,
    ProgramOptimizerObservationV1,
    StructuredSurrogateProgramSearchAdapter,
    UniformProgramSearchAdapter,
    program_availability_entries_v1,
    program_optimizer_lane_v1,
)
from our_system_phase2.services.search_v2_admission import (
    AbsoluteEconomicAdmission,
)
from our_system_phase2.services.search_v2_conditional_uplift import (
    ProgramUpliftCredit,
)


def _space_rows(count: int = 96) -> list[dict[str, object]]:
    rows = []
    for index in range(count):
        template = ("BASE_TEMPORAL", "BASE_MARKET", "BASE_EVENT")[index % 3]
        genes = {
            "skeleton_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
            "gene_surface_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
            "program_template_id": template,
            "active_component_roles": (
                "base+temporal"
                if template == "BASE_TEMPORAL"
                else "base+market"
                if template == "BASE_MARKET"
                else "base+event"
            ),
            "composition_topology": template,
            "combination_temporal": ("ADD", "MAX")[index % 2],
            "combination_market": ("FILTER", "GATE")[index % 2],
            "combination_event_application": ("FILTER", "GATE")[index % 2],
            "base__route_id": "SLOW_CROSS_SECTIONAL_LEVEL",
            "base__skeleton_id": f"base.{index % 4}",
            "base__primary_field_id": f"field_{index % 8}",
            "enhancer__route_id": (
                "SLOW_TEMPORAL_CHANGE"
                if template == "BASE_TEMPORAL"
                else "MARKET_REGIME_CONDITION"
                if template == "BASE_MARKET"
                else "DISCLOSURE_EVENT"
            ),
            "enhancer__skeleton_id": f"enhancer.{index % 6}",
            "enhancer__primary_field_id": f"field_{(index * 3) % 11}",
            "raw_field_count": str(2 + index % 4),
            "rolling_node_count": str(index % 3),
            "lag_class": f"lag_{index % 3}",
            "joint_clock_class": "PRIOR_CLOSE_NEXT_OPEN",
            "structural_complexity_class": f"complexity_{index % 5}",
            "interaction_topology": f"interaction_{index % 4}",
        }
        rows.append({"genes": genes})
    return rows


def _admission(*, admitted: bool, identity: str) -> AbsoluteEconomicAdmission:
    return AbsoluteEconomicAdmission(
        record_payload_sha256=f"record-{identity}",
        pair_id=f"pair-{identity}",
        program_id=f"program-{identity}",
        control_program_id=f"control-{identity}",
        admitted=admitted,
        failure_reasons=() if admitted else ("PRIMARY_NET_REWARD_NOT_POSITIVE",),
        metrics={"synthetic": True},
    )


def _observation(
    ask: dict[str, object], *, admitted: bool, uplift: float = 0.0
) -> ProgramOptimizerObservationV1:
    identity = str(ask["exact_identity"])
    admission = _admission(admitted=admitted, identity=identity)
    credit = (
        ProgramUpliftCredit(
            record_payload_sha256=admission.record_payload_sha256,
            pair_id=admission.pair_id,
            program_id=admission.program_id,
            control_program_id=admission.control_program_id,
            program_credit={
                "matched_cumulative_return_increment": uplift,
                "matched_net_reward_increment": uplift * 2.0,
            },
        )
        if admitted
        else None
    )
    return ProgramOptimizerObservationV1(
        proposal_id=str(ask["proposal_id"]),
        exact_identity=identity,
        admission=admission,
        uplift=credit,
    )


def test_all_three_adapters_share_one_program_space_and_availability() -> None:
    entries = program_availability_entries_v1(_space_rows())
    uniform = UniformProgramSearchAdapter(
        entries=entries, seen_exact_identities=(), seed=11
    )
    tpe = HybridTPEProgramSearchAdapter(
        entries=entries,
        seen_exact_identities=(),
        seed=12,
        n_startup_trials=2,
        n_ei_candidates=8,
    )
    surrogate = StructuredSurrogateProgramSearchAdapter(
        entries=entries,
        seen_exact_identities=(),
        seed=13,
        cold_start_asks=4,
        n_estimators=32,
    )
    assert uniform.optimizer_metadata()["optimizer_arm"] == UNIFORM_CONTROL
    assert tpe.optimizer_metadata()["optimizer_arm"] == HYBRID_TPE_PROGRAM
    assert surrogate.optimizer_metadata()["optimizer_arm"] == STRUCTURED_SURROGATE_PROGRAM
    assert {
        uniform.optimizer_metadata()["program_space_id"],
        tpe.optimizer_metadata()["program_space_id"],
        surrogate.optimizer_metadata()["program_space_id"],
    } == {"CN_TYPED_PROGRAM_GENE_SPACE_V1"}
    assert all(entry.route_id == PROGRAM_ROUTE_ID for entry in entries)


def test_uniform_closes_asks_without_retaining_economic_feedback() -> None:
    entries = program_availability_entries_v1(_space_rows(8))
    adapter = UniformProgramSearchAdapter(
        entries=entries, seen_exact_identities=(), seed=11
    )
    asked = adapter.ask(checkpoint_id="checkpoint_001", count=4)

    receipt = adapter.tell(
        [_observation(row, admitted=True, uplift=9.0) for row in asked]
    )

    assert receipt["optimizer_feedback_applied"] is False
    assert receipt["economic_observations_retained"] == 0
    assert "admitted_count" not in receipt
    assert "conditional_objective_count" not in receipt
    assert adapter.observation_count == 0


def test_mixed_template_role_slots_are_one_fixed_conditional_space() -> None:
    entries = program_availability_entries_v1(
        [
            {
                "genes": {
                    "skeleton_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "program_template_id": "BASE_TEMPORAL",
                    "base__route_id": "BASE",
                    "temporal__primitive": "ROLL",
                }
            },
            {
                "genes": {
                    "skeleton_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "program_template_id": "BASE_EVENT",
                    "base__route_id": "BASE",
                    "event__episode_type": "DISCLOSURE",
                }
            },
        ]
    )
    assert tuple(entries[0].genes) == tuple(entries[1].genes)
    assert entries[0].genes["event__episode_type"] == "__INACTIVE__"
    assert entries[1].genes["temporal__primitive"] == "__INACTIVE__"
    lane = program_optimizer_lane_v1(entries)["CN_TYPED_PROGRAM_GENE_SPACE_V1"]
    assert lane["ordered_categories_by_slot"]["program_template_id"] == [
        "BASE_TEMPORAL",
        "BASE_EVENT",
    ]


def test_program_genes_accept_legal_zero_free_gene_component(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from our_system_phase2.services import program_search_optimizer_v1 as module

    monkeypatch.setattr(
        module,
        "route_generation_receipt_v1",
        lambda _candidate: {
            "skeleton_id": "DETERMINISTIC_BASE_SKELETON",
            "categorical_genes": {},
        },
    )
    component = type(
        "Component",
        (),
        {
            "route_id": "DETERMINISTIC_BASE_ROUTE",
            "primary": {},
        },
    )()
    program = type(
        "Program",
        (),
        {
            "joint_clock_contract": type(
                "Clock",
                (),
                {key: key for key in module.PROGRAM_CLOCK_KEYS},
            )(),
            "to_record": lambda self: {"nodes": []},
        },
    )()
    compiled = type(
        "Compiled",
        (),
        {
            "field_lags": {},
            "complexity_report": {},
            "physical_leaf_ids": (),
        },
    )()

    genes = module.program_structural_genes_v1(
        program_template_id="BASE",
        components={"base": component},
        combination_policy=None,
        program=program,
        compiled=compiled,
    )

    assert genes["base__route_id"] == "DETERMINISTIC_BASE_ROUTE"
    assert genes["base__skeleton_id"] == "DETERMINISTIC_BASE_SKELETON"
    assert set(key for key in genes if key.startswith("base__")) == {
        "base__route_id",
        "base__skeleton_id",
    }


def test_hybrid_tpe_uses_real_trials_and_separate_feasibility_constraint() -> None:
    entries = program_availability_entries_v1(_space_rows())
    adapter = HybridTPEProgramSearchAdapter(
        entries=entries,
        seen_exact_identities=(),
        seed=17,
        n_startup_trials=2,
        n_ei_candidates=8,
    )
    asked = adapter.ask(checkpoint_id="checkpoint_001", count=6)
    assert len(asked) == 6
    assert all(isinstance(row["trial_number"], int) for row in asked)
    receipt = adapter.tell(
        [
            _observation(
                row,
                admitted=index % 2 == 0,
                uplift=0.1 + index * 0.01,
            )
            for index, row in enumerate(asked)
        ]
    )
    transcript = adapter.tpe.history[-1]
    told = transcript["observations"]
    completed = [row for row in told if row["state"] == "COMPLETE"]
    assert receipt["absolute_admission_scalarized_with_uplift"] is False
    assert all(
        row["objective_domain_eligible"] == (row["optimizer_reward"] is not None)
        for row in completed
    )
    assert all(
        row["admission_constraint_violation"]
        == (0.0 if row["objective_domain_eligible"] else 1.0)
        for row in completed
    )
    assert all(
        row["study_value"] == 0.0
        for row in completed
        if not row["objective_domain_eligible"]
    )
    snapshot = adapter.snapshot()
    restored = HybridTPEProgramSearchAdapter.restore(
        snapshot=snapshot,
        entries=entries,
        seen_exact_identities=(),
        seed=17,
        n_startup_trials=2,
        n_ei_candidates=8,
    )
    restored_snapshot = restored.snapshot()
    assert restored_snapshot["availability"] == snapshot["availability"]
    assert restored_snapshot["history"] == snapshot["history"]
    assert restored_snapshot["tpe_history"] == snapshot["tpe_history"]


def test_dual_head_rejects_fake_uplift_for_admission_failure() -> None:
    entries = program_availability_entries_v1(_space_rows(8))
    adapter = UniformProgramSearchAdapter(
        entries=entries, seen_exact_identities=(), seed=19
    )
    ask = adapter.ask(checkpoint_id="checkpoint_001", count=1)[0]
    valid = _observation(ask, admitted=True, uplift=0.2)
    with pytest.raises(ValueError, match="DUAL_HEAD_DOMAIN_DRIFT"):
        replace(valid, admission=_admission(admitted=False, identity="forged"))


def test_structured_surrogate_generalizes_and_distinguishes_unseen_programs() -> None:
    entries = program_availability_entries_v1(_space_rows())
    adapter = StructuredSurrogateProgramSearchAdapter(
        entries=entries,
        seen_exact_identities=(),
        seed=23,
        cold_start_asks=8,
        candidate_pool_size=96,
        n_estimators=64,
        min_samples_leaf=1,
        exploration_beta=0.5,
    )
    for checkpoint in range(3):
        asked = adapter.ask(
            checkpoint_id=f"checkpoint_{checkpoint + 1:03d}", count=8
        )
        adapter.tell(
            [
                _observation(
                    row,
                    admitted=(
                        row["program_genes"]["program_template_id"]
                        != "BASE_EVENT"
                    ),
                    uplift=(
                        0.5
                        if row["program_genes"]["program_template_id"]
                        == "BASE_TEMPORAL"
                        else 0.1
                    ),
                )
                for row in asked
            ]
        )
    remaining = adapter.controller.remaining_entries(route_id=PROGRAM_ROUTE_ID)
    temporal = next(
        entry
        for entry in remaining
        if entry.genes["program_template_id"] == "BASE_TEMPORAL"
    )
    event = next(
        entry
        for entry in remaining
        if entry.genes["program_template_id"] == "BASE_EVENT"
    )
    predictions = adapter.predict_acquisition([temporal.genes, event.genes])
    assert predictions[0]["acquisition"] != predictions[1]["acquisition"]
    assert predictions[0]["feasibility_probability"] > predictions[1][
        "feasibility_probability"
    ]
    assert predictions[0]["uplift_mean"] != predictions[1]["uplift_mean"]
    snapshot = adapter.snapshot()
    restored = StructuredSurrogateProgramSearchAdapter.restore(
        snapshot=snapshot,
        entries=entries,
        seen_exact_identities=(),
        seed=23,
        cold_start_asks=8,
        candidate_pool_size=96,
        n_estimators=64,
        min_samples_leaf=1,
        exploration_beta=0.5,
    )
    assert restored.predict_acquisition([temporal.genes, event.genes]) == predictions
