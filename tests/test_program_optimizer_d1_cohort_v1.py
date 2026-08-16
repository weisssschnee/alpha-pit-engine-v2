from __future__ import annotations

import json

from our_system_phase2.services.program_optimizer_d1_cohort_v1 import (
    D1_LOGICAL_RECORDS,
    D1_SELECTOR_COUNTS,
    D1_SELECTION_SURROGATE,
    D1_SELECTION_TPE,
    D1_SELECTION_UNIFORM,
    ProgramOptimizerD1CohortV1,
)
from our_system_phase2.services.program_optimizer_successor_benchmark_v1 import (
    POLICY_D1,
    POLICY_D2,
    POLICY_TPE,
    POLICY_UNIFORM,
    PhysicalProgramResultV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    program_availability_entries_v1,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import ProgramUpliftCredit
from our_system_phase2.services.unified_capability_registry import stable_hash


TEMPLATES = (
    "BASE_TEMPORAL",
    "BASE_MARKET",
    "BASE_EVENT",
    "BASE_TEMPORAL_MARKET",
    "BASE_TEMPORAL_EVENT",
    "BASE_MARKET_EVENT",
    "BASE_TEMPORAL_MARKET_EVENT",
)


def _space():
    rows = []
    groups = []
    for template in TEMPLATES:
        for group in range(24):
            for variant in range(2):
                rows.append(
                    {
                        "genes": {
                            "skeleton_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                            "gene_surface_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                            "program_template_id": template,
                            "active_component_roles": template.lower(),
                            "composition_topology": template,
                            "base__route_id": "SLOW_CROSS_SECTIONAL_LEVEL",
                            "base__skeleton_id": f"base.{group}",
                            "enhancer__route_id": f"route.{template}",
                            "enhancer__skeleton_id": f"enhancer.{group % 5}",
                            "combination_temporal": "ADD" if variant == 0 else "MAX",
                            "combination_market": "FILTER" if variant == 0 else "GATE",
                            "combination_event_application": "FILTER",
                            "raw_field_count": str(2 + variant),
                            "rolling_node_count": str(group % 3),
                            "lag_class": f"lag.{group % 4}",
                            "joint_clock_class": "PRIOR_CLOSE_NEXT_OPEN",
                            "structural_complexity_class": f"c.{group % 6}",
                            "interaction_topology": f"i.{group}.{variant}",
                        }
                    }
                )
                groups.append(f"{template}.base.{group}")
    entries = program_availability_entries_v1(rows)
    return entries, {
        entry.exact_identity: group
        for entry, group in zip(entries, groups, strict=True)
    }


def _cohort(*, prior_exact_identities=()):
    entries, groups = _space()
    return ProgramOptimizerD1CohortV1(
        entries=entries,
        template_ids=TEMPLATES,
        group_by_exact_identity=groups,
        uniform_seed=101,
        tpe_seed=103,
        surrogate_seed=107,
        tpe_config={"n_startup_trials": 2, "n_ei_candidates": 8},
        surrogate_config={
            "cold_start_asks": 24,
            "candidate_pool_size": 64,
            "n_estimators": 32,
            "min_samples_leaf": 1,
            "exploration_beta": 1.0,
        },
        prior_exact_identities=prior_exact_identities,
    )


def _physical(exact_identity: str) -> PhysicalProgramResultV1:
    admitted = int(stable_hash(exact_identity)[-1], 16) % 3 != 0
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=f"record-{exact_identity}",
        pair_id=f"pair-{exact_identity}",
        program_id=f"program-{exact_identity}",
        control_program_id=f"control-{exact_identity}",
        admitted=admitted,
        failure_reasons=() if admitted else ("PRIMARY_NET_REWARD_NOT_POSITIVE",),
        metrics={"synthetic": True},
    )
    uplift = (
        ProgramUpliftCredit(
            record_payload_sha256=admission.record_payload_sha256,
            pair_id=admission.pair_id,
            program_id=admission.program_id,
            control_program_id=admission.control_program_id,
            program_credit={
                "matched_cumulative_net_return_increment": 0.1,
                "matched_net_reward_increment": 0.2,
            },
        )
        if admitted
        else None
    )
    return PhysicalProgramResultV1.create(
        exact_identity=exact_identity,
        admission=admission,
        uplift=uplift,
    )


def _commit(cohort, prepared):
    return cohort.commit_wave(
        prepared,
        {exact: _physical(exact) for exact in prepared.physical_exact_identities},
    )


def test_wave0_uniform_feedback_updates_bootstrap_tpe() -> None:
    cohort = _cohort()
    before = cohort._inner._tpe_snapshots[POLICY_TPE]
    prepared = cohort.prepare_wave()
    assert {row.selection_kind for row in prepared.asks} == {D1_SELECTION_UNIFORM}
    assert all(row.optimizer_ask is not None for row in prepared.asks)
    _commit(cohort, prepared)
    assert cohort._inner._tpe_snapshots[POLICY_TPE] != before
    assert len(cohort.completed_rows()) == 7


def test_handoff_preserves_exact_tested_selector_order() -> None:
    cohort = _cohort()
    kinds = []
    for wave in range(6):
        prepared = cohort.prepare_wave()
        kinds.append({row.selection_kind for row in prepared.asks})
        _commit(cohort, prepared)
    assert kinds == [
        {D1_SELECTION_UNIFORM},
        {D1_SELECTION_TPE},
        {D1_SELECTION_TPE},
        {D1_SELECTION_TPE},
        {D1_SELECTION_UNIFORM},
        {D1_SELECTION_SURROGATE},
    ]
    assert len(cohort.completed_rows()) == 42


def test_prior_exclusion_and_json_roundtrip_preserve_next_ask() -> None:
    entries, _ = _space()
    prior = tuple(
        next(
            entry.exact_identity
            for entry in entries
            if str(entry.genes["program_template_id"]) == template
        )
        for template in TEMPLATES
    )
    cohort = _cohort(prior_exact_identities=prior)
    for _ in range(5):
        _commit(cohort, cohort.prepare_wave())
    snapshot = json.loads(json.dumps(cohort.snapshot()))
    entries, groups = _space()
    restored = ProgramOptimizerD1CohortV1.restore(
        snapshot,
        entries=entries,
        template_ids=TEMPLATES,
        group_by_exact_identity=groups,
        uniform_seed=101,
        tpe_seed=103,
        surrogate_seed=107,
        tpe_config={"n_startup_trials": 2, "n_ei_candidates": 8},
        surrogate_config={
            "cold_start_asks": 24,
            "candidate_pool_size": 64,
            "n_estimators": 32,
            "min_samples_leaf": 1,
            "exploration_beta": 1.0,
        },
        prior_exact_identities=prior,
    )
    assert restored.snapshot() == snapshot
    expected = cohort.prepare_wave()
    observed = restored.prepare_wave()
    assert [row.exact_identity for row in observed.asks] == [
        row.exact_identity for row in expected.asks
    ]
    assert set(prior).isdisjoint(observed.physical_exact_identities)


def test_full_synthetic_cohort_is_d1_only_and_has_frozen_selector_counts() -> None:
    cohort = _cohort()
    observed = {key: 0 for key in D1_SELECTOR_COUNTS}
    for _ in range(20):
        prepared = cohort.prepare_wave()
        for row in prepared.asks:
            observed[row.selection_kind] += 1
        _commit(cohort, prepared)
    cohort.verify_terminal_shape()
    assert observed == D1_SELECTOR_COUNTS
    assert cohort.selector_counts() == D1_SELECTOR_COUNTS
    assert len(cohort.completed_rows()) == D1_LOGICAL_RECORDS
    assert not cohort._inner._completed_rows[POLICY_UNIFORM]
    assert not cohort._inner._completed_rows[POLICY_TPE]
    assert not cohort._inner._completed_rows[POLICY_D2]
    assert len(cohort._inner._completed_rows[POLICY_D1]) == D1_LOGICAL_RECORDS
