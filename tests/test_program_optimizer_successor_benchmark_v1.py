from __future__ import annotations

import copy
import json

import pytest

from our_system_phase2.services.program_optimizer_successor_benchmark_v1 import (
    BOOTSTRAP_WAVES,
    MAX_GATE_RAW_ATTEMPTS_PER_ECONOMIC_ASK,
    POLICY_D1,
    POLICY_D2,
    POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY_DENOMINATOR,
    POLICY_TPE,
    POLICY_UNIFORM,
    POST_BOOTSTRAP_EFFICIENCY_DENOMINATOR,
    TOTAL_POLICY_EFFICIENCY_DENOMINATOR,
    UNIFORM_FLOOR_WAVES,
    PhysicalProgramResultV1,
    ProgramOptimizerSuccessorBenchmarkV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    PROGRAM_ROUTE_ID,
    HybridTPEProgramSearchAdapter,
    StructuredSurrogateProgramSearchAdapter,
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


def _benchmark(
    *, prior_exact_identities=()
) -> ProgramOptimizerSuccessorBenchmarkV1:
    entries, groups = _space()
    return ProgramOptimizerSuccessorBenchmarkV1(
        entries=entries,
        template_ids=TEMPLATES,
        group_by_exact_identity=groups,
        uniform_seed=101,
        tpe_seed=103,
        d1_surrogate_seed=107,
        d2_surrogate_seed=109,
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


def _physical(exact_identity: str, *, admitted: bool | None = None) -> PhysicalProgramResultV1:
    if admitted is None:
        admitted = int(stable_hash(exact_identity)[-1], 16) % 3 != 0
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=f"record-{exact_identity}",
        pair_id=f"pair-{exact_identity}",
        program_id=f"program-{exact_identity}",
        control_program_id=f"control-{exact_identity}",
        admitted=bool(admitted),
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


def _commit_prepared(benchmark, prepared):
    physical = {
        exact: _physical(exact)
        for exact in prepared.physical_exact_identities
    }
    return benchmark.commit_wave(prepared, physical)


def _run_bootstrap(benchmark):
    prepared_rows = []
    for _ in range(BOOTSTRAP_WAVES):
        prepared = benchmark.prepare_wave()
        prepared_rows.append(prepared)
        _commit_prepared(benchmark, prepared)
    return prepared_rows


def test_contract_denominators_and_floor_waves_are_frozen() -> None:
    assert TOTAL_POLICY_EFFICIENCY_DENOMINATOR == 140
    assert POST_BOOTSTRAP_EFFICIENCY_DENOMINATOR == 112
    assert POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY_DENOMINATOR == 84
    assert len(UNIFORM_FLOOR_WAVES) == 5
    assert UNIFORM_FLOOR_WAVES[0] == 0
    assert not set(UNIFORM_FLOOR_WAVES).intersection({1, 2, 3})
    assert MAX_GATE_RAW_ATTEMPTS_PER_ECONOMIC_ASK == 32


def test_prior_evaluated_exact_programs_are_unavailable_to_every_policy() -> None:
    entries, _ = _space()
    prior = tuple(
        next(
            row.exact_identity
            for row in entries
            if str(row.genes["program_template_id"]) == template
        )
        for template in TEMPLATES
    )
    benchmark = _benchmark(prior_exact_identities=prior)
    prepared = benchmark.prepare_wave()
    assert set(prior).isdisjoint(prepared.physical_exact_identities)
    for policy in (POLICY_TPE, POLICY_D2):
        adapter = benchmark._restore_tpe(benchmark._tpe_snapshots[policy])
        remaining = {
            row.exact_identity
            for row in adapter.controller.remaining_entries(route_id=PROGRAM_ROUTE_ID)
        }
        assert set(prior).isdisjoint(remaining)
    assert benchmark.snapshot()["configuration"]["prior_exact_identity_count"] == 7


def test_nonempty_prior_set_survives_json_state_roundtrip() -> None:
    entries, groups = _space()
    prior = tuple(
        next(
            row.exact_identity
            for row in entries
            if str(row.genes["program_template_id"]) == template
        )
        for template in TEMPLATES
    )
    benchmark = _benchmark(prior_exact_identities=prior)
    snapshot = json.loads(json.dumps(benchmark.snapshot(), sort_keys=True))
    restored = ProgramOptimizerSuccessorBenchmarkV1.restore(
        snapshot,
        entries=entries,
        template_ids=TEMPLATES,
        group_by_exact_identity=groups,
        uniform_seed=101,
        tpe_seed=103,
        d1_surrogate_seed=107,
        d2_surrogate_seed=109,
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
    assert [row.exact_identity for row in restored.prepare_wave().asks] == [
        row.exact_identity for row in benchmark.prepare_wave().asks
    ]


def test_wave_template_serialization_rotates_without_changing_membership() -> None:
    benchmark = _benchmark()
    assert benchmark._wave_template_order(0) == TEMPLATES
    assert benchmark._wave_template_order(1) == TEMPLATES[1:] + TEMPLATES[:1]
    assert benchmark._wave_template_order(7) == TEMPLATES
    for wave in range(7):
        assert set(benchmark._wave_template_order(wave)) == set(TEMPLATES)


def test_prepare_wave_is_blind_atomic_and_cross_policy_isolated() -> None:
    benchmark = _benchmark()
    before = benchmark.snapshot()
    prepared = benchmark.prepare_wave()
    assert benchmark.snapshot() == before
    assert len(prepared.asks) == 28
    for policy in (POLICY_UNIFORM, POLICY_TPE, POLICY_D1, POLICY_D2):
        policy_rows = [row for row in prepared.asks if row.policy == policy]
        assert {row.template_id for row in policy_rows} == set(TEMPLATES)
        assert len(policy_rows) == 7
    # Wave zero is the common floor: all four logical policies can share one
    # physical Program per template without depleting one another's virtual state.
    assert len(prepared.physical_exact_identities) == 7
    assert all(len(policies) == 4 for policies in prepared.overlap_map().values())


def test_one_policy_reservation_does_not_deplete_peer_virtual_pool() -> None:
    benchmark = _benchmark()
    left = benchmark._restore_tpe(benchmark._tpe_snapshots[POLICY_TPE])
    right = benchmark._restore_tpe(benchmark._tpe_snapshots[POLICY_D2])
    exact = next(
        row.exact_identity
        for row in left.controller.remaining_entries(route_id=PROGRAM_ROUTE_ID)
        if str(row.genes["program_template_id"]) == TEMPLATES[0]
    )
    assert left.controller.reserve_exact(
        route_id=PROGRAM_ROUTE_ID,
        exact_identity=exact,
        emission_mode="AVAILABILITY_AWARE_UNIFORM",
        source_exact_identity="test",
    ) is not None
    assert exact not in {
        row.exact_identity
        for row in left.controller.remaining_entries(route_id=PROGRAM_ROUTE_ID)
    }
    assert exact in {
        row.exact_identity
        for row in right.controller.remaining_entries(route_id=PROGRAM_ROUTE_ID)
    }


def test_partial_wave_commit_failure_keeps_durable_state_exact() -> None:
    benchmark = _benchmark()
    prepared = benchmark.prepare_wave()
    before = benchmark.snapshot()
    exacts = prepared.physical_exact_identities
    incomplete = {exact: _physical(exact) for exact in exacts[:-1]}
    with pytest.raises(ValueError, match="PHYSICAL_RESULT_COVERAGE"):
        benchmark.commit_wave(prepared, incomplete)
    assert benchmark.snapshot() == before


def test_physical_dedup_backs_multiple_logical_receipts_with_one_hash() -> None:
    benchmark = _benchmark()
    prepared = benchmark.prepare_wave()
    receipt = _commit_prepared(benchmark, prepared)
    assert receipt["physical_unique_count"] == 7
    assert receipt["logical_ask_count"] == 28
    for exact, policies in receipt["overlap_map"].items():
        rows = [
            row for row in receipt["logical_receipts"]
            if row["exact_identity"] == exact
        ]
        assert len(rows) == len(policies) == 4
        assert len({row["physical_result_hash"] for row in rows}) == 1


def test_physical_result_hash_is_reverified_at_commit() -> None:
    benchmark = _benchmark()
    prepared = benchmark.prepare_wave()
    results = {
        exact: _physical(exact) for exact in prepared.physical_exact_identities
    }
    exact = prepared.physical_exact_identities[0]
    good = results[exact]
    results[exact] = PhysicalProgramResultV1(
        exact_identity=good.exact_identity,
        admission=good.admission,
        uplift=good.uplift,
        physical_result_hash="0" * 64,
    )
    before = benchmark.snapshot()
    with pytest.raises(ValueError, match="PHYSICAL_RESULT_HASH_DRIFT"):
        benchmark.commit_wave(prepared, results)
    assert benchmark.snapshot() == before


def test_common_bootstrap_is_identical_and_handoff_occurs_after_28() -> None:
    benchmark = _benchmark()
    waves = _run_bootstrap(benchmark)
    for prepared in waves:
        by_policy = {
            policy: [
                row.exact_identity
                for row in prepared.asks
                if row.policy == policy
            ]
            for policy in (POLICY_TPE, POLICY_D1, POLICY_D2)
        }
        assert by_policy[POLICY_TPE] == by_policy[POLICY_D1] == by_policy[POLICY_D2]
    assert benchmark.wave_index == 4
    snapshot = benchmark.snapshot()
    assert len(snapshot["completed_rows"][POLICY_D1]) == 28
    assert len(snapshot["completed_rows"][POLICY_D2]) == 28
    assert snapshot["d1_surrogate_snapshot"]["optimizer_metadata"]["optimizer_arm"] == (
        "STRUCTURED_SURROGATE_PROGRAM"
    )
    assert len(snapshot["d1_surrogate_snapshot"]["observations"]) == 28
    assert len(snapshot["d2_surrogate_snapshot"]["observations"]) == 28


def test_successor_state_roundtrip_is_exact_at_wave_boundary() -> None:
    benchmark = _benchmark()
    _run_bootstrap(benchmark)
    snapshot = json.loads(json.dumps(benchmark.snapshot(), sort_keys=True))
    entries, groups = _space()
    restored = ProgramOptimizerSuccessorBenchmarkV1.restore(
        snapshot,
        entries=entries,
        template_ids=TEMPLATES,
        group_by_exact_identity=groups,
        uniform_seed=101,
        tpe_seed=103,
        d1_surrogate_seed=107,
        d2_surrogate_seed=109,
        tpe_config={"n_startup_trials": 2, "n_ei_candidates": 8},
        surrogate_config={
            "cold_start_asks": 24,
            "candidate_pool_size": 64,
            "n_estimators": 32,
            "min_samples_leaf": 1,
            "exploration_beta": 1.0,
        },
    )
    assert restored.snapshot() == snapshot
    assert [row.exact_identity for row in restored.prepare_wave().asks] == [
        row.exact_identity for row in benchmark.prepare_wave().asks
    ]


def test_feasibility_prediction_ignores_uplift_values() -> None:
    entries, _ = _space()
    config = dict(
        entries=entries,
        seen_exact_identities=(),
        seed=211,
        cold_start_asks=4,
        candidate_pool_size=64,
        n_estimators=32,
        min_samples_leaf=1,
    )
    left = StructuredSurrogateProgramSearchAdapter(**config)
    right = StructuredSurrogateProgramSearchAdapter(**config)
    selected = entries[:12]
    left_rows = []
    right_rows = []
    for index, entry in enumerate(selected):
        admitted = index % 3 != 0
        common = {
            "proposal_id": f"p-{index}",
            "exact_identity": entry.exact_identity,
            "admitted": admitted,
        }
        left_rows.append({**common, "uplift": 0.1 + index if admitted else None})
        right_rows.append({**common, "uplift": 100.0 - index if admitted else None})
    left.ingest_completed_observations(left_rows)
    right.ingest_completed_observations(right_rows)
    probe = [entry.genes for entry in entries[20:40]]
    assert left.predict_feasibility(probe) == right.predict_feasibility(probe)
    assert left.observation_count == right.observation_count == 12


def test_d1_and_d2_continue_after_bootstrap_without_cross_policy_state() -> None:
    benchmark = _benchmark()
    _run_bootstrap(benchmark)
    # wave 4 is the next common floor and still produces no adaptive tell.
    floor = benchmark.prepare_wave()
    _commit_prepared(benchmark, floor)
    # wave 5 is the first post-bootstrap optimized wave for D1/D2.
    prepared = benchmark.prepare_wave()
    assert prepared.wave_index == 5
    d1 = [row for row in prepared.asks if row.policy == POLICY_D1]
    d2 = [row for row in prepared.asks if row.policy == POLICY_D2]
    assert len(d1) == len(d2) == 7
    assert all(row.selection_kind == "SURROGATE_OPTIMIZED" for row in d1)
    assert all(row.selection_kind == "D2_FEASIBILITY_GATED_TPE" for row in d2)
    before = benchmark.snapshot()
    _commit_prepared(benchmark, prepared)
    after = benchmark.snapshot()
    assert len(after["completed_rows"][POLICY_D1]) == len(before["completed_rows"][POLICY_D1]) + 7
    assert len(after["completed_rows"][POLICY_D2]) == len(before["completed_rows"][POLICY_D2]) + 7
    assert after["d1_surrogate_snapshot"]["observations"] != after["d2_surrogate_snapshot"]["observations"]


def test_d2_gate_rejects_outside_top_half_without_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    benchmark = _benchmark()
    _run_bootstrap(benchmark)
    # Advance the floor wave so D2 gating is active on wave 5.
    _commit_prepared(benchmark, benchmark.prepare_wave())
    adapter = benchmark._restore_tpe(benchmark._tpe_snapshots[POLICY_D2])
    model = benchmark._restore_surrogate(
        snapshot=benchmark._d2_surrogate_snapshot,
        seed=benchmark.d2_surrogate_seed,
        policy=POLICY_D2,
    )
    template_entries = benchmark._available_for_policy(
        policy=POLICY_D2,
        template_id=TEMPLATES[0],
        wave_index=benchmark.wave_index,
        adapter=model,
        enforce_groups=False,
    )
    # Force a deterministic gate: first half scores 1, second half 0.
    high = {row.exact_identity for row in template_entries[: len(template_entries) // 2]}

    def fake_predict(rows):
        return [
            {
                "cold_start": False,
                "feasibility_probability": 1.0 if stable_hash(dict(genes)) in high else 0.0,
            }
            for genes in rows
        ]

    # exact identity is stable_hash(genes) for AvailabilityEntry in this test space.
    monkeypatch.setattr(model, "predict_feasibility", fake_predict)
    outside = next(row for row in template_entries if row.exact_identity not in high)
    inside = next(row for row in template_entries if row.exact_identity in high)
    originals = adapter.tpe.ask_trial
    queue = [outside.exact_identity, inside.exact_identity]

    def fake_ask_trial(**kwargs):
        exact = queue.pop(0) if queue else inside.exact_identity
        # Use a real pending TPE trial, but force its recorded exact category for the
        # gate decision. This keeps tell/replay semantics real for the accepted test.
        row = originals(**kwargs)
        row = copy.deepcopy(row)
        row["genes"]["program_exact_identity"] = exact
        row["proposal_id"] = f"forced-{len(queue)}-{exact[:8]}"
        return row

    monkeypatch.setattr(adapter.tpe, "ask_trial", fake_ask_trial)
    monkeypatch.setattr(adapter.tpe, "annotate_pending_trial", lambda *_args, **_kwargs: None)
    stats = {
        "gate_raw_ask_count": 0,
        "gate_rejection_count": 0,
        "economic_ask_count": 0,
        "ordinary_projection_count": 0,
    }
    gate_rejections = []
    # Restrict the helper to one template for this unit-level gate test.
    old_templates = benchmark.template_ids
    benchmark.template_ids = (TEMPLATES[0],)
    try:
        asks = benchmark._ask_tpe_wave(
            adapter=adapter,
            policy=POLICY_D2,
            wave_index=benchmark.wave_index,
            feasibility_model=model,
            gate_statistics=stats,
            gate_rejections=gate_rejections,
        )
    finally:
        benchmark.template_ids = old_templates
    assert len(asks) == 1
    assert stats["gate_raw_ask_count"] == 2
    assert stats["gate_rejection_count"] == 1
    assert stats["economic_ask_count"] == 1
    assert stats["ordinary_projection_count"] == 0
    rejected = list(adapter._tpe_internal_observations.values())
    assert any(row["outcome_class"] == "SURROGATE_FEASIBILITY_REJECTED" for row in rejected)
    assert len(gate_rejections) == 1
    assert gate_rejections[0]["outcome_class"] == "SURROGATE_FEASIBILITY_REJECTED"
    assert gate_rejections[0]["raw_exact_identity"]
    assert gate_rejections[0]["trial_number"] >= 0
    assert 0.0 <= gate_rejections[0]["feasibility_probability"] <= 1.0


def test_d2_in_gate_base_conflict_uses_ordinary_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _benchmark()
    _run_bootstrap(benchmark)
    _commit_prepared(benchmark, benchmark.prepare_wave())
    adapter = benchmark._restore_tpe(benchmark._tpe_snapshots[POLICY_D2])
    model = benchmark._restore_surrogate(
        snapshot=benchmark._d2_surrogate_snapshot,
        seed=benchmark.d2_surrogate_seed,
        policy=POLICY_D2,
    )
    template = TEMPLATES[0]
    used_groups = benchmark._group_counts[POLICY_D2][template]
    reused_group = next(group for group, count in used_groups.items() if count > 0)
    target = next(
        row
        for row in model.controller.remaining_entries(route_id=PROGRAM_ROUTE_ID)
        if str(row.genes["program_template_id"]) == template
        and benchmark.group_by_exact_identity[row.exact_identity] == reused_group
    )
    monkeypatch.setattr(
        model,
        "predict_feasibility",
        lambda genes: [
            {
                "cold_start": False,
                "feasibility_probability": (
                    1.0 if stable_hash(dict(row)) == target.exact_identity else 0.0
                ),
            }
            for row in genes
        ],
    )
    monkeypatch.setattr(
        adapter.tpe,
        "ask_trial",
        lambda **_kwargs: {
            "proposal_id": "forced-in-gate",
            "trial_number": 9001,
            "genes": {"program_exact_identity": target.exact_identity},
        },
    )
    monkeypatch.setattr(adapter.tpe, "annotate_pending_trial", lambda *_args, **_kwargs: None)
    old_templates = benchmark.template_ids
    benchmark.template_ids = (template,)
    stats = {
        "gate_raw_ask_count": 0,
        "gate_rejection_count": 0,
        "economic_ask_count": 0,
        "ordinary_projection_count": 0,
    }
    gate_rejections = []
    try:
        asks = benchmark._ask_tpe_wave(
            adapter=adapter,
            policy=POLICY_D2,
            wave_index=benchmark.wave_index,
            feasibility_model=model,
            gate_statistics=stats,
            gate_rejections=gate_rejections,
        )
    finally:
        benchmark.template_ids = old_templates
    assert len(asks) == 1
    assert asks[0].exact_identity != target.exact_identity
    assert stats["gate_rejection_count"] == 0
    assert stats["ordinary_projection_count"] == 1
    projection = dict(asks[0].optimizer_ask["acquisition"])["projection"]
    assert projection["availability_replacement_applied"] is True
    assert projection["global_fallback"] is False


def test_d2_attempt_exhaustion_is_prefinancial_and_does_not_mutate_durable_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _benchmark()
    _run_bootstrap(benchmark)
    _commit_prepared(benchmark, benchmark.prepare_wave())
    before = benchmark.snapshot()
    adapter = benchmark._restore_tpe(benchmark._tpe_snapshots[POLICY_D2])
    model = benchmark._restore_surrogate(
        snapshot=benchmark._d2_surrogate_snapshot,
        seed=benchmark.d2_surrogate_seed,
        policy=POLICY_D2,
    )
    entries = benchmark._available_for_policy(
        policy=POLICY_D2,
        template_id=TEMPLATES[0],
        wave_index=benchmark.wave_index,
        adapter=model,
        enforce_groups=False,
    )
    accepted = min(entries, key=lambda row: row.exact_identity).exact_identity
    rejected = max(entries, key=lambda row: row.exact_identity).exact_identity
    monkeypatch.setattr(
        model,
        "predict_feasibility",
        lambda genes: [
            {
                "cold_start": False,
                "feasibility_probability": 1.0 if stable_hash(dict(row)) == accepted else 0.0,
            }
            for row in genes
        ],
    )
    counter = 0

    def always_rejected(**_kwargs):
        nonlocal counter
        counter += 1
        return {
            "proposal_id": f"reject-{counter}",
            "trial_number": counter,
            "genes": {"program_exact_identity": rejected},
        }

    monkeypatch.setattr(adapter.tpe, "ask_trial", always_rejected)
    old_templates = benchmark.template_ids
    benchmark.template_ids = (TEMPLATES[0],)
    try:
        with pytest.raises(RuntimeError, match="GATE_RAW_ATTEMPT_EXHAUSTED"):
            benchmark._ask_tpe_wave(
                adapter=adapter,
                policy=POLICY_D2,
                wave_index=benchmark.wave_index,
                feasibility_model=model,
                gate_statistics={
                    "gate_raw_ask_count": 0,
                    "gate_rejection_count": 0,
                    "economic_ask_count": 0,
                    "ordinary_projection_count": 0,
                },
                gate_rejections=[],
            )
    finally:
        benchmark.template_ids = old_templates
    assert counter == MAX_GATE_RAW_ATTEMPTS_PER_ECONOMIC_ASK
    assert benchmark.snapshot() == before
