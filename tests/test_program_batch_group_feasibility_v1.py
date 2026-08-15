from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from scripts import run_cn_joint_program_phase_c_v0 as phase_c_runner
from scripts import run_cn_program_optimizer_tournament_v1 as tournament_runner
from scripts import rehearse_cn_program_optimizer_tournament_prefinancial_v1 as rehearsal
from our_system_phase2.services.program_tournament_freeze_v1 import stage01_asks_v1

from our_system_phase2.services.program_optimizer_tournament_v1 import (
    ProgramOptimizerTournamentV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    PROGRAM_OPTIMIZER_ARMS,
    STRUCTURED_SURROGATE_PROGRAM,
    UNIFORM_CONTROL,
    HybridTPEProgramSearchAdapter,
    StructuredSurrogateProgramSearchAdapter,
    UniformProgramSearchAdapter,
    program_availability_entries_v1,
    verify_program_batch_group_selection_v1,
)


def _grouped_entries(*, groups: int = 24, variants: int = 6):
    return program_availability_entries_v1(
        [
            {
                "genes": {
                    "skeleton_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "gene_surface_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "program_template_id": "BASE_TEMPORAL",
                    "active_component_roles": "base+temporal",
                    "base__route_id": f"base_{group}",
                    "temporal__primitive": f"temporal_{variant}",
                    "program_variant": f"{group}:{variant}",
                    "base_group": f"group_{group}",
                }
            }
            for group in range(groups)
            for variant in range(variants)
        ]
    )


def _constraint(
    entries,
    *,
    current: dict[str, int] | None = None,
    minimum: int = 16,
    maximum: int = 4,
) -> dict[str, object]:
    return {
        "schema_version": "cn_program_batch_group_constraint_v1",
        "group_by_exact_identity": {
            entry.exact_identity: str(entry.genes["base_group"])
            for entry in entries
        },
        "current_group_counts": dict(current or {}),
        "minimum_distinct_groups": minimum,
        "maximum_per_group": maximum,
    }


def _tournament(entries) -> ProgramOptimizerTournamentV1:
    return ProgramOptimizerTournamentV1.fresh(
        campaign_id="batch-group-feasibility-test",
        entries_by_arm={arm: entries for arm in PROGRAM_OPTIMIZER_ARMS},
        seeds={
            UNIFORM_CONTROL: 101,
            HYBRID_TPE_PROGRAM: 103,
            STRUCTURED_SURROGATE_PROGRAM: 107,
        },
        tpe_config={"n_startup_trials": 2, "n_ei_candidates": 8},
        surrogate_config={
            "cold_start_asks": 4,
            "candidate_pool_size": len(entries),
            "n_estimators": 16,
            "min_samples_leaf": 1,
        },
    )


def _ask_groups(arm: str, current: dict[str, int]) -> tuple[list[str], dict]:
    entries = _grouped_entries()
    constraint = _constraint(entries, current=current)
    tournament = _tournament(entries)
    eligible = [entry.exact_identity for entry in entries]
    preview = tournament.ask(
        arm=arm,
        checkpoint_id="checkpoint_batch_group",
        count=8,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=eligible,
        batch_group_constraint=constraint,
    )
    tournament.commit_ask(
        arm=arm,
        checkpoint_id="checkpoint_batch_group",
        count=8,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=eligible,
        batch_group_constraint=constraint,
        expected_asks=preview,
    )
    receipt = verify_program_batch_group_selection_v1(
        [str(row["exact_identity"]) for row in preview],
        constraint=constraint,
    )
    groups = [
        str(constraint["group_by_exact_identity"][row["exact_identity"]])
        for row in preview
    ]
    return groups, receipt


@pytest.mark.parametrize("arm", PROGRAM_OPTIMIZER_ARMS)
def test_all_arms_enforce_batch_local_minimum_and_capacity(arm: str) -> None:
    groups, receipt = _ask_groups(arm, {})
    assert len(groups) == 8
    assert len(set(groups)) == 8
    assert max(receipt["group_counts_after"].values()) == 1

    current = {f"group_{index}": 1 for index in range(8)}
    groups, receipt = _ask_groups(arm, current)
    assert len(groups) == 8
    assert set(groups).isdisjoint(current)
    assert len(receipt["group_counts_after"]) == 16

    current = {f"group_{index}": 1 for index in range(15)}
    groups, receipt = _ask_groups(arm, current)
    assert len(groups) == 8
    assert groups[0] not in current
    assert len(receipt["group_counts_after"]) >= 16
    assert max(receipt["group_counts_after"].values()) <= 4

    current = {f"group_{index}": 1 for index in range(16)}
    groups, receipt = _ask_groups(arm, current)
    assert len(groups) == 8
    assert max(receipt["group_counts_after"].values()) <= 4

    current = {"group_0": 4, **{f"group_{index}": 1 for index in range(1, 16)}}
    groups, receipt = _ask_groups(arm, current)
    assert len(groups) == 8
    assert "group_0" not in groups
    assert receipt["group_counts_after"]["group_0"] == 4


@pytest.mark.parametrize("arm", PROGRAM_OPTIMIZER_ARMS)
@pytest.mark.parametrize("minimum", [8, 16])
def test_stage2_dynamic_minimum_is_enforced_per_batch(
    arm: str, minimum: int
) -> None:
    entries = _grouped_entries()
    constraint = _constraint(entries, minimum=minimum)
    tournament = _tournament(entries)
    asked = tournament.ask(
        arm=arm,
        checkpoint_id=f"checkpoint_stage2_min_{minimum}",
        count=minimum,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=[entry.exact_identity for entry in entries],
        batch_group_constraint=constraint,
    )
    receipt = verify_program_batch_group_selection_v1(
        [str(row["exact_identity"]) for row in asked],
        constraint=constraint,
    )
    assert len(asked) == minimum
    assert len(receipt["group_counts_after"]) == minimum


@pytest.mark.parametrize(
    ("adapter_type", "config"),
    [
        (UniformProgramSearchAdapter, {}),
        (
            HybridTPEProgramSearchAdapter,
            {"n_startup_trials": 2, "n_ei_candidates": 8},
        ),
        (
            StructuredSurrogateProgramSearchAdapter,
            {
                "cold_start_asks": 4,
                "candidate_pool_size": 144,
                "n_estimators": 16,
                "min_samples_leaf": 1,
            },
        ),
    ],
)
def test_batch_constraint_preserves_exact_snapshot_restore_next_ask(
    adapter_type, config
) -> None:
    entries = _grouped_entries()
    common = {
        "entries": entries,
        "seen_exact_identities": (),
        "seed": 113,
        **config,
    }
    live = adapter_type(**common)
    snapshot = live.snapshot()
    restored = adapter_type.restore(snapshot=snapshot, **common)
    constraint = _constraint(
        entries,
        current={f"group_{index}": 1 for index in range(8)},
    )
    kwargs = {
        "checkpoint_id": "checkpoint_restore_batch_group",
        "count": 8,
        "required_program_template_id": "BASE_TEMPORAL",
        "eligible_exact_identities": [entry.exact_identity for entry in entries],
        "batch_group_constraint": constraint,
    }
    assert restored.ask(**kwargs) == live.ask(**kwargs)


def test_insufficient_distinct_supply_returns_short_batch_for_runner_gate() -> None:
    entries = _grouped_entries(groups=7, variants=6)
    constraint = _constraint(entries, minimum=16)
    for adapter in (
        UniformProgramSearchAdapter(
            entries=entries, seen_exact_identities=(), seed=127
        ),
        HybridTPEProgramSearchAdapter(
            entries=entries,
            seen_exact_identities=(),
            seed=131,
            n_startup_trials=2,
            n_ei_candidates=8,
        ),
        StructuredSurrogateProgramSearchAdapter(
            entries=entries,
            seen_exact_identities=(),
            seed=137,
            cold_start_asks=4,
            candidate_pool_size=len(entries),
            n_estimators=16,
            min_samples_leaf=1,
        ),
    ):
        asked = adapter.ask(
            checkpoint_id="checkpoint_insufficient_distinct_supply",
            count=8,
            required_program_template_id="BASE_TEMPORAL",
            eligible_exact_identities=[entry.exact_identity for entry in entries],
            batch_group_constraint=constraint,
        )
        assert len(asked) == 7


def test_tpe_projects_raw_newly_infeasible_group_without_reward_or_fallback(
    monkeypatch,
) -> None:
    entries = _grouped_entries(groups=4, variants=2)
    adapter = HybridTPEProgramSearchAdapter(
        entries=entries,
        seen_exact_identities=(),
        seed=139,
        n_startup_trials=2,
        n_ei_candidates=8,
    )
    group_zero = [entry for entry in entries if entry.genes["base_group"] == "group_0"]
    raw_trials = iter(
        [
            {
                "proposal_id": "raw-0",
                "trial_number": 0,
                "genes": {"program_exact_identity": group_zero[0].exact_identity},
            },
            {
                "proposal_id": "raw-1",
                "trial_number": 1,
                "genes": {"program_exact_identity": group_zero[1].exact_identity},
            },
        ]
    )
    monkeypatch.setattr(adapter.tpe, "ask_trial", lambda **_: next(raw_trials))
    monkeypatch.setattr(
        adapter.tpe,
        "enqueue_fixed_trial",
        lambda **kwargs: {
            "proposal_id": "projected-1",
            "trial_number": 101,
            "genes": dict(kwargs["genes"]),
        },
    )
    monkeypatch.setattr(adapter.tpe, "annotate_pending_trial", lambda *_: None)
    constraint = _constraint(entries, minimum=4)
    asked = adapter.ask(
        checkpoint_id="checkpoint_tpe_batch_projection",
        count=2,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=[entry.exact_identity for entry in entries],
        batch_group_constraint=constraint,
    )
    mapping = constraint["group_by_exact_identity"]
    assert mapping[asked[0]["exact_identity"]] == "group_0"
    assert mapping[asked[1]["exact_identity"]] != "group_0"
    projection = asked[1]["acquisition"]["projection"]
    assert projection["availability_replacement_reason"] == (
        "BASE_GROUP_DIVERSITY_OR_CAPACITY_CONSTRAINT"
    )
    assert projection["global_fallback"] is False
    assert adapter._tpe_internal_observations["raw-1"]["optimizer_reward"] is None
    assert adapter.projection_statistics()["global_fallback_count"] == 0


def test_surrogate_scores_full_eligible_once_then_walks_fixed_order(
    monkeypatch,
) -> None:
    entries = _grouped_entries(groups=4, variants=3)
    adapter = StructuredSurrogateProgramSearchAdapter(
        entries=entries,
        seen_exact_identities=(),
        seed=149,
        cold_start_asks=4,
        candidate_pool_size=len(entries),
        n_estimators=16,
        min_samples_leaf=1,
    )
    captured: list[str] = []

    def fixed_scores(scored_entries):
        captured.extend(entry.exact_identity for entry in scored_entries)
        return [
            {
                "entry": entry,
                "cold_start": False,
                "acquisition": float(len(scored_entries) - index),
            }
            for index, entry in enumerate(scored_entries)
        ]

    monkeypatch.setattr(adapter, "_acquisition_rows", fixed_scores)
    constraint = _constraint(entries, minimum=4, maximum=4)
    asked = adapter.ask(
        checkpoint_id="checkpoint_surrogate_batch_filter",
        count=4,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=[entry.exact_identity for entry in entries],
        batch_group_constraint=constraint,
    )
    assert set(captured) == {entry.exact_identity for entry in entries}
    assert len(captured) == len(entries)
    assert len({constraint["group_by_exact_identity"][row["exact_identity"]] for row in asked}) == 4
    assert all(
        row["batch_group_feasibility"]["initial_eligible_scored_count"]
        == len(entries)
        for row in asked
    )
    assert [row["acquisition"]["acquisition"] for row in asked] == [
        float(len(entries) - captured.index(row["exact_identity"]))
        for row in asked
    ]


def test_batch_receipt_rejects_same_batch_capacity_violation() -> None:
    entries = _grouped_entries(groups=1, variants=6)
    constraint = _constraint(entries, minimum=0, maximum=4)
    with pytest.raises(RuntimeError, match="PROGRAM_BATCH_GROUP_CONSTRAINT_VIOLATION"):
        verify_program_batch_group_selection_v1(
            [entry.exact_identity for entry in entries[:5]],
            constraint=constraint,
        )


def test_batch_receipt_count_distribution_matches_sequential_replay() -> None:
    entries = _grouped_entries(groups=4, variants=2)
    constraint = _constraint(entries, minimum=4)
    selected = [entries[index * 2].exact_identity for index in range(4)]
    receipt = verify_program_batch_group_selection_v1(
        selected, constraint=constraint
    )
    assert Counter(receipt["group_counts_after"].values()) == Counter({1: 4})


def test_tournament_short_batch_fails_before_schedule_or_evaluator(
    monkeypatch,
) -> None:
    entries = _grouped_entries(groups=7, variants=1)

    class ShortBandit:
        entries_by_arm = {UNIFORM_CONTROL: entries}

        @staticmethod
        def ask(**kwargs):
            return [
                {"exact_identity": entry.exact_identity}
                for entry in entries[: int(kwargs["count"]) - 1]
            ]

    monkeypatch.setattr(
        tournament_runner,
        "_eligible_entries",
        lambda *_, **__: ([], [entry.exact_identity for entry in entries]),
    )
    monkeypatch.setattr(
        tournament_runner.engine,
        "_schedule_record",
        lambda *_, **__: pytest.fail("schedule/evaluator boundary was reached"),
    )
    asks = [
        {
            "generation_arm": UNIFORM_CONTROL,
            "template_id": "BASE_TEMPORAL",
            "checkpoint_ordinal": 0,
        }
        for _ in range(8)
    ]
    with pytest.raises(
        RuntimeError, match="PROGRAM_TOURNAMENT_CHECKPOINT_SUPPLY_EXHAUSTED"
    ):
        tournament_runner._select_checkpoint(
            asks,
            catalog={},
            bandit=ShortBandit(),
            state={"base_counts": Counter()},
            components_by_id={},
            adapter=None,
            compiler=None,
        )


def test_phase_freeze_supplies_dynamic_stage2_group_thresholds(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        tournament_runner, "verify_phase_freeze_v1", lambda _: {"status": "PASS"}
    )

    def read_json(path: Path):
        if path.name == "phase_c_run_contract.json":
            return {
                "main_record_count": 64,
                "checkpoint_count": 8,
                "minimum_base_identities_per_template": 8,
                "maximum_variants_per_base_per_template": 4,
            }
        if path.name == "prior_program_exact_identities.json":
            return {"exact_identities": []}
        raise AssertionError(path)

    monkeypatch.setattr(tournament_runner.engine, "_read_json", read_json)
    tournament_runner._verify_freeze(tmp_path)
    assert tournament_runner.engine.MIN_BASE_IDENTITIES_PER_TEMPLATE == 8
    assert tournament_runner.engine.MAX_VARIANTS_PER_BASE_PER_TEMPLATE == 4


def test_root_gate_reports_every_failing_enhanced_template(monkeypatch) -> None:
    monkeypatch.setattr(phase_c_runner, "MIN_BASE_IDENTITIES_PER_TEMPLATE", 16)
    monkeypatch.setattr(phase_c_runner, "MAX_VARIANTS_PER_BASE_PER_TEMPLATE", 4)
    schedules = []
    first, second = phase_c_runner.ENHANCED_TEMPLATE_ORDER[:2]
    for template_id in phase_c_runner.ENHANCED_TEMPLATE_ORDER:
        group_ids = [f"{template_id}-base-{index}" for index in range(16)]
        if template_id == first:
            group_ids[-1] = group_ids[0]
        if template_id == second:
            group_ids.extend([group_ids[0]] * 4)
        schedules.extend(
            {
                "template_id": template_id,
                "components": {"base": {"component_id": group_id}},
            }
            for group_id in group_ids
        )
    summary = phase_c_runner._base_diversity_summary(schedules)
    assert summary[first]["minimum_distinct_groups_pass"] is False
    assert summary[second]["maximum_per_group_pass"] is False
    with pytest.raises(RuntimeError) as failure:
        phase_c_runner._verify_base_diversity_root_gate(schedules)
    assert first in str(failure.value)
    assert second in str(failure.value)


def test_real_space_rehearsal_covers_every_arm_by_enhanced_template() -> None:
    matrix = rehearsal._first_enhanced_checkpoint_by_arm_template(stage01_asks_v1())
    assert set(matrix) == set(PROGRAM_OPTIMIZER_ARMS)
    for arm_rows in matrix.values():
        assert set(arm_rows) == set(phase_c_runner.ENHANCED_TEMPLATE_ORDER)
        assert all(len(checkpoint) == 8 for checkpoint in arm_rows.values())


def test_rehearsal_tpe_statistics_come_from_projection_receipts() -> None:
    projections = [
        {
            "raw_legality": "LEGAL_FROZEN_EXACT",
            "availability_replacement_applied": False,
            "availability_replacement_reason": None,
            "same_bucket": True,
            "intent_preserved": True,
            "global_fallback": False,
        },
        {
            "raw_legality": "LEGAL_FROZEN_EXACT",
            "availability_replacement_applied": True,
            "availability_replacement_reason": (
                "BASE_GROUP_DIVERSITY_OR_CAPACITY_CONSTRAINT"
            ),
            "same_bucket": False,
            "intent_preserved": True,
            "global_fallback": False,
        },
    ]
    stats = rehearsal._tpe_projection_statistics_v1(projections)
    assert stats["tpe_raw_ask_count"] == 2
    assert stats["direct_exact_hit_count"] == 1
    assert stats["legal_projection_count"] == 1
    assert stats["batch_group_constraint_replacement_count"] == 1
    assert stats["global_fallback_count"] == 0
    assert stats["legal_projection_rate"] == 0.5
