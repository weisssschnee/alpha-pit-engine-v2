from __future__ import annotations

from collections import Counter
from pathlib import Path

from scripts import run_cn_joint_program_allocator_repair_canary_v0 as runner
from our_system_phase2.runtime.cn_joint_program_allocator_repair_canary_v0 import (
    DECISION_GATES,
    ENHANCED_TEMPLATE_ORDER,
    EXPECTED_RECORDS,
    TEMPLATE_ORDER,
    build_ask_plan_v0,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    PROGRAM_JOIN_POLICY_ID,
    PROGRAM_PROPOSAL_ADAPTER_VERSION,
    ProgramProposalReceiptV0,
)
from our_system_phase2.services.program_allocator_repair_bandit_v0 import (
    ProgramAllocatorRepairBanditV0,
    allocator_outcome_from_record_v0,
)


def _receipt(index: int) -> ProgramProposalReceiptV0:
    base_id = f"base-{index}"
    temporal_id = f"temporal-{index}"
    return ProgramProposalReceiptV0(
        program_template_id="BASE_TEMPORAL",
        adapter_version=PROGRAM_PROPOSAL_ADAPTER_VERSION,
        join_policy_id=PROGRAM_JOIN_POLICY_ID,
        batch_id="allocator-repair-test",
        ask_ordinal=index,
        generation_arm="UNIFORM_FRESH",
        component_candidate_ids={"base": base_id, "temporal": temporal_id},
        component_control_ids={
            "base": f"{base_id}-control",
            "temporal": f"{temporal_id}-control",
        },
        component_pair_ids={"base": f"pair-{base_id}", "temporal": f"pair-{temporal_id}"},
        component_route_ids={
            "base": "SLOW_CROSS_SECTIONAL_LEVEL",
            "temporal": "SLOW_TEMPORAL_CHANGE",
        },
        component_proposal_ids={"base": f"proposal-{base_id}", "temporal": f"proposal-{temporal_id}"},
        component_trial_numbers={"base": None, "temporal": index},
        component_sampling_phases={"base": "AVAILABILITY_FIXED", "temporal": "TPE_GUIDED"},
        component_generation_receipt_hashes={"base": f"{index + 1:064x}", "temporal": f"{index + 2:064x}"},
        component_credit_eligible={"base": True, "temporal": True},
        combination_policy={
            "temporal": "ADD",
            "market": "FILTER",
            "event_episode": "SOURCE_ROUTE_EPISODE",
            "event_application": "FILTER",
        },
        semantic_program_hash=f"semantic-{index}",
        program_id=f"program-{index}",
    )


def _record(
    *,
    reward: float = 0.2,
    cumulative_return: float = 0.1,
    turnover_efficiency: float = 0.5,
    matched_reward: float = 0.1,
    matched_return: float = 0.05,
    blocked: bool = False,
) -> dict:
    if blocked:
        return {
            "replay_status": "PAIR_REPLAY_BLOCKED",
            "blockers": ["CORPORATE_ACTION_FRACTIONAL_SHARES"],
            "primary": None,
            "base_control": None,
            "matched_net_reward_increment": None,
            "matched_cumulative_return_increment": None,
        }
    return {
        "replay_status": "PAIR_REPLAY_COMPLETE",
        "blockers": [],
        "primary": {
            "continuous_book_net_reward": reward,
            "cumulative_net_return": cumulative_return,
            "net_return_per_turnover": turnover_efficiency,
            "development_subwindows": [
                {"window_id": "w1", "cumulative_net_return": 0.02},
                {"window_id": "w2", "cumulative_net_return": 0.01},
                {"window_id": "w3", "cumulative_net_return": -0.01},
            ],
        },
        "base_control": {},
        "matched_net_reward_increment": matched_reward,
        "matched_cumulative_return_increment": matched_return,
    }


def test_allocator_repair_plan_is_exact_fixed_256_with_per_checkpoint_floors() -> None:
    rows = build_ask_plan_v0()
    assert len(rows) == EXPECTED_RECORDS == 256
    assert [row["main_record_ordinal"] for row in rows] == list(range(256))
    assert Counter(row["template_id"] for row in rows) == Counter(
        {template_id: 32 for template_id in TEMPLATE_ORDER}
    )
    for template_id in ENHANCED_TEMPLATE_ORDER:
        template_rows = [row for row in rows if row["template_id"] == template_id]
        assert Counter(row["generation_arm"] for row in template_rows) == Counter(
            {"UNIFORM_FRESH": 12, "REVISED_EXPLOIT": 12, "NOVELTY_RESERVE": 8}
        )
        for offset in range(0, 32, 8):
            assert Counter(
                row["generation_arm"] for row in template_rows[offset : offset + 8]
            ) == Counter(
                {"UNIFORM_FRESH": 3, "REVISED_EXPLOIT": 3, "NOVELTY_RESERVE": 2}
            )
        assert all(not row["adaptive_budget_reallocation_allowed"] for row in template_rows)


def test_blocker_is_zero_credit_risk_observation_and_roundtrips() -> None:
    bandit = ProgramAllocatorRepairBanditV0("test")
    outcome = bandit.observe_record(_receipt(0), _record(blocked=True))
    assert set(outcome.values()) == {0.0}
    restored = ProgramAllocatorRepairBanditV0.restore(bandit.snapshot())
    assert restored.observations == 1
    score = restored.score_receipt(_receipt(0))
    assert score["allocator_signal_available"] is False


def test_absolute_first_hierarchy_beats_matched_only_credit() -> None:
    bandit = ProgramAllocatorRepairBanditV0("test")
    absolute = _receipt(1)
    matched_only = _receipt(2)
    for _ in range(4):
        bandit.observe_record(
            absolute,
            _record(
                reward=0.1,
                cumulative_return=0.05,
                turnover_efficiency=0.2,
                matched_reward=0.05,
                matched_return=0.02,
            ),
        )
        bandit.observe_record(
            matched_only,
            _record(
                reward=-0.1,
                cumulative_return=-0.05,
                turnover_efficiency=-0.2,
                matched_reward=10.0,
                matched_return=10.0,
            ),
        )
    assert tuple(bandit.score_receipt(absolute)["selection_rank"]) > tuple(
        bandit.score_receipt(matched_only)["selection_rank"]
    )


def test_allocator_outcome_requires_exact_three_development_windows() -> None:
    row = _record()
    row["primary"]["development_subwindows"] = row["primary"][
        "development_subwindows"
    ][:2]
    assert allocator_outcome_from_record_v0(row)["stable_two_of_three"] == 0.0


def test_runner_contract_configuration_declares_256_shape() -> None:
    assert runner.EXPECTED_RECORDS == 256
    assert runner.CHECKPOINT_COUNT == 32
    assert runner.MIN_BASE_IDENTITIES_PER_TEMPLATE == 16


def test_freeze_decision_gates_are_numeric_and_unambiguous() -> None:
    assert DECISION_GATES == {
        "productive_rate_delta_vs_uniform_minimum": 0.05,
        "all_four_positive_rate_delta_vs_uniform_minimum": 1e-12,
        "primary_reward_positive_rate_delta_vs_uniform_minimum": -0.02,
        "primary_return_positive_rate_delta_vs_uniform_minimum": -0.02,
        "three_window_positive_rate_delta_vs_uniform_minimum": 0.0,
        "median_return_per_turnover_delta_vs_uniform_minimum": 0.0,
        "blocked_rate_delta_vs_uniform_maximum": 0.0,
        "minimum_improved_enhanced_templates": 4,
        "enhanced_template_improvement_metric": "all_four_positive_rate",
        "enhanced_template_blocked_rate_worsening_allowed": False,
    }


def test_launcher_uses_reused_engine_bootstrap_allowlisted_log_names() -> None:
    launcher = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_cn_joint_program_allocator_repair_canary_77o.ps1"
    ).read_text(encoding="utf-8")
    assert "joint_program_phase_c.stdout.log" in launcher
    assert "joint_program_phase_c.stderr.log" in launcher
    assert "allocator_repair_canary.stdout.log" not in launcher
