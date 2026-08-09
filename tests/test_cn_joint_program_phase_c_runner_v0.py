from __future__ import annotations

from collections import Counter
import inspect
from pathlib import Path

import pytest

from scripts import run_cn_joint_program_phase_c_v0 as runner
from our_system_phase2.services.candidate_program_proposal_v0 import (
    PROGRAM_JOIN_POLICY_ID,
    PROGRAM_PROPOSAL_ADAPTER_VERSION,
    ProgramProposalReceiptV0,
)
from our_system_phase2.services.program_factorized_bandit_v0 import (
    ProgramFactorizedBanditV0,
)


def _receipt(index: int, *, base: int | None = None) -> ProgramProposalReceiptV0:
    base_id = f"base-{index if base is None else base}"
    temporal_id = f"temporal-{index}"
    return ProgramProposalReceiptV0(
        program_template_id="BASE_TEMPORAL",
        adapter_version=PROGRAM_PROPOSAL_ADAPTER_VERSION,
        join_policy_id=PROGRAM_JOIN_POLICY_ID,
        batch_id="test-phase-c",
        ask_ordinal=0,
        generation_arm="UNIFORM_FRESH",
        component_candidate_ids={"base": base_id, "temporal": temporal_id},
        component_control_ids={
            "base": f"{base_id}-control",
            "temporal": f"{temporal_id}-control",
        },
        component_pair_ids={
            "base": f"pair-{base_id}",
            "temporal": f"pair-{temporal_id}",
        },
        component_route_ids={
            "base": "SLOW_CROSS_SECTIONAL_LEVEL",
            "temporal": "SLOW_TEMPORAL_CHANGE",
        },
        component_proposal_ids={
            "base": f"proposal-{base_id}",
            "temporal": f"proposal-{temporal_id}",
        },
        component_trial_numbers={"base": None, "temporal": index},
        component_sampling_phases={
            "base": "AVAILABILITY_FIXED",
            "temporal": "TPE_GUIDED",
        },
        component_generation_receipt_hashes={
            "base": f"{index + 1:064x}",
            "temporal": f"{index + 2:064x}",
        },
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


def _entry(index: int, *, base: int | None = None) -> dict:
    receipt = _receipt(index, base=base)
    base_id = f"base-{index if base is None else base}"
    return {
        "status": "EXECUTABLE",
        "template_id": "BASE_TEMPORAL",
        "program_id": f"program-{index}",
        "base_component_id": base_id,
        "component_ids": tuple(receipt.component_candidate_ids.values()),
        "combination_id": f"combination-{index}",
        "score_receipt": receipt.to_record(),
        "reservoir": {
            "template_reservoir_ordinal": index,
            "reservoir_record_sha256": f"{index + 10:064x}",
            "raw_combination_sha256": f"{index + 20:064x}",
        },
    }


def _ask(ordinal: int, arm: str) -> dict:
    return {
        "main_record_ordinal": 64 + ordinal,
        "template_id": "BASE_TEMPORAL",
        "template_record_ordinal": ordinal,
        "generation_arm": arm,
        "ask_record_sha256": f"{ordinal + 30:064x}",
    }


def test_phase_c_recycles_pool_then_updates_and_seals_checkpoint() -> None:
    source = inspect.getsource(runner.run)
    checkpoint_loop = source.index(
        "for checkpoint_index in range(closed_checkpoints, CHECKPOINT_COUNT):"
    )
    process_pool = source.index("with ProcessPoolExecutor(", checkpoint_loop)
    resource_gate = source.index(
        "if available < MINIMUM_FREE_MEMORY_BYTES:", process_pool
    )
    feedback = source.index("feedback = _feedback_update(", resource_gate)
    close = source.index("previous_manifest = _close_checkpoint(", feedback)
    assert checkpoint_loop < process_pool < resource_gate < feedback < close
    assert '"executor_lifecycle": "CHECKPOINT_SCOPED_RECYCLE"' in source


def test_phase_c_first_16_uniform_asks_force_distinct_bases() -> None:
    bandit = ProgramFactorizedBanditV0("test-campaign")
    state = runner._selection_state()
    entries = [_entry(index) for index in range(20)]
    selected = []
    for ordinal in range(16):
        entry, _ = runner._choose_entry(
            _ask(ordinal, "UNIFORM_FRESH"),
            entries,
            bandit=bandit,
            state=state,
        )
        selected.append(entry["base_component_id"])
    assert len(set(selected)) == 16


def test_phase_c_base_parity_allows_frozen_program_repeat_on_distinct_reservoir_rows() -> None:
    bandit = ProgramFactorizedBanditV0("test-campaign")
    state = runner._selection_state()
    receipt = _receipt(0)
    entries = []
    for index in range(2):
        entries.append(
            {
                "status": "EXECUTABLE",
                "template_id": "BASE",
                "program_id": "same-base-program",
                "base_component_id": "same-base-component",
                "component_ids": ("same-base",),
                "combination_id": "same-combination",
                "score_receipt": receipt.to_record(),
                "reservoir": {
                    "template_reservoir_ordinal": index,
                    "reservoir_record_sha256": f"{index + 100:064x}",
                    "raw_combination_sha256": f"{index + 200:064x}",
                },
            }
        )
    asks = [
        {
            "main_record_ordinal": index,
            "template_id": "BASE",
            "template_record_ordinal": index,
            "generation_arm": "UNIFORM_FRESH",
            "ask_record_sha256": f"{index + 300:064x}",
        }
        for index in range(2)
    ]
    first, _ = runner._choose_entry(asks[0], entries, bandit=bandit, state=state)
    second, _ = runner._choose_entry(asks[1], entries, bandit=bandit, state=state)
    assert first["program_id"] == second["program_id"]
    assert (
        first["reservoir"]["reservoir_record_sha256"]
        != second["reservoir"]["reservoir_record_sha256"]
    )


def test_phase_c_factorized_exploit_prefers_supported_positive_entry() -> None:
    bandit = ProgramFactorizedBanditV0("test-campaign")
    entries = [_entry(index) for index in range(3)]
    target_receipt = ProgramProposalReceiptV0.from_record(
        entries[2]["score_receipt"]
    )
    for _ in range(4):
        bandit.observe(target_receipt, matched_increment=0.25)
    selected, decision = runner._choose_entry(
        _ask(16, "FACTORIZED_EXPLOIT"),
        entries,
        bandit=bandit,
        state=runner._selection_state(),
    )
    assert selected["program_id"] == "program-2"
    assert decision["adaptive_template_credit_used"] is True
    assert decision["selection_metrics"]["factorized_signal_available"] is True


def _result(ordinal: int, *, blocked: bool) -> dict:
    return {
        "main_record_ordinal": ordinal,
        "template_id": "BASE_TEMPORAL",
        "generation_arm": "UNIFORM_FRESH",
        "record_payload_sha256": f"{ordinal + 40:064x}",
        "replay_status": "PAIR_REPLAY_BLOCKED" if blocked else "PAIR_REPLAY_COMPLETE",
        "blockers": ["CORPORATE_ACTION_FRACTIONAL_SHARES"] if blocked else [],
        "matched_net_reward_increment": None if blocked else 0.1,
        "primary": (
            None
            if blocked
            else {
                "behavior_identity": "behavior-a",
                "mean_one_way_turnover": 0.2,
                "development_subwindows": [
                    {"window_id": "w1", "cumulative_net_return": 0.1}
                ],
            }
        ),
        "base_control": (
            None
            if blocked
            else {
                "mean_one_way_turnover": 0.1,
                "development_subwindows": [
                    {"window_id": "w1", "cumulative_net_return": 0.0}
                ],
            }
        ),
    }


def test_phase_c_feedback_updates_complete_rows_only() -> None:
    schedules = [
        {
            "main_record_ordinal": ordinal,
            "proposal_receipt": _receipt(ordinal).to_record(),
        }
        for ordinal in range(2)
    ]
    bandit = ProgramFactorizedBanditV0("test-campaign")
    ledger = runner._feedback_update(
        [_result(0, blocked=False), _result(1, blocked=True)],
        schedules,
        bandit=bandit,
        behavior_counts=Counter(),
    )
    assert bandit.observations == 1
    assert ledger[0]["bandit_update_applied"] is True
    assert ledger[1]["bandit_update_applied"] is False
    assert ledger[1]["reason"] == "REPLAY_BLOCKED_NO_UPDATE"


def test_phase_c_worker_relabels_phase_b_payload_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake(record, target):
        payload = runner._self_hashed(
            {
                "schema_version": "cn_joint_program_phase_b_record_v0",
                "status": "JOINT_PROGRAM_PHASE_B_RECORD_CLOSED_IMMUTABLE",
                "main_record_ordinal": 64,
                "template_id": "BASE_TEMPORAL",
                "input_binding_sha256": "a" * 64,
                "schedule_record_sha256": "b" * 64,
                "generation_arm": "UNIFORM_FRESH",
            },
            "record_payload_sha256",
        )
        runner._write_json(Path(target), payload)
        return payload

    monkeypatch.setattr(runner.phase_b, "_evaluate_record", fake)
    target = tmp_path / "record_0064.json"
    payload = runner._evaluate_record(
        {
            "main_record_ordinal": 64,
            "template_id": "BASE_TEMPORAL",
            "generation_arm": "FACTORIZED_EXPLOIT",
            "adaptive_template_credit_used": True,
            "ask_record_sha256": "c" * 64,
            "reservoir_record_sha256": "d" * 64,
            "raw_combination_sha256": "e" * 64,
            "selection_decision_sha256": "f" * 64,
        },
        str(target),
    )
    assert payload["status"] == "JOINT_PROGRAM_PHASE_C_RECORD_CLOSED_IMMUTABLE"
    assert payload["generation_arm"] == "FACTORIZED_EXPLOIT"
    assert payload["adaptive_template_credit_used"] is True
    assert target.is_file()
    assert not target.with_suffix(".phase_b.tmp.json").exists()
