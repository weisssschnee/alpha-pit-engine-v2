from __future__ import annotations

import csv
import hashlib
import json

import numpy as np
import pytest

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    _candidate_summary_from_reward_atoms,
)
from our_system_phase2.services.phase3cm_streaming_checkpoint import (
    CheckpointDriftError,
    StreamingCheckpointPayload,
    load_checkpoint,
    write_checkpoint,
)
from our_system_phase2.services.phase3cm_streaming_dag import SharedMultiCandidateDAGPlan
from our_system_phase2.services.phase3cm_streaming_portfolio import BatchedPortfolioKernel
from our_system_phase2.services.phase3cm_streaming_reducer import StreamingPortfolioReducer
from scripts.run_cn_phase3cm_streaming_qualification import (
    _candidate_finalization_seed_map,
    _candidate_pairs,
    _order_candidate_pairs,
    _phase_e_plan_pair_order,
    _portfolio_continuation_payload,
    _read_csv,
    _restore_portfolio_continuation_payload,
    _verify_selected_candidate_semantics,
)


def _inputs() -> tuple[list[dict[str, object]], dict[str, object]]:
    candidates: list[dict[str, object]] = []
    members: list[dict[str, object]] = []
    for role, candidate_id, expression in (
        ("PRIMARY", "primary", "CSRank($x)"),
        ("CONTROL", "control", "$x"),
    ):
        row = {
            "candidate_id": candidate_id,
            "pair_id": "pair.1",
            "pair_member_role": role,
            "expression": expression,
            "canonical_expression": expression,
            "route_id": "STATIC_CROSS_SECTIONAL",
            "field_ids": '["x"]',
        }
        candidates.append(row)
        members.append(
            {
                **row,
                "field_ids": ["x"],
                "clock_namespace": "active_bar",
            }
        )
    binding = {
        "candidate_members": members,
        "pairs": [
            {
                "pair_id": "pair.1",
                "candidate_id": "primary",
                "control_candidate_id": "control",
                "clock_namespace": "active_bar",
            }
        ],
    }
    return candidates, binding


def test_selected_candidate_semantics_match_binding_exactly() -> None:
    candidates, binding = _inputs()
    _verify_selected_candidate_semantics(
        candidates,
        binding=binding,
        clock="active_bar",
    )


def test_selected_candidate_semantics_reject_same_id_with_changed_expression() -> None:
    candidates, binding = _inputs()
    candidates[0]["expression"] = "CSRank($future_x)"
    with pytest.raises(RuntimeError, match="candidate semantic drift primary:expression"):
        _verify_selected_candidate_semantics(
            candidates,
            binding=binding,
            clock="active_bar",
        )


def test_selected_candidate_semantics_reject_pair_member_swap() -> None:
    candidates, binding = _inputs()
    candidates[0]["pair_member_role"] = "CONTROL"
    with pytest.raises(RuntimeError, match="pair_member_role"):
        _verify_selected_candidate_semantics(
            candidates,
            binding=binding,
            clock="active_bar",
        )


def _three_pairs() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for pair_id, field in (("pair.1", "x"), ("pair.2", "y"), ("pair.3", "z")):
        for role, expression in (
            ("PRIMARY", f"CSRank(Persistence(${field},5))"),
            ("CONTROL", f"Persistence(${field},5)"),
        ):
            rows.append(
                {
                    "candidate_id": f"{pair_id}.{role.lower()}",
                    "pair_id": pair_id,
                    "pair_member_role": role,
                    "expression": expression,
                    "canonical_expression": expression,
                    "route_id": "STATIC_CROSS_SECTIONAL",
                    "field_ids": [field],
                    "clock_namespace": "active_bar",
                }
            )
    return rows


def test_phase_e_shuffled_plan_order_preserves_semantics_and_checkpoint_identity(
    tmp_path,
) -> None:
    candidates = _three_pairs()
    ordered_pair_ids = _phase_e_plan_pair_order(
        pair_batches=(("pair.3", "pair.1"), ("pair.2",)),
        candidate_pair_ids=("pair.1", "pair.2", "pair.3"),
        maximum_batch_size=2,
    )
    ordered = _order_candidate_pairs(candidates, ordered_pair_ids)
    assert [ordered[index]["pair_id"] for index in range(0, len(ordered), 2)] == [
        "pair.3",
        "pair.1",
        "pair.2",
    ]
    assert [ordered[index]["pair_member_role"] for index in range(len(ordered))] == [
        "PRIMARY",
        "CONTROL",
    ] * 3
    semantic = lambda rows: sorted(
        (
            str(row["candidate_id"]),
            str(row["pair_id"]),
            str(row["pair_member_role"]),
            str(row["canonical_expression"]),
        )
        for row in rows
    )
    assert semantic(ordered) == semantic(candidates)
    finalization_seeds = _candidate_finalization_seed_map(candidates)
    assert list(finalization_seeds.values()) == list(
        range(20260623, 20260623 + len(candidates))
    )
    assert {
        str(row["candidate_id"]): finalization_seeds[str(row["candidate_id"])]
        for row in ordered
    } == finalization_seeds
    assert _candidate_finalization_seed_map(ordered) != finalization_seeds
    assert finalization_seeds["pair.1.primary"] == 20260623
    assert finalization_seeds["pair.1.control"] == 20260624
    assert SharedMultiCandidateDAGPlan.build(ordered).plan_hash == (
        SharedMultiCandidateDAGPlan.build(candidates).plan_hash
    )

    plan_hash = hashlib.sha256(
        json.dumps(list(ordered_pair_ids), separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    payload = StreamingCheckpointPayload(
        execution_plan_hash=plan_hash,
        input_binding_hash="binding",
        backend_identity="active_bar",
        route_cohort="synthetic",
        execution_position={"pair_order": list(ordered_pair_ids)},
        completed_blocks=[0],
        completed_pair_batches=["pair.3|pair.1", "pair.2"],
        temporal_continuation_payload={},
        state_event_continuation_payload={},
        portfolio_continuation_payload={},
        streaming_reducer_payload={},
    )
    record = write_checkpoint(tmp_path, payload, checkpoint_ordinal=1)
    restored = load_checkpoint(
        record["checkpoint_path"],
        expected_execution_plan_hash=plan_hash,
        expected_input_binding_hash="binding",
    )
    assert restored.execution_position["pair_order"] == list(ordered_pair_ids)
    with pytest.raises(CheckpointDriftError, match="execution-plan hash drift"):
        load_checkpoint(
            record["checkpoint_path"],
            expected_execution_plan_hash="different",
            expected_input_binding_hash="binding",
        )


def test_phase_e_csv_order_a_plan_order_b_runtime_reward_and_resume_are_exact(
    tmp_path,
) -> None:
    source_rows = _three_pairs()[:4]
    for row in source_rows:
        row["expression_hash"] = hashlib.sha256(
            str(row["canonical_expression"]).encode("utf-8")
        ).hexdigest()
        row["open_direction"] = "long_top"
        row["field_ids"] = json.dumps(row["field_ids"])
    table = tmp_path / "candidates.csv"
    with table.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(source_rows[0]))
        writer.writeheader()
        writer.writerows(source_rows)
    loaded = _candidate_pairs(
        _read_csv(table),
        pair_limit=2,
        clock="active_bar",
        require_exact_count=True,
    )
    frozen_seeds = _candidate_finalization_seed_map(loaded)
    planned_pair_ids = _phase_e_plan_pair_order(
        pair_batches=(("pair.2",), ("pair.1",)),
        candidate_pair_ids=("pair.1", "pair.2"),
        maximum_batch_size=1,
    )
    planned = _order_candidate_pairs(loaded, planned_pair_ids)
    assert SharedMultiCandidateDAGPlan.build(loaded).plan_hash == (
        SharedMultiCandidateDAGPlan.build(planned).plan_hash
    )

    signal_by_id: dict[str, np.ndarray] = {}
    time_ids = np.repeat(np.arange(4, dtype=np.int64), 6)
    code_ids = np.tile(np.arange(6, dtype=np.int32), 4)
    base = code_ids.astype(np.float64) + 0.25 * time_ids
    for index, candidate in enumerate(loaded):
        sign = 1.0 if str(candidate["pair_member_role"]) == "PRIMARY" else -1.0
        signal_by_id[str(candidate["candidate_id"])] = sign * (base + 0.1 * index)

    common = {
        "code_count": 6,
        "horizons": (1,),
        "compute_threads": 1,
        "min_obs": 3,
        "top_quantile": 0.25,
        "cost_bps": 5.0,
        "portfolio_mode": "long_only_top",
    }

    def execute(
        candidates: list[dict[str, object]],
        pair_batches: tuple[tuple[str, ...], ...],
        *,
        checkpoint_root=None,
    ) -> StreamingPortfolioReducer:
        pair_index = {
            str(candidates[index]["pair_id"]): index // 2
            for index in range(0, len(candidates), 2)
        }

        def make_batches():
            batches = []
            for pair_batch in pair_batches:
                indices = tuple(
                    candidate_index
                    for pair_id in pair_batch
                    for candidate_index in (
                        2 * pair_index[pair_id],
                        2 * pair_index[pair_id] + 1,
                    )
                )
                batches.append(
                    {
                        "pair_ids": pair_batch,
                        "pair_indices": tuple(pair_index[pair_id] for pair_id in pair_batch),
                        "candidate_indices": indices,
                        "kernel": BatchedPortfolioKernel(
                            candidate_count=len(indices), **common
                        ),
                    }
                )
            return batches

        batches = make_batches()
        reducer = StreamingPortfolioReducer(candidates=candidates, horizons=(1,))
        plan_hash = hashlib.sha256(
            json.dumps(pair_batches, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        for block_index, day in enumerate(("2025-01-02", "2025-01-03")):
            labels = {
                1: (code_ids.astype(np.float64) - 2.5 + 0.1 * time_ids + block_index)
                / 100.0
            }
            for batch_index, batch in enumerate(batches):
                indices = batch["candidate_indices"]
                signals = np.vstack(
                    [signal_by_id[str(candidates[index]["candidate_id"])] for index in indices]
                )
                result = batch["kernel"].evaluate_block(
                    signals=signals,
                    labels=labels,
                    time_ids=time_ids,
                    code_ids=code_ids,
                    day_ids=np.zeros(len(code_ids), dtype=np.int32),
                    directions=np.ones(len(indices), dtype=np.float64),
                    day_count=1,
                )
                reducer.update(
                    result,
                    day_labels=(day,),
                    candidate_indices=indices,
                    complete_block=batch_index == len(batches) - 1,
                )
            if block_index == 0 and checkpoint_root is not None:
                payload = StreamingCheckpointPayload(
                    execution_plan_hash=plan_hash,
                    input_binding_hash="binding",
                    backend_identity="active_bar",
                    route_cohort="synthetic",
                    execution_position={"block_ordinal": 0},
                    completed_blocks=[0],
                    completed_pair_batches=["|".join(batch) for batch in pair_batches],
                    temporal_continuation_payload={},
                    state_event_continuation_payload={},
                    portfolio_continuation_payload=_portfolio_continuation_payload(batches),
                    streaming_reducer_payload=reducer.continuation_payload(),
                )
                record = write_checkpoint(checkpoint_root, payload, checkpoint_ordinal=1)
                restored = load_checkpoint(
                    record["checkpoint_path"],
                    expected_execution_plan_hash=plan_hash,
                    expected_input_binding_hash="binding",
                )
                reducer = StreamingPortfolioReducer(candidates=candidates, horizons=(1,))
                reducer.restore_continuation_payload(restored.streaming_reducer_payload)
                batches = make_batches()
                _restore_portfolio_continuation_payload(
                    batches, restored.portfolio_continuation_payload
                )
        return reducer

    baseline = execute(loaded, (("pair.1",), ("pair.2",)))
    resumed = execute(
        planned,
        (("pair.2",), ("pair.1",)),
        checkpoint_root=tmp_path / "checkpoint",
    )
    baseline_atoms = sorted(
        baseline.reward_atoms(),
        key=lambda row: (str(row["candidate_id"]), str(row["trade_date"])),
    )
    resumed_atoms = sorted(
        resumed.reward_atoms(),
        key=lambda row: (str(row["candidate_id"]), str(row["trade_date"])),
    )
    assert resumed_atoms == baseline_atoms

    def reward_by_id(candidates, reducer):
        atoms = reducer.reward_atoms()
        output = {}
        for candidate in candidates:
            candidate_id = str(candidate["candidate_id"])
            _, reward = _candidate_summary_from_reward_atoms(
                dict(candidate),
                [row for row in atoms if str(row["candidate_id"]) == candidate_id],
                (1,),
                seed=frozen_seeds[candidate_id],
                rank_ic_loss_weight=6.0,
                rank_ic_component_cap=0.35,
                regime_stability_weight=0.08,
                regime_component_cap=0.10,
            )
            output[candidate_id] = reward
        return output

    baseline_rewards = reward_by_id(loaded, baseline)
    resumed_rewards = reward_by_id(planned, resumed)
    for candidate_id in frozen_seeds:
        assert resumed_rewards[candidate_id]["optimizer_reward"] == (
            baseline_rewards[candidate_id]["optimizer_reward"]
        )
    pair_members = {
        "pair.1": ("pair.1.primary", "pair.1.control"),
        "pair.2": ("pair.2.primary", "pair.2.control"),
    }
    for primary_id, control_id in pair_members.values():
        assert (
            float(resumed_rewards[primary_id]["optimizer_reward"])
            - float(resumed_rewards[control_id]["optimizer_reward"])
        ) == (
            float(baseline_rewards[primary_id]["optimizer_reward"])
            - float(baseline_rewards[control_id]["optimizer_reward"])
        )


@pytest.mark.parametrize(
    ("batches", "message"),
    [
        (((), ("pair.1", "pair.2", "pair.3")), "empty pair batch"),
        ((("pair.1", "pair.1"), ("pair.2", "pair.3")), "duplicate pair identities"),
        ((("pair.1", "pair.2", "pair.3"),), "exceeds the frozen pair batch size"),
        ((("pair.1", "pair.2"),), "pair identities differ"),
    ],
)
def test_phase_e_plan_pair_order_fails_closed(batches, message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        _phase_e_plan_pair_order(
            pair_batches=batches,
            candidate_pair_ids=("pair.1", "pair.2", "pair.3"),
            maximum_batch_size=2,
        )
