from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from scripts.finalize_cn_phase3cm_current_kernel_146_parity import finalize_replay
from our_system_phase2.services.phase3cm_streaming_checkpoint import (
    StreamingCheckpointPayload,
    write_checkpoint,
)


def _digest(value) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _pair_ids(partition: int) -> list[str]:
    return [f"p{partition}.{index:03d}" for index in range(73)]


def _candidate_ids(pair_ids: list[str]) -> list[str]:
    return sorted(
        candidate_id
        for pair_id in pair_ids
        for candidate_id in (f"{pair_id}.primary", f"{pair_id}.control")
    )


def _write_run(root: Path, pair_ids: list[str], *, blocks: list[int] | None = None) -> None:
    blocks = [0, 1] if blocks is None else blocks
    root.mkdir(parents=True, exist_ok=True)
    plan_hash = _digest({"partition": root.parent.name, "pairs": pair_ids})
    binding_hash = _digest({"binding": "frozen-146"})
    pair_batches = [pair_ids[:37], pair_ids[37:]]
    plan = {
        "execution_plan_hash": plan_hash,
        "compute_threads": 11,
        "pair_batches": pair_batches,
    }
    _write_json(root / "CN_FROZEN_EXECUTION_PLAN.json", plan)
    _write_json(root / "CN_SHARED_DAG_PLAN.json", {"plan_hash": "dag"})
    _write_json(root / "CN_STREAMING_REDUCER_CONTRACT.json", {"schema_version": "reducer-v1"})

    candidate_ids = _candidate_ids(pair_ids)
    pair_results = []
    for pair_id in pair_ids:
        pair_results.append(
            {
                "pair_id": pair_id,
                "primary_candidate_id": f"{pair_id}.primary",
                "control_candidate_id": f"{pair_id}.control",
                "pair_support_identity": _digest([pair_id, "support"]),
                "pair_support_count": 100,
                "pair_support_overlap": 1.0,
                "primary_behavior_identity": _digest([pair_id, "primary", "behavior"]),
                "control_behavior_identity": _digest([pair_id, "control", "behavior"]),
                "streaming_identity_schema": "block_composable_v1",
                "pair_evaluation_status": "PAIR_EVALUATED",
                "pair_evaluation_blockers": "",
                "primary_train_reward": 0.1,
                "control_train_reward": 0.0,
                "pair_train_reward": 0.1,
            }
        )
    result = {
        "schema_version": "cn_phase3cm_streaming_backend_result_v1",
        "status": "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED",
        "backend": "active_bar",
        "phase": "E",
        "execution_plan_hash": plan_hash,
        "input_binding_hash": binding_hash,
        "blocks_processed": len(blocks),
        "pair_count": len(pair_ids),
        "candidate_count": len(candidate_ids),
        "candidate_rewards": [{"candidate_id": value} for value in candidate_ids],
        "pair_results": pair_results,
        "support_identities": {
            row["pair_id"]: {
                "support_identity": row["pair_support_identity"],
                "count": row["pair_support_count"],
            }
            for row in pair_results
        },
        "reward_atoms": {"path": "CN_STREAMING_REWARD_ATOMS.csv"},
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }
    _write_json(root / "CN_STREAMING_BACKEND_RESULT.json", result)

    columns = [
        "candidate_id",
        "expression_hash",
        "split",
        "horizon_min",
        "trade_date",
        "curve_count",
        "net_return_sum",
        "raw_return_sum",
        "turnover_sum",
        "turnover_count",
        "rank_ic_sum",
        "rank_ic_count",
        "support_count",
        "selected_count",
    ]
    with (root / "CN_STREAMING_REWARD_ATOMS.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for candidate_id in candidate_ids:
            writer.writerow(
                {
                    "candidate_id": candidate_id,
                    "expression_hash": _digest(candidate_id),
                    "split": "train",
                    "horizon_min": "1",
                    "trade_date": "2024-01-02",
                    "curve_count": 1,
                    "net_return_sum": 0.01,
                    "raw_return_sum": 0.02,
                    "turnover_sum": 0.2,
                    "turnover_count": 1,
                    "rank_ic_sum": 0.03,
                    "rank_ic_count": 1,
                    "support_count": 100,
                    "selected_count": 20,
                }
            )

    payload = StreamingCheckpointPayload(
        execution_plan_hash=plan_hash,
        input_binding_hash=binding_hash,
        backend_identity="active_bar",
        route_cohort="active_bar:73pairs",
        execution_position={"block_ordinal": blocks[-1]},
        completed_blocks=blocks,
        completed_pair_batches=[_digest(batch) for batch in pair_batches],
        temporal_continuation_payload={"rolling": [1, 2]},
        state_event_continuation_payload={"state": [3], "support": [4]},
        portfolio_continuation_payload={"portfolio": [5]},
        streaming_reducer_payload={"reducer": [6]},
    )
    write_checkpoint(
        root / "checkpoints" / "active_bar",
        payload,
        checkpoint_ordinal=1,
    )


def _qualification(reference: Path, candidate: Path) -> dict:
    return {
        "schema_version": "cn_batched_portfolio_kernel_subset_qualification_v1",
        "status": "CN_BATCHED_PORTFOLIO_KERNEL_PARTIALLY_QUALIFIED",
        "reference_root": str(reference),
        "candidate_root": str(candidate),
        "comparable": True,
        "semantic_parity_exact": True,
        "comparability": {
            "candidate_identity": True,
            "pair_identity": True,
            "required_artifacts_present": True,
            "artifact:CN_FROZEN_EXECUTION_PLAN.json": True,
            "artifact:CN_SHARED_DAG_PLAN.json": True,
        },
        "semantic_parity": {
            "result:candidate_rewards": True,
            "result:pair_results": True,
            "result:reward_atoms": True,
            "result:support_identities": True,
            "artifact:CN_STREAMING_REWARD_ATOMS.csv": True,
            "artifact:CN_STREAMING_REDUCER_CONTRACT.json": True,
        },
        "candidate_artifact_hashes": {
            "CN_FROZEN_EXECUTION_PLAN.json": "a" * 64,
            "CN_SHARED_DAG_PLAN.json": "b" * 64,
            "CN_STREAMING_REWARD_ATOMS.csv": "c" * 64,
            "CN_STREAMING_REDUCER_CONTRACT.json": "d" * 64,
        },
        "performance": {"wall_speedup": 1.90},
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }


def _checkpoint_receipt() -> dict:
    return {
        "status": "CN_PHASE3CM_SCALING_PROBE_PARITY_PASS",
        "backend": "active_bar",
        "data_role": "development_train_only",
        "parity": {
            name: {"exact": True, "mismatches": []}
            for name in ("temporal", "state", "support", "portfolio", "reducer")
        },
        "mapping_speedup": 1.8,
        "compute_speedup": 1.9,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def _fixture(tmp_path: Path) -> dict:
    partitions = []
    qualifications: dict[str, Path] = {}
    checkpoints: dict[str, Path] = {}
    for partition_index in range(2):
        partition_id = f"active_bar_p{partition_index}"
        pair_ids = _pair_ids(partition_index)
        reference = tmp_path / partition_id / "reference"
        candidate = tmp_path / partition_id / "candidate"
        _write_run(reference, pair_ids)
        _write_run(candidate, pair_ids)
        qualification_path = tmp_path / f"qualification_{partition_index}.json"
        checkpoint_path = tmp_path / f"checkpoint_{partition_index}.json"
        _write_json(qualification_path, _qualification(reference, candidate))
        _write_json(checkpoint_path, _checkpoint_receipt())
        qualifications[partition_id] = qualification_path
        checkpoints[partition_id] = checkpoint_path
        partitions.append(
            {
                "partition_id": partition_id,
                "pair_ids": pair_ids,
                "candidate_ids": _candidate_ids(pair_ids),
                "backend": "active_bar",
                "route_cohort": "active_bar:73pairs",
            }
        )
    contract = {
        "schema_version": "cn_phase3cm_current_kernel_146_parity_contract_v1",
        "pair_count": 146,
        "candidate_count": 292,
        "partitions": partitions,
    }
    contract_path = tmp_path / "contract.json"
    _write_json(contract_path, contract)
    return {
        "contract": contract_path,
        "qualifications": qualifications,
        "checkpoints": checkpoints,
    }


def _finalize(fixture: dict) -> dict:
    return finalize_replay(
        contract_path=fixture["contract"],
        qualification_paths=fixture["qualifications"],
        checkpoint_paths=fixture["checkpoints"],
    )


def test_exact_146_pair_replay_passes_without_two_x_gate(tmp_path: Path) -> None:
    result = _finalize(_fixture(tmp_path))

    assert result["status"] == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_PASS"
    assert result["identity_coverage_exact"] is True
    assert result["identity_coverage"]["observed_pair_count"] == 146
    assert result["identity_coverage"]["observed_candidate_count"] == 292
    assert result["two_x_speedup_required"] is False
    assert result["performance_threshold_gate"] == "NOT_USED"
    assert result["next_decision"] == "FREEZE_1024_RESOURCE_AND_EXECUTION_CONTRACT"
    assert all(
        row["qualification"]["performance_observed_not_gated"]["wall_speedup"] == 1.9
        for row in result["partitions"]
    )


def test_exact_checkpoint_payload_is_not_rejected_by_scaling_diagnostic(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    for path in fixture["checkpoints"].values():
        receipt = json.loads(path.read_text(encoding="utf-8"))
        receipt["status"] = "CN_PHASE3CM_SCALING_PROBE_FAIL_CLOSED"
        receipt["candidate"] = {"parallelism_not_engaged_events": 3}
        _write_json(path, receipt)

    result = _finalize(fixture)

    assert result["status"] == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_PASS"
    assert all(
        row["checkpoint"]["scaling_status_observed_not_gated"]
        == "CN_PHASE3CM_SCALING_PROBE_FAIL_CLOSED"
        for row in result["partitions"]
    )
    assert all(
        row["checkpoint"]["performance_observed_not_gated"][
            "parallelism_not_engaged_events"
        ]
        == 3
        for row in result["partitions"]
    )


def test_partition_identity_drift_fails_closed(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    path = fixture["qualifications"]["active_bar_p1"]
    receipt = json.loads(path.read_text(encoding="utf-8"))
    candidate_result_path = Path(receipt["candidate_root"]) / "CN_STREAMING_BACKEND_RESULT.json"
    candidate = json.loads(candidate_result_path.read_text(encoding="utf-8"))
    candidate["pair_results"][0]["pair_id"] = "p0.000"
    _write_json(candidate_result_path, candidate)

    result = _finalize(fixture)

    assert result["status"] == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_FAIL_CLOSED"
    assert result["next_decision"] == "STOP_BEFORE_1024_ROUTE_ASYMMETRIC_CONFIRMATION"
    assert any("identity" in error or "membership" in error for error in result["errors"])


def test_missing_checkpoint_continuation_category_fails_closed(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    path = fixture["checkpoints"]["active_bar_p0"]
    receipt = json.loads(path.read_text(encoding="utf-8"))
    del receipt["parity"]["reducer"]
    _write_json(path, receipt)

    result = _finalize(fixture)

    assert result["status"] == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_FAIL_CLOSED"
    assert any("reducer" in error for error in result["errors"])


def test_checkpoint_mismatch_fails_closed_even_when_scaling_status_passes(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    path = fixture["checkpoints"]["active_bar_p0"]
    receipt = json.loads(path.read_text(encoding="utf-8"))
    receipt["parity"]["portfolio"] = {
        "exact": False,
        "mismatches": ["portfolio.weights"],
    }
    _write_json(path, receipt)

    result = _finalize(fixture)

    assert result["status"] == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_FAIL_CLOSED"
    assert any("portfolio" in error for error in result["errors"])


def test_promotion_boundary_drift_fails_closed(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    path = fixture["qualifications"]["active_bar_p0"]
    receipt = json.loads(path.read_text(encoding="utf-8"))
    receipt["promotion"] = "ALLOWED"
    _write_json(path, receipt)

    result = _finalize(fixture)

    assert result["status"] == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_FAIL_CLOSED"
    assert any("promotion" in error for error in result["errors"])


def test_contract_required_checkpoint_field_absence_fails_closed(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    contract = json.loads(fixture["contract"].read_text(encoding="utf-8"))
    contract["partitions"][0]["checkpoint_required_fields"] = ["completed_blocks"]
    _write_json(fixture["contract"], contract)

    result = _finalize(fixture)

    assert result["status"] == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_FAIL_CLOSED"
    assert any("completed_blocks" in error for error in result["errors"])
