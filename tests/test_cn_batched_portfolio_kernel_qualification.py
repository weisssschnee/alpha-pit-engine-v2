from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scripts.qualify_cn_batched_portfolio_kernel import compare_kernel_runs


ARTIFACTS = {
    "CN_FROZEN_EXECUTION_PLAN.json": {
        "execution_plan_hash": "plan-1",
        "compute_threads": 8,
        "pair_batches": [["p1"]],
    },
    "CN_SHARED_DAG_PLAN.json": {"plan_hash": "dag-1", "nodes": ["x"]},
    "CN_STREAMING_REWARD_ATOMS.csv": "candidate_id,reward\nc1,0.1\n",
    "CN_STREAMING_REDUCER_CONTRACT.json": {"schema_version": "reducer-v1"},
}
SPLIT_MANIFEST_HASH = "a" * 64


def _result(wall_seconds: float) -> dict:
    mapping_wall = wall_seconds * 0.75
    return {
        "schema_version": "cn_phase3cm_streaming_backend_result_v1",
        "status": "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED",
        "backend": "active_bar",
        "phase": "D",
        "pair_count": 1,
        "candidate_count": 2,
        "rows_processed": 100,
        "blocks_processed": 4,
        "wall_seconds": wall_seconds,
        "telemetry_write_wall_seconds": wall_seconds / 100.0,
        "telemetry_overhead_ratio_upper_bound": 0.01,
        "peak_rss_bytes": 1234,
        "execution_plan_hash": "plan-1",
        "capacity_receipt_hash": "capacity-1",
        "max_block_rows_contract": 50,
        "max_observed_block_rows": 50,
        "block_row_guard_status": "NOT_ENFORCED_NON_PHASE_E",
        "input_binding_hash": "binding-1",
        "split_manifest_hash": SPLIT_MANIFEST_HASH,
        "data_role": "development_train_only",
        "eligible_train_date_count": 10,
        "dag_plan_hash": "dag-1",
        "coordinate_rows_retained": 0,
        "parallelism_status": "PARALLELISM_ENGAGED",
        "compute_phase_parallelism": {"cross_sectional_rank_mapping": {}},
        "hot_path_bottleneck": "BATCHED_PORTFOLIO_KERNEL_BOTTLENECK",
        "phase_timing_coverage": 1.0,
        "phase_totals": {
            "expression_value_dag": {
                "wall_seconds": wall_seconds * 0.20,
                "cpu_seconds": wall_seconds * 0.20,
            },
            "cross_sectional_rank_mapping": {
                "wall_seconds": mapping_wall,
                "cpu_seconds": mapping_wall * 6.0,
            },
            "turnover_and_cost": {
                "wall_seconds": wall_seconds * 0.05,
                "cpu_seconds": wall_seconds * 0.05,
            },
        },
        "support_identities": {"support": "same"},
        "candidate_rewards": [
            {"candidate_id": "c1", "train_reward": 0.1},
            {"candidate_id": "c0", "train_reward": 0.0},
        ],
        "pair_results": [
            {
                "pair_id": "p1",
                "primary_candidate_id": "c1",
                "control_candidate_id": "c0",
                "primary_receipt_hash": "r1",
                "control_receipt_hash": "r0",
                "pair_receipt_hash": "rp1",
                "pair_train_reward": 0.1,
            }
        ],
        "reward_atoms": {"path": "CN_STREAMING_REWARD_ATOMS.csv", "rows": 1},
        "split_rows": [{"candidate_id": "c1", "split": "train", "reward": 0.1}],
        "expression_audits": [
            {
                "block_index": 0,
                "value_node_evaluations": 1,
                "last_evaluate_wall_seconds": wall_seconds / 10.0,
                "last_evaluate_cpu_seconds": wall_seconds / 5.0,
                "last_evaluate_effective_cores": 2.0,
                "cache_current_bytes": 100,
                "cache_peak_bytes": 200,
                "cache_entry_count": 3,
            }
        ],
        "portfolio_audits": [{"mapping_wall_seconds": mapping_wall}],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }


def _write_run(root: Path, result: dict, artifacts: dict | None = None) -> None:
    root.mkdir(parents=True)
    (root / "CN_STREAMING_BACKEND_RESULT.json").write_text(
        json.dumps(result, sort_keys=True, allow_nan=True) + "\n", encoding="utf-8"
    )
    for name, value in (artifacts or ARTIFACTS).items():
        path = root / name
        if isinstance(value, str):
            path.write_text(value, encoding="utf-8")
        else:
            path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def _compare(
    tmp_path: Path,
    *,
    reference_wall: float = 20.0,
    candidate_wall: float = 8.0,
    mutate_reference=None,
    mutate_candidate=None,
    reference_artifacts: dict | None = None,
    candidate_artifacts: dict | None = None,
) -> dict:
    reference = _result(reference_wall)
    candidate = _result(candidate_wall)
    if mutate_reference:
        mutate_reference(reference)
    if mutate_candidate:
        mutate_candidate(candidate)
    reference_root = tmp_path / "reference"
    candidate_root = tmp_path / "candidate"
    _write_run(reference_root, reference, reference_artifacts)
    _write_run(candidate_root, candidate, candidate_artifacts)
    return compare_kernel_runs(reference_root, candidate_root)


def test_exact_semantics_and_twenty_over_eight_is_qualified(tmp_path: Path) -> None:
    result = _compare(tmp_path)

    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED"
    assert result["comparable"] is True
    assert result["semantic_parity_exact"] is True
    assert result["performance"]["wall_speedup"] == pytest.approx(2.5)
    assert result["performance"]["mapping_speedup"] == pytest.approx(2.5)
    assert result["performance"]["candidate_mapping_effective_cores"] == pytest.approx(6.0)
    assert result["next_decision"] == "RUN_146_ACTIVE_BAR_REPLAY"


def test_timing_and_resource_differences_are_allowed(tmp_path: Path) -> None:
    def mutate(candidate: dict) -> None:
        candidate["peak_rss_bytes"] = 999_999
        candidate["compute_phase_parallelism"] = {"different_timing_evidence": True}
        candidate["phase_timing_coverage"] = 0.5
        candidate["portfolio_audits"] = [{"mapping_wall_seconds": 123.0}]
        candidate["expression_audits"][0].update(
            {
                "last_evaluate_wall_seconds": 123.0,
                "last_evaluate_cpu_seconds": 456.0,
                "last_evaluate_effective_cores": 3.7,
                "cache_current_bytes": 10_000,
                "cache_peak_bytes": 20_000,
                "cache_entry_count": 99,
            }
        )

    result = _compare(tmp_path, mutate_candidate=mutate)

    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED"
    assert result["semantic_parity_exact"] is True


def test_expression_audit_structural_counter_drift_fails_parity(tmp_path: Path) -> None:
    def mutate(candidate: dict) -> None:
        candidate["expression_audits"][0]["value_node_evaluations"] = 2

    result = _compare(tmp_path, mutate_candidate=mutate)

    assert result["comparable"] is True
    assert result["semantic_parity"]["result:expression_audits"] is False
    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_PARITY_FAILED"


def test_parallelism_gate_fails_closed_even_with_two_x_speedup(tmp_path: Path) -> None:
    def mutate(candidate: dict) -> None:
        candidate["parallelism_status"] = "PARALLELISM_NOT_ENGAGED"
        candidate["phase_totals"]["cross_sectional_rank_mapping"]["cpu_seconds"] = 1.0

    result = _compare(tmp_path, mutate_candidate=mutate)

    assert result["semantic_parity_exact"] is True
    assert result["performance"]["mapping_parallelism_engaged"] is False
    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_NOT_QUALIFIED"


def test_semantic_change_fails_parity(tmp_path: Path) -> None:
    def mutate(candidate: dict) -> None:
        candidate["candidate_rewards"][0]["train_reward"] = 0.2

    result = _compare(tmp_path, mutate_candidate=mutate)

    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_PARITY_FAILED"
    assert result["comparable"] is True
    assert result["semantic_parity"]["result:candidate_rewards"] is False
    assert result["next_decision"] == "STOP_BEFORE_146_ACTIVE_BAR_REPLAY"


@pytest.mark.parametrize("kind", ["input", "plan", "candidate_pair"])
def test_input_plan_or_candidate_pair_drift_is_not_comparable(
    tmp_path: Path, kind: str
) -> None:
    candidate_artifacts = copy.deepcopy(ARTIFACTS)

    def mutate(candidate: dict) -> None:
        if kind == "input":
            candidate["input_binding_hash"] = "binding-drift"
        elif kind == "plan":
            candidate["execution_plan_hash"] = "plan-drift"
            candidate_artifacts["CN_FROZEN_EXECUTION_PLAN.json"] = {
                "execution_plan_hash": "plan-drift",
                "compute_threads": 4,
                "pair_batches": [["p1"]],
            }
        else:
            candidate["candidate_rewards"][0]["candidate_id"] = "different"
            candidate["pair_results"][0]["primary_candidate_id"] = "different"

    result = _compare(
        tmp_path,
        mutate_candidate=mutate,
        candidate_artifacts=candidate_artifacts,
    )

    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_SUBSET_NOT_COMPARABLE"
    assert result["comparable"] is False


def test_one_point_three_to_two_speedup_is_partial(tmp_path: Path) -> None:
    result = _compare(tmp_path, candidate_wall=12.0)

    assert result["performance"]["wall_speedup"] == pytest.approx(5.0 / 3.0)
    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_PARTIALLY_QUALIFIED"
    assert result["next_decision"] == "STOP_BEFORE_146_ACTIVE_BAR_REPLAY"


def test_less_than_one_point_three_speedup_is_not_qualified(tmp_path: Path) -> None:
    result = _compare(tmp_path, candidate_wall=16.0)

    assert result["performance"]["wall_speedup"] == pytest.approx(1.25)
    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_NOT_QUALIFIED"


def test_non_finite_values_are_stable_and_do_not_escape_output_contract(
    tmp_path: Path,
) -> None:
    def same_non_finite(result: dict) -> None:
        result["candidate_rewards"][0]["nan_diagnostic"] = float("nan")
        result["candidate_rewards"][0]["infinite_diagnostic"] = float("inf")

    result = _compare(
        tmp_path,
        mutate_reference=same_non_finite,
        mutate_candidate=same_non_finite,
    )
    json.dumps(result, allow_nan=False)
    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED"

    other_root = tmp_path / "different"

    def different_non_finite(candidate: dict) -> None:
        same_non_finite(candidate)
        candidate["candidate_rewards"][0]["infinite_diagnostic"] = float("-inf")

    result = _compare(
        other_root,
        mutate_reference=same_non_finite,
        mutate_candidate=different_non_finite,
    )
    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_PARITY_FAILED"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("validation_reads", 1),
        ("holdout_reads", 1),
        ("forward_2026_reads", 1),
        ("promotion", "ALLOWED"),
        ("strict_stage_a", "AUTHORIZED"),
        ("data_role", "validation"),
    ],
)
def test_result_access_contract_fails_closed(
    tmp_path: Path, field: str, value: object
) -> None:
    def mutate(candidate: dict) -> None:
        candidate[field] = value

    with pytest.raises(ValueError):
        _compare(tmp_path, mutate_candidate=mutate)


def test_split_manifest_must_be_valid_and_identical(tmp_path: Path) -> None:
    def mutate(candidate: dict) -> None:
        candidate["split_manifest_hash"] = "b" * 64

    with pytest.raises(ValueError, match="split_manifest_hash differ"):
        _compare(tmp_path, mutate_candidate=mutate)
