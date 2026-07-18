from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from scripts.build_cn_batched_portfolio_kernel_artifacts import build_final_artifacts
from scripts.qualify_cn_batched_portfolio_kernel import compare_kernel_runs


REPO_ROOT = Path(__file__).resolve().parents[1]
PORTFOLIO_SOURCE = REPO_ROOT / "src/our_system_phase2/services/phase3cm_streaming_portfolio.py"
REPO_SHA = subprocess.check_output(
    ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], text=True
).strip()
BASELINE_PORTFOLIO_SHA = hashlib.sha256(
    subprocess.check_output(
        [
            "git",
            "-C",
            str(REPO_ROOT),
            "show",
            f"{REPO_SHA}:src/our_system_phase2/services/phase3cm_streaming_portfolio.py",
        ]
    )
).hexdigest()
FB25_PORTFOLIO_SHA = BASELINE_PORTFOLIO_SHA
ITERATION2_PORTFOLIO_SHA = hashlib.sha256(PORTFOLIO_SOURCE.read_bytes()).hexdigest()
SPLIT_MANIFEST_HASH = "a" * 64


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def _stable_hash(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _make_golden(root: Path) -> Path:
    bundle = root / "golden"
    artifact = bundle / "phase3cm" / "golden.json"
    _write_json(artifact, {"historical": True})
    index = bundle / "artifact_index.json"
    _write_json(
        index,
        {
            "schema_version": "golden-v1",
            "repo_heads": {"phase3cm_evaluator_sha": REPO_SHA},
            "boundaries": {
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
            },
            "phase3cm_summary": {
                "hot_path_bottleneck": "BATCHED_PORTFOLIO_KERNEL_BOTTLENECK"
            },
            "artifacts": [
                {
                    "path": "phase3cm/golden.json",
                    "sha256": _sha256(artifact),
                    "bytes": artifact.stat().st_size,
                }
            ],
        },
    )
    return index


def _make_binding(root: Path) -> tuple[Path, str, str]:
    body = {
        "status": "FROZEN",
        "split_manifest_hash": SPLIT_MANIFEST_HASH,
        "data_role": "development",
        "sealed_reads": {"validation": 0, "holdout": 0, "forward_2026": 0},
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }
    binding_hash = _stable_hash(body)
    path = root / "binding.json"
    _write_json(path, {**body, "binding_hash": binding_hash})
    return path, _sha256(path), binding_hash


def _make_result(
    root: Path, binding_hash: str, wall: float, *, source_label: str
) -> Path:
    root.mkdir(parents=True)
    _write_json(
        root / "CN_STREAMING_BACKEND_RESULT.json",
        {
            "status": "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED",
            "backend": "active_bar",
            "phase": "D",
            "pair_count": 4,
            "candidate_count": 8,
            "wall_seconds": wall,
            "peak_rss_bytes": 1000,
            "parallelism_status": "PARALLELISM_ENGAGED",
            "input_binding_hash": binding_hash,
            "split_manifest_hash": SPLIT_MANIFEST_HASH,
            "data_role": "development_train_only",
            "eligible_train_date_count": 10,
            "split_rows": [
                {"split": "train", "curve_count": 10, "day_count": 2},
                {
                    "split": "validation",
                    "curve_count": 0,
                    "day_count": 0,
                    "rank_ic_obs": 0,
                    "day_mcmc_iterations": 0,
                },
                {
                    "split": "holdout",
                    "curve_count": 0,
                    "day_count": 0,
                    "rank_ic_obs": 0,
                    "day_mcmc_iterations": 0,
                },
            ],
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
                }
            ],
            "phase_totals": {
                "expression_value_dag": {
                    "wall_seconds": wall * 0.2,
                    "cpu_seconds": wall * 0.2,
                },
                "cross_sectional_rank_mapping": {
                    "wall_seconds": wall * 0.75,
                    "cpu_seconds": wall * 0.75 * 6.0,
                },
                "turnover_and_cost": {
                    "wall_seconds": wall * 0.05,
                    "cpu_seconds": wall * 0.05,
                },
            },
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
            "strict_stage_a": "NOT_AUTHORIZED",
        },
    )
    _write_json(
        root / "CN_FROZEN_EXECUTION_PLAN.json",
        {
            "execution_plan_hash": "plan-1",
            "compute_threads": 8,
            "pair_batches": [["p1", "p2", "p3", "p4"]],
        },
    )
    _write_json(root / "CN_SHARED_DAG_PLAN.json", {"plan_hash": "dag-1"})
    (root / "CN_STREAMING_REWARD_ATOMS.csv").write_text(
        "candidate_id,reward\nc1,0.1\n", encoding="utf-8"
    )
    _write_json(root / "CN_STREAMING_REDUCER_CONTRACT.json", {"version": 1})
    _write_json(root.parent / "command.json", {"source_label": source_label})
    return root


def _make_attempt(
    root: Path,
    reference_root: Path,
    result_root: Path,
    *,
    status: str,
    exact: bool = True,
    checkpoint_pass: bool = True,
) -> tuple[Path, Path]:
    if not exact:
        result_path = result_root / "CN_STREAMING_BACKEND_RESULT.json"
        result = json.loads(result_path.read_text())
        result["candidate_rewards"][0]["train_reward"] = 0.2
        _write_json(result_path, result)
    qualification_payload = compare_kernel_runs(reference_root, result_root)
    assert qualification_payload["status"] == status
    qualification = root / "qualification.json"
    _write_json(qualification, qualification_payload)
    reference = json.loads((reference_root / "CN_STREAMING_BACKEND_RESULT.json").read_text())
    candidate = json.loads((result_root / "CN_STREAMING_BACKEND_RESULT.json").read_text())
    reference_plan = json.loads((reference_root / "CN_FROZEN_EXECUTION_PLAN.json").read_text())
    candidate_plan = json.loads((result_root / "CN_FROZEN_EXECUTION_PLAN.json").read_text())

    def checkpoint_side(result: dict, plan: dict) -> dict:
        phase_totals = result["phase_totals"]
        return {
            "compute_threads": plan["compute_threads"],
            "compute_wall_seconds": sum(
                phase_totals[name]["wall_seconds"]
                for name in (
                    "expression_value_dag",
                    "cross_sectional_rank_mapping",
                    "turnover_and_cost",
                )
            ),
            "mapping_wall_seconds": phase_totals["cross_sectional_rank_mapping"][
                "wall_seconds"
            ],
            "pair_batch_size": 4,
            "peak_rss_bytes": result["peak_rss_bytes"],
        }

    checkpoint = root / "checkpoint_parity.json"
    _write_json(
        checkpoint,
        {
            "status": (
                "CN_PHASE3CM_SCALING_PROBE_PARITY_PASS"
                if checkpoint_pass
                else "CN_PHASE3CM_SCALING_PROBE_FAIL_CLOSED"
            ),
            "parity": {
                name: {"exact": checkpoint_pass, "mismatches": [] if checkpoint_pass else ["drift"]}
                for name in ("temporal", "state", "support", "portfolio", "reducer")
            },
            "backend": "active_bar",
            "data_role": "development_train_only",
            "reference": checkpoint_side(reference, reference_plan),
            "candidate": checkpoint_side(candidate, candidate_plan),
            "mapping_speedup": qualification_payload["performance"]["mapping_speedup"],
            "compute_speedup": qualification_payload["performance"]["compute_speedup"],
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
    )
    return qualification, checkpoint


def _fixture(tmp_path: Path, status: str) -> dict:
    golden = _make_golden(tmp_path)
    binding, binding_sha, binding_hash = _make_binding(tmp_path)
    baseline = _make_result(
        tmp_path / "baseline" / "run",
        binding_hash,
        20.0,
        source_label=f"golden_{REPO_SHA[:7]}",
    )
    candidate_wall = 8.0 if status == "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED" else 12.0
    fb25 = _make_result(
        tmp_path / "fb25" / "run",
        binding_hash,
        candidate_wall,
        source_label="fixture_fb25",
    )
    qualification, checkpoint = _make_attempt(
        tmp_path / "fb25_evidence", baseline, fb25, status=status
    )
    return {
        "golden_artifact_index": golden,
        "frozen_binding": binding,
        "frozen_binding_sha256": binding_sha,
        "baseline_result_root": baseline,
        "fb25_result_root": fb25,
        "fb25_qualification": qualification,
        "fb25_checkpoint_parity": checkpoint,
        "repo_sha": REPO_SHA,
        "repo_root": REPO_ROOT,
        "fb25_repo_revision": REPO_SHA,
        "baseline_portfolio_sha256": BASELINE_PORTFOLIO_SHA,
        "fb25_portfolio_sha256": FB25_PORTFOLIO_SHA,
        "output_root": tmp_path / "output",
        "remote_machine": "DESKTOP-77OPJ6F",
        "test_evidence": ["portfolio unit parity: passed"],
    }


def test_qualified_subset_emits_exactly_four_and_only_prepares_146(tmp_path: Path) -> None:
    args = _fixture(tmp_path, "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED")

    result = build_final_artifacts(**args)

    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED"
    assert result["next_decision"] == "PREPARE_SEPARATE_146_REPLAY"
    assert {path.name for path in args["output_root"].iterdir()} == {
        "frozen_contract.json",
        "benchmark_profile.json",
        "parity_receipt.json",
        "run_manifest.json",
    }
    parity = json.loads((args["output_root"] / "parity_receipt.json").read_text())
    assert parity["full_256"]["status"] == "NOT_EXECUTED_BY_CURRENT_KERNEL"
    assert parity["full_256"]["historical_golden_unchanged"] is True
    assert parity["full_256"]["current_active_bar_pairs_replayed"] == 0
    manifest = json.loads((args["output_root"] / "run_manifest.json").read_text())
    assert manifest["boundaries"]["forward_2026_reads"] == 0
    assert manifest["boundaries"]["wave_1024"] == "REQUIRES_SEPARATE_USER_AUTHORIZATION"


def test_partial_does_not_authorize_146_or_claim_current_256(tmp_path: Path) -> None:
    args = _fixture(tmp_path, "CN_BATCHED_PORTFOLIO_KERNEL_PARTIALLY_QUALIFIED")
    report = tmp_path / "reports" / "short.md"
    args["report_path"] = report

    result = build_final_artifacts(**args)

    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_PARTIALLY_QUALIFIED"
    assert result["next_decision"] == "STOP_BEFORE_146_REPLAY"
    parity = json.loads((args["output_root"] / "parity_receipt.json").read_text())
    assert parity["next_decision"] == "STOP_BEFORE_146_REPLAY"
    assert parity["retained_engineering_attempt_id"] == "fb25a91"
    benchmark = json.loads((args["output_root"] / "benchmark_profile.json").read_text())
    assert benchmark["same_input_subset"]["retained_engineering_attempt_id"] == "fb25a91"
    assert benchmark["same_input_subset"]["replay_candidate_attempt_id"] is None
    manifest = json.loads((args["output_root"] / "run_manifest.json").read_text())
    assert manifest["decision"]["retained_engineering_attempt_id"] == "fb25a91"
    assert manifest["decision"]["replay_candidate_attempt_id"] is None
    assert manifest["execution_evidence"]["remote_machine"] == "DESKTOP-77OPJ6F"
    assert manifest["execution_evidence"]["tests"] == ["portfolio unit parity: passed"]
    assert "Retained engineering attempt: `fb25a91`" in report.read_text()
    assert "Same-input subset wall speedup: `1.6666666666666667`" in report.read_text()
    assert "Mapping / compute speedup:" in report.read_text()
    assert "Observed peak RSS delta bytes:" in report.read_text()
    assert "Mapping parallelism engaged: `True`" in report.read_text()
    assert "mapping_temporary_bytes" in report.read_text()
    assert "Current-kernel 146/256 replay: `NOT_EXECUTED`" in report.read_text()


def test_execution_summary_cannot_replace_original_frozen_binding(tmp_path: Path) -> None:
    args = _fixture(tmp_path, "CN_BATCHED_PORTFOLIO_KERNEL_PARTIALLY_QUALIFIED")
    binding_sha = args["frozen_binding_sha256"]
    binding_hash = json.loads(args["frozen_binding"].read_text())["binding_hash"]
    metadata = tmp_path / "execution_summary.json"
    _write_json(
        metadata,
        {
            "status": "SUBSET_EXECUTED",
            "input_binding_sha256": binding_sha,
            "input_binding_hash": binding_hash,
            "boundaries": {
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
            },
        },
    )
    args["frozen_binding"] = metadata

    with pytest.raises(ValueError, match="original binding content"):
        build_final_artifacts(**args)


def test_failed_iteration2_is_recorded_without_promoting_or_faking_256(tmp_path: Path) -> None:
    args = _fixture(tmp_path, "CN_BATCHED_PORTFOLIO_KERNEL_PARTIALLY_QUALIFIED")
    binding = json.loads(args["frozen_binding"].read_text())
    iteration2 = _make_result(
        tmp_path / "iteration2" / "run",
        binding["binding_hash"],
        7.0,
        source_label="fixture_order_reuse",
    )
    qualification, checkpoint = _make_attempt(
        tmp_path / "iteration2_evidence",
        args["baseline_result_root"],
        iteration2,
        status="CN_BATCHED_PORTFOLIO_KERNEL_PARITY_FAILED",
        exact=False,
    )
    args.update(
        {
            "iteration2_result_root": iteration2,
            "iteration2_qualification": qualification,
            "iteration2_checkpoint_parity": checkpoint,
            "iteration2_portfolio_sha256": ITERATION2_PORTFOLIO_SHA,
        }
    )

    result = build_final_artifacts(**args)

    assert result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_PARITY_FAILED"
    receipt = json.loads((args["output_root"] / "parity_receipt.json").read_text())
    assert receipt["full_256"]["status"] == "NOT_EXECUTED_BY_CURRENT_KERNEL"
    assert receipt["next_decision"] == "STOP_BEFORE_146_REPLAY"


def test_forbidden_read_evidence_fails_closed(tmp_path: Path) -> None:
    args = _fixture(tmp_path, "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED")
    result_path = args["fb25_result_root"] / "CN_STREAMING_BACKEND_RESULT.json"
    result = json.loads(result_path.read_text())
    result["forward_2026_reads"] = 1
    _write_json(result_path, result)

    with pytest.raises(ValueError, match="forward_2026_reads=0"):
        build_final_artifacts(**args)


def test_qualification_roots_are_bound_to_actual_input_directories(tmp_path: Path) -> None:
    args = _fixture(tmp_path, "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED")
    qualification = json.loads(args["fb25_qualification"].read_text())
    qualification["candidate_root"] = str(tmp_path / "different")
    _write_json(args["fb25_qualification"], qualification)

    with pytest.raises(ValueError, match="candidate_root mismatch"):
        build_final_artifacts(**args)


def test_checkpoint_requires_all_exact_bound_sections(tmp_path: Path) -> None:
    args = _fixture(tmp_path, "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED")
    checkpoint = json.loads(args["fb25_checkpoint_parity"].read_text())
    checkpoint["parity"].pop("reducer")
    _write_json(args["fb25_checkpoint_parity"], checkpoint)

    with pytest.raises(ValueError, match="checkpoint parity is incomplete"):
        build_final_artifacts(**args)


def test_repo_sha_and_iteration2_source_hash_fail_closed(tmp_path: Path) -> None:
    args = _fixture(tmp_path, "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED")
    args["repo_sha"] = "f" * 40
    with pytest.raises(ValueError, match="does not match Git HEAD"):
        build_final_artifacts(**args)


def test_strict_output_root_rejects_unrelated_entries(tmp_path: Path) -> None:
    args = _fixture(tmp_path, "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED")
    args["output_root"].mkdir()
    (args["output_root"] / "extra.txt").write_text("unexpected", encoding="utf-8")

    with pytest.raises(ValueError, match="unexpected entries"):
        build_final_artifacts(**args)
