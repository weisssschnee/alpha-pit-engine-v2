from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from scripts.freeze_cn_phase3cm_1024_resource_contract import (
    FAIL_STATUS,
    FREEZE_ARTIFACT_NAMES,
    FREEZE_ROUTE_QUOTAS,
    PASS_STATUS,
    contract_hash,
    freeze_resource_contract,
)


GIB = 1024**3


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _self_hash(payload: dict, field: str) -> str:
    body = copy.deepcopy(payload)
    body.pop(field, None)
    return _digest(body)


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _boundaries() -> dict:
    return {
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }


def _fixture(tmp_path: Path) -> dict[str, Path]:
    freeze = tmp_path / "CN_RESOURCE_PREFLIGHT_FREEZE.json"
    subset = tmp_path / "historical_subset.json"
    parity = tmp_path / "readjudicated_parity.json"
    readjudication = tmp_path / "readjudication_manifest.json"
    execution = tmp_path / "execution_receipt.json"
    historical_execution = tmp_path / "historical_session_execution.json"
    historical_result = tmp_path / "historical_session_result.json"
    sidecar = tmp_path / "sidecar_closure.json"
    source = tmp_path / "source_closure.json"
    output = tmp_path / "CN_PHASE3CM_1024_RESOURCE_CONTRACT.json"

    freeze_artifacts: list[dict[str, object]] = []
    for ordinal, name in enumerate(FREEZE_ARTIFACT_NAMES):
        artifact_path = tmp_path / name
        artifact_path.write_bytes(f"fixture-{ordinal}-{name}\n".encode("utf-8"))
        freeze_artifacts.append(
            {
                "path": name,
                "sha256": _sha(artifact_path),
                "bytes": artifact_path.stat().st_size,
            }
        )
    _write(
        freeze,
        {
            # The authoritative producer does not emit a schema_version.
            "status": "CN_COMPOSITIONAL_RESOURCE_PREFLIGHT_PACK_FROZEN",
            "selection_used_performance": False,
            "data_role": "development",
            "validation_holdout_forward_read": False,
            "pair_count": 1024,
            "evaluation_round_id": "STRICT_WAVE_01024",
            "evaluator_call_count": 2048,
            "route_quotas": dict(FREEZE_ROUTE_QUOTAS),
            "policy_counts": {
                f"typed_random_partition_{ordinal:02d}": 128
                for ordinal in range(8)
            },
            "seed_counts": {
                str(seed): 128
                for seed in (1729, 2718, 31415, 65537, 104729, 130363, 155921, 196613)
            },
            "clock_counts": {"active_bar": 584, "stock_session": 440},
            "release_hash": "1" * 64,
            "release_rows": 446_443_583,
            "source_rows_before_role_filter": 598_061_503,
            "split_manifest_hash": "2" * 64,
            "registry_hash": "3" * 64,
            "pack_identity": "4" * 64,
            "source_runtime_root": str(tmp_path),
            "artifacts": freeze_artifacts,
        },
    )
    _write(
        subset,
        {
            "schema_version": "cn_phase3cm_historical_256_semantic_subset_receipt_v2",
            "status": "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_PASS",
            "historical_pair_count": 256,
            "freeze_pair_count": 1024,
            "historical_candidate_count": 512,
            "freeze_candidate_count": 2048,
            "pair_identity_subset_exact": True,
            "candidate_identity_subset_exact": True,
            "pair_semantics_exact": True,
            "candidate_semantics_exact": True,
            "missing_pair_ids": [],
            "missing_candidate_ids": [],
            "pair_semantic_mismatch_ids": [],
            "candidate_semantic_mismatch_ids": [],
        },
    )

    parity_payload = {
        "schema_version": "cn_phase3cm_current_kernel_146_parity_final_v1",
        "status": "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_PASS",
        "kernel_state": "PARTIALLY_QUALIFIED",
        "backend_authority": "EXPERIMENTAL_BACKEND",
        "formal_evaluator_authority": "UNCHANGED",
        "identity_coverage": {
            "expected_pair_count": 146,
            "observed_pair_count": 146,
            "expected_candidate_count": 292,
            "observed_candidate_count": 292,
            "pair_union_digest": "1" * 64,
            "candidate_union_digest": "2" * 64,
        },
        "identity_coverage_exact": True,
        "partitions": [
            {
                "partition_id": f"active_bar_p{ordinal}",
                "status": "EXACT_PARITY_PASS",
                "pair_count": 73,
                "candidate_count": 146,
                "qualification": {
                    "comparable": True,
                    "semantic_parity_exact": True,
                    "status_observed_not_gated": "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED",
                    "performance_observed_not_gated": {"wall_speedup": 1.0},
                },
                "checkpoint": {
                    "continuation_parity": {
                        key: True
                        for key in ("temporal", "state", "support", "portfolio", "reducer")
                    },
                    "contract_parity": {"backend": True},
                    "scaling_status_observed_not_gated": "CN_PHASE3CM_SCALING_PROBE_FAIL_CLOSED",
                    "performance_observed_not_gated": {
                        "parallelism_not_engaged_events": 3
                    },
                },
            }
            for ordinal in range(2)
        ],
        "boundaries": _boundaries(),
        "performance_threshold_gate": "NOT_USED",
        "two_x_speedup_required": False,
        "errors": [],
        "next_decision": "FREEZE_1024_RESOURCE_AND_EXECUTION_CONTRACT",
    }
    _write(parity, parity_payload)

    execution_payload = {
        "schema_version": "cn_phase3cm_current_kernel_146_replay_execution_receipt_v1",
        "status": "CN_PHASE3CM_CURRENT_KERNEL_146_REPLAY_FAIL_CLOSED",
        "authority_scope": "PARITY_REPLAY_ONLY_NOT_FORMAL_EVALUATOR",
        "expected_repo_sha": "a" * 40,
        "observed_repo_sha": "a" * 40,
        "source_closure": {
            "mode": "EXPLICIT_SOURCE_CLOSURE_MANIFEST_VERIFIED",
            "repo_sha": "a" * 40,
            "manifest_sha256": "3" * 64,
            "manifest_hash": "4" * 64,
            "source_closure_hash": "5" * 64,
            "source_count": 42,
        },
        "wall_seconds": 2179.0138645,
        "wall_seconds_hard": 21_600,
        "global_peak_rss_bytes": 29_656_395_776,
        "global_rss_hard_bytes": 60 * GIB,
        "output_peak_bytes": 435_602_403,
        "output_final_bytes": 435_618_229,
        "output_bytes_hard": 64 * GIB,
        "gate_failure": None,
        "launch_failure": None,
        "partitions": [
            {
                "partition_id": f"active_bar_p{ordinal}",
                "execution_pass": True,
                "qualification_exact_pass": True,
                "checkpoint_exact_pass": False,
                "process_exit_code": 0,
                "peak_process_tree_rss_bytes": peak,
            }
            for ordinal, peak in enumerate((13_848_854_528, 15_855_566_848))
        ],
        "final_parity_pass": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "speedup_threshold_gate": "NOT_USED",
    }
    _write(execution, execution_payload)

    historical_result_payload = {
        "schema_version": "cn_phase3cm_streaming_backend_result_v1",
        "status": "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED",
        "backend": "stock_session",
        "phase": "E",
        "pair_count": 110,
        "candidate_count": 220,
        "wall_seconds": 110 * 1.4299636,
        "peak_rss_bytes": 3 * GIB,
        "parallelism_status": "PARALLELISM_ENGAGED",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }
    _write(historical_result, historical_result_payload)
    _write(
        historical_execution,
        {
            "schema_version": "cn_phase3cm_phase_e_combined_execution_receipt_v1",
            "status": "CN_PHASE3CM_PHASE_E_STRICT_WAVE_FAIL",
            "combined_contract_repo_sha": "b" * 40,
            "active_exit_code": 1,
            "session_exit_code": 0,
            "active_pair_count": 146,
            "session_pair_count": 110,
            "total_pair_count": 256,
            "heavy_processes": 2,
            "compute_threads_by_backend": {"active_bar": 11, "stock_session": 3},
            "global_active_native_compute_threads": 14,
            "session_result": str(historical_result.resolve()),
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
            "strict_stage_a": "NOT_AUTHORIZED",
        },
    )

    sidecar_payload = {
        "schema_version": "cn_phase3cm_1024_sidecar_closure_v1",
        "status": "CN_PHASE3CM_1024_SIDECAR_CLOSURE_PASS",
        "data_role": "development_train_only",
        "pair_counts": {"active_bar": 584, "stock_session": 440, "total": 1024},
        "coverage_audit": {"all_required_fields_present": True},
        "sealed_reads": {
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "formal_evaluator_authority": "UNCHANGED",
        "streaming_backend_authority": "EXPERIMENTAL_BACKEND",
    }
    sidecar_payload["closure_hash"] = _self_hash(sidecar_payload, "closure_hash")
    _write(sidecar, sidecar_payload)

    sources = [
        {
            "path": "scripts/run_cn_phase3cm_streaming_qualification.py",
            "sha256": "6" * 64,
            "git_blob_sha": "7" * 40,
            "git_mode": "100644",
            "normalization": "TEXT_CRLF_TO_LF",
        }
    ]
    source_payload = {
        "schema_version": "cn_phase3cm_source_closure_manifest_v1",
        "status": "CN_PHASE3CM_SOURCE_CLOSURE_MANIFEST_READY",
        "repo_sha": "c" * 40,
        "source_paths": [row["path"] for row in sources],
        "sources": sources,
        "source_closure_hash": _digest(sources),
    }
    source_payload["manifest_hash"] = _self_hash(source_payload, "manifest_hash")
    _write(source, source_payload)

    _write(
        readjudication,
        {
            "schema_version": "cn_phase3cm_current_kernel_146_readjudication_manifest_v1",
            "status": "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_READJUDICATED_PASS",
            "reason": "CHECKPOINT_EXACTNESS_WAS_CONFLATED_WITH_SCALING_PARALLELISM_DIAGNOSTIC",
            "repo_sha": "c" * 40,
            # This is the historical readjudication source closure.  The
            # 1,024 launcher has a separate, current source closure input.
            "source_closure_manifest_sha256": "9" * 64,
            "original_execution_receipt_sha256": _sha(execution),
            "readjudicated_parity_receipt_sha256": _sha(parity),
            "identity_coverage_exact": True,
            "checkpoint_categories": [
                "temporal",
                "state",
                "support",
                "portfolio",
                "reducer",
            ],
            "scaling_status_observed_not_gated": [
                "CN_PHASE3CM_SCALING_PROBE_FAIL_CLOSED",
                "CN_PHASE3CM_SCALING_PROBE_FAIL_CLOSED",
            ],
            "boundaries": _boundaries(),
            "next_decision": "FREEZE_1024_RESOURCE_AND_EXECUTION_CONTRACT",
        },
    )

    return {
        "freeze": freeze,
        "subset": subset,
        "parity": parity,
        "readjudication": readjudication,
        "execution": execution,
        "historical_execution": historical_execution,
        "historical_result": historical_result,
        "sidecar": sidecar,
        "source": source,
        "output": output,
    }


def _rebind_readjudication(paths: dict[str, Path]) -> None:
    payload = json.loads(paths["readjudication"].read_text(encoding="utf-8"))
    payload["original_execution_receipt_sha256"] = _sha(paths["execution"])
    payload["readjudicated_parity_receipt_sha256"] = _sha(paths["parity"])
    _write(paths["readjudication"], payload)


def _freeze(paths: dict[str, Path], **kwargs: object) -> dict:
    return freeze_resource_contract(
        freeze_path=paths["freeze"],
        historical_subset_path=paths["subset"],
        readjudicated_parity_path=paths["parity"],
        readjudication_manifest_path=paths["readjudication"],
        execution_receipt_path=paths["execution"],
        historical_session_execution_path=paths["historical_execution"],
        historical_session_result_path=paths["historical_result"],
        sidecar_closure_path=paths["sidecar"],
        source_closure_path=paths["source"],
        output_path=paths["output"],
        **kwargs,
    )


def test_freezes_resource_contract_from_superseded_146_evidence(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)

    contract = _freeze(paths)

    assert contract["status"] == PASS_STATUS
    assert contract["schema_version"] == "cn_phase3cm_1024_resource_contract_v1"
    assert contract["repo_sha"] == "c" * 40
    assert contract["contract_hash"] == contract_hash(contract)
    assert json.loads(paths["output"].read_text(encoding="utf-8")) == contract
    assert contract["total_pair_count"] == 1024
    assert contract["backend_pair_counts"] == {"active_bar": 584, "stock_session": 440}
    assert contract["heavy_processes"] == 2
    assert contract["native_compute_threads_by_backend"] == {
        "active_bar": 11,
        "stock_session": 2,
    }
    assert contract["global_active_native_compute_threads"] == 13
    assert contract["global_native_compute_threads_hard_max"] == 24
    assert contract["worker_rss_soft_bytes"] == 20 * GIB
    assert contract["worker_rss_hard_bytes"] == 24 * GIB
    assert contract["rss_hard_bytes_by_backend"] == {
        "active_bar": 24 * GIB,
        "stock_session": 24 * GIB,
    }
    assert contract["global_rss_projected_bytes"] == 48 * GIB
    assert contract["global_rss_hard_bytes"] == 60 * GIB
    assert contract["output_bytes_hard"] == 64 * GIB
    assert contract["wall_seconds_hard_max"] == 12 * 3600
    assert contract["host_hours_hard_max"] == 12.0
    assert contract["speedup_threshold"] == "NONE"
    assert contract["data_role"] == "development_train_only"
    assert contract["validation_reads"] == 0
    assert contract["holdout_reads"] == 0
    assert contract["forward_2026_reads"] == 0
    assert contract["promotion"] == "FORBIDDEN"
    assert contract["strict_stage_a"] == "NOT_AUTHORIZED"
    assert contract["formal_evaluator_authority"] == "UNCHANGED"
    assert contract["streaming_backend_authority"] == "EXPERIMENTAL_BACKEND"
    assert contract["kernel_state"] == "PARTIALLY_QUALIFIED"
    projection = contract["resource_projection"]
    assert projection["active_bar"]["projected_wall_seconds"] == pytest.approx(
        1.25 * 4 * 2179.0138645
    )
    assert projection["stock_session"]["historical_native_threads"] == 3
    assert projection["stock_session"]["target_native_threads"] == 2
    assert projection["stock_session"]["thread_normalization_multiplier"] == 1.5
    assert projection["stock_session"]["raw_measured_seconds_per_pair"] == pytest.approx(
        1.4299636
    )
    assert projection["stock_session"]["thread_normalized_seconds_per_pair"] == pytest.approx(
        1.4299636 * 1.5
    )
    assert projection["stock_session"]["conservative_measured_seconds_per_pair"] == pytest.approx(
        1.4299636 * 1.5
    )
    assert projection["stock_session"]["projected_wall_seconds"] == pytest.approx(
        110 * 1.4299636 * 1.5 * 4
    )
    assert contract["host_hours_projected_raw"] == pytest.approx(
        (
            projection["active_bar"]["projected_wall_seconds"]
            + projection["stock_session"]["projected_wall_seconds"]
        )
        / 3600
    )
    assert contract["host_hours_projected"] == 3.5
    assert projection["host_hour_rounding"] == "CEILING_TO_0.25_HOUR"
    assert contract["evidence"]["execution_receipt"]["superseded_status"] == (
        "CN_PHASE3CM_CURRENT_KERNEL_146_REPLAY_FAIL_CLOSED"
    )
    assert contract["evidence"]["freeze"]["schema_version"] == ""
    assert contract["evidence"]["freeze"]["artifact_count"] == 7
    assert len(contract["evidence"]["freeze"]["artifact_manifest_hash"]) == 64
    historical_execution = contract["evidence"]["historical_session_execution"]
    historical_result = contract["evidence"]["historical_session_result"]
    assert historical_execution["historical_stock_session_native_threads"] == 3
    assert historical_execution["target_stock_session_native_threads"] == 2
    assert historical_execution["thread_normalization_multiplier"] == 1.5
    assert historical_result["raw_measured_seconds_per_pair"] == pytest.approx(
        1.4299636
    )
    assert historical_result["thread_normalized_seconds_per_pair"] == pytest.approx(
        1.4299636 * 1.5
    )


def test_freeze_performance_selection_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = json.loads(paths["freeze"].read_text(encoding="utf-8"))
    payload["selection_used_performance"] = True
    _write(paths["freeze"], payload)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any("used performance" in error for error in contract["errors"])


def test_freeze_forbidden_data_read_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = json.loads(paths["freeze"].read_text(encoding="utf-8"))
    payload["validation_holdout_forward_read"] = True
    _write(paths["freeze"], payload)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any("remained unread" in error for error in contract["errors"])


def test_freeze_evaluator_call_count_drift_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = json.loads(paths["freeze"].read_text(encoding="utf-8"))
    payload["evaluator_call_count"] = 2047
    _write(paths["freeze"], payload)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any("evaluator call count" in error for error in contract["errors"])


def test_freeze_artifact_content_drift_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    artifact = tmp_path / "preflight_active_candidates.csv"
    artifact.write_bytes(artifact.read_bytes() + b"drift\n")

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any(
        "artifact byte count drift" in error or "artifact SHA-256 drift" in error
        for error in contract["errors"]
    )


def test_execution_hash_must_be_superseded_by_exact_manifest_binding(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    manifest = json.loads(paths["readjudication"].read_text(encoding="utf-8"))
    manifest["original_execution_receipt_sha256"] = "f" * 64
    _write(paths["readjudication"], manifest)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any("execution receipt SHA-256" in error for error in contract["errors"])
    assert contract["contract_hash"] == contract_hash(contract)


def test_generic_scaling_failure_and_old_checkpoint_flag_are_not_resource_gates(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    result = json.loads(paths["historical_result"].read_text(encoding="utf-8"))
    result["parallelism_status"] = "GENERIC_SCALING_STATUS_FAIL_CLOSED"
    _write(paths["historical_result"], result)

    contract = _freeze(paths)

    assert contract["status"] == PASS_STATUS
    assert contract["speedup_threshold"] == "NONE"
    assert contract["adjudication"]["generic_scaling_status_used_as_gate"] is False
    assert contract["adjudication"]["legacy_checkpoint_exact_pass_used_as_gate"] is False


def test_forbidden_read_fails_closed_even_with_recomputed_sidecar_hash(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = json.loads(paths["sidecar"].read_text(encoding="utf-8"))
    payload["validation_reads"] = 1
    payload["closure_hash"] = _self_hash(payload, "closure_hash")
    _write(paths["sidecar"], payload)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any("validation_reads=0" in error for error in contract["errors"])


def test_source_closure_self_hash_drift_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = json.loads(paths["source"].read_text(encoding="utf-8"))
    payload["sources"][0]["sha256"] = "8" * 64
    _write(paths["source"], payload)
    _rebind_readjudication(paths)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any("source closure" in error and "hash" in error for error in contract["errors"])


def test_pair_count_drift_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = json.loads(paths["freeze"].read_text(encoding="utf-8"))
    payload["clock_counts"]["stock_session"] = 439
    _write(paths["freeze"], payload)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any("clock counts" in error for error in contract["errors"])


def test_projected_host_hours_over_cap_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = json.loads(paths["historical_result"].read_text(encoding="utf-8"))
    payload["wall_seconds"] = 110 * 100.0
    _write(paths["historical_result"], payload)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert contract["host_hours_projected"] > 12.0
    assert any("host-hour" in error for error in contract["errors"])


def test_observed_global_rss_over_hard_cap_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = json.loads(paths["execution"].read_text(encoding="utf-8"))
    payload["global_peak_rss_bytes"] = 61 * GIB
    _write(paths["execution"], payload)
    _rebind_readjudication(paths)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any("global RSS hard cap" in error for error in contract["errors"])


def test_thread_shape_drift_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)

    contract = _freeze(paths, active_native_threads=12)

    assert contract["status"] == FAIL_STATUS
    assert any("route-asymmetric thread shape" in error for error in contract["errors"])


def test_historical_session_thread_fact_drift_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = json.loads(paths["historical_execution"].read_text(encoding="utf-8"))
    payload["compute_threads_by_backend"]["stock_session"] = 2
    _write(paths["historical_execution"], payload)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any(
        "historical execution route-asymmetric thread shape drift" in error
        for error in contract["errors"]
    )


def test_promotion_boundary_drift_fails_closed(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = json.loads(paths["historical_execution"].read_text(encoding="utf-8"))
    payload["promotion"] = "ALLOWED"
    _write(paths["historical_execution"], payload)

    contract = _freeze(paths)

    assert contract["status"] == FAIL_STATUS
    assert any("promotion=FORBIDDEN" in error for error in contract["errors"])
