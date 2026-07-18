from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from scripts.freeze_cn_phase3cm_backend_partitions import (
    _require_zero_access_evidence,
    freeze_backend_partitions,
    validate_partition_contract,
)
from scripts.preflight_cn_phase3cm_dag_cache import (
    SOURCE_CLOSURE_PATHS,
    _canonical_source_sha256,
    _git_blob_hash,
    _stable_hash as _source_stable_hash,
)
from our_system_phase2.services.phase3cm_streaming_resource_contract import FrozenExecutionPlan


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    pairs = [f"p{index}" for index in range(4)]
    candidate_rows = []
    candidate_members = []
    for pair_id in pairs:
        for role in ("PRIMARY", "CONTROL"):
            candidate_id = f"{pair_id}.{role.lower()}"
            candidate_rows.append(
                {
                    "pair_id": pair_id,
                    "pair_member_role": role,
                    "candidate_id": candidate_id,
                    "expression": f"Field(${candidate_id})",
                    "route_id": "STATIC_CROSS_SECTIONAL",
                    "clock_namespace": "active_bar",
                }
            )
            candidate_members.append(
                {
                    "candidate_id": candidate_id,
                    "pair_id": pair_id,
                    "pair_member_role": role,
                    "expression": f"Field(${candidate_id})",
                    "canonical_expression": f"Field(${candidate_id})",
                    "route_id": "STATIC_CROSS_SECTIONAL",
                    "clock_namespace": "active_bar",
                }
            )
    candidate_table = tmp_path / "candidates.csv"
    with candidate_table.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "pair_id",
                "pair_member_role",
                "candidate_id",
                "expression",
                "route_id",
                "clock_namespace",
            ],
        )
        writer.writeheader()
        writer.writerows(candidate_rows)
    binding_path = tmp_path / "binding.json"
    _write_json(
        binding_path,
        {
            "status": "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND",
            "data_role": "development",
            "sealed_reads": {"validation": 0, "holdout": 0, "forward_2026": 0},
            "binding_hash": "b" * 64,
            "pairs": [
                {
                    "pair_id": pair_id,
                    "clock_namespace": "active_bar",
                    "candidate_id": f"{pair_id}.primary",
                    "control_candidate_id": f"{pair_id}.control",
                }
                for pair_id in pairs
            ],
            "candidate_members": candidate_members,
        },
    )
    binding_payload = json.loads(binding_path.read_text(encoding="utf-8"))
    binding_payload_without_hash = dict(binding_payload)
    binding_payload_without_hash.pop("binding_hash", None)
    binding_payload["binding_hash"] = hashlib.sha256(
        json.dumps(
            binding_payload_without_hash,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    _write_json(binding_path, binding_payload)
    plan = FrozenExecutionPlan.create(
        phase="E",
        block_size=2,
        block_boundaries=(("2024-01-01", "2024-01-03"),),
        pair_batches=(tuple(pairs),),
        heavy_processes=2,
        compute_threads=3,
        primary_thread_pool="numba",
        cache_caps={"dag_block_cache_bytes": 1024},
        checkpoint_every_blocks=1,
        rss_soft_bytes=2048,
        rss_hard_bytes=4096,
        global_rss_hard_bytes=8192,
    )
    plan_path = tmp_path / "plan.json"
    _write_json(plan_path, plan.to_dict())
    predecessor = tmp_path / "predecessor.json"
    _write_json(
        predecessor,
        {
            "schema_version": "cn_phase3cm_r6_successor_engineering_receipt_v1",
            "status": "CN_PHASE3CM_R6_SUCCESSOR_ENGINEERING_PASS",
            "repo_sha": "d" * 40,
            "data_role": "development_train_only",
            "access": {
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
            },
            "promotion": "FORBIDDEN",
            "pair_count": 64,
            "backend_pair_counts": {"active_bar": 36, "stock_session": 28},
            "research_parity": {
                "status": "BYTE_IDENTICAL_PAIR_ANALYSIS",
                "pair_csv_sha256": "e" * 64,
            },
            "active_backend": {"parallelism_status": "PARALLELISM_ENGAGED"},
            "stock_session_backend": {"parallelism_status": "PARALLELISM_ENGAGED"},
            "supersession": {
                "original_r6_status": "CN_PHASE3CM_PHASE_E_STRICT_WAVE_FAIL",
                "research_rows_changed": False,
                "original_backend_exit_codes": {"active_bar": 0, "stock_session": 0},
            },
        },
    )
    repo_root = Path(__file__).resolve().parents[1]
    sources = [
        {
            "path": relative,
            "sha256": _canonical_source_sha256(repo_root / relative),
            "git_blob_sha": _git_blob_hash(repo_root / relative),
            "git_mode": "100644",
            "normalization": "TEXT_CRLF_TO_LF",
        }
        for relative in SOURCE_CLOSURE_PATHS
    ]
    source_manifest_body = {
        "schema_version": "cn_phase3cm_source_closure_manifest_v1",
        "status": "CN_PHASE3CM_SOURCE_CLOSURE_MANIFEST_READY",
        "repo_sha": "a" * 40,
        "source_paths": list(SOURCE_CLOSURE_PATHS),
        "sources": sources,
        "source_closure_hash": _source_stable_hash(sources),
    }
    source_manifest = tmp_path / "CN_PHASE3CM_SOURCE_CLOSURE_MANIFEST.json"
    _write_json(
        source_manifest,
        {
            **source_manifest_body,
            "manifest_hash": _source_stable_hash(source_manifest_body),
        },
    )
    return candidate_table, binding_path, plan_path, predecessor, source_manifest


def _add_reused_backend_fixture(
    tmp_path: Path,
    binding_path: Path,
) -> tuple[Path, Path]:
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    binding["pairs"].append(
        {
            "pair_id": "session.p0",
            "clock_namespace": "stock_session",
            "candidate_id": "session.p0.primary",
            "control_candidate_id": "session.p0.control",
        }
    )
    binding_without_hash = dict(binding)
    binding_without_hash.pop("binding_hash", None)
    binding["binding_hash"] = hashlib.sha256(
        json.dumps(
            binding_without_hash,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    _write_json(binding_path, binding)
    reused = tmp_path / "session_result.json"
    _write_json(
        reused,
        {
            "backend": "stock_session",
            "status": "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED",
            "parallelism_status": "PARALLELISM_ENGAGED",
            "input_binding_hash": binding["binding_hash"],
            "pair_count": 1,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "pair_results": [{"pair_id": "session.p0"}],
        },
    )
    source_receipt = tmp_path / "source_execution_receipt.json"
    _write_json(
        source_receipt,
        {
            "schema_version": "cn_phase3cm_phase_e_combined_execution_receipt_v1",
            "status": "CN_PHASE3CM_PHASE_E_STRICT_WAVE_FAIL",
            "combined_contract_repo_sha": "c" * 40,
            "active_exit_code": 1,
            "session_exit_code": 0,
            "session_result": str(reused.resolve()),
            "active_pair_count": 4,
            "session_pair_count": 1,
            "total_pair_count": 5,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
            "strict_stage_a": "NOT_AUTHORIZED",
        },
    )
    return reused, source_receipt


def test_freeze_creates_disjoint_complete_deterministic_partitions(tmp_path: Path) -> None:
    candidate_table, binding, plan, predecessor, source_manifest = _fixture(tmp_path)
    contract = freeze_backend_partitions(
        candidate_table=candidate_table,
        binding_path=binding,
        source_plan_path=plan,
        output_root=tmp_path / "out",
        repo_sha="a" * 40,
        logical_backend="active_bar",
        partition_count=2,
        compute_threads_per_partition=3,
        pair_batch_size=1,
        predecessor_engineering_receipt=predecessor,
        source_closure_manifest=source_manifest,
    )

    assert contract["status"] == "CN_PHASE3CM_BACKEND_PARTITIONS_FROZEN"
    assert [row["pair_count"] for row in contract["partitions"]] == [2, 2]
    pair_sets = [set(row["pair_ids"]) for row in contract["partitions"]]
    assert pair_sets[0].isdisjoint(pair_sets[1])
    assert pair_sets[0] | pair_sets[1] == {"p0", "p1", "p2", "p3"}
    assert contract["global_active_native_compute_threads"] == 6
    persisted = json.loads(
        Path(contract["contract_path"]).read_text(encoding="utf-8")
    )
    assert persisted["contract_hash"] == contract["contract_hash"]
    for row in contract["partitions"]:
        frozen = FrozenExecutionPlan.from_dict(
            json.loads(Path(row["execution_plan"]).read_text(encoding="utf-8"))
        )
        assert frozen.compute_threads == 3
        assert sum(map(len, frozen.pair_batches)) == 2
    validation = validate_partition_contract(
        Path(contract["contract_path"]),
        expected_repo_sha="a" * 40,
        source_closure_manifest=source_manifest,
    )
    assert validation["status"] == "CN_PHASE3CM_BACKEND_PARTITION_CONTRACT_VALIDATED"


def test_freeze_fails_when_global_thread_budget_is_exceeded(tmp_path: Path) -> None:
    candidate_table, binding, plan, predecessor, source_manifest = _fixture(tmp_path)
    with pytest.raises(ValueError, match="thread budget"):
        freeze_backend_partitions(
            candidate_table=candidate_table,
            binding_path=binding,
            source_plan_path=plan,
            output_root=tmp_path / "out",
            repo_sha="a" * 40,
            logical_backend="active_bar",
            partition_count=2,
            compute_threads_per_partition=13,
            pair_batch_size=1,
            predecessor_engineering_receipt=predecessor,
            source_closure_manifest=source_manifest,
        )


def test_freeze_fails_when_candidate_table_is_incomplete(tmp_path: Path) -> None:
    candidate_table, binding, plan, predecessor, source_manifest = _fixture(tmp_path)
    rows = list(csv.DictReader(candidate_table.open("r", encoding="utf-8")))
    with candidate_table.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows[:-2])
    with pytest.raises(ValueError, match="exactly cover"):
        freeze_backend_partitions(
            candidate_table=candidate_table,
            binding_path=binding,
            source_plan_path=plan,
            output_root=tmp_path / "out",
            repo_sha="a" * 40,
            logical_backend="active_bar",
            partition_count=2,
            compute_threads_per_partition=3,
            pair_batch_size=1,
            predecessor_engineering_receipt=predecessor,
            source_closure_manifest=source_manifest,
        )


def test_freeze_binds_a_complete_reused_backend_result(tmp_path: Path) -> None:
    candidate_table, binding_path, plan, predecessor, source_manifest = _fixture(tmp_path)
    reused, source_receipt = _add_reused_backend_fixture(tmp_path, binding_path)

    contract = freeze_backend_partitions(
        candidate_table=candidate_table,
        binding_path=binding_path,
        source_plan_path=plan,
        output_root=tmp_path / "out",
        repo_sha="a" * 40,
        logical_backend="active_bar",
        partition_count=2,
        compute_threads_per_partition=3,
        pair_batch_size=1,
        predecessor_engineering_receipt=predecessor,
        source_closure_manifest=source_manifest,
        reused_backend_result=reused,
        reused_backend_execution_receipt=source_receipt,
    )

    assert contract["reused_backend_result"]["logical_backend"] == "stock_session"
    assert contract["reused_backend_result"]["pair_count"] == 1
    assert contract["reused_backend_result"]["access_evidence_complete"] is True
    assert contract["reused_backend_result"]["source_execution_receipt"][
        "access_evidence_complete"
    ] is True
    assert contract["total_bound_pair_count"] == 5


@pytest.mark.parametrize(
    "payload",
    [
        {"holdout_reads": 0, "forward_2026_reads": 0},
        {"validation_reads": "0", "holdout_reads": 0, "forward_2026_reads": 0},
        {"validation_reads": False, "holdout_reads": 0, "forward_2026_reads": 0},
    ],
)
def test_zero_access_evidence_rejects_missing_or_coerced_values(payload: dict) -> None:
    with pytest.raises(ValueError, match="access evidence"):
        _require_zero_access_evidence(payload, context="test receipt")


@pytest.mark.parametrize(
    ("target", "mutated_value"),
    [
        ("reused_missing", None),
        ("reused_string", "0"),
        ("source_boolean", False),
    ],
)
def test_freeze_rejects_incomplete_or_non_integer_reused_access_evidence(
    tmp_path: Path,
    target: str,
    mutated_value: object,
) -> None:
    candidate_table, binding, plan, predecessor, source_manifest = _fixture(tmp_path)
    reused, source_receipt = _add_reused_backend_fixture(tmp_path, binding)
    mutation_path = source_receipt if target.startswith("source_") else reused
    payload = json.loads(mutation_path.read_text(encoding="utf-8"))
    if target.endswith("missing"):
        payload.pop("validation_reads")
    else:
        payload["validation_reads"] = mutated_value
    _write_json(mutation_path, payload)

    with pytest.raises(ValueError, match="access evidence"):
        freeze_backend_partitions(
            candidate_table=candidate_table,
            binding_path=binding,
            source_plan_path=plan,
            output_root=tmp_path / "out",
            repo_sha="a" * 40,
            logical_backend="active_bar",
            partition_count=2,
            compute_threads_per_partition=3,
            pair_batch_size=1,
            predecessor_engineering_receipt=predecessor,
            source_closure_manifest=source_manifest,
            reused_backend_result=reused,
            reused_backend_execution_receipt=source_receipt,
        )


def test_partition_contract_validation_rejects_table_tamper(tmp_path: Path) -> None:
    candidate_table, binding, plan, predecessor, source_manifest = _fixture(tmp_path)
    contract = freeze_backend_partitions(
        candidate_table=candidate_table,
        binding_path=binding,
        source_plan_path=plan,
        output_root=tmp_path / "out",
        repo_sha="a" * 40,
        logical_backend="active_bar",
        partition_count=2,
        compute_threads_per_partition=3,
        pair_batch_size=1,
        predecessor_engineering_receipt=predecessor,
        source_closure_manifest=source_manifest,
    )
    Path(contract["partitions"][0]["candidate_table"]).write_text(
        "tampered\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="candidate table drift"):
        validate_partition_contract(
            Path(contract["contract_path"]),
            expected_repo_sha="a" * 40,
            source_closure_manifest=source_manifest,
        )


def test_partition_contract_validation_rejects_source_manifest_tamper(
    tmp_path: Path,
) -> None:
    candidate_table, binding, plan, predecessor, source_manifest = _fixture(tmp_path)
    contract = freeze_backend_partitions(
        candidate_table=candidate_table,
        binding_path=binding,
        source_plan_path=plan,
        output_root=tmp_path / "out",
        repo_sha="a" * 40,
        logical_backend="active_bar",
        partition_count=2,
        compute_threads_per_partition=3,
        pair_batch_size=1,
        predecessor_engineering_receipt=predecessor,
        source_closure_manifest=source_manifest,
    )
    tampered = json.loads(source_manifest.read_text(encoding="utf-8"))
    tampered["tamper_marker"] = True
    body = dict(tampered)
    body.pop("manifest_hash", None)
    tampered["manifest_hash"] = _source_stable_hash(body)
    _write_json(source_manifest, tampered)

    with pytest.raises(ValueError, match="partition source closure manifest drift"):
        validate_partition_contract(
            Path(contract["contract_path"]),
            expected_repo_sha="a" * 40,
            source_closure_manifest=source_manifest,
        )


def test_partition_launcher_gates_before_creating_output_or_heavy_processes() -> None:
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_cn_phase3cm_partitioned_backend_77o.ps1"
    ).read_text(encoding="utf-8-sig")

    contract_gate = script.index("CN_PHASE3CM_BACKEND_PARTITION_CONTRACT_VALIDATED")
    capacity_gate = script.index("CN_PHASE3CM_DAG_CACHE_RECEIPT_VALIDATED_FOR_LAUNCH")
    source_closure_gate = script.index(
        "capacity receipt source closure differs from frozen partition contract"
    )
    output_creation = script.index("New-Item -ItemType Directory -Force -Path $RunRoot")
    process_start = script.index("Start-Process -FilePath $PowerShellExe")
    assert contract_gate < output_creation < process_start
    assert capacity_gate < output_creation
    assert source_closure_gate < output_creation
    assert script.count("--source-closure-manifest") >= 2
    assert "[string]$SourceClosureManifest" in script
    assert "function Test-CnZeroAccessEvidence" in script
    assert '$Payload.PSObject.Properties[$Name]' in script
    assert "access_evidence_complete = $ResultAccessEvidenceComplete" in script
    assert '"--max-block-rows"' in script
    assert '"--capacity-receipt-hash"' in script
    assert "Get-CnProcessTreeRssSnapshot" in script
    assert "CN_PHASE3CM_PARTITIONED_BACKEND_PASS" in script
    assert "launcher binding path differs from frozen partition contract" in script
    assert "Remove-Item -LiteralPath $StaleArtifact -Force" in script
    assert "$Processes[$Index].ExitCode" in script
    assert "CN_PHASE3CM_BACKEND_PROCESS_COMPLETED" in script
    assert "@(Compare-Object $ExpectedPairIds $ObservedPairIds).Count -eq 0" in script
    assert "argument_file_sha256" in script
    assert "execution_plan_hash" in script
    assert "block_row_guard_status" in script
    assert "max_observed_block_rows" in script
    assert "$WallGateFailure = $true" in script
    assert "Stop-CnProcessTrees -Roots $Roots" in script
