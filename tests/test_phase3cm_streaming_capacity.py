from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path

import numpy as np
import pytest

from our_system_phase2.services.phase3cm_streaming_capacity import (
    BlockRowLimitError,
    enforce_block_row_limit,
    predict_dag_cache_peak,
)
from our_system_phase2.services.phase3cm_streaming_dag import SharedMultiCandidateDAGPlan
from our_system_phase2.services.phase3cm_streaming_expression import (
    StreamingExpressionExecutor,
)
from our_system_phase2.services.phase3cm_streaming_resource_contract import (
    FrozenExecutionPlan,
)
import scripts.preflight_cn_phase3cm_dag_cache as capacity_preflight
from scripts.preflight_cn_phase3cm_dag_cache import (
    SOURCE_CLOSURE_PATHS,
    build_receipt,
    validate_receipt_for_launch,
)


def _candidate(
    candidate_id: str,
    *,
    pair_id: str,
    role: str,
    expression: str,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "pair_id": pair_id,
        "pair_member_role": role,
        "clock_namespace": "active_bar",
        "expression": expression,
        "declared_field_ids": ["x"],
        "support_unit": "stock-minute cross-section",
        "maturity_contract": "bar close",
        "outer_mapping": "cross_sectional",
        "portfolio_mode": "long_only_top",
    }


def _shared_plan() -> SharedMultiCandidateDAGPlan:
    return SharedMultiCandidateDAGPlan.build(
        [
            _candidate(
                "candidate.a",
                pair_id="pair.a",
                role="PRIMARY",
                expression="Add($x,1)",
            ),
            _candidate(
                "candidate.b",
                pair_id="pair.a",
                role="CONTROL",
                expression="Mul(Add($x,1),2)",
            ),
        ]
    )


def _sidecar_manifests(tmp_path: Path) -> tuple[Path, Path]:
    split_hash = "a" * 64
    field_root = tmp_path / "fields"
    label_root = tmp_path / "labels"
    field_root.mkdir()
    label_root.mkdir()
    (field_root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json").write_text(
        json.dumps(
            {
                "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
                "data_role": "development_train_only",
                "split_manifest_hash": split_hash,
                "source_shard_count": 16,
                "sidecar_rows": 100,
                "shards": [{"rows": 1} for _ in range(16)],
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
            }
        ),
        encoding="utf-8",
    )
    (label_root / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json").write_text(
        json.dumps(
            {
                "status": "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY",
                "data_role": "development_train_only",
                "split_manifest_hash": split_hash,
                "source_shard_count": 16,
                "output_rows": 100,
                "shards": [{"rows": 1} for _ in range(16)],
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
            }
        ),
        encoding="utf-8",
    )
    return field_root, label_root


def _explicit_source_closure_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[str, Path]:
    repo_sha = "b" * 40
    repo_root = Path(__file__).resolve().parents[1]
    sources = []
    for relative in SOURCE_CLOSURE_PATHS:
        source = repo_root / relative
        sources.append(
            {
                "path": relative,
                "sha256": capacity_preflight._canonical_source_sha256(source),
                "git_blob_sha": capacity_preflight._git_blob_hash(source),
                "git_mode": "100644",
                "normalization": "TEXT_CRLF_TO_LF",
            }
        )
    manifest = {
        "schema_version": "cn_phase3cm_source_closure_manifest_v1",
        "status": "CN_PHASE3CM_SOURCE_CLOSURE_MANIFEST_READY",
        "repo_sha": repo_sha,
        "source_paths": list(SOURCE_CLOSURE_PATHS),
        "sources": sources,
        "source_closure_hash": capacity_preflight._stable_hash(sources),
    }
    manifest["manifest_hash"] = capacity_preflight._stable_hash(manifest)
    path = tmp_path / "CN_PHASE3CM_SOURCE_CLOSURE_MANIFEST.json"
    path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    monkeypatch.setattr(
        capacity_preflight,
        "_git_repository_available",
        lambda _repo_root: False,
    )
    return repo_sha, path


def test_prediction_matches_executor_cache_accounting_and_last_consumer_release() -> None:
    rows = 11
    plan = _shared_plan()
    order = ("candidate.a", "candidate.b")
    predicted = predict_dag_cache_peak(
        plan,
        ordered_candidate_ids=order,
        max_block_rows=rows,
        dag_block_cache_bytes=10_000,
    )

    node_by_id = {node.node_id: node for node in plan.nodes}
    root_by_candidate = {root.candidate_id: root for root in plan.candidate_roots}
    release_ids = plan.release_node_ids_by_candidate_batch(tuple((value,) for value in order))
    release_keys = tuple(
        tuple(
            StreamingExpressionExecutor.cache_key(
                node_by_id[node_id].canonical_expression,
                value_namespace=node_by_id[node_id].cohort_id,
                mapping_namespace=(
                    node_by_id[node_id].mapping_subcohort_id
                    if node_by_id[node_id].layer == "MAPPING"
                    else None
                ),
            )
            for node_id in ids
        )
        for ids in release_ids
    )
    roots = tuple(root_by_candidate[candidate_id] for candidate_id in order)
    executor = StreamingExpressionExecutor(
        code_count=2,
        compute_threads=1,
        cache_max_bytes=10_000,
    ).bind_block(
        raw_fields={"x": np.linspace(1.0, 2.0, rows)},
        code_ids=np.arange(rows, dtype=np.int32) % 2,
        time_ids=np.arange(rows, dtype=np.int64),
    )
    executor.evaluate_ordered_into(
        (root.expression for root in roots),
        value_namespaces=(root.value_cohort_id for root in roots),
        mapping_namespaces=(root.mapping_subcohort_id for root in roots),
        release_keys_after_each=release_keys,
    )

    assert predicted.status == "CN_PHASE3CM_DAG_CACHE_PREFLIGHT_PASS"
    assert predicted.predicted_peak_owned_arrays == 4
    assert predicted.predicted_peak_bytes == executor.audit["cache_peak_bytes"]
    assert predicted.predicted_peak_entries == 5
    assert executor.audit["cache_current_bytes"] == predicted.final_cache_bytes == 0
    assert executor.audit["cache_entry_count"] == predicted.final_cache_entries == 0


def test_rolling_parameter_atoms_are_not_predicted_as_cache_arrays() -> None:
    plan = SharedMultiCandidateDAGPlan.build(
        [
            _candidate(
                "candidate.delta",
                pair_id="pair.delta",
                role="PRIMARY",
                expression="Delta($x,5)",
            )
        ]
    )
    predicted = predict_dag_cache_peak(
        plan,
        ordered_candidate_ids=("candidate.delta",),
        max_block_rows=100,
        dag_block_cache_bytes=800,
    )

    assert predicted.predicted_peak_owned_arrays == 1
    assert predicted.predicted_peak_bytes == 800
    assert predicted.skipped_parameter_node_count == 1
    assert predicted.status == "CN_PHASE3CM_DAG_CACHE_PREFLIGHT_PASS"


def test_preflight_fails_closed_without_changing_the_cap() -> None:
    plan = _shared_plan()
    predicted = predict_dag_cache_peak(
        plan,
        ordered_candidate_ids=("candidate.a", "candidate.b"),
        max_block_rows=11,
        dag_block_cache_bytes=(4 * 11 * 8) - 1,
    )

    assert predicted.status == "CN_PHASE3CM_DAG_CACHE_PREFLIGHT_FAIL_CLOSED"
    assert predicted.byte_headroom == -1
    assert predicted.max_safe_block_rows == 10


def test_runtime_row_guard_rejects_a_falsely_low_preflight_bound() -> None:
    assert enforce_block_row_limit(11, max_block_rows=11, block_ordinal=3) == 11
    with pytest.raises(BlockRowLimitError, match="actual_rows=12 max_block_rows=11"):
        enforce_block_row_limit(12, max_block_rows=11, block_ordinal=3)


def test_source_closure_rejects_any_critical_source_drift(
    tmp_path,
    monkeypatch,
) -> None:
    def fake_git(_repo_root: Path, *arguments: str) -> str:
        if arguments[:2] == ("rev-parse", "HEAD"):
            return "c" * 40
        if arguments and arguments[0] == "ls-files":
            return str(arguments[-1])
        if arguments and arguments[0] == "status":
            return " M src/our_system_phase2/services/phase3cm_streaming_dag.py"
        raise AssertionError(f"unexpected git invocation: {arguments}")

    monkeypatch.setattr(capacity_preflight, "_git_output", fake_git)
    with pytest.raises(RuntimeError, match="source closure is not clean"):
        capacity_preflight._clean_source_closure(tmp_path)


def test_no_git_source_closure_requires_explicit_manifest(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        capacity_preflight,
        "_git_repository_available",
        lambda _repo_root: False,
    )
    with pytest.raises(RuntimeError, match="requires an explicit source closure"):
        capacity_preflight._resolve_source_closure(
            Path(__file__).resolve().parents[1],
            source_closure_manifest=None,
            expected_repo_sha="b" * 40,
        )


def test_source_closure_recursively_covers_live_local_imports_and_launch_helpers() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    frozen_sources = set(SOURCE_CLOSURE_PATHS)
    assert capacity_preflight.discover_source_closure_paths(repo_root) == (
        SOURCE_CLOSURE_PATHS
    )
    for relative in SOURCE_CLOSURE_PATHS:
        assert set(
            capacity_preflight._direct_local_import_files(repo_root, relative)
        ) <= frozen_sources
    assert {
        "scripts/invoke_cn_phase3cm_backend_with_exit_receipt.ps1",
        "scripts/cn_phase3cm_process_tree_monitor.ps1",
        "scripts/run_cn_phase3cm_partitioned_backend_77o.ps1",
        "src/our_system_phase2/services/phase3cm_time_major_sidecar.py",
        "src/our_system_phase2/services/candidate_schema.py",
        "src/our_system_phase2/runtime/phase3bl_bk_priority_signal_materialization.py",
        "src/our_system_phase2/services/candidate_submission_receipt.py",
        "src/our_system_phase2/services/fixed_split_authority.py",
        "src/our_system_phase2/services/legacy_field_aliases.py",
        "src/our_system_phase2/services/matched_control_pairs.py",
        "src/our_system_phase2/services/real_market_validation.py",
        "src/our_system_phase2/services/signal_vector_semantics.py",
        "src/our_system_phase2/services/unified_capability_registry.py",
    } <= frozen_sources


def test_source_blob_identity_is_portable_across_windows_line_endings(tmp_path) -> None:
    lf = tmp_path / "lf.py"
    crlf = tmp_path / "crlf.py"
    lf.write_bytes(b"x = 1\ny = 2\n")
    crlf.write_bytes(b"x = 1\r\ny = 2\r\n")
    assert capacity_preflight._canonical_source_sha256(lf) == (
        capacity_preflight._canonical_source_sha256(crlf)
    )
    assert capacity_preflight._git_blob_hash(lf) == capacity_preflight._git_blob_hash(
        crlf
    )


def test_clean_commit_generates_git_blob_and_mode_manifest(tmp_path) -> None:
    source_repo = Path(__file__).resolve().parents[1]
    for relative in SOURCE_CLOSURE_PATHS:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(capacity_preflight._canonical_source_bytes(source_repo / relative))
    commands = (
        ("git", "init", "--quiet"),
        ("git", "add", "--", *SOURCE_CLOSURE_PATHS),
        (
            "git",
            "-c",
            "user.name=capacity-test",
            "-c",
            "user.email=capacity-test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "source closure fixture",
        ),
    )
    for command in commands:
        subprocess.run(command, cwd=tmp_path, check=True, capture_output=True)

    manifest = capacity_preflight._source_closure_manifest_body_from_git(tmp_path)
    assert manifest["source_paths"] == list(SOURCE_CLOSURE_PATHS)
    assert all(row["git_mode"] == "100644" for row in manifest["sources"])
    assert all(len(row["git_blob_sha"]) == 40 for row in manifest["sources"])
    assert manifest["manifest_hash"] == capacity_preflight._stable_hash(
        {key: value for key, value in manifest.items() if key != "manifest_hash"}
    )


def test_candidate_order_must_cover_frozen_roots_exactly() -> None:
    with pytest.raises(ValueError, match="cover frozen DAG roots exactly"):
        predict_dag_cache_peak(
            _shared_plan(),
            ordered_candidate_ids=("candidate.a",),
            max_block_rows=11,
            dag_block_cache_bytes=10_000,
        )


def test_receipt_binds_inputs_manifests_source_closure_and_launch_validation(
    tmp_path,
    monkeypatch,
) -> None:
    repo_sha, source_manifest = _explicit_source_closure_manifest(
        tmp_path,
        monkeypatch,
    )
    candidates = [
        _candidate(
            "candidate.a",
            pair_id="pair.a",
            role="PRIMARY",
            expression="Add($x,1)",
        ),
        _candidate(
            "candidate.b",
            pair_id="pair.a",
            role="CONTROL",
            expression="Mul(Add($x,1),2)",
        ),
    ]
    candidate_path = tmp_path / "candidates.csv"
    headers = list(candidates[0])
    with candidate_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in candidates:
            writer.writerow(
                {
                    key: json.dumps(value) if isinstance(value, list) else value
                    for key, value in row.items()
                }
            )
    execution_plan = FrozenExecutionPlan.create(
        phase="E",
        block_size=5,
        block_boundaries=(("2024-01-02", "2024-01-09"),),
        pair_batches=(("pair.a",),),
        heavy_processes=1,
        compute_threads=1,
        primary_thread_pool="numba",
        cache_caps={"dag_block_cache_bytes": 10_000},
        checkpoint_every_blocks=1,
        rss_soft_bytes=20_000,
        rss_hard_bytes=30_000,
        global_rss_hard_bytes=40_000,
    )
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(execution_plan.to_dict()), encoding="utf-8")
    field_root, label_root = _sidecar_manifests(tmp_path)

    receipt = build_receipt(
        candidate_table=candidate_path,
        execution_plan=plan_path,
        backend="active_bar",
        max_block_rows=11,
        cache_max_entries=2048,
        field_sidecar_root=field_root,
        label_sidecar_root=label_root,
        source_closure_manifest=source_manifest,
        expected_repo_sha=repo_sha,
    )

    assert receipt["status"] == "CN_PHASE3CM_DAG_CACHE_PREFLIGHT_PASS"
    assert receipt["execution_plan_hash"] == execution_plan.execution_plan_hash
    assert receipt["sealed_reads"] == {
        "validation": 0,
        "holdout": 0,
        "forward_2026": 0,
    }
    assert receipt["changes_to_frozen_execution_plan"] == 0
    assert receipt["preflight_source_contract"]["repo_sha"] == repo_sha
    assert receipt["preflight_source_contract"]["contract_hash"]
    assert {
        row["path"] for row in receipt["preflight_source_contract"]["sources"]
    } == set(SOURCE_CLOSURE_PATHS)
    assert receipt["block_row_evidence"]["kind"] == (
        "RUNTIME_ENFORCED_UPPER_BOUND_WITH_SIDECAR_MANIFESTS"
    )
    assert receipt["block_row_evidence"]["field_sidecar_manifest"][
        "manifest_sha256"
    ]
    assert receipt["block_row_evidence"]["label_sidecar_manifest"][
        "manifest_sha256"
    ]
    assert receipt["runtime_cache_entry_contract"] == {
        "preflight_cache_max_entries": 2048,
        "executor_cache_max_entries_default": 2048,
        "matches_active_executor": True,
    }
    assert receipt["receipt_hash"]

    receipt_path = tmp_path / "capacity_receipt.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    validated = validate_receipt_for_launch(
        receipt_path=receipt_path,
        candidate_table=candidate_path,
        execution_plan=plan_path,
        backend="active_bar",
        field_sidecar_root=field_root,
        label_sidecar_root=label_root,
        repo_root=Path(__file__).resolve().parents[1],
        expected_repo_sha=repo_sha,
        source_closure_manifest=source_manifest,
    )
    assert validated == {
        "status": "CN_PHASE3CM_DAG_CACHE_RECEIPT_VALIDATED_FOR_LAUNCH",
        "backend": "active_bar",
        "receipt_hash": receipt["receipt_hash"],
        "execution_plan_hash": execution_plan.execution_plan_hash,
        "max_block_rows": 11,
        "source_closure_hash": receipt["preflight_source_contract"][
            "source_closure_hash"
        ],
        "source_closure_manifest_hash": receipt["preflight_source_contract"][
            "source_closure_manifest_hash"
        ],
        "source_closure_manifest_sha256": capacity_preflight._sha256(
            source_manifest
        ),
    }

    with pytest.raises(ValueError, match="must equal the active executor default"):
        build_receipt(
            candidate_table=candidate_path,
            execution_plan=plan_path,
            backend="active_bar",
            max_block_rows=11,
            cache_max_entries=1024,
            field_sidecar_root=field_root,
            label_sidecar_root=label_root,
            source_closure_manifest=source_manifest,
            expected_repo_sha=repo_sha,
        )

    tampered = dict(receipt)
    tampered["receipt_hash"] = "0" * 64
    receipt_path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(RuntimeError, match="self-hash drift"):
        validate_receipt_for_launch(
            receipt_path=receipt_path,
            candidate_table=candidate_path,
            execution_plan=plan_path,
            backend="active_bar",
            field_sidecar_root=field_root,
            label_sidecar_root=label_root,
            repo_root=Path(__file__).resolve().parents[1],
            expected_repo_sha=repo_sha,
            source_closure_manifest=source_manifest,
        )


def test_sidecar_manifest_drift_invalidates_launch_receipt(tmp_path, monkeypatch) -> None:
    repo_sha, source_manifest = _explicit_source_closure_manifest(
        tmp_path,
        monkeypatch,
    )
    candidates = [
        _candidate(
            "candidate.a",
            pair_id="pair.a",
            role="PRIMARY",
            expression="Add($x,1)",
        ),
        _candidate(
            "candidate.b",
            pair_id="pair.a",
            role="CONTROL",
            expression="Mul(Add($x,1),2)",
        ),
    ]
    candidate_path = tmp_path / "candidates.csv"
    with candidate_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(candidates[0]))
        writer.writeheader()
        for row in candidates:
            writer.writerow(
                {
                    key: json.dumps(value) if isinstance(value, list) else value
                    for key, value in row.items()
                }
            )
    plan = FrozenExecutionPlan.create(
        phase="E",
        block_size=5,
        block_boundaries=(("2024-01-02", "2024-01-09"),),
        pair_batches=(("pair.a",),),
        heavy_processes=1,
        compute_threads=1,
        primary_thread_pool="numba",
        cache_caps={"dag_block_cache_bytes": 10_000},
        checkpoint_every_blocks=1,
        rss_soft_bytes=20_000,
        rss_hard_bytes=30_000,
        global_rss_hard_bytes=40_000,
    )
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan.to_dict()), encoding="utf-8")
    field_root, label_root = _sidecar_manifests(tmp_path)
    receipt = build_receipt(
        candidate_table=candidate_path,
        execution_plan=plan_path,
        backend="active_bar",
        max_block_rows=11,
        cache_max_entries=2048,
        field_sidecar_root=field_root,
        label_sidecar_root=label_root,
        source_closure_manifest=source_manifest,
        expected_repo_sha=repo_sha,
    )
    receipt_path = tmp_path / "receipt.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    field_manifest = field_root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    field_payload = json.loads(field_manifest.read_text(encoding="utf-8"))
    field_payload["sidecar_rows"] = 101
    field_manifest.write_text(json.dumps(field_payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="does not exactly match"):
        validate_receipt_for_launch(
            receipt_path=receipt_path,
            candidate_table=candidate_path,
            execution_plan=plan_path,
            backend="active_bar",
            field_sidecar_root=field_root,
            label_sidecar_root=label_root,
            repo_root=Path(__file__).resolve().parents[1],
            expected_repo_sha=repo_sha,
            source_closure_manifest=source_manifest,
        )


def test_no_git_manifest_tamper_and_wrong_repo_fail_closed(
    tmp_path,
    monkeypatch,
) -> None:
    repo_sha, source_manifest = _explicit_source_closure_manifest(
        tmp_path,
        monkeypatch,
    )
    repo_root = Path(__file__).resolve().parents[1]
    with pytest.raises(RuntimeError, match="repo SHA drift"):
        capacity_preflight.validate_source_closure_manifest(
            source_manifest,
            repo_root=repo_root,
            expected_repo_sha="c" * 40,
        )

    original = json.loads(source_manifest.read_text(encoding="utf-8"))
    payload = json.loads(json.dumps(original))
    payload["source_paths"].append("scripts/forged.py")
    payload.pop("manifest_hash")
    payload["manifest_hash"] = capacity_preflight._stable_hash(payload)
    source_manifest.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    with pytest.raises(RuntimeError, match="path set/order drift"):
        capacity_preflight.validate_source_closure_manifest(
            source_manifest,
            repo_root=repo_root,
            expected_repo_sha=repo_sha,
        )

    payload = json.loads(json.dumps(original))
    payload["sources"][0]["sha256"] = "0" * 64
    payload["source_closure_hash"] = capacity_preflight._stable_hash(
        payload["sources"]
    )
    payload.pop("manifest_hash")
    payload["manifest_hash"] = capacity_preflight._stable_hash(payload)
    source_manifest.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    with pytest.raises(RuntimeError, match="source file SHA-256 drift"):
        capacity_preflight.validate_source_closure_manifest(
            source_manifest,
            repo_root=repo_root,
            expected_repo_sha=repo_sha,
        )


def test_phase_e_launcher_validates_receipts_before_heavy_processes() -> None:
    launcher = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_cn_phase3cm_phase_e_qualification_77o.ps1"
    ).read_text(encoding="utf-8")
    gate = launcher.index("$ActiveCapacityValidation = Confirm-CapacityReceipt")
    output_creation = launcher.index("New-Item -ItemType Directory -Force -Path $RunRoot")
    heavy_launch = launcher.index("$Active = Start-Process")
    assert gate < output_creation < heavy_launch
    assert "[string]$ActiveCapacityReceipt" in launcher
    assert "[string]$SessionCapacityReceipt" in launcher
    assert "[string]$PredecessorEngineeringReceipt" in launcher
    assert "[string]$SourceClosureManifest" in launcher
    assert "--source-closure-manifest $SourceClosureManifest" in launcher
    assert "CN_PHASE3CM_R6_SUCCESSOR_ENGINEERING_PASS" in launcher
    assert "CN_PHASE3CM_PHASE_E_STRICT_WAVE_FAIL" in launcher
    assert "886aa3b9dcf16001cf1ca7b574d7193e1f96dc18f49d52d5c512d4ac8ba5d413" in launcher
    assert '"--max-block-rows"' in launcher
    assert '"--capacity-receipt-hash"' in launcher
    assert "execution_plan_hash -ne" in launcher
    assert "execution plan file hash drifts" in launcher

    runner = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_cn_phase3cm_streaming_qualification.py"
    ).read_text(encoding="utf-8")
    block_read = runner.index("block = reader.read_block(")
    row_guard = runner.index("                enforce_block_row_limit(")
    dag_bind = runner.index("            expression.bind_block(", row_guard)
    assert block_read < row_guard < dag_bind
