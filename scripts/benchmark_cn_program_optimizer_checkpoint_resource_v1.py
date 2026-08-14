from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
import gc
import json
from pathlib import Path
import platform
import shutil
import sys
import time
from typing import Any, Mapping, Sequence

import psutil

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_tournament_v1 as tournament
from our_system_phase2.services.candidate_program_proposal_v0 import (
    CandidateProgramProposalAdapterV0,
)
from our_system_phase2.services.candidate_program_v1 import ProgramCompilerV1
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
PARITY_FIELDS = (
    "main_record_ordinal",
    "template_id",
    "record_kind",
    "schedule_record_sha256",
    "program_id",
    "control_program_id",
    "pair_id",
    "replay_status",
    "replay_blocker",
    "primary",
    "base_control",
    "matched_net_reward_increment",
    "matched_cumulative_return_increment",
    "search_score",
    "productive",
    "blockers",
    "base_wrapper_parity",
)


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return path


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        "".join(
            json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )
    temporary.replace(path)
    return path


def _reconstruct_checkpoint(
    *,
    freeze_root: Path,
    run_root: Path,
    checkpoint_index: int,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    tournament._configure_engine()
    tournament._verify_freeze(freeze_root)
    contract = engine._read_json(freeze_root / "phase_c_run_contract.json")
    asks = engine._read_jsonl(freeze_root / "phase_c_ask_plan.jsonl")
    reservoir = engine._read_jsonl(
        freeze_root / "phase_c_raw_program_reservoir.jsonl"
    )
    component_rows = engine._read_jsonl(
        freeze_root / "phase_c_session_executable_component_pool.jsonl"
    )
    registry = UnifiedCapabilityRegistry.read(Path(str(contract["registry_path"])))
    catalog, _ = engine._build_catalog(
        reservoir=reservoir,
        component_rows=component_rows,
        registry=registry,
    )
    components_by_id = {
        component.component_id: component
        for component in (engine._component_from_row(row) for row in component_rows)
    }
    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    initial_bandit = engine._read_json(freeze_root / "initial_bandit_state.json")
    bandit = engine.ProgramFactorizedBanditV0.restore(initial_bandit)
    selection_state = engine._selection_state()
    behavior_counts: Counter[str] = Counter()
    input_binding = engine._read_json(run_root / "input_binding.json")
    input_hash = str(input_binding["input_binding_sha256"])
    previous_manifest: Path | None = None
    for index in range(checkpoint_index + 1):
        checkpoint_id = f"checkpoint_{index + 1:03d}"
        checkpoint_asks = asks[
            index * engine.RECORDS_PER_CHECKPOINT :
            (index + 1) * engine.RECORDS_PER_CHECKPOINT
        ]
        schedules, decisions = engine._select_checkpoint(
            checkpoint_asks,
            catalog=catalog,
            bandit=bandit,
            state=selection_state,
            components_by_id=components_by_id,
            adapter=adapter,
            compiler=compiler,
        )
        if index == checkpoint_index:
            return schedules, input_binding, contract
        previous_manifest, _, bandit = engine._verify_checkpoint(
            run_root / "checkpoints" / checkpoint_id,
            checkpoint_id=checkpoint_id,
            previous_manifest=previous_manifest,
            input_hash=input_hash,
            expected_schedules=schedules,
            expected_decisions=decisions,
            bandit=bandit,
            behavior_counts=behavior_counts,
        )
    raise AssertionError("checkpoint reconstruction did not return")


def _parity_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {field: row.get(field) for field in PARITY_FIELDS}


def _benchmark_worker_count(
    *,
    worker_count: int,
    schedules: Sequence[Mapping[str, Any]],
    baseline: Mapping[int, Mapping[str, Any]],
    scratch_root: Path,
    initializer_args: tuple[Any, ...],
) -> dict[str, Any]:
    record_root = scratch_root / "records"
    record_root.mkdir(parents=True)
    resource_before = engine._runtime_resource_snapshot()
    engine._require_runtime_resource_safety(resource_before)
    minimum_available = int(resource_before["available_physical_bytes"])
    minimum_commit_headroom = int(resource_before["commit_headroom_bytes"])
    maximum_committed = int(resource_before["committed_bytes"])
    maximum_tree_rss = int(resource_before["process_tree_rss_bytes"])
    children_before = {child.pid for child in psutil.Process().children(recursive=True)}
    futures = {}
    started = time.perf_counter()
    try:
        with ProcessPoolExecutor(
            max_workers=worker_count,
            initializer=engine._initialize_worker,
            initargs=initializer_args,
        ) as executor:
            for schedule in schedules:
                ordinal = int(schedule["main_record_ordinal"])
                target = record_root / f"record_{ordinal:04d}.json"
                futures[executor.submit(engine._evaluate_record, schedule, str(target))] = ordinal
            pending = set(futures)
            while pending:
                done, pending = wait(
                    pending,
                    timeout=2.0,
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    future.result()
                sample = engine._runtime_resource_snapshot()
                engine._require_runtime_resource_safety(sample)
                minimum_available = min(
                    minimum_available, int(sample["available_physical_bytes"])
                )
                minimum_commit_headroom = min(
                    minimum_commit_headroom, int(sample["commit_headroom_bytes"])
                )
                maximum_committed = max(
                    maximum_committed, int(sample["committed_bytes"])
                )
                maximum_tree_rss = max(
                    maximum_tree_rss, int(sample["process_tree_rss_bytes"])
                )
        gc.collect()
        resource_after = engine._runtime_resource_snapshot()
        engine._require_runtime_resource_safety(resource_after)
        orphan_pids = engine._new_child_process_ids(children_before)
        if orphan_pids:
            raise RuntimeError(f"benchmark left orphan workers: {orphan_pids}")
        parity_drift: dict[int, list[str]] = {}
        projection_hashes: dict[int, str] = {}
        result_file_hashes: dict[int, str] = {}
        for schedule in schedules:
            ordinal = int(schedule["main_record_ordinal"])
            path = record_root / f"record_{ordinal:04d}.json"
            row = engine._verify_record(
                path,
                expected_input_hash=str(baseline[ordinal]["input_binding_sha256"]),
                schedule=schedule,
            )
            expected = _parity_payload(baseline[ordinal])
            observed = _parity_payload(row)
            drift = [field for field in PARITY_FIELDS if observed[field] != expected[field]]
            if drift:
                parity_drift[ordinal] = drift
            projection_hashes[ordinal] = stable_hash(observed)
            result_file_hashes[ordinal] = engine._sha256(path)
        return {
            "worker_count": worker_count,
            "wall_seconds": float(time.perf_counter() - started),
            "records_per_hour": len(schedules)
            / max((time.perf_counter() - started) / 3600.0, 1e-12),
            "minimum_available_physical_bytes": minimum_available,
            "minimum_commit_headroom_bytes": minimum_commit_headroom,
            "maximum_committed_bytes": maximum_committed,
            "maximum_process_tree_rss_bytes": maximum_tree_rss,
            "post_pool_available_physical_bytes": int(
                resource_after["available_physical_bytes"]
            ),
            "post_pool_commit_headroom_bytes": int(
                resource_after["commit_headroom_bytes"]
            ),
            "pagefile_used_before_bytes": int(resource_before["pagefile_used_bytes"]),
            "pagefile_used_after_bytes": int(resource_after["pagefile_used_bytes"]),
            "pagefile_pages_in_delta_bytes": max(
                0,
                int(resource_after["pagefile_pages_in_bytes"])
                - int(resource_before["pagefile_pages_in_bytes"]),
            ),
            "pagefile_pages_out_delta_bytes": max(
                0,
                int(resource_after["pagefile_pages_out_bytes"])
                - int(resource_before["pagefile_pages_out_bytes"]),
            ),
            "orphan_worker_pids": orphan_pids,
            "parity_status": "PASS" if not parity_drift else "FAIL",
            "parity_drift": parity_drift,
            "economic_projection_sha256_by_ordinal": projection_hashes,
            "diagnostic_result_file_sha256_by_ordinal": result_file_hashes,
        }
    finally:
        resolved_scratch = scratch_root.resolve()
        if not resolved_scratch.is_relative_to(scratch_root.parent.resolve()):
            raise RuntimeError("benchmark scratch root escape")
        if resolved_scratch.exists():
            shutil.rmtree(resolved_scratch)


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(f"checkpoint benchmark is authorized only on {AUTHORIZED_HOST}")
    original_root = args.original_output_root.resolve()
    output_root = args.output_root.resolve()
    if not original_root.is_dir() or output_root.exists():
        raise RuntimeError("checkpoint benchmark root state is invalid")
    freeze_root = original_root / "prefinancial_freeze_stage01"
    run_root = original_root / "stage01_run"
    schedules, input_binding, contract = _reconstruct_checkpoint(
        freeze_root=freeze_root,
        run_root=run_root,
        checkpoint_index=2,
    )
    if [int(row["main_record_ordinal"]) for row in schedules] != list(range(16, 24)):
        raise RuntimeError("checkpoint_003 ordinal reconstruction drift")
    old_record_root = run_root / "inflight" / "checkpoint_003" / "records"
    baseline: dict[int, dict[str, Any]] = {}
    for schedule in schedules:
        ordinal = int(schedule["main_record_ordinal"])
        baseline[ordinal] = engine._verify_record(
            old_record_root / f"record_{ordinal:04d}.json",
            expected_input_hash=str(input_binding["input_binding_sha256"]),
            schedule=schedule,
        )
    execution_contract = args.execution_contract.resolve()
    train_field_root = Path(str(contract["accepted_field_manifest_path"])).resolve().parent
    train_price_root = args.train_price_root.resolve()
    execution_payload = engine._read_json(execution_contract)
    price_manifest, price_manifest_path = engine.phase_b._validate_phase_b_execution_price_sidecar(
        train_price_root,
        split_manifest_sha256=str(execution_payload["split_manifest_sha256"]),
        expected_manifest_file_sha256=str(input_binding["train_price_manifest_sha256"]),
    )
    windows = tuple(
        dict(row)
        for row in engine._read_json(
            Path(str(contract["phase_b_freeze_root"])) / "phase_b_run_contract.json"
        )["development_subwindows"]
    )
    checkpoint_columns = engine._checkpoint_field_columns(schedules)
    initializer_args = (
        str(execution_contract),
        str(train_field_root),
        str(train_price_root),
        price_manifest,
        str(price_manifest_path),
        str(contract["registry_path"]),
        str(input_binding["input_binding_sha256"]),
        windows,
        str(contract["accepted_field_manifest_file_sha256"]),
        str(contract["accepted_field_manifest_payload_sha256"]),
        checkpoint_columns,
    )
    output_root.mkdir(parents=True)
    schedule_path = _write_jsonl(output_root / "checkpoint_003_schedule.jsonl", schedules)
    results = [
        _benchmark_worker_count(
            worker_count=worker_count,
            schedules=schedules,
            baseline=baseline,
            scratch_root=output_root / f"worker_{worker_count}_scratch",
            initializer_args=initializer_args,
        )
        for worker_count in args.worker_counts
    ]
    if any(result["parity_status"] != "PASS" for result in results):
        raise RuntimeError("checkpoint benchmark economic parity failed")
    chosen = max(
        results,
        key=lambda row: (
            float(row["records_per_hour"]),
            int(row["minimum_commit_headroom_bytes"]),
        ),
    )
    body = {
        "schema_version": "cn_program_optimizer_checkpoint_resource_benchmark_v1",
        "status": "PASS",
        "original_output_root": str(original_root),
        "checkpoint_builder_repo_sha": str(input_binding["runner_repo_sha"]),
        "checkpoint_id": "checkpoint_003",
        "schedule_file": str(schedule_path),
        "schedule_file_sha256": engine._sha256(schedule_path),
        "schedule_sha256": stable_hash(schedules),
        "record_ordinals": list(range(16, 24)),
        "field_column_count": len(checkpoint_columns),
        "field_columns_sha256": stable_hash(list(checkpoint_columns)),
        "benchmark_results": results,
        "selected_checkpoint_worker_cap": int(chosen["worker_count"]),
        "selection_rule": "HIGHEST_OBSERVED_RECORDS_PER_HOUR_SUBJECT_TO_PARITY_AND_RESOURCE_SAFETY",
        "baseline_inflight_results_used_for_parity_only": True,
        "baseline_inflight_results_reused_for_tournament": False,
        "diagnostic_financial_results_reusable": False,
        "diagnostic_scratch_records_retained": False,
        "formal_tournament_records_closed": 0,
        "checkpoint_closed": False,
        "optimizer_tell_count": 0,
        "project_control": "NOT_REQUESTED",
        "recovery": "NOT_RUN",
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    body["benchmark_payload_sha256"] = stable_hash(body)
    return engine._read_json(_write_json(output_root / "benchmark_summary.json", body))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--original-output-root", required=True, type=Path)
    parser.add_argument("--execution-contract", required=True, type=Path)
    parser.add_argument("--train-price-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument(
        "--worker-counts", nargs="+", type=int, choices=engine.CHECKPOINT_WORKER_CHOICES,
        default=[4, 8],
    )
    args = parser.parse_args(argv)
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
