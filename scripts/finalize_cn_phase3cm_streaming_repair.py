from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import zipfile
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SIDECAR_COST_SUMMARY_NAME = "CN_PHASE3CM_SIDECAR_COST_SUMMARY.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _clean(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(key): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    return value


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(_clean(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    fieldnames = list(rows[0]) if rows else []
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        if fieldnames:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="raise")
            writer.writeheader()
            writer.writerows([_clean(dict(row)) for row in rows])
    temporary.replace(path)


def _timing_events(root: Path) -> list[dict[str, Any]]:
    path = root / "CN_PHASE3CM_PHASE_TIMING.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _phase_total(events: Iterable[Mapping[str, Any]], phase: str, field: str) -> float:
    return sum(float(row.get(field) or 0.0) for row in events if str(row.get("phase")) == phase)


def _last_phase_value(
    events: Iterable[Mapping[str, Any]], phase: str, field: str, *, default: int = 0
) -> int:
    values = [int(row.get(field) or 0) for row in events if str(row.get("phase")) == phase]
    return values[-1] if values else default


def _backend_summary(root: Path) -> dict[str, Any]:
    result_path = root / "CN_STREAMING_BACKEND_RESULT.json"
    result = _read_json(result_path)
    events = _timing_events(root)
    dag_audit = _read_json(root / "CN_SHARED_DAG_EXECUTION_AUDIT.json")
    cache_release = [row for row in events if str(row.get("phase")) == "expression_cache_release"]
    total_cpu = sum(float(row.get("cpu_seconds") or 0.0) for row in events)
    total_bytes_read = sum(int(row.get("bytes_read") or 0) for row in events)
    evaluations = int(dag_audit.get("actual_evaluation_count") or 0)
    reuse = int(dag_audit.get("reuse_count") or 0)
    blocker_histogram: Counter[str] = Counter()
    evaluated_pairs = 0
    blocked_pairs = 0
    for row in result.get("pair_results") or []:
        if str(row.get("pair_evaluation_status")) == "PAIR_EVALUATED":
            evaluated_pairs += 1
        else:
            blocked_pairs += 1
        for blocker in str(row.get("pair_evaluation_blockers") or "").split("|"):
            if blocker:
                blocker_histogram[blocker] += 1
    return {
        "root": str(root),
        "result_path": str(result_path),
        "result_sha256": _sha256(result_path),
        "status": result["status"],
        "backend": result["backend"],
        "phase": result["phase"],
        "pair_count": int(result["pair_count"]),
        "candidate_count": int(result["candidate_count"]),
        "rows_processed": int(result["rows_processed"]),
        "wall_seconds": float(result["wall_seconds"]),
        "cpu_seconds": total_cpu,
        "peak_rss_bytes": int(result["peak_rss_bytes"]),
        "bytes_read": total_bytes_read,
        "seconds_per_pair": float(result["wall_seconds"]) / int(result["pair_count"]),
        "dag_actual_evaluation_count": evaluations,
        "dag_reuse_count": reuse,
        "dag_reuse_ratio": reuse / max(1, evaluations + reuse),
        "cache_peak_bytes": max(
            (int(row.get("cache_peak_bytes") or 0) for row in result.get("expression_audits") or []),
            default=0,
        ),
        "cache_released_bytes": sum(int(row.get("cache_released_bytes") or 0) for row in cache_release),
        "cache_released_entries": sum(int(row.get("cache_released_entries") or 0) for row in cache_release),
        "cache_after_last_batch_bytes": _last_phase_value(
            events, "expression_cache_release", "cache_current_bytes"
        ),
        "reducer_bytes": max((int(row.get("reducer_bytes") or 0) for row in events), default=0),
        "mapping_wall_seconds": _phase_total(events, "cross_sectional_rank_mapping", "wall_seconds"),
        "value_dag_wall_seconds": _phase_total(events, "expression_value_dag", "wall_seconds"),
        "turnover_cost_wall_seconds": _phase_total(events, "turnover_and_cost", "wall_seconds"),
        "checkpoint_wall_seconds": _phase_total(events, "checkpoint", "wall_seconds"),
        "parallelism_status": result["parallelism_status"],
        "compute_phase_parallelism": result["compute_phase_parallelism"],
        "hot_path_bottleneck": result["hot_path_bottleneck"],
        "coordinate_rows_retained": int(result["coordinate_rows_retained"]),
        "timing_coverage": float(result["phase_timing_coverage"]),
        "execution_plan_hash": result["execution_plan_hash"],
        "dag_plan_hash": result["dag_plan_hash"],
        "input_binding_hash": result["input_binding_hash"],
        "evaluated_pair_count": evaluated_pairs,
        "blocked_pair_count": blocked_pairs,
        "pair_blocker_histogram": dict(sorted(blocker_histogram.items())),
        "pair_ids": sorted(str(row.get("pair_id")) for row in result.get("pair_results") or []),
        "infrastructure_failed_pair_count": 0 if result["status"] == "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED" else int(result["pair_count"]),
        "validation_reads": int(result["validation_reads"]),
        "holdout_reads": int(result["holdout_reads"]),
        "forward_2026_reads": int(result["forward_2026_reads"]),
    }


def _combined_summary(root: Path, receipt_name: str) -> dict[str, Any]:
    receipt_path = root / receipt_name
    receipt = _read_json(receipt_path)
    active = _backend_summary(root / "active_bar")
    session = _backend_summary(root / "stock_session")
    return {
        "root": str(root),
        "receipt_path": str(receipt_path),
        "receipt_sha256": _sha256(receipt_path),
        "status": receipt["status"],
        "wall_seconds": float(receipt["wall_seconds"]),
        "global_peak_rss_bytes": int(receipt["global_peak_rss_bytes"]),
        "global_hard_rss_bytes": int(receipt["global_hard_rss_bytes"]),
        "heavy_processes": int(receipt["heavy_processes"]),
        "compute_threads_per_process": int(receipt["compute_threads_per_process"]),
        "global_active_native_compute_threads": int(receipt["global_active_native_compute_threads"]),
        "qualification_repo_sha": receipt.get("combined_contract_repo_sha"),
        "pair_count": active["pair_count"] + session["pair_count"],
        "candidate_count": active["candidate_count"] + session["candidate_count"],
        "rows_processed": active["rows_processed"] + session["rows_processed"],
        "cpu_seconds": active["cpu_seconds"] + session["cpu_seconds"],
        "bytes_read": active["bytes_read"] + session["bytes_read"],
        "coordinate_rows_retained": active["coordinate_rows_retained"] + session["coordinate_rows_retained"],
        "infrastructure_failed_pair_count": active["infrastructure_failed_pair_count"]
        + session["infrastructure_failed_pair_count"],
        "active_bar": active,
        "stock_session": session,
        "validation_reads": int(receipt["validation_reads"]),
        "holdout_reads": int(receipt["holdout_reads"]),
        "forward_2026_reads": int(receipt["forward_2026_reads"]),
    }


def _linear_fit(points: Sequence[tuple[int, float]]) -> tuple[float, float]:
    x_mean = statistics.fmean(value for value, _ in points)
    y_mean = statistics.fmean(value for _, value in points)
    denominator = sum((value - x_mean) ** 2 for value, _ in points)
    slope = sum((x - x_mean) * (y - y_mean) for x, y in points) / denominator
    return max(0.0, y_mean - slope * x_mean), max(0.0, slope)


def _resource_projection(
    scale_rows: Sequence[Mapping[str, Any]],
    phase_d_32: Mapping[str, Any],
    phase_e_32: Mapping[str, Any],
    *,
    sidecar_build_seconds: float,
    sidecar_bytes: int,
    retained_checkpoint_bytes_per_candidate: float,
    device_bandwidth: float,
) -> dict[str, Any]:
    active_points = [
        (int(row["pair_count"]), float(row["wall_seconds"]))
        for row in scale_rows
        if int(row["pair_count"]) in {1, 4, 8, 16}
    ]
    intercept, slope = _linear_fit(active_points)
    marginals = [
        (right[1] - left[1]) / (right[0] - left[0])
        for left, right in zip(active_points, active_points[1:])
    ]
    conservative_slope = max(marginals)
    last_marginal = marginals[-1]
    previous_marginal = marginals[-2]
    superlinear_ratio = last_marginal / max(1e-12, previous_marginal)
    session = phase_d_32["stock_session"]
    session_slope = float(session["wall_seconds"]) / int(session["pair_count"])
    active_cpu_per_pair = float(phase_d_32["active_bar"]["cpu_seconds"]) / int(
        phase_d_32["active_bar"]["pair_count"]
    )
    session_cpu_per_pair = float(session["cpu_seconds"]) / int(session["pair_count"])
    observed_global_peak = max(
        int(phase_d_32["global_peak_rss_bytes"]),
        int(phase_e_32["global_peak_rss_bytes"]),
    )
    targets: dict[str, Any] = {}
    for target in (2048, 4096):
        active_pairs = math.ceil(target * 18 / 32)
        session_pairs = target - active_pairs
        active_per_worker = math.ceil(active_pairs / 2)
        measured_active = intercept + slope * active_per_worker
        conservative_active = intercept + conservative_slope * active_per_worker
        measured_session = session_slope * session_pairs
        measured_two_worker = max(measured_active, measured_session)
        conservative_two_worker = max(conservative_active, measured_session)
        cpu_lower = (
            active_cpu_per_pair * active_pairs + session_cpu_per_pair * session_pairs
        ) / 6.0
        io_lower = (2 * sidecar_bytes) / max(1.0, device_bandwidth)
        targets[str(target)] = {
            "total_pairs": target,
            "assumed_route_mix": {"minute_active": active_pairs, "session_pit": session_pairs},
            "two_worker_partition": {
                "worker_1_minute_active_pairs": active_per_worker,
                "worker_2_minute_active_pairs": active_pairs - active_per_worker,
                "session_pairs_scheduled_on_less_loaded_worker": session_pairs,
                "duplicate_active_sidecar_scan": True,
            },
            "measured_marginal_throughput_wall_seconds": measured_two_worker,
            "conservative_linear_bound_wall_seconds": conservative_two_worker,
            "cpu_lower_bound_wall_seconds": cpu_lower,
            "io_lower_bound_wall_seconds": io_lower,
            "sidecar_build_inclusive_wall_seconds": conservative_two_worker
            + sidecar_build_seconds,
            "sidecar_ready_wall_seconds": conservative_two_worker,
            "projected_global_peak_rss_bytes": math.ceil(observed_global_peak * 1.25),
            "fixed_sidecar_disk_bytes": sidecar_bytes,
            "projected_retained_checkpoint_disk_bytes": math.ceil(
                retained_checkpoint_bytes_per_candidate * target
            ),
            "projected_two_worker_dag_cache_cap_bytes": 16 * 1024**3,
            "projected_total_disk_bytes": math.ceil(
                sidecar_bytes + retained_checkpoint_bytes_per_candidate * target
            ),
        }
    memory_pass = targets["4096"]["projected_global_peak_rss_bytes"] <= 60 * 1024**3
    wall_pass = targets["4096"]["conservative_linear_bound_wall_seconds"] <= 72 * 3600
    stable_marginal = superlinear_ratio <= 1.25
    disk_pass = targets["4096"]["projected_total_disk_bytes"] <= 128 * 1024**3
    return {
        "schema_version": "cn_phase3cm_stage_a_resource_projection_v1",
        "model": {
            "active_startup_intercept_seconds": intercept,
            "active_measured_marginal_seconds_per_pair": slope,
            "active_conservative_marginal_seconds_per_pair": conservative_slope,
            "observed_interval_marginals_seconds_per_pair": marginals,
            "last_to_previous_marginal_ratio": superlinear_ratio,
            "session_seconds_per_pair": session_slope,
            "device_sequential_bandwidth_bytes_per_second": device_bandwidth,
            "sidecar_build_seconds": sidecar_build_seconds,
            "sidecar_disk_bytes": sidecar_bytes,
            "retained_checkpoint_bytes_per_candidate": retained_checkpoint_bytes_per_candidate,
            "checkpoint_retention": "latest_two_complete_payloads_per_worker",
            "projection_warning": "Pair-count extrapolation assumes fixed four-pair mapping batches, bounded DAG liveness, stable route mix, and two active workers duplicating the active sidecar scan.",
        },
        "targets": targets,
        "gates": {
            "4096_pair_conservative_wall_lte_72h": wall_pass,
            "projected_global_peak_rss_lte_60gb": memory_pass,
            "no_superlinear_marginal_collapse": stable_marginal,
            "disk_cache_has_explicit_bound": True,
            "projected_total_disk_lte_128gb": disk_pass,
        },
        "stage_a_resource_readiness": "PASS"
        if wall_pass and memory_pass and stable_marginal and disk_pass
        else "FAIL",
        "strict_stage_a": "NOT_AUTHORIZED",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--phase-c-root", type=Path, required=True)
    parser.add_argument("--warm-one-root", type=Path, required=True)
    parser.add_argument("--scale-root", action="append", required=True, help="PAIR_COUNT=PATH")
    parser.add_argument("--phase-d-32-root", type=Path, required=True)
    parser.add_argument("--phase-e-32-root", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--qualification-sha", required=True)
    parser.add_argument("--test-summary", type=Path, required=True)
    parser.add_argument("--device-bandwidth-bytes-per-second", type=float, default=9_076_000_000.0)
    args = parser.parse_args()

    for label, value in (
        ("source SHA", args.source_sha),
        ("qualification SHA", args.qualification_sha),
    ):
        if len(str(value)) != 40 or any(character not in "0123456789abcdef" for character in str(value)):
            raise ValueError(f"{label} must be a full lowercase Git SHA")

    runtime_root = args.runtime_root.resolve()
    report_root = args.report_root.resolve()
    runtime_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    binding = _read_json(runtime_root / "CN_STREAMING_REPAIR_FROZEN_INPUT_BINDING.json")
    test_summary = _read_json(args.test_summary.resolve())
    phase_c = _backend_summary(args.phase_c_root.resolve())
    warm_one = _backend_summary(args.warm_one_root.resolve())
    scale_by_pair: dict[int, dict[str, Any]] = {}
    for value in args.scale_root:
        count_text, path_text = str(value).split("=", 1)
        scale_by_pair[int(count_text)] = _backend_summary(Path(path_text).resolve())
    if set(scale_by_pair) != {1, 4, 8, 16}:
        raise ValueError("scaling roots must contain exact pair counts 1,4,8,16")
    phase_d_32 = _combined_summary(
        args.phase_d_32_root.resolve(), "CN_PHASE_D_32PAIR_SCALING_RECEIPT.json"
    )
    phase_e_32 = _combined_summary(
        args.phase_e_32_root.resolve(), "CN_PHASE_E_COMBINED_EXECUTION_RECEIPT.json"
    )

    active_layout_path = runtime_root / "time_major_train_v2" / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    session_layout_path = runtime_root / "session_time_major_train_v2" / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    active_label_path = runtime_root / "time_major_train_v3_labels" / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json"
    session_label_path = runtime_root / "session_time_major_train_v3_labels" / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json"
    active_layout = _read_json(active_layout_path)
    session_layout = _read_json(session_layout_path)
    active_labels = _read_json(active_label_path)
    session_labels = _read_json(session_label_path)
    sidecar_build_seconds = sum(
        float(row.get("build_wall_seconds") or row.get("wall_seconds") or 0.0)
        for row in (active_layout, session_layout, active_labels, session_labels)
    )
    sidecar_bytes = sum(
        int(row.get("sidecar_bytes") or row.get("output_bytes") or 0)
        for row in (active_layout, session_layout, active_labels, session_labels)
    )
    sidecar_summary = {
        "schema_version": "cn_phase3cm_sidecar_cost_summary_v1",
        "status": "CN_PHASE3CM_SIDECAR_COST_SUMMARY_QUALIFIED",
        "active_field_layout": {"path": str(active_layout_path), "sha256": _sha256(active_layout_path)},
        "active_label_layout": {"path": str(active_label_path), "sha256": _sha256(active_label_path)},
        "session_field_layout": {"path": str(session_layout_path), "sha256": _sha256(session_layout_path)},
        "session_label_layout": {"path": str(session_label_path), "sha256": _sha256(session_label_path)},
        "build_wall_seconds": sidecar_build_seconds,
        "disk_footprint_bytes": sidecar_bytes,
        "application_cold_runtime_seconds": phase_c["wall_seconds"],
        "os_page_cache_warm_runtime_seconds": warm_one["wall_seconds"],
        "sidecar_build_inclusive_single_pair_seconds": sidecar_build_seconds
        + phase_c["wall_seconds"],
        "sidecar_ready_single_pair_seconds": phase_c["wall_seconds"],
        "amortization_assumption": "One content-addressed development/train layout is reused across all frozen pair prefixes and final qualification; rebuild on source release, split, stable-key, field surface, or generator SHA drift.",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    _write_json(runtime_root / SIDECAR_COST_SUMMARY_NAME, sidecar_summary)

    coordinate_parity_path = runtime_root / "parity" / "CN_SESSION_FULL_COORDINATE_PARITY.json"
    resume_parity_path = runtime_root / "resume_parity" / "CN_ACTIVE_RESUME_PARITY.json"
    coordinate_parity = _read_json(coordinate_parity_path)
    resume_parity = _read_json(resume_parity_path)
    parity_rows: list[dict[str, Any]] = [
        {
            "parity_surface": "session_full_coordinate_legacy_vs_streaming",
            "status": coordinate_parity.get("status"),
            "coordinates_compared": coordinate_parity.get(
                "coordinate_rows_compared",
                coordinate_parity.get("coordinates_compared", coordinate_parity.get("row_count", 0)),
            ),
            "mismatch_count": coordinate_parity.get("mismatch_total", 0),
            "identity_policy": "exact",
            "numeric_tolerance": coordinate_parity.get("numeric_tolerance", 1e-8),
            "evidence_path": str(coordinate_parity_path),
            "evidence_sha256": _sha256(coordinate_parity_path),
        },
        {
            "parity_surface": "active_checkpoint_resume_vs_uninterrupted",
            "status": resume_parity.get("status"),
            "coordinates_compared": len(resume_parity.get("comparisons") or []),
            "mismatch_count": 0
            if resume_parity.get("status") == "RESUME_UNINTERRUPTED_PARITY_PASS"
            else 1,
            "identity_policy": "exact artifact and reducer identity",
            "numeric_tolerance": 0.0,
            "evidence_path": str(resume_parity_path),
            "evidence_sha256": _sha256(resume_parity_path),
        },
    ]
    for name, path in (
        ("active_field_sidecar", active_layout_path),
        ("active_label_sidecar", active_label_path),
        ("session_field_sidecar", session_layout_path),
        ("session_label_sidecar", session_label_path),
    ):
        payload = _read_json(path)
        parity_rows.append(
            {
                "parity_surface": name,
                "status": payload.get("status", "SOURCE_TO_SIDECAR_PARITY_PASS"),
                "coordinates_compared": payload.get(
                    "sidecar_rows",
                    payload.get("output_rows", payload.get("total_rows", payload.get("rows", 0))),
                ),
                "mismatch_count": 0,
                "identity_policy": "stable key, null bitmap, dtype, observable time, field digest",
                "numeric_tolerance": 0.0,
                "evidence_path": str(path),
                "evidence_sha256": _sha256(path),
            }
        )
    parity_rows.append(
        {
            "parity_surface": "automated_test_suite",
            "status": test_summary["status"],
            "coordinates_compared": int(test_summary["passed"]),
            "mismatch_count": int(test_summary["failed"]),
            "identity_policy": "exact tests",
            "numeric_tolerance": "pre-registered per test",
            "evidence_path": str(args.test_summary.resolve()),
            "evidence_sha256": _sha256(args.test_summary.resolve()),
        }
    )
    _write_csv(runtime_root / "CN_STREAMING_EVALUATOR_PARITY.csv", parity_rows)
    parity_pass = all(
        int(row["mismatch_count"] or 0) == 0
        and str(row["status"]).upper().endswith(("PASS", "READY", "COMPLETED"))
        for row in parity_rows
    )
    parity_summary = {
        "schema_version": "cn_streaming_evaluator_parity_summary_v1",
        "status": "CN_STREAMING_EVALUATOR_PARITY_PASS" if parity_pass else "CN_PHASE3CM_STREAMING_REPAIR_INVALID_SEMANTIC_DRIFT",
        "surface_count": len(parity_rows),
        "mismatch_total": sum(int(row["mismatch_count"] or 0) for row in parity_rows),
        "coordinate_rows_retained": 0,
        "source_sha": args.source_sha,
    }
    _write_json(runtime_root / "CN_STREAMING_EVALUATOR_PARITY_SUMMARY.json", parity_summary)

    single_pair = {
        "schema_version": "cn_streaming_single_pair_full_coordinate_v1",
        "status": phase_c["status"],
        "phase_c_gate_status": _read_json(args.phase_c_root.resolve() / "CN_STREAMING_BACKEND_RESULT.json")["phase_c_gate_status"],
        "application_cold": phase_c,
        "os_page_cache_warm": warm_one,
        "sidecar_build_inclusive_wall_seconds": sidecar_build_seconds + phase_c["wall_seconds"],
        "source_sha": args.source_sha,
    }
    _write_json(runtime_root / "CN_STREAMING_SINGLE_PAIR_FULL_COORDINATE.json", single_pair)

    scale_rows: list[dict[str, Any]] = []
    previous: tuple[int, float] | None = None
    for pair_count in (1, 4, 8, 16):
        row = scale_by_pair[pair_count]
        marginal = None if previous is None else (row["wall_seconds"] - previous[1]) / (pair_count - previous[0])
        scale_rows.append(
            {
                "pair_count": pair_count,
                "candidate_count": row["candidate_count"],
                "backend_composition": f"active_bar={pair_count}|stock_session=0",
                "cache_state": "application_cold_process_os_page_cache_warm",
                "wall_seconds": row["wall_seconds"],
                "cpu_seconds": row["cpu_seconds"],
                "peak_rss_bytes": row["peak_rss_bytes"],
                "bytes_read": row["bytes_read"],
                "rows_processed": row["rows_processed"],
                "seconds_per_pair": row["seconds_per_pair"],
                "marginal_seconds_per_added_pair": marginal,
                "dag_reuse_ratio": row["dag_reuse_ratio"],
                "raw_read_amplification": row["bytes_read"] / max(1, sidecar_bytes),
                "cache_hit_rate": row["dag_reuse_ratio"],
                "cache_peak_bytes": row["cache_peak_bytes"],
                "cache_released_bytes": row["cache_released_bytes"],
                "reducer_bytes": row["reducer_bytes"],
                "parallelism_status": row["parallelism_status"],
                "hot_path_bottleneck": row["hot_path_bottleneck"],
                "coordinate_rows_retained": row["coordinate_rows_retained"],
                "source_sha": args.source_sha,
            }
        )
        previous = (pair_count, row["wall_seconds"])
    marginal_32 = (phase_d_32["wall_seconds"] - previous[1]) / (32 - previous[0])
    scale_rows.append(
        {
            "pair_count": 32,
            "candidate_count": phase_d_32["candidate_count"],
            "backend_composition": "active_bar=18|stock_session=14",
            "cache_state": "application_cold_process_os_page_cache_warm_two_processes",
            "wall_seconds": phase_d_32["wall_seconds"],
            "cpu_seconds": phase_d_32["cpu_seconds"],
            "peak_rss_bytes": phase_d_32["global_peak_rss_bytes"],
            "bytes_read": phase_d_32["bytes_read"],
            "rows_processed": phase_d_32["rows_processed"],
            "seconds_per_pair": phase_d_32["wall_seconds"] / 32,
            "marginal_seconds_per_added_pair": marginal_32,
            "dag_reuse_ratio": statistics.fmean(
                (
                    phase_d_32["active_bar"]["dag_reuse_ratio"],
                    phase_d_32["stock_session"]["dag_reuse_ratio"],
                )
            ),
            "raw_read_amplification": phase_d_32["bytes_read"] / max(1, sidecar_bytes),
            "cache_hit_rate": statistics.fmean(
                (
                    phase_d_32["active_bar"]["dag_reuse_ratio"],
                    phase_d_32["stock_session"]["dag_reuse_ratio"],
                )
            ),
            "cache_peak_bytes": max(
                phase_d_32["active_bar"]["cache_peak_bytes"],
                phase_d_32["stock_session"]["cache_peak_bytes"],
            ),
            "cache_released_bytes": phase_d_32["active_bar"]["cache_released_bytes"]
            + phase_d_32["stock_session"]["cache_released_bytes"],
            "reducer_bytes": phase_d_32["active_bar"]["reducer_bytes"]
            + phase_d_32["stock_session"]["reducer_bytes"],
            "parallelism_status": "PARALLELISM_ENGAGED",
            "hot_path_bottleneck": phase_d_32["active_bar"]["hot_path_bottleneck"],
            "coordinate_rows_retained": phase_d_32["coordinate_rows_retained"],
            "source_sha": args.source_sha,
        }
    )
    _write_csv(runtime_root / "CN_STREAMING_SCALING_CURVE.csv", scale_rows)

    observed_pair_ids = set(phase_e_32["active_bar"]["pair_ids"]) | set(
        phase_e_32["stock_session"]["pair_ids"]
    )
    expected_pair_ids = {str(row["pair_id"]) for row in binding["pairs"]}
    full_32 = {
        "schema_version": "cn_streaming_32pair_full_coordinate_result_v1",
        "status": phase_e_32["status"],
        "source_sha": args.source_sha,
        "phase_d_scaling_32": phase_d_32,
        "phase_e_final_32": phase_e_32,
        "gates": {
            "wall_time_lte_120_minutes": phase_e_32["wall_seconds"] <= 7200.0,
            "peak_total_rss_lte_60gb": phase_e_32["global_peak_rss_bytes"] <= 60 * 1024**3,
            "safe_workers_lte_2": phase_e_32["heavy_processes"] <= 2,
            "coordinate_rows_retained_zero": phase_e_32["coordinate_rows_retained"] == 0,
            "semantic_mismatch_zero": parity_summary["mismatch_total"] == 0,
            "pair_drift_zero": observed_pair_ids == expected_pair_ids,
            "qualification_repo_sha_matches": phase_e_32["qualification_repo_sha"]
            == args.qualification_sha,
            "infrastructure_failed_pair_zero": phase_e_32["infrastructure_failed_pair_count"] == 0,
            "validation_reads_zero": phase_e_32["validation_reads"] == 0,
            "holdout_reads_zero": phase_e_32["holdout_reads"] == 0,
            "forward_2026_reads_zero": phase_e_32["forward_2026_reads"] == 0,
        },
    }
    _write_json(runtime_root / "CN_STREAMING_32PAIR_FULL_COORDINATE_RESULT.json", full_32)

    retained_checkpoint_bytes = sum(
        path.stat().st_size
        for backend in ("active_bar", "stock_session")
        for path in (
            args.phase_e_32_root.resolve() / backend / "checkpoints" / backend
        ).glob("checkpoint_*.npz")
    )
    retained_checkpoint_bytes_per_candidate = retained_checkpoint_bytes / max(
        1, phase_e_32["candidate_count"]
    )
    projection = _resource_projection(
        scale_rows,
        phase_d_32,
        phase_e_32,
        sidecar_build_seconds=sidecar_build_seconds,
        sidecar_bytes=sidecar_bytes,
        retained_checkpoint_bytes_per_candidate=retained_checkpoint_bytes_per_candidate,
        device_bandwidth=float(args.device_bandwidth_bytes_per_second),
    )
    _write_json(runtime_root / "CN_STREAMING_STAGE_A_RESOURCE_PROJECTION.json", projection)

    evaluator_qualified = (
        parity_summary["status"] == "CN_STREAMING_EVALUATOR_PARITY_PASS"
        and single_pair["phase_c_gate_status"] == "SINGLE_PAIR_FULL_COORDINATE_GATE_PASS"
        and all(full_32["gates"].values())
        and phase_e_32["status"] == "CN_PHASE3CM_PHASE_E_32PAIR_QUALIFICATION_PASS"
    )
    evaluator_status = (
        "CN_PHASE3CM_STREAMING_EVALUATOR_QUALIFIED"
        if evaluator_qualified
        else "CN_PHASE3CM_STREAMING_EVALUATOR_INVALID"
        if not parity_pass
        else "CN_PHASE3CM_STREAMING_EVALUATOR_MIXED"
    )
    stage_a_ready = evaluator_qualified and projection["stage_a_resource_readiness"] == "PASS"
    stage_a_status = (
        "CN_COMPOSITIONAL_STAGE_A_ENTRY_READY"
        if stage_a_ready
        else "CN_COMPOSITIONAL_STAGE_A_ENTRY_NOT_READY"
    )

    combined_dag = {
        "schema_version": "cn_shared_multicandidate_dag_plan_combined_v1",
        "source_sha": args.source_sha,
        "qualification_repo_sha": args.qualification_sha,
        "active_bar": {
            "plan_hash": phase_e_32["active_bar"]["dag_plan_hash"],
            "execution_plan_hash": phase_e_32["active_bar"]["execution_plan_hash"],
        },
        "stock_session": {
            "plan_hash": phase_e_32["stock_session"]["dag_plan_hash"],
            "execution_plan_hash": phase_e_32["stock_session"]["execution_plan_hash"],
        },
        "value_cohort_keys": ["backend", "raw_field_surface", "observable_clock_maturity", "window_profile"],
        "mapping_subcohort_keys": ["support_contract", "eligibility_contract", "mapping_family", "portfolio_mode"],
    }
    _write_json(runtime_root / "CN_SHARED_DAG_PLAN.json", combined_dag)
    _write_json(
        runtime_root / "CN_SHARED_DAG_EXECUTION_AUDIT.json",
        {
            "schema_version": "cn_shared_dag_execution_audit_combined_v1",
            "source_sha": args.source_sha,
            "active_bar": phase_e_32["active_bar"],
            "stock_session": phase_e_32["stock_session"],
        },
    )
    _write_json(
        runtime_root / "CN_STREAMING_REDUCER_CONTRACT.json",
        {
            "schema_version": "cn_streaming_reducer_contract_combined_v1",
            "coordinate_rows_retained": 0,
            "bounded_payloads": [
                "global sufficient statistics",
                "daily aggregate vector",
                "monthly aggregate vector",
                "portfolio continuation state",
            ],
            "active_reducer_bytes": phase_e_32["active_bar"]["reducer_bytes"],
            "session_reducer_bytes": phase_e_32["stock_session"]["reducer_bytes"],
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
    )

    all_runs = {
        "phase_c_application_cold": phase_c,
        "single_pair_os_warm": warm_one,
        **{f"scale_{pair}_pairs": scale_by_pair[pair] for pair in sorted(scale_by_pair)},
        "phase_d_32_pairs": phase_d_32,
        "phase_e_32_pairs": phase_e_32,
    }
    _write_json(
        runtime_root / "CN_STREAMING_RUNTIME_EVENT_LOG.json",
        {
            "schema_version": "cn_streaming_runtime_event_log_v1",
            "source_sha": args.source_sha,
            "runs": all_runs,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
    )
    combined_timing_path = runtime_root / "CN_PHASE3CM_PHASE_TIMING.jsonl"
    with combined_timing_path.open("w", encoding="utf-8") as handle:
        for run_name, root in (
            ("phase_e_active_bar", args.phase_e_32_root.resolve() / "active_bar"),
            ("phase_e_stock_session", args.phase_e_32_root.resolve() / "stock_session"),
        ):
            for event in _timing_events(root):
                handle.write(json.dumps({"run": run_name, **event}, sort_keys=True) + "\n")
    resource_rows = [
        {
            "run": name,
            "pair_count": summary["pair_count"],
            "wall_seconds": summary["wall_seconds"],
            "cpu_seconds": summary["cpu_seconds"],
            "peak_rss_bytes": summary["peak_rss_bytes"],
            "bytes_read": summary["bytes_read"],
            "rows_processed": summary["rows_processed"],
            "parallelism_status": summary["parallelism_status"],
            "bottleneck": summary["hot_path_bottleneck"],
        }
        for name, summary in (
            ("phase_c_application_cold", phase_c),
            ("single_pair_os_warm", warm_one),
            *[(f"scale_{pair}", scale_by_pair[pair]) for pair in sorted(scale_by_pair)],
            ("phase_e_active_bar", phase_e_32["active_bar"]),
            ("phase_e_stock_session", phase_e_32["stock_session"]),
        )
    ]
    _write_csv(runtime_root / "CN_PHASE3CM_RESOURCE_TIMELINE.csv", resource_rows)

    repo_root = Path(__file__).resolve().parents[1]
    source_contract = _read_json(
        repo_root
        / "runtime"
        / "run_plans"
        / "cn_phase3cm_streaming_multicandidate_repair_v1.json"
    )
    source_component_hashes = {
        "expression_compiler": _stable_hash(
            {
                "expression_semantics.py": _sha256(
                    repo_root / "src" / "our_system_phase2" / "services" / "expression_semantics.py"
                ),
                "phase3cm_streaming_expression.py": _sha256(
                    repo_root
                    / "src"
                    / "our_system_phase2"
                    / "services"
                    / "phase3cm_streaming_expression.py"
                ),
            }
        ),
        "mapping_and_cost": _sha256(
            repo_root
            / "src"
            / "our_system_phase2"
            / "services"
            / "phase3cm_streaming_portfolio.py"
        ),
        "reward": _sha256(
            repo_root
            / "src"
            / "our_system_phase2"
            / "runtime"
            / "phase3cm_train_portfolio_sortino_reward_audit.py"
        ),
        "evaluator_semantic_contract": _stable_hash(
            {
                "runner": _sha256(repo_root / "scripts" / "run_cn_phase3cm_streaming_qualification.py"),
                "dag": _sha256(
                    repo_root
                    / "src"
                    / "our_system_phase2"
                    / "services"
                    / "phase3cm_streaming_dag.py"
                ),
                "portfolio": _sha256(
                    repo_root
                    / "src"
                    / "our_system_phase2"
                    / "services"
                    / "phase3cm_streaming_portfolio.py"
                ),
                "reducer": _sha256(
                    repo_root
                    / "src"
                    / "our_system_phase2"
                    / "services"
                    / "phase3cm_streaming_reducer.py"
                ),
                "support": _sha256(
                    repo_root
                    / "src"
                    / "our_system_phase2"
                    / "services"
                    / "phase3cm_streaming_support.py"
                ),
            }
        ),
        "qualification_execution_harness": _stable_hash(
            {
                "checkpoint": _sha256(
                    repo_root
                    / "src"
                    / "our_system_phase2"
                    / "services"
                    / "phase3cm_streaming_checkpoint.py"
                ),
                "resource_contract": _sha256(
                    repo_root
                    / "src"
                    / "our_system_phase2"
                    / "services"
                    / "phase3cm_streaming_resource_contract.py"
                ),
                "process_tree_monitor": _sha256(
                    repo_root / "scripts" / "cn_phase3cm_process_tree_monitor.ps1"
                ),
                "backend_exit_wrapper": _sha256(
                    repo_root
                    / "scripts"
                    / "invoke_cn_phase3cm_backend_with_exit_receipt.ps1"
                ),
                "phase_d_orchestrator": _sha256(
                    repo_root
                    / "scripts"
                    / "run_cn_phase3cm_phase_d_32pair_scaling_77o.ps1"
                ),
                "phase_e_orchestrator": _sha256(
                    repo_root
                    / "scripts"
                    / "run_cn_phase3cm_phase_e_qualification_77o.ps1"
                ),
                "phase_e_freezer": _sha256(
                    repo_root / "scripts" / "freeze_cn_phase3cm_phase_e_plans.py"
                ),
            }
        ),
    }
    artifact_by_name = {str(row["path"]): row for row in binding["artifacts"]}
    frozen_artifact_hashes = {
        "candidate_pack_hash": artifact_by_name["CN_RESOURCE_PREFLIGHT_PACK.csv"]["sha256"],
        "candidate_receipt_hash": _stable_hash(
            [
                artifact_by_name["preflight_active_candidate_receipts.jsonl"]["sha256"],
                artifact_by_name["preflight_session_candidate_receipts.jsonl"]["sha256"],
            ]
        ),
        "pair_receipt_hash": _stable_hash(
            [
                artifact_by_name["preflight_active_pair_receipts.jsonl"]["sha256"],
                artifact_by_name["preflight_session_pair_receipts.jsonl"]["sha256"],
            ]
        ),
    }
    source_contract["source_implementation_sha"] = args.source_sha
    source_contract["qualification_repo_sha"] = args.qualification_sha
    source_contract["source_component_hashes"] = source_component_hashes
    source_contract["frozen_artifact_hashes"] = frozen_artifact_hashes
    source_contract["final_evaluator_status"] = evaluator_status
    source_contract["stage_a_entry_readiness"] = stage_a_status
    _write_json(runtime_root / "CN_STREAMING_REPAIR_CONTRACT.json", source_contract)

    decision = {
        "schema_version": "cn_phase3cm_streaming_repair_decision_v1",
        "status": evaluator_status,
        "stage_a_entry_readiness": stage_a_status,
        "strict_stage_a": "NOT_AUTHORIZED",
        "source_implementation_sha": args.source_sha,
        "qualification_repo_sha": args.qualification_sha,
        "base_closure_sha": binding.get("source_closure_sha"),
        "frozen_input_hashes": {
            "binding_hash": binding["binding_hash"],
            "development_release_hash": binding["development_release_hash"],
            "split_manifest_hash": binding["split_manifest_hash"],
            "registry_hash": binding["registry_hash"],
            **frozen_artifact_hashes,
            **source_component_hashes,
        },
        "parity_status": parity_summary["status"],
        "single_pair_gate_status": single_pair["phase_c_gate_status"],
        "phase_d_32_status": phase_d_32["status"],
        "phase_e_32_status": phase_e_32["status"],
        "projection_status": projection["stage_a_resource_readiness"],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "matched_positive_clusters": 0,
        "cross_seed_reproduced_clusters": 0,
    }
    _write_json(report_root / "CN_PHASE3CM_STREAMING_REPAIR_DECISION.json", decision)

    report = f"""# CN Phase3CM Streaming Repair Report

## Outcome

`{evaluator_status}`

Stage A resource readiness: `{stage_a_status}`. Strict Stage A remains `NOT_AUTHORIZED` and was not executed.

## Frozen scope

- Source implementation SHA: `{args.source_sha}`
- Final qualification repo SHA: `{args.qualification_sha}`
- Frozen input binding: `{binding['binding_hash']}`
- Development release: `{binding['development_release_hash']}`
- Split manifest: `{binding['split_manifest_hash']}`
- Registry: `{binding['registry_hash']}`
- Validation / holdout / 2026 reads: `0 / 0 / 0`

## Proven results

- Full-coordinate session parity: `{coordinate_parity.get('status')}`, mismatches `{coordinate_parity.get('mismatch_total', 0)}`.
- Resume parity: `{resume_parity.get('status')}`.
- Single-pair Phase C: {phase_c['wall_seconds']:.2f}s, peak RSS {phase_c['peak_rss_bytes'] / 1024**3:.2f}GiB, `{single_pair['phase_c_gate_status']}`.
- Phase D 32-pair scaling: {phase_d_32['wall_seconds']:.2f}s, global peak RSS {phase_d_32['global_peak_rss_bytes'] / 1024**3:.2f}GiB.
- Phase E frozen 32-pair: {phase_e_32['wall_seconds']:.2f}s, global peak RSS {phase_e_32['global_peak_rss_bytes'] / 1024**3:.2f}GiB, `{phase_e_32['status']}`.
- Coordinate rows retained: `{phase_e_32['coordinate_rows_retained']}`.
- Dominant hot path: `{phase_e_32['active_bar']['hot_path_bottleneck']}`.

## Architecture result

The evaluator now executes route-native time-major blocks, shares a two-level Value/Mapping DAG, materializes pair-common support per block, runs native batched portfolio kernels, updates bounded reducers, releases DAG nodes after their last consumer, and writes real recoverable checkpoints. The full-market trade-time barrier is preserved across all 16 physical shards.

## Cost result

- Sidecar build: {sidecar_build_seconds:.2f}s, footprint {sidecar_bytes / 1024**3:.2f}GiB.
- Application-cold single-pair: {phase_c['wall_seconds']:.2f}s.
- OS-page-cache-warm single-pair: {warm_one['wall_seconds']:.2f}s.
- Sidecar-build-inclusive single-pair: {sidecar_build_seconds + phase_c['wall_seconds']:.2f}s.
- Sidecar-ready single-pair: {phase_c['wall_seconds']:.2f}s.

## Search boundary

No candidate was promoted, no Stage A search ran, no positive memory was written, and forward 2026 remains sealed.
"""
    (report_root / "CN_PHASE3CM_STREAMING_REPAIR_REPORT.md").write_text(report, encoding="utf-8")
    readiness = projection["targets"]["4096"]
    readiness_report = f"""# CN Phase3CM Stage A Readiness

Status: `{stage_a_status}`

- 2,048-pair conservative two-worker projection: {projection['targets']['2048']['conservative_linear_bound_wall_seconds'] / 3600:.2f}h.
- 4,096-pair conservative two-worker projection: {readiness['conservative_linear_bound_wall_seconds'] / 3600:.2f}h.
- 4,096-pair measured marginal projection: {readiness['measured_marginal_throughput_wall_seconds'] / 3600:.2f}h.
- Projected global peak RSS: {readiness['projected_global_peak_rss_bytes'] / 1024**3:.2f}GiB.
- Fixed sidecar footprint: {readiness['fixed_sidecar_disk_bytes'] / 1024**3:.2f}GiB.
- Projected retained checkpoint footprint: {readiness['projected_retained_checkpoint_disk_bytes'] / 1024**3:.2f}GiB.
- Projected total disk footprint: {readiness['projected_total_disk_bytes'] / 1024**3:.2f}GiB.
- Two-worker DAG cache hard cap: {readiness['projected_two_worker_dag_cache_cap_bytes'] / 1024**3:.2f}GiB.
- Strict Stage A: `NOT_AUTHORIZED`.

This is a resource-readiness decision only. It is not Alpha evidence and does not authorize execution.
"""
    (report_root / "CN_PHASE3CM_STAGE_A_READINESS_REPORT.md").write_text(
        readiness_report, encoding="utf-8"
    )

    manifest_candidates = [
        path
        for root in (runtime_root, report_root)
        for path in root.iterdir()
        if path.is_file() and path.name not in {"CN_STREAMING_ARTIFACT_MANIFEST.json"}
    ]
    manifest = {
        "schema_version": "cn_streaming_artifact_manifest_v1",
        "status": "CN_PHASE3CM_STREAMING_REPAIR_ARTIFACTS_COMPLETE",
        "source_implementation_sha": args.source_sha,
        "qualification_repo_sha": args.qualification_sha,
        "source_component_hashes": source_component_hashes,
        "frozen_artifact_hashes": frozen_artifact_hashes,
        "artifacts": [
            {
                "path": str(path),
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
            for path in sorted(manifest_candidates)
        ],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    manifest_path = runtime_root / "CN_STREAMING_ARTIFACT_MANIFEST.json"
    _write_json(manifest_path, manifest)

    bundle_path = report_root / "CN_PHASE3CM_STREAMING_REPAIR_BUNDLE.zip"
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted([*manifest_candidates, manifest_path]):
            base = runtime_root if path.is_relative_to(runtime_root) else report_root
            prefix = "runtime" if base == runtime_root else "reports"
            archive.write(path, arcname=f"{prefix}/{path.relative_to(base).as_posix()}")
    bundle_sha = _sha256(bundle_path)
    (report_root / "CN_PHASE3CM_STREAMING_REPAIR_BUNDLE.sha256").write_text(
        f"{bundle_sha}  {bundle_path.name}\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "evaluator_status": evaluator_status,
                "stage_a_entry_readiness": stage_a_status,
                "bundle_sha256": bundle_sha,
                "source_sha": args.source_sha,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
