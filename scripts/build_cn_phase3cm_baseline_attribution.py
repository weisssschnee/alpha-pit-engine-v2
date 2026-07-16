from __future__ import annotations

import argparse
import json
from pathlib import Path


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--historical-abort", type=Path, required=True)
    args = parser.parse_args()
    root = args.runtime_root.resolve()
    telemetry = _json(root / "CN_PHASE3CM_LEGACY_BASELINE_TELEMETRY.json")
    environment = _json(root / "CN_PHASE3CM_77O_ENVIRONMENT_AUDIT.json")
    history = _json(args.historical_abort.resolve())
    timing = [json.loads(line) for line in (root / "CN_PHASE3CM_PHASE_TIMING.jsonl").read_text().splitlines() if line]
    by_phase = {row["phase"]: row for row in timing}
    dominant = max(timing, key=lambda row: float(row.get("cpu_seconds", 0.0)))
    allocated = int(telemetry["allocated_compute_threads"])
    effective = float(telemetry["effective_cores"])
    read_throughput = float(telemetry["read_bytes"]) / float(telemetry["wall_seconds"])
    device_throughput = float(environment["device_read_benchmark"]["bytes_per_second"])
    attribution = {
        "schema_version": "cn_phase3cm_baseline_bottleneck_attribution_v1",
        "status": "CN_PHASE3CM_LEGACY_BASELINE_ATTRIBUTED",
        "scope": "RECEIPT_FROZEN_ONE_PAIR_FULL_COORDINATE_CONTROLLED_STOP",
        "phase0_is_qualification_evidence": False,
        "legacy_evaluator_receipt_hash": environment["legacy_evaluator"]["receipt_context_evaluator_hash"],
        "controlled_baseline": {
            "wall_seconds": telemetry["wall_seconds"],
            "cpu_seconds": telemetry["cpu_seconds"],
            "effective_cores": effective,
            "allocated_compute_threads": allocated,
            "parallel_efficiency": effective / allocated,
            "parallelism_status": (
                "PARALLELISM_ENGAGED" if effective >= 0.5 * allocated else "PARALLELISM_NOT_ENGAGED"
            ),
            "peak_working_set_bytes": telemetry["peak_working_set_bytes"],
            "bytes_read": telemetry["read_bytes"],
            "read_throughput_bytes_per_second": read_throughput,
            "device_bound_bytes_per_second": device_throughput,
            "device_throughput_utilization": read_throughput / device_throughput,
            "io_status": "EXECUTION_ARCHITECTURE_BOTTLENECK_NOT_DEVICE_SATURATION",
        },
        "phase_timing": timing,
        "hot_path_decision": {
            "dominant_completed_or_active_phase": dominant["phase"],
            "dominant_compute_share": dominant["compute_share"],
            "value_dag_compute_share": by_phase.get("expression_value_dag", {}).get("compute_share", 0.0),
            "rank_mapping_compute_share_observed_before_stop": sum(
                float(by_phase.get(name, {}).get("compute_share", 0.0))
                for name in ("cross_sectional_rank", "portfolio_mapping_turnover_cost")
            ),
            "primary_bottleneck": "VALUE_DAG_FULL_SERIES_AND_PANDAS_BOTTLENECK",
            "batched_portfolio_kernel_still_required": True,
            "caveat": "The controlled stop occurred during the first candidate expression on the first stock shard; downstream mapping did not receive a complete full-market block, so its eventual share is not inferable from this baseline.",
        },
        "historical_full_coordinate_gate": {
            "status": history["status"],
            "wall_time_lower_bound_seconds": history["wall_time_lower_bound_seconds"],
            "cpu_seconds": history["cpu_seconds"],
            "effective_cores": float(history["cpu_seconds"]) / float(history["wall_time_lower_bound_seconds"]),
            "peak_working_set_bytes": history["peak_working_set_bytes"],
            "read_transfer_bytes": history["read_transfer_bytes"],
            "evaluated_pairs": int(history.get("evaluated", 0)),
        },
        "semantic_architecture_finding": {
            "physical_layout": environment["physical_release_layout"]["partitioning_finding"],
            "legacy_execution": "SHARD_LOCAL_CROSS_SECTIONAL_MAPPING",
            "legacy_execution_is_full_market": False,
            "required_execution": environment["physical_release_layout"]["required_repair"],
            "parity_authority": "LEGACY_KERNEL_ON_MERGED_ALL_SHARDS_FIXTURE_NOT_LEGACY_SHARD_LOOP_OUTPUT",
        },
        "repair_priorities": [
            "GLOBAL_TRADE_TIME_BARRIER_ACROSS_16_STOCK_SHARDS",
            "SHARED_VALUE_DAG_WITH_BOUNDED_BLOCK_CACHE",
            "BATCHED_NATIVE_CROSS_SECTIONAL_RANK_MAPPING_TURNOVER_COST",
            "ONLINE_REDUCER_ZERO_COORDINATE_ROW_RETENTION",
            "PERIODIC_ATOMIC_CHECKPOINT_AND_FAIL_CLOSED_RSS_GATES",
        ],
        "sealed_reads": {"validation": 0, "holdout": 0, "forward_2026": 0},
        "strict_stage_a": "NOT_AUTHORIZED",
    }
    (root / "CN_PHASE3CM_BASELINE_BOTTLENECK_ATTRIBUTION.json").write_text(
        json.dumps(attribution, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"status": attribution["status"], "dominant": dominant["phase"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
