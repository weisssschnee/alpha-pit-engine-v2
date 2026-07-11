"""Validate NEXTGEN-DARK closure without executing its CANARY plan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REQUIRED_NODES = {
    "nextgen_field_registry_121", "feature_state_fabric", "typed_temporal_program",
    "nextgen_event_state_system", "pit_group_sidecar", "hypothesis_lane_registry",
    "admission_diversity", "benchmark_competitor_harness", "coverage_metrics",
    "atomic_checkpoint_resume", "nextgen_canary_plan", "formal_search_frozen",
}


def validate_nextgen_dark(repo: Path) -> dict[str, Any]:
    manifest = json.loads((repo / "runtime/run_plans/nextgen_dark_run_manifest_v1.json").read_text(encoding="utf-8"))
    canary = json.loads((repo / "runtime/run_plans/nextgen_dark_canary_plan_v1.json").read_text(encoding="utf-8"))
    registry = json.loads((repo / "runtime/field_registry/nextgen_dark_field_registry_v1.json").read_text(encoding="utf-8"))
    graph = json.loads((repo / ".planning/graphs/graph.json").read_text(encoding="utf-8"))
    artifact_index = json.loads((repo / "reports/nextgen_dark_20260711/ARTIFACT_INDEX.json").read_text(encoding="utf-8"))
    temporal = json.loads((repo / "runtime/run_plans/nextgen_dark_temporal_registry_v1.json").read_text(encoding="utf-8"))
    events = json.loads((repo / "runtime/run_plans/nextgen_dark_event_registry_v1.json").read_text(encoding="utf-8"))
    lanes = json.loads((repo / "runtime/run_plans/nextgen_dark_hypothesis_lane_registry_v1.json").read_text(encoding="utf-8"))
    benchmarks = json.loads((repo / "runtime/run_plans/nextgen_dark_benchmark_registry_v1.json").read_text(encoding="utf-8"))
    nodes = {node["id"]: node for node in graph["nodes"]}
    missing_nodes = sorted(REQUIRED_NODES - set(nodes))
    if missing_nodes:
        raise RuntimeError(f"NEXTGEN graph missing nodes: {missing_nodes}")
    if registry["field_count"] != 121:
        raise RuntimeError("NEXTGEN field registry must contain the verified 121-field schema")
    if temporal["primitive_count"] != 15 or events["feature_count"] != 16 or lanes["lane_count"] != 7:
        raise RuntimeError("NEXTGEN typed/event/lane registry count mismatch")
    if len(benchmarks["benchmarks"]) != 16 or benchmarks["performance_comparison_executed"]:
        raise RuntimeError("NEXTGEN benchmark registry mismatch or execution leak")
    if manifest["status"] != "NEXTGEN_DARK_INFRASTRUCTURE_PARTIALLY_READY":
        raise RuntimeError("unexpected NEXTGEN closure status")
    if manifest["formal_search_executed"] or manifest["forward_2026_accessed"] or manifest["adaptive_reward_restored"]:
        raise RuntimeError("NEXTGEN closure crossed a frozen execution boundary")
    if canary["execution_state"] != "PREPARED_NOT_STARTED_REQUIRES_INDEPENDENT_AUTHORIZATION":
        raise RuntimeError("CANARY plan must remain not started")
    if canary["allowed_data_roles"] != ["development"]:
        raise RuntimeError("CANARY plan must be development-only")
    if canary["online_policy_update_allowed"] or canary["forward_2026_allowed"]:
        raise RuntimeError("CANARY plan cannot enable online tuning or forward data")
    partial = [row["id"] for row in artifact_index["artifacts"] if row["state"] == "PARTIAL"]
    if partial != ["pit_historical_membership_release"]:
        raise RuntimeError(f"unexpected NEXTGEN partial artifacts: {partial}")
    if nodes["pit_group_sidecar"]["status"] != "PARTIAL":
        raise RuntimeError("PIT group node must expose the missing historical source blocker")
    return {
        "status": manifest["status"],
        "graph_node_count": len(nodes),
        "graph_edge_count": len(graph["links"]),
        "field_count": registry["field_count"],
        "temporal_primitive_count": temporal["primitive_count"],
        "event_feature_count": events["feature_count"],
        "lane_count": lanes["lane_count"],
        "benchmark_count": len(benchmarks["benchmarks"]),
        "canary_execution_state": canary["execution_state"],
        "partial_artifacts": partial,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args(argv)
    print(json.dumps(validate_nextgen_dark(args.repo.resolve()), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
