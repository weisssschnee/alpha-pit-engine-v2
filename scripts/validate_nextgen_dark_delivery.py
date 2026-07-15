"""Validate NEXTGEN-DARK closure without executing its CANARY plan."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


REQUIRED_CURRENT_NODES = {
    "feature_state_fabric",
    "typed_temporal_program",
    "broad_event_system",
    "hypothesis_lanes",
    "plate_industry_pit_route",
    "formal_search",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_nextgen_dark(repo: Path) -> dict[str, Any]:
    manifest = json.loads((repo / "runtime/run_plans/nextgen_dark_run_manifest_v1.json").read_text(encoding="utf-8"))
    canary = json.loads((repo / "runtime/run_plans/nextgen_dark_canary_plan_v1.json").read_text(encoding="utf-8"))
    registry = json.loads((repo / "runtime/field_registry/nextgen_dark_field_registry_v2.json").read_text(encoding="utf-8"))
    graph = json.loads((repo / ".planning/graphs/current.json").read_text(encoding="utf-8"))
    artifact_index = json.loads((repo / "reports/nextgen_dark_20260711/ARTIFACT_INDEX.json").read_text(encoding="utf-8"))
    temporal = json.loads((repo / "runtime/run_plans/nextgen_dark_temporal_registry_v1.json").read_text(encoding="utf-8"))
    events = json.loads((repo / "runtime/run_plans/nextgen_dark_event_registry_v1.json").read_text(encoding="utf-8"))
    lanes = json.loads((repo / "runtime/run_plans/nextgen_dark_hypothesis_lane_registry_v1.json").read_text(encoding="utf-8"))
    benchmarks = json.loads((repo / "runtime/run_plans/nextgen_dark_benchmark_registry_v1.json").read_text(encoding="utf-8"))
    nodes = {node["id"]: node for node in graph["nodes"]}
    missing_nodes = sorted(REQUIRED_CURRENT_NODES - set(nodes))
    if missing_nodes:
        raise RuntimeError(f"NEXTGEN graph missing nodes: {missing_nodes}")
    if registry["field_count"] != 121:
        raise RuntimeError("NEXTGEN field registry must contain the verified 121-field schema")
    if temporal["primitive_count"] != 15 or events["feature_count"] != 16 or lanes["lane_count"] != 7:
        raise RuntimeError("NEXTGEN typed/event/lane registry count mismatch")
    if len(benchmarks["benchmarks"]) != 16 or benchmarks["performance_comparison_executed"]:
        raise RuntimeError("NEXTGEN benchmark registry mismatch or execution leak")
    if manifest["status"] != "NEXTGEN_DARK_INFRASTRUCTURE_READY":
        raise RuntimeError("unexpected NEXTGEN closure status")
    if manifest["formal_search_executed"] or manifest["forward_2026_accessed"] or manifest["adaptive_reward_restored"]:
        raise RuntimeError("NEXTGEN closure crossed a frozen execution boundary")
    if canary["execution_state"] != "PREPARED_NOT_STARTED_REQUIRES_INDEPENDENT_AUTHORIZATION":
        raise RuntimeError("CANARY plan must remain not started")
    if canary["allowed_data_roles"] != ["development"]:
        raise RuntimeError("CANARY plan must be development-only")
    if canary["online_policy_update_allowed"] or canary["forward_2026_allowed"]:
        raise RuntimeError("CANARY plan cannot enable online tuning or forward data")
    if canary["plate_industry_linkage"] != {
        "enabled": False,
        "blocker": (
            "USER_DEFERRED_EXCLUDED_FROM_CLOSURE; "
            "re-enable requires new explicit authorization"
        ),
    }:
        raise RuntimeError("CANARY plan must keep user-deferred plate linkage disabled")
    if benchmarks.get("disabled_benchmark_ids") != ["plate_industry_linkage"]:
        raise RuntimeError("plate/industry benchmark must be explicitly disabled")
    if benchmarks.get("disabled_reason") != "USER_DEFERRED_EXCLUDED_FROM_CLOSURE":
        raise RuntimeError("plate/industry benchmark needs the authorized defer reason")
    partial = [row["id"] for row in artifact_index["artifacts"] if row["state"] == "PARTIAL"]
    if partial:
        raise RuntimeError(f"unexpected NEXTGEN partial artifacts: {partial}")
    deferred = [
        row["id"] for row in artifact_index["artifacts"] if row["state"] == "DEFERRED"
    ]
    if deferred != ["pit_historical_membership_release"]:
        raise RuntimeError(f"unexpected NEXTGEN deferred artifacts: {deferred}")
    plate_scope = manifest.get("scope_exclusions", {}).get("plate_industry")
    if plate_scope != "USER_DEFERRED_EXCLUDED_FROM_CLOSURE":
        raise RuntimeError("plate/industry scope must be explicitly excluded from closure")
    if nodes["plate_industry_pit_route"]["lifecycle"] != "FORBIDDEN":
        raise RuntimeError("deferred PIT group route must remain forbidden")
    edges = {row["id"]: row for row in graph["edges"]}
    if not edges["plate_to_lanes_forbidden"]["forbidden"]:
        raise RuntimeError("deferred PIT group route cannot feed NEXTGEN candidates")
    for row in artifact_index["artifacts"]:
        path = repo / row["path"]
        if row["exists"] != path.is_file():
            raise RuntimeError(f"artifact existence drift: {row['id']}")
        if path.is_file() and row["last_verified_sha"] != _sha256(path):
            raise RuntimeError(f"artifact hash drift: {row['id']}")
    return {
        "status": manifest["status"],
        "graph_node_count": len(nodes),
        "graph_edge_count": len(graph["edges"]),
        "field_count": registry["field_count"],
        "temporal_primitive_count": temporal["primitive_count"],
        "event_feature_count": events["feature_count"],
        "lane_count": lanes["lane_count"],
        "benchmark_count": len(benchmarks["benchmarks"]),
        "canary_execution_state": canary["execution_state"],
        "partial_artifacts": partial,
        "deferred_artifacts": deferred,
        "plate_scope": plate_scope,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args(argv)
    print(json.dumps(validate_nextgen_dark(args.repo.resolve()), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
