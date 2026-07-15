"""Validate required Phase A architecture and evidence deliverables."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

REQUIRED_FILES = {
    "code": "src/our_system_phase2/services/evaluation_access_guard.py",
    "sketch_code": "src/our_system_phase2/services/deterministic_signal_sketch.py",
    "tests": "tests/test_deterministic_signal_sketch.py",
    "architecture_overlay": "config/architecture_overlay.json",
    "raw_graph": ".planning/graphs/graph.json",
    "current_architecture": ".planning/graphs/current.json",
    "state": ".planning/STATE.md",
    "decision_log": "docs/evalreset/PHASE1_DECISION_CHANGE_LOG.md",
    "run_manifest": "runtime/run_plans/evalreset_phase1_run_manifest_v1.json",
    "artifact_index": "reports/evalreset_phase1_20260711/ARTIFACT_INDEX.json",
}


def validate_delivery(repo: Path, *, require_complete: bool) -> dict[str, Any]:
    missing = [f"{name}:{path}" for name, path in REQUIRED_FILES.items() if not (repo / path).is_file()]
    if missing:
        raise RuntimeError(f"missing Phase A deliverables: {missing}")
    graph = json.loads((repo / REQUIRED_FILES["current_architecture"]).read_text(encoding="utf-8"))
    nodes = {node["id"]: node for node in graph["nodes"]}
    required_nodes = {
        "development_data",
        "fixed_split_authority",
        "validation_holdout_feedback",
        "forward_2026",
        "formal_search",
        "cross_epoch_memory",
    }
    if not required_nodes <= set(nodes):
        raise RuntimeError(f"graph missing nodes: {sorted(required_nodes - set(nodes))}")
    forbidden_edges = {edge["id"] for edge in graph["edges"] if edge["forbidden"]}
    required_forbidden_edges = {
        "validation_to_feedback_forbidden",
        "validation_to_memory_forbidden",
        "forward_to_search_forbidden",
        "forward_to_memory_forbidden",
    }
    if not required_forbidden_edges <= forbidden_edges:
        raise RuntimeError(
            f"graph missing forbidden edges: {sorted(required_forbidden_edges - forbidden_edges)}"
        )
    state = (repo / REQUIRED_FILES["state"]).read_text(encoding="utf-8")
    for token in ("A_EVALRESET", "2026", "FROZEN", "signal-sketch", "Next formal decision point"):
        if token not in state:
            raise RuntimeError(f"STATE.md missing required token: {token}")
    manifest = json.loads((repo / REQUIRED_FILES["run_manifest"]).read_text(encoding="utf-8"))
    artifact_index = json.loads((repo / REQUIRED_FILES["artifact_index"]).read_text(encoding="utf-8"))
    if require_complete:
        accepted_states = {
            "READY_FOR_PHASE_A_COMMIT",
            "PHASE_A_COMMITTED_AWAITING_USER_ACCEPTANCE",
        }
        if manifest.get("status") != "COMPLETE" or manifest.get("acceptance_state") not in accepted_states:
            raise RuntimeError("run manifest is not complete or committed for Phase A")
        incomplete = [row["id"] for row in artifact_index["artifacts"] if row.get("state") != "IMPLEMENTED"]
        if incomplete:
            raise RuntimeError(f"artifact index has incomplete assets: {incomplete}")
    return {
        "required_file_count": len(REQUIRED_FILES),
        "graph_node_count": len(nodes),
        "graph_edge_count": len(graph["edges"]),
        "forbidden_edge_count": len(forbidden_edges),
        "require_complete": require_complete,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args(argv)
    print(json.dumps(validate_delivery(args.repo.resolve(), require_complete=args.require_complete), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
