"""Build and validate the Phase A architecture graph from its registry."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STATUSES = {"IMPLEMENTED", "PARTIAL", "PLANNED", "FROZEN", "DEPRECATED"}
NODE_FIELDS = {
    "id",
    "label",
    "status",
    "implementation_paths",
    "input_artifacts",
    "output_artifacts",
    "data_role",
    "feedback_permission",
    "test_evidence",
    "verification_paths",
    "blocker",
}
REQUIRED_FORBIDDEN_RELATIONS = {
    "validation_holdout_to_candidate_reward",
    "validation_holdout_to_scheduler",
    "validation_holdout_to_family_kill_freeze",
    "forward_to_search_policy",
    "spent_evaluation_to_development",
    "report_only_to_positive_memory",
}
REQUIRED_NODE_IDS = {
    "true1min_16_shards",
    "fixed_calendar_split",
    "field_sidecar",
    "firstn_sidecar",
    "event_state_sidecar",
    "plate_industry_sidecar",
    "generation_24576",
    "proxy_1536",
    "admission_384",
    "strict_reward_324",
    "coverage_qualified_202",
    "signal_sketch_audit",
    "candidate_identity_registry",
    "cluster_registry",
    "development_role",
    "spent_role",
    "sealed_role",
    "forward_2026",
    "evaluation_access_ledger",
    "scheduler_memory_feedback",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _combined_sha(repo: Path, paths: list[str]) -> str:
    digest = hashlib.sha256()
    for raw in sorted(paths):
        path = Path(raw)
        resolved = path if path.is_absolute() else repo / path
        if not resolved.is_file():
            raise RuntimeError(f"verification path is not a file: {raw}")
        digest.update(raw.replace("\\", "/").encode())
        digest.update(bytes.fromhex(_sha256(resolved)))
    return digest.hexdigest()


def build_graph(registry: dict[str, Any], *, repo: Path) -> dict[str, Any]:
    nodes = list(registry.get("nodes") or [])
    edges = list(registry.get("edges") or [])
    ids: set[str] = set()
    output_nodes: list[dict[str, Any]] = []
    for node in nodes:
        missing = sorted(NODE_FIELDS - set(node))
        if missing:
            raise RuntimeError(f"node {node.get('id')} missing fields: {missing}")
        node_id = str(node["id"])
        if node_id in ids:
            raise RuntimeError(f"duplicate node id: {node_id}")
        ids.add(node_id)
        if node["status"] not in STATUSES:
            raise RuntimeError(f"node {node_id} has invalid status {node['status']}")
        for raw in node["implementation_paths"]:
            path = Path(raw)
            resolved = path if path.is_absolute() else repo / path
            if not resolved.exists():
                raise RuntimeError(f"node {node_id} implementation path missing: {raw}")
        evidence_missing = [raw for raw in node["test_evidence"] if not (Path(raw) if Path(raw).is_absolute() else repo / raw).exists()]
        if evidence_missing:
            raise RuntimeError(f"node {node_id} test/evidence missing: {evidence_missing}")
        output_nodes.append(
            {
                **node,
                "implementation_path": node["implementation_paths"],
                "input_artifact": node["input_artifacts"],
                "output_artifact": node["output_artifacts"],
                "test/evidence": node["test_evidence"],
                "last_verified_sha": _combined_sha(repo, list(node["verification_paths"])),
                "last_verified_sha_scope": list(node["verification_paths"]),
            }
        )
    missing_nodes = sorted(REQUIRED_NODE_IDS - ids)
    if missing_nodes:
        raise RuntimeError(f"registry missing required Phase A nodes: {missing_nodes}")
    forbidden = {str(edge.get("relation")) for edge in edges if edge.get("permission") == "FORBIDDEN"}
    missing_forbidden = sorted(REQUIRED_FORBIDDEN_RELATIONS - forbidden)
    if missing_forbidden:
        raise RuntimeError(f"registry missing forbidden edges: {missing_forbidden}")
    for edge in edges:
        if edge.get("source") not in ids or edge.get("target") not in ids:
            raise RuntimeError(f"edge references unknown node: {edge}")
        if edge.get("permission") not in {"ALLOWED", "FORBIDDEN"}:
            raise RuntimeError(f"edge has invalid permission: {edge}")
    registry_bytes = json.dumps(registry, sort_keys=True, separators=(",", ":")).encode()
    return {
        "directed": True,
        "multigraph": True,
        "graph": {
            "graph_type": registry.get("graph_type", "EVALRESET_PHASE_A_ARCHITECTURE_CONTRACT"),
            "registry_version": registry["registry_version"],
            "phase": registry["phase"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "registry_sha256": hashlib.sha256(registry_bytes).hexdigest(),
            "status_values": sorted(STATUSES),
            "forbidden_edge_contract": sorted(REQUIRED_FORBIDDEN_RELATIONS),
        },
        "nodes": output_nodes,
        "links": edges,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    registry = json.loads(args.registry.read_text(encoding="utf-8"))
    graph = build_graph(registry, repo=repo)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(graph, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"nodes": len(graph["nodes"]), "edges": len(graph["links"]), "output": str(args.output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
