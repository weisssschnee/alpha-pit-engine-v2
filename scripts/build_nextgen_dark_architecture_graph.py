"""Merge accepted Phase A and NEXTGEN-DARK registries into the current graph."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.build_evalreset_architecture_graph import build_graph


REQUIRED_NEXTGEN_NODES = {
    "nextgen_field_registry_121",
    "feature_state_fabric",
    "typed_temporal_program",
    "nextgen_event_state_system",
    "chip_pit_sidecar",
    "pit_group_sidecar",
    "hypothesis_lane_registry",
    "admission_diversity",
    "benchmark_competitor_harness",
    "coverage_metrics",
    "atomic_checkpoint_resume",
    "nextgen_canary_plan",
    "formal_search_frozen",
}


def merge_registries(base: dict, overlay: dict) -> dict:
    overrides = {str(key): dict(value) for key, value in overlay.get("node_overrides", {}).items()}
    base_nodes = [
        {**node, **overrides.get(str(node["id"]), {})}
        for node in base["nodes"]
    ]
    unknown_overrides = sorted(set(overrides) - {str(node["id"]) for node in base["nodes"]})
    if unknown_overrides:
        raise RuntimeError(f"NEXTGEN node overrides reference unknown base nodes: {unknown_overrides}")
    nodes = [*base_nodes, *overlay["nodes"]]
    edges = [*base["edges"], *overlay["edges"]]
    ids = [str(node["id"]) for node in nodes]
    if len(ids) != len(set(ids)):
        raise RuntimeError("base/overlay node IDs collide")
    missing = sorted(REQUIRED_NEXTGEN_NODES - set(ids))
    if missing:
        raise RuntimeError(f"NEXTGEN registry missing nodes: {missing}")
    return {
        "registry_version": overlay["registry_version"],
        "phase": overlay["phase"],
        "graph_type": "NEXTGEN_DARK_CURRENT_ARCHITECTURE_CONTRACT",
        "nodes": nodes,
        "edges": edges,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-registry", type=Path, required=True)
    parser.add_argument("--overlay-registry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    base = json.loads(args.base_registry.read_text(encoding="utf-8"))
    overlay = json.loads(args.overlay_registry.read_text(encoding="utf-8"))
    graph = build_graph(merge_registries(base, overlay), repo=repo)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(graph, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"nodes": len(graph["nodes"]), "edges": len(graph["links"]), "output": str(args.output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
