from __future__ import annotations

import json
from pathlib import Path

from scripts.build_evalreset_architecture_graph import (
    REQUIRED_FORBIDDEN_RELATIONS,
    REQUIRED_NODE_IDS,
    build_graph,
)


REPO = Path(__file__).resolve().parents[1]


def test_phase_a_architecture_registry_builds_required_contract() -> None:
    registry = json.loads(
        (REPO / "runtime/run_plans/evalreset_phase1_architecture_registry_v1.json").read_text(encoding="utf-8")
    )

    graph = build_graph(registry, repo=REPO)

    node_ids = {node["id"] for node in graph["nodes"]}
    forbidden = {edge["relation"] for edge in graph["links"] if edge["permission"] == "FORBIDDEN"}
    assert REQUIRED_NODE_IDS <= node_ids
    assert REQUIRED_FORBIDDEN_RELATIONS <= forbidden
    assert all(node["last_verified_sha"] for node in graph["nodes"])
    assert all(node["status"] in {"IMPLEMENTED", "PARTIAL", "PLANNED", "FROZEN", "DEPRECATED"} for node in graph["nodes"])
