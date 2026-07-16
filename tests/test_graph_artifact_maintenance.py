from __future__ import annotations

import hashlib
import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
GRAPHS = REPO / ".planning/graphs"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def test_graphskill_has_one_raw_and_one_current_control_plane() -> None:
    config = _load(REPO / ".planning/config.json")
    overlay = _load(REPO / "config/architecture_overlay.json")
    graphify_ignore = (REPO / ".graphifyignore").read_text(encoding="utf-8")

    assert config["graphify"] == {
        "enabled": True,
        "build_timeout": 600,
        "audit_timeout": 120,
    }
    assert overlay["schema_version"] == 1
    assert overlay["schema_ref"].endswith("architecture-overlay.schema.json")
    assert ".planning/graphs/" in graphify_ignore.splitlines()
    assert not (REPO / ".planning/architecture").exists()
    assert not (REPO / ".planning/codegraph").exists()
    assert {path.name for path in GRAPHS.iterdir() if path.is_file()} >= {
        "graph.json",
        "graph.html",
        "GRAPH_REPORT.md",
        "current.json",
        "current.html",
    }
    assert not (GRAPHS / ".last-build-snapshot.json").exists()


def test_raw_graph_is_real_sha_bound_graphify_output() -> None:
    graph = _load(GRAPHS / "graph.json")

    assert graph.get("kind") != "DEPRECATED_COMPATIBILITY_POINTER"
    assert isinstance(graph["directed"], bool)
    assert isinstance(graph["multigraph"], bool)
    assert len(graph["nodes"]) > 3000
    assert len(graph["links"]) > 7000
    assert len(graph.get("hyperedges", [])) > 0
    assert len(graph["built_at_commit"]) == 40
    assert all("id" in row for row in graph["nodes"][:100])
    assert all("source" in row and "target" in row for row in graph["links"][:100])


def test_current_is_generated_from_raw_and_overlay_without_runtime_inference() -> None:
    current = _load(GRAPHS / "current.json")
    nodes = {row["id"]: row for row in current["nodes"]}
    edges = {row["id"]: row for row in current["edges"]}

    assert current["kind"] == "current-architecture"
    assert current["status"] == "INSUFFICIENT_EVIDENCE"
    assert current["strict_ready"] is False
    assert current["raw"]["path"] == ".planning/graphs/graph.json"
    assert current["raw"]["sha256"] == _sha256(GRAPHS / "graph.json")
    assert current["overlay"]["path"] == "config/architecture_overlay.json"
    assert current["overlay"]["sha256"] == _sha256(
        REPO / "config/architecture_overlay.json"
    )
    assert current["trace"]["path"] is None
    assert current["summary"]["runtime_nodes"] == 0
    assert current["summary"]["runtime_edges"] == 0

    assert nodes["matched_control_pair_authority"]["lifecycle"] == "ACTIVE"
    assert nodes["candidate_parallel_evaluator"]["lifecycle"] == "ACTIVE"
    assert nodes["formal_search"]["lifecycle"] == "FORBIDDEN"
    assert nodes["compositional_nline_bounded_search"]["lifecycle"] == "EXPERIMENTAL"
    assert nodes["phase3cm_streaming_multicandidate_evaluator"]["lifecycle"] == "EXPERIMENTAL"
    assert nodes["shard_parallel_evaluation"]["lifecycle"] == "DEPRECATED"
    assert nodes["mean_shard_reward_fallback"]["lifecycle"] == "FORBIDDEN"
    assert edges["validation_to_feedback_forbidden"]["forbidden"] is True
    assert edges["forward_to_search_forbidden"]["forbidden"] is True
    assert edges["mean_shard_to_feedback_forbidden"]["forbidden"] is True
    assert edges["compositional_to_receipt"]["relation"] == "EXPERIMENTAL_CURRENT_NON_FORMAL"
    assert edges["compositional_to_streaming_evaluator"]["relation"] == (
        "resource_qualified_for_separate_authorization"
    )
    assert edges["streaming_evaluator_to_search_forbidden"]["forbidden"] is True
