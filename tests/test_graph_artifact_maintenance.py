from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.check_architecture_freshness import check_freshness


REPO = Path(__file__).resolve().parents[1]
RAW_SHA = "18f8e34021b871b10ebb794a0e10ec0e5e2b4587"


def test_status_authority_and_graph_namespaces_are_separate() -> None:
    registry = json.loads(
        (REPO / ".planning/architecture/architecture_registry.json").read_text(encoding="utf-8")
    )
    graph = json.loads(
        (REPO / ".planning/architecture/architecture_graph.json").read_text(encoding="utf-8")
    )
    compatibility = json.loads(
        (REPO / ".planning/graphs/graph.json").read_text(encoding="utf-8")
    )

    assert registry["authority_contract"]["status_authority"] == ".planning/architecture/architecture_registry.json"
    assert registry["authority_contract"]["raw_codegraph_may_assert_research_status"] is False
    assert graph["graph"]["status_authority_path"] == registry["authority_contract"]["status_authority"]
    assert compatibility["kind"] == "DEPRECATED_COMPATIBILITY_POINTER"
    assert compatibility["may_assert_current_status"] is False


def test_sha_bound_raw_codegraph_manifest_and_artifacts_are_intact() -> None:
    root = REPO / ".planning/codegraph"
    manifest = json.loads(
        (root / f"CODEGRAPH_BUILD_MANIFEST_{RAW_SHA}.json").read_text(encoding="utf-8-sig")
    )
    assert manifest["source_repo_full_sha"] == RAW_SHA
    assert manifest["node_count"] == 2619
    assert manifest["edge_count"] == 6602
    expected = {
        f"codegraph_{RAW_SHA}.json",
        f"codegraph_{RAW_SHA}.html",
        f"CODEGRAPH_REPORT_{RAW_SHA}.md",
    }
    assert {artifact["path"] for artifact in manifest["artifacts"]} == expected
    for artifact in manifest["artifacts"]:
        path = root / artifact["path"]
        assert path.stat().st_size == artifact["size"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact["sha256"]
    snapshot = json.loads((root / f"CODEGRAPH_SNAPSHOT_{RAW_SHA}.json").read_text(encoding="utf-8"))
    assert len(snapshot["nodes"]) == manifest["node_count"]
    assert len(snapshot["edges"]) == manifest["edge_count"]


def test_architecture_freshness_checks_registry_repo_generator_and_all_views() -> None:
    report = check_freshness(REPO)
    assert report["stale"] is False, report
    assert report["status_authority"] == ".planning/architecture/architecture_registry.json"
    assert report["derived_artifact_count"] >= 5
