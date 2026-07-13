from __future__ import annotations

import hashlib
import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


def test_raw_and_curated_graphs_share_a_source_snapshot() -> None:
    curated = json.loads((REPO / ".planning/graphs/graph.json").read_text(encoding="utf-8"))
    raw_manifest = json.loads(
        (REPO / ".planning/graphs/raw/BUILD_MANIFEST.json").read_text(encoding="utf-8-sig")
    )

    assert len(curated["graph"]["source_repo_sha"]) == 40
    assert raw_manifest["node_count"] > len(curated["nodes"])
    assert raw_manifest["edge_count"] > len(curated["links"])
    assert raw_manifest["source_repo_full_sha"] == curated["graph"]["source_repo_sha"]
    assert raw_manifest["node_count"] == 2619
    assert raw_manifest["edge_count"] == 6602
    for artifact in raw_manifest["artifacts"]:
        path = REPO / ".planning/graphs/raw" / artifact["path"]
        assert path.stat().st_size == artifact["size"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact["sha256"]

    snapshot = json.loads(
        (REPO / ".planning/graphs/raw/.last-build-snapshot.json").read_text(encoding="utf-8")
    )
    assert len(snapshot["nodes"]) == raw_manifest["node_count"]
    assert len(snapshot["edges"]) == raw_manifest["edge_count"]
