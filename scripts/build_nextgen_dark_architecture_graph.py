"""Build the authoritative architecture graph and its derived presentation."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import subprocess
from datetime import datetime, timezone
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
    "development_only_release",
    "formal_b1s_canary",
    "generator_research_sprint1",
    "sprint2_strict_priority_selector",
    "sprint2_event_generator",
    "broad_event_recovery",
    "sprint2_state_generator",
    "sprint2_rx_ucb",
    "sprint2_epoch_c",
    "sprint2_research_pack",
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
        "graph_type": "CN_SEARCH_SELECTION_EVENT_STATE_SPRINT2_CURRENT_ARCHITECTURE_CONTRACT",
        "authority_contract": overlay["authority_contract"],
        "nodes": nodes,
        "edges": edges,
    }


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _render_html(graph: dict) -> str:
    metadata = graph["graph"]
    rows = []
    for node in sorted(graph["nodes"], key=lambda item: (item["status"], item["id"])):
        rows.append(
            "<tr>"
            f"<td><code>{html.escape(str(node['id']))}</code></td>"
            f"<td>{html.escape(str(node['status']))}</td>"
            f"<td>{html.escape(str(node['data_role']))}</td>"
            f"<td>{html.escape(str(node['feedback_permission']))}</td>"
            f"<td>{html.escape(str(node['blocker']))}</td>"
            "</tr>"
        )
    edge_rows = []
    for edge in graph["links"]:
        edge_rows.append(
            "<tr>"
            f"<td><code>{html.escape(str(edge['source']))}</code></td>"
            f"<td>{html.escape(str(edge['relation']))}</td>"
            f"<td><code>{html.escape(str(edge['target']))}</code></td>"
            f"<td>{html.escape(str(edge['permission']))}</td>"
            "</tr>"
        )
    return f"""<!doctype html>
<html lang=\"en\"><head><meta charset=\"utf-8\"><title>CN Architecture Graph</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #17202a; }}
code {{ font-size: .85rem; }} table {{ border-collapse: collapse; width: 100%; margin: 1rem 0 2rem; }}
th, td {{ border: 1px solid #d5d8dc; padding: .45rem; text-align: left; vertical-align: top; }}
th {{ background: #f4f6f7; position: sticky; top: 0; }}
.meta {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(240px,1fr)); gap: .6rem; }}
.card {{ border: 1px solid #d5d8dc; border-radius: 8px; padding: .7rem; }}
</style></head><body>
<h1>CN Curated Architecture</h1>
<div class=\"meta\">
<div class=\"card\"><b>Phase</b><br>{html.escape(str(metadata['phase']))}</div>
<div class=\"card\"><b>Status authority</b><br><code>{html.escape(str(metadata['status_authority_path']))}</code></div>
<div class=\"card\"><b>Source repo SHA</b><br><code>{html.escape(str(metadata['source_repo_sha']))}</code></div>
<div class=\"card\"><b>Generated</b><br>{html.escape(str(metadata['generated_at']))}</div>
</div>
<h2>Nodes ({len(graph['nodes'])})</h2>
<table><thead><tr><th>ID</th><th>Status</th><th>Data role</th><th>Feedback</th><th>Blocker</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
<h2>Edges ({len(graph['links'])})</h2>
<table><thead><tr><th>Source</th><th>Relation</th><th>Target</th><th>Permission</th></tr></thead>
<tbody>{''.join(edge_rows)}</tbody></table>
</body></html>"""


def _relative(repo: Path, path: Path) -> str:
    return path.resolve().relative_to(repo.resolve()).as_posix()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-registry", type=Path, required=True)
    parser.add_argument("--overlay-registry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--html-output", type=Path, required=True)
    parser.add_argument("--freshness-output", type=Path, required=True)
    parser.add_argument("--view", action="append", type=Path, default=[])
    parser.add_argument("--source-repo-sha")
    args = parser.parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    base = json.loads(args.base_registry.read_text(encoding="utf-8"))
    overlay = json.loads(args.overlay_registry.read_text(encoding="utf-8"))
    merged_registry = merge_registries(base, overlay)
    graph = build_graph(merged_registry, repo=repo)
    source_repo_sha = args.source_repo_sha or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo, text=True
    ).strip()
    if len(source_repo_sha) != 40:
        raise RuntimeError("source repo SHA must be a full 40-character commit")
    generator_path = Path(__file__).resolve()
    graph["graph"].update(
        {
            "source_repo_sha": source_repo_sha,
            "status_authority_path": _relative(repo, args.overlay_registry),
            "status_authority_sha256": _sha256(args.overlay_registry),
            "base_registry_path": _relative(repo, args.base_registry),
            "base_registry_sha256": _sha256(args.base_registry),
            "generator_path": _relative(repo, generator_path),
            "generator_sha256": _sha256(generator_path),
            "authority_rules": ["SINGLE_STATUS_AUTHORITY", "SEPARATE_GRAPH_NAMESPACES"],
        }
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(graph, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.html_output.write_text(_render_html(graph), encoding="utf-8")
    derived = [args.output, args.html_output, *args.view]
    missing_views = [str(path) for path in derived if not path.is_file()]
    if missing_views:
        raise RuntimeError(f"architecture derived views missing: {missing_views}")
    freshness = {
        "version": "cn_architecture_freshness_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stale": False,
        "status_authority_path": _relative(repo, args.overlay_registry),
        "status_authority_sha256": _sha256(args.overlay_registry),
        "source_repo_sha": source_repo_sha,
        "generator_path": _relative(repo, generator_path),
        "generator_sha256": _sha256(generator_path),
        "derived_artifacts": [
            {
                "path": _relative(repo, path),
                "sha256": _sha256(path),
                "size": path.stat().st_size,
            }
            for path in derived
        ],
        "allowed_post_source_paths": [
            ".planning/architecture/architecture_graph.json",
            ".planning/architecture/architecture_graph.html",
            ".planning/architecture/ARCHITECTURE_FRESHNESS.json",
            "reports/nextgen_dark_20260711/ARTIFACT_INDEX.json",
        ],
        "authority_rules": ["SINGLE_STATUS_AUTHORITY", "SEPARATE_GRAPH_NAMESPACES"],
    }
    args.freshness_output.write_text(
        json.dumps(freshness, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"nodes": len(graph["nodes"]), "edges": len(graph["links"]), "output": str(args.output), "html": str(args.html_output), "freshness": str(args.freshness_output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
