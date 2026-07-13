"""Fail closed when curated architecture artifacts are stale or ambiguous."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=repo, text=True).strip()


def check_freshness(repo: Path, *, check_worktree: bool = True) -> dict:
    manifest_path = repo / ".planning/architecture/ARCHITECTURE_FRESHNESS.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    errors: list[str] = []
    authority = repo / manifest["status_authority_path"]
    generator = repo / manifest["generator_path"]
    if manifest["status_authority_path"] != ".planning/architecture/architecture_registry.json":
        errors.append("noncanonical_status_authority")
    for path, expected, label in (
        (authority, manifest["status_authority_sha256"], "status_authority"),
        (generator, manifest["generator_sha256"], "generator"),
    ):
        if not path.is_file() or _sha256(path) != expected:
            errors.append(f"{label}_hash_mismatch")
    for artifact in manifest["derived_artifacts"]:
        path = repo / artifact["path"]
        if not path.is_file():
            errors.append(f"missing_derived:{artifact['path']}")
        elif path.stat().st_size != artifact["size"] or _sha256(path) != artifact["sha256"]:
            errors.append(f"derived_hash_mismatch:{artifact['path']}")

    graph_path = repo / ".planning/architecture/architecture_graph.json"
    graph = json.loads(graph_path.read_text(encoding="utf-8"))
    metadata = graph["graph"]
    if metadata.get("status_authority_sha256") != manifest["status_authority_sha256"]:
        errors.append("graph_authority_hash_mismatch")
    if metadata.get("generator_sha256") != manifest["generator_sha256"]:
        errors.append("graph_generator_hash_mismatch")
    if metadata.get("source_repo_sha") != manifest["source_repo_sha"]:
        errors.append("graph_repo_sha_mismatch")
    if metadata.get("authority_rules") != ["SINGLE_STATUS_AUTHORITY", "SEPARATE_GRAPH_NAMESPACES"]:
        errors.append("graph_authority_rules_missing")

    source_sha = manifest["source_repo_sha"]
    head = _git(repo, "rev-parse", "HEAD")
    try:
        _git(repo, "merge-base", "--is-ancestor", source_sha, head)
    except subprocess.CalledProcessError:
        errors.append("source_repo_sha_not_ancestor")
    allowed = set(manifest["allowed_post_source_paths"])
    committed_after_source = set(filter(None, _git(repo, "diff", "--name-only", f"{source_sha}..{head}").splitlines()))
    unexpected_committed = sorted(committed_after_source - allowed)
    if unexpected_committed:
        errors.append("non_graph_commits_after_source:" + ",".join(unexpected_committed))
    if check_worktree:
        dirty = set(filter(None, _git(repo, "status", "--porcelain").splitlines()))
        if dirty:
            errors.append("worktree_not_clean")
    return {
        "version": manifest["version"],
        "stale": bool(errors),
        "errors": errors,
        "source_repo_sha": source_sha,
        "head": head,
        "status_authority": manifest["status_authority_path"],
        "derived_artifact_count": len(manifest["derived_artifacts"]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args(argv)
    report = check_freshness(args.repo.resolve(), check_worktree=not args.allow_dirty)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if report["stale"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
