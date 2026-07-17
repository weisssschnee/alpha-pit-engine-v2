from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.compositional_generation_epoch import (  # noqa: E402
    build_compositional_generation_epoch,
    select_structural_preadmission,
)
from our_system_phase2.services.unified_capability_registry import (  # noqa: E402
    UnifiedCapabilityRegistry,
    stable_hash,
)


DEFAULT_PLAN = REPO / "runtime/run_plans/cn_compositional_nline_bounded_search_epoch1_v1.json"
DEFAULT_OUTPUT = REPO / "runtime/cn_compositional_nline_large_search_20260715"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _source_sha256(path: Path) -> str:
    data = path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(data).hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n")


def _git(*args: str) -> str:
    executable = os.environ.get("GIT_EXECUTABLE", "git")
    return subprocess.check_output(
        [executable, "-C", str(REPO), *args],
        text=True,
        encoding="utf-8",
    ).strip()


def _source_identity(plan: Mapping[str, Any], *, repo_sha: str, tree_sha: str) -> tuple[str, str]:
    git_executable = os.environ.get("GIT_EXECUTABLE", "git")
    git_available = bool(shutil.which(git_executable)) and (REPO / ".git").exists()
    if git_available:
        status_before = _git("status", "--porcelain", "--untracked-files=no")
        if status_before:
            raise RuntimeError("tracked working tree must be clean before generation epoch")
        observed_repo_sha = _git("rev-parse", "HEAD")
        observed_tree_sha = _git("rev-parse", "HEAD^{tree}")
    else:
        if not repo_sha or not tree_sha:
            raise RuntimeError(
                "packaged no-git execution requires explicit --repo-sha and --tree-sha"
            )
        observed_repo_sha = str(repo_sha)
        observed_tree_sha = str(tree_sha)

    if plan.get("repo_sha") and str(plan["repo_sha"]) != observed_repo_sha:
        raise RuntimeError("generation contract repo SHA drift")
    if plan.get("tree_sha") and str(plan["tree_sha"]) != observed_tree_sha:
        raise RuntimeError("generation contract tree SHA drift")

    code_paths = {
        "grammar": REPO / "src/our_system_phase2/services/compositional_grammar.py",
        "generation_epoch": REPO / "src/our_system_phase2/services/compositional_generation_epoch.py",
        "generation_runner": REPO / "scripts/run_cn_compositional_generation_epoch.py",
        "typed_compiler": REPO / "src/our_system_phase2/services/typed_route_compiler.py",
        "streaming_evaluator": REPO / "scripts/run_cn_phase3cm_streaming_qualification.py",
    }
    for name, expected in dict(plan.get("code_hashes") or {}).items():
        path = code_paths.get(str(name))
        if path is None or not path.exists() or _source_sha256(path) != str(expected):
            raise RuntimeError(f"generation contract code hash drift: {name}")
    return observed_repo_sha, observed_tree_sha


def _route_skeleton_rows(unique_pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in unique_pairs:
        grouped[(str(row["route_id"]), str(row["skeleton_id"]))].append(row)
    output: list[dict[str, Any]] = []
    for (route_id, skeleton_id), rows in sorted(grouped.items()):
        output.append(
            {
                "route_id": route_id,
                "skeleton_id": skeleton_id,
                "exact_unique": len(rows),
                "canonical_unique": len({row["canonical_identity"] for row in rows}),
                "policy_count": len({row["policy_id"] for row in rows}),
                "seed_count": len({row["seed"] for row in rows}),
                "source_family_count": len(
                    {family for row in rows for family in row["source_families"]}
                ),
                "depth_min": min(row["expression_depth"] for row in rows),
                "depth_max": max(row["expression_depth"] for row in rows),
                "behavior_unique": None,
                "strict_pairs": 0,
                "matched_positive": 0,
            }
        )
    return output


def _compact_pair_receipt(row: Mapping[str, Any]) -> dict[str, Any]:
    primary = row["primary"]
    control = row["control"]
    return {
        "receipt_schema": "cn_compositional_pair_reconstruction_receipt_v1",
        "attempt_id": row["attempt_id"],
        "route_attempt_index": row["route_attempt_index"],
        "candidate_id": row["candidate_id"],
        "control_candidate_id": row["control_candidate_id"],
        "pair_id": row["pair_id"],
        "route_id": row["route_id"],
        "skeleton_id": row["skeleton_id"],
        "policy_id": row["policy_id"],
        "seed": row["seed"],
        "exact_identity": row["exact_identity"],
        "control_exact_identity": control["exact_identity"],
        "canonical_identity": row["canonical_identity"],
        "canonical_expression": row["canonical_expression"],
        "control_canonical_expression": control["canonical_expression"],
        "declared_field_ids": row["declared_field_ids"],
        "condition_field_ids": list(primary.get("condition_field_ids", ())),
        "source_field_ids": list(primary.get("source_field_ids", ())),
        "representation_ids": list(primary.get("representation_ids", ())),
        "source_families": row["source_families"],
        "expression_depth": row["expression_depth"],
        "operator_path_hash": row["operator_path_hash"],
        "control_constructor_id": primary["control_constructor_id"],
        "clock_contract": primary["clock_contract"],
        "maturity_contract": primary["maturity_contract"],
        "support_unit": primary["support_unit"],
        "parent_candidate_id": row["parent_candidate_id"],
        "mutation_receipt": row["mutation_receipt"],
        "discovering_policy_ids": row["discovering_policy_ids"],
        "discovering_seeds": row["discovering_seeds"],
        "proposal_exposure_count": row["proposal_exposure_count"],
        "owner_assignment_key": row["owner_assignment_key"],
        "access_roles": ["development"],
        "promotion_allowed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--repo-sha", default="")
    parser.add_argument("--tree-sha", default="")
    args = parser.parse_args()

    plan_path = args.plan.resolve()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    registry_path = (REPO / plan["unified_registry_path"]).resolve()
    registry = UnifiedCapabilityRegistry.read(registry_path)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)

    repo_sha, tree_sha = _source_identity(
        plan,
        repo_sha=args.repo_sha,
        tree_sha=args.tree_sha,
    )

    result = build_compositional_generation_epoch(
        registry,
        route_attempt_quotas=plan["generation_contract"]["route_attempt_quotas"],
        policies=plan["policies"],
        seeds=plan["seeds"],
        route_root_allowlist=plan.get("route_root_allowlists"),
    )
    preadmission_cap = min(
        len(result.unique_pairs),
        int(
            plan["generation_contract"].get(
                "structural_preadmission_maximum_pairs",
                int(plan["admission_contract"]["maximum_pairs"]) * 2,
            )
        ),
    )
    preadmission = select_structural_preadmission(
        result.unique_pairs,
        maximum_pairs=preadmission_cap,
    )

    ledger_path = output / "CN_PROPOSAL_EXPOSURE_LEDGER.parquet"
    pd.DataFrame(result.ledger).to_parquet(ledger_path, index=False)
    pair_receipts_path = output / "CN_PAIR_RECEIPTS.jsonl"
    _write_jsonl(
        pair_receipts_path,
        (_compact_pair_receipt(row) for row in result.unique_pairs),
    )
    preadmission_path = output / "CN_STRUCTURAL_PREADMISSION.json"
    _write_json(
        preadmission_path,
        {
            "status": "STRUCTURAL_PREADMISSION_COMPLETE_SIGNAL_SKETCH_PENDING",
            "selection_reward_accessed": False,
            "behavior_sketch_used": False,
            "maximum_final_admission_pairs": int(plan["admission_contract"]["maximum_pairs"]),
            "structural_preadmission_pairs": len(preadmission),
            "exact_identities": [row["exact_identity"] for row in preadmission],
        },
    )

    waterfall_path = output / "CN_ADMISSION_WATERFALL.csv"
    with waterfall_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(result.waterfall[0]))
        writer.writeheader()
        writer.writerows(result.waterfall)
    metrics = _route_skeleton_rows(result.unique_pairs)
    metrics_path = output / "CN_ROUTE_SKELETON_METRICS.csv"
    with metrics_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(metrics[0]))
        writer.writeheader()
        writer.writerows(metrics)

    policy_counts = Counter(str(row["policy_id"]) for row in result.unique_pairs)
    seed_counts = Counter(str(row["seed"]) for row in result.unique_pairs)
    summary = {
        **result.summary,
        "status": (
            "GENERATION_TARGET_MET"
            if len(result.unique_pairs) >= int(plan["generation_contract"]["legal_exact_unique_target"])
            else "GENERATION_NATURAL_UNDERFILL"
        ),
        "repo_sha": repo_sha,
        "tree_sha": tree_sha,
        "plan_sha256": _sha256(plan_path),
        "registry_sha256": _sha256(registry_path),
        "registry_content_hash": registry.registry_hash,
        "structural_preadmission_pairs": len(preadmission),
        "policy_exact_unique_counts": dict(sorted(policy_counts.items())),
        "seed_exact_unique_counts": dict(sorted(seed_counts.items())),
        "behavior_identity_status": "PENDING_DEVELOPMENT_SIGNAL_SKETCH",
        "proposal_partition_semantics": result.summary["proposal_partition_semantics"],
        "proposal_root_scope_hash": result.summary["proposal_root_scope_hash"],
        "formal_search_unfrozen": False,
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
    }
    summary_path = output / "CN_GENERATION_EPOCH_SUMMARY.json"
    _write_json(summary_path, summary)

    artifact_paths = (
        ledger_path,
        pair_receipts_path,
        preadmission_path,
        waterfall_path,
        metrics_path,
        summary_path,
    )
    manifest = {
        "manifest_version": "cn_compositional_nline_epoch1_generation_manifest_v1",
        "stage": "GENERATION_AND_STRUCTURAL_PREADMISSION",
        "repo_sha": repo_sha,
        "tree_sha": tree_sha,
        "plan_sha256": _sha256(plan_path),
        "registry_sha256": _sha256(registry_path),
        "artifacts": [
            {
                "path": str(path.relative_to(REPO)).replace("\\", "/"),
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
            for path in artifact_paths
        ],
        "data_roles_accessed": [],
        "economic_evaluator_accessed": False,
        "manifest_hash": "",
    }
    manifest["manifest_hash"] = stable_hash({key: value for key, value in manifest.items() if key != "manifest_hash"})
    _write_json(output / "CN_GENERATION_ARTIFACT_MANIFEST.json", manifest)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
