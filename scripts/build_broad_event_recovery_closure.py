"""Build the Broad Event discovery entry pack and evidence indexes from r5 outputs."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def build(repo: Path, report_root: Path) -> dict[str, object]:
    r5 = report_root / "r5_completed"
    summary = json.loads((r5 / "summary.json").read_text(encoding="utf-8"))
    preflight = json.loads((r5 / "preflight.json").read_text(encoding="utf-8"))
    access = json.loads((r5 / "access_ledger.json").read_text(encoding="utf-8"))
    candidates = pd.read_csv(r5 / "candidate_results.csv")
    if summary["status"] != "BROAD_EVENT_INCREMENT_OBSERVED_REPRODUCIBLE":
        raise RuntimeError("r5 is not eligible for a discovery entry pack")
    if not preflight.get("canary_authorized") or any(
        int(access.get(key, -1)) != 0
        for key in (
            "validation_reads", "holdout_reads", "forward_2026_reads",
            "forbidden_file_reads", "forbidden_row_group_reads",
        )
    ):
        raise RuntimeError("r5 access or preflight evidence is not clean")
    shared = set(summary["shared_new_behavior_mechanisms"])
    event = candidates.loc[
        candidates["lane"].eq("event_conditioned") & candidates["mechanism_id"].isin(shared)
    ].copy()
    if event.groupby("mechanism_id")["seed"].nunique().ne(2).any():
        raise RuntimeError("a discovery mechanism is not reproduced by both seeds")
    mechanisms = []
    for mechanism_id, group in event.groupby("mechanism_id", sort=True):
        first = group.iloc[0]
        mechanisms.append(
            {
                "mechanism_id": str(mechanism_id),
                "behavior_cluster_id": str(first["behavior_cluster_id"]),
                "source": str(first["source"]),
                "horizon_bars": int(first["horizon"]),
                "transform": str(first["transform"]),
                "variant": str(first["variant"]),
                "candidate_promotion": False,
                "seed_evidence": [
                    {
                        "seed": int(row.seed),
                        "exact_behavior_id": str(row.exact_behavior_id),
                        "support": int(row.support),
                        "matched_increment": float(row.matched_increment),
                        "consistent_positive_blocks": int(row.consistent_positive_blocks),
                        "top_date_share": float(row.top_date_share),
                        "top_symbol_share": float(row.top_symbol_share),
                    }
                    for row in group.sort_values("seed").itertuples(index=False)
                ],
            }
        )
    pack = {
        "version": "cn_broad_event_discovery_entry_pack_v1",
        "status": "BROAD_EVENT_DISCOVERY_ENTRY_AUTHORIZED",
        "source_run_repo_sha": summary["repo_sha"],
        "contract_hash": summary["contract_hash"],
        "semantic_registry_hash": summary["semantic_registry_hash"],
        "mechanism_count": len(mechanisms),
        "behavior_cluster_count": len({row["behavior_cluster_id"] for row in mechanisms}),
        "sources": sorted({row["source"] for row in mechanisms}),
        "mechanisms": mechanisms,
        "boundaries": {
            "development_only": True,
            "candidate_promotion": False,
            "forward_2026_access": False,
            "cross_sprint_memory": False,
            "automatic_main_search_entry": False,
        },
    }
    _write_json(report_root / "DISCOVERY_ENTRY_PACK.json", pack)
    status = {
        "version": "cn_broad_event_recovery_status_v2",
        "status": "CN_BROAD_EVENT_SYSTEM_RECOVERY_COMPLETED_DISCOVERY_ELIGIBLE",
        "supersedes": "runtime/run_plans/cn_broad_event_recovery_status_v1.json",
        "supersession_scope": "active Broad Event recovery status only; historical Sprint-2 tables remain unchanged",
        "fixed_conclusions": [
            "BROAD_EVENT_INCREMENT_OBSERVED_REPRODUCIBLE",
            "BROAD_EVENT_DISCOVERY_ENTRY_AUTHORIZED",
            "FORWARD_2026_SEALED",
            "NO_CANDIDATE_PROMOTION",
            "NO_CROSS_SPRINT_ADAPTIVE_MEMORY",
        ],
        "episode_count": int(summary["episode_count"]),
        "operational_event_sources": summary["operational_event_sources"],
        "reproduced_mechanism_count": len(mechanisms),
        "reproduced_new_behavior_cluster_count": pack["behavior_cluster_count"],
        "reproduced_sources": pack["sources"],
        "limit_lifecycle": summary["limit_validation"],
        "mean_all_candidate_matched_increment": summary["mean_matched_increment"],
        "interpretation": "localized reproducible Event mechanisms exist; Broad Event is not uniformly superior",
        "evidence": {
            "summary": "reports/cn_broad_event_recovery_20260713/r5_completed/summary.json",
            "behavior_registry": "reports/cn_broad_event_recovery_20260713/r5_completed/behavior_registry.json",
            "candidate_results": "reports/cn_broad_event_recovery_20260713/r5_completed/candidate_results.csv",
            "run_manifest": "reports/cn_broad_event_recovery_20260713/r5_completed/run_manifest.json",
            "discovery_entry_pack": "reports/cn_broad_event_recovery_20260713/DISCOVERY_ENTRY_PACK.json",
        },
    }
    _write_json(repo / "runtime/run_plans/cn_broad_event_recovery_status_v2.json", status)
    run_manifest = {
        "version": "cn_broad_event_recovery_closure_manifest_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status["status"],
        "runs": [
            {"id": "r2", "status": "INSUFFICIENT_SUPPORT", "canary_executed": False},
            {"id": "r3", "status": "SEMANTIC_CONTRACT_INVALID", "canary_executed": False},
            {"id": "r4", "status": "CANARY_NO_INCREMENT_OBSERVED_REPORTING_CONTRACT_SUPERSEDED", "canary_executed": True},
            {"id": "r5", "status": summary["status"], "canary_executed": True, "repo_sha": summary["repo_sha"]},
        ],
        "r5_contract_hash": summary["contract_hash"],
        "data_release_manifest_sha256": summary["data_release_manifest_sha256"],
        "split_manifest_sha256": summary["split_manifest_sha256"],
        "forbidden_access_counters_zero": True,
        "candidate_promotion": False,
        "forward_2026_accessed": False,
        "cross_sprint_memory_persisted": False,
        "discovery_entry_pack_sha256": _sha256(report_root / "DISCOVERY_ENTRY_PACK.json"),
        "r5_run_manifest_sha256": _sha256(r5 / "run_manifest.json"),
    }
    _write_json(report_root / "RUN_MANIFEST.json", run_manifest)
    return {"pack": pack, "status": status, "manifest": run_manifest}


def refresh_index(repo: Path, report_root: Path) -> dict[str, object]:
    artifacts = []
    for path in sorted(report_root.rglob("*")):
        if not path.is_file() or path.name == "ARTIFACT_INDEX.json":
            continue
        artifacts.append(
            {
                "path": path.relative_to(repo).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    status_path = repo / "runtime/run_plans/cn_broad_event_recovery_status_v2.json"
    artifacts.append(
        {
            "path": status_path.relative_to(repo).as_posix(),
            "size_bytes": status_path.stat().st_size,
            "sha256": _sha256(status_path),
        }
    )
    index = {
        "version": "cn_broad_event_recovery_artifact_index_v1",
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
        "status": "CN_BROAD_EVENT_SYSTEM_RECOVERY_COMPLETED_DISCOVERY_ELIGIBLE",
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }
    _write_json(report_root / "ARTIFACT_INDEX.json", index)
    return index


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--report-root", type=Path, default=Path("reports/cn_broad_event_recovery_20260713"))
    args = parser.parse_args()
    repo = args.repo.resolve()
    report_root = args.report_root if args.report_root.is_absolute() else repo / args.report_root
    result = build(repo, report_root)
    index = refresh_index(repo, report_root)
    print(json.dumps({
        "status": result["status"]["status"],
        "mechanisms": result["pack"]["mechanism_count"],
        "behavior_clusters": result["pack"]["behavior_cluster_count"],
        "artifacts": index["artifact_count"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
