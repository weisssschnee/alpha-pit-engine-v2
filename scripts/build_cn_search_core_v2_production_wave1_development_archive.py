"""Cluster and rank Production Wave 1 candidates using DEVELOPMENT_ONLY evidence.

This is not an alpha qualification step.  It consumes only the Wave 1 result
archive after the post-run audit has closed and produces deterministic behavior
clusters plus a development-priority shortlist.  No validation/OOS data is read.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE1_DEVELOPMENT_ARCHIVE_READY_NOT_ALPHA_QUALIFIED"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _credit(row: Mapping[str, Any]) -> dict[str, Any]:
    uplift = dict(row.get("uplift") or {})
    credit = dict(uplift.get("program_credit") or {})
    required = {
        "matched_net_reward_increment",
        "matched_cumulative_net_return_increment",
        "cross_window_positive_increment_count",
        "cross_window_matched_consistency",
        "robust_median_window_return_increment",
        "lower_tail_window_return_increment",
        "turnover_differential",
    }
    missing = sorted(required - set(credit))
    if missing:
        raise RuntimeError(f"development candidate credit incomplete: {missing}")
    return credit


def development_priority_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Higher-quality DEVELOPMENT evidence sorts first; exact identity breaks ties.

    The ordering is deliberately lexicographic rather than a learned/weighted
    score so this archive step cannot tune a new objective after observing the
    Wave.  Stability and worst-window evidence dominate aggregate uplift.
    """
    credit = _credit(row)
    return (
        -int(bool(row.get("stable"))),
        -int(credit["cross_window_positive_increment_count"]),
        -float(credit["cross_window_matched_consistency"]),
        -float(credit["lower_tail_window_return_increment"]),
        -float(credit["robust_median_window_return_increment"]),
        -float(credit["matched_cumulative_net_return_increment"]),
        -float(credit["matched_net_reward_increment"]),
        abs(float(credit["turnover_differential"])),
        str(row["exact_identity"]),
    )


def _archive_row(row: Mapping[str, Any], *, behavior_cluster_size: int, behavior_rank: int, global_rank: int) -> dict[str, Any]:
    credit = _credit(row)
    payload = {
        "schema_version": "cn_search_core_v2_production_wave1_development_candidate_v1",
        "development_rank": int(global_rank),
        "behavior_cluster_rank": int(behavior_rank),
        "behavior_cluster_size": int(behavior_cluster_size),
        "exact_identity": str(row["exact_identity"]),
        "template_id": str(row["template_id"]),
        "base_component_id": str(row["base_component_id"]),
        "behavior_pair_identity": str(row.get("behavior_pair_identity") or ""),
        "structural_region_identity": str(row["structural_region_identity"]),
        "productive": bool(row["productive"]),
        "stable": bool(row["stable"]),
        "development_credit": {
            "matched_net_reward_increment": float(credit["matched_net_reward_increment"]),
            "matched_cumulative_net_return_increment": float(credit["matched_cumulative_net_return_increment"]),
            "cross_window_positive_increment_count": int(credit["cross_window_positive_increment_count"]),
            "cross_window_matched_consistency": float(credit["cross_window_matched_consistency"]),
            "robust_median_window_return_increment": float(credit["robust_median_window_return_increment"]),
            "lower_tail_window_return_increment": float(credit["lower_tail_window_return_increment"]),
            "turnover_differential": float(credit["turnover_differential"]),
        },
        "classification": "STABLE_DEVELOPMENT_CANDIDATE" if bool(row["stable"]) else "PRODUCTIVE_DEVELOPMENT_CANDIDATE",
        "alpha_qualified": False,
        "validation_read": False,
        "oos_read": False,
        "source_result_payload_sha256": str(row["result_payload_sha256"]),
    }
    payload["archive_row_sha256"] = stable_hash(payload)
    return payload


def build(
    *,
    postrun_audit: Path,
    productive_archive: Path,
    stable_archive: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    audit = _read(postrun_audit)
    audit_hash = _verify(audit, "audit_payload_sha256", "Production Wave 1 postrun audit")
    if audit.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_POSTRUN_AUDIT_COMPLETE_ARCHIVE_READY":
        raise RuntimeError("Production Wave 1 postrun audit not archive-ready")
    if audit["project_control_recommendation"].get("automatic_validation_authorized") is not False:
        raise RuntimeError("development archive must not carry validation authority")
    productive = _read_jsonl(productive_archive)
    stable = _read_jsonl(stable_archive)
    if len(productive) != int(audit["candidate_archives"]["productive_count"]):
        raise RuntimeError("productive archive count drift")
    if len(stable) != int(audit["candidate_archives"]["stable_count"]):
        raise RuntimeError("stable archive count drift")
    if _sha(productive_archive) != str(audit["candidate_archives"]["productive_file_sha256"]):
        raise RuntimeError("productive archive file drift")
    if _sha(stable_archive) != str(audit["candidate_archives"]["stable_file_sha256"]):
        raise RuntimeError("stable archive file drift")
    stable_ids = {str(row["exact_identity"]) for row in stable}
    productive_ids = {str(row["exact_identity"]) for row in productive}
    if not stable_ids <= productive_ids:
        raise RuntimeError("stable archive must be subset of productive archive")
    if len(productive_ids) != len(productive):
        raise RuntimeError("productive archive exact duplicate")

    clusters: dict[str, list[dict[str, Any]]] = {}
    for row in productive:
        behavior = str(row.get("behavior_pair_identity") or "")
        cluster_id = behavior or f"NO_BEHAVIOR::{row['structural_region_identity']}"
        clusters.setdefault(cluster_id, []).append(row)
    ranked_all = sorted(productive, key=development_priority_key)
    global_rank = {str(row["exact_identity"]): ordinal + 1 for ordinal, row in enumerate(ranked_all)}
    representatives: list[dict[str, Any]] = []
    cluster_rows: list[dict[str, Any]] = []
    for cluster_id in sorted(clusters):
        rows = sorted(clusters[cluster_id], key=development_priority_key)
        representative = rows[0]
        representatives.append(representative)
        cluster_rows.append(
            {
                "schema_version": "cn_search_core_v2_production_wave1_behavior_cluster_v1",
                "behavior_cluster_id": cluster_id,
                "cluster_size": len(rows),
                "stable_count": sum(bool(row["stable"]) for row in rows),
                "template_count": len({str(row["template_id"]) for row in rows}),
                "structural_region_count": len({str(row["structural_region_identity"]) for row in rows}),
                "representative_exact_identity": str(representative["exact_identity"]),
                "member_exact_identities": [str(row["exact_identity"]) for row in rows],
            }
        )
        cluster_rows[-1]["cluster_row_sha256"] = stable_hash(cluster_rows[-1])
    representatives.sort(key=development_priority_key)
    representative_rows = [
        _archive_row(
            row,
            behavior_cluster_size=len(clusters[str(row.get("behavior_pair_identity") or "") or f"NO_BEHAVIOR::{row['structural_region_identity']}"]),
            behavior_rank=ordinal + 1,
            global_rank=global_rank[str(row["exact_identity"])],
        )
        for ordinal, row in enumerate(representatives)
    ]

    # Deterministic diversity frontier: first representative for each previously
    # unseen (template, structural region), preserving development-priority order.
    frontier: list[dict[str, Any]] = []
    seen_cells: set[tuple[str, str]] = set()
    for row in representative_rows:
        cell = (str(row["template_id"]), str(row["structural_region_identity"]))
        if cell in seen_cells:
            continue
        seen_cells.add(cell)
        frontier.append(row)

    summary = {
        "schema_version": "cn_search_core_v2_production_wave1_development_archive_summary_v1",
        "status": STATUS,
        "postrun_audit_payload_sha256": audit_hash,
        "source_productive_file_sha256": _sha(productive_archive),
        "source_stable_file_sha256": _sha(stable_archive),
        "productive_candidate_count": len(productive),
        "stable_candidate_count": len(stable),
        "behavior_cluster_count": len(clusters),
        "behavior_representative_count": len(representative_rows),
        "diversity_frontier_count": len(frontier),
        "stable_behavior_representative_count": sum(bool(row["stable"]) for row in representative_rows),
        "template_representative_counts": dict(sorted(Counter(str(row["template_id"]) for row in representative_rows).items())),
        "classification": "DEVELOPMENT_ONLY_NOT_ALPHA_QUALIFIED",
        "ranking_contract": [
            "stable_desc",
            "cross_window_positive_increment_count_desc",
            "cross_window_matched_consistency_desc",
            "lower_tail_window_return_increment_desc",
            "robust_median_window_return_increment_desc",
            "matched_cumulative_net_return_increment_desc",
            "matched_net_reward_increment_desc",
            "abs_turnover_differential_asc",
            "exact_identity_asc",
        ],
        "validation_read": False,
        "holdout_read": False,
        "forward_read": False,
        "oos_authority": "NONE",
        "automatic_promotion_authorized": False,
        "financial_evaluation_executed": False,
    }
    summary["archive_summary_payload_sha256"] = stable_hash(summary)
    return summary, representative_rows, frontier


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postrun-audit", type=Path, required=True)
    parser.add_argument("--productive-archive", type=Path, required=True)
    parser.add_argument("--stable-archive", type=Path, required=True)
    parser.add_argument("--summary-output", type=Path, required=True)
    parser.add_argument("--representatives-output", type=Path, required=True)
    parser.add_argument("--frontier-output", type=Path, required=True)
    args = parser.parse_args(argv)
    summary, representatives, frontier = build(
        postrun_audit=args.postrun_audit,
        productive_archive=args.productive_archive,
        stable_archive=args.stable_archive,
    )
    args.summary_output.parent.mkdir(parents=True, exist_ok=True)
    args.summary_output.write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    _write_jsonl(args.representatives_output, representatives)
    _write_jsonl(args.frontier_output, frontier)
    print(json.dumps({"status": summary["status"], "productive": summary["productive_candidate_count"], "stable": summary["stable_candidate_count"], "clusters": summary["behavior_cluster_count"], "representatives": summary["behavior_representative_count"], "frontier": summary["diversity_frontier_count"], "payload": summary["archive_summary_payload_sha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
