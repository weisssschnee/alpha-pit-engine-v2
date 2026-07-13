"""Analyze the two frozen Sprint-2 Repair CANARY seeds and build a union rerank."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score

from our_system_phase2.runtime.cn_b1s_development_canary import (
    _atomic_csv,
    _json_hash,
    _sha256,
    _select_seed_set,
    build_admissions,
    build_strict_pack,
)
from our_system_phase2.services.atomic_checkpoint import atomic_write_json


ANALYSIS_VERSION = "cn_sprint2_repair_canary_analysis_v1"
TARGET_HORIZON = 5


def _artifact_root(seed_root: Path) -> Path:
    latest = json.loads(
        (seed_root / "output/run_records/cn_b1s_development_canary.latest.json").read_text(
            encoding="utf-8-sig"
        )
    )
    attempt = json.loads(Path(latest["latest_attempt"]).read_text(encoding="utf-8-sig"))
    if attempt["status"] not in {
        "CN_B1S_CANARY_COMPLETED", "CN_B1S_CANARY_COMPLETED_WITH_NATURAL_UNDERFILL"
    }:
        raise ValueError(f"Repair CANARY seed is incomplete: {seed_root}")
    summary_path = next(
        Path(row["path"]) for row in attempt["outputs"] if row["purpose"] == "summary"
    )
    return summary_path.parent


def _strict_evidence(proposals: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    for column in (
        "horizon_bars", "ic_mean", "ic_abs_lcb95", "cost_adjusted_abs_ic",
        "mean_one_way_turnover",
    ):
        metrics[column] = pd.to_numeric(metrics[column], errors="coerce")
    h5 = metrics[metrics["horizon_bars"].eq(TARGET_HORIZON)].copy()
    benchmark = h5[h5["lane_id"].eq("benchmark_competitor")]["cost_adjusted_abs_ic"]
    benchmark_median = float(benchmark.median()) if len(benchmark) else 0.0
    aggregate = metrics.groupby("candidate_id", sort=False).agg(
        strict_min_lcb=("ic_abs_lcb95", "min"),
        strict_min_cost_adjusted=("cost_adjusted_abs_ic", "min"),
        strict_max_turnover=("mean_one_way_turnover", "max"),
        strict_horizon_count=("horizon_bars", "nunique"),
        strict_signed_min=("ic_mean", "min"),
        strict_signed_max=("ic_mean", "max"),
    ).reset_index()
    evidence = h5.merge(aggregate, on="candidate_id", validate="one_to_one")
    proposal_columns = [
        column for column in proposals.columns
        if column not in evidence.columns or column == "candidate_id"
    ]
    evidence = evidence.merge(
        proposals[proposal_columns], on="candidate_id", how="left", validate="one_to_one"
    )
    same_sign = evidence["strict_signed_min"].mul(evidence["strict_signed_max"]).gt(0)
    block_ok = (
        pd.to_numeric(evidence["proxy_worst_time_block_abs_ic"], errors="coerce").fillna(0).gt(0)
        & pd.to_numeric(evidence["proxy_time_block_stability"], errors="coerce").fillna(0).ge(0.5)
    )
    evidence["strict_benchmark_increment"] = evidence["cost_adjusted_abs_ic"] - benchmark_median
    evidence["strict_stable"] = (
        evidence["strict_min_lcb"].gt(0)
        & evidence["strict_min_cost_adjusted"].gt(0)
        & evidence["strict_max_turnover"].le(0.75)
        & evidence["strict_horizon_count"].ge(4)
        & block_ok
        & same_sign
    )
    evidence["strict_priority_hit"] = (
        evidence["strict_benchmark_increment"].gt(0) & evidence["strict_stable"]
    ).astype(int)
    evidence["benchmark_h5_cost_adjusted_median"] = benchmark_median
    return evidence


def _precision(labels: pd.Series, scores: pd.Series, fraction: float = 0.10) -> dict[str, Any]:
    count = max(1, int(math.ceil(len(labels) * fraction)))
    order = np.argsort(-pd.to_numeric(scores, errors="coerce").fillna(-np.inf).to_numpy(), kind="stable")
    base = float(labels.mean()) if len(labels) else 0.0
    precision = float(labels.iloc[order[:count]].mean()) if len(labels) else 0.0
    return {
        "count": len(labels),
        "selected_count": count,
        "base_rate": base,
        "precision": precision,
        "lift": precision / base if base > 0 else 0.0,
    }


def _rx_strict(evidence: pd.DataFrame) -> dict[str, Any]:
    rx = evidence[evidence["lane_id"].eq("rx_ucb")].copy()
    output: dict[str, Any] = {}
    for stage in ("control", "adaptive"):
        source = rx[rx["proposal_stage"].eq(stage)]
        output[stage] = {
            "strict_candidate_count": len(source),
            "strict_hit_rate": float(source["strict_priority_hit"].mean()) if len(source) else 0.0,
            "mean_h5_cost_adjusted_abs_ic": float(source["cost_adjusted_abs_ic"].mean()) if len(source) else 0.0,
            "mean_strict_min_lcb": float(source["strict_min_lcb"].mean()) if len(source) else 0.0,
            "mean_benchmark_increment": float(source["strict_benchmark_increment"].mean()) if len(source) else 0.0,
            "cluster_count": int(source["signal_cluster_id"].nunique()),
            "cluster_yield": float(source["signal_cluster_id"].nunique() / max(1, len(source))),
        }
    control, adaptive = output["control"], output["adaptive"]
    deltas = {
        key: float(adaptive[key] - control[key])
        for key in (
            "strict_hit_rate", "mean_h5_cost_adjusted_abs_ic", "mean_strict_min_lcb",
            "mean_benchmark_increment", "cluster_yield",
        )
    }
    output["deltas"] = deltas
    output["strict_outperformance_gate"] = bool(
        deltas["mean_h5_cost_adjusted_abs_ic"] > 0
        and deltas["mean_strict_min_lcb"] > 0
        and deltas["mean_benchmark_increment"] > 0
        and deltas["cluster_yield"] >= -0.05
    )
    return output


def _admission_strict_relationship(
    artifact_root: Path, evidence: pd.DataFrame
) -> dict[str, Any]:
    evaluated = set(evidence["exact_identity"].astype(str))
    by_exact = evidence.set_index(evidence["exact_identity"].astype(str))
    output = {}
    for strategy in (
        "current_scalar", "strict_priority", "stratified_diversity", "quality_diversity_hybrid"
    ):
        frame = pd.read_csv(artifact_root / f"admission_{strategy}.csv", low_memory=False)
        identities = set(frame["exact_identity"].astype(str))
        overlap = sorted(evaluated & identities)
        subset = by_exact.loc[overlap] if overlap else evidence.iloc[0:0]
        output[strategy] = {
            "admission_count": len(frame),
            "strict_evaluated_overlap_count": len(subset),
            "strict_hit_rate_on_evaluated_overlap": (
                float(subset["strict_priority_hit"].mean()) if len(subset) else None
            ),
            "mean_h5_cost_adjusted_on_evaluated_overlap": (
                float(subset["cost_adjusted_abs_ic"].mean()) if len(subset) else None
            ),
        }
    return output


def _seed_report(seed: str, artifact_root: Path) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    proposals = pd.read_csv(artifact_root / "candidate_proposals.csv", low_memory=False)
    metrics = pd.read_csv(artifact_root / "strict_metrics.csv", low_memory=False)
    summary = json.loads((artifact_root / "summary.json").read_text(encoding="utf-8-sig"))
    ledger = json.loads(
        (artifact_root / "development_only_read_ledger.json").read_text(encoding="utf-8-sig")
    )
    new_clusters = json.loads(
        (artifact_root / "temporal_event_state_new_clusters.json").read_text(encoding="utf-8-sig")
    )
    evidence = _strict_evidence(proposals, metrics)
    selector = {
        "strict_priority": _precision(evidence["strict_priority_hit"], evidence["strict_priority_score"]),
        "current_scalar": _precision(evidence["strict_priority_hit"], evidence["proxy_reward"]),
        "hard_gate_rank": _precision(evidence["strict_priority_hit"], evidence["hard_gate_rank_score"]),
    }
    zero_access = all(
        int(ledger[key]) == 0
        for key in (
            "validation_rows_read", "holdout_rows_read", "forward_rows_read",
            "forbidden_file_open_count", "forbidden_row_group_read_count",
        )
    )
    report = {
        "seed": seed,
        "status": summary["status"],
        "proposal_count": int(summary["proposal_count"]),
        "development_eligible_count": int(summary["development_eligible_count"]),
        "strict_priority_eligible_count": int(summary["strict_priority_eligible_count"]),
        "strict_count": int(summary["strict_count"]),
        "zero_forbidden_access": zero_access,
        "selector_realized_strict": selector,
        "rx_ucb_strict": _rx_strict(evidence),
        "new_clusters": new_clusters,
        "admission_strict_relationship": _admission_strict_relationship(artifact_root, evidence),
    }
    return report, proposals, evidence


def _shared_stability(left: pd.DataFrame, right: pd.DataFrame) -> dict[str, Any]:
    stages = {"fixed", "control"}
    a = (
        left[left["proposal_stage"].isin(stages)]
        .sort_values("candidate_id", kind="mergesort")
        .drop_duplicates("exact_identity", keep="first")
        .copy()
    )
    b = (
        right[right["proposal_stage"].isin(stages)]
        .sort_values("candidate_id", kind="mergesort")
        .drop_duplicates("exact_identity", keep="first")
        .copy()
    )
    shared = a.merge(b, on="exact_identity", suffixes=("_a", "_b"), validate="one_to_one")
    expected = min(len(a), len(b))
    proxy_rank_a = pd.to_numeric(shared["proxy_reward_a"], errors="coerce").rank(method="average")
    proxy_rank_b = pd.to_numeric(shared["proxy_reward_b"], errors="coerce").rank(method="average")
    priority_rank_a = pd.to_numeric(shared["strict_priority_score_a"], errors="coerce").rank(method="average")
    priority_rank_b = pd.to_numeric(shared["strict_priority_score_b"], errors="coerce").rank(method="average")
    return {
        "expected_backbone_count": expected,
        "shared_exact_count": len(shared),
        "exact_identity_complete": len(shared) == expected,
        "development_proxy_rank_correlation": float(proxy_rank_a.corr(proxy_rank_b)),
        "strict_priority_rank_correlation": float(priority_rank_a.corr(priority_rank_b)),
        "behaviour_cluster_ari": float(
            adjusted_rand_score(shared["signal_cluster_id_a"], shared["signal_cluster_id_b"])
        ),
        "behaviour_cluster_nmi": float(
            normalized_mutual_info_score(shared["signal_cluster_id_a"], shared["signal_cluster_id_b"])
        ),
        "development_eligibility_agreement": float(
            np.mean(shared["development_eligible_a"].astype(str) == shared["development_eligible_b"].astype(str))
        ),
        "strict_priority_gate_agreement": float(
            np.mean(shared["strict_priority_eligible_a"].astype(str) == shared["strict_priority_eligible_b"].astype(str))
        ),
    }


def _set_overlap(left: set[str], right: set[str]) -> dict[str, Any]:
    union = left | right
    return {
        "left_count": len(left), "right_count": len(right),
        "intersection_count": len(left & right),
        "jaccard": len(left & right) / max(1, len(union)),
    }


def _union_rerank(
    left: pd.DataFrame,
    right: pd.DataFrame,
    contract: Mapping[str, Any],
    output: Path,
) -> dict[str, Any]:
    a = left.copy()
    b = right.copy()
    for frame in (a, b):
        for column in (
            "parent_id", "family_id", "semantic_bucket", "canonical_identity",
            "exact_identity", "candidate_id", "lane_id", "proposal_stage",
        ):
            frame[column] = frame[column].fillna("").astype(str)
        for column in ("development_eligible", "strict_priority_eligible", "legal", "materialized"):
            frame[column] = frame[column].map(
                lambda value: value if isinstance(value, (bool, np.bool_))
                else str(value).strip().lower() in {"1", "true", "yes"}
            )
    a["source_seed"] = "seed_a"
    b["source_seed"] = "seed_b"
    # Align seed-B clusters through shared exact identities. Unmapped adaptive
    # clusters remain seed-specific, which is conservative for one-cluster-one-vote.
    shared = a[["exact_identity", "signal_cluster_id"]].drop_duplicates("exact_identity").merge(
        b[["exact_identity", "signal_cluster_id"]].drop_duplicates("exact_identity"),
        on="exact_identity", suffixes=("_a", "_b"), validate="one_to_one"
    )
    b_to_a = (
        shared.groupby("signal_cluster_id_b")["signal_cluster_id_a"]
        .agg(lambda values: int(pd.Series(values).mode().iloc[0]))
        .to_dict()
    )
    max_a = int(pd.to_numeric(a["signal_cluster_id"], errors="coerce").max())
    b_unique = sorted(set(pd.to_numeric(b["signal_cluster_id"], errors="coerce").fillna(0).astype(int)) - set(b_to_a))
    b_fallback = {cluster: max_a + index + 1 for index, cluster in enumerate(b_unique)}
    b["signal_cluster_id"] = [
        int(b_to_a[int(value)] if int(value) in b_to_a else b_fallback[int(value)])
        for value in pd.to_numeric(b["signal_cluster_id"], errors="coerce").fillna(0)
    ]
    combined = pd.concat([a, b], ignore_index=True, sort=False)
    combined = combined.sort_values(
        ["strict_priority_score", "proxy_reward", "source_seed", "candidate_id"],
        ascending=[False, False, True, True], kind="mergesort",
    ).drop_duplicates("exact_identity", keep="first")
    rows = combined.to_dict("records")
    admissions = build_admissions(rows, contract)
    strict = build_strict_pack(admissions["quality_diversity_hybrid"], contract)
    output.mkdir(parents=True, exist_ok=True)
    _atomic_csv(combined, output / "union_exact_deduplicated_proposals.csv")
    for strategy in (
        "current_scalar", "strict_priority", "stratified_diversity", "quality_diversity_hybrid"
    ):
        _atomic_csv(pd.DataFrame(admissions[strategy]), output / f"union_admission_{strategy}.csv")
    strict_frame = pd.DataFrame(strict)
    strict_frame["pack_role"] = "UNION_RERANK_RESEARCH_ONLY_NOT_PROMOTED"
    strict_frame["forward_2026_allowed"] = False
    _atomic_csv(strict_frame, output / "union_reranked_pack.pre_metrics.csv")
    return {
        "exact_union_count": len(combined),
        "shared_exact_count": len(set(a["exact_identity"].astype(str)) & set(b["exact_identity"].astype(str))),
        "admission_counts": {name: len(admissions[name]) for name in (
            "current_scalar", "strict_priority", "stratified_diversity", "quality_diversity_hybrid"
        )},
        "reranked_pack_count": len(strict),
        "cluster_alignment_policy": "shared_exact_majority_map_then_seed_specific_unmapped_clusters",
        "promotion_made": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed-a-root", type=Path, required=True)
    parser.add_argument("--seed-b-root", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--expected-repo-sha", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    started = datetime.now(timezone.utc)
    roots = {"seed_a": _artifact_root(args.seed_a_root), "seed_b": _artifact_root(args.seed_b_root)}
    reports: dict[str, Any] = {}
    proposals: dict[str, pd.DataFrame] = {}
    evidence: dict[str, pd.DataFrame] = {}
    for seed in ("seed_a", "seed_b"):
        reports[seed], proposals[seed], evidence[seed] = _seed_report(seed, roots[seed])
    contract_payload = json.loads(args.contract.read_text(encoding="utf-8-sig"))
    contract = _select_seed_set(contract_payload, "seed_a")
    shared = _shared_stability(proposals["seed_a"], proposals["seed_b"])
    pack_sets = {
        seed: set(evidence[seed]["exact_identity"].astype(str)) for seed in ("seed_a", "seed_b")
    }
    mechanism_sets = {
        seed: set(evidence[seed]["hypothesis_arm"].astype(str)) for seed in ("seed_a", "seed_b")
    }
    primitive_sets = {
        seed: set(evidence[seed]["primitive_family"].astype(str)) for seed in ("seed_a", "seed_b")
    }
    union_root = args.output_root / "union_rerank"
    union = _union_rerank(proposals["seed_a"], proposals["seed_b"], contract, union_root)
    event_failed = all(
        int(reports[seed]["new_clusters"]["event_conditioned"]["new_vs_static_count"]) == 0
        for seed in reports
    )
    state_succeeded = all(
        int(reports[seed]["new_clusters"]["state_transition"]["new_vs_static_count"]) > 0
        for seed in reports
    )
    zero_access = all(report["zero_forbidden_access"] for report in reports.values())
    rx_two_seed = all(report["rx_ucb_strict"]["strict_outperformance_gate"] for report in reports.values())
    epoch_c_gate = bool(
        contract["strict_priority_contract"]["offline_evidence_gate_passed"]
        and state_succeeded
        and rx_two_seed
        and zero_access
        and shared["exact_identity_complete"]
    )
    report = {
        "analysis_version": ANALYSIS_VERSION,
        "repo_sha": args.expected_repo_sha,
        "started_at": started.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "contract_canonical_sha256": _json_hash(contract_payload),
        "seed_reports": reports,
        "shared_backbone": shared,
        "strict_pack_overlap": _set_overlap(pack_sets["seed_a"], pack_sets["seed_b"]),
        "economic_hypothesis_overlap": _set_overlap(mechanism_sets["seed_a"], mechanism_sets["seed_b"]),
        "primitive_family_overlap": _set_overlap(primitive_sets["seed_a"], primitive_sets["seed_b"]),
        "union_rerank": union,
        "event_decision": "EVENT_GENERATOR_NOT_OPERATIONAL" if event_failed else "EVENT_GENERATOR_OPERATIONAL",
        "state_decision": "STATE_GENERATOR_OPERATIONAL" if state_succeeded else "STATE_GENERATOR_NOT_OPERATIONAL",
        "rx_two_seed_strict_outperformance": rx_two_seed,
        "zero_forbidden_access": zero_access,
        "epoch_c_entry_gate": epoch_c_gate,
        "next_state": "RUN_FROZEN_DEVELOPMENT_EPOCH_C" if epoch_c_gate else "REVISE_BEFORE_EPOCH_C",
        "boundaries": {
            "forward_2026_sealed": True,
            "candidate_promotion_made": False,
            "cross_sprint_memory_written": False,
        },
    }
    args.output_root.mkdir(parents=True, exist_ok=True)
    report_path = args.output_root / "repair_canary_joint_analysis.json"
    atomic_write_json(report_path, report)
    outputs = [report_path, *sorted(union_root.glob("*"))]
    manifest = {
        "manifest_version": "cn_sprint2_repair_canary_analysis_manifest_v1",
        "status": "COMPLETED",
        "experiment_identity": {
            "repo_sha": args.expected_repo_sha,
            "analyzer_code_sha256": _sha256(Path(__file__)),
            "contract_canonical_sha256": _json_hash(contract_payload),
            "seed_roots": {seed: str(root) for seed, root in roots.items()},
        },
        "outputs": [
            {"path": str(path), "size": path.stat().st_size, "sha256": _sha256(path)}
            for path in outputs
        ],
        "result": {
            "event_decision": report["event_decision"],
            "state_decision": report["state_decision"],
            "rx_two_seed_strict_outperformance": rx_two_seed,
            "epoch_c_entry_gate": epoch_c_gate,
        },
        "reproducibility": "YES_FOR_FROZEN_INPUTS",
        "continuation": report["next_state"],
        "failure": None,
    }
    atomic_write_json(args.output_root / "run_manifest.json", manifest)
    print(json.dumps(manifest["result"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
