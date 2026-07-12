"""Machine-readable diagnostics for the CN generator research funnel."""

from __future__ import annotations

from collections import Counter
import math
import re
from typing import Any, Mapping

import numpy as np
import pandas as pd


DIAGNOSTIC_VERSION = "cn_generator_funnel_diagnostics_v1"
_PRIMITIVE = re.compile(r"([A-Za-z][A-Za-z0-9_]*)\(")


def _as_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().eq("true")


def _finite(values: pd.Series) -> np.ndarray:
    return pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)


def _median(values: pd.Series | np.ndarray) -> float | None:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    return float(np.median(array)) if len(array) else None


def _normalized_entropy(values: pd.Series) -> float:
    counts = values.fillna("<missing>").astype(str).value_counts().to_numpy(dtype=float)
    if len(counts) <= 1:
        return 0.0
    shares = counts / counts.sum()
    return float(-(shares * np.log(shares)).sum() / math.log(len(counts)))


def _primitive_counts(expressions: pd.Series) -> Counter[str]:
    counts: Counter[str] = Counter()
    for expression in expressions.fillna("").astype(str):
        counts.update(_PRIMITIVE.findall(expression))
    return counts


def _strict_lane_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {
            "strict_candidate_count": 0,
            "strict_cluster_count": 0,
            "median_abs_ic_h5": None,
            "median_signed_ic_h5": None,
            "median_turnover": None,
            "p90_turnover": None,
            "median_worst_horizon_abs_ic": None,
            "same_sign_all_horizons_share": None,
            "ic_confidence_bound_available": False,
        }
    rows = rows.copy()
    rows["horizon_bars"] = pd.to_numeric(rows["horizon_bars"], errors="coerce")
    rows["ic_mean"] = pd.to_numeric(rows["ic_mean"], errors="coerce")
    rows["ic_abs_mean"] = pd.to_numeric(rows["ic_abs_mean"], errors="coerce")
    rows["ic_count"] = pd.to_numeric(rows["ic_count"], errors="coerce")
    rows["mean_one_way_turnover"] = pd.to_numeric(
        rows["mean_one_way_turnover"], errors="coerce"
    )
    horizon = rows[rows["horizon_bars"].eq(5)]
    if horizon.empty:
        horizon = rows
    candidate_rows = []
    for candidate_id, group in rows.groupby("candidate_id", sort=True):
        signs = np.sign(group["ic_mean"].dropna().to_numpy(dtype=float))
        nonzero = signs[signs != 0]
        candidate_rows.append(
            {
                "candidate_id": candidate_id,
                "worst_abs_ic": float(group["ic_abs_mean"].min()),
                "same_sign": bool(len(nonzero) and np.all(nonzero == nonzero[0])),
            }
        )
    candidate_frame = pd.DataFrame(candidate_rows)
    turnover = horizon.drop_duplicates("candidate_id")["mean_one_way_turnover"].dropna()
    return {
        "strict_candidate_count": int(rows["candidate_id"].nunique()),
        "strict_cluster_count": int(rows["signal_cluster_id"].nunique()),
        "median_abs_ic_h5": _median(horizon["ic_abs_mean"]),
        "median_signed_ic_h5": _median(horizon["ic_mean"]),
        "median_turnover": _median(turnover),
        "p90_turnover": (
            float(np.quantile(turnover.to_numpy(dtype=float), 0.9)) if len(turnover) else None
        ),
        "median_worst_horizon_abs_ic": _median(candidate_frame["worst_abs_ic"]),
        "same_sign_all_horizons_share": float(candidate_frame["same_sign"].mean()),
        "ic_confidence_bound_available": False,
    }


def analyze_generator_funnel(
    proposals: pd.DataFrame,
    strict_metrics: pd.DataFrame,
    lane_funnel: pd.DataFrame,
    benchmark_increment: Mapping[str, Any],
    admissions: Mapping[str, pd.DataFrame] | None = None,
    adaptive_vs_control: list[Mapping[str, Any]] | None = None,
    bottleneck_diagnosis: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    required = {
        "candidate_id", "lane_id", "exact_identity", "canonical_identity",
        "semantic_bucket", "family_id", "motif", "expression", "legal",
        "materialized", "survivor", "signal_cluster_id", "proxy_reward",
    }
    missing = sorted(required - set(proposals.columns))
    if missing:
        raise ValueError(f"proposal table missing columns: {missing}")
    proposals = proposals.copy()
    proposals["legal_bool"] = _as_bool(proposals["legal"])
    proposals["materialized_bool"] = _as_bool(proposals["materialized"])
    proposals["survivor_bool"] = _as_bool(proposals["survivor"])
    benchmark_median = float(benchmark_increment["benchmark_median_reward"])
    admissions = admissions or {}
    adaptive_vs_control = adaptive_vs_control or []
    funnel_by_lane = {
        str(row["lane_id"]): row for row in lane_funnel.to_dict(orient="records")
    }
    materialized_cluster_sets = {
        str(lane): set(group.loc[group["materialized_bool"], "signal_cluster_id"].dropna().astype(str))
        for lane, group in proposals.groupby("lane_id", sort=True)
    }
    survivor_cluster_sets = {
        str(lane): set(group.loc[group["survivor_bool"], "signal_cluster_id"].dropna().astype(str))
        for lane, group in proposals.groupby("lane_id", sort=True)
    }
    all_lanes = sorted(materialized_cluster_sets)
    overlap_rows = []
    for left_index, left in enumerate(all_lanes):
        for right in all_lanes[left_index + 1 :]:
            materialized_intersection = materialized_cluster_sets[left] & materialized_cluster_sets[right]
            materialized_union = materialized_cluster_sets[left] | materialized_cluster_sets[right]
            survivor_intersection = survivor_cluster_sets[left] & survivor_cluster_sets[right]
            survivor_union = survivor_cluster_sets[left] | survivor_cluster_sets[right]
            overlap_rows.append(
                {
                    "left_lane": left,
                    "right_lane": right,
                    "materialized_shared_cluster_count": len(materialized_intersection),
                    "materialized_cluster_jaccard": (
                        float(len(materialized_intersection) / len(materialized_union))
                        if materialized_union else 0.0
                    ),
                    "survivor_shared_cluster_count": len(survivor_intersection),
                    "survivor_cluster_jaccard": (
                        float(len(survivor_intersection) / len(survivor_union))
                        if survivor_union else 0.0
                    ),
                }
            )

    lane_rows = []
    for lane, group in proposals.groupby("lane_id", sort=True):
        lane = str(lane)
        strict = strict_metrics[strict_metrics["lane_id"].astype(str).eq(lane)]
        legal = group[group["legal_bool"]]
        materialized = group[group["materialized_bool"]]
        survivors = group[group["survivor_bool"]]
        proposal_exact_count = int(group["exact_identity"].nunique())
        proposal_canonical_count = int(group["canonical_identity"].nunique())
        materialized_clusters = materialized_cluster_sets[lane]
        survivor_clusters = survivor_cluster_sets[lane]
        other_materialized_clusters: set[str] = set()
        other_survivor_clusters: set[str] = set()
        for other in all_lanes:
            if other != lane:
                other_materialized_clusters.update(materialized_cluster_sets[other])
                other_survivor_clusters.update(survivor_cluster_sets[other])
        unique_materialized_clusters = materialized_clusters - other_materialized_clusters
        unique_survivor_clusters = survivor_clusters - other_survivor_clusters
        primitive_counts = _primitive_counts(group["expression"])
        proxy = pd.to_numeric(survivors["proxy_reward"], errors="coerce")
        strict_summary = _strict_lane_metrics(strict)
        funnel = funnel_by_lane.get(lane, {})
        recorded_survivor_cluster_count = int(funnel.get("signal_cluster_count", 0))
        recorded_legal_exact_count = int(funnel.get("exact_count", 0))
        admission_counts = {
            name: int(frame["lane_id"].astype(str).eq(lane).sum())
            for name, frame in admissions.items()
        }
        lane_rows.append(
            {
                "lane_id": lane,
                "proposal_count": int(len(group)),
                "legal_count": int(group["legal_bool"].sum()),
                "materialized_count": int(group["materialized_bool"].sum()),
                "proposal_exact_identity_count": proposal_exact_count,
                "legal_exact_identity_count": int(legal["exact_identity"].nunique()),
                "recorded_legal_exact_identity_count": recorded_legal_exact_count,
                "legal_exact_identity_count_matches_funnel": (
                    int(legal["exact_identity"].nunique()) == recorded_legal_exact_count
                ),
                "materialized_exact_identity_count": int(materialized["exact_identity"].nunique()),
                "survivor_exact_identity_count": int(survivors["exact_identity"].nunique()),
                "proposal_canonical_identity_count": proposal_canonical_count,
                "legal_canonical_identity_count": int(legal["canonical_identity"].nunique()),
                "materialized_canonical_identity_count": int(materialized["canonical_identity"].nunique()),
                "survivor_canonical_identity_count": int(survivors["canonical_identity"].nunique()),
                "proposal_exact_duplicate_rate": float(1.0 - proposal_exact_count / len(group)),
                "proposal_canonical_duplicate_rate": float(1.0 - proposal_canonical_count / len(group)),
                "semantic_bucket_count": int(group["semantic_bucket"].nunique()),
                "semantic_bucket_entropy": _normalized_entropy(group["semantic_bucket"]),
                "family_count": int(group["family_id"].nunique()),
                "motif_count": int(group["motif"].nunique()),
                "top_primitives": [
                    {"primitive": name, "uses": count}
                    for name, count in primitive_counts.most_common(8)
                ],
                "materialized_signal_cluster_count": len(materialized_clusters),
                "survivor_signal_cluster_count": len(survivor_clusters),
                "recorded_survivor_cluster_count": recorded_survivor_cluster_count,
                "survivor_cluster_count_matches_funnel": (
                    len(survivor_clusters) == recorded_survivor_cluster_count
                ),
                "materialized_unique_vs_other_lane_cluster_count": len(unique_materialized_clusters),
                "survivor_unique_vs_other_lane_cluster_count": len(unique_survivor_clusters),
                "materialized_clusters_per_100_proposals": float(
                    100.0 * len(materialized_clusters) / len(group)
                ),
                "survivor_clusters_per_100_proposals": float(
                    100.0 * len(survivor_clusters) / len(group)
                ),
                "survivor_count": int(len(survivors)),
                "survivor_conversion": float(len(survivors) / len(group)),
                "n_eff": float(funnel.get("n_eff", 0.0)),
                "top1_cluster_share": float(funnel.get("top1_cluster_share", 0.0)),
                "median_survivor_proxy_reward": _median(proxy),
                "proxy_increment_vs_benchmark_median": (
                    None if _median(proxy) is None else float(_median(proxy) - benchmark_median)
                ),
                "new_clusters_per_strict": (
                    float(len(unique_survivor_clusters) / strict_summary["strict_candidate_count"])
                    if strict_summary["strict_candidate_count"]
                    else None
                ),
                "cost_adjusted_quality_available": False,
                "worst_time_block_available": False,
                "runtime_attribution_available": False,
                "admission_counts": admission_counts,
                **strict_summary,
            }
        )

    defects = []
    for row in lane_rows:
        if row["proposal_exact_duplicate_rate"] >= 0.15:
            defects.append({"lane_id": row["lane_id"], "defect": "HIGH_EXACT_DUPLICATION"})
        if row["survivor_clusters_per_100_proposals"] < 15.0:
            defects.append({"lane_id": row["lane_id"], "defect": "LOW_CLUSTER_YIELD"})
        if row["top1_cluster_share"] >= 0.20:
            defects.append({"lane_id": row["lane_id"], "defect": "HIGH_CLUSTER_CONCENTRATION"})
        increment = row["proxy_increment_vs_benchmark_median"]
        if increment is not None and increment <= 0:
            defects.append({"lane_id": row["lane_id"], "defect": "NON_POSITIVE_BENCHMARK_INCREMENT"})
    admission_summaries = {}
    for name, frame in admissions.items():
        reward = pd.to_numeric(frame["proxy_reward"], errors="coerce")
        admission_summaries[name] = {
            "count": int(len(frame)),
            "lane_count": int(frame["lane_id"].nunique()),
            "exact_identity_count": int(frame["exact_identity"].nunique()),
            "signal_cluster_count": int(frame["signal_cluster_id"].nunique()),
            "median_proxy_reward": _median(reward),
            "per_lane_count": {
                str(lane): int(count)
                for lane, count in frame.groupby("lane_id", sort=True).size().items()
            },
        }
    adaptive_summary = {
        "lane_count": len(adaptive_vs_control),
        "winner_count": sum(
            bool(row.get("adaptive_outperformed_matched_control"))
            for row in adaptive_vs_control
        ),
        "winners": sorted(
            str(row.get("lane_id"))
            for row in adaptive_vs_control
            if bool(row.get("adaptive_outperformed_matched_control"))
        ),
        "per_lane": list(adaptive_vs_control),
    }
    return {
        "diagnostic_version": DIAGNOSTIC_VERSION,
        "funnel_totals": {
            "proposal_count": int(len(proposals)),
            "legal_count": int(proposals["legal_bool"].sum()),
            "materialized_count": int(proposals["materialized_bool"].sum()),
            "proposal_exact_identity_count": int(proposals["exact_identity"].nunique()),
            "legal_exact_identity_count": int(
                proposals.loc[proposals["legal_bool"], "exact_identity"].nunique()
            ),
            "materialized_exact_identity_count": int(
                proposals.loc[proposals["materialized_bool"], "exact_identity"].nunique()
            ),
            "survivor_exact_identity_count": int(
                proposals.loc[proposals["survivor_bool"], "exact_identity"].nunique()
            ),
            "proposal_canonical_identity_count": int(proposals["canonical_identity"].nunique()),
            "legal_canonical_identity_count": int(
                proposals.loc[proposals["legal_bool"], "canonical_identity"].nunique()
            ),
            "materialized_canonical_identity_count": int(
                proposals.loc[proposals["materialized_bool"], "canonical_identity"].nunique()
            ),
            "survivor_canonical_identity_count": int(
                proposals.loc[proposals["survivor_bool"], "canonical_identity"].nunique()
            ),
            "materialized_signal_cluster_count": int(
                proposals.loc[proposals["materialized_bool"], "signal_cluster_id"].dropna().nunique()
            ),
            "survivor_signal_cluster_count": int(
                proposals.loc[proposals["survivor_bool"], "signal_cluster_id"].dropna().nunique()
            ),
            "survivor_count": int(proposals["survivor_bool"].sum()),
            "strict_candidate_count": int(strict_metrics["candidate_id"].nunique()),
        },
        "benchmark_median_reward": benchmark_median,
        "per_lane": lane_rows,
        "cross_lane_overlap": overlap_rows,
        "admission_strategies": admission_summaries,
        "adaptive_vs_matched_control": adaptive_summary,
        "recorded_bottleneck_diagnosis": dict(bottleneck_diagnosis or {}),
        "priority_defects": defects,
        "missing_evidence": [
            "cost-adjusted strict quality",
            "IC uncertainty or standard error for confidence bounds",
            "worst time-block quality",
            "per-lane runtime attribution",
            "portfolio mapping and net benchmark increment",
        ],
    }
