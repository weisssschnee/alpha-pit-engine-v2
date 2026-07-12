"""Hard-gated Pareto admission for development-only Sprint-1 research."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

import numpy as np


PARETO_VERSION = "cn_development_pareto_v1"
MAXIMIZE = (
    "signal_quality", "cost_adjusted_quality", "worst_time_block_quality",
    "stability", "benchmark_increment", "behaviour_novelty",
)
MINIMIZE = ("turnover", "concentration", "complexity")


@dataclass(frozen=True, slots=True)
class GateDecision:
    allowed: bool
    reasons: tuple[str, ...]


def objective_vector(
    row: Mapping[str, Any],
    *,
    benchmark_median: float,
    cluster_size: int,
) -> dict[str, float]:
    quality = float(row.get("proxy_ic_abs_lcb95") or 0.0)
    if quality <= 0.0 and row.get("proxy_ic_abs_lcb95") is None:
        quality = abs(float(row.get("proxy_ic_mean") or 0.0))
    turnover = float(row.get("proxy_turnover") or 0.0)
    cost_rate = float(row.get("objective_cost_rate") or 0.0025)
    worst = float(row.get("proxy_worst_time_block_abs_ic") or 0.0)
    stability = float(row.get("proxy_time_block_stability") or 0.0)
    concentration = float(row.get("proxy_signal_concentration") or 1.0)
    reward = float(row.get("proxy_reward") or 0.0)
    return {
        "signal_quality": quality,
        "cost_adjusted_quality": quality - cost_rate * turnover,
        "turnover": turnover,
        "worst_time_block_quality": worst,
        "stability": stability,
        "concentration": concentration,
        "benchmark_increment": reward - float(benchmark_median),
        "complexity": float(row.get("complexity") or str(row.get("expression", "")).count("(")),
        "behaviour_novelty": 1.0 / max(1, int(cluster_size)),
    }


def hard_gate(
    row: Mapping[str, Any],
    vector: Mapping[str, float],
    *,
    require_nonnegative_benchmark_increment: bool,
    minimum_worst_block_quality: float,
    maximum_turnover: float,
) -> GateDecision:
    reasons = []
    if not bool(row.get("legal")):
        reasons.append("ILLEGAL")
    if not bool(row.get("survivor")):
        reasons.append("NOT_DEVELOPMENT_SURVIVOR")
    if float(vector["worst_time_block_quality"]) < float(minimum_worst_block_quality):
        reasons.append("WORST_BLOCK_BELOW_FLOOR")
    if float(vector["turnover"]) > float(maximum_turnover):
        reasons.append("TURNOVER_ABOVE_CAP")
    if (
        require_nonnegative_benchmark_increment
        and str(row.get("lane_id")) != "benchmark_competitor"
        and float(vector["benchmark_increment"]) < 0.0
    ):
        reasons.append("NEGATIVE_BENCHMARK_INCREMENT")
    return GateDecision(not reasons, tuple(reasons))


def dominates(left: Mapping[str, float], right: Mapping[str, float]) -> bool:
    no_worse = all(float(left[key]) >= float(right[key]) for key in MAXIMIZE)
    no_worse = no_worse and all(float(left[key]) <= float(right[key]) for key in MINIMIZE)
    strictly_better = any(float(left[key]) > float(right[key]) for key in MAXIMIZE)
    strictly_better = strictly_better or any(float(left[key]) < float(right[key]) for key in MINIMIZE)
    return no_worse and strictly_better


def pareto_fronts(rows: Sequence[Mapping[str, Any]]) -> list[list[int]]:
    remaining = list(range(len(rows)))
    fronts: list[list[int]] = []
    while remaining:
        front = [
            index for index in remaining
            if not any(
                other != index and dominates(rows[other]["objective_vector"], rows[index]["objective_vector"])
                for other in remaining
            )
        ]
        front.sort(key=lambda index: str(rows[index].get("candidate_id", "")))
        fronts.append(front)
        members = set(front)
        remaining = [index for index in remaining if index not in members]
    return fronts


def _limited_scalar(vector: Mapping[str, float]) -> float:
    """Bounded tie-break only; Pareto rank always has priority."""
    return float(
        0.30 * np.tanh(5.0 * vector["signal_quality"])
        + 0.20 * np.tanh(5.0 * vector["worst_time_block_quality"])
        + 0.15 * np.tanh(5.0 * vector["cost_adjusted_quality"])
        + 0.10 * np.tanh(5.0 * vector["benchmark_increment"])
        + 0.10 * vector["stability"]
        + 0.05 * vector["behaviour_novelty"]
        - 0.05 * min(1.0, vector["turnover"])
        - 0.03 * min(1.0, vector["concentration"])
        - 0.02 * min(1.0, vector["complexity"] / 20.0)
    )


def prepare_objectives(
    rows: Iterable[Mapping[str, Any]],
    *,
    benchmark_median: float,
    require_nonnegative_benchmark_increment: bool = True,
    minimum_worst_block_quality: float = 0.0,
    maximum_turnover: float = 1.0,
) -> list[dict[str, Any]]:
    source = [dict(row) for row in rows]
    cluster_sizes = Counter(int(row.get("signal_cluster_id") or 0) for row in source)
    output = []
    for row in source:
        cluster = int(row.get("signal_cluster_id") or 0)
        vector = objective_vector(
            row,
            benchmark_median=benchmark_median,
            cluster_size=cluster_sizes[cluster],
        )
        gate = hard_gate(
            row,
            vector,
            require_nonnegative_benchmark_increment=require_nonnegative_benchmark_increment,
            minimum_worst_block_quality=minimum_worst_block_quality,
            maximum_turnover=maximum_turnover,
        )
        row["objective_vector"] = vector
        row["objective_gate_allowed"] = gate.allowed
        row["objective_gate_reasons"] = list(gate.reasons)
        row["limited_scalar_tiebreak"] = _limited_scalar(vector)
        output.append(row)
    legal = [row for row in output if row["objective_gate_allowed"]]
    for rank, front in enumerate(pareto_fronts(legal), start=1):
        for index in front:
            legal[index]["pareto_rank"] = rank
    return output


def select_pareto(
    rows: Iterable[Mapping[str, Any]],
    *,
    cap: int,
    lane_floor: int,
    family_cap: int,
    parent_cap: int,
) -> list[dict[str, Any]]:
    prepared = [dict(row) for row in rows if bool(row.get("objective_gate_allowed"))]
    ordered = sorted(
        prepared,
        key=lambda row: (
            int(row.get("pareto_rank") or 10**9),
            -float(row.get("limited_scalar_tiebreak") or 0.0),
            str(row.get("candidate_id", "")),
        ),
    )
    selected: list[dict[str, Any]] = []
    identities: set[str] = set()
    clusters: set[int] = set()
    families: Counter[str] = Counter()
    parents: Counter[str] = Counter()

    def take(row: dict[str, Any]) -> bool:
        identity = str(row["exact_identity"])
        cluster = int(row["signal_cluster_id"])
        family = str(row["family_id"])
        parent = str(row.get("parent_id") or "")
        if identity in identities or cluster in clusters:
            return False
        if families[family] >= family_cap or (parent and parents[parent] >= parent_cap):
            return False
        selected.append(row)
        identities.add(identity)
        clusters.add(cluster)
        families[family] += 1
        if parent:
            parents[parent] += 1
        return True

    for lane in sorted({str(row["lane_id"]) for row in ordered}):
        count = 0
        for row in ordered:
            if str(row["lane_id"]) == lane and take(row):
                count += 1
                if count >= lane_floor or len(selected) >= cap:
                    break
    for row in ordered:
        if len(selected) >= cap:
            break
        take(row)
    return selected
