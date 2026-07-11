"""Coverage-only metrics with an explicit ban on performance/evaluation inputs."""

from __future__ import annotations

import math
from collections import Counter
from typing import Any, Iterable, Mapping


FORBIDDEN_COVERAGE_TOKENS = (
    "reward", "validation", "holdout", "forward", "oos", "label", "winner",
    "sortino", "sharpe", "return", "performance",
)


def _entropy(values: Iterable[str]) -> float:
    counts = Counter(values)
    total = sum(counts.values())
    if not total:
        return 0.0
    return -sum((count / total) * math.log(count / total) for count in counts.values())


def _coverage(rows: list[Mapping[str, Any]], key: str) -> dict[str, Any]:
    values: list[str] = []
    for row in rows:
        value = row.get(key, "unknown")
        if isinstance(value, (list, tuple, set)):
            values.extend(str(item) for item in value)
        else:
            values.append(str(value or "unknown"))
    counts = Counter(values)
    return {"distinct": len(counts), "counts": dict(sorted(counts.items()))}


def compute_coverage_metrics(candidates: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(row) for row in candidates]
    forbidden = sorted(
        {
            key
            for row in rows
            for key in row
            if any(token in str(key).lower() for token in FORBIDDEN_COVERAGE_TOKENS)
        }
    )
    if forbidden:
        raise ValueError(f"performance fields are forbidden in coverage metrics: {forbidden}")
    exact = [str(row.get("exact_identity") or row.get("candidate_id") or "") for row in rows]
    semantic = [str(row.get("semantic_key") or "unknown") for row in rows]
    grammar = [str(row.get("grammar_cell") or "unknown") for row in rows]
    lineage = [str(row.get("lineage_key") or "unknown") for row in rows]
    semantic_counts = Counter(semantic)
    mean_volume = sum(semantic_counts.values()) / max(1, len(semantic_counts))
    return {
        "candidate_count": len(rows),
        "field_family_coverage": _coverage(rows, "field_family"),
        "primitive_family_coverage": _coverage(rows, "primitive_family"),
        "temporal_cell_coverage": _coverage(rows, "temporal_cell"),
        "event_state_coverage": _coverage(rows, "event_state_family"),
        "plate_industry_coverage": _coverage(rows, "plate_industry_family"),
        "economic_hypothesis_coverage": _coverage(rows, "economic_hypothesis"),
        "grammar_cell_entropy": _entropy(grammar),
        "lineage_entropy": _entropy(lineage),
        "semantic_volume_imbalance": max(semantic_counts.values(), default=0) / max(1.0, mean_volume),
        "signal_cluster_potential": {
            "semantic_keys": len(set(semantic)),
            "exact_identities": len(set(exact)),
            "ratio": len(set(semantic)) / max(1, len(set(exact))),
            "status": "structural_potential_only_no_signal_materialization",
        },
        "per_lane_proposal_distribution": dict(sorted(Counter(str(row.get("lane_id") or "unknown") for row in rows).items())),
        "performance_used": False,
    }
