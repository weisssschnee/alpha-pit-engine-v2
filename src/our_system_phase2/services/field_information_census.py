"""Non-performance information census and representative field selection.

The census measures data availability, variation and field-to-field redundancy.
It never accepts a return, label, reward or performance column.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import log, sqrt
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class InformationCensusPolicy:
    bins: int = 16
    min_coverage: float = 0.60
    min_sample_unique: int = 32
    min_temporal_change_rate: float = 0.01
    redundancy_nmi: float = 0.95


def quantile_codes(values: pd.Series, *, bins: int) -> np.ndarray:
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(dtype="float64")
    output = np.full(len(numeric), -1, dtype="int16")
    finite = np.isfinite(numeric)
    if finite.sum() < 2:
        return output
    unique = np.unique(numeric[finite])
    if len(unique) < 2:
        output[finite] = 0
        return output
    probabilities = np.linspace(0.0, 1.0, min(bins, len(unique)) + 1)
    edges = np.unique(np.quantile(numeric[finite], probabilities))
    if len(edges) < 2:
        output[finite] = 0
        return output
    output[finite] = np.searchsorted(edges[1:-1], numeric[finite], side="right")
    return output


def normalized_entropy(codes: np.ndarray) -> float:
    valid = codes[codes >= 0]
    if len(valid) == 0:
        return 0.0
    counts = np.bincount(valid)
    counts = counts[counts > 0]
    if len(counts) <= 1:
        return 0.0
    probabilities = counts / counts.sum()
    entropy = float(-(probabilities * np.log(probabilities)).sum())
    return entropy / log(len(counts))


def normalized_mutual_information(left: np.ndarray, right: np.ndarray) -> tuple[float, int]:
    mask = (left >= 0) & (right >= 0)
    support = int(mask.sum())
    if support == 0:
        return 0.0, 0
    x = left[mask].astype("int64", copy=False)
    y = right[mask].astype("int64", copy=False)
    joint = np.zeros((int(x.max()) + 1, int(y.max()) + 1), dtype="int64")
    np.add.at(joint, (x, y), 1)
    probability = joint / support
    px = probability.sum(axis=1)
    py = probability.sum(axis=0)
    expected = px[:, None] * py[None, :]
    positive = probability > 0
    mi = float((probability[positive] * np.log(probability[positive] / expected[positive])).sum())
    hx = float(-(px[px > 0] * np.log(px[px > 0])).sum())
    hy = float(-(py[py > 0] * np.log(py[py > 0])).sum())
    denominator = sqrt(hx * hy)
    return (mi / denominator if denominator > 0 else 0.0), support


def field_metrics(
    sample: pd.DataFrame,
    *,
    fields: Iterable[str],
    full_counts: Mapping[str, Mapping[str, Any]],
    policy: InformationCensusPolicy,
) -> tuple[list[dict[str, Any]], dict[str, np.ndarray]]:
    rows: list[dict[str, Any]] = []
    codes: dict[str, np.ndarray] = {}
    for field in fields:
        field_codes = quantile_codes(sample[field], bins=policy.bins)
        codes[field] = field_codes
        source = full_counts[field]
        rows.append(
            {
                "field_id": field,
                "row_count": int(source["row_count"]),
                "finite_count": int(source["finite_count"]),
                "coverage": float(source["finite_count"]) / max(1, int(source["row_count"])),
                "sample_count": int((field_codes >= 0).sum()),
                "sample_unique": int(pd.to_numeric(sample[field], errors="coerce").nunique(dropna=True)),
                "normalized_entropy": normalized_entropy(field_codes),
                "temporal_comparison_count": int(source["temporal_comparison_count"]),
                "temporal_change_rate": float(source["temporal_change_count"])
                / max(1, int(source["temporal_comparison_count"])),
                "cross_sectional_std_mean": float(source["cross_sectional_std_mean"]),
                "minimum": source["minimum"],
                "maximum": source["maximum"],
            }
        )
    return rows, codes


def pairwise_nmi(
    codes: Mapping[str, np.ndarray],
    *,
    sample_buckets: np.ndarray | None = None,
) -> list[dict[str, Any]]:
    fields = sorted(codes)
    rows: list[dict[str, Any]] = []
    for left_index, left in enumerate(fields):
        for right in fields[left_index + 1 :]:
            combined, support = normalized_mutual_information(codes[left], codes[right])
            row: dict[str, Any] = {
                "left_field_id": left,
                "right_field_id": right,
                "normalized_mutual_information": combined,
                "joint_support": support,
            }
            if sample_buckets is not None:
                a, support_a = normalized_mutual_information(
                    np.where(sample_buckets == 0, codes[left], -1),
                    np.where(sample_buckets == 0, codes[right], -1),
                )
                b, support_b = normalized_mutual_information(
                    np.where(sample_buckets == 1, codes[left], -1),
                    np.where(sample_buckets == 1, codes[right], -1),
                )
                row.update(
                    nmi_a=a,
                    nmi_b=b,
                    nmi_abs_difference=abs(a - b),
                    support_a=support_a,
                    support_b=support_b,
                )
            rows.append(row)
    return rows


def select_core_pack(
    metrics: Iterable[Mapping[str, Any]],
    pair_rows: Iterable[Mapping[str, Any]],
    *,
    roles: Mapping[str, str],
    policy: InformationCensusPolicy,
) -> dict[str, Any]:
    metric_rows = {str(row["field_id"]): dict(row) for row in metrics}
    redundancy: dict[frozenset[str], float] = {
        frozenset((str(row["left_field_id"]), str(row["right_field_id"]))): float(
            row["normalized_mutual_information"]
        )
        for row in pair_rows
    }
    candidates = [
        row
        for row in metric_rows.values()
        if roles.get(str(row["field_id"])) == "interaction-only"
        and float(row["coverage"]) >= policy.min_coverage
        and int(row["sample_unique"]) >= policy.min_sample_unique
        and float(row["temporal_change_rate"]) >= policy.min_temporal_change_rate
    ]
    candidates.sort(
        key=lambda row: (
            -float(row["coverage"]),
            -float(row["normalized_entropy"]),
            str(row["field_id"]),
        )
    )
    selected: list[str] = []
    decisions: list[dict[str, Any]] = []
    for row in candidates:
        field = str(row["field_id"])
        strongest = max(
            ((redundancy.get(frozenset((field, prior)), 0.0), prior) for prior in selected),
            default=(0.0, ""),
        )
        if strongest[0] >= policy.redundancy_nmi:
            decisions.append(
                {
                    "field_id": field,
                    "decision": "REDUNDANCY_ARCHIVE",
                    "reason": "NMI_AT_OR_ABOVE_THRESHOLD",
                    "representative_field_id": strongest[1],
                    "representative_nmi": strongest[0],
                }
            )
        else:
            selected.append(field)
            decisions.append(
                {
                    "field_id": field,
                    "decision": "EXPLORATORY_CORE",
                    "reason": "NON_PERFORMANCE_QUALITY_AND_DISTINCTNESS_GATES_PASS",
                    "representative_field_id": "",
                    "representative_nmi": strongest[0],
                }
            )
    for field, role in sorted(roles.items()):
        if role != "interaction-only":
            decisions.append(
                {
                    "field_id": field,
                    "decision": "CONTROL_ONLY",
                    "reason": f"ROLE_{role.upper().replace('-', '_')}",
                    "representative_field_id": "",
                    "representative_nmi": 0.0,
                }
            )
    return {
        "status": "EXPLORATORY_NON_PERFORMANCE_CORE_PACK",
        "selected_field_ids": selected,
        "decisions": sorted(decisions, key=lambda row: str(row["field_id"])),
        "performance_claim_allowed": False,
    }

