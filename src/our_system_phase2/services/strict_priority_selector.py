"""Auditable pre-strict selector trained only from frozen development evidence."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import hashlib
import math
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


SELECTOR_VERSION = "cn_sprint2_strict_priority_selector_v1"
NUMERIC_FEATURES = (
    "proxy_ic_abs_lcb95",
    "proxy_reward",
    "proxy_worst_time_block_abs_ic",
    "proxy_time_block_stability",
    "proxy_turnover",
    "proxy_signal_concentration",
    "proxy_signal_unique_log1p",
    "proxy_finite_ratio",
    "control_increment_vs_benchmark",
    "information_gain",
    "complexity",
)
CATEGORICAL_FEATURES = (
    "lane_id", "primitive_family", "hypothesis_arm", "proposal_stage", "field_family",
)
HASH_BUCKETS = 64


def _float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def development_eligibility(
    row: Mapping[str, Any], contract: Mapping[str, Any]
) -> tuple[bool, tuple[str, ...]]:
    """Return evaluability eligibility; this is not a quality-survivor claim."""
    reasons: list[str] = []
    if not _bool(row.get("legal")):
        reasons.append("ILLEGAL")
    if not _bool(row.get("materialized")):
        reasons.append("NOT_MATERIALIZED")
    if _float(row.get("proxy_finite_ratio")) < _float(contract["minimum_finite_ratio"]):
        reasons.append("FINITE_RATIO")
    if int(_float(row.get("proxy_signal_unique"))) < int(contract["minimum_signal_unique"]):
        reasons.append("SIGNAL_UNIQUE")
    if int(_float(row.get("proxy_ic_count"))) < int(contract["minimum_eligible_cross_sections"]):
        reasons.append("CROSS_SECTION_COUNT")
    if _float(row.get("proxy_reward")) < _float(contract["minimum_proxy_reward"]):
        reasons.append("PROXY_REWARD_FLOOR")
    return not reasons, tuple(reasons)


def apply_development_eligibility(
    rows: Iterable[Mapping[str, Any]], contract: Mapping[str, Any]
) -> list[dict[str, Any]]:
    output = []
    for source in rows:
        row = dict(source)
        allowed, reasons = development_eligibility(row, contract)
        row["legacy_survivor"] = bool(row.get("survivor"))
        row["development_eligible"] = allowed
        row["development_eligibility_reasons"] = list(reasons)
        output.append(row)
    return output


def _hash_bucket(field: str, value: Any) -> int:
    digest = hashlib.sha256(f"{field}={value}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % HASH_BUCKETS


def feature_matrix(
    rows: Sequence[Mapping[str, Any]],
    *,
    means: Sequence[float] | None = None,
    scales: Sequence[float] | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    numeric = np.asarray(
        [
            [
                math.log1p(max(0.0, _float(row.get("proxy_signal_unique"))))
                if name == "proxy_signal_unique_log1p"
                else _float(row.get(name))
                for name in NUMERIC_FEATURES
            ]
            for row in rows
        ],
        dtype=float,
    )
    center = np.asarray(means, dtype=float) if means is not None else np.nanmean(numeric, axis=0)
    scale = np.asarray(scales, dtype=float) if scales is not None else np.nanstd(numeric, axis=0)
    center = np.where(np.isfinite(center), center, 0.0)
    scale = np.where(np.isfinite(scale) & (scale > 1e-9), scale, 1.0)
    standardized = (np.nan_to_num(numeric, nan=0.0) - center) / scale
    categorical = np.zeros((len(rows), HASH_BUCKETS), dtype=float)
    for index, row in enumerate(rows):
        for field in CATEGORICAL_FEATURES:
            categorical[index, _hash_bucket(field, row.get(field, ""))] += 1.0
    matrix = np.column_stack([np.ones(len(rows)), standardized, categorical])
    return matrix, center, scale


@dataclass(frozen=True, slots=True)
class StrictPriorityModel:
    weights: tuple[float, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]

    def score(self, rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
        matrix, _, _ = feature_matrix(rows, means=self.means, scales=self.scales)
        logits = np.clip(matrix @ np.asarray(self.weights, dtype=float), -30.0, 30.0)
        return 1.0 / (1.0 + np.exp(-logits))

    def artifact(self) -> dict[str, Any]:
        return {
            "selector_version": SELECTOR_VERSION,
            "numeric_features": list(NUMERIC_FEATURES),
            "categorical_features": list(CATEGORICAL_FEATURES),
            "hash_buckets": HASH_BUCKETS,
            "weights": list(self.weights),
            "means": list(self.means),
            "scales": list(self.scales),
        }

    @classmethod
    def from_artifact(cls, payload: Mapping[str, Any]) -> "StrictPriorityModel":
        if payload.get("selector_version") != SELECTOR_VERSION:
            raise ValueError("strict-priority selector version mismatch")
        return cls(
            tuple(float(value) for value in payload["weights"]),
            tuple(float(value) for value in payload["means"]),
            tuple(float(value) for value in payload["scales"]),
        )


def fit_model(
    rows: Sequence[Mapping[str, Any]], labels: Sequence[int], *, iterations: int = 600
) -> StrictPriorityModel:
    matrix, center, scale = feature_matrix(rows)
    target = np.asarray(labels, dtype=float)
    if len(np.unique(target)) < 2:
        prior = float(np.clip(target.mean() if len(target) else 0.5, 1e-4, 1 - 1e-4))
        weights = np.zeros(matrix.shape[1], dtype=float)
        weights[0] = math.log(prior / (1.0 - prior))
        return StrictPriorityModel(tuple(weights), tuple(center), tuple(scale))
    positives = max(1.0, float(target.sum()))
    negatives = max(1.0, float(len(target) - target.sum()))
    sample_weight = np.where(target > 0.5, len(target) / (2.0 * positives), len(target) / (2.0 * negatives))
    weights = np.zeros(matrix.shape[1], dtype=float)
    learning_rate = 0.08
    regularization = 0.02
    for _ in range(iterations):
        logits = np.clip(matrix @ weights, -30.0, 30.0)
        probability = 1.0 / (1.0 + np.exp(-logits))
        gradient = matrix.T @ ((probability - target) * sample_weight) / len(target)
        penalty = regularization * weights
        penalty[0] = 0.0
        weights -= learning_rate * (gradient + penalty)
    return StrictPriorityModel(tuple(weights), tuple(center), tuple(scale))


class _UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, value: int) -> int:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, left: int, right: int) -> None:
        a, b = self.find(left), self.find(right)
        if a != b:
            self.parent[max(a, b)] = min(a, b)


def leakage_components(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    """Keep exact identities, near families and within-run clusters in one fold."""
    union = _UnionFind(len(rows))
    owners: dict[tuple[str, str], int] = {}
    for index, row in enumerate(rows):
        keys = [
            ("exact", str(row.get("exact_identity") or row.get("canonical_identity") or "")),
            ("family", str(row.get("family_id") or "")),
            (
                "cluster",
                f"{row.get('run_id', '')}:{row.get('signal_cluster_id', '')}",
            ),
        ]
        for key in keys:
            if not key[1]:
                continue
            if key in owners:
                union.union(index, owners[key])
            else:
                owners[key] = index
    return [f"component_{union.find(index):06d}" for index in range(len(rows))]


def balanced_group_folds(groups: Sequence[str], n_splits: int = 5) -> np.ndarray:
    members: dict[str, list[int]] = defaultdict(list)
    for index, group in enumerate(groups):
        members[str(group)].append(index)
    if len(members) < 2:
        raise ValueError("cross-fitting requires at least two leakage groups")
    fold_count = min(int(n_splits), len(members))
    sizes = [0] * fold_count
    assignment: dict[str, int] = {}
    for group, indices in sorted(members.items(), key=lambda item: (-len(item[1]), item[0])):
        fold = min(range(fold_count), key=lambda value: (sizes[value], value))
        assignment[group] = fold
        sizes[fold] += len(indices)
    return np.asarray([assignment[str(group)] for group in groups], dtype=int)


def cross_fitted_scores(
    rows: Sequence[Mapping[str, Any]], labels: Sequence[int], folds: Sequence[int]
) -> np.ndarray:
    fold_array = np.asarray(folds, dtype=int)
    output = np.full(len(rows), np.nan, dtype=float)
    for fold in sorted(set(fold_array.tolist())):
        train = np.flatnonzero(fold_array != fold)
        test = np.flatnonzero(fold_array == fold)
        model = fit_model([rows[index] for index in train], [labels[index] for index in train])
        output[test] = model.score([rows[index] for index in test])
    if not np.isfinite(output).all():
        raise RuntimeError("cross-fitted selector left missing predictions")
    return output


def percentile_rank_score(rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
    frame = pd.DataFrame(
        {
            "quality": [_float(row.get("proxy_ic_abs_lcb95")) for row in rows],
            "reward": [_float(row.get("proxy_reward")) for row in rows],
            "worst": [_float(row.get("proxy_worst_time_block_abs_ic")) for row in rows],
            "stability": [_float(row.get("proxy_time_block_stability")) for row in rows],
            "turnover": [-_float(row.get("proxy_turnover")) for row in rows],
            "concentration": [-_float(row.get("proxy_signal_concentration"), 1.0) for row in rows],
            "complexity": [-_float(row.get("complexity")) for row in rows],
        }
    )
    return frame.rank(pct=True, method="average").mean(axis=1).to_numpy(dtype=float)


def current_scalar_score(rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
    return np.asarray(
        [
            _float(row.get("limited_scalar_tiebreak"), _float(row.get("proxy_reward")))
            for row in rows
        ],
        dtype=float,
    )


def deterministic_random_score(rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
    return np.asarray(
        [
            int.from_bytes(
                hashlib.sha256(
                    str(row.get("exact_identity") or row.get("candidate_id")).encode("utf-8")
                ).digest()[:8],
                "big",
            )
            / float(2**64 - 1)
            for row in rows
        ],
        dtype=float,
    )


def precision_metrics(labels: Sequence[int], scores: Sequence[float]) -> dict[str, float]:
    target = np.asarray(labels, dtype=int)
    value = np.asarray(scores, dtype=float)
    base = float(target.mean()) if len(target) else 0.0
    result: dict[str, float] = {"count": int(len(target)), "base_rate": base}
    order = np.argsort(-value, kind="stable")
    for fraction, name in ((0.05, "top_5pct"), (0.10, "top_10pct")):
        count = max(1, int(math.ceil(len(target) * fraction)))
        precision = float(target[order[:count]].mean())
        result[f"{name}_count"] = count
        result[f"{name}_precision"] = precision
        result[f"{name}_lift"] = precision / base if base > 0 else 0.0
        result[f"{name}_effective_per_100_strict"] = 100.0 * precision
    return result


def calibration_rows(labels: Sequence[int], scores: Sequence[float], bins: int = 10) -> list[dict[str, float]]:
    target = np.asarray(labels, dtype=int)
    value = np.asarray(scores, dtype=float)
    order = np.argsort(value, kind="stable")
    output = []
    for indices in np.array_split(order, min(bins, len(order))):
        if not len(indices):
            continue
        output.append(
            {
                "count": int(len(indices)),
                "mean_score": float(value[indices].mean()),
                "hit_rate": float(target[indices].mean()),
            }
        )
    return output


def bias_rows(
    rows: Sequence[Mapping[str, Any]], labels: Sequence[int], scores: Sequence[float], field: str
) -> list[dict[str, Any]]:
    groups: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        groups[str(row.get(field) or "unknown")].append(index)
    target = np.asarray(labels, dtype=int)
    value = np.asarray(scores, dtype=float)
    return [
        {
            field: group,
            "count": len(indices),
            "mean_score": float(value[indices].mean()),
            "hit_rate": float(target[indices].mean()),
        }
        for group, indices in sorted(groups.items())
    ]


def selector_comparison(rows: Sequence[Mapping[str, Any]], labels: Sequence[int]) -> dict[str, Any]:
    if len(rows) != len(labels) or not rows:
        raise ValueError("selector comparison requires nonempty aligned evidence")
    components = leakage_components(rows)
    component_folds = balanced_group_folds(components, n_splits=5)
    seeds = [str(row.get("seed_set") or row.get("run_id") or "unknown") for row in rows]
    seed_folds = balanced_group_folds(seeds, n_splits=len(set(seeds)))
    scores = {
        "random": deterministic_random_score(rows),
        "current_scalar": current_scalar_score(rows),
        "hard_gate_rank_rule": percentile_rank_score(rows),
        "model_component_oof": cross_fitted_scores(rows, labels, component_folds),
        "model_seed_oof": cross_fitted_scores(rows, labels, seed_folds),
    }
    metrics = {name: precision_metrics(labels, value) for name, value in scores.items()}
    baselines = ("random", "current_scalar", "hard_gate_rank_rule")
    best_baseline = max(metrics[name]["top_10pct_precision"] for name in baselines)
    component_precision = metrics["model_component_oof"]["top_10pct_precision"]
    seed_precision = metrics["model_seed_oof"]["top_10pct_precision"]
    gate = (
        component_precision >= best_baseline + 0.03
        and seed_precision >= best_baseline + 0.03
        and metrics["model_component_oof"]["top_10pct_lift"] >= 1.10
        and metrics["model_seed_oof"]["top_10pct_lift"] >= 1.10
    )
    full_model = fit_model(rows, labels)
    return {
        "selector_version": SELECTOR_VERSION,
        "metrics": metrics,
        "gate": {
            "minimum_absolute_precision_gain": 0.03,
            "minimum_model_lift": 1.10,
            "best_baseline_top_10pct_precision": best_baseline,
            "passed": bool(gate),
        },
        "calibration": {
            name: calibration_rows(labels, value) for name, value in scores.items()
        },
        "lane_bias": bias_rows(rows, labels, scores["model_component_oof"], "lane_id"),
        "family_bias": bias_rows(rows, labels, scores["model_component_oof"], "family_id"),
        "folds": {
            "component_fold_count": int(len(set(component_folds.tolist()))),
            "seed_fold_count": int(len(set(seed_folds.tolist()))),
            "leakage_component_count": len(set(components)),
        },
        "full_model": full_model.artifact(),
    }
