"""Diagnostic outer-LOCO benchmark for Search Core V2 Transfer V3 research."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from scripts.apply_cn_program_optimizer_d1_transfer_filter_v2 import _score
from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_TRANSFER_V3_DIAGNOSTIC_BENCHMARK_COMPLETE_NO_MODEL_ADOPTION"
MODELS = ("RAW5", "RANK5", "TEMPLATE_ONLY", "RAW5_TEMPLATE", "RANK5_TEMPLATE")


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _auc(scores: Sequence[float], labels: Sequence[int]) -> float | None:
    positive = [float(s) for s, y in zip(scores, labels, strict=True) if int(y) == 1]
    negative = [float(s) for s, y in zip(scores, labels, strict=True) if int(y) == 0]
    if not positive or not negative:
        return None
    wins = 0.0
    for p in positive:
        for n in negative:
            wins += 1.0 if p > n else 0.5 if p == n else 0.0
    return wins / (len(positive) * len(negative))


def _ap(scores: Sequence[float], labels: Sequence[int]) -> float | None:
    order = sorted(range(len(scores)), key=lambda i: (-float(scores[i]), i))
    total = sum(int(labels[i]) for i in order)
    if total == 0:
        return None
    hits = 0
    acc = 0.0
    for rank, index in enumerate(order, 1):
        if int(labels[index]):
            hits += 1
            acc += hits / rank
    return acc / total


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    lm = sum(left) / len(left)
    rm = sum(right) / len(right)
    ld = [float(value) - lm for value in left]
    rd = [float(value) - rm for value in right]
    denom = math.sqrt(sum(value * value for value in ld) * sum(value * value for value in rd))
    return sum(a * b for a, b in zip(ld, rd, strict=True)) / denom if denom else None


def _top_fraction(scores: Sequence[float], labels: Sequence[int], fraction: float = 0.4) -> dict[str, Any]:
    count = int(math.ceil(fraction * len(scores)))
    order = sorted(range(len(scores)), key=lambda i: (-float(scores[i]), i))
    chosen = order[:count]
    positive = sum(int(labels[i]) for i in chosen)
    total_positive = sum(map(int, labels))
    base = total_positive / len(labels)
    precision = positive / count if count else 0.0
    return {
        "selected": count,
        "productive": positive,
        "precision": precision,
        "recall": positive / total_positive if total_positive else None,
        "base_rate": base,
        "lift_pp": precision - base,
    }


def _percentile_ranks(rows: Sequence[Mapping[str, Any]], features: Sequence[str]) -> np.ndarray:
    matrix = np.asarray([[float(row[f]) for f in features] for row in rows], dtype=float)
    ranked = np.zeros_like(matrix)
    n = len(rows)
    for column in range(matrix.shape[1]):
        values = matrix[:, column]
        order = np.argsort(values, kind="mergesort")
        i = 0
        while i < n:
            j = i + 1
            while j < n and values[order[j]] == values[order[i]]:
                j += 1
            average_rank = ((i + 1) + j) / 2.0
            percentile = (average_rank - 0.5) / n
            ranked[order[i:j], column] = percentile
            i = j
    return ranked


def _template_matrix(rows: Sequence[Mapping[str, Any]], templates: Sequence[str]) -> np.ndarray:
    index = {template: i for i, template in enumerate(templates)}
    matrix = np.zeros((len(rows), len(templates)), dtype=float)
    for row_index, row in enumerate(rows):
        matrix[row_index, index[str(row["template_id"])]] = 1.0
    return matrix


def _numeric_matrix(rows: Sequence[Mapping[str, Any]], features: Sequence[str], *, rank: bool) -> np.ndarray:
    if rank:
        # Cohort-relative transform uses DEVELOPMENT features only, including
        # unlabeled target-cohort feature distribution. No validation label is used.
        by_cohort: dict[str, list[int]] = {}
        for index, row in enumerate(rows):
            by_cohort.setdefault(str(row["source_cohort"]), []).append(index)
        output = np.zeros((len(rows), len(features)), dtype=float)
        for indices in by_cohort.values():
            sub = [rows[index] for index in indices]
            ranked = _percentile_ranks(sub, features)
            output[np.asarray(indices), :] = ranked
        return output
    return np.asarray([[float(row[f]) for f in features] for row in rows], dtype=float)


def _design(
    train_rows: Sequence[Mapping[str, Any]],
    test_rows: Sequence[Mapping[str, Any]],
    *,
    features: Sequence[str],
    templates: Sequence[str],
    model_id: str,
) -> tuple[np.ndarray, np.ndarray]:
    rank = model_id.startswith("RANK5")
    include_numeric = model_id != "TEMPLATE_ONLY"
    include_template = "TEMPLATE" in model_id
    if include_numeric:
        # Rank transform is cohort-local, so concatenate only after transforming
        # train/test cohorts independently.
        train_numeric = _numeric_matrix(train_rows, features, rank=rank)
        test_numeric = _numeric_matrix(test_rows, features, rank=rank)
        scaler = StandardScaler().fit(train_numeric)
        train_parts = [scaler.transform(train_numeric)]
        test_parts = [scaler.transform(test_numeric)]
    else:
        train_parts = []
        test_parts = []
    if include_template:
        train_parts.append(_template_matrix(train_rows, templates))
        test_parts.append(_template_matrix(test_rows, templates))
    return np.concatenate(train_parts, axis=1), np.concatenate(test_parts, axis=1)


def _fit_predict(train_x: np.ndarray, train_y: np.ndarray, test_x: np.ndarray) -> np.ndarray:
    model = LogisticRegression(
        C=0.5,
        class_weight="balanced",
        solver="liblinear",
        random_state=82625,
        max_iter=1000,
    )
    model.fit(train_x, train_y)
    return model.decision_function(test_x)


def _fold_metric(scores: Sequence[float], labels: Sequence[int]) -> dict[str, Any]:
    return {
        "n": len(labels),
        "productive": int(sum(map(int, labels))),
        "auc": _auc(scores, labels),
        "average_precision": _ap(scores, labels),
        "top40": _top_fraction(scores, labels),
    }


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    rp = repo / "runtime/run_plans"
    dataset_path = rp / "cn_search_core_v2_transfer_v3_diagnostic_dataset_20260825.json"
    dataset = _read(dataset_path)
    dataset_hash = _verify(dataset, "dataset_payload_sha256", "Transfer V3 diagnostic dataset")
    if dataset.get("status") != "SEARCH_CORE_V2_TRANSFER_V3_DIAGNOSTIC_DATASET_FROZEN_CONSUMED_VALIDATION_ONLY":
        raise RuntimeError("Transfer V3 diagnostic dataset status drift")
    rows = list(dataset["rows"])
    features = tuple(dataset["features"])
    templates = tuple(sorted({str(row["template_id"]) for row in rows}))
    cohorts = tuple(sorted({str(row["source_cohort"]) for row in rows}))
    if len(rows) != 225 or len(cohorts) != 4 or len(templates) != 7:
        raise RuntimeError("Transfer V3 benchmark geometry drift")

    model_results: dict[str, Any] = {}
    wave_scores: dict[str, list[float]] = {}
    for model_id in MODELS:
        folds = {}
        for test_cohort in cohorts:
            train_rows = [row for row in rows if str(row["source_cohort"]) != test_cohort]
            test_rows = [row for row in rows if str(row["source_cohort"]) == test_cohort]
            train_y = np.asarray([int(bool(row["validation_productive"])) for row in train_rows], dtype=int)
            test_y = np.asarray([int(bool(row["validation_productive"])) for row in test_rows], dtype=int)
            train_x, test_x = _design(train_rows, test_rows, features=features, templates=templates, model_id=model_id)
            scores = _fit_predict(train_x, train_y, test_x)
            fold = _fold_metric(scores, test_y)
            fold["feature_dimension"] = int(train_x.shape[1])
            folds[test_cohort] = fold
            if test_cohort == "SEARCH_CORE_V2_WAVE1":
                wave_scores[model_id] = list(map(float, scores))
        aucs = [float(fold["auc"]) for fold in folds.values() if fold["auc"] is not None]
        aps = [float(fold["average_precision"]) for fold in folds.values() if fold["average_precision"] is not None]
        lifts = [float(fold["top40"]["lift_pp"]) for fold in folds.values()]
        model_results[model_id] = {
            "folds": folds,
            "macro_auc": sum(aucs) / len(aucs),
            "macro_average_precision": sum(aps) / len(aps),
            "macro_top40_lift_pp": sum(lifts) / len(lifts),
            "worst_cohort_auc": min(aucs),
            "worst_cohort_top40_lift_pp": min(lifts),
        }

    wave_rows = [row for row in rows if str(row["source_cohort"]) == "SEARCH_CORE_V2_WAVE1"]
    wave_productive = [int(bool(row["validation_productive"])) for row in wave_rows]
    wave_stable = [int(bool(row["validation_stable_2of3"])) for row in wave_rows]
    secondary = {}
    for model_id, scores in wave_scores.items():
        secondary[model_id] = {
            "productive_auc": _auc(scores, wave_productive),
            "productive_average_precision": _ap(scores, wave_productive),
            "stable2_auc": _auc(scores, wave_stable),
            "stable2_average_precision": _ap(scores, wave_stable),
            "stable2_top40": _top_fraction(scores, wave_stable),
        }

    # Exact reproduction check: RAW5 on the Wave1 outer fold trains on the
    # original 183 D1 rows, so its score should be affine-equivalent to frozen V2.
    frozen_filter = _read(rp / "cn_program_optimizer_d1_transfer_filter_v2_20260817.json")
    _verify(frozen_filter, "filter_payload_sha256", "Frozen D1 Transfer V2")
    frozen_scores = []
    for row in wave_rows:
        frozen_scores.append(float(_score({feature: float(row[feature]) for feature in features}, frozen_filter)))
    raw_scores = wave_scores["RAW5"]
    affine_correlation = _pearson(raw_scores, frozen_scores)
    frozen_rank_match = sorted(range(len(raw_scores)), key=lambda i: (-raw_scores[i], i)) == sorted(range(len(frozen_scores)), key=lambda i: (-frozen_scores[i], i))

    ranked_models = sorted(
        MODELS,
        key=lambda model_id: (
            -float(model_results[model_id]["folds"]["SEARCH_CORE_V2_WAVE1"]["auc"]),
            -float(model_results[model_id]["folds"]["SEARCH_CORE_V2_WAVE1"]["average_precision"]),
            model_id,
        ),
    )
    payload = {
        "schema_version": "cn_search_core_v2_transfer_v3_diagnostic_benchmark_v1",
        "status": STATUS,
        "dataset_payload_sha256": dataset_hash,
        "model_contract": {
            "models": list(MODELS),
            "logistic_C": 0.5,
            "class_weight": "balanced",
            "solver": "liblinear",
            "random_state": 82625,
            "hyperparameter_tuning": False,
            "outer_split": "LEAVE_ONE_SOURCE_COHORT_OUT",
            "rank_transform": "COHORT_RELATIVE_PERCENTILE_FROM_DEVELOPMENT_FEATURES_ONLY",
            "template_categories": list(templates),
        },
        "model_results": model_results,
        "wave1_secondary_stability_diagnostic": secondary,
        "wave1_models_ranked_by_productive_auc": list(ranked_models),
        "raw5_frozen_v2_reproduction": {
            "wave1_score_pearson": affine_correlation,
            "wave1_rank_exact_match": frozen_rank_match,
        },
        "research_boundaries": {
            "financial_evaluation_executed": False,
            "consumed_validation_labels_used": True,
            "prospective_claim_authorized": False,
            "same_validation_reuse_for_prospective_claim": False,
            "holdout_read": False,
            "forward_read": False,
            "automatic_model_adoption_authorized": False,
            "promotion_authorized": False,
        },
    }
    payload["benchmark_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, default=Path("runtime/run_plans/cn_search_core_v2_transfer_v3_diagnostic_benchmark_20260825.json"))
    args = parser.parse_args(argv)
    payload = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    wave = {model: payload["model_results"][model]["folds"]["SEARCH_CORE_V2_WAVE1"] for model in MODELS}
    print(json.dumps({"status": payload["status"], "payload": payload["benchmark_payload_sha256"], "wave1": wave, "ranking": payload["wave1_models_ranked_by_productive_auc"], "stable2": payload["wave1_secondary_stability_diagnostic"], "output": str(output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
