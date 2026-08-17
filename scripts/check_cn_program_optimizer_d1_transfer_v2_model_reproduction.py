"""Independently reproduce frozen D1 Transfer Filter V2 from a durable 183-row dataset.

This checker performs no financial evaluation.  It fits the already-frozen
five-feature LogisticRegression contract, evaluates leave-one-cohort-out (LOCO)
ranking on the three burned validation cohorts, and compares coefficients,
intercept and metrics to the committed V2 filter/autopsy evidence.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from our_system_phase2.services.unified_capability_registry import stable_hash

V2_FILTER_REL = Path("runtime/run_plans/cn_program_optimizer_d1_transfer_filter_v2_20260817.json")
V2_AUTOPSY_REL = Path("runtime/run_plans/cn_program_optimizer_d1_transfer_v2_autopsy_20260817.json")
FEATURES = (
    "dev_matched_return",
    "dev_matched_reward",
    "dev_window_1_increment",
    "dev_window_2_increment",
    "dev_window_3_increment",
)
COHORTS = ("D1_CONTINUATION_B", "D1_FRESH", "SUCCESSOR_D1")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")


def _model() -> LogisticRegression:
    return LogisticRegression(
        C=0.5,
        class_weight="balanced",
        solver="liblinear",
        random_state=82617,
    )


def _top40(rows: Sequence[Mapping[str, Any]], scores: np.ndarray, labels: np.ndarray) -> dict[str, Any]:
    if len(rows) != len(scores) or len(rows) != len(labels):
        raise ValueError("top40 vector cardinality drift")
    k = int(math.ceil(0.4 * len(rows)))
    order = sorted(range(len(rows)), key=lambda idx: (-float(scores[idx]), str(rows[idx]["exact_identity"])))
    chosen = order[:k]
    productive = int(sum(int(labels[idx]) for idx in chosen))
    total_positive = int(labels.sum())
    base_precision = float(labels.mean()) if len(labels) else 0.0
    precision = productive / k if k else 0.0
    recall = productive / total_positive if total_positive else 0.0
    return {
        "population": len(rows),
        "top40_selected": k,
        "top40_productive": productive,
        "validation_productive": total_positive,
        "base_precision": base_precision,
        "top40_precision": precision,
        "top40_recall": recall,
        "top40_lift_pp": precision - base_precision,
    }


def _close(left: float, right: float, tol: float = 1e-10) -> bool:
    return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=tol)


def check(*, repo_root: Path, dataset_path: Path) -> dict[str, Any]:
    root = repo_root.resolve()
    dataset = _read_json(dataset_path.resolve())
    filter_payload = _read_json(root / V2_FILTER_REL)
    autopsy = _read_json(root / V2_AUTOPSY_REL)
    _verify_self_hash(dataset, "dataset_payload_sha256", "V2 reproduction dataset")
    _verify_self_hash(filter_payload, "filter_payload_sha256", "V2 filter")
    _verify_self_hash(autopsy, "autopsy_payload_sha256", "V2 autopsy")

    rows = list(dataset.get("rows") or ())
    if int(dataset.get("candidate_count") or 0) != 183 or len(rows) != 183:
        raise RuntimeError("V2 reproduction dataset cardinality drift")
    if int(dataset.get("validation_productive_count") or 0) != 50:
        raise RuntimeError("V2 reproduction positive-count drift")
    if tuple(filter_payload.get("features") or ()) != FEATURES:
        raise RuntimeError("frozen V2 feature contract drift")
    if tuple(dict(filter_payload.get("model") or {}).get(key) for key in ("C", "class_weight", "solver", "random_state")) != (0.5, "balanced", "liblinear", 82617):
        raise RuntimeError("frozen V2 model contract drift")

    exacts = [str(row.get("exact_identity") or "") for row in rows]
    if any(not exact for exact in exacts) or len(set(exacts)) != len(exacts):
        raise RuntimeError("V2 reproduction exact identity drift")
    for row in rows:
        if str(row.get("source_cohort") or "") not in COHORTS:
            raise RuntimeError("V2 reproduction cohort drift")
        for feature in FEATURES:
            value = float(row[feature])
            if not math.isfinite(value):
                raise RuntimeError(f"non-finite reproduction feature: {feature}")

    x = np.asarray([[float(row[feature]) for feature in FEATURES] for row in rows], dtype=float)
    y = np.asarray([1 if bool(row["validation_productive"]) else 0 for row in rows], dtype=int)
    full = _model().fit(x, y)
    fitted_coefficients = {feature: float(value) for feature, value in zip(FEATURES, full.coef_[0], strict=True)}
    fitted_intercept = float(full.intercept_[0])
    frozen_formula = dict(filter_payload["score_formula_raw"])
    frozen_coefficients = dict(frozen_formula["coefficients"])
    coefficient_checks = {feature: _close(fitted_coefficients[feature], float(frozen_coefficients[feature])) for feature in FEATURES}
    intercept_check = _close(fitted_intercept, float(frozen_formula["intercept"]))

    expected_folds = dict(dict(autopsy["leave_one_cohort_out"])["top40_folds"])
    fold_results: dict[str, Any] = {}
    aggregate_scores = np.zeros(len(rows), dtype=float)
    aggregate_seen = np.zeros(len(rows), dtype=bool)
    fold_checks: dict[str, bool] = {}

    cohort_array = np.asarray([str(row["source_cohort"]) for row in rows], dtype=object)
    for cohort in COHORTS:
        test_idx = np.where(cohort_array == cohort)[0]
        train_idx = np.where(cohort_array != cohort)[0]
        if not len(test_idx) or not len(train_idx):
            raise RuntimeError(f"LOCO cohort split empty: {cohort}")
        model = _model().fit(x[train_idx], y[train_idx])
        scores = model.decision_function(x[test_idx])
        aggregate_scores[test_idx] = scores
        aggregate_seen[test_idx] = True
        held_rows = [rows[int(idx)] for idx in test_idx]
        held_y = y[test_idx]
        metric = _top40(held_rows, scores, held_y)
        metric["auc"] = float(roc_auc_score(held_y, scores))
        fold_results[cohort] = metric
        expected = dict(expected_folds[cohort])
        keys = (
            "population",
            "top40_selected",
            "top40_productive",
            "validation_productive",
            "base_precision",
            "top40_precision",
            "top40_recall",
            "top40_lift_pp",
            "auc",
        )
        fold_checks[cohort] = all(
            int(metric[key]) == int(expected[key]) if key in {"population", "top40_selected", "top40_productive", "validation_productive"}
            else _close(float(metric[key]), float(expected[key]))
            for key in keys
        )

    if not bool(aggregate_seen.all()):
        raise RuntimeError("LOCO aggregate prediction coverage drift")
    aggregate_auc = float(roc_auc_score(y, aggregate_scores))
    aggregate_ap = float(average_precision_score(y, aggregate_scores))
    expected_loco = dict(autopsy["leave_one_cohort_out"])
    aggregate_checks = {
        "aggregate_auc": _close(aggregate_auc, float(expected_loco["aggregate_auc"])),
        "aggregate_average_precision": _close(aggregate_ap, float(expected_loco["aggregate_average_precision"])),
    }

    checks = {
        "coefficients": all(coefficient_checks.values()),
        "intercept": intercept_check,
        "all_LOCO_folds": all(fold_checks.values()),
        "aggregate_auc": aggregate_checks["aggregate_auc"],
        "aggregate_average_precision": aggregate_checks["aggregate_average_precision"],
    }
    status = "PASS_V2_MODEL_AND_LOCO_REPRODUCTION" if all(checks.values()) else "FAIL_CLOSED_V2_MODEL_REPRODUCTION_DRIFT"
    report: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_d1_transfer_v2_model_reproduction_check_v1",
        "status": status,
        "dataset_payload_sha256": dataset["dataset_payload_sha256"],
        "candidate_count": len(rows),
        "validation_productive_count": int(y.sum()),
        "fitted_model": {
            "features": list(FEATURES),
            "coefficients": fitted_coefficients,
            "intercept": fitted_intercept,
        },
        "frozen_model": {
            "filter_payload_sha256": filter_payload["filter_payload_sha256"],
            "coefficients": {feature: float(frozen_coefficients[feature]) for feature in FEATURES},
            "intercept": float(frozen_formula["intercept"]),
        },
        "coefficient_checks": coefficient_checks,
        "intercept_check": intercept_check,
        "LOCO_folds": fold_results,
        "LOCO_fold_checks": fold_checks,
        "aggregate_LOCO": {"auc": aggregate_auc, "average_precision": aggregate_ap},
        "aggregate_LOCO_checks": aggregate_checks,
        "checks": checks,
        "financial_evaluation_performed_by_this_check": False,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "promotion_authorized": False,
    }
    report["check_payload_sha256"] = stable_hash(report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = check(repo_root=args.repo_root, dataset_path=args.dataset)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["status"].startswith("PASS_") else 2


if __name__ == "__main__":
    raise SystemExit(main())
