"""Diagnostic LOCO benchmark for validation stability transfer.

Uses only already-consumed validation labels. Historical D1 stable-2-of-3 labels
are recovered from immutable formal results and joined by exact identity to the
frozen Transfer V3 diagnostic dataset. No new financial/OOS read occurs and no
model adoption is authorized.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from scripts import benchmark_cn_search_core_v2_transfer_v3_diagnostic as base
from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_TRANSFER_STABILITY_DIAGNOSTIC_BENCHMARK_COMPLETE_NO_MODEL_ADOPTION"
WAVE = "SEARCH_CORE_V2_WAVE1"


def _fold_metric(scores: list[float] | np.ndarray, labels: np.ndarray) -> dict[str, Any]:
    values = list(map(float, scores))
    target = list(map(int, labels))
    top40 = base._top_fraction(values, target)
    top40["stable_2of3"] = top40.pop("productive")
    return {
        "n": len(target),
        "stable_2of3": int(sum(target)),
        "stable_2of3_rate": float(sum(target) / len(target)) if target else 0.0,
        "auc": base._auc(values, target),
        "average_precision": base._ap(values, target),
        "top40": top40,
    }


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    rp = repo / "runtime/run_plans"
    dataset_path = rp / "cn_search_core_v2_transfer_v3_diagnostic_dataset_20260825.json"
    stability_path = rp / "cn_program_optimizer_d1_validation_stability_labels_20260825.json"
    dataset = base._read(dataset_path)
    dataset_hash = base._verify(dataset, "dataset_payload_sha256", "Transfer V3 diagnostic dataset")
    stability = base._read(stability_path)
    stability_hash = base._verify(stability, "evidence_payload_sha256", "D1 validation stability labels")
    if dataset.get("status") != "SEARCH_CORE_V2_TRANSFER_V3_DIAGNOSTIC_DATASET_FROZEN_CONSUMED_VALIDATION_ONLY":
        raise RuntimeError("Transfer V3 diagnostic dataset status drift")
    if stability.get("status") != "D1_VALIDATION_STABILITY_LABELS_RECOVERED_FROM_IMMUTABLE_CONSUMED_RESULTS":
        raise RuntimeError("D1 stability evidence status drift")
    if bool(stability["research_boundaries"]["financial_evaluation_performed"]):
        raise RuntimeError("D1 stability evidence unexpectedly performed financial evaluation")

    rows = [dict(row) for row in dataset["rows"]]
    if len(rows) != 225:
        raise RuntimeError("stability benchmark dataset cardinality drift")
    historical = [row for row in rows if str(row["source_cohort"]) != WAVE]
    wave = [row for row in rows if str(row["source_cohort"]) == WAVE]
    labels = {str(row["exact_identity"]): dict(row) for row in stability["label_rows"]}
    historical_exact = {str(row["exact_identity"]) for row in historical}
    if len(historical) != 183 or len(labels) != 183 or historical_exact != set(labels):
        raise RuntimeError("historical stability label exact coverage drift")
    if len(wave) != 42 or any(row.get("validation_stable_2of3") is None for row in wave):
        raise RuntimeError("Wave1 stable2 labels missing from frozen diagnostic dataset")

    joined: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        if str(row["source_cohort"]) != WAVE:
            label = labels[str(row["exact_identity"])]
            if str(label["source_cohort"]) != str(row["source_cohort"]):
                raise RuntimeError("historical stability source cohort drift")
            if str(label["template_id"]) != str(row["template_id"]):
                raise RuntimeError("historical stability template drift")
            payload["validation_stable_2of3"] = bool(label["validation_stable_2of3"])
            payload["stable_label_authority_sha256"] = str(label["label_row_sha256"])
        joined.append(payload)

    features = tuple(dataset["features"])
    templates = tuple(sorted({str(row["template_id"]) for row in joined}))
    cohorts = tuple(sorted({str(row["source_cohort"]) for row in joined}))
    if len(features) != 5 or len(templates) != 7 or len(cohorts) != 4:
        raise RuntimeError("stability benchmark geometry drift")

    cohort_labels = {
        cohort: {
            "n": sum(str(row["source_cohort"]) == cohort for row in joined),
            "stable_2of3": sum(
                str(row["source_cohort"]) == cohort and bool(row["validation_stable_2of3"])
                for row in joined
            ),
        }
        for cohort in cohorts
    }

    model_results: dict[str, Any] = {}
    for model_id in base.MODELS:
        folds: dict[str, Any] = {}
        for test_cohort in cohorts:
            train_rows = [row for row in joined if str(row["source_cohort"]) != test_cohort]
            test_rows = [row for row in joined if str(row["source_cohort"]) == test_cohort]
            train_y = np.asarray([int(bool(row["validation_stable_2of3"])) for row in train_rows], dtype=int)
            test_y = np.asarray([int(bool(row["validation_stable_2of3"])) for row in test_rows], dtype=int)
            if len(set(map(int, train_y))) != 2:
                raise RuntimeError(f"stable2 train class collapse: {test_cohort}")
            train_x, test_x = base._design(
                train_rows,
                test_rows,
                features=features,
                templates=templates,
                model_id=model_id,
            )
            scores = base._fit_predict(train_x, train_y, test_x)
            fold = _fold_metric(scores, test_y)
            fold["feature_dimension"] = int(train_x.shape[1])
            folds[test_cohort] = fold
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

    ranked = sorted(
        base.MODELS,
        key=lambda model_id: (
            -float(model_results[model_id]["folds"][WAVE]["auc"]),
            -float(model_results[model_id]["folds"][WAVE]["average_precision"]),
            model_id,
        ),
    )
    payload = {
        "schema_version": "cn_search_core_v2_transfer_stability_diagnostic_benchmark_v1",
        "status": STATUS,
        "dataset_payload_sha256": dataset_hash,
        "stability_evidence_payload_sha256": stability_hash,
        "candidate_count": len(joined),
        "stable_2of3_count": sum(bool(row["validation_stable_2of3"]) for row in joined),
        "cohort_labels": cohort_labels,
        "model_contract": {
            "models": list(base.MODELS),
            "target": "VALIDATION_STABLE_2OF3",
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
        "wave1_models_ranked_by_stable2_auc": list(ranked),
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
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_search_core_v2_transfer_stability_diagnostic_benchmark_20260825.json"),
    )
    args = parser.parse_args(argv)
    payload = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": payload["status"],
                "payload": payload["benchmark_payload_sha256"],
                "labels": payload["cohort_labels"],
                "wave1": {model: payload["model_results"][model]["folds"][WAVE] for model in base.MODELS},
                "ranking": payload["wave1_models_ranked_by_stable2_auc"],
                "output": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
