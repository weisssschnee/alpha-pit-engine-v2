"""Pair-native Transfer V4 diagnostic benchmark on consumed validation labels.

This benchmark asks whether separating candidate primary strength from matched
base-control weakness improves cross-cohort transfer relative to matched-uplift
features alone.  It performs no financial evaluation, no hyperparameter tuning,
and authorizes no model adoption or prospective claim.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from scripts.benchmark_cn_search_core_v2_transfer_v3_diagnostic import _auc, _ap
from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_PAIR_NATIVE_TRANSFER_V4_DIAGNOSTIC_COMPLETE_NO_MODEL_ADOPTION"
COHORT_WAVE1 = "SEARCH_CORE_V2_WAVE1"
TARGETS = ("validation_productive", "validation_stable_2of3")
BASE_MODELS = ("MATCHED5", "PRIMARY4", "CONTROL4", "PAIR8")
MODELS = tuple(item for base in BASE_MODELS for item in (base, f"{base}_TEMPLATE"))


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _template_matrix(rows: Sequence[Mapping[str, Any]], templates: Sequence[str]) -> np.ndarray:
    index = {template: i for i, template in enumerate(templates)}
    matrix = np.zeros((len(rows), len(templates)), dtype=float)
    for row_i, row in enumerate(rows):
        matrix[row_i, index[str(row["template_id"])]] = 1.0
    return matrix


def _feature_keys(model_id: str) -> tuple[str, ...]:
    base = model_id.removesuffix("_TEMPLATE")
    if base == "MATCHED5":
        return (
            "matched_total_return",
            "matched_reward",
            "matched_w1",
            "matched_w2",
            "matched_w3",
        )
    if base == "PRIMARY4":
        return ("primary_total_return", "primary_w1", "primary_w2", "primary_w3")
    if base == "CONTROL4":
        return ("control_total_return", "control_w1", "control_w2", "control_w3")
    if base == "PAIR8":
        return (
            "primary_total_return",
            "primary_w1",
            "primary_w2",
            "primary_w3",
            "control_total_return",
            "control_w1",
            "control_w2",
            "control_w3",
        )
    raise KeyError(model_id)


def _design(
    train_rows: Sequence[Mapping[str, Any]],
    test_rows: Sequence[Mapping[str, Any]],
    *,
    templates: Sequence[str],
    model_id: str,
) -> tuple[np.ndarray, np.ndarray]:
    keys = _feature_keys(model_id)
    train_raw = np.asarray([[float(row[key]) for key in keys] for row in train_rows], dtype=float)
    test_raw = np.asarray([[float(row[key]) for key in keys] for row in test_rows], dtype=float)
    scaler = StandardScaler().fit(train_raw)
    train_parts = [scaler.transform(train_raw)]
    test_parts = [scaler.transform(test_raw)]
    if model_id.endswith("_TEMPLATE"):
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


def _top40(scores: Sequence[float], labels: Sequence[int]) -> dict[str, Any]:
    count = int(math.ceil(0.4 * len(scores)))
    order = sorted(range(len(scores)), key=lambda i: (-float(scores[i]), i))
    chosen = order[:count]
    positive = sum(int(labels[i]) for i in chosen)
    total_positive = sum(map(int, labels))
    base = total_positive / len(labels)
    precision = positive / count if count else 0.0
    return {
        "selected": count,
        "positive": positive,
        "precision": precision,
        "recall": positive / total_positive if total_positive else None,
        "base_rate": base,
        "lift_pp": precision - base,
    }


def _fold(scores: Sequence[float], labels: Sequence[int]) -> dict[str, Any]:
    return {
        "n": len(labels),
        "positive": int(sum(map(int, labels))),
        "positive_rate": sum(map(int, labels)) / len(labels),
        "auc": _auc(scores, labels),
        "average_precision": _ap(scores, labels),
        "top40": _top40(scores, labels),
    }


def _assemble(repo: Path) -> tuple[list[dict[str, Any]], dict[str, str]]:
    rp = repo / "runtime/run_plans"
    v3_path = rp / "cn_search_core_v2_transfer_v3_diagnostic_dataset_20260825.json"
    d1_pair_path = rp / "cn_program_optimizer_d1_pair_native_development_features_20260825.json"
    stability_path = rp / "cn_program_optimizer_d1_validation_stability_labels_20260825.json"
    wave_pair_path = rp / "cn_search_core_v2_wave1_development_pair_windows_shortlist42_20260825.json"

    v3 = _read(v3_path)
    d1_pair = _read(d1_pair_path)
    stability = _read(stability_path)
    wave_pair = _read(wave_pair_path)
    hashes = {
        "v3": _verify(v3, "dataset_payload_sha256", "Transfer V3 dataset"),
        "d1_pair": _verify(d1_pair, "evidence_payload_sha256", "D1 pair-native development evidence"),
        "stability": _verify(stability, "evidence_payload_sha256", "D1 validation stability labels"),
        "wave_pair": _verify(wave_pair, "evidence_payload_sha256", "Wave1 development pair evidence"),
    }

    if len(v3["rows"]) != 225 or len(d1_pair["feature_rows"]) != 183 or len(stability["label_rows"]) != 183 or len(wave_pair["rows"]) != 42:
        raise RuntimeError("Transfer V4 source geometry drift")

    v3_by = {str(row["exact_identity"]): dict(row) for row in v3["rows"]}
    d1_pair_by = {str(row["exact_identity"]): dict(row) for row in d1_pair["feature_rows"]}
    stable_by = {str(row["exact_identity"]): dict(row) for row in stability["label_rows"]}
    wave_pair_by = {str(row["exact_identity"]): dict(row) for row in wave_pair["rows"]}
    historical = {exact for exact, row in v3_by.items() if str(row["source_cohort"]) != COHORT_WAVE1}
    wave = {exact for exact, row in v3_by.items() if str(row["source_cohort"]) == COHORT_WAVE1}
    if historical != set(d1_pair_by) or historical != set(stable_by) or wave != set(wave_pair_by):
        raise RuntimeError("Transfer V4 exact-set authority drift")

    rows: list[dict[str, Any]] = []
    for exact in sorted(v3_by):
        base = v3_by[exact]
        if exact in historical:
            pair = d1_pair_by[exact]
            stable = bool(stable_by[exact]["validation_stable_2of3"])
            if str(pair["source_cohort"]) != str(base["source_cohort"]) or str(pair["template_id"]) != str(base["template_id"]):
                raise RuntimeError(f"historical pair identity drift: {exact}")
            primary = list(map(float, pair["primary_window_returns"]))
            control = list(map(float, pair["control_window_returns"]))
            matched = list(map(float, pair["matched_window_return_increments"]))
            primary_total = float(pair["primary_cumulative_net_return"])
            control_total = float(pair["control_cumulative_net_return"])
        else:
            pair = wave_pair_by[exact]
            stable = bool(base["validation_stable_2of3"])
            if str(pair["template_id"]) != str(base["template_id"]):
                raise RuntimeError(f"wave pair identity drift: {exact}")
            primary = list(map(float, pair["primary_window_returns"]))
            control = list(map(float, pair["control_window_returns"]))
            matched = list(map(float, pair["matched_window_return_increments"]))
            primary_total = float(pair["primary_cumulative_net_return"])
            control_total = float(pair["control_cumulative_net_return"])
        if len(primary) != 3 or len(control) != 3 or len(matched) != 3:
            raise RuntimeError(f"Transfer V4 window geometry drift: {exact}")
        expected_matched = [a - b for a, b in zip(primary, control, strict=True)]
        if any(abs(a - b) > 1e-14 for a, b in zip(expected_matched, matched, strict=True)):
            raise RuntimeError(f"Transfer V4 matched-window reconstruction drift: {exact}")
        if abs((primary_total - control_total) - float(base["dev_matched_return"])) > 1e-12:
            raise RuntimeError(f"Transfer V4 matched-total reconstruction drift: {exact}")
        if any(abs(float(base[f"dev_window_{i+1}_increment"]) - matched[i]) > 1e-14 for i in range(3)):
            raise RuntimeError(f"Transfer V4 frozen matched feature drift: {exact}")
        rows.append(
            {
                "exact_identity": exact,
                "source_cohort": str(base["source_cohort"]),
                "template_id": str(base["template_id"]),
                "matched_total_return": float(base["dev_matched_return"]),
                "matched_reward": float(base["dev_matched_reward"]),
                "matched_w1": matched[0],
                "matched_w2": matched[1],
                "matched_w3": matched[2],
                "primary_total_return": primary_total,
                "primary_w1": primary[0],
                "primary_w2": primary[1],
                "primary_w3": primary[2],
                "control_total_return": control_total,
                "control_w1": control[0],
                "control_w2": control[1],
                "control_w3": control[2],
                "validation_productive": bool(base["validation_productive"]),
                "validation_stable_2of3": stable,
            }
        )
    return rows, hashes


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    rows, source_hashes = _assemble(repo)
    cohorts = tuple(sorted({str(row["source_cohort"]) for row in rows}))
    templates = tuple(sorted({str(row["template_id"]) for row in rows}))
    if len(rows) != 225 or len(cohorts) != 4 or len(templates) != 7:
        raise RuntimeError("Transfer V4 benchmark geometry drift")

    results: dict[str, Any] = {}
    for target in TARGETS:
        target_results: dict[str, Any] = {}
        for model_id in MODELS:
            folds: dict[str, Any] = {}
            for test_cohort in cohorts:
                train_rows = [row for row in rows if str(row["source_cohort"]) != test_cohort]
                test_rows = [row for row in rows if str(row["source_cohort"]) == test_cohort]
                train_y = np.asarray([int(bool(row[target])) for row in train_rows], dtype=int)
                test_y = np.asarray([int(bool(row[target])) for row in test_rows], dtype=int)
                train_x, test_x = _design(train_rows, test_rows, templates=templates, model_id=model_id)
                scores = _fit_predict(train_x, train_y, test_x)
                metric = _fold(scores, test_y)
                metric["feature_dimension"] = int(train_x.shape[1])
                folds[test_cohort] = metric
            aucs = [float(row["auc"]) for row in folds.values() if row["auc"] is not None]
            aps = [float(row["average_precision"]) for row in folds.values() if row["average_precision"] is not None]
            lifts = [float(row["top40"]["lift_pp"]) for row in folds.values()]
            target_results[model_id] = {
                "folds": folds,
                "macro_auc": sum(aucs) / len(aucs),
                "macro_average_precision": sum(aps) / len(aps),
                "macro_top40_lift_pp": sum(lifts) / len(lifts),
                "worst_cohort_auc": min(aucs),
                "worst_cohort_top40_lift_pp": min(lifts),
            }
        results[target] = target_results

    wave_comparisons: dict[str, Any] = {}
    for target in TARGETS:
        tr = results[target]
        wave_comparisons[target] = {}
        for pair_model, baseline in (("PAIR8", "MATCHED5"), ("PAIR8_TEMPLATE", "MATCHED5_TEMPLATE")):
            p = tr[pair_model]["folds"][COHORT_WAVE1]
            b = tr[baseline]["folds"][COHORT_WAVE1]
            wave_comparisons[target][pair_model] = {
                "baseline": baseline,
                "auc_delta": float(p["auc"]) - float(b["auc"]),
                "average_precision_delta": float(p["average_precision"]) - float(b["average_precision"]),
                "top40_lift_delta_pp": float(p["top40"]["lift_pp"]) - float(b["top40"]["lift_pp"]),
                "pair_metric": p,
                "baseline_metric": b,
            }

    rankings = {
        target: sorted(
            MODELS,
            key=lambda model_id: (
                -float(results[target][model_id]["folds"][COHORT_WAVE1]["auc"]),
                -float(results[target][model_id]["folds"][COHORT_WAVE1]["average_precision"]),
                model_id,
            ),
        )
        for target in TARGETS
    }
    payload = {
        "schema_version": "cn_search_core_v2_pair_native_transfer_v4_diagnostic_v1",
        "status": STATUS,
        "source_payload_sha256": source_hashes,
        "candidate_count": len(rows),
        "cohort_counts": {
            cohort: sum(str(row["source_cohort"]) == cohort for row in rows) for cohort in cohorts
        },
        "label_counts": {
            target: {
                cohort: sum(bool(row[target]) for row in rows if str(row["source_cohort"]) == cohort)
                for cohort in cohorts
            }
            for target in TARGETS
        },
        "feature_contract": {
            "models": list(MODELS),
            "MATCHED5": list(_feature_keys("MATCHED5")),
            "PRIMARY4": list(_feature_keys("PRIMARY4")),
            "CONTROL4": list(_feature_keys("CONTROL4")),
            "PAIR8": list(_feature_keys("PAIR8")),
            "template_categories": list(templates),
        },
        "model_contract": {
            "targets": list(TARGETS),
            "outer_split": "LEAVE_ONE_SOURCE_COHORT_OUT",
            "logistic_C": 0.5,
            "class_weight": "balanced",
            "solver": "liblinear",
            "random_state": 82625,
            "standard_scaler_fit_on_train_only": True,
            "hyperparameter_tuning": False,
            "threshold_tuning": False,
        },
        "model_results": results,
        "wave1_pair_vs_matched": wave_comparisons,
        "wave1_rankings": rankings,
        "research_boundaries": {
            "financial_evaluation_executed": False,
            "consumed_validation_labels_used": True,
            "validation_domain_already_consumed": True,
            "same_validation_reuse_for_prospective_claim": False,
            "prospective_claim_authorized": False,
            "holdout_read": False,
            "forward_read": False,
            "automatic_model_adoption_authorized": False,
            "automatic_search_policy_change_authorized": False,
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
        default=Path("runtime/run_plans/cn_search_core_v2_pair_native_transfer_v4_diagnostic_20260825.json"),
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
                "rankings": payload["wave1_rankings"],
                "pair_vs_matched": payload["wave1_pair_vs_matched"],
                "output": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
