"""Audit whether frozen D1 Transfer Filter V2 is reproducible from durable Git evidence.

This is a zero-financial-read checker.  It never opens development/validation
field stores, holdout, Forward-B, or Forward-2026.  It only inspects already
committed run-plan artifacts.  The audit fails closed when the chronological
three-window development features or per-exact validation labels needed to
rebuild the 183-row V2 tuning population are not durably frozen.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from our_system_phase2.services.unified_capability_registry import stable_hash

OLD_MEMBERS_REL = Path("runtime/run_plans/cn_program_optimizer_d1_validation_candidate_members_20260816.jsonl")
OLD_SURVIVORS_REL = Path("runtime/run_plans/cn_program_optimizer_d1_validation_survivor_freeze_20260817.json")
B_MEMBERS_REL = Path("runtime/run_plans/cn_program_optimizer_d1_transfer_B_validation_freeze_20260817/validation_candidate_members.jsonl")
B_OUTCOME_REL = Path("runtime/run_plans/cn_program_optimizer_d1_transfer_B_validation_outcome_20260817.json")
V2_AUTOPSY_REL = Path("runtime/run_plans/cn_program_optimizer_d1_transfer_v2_autopsy_20260817.json")
V2_FILTER_REL = Path("runtime/run_plans/cn_program_optimizer_d1_transfer_filter_v2_20260817.json")
RECOVERY_DIR_REL = Path("runtime/run_plans/cn_program_optimizer_d1_transfer_v2_recovery_20260817")
RECOVERED_WINDOWS_REL = RECOVERY_DIR_REL / "development_window_features.json"
RECOVERED_B_LABELS_REL = RECOVERY_DIR_REL / "B63_validation_labels.json"
RECOVERED_DATASET_REL = RECOVERY_DIR_REL / "combined_183_dataset.json"
RECOVERED_MODEL_REL = RECOVERY_DIR_REL / "model_reproduction_check_standardized.json"
EXPECTED_WINDOWS = ("development_1", "development_2", "development_3")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")


def _window_values(row: Mapping[str, Any]) -> tuple[float, float, float] | None:
    values = row.get("development_window_return_increments")
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)) or len(values) != 3:
        return None
    try:
        return tuple(float(value) for value in values)  # type: ignore[return-value]
    except (TypeError, ValueError):
        return None


def _exact_labels_from_members(rows: Sequence[Mapping[str, Any]]) -> dict[str, bool]:
    labels: dict[str, bool] = {}
    for row in rows:
        if "validation_productive" not in row:
            continue
        exact = str(row.get("exact_identity") or "")
        if not exact or exact in labels:
            raise RuntimeError("duplicate/empty durable validation label exact identity")
        labels[exact] = bool(row["validation_productive"])
    return labels


def _load_recovered_evidence(
    root: Path,
    *,
    expected_exacts: set[str],
    expected_b_exacts: set[str],
    v2_filter: Mapping[str, Any],
    v2_autopsy: Mapping[str, Any],
) -> dict[str, Any] | None:
    paths = {
        "development_windows": root / RECOVERED_WINDOWS_REL,
        "B_labels": root / RECOVERED_B_LABELS_REL,
        "dataset": root / RECOVERED_DATASET_REL,
        "model_reproduction": root / RECOVERED_MODEL_REL,
    }
    existing = {name: path.is_file() for name, path in paths.items()}
    if not any(existing.values()):
        return None
    if not all(existing.values()):
        raise RuntimeError(f"partial V2 recovery pack: {existing}")

    windows = _read_json(paths["development_windows"])
    labels = _read_json(paths["B_labels"])
    dataset = _read_json(paths["dataset"])
    model = _read_json(paths["model_reproduction"])
    _verify_self_hash(windows, "evidence_payload_sha256", "recovered development windows")
    _verify_self_hash(labels, "evidence_payload_sha256", "recovered B labels")
    _verify_self_hash(dataset, "dataset_payload_sha256", "recovered V2 dataset")
    _verify_self_hash(model, "check_payload_sha256", "recovered V2 model reproduction")

    window_rows = list(windows.get("feature_rows") or ())
    label_rows = list(labels.get("label_rows") or ())
    dataset_rows = list(dataset.get("rows") or ())
    window_exacts = {str(row.get("exact_identity") or "") for row in window_rows}
    label_exacts = {str(row.get("exact_identity") or "") for row in label_rows}
    dataset_exacts = {str(row.get("exact_identity") or "") for row in dataset_rows}
    if (
        int(windows.get("candidate_count") or 0) != 183
        or len(window_rows) != 183
        or tuple(windows.get("development_window_ids") or ()) != EXPECTED_WINDOWS
        or stable_hash(window_rows) != str(windows.get("feature_rows_sha256") or "")
        or window_exacts != expected_exacts
        or int(labels.get("candidate_count") or 0) != 63
        or int(labels.get("validation_productive_count") or 0) != 20
        or len(label_rows) != 63
        or stable_hash(label_rows) != str(labels.get("label_rows_sha256") or "")
        or label_exacts != expected_b_exacts
        or int(dataset.get("candidate_count") or 0) != 183
        or int(dataset.get("validation_productive_count") or 0) != 50
        or len(dataset_rows) != 183
        or stable_hash(dataset_rows) != str(dataset.get("rows_sha256") or "")
        or dataset_exacts != expected_exacts
    ):
        raise RuntimeError("recovered V2 evidence semantic drift")

    expected_loco = dict(v2_autopsy.get("leave_one_cohort_out") or {})
    observed_loco = dict(model.get("aggregate_LOCO") or {})
    preprocessing = dict(model.get("recovered_preprocessing_contract") or {})
    if (
        model.get("status") != "PASS_V2_MODEL_AND_LOCO_REPRODUCTION"
        or not all(bool(value) for value in dict(model.get("checks") or {}).values())
        or not all(bool(value) for value in dict(model.get("LOCO_fold_checks") or {}).values())
        or int(model.get("candidate_count") or 0) != 183
        or int(model.get("validation_productive_count") or 0) != 50
        or model.get("dataset_payload_sha256") != dataset.get("dataset_payload_sha256")
        or model.get("frozen_model", {}).get("filter_payload_sha256") != v2_filter.get("filter_payload_sha256")
        or preprocessing.get("family") != "STANDARD_SCALER"
        or preprocessing.get("fit_scope") != "FIT_ON_TRAINING_ROWS_FOR_EACH_MODEL_FIT"
        or float(observed_loco.get("auc")) != float(expected_loco.get("aggregate_auc"))
        or float(observed_loco.get("average_precision")) != float(expected_loco.get("aggregate_average_precision"))
        or int(model.get("holdout_reads") or 0) != 0
        or int(model.get("forward_2026_reads") or 0) != 0
    ):
        raise RuntimeError("recovered V2 model/LOCO contract drift")

    return {
        "status": "PASS_V2_MODEL_AND_LOCO_REPRODUCTION",
        "development_window_rows": len(window_rows),
        "B_label_rows": len(label_rows),
        "candidate_count": int(dataset["candidate_count"]),
        "validation_productive_count": int(dataset["validation_productive_count"]),
        "dataset_rows_sha256": str(dataset["rows_sha256"]),
        "legacy_dataset_serialization_sha256_claim": str(
            dict(v2_autopsy.get("tuning_population") or {}).get("combined_183_window_dataset_sha256") or ""
        ),
        "legacy_dataset_serialization_hash_matches_recovered_rows": bool(
            dataset.get("rows_sha256_matches_frozen_claim")
        ),
        "legacy_dataset_hash_interpretation": (
            "LEGACY_TEMPORARY_SERIALIZATION_NOT_BYTE_REPRODUCED; CANDIDATE/LABEL POPULATION, "
            "STANDARDIZED MODEL, RAW-SPACE SCORE, ALL LOCO FOLDS AND AGGREGATE METRICS REPRODUCED EXACTLY"
        ),
        "preprocessing_contract": preprocessing,
        "aggregate_LOCO": observed_loco,
        "model_reproduction_payload_sha256": str(model["check_payload_sha256"]),
        "sources": {
            name: {
                "relative_path": path.relative_to(root).as_posix(),
                "file_sha256": _sha256(path),
            }
            for name, path in paths.items()
        },
    }


def build_audit(repo_root: Path) -> dict[str, Any]:
    root = repo_root.resolve()
    old_members_path = root / OLD_MEMBERS_REL
    old_survivors_path = root / OLD_SURVIVORS_REL
    b_members_path = root / B_MEMBERS_REL
    b_outcome_path = root / B_OUTCOME_REL
    v2_autopsy_path = root / V2_AUTOPSY_REL
    v2_filter_path = root / V2_FILTER_REL

    old_members = _read_jsonl(old_members_path)
    old_survivors = _read_json(old_survivors_path)
    b_members = _read_jsonl(b_members_path)
    b_outcome = _read_json(b_outcome_path)
    v2_autopsy = _read_json(v2_autopsy_path)
    v2_filter = _read_json(v2_filter_path)

    _verify_self_hash(old_survivors, "freeze_payload_sha256", "old validation survivor freeze")
    _verify_self_hash(b_outcome, "outcome_payload_sha256", "B prospective validation outcome")
    _verify_self_hash(v2_autopsy, "autopsy_payload_sha256", "V2 autopsy")
    _verify_self_hash(v2_filter, "filter_payload_sha256", "V2 filter")

    if len(old_members) != 120 or len(b_members) != 63:
        raise RuntimeError("durable transfer cohort cardinality drift")
    if len({str(row["exact_identity"]) for row in old_members}) != len(old_members):
        raise RuntimeError("old transfer cohort exact identity duplication")
    if len({str(row["exact_identity"]) for row in b_members}) != len(b_members):
        raise RuntimeError("B transfer cohort exact identity duplication")

    survivor_ids = set(map(str, old_survivors.get("exact_identities") or ()))
    old_ids = {str(row["exact_identity"]) for row in old_members}
    if len(survivor_ids) != 30 or not survivor_ids.issubset(old_ids):
        raise RuntimeError("old 120 validation label freeze drift")
    old_labels = {exact: exact in survivor_ids for exact in old_ids}

    b_member_labels = _exact_labels_from_members(b_members)
    b_productive = int(dict(b_outcome["all_development_positive_metric"])["productive"])
    if int(b_outcome["candidate_count"]) != 63 or b_productive != 20:
        raise RuntimeError("B aggregate prospective outcome drift")

    old_window_rows = sum(_window_values(row) is not None for row in old_members)
    b_window_rows = sum(_window_values(row) is not None for row in b_members)
    b_exact_label_rows = len(b_member_labels)

    tuning = dict(v2_autopsy.get("tuning_population") or {})
    cohorts = dict(tuning.get("cohorts") or {})
    if int(tuning.get("candidate_count") or 0) != 183 or int(tuning.get("validation_productive_count") or 0) != 50:
        raise RuntimeError("V2 autopsy tuning population drift")
    if cohorts != {"D1_CONTINUATION_B": 63, "D1_FRESH": 64, "SUCCESSOR_D1": 56}:
        raise RuntimeError("V2 autopsy cohort composition drift")
    if tuple(v2_filter.get("development_window_ids") or ()) != EXPECTED_WINDOWS:
        raise RuntimeError("V2 filter development window contract drift")
    if v2_filter.get("status") != "FROZEN_BEFORE_C_DEVELOPMENT_AND_VALIDATION":
        raise RuntimeError("V2 filter freeze status drift")

    recovered = _load_recovered_evidence(
        root,
        expected_exacts=old_ids.union({str(row["exact_identity"]) for row in b_members}),
        expected_b_exacts={str(row["exact_identity"]) for row in b_members},
        v2_filter=v2_filter,
        v2_autopsy=v2_autopsy,
    )

    missing: list[dict[str, Any]] = []
    if old_window_rows != len(old_members):
        missing.append({
            "requirement": "OLD_120_CHRONOLOGICAL_DEVELOPMENT_WINDOWS",
            "available_rows": old_window_rows,
            "required_rows": len(old_members),
            "missing_rows": len(old_members) - old_window_rows,
            "reason": "durable old candidate members contain summary statistics but not ordered development_1/2/3 increments",
        })
    if b_window_rows != len(b_members):
        missing.append({
            "requirement": "B_63_CHRONOLOGICAL_DEVELOPMENT_WINDOWS",
            "available_rows": b_window_rows,
            "required_rows": len(b_members),
            "missing_rows": len(b_members) - b_window_rows,
            "reason": "durable B candidate members do not contain ordered development_1/2/3 increments",
        })
    if b_exact_label_rows != len(b_members):
        missing.append({
            "requirement": "B_63_PER_EXACT_VALIDATION_LABELS",
            "available_rows": b_exact_label_rows,
            "required_rows": len(b_members),
            "missing_rows": len(b_members) - b_exact_label_rows,
            "aggregate_positive_count_available": b_productive,
            "reason": "B outcome freezes only aggregate productive count; per-exact validation_productive labels are not durably frozen",
        })

    if recovered is not None:
        status = "PASS_V2_REPRODUCIBILITY_RESTORED_FROM_DURABLE_EVIDENCE"
    else:
        status = "PASS_V2_REPRODUCIBLE_FROM_DURABLE_EVIDENCE" if not missing else "FAIL_CLOSED_V2_REPRODUCIBILITY_EVIDENCE_GAP"
    report: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_d1_transfer_v2_reproducibility_audit_v1",
        "status": status,
        "purpose": "Determine whether the frozen 183-row Transfer Filter V2 derivation can be independently rebuilt from durable Git evidence without reopening financial datasets.",
        "durable_sources": {
            "old_120_members": {"relative_path": OLD_MEMBERS_REL.as_posix(), "file_sha256": _sha256(old_members_path), "count": len(old_members)},
            "old_120_validation_labels": {"relative_path": OLD_SURVIVORS_REL.as_posix(), "file_sha256": _sha256(old_survivors_path), "label_count": len(old_labels), "positive_count": len(survivor_ids)},
            "B_63_members": {"relative_path": B_MEMBERS_REL.as_posix(), "file_sha256": _sha256(b_members_path), "count": len(b_members)},
            "B_validation_outcome": {"relative_path": B_OUTCOME_REL.as_posix(), "file_sha256": _sha256(b_outcome_path), "aggregate_positive_count": b_productive},
            "V2_autopsy": {"relative_path": V2_AUTOPSY_REL.as_posix(), "file_sha256": _sha256(v2_autopsy_path), "payload_sha256": v2_autopsy["autopsy_payload_sha256"]},
            "V2_filter": {"relative_path": V2_FILTER_REL.as_posix(), "file_sha256": _sha256(v2_filter_path), "payload_sha256": v2_filter["filter_payload_sha256"]},
        },
        "rebuild_requirements": {
            "candidate_count": 183,
            "validation_productive_count": 50,
            "features": ["dev_matched_return", "dev_matched_reward", "dev_window_1_increment", "dev_window_2_increment", "dev_window_3_increment"],
            "development_window_ids": list(EXPECTED_WINDOWS),
            "per_exact_validation_label_required": True,
        },
        "recovery_tools": {
            "freeze_development_windows": "scripts/freeze_cn_program_optimizer_d1_transfer_development_window_features_v2.py",
            "freeze_validation_labels": "scripts/freeze_cn_program_optimizer_d1_transfer_validation_labels_v1.py",
            "build_183_row_dataset": "scripts/build_cn_program_optimizer_d1_transfer_v2_repro_dataset.py",
            "check_model_and_LOCO": "scripts/check_cn_program_optimizer_d1_transfer_v2_model_reproduction.py",
        },
        "durable_coverage": {
            "old_120_validation_label_rows": len(old_labels),
            "old_120_window_feature_rows": old_window_rows,
            "B_63_validation_label_rows": b_exact_label_rows,
            "B_63_window_feature_rows": b_window_rows,
        },
        "historical_missing_requirements_before_recovery": missing if recovered is not None else [],
        "missing_requirements": [] if recovered is not None else missing,
        "recovered_durable_evidence": recovered,
        "frozen_V2_claims_preserved_but_not_rederived": bool(missing) and recovered is None,
        "C_development_execution_authorized_by_this_audit": False,
        "C_validation_execution_authorized_by_this_audit": False,
        "promotion_authorized": False,
        "optimizer_feedback_write": "FORBIDDEN",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "next_action": (
            "KEEP_V2_FROZEN_AND_USE_ONLY_SEPARATELY_FROZEN_PROSPECTIVE_C_MEMBERSHIP; DO_NOT READ C VALIDATION FROM THIS AUDIT"
            if recovered is not None
            else (
                "RECOVER_HASH_BOUND_LEGACY_DEVELOPMENT_WINDOW_FEATURES_AND_B_PER_EXACT_VALIDATION_LABELS_THEN_REBUILD_V2"
                if missing
                else "INDEPENDENTLY_REBUILD_AND_COMPARE_V2_MODEL_AND_LOCO_METRICS"
            )
        ),
    }
    report["audit_payload_sha256"] = stable_hash(report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = build_audit(args.repo_root)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["status"].startswith("PASS_") else 2


if __name__ == "__main__":
    raise SystemExit(main())
