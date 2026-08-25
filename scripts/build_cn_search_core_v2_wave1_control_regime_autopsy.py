"""Diagnose weak-control selection bias and Wave1 validation regime inversion.

Consumes only already-frozen DEVELOPMENT pair evidence and already-consumed
report-only validation pair records.  This is diagnostic evidence only: it does
not evaluate candidates, read holdout/forward data, tune a selector, or change
Search Core policy.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_WAVE1_WEAK_CONTROL_SELECTION_BIAS_WITH_VALIDATION1_BASE_CONTROL_REGIME_INVERSION"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _average_ranks(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: (float(values[index]), index))
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i + 1
        while j < len(order) and float(values[order[j]]) == float(values[order[i]]):
            j += 1
        average = ((i + 1) + j) / 2.0
        for index in order[i:j]:
            ranks[index] = average
        i = j
    return ranks


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    lm = sum(map(float, left)) / len(left)
    rm = sum(map(float, right)) / len(right)
    ld = [float(value) - lm for value in left]
    rd = [float(value) - rm for value in right]
    denom = math.sqrt(sum(value * value for value in ld) * sum(value * value for value in rd))
    return sum(a * b for a, b in zip(ld, rd, strict=True)) / denom if denom else None


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    return _pearson(_average_ranks(left), _average_ranks(right))


def _annualize(total_return: float, sessions: int) -> float:
    if int(sessions) <= 0 or float(total_return) <= -1.0:
        raise RuntimeError("invalid return/session for annualization")
    return math.expm1(math.log1p(float(total_return)) * 252.0 / int(sessions))


def _summary(values: Sequence[float]) -> dict[str, Any]:
    vals = list(map(float, values))
    return {
        "n": len(vals),
        "mean": sum(vals) / len(vals),
        "median": statistics.median(vals),
        "positive_count": sum(value > 0.0 for value in vals),
        "positive_rate": sum(value > 0.0 for value in vals) / len(vals),
    }


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    rp = repo / "runtime/run_plans"
    dev_path = rp / "cn_search_core_v2_wave1_development_pair_windows_shortlist42_20260825.json"
    shortlist_path = rp / "cn_search_core_v2_production_wave1_validation_shortlist_freeze_20260825.json"
    validation_audit_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_postrun_audit_20260825.json"
    validation_pair_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_pair_records_0438beb_20260825.jsonl"
    v3_dataset_path = rp / "cn_search_core_v2_transfer_v3_diagnostic_dataset_20260825.json"
    for path in (dev_path, shortlist_path, validation_audit_path, validation_pair_path, v3_dataset_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    dev = _read(dev_path)
    dev_hash = _verify(dev, "evidence_payload_sha256", "Wave1 development pair windows")
    shortlist = _read(shortlist_path)
    shortlist_hash = _verify(shortlist, "freeze_payload_sha256", "Wave1 validation shortlist")
    validation_audit = _read(validation_audit_path)
    validation_audit_hash = _verify(validation_audit, "audit_payload_sha256", "Wave1 validation postrun audit")
    v3 = _read(v3_dataset_path)
    v3_hash = _verify(v3, "dataset_payload_sha256", "Transfer V3 diagnostic dataset")
    validation_pairs = _read_jsonl(validation_pair_path)

    if (
        dev.get("status") != "RECOVERED_FROM_IMMUTABLE_DEVELOPMENT_RESULTS_NO_FINANCIAL_READ"
        or int(dev.get("candidate_count") or 0) != 42
        or dev.get("targeted_shortlist") is not True
        or dev.get("skipped_no_pair_count") is None
        or int(dev["skipped_no_pair_count"]) != 0
        or bool(dev["research_boundaries"]["financial_evaluation_performed"])
        or bool(dev["research_boundaries"]["validation_read"])
    ):
        raise RuntimeError("Wave1 development pair evidence contract drift")
    if (
        shortlist.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_SHORTLIST_FROZEN_BEFORE_VALIDATION"
        or int(shortlist.get("candidate_count") or 0) != 42
        or shortlist.get("membership_frozen_before_validation") is not True
        or bool(shortlist.get("validation_read"))
    ):
        raise RuntimeError("Wave1 shortlist authority drift")
    if validation_audit.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_POSTRUN_AUDIT_COMPLETE_TRANSFER_FAILED_NO_HOLDOUT":
        raise RuntimeError("Wave1 validation audit status drift")
    if validation_audit["project_control_recommendation"].get("holdout_action") != "DO_NOT_READ":
        raise RuntimeError("Wave1 validation holdout boundary drift")
    if v3.get("status") != "SEARCH_CORE_V2_TRANSFER_V3_DIAGNOSTIC_DATASET_FROZEN_CONSUMED_VALIDATION_ONLY":
        raise RuntimeError("Transfer V3 dataset status drift")
    if len(validation_pairs) != 42:
        raise RuntimeError("Wave1 validation pair cardinality drift")

    dev_rows = {str(row["exact_identity"]): dict(row) for row in dev["rows"]}
    validation_rows: dict[str, dict[str, Any]] = {}
    for row in validation_pairs:
        pair_hash = _verify(row, "record_payload_sha256", "Wave1 validation pair")
        payload = dict(row)
        payload["record_payload_sha256"] = pair_hash
        validation_rows[str(row["exact_identity"])] = payload
    shortlist_exacts = set(map(str, shortlist["candidate_exact_identities"]))
    v3_rows = {
        str(row["exact_identity"]): dict(row)
        for row in v3["rows"]
        if str(row["source_cohort"]) == "SEARCH_CORE_V2_WAVE1"
    }
    if not (
        len(dev_rows) == len(validation_rows) == len(shortlist_exacts) == len(v3_rows) == 42
        and set(dev_rows) == set(validation_rows) == shortlist_exacts == set(v3_rows)
    ):
        raise RuntimeError("Wave1 autopsy exact-set drift")

    records: list[dict[str, Any]] = []
    for exact in sorted(shortlist_exacts):
        development = dev_rows[exact]
        validation = validation_rows[exact]
        frozen = v3_rows[exact]
        if not (
            str(development["template_id"])
            == str(validation["template_id"])
            == str(frozen["template_id"])
        ):
            raise RuntimeError("Wave1 template identity drift")
        if str(development["control_program_id"]) != str(validation["control_program_id"]):
            raise RuntimeError("Wave1 control program identity drift")
        primary_windows = {
            str(row["window_id"]): float(row["cumulative_net_return"])
            for row in validation["primary"]["development_subwindows"]
        }
        control_windows = {
            str(row["window_id"]): float(row["cumulative_net_return"])
            for row in validation["base_control"]["development_subwindows"]
        }
        validation_ids = ("validation_1", "validation_2", "validation_3")
        if set(primary_windows) != set(validation_ids) or set(control_windows) != set(validation_ids):
            raise RuntimeError("Wave1 validation window identity drift")
        dev_primary = list(map(float, development["primary_window_returns"]))
        dev_control = list(map(float, development["control_window_returns"]))
        dev_match = list(map(float, development["matched_window_return_increments"]))
        dev_sessions = list(map(int, development["session_counts"]))
        if len(dev_primary) != 3 or len(dev_control) != 3 or len(dev_match) != 3 or len(dev_sessions) != 3:
            raise RuntimeError("Wave1 development window geometry drift")
        val_primary = [primary_windows[window_id] for window_id in validation_ids]
        val_control = [control_windows[window_id] for window_id in validation_ids]
        val_match = [a - b for a, b in zip(val_primary, val_control, strict=True)]
        records.append(
            {
                "exact_identity": exact,
                "template_id": str(validation["template_id"]),
                "control_program_id": str(validation["control_program_id"]),
                "development_matched_return": float(frozen["dev_matched_return"]),
                "development_primary_total_return": float(development["primary_cumulative_net_return"]),
                "development_control_total_return": float(development["control_cumulative_net_return"]),
                "development_primary_window_returns": dev_primary,
                "development_control_window_returns": dev_control,
                "development_matched_window_increments": dev_match,
                "development_session_counts": dev_sessions,
                "validation_primary_window_returns": val_primary,
                "validation_control_window_returns": val_control,
                "validation_matched_window_increments": val_match,
                "validation_session_counts": [24, 24, 25],
                "source_development_record_payload_sha256": str(development["source_record_payload_sha256"]),
                "source_validation_pair_payload_sha256": str(validation["record_payload_sha256"]),
            }
        )

    control_ids = {row["control_program_id"] for row in records}
    if len(control_ids) != 42:
        raise RuntimeError("Wave1 control-program duplication confound")

    dev_uplift = [row["development_matched_return"] for row in records]
    decomposition = {
        "development_matched_vs_development_primary_total_spearman": _spearman(
            dev_uplift, [row["development_primary_total_return"] for row in records]
        ),
        "development_matched_vs_development_control_total_spearman": _spearman(
            dev_uplift, [row["development_control_total_return"] for row in records]
        ),
        "development_matched_vs_validation1_primary_spearman": _spearman(
            dev_uplift, [row["validation_primary_window_returns"][0] for row in records]
        ),
        "development_matched_vs_validation1_control_spearman": _spearman(
            dev_uplift, [row["validation_control_window_returns"][0] for row in records]
        ),
        "development_matched_vs_validation1_matched_spearman": _spearman(
            dev_uplift, [row["validation_matched_window_increments"][0] for row in records]
        ),
    }

    raw_windows: dict[str, Any] = {}
    annualized_windows: dict[str, Any] = {}
    for phase, session_key in (("development", "development_session_counts"), ("validation", "validation_session_counts")):
        for side in ("primary", "control"):
            source_key = f"{phase}_{side}_window_returns"
            for index in range(3):
                raw = [row[source_key][index] for row in records]
                annualized = [
                    _annualize(row[source_key][index], row[session_key][index]) for row in records
                ]
                raw_windows[f"{phase}_{side}_{index + 1}"] = _summary(raw)
                annualized_windows[f"{phase}_{side}_{index + 1}"] = _summary(annualized)

    validation_matched_windows = {
        f"validation_{index + 1}": _summary(
            [row["validation_matched_window_increments"][index] for row in records]
        )
        for index in range(3)
    }
    development_to_validation_spearman = [
        [
            _spearman(
                [row["development_matched_window_increments"][dev_index] for row in records],
                [row["validation_matched_window_increments"][val_index] for row in records],
            )
            for val_index in range(3)
        ]
        for dev_index in range(3)
    ]
    validation_cross_spearman = [
        [
            _spearman(
                [row["validation_matched_window_increments"][left] for row in records],
                [row["validation_matched_window_increments"][right] for row in records],
            )
            for right in range(3)
        ]
        for left in range(3)
    ]

    validation1_control_ann = [
        _annualize(row["validation_control_window_returns"][0], row["validation_session_counts"][0])
        for row in records
    ]
    control_transition: dict[str, Any] = {}
    for index in range(3):
        development_control_ann = [
            _annualize(row["development_control_window_returns"][index], row["development_session_counts"][index])
            for row in records
        ]
        deltas = [
            validation_value - development_value
            for validation_value, development_value in zip(
                validation1_control_ann, development_control_ann, strict=True
            )
        ]
        control_transition[f"validation1_minus_development{index + 1}_control_annualized"] = {
            "median_delta": statistics.median(deltas),
            "positive_delta_count": sum(delta > 0.0 for delta in deltas),
            "candidate_count": 42,
            "spearman_development_control_vs_validation1_control": _spearman(
                development_control_ann, validation1_control_ann
            ),
        }

    per_template: dict[str, Any] = {}
    for template in sorted({row["template_id"] for row in records}):
        group = [row for row in records if row["template_id"] == template]
        per_template[template] = {
            "candidate_count": len(group),
            "development_matched_return": _summary(
                [row["development_matched_return"] for row in group]
            ),
            "validation1_primary_return": _summary(
                [row["validation_primary_window_returns"][0] for row in group]
            ),
            "validation1_control_return": _summary(
                [row["validation_control_window_returns"][0] for row in group]
            ),
            "validation1_matched_increment": _summary(
                [row["validation_matched_window_increments"][0] for row in group]
            ),
        }

    classification_checks = {
        "development_uplift_more_control_driven_than_primary": (
            abs(float(decomposition["development_matched_vs_development_control_total_spearman"] or 0.0))
            > abs(float(decomposition["development_matched_vs_development_primary_total_spearman"] or 0.0))
        ),
        "development_uplift_negatively_transfers_to_validation1": float(
            decomposition["development_matched_vs_validation1_matched_spearman"] or 0.0
        ) < -0.5,
        "validation1_control_dominates_matched_pair": validation_matched_windows["validation_1"]["positive_rate"] < 0.10,
        "validation1_primary_mostly_positive": raw_windows["validation_primary_1"]["positive_rate"] > 0.80,
        "validation1_control_mostly_positive": raw_windows["validation_control_1"]["positive_rate"] > 0.90,
        "control_regime_shift_broad_across_candidates": all(
            int(value["positive_delta_count"]) >= 38 for value in control_transition.values()
        ),
        "control_programs_unique": len(control_ids) == 42,
    }
    if not all(classification_checks.values()):
        raise RuntimeError(f"Wave1 control-regime classification checks failed: {classification_checks}")

    payload = {
        "schema_version": "cn_search_core_v2_wave1_control_regime_autopsy_v1",
        "status": STATUS,
        "source_authority": {
            "development_pair_evidence_file_sha256": _sha(dev_path),
            "development_pair_evidence_payload_sha256": dev_hash,
            "validation_shortlist_file_sha256": _sha(shortlist_path),
            "validation_shortlist_payload_sha256": shortlist_hash,
            "validation_postrun_audit_file_sha256": _sha(validation_audit_path),
            "validation_postrun_audit_payload_sha256": validation_audit_hash,
            "validation_pair_records_file_sha256": _sha(validation_pair_path),
            "transfer_v3_dataset_file_sha256": _sha(v3_dataset_path),
            "transfer_v3_dataset_payload_sha256": v3_hash,
        },
        "candidate_count": 42,
        "distinct_control_program_count": len(control_ids),
        "decomposition_spearman": decomposition,
        "development_to_validation_matched_window_spearman": development_to_validation_spearman,
        "validation_matched_cross_window_spearman": validation_cross_spearman,
        "raw_window_summaries": raw_windows,
        "annualized_window_summaries": annualized_windows,
        "validation_matched_window_summaries": validation_matched_windows,
        "control_regime_transition": control_transition,
        "per_template_validation1": per_template,
        "classification_checks": classification_checks,
        "interpretation": {
            "primary_failure": "DEVELOPMENT_MATCHED_UPLIFT_IS_DOMINATED_BY_WEAK_CONTROL_SELECTION_MORE_THAN_PRIMARY_STRENGTH",
            "validation1_failure": "SAME_HASH_BOUND_BASE_CONTROLS_BECOME_SYSTEMICALLY_STRONG_AND_REVERSE_MATCHED_UPLIFT",
            "selector_implication": "DO_NOT_TREAT_MATCHED_UPLIFT_ALONE_AS_TRANSFERABLE_ALPHA_STRENGTH",
            "next_diagnostic": "PAIR_NATIVE_TRANSFER_FEATURES_SEPARATING_PRIMARY_ABSOLUTE_STRENGTH_FROM_CONTROL_WEAKNESS",
        },
        "research_boundaries": {
            "financial_evaluation_executed": False,
            "consumed_validation_results_used": True,
            "validation_domain_already_consumed": True,
            "holdout_read": False,
            "forward_read": False,
            "prospective_claim_authorized": False,
            "model_adoption_authorized": False,
            "search_policy_change_authorized": False,
            "promotion_authorized": False,
        },
    }
    payload["autopsy_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_search_core_v2_wave1_control_regime_autopsy_20260825.json"),
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
                "payload": payload["autopsy_payload_sha256"],
                "candidate_count": payload["candidate_count"],
                "control_count": payload["distinct_control_program_count"],
                "decomposition": payload["decomposition_spearman"],
                "validation1": payload["validation_matched_window_summaries"]["validation_1"],
                "output": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
