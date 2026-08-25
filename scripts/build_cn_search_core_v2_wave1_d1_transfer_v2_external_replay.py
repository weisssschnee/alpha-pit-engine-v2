"""Replay the pre-frozen D1 Transfer V2 model on Wave1 without new financial reads.

The model/filter was frozen on 2026-08-17, before Production Wave 1.  This
script scores the full 175-member Wave1 development-productive population using
only development evidence, applies the original top-40% contract, and reports
how that frozen ranking aligns with the already-consumed 42-member Wave1
validation shortlist.  It is diagnostic/external replay only: the other 52
members of the top-70 are not newly evaluated on validation here.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts.apply_cn_program_optimizer_d1_transfer_filter_v2 import (
    EXPECTED_FEATURES,
    FILTER_ID,
    _score,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_WAVE1_D1_TRANSFER_V2_EXTERNAL_REPLAY_COMPLETE_WEAK_PRODUCTIVITY_NO_STABILITY"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _auc(rows: Sequence[Mapping[str, Any]], label: str) -> float | None:
    positive = [float(row["score"]) for row in rows if bool(row[label])]
    negative = [float(row["score"]) for row in rows if not bool(row[label])]
    if not positive or not negative:
        return None
    wins = 0.0
    for p in positive:
        for n in negative:
            wins += 1.0 if p > n else 0.5 if p == n else 0.0
    return wins / (len(positive) * len(negative))


def _average_precision(rows: Sequence[Mapping[str, Any]], label: str) -> float | None:
    ordered = sorted(rows, key=lambda row: (-float(row["score"]), str(row["exact_identity"])))
    total_positive = sum(bool(row[label]) for row in ordered)
    if total_positive == 0:
        return None
    hits = 0
    precision_sum = 0.0
    for index, row in enumerate(ordered, 1):
        if bool(row[label]):
            hits += 1
            precision_sum += hits / index
    return precision_sum / total_positive


def _features(row: Mapping[str, Any]) -> dict[str, float]:
    admission = dict(row["admission"])
    uplift = dict(row["uplift"])
    credit = dict(uplift["program_credit"])
    ids = list(admission["metrics"]["development_window_ids"])
    windows = list(map(float, credit["window_return_increments"]))
    if ids != ["development_1", "development_2", "development_3"] or len(windows) != 3:
        raise RuntimeError("Wave1 development window contract drift")
    values = {
        "dev_matched_return": float(credit["matched_cumulative_net_return_increment"]),
        "dev_matched_reward": float(credit["matched_net_reward_increment"]),
        "dev_window_1_increment": windows[0],
        "dev_window_2_increment": windows[1],
        "dev_window_3_increment": windows[2],
    }
    if tuple(values) != EXPECTED_FEATURES:
        raise RuntimeError("D1 V2 feature order drift")
    return values


def _metric(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    productive = sum(bool(row["productive"]) for row in rows)
    stable = sum(bool(row["stable2"]) for row in rows)
    return {
        "n": n,
        "productive": productive,
        "productive_rate": productive / n if n else None,
        "stable_2of3": stable,
        "stable_2of3_rate": stable / n if n else None,
    }


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    rp = repo / "runtime/run_plans"
    filter_path = rp / "cn_program_optimizer_d1_transfer_filter_v2_20260817.json"
    productive_path = rp / "cn_search_core_v2_production_wave1_productive_candidates_412d51c_20260825.jsonl"
    validation_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_results_0438beb_20260825.jsonl"
    audit_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_postrun_audit_20260825.json"
    flt = _read(filter_path)
    filter_hash = _verify(flt, "filter_payload_sha256", "D1 Transfer V2 filter")
    audit = _read(audit_path)
    audit_hash = _verify(audit, "audit_payload_sha256", "Wave1 validation audit")
    if (
        flt.get("filter_id") != FILTER_ID
        or flt.get("status") != "FROZEN_BEFORE_C_DEVELOPMENT_AND_VALIDATION"
        or flt.get("rule_change_after_C_development_or_validation") != "FORBIDDEN"
        or audit.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_POSTRUN_AUDIT_COMPLETE_TRANSFER_FAILED_NO_HOLDOUT"
    ):
        raise RuntimeError("External replay source authority drift")
    development = _read_jsonl(productive_path)
    validation = _read_jsonl(validation_path)
    if len(development) != 175 or len(validation) != 42:
        raise RuntimeError("External replay population drift")
    scored: list[dict[str, Any]] = []
    for row in development:
        features = _features(row)
        scored.append(
            {
                "exact_identity": str(row["exact_identity"]),
                "template_id": str(row["template_id"]),
                "score": float(_score(features, flt)),
                "features": features,
            }
        )
    scored.sort(key=lambda row: (-float(row["score"]), str(row["exact_identity"])))
    fraction = float(flt["selection_contract"]["selected_fraction"])
    selected_count = int(math.ceil(fraction * len(scored)))
    selected = {str(row["exact_identity"]) for row in scored[:selected_count]}
    rank = {str(row["exact_identity"]): index + 1 for index, row in enumerate(scored)}
    score_by_exact = {str(row["exact_identity"]): float(row["score"]) for row in scored}
    tested = []
    for result in validation:
        exact = str(result["exact_identity"])
        tested.append(
            {
                "exact_identity": exact,
                "template_id": str(result["template_id"]),
                "score": score_by_exact[exact],
                "rank_in_175": rank[exact],
                "selected_by_frozen_v2": exact in selected,
                "productive": bool(result["validation_productive"]),
                "stable2": bool(result["validation_stable_2of3"]),
            }
        )
    selected_tested = [row for row in tested if row["selected_by_frozen_v2"]]
    not_selected_tested = [row for row in tested if not row["selected_by_frozen_v2"]]
    all_metric = _metric(tested)
    selected_metric = _metric(selected_tested)
    nonselected_metric = _metric(not_selected_tested)
    productive_total = int(all_metric["productive"])
    stable_total = int(all_metric["stable_2of3"])
    original_acceptance = dict(flt["prospective_C_validation_acceptance"])
    payload = {
        "schema_version": "cn_search_core_v2_wave1_d1_transfer_v2_external_replay_v1",
        "status": STATUS,
        "filter_id": FILTER_ID,
        "filter_payload_sha256": filter_hash,
        "filter_was_frozen_before_wave1": True,
        "wave1_validation_audit_payload_sha256": audit_hash,
        "wave1_development_productive_population": len(scored),
        "selected_fraction_contract": fraction,
        "selected_count_contract": selected_count,
        "selected_exact_identities_sha256": stable_hash([row["exact_identity"] for row in scored[:selected_count]]),
        "already_validated_shortlist_count": len(tested),
        "validated_selected_intersection": selected_metric,
        "validated_not_selected_intersection": nonselected_metric,
        "validated_all": all_metric,
        "productive_precision_lift_vs_validated_all": float(selected_metric["productive_rate"]) - float(all_metric["productive_rate"]),
        "productive_precision_lift_vs_validated_nonselected": float(selected_metric["productive_rate"]) - float(nonselected_metric["productive_rate"]),
        "productive_recall_within_validated_shortlist": int(selected_metric["productive"]) / productive_total if productive_total else None,
        "stable2_recall_within_validated_shortlist": int(selected_metric["stable_2of3"]) / stable_total if stable_total else None,
        "productive_auc_on_validated_shortlist": _auc(tested, "productive"),
        "productive_average_precision_on_validated_shortlist": _average_precision(tested, "productive"),
        "stable2_auc_on_validated_shortlist": _auc(tested, "stable2"),
        "stable2_average_precision_on_validated_shortlist": _average_precision(tested, "stable2"),
        "original_v2_acceptance_contract": original_acceptance,
        "formal_v2_acceptance_test": False,
        "formal_test_blocker": "ONLY_42_OF_175_WAVE1_PRODUCTIVE_PROGRAMS_HAVE_FROZEN_VALIDATION_RESULTS;_DO_NOT_EXPAND_SAME_VALIDATION_AFTER_OBSERVING_RESULTS",
        "diagnostic_interpretation": "WEAK_PRODUCTIVITY_ENRICHMENT_BUT_NO_STABILITY_ENRICHMENT",
        "new_financial_reads": 0,
        "holdout_reads": 0,
        "forward_reads": 0,
        "automatic_filter_adoption_authorized": False,
        "promotion_authorized": False,
    }
    payload["replay_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, default=Path("runtime/run_plans/cn_search_core_v2_wave1_d1_transfer_v2_external_replay_20260825.json"))
    args = parser.parse_args(argv)
    payload = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "payload": payload["replay_payload_sha256"], "selected_intersection": payload["validated_selected_intersection"], "productive_auc": payload["productive_auc_on_validated_shortlist"], "stable2_auc": payload["stable2_auc_on_validated_shortlist"], "output": str(output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
