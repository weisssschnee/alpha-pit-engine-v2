"""Diagnostic-only autopsy of Wave1 development-to-validation transfer.

Consumes only already-frozen DEVELOPMENT evidence plus the already-consumed
Wave1 report-only validation result.  It does not evaluate candidates, tune a
production selector, authorize holdout/forward reads, or make a prospective
qualification claim.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_WAVE1_VALIDATION_TRANSFER_AUTOPSY_COMPLETE_DIAGNOSTIC_ONLY"
DEV_FEATURES = (
    "matched_cumulative_net_return_increment",
    "matched_net_reward_increment",
    "cross_window_positive_increment_count",
    "cross_window_matched_consistency",
    "robust_median_window_return_increment",
    "lower_tail_window_return_increment",
    "turnover_differential",
)


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


def _mean(values: Sequence[float]) -> float | None:
    return statistics.mean(values) if values else None


def _median(values: Sequence[float]) -> float | None:
    return statistics.median(values) if values else None


def _rankdata(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i + 1
        while j < len(order) and values[order[j]] == values[order[i]]:
            j += 1
        rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            ranks[order[k]] = rank
        i = j
    return ranks


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    lm = statistics.mean(left)
    rm = statistics.mean(right)
    ld = [x - lm for x in left]
    rd = [y - rm for y in right]
    denom = math.sqrt(sum(x * x for x in ld) * sum(y * y for y in rd))
    return sum(x * y for x, y in zip(ld, rd, strict=True)) / denom if denom else None


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    return _pearson(_rankdata(left), _rankdata(right))


def _feature_diagnostic(rows: Sequence[Mapping[str, Any]], feature: str) -> dict[str, Any]:
    values = [float(row["development_credit"][feature]) for row in rows]
    val_increment = [float(row["validation_matched_return_increment"]) for row in rows]
    productive = [1.0 if bool(row["validation_productive"]) else 0.0 for row in rows]
    stable = [1.0 if bool(row["validation_stable_2of3"]) else 0.0 for row in rows]
    prod_values = [value for value, row in zip(values, rows, strict=True) if bool(row["validation_productive"])]
    nonprod_values = [value for value, row in zip(values, rows, strict=True) if not bool(row["validation_productive"])]
    ordered = sorted(zip(values, rows, strict=True), key=lambda pair: pair[0], reverse=(feature != "turnover_differential"))
    quartile = max(1, len(rows) // 4)
    top = [row for _, row in ordered[:quartile]]
    bottom = [row for _, row in ordered[-quartile:]]
    return {
        "unique_value_count": len(set(values)),
        "min": min(values),
        "median": _median(values),
        "max": max(values),
        "spearman_vs_validation_matched_return_increment": _spearman(values, val_increment),
        "spearman_vs_validation_productive": _spearman(values, productive),
        "spearman_vs_validation_stable_2of3": _spearman(values, stable),
        "productive_mean": _mean(prod_values),
        "nonproductive_mean": _mean(nonprod_values),
        "top_quartile_productive_count": sum(bool(row["validation_productive"]) for row in top),
        "top_quartile_stable_2of3_count": sum(bool(row["validation_stable_2of3"]) for row in top),
        "bottom_quartile_productive_count": sum(bool(row["validation_productive"]) for row in bottom),
        "bottom_quartile_stable_2of3_count": sum(bool(row["validation_stable_2of3"]) for row in bottom),
    }


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    rp = repo / "runtime/run_plans"
    audit_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_postrun_audit_20260825.json"
    results_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_results_0438beb_20260825.jsonl"
    pairs_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_pair_records_0438beb_20260825.jsonl"
    development_path = rp / "cn_search_core_v2_production_wave1_validation_shortlist_members_20260825.jsonl"
    audit = _read(audit_path)
    audit_hash = _verify(audit, "audit_payload_sha256", "Wave1 validation postrun audit")
    if audit.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_POSTRUN_AUDIT_COMPLETE_TRANSFER_FAILED_NO_HOLDOUT":
        raise RuntimeError("Wave1 validation audit is not transfer-failed terminal evidence")
    results = _read_jsonl(results_path)
    pairs = _read_jsonl(pairs_path)
    development = _read_jsonl(development_path)
    if len(results) != 42 or len(pairs) != 42 or len(development) != 42:
        raise RuntimeError("Wave1 transfer autopsy cardinality drift")
    result_by_exact = {str(row["exact_identity"]): row for row in results}
    pair_by_exact = {str(row["exact_identity"]): row for row in pairs}
    rows: list[dict[str, Any]] = []
    for member in development:
        exact = str(member["exact_identity"])
        result = result_by_exact[exact]
        pair = pair_by_exact[exact]
        rows.append(
            {
                "exact_identity": exact,
                "template_id": str(member["template_id"]),
                "development_rank": int(member["development_rank"]),
                "development_credit": dict(member["development_credit"]),
                "validation_admitted": bool(result["admission"]["admitted"]),
                "validation_productive": bool(result["validation_productive"]),
                "validation_stable_2of3": bool(result["validation_stable_2of3"]),
                "validation_stable_3of3": bool(result["validation_stable_3of3"]),
                "validation_matched_return_increment": float(pair["matched_cumulative_return_increment"]),
                "validation_matched_reward_increment": float(pair["matched_net_reward_increment"]),
                "validation_primary_return": float(pair["primary"]["cumulative_net_return"]),
                "validation_control_return": float(pair["base_control"]["cumulative_net_return"]),
            }
        )
    feature_diagnostics = {feature: _feature_diagnostic(rows, feature) for feature in DEV_FEATURES}
    ranked_by_absolute_signal = sorted(
        (
            (feature, diagnostics["spearman_vs_validation_matched_return_increment"])
            for feature, diagnostics in feature_diagnostics.items()
            if diagnostics["spearman_vs_validation_matched_return_increment"] is not None
        ),
        key=lambda item: abs(float(item[1])),
        reverse=True,
    )
    template_diagnostics = {}
    for template in sorted({str(row["template_id"]) for row in rows}):
        group = [row for row in rows if str(row["template_id"]) == template]
        template_diagnostics[template] = {
            "count": len(group),
            "admitted": sum(bool(row["validation_admitted"]) for row in group),
            "productive": sum(bool(row["validation_productive"]) for row in group),
            "stable_2of3": sum(bool(row["validation_stable_2of3"]) for row in group),
            "stable_3of3": sum(bool(row["validation_stable_3of3"]) for row in group),
            "median_validation_matched_return_increment": _median([float(row["validation_matched_return_increment"]) for row in group]),
        }
    payload = {
        "schema_version": "cn_search_core_v2_wave1_validation_transfer_autopsy_v1",
        "status": STATUS,
        "source_validation_audit_payload_sha256": audit_hash,
        "candidate_count": 42,
        "feature_diagnostics": feature_diagnostics,
        "features_ranked_by_abs_spearman_to_validation_increment": [
            {"feature": feature, "spearman": value} for feature, value in ranked_by_absolute_signal
        ],
        "template_diagnostics": template_diagnostics,
        "observed_transfer": {
            "productive": 8,
            "stable_2of3": 5,
            "stable_3of3": 0,
            "development_stable_input_count": 42,
            "development_stable_to_validation_stable_2of3_rate": 5 / 42,
        },
        "diagnosis": {
            "development_stability_filter_sufficient": False,
            "development_priority_rank_transfer_predictive": False,
            "search_policy_development_efficiency_invalidated": False,
            "search_policy_role": "RETAIN_AS_DEVELOPMENT_SEARCH_ENGINE_ONLY",
            "selection_layer_status": "UNSOLVED_DEVELOPMENT_TO_VALIDATION_TRANSFER",
            "same_validation_slice_can_support_prospective_selector_claim": False,
        },
        "research_boundaries": {
            "financial_evaluation_executed": False,
            "validation_data_reused_for_diagnostic_only": True,
            "validation_domain_consumed": True,
            "holdout_read": False,
            "forward_read": False,
            "promotion_authorized": False,
            "automatic_selector_adoption_authorized": False,
            "next_action": "DESIGN_TRANSFER_LAYER_DIAGNOSTIC_OR_FROZEN_SELECTOR_REQUIRING_DISTINCT_UNSEEN_EVALUATION_DOMAIN",
        },
    }
    payload["autopsy_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, default=Path("runtime/run_plans/cn_search_core_v2_wave1_validation_transfer_autopsy_20260825.json"))
    args = parser.parse_args(argv)
    payload = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "payload": payload["autopsy_payload_sha256"], "top_features": payload["features_ranked_by_abs_spearman_to_validation_increment"][:3], "output": str(output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
