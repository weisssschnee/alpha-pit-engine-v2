"""Freeze a pair-native control-robust qualification challenger for future unseen testing.

The rule is diagnostic-only and was motivated by consumed validation evidence.
It cannot be claimed prospective on the already-consumed Wave1 validation domain.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from scripts.benchmark_cn_search_core_v2_pair_native_transfer_v4_diagnostic import _assemble
from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "PAIR_NATIVE_CONTROL_ROBUST_CHALLENGER_V1_FROZEN_DIAGNOSTIC_ONLY_UNSEEN_TEST_REQUIRED"
RULE_ID = "PAIR_NATIVE_PRIMARY_3OF3_CONTROL_2OF3_POSITIVE_V1"
WAVE = "SEARCH_CORE_V2_WAVE1"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _selected(row: Mapping[str, Any]) -> bool:
    primary_positive = sum(float(row[f"primary_w{i}"]) > 0.0 for i in (1, 2, 3))
    control_positive = sum(float(row[f"control_w{i}"]) > 0.0 for i in (1, 2, 3))
    return primary_positive == 3 and control_positive >= 2


def _metric(rows: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    productive = sum(bool(row["validation_productive"]) for row in rows)
    stable = sum(bool(row["validation_stable_2of3"]) for row in rows)
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
    benchmark_path = rp / "cn_search_core_v2_pair_native_transfer_v4_diagnostic_20260825.json"
    benchmark = _read(benchmark_path)
    benchmark_hash = _verify(benchmark, "benchmark_payload_sha256", "Transfer V4 benchmark")
    if benchmark.get("status") != "SEARCH_CORE_V2_PAIR_NATIVE_TRANSFER_V4_DIAGNOSTIC_COMPLETE_NO_MODEL_ADOPTION":
        raise RuntimeError("Transfer V4 benchmark status drift")
    rows, source_hashes = _assemble(repo)
    if len(rows) != 225:
        raise RuntimeError("pair-native challenger source geometry drift")

    cohorts = sorted({str(row["source_cohort"]) for row in rows})
    per_cohort: dict[str, Any] = {}
    for cohort in cohorts:
        cohort_rows = [row for row in rows if str(row["source_cohort"]) == cohort]
        chosen = [row for row in cohort_rows if _selected(row)]
        base = _metric(cohort_rows)
        selected = _metric(chosen)
        per_cohort[cohort] = {
            "base": base,
            "selected": selected,
            "coverage": len(chosen) / len(cohort_rows),
            "productive_lift_pp": selected["productive_rate"] - base["productive_rate"] if chosen else None,
            "stable_2of3_lift_pp": selected["stable_2of3_rate"] - base["stable_2of3_rate"] if chosen else None,
        }

    historical_rows = [row for row in rows if str(row["source_cohort"]) != WAVE]
    historical_selected = [row for row in historical_rows if _selected(row)]
    wave_rows = [row for row in rows if str(row["source_cohort"]) == WAVE]
    wave_selected = [row for row in wave_rows if _selected(row)]
    hist_base = _metric(historical_rows)
    hist_selected = _metric(historical_selected)
    wave_base = _metric(wave_rows)
    wave_sel = _metric(wave_selected)

    history_names = [name for name in cohorts if name != WAVE]
    historical_consistency = {
        "all_three_cohorts_nonempty": all(per_cohort[name]["selected"]["n"] > 0 for name in history_names),
        "all_three_productive_rate_improved": all(per_cohort[name]["productive_lift_pp"] > 0 for name in history_names),
        "all_three_stable_2of3_rate_improved": all(per_cohort[name]["stable_2of3_lift_pp"] > 0 for name in history_names),
    }
    if not all(historical_consistency.values()):
        raise RuntimeError("pair-native challenger historical consistency failed")

    payload = {
        "schema_version": "cn_search_core_v2_pair_native_qualification_challenger_v1",
        "status": STATUS,
        "rule_id": RULE_ID,
        "rule_contract": {
            "primary_positive_development_window_count_required": 3,
            "control_positive_development_window_count_minimum": 2,
            "development_window_count": 3,
            "uses_validation_features": False,
            "uses_validation_labels_at_application_time": False,
            "uses_template_identity": False,
            "uses_learned_parameters": False,
            "threshold_tuning": False,
        },
        "source_transfer_v4_benchmark_payload_sha256": benchmark_hash,
        "source_payload_sha256": source_hashes,
        "per_cohort_retrospective_diagnostics": per_cohort,
        "historical_d1_aggregate": {
            "base": hist_base,
            "selected": hist_selected,
            "coverage": len(historical_selected) / len(historical_rows),
            "productive_lift_pp": hist_selected["productive_rate"] - hist_base["productive_rate"],
            "stable_2of3_lift_pp": hist_selected["stable_2of3_rate"] - hist_base["stable_2of3_rate"],
        },
        "wave1_retrospective_external_check": {
            "base": wave_base,
            "selected": wave_sel,
            "coverage": len(wave_selected) / len(wave_rows),
            "productive_lift_pp": wave_sel["productive_rate"] - wave_base["productive_rate"] if wave_selected else None,
            "stable_2of3_lift_pp": wave_sel["stable_2of3_rate"] - wave_base["stable_2of3_rate"] if wave_selected else None,
            "sample_size_warning": "SELECTED_N_TOO_SMALL_FOR_POLICY_ADOPTION" if len(wave_selected) < 10 else None,
        },
        "historical_consistency_checks": historical_consistency,
        "authority_boundaries": {
            "current_search_policy_changed": False,
            "current_validation_shortlist_policy_changed": False,
            "automatic_challenger_adoption_authorized": False,
            "wave1_validation_domain_consumed": True,
            "wave1_validation_reuse_for_prospective_claim": False,
            "same_slice_reselection_allowed": False,
            "holdout_read": False,
            "forward_read": False,
            "promotion_authorized": False,
            "next_required_evidence": "DISTINCT_UNSEEN_EVALUATION_DOMAIN_WITH_RULE_FROZEN_BEFORE_LABEL_ACCESS",
        },
        "financial_evaluation_executed": False,
    }
    payload["challenger_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_search_core_v2_pair_native_qualification_challenger_v1_20260825.json"),
    )
    args = parser.parse_args(argv)
    payload = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": payload["status"],
        "rule_id": payload["rule_id"],
        "historical": payload["historical_d1_aggregate"],
        "wave1": payload["wave1_retrospective_external_check"],
        "payload": payload["challenger_payload_sha256"],
        "output": str(output.resolve()),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
