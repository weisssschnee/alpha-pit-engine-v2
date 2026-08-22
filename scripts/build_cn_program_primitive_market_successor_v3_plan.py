from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from our_system_phase2.services.unified_capability_registry import stable_hash

ROOT = Path(__file__).resolve().parents[1]
SUPPLY = ROOT / "runtime/run_plans/cn_program_primitive_market_successor_v3_fresh_supply_20260822.json"
V2_AUDIT = ROOT / "runtime/run_plans/cn_program_primitive_market_successor_v2_9f458e3_independent_audit_20260822.json"
V2_OUTCOME = ROOT / "runtime/run_plans/cn_program_primitive_market_successor_v2_9f458e3_outcome_20260822.json"
FOCUS_EVIDENCE = ROOT / "runtime/run_plans/cn_program_primitive_market_successor_v2_focus_redirect_evidence_20260822.json"
ACCEL = ROOT / "runtime/run_plans/cn_program_primitive_market_successor_acceleration_accuracy_audit_20260821.json"
OUT = ROOT / "runtime/run_plans/cn_program_primitive_market_successor_v3_plan.json"

P = "PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1"
U = "UNIFORM_CONTROL"
E = "CATALOG_TYPED_EVOLUTION_PROGRAM_V2"
FOCUS = ("BASE_EVENT", "BASE_TEMPORAL_MARKET_EVENT", "BASE_MARKET_EVENT")
SCHEDULE = [
    {"checkpoint": 0, "template": "BASE_EVENT", "arm": P, "role": "FOCUS_PRIMITIVE"},
    {"checkpoint": 1, "template": "BASE_TEMPORAL_MARKET_EVENT", "arm": P, "role": "FOCUS_PRIMITIVE"},
    {"checkpoint": 2, "template": "BASE_MARKET_EVENT", "arm": P, "role": "FOCUS_PRIMITIVE"},
    {"checkpoint": 3, "template": "BASE_EVENT", "arm": U, "role": "FOCUS_CONTROL"},
    {"checkpoint": 4, "template": "BASE_EVENT", "arm": E, "role": "FOCUS_CONTROL"},
    {"checkpoint": 5, "template": "BASE_TEMPORAL_MARKET_EVENT", "arm": U, "role": "FOCUS_CONTROL"},
    {"checkpoint": 6, "template": "BASE_TEMPORAL_MARKET_EVENT", "arm": E, "role": "FOCUS_CONTROL"},
    {"checkpoint": 7, "template": "BASE_MARKET_EVENT", "arm": U, "role": "FOCUS_CONTROL"},
    {"checkpoint": 8, "template": "BASE_MARKET_EVENT", "arm": E, "role": "FOCUS_CONTROL"},
    {"checkpoint": 9, "template": "BASE_EVENT", "arm": P, "role": "FOCUS_PRIMITIVE"},
    {"checkpoint": 10, "template": "BASE_TEMPORAL_MARKET_EVENT", "arm": P, "role": "FOCUS_PRIMITIVE"},
    {"checkpoint": 11, "template": "BASE_MARKET_EVENT", "arm": P, "role": "FOCUS_PRIMITIVE"},
    {"checkpoint": 12, "template": "BASE_EVENT", "arm": P, "role": "FOCUS_PRIMITIVE"},
    {"checkpoint": 13, "template": "BASE_TEMPORAL_MARKET_EVENT", "arm": P, "role": "FOCUS_PRIMITIVE"},
    {"checkpoint": 14, "template": "BASE_MARKET_EVENT", "arm": P, "role": "FOCUS_PRIMITIVE"},
]


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _self_hashed(path: Path, field: str) -> tuple[dict[str, Any], str]:
    row = _read(path)
    body = dict(row)
    claim = str(body.pop(field, ""))
    if not claim or stable_hash(body) != claim:
        raise RuntimeError(f"self hash drift: {path}")
    return row, claim


def build() -> dict[str, Any]:
    supply, supply_hash = _self_hashed(SUPPLY, "audit_payload_sha256")
    audit, audit_hash = _self_hashed(V2_AUDIT, "audit_payload_sha256")
    outcome, outcome_hash = _self_hashed(V2_OUTCOME, "outcome_payload_sha256")
    focus, focus_hash = _self_hashed(FOCUS_EVIDENCE, "evidence_payload_sha256")
    accel, accel_hash = _self_hashed(ACCEL, "audit_payload_sha256")

    if supply.get("status") != "ZERO_FINANCIAL_PRIMITIVE_MARKET_SUCCESSOR_V3_FRESH_SUPPLY_READY":
        raise RuntimeError("v3 supply status drift")
    if int(supply["effective_spent_exact_count"]) != 8294 or int(supply["fresh_unique_count"]) != 3302 or int(supply["raw_ordinal_offset"]) != 3072:
        raise RuntimeError("v3 supply geometry drift")
    if any(int(supply["per_template_fresh"].get(t) or 0) < 120 for t in FOCUS):
        raise RuntimeError("v3 focus supply insufficient")
    if any(bool(supply.get(k)) for k in ("production_financial_labels_read_by_builder", "successor_v1_financial_labels_read_by_builder", "successor_v2_financial_labels_read_by_builder")):
        raise RuntimeError("v3 supply label-read drift")
    if any(int(supply.get(k) or 0) != 0 for k in ("validation_reads", "holdout_reads", "historical_2023_reads", "forward_b_reads", "forward_2026_reads")):
        raise RuntimeError("v3 supply restricted-read drift")

    if audit.get("status") != "PASS_INDEPENDENT_TERMINAL_AUDIT" or audit.get("gate_status") != "PRIMITIVE_MARKET_SUCCESSOR_V2_TRANSFER_FAIL" or list(audit.get("failed_gate_checks") or []) != ["each_core_template_uplift_stable"]:
        raise RuntimeError("v2 terminal audit redirect contract drift")
    if outcome.get("status") != "PRIMITIVE_MARKET_SUCCESSOR_V2_POSTRUN_CLOSED_REDIRECT" or outcome.get("post_batch_recommendation") != "REDIRECT":
        raise RuntimeError("v2 postrun redirect contract drift")
    if focus.get("status") != "V2_FOCUS_REDIRECT_EVIDENCE_FROZEN" or tuple(focus.get("redirect_focus_templates") or ()) != FOCUS or focus.get("removed_focus_template") != "BASE_MARKET":
        raise RuntimeError("v2 focus redirect evidence drift")
    if accel.get("status") != "PASS_SUCCESSOR_REAUTH_ELIGIBLE" or int(accel["decision"]["executor_workers_primary"]) != 24 or accel["decision"]["evaluator_pool_lifetime"] != "PERSISTENT_RUN_SCOPE":
        raise RuntimeError("acceleration authority drift")

    template_start: dict[str, int] = {}
    counts: dict[tuple[str, str], int] = {}
    for row in SCHEDULE:
        row["checkpoint_size"] = 24
        row["start_ordinal"] = int(row["checkpoint"]) * 24
        row["template_start_ordinal"] = template_start.get(row["template"], 0)
        template_start[row["template"]] = row["template_start_ordinal"] + 24
        counts[(row["template"], row["arm"])] = counts.get((row["template"], row["arm"]), 0) + 24
    for template in FOCUS:
        if counts[(template, P)] != 72 or counts[(template, U)] != 24 or counts[(template, E)] != 24:
            raise RuntimeError("v3 controlled allocation drift")

    payload: dict[str, Any] = {
        "schema_version": "cn_program_primitive_market_successor_v3_plan_v1",
        "status": "PRIMITIVE_MARKET_SUCCESSOR_V3_PLAN_FROZEN_NOT_RUN",
        "candidate_evaluation_executed": False,
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "design_reason": "V2 failed only the frozen per-core stable-uplift gate because BASE_MARKET transferred poorly on stable uplift, while BASE_EVENT and BASE_TEMPORAL_MARKET_EVENT showed high productive/stable yield without controls and BASE_MARKET_EVENT retained a controlled Primitive advantage. V3 removes BASE_MARKET and converts the three surviving frontiers into equal controlled confirmation blocks. Scorer, primitive stats, evaluator, component universe, diversity contract and resource profile remain frozen.",
        "source_v2_terminal_audit": {"relative_path": str(V2_AUDIT.relative_to(ROOT)).replace("\\", "/"), "file_sha256": _sha(V2_AUDIT), "payload_sha256": audit_hash, "gate_status": audit["gate_status"], "failed_gate_checks": audit["failed_gate_checks"], "records": audit["counts"]["records"]},
        "source_v2_postrun_outcome": {"relative_path": str(V2_OUTCOME.relative_to(ROOT)).replace("\\", "/"), "file_sha256": _sha(V2_OUTCOME), "payload_sha256": outcome_hash, "recommendation": outcome["post_batch_recommendation"]},
        "source_v2_focus_redirect_evidence": {"relative_path": str(FOCUS_EVIDENCE.relative_to(ROOT)).replace("\\", "/"), "file_sha256": _sha(FOCUS_EVIDENCE), "payload_sha256": focus_hash, "redirect_focus_templates": list(FOCUS), "removed_focus_template": "BASE_MARKET"},
        "acceleration_accuracy_audit": {"relative_path": str(ACCEL.relative_to(ROOT)).replace("\\", "/"), "file_sha256": _sha(ACCEL), "payload_sha256": accel_hash},
        "fresh_supply": {"relative_path": str(SUPPLY.relative_to(ROOT)).replace("\\", "/"), "file_sha256": _sha(SUPPLY), "payload_sha256": supply_hash, "raw_ordinal_offset": 3072, "effective_spent_exact_count": 8294, "fresh_unique_count": 3302, "fresh_exact_identities_sha256": supply["fresh_exact_identities_sha256"], "per_template_fresh": {t: int(supply["per_template_fresh"][t]) for t in FOCUS}},
        "search_authority": {"primitive_stats_relative_path": "runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json", "primitive_stats_payload_sha256": "fb5c53287eaaec0b6bfb3dddf6ba0f2846a64b61e888503b2fc00a2b426f7b1d", "production_feedback_imported_into_primitive_stats": False, "v1_successor_feedback_imported_into_primitive_stats": False, "v2_successor_feedback_imported_into_primitive_stats": False, "stage_c_results_in_stats": 0, "stage_d_results_in_stats": 0, "production_results_in_stats": 0, "v1_successor_results_in_stats": 0, "v2_successor_results_in_stats": 0, "diversity_contract_changed": False, "max_variants_per_base_per_template": 4},
        "focus_templates": list(FOCUS),
        "schedule": SCHEDULE,
        "budget": {"checkpoint_size": 24, "checkpoint_count": 15, "hard_cap_logical_records": 360, "primitive_records": 216, "uniform_records": 72, "typed_evolution_records": 72, "focus_records": 360, "per_focus_template_records": 120},
        "prospective_gate": {"focus_primitive_productive_rate_min": 0.40, "focus_primitive_uplift_stable_2of3_rate_min": 0.30, "each_focus_template_primitive_productive_rate_min": 0.30, "each_focus_template_primitive_uplift_stable_2of3_rate_min": 0.25, "each_focus_template_primitive_to_controls_productive_ratio_min": 1.20, "each_focus_template_primitive_to_controls_uplift_stable_ratio_min": 1.20, "total_productive_count_min": 100, "behavior_pair_rate_min": 0.70, "effective_spent_overlap_count_required": 0, "restricted_reads_required_zero": True},
        "resource_contract": {"profile": "SEARCH_DUAL_24", "primary_executor_workers": 24, "fallback_executor_workers": 16, "candidate_evaluation_during_canary": False, "evaluator_pool_lifetime": "PERSISTENT_RUN_SCOPE", "minimum_records_per_hour_after_first_checkpoint": 650.0, "wall_clock_budget_minutes": 50, "minimum_free_memory_bytes": 24 * 1024**3, "throughput_enforcement_after_warm_checkpoints": 2, "persistent_pool_record_hash_parity_required": True},
        "validation_feedback_used": False,
        "oos_authority": "NONE",
        "promotion_authorized": False,
        "automatic_successor_authorized": False,
        "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
    }
    payload["plan_payload_sha256"] = stable_hash(payload)
    return payload


def main() -> None:
    payload = build()
    OUT.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "payload": payload["plan_payload_sha256"], "records": payload["budget"]["hard_cap_logical_records"], "fresh": payload["fresh_supply"]["fresh_unique_count"], "spent": payload["fresh_supply"]["effective_spent_exact_count"], "focus": payload["focus_templates"]}, sort_keys=True))


if __name__ == "__main__":
    main()
