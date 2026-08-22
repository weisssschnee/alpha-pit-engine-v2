from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from our_system_phase2.runtime import cn_program_primitive_main_production_v1 as source_runtime
from our_system_phase2.runtime import cn_program_primitive_market_successor_v3 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH, ACTION_RETRY, sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _self_hashed(path: Path, field: str) -> tuple[dict[str, Any], str]:
    row = _read(path)
    body = dict(row)
    claim = str(body.pop(field, ""))
    if not claim or stable_hash(body) != claim:
        raise ValueError(f"self-hash drift: {path}")
    return row, claim


def _bound(repo: Path, plan: dict[str, Any], key: str, field: str) -> tuple[Path, dict[str, Any], str]:
    binding = dict(plan[key])
    path = (repo / Path(str(binding["relative_path"]))).resolve()
    if not path.is_relative_to(repo) or not path.is_file() or sha256_file(path) != str(binding["file_sha256"]):
        raise ValueError(f"{key} file drift")
    row, claim = _self_hashed(path, field)
    if claim != str(binding["payload_sha256"]):
        raise ValueError(f"{key} payload drift")
    return path, row, claim


def build(repo: Path, canary: Path) -> dict[str, Any]:
    repo = repo.resolve()
    source_path = repo / source_runtime.AUTHORIZATION_RELATIVE_PATH
    source = source_runtime.verify_authorization(source_path, repo_root=repo)

    plan_path = repo / runtime.PLAN_RELATIVE_PATH
    plan, plan_hash = _self_hashed(plan_path, "plan_payload_sha256")
    if plan.get("status") != "PRIMITIVE_MARKET_SUCCESSOR_V3_PLAN_FROZEN_NOT_RUN":
        raise ValueError("v3 plan status drift")

    supply_path, supply, supply_hash = _bound(repo, plan, "fresh_supply", "audit_payload_sha256")
    accel_path, accel, accel_hash = _bound(repo, plan, "acceleration_accuracy_audit", "audit_payload_sha256")
    v2_audit_path, v2_audit, v2_audit_hash = _bound(repo, plan, "source_v2_terminal_audit", "audit_payload_sha256")
    v2_outcome_path, v2_outcome, v2_outcome_hash = _bound(repo, plan, "source_v2_postrun_outcome", "outcome_payload_sha256")
    focus_path, focus, focus_hash = _bound(repo, plan, "source_v2_focus_redirect_evidence", "evidence_payload_sha256")

    if supply.get("status") != "ZERO_FINANCIAL_PRIMITIVE_MARKET_SUCCESSOR_V3_FRESH_SUPPLY_READY" or int(supply["effective_spent_exact_count"]) != 8294 or int(supply["fresh_unique_count"]) != 3302:
        raise ValueError("v3 fresh supply drift")
    if accel.get("status") != "PASS_SUCCESSOR_REAUTH_ELIGIBLE":
        raise ValueError("acceleration audit drift")
    if v2_audit.get("status") != "PASS_INDEPENDENT_TERMINAL_AUDIT" or v2_audit.get("gate_status") != "PRIMITIVE_MARKET_SUCCESSOR_V2_TRANSFER_FAIL" or list(v2_audit.get("failed_gate_checks") or []) != ["each_core_template_uplift_stable"]:
        raise ValueError("v2 terminal evidence drift")
    if v2_outcome.get("status") != "PRIMITIVE_MARKET_SUCCESSOR_V2_POSTRUN_CLOSED_REDIRECT" or v2_outcome.get("post_batch_recommendation") != "REDIRECT":
        raise ValueError("v2 redirect outcome drift")
    if focus.get("status") != "V2_FOCUS_REDIRECT_EVIDENCE_FROZEN" or list(focus.get("redirect_focus_templates") or []) != ["BASE_EVENT", "BASE_TEMPORAL_MARKET_EVENT", "BASE_MARKET_EVENT"] or focus.get("removed_focus_template") != "BASE_MARKET":
        raise ValueError("v2 focus redirect evidence drift")

    raw = canary.resolve().read_bytes()
    canary_file_hash = hashlib.sha256(raw).hexdigest()
    canary_row = json.loads(raw.decode("utf-8-sig"))
    canary_body = dict(canary_row)
    canary_hash = str(canary_body.pop("official_canary_payload_sha256", ""))
    if not canary_hash or stable_hash(canary_body) != canary_hash:
        raise ValueError("v3 canary self-hash drift")
    if (
        canary_row.get("status") != "PASS"
        or canary_row.get("candidate_evaluation_executed") is not False
        or int(canary_row.get("preview_selected_count") or 0) != 360
        or int(canary_row.get("field_column_count") or 0) != 53
        or canary_row.get("evaluator_pool_lifetime") != "PERSISTENT_RUN_SCOPE"
        or canary_row.get("successor_plan_payload_sha256") != plan_hash
        or canary_row.get("fresh_supply_payload_sha256") != supply_hash
        or canary_row.get("source_v2_terminal_audit_payload_sha256") != v2_audit_hash
        or canary_row.get("source_v2_postrun_outcome_payload_sha256") != v2_outcome_hash
        or canary_row.get("source_v2_focus_redirect_evidence_payload_sha256") != focus_hash
        or canary_row.get("acceleration_accuracy_audit_payload_sha256") != accel_hash
    ):
        raise ValueError("v3 canary binding drift")

    runner = repo / "scripts/run_cn_program_primitive_market_successor_v3.py"
    runner_hash = sha256_file(runner)
    if canary_row.get("runner_source_file_sha256") != runner_hash:
        raise ValueError("v3 canary runner drift")

    payload: dict[str, Any] = {
        "schema_version": runtime.AUTHORIZATION_SCHEMA,
        "status": "PRIMITIVE_MARKET_SUCCESSOR_V3_AUTHORIZED_NOT_RUN",
        "execution_authorized": True,
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "project_control_route_id": runtime.ROUTE_ID,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "source_production_authorization": {
            "relative_path": str(source_runtime.AUTHORIZATION_RELATIVE_PATH).replace("\\", "/"),
            "file_sha256": sha256_file(source_path),
            "payload_sha256": source["authorization_payload_sha256"],
        },
        "source_prior_exact": dict(source["source_prior_exact"]),
        "source_evaluator_authority": dict(source["source_evaluator_authority"]),
        "source_v2_terminal_audit": {
            "relative_path": str(plan["source_v2_terminal_audit"]["relative_path"]),
            "file_sha256": sha256_file(v2_audit_path),
            "payload_sha256": v2_audit_hash,
            "closure_payload_sha256": v2_audit["closure_payload_sha256"],
            "records": 360,
            "gate_status": v2_audit["gate_status"],
        },
        "source_v2_postrun_outcome": {
            "relative_path": str(plan["source_v2_postrun_outcome"]["relative_path"]),
            "file_sha256": sha256_file(v2_outcome_path),
            "payload_sha256": v2_outcome_hash,
            "recommendation": "REDIRECT",
        },
        "source_v2_focus_redirect_evidence": {
            "relative_path": str(plan["source_v2_focus_redirect_evidence"]["relative_path"]),
            "file_sha256": sha256_file(focus_path),
            "payload_sha256": focus_hash,
            "focus_templates": ["BASE_EVENT", "BASE_TEMPORAL_MARKET_EVENT", "BASE_MARKET_EVENT"],
            "removed_focus_template": "BASE_MARKET",
        },
        "successor_plan": {
            "relative_path": str(runtime.PLAN_RELATIVE_PATH).replace("\\", "/"),
            "file_sha256": sha256_file(plan_path),
            "payload_sha256": plan_hash,
            "hard_cap_logical_records": 360,
        },
        "fresh_supply": {
            "relative_path": str(plan["fresh_supply"]["relative_path"]),
            "file_sha256": sha256_file(supply_path),
            "payload_sha256": supply_hash,
            "effective_spent_exact_count": 8294,
            "fresh_unique_count": 3302,
        },
        "acceleration_accuracy_audit": {
            "relative_path": str(plan["acceleration_accuracy_audit"]["relative_path"]),
            "file_sha256": sha256_file(accel_path),
            "payload_sha256": accel_hash,
            "status": accel["status"],
        },
        "implementation": {"runner_source_file_sha256": runner_hash},
        "resource_contract": {
            "profile": "SEARCH_DUAL_24",
            "cpu_threads": 24,
            "primary_executor_workers": 24,
            "fallback_executor_workers": 16,
            "checkpoint_size": 24,
            "checkpoint_count": 15,
            "hard_cap_logical_records": 360,
            "field_column_count": int(canary_row["field_column_count"]),
            "field_columns_sha256": str(canary_row["field_columns_sha256"]),
            "candidate_evaluation_during_canary": False,
            "evaluator_pool_lifetime": "PERSISTENT_RUN_SCOPE",
            "minimum_records_per_hour_after_first_checkpoint": float(plan["resource_contract"]["minimum_records_per_hour_after_first_checkpoint"]),
            "wall_clock_budget_minutes": int(plan["resource_contract"]["wall_clock_budget_minutes"]),
            "minimum_free_memory_bytes": int(plan["resource_contract"]["minimum_free_memory_bytes"]),
            "throughput_enforcement_after_warm_checkpoints": int(plan["resource_contract"]["throughput_enforcement_after_warm_checkpoints"]),
            "persistent_pool_record_hash_parity_required": True,
        },
        "official_resource_canary": {
            "file_sha256": canary_file_hash,
            "payload_sha256": canary_hash,
            "runner_source_file_sha256": runner_hash,
            "field_column_count": int(canary_row["field_column_count"]),
            "field_columns_sha256": str(canary_row["field_columns_sha256"]),
            "acceleration_accuracy_audit_payload_sha256": accel_hash,
            "source_v2_terminal_audit_payload_sha256": v2_audit_hash,
            "source_v2_postrun_outcome_payload_sha256": v2_outcome_hash,
            "source_v2_focus_redirect_evidence_payload_sha256": focus_hash,
            "evaluator_pool_lifetime": "PERSISTENT_RUN_SCOPE",
        },
        "production_feedback_imported_into_primitive_stats": False,
        "v1_successor_feedback_imported_into_primitive_stats": False,
        "v2_successor_feedback_imported_into_primitive_stats": False,
        "diversity_contract_changed": False,
        "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
        "validation_feedback_used": False,
        "oos_authority": "NONE",
        "promotion_authorized": False,
        "automatic_successor_authorized": False,
    }
    payload["authorization_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--official-resource-canary", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=runtime.AUTHORIZATION_RELATIVE_PATH)
    args = parser.parse_args(argv)
    payload = build(args.repo_root, args.official_resource_canary)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "authorization_payload_sha256": payload["authorization_payload_sha256"], "output": str(output.resolve())}, sort_keys=True))


if __name__ == "__main__":
    main()
