"""Build Production Wave 3 authorization after zero-financial supply/canary."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as source_runtime
from our_system_phase2.runtime import cn_search_core_v2_production_wave3_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH, ACTION_RETRY, sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def build(repo: Path, *, canary: Path, supply_audit: Path) -> dict[str, Any]:
    repo = repo.resolve()
    source_path = repo / source_runtime.AUTHORIZATION_RELATIVE_PATH
    source = source_runtime.verify_authorization(source_path, repo_root=repo)
    policy_path = repo / "runtime/run_plans/cn_search_core_v2_policy_review_20260825.json"
    policy = _read(policy_path)
    policy_hash = _verify(policy, "policy_review_payload_sha256", "policy review")
    pre_path = repo / runtime.PREFREEZE_RELATIVE_PATH
    prefreeze = _read(pre_path)
    prefreeze_hash = _verify(prefreeze, "prefreeze_payload_sha256", "Production Wave 3 prefreeze")
    supply_path = supply_audit.resolve()
    supply = _read(supply_path)
    supply_hash = _verify(supply, "audit_payload_sha256", "Production Wave 3 supply")
    canary_path = canary.resolve()
    canary_payload = _read(canary_path)
    canary_hash = _verify(canary_payload, "official_canary_payload_sha256", "Production Wave 3 canary")
    runner = repo / "scripts/run_cn_search_core_v2_production_wave3_v1.py"
    runner_sha = sha256_file(runner)
    if (
        policy.get("status") != "SEARCH_CORE_V2_POLICY_REVIEW_COMPLETE_MATURE_STATE_JUMP_PRIMARY"
        or policy["project_control_decision"].get("search_core_policy_change_authorized") is not True
        or policy["project_control_decision"].get("automatic_financial_campaign_launch_authorized") is not False
    ):
        raise RuntimeError("Production Wave 3 policy not approved")
    pair_contract = dict(prefreeze.get("pair_native_annotation_contract") or {})
    if (
        prefreeze.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE3_PREFROZEN_BEFORE_FINANCIAL_READ"
        or int(prefreeze["production_wave3"]["total_financial_evaluations"]) != runtime.TOTAL_EVALUATIONS
        or bool(prefreeze.get("financial_labels_read_by_builder"))
        or pair_contract.get("application_scope") != "DEVELOPMENT_RESULT_ANNOTATION_AND_ARCHIVE_ONLY"
        or pair_contract.get("optimizer_feedback_write") is not False
        or pair_contract.get("generator_ask_order_changed") is not False
        or pair_contract.get("candidate_admission_changed") is not False
        or pair_contract.get("automatic_policy_adoption") is not False
        or pair_contract.get("validation_label_read_at_application") is not False
    ):
        raise RuntimeError("Production Wave 3 prefreeze drift")
    if (
        supply.get("status") != "PASS_ZERO_FINANCIAL_PRODUCTION_WAVE3_CHECKPOINT_SUPPLY_AUDIT"
        or int(supply.get("generated_total") or 0) != runtime.SUPPLY_PROBE_TOTAL
        or int(supply.get("prior_overlap_count", -1)) != 0
        or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("financial_sidecar_read"))
    ):
        raise RuntimeError("Production Wave 3 supply drift")
    if (
        canary_payload.get("status") != "PASS"
        or int(canary_payload.get("requested_workers") or 0) != 24
        or canary_payload.get("candidate_evaluation_executed") is not False
        or canary_payload.get("runner_source_file_sha256") != runner_sha
    ):
        raise RuntimeError("Production Wave 3 canary drift")
    payload = {
        "schema_version": runtime.AUTHORIZATION_SCHEMA,
        "status": "SEARCH_CORE_V2_PRODUCTION_WAVE3_AUTHORIZED_NOT_RUN",
        "execution_authorized": True,
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "project_control_route_id": runtime.ROUTE_ID,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "source_stage_d_authorization": {
            "relative_path": str(source_runtime.AUTHORIZATION_RELATIVE_PATH).replace("\\", "/"),
            "file_sha256": sha256_file(source_path),
            "payload_sha256": source["authorization_payload_sha256"],
        },
        "source_prior_exact": dict(source["source_prior_exact"]),
        "source_evaluator_authority": dict(source["source_evaluator_authority"]),
        "source_policy_review": {
            "relative_path": str(policy_path.relative_to(repo)).replace("\\", "/"),
            "file_sha256": sha256_file(policy_path),
            "payload_sha256": policy_hash,
        },
        "production_wave3_prefreeze": {
            "relative_path": str(runtime.PREFREEZE_RELATIVE_PATH).replace("\\", "/"),
            "file_sha256": sha256_file(pre_path),
            "payload_sha256": prefreeze_hash,
        },
        "production_wave3_supply_audit": {
            "relative_path": str(supply_path.relative_to(repo)).replace("\\", "/"),
            "file_sha256": sha256_file(supply_path),
            "payload_sha256": supply_hash,
            "generated_total": supply["generated_total"],
            "field_column_count": supply["resource_field_surface"]["field_column_count"],
            "field_columns_sha256": supply["resource_field_surface"]["field_columns_sha256"],
        },
        "official_resource_canary": {
            "relative_path": str(canary_path.relative_to(repo)).replace("\\", "/"),
            "file_sha256": sha256_file(canary_path),
            "payload_sha256": canary_hash,
            "runner_source_file_sha256": runner_sha,
            "field_column_count": canary_payload["field_column_count"],
            "field_columns_sha256": canary_payload["field_columns_sha256"],
        },
        "resource_contract": {
            "profile": "SEARCH_DUAL_24",
            "cpu_threads": 24,
            "executor_workers": 24,
            "checkpoint_size": 24,
            "checkpoint_count": runtime.CHECKPOINT_COUNT,
            "candidate_evaluation_during_canary": False,
            "field_column_count": canary_payload["field_column_count"],
            "field_columns_sha256": canary_payload["field_columns_sha256"],
        },
        "production_contract": dict(prefreeze["production_wave3"]),
        "pair_native_annotation_contract": pair_contract,
        "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
        "validation_feedback_used": False,
        "promotion_authorized": False,
        "oos_authority": "NONE",
        "capital_action_authorized": False,
        "automatic_followon_authorized": False,
    }
    payload["authorization_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--official-resource-canary", type=Path, required=True)
    parser.add_argument("--production-wave3-supply-audit", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=runtime.AUTHORIZATION_RELATIVE_PATH)
    args = parser.parse_args(argv)
    payload = build(args.repo_root, canary=args.official_resource_canary, supply_audit=args.production_wave3_supply_audit)
    out = args.output if args.output.is_absolute() else args.repo_root / args.output
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "payload": payload["authorization_payload_sha256"], "output": str(out.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
