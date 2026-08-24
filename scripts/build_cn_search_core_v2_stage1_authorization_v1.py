"""Build final Search Core V2 Stage-1 authorization after real-supply audit and official canary."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as source_runtime
from our_system_phase2.runtime import cn_search_core_v2_stage1_v1 as runtime
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    sha256_file,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _selfhash(path: Path, field: str, label: str) -> tuple[dict[str, Any], str]:
    payload = _read(path)
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError(f"{label} self-hash drift")
    return payload, claimed


def build(repo: Path, *, canary: Path, supply_audit: Path) -> dict[str, Any]:
    repo = repo.resolve()
    source_path = repo / source_runtime.AUTHORIZATION_RELATIVE_PATH
    source = source_runtime.verify_authorization(source_path, repo_root=repo)

    pre_path = repo / runtime.PREFREEZE_RELATIVE_PATH
    prefreeze, pre_hash = _selfhash(
        pre_path, "prefreeze_payload_sha256", "Search Core V2 Stage-1 prefreeze"
    )
    if (
        prefreeze.get("status")
        != "SEARCH_CORE_V2_STAGE1_PREFROZEN_BEFORE_FINANCIAL_READ"
        or int(prefreeze["stage1"]["total_budget_per_arm"]) != runtime.TOTAL_PER_ARM
        or int(prefreeze["stage1"]["total_financial_evaluations"])
        != runtime.TOTAL_EVALUATIONS
        or bool(prefreeze.get("financial_labels_read_by_builder"))
    ):
        raise ValueError("Search Core V2 prefreeze contract drift")

    supply_path = supply_audit.resolve()
    supply, supply_hash = _selfhash(
        supply_path,
        "audit_payload_sha256",
        "Search Core V2 real state-jump supply audit",
    )
    if (
        supply.get("status")
        != "PASS_ZERO_FINANCIAL_STATE_JUMP_REAL_SUPPLY_AUDIT"
        or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("financial_sidecar_read"))
        or int(supply["generator"]["generated_total"]) < runtime.TOTAL_EVALUATIONS
        or any(
            int(row.get("generated") or 0) < 48
            for row in dict(supply["generator"]["per_template"]).values()
        )
    ):
        raise ValueError("Search Core V2 real state-jump supply audit not PASS")

    raw = canary.resolve().read_bytes()
    canary_file_hash = hashlib.sha256(raw).hexdigest()
    canary_row = json.loads(raw.decode("utf-8-sig"))
    canary_body = dict(canary_row)
    canary_hash = str(canary_body.pop("official_canary_payload_sha256", ""))
    if (
        not canary_hash
        or stable_hash(canary_body) != canary_hash
        or canary_row.get("status") != "PASS"
        or canary_row.get("candidate_evaluation_executed") is not False
    ):
        raise ValueError("Search Core V2 official canary not PASS")
    runner_path = repo / "scripts/run_cn_search_core_v2_stage1_v1.py"
    runner_sha = sha256_file(runner_path)
    if (
        canary_row.get("runner_source_file_sha256") != runner_sha
        or canary_row.get("prefreeze_payload_sha256") != pre_hash
        or canary_row.get("state_jump_supply_audit_payload_sha256") != supply_hash
        or int(canary_row.get("requested_workers") or 0)
        != runtime.PRIMARY_EXECUTOR_WORKERS
        or int(canary_row.get("field_column_count") or 0) < 1
    ):
        raise ValueError("Search Core V2 official canary implementation binding drift")

    payload = {
        "schema_version": runtime.AUTHORIZATION_SCHEMA,
        "status": "SEARCH_CORE_V2_STAGE1_AUTHORIZED_NOT_RUN",
        "execution_authorized": True,
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "project_control_route_id": runtime.ROUTE_ID,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "source_stage_d_authorization": {
            "relative_path": str(source_runtime.AUTHORIZATION_RELATIVE_PATH).replace("\\", "/"),
            "file_sha256": sha256_file(source_path),
            "payload_sha256": str(source["authorization_payload_sha256"]),
        },
        # Reuse the already audited development data/evaluator bindings.  The new
        # campaign changes proposal generation only.
        "source_prior_exact": dict(source["source_prior_exact"]),
        "source_evaluator_authority": dict(source["source_evaluator_authority"]),
        "stage1_prefreeze": {
            "relative_path": str(runtime.PREFREEZE_RELATIVE_PATH).replace("\\", "/"),
            "file_sha256": sha256_file(pre_path),
            "payload_sha256": pre_hash,
            "arm_a_selected_exact_identities_sha256": str(
                prefreeze["arm_a_primitive"]["selected_exact_identities_sha256"]
            ),
            "budget_per_arm": runtime.TOTAL_PER_ARM,
            "total_financial_evaluations": runtime.TOTAL_EVALUATIONS,
        },
        "state_jump_supply_audit": {
            "relative_path": str(supply_path.relative_to(repo)).replace("\\", "/"),
            "file_sha256": sha256_file(supply_path),
            "payload_sha256": supply_hash,
            "generated_total": int(supply["generator"]["generated_total"]),
            "field_column_count": int(
                supply["resource_field_surface"]["field_column_count"]
            ),
            "field_columns_sha256": str(
                supply["resource_field_surface"]["field_columns_sha256"]
            ),
        },
        "official_resource_canary": {
            "relative_path": str(canary.resolve().relative_to(repo)).replace("\\", "/"),
            "file_sha256": canary_file_hash,
            "payload_sha256": canary_hash,
            "runner_source_file_sha256": runner_sha,
            "field_column_count": int(canary_row["field_column_count"]),
            "field_columns_sha256": str(canary_row["field_columns_sha256"]),
        },
        "resource_contract": {
            "profile": "SEARCH_DUAL_24",
            "cpu_threads": 24,
            "executor_workers": runtime.PRIMARY_EXECUTOR_WORKERS,
            "checkpoint_size": runtime.CHECKPOINT_SIZE,
            "candidate_evaluation_during_canary": False,
            "field_column_count": int(canary_row["field_column_count"]),
            "field_columns_sha256": str(canary_row["field_columns_sha256"]),
        },
        "stage1_contract": dict(prefreeze["stage1"]),
        "restricted_reads": {
            "validation": 0,
            "holdout": 0,
            "historical_2023": 0,
            "forward_b": 0,
            "forward_2026": 0,
        },
        "validation_feedback_used": False,
        "promotion_authorized": False,
        "oos_authority": "NONE",
        "automatic_stage2_authorized": False,
    }
    payload["authorization_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--official-resource-canary", type=Path, required=True)
    parser.add_argument("--state-jump-supply-audit", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=runtime.AUTHORIZATION_RELATIVE_PATH)
    args = parser.parse_args(argv)
    payload = build(
        args.repo_root,
        canary=args.official_resource_canary,
        supply_audit=args.state_jump_supply_audit,
    )
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "status": payload["status"],
                "authorization_payload_sha256": payload["authorization_payload_sha256"],
                "output": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
