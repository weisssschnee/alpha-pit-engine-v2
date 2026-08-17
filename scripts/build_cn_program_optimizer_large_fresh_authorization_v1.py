"""Build the zero-financial supply audit and frozen large-fresh authorization."""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from scripts import run_cn_program_optimizer_successor_benchmark_v1 as shared
from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import (
    CAMPAIGN_ID,
    CAMPAIGN_PROFILE,
    CHECKPOINT_BATCH_SIZE,
    ENHANCED_TEMPLATES,
    MINIMUM_DISTINCT_BASE_GROUPS_PER_TEMPLATE,
    MAXIMUM_VARIANTS_PER_BASE_GROUP_PER_TEMPLATE,
    MAX_BALANCED_MACROS,
    NEW_BEHAVIOR_PAIR_RATE_COLLAPSE_MAX,
    MINIMUM_PAIRS_PER_HOUR_AFTER_FIRST_MACRO,
    REFERENCE_C140_PAIRS_PER_HOUR,
    PRIMARY_EXECUTOR_WORKERS,
    PRIOR_EXACT_COUNT,
    PRIOR_EXACT_IDENTITIES_SHA256,
    PRIOR_FREEZE_PAYLOAD_SHA256,
    PRODUCTIVE_EFFICIENCY_COLLAPSE_MAX,
    RECORDS_PER_MACRO,
    REMAINING_ENHANCED_PROGRAM_COUNT,
    REQUESTED_HARD_CAP,
    RESOURCE_CPU_THREADS,
    RESOURCE_FALLBACK_EXECUTOR_WORKERS,
    RESOURCE_PROFILE,
    ROUTE_ID,
    MINIMUM_MACROS_BEFORE_VALUE_COLLAPSE_TEST,
    VALUE_COLLAPSE_LOOKBACK_MACROS,
)
from our_system_phase2.runtime.cn_program_optimizer_d1_development_v1 import (
    AUTHORIZED_HOST,
    FROZEN_ENHANCED_PROGRAM_COUNT,
    FROZEN_PROGRAM_SPACE_COUNT,
    FROZEN_PROGRAM_SPACE_SHA256,
    SOURCE_COMPONENT_POOL_SHA256,
    SOURCE_EXECUTION_CONTRACT_SHA256,
    SOURCE_FREEZE_CLOSURE_FILE_SHA256,
    SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256,
    SOURCE_NODE_CAPACITY_SHA256,
    SOURCE_RAW_RESERVOIR_SHA256,
    SOURCE_REGISTRY_SHA256,
    SOURCE_RUN_CONTRACT_FILE_SHA256,
    SOURCE_RUN_CONTRACT_PAYLOAD_SHA256,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    UNIFORM_CONTROL,
)
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH, ACTION_RETRY
from our_system_phase2.services.unified_capability_registry import stable_hash


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build(args: argparse.Namespace) -> dict[str, Any]:
    repo = args.repo_root.resolve()
    prior = json.loads(args.prior_exact_freeze.read_text(encoding="utf-8-sig"))
    prior_body = dict(prior)
    claimed = str(prior_body.pop("freeze_payload_sha256", ""))
    if claimed != stable_hash(prior_body) or claimed != PRIOR_FREEZE_PAYLOAD_SHA256:
        raise RuntimeError("post-C prior freeze drift")
    prior_ids = tuple(map(str, prior["combined_prior_exact_identities"]))
    if (
        len(prior_ids) != PRIOR_EXACT_COUNT
        or stable_hash(list(prior_ids)) != PRIOR_EXACT_IDENTITIES_SHA256
        or int(prior["remaining_enhanced_program_count"])
        != REMAINING_ENHANCED_PROGRAM_COUNT
    ):
        raise RuntimeError("post-C prior identity drift")

    proto = {
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "authorization_payload_sha256": "ZERO_FINANCIAL_SUPPLY_AUDIT_DRAFT",
    }
    authority = shared._load_authority(
        args,
        authorization=proto,
        repo_sha=str(args.repo_sha),
        campaign_id=CAMPAIGN_ID,
        campaign_profile=CAMPAIGN_PROFILE,
        prior_freeze_payload_sha256=PRIOR_FREEZE_PAYLOAD_SHA256,
        prior_exact_count=PRIOR_EXACT_COUNT,
        prior_exact_identities_sha256=PRIOR_EXACT_IDENTITIES_SHA256,
        prior_identity_field="combined_prior_exact_identities",
        input_binding_schema_version="cn_program_optimizer_large_fresh_supply_input_v1",
        resource_profile_id=RESOURCE_PROFILE,
        resource_profile_role="SEARCH",
        maximum_executor_workers=RESOURCE_CPU_THREADS,
    )

    prior_set = set(prior_ids)
    remaining = [
        entry for entry in authority["entries"]
        if entry.exact_identity not in prior_set
        and str(entry.genes["program_template_id"]) in set(ENHANCED_TEMPLATES)
    ]
    if len(remaining) != REMAINING_ENHANCED_PROGRAM_COUNT:
        raise RuntimeError(
            f"remaining enhanced exact drift: {len(remaining)}"
        )
    raw_counts: dict[str, int] = {}
    group_counts: dict[str, dict[str, int]] = {}
    group_capped: dict[str, int] = {}
    distinct_groups: dict[str, int] = {}
    for template in ENHANCED_TEMPLATES:
        rows = [
            entry for entry in remaining
            if str(entry.genes["program_template_id"]) == template
        ]
        counts = Counter(
            str(authority["group_by_exact"][entry.exact_identity])
            for entry in rows
        )
        raw_counts[template] = len(rows)
        group_counts[template] = dict(sorted(counts.items()))
        distinct_groups[template] = len(counts)
        group_capped[template] = sum(
            min(MAXIMUM_VARIANTS_PER_BASE_GROUP_PER_TEMPLATE, count)
            for count in counts.values()
        )
        if len(counts) < MINIMUM_DISTINCT_BASE_GROUPS_PER_TEMPLATE:
            raise RuntimeError(f"insufficient base diversity: {template}")

    supply_macro_cap = min(
        value // CHECKPOINT_BATCH_SIZE for value in group_capped.values()
    )
    macro_count = min(MAX_BALANCED_MACROS, supply_macro_cap)
    if macro_count < 1:
        raise RuntimeError("large fresh balanced supply exhausted")
    logical_cap = macro_count * RECORDS_PER_MACRO
    supply = {
        "schema_version": "cn_program_optimizer_large_fresh_supply_audit_v1",
        "status": "PASS_ZERO_FINANCIAL_SUPPLY_AUDIT",
        "repo_sha": str(args.repo_sha),
        "program_space_count": FROZEN_PROGRAM_SPACE_COUNT,
        "program_space_sha256": FROZEN_PROGRAM_SPACE_SHA256,
        "prior_exact_count": PRIOR_EXACT_COUNT,
        "prior_exact_identities_sha256": PRIOR_EXACT_IDENTITIES_SHA256,
        "prior_freeze_payload_sha256": PRIOR_FREEZE_PAYLOAD_SHA256,
        "remaining_exact_count": len(remaining),
        "raw_remaining_by_template": raw_counts,
        "distinct_base_groups_by_template": distinct_groups,
        "group_capped_remaining_capacity": group_capped,
        "maximum_variants_per_base_group_per_template": (
            MAXIMUM_VARIANTS_PER_BASE_GROUP_PER_TEMPLATE
        ),
        "checkpoint_batch_size": CHECKPOINT_BATCH_SIZE,
        "balanced_supply_macro_cap": supply_macro_cap,
        "requested_hard_cap": REQUESTED_HARD_CAP,
        "authorized_macro_count": macro_count,
        "authorized_logical_record_cap": logical_cap,
        "financial_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    supply["supply_audit_payload_sha256"] = stable_hash(supply)
    _write(args.supply_output, supply)

    authorization = {
        "schema_version": "cn_program_optimizer_large_fresh_development_authorization_v1",
        "status": "LARGE_FRESH_DEVELOPMENT_FROZEN_NOT_RUN",
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "project_control_route_id": ROUTE_ID,
        "execution_authorized": True,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "authorized_host": AUTHORIZED_HOST,
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "formal_search_authority": "HYBRID_TPE_AVAILABILITY",
        "validation_feedback_used": False,
        "program_space": {
            "full_count": FROZEN_PROGRAM_SPACE_COUNT,
            "full_sha256": FROZEN_PROGRAM_SPACE_SHA256,
            "enhanced_count": FROZEN_ENHANCED_PROGRAM_COUNT,
            "prior_exact_count": PRIOR_EXACT_COUNT,
            "prior_exact_identities_sha256": PRIOR_EXACT_IDENTITIES_SHA256,
            "prior_freeze_relative_path": (
                "runtime/run_plans/"
                "cn_program_optimizer_d1_continuation_C_postrun_prior_exact_freeze_20260817.json"
            ),
            "prior_freeze_payload_sha256": PRIOR_FREEZE_PAYLOAD_SHA256,
            "remaining_enhanced_exact_count": REMAINING_ENHANCED_PROGRAM_COUNT,
            "prior_results_imported_as_optimizer_feedback": False,
        },
        "supply_audit": {
            "relative_path": str(args.supply_output.resolve().relative_to(repo)).replace("\\", "/"),
            "file_sha256": _sha256(args.supply_output),
            "payload_sha256": supply["supply_audit_payload_sha256"],
            "remaining_exact_count": len(remaining),
            "raw_remaining_by_template": raw_counts,
            "distinct_base_groups_by_template": distinct_groups,
            "group_capped_remaining_capacity": group_capped,
        },
        "search_design": {
            "template_order": list(ENHANCED_TEMPLATES),
            "checkpoint_batch_size": CHECKPOINT_BATCH_SIZE,
            "records_per_macro": RECORDS_PER_MACRO,
            "requested_hard_cap": REQUESTED_HARD_CAP,
            "macro_count": macro_count,
            "logical_record_cap": logical_cap,
            "formal_optimizer_arm": HYBRID_TPE_PROGRAM,
            "uniform_reserve_arm": UNIFORM_CONTROL,
            "uniform_reserve_schedule": (
                "ONE_ROTATING_TEMPLATE_CHECKPOINT_PER_MACRO"
            ),
            "structured_surrogate_selection_authorized": False,
            "structured_surrogate_feedback_authorized": False,
            "minimum_distinct_base_groups_per_template": (
                MINIMUM_DISTINCT_BASE_GROUPS_PER_TEMPLATE
            ),
            "maximum_variants_per_base_group_per_template": (
                MAXIMUM_VARIANTS_PER_BASE_GROUP_PER_TEMPLATE
            ),
            "adaptive_template_credit_used": False,
            "template_order_or_budget_changes_after_results": "FORBIDDEN",
        },
        "resource_contract": {
            "profile": RESOURCE_PROFILE,
            "cpu_threads": RESOURCE_CPU_THREADS,
            "primary_executor_workers": PRIMARY_EXECUTOR_WORKERS,
            "pre_evaluation_fallback_executor_workers": (
                RESOURCE_FALLBACK_EXECUTOR_WORKERS
            ),
            "fallback_scope": (
                "RESOURCE_CANARY_ONLY_BEFORE_FIRST_CANDIDATE_EVALUATION"
            ),
            "resource_canary_required_before_first_candidate_evaluation": True,
            "resource_canary_financial_candidate_evaluation": False,
        },
        "stopping_contract": {
            "rule_frozen_before_first_financial_read": True,
            "supply_exhaustion": "STOP",
            "integrity_or_resource_failure": "FAIL_CLOSED",
            "minimum_macros_before_value_collapse_test": (
                MINIMUM_MACROS_BEFORE_VALUE_COLLAPSE_TEST
            ),
            "lookback_macros": VALUE_COLLAPSE_LOOKBACK_MACROS,
            "productive_efficiency_collapse_max": (
                PRODUCTIVE_EFFICIENCY_COLLAPSE_MAX
            ),
            "new_behavior_pair_rate_collapse_max": (
                NEW_BEHAVIOR_PAIR_RATE_COLLAPSE_MAX
            ),
            "reference_C140_pairs_per_hour": REFERENCE_C140_PAIRS_PER_HOUR,
            "minimum_pairs_per_hour_after_first_macro": (
                MINIMUM_PAIRS_PER_HOUR_AFTER_FIRST_MACRO
            ),
            "throughput_gate_scope": "INFRASTRUCTURE_ONLY_AFTER_FIRST_COMPLETE_MACRO",
            "value_collapse_rule": (
                "BOTH_THRESHOLDS_TRUE_IN_EACH_OF_LAST_TWO_MACROS"
            ),
            "hard_cap_logical_records": logical_cap,
            "automatic_extension_beyond_hard_cap": False,
        },
        "source_evaluator_authority": {
            "source_freeze_closure_file_sha256": (
                SOURCE_FREEZE_CLOSURE_FILE_SHA256
            ),
            "source_freeze_closure_payload_sha256": (
                SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256
            ),
            "source_run_contract_file_sha256": SOURCE_RUN_CONTRACT_FILE_SHA256,
            "source_run_contract_payload_sha256": (
                SOURCE_RUN_CONTRACT_PAYLOAD_SHA256
            ),
            "raw_reservoir_file_sha256": SOURCE_RAW_RESERVOIR_SHA256,
            "component_pool_file_sha256": SOURCE_COMPONENT_POOL_SHA256,
            "registry_sha256": SOURCE_REGISTRY_SHA256,
            "node_capacity_sha256": SOURCE_NODE_CAPACITY_SHA256,
            "execution_contract_sha256": SOURCE_EXECUTION_CONTRACT_SHA256,
        },
        "restricted_reads": {
            "validation": 0,
            "holdout": 0,
            "historical_2023": 0,
            "forward_b": 0,
            "forward_2026": 0,
        },
        "oos_authority": "NONE",
        "promotion_authorized": False,
        "automatic_successor_authorized": False,
    }
    authorization["authorization_payload_sha256"] = stable_hash(authorization)
    _write(args.authorization_output, authorization)
    return {
        "status": authorization["status"],
        "macro_count": macro_count,
        "logical_record_cap": logical_cap,
        "remaining_exact_count": len(remaining),
        "supply_audit_payload_sha256": supply["supply_audit_payload_sha256"],
        "authorization_payload_sha256": authorization[
            "authorization_payload_sha256"
        ],
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", type=Path, required=True)
    p.add_argument("--repo-sha", required=True)
    p.add_argument("--source-freeze-root", type=Path, required=True)
    p.add_argument("--prior-exact-freeze", type=Path, required=True)
    p.add_argument("--execution-contract", type=Path, required=True)
    p.add_argument("--train-field-root", type=Path, required=True)
    p.add_argument("--train-price-root", type=Path, required=True)
    p.add_argument("--registry", type=Path, required=True)
    p.add_argument("--node-resource-capacity", type=Path, required=True)
    p.add_argument("--executor-workers", type=int, default=PRIMARY_EXECUTOR_WORKERS)
    p.add_argument("--supply-output", type=Path, required=True)
    p.add_argument("--authorization-output", type=Path, required=True)
    args = p.parse_args()
    result = build(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
