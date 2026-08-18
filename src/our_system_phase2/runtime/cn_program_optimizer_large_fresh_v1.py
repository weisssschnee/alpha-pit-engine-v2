"""Project-Control entry for the large fresh development-only Program search.

Hybrid TPE + Availability remains the formal development search authority.
Uniform is a fixed exploration reserve. Structured Surrogate cannot select or
receive economic feedback in this campaign.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import TEMPLATE_ORDER
from our_system_phase2.runtime.cn_program_optimizer_d1_development_v1 import (
    AUTHORIZED_HOST,
    FROZEN_ENHANCED_PROGRAM_COUNT,
    FROZEN_PROGRAM_SPACE_COUNT,
    FROZEN_PROGRAM_SPACE_SHA256,
)
from our_system_phase2.services.node_resource_governor import (
    validate_node_resource_lease_receipt,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    UNIFORM_CONTROL,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    ProjectControlDenied,
    consume_active_admission,
    sha256_file,
    verify_campaign_authorization_binding,
    verify_consumed_admission_target,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

ROUTE_ID = "cn-program-optimizer-large-fresh-development-v1"
CAMPAIGN_ID = "CN_PROGRAM_OPTIMIZER_LARGE_FRESH_DEVELOPMENT_V1"
CAMPAIGN_PROFILE = "cn_program_optimizer_large_fresh_development_v1"
AUTHORIZATION_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_large_fresh_development_v1.json"
)
PRIOR_FREEZE_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_d1_continuation_C_postrun_prior_exact_freeze_20260817.json"
)
PRIOR_EXACT_COUNT = 1310
PRIOR_EXACT_IDENTITIES_SHA256 = (
    "90b0f3b13ffdc83e4a3398fc4c78499a7273b1dd72dbc86a04db4cbc84d6ab18"
)
PRIOR_FREEZE_PAYLOAD_SHA256 = (
    "7d9c18e664326a53cebaf699a077672b7185ff89a81a17c2e0471703bcf862fa"
)
REMAINING_ENHANCED_PROGRAM_COUNT = 2274
ENHANCED_TEMPLATES = tuple(
    template for template in TEMPLATE_ORDER if str(template) != "BASE"
)
REQUESTED_HARD_CAP = 1960
CHECKPOINT_BATCH_SIZE = 24
CHECKPOINTS_PER_MACRO = len(ENHANCED_TEMPLATES)
RECORDS_PER_MACRO = CHECKPOINT_BATCH_SIZE * CHECKPOINTS_PER_MACRO
MAX_BALANCED_MACROS = REQUESTED_HARD_CAP // RECORDS_PER_MACRO
RESOURCE_PROFILE = "SEARCH_DUAL_24"
RESOURCE_CPU_THREADS = 24
PRIMARY_EXECUTOR_WORKERS = 24
RESOURCE_FALLBACK_EXECUTOR_WORKERS = 16
MINIMUM_DISTINCT_BASE_GROUPS_PER_TEMPLATE = 16
MAXIMUM_VARIANTS_PER_BASE_GROUP_PER_TEMPLATE = 4
MINIMUM_MACROS_BEFORE_VALUE_COLLAPSE_TEST = 4
VALUE_COLLAPSE_LOOKBACK_MACROS = 2
PRODUCTIVE_EFFICIENCY_COLLAPSE_MAX = 0.15
NEW_BEHAVIOR_PAIR_RATE_COLLAPSE_MAX = 0.05
REFERENCE_C140_PAIRS_PER_HOUR = 140.0 * 3600.0 / 1919.003046500031
MINIMUM_PAIRS_PER_HOUR_AFTER_FIRST_MACRO = 200.0


def _read_self_hashed(path: Path, field: str, label: str) -> dict[str, Any]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError(f"{label} self-hash drift")
    return payload


def verify_authorization(
    path: Path,
    *,
    repo_root: Path | None = None,
    expected_schema_version: str = "cn_program_optimizer_large_fresh_development_authorization_v1",
    expected_campaign_id: str = CAMPAIGN_ID,
    expected_campaign_profile: str = CAMPAIGN_PROFILE,
    expected_route_id: str = ROUTE_ID,
    expected_formal_search_authority: str = "HYBRID_TPE_AVAILABILITY",
    expected_formal_optimizer_arm: str = HYBRID_TPE_PROGRAM,
) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = _read_self_hashed(
        path, "authorization_payload_sha256", "large fresh Program authorization"
    )
    if (
        payload.get("schema_version") != expected_schema_version
        or payload.get("status") != "LARGE_FRESH_DEVELOPMENT_FROZEN_NOT_RUN"
        or payload.get("campaign_id") != expected_campaign_id
        or payload.get("campaign_profile") != expected_campaign_profile
        or payload.get("project_control_route_id") != expected_route_id
        or not bool(payload.get("execution_authorized"))
        or list(payload.get("permitted_project_control_actions") or ())
        != [ACTION_LAUNCH, ACTION_RETRY]
        or str(payload.get("authorized_host") or "").upper() != AUTHORIZED_HOST
        or payload.get("evaluation_data_role") != "DEVELOPMENT_ONLY"
        or payload.get("formal_search_authority") != expected_formal_search_authority
        or payload.get("validation_feedback_used") is not False
        or bool(payload.get("promotion_authorized"))
        or bool(payload.get("automatic_successor_authorized"))
    ):
        raise ValueError("large fresh Program authorization contract drift")

    space = dict(payload.get("program_space") or {})
    if (
        int(space.get("full_count") or 0) != FROZEN_PROGRAM_SPACE_COUNT
        or str(space.get("full_sha256") or "") != FROZEN_PROGRAM_SPACE_SHA256
        or int(space.get("enhanced_count") or 0) != FROZEN_ENHANCED_PROGRAM_COUNT
        or int(space.get("prior_exact_count") or 0) != PRIOR_EXACT_COUNT
        or str(space.get("prior_exact_identities_sha256") or "")
        != PRIOR_EXACT_IDENTITIES_SHA256
        or str(space.get("prior_freeze_payload_sha256") or "")
        != PRIOR_FREEZE_PAYLOAD_SHA256
        or int(space.get("remaining_enhanced_exact_count") or 0)
        != REMAINING_ENHANCED_PROGRAM_COUNT
        or bool(space.get("prior_results_imported_as_optimizer_feedback"))
    ):
        raise ValueError("large fresh Program-space binding drift")
    prior_path = (root / PRIOR_FREEZE_RELATIVE_PATH).resolve()
    prior = _read_self_hashed(
        prior_path, "freeze_payload_sha256", "post-C prior exact freeze"
    )
    prior_ids = list(map(str, prior.get("combined_prior_exact_identities") or ()))
    if (
        len(prior_ids) != PRIOR_EXACT_COUNT
        or len(set(prior_ids)) != PRIOR_EXACT_COUNT
        or stable_hash(prior_ids) != PRIOR_EXACT_IDENTITIES_SHA256
        or int(prior.get("remaining_enhanced_program_count") or 0)
        != REMAINING_ENHANCED_PROGRAM_COUNT
    ):
        raise ValueError("large fresh prior exact freeze drift")

    design = dict(payload.get("search_design") or {})
    macro_count = int(design.get("macro_count") or 0)
    logical_cap = int(design.get("logical_record_cap") or 0)
    if (
        list(design.get("template_order") or ()) != list(ENHANCED_TEMPLATES)
        or int(design.get("checkpoint_batch_size") or 0) != CHECKPOINT_BATCH_SIZE
        or int(design.get("records_per_macro") or 0) != RECORDS_PER_MACRO
        or int(design.get("requested_hard_cap") or 0) != REQUESTED_HARD_CAP
        or not 1 <= macro_count <= MAX_BALANCED_MACROS
        or logical_cap != macro_count * RECORDS_PER_MACRO
        or logical_cap > REQUESTED_HARD_CAP
        or design.get("formal_optimizer_arm") != expected_formal_optimizer_arm
        or design.get("uniform_reserve_arm") != UNIFORM_CONTROL
        or design.get("uniform_reserve_schedule")
        != "ONE_ROTATING_TEMPLATE_CHECKPOINT_PER_MACRO"
        or bool(design.get("structured_surrogate_selection_authorized"))
        or bool(design.get("structured_surrogate_feedback_authorized"))
        or int(design.get("minimum_distinct_base_groups_per_template") or 0)
        != MINIMUM_DISTINCT_BASE_GROUPS_PER_TEMPLATE
        or int(design.get("maximum_variants_per_base_group_per_template") or 0)
        != MAXIMUM_VARIANTS_PER_BASE_GROUP_PER_TEMPLATE
        or bool(design.get("adaptive_template_credit_used"))
        or design.get("template_order_or_budget_changes_after_results") != "FORBIDDEN"
    ):
        raise ValueError("large fresh search design drift")

    supply = dict(payload.get("supply_audit") or {})
    supply_path = (root / Path(str(supply.get("relative_path") or ""))).resolve()
    if not supply_path.is_relative_to(root) or not supply_path.is_file():
        raise ValueError("large fresh supply audit path drift")
    if sha256_file(supply_path) != str(supply.get("file_sha256") or ""):
        raise ValueError("large fresh supply audit file drift")
    supply_payload = _read_self_hashed(
        supply_path, "supply_audit_payload_sha256", "large fresh supply audit"
    )
    if supply_payload.get("supply_audit_payload_sha256") != supply.get("payload_sha256"):
        raise ValueError("large fresh supply audit payload drift")
    per_template_capacity = {
        str(key): int(value)
        for key, value in dict(
            supply.get("group_capped_remaining_capacity") or {}
        ).items()
    }
    if set(per_template_capacity) != set(ENHANCED_TEMPLATES):
        raise ValueError("large fresh supply template coverage drift")
    if min(per_template_capacity.values(), default=0) < (
        macro_count * CHECKPOINT_BATCH_SIZE
    ):
        raise ValueError("large fresh balanced supply insufficient")
    if int(supply.get("remaining_exact_count") or 0) != REMAINING_ENHANCED_PROGRAM_COUNT:
        raise ValueError("large fresh remaining supply drift")

    resource = dict(payload.get("resource_contract") or {})
    if (
        resource.get("profile") != RESOURCE_PROFILE
        or int(resource.get("cpu_threads") or 0) != RESOURCE_CPU_THREADS
        or int(resource.get("primary_executor_workers") or 0)
        != PRIMARY_EXECUTOR_WORKERS
        or int(resource.get("pre_evaluation_fallback_executor_workers") or 0)
        != RESOURCE_FALLBACK_EXECUTOR_WORKERS
        or resource.get("fallback_scope")
        != "RESOURCE_CANARY_ONLY_BEFORE_FIRST_CANDIDATE_EVALUATION"
        or resource.get("resource_canary_required_before_first_candidate_evaluation") is not True
        or resource.get("resource_canary_financial_candidate_evaluation") is not False
    ):
        raise ValueError("large fresh resource contract drift")

    stop = dict(payload.get("stopping_contract") or {})
    if (
        int(stop.get("minimum_macros_before_value_collapse_test") or 0)
        != MINIMUM_MACROS_BEFORE_VALUE_COLLAPSE_TEST
        or int(stop.get("lookback_macros") or 0) != VALUE_COLLAPSE_LOOKBACK_MACROS
        or float(stop.get("productive_efficiency_collapse_max") or -1.0)
        != PRODUCTIVE_EFFICIENCY_COLLAPSE_MAX
        or float(stop.get("new_behavior_pair_rate_collapse_max") or -1.0)
        != NEW_BEHAVIOR_PAIR_RATE_COLLAPSE_MAX
        or float(stop.get("reference_C140_pairs_per_hour") or 0.0)
        != REFERENCE_C140_PAIRS_PER_HOUR
        or float(stop.get("minimum_pairs_per_hour_after_first_macro") or 0.0)
        != MINIMUM_PAIRS_PER_HOUR_AFTER_FIRST_MACRO
        or stop.get("throughput_gate_scope")
        != "INFRASTRUCTURE_ONLY_AFTER_FIRST_COMPLETE_MACRO"
        or stop.get("value_collapse_rule")
        != "BOTH_THRESHOLDS_TRUE_IN_EACH_OF_LAST_TWO_MACROS"
        or stop.get("rule_frozen_before_first_financial_read") is not True
        or int(stop.get("hard_cap_logical_records") or 0) != logical_cap
        or bool(stop.get("automatic_extension_beyond_hard_cap"))
    ):
        raise ValueError("large fresh stopping contract drift")

    restricted = dict(payload.get("restricted_reads") or {})
    if any(
        int(restricted.get(key) or 0) != 0
        for key in (
            "validation",
            "holdout",
            "historical_2023",
            "forward_b",
            "forward_2026",
        )
    ):
        raise ValueError("large fresh restricted-read contract drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(
        "cn-program-optimizer-large-fresh-development-v1",
        {ACTION_LAUNCH, ACTION_RETRY},
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--node-resource-lease-receipt", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)

    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(
        admission, args.campaign_authorization
    )
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("large fresh authorization payload drift")
    validate_node_resource_lease_receipt(
        args.node_resource_lease_receipt.resolve(),
        expected_role="SEARCH",
        expected_cpu_threads=RESOURCE_CPU_THREADS,
    )

    from scripts.run_cn_program_optimizer_large_fresh_v1 import run

    result = run(args, admission=admission, authorization=authorization)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
