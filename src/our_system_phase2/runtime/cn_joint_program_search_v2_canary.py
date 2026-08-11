"""Search Engine V2 prospective 512-ask development canary.

The committed authorization freezes the design.  This module is the physical
high-cost entry and therefore consumes Project Control before argument parsing.
No code in this module grants execution authority by itself.
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
import math
from pathlib import Path
from statistics import mean, median
from typing import Any, Mapping, Sequence

from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import TEMPLATE_ORDER
from our_system_phase2.services.development_feedback_provenance import (
    build_development_feedback_provenance,
)
from our_system_phase2.services.evaluation_asset_authority import (
    verify_search_feedback_boundary,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RECOVERY,
    ACTION_RETRY,
    ProjectControlDenied,
    consume_active_admission,
    verify_campaign_authorization_binding,
    verify_consumed_admission_target,
)
from our_system_phase2.services.search_v2_conditional_uplift import (
    MATCHED_CONTROL_CONTRACT_ID,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


ROUTE_ID = "cn-joint-program-search-v2-canary"
CANARY_PROFILE = "cn_joint_program_search_v2_prospective_512_v1"
CAMPAIGN_ID = "CN_JOINT_PROGRAM_SEARCH_ENGINE_V2_CANARY_V1"
BATCH_ID = "SEARCH_V2_PROSPECTIVE_512_FROZEN_V1"
CANARY_AUTHORIZATION_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_search_engine_v2_prospective_512_canary_v1.json"
)
CANARY_AUTHORIZATION_SCHEMA = "cn_search_engine_v2_canary_authorization_v1"
CANARY_AUTHORIZATION_STATUS = "SEARCH_ENGINE_V2_CANARY_FROZEN_NOT_RUN"
EXPECTED_RECORDS = 512
RECORDS_PER_CHECKPOINT = 8
CHECKPOINT_COUNT = EXPECTED_RECORDS // RECORDS_PER_CHECKPOINT
RECORDS_PER_TEMPLATE = 64
RAW_RESERVOIR_PER_ENHANCED_TEMPLATE = 512
MAX_VARIANTS_PER_BASE_PER_TEMPLATE = 4
MIN_BASE_IDENTITIES_PER_TEMPLATE = 16

ARM_UNIFORM = "UNIFORM_FRESH"
ARM_CONDITIONAL_UPLIFT = "CONDITIONAL_UPLIFT_EXPLOIT"
ARM_NOVELTY = "NOVELTY_RESERVE"
ENHANCED_ARM_PATTERN = (
    ARM_UNIFORM,
    ARM_UNIFORM,
    ARM_UNIFORM,
    ARM_UNIFORM,
    ARM_CONDITIONAL_UPLIFT,
    ARM_CONDITIONAL_UPLIFT,
    ARM_CONDITIONAL_UPLIFT,
    ARM_NOVELTY,
)

MATCHED_EVALUATOR_CONTRACT = {
    "evaluator_authority": "PHASE3CM_STREAMING_MULTICANDIDATE_EVALUATOR",
    "result_record_schema": "cn_joint_program_phase_c_record_v0",
    "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
    "portfolio_decoder_id": "TOPK_10_EQUAL",
    "economic_fields_reused": [
        "replay_status",
        "blockers",
        "primary.continuous_book_net_reward",
        "primary.cumulative_net_return",
        "primary.development_subwindows",
        "primary.net_return_per_turnover",
        "primary.mean_one_way_turnover",
        "primary.fill_count",
        "base_control",
        "matched_net_reward_increment",
        "matched_cumulative_return_increment",
    ],
    "new_evaluator_created": False,
}

PROSPECTIVE_SUCCESS_GATES = {
    "decision_rule": "NONINFERIOR_ABSOLUTE_AND_SUPERIOR_CONDITIONAL_UPLIFT",
    "tradeoff_between_heads_allowed": False,
    "comparison": f"{ARM_CONDITIONAL_UPLIFT}_VS_{ARM_UNIFORM}",
    "absolute_noninferiority": {
        "margin": 0.0,
        "required_exact_records_per_arm": {
            ARM_UNIFORM: 224,
            ARM_CONDITIONAL_UPLIFT: 168,
        },
        "minimum_global_records_per_arm": 128,
        "minimum_records_per_template_per_arm": {
            ARM_UNIFORM: 32,
            ARM_CONDITIONAL_UPLIFT: 24,
        },
        "checks": {
            "admission_rate_delta_minimum": 0.0,
            "primary_reward_positive_rate_delta_minimum": 0.0,
            "primary_return_positive_rate_delta_minimum": 0.0,
            "stable_two_of_three_rate_delta_minimum": 0.0,
            "blocker_rate_delta_maximum": 0.0,
            "median_return_per_turnover_delta_minimum": 0.0,
        },
    },
    "conditional_superiority": {
        "minimum_admitted_records_per_arm": 64,
        "minimum_templates_with_eight_admitted_per_arm": 4,
        "minimum_improved_templates": 4,
        "template_improvement_requires_all_strict_checks": True,
        "strict_checks": {
            "matched_reward_increment_mean_delta_minimum_exclusive": 0.0,
            "matched_reward_increment_median_delta_minimum_exclusive": 0.0,
            "matched_return_increment_mean_delta_minimum_exclusive": 0.0,
            "matched_return_increment_median_delta_minimum_exclusive": 0.0,
            "cross_window_matched_consistency_delta_minimum_exclusive": 0.0,
        },
    },
    "insufficient_support_result": "FAIL_CLOSED_INSUFFICIENT_PROSPECTIVE_SUPPORT",
    "post_read_gate_change_allowed": False,
    "promotion_authorized": False,
}


def _finite_metric(value: Any, label: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Search V2 gate metric invalid: {label}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"Search V2 gate metric invalid: {label}")
    return parsed


def _absolute_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    if not count:
        return {
            "records": 0,
            "admission_rate": 0.0,
            "primary_reward_positive_rate": 0.0,
            "primary_return_positive_rate": 0.0,
            "stable_two_of_three_rate": 0.0,
            "blocker_rate": 0.0,
            "median_return_per_turnover": None,
        }
    admissions = [dict(row.get("absolute_admission") or {}) for row in rows]
    metrics = [dict(row.get("metrics") or {}) for row in admissions]
    turnover = [
        _finite_metric(row.get("primary_net_return_per_turnover"), "turnover")
        for row in metrics
        if row.get("primary_net_return_per_turnover") is not None
    ]
    return {
        "records": count,
        "admission_rate": sum(bool(row.get("admitted")) for row in admissions)
        / count,
        "primary_reward_positive_rate": sum(
            _finite_metric(row.get("primary_continuous_book_net_reward"), "reward")
            > 0.0
            for row in metrics
            if row.get("primary_continuous_book_net_reward") is not None
        )
        / count,
        "primary_return_positive_rate": sum(
            _finite_metric(row.get("primary_cumulative_net_return"), "return")
            > 0.0
            for row in metrics
            if row.get("primary_cumulative_net_return") is not None
        )
        / count,
        "stable_two_of_three_rate": sum(
            int(row.get("positive_development_window_count") or 0) >= 2
            for row in metrics
        )
        / count,
        "blocker_rate": sum(
            "CANDIDATE_LOCAL_BLOCKER" in tuple(row.get("failure_reasons") or ())
            for row in admissions
        )
        / count,
        "median_return_per_turnover": (
            float(median(turnover)) if turnover else None
        ),
    }


def _conditional_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    credits = [
        dict(dict(row["enhancer_credit"])["program_credit"])
        for row in rows
        if row.get("enhancer_credit") is not None
    ]
    keys = {
        "matched_reward_increment": "matched_net_reward_increment",
        "matched_return_increment": "matched_cumulative_net_return_increment",
        "cross_window_matched_consistency": "cross_window_matched_consistency",
    }
    output: dict[str, Any] = {"admitted_records": len(credits)}
    for label, key in keys.items():
        values = [_finite_metric(row.get(key), key) for row in credits]
        output[f"{label}_mean"] = float(mean(values)) if values else None
        output[f"{label}_median"] = float(median(values)) if values else None
    return output


def evaluate_prospective_canary_v1(
    feedback_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Apply the frozen dual gate to immutable Search V2 feedback rows."""

    compared = {
        arm: [
            dict(row)
            for row in feedback_rows
            if str(row.get("template_id") or "") != "BASE"
            and str(row.get("generation_arm") or "") == arm
        ]
        for arm in (ARM_UNIFORM, ARM_CONDITIONAL_UPLIFT)
    }
    absolute = {arm: _absolute_summary(rows) for arm, rows in compared.items()}
    conditional = {
        arm: _conditional_summary(rows) for arm, rows in compared.items()
    }
    exact_counts = PROSPECTIVE_SUCCESS_GATES["absolute_noninferiority"][
        "required_exact_records_per_arm"
    ]
    absolute_checks = {
        "exact_record_support": all(
            absolute[arm]["records"] == int(exact_counts[arm]) for arm in exact_counts
        ),
        "per_template_support": all(
            sum(str(row.get("template_id") or "") == template_id for row in compared[arm])
            == quota
            for template_id in TEMPLATE_ORDER
            if template_id != "BASE"
            for arm, quota in ((ARM_UNIFORM, 32), (ARM_CONDITIONAL_UPLIFT, 24))
        ),
        "admission_rate_noninferior": absolute[ARM_CONDITIONAL_UPLIFT][
            "admission_rate"
        ]
        >= absolute[ARM_UNIFORM]["admission_rate"],
        "primary_reward_positive_rate_noninferior": absolute[
            ARM_CONDITIONAL_UPLIFT
        ]["primary_reward_positive_rate"]
        >= absolute[ARM_UNIFORM]["primary_reward_positive_rate"],
        "primary_return_positive_rate_noninferior": absolute[
            ARM_CONDITIONAL_UPLIFT
        ]["primary_return_positive_rate"]
        >= absolute[ARM_UNIFORM]["primary_return_positive_rate"],
        "stability_noninferior": absolute[ARM_CONDITIONAL_UPLIFT][
            "stable_two_of_three_rate"
        ]
        >= absolute[ARM_UNIFORM]["stable_two_of_three_rate"],
        "blocker_rate_noninferior": absolute[ARM_CONDITIONAL_UPLIFT]["blocker_rate"]
        <= absolute[ARM_UNIFORM]["blocker_rate"],
        "turnover_efficiency_noninferior": (
            absolute[ARM_CONDITIONAL_UPLIFT]["median_return_per_turnover"]
            is not None
            and absolute[ARM_UNIFORM]["median_return_per_turnover"] is not None
            and absolute[ARM_CONDITIONAL_UPLIFT]["median_return_per_turnover"]
            >= absolute[ARM_UNIFORM]["median_return_per_turnover"]
        ),
    }
    minimum_admitted = int(
        PROSPECTIVE_SUCCESS_GATES["conditional_superiority"][
            "minimum_admitted_records_per_arm"
        ]
    )
    minimum_template_support = int(
        PROSPECTIVE_SUCCESS_GATES["conditional_superiority"][
            "minimum_templates_with_eight_admitted_per_arm"
        ]
    )
    supported_templates = 0
    improved_templates = 0
    for template_id in TEMPLATE_ORDER:
        if template_id == "BASE":
            continue
        by_arm = {
            arm: [
                row
                for row in compared[arm]
                if str(row.get("template_id") or "") == template_id
            ]
            for arm in compared
        }
        summaries = {
            arm: _conditional_summary(rows) for arm, rows in by_arm.items()
        }
        if all(summary["admitted_records"] >= 8 for summary in summaries.values()):
            supported_templates += 1
            strict_keys = (
                "matched_reward_increment_mean",
                "matched_reward_increment_median",
                "matched_return_increment_mean",
                "matched_return_increment_median",
                "cross_window_matched_consistency_mean",
            )
            if all(
                summaries[ARM_CONDITIONAL_UPLIFT][key]
                > summaries[ARM_UNIFORM][key]
                for key in strict_keys
            ):
                improved_templates += 1
    strict_global_keys = (
        "matched_reward_increment_mean",
        "matched_reward_increment_median",
        "matched_return_increment_mean",
        "matched_return_increment_median",
        "cross_window_matched_consistency_mean",
    )
    conditional_checks = {
        "global_admitted_support": all(
            conditional[arm]["admitted_records"] >= minimum_admitted
            for arm in conditional
        ),
        "template_admitted_support": supported_templates >= minimum_template_support,
        "minimum_improved_templates": improved_templates
        >= int(
            PROSPECTIVE_SUCCESS_GATES["conditional_superiority"][
                "minimum_improved_templates"
            ]
        ),
        "strict_global_superiority": all(
            conditional[ARM_CONDITIONAL_UPLIFT][key]
            is not None
            and conditional[ARM_UNIFORM][key] is not None
            and conditional[ARM_CONDITIONAL_UPLIFT][key]
            > conditional[ARM_UNIFORM][key]
            for key in strict_global_keys
        ),
    }
    passed = all(absolute_checks.values()) and all(conditional_checks.values())
    result = {
        "schema_version": "cn_search_engine_v2_prospective_gate_result_v1",
        "decision_rule": PROSPECTIVE_SUCCESS_GATES["decision_rule"],
        "decision": "PASS" if passed else "FAIL",
        "absolute": absolute,
        "conditional": conditional,
        "absolute_checks": absolute_checks,
        "conditional_checks": conditional_checks,
        "supported_templates": supported_templates,
        "improved_templates": improved_templates,
        "tradeoff_between_heads_allowed": False,
        "promotion_authorized": False,
    }
    result["gate_result_sha256"] = stable_hash(result)
    return result


def canary_provenance_v1() -> dict[str, Any]:
    return build_development_feedback_provenance(
        serialized_optimizer_state_imported=False,
        development_financial_observations_imported=False,
        development_observation_count=0,
        candidate_results_imported=False,
        factor_statistics_imported=False,
        behavior_statistics_imported=False,
        template_classification_imported=False,
        manual_diagnosis_imported=True,
        objective_designed_after_parent_results=True,
    )


def generation_arm_v1(template_id: str, template_record_ordinal: int) -> str:
    ordinal = int(template_record_ordinal)
    if template_id not in TEMPLATE_ORDER or ordinal not in range(RECORDS_PER_TEMPLATE):
        raise ValueError("Search V2 template or ordinal is out of range")
    if template_id == "BASE":
        return ARM_UNIFORM
    return ENHANCED_ARM_PATTERN[ordinal % RECORDS_PER_CHECKPOINT]


def build_ask_plan_v1() -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    for template_id in TEMPLATE_ORDER:
        for template_ordinal in range(RECORDS_PER_TEMPLATE):
            ordinal = len(rows)
            arm = generation_arm_v1(template_id, template_ordinal)
            row = {
                "schema_version": "cn_search_engine_v2_canary_ask_v1",
                "main_record_ordinal": ordinal,
                "checkpoint_ordinal": ordinal // RECORDS_PER_CHECKPOINT,
                "template_id": template_id,
                "template_record_ordinal": template_ordinal,
                "generation_arm": arm,
                "baseline_comparison_member": bool(
                    template_id != "BASE" and arm == ARM_UNIFORM
                ),
                "absolute_admission_head_eligible": template_id != "BASE",
                "conditional_uplift_head_eligible": template_id != "BASE",
                "adaptive_budget_reallocation_allowed": False,
                "early_template_cancellation_allowed": False,
                "maximum_variants_per_base_per_template": (
                    MAX_VARIANTS_PER_BASE_PER_TEMPLATE
                ),
                "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
                "canary_profile": CANARY_PROFILE,
            }
            row["ask_record_sha256"] = stable_hash(row)
            rows.append(row)
    if len(rows) != EXPECTED_RECORDS:
        raise RuntimeError("Search V2 ask plan cardinality drift")
    return tuple(rows)


def authorization_payload_v1() -> dict[str, Any]:
    asks = build_ask_plan_v1()
    arm_counts = Counter(str(row["generation_arm"]) for row in asks)
    template_counts = Counter(str(row["template_id"]) for row in asks)
    payload = {
        "schema_version": CANARY_AUTHORIZATION_SCHEMA,
        "status": CANARY_AUTHORIZATION_STATUS,
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CANARY_PROFILE,
        "canary_status": "FROZEN_NOT_RUN",
        "execution_authorized": True,
        "authorized_host": "77O",
        "project_control_required": True,
        "project_control_route_id": ROUTE_ID,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY, ACTION_RECOVERY],
        "frozen_governance_baseline_sha": (
            "2d1f947b4d5a75b27810748da3c0e64f46286022"
        ),
        "exact_live_repo_sha_required": True,
        "main_record_count": EXPECTED_RECORDS,
        "checkpoint_count": CHECKPOINT_COUNT,
        "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
        "template_order": list(TEMPLATE_ORDER),
        "template_quotas": dict(sorted(template_counts.items())),
        "arm_quotas": dict(sorted(arm_counts.items())),
        "enhanced_template_arm_quotas": {
            ARM_UNIFORM: 32,
            ARM_CONDITIONAL_UPLIFT: 24,
            ARM_NOVELTY: 8,
        },
        "ask_plan_sha256": stable_hash(list(asks)),
        "ask_plan_generator": (
            "our_system_phase2.runtime.cn_joint_program_search_v2_canary:"
            "build_ask_plan_v1"
        ),
        "search_engine_architecture": {
            "absolute_economics_role": "ADMISSION_GATE_ONLY",
            "conditional_enhancer_uplift_role": "PROGRAM_LEVEL_SEARCH_CREDIT",
            "scalar_absolute_plus_uplift_reward": None,
            "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
            "formal_development_search_authority_replaced": False,
            "lifecycle": "EXPERIMENTAL",
        },
        "matched_evaluator_contract": MATCHED_EVALUATOR_CONTRACT,
        "prospective_success_gates": PROSPECTIVE_SUCCESS_GATES,
        "development_feedback_provenance": canary_provenance_v1(),
        "source_phase_b_outcome_path": (
            "runtime/run_plans/"
            "cn_joint_program_rolling_search_v0_phase_b_outcome_20260809.json"
        ),
        "source_phase_b_root": (
            "D:\\ChengboRemote\\runtime\\"
            "cn_joint_program_rolling_search_v0_phase_b_blocker_safe_recovery_"
            "20260809_bd771fd_10w"
        ),
        "source_phase_b_closure_file_sha256": (
            "14623ebc131b84660bd131b11268e40433ea1f219d1ed77caff2eb6ab147106a"
        ),
        "source_phase_b_closure_payload_sha256": (
            "0765c684c3f67ab592eb1006dbd1a5dd6e96dad7825fef251ac0d4f6ee36f3cf"
        ),
        "source_phase_b_financial_observations_imported": False,
        "source_phase_c_financial_observations_imported": False,
        "source_phase_d_financial_observations_imported": False,
        "serialized_optimizer_state_imported": False,
        "candidate_results_imported": False,
        "financial_data_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
        "automatic_successor_authorized": False,
    }
    payload["authorization_payload_sha256"] = stable_hash(payload)
    return payload


def verify_canary_authorization(path: Path) -> dict[str, Any]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop("authorization_payload_sha256", ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError("Search V2 canary authorization self-hash drift")
    expected = authorization_payload_v1()
    if payload != expected:
        raise ValueError("Search V2 canary authorization contract drift")
    if payload["ask_plan_sha256"] != stable_hash(list(build_ask_plan_v1())):
        raise ValueError("Search V2 canary ask-plan hash drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(
        "cn-joint-program-search-v2-canary",
        {ACTION_LAUNCH, ACTION_RETRY, ACTION_RECOVERY},
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--phase-b-freeze-root", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--accepted-field-manifest", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--root-finalization-recovery-from-repo-sha")
    parser.add_argument("--root-finalization-incident", type=Path)
    parser.add_argument("--root-finalization-deployment-manifest", type=Path)
    parser.add_argument("--checkpoint-recovery-from-repo-sha")
    parser.add_argument("--checkpoint-recovery-incident", type=Path)
    parser.add_argument("--checkpoint-recovery-diagnostic-audit", type=Path)
    parser.add_argument("--checkpoint-recovery-deployment-manifest", type=Path)
    parser.add_argument("--executor-workers", type=int, default=10)
    args = parser.parse_args(argv)
    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(
        admission, args.campaign_authorization
    )
    authorization = verify_canary_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("Search V2 verified authorization payload drift")
    action = str(admission.get("requested_action") or "")
    lineage = str(admission.get("execution_lineage_action") or action)
    if lineage not in {ACTION_LAUNCH, ACTION_RETRY} or (
        action != ACTION_RECOVERY and action != lineage
    ):
        raise ProjectControlDenied("Search V2 Project Control lineage drift")

    repo_root = Path(__file__).resolve().parents[3]
    verify_search_feedback_boundary(
        role_registry_path=(
            repo_root / "runtime/run_plans/evaluation_data_roles_v1.json"
        ),
        access_started_path=(
            repo_root
            / "runtime/run_plans/cn_historical_challenge_2023_access_started.json"
        ),
        outcome_path=(
            repo_root
            / "runtime/run_plans/"
            "cn_fixed10_historical_challenge_2023_outcome_20260806.json"
        ),
    )

    from scripts.run_cn_joint_program_search_v2_canary import run_authorized_canary

    result = run_authorized_canary(
        args,
        admission=admission,
        authorization=authorization,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
