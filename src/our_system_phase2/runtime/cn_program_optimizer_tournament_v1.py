"""Frozen one-shot prospective Program optimizer tournament entry.

This module defines the immutable design and the Project Control entrance.  It
does not grant or request an admission and importing it performs no data read.
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import TEMPLATE_ORDER
from our_system_phase2.services.evaluation_asset_authority import (
    verify_search_feedback_boundary,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    PROGRAM_OPTIMIZER_ARMS,
    PROGRAM_SPACE_ID,
    STRUCTURED_SURROGATE_PROGRAM,
    UNIFORM_CONTROL,
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


ROUTE_ID = "cn-program-optimizer-tournament-v1"
CAMPAIGN_ID = "CN_PROGRAM_OPTIMIZER_TOURNAMENT_V1"
CAMPAIGN_PROFILE = "cn_program_optimizer_tournament_v1"
BATCH_ID = "CN_PROGRAM_OPTIMIZER_TOURNAMENT_FROZEN_V1"
AUTHORIZATION_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_tournament_v1.json"
)
AUTHORIZATION_SCHEMA = "cn_program_optimizer_tournament_authorization_v1"
AUTHORIZATION_STATUS = "PROGRAM_OPTIMIZER_TOURNAMENT_FROZEN_NOT_RUN"
RECORDS_PER_CHECKPOINT = 8
ENHANCED_TEMPLATES = tuple(
    template_id for template_id in TEMPLATE_ORDER if template_id != "BASE"
)
BASE_PARITY_RECORDS = 32
STAGE0_PER_TEMPLATE_PER_ARM = 8
STAGE1_PER_TEMPLATE_PER_ARM = 8
STAGE2_UNIFORM_PER_TEMPLATE = 8
STAGE2_SMART_PER_TEMPLATE = 8
MAXIMUM_ENHANCED_RECORDS = (
    len(ENHANCED_TEMPLATES)
    * (
        len(PROGRAM_OPTIMIZER_ARMS)
        * (STAGE0_PER_TEMPLATE_PER_ARM + STAGE1_PER_TEMPLATE_PER_ARM)
        + STAGE2_UNIFORM_PER_TEMPLATE
        + 2 * STAGE2_SMART_PER_TEMPLATE
    )
)
MAXIMUM_TOTAL_RECORDS = BASE_PARITY_RECORDS + MAXIMUM_ENHANCED_RECORDS
SEEDS = {
    UNIFORM_CONTROL: 721003,
    HYBRID_TPE_PROGRAM: 721013,
    STRUCTURED_SURROGATE_PROGRAM: 721019,
}
TPE_CONFIG = {
    "n_startup_trials": 24,
    "n_ei_candidates": 64,
    "multivariate": True,
    "group": True,
    "constant_liar": True,
    "constraints_enabled": True,
    "legal_program_representation": (
        "TEMPLATE_CONDITIONAL_EXACT_IDENTITY_CATEGORY_V1"
    ),
    "categorical_distance": (
        "NORMALIZED_HAMMING_OVER_FULL_FROZEN_PROGRAM_GENES_V1"
    ),
    "unavailable_exact_projection": (
        "FULL_LEGAL_SET_MINIMUM_STRUCTURAL_DISTANCE_V1"
    ),
    "global_fallback_allowed": False,
}
SURROGATE_CONFIG = {
    "implementation": "sklearn.ensemble.ExtraTreesClassifier+ExtraTreesRegressor",
    "cold_start_asks": 24,
    "candidate_pool_size": 256,
    "candidate_pool_semantics": (
        "INFERENCE_BATCH_SIZE_ONLY_FULL_ELIGIBLE_SET_ALWAYS_SCORED"
    ),
    "n_estimators": 256,
    "min_samples_leaf": 2,
    "exploration_beta": 1.0,
    "n_jobs": 1,
}
COMMON_PROGRAM_SPACE_CONTRACT = {
    "program_space_id": PROGRAM_SPACE_ID,
    "source_component_authority": "SEARCH_V2_PHASE_B_SESSION_EXECUTABLE_COMPONENT_POOL",
    "source_raw_program_authority": "SEARCH_V2_PREFINANCIAL_RAW_PROGRAM_RESERVOIR",
    "program_gene_builder": (
        "our_system_phase2.services.program_search_optimizer_v1:"
        "program_structural_genes_v1"
    ),
    "availability_entry_builder": (
        "our_system_phase2.services.program_search_optimizer_v1:"
        "program_availability_entries_v1"
    ),
    "same_exact_entries_for_all_arms": True,
    "candidate_program_composer": "CandidateProgramProposalAdapterV0.compose",
    "compiler": "ProgramCompilerV1.compile",
    "materializer": "prepare_cn_program_materialized_session_sidecar_v1",
    "evaluator": "PHASE3CM_STREAMING_MULTICANDIDATE_EVALUATOR",
    "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
}
COMMON_PROGRAM_SPACE_CONTRACT_SHA256 = stable_hash(COMMON_PROGRAM_SPACE_CONTRACT)
FROZEN_PROGRAM_SPACE_ENTRY_COUNT = 3616
FROZEN_PROGRAM_SPACE_SHA256 = (
    "86d9bce8f7bdc75e55c791b6e101ec4beefe093e346abfc39c76ee1c30f354e0"
)
FROZEN_PROGRAM_SPACE_SOURCE_SHA256 = {
    "raw_program_reservoir": (
        "c284a095171b3634083484a0e810d8a8bbc6876328adf1972dc473e4ae7f5608"
    ),
    "session_executable_component_pool": (
        "64ff0e47dab2d92a2bfc53049bf2fc9d42ae2bc757c1dc5e81139e059ee9d12e"
    ),
    "unified_capability_registry": (
        "449fea36daaba8e501bd03d052497b881ac03c601cee701f3ebfe069c7ae61d7"
    ),
}


def development_feedback_provenance_v1() -> dict[str, Any]:
    return {
        "manual_diagnosis_imported": True,
        "objective_designed_after_parent_results": True,
        "cross_campaign_development_feedback": True,
        "serialized_optimizer_state_imported": False,
        "development_financial_observations_imported": False,
        "development_observation_count": 0,
        "candidate_results_imported": False,
        "factor_statistics_imported": False,
        "behavior_statistics_imported": False,
        "template_classification_imported": False,
    }


def _stage_rows(
    *, stage: int, per_template_by_arm: Mapping[str, int]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for arm in PROGRAM_OPTIMIZER_ARMS:
        per_template = int(per_template_by_arm.get(arm, 0))
        for template_id in ENHANCED_TEMPLATES:
            for block_start in range(0, per_template, RECORDS_PER_CHECKPOINT):
                block_size = min(
                    RECORDS_PER_CHECKPOINT, per_template - block_start
                )
                for local_offset in range(block_size):
                    rows.append(
                        {
                            "schema_version": "cn_program_optimizer_tournament_ask_v1",
                            "stage": int(stage),
                            "optimizer_arm": arm,
                            "template_id": template_id,
                            "template_stage_ordinal": block_start + local_offset,
                            "absolute_admission_head_eligible": True,
                            "conditional_uplift_head_eligible": True,
                            "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
                            "program_level_credit_only": True,
                            "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
                        }
                    )
    return rows


def build_maximum_ask_plan_v1() -> tuple[dict[str, Any], ...]:
    rows = [
        {
            "schema_version": "cn_program_optimizer_tournament_ask_v1",
            "stage": 0,
            "optimizer_arm": UNIFORM_CONTROL,
            "template_id": "BASE",
            "template_stage_ordinal": ordinal,
            "absolute_admission_head_eligible": False,
            "conditional_uplift_head_eligible": False,
            "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
            "program_level_credit_only": True,
            "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
        }
        for ordinal in range(BASE_PARITY_RECORDS)
    ]
    rows.extend(
        _stage_rows(
            stage=0,
            per_template_by_arm={
                arm: STAGE0_PER_TEMPLATE_PER_ARM for arm in PROGRAM_OPTIMIZER_ARMS
            },
        )
    )
    rows.extend(
        _stage_rows(
            stage=1,
            per_template_by_arm={
                arm: STAGE1_PER_TEMPLATE_PER_ARM for arm in PROGRAM_OPTIMIZER_ARMS
            },
        )
    )
    rows.extend(
        _stage_rows(
            stage=2,
            per_template_by_arm={
                UNIFORM_CONTROL: STAGE2_UNIFORM_PER_TEMPLATE,
                HYBRID_TPE_PROGRAM: STAGE2_SMART_PER_TEMPLATE,
                STRUCTURED_SURROGATE_PROGRAM: STAGE2_SMART_PER_TEMPLATE,
            },
        )
    )
    if len(rows) != MAXIMUM_TOTAL_RECORDS:
        raise RuntimeError("PROGRAM_TOURNAMENT_MAXIMUM_BUDGET_DRIFT")
    output = []
    for ordinal, source in enumerate(rows):
        row = {
            **source,
            "main_record_ordinal": ordinal,
            "checkpoint_ordinal": ordinal // RECORDS_PER_CHECKPOINT,
            "campaign_profile": CAMPAIGN_PROFILE,
        }
        row["ask_record_sha256"] = stable_hash(row)
        output.append(row)
    return tuple(output)


RACING_RULES = {
    "comparison_boundary": "AFTER_STAGE_1_ONLY",
    "manual_change_after_financial_read_allowed": False,
    "uniform_control_stage_2_floor_per_template": STAGE2_UNIFORM_PER_TEMPLATE,
    "smart_arm_stage_2_maximum_per_template": STAGE2_SMART_PER_TEMPLATE,
    "winner_takes_all_allowed": False,
    "minimum_support_before_pruning": {
        "evaluated_per_enhanced_template_per_arm": 16,
        "admitted_global_per_arm": 48,
        "templates_with_four_admitted_per_arm": 5,
    },
    "futility_rule": {
        "confidence_interval": "ONE_SIDED_95_PERCENT_WILSON",
        "absolute_admission_noninferiority_margin": -0.05,
        "positive_conditional_uplift_efficiency_margin": -0.05,
        "stop_smart_arm_only_if": (
            "SUPPORT_MET_AND_EITHER_SMART_ADMISSION_UPPER_LT_"
            "UNIFORM_LOWER_MINUS_0P05_OR_SMART_PRODUCTIVE_UPPER_LT_"
            "UNIFORM_LOWER_MINUS_0P05"
        ),
        "insufficient_support_action": "CONTINUE_TO_STAGE_2_WITHIN_FROZEN_CAP",
    },
    "stage_2_allocation": (
        "UNIFORM_FLOOR_ALWAYS_PLUS_EACH_NONFUTILE_SMART_ARM_FULL_STAGE2_QUOTA"
    ),
}


def _wilson_bound(successes: int, total: int, *, upper: bool) -> float:
    if total <= 0:
        return 1.0 if upper else 0.0
    z = 1.6448536269514722
    rate = successes / total
    denominator = 1.0 + z * z / total
    center = rate + z * z / (2.0 * total)
    radius = z * math.sqrt(
        rate * (1.0 - rate) / total + z * z / (4.0 * total * total)
    )
    return (center + radius if upper else center - radius) / denominator


def freeze_racing_decision_v1(
    feedback_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Apply the frozen Stage-1 futility rule without a manual decision point."""

    relevant = [
        dict(row)
        for row in feedback_rows
        if str(row.get("template_id") or "") != "BASE"
    ]
    summaries: dict[str, dict[str, Any]] = {}
    for arm in PROGRAM_OPTIMIZER_ARMS:
        rows = [row for row in relevant if str(row.get("generation_arm")) == arm]
        admitted = [
            row
            for row in rows
            if bool(dict(row.get("absolute_admission") or {}).get("admitted"))
        ]
        productive = []
        for row in admitted:
            credit = dict(row.get("enhancer_credit") or {})
            program_credit = dict(credit.get("program_credit") or {})
            if (
                float(program_credit.get("matched_net_reward_increment", 0.0)) > 0.0
                and float(
                    program_credit.get("matched_cumulative_net_return_increment", 0.0)
                )
                > 0.0
            ):
                productive.append(row)
        per_template = {
            template_id: {
                "evaluated": sum(
                    str(row.get("template_id")) == template_id for row in rows
                ),
                "admitted": sum(
                    str(row.get("template_id")) == template_id for row in admitted
                ),
            }
            for template_id in ENHANCED_TEMPLATES
        }
        summaries[arm] = {
            "evaluated": len(rows),
            "admitted": len(admitted),
            "productive_positive_uplift": len(productive),
            "admission_rate": len(admitted) / len(rows) if rows else 0.0,
            "productive_efficiency": len(productive) / len(rows) if rows else 0.0,
            "per_template": per_template,
            "admission_wilson_one_sided_95": {
                "lower": _wilson_bound(len(admitted), len(rows), upper=False),
                "upper": _wilson_bound(len(admitted), len(rows), upper=True),
            },
            "productive_wilson_one_sided_95": {
                "lower": _wilson_bound(len(productive), len(rows), upper=False),
                "upper": _wilson_bound(len(productive), len(rows), upper=True),
            },
        }

    uniform = summaries[UNIFORM_CONTROL]
    active = [UNIFORM_CONTROL]
    decisions = {
        UNIFORM_CONTROL: {
            "stage2_active": True,
            "reason": "FROZEN_UNIFORM_CONTROL_FLOOR",
        }
    }
    support_rule = RACING_RULES["minimum_support_before_pruning"]
    margin = abs(
        float(
            RACING_RULES["futility_rule"][
                "absolute_admission_noninferiority_margin"
            ]
        )
    )
    for arm in (HYBRID_TPE_PROGRAM, STRUCTURED_SURROGATE_PROGRAM):
        summary = summaries[arm]
        support_met = (
            all(
                int(row["evaluated"])
                >= int(support_rule["evaluated_per_enhanced_template_per_arm"])
                for row in summary["per_template"].values()
            )
            and int(summary["admitted"])
            >= int(support_rule["admitted_global_per_arm"])
            and sum(
                int(summary["per_template"][template]["admitted"]) >= 4
                and int(uniform["per_template"][template]["admitted"]) >= 4
                for template in ENHANCED_TEMPLATES
            )
            >= int(support_rule["templates_with_four_admitted_per_arm"])
        )
        admission_futile = (
            float(summary["admission_wilson_one_sided_95"]["upper"])
            < float(uniform["admission_wilson_one_sided_95"]["lower"]) - margin
        )
        productive_futile = (
            float(summary["productive_wilson_one_sided_95"]["upper"])
            < float(uniform["productive_wilson_one_sided_95"]["lower"]) - margin
        )
        futile = support_met and (admission_futile or productive_futile)
        decisions[arm] = {
            "support_met": support_met,
            "admission_futile": admission_futile,
            "productive_efficiency_futile": productive_futile,
            "stage2_active": not futile,
            "reason": (
                "FROZEN_FUTILITY_STOP"
                if futile
                else "FROZEN_CONTINUE_NONFUTILE_OR_INSUFFICIENT_SUPPORT"
            ),
        }
        if not futile:
            active.append(arm)
    payload = {
        "schema_version": "cn_program_optimizer_tournament_racing_decision_v1",
        "status": "FROZEN_STAGE_1_RACING_DECISION_COMPLETE",
        "campaign_id": CAMPAIGN_ID,
        "source_feedback_rows_sha256": stable_hash(relevant),
        "source_feedback_row_count": len(relevant),
        "rules": RACING_RULES,
        "arm_summaries": summaries,
        "arm_decisions": decisions,
        "stage2_active_arms": active,
        "manual_change_after_financial_read_allowed": False,
        "automatic_successor_authorized": False,
        "promotion_authorized": False,
    }
    payload["racing_decision_sha256"] = stable_hash(payload)
    return payload


def verify_racing_decision_v1(payload: Mapping[str, Any]) -> dict[str, Any]:
    decision = dict(payload)
    claimed = str(decision.pop("racing_decision_sha256", ""))
    if not claimed or stable_hash(decision) != claimed:
        raise ValueError("Program tournament racing decision self-hash drift")
    decision["racing_decision_sha256"] = claimed
    if (
        decision.get("schema_version")
        != "cn_program_optimizer_tournament_racing_decision_v1"
        or str(decision.get("campaign_id")) != CAMPAIGN_ID
        or UNIFORM_CONTROL not in tuple(decision.get("stage2_active_arms") or ())
        or bool(decision.get("manual_change_after_financial_read_allowed"))
    ):
        raise ValueError("Program tournament racing decision contract drift")
    return decision


def authorization_payload_v1() -> dict[str, Any]:
    asks = build_maximum_ask_plan_v1()
    payload = {
        "schema_version": AUTHORIZATION_SCHEMA,
        "status": AUTHORIZATION_STATUS,
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "batch_id": BATCH_ID,
        "execution_authorized": True,
        "tournament_status": "FROZEN_NOT_RUN",
        "authorized_host": "DESKTOP-77OPJ6F",
        "project_control_required": True,
        "project_control_route_id": ROUTE_ID,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "optimizer_arms": list(PROGRAM_OPTIMIZER_ARMS),
        "common_program_space_contract": COMMON_PROGRAM_SPACE_CONTRACT,
        "common_program_space_contract_sha256": (
            COMMON_PROGRAM_SPACE_CONTRACT_SHA256
        ),
        "frozen_program_space_entry_count": FROZEN_PROGRAM_SPACE_ENTRY_COUNT,
        "frozen_program_space_sha256": FROZEN_PROGRAM_SPACE_SHA256,
        "frozen_program_space_source_sha256": (
            FROZEN_PROGRAM_SPACE_SOURCE_SHA256
        ),
        "realized_program_space_hash_freeze_boundary": (
            "PREFINANCIAL_AFTER_EXACT_CATALOG_BUILD_BEFORE_MARKET_READ"
        ),
        "tpe_policy": TPE_CONFIG,
        "surrogate_policy": SURROGATE_CONFIG,
        "uniform_policy": {
            "implementation": "RouteLocalAvailabilityController.reserve_exact",
            "selection": "SEED_BOUND_HASH_PERMUTATION_WITHOUT_REPLACEMENT",
            "learning": False,
        },
        "seeds": SEEDS,
        "ask_tell_contract": (
            "ProgramSearchOptimizerAdapter->CandidateProgram->FullBaseEvaluator->"
            "AbsoluteAdmission->ConditionalUplift->ProgramOptimizerObservationV1"
        ),
        "absolute_admission_contract": "CN_SEARCH_V2_ABSOLUTE_ECONOMIC_ADMISSION_V1",
        "conditional_uplift_contract": (
            "CN_SEARCH_V2_CONDITIONAL_FULL_VS_BASE_UPLIFT_V1"
        ),
        "scalar_absolute_plus_uplift_reward": None,
        "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
        "racing_rules": RACING_RULES,
        "base_parity_records": BASE_PARITY_RECORDS,
        "maximum_total_records": MAXIMUM_TOTAL_RECORDS,
        "maximum_ask_plan_sha256": stable_hash(list(asks)),
        "maximum_ask_plan_generator": (
            "our_system_phase2.runtime.cn_program_optimizer_tournament_v1:"
            "build_maximum_ask_plan_v1"
        ),
        "template_order": list(TEMPLATE_ORDER),
        "maximum_arm_counts": dict(
            sorted(Counter(row["optimizer_arm"] for row in asks).items())
        ),
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "development_feedback_provenance": development_feedback_provenance_v1(),
        "source_search_v2_authorization_path": (
            "runtime/run_plans/cn_search_engine_v2_prospective_512_canary_v1.json"
        ),
        "source_phase_b_outcome_path": (
            "runtime/run_plans/"
            "cn_joint_program_rolling_search_v0_phase_b_outcome_20260809.json"
        ),
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


def verify_authorization(path: Path) -> dict[str, Any]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8-sig"))
    body = dict(payload)
    expected_hash = str(body.pop("authorization_payload_sha256", ""))
    if not expected_hash or stable_hash(body) != expected_hash:
        raise ValueError("Program tournament authorization self-hash drift")
    if payload != authorization_payload_v1():
        raise ValueError("Program tournament authorization contract drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(
        "cn-program-optimizer-tournament-v1", {ACTION_LAUNCH, ACTION_RETRY}
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--phase-b-freeze-root", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--accepted-field-manifest", type=Path, required=True)
    parser.add_argument("--information-metrics", type=Path, required=True)
    parser.add_argument("--bar-source-root", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--executor-workers", type=int, default=10)
    args = parser.parse_args(argv)
    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(
        admission, args.campaign_authorization
    )
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied(
            "Program tournament verified authorization payload drift"
        )
    if not bool(authorization.get("execution_authorized")):
        raise ProjectControlDenied(
            "Program tournament is frozen but has no execution authorization"
        )
    action = str(admission.get("requested_action") or "")
    lineage = str(admission.get("execution_lineage_action") or action)
    if action not in {ACTION_LAUNCH, ACTION_RETRY} or lineage != action:
        raise ProjectControlDenied("Program tournament Project Control lineage drift")
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
    from scripts.run_cn_program_optimizer_tournament_v1 import run_authorized_tournament

    result = run_authorized_tournament(
        args, admission=admission, authorization=authorization
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
