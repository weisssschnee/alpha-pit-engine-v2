"""Freeze Search Core V2 Stage-1 before any new development financial read.

Arm A is the confirmed Primitive V1 policy over the still-unspent remainder of
the already-frozen Stage-D supply.  Arm B is the Semantic State-Jump Generator
V2 with within-campaign development-only adaptation.  No MCTS/Evolution rerun is
included because those challengers already have sufficient development evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.runtime.cn_program_optimizer_successor_benchmark_v1 import (
    FROZEN_PROGRAM_SPACE_COUNT,
    FROZEN_PROGRAM_SPACE_SHA256,
    SOURCE_COMPONENT_POOL_SHA256,
    SOURCE_FREEZE_ROOT,
    SOURCE_RAW_RESERVOIR_SHA256,
)
from our_system_phase2.services.program_search_primitive_credit_v1 import (
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
    HierarchicalPrimitiveCreditScorerV1,
)
from our_system_phase2.services.program_search_state_jump_generator_v2 import (
    DEFAULT_OPERATION_PRIORS,
    SEMANTIC_STATE_JUMP_GENERATOR_V2,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


SCHEMA = "cn_search_core_v2_stage1_prefreeze_v1"
STATUS = "SEARCH_CORE_V2_STAGE1_PREFROZEN_BEFORE_FINANCIAL_READ"
TEMPLATES = (
    "BASE_EVENT",
    "BASE_MARKET",
    "BASE_MARKET_EVENT",
    "BASE_TEMPORAL",
    "BASE_TEMPORAL_EVENT",
    "BASE_TEMPORAL_MARKET",
    "BASE_TEMPORAL_MARKET_EVENT",
)
PER_TEMPLATE_BUDGET = 48
TOTAL_PER_ARM = len(TEMPLATES) * PER_TEMPLATE_BUDGET
STATE_JUMP_SEED = 826241
STATE_JUMP_MAXIMUM_ATTEMPTS = 192


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _verify_self(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _primitive_metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    genes = dict(row["program_genes"])
    components = {}
    for role, raw_binding in sorted(dict(row["components"]).items()):
        binding = dict(raw_binding)
        components[str(role)] = {
            "component_id": str(binding["component_id"]),
            "route_id": str(binding["route_id"]),
            "skeleton_id": str(genes[f"{role}__skeleton_id"]),
        }
    return {
        "template_id": str(row["template_id"]),
        "components": components,
        "tie_break_identity": str(row["exact_identity"]),
    }


def build(repo: Path, *, source_repo_sha: str) -> dict[str, Any]:
    repo = repo.resolve()
    supply_path = repo / "runtime/run_plans/cn_stage_d_expanded_supply_audit_cbaaaee_20260820.json"
    stage_d_path = repo / "runtime/run_plans/cn_program_stage_d_primitive_confirmation_prefreeze_v1.json"
    stats_path = repo / "runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json"
    stage_c_audit_path = repo / "runtime/run_plans/cn_program_stage_c_cbaaaee_independent_audit_20260820.json"
    stage_d_audit_path = repo / "runtime/run_plans/cn_program_stage_d_5e2dd3f_independent_audit_20260820.json"

    supply = _read(supply_path)
    supply_payload = _verify_self(supply, "audit_payload_sha256", "Stage-D supply")
    stage_d = _read(stage_d_path)
    stage_d_payload = _verify_self(stage_d, "prefreeze_payload_sha256", "Stage-D prefreeze")
    stats = _read(stats_path)
    stats_payload = _verify_self(stats, "stats_payload_sha256", "Primitive stats")
    stage_c_audit = _read(stage_c_audit_path)
    stage_d_audit = _read(stage_d_audit_path)

    if (
        supply.get("status") != "ZERO_FINANCIAL_STAGE_D_EXPANDED_SUPPLY_AUDIT_COMPLETE"
        or int(supply.get("fresh_unique_count") or 0) != 3584
        or int(supply.get("effective_spent_exact_count") or 0) != 4718
        or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("stage_c_financial_labels_read_by_builder"))
    ):
        raise RuntimeError("Stage-D supply contract drift")
    if (
        stage_d.get("status") != "STAGE_D_PRIMITIVE_CONFIRMATION_PREFROZEN_BEFORE_FINANCIAL_READ"
        or int(stage_d["cohort"]["total_records"]) != 2016
        or bool(stage_d.get("stage_d_financial_labels_read_during_freeze"))
    ):
        raise RuntimeError("Stage-D prefreeze contract drift")
    if stats.get("status") != "SPENT_DEVELOPMENT_PRIMITIVE_CREDIT_STATS_FROZEN":
        raise RuntimeError("Primitive stats contract drift")

    primitive_scorer = HierarchicalPrimitiveCreditScorerV1(
        primitive_stats=stats,
        primitive_stats_payload_sha256=stats_payload,
    )
    already_evaluated = {str(row["exact_identity"]) for row in stage_d["cohort"]["candidates"]}
    remaining_by_template = {template: [] for template in TEMPLATES}
    for source in supply["fresh_entries"]:
        exact = str(source["exact_identity"])
        if exact in already_evaluated:
            continue
        template = str(source["template_id"])
        if template not in remaining_by_template:
            raise RuntimeError(f"unexpected template: {template}")
        row = dict(source)
        score = primitive_scorer.score(_primitive_metadata(row))
        row["primitive_score"] = float(score["primitive_score"])
        row["novelty_score"] = float(score["novelty_score"])
        row["primitive_score_detail"] = score
        remaining_by_template[template].append(row)

    if any(len(rows) != 224 for rows in remaining_by_template.values()):
        raise RuntimeError("unspent Stage-D remainder geometry drift")

    baseline_selected: list[dict[str, Any]] = []
    baseline_orders: dict[str, list[str]] = {}
    for template in TEMPLATES:
        ordered = sorted(
            remaining_by_template[template],
            key=lambda row: (-float(row["primitive_score"]), str(row["exact_identity"])),
        )
        baseline_orders[template] = [str(row["exact_identity"]) for row in ordered]
        baseline_selected.extend(ordered[:PER_TEMPLATE_BUDGET])
    baseline_exacts = [str(row["exact_identity"]) for row in baseline_selected]
    if len(baseline_exacts) != TOTAL_PER_ARM or len(set(baseline_exacts)) != TOTAL_PER_ARM:
        raise RuntimeError("Stage-1 baseline exact coverage drift")
    if set(baseline_exacts).intersection(already_evaluated):
        raise RuntimeError("Stage-1 baseline reuses Stage-D financial exact")

    # Previous independent 336-budget Primitive results are used only as a
    # sanity band; they are not candidate labels and do not select this cohort.
    historical_sanity = {
        "stage_c_336": {
            "productive": int(stage_c_audit["budget336"]["static"]["PRIMITIVE_LOCAL_HIERARCHICAL_V1"]["productive"]),
            "distinct_behavior_pair_count": int(stage_c_audit["budget336"]["static"]["PRIMITIVE_LOCAL_HIERARCHICAL_V1"]["distinct_behavior_pair_count"]),
        },
        "stage_d_336": {
            "productive": int(stage_d_audit["budget336"]["primitive_local"]["productive"]),
            "distinct_behavior_pair_count": int(stage_d_audit["budget336"]["primitive_local"]["distinct_behavior_pair_count"]),
        },
    }
    if historical_sanity != {
        "stage_c_336": {"productive": 140, "distinct_behavior_pair_count": 251},
        "stage_d_336": {"productive": 139, "distinct_behavior_pair_count": 247},
    }:
        raise RuntimeError("historical Primitive 336 sanity band drift")

    code_paths = {
        "state_jump_generator": repo / "src/our_system_phase2/services/program_search_state_jump_generator_v2.py",
        "state_jump_adapter": repo / "src/our_system_phase2/services/program_search_state_jump_adapter_v2.py",
        "search_core_state": repo / "src/our_system_phase2/services/program_optimizer_search_core_v2.py",
        "primitive_scorer": repo / "src/our_system_phase2/services/program_search_primitive_credit_v1.py",
        "program_proposal": repo / "src/our_system_phase2/services/candidate_program_proposal_v0.py",
        "program_compiler": repo / "src/our_system_phase2/services/candidate_program_v1.py",
    }
    code_hashes = {name: _sha(path) for name, path in code_paths.items()}

    payload: dict[str, Any] = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "source_repo_sha": str(source_repo_sha),
        "hypothesis": "TRUE_SEMANTIC_STATE_JUMP_GENERATION_IMPROVES_FRESH_DEVELOPMENT_DISCOVERY_EFFICIENCY_VS_CONFIRMED_PRIMITIVE_BASELINE",
        "stage1": {
            "arms": [
                PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
                SEMANTIC_STATE_JUMP_GENERATOR_V2,
            ],
            "per_template_budget_per_arm": PER_TEMPLATE_BUDGET,
            "total_budget_per_arm": TOTAL_PER_ARM,
            "total_financial_evaluations": 2 * TOTAL_PER_ARM,
            "templates": list(TEMPLATES),
            "checkpoint_batch_size": 24,
            "checkpoints_per_template_per_arm": 2,
            "automatic_stage2_launch": False,
        },
        "arm_a_primitive": {
            "policy_id": PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
            "source_supply_relative_path": str(supply_path.relative_to(repo)).replace("\\", "/"),
            "source_supply_file_sha256": _sha(supply_path),
            "source_supply_payload_sha256": supply_payload,
            "stage_d_prefreeze_relative_path": str(stage_d_path.relative_to(repo)).replace("\\", "/"),
            "stage_d_prefreeze_file_sha256": _sha(stage_d_path),
            "stage_d_prefreeze_payload_sha256": stage_d_payload,
            "unspent_candidate_count": 1568,
            "unspent_per_template": 224,
            "eligible_orders_by_template": baseline_orders,
            "selected_exact_identities": baseline_exacts,
            "selected_exact_identities_sha256": stable_hash(baseline_exacts),
            "selected_count": len(baseline_exacts),
            "candidate_labels_read_during_selection": False,
        },
        "arm_b_state_jump": {
            "policy_id": SEMANTIC_STATE_JUMP_GENERATOR_V2,
            "seed": STATE_JUMP_SEED,
            "maximum_attempts": STATE_JUMP_MAXIMUM_ATTEMPTS,
            "operation_priors": dict(DEFAULT_OPERATION_PRIORS),
            "initial_memory": "EMPTY_CAMPAIGN_LOCAL_DEVELOPMENT_ONLY",
            "within_campaign_ask_tell_adaptation": True,
            "cross_campaign_reward_memory": False,
            "candidate_exact_membership_prefrozen": False,
            "candidate_generation_contract_prefrozen": True,
            "must_be_outside_all_previously_seen_normalized_exact_identities": True,
            "source_frozen_program_space_count": FROZEN_PROGRAM_SPACE_COUNT,
            "source_frozen_program_space_sha256": FROZEN_PROGRAM_SPACE_SHA256,
            "source_component_pool_sha256": SOURCE_COMPONENT_POOL_SHA256,
            "source_raw_reservoir_sha256": SOURCE_RAW_RESERVOIR_SHA256,
            "source_freeze_root": SOURCE_FREEZE_ROOT,
            "zero_financial_supply_audit_required_before_project_control": True,
        },
        "algorithm_code_sha256": code_hashes,
        "primitive_stats": {
            "relative_path": str(stats_path.relative_to(repo)).replace("\\", "/"),
            "file_sha256": _sha(stats_path),
            "payload_sha256": stats_payload,
        },
        "historical_336_sanity_band": historical_sanity,
        "stage1_decision_contract": {
            "primary_comparator": "SAME_RUN_ARM_A_VS_ARM_B",
            "clear_generator_win": (
                "B productive >= A*1.05 with behavior >= A*0.95, OR "
                "B behavior >= A*1.05 with productive >= A*0.97"
            ),
            "clear_generator_loss": (
                "B productive < A*0.95 AND B behavior <= A, OR "
                "B behavior < A*0.95 AND B productive <= A"
            ),
            "otherwise": "AMBIGUOUS_REVIEW_NO_AUTOMATIC_STAGE2",
            "historical_sanity_only": (
                "Arm A should remain near prior independent 336-budget Primitive band "
                "139-140 productive / 247-251 behavior pairs; material deviation triggers audit, not B rescue."
            ),
            "stage2_requires_separate_project_control_review": True,
        },
        "stage2_design_if_authorized": {
            "eligible_arms": "TOP_CLEAR_STAGE1_ARM_PLUS_PRIMITIVE_BASELINE_IF_GENERATOR_WINS",
            "target_total_budget_per_arm": 1008,
            "additional_budget_per_arm_after_stage1": 672,
        },
        "metrics": [
            "productive_count",
            "productive_rate",
            "stable_count",
            "stable_rate",
            "distinct_behavior_pair_count",
            "distinct_behavior_pair_rate",
            "new_structural_region_count",
            "duplicate_or_redundant_rate",
            "per_template_productive_count",
            "late_checkpoint_productive_yield",
            "wall_seconds_per_productive",
        ],
        "candidate_evaluation_executed": False,
        "financial_labels_read_by_builder": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
        "oos_authority": "NONE",
    }
    payload["prefreeze_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source-repo-sha", required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_search_core_v2_stage1_prefreeze_20260824.json"),
    )
    args = parser.parse_args(argv)
    payload = build(args.repo_root, source_repo_sha=args.source_repo_sha)
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
                "payload": payload["prefreeze_payload_sha256"],
                "arm_a_selected": payload["arm_a_primitive"]["selected_count"],
                "arm_b_budget": payload["stage1"]["total_budget_per_arm"],
                "total_financial_evaluations": payload["stage1"]["total_financial_evaluations"],
                "output": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
