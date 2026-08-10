from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_joint_program_phase_c_v0 as engine
from our_system_phase2.runtime.cn_joint_program_allocator_repair_canary_v0 import (
    BANDIT_POLICY_ID,
    BATCH_ID,
    CHECKPOINT_COUNT,
    EXPECTED_RECORDS,
    FREEZE_CLOSURE_NAME,
    MIN_BASE_IDENTITIES_PER_TEMPLATE,
    RECORDS_PER_CHECKPOINT,
    verify_prefinancial_freeze_v0,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    ProgramProposalReceiptV0,
)
from our_system_phase2.services.program_allocator_repair_bandit_v0 import (
    OUTCOME_FIELDS,
    ProgramAllocatorRepairBanditV0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


STATUS = "CN_JOINT_PROGRAM_ALLOCATOR_REPAIR_CANARY_COMPLETE"
CLOSURE_NAME = "CN_JOINT_PROGRAM_ALLOCATOR_REPAIR_CANARY_COMPLETE.json"


def _choose_entry(
    ask: Mapping[str, Any],
    entries: Sequence[Mapping[str, Any]],
    *,
    bandit: ProgramAllocatorRepairBanditV0,
    state: MutableMapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    template_id = str(ask["template_id"])
    arm = str(ask["generation_arm"])
    candidates = [
        dict(entry)
        for entry in entries
        if str(entry["reservoir"]["reservoir_record_sha256"])
        not in state["reservoir_ids"]
        and (
            template_id == "BASE"
            or str(entry["program_id"]) not in state["program_ids"]
        )
        and int(state["base_counts"][(template_id, str(entry["base_component_id"]))])
        < engine.MAX_VARIANTS_PER_BASE_PER_TEMPLATE
    ]
    require_new_base = (
        template_id != "BASE"
        and int(ask["template_record_ordinal"]) < MIN_BASE_IDENTITIES_PER_TEMPLATE
    )
    if require_new_base:
        candidates = [
            entry
            for entry in candidates
            if int(
                state["base_counts"][(template_id, str(entry["base_component_id"]))]
            )
            == 0
        ]
    if not candidates:
        raise RuntimeError(f"allocator-repair selection supply exhausted: {template_id}/{arm}")

    scored: list[tuple[tuple[Any, ...], dict[str, Any], dict[str, Any]]] = []
    for entry in candidates:
        receipt = ProgramProposalReceiptV0.from_record(entry["score_receipt"])
        allocator = bandit.score_receipt(receipt)
        unseen_components = sum(
            component_id not in state["component_ids"]
            for component_id in entry["component_ids"]
        )
        unseen_combination = int(
            str(entry["combination_id"]) not in state["combination_ids"]
        )
        base_count = int(
            state["base_counts"][(template_id, str(entry["base_component_id"]))]
        )
        reservoir_ordinal = int(entry["reservoir"]["template_reservoir_ordinal"])
        if arm == "REVISED_EXPLOIT":
            key = (
                -int(bool(allocator["allocator_signal_available"])),
                *tuple(-float(value) for value in allocator["selection_rank"]),
                -int(allocator["eligible_factor_count"]),
                base_count,
                reservoir_ordinal,
                str(entry["reservoir"]["raw_combination_sha256"]),
            )
        elif arm == "NOVELTY_RESERVE":
            key = (
                -unseen_components,
                -unseen_combination,
                base_count,
                reservoir_ordinal,
                str(entry["reservoir"]["raw_combination_sha256"]),
            )
        elif arm == "UNIFORM_FRESH":
            key = (
                base_count,
                reservoir_ordinal,
                str(entry["reservoir"]["raw_combination_sha256"]),
            )
        else:
            raise RuntimeError(f"unknown allocator-repair generation arm: {arm}")
        metrics = {
            "allocator_signal_available": bool(
                allocator["allocator_signal_available"]
            ),
            "eligible_factor_count": int(allocator["eligible_factor_count"]),
            "selection_rank": list(allocator["selection_rank"]),
            "selection_hierarchy": list(OUTCOME_FIELDS),
            "unseen_component_count": int(unseen_components),
            "unseen_combination": bool(unseen_combination),
            "prior_base_variant_count": base_count,
        }
        scored.append((key, entry, metrics))
    _, selected, metrics = min(scored, key=lambda item: item[0])
    state["program_ids"].add(str(selected["program_id"]))
    state["reservoir_ids"].add(
        str(selected["reservoir"]["reservoir_record_sha256"])
    )
    state["base_counts"][(template_id, str(selected["base_component_id"]))] += 1
    state["component_ids"].update(selected["component_ids"])
    state["combination_ids"].add(str(selected["combination_id"]))
    decision = engine._self_hashed(
        {
            "schema_version": "cn_joint_program_allocator_repair_selection_v0",
            "main_record_ordinal": int(ask["main_record_ordinal"]),
            "template_id": template_id,
            "generation_arm": arm,
            "ask_record_sha256": str(ask["ask_record_sha256"]),
            "reservoir_record_sha256": str(
                selected["reservoir"]["reservoir_record_sha256"]
            ),
            "raw_combination_sha256": str(
                selected["reservoir"]["raw_combination_sha256"]
            ),
            "program_id": str(selected["program_id"]),
            "base_component_id": str(selected["base_component_id"]),
            "selection_metrics": metrics,
            "bandit_state_before_selection_sha256": str(
                bandit.snapshot()["bandit_state_sha256"]
            ),
            "adaptive_template_credit_used": bool(
                arm == "REVISED_EXPLOIT"
                and metrics["allocator_signal_available"]
            ),
            "adaptive_budget_reallocation_used": False,
        },
        "selection_decision_sha256",
    )
    return selected, decision


def _feedback_update(
    records: Sequence[Mapping[str, Any]],
    schedules: Sequence[Mapping[str, Any]],
    *,
    bandit: ProgramAllocatorRepairBanditV0,
    behavior_counts: Counter[str],
) -> list[dict[str, Any]]:
    schedules_by_ordinal = {
        int(row["main_record_ordinal"]): dict(row) for row in schedules
    }
    ledger: list[dict[str, Any]] = []
    for row in sorted(records, key=lambda item: int(item["main_record_ordinal"])):
        ordinal = int(row["main_record_ordinal"])
        schedule = schedules_by_ordinal[ordinal]
        updated = str(row["template_id"]) != "BASE"
        before = str(bandit.snapshot()["bandit_state_sha256"])
        outcome = None
        if updated:
            receipt = ProgramProposalReceiptV0.from_record(
                dict(schedule["proposal_receipt"])
            )
            outcome = bandit.observe_record(receipt, row)
        behavior_identity = str(
            (row.get("primary") or {}).get("behavior_identity") or ""
        )
        if behavior_identity:
            behavior_counts[behavior_identity] += 1
        replay_admissible = bool(outcome and outcome["replay_admissible"] > 0.0)
        ledger.append(
            engine._self_hashed(
                {
                    "schema_version": "cn_joint_program_allocator_repair_feedback_v0",
                    "main_record_ordinal": ordinal,
                    "template_id": str(row["template_id"]),
                    "generation_arm": str(row["generation_arm"]),
                    "record_payload_sha256": str(row["record_payload_sha256"]),
                    "proposal_receipt_sha256": str(
                        schedule["proposal_receipt"]["proposal_receipt_sha256"]
                    ),
                    "bandit_update_applied": updated,
                    "positive_credit_applied": bool(updated and replay_admissible),
                    "risk_observation_applied": bool(updated and not replay_admissible),
                    "reason": (
                        "BASE_PARITY_NOT_CREDIT_ELIGIBLE"
                        if not updated
                        else "ABSOLUTE_FIRST_OUTCOME_OBSERVED"
                        if replay_admissible
                        else "BLOCKED_OR_INADMISSIBLE_ZERO_CREDIT_RISK_OBSERVATION"
                    ),
                    "allocator_outcome": outcome,
                    "bandit_state_before_sha256": before,
                    "bandit_state_after_sha256": str(
                        bandit.snapshot()["bandit_state_sha256"]
                    ),
                    "validation_feedback_used": False,
                    "phase_c_financial_feedback_used": False,
                    "cross_campaign_state_imported": False,
                    "adaptive_budget_reallocation_used": False,
                },
                "feedback_record_sha256",
            )
        )
    return ledger


def _verify_run_contract(
    contract: Mapping[str, Any], *, executor_workers: int
) -> None:
    expected = {
        "main_record_count": EXPECTED_RECORDS,
        "checkpoint_count": CHECKPOINT_COUNT,
        "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
        "executor_backend": "PROCESS_POOL",
        "executor_workers": executor_workers,
        "executor_lifecycle": "CHECKPOINT_SCOPED_RECYCLE",
        "minimum_free_memory_bytes": engine.MINIMUM_FREE_MEMORY_BYTES,
        "portfolio_decoder_id": "TOPK_10_EQUAL",
        "initial_uniform_baseline_per_enhanced_template": 12,
        "maximum_variants_per_base_per_template": (
            engine.MAX_VARIANTS_PER_BASE_PER_TEMPLATE
        ),
        "minimum_base_identities_per_template": MIN_BASE_IDENTITIES_PER_TEMPLATE,
        "no_early_template_cancellation": True,
        "adaptive_budget_reallocation": False,
        "fixed_arm_floors": True,
        "bandit_policy_id": BANDIT_POLICY_ID,
        "blocked_positive_credit": False,
        "cross_campaign_optimizer_state_import": False,
        "route_local_tpe_authority_unchanged": True,
    }
    drift = [key for key, value in expected.items() if contract.get(key) != value]
    if drift:
        raise RuntimeError("allocator-repair frozen run contract drift: " + ",".join(drift))


def _configure_engine() -> None:
    engine.EXPECTED_RECORDS = EXPECTED_RECORDS
    engine.CHECKPOINT_COUNT = CHECKPOINT_COUNT
    engine.RECORDS_PER_CHECKPOINT = RECORDS_PER_CHECKPOINT
    engine.PHASE_C_BATCH_ID = BATCH_ID
    engine.FREEZE_CLOSURE_NAME = FREEZE_CLOSURE_NAME
    engine.STATUS = STATUS
    engine.CLOSURE_NAME = CLOSURE_NAME
    engine.CATALOG_MIN_RECORDS_PER_TEMPLATE = 32
    engine.ProgramFactorizedBanditV0 = ProgramAllocatorRepairBanditV0
    engine.verify_phase_c_prefinancial_freeze_v0 = verify_prefinancial_freeze_v0
    engine._choose_entry = _choose_entry
    engine._feedback_update = _feedback_update
    engine._verify_run_contract = _verify_run_contract


def run(args: argparse.Namespace) -> dict[str, Any]:
    _configure_engine()
    base_args = argparse.Namespace(**vars(args))
    base_args.phase_c_freeze_root = args.freeze_root
    closure = engine.run(base_args)
    body = dict(closure)
    body.pop("closure_payload_sha256", None)
    body.update(
        {
            "schema_version": "cn_joint_program_allocator_repair_canary_closure_v0",
            "status": STATUS,
            "experiment_profile": "ALLOCATOR_REPAIR_PROSPECTIVE_DEVELOPMENT_ONLY",
            "allocator_policy_id": BANDIT_POLICY_ID,
            "phase_c_financial_records_reused": False,
            "adaptive_budget_reallocation": False,
            "phase_d_launched": False,
        }
    )
    closed = engine._self_hashed(body, "closure_payload_sha256")
    return engine._read_json(engine._write_json(args.output_root / CLOSURE_NAME, closed))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--freeze-root", required=True, type=Path)
    parser.add_argument("--execution-contract", required=True, type=Path)
    parser.add_argument("--train-field-root", required=True, type=Path)
    parser.add_argument("--train-price-root", required=True, type=Path)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--node-resource-capacity", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--builder-commit-sha", required=True)
    parser.add_argument("--root-finalization-recovery-from-repo-sha")
    parser.add_argument("--root-finalization-incident", type=Path)
    parser.add_argument("--root-finalization-deployment-manifest", type=Path)
    parser.add_argument("--executor-workers", type=int, default=10)
    args = parser.parse_args(argv)
    if len(str(args.builder_commit_sha)) != 40:
        parser.error("builder-commit-sha must be a full Git SHA")
    closure = run(args)
    print(json.dumps(closure, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
