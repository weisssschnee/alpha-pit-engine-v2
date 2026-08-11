"""Thin Search V2 adapter over the existing Joint Program execution engine."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_joint_program_phase_c_v0 as engine
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import (
    EXECUTOR_WORKERS,
    MINIMUM_FREE_MEMORY_BYTES,
    RESOURCE_PROFILE,
)
from our_system_phase2.runtime.cn_joint_program_search_v2_canary import (
    ARM_CONDITIONAL_UPLIFT,
    ARM_NOVELTY,
    ARM_UNIFORM,
    BATCH_ID,
    CAMPAIGN_ID,
    CANARY_PROFILE,
    CHECKPOINT_COUNT,
    EXPECTED_RECORDS,
    MAX_VARIANTS_PER_BASE_PER_TEMPLATE,
    MIN_BASE_IDENTITIES_PER_TEMPLATE,
    PROSPECTIVE_SUCCESS_GATES,
    RECORDS_PER_CHECKPOINT,
    evaluate_prospective_canary_v1,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    ProgramProposalReceiptV0,
)
from our_system_phase2.services.project_control_admission import ACTION_RECOVERY
from our_system_phase2.services.search_v2_admission import (
    AbsoluteEconomicAdmission,
)
from our_system_phase2.services.search_v2_canary_freeze import (
    FREEZE_CLOSURE_NAME,
    build_prefinancial_freeze_v1,
    verify_prefinancial_freeze_v1,
)
from our_system_phase2.services.search_v2_conditional_uplift import (
    conditional_uplift_credit,
)
from our_system_phase2.services.search_v2_scheduler import (
    SCHEDULER_POLICY_ID,
    SearchV2SchedulerV1,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


STATUS = "CN_JOINT_PROGRAM_SEARCH_V2_CANARY_COMPLETE"
CLOSURE_NAME = "CN_JOINT_PROGRAM_SEARCH_V2_CANARY_COMPLETE.json"
ENGINE_CLOSURE_NAME = "CN_JOINT_PROGRAM_SEARCH_V2_ENGINE_COMPLETE.json"


def _verified_feedback_rows(
    output_root: Path, closure: Mapping[str, Any]
) -> list[dict[str, Any]]:
    root = output_root.resolve()
    manifest_binding = dict(closure.get("artifact_manifest") or {})
    manifest_relative = str(
        manifest_binding.get("relative_path") or manifest_binding.get("path") or ""
    )
    manifest_size = int(
        manifest_binding.get("size_bytes")
        if manifest_binding.get("size_bytes") is not None
        else manifest_binding.get("bytes", -1)
    )
    manifest_path = (root / manifest_relative).resolve()
    if not manifest_path.is_relative_to(root):
        raise RuntimeError("Search V2 engine manifest escapes the admitted run root")
    manifest_raw = manifest_path.read_bytes()
    if (
        hashlib.sha256(manifest_raw).hexdigest()
        != str(manifest_binding.get("sha256") or "")
        or len(manifest_raw) != manifest_size
    ):
        raise RuntimeError("Search V2 engine manifest binding drift")
    manifest = json.loads(manifest_raw.decode("utf-8-sig"))
    manifest_body = dict(manifest)
    manifest_hash = str(manifest_body.pop("artifact_manifest_sha256", ""))
    if not manifest_hash or stable_hash(manifest_body) != manifest_hash:
        raise RuntimeError("Search V2 engine manifest self-hash drift")
    feedback_binding = next(
        (
            dict(row)
            for row in manifest.get("artifacts") or ()
            if str(row.get("relative_path") or row.get("path") or "")
            == "phase_c_bandit_feedback_ledger.jsonl"
        ),
        None,
    )
    if feedback_binding is None:
        raise RuntimeError("Search V2 feedback ledger is absent from engine manifest")
    feedback_relative = str(
        feedback_binding.get("relative_path") or feedback_binding.get("path") or ""
    )
    feedback_size = int(
        feedback_binding.get("size_bytes")
        if feedback_binding.get("size_bytes") is not None
        else feedback_binding.get("bytes", -1)
    )
    feedback_path = (root / feedback_relative).resolve()
    if not feedback_path.is_relative_to(root):
        raise RuntimeError("Search V2 feedback ledger escapes the admitted run root")
    feedback_raw = feedback_path.read_bytes()
    if (
        hashlib.sha256(feedback_raw).hexdigest()
        != str(feedback_binding.get("sha256") or "")
        or len(feedback_raw) != feedback_size
    ):
        raise RuntimeError("Search V2 feedback ledger binding drift")
    return [
        json.loads(line)
        for line in feedback_raw.decode("utf-8").splitlines()
        if line.strip()
    ]


def _choose_entry(
    ask: Mapping[str, Any],
    entries: Sequence[Mapping[str, Any]],
    *,
    bandit: SearchV2SchedulerV1,
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
        < MAX_VARIANTS_PER_BASE_PER_TEMPLATE
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
        raise RuntimeError(f"Search V2 selection supply exhausted: {template_id}/{arm}")

    scored: list[tuple[tuple[Any, ...], dict[str, Any], dict[str, Any]]] = []
    for entry in candidates:
        receipt = ProgramProposalReceiptV0.from_record(entry["score_receipt"])
        heads = bandit.score_receipt(receipt)
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
        if arm == ARM_CONDITIONAL_UPLIFT:
            key = (
                -int(bool(heads["conditional_uplift_exploit_eligible"])),
                *tuple(-float(value) for value in heads["acquisition_rank"]),
                base_count,
                reservoir_ordinal,
                str(entry["reservoir"]["raw_combination_sha256"]),
            )
        elif arm == ARM_NOVELTY:
            key = (
                -unseen_components,
                -unseen_combination,
                base_count,
                reservoir_ordinal,
                str(entry["reservoir"]["raw_combination_sha256"]),
            )
        elif arm == ARM_UNIFORM:
            key = (
                base_count,
                reservoir_ordinal,
                str(entry["reservoir"]["raw_combination_sha256"]),
            )
        else:
            raise RuntimeError(f"unknown Search V2 generation arm: {arm}")
        metrics = {
            "admission_head": dict(heads["admission_head"]),
            "conditional_uplift_head": dict(heads["conditional_uplift_head"]),
            "conditional_uplift_exploit_eligible": bool(
                heads["conditional_uplift_exploit_eligible"]
            ),
            "acquisition_order": list(heads["acquisition_order"]),
            "acquisition_rank": list(heads["acquisition_rank"]),
            "scalar_absolute_plus_uplift_reward": None,
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
            "schema_version": "cn_search_engine_v2_selection_decision_v1",
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
                arm == ARM_CONDITIONAL_UPLIFT
                and metrics["conditional_uplift_exploit_eligible"]
            ),
            "adaptive_budget_reallocation_used": False,
            "program_level_credit_only": True,
            "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
        },
        "selection_decision_sha256",
    )
    return selected, decision


def _feedback_update(
    records: Sequence[Mapping[str, Any]],
    schedules: Sequence[Mapping[str, Any]],
    *,
    bandit: SearchV2SchedulerV1,
    behavior_counts: Counter[str],
) -> list[dict[str, Any]]:
    schedules_by_ordinal = {
        int(row["main_record_ordinal"]): dict(row) for row in schedules
    }
    ledger: list[dict[str, Any]] = []
    for record in sorted(records, key=lambda row: int(row["main_record_ordinal"])):
        ordinal = int(record["main_record_ordinal"])
        schedule = schedules_by_ordinal[ordinal]
        before = str(bandit.snapshot()["bandit_state_sha256"])
        admission = None
        credit = None
        updated = str(record["template_id"]) != "BASE"
        if updated:
            admission = AbsoluteEconomicAdmission.evaluate(
                record,
                expected_pair_id=str(schedule["pair_id"]),
                expected_program_id=str(schedule["primary_program"]["program_id"]),
                expected_control_program_id=str(
                    schedule["control_program"]["program_id"]
                ),
            )
            credit = conditional_uplift_credit(record, admission)
            receipt = ProgramProposalReceiptV0.from_record(
                dict(schedule["proposal_receipt"])
            )
            bandit.observe(receipt, admission, credit)
        behavior_identity = str(
            (record.get("primary") or {}).get("behavior_identity") or ""
        )
        if behavior_identity:
            behavior_counts[behavior_identity] += 1
        ledger.append(
            engine._self_hashed(
                {
                    "schema_version": "cn_search_engine_v2_feedback_v1",
                    "main_record_ordinal": ordinal,
                    "template_id": str(record["template_id"]),
                    "generation_arm": str(record["generation_arm"]),
                    "record_payload_sha256": str(record["record_payload_sha256"]),
                    "proposal_receipt_sha256": str(
                        schedule["proposal_receipt"]["proposal_receipt_sha256"]
                    ),
                    "bandit_update_applied": updated,
                    "reason": (
                        "ABSOLUTE_ADMISSION_OBSERVED"
                        if updated
                        else "BASE_PARITY_NOT_SEARCH_V2_HEAD_ELIGIBLE"
                    ),
                    "admission_head_observation_applied": updated,
                    "absolute_admission": (
                        admission.to_record() if admission is not None else None
                    ),
                    "conditional_uplift_observation_applied": bool(
                        credit is not None
                    ),
                    "enhancer_credit": (
                        credit.to_record() if credit is not None else None
                    ),
                    "failed_candidate_negative_enhancer_reward": False,
                    "program_level_credit_only": True,
                    "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
                    "bandit_state_before_sha256": before,
                    "bandit_state_after_sha256": str(
                        bandit.snapshot()["bandit_state_sha256"]
                    ),
                    "validation_feedback_used": False,
                    "cross_campaign_state_imported": False,
                },
                "feedback_record_sha256",
            )
        )
    return ledger


def _verify_run_contract(
    contract: Mapping[str, Any], *, executor_workers: int
) -> None:
    expected = {
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CANARY_PROFILE,
        "main_record_count": EXPECTED_RECORDS,
        "checkpoint_count": CHECKPOINT_COUNT,
        "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
        "executor_backend": "PROCESS_POOL",
        "executor_workers": executor_workers,
        "executor_lifecycle": "CHECKPOINT_SCOPED_RECYCLE",
        "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "resource_profile": RESOURCE_PROFILE,
        "portfolio_decoder_id": "TOPK_10_EQUAL",
        "initial_uniform_baseline_per_enhanced_template": 32,
        "maximum_variants_per_base_per_template": (
            MAX_VARIANTS_PER_BASE_PER_TEMPLATE
        ),
        "minimum_base_identities_per_template": MIN_BASE_IDENTITIES_PER_TEMPLATE,
        "no_early_template_cancellation": True,
        "adaptive_budget_reallocation": False,
        "fixed_arm_floors": True,
        "bandit_policy_id": SCHEDULER_POLICY_ID,
        "absolute_admission_and_conditional_uplift_separate": True,
        "scalar_absolute_plus_uplift_reward": None,
        "component_credit_authoritative": False,
        "serialized_optimizer_state_imported": False,
        "development_financial_observations_imported": False,
        "development_observation_count": 0,
        "candidate_results_imported": False,
        "route_local_tpe_authority_unchanged": True,
        "formal_development_search_authority_replaced": False,
        "decision_gates": PROSPECTIVE_SUCCESS_GATES,
    }
    drift = [key for key, value in expected.items() if contract.get(key) != value]
    if drift:
        raise RuntimeError("Search V2 frozen run contract drift: " + ",".join(drift))


def _configure_engine() -> None:
    engine.EXPECTED_RECORDS = EXPECTED_RECORDS
    engine.CHECKPOINT_COUNT = CHECKPOINT_COUNT
    engine.RECORDS_PER_CHECKPOINT = RECORDS_PER_CHECKPOINT
    engine.PHASE_C_BATCH_ID = BATCH_ID
    engine.FREEZE_CLOSURE_NAME = FREEZE_CLOSURE_NAME
    engine.STATUS = STATUS
    engine.CLOSURE_NAME = ENGINE_CLOSURE_NAME
    engine.CATALOG_MIN_RECORDS_PER_TEMPLATE = 64
    engine.MINIMUM_FREE_MEMORY_BYTES = MINIMUM_FREE_MEMORY_BYTES
    engine.ProgramFactorizedBanditV0 = SearchV2SchedulerV1
    engine.verify_phase_c_prefinancial_freeze_v0 = verify_prefinancial_freeze_v1
    engine._choose_entry = _choose_entry
    engine._feedback_update = _feedback_update
    engine._verify_run_contract = _verify_run_contract


def run(args: argparse.Namespace) -> dict[str, Any]:
    _configure_engine()
    closure = engine.run(args)
    gate_result = evaluate_prospective_canary_v1(
        _verified_feedback_rows(args.output_root, closure)
    )
    body = dict(closure)
    body.pop("closure_payload_sha256", None)
    body.update(
        {
            "schema_version": "cn_joint_program_search_v2_canary_closure_v1",
            "status": STATUS,
            "experiment_profile": CANARY_PROFILE,
            "search_engine_v2": True,
            "absolute_economics_role": "ADMISSION_GATE_ONLY",
            "conditional_uplift_role": "PROGRAM_LEVEL_SEARCH_CREDIT",
            "scalar_absolute_plus_uplift_reward": None,
            "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
            "scheduler_policy_id": SCHEDULER_POLICY_ID,
            "prospective_success_gates": PROSPECTIVE_SUCCESS_GATES,
            "prospective_gate_result": gate_result,
            "phase_b_financial_observations_imported": False,
            "phase_c_financial_observations_imported": False,
            "phase_d_financial_observations_imported": False,
            "serialized_optimizer_state_imported": False,
            "adaptive_budget_reallocation": False,
            "automatic_successor_launch": False,
            "formal_development_search_authority_replaced": False,
            "promotion_authorized": False,
        }
    )
    closed = engine._self_hashed(body, "closure_payload_sha256")
    return engine._read_json(
        engine._write_json(args.output_root / CLOSURE_NAME, closed)
    )


def run_authorized_canary(
    args: argparse.Namespace,
    *,
    admission: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    admitted_root = args.output_root.resolve()
    freeze_root = admitted_root / "prefinancial_freeze"
    run_root = admitted_root / "run"
    action = str(admission.get("requested_action") or "")
    if action == ACTION_RECOVERY:
        verify_prefinancial_freeze_v1(freeze_root)
    else:
        phase_b_outcome = PROJECT_ROOT / str(
            authorization["source_phase_b_outcome_path"]
        )
        build_prefinancial_freeze_v1(
            output_root=freeze_root,
            phase_b_freeze_root=args.phase_b_freeze_root,
            phase_b_result_root=Path(str(authorization["source_phase_b_root"])),
            phase_b_outcome_path=phase_b_outcome,
            registry_path=args.registry,
            accepted_field_manifest_path=args.accepted_field_manifest,
            information_metrics_path=args.information_metrics,
            bar_source_root=args.bar_source_root,
            node_resource_capacity_path=args.node_resource_capacity,
            repo_sha=str(admission["repo_sha"]),
            authorization=authorization,
        )
        verify_prefinancial_freeze_v1(freeze_root)
    run_args = argparse.Namespace(**vars(args))
    run_args.phase_c_freeze_root = freeze_root
    run_args.output_root = run_root
    run_args.builder_commit_sha = str(admission["repo_sha"])
    run_contract = engine._read_json(freeze_root / "phase_c_run_contract.json")
    run_args.train_field_root = Path(
        str(run_contract["accepted_field_manifest_path"])
    ).resolve().parent
    return run(run_args)


def main(argv: Sequence[str] | None = None) -> int:
    raise PermissionError(
        "Direct Search V2 runner invocation is forbidden; use app.py and Project Control"
    )


if __name__ == "__main__":
    raise SystemExit(main())
