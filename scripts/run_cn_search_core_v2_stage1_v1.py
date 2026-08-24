"""Two-arm fresh-development Search Core V2 Stage-1 tournament.

A: confirmed Primitive V1 over 336 never-evaluated Stage-D remainder Programs.
B: semantic State-Jump Generator V2, adapting only from earlier checkpoints in
this development campaign.  Every Program reuses the existing compiler,
matched-control, evaluator, absolute-admission and conditional-uplift stack.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as successor
from our_system_phase2.runtime import cn_search_core_v2_stage1_v1 as runtime
from our_system_phase2.services.candidate_program_proposal_v0 import PROGRAM_TEMPLATE_COMPONENTS
from our_system_phase2.services.program_search_optimizer_v1 import (
    ProgramOptimizerObservationV1,
    program_availability_entries_v1,
)
from our_system_phase2.services.program_search_primitive_credit_v1 import (
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
)
from our_system_phase2.services.program_search_state_jump_adapter_v2 import (
    StateJumpProgramSearchAdapterV2,
)
from our_system_phase2.services.program_search_state_jump_generator_v2 import (
    SEMANTIC_STATE_JUMP_GENERATOR_V2,
)
from our_system_phase2.services.search_v2_conditional_uplift import (
    MATCHED_CONTROL_CONTRACT_ID,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


CHECKPOINT_SIZE = runtime.CHECKPOINT_SIZE
PRIMARY_EXECUTOR_WORKERS = runtime.PRIMARY_EXECUTOR_WORKERS
TEMPLATES = (
    "BASE_EVENT",
    "BASE_MARKET",
    "BASE_MARKET_EVENT",
    "BASE_TEMPORAL",
    "BASE_TEMPORAL_EVENT",
    "BASE_TEMPORAL_MARKET",
    "BASE_TEMPORAL_MARKET_EVENT",
)
ARMS = (
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
    SEMANTIC_STATE_JUMP_GENERATOR_V2,
)


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify_self(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def verify_prefreeze(path: Path) -> dict[str, Any]:
    payload = _read(path)
    _verify_self(payload, "prefreeze_payload_sha256", "Search Core V2 Stage-1 prefreeze")
    if (
        payload.get("status")
        != "SEARCH_CORE_V2_STAGE1_PREFROZEN_BEFORE_FINANCIAL_READ"
        or int(payload["stage1"]["total_budget_per_arm"]) != 336
        or int(payload["stage1"]["total_financial_evaluations"]) != 672
        or bool(payload.get("financial_labels_read_by_builder"))
        or bool(payload["stage1"].get("automatic_stage2_launch"))
    ):
        raise RuntimeError("SEARCH_CORE_V2_STAGE1_PREFREEZE_CONTRACT_DRIFT")
    selected = list(map(str, payload["arm_a_primitive"]["selected_exact_identities"]))
    if len(selected) != 336 or len(set(selected)) != 336:
        raise RuntimeError("SEARCH_CORE_V2_STAGE1_ARM_A_SELECTION_DRIFT")
    for template, order in dict(
        payload["arm_a_primitive"]["eligible_orders_by_template"]
    ).items():
        if template not in TEMPLATES or len(order) != 224 or len(set(map(str, order))) != 224:
            raise RuntimeError("SEARCH_CORE_V2_STAGE1_ARM_A_ORDER_DRIFT")
    return payload


def _load_authority(
    args: argparse.Namespace,
    *,
    authorization: Mapping[str, Any],
    repo_sha: str,
) -> dict[str, Any]:
    prior = dict(authorization["source_prior_exact"])
    return successor._load_authority(
        args,
        authorization=authorization,
        repo_sha=repo_sha,
        campaign_id=str(authorization["campaign_id"]),
        campaign_profile=str(authorization["campaign_profile"]),
        prior_freeze_payload_sha256=str(prior["payload_sha256"]),
        prior_exact_count=int(prior["count"]),
        prior_exact_identities_sha256=str(prior["exact_identities_sha256"]),
        prior_identity_field=str(prior["identity_field"]),
        input_binding_schema_version="cn_search_core_v2_stage1_input_binding_v1",
        resource_profile_id="SEARCH_DUAL_24",
        resource_profile_role="SEARCH",
        maximum_executor_workers=PRIMARY_EXECUTOR_WORKERS,
    )


def _stage_d_supply(repo: Path, prefreeze: Mapping[str, Any]) -> dict[str, Any]:
    binding = dict(prefreeze["arm_a_primitive"])
    path = repo / Path(str(binding["source_supply_relative_path"]))
    if engine._sha256(path) != str(binding["source_supply_file_sha256"]):
        raise RuntimeError("SEARCH_CORE_V2_STAGE_D_SUPPLY_FILE_DRIFT")
    payload = _read(path)
    claimed = _verify_self(payload, "audit_payload_sha256", "Search Core V2 Stage-D supply")
    if claimed != str(binding["source_supply_payload_sha256"]):
        raise RuntimeError("SEARCH_CORE_V2_STAGE_D_SUPPLY_PAYLOAD_DRIFT")
    return payload


def _known_normalized_exact_ids(
    repo: Path,
    authority: Mapping[str, Any],
) -> tuple[str, ...]:
    known = {entry.exact_identity for entry in authority["entries"]}
    for relative in (
        "runtime/run_plans/cn_stage_c_expanded_supply_audit_d0160c6_20260820.json",
        "runtime/run_plans/cn_stage_d_expanded_supply_audit_cbaaaee_20260820.json",
    ):
        payload = _read(repo / relative)
        rows = list(payload["fresh_entries"])
        normalized = program_availability_entries_v1(
            [{"genes": dict(row["program_genes"])} for row in rows]
        )
        known.update(entry.exact_identity for entry in normalized)
    return tuple(sorted(known))


def _components_by_role(authority: Mapping[str, Any]) -> dict[str, tuple[Any, ...]]:
    output: dict[str, list[Any]] = {}
    for component in authority["components_by_id"].values():
        output.setdefault(component.role, []).append(component)
    return {
        role: tuple(sorted(rows, key=lambda component: component.component_id))
        for role, rows in output.items()
    }


def _reservoir(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "cn_joint_program_phase_c_reservoir_record_v0",
        "template_id": str(candidate["template_id"]),
        "components": dict(candidate["components"]),
        "combination_policy": dict(candidate["combination_policy"]),
        "raw_combination_sha256": str(candidate["raw_combination_sha256"]),
        "reservoir_record_sha256": str(candidate["reservoir_record_sha256"]),
        "semantic_compile_required_at_selection": True,
        "semantic_noop_does_not_count_toward_quota": True,
        "financial_evaluation_executed": False,
    }


def _ask_record(
    *,
    global_ordinal: int,
    checkpoint_ordinal: int,
    template_id: str,
    template_ordinal: int,
    arm: str,
) -> dict[str, Any]:
    row = {
        "schema_version": "cn_search_core_v2_stage1_ask_v1",
        "main_record_ordinal": int(global_ordinal),
        "checkpoint_ordinal": int(checkpoint_ordinal),
        "template_id": str(template_id),
        "template_record_ordinal": int(template_ordinal),
        "generation_arm": str(arm),
        "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
        "absolute_admission_head_eligible": True,
        "conditional_uplift_head_eligible": True,
    }
    row["ask_record_sha256"] = stable_hash(row)
    return row


def _static_schedule(
    candidate: Mapping[str, Any],
    *,
    authority: Mapping[str, Any],
    global_ordinal: int,
    checkpoint_ordinal: int,
    template_ordinal: int,
) -> dict[str, Any]:
    reservoir = _reservoir(candidate)
    entry = engine._catalog_entry(
        reservoir,
        components_by_id=authority["components_by_id"],
        adapter=authority["adapter"],
        compiler=authority["compiler"],
    )
    if entry.get("status") != "EXECUTABLE":
        raise RuntimeError("SEARCH_CORE_V2_ARM_A_NOT_EXECUTABLE")
    exact = stable_hash(dict(entry["program_genes"]))
    if exact != str(candidate["exact_identity"]):
        raise RuntimeError("SEARCH_CORE_V2_ARM_A_EXACT_DRIFT")
    ask = _ask_record(
        global_ordinal=global_ordinal,
        checkpoint_ordinal=checkpoint_ordinal,
        template_id=str(candidate["template_id"]),
        template_ordinal=template_ordinal,
        arm=PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
    )
    decision = engine._self_hashed(
        {
            "schema_version": "cn_search_core_v2_arm_a_selection_v1",
            "selection_mode": "PREFROZEN_PRIMITIVE_TOP48_UNSPENT_STAGE_D_REMAINDER",
            "exact_identity": exact,
            "adaptive_template_credit_used": False,
        },
        "selection_decision_sha256",
    )
    schedule = engine._schedule_record(
        ask,
        entry,
        decision,
        components_by_id=authority["components_by_id"],
        adapter=authority["adapter"],
        compiler=authority["compiler"],
    )
    schedule.update(
        {
            "search_core_v2_arm": PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
            "search_core_exact_identity": exact,
            "optimizer_selection_used": True,
            "online_feedback_used": False,
        }
    )
    schedule["schedule_record_sha256"] = stable_hash(
        {k: v for k, v in schedule.items() if k != "schedule_record_sha256"}
    )
    return schedule


def _generated_reservoir(generated: Any) -> dict[str, Any]:
    payload = {
        "schema_version": "cn_joint_program_phase_c_reservoir_record_v0",
        "template_id": generated.template_id,
        "components": {
            role: {
                "component_id": component.component_id,
                "route_id": component.route_id,
                "proposal_id": component.proposal_id,
                "generation_receipt_hash": component.generation_receipt_hash,
            }
            for role, component in generated.components.items()
        },
        "combination_policy": dict(generated.combination_policy),
        "raw_combination_sha256": stable_hash(generated.summary()),
        "semantic_compile_required_at_selection": True,
        "semantic_noop_does_not_count_toward_quota": True,
        "financial_evaluation_executed": False,
    }
    payload["reservoir_record_sha256"] = stable_hash(payload)
    return payload


def _generated_schedules(
    optimizer: StateJumpProgramSearchAdapterV2,
    *,
    authority: Mapping[str, Any],
    template_id: str,
    checkpoint_id: str,
    checkpoint_ordinal: int,
    global_start: int,
    template_start: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    optimizer_asks = optimizer.ask(
        checkpoint_id=checkpoint_id,
        count=CHECKPOINT_SIZE,
        required_program_template_id=template_id,
        eligible_exact_identities=None,
        batch_group_constraint=None,
    )
    if len(optimizer_asks) != CHECKPOINT_SIZE:
        raise RuntimeError("SEARCH_CORE_V2_ARM_B_ASK_UNDERFILL")
    schedules: list[dict[str, Any]] = []
    for offset, optimizer_ask in enumerate(optimizer_asks):
        generated = optimizer.generated_for_proposal(str(optimizer_ask["proposal_id"]))
        reservoir = _generated_reservoir(generated)
        entry = engine._catalog_entry(
            reservoir,
            components_by_id=authority["components_by_id"],
            adapter=authority["adapter"],
            compiler=authority["compiler"],
        )
        if entry.get("status") != "EXECUTABLE":
            raise RuntimeError("SEARCH_CORE_V2_ARM_B_NOT_EXECUTABLE")
        observed_exact = stable_hash(dict(entry["program_genes"]))
        if observed_exact != str(optimizer_ask["exact_identity"]):
            raise RuntimeError("SEARCH_CORE_V2_ARM_B_NORMALIZED_EXACT_DRIFT")
        ask = _ask_record(
            global_ordinal=global_start + offset,
            checkpoint_ordinal=checkpoint_ordinal,
            template_id=template_id,
            template_ordinal=template_start + offset,
            arm=SEMANTIC_STATE_JUMP_GENERATOR_V2,
        )
        decision = engine._self_hashed(
            {
                "schema_version": "cn_search_core_v2_arm_b_selection_v1",
                "selection_mode": "CAUSAL_SEMANTIC_STATE_JUMP_GENERATOR_V2",
                "exact_identity": observed_exact,
                "optimizer_proposal_id": str(optimizer_ask["proposal_id"]),
                "generator_summary": generated.summary(),
                "adaptive_template_credit_used": True,
            },
            "selection_decision_sha256",
        )
        schedule = engine._schedule_record(
            ask,
            entry,
            decision,
            components_by_id=authority["components_by_id"],
            adapter=authority["adapter"],
            compiler=authority["compiler"],
        )
        schedule.update(
            {
                "optimizer_ask": dict(optimizer_ask),
                "search_core_v2_arm": SEMANTIC_STATE_JUMP_GENERATOR_V2,
                "search_core_exact_identity": observed_exact,
                "generator_summary": generated.summary(),
                "optimizer_selection_used": True,
                "online_feedback_used": True,
            }
        )
        schedule["schedule_record_sha256"] = stable_hash(
            {k: v for k, v in schedule.items() if k != "schedule_record_sha256"}
        )
        schedules.append(schedule)
    exacts = [str(row["search_core_exact_identity"]) for row in schedules]
    if len(exacts) != CHECKPOINT_SIZE or len(set(exacts)) != CHECKPOINT_SIZE:
        raise RuntimeError("SEARCH_CORE_V2_ARM_B_CHECKPOINT_EXACT_DUPLICATE")
    if set(exacts).intersection(optimizer.seen_exact_identities):
        raise RuntimeError("SEARCH_CORE_V2_ARM_B_KNOWN_SPACE_OVERLAP")
    return schedules, optimizer_asks


def _behavior_pair(record: Mapping[str, Any]) -> str | None:
    primary = dict(record.get("primary") or {})
    control = dict(record.get("base_control") or {})
    left = str(primary.get("behavior_identity") or "")
    right = str(control.get("behavior_identity") or "")
    if not left or not right or left == right:
        return None
    return stable_hash({"primary": left, "base_control": right})


def _result_record(
    record: Mapping[str, Any],
    schedule: Mapping[str, Any],
) -> tuple[dict[str, Any], Any]:
    physical = successor._physical_result(record, schedule)
    admission = physical.admission.to_record()
    uplift = None if physical.uplift is None else physical.uplift.to_record()
    credit = dict(dict(uplift or {}).get("program_credit") or {})
    productive = bool(admission.get("admitted")) and (
        float(credit.get("matched_cumulative_net_return_increment") or 0.0) > 0.0
        and float(credit.get("matched_net_reward_increment") or 0.0) > 0.0
    )
    stable = productive and int(credit.get("cross_window_positive_increment_count") or 0) >= 2
    payload = {
        "schema_version": "cn_search_core_v2_stage1_result_v1",
        "main_record_ordinal": int(record["main_record_ordinal"]),
        "arm": str(schedule["search_core_v2_arm"]),
        "exact_identity": str(schedule["search_core_exact_identity"]),
        "template_id": str(schedule["template_id"]),
        "checkpoint_ordinal": int(schedule["checkpoint_ordinal"]),
        "round_index": int(schedule["search_core_round_index"]),
        "base_component_id": str(dict(schedule["components"])["base"]["component_id"]),
        "admission": admission,
        "uplift": uplift,
        "productive": productive,
        "stable": stable,
        "behavior_pair_identity": _behavior_pair(record),
        "structural_region_identity": stable_hash(
            {
                "template_id": str(schedule["template_id"]),
                "component_ids": {
                    role: str(binding["component_id"])
                    for role, binding in dict(schedule["components"]).items()
                },
                "combination_policy": dict(schedule["combination_policy"]),
            }
        ),
        "source_record_payload_sha256": str(record["record_payload_sha256"]),
        "validation_feedback_used": False,
    }
    payload["result_payload_sha256"] = stable_hash(payload)
    return payload, physical


def _metric(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(row) for row in rows]
    n = len(rows)
    productive = sum(bool(row.get("productive")) for row in rows)
    stable = sum(bool(row.get("stable")) for row in rows)
    admitted = sum(bool(dict(row.get("admission") or {}).get("admitted")) for row in rows)
    behaviors = {str(row["behavior_pair_identity"]) for row in rows if row.get("behavior_pair_identity")}
    regions = {str(row["structural_region_identity"]) for row in rows}
    return {
        "evaluated": n,
        "admitted": admitted,
        "admission_rate": admitted / n if n else 0.0,
        "productive": productive,
        "productive_rate": productive / n if n else 0.0,
        "stable": stable,
        "stable_rate": stable / n if n else 0.0,
        "distinct_behavior_pair_count": len(behaviors),
        "distinct_behavior_pair_rate": len(behaviors) / n if n else 0.0,
        "distinct_structural_region_count": len(regions),
    }


def _decision(
    arm_a: Mapping[str, Any],
    arm_b: Mapping[str, Any],
) -> dict[str, Any]:
    a_prod = float(arm_a["productive"])
    b_prod = float(arm_b["productive"])
    a_beh = float(arm_a["distinct_behavior_pair_count"])
    b_beh = float(arm_b["distinct_behavior_pair_count"])
    baseline_sane = 139 <= a_prod <= 140 and 247 <= a_beh <= 251
    clear_win = (
        b_prod >= a_prod * 1.05 and b_beh >= a_beh * 0.95
    ) or (
        b_beh >= a_beh * 1.05 and b_prod >= a_prod * 0.97
    )
    clear_loss = (
        b_prod < a_prod * 0.95 and b_beh <= a_beh
    ) or (
        b_beh < a_beh * 0.95 and b_prod <= a_prod
    )
    if clear_win:
        status = "CLEAR_GENERATOR_V2_STAGE1_WIN_STAGE2_REVIEW_ELIGIBLE"
    elif clear_loss:
        status = "CLEAR_GENERATOR_V2_STAGE1_LOSS_STOP"
    else:
        status = "AMBIGUOUS_STAGE1_REVIEW_NO_AUTOMATIC_STAGE2"
    return {
        "status": status,
        "baseline_historical_sanity_pass": baseline_sane,
        "productive_ratio_b_vs_a": b_prod / a_prod if a_prod else None,
        "productive_delta_b_vs_a": b_prod - a_prod,
        "behavior_ratio_b_vs_a": b_beh / a_beh if a_beh else None,
        "behavior_delta_b_vs_a": b_beh - a_beh,
        "automatic_stage2_authorized": False,
    }


def _close_checkpoint(
    inflight: Path,
    closed: Path,
    *,
    previous_sha: str,
    checkpoint_ordinal: int,
) -> str:
    artifacts = [
        successor._artifact(path, inflight)
        for path in sorted(inflight.rglob("*"))
        if path.is_file() and path.name != "checkpoint_manifest.json"
    ]
    manifest = engine._self_hashed(
        {
            "schema_version": "cn_search_core_v2_stage1_checkpoint_manifest_v1",
            "status": "SEARCH_CORE_V2_STAGE1_CHECKPOINT_CLOSED_IMMUTABLE",
            "checkpoint_ordinal": int(checkpoint_ordinal),
            "previous_checkpoint_manifest_file_sha256": str(previous_sha),
            "artifacts": artifacts,
        },
        "manifest_payload_sha256",
    )
    engine._write_json(inflight / "checkpoint_manifest.json", manifest)
    inflight.replace(closed)
    return engine._sha256(closed / "checkpoint_manifest.json")


def run(
    args: argparse.Namespace,
    *,
    admission: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    root = args.output_root.resolve()
    if (
        not root.is_dir()
        or not (root / ".project_control_execution").is_dir()
        or {path.name for path in root.iterdir()} != {".project_control_execution"}
    ):
        raise RuntimeError("SEARCH_CORE_V2_ADMITTED_ROOT_NOT_CLEAN")
    repo = Path(__file__).resolve().parents[1]
    prefreeze = verify_prefreeze(args.stage1_prefreeze)
    authority = _load_authority(
        args,
        authorization=authorization,
        repo_sha=str(admission["repo_sha"]),
    )
    stage_d_supply = _stage_d_supply(repo, prefreeze)
    supply_by_exact = {
        str(row["exact_identity"]): dict(row)
        for row in stage_d_supply["fresh_entries"]
    }
    baseline_orders = {
        template: list(map(str, order))[:48]
        for template, order in dict(
            prefreeze["arm_a_primitive"]["eligible_orders_by_template"]
        ).items()
    }
    if any(len(order) != 48 for order in baseline_orders.values()):
        raise RuntimeError("SEARCH_CORE_V2_ARM_A_BUDGET_DRIFT")

    known_exact = _known_normalized_exact_ids(repo, authority)
    state_jump = StateJumpProgramSearchAdapterV2(
        adapter=authority["adapter"],
        compiler=authority["compiler"],
        components_by_role=_components_by_role(authority),
        ordered_gene_slots=tuple(authority["entries"][0].genes),
        seen_exact_identities=known_exact,
        seed=int(prefreeze["arm_b_state_jump"]["seed"]),
        operation_priors=dict(prefreeze["arm_b_state_jump"]["operation_priors"]),
        maximum_attempts=int(prefreeze["arm_b_state_jump"]["maximum_attempts"]),
    )
    engine._write_json(root / "input_binding.json", authority["input_binding"])
    engine._write_json(
        root / "search_core_v2_policy_binding.json",
        {
            "schema_version": "cn_search_core_v2_stage1_policy_binding_v1",
            "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
            "arm_a_selected_exact_identities_sha256": str(
                prefreeze["arm_a_primitive"]["selected_exact_identities_sha256"]
            ),
            "known_normalized_exact_identities_sha256": stable_hash(list(known_exact)),
            "state_jump_seed": int(prefreeze["arm_b_state_jump"]["seed"]),
            "state_jump_operation_priors": dict(
                prefreeze["arm_b_state_jump"]["operation_priors"]
            ),
            "validation_feedback_used": False,
        },
    )
    input_hash = str(authority["input_binding"]["input_binding_sha256"])
    previous_manifest = "GENESIS"
    results_by_arm: dict[str, list[dict[str, Any]]] = {arm: [] for arm in ARMS}
    checkpoint_metrics: list[dict[str, Any]] = []
    global_ordinal = 0
    template_ordinals = Counter()
    checkpoint_ordinal = 0
    started = time.perf_counter()

    for round_index in range(2):
        for template in TEMPLATES:
            for arm in ARMS:
                inflight = root / f"checkpoint_{checkpoint_ordinal:04d}.inflight"
                closed = root / f"checkpoint_{checkpoint_ordinal:04d}"
                inflight.mkdir()
                if arm == PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1:
                    candidates = [
                        supply_by_exact[exact]
                        for exact in baseline_orders[template][
                            round_index * CHECKPOINT_SIZE : (round_index + 1) * CHECKPOINT_SIZE
                        ]
                    ]
                    schedules = []
                    for candidate in candidates:
                        schedules.append(
                            _static_schedule(
                                candidate,
                                authority=authority,
                                global_ordinal=global_ordinal,
                                checkpoint_ordinal=checkpoint_ordinal,
                                template_ordinal=template_ordinals[(arm, template)],
                            )
                        )
                        global_ordinal += 1
                        template_ordinals[(arm, template)] += 1
                    engine._write_json(
                        inflight / "optimizer_state_before.json",
                        {
                            "arm": arm,
                            "learning": False,
                            "prefrozen": True,
                        },
                    )
                else:
                    before = state_jump.snapshot()
                    engine._write_json(inflight / "optimizer_state_before.json", before)
                    schedules, _optimizer_asks = _generated_schedules(
                        state_jump,
                        authority=authority,
                        template_id=template,
                        checkpoint_id=f"SEARCH_CORE_V2_R{round_index}_{template}",
                        checkpoint_ordinal=checkpoint_ordinal,
                        global_start=global_ordinal,
                        template_start=template_ordinals[(arm, template)],
                    )
                    global_ordinal += len(schedules)
                    template_ordinals[(arm, template)] += len(schedules)
                for schedule in schedules:
                    schedule["search_core_round_index"] = int(round_index)
                    schedule["schedule_record_sha256"] = stable_hash(
                        {
                            key: value
                            for key, value in schedule.items()
                            if key != "schedule_record_sha256"
                        }
                    )
                engine._write_jsonl(inflight / "selected_schedule.jsonl", schedules)
                records = successor._evaluate_schedules(
                    schedules,
                    record_root=inflight / "records",
                    authority=authority,
                    input_hash=input_hash,
                    executor_workers=PRIMARY_EXECUTOR_WORKERS,
                )
                for record in records:
                    if any(
                        int(record.get(key) or 0) != 0
                        for key in (
                            "validation_reads",
                            "holdout_reads",
                            "historical_2023_reads",
                            "forward_b_reads",
                            "forward_2026_reads",
                        )
                    ):
                        raise RuntimeError("SEARCH_CORE_V2_STAGE1_RESTRICTED_READ_DRIFT")
                by_ordinal = {
                    int(schedule["main_record_ordinal"]): schedule
                    for schedule in schedules
                }
                result_rows: list[dict[str, Any]] = []
                observations: list[ProgramOptimizerObservationV1] = []
                for record in records:
                    schedule = by_ordinal[int(record["main_record_ordinal"])]
                    result, physical = _result_record(record, schedule)
                    result_rows.append(result)
                    if arm == SEMANTIC_STATE_JUMP_GENERATOR_V2:
                        optimizer_ask = dict(schedule["optimizer_ask"])
                        observations.append(
                            ProgramOptimizerObservationV1(
                                proposal_id=str(optimizer_ask["proposal_id"]),
                                exact_identity=str(optimizer_ask["exact_identity"]),
                                admission=physical.admission,
                                uplift=physical.uplift,
                            )
                        )
                if arm == SEMANTIC_STATE_JUMP_GENERATOR_V2:
                    tell = state_jump.tell(observations)
                    engine._write_json(inflight / "optimizer_tell_receipt.json", tell)
                    engine._write_json(
                        inflight / "optimizer_state_after.json", state_jump.snapshot()
                    )
                else:
                    engine._write_json(
                        inflight / "optimizer_state_after.json",
                        {
                            "arm": arm,
                            "learning": False,
                            "prefrozen": True,
                        },
                    )
                engine._write_jsonl(inflight / "candidate_results.jsonl", result_rows)
                metric = {
                    "checkpoint_ordinal": checkpoint_ordinal,
                    "round_index": round_index,
                    "template_id": template,
                    "arm": arm,
                    **_metric(result_rows),
                }
                engine._write_json(inflight / "checkpoint_metric.json", metric)
                previous_manifest = _close_checkpoint(
                    inflight,
                    closed,
                    previous_sha=previous_manifest,
                    checkpoint_ordinal=checkpoint_ordinal,
                )
                checkpoint_metrics.append(metric)
                results_by_arm[arm].extend(result_rows)
                checkpoint_ordinal += 1

    if global_ordinal != 672 or checkpoint_ordinal != 28:
        raise RuntimeError("SEARCH_CORE_V2_STAGE1_EXECUTION_COUNT_DRIFT")
    arm_metrics = {arm: _metric(rows) for arm, rows in results_by_arm.items()}
    per_template = {
        arm: {
            template: _metric(
                [row for row in rows if str(row["template_id"]) == template]
            )
            for template in TEMPLATES
        }
        for arm, rows in results_by_arm.items()
    }
    round_metrics = {
        arm: {
            str(round_index): _metric(
                [
                    row
                    for row in results_by_arm[arm]
                    if int(row["round_index"]) == round_index
                ]
            )
            for round_index in (0, 1)
        }
        for arm in ARMS
    }
    if any(
        int(round_metrics[arm][str(round_index)]["evaluated"]) != 168
        for arm in ARMS
        for round_index in (0, 1)
    ):
        raise RuntimeError("SEARCH_CORE_V2_STAGE1_ROUND_COVERAGE_DRIFT")
    late_round_metrics = {
        arm: dict(round_metrics[arm]["1"])
        for arm in ARMS
    }
    decision = _decision(
        arm_metrics[PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1],
        arm_metrics[SEMANTIC_STATE_JUMP_GENERATOR_V2],
    )
    wall = time.perf_counter() - started
    closure = engine._self_hashed(
        {
            "schema_version": "cn_search_core_v2_stage1_complete_v1",
            "status": "SEARCH_CORE_V2_STAGE1_COMPLETE",
            "repo_sha": str(admission["repo_sha"]),
            "authorization_payload_sha256": str(
                authorization["authorization_payload_sha256"]
            ),
            "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
            "evaluated": 672,
            "evaluated_per_arm": 336,
            "checkpoint_count": 28,
            "last_checkpoint_manifest_file_sha256": previous_manifest,
            "arm_metrics": arm_metrics,
            "per_template_metrics": per_template,
            "checkpoint_metrics": checkpoint_metrics,
            "round_metrics": round_metrics,
            "late_round_metrics": late_round_metrics,
            "decision": decision,
            "state_jump_final_diagnostics": state_jump.generator.diagnostics(),
            "wall_seconds": wall,
            "wall_seconds_per_productive": {
                arm: wall / float(metric["productive"])
                if int(metric["productive"]) > 0
                else None
                for arm, metric in arm_metrics.items()
            },
            "evaluation_data_role": "DEVELOPMENT_ONLY",
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
        },
        "closure_payload_sha256",
    )
    engine._write_json(root / "CN_SEARCH_CORE_V2_STAGE1_COMPLETE.json", closure)
    return closure


__all__ = [
    "verify_prefreeze",
    "run",
]
