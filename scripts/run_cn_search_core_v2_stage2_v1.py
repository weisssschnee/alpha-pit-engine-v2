"""Search Core V2 Stage-2 mature-state continuation confirmation."""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as successor
from scripts import run_cn_search_core_v2_stage1_v1 as stage1
from our_system_phase2.runtime import cn_search_core_v2_stage2_v1 as runtime
from our_system_phase2.services.program_search_optimizer_v1 import ProgramOptimizerObservationV1
from our_system_phase2.services.program_search_primitive_credit_v1 import PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
from our_system_phase2.services.program_search_state_jump_adapter_v2 import StateJumpProgramSearchAdapterV2
from our_system_phase2.services.program_search_state_jump_generator_v2 import SEMANTIC_STATE_JUMP_GENERATOR_V2
from our_system_phase2.services.search_v2_conditional_uplift import MATCHED_CONTROL_CONTRACT_ID
from our_system_phase2.services.unified_capability_registry import stable_hash

TEMPLATES = stage1.TEMPLATES
ARMS = (PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1, SEMANTIC_STATE_JUMP_GENERATOR_V2)
CHECKPOINT_SIZE = 24
PRIMARY_EXECUTOR_WORKERS = 24


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify_self(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def verify_prefreeze(path: Path) -> dict[str, Any]:
    payload = _read(path.resolve())
    _verify_self(payload, "prefreeze_payload_sha256", "Search Core V2 Stage-2 prefreeze")
    if (
        payload.get("status") != "SEARCH_CORE_V2_STAGE2_PREFROZEN_BEFORE_FINANCIAL_READ"
        or int(payload["stage2"]["total_budget_per_arm"]) != 504
        or int(payload["stage2"]["total_financial_evaluations"]) != 1008
        or bool(payload.get("financial_labels_read_by_builder"))
        or bool(payload.get("candidate_evaluation_executed"))
        or bool(payload.get("automatic_policy_change_authorized"))
        or any(int(v) != 0 for v in dict(payload["restricted_reads"]).values())
    ):
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_PREFREEZE_CONTRACT_DRIFT")
    return payload


def _static_schedule(
    candidate: Mapping[str, Any], *, authority: Mapping[str, Any],
    global_ordinal: int, checkpoint_ordinal: int, template_ordinal: int,
) -> dict[str, Any]:
    reservoir = stage1._reservoir(candidate)
    entry = engine._catalog_entry(
        reservoir,
        components_by_id=authority["components_by_id"],
        adapter=authority["adapter"],
        compiler=authority["compiler"],
    )
    if entry.get("status") != "EXECUTABLE":
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_ARM_A_NOT_EXECUTABLE")
    exact = stable_hash(dict(entry["program_genes"]))
    if exact != str(candidate["exact_identity"]):
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_ARM_A_EXACT_DRIFT")
    ask = stage1._ask_record(
        global_ordinal=global_ordinal,
        checkpoint_ordinal=checkpoint_ordinal,
        template_id=str(candidate["template_id"]),
        template_ordinal=template_ordinal,
        arm=PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
    )
    decision = engine._self_hashed(
        {
            "schema_version": "cn_search_core_v2_stage2_arm_a_selection_v1",
            "selection_mode": "PREFROZEN_PRIMITIVE_STAGE1_ORDER_INDEX_72_144",
            "exact_identity": exact,
            "adaptive_template_credit_used": False,
        },
        "selection_decision_sha256",
    )
    schedule = engine._schedule_record(
        ask, entry, decision,
        components_by_id=authority["components_by_id"],
        adapter=authority["adapter"], compiler=authority["compiler"],
    )
    schedule.update({
        "search_core_v2_arm": PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
        "search_core_exact_identity": exact,
        "successor_exact_identity": exact,
        "optimizer_selection_used": True,
        "online_feedback_used": False,
        "search_core_stage": "STAGE2",
    })
    schedule["schedule_record_sha256"] = stable_hash(
        {k: v for k, v in schedule.items() if k != "schedule_record_sha256"}
    )
    return schedule



def _generated_schedules_batch(
    optimizer: StateJumpProgramSearchAdapterV2,
    *,
    authority: Mapping[str, Any],
    template_id: str,
    checkpoint_id: str,
    checkpoint_ordinal: int,
    global_start: int,
    template_start: int,
    batch_size: int,
) -> list[dict[str, Any]]:
    optimizer_asks = optimizer.ask(
        checkpoint_id=checkpoint_id,
        count=int(batch_size),
        required_program_template_id=template_id,
        eligible_exact_identities=None,
        batch_group_constraint=None,
    )
    if len(optimizer_asks) != int(batch_size):
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_ARM_B_ASK_UNDERFILL")
    schedules: list[dict[str, Any]] = []
    for offset, optimizer_ask in enumerate(optimizer_asks):
        generated = optimizer.generated_for_proposal(str(optimizer_ask["proposal_id"]))
        reservoir = stage1._generated_reservoir(generated)
        entry = engine._catalog_entry(
            reservoir,
            components_by_id=authority["components_by_id"],
            adapter=authority["adapter"],
            compiler=authority["compiler"],
        )
        if entry.get("status") != "EXECUTABLE":
            raise RuntimeError("SEARCH_CORE_V2_STAGE2_ARM_B_NOT_EXECUTABLE")
        observed_genes = dict(entry["program_genes"])
        asked_genes = dict(optimizer_ask["program_genes"])
        if observed_genes != asked_genes:
            raise RuntimeError("SEARCH_CORE_V2_STAGE2_ARM_B_PROGRAM_GENE_DRIFT")
        observed_exact = stage1.normalized_program_gene_identity_v1(
            observed_genes,
            ordered_slots=optimizer.ordered_gene_slots,
        )
        if observed_exact != str(optimizer_ask["exact_identity"]):
            raise RuntimeError("SEARCH_CORE_V2_STAGE2_ARM_B_NORMALIZED_EXACT_DRIFT")
        ask = stage1._ask_record(
            global_ordinal=global_start + offset,
            checkpoint_ordinal=checkpoint_ordinal,
            template_id=template_id,
            template_ordinal=template_start + offset,
            arm=SEMANTIC_STATE_JUMP_GENERATOR_V2,
        )
        decision = engine._self_hashed(
            {
                "schema_version": "cn_search_core_v2_stage2_arm_b_selection_v1",
                "selection_mode": "CAUSAL_MATURE_SEMANTIC_STATE_JUMP_GENERATOR_V2",
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
                "successor_exact_identity": observed_exact,
                "generator_summary": generated.summary(),
                "optimizer_selection_used": True,
                "online_feedback_used": True,
                "search_core_stage": "STAGE2",
            }
        )
        schedule["schedule_record_sha256"] = stable_hash(
            {key: value for key, value in schedule.items() if key != "schedule_record_sha256"}
        )
        schedules.append(schedule)
    exacts = [str(row["search_core_exact_identity"]) for row in schedules]
    if len(exacts) != int(batch_size) or len(set(exacts)) != int(batch_size):
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_ARM_B_CHECKPOINT_EXACT_DUPLICATE")
    if set(exacts).intersection(optimizer.seen_exact_identities):
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_ARM_B_KNOWN_SPACE_OVERLAP")
    return schedules


def _execution_plan(prefreeze: Mapping[str, Any]) -> list[dict[str, Any]]:
    batch_map = {
        str(template): int(value)
        for template, value in dict(prefreeze["stage2"]["template_batch_size"]).items()
    }
    expected = {template: (12 if template == "BASE_EVENT" else 24) for template in TEMPLATES}
    if batch_map != expected:
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_TEMPLATE_BATCH_SIZE_DRIFT")
    plan: list[dict[str, Any]] = []
    for stage2_round_index in range(3):
        for template in TEMPLATES:
            batch_size = batch_map[template]
            if 24 % batch_size != 0:
                raise RuntimeError("SEARCH_CORE_V2_STAGE2_MICROBATCH_GEOMETRY_DRIFT")
            micro_count = 24 // batch_size
            for microbatch_index in range(micro_count):
                start = stage2_round_index * 24 + microbatch_index * batch_size
                plan.append(
                    {
                        "stage2_round_index": stage2_round_index,
                        "template_id": template,
                        "microbatch_index": microbatch_index,
                        "batch_size": batch_size,
                        "slice_start": start,
                        "slice_end": start + batch_size,
                    }
                )
    if len(plan) != 24 or sum(int(row["batch_size"]) for row in plan) != 504:
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_EXECUTION_PLAN_DRIFT")
    return plan


def _decision(arm_a: Mapping[str, Any], arm_b: Mapping[str, Any], per_template: Mapping[str, Any]) -> dict[str, Any]:
    a_prod = float(arm_a["productive"]); b_prod = float(arm_b["productive"])
    a_stable = float(arm_a["stable"]); b_stable = float(arm_b["stable"])
    a_beh = float(arm_a["distinct_behavior_pair_count"]); b_beh = float(arm_b["distinct_behavior_pair_count"])
    deltas = {
        template: int(per_template[SEMANTIC_STATE_JUMP_GENERATOR_V2][template]["productive"])
        - int(per_template[PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1][template]["productive"])
        for template in TEMPLATES
    }
    wins = sum(delta > 0 for delta in deltas.values())
    positive = {k: max(0, v) for k, v in deltas.items()}
    positive_total = sum(positive.values())
    max_fraction = max(positive.values()) / positive_total if positive_total else None
    confirmation = (
        b_prod >= a_prod * 1.05
        and b_stable >= a_stable * 1.00
        and b_beh >= a_beh * 0.95
        and wins >= 4
        and max_fraction is not None and max_fraction <= 0.40
    )
    clear_loss = b_prod < a_prod * 0.95 and b_stable <= a_stable and b_beh <= a_beh
    if confirmation:
        status = "MATURE_GENERATOR_STAGE2_SCALE_CONFIRMATION_PASS_SEARCH_CORE_POLICY_REVIEW_ELIGIBLE"
    elif clear_loss:
        status = "MATURE_GENERATOR_STAGE2_SCALE_CLEAR_LOSS_STOP"
    else:
        status = "AMBIGUOUS_STAGE2_SCALE_CONFIRMATION_NO_POLICY_CHANGE"
    return {
        "status": status,
        "productive_ratio_b_vs_a": b_prod / a_prod if a_prod else None,
        "productive_delta_b_vs_a": b_prod - a_prod,
        "stable_ratio_b_vs_a": b_stable / a_stable if a_stable else None,
        "stable_delta_b_vs_a": b_stable - a_stable,
        "behavior_ratio_b_vs_a": b_beh / a_beh if a_beh else None,
        "behavior_delta_b_vs_a": b_beh - a_beh,
        "productive_template_deltas_b_minus_a": deltas,
        "productive_template_win_count_b": wins,
        "maximum_positive_productive_gain_template_fraction": max_fraction,
        "automatic_policy_change_authorized": False,
    }


def run(args: argparse.Namespace, *, admission: Mapping[str, Any], authorization: Mapping[str, Any]) -> dict[str, Any]:
    root = args.output_root.resolve()
    if (
        not root.is_dir() or not (root / ".project_control_execution").is_dir()
        or {p.name for p in root.iterdir()} != {".project_control_execution"}
    ):
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_ADMITTED_ROOT_NOT_CLEAN")
    repo = Path(__file__).resolve().parents[1]
    prefreeze = verify_prefreeze(args.stage2_prefreeze)
    authority = stage1._load_authority(args, authorization=authorization, repo_sha=str(admission["repo_sha"]))
    stage_d_supply = stage1._stage_d_supply(repo, prefreeze)
    supply_by_exact = {str(row["exact_identity"]): dict(row) for row in stage_d_supply["fresh_entries"]}
    selected = {
        template: list(map(str, prefreeze["arm_a_primitive"]["selected_by_template"][template]))
        for template in TEMPLATES
    }
    if any(len(rows) != 72 for rows in selected.values()):
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_ARM_A_BUDGET_DRIFT")

    snapshot_path = repo / Path(str(prefreeze["arm_b_state_jump"]["source_snapshot_relative_path"]))
    if engine._sha256(snapshot_path) != str(prefreeze["arm_b_state_jump"]["source_snapshot_file_sha256"]):
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_SNAPSHOT_FILE_DRIFT")
    snapshot = _read(snapshot_path)
    if str(snapshot.get("snapshot_hash") or "") != str(prefreeze["arm_b_state_jump"]["source_snapshot_payload_sha256"]):
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_SNAPSHOT_PAYLOAD_DRIFT")
    state_jump = StateJumpProgramSearchAdapterV2.restore(
        snapshot=snapshot,
        adapter=authority["adapter"], compiler=authority["compiler"],
        components_by_role=stage1._components_by_role(authority), seed=0,
    )
    initial_snapshot_hash = str(state_jump.snapshot()["snapshot_hash"])

    engine._write_json(root / "input_binding.json", authority["input_binding"])
    engine._write_json(root / "search_core_v2_stage2_policy_binding.json", {
        "schema_version": "cn_search_core_v2_stage2_policy_binding_v1",
        "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
        "arm_a_selected_exact_identities_sha256": str(prefreeze["arm_a_primitive"]["selected_exact_identities_sha256"]),
        "source_mature_snapshot_payload_sha256": initial_snapshot_hash,
        "source_mature_history_count": len(snapshot["history"]),
        "source_mature_generated_exact_count": len(snapshot["generated_exact_identities"]),
        "validation_feedback_used": False,
    })

    input_hash = str(authority["input_binding"]["input_binding_sha256"])
    previous_manifest = "GENESIS"
    results_by_arm: dict[str, list[dict[str, Any]]] = {arm: [] for arm in ARMS}
    checkpoint_metrics: list[dict[str, Any]] = []
    template_ordinals = Counter()
    global_ordinal = 0
    checkpoint_ordinal = 0
    started = time.perf_counter()

    execution_plan = _execution_plan(prefreeze)
    for step in execution_plan:
        stage2_round_index = int(step["stage2_round_index"])
        template = str(step["template_id"])
        microbatch_index = int(step["microbatch_index"])
        batch_size = int(step["batch_size"])
        slice_start = int(step["slice_start"])
        slice_end = int(step["slice_end"])
        for arm in ARMS:
            inflight = root / f"checkpoint_{checkpoint_ordinal:04d}.inflight"
            closed = root / f"checkpoint_{checkpoint_ordinal:04d}"
            inflight.mkdir()
            if arm == PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1:
                candidates = selected[template][slice_start:slice_end]
                if len(candidates) != batch_size:
                    raise RuntimeError("SEARCH_CORE_V2_STAGE2_ARM_A_MICROBATCH_UNDERFILL")
                schedules = []
                for exact in candidates:
                    schedule = _static_schedule(
                        supply_by_exact[exact],
                        authority=authority,
                        global_ordinal=global_ordinal,
                        checkpoint_ordinal=checkpoint_ordinal,
                        template_ordinal=template_ordinals[(arm, template)],
                    )
                    schedules.append(schedule)
                    global_ordinal += 1
                    template_ordinals[(arm, template)] += 1
                engine._write_json(
                    inflight / "optimizer_state_before.json",
                    {"arm": arm, "learning": False, "prefrozen": True},
                )
            else:
                engine._write_json(inflight / "optimizer_state_before.json", state_jump.snapshot())
                schedules = _generated_schedules_batch(
                    state_jump,
                    authority=authority,
                    template_id=template,
                    checkpoint_id=(
                        f"SEARCH_CORE_V2_STAGE2_R{stage2_round_index}_{template}_M{microbatch_index}"
                    ),
                    checkpoint_ordinal=checkpoint_ordinal,
                    global_start=global_ordinal,
                    template_start=template_ordinals[(arm, template)],
                    batch_size=batch_size,
                )
                global_ordinal += len(schedules)
                template_ordinals[(arm, template)] += len(schedules)
            for schedule in schedules:
                schedule["stage2_round_index"] = stage2_round_index
                schedule["stage2_microbatch_index"] = microbatch_index
                schedule["stage2_batch_size"] = batch_size
                schedule["search_core_round_index"] = 3 + stage2_round_index
                schedule["search_core_stage"] = "STAGE2"
                schedule["schedule_record_sha256"] = stable_hash(
                    {key: value for key, value in schedule.items() if key != "schedule_record_sha256"}
                )
            engine._write_jsonl(inflight / "selected_schedule.jsonl", schedules)

            records = successor._evaluate_schedules(
                schedules,
                record_root=inflight / "records",
                authority=authority,
                input_hash=input_hash,
                executor_workers=min(PRIMARY_EXECUTOR_WORKERS, batch_size),
            )
            for record in records:
                if any(
                    int(record.get(key) or 0) != 0
                    for key in (
                        "validation_reads", "holdout_reads", "historical_2023_reads",
                        "forward_b_reads", "forward_2026_reads",
                    )
                ):
                    raise RuntimeError("SEARCH_CORE_V2_STAGE2_RESTRICTED_READ_DRIFT")
            by_ordinal = {int(schedule["main_record_ordinal"]): schedule for schedule in schedules}
            result_rows: list[dict[str, Any]] = []
            observations: list[ProgramOptimizerObservationV1] = []
            for record in records:
                schedule = by_ordinal[int(record["main_record_ordinal"])]
                result, physical = stage1._result_record(record, schedule)
                result["search_core_stage"] = "STAGE2"
                result["stage2_round_index"] = stage2_round_index
                result["stage2_microbatch_index"] = microbatch_index
                result["stage2_batch_size"] = batch_size
                result["search_core_round_index"] = 3 + stage2_round_index
                result_rows.append(result)
                if arm == SEMANTIC_STATE_JUMP_GENERATOR_V2:
                    ask = dict(schedule["optimizer_ask"])
                    observations.append(
                        ProgramOptimizerObservationV1(
                            proposal_id=str(ask["proposal_id"]),
                            exact_identity=str(ask["exact_identity"]),
                            admission=physical.admission,
                            uplift=physical.uplift,
                        )
                    )
            if arm == SEMANTIC_STATE_JUMP_GENERATOR_V2:
                tell = state_jump.tell(observations)
                engine._write_json(inflight / "optimizer_tell_receipt.json", tell)
                engine._write_json(inflight / "optimizer_state_after.json", state_jump.snapshot())
            else:
                engine._write_json(
                    inflight / "optimizer_state_after.json",
                    {"arm": arm, "learning": False, "prefrozen": True},
                )
            engine._write_jsonl(inflight / "candidate_results.jsonl", result_rows)
            metric = {
                "checkpoint_ordinal": checkpoint_ordinal,
                "stage2_round_index": stage2_round_index,
                "stage2_microbatch_index": microbatch_index,
                "stage2_batch_size": batch_size,
                "search_core_round_index": 3 + stage2_round_index,
                "template_id": template,
                "arm": arm,
                **stage1._metric(result_rows),
            }
            engine._write_json(inflight / "checkpoint_metric.json", metric)
            previous_manifest = stage1._close_checkpoint(
                inflight,
                closed,
                previous_sha=previous_manifest,
                checkpoint_ordinal=checkpoint_ordinal,
            )
            checkpoint_metrics.append(metric)
            results_by_arm[arm].extend(result_rows)
            checkpoint_ordinal += 1

    if global_ordinal != 1008 or checkpoint_ordinal != 48:
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_EXECUTION_COUNT_DRIFT")
    arm_metrics = {arm: stage1._metric(rows) for arm, rows in results_by_arm.items()}
    per_template = {arm: {template: stage1._metric([r for r in rows if str(r["template_id"]) == template]) for template in TEMPLATES} for arm, rows in results_by_arm.items()}
    round_metrics = {
        arm: {
            str(round_index): stage1._metric([
                row for row in results_by_arm[arm]
                if int(row["stage2_round_index"]) == round_index
            ])
            for round_index in range(3)
        }
        for arm in ARMS
    }
    if any(int(round_metrics[arm][str(round_index)]["evaluated"]) != 168 for arm in ARMS for round_index in range(3)):
        raise RuntimeError("SEARCH_CORE_V2_STAGE2_ROUND_COVERAGE_DRIFT")
    decision = _decision(arm_metrics[PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1], arm_metrics[SEMANTIC_STATE_JUMP_GENERATOR_V2], per_template)
    wall = time.perf_counter() - started
    final_snapshot = state_jump.snapshot()
    closure = engine._self_hashed({
        "schema_version": "cn_search_core_v2_stage2_complete_v1",
        "status": "SEARCH_CORE_V2_STAGE2_COMPLETE",
        "repo_sha": str(admission["repo_sha"]),
        "authorization_payload_sha256": str(authorization["authorization_payload_sha256"]),
        "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
        "source_stage15_terminal_payload_sha256": str(prefreeze["source_stage15_terminal"]["payload_sha256"]),
        "initial_mature_snapshot_payload_sha256": initial_snapshot_hash,
        "final_mature_snapshot_payload_sha256": str(final_snapshot["snapshot_hash"]),
        "evaluated": 1008, "evaluated_per_arm": 504, "checkpoint_count": 48,
        "template_batch_size": dict(prefreeze["stage2"]["template_batch_size"]),
        "last_checkpoint_manifest_file_sha256": previous_manifest,
        "arm_metrics": arm_metrics, "per_template_metrics": per_template,
        "round_metrics": round_metrics, "checkpoint_metrics": checkpoint_metrics, "decision": decision,
        "state_jump_final_diagnostics": state_jump.generator.diagnostics(),
        "wall_seconds": wall,
        "wall_seconds_per_productive": {arm: wall / float(m["productive"]) if int(m["productive"]) > 0 else None for arm, m in arm_metrics.items()},
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "restricted_reads": {"validation":0,"holdout":0,"historical_2023":0,"forward_b":0,"forward_2026":0},
        "validation_feedback_used": False, "promotion_authorized": False,
        "oos_authority": "NONE", "automatic_policy_change_authorized": False,
    }, "closure_payload_sha256")
    engine._write_json(root / "CN_SEARCH_CORE_V2_STAGE2_COMPLETE.json", closure)
    engine._write_json(root / "FINAL_MATURE_OPTIMIZER_STATE.json", final_snapshot)
    return closure

__all__ = ["verify_prefreeze", "run", "_decision"]
