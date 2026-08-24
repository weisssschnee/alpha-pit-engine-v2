"""Search Core V2 Stage-1.5 mature-state continuation confirmation."""
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
from our_system_phase2.runtime import cn_search_core_v2_stage15_v1 as runtime
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
    _verify_self(payload, "prefreeze_payload_sha256", "Search Core V2 Stage-1.5 prefreeze")
    if (
        payload.get("status") != "SEARCH_CORE_V2_STAGE15_PREFROZEN_BEFORE_FINANCIAL_READ"
        or int(payload["stage15"]["total_budget_per_arm"]) != 168
        or int(payload["stage15"]["total_financial_evaluations"]) != 336
        or bool(payload.get("financial_labels_read_by_builder"))
        or bool(payload.get("candidate_evaluation_executed"))
        or bool(payload.get("automatic_stage2_authorized"))
        or any(int(v) != 0 for v in dict(payload["restricted_reads"]).values())
    ):
        raise RuntimeError("SEARCH_CORE_V2_STAGE15_PREFREEZE_CONTRACT_DRIFT")
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
        raise RuntimeError("SEARCH_CORE_V2_STAGE15_ARM_A_NOT_EXECUTABLE")
    exact = stable_hash(dict(entry["program_genes"]))
    if exact != str(candidate["exact_identity"]):
        raise RuntimeError("SEARCH_CORE_V2_STAGE15_ARM_A_EXACT_DRIFT")
    ask = stage1._ask_record(
        global_ordinal=global_ordinal,
        checkpoint_ordinal=checkpoint_ordinal,
        template_id=str(candidate["template_id"]),
        template_ordinal=template_ordinal,
        arm=PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
    )
    decision = engine._self_hashed(
        {
            "schema_version": "cn_search_core_v2_stage15_arm_a_selection_v1",
            "selection_mode": "PREFROZEN_PRIMITIVE_STAGE1_ORDER_INDEX_48_72",
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
        "search_core_stage": "STAGE1_5",
    })
    schedule["schedule_record_sha256"] = stable_hash(
        {k: v for k, v in schedule.items() if k != "schedule_record_sha256"}
    )
    return schedule


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
        and b_stable >= a_stable * 0.97
        and b_beh >= a_beh * 0.95
        and wins >= 4
        and max_fraction is not None and max_fraction <= 0.50
    )
    clear_loss = b_prod < a_prod * 0.95 and b_stable <= a_stable and b_beh <= a_beh
    if confirmation:
        status = "MATURE_GENERATOR_STAGE15_CONFIRMATION_PASS_STAGE2_REVIEW_ELIGIBLE"
    elif clear_loss:
        status = "MATURE_GENERATOR_STAGE15_CLEAR_LOSS_STOP"
    else:
        status = "AMBIGUOUS_MATURE_STATE_CONFIRMATION_NO_STAGE2"
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
        "automatic_stage2_authorized": False,
    }


def run(args: argparse.Namespace, *, admission: Mapping[str, Any], authorization: Mapping[str, Any]) -> dict[str, Any]:
    root = args.output_root.resolve()
    if (
        not root.is_dir() or not (root / ".project_control_execution").is_dir()
        or {p.name for p in root.iterdir()} != {".project_control_execution"}
    ):
        raise RuntimeError("SEARCH_CORE_V2_STAGE15_ADMITTED_ROOT_NOT_CLEAN")
    repo = Path(__file__).resolve().parents[1]
    prefreeze = verify_prefreeze(args.stage15_prefreeze)
    authority = stage1._load_authority(args, authorization=authorization, repo_sha=str(admission["repo_sha"]))
    stage_d_supply = stage1._stage_d_supply(repo, prefreeze)
    supply_by_exact = {str(row["exact_identity"]): dict(row) for row in stage_d_supply["fresh_entries"]}
    selected = {
        template: list(map(str, prefreeze["arm_a_primitive"]["selected_by_template"][template]))
        for template in TEMPLATES
    }
    if any(len(rows) != 24 for rows in selected.values()):
        raise RuntimeError("SEARCH_CORE_V2_STAGE15_ARM_A_BUDGET_DRIFT")

    snapshot_path = repo / Path(str(prefreeze["arm_b_state_jump"]["source_snapshot_relative_path"]))
    if engine._sha256(snapshot_path) != str(prefreeze["arm_b_state_jump"]["source_snapshot_file_sha256"]):
        raise RuntimeError("SEARCH_CORE_V2_STAGE15_SNAPSHOT_FILE_DRIFT")
    snapshot = _read(snapshot_path)
    if str(snapshot.get("snapshot_hash") or "") != str(prefreeze["arm_b_state_jump"]["source_snapshot_payload_sha256"]):
        raise RuntimeError("SEARCH_CORE_V2_STAGE15_SNAPSHOT_PAYLOAD_DRIFT")
    state_jump = StateJumpProgramSearchAdapterV2.restore(
        snapshot=snapshot,
        adapter=authority["adapter"], compiler=authority["compiler"],
        components_by_role=stage1._components_by_role(authority), seed=0,
    )
    initial_snapshot_hash = str(state_jump.snapshot()["snapshot_hash"])

    engine._write_json(root / "input_binding.json", authority["input_binding"])
    engine._write_json(root / "search_core_v2_stage15_policy_binding.json", {
        "schema_version": "cn_search_core_v2_stage15_policy_binding_v1",
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

    for template in TEMPLATES:
        for arm in ARMS:
            inflight = root / f"checkpoint_{checkpoint_ordinal:04d}.inflight"
            closed = root / f"checkpoint_{checkpoint_ordinal:04d}"
            inflight.mkdir()
            if arm == PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1:
                schedules = []
                for exact in selected[template]:
                    schedule = _static_schedule(
                        supply_by_exact[exact], authority=authority,
                        global_ordinal=global_ordinal, checkpoint_ordinal=checkpoint_ordinal,
                        template_ordinal=template_ordinals[(arm, template)],
                    )
                    schedules.append(schedule)
                    global_ordinal += 1; template_ordinals[(arm, template)] += 1
                engine._write_json(inflight / "optimizer_state_before.json", {"arm": arm, "learning": False, "prefrozen": True})
            else:
                engine._write_json(inflight / "optimizer_state_before.json", state_jump.snapshot())
                schedules, _ = stage1._generated_schedules(
                    state_jump, authority=authority, template_id=template,
                    checkpoint_id=f"SEARCH_CORE_V2_STAGE15_{template}",
                    checkpoint_ordinal=checkpoint_ordinal, global_start=global_ordinal,
                    template_start=template_ordinals[(arm, template)],
                )
                global_ordinal += len(schedules); template_ordinals[(arm, template)] += len(schedules)
            for schedule in schedules:
                schedule["search_core_round_index"] = 2
                schedule["search_core_stage"] = "STAGE1_5"
                schedule["schedule_record_sha256"] = stable_hash({k: v for k, v in schedule.items() if k != "schedule_record_sha256"})
            engine._write_jsonl(inflight / "selected_schedule.jsonl", schedules)

            records = successor._evaluate_schedules(
                schedules, record_root=inflight / "records", authority=authority,
                input_hash=input_hash, executor_workers=PRIMARY_EXECUTOR_WORKERS,
            )
            for record in records:
                if any(int(record.get(k) or 0) != 0 for k in ("validation_reads","holdout_reads","historical_2023_reads","forward_b_reads","forward_2026_reads")):
                    raise RuntimeError("SEARCH_CORE_V2_STAGE15_RESTRICTED_READ_DRIFT")
            by_ordinal = {int(s["main_record_ordinal"]): s for s in schedules}
            result_rows = []
            observations = []
            for record in records:
                schedule = by_ordinal[int(record["main_record_ordinal"])]
                result, physical = stage1._result_record(record, schedule)
                result["search_core_stage"] = "STAGE1_5"
                result_rows.append(result)
                if arm == SEMANTIC_STATE_JUMP_GENERATOR_V2:
                    ask = dict(schedule["optimizer_ask"])
                    observations.append(ProgramOptimizerObservationV1(
                        proposal_id=str(ask["proposal_id"]), exact_identity=str(ask["exact_identity"]),
                        admission=physical.admission, uplift=physical.uplift,
                    ))
            if arm == SEMANTIC_STATE_JUMP_GENERATOR_V2:
                tell = state_jump.tell(observations)
                engine._write_json(inflight / "optimizer_tell_receipt.json", tell)
                engine._write_json(inflight / "optimizer_state_after.json", state_jump.snapshot())
            else:
                engine._write_json(inflight / "optimizer_state_after.json", {"arm": arm, "learning": False, "prefrozen": True})
            engine._write_jsonl(inflight / "candidate_results.jsonl", result_rows)
            metric = {"checkpoint_ordinal": checkpoint_ordinal, "template_id": template, "arm": arm, **stage1._metric(result_rows)}
            engine._write_json(inflight / "checkpoint_metric.json", metric)
            previous_manifest = stage1._close_checkpoint(inflight, closed, previous_sha=previous_manifest, checkpoint_ordinal=checkpoint_ordinal)
            checkpoint_metrics.append(metric); results_by_arm[arm].extend(result_rows); checkpoint_ordinal += 1

    if global_ordinal != 336 or checkpoint_ordinal != 14:
        raise RuntimeError("SEARCH_CORE_V2_STAGE15_EXECUTION_COUNT_DRIFT")
    arm_metrics = {arm: stage1._metric(rows) for arm, rows in results_by_arm.items()}
    per_template = {arm: {template: stage1._metric([r for r in rows if str(r["template_id"]) == template]) for template in TEMPLATES} for arm, rows in results_by_arm.items()}
    decision = _decision(arm_metrics[PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1], arm_metrics[SEMANTIC_STATE_JUMP_GENERATOR_V2], per_template)
    wall = time.perf_counter() - started
    final_snapshot = state_jump.snapshot()
    closure = engine._self_hashed({
        "schema_version": "cn_search_core_v2_stage15_complete_v1",
        "status": "SEARCH_CORE_V2_STAGE15_COMPLETE",
        "repo_sha": str(admission["repo_sha"]),
        "authorization_payload_sha256": str(authorization["authorization_payload_sha256"]),
        "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
        "source_stage1_terminal_payload_sha256": str(prefreeze["source_stage1_terminal"]["payload_sha256"]),
        "initial_mature_snapshot_payload_sha256": initial_snapshot_hash,
        "final_mature_snapshot_payload_sha256": str(final_snapshot["snapshot_hash"]),
        "evaluated": 336, "evaluated_per_arm": 168, "checkpoint_count": 14,
        "last_checkpoint_manifest_file_sha256": previous_manifest,
        "arm_metrics": arm_metrics, "per_template_metrics": per_template,
        "checkpoint_metrics": checkpoint_metrics, "decision": decision,
        "state_jump_final_diagnostics": state_jump.generator.diagnostics(),
        "wall_seconds": wall,
        "wall_seconds_per_productive": {arm: wall / float(m["productive"]) if int(m["productive"]) > 0 else None for arm, m in arm_metrics.items()},
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "restricted_reads": {"validation":0,"holdout":0,"historical_2023":0,"forward_b":0,"forward_2026":0},
        "validation_feedback_used": False, "promotion_authorized": False,
        "oos_authority": "NONE", "automatic_stage2_authorized": False,
    }, "closure_payload_sha256")
    engine._write_json(root / "CN_SEARCH_CORE_V2_STAGE15_COMPLETE.json", closure)
    engine._write_json(root / "FINAL_MATURE_OPTIMIZER_STATE.json", final_snapshot)
    return closure

__all__ = ["verify_prefreeze", "run", "_decision"]
