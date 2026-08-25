"""Single-arm mature State-Jump Generator production Wave 4 with pair-native development annotation."""
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
from scripts import run_cn_search_core_v2_stage2_v1 as stage2
from our_system_phase2.services.program_search_core_policy_v2 import validate_search_core_v2_mature_continuation_snapshot
from our_system_phase2.services.program_search_optimizer_v1 import ProgramOptimizerObservationV1
from our_system_phase2.services.program_search_state_jump_adapter_v2 import StateJumpProgramSearchAdapterV2
from our_system_phase2.services.program_search_state_jump_generator_v2 import SEMANTIC_STATE_JUMP_GENERATOR_V2
from our_system_phase2.services.unified_capability_registry import stable_hash

TEMPLATES = (
    "BASE_EVENT",
    "BASE_MARKET_EVENT",
    "BASE_TEMPORAL_EVENT",
    "BASE_TEMPORAL_MARKET_EVENT",
)
ARM = SEMANTIC_STATE_JUMP_GENERATOR_V2
ROUNDS = 2
BATCH_SIZE = 24
TOTAL_EVALUATIONS = 192
CHECKPOINT_COUNT = 8
SOURCE_OBSERVATIONS = 1920


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


PAIR_WINDOWS = ("development_1", "development_2", "development_3")


def _pair_native_annotation(record: Mapping[str, Any], contract: Mapping[str, Any]) -> dict[str, Any]:
    rule = dict(contract.get("rule_contract") or {})
    rule_id = str(contract.get("rule_id") or "")
    primary = record.get("primary")
    control = record.get("base_control")
    if primary is None or control is None:
        return {
            "schema_version": "cn_search_core_v2_pair_native_development_annotation_v1",
            "available": False,
            "rule_id": rule_id,
            "primary_positive_window_count": None,
            "control_positive_window_count": None,
            "challenger_hit": False,
            "annotation_only": True,
            "optimizer_feedback_used": False,
        }
    primary_windows = {str(row["window_id"]): dict(row) for row in primary["development_subwindows"]}
    control_windows = {str(row["window_id"]): dict(row) for row in control["development_subwindows"]}
    if set(primary_windows) != set(PAIR_WINDOWS) or set(control_windows) != set(PAIR_WINDOWS):
        raise RuntimeError("PRODUCTION_WAVE4_PAIR_NATIVE_WINDOW_DRIFT")
    primary_returns = [float(primary_windows[window]["cumulative_net_return"]) for window in PAIR_WINDOWS]
    control_returns = [float(control_windows[window]["cumulative_net_return"]) for window in PAIR_WINDOWS]
    primary_positive = sum(value > 0.0 for value in primary_returns)
    control_positive = sum(value > 0.0 for value in control_returns)
    required_primary = int(rule.get("primary_positive_development_window_count_required") or 0)
    minimum_control = int(rule.get("control_positive_development_window_count_minimum") or 0)
    hit = primary_positive >= required_primary and control_positive >= minimum_control
    return {
        "schema_version": "cn_search_core_v2_pair_native_development_annotation_v1",
        "available": True,
        "rule_id": rule_id,
        "window_ids": list(PAIR_WINDOWS),
        "primary_cumulative_net_return": float(primary["cumulative_net_return"]),
        "control_cumulative_net_return": float(control["cumulative_net_return"]),
        "primary_window_returns": primary_returns,
        "control_window_returns": control_returns,
        "primary_positive_window_count": primary_positive,
        "control_positive_window_count": control_positive,
        "challenger_hit": bool(hit),
        "annotation_only": True,
        "optimizer_feedback_used": False,
    }

def verify_prefreeze(path: Path) -> dict[str, Any]:
    payload = _read(path)
    _verify(payload, "prefreeze_payload_sha256", "Production Wave 4 prefreeze")
    wave = dict(payload.get("production_wave4") or {})
    mature = dict(payload.get("mature_state") or {})
    pair_contract = dict(payload.get("pair_native_annotation_contract") or {})
    if (
        payload.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE4_PREFROZEN_BEFORE_FINANCIAL_READ"
        or wave.get("arm") != ARM
        or int(wave.get("rounds") or 0) != ROUNDS
        or int(wave.get("batch_size") or 0) != BATCH_SIZE
        or int(wave.get("total_financial_evaluations") or 0) != TOTAL_EVALUATIONS
        or int(wave.get("total_checkpoint_count") or 0) != CHECKPOINT_COUNT
        or int(mature.get("source_development_observations") or 0) != SOURCE_OBSERVATIONS
        or int(mature.get("expected_final_development_observations") or 0) != SOURCE_OBSERVATIONS + TOTAL_EVALUATIONS
        or bool(payload.get("financial_labels_read_by_builder"))
        or bool(payload.get("candidate_evaluation_executed"))
        or pair_contract.get("application_scope") != "DEVELOPMENT_RESULT_ANNOTATION_AND_ARCHIVE_ONLY"
        or pair_contract.get("optimizer_feedback_write") is not False
        or pair_contract.get("generator_ask_order_changed") is not False
        or pair_contract.get("candidate_admission_changed") is not False
        or pair_contract.get("automatic_policy_adoption") is not False
        or pair_contract.get("validation_label_read_at_application") is not False
        or any(int(value) != 0 for value in dict(payload.get("restricted_reads") or {}).values())
    ):
        raise RuntimeError("Production Wave 4 prefreeze contract drift")
    return payload


def _execution_plan() -> list[dict[str, Any]]:
    rows = [
        {"production_round_index": round_index, "template_id": template, "batch_size": BATCH_SIZE}
        for round_index in range(ROUNDS)
        for template in TEMPLATES
    ]
    if len(rows) != CHECKPOINT_COUNT or sum(row["batch_size"] for row in rows) != TOTAL_EVALUATIONS:
        raise RuntimeError("Production Wave 4 execution plan drift")
    return rows


def run(args: argparse.Namespace, *, admission: Mapping[str, Any], authorization: Mapping[str, Any]) -> dict[str, Any]:
    root = args.output_root.resolve()
    if (
        not root.is_dir()
        or not (root / ".project_control_execution").is_dir()
        or {path.name for path in root.iterdir()} != {".project_control_execution"}
    ):
        raise RuntimeError("PRODUCTION_WAVE4_ADMITTED_ROOT_NOT_CLEAN")
    repo = Path(__file__).resolve().parents[1]
    prefreeze = verify_prefreeze(args.production_wave4_prefreeze)
    pair_contract = dict(prefreeze["pair_native_annotation_contract"])
    authority = stage1._load_authority(args, authorization=authorization, repo_sha=str(admission["repo_sha"]))
    state_path = repo / Path(str(prefreeze["mature_state"]["relative_path"]))
    if engine._sha256(state_path) != str(prefreeze["mature_state"]["file_sha256"]):
        raise RuntimeError("PRODUCTION_WAVE4_SOURCE_STATE_FILE_DRIFT")
    snapshot = _read(state_path)
    if str(snapshot.get("snapshot_hash") or "") != str(prefreeze["mature_state"]["payload_sha256"]):
        raise RuntimeError("PRODUCTION_WAVE4_SOURCE_STATE_PAYLOAD_DRIFT")
    policy = validate_search_core_v2_mature_continuation_snapshot(snapshot)
    if policy["primary_arm"] != ARM or int(policy["development_observations"]) != SOURCE_OBSERVATIONS:
        raise RuntimeError("PRODUCTION_WAVE4_PRIMARY_POLICY_DRIFT")
    state = StateJumpProgramSearchAdapterV2.restore(
        snapshot=snapshot,
        adapter=authority["adapter"],
        compiler=authority["compiler"],
        components_by_role=stage1._components_by_role(authority),
        seed=0,
    )
    initial_hash = str(state.snapshot()["snapshot_hash"])
    engine._write_json(root / "input_binding.json", authority["input_binding"])
    engine._write_json(
        root / "search_core_v2_production_wave4_policy_binding.json",
        {
            "schema_version": "cn_search_core_v2_production_wave4_policy_binding_v1",
            "prefreeze_payload_sha256": prefreeze["prefreeze_payload_sha256"],
            "policy_review_payload_sha256": prefreeze["source_policy_review"]["payload_sha256"],
            "policy_id": prefreeze["source_policy_review"]["policy_id"],
            "source_mature_snapshot_payload_sha256": initial_hash,
            "source_mature_observations": SOURCE_OBSERVATIONS,
            "pair_native_rule_id": pair_contract["rule_id"],
            "pair_native_challenger_payload_sha256": pair_contract["source_payload_sha256"],
            "pair_native_application_scope": pair_contract["application_scope"],
            "pair_native_optimizer_feedback_write": False,
            "validation_feedback_used": False,
        },
    )
    input_hash = str(authority["input_binding"]["input_binding_sha256"])
    previous_manifest = "GENESIS"
    results: list[dict[str, Any]] = []
    checkpoint_metrics: list[dict[str, Any]] = []
    template_ordinals: Counter[str] = Counter()
    global_ordinal = 0
    checkpoint_ordinal = 0
    started = time.perf_counter()

    for step in _execution_plan():
        round_index = int(step["production_round_index"])
        template = str(step["template_id"])
        inflight = root / f"checkpoint_{checkpoint_ordinal:04d}.inflight"
        closed = root / f"checkpoint_{checkpoint_ordinal:04d}"
        inflight.mkdir()
        engine._write_json(inflight / "optimizer_state_before.json", state.snapshot())
        schedules = stage2._generated_schedules_batch(
            state,
            authority=authority,
            template_id=template,
            checkpoint_id=f"SEARCH_CORE_V2_PRODUCTION_WAVE4_R{round_index}_{template}",
            checkpoint_ordinal=checkpoint_ordinal,
            global_start=global_ordinal,
            template_start=template_ordinals[template],
            batch_size=BATCH_SIZE,
        )
        global_ordinal += len(schedules)
        template_ordinals[template] += len(schedules)
        for schedule in schedules:
            schedule["production_wave_id"] = "SEARCH_CORE_V2_PRODUCTION_WAVE4_V1"
            schedule["production_round_index"] = round_index
            schedule["search_core_round_index"] = 12 + round_index
            schedule["search_core_stage"] = "PRODUCTION_WAVE4"
            schedule["schedule_record_sha256"] = stable_hash(
                {key: value for key, value in schedule.items() if key != "schedule_record_sha256"}
            )
        engine._write_jsonl(inflight / "selected_schedule.jsonl", schedules)
        records = successor._evaluate_schedules(
            schedules,
            record_root=inflight / "records",
            authority=authority,
            input_hash=input_hash,
            executor_workers=BATCH_SIZE,
        )
        for record in records:
            if any(
                int(record.get(key) or 0) != 0
                for key in ("validation_reads", "holdout_reads", "historical_2023_reads", "forward_b_reads", "forward_2026_reads")
            ):
                raise RuntimeError("PRODUCTION_WAVE4_RESTRICTED_READ_DRIFT")
        by_ordinal = {int(schedule["main_record_ordinal"]): schedule for schedule in schedules}
        result_rows: list[dict[str, Any]] = []
        observations: list[ProgramOptimizerObservationV1] = []
        for record in records:
            schedule = by_ordinal[int(record["main_record_ordinal"])]
            result, physical = stage1._result_record(record, schedule)
            result["production_wave_id"] = "SEARCH_CORE_V2_PRODUCTION_WAVE4_V1"
            result["production_round_index"] = round_index
            result["search_core_stage"] = "PRODUCTION_WAVE4"
            result["pair_native_robustness"] = _pair_native_annotation(record, pair_contract)
            result["result_payload_sha256"] = stable_hash(
                {key: value for key, value in result.items() if key != "result_payload_sha256"}
            )
            result_rows.append(result)
            ask = dict(schedule["optimizer_ask"])
            observations.append(
                ProgramOptimizerObservationV1(
                    proposal_id=str(ask["proposal_id"]),
                    exact_identity=str(ask["exact_identity"]),
                    admission=physical.admission,
                    uplift=physical.uplift,
                )
            )
        tell = state.tell(observations)
        engine._write_json(inflight / "optimizer_tell_receipt.json", tell)
        engine._write_json(inflight / "optimizer_state_after.json", state.snapshot())
        engine._write_jsonl(inflight / "candidate_results.jsonl", result_rows)
        metric = {
            "checkpoint_ordinal": checkpoint_ordinal,
            "production_round_index": round_index,
            "template_id": template,
            "arm": ARM,
            **stage1._metric(result_rows),
            "pair_native_available_count": sum(bool(row["pair_native_robustness"]["available"]) for row in result_rows),
            "pair_native_rule_hit_count": sum(bool(row["pair_native_robustness"]["challenger_hit"]) for row in result_rows),
            "pair_native_productive_rule_hit_count": sum(
                bool(row["productive"]) and bool(row["pair_native_robustness"]["challenger_hit"])
                for row in result_rows
            ),
            "pair_native_stable_rule_hit_count": sum(
                bool(row["stable"]) and bool(row["pair_native_robustness"]["challenger_hit"])
                for row in result_rows
            ),
        }
        engine._write_json(inflight / "checkpoint_metric.json", metric)
        previous_manifest = stage1._close_checkpoint(
            inflight, closed, previous_sha=previous_manifest, checkpoint_ordinal=checkpoint_ordinal
        )
        checkpoint_metrics.append(metric)
        results.extend(result_rows)
        checkpoint_ordinal += 1

    if global_ordinal != TOTAL_EVALUATIONS or checkpoint_ordinal != CHECKPOINT_COUNT or len(results) != TOTAL_EVALUATIONS:
        raise RuntimeError("PRODUCTION_WAVE4_EXECUTION_COUNT_DRIFT")
    exacts = [str(row["exact_identity"]) for row in results]
    if len(set(exacts)) != TOTAL_EVALUATIONS:
        raise RuntimeError("PRODUCTION_WAVE4_EXACT_DUPLICATE")
    overall = stage1._metric(results)
    per_template = {
        template: stage1._metric([row for row in results if str(row["template_id"]) == template])
        for template in TEMPLATES
    }
    round_metrics = {
        str(round_index): stage1._metric(
            [row for row in results if int(row["production_round_index"]) == round_index]
        )
        for round_index in range(ROUNDS)
    }
    productive = [row for row in results if bool(row["productive"])]
    stable = [row for row in results if bool(row["stable"])]
    pair_native_available = [row for row in results if bool(row["pair_native_robustness"]["available"])]
    pair_native_hits = [row for row in results if bool(row["pair_native_robustness"]["challenger_hit"])]
    pair_native_challenger = [
        row for row in productive if bool(row["pair_native_robustness"]["challenger_hit"])
    ]
    pair_native_stable_challenger = [
        row for row in stable if bool(row["pair_native_robustness"]["challenger_hit"])
    ]
    engine._write_jsonl(root / "PRODUCTIVE_CANDIDATES.jsonl", productive)
    engine._write_jsonl(root / "STABLE_CANDIDATES.jsonl", stable)
    engine._write_jsonl(root / "PAIR_NATIVE_CHALLENGER_CANDIDATES.jsonl", pair_native_challenger)
    final_state = state.snapshot()
    history_count = len(final_state["history"])
    generated_count = len(final_state["generated_exact_identities"])
    observations = int(final_state["history"][-1]["generator_diagnostics"]["memory_observations"])
    if (
        observations != int(prefreeze["mature_state"]["expected_final_development_observations"])
        or history_count != int(prefreeze["mature_state"]["expected_final_history_count"])
        or generated_count != int(prefreeze["mature_state"]["expected_final_generated_exact_count"])
    ):
        raise RuntimeError("PRODUCTION_WAVE4_MATURE_STATE_LINEAGE_DRIFT")
    engine._write_json(root / "FINAL_MATURE_OPTIMIZER_STATE.json", final_state)
    wall = time.perf_counter() - started
    closure = engine._self_hashed(
        {
            "schema_version": "cn_search_core_v2_production_wave4_complete_v1",
            "status": "SEARCH_CORE_V2_PRODUCTION_WAVE4_COMPLETE",
            "repo_sha": str(admission["repo_sha"]),
            "authorization_payload_sha256": str(authorization["authorization_payload_sha256"]),
            "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
            "policy_review_payload_sha256": str(prefreeze["source_policy_review"]["payload_sha256"]),
            "initial_mature_snapshot_payload_sha256": initial_hash,
            "final_mature_snapshot_payload_sha256": str(final_state["snapshot_hash"]),
            "source_mature_observations": SOURCE_OBSERVATIONS,
            "final_mature_observations": observations,
            "evaluated": TOTAL_EVALUATIONS,
            "checkpoint_count": CHECKPOINT_COUNT,
            "last_checkpoint_manifest_file_sha256": previous_manifest,
            "overall_metrics": overall,
            "per_template_metrics": per_template,
            "round_metrics": round_metrics,
            "checkpoint_metrics": checkpoint_metrics,
            "productive_candidate_count": len(productive),
            "stable_candidate_count": len(stable),
            "productive_archive_file_sha256": engine._sha256(root / "PRODUCTIVE_CANDIDATES.jsonl"),
            "stable_archive_file_sha256": engine._sha256(root / "STABLE_CANDIDATES.jsonl"),
            "pair_native_annotation": {
                "rule_id": pair_contract["rule_id"],
                "source_payload_sha256": pair_contract["source_payload_sha256"],
                "application_scope": pair_contract["application_scope"],
                "available_count": len(pair_native_available),
                "rule_hit_count": len(pair_native_hits),
                "productive_rule_hit_count": len(pair_native_challenger),
                "stable_rule_hit_count": len(pair_native_stable_challenger),
                "productive_rule_hit_rate": len(pair_native_challenger) / len(productive) if productive else None,
                "archive_file_sha256": engine._sha256(root / "PAIR_NATIVE_CHALLENGER_CANDIDATES.jsonl"),
                "optimizer_feedback_write": False,
                "candidate_admission_changed": False,
                "generator_ask_order_changed": False,
            },
            "state_jump_final_diagnostics": state.generator.diagnostics(),
            "wall_seconds": wall,
            "wall_seconds_per_productive": wall / len(productive) if productive else None,
            "evaluation_data_role": "DEVELOPMENT_ONLY",
            "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
            "validation_feedback_used": False,
            "promotion_authorized": False,
            "oos_authority": "NONE",
            "capital_action_authorized": False,
            "automatic_followon_authorized": False,
        },
        "closure_payload_sha256",
    )
    engine._write_json(root / "CN_SEARCH_CORE_V2_PRODUCTION_WAVE4_COMPLETE.json", closure)
    return closure


__all__ = ["verify_prefreeze", "run", "_execution_plan"]
