from __future__ import annotations

import argparse, json, shutil, time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_large_fresh_v1 as large
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as successor
from scripts import run_cn_program_optimizer_tournament_v1 as tournament
from scripts import run_cn_program_primitive_main_production_v1 as base
from scripts.run_cn_program_optimizer_large_fresh_v3 import ARMS, EVOLUTION_CONFIG, _checkpoint_arm_v3
from our_system_phase2.services.program_optimizer_large_fresh_v3 import LargeFreshProgramBanditV3
from our_system_phase2.services.project_control_admission import sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash

CAMPAIGN_ID = "CN_PROGRAM_PRIMITIVE_MAIN_PRODUCTION_RECOVERY_V1"
CAMPAIGN_PROFILE = "cn_program_primitive_main_production_recovery_v1"
STATUS_COMPLETE = "PRIMITIVE_MAIN_PRODUCTION_RECOVERY_COMPLETE"
CLOSURE_NAME = "CN_PROGRAM_PRIMITIVE_MAIN_PRODUCTION_RECOVERY_COMPLETE.json"
RECOVERY_STATUS = "PRIMITIVE_MAIN_RECOVERY_PREFIX_FROZEN"
RECOVERED_RECORDS = 24
FINAL_HARD_CAP = 840
TEMPLATES = base.TEMPLATES


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def verify_recovery_prefix(path: Path) -> dict[str, Any]:
    payload = base._read(path)
    body = dict(payload)
    claimed = str(body.pop("recovery_prefix_payload_sha256", ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_PREFIX_HASH_DRIFT")
    if (
        payload.get("status") != RECOVERY_STATUS
        or int(payload.get("completed_logical_records") or 0) != RECOVERED_RECORDS
        or int(payload.get("remaining_new_evaluations") or 0) != FINAL_HARD_CAP - RECOVERED_RECORDS
        or int(payload.get("final_total_logical_records") or 0) != FINAL_HARD_CAP
        or int(payload.get("resume_checkpoint_ordinal") or -1) != 1
        or int(payload.get("resume_macro_index", -1)) != 0
        or int(payload.get("resume_template_index") or -1) != 1
        or int(payload.get("resume_main_record_ordinal") or -1) != 24
        or payload.get("source_checkpoint_optimizer_arm") != "UNIFORM_CONTROL"
        or payload.get("source_checkpoint1_generation_arm") != "PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1"
        or int(payload.get("source_checkpoint1_candidate_records", -1)) != 0
        or payload.get("financial_evaluator_reexecution_authorized") is not False
        or payload.get("financial_evaluator_reexecution_performed") is not False
        or payload.get("recovery_copy_only") is not True
        or payload.get("search_design_changed") is not False
        or any(int(v) != 0 for v in dict(payload.get("restricted_reads") or {}).values())
        or payload.get("promotion_authorized") is not False
        or payload.get("oos_authority") != "NONE"
        or payload.get("automatic_successor_authorized") is not False
    ):
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_PREFIX_CONTRACT_DRIFT")
    if len(payload.get("selected_normalized_exact_identities") or []) != RECOVERED_RECORDS or len(payload.get("selected_physical_exact_identities") or []) != RECOVERED_RECORDS:
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_PREFIX_EXACT_COUNT_DRIFT")
    return payload


def _selection_state_from_record(record: Mapping[str, Any]) -> dict[str, Any]:
    state = engine._selection_state()
    state["program_ids"].update(map(str, record.get("program_ids") or ()))
    state["reservoir_ids"].update(map(str, record.get("reservoir_ids") or ()))
    state["component_ids"].update(map(str, record.get("component_ids") or ()))
    state["combination_ids"].update(map(str, record.get("combination_ids") or ()))
    for template, counts in dict(record.get("base_counts") or {}).items():
        for base_id, count in dict(counts).items():
            state["base_counts"][(str(template), str(base_id))] = int(count)
    if large._selection_state_record(state) != dict(record):
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_SELECTION_STATE_DRIFT")
    return state


def _verify_source_checkpoint(recovery: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    source_root = Path(str(recovery["source_output_root"])).resolve()
    checkpoint = source_root / str(recovery["source_checkpoint_relative_path"])
    if not checkpoint.is_dir():
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_SOURCE_CHECKPOINT_MISSING")
    manifest_path = checkpoint / "checkpoint_manifest.json"
    if sha256_file(manifest_path) != str(recovery["source_checkpoint_manifest_file_sha256"]):
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_MANIFEST_FILE_DRIFT")
    manifest = base._read(manifest_path)
    body = dict(manifest)
    claimed = str(body.pop("manifest_payload_sha256", ""))
    if claimed != str(recovery["source_checkpoint_manifest_payload_sha256"]) or stable_hash(body) != claimed:
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_MANIFEST_PAYLOAD_DRIFT")
    for artifact in manifest["artifacts"]:
        p = checkpoint / str(artifact["path"])
        if not p.is_file() or p.stat().st_size != int(artifact["bytes"]) or sha256_file(p) != str(artifact["sha256"]):
            raise RuntimeError(f"PRIMITIVE_MAIN_RECOVERY_ARTIFACT_DRIFT:{artifact['path']}")
    return checkpoint, manifest


def _restore_runtime_state(plan: Mapping[str, Any], fresh: Mapping[str, Any], repo_root: Path, checkpoint: Path) -> tuple[LargeFreshProgramBanditV3, dict[str, Any]]:
    initial = base._bandit(plan, fresh, repo_root)
    snapshot = base._read(checkpoint / "optimizer_state_after.json")
    bandit = LargeFreshProgramBanditV3.restore(
        snapshot,
        entries_by_arm=initial.entries_by_arm,
        primitive_config=initial.primitive_config,
        evolution_config=initial.evolution_config,
        expected_campaign_id=base.CAMPAIGN_ID,
    )
    state = _selection_state_from_record(base._read(checkpoint / "selection_state_after.json"))
    return bandit, state


def prefinancial_rehearsal(args: argparse.Namespace, *, authorization: Mapping[str, Any], repo_sha: str) -> dict[str, Any]:
    plan = base.verify_plan(args.production_plan)
    recovery = verify_recovery_prefix(args.recovery_prefix)
    repo_root = Path(__file__).resolve().parents[1]
    checkpoint, _ = _verify_source_checkpoint(recovery)
    authority = base._load_authority(args, authorization=authorization, repo_sha=repo_sha)
    fresh = base._fresh_catalog(plan, authority, repo_root)
    bandit, state = _restore_runtime_state(plan, fresh, repo_root, checkpoint)
    prefix_norm = set(map(str, recovery["selected_normalized_exact_identities"]))
    prefix_raw = set(map(str, recovery["selected_physical_exact_identities"]))
    if not prefix_norm <= set(fresh["catalog_by_exact"]) or not prefix_raw <= set(fresh["physical_by_normalized"].values()):
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_PREFIX_OUTSIDE_FRESH_SUPPLY")
    asks = base._ask_rows(0, 1, 1, 24, _checkpoint_arm_v3(0, 1))
    schedules, _ = tournament._select_checkpoint(
        asks,
        catalog=fresh["catalog"],
        bandit=bandit,
        state=state,
        components_by_id=authority["components_by_id"],
        adapter=authority["adapter"],
        compiler=authority["compiler"],
        prior_exact_identities=(),
    )
    norm = [str(s["optimizer_ask"]["exact_identity"]) for s in schedules]
    raw = [str(fresh["physical_by_normalized"][x]) for x in norm]
    if len(schedules) != 24 or set(norm) & prefix_norm or set(raw) & prefix_raw:
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_PREVIEW_OVERLAP")
    fields = base._full_field_union(fresh, authority)
    return {
        "status": "ZERO_FINANCIAL_PRIMITIVE_MAIN_RECOVERY_PREFLIGHT_READY",
        "recovery_prefix_payload_sha256": recovery["recovery_prefix_payload_sha256"],
        "recovered_logical_records": RECOVERED_RECORDS,
        "remaining_new_evaluations": FINAL_HARD_CAP - RECOVERED_RECORDS,
        "resume_checkpoint_ordinal": 1,
        "resume_template_index": 1,
        "preview_asks": 24,
        "preview_generation_arm": _checkpoint_arm_v3(0, 1),
        "preview_prefix_overlap": 0,
        "field_column_count": len(fields),
        "field_columns": list(fields),
        "field_columns_sha256": stable_hash(list(fields)),
        "candidate_evaluation_executed": False,
        "financial_evaluator_reexecution_performed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def _productive_rows(feedback: Sequence[Mapping[str, Any]], schedules: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_ord = {int(s["main_record_ordinal"]): dict(s) for s in schedules}
    output = []
    for f in feedback:
        if not large._productive_feedback(f):
            continue
        s = by_ord[int(f["main_record_ordinal"])]
        output.append({
            "main_record_ordinal": int(f["main_record_ordinal"]),
            "template_id": str(f["template_id"]),
            "generation_arm": str(f["generation_arm"]),
            "fresh_physical_exact_identity": str(s["fresh_physical_exact_identity"]),
            "normalized_search_exact_identity": str(s["optimizer_ask"]["exact_identity"]),
            "program_id": str(s["primary_program"]["program_id"]),
            "absolute_admission": dict(f["absolute_admission"]),
            "enhancer_credit": dict(f["enhancer_credit"]),
        })
    return output


def run(args: argparse.Namespace, *, admission: Mapping[str, Any], authorization: Mapping[str, Any]) -> dict[str, Any]:
    repo_sha = str(admission["repo_sha"])
    repo_root = Path(__file__).resolve().parents[1]
    plan = base.verify_plan(args.production_plan)
    recovery = verify_recovery_prefix(args.recovery_prefix)
    source_checkpoint, source_manifest = _verify_source_checkpoint(recovery)
    authority = base._load_authority(args, authorization=authorization, repo_sha=repo_sha)
    fresh = base._fresh_catalog(plan, authority, repo_root)
    bandit, state = _restore_runtime_state(plan, fresh, repo_root, source_checkpoint)
    root = args.output_root.resolve()
    if not root.is_dir() or not (root / ".project_control_execution").is_dir() or {p.name for p in root.iterdir()} != {".project_control_execution"}:
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_ADMITTED_ROOT_NOT_CLEAN")
    fields = base._full_field_union(fresh, authority)
    input_binding = engine._self_hashed({
        "schema_version": "cn_program_primitive_main_production_recovery_input_binding_v1",
        "repo_sha": repo_sha,
        "authorization_payload_sha256": authorization["authorization_payload_sha256"],
        "production_plan_payload_sha256": plan["plan_payload_sha256"],
        "fresh_supply_payload_sha256": plan["fresh_supply"]["payload_sha256"],
        "recovery_prefix_payload_sha256": recovery["recovery_prefix_payload_sha256"],
        "recovery_checkpoint_manifest_file_sha256": recovery["source_checkpoint_manifest_file_sha256"],
        "recovered_logical_records": RECOVERED_RECORDS,
        "remaining_new_evaluations": FINAL_HARD_CAP - RECOVERED_RECORDS,
        "effective_spent_exact_count_before_production": 6734,
        "field_columns_sha256": stable_hash(list(fields)),
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "financial_evaluator_reexecution_authorized": False,
        "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
    }, "input_binding_sha256")
    large._write_json(root / "input_binding.json", input_binding)
    input_hash = str(input_binding["input_binding_sha256"])
    workers = None
    decision = None
    for w in (24, 16):
        try:
            receipt = base._resource_canary(authority, input_hash, w, fields)
            workers = w
            decision = {"status": "PASS", "selected_executor_workers": w, "attempts": [receipt], "fallback_applied": w != 24, "field_columns": list(fields), "field_columns_sha256": stable_hash(list(fields))}
            break
        except Exception as exc:
            decision = {"status": "FAIL", "error": f"{type(exc).__name__}:{exc}"}
    if workers is None:
        raise RuntimeError(f"PRIMITIVE_MAIN_RECOVERY_RESOURCE_CANARY_FAILED:{decision}")
    large._write_json(root / "resource_canary.json", decision)
    shutil.copytree(source_checkpoint, root / "checkpoint_0000")
    if sha256_file(root / "checkpoint_0000" / "checkpoint_manifest.json") != str(recovery["source_checkpoint_manifest_file_sha256"]):
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_COPIED_PREFIX_DRIFT")
    shutil.copy2(source_checkpoint / "optimizer_state_before.json", root / "optimizer_state_genesis.json")
    shutil.copy2(source_checkpoint / "selection_state_before.json", root / "selection_state_genesis.json")
    large._write_json(root / "recovery_prefix_binding.json", {"recovery_prefix_payload_sha256": recovery["recovery_prefix_payload_sha256"], "source_output_root": recovery["source_output_root"], "source_checkpoint_manifest_file_sha256": recovery["source_checkpoint_manifest_file_sha256"], "financial_evaluator_reexecution_performed": False})

    prefix_schedules = _read_jsonl(source_checkpoint / "selected_schedule.jsonl")
    prefix_feedback = _read_jsonl(source_checkpoint / "feedback.jsonl")
    prefix_records = [base._read(p) for p in sorted((source_checkpoint / "records").glob("record_*.json"))]
    selected_norm = set(map(str, recovery["selected_normalized_exact_identities"]))
    selected_raw = set(map(str, recovery["selected_physical_exact_identities"]))
    all_feedback = list(prefix_feedback)
    all_records = list(prefix_records)
    all_schedules = list(prefix_schedules)
    productive = _productive_rows(prefix_feedback, prefix_schedules)
    seen_pairs: set[str] = set()
    macros: list[dict[str, Any]] = []
    previous = sha256_file(root / "checkpoint_0000" / "checkpoint_manifest.json")
    checkpoint = 1
    next_ord = 24
    start = time.perf_counter()
    stop_reason = "HARD_CAP_REACHED"
    physical_by_norm = fresh["physical_by_normalized"]
    fresh_raw = set(physical_by_norm.values())

    for macro in range(int(plan["search_design"]["macro_count"])):
        mf = list(prefix_feedback) if macro == 0 else []
        mr = list(prefix_records) if macro == 0 else []
        start_ti = 1 if macro == 0 else 0
        for ti in range(start_ti, len(TEMPLATES)):
            t = TEMPLATES[ti]
            arm = _checkpoint_arm_v3(macro, ti)
            asks = base._ask_rows(macro, ti, checkpoint, next_ord, arm)
            inflight = root / f"checkpoint_{checkpoint:04d}.inflight"
            closed = root / f"checkpoint_{checkpoint:04d}"
            inflight.mkdir(parents=False, exist_ok=False)
            large._write_jsonl(inflight / "logical_asks.jsonl", asks)
            large._write_json(inflight / "optimizer_state_before.json", bandit.snapshot())
            large._write_json(inflight / "selection_state_before.json", large._selection_state_record(state))
            schedules, decisions = tournament._select_checkpoint(asks, catalog=fresh["catalog"], bandit=bandit, state=state, components_by_id=authority["components_by_id"], adapter=authority["adapter"], compiler=authority["compiler"], prior_exact_identities=())
            norms = [str(s["optimizer_ask"]["exact_identity"]) for s in schedules]
            raws = [physical_by_norm[n] for n in norms]
            if len(set(norms)) != 24 or len(set(raws)) != 24 or set(norms) & selected_norm or set(raws) & selected_raw or not set(raws) <= fresh_raw:
                raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_FRESH_SELECTION_DRIFT")
            selected_norm.update(norms); selected_raw.update(raws)
            for s, raw in zip(schedules, raws, strict=True):
                s["fresh_physical_exact_identity"] = raw
                s["schedule_record_sha256"] = stable_hash({k: v for k, v in s.items() if k != "schedule_record_sha256"})
            large._write_jsonl(inflight / "selected_schedule.jsonl", schedules)
            large._write_jsonl(inflight / "selection_ledger.jsonl", decisions)
            records = successor._evaluate_schedules(schedules, record_root=inflight / "records", authority=authority, input_hash=input_hash, executor_workers=workers)
            for r in records:
                if any(int(r.get(k) or 0) != 0 for k in ("validation_reads", "holdout_reads", "historical_2023_reads", "forward_b_reads", "forward_2026_reads")):
                    raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_RESTRICTED_READ_DRIFT")
            feedback = tournament._feedback_update(records, schedules, bandit=bandit, behavior_counts=Counter())
            large._write_jsonl(inflight / "feedback.jsonl", feedback)
            large._write_json(inflight / "optimizer_state_after.json", bandit.snapshot())
            large._write_json(inflight / "selection_state_after.json", large._selection_state_record(state))
            previous = large._close_checkpoint(inflight=inflight, closed=closed, previous_manifest_sha256=previous, macro_index=macro, template_id=t, optimizer_arm=arm)
            productive.extend(_productive_rows(feedback, schedules))
            mf.extend(feedback); mr.extend(records); all_feedback.extend(feedback); all_records.extend(records); all_schedules.extend(schedules)
            checkpoint += 1; next_ord += 24
        mm = base._macro_metric(mf, mr, seen_pairs); mm["macro_index"] = macro; macros.append(mm)
        large._write_json(root / f"macro_{macro:02d}_receipt.json", engine._self_hashed({"schema_version": "cn_program_primitive_main_recovery_macro_receipt_v1", "macro_index": macro, "metric": mm, "selected_exact_count_cumulative": len(selected_raw), "recovery_prefix_count": RECOVERED_RECORDS}, "macro_payload_sha256"))
        large._write_json(root / "macro_metrics.json", {"macros": macros})
        if macro >= 3 and len(macros) >= 2 and all(float(x["productive_efficiency"]) <= 0.15 and float(x["new_behavior_pair_rate"]) <= 0.05 for x in macros[-2:]):
            stop_reason = "SUSTAINED_DEVELOPMENT_VALUE_COLLAPSE"
            break

    if len(all_feedback) != len(all_records) or len(all_feedback) != len(all_schedules) or len(selected_norm) != len(all_feedback) or len(selected_raw) != len(all_feedback):
        raise RuntimeError("PRIMITIVE_MAIN_RECOVERY_TERMINAL_CARDINALITY_DRIFT")
    large._write_jsonl(root / "productive_discoveries.jsonl", productive)
    total = large._metric_block(all_feedback)
    per_arm = {arm: large._metric_block([r for r in all_feedback if str(r.get("generation_arm")) == arm]) for arm in ARMS}
    per_template = {t: large._metric_block([r for r in all_feedback if str(r.get("template_id")) == t]) for t in TEMPLATES}
    closure = engine._self_hashed({
        "schema_version": "cn_program_primitive_main_production_recovery_complete_v1",
        "status": STATUS_COMPLETE,
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "repo_sha": repo_sha,
        "authorization_payload_sha256": authorization["authorization_payload_sha256"],
        "production_plan_payload_sha256": plan["plan_payload_sha256"],
        "recovery_prefix_payload_sha256": recovery["recovery_prefix_payload_sha256"],
        "input_binding_sha256": input_hash,
        "logical_records": len(all_feedback),
        "recovered_logical_records": RECOVERED_RECORDS,
        "newly_evaluated_logical_records": len(all_feedback) - RECOVERED_RECORDS,
        "financial_evaluator_reexecution_performed": False,
        "unique_normalized_exact_count": len(selected_norm),
        "unique_fresh_physical_exact_count": len(selected_raw),
        "effective_spent_exact_count_before_production": 6734,
        "effective_spent_overlap_count": 0,
        "closed_checkpoints": checkpoint,
        "closed_macros": len(macros),
        "stop_reason": stop_reason,
        "total_metrics": total,
        "per_arm": per_arm,
        "per_template": per_template,
        "productive_discovery_count": len(productive),
        "behavior_pair_count": len(seen_pairs),
        "optimizer_final_state_sha256": bandit.snapshot()["bandit_state_sha256"],
        "optimizer_metadata": bandit.optimizer_metadata(),
        "wall_seconds_recovery_run": time.perf_counter() - start,
        "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
        "validation_feedback_used": False,
        "oos_authority": "NONE",
        "promotion_authorized": False,
        "automatic_successor_authorized": False,
    }, "closure_payload_sha256")
    large._write_json(root / CLOSURE_NAME, closure)
    return closure

__all__ = ["verify_recovery_prefix", "prefinancial_rehearsal", "run"]
