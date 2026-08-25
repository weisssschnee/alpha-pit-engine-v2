"""Post-run audit for Search Core V2 Stage-2 scale confirmation."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from scripts import run_cn_search_core_v2_stage2_v1 as stage2
from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA = "cn_search_core_v2_stage2_postrun_audit_v1"
A = stage2.PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
B = stage2.SEMANTIC_STATE_JUMP_GENERATOR_V2
TEMPLATES = stage2.TEMPLATES


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_self(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def audit(args: argparse.Namespace) -> dict[str, Any]:
    repo = args.repo_root.resolve()
    root = args.output_root.resolve()
    terminal_path = args.terminal.resolve()
    final_state_path = args.final_optimizer_state.resolve()
    terminal = _read(terminal_path)
    closure_hash = _verify_self(
        terminal, "closure_payload_sha256", "Search Core V2 Stage-2 terminal"
    )
    if (
        terminal.get("status") != "SEARCH_CORE_V2_STAGE2_COMPLETE"
        or int(terminal.get("evaluated") or 0) != 1008
        or int(terminal.get("evaluated_per_arm") or 0) != 504
        or int(terminal.get("checkpoint_count") or 0) != 42
        or dict(terminal.get("template_batch_size") or {})
        != {template: 24 for template in TEMPLATES}
        or terminal.get("evaluation_data_role") != "DEVELOPMENT_ONLY"
        or any(int(value) != 0 for value in dict(terminal.get("restricted_reads") or {}).values())
        or terminal.get("validation_feedback_used") is not False
        or terminal.get("promotion_authorized") is not False
        or terminal.get("oos_authority") != "NONE"
        or terminal.get("automatic_policy_change_authorized") is not False
    ):
        raise RuntimeError("Stage-2 terminal contract drift")

    pre_path = repo / "runtime/run_plans/cn_search_core_v2_stage2_prefreeze_20260825.json"
    supply_path = repo / "runtime/run_plans/cn_search_core_v2_stage2_mature_supply_audit_20260825.json"
    canary_path = repo / "runtime/run_plans/cn_search_core_v2_stage2_official_resource_canary_20260825.json"
    auth_path = repo / "runtime/run_plans/cn_search_core_v2_stage2_authorization_v1.json"
    repair_path = repo / "runtime/run_plans/cn_search_core_v2_stage2_exploration_collapse_repair_audit_20260825.json"
    for path in (pre_path, supply_path, canary_path, auth_path, repair_path):
        if not path.is_file():
            raise FileNotFoundError(path)
    pre = _read(pre_path)
    supply = _read(supply_path)
    canary = _read(canary_path)
    auth = _read(auth_path)
    repair = _read(repair_path)
    pre_hash = _verify_self(pre, "prefreeze_payload_sha256", "Stage-2 prefreeze")
    supply_hash = _verify_self(supply, "audit_payload_sha256", "Stage-2 supply audit")
    canary_hash = _verify_self(canary, "official_canary_payload_sha256", "Stage-2 canary")
    auth_hash = _verify_self(auth, "authorization_payload_sha256", "Stage-2 authorization")
    repair_hash = _verify_self(repair, "audit_payload_sha256", "Stage-2 repair audit")
    if (
        pre.get("status") != "SEARCH_CORE_V2_STAGE2_PREFROZEN_BEFORE_FINANCIAL_READ"
        or int(pre["stage2"]["total_financial_evaluations"]) != 1008
        or bool(pre.get("financial_labels_read_by_builder"))
        or pre.get("failed_stage2_financial_records_reused") is not False
        or str(pre["source_stage2_repair_audit"]["payload_sha256"]) != repair_hash
        or supply.get("status")
        != "PASS_ZERO_FINANCIAL_MATURE_STATE_JUMP_STAGE2_CHECKPOINT_SUPPLY_AUDIT"
        or int(supply.get("generated_total") or 0) != 168
        or int(supply.get("unique_exact_count") or 0) != 168
        or int(supply.get("arm_a_overlap_count", -1)) != 0
        or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("financial_sidecar_read"))
        or int(supply.get("base_event_microbatch_robustness_state_count") or 0) != 8
        or int(supply.get("base_event_microbatch_robustness_batch_size") or 0) != 24
        or supply.get("synthetic_tell_used") is not False
        or supply.get("future_checkpoint_supply_fail_closed") is not True
        or canary.get("status") != "PASS"
        or int(canary.get("requested_workers") or 0) != 24
        or canary.get("candidate_evaluation_executed") is not False
        or auth.get("status") != "SEARCH_CORE_V2_STAGE2_AUTHORIZED_NOT_RUN"
        or auth.get("automatic_policy_change_authorized") is not False
        or repair.get("status")
        != "SEARCH_CORE_V2_STAGE2_EXPLORATION_COLLAPSE_REPAIR_PASS_FRESH_RESTART_REQUIRED"
        or repair["failed_run"].get("financial_results_reusable") is not False
        or repair.get("failed_financial_records_reused") is not False
    ):
        raise RuntimeError("Stage-2 prefinancial authority drift")

    runner_path = repo / "scripts/run_cn_search_core_v2_stage2_v1.py"
    if _sha(runner_path) != str(canary["runner_source_file_sha256"]):
        raise RuntimeError("Stage-2 runner changed after official canary")
    if str(terminal.get("authorization_payload_sha256") or "") != auth_hash:
        raise RuntimeError("Stage-2 terminal authorization binding drift")
    if str(terminal.get("prefreeze_payload_sha256") or "") != pre_hash:
        raise RuntimeError("Stage-2 terminal prefreeze binding drift")

    source_state_path = repo / Path(str(pre["arm_b_state_jump"]["source_snapshot_relative_path"]))
    source_state = _read(source_state_path)
    source_snapshot_hash = _verify_self(source_state, "snapshot_hash", "Stage-2 source optimizer")
    final_state = _read(final_state_path)
    final_snapshot_hash = _verify_self(final_state, "snapshot_hash", "Stage-2 final optimizer")
    if (
        source_snapshot_hash != str(pre["arm_b_state_jump"]["source_snapshot_payload_sha256"])
        or str(terminal.get("initial_mature_snapshot_payload_sha256") or "") != source_snapshot_hash
        or str(terminal.get("final_mature_snapshot_payload_sha256") or "") != final_snapshot_hash
    ):
        raise RuntimeError("Stage-2 optimizer snapshot binding drift")
    source_history = list(source_state["history"])
    final_history = list(final_state["history"])
    source_generated = set(map(str, source_state["generated_exact_identities"]))
    final_generated = set(map(str, final_state["generated_exact_identities"]))
    new_generated = final_generated - source_generated
    arm_a_exact = set(map(str, pre["arm_a_primitive"]["selected_exact_identities"]))
    lineage = {
        "source_history_count": len(source_history),
        "final_history_count": len(final_history),
        "history_prefix_exact": final_history[: len(source_history)] == source_history,
        "source_generated_exact_count": len(source_generated),
        "final_generated_exact_count": len(final_generated),
        "new_generated_exact_count": len(new_generated),
        "source_generated_subset_final": source_generated <= final_generated,
        "new_generator_vs_arm_a_overlap_count": len(new_generated & arm_a_exact),
        "final_memory_observations": int(final_history[-1]["generator_diagnostics"]["memory_observations"]),
        "final_elite_count": int(final_history[-1]["generator_diagnostics"]["elite_count"]),
        "final_generated_unique_count": int(final_history[-1]["generator_diagnostics"]["generated_unique_count"]),
    }
    if (
        lineage["source_history_count"] != 21
        or lineage["final_history_count"] != 42
        or lineage["history_prefix_exact"] is not True
        or lineage["source_generated_exact_count"] != 504
        or lineage["final_generated_exact_count"] != 1008
        or lineage["new_generated_exact_count"] != 504
        or lineage["source_generated_subset_final"] is not True
        or lineage["new_generator_vs_arm_a_overlap_count"] != 0
        or lineage["final_memory_observations"] != 1008
    ):
        raise RuntimeError("Stage-2 mature-state lineage drift")

    a = dict(terminal["arm_metrics"][A])
    b = dict(terminal["arm_metrics"][B])
    per_template = {
        arm: {
            template: dict(terminal["per_template_metrics"][arm][template])
            for template in TEMPLATES
        }
        for arm in (A, B)
    }
    recomputed = stage2._decision(a, b, per_template)
    terminal_decision = dict(terminal["decision"])
    if recomputed != terminal_decision:
        raise RuntimeError("Stage-2 terminal decision replay drift")

    round_metrics = {
        arm: {str(index): dict(terminal["round_metrics"][arm][str(index)]) for index in range(3)}
        for arm in (A, B)
    }
    if any(
        int(round_metrics[arm][str(index)]["evaluated"]) != 168
        for arm in (A, B)
        for index in range(3)
    ):
        raise RuntimeError("Stage-2 round coverage drift")

    checkpoint_dirs = sorted(
        path for path in root.iterdir()
        if path.is_dir() and path.name.startswith("checkpoint_") and not path.name.endswith(".inflight")
    )
    inflight_dirs = sorted(
        path.name for path in root.iterdir()
        if path.is_dir() and path.name.endswith(".inflight")
    )
    if len(checkpoint_dirs) != 42 or inflight_dirs:
        raise RuntimeError("Stage-2 checkpoint closure drift")
    for ordinal, checkpoint in enumerate(checkpoint_dirs):
        metric = _read(checkpoint / "checkpoint_metric.json")
        if int(metric["checkpoint_ordinal"]) != ordinal or int(metric["evaluated"]) != 24:
            raise RuntimeError("Stage-2 checkpoint metric cardinality drift")

    job_status = _read(args.job_status.resolve())
    exit_receipt = _read(args.exit_receipt.resolve())
    resource_state = _read(args.resource_state.resolve())
    stderr_path = args.stderr_log.resolve()
    active_matching = [
        row for row in list(resource_state.get("leases") or ())
        if str(row.get("workload_id") or "") == str(root)
    ]
    stderr_bytes = stderr_path.stat().st_size if stderr_path.exists() else 0
    execution = {
        "job_exit_code": int(job_status.get("exit_code", -1)),
        "launcher_exit_code": int(exit_receipt.get("exit_code", -1)),
        "launcher_repo_sha": str(exit_receipt.get("repo_sha") or ""),
        "launcher_output_root": str(exit_receipt.get("output_root") or ""),
        "closed_checkpoint_count": len(checkpoint_dirs),
        "inflight_checkpoint_dirs": inflight_dirs,
        "active_matching_lease_count": len(active_matching),
        "stderr_bytes": int(stderr_bytes),
    }
    if (
        execution["job_exit_code"] != 0
        or execution["launcher_exit_code"] != 0
        or execution["launcher_repo_sha"] != str(terminal["repo_sha"])
        or Path(execution["launcher_output_root"]).resolve() != root
        or execution["closed_checkpoint_count"] != 42
        or execution["inflight_checkpoint_dirs"]
        or execution["active_matching_lease_count"] != 0
        or execution["stderr_bytes"] != 0
    ):
        raise RuntimeError("Stage-2 execution closure drift")

    if terminal_decision["status"] == "MATURE_GENERATOR_STAGE2_SCALE_CONFIRMATION_PASS_SEARCH_CORE_POLICY_REVIEW_ELIGIBLE":
        status = "SEARCH_CORE_V2_STAGE2_POSTRUN_AUDIT_COMPLETE_POLICY_REVIEW_ELIGIBLE"
        recommendation = "SEARCH_CORE_POLICY_REVIEW_ELIGIBLE_NOT_AUTOMATICALLY_AUTHORIZED"
    elif terminal_decision["status"] == "MATURE_GENERATOR_STAGE2_SCALE_CLEAR_LOSS_STOP":
        status = "SEARCH_CORE_V2_STAGE2_POSTRUN_AUDIT_COMPLETE_CLEAR_LOSS_STOP"
        recommendation = "STOP_GENERATOR_SCALE_CONFIRMATION"
    else:
        status = "SEARCH_CORE_V2_STAGE2_POSTRUN_AUDIT_COMPLETE_AMBIGUOUS_NO_POLICY_CHANGE"
        recommendation = "NO_SEARCH_CORE_POLICY_CHANGE"

    payload = {
        "schema_version": SCHEMA,
        "status": status,
        "audit_repo_sha": str(args.audit_repo_sha),
        "terminal": {
            "path": str(terminal_path),
            "file_sha256": _sha(terminal_path),
            "closure_payload_sha256": closure_hash,
            "repo_sha": str(terminal["repo_sha"]),
            "evaluated": int(terminal["evaluated"]),
            "restricted_reads": dict(terminal["restricted_reads"]),
        },
        "prefinancial_authority": {
            "prefreeze_payload_sha256": pre_hash,
            "supply_audit_payload_sha256": supply_hash,
            "resource_canary_payload_sha256": canary_hash,
            "authorization_payload_sha256": auth_hash,
            "repair_audit_payload_sha256": repair_hash,
            "failed_prior_stage2_financial_records_reused": False,
        },
        "mature_state_lineage": {
            "source_snapshot_file_sha256": _sha(source_state_path),
            "source_snapshot_payload_sha256": source_snapshot_hash,
            "final_snapshot_file_sha256": _sha(final_state_path),
            "final_snapshot_payload_sha256": final_snapshot_hash,
            **lineage,
        },
        "execution_closure": execution,
        "metrics": {
            "arm_a": a,
            "arm_b": b,
            "productive_delta_b_vs_a": int(b["productive"]) - int(a["productive"]),
            "stable_delta_b_vs_a": int(b["stable"]) - int(a["stable"]),
            "behavior_delta_b_vs_a": int(b["distinct_behavior_pair_count"]) - int(a["distinct_behavior_pair_count"]),
            "per_template": per_template,
            "round_metrics": round_metrics,
        },
        "decision_replay": terminal_decision,
        "project_control_recommendation": {
            "verdict": recommendation,
            "automatic_policy_change_authorized": False,
            "promotion_authorized": False,
            "oos_authority": "NONE",
            "cumulative_development_budget_per_arm": 1008,
        },
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--audit-repo-sha", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--terminal", type=Path, required=True)
    parser.add_argument("--final-optimizer-state", type=Path, required=True)
    parser.add_argument("--job-status", type=Path, required=True)
    parser.add_argument("--exit-receipt", type=Path, required=True)
    parser.add_argument("--resource-state", type=Path, required=True)
    parser.add_argument("--stderr-log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    audit_sha = str(args.audit_repo_sha).lower()
    if len(audit_sha) != 40 or any(ch not in "0123456789abcdef" for ch in audit_sha):
        parser.error("--audit-repo-sha must be a 40-character hex Git SHA")
    args.audit_repo_sha = audit_sha
    payload = audit(args)
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
                "audit_payload_sha256": payload["audit_payload_sha256"],
                "productive_delta_b_vs_a": payload["metrics"]["productive_delta_b_vs_a"],
                "stable_delta_b_vs_a": payload["metrics"]["stable_delta_b_vs_a"],
                "behavior_delta_b_vs_a": payload["metrics"]["behavior_delta_b_vs_a"],
                "decision": payload["decision_replay"]["status"],
                "output": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())