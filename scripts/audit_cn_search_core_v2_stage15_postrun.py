"""Post-run audit for Search Core V2 Stage-1.5 mature-state confirmation."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from scripts import run_cn_search_core_v2_stage15_v1 as stage15
from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA = "cn_search_core_v2_stage15_postrun_audit_v1"
STATUS = "SEARCH_CORE_V2_STAGE15_POSTRUN_AUDIT_COMPLETE_STAGE2_REVIEW_ELIGIBLE"
A = stage15.PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
B = stage15.SEMANTIC_STATE_JUMP_GENERATOR_V2
TEMPLATES = stage15.TEMPLATES


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
    job_status_path = args.job_status.resolve()
    exit_receipt_path = args.exit_receipt.resolve()
    resource_state_path = args.resource_state.resolve()
    stderr_path = args.stderr_log.resolve()

    terminal = _read(terminal_path)
    closure_hash = _verify_self(
        terminal, "closure_payload_sha256", "Search Core V2 Stage-1.5 terminal"
    )
    if (
        terminal.get("status") != "SEARCH_CORE_V2_STAGE15_COMPLETE"
        or int(terminal.get("evaluated") or 0) != 336
        or int(terminal.get("evaluated_per_arm") or 0) != 168
        or int(terminal.get("checkpoint_count") or 0) != 14
        or any(int(v) != 0 for v in dict(terminal.get("restricted_reads") or {}).values())
        or terminal.get("validation_feedback_used") is not False
        or terminal.get("promotion_authorized") is not False
        or terminal.get("oos_authority") != "NONE"
        or terminal.get("automatic_stage2_authorized") is not False
    ):
        raise RuntimeError("Stage-1.5 terminal contract drift")

    prefreeze_path = repo / "runtime/run_plans/cn_search_core_v2_stage15_prefreeze_20260824.json"
    supply_path = repo / "runtime/run_plans/cn_search_core_v2_stage15_mature_supply_audit_20260824.json"
    canary_path = repo / "runtime/run_plans/cn_search_core_v2_stage15_official_resource_canary_20260824.json"
    auth_path = repo / "runtime/run_plans/cn_search_core_v2_stage15_authorization_v1.json"
    prefreeze = _read(prefreeze_path)
    supply = _read(supply_path)
    canary = _read(canary_path)
    authorization = _read(auth_path)
    prefreeze_hash = _verify_self(prefreeze, "prefreeze_payload_sha256", "Stage-1.5 prefreeze")
    supply_hash = _verify_self(supply, "audit_payload_sha256", "Stage-1.5 mature supply audit")
    canary_hash = _verify_self(canary, "official_canary_payload_sha256", "Stage-1.5 resource canary")
    authorization_hash = _verify_self(authorization, "authorization_payload_sha256", "Stage-1.5 authorization")
    if (
        prefreeze.get("status") != "SEARCH_CORE_V2_STAGE15_PREFROZEN_BEFORE_FINANCIAL_READ"
        or int(prefreeze["stage15"]["total_financial_evaluations"]) != 336
        or bool(prefreeze.get("financial_labels_read_by_builder"))
        or supply.get("status") != "PASS_ZERO_FINANCIAL_MATURE_STATE_JUMP_SUPPLY_AUDIT"
        or int(supply.get("generated_total") or 0) != 168
        or int(supply.get("unique_exact_count") or 0) != 168
        or int(supply.get("arm_a_overlap_count") or -1) != 0
        or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("financial_sidecar_read"))
        or canary.get("status") != "PASS"
        or int(canary.get("requested_workers") or 0) != 24
        or canary.get("candidate_evaluation_executed") is not False
        or authorization.get("status") != "SEARCH_CORE_V2_STAGE15_AUTHORIZED_NOT_RUN"
        or authorization.get("automatic_stage2_authorized") is not False
    ):
        raise RuntimeError("Stage-1.5 prefinancial authority drift")

    runner_path = repo / "scripts/run_cn_search_core_v2_stage15_v1.py"
    if _sha(runner_path) != str(canary["runner_source_file_sha256"]):
        raise RuntimeError("Stage-1.5 runner changed after official canary")

    final_state = _read(final_state_path)
    final_snapshot_hash = _verify_self(final_state, "snapshot_hash", "Stage-1.5 final optimizer state")
    source_state_path = repo / Path(str(prefreeze["arm_b_state_jump"]["source_snapshot_relative_path"]))
    source_state = _read(source_state_path)
    source_snapshot_hash = _verify_self(source_state, "snapshot_hash", "Stage-1 source optimizer state")
    if source_snapshot_hash != str(prefreeze["arm_b_state_jump"]["source_snapshot_payload_sha256"]):
        raise RuntimeError("Stage-1.5 source snapshot binding drift")
    source_history = list(source_state["history"])
    final_history = list(final_state["history"])
    source_generated = set(map(str, source_state["generated_exact_identities"]))
    final_generated = set(map(str, final_state["generated_exact_identities"]))
    new_generated = final_generated - source_generated
    arm_a_exact = set(map(str, prefreeze["arm_a_primitive"]["selected_exact_identities"]))
    continuation_checks = {
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
    if continuation_checks != {
        "source_history_count": 14,
        "final_history_count": 21,
        "history_prefix_exact": True,
        "source_generated_exact_count": 336,
        "final_generated_exact_count": 504,
        "new_generated_exact_count": 168,
        "source_generated_subset_final": True,
        "new_generator_vs_arm_a_overlap_count": 0,
        "final_memory_observations": 504,
        "final_elite_count": continuation_checks["final_elite_count"],
        "final_generated_unique_count": continuation_checks["final_generated_unique_count"],
    }:
        raise RuntimeError("Stage-1.5 mature continuation lineage drift")

    a = dict(terminal["arm_metrics"][A])
    b = dict(terminal["arm_metrics"][B])
    per_template = {
        arm: {template: dict(terminal["per_template_metrics"][arm][template]) for template in TEMPLATES}
        for arm in (A, B)
    }
    recomputed = stage15._decision(a, b, per_template)
    terminal_decision = dict(terminal["decision"])
    if recomputed != terminal_decision:
        raise RuntimeError("Stage-1.5 terminal decision replay drift")
    if terminal_decision.get("status") != "MATURE_GENERATOR_STAGE15_CONFIRMATION_PASS_STAGE2_REVIEW_ELIGIBLE":
        raise RuntimeError("Stage-1.5 did not pass mature confirmation")

    job_status = _read(job_status_path)
    exit_receipt = _read(exit_receipt_path)
    resource_state = _read(resource_state_path)
    workload = str(root)
    active_matching_leases = [
        row for row in list(resource_state.get("leases") or ())
        if str(row.get("workload_id") or "") == workload
    ]
    stderr_bytes = stderr_path.stat().st_size if stderr_path.exists() else 0
    checkpoint_dirs = sorted(
        p for p in root.glob("checkpoint_[0-9][0-9][0-9][0-9]") if p.is_dir()
    )
    inflight_dirs = sorted(p.name for p in root.glob("checkpoint_*.inflight") if p.is_dir())
    execution_checks = {
        "job_exit_code": int(job_status.get("exit_code", -1)),
        "launcher_exit_code": int(exit_receipt.get("exit_code", -1)),
        "launcher_repo_sha": str(exit_receipt.get("repo_sha") or ""),
        "launcher_output_root": str(exit_receipt.get("output_root") or ""),
        "closed_checkpoint_count": len(checkpoint_dirs),
        "inflight_checkpoint_dirs": inflight_dirs,
        "active_matching_lease_count": len(active_matching_leases),
        "stderr_bytes": int(stderr_bytes),
    }
    if (
        execution_checks["job_exit_code"] != 0
        or execution_checks["launcher_exit_code"] != 0
        or execution_checks["launcher_repo_sha"] != str(terminal["repo_sha"])
        or Path(execution_checks["launcher_output_root"]).resolve() != root
        or execution_checks["closed_checkpoint_count"] != 14
        or execution_checks["inflight_checkpoint_dirs"]
        or execution_checks["active_matching_lease_count"] != 0
        or execution_checks["stderr_bytes"] != 0
    ):
        raise RuntimeError("Stage-1.5 execution closure drift")

    template_rows: dict[str, Any] = {}
    for template in TEMPLATES:
        ma = per_template[A][template]
        mb = per_template[B][template]
        template_rows[template] = {
            "arm_a_productive": int(ma["productive"]),
            "arm_b_productive": int(mb["productive"]),
            "productive_delta_b_minus_a": int(mb["productive"]) - int(ma["productive"]),
            "arm_a_stable": int(ma["stable"]),
            "arm_b_stable": int(mb["stable"]),
            "stable_delta_b_minus_a": int(mb["stable"]) - int(ma["stable"]),
            "arm_a_behavior": int(ma["distinct_behavior_pair_count"]),
            "arm_b_behavior": int(mb["distinct_behavior_pair_count"]),
            "behavior_delta_b_minus_a": int(mb["distinct_behavior_pair_count"]) - int(ma["distinct_behavior_pair_count"]),
        }

    completed_per_arm = 336 + 168
    target_total_per_arm = 1008
    remaining_per_arm = target_total_per_arm - completed_per_arm
    if remaining_per_arm != 504 or remaining_per_arm % len(TEMPLATES) != 0:
        raise RuntimeError("Stage-2 remaining budget geometry drift")

    payload = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "audit_repo_sha": str(args.audit_repo_sha),
        "stage15_terminal": {
            "path": str(terminal_path),
            "file_sha256": _sha(terminal_path),
            "closure_payload_sha256": closure_hash,
            "repo_sha": str(terminal["repo_sha"]),
            "evaluated": int(terminal["evaluated"]),
            "restricted_reads": dict(terminal["restricted_reads"]),
        },
        "prefinancial_authority": {
            "prefreeze_payload_sha256": prefreeze_hash,
            "mature_supply_audit_payload_sha256": supply_hash,
            "resource_canary_payload_sha256": canary_hash,
            "authorization_payload_sha256": authorization_hash,
            "candidate_evaluation_during_supply_or_canary": False,
        },
        "mature_state_lineage": {
            "source_snapshot_file_sha256": _sha(source_state_path),
            "source_snapshot_payload_sha256": source_snapshot_hash,
            "final_snapshot_file_sha256": _sha(final_state_path),
            "final_snapshot_payload_sha256": final_snapshot_hash,
            **continuation_checks,
        },
        "execution_closure": execution_checks,
        "metrics": {
            "arm_a": a,
            "arm_b": b,
            "productive_delta_b_vs_a": int(b["productive"]) - int(a["productive"]),
            "stable_delta_b_vs_a": int(b["stable"]) - int(a["stable"]),
            "behavior_delta_b_vs_a": int(b["distinct_behavior_pair_count"]) - int(a["distinct_behavior_pair_count"]),
            "per_template": template_rows,
        },
        "decision_replay": terminal_decision,
        "project_control_recommendation": {
            "verdict": "STAGE2_PREFREEZE_REVIEW_ELIGIBLE_NOT_AUTOMATICALLY_AUTHORIZED",
            "reason": "MATURE_GENERATOR_ADVANTAGE_CONFIRMED_ON_DISJOINT_FRESH_CONTINUATION",
            "cumulative_completed_budget_per_arm": completed_per_arm,
            "frozen_target_total_budget_per_arm": target_total_per_arm,
            "remaining_stage2_budget_per_arm": remaining_per_arm,
            "remaining_stage2_budget_total": remaining_per_arm * 2,
            "remaining_stage2_budget_per_template_per_arm": remaining_per_arm // len(TEMPLATES),
            "primitive_next_slice": "FROZEN_STAGE1_ORDER_INDEX_72_TO_143_PER_TEMPLATE",
            "generator_source": "RESTORE_EXACT_STAGE15_FINAL_OPTIMIZER_STATE",
            "automatic_stage2_authorized": False,
            "oos_authority": "NONE",
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
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": payload["status"],
        "audit_payload_sha256": payload["audit_payload_sha256"],
        "output": str(output.resolve()),
        "productive_delta_b_vs_a": payload["metrics"]["productive_delta_b_vs_a"],
        "remaining_stage2_budget_per_arm": payload["project_control_recommendation"]["remaining_stage2_budget_per_arm"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())