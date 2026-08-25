"""Post-run audit for Search Core V2 Production Wave 2."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_search_core_v2_stage1_v1 as stage1
from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE2_POSTRUN_AUDIT_COMPLETE_ARCHIVE_READY"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit(args: argparse.Namespace) -> dict[str, Any]:
    repo = args.repo_root.resolve()
    root = args.output_root.resolve()
    terminal = _read(args.terminal.resolve())
    terminal_hash = _verify(terminal, "closure_payload_sha256", "Production Wave 2 terminal")
    final_state = _read(args.final_optimizer_state.resolve())
    final_state_hash = _verify(final_state, "snapshot_hash", "Production Wave 2 final optimizer state")
    prefreeze = _read(repo / "runtime/run_plans/cn_search_core_v2_production_wave2_prefreeze_20260825.json")
    prefreeze_hash = _verify(prefreeze, "prefreeze_payload_sha256", "Production Wave 2 prefreeze")
    supply = _read(repo / "runtime/run_plans/cn_search_core_v2_production_wave2_supply_audit_20260825.json")
    supply_hash = _verify(supply, "audit_payload_sha256", "Production Wave 2 supply audit")
    canary = _read(repo / "runtime/run_plans/cn_search_core_v2_production_wave2_official_resource_canary_20260825.json")
    canary_hash = _verify(canary, "official_canary_payload_sha256", "Production Wave 2 canary")
    authorization = _read(repo / "runtime/run_plans/cn_search_core_v2_production_wave2_authorization_v1.json")
    authorization_hash = _verify(authorization, "authorization_payload_sha256", "Production Wave 2 authorization")
    source_state_path = repo / Path(str(prefreeze["mature_state"]["relative_path"]))
    source_state = _read(source_state_path)
    source_state_hash = _verify(source_state, "snapshot_hash", "Production Wave 2 source mature state")

    if (
        terminal.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE2_COMPLETE"
        or int(terminal.get("evaluated") or 0) != 336
        or int(terminal.get("checkpoint_count") or 0) != 14
        or str(terminal.get("prefreeze_payload_sha256") or "") != prefreeze_hash
        or str(terminal.get("authorization_payload_sha256") or "") != authorization_hash
        or str(terminal.get("initial_mature_snapshot_payload_sha256") or "") != source_state_hash
        or str(terminal.get("final_mature_snapshot_payload_sha256") or "") != final_state_hash
        or int(terminal.get("source_mature_observations") or 0) != 1344
        or int(terminal.get("final_mature_observations") or 0) != 1680
        or terminal.get("validation_feedback_used") is not False
        or terminal.get("promotion_authorized") is not False
        or terminal.get("oos_authority") != "NONE"
        or terminal.get("capital_action_authorized") is not False
        or terminal.get("automatic_followon_authorized") is not False
        or any(int(value) != 0 for value in dict(terminal.get("restricted_reads") or {}).values())
    ):
        raise RuntimeError("Production Wave 2 terminal contract drift")
    pair_contract = dict(prefreeze.get("pair_native_annotation_contract") or {})
    if (
        supply.get("status") != "PASS_ZERO_FINANCIAL_PRODUCTION_WAVE2_CHECKPOINT_SUPPLY_AUDIT"
        or int(supply.get("generated_total") or 0) != 168
        or int(supply.get("unique_exact_count") or 0) != 168
        or int(supply.get("prior_overlap_count", -1)) != 0
        or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("financial_sidecar_read"))
        or canary.get("status") != "PASS"
        or int(canary.get("requested_workers") or 0) != 24
        or int(canary.get("field_column_count") or 0) != 62
        or canary.get("candidate_evaluation_executed") is not False
        or int(dict(canary.get("resource_probe") or {}).get("pagefile_pages_in_delta_bytes", -1)) != 0
        or int(dict(canary.get("resource_probe") or {}).get("pagefile_pages_out_delta_bytes", -1)) != 0
        or authorization.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE2_AUTHORIZED_NOT_RUN"
        or dict(authorization.get("pair_native_annotation_contract") or {}) != pair_contract
        or pair_contract.get("application_scope") != "DEVELOPMENT_RESULT_ANNOTATION_AND_ARCHIVE_ONLY"
        or pair_contract.get("optimizer_feedback_write") is not False
        or pair_contract.get("candidate_admission_changed") is not False
        or pair_contract.get("generator_ask_order_changed") is not False
        or pair_contract.get("automatic_policy_adoption") is not False
        or pair_contract.get("validation_label_read_at_application") is not False
    ):
        raise RuntimeError("Production Wave 2 prefinancial authority drift")
    runner_path = repo / "scripts/run_cn_search_core_v2_production_wave2_v1.py"
    if _sha(runner_path) != str(canary["runner_source_file_sha256"]):
        raise RuntimeError("Production Wave 2 runner changed after canary")

    checkpoint_dirs = [root / f"checkpoint_{ordinal:04d}" for ordinal in range(14)]
    if any(not path.is_dir() for path in checkpoint_dirs):
        raise RuntimeError("Production Wave 2 missing closed checkpoint")
    if list(root.glob("checkpoint_*.inflight")):
        raise RuntimeError("Production Wave 2 has inflight checkpoint after terminal")
    all_rows: list[dict[str, Any]] = []
    checkpoint_metrics: list[dict[str, Any]] = []
    pair_available_count = 0
    pair_rule_hit_count = 0
    pair_productive_hit_count = 0
    pair_stable_hit_count = 0
    previous = "GENESIS"
    for ordinal, checkpoint in enumerate(checkpoint_dirs):
        manifest_path = checkpoint / "checkpoint_manifest.json"
        manifest = _read(manifest_path)
        manifest_hash = _verify(manifest, "manifest_payload_sha256", f"checkpoint {ordinal} manifest")
        observed_checkpoint_ordinal = manifest.get("checkpoint_ordinal")
        if (
            observed_checkpoint_ordinal is None
            or int(observed_checkpoint_ordinal) != ordinal
            or str(manifest.get("previous_checkpoint_manifest_file_sha256") or "") != previous
        ):
            raise RuntimeError("Production Wave 2 checkpoint chain drift")
        for artifact in list(manifest.get("artifacts") or ()):
            artifact_path = checkpoint / Path(str(artifact["path"]))
            if not artifact_path.is_file() or _sha(artifact_path) != str(artifact["sha256"]):
                raise RuntimeError("Production Wave 2 checkpoint artifact drift")
        previous = _sha(manifest_path)
        rows = _read_jsonl(checkpoint / "candidate_results.jsonl")
        if len(rows) != 24:
            raise RuntimeError("Production Wave 2 checkpoint result count drift")
        local_available = 0
        local_hits = 0
        local_productive_hits = 0
        local_stable_hits = 0
        for row in rows:
            _verify(row, "result_payload_sha256", f"checkpoint {ordinal} result")
            if row.get("production_wave_id") != "SEARCH_CORE_V2_PRODUCTION_WAVE2_V1":
                raise RuntimeError("Production Wave 2 result identity drift")
            annotation = dict(row.get("pair_native_robustness") or {})
            if (
                annotation.get("schema_version") != "cn_search_core_v2_pair_native_development_annotation_v1"
                or annotation.get("rule_id") != pair_contract.get("rule_id")
                or annotation.get("annotation_only") is not True
                or annotation.get("optimizer_feedback_used") is not False
            ):
                raise RuntimeError("Production Wave 2 pair-native annotation drift")
            available = bool(annotation.get("available"))
            hit = bool(annotation.get("challenger_hit"))
            if hit and not available:
                raise RuntimeError("Production Wave 2 unavailable pair cannot hit challenger")
            local_available += int(available)
            local_hits += int(hit)
            local_productive_hits += int(hit and bool(row["productive"]))
            local_stable_hits += int(hit and bool(row["stable"]))
        tell = _read(checkpoint / "optimizer_tell_receipt.json")
        if (
            int(tell.get("asked_count") or 0) != 24
            or tell.get("feedback_domain") != "DEVELOPMENT_ONLY"
            or tell.get("sealed_feedback_used") is not False
            or tell.get("optimizer_feedback_applied") is not True
            or any("pair" in str(key).lower() or "challenger" in str(key).lower() for key in tell)
        ):
            raise RuntimeError("Production Wave 2 pair-native/tell separation drift")
        metric = _read(checkpoint / "checkpoint_metric.json")
        if (
            int(metric.get("pair_native_available_count") or 0) != local_available
            or int(metric.get("pair_native_rule_hit_count") or 0) != local_hits
            or int(metric.get("pair_native_productive_rule_hit_count") or 0) != local_productive_hits
            or int(metric.get("pair_native_stable_rule_hit_count") or 0) != local_stable_hits
        ):
            raise RuntimeError("Production Wave 2 checkpoint pair-native metric drift")
        pair_available_count += local_available
        pair_rule_hit_count += local_hits
        pair_productive_hit_count += local_productive_hits
        pair_stable_hit_count += local_stable_hits
        all_rows.extend(rows)
        checkpoint_metrics.append(metric)
    if previous != str(terminal["last_checkpoint_manifest_file_sha256"]):
        raise RuntimeError("Production Wave 2 terminal checkpoint chain drift")
    if len(all_rows) != 336 or len({str(row["exact_identity"]) for row in all_rows}) != 336:
        raise RuntimeError("Production Wave 2 result exact coverage drift")

    recomputed_overall = stage1._metric(all_rows)
    if recomputed_overall != dict(terminal["overall_metrics"]):
        raise RuntimeError("Production Wave 2 overall metric drift")
    for template in stage1.TEMPLATES:
        metric = stage1._metric([row for row in all_rows if str(row["template_id"]) == template])
        if metric != dict(terminal["per_template_metrics"][template]):
            raise RuntimeError("Production Wave 2 per-template metric drift")
    for round_index in range(2):
        metric = stage1._metric([row for row in all_rows if int(row["production_round_index"]) == round_index])
        if metric != dict(terminal["round_metrics"][str(round_index)]):
            raise RuntimeError("Production Wave 2 round metric drift")

    source_history = list(source_state["history"])
    final_history = list(final_state["history"])
    source_generated = set(map(str, source_state["generated_exact_identities"]))
    final_generated = set(map(str, final_state["generated_exact_identities"]))
    source_seen = set(map(str, source_state["seen_exact_identities"]))
    result_exacts = {str(row["exact_identity"]) for row in all_rows}
    new_generated = final_generated - source_generated
    if (
        len(source_history) != 56
        or len(final_history) != 70
        or final_history[:56] != source_history
        or len(source_generated) != 1344
        or len(final_generated) != 1680
        or len(new_generated) != 336
        or new_generated != result_exacts
        or len(new_generated & source_seen) != 0
        or int(final_history[-1]["generator_diagnostics"]["memory_observations"]) != 1680
    ):
        raise RuntimeError("Production Wave 2 mature state lineage drift")

    productive_path = root / "PRODUCTIVE_CANDIDATES.jsonl"
    stable_path = root / "STABLE_CANDIDATES.jsonl"
    pair_native_path = root / "PAIR_NATIVE_CHALLENGER_CANDIDATES.jsonl"
    productive_rows = _read_jsonl(productive_path)
    stable_rows = _read_jsonl(stable_path)
    pair_native_rows = _read_jsonl(pair_native_path)
    expected_productive = [row for row in all_rows if bool(row["productive"])]
    expected_stable = [row for row in all_rows if bool(row["stable"])]
    expected_pair_native = [
        row
        for row in expected_productive
        if bool(dict(row["pair_native_robustness"])["challenger_hit"])
    ]
    terminal_pair = dict(terminal.get("pair_native_annotation") or {})
    if (
        productive_rows != expected_productive
        or stable_rows != expected_stable
        or pair_native_rows != expected_pair_native
        or len(productive_rows) != int(terminal["productive_candidate_count"])
        or len(stable_rows) != int(terminal["stable_candidate_count"])
        or _sha(productive_path) != str(terminal["productive_archive_file_sha256"])
        or _sha(stable_path) != str(terminal["stable_archive_file_sha256"])
        or terminal_pair.get("rule_id") != pair_contract.get("rule_id")
        or terminal_pair.get("source_payload_sha256") != pair_contract.get("source_payload_sha256")
        or terminal_pair.get("application_scope") != "DEVELOPMENT_RESULT_ANNOTATION_AND_ARCHIVE_ONLY"
        or int(terminal_pair.get("available_count") or 0) != pair_available_count
        or int(terminal_pair.get("rule_hit_count") or 0) != pair_rule_hit_count
        or int(terminal_pair.get("productive_rule_hit_count") or 0) != pair_productive_hit_count
        or int(terminal_pair.get("stable_rule_hit_count") or 0) != pair_stable_hit_count
        or _sha(pair_native_path) != str(terminal_pair.get("archive_file_sha256") or "")
        or terminal_pair.get("optimizer_feedback_write") is not False
        or terminal_pair.get("candidate_admission_changed") is not False
        or terminal_pair.get("generator_ask_order_changed") is not False
    ):
        raise RuntimeError("Production Wave 2 candidate/pair-native archive drift")

    job = _read(args.job_status.resolve())
    exit_receipt = _read(args.exit_receipt.resolve())
    resource_state = _read(args.resource_state.resolve())
    stderr_bytes = args.stderr_log.resolve().stat().st_size if args.stderr_log.resolve().is_file() else 0
    if (
        int(job.get("exit_code", -1)) != 0
        or int(exit_receipt.get("exit_code", -1)) != 0
        or str(exit_receipt.get("repo_sha") or "") != str(terminal["repo_sha"])
        or list(resource_state.get("leases") or ())
        or stderr_bytes != 0
    ):
        raise RuntimeError("Production Wave 2 execution terminal state drift")

    payload = {
        "schema_version": "cn_search_core_v2_production_wave2_postrun_audit_v1",
        "status": STATUS,
        "audit_repo_sha": str(args.audit_repo_sha),
        "terminal": {
            "file_sha256": _sha(args.terminal.resolve()),
            "closure_payload_sha256": terminal_hash,
            "repo_sha": terminal["repo_sha"],
            "evaluated": 336,
            "checkpoint_count": 14,
        },
        "prefinancial_authority": {
            "prefreeze_payload_sha256": prefreeze_hash,
            "supply_audit_payload_sha256": supply_hash,
            "canary_payload_sha256": canary_hash,
            "authorization_payload_sha256": authorization_hash,
        },
        "mature_state_lineage": {
            "source_snapshot_payload_sha256": source_state_hash,
            "final_snapshot_payload_sha256": final_state_hash,
            "source_history_count": 56,
            "final_history_count": 70,
            "source_generated_exact_count": 1344,
            "final_generated_exact_count": 1680,
            "new_generated_exact_count": 336,
            "new_generated_vs_source_seen_overlap_count": 0,
            "final_memory_observations": 1680,
        },
        "development_yield": {
            "evaluated": 336,
            "productive": int(recomputed_overall["productive"]),
            "stable": int(recomputed_overall["stable"]),
            "distinct_behavior_pair_count": int(recomputed_overall["distinct_behavior_pair_count"]),
            "productive_rate": float(recomputed_overall["productive_rate"]),
            "stable_rate": float(recomputed_overall["stable_rate"]),
            "per_template": {template: dict(terminal["per_template_metrics"][template]) for template in stage1.TEMPLATES},
            "per_round": dict(terminal["round_metrics"]),
        },
        "candidate_archives": {
            "productive_count": len(productive_rows),
            "stable_count": len(stable_rows),
            "pair_native_challenger_count": len(pair_native_rows),
            "productive_file_sha256": _sha(productive_path),
            "stable_file_sha256": _sha(stable_path),
            "pair_native_challenger_file_sha256": _sha(pair_native_path),
            "pair_native_rule_id": pair_contract["rule_id"],
            "pair_native_available_count": pair_available_count,
            "pair_native_rule_hit_count": pair_rule_hit_count,
            "pair_native_productive_rule_hit_count": pair_productive_hit_count,
            "pair_native_stable_rule_hit_count": pair_stable_hit_count,
            "pair_native_optimizer_feedback_used": False,
            "classification": "DEVELOPMENT_ONLY_NOT_ALPHA_QUALIFIED",
        },
        "execution_terminal": {
            "job_exit_code": int(job["exit_code"]),
            "launcher_exit_code": int(exit_receipt["exit_code"]),
            "active_lease_count": len(list(resource_state.get("leases") or ())),
            "stderr_bytes": stderr_bytes,
            "restricted_reads": dict(terminal["restricted_reads"]),
        },
        "project_control_recommendation": {
            "verdict": "HOLD_DEVELOPMENT_ARCHIVE_READY_NO_AUTOMATIC_OOS",
            "next_action": "CLUSTER_AND_RANK_NEW_DEVELOPMENT_CANDIDATES_WITHOUT_READING_VALIDATION_OR_OOS",
            "automatic_validation_authorized": False,
            "automatic_promotion_authorized": False,
            "capital_action_authorized": False,
        },
        "financial_evaluation_executed_by_audit": False,
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
    payload = audit(args)
    out = args.output if args.output.is_absolute() else args.repo_root / args.output
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "productive": payload["development_yield"]["productive"], "stable": payload["development_yield"]["stable"], "behaviors": payload["development_yield"]["distinct_behavior_pair_count"], "payload": payload["audit_payload_sha256"], "output": str(out.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
