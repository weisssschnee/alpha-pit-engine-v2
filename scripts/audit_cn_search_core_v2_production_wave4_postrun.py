"""Post-run audit for Search Core V2 Production Wave 4."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_search_core_v2_stage1_v1 as stage1
from scripts import run_cn_search_core_v2_production_wave4_v1 as wave4
from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE4_POSTRUN_AUDIT_COMPLETE_DEVELOPMENT_ARCHIVE_READY"


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
    terminal_hash = _verify(terminal, "closure_payload_sha256", "Production Wave 4 terminal")
    final_state = _read(args.final_optimizer_state.resolve())
    final_state_hash = _verify(final_state, "snapshot_hash", "Production Wave 4 final optimizer state")
    prefreeze = _read(repo / "runtime/run_plans/cn_search_core_v2_production_wave4_prefreeze_20260826.json")
    prefreeze_hash = _verify(prefreeze, "prefreeze_payload_sha256", "Production Wave 4 prefreeze")
    supply = _read(repo / "runtime/run_plans/cn_search_core_v2_production_wave4_supply_audit_20260826.json")
    supply_hash = _verify(supply, "audit_payload_sha256", "Production Wave 4 supply audit")
    canary = _read(repo / "runtime/run_plans/cn_search_core_v2_production_wave4_official_resource_canary_20260826.json")
    canary_hash = _verify(canary, "official_canary_payload_sha256", "Production Wave 4 canary")
    authorization = _read(repo / "runtime/run_plans/cn_search_core_v2_production_wave4_authorization_v1.json")
    authorization_hash = _verify(authorization, "authorization_payload_sha256", "Production Wave 4 authorization")
    source_state_path = repo / Path(str(prefreeze["mature_state"]["relative_path"]))
    source_state = _read(source_state_path)
    source_state_hash = _verify(source_state, "snapshot_hash", "Production Wave 4 source mature state")
    pair_contract = dict(prefreeze.get("pair_native_annotation_contract") or {})
    pair_rule_id = str(pair_contract.get("rule_id") or "")
    rule_contract = dict(pair_contract.get("rule_contract") or {})
    required_primary = int(rule_contract.get("primary_positive_development_window_count_required") or 0)
    minimum_control = int(rule_contract.get("control_positive_development_window_count_minimum") or 0)

    if (
        terminal.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE4_COMPLETE"
        or int(terminal.get("evaluated") or 0) != 192
        or int(terminal.get("checkpoint_count") or 0) != 8
        or str(terminal.get("prefreeze_payload_sha256") or "") != prefreeze_hash
        or str(terminal.get("authorization_payload_sha256") or "") != authorization_hash
        or str(terminal.get("initial_mature_snapshot_payload_sha256") or "") != source_state_hash
        or str(terminal.get("final_mature_snapshot_payload_sha256") or "") != final_state_hash
        or int(terminal.get("source_mature_observations") or 0) != 1920
        or int(terminal.get("final_mature_observations") or 0) != 2112
        or terminal.get("validation_feedback_used") is not False
        or terminal.get("promotion_authorized") is not False
        or terminal.get("oos_authority") != "NONE"
        or terminal.get("capital_action_authorized") is not False
        or terminal.get("automatic_followon_authorized") is not False
        or any(int(value) != 0 for value in dict(terminal.get("restricted_reads") or {}).values())
    ):
        raise RuntimeError("Production Wave 4 terminal contract drift")
    pair_terminal = dict(terminal.get("pair_native_annotation") or {})
    if (
        pair_terminal.get("rule_id") != pair_rule_id
        or pair_terminal.get("source_payload_sha256") != pair_contract.get("source_payload_sha256")
        or pair_terminal.get("application_scope") != "DEVELOPMENT_RESULT_ANNOTATION_AND_ARCHIVE_ONLY"
        or pair_terminal.get("optimizer_feedback_write") is not False
        or pair_terminal.get("candidate_admission_changed") is not False
        or pair_terminal.get("generator_ask_order_changed") is not False
    ):
        raise RuntimeError("Production Wave 4 pair-native terminal contract drift")
    if (
        supply.get("status") != "PASS_ZERO_FINANCIAL_PRODUCTION_WAVE4_CHECKPOINT_SUPPLY_AUDIT"
        or int(supply.get("generated_total") or 0) != 96
        or int(supply.get("prior_overlap_count", -1)) != 0
        or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("financial_sidecar_read"))
        or canary.get("status") != "PASS"
        or canary.get("candidate_evaluation_executed") is not False
        or authorization.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE4_AUTHORIZED_NOT_RUN"
    ):
        raise RuntimeError("Production Wave 4 prefinancial authority drift")

    checkpoint_dirs = [root / f"checkpoint_{ordinal:04d}" for ordinal in range(8)]
    if any(not path.is_dir() for path in checkpoint_dirs):
        raise RuntimeError("Production Wave 4 missing closed checkpoint")
    if list(root.glob("checkpoint_*.inflight")):
        raise RuntimeError("Production Wave 4 has inflight checkpoint after terminal")
    all_rows: list[dict[str, Any]] = []
    checkpoint_metrics: list[dict[str, Any]] = []
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
            raise RuntimeError("Production Wave 4 checkpoint chain drift")
        for artifact in list(manifest.get("artifacts") or ()):
            artifact_path = checkpoint / Path(str(artifact["path"]))
            if not artifact_path.is_file() or _sha(artifact_path) != str(artifact["sha256"]):
                raise RuntimeError("Production Wave 4 checkpoint artifact drift")
        previous = _sha(manifest_path)
        rows = _read_jsonl(checkpoint / "candidate_results.jsonl")
        if len(rows) != 24:
            raise RuntimeError("Production Wave 4 checkpoint result count drift")
        for row in rows:
            _verify(row, "result_payload_sha256", f"checkpoint {ordinal} result")
            if row.get("production_wave_id") != "SEARCH_CORE_V2_PRODUCTION_WAVE4_V1":
                raise RuntimeError("Production Wave 4 result identity drift")
            annotation = dict(row.get("pair_native_robustness") or {})
            if (
                annotation.get("schema_version") != "cn_search_core_v2_pair_native_development_annotation_v1"
                or annotation.get("rule_id") != pair_rule_id
                or annotation.get("annotation_only") is not True
                or annotation.get("optimizer_feedback_used") is not False
            ):
                raise RuntimeError("Production Wave 4 pair-native annotation contract drift")
            if bool(annotation.get("available")):
                primary_count = int(annotation.get("primary_positive_window_count") or 0)
                control_count = int(annotation.get("control_positive_window_count") or 0)
                expected_hit = primary_count >= required_primary and control_count >= minimum_control
                if (
                    list(annotation.get("window_ids") or ()) != ["development_1", "development_2", "development_3"]
                    or len(list(annotation.get("primary_window_returns") or ())) != 3
                    or len(list(annotation.get("control_window_returns") or ())) != 3
                    or bool(annotation.get("challenger_hit")) != expected_hit
                ):
                    raise RuntimeError("Production Wave 4 pair-native annotation recomputation drift")
            elif bool(annotation.get("challenger_hit")):
                raise RuntimeError("Production Wave 4 unavailable pair marked challenger")
        tell = _read(checkpoint / "optimizer_tell_receipt.json")
        tell_text = json.dumps(tell, ensure_ascii=False, sort_keys=True).lower()
        if "pair_native" in tell_text or "challenger" in tell_text:
            raise RuntimeError("Production Wave 4 pair-native feedback leaked into optimizer tell")
        all_rows.extend(rows)
        checkpoint_metrics.append(_read(checkpoint / "checkpoint_metric.json"))
    if previous != str(terminal["last_checkpoint_manifest_file_sha256"]):
        raise RuntimeError("Production Wave 4 terminal checkpoint chain drift")
    if len(all_rows) != 192 or len({str(row["exact_identity"]) for row in all_rows}) != 192:
        raise RuntimeError("Production Wave 4 result exact coverage drift")
    active_templates = tuple(map(str, prefreeze["production_wave4"]["templates"]))
    held_templates = tuple(map(str, prefreeze["production_wave4"]["held_templates"]))
    observed_templates = {str(row["template_id"]) for row in all_rows}
    if tuple(active_templates) != tuple(wave4.TEMPLATES) or observed_templates != set(active_templates) or observed_templates & set(held_templates):
        raise RuntimeError("Production Wave 4 focused template budget drift")

    recomputed_overall = stage1._metric(all_rows)
    if recomputed_overall != dict(terminal["overall_metrics"]):
        raise RuntimeError("Production Wave 4 overall metric drift")
    for template in wave4.TEMPLATES:
        metric = stage1._metric([row for row in all_rows if str(row["template_id"]) == template])
        if metric != dict(terminal["per_template_metrics"][template]):
            raise RuntimeError("Production Wave 4 per-template metric drift")
    for round_index in range(2):
        metric = stage1._metric([row for row in all_rows if int(row["production_round_index"]) == round_index])
        if metric != dict(terminal["round_metrics"][str(round_index)]):
            raise RuntimeError("Production Wave 4 round metric drift")

    source_history = list(source_state["history"])
    final_history = list(final_state["history"])
    source_generated = set(map(str, source_state["generated_exact_identities"]))
    final_generated = set(map(str, final_state["generated_exact_identities"]))
    source_seen = set(map(str, source_state["seen_exact_identities"]))
    result_exacts = {str(row["exact_identity"]) for row in all_rows}
    new_generated = final_generated - source_generated
    if (
        len(source_history) != 80
        or len(final_history) != 88
        or final_history[:80] != source_history
        or len(source_generated) != 1920
        or len(final_generated) != 2112
        or len(new_generated) != 192
        or new_generated != result_exacts
        or len(new_generated & source_seen) != 0
        or int(final_history[-1]["generator_diagnostics"]["memory_observations"]) != 2112
    ):
        raise RuntimeError("Production Wave 4 mature state lineage drift")

    productive_path = root / "PRODUCTIVE_CANDIDATES.jsonl"
    stable_path = root / "STABLE_CANDIDATES.jsonl"
    productive_rows = _read_jsonl(productive_path)
    stable_rows = _read_jsonl(stable_path)
    expected_productive = [row for row in all_rows if bool(row["productive"])]
    expected_stable = [row for row in all_rows if bool(row["stable"])]
    if (
        productive_rows != expected_productive
        or stable_rows != expected_stable
        or len(productive_rows) != int(terminal["productive_candidate_count"])
        or len(stable_rows) != int(terminal["stable_candidate_count"])
        or _sha(productive_path) != str(terminal["productive_archive_file_sha256"])
        or _sha(stable_path) != str(terminal["stable_archive_file_sha256"])
    ):
        raise RuntimeError("Production Wave 4 candidate archive drift")

    pair_available = [row for row in all_rows if bool(row["pair_native_robustness"]["available"])]
    pair_hits = [row for row in all_rows if bool(row["pair_native_robustness"]["challenger_hit"])]
    pair_productive = [row for row in expected_productive if bool(row["pair_native_robustness"]["challenger_hit"])]
    pair_stable = [row for row in expected_stable if bool(row["pair_native_robustness"]["challenger_hit"])]
    pair_archive_path = root / "PAIR_NATIVE_CHALLENGER_CANDIDATES.jsonl"
    pair_archive_rows = _read_jsonl(pair_archive_path)
    if (
        pair_archive_rows != pair_productive
        or len(pair_available) != int(pair_terminal.get("available_count") or 0)
        or len(pair_hits) != int(pair_terminal.get("rule_hit_count") or 0)
        or len(pair_productive) != int(pair_terminal.get("productive_rule_hit_count") or 0)
        or len(pair_stable) != int(pair_terminal.get("stable_rule_hit_count") or 0)
        or _sha(pair_archive_path) != str(pair_terminal.get("archive_file_sha256") or "")
    ):
        raise RuntimeError("Production Wave 4 pair-native challenger archive drift")
    for ordinal, metric in enumerate(checkpoint_metrics):
        cp_rows = [row for row in all_rows if row.get("checkpoint_ordinal") is not None and int(row["checkpoint_ordinal"]) == ordinal]
        cp_available = sum(bool(row["pair_native_robustness"]["available"]) for row in cp_rows)
        cp_hits = sum(bool(row["pair_native_robustness"]["challenger_hit"]) for row in cp_rows)
        cp_productive = sum(
            bool(row["productive"]) and bool(row["pair_native_robustness"]["challenger_hit"])
            for row in cp_rows
        )
        cp_stable = sum(
            bool(row["stable"]) and bool(row["pair_native_robustness"]["challenger_hit"])
            for row in cp_rows
        )
        if (
            int(metric.get("pair_native_available_count") or 0) != cp_available
            or int(metric.get("pair_native_rule_hit_count") or 0) != cp_hits
            or int(metric.get("pair_native_productive_rule_hit_count") or 0) != cp_productive
            or int(metric.get("pair_native_stable_rule_hit_count") or 0) != cp_stable
        ):
            raise RuntimeError("Production Wave 4 checkpoint pair-native metric drift")

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
        raise RuntimeError("Production Wave 4 execution terminal state drift")

    payload = {
        "schema_version": "cn_search_core_v2_production_wave4_postrun_audit_v1",
        "status": STATUS,
        "audit_repo_sha": str(args.audit_repo_sha),
        "terminal": {
            "file_sha256": _sha(args.terminal.resolve()),
            "closure_payload_sha256": terminal_hash,
            "repo_sha": terminal["repo_sha"],
            "evaluated": 192,
            "checkpoint_count": 8,
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
            "source_history_count": 80,
            "final_history_count": 88,
            "source_generated_exact_count": 1920,
            "final_generated_exact_count": 2112,
            "new_generated_exact_count": 192,
            "new_generated_vs_source_seen_overlap_count": 0,
            "final_memory_observations": 2112,
        },
        "development_yield": {
            "evaluated": 192,
            "productive": int(recomputed_overall["productive"]),
            "stable": int(recomputed_overall["stable"]),
            "distinct_behavior_pair_count": int(recomputed_overall["distinct_behavior_pair_count"]),
            "productive_rate": float(recomputed_overall["productive_rate"]),
            "stable_rate": float(recomputed_overall["stable_rate"]),
            "per_template": {template: dict(terminal["per_template_metrics"][template]) for template in wave4.TEMPLATES},
            "per_round": dict(terminal["round_metrics"]),
        },
        "candidate_archives": {
            "productive_count": len(productive_rows),
            "stable_count": len(stable_rows),
            "productive_file_sha256": _sha(productive_path),
            "stable_file_sha256": _sha(stable_path),
            "classification": "DEVELOPMENT_ONLY_NOT_ALPHA_QUALIFIED",
        },
        "pair_native_annotation": {
            "rule_id": pair_rule_id,
            "source_payload_sha256": pair_contract["source_payload_sha256"],
            "application_scope": pair_contract["application_scope"],
            "available_count": len(pair_available),
            "rule_hit_count": len(pair_hits),
            "productive_rule_hit_count": len(pair_productive),
            "stable_rule_hit_count": len(pair_stable),
            "challenger_archive_file_sha256": _sha(pair_archive_path),
            "optimizer_feedback_write": False,
            "candidate_admission_changed": False,
            "generator_ask_order_changed": False,
            "all_checkpoint_tell_receipts_pair_native_free": True,
            "classification": "DEVELOPMENT_ONLY_CHALLENGER_NOT_ALPHA_QUALIFIED",
        },
        "execution_terminal": {
            "job_exit_code": int(job["exit_code"]),
            "launcher_exit_code": int(exit_receipt["exit_code"]),
            "active_lease_count": len(list(resource_state.get("leases") or ())),
            "stderr_bytes": stderr_bytes,
            "restricted_reads": dict(terminal["restricted_reads"]),
        },
        "project_control_recommendation": {
            "verdict": "HOLD_WAVE4_DEVELOPMENT_ARCHIVE_AND_PAIR_CHALLENGER_NO_AUTOMATIC_VALIDATION",
            "next_action": "FREEZE_WAVE4_DEVELOPMENT_EVIDENCE_AND_COMPARE_WAVE1_WAVE2_WAVE3_WAVE4_YIELD_WITHOUT_READING_VALIDATION_OR_OOS",
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
