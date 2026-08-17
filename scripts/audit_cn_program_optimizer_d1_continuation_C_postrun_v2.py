"""Close fresh D1 continuation C at the pre-validation boundary.

This checker is post-development and zero-validation-read. It independently
verifies the immutable 20-wave C run, its Project Control consumption, prior
exclusion, the restored Transfer Filter V2 derivation, and the already-frozen
V2 top-40% membership. It also emits the next combined prior-exact freeze.
No financial evaluation or validation/holdout/forward access occurs here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from our_system_phase2.services.unified_capability_registry import stable_hash

CLOSURE_NAME = "CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_COMPLETE.json"
EXPECTED_POLICY = "CN_PROGRAM_OPTIMIZER_TPE_BOOTSTRAP_TO_SURROGATE_D1_V1"
EXPECTED_SELECTORS = {
    "SURROGATE_FULL_ACQUISITION": 84,
    "TPE_BOOTSTRAP": 21,
    "UNIFORM": 35,
}
EXPECTED_RESTRICTED = {
    "validation": 0,
    "holdout": 0,
    "historical_2023": 0,
    "forward_b": 0,
    "forward_2026": 0,
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _productive(row: Mapping[str, Any]) -> bool:
    admission = dict(row.get("admission") or {})
    uplift = row.get("uplift")
    if not bool(admission.get("admitted")) or not isinstance(uplift, Mapping):
        return False
    credit = dict(uplift.get("program_credit") or {})
    return (
        float(credit.get("matched_cumulative_net_return_increment") or 0.0) > 0.0
        and float(credit.get("matched_net_reward_increment") or 0.0) > 0.0
    )


def close_postrun(
    *,
    run_root: Path,
    source_prior_freeze: Path,
    transfer_freeze_root: Path,
    v2_reproduction_receipt: Path,
    project_control_admission: Path,
    output_root: Path,
    evidence_repo_sha: str,
) -> dict[str, Any]:
    run_root = run_root.resolve()
    source_prior_freeze = source_prior_freeze.resolve()
    transfer_freeze_root = transfer_freeze_root.resolve()
    v2_reproduction_receipt = v2_reproduction_receipt.resolve()
    project_control_admission = project_control_admission.resolve()
    output_root = output_root.resolve()
    if output_root.exists():
        raise FileExistsError(output_root)

    closure_path = run_root / CLOSURE_NAME
    closure = _read_json(closure_path)
    closure_payload_sha = _verify(closure, "closure_payload_sha256", "C closure")
    if (
        closure.get("status") != "CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_COMPLETE"
        or closure.get("campaign_id") != "CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_CONTINUATION_C_V1"
        or closure.get("policy_id") != EXPECTED_POLICY
        or int(closure.get("closed_waves") or 0) != 20
        or int(closure.get("logical_records") or 0) != 140
        or int(closure.get("physical_evaluation_calls") or 0) != 140
        or dict(closure.get("selector_counts") or {}) != EXPECTED_SELECTORS
        or dict(closure.get("restricted_reads") or {}) != EXPECTED_RESTRICTED
        or str(closure.get("oos_authority") or "") != "NONE"
        or bool(closure.get("promotion_authorized"))
        or bool(closure.get("automatic_successor_authorized"))
    ):
        raise RuntimeError("C closure contract drift")

    source_prior = _read_json(source_prior_freeze)
    source_prior_payload_sha = _verify(source_prior, "freeze_payload_sha256", "C source prior freeze")
    source_prior_ids = list(map(str, source_prior.get("combined_prior_exact_identities") or ()))
    if (
        int(source_prior.get("combined_prior_exact_count") or 0) != 1170
        or len(source_prior_ids) != 1170
        or len(set(source_prior_ids)) != 1170
        or stable_hash(source_prior_ids) != str(source_prior.get("combined_prior_exact_identities_sha256") or "")
        or int(source_prior.get("remaining_enhanced_program_count") or 0) != 2414
        or int(source_prior.get("validation_reads") or 0) != 0
        or int(source_prior.get("holdout_reads") or 0) != 0
        or int(source_prior.get("forward_2026_reads") or 0) != 0
    ):
        raise RuntimeError("C source prior contract drift")
    prior_set = set(source_prior_ids)

    closure_manifest_hashes = list(map(str, closure.get("wave_manifest_sha256") or ()))
    if len(closure_manifest_hashes) != 20:
        raise RuntimeError("C closure wave-manifest coverage drift")
    previous_manifest_sha = "GENESIS"
    all_exacts: list[str] = []
    selector_counts: dict[str, int] = {}
    admitted = 0
    productive = 0
    post_bootstrap_admitted = 0
    post_bootstrap_productive = 0
    surrogate_admitted = 0
    surrogate_productive = 0
    errors: list[str] = []

    for wave in range(20):
        wave_root = run_root / f"wave_{wave:03d}"
        manifest_path = wave_root / "wave_manifest.json"
        manifest = _read_json(manifest_path)
        _verify(manifest, "manifest_payload_sha256", f"C wave {wave} manifest")
        manifest_file_sha = _sha256(manifest_path)
        if manifest_file_sha != closure_manifest_hashes[wave]:
            raise RuntimeError(f"C wave {wave} closure/manifest hash drift")
        if str(manifest.get("previous_wave_manifest_sha256") or "") != previous_manifest_sha:
            raise RuntimeError(f"C wave {wave} manifest chain drift")
        manifest_wave = manifest.get("wave_index")
        if manifest_wave is None or int(manifest_wave) != wave:
            raise RuntimeError(f"C wave {wave} manifest ordinal drift")
        previous_manifest_sha = manifest_file_sha

        asks = _read_jsonl(wave_root / "logical_asks.jsonl")
        schedules = _read_jsonl(wave_root / "physical_schedules.jsonl")
        results = _read_jsonl(wave_root / "physical_results.jsonl")
        if len(asks) != 7 or len(schedules) != 7 or len(results) != 7:
            raise RuntimeError(f"C wave {wave} 7/7/7 cardinality drift")
        ask_by_exact = {str(row.get("exact_identity") or ""): row for row in asks}
        schedule_by_exact = {str(row.get("d1_exact_identity") or ""): row for row in schedules}
        result_by_exact = {str(row.get("exact_identity") or ""): row for row in results}
        if len(ask_by_exact) != 7 or set(ask_by_exact) != set(schedule_by_exact) or set(ask_by_exact) != set(result_by_exact):
            raise RuntimeError(f"C wave {wave} exact-set drift")

        for exact, ask in ask_by_exact.items():
            if not exact:
                raise RuntimeError("C empty exact identity")
            schedule = schedule_by_exact[exact]
            result = result_by_exact[exact]
            schedule_body = {key: value for key, value in schedule.items() if key != "schedule_record_sha256"}
            if stable_hash(schedule_body) != str(schedule.get("schedule_record_sha256") or ""):
                raise RuntimeError(f"C schedule self-hash drift: {exact}")
            if (
                int(schedule.get("d1_wave_index") if schedule.get("d1_wave_index") is not None else -1) != wave
                or str(schedule.get("d1_logical_proposal_id") or "") != str(ask.get("logical_proposal_id") or "")
                or str(schedule.get("d1_selection_kind") or "") != str(ask.get("selection_kind") or "")
            ):
                raise RuntimeError(f"C logical/schedule lineage drift: {exact}")
            selection = str(ask.get("selection_kind") or "")
            selector_counts[selection] = selector_counts.get(selection, 0) + 1
            is_admitted = bool(dict(result.get("admission") or {}).get("admitted"))
            is_productive = _productive(result)
            admitted += int(is_admitted)
            productive += int(is_productive)
            if wave >= 4:
                post_bootstrap_admitted += int(is_admitted)
                post_bootstrap_productive += int(is_productive)
            if selection == "SURROGATE_FULL_ACQUISITION":
                surrogate_admitted += int(is_admitted)
                surrogate_productive += int(is_productive)
            all_exacts.append(exact)

    unique_exacts = sorted(set(all_exacts))
    overlap = sorted(set(unique_exacts).intersection(prior_set))
    if len(all_exacts) != 140 or len(unique_exacts) != 140 or overlap:
        raise RuntimeError("C fresh-exact contract drift")
    if selector_counts != EXPECTED_SELECTORS:
        raise RuntimeError("C selector-count drift")

    metrics = {
        "total": {"evaluated": 140, "admitted": admitted, "productive": productive},
        "post_bootstrap": {"evaluated": 112, "admitted": post_bootstrap_admitted, "productive": post_bootstrap_productive},
        "surrogate_optimized": {"evaluated": 84, "admitted": surrogate_admitted, "productive": surrogate_productive},
    }
    closure_metrics = dict(closure.get("policy_metrics") or {})
    for name, closure_key in (
        ("total", "TOTAL_POLICY_EFFICIENCY"),
        ("post_bootstrap", "POST_BOOTSTRAP_EFFICIENCY"),
        ("surrogate_optimized", "SURROGATE_OPTIMIZED_EFFICIENCY"),
    ):
        observed = metrics[name]
        expected = dict(closure_metrics.get(closure_key) or {})
        if any(int(expected.get(key) or 0) != int(observed[key]) for key in ("evaluated", "admitted", "productive")):
            raise RuntimeError(f"C {name} metric drift")

    admission = _read_json(project_control_admission)
    admission_payload_sha = _verify(admission, "admission_payload_sha256", "C Project Control admission")
    if (
        admission.get("requested_action") != "LAUNCH_HIGH_COST_CAMPAIGN"
        or admission.get("target_campaign_id") != "cn-program-optimizer-d1-development-v1"
        or admission.get("target_run_id") != run_root.name
        or Path(str(admission.get("target_output_root") or "")).resolve() != run_root
        or admission.get("repo_sha") != closure.get("repo_sha")
        or admission.get("project_control_preflight", {}).get("request", {}).get("target_campaign_instance_id") != "CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_CONTINUATION_C_V1"
    ):
        raise RuntimeError("C Project Control target drift")
    consumption_root = run_root / ".project_control_execution" / "consumptions"
    consumption_files = list(consumption_root.glob("*.json"))
    if len(consumption_files) != 1 or consumption_files[0].stem != admission_payload_sha:
        raise RuntimeError("C Project Control consumption drift")

    v2_repro = _read_json(v2_reproduction_receipt)
    v2_repro_payload_sha = _verify(v2_repro, "check_payload_sha256", "V2 reproduction receipt")
    if (
        v2_repro.get("status") != "PASS_V2_MODEL_AND_LOCO_REPRODUCTION"
        or not all(bool(value) for value in dict(v2_repro.get("checks") or {}).values())
        or int(v2_repro.get("candidate_count") or 0) != 183
        or int(v2_repro.get("validation_productive_count") or 0) != 50
        or int(v2_repro.get("holdout_reads") or 0) != 0
        or int(v2_repro.get("forward_2026_reads") or 0) != 0
        or v2_repro.get("frozen_model", {}).get("filter_payload_sha256") != "512c81728e3fca363ffcb3a148b3fd09f088e1f56d9f3f330b9a9f1170466081"
    ):
        raise RuntimeError("V2 reproduction PASS contract drift")

    transfer_freeze_path = transfer_freeze_root / "validation_candidate_freeze.json"
    transfer_members_path = transfer_freeze_root / "validation_candidate_members.jsonl"
    transfer_application_path = transfer_freeze_root / "transfer_filter_application.json"
    transfer_freeze = _read_json(transfer_freeze_path)
    transfer_freeze_payload_sha = _verify(transfer_freeze, "freeze_payload_sha256", "C transfer freeze")
    transfer_application = _read_json(transfer_application_path)
    transfer_application_payload_sha = _verify(transfer_application, "application_payload_sha256", "C transfer application")
    transfer_members = _read_jsonl(transfer_members_path)
    selected_ids = sorted(str(row["exact_identity"]) for row in transfer_members if bool(row.get("transfer_filter_selected")))
    member_ids = sorted(str(row["exact_identity"]) for row in transfer_members)
    if (
        transfer_freeze.get("status") != "FROZEN_BEFORE_VALIDATION_ACCESS"
        or transfer_freeze.get("source_cohort") != "D1_CONTINUATION_C"
        or int(transfer_freeze.get("candidate_count") or 0) != productive
        or int(transfer_freeze.get("transfer_filter_selected_count") or 0) != 27
        or stable_hash(transfer_members) != str(transfer_freeze.get("candidate_members_payload_sha256") or "")
        or stable_hash(member_ids) != str(transfer_freeze.get("candidate_exact_identities_sha256") or "")
        or stable_hash(selected_ids) != str(transfer_freeze.get("transfer_filter_selected_exact_identities_sha256") or "")
        or int(transfer_application.get("development_productive_count") or 0) != productive
        or int(transfer_application.get("selected_count") or 0) != 27
        or int(transfer_application.get("validation_reads") or 0) != 0
        or int(transfer_application.get("holdout_reads") or 0) != 0
        or int(transfer_application.get("forward_2026_reads") or 0) != 0
        or not bool(transfer_application.get("selection_frozen_before_validation"))
    ):
        raise RuntimeError("C transfer membership freeze drift")
    if not set(member_ids).issubset(set(unique_exacts)):
        raise RuntimeError("C transfer member not in C development exact set")
    for member in transfer_members:
        wins = list(member.get("development_window_return_increments") or ())
        if len(wins) != 3:
            raise RuntimeError("C transfer member missing ordered development windows")

    combined_ids = sorted(prior_set.union(unique_exacts))
    if len(combined_ids) != 1310:
        raise RuntimeError("C combined prior count drift")
    postrun_prior: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_d1_continuation_C_postrun_prior_exact_freeze_v1",
        "status": "FROZEN_AFTER_C_DEVELOPMENT_BEFORE_VALIDATION_ACCESS",
        "source_run_root": str(run_root),
        "source_run_closure_file_sha256": _sha256(closure_path),
        "source_run_closure_payload_sha256": closure_payload_sha,
        "source_prior_freeze_file_sha256": _sha256(source_prior_freeze),
        "source_prior_freeze_payload_sha256": source_prior_payload_sha,
        "source_prior_exact_count": 1170,
        "source_prior_exact_identities_sha256": str(source_prior["combined_prior_exact_identities_sha256"]),
        "new_development_exact_count": 140,
        "new_development_exact_identities_sha256": stable_hash(unique_exacts),
        "combined_prior_exact_count": 1310,
        "combined_prior_exact_identities": combined_ids,
        "combined_prior_exact_identities_sha256": stable_hash(combined_ids),
        "enhanced_program_count": 3584,
        "remaining_enhanced_program_count": 2274,
        "prior_results_imported_as_optimizer_feedback": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    postrun_prior["freeze_payload_sha256"] = stable_hash(postrun_prior)

    audit: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_d1_continuation_C_postrun_independent_audit_v1",
        "status": "PASS",
        "source_run_root": str(run_root),
        "audit": {
            "status": "PASS",
            "errors": errors,
            "closed_waves": 20,
            "logical_records": 140,
            "physical_evaluation_calls": 140,
            "unique_exact": 140,
            "prior_overlap_count": 0,
            "selector_counts": selector_counts,
            "metric_total": metrics["total"],
            "metric_post_bootstrap": metrics["post_bootstrap"],
            "metric_surrogate_optimized": metrics["surrogate_optimized"],
            "restricted_reads": EXPECTED_RESTRICTED,
            "project_control_consumption_count": 1,
            "project_control_task_id": str(admission.get("project_control_preflight", {}).get("task_id") or ""),
            "project_control_run_id": str(admission.get("project_control_preflight", {}).get("run_id") or ""),
            "project_control_admission_payload_sha256": admission_payload_sha,
            "v2_reproduction_status": v2_repro["status"],
            "v2_reproduction_payload_sha256": v2_repro_payload_sha,
            "transfer_filter_id": str(transfer_freeze.get("transfer_filter_id") or ""),
            "transfer_candidate_count": len(transfer_members),
            "transfer_selected_count": len(selected_ids),
            "transfer_selected_exact_identities_sha256": str(transfer_freeze["transfer_filter_selected_exact_identities_sha256"]),
            "validation_accessed": False,
        },
        "closure_file_sha256": _sha256(closure_path),
        "closure_payload_sha256": closure_payload_sha,
        "transfer_freeze_file_sha256": _sha256(transfer_freeze_path),
        "transfer_freeze_payload_sha256": transfer_freeze_payload_sha,
        "transfer_application_file_sha256": _sha256(transfer_application_path),
        "transfer_application_payload_sha256": transfer_application_payload_sha,
        "v2_reproduction_receipt_file_sha256": _sha256(v2_reproduction_receipt),
        "v2_reproduction_receipt_payload_sha256": v2_repro_payload_sha,
        "project_control_admission_file_sha256": _sha256(project_control_admission),
        "project_control_admission_payload_sha256": admission_payload_sha,
        "postrun_prior_payload_sha256": postrun_prior["freeze_payload_sha256"],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    audit["audit_payload_sha256"] = stable_hash(audit)

    outcome: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_d1_continuation_C_postrun_outcome_v1",
        "status": "C_DEVELOPMENT_COMPLETE_AND_V2_MEMBERSHIP_FROZEN_BEFORE_VALIDATION",
        "decision": "READY_FOR_SEPARATELY_AUTHORIZED_PROSPECTIVE_C_VALIDATION",
        "execution_repo_sha": str(closure["repo_sha"]),
        "postrun_evidence_repo_sha": str(evidence_repo_sha),
        "candidate_count": 140,
        "development_admitted_count": admitted,
        "development_productive_count": productive,
        "prior_overlap_count": 0,
        "transfer_filter_candidate_count": len(transfer_members),
        "transfer_filter_selected_count": len(selected_ids),
        "transfer_filter_selected_exact_identities_sha256": str(transfer_freeze["transfer_filter_selected_exact_identities_sha256"]),
        "v2_reproduction_status": v2_repro["status"],
        "v2_reproduction_payload_sha256": v2_repro_payload_sha,
        "postrun_prior_exact_count": 1310,
        "remaining_unseen_enhanced_count": 2274,
        "independent_audit_status": audit["status"],
        "independent_audit_payload_sha256": audit["audit_payload_sha256"],
        "validation_accessed": False,
        "validation_authorized_by_this_outcome": False,
        "holdout_authorized": False,
        "forward_2026_authorized": False,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion_authorized": False,
        "automatic_successor_authorized": False,
        "next_action": "EXTERNAL_REVIEW_THEN_SEPARATELY_AUTHORIZE_REPORT_ONLY_C_VALIDATION_IF_APPROVED",
    }
    outcome["outcome_payload_sha256"] = stable_hash(outcome)

    evidence_note: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_d1_continuation_C_postrun_evidence_note_v1",
        "status": outcome["status"],
        "headline": "Fresh C development closed 140/140 with zero prior overlap and zero restricted reads; 67 development-positive Programs were scored by the independently reproduced frozen V2 filter and 27 were frozen before any C validation access.",
        "C_execution_repo_sha": str(closure["repo_sha"]),
        "postrun_evidence_repo_sha": outcome["postrun_evidence_repo_sha"],
        "C_development_productive": productive,
        "V2_selected": len(selected_ids),
        "V2_selected_exact_identities_sha256": str(transfer_freeze["transfer_filter_selected_exact_identities_sha256"]),
        "V2_reproduction_payload_sha256": v2_repro_payload_sha,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
        "next_decision": outcome["next_action"],
    }
    evidence_note["evidence_note_payload_sha256"] = stable_hash(evidence_note)

    output_root.mkdir(parents=True, exist_ok=False)
    prior_path = _write_json(output_root / "cn_program_optimizer_d1_continuation_C_postrun_prior_exact_freeze_20260817.json", postrun_prior)
    audit_path = _write_json(output_root / "cn_program_optimizer_d1_continuation_C_postrun_independent_audit_20260817.json", audit)
    outcome_path = _write_json(output_root / "cn_program_optimizer_d1_continuation_C_postrun_outcome_20260817.json", outcome)
    note_path = _write_json(output_root / "cn_program_optimizer_d1_continuation_C_postrun_evidence_note_20260817.json", evidence_note)
    result = {
        "status": outcome["status"],
        "development_productive_count": productive,
        "transfer_selected_count": len(selected_ids),
        "postrun_prior_exact_count": 1310,
        "remaining_unseen_enhanced_count": 2274,
        "audit_payload_sha256": audit["audit_payload_sha256"],
        "outcome_payload_sha256": outcome["outcome_payload_sha256"],
        "postrun_prior_payload_sha256": postrun_prior["freeze_payload_sha256"],
        "evidence_note_payload_sha256": evidence_note["evidence_note_payload_sha256"],
        "files": {
            "prior": str(prior_path),
            "audit": str(audit_path),
            "outcome": str(outcome_path),
            "evidence_note": str(note_path),
        },
    }
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--source-prior-freeze", type=Path, required=True)
    parser.add_argument("--transfer-freeze-root", type=Path, required=True)
    parser.add_argument("--v2-reproduction-receipt", type=Path, required=True)
    parser.add_argument("--project-control-admission", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--evidence-repo-sha", required=True)
    args = parser.parse_args(argv)
    result = close_postrun(
        run_root=args.run_root,
        source_prior_freeze=args.source_prior_freeze,
        transfer_freeze_root=args.transfer_freeze_root,
        v2_reproduction_receipt=args.v2_reproduction_receipt,
        project_control_admission=args.project_control_admission,
        output_root=args.output_root,
        evidence_repo_sha=str(args.evidence_repo_sha),
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
