"""Fixed-schedule development falsification for the disclosure-timing mechanism family."""
from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_large_fresh_v1 as large_fresh
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as successor
from our_system_phase2.services.program_search_optimizer_v1 import program_structural_genes_v1
from our_system_phase2.services.unified_capability_registry import stable_hash

TEMPLATE_ID = "BASE_TEMPORAL_EVENT"
PHYSICAL_GENERATION_ARM = "UNIFORM_FRESH"
CHECKPOINT_SIZE = 24
PRIMARY_EXECUTOR_WORKERS = 24
RESOURCE_CANARY_PROBE_SECONDS = 30.0
ORIGINAL_EVENT_COMPONENT_ID = (
    "d692016e07c0d0ee562a9bd9c060c3c9a6efb7454c1b0803710baf82d0aff2bc"
)
STAGE_A = "A_EVENT_TRANSFER"
STAGE_B = "B_JOINT_TRANSFER"
CLASS_LOCAL = "LOCAL_PRIMITIVE_WIN_NOT_SYSTEMATIC_EVENT_FAMILY"
CLASS_EVENT_ONLY = "EVENT_FAMILY_WIN_BUT_TEMPORAL_ADAPTATION_NOT_TRANSFERABLE"
CLASS_SYSTEMATIC = "SYSTEMATIC_DISCLOSURE_TIMING_X_FUNDAMENTAL_STATE_DEVELOPMENT_FAMILY"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]


def _self_hashed(payload: Mapping[str, Any], field: str) -> dict[str, Any]:
    body = dict(payload)
    body[field] = stable_hash(body)
    return body


def verify_prefreeze(path: Path) -> dict[str, Any]:
    payload = _read_json(path)
    body = dict(payload)
    claimed = str(body.pop("prefreeze_payload_sha256", ""))
    if claimed != stable_hash(body):
        raise RuntimeError("MECHANISM_PREFREEZE_HASH_DRIFT")
    if (
        payload.get("status") != "PREFINANCIAL_MECHANISM_FALSIFICATION_DESIGN_FROZEN"
        or bool(payload.get("financial_candidate_evaluation_performed"))
        or int(dict(payload.get("design") or {}).get("stage_a", {}).get("records") or 0) != 288
        or int(dict(payload.get("design") or {}).get("stage_b", {}).get("records") or 0) != 264
    ):
        raise RuntimeError("MECHANISM_PREFREEZE_CONTRACT_DRIFT")
    candidates = dict(payload.get("candidates") or {})
    stage_a = list(candidates.get("stage_a") or ())
    stage_b = list(candidates.get("stage_b") or ())
    if len(stage_a) != 288 or len(stage_b) != 264:
        raise RuntimeError("MECHANISM_PREFREEZE_CANDIDATE_COUNT_DRIFT")
    exacts = [str(row.get("exact_identity") or "") for row in stage_a + stage_b]
    if not all(exacts) or len(set(exacts)) != 552:
        raise RuntimeError("MECHANISM_PREFREEZE_EXACT_IDENTITY_DRIFT")
    fields = tuple(map(str, dict(payload["resource_preview"])["union_field_columns"]))
    if (
        len(fields) != int(dict(payload["resource_preview"])["union_field_count"])
        or stable_hash(list(fields))
        != str(dict(payload["resource_preview"])["union_field_columns_sha256"])
    ):
        raise RuntimeError("MECHANISM_PREFREEZE_FIELD_UNION_DRIFT")
    return payload


def verify_spent_freeze(path: Path) -> dict[str, Any]:
    payload = _read_json(path)
    body = dict(payload)
    claimed = str(body.pop("freeze_payload_sha256", ""))
    if claimed != stable_hash(body):
        raise RuntimeError("MECHANISM_SPENT_FREEZE_HASH_DRIFT")
    ids = list(map(str, payload.get("combined_spent_exact_identities") or ()))
    if (
        payload.get("status") != "SPENT_EXACT_DENY_SET_FROZEN"
        or len(ids) != 2150
        or len(set(ids)) != 2150
        or stable_hash(ids)
        != str(payload.get("combined_spent_exact_identities_sha256") or "")
    ):
        raise RuntimeError("MECHANISM_SPENT_FREEZE_IDENTITY_DRIFT")
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
        input_binding_schema_version=(
            "cn_program_disclosure_timing_mechanism_successor_input_binding_v1"
        ),
        resource_profile_id="SEARCH_DUAL_24",
        resource_profile_role="SEARCH",
        maximum_executor_workers=PRIMARY_EXECUTOR_WORKERS,
    )


def _reservoir_record(
    candidate: Mapping[str, Any],
    *,
    components_by_id: Mapping[str, Any],
) -> dict[str, Any]:
    components = {
        role: components_by_id[str(candidate[f"{role}_component_id"])]
        for role in ("base", "temporal", "event")
    }
    policy = dict(candidate["combination_policy"])
    record: dict[str, Any] = {
        "schema_version": "cn_joint_program_phase_c_reservoir_record_v0",
        "template_id": TEMPLATE_ID,
        "raw_reservoir_ordinal": int(candidate["stage_ordinal"]),
        "template_reservoir_ordinal": int(candidate["stage_ordinal"]),
        "components": {
            role: {
                "component_id": component.component_id,
                "route_id": component.route_id,
                "proposal_id": component.proposal_id,
                "generation_receipt_hash": component.generation_receipt_hash,
            }
            for role, component in components.items()
        },
        "combination_policy": policy,
        "semantic_compile_required_at_selection": True,
        "semantic_noop_does_not_count_toward_quota": True,
        "financial_evaluation_executed": False,
    }
    record["raw_combination_sha256"] = stable_hash(
        {
            "template_id": TEMPLATE_ID,
            "component_ids": {
                role: component.component_id
                for role, component in sorted(components.items())
            },
            "combination_policy": policy,
        }
    )
    record["reservoir_record_sha256"] = stable_hash(record)
    return record


def _fixed_schedule(
    candidate: Mapping[str, Any],
    *,
    authority: Mapping[str, Any],
    global_ordinal: int,
    checkpoint_ordinal: int,
) -> dict[str, Any]:
    reservoir = _reservoir_record(
        candidate, components_by_id=authority["components_by_id"]
    )
    entry = engine._catalog_entry(
        reservoir,
        components_by_id=authority["components_by_id"],
        adapter=authority["adapter"],
        compiler=authority["compiler"],
    )
    if entry.get("status") != "EXECUTABLE":
        raise RuntimeError("MECHANISM_FIXED_CANDIDATE_NOT_EXECUTABLE")
    exact = stable_hash(dict(entry["program_genes"]))
    if exact != str(candidate["exact_identity"]):
        raise RuntimeError("MECHANISM_FIXED_CANDIDATE_EXACT_DRIFT")
    if str(entry["program_id"]) != str(candidate["program_id"]):
        raise RuntimeError("MECHANISM_FIXED_CANDIDATE_PROGRAM_DRIFT")

    ask: dict[str, Any] = {
        "schema_version": "cn_program_disclosure_timing_fixed_ask_v1",
        "main_record_ordinal": int(global_ordinal),
        "checkpoint_ordinal": int(checkpoint_ordinal),
        "template_id": TEMPLATE_ID,
        "template_record_ordinal": int(candidate["stage_ordinal"]),
        "generation_arm": PHYSICAL_GENERATION_ARM,
        "absolute_admission_head_eligible": True,
        "conditional_uplift_head_eligible": True,
        "mechanism_stage": str(candidate["stage"]),
        "mechanism_exact_identity": exact,
    }
    ask["ask_record_sha256"] = stable_hash(ask)
    decision = _self_hashed(
        {
            "schema_version": "cn_program_disclosure_timing_fixed_selection_v1",
            "selection_mode": "PREFROZEN_FIXED_SCHEDULE",
            "mechanism_stage": str(candidate["stage"]),
            "mechanism_exact_identity": exact,
            "adaptive_template_credit_used": False,
            "optimizer_selection_used": False,
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
            "mechanism_stage": str(candidate["stage"]),
            "mechanism_stage_ordinal": int(candidate["stage_ordinal"]),
            "mechanism_exact_identity": exact,
            "successor_exact_identity": exact,
            "event_component_id": str(candidate["event_component_id"]),
            "temporal_component_id": str(candidate["temporal_component_id"]),
            "base_component_id": str(candidate["base_component_id"]),
            "event_representation_family": str(
                candidate["event_representation_family"]
            ),
            "event_pulse_family": str(candidate["event_pulse_family"]),
            "event_expression": str(candidate["event_expression"]),
            "temporal_expression": str(candidate["temporal_expression"]),
            "optimizer_selection_used": False,
        }
    )
    schedule["schedule_record_sha256"] = stable_hash(
        {
            key: value
            for key, value in schedule.items()
            if key != "schedule_record_sha256"
        }
    )
    expected_fields = tuple(sorted(map(str, candidate["physical_field_columns"])))
    observed_fields = engine._checkpoint_field_columns([schedule])
    if tuple(observed_fields) != expected_fields:
        raise RuntimeError("MECHANISM_FIXED_CANDIDATE_FIELD_DRIFT")
    return schedule


def reconstruct_prefrozen_schedules(
    prefreeze: Mapping[str, Any],
    spent_freeze: Mapping[str, Any],
    *,
    authority: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    spent = frozenset(map(str, spent_freeze["combined_spent_exact_identities"]))
    output: dict[str, list[dict[str, Any]]] = {STAGE_A: [], STAGE_B: []}
    global_ordinal = 0
    checkpoint_ordinal = 0
    seen: set[str] = set()
    for stage_key, stage_name in (("stage_a", STAGE_A), ("stage_b", STAGE_B)):
        candidates = list(dict(prefreeze["candidates"])[stage_key])
        for index, candidate in enumerate(candidates):
            exact = str(candidate["exact_identity"])
            if exact in spent or exact in seen:
                raise RuntimeError("MECHANISM_PREFROZEN_CANDIDATE_NOT_FRESH")
            if str(candidate["stage"]) != stage_name:
                raise RuntimeError("MECHANISM_PREFROZEN_STAGE_DRIFT")
            schedule = _fixed_schedule(
                candidate,
                authority=authority,
                global_ordinal=global_ordinal,
                checkpoint_ordinal=checkpoint_ordinal,
            )
            output[stage_name].append(schedule)
            seen.add(exact)
            global_ordinal += 1
            if (index + 1) % CHECKPOINT_SIZE == 0:
                checkpoint_ordinal += 1
        if len(candidates) % CHECKPOINT_SIZE:
            checkpoint_ordinal += 1
    if len(output[STAGE_A]) != 288 or len(output[STAGE_B]) != 264:
        raise RuntimeError("MECHANISM_RECONSTRUCTED_SCHEDULE_COUNT_DRIFT")
    fields = tuple(sorted(engine._checkpoint_field_columns(output[STAGE_A] + output[STAGE_B])))
    frozen_fields = tuple(map(str, prefreeze["resource_preview"]["union_field_columns"]))
    if fields != frozen_fields:
        raise RuntimeError("MECHANISM_RECONSTRUCTED_FIELD_UNION_DRIFT")
    return output


def prefinancial_rehearsal(
    args: argparse.Namespace,
    *,
    authorization: Mapping[str, Any],
    repo_sha: str,
) -> dict[str, Any]:
    prefreeze = verify_prefreeze(args.mechanism_prefreeze)
    spent_freeze = verify_spent_freeze(args.spent_exact_freeze)
    authority = _load_authority(args, authorization=authorization, repo_sha=repo_sha)
    schedules = reconstruct_prefrozen_schedules(
        prefreeze, spent_freeze, authority=authority
    )
    return {
        "status": "ZERO_FINANCIAL_PREFLIGHT_READY",
        "stage_a_schedule_count": len(schedules[STAGE_A]),
        "stage_b_schedule_count": len(schedules[STAGE_B]),
        "combined_schedule_count": len(schedules[STAGE_A]) + len(schedules[STAGE_B]),
        "spent_exact_count": len(spent_freeze["combined_spent_exact_identities"]),
        "field_column_count": len(prefreeze["resource_preview"]["union_field_columns"]),
        "field_columns_sha256": prefreeze["resource_preview"][
            "union_field_columns_sha256"
        ],
        "candidate_evaluation_executed": False,
        "restricted_reads": 0,
    }


def _result_record(
    record: Mapping[str, Any], schedule: Mapping[str, Any]
) -> dict[str, Any]:
    physical = successor._physical_result(record, schedule)
    admission = physical.admission.to_record()
    uplift = None if physical.uplift is None else physical.uplift.to_record()
    payload = {
        "schema_version": "cn_program_disclosure_timing_mechanism_result_v1",
        "mechanism_stage": str(schedule["mechanism_stage"]),
        "mechanism_stage_ordinal": int(schedule["mechanism_stage_ordinal"]),
        "exact_identity": str(schedule["mechanism_exact_identity"]),
        "event_component_id": str(schedule["event_component_id"]),
        "temporal_component_id": str(schedule["temporal_component_id"]),
        "base_component_id": str(schedule["base_component_id"]),
        "event_representation_family": str(schedule["event_representation_family"]),
        "event_pulse_family": str(schedule["event_pulse_family"]),
        "event_expression": str(schedule["event_expression"]),
        "temporal_expression": str(schedule["temporal_expression"]),
        "combination_policy": dict(schedule["combination_policy"]),
        "admission": admission,
        "uplift": uplift,
        "source_record_payload_sha256": str(record["record_payload_sha256"]),
        "optimizer_selection_used": False,
        "validation_feedback_used": False,
    }
    payload["result_payload_sha256"] = stable_hash(payload)
    return payload


def _productive(row: Mapping[str, Any]) -> bool:
    admission = dict(row.get("admission") or {})
    uplift = dict(row.get("uplift") or {})
    credit = dict(uplift.get("program_credit") or {})
    return bool(admission.get("admitted")) and (
        float(credit.get("matched_cumulative_net_return_increment") or 0.0) > 0.0
        and float(credit.get("matched_net_reward_increment") or 0.0) > 0.0
    )


def _median(rows: Sequence[Mapping[str, Any]], key: str) -> float | None:
    values = []
    for row in rows:
        credit = dict(dict(row.get("uplift") or {}).get("program_credit") or {})
        value = credit.get(key)
        if value is not None:
            values.append(float(value))
    return statistics.median(values) if values else None


def _metric(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(rows)
    n = len(rows)
    productive = sum(_productive(row) for row in rows)
    admitted = sum(bool(dict(row.get("admission") or {}).get("admitted")) for row in rows)
    return {
        "evaluated": n,
        "admitted": admitted,
        "productive": productive,
        "productive_rate": productive / n if n else 0.0,
        "admission_rate": admitted / n if n else 0.0,
        "median_matched_cumulative_net_return_increment": _median(
            rows, "matched_cumulative_net_return_increment"
        ),
        "median_matched_net_reward_increment": _median(
            rows, "matched_net_reward_increment"
        ),
    }


def _group_metrics(
    rows: Sequence[Mapping[str, Any]], key: str
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row[key])].append(row)
    return {name: _metric(group) for name, group in sorted(groups.items())}


def _leave_one_out_min(rows: Sequence[Mapping[str, Any]], key: str) -> float:
    keys = sorted({str(row[key]) for row in rows})
    rates = [
        _metric([row for row in rows if str(row[key]) != value])["productive_rate"]
        for value in keys
    ]
    return min(rates) if rates else 0.0


def _stage_a_gate(
    rows: Sequence[Mapping[str, Any]], gates: Mapping[str, Any]
) -> dict[str, Any]:
    anchor = [
        row for row in rows if str(row["event_component_id"]) == ORIGINAL_EVENT_COMPONENT_ID
    ]
    siblings = [
        row for row in rows if str(row["event_component_id"]) != ORIGINAL_EVENT_COMPONENT_ID
    ]
    anchor_metric = _metric(anchor)
    sibling_metric = _metric(siblings)
    by_event = _group_metrics(siblings, "event_component_id")
    by_rep = _group_metrics(siblings, "event_representation_family")
    by_pulse = _group_metrics(siblings, "event_pulse_family")
    anchor_rule = dict(gates["stage_a_anchor_gate"])
    family_rule = dict(gates["stage_a_systematic_event_family_gate"])
    checks = {
        "anchor_productive_rate": anchor_metric["productive_rate"]
        >= float(anchor_rule["minimum_productive_rate"]),
        "sibling_aggregate_productive_rate": sibling_metric["productive_rate"]
        >= float(family_rule["sibling_aggregate_minimum_productive_rate"]),
        "sibling_matched_return_median_positive": float(
            sibling_metric["median_matched_cumulative_net_return_increment"] or 0.0
        )
        > 0.0,
        "sibling_matched_reward_median_positive": float(
            sibling_metric["median_matched_net_reward_increment"] or 0.0
        )
        > 0.0,
        "representation_breadth": sum(
            metric["productive_rate"] >= 0.40 for metric in by_rep.values()
        )
        >= int(
            family_rule["minimum_representation_families_with_productive_rate_ge_0_40"]
        ),
        "pulse_breadth": sum(
            metric["productive_rate"] >= 0.40 for metric in by_pulse.values()
        )
        >= int(family_rule["minimum_pulse_families_with_productive_rate_ge_0_40"]),
        "leave_one_event_out": _leave_one_out_min(siblings, "event_component_id")
        >= float(family_rule["leave_one_event_component_out_minimum_productive_rate"]),
        "event_component_breadth": sum(
            metric["productive_rate"] >= 0.33 for metric in by_event.values()
        )
        >= int(
            family_rule[
                "minimum_sibling_event_components_with_productive_rate_ge_0_33"
            ]
        ),
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "anchor": anchor_metric,
        "siblings": sibling_metric,
        "by_event_component": by_event,
        "by_representation_family": by_rep,
        "by_pulse_family": by_pulse,
        "leave_one_event_component_out_minimum_productive_rate": _leave_one_out_min(
            siblings, "event_component_id"
        ),
    }


def _stage_b_gate(
    rows: Sequence[Mapping[str, Any]], gates: Mapping[str, Any]
) -> dict[str, Any]:
    rule = dict(gates["stage_b_joint_transfer_gate"])
    metric = _metric(rows)
    by_temporal = _group_metrics(rows, "temporal_component_id")
    by_rep = _group_metrics(rows, "event_representation_family")
    by_pulse = _group_metrics(rows, "event_pulse_family")
    checks = {
        "aggregate_productive_rate": metric["productive_rate"]
        >= float(rule["aggregate_minimum_productive_rate"]),
        "matched_return_median_positive": float(
            metric["median_matched_cumulative_net_return_increment"] or 0.0
        )
        > 0.0,
        "matched_reward_median_positive": float(
            metric["median_matched_net_reward_increment"] or 0.0
        )
        > 0.0,
        "unseen_temporal_breadth": sum(
            cell["productive_rate"] >= 0.30 for cell in by_temporal.values()
        )
        >= int(rule["minimum_unseen_temporal_components_with_productive_rate_ge_0_30"]),
        "leave_one_temporal_out": _leave_one_out_min(rows, "temporal_component_id")
        >= float(rule["leave_one_temporal_component_out_minimum_productive_rate"]),
        "representation_breadth": sum(
            cell["productive_rate"] >= 0.35 for cell in by_rep.values()
        )
        >= int(
            rule["minimum_representation_families_with_productive_rate_ge_0_35"]
        ),
        "pulse_breadth": sum(
            cell["productive_rate"] >= 0.35 for cell in by_pulse.values()
        )
        >= int(rule["minimum_pulse_families_with_productive_rate_ge_0_35"]),
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "aggregate": metric,
        "by_temporal_component": by_temporal,
        "by_representation_family": by_rep,
        "by_pulse_family": by_pulse,
        "leave_one_temporal_component_out_minimum_productive_rate": _leave_one_out_min(
            rows, "temporal_component_id"
        ),
    }


def _close_checkpoint(
    *,
    inflight: Path,
    closed: Path,
    previous_manifest_file_sha256: str,
    stage: str,
    checkpoint_ordinal: int,
) -> str:
    artifacts = [
        successor._artifact(path, inflight)
        for path in sorted(inflight.rglob("*"))
        if path.is_file() and path.name != "checkpoint_manifest.json"
    ]
    manifest = engine._self_hashed(
        {
            "schema_version": "cn_program_disclosure_timing_checkpoint_manifest_v1",
            "status": "MECHANISM_CHECKPOINT_CLOSED_IMMUTABLE",
            "stage": stage,
            "checkpoint_ordinal": int(checkpoint_ordinal),
            "previous_checkpoint_manifest_file_sha256": str(
                previous_manifest_file_sha256
            ),
            "artifacts": artifacts,
        },
        "manifest_payload_sha256",
    )
    engine._write_json(inflight / "checkpoint_manifest.json", manifest)
    if closed.exists():
        raise RuntimeError("MECHANISM_CLOSED_CHECKPOINT_ALREADY_EXISTS")
    inflight.replace(closed)
    return engine._sha256(closed / "checkpoint_manifest.json")


def _run_stage(
    stage: str,
    schedules: Sequence[Mapping[str, Any]],
    *,
    root: Path,
    authority: Mapping[str, Any],
    input_hash: str,
    previous_manifest_file_sha256: str,
    starting_checkpoint_ordinal: int,
) -> tuple[list[dict[str, Any]], str, int]:
    results: list[dict[str, Any]] = []
    checkpoint_ordinal = int(starting_checkpoint_ordinal)
    for offset in range(0, len(schedules), CHECKPOINT_SIZE):
        batch = [dict(row) for row in schedules[offset : offset + CHECKPOINT_SIZE]]
        if len(batch) != CHECKPOINT_SIZE:
            raise RuntimeError("MECHANISM_CHECKPOINT_SIZE_DRIFT")
        inflight = root / f"checkpoint_{checkpoint_ordinal:04d}.inflight"
        closed = root / f"checkpoint_{checkpoint_ordinal:04d}"
        if inflight.exists() or closed.exists():
            raise RuntimeError("MECHANISM_CHECKPOINT_PATH_NOT_FRESH")
        inflight.mkdir(parents=False, exist_ok=False)
        engine._write_jsonl(inflight / "selected_schedule.jsonl", batch)
        records = successor._evaluate_schedules(
            batch,
            record_root=inflight / "records",
            authority=authority,
            input_hash=input_hash,
            executor_workers=PRIMARY_EXECUTOR_WORKERS,
        )
        schedule_by_ordinal = {
            int(row["main_record_ordinal"]): row for row in batch
        }
        checkpoint_results = [
            _result_record(
                record, schedule_by_ordinal[int(record["main_record_ordinal"])]
            )
            for record in records
        ]
        engine._write_jsonl(inflight / "candidate_results.jsonl", checkpoint_results)
        previous_manifest_file_sha256 = _close_checkpoint(
            inflight=inflight,
            closed=closed,
            previous_manifest_file_sha256=previous_manifest_file_sha256,
            stage=stage,
            checkpoint_ordinal=checkpoint_ordinal,
        )
        results.extend(checkpoint_results)
        checkpoint_ordinal += 1
    return results, previous_manifest_file_sha256, checkpoint_ordinal


def run(
    args: argparse.Namespace,
    *,
    admission: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    root = args.output_root.resolve()
    if not root.is_dir() or not (root / ".project_control_execution").is_dir():
        raise RuntimeError("MECHANISM_ADMITTED_OUTPUT_ROOT_MISSING")
    unexpected = {
        path.name for path in root.iterdir() if path.name != ".project_control_execution"
    }
    if unexpected:
        raise RuntimeError("MECHANISM_ADMITTED_OUTPUT_ROOT_NOT_CLEAN")

    prefreeze = verify_prefreeze(args.mechanism_prefreeze)
    spent_freeze = verify_spent_freeze(args.spent_exact_freeze)
    authority = _load_authority(
        args, authorization=authorization, repo_sha=str(admission["repo_sha"])
    )
    schedules = reconstruct_prefrozen_schedules(
        prefreeze, spent_freeze, authority=authority
    )
    engine._write_json(root / "input_binding.json", authority["input_binding"])
    engine._write_json(
        root / "prefreeze_binding.json",
        {
            "schema_version": "cn_program_disclosure_timing_prefreeze_binding_v1",
            "prefreeze_payload_sha256": prefreeze["prefreeze_payload_sha256"],
            "spent_freeze_payload_sha256": spent_freeze["freeze_payload_sha256"],
            "stage_a_count": len(schedules[STAGE_A]),
            "stage_b_count": len(schedules[STAGE_B]),
            "stage_b_candidate_set_frozen_before_stage_a": True,
            "optimizer_selection_used": False,
        },
    )
    input_hash = str(authority["input_binding"]["input_binding_sha256"])
    fields = tuple(map(str, prefreeze["resource_preview"]["union_field_columns"]))
    old_probe = large_fresh.RESOURCE_CANARY_PROBE_SECONDS
    try:
        large_fresh.RESOURCE_CANARY_PROBE_SECONDS = RESOURCE_CANARY_PROBE_SECONDS
        canary = large_fresh._resource_canary(
            authority, input_hash, PRIMARY_EXECUTOR_WORKERS, fields
        )
    finally:
        large_fresh.RESOURCE_CANARY_PROBE_SECONDS = old_probe
    if (
        canary.get("status") != "PASS_ZERO_CANDIDATE_EVALUATION_RESOURCE_CANARY"
        or int(canary.get("requested_workers") or 0) != PRIMARY_EXECUTOR_WORKERS
        or int(canary.get("field_column_count") or 0) != len(fields)
        or int(canary.get("pagefile_pages_in_delta_bytes", -1)) != 0
        or int(canary.get("pagefile_pages_out_delta_bytes", -1)) != 0
        or canary.get("candidate_evaluation_executed") is not False
    ):
        raise RuntimeError("MECHANISM_RESOURCE_CANARY_FAILED")
    engine._write_json(root / "resource_canary.json", canary)

    started = time.perf_counter()
    previous_manifest = "GENESIS"
    checkpoint_ordinal = 0
    stage_a_results, previous_manifest, checkpoint_ordinal = _run_stage(
        STAGE_A,
        schedules[STAGE_A],
        root=root,
        authority=authority,
        input_hash=input_hash,
        previous_manifest_file_sha256=previous_manifest,
        starting_checkpoint_ordinal=checkpoint_ordinal,
    )
    stage_a_gate = _stage_a_gate(stage_a_results, dict(prefreeze["gates"]))
    engine._write_json(root / "stage_a_gate.json", stage_a_gate)
    if stage_a_gate["status"] != "PASS":
        classification = CLASS_LOCAL
        stage_b_results: list[dict[str, Any]] = []
        stage_b_gate = None
    else:
        stage_b_results, previous_manifest, checkpoint_ordinal = _run_stage(
            STAGE_B,
            schedules[STAGE_B],
            root=root,
            authority=authority,
            input_hash=input_hash,
            previous_manifest_file_sha256=previous_manifest,
            starting_checkpoint_ordinal=checkpoint_ordinal,
        )
        stage_b_gate = _stage_b_gate(stage_b_results, dict(prefreeze["gates"]))
        engine._write_json(root / "stage_b_gate.json", stage_b_gate)
        classification = (
            CLASS_SYSTEMATIC if stage_b_gate["status"] == "PASS" else CLASS_EVENT_ONLY
        )

    closure = engine._self_hashed(
        {
            "schema_version": "cn_program_disclosure_timing_mechanism_successor_complete_v1",
            "status": "DISCLOSURE_TIMING_MECHANISM_SUCCESSOR_COMPLETE",
            "repo_sha": str(admission["repo_sha"]),
            "authorization_payload_sha256": str(
                authorization["authorization_payload_sha256"]
            ),
            "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
            "spent_freeze_payload_sha256": str(spent_freeze["freeze_payload_sha256"]),
            "classification": classification,
            "stage_a_gate": stage_a_gate,
            "stage_b_gate": stage_b_gate,
            "stage_a_evaluated": len(stage_a_results),
            "stage_b_evaluated": len(stage_b_results),
            "total_evaluated": len(stage_a_results) + len(stage_b_results),
            "checkpoint_count": checkpoint_ordinal,
            "last_checkpoint_manifest_file_sha256": previous_manifest,
            "wall_seconds": time.perf_counter() - started,
            "optimizer_selection_used": False,
            "stage_b_candidate_set_frozen_before_stage_a": True,
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
            "automatic_successor_authorized": False,
            "oos_authority": "NONE",
        },
        "closure_payload_sha256",
    )
    engine._write_json(
        root / "CN_PROGRAM_DISCLOSURE_TIMING_MECHANISM_SUCCESSOR_COMPLETE.json",
        closure,
    )
    return closure


__all__ = [
    "verify_prefreeze",
    "verify_spent_freeze",
    "reconstruct_prefrozen_schedules",
    "prefinancial_rehearsal",
    "_stage_a_gate",
    "_stage_b_gate",
    "run",
]
