"""Report-only validation of the frozen Search Core V2 Production Wave 1 shortlist.

The 42 candidates, compiled primary/control schedules, validation windows and
materialized validation context are all frozen before this runner starts.  The
runner reports transfer evidence only: it cannot mutate optimizer/policy state,
reselect candidates, tune thresholds, promote candidates, or read holdout/
forward domains.
"""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
import gc
import json
import math
from pathlib import Path
import sys
import time
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_joint_program_phase_b_v0 as phase_b
from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_portfolio_decoder_v2_oos as oos
from our_system_phase2.services.candidate_program_execution_v1 import apply_compiled_candidate_program_v1
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import (
    MATCHED_CONTROL_CONTRACT_ID,
    conditional_uplift_credit,
)
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry, stable_hash

SCHEMA_VERSION = "cn_search_core_v2_production_wave1_report_only_validation_v1"
STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE1_REPORT_ONLY_VALIDATION_COMPLETE"
CLOSURE_NAME = "CN_SEARCH_CORE_V2_PRODUCTION_WAVE1_REPORT_ONLY_VALIDATION_COMPLETE.json"
RESULTS_NAME = "VALIDATION_RESULTS.jsonl"
CANDIDATE_COUNT = 42
FREEZE_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_shortlist_freeze_20260825.json")
MEMBERS_RELATIVE_PATH = Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_shortlist_members_20260825.jsonl")

_PROCESS_CONTEXT: dict[str, Any] | None = None
_PROCESS_REGISTRY: UnifiedCapabilityRegistry | None = None
_PROCESS_INPUT_HASH: str | None = None
_PROCESS_RECORD_ROOT: Path | None = None
_PROCESS_MEMBER_BY_EXACT: dict[str, dict[str, Any]] | None = None
_PROCESS_WINDOWS: tuple[dict[str, Any], ...] | None = None


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
    return path


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _sha(path: Path) -> str:
    return engine._sha256(path.resolve())


def _wilson(successes: int, total: int) -> dict[str, float]:
    if total <= 0:
        return {"lower": 0.0, "upper": 1.0}
    z = 1.959963984540054
    rate = successes / total
    denom = 1.0 + z * z / total
    center = rate + z * z / (2.0 * total)
    radius = z * math.sqrt(rate * (1.0 - rate) / total + z * z / (4.0 * total * total))
    return {"lower": (center - radius) / denom, "upper": (center + radius) / denom}


def _evaluate_compiled_validation(
    compiled: Any,
    *,
    context: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    output = apply_compiled_candidate_program_v1(
        context["field_frame"],
        compiled,
        data_role="validation_report_only",
        materialized_sidecar_clock_column="trade_time",
        materialized_sidecar_authority="PIT_MATERIALIZED_FIELD_SIDECAR",
    )
    diagnostics = phase_b._signal_diagnostics(output, output["signal"])
    return phase_b._replay_signal(
        output["signal"],
        context=context,
        signal_diagnostics=diagnostics,
        windows=tuple(dict(row) for row in windows),
    )


def _validation_flags(admission: AbsoluteEconomicAdmission, uplift: Any) -> tuple[bool, bool, bool]:
    if not admission.admitted or uplift is None:
        return False, False, False
    credit = dict(uplift.program_credit)
    productive = (
        float(credit.get("matched_cumulative_net_return_increment") or 0.0) > 0.0
        and float(credit.get("matched_net_reward_increment") or 0.0) > 0.0
    )
    positive_windows = int(credit.get("cross_window_positive_increment_count") or 0)
    return productive, productive and positive_windows >= 2, productive and positive_windows >= 3


def _evaluate_one(schedule: Mapping[str, Any], *, ordinal: int) -> dict[str, Any]:
    if (
        _PROCESS_CONTEXT is None
        or _PROCESS_REGISTRY is None
        or _PROCESS_INPUT_HASH is None
        or _PROCESS_RECORD_ROOT is None
        or _PROCESS_MEMBER_BY_EXACT is None
        or _PROCESS_WINDOWS is None
    ):
        raise RuntimeError("Wave1 validation worker is not initialized")
    exact = str(schedule.get("search_core_exact_identity") or schedule.get("successor_exact_identity") or "")
    member = dict(_PROCESS_MEMBER_BY_EXACT[exact])
    primary_compiled = phase_b._compiled(
        schedule,
        program_key="primary_program",
        compiled_key="primary_compiled",
        registry=_PROCESS_REGISTRY,
    )
    control_compiled = phase_b._compiled(
        schedule,
        program_key="control_program",
        compiled_key="control_compiled",
        registry=_PROCESS_REGISTRY,
    )
    primary = None
    control = None
    blocker = None
    try:
        primary = _evaluate_compiled_validation(primary_compiled, context=_PROCESS_CONTEXT, windows=_PROCESS_WINDOWS)
    except phase_b.AShareCandidateReplayBlockerError as exc:
        blocker = {"leg": "PRIMARY", "fail_closed": True, **exc.blocker_details()}
    if blocker is None:
        try:
            control = _evaluate_compiled_validation(control_compiled, context=_PROCESS_CONTEXT, windows=_PROCESS_WINDOWS)
        except phase_b.AShareCandidateReplayBlockerError as exc:
            blocker = {"leg": "BASE_CONTROL", "fail_closed": True, **exc.blocker_details()}
    replay_complete = blocker is None
    reward_increment = (
        float(primary["continuous_book_net_reward"]) - float(control["continuous_book_net_reward"])
        if replay_complete and primary is not None and control is not None
        else None
    )
    return_increment = (
        float(primary["cumulative_net_return"]) - float(control["cumulative_net_return"])
        if replay_complete and primary is not None and control is not None
        else None
    )
    blockers = [str(blocker.get("blocker_code") or "REPLAY_BLOCKED")] if blocker else []
    if replay_complete and primary is not None and control is not None:
        if int(primary["fill_count"]) == 0:
            blockers.append("NO_EXECUTABLE_FILLS")
        if str(primary["behavior_identity"]) == str(control["behavior_identity"]):
            blockers.append("BEHAVIOR_EQUIVALENT_TO_BASE")
    pair_record = {
        "schema_version": "cn_search_core_v2_production_wave1_validation_pair_record_v1",
        "status": "WAVE1_VALIDATION_PAIR_CLOSED_IMMUTABLE",
        "input_binding_sha256": _PROCESS_INPUT_HASH,
        "validation_record_ordinal": int(ordinal),
        "exact_identity": exact,
        "template_id": str(member["template_id"]),
        "development_rank": int(member["development_rank"]),
        "template_shortlist_rank": int(member["template_shortlist_rank"]),
        "behavior_pair_identity": str(member["behavior_pair_identity"]),
        "structural_region_identity": str(member["structural_region_identity"]),
        "record_kind": "ENHANCED_FULL_BASE_PAIR",
        "source_schedule_record_sha256": str(schedule["schedule_record_sha256"]),
        "program_id": str(schedule["primary_program"]["program_id"]),
        "control_program_id": str(schedule["control_program"]["program_id"]),
        "pair_id": str(schedule["pair_id"]),
        "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
        "control_contract_valid": True,
        "compile_status": "PASS",
        "physical_ready": True,
        "dag_ready": True,
        "semantic_noop": False,
        "replay_status": phase_b.PAIR_REPLAY_COMPLETE if replay_complete else phase_b.PAIR_REPLAY_BLOCKED,
        "replay_blocker": blocker,
        "primary": primary,
        "base_control": control,
        "matched_net_reward_increment": reward_increment,
        "matched_cumulative_return_increment": return_increment,
        "blockers": blockers,
        "validation_reads": int(_PROCESS_CONTEXT["validation_reads"]),
        "holdout_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "policy_memory_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    pair_record["record_payload_sha256"] = stable_hash(pair_record)
    admission = AbsoluteEconomicAdmission.evaluate(
        pair_record,
        expected_pair_id=pair_record["pair_id"],
        expected_program_id=pair_record["program_id"],
        expected_control_program_id=pair_record["control_program_id"],
    )
    uplift = conditional_uplift_credit(pair_record, admission)
    productive, stable_2of3, stable_3of3 = _validation_flags(admission, uplift)
    result = {
        "schema_version": "cn_search_core_v2_production_wave1_validation_result_v1",
        "status": "WAVE1_VALIDATION_RESULT_CLOSED_IMMUTABLE",
        "validation_record_ordinal": int(ordinal),
        "exact_identity": exact,
        "template_id": str(member["template_id"]),
        "development_rank": int(member["development_rank"]),
        "template_shortlist_rank": int(member["template_shortlist_rank"]),
        "development_credit": dict(member["development_credit"]),
        "development_productive": bool(member["productive"]),
        "development_stable": bool(member["stable"]),
        "pair_record_sha256": pair_record["record_payload_sha256"],
        "admission": admission.to_record(),
        "uplift": None if uplift is None else uplift.to_record(),
        "validation_productive": productive,
        "validation_stable_2of3": stable_2of3,
        "validation_stable_3of3": stable_3of3,
        "validation_reads": int(_PROCESS_CONTEXT["validation_reads"]),
        "holdout_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "policy_memory_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion_authorized": False,
    }
    result["result_payload_sha256"] = stable_hash(result)
    _write_json(
        _PROCESS_RECORD_ROOT / f"candidate_{ordinal:03d}_{exact[:12]}.json",
        {"pair_record": pair_record, "validation_result": result},
    )
    gc.collect()
    return result


def _initialize_worker(
    source_contract: str,
    prepared_binding: Mapping[str, Any],
    registry_path: str,
    input_hash: str,
    record_root: str,
    members: Sequence[Mapping[str, Any]],
) -> None:
    global _PROCESS_CONTEXT, _PROCESS_REGISTRY, _PROCESS_INPUT_HASH, _PROCESS_RECORD_ROOT, _PROCESS_MEMBER_BY_EXACT, _PROCESS_WINDOWS
    prepared = dict(prepared_binding)
    _PROCESS_CONTEXT = oos._load_validation_context(
        source_contract_path=Path(source_contract),
        validation_field_root=Path(str(prepared["validation_field_root"])),
        validation_label_root=Path(str(prepared["validation_label_root"])),
        validation_session_authority_root=Path(str(prepared["validation_session_authority_root"])),
    )
    _PROCESS_REGISTRY = UnifiedCapabilityRegistry.read(Path(registry_path))
    _PROCESS_INPUT_HASH = str(input_hash)
    _PROCESS_RECORD_ROOT = Path(record_root)
    _PROCESS_MEMBER_BY_EXACT = {str(row["exact_identity"]): dict(row) for row in members}
    _PROCESS_WINDOWS = tuple(dict(row) for row in prepared["validation_windows"])


def _metric(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    admitted = sum(bool(dict(row["admission"])["admitted"]) for row in rows)
    productive = sum(bool(row["validation_productive"]) for row in rows)
    stable_2of3 = sum(bool(row["validation_stable_2of3"]) for row in rows)
    stable_3of3 = sum(bool(row["validation_stable_3of3"]) for row in rows)
    return {
        "evaluated": n,
        "admitted": admitted,
        "productive": productive,
        "stable_2of3": stable_2of3,
        "stable_3of3": stable_3of3,
        "admission_rate": admitted / n if n else 0.0,
        "productive_transfer_rate": productive / n if n else 0.0,
        "stable_2of3_rate": stable_2of3 / n if n else 0.0,
        "stable_3of3_rate": stable_3of3 / n if n else 0.0,
        "productive_wilson_95": _wilson(productive, n),
        "stable_2of3_wilson_95": _wilson(stable_2of3, n),
    }


def _grouped(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    values = sorted({str(row[field]) for row in rows})
    return {value: _metric([row for row in rows if str(row[field]) == value]) for value in values}


def _verify_output_root(output_root: Path) -> Path:
    root = output_root.resolve()
    if not root.is_dir() or not (root / ".project_control_execution").is_dir():
        raise RuntimeError("WAVE1_VALIDATION_ADMITTED_OUTPUT_ROOT_MISSING")
    unexpected = {path.name for path in root.iterdir() if path.name != ".project_control_execution"}
    if unexpected:
        raise RuntimeError("WAVE1_VALIDATION_ADMITTED_OUTPUT_ROOT_NOT_CLEAN")
    return root


def run(
    args: argparse.Namespace,
    *,
    admission: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    repo = args.repo_root.resolve()
    prepared = _read(args.prepared_binding.resolve())
    prepared_hash = _verify(prepared, "prepared_binding_payload_sha256", "Wave1 validation prepared binding")
    if (
        prepared.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_PREFINANCIAL_READY"
        or int(prepared.get("candidate_count") or 0) != CANDIDATE_COUNT
        or prepared.get("candidate_evaluation_executed") is not False
        or prepared.get("candidate_results_generated") is not False
        or int(prepared.get("holdout_reads") or 0) != 0
        or int(prepared.get("forward_reads") or 0) != 0
    ):
        raise RuntimeError("Wave1 validation prepared binding contract drift")
    freeze_path = repo / FREEZE_RELATIVE_PATH
    members_path = repo / MEMBERS_RELATIVE_PATH
    freeze = _read(freeze_path)
    freeze_hash = _verify(freeze, "freeze_payload_sha256", "Wave1 validation shortlist freeze")
    members = _read_jsonl(members_path)
    if (
        freeze.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_SHORTLIST_FROZEN_BEFORE_VALIDATION"
        or int(freeze.get("candidate_count") or 0) != CANDIDATE_COUNT
        or len(members) != CANDIDATE_COUNT
        or freeze.get("membership_frozen_before_validation") is not True
        or freeze.get("threshold_tuning_allowed") is not False
        or freeze.get("same_slice_reselection_allowed") is not False
    ):
        raise RuntimeError("Wave1 validation shortlist contract drift")
    for row in members:
        body = dict(row)
        claimed = str(body.pop("member_payload_sha256", ""))
        if not claimed or stable_hash(body) != claimed:
            raise RuntimeError("Wave1 validation member self-hash drift")
    member_exacts = [str(row["exact_identity"]) for row in members]
    if len(set(member_exacts)) != CANDIDATE_COUNT or stable_hash(member_exacts) != str(freeze["candidate_exact_identities_sha256"]):
        raise RuntimeError("Wave1 validation member exact drift")
    schedules_path = Path(str(prepared["resolved_schedule_path"])).resolve()
    if not schedules_path.is_file() or _sha(schedules_path) != str(prepared["resolved_schedule_file_sha256"]):
        raise RuntimeError("Wave1 validation resolved schedule file drift")
    schedules = _read_jsonl(schedules_path)
    if len(schedules) != CANDIDATE_COUNT:
        raise RuntimeError("Wave1 validation schedule count drift")
    schedule_exacts = []
    member_by_exact = {str(row["exact_identity"]): dict(row) for row in members}
    for schedule in schedules:
        body = {key: value for key, value in schedule.items() if key != "schedule_record_sha256"}
        if stable_hash(body) != str(schedule.get("schedule_record_sha256") or ""):
            raise RuntimeError("Wave1 validation schedule self-hash drift")
        exact = str(schedule.get("search_core_exact_identity") or schedule.get("successor_exact_identity") or "")
        if exact not in member_by_exact:
            raise RuntimeError("Wave1 validation schedule/member exact drift")
        schedule_exacts.append(exact)
    if schedule_exacts != member_exacts:
        raise RuntimeError("Wave1 validation schedule order/membership drift")

    output_root = _verify_output_root(args.output_root)
    record_root = output_root / "records"
    record_root.mkdir()
    input_binding = {
        "schema_version": "cn_search_core_v2_production_wave1_validation_input_binding_v1",
        "repo_sha": str(admission["repo_sha"]),
        "authorization_payload_sha256": str(authorization["authorization_payload_sha256"]),
        "prepared_binding_payload_sha256": prepared_hash,
        "prepared_binding_file_sha256": _sha(args.prepared_binding.resolve()),
        "shortlist_freeze_payload_sha256": freeze_hash,
        "candidate_exact_identities_sha256": str(freeze["candidate_exact_identities_sha256"]),
        "resolved_schedule_file_sha256": str(prepared["resolved_schedule_file_sha256"]),
        "source_contract_path": str(args.source_contract.resolve()),
        "source_contract_sha256": _sha(args.source_contract.resolve()),
        "registry_path": str(args.registry.resolve()),
        "registry_sha256": _sha(args.registry.resolve()),
        "validation_field_manifest_sha256": str(prepared["validation_field_manifest_sha256"]),
        "validation_session_authority_manifest_sha256": str(prepared["validation_session_authority_manifest_sha256"]),
        "validation_windows": list(prepared["validation_windows"]),
        "evaluation_role": "validation",
        "data_role": "validation_report_only",
        "usage": "REPORT_ONLY_FROZEN_WAVE1_SHORTLIST",
        "optimizer_feedback_write": "FORBIDDEN",
        "policy_memory_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "threshold_tuning_allowed": False,
        "same_slice_reselection_allowed": False,
        "promotion": "FORBIDDEN",
        "holdout_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    input_binding["input_binding_sha256"] = stable_hash(input_binding)
    _write_json(output_root / "input_binding.json", input_binding)

    started = time.perf_counter()
    results: list[dict[str, Any]] = []
    with ProcessPoolExecutor(
        max_workers=int(args.workers),
        initializer=_initialize_worker,
        initargs=(
            str(args.source_contract.resolve()),
            dict(prepared),
            str(args.registry.resolve()),
            str(input_binding["input_binding_sha256"]),
            str(record_root),
            tuple(members),
        ),
    ) as executor:
        futures = {
            executor.submit(_evaluate_one, schedule, ordinal=ordinal): ordinal
            for ordinal, schedule in enumerate(schedules)
        }
        pending = set(futures)
        while pending:
            done, pending = wait(pending, timeout=2.0, return_when=FIRST_COMPLETED)
            for future in done:
                results.append(future.result())
            engine._require_runtime_resource_safety(engine._runtime_resource_snapshot())
    results.sort(key=lambda row: int(row["validation_record_ordinal"]))
    if len(results) != CANDIDATE_COUNT or [str(row["exact_identity"]) for row in results] != member_exacts:
        raise RuntimeError("Wave1 validation result coverage/order drift")
    _write_jsonl(output_root / RESULTS_NAME, results)

    metrics = {
        "total": _metric(results),
        "per_template": _grouped(results, "template_id"),
    }
    _write_json(output_root / "validation_metrics.json", metrics)
    closure = {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "repo_sha": str(admission["repo_sha"]),
        "authorization_payload_sha256": str(authorization["authorization_payload_sha256"]),
        "prepared_binding_payload_sha256": prepared_hash,
        "candidate_count": CANDIDATE_COUNT,
        "candidate_exact_identities_sha256": str(freeze["candidate_exact_identities_sha256"]),
        "input_binding_sha256": input_binding["input_binding_sha256"],
        "results_file_sha256": _sha(output_root / RESULTS_NAME),
        "metrics": metrics,
        "validation_windows": list(prepared["validation_windows"]),
        "wall_seconds": float(time.perf_counter() - started),
        "validation_reads_per_worker_context": int(results[0]["validation_reads"]) if results else 0,
        "candidate_evaluation_executed": True,
        "evaluation_data_role": "VALIDATION_REPORT_ONLY",
        "optimizer_feedback_write": "FORBIDDEN",
        "policy_memory_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "threshold_tuning_allowed": False,
        "same_slice_reselection_allowed": False,
        "holdout_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
        "automatic_followon_authorized": False,
        "oos_authority": "VALIDATION_REPORT_ONLY_EVIDENCE_ONLY",
        "project_control_next": "EXTERNAL_REVIEW_REQUIRED_NO_AUTOMATIC_PROMOTION",
    }
    closure["closure_payload_sha256"] = stable_hash(closure)
    _write_json(output_root / CLOSURE_NAME, closure)
    return closure


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", type=Path, required=True)
    p.add_argument("--prepared-binding", type=Path, required=True)
    p.add_argument("--source-contract", type=Path, required=True)
    p.add_argument("--registry", type=Path, required=True)
    p.add_argument("--output-root", type=Path, required=True)
    p.add_argument("--workers", type=int, default=4)
    return p


__all__ = ["STATUS", "CLOSURE_NAME", "RESULTS_NAME", "CANDIDATE_COUNT", "_metric", "run", "parser"]
