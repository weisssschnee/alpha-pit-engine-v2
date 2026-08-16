"""Report-only validation for the frozen 120-member D1 Program cohort."""

from __future__ import annotations

import argparse
from collections import Counter
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
from scripts.prepare_cn_program_optimizer_d1_report_only_validation_v1 import (
    EXPECTED_CANDIDATE_COUNT,
    EXPECTED_CANDIDATE_EXACT_SHA256,
    EXPECTED_FREEZE_PAYLOAD_SHA256,
    FREEZE_RELATIVE_PATH,
    MEMBERS_RELATIVE_PATH,
    VALIDATION_WINDOWS,
    _load_frozen_members,
    _sha256,
    _verify_self_hash,
)
from our_system_phase2.services.candidate_program_execution_v1 import (
    apply_compiled_candidate_program_v1,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import (
    MATCHED_CONTROL_CONTRACT_ID,
    conditional_uplift_credit,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


SCHEMA_VERSION = "cn_program_optimizer_d1_report_only_validation_v1"
STATUS = "CN_PROGRAM_OPTIMIZER_D1_REPORT_ONLY_VALIDATION_COMPLETE"
CLOSURE_NAME = "CN_PROGRAM_OPTIMIZER_D1_REPORT_ONLY_VALIDATION_COMPLETE.json"

_PROCESS_CONTEXT: dict[str, Any] | None = None
_PROCESS_REGISTRY: UnifiedCapabilityRegistry | None = None
_PROCESS_INPUT_HASH: str | None = None
_PROCESS_RECORD_ROOT: Path | None = None
_PROCESS_MEMBER_BY_EXACT: dict[str, dict[str, Any]] | None = None


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _wilson(successes: int, total: int) -> dict[str, float]:
    if total <= 0:
        return {"lower": 0.0, "upper": 1.0}
    z = 1.959963984540054
    rate = successes / total
    denom = 1.0 + z * z / total
    center = rate + z * z / (2.0 * total)
    radius = z * math.sqrt(rate * (1.0 - rate) / total + z * z / (4.0 * total * total))
    return {"lower": (center - radius) / denom, "upper": (center + radius) / denom}


def _evaluate_compiled_validation(compiled, *, context: Mapping[str, Any]) -> dict[str, Any]:
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
        windows=VALIDATION_WINDOWS,
    )


def _evaluate_one(schedule: Mapping[str, Any], *, ordinal: int) -> dict[str, Any]:
    if _PROCESS_CONTEXT is None or _PROCESS_REGISTRY is None or _PROCESS_INPUT_HASH is None or _PROCESS_RECORD_ROOT is None or _PROCESS_MEMBER_BY_EXACT is None:
        raise RuntimeError("D1 validation worker is not initialized")
    exact = str(schedule.get("d1_exact_identity") or schedule.get("successor_exact_identity") or "")
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
        primary = _evaluate_compiled_validation(primary_compiled, context=_PROCESS_CONTEXT)
    except phase_b.AShareCandidateReplayBlockerError as exc:
        blocker = {"leg": "PRIMARY", "fail_closed": True, **exc.blocker_details()}
    if blocker is None:
        try:
            control = _evaluate_compiled_validation(control_compiled, context=_PROCESS_CONTEXT)
        except phase_b.AShareCandidateReplayBlockerError as exc:
            blocker = {"leg": "BASE_CONTROL", "fail_closed": True, **exc.blocker_details()}
    replay_complete = blocker is None
    reward_increment = (
        float(primary["continuous_book_net_reward"]) - float(control["continuous_book_net_reward"])
        if replay_complete and primary is not None and control is not None else None
    )
    return_increment = (
        float(primary["cumulative_net_return"]) - float(control["cumulative_net_return"])
        if replay_complete and primary is not None and control is not None else None
    )
    blockers = [str(blocker.get("blocker_code") or "REPLAY_BLOCKED")] if blocker else []
    if replay_complete and primary is not None and control is not None:
        if int(primary["fill_count"]) == 0:
            blockers.append("NO_EXECUTABLE_FILLS")
        if str(primary["behavior_identity"]) == str(control["behavior_identity"]):
            blockers.append("BEHAVIOR_EQUIVALENT_TO_BASE")
    record = {
        "schema_version": "cn_program_optimizer_d1_validation_pair_record_v1",
        "status": "D1_VALIDATION_PAIR_CLOSED_IMMUTABLE",
        "input_binding_sha256": _PROCESS_INPUT_HASH,
        "validation_record_ordinal": int(ordinal),
        "exact_identity": exact,
        "source_cohort": str(member["source_cohort"]),
        "source_wave": int(member["source_wave"]),
        "template_id": str(member["template_id"]),
        "selection_kind": str(member["selection_kind"]),
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
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    record["record_payload_sha256"] = stable_hash(record)
    admission = AbsoluteEconomicAdmission.evaluate(
        record,
        expected_pair_id=record["pair_id"],
        expected_program_id=record["program_id"],
        expected_control_program_id=record["control_program_id"],
    )
    uplift = conditional_uplift_credit(record, admission)
    productive = _validation_productive(admission, uplift)
    result = {
        "schema_version": "cn_program_optimizer_d1_validation_result_v1",
        "status": "D1_VALIDATION_RESULT_CLOSED_IMMUTABLE",
        "validation_record_ordinal": int(ordinal),
        "exact_identity": exact,
        "candidate_id": str(member["candidate_id"]),
        "source_cohort": str(member["source_cohort"]),
        "source_wave": int(member["source_wave"]),
        "template_id": str(member["template_id"]),
        "selection_kind": str(member["selection_kind"]),
        "development_productive": True,
        "development_matched_cumulative_net_return_increment": float(member["development_matched_cumulative_net_return_increment"]),
        "development_matched_net_reward_increment": float(member["development_matched_net_reward_increment"]),
        "pair_record_sha256": record["record_payload_sha256"],
        "admission": admission.to_record(),
        "uplift": None if uplift is None else uplift.to_record(),
        "validation_productive": productive,
        "validation_reads": int(_PROCESS_CONTEXT["validation_reads"]),
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "promotion_authorized": False,
    }
    result["result_payload_sha256"] = stable_hash(result)
    _write_json(_PROCESS_RECORD_ROOT / f"candidate_{ordinal:03d}_{exact[:12]}.json", {"pair_record": record, "validation_result": result})
    gc.collect()
    return result


def _initialize_worker(
    source_contract: str,
    validation_field_root: str,
    validation_label_root: str,
    validation_session_authority_root: str,
    registry_path: str,
    input_hash: str,
    record_root: str,
    members: Sequence[Mapping[str, Any]],
) -> None:
    global _PROCESS_CONTEXT, _PROCESS_REGISTRY, _PROCESS_INPUT_HASH, _PROCESS_RECORD_ROOT, _PROCESS_MEMBER_BY_EXACT
    _PROCESS_CONTEXT = oos._load_validation_context(
        source_contract_path=Path(source_contract),
        validation_field_root=Path(validation_field_root),
        validation_label_root=Path(validation_label_root),
        validation_session_authority_root=Path(validation_session_authority_root),
    )
    _PROCESS_REGISTRY = UnifiedCapabilityRegistry.read(Path(registry_path))
    _PROCESS_INPUT_HASH = str(input_hash)
    _PROCESS_RECORD_ROOT = Path(record_root)
    _PROCESS_MEMBER_BY_EXACT = {str(row["exact_identity"]): dict(row) for row in members}


def _validation_productive(
    admission: AbsoluteEconomicAdmission,
    uplift: Any,
) -> bool:
    return bool(
        admission.admitted
        and uplift is not None
        and float(uplift.program_credit["matched_cumulative_net_return_increment"]) > 0.0
        and float(uplift.program_credit["matched_net_reward_increment"]) > 0.0
    )


def _metric(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    admitted = sum(bool(dict(row["admission"])["admitted"]) for row in rows)
    productive = sum(bool(row["validation_productive"]) for row in rows)
    return {
        "evaluated": n,
        "admitted": admitted,
        "productive": productive,
        "admission_rate": admitted / n if n else 0.0,
        "productive_transfer_rate": productive / n if n else 0.0,
        "productive_wilson_95": _wilson(productive, n),
    }


def _grouped_metrics(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    values = sorted(set(str(row[field]) for row in rows))
    return {value: _metric([row for row in rows if str(row[field]) == value]) for value in values}


def _verify_admitted_output_root(output_root: Path) -> Path:
    root = Path(output_root).resolve()
    if not root.is_dir() or not (root / ".project_control_execution").is_dir():
        raise RuntimeError("D1_VALIDATION_ADMITTED_OUTPUT_ROOT_MISSING")
    unexpected = {
        path.name
        for path in root.iterdir()
        if path.name != ".project_control_execution"
    }
    if unexpected:
        raise RuntimeError("D1_VALIDATION_ADMITTED_OUTPUT_ROOT_NOT_CLEAN")
    return root


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo_root = args.repo_root.resolve()
    prepared = _read_json(args.prepared_binding.resolve())
    _verify_self_hash(prepared, "prepared_binding_payload_sha256", "D1 validation prepared binding")
    if str(prepared.get("status")) != "D1_VALIDATION_PREFINANCIAL_READY":
        raise RuntimeError("D1 validation prepared binding is not ready")
    freeze, members = _load_frozen_members(repo_root)
    if (
        str(prepared["candidate_freeze_payload_sha256"]) != EXPECTED_FREEZE_PAYLOAD_SHA256
        or str(prepared["candidate_exact_identities_sha256"]) != EXPECTED_CANDIDATE_EXACT_SHA256
        or int(prepared["candidate_count"]) != EXPECTED_CANDIDATE_COUNT
    ):
        raise RuntimeError("D1 validation prepared/frozen candidate drift")
    schedules = _read_jsonl(Path(str(prepared["resolved_schedule_path"])))
    if _sha256(Path(str(prepared["resolved_schedule_path"]))) != str(prepared["resolved_schedule_file_sha256"]):
        raise RuntimeError("D1 validation resolved schedule file drift")
    if len(schedules) != EXPECTED_CANDIDATE_COUNT:
        raise RuntimeError("D1 validation schedule count drift")
    exacts = [str(row.get("d1_exact_identity") or row.get("successor_exact_identity") or "") for row in schedules]
    if len(set(exacts)) != EXPECTED_CANDIDATE_COUNT or stable_hash(sorted(exacts)) != EXPECTED_CANDIDATE_EXACT_SHA256:
        raise RuntimeError("D1 validation schedule exact set drift")
    member_by_exact = {str(row["exact_identity"]): dict(row) for row in members}
    for schedule, exact in zip(schedules, exacts, strict=True):
        member = member_by_exact[exact]
        body = {
            key: value
            for key, value in schedule.items()
            if key != "schedule_record_sha256"
        }
        if stable_hash(body) != str(schedule.get("schedule_record_sha256") or ""):
            raise RuntimeError("D1 validation schedule self-hash drift")
        if (
            str(schedule["schedule_record_sha256"]) != str(member["schedule_record_sha256"])
            or str(schedule["pair_id"]) != str(member["pair_id"])
            or str(schedule["primary_program"]["program_id"]) != str(member["program_id"])
            or str(schedule["control_program"]["program_id"]) != str(member["control_program_id"])
        ):
            raise RuntimeError("D1 validation schedule/member economic identity drift")

    output_root = _verify_admitted_output_root(args.output_root)
    record_root = output_root / "records"
    record_root.mkdir()
    input_binding = {
        "schema_version": "cn_program_optimizer_d1_validation_input_binding_v1",
        "candidate_freeze_payload_sha256": EXPECTED_FREEZE_PAYLOAD_SHA256,
        "candidate_exact_identities_sha256": EXPECTED_CANDIDATE_EXACT_SHA256,
        "prepared_binding_payload_sha256": prepared["prepared_binding_payload_sha256"],
        "prepared_binding_file_sha256": _sha256(args.prepared_binding.resolve()),
        "source_contract_path": str(args.source_contract.resolve()),
        "source_contract_sha256": _sha256(args.source_contract.resolve()),
        "validation_field_manifest_sha256": str(prepared["validation_field_manifest_sha256"]),
        "validation_session_authority_manifest_sha256": str(prepared["validation_session_authority_manifest_sha256"]),
        "validation_windows": list(VALIDATION_WINDOWS),
        "evaluation_role": "validation",
        "data_role": "validation_report_only",
        "usage": "REPORT_ONLY_CANDIDATE_TRANSFER",
        "optimizer_feedback_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "holdout_reads": 0,
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
            str(Path(str(prepared["validation_field_root"]))),
            str(Path(str(prepared["validation_label_root"]))),
            str(Path(str(prepared["validation_session_authority_root"]))),
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
    if len(results) != EXPECTED_CANDIDATE_COUNT:
        raise RuntimeError("D1 validation result coverage drift")

    metrics = {
        "total": _metric(results),
        "per_source_cohort": _grouped_metrics(results, "source_cohort"),
        "per_template": _grouped_metrics(results, "template_id"),
        "per_selector": _grouped_metrics(results, "selection_kind"),
    }
    _write_json(output_root / "validation_metrics.json", metrics)
    closure = {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "candidate_count": EXPECTED_CANDIDATE_COUNT,
        "candidate_exact_identities_sha256": EXPECTED_CANDIDATE_EXACT_SHA256,
        "input_binding_sha256": input_binding["input_binding_sha256"],
        "metrics": metrics,
        "validation_windows": list(VALIDATION_WINDOWS),
        "wall_seconds": float(time.perf_counter() - started),
        "validation_reads_per_worker_context": int(results[0]["validation_reads"]) if results else 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion_authorized": False,
        "oos_authority": "VALIDATION_REPORT_ONLY_EVIDENCE_ONLY",
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


def main(argv: Sequence[str] | None = None) -> int:
    result = run(parser().parse_args(argv))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
