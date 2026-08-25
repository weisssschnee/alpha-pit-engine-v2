"""Resolve frozen Wave 1 validation members to immutable source schedules.

This step reads only the completed DEVELOPMENT_ONLY Production Wave 1 runtime.
It does not read validation/OOS data and does not evaluate candidates.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_SCHEDULES_RESOLVED_BEFORE_VALIDATION"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload); claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def resolve(*, freeze_path: Path, members_path: Path, source_root: Path, schedules_output: Path, resolver_repo_sha: str) -> dict[str, Any]:
    freeze = _read(freeze_path)
    freeze_hash = _verify(freeze, "freeze_payload_sha256", "Wave1 validation shortlist freeze")
    if (
        freeze.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_SHORTLIST_FROZEN_BEFORE_VALIDATION"
        or int(freeze.get("candidate_count") or 0) != 42
        or freeze.get("membership_frozen_before_validation") is not True
        or freeze.get("validation_read") is not False
        or freeze.get("optimizer_feedback_allowed") is not False
    ):
        raise RuntimeError("Wave1 validation shortlist authority drift")
    members = _read_jsonl(members_path)
    if len(members) != 42:
        raise RuntimeError("Wave1 validation member count drift")
    for row in members:
        body = dict(row); claimed = str(body.pop("member_payload_sha256", ""))
        if not claimed or stable_hash(body) != claimed:
            raise RuntimeError("Wave1 validation member self-hash drift")
    member_exacts = [str(row["exact_identity"]) for row in members]
    if member_exacts != list(map(str, freeze["candidate_exact_identities"])) or stable_hash(member_exacts) != str(freeze["candidate_exact_identities_sha256"]):
        raise RuntimeError("Wave1 validation member exact order drift")

    terminal_path = source_root / "CN_SEARCH_CORE_V2_PRODUCTION_WAVE1_COMPLETE.json"
    terminal = _read(terminal_path)
    terminal_hash = _verify(terminal, "closure_payload_sha256", "Wave1 source terminal")
    if (
        terminal.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_COMPLETE"
        or int(terminal.get("evaluated") or 0) != 336
        or terminal.get("evaluation_data_role") != "DEVELOPMENT_ONLY"
        or any(int(v) != 0 for v in dict(terminal.get("restricted_reads") or {}).values())
        or str(terminal.get("productive_archive_file_sha256") or "") != str(freeze["source_productive_archive_file_sha256"])
    ):
        raise RuntimeError("Wave1 validation source terminal drift")

    resolved: list[dict[str, Any]] = []
    required_fields: set[str] = set()
    checkpoint_schedule_file_sha256: dict[str, str] = {}
    checkpoint_result_file_sha256: dict[str, str] = {}
    for member in members:
        ordinal = int(member["source_checkpoint_ordinal"])
        checkpoint = source_root / f"checkpoint_{ordinal:04d}"
        schedule_path = checkpoint / "selected_schedule.jsonl"
        result_path = checkpoint / "candidate_results.jsonl"
        if not schedule_path.is_file() or not result_path.is_file():
            raise FileNotFoundError(checkpoint)
        checkpoint_schedule_file_sha256[str(ordinal)] = _sha(schedule_path)
        checkpoint_result_file_sha256[str(ordinal)] = _sha(result_path)
        exact = str(member["exact_identity"])
        schedules = [
            row for row in _read_jsonl(schedule_path)
            if str(row.get("search_core_exact_identity") or "") == exact
        ]
        results = [
            row for row in _read_jsonl(result_path)
            if str(row.get("exact_identity") or "") == exact
        ]
        if len(schedules) != 1 or len(results) != 1:
            raise RuntimeError(f"Wave1 validation source exact cardinality drift: {exact}")
        schedule = dict(schedules[0]); result = dict(results[0])
        schedule_body = {k: v for k, v in schedule.items() if k != "schedule_record_sha256"}
        if stable_hash(schedule_body) != str(schedule.get("schedule_record_sha256") or ""):
            raise RuntimeError(f"Wave1 validation source schedule self-hash drift: {exact}")
        result_body = {k: v for k, v in result.items() if k != "result_payload_sha256"}
        if stable_hash(result_body) != str(result.get("result_payload_sha256") or ""):
            raise RuntimeError(f"Wave1 validation source result self-hash drift: {exact}")
        if (
            str(schedule.get("successor_exact_identity") or "") != exact
            or str(schedule.get("template_id") or "") != str(member["template_id"])
            or int(schedule.get("checkpoint_ordinal", -1)) != ordinal
            or schedule.get("production_wave_id") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_V1"
            or schedule.get("search_core_stage") != "PRODUCTION_WAVE1"
            or str(result["result_payload_sha256"]) != str(member["source_result_payload_sha256"])
            or not bool(result.get("productive"))
            or not bool(result.get("stable"))
        ):
            raise RuntimeError(f"Wave1 validation source provenance drift: {exact}")
        for key in ("primary_compiled", "control_compiled"):
            required_fields.update(map(str, dict(schedule[key]).get("physical_leaf_ids") or ()))
        resolved.append(schedule)

    resolved_exacts = [str(row["search_core_exact_identity"]) for row in resolved]
    if resolved_exacts != member_exacts or len(set(resolved_exacts)) != 42:
        raise RuntimeError("Wave1 validation resolved schedule order/uniqueness drift")
    _write_jsonl(schedules_output, resolved)
    receipt = {
        "schema_version": "cn_search_core_v2_production_wave1_validation_schedule_resolution_v1",
        "status": STATUS,
        "resolver_repo_sha": str(resolver_repo_sha),
        "resolver_source_file_sha256": _sha(Path(__file__).resolve()),
        "source_run_id": source_root.name,
        "source_terminal_file_sha256": _sha(terminal_path),
        "source_terminal_payload_sha256": terminal_hash,
        "shortlist_freeze_file_sha256": _sha(freeze_path),
        "shortlist_freeze_payload_sha256": freeze_hash,
        "shortlist_members_file_sha256": _sha(members_path),
        "candidate_count": len(resolved),
        "candidate_exact_identities_sha256": stable_hash(resolved_exacts),
        "resolved_schedule_file_sha256": _sha(schedules_output),
        "source_checkpoint_schedule_file_sha256": dict(sorted(checkpoint_schedule_file_sha256.items(), key=lambda x: int(x[0]))),
        "source_checkpoint_result_file_sha256": dict(sorted(checkpoint_result_file_sha256.items(), key=lambda x: int(x[0]))),
        "required_physical_leaf_count": len(required_fields),
        "required_physical_leaf_ids": sorted(required_fields),
        "required_physical_leaf_ids_sha256": stable_hash(sorted(required_fields)),
        "validation_read": False,
        "holdout_read": False,
        "forward_read": False,
        "candidate_evaluation_executed": False,
        "optimizer_feedback_written": False,
        "policy_memory_written": False,
        "oos_authority": "NONE",
    }
    receipt["resolution_payload_sha256"] = stable_hash(receipt)
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shortlist-freeze", type=Path, required=True)
    parser.add_argument("--shortlist-members", type=Path, required=True)
    parser.add_argument("--source-output-root", type=Path, required=True)
    parser.add_argument("--schedules-output", type=Path, required=True)
    parser.add_argument("--receipt-output", type=Path, required=True)
    parser.add_argument("--resolver-repo-sha", required=True)
    args = parser.parse_args(argv)
    payload = resolve(
        freeze_path=args.shortlist_freeze.resolve(),
        members_path=args.shortlist_members.resolve(),
        source_root=args.source_output_root.resolve(),
        schedules_output=args.schedules_output.resolve(),
        resolver_repo_sha=args.resolver_repo_sha,
    )
    args.receipt_output.parent.mkdir(parents=True, exist_ok=True)
    args.receipt_output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status":payload["status"],"candidates":payload["candidate_count"],"fields":payload["required_physical_leaf_count"],"schedule_sha":payload["resolved_schedule_file_sha256"],"payload":payload["resolution_payload_sha256"]},sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())