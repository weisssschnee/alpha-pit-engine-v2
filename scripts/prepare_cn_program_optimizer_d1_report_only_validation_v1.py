"""Prepare the frozen D1 Program cohort for report-only validation.

This script resolves immutable development schedules, builds a Program-complete
validation session sidecar, rebuilds the validation session authority against
that exact field manifest, and proves the validation replay context loads.  It
never evaluates a candidate Program and never writes optimizer feedback.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_portfolio_decoder_v2_oos as oos
from our_system_phase2.services.unified_capability_registry import stable_hash


FREEZE_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_d1_validation_candidate_freeze_20260816.json"
)
MEMBERS_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_d1_validation_candidate_members_20260816.jsonl"
)
EXPECTED_CANDIDATE_COUNT = 120
EXPECTED_CANDIDATE_EXACT_SHA256 = (
    "ec419271b7bc2a3fb5d039c6e1441c489961601fbd788ccd84c2d80231cc3687"
)
EXPECTED_FREEZE_PAYLOAD_SHA256 = (
    "0ee0f43f4cc6a83b46faa000665958243ac826b7c8cb88a0b50c3e2e39bc8044"
)
EXPECTED_MEMBERS_FILE_SHA256 = (
    "4f0befede84762a81b1a458178c7e9a294821c3cb8a61f512e7a3bf990ef19da"
)
EXPECTED_SPLIT_SHA256 = (
    "fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241"
)
EXPECTED_PUBLIC_SOURCE_MANIFEST_SHA256 = (
    "0c06baa7c24109ea8a1064a291565ce6352ab2a0ca32f20f6cc257261e25514d"
)
EXPECTED_DAILY_ST_SOURCE_SHA256 = (
    "7060dd78cde6b826157f10c03541a4c302d11fe3213517236dc893393c071e68"
)
VALIDATION_WINDOWS = (
    {"window_id": "validation_1", "start_date": "2025-07-08", "end_date": "2025-08-08", "session_count": 24},
    {"window_id": "validation_2", "start_date": "2025-08-11", "end_date": "2025-09-11", "session_count": 24},
    {"window_id": "validation_3", "start_date": "2025-09-12", "end_date": "2025-10-24", "session_count": 25},
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )
    return path


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")


def _load_frozen_members(repo_root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    freeze_path = (repo_root / FREEZE_RELATIVE_PATH).resolve()
    members_path = (repo_root / MEMBERS_RELATIVE_PATH).resolve()
    freeze = _read_json(freeze_path)
    _verify_self_hash(freeze, "freeze_payload_sha256", "D1 validation candidate freeze")
    if str(freeze["freeze_payload_sha256"]) != EXPECTED_FREEZE_PAYLOAD_SHA256:
        raise RuntimeError("D1 validation candidate freeze payload drift")
    if _sha256(members_path) != EXPECTED_MEMBERS_FILE_SHA256:
        raise RuntimeError("D1 validation member file drift")
    members = _read_jsonl(members_path)
    ids = sorted(str(row["exact_identity"]) for row in members)
    if (
        len(members) != EXPECTED_CANDIDATE_COUNT
        or len(set(ids)) != EXPECTED_CANDIDATE_COUNT
        or stable_hash(ids) != EXPECTED_CANDIDATE_EXACT_SHA256
        or int(freeze["frozen_candidate_count"]) != EXPECTED_CANDIDATE_COUNT
        or str(freeze["frozen_candidate_exact_identities_sha256"])
        != EXPECTED_CANDIDATE_EXACT_SHA256
    ):
        raise RuntimeError("D1 validation candidate cardinality/identity drift")
    return freeze, members


def _schedule_path(source_root: Path, wave: int) -> Path:
    return source_root / f"wave_{wave:03d}" / "physical_schedules.jsonl"


def _resolve_schedules(members: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    cached: dict[tuple[str, int], list[dict[str, Any]]] = {}
    resolved: list[dict[str, Any]] = []
    for member in members:
        root = Path(str(member["source_root"])).resolve()
        wave = int(member["source_wave"])
        manifest_path = root / f"wave_{wave:03d}" / "wave_manifest.json"
        if _sha256(manifest_path) != str(member["source_wave_manifest_sha256"]):
            raise RuntimeError("D1 validation source wave manifest drift")
        key = (str(root), wave)
        if key not in cached:
            cached[key] = _read_jsonl(_schedule_path(root, wave))
        exact = str(member["exact_identity"])
        matches = [
            row
            for row in cached[key]
            if str(row.get("d1_exact_identity") or row.get("successor_exact_identity") or "")
            == exact
            and str(row.get("schedule_record_sha256") or "")
            == str(member["schedule_record_sha256"])
        ]
        if len(matches) != 1:
            raise RuntimeError(f"D1 validation frozen schedule cardinality drift: {exact}")
        schedule = dict(matches[0])
        body = {key: value for key, value in schedule.items() if key != "schedule_record_sha256"}
        if stable_hash(body) != str(schedule["schedule_record_sha256"]):
            raise RuntimeError(f"D1 validation source schedule self-hash drift: {exact}")
        resolved.append(schedule)
    if len(resolved) != EXPECTED_CANDIDATE_COUNT:
        raise RuntimeError("D1 validation resolved schedule count drift")
    return resolved


def _required_fields(schedules: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    required: set[str] = set()
    for row in schedules:
        for key in ("primary_compiled", "control_compiled"):
            required.update(map(str, dict(row[key]).get("physical_leaf_ids") or ()))
    return tuple(sorted(required))


def _write_requirements_table(path: Path, fields: Sequence[str]) -> Path:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["candidate_id", "expression"])
        writer.writeheader()
        for field in fields:
            writer.writerow({"candidate_id": f"required::{field}", "expression": f"${field}"})
    return path


def _run(command: Sequence[str]) -> None:
    subprocess.run(list(command), check=True)


def prepare(args: argparse.Namespace) -> dict[str, Any]:
    repo_root = args.repo_root.resolve()
    output_root = args.output_root.resolve()
    if output_root.exists():
        raise FileExistsError(output_root)
    output_root.mkdir(parents=True)
    freeze, members = _load_frozen_members(repo_root)
    schedules = _resolve_schedules(members)
    required = _required_fields(schedules)
    if len(required) != 48:
        raise RuntimeError(f"D1 validation required field count drift: {len(required)}")
    schedule_path = _write_jsonl(output_root / "resolved_program_schedules.jsonl", schedules)
    requirements_path = _write_requirements_table(
        output_root / "validation_program_required_fields.csv", required
    )
    selection_binding = {
        "schema_version": "cn_program_optimizer_d1_validation_selection_binding_v1",
        "status": "FROZEN_CANDIDATE_SELECTION_REPLAYED_BEFORE_VALIDATION_RESULT",
        "candidate_freeze_payload_sha256": freeze["freeze_payload_sha256"],
        "candidate_exact_identities_sha256": EXPECTED_CANDIDATE_EXACT_SHA256,
        "candidate_count": EXPECTED_CANDIDATE_COUNT,
        "resolved_schedule_count": len(schedules),
        "resolved_schedule_file_sha256": _sha256(schedule_path),
        "required_physical_leaf_count": len(required),
        "required_physical_leaf_ids": list(required),
        "requirements_table_file_sha256": _sha256(requirements_path),
        "validation_windows": list(VALIDATION_WINDOWS),
        "validation_reads": 0,
        "candidate_evaluation_executed": False,
    }
    selection_binding["binding_payload_sha256"] = stable_hash(selection_binding)
    _write_json(output_root / "selection_binding.json", selection_binding)

    field_root = output_root / "program_validation_session_fields"
    _run(
        [
            sys.executable,
            str(repo_root / "scripts/build_cn_core_pack_validation_session_sidecar.py"),
            "--source-root", str(args.minute_source_root.resolve()),
            "--evaluation-role", "validation",
            "--output-root", str(field_root),
            "--candidate-table", str(requirements_path),
            "--registry", str(args.registry.resolve()),
            "--split-manifest", str(args.split_manifest.resolve()),
            "--split-manifest-hash", EXPECTED_SPLIT_SHA256,
            "--fundamental-root", str(args.fundamental_root.resolve()),
            "--chip-root", str(args.chip_root.resolve()),
            "--max-shards", "16",
        ]
    )
    field_manifest_path = field_root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    field_manifest_sha = _sha256(field_manifest_path)

    session_authority_root = output_root / "validation_session_authority"
    _run(
        [
            sys.executable,
            str(repo_root / "scripts/build_cn_validation_session_authority.py"),
            "--field-manifest", str(field_manifest_path),
            "--public-source-root", str(args.public_source_root.resolve()),
            "--output-root", str(session_authority_root),
            "--expected-field-manifest-sha256", field_manifest_sha,
            "--expected-source-manifest-sha256", EXPECTED_PUBLIC_SOURCE_MANIFEST_SHA256,
            "--historical-daily-st-source", str(args.daily_st_source.resolve()),
            "--expected-daily-st-source-sha256", EXPECTED_DAILY_ST_SOURCE_SHA256,
            "--builder-commit-sha", str(args.repo_sha),
            "--evaluation-role", "validation",
        ]
    )
    authority_audit_root = output_root / "validation_session_authority_audit"
    _run(
        [
            sys.executable,
            str(repo_root / "scripts/verify_cn_validation_session_authority.py"),
            "--authority-root", str(session_authority_root),
            "--output-root", str(authority_audit_root),
        ]
    )
    authority_audit = _read_json(authority_audit_root / "audit.json")
    if str(authority_audit.get("status")) != "PASS_INDEPENDENT_VALIDATION_SESSION_AUTHORITY_VERIFICATION":
        raise RuntimeError("D1 validation session authority independent audit failed")

    context = oos._load_validation_context(
        source_contract_path=args.source_contract.resolve(),
        validation_field_root=field_root,
        validation_label_root=args.validation_label_root.resolve(),
        validation_session_authority_root=session_authority_root,
    )
    missing = sorted(set(required) - set(context["field_frame"].columns))
    if missing:
        raise RuntimeError(f"D1 validation prepared field context misses leaves: {missing}")
    if len(set(context["dates"])) <= 0:
        raise RuntimeError("D1 validation prepared context has no dates")

    prepared = {
        "schema_version": "cn_program_optimizer_d1_validation_prepared_binding_v1",
        "status": "D1_VALIDATION_PREFINANCIAL_READY",
        "repo_sha": str(args.repo_sha),
        "candidate_freeze_payload_sha256": freeze["freeze_payload_sha256"],
        "candidate_exact_identities_sha256": EXPECTED_CANDIDATE_EXACT_SHA256,
        "candidate_count": EXPECTED_CANDIDATE_COUNT,
        "resolved_schedule_path": str(schedule_path),
        "resolved_schedule_file_sha256": _sha256(schedule_path),
        "required_physical_leaf_count": len(required),
        "required_physical_leaf_ids": list(required),
        "validation_field_root": str(field_root),
        "validation_field_manifest_sha256": field_manifest_sha,
        "validation_label_root": str(args.validation_label_root.resolve()),
        "validation_session_authority_root": str(session_authority_root),
        "validation_session_authority_manifest_sha256": _sha256(
            session_authority_root / "validation_session_authority_manifest.json"
        ),
        "validation_session_authority_audit_sha256": _sha256(
            authority_audit_root / "audit.json"
        ),
        "validation_windows": list(VALIDATION_WINDOWS),
        "validation_reads_during_context_smoke": int(context["validation_reads"]),
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "candidate_evaluation_executed": False,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    prepared["prepared_binding_payload_sha256"] = stable_hash(prepared)
    path = _write_json(output_root / "D1_VALIDATION_PREFINANCIAL_READY.json", prepared)
    return {**prepared, "prepared_binding_file_sha256": _sha256(path)}


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", type=Path, required=True)
    p.add_argument("--repo-sha", required=True)
    p.add_argument("--output-root", type=Path, required=True)
    p.add_argument("--minute-source-root", type=Path, required=True)
    p.add_argument("--fundamental-root", type=Path, required=True)
    p.add_argument("--chip-root", type=Path, required=True)
    p.add_argument("--registry", type=Path, required=True)
    p.add_argument("--split-manifest", type=Path, required=True)
    p.add_argument("--public-source-root", type=Path, required=True)
    p.add_argument("--daily-st-source", type=Path, required=True)
    p.add_argument("--source-contract", type=Path, required=True)
    p.add_argument("--validation-label-root", type=Path, required=True)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    result = prepare(parser().parse_args(argv))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
