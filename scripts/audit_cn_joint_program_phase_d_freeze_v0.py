from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from our_system_phase2.runtime.cn_joint_program_phase_d_v0 import (
    CHECKPOINT_COUNT,
    DECISION_GATES,
    EXPECTED_RECORDS,
    FREEZE_CLOSURE_NAME,
    FREE_MEMORY_ENFORCEMENT,
    IMPROVED_TEMPLATES,
    RESOURCE_PROFILE,
    WEAK_TEMPLATES,
    build_ask_plan_v0,
    verify_prefinancial_freeze_v0,
)
from our_system_phase2.services.program_allocator_repair_bandit_v0 import (
    ProgramAllocatorRepairBanditV0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    expected = str(body.pop(field, ""))
    if expected != stable_hash(body):
        raise ValueError(f"{label} self-hash drift")


def audit(root: Path, expected_repo_sha: str) -> dict[str, Any]:
    root = root.resolve()
    closure = verify_prefinancial_freeze_v0(root)
    if str(closure["repo_sha"]) != expected_repo_sha:
        raise ValueError("Phase D freeze repo SHA drift")
    asks = _read_jsonl(root / "phase_c_ask_plan.jsonl")
    if asks != list(build_ask_plan_v0()) or len(asks) != EXPECTED_RECORDS:
        raise ValueError("Phase D ask-plan independent replay drift")
    arm_counts = Counter(str(row["generation_arm"]) for row in asks)
    if arm_counts != Counter(
        {"UNIFORM_FRESH": 304, "REVISED_EXPLOIT": 96, "NOVELTY_RESERVE": 112}
    ):
        raise ValueError("Phase D total arm quota drift")
    for template_id in IMPROVED_TEMPLATES:
        rows = [row for row in asks if row["template_id"] == template_id]
        if Counter(row["generation_arm"] for row in rows) != Counter(
            {"UNIFORM_FRESH": 24, "REVISED_EXPLOIT": 24, "NOVELTY_RESERVE": 16}
        ):
            raise ValueError(f"Phase D improved-template arm drift: {template_id}")
    for template_id in WEAK_TEMPLATES:
        rows = [row for row in asks if row["template_id"] == template_id]
        if Counter(row["generation_arm"] for row in rows) != Counter(
            {"UNIFORM_FRESH": 48, "NOVELTY_RESERVE": 16}
        ):
            raise ValueError(f"Phase D weak-template arm drift: {template_id}")
    for checkpoint in range(CHECKPOINT_COUNT):
        rows = asks[checkpoint * 8 : (checkpoint + 1) * 8]
        if len({row["template_id"] for row in rows}) != 1:
            raise ValueError("Phase D checkpoint crosses templates")

    seed_rows = _read_jsonl(root / "phase_b_feedback_seed.jsonl")
    for row in seed_rows:
        _verify_hash(row, "feedback_seed_record_sha256", "Phase D seed")
        if bool(row.get("phase_c_financial_result_used")):
            raise PermissionError("Phase C financial result entered Phase D seed")
    bandit = ProgramAllocatorRepairBanditV0.restore(
        _read_json(root / "initial_bandit_state.json")
    )
    if bandit.observations != 56:
        raise ValueError("Phase D seed must contain exactly 56 Phase B observations")
    contract = _read_json(root / "phase_c_run_contract.json")
    _verify_hash(contract, "run_contract_sha256", "Phase D contract")
    if (
        contract["decision_gates"] != DECISION_GATES
        or contract["free_memory_enforcement"] != FREE_MEMORY_ENFORCEMENT
        or contract["resource_profile"] != RESOURCE_PROFILE
        or int(contract["minimum_free_memory_bytes"]) != 1
        or bool(contract["allocator_canary_financial_records_imported"])
        or bool(contract["phase_c_financial_records_imported_into_phase_d"])
    ):
        raise ValueError("Phase D contract authority drift")
    canary_outcome = Path(str(contract["allocator_canary_outcome_path"]))
    if (
        not canary_outcome.is_file()
        or _sha256(canary_outcome)
        != str(contract["allocator_canary_outcome_file_sha256"])
    ):
        raise ValueError("Phase D canary evidence binding drift")
    access = _read_json(root / "access_ledger.json")
    _verify_hash(access, "access_ledger_sha256", "Phase D access")
    if bool(access["financial_evaluation_executed"]) or any(
        int(access[key])
        for key in (
            "market_price_rows_read",
            "label_rows_read",
            "validation_reads",
            "holdout_reads",
            "historical_2023_reads",
            "forward_b_reads",
            "forward_2026_reads",
        )
    ):
        raise PermissionError("Phase D freeze audit found prohibited reads")
    return {
        "schema_version": "cn_joint_program_phase_d_freeze_audit_v0",
        "status": "PASS",
        "freeze_root": str(root),
        "freeze_closure_file_sha256": _sha256(root / FREEZE_CLOSURE_NAME),
        "freeze_closure_payload_sha256": str(closure["closure_sha256"]),
        "repo_sha": expected_repo_sha,
        "record_count": len(asks),
        "checkpoint_count": CHECKPOINT_COUNT,
        "arm_counts": dict(sorted(arm_counts.items())),
        "improved_templates": list(IMPROVED_TEMPLATES),
        "weak_templates": list(WEAK_TEMPLATES),
        "phase_b_seed_observations": bandit.observations,
        "allocator_canary_financial_records_imported": False,
        "phase_c_financial_records_imported": False,
        "fixed_24_gib_hard_gate_applied": False,
        "financial_evaluation_executed": False,
        "sealed_reads": 0,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--freeze-root", required=True, type=Path)
    parser.add_argument("--audit-root", required=True, type=Path)
    parser.add_argument("--expected-repo-sha", required=True)
    args = parser.parse_args(argv)
    if len(args.expected_repo_sha) != 40:
        parser.error("expected-repo-sha must be a full Git SHA")
    audit_root = args.audit_root.resolve()
    if audit_root.exists():
        raise FileExistsError(f"Phase D audit root is not fresh: {audit_root}")
    audit_root.mkdir(parents=True)
    payload = audit(args.freeze_root, args.expected_repo_sha)
    payload["audit_payload_sha256"] = stable_hash(payload)
    output = audit_root / "audit.json"
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
