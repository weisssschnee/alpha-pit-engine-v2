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

from our_system_phase2.runtime.cn_joint_program_allocator_repair_canary_v0 import (
    ENHANCED_TEMPLATE_ORDER,
    EXPECTED_RECORDS,
    FREEZE_CLOSURE_NAME,
    ProgramAllocatorRepairBanditV0,
    build_ask_plan_v0,
    verify_prefinancial_freeze_v0,
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
        raise ValueError("allocator-repair freeze repo SHA drift")
    asks = _read_jsonl(root / "phase_c_ask_plan.jsonl")
    if asks != list(build_ask_plan_v0()) or len(asks) != EXPECTED_RECORDS:
        raise ValueError("allocator-repair ask-plan independent replay drift")
    arm_counts = Counter(str(row["generation_arm"]) for row in asks)
    if arm_counts != Counter(
        {"UNIFORM_FRESH": 116, "REVISED_EXPLOIT": 84, "NOVELTY_RESERVE": 56}
    ):
        raise ValueError("allocator-repair total arm quota drift")
    checkpoint_shapes = Counter()
    for checkpoint in range(EXPECTED_RECORDS // 8):
        rows = asks[checkpoint * 8 : (checkpoint + 1) * 8]
        template_id = str(rows[0]["template_id"])
        if any(str(row["template_id"]) != template_id for row in rows):
            raise ValueError("allocator-repair checkpoint crosses templates")
        shape = tuple(sorted(Counter(str(row["generation_arm"]) for row in rows).items()))
        checkpoint_shapes[(template_id, shape)] += 1
    for template_id in ENHANCED_TEMPLATE_ORDER:
        expected_shape = tuple(
            sorted(
                {
                    "UNIFORM_FRESH": 3,
                    "REVISED_EXPLOIT": 3,
                    "NOVELTY_RESERVE": 2,
                }.items()
            )
        )
        if checkpoint_shapes[(template_id, expected_shape)] != 4:
            raise ValueError("allocator-repair checkpoint arm-floor drift")

    seed_rows = _read_jsonl(root / "phase_b_feedback_seed.jsonl")
    for row in seed_rows:
        _verify_hash(row, "feedback_seed_record_sha256", "allocator-repair seed")
        if bool(row.get("phase_c_financial_result_used")):
            raise PermissionError("Phase C financial record entered the allocator seed")
        outcome = row.get("allocator_outcome")
        if row["reason"] == "REPLAY_BLOCKED_ZERO_CREDIT_RISK_OBSERVATION":
            if bool(row["positive_credit_applied"]) or set(outcome.values()) != {0.0}:
                raise ValueError("blocked Phase B row received allocator credit")
    bandit = ProgramAllocatorRepairBanditV0.restore(
        _read_json(root / "initial_bandit_state.json")
    )
    expected_observations = sum(
        bool(row["bandit_observation_applied"]) for row in seed_rows
    )
    if bandit.observations != expected_observations:
        raise ValueError("allocator-repair seed observation count drift")
    contract = _read_json(root / "phase_c_run_contract.json")
    _verify_hash(contract, "run_contract_sha256", "allocator-repair contract")
    parent_outcome = Path(str(contract["phase_c_parent_outcome_path"]))
    if (
        not parent_outcome.is_file()
        or _sha256(parent_outcome) != str(contract["phase_c_parent_outcome_file_sha256"])
        or bool(contract["phase_c_financial_records_imported"])
        or bool(contract["adaptive_budget_reallocation"])
    ):
        raise ValueError("allocator-repair parent or no-reuse binding drift")
    access = _read_json(root / "access_ledger.json")
    _verify_hash(access, "access_ledger_sha256", "allocator-repair access")
    prohibited_reads = sum(
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
    )
    if prohibited_reads or bool(access["financial_evaluation_executed"]):
        raise PermissionError("allocator-repair freeze audit found prohibited reads")
    return {
        "schema_version": "cn_joint_program_allocator_repair_freeze_audit_v0",
        "status": "PASS",
        "freeze_root": str(root),
        "freeze_closure_file_sha256": _sha256(root / FREEZE_CLOSURE_NAME),
        "freeze_closure_payload_sha256": str(closure["closure_sha256"]),
        "repo_sha": expected_repo_sha,
        "record_count": len(asks),
        "checkpoint_count": EXPECTED_RECORDS // 8,
        "arm_counts": dict(sorted(arm_counts.items())),
        "phase_b_seed_observations": bandit.observations,
        "phase_c_financial_records_imported": False,
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
    root = args.audit_root.resolve()
    if root.exists():
        raise FileExistsError(f"allocator-repair audit root is not fresh: {root}")
    root.mkdir(parents=True)
    payload = audit(args.freeze_root, args.expected_repo_sha)
    payload["audit_payload_sha256"] = stable_hash(payload)
    path = root / "audit.json"
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
