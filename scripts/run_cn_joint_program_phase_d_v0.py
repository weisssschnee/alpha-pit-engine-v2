from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_joint_program_allocator_repair_canary_v0 as allocator
from our_system_phase2.runtime.cn_joint_program_phase_d_v0 import (
    BATCH_ID,
    CHECKPOINT_COUNT,
    DECISION_GATES,
    EXPECTED_RECORDS,
    FREE_MEMORY_ENFORCEMENT,
    FREEZE_CLOSURE_NAME,
    MIN_BASE_IDENTITIES_PER_TEMPLATE,
    MINIMUM_FREE_MEMORY_BYTES,
    RECORDS_PER_CHECKPOINT,
    RESOURCE_PROFILE,
    verify_prefinancial_freeze_v0,
)
from our_system_phase2.services.program_allocator_repair_bandit_v0 import (
    BANDIT_POLICY_ID,
    ProgramAllocatorRepairBanditV0,
)


STATUS = "CN_JOINT_PROGRAM_PHASE_D_COMPLETE"
CLOSURE_NAME = "CN_JOINT_PROGRAM_PHASE_D_COMPLETE.json"


def _verify_run_contract(
    contract: Mapping[str, Any], *, executor_workers: int
) -> None:
    expected = {
        "main_record_count": EXPECTED_RECORDS,
        "checkpoint_count": CHECKPOINT_COUNT,
        "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
        "executor_backend": "PROCESS_POOL",
        "executor_workers": executor_workers,
        "executor_lifecycle": "CHECKPOINT_SCOPED_RECYCLE",
        "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "free_memory_enforcement": FREE_MEMORY_ENFORCEMENT,
        "resource_profile": RESOURCE_PROFILE,
        "portfolio_decoder_id": "TOPK_10_EQUAL",
        "initial_uniform_baseline_per_enhanced_template": 24,
        "maximum_variants_per_base_per_template": 4,
        "minimum_base_identities_per_template": MIN_BASE_IDENTITIES_PER_TEMPLATE,
        "no_early_template_cancellation": True,
        "adaptive_budget_reallocation": False,
        "fixed_arm_floors": True,
        "bandit_policy_id": BANDIT_POLICY_ID,
        "blocked_positive_credit": False,
        "cross_campaign_optimizer_state_import": False,
        "route_local_tpe_authority_unchanged": True,
        "allocator_canary_financial_records_imported": False,
        "phase_c_financial_records_imported_into_phase_d": False,
        "decision_gates": DECISION_GATES,
    }
    drift = [key for key, value in expected.items() if contract.get(key) != value]
    if drift:
        raise RuntimeError("Phase D frozen run contract drift: " + ",".join(drift))


def _configure_engine() -> None:
    engine.EXPECTED_RECORDS = EXPECTED_RECORDS
    engine.CHECKPOINT_COUNT = CHECKPOINT_COUNT
    engine.RECORDS_PER_CHECKPOINT = RECORDS_PER_CHECKPOINT
    engine.PHASE_C_BATCH_ID = BATCH_ID
    engine.FREEZE_CLOSURE_NAME = FREEZE_CLOSURE_NAME
    engine.STATUS = STATUS
    engine.CLOSURE_NAME = CLOSURE_NAME
    engine.CATALOG_MIN_RECORDS_PER_TEMPLATE = 64
    engine.MINIMUM_FREE_MEMORY_BYTES = MINIMUM_FREE_MEMORY_BYTES
    engine.ProgramFactorizedBanditV0 = ProgramAllocatorRepairBanditV0
    engine.verify_phase_c_prefinancial_freeze_v0 = verify_prefinancial_freeze_v0
    engine._choose_entry = allocator._choose_entry
    engine._feedback_update = allocator._feedback_update
    engine._verify_run_contract = _verify_run_contract


def run(args: argparse.Namespace) -> dict[str, Any]:
    _configure_engine()
    base_args = argparse.Namespace(**vars(args))
    base_args.phase_c_freeze_root = args.freeze_root
    closure = engine.run(base_args)
    body = dict(closure)
    body.pop("closure_payload_sha256", None)
    body.update(
        {
            "schema_version": "cn_joint_program_phase_d_closure_v0",
            "status": STATUS,
            "experiment_profile": "PHASE_D_PROSPECTIVE_DEVELOPMENT_ONLY",
            "allocator_policy_id": BANDIT_POLICY_ID,
            "free_memory_enforcement": FREE_MEMORY_ENFORCEMENT,
            "fixed_24_gib_hard_gate_applied": False,
            "memory_telemetry_recorded": True,
            "allocator_canary_financial_records_reused": False,
            "phase_c_financial_records_reused": False,
            "adaptive_budget_reallocation": False,
            "automatic_next_phase_launch": False,
        }
    )
    closed = engine._self_hashed(body, "closure_payload_sha256")
    return engine._read_json(engine._write_json(args.output_root / CLOSURE_NAME, closed))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--freeze-root", required=True, type=Path)
    parser.add_argument("--execution-contract", required=True, type=Path)
    parser.add_argument("--train-field-root", required=True, type=Path)
    parser.add_argument("--train-price-root", required=True, type=Path)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--node-resource-capacity", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--builder-commit-sha", required=True)
    parser.add_argument("--root-finalization-recovery-from-repo-sha")
    parser.add_argument("--root-finalization-incident", type=Path)
    parser.add_argument("--root-finalization-deployment-manifest", type=Path)
    parser.add_argument("--checkpoint-recovery-from-repo-sha")
    parser.add_argument("--checkpoint-recovery-incident", type=Path)
    parser.add_argument("--checkpoint-recovery-diagnostic-audit", type=Path)
    parser.add_argument("--checkpoint-recovery-deployment-manifest", type=Path)
    parser.add_argument("--executor-workers", type=int, default=10)
    args = parser.parse_args(argv)
    if len(str(args.builder_commit_sha)) != 40:
        parser.error("builder-commit-sha must be a full Git SHA")
    closure = run(args)
    print(json.dumps(closure, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
