from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

from our_system_phase2.services.phase3cm_streaming_resource_contract import FrozenExecutionPlan


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _load_plan(path: Path) -> FrozenExecutionPlan:
    return FrozenExecutionPlan.from_dict(json.loads(path.read_text(encoding="utf-8")))


def _phase_e(
    source: FrozenExecutionPlan,
    *,
    heavy_processes: int,
    pair_ids: list[str],
    pair_batch_size: int,
) -> FrozenExecutionPlan:
    if source.phase != "D":
        raise ValueError("Phase E freeze requires a completed Phase D execution plan")
    return FrozenExecutionPlan.create(
        phase="E",
        block_size=source.block_size,
        block_boundaries=source.block_boundaries,
        pair_batches=tuple(
            tuple(pair_ids[start : start + int(pair_batch_size)])
            for start in range(0, len(pair_ids), int(pair_batch_size))
        ),
        heavy_processes=int(heavy_processes),
        compute_threads=source.compute_threads,
        primary_thread_pool=source.primary_thread_pool,
        cache_caps=source.cache_caps,
        checkpoint_every_blocks=source.checkpoint_every_blocks,
        rss_soft_bytes=source.rss_soft_bytes,
        rss_hard_bytes=source.rss_hard_bytes,
        global_rss_hard_bytes=source.global_rss_hard_bytes,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--active-phase-d-plan", type=Path, required=True)
    parser.add_argument("--session-phase-d-plan", type=Path, required=True)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--heavy-processes", type=int, default=2)
    parser.add_argument("--pair-batch-size", type=int, default=4)
    args = parser.parse_args()

    if not re.fullmatch(r"[0-9a-f]{40}", str(args.repo_sha)):
        raise ValueError("repo SHA must be a full lowercase 40-character Git SHA")
    if int(args.heavy_processes) != 2:
        raise ValueError("the final two-backend qualification freezes exactly two heavy processes")
    active_source = _load_plan(args.active_phase_d_plan.resolve())
    session_source = _load_plan(args.session_phase_d_plan.resolve())
    if active_source.block_boundaries != session_source.block_boundaries:
        raise ValueError("active and session Phase D calendar blocks drift")
    if active_source.compute_threads != session_source.compute_threads:
        raise ValueError("active and session Phase D compute-thread counts drift")
    if int(args.pair_batch_size) <= 0:
        raise ValueError("pair batch size must be positive")
    if int(args.heavy_processes) * active_source.compute_threads > 24:
        raise ValueError("global native compute thread budget exceeds 24")

    binding_path = args.binding.resolve()
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    if str(binding.get("status")) != "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND":
        raise ValueError("frozen input binding status drift")
    if binding.get("sealed_reads") != {"validation": 0, "holdout": 0, "forward_2026": 0}:
        raise ValueError("frozen input binding sealed-read contract drift")

    bound_pairs = list(binding.get("pairs") or [])
    active_pair_ids = [
        str(row["pair_id"])
        for row in bound_pairs
        if str(row.get("clock_namespace")) == "active_bar"
    ]
    session_pair_ids = [
        str(row["pair_id"])
        for row in bound_pairs
        if str(row.get("clock_namespace")) == "stock_session"
    ]
    if not active_pair_ids or not session_pair_ids:
        raise ValueError("Phase E binding must contain both active and session pairs")
    if len(active_pair_ids) + len(session_pair_ids) != int(binding.get("pair_count") or -1):
        raise ValueError("Phase E binding pair counts drift")

    active = _phase_e(
        active_source,
        heavy_processes=int(args.heavy_processes),
        pair_ids=active_pair_ids,
        pair_batch_size=int(args.pair_batch_size),
    )
    session = _phase_e(
        session_source,
        heavy_processes=int(args.heavy_processes),
        pair_ids=session_pair_ids,
        pair_batch_size=int(args.pair_batch_size),
    )
    output_root = args.output_root.resolve()
    active_path = output_root / "active_bar" / "CN_FROZEN_EXECUTION_PLAN.json"
    session_path = output_root / "stock_session" / "CN_FROZEN_EXECUTION_PLAN.json"
    _write_json(active_path, active.to_dict())
    _write_json(session_path, session.to_dict())
    combined = {
        "schema_version": "cn_phase3cm_phase_e_combined_execution_contract_v1",
        "status": "CN_PHASE3CM_PHASE_E_EXECUTION_PLANS_FROZEN",
        "repo_sha": str(args.repo_sha),
        "input_binding_hash": str(binding["binding_hash"]),
        "input_binding_sha256": _sha256(binding_path),
        "heavy_processes": int(args.heavy_processes),
        "compute_threads_per_process": active.compute_threads,
        "global_active_native_compute_threads": int(args.heavy_processes)
        * active.compute_threads,
        "primary_thread_pool": active.primary_thread_pool,
        "thread_environment": active.thread_environment,
        "adaptation": "FORBIDDEN",
        "resource_gate_action": "FAIL_CLOSED_NO_PLAN_CHANGE",
        "plans": {
            "active_bar": {
                "path": str(active_path),
                "pair_count": len(active_pair_ids),
                "execution_plan_hash": active.execution_plan_hash,
                "source_phase_d_execution_plan_hash": active_source.execution_plan_hash,
                "source_phase_d_pair_count": sum(map(len, active_source.pair_batches)),
                "sha256": _sha256(active_path),
            },
            "stock_session": {
                "path": str(session_path),
                "pair_count": len(session_pair_ids),
                "execution_plan_hash": session.execution_plan_hash,
                "source_phase_d_execution_plan_hash": session_source.execution_plan_hash,
                "source_phase_d_pair_count": sum(map(len, session_source.pair_batches)),
                "sha256": _sha256(session_path),
            },
        },
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }
    combined_path = output_root / "CN_PHASE_E_COMBINED_EXECUTION_CONTRACT.json"
    _write_json(combined_path, combined)
    print(json.dumps(combined, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
