"""Rebuild exact all-shard Phase3CM rewards from completed atom chunks."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import _write_csv, _write_json
from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    _candidate_summary_from_reward_atoms,
)
from our_system_phase2.services.candidate_schema import safe_float
from our_system_phase2.services.expression_semantics import analyze_expression


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _shard_indices(summary: dict[str, Any]) -> set[int]:
    raw = summary.get("selected_shard_indices") or summary.get("parallel_shard_indices") or []
    if isinstance(raw, str):
        raw = [item.strip() for item in raw.split(",") if item.strip()]
    return {int(value) for value in raw}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-table", type=Path, required=True)
    parser.add_argument("--chunk-dir", type=Path, action="append", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--expected-shard-count", type=int, default=12)
    parser.add_argument("--horizons", default="1,5,10,15")
    parser.add_argument("--rank-ic-loss-weight", type=float, default=6.0)
    parser.add_argument("--rank-ic-component-cap", type=float, default=0.35)
    parser.add_argument("--regime-stability-weight", type=float, default=0.06)
    parser.add_argument("--regime-component-cap", type=float, default=0.08)
    args = parser.parse_args(argv)

    candidates = _read_csv(args.candidate_table.resolve())
    if not candidates:
        raise RuntimeError("candidate table is empty")
    expected_shards = set(range(max(1, int(args.expected_shard_count))))
    atom_rows_by_hash: dict[str, list[dict[str, Any]]] = {}
    coverage_by_hash: dict[str, set[int]] = {}
    chunk_rows: list[dict[str, Any]] = []

    for raw_dir in args.chunk_dir:
        chunk_dir = raw_dir.resolve()
        summary_path = chunk_dir / "phase3cm_train_reward_audit_summary.json"
        reward_path = chunk_dir / "phase3cm_train_reward.csv"
        atom_path = chunk_dir / "phase3cm_reward_atoms.csv"
        summary = _read_json(summary_path)
        shard_indices = _shard_indices(summary)
        reward_rows = _read_csv(reward_path)
        atom_rows = _read_csv(atom_path)
        if not shard_indices or not reward_rows or not atom_rows:
            raise RuntimeError(
                f"incomplete atom chunk {chunk_dir}: shards={sorted(shard_indices)} "
                f"rewards={len(reward_rows)} atoms={len(atom_rows)}"
            )
        for row in reward_rows:
            digest = str(row.get("expression_hash") or "")
            if digest:
                coverage_by_hash.setdefault(digest, set()).update(shard_indices)
        for row in atom_rows:
            digest = str(row.get("expression_hash") or "")
            if digest:
                atom_rows_by_hash.setdefault(digest, []).append(row)
        chunk_rows.append(
            {
                "chunk_dir": str(chunk_dir),
                "selected_shard_indices": ",".join(str(value) for value in sorted(shard_indices)),
                "reward_row_count": len(reward_rows),
                "reward_atom_count": len(atom_rows),
            }
        )

    incomplete: list[dict[str, Any]] = []
    for candidate in candidates:
        digest = str(candidate.get("expression_hash") or "")
        coverage = coverage_by_hash.get(digest, set())
        missing = sorted(expected_shards - coverage)
        extra = sorted(coverage - expected_shards)
        if missing or extra:
            incomplete.append(
                {
                    "candidate_id": candidate.get("candidate_id"),
                    "expression_hash": digest,
                    "observed_shards": ",".join(str(value) for value in sorted(coverage)),
                    "missing_shards": ",".join(str(value) for value in missing),
                    "extra_shards": ",".join(str(value) for value in extra),
                }
            )
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "phase3cm_exact_atom_coverage.csv", incomplete)
    if incomplete:
        raise RuntimeError(f"exact reward atom coverage failed for {len(incomplete)} candidates")

    horizons = tuple(int(item.strip()) for item in str(args.horizons).split(",") if item.strip())
    valid_reward_rows: list[dict[str, Any]] = []
    semantic_blocked_rows: list[dict[str, Any]] = []
    split_rows: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates, 1):
        digest = str(candidate.get("expression_hash") or "")
        per_split, reward_row = _candidate_summary_from_reward_atoms(
            candidate,
            atom_rows_by_hash.get(digest, []),
            horizons,
            seed=20260623 + index,
            rank_ic_loss_weight=args.rank_ic_loss_weight,
            rank_ic_component_cap=args.rank_ic_component_cap,
            regime_stability_weight=args.regime_stability_weight,
            regime_component_cap=args.regime_component_cap,
        )
        semantic = analyze_expression(str(candidate.get("expression") or ""))
        reward_row.update(semantic.to_row())
        if semantic.hard_blocked:
            blocked_row = dict(reward_row)
            blocked_row["raw_train_reward_before_semantic_gate"] = reward_row.get("train_reward")
            blocked_row["raw_optimizer_reward_before_semantic_gate"] = reward_row.get("optimizer_reward")
            blocked_row["train_reward"] = -2.5
            blocked_row["optimizer_reward"] = -2.5
            blocked_row["train_reward_decision"] = "REJECT_SEMANTIC_DEGENERACY"
            blocked_row["train_reward_blockers"] = "semantic_degeneracy:" + "|".join(semantic.issue_codes)
            semantic_blocked_rows.append(blocked_row)
            continue
        split_rows.extend(
            {
                "candidate_id": candidate.get("candidate_id"),
                "expression_hash": digest,
                "generator_arm": candidate.get("generator_arm"),
                "factor_lane": candidate.get("factor_lane"),
                "expression": candidate.get("expression"),
                **row,
            }
            for row in per_split
        )
        valid_reward_rows.append(reward_row)

    valid_reward_rows.sort(key=lambda row: safe_float(row.get("train_reward"), -999.0), reverse=True)
    reward_rows: list[dict[str, Any]] = []
    semantic_equivalent_rows: list[dict[str, Any]] = []
    semantic_owner: dict[str, dict[str, Any]] = {}
    for row in valid_reward_rows:
        semantic_key = str(row.get("semantic_key") or row.get("expression_hash") or "")
        owner = semantic_owner.get(semantic_key)
        if owner is not None:
            duplicate = dict(row)
            duplicate["semantic_equivalent_to_candidate_id"] = owner.get("candidate_id")
            duplicate["semantic_equivalent_to_expression_hash"] = owner.get("expression_hash")
            semantic_equivalent_rows.append(duplicate)
            continue
        semantic_owner[semantic_key] = row
        reward_rows.append(row)
    kept_hashes = {str(row.get("expression_hash") or "") for row in reward_rows}
    split_rows = [row for row in split_rows if str(row.get("expression_hash") or "") in kept_hashes]
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3CM_EXACT_ALL_SHARD_REWARD_ATOM_RECOVERY_READY_DIAGNOSTIC_ONLY",
        "candidate_count": len(candidates),
        "semantically_valid_candidate_count": len(valid_reward_rows),
        "semantic_unique_reward_count": len(reward_rows),
        "semantic_equivalent_candidate_count": len(semantic_equivalent_rows),
        "semantic_blocked_candidate_count": len(semantic_blocked_rows),
        "expected_shard_count": len(expected_shards),
        "coverage_failure_count": 0,
        "chunk_count": len(chunk_rows),
        "reward_atom_count": sum(len(rows) for rows in atom_rows_by_hash.values()),
        "followup_count": sum(
            row.get("train_reward_decision") == "TRAIN_REWARD_FOLLOWUP_READY"
            for row in reward_rows
        ),
        "bootstrap_engine": "numpy_vectorized_v1",
        "reward_aggregation_mode": "reward_atoms_exact_all_shard_curve",
        "validation_usage": "report_only",
        "holdout_usage": "report_only",
        "chunks": chunk_rows,
        "top_candidates": [
            {
                "candidate_id": row.get("candidate_id"),
                "expression_hash": row.get("expression_hash"),
                "generator_arm": row.get("generator_arm"),
                "optimizer_reward": row.get("optimizer_reward"),
                "train_day_sortino": row.get("train_day_sortino"),
                "train_day_mcmc_prob_gt_0": row.get("train_day_mcmc_prob_gt_0"),
                "validation_day_sortino": row.get("validation_day_sortino"),
                "holdout_day_sortino": row.get("holdout_day_sortino"),
                "train_reward_decision": row.get("train_reward_decision"),
                "expression": row.get("expression"),
            }
            for row in reward_rows[:30]
        ],
    }
    _write_csv(output_root / "phase3cm_train_reward.csv", reward_rows)
    _write_csv(output_root / "phase3cm_semantic_blocked_reward_audit.csv", semantic_blocked_rows)
    _write_csv(output_root / "phase3cm_semantic_equivalent_reward_audit.csv", semantic_equivalent_rows)
    _write_csv(output_root / "phase3cm_candidate_split_horizon_summary.csv", split_rows)
    _write_json(output_root / "phase3cm_exact_reward_atom_recovery_summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
