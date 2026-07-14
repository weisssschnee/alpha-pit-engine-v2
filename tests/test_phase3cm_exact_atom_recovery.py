from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.recover_phase3cm_exact_reward_atoms import main
from our_system_phase2.services.candidate_submission_receipt import (
    CandidateSubmissionAuthority,
    ReceiptContext,
    write_receipt_table,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry
from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator


REPO = Path(__file__).resolve().parents[1]
SPLIT = REPO / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
REGISTRY = REPO / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry/unified_capability_registry.json"
EVALUATOR = REPO / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
DATA_RELEASE_HASH = "cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827"


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _atom_rows(digest: str, date: str, value: float):
    rows = []
    for split in ("train", "validation", "holdout"):
        for horizon in ("all", "1"):
            rows.append(
                {
                    "expression_hash": digest,
                    "split": split,
                    "horizon_min": horizon,
                    "trade_date": date,
                    "curve_count": 2,
                    "net_return_sum": value,
                    "raw_return_sum": value,
                    "net_positive_count": int(value > 0),
                    "downside_square_sum": min(0.0, value) ** 2,
                    "daily_net_return": value,
                    "market_mean_return_sum": value / 2,
                    "market_mean_return_count": 1,
                    "turnover_sum": 0.2,
                    "turnover_count": 1,
                    "rank_ic_sum": 0.03,
                    "rank_ic_count": 1,
                    "rank_ic_positive_count": 1,
                }
            )
    return rows


def test_exact_atom_recovery_requires_and_accepts_complete_shard_coverage(tmp_path) -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    candidates = RegistryDrivenGenerator(registry).generate_route("MINUTE_STATIC", proposal_budget=2, seed=97)
    for index, candidate in enumerate(candidates):
        candidate["expression_hash"] = f"digest-{index}"
        candidate["generator_arm"] = "receipt_gated_test"
        candidate["open_direction"] = "long_top"
    candidate_table = tmp_path / "candidates.csv"
    _write_csv(candidate_table, candidates)
    split = FixedSplitAuthority.read(SPLIT)
    receipt_context = ReceiptContext.build(
        registry=registry,
        split_authority=split,
        data_release_hash=DATA_RELEASE_HASH,
        evaluator_paths=[EVALUATOR],
    )
    receipt_table = tmp_path / "receipts.jsonl"
    write_receipt_table(receipt_table, CandidateSubmissionAuthority(registry, receipt_context).authorize_table(candidates))
    chunks = []
    for shard, value in ((0, 0.01), (1, -0.005)):
        chunk = tmp_path / f"chunk-{shard}"
        chunk.mkdir()
        (chunk / "phase3cm_train_reward_audit_summary.json").write_text(
            json.dumps({"selected_shard_indices": [shard]}), encoding="utf-8"
        )
        _write_csv(
            chunk / "phase3cm_train_reward.csv",
            [{"expression_hash": candidate["expression_hash"]} for candidate in candidates],
        )
        _write_csv(
            chunk / "phase3cm_reward_atoms.csv",
            [row for candidate in candidates for row in _atom_rows(candidate["expression_hash"], f"2024-01-0{shard + 2}", value)],
        )
        chunks.append(chunk)
    output = tmp_path / "output"

    result = main(
        [
            "--candidate-table",
            str(candidate_table),
            "--chunk-dir",
            str(chunks[0]),
            "--chunk-dir",
            str(chunks[1]),
            "--output-root",
            str(output),
            "--expected-shard-count",
            "2",
            "--horizons",
            "1",
            "--train-fraction",
            "0.75",
            "--validation-fraction",
            "0.15",
            "--split-manifest",
            str(SPLIT),
            "--candidate-receipt-table",
            str(receipt_table),
            "--unified-registry",
            str(REGISTRY),
            "--data-release-hash",
            DATA_RELEASE_HASH,
        ]
    )

    summary = json.loads(
        (output / "phase3cm_exact_reward_atom_recovery_summary.json").read_text(encoding="utf-8")
    )
    assert result == 0
    assert summary["coverage_failure_count"] == 0
    assert summary["candidate_count"] == 2
    assert summary["semantically_valid_candidate_count"] == 2
    assert summary["semantic_unique_reward_count"] == 2
    assert summary["semantic_equivalent_candidate_count"] == 0
    assert summary["semantic_blocked_candidate_count"] == 0
    assert summary["reward_aggregation_mode"] == "reward_atoms_exact_all_shard_curve"
    assert summary["split_policy"] == "fixed_trade_date_manifest"
    assert summary["split_audit"]["post_normalization_cross_split_date_count"] == 0
    assert (output / "phase3cm_split_manifest.csv").exists()
    assert (output / "phase3cm_split_reassignment_audit.json").exists()
