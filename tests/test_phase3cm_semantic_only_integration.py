from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd

from our_system_phase2.runtime import phase3cm_train_portfolio_sortino_reward_audit as phase3cm
from our_system_phase2.services.candidate_submission_receipt import (
    CandidateSubmissionAuthority,
    ReceiptContext,
    write_receipt_table,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry


REPO = Path(__file__).resolve().parents[1]
SPLIT = REPO / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
REGISTRY = REPO / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry/unified_capability_registry.json"
EVALUATOR = REPO / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
DATA_RELEASE_HASH = "cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827"


def test_semantic_only_main_never_builds_future_return_labels(tmp_path, monkeypatch) -> None:
    shard_root = tmp_path / "shards"
    panel_dir = shard_root / "shard_00" / "phase3aq_wide_true1min" / "canary"
    panel_dir.mkdir(parents=True)
    panel = panel_dir / "phase3aq_true_1min_formula_canary.parquet"
    trade_times = pd.to_datetime(
        [
            "2024-01-02 09:30:00",
            "2024-01-02 09:31:00",
        ]
    )
    pd.DataFrame(
        {
            "code": ["A", "B", "C", "A", "B", "C"],
            "trade_time": [trade_times[0]] * 3 + [trade_times[1]] * 3,
            "date": pd.to_datetime(["2024-01-02"] * 6),
            "close": [10.0, 20.0, 30.0, 10.1, 19.9, 30.2],
            "x": [1.0, 3.0, 2.0, 2.0, 1.0, 3.0],
        }
    ).to_parquet(panel, index=False)
    candidate = {
        "candidate_id": "candidate-a",
        "expression_hash": "hash-a",
        "expression": "CSRank($close)",
        "generator_arm": "typed_ast_fresh",
        "route_id": "MINUTE_STATIC",
        "operator_family": "CSRank",
        "matched_control_id": "candidate-a-control",
        "declared_field_ids": ["close"],
        "condition_field_ids": [],
        "is_matched_control": False,
        "vote_policy": "ONE_SUPPORT_UNIT_ONE_VOTE",
        "maturity_contract_registered": False,
        "exposure_ledger_required": True,
        "access_roles": ["development"],
        "uses_future_revision": False,
        "requires_intrabar_order": False,
        "proposal_origin": "test",
    }
    control = {
        **candidate,
        "candidate_id": "candidate-a-control",
        "expression": "CSRank(Sign($close))",
        "matched_control_id": "candidate-a",
        "is_matched_control": True,
        "vote_policy": "CONTROL_NO_SEPARATE_VOTE",
    }
    candidate_table = tmp_path / "candidates.csv"
    with candidate_table.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(candidate))
        writer.writeheader()
        writer.writerow(candidate)
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    split = FixedSplitAuthority.read(SPLIT)
    context = ReceiptContext.build(
        registry=registry,
        split_authority=split,
        data_release_hash=DATA_RELEASE_HASH,
        evaluator_paths=[EVALUATOR],
    )
    receipt_table = tmp_path / "receipts.jsonl"
    write_receipt_table(
        receipt_table,
        CandidateSubmissionAuthority(registry, context).authorize_table([candidate, control]),
    )

    def fail_if_labels_are_built(*_args, **_kwargs):
        raise AssertionError("semantic-only must not build future-return labels")

    monkeypatch.setattr(phase3cm, "_future_returns", fail_if_labels_are_built)
    output_root = tmp_path / "output"
    report_root = tmp_path / "report"

    result = phase3cm.main(
        [
            "--candidate-audit",
            str(candidate_table),
            "--shard-root",
            str(shard_root),
            "--output-root",
            str(output_root),
            "--report-root",
            str(report_root),
            "--candidate-limit",
            "1",
            "--max-shards",
            "1",
            "--sample-trade-times-per-shard",
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
            "--min-obs-per-time",
            "2",
            "--semantic-only",
            "--semantic-sketch-size",
            "16",
            "--checkpoint-every-candidates",
            "1",
        ]
    )

    assert result == 0
    with (output_root / "phase3cm_candidate_progress.csv").open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        progress = list(csv.DictReader(handle))
    summary = json.loads(
        (output_root / "phase3cm_train_reward_audit_summary.json").read_text(encoding="utf-8")
    )
    assert len(progress) == 1
    assert int(progress[0]["rows_added"]) == 0
    assert int(progress[0]["signal_finite_count"]) == 6
    assert progress[0]["signal_rank_sketch"]
    assert summary["semantic_only"] is True
    assert summary["optimizer_reward_metric"] is None
