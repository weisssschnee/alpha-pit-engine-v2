from __future__ import annotations

import csv
import json

import pandas as pd

from our_system_phase2.runtime import phase3cm_train_portfolio_sortino_reward_audit as phase3cm


def test_semantic_only_main_never_builds_future_return_labels(tmp_path, monkeypatch) -> None:
    shard_root = tmp_path / "shards"
    panel_dir = shard_root / "shard_00" / "phase3aq_wide_true1min" / "canary"
    panel_dir.mkdir(parents=True)
    panel = panel_dir / "phase3aq_true_1min_formula_canary.parquet"
    trade_times = pd.to_datetime(
        [
            "2026-01-05 09:30:00",
            "2026-01-05 09:31:00",
        ]
    )
    pd.DataFrame(
        {
            "code": ["A", "B", "C", "A", "B", "C"],
            "trade_time": [trade_times[0]] * 3 + [trade_times[1]] * 3,
            "date": pd.to_datetime(["2026-01-05"] * 6),
            "close": [10.0, 20.0, 30.0, 10.1, 19.9, 30.2],
            "x": [1.0, 3.0, 2.0, 2.0, 1.0, 3.0],
        }
    ).to_parquet(panel, index=False)
    candidate_table = tmp_path / "candidates.csv"
    with candidate_table.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["candidate_id", "expression_hash", "expression", "generator_arm"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "candidate_id": "candidate-a",
                "expression_hash": "hash-a",
                "expression": "CSRank($x)",
                "generator_arm": "typed_ast_fresh",
            }
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
            "0.5",
            "--validation-fraction",
            "0.25",
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
