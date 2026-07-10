from __future__ import annotations

import csv
import json

from scripts.recover_phase3cm_exact_reward_atoms import main


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
    digest = "digest-a"
    equivalent_digest = "digest-equivalent"
    blocked_digest = "digest-blocked"
    candidate_table = tmp_path / "candidates.csv"
    _write_csv(
        candidate_table,
        [
            {
                "candidate_id": "candidate-a",
                "expression_hash": digest,
                "expression": "CSRank($x)",
                "generator_arm": "typed_ast_fresh",
                "open_direction": "long_top",
            },
            {
                "candidate_id": "candidate-equivalent",
                "expression_hash": equivalent_digest,
                "expression": "Abs(CSRank($x))",
                "generator_arm": "random_orthogonal",
                "open_direction": "long_top",
            },
            {
                "candidate_id": "candidate-blocked",
                "expression_hash": blocked_digest,
                "expression": "Sign(CSRank($x))",
                "generator_arm": "cem_exploit",
                "open_direction": "long_top",
            },
        ],
    )
    chunks = []
    for shard, value in ((0, 0.01), (1, -0.005)):
        chunk = tmp_path / f"chunk-{shard}"
        chunk.mkdir()
        (chunk / "phase3cm_train_reward_audit_summary.json").write_text(
            json.dumps({"selected_shard_indices": [shard]}), encoding="utf-8"
        )
        _write_csv(
            chunk / "phase3cm_train_reward.csv",
            [
                {"expression_hash": digest},
                {"expression_hash": equivalent_digest},
                {"expression_hash": blocked_digest},
            ],
        )
        _write_csv(
            chunk / "phase3cm_reward_atoms.csv",
            [
                *_atom_rows(digest, f"2026-01-0{shard + 5}", value),
                *_atom_rows(equivalent_digest, f"2026-01-0{shard + 5}", value),
                *_atom_rows(blocked_digest, f"2026-01-0{shard + 5}", value),
            ],
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
        ]
    )

    summary = json.loads(
        (output / "phase3cm_exact_reward_atom_recovery_summary.json").read_text(encoding="utf-8")
    )
    assert result == 0
    assert summary["coverage_failure_count"] == 0
    assert summary["candidate_count"] == 3
    assert summary["semantically_valid_candidate_count"] == 2
    assert summary["semantic_unique_reward_count"] == 1
    assert summary["semantic_equivalent_candidate_count"] == 1
    assert summary["semantic_blocked_candidate_count"] == 1
    assert summary["reward_aggregation_mode"] == "reward_atoms_exact_all_shard_curve"
