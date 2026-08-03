from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.freeze_cn_mark_to_market_finalists import (
    freeze_mark_to_market_finalists,
)
from scripts.freeze_cn_productive_keep_review_cohort import (
    _artifact,
    _payload_sha256,
    _sha256,
    _write_json,
)


def _fixture(tmp_path: Path) -> Path:
    root = tmp_path / "mtm"
    prepared = root / "prepared"
    prepared.mkdir(parents=True)
    pair_ids = ["pair-good", "pair-primary-loss", "pair-heavy", "pair-blocked"]
    pairs = pd.DataFrame(
        [
            {
                "pair_id": pair_id,
                "economic_mechanism_id": f"mechanism-{pair_id}",
                "portfolio_exposure_family_id": f"exposure-{pair_id}",
                "train_stability_score": 1.0 - index * 0.1,
                "search_score": 0.9 - index * 0.1,
            }
            for index, pair_id in enumerate(pair_ids)
        ]
    )
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": f"{pair_id}-{role.lower()}",
                "pair_id": pair_id,
                "pair_member_role": role,
            }
            for pair_id in pair_ids
            for role in ("PRIMARY", "CONTROL")
        ]
    )
    pair_input = prepared / "keep_review_pairs.parquet"
    candidate_input = prepared / "keep_review_candidates.parquet"
    pairs.to_parquet(pair_input, index=False)
    candidates.to_parquet(candidate_input, index=False)
    freeze = {
        "schema_version": "fixture",
        "status": "FROZEN_UNCHANGED_FIXED_COHORT",
        "selection_payload_sha256": "a" * 64,
        "pair_count": len(pairs),
        "candidate_member_count": len(candidates),
        "pair_ids": pair_ids,
        "candidate_ids": candidates["candidate_id"].tolist(),
        "pair_artifact": _artifact(pair_input, root=root),
        "candidate_artifact": _artifact(candidate_input, root=root),
    }
    freeze["manifest_body_sha256"] = _payload_sha256(freeze)
    freeze_path = _write_json(
        prepared / "finalist_replay_then_oos_freeze.json", freeze
    )

    mtm_pairs = pd.DataFrame(
        [
            {
                "pair_id": "pair-good",
                "pair_mark_to_market_status": "PAIR_MARK_TO_MARKET_COMPLETE",
                "primary_mark_to_market_net_reward": 0.3,
                "mark_to_market_net_increment": 0.2,
                "primary_cumulative_net_return": 0.08,
                "cumulative_net_return_increment": 0.03,
                "primary_ending_holdings_weight": 0.01,
                "control_ending_holdings_weight": 0.02,
            },
            {
                "pair_id": "pair-primary-loss",
                "pair_mark_to_market_status": "PAIR_MARK_TO_MARKET_COMPLETE",
                "primary_mark_to_market_net_reward": -0.1,
                "mark_to_market_net_increment": 0.3,
                "primary_cumulative_net_return": -0.02,
                "cumulative_net_return_increment": 0.04,
                "primary_ending_holdings_weight": 0.001,
                "control_ending_holdings_weight": 0.001,
            },
            {
                "pair_id": "pair-heavy",
                "pair_mark_to_market_status": "PAIR_MARK_TO_MARKET_COMPLETE",
                "primary_mark_to_market_net_reward": 0.4,
                "mark_to_market_net_increment": 0.3,
                "primary_cumulative_net_return": 0.12,
                "cumulative_net_return_increment": 0.05,
                "primary_ending_holdings_weight": 0.2,
                "control_ending_holdings_weight": 0.01,
            },
            {
                "pair_id": "pair-blocked",
                "pair_mark_to_market_status": "PAIR_MARK_TO_MARKET_BLOCKED",
                "primary_mark_to_market_net_reward": None,
                "mark_to_market_net_increment": None,
                "primary_cumulative_net_return": None,
                "cumulative_net_return_increment": None,
                "primary_ending_holdings_weight": None,
                "control_ending_holdings_weight": None,
            },
        ]
    )
    mtm_candidates = candidates.assign(
        candidate_mark_to_market_status="CANDIDATE_MARK_TO_MARKET_COMPLETE"
    )
    pair_result_path = root / "pair_mark_to_market_results.parquet"
    candidate_result_path = root / "candidate_mark_to_market_results.parquet"
    mtm_pairs.to_parquet(pair_result_path, index=False)
    mtm_candidates.to_parquet(candidate_result_path, index=False)
    closure_artifacts = []
    for path in (freeze_path, pair_result_path, candidate_result_path):
        closure_artifacts.append(
            {
                "path": str(path.resolve()),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    closure = {
        "schema_version": "fixture",
        "status": (
            "FINAL_CLOSE_MARK_TO_MARKET_REPLAY_CLOSED_IMMUTABLE_"
            "DIAGNOSTIC_ONLY"
        ),
        "no_fabricated_terminal_sale": True,
        "terminal_sale_fee_applied": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "artifacts": closure_artifacts,
    }
    closure["manifest_body_sha256"] = _payload_sha256(closure)
    _write_json(root / "MARK_TO_MARKET_REPLAY_COMPLETE.json", closure)
    return root


def test_mtm_freeze_requires_absolute_and_relative_economics_not_low_exposure(
    tmp_path: Path,
) -> None:
    root = _fixture(tmp_path)
    output = tmp_path / "finalists"
    result = freeze_mark_to_market_finalists(
        mark_to_market_root=root,
        output_root=output,
        generator_repo_sha="b" * 40,
    )
    assert result["selected_pair_ids"] == ["pair-heavy", "pair-good"]
    assert result["selected_pairs"] == 2
    summary = json.loads(
        (output / "finalist_summary.json").read_text(encoding="utf-8")
    )
    assert summary["strict_execution_ready"] is False
    assert summary["blocked_rows_backfilled"] == 0
    review = pd.read_parquet(output / "finalist_review_ledger.parquet")
    primary_loss = review.loc[review["pair_id"].eq("pair-primary-loss")].iloc[0]
    assert "PRIMARY_ABSOLUTE_NET_REWARD_NOT_POSITIVE" in (
        primary_loss["economic_admission_blockers"]
    )
    heavy = review.loc[review["pair_id"].eq("pair-heavy")].iloc[0]
    assert heavy["economic_admission_blockers"] == ""
