from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.freeze_cn_decoder_v2_finalists import (
    DECODER_ID,
    freeze_decoder_v2_finalists,
)
from scripts.freeze_cn_productive_keep_review_cohort import (
    _artifact,
    _payload_sha256,
    _sha256,
    _write_json,
)
from scripts.verify_cn_decoder_v2_finalists import (
    verify_decoder_v2_finalists,
)


SELECTION = "a" * 64
DECODER_CONTRACT = "b" * 64


def _fixture(tmp_path: Path) -> tuple[Path, Path]:
    source_root = tmp_path / "source"
    prepared = source_root / "prepared"
    prepared.mkdir(parents=True)
    pair_rows = [
        {
            "pair_id": "pair-first",
            "primary_candidate_id": "first-primary",
            "control_candidate_id": "first-control",
            "economic_mechanism_id": "mechanism-a",
            "portfolio_exposure_family_id": "exposure-1",
            "structural_family_id": "structural",
            "signal_cluster_id": "signal",
            "operator_family": "operator",
            "skeleton_id": "skeleton",
            "source_field_ids": ["f1", "f2"],
            "operator_paths": ["p1"],
            "financial_hypothesis": "hypothesis",
            "route_id": "SLOW_TEMPORAL_CHANGE",
        },
        {
            "pair_id": "pair-duplicate",
            "primary_candidate_id": "duplicate-primary",
            "control_candidate_id": "duplicate-control",
            "economic_mechanism_id": "mechanism-a",
            "portfolio_exposure_family_id": "exposure-1",
        },
        {
            "pair_id": "pair-reward-fail",
            "primary_candidate_id": "reward-fail-primary",
            "control_candidate_id": "reward-fail-control",
            "economic_mechanism_id": "mechanism-b",
            "portfolio_exposure_family_id": "exposure-2",
        },
        {
            "pair_id": "pair-last",
            "primary_candidate_id": "last-primary",
            "control_candidate_id": "last-control",
            "economic_mechanism_id": "mechanism-c",
            "portfolio_exposure_family_id": "exposure-2",
        },
    ]
    for index in range(4, 32):
        pair_rows.append(
            {
                "pair_id": f"pair-ineligible-{index:02d}",
                "primary_candidate_id": f"ineligible-{index:02d}-primary",
                "control_candidate_id": f"ineligible-{index:02d}-control",
                "economic_mechanism_id": f"mechanism-{index:02d}",
                "portfolio_exposure_family_id": "exposure-other",
            }
        )
    for rank, row in enumerate(pair_rows, 1):
        row["keep_review_rank"] = rank
    pairs = pd.DataFrame(pair_rows)
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": row[f"{role.lower()}_candidate_id"],
                "pair_id": row["pair_id"],
                "pair_member_role": role.upper(),
            }
            for row in pair_rows
            for role in ("primary", "control")
        ]
    )
    pair_path = prepared / "finalist_pairs.parquet"
    candidate_path = prepared / "finalist_candidates.parquet"
    pairs.to_parquet(pair_path, index=False)
    candidates.to_parquet(candidate_path, index=False)
    freeze = {
        "schema_version": "fixture",
        "status": "FROZEN_UNCHANGED_FIXED_COHORT",
        "selection_payload_sha256": SELECTION,
        "pair_count": 32,
        "candidate_member_count": 64,
        "pair_ids": pairs["pair_id"].tolist(),
        "candidate_ids": candidates["candidate_id"].tolist(),
        "pair_artifact": _artifact(pair_path, root=source_root),
        "candidate_artifact": _artifact(candidate_path, root=source_root),
    }
    freeze["manifest_body_sha256"] = _payload_sha256(freeze)
    _write_json(prepared / "finalist_replay_then_oos_freeze.json", freeze)

    decoder_root = tmp_path / "decoder"
    decoder_root.mkdir()
    binding = {
        "schema_version": "fixture",
        "selection_payload_sha256": SELECTION,
        "decoder_contract": [
            {
                "decoder_id": DECODER_ID,
                "selection": "TOP_K",
                "top_k": 10,
                "weighting": "EQUAL",
                "target_refresh_clock": (
                    "EACH_SESSION_OPEN_FROM_PRIOR_CLOSE_SIGNAL"
                ),
                "session_end_policy": "FINAL_CLOSE_MARK_NO_FORCED_SALE",
                "payload_sha256": DECODER_CONTRACT,
            }
        ],
    }
    binding_path = _write_json(decoder_root / "input_binding.json", binding)
    metric_rows = []
    for decoder_id in (
        "CURRENT_TOP20PCT_EQUAL",
        DECODER_ID,
        "TOPK_10_RANK",
    ):
        for row in reversed(pair_rows):
            good = row["pair_id"] in {
                "pair-first",
                "pair-duplicate",
                "pair-last",
            }
            reward_fail = row["pair_id"] == "pair-reward-fail"
            metric_rows.append(
                {
                    "pair_id": row["pair_id"],
                    "decoder_id": decoder_id,
                    "primary_candidate_id": row["primary_candidate_id"],
                    "control_candidate_id": row["control_candidate_id"],
                    "primary_continuous_book_net_reward": (
                        -0.1 if reward_fail else (0.2 if good else -0.2)
                    ),
                    "matched_continuous_book_net_reward_increment": (
                        0.2 if good or reward_fail else -0.2
                    ),
                    "primary_cumulative_net_return": (
                        0.1 if good or reward_fail else -0.1
                    ),
                    "matched_cumulative_net_return_increment": (
                        0.05 if good or reward_fail else -0.05
                    ),
                }
            )
    metrics_path = decoder_root / "decoder_pair_metrics.parquet"
    pd.DataFrame(metric_rows).to_parquet(metrics_path, index=False)
    closure = {
        "schema_version": "fixture",
        "status": (
            "CN_PORTFOLIO_DECODER_V2_CLOSED_IMMUTABLE_DIAGNOSTIC_ONLY"
        ),
        "selection_payload_sha256": SELECTION,
        "pair_count": 32,
        "candidate_member_count": 64,
        "pair_metric_count": 96,
        "accounting_invariants_status": "PASS",
        "baseline_parity_status": "PASS",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "artifacts": [
            _artifact(binding_path, root=decoder_root),
            _artifact(metrics_path, root=decoder_root),
        ],
    }
    closure["manifest_body_sha256"] = _payload_sha256(closure)
    _write_json(decoder_root / "DECODER_V2_COMPLETE.json", closure)
    return decoder_root, source_root


def test_freeze_uses_four_gates_original_order_and_first_mechanism(
    tmp_path: Path,
) -> None:
    decoder_root, source_root = _fixture(tmp_path)
    output = tmp_path / "finalists"
    result = freeze_decoder_v2_finalists(
        decoder_root=decoder_root,
        source_replay_root=source_root,
        output_root=output,
        generator_repo_sha="c" * 40,
        expected_selection_payload_sha256=SELECTION,
        expected_decoder_contract_payload_sha256=DECODER_CONTRACT,
        expected_selected_pairs=2,
    )
    assert result["selected_pair_ids"] == ["pair-first", "pair-last"]
    review = pd.read_parquet(output / "finalist_review_ledger.parquet")
    failed = review.loc[review["pair_id"].eq("pair-reward-fail")].iloc[0]
    assert "PRIMARY_CONTINUOUS_BOOK_NET_REWARD_NOT_POSITIVE" in failed[
        "economic_admission_blockers"
    ]
    duplicate = review.loc[review["pair_id"].eq("pair-duplicate")].iloc[0]
    assert duplicate["finalist_outcome"] == (
        "DUPLICATE_ECONOMIC_MECHANISM_FIRST_OCCURRENCE_KEPT"
    )
    summary = json.loads(
        (output / "finalist_summary.json").read_text(encoding="utf-8")
    )
    assert summary["blocked_rows_backfilled"] == 0
    assert summary["selected_economic_mechanism_unique"] == 2
    assert summary["validation_reads"] == 0

    receipt = tmp_path / "verification.json"
    verified = verify_decoder_v2_finalists(
        finalist_root=output, receipt_path=receipt
    )
    assert verified["status"] == (
        "PASS_INDEPENDENT_TRAIN_ONLY_FREEZE_VERIFICATION"
    )
    assert verified["selected_pair_ids"] == ["pair-first", "pair-last"]


def test_freeze_fails_closed_when_expected_count_drifts(tmp_path: Path) -> None:
    decoder_root, source_root = _fixture(tmp_path)
    try:
        freeze_decoder_v2_finalists(
            decoder_root=decoder_root,
            source_replay_root=source_root,
            output_root=tmp_path / "finalists",
            generator_repo_sha="d" * 40,
            expected_selection_payload_sha256=SELECTION,
            expected_decoder_contract_payload_sha256=DECODER_CONTRACT,
            expected_selected_pairs=3,
        )
    except RuntimeError as exc:
        assert "finalist count drift" in str(exc)
    else:
        raise AssertionError("expected deterministic count drift failure")
