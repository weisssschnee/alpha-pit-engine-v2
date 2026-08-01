from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.freeze_cn_productive_keep_review_cohort import (
    _artifact,
    _payload_sha256,
    _sha256,
    _write_json,
)
from scripts.freeze_cn_train_replay_finalists import (
    freeze_train_replay_finalists,
)
from scripts.verify_cn_train_replay_finalists import (
    verify_train_replay_finalists,
)


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    input_root = tmp_path / "input"
    input_root.mkdir()
    pairs = pd.DataFrame(
        [
            {
                "pair_id": "pair-positive",
                "economic_mechanism_id": "mechanism-a",
                "portfolio_exposure_family_id": "exposure-a",
                "train_stability_score": 0.9,
                "train_stability_floor": 0.8,
                "train_stability_median": 0.85,
                "search_score": 0.7,
            },
            {
                "pair_id": "pair-negative",
                "economic_mechanism_id": "mechanism-b",
                "portfolio_exposure_family_id": "exposure-b",
                "train_stability_score": 0.8,
                "train_stability_floor": 0.7,
                "train_stability_median": 0.75,
                "search_score": 0.6,
            },
            {
                "pair_id": "pair-blocked",
                "economic_mechanism_id": "mechanism-c",
                "portfolio_exposure_family_id": "exposure-c",
                "train_stability_score": 1.0,
                "train_stability_floor": 1.0,
                "train_stability_median": 1.0,
                "search_score": 1.0,
            },
        ]
    )
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": f"{pair_id}-{role}",
                "pair_id": pair_id,
                "pair_member_role": role,
            }
            for pair_id in pairs["pair_id"]
            for role in ("primary", "control")
        ]
    )
    pair_path = input_root / "keep_review_pairs.parquet"
    candidate_path = input_root / "keep_review_candidates.parquet"
    pairs.to_parquet(pair_path, index=False)
    candidates.to_parquet(candidate_path, index=False)
    input_manifest = {
        "schema_version": "fixture",
        "status": "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE",
        "artifacts": [
            _artifact(pair_path, root=input_root),
            _artifact(candidate_path, root=input_root),
        ],
    }
    input_manifest["manifest_payload_sha256"] = _payload_sha256(input_manifest)
    _write_json(input_root / "keep_review_manifest.json", input_manifest)

    campaign_root = tmp_path / "campaign"
    replay_root = campaign_root / "replay"
    replay_root.mkdir(parents=True)
    replay_pairs = pd.DataFrame(
        [
            {
                "pair_id": "pair-positive",
                "a_share_replay_status": "PAIR_REPLAY_COMPLETE",
                "a_share_executable_net_increment": 0.2,
            },
            {
                "pair_id": "pair-negative",
                "a_share_replay_status": "PAIR_REPLAY_COMPLETE",
                "a_share_executable_net_increment": -0.1,
            },
            {
                "pair_id": "pair-blocked",
                "a_share_replay_status": "PAIR_REPLAY_BLOCKED",
                "a_share_executable_net_increment": 0.5,
            },
        ]
    )
    replay_candidates = candidates.assign(
        candidate_replay_status="CANDIDATE_REPLAY_COMPLETE"
    )
    replay_pair_path = replay_root / "pair_replay_results.parquet"
    replay_candidate_path = replay_root / "candidate_replay_results.parquet"
    replay_pairs.to_parquet(replay_pair_path, index=False)
    replay_candidates.to_parquet(replay_candidate_path, index=False)
    replay_closure = {
        "schema_version": "fixture",
        "status": "A_SHARE_REPLAY_CLOSED_IMMUTABLE",
        "artifacts": [
            {
                "path": str(replay_pair_path.resolve()),
                "bytes": replay_pair_path.stat().st_size,
                "sha256": _sha256(replay_pair_path),
            },
            {
                "path": str(replay_candidate_path.resolve()),
                "bytes": replay_candidate_path.stat().st_size,
                "sha256": _sha256(replay_candidate_path),
            },
        ],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    replay_closure["manifest_body_sha256"] = _payload_sha256(replay_closure)
    replay_closure_path = _write_json(
        replay_root / "REPLAY_COMPLETE.json", replay_closure
    )
    _write_json(
        campaign_root / "TRAIN_REPLAY_ONLY_COMPLETE.json",
        {
            "status": "TRAIN_REPLAY_ONLY_CLOSED",
            "replay_closure_sha256": _sha256(replay_closure_path),
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
    )

    funnel_contract = {
        "schema_version": "fixture",
        "status": "FROZEN_USER_AUTHORIZED_TRAIN_ONLY_FUNNEL",
        "finalist_freeze": {
            "target_pairs_minimum": 24,
            "target_pairs_maximum": 32,
            "insufficient_supply_action": (
                "FREEZE_ACTUAL_SMALLER_COUNT_AND_REPORT_DO_NOT_BACKFILL_WITH_BLOCKED_ROWS"
            ),
        },
    }
    funnel_contract["contract_payload_sha256"] = _payload_sha256(
        funnel_contract
    )
    funnel_contract_path = _write_json(
        tmp_path / "funnel_contract.json", funnel_contract
    )
    return replay_root, input_root, funnel_contract_path


def test_freeze_actual_smaller_without_blocked_backfill(tmp_path: Path) -> None:
    replay_root, input_root, funnel_contract_path = _fixture(tmp_path)
    output_root = tmp_path / "finalists"
    result = freeze_train_replay_finalists(
        replay_root=replay_root,
        replay_input_root=input_root,
        funnel_contract_path=funnel_contract_path,
        output_root=output_root,
        generator_repo_sha="a" * 40,
    )

    assert result["status"] == (
        "TRAIN_ONLY_FINALISTS_CLOSED_ACTUAL_SMALLER_NO_BACKFILL"
    )
    assert result["selected_pairs"] == 1
    assert result["selected_pair_ids"] == ["pair-positive"]
    summary = json.loads(
        (output_root / "finalist_summary.json").read_text(encoding="utf-8")
    )
    assert summary["blocked_rows_backfilled"] == 0
    assert summary["insufficient_supply"] is True


def test_independent_verifier_recomputes_selection(tmp_path: Path) -> None:
    replay_root, input_root, funnel_contract_path = _fixture(tmp_path)
    output_root = tmp_path / "finalists"
    freeze_train_replay_finalists(
        replay_root=replay_root,
        replay_input_root=input_root,
        funnel_contract_path=funnel_contract_path,
        output_root=output_root,
        generator_repo_sha="b" * 40,
    )
    receipt_path = tmp_path / "verification.json"
    result = verify_train_replay_finalists(
        finalist_root=output_root, receipt_path=receipt_path
    )

    assert result["status"] == "PASS_INDEPENDENT_VERIFICATION"
    assert result["selected_pairs"] == 1
    assert result["selected_pair_ids"] == ["pair-positive"]
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["validation_reads"] == 0
    assert receipt["promotion_eligible"] is False
