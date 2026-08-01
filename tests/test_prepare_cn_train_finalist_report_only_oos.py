from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.prepare_cn_train_finalist_report_only_oos import prepare, verify
from scripts.run_cn_finalist_replay_then_oos import (
    _artifact,
    _sha256,
    _stable_hash,
    _write_json,
)


def _self_hashed(payload: dict, field: str) -> dict:
    payload[field] = _stable_hash(payload)
    return payload


def _fixture(tmp_path: Path) -> tuple[Path, Path]:
    campaign_root = tmp_path / "campaign"
    prepared = campaign_root / "prepared"
    replay_root = campaign_root / "replay"
    prepared.mkdir(parents=True)
    replay_root.mkdir()
    protected = tmp_path / "protected.json"
    protected.write_text('{"authority":"fixed"}\n', encoding="utf-8")

    pair_id = "pair-one"
    candidate_ids = ["candidate-primary", "candidate-control"]
    source_candidates = pd.DataFrame(
        [
            {
                "candidate_id": candidate_ids[0],
                "pair_id": pair_id,
                "pair_member_role": "PRIMARY",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "formula": "x",
            },
            {
                "candidate_id": candidate_ids[1],
                "pair_id": pair_id,
                "pair_member_role": "CONTROL",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "formula": "y",
            },
        ]
    )
    source_pairs = pd.DataFrame(
        [
            {
                "pair_id": pair_id,
                "a_share_replay_status": "PAIR_REPLAY_COMPLETE",
                "a_share_executable_net_increment": 0.2,
                "primary_a_share_executable_net_reward": 0.5,
                "control_a_share_executable_net_reward": 0.3,
            }
        ]
    )
    source_candidate_results = source_candidates.assign(
        candidate_replay_status="CANDIDATE_REPLAY_COMPLETE"
    )
    source_pair_path = replay_root / "pair_replay_results.parquet"
    source_candidate_path = replay_root / "candidate_replay_results.parquet"
    source_pairs.to_parquet(source_pair_path, index=False)
    source_candidate_results.to_parquet(source_candidate_path, index=False)
    source_closure = _self_hashed(
        {
            "schema_version": "fixture",
            "status": "A_SHARE_REPLAY_CLOSED_IMMUTABLE",
            "selection_payload_sha256": "source-selection",
            "train_reads": 100,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "promotion": "FORBIDDEN",
            "artifacts": [
                _artifact(source_pair_path),
                _artifact(source_candidate_path),
            ],
        },
        "manifest_body_sha256",
    )
    source_closure_path = _write_json(
        replay_root / "REPLAY_COMPLETE.json", source_closure
    )

    split = tmp_path / "split.csv"
    registry = tmp_path / "registry.json"
    split.write_text("date,role\n", encoding="utf-8")
    registry.write_text("{}\n", encoding="utf-8")
    execution_contract = _self_hashed(
        {
            "schema_version": "fixture",
            "status": "ACTIVE_FIXED_COHORT_REPLAY_THEN_OOS",
            "repo_sha": "a" * 40,
            "selection_payload_sha256": "source-selection",
            "pair_count": 64,
            "candidate_member_count": 128,
            "route_counts": {"SLOW_TEMPORAL_CHANGE": 64},
            "sequence": ["REPLAY", "OOS"],
            "split_manifest": str(split),
            "split_manifest_sha256": _sha256(split),
            "registry": str(registry),
            "registry_sha256": _sha256(registry),
            "protected_source_hashes": {str(protected): _sha256(protected)},
            "feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "promotion": "FORBIDDEN",
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
        "contract_payload_sha256",
    )
    execution_contract_path = _write_json(
        prepared / "replay_then_oos_execution_contract.json",
        execution_contract,
    )
    source_freeze = _self_hashed(
        {
            "schema_version": "fixture",
            "status": "FROZEN_UNCHANGED_FIXED_COHORT",
            "selection_payload_sha256": "source-selection",
            "pair_count": 64,
            "candidate_member_count": 128,
            "candidate_ids": candidate_ids + [f"other-{i}" for i in range(126)],
            "pair_ids": [pair_id] + [f"other-pair-{i}" for i in range(63)],
            "execution_contract_artifact": _artifact(
                execution_contract_path, root=campaign_root
            ),
        },
        "manifest_body_sha256",
    )
    _write_json(
        prepared / "finalist_replay_then_oos_freeze.json", source_freeze
    )

    finalist_root = tmp_path / "finalists"
    finalist_root.mkdir()
    finalist_pair_path = finalist_root / "finalist_pairs.parquet"
    finalist_candidate_path = finalist_root / "finalist_candidates.parquet"
    source_pairs.assign(
        economic_mechanism_id="mechanism-one",
        portfolio_exposure_family_id="exposure-one",
    ).to_parquet(finalist_pair_path, index=False)
    source_candidates.iloc[::-1].to_parquet(
        finalist_candidate_path, index=False
    )
    finalist_contract = _self_hashed(
        {
            "schema_version": "fixture",
            "status": "TRAIN_ONLY_FINALISTS_CLOSED_ACTUAL_SMALLER_NO_BACKFILL",
            "strict_replay_root": str(replay_root),
            "strict_replay_closure_sha256": _sha256(source_closure_path),
            "authority_boundary": {
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
                "optimizer_feedback_write": "FORBIDDEN",
                "scheduler_write": "FORBIDDEN",
                "archive_write": "FORBIDDEN",
                "promotion": "FORBIDDEN",
                "automatic_report_only_oos": "FORBIDDEN",
                "successor_search": "FORBIDDEN",
            },
        },
        "contract_payload_sha256",
    )
    finalist_contract_path = _write_json(
        finalist_root / "finalist_contract.json", finalist_contract
    )
    summary_path = _write_json(
        finalist_root / "finalist_summary.json",
        {
            "selected_pairs": 1,
            "blocked_rows_backfilled": 0,
            "selection_payload_sha256": "finalist-selection",
        },
    )
    manifest = _self_hashed(
        {
            "schema_version": "fixture",
            "status": "TRAIN_ONLY_FINALIST_FREEZE_CLOSED_IMMUTABLE",
            "selection_payload_sha256": "finalist-selection",
            "selected_pairs": 1,
            "selected_candidate_members": 2,
            "artifacts": [
                _artifact(finalist_contract_path, root=finalist_root),
                _artifact(summary_path, root=finalist_root),
                _artifact(finalist_pair_path, root=finalist_root),
                _artifact(finalist_candidate_path, root=finalist_root),
            ],
        },
        "manifest_payload_sha256",
    )
    _write_json(finalist_root / "finalist_manifest.json", manifest)
    return finalist_root, source_closure_path


def test_prepare_and_verify_reuses_replay_without_financial_recompute(
    tmp_path: Path,
) -> None:
    finalist_root, _ = _fixture(tmp_path)
    output_root = tmp_path / "oos"
    prepared = prepare(
        finalist_root=finalist_root,
        output_root=output_root,
        generator_repo_sha="b" * 40,
    )
    receipt = verify(
        output_root=output_root,
        receipt_path=output_root / "verification.json",
    )

    assert prepared["pair_ids"] == ["pair-one"]
    assert prepared["validation_reads"] == 0
    assert receipt["status"] == (
        "PASS_ZERO_FINANCIAL_SINGLE_PAIR_OOS_FREEZE"
    )
    derived = json.loads(
        (
            output_root / "derived_replay_evidence" / "REPLAY_COMPLETE.json"
        ).read_text(encoding="utf-8")
    )
    assert derived["financial_result_recomputed"] is False
    assert derived["pair_count"] == 1


def test_preflight_fails_if_protected_source_changes(tmp_path: Path) -> None:
    finalist_root, source_closure_path = _fixture(tmp_path)
    output_root = tmp_path / "oos"
    prepare(
        finalist_root=finalist_root,
        output_root=output_root,
        generator_repo_sha="c" * 40,
    )
    source_closure_path.write_text("{}\n", encoding="utf-8")

    try:
        verify(
            output_root=output_root,
            receipt_path=output_root / "verification.json",
        )
    except RuntimeError as exc:
        assert "protected source hash drift" in str(exc)
    else:
        raise AssertionError("protected source drift was accepted")


def test_launcher_never_invokes_train_replay() -> None:
    launcher = Path(
        "scripts/run_cn_train_finalist_report_only_oos_77o.ps1"
    ).read_text(encoding="utf-8")
    assert "$oosRunner replay" not in launcher
    assert "--train-field-root" not in launcher
    assert "$oosRunner oos" in launcher
    assert "--expected-pair-count 1" in launcher
