from __future__ import annotations

import json
from pathlib import Path

import pytest

from our_system_phase2.services.post_train_validation import (
    run_automatic_post_train_validation,
)


def _write_json(path: Path, value: dict) -> Path:
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def test_validation_runs_automatically_only_after_immutable_train_complete(
    tmp_path: Path,
) -> None:
    candidate_pack = (tmp_path / "train_candidates.csv")
    candidate_pack.write_text("pair_id\npair.1\n", encoding="utf-8")
    archive = (tmp_path / "train_behavior.parquet")
    archive.write_bytes(b"immutable-train-archive")
    train_manifest = _write_json(
        tmp_path / "train_complete_manifest.json",
        {
            "status": "TRAIN_COMPLETE",
            "candidate_pack": str(candidate_pack),
            "promotion": "FORBIDDEN",
        },
    )
    calls = []

    def validation_runner() -> dict:
        calls.append("validation")
        return {
            "status": "VALIDATION_COMPLETE",
            "evaluation_role": "validation",
            "validation_usage": "report_only",
            "validation_reads": 73,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "promotion": "FORBIDDEN",
        }

    receipt = run_automatic_post_train_validation(
        train_manifest_path=train_manifest,
        validation_output_root=tmp_path / "validation",
        protected_train_artifacts=(candidate_pack, archive),
        validation_runner=validation_runner,
    )

    assert calls == ["validation"]
    assert receipt["status"] == "AUTOMATIC_POST_TRAIN_VALIDATION_COMPLETE"
    assert receipt["train_artifacts_immutable"] is True
    assert receipt["validation_feedback"] == "FORBIDDEN"


def test_validation_refuses_to_start_before_train_complete(tmp_path: Path) -> None:
    manifest = _write_json(tmp_path / "train.json", {"status": "TRAIN_RUNNING"})

    with pytest.raises(RuntimeError, match="TRAIN_NOT_COMPLETE"):
        run_automatic_post_train_validation(
            train_manifest_path=manifest,
            validation_output_root=tmp_path / "validation",
            protected_train_artifacts=(),
            validation_runner=lambda: {},
        )


def test_validation_result_cannot_claim_feedback_or_promotion(tmp_path: Path) -> None:
    manifest = _write_json(
        tmp_path / "train.json",
        {"status": "TRAIN_COMPLETE", "promotion": "FORBIDDEN"},
    )

    with pytest.raises(RuntimeError, match="VALIDATION_BOUNDARY_VIOLATION"):
        run_automatic_post_train_validation(
            train_manifest_path=manifest,
            validation_output_root=tmp_path / "validation",
            protected_train_artifacts=(),
            validation_runner=lambda: {
                "status": "VALIDATION_COMPLETE",
                "evaluation_role": "validation",
                "validation_usage": "report_only",
                "validation_reads": 1,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
                "feedback_write": "ALLOWED",
                "scheduler_write": "FORBIDDEN",
                "archive_write": "FORBIDDEN",
                "promotion": "FORBIDDEN",
            },
        )
