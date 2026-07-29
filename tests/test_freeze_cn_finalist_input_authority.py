from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import freeze_cn_finalist_input_authority as subject
from scripts import verify_cn_finalist_input_authority as verifier


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def _cohort(root: Path) -> None:
    root.mkdir(parents=True)
    pairs = pd.DataFrame({"pair_id": [f"pair-{idx:03d}" for idx in range(64)]})
    candidates = pd.DataFrame(
        {
            "candidate_id": [
                f"candidate-{idx:03d}" for idx in range(128)
            ]
        }
    )
    pairs.to_parquet(root / "keep_review_pairs.parquet", index=False)
    candidates.to_parquet(
        root / "keep_review_candidates.parquet",
        index=False,
    )
    _write_json(root / "keep_review_contract.json", {"contract": "test"})
    _write_json(root / "keep_review_summary.json", {"summary": "test"})
    pd.DataFrame({"row": [1]}).to_parquet(
        root / "productive_review_ledger.parquet",
        index=False,
    )
    artifacts = []
    for name in (
        "keep_review_contract.json",
        "productive_review_ledger.parquet",
        "keep_review_pairs.parquet",
        "keep_review_candidates.parquet",
        "keep_review_summary.json",
    ):
        path = root / name
        artifacts.append(
            {
                "path": name,
                "bytes": path.stat().st_size,
                "sha256": subject._sha256(path),
            }
        )
    manifest = {
        "schema_version": "cn_productive_keep_review_freeze_v1",
        "status": "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE",
        "selection_payload_sha256": "a" * 64,
        "artifacts": artifacts,
    }
    manifest["manifest_payload_sha256"] = subject._payload_sha256(manifest)
    _write_json(root / "keep_review_manifest.json", manifest)


def _release(root: Path, columns: tuple[str, ...]) -> None:
    panel = (
        root
        / "shard_00"
        / "phase3aq_wide_true1min"
        / "canary"
        / "phase3aq_true_1min_formula_canary.parquet"
    )
    panel.parent.mkdir(parents=True)
    values: dict[str, list[object]] = {}
    for column in columns:
        if column in {"code", "security_type", "exchange"}:
            values[column] = ["600000"]
        elif column == "trade_time":
            values[column] = [pd.Timestamp("2024-01-02 09:31")]
        elif column.startswith("is_") or column in {
            "universe_eligible",
            "suspended",
        }:
            values[column] = [False]
        else:
            values[column] = [1.0]
    pd.DataFrame(values).to_parquet(panel, index=False)
    _write_json(
        root / "development_only_release_manifest.json",
        {
            "release_id": "synthetic-development-release",
            "release_hash": "b" * 64,
            "data_role": "development",
            "forward_2026_present": False,
            "schema_sha256": "c" * 64,
            "allowed_dates": {
                "min": "2024-01-02",
                "max": "2025-07-07",
            },
        },
    )


def test_existing_release_is_bound_without_inventing_missing_authorities(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cohort = tmp_path / "cohort"
    release = tmp_path / "release"
    output = tmp_path / "output"
    _cohort(cohort)
    _release(
        release,
        (
            "code",
            "trade_time",
            "open",
            "high",
            "low",
            "close",
            "ctx_hfq_is_st",
        ),
    )
    monkeypatch.setattr(subject, "_repo_sha", lambda _: "d" * 40)

    result = subject.freeze(
        repo_root=tmp_path,
        cohort_root=cohort,
        release_root=release,
        output_root=output,
    )

    assert result["status"] == "HOLD_RESEARCH_FINALIST_INPUTS_INCOMPLETE"
    assert "session_field_not_bound:suspended" in result["blockers"]
    assert "actual_account_fee_contract_missing" in result["blockers"]
    assert "promotion_grade_universe_manifest_missing" in result["blockers"]
    binding = json.loads(
        (output / "finalist_input_authority_binding.json").read_text(
            encoding="utf-8"
        )
    )
    assert binding["new_authority_node_created"] is False
    assert binding["financial_replay_authorized"] is False
    assert binding["validation_reads"] == 0
    verified = verifier.verify(
        output_root=output,
        verification_root=tmp_path / "verification",
    )
    assert verified["status"] == "INDEPENDENT_VERIFICATION_PASS"
    assert verified["source_status"] == (
        "HOLD_RESEARCH_FINALIST_INPUTS_INCOMPLETE"
    )


def test_complete_frozen_inputs_can_close_zero_financial_binding(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cohort = tmp_path / "cohort"
    release = tmp_path / "release"
    output = tmp_path / "output"
    _cohort(cohort)
    _release(release, subject.DIRECT_REQUIRED_COLUMNS)
    universe = tmp_path / "universe.json"
    _write_json(
        universe,
        {
            "schema_version": "test_pit_universe_v1",
            "pit_membership": True,
            "survivorship_free": True,
            "delisting_history_included": True,
            "date_min": "2024-01-02",
            "date_max": "2025-07-07",
            "security_count": 5000,
            "source_reference": "synthetic_authoritative_master",
        },
    )
    fee = tmp_path / "fee.json"
    _write_json(
        fee,
        {
            "schema_version": "a_share_fee_contract_v1",
            "account_contract_confirmed": True,
            "fee_schedule": {
                "commission_bps": 2.5,
                "minimum_commission_cny": 5.0,
                "exchange_handling_bps": 0.341,
                "transfer_fee_bps": 0.1,
                "sell_stamp_duty_bps": 5.0,
                "effective_start": "2023-08-28",
                "effective_end": "2025-12-31",
                "source_reference": "test_account_and_statutory_sources",
            },
        },
    )
    monkeypatch.setattr(subject, "_repo_sha", lambda _: "d" * 40)

    result = subject.freeze(
        repo_root=tmp_path,
        cohort_root=cohort,
        release_root=release,
        output_root=output,
        universe_manifest=universe,
        fee_contract=fee,
    )

    assert result["status"] == "FINALIST_INPUT_AUTHORITY_READY"
    assert result["blocker_count"] == 0
    assert result["financial_replay_authorized"] is False


def test_explicit_deployment_sha_is_strictly_validated(
    tmp_path: Path,
) -> None:
    assert subject._resolve_repo_sha(tmp_path, "A" * 40) == "a" * 40
    with pytest.raises(ValueError, match="exact 40-character"):
        subject._resolve_repo_sha(tmp_path, "not-a-sha")
