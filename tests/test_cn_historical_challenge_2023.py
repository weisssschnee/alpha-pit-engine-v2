from __future__ import annotations

import importlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import build_cn_historical_challenge_split_manifest as split_builder
from scripts import build_cn_portfolio_decoder_autopsy_v1 as v1
from scripts import run_cn_finalist_replay_then_oos as replay
from scripts import run_cn_fixed_survivor_forward_2026 as fixed10


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_unopened_synthetic_historical_authorization_is_fail_closed(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("CN_FIXED10_RUN_MODE", "historical_challenge")
    module = importlib.reload(fixed10)
    try:
        path = (
            PROJECT_ROOT
            / "runtime"
            / "run_plans"
            / "cn_historical_challenge_2023_authorization.json"
        )
        role_registry_path = tmp_path / "roles.json"
        access_started_path = tmp_path / "access.json"
        outcome_path = tmp_path / "outcome.json"
        role_registry_path.write_text(
            json.dumps(
                {
                    "default_deny": True,
                    "asset_states": {
                        "historical_challenge_2023_b05e2ca0": {
                            "current_role": "challenge",
                            "performance_rows_read": 0,
                        },
                        "forward_b_tdx_lc1_20260413_20260514_f69cc84f": {
                            "current_role": "forward",
                            "performance_rows_read": 0,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        access_started_path.write_text(
            json.dumps(
                {
                    "asset_id": "historical_challenge_2023_b05e2ca0",
                    "status": "AUTHORIZED_UNOPENED",
                    "data_role_after_transition": "challenge",
                }
            ),
            encoding="utf-8",
        )
        outcome_path.write_text(
            json.dumps(
                {
                    "access_decision": {
                        "historical_challenge_2023_state": "UNOPENED",
                        "forward_b_state": "SEALED",
                        "forward_b_access": "NOT_AUTHORIZED",
                    },
                    "provenance": {"historical_challenge_reads": 0},
                }
            ),
            encoding="utf-8",
        )
        authorization = module._verify_authorization(
            path,
            expected_sha256=v1._sha256(path),
            expected_selection_payload_sha256=(
                "7cfc2e454da7ae7561b57979db8010324422cd87ca3eef42167809400d59ef77"
            ),
            expected_forward_split_sha256="not-used-for-historical-role",
            role_registry_path=role_registry_path,
            access_started_path=access_started_path,
            outcome_path=outcome_path,
        )
        assert module.EVALUATION_ROLE == "historical_challenge"
        assert module.DATA_ROLE == "historical_challenge_report_only"
        assert module.READS_FIELD == "historical_challenge_reads"
        assert authorization["performance_rows_read_before_freeze"] == 0
    finally:
        monkeypatch.delenv("CN_FIXED10_RUN_MODE")
        importlib.reload(module)


def test_historical_fee_schedule_uses_the_statutory_2023_transition(
    monkeypatch,
) -> None:
    monkeypatch.setenv("CN_FIXED10_RUN_MODE", "historical_challenge")
    module = importlib.reload(fixed10)
    try:
        schedule, mode = module._fee_schedule_for_run(
            {
                "commission_bps": 3.0,
                "minimum_commission_cny": 5.0,
                "exchange_handling_bps": 0.541,
                "transfer_fee_bps": 0.1,
                "sell_stamp_duty_bps": 5.0,
                "effective_start": "2023-08-28",
                "effective_end": "2025-12-31",
                "source_reference": "frozen research fee authority",
            },
            evaluation_dates=set(
                pd.to_datetime(["2023-01-03", "2023-12-29"]).normalize()
            ),
        )
        assert mode == "HISTORICAL_DATED_STAMP_DUTY"
        assert schedule.effective_start == "2023-01-01"
        assert schedule.fee(
            100_000.0, side="SELL", trade_date="2023-08-25"
        ) - schedule.fee(
            100_000.0, side="SELL", trade_date="2023-08-28"
        ) == pytest.approx(50.0)
    finally:
        monkeypatch.delenv("CN_FIXED10_RUN_MODE")
        importlib.reload(module)


def test_historical_split_freezes_every_observed_date_without_filtering(
    tmp_path: Path,
) -> None:
    session_root = tmp_path / "sessions"
    session_root.mkdir()
    dates = pd.bdate_range("2023-01-03", periods=220)
    for index in range(12):
        pd.DataFrame(
            {
                "date": dates,
                "code": f"{index + 1:06d}",
            }
        ).to_parquet(session_root / f"shard_{index:02d}.parquet", index=False)
    manifest = {
        "schema_version": "cn_yearly_1min_zip_session_shards_v1",
        "status": "SESSIONIZED_ARCHIVE_COMPLETE",
        "expected_year": 2023,
        "shard_count": 12,
    }
    manifest["manifest_payload_sha256"] = v1._stable_hash(manifest)
    v1._write_json(session_root / "SESSIONIZED_ARCHIVE_COMPLETE.json", manifest)

    result = split_builder.build(
        sessionized_root=session_root,
        output_root=tmp_path / "split",
    )
    split = pd.read_csv(result["split_manifest"])
    assert len(split) == 220
    assert split["split"].eq("historical_challenge").all()
    assert split["optimizer_usage"].eq("report_only").all()
    receipt = v1._read_json(Path(result["receipt"]))
    body = dict(receipt)
    declared = body.pop("receipt_payload_sha256")
    assert v1._stable_hash(body) == declared
    assert receipt["outcome_based_filtering"] == "FORBIDDEN"


def test_access_transition_and_launcher_preserve_forward_b_seal() -> None:
    access_path = (
        PROJECT_ROOT
        / "runtime"
        / "run_plans"
        / "cn_historical_challenge_2023_access_started.json"
    )
    access = json.loads(access_path.read_text(encoding="utf-8"))
    body = dict(access)
    declared = body.pop("access_payload_sha256")
    assert v1._stable_hash(body) == declared
    assert access["data_role_after_transition"] == "spent"
    assert access["forward_b_reads"] == 0

    launcher = (
        PROJECT_ROOT
        / "scripts"
        / "run_cn_fixed_survivor_historical_challenge_2023_77o.ps1"
    ).read_text(encoding="utf-8")
    assert "$env:CN_FIXED10_RUN_MODE = 'historical_challenge'" in launcher
    assert "[int]$ExecutorWorkerCount = 12" in launcher
    assert "VALIDATION_EXCLUSIVE_32" in launcher
    assert "$deployment.PSObject.Properties['remote_workspace']" in launcher
    assert "[string]$deployment.remote_workspace" in launcher
    assert "--evaluation-role historical_challenge" in launcher
    assert "forward_b_reads = 0" in launcher
    assert "HISTORICAL_CHALLENGE_2023_COMPLETE.json" in launcher
    assert "optimizer_feedback_write = 'FORBIDDEN'" in launcher


def test_historical_sidecars_are_role_bound_and_cross_read_fail_closed(
    tmp_path: Path,
) -> None:
    field_root = tmp_path / "fields"
    label_root = tmp_path / "labels"
    field_root.mkdir()
    label_root.mkdir()
    split_hash = "a" * 64
    field_manifest = {
        "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
        "evaluation_role": "historical_challenge",
        "data_role": "historical_challenge_report_only",
        "split_manifest_hash": split_hash,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "historical_challenge_reads": 2,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "fields": ["open", "close"],
        "shards": [],
    }
    label_manifest = {
        "status": "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY",
        "evaluation_role": "historical_challenge",
        "data_role": "historical_challenge_report_only",
        "split_manifest_hash": split_hash,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "historical_challenge_reads": 2,
    }
    v1._write_json(
        field_root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json",
        field_manifest,
    )
    v1._write_json(
        label_root / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json",
        label_manifest,
    )

    replay._validate_sidecar(
        field_root,
        evaluation_role="historical_challenge",
        split_hash=split_hash,
    )
    replay._validate_label_sidecar(
        label_root,
        evaluation_role="historical_challenge",
        split_hash=split_hash,
    )

    field_manifest["validation_reads"] = 1
    v1._write_json(
        field_root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json",
        field_manifest,
    )
    with pytest.raises(RuntimeError, match="read validation"):
        replay._validate_sidecar(
            field_root,
            evaluation_role="historical_challenge",
            split_hash=split_hash,
        )
