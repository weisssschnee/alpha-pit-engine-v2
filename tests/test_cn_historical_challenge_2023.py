from __future__ import annotations

import importlib
import json
from pathlib import Path

import pandas as pd

from scripts import build_cn_historical_challenge_split_manifest as split_builder
from scripts import build_cn_portfolio_decoder_autopsy_v1 as v1
from scripts import run_cn_fixed_survivor_forward_2026 as fixed10


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_historical_authorization_and_role_are_fail_closed(monkeypatch) -> None:
    monkeypatch.setenv("CN_FIXED10_RUN_MODE", "historical_challenge")
    module = importlib.reload(fixed10)
    try:
        path = (
            PROJECT_ROOT
            / "runtime"
            / "run_plans"
            / "cn_historical_challenge_2023_authorization.json"
        )
        authorization = module._verify_authorization(
            path,
            expected_sha256=v1._sha256(path),
            expected_selection_payload_sha256=(
                "7cfc2e454da7ae7561b57979db8010324422cd87ca3eef42167809400d59ef77"
            ),
            expected_forward_split_sha256="not-used-for-historical-role",
        )
        assert module.EVALUATION_ROLE == "historical_challenge"
        assert module.DATA_ROLE == "historical_challenge_report_only"
        assert module.READS_FIELD == "historical_challenge_reads"
        assert authorization["performance_rows_read_before_freeze"] == 0
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
