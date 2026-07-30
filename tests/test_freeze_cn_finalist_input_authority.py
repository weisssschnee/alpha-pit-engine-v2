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


def _session_authority(
    root: Path,
    *,
    fee_mode: str = "CONSERVATIVE_RESEARCH_UPPER_BOUND_NON_PROMOTION",
) -> Path:
    root.mkdir(parents=True)
    st_root = root / "pit_st"
    st_root.mkdir()
    st_artifact = st_root / "pit_historical_st.parquet"
    pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-02")],
            "code": ["600000"],
            "is_st": [True],
        }
    ).to_parquet(st_artifact, index=False)
    st_manifest_payload = {
        "schema_version": "cn_finalist_pit_historical_st_source_v1",
        "status": "PIT_HISTORICAL_ST_SOURCE_CLOSED_IMMUTABLE",
        "artifact": st_artifact.name,
        "artifact_sha256": subject._sha256(st_artifact),
    }
    st_manifest_payload["manifest_payload_sha256"] = subject._payload_sha256(
        st_manifest_payload
    )
    st_manifest = st_root / "pit_st_source_manifest.json"
    _write_json(st_manifest, st_manifest_payload)
    sidecar = root / "session_authority.parquet"
    values: dict[str, list[object]] = {}
    for column in subject.DIRECT_REQUIRED_COLUMNS:
        if column in {"trade_time", "open", "high", "low", "close"}:
            continue
        if column == "code":
            values[column] = ["600000"]
        elif column in {"security_type"}:
            values[column] = ["A_SHARE"]
        elif column == "exchange":
            values[column] = ["SSE"]
        elif column.startswith("is_") or column in {
            "universe_eligible",
            "suspended",
        }:
            values[column] = [False]
        else:
            values[column] = [1.0]
    values["date"] = [pd.Timestamp("2024-01-02")]
    pd.DataFrame(values).to_parquet(sidecar, index=False)
    universe = root / "universe_manifest.json"
    _write_json(
        universe,
        {
            "schema_version": "test_pit_universe_v1",
            "pit_membership": True,
            "survivorship_free": True,
            "delisting_history_included": True,
            "date_min": "2024-01-02",
            "date_max": "2025-07-07",
            "security_count": 1,
            "source_reference": "synthetic_authoritative_master",
        },
    )
    fee = root / "fee_contract.json"
    fee_payload: dict[str, object] = {
        "schema_version": "a_share_fee_contract_v2",
        "account_contract_confirmed": fee_mode == "ACTUAL_ACCOUNT_CONTRACT",
        "fee_schedule": {
            "commission_bps": 3.0,
            "minimum_commission_cny": 5.0,
            "exchange_handling_bps": 0.541,
            "transfer_fee_bps": 0.1,
            "sell_stamp_duty_bps": 5.0,
            "effective_start": "2023-08-28",
            "effective_end": "2025-12-31",
            "source_reference": "synthetic statutory sources",
        },
    }
    if fee_mode != "ACTUAL_ACCOUNT_CONTRACT":
        fee_payload.update(
            {
                "contract_mode": fee_mode,
                "research_upper_bound_confirmed": True,
                "promotion_authorized": False,
            }
        )
    _write_json(fee, fee_payload)
    artifacts = []
    for path in (sidecar, universe, fee):
        artifacts.append(
            {
                "path": path.name,
                "bytes": path.stat().st_size,
                "sha256": subject._sha256(path),
            }
        )
    manifest = {
        "schema_version": "cn_finalist_session_authority_v1",
        "status": "SESSION_AUTHORITY_CLOSED_IMMUTABLE",
        "data_role": "development",
        "new_authority_node_created": False,
        "date_min": "2024-01-02",
        "date_max": "2025-07-07",
        "security_count": 1,
        "session_row_count": 1,
        "observed_session_row_count": 1,
        "st_known_observed_session_count": 1,
        "st_true_observed_session_count": 1,
        "pit_st_observed_session_coverage": 1.0,
        "leading_unknown_st_blocked_session_count": 0,
        "pit_historical_st_source": {
            "manifest": str(st_manifest),
            "manifest_file_sha256": subject._sha256(st_manifest),
            "artifact": str(st_artifact),
            "artifact_sha256": subject._sha256(st_artifact),
            "st_true_row_count": 1,
            "semantics": "EXACT_CODE_DATE_NAME_STATE_NO_FORWARD_BACKFILL",
        },
        "columns": list(pd.read_parquet(sidecar).columns),
        "session_authority_path": str(sidecar),
        "universe_manifest_path": str(universe),
        "fee_contract_path": str(fee),
        "artifacts": artifacts,
        "financial_reads": 0,
        "financial_result_recomputed": False,
        "optimizer_feedback_writes": 0,
        "scheduler_writes": 0,
        "archive_writes": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    manifest["manifest_payload_sha256"] = subject._payload_sha256(manifest)
    manifest_path = root / "session_authority_manifest.json"
    _write_json(manifest_path, manifest)
    return manifest_path


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


def test_all_null_or_all_blocked_st_cannot_close_ready_binding(
    tmp_path: Path,
) -> None:
    session = tmp_path / "session"
    manifest_path = _session_authority(session)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["st_known_observed_session_count"] = 0
    manifest["pit_st_observed_session_coverage"] = 0.0
    manifest["leading_unknown_st_blocked_session_count"] = 1
    manifest["manifest_payload_sha256"] = subject._payload_sha256(
        {
            key: value
            for key, value in manifest.items()
            if key != "manifest_payload_sha256"
        }
    )
    _write_json(manifest_path, manifest)

    _, _, blockers = subject._validate_session_authority_manifest(
        manifest_path,
        date_min="2024-01-02",
        date_max="2025-07-07",
    )

    assert "pit_historical_st_observed_coverage_incomplete" in blockers
    assert "pit_historical_st_observed_coverage_not_one" in blockers
    assert "pit_historical_st_blocks_all_sessions" in blockers


def test_complete_frozen_inputs_can_close_zero_financial_binding(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cohort = tmp_path / "cohort"
    release = tmp_path / "release"
    session = tmp_path / "session"
    output = tmp_path / "output"
    _cohort(cohort)
    _release(
        release,
        ("code", "trade_time", "open", "high", "low", "close"),
    )
    session_manifest = _session_authority(session)
    monkeypatch.setattr(subject, "_repo_sha", lambda _: "d" * 40)

    result = subject.freeze(
        repo_root=tmp_path,
        cohort_root=cohort,
        release_root=release,
        output_root=output,
        session_authority_manifest=session_manifest,
    )

    assert result["status"] == "FINALIST_INPUT_AUTHORITY_READY"
    assert result["blocker_count"] == 0
    assert result["financial_replay_authorized"] is False
    binding = json.loads(
        (output / "finalist_input_authority_binding.json").read_text(
            encoding="utf-8"
        )
    )
    assert binding["fee_contract"]["account_contract_confirmed"] is False
    assert binding["fee_contract"]["research_upper_bound_confirmed"] is True
    assert binding["promotion_authorized"] is False
    verified = verifier.verify(
        output_root=output,
        verification_root=tmp_path / "verification",
    )
    assert verified["status"] == "INDEPENDENT_VERIFICATION_PASS"


def test_research_fee_contract_cannot_claim_promotion(
    tmp_path: Path,
) -> None:
    session = tmp_path / "session"
    manifest_path = _session_authority(session)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    fee_path = Path(manifest["fee_contract_path"])
    fee = json.loads(fee_path.read_text(encoding="utf-8"))
    fee["promotion_authorized"] = True
    _write_json(fee_path, fee)

    validated, blockers = subject._validate_fee_contract(
        fee_path,
        date_min="2024-01-02",
        date_max="2025-07-07",
    )

    assert validated is None
    assert blockers == ["fee_contract_not_confirmed_for_research_or_account"]


def test_explicit_deployment_sha_is_strictly_validated(
    tmp_path: Path,
) -> None:
    assert subject._resolve_repo_sha(tmp_path, "A" * 40) == "a" * 40
    with pytest.raises(ValueError, match="exact 40-character"):
        subject._resolve_repo_sha(tmp_path, "not-a-sha")
