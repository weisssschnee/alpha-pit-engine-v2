from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import pandas as pd

from scripts.build_cn_stock_session_field_capability import _required_fields

from our_system_phase2.services.execution_clock_capability import (
    ExecutionClockCapabilityDriftError,
    assess_candidate_field_capability,
    expression_field_ids,
    load_execution_capability_manifest,
)


def test_expression_fields_are_checked_per_candidate_not_route() -> None:
    manifest = {
        "execution_clock": "stock_session",
        "fields": {
            "lagged_daily_value": {"status": "SUPPORTED", "reason": ""},
            "intraday_ret_from_open": {
                "status": "UNSUPPORTED",
                "reason": "VARIES_WITHIN_SESSION",
            },
        },
    }
    compatible = assess_candidate_field_capability(
        ("Rank($lagged_daily_value)",), manifest
    )
    incompatible = assess_candidate_field_capability(
        ("Sign($intraday_ret_from_open)",), manifest
    )
    assert compatible["compatible"] is True
    assert incompatible["compatible"] is False
    assert incompatible["unsupported_field_ids"] == [
        "intraday_ret_from_open"
    ]


def test_session_close_snapshot_of_intraday_field_is_compatible() -> None:
    manifest = {
        "execution_clock": "stock_session",
        "signal_clock": "SESSION_CLOSE_T",
        "fields": {
            "intraday_ret_from_open": {
                "status": "SUPPORTED",
                "reason": "DIRECT_FIELD_FINAL_PIT_CLOSE_SNAPSHOT",
                "session_materialization_policy": (
                    "LAST_OBSERVED_VALUE_AT_OR_BEFORE_SESSION_CLOSE_PIT"
                ),
            }
        },
    }
    assessed = assess_candidate_field_capability(
        ("Sign($intraday_ret_from_open)",), manifest
    )
    assert assessed["compatible"] is True
    assert assessed["unsupported_field_ids"] == []


def test_expression_field_parser_is_deterministic() -> None:
    assert expression_field_ids("Add($b, Mul($a, $b))") == ("a", "b")


def test_capability_authority_rejects_prohibited_side_effects(
    tmp_path: Path,
) -> None:
    payload = {
        "status": "ZERO_FINANCIAL_CAPABILITY_CLOSED",
        "execution_clock": "stock_session",
        "fields": {},
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_writes": 1,
        "feedback_writes": 0,
        "archive_writes": 0,
        "promotion_writes": 0,
    }
    payload["capability_manifest_sha256"] = hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    path = tmp_path / "capability.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(
        ExecutionClockCapabilityDriftError,
        match="optimizer_writes",
    ):
        load_execution_capability_manifest(path)


def test_required_fields_reads_authoritative_parquet_ledger(
    tmp_path: Path,
) -> None:
    path = tmp_path / "candidate_ledger.parquet"
    pd.DataFrame(
        {
            "canonical_expression": [
                "Add($lagged_daily_value, $close)",
                "Rank($close)",
            ]
        }
    ).to_parquet(path, index=False)
    assert _required_fields(path) == ("close", "lagged_daily_value")
