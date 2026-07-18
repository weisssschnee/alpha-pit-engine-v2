from __future__ import annotations

import pytest

from our_system_phase2.services.compositional_signal_sketch_inputs import (
    classify_signal_sketch_receipts,
)


def test_signal_sketch_inputs_split_intraday_and_session_clocks_without_reward() -> None:
    receipts = [
        {
            "exact_identity": "minute",
            "candidate_id": "m",
            "route_id": "MINUTE_STATIC",
            "canonical_expression": "CSRank($close)",
            "skeleton_id": "minute.level",
            "declared_field_ids": ["close"],
            "source_families": ["raw_1min"],
        },
        {
            "exact_identity": "slow_context",
            "candidate_id": "s1",
            "route_id": "SLOW_TEMPORAL_CHANGE",
            "canonical_expression": "CSRank(Delta($ctx_pb,5))",
            "skeleton_id": "slow.delta",
            "declared_field_ids": ["ctx_pb"],
            "source_families": ["lagged_daily_context"],
        },
        {
            "exact_identity": "slow_fund",
            "candidate_id": "s2",
            "route_id": "SLOW_CROSS_SECTIONAL_LEVEL",
            "canonical_expression": "CSRank($fund_roa)",
            "skeleton_id": "slow.level",
            "declared_field_ids": ["fund_roa"],
            "source_families": ["canonical_fundamental_profitability"],
        },
    ]

    result = classify_signal_sketch_receipts(
        receipts,
        {"minute", "slow_context", "slow_fund"},
        source_family_by_field={
            "close": "raw_1min",
            "ctx_pb": "lagged_daily_context",
            "fund_roa": "canonical_fundamental_profitability",
        },
    )

    assert [row["candidate_id"] for row in result.active_rows] == ["m"]
    assert [row["candidate_id"] for row in result.session_rows] == ["s1", "s2"]
    assert result.session_context_fields == ("ctx_pb",)
    assert result.canonical_fundamental_fields == ("fund_roa",)
    assert result.summary["reward_columns_read"] == 0
    assert result.summary["validation_holdout_forward_read"] is False


def test_signal_sketch_rejects_unmaterialized_supplemental_root() -> None:
    receipt = {
        "exact_identity": "blocked",
        "candidate_id": "blocked",
        "route_id": "SLOW_CROSS_SECTIONAL_LEVEL",
        "declared_field_ids": ["fund_disclosure_balance_age_sessions"],
        "materialization_status": "NOT_MATERIALIZED",
        "signal_sketch_allowed": False,
    }
    with pytest.raises(RuntimeError, match="cannot enter signal sketch"):
        classify_signal_sketch_receipts(
            [receipt],
            {"blocked"},
            source_family_by_field={
                "fund_disclosure_balance_age_sessions": (
                    "canonical_fundamental_disclosure_timing_staleness"
                )
            },
        )


def test_signal_sketch_requires_exact_candidate_receipt_hash_injection() -> None:
    receipt = {
        "exact_identity": "bound",
        "candidate_id": "bound",
        "route_id": "SLOW_CROSS_SECTIONAL_LEVEL",
        "declared_field_ids": ["fund_disclosure_balance_age_sessions"],
        "runtime_ready": True,
        "materialization_status": "MATERIALIZED_DEVELOPMENT_ONLY",
        "signal_sketch_allowed": True,
        "materialization_support_receipt_hashes": {
            "fund_disclosure_balance_age_sessions": "a" * 64,
        },
    }
    source_families = {
        "fund_disclosure_balance_age_sessions": (
            "canonical_fundamental_disclosure_timing_staleness"
        )
    }

    accepted = classify_signal_sketch_receipts(
        [receipt],
        {"bound"},
        source_family_by_field=source_families,
    )
    assert [row["candidate_id"] for row in accepted.session_rows] == ["bound"]

    forged = dict(receipt, materialization_support_receipt_hashes={})
    with pytest.raises(RuntimeError, match="exact per-root"):
        classify_signal_sketch_receipts(
            [forged],
            {"bound"},
            source_family_by_field=source_families,
        )
