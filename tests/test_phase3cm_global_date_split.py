from __future__ import annotations

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    normalize_against_fixed_manifest,
)
import pytest


def test_fixed_manifest_removes_cross_shard_date_overlap() -> None:
    rows = []
    for day in range(1, 11):
        trade_date = f"2026-01-{day:02d}"
        rows.extend(
            [
                {"trade_date": trade_date, "split": "train", "shard_index": 0},
                {"trade_date": trade_date, "split": "validation", "shard_index": 1},
            ]
        )

    fixed_manifest = [
        {
            "trade_date": f"2026-01-{day:02d}",
            "split": "train" if day <= 6 else "validation" if day <= 8 else "holdout",
        }
        for day in range(1, 11)
    ]
    manifest, audit = normalize_against_fixed_manifest(
        rows,
        train_fraction=0.60,
        validation_fraction=0.20,
        split_manifest=fixed_manifest,
    )

    split_by_date = {row["trade_date"]: row["split"] for row in manifest}
    assert list(split_by_date.values()).count("train") == 6
    assert list(split_by_date.values()).count("validation") == 2
    assert list(split_by_date.values()).count("holdout") == 2
    assert {row["split"] for row in rows if row["trade_date"] == "2026-01-01"} == {"train"}
    assert {row["split"] for row in rows if row["trade_date"] == "2026-01-07"} == {"validation"}
    assert {row["split"] for row in rows if row["trade_date"] == "2026-01-09"} == {"holdout"}
    assert audit["preexisting_cross_split_date_count"] == 10
    assert audit["post_normalization_cross_split_date_count"] == 0
    assert audit["reassigned_row_count"] > 0
    assert audit["boundaries"]["train_end"] == "2026-01-06"
    assert audit["boundaries"]["validation_end"] == "2026-01-08"


def test_fixed_trade_date_manifest_is_authoritative() -> None:
    rows = [
        {"trade_date": "2026-01-02", "split": "holdout"},
        {"trade_date": "2026-01-04", "split": "train"},
    ]
    fixed_manifest = [
        {"trade_date": "2026-01-01", "split": "train"},
        {"trade_date": "2026-01-02", "split": "train"},
        {"trade_date": "2026-01-03", "split": "validation"},
        {"trade_date": "2026-01-04", "split": "holdout"},
    ]

    manifest, audit = normalize_against_fixed_manifest(
        rows,
        train_fraction=0.50,
        validation_fraction=0.25,
        split_manifest=fixed_manifest,
    )

    assert [row["split"] for row in rows] == ["train", "holdout"]
    assert len(manifest) == 4
    assert audit["split_policy"] == "fixed_trade_date_manifest"
    assert audit["trade_date_count"] == 2
    assert audit["manifest_trade_date_count"] == 4
    assert audit["manifest_unused_date_count"] == 2
    assert audit["post_normalization_cross_split_date_count"] == 0


def test_formal_split_normalization_has_no_derived_fallback() -> None:
    with pytest.raises(ValueError, match="requires a non-empty fixed manifest"):
        normalize_against_fixed_manifest(
            [{"trade_date": "2024-01-02", "split": "train"}],
            train_fraction=0.75,
            validation_fraction=0.15,
            split_manifest=[],
        )
