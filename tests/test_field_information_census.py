from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.run_cn_full_field_information_research import collect_true1min

from our_system_phase2.services.field_information_census import (
    InformationCensusPolicy,
    aligned_pairwise_nmi,
    field_metrics,
    normalized_mutual_information,
    pairwise_nmi,
    quantile_codes,
    select_core_pack,
    select_representative_core_pack,
)


def test_nmi_detects_duplicate_without_using_performance() -> None:
    frame = pd.DataFrame({"a": np.arange(200), "b": np.arange(200), "c": np.tile([0, 1], 100)})
    codes = {name: quantile_codes(frame[name], bins=8) for name in frame}
    duplicate, support = normalized_mutual_information(codes["a"], codes["b"])
    unrelated, _ = normalized_mutual_information(codes["a"], codes["c"])
    assert support == 200
    assert duplicate == 1.0
    assert unrelated < 0.05


def test_nmi_handles_degenerate_distribution_without_domain_error() -> None:
    constant = np.zeros(10_000, dtype="int16")
    varied = np.arange(10_000, dtype="int16") % 16
    value, support = normalized_mutual_information(constant, varied)
    assert value == 0.0
    assert support == len(constant)


def test_core_pack_is_role_gated_and_redundancy_bounded() -> None:
    sample = pd.DataFrame(
        {
            "a": np.arange(200, dtype=float),
            "b": np.arange(200, dtype=float),
            "control": np.arange(200, dtype=float),
        }
    )
    full = {
        field: {
            "row_count": 200,
            "finite_count": 200,
            "temporal_comparison_count": 199,
            "temporal_change_count": 199,
            "cross_sectional_std_mean": 1.0,
            "minimum": 0.0,
            "maximum": 199.0,
        }
        for field in sample
    }
    policy = InformationCensusPolicy(bins=8, redundancy_nmi=0.95)
    metrics, codes = field_metrics(sample, fields=sample.columns, full_counts=full, policy=policy)
    pairs = pairwise_nmi(codes)
    pack = select_core_pack(
        metrics,
        pairs,
        roles={"a": "interaction-only", "b": "interaction-only", "control": "condition-only"},
        policy=policy,
    )
    assert len(pack["selected_field_ids"]) == 1
    decisions = {row["field_id"]: row["decision"] for row in pack["decisions"]}
    assert decisions["control"] == "CONTROL_ONLY"
    assert {decisions["a"], decisions["b"]} == {"EXPLORATORY_CORE", "REDUNDANCY_ARCHIVE"}


def test_aligned_pairwise_nmi_respects_coordinates_and_groups() -> None:
    left_index = pd.MultiIndex.from_tuples([("000001", day) for day in range(100)])
    shifted_index = pd.MultiIndex.from_tuples([("000001", day) for day in range(50, 150)])
    series = {
        "a": pd.Series(np.arange(100, dtype=float), index=left_index),
        "b": pd.Series(np.arange(50, 150, dtype=float), index=shifted_index),
        "c": pd.Series(np.arange(100, dtype=float), index=left_index),
    }
    rows = aligned_pairwise_nmi(
        series,
        groups={"a": "same", "b": "same", "c": "different"},
        bins=8,
    )
    assert len(rows) == 1
    assert rows[0]["left_field_id"] == "a"
    assert rows[0]["right_field_id"] == "b"
    assert rows[0]["joint_support"] == 50


def test_representative_core_pack_deduplicates_only_within_group() -> None:
    metrics = [
        {
            "field_id": "a",
            "field_role": "primary",
            "information_qualified": True,
            "semantic_support_group": "g1",
            "coverage": 1.0,
            "normalized_entropy": 1.0,
            "information_status": "EVALUATED",
        },
        {
            "field_id": "b",
            "field_role": "primary",
            "information_qualified": True,
            "semantic_support_group": "g1",
            "coverage": 1.0,
            "normalized_entropy": 0.9,
            "information_status": "EVALUATED",
        },
        {
            "field_id": "c",
            "field_role": "primary",
            "information_qualified": True,
            "semantic_support_group": "g2",
            "coverage": 1.0,
            "normalized_entropy": 0.8,
            "information_status": "EVALUATED",
        },
        {
            "field_id": "condition",
            "field_role": "condition-only",
            "information_qualified": True,
            "semantic_support_group": "g1",
            "coverage": 1.0,
            "normalized_entropy": 1.0,
            "information_status": "EVALUATED",
        },
    ]
    pairs = [
        {
            "left_field_id": "a",
            "right_field_id": "b",
            "semantic_support_group": "g1",
            "normalized_mutual_information": 1.0,
            "joint_support": 100,
        }
    ]
    pack = select_representative_core_pack(metrics, pairs)
    assert set(pack["selected_field_ids"]) == {"a", "c"}
    decisions = {row["field_id"]: row["decision"] for row in pack["decisions"]}
    assert decisions["b"] == "REDUNDANCY_ARCHIVE"
    assert decisions["condition"] == "CONTROL_ONLY"


def test_full_true1min_census_uses_development_release_and_materializes_states(tmp_path) -> None:
    shard = tmp_path / "shard.parquet"
    frame = pd.DataFrame(
        {
            "code": ["000001"] * 4 + ["000002"] * 4,
            "trade_time": pd.date_range("2025-01-02 09:30", periods=8, freq="min"),
            "close": [10.0, 10.1, 10.2, 10.3, 20.0, 19.9, 20.1, 20.2],
            "high": [10.1, 10.2, 10.3, 10.4, 20.1, 20.1, 20.2, 20.3],
            "low": [9.9, 10.0, 10.1, 10.2, 19.9, 19.8, 19.9, 20.0],
            "ret_1m": [0.0, 0.01, 0.01, 0.01, 0.0, -0.005, 0.01, 0.005],
            "intraday_ret_from_open": [0.0, 0.01, 0.02, 0.03, 0.0, -0.005, 0.005, 0.01],
        }
    )
    frame.to_parquet(shard, index=False, row_group_size=4)
    manifest = tmp_path / "development_only_release_manifest.json"
    manifest.write_text(
        __import__("json").dumps(
            {
                "data_role": "development",
                "forward_2026_present": False,
                "release_hash": "test",
                "schema_sha256": "test",
                "shards": [{"output_path": str(shard)}],
            }
        ),
        encoding="utf-8",
    )
    base = {
        "record_kind": "CAPABILITY_FIELD",
        "source_table": "true1min_augmented_panel",
        "field_role": "primary",
        "semantic_role": "primary",
        "data_family": "raw_1min",
    }
    master = [
        {**base, "field_name": field, "source_field": field}
        for field in ("close", "high", "low", "ret_1m", "intraday_ret_from_open")
    ]
    master.extend(
        {
            **base,
            "field_name": field,
            "source_field": source,
            "field_role": "state-only",
            "semantic_role": "state-only",
            "data_family": "intraday_derived_state",
        }
        for field, source in (
            ("state_bar_return_sign", "ret_1m"),
            ("state_intraday_return_sign", "intraday_ret_from_open"),
            ("state_close_range_location_sign", "close|high|low"),
        )
    )
    metrics, _, _, evidence = collect_true1min(
        manifest,
        master,
        row_groups_per_file=2,
        sample_modulus=1,
    )
    ids = {row["field_id"] for row in metrics}
    assert "state_bar_return_sign" in ids
    assert "state_close_range_location_sign" in ids
    assert evidence["full_release_row_count"] == len(frame)
    assert evidence["deterministic_sample_row_count"] == len(frame)
