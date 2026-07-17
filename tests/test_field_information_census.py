from __future__ import annotations

import numpy as np
import pandas as pd

from our_system_phase2.services.field_information_census import (
    InformationCensusPolicy,
    field_metrics,
    normalized_mutual_information,
    pairwise_nmi,
    quantile_codes,
    select_core_pack,
)


def test_nmi_detects_duplicate_without_using_performance() -> None:
    frame = pd.DataFrame({"a": np.arange(200), "b": np.arange(200), "c": np.tile([0, 1], 100)})
    codes = {name: quantile_codes(frame[name], bins=8) for name in frame}
    duplicate, support = normalized_mutual_information(codes["a"], codes["b"])
    unrelated, _ = normalized_mutual_information(codes["a"], codes["c"])
    assert support == 200
    assert duplicate == 1.0
    assert unrelated < 0.05


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

