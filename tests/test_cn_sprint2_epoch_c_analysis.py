from __future__ import annotations

import pandas as pd

from our_system_phase2.runtime.cn_sprint2_epoch_c_analysis import (
    SEEDS,
    _proposal_union_metrics,
    _triple_overlap,
)


def _seed_frame(seed: str) -> pd.DataFrame:
    shared = [
        {
            "expression": f"shared_{index}",
            "proposal_stage": "fixed",
            "legal": index != 1,
            "exact_identity": f"shared_exact_{index}",
        }
        for index in range(2)
    ]
    adaptive = [
        {
            "expression": f"{seed}_adaptive_{index}",
            "proposal_stage": "adaptive",
            "legal": True,
            "exact_identity": f"{seed}_adaptive_exact_{index}",
        }
        for index in range(2)
    ]
    return pd.DataFrame([*shared, *adaptive])


def test_proposal_union_separates_generated_uniqueness_from_legal_exact() -> None:
    proposals = {seed: _seed_frame(seed) for seed in SEEDS}
    contract = {
        "epoch_budget": {
            "unique_proposal_total": 8,
            "shared_deterministic_backbone": 2,
            "seed_specific_expansion_per_seed": 2,
        }
    }

    result = _proposal_union_metrics(proposals, contract)

    assert result["execution_row_count"] == 12
    assert result["unique_exact_identity_union_count"] == 8
    assert result["unique_proposal_contract_met"] is True
    assert result["shared_backbone"]["contract_met"] is True
    assert result["seed_specific_expansion"]["contract_met"] is True
    assert result["legal_exact_identity_overlap"]["union_count"] == 7
    assert result["typed_gate_rejection_count"] == 3


def test_triple_overlap_reports_pairwise_and_three_way_sets() -> None:
    result = _triple_overlap(
        {
            "seed_a": {"shared", "a"},
            "seed_b": {"shared", "b"},
            "seed_c": {"shared", "c"},
        }
    )

    assert result["union_count"] == 4
    assert result["three_way_intersection_count"] == 1
    assert result["three_way_jaccard"] == 0.25
    assert result["pairwise"]["seed_a__seed_b"]["jaccard"] == 1 / 3
