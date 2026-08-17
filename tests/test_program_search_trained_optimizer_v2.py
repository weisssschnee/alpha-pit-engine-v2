from __future__ import annotations

import math

from our_system_phase2.services.program_search_trained_optimizer_v2 import (
    StructuredExtraTreesReplayV1,
    StructuredMultiHeadSearchV2,
    structural_feature_dict,
)
from scripts.run_cn_program_optimizer_search_core_replay_v2 import _balanced_order


def _row(i: int, *, admitted: bool, productive: bool, template: str = "BASE_TEMPORAL"):
    ret = 0.15 + 0.01 * i if admitted else None
    reward = 0.7 + 0.03 * i if admitted else None
    if admitted and not productive:
        reward = -abs(reward)
    return {
        "exact_identity": f"exact-{i:03d}",
        "source_cohort": "COHORT_SHOULD_NOT_BE_FEATURE",
        "cohort_index": 99,
        "source_order": i,
        "source_wave": i // 7,
        "template_id": template,
        "selection_kind": "SHOULD_NOT_BE_FEATURE",
        "admitted": admitted,
        "productive": productive,
        "relevance_grade": 2 if productive else 1 if admitted else 0,
        "matched_cumulative_net_return_increment": ret,
        "matched_net_reward_increment": reward,
        "base_group_id": f"base-{i % 4}",
        "query_group": f"Q::{template}",
        "structural_genes": {
            "program_template_id": template,
            "base__route_id": "R_GOOD" if productive else "R_BAD",
            "base__skeleton_id": f"S{i % 3}",
            "raw_field_count": str(1 + i % 4),
            "rolling_node_count": str(i % 3),
            "combination_temporal": "AND" if productive else "OR",
        },
    }


def test_structural_feature_surface_excludes_search_history_fields() -> None:
    row = _row(1, admitted=True, productive=True)
    features = structural_feature_dict(row)
    joined = "\n".join(features)
    assert "source_cohort" not in joined
    assert "selection_kind" not in joined
    assert "source_wave" not in joined
    assert "source_order" not in joined
    assert features["gene::raw_field_count"] == 2.0
    assert features["base_group_id"] == "base-1"


def test_multihead_and_v1_fit_and_score_structured_rows() -> None:
    train = [
        _row(i, admitted=(i % 5 != 0), productive=(i % 3 == 0 and i % 5 != 0))
        for i in range(60)
    ]
    test = [_row(100 + i, admitted=True, productive=(i % 2 == 0)) for i in range(8)]
    old = StructuredExtraTreesReplayV1(seed=7).fit(train).score_rows(test)
    new = StructuredMultiHeadSearchV2(seed=7).fit(train).score_rows(test)
    assert len(old) == len(test)
    assert len(new) == len(test)
    assert all(math.isfinite(row["score"]) and row["score"] >= 0.0 for row in old)
    assert all(math.isfinite(row["score"]) and row["score"] >= 0.0 for row in new)
    assert all(0.0 <= row["p_admit"] <= 1.0 for row in new)
    assert all(0.0 <= row["p_productive"] <= 1.0 for row in new)
    assert all("reward_mean" in row and "return_mean" in row for row in new)


def test_balanced_order_round_robins_templates() -> None:
    templates = (
        "BASE_EVENT",
        "BASE_MARKET",
        "BASE_MARKET_EVENT",
        "BASE_TEMPORAL",
        "BASE_TEMPORAL_EVENT",
        "BASE_TEMPORAL_MARKET",
        "BASE_TEMPORAL_MARKET_EVENT",
    )
    rows = []
    scores = []
    for t_index, template in enumerate(templates):
        for local in range(3):
            rows.append(_row(t_index * 10 + local, admitted=True, productive=True, template=template))
            scores.append(float(1000 * t_index + local))
    order = _balanced_order(rows, scores)
    first_round = [rows[index]["template_id"] for index in order[:7]]
    second_round = [rows[index]["template_id"] for index in order[7:14]]
    assert first_round == list(templates)
    assert second_round == list(templates)
    assert len(order) == 21
    assert len(set(order)) == 21
