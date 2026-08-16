from __future__ import annotations

import json
from pathlib import Path

from scripts.apply_cn_program_optimizer_d1_transfer_filter_v1 import (
    EXPECTED_FEATURES,
    FILTER_ID,
    _rank_and_mark,
    _score,
    _verify_self_hash,
)

REPO = Path(__file__).resolve().parents[1]
FILTER = REPO / "runtime/run_plans/cn_program_optimizer_d1_transfer_filter_v1_20260817.json"


def _filter() -> dict:
    payload = json.loads(FILTER.read_text(encoding="utf-8-sig"))
    _verify_self_hash(payload, "filter_payload_sha256")
    return payload


def test_frozen_transfer_filter_uses_only_four_economic_features() -> None:
    payload = _filter()
    assert payload["filter_id"] == FILTER_ID
    assert tuple(payload["features"]) == EXPECTED_FEATURES
    assert payload["selection_contract"]["selected_fraction"] == 0.40
    assert payload["selection_contract"]["selector_template_cohort_wave_features"] == "FORBIDDEN"
    assert payload["holdout_reads"] == 0
    assert payload["forward_2026_reads"] == 0
    assert payload["promotion_authorized"] is False


def test_transfer_score_matches_frozen_raw_linear_formula() -> None:
    payload = _filter()
    features = {
        "dev_matched_return": 0.4,
        "dev_matched_reward": 2.0,
        "dev_robust_median": 0.1,
        "dev_lower_tail": -0.05,
    }
    formula = payload["score_formula_raw"]
    expected = float(formula["intercept"]) + sum(
        float(formula["coefficients"][name]) * features[name]
        for name in EXPECTED_FEATURES
    )
    assert abs(_score(features, payload) - expected) < 1e-15


def test_transfer_filter_selects_ceil_top_40_with_exact_identity_tie_break() -> None:
    payload = _filter()
    rows = []
    for exact, ret in (("b", 0.4), ("a", 0.4), ("c", 0.1), ("d", 0.0), ("e", -0.1)):
        rows.append(
            {
                "exact_identity": exact,
                "dev_matched_return": ret,
                "dev_matched_reward": 1.0,
                "dev_robust_median": 0.0,
                "dev_lower_tail": 0.0,
            }
        )
    ordered, selected_count = _rank_and_mark(rows, payload)
    assert selected_count == 2
    assert [row["exact_identity"] for row in ordered[:2]] == ["a", "b"]
    assert {row["exact_identity"] for row in rows if row["transfer_filter_selected"]} == {"a", "b"}
