from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.apply_cn_program_optimizer_d1_transfer_filter_v2 import (
    EXPECTED_FEATURES,
    EXPECTED_WINDOWS,
    FILTER_ID,
    _features_from_result,
    _rank_and_mark,
    _score,
    _verify,
)

REPO=Path(__file__).resolve().parents[1]
FILTER=REPO/"runtime/run_plans/cn_program_optimizer_d1_transfer_filter_v2_20260817.json"

def _filter()->dict:
    p=json.loads(FILTER.read_text(encoding="utf-8-sig")); _verify(p,"filter_payload_sha256"); return p

def test_v2_filter_is_frozen_before_C_and_uses_only_temporal_economics()->None:
    p=_filter()
    assert p["filter_id"]==FILTER_ID
    assert p["status"]=="FROZEN_BEFORE_C_DEVELOPMENT_AND_VALIDATION"
    assert tuple(p["features"])==EXPECTED_FEATURES
    assert tuple(p["development_window_ids"])==EXPECTED_WINDOWS
    assert p["selection_contract"]["selected_fraction"]==0.40
    assert p["model"]["selector_features_used"] is False
    assert p["model"]["template_features_used"] is False
    assert p["model"]["cohort_features_used"] is False
    assert p["holdout_reads"]==0 and p["forward_2026_reads"]==0
    assert p["promotion_authorized"] is False
    contract=p["prospective_C_validation_acceptance"]
    assert contract["filtered_precision_minimum"]==0.35
    assert contract["filtered_minus_unfiltered_precision_minimum"]==0.10
    assert contract["filtered_recall_minimum"]==0.60

def test_v2_score_matches_frozen_raw_formula()->None:
    p=_filter(); f={"dev_matched_return":0.4,"dev_matched_reward":2.0,"dev_window_1_increment":0.1,"dev_window_2_increment":0.2,"dev_window_3_increment":-0.1}
    form=p["score_formula_raw"]; exp=float(form["intercept"])+sum(float(form["coefficients"][k])*f[k] for k in EXPECTED_FEATURES)
    assert abs(_score(f,p)-exp)<1e-15

def test_v2_feature_extraction_requires_exact_development_window_order()->None:
    row={"admission":{"admitted":True,"metrics":{"development_window_ids":list(EXPECTED_WINDOWS)}},"uplift":{"program_credit":{"matched_cumulative_net_return_increment":0.3,"matched_net_reward_increment":1.2,"window_return_increments":[0.1,-0.05,0.25]}}}
    f=_features_from_result(row)
    assert f=={"dev_matched_return":0.3,"dev_matched_reward":1.2,"dev_window_1_increment":0.1,"dev_window_2_increment":-0.05,"dev_window_3_increment":0.25}
    row["admission"]["metrics"]["development_window_ids"]=["development_2","development_1","development_3"]
    with pytest.raises(RuntimeError,match="window identity drift"):
        _features_from_result(row)

def test_v2_filter_selects_ceil_top40_with_exact_tie_break()->None:
    p=_filter(); rows=[]
    for exact,reward in (("b",2.0),("a",2.0),("c",1.0),("d",0.5),("e",0.1)):
        rows.append({"exact_identity":exact,"dev_matched_return":0.0,"dev_matched_reward":reward,"dev_window_1_increment":0.0,"dev_window_2_increment":0.0,"dev_window_3_increment":0.0})
    ordered,k=_rank_and_mark(rows,p)
    assert k==2
    assert [r["exact_identity"] for r in ordered[:2]]==["a","b"]
    assert {r["exact_identity"] for r in rows if r["transfer_filter_v2_selected"]}=={"a","b"}
