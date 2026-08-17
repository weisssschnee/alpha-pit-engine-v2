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


def test_C_freeze_binds_logical_proposal_to_schedule_before_validation(tmp_path: Path)->None:
    from scripts.freeze_cn_program_optimizer_d1_transfer_C_validation_v2 import freeze as freeze_c
    from our_system_phase2.services.unified_capability_registry import stable_hash

    run=tmp_path/"c_run"
    run.mkdir()
    closure={
        "schema_version":"test",
        "status":"CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_COMPLETE",
        "closed_waves":20,
        "logical_records":140,
        "physical_evaluation_calls":140,
        "restricted_reads":{"validation":0,"holdout":0,"historical_2023":0,"forward_b":0,"forward_2026":0},
    }
    closure["closure_payload_sha256"]=stable_hash(closure)
    (run/"CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_COMPLETE.json").write_text(json.dumps(closure,sort_keys=True),encoding="utf-8")

    first_schedule=None
    for wave in range(20):
        wr=run/f"wave_{wave:03d}"
        wr.mkdir()
        asks=[]; schedules=[]; results=[]
        for slot in range(2):
            exact=(f"{wave:02x}{slot:02x}"+"a"*64)[:64]
            logical=f"logical-{wave}-{slot}"
            kind="SURROGATE_FULL_ACQUISITION" if slot else "UNIFORM"
            asks.append({"exact_identity":exact,"logical_proposal_id":logical,"template_id":"BASE_EVENT","selection_kind":kind})
            body={
                "d1_exact_identity":exact,
                "successor_exact_identity":exact,
                "d1_wave_index":wave,
                "d1_logical_proposal_id":logical,
                "d1_selection_kind":kind,
                "pair_id":f"pair-{wave}-{slot}",
                "primary_program":{"program_id":f"program-{wave}-{slot}"},
                "control_program":{"program_id":f"control-{wave}-{slot}"},
            }
            schedule={**body,"schedule_record_sha256":stable_hash(body)}
            schedules.append(schedule)
            if first_schedule is None: first_schedule=schedule
            results.append({
                "exact_identity":exact,
                "physical_result_hash":f"physical-{wave}-{slot}",
                "source_record_sha256":f"source-{wave}-{slot}",
                "admission":{"admitted":True,"metrics":{"development_window_ids":["development_1","development_2","development_3"]}},
                "uplift":{"program_credit":{"matched_cumulative_net_return_increment":0.1+wave/100+slot/1000,"matched_net_reward_increment":1.0+wave/10+slot/100,"window_return_increments":[0.1+slot/100,0.2+wave/100,0.3-wave/200]}},
            })
        (wr/"logical_asks.jsonl").write_text("".join(json.dumps(x,sort_keys=True)+"\n" for x in asks),encoding="utf-8")
        (wr/"physical_schedules.jsonl").write_text("".join(json.dumps(x,sort_keys=True)+"\n" for x in schedules),encoding="utf-8")
        (wr/"physical_results.jsonl").write_text("".join(json.dumps(x,sort_keys=True)+"\n" for x in results),encoding="utf-8")
        (wr/"wave_manifest.json").write_text(json.dumps({"wave":wave},sort_keys=True),encoding="utf-8")

    out=tmp_path/"freeze_good"
    frozen=freeze_c(run_root=run,filter_path=FILTER,output_root=out)
    assert frozen["status"]=="FROZEN_BEFORE_VALIDATION_ACCESS"
    assert frozen["candidate_count"]==40
    members=[json.loads(line) for line in (out/"validation_candidate_members.jsonl").read_text(encoding="utf-8").splitlines() if line]
    assert all(len(row["development_window_return_increments"])==3 for row in members)

    schedule_path=run/"wave_000"/"physical_schedules.jsonl"
    schedule_rows=[json.loads(line) for line in schedule_path.read_text(encoding="utf-8").splitlines() if line]
    schedule_rows[0]["d1_logical_proposal_id"]="WRONG-LOGICAL"
    body={k:v for k,v in schedule_rows[0].items() if k!="schedule_record_sha256"}
    schedule_rows[0]["schedule_record_sha256"]=stable_hash(body)
    schedule_path.write_text("".join(json.dumps(x,sort_keys=True)+"\n" for x in schedule_rows),encoding="utf-8")
    with pytest.raises(RuntimeError,match="logical/schedule lineage drift"):
        freeze_c(run_root=run,filter_path=FILTER,output_root=tmp_path/"freeze_bad")


def test_prospective_prepare_fails_closed_on_missing_or_wrong_d1_lineage(tmp_path: Path)->None:
    import hashlib
    from scripts.prepare_cn_program_optimizer_d1_transfer_prospective_validation_v1 import _resolve_schedules
    from our_system_phase2.services.unified_capability_registry import stable_hash

    root=tmp_path/"source"
    exact="d"*64
    logical="logical-0"
    selection="SURROGATE_FULL_ACQUISITION"
    manifest_hash=""
    base_body={
        "d1_exact_identity":exact,
        "successor_exact_identity":exact,
        "d1_wave_index":0,
        "d1_logical_proposal_id":logical,
        "d1_selection_kind":selection,
        "pair_id":"pair-0",
        "primary_program":{"program_id":"program-0"},
        "control_program":{"program_id":"control-0"},
    }
    for wave in range(20):
        wr=root/f"wave_{wave:03d}"
        wr.mkdir(parents=True)
        manifest=wr/"wave_manifest.json"
        manifest.write_text(json.dumps({"wave":wave},sort_keys=True),encoding="utf-8")
        if wave==0:
            manifest_hash=hashlib.sha256(manifest.read_bytes()).hexdigest()
        schedule_path=wr/"physical_schedules.jsonl"
        if wave==0:
            row={**base_body,"schedule_record_sha256":stable_hash(base_body)}
            schedule_path.write_text(json.dumps(row,sort_keys=True)+"\n",encoding="utf-8")
        else:
            schedule_path.write_text("",encoding="utf-8")

    def member_for(row:dict)->dict:
        return {
            "source_root":str(root),
            "source_wave":0,
            "source_wave_manifest_sha256":manifest_hash,
            "exact_identity":exact,
            "schedule_record_sha256":row["schedule_record_sha256"],
            "pair_id":"pair-0",
            "program_id":"program-0",
            "control_program_id":"control-0",
            "logical_proposal_id":logical,
            "selection_kind":selection,
        }

    schedule_path=root/"wave_000"/"physical_schedules.jsonl"
    good=json.loads(schedule_path.read_text(encoding="utf-8").strip())
    resolved=_resolve_schedules([member_for(good)])
    assert resolved[0]["d1_logical_proposal_id"]==logical

    missing=dict(good)
    missing.pop("d1_logical_proposal_id")
    missing_body={k:v for k,v in missing.items() if k!="schedule_record_sha256"}
    missing["schedule_record_sha256"]=stable_hash(missing_body)
    schedule_path.write_text(json.dumps(missing,sort_keys=True)+"\n",encoding="utf-8")
    with pytest.raises(RuntimeError,match="logical lineage drift"):
        _resolve_schedules([member_for(missing)])

    wrong=dict(good)
    wrong["d1_selection_kind"]="UNIFORM"
    wrong_body={k:v for k,v in wrong.items() if k!="schedule_record_sha256"}
    wrong["schedule_record_sha256"]=stable_hash(wrong_body)
    schedule_path.write_text(json.dumps(wrong,sort_keys=True)+"\n",encoding="utf-8")
    with pytest.raises(RuntimeError,match="logical lineage drift"):
        _resolve_schedules([member_for(wrong)])
