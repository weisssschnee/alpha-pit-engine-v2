from __future__ import annotations
import json
from pathlib import Path
import app
from scripts import build_cn_program_base_event_tier_a_report_only_validation_authorization_v1 as auth_builder
from scripts import run_cn_program_base_event_tier_a_report_only_validation_v1 as r
from our_system_phase2.runtime import cn_program_base_event_tier_a_report_only_validation_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,CAMPAIGN_AUTHORIZATION_BOUND_ROUTES

ROOT=Path(__file__).resolve().parents[1]
PLAN=ROOT/'runtime/run_plans/cn_program_base_event_tier_a_report_only_validation_plan.json'

def test_tier_a_validation_route_is_high_cost_and_authorization_bound()->None:
    route=runtime.ROUTE_ID
    assert app.ROUTES[route]=='our_system_phase2.runtime.cn_program_base_event_tier_a_report_only_validation_v1'
    assert app.HIGH_COST_ROUTE_ACTIONS[route]=={ACTION_LAUNCH,ACTION_RETRY}
    assert route in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES

def test_tier_a_validation_plan_freezes_five_candidates_two_families_and_eight_fields()->None:
    p=r.verify_plan(PLAN,repo_root=ROOT)
    assert p['status']=='BASE_EVENT_TIER_A_REPORT_ONLY_VALIDATION_PLAN_FROZEN_NOT_RUN'
    assert p['candidate_count']==5 and len(p['family_ids'])==2
    assert sorted(p['family_candidate_counts'].values())==[2,3]
    assert len(p['required_physical_leaf_ids'])==8
    assert p['validation_access_authorized'] is False
    assert p['promotion_authorized'] is False and p['automatic_successor_authorized'] is False
    assert all(p[k]==0 for k in ('holdout_reads','historical_challenge_reads','forward_b_reads','forward_2026_reads'))

def test_tier_a_validation_source_bindings_are_frozen()->None:
    p=r.verify_plan(PLAN,repo_root=ROOT); s=p['source_data']
    assert s['source_contract']['sha256']=='23d756eeda5341c9a87091c3b6a420d6314acdb6399c4bd64ee046df96552ec9'
    assert s['registry']['sha256']=='449fea36daaba8e501bd03d052497b881ac03c601cee701f3ebfe069c7ae61d7'
    assert s['split_manifest']['sha256']=='fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241'
    assert s['public_source_manifest']['sha256']=='0c06baa7c24109ea8a1064a291565ce6352ab2a0ca32f20f6cc257261e25514d'
    assert s['daily_st_source']['sha256']=='7060dd78cde6b826157f10c03541a4c302d11fe3213517236dc893393c071e68'
    assert s['validation_label_manifest']['sha256']=='a7ecab7d11bc2e585a0cdfc63a4b0f08194dd6065b277d7a6e5a9e0f80f92721'

def _summary(fid:str,exact:str,survivor:bool)->dict:
    return {'family_id':fid,'exact_identity':exact,'candidate_survivor':survivor,'validation_productive':survivor,'validation_stable_2of3':survivor}

def test_tier_a_mechanism_support_requires_each_family_survivor()->None:
    p=r.verify_plan(PLAN,repo_root=ROOT); a,b=p['family_ids']
    ok=r._mechanism_metrics(p,[_summary(a,'a1',True),_summary(a,'a2',False),_summary(b,'b1',True)])
    assert ok['status']==r.SUPPORTED and ok['family_support_observed']==2
    fail=r._mechanism_metrics(p,[_summary(a,'a1',True),_summary(b,'b1',False)])
    assert fail['status']==r.NOT_SUPPORTED and fail['family_support_observed']==1

def test_tier_a_runner_reuses_d1_evaluator_core_and_builds_context_after_admission()->None:
    src=(ROOT/'scripts/run_cn_program_base_event_tier_a_report_only_validation_v1.py').read_text(encoding='utf-8-sig')
    assert 'd1._initialize_worker' in src and 'd1._evaluate_one' in src
    assert 'build_cn_core_pack_validation_session_sidecar.py' in src
    assert 'build_cn_validation_session_authority.py' in src
    assert "'promotion_authorized':False" in src

def test_tier_a_authorization_binds_implementation_and_report_only_boundary(tmp_path:Path)->None:
    row=auth_builder.build(ROOT)
    assert row['status']=='BASE_EVENT_TIER_A_REPORT_ONLY_VALIDATION_AUTHORIZED_NOT_RUN'
    assert row['evaluation_role']=='validation' and row['usage']=='REPORT_ONLY_CANDIDATE_TRANSFER'
    assert row['resource_contract']=={'profile':'VALIDATION_DUAL_8','cpu_threads':8,'evaluator_workers':4,'candidate_count':5}
    assert row['oos_authority']=='VALIDATION_REPORT_ONLY_EVIDENCE_ONLY'
    assert row['promotion_authorized'] is False and row['automatic_successor_authorized'] is False
    path=tmp_path/'auth.json'; path.write_text(json.dumps(row,sort_keys=True),encoding='utf-8')
    verified=runtime.verify_authorization(path,repo_root=ROOT)
    assert verified['authorization_payload_sha256']==row['authorization_payload_sha256']
