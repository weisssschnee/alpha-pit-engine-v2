from __future__ import annotations
from pathlib import Path

import app
from scripts import run_cn_program_stage_c_system_search_v1 as runner
from our_system_phase2.runtime import cn_program_stage_c_system_search_v1 as runtime
from our_system_phase2.services.project_control_admission import CAMPAIGN_AUTHORIZATION_BOUND_ROUTES
from our_system_phase2.services.program_search_trained_optimizer_v2 import structural_feature_dict

REPO=Path(__file__).resolve().parents[1]
PREFREEZE=REPO/'runtime/run_plans/cn_program_stage_c_system_search_prefreeze_v1.json'


def test_stage_c_route_is_high_cost_and_authorization_bound() -> None:
    assert app.ROUTES[runtime.ROUTE_ID] == 'our_system_phase2.runtime.cn_program_stage_c_system_search_v1'
    assert runtime.ROUTE_ID in app.HIGH_COST_ROUTE_ACTIONS
    assert runtime.ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_stage_c_prefreeze_is_balanced_and_label_sealed() -> None:
    p=runner.verify_prefreeze(PREFREEZE)
    assert p['stage_c_financial_labels_read_during_freeze'] is False
    assert p['candidate_evaluation_executed'] is False
    assert p['cohort']['total_records']==2016
    assert len(p['cohort']['physical_order'])==2016
    assert p['evaluation']['primary_total_budget']==1008


def test_trained_feature_surface_excludes_search_history() -> None:
    row={'structural_genes':{'program_template_id':'BASE_EVENT','raw_field_count':'3','rolling_node_count':'1','base__route_id':'R'},'base_group_id':'base-x','source_wave':99,'selection_kind':'LEAK'}
    f=structural_feature_dict(row); joined='\n'.join(f)
    assert 'source_wave' not in joined and 'selection_kind' not in joined
    assert f['gene::raw_field_count']==3.0


def test_typed_evolution_causal_replay_covers_1008_on_real_stage_c_geometry() -> None:
    p=runner.verify_prefreeze(PREFREEZE)
    result={}
    for c in p['cohort']['candidates']:
        exact=str(c['exact_identity'])
        result[exact]={
            'exact_identity':exact,
            'template_id':str(c['template_id']),
            'behavior_pair_identity':f'behavior::{exact}',
            'admission':{
                'policy_id':'CN_SEARCH_V2_ABSOLUTE_ECONOMIC_ADMISSION_V1','record_payload_sha256':f'record::{exact}','pair_id':f'pair::{exact}','program_id':str(c['program_id']),'control_program_id':f'control::{exact}','admitted':True,'failure_reasons':[],'metrics':{},
            },
            'uplift':{
                'policy_id':'CN_SEARCH_V2_CONDITIONAL_FULL_VS_BASE_UPLIFT_V1','record_payload_sha256':f'record::{exact}','pair_id':f'pair::{exact}','program_id':str(c['program_id']),'control_program_id':f'control::{exact}','program_credit':{'matched_cumulative_net_return_increment':1.0,'matched_net_reward_increment':1.0},'component_attribution_status':'COMPONENT_ATTRIBUTION_UNIDENTIFIED','component_credits':None,
            },
        }
    curve=runner._curve(p,result)
    assert all(len(v)==144 for v in curve['typed_evolution_orders_by_template'].values())
    assert curve['budgets']['1008']['typed_evolution']['evaluated']==1008
