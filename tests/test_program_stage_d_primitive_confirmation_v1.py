from __future__ import annotations
from pathlib import Path
import pytest

import app
from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,ProjectControlDenied
from scripts import run_cn_program_stage_d_primitive_confirmation_v1 as runner
from scripts import run_cn_program_stage_c_system_search_v1 as stagec

REPO=Path(__file__).resolve().parents[1]
PREFREEZE=REPO/'runtime/run_plans/cn_program_stage_d_primitive_confirmation_prefreeze_v1.json'


def _fake_result(candidate:dict)->dict:
    exact=str(candidate['exact_identity'])
    program_id=str(candidate['program_id'])
    return {
        'exact_identity':exact,
        'template_id':str(candidate['template_id']),
        'behavior_pair_identity':f'behavior::{exact}',
        'admission':{
            'policy_id':'CN_SEARCH_V2_ABSOLUTE_ECONOMIC_ADMISSION_V1','record_payload_sha256':f'record::{exact}','pair_id':f'pair::{exact}','program_id':program_id,'control_program_id':f'control::{exact}','admitted':True,'failure_reasons':[],'metrics':{},
        },
        'uplift':{
            'policy_id':'CN_SEARCH_V2_CONDITIONAL_FULL_VS_BASE_UPLIFT_V1','record_payload_sha256':f'record::{exact}','pair_id':f'pair::{exact}','program_id':program_id,'control_program_id':f'control::{exact}','program_credit':{'matched_cumulative_net_return_increment':1.0,'matched_net_reward_increment':1.0},'component_attribution_status':'COMPONENT_ATTRIBUTION_UNIDENTIFIED','component_credits':None,
        },
    }


def test_route_is_high_cost_and_authorization_bound() -> None:
    assert app.ROUTES[runtime.ROUTE_ID]=='our_system_phase2.runtime.cn_program_stage_d_primitive_confirmation_v1'
    assert app.HIGH_COST_ROUTE_ACTIONS[runtime.ROUTE_ID]=={ACTION_LAUNCH,ACTION_RETRY}
    assert runtime.ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_prefreeze_is_disjoint_confirmatory_and_reuses_pre_stage_c_primitive_score() -> None:
    p=runner.verify_prefreeze(PREFREEZE)
    assert p['no_stage_c_label_retraining_or_tuning'] is True
    assert p['primitive_score_source']['stage_c_results_in_stats']==0
    assert p['primitive_score_source']['stats_unique_program_exact_count']==1392
    assert p['expanded_supply']['raw_ordinal_offset']==1024
    assert p['expanded_supply']['effective_spent_exact_count']==4718
    assert p['cohort']['total_records']==2016
    assert p['evaluation']['primary_total_budget']==1008
    assert p['evaluation']['confirmatory_victory_gate']['minimum_distinct_behavior_pair_rate_at_1008']==pytest.approx(2/3)
    assert p['evaluation']['confirmatory_victory_gate']['minimum_distinct_behavior_pair_count_vs_uniform_mean_ratio_at_1008']==0.80


def test_typed_evolution_causal_replay_covers_1008_on_stage_d_geometry() -> None:
    p=runner.verify_prefreeze(PREFREEZE)
    result={str(c['exact_identity']):_fake_result(dict(c)) for c in p['cohort']['candidates']}
    orders=stagec._evolution_orders(p,result)
    assert set(orders)==set(stagec.TEMPLATES)
    assert all(len(order)==144 and len(set(order))==144 for order in orders.values())
    assert sum(len(order) for order in orders.values())==1008


def test_all_equal_synthetic_labels_do_not_false_confirm() -> None:
    p=runner.verify_prefreeze(PREFREEZE)
    result={str(c['exact_identity']):_fake_result(dict(c)) for c in p['cohort']['candidates']}
    curve=runner._curve(p,result)
    assert curve['status']=='PRIMITIVE_LOCAL_LARGE_SCALE_CONFIRMATION_FAIL_STAGE_D'
    assert curve['victory_checks']['primitive_vs_uniform_ratio_1008'] is False


def test_direct_runtime_invocation_is_denied_before_argument_parsing() -> None:
    with pytest.raises(ProjectControlDenied):
        runtime.main([])
