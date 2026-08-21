from pathlib import Path
import app
from scripts import run_cn_program_primitive_market_successor_v1 as r
from our_system_phase2.services.candidate_program_proposal_v0 import GENERATION_ARMS
ROOT=Path(__file__).resolve().parents[1]
PLAN=ROOT/'runtime/run_plans/cn_program_primitive_market_successor_plan_v1.json'

def test_route_registered_high_cost():
 assert app.ROUTES['cn-program-primitive-market-successor-v1']=='our_system_phase2.runtime.cn_program_primitive_market_successor_v1'
 assert 'cn-program-primitive-market-successor-v1' in app.HIGH_COST_ROUTE_ACTIONS

def test_plan_is_frozen_360_and_stats_unchanged():
 p=r.verify_plan(PLAN)
 assert p['budget']['hard_cap_logical_records']==360
 assert p['budget']['primitive_records']==264
 assert p['budget']['uniform_records']==48
 assert p['budget']['typed_evolution_records']==48
 assert p['search_authority']['production_results_in_stats']==0
 assert p['search_authority']['diversity_contract_changed'] is False
 assert p['fresh_supply']['effective_spent_exact_count']==7574
 assert p['fresh_supply']['fresh_unique_count']==3430
 assert p['acceleration_accuracy_audit']['status']=='PASS_SUCCESSOR_REAUTH_ELIGIBLE'
 assert p['resource_contract']['evaluator_pool_lifetime']=='PERSISTENT_RUN_SCOPE'
 assert p['resource_contract']['primary_executor_workers']==24
 assert p['resource_contract']['fallback_executor_workers']==16
 assert p['resource_contract']['minimum_records_per_hour_after_first_checkpoint']==650.0

def test_schedule_core_and_exploration_geometry():
 p=r.verify_plan(PLAN); s=p['schedule']
 assert len(s)==15
 assert sum(x['checkpoint_size'] for x in s)==360
 assert sum(x['checkpoint_size'] for x in s if x['role']=='CORE')==144
 assert sum(x['checkpoint_size'] for x in s if x['role']=='CORE_CONTROL')==96
 assert sum(x['checkpoint_size'] for x in s if x['role']=='EXPLORATION')==120
 assert sum(x['checkpoint_size'] for x in s if x['template']=='BASE_MARKET')==120
 assert sum(x['checkpoint_size'] for x in s if x['template']=='BASE_MARKET_EVENT')==120
 assert all(x['arm'] in GENERATION_ARMS for x in s)

def test_template_ordinals_are_campaign_global_per_template():
 p=r.verify_plan(PLAN); seen={}
 for row in p['schedule']:
  assert row['template_start_ordinal']==seen.get(row['template'],0)
  asks=r._ask_rows(row)
  assert asks[0]['template_record_ordinal']==row['template_start_ordinal']
  assert asks[-1]['template_record_ordinal']==row['template_start_ordinal']+23
  seen[row['template']]=row['template_start_ordinal']+24

def test_core_gate_is_prospective_and_non_oos():
 p=r.verify_plan(PLAN); g=p['prospective_gate']
 assert g['core_primitive_productive_rate_min']==0.28
 assert g['effective_spent_overlap_count_required']==0
 assert p['validation_feedback_used'] is False
 assert p['oos_authority']=='NONE'
 assert p['automatic_successor_authorized'] is False


def test_persistent_executor_uses_full_frozen_field_union():
 authority={'execution_contract_path':'contract','train_field_root':'fields','train_price_root':'prices','price_manifest':{'x':1},'price_manifest_path':'price_manifest','registry_path':'registry','windows':({'window_id':'w'},),'field_manifest_file_sha':'f','field_manifest_payload_sha':'p'}
 fields=('trade_time','code','close','open','ctx_sent_uplimit_num')
 opts=r._persistent_executor_options(authority,'inputhash',24,fields)
 assert opts['max_workers']==24
 assert opts['initializer'] is r.engine._initialize_worker
 assert opts['initargs'][-1]==fields
 assert opts['initargs'][6]=='inputhash'

def test_input_binding_declares_persistent_run_scope_executor():
 source=(ROOT/'scripts/run_cn_program_primitive_market_successor_v1.py').read_text(encoding='utf-8')
 assert "'evaluator_pool_lifetime':'PERSISTENT_RUN_SCOPE'" in source
 assert "'executor_lifetime':'PERSISTENT_RUN_SCOPE'" in source
 assert 'evaluator_telemetry.json' in source


def test_uplift_stable_metric_requires_productive_and_two_positive_windows():
 row={'absolute_admission':{'admitted':True},'enhancer_credit':{'program_credit':{'matched_cumulative_net_return_increment':0.1,'matched_net_reward_increment':0.2,'cross_window_positive_increment_count':2}}}
 assert r._uplift_stable_2of3(row) is True
 row['enhancer_credit']['program_credit']['cross_window_positive_increment_count']=1
 assert r._uplift_stable_2of3(row) is False
 row['enhancer_credit']['program_credit']['cross_window_positive_increment_count']=3
 row['absolute_admission']['admitted']=False
 assert r._uplift_stable_2of3(row) is False

def test_plan_freezes_prospective_uplift_stability_gates():
 p=r.verify_plan(PLAN);g=p['prospective_gate'];e=p['production_evidence']
 assert g['core_primitive_uplift_stable_2of3_rate_min']==0.20
 assert g['core_primitive_to_combined_core_controls_uplift_stable_rate_ratio_min']==1.50
 assert g['each_core_template_primitive_uplift_stable_2of3_rate_min']==0.15
 assert e['primitive_core_market']['uplift_stable_2of3']==60
 assert e['combined_core_controls']['uplift_stable_2of3']==4
