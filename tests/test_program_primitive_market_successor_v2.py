from pathlib import Path
import app
from scripts import run_cn_program_primitive_market_successor_v2 as r
from our_system_phase2.services.project_control_admission import CAMPAIGN_AUTHORIZATION_BOUND_ROUTES
ROOT=Path(__file__).resolve().parents[1]
PLAN=ROOT/'runtime/run_plans/cn_program_primitive_market_successor_v2_plan.json'

def test_v2_route_registered_high_cost_and_authorization_bound():
 assert app.ROUTES['cn-program-primitive-market-successor-v2']=='our_system_phase2.runtime.cn_program_primitive_market_successor_v2'
 assert 'cn-program-primitive-market-successor-v2' in app.HIGH_COST_ROUTE_ACTIONS
 assert 'cn-program-primitive-market-successor-v2' in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES

def test_v2_plan_fresh_spent_and_stats_contract():
 p=r.verify_plan(PLAN)
 assert p['budget']['hard_cap_logical_records']==360
 assert p['fresh_supply']['raw_ordinal_offset']==2560
 assert p['fresh_supply']['effective_spent_exact_count']==7934
 assert p['fresh_supply']['fresh_unique_count']==3139
 assert p['search_authority']['production_results_in_stats']==0
 assert p['search_authority']['v1_successor_results_in_stats']==0
 assert p['search_authority']['v1_successor_feedback_imported_into_primitive_stats'] is False
 assert p['search_authority']['diversity_contract_changed'] is False

def test_v1_fresh_evidence_and_accelerated_duplicate_classification():
 p=r.verify_plan(PLAN)
 assert p['source_v1_terminal_audit']['records']==360
 assert p['source_v1_terminal_audit']['productive']==127
 assert p['source_v1_acceleration_full_parity']['economic_drift_count']==0
 assert p['source_v1_acceleration_full_parity']['selected_physical_overlap_count']==360
 assert p['source_v1_acceleration_full_parity']['accelerated_run_classification']=='DIAGNOSTIC_PARITY_REPLAY_ONLY'
 assert p['source_v1_acceleration_full_parity']['accelerated_run_search_evidence_authorized'] is False

def test_v2_reallocated_exploration_geometry():
 p=r.verify_plan(PLAN);s=p['schedule']
 def n(template,arm=None):
  return sum(x['checkpoint_size'] for x in s if x['template']==template and (arm is None or x['arm']==arm))
 assert n('BASE_MARKET')==120
 assert n('BASE_MARKET_EVENT')==120
 assert n('BASE_EVENT',r.P)==72
 assert n('BASE_TEMPORAL_MARKET_EVENT',r.P)==48
 assert n('BASE_TEMPORAL_EVENT')==0
 assert sum(x['checkpoint_size'] for x in s)==360

def test_v2_template_ordinals_are_campaign_global():
 p=r.verify_plan(PLAN);seen={}
 for row in p['schedule']:
  assert row['template_start_ordinal']==seen.get(row['template'],0)
  asks=r._ask_rows(row)
  assert asks[0]['template_record_ordinal']==row['template_start_ordinal']
  assert asks[-1]['template_record_ordinal']==row['template_start_ordinal']+23
  seen[row['template']]=row['template_start_ordinal']+24

def test_v2_stable_uplift_and_throughput_contract():
 p=r.verify_plan(PLAN);g=p['prospective_gate'];rc=p['resource_contract']
 assert g['core_primitive_uplift_stable_2of3_rate_min']==0.20
 assert g['core_primitive_to_combined_core_controls_uplift_stable_rate_ratio_min']==1.50
 assert g['each_core_template_primitive_uplift_stable_2of3_rate_min']==0.15
 assert rc['evaluator_pool_lifetime']=='PERSISTENT_RUN_SCOPE'
 assert rc['primary_executor_workers']==24 and rc['fallback_executor_workers']==16
 assert rc['minimum_records_per_hour_after_first_checkpoint']==650.0
 assert rc['throughput_enforcement_after_warm_checkpoints']==2
 assert rc['minimum_free_memory_bytes']==24*1024**3

def test_v2_persistent_executor_uses_full_frozen_union():
 authority={'execution_contract_path':'contract','train_field_root':'fields','train_price_root':'prices','price_manifest':{'x':1},'price_manifest_path':'price_manifest','registry_path':'registry','windows':({'window_id':'w'},),'field_manifest_file_sha':'f','field_manifest_payload_sha':'p'}
 fields=('trade_time','code','close','open')
 opts=r._persistent_executor_options(authority,'inputhash',24,fields)
 assert opts['max_workers']==24
 assert opts['initializer'] is r.engine._initialize_worker
 assert opts['initargs'][-1]==fields
 assert opts['initargs'][6]=='inputhash'
