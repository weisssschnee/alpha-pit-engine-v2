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
