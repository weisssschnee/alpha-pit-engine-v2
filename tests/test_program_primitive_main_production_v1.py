from __future__ import annotations
import json
from pathlib import Path
import app
from scripts import run_cn_program_primitive_main_production_v1 as runner
from scripts.run_cn_program_optimizer_large_fresh_v3 import _checkpoint_arm_v3
from our_system_phase2.services.program_search_primitive_credit_v1 import PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
from our_system_phase2.services.program_search_optimizer_historical_v2 import CATALOG_TYPED_EVOLUTION_PROGRAM_V2
from our_system_phase2.services.program_search_optimizer_v1 import UNIFORM_CONTROL
ROOT=Path(__file__).resolve().parents[1]
PLAN=ROOT/'runtime/run_plans/cn_program_primitive_main_production_plan_v1.json'
def test_route_registered_high_cost():
 assert app.ROUTES['cn-program-primitive-main-production-v1']=='our_system_phase2.runtime.cn_program_primitive_main_production_v1'
 assert 'cn-program-primitive-main-production-v1' in app.HIGH_COST_ROUTE_ACTIONS
def test_plan_is_frozen_fresh_and_840():
 p=runner.verify_plan(PLAN)
 assert p['fresh_supply']['effective_spent_exact_count']==6734
 assert p['fresh_supply']['fresh_unique_count']==3576
 assert p['search_design']['hard_cap_logical_records']==840
 assert p['search_authority']['stage_c_results_in_stats']==0
 assert p['search_authority']['stage_d_results_in_stats']==0
def test_v3_rotation_is_5_1_1_for_all_five_macros():
 for macro in range(5):
  arms=[_checkpoint_arm_v3(macro,i) for i in range(7)]
  assert arms.count(PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1)==5
  assert arms.count(UNIFORM_CONTROL)==1
  assert arms.count(CATALOG_TYPED_EVOLUTION_PROGRAM_V2)==1

def test_production_bandit_uses_v3_primitive_config_contract(monkeypatch):
 captured={}
 class FakeBandit:
  def __init__(self,**kwargs): captured.update(kwargs)
 monkeypatch.setattr(runner,'LargeFreshProgramBanditV3',FakeBandit)
 plan=runner.verify_plan(PLAN)
 result=runner._bandit(plan,{'entries':(), 'metadata':{}},ROOT)
 assert isinstance(result,FakeBandit)
 assert set(captured)=={'campaign_id','entries_by_arm','seeds','primitive_config','evolution_config'}
 assert captured['primitive_config']['metadata_by_exact_identity']=={}
 assert captured['primitive_config']['primitive_stats_payload_sha256']==plan['search_authority']['primitive_stats_payload_sha256']
