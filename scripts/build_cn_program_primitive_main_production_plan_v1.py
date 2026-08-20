from __future__ import annotations

import argparse, hashlib, json
from pathlib import Path
from typing import Any, Mapping

from scripts.run_cn_program_optimizer_large_fresh_v3 import (
    _checkpoint_arm_v3,
    ENHANCED_TEMPLATES,
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
    UNIFORM_CONTROL,
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

MACRO_COUNT=5
CHECKPOINT_SIZE=24
RECORDS_PER_MACRO=168
TOTAL_RECORDS=MACRO_COUNT*RECORDS_PER_MACRO
MIN_DISTINCT_BASE_GROUPS=16
MAX_VARIANTS_PER_BASE_GROUP=4
QUALIFICATION_PAYLOAD='0a976836d897f6f432a973b431c098f4c04fd91f3caba3e30ee7284e254edc5c'
STATS_PAYLOAD='fb5c53287eaaec0b6bfb3dddf6ba0f2846a64b61e888503b2fc00a2b426f7b1d'

def read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p:Path)->str:
 h=hashlib.sha256();
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1<<20),b''): h.update(b)
 return h.hexdigest()
def verify_self(p:Mapping[str,Any],field:str,label:str)->str:
 body=dict(p); claimed=str(body.pop(field,''))
 if not claimed or stable_hash(body)!=claimed: raise RuntimeError(f'{label} self-hash drift')
 return claimed

def build(root:Path)->dict[str,Any]:
 root=root.resolve()
 supply_path=root/'runtime/run_plans/cn_program_primitive_main_fresh_supply_134227f_20260820.json'; supply=read(supply_path); supply_hash=verify_self(supply,'audit_payload_sha256','fresh supply')
 if supply.get('status')!='ZERO_FINANCIAL_PRIMITIVE_MAIN_FRESH_SUPPLY_READY' or int(supply.get('effective_spent_exact_count') or 0)!=6734 or int(supply.get('fresh_unique_count') or 0)!=3576 or int(supply.get('raw_ordinal_offset') or -1)!=1536 or bool(supply.get('candidate_evaluation_executed')) or bool(supply.get('stage_c_financial_labels_read_by_builder')) or bool(supply.get('stage_d_financial_labels_read_by_builder')):
  raise RuntimeError('fresh supply contract drift')
 q_path=root/'runtime/run_plans/cn_program_primitive_main_search_qualification_v1.json'; q=read(q_path); q_hash=verify_self(q,'qualification_payload_sha256','primitive qualification')
 if q_hash!=QUALIFICATION_PAYLOAD or q.get('status')!='PRIMITIVE_LOCAL_MAIN_SEARCH_QUALIFIED_V1' or q.get('decision')!='PRIMITIVE_PRIMARY_UNIFORM_RESERVE_EVOLUTION_CHALLENGER' or q.get('execution_authorized_by_qualification') is not False or q.get('old_v2_catalog_launch_authorized') is not False:
  raise RuntimeError('primitive qualification drift')
 stats_path=root/'runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json'; stats=read(stats_path); stats_hash=verify_self(stats,'stats_payload_sha256','primitive stats')
 if stats_hash!=STATS_PAYLOAD or int(stats.get('unique_program_exact_count') or 0)!=1392: raise RuntimeError('primitive stats drift')
 per={str(k):int(v) for k,v in dict(supply.get('per_template_fresh') or {}).items()}
 if set(per)!=set(ENHANCED_TEMPLATES) or min(per.values())<120: raise RuntimeError('fresh supply template capacity drift')
 rotations={}
 for macro in range(MACRO_COUNT):
  arms=[_checkpoint_arm_v3(macro,i) for i in range(len(ENHANCED_TEMPLATES))]
  rotations[str(macro)]={'primitive':arms.count(PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1),'uniform':arms.count(UNIFORM_CONTROL),'evolution':arms.count(CATALOG_TYPED_EVOLUTION_PROGRAM_V2),'arms':arms}
  if [rotations[str(macro)][k] for k in ('primitive','uniform','evolution')]!=[5,1,1]: raise RuntimeError('V3 rotation drift')
 payload={
  'schema_version':'cn_program_primitive_main_production_plan_v1',
  'status':'PRIMITIVE_MAIN_PRODUCTION_PLAN_FROZEN_NOT_RUN',
  'evaluation_data_role':'DEVELOPMENT_ONLY',
  'fresh_supply':{'relative_path':'runtime/run_plans/cn_program_primitive_main_fresh_supply_134227f_20260820.json','file_sha256':sha(supply_path),'payload_sha256':supply_hash,'raw_ordinal_offset':1536,'raw_records_per_template':512,'effective_spent_exact_count':6734,'effective_spent_exact_identities_sha256':str(supply['effective_spent_exact_identities_sha256']),'fresh_unique_count':3576,'fresh_exact_identities_sha256':str(supply['fresh_exact_identities_sha256']),'per_template_fresh':per},
  'search_authority':{'qualification_relative_path':'runtime/run_plans/cn_program_primitive_main_search_qualification_v1.json','qualification_file_sha256':sha(q_path),'qualification_payload_sha256':q_hash,'decision':q['decision'],'primitive_stats_relative_path':'runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json','primitive_stats_file_sha256':sha(stats_path),'primitive_stats_payload_sha256':stats_hash,'stage_c_results_in_stats':0,'stage_d_results_in_stats':0},
  'search_design':{'template_order':list(ENHANCED_TEMPLATES),'macro_count':MACRO_COUNT,'checkpoint_batch_size':CHECKPOINT_SIZE,'records_per_macro':RECORDS_PER_MACRO,'hard_cap_logical_records':TOTAL_RECORDS,'primitive_checkpoints_per_macro':5,'uniform_reserve_checkpoints_per_macro':1,'typed_evolution_challenger_checkpoints_per_macro':1,'rotation_by_macro':rotations,'minimum_distinct_base_groups_per_template':MIN_DISTINCT_BASE_GROUPS,'maximum_variants_per_base_group_per_template':MAX_VARIANTS_PER_BASE_GROUP,'template_order_or_budget_changes_after_results':'FORBIDDEN','primitive_online_retraining':False,'primitive_cross_campaign_stats_mutation':False},
  'stopping_contract':{'hard_cap_logical_records':TOTAL_RECORDS,'minimum_macros_before_value_collapse_test':4,'lookback_macros':2,'productive_efficiency_collapse_max':0.15,'new_behavior_pair_rate_collapse_max':0.05,'value_collapse_rule':'BOTH_THRESHOLDS_TRUE_IN_EACH_OF_LAST_TWO_MACROS','automatic_extension_beyond_hard_cap':False,'successor_requires_new_fresh_interval_and_new_authorization':True},
  'resource_contract':{'profile':'SEARCH_DUAL_24','cpu_threads':24,'primary_executor_workers':24,'pre_evaluation_fallback_executor_workers':16,'resource_canary_required_before_first_candidate_evaluation':True,'resource_canary_financial_candidate_evaluation':False},
  'restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0},
  'validation_feedback_used':False,'promotion_authorized':False,'oos_authority':'NONE','automatic_successor_authorized':False,'candidate_evaluation_executed':False,
 }
 payload['plan_payload_sha256']=stable_hash(payload); return payload

def main(argv=None)->int:
 p=argparse.ArgumentParser(); p.add_argument('--repo-root',type=Path,default=Path(__file__).resolve().parents[1]); p.add_argument('--output',type=Path,default=Path('runtime/run_plans/cn_program_primitive_main_production_plan_v1.json')); a=p.parse_args(argv)
 payload=build(a.repo_root); out=a.output if a.output.is_absolute() else a.repo_root/a.output; out.parent.mkdir(parents=True,exist_ok=True); out.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8'); print(json.dumps({'status':payload['status'],'records':TOTAL_RECORDS,'plan_payload_sha256':payload['plan_payload_sha256'],'fresh':payload['fresh_supply']['fresh_unique_count'],'spent':payload['fresh_supply']['effective_spent_exact_count']},sort_keys=True)); return 0
if __name__=='__main__': raise SystemExit(main())
