from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
from our_system_phase2.services.unified_capability_registry import stable_hash
ROOT=Path(__file__).resolve().parents[1]
SUPPLY=ROOT/'runtime/run_plans/cn_program_primitive_market_successor_v2_fresh_supply_20260821.json'
V1_AUDIT=ROOT/'runtime/run_plans/cn_program_primitive_market_successor_v1_independent_audit_c38fe0a_20260821.json'
PARITY=ROOT/'runtime/run_plans/cn_program_primitive_market_successor_v1_acceleration_full_parity_20260821.json'
ACCEL=ROOT/'runtime/run_plans/cn_program_primitive_market_successor_acceleration_accuracy_audit_20260821.json'
OUT=ROOT/'runtime/run_plans/cn_program_primitive_market_successor_v2_plan.json'
P='PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1';U='UNIFORM_CONTROL';E='CATALOG_TYPED_EVOLUTION_PROGRAM_V2'
SCHEDULE=[
 {'checkpoint':0,'template':'BASE_MARKET','arm':P,'role':'CORE'},
 {'checkpoint':1,'template':'BASE_MARKET_EVENT','arm':P,'role':'CORE'},
 {'checkpoint':2,'template':'BASE_EVENT','arm':P,'role':'REALLOCATED_EXPLORATION'},
 {'checkpoint':3,'template':'BASE_MARKET','arm':U,'role':'CORE_CONTROL'},
 {'checkpoint':4,'template':'BASE_MARKET_EVENT','arm':E,'role':'CORE_CONTROL'},
 {'checkpoint':5,'template':'BASE_MARKET','arm':P,'role':'CORE'},
 {'checkpoint':6,'template':'BASE_MARKET_EVENT','arm':P,'role':'CORE'},
 {'checkpoint':7,'template':'BASE_TEMPORAL_MARKET_EVENT','arm':P,'role':'REALLOCATED_EXPLORATION'},
 {'checkpoint':8,'template':'BASE_MARKET','arm':E,'role':'CORE_CONTROL'},
 {'checkpoint':9,'template':'BASE_MARKET_EVENT','arm':U,'role':'CORE_CONTROL'},
 {'checkpoint':10,'template':'BASE_MARKET','arm':P,'role':'CORE'},
 {'checkpoint':11,'template':'BASE_MARKET_EVENT','arm':P,'role':'CORE'},
 {'checkpoint':12,'template':'BASE_EVENT','arm':P,'role':'REALLOCATED_EXPLORATION'},
 {'checkpoint':13,'template':'BASE_TEMPORAL_MARKET_EVENT','arm':P,'role':'REALLOCATED_EXPLORATION'},
 {'checkpoint':14,'template':'BASE_EVENT','arm':P,'role':'REALLOCATED_EXPLORATION'},
]
def read(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def selfhash(p,field):
 x=read(p);b=dict(x);c=str(b.pop(field,''))
 if not c or stable_hash(b)!=c:raise RuntimeError(f'self hash drift:{p}')
 return x,c
def build():
 s,sh=selfhash(SUPPLY,'audit_payload_sha256');v1,v1h=selfhash(V1_AUDIT,'audit_payload_sha256');acc,acch=selfhash(ACCEL,'audit_payload_sha256');par=read(PARITY)
 if s.get('status')!='ZERO_FINANCIAL_PRIMITIVE_MARKET_SUCCESSOR_V2_FRESH_SUPPLY_READY' or int(s['effective_spent_exact_count'])!=7934 or int(s['fresh_unique_count'])!=3139 or int(s['raw_ordinal_offset'])!=2560:raise RuntimeError('v2 supply drift')
 if v1.get('status')!='PASS_INDEPENDENT_TERMINAL_AUDIT' or v1['closure_payload_sha256']!='c6785ea4739499b123f471cf707760f92f9e5db42df420d7064f9dd1c48f2952':raise RuntimeError('v1 terminal evidence drift')
 if par.get('status')!='PASS_FULL_360_ECONOMIC_PARITY' or int(par.get('economic_drift_count',-1))!=0 or par.get('accelerated_run_classification')!='DIAGNOSTIC_PARITY_REPLAY_ONLY' or par.get('selected_set_reused_for_search_evidence') is not False:raise RuntimeError('full acceleration parity drift')
 if acc.get('status')!='PASS_SUCCESSOR_REAUTH_ELIGIBLE' or acc['decision']['evaluator_pool_lifetime']!='PERSISTENT_RUN_SCOPE' or int(acc['decision']['executor_workers_primary'])!=24:raise RuntimeError('acceleration authority drift')
 if int(s['per_template_fresh']['BASE_MARKET'])<120 or int(s['per_template_fresh']['BASE_MARKET_EVENT'])<120 or int(s['per_template_fresh']['BASE_EVENT'])<72 or int(s['per_template_fresh']['BASE_TEMPORAL_MARKET_EVENT'])<48:raise RuntimeError('v2 target supply insufficient')
 counts={};tc={}
 for row in SCHEDULE:
  row['checkpoint_size']=24;row['start_ordinal']=row['checkpoint']*24;row['template_start_ordinal']=tc.get(row['template'],0);tc[row['template']]=row['template_start_ordinal']+24;counts[(row['template'],row['arm'])]=counts.get((row['template'],row['arm']),0)+24
 if len(SCHEDULE)!=15 or sum(x['checkpoint_size'] for x in SCHEDULE)!=360:raise RuntimeError('v2 schedule geometry drift')
 if counts[('BASE_MARKET',P)]!=72 or counts[('BASE_MARKET_EVENT',P)]!=72 or counts[('BASE_MARKET',U)]!=24 or counts[('BASE_MARKET',E)]!=24 or counts[('BASE_MARKET_EVENT',U)]!=24 or counts[('BASE_MARKET_EVENT',E)]!=24 or counts[('BASE_EVENT',P)]!=72 or counts[('BASE_TEMPORAL_MARKET_EVENT',P)]!=48:raise RuntimeError('v2 allocation drift')
 payload={
  'schema_version':'cn_program_primitive_market_successor_v2_plan_v1','status':'PRIMITIVE_MARKET_SUCCESSOR_V2_PLAN_FROZEN_NOT_RUN','candidate_evaluation_executed':False,'evaluation_data_role':'DEVELOPMENT_ONLY',
  'design_reason':'V1 fresh prospective successor passed all frozen core gates. V2 preserves scorer, primitive stats, evaluator, diversity and core control geometry; only exploration allocation moves from weak BASE_TEMPORAL_EVENT toward V1-productive BASE_EVENT and BASE_TEMPORAL_MARKET_EVENT. The accelerated V1 replay is diagnostic parity only and contributes no new search evidence or spent count.',
  'source_v1_terminal_audit':{'relative_path':'runtime/run_plans/cn_program_primitive_market_successor_v1_independent_audit_c38fe0a_20260821.json','file_sha256':sha(V1_AUDIT),'payload_sha256':v1h,'source_repo_sha':'c38fe0a5d153506ae084c9d4aa0b1dc519d9e021','closure_payload_sha256':v1['closure_payload_sha256'],'records':360,'productive':127,'behavior_pairs':289,'core_primitive_productive_rate':v1['core_primitive']['productive_rate'],'core_controls_productive_rate':v1['core_controls']['productive_rate'],'core_productive_rate_ratio':v1['core_productive_rate_ratio'],'core_primitive_stable_uplift_2of3_rate':v1['core_primitive']['stable_uplift_2of3_rate'],'core_controls_stable_uplift_2of3_rate':v1['core_controls']['stable_uplift_2of3_rate'],'core_stable_uplift_rate_ratio':v1['core_stable_uplift_rate_ratio']},
  'source_v1_acceleration_full_parity':{'relative_path':'runtime/run_plans/cn_program_primitive_market_successor_v1_acceleration_full_parity_20260821.json','file_sha256':sha(PARITY),'payload_sha256':str(par['audit_payload_sha256']),'status':par['status'],'economic_drift_count':0,'selected_physical_overlap_count':360,'accelerated_run_classification':'DIAGNOSTIC_PARITY_REPLAY_ONLY','accelerated_run_search_evidence_authorized':False,'old_wall_seconds':float(par['old_wall_seconds']),'accelerated_wall_seconds':float(par['accelerated_wall_seconds'])},
  'acceleration_accuracy_audit':{'relative_path':'runtime/run_plans/cn_program_primitive_market_successor_acceleration_accuracy_audit_20260821.json','file_sha256':sha(ACCEL),'payload_sha256':acch},
  'fresh_supply':{'relative_path':'runtime/run_plans/cn_program_primitive_market_successor_v2_fresh_supply_20260821.json','file_sha256':sha(SUPPLY),'payload_sha256':sh,'raw_ordinal_offset':2560,'effective_spent_exact_count':7934,'fresh_unique_count':3139,'fresh_exact_identities_sha256':s['fresh_exact_identities_sha256']},
  'search_authority':{'primitive_stats_relative_path':'runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json','primitive_stats_payload_sha256':'fb5c53287eaaec0b6bfb3dddf6ba0f2846a64b61e888503b2fc00a2b426f7b1d','production_feedback_imported_into_primitive_stats':False,'v1_successor_feedback_imported_into_primitive_stats':False,'stage_c_results_in_stats':0,'stage_d_results_in_stats':0,'production_results_in_stats':0,'v1_successor_results_in_stats':0,'diversity_contract_changed':False,'max_variants_per_base_per_template':4},
  'schedule':SCHEDULE,
  'budget':{'checkpoint_size':24,'checkpoint_count':15,'hard_cap_logical_records':360,'primitive_records':264,'uniform_records':48,'typed_evolution_records':48,'core_template_records':240,'reallocated_exploration_records':120},
  'core_templates':['BASE_MARKET','BASE_MARKET_EVENT'],'reallocated_exploration_templates':['BASE_EVENT','BASE_TEMPORAL_MARKET_EVENT'],'removed_exploration_template':'BASE_TEMPORAL_EVENT',
  'prospective_gate':{'core_primitive_productive_rate_min':0.28,'core_primitive_to_combined_core_controls_rate_ratio_min':1.15,'each_core_template_primitive_productive_rate_min':0.25,'core_primitive_uplift_stable_2of3_rate_min':0.20,'core_primitive_to_combined_core_controls_uplift_stable_rate_ratio_min':1.50,'each_core_template_primitive_uplift_stable_2of3_rate_min':0.15,'total_productive_count_min':70,'behavior_pair_rate_min':0.70,'effective_spent_overlap_count_required':0,'restricted_reads_required_zero':True},
  'resource_contract':{'profile':'SEARCH_DUAL_24','primary_executor_workers':24,'fallback_executor_workers':16,'candidate_evaluation_during_canary':False,'evaluator_pool_lifetime':'PERSISTENT_RUN_SCOPE','minimum_records_per_hour_after_first_checkpoint':650.0,'wall_clock_budget_minutes':50,'minimum_free_memory_bytes':24*1024**3,'throughput_enforcement_after_warm_checkpoints':2,'persistent_pool_record_hash_parity_required':True},
  'validation_feedback_used':False,'oos_authority':'NONE','promotion_authorized':False,'automatic_successor_authorized':False,'restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0}}
 payload['plan_payload_sha256']=stable_hash(payload);return payload
def main(argv=None):
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,default=OUT);a=p.parse_args(argv);x=build();a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(x,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8');print(json.dumps({'status':x['status'],'payload':x['plan_payload_sha256'],'records':x['budget']['hard_cap_logical_records'],'spent':x['fresh_supply']['effective_spent_exact_count'],'fresh':x['fresh_supply']['fresh_unique_count']},sort_keys=True))
if __name__=='__main__':main()
