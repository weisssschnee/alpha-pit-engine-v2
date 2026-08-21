from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from our_system_phase2.services.unified_capability_registry import stable_hash

ROOT=Path(__file__).resolve().parents[1]
SUPPLY=ROOT/'runtime/run_plans/cn_program_primitive_market_successor_fresh_supply_20260821.json'
OUT=ROOT/'runtime/run_plans/cn_program_primitive_market_successor_plan_v1.json'
P='PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1'; U='UNIFORM_CONTROL'; E='CATALOG_TYPED_EVOLUTION_PROGRAM_V2'
SCHEDULE=[
 {'checkpoint':0,'template':'BASE_MARKET','arm':P,'role':'CORE'},
 {'checkpoint':1,'template':'BASE_MARKET_EVENT','arm':P,'role':'CORE'},
 {'checkpoint':2,'template':'BASE_EVENT','arm':P,'role':'EXPLORATION'},
 {'checkpoint':3,'template':'BASE_MARKET','arm':U,'role':'CORE_CONTROL'},
 {'checkpoint':4,'template':'BASE_MARKET_EVENT','arm':E,'role':'CORE_CONTROL'},
 {'checkpoint':5,'template':'BASE_MARKET','arm':P,'role':'CORE'},
 {'checkpoint':6,'template':'BASE_MARKET_EVENT','arm':P,'role':'CORE'},
 {'checkpoint':7,'template':'BASE_TEMPORAL_EVENT','arm':P,'role':'EXPLORATION'},
 {'checkpoint':8,'template':'BASE_MARKET','arm':E,'role':'CORE_CONTROL'},
 {'checkpoint':9,'template':'BASE_MARKET_EVENT','arm':U,'role':'CORE_CONTROL'},
 {'checkpoint':10,'template':'BASE_MARKET','arm':P,'role':'CORE'},
 {'checkpoint':11,'template':'BASE_MARKET_EVENT','arm':P,'role':'CORE'},
 {'checkpoint':12,'template':'BASE_TEMPORAL_MARKET_EVENT','arm':P,'role':'EXPLORATION'},
 {'checkpoint':13,'template':'BASE_EVENT','arm':P,'role':'EXPLORATION'},
 {'checkpoint':14,'template':'BASE_TEMPORAL_EVENT','arm':P,'role':'EXPLORATION'},
]

def read(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()

def build()->dict:
 s=read(SUPPLY); body=dict(s); claim=str(body.pop('audit_payload_sha256',''))
 if not claim or stable_hash(body)!=claim or s.get('status')!='ZERO_FINANCIAL_PRIMITIVE_MARKET_SUCCESSOR_FRESH_SUPPLY_READY': raise RuntimeError('successor supply drift')
 if int(s['effective_spent_exact_count'])!=7574 or int(s['fresh_unique_count'])!=3430 or int(s['raw_ordinal_offset'])!=2048: raise RuntimeError('successor supply geometry drift')
 for t in ('BASE_MARKET','BASE_MARKET_EVENT'):
  if int(s['per_template_fresh'][t])!=512: raise RuntimeError(f'core supply drift:{t}')
 counts={}; template_counts={}
 for row in SCHEDULE:
  row['checkpoint_size']=24; row['start_ordinal']=row['checkpoint']*24
  row['template_start_ordinal']=template_counts.get(row['template'],0); template_counts[row['template']]=row['template_start_ordinal']+24
  key=(row['template'],row['arm']); counts[key]=counts.get(key,0)+24
 if len(SCHEDULE)!=15 or sum(x['checkpoint_size'] for x in SCHEDULE)!=360: raise RuntimeError('schedule geometry drift')
 if counts[('BASE_MARKET',P)]!=72 or counts[('BASE_MARKET_EVENT',P)]!=72 or counts[('BASE_MARKET',U)]!=24 or counts[('BASE_MARKET',E)]!=24 or counts[('BASE_MARKET_EVENT',U)]!=24 or counts[('BASE_MARKET_EVENT',E)]!=24: raise RuntimeError('core control geometry drift')
 payload={
  'schema_version':'cn_program_primitive_market_successor_plan_v1',
  'status':'PRIMITIVE_MARKET_SUCCESSOR_PLAN_FROZEN_NOT_RUN',
  'candidate_evaluation_executed':False,
  'evaluation_data_role':'DEVELOPMENT_ONLY',
  'design_reason':'Production search showed strong template heterogeneity: unchanged Primitive scored 42/96 productive on BASE_MARKET and 36/96 on BASE_MARKET_EVENT, versus 43/408 across all other Primitive templates and 0/72 on BASE_TEMPORAL. This successor changes only budget allocation; scorer, primitive stats, evaluator, diversity contract and component universe remain frozen.',
  'production_evidence':{
   'source_terminal_evidence_commit':'03bc18b72eabfb075bf60108a4f8c666ffc4c945',
   'total_records':840,'total_productive':162,
   'primitive_total':{'evaluated':600,'productive':121,'rate':121/600},
   'uniform_total':{'evaluated':120,'productive':20,'rate':20/120},
   'typed_evolution_total':{'evaluated':120,'productive':21,'rate':21/120},
   'primitive_core_market':{'evaluated':192,'productive':78,'rate':78/192,'base_market':{'evaluated':96,'productive':42},'base_market_event':{'evaluated':96,'productive':36}},
   'primitive_noncore':{'evaluated':408,'productive':43,'rate':43/408},
   'primitive_base_temporal':{'evaluated':72,'productive':0,'rate':0.0},
   'production_labels_used_to_mutate_scorer':False,
  },
  'fresh_supply':{'relative_path':'runtime/run_plans/cn_program_primitive_market_successor_fresh_supply_20260821.json','file_sha256':sha(SUPPLY),'payload_sha256':claim,'raw_ordinal_offset':2048,'effective_spent_exact_count':7574,'fresh_unique_count':3430,'fresh_exact_identities_sha256':s['fresh_exact_identities_sha256']},
  'search_authority':{
   'primitive_stats_relative_path':'runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json',
   'primitive_stats_payload_sha256':'fb5c53287eaaec0b6bfb3dddf6ba0f2846a64b61e888503b2fc00a2b426f7b1d',
   'production_feedback_imported_into_primitive_stats':False,
   'stage_c_results_in_stats':0,'stage_d_results_in_stats':0,'production_results_in_stats':0,
   'diversity_contract_changed':False,'max_variants_per_base_per_template':4,
  },
  'schedule':SCHEDULE,
  'budget':{'checkpoint_size':24,'checkpoint_count':15,'hard_cap_logical_records':360,'primitive_records':264,'uniform_records':48,'typed_evolution_records':48,'core_template_records':240,'exploration_records':120},
  'core_templates':['BASE_MARKET','BASE_MARKET_EVENT'],
  'exploration_templates':['BASE_EVENT','BASE_TEMPORAL_EVENT','BASE_TEMPORAL_MARKET_EVENT'],
  'excluded_from_successor':['BASE_TEMPORAL','BASE_TEMPORAL_MARKET'],
  'prospective_gate':{
   'core_primitive_productive_rate_min':0.28,
   'core_primitive_to_combined_core_controls_rate_ratio_min':1.15,
   'each_core_template_primitive_productive_rate_min':0.25,
   'total_productive_count_min':70,
   'behavior_pair_rate_min':0.70,
   'effective_spent_overlap_count_required':0,
   'restricted_reads_required_zero':True,
  },
  'resource_contract':{'profile':'SEARCH_DUAL_24','primary_executor_workers':24,'fallback_executor_workers':16,'candidate_evaluation_during_canary':False},
  'validation_feedback_used':False,'oos_authority':'NONE','promotion_authorized':False,'automatic_successor_authorized':False,
  'restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0},
 }
 payload['plan_payload_sha256']=stable_hash(payload)
 return payload

def main(argv=None):
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,default=OUT);a=p.parse_args(argv);payload=build();a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8');print(json.dumps({'status':payload['status'],'payload':payload['plan_payload_sha256'],'records':payload['budget']['hard_cap_logical_records'],'core':payload['budget']['core_template_records'],'exploration':payload['budget']['exploration_records']},sort_keys=True))
if __name__=='__main__':main()
