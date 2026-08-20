from __future__ import annotations

import argparse, json, time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_large_fresh_v1 as large
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as successor
from scripts import run_cn_program_optimizer_tournament_v1 as tournament
from scripts.run_cn_program_optimizer_large_fresh_v3 import _checkpoint_arm_v3
from our_system_phase2.services.program_optimizer_large_fresh_v3 import LargeFreshProgramBanditV3
from our_system_phase2.services.program_search_optimizer_v1 import UNIFORM_CONTROL, normalized_program_gene_identity_v1
from our_system_phase2.services.program_search_primitive_credit_v1 import primitive_program_metadata_v1
from our_system_phase2.services.unified_capability_registry import stable_hash

CAMPAIGN_ID='CN_PROGRAM_PRIMITIVE_MAIN_PRODUCTION_V1'
CAMPAIGN_PROFILE='cn_program_primitive_main_production_v1'
STATUS_COMPLETE='PRIMITIVE_MAIN_PRODUCTION_COMPLETE'
CLOSURE_NAME='CN_PROGRAM_PRIMITIVE_MAIN_PRODUCTION_COMPLETE.json'
CHECKPOINT_SIZE=24
PRIMARY_EXECUTOR_WORKERS=24
FALLBACK_EXECUTOR_WORKERS=16
TEMPLATES=tuple(large.ENHANCED_TEMPLATES)

def _read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding='utf-8-sig'))
def verify_plan(path:Path)->dict[str,Any]:
 p=_read(path); body=dict(p); claim=str(body.pop('plan_payload_sha256',''))
 if not claim or stable_hash(body)!=claim or p.get('status')!='PRIMITIVE_MAIN_PRODUCTION_PLAN_FROZEN_NOT_RUN' or p.get('candidate_evaluation_executed') is not False: raise RuntimeError('PRIMITIVE_MAIN_PRODUCTION_PLAN_DRIFT')
 if int(p['search_design']['hard_cap_logical_records'])!=840 or int(p['fresh_supply']['fresh_unique_count'])!=3576 or int(p['fresh_supply']['effective_spent_exact_count'])!=6734: raise RuntimeError('PRIMITIVE_MAIN_PRODUCTION_PLAN_GEOMETRY_DRIFT')
 return p

def _load_authority(args:argparse.Namespace,*,authorization:Mapping[str,Any],repo_sha:str)->dict[str,Any]:
 prior=dict(authorization['source_prior_exact'])
 return successor._load_authority(args,authorization=authorization,repo_sha=repo_sha,campaign_id=str(authorization['campaign_id']),campaign_profile=str(authorization['campaign_profile']),prior_freeze_payload_sha256=str(prior['payload_sha256']),prior_exact_count=int(prior['count']),prior_exact_identities_sha256=str(prior['exact_identities_sha256']),prior_identity_field=str(prior['identity_field']),input_binding_schema_version='cn_program_primitive_main_production_input_binding_v1',resource_profile_id='SEARCH_DUAL_24',resource_profile_role='SEARCH',maximum_executor_workers=PRIMARY_EXECUTOR_WORKERS)

def _fresh_catalog(plan:Mapping[str,Any],authority:Mapping[str,Any],repo_root:Path)->dict[str,Any]:
 binding=dict(plan['fresh_supply']); supply_path=repo_root/Path(binding['relative_path']); supply=_read(supply_path); body=dict(supply); claim=str(body.pop('audit_payload_sha256',''))
 if claim!=binding['payload_sha256'] or stable_hash(body)!=claim or int(supply['fresh_unique_count'])!=3576: raise RuntimeError('PRIMITIVE_MAIN_FRESH_SUPPLY_DRIFT')
 catalog={t:[] for t in TEMPLATES}; raw_expected={str(r['exact_identity']) for r in supply['fresh_entries']}; raw_by_reservoir={str(r['reservoir_record_sha256']):str(r['exact_identity']) for r in supply['fresh_entries']}
 for row in supply['fresh_entries']:
  reservoir={'schema_version':'cn_joint_program_phase_c_reservoir_record_v0','template_id':str(row['template_id']),'components':dict(row['components']),'combination_policy':dict(row['combination_policy']),'raw_combination_sha256':str(row['raw_combination_sha256']),'reservoir_record_sha256':str(row['reservoir_record_sha256']),'semantic_compile_required_at_selection':True,'semantic_noop_does_not_count_toward_quota':True,'financial_evaluation_executed':False}
  entry=engine._catalog_entry(reservoir,components_by_id=authority['components_by_id'],adapter=authority['adapter'],compiler=authority['compiler'])
  if entry.get('status')!='EXECUTABLE' or stable_hash(dict(entry['program_genes']))!=str(row['exact_identity']): raise RuntimeError('PRIMITIVE_MAIN_FRESH_CATALOG_REBUILD_DRIFT')
  catalog[str(entry['template_id'])].append(entry)
 entries=tournament._program_entries(catalog); slots=tuple(entries[0].genes); catalog_by_exact={}; group_by_exact={}; physical_by_normalized={}
 for t in TEMPLATES:
  for source in catalog[t]:
   norm=normalized_program_gene_identity_v1(dict(source['program_genes']),ordered_slots=slots); raw=stable_hash(dict(source['program_genes']))
   if norm in catalog_by_exact or raw not in raw_expected: raise RuntimeError('PRIMITIVE_MAIN_IDENTITY_ALIAS_DRIFT')
   catalog_by_exact[norm]=source; group_by_exact[norm]=str(source['base_component_id']); physical_by_normalized[norm]=raw
 metadata=primitive_program_metadata_v1(entries=entries,catalog_by_exact=catalog_by_exact)
 if len(entries)!=3576 or set(catalog_by_exact)!={e.exact_identity for e in entries} or len(set(physical_by_normalized.values()))!=3576: raise RuntimeError('PRIMITIVE_MAIN_FRESH_CATALOG_COVERAGE_DRIFT')
 return {'supply':supply,'catalog':catalog,'entries':entries,'catalog_by_exact':catalog_by_exact,'group_by_exact':group_by_exact,'physical_by_normalized':physical_by_normalized,'raw_by_reservoir':raw_by_reservoir,'metadata':metadata}

def _bandit(plan:Mapping[str,Any],fresh:Mapping[str,Any],repo_root:Path)->LargeFreshProgramBanditV3:
 stats_binding=dict(plan['search_authority']); stats_path=repo_root/Path(stats_binding['primitive_stats_relative_path']); stats=_read(stats_path); body=dict(stats); claim=str(body.pop('stats_payload_sha256',''))
 if claim!=stats_binding['primitive_stats_payload_sha256'] or stable_hash(body)!=claim: raise RuntimeError('PRIMITIVE_MAIN_STATS_DRIFT')
 entries=tuple(fresh['entries']); from scripts.run_cn_program_optimizer_large_fresh_v3 import ARMS,SEEDS,EVOLUTION_CONFIG
 primitive_config={'metadata_by_exact_identity':fresh['metadata'],'primitive_stats':stats,'primitive_stats_payload_sha256':claim}
 return LargeFreshProgramBanditV3(campaign_id=CAMPAIGN_ID,entries_by_arm={arm:entries for arm in ARMS},seeds=SEEDS,primitive_config=primitive_config,evolution_config=EVOLUTION_CONFIG)

def _ask_rows(macro:int,template_index:int,checkpoint:int,start:int,arm:str)->list[dict[str,Any]]:
 t=TEMPLATES[template_index]; out=[]
 for local in range(CHECKPOINT_SIZE):
  row={'schema_version':'cn_program_primitive_main_production_ask_v1','macro_index':macro,'optimizer_arm':arm,'generation_arm':arm,'template_id':t,'template_record_ordinal':macro*CHECKPOINT_SIZE+local,'main_record_ordinal':start+local,'checkpoint_ordinal':checkpoint,'campaign_profile':CAMPAIGN_PROFILE,'absolute_admission_head_eligible':True,'conditional_uplift_head_eligible':True,'matched_control_contract_id':'CANDIDATE_PROGRAM_V1_FULL_VS_BASE_MATCHED_CONTROL','program_level_credit_only':True,'component_attribution':'COMPONENT_ATTRIBUTION_UNIDENTIFIED'}; row['ask_record_sha256']=stable_hash(row); out.append(row)
 return out

def _macro_metric(feedback:Sequence[Mapping[str,Any]],records:Sequence[Mapping[str,Any]],seen:set[str])->dict[str,Any]:
 block=large._metric_block(feedback); new=[]; observed=[]
 for record in records:
  ident=large._behavior_pair_identity(record)
  if ident is None: continue
  observed.append(ident)
  if ident not in seen: seen.add(ident); new.append(ident)
 from scripts.run_cn_program_optimizer_large_fresh_v3 import ARMS
 per_arm={arm:large._metric_block([r for r in feedback if str(r.get('generation_arm'))==arm]) for arm in ARMS}
 return {**block,'behavior_pair_observations':len(observed),'new_behavior_pair_count':len(new),'new_behavior_pair_rate':len(new)/block['evaluated'] if block['evaluated'] else 0.0,'cumulative_behavior_pair_count':len(seen),'per_arm':per_arm,'per_template':{t:large._metric_block([r for r in feedback if str(r.get('template_id'))==t]) for t in TEMPLATES}}

def _full_field_union(fresh:Mapping[str,Any],authority:Mapping[str,Any])->tuple[str,...]:
 fields=set(); ordinal=0
 for t in TEMPLATES:
  for source in fresh['catalog'][t]:
   ask={'template_id':t,'main_record_ordinal':ordinal,'checkpoint_ordinal':0,'template_record_ordinal':ordinal,'generation_arm':UNIFORM_CONTROL,'ask_record_sha256':'RESOURCE_ONLY','matched_control_contract_id':'CANDIDATE_PROGRAM_V1_FULL_VS_BASE_MATCHED_CONTROL','absolute_admission_head_eligible':True,'conditional_uplift_head_eligible':True}
   decision={'adaptive_template_credit_used':False,'selection_decision_sha256':'RESOURCE_ONLY'}
   s=engine._schedule_record(ask,source,decision,components_by_id=authority['components_by_id'],adapter=authority['adapter'],compiler=authority['compiler']); fields.update(engine._checkpoint_field_columns([s])); ordinal+=1
 return tuple(sorted(fields))

def prefinancial_rehearsal(args:argparse.Namespace,*,authorization:Mapping[str,Any],repo_sha:str)->dict[str,Any]:
 plan=verify_plan(args.production_plan); root=Path(__file__).resolve().parents[1]; authority=_load_authority(args,authorization=authorization,repo_sha=repo_sha); fresh=_fresh_catalog(plan,authority,root); bandit=_bandit(plan,fresh,root); before=bandit.snapshot(); state=engine._selection_state(); asks=_ask_rows(0,0,0,0,_checkpoint_arm_v3(0,0)); schedules,_=tournament._select_checkpoint(asks,catalog=fresh['catalog'],bandit=bandit,state=state,components_by_id=authority['components_by_id'],adapter=authority['adapter'],compiler=authority['compiler'],prior_exact_identities=())
 if bandit.snapshot()!=before or len(schedules)!=24: raise RuntimeError('PRIMITIVE_MAIN_PREFLIGHT_PREVIEW_DRIFT')
 fields=_full_field_union(fresh,authority)
 return {'status':'ZERO_FINANCIAL_PRIMITIVE_MAIN_PRODUCTION_PREFLIGHT_READY','fresh_catalog_count':len(fresh['entries']),'preview_asks':24,'preview_state_unchanged':True,'field_column_count':len(fields),'field_columns':list(fields),'field_columns_sha256':stable_hash(list(fields)),'candidate_evaluation_executed':False,'validation_reads':0,'holdout_reads':0,'forward_2026_reads':0}

def run(args:argparse.Namespace,*,admission:Mapping[str,Any],authorization:Mapping[str,Any])->dict[str,Any]:
 repo_sha=str(admission['repo_sha']); repo_root=Path(__file__).resolve().parents[1]; plan=verify_plan(args.production_plan); authority=_load_authority(args,authorization=authorization,repo_sha=repo_sha); fresh=_fresh_catalog(plan,authority,repo_root); bandit=_bandit(plan,fresh,repo_root)
 root=args.output_root.resolve()
 if not root.is_dir() or not (root/'.project_control_execution').is_dir() or {p.name for p in root.iterdir()}!={'.project_control_execution'}: raise RuntimeError('PRIMITIVE_MAIN_ADMITTED_ROOT_NOT_CLEAN')
 fields=_full_field_union(fresh,authority); input_binding=engine._self_hashed({'schema_version':'cn_program_primitive_main_production_input_binding_v1','repo_sha':repo_sha,'authorization_payload_sha256':authorization['authorization_payload_sha256'],'production_plan_payload_sha256':plan['plan_payload_sha256'],'fresh_supply_payload_sha256':plan['fresh_supply']['payload_sha256'],'effective_spent_exact_count':6734,'effective_spent_exact_identities_sha256':plan['fresh_supply']['effective_spent_exact_identities_sha256'],'fresh_exact_identities_sha256':plan['fresh_supply']['fresh_exact_identities_sha256'],'primitive_stats_payload_sha256':plan['search_authority']['primitive_stats_payload_sha256'],'qualification_payload_sha256':plan['search_authority']['qualification_payload_sha256'],'field_columns_sha256':stable_hash(list(fields)),'evaluation_data_role':'DEVELOPMENT_ONLY','restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0}},'input_binding_sha256'); large._write_json(root/'input_binding.json',input_binding)
 input_hash=str(input_binding['input_binding_sha256']); workers=None; decision=None
 for w in (24,16):
  try: receipt=large._resource_canary(authority,input_hash,w,fields); workers=w; decision={'status':'PASS','selected_executor_workers':w,'attempts':[receipt],'fallback_applied':w!=24,'field_columns':list(fields),'field_columns_sha256':stable_hash(list(fields))}; break
  except Exception as exc: decision={'status':'FAIL','error':f'{type(exc).__name__}:{exc}'}
 if workers is None: raise RuntimeError(f'PRIMITIVE_MAIN_RESOURCE_CANARY_FAILED:{decision}')
 large._write_json(root/'resource_canary.json',decision); large._write_json(root/'optimizer_state_genesis.json',bandit.snapshot()); state=engine._selection_state(); large._write_json(root/'selection_state_genesis.json',large._selection_state_record(state))
 previous='GENESIS'; checkpoint=0; next_ord=0; selected_norm=set(); selected_raw=set(); seen_pairs=set(); all_feedback=[]; all_records=[]; all_schedules=[]; macros=[]; productive=[]; start=time.perf_counter(); stop_reason='HARD_CAP_REACHED'; physical_by_norm=fresh['physical_by_normalized']; fresh_raw=set(physical_by_norm.values())
 for macro in range(int(plan['search_design']['macro_count'])):
  mf=[]; mr=[]
  for ti,t in enumerate(TEMPLATES):
   arm=_checkpoint_arm_v3(macro,ti); asks=_ask_rows(macro,ti,checkpoint,next_ord,arm); inflight=root/f'checkpoint_{checkpoint:04d}.inflight'; closed=root/f'checkpoint_{checkpoint:04d}'; inflight.mkdir(parents=False,exist_ok=False); large._write_jsonl(inflight/'logical_asks.jsonl',asks); large._write_json(inflight/'optimizer_state_before.json',bandit.snapshot()); large._write_json(inflight/'selection_state_before.json',large._selection_state_record(state))
   schedules,decisions=tournament._select_checkpoint(asks,catalog=fresh['catalog'],bandit=bandit,state=state,components_by_id=authority['components_by_id'],adapter=authority['adapter'],compiler=authority['compiler'],prior_exact_identities=())
   norms=[str(s['optimizer_ask']['exact_identity']) for s in schedules]; raws=[physical_by_norm[n] for n in norms]
   if len(set(norms))!=24 or len(set(raws))!=24 or set(norms)&selected_norm or set(raws)&selected_raw or not set(raws)<=fresh_raw: raise RuntimeError('PRIMITIVE_MAIN_FRESH_SELECTION_DRIFT')
   selected_norm.update(norms); selected_raw.update(raws)
   for s,raw in zip(schedules,raws,strict=True): s['fresh_physical_exact_identity']=raw; s['schedule_record_sha256']=stable_hash({k:v for k,v in s.items() if k!='schedule_record_sha256'})
   large._write_jsonl(inflight/'selected_schedule.jsonl',schedules); large._write_jsonl(inflight/'selection_ledger.jsonl',decisions)
   records=successor._evaluate_schedules(schedules,record_root=inflight/'records',authority=authority,input_hash=input_hash,executor_workers=workers)
   for r in records:
    if any(int(r.get(k) or 0)!=0 for k in ('validation_reads','holdout_reads','historical_2023_reads','forward_b_reads','forward_2026_reads')): raise RuntimeError('PRIMITIVE_MAIN_RESTRICTED_READ_DRIFT')
   feedback=tournament._feedback_update(records,schedules,bandit=bandit,behavior_counts=Counter()); large._write_jsonl(inflight/'feedback.jsonl',feedback); large._write_json(inflight/'optimizer_state_after.json',bandit.snapshot()); large._write_json(inflight/'selection_state_after.json',large._selection_state_record(state)); previous=large._close_checkpoint(inflight=inflight,closed=closed,previous_manifest_sha256=previous,macro_index=macro,template_id=t,optimizer_arm=arm)
   by_ord={int(s['main_record_ordinal']):s for s in schedules}
   for f in feedback:
    if large._productive_feedback(f):
     s=by_ord[int(f['main_record_ordinal'])]; productive.append({'main_record_ordinal':int(f['main_record_ordinal']),'template_id':str(f['template_id']),'generation_arm':str(f['generation_arm']),'fresh_physical_exact_identity':str(s['fresh_physical_exact_identity']),'normalized_search_exact_identity':str(s['optimizer_ask']['exact_identity']),'program_id':str(s['primary_program']['program_id']),'absolute_admission':dict(f['absolute_admission']),'enhancer_credit':dict(f['enhancer_credit'])})
   mf.extend(feedback); mr.extend(records); all_feedback.extend(feedback); all_records.extend(records); all_schedules.extend(schedules); checkpoint+=1; next_ord+=24
  mm=_macro_metric(mf,mr,seen_pairs); mm['macro_index']=macro; macros.append(mm); large._write_json(root/f'macro_{macro:02d}_receipt.json',engine._self_hashed({'schema_version':'cn_program_primitive_main_macro_receipt_v1','macro_index':macro,'metric':mm,'selected_exact_count_cumulative':len(selected_raw)},'macro_payload_sha256')); large._write_json(root/'macro_metrics.json',{'macros':macros})
  if macro>=3 and len(macros)>=2 and all(float(x['productive_efficiency'])<=0.15 and float(x['new_behavior_pair_rate'])<=0.05 for x in macros[-2:]): stop_reason='SUSTAINED_DEVELOPMENT_VALUE_COLLAPSE'; break
 large._write_jsonl(root/'productive_discoveries.jsonl',productive); total=large._metric_block(all_feedback); from scripts.run_cn_program_optimizer_large_fresh_v3 import ARMS
 per_arm={arm:large._metric_block([r for r in all_feedback if str(r.get('generation_arm'))==arm]) for arm in ARMS}; per_template={t:large._metric_block([r for r in all_feedback if str(r.get('template_id'))==t]) for t in TEMPLATES}
 closure=engine._self_hashed({'schema_version':'cn_program_primitive_main_production_complete_v1','status':STATUS_COMPLETE,'campaign_id':CAMPAIGN_ID,'campaign_profile':CAMPAIGN_PROFILE,'repo_sha':repo_sha,'authorization_payload_sha256':authorization['authorization_payload_sha256'],'production_plan_payload_sha256':plan['plan_payload_sha256'],'input_binding_sha256':input_hash,'logical_records':len(all_feedback),'unique_normalized_exact_count':len(selected_norm),'unique_fresh_physical_exact_count':len(selected_raw),'effective_spent_exact_count':6734,'effective_spent_overlap_count':0,'closed_checkpoints':checkpoint,'closed_macros':len(macros),'stop_reason':stop_reason,'total_metrics':total,'per_arm':per_arm,'per_template':per_template,'productive_discovery_count':len(productive),'behavior_pair_count':len(seen_pairs),'optimizer_final_state_sha256':bandit.snapshot()['bandit_state_sha256'],'optimizer_metadata':bandit.optimizer_metadata(),'wall_seconds':time.perf_counter()-start,'restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0},'validation_feedback_used':False,'oos_authority':'NONE','promotion_authorized':False,'automatic_successor_authorized':False},'closure_payload_sha256'); large._write_json(root/CLOSURE_NAME,closure); return closure

__all__=['verify_plan','prefinancial_rehearsal','run']
