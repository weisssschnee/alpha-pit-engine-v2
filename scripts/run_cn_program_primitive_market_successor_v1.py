from __future__ import annotations

import argparse, json, time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_large_fresh_v1 as large
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as successor
from scripts import run_cn_program_optimizer_tournament_v1 as tournament
from scripts import run_cn_program_primitive_main_production_v1 as base
from scripts.run_cn_program_optimizer_large_fresh_v3 import ARMS, SEEDS, EVOLUTION_CONFIG
from our_system_phase2.services.program_optimizer_large_fresh_v3 import LargeFreshProgramBanditV3
from our_system_phase2.services.program_search_optimizer_v1 import normalized_program_gene_identity_v1
from our_system_phase2.services.program_search_primitive_credit_v1 import primitive_program_metadata_v1
from our_system_phase2.services.unified_capability_registry import stable_hash

CAMPAIGN_ID='CN_PROGRAM_PRIMITIVE_MARKET_SUCCESSOR_V1'
CAMPAIGN_PROFILE='cn_program_primitive_market_successor_v1'
STATUS_COMPLETE='PRIMITIVE_MARKET_SUCCESSOR_COMPLETE'
CLOSURE_NAME='CN_PROGRAM_PRIMITIVE_MARKET_SUCCESSOR_COMPLETE.json'
CHECKPOINT_SIZE=24
PLAN_RELATIVE_PATH=Path('runtime/run_plans/cn_program_primitive_market_successor_plan_v1.json')
P='PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1'; U='UNIFORM_CONTROL'; E='CATALOG_TYPED_EVOLUTION_PROGRAM_V2'
CORE=frozenset({'BASE_MARKET','BASE_MARKET_EVENT'})

def _read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding='utf-8-sig'))
def _read_jsonl(p:Path)->list[dict[str,Any]]: return [json.loads(x) for x in p.read_text(encoding='utf-8-sig').splitlines() if x.strip()]

def verify_plan(path:Path)->dict[str,Any]:
 p=_read(path); body=dict(p); claim=str(body.pop('plan_payload_sha256',''))
 if not claim or stable_hash(body)!=claim or p.get('status')!='PRIMITIVE_MARKET_SUCCESSOR_PLAN_FROZEN_NOT_RUN' or p.get('candidate_evaluation_executed') is not False: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_PLAN_DRIFT')
 b=dict(p['budget']); f=dict(p['fresh_supply']); a=dict(p['search_authority'])
 if int(b['hard_cap_logical_records'])!=360 or int(b['checkpoint_count'])!=15 or int(b['checkpoint_size'])!=24 or int(f['effective_spent_exact_count'])!=7574 or int(f['fresh_unique_count'])!=3430: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_PLAN_GEOMETRY_DRIFT')
 if a.get('production_feedback_imported_into_primitive_stats') is not False or int(a.get('production_results_in_stats') or 0)!=0 or a.get('diversity_contract_changed') is not False or int(a.get('max_variants_per_base_per_template') or 0)!=4: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_SEARCH_AUTHORITY_DRIFT')
 sched=list(p['schedule'])
 if len(sched)!=15 or [int(x['checkpoint']) for x in sched]!=list(range(15)) or [int(x['start_ordinal']) for x in sched]!=[24*i for i in range(15)]: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_SCHEDULE_DRIFT')
 return p

def _load_authority(args:argparse.Namespace,*,authorization:Mapping[str,Any],repo_sha:str)->dict[str,Any]:
 return base._load_authority(args,authorization=authorization,repo_sha=repo_sha)

def _fresh_catalog(plan:Mapping[str,Any],authority:Mapping[str,Any],repo_root:Path)->dict[str,Any]:
 bind=dict(plan['fresh_supply']); path=repo_root/Path(bind['relative_path']); supply=_read(path); body=dict(supply); claim=str(body.pop('audit_payload_sha256',''))
 if claim!=bind['payload_sha256'] or stable_hash(body)!=claim or supply.get('status')!='ZERO_FINANCIAL_PRIMITIVE_MARKET_SUCCESSOR_FRESH_SUPPLY_READY' or int(supply['fresh_unique_count'])!=3430 or int(supply['effective_spent_exact_count'])!=7574: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_FRESH_SUPPLY_DRIFT')
 catalog={t:[] for t in large.ENHANCED_TEMPLATES}; raw_expected={str(r['exact_identity']) for r in supply['fresh_entries']}
 for row in supply['fresh_entries']:
  reservoir={'schema_version':'cn_joint_program_phase_c_reservoir_record_v0','template_id':str(row['template_id']),'components':dict(row['components']),'combination_policy':dict(row['combination_policy']),'raw_combination_sha256':str(row['raw_combination_sha256']),'reservoir_record_sha256':str(row['reservoir_record_sha256']),'semantic_compile_required_at_selection':True,'semantic_noop_does_not_count_toward_quota':True,'financial_evaluation_executed':False}
  entry=engine._catalog_entry(reservoir,components_by_id=authority['components_by_id'],adapter=authority['adapter'],compiler=authority['compiler'])
  if entry.get('status')!='EXECUTABLE' or stable_hash(dict(entry['program_genes']))!=str(row['exact_identity']): raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_CATALOG_REBUILD_DRIFT')
  catalog[str(entry['template_id'])].append(entry)
 entries=tournament._program_entries(catalog); slots=tuple(entries[0].genes); catalog_by_exact={}; physical_by_normalized={}
 for t in large.ENHANCED_TEMPLATES:
  for source in catalog[t]:
   norm=normalized_program_gene_identity_v1(dict(source['program_genes']),ordered_slots=slots); raw=stable_hash(dict(source['program_genes']))
   if norm in catalog_by_exact or raw not in raw_expected: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_IDENTITY_ALIAS_DRIFT')
   catalog_by_exact[norm]=source; physical_by_normalized[norm]=raw
 metadata=primitive_program_metadata_v1(entries=entries,catalog_by_exact=catalog_by_exact)
 if len(entries)!=3430 or set(catalog_by_exact)!={e.exact_identity for e in entries} or len(set(physical_by_normalized.values()))!=3430: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_CATALOG_COVERAGE_DRIFT')
 return {'supply':supply,'catalog':catalog,'entries':entries,'catalog_by_exact':catalog_by_exact,'physical_by_normalized':physical_by_normalized,'metadata':metadata}

def _bandit(plan:Mapping[str,Any],fresh:Mapping[str,Any],repo_root:Path)->LargeFreshProgramBanditV3:
 a=dict(plan['search_authority']); path=repo_root/Path(a['primitive_stats_relative_path']); stats=_read(path); body=dict(stats); claim=str(body.pop('stats_payload_sha256',''))
 if claim!=a['primitive_stats_payload_sha256'] or stable_hash(body)!=claim: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_STATS_DRIFT')
 entries=tuple(fresh['entries']); primitive_config={'metadata_by_exact_identity':fresh['metadata'],'primitive_stats':stats,'primitive_stats_payload_sha256':claim}
 return LargeFreshProgramBanditV3(campaign_id=CAMPAIGN_ID,entries_by_arm={arm:entries for arm in ARMS},seeds=SEEDS,primitive_config=primitive_config,evolution_config=EVOLUTION_CONFIG)

def _ask_rows(row:Mapping[str,Any])->list[dict[str,Any]]:
 out=[]; cp=int(row['checkpoint']); start=int(row['start_ordinal']); ts=int(row['template_start_ordinal']); t=str(row['template']); arm=str(row['arm'])
 for local in range(CHECKPOINT_SIZE):
  x={'schema_version':'cn_program_primitive_market_successor_ask_v1','checkpoint_ordinal':cp,'optimizer_arm':arm,'generation_arm':arm,'template_id':t,'template_record_ordinal':ts+local,'main_record_ordinal':start+local,'campaign_profile':CAMPAIGN_PROFILE,'allocation_role':str(row['role']),'absolute_admission_head_eligible':True,'conditional_uplift_head_eligible':True,'matched_control_contract_id':'CANDIDATE_PROGRAM_V1_FULL_VS_BASE_MATCHED_CONTROL','program_level_credit_only':True,'component_attribution':'COMPONENT_ATTRIBUTION_UNIDENTIFIED'}; x['ask_record_sha256']=stable_hash(x); out.append(x)
 return out

def _productive_rows(feedback:Sequence[Mapping[str,Any]],schedules:Sequence[Mapping[str,Any]])->list[dict[str,Any]]:
 by={int(s['main_record_ordinal']):s for s in schedules}; out=[]
 for f in feedback:
  if not large._productive_feedback(f): continue
  s=by[int(f['main_record_ordinal'])]
  out.append({'main_record_ordinal':int(f['main_record_ordinal']),'template_id':str(f['template_id']),'generation_arm':str(f['generation_arm']),'fresh_physical_exact_identity':str(s['fresh_physical_exact_identity']),'normalized_search_exact_identity':str(s['optimizer_ask']['exact_identity']),'program_id':str(s['primary_program']['program_id']),'absolute_admission':dict(f['absolute_admission']),'enhancer_credit':dict(f['enhancer_credit'])})
 return out

def _behavior_count(records:Sequence[Mapping[str,Any]])->int:
 seen=set()
 for r in records:
  x=large._behavior_pair_identity(r)
  if x is not None: seen.add(x)
 return len(seen)

def _gate(plan:Mapping[str,Any],feedback:Sequence[Mapping[str,Any]],records:Sequence[Mapping[str,Any]])->dict[str,Any]:
 g=dict(plan['prospective_gate']); corep=[r for r in feedback if str(r.get('generation_arm'))==P and str(r.get('template_id')) in CORE]; controls=[r for r in feedback if str(r.get('generation_arm')) in {U,E} and str(r.get('template_id')) in CORE]
 bp=large._metric_block(corep); bc=large._metric_block(controls); total=large._metric_block(feedback); per={t:large._metric_block([r for r in corep if str(r.get('template_id'))==t]) for t in sorted(CORE)}; br=_behavior_count(records)/len(records) if records else 0.0; control_rate=float(bc['productive_efficiency']); primitive_rate=float(bp['productive_efficiency']); ratio=(primitive_rate/control_rate) if control_rate>0 else (999.0 if primitive_rate>0 else 0.0)
 checks={'core_primitive_productive_rate':float(bp['productive_efficiency'])>=float(g['core_primitive_productive_rate_min']),'core_primitive_vs_controls_ratio':ratio>=float(g['core_primitive_to_combined_core_controls_rate_ratio_min']),'each_core_template':all(float(v['productive_efficiency'])>=float(g['each_core_template_primitive_productive_rate_min']) for v in per.values()),'total_productive':int(total['productive'])>=int(g['total_productive_count_min']),'behavior_pair_rate':br>=float(g['behavior_pair_rate_min'])}
 return {'status':'PRIMITIVE_MARKET_SUCCESSOR_TRANSFER_PASS' if all(checks.values()) else 'PRIMITIVE_MARKET_SUCCESSOR_TRANSFER_FAIL','checks':checks,'core_primitive':bp,'combined_core_controls':bc,'core_rate_ratio_vs_controls':ratio,'per_core_template_primitive':per,'total':total,'behavior_pair_count':_behavior_count(records),'behavior_pair_rate':br}

def prefinancial_rehearsal(args:argparse.Namespace,*,authorization:Mapping[str,Any],repo_sha:str)->dict[str,Any]:
 plan=verify_plan(args.successor_plan); root=Path(__file__).resolve().parents[1]; authority=_load_authority(args,authorization=authorization,repo_sha=repo_sha); fresh=_fresh_catalog(plan,authority,root); bandit=_bandit(plan,fresh,root); state=engine._selection_state(); selected_norm=set(); selected_raw=set(); physical=fresh['physical_by_normalized']; fresh_raw=set(physical.values())
 for row in plan['schedule']:
  asks=_ask_rows(row); schedules,_=tournament._select_checkpoint(asks,catalog=fresh['catalog'],bandit=bandit,state=state,components_by_id=authority['components_by_id'],adapter=authority['adapter'],compiler=authority['compiler'],prior_exact_identities=())
  norms=[str(s['optimizer_ask']['exact_identity']) for s in schedules]; raws=[physical[n] for n in norms]
  if len(set(norms))!=24 or len(set(raws))!=24 or set(norms)&selected_norm or set(raws)&selected_raw or not set(raws)<=fresh_raw: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_PREFLIGHT_SELECTION_DRIFT')
  selected_norm.update(norms);selected_raw.update(raws);expected=[dict(s['optimizer_ask']) for s in schedules];bandit.commit_ask(arm=str(row['arm']),checkpoint_id=str(schedules[0]['optimizer_ask']['checkpoint_id']),count=len(schedules),required_program_template_id=str(row['template']),eligible_exact_identities=list(schedules[0]['optimizer_eligible_exact_identities']),batch_group_constraint=dict(schedules[0]['optimizer_batch_group_constraint']),expected_asks=expected);bandit.discard_nonlearning_pending(arm=str(row['arm']),expected_asks=expected)
 fields=base._full_field_union(fresh,authority)
 return {'status':'ZERO_FINANCIAL_PRIMITIVE_MARKET_SUCCESSOR_PREFLIGHT_READY','fresh_catalog_count':len(fresh['entries']),'preview_schedule_count':len(plan['schedule']),'preview_selected_count':len(selected_raw),'preview_unique_normalized_count':len(selected_norm),'field_column_count':len(fields),'field_columns':list(fields),'field_columns_sha256':stable_hash(list(fields)),'candidate_evaluation_executed':False,'production_feedback_imported_into_primitive_stats':False,'diversity_contract_changed':False,'validation_reads':0,'holdout_reads':0,'historical_2023_reads':0,'forward_b_reads':0,'forward_2026_reads':0}

def run(args:argparse.Namespace,*,admission:Mapping[str,Any],authorization:Mapping[str,Any])->dict[str,Any]:
 repo_sha=str(admission['repo_sha']); root_repo=Path(__file__).resolve().parents[1]; plan=verify_plan(args.successor_plan); authority=_load_authority(args,authorization=authorization,repo_sha=repo_sha); fresh=_fresh_catalog(plan,authority,root_repo); bandit=_bandit(plan,fresh,root_repo); root=args.output_root.resolve()
 if not root.is_dir() or not (root/'.project_control_execution').is_dir() or {p.name for p in root.iterdir()}!={'.project_control_execution'}: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_ADMITTED_ROOT_NOT_CLEAN')
 fields=base._full_field_union(fresh,authority); input_binding=engine._self_hashed({'schema_version':'cn_program_primitive_market_successor_input_binding_v1','repo_sha':repo_sha,'authorization_payload_sha256':authorization['authorization_payload_sha256'],'successor_plan_payload_sha256':plan['plan_payload_sha256'],'fresh_supply_payload_sha256':plan['fresh_supply']['payload_sha256'],'effective_spent_exact_count':7574,'fresh_exact_identities_sha256':plan['fresh_supply']['fresh_exact_identities_sha256'],'primitive_stats_payload_sha256':plan['search_authority']['primitive_stats_payload_sha256'],'production_results_in_primitive_stats':0,'field_columns_sha256':stable_hash(list(fields)),'evaluation_data_role':'DEVELOPMENT_ONLY','restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0}},'input_binding_sha256'); large._write_json(root/'input_binding.json',input_binding)
 input_hash=str(input_binding['input_binding_sha256']); workers=None;decision=None
 for w in (24,16):
  try:
   receipt=base._resource_canary(authority,input_hash,w,fields);workers=w;decision={'status':'PASS','selected_executor_workers':w,'attempts':[receipt],'fallback_applied':w!=24,'field_columns':list(fields),'field_columns_sha256':stable_hash(list(fields))};break
  except Exception as exc: decision={'status':'FAIL','error':f'{type(exc).__name__}:{exc}'}
 if workers is None: raise RuntimeError(f'PRIMITIVE_MARKET_SUCCESSOR_RESOURCE_CANARY_FAILED:{decision}')
 large._write_json(root/'resource_canary.json',decision);large._write_json(root/'optimizer_state_genesis.json',bandit.snapshot());state=engine._selection_state();large._write_json(root/'selection_state_genesis.json',large._selection_state_record(state))
 previous='GENESIS'; selected_norm=set();selected_raw=set();all_feedback=[];all_records=[];all_schedules=[];productive=[];physical=fresh['physical_by_normalized'];fresh_raw=set(physical.values());started=time.perf_counter()
 for row in plan['schedule']:
  cp=int(row['checkpoint']);t=str(row['template']);arm=str(row['arm']);asks=_ask_rows(row);inflight=root/f'checkpoint_{cp:04d}.inflight';closed=root/f'checkpoint_{cp:04d}';inflight.mkdir(parents=False,exist_ok=False);large._write_jsonl(inflight/'logical_asks.jsonl',asks);large._write_json(inflight/'optimizer_state_before.json',bandit.snapshot());large._write_json(inflight/'selection_state_before.json',large._selection_state_record(state))
  schedules,decisions=tournament._select_checkpoint(asks,catalog=fresh['catalog'],bandit=bandit,state=state,components_by_id=authority['components_by_id'],adapter=authority['adapter'],compiler=authority['compiler'],prior_exact_identities=())
  norms=[str(s['optimizer_ask']['exact_identity']) for s in schedules];raws=[physical[n] for n in norms]
  if len(set(norms))!=24 or len(set(raws))!=24 or set(norms)&selected_norm or set(raws)&selected_raw or not set(raws)<=fresh_raw: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_FRESH_SELECTION_DRIFT')
  selected_norm.update(norms);selected_raw.update(raws)
  for s,raw in zip(schedules,raws,strict=True): s['fresh_physical_exact_identity']=raw;s['allocation_role']=str(row['role']);s['schedule_record_sha256']=stable_hash({k:v for k,v in s.items() if k!='schedule_record_sha256'})
  large._write_jsonl(inflight/'selected_schedule.jsonl',schedules);large._write_jsonl(inflight/'selection_ledger.jsonl',decisions);records=successor._evaluate_schedules(schedules,record_root=inflight/'records',authority=authority,input_hash=input_hash,executor_workers=workers)
  for r in records:
   if any(int(r.get(k) or 0)!=0 for k in ('validation_reads','holdout_reads','historical_2023_reads','forward_b_reads','forward_2026_reads')): raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_RESTRICTED_READ_DRIFT')
  feedback=tournament._feedback_update(records,schedules,bandit=bandit,behavior_counts=Counter());large._write_jsonl(inflight/'feedback.jsonl',feedback);large._write_json(inflight/'optimizer_state_after.json',bandit.snapshot());large._write_json(inflight/'selection_state_after.json',large._selection_state_record(state));previous=large._close_checkpoint(inflight=inflight,closed=closed,previous_manifest_sha256=previous,macro_index=cp//5,template_id=t,optimizer_arm=arm)
  productive.extend(_productive_rows(feedback,schedules));all_feedback.extend(feedback);all_records.extend(records);all_schedules.extend(schedules);large._write_json(root/'progress_metrics.json',{'closed_checkpoints':cp+1,'evaluated':len(all_feedback),'productive':large._metric_block(all_feedback)['productive'],'behavior_pair_count':_behavior_count(all_records)})
 if len(all_feedback)!=360 or len(selected_norm)!=360 or len(selected_raw)!=360: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_TERMINAL_CARDINALITY_DRIFT')
 large._write_jsonl(root/'productive_discoveries.jsonl',productive); gate=_gate(plan,all_feedback,all_records);per_arm={a:large._metric_block([r for r in all_feedback if str(r.get('generation_arm'))==a]) for a in ARMS};per_template={t:large._metric_block([r for r in all_feedback if str(r.get('template_id'))==t]) for t in large.ENHANCED_TEMPLATES}
 closure=engine._self_hashed({'schema_version':'cn_program_primitive_market_successor_complete_v1','status':STATUS_COMPLETE,'prospective_gate_status':gate['status'],'campaign_id':CAMPAIGN_ID,'campaign_profile':CAMPAIGN_PROFILE,'repo_sha':repo_sha,'authorization_payload_sha256':authorization['authorization_payload_sha256'],'successor_plan_payload_sha256':plan['plan_payload_sha256'],'input_binding_sha256':input_hash,'logical_records':360,'unique_normalized_exact_count':360,'unique_fresh_physical_exact_count':360,'effective_spent_exact_count_before_successor':7574,'effective_spent_overlap_count':0,'closed_checkpoints':15,'productive_discovery_count':len(productive),'total_metrics':large._metric_block(all_feedback),'per_arm':per_arm,'per_template':per_template,'prospective_gate':gate,'optimizer_final_state_sha256':bandit.snapshot()['bandit_state_sha256'],'optimizer_metadata':bandit.optimizer_metadata(),'behavior_pair_count':_behavior_count(all_records),'wall_seconds':time.perf_counter()-started,'production_feedback_imported_into_primitive_stats':False,'diversity_contract_changed':False,'restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0},'validation_feedback_used':False,'oos_authority':'NONE','promotion_authorized':False,'automatic_successor_authorized':False},'closure_payload_sha256');large._write_json(root/CLOSURE_NAME,closure);return closure

__all__=['verify_plan','prefinancial_rehearsal','run']
