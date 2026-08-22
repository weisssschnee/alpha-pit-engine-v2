from __future__ import annotations

import argparse, gc, hashlib, json, time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Any, Mapping, Sequence

import psutil

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

CAMPAIGN_ID='CN_PROGRAM_PRIMITIVE_MARKET_SUCCESSOR_V3'
CAMPAIGN_PROFILE='cn_program_primitive_market_successor_v3'
STATUS_COMPLETE='PRIMITIVE_MARKET_SUCCESSOR_V3_COMPLETE'
CLOSURE_NAME='CN_PROGRAM_PRIMITIVE_MARKET_SUCCESSOR_V3_COMPLETE.json'
CHECKPOINT_SIZE=24
PLAN_RELATIVE_PATH=Path('runtime/run_plans/cn_program_primitive_market_successor_v3_plan.json')
P='PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1'; U='UNIFORM_CONTROL'; E='CATALOG_TYPED_EVOLUTION_PROGRAM_V2'
FOCUS=frozenset({'BASE_EVENT','BASE_TEMPORAL_MARKET_EVENT','BASE_MARKET_EVENT'})

def _read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding='utf-8-sig'))
def _read_jsonl(p:Path)->list[dict[str,Any]]: return [json.loads(x) for x in p.read_text(encoding='utf-8-sig').splitlines() if x.strip()]

def verify_plan(path:Path)->dict[str,Any]:
 p=_read(path); body=dict(p); claim=str(body.pop('plan_payload_sha256',''))
 if not claim or stable_hash(body)!=claim or p.get('status')!='PRIMITIVE_MARKET_SUCCESSOR_V3_PLAN_FROZEN_NOT_RUN' or p.get('candidate_evaluation_executed') is not False: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_PLAN_DRIFT')
 b=dict(p['budget']); f=dict(p['fresh_supply']); a=dict(p['search_authority']); per_fresh=dict(f.get('per_template_fresh') or {})
 if int(b['hard_cap_logical_records'])!=360 or int(b['checkpoint_count'])!=15 or int(b['checkpoint_size'])!=24 or int(f['effective_spent_exact_count'])!=8294 or int(f['fresh_unique_count'])<360: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_PLAN_GEOMETRY_DRIFT')
 if any(int(per_fresh.get(t) or 0)<120 for t in FOCUS): raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_FOCUS_SUPPLY_INSUFFICIENT')
 if a.get('production_feedback_imported_into_primitive_stats') is not False or a.get('v1_successor_feedback_imported_into_primitive_stats') is not False or a.get('v2_successor_feedback_imported_into_primitive_stats') is not False or int(a.get('production_results_in_stats') or 0)!=0 or int(a.get('v1_successor_results_in_stats') or 0)!=0 or int(a.get('v2_successor_results_in_stats') or 0)!=0 or a.get('diversity_contract_changed') is not False or int(a.get('max_variants_per_base_per_template') or 0)!=4: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_SEARCH_AUTHORITY_DRIFT')
 rc=dict(p['resource_contract']); root=Path(__file__).resolve().parents[1]; _verify_acceleration_accuracy_audit(p,root); _verify_v2_evidence(p,root); g=dict(p['prospective_gate'])
 if rc.get('profile')!='SEARCH_DUAL_24' or int(rc.get('primary_executor_workers') or 0)!=24 or int(rc.get('fallback_executor_workers') or 0)!=16 or rc.get('evaluator_pool_lifetime')!='PERSISTENT_RUN_SCOPE' or float(rc.get('minimum_records_per_hour_after_first_checkpoint') or 0)<650.0 or int(rc.get('minimum_free_memory_bytes') or 0)<24*1024**3 or int(rc.get('throughput_enforcement_after_warm_checkpoints') or 0)!=2 or rc.get('persistent_pool_record_hash_parity_required') is not True: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_RESOURCE_AUDIT_DRIFT')
 expected_gate={'focus_primitive_productive_rate_min':0.40,'focus_primitive_uplift_stable_2of3_rate_min':0.30,'each_focus_template_primitive_productive_rate_min':0.30,'each_focus_template_primitive_uplift_stable_2of3_rate_min':0.25,'each_focus_template_primitive_to_controls_productive_ratio_min':1.20,'each_focus_template_primitive_to_controls_uplift_stable_ratio_min':1.20,'total_productive_count_min':100,'behavior_pair_rate_min':0.70}
 if any(float(g.get(k) or 0)!=float(v) for k,v in expected_gate.items()): raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_GATE_DRIFT')
 sched=list(p['schedule'])
 if len(sched)!=15 or [int(x['checkpoint']) for x in sched]!=list(range(15)) or [int(x['start_ordinal']) for x in sched]!=[24*i for i in range(15)]: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_SCHEDULE_DRIFT')
 counts=Counter((str(x['template']),str(x['arm'])) for x in sched)
 for t in FOCUS:
  if counts[(t,P)]!=3 or counts[(t,U)]!=1 or counts[(t,E)]!=1: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_CONTROL_GEOMETRY_DRIFT')
 return p

def _verify_acceleration_accuracy_audit(plan:Mapping[str,Any],repo_root:Path)->dict[str,Any]:
 bind=dict(plan['acceleration_accuracy_audit']);path=(repo_root/Path(str(bind['relative_path']))).resolve()
 if not path.is_relative_to(repo_root.resolve()) or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=str(bind['file_sha256']): raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_ACCEL_AUDIT_FILE_DRIFT')
 row=_read(path);body=dict(row);claim=str(body.pop('audit_payload_sha256',''))
 if claim!=str(bind['payload_sha256']) or stable_hash(body)!=claim or row.get('status')!='PASS_SUCCESSOR_REAUTH_ELIGIBLE': raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_ACCEL_AUDIT_PAYLOAD_DRIFT')
 decision=dict(row['decision'])
 if int(decision.get('executor_workers_primary') or 0)!=24 or int(decision.get('executor_workers_fallback') or 0)!=16 or decision.get('evaluator_pool_lifetime')!='PERSISTENT_RUN_SCOPE' or float(decision.get('minimum_records_per_hour_after_first_checkpoint') or 0)<650.0: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_ACCEL_AUDIT_DECISION_DRIFT')
 return row

def _verify_v2_evidence(plan:Mapping[str,Any],repo_root:Path)->dict[str,Any]:
 def bound(key:str,field:str,label:str)->dict[str,Any]:
  b=dict(plan[key]); path=(repo_root/Path(str(b['relative_path']))).resolve()
  if not path.is_relative_to(repo_root.resolve()) or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=str(b['file_sha256']): raise RuntimeError(f'{label}_FILE_DRIFT')
  row=_read(path); body=dict(row); claim=str(body.pop(field,''))
  if claim!=str(b['payload_sha256']) or stable_hash(body)!=claim: raise RuntimeError(f'{label}_PAYLOAD_DRIFT')
  return row
 audit=bound('source_v2_terminal_audit','audit_payload_sha256','PRIMITIVE_MARKET_SUCCESSOR_V3_V2_AUDIT')
 if audit.get('status')!='PASS_INDEPENDENT_TERMINAL_AUDIT' or audit.get('gate_status')!='PRIMITIVE_MARKET_SUCCESSOR_V2_TRANSFER_FAIL' or list(audit.get('failed_gate_checks') or [])!=['each_core_template_uplift_stable'] or int(audit['counts']['records'])!=360 or int(audit['counts']['selected_unique'])!=360 or audit.get('oos_authority')!='NONE': raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_V2_AUDIT_CONTRACT_DRIFT')
 outcome=bound('source_v2_postrun_outcome','outcome_payload_sha256','PRIMITIVE_MARKET_SUCCESSOR_V3_V2_OUTCOME')
 if outcome.get('status')!='PRIMITIVE_MARKET_SUCCESSOR_V2_POSTRUN_CLOSED_REDIRECT' or outcome.get('post_batch_recommendation')!='REDIRECT' or outcome.get('automatic_successor_authorized') is not False: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_V2_OUTCOME_CONTRACT_DRIFT')
 focus=bound('source_v2_focus_redirect_evidence','evidence_payload_sha256','PRIMITIVE_MARKET_SUCCESSOR_V3_FOCUS_EVIDENCE')
 if focus.get('status')!='V2_FOCUS_REDIRECT_EVIDENCE_FROZEN' or list(focus.get('redirect_focus_templates') or [])!=['BASE_EVENT','BASE_TEMPORAL_MARKET_EVENT','BASE_MARKET_EVENT'] or focus.get('removed_focus_template')!='BASE_MARKET' or focus.get('scorer_mutation_authorized') is not False: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_FOCUS_EVIDENCE_CONTRACT_DRIFT')
 return {'v2_terminal_audit':audit,'v2_postrun_outcome':outcome,'focus_redirect_evidence':focus}

def _load_authority(args:argparse.Namespace,*,authorization:Mapping[str,Any],repo_sha:str)->dict[str,Any]:
 return base._load_authority(args,authorization=authorization,repo_sha=repo_sha)

def _fresh_catalog(plan:Mapping[str,Any],authority:Mapping[str,Any],repo_root:Path)->dict[str,Any]:
 bind=dict(plan['fresh_supply']); path=repo_root/Path(bind['relative_path']); supply=_read(path); body=dict(supply); claim=str(body.pop('audit_payload_sha256',''))
 expected_count=int(bind['fresh_unique_count'])
 if claim!=bind['payload_sha256'] or stable_hash(body)!=claim or supply.get('status')!='ZERO_FINANCIAL_PRIMITIVE_MARKET_SUCCESSOR_V3_FRESH_SUPPLY_READY' or int(supply['fresh_unique_count'])!=expected_count or int(supply['effective_spent_exact_count'])!=8294: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_FRESH_SUPPLY_DRIFT')
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
 if len(entries)!=expected_count or set(catalog_by_exact)!={e.exact_identity for e in entries} or len(set(physical_by_normalized.values()))!=expected_count: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_V3_CATALOG_COVERAGE_DRIFT')
 return {'supply':supply,'catalog':catalog,'entries':entries,'catalog_by_exact':catalog_by_exact,'physical_by_normalized':physical_by_normalized,'metadata':metadata}

def _bandit(plan:Mapping[str,Any],fresh:Mapping[str,Any],repo_root:Path)->LargeFreshProgramBanditV3:
 a=dict(plan['search_authority']); path=repo_root/Path(a['primitive_stats_relative_path']); stats=_read(path); body=dict(stats); claim=str(body.pop('stats_payload_sha256',''))
 if claim!=a['primitive_stats_payload_sha256'] or stable_hash(body)!=claim: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_STATS_DRIFT')
 entries=tuple(fresh['entries']); primitive_config={'metadata_by_exact_identity':fresh['metadata'],'primitive_stats':stats,'primitive_stats_payload_sha256':claim}
 return LargeFreshProgramBanditV3(campaign_id=CAMPAIGN_ID,entries_by_arm={arm:entries for arm in ARMS},seeds=SEEDS,primitive_config=primitive_config,evolution_config=EVOLUTION_CONFIG)

def _ask_rows(row:Mapping[str,Any])->list[dict[str,Any]]:
 out=[]; cp=int(row['checkpoint']); start=int(row['start_ordinal']); ts=int(row['template_start_ordinal']); t=str(row['template']); arm=str(row['arm'])
 for local in range(CHECKPOINT_SIZE):
  x={'schema_version':'cn_program_primitive_market_successor_v3_ask_v1','checkpoint_ordinal':cp,'optimizer_arm':arm,'generation_arm':arm,'template_id':t,'template_record_ordinal':ts+local,'main_record_ordinal':start+local,'campaign_profile':CAMPAIGN_PROFILE,'allocation_role':str(row['role']),'absolute_admission_head_eligible':True,'conditional_uplift_head_eligible':True,'matched_control_contract_id':'CANDIDATE_PROGRAM_V1_FULL_VS_BASE_MATCHED_CONTROL','program_level_credit_only':True,'component_attribution':'COMPONENT_ATTRIBUTION_UNIDENTIFIED'}; x['ask_record_sha256']=stable_hash(x); out.append(x)
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

def _uplift_stable_2of3(row:Mapping[str,Any])->bool:
 if not large._productive_feedback(row): return False
 credit=dict(dict(row.get('enhancer_credit') or {}).get('program_credit') or {})
 return int(credit.get('cross_window_positive_increment_count') or 0)>=2

def _stable_block(rows:Sequence[Mapping[str,Any]])->dict[str,Any]:
 items=[dict(x) for x in rows];stable=sum(_uplift_stable_2of3(x) for x in items)
 return {'evaluated':len(items),'uplift_stable_2of3':stable,'uplift_stable_2of3_rate':stable/len(items) if items else 0.0}

def _gate(plan:Mapping[str,Any],feedback:Sequence[Mapping[str,Any]],records:Sequence[Mapping[str,Any]])->dict[str,Any]:
 g=dict(plan['prospective_gate']); total=large._metric_block(feedback); br=_behavior_count(records)/len(records) if records else 0.0
 focusp=[r for r in feedback if str(r.get('generation_arm'))==P and str(r.get('template_id')) in FOCUS]; fp=large._metric_block(focusp); fstable=_stable_block(focusp)
 per={}; checks={}
 prod_ratios={}; stable_ratios={}
 for t in sorted(FOCUS):
  pr=[r for r in feedback if str(r.get('generation_arm'))==P and str(r.get('template_id'))==t]; cr=[r for r in feedback if str(r.get('generation_arm')) in {U,E} and str(r.get('template_id'))==t]
  pm=large._metric_block(pr); cm=large._metric_block(cr); ps=_stable_block(pr); cs=_stable_block(cr)
  p_rate=float(pm['productive_efficiency']); c_rate=float(cm['productive_efficiency']); p_st=float(ps['uplift_stable_2of3_rate']); c_st=float(cs['uplift_stable_2of3_rate'])
  prod_ratio=p_rate/c_rate if c_rate>0 else (999.0 if p_rate>0 else 0.0); stable_ratio=p_st/c_st if c_st>0 else (999.0 if p_st>0 else 0.0)
  prod_ratios[t]=prod_ratio; stable_ratios[t]=stable_ratio; per[t]={'primitive':pm,'controls':cm,'primitive_stable':ps,'controls_stable':cs,'productive_rate_ratio':prod_ratio,'stable_rate_ratio':stable_ratio}
 checks={'focus_primitive_productive_rate':float(fp['productive_efficiency'])>=float(g['focus_primitive_productive_rate_min']),'focus_primitive_uplift_stable_2of3_rate':float(fstable['uplift_stable_2of3_rate'])>=float(g['focus_primitive_uplift_stable_2of3_rate_min']),'each_focus_template_primitive_productive_rate':all(float(per[t]['primitive']['productive_efficiency'])>=float(g['each_focus_template_primitive_productive_rate_min']) for t in FOCUS),'each_focus_template_primitive_uplift_stable':all(float(per[t]['primitive_stable']['uplift_stable_2of3_rate'])>=float(g['each_focus_template_primitive_uplift_stable_2of3_rate_min']) for t in FOCUS),'each_focus_template_productive_advantage':all(prod_ratios[t]>=float(g['each_focus_template_primitive_to_controls_productive_ratio_min']) for t in FOCUS),'each_focus_template_stable_advantage':all(stable_ratios[t]>=float(g['each_focus_template_primitive_to_controls_uplift_stable_ratio_min']) for t in FOCUS),'total_productive':int(total['productive'])>=int(g['total_productive_count_min']),'behavior_pair_rate':br>=float(g['behavior_pair_rate_min'])}
 return {'status':'PRIMITIVE_FRONTIER_CONTROLLED_V3_PASS' if all(checks.values()) else 'PRIMITIVE_FRONTIER_CONTROLLED_V3_FAIL','checks':checks,'focus_primitive':fp,'focus_primitive_uplift_stable':fstable,'per_focus_template':per,'total':total,'behavior_pair_count':_behavior_count(records),'behavior_pair_rate':br}

def _persistent_executor_options(authority:Mapping[str,Any],input_hash:str,workers:int,field_columns:Sequence[str])->dict[str,Any]:
 return {'max_workers':int(workers),'initializer':engine._initialize_worker,'initargs':(str(authority['execution_contract_path']),str(authority['train_field_root']),str(authority['train_price_root']),authority['price_manifest'],str(authority['price_manifest_path']),str(authority['registry_path']),str(input_hash),authority['windows'],authority['field_manifest_file_sha'],authority['field_manifest_payload_sha'],tuple(field_columns))}

def _evaluate_schedules_persistent(schedules:Sequence[Mapping[str,Any]],*,record_root:Path,executor:ProcessPoolExecutor,input_hash:str,checkpoint:int)->tuple[list[dict[str,Any]],dict[str,Any]]:
 if not schedules: return [],{'checkpoint':int(checkpoint),'wall_seconds':0.0,'records_per_hour':0.0}
 record_root.mkdir(parents=True,exist_ok=False); before=engine._runtime_resource_snapshot();engine._require_runtime_resource_safety(before);minimum_free=int(before['available_physical_bytes']);minimum_commit=int(before['commit_headroom_bytes']);maximum_tree=int(before['process_tree_rss_bytes']);maximum_committed=int(before['committed_bytes']);futures={};completed=set();samples=[];first_result_seconds=None;started=time.perf_counter();psutil.cpu_percent(interval=None)
 for schedule in schedules:
  ordinal=int(schedule['main_record_ordinal']);target=record_root/f'record_{ordinal:04d}.json';futures[executor.submit(engine._evaluate_record,schedule,str(target))]=ordinal
 pending=set(futures)
 while pending:
  done,pending=wait(pending,timeout=2.0,return_when=FIRST_COMPLETED)
  if done and first_result_seconds is None: first_result_seconds=float(time.perf_counter()-started)
  for future in done:
   payload=future.result();completed.add(int(payload['main_record_ordinal']))
  snap=engine._runtime_resource_snapshot();engine._require_runtime_resource_safety(snap);minimum_free=min(minimum_free,int(snap['available_physical_bytes']));minimum_commit=min(minimum_commit,int(snap['commit_headroom_bytes']));maximum_tree=max(maximum_tree,int(snap['process_tree_rss_bytes']));maximum_committed=max(maximum_committed,int(snap['committed_bytes']));samples.append(float(psutil.cpu_percent(interval=None)))
 expected={int(row['main_record_ordinal']) for row in schedules}
 if completed!=expected: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_PERSISTENT_EVALUATION_COVERAGE_DRIFT')
 by={int(row['main_record_ordinal']):row for row in schedules};records=[]
 for path in sorted(record_root.glob('record_*.json')):
  ordinal=int(path.stem.split('_')[-1]);records.append(engine._verify_record(path,expected_input_hash=input_hash,schedule=by[ordinal]))
 if len(records)!=len(schedules): raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_PERSISTENT_VERIFIED_RECORD_COUNT_DRIFT')
 elapsed=float(time.perf_counter()-started);logical=int(psutil.cpu_count(logical=True) or 1);mean_cpu=(sum(samples)/len(samples)) if samples else 0.0;threshold=70.0*len(schedules)/logical
 return records,{'schema_version':'cn_program_primitive_market_successor_v3_checkpoint_evaluator_telemetry_v1','checkpoint':int(checkpoint),'executor_lifetime':'PERSISTENT_RUN_SCOPE','field_projection':'FULL_FROZEN_53_FIELD_UNION','records':len(records),'wall_seconds':elapsed,'first_result_seconds':first_result_seconds,'records_per_hour':len(records)/max(elapsed/3600.0,1e-12),'host_cpu_mean_percent':mean_cpu,'effective_cores':mean_cpu*logical/100.0,'saturated_compute_sample_fraction':(sum(1 for x in samples if x>=threshold)/len(samples)) if samples else 0.0,'minimum_free_memory_bytes':minimum_free,'minimum_commit_headroom_bytes':minimum_commit,'maximum_process_tree_rss_bytes':maximum_tree,'maximum_committed_bytes':maximum_committed,'candidate_evaluation_executed':True,'validation_reads':0,'holdout_reads':0,'forward_2026_reads':0}

def prefinancial_rehearsal(args:argparse.Namespace,*,authorization:Mapping[str,Any],repo_sha:str)->dict[str,Any]:
 plan=verify_plan(args.successor_plan); root=Path(__file__).resolve().parents[1]; _verify_acceleration_accuracy_audit(plan,root); authority=_load_authority(args,authorization=authorization,repo_sha=repo_sha); fresh=_fresh_catalog(plan,authority,root); bandit=_bandit(plan,fresh,root); state=engine._selection_state(); selected_norm=set(); selected_raw=set(); physical=fresh['physical_by_normalized']; fresh_raw=set(physical.values())
 for row in plan['schedule']:
  asks=_ask_rows(row); schedules,_=tournament._select_checkpoint(asks,catalog=fresh['catalog'],bandit=bandit,state=state,components_by_id=authority['components_by_id'],adapter=authority['adapter'],compiler=authority['compiler'],prior_exact_identities=())
  norms=[str(s['optimizer_ask']['exact_identity']) for s in schedules]; raws=[physical[n] for n in norms]
  if len(set(norms))!=24 or len(set(raws))!=24 or set(norms)&selected_norm or set(raws)&selected_raw or not set(raws)<=fresh_raw: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_PREFLIGHT_SELECTION_DRIFT')
  selected_norm.update(norms);selected_raw.update(raws);expected=[dict(s['optimizer_ask']) for s in schedules];bandit.commit_ask(arm=str(row['arm']),checkpoint_id=str(schedules[0]['optimizer_ask']['checkpoint_id']),count=len(schedules),required_program_template_id=str(row['template']),eligible_exact_identities=list(schedules[0]['optimizer_eligible_exact_identities']),batch_group_constraint=dict(schedules[0]['optimizer_batch_group_constraint']),expected_asks=expected);bandit.discard_nonlearning_pending(arm=str(row['arm']),expected_asks=expected)
 fields=base._full_field_union(fresh,authority)
 return {'status':'ZERO_FINANCIAL_PRIMITIVE_MARKET_SUCCESSOR_V3_PREFLIGHT_READY','fresh_catalog_count':len(fresh['entries']),'preview_schedule_count':len(plan['schedule']),'preview_selected_count':len(selected_raw),'preview_unique_normalized_count':len(selected_norm),'field_column_count':len(fields),'field_columns':list(fields),'field_columns_sha256':stable_hash(list(fields)),'candidate_evaluation_executed':False,'production_feedback_imported_into_primitive_stats':False,'v1_successor_feedback_imported_into_primitive_stats':False,'v2_successor_feedback_imported_into_primitive_stats':False,'diversity_contract_changed':False,'validation_reads':0,'holdout_reads':0,'historical_2023_reads':0,'forward_b_reads':0,'forward_2026_reads':0}

def run(args:argparse.Namespace,*,admission:Mapping[str,Any],authorization:Mapping[str,Any])->dict[str,Any]:
 repo_sha=str(admission['repo_sha']); root_repo=Path(__file__).resolve().parents[1]; plan=verify_plan(args.successor_plan); _verify_acceleration_accuracy_audit(plan,root_repo); authority=_load_authority(args,authorization=authorization,repo_sha=repo_sha); fresh=_fresh_catalog(plan,authority,root_repo); bandit=_bandit(plan,fresh,root_repo); root=args.output_root.resolve()
 if not root.is_dir() or not (root/'.project_control_execution').is_dir() or {p.name for p in root.iterdir()}!={'.project_control_execution'}: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_ADMITTED_ROOT_NOT_CLEAN')
 fields=base._full_field_union(fresh,authority); input_binding=engine._self_hashed({'schema_version':'cn_program_primitive_market_successor_v3_input_binding_v1','repo_sha':repo_sha,'authorization_payload_sha256':authorization['authorization_payload_sha256'],'successor_plan_payload_sha256':plan['plan_payload_sha256'],'fresh_supply_payload_sha256':plan['fresh_supply']['payload_sha256'],'effective_spent_exact_count':8294,'fresh_exact_identities_sha256':plan['fresh_supply']['fresh_exact_identities_sha256'],'primitive_stats_payload_sha256':plan['search_authority']['primitive_stats_payload_sha256'],'production_results_in_primitive_stats':0,'v1_successor_results_in_primitive_stats':0,'v2_successor_results_in_primitive_stats':0,'source_v2_terminal_audit_payload_sha256':plan['source_v2_terminal_audit']['payload_sha256'],'source_v2_postrun_outcome_payload_sha256':plan['source_v2_postrun_outcome']['payload_sha256'],'source_v2_focus_redirect_evidence_payload_sha256':plan['source_v2_focus_redirect_evidence']['payload_sha256'],'acceleration_accuracy_audit_payload_sha256':plan['acceleration_accuracy_audit']['payload_sha256'],'field_columns_sha256':stable_hash(list(fields)),'evaluation_data_role':'DEVELOPMENT_ONLY','evaluator_pool_lifetime':'PERSISTENT_RUN_SCOPE','restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0}},'input_binding_sha256'); large._write_json(root/'input_binding.json',input_binding)
 input_hash=str(input_binding['input_binding_sha256']); workers=None;decision=None
 for w in (24,16):
  try:
   receipt=base._resource_canary(authority,input_hash,w,fields);workers=w;decision={'status':'PASS','selected_executor_workers':w,'attempts':[receipt],'fallback_applied':w!=24,'field_columns':list(fields),'field_columns_sha256':stable_hash(list(fields))};break
  except Exception as exc: decision={'status':'FAIL','error':f'{type(exc).__name__}:{exc}'}
 if workers is None: raise RuntimeError(f'PRIMITIVE_MARKET_SUCCESSOR_RESOURCE_CANARY_FAILED:{decision}')
 large._write_json(root/'resource_canary.json',decision);large._write_json(root/'optimizer_state_genesis.json',bandit.snapshot());state=engine._selection_state();large._write_json(root/'selection_state_genesis.json',large._selection_state_record(state))
 previous='GENESIS'; selected_norm=set();selected_raw=set();all_feedback=[];all_records=[];all_schedules=[];productive=[];physical=fresh['physical_by_normalized'];fresh_raw=set(physical.values());started=time.perf_counter();evaluator_telemetry=[];before_children={child.pid for child in psutil.Process().children(recursive=True)};resource_contract=dict(plan['resource_contract']);throughput_min=float(resource_contract['minimum_records_per_hour_after_first_checkpoint']);wall_budget_seconds=float(resource_contract['wall_clock_budget_minutes'])*60.0;warm_enforce=int(resource_contract['throughput_enforcement_after_warm_checkpoints'])
 executor_options=_persistent_executor_options(authority,input_hash,workers,fields)
 with ProcessPoolExecutor(**executor_options) as eval_executor:
  for row in plan['schedule']:
   cp=int(row['checkpoint']);t=str(row['template']);arm=str(row['arm']);asks=_ask_rows(row);inflight=root/f'checkpoint_{cp:04d}.inflight';closed=root/f'checkpoint_{cp:04d}';inflight.mkdir(parents=False,exist_ok=False);large._write_jsonl(inflight/'logical_asks.jsonl',asks);large._write_json(inflight/'optimizer_state_before.json',bandit.snapshot());large._write_json(inflight/'selection_state_before.json',large._selection_state_record(state))
   schedules,decisions=tournament._select_checkpoint(asks,catalog=fresh['catalog'],bandit=bandit,state=state,components_by_id=authority['components_by_id'],adapter=authority['adapter'],compiler=authority['compiler'],prior_exact_identities=())
   norms=[str(s['optimizer_ask']['exact_identity']) for s in schedules];raws=[physical[n] for n in norms]
   if len(set(norms))!=24 or len(set(raws))!=24 or set(norms)&selected_norm or set(raws)&selected_raw or not set(raws)<=fresh_raw: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_FRESH_SELECTION_DRIFT')
   selected_norm.update(norms);selected_raw.update(raws)
   for s,raw in zip(schedules,raws,strict=True): s['fresh_physical_exact_identity']=raw;s['allocation_role']=str(row['role']);s['schedule_record_sha256']=stable_hash({k:v for k,v in s.items() if k!='schedule_record_sha256'})
   large._write_jsonl(inflight/'selected_schedule.jsonl',schedules);large._write_jsonl(inflight/'selection_ledger.jsonl',decisions);records,telemetry=_evaluate_schedules_persistent(schedules,record_root=inflight/'records',executor=eval_executor,input_hash=input_hash,checkpoint=cp);evaluator_telemetry.append(telemetry);large._write_json(root/'evaluator_telemetry.json',{'schema_version':'cn_program_primitive_market_successor_v3_evaluator_telemetry_v1','executor_lifetime':'PERSISTENT_RUN_SCOPE','selected_executor_workers':workers,'field_column_count':len(fields),'checkpoints':evaluator_telemetry})
   for r in records:
    if any(int(r.get(k) or 0)!=0 for k in ('validation_reads','holdout_reads','historical_2023_reads','forward_b_reads','forward_2026_reads')): raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_RESTRICTED_READ_DRIFT')
   feedback=tournament._feedback_update(records,schedules,bandit=bandit,behavior_counts=Counter());large._write_jsonl(inflight/'feedback.jsonl',feedback);large._write_json(inflight/'optimizer_state_after.json',bandit.snapshot());large._write_json(inflight/'selection_state_after.json',large._selection_state_record(state));previous=large._close_checkpoint(inflight=inflight,closed=closed,previous_manifest_sha256=previous,macro_index=cp//5,template_id=t,optimizer_arm=arm)
   productive.extend(_productive_rows(feedback,schedules));all_feedback.extend(feedback);all_records.extend(records);all_schedules.extend(schedules)
   warm=evaluator_telemetry[1:];warm_records=sum(int(x['records']) for x in warm);warm_wall=sum(float(x['wall_seconds']) for x in warm);warm_rate=(warm_records/max(warm_wall/3600.0,1e-12)) if warm else None;elapsed=float(time.perf_counter()-started);remaining=360-len(all_feedback);remaining_checkpoints=max(0,15-(cp+1));projected_total=(elapsed+(remaining/max((warm_rate or 1.0)/3600.0,1e-12))+remaining_checkpoints*7.5) if warm_rate else None;throughput_status='WARMUP' if len(warm)<warm_enforce else ('PASS' if warm_rate>=throughput_min and projected_total<=wall_budget_seconds else 'FAIL')
   large._write_json(root/'progress_metrics.json',{'closed_checkpoints':cp+1,'evaluated':len(all_feedback),'productive':large._metric_block(all_feedback)['productive'],'uplift_stable_2of3':sum(_uplift_stable_2of3(x) for x in all_feedback),'behavior_pair_count':_behavior_count(all_records),'last_checkpoint_wall_seconds':telemetry['wall_seconds'],'last_checkpoint_host_cpu_mean_percent':telemetry['host_cpu_mean_percent'],'last_checkpoint_effective_cores':telemetry['effective_cores'],'warm_records_per_hour':warm_rate,'projected_total_wall_seconds':projected_total,'throughput_contract_status':throughput_status})
   if throughput_status=='FAIL': raise RuntimeError(f'PRIMITIVE_MARKET_SUCCESSOR_THROUGHPUT_CONTRACT_FAILED:rate={warm_rate}:projected={projected_total}:budget={wall_budget_seconds}')
 gc.collect();engine._require_runtime_resource_safety(engine._runtime_resource_snapshot());orphans=engine._new_child_process_ids(before_children)
 if orphans: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_PERSISTENT_POOL_LEFT_ORPHANS:'+','.join(map(str,orphans)))
 if len(all_feedback)!=360 or len(selected_norm)!=360 or len(selected_raw)!=360: raise RuntimeError('PRIMITIVE_MARKET_SUCCESSOR_TERMINAL_CARDINALITY_DRIFT')
 large._write_jsonl(root/'productive_discoveries.jsonl',productive); gate=_gate(plan,all_feedback,all_records);per_arm={a:large._metric_block([r for r in all_feedback if str(r.get('generation_arm'))==a]) for a in ARMS};per_template={t:large._metric_block([r for r in all_feedback if str(r.get('template_id'))==t]) for t in large.ENHANCED_TEMPLATES};mean_cpu=sum(float(x['host_cpu_mean_percent']) for x in evaluator_telemetry)/len(evaluator_telemetry);mean_wall=sum(float(x['wall_seconds']) for x in evaluator_telemetry)/len(evaluator_telemetry)
 closure=engine._self_hashed({'schema_version':'cn_program_primitive_market_successor_v3_complete_v1','status':STATUS_COMPLETE,'prospective_gate_status':gate['status'],'campaign_id':CAMPAIGN_ID,'campaign_profile':CAMPAIGN_PROFILE,'repo_sha':repo_sha,'authorization_payload_sha256':authorization['authorization_payload_sha256'],'successor_plan_payload_sha256':plan['plan_payload_sha256'],'source_v2_terminal_audit_payload_sha256':plan['source_v2_terminal_audit']['payload_sha256'],'source_v2_postrun_outcome_payload_sha256':plan['source_v2_postrun_outcome']['payload_sha256'],'source_v2_focus_redirect_evidence_payload_sha256':plan['source_v2_focus_redirect_evidence']['payload_sha256'],'input_binding_sha256':input_hash,'logical_records':360,'unique_normalized_exact_count':360,'unique_fresh_physical_exact_count':360,'effective_spent_exact_count_before_successor':8294,'effective_spent_overlap_count':0,'closed_checkpoints':15,'productive_discovery_count':len(productive),'total_metrics':large._metric_block(all_feedback),'per_arm':per_arm,'per_template':per_template,'prospective_gate':gate,'optimizer_final_state_sha256':bandit.snapshot()['bandit_state_sha256'],'optimizer_metadata':bandit.optimizer_metadata(),'behavior_pair_count':_behavior_count(all_records),'uplift_stable_2of3_count':sum(_uplift_stable_2of3(x) for x in all_feedback),'wall_seconds':time.perf_counter()-started,'evaluator_acceleration':{'executor_lifetime':'PERSISTENT_RUN_SCOPE','selected_executor_workers':workers,'field_column_count':len(fields),'mean_checkpoint_wall_seconds':mean_wall,'mean_host_cpu_percent':mean_cpu,'checkpoint_telemetry_count':len(evaluator_telemetry),'warm_records_per_hour':warm_rate,'throughput_contract_status':throughput_status,'wall_clock_budget_seconds':wall_budget_seconds},'production_feedback_imported_into_primitive_stats':False,'v1_successor_feedback_imported_into_primitive_stats':False,'v2_successor_feedback_imported_into_primitive_stats':False,'diversity_contract_changed':False,'restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0},'validation_feedback_used':False,'oos_authority':'NONE','promotion_authorized':False,'automatic_successor_authorized':False},'closure_payload_sha256');large._write_json(root/CLOSURE_NAME,closure);return closure

__all__=['verify_plan','prefinancial_rehearsal','run']
