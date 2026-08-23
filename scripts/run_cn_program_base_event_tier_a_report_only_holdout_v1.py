"""Report-only holdout for the two frozen BASE_EVENT Tier-A mechanism survivors."""
from __future__ import annotations
import argparse,csv,gc,hashlib,json,os,subprocess,sys,time
from concurrent.futures import FIRST_COMPLETED,ProcessPoolExecutor,wait
from pathlib import Path
from typing import Any,Mapping,Sequence
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
for _p in (ROOT,ROOT/'src'):
    if str(_p) not in sys.path: sys.path.insert(0,str(_p))
from scripts import run_cn_finalist_replay_then_oos as base
from scripts import run_cn_joint_program_phase_b_v0 as phase_b
from scripts import run_cn_portfolio_decoder_v2_oos as oos
from our_system_phase2.services.a_share_executable_replay import AShareCorporateActionPolicy,AShareExecutionPolicy,AShareFeeSchedule,AShareUniversePolicy
from our_system_phase2.services.candidate_program_execution_v1 import apply_compiled_candidate_program_v1
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import MATCHED_CONTROL_CONTRACT_ID,conditional_uplift_credit
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry,stable_hash

PLAN_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_report_only_holdout_plan.json')
CLOSURE_NAME='CN_PROGRAM_BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_COMPLETE.json'
STATUS='BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_COMPLETE';SUPPORTED='BASE_EVENT_TIER_A_HOLDOUT_MECHANISM_TRANSFER_SUPPORTED';NOT_SUPPORTED='BASE_EVENT_TIER_A_HOLDOUT_MECHANISM_TRANSFER_NOT_SUPPORTED'
_PROCESS_CONTEXT=None;_PROCESS_REGISTRY=None;_PROCESS_INPUT_HASH=None;_PROCESS_RECORD_ROOT=None;_PROCESS_MEMBER_BY_EXACT=None;_PROCESS_WINDOWS=None

def _read(p:Path)->dict[str,Any]:return json.loads(p.read_text(encoding='utf-8-sig'))
def _readjl(p:Path)->list[dict[str,Any]]:return [json.loads(x) for x in p.read_text(encoding='utf-8-sig').splitlines() if x.strip()]
def _write(p:Path,x:Mapping[str,Any])->Path:p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(dict(x),ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8');return p
def _writejl(p:Path,xs:Sequence[Mapping[str,Any]])->Path:p.parent.mkdir(parents=True,exist_ok=True);p.write_text(''.join(json.dumps(dict(x),ensure_ascii=False,sort_keys=True)+'\n' for x in xs),encoding='utf-8');return p
def _sha(p:Path)->str:return hashlib.sha256(p.read_bytes()).hexdigest()
def _self(p:Path,f:str)->dict[str,Any]:
 x=_read(p);b=dict(x);c=str(b.pop(f,''));
 if not c or stable_hash(b)!=c:raise RuntimeError(f'SELF_HASH_DRIFT:{p}')
 return x
def _bound(repo:Path,b:Mapping[str,Any],f:str|None=None):
 p=(repo/Path(str(b['relative_path']))).resolve()
 if not p.is_relative_to(repo.resolve()) or not p.is_file() or _sha(p)!=str(b['file_sha256']):raise RuntimeError(f'BOUND_FILE_DRIFT:{p}')
 if p.suffix.lower()=='.jsonl':
  xs=_readjl(p)
  if f and stable_hash(xs)!=str(b.get('payload_sha256') or ''):raise RuntimeError(f'BOUND_JSONL_DRIFT:{p}')
  return xs
 x=_self(p,f) if f else _read(p)
 if f and str(x[f])!=str(b.get('payload_sha256') or ''):raise RuntimeError(f'BOUND_PAYLOAD_DRIFT:{p}')
 return x
def verify_plan(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
 repo=Path(repo_root or ROOT).resolve();p=_self(path.resolve(),'plan_payload_sha256')
 if p.get('schema_version')!='cn_program_base_event_tier_a_report_only_holdout_plan_v1' or p.get('status')!='BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_PLAN_FROZEN_NOT_RUN' or int(p.get('candidate_count') or 0)!=2 or len(p.get('family_ids') or [])!=2 or len(p.get('required_physical_leaf_ids') or [])!=3:raise RuntimeError('TIER_A_HOLDOUT_PLAN_DRIFT')
 if p.get('evaluation_role')!='holdout' or p.get('usage')!='REPORT_ONLY_CANDIDATE_TRANSFER' or p.get('holdout_access_authorized') is not False or p.get('promotion_authorized') is not False or p.get('automatic_successor_authorized') is not False:raise RuntimeError('TIER_A_HOLDOUT_BOUNDARY_DRIFT')
 if any(int(p.get(k) or 0)!=0 for k in ('holdout_reads','validation_reads','historical_challenge_reads','forward_b_reads','forward_2026_reads')):raise RuntimeError('TIER_A_HOLDOUT_READ_BOUNDARY_DRIFT')
 fr=_bound(repo,p['candidate_freeze'],'freeze_payload_sha256');mem=_bound(repo,p['candidate_members'],'candidate_members_payload_sha256');pre=_bound(repo,p['selection_preflight'],'preflight_payload_sha256');rev=_bound(repo,p['access_review'],'review_payload_sha256');dec=_bound(repo,p['decision_packet'],'decision_payload_sha256');va=_bound(repo,p['source_validation_audit'],'audit_payload_sha256');vo=_bound(repo,p['source_validation_outcome'],'outcome_payload_sha256');sched=_bound(repo,p['resolved_schedules'])
 assert isinstance(fr,dict) and isinstance(mem,list) and isinstance(pre,dict) and isinstance(rev,dict) and isinstance(dec,dict) and isinstance(va,dict) and isinstance(vo,dict) and isinstance(sched,list)
 if fr.get('holdout_access_authorized') is not False or int(fr.get('candidate_count') or 0)!=2 or va.get('status')!='PASS_INDEPENDENT_TERMINAL_AUDIT' or vo.get('post_batch_recommendation')!='REDIRECT' or vo.get('redirect_target')!='HOLDOUT_ACCESS_PROJECT_CONTROL_REVIEW':raise RuntimeError('TIER_A_HOLDOUT_LINEAGE_DRIFT')
 if int(pre.get('holdout_trade_date_count') or 0)!=48 or len(pre.get('holdout_windows') or [])!=3 or any(int(x['session_count'])!=16 for x in pre['holdout_windows']):raise RuntimeError('TIER_A_HOLDOUT_WINDOW_DRIFT')
 if list(pre['required_physical_leaf_ids'])!=list(p['required_physical_leaf_ids']) or pre['required_physical_leaf_ids_sha256']!=p['required_physical_leaf_ids_sha256']:raise RuntimeError('TIER_A_HOLDOUT_FIELDS_DRIFT')
 if dict(rev.get('infrastructure') or {}).get('implementation_strategy')!='AFTER_SEPARATE_PROJECT_CONTROL_ADMISSION_BUILD_FRESH_CANDIDATE_BOUND_HOLDOUT_FIELD_AND_LABEL_SIDECARS_AND_HOLDOUT_SESSION_AUTHORITY_THEN_REUSE_COMPILED_PROGRAM_MATCHED_CONTROL_REPLAY_SEMANTICS':raise RuntimeError('TIER_A_HOLDOUT_INFRA_DRIFT')
 if dict(p['resource_contract'])!={'profile':'VALIDATION_DUAL_8','cpu_threads':8,'evaluator_workers':2,'candidate_count':2}:raise RuntimeError('TIER_A_HOLDOUT_RESOURCE_DRIFT')
 return p

def _verify_file(path:Path,h:str,label:str):
 if not path.is_file() or _sha(path)!=h:raise RuntimeError(f'{label}_DRIFT')
def _verify_sources(p:Mapping[str,Any],a:argparse.Namespace):
 s=dict(p['source_data'])
 for k,x in (('minute_source_root',a.minute_source_root),('fundamental_root',a.fundamental_root),('chip_root',a.chip_root),('public_source_root',a.public_source_root)):
  if str(Path(x).resolve())!=str(Path(s[k]).resolve()):raise RuntimeError(f'{k.upper()}_PATH_DRIFT')
 for k,x in (('source_contract',a.source_contract),('registry',a.registry),('split_manifest',a.split_manifest),('daily_st_source',a.daily_st_source)):
  b=dict(s[k]);rp=Path(x).resolve()
  if str(rp)!=str(Path(b['path']).resolve()):raise RuntimeError(f'{k.upper()}_PATH_DRIFT')
  _verify_file(rp,str(b['sha256']),k.upper())
 for k in ('chip_manifest','public_source_manifest'):
  b=dict(s[k]);_verify_file(Path(b['path']),str(b['sha256']),k.upper())
def _requirements(path:Path,fields:Sequence[str])->Path:
 with path.open('w',encoding='utf-8-sig',newline='') as h:
  w=csv.DictWriter(h,fieldnames=['candidate_id','expression']);w.writeheader()
  for f in fields:w.writerow({'candidate_id':f'required::{f}','expression':f'${f}'})
 return path
_LABEL_SERIAL_ENV={'NUMBA_NUM_THREADS':'1','ARROW_NUM_THREADS':'1','OMP_NUM_THREADS':'1','MKL_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1','NUMEXPR_MAX_THREADS':'1'}
def _label_builder_env(polars_threads:int=8)->dict[str,str]:
 env=os.environ.copy();env.update(_LABEL_SERIAL_ENV);env['POLARS_MAX_THREADS']=str(int(polars_threads));return env
def _run(xs:Sequence[Any],*,env:Mapping[str,str]|None=None):
 cp=subprocess.run([str(x) for x in xs],text=True,capture_output=True,env=None if env is None else dict(env))
 if cp.returncode:raise RuntimeError(f'SUBPROCESS_FAILED:{xs[1] if len(xs)>1 else xs[0]}\nOUT={cp.stdout}\nERR={cp.stderr}')
def _root(p:Path)->Path:
 p=p.resolve()
 if not p.is_dir() or not (p/'.project_control_execution').is_dir():raise RuntimeError('TIER_A_HOLDOUT_ADMITTED_ROOT_MISSING')
 u={x.name for x in p.iterdir() if x.name!='.project_control_execution'}
 if u:raise RuntimeError('TIER_A_HOLDOUT_ROOT_NOT_CLEAN:'+','.join(sorted(u)))
 return p
def _load_holdout_context(*,source_contract:Path,field_root:Path,label_root:Path,authority_root:Path,expected_status:str,expected_schema:str)->dict[str,Any]:
 contract=oos.v1._read_json(source_contract);base._verify_payload_hash(contract,field='contract_payload_sha256',label='source replay/OOS execution contract');split_hash=str(contract['split_manifest_sha256'])
 fm,fmp=base._validate_sidecar(field_root,evaluation_role='holdout',split_hash=split_hash);lm,lmp=base._validate_label_sidecar(label_root,split_hash=split_hash,evaluation_role='holdout');ff=base._load_field_frame(field_root,fm)
 if not ff['trade_time'].dt.strftime('%H:%M:%S').eq('15:00:00').all():raise RuntimeError('holdout sidecar intraday clocks')
 dateset=set(pd.to_datetime(ff['date']).dt.normalize())
 if len(dateset)!=48 or len(dateset)!=int(fm['eligible_holdout_date_count']):raise RuntimeError('holdout calendar count drift')
 mp=authority_root/'holdout_session_authority_manifest.json';m=oos.v1._read_json(mp);b=dict(m);c=str(b.pop('manifest_payload_sha256',''))
 if not c or oos.v1._stable_hash(b)!=c:raise RuntimeError('holdout session authority self-hash drift')
 req={'schema_version':expected_schema,'status':expected_status,'evaluation_role':'holdout','data_role':'holdout_report_only','field_manifest_sha256':oos.v1._sha256(fmp),'validation_reads':0,'forward_2026_reads':0,'promotion':'FORBIDDEN'}
 if [k for k,v in req.items() if m.get(k)!=v] or int(m.get('holdout_reads') or 0)<=0:raise RuntimeError('holdout session authority drift')
 for x in m.get('artifacts') or ():
  q=authority_root/str(x['path'])
  if not q.is_file() or q.stat().st_size!=int(x['bytes']) or oos.v1._sha256(q)!=str(x['sha256']):raise RuntimeError('holdout authority artifact drift')
 sp=authority_root/'holdout_session_authority.parquet';sa=pd.read_parquet(sp);sd=pd.to_datetime(sa['date'],errors='raise').dt.normalize();sa=sa[sd.isin(dateset)].copy()
 if set(pd.to_datetime(sa['date']).dt.normalize())!=dateset:raise RuntimeError('holdout authority calendar drift')
 master,observed,authority=base._materialize_replay_master(ff,sa);u0=dict(contract['universe_policy']);u0['allowed_exchanges']=tuple(u0['allowed_exchanges']);universe=AShareUniversePolicy(**u0);fee=AShareFeeSchedule(**dict(contract['fee_schedule']));execution=AShareExecutionPolicy(**dict(contract['execution_policy']));corporate=AShareCorporateActionPolicy(**dict(contract['corporate_action_policy']));prepared=oos._prepare_sessions(master.assign(signal=0.0),universe_policy=universe).drop(columns=['signal']);pi=pd.MultiIndex.from_frame(prepared[['date','code']])
 if set(pi)!=set(authority):raise RuntimeError('holdout prepared/session coordinate drift')
 return {'contract':contract,'field_manifest':fm,'label_manifest':lm,'field_frame':ff,'master':master,'observed_index':observed,'authority_index':authority,'universe':universe,'fee':fee,'execution':execution,'corporate':corporate,'holdout_field_reads':int(fm['holdout_reads']),'holdout_authority_reads':int(m['holdout_reads']),'holdout_reads':int(fm['holdout_reads'])+int(m['holdout_reads'])}
def _prepare(repo:Path,p:Mapping[str,Any],a:argparse.Namespace,out:Path,repo_sha:str)->dict[str,Any]:
 _verify_sources(p,a);prep=out/'holdout_preparation';prep.mkdir();fields=tuple(map(str,p['required_physical_leaf_ids']));req=_requirements(prep/'holdout_program_required_fields.csv',fields);field=prep/'program_holdout_session_fields'
 _run([sys.executable,repo/'scripts/build_cn_core_pack_validation_session_sidecar.py','--source-root',a.minute_source_root,'--evaluation-role','holdout','--output-root',field,'--candidate-table',req,'--registry',a.registry,'--split-manifest',a.split_manifest,'--split-manifest-hash',p['source_data']['split_manifest']['sha256'],'--fundamental-root',a.fundamental_root,'--chip-root',a.chip_root,'--max-shards','16']);fm=field/'CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json';fsha=_sha(fm)
 labels=prep/'program_holdout_session_labels';_run([sys.executable,repo/'scripts/build_cn_phase3cm_forward_label_sidecars.py','--source-root',field,'--evaluation-role','holdout','--output-root',labels,'--split-manifest',a.split_manifest,'--split-manifest-hash',p['source_data']['split_manifest']['sha256'],'--horizons','1,5,15,30','--max-shards','16','--polars-threads','8'],env=_label_builder_env(8))
 auth=prep/'holdout_session_authority';_run([sys.executable,repo/'scripts/build_cn_validation_session_authority.py','--field-manifest',fm,'--public-source-root',a.public_source_root,'--output-root',auth,'--expected-field-manifest-sha256',fsha,'--expected-source-manifest-sha256',p['source_data']['public_source_manifest']['sha256'],'--historical-daily-st-source',a.daily_st_source,'--expected-daily-st-source-sha256',p['source_data']['daily_st_source']['sha256'],'--builder-commit-sha',repo_sha,'--evaluation-role','holdout','--date-min',p['holdout_windows'][0]['start_date'],'--date-max',p['holdout_windows'][-1]['end_date'],'--schema-version','cn_holdout_session_authority_v1','--status','HOLDOUT_SESSION_AUTHORITY_CLOSED_IMMUTABLE','--evidence-scope','BASE_EVENT_TIER_A_HOLDOUT_REPORT_ONLY_INPUT'])
 audit=prep/'holdout_session_authority_audit';_run([sys.executable,repo/'scripts/verify_cn_report_only_session_authority.py','--authority-root',auth,'--output-root',audit,'--evaluation-role','holdout','--date-min',p['holdout_windows'][0]['start_date'],'--date-max',p['holdout_windows'][-1]['end_date'],'--expected-schema-version','cn_holdout_session_authority_v1','--expected-status','HOLDOUT_SESSION_AUTHORITY_CLOSED_IMMUTABLE']);ar=_read(audit/'audit.json')
 if ar.get('status')!='PASS_INDEPENDENT_REPORT_ONLY_SESSION_AUTHORITY_VERIFICATION':raise RuntimeError('TIER_A_HOLDOUT_AUTHORITY_AUDIT_FAIL')
 ctx=_load_holdout_context(source_contract=a.source_contract.resolve(),field_root=field,label_root=labels,authority_root=auth,expected_status='HOLDOUT_SESSION_AUTHORITY_CLOSED_IMMUTABLE',expected_schema='cn_holdout_session_authority_v1');missing=sorted(set(fields)-set(ctx['field_frame'].columns))
 if missing:raise RuntimeError('TIER_A_HOLDOUT_FIELDS_MISSING:'+','.join(missing))
 prepared={'schema_version':'cn_program_base_event_tier_a_holdout_prepared_binding_v1','status':'BASE_EVENT_TIER_A_HOLDOUT_CONTEXT_READY_AFTER_ADMISSION','repo_sha':repo_sha,'plan_payload_sha256':p['plan_payload_sha256'],'candidate_exact_identities_sha256':p['candidate_exact_identities_sha256'],'candidate_count':2,'required_physical_leaf_ids':list(fields),'required_physical_leaf_ids_sha256':p['required_physical_leaf_ids_sha256'],'field_manifest_sha256':fsha,'label_manifest_sha256':_sha(labels/'CN_FORWARD_LABEL_SIDECAR_MANIFEST.json'),'holdout_session_authority_manifest_sha256':_sha(auth/'holdout_session_authority_manifest.json'),'holdout_session_authority_audit_sha256':_sha(audit/'audit.json'),'holdout_windows':list(p['holdout_windows']),'holdout_reads_during_context_smoke':int(ctx['holdout_reads']),'validation_reads':0,'historical_challenge_reads':0,'forward_b_reads':0,'forward_2026_reads':0,'candidate_evaluation_executed':False,'optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN','promotion':'FORBIDDEN','field_root':str(field),'label_root':str(labels),'authority_root':str(auth)};prepared['prepared_binding_payload_sha256']=stable_hash(prepared);_write(prep/'BASE_EVENT_TIER_A_HOLDOUT_CONTEXT_READY.json',prepared);return prepared

def _adapt(repo:Path,p:Mapping[str,Any])->tuple[list[dict[str,Any]],list[dict[str,Any]]]:
 schedules=_readjl(repo/Path(p['resolved_schedules']['relative_path']));members=_readjl(repo/Path(p['candidate_members']['relative_path']));by={str(x['exact_identity']):dict(x) for x in members};out=[]
 for s in schedules:
  exact=str(dict(s.get('optimizer_ask') or {}).get('exact_identity') or '');m=by[exact];body={k:v for k,v in s.items() if k!='schedule_record_sha256'}
  if stable_hash(body)!=str(s['schedule_record_sha256']) or int(s['main_record_ordinal'])!=int(m['source_main_record_ordinal']) or str(s.get('replicate_id'))!=str(m['source_replicate']):raise RuntimeError('TIER_A_HOLDOUT_SCHEDULE_MEMBER_DRIFT')
  x=dict(s);x['holdout_exact_identity']=exact;out.append(x)
 if len(out)!=2 or len({x['holdout_exact_identity'] for x in out})!=2:raise RuntimeError('TIER_A_HOLDOUT_ADAPTER_DRIFT')
 return out,members

def _eval_compiled(compiled,context,windows):
 output=apply_compiled_candidate_program_v1(context['field_frame'],compiled,data_role='holdout_report_only',materialized_sidecar_clock_column='trade_time',materialized_sidecar_authority='PIT_MATERIALIZED_FIELD_SIDECAR');diag=phase_b._signal_diagnostics(output,output['signal']);return phase_b._replay_signal(output['signal'],context=context,signal_diagnostics=diag,windows=windows)
def _initialize(source_contract,field_root,label_root,authority_root,registry,input_hash,record_root,members,windows):
 global _PROCESS_CONTEXT,_PROCESS_REGISTRY,_PROCESS_INPUT_HASH,_PROCESS_RECORD_ROOT,_PROCESS_MEMBER_BY_EXACT,_PROCESS_WINDOWS
 _PROCESS_CONTEXT=_load_holdout_context(source_contract=Path(source_contract),field_root=Path(field_root),label_root=Path(label_root),authority_root=Path(authority_root),expected_status='HOLDOUT_SESSION_AUTHORITY_CLOSED_IMMUTABLE',expected_schema='cn_holdout_session_authority_v1');_PROCESS_REGISTRY=UnifiedCapabilityRegistry.read(Path(registry));_PROCESS_INPUT_HASH=input_hash;_PROCESS_RECORD_ROOT=Path(record_root);_PROCESS_MEMBER_BY_EXACT={str(x['exact_identity']):dict(x) for x in members};_PROCESS_WINDOWS=list(windows)
def _one(schedule,ordinal):
 if _PROCESS_CONTEXT is None or _PROCESS_REGISTRY is None or _PROCESS_WINDOWS is None:raise RuntimeError('TIER_A_HOLDOUT_WORKER_NOT_INITIALIZED')
 exact=str(schedule['holdout_exact_identity']);m=dict(_PROCESS_MEMBER_BY_EXACT[exact]);pc=phase_b._compiled(schedule,program_key='primary_program',compiled_key='primary_compiled',registry=_PROCESS_REGISTRY);cc=phase_b._compiled(schedule,program_key='control_program',compiled_key='control_compiled',registry=_PROCESS_REGISTRY);primary=None;control=None;blocker=None
 try:primary=_eval_compiled(pc,_PROCESS_CONTEXT,_PROCESS_WINDOWS)
 except phase_b.AShareCandidateReplayBlockerError as e:blocker={'leg':'PRIMARY','fail_closed':True,**e.blocker_details()}
 if blocker is None:
  try:control=_eval_compiled(cc,_PROCESS_CONTEXT,_PROCESS_WINDOWS)
  except phase_b.AShareCandidateReplayBlockerError as e:blocker={'leg':'BASE_CONTROL','fail_closed':True,**e.blocker_details()}
 complete=blocker is None;reward=float(primary['continuous_book_net_reward'])-float(control['continuous_book_net_reward']) if complete else None;ret=float(primary['cumulative_net_return'])-float(control['cumulative_net_return']) if complete else None;blockers=[str(blocker.get('blocker_code') or 'REPLAY_BLOCKED')] if blocker else []
 if complete:
  if int(primary['fill_count'])==0:blockers.append('NO_EXECUTABLE_FILLS')
  if str(primary['behavior_identity'])==str(control['behavior_identity']):blockers.append('BEHAVIOR_EQUIVALENT_TO_BASE')
 record={'schema_version':'cn_program_base_event_tier_a_holdout_pair_record_v1','status':'TIER_A_HOLDOUT_PAIR_CLOSED_IMMUTABLE','input_binding_sha256':_PROCESS_INPUT_HASH,'holdout_record_ordinal':ordinal,'exact_identity':exact,'family_id':m['family_id'],'record_kind':'ENHANCED_FULL_BASE_PAIR','source_schedule_record_sha256':schedule['schedule_record_sha256'],'program_id':schedule['primary_program']['program_id'],'control_program_id':schedule['control_program']['program_id'],'pair_id':schedule['pair_id'],'matched_control_contract_id':MATCHED_CONTROL_CONTRACT_ID,'replay_status':phase_b.PAIR_REPLAY_COMPLETE if complete else phase_b.PAIR_REPLAY_BLOCKED,'replay_blocker':blocker,'primary':primary,'base_control':control,'matched_net_reward_increment':reward,'matched_cumulative_return_increment':ret,'blockers':blockers,'holdout_reads':int(_PROCESS_CONTEXT['holdout_reads']),'validation_reads':0,'historical_challenge_reads':0,'forward_b_reads':0,'forward_2026_reads':0,'optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN','promotion':'FORBIDDEN'};record['record_payload_sha256']=stable_hash(record);ad=AbsoluteEconomicAdmission.evaluate(record,expected_pair_id=record['pair_id'],expected_program_id=record['program_id'],expected_control_program_id=record['control_program_id']);up=conditional_uplift_credit(record,ad);productive=bool(ad.admitted and up is not None and float(up.program_credit['matched_cumulative_net_return_increment'])>0 and float(up.program_credit['matched_net_reward_increment'])>0);cross=int((up.program_credit if up else {}).get('cross_window_positive_increment_count') or 0);survivor=bool(complete and productive and cross>=2)
 result={'schema_version':'cn_program_base_event_tier_a_holdout_result_v1','status':'TIER_A_HOLDOUT_RESULT_CLOSED_IMMUTABLE','holdout_record_ordinal':ordinal,'candidate_id':m['candidate_id'],'exact_identity':exact,'family_id':m['family_id'],'pair_record_sha256':record['record_payload_sha256'],'admission':ad.to_record(),'uplift':None if up is None else up.to_record(),'holdout_productive':productive,'cross_window_positive_increment_count':cross,'holdout_stable_2of3':bool(productive and cross>=2),'candidate_holdout_survivor':survivor,'holdout_reads':int(_PROCESS_CONTEXT['holdout_reads']),'validation_reads':0,'historical_challenge_reads':0,'forward_b_reads':0,'forward_2026_reads':0,'optimizer_feedback_write':'FORBIDDEN','promotion_authorized':False};result['result_payload_sha256']=stable_hash(result);_write(_PROCESS_RECORD_ROOT/f'candidate_{ordinal:02d}_{exact[:12]}.json',{'pair_record':record,'holdout_result':result});gc.collect();return result
def _metrics(p,results):
 by={str(x['family_id']):x for x in results};per={}
 for fid in p['family_ids']:
  row=by[str(fid)];per[str(fid)]={'candidate_exact_identity':row['exact_identity'],'survivor':bool(row['candidate_holdout_survivor']),'holdout_productive':bool(row['holdout_productive']),'holdout_stable_2of3':bool(row['holdout_stable_2of3']),'cross_window_positive_increment_count':int(row['cross_window_positive_increment_count'])}
 supported=all(x['survivor'] for x in per.values());return {'status':SUPPORTED if supported else NOT_SUPPORTED,'family_support_required':2,'family_support_observed':sum(bool(x['survivor']) for x in per.values()),'candidate_survivor_count':sum(bool(x['candidate_holdout_survivor']) for x in results),'holdout_productive_count':sum(bool(x['holdout_productive']) for x in results),'holdout_stable_2of3_count':sum(bool(x['holdout_stable_2of3']) for x in results),'per_family':per}
def run(a:argparse.Namespace,*,repo_sha:str):
 repo=a.repo_root.resolve();p=verify_plan(a.holdout_plan,repo_root=repo);root=_root(a.output_root);prepared=_prepare(repo,p,a,root,repo_sha);schedules,members=_adapt(repo,p);record=root/'records';record.mkdir();binding={'schema_version':'cn_program_base_event_tier_a_holdout_input_binding_v1','repo_sha':repo_sha,'plan_payload_sha256':p['plan_payload_sha256'],'candidate_exact_identities_sha256':p['candidate_exact_identities_sha256'],'prepared_binding_payload_sha256':prepared['prepared_binding_payload_sha256'],'source_contract_sha256':p['source_data']['source_contract']['sha256'],'registry_sha256':p['source_data']['registry']['sha256'],'field_manifest_sha256':prepared['field_manifest_sha256'],'label_manifest_sha256':prepared['label_manifest_sha256'],'holdout_session_authority_manifest_sha256':prepared['holdout_session_authority_manifest_sha256'],'holdout_windows':list(p['holdout_windows']),'evaluation_role':'holdout','usage':'REPORT_ONLY_CANDIDATE_TRANSFER','optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN','validation_reads':0,'forward_2026_reads':0};binding['input_binding_sha256']=stable_hash(binding);_write(root/'input_binding.json',binding);started=time.perf_counter();results=[]
 with ProcessPoolExecutor(max_workers=int(a.workers),initializer=_initialize,initargs=(str(a.source_contract.resolve()),prepared['field_root'],prepared['label_root'],prepared['authority_root'],str(a.registry.resolve()),binding['input_binding_sha256'],str(record),tuple(members),tuple(p['holdout_windows']))) as ex:
  fut={ex.submit(_one,s,i):i for i,s in enumerate(schedules)};pending=set(fut)
  while pending:
   done,pending=wait(pending,timeout=2,return_when=FIRST_COMPLETED)
   for f in done:results.append(f.result())
   phase_b.engine._require_runtime_resource_safety(phase_b.engine._runtime_resource_snapshot())
 results.sort(key=lambda x:int(x['holdout_record_ordinal']))
 if len(results)!=2:raise RuntimeError('TIER_A_HOLDOUT_RESULT_COVERAGE_DRIFT')
 met=_metrics(p,results);_writejl(root/'candidate_holdout_summary.jsonl',results);_write(root/'mechanism_holdout_metrics.json',met);closure={'schema_version':'cn_program_base_event_tier_a_report_only_holdout_complete_v1','status':STATUS,'mechanism_support_status':met['status'],'repo_sha':repo_sha,'plan_payload_sha256':p['plan_payload_sha256'],'candidate_count':2,'candidate_exact_identities_sha256':p['candidate_exact_identities_sha256'],'candidate_survivor_count':met['candidate_survivor_count'],'holdout_productive_count':met['holdout_productive_count'],'holdout_stable_2of3_count':met['holdout_stable_2of3_count'],'mechanism_metrics':met,'input_binding_sha256':binding['input_binding_sha256'],'prepared_binding_payload_sha256':prepared['prepared_binding_payload_sha256'],'wall_seconds':time.perf_counter()-started,'holdout_windows':list(p['holdout_windows']),'holdout_reads_per_worker_context':max((int(x['holdout_reads']) for x in results),default=0),'validation_reads':0,'historical_challenge_reads':0,'forward_b_reads':0,'forward_2026_reads':0,'optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN','promotion_authorized':False,'automatic_successor_authorized':False,'oos_authority':'HOLDOUT_REPORT_ONLY_EVIDENCE_ONLY'};closure['closure_payload_sha256']=stable_hash(closure);_write(root/CLOSURE_NAME,closure);return closure
def parser():
 p=argparse.ArgumentParser();p.add_argument('--repo-root',type=Path,required=True);p.add_argument('--holdout-plan',type=Path,required=True);p.add_argument('--source-contract',type=Path,required=True);p.add_argument('--registry',type=Path,required=True);p.add_argument('--minute-source-root',type=Path,required=True);p.add_argument('--fundamental-root',type=Path,required=True);p.add_argument('--chip-root',type=Path,required=True);p.add_argument('--split-manifest',type=Path,required=True);p.add_argument('--public-source-root',type=Path,required=True);p.add_argument('--daily-st-source',type=Path,required=True);p.add_argument('--output-root',type=Path,required=True);p.add_argument('--repo-sha',required=True);p.add_argument('--workers',type=int,default=2);return p
def main(argv:Sequence[str]|None=None):
 a=parser().parse_args(argv);print(json.dumps(run(a,repo_sha=a.repo_sha),ensure_ascii=False,sort_keys=True));return 0
if __name__=='__main__':raise SystemExit(main())
