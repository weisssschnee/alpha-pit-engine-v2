"""Report-only validation for the frozen five-member BASE_EVENT Tier-A cohort."""
from __future__ import annotations
import argparse,csv,gc,hashlib,json,subprocess,sys,time
from concurrent.futures import FIRST_COMPLETED,ProcessPoolExecutor,wait
from pathlib import Path
from typing import Any,Mapping,Sequence

PROJECT_ROOT=Path(__file__).resolve().parents[1]
for _p in (PROJECT_ROOT,PROJECT_ROOT/'src'):
    if str(_p) not in sys.path: sys.path.insert(0,str(_p))

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_portfolio_decoder_v2_oos as oos
from scripts import run_cn_program_optimizer_d1_report_only_validation_v1 as d1
from our_system_phase2.services.unified_capability_registry import stable_hash

PLAN_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_report_only_validation_plan.json')
CLOSURE_NAME='CN_PROGRAM_BASE_EVENT_TIER_A_REPORT_ONLY_VALIDATION_COMPLETE.json'
STATUS='BASE_EVENT_TIER_A_REPORT_ONLY_VALIDATION_COMPLETE'
SUPPORTED='BASE_EVENT_TIER_A_VALIDATION_MECHANISM_TRANSFER_SUPPORTED'
NOT_SUPPORTED='BASE_EVENT_TIER_A_VALIDATION_MECHANISM_TRANSFER_NOT_SUPPORTED'


def _read_json(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding='utf-8-sig'))
def _read_jsonl(path:Path)->list[dict[str,Any]]: return [json.loads(x) for x in path.read_text(encoding='utf-8-sig').splitlines() if x.strip()]
def _write_json(path:Path,payload:Mapping[str,Any])->Path:
    path.parent.mkdir(parents=True,exist_ok=True); path.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8'); return path
def _write_jsonl(path:Path,rows:Sequence[Mapping[str,Any]])->Path:
    path.parent.mkdir(parents=True,exist_ok=True); path.write_text(''.join(json.dumps(dict(x),ensure_ascii=False,sort_keys=True)+'\n' for x in rows),encoding='utf-8'); return path
def _sha(path:Path)->str: return hashlib.sha256(path.read_bytes()).hexdigest()
def _self_hashed(path:Path,field:str)->dict[str,Any]:
    row=_read_json(path); body=dict(row); claim=str(body.pop(field,''))
    if not claim or stable_hash(body)!=claim: raise RuntimeError(f'SELF_HASH_DRIFT:{path}')
    return row

def _bound(repo:Path,b:Mapping[str,Any],field:str|None=None)->dict[str,Any]|list[dict[str,Any]]:
    p=(repo/Path(str(b['relative_path']))).resolve()
    if not p.is_relative_to(repo.resolve()) or not p.is_file() or _sha(p)!=str(b['file_sha256']): raise RuntimeError(f'BOUND_FILE_DRIFT:{p}')
    if p.suffix.lower()=='.jsonl':
        rows=_read_jsonl(p)
        if field and stable_hash(rows)!=str(b.get('payload_sha256') or ''): raise RuntimeError(f'BOUND_JSONL_PAYLOAD_DRIFT:{p}')
        return rows
    row=_self_hashed(p,field) if field else _read_json(p)
    if field and str(row[field])!=str(b.get('payload_sha256') or ''): raise RuntimeError(f'BOUND_PAYLOAD_DRIFT:{p}')
    return row

def verify_plan(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
    repo=Path(repo_root or PROJECT_ROOT).resolve(); plan=_self_hashed(path.resolve(),'plan_payload_sha256')
    if plan.get('schema_version')!='cn_program_base_event_tier_a_report_only_validation_plan_v1' or plan.get('status')!='BASE_EVENT_TIER_A_REPORT_ONLY_VALIDATION_PLAN_FROZEN_NOT_RUN': raise RuntimeError('TIER_A_VALIDATION_PLAN_STATUS_DRIFT')
    if int(plan.get('candidate_count') or 0)!=5 or len(plan.get('family_ids') or [])!=2 or len(plan.get('required_physical_leaf_ids') or [])!=8: raise RuntimeError('TIER_A_VALIDATION_PLAN_CARDINALITY_DRIFT')
    if plan.get('evaluation_role')!='validation' or plan.get('usage')!='REPORT_ONLY_CANDIDATE_TRANSFER' or plan.get('validation_access_authorized') is not False: raise RuntimeError('TIER_A_VALIDATION_PLAN_ROLE_DRIFT')
    if any(int(plan.get(k) or 0)!=0 for k in ('holdout_reads','historical_challenge_reads','forward_b_reads','forward_2026_reads')) or plan.get('promotion_authorized') is not False or plan.get('automatic_successor_authorized') is not False: raise RuntimeError('TIER_A_VALIDATION_PLAN_BOUNDARY_DRIFT')
    freeze=_bound(repo,plan['candidate_freeze'],'freeze_payload_sha256'); members=_bound(repo,plan['candidate_members'],'candidate_members_payload_sha256'); decision=_bound(repo,plan['decision_packet'],'decision_payload_sha256'); pre=_bound(repo,plan['selection_preflight'],'preflight_payload_sha256'); infra=_bound(repo,plan['infrastructure_review'],'review_payload_sha256'); schedules=_bound(repo,plan['resolved_schedules'])
    assert isinstance(freeze,dict) and isinstance(members,list) and isinstance(decision,dict) and isinstance(pre,dict) and isinstance(infra,dict) and isinstance(schedules,list)
    exacts=sorted(str(x['exact_identity']) for x in members)
    if len(exacts)!=5 or stable_hash(exacts)!=str(plan['candidate_exact_identities_sha256']) or sorted(str(x.get('optimizer_ask',{}).get('exact_identity') or '') for x in schedules)!=exacts: raise RuntimeError('TIER_A_VALIDATION_EXACT_BINDING_DRIFT')
    if freeze.get('validation_reads_performed_by_freeze')!=0 or decision.get('validation_access_authorized') is not False or int(pre.get('validation_reads') or 0)!=0 or pre.get('candidate_evaluation_executed') is not False: raise RuntimeError('TIER_A_VALIDATION_PREFINANCIAL_BOUNDARY_DRIFT')
    if list(pre['required_physical_leaf_ids'])!=list(plan['required_physical_leaf_ids']) or pre['required_physical_leaf_ids_sha256']!=plan['required_physical_leaf_ids_sha256']: raise RuntimeError('TIER_A_VALIDATION_FIELD_DRIFT')
    if dict(infra.get('implementation_decision') or {}).get('context_strategy')!='BUILD_FRESH_8_FIELD_VALIDATION_CONTEXT_ONLY_AFTER_PROJECT_CONTROL_ADMISSION': raise RuntimeError('TIER_A_VALIDATION_INFRA_STRATEGY_DRIFT')
    rc=dict(plan['resource_contract'])
    if rc!={'profile':'VALIDATION_DUAL_8','cpu_threads':8,'evaluator_workers':4,'candidate_count':5}: raise RuntimeError('TIER_A_VALIDATION_RESOURCE_DRIFT')
    return plan

def _verify_file(path:Path,sha256:str,label:str)->None:
    if not path.is_file() or _sha(path)!=sha256: raise RuntimeError(f'{label}_DRIFT')
def _verify_sources(plan:Mapping[str,Any],args:argparse.Namespace)->None:
    s=dict(plan['source_data'])
    path_pairs=(('minute_source_root',args.minute_source_root),('fundamental_root',args.fundamental_root),('chip_root',args.chip_root),('public_source_root',args.public_source_root),('validation_label_root',args.validation_label_root))
    for key,p in path_pairs:
        if str(Path(p).resolve())!=str(Path(s[key]).resolve()): raise RuntimeError(f'{key.upper()}_PATH_DRIFT')
    file_pairs=(('source_contract',args.source_contract),('registry',args.registry),('split_manifest',args.split_manifest),('daily_st_source',args.daily_st_source))
    for key,p in file_pairs:
        b=dict(s[key]); rp=Path(p).resolve()
        if str(rp)!=str(Path(b['path']).resolve()): raise RuntimeError(f'{key.upper()}_PATH_DRIFT')
        _verify_file(rp,str(b['sha256']),key.upper())
    for key in ('chip_manifest','public_source_manifest','validation_label_manifest'):
        b=dict(s[key]); _verify_file(Path(b['path']),str(b['sha256']),key.upper())

def _requirements(path:Path,fields:Sequence[str])->Path:
    with path.open('w',encoding='utf-8-sig',newline='') as h:
        w=csv.DictWriter(h,fieldnames=['candidate_id','expression']); w.writeheader()
        for f in fields: w.writerow({'candidate_id':f'required::{f}','expression':f'${f}'})
    return path

def _run(xs:Sequence[Any])->None:
    cp=subprocess.run([str(x) for x in xs],text=True,capture_output=True)
    if cp.returncode: raise RuntimeError(f'SUBPROCESS_FAILED:{xs[1] if len(xs)>1 else xs[0]}\nOUT={cp.stdout}\nERR={cp.stderr}')

def _admitted_root(root:Path)->Path:
    root=root.resolve()
    if not root.is_dir() or not (root/'.project_control_execution').is_dir(): raise RuntimeError('TIER_A_VALIDATION_ADMITTED_OUTPUT_ROOT_MISSING')
    unexpected={p.name for p in root.iterdir() if p.name!='.project_control_execution'}
    if unexpected: raise RuntimeError('TIER_A_VALIDATION_ADMITTED_OUTPUT_ROOT_NOT_CLEAN:'+','.join(sorted(unexpected)))
    return root

def _prepare(repo:Path,plan:Mapping[str,Any],args:argparse.Namespace,output_root:Path,repo_sha:str)->dict[str,Any]:
    _verify_sources(plan,args); prep=output_root/'validation_preparation'; prep.mkdir()
    fields=tuple(map(str,plan['required_physical_leaf_ids'])); req=_requirements(prep/'validation_program_required_fields.csv',fields)
    field_root=prep/'program_validation_session_fields'
    _run([sys.executable,repo/'scripts/build_cn_core_pack_validation_session_sidecar.py','--source-root',args.minute_source_root,'--evaluation-role','validation','--output-root',field_root,'--candidate-table',req,'--registry',args.registry,'--split-manifest',args.split_manifest,'--split-manifest-hash',plan['source_data']['split_manifest']['sha256'],'--fundamental-root',args.fundamental_root,'--chip-root',args.chip_root,'--max-shards','16'])
    field_manifest=field_root/'CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json'; field_sha=_sha(field_manifest)
    auth_root=prep/'validation_session_authority'
    _run([sys.executable,repo/'scripts/build_cn_validation_session_authority.py','--field-manifest',field_manifest,'--public-source-root',args.public_source_root,'--output-root',auth_root,'--expected-field-manifest-sha256',field_sha,'--expected-source-manifest-sha256',plan['source_data']['public_source_manifest']['sha256'],'--historical-daily-st-source',args.daily_st_source,'--expected-daily-st-source-sha256',plan['source_data']['daily_st_source']['sha256'],'--builder-commit-sha',repo_sha,'--evaluation-role','validation'])
    audit_root=prep/'validation_session_authority_audit'; _run([sys.executable,repo/'scripts/verify_cn_validation_session_authority.py','--authority-root',auth_root,'--output-root',audit_root])
    audit=_read_json(audit_root/'audit.json')
    if audit.get('status')!='PASS_INDEPENDENT_VALIDATION_SESSION_AUTHORITY_VERIFICATION': raise RuntimeError('TIER_A_VALIDATION_AUTHORITY_AUDIT_FAIL')
    context=oos._load_validation_context(source_contract_path=args.source_contract.resolve(),validation_field_root=field_root,validation_label_root=args.validation_label_root.resolve(),validation_session_authority_root=auth_root)
    missing=sorted(set(fields)-set(context['field_frame'].columns))
    if missing: raise RuntimeError('TIER_A_VALIDATION_FIELDS_MISSING:'+','.join(missing))
    prepared={'schema_version':'cn_program_base_event_tier_a_validation_prepared_binding_v1','status':'BASE_EVENT_TIER_A_VALIDATION_CONTEXT_READY_AFTER_ADMISSION','repo_sha':repo_sha,'plan_payload_sha256':plan['plan_payload_sha256'],'candidate_exact_identities_sha256':plan['candidate_exact_identities_sha256'],'candidate_count':5,'required_physical_leaf_ids':list(fields),'required_physical_leaf_ids_sha256':plan['required_physical_leaf_ids_sha256'],'requirements_table_file_sha256':_sha(req),'validation_field_root':str(field_root),'validation_field_manifest_sha256':field_sha,'validation_label_root':str(args.validation_label_root.resolve()),'validation_label_manifest_sha256':plan['source_data']['validation_label_manifest']['sha256'],'validation_session_authority_root':str(auth_root),'validation_session_authority_manifest_sha256':_sha(auth_root/'validation_session_authority_manifest.json'),'validation_session_authority_audit_sha256':_sha(audit_root/'audit.json'),'validation_windows':list(plan['validation_windows']),'validation_reads_during_context_smoke':int(context['validation_reads']),'holdout_reads':0,'forward_2026_reads':0,'candidate_evaluation_executed':False,'optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN','promotion':'FORBIDDEN'}
    prepared['prepared_binding_payload_sha256']=stable_hash(prepared); _write_json(prep/'BASE_EVENT_TIER_A_VALIDATION_CONTEXT_READY.json',prepared); return prepared

def _adapt(repo:Path,plan:Mapping[str,Any])->tuple[list[dict[str,Any]],list[dict[str,Any]]]:
    schedules=_read_jsonl(repo/Path(plan['resolved_schedules']['relative_path'])); members=_read_jsonl(repo/Path(plan['candidate_members']['relative_path'])); by={str(x['exact_identity']):dict(x) for x in members}; out=[]; adapted=[]
    for schedule in schedules:
        exact=str(dict(schedule.get('optimizer_ask') or {}).get('exact_identity') or ''); member=by[exact]
        body={k:v for k,v in schedule.items() if k!='schedule_record_sha256'}
        if stable_hash(body)!=str(schedule['schedule_record_sha256']) or str(member['schedule_record_sha256'])!=str(schedule['schedule_record_sha256']) or str(member['program_id'])!=str(schedule['primary_program']['program_id']) or str(member['control_program_id'])!=str(schedule['control_program']['program_id']) or str(member['pair_id'])!=str(schedule['pair_id']): raise RuntimeError('TIER_A_VALIDATION_SCHEDULE_MEMBER_DRIFT')
        s=dict(schedule); s['successor_exact_identity']=exact; out.append(s)
        m=dict(member); m['source_wave']=int(str(member['source_checkpoint']).split('_')[-1]); adapted.append(m)
    if len(out)!=5 or len({str(x['successor_exact_identity']) for x in out})!=5: raise RuntimeError('TIER_A_VALIDATION_ADAPTER_CARDINALITY_DRIFT')
    return out,adapted

def _candidate_summaries(results:Sequence[Mapping[str,Any]],members:Sequence[Mapping[str,Any]],record_root:Path)->list[dict[str,Any]]:
    by={str(x['exact_identity']):dict(x) for x in members}; summaries=[]
    for result in results:
        exact=str(result['exact_identity']); member=by[exact]; ordinal=int(result['validation_record_ordinal']); payload=_read_json(record_root/f'candidate_{ordinal:03d}_{exact[:12]}.json'); pair=dict(payload['pair_record']); uplift=dict(result.get('uplift') or {}); pc=dict(uplift.get('program_credit') or {}); cross=int(pc.get('cross_window_positive_increment_count') or 0); replay_complete=str(pair.get('replay_status'))=='PAIR_REPLAY_COMPLETE'; survivor=bool(replay_complete and result.get('validation_productive') and cross>=2)
        row={'candidate_id':member['candidate_id'],'exact_identity':exact,'family_id':member['family_id'],'family':member['family'],'source_replicate':member['source_replicate'],'source_main_record_ordinal':member['source_main_record_ordinal'],'validation_record_ordinal':ordinal,'replay_complete':replay_complete,'validation_admitted':bool(dict(result['admission']).get('admitted')),'validation_productive':bool(result['validation_productive']),'cross_window_positive_increment_count':cross,'validation_stable_2of3':bool(result['validation_productive'] and cross>=2),'candidate_survivor':survivor,'pair_record_sha256':result['pair_record_sha256'],'result_payload_sha256':result['result_payload_sha256']}; row['summary_payload_sha256']=stable_hash(row); summaries.append(row)
    return sorted(summaries,key=lambda x:int(x['validation_record_ordinal']))
def _mechanism_metrics(plan:Mapping[str,Any],summaries:Sequence[Mapping[str,Any]])->dict[str,Any]:
    per={}
    for fid in plan['family_ids']:
        rows=[x for x in summaries if str(x['family_id'])==str(fid)]; survivors=sum(bool(x['candidate_survivor']) for x in rows); per[str(fid)]={'candidate_count':len(rows),'survivor_count':survivors,'supported':survivors>=1,'candidate_exact_identities':sorted(str(x['exact_identity']) for x in rows)}
    supported=all(x['supported'] for x in per.values())
    return {'status':SUPPORTED if supported else NOT_SUPPORTED,'family_support_required':len(per),'family_support_observed':sum(bool(x['supported']) for x in per.values()),'per_family':per,'candidate_survivor_count':sum(bool(x['candidate_survivor']) for x in summaries),'validation_productive_count':sum(bool(x['validation_productive']) for x in summaries),'validation_stable_2of3_count':sum(bool(x['validation_stable_2of3']) for x in summaries)}

def run(args:argparse.Namespace,*,repo_sha:str)->dict[str,Any]:
    repo=args.repo_root.resolve(); plan=verify_plan(args.validation_plan,repo_root=repo); root=_admitted_root(args.output_root); prepared=_prepare(repo,plan,args,root,repo_sha); schedules,members=_adapt(repo,plan); record_root=root/'records'; record_root.mkdir()
    input_binding={'schema_version':'cn_program_base_event_tier_a_validation_input_binding_v1','repo_sha':repo_sha,'plan_payload_sha256':plan['plan_payload_sha256'],'candidate_exact_identities_sha256':plan['candidate_exact_identities_sha256'],'prepared_binding_payload_sha256':prepared['prepared_binding_payload_sha256'],'source_contract_sha256':plan['source_data']['source_contract']['sha256'],'registry_sha256':plan['source_data']['registry']['sha256'],'validation_field_manifest_sha256':prepared['validation_field_manifest_sha256'],'validation_session_authority_manifest_sha256':prepared['validation_session_authority_manifest_sha256'],'validation_windows':list(plan['validation_windows']),'evaluation_role':'validation','usage':'REPORT_ONLY_CANDIDATE_TRANSFER','optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN','holdout_reads':0,'forward_2026_reads':0}; input_binding['input_binding_sha256']=stable_hash(input_binding); _write_json(root/'input_binding.json',input_binding)
    started=time.perf_counter(); results=[]
    with ProcessPoolExecutor(max_workers=int(args.workers),initializer=d1._initialize_worker,initargs=(str(args.source_contract.resolve()),prepared['validation_field_root'],str(args.validation_label_root.resolve()),prepared['validation_session_authority_root'],str(args.registry.resolve()),input_binding['input_binding_sha256'],str(record_root),tuple(members))) as ex:
        futures={ex.submit(d1._evaluate_one,s,ordinal=i):i for i,s in enumerate(schedules)}; pending=set(futures)
        while pending:
            done,pending=wait(pending,timeout=2.0,return_when=FIRST_COMPLETED)
            for f in done: results.append(f.result())
            engine._require_runtime_resource_safety(engine._runtime_resource_snapshot())
    results.sort(key=lambda x:int(x['validation_record_ordinal']))
    if len(results)!=5: raise RuntimeError('TIER_A_VALIDATION_RESULT_COVERAGE_DRIFT')
    summaries=_candidate_summaries(results,members,record_root); metrics=_mechanism_metrics(plan,summaries); _write_jsonl(root/'candidate_validation_summary.jsonl',summaries); _write_json(root/'mechanism_validation_metrics.json',metrics)
    closure={'schema_version':'cn_program_base_event_tier_a_report_only_validation_complete_v1','status':STATUS,'mechanism_support_status':metrics['status'],'repo_sha':repo_sha,'plan_payload_sha256':plan['plan_payload_sha256'],'candidate_count':5,'candidate_exact_identities_sha256':plan['candidate_exact_identities_sha256'],'candidate_survivor_count':metrics['candidate_survivor_count'],'validation_productive_count':metrics['validation_productive_count'],'validation_stable_2of3_count':metrics['validation_stable_2of3_count'],'mechanism_metrics':metrics,'input_binding_sha256':input_binding['input_binding_sha256'],'prepared_binding_payload_sha256':prepared['prepared_binding_payload_sha256'],'wall_seconds':time.perf_counter()-started,'validation_windows':list(plan['validation_windows']),'validation_reads_per_worker_context':max((int(x.get('validation_reads') or 0) for x in results),default=0),'holdout_reads':0,'historical_challenge_reads':0,'forward_b_reads':0,'forward_2026_reads':0,'optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN','promotion_authorized':False,'automatic_successor_authorized':False,'oos_authority':'VALIDATION_REPORT_ONLY_EVIDENCE_ONLY'}; closure['closure_payload_sha256']=stable_hash(closure); _write_json(root/CLOSURE_NAME,closure); gc.collect(); return closure

def parser()->argparse.ArgumentParser:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--repo-root',type=Path,required=True); p.add_argument('--validation-plan',type=Path,required=True); p.add_argument('--source-contract',type=Path,required=True); p.add_argument('--registry',type=Path,required=True); p.add_argument('--minute-source-root',type=Path,required=True); p.add_argument('--fundamental-root',type=Path,required=True); p.add_argument('--chip-root',type=Path,required=True); p.add_argument('--split-manifest',type=Path,required=True); p.add_argument('--public-source-root',type=Path,required=True); p.add_argument('--daily-st-source',type=Path,required=True); p.add_argument('--validation-label-root',type=Path,required=True); p.add_argument('--output-root',type=Path,required=True); p.add_argument('--repo-sha',required=True); p.add_argument('--workers',type=int,default=4); return p

def main(argv:Sequence[str]|None=None)->int:
    a=parser().parse_args(argv); print(json.dumps(run(a,repo_sha=a.repo_sha),ensure_ascii=False,sort_keys=True)); return 0
if __name__=='__main__': raise SystemExit(main())
