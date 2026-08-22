"""Project-Control entry for BASE_EVENT Tier-A report-only validation."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
from typing import Any,Mapping,Sequence

from our_system_phase2.services.node_resource_governor import validate_node_resource_lease_receipt
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,ProjectControlDenied,consume_active_admission,sha256_file,verify_campaign_authorization_binding,verify_consumed_admission_target
from our_system_phase2.services.unified_capability_registry import stable_hash

ROUTE_ID='cn-program-base-event-tier-a-report-only-validation-v1'
CAMPAIGN_ID='CN_PROGRAM_BASE_EVENT_TIER_A_REPORT_ONLY_VALIDATION_V1'
CAMPAIGN_PROFILE='cn_program_base_event_tier_a_report_only_validation_v1'
AUTHORIZATION_SCHEMA='cn_program_base_event_tier_a_report_only_validation_authorization_v1'
AUTHORIZATION_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_report_only_validation_authorization_v1.json')
PLAN_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_report_only_validation_plan.json')

def _read(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding='utf-8-sig'))
def _self(path:Path,field:str)->dict[str,Any]:
    row=_read(path); body=dict(row); claim=str(body.pop(field,''))
    if not claim or stable_hash(body)!=claim: raise ValueError(f'self-hash drift:{path}')
    return row

def verify_authorization(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
    root=Path(repo_root or Path(__file__).resolve().parents[3]).resolve(); p=_self(path.resolve(),'authorization_payload_sha256')
    if p.get('schema_version')!=AUTHORIZATION_SCHEMA or p.get('status')!='BASE_EVENT_TIER_A_REPORT_ONLY_VALIDATION_AUTHORIZED_NOT_RUN' or p.get('execution_authorized') is not True or p.get('campaign_id')!=CAMPAIGN_ID or p.get('campaign_profile')!=CAMPAIGN_PROFILE or p.get('project_control_route_id')!=ROUTE_ID or list(p.get('permitted_project_control_actions') or ())!=[ACTION_LAUNCH,ACTION_RETRY] or p.get('evaluation_role')!='validation' or p.get('usage')!='REPORT_ONLY_CANDIDATE_TRANSFER' or p.get('oos_authority')!='VALIDATION_REPORT_ONLY_EVIDENCE_ONLY' or p.get('promotion_authorized') is not False or p.get('automatic_successor_authorized') is not False: raise ValueError('tier-a validation authorization contract drift')
    if any(int(p.get(k) or 0)!=0 for k in ('holdout_reads','historical_challenge_reads','forward_b_reads','forward_2026_reads')) or p.get('optimizer_feedback_write')!='FORBIDDEN' or p.get('scheduler_write')!='FORBIDDEN' or p.get('archive_write')!='FORBIDDEN': raise ValueError('tier-a validation authority boundary drift')
    b=dict(p['validation_plan']); plan_path=(root/Path(str(b['relative_path']))).resolve()
    if not plan_path.is_relative_to(root) or not plan_path.is_file() or sha256_file(plan_path)!=str(b['file_sha256']): raise ValueError('tier-a validation plan file drift')
    plan=_self(plan_path,'plan_payload_sha256')
    if plan['plan_payload_sha256']!=b['payload_sha256'] or int(plan['candidate_count'])!=5: raise ValueError('tier-a validation plan payload drift')
    if dict(p.get('source_data') or {})!=dict(plan['source_data']) or dict(p.get('endpoint_contract') or {})!=dict(plan['endpoint_contract']) or list(p.get('validation_windows') or [])!=list(plan['validation_windows']): raise ValueError('tier-a validation authorization-plan drift')
    impl=dict(p['implementation']); runner=root/'scripts/run_cn_program_base_event_tier_a_report_only_validation_v1.py'
    if sha256_file(runner)!=str(impl['runner_source_file_sha256']) or sha256_file(Path(__file__).resolve())!=str(impl['runtime_source_file_sha256']): raise ValueError('tier-a validation implementation drift')
    rc=dict(p['resource_contract'])
    if rc!={'profile':'VALIDATION_DUAL_8','cpu_threads':8,'evaluator_workers':4,'candidate_count':5}: raise ValueError('tier-a validation resource contract drift')
    return p

def main(argv:Sequence[str]|None=None)->int:
    admission=consume_active_admission(ROUTE_ID,{ACTION_LAUNCH,ACTION_RETRY})
    q=argparse.ArgumentParser(description=__doc__); q.add_argument('--campaign-authorization',type=Path,required=True); q.add_argument('--validation-plan',type=Path,required=True); q.add_argument('--source-contract',type=Path,required=True); q.add_argument('--registry',type=Path,required=True); q.add_argument('--minute-source-root',type=Path,required=True); q.add_argument('--fundamental-root',type=Path,required=True); q.add_argument('--chip-root',type=Path,required=True); q.add_argument('--split-manifest',type=Path,required=True); q.add_argument('--public-source-root',type=Path,required=True); q.add_argument('--daily-st-source',type=Path,required=True); q.add_argument('--validation-label-root',type=Path,required=True); q.add_argument('--node-resource-lease-receipt',type=Path,required=True); q.add_argument('--output-root',type=Path,required=True); q.add_argument('--workers',type=int,default=4); a=q.parse_args(argv)
    verify_consumed_admission_target(admission,output_root=a.output_root); verified=verify_campaign_authorization_binding(admission,a.campaign_authorization); root=Path(__file__).resolve().parents[3]; auth=verify_authorization(verified.path,repo_root=root)
    if dict(verified.payload)!=auth: raise ProjectControlDenied('tier-a validation campaign authorization payload drift')
    if a.validation_plan.resolve()!=(root/PLAN_RELATIVE_PATH).resolve(): raise ProjectControlDenied('tier-a validation plan path outside authorization')
    if int(a.workers)!=4: raise ProjectControlDenied('tier-a validation worker count drift')
    validate_node_resource_lease_receipt(a.node_resource_lease_receipt.resolve(),expected_role='VALIDATION',expected_cpu_threads=8)
    from scripts.run_cn_program_base_event_tier_a_report_only_validation_v1 import run,verify_plan,_verify_sources
    plan=verify_plan(a.validation_plan,repo_root=root); _verify_sources(plan,a)
    result=run(argparse.Namespace(repo_root=root,validation_plan=a.validation_plan,source_contract=a.source_contract,registry=a.registry,minute_source_root=a.minute_source_root,fundamental_root=a.fundamental_root,chip_root=a.chip_root,split_manifest=a.split_manifest,public_source_root=a.public_source_root,daily_st_source=a.daily_st_source,validation_label_root=a.validation_label_root,output_root=a.output_root,workers=4),repo_sha=str(admission['repo_sha']))
    print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0
if __name__=='__main__': raise SystemExit(main())
