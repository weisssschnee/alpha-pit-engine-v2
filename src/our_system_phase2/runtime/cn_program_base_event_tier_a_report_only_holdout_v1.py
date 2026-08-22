"""Project-Control entry for BASE_EVENT Tier-A report-only holdout."""
from __future__ import annotations
import argparse,json
from pathlib import Path
from typing import Any,Sequence
from our_system_phase2.services.node_resource_governor import validate_node_resource_lease_receipt
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,ProjectControlDenied,consume_active_admission,sha256_file,verify_campaign_authorization_binding,verify_consumed_admission_target
from our_system_phase2.services.unified_capability_registry import stable_hash
ROUTE_ID='cn-program-base-event-tier-a-report-only-holdout-v1';CAMPAIGN_ID='CN_PROGRAM_BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_V1';CAMPAIGN_PROFILE='cn_program_base_event_tier_a_report_only_holdout_v1';AUTHORIZATION_SCHEMA='cn_program_base_event_tier_a_report_only_holdout_authorization_v1';AUTHORIZATION_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_report_only_holdout_authorization_v1.json');PLAN_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_report_only_holdout_plan.json')
def _read(p:Path)->dict[str,Any]:return json.loads(p.read_text(encoding='utf-8-sig'))
def _self(p:Path,f:str)->dict[str,Any]:
 x=_read(p);b=dict(x);c=str(b.pop(f,''));
 if not c or stable_hash(b)!=c:raise ValueError(f'self-hash drift:{p}')
 return x
def verify_authorization(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
 root=Path(repo_root or Path(__file__).resolve().parents[3]).resolve();x=_self(path.resolve(),'authorization_payload_sha256')
 if x.get('schema_version')!=AUTHORIZATION_SCHEMA or x.get('status')!='BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_AUTHORIZED_NOT_RUN' or x.get('execution_authorized') is not True or x.get('campaign_id')!=CAMPAIGN_ID or x.get('campaign_profile')!=CAMPAIGN_PROFILE or x.get('project_control_route_id')!=ROUTE_ID or list(x.get('permitted_project_control_actions') or ())!=[ACTION_LAUNCH,ACTION_RETRY] or x.get('evaluation_role')!='holdout' or x.get('usage')!='REPORT_ONLY_CANDIDATE_TRANSFER' or x.get('oos_authority')!='HOLDOUT_REPORT_ONLY_EVIDENCE_ONLY' or x.get('promotion_authorized') is not False or x.get('automatic_successor_authorized') is not False:raise ValueError('tier-a holdout authorization contract drift')
 if any(int(x.get(k) or 0)!=0 for k in ('validation_reads','holdout_reads','historical_challenge_reads','forward_b_reads','forward_2026_reads')) or x.get('optimizer_feedback_write')!='FORBIDDEN' or x.get('scheduler_write')!='FORBIDDEN' or x.get('archive_write')!='FORBIDDEN':raise ValueError('tier-a holdout boundary drift')
 b=dict(x['holdout_plan']);pp=(root/Path(str(b['relative_path']))).resolve()
 if not pp.is_relative_to(root) or not pp.is_file() or sha256_file(pp)!=str(b['file_sha256']):raise ValueError('tier-a holdout plan file drift')
 p=_self(pp,'plan_payload_sha256')
 if p['plan_payload_sha256']!=b['payload_sha256'] or int(p['candidate_count'])!=2:raise ValueError('tier-a holdout plan payload drift')
 if dict(x.get('source_data') or {})!=dict(p['source_data']) or dict(x.get('endpoint_contract') or {})!=dict(p['endpoint_contract']) or list(x.get('holdout_windows') or [])!=list(p['holdout_windows']):raise ValueError('tier-a holdout authorization-plan drift')
 impl=dict(x['implementation']);runner=root/'scripts/run_cn_program_base_event_tier_a_report_only_holdout_v1.py';verifier=root/'scripts/verify_cn_report_only_session_authority.py'
 if sha256_file(runner)!=str(impl['runner_source_file_sha256']) or sha256_file(Path(__file__).resolve())!=str(impl['runtime_source_file_sha256']) or sha256_file(verifier)!=str(impl['session_authority_verifier_sha256']):raise ValueError('tier-a holdout implementation drift')
 if dict(x['resource_contract'])!={'profile':'VALIDATION_DUAL_8','cpu_threads':8,'evaluator_workers':2,'candidate_count':2}:raise ValueError('tier-a holdout resource drift')
 return x
def main(argv:Sequence[str]|None=None)->int:
 admission=consume_active_admission(ROUTE_ID,{ACTION_LAUNCH,ACTION_RETRY});q=argparse.ArgumentParser(description=__doc__);q.add_argument('--campaign-authorization',type=Path,required=True);q.add_argument('--holdout-plan',type=Path,required=True);q.add_argument('--source-contract',type=Path,required=True);q.add_argument('--registry',type=Path,required=True);q.add_argument('--minute-source-root',type=Path,required=True);q.add_argument('--fundamental-root',type=Path,required=True);q.add_argument('--chip-root',type=Path,required=True);q.add_argument('--split-manifest',type=Path,required=True);q.add_argument('--public-source-root',type=Path,required=True);q.add_argument('--daily-st-source',type=Path,required=True);q.add_argument('--node-resource-lease-receipt',type=Path,required=True);q.add_argument('--output-root',type=Path,required=True);q.add_argument('--workers',type=int,default=2);a=q.parse_args(argv);verify_consumed_admission_target(admission,output_root=a.output_root);verified=verify_campaign_authorization_binding(admission,a.campaign_authorization);root=Path(__file__).resolve().parents[3];auth=verify_authorization(verified.path,repo_root=root)
 if dict(verified.payload)!=auth:raise ProjectControlDenied('tier-a holdout campaign authorization payload drift')
 if a.holdout_plan.resolve()!=(root/PLAN_RELATIVE_PATH).resolve() or int(a.workers)!=2:raise ProjectControlDenied('tier-a holdout route argument drift')
 validate_node_resource_lease_receipt(a.node_resource_lease_receipt.resolve(),expected_role='VALIDATION',expected_cpu_threads=8)
 from scripts.run_cn_program_base_event_tier_a_report_only_holdout_v1 import run,verify_plan,_verify_sources
 p=verify_plan(a.holdout_plan,repo_root=root);_verify_sources(p,a);result=run(argparse.Namespace(repo_root=root,holdout_plan=a.holdout_plan,source_contract=a.source_contract,registry=a.registry,minute_source_root=a.minute_source_root,fundamental_root=a.fundamental_root,chip_root=a.chip_root,split_manifest=a.split_manifest,public_source_root=a.public_source_root,daily_st_source=a.daily_st_source,output_root=a.output_root,workers=2),repo_sha=str(admission['repo_sha']));print(json.dumps(result,ensure_ascii=False,sort_keys=True));return 0
if __name__=='__main__':raise SystemExit(main())
