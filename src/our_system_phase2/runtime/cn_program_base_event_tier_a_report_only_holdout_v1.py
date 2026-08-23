"""Project-Control entry for BASE_EVENT Tier-A report-only holdout."""
from __future__ import annotations
import argparse,json
from pathlib import Path
from typing import Any,Sequence
from our_system_phase2.services.node_resource_governor import validate_node_resource_lease_receipt
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,ProjectControlDenied,consume_active_admission,sha256_file,verify_campaign_authorization_binding,verify_consumed_admission_target
from our_system_phase2.services.unified_capability_registry import stable_hash
ROUTE_ID='cn-program-base-event-tier-a-report-only-holdout-v1';CAMPAIGN_ID='CN_PROGRAM_BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_V1';CAMPAIGN_PROFILE='cn_program_base_event_tier_a_report_only_holdout_v1';AUTHORIZATION_SCHEMA='cn_program_base_event_tier_a_report_only_holdout_authorization_v1';AUTHORIZATION_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_report_only_holdout_authorization_v1.json');RECOVERY_AUTHORIZATION_SCHEMA='cn_program_base_event_tier_a_report_only_holdout_recovery_authorization_v1';RECOVERY_AUTHORIZATION_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_report_only_holdout_recovery_authorization_v1.json');PLAN_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_report_only_holdout_plan.json')
def _read(p:Path)->dict[str,Any]:return json.loads(p.read_text(encoding='utf-8-sig'))
def _self(p:Path,f:str)->dict[str,Any]:
 x=_read(p);b=dict(x);c=str(b.pop(f,''));
 if not c or stable_hash(b)!=c:raise ValueError(f'self-hash drift:{p}')
 return x
def verify_authorization(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
 root=Path(repo_root or Path(__file__).resolve().parents[3]).resolve();x=_self(path.resolve(),'authorization_payload_sha256');schema=x.get('schema_version');actions=list(x.get('permitted_project_control_actions') or ())
 if schema==AUTHORIZATION_SCHEMA:
  if x.get('status')!='BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_AUTHORIZED_NOT_RUN' or actions!=[ACTION_LAUNCH,ACTION_RETRY]:raise ValueError('tier-a holdout authorization contract drift')
 elif schema==RECOVERY_AUTHORIZATION_SCHEMA:
  if x.get('status')!='BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_RECOVERY_AUTHORIZED_NOT_RUN' or actions!=[ACTION_RETRY]:raise ValueError('tier-a holdout recovery authorization contract drift')
  ib=dict(x.get('recovery_incident') or {});ip=(root/Path(str(ib.get('relative_path') or ''))).resolve()
  if not ip.is_relative_to(root) or not ip.is_file() or sha256_file(ip)!=str(ib.get('file_sha256') or ''):raise ValueError('tier-a holdout recovery incident file drift')
  incident=_self(ip,'incident_payload_sha256')
  if incident.get('incident_payload_sha256')!=ib.get('payload_sha256') or incident.get('status')!='HOLDOUT_INFRASTRUCTURE_PRE_EVALUATION_FAILURE_RECOVERY_REQUIRED' or incident.get('failed_repo_sha')!='e69813c93f05db37459d06529957c5e79e11bf6d' or incident.get('holdout_governance',{}).get('ordinary_relaunch_forbidden') is not True or incident.get('holdout_governance',{}).get('recovery_requires_new_project_control_retry') is not True or incident.get('observed_failed_root_state',{}).get('candidate_evaluation_executed') is not False or incident.get('fix_contract',{}).get('scope')!='PRE_EVALUATION_INFRASTRUCTURE_COMPATIBILITY_ONLY' or incident.get('retry1_failure_assessment',{}).get('status')!='REMOTE_TRACE_CONFIRMED' or incident.get('retry1_failure_assessment',{}).get('confirmed_error')!='holdout field sidecar drift: holdout_reads':raise ValueError('tier-a holdout recovery incident semantic drift')
  ob=dict(x.get('original_authorization') or {});op=(root/Path(str(ob.get('relative_path') or ''))).resolve()
  if not op.is_relative_to(root) or not op.is_file() or sha256_file(op)!=str(ob.get('file_sha256') or ''):raise ValueError('tier-a holdout original authorization file drift')
  original=_self(op,'authorization_payload_sha256')
  if original.get('authorization_payload_sha256')!=ob.get('payload_sha256') or original.get('schema_version')!=AUTHORIZATION_SCHEMA or original.get('status')!='BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_AUTHORIZED_NOT_RUN' or original.get('campaign_id')!=CAMPAIGN_ID:raise ValueError('tier-a holdout original authorization semantic drift')
  rc=dict(x.get('recovery_contract') or {})
  if rc!={'recovery_kind':'INFRASTRUCTURE_PRE_EVALUATION_RETRY_FAILURE','failed_target_run_id':'cn_program_base_event_tier_a_report_only_holdout_retry1_20260823_5a04996','failed_output_root':'D:\\ChengboRemote\\runtime\\cn_program_base_event_tier_a_report_only_holdout_retry1_20260823_5a04996','failed_admission_file_sha256':'937a868f164b6cee132b677310cac1347d420f07571e9146a46e3c5ef7674f86','recovery_scope':'PRE_EVALUATION_INFRASTRUCTURE_COMPATIBILITY_ONLY','fresh_output_root_required':True,'candidate_set_change_forbidden':True,'holdout_windows_change_forbidden':True,'endpoint_change_forbidden':True,'source_binding_change_forbidden':True}:raise ValueError('tier-a holdout recovery contract drift')
 else:raise ValueError('tier-a holdout authorization schema drift')
 if x.get('execution_authorized') is not True or x.get('campaign_id')!=CAMPAIGN_ID or x.get('campaign_profile')!=CAMPAIGN_PROFILE or x.get('project_control_route_id')!=ROUTE_ID or x.get('evaluation_role')!='holdout' or x.get('usage')!='REPORT_ONLY_CANDIDATE_TRANSFER' or x.get('oos_authority')!='HOLDOUT_REPORT_ONLY_EVIDENCE_ONLY' or x.get('promotion_authorized') is not False or x.get('automatic_successor_authorized') is not False:raise ValueError('tier-a holdout authorization common contract drift')
 if any(int(x.get(k) or 0)!=0 for k in ('validation_reads','holdout_reads','historical_challenge_reads','forward_b_reads','forward_2026_reads')) or x.get('optimizer_feedback_write')!='FORBIDDEN' or x.get('scheduler_write')!='FORBIDDEN' or x.get('archive_write')!='FORBIDDEN':raise ValueError('tier-a holdout boundary drift')
 b=dict(x['holdout_plan']);pp=(root/Path(str(b['relative_path']))).resolve()
 if not pp.is_relative_to(root) or not pp.is_file() or sha256_file(pp)!=str(b['file_sha256']):raise ValueError('tier-a holdout plan file drift')
 p=_self(pp,'plan_payload_sha256')
 if p['plan_payload_sha256']!=b['payload_sha256'] or int(p['candidate_count'])!=2:raise ValueError('tier-a holdout plan payload drift')
 if dict(x.get('source_data') or {})!=dict(p['source_data']) or dict(x.get('endpoint_contract') or {})!=dict(p['endpoint_contract']) or list(x.get('holdout_windows') or [])!=list(p['holdout_windows']):raise ValueError('tier-a holdout authorization-plan drift')
 impl=dict(x['implementation']);runner=root/'scripts/run_cn_program_base_event_tier_a_report_only_holdout_v1.py';verifier=root/'scripts/verify_cn_report_only_session_authority.py';replay_validator=root/'scripts/run_cn_finalist_replay_then_oos.py';label_builder=root/'scripts/build_cn_phase3cm_forward_label_sidecars.py'
 if sha256_file(runner)!=str(impl['runner_source_file_sha256']) or sha256_file(Path(__file__).resolve())!=str(impl['runtime_source_file_sha256']) or sha256_file(verifier)!=str(impl['session_authority_verifier_sha256']) or sha256_file(replay_validator)!=str(impl['generic_replay_validator_sha256']) or sha256_file(label_builder)!=str(impl['forward_label_builder_sha256']):raise ValueError('tier-a holdout implementation drift')
 if dict(x['resource_contract'])!={'profile':'VALIDATION_DUAL_8','cpu_threads':8,'evaluator_workers':2,'candidate_count':2}:raise ValueError('tier-a holdout resource drift')
 return x
def main(argv:Sequence[str]|None=None)->int:
 admission=consume_active_admission(ROUTE_ID,{ACTION_LAUNCH,ACTION_RETRY});q=argparse.ArgumentParser(description=__doc__);q.add_argument('--campaign-authorization',type=Path,required=True);q.add_argument('--holdout-plan',type=Path,required=True);q.add_argument('--source-contract',type=Path,required=True);q.add_argument('--registry',type=Path,required=True);q.add_argument('--minute-source-root',type=Path,required=True);q.add_argument('--fundamental-root',type=Path,required=True);q.add_argument('--chip-root',type=Path,required=True);q.add_argument('--split-manifest',type=Path,required=True);q.add_argument('--public-source-root',type=Path,required=True);q.add_argument('--daily-st-source',type=Path,required=True);q.add_argument('--node-resource-lease-receipt',type=Path,required=True);q.add_argument('--output-root',type=Path,required=True);q.add_argument('--workers',type=int,default=2);a=q.parse_args(argv);verify_consumed_admission_target(admission,output_root=a.output_root);verified=verify_campaign_authorization_binding(admission,a.campaign_authorization);root=Path(__file__).resolve().parents[3];auth=verify_authorization(verified.path,repo_root=root)
 if dict(verified.payload)!=auth:raise ProjectControlDenied('tier-a holdout campaign authorization payload drift')
 if str(admission.get('requested_action') or '') not in set(auth.get('permitted_project_control_actions') or ()):raise ProjectControlDenied('tier-a holdout admission action is outside campaign authorization')
 if a.holdout_plan.resolve()!=(root/PLAN_RELATIVE_PATH).resolve() or int(a.workers)!=2:raise ProjectControlDenied('tier-a holdout route argument drift')
 validate_node_resource_lease_receipt(a.node_resource_lease_receipt.resolve(),expected_role='VALIDATION',expected_cpu_threads=8)
 from scripts.run_cn_program_base_event_tier_a_report_only_holdout_v1 import run,verify_plan,_verify_sources
 p=verify_plan(a.holdout_plan,repo_root=root);_verify_sources(p,a);result=run(argparse.Namespace(repo_root=root,holdout_plan=a.holdout_plan,source_contract=a.source_contract,registry=a.registry,minute_source_root=a.minute_source_root,fundamental_root=a.fundamental_root,chip_root=a.chip_root,split_manifest=a.split_manifest,public_source_root=a.public_source_root,daily_st_source=a.daily_st_source,output_root=a.output_root,workers=2),repo_sha=str(admission['repo_sha']));print(json.dumps(result,ensure_ascii=False,sort_keys=True));return 0
if __name__=='__main__':raise SystemExit(main())
