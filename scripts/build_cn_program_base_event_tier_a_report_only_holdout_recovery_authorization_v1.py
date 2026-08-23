from __future__ import annotations
import argparse,json
from pathlib import Path
from typing import Any
from scripts import run_cn_program_base_event_tier_a_report_only_holdout_v1 as runner
from our_system_phase2.runtime import cn_program_base_event_tier_a_report_only_holdout_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_RETRY,sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash

INCIDENT_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_holdout_recovery_incident_20260823.json')

def _read_self(path:Path,field:str)->dict[str,Any]:
 x=json.loads(path.read_text(encoding='utf-8-sig'));body=dict(x);claim=str(body.pop(field,''))
 if not claim or stable_hash(body)!=claim:raise ValueError(f'self-hash drift:{path}')
 return x

def build(repo:Path)->dict[str,Any]:
 repo=repo.resolve();pp=repo/runtime.PLAN_RELATIVE_PATH;p=runner.verify_plan(pp,repo_root=repo)
 original_path=repo/runtime.AUTHORIZATION_RELATIVE_PATH;original=_read_self(original_path,'authorization_payload_sha256')
 if original.get('schema_version')!=runtime.AUTHORIZATION_SCHEMA or original.get('status')!='BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_AUTHORIZED_NOT_RUN' or original.get('campaign_id')!=runtime.CAMPAIGN_ID:raise ValueError('original holdout authorization drift')
 incident_path=repo/INCIDENT_RELATIVE_PATH;incident=_read_self(incident_path,'incident_payload_sha256')
 if incident.get('status')!='HOLDOUT_INFRASTRUCTURE_PRE_EVALUATION_FAILURE_RECOVERY_REQUIRED' or incident.get('failed_repo_sha')!='e69813c93f05db37459d06529957c5e79e11bf6d' or incident.get('observed_failed_root_state',{}).get('candidate_evaluation_executed') is not False or incident.get('holdout_governance',{}).get('recovery_requires_new_project_control_retry') is not True or incident.get('fix_contract',{}).get('scope')!='SUBPROCESS_ENVIRONMENT_ONLY':raise ValueError('recovery incident drift')
 x={
  'schema_version':runtime.RECOVERY_AUTHORIZATION_SCHEMA,
  'status':'BASE_EVENT_TIER_A_REPORT_ONLY_HOLDOUT_RECOVERY_AUTHORIZED_NOT_RUN',
  'execution_authorized':True,
  'campaign_id':runtime.CAMPAIGN_ID,
  'campaign_profile':runtime.CAMPAIGN_PROFILE,
  'project_control_route_id':runtime.ROUTE_ID,
  'permitted_project_control_actions':[ACTION_RETRY],
  'evaluation_role':'holdout','usage':'REPORT_ONLY_CANDIDATE_TRANSFER',
  'holdout_plan':{'relative_path':str(runtime.PLAN_RELATIVE_PATH).replace('\\','/'),'file_sha256':sha256_file(pp),'payload_sha256':p['plan_payload_sha256'],'candidate_count':2,'candidate_exact_identities_sha256':p['candidate_exact_identities_sha256'],'family_ids':list(p['family_ids']),'required_physical_leaf_ids_sha256':p['required_physical_leaf_ids_sha256']},
  'original_authorization':{'relative_path':str(runtime.AUTHORIZATION_RELATIVE_PATH).replace('\\','/'),'file_sha256':sha256_file(original_path),'payload_sha256':original['authorization_payload_sha256']},
  'recovery_incident':{'relative_path':str(INCIDENT_RELATIVE_PATH).replace('\\','/'),'file_sha256':sha256_file(incident_path),'payload_sha256':incident['incident_payload_sha256']},
  'recovery_contract':{
   'recovery_kind':'INFRASTRUCTURE_PRE_EVALUATION_FAILURE',
   'failed_target_run_id':'cn_program_base_event_tier_a_report_only_holdout_20260823_e69813c',
   'failed_output_root':r'D:\ChengboRemote\runtime\cn_program_base_event_tier_a_report_only_holdout_20260823_e69813c',
   'failed_admission_file_sha256':'f70f343822f87b1666719dc1905c140e0e17edb14cb4e6e03853907ac52d6375',
   'recovery_scope':'SUBPROCESS_ENVIRONMENT_ONLY',
   'fresh_output_root_required':True,
   'candidate_set_change_forbidden':True,
   'holdout_windows_change_forbidden':True,
   'endpoint_change_forbidden':True,
   'source_binding_change_forbidden':True,
  },
  'source_data':dict(p['source_data']),'endpoint_contract':dict(p['endpoint_contract']),'holdout_windows':list(p['holdout_windows']),
  'implementation':{'runner_source_file_sha256':sha256_file(repo/'scripts/run_cn_program_base_event_tier_a_report_only_holdout_v1.py'),'runtime_source_file_sha256':sha256_file(repo/'src/our_system_phase2/runtime/cn_program_base_event_tier_a_report_only_holdout_v1.py'),'session_authority_verifier_sha256':sha256_file(repo/'scripts/verify_cn_report_only_session_authority.py')},
  'resource_contract':dict(p['resource_contract']),
  'optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN',
  'validation_reads':0,'holdout_reads':0,'historical_challenge_reads':0,'forward_b_reads':0,'forward_2026_reads':0,
  'oos_authority':'HOLDOUT_REPORT_ONLY_EVIDENCE_ONLY','promotion_authorized':False,'automatic_successor_authorized':False,
 }
 x['authorization_payload_sha256']=stable_hash(x);return x

def main(argv=None):
 q=argparse.ArgumentParser();q.add_argument('--repo-root',type=Path,default=Path(__file__).resolve().parents[1]);q.add_argument('--output',type=Path,default=runtime.RECOVERY_AUTHORIZATION_RELATIVE_PATH);a=q.parse_args(argv);x=build(a.repo_root);out=a.output if a.output.is_absolute() else a.repo_root/a.output;out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(x,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8');print(json.dumps({'status':x['status'],'authorization_payload_sha256':x['authorization_payload_sha256'],'output':str(out.resolve())},sort_keys=True))
if __name__=='__main__':main()
