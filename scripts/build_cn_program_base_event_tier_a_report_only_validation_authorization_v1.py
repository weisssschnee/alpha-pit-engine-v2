from __future__ import annotations
import argparse,json
from pathlib import Path
from typing import Any

from scripts import run_cn_program_base_event_tier_a_report_only_validation_v1 as runner
from our_system_phase2.runtime import cn_program_base_event_tier_a_report_only_validation_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash

def build(repo:Path)->dict[str,Any]:
    repo=repo.resolve(); plan_path=repo/runtime.PLAN_RELATIVE_PATH; plan=runner.verify_plan(plan_path,repo_root=repo)
    payload={'schema_version':runtime.AUTHORIZATION_SCHEMA,'status':'BASE_EVENT_TIER_A_REPORT_ONLY_VALIDATION_AUTHORIZED_NOT_RUN','execution_authorized':True,'campaign_id':runtime.CAMPAIGN_ID,'campaign_profile':runtime.CAMPAIGN_PROFILE,'project_control_route_id':runtime.ROUTE_ID,'permitted_project_control_actions':[ACTION_LAUNCH,ACTION_RETRY],'evaluation_role':'validation','usage':'REPORT_ONLY_CANDIDATE_TRANSFER','validation_plan':{'relative_path':str(runtime.PLAN_RELATIVE_PATH).replace('\\','/'),'file_sha256':sha256_file(plan_path),'payload_sha256':plan['plan_payload_sha256'],'candidate_count':5,'candidate_exact_identities_sha256':plan['candidate_exact_identities_sha256'],'family_ids':list(plan['family_ids']),'required_physical_leaf_ids_sha256':plan['required_physical_leaf_ids_sha256']},'source_data':dict(plan['source_data']),'endpoint_contract':dict(plan['endpoint_contract']),'implementation':{'runner_source_file_sha256':sha256_file(repo/'scripts/run_cn_program_base_event_tier_a_report_only_validation_v1.py'),'runtime_source_file_sha256':sha256_file(repo/'src/our_system_phase2/runtime/cn_program_base_event_tier_a_report_only_validation_v1.py')},'resource_contract':dict(plan['resource_contract']),'validation_windows':list(plan['validation_windows']),'optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN','holdout_reads':0,'historical_challenge_reads':0,'forward_b_reads':0,'forward_2026_reads':0,'oos_authority':'VALIDATION_REPORT_ONLY_EVIDENCE_ONLY','promotion_authorized':False,'automatic_successor_authorized':False}
    payload['authorization_payload_sha256']=stable_hash(payload); return payload

def main(argv=None)->None:
    p=argparse.ArgumentParser(); p.add_argument('--repo-root',type=Path,default=Path(__file__).resolve().parents[1]); p.add_argument('--output',type=Path,default=runtime.AUTHORIZATION_RELATIVE_PATH); a=p.parse_args(argv); row=build(a.repo_root); out=a.output if a.output.is_absolute() else a.repo_root/a.output; out.parent.mkdir(parents=True,exist_ok=True); out.write_text(json.dumps(row,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8'); print(json.dumps({'status':row['status'],'authorization_payload_sha256':row['authorization_payload_sha256'],'output':str(out.resolve())},sort_keys=True))
if __name__=='__main__': main()
