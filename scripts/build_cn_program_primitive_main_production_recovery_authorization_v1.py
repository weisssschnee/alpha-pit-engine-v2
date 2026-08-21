from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Any
from our_system_phase2.runtime import cn_program_primitive_main_production_v1 as source_runtime
from our_system_phase2.runtime import cn_program_primitive_main_production_recovery_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH, ACTION_RETRY, sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash

def read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding='utf-8-sig'))
def selfhash(p:Path,field:str)->tuple[dict[str,Any],str]:
 row=read(p);body=dict(row);claim=str(body.pop(field,''))
 if not claim or stable_hash(body)!=claim: raise ValueError(f'self-hash drift:{p}')
 return row,claim

def build(repo:Path,canary:Path)->dict[str,Any]:
 repo=repo.resolve();source_path=repo/source_runtime.AUTHORIZATION_RELATIVE_PATH;source=source_runtime.verify_authorization(source_path,repo_root=repo)
 recovery_path=repo/runtime.RECOVERY_RELATIVE_PATH;recovery,recovery_hash=selfhash(recovery_path,'recovery_prefix_payload_sha256')
 raw=canary.resolve().read_bytes();canary_file=hashlib.sha256(raw).hexdigest();c=json.loads(raw.decode('utf-8-sig'));body=dict(c);canary_hash=str(body.pop('official_canary_payload_sha256',''))
 if not canary_hash or stable_hash(body)!=canary_hash or c.get('status')!='PASS' or c.get('candidate_evaluation_executed') is not False or c.get('financial_evaluator_reexecution_performed') is not False: raise ValueError('recovery canary not PASS')
 runner=repo/'scripts/run_cn_program_primitive_main_production_recovery_v1.py';proposal=repo/'src/our_system_phase2/services/candidate_program_proposal_v0.py'
 if c.get('recovery_runner_source_file_sha256')!=sha256_file(runner) or c.get('proposal_registry_source_file_sha256')!=sha256_file(proposal): raise ValueError('recovery canary implementation binding drift')
 payload={'schema_version':runtime.AUTHORIZATION_SCHEMA,'status':'PRIMITIVE_MAIN_PRODUCTION_RECOVERY_AUTHORIZED_NOT_RUN','execution_authorized':True,'campaign_id':runtime.CAMPAIGN_ID,'campaign_profile':runtime.CAMPAIGN_PROFILE,'project_control_route_id':runtime.ROUTE_ID,'permitted_project_control_actions':[ACTION_LAUNCH,ACTION_RETRY],'evaluation_data_role':'DEVELOPMENT_ONLY','source_production_authorization':{'relative_path':str(source_runtime.AUTHORIZATION_RELATIVE_PATH).replace('\\','/'),'file_sha256':sha256_file(source_path),'payload_sha256':source['authorization_payload_sha256'],'production_plan_relative_path':str(source['production_plan']['relative_path'])},'source_prior_exact':dict(source['source_prior_exact']),'source_evaluator_authority':dict(source['source_evaluator_authority']),'recovery_prefix':{'relative_path':str(runtime.RECOVERY_RELATIVE_PATH).replace('\\','/'),'file_sha256':sha256_file(recovery_path),'payload_sha256':recovery_hash,'source_repo_sha':recovery['source_repo_sha'],'completed_logical_records':24,'remaining_new_evaluations':816,'final_total_logical_records':840,'source_checkpoint_manifest_file_sha256':recovery['source_checkpoint_manifest_file_sha256']},'implementation':{'recovery_runner_source_file_sha256':sha256_file(runner),'proposal_registry_source_file_sha256':sha256_file(proposal)},'resource_contract':{'profile':'SEARCH_DUAL_24','cpu_threads':24,'primary_executor_workers':24,'fallback_executor_workers':16,'checkpoint_size':24,'recovered_records':24,'remaining_new_evaluations':816,'final_hard_cap':840,'field_column_count':int(c['field_column_count']),'field_columns_sha256':str(c['field_columns_sha256'])},'official_resource_canary':{'file_sha256':canary_file,'payload_sha256':canary_hash,'field_column_count':int(c['field_column_count']),'field_columns_sha256':str(c['field_columns_sha256']),'recovery_runner_source_file_sha256':sha256_file(runner)},'financial_evaluator_reexecution_authorized':False,'restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0},'validation_feedback_used':False,'promotion_authorized':False,'oos_authority':'NONE','automatic_successor_authorized':False}
 payload['authorization_payload_sha256']=stable_hash(payload);return payload

def main(argv=None)->int:
 p=argparse.ArgumentParser();p.add_argument('--repo-root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--official-resource-canary',type=Path,required=True);p.add_argument('--output',type=Path,default=runtime.AUTHORIZATION_RELATIVE_PATH);a=p.parse_args(argv);payload=build(a.repo_root,a.official_resource_canary);out=a.output if a.output.is_absolute() else a.repo_root/a.output;out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8');print(json.dumps({'status':payload['status'],'authorization_payload_sha256':payload['authorization_payload_sha256'],'output':str(out.resolve())},sort_keys=True));return 0
if __name__=='__main__': raise SystemExit(main())