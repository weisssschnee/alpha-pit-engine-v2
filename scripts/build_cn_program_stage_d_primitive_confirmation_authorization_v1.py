"""Build final Stage-D authorization after exact-runner official resource canary exists."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Any

from our_system_phase2.runtime import cn_program_stage_c_system_search_v1 as source_runtime
from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash


def read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding='utf-8-sig'))
def selfhash(p:Path,field:str)->tuple[dict[str,Any],str]:
    row=read(p); body=dict(row); claimed=str(body.pop(field,''))
    if not claimed or stable_hash(body)!=claimed: raise ValueError(f'self-hash drift: {p}')
    return row,claimed


def build(repo:Path,canary:Path)->dict[str,Any]:
    repo=repo.resolve(); source_path=repo/source_runtime.AUTHORIZATION_RELATIVE_PATH; source=source_runtime.verify_authorization(source_path,repo_root=repo)
    pre_path=repo/'runtime/run_plans/cn_program_stage_d_primitive_confirmation_prefreeze_v1.json'; pre,pre_hash=selfhash(pre_path,'prefreeze_payload_sha256')
    supply_path=repo/'runtime/run_plans/cn_stage_d_expanded_supply_audit_cbaaaee_20260820.json'; supply,supply_hash=selfhash(supply_path,'audit_payload_sha256')
    stats_path=repo/'runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json'; stats,stats_hash=selfhash(stats_path,'stats_payload_sha256')
    stagec_evidence_path=repo/'runtime/run_plans/cn_program_stage_c_terminal_evidence_for_stage_d_20260820.json'; stagec_evidence,stagec_evidence_hash=selfhash(stagec_evidence_path,'evidence_payload_sha256')
    rehearsal_path=repo/'runtime/run_plans/cn_program_stage_d_prefinancial_rehearsal_20260820.json'; rehearsal,rehearsal_hash=selfhash(rehearsal_path,'rehearsal_payload_sha256')
    raw=canary.resolve().read_bytes(); canary_file=hashlib.sha256(raw).hexdigest(); c=json.loads(raw.decode('utf-8-sig')); body=dict(c); c_hash=str(body.pop('official_canary_payload_sha256',''))
    if not c_hash or stable_hash(body)!=c_hash or c.get('status')!='PASS' or c.get('candidate_evaluation_executed') is not False: raise ValueError('official Stage D canary not PASS')
    runner_path=repo/'scripts/run_cn_program_stage_d_primitive_confirmation_v1.py'; runner_sha=sha256_file(runner_path)
    if c.get('runner_source_file_sha256')!=runner_sha or c.get('prefreeze_payload_sha256')!=pre_hash: raise ValueError('official Stage D canary implementation binding drift')
    if int(c.get('field_column_count') or 0)!=int(rehearsal.get('field_column_count') or 0) or str(c.get('field_columns_sha256') or '')!=str(rehearsal.get('field_columns_sha256') or ''): raise ValueError('Stage D canary/rehearsal geometry drift')
    if stagec_evidence.get('status')!='STAGE_C_TERMINAL_EVIDENCE_FROZEN' or stagec_evidence.get('system_search_status')!='NO_SYSTEM_LARGE_SCALE_SEARCH_VICTORY' or stagec_evidence.get('stage_d_policy_training_stage_c_results_used') is not False: raise ValueError('Stage C terminal evidence drift')
    payload={
        'schema_version':runtime.AUTHORIZATION_SCHEMA,'status':'STAGE_D_PRIMITIVE_CONFIRMATION_AUTHORIZED_NOT_RUN','execution_authorized':True,
        'campaign_id':runtime.CAMPAIGN_ID,'campaign_profile':runtime.CAMPAIGN_PROFILE,'project_control_route_id':runtime.ROUTE_ID,
        'permitted_project_control_actions':[ACTION_LAUNCH,ACTION_RETRY],'evaluation_data_role':'DEVELOPMENT_ONLY',
        'source_stage_c_authorization':{'relative_path':str(source_runtime.AUTHORIZATION_RELATIVE_PATH).replace('\\','/'),'file_sha256':sha256_file(source_path),'payload_sha256':source['authorization_payload_sha256']},
        'source_prior_exact':dict(source['source_prior_exact']),'source_evaluator_authority':dict(source['source_evaluator_authority']),
        'stage_d_prefreeze':{'relative_path':'runtime/run_plans/cn_program_stage_d_primitive_confirmation_prefreeze_v1.json','file_sha256':sha256_file(pre_path),'payload_sha256':pre_hash,'cohort_exact_identities_sha256':pre['cohort']['exact_identities_sha256'],'total_records':2016,'stage_c_results_in_stats':0},
        'stage_d_supply_audit':{'relative_path':'runtime/run_plans/cn_stage_d_expanded_supply_audit_cbaaaee_20260820.json','file_sha256':sha256_file(supply_path),'payload_sha256':supply_hash,'fresh_unique_count':3584,'effective_spent_exact_count':4718,'raw_ordinal_offset':1024},
        'primitive_stats':{'relative_path':'runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json','file_sha256':sha256_file(stats_path),'payload_sha256':stats_hash,'rows':1392,'stage_c_results_in_stats':0},
        'stage_c_terminal_evidence':{'relative_path':'runtime/run_plans/cn_program_stage_c_terminal_evidence_for_stage_d_20260820.json','file_sha256':sha256_file(stagec_evidence_path),'payload_sha256':stagec_evidence_hash,'system_search_status':stagec_evidence['system_search_status'],'policy_training_stage_c_results_used':False},
        'prefinancial_rehearsal':{'relative_path':'runtime/run_plans/cn_program_stage_d_prefinancial_rehearsal_20260820.json','file_sha256':sha256_file(rehearsal_path),'payload_sha256':rehearsal_hash,'schedule_count':2016,'field_column_count':int(rehearsal['field_column_count']),'field_columns_sha256':str(rehearsal['field_columns_sha256'])},
        'resource_contract':{'profile':'SEARCH_DUAL_24','cpu_threads':24,'executor_workers':24,'checkpoint_size':48,'candidate_evaluation_during_canary':False,'field_column_count':int(c['field_column_count']),'field_columns_sha256':str(c['field_columns_sha256'])},
        'official_resource_canary':{'file_sha256':canary_file,'payload_sha256':c_hash,'field_column_count':int(c['field_column_count']),'field_columns_sha256':str(c['field_columns_sha256']),'runner_source_file_sha256':runner_sha},
        'confirmation_contract':dict(pre['evaluation']),
        'restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0},'validation_feedback_used':False,'promotion_authorized':False,'oos_authority':'NONE','automatic_successor_authorized':False,
    }
    payload['authorization_payload_sha256']=stable_hash(payload); return payload


def main(argv=None)->int:
    p=argparse.ArgumentParser(); p.add_argument('--repo-root',type=Path,default=Path(__file__).resolve().parents[1]); p.add_argument('--official-resource-canary',type=Path,required=True); p.add_argument('--output',type=Path,default=runtime.AUTHORIZATION_RELATIVE_PATH); a=p.parse_args(argv)
    payload=build(a.repo_root,a.official_resource_canary); out=a.output if a.output.is_absolute() else a.repo_root/a.output; out.parent.mkdir(parents=True,exist_ok=True); out.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8'); print(json.dumps({'status':payload['status'],'authorization_payload_sha256':payload['authorization_payload_sha256'],'output':str(out.resolve())},sort_keys=True)); return 0

if __name__=='__main__': raise SystemExit(main())
