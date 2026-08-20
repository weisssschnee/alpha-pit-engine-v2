"""Project-Control route for the disjoint Stage-D primitive-local confirmation."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.runtime import cn_program_stage_c_system_search_v1 as source_runtime
from our_system_phase2.services.node_resource_governor import validate_node_resource_lease_receipt
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,ProjectControlDenied,consume_active_admission,sha256_file,verify_campaign_authorization_binding,verify_consumed_admission_target
from our_system_phase2.services.unified_capability_registry import stable_hash

ROUTE_ID='cn-program-stage-d-primitive-confirmation-v1'
CAMPAIGN_ID='CN_PROGRAM_STAGE_D_PRIMITIVE_CONFIRMATION_V1'
CAMPAIGN_PROFILE='cn_program_stage_d_primitive_confirmation_v1'
AUTHORIZATION_SCHEMA='cn_program_stage_d_primitive_confirmation_authorization_v1'
AUTHORIZATION_RELATIVE_PATH=Path('runtime/run_plans/cn_program_stage_d_primitive_confirmation_authorization_v1.json')
PRIMARY_EXECUTOR_WORKERS=24
CHECKPOINT_SIZE=48


def _read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding='utf-8-sig'))
def _bound(root:Path,binding:Mapping[str,Any],*,payload_field:str,label:str)->dict[str,Any]:
    path=(root/Path(str(binding['relative_path']))).resolve()
    if not path.is_relative_to(root) or not path.is_file() or sha256_file(path)!=str(binding['file_sha256']): raise ValueError(f'{label} file drift')
    p=_read(path); body=dict(p); claimed=str(body.pop(payload_field,''))
    if claimed!=str(binding['payload_sha256']) or stable_hash(body)!=claimed: raise ValueError(f'{label} payload drift')
    return p


def verify_authorization(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
    root=Path(repo_root or Path(__file__).resolve().parents[3]).resolve(); p=_read(path.resolve()); body=dict(p); claimed=str(body.pop('authorization_payload_sha256',''))
    if not claimed or stable_hash(body)!=claimed: raise ValueError('Stage D authorization self-hash drift')
    if p.get('schema_version')!=AUTHORIZATION_SCHEMA or p.get('status')!='STAGE_D_PRIMITIVE_CONFIRMATION_AUTHORIZED_NOT_RUN' or p.get('campaign_id')!=CAMPAIGN_ID or p.get('campaign_profile')!=CAMPAIGN_PROFILE or p.get('project_control_route_id')!=ROUTE_ID or list(p.get('permitted_project_control_actions') or ())!=[ACTION_LAUNCH,ACTION_RETRY] or p.get('evaluation_data_role')!='DEVELOPMENT_ONLY' or p.get('execution_authorized') is not True or p.get('validation_feedback_used') is not False or p.get('promotion_authorized') is not False or p.get('oos_authority')!='NONE' or p.get('automatic_successor_authorized') is not False:
        raise ValueError('Stage D authorization contract drift')
    source=dict(p['source_stage_c_authorization']); source_path=(root/Path(str(source['relative_path']))).resolve()
    if sha256_file(source_path)!=str(source['file_sha256']): raise ValueError('Stage D source Stage C authorization file drift')
    source_auth=source_runtime.verify_authorization(source_path,repo_root=root)
    if source_auth.get('authorization_payload_sha256')!=source['payload_sha256']: raise ValueError('Stage D source Stage C authorization payload drift')
    pre=_bound(root,p['stage_d_prefreeze'],payload_field='prefreeze_payload_sha256',label='Stage D prefreeze')
    if pre.get('status')!='STAGE_D_PRIMITIVE_CONFIRMATION_PREFROZEN_BEFORE_FINANCIAL_READ' or int(dict(pre['cohort'])['total_records'] or 0)!=2016 or pre.get('stage_d_financial_labels_read_during_freeze') is not False or pre.get('no_stage_c_label_retraining_or_tuning') is not True: raise ValueError('Stage D prefreeze binding drift')
    supply=_bound(root,p['stage_d_supply_audit'],payload_field='audit_payload_sha256',label='Stage D supply audit')
    if supply.get('status')!='ZERO_FINANCIAL_STAGE_D_EXPANDED_SUPPLY_AUDIT_COMPLETE' or int(supply.get('fresh_unique_count') or 0)!=3584 or int(supply.get('effective_spent_exact_count') or 0)!=4718: raise ValueError('Stage D supply evidence drift')
    stats=_bound(root,p['primitive_stats'],payload_field='stats_payload_sha256',label='Stage D primitive stats')
    if stats.get('status')!='SPENT_DEVELOPMENT_PRIMITIVE_CREDIT_STATS_FROZEN' or int(stats.get('unique_program_exact_count') or 0)!=1392: raise ValueError('Stage D primitive stats drift')
    stagec_evidence=_bound(root,p['stage_c_terminal_evidence'],payload_field='evidence_payload_sha256',label='Stage C terminal evidence')
    if stagec_evidence.get('status')!='STAGE_C_TERMINAL_EVIDENCE_FROZEN' or stagec_evidence.get('system_search_status')!='NO_SYSTEM_LARGE_SCALE_SEARCH_VICTORY' or stagec_evidence.get('stage_d_policy_training_stage_c_results_used') is not False: raise ValueError('Stage C terminal evidence drift')
    resource=dict(p['resource_contract'])
    if resource.get('profile')!='SEARCH_DUAL_24' or int(resource.get('cpu_threads') or 0)!=24 or int(resource.get('executor_workers') or 0)!=24 or int(resource.get('checkpoint_size') or 0)!=48 or resource.get('candidate_evaluation_during_canary') is not False: raise ValueError('Stage D resource contract drift')
    if any(int(v)!=0 for v in dict(p['restricted_reads']).values()): raise ValueError('Stage D restricted-read authorization drift')
    return p


def verify_official_canary(path:Path,authorization:Mapping[str,Any],*,repo_root:Path)->dict[str,Any]:
    raw=path.resolve().read_bytes(); observed=hashlib.sha256(raw).hexdigest(); binding=dict(authorization['official_resource_canary'])
    if observed!=str(binding['file_sha256']): raise ProjectControlDenied('Stage D official canary file drift')
    p=json.loads(raw.decode('utf-8-sig')); body=dict(p); claimed=str(body.pop('official_canary_payload_sha256',''))
    if claimed!=str(binding['payload_sha256']) or stable_hash(body)!=claimed or p.get('status')!='PASS' or p.get('candidate_evaluation_executed') is not False or int(p.get('field_column_count') or 0)!=int(binding['field_column_count']) or p.get('field_columns_sha256')!=binding['field_columns_sha256']:
        raise ProjectControlDenied('Stage D official canary payload drift')
    runner=repo_root/'scripts/run_cn_program_stage_d_primitive_confirmation_v1.py'
    if sha256_file(runner)!=str(binding['runner_source_file_sha256']): raise ProjectControlDenied('Stage D runner changed after official canary')
    return p


def main(argv:Sequence[str]|None=None)->int:
    admission=consume_active_admission(ROUTE_ID,{ACTION_LAUNCH,ACTION_RETRY}); parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--campaign-authorization',type=Path,required=True); parser.add_argument('--stage-d-prefreeze',type=Path,required=True); parser.add_argument('--official-resource-canary',type=Path,required=True); parser.add_argument('--source-freeze-root',type=Path,required=True); parser.add_argument('--prior-exact-freeze',type=Path,required=True); parser.add_argument('--execution-contract',type=Path,required=True); parser.add_argument('--train-field-root',type=Path,required=True); parser.add_argument('--train-price-root',type=Path,required=True); parser.add_argument('--registry',type=Path,required=True); parser.add_argument('--node-resource-capacity',type=Path,required=True); parser.add_argument('--node-resource-lease-receipt',type=Path,required=True); parser.add_argument('--output-root',type=Path,required=True); args=parser.parse_args(argv); args.executor_workers=24
    verify_consumed_admission_target(admission,output_root=args.output_root); verified=verify_campaign_authorization_binding(admission,args.campaign_authorization); auth=verify_authorization(verified.path)
    if dict(verified.payload)!=auth: raise ProjectControlDenied('Stage D authorization payload drift')
    root=Path(__file__).resolve().parents[3]; expected=(root/Path(str(auth['stage_d_prefreeze']['relative_path']))).resolve()
    if args.stage_d_prefreeze.resolve()!=expected: raise ProjectControlDenied('Stage D prefreeze path outside authorization')
    verify_official_canary(args.official_resource_canary,auth,repo_root=root); validate_node_resource_lease_receipt(args.node_resource_lease_receipt.resolve(),expected_role='SEARCH',expected_cpu_threads=24)
    from scripts.run_cn_program_stage_d_primitive_confirmation_v1 import run
    result=run(args,admission=admission,authorization=auth); print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0

if __name__=='__main__': raise SystemExit(main())
