from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
from typing import Any,Mapping,Sequence
from our_system_phase2.runtime import cn_program_disclosure_timing_mechanism_successor_v1 as source_runtime
from our_system_phase2.services.node_resource_governor import validate_node_resource_lease_receipt
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,ProjectControlDenied,consume_active_admission,sha256_file,verify_campaign_authorization_binding,verify_consumed_admission_target
from our_system_phase2.services.unified_capability_registry import stable_hash
ROUTE_ID='cn-program-primitive-main-production-v1'; CAMPAIGN_ID='CN_PROGRAM_PRIMITIVE_MAIN_PRODUCTION_V1'; CAMPAIGN_PROFILE='cn_program_primitive_main_production_v1'; AUTHORIZATION_SCHEMA='cn_program_primitive_main_production_authorization_v1'; AUTHORIZATION_RELATIVE_PATH=Path('runtime/run_plans/cn_program_primitive_main_production_authorization_v1.json')
def _read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding='utf-8-sig'))
def _bound(root:Path,b:Mapping[str,Any],field:str,label:str)->dict[str,Any]:
 p=(root/Path(str(b['relative_path']))).resolve()
 if not p.is_relative_to(root) or not p.is_file() or sha256_file(p)!=str(b['file_sha256']): raise ValueError(f'{label} file drift')
 row=_read(p); body=dict(row); claim=str(body.pop(field,''))
 if claim!=str(b['payload_sha256']) or stable_hash(body)!=claim: raise ValueError(f'{label} payload drift')
 return row
def verify_authorization(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
 root=Path(repo_root or Path(__file__).resolve().parents[3]).resolve(); p=_read(path.resolve()); body=dict(p); claim=str(body.pop('authorization_payload_sha256',''))
 if not claim or stable_hash(body)!=claim: raise ValueError('primitive production authorization self-hash drift')
 if p.get('schema_version')!=AUTHORIZATION_SCHEMA or p.get('status')!='PRIMITIVE_MAIN_PRODUCTION_AUTHORIZED_NOT_RUN' or p.get('campaign_id')!=CAMPAIGN_ID or p.get('campaign_profile')!=CAMPAIGN_PROFILE or p.get('project_control_route_id')!=ROUTE_ID or list(p.get('permitted_project_control_actions') or ())!=[ACTION_LAUNCH,ACTION_RETRY] or p.get('evaluation_data_role')!='DEVELOPMENT_ONLY' or p.get('execution_authorized') is not True or p.get('validation_feedback_used') is not False or p.get('promotion_authorized') is not False or p.get('oos_authority')!='NONE' or p.get('automatic_successor_authorized') is not False: raise ValueError('primitive production authorization contract drift')
 source=dict(p['source_mechanism_authorization']); sp=(root/Path(source['relative_path'])).resolve()
 if sha256_file(sp)!=source['file_sha256']: raise ValueError('source authorization file drift')
 sa=source_runtime.verify_authorization(sp,repo_root=root)
 if sa['authorization_payload_sha256']!=source['payload_sha256']: raise ValueError('source authorization payload drift')
 plan=_bound(root,p['production_plan'],'plan_payload_sha256','production plan'); q=_bound(root,p['main_search_qualification'],'qualification_payload_sha256','main search qualification'); supply=_bound(root,p['fresh_supply'],'audit_payload_sha256','fresh supply'); stats=_bound(root,p['primitive_stats'],'stats_payload_sha256','primitive stats')
 if plan.get('status')!='PRIMITIVE_MAIN_PRODUCTION_PLAN_FROZEN_NOT_RUN' or q.get('decision')!='PRIMITIVE_PRIMARY_UNIFORM_RESERVE_EVOLUTION_CHALLENGER' or int(supply.get('fresh_unique_count') or 0)!=3576 or int(supply.get('effective_spent_exact_count') or 0)!=6734 or int(stats.get('unique_program_exact_count') or 0)!=1392: raise ValueError('primitive production evidence drift')
 r=dict(p['resource_contract'])
 if r.get('profile')!='SEARCH_DUAL_24' or int(r.get('cpu_threads') or 0)!=24 or int(r.get('primary_executor_workers') or 0)!=24 or int(r.get('fallback_executor_workers') or 0)!=16 or int(r.get('checkpoint_size') or 0)!=24 or int(r.get('macro_count') or 0)!=5 or int(r.get('hard_cap_logical_records') or 0)!=840: raise ValueError('primitive production resource contract drift')
 if any(int(v)!=0 for v in dict(p['restricted_reads']).values()): raise ValueError('primitive production restricted-read drift')
 return p
def verify_official_canary(path:Path,authorization:Mapping[str,Any],*,repo_root:Path)->dict[str,Any]:
 raw=path.resolve().read_bytes(); binding=dict(authorization['official_resource_canary'])
 if hashlib.sha256(raw).hexdigest()!=str(binding['file_sha256']): raise ProjectControlDenied('primitive production canary file drift')
 p=json.loads(raw.decode('utf-8-sig')); body=dict(p); claim=str(body.pop('official_canary_payload_sha256',''))
 if claim!=binding['payload_sha256'] or stable_hash(body)!=claim or p.get('status')!='PASS' or p.get('candidate_evaluation_executed') is not False or int(p.get('field_column_count') or 0)!=int(binding['field_column_count']) or p.get('field_columns_sha256')!=binding['field_columns_sha256']: raise ProjectControlDenied('primitive production canary payload drift')
 runner=repo_root/'scripts/run_cn_program_primitive_main_production_v1.py'
 if sha256_file(runner)!=binding['runner_source_file_sha256']: raise ProjectControlDenied('primitive production runner changed after canary')
 return p
def main(argv:Sequence[str]|None=None)->int:
 admission=consume_active_admission(ROUTE_ID,{ACTION_LAUNCH,ACTION_RETRY}); parser=argparse.ArgumentParser(); parser.add_argument('--campaign-authorization',type=Path,required=True); parser.add_argument('--production-plan',type=Path,required=True); parser.add_argument('--official-resource-canary',type=Path,required=True); parser.add_argument('--source-freeze-root',type=Path,required=True); parser.add_argument('--prior-exact-freeze',type=Path,required=True); parser.add_argument('--execution-contract',type=Path,required=True); parser.add_argument('--train-field-root',type=Path,required=True); parser.add_argument('--train-price-root',type=Path,required=True); parser.add_argument('--registry',type=Path,required=True); parser.add_argument('--node-resource-capacity',type=Path,required=True); parser.add_argument('--node-resource-lease-receipt',type=Path,required=True); parser.add_argument('--output-root',type=Path,required=True); args=parser.parse_args(argv); args.executor_workers=24
 verify_consumed_admission_target(admission,output_root=args.output_root); verified=verify_campaign_authorization_binding(admission,args.campaign_authorization); auth=verify_authorization(verified.path)
 if dict(verified.payload)!=auth: raise ProjectControlDenied('primitive production authorization payload drift')
 root=Path(__file__).resolve().parents[3]; expected=(root/Path(auth['production_plan']['relative_path'])).resolve()
 if args.production_plan.resolve()!=expected: raise ProjectControlDenied('production plan path outside authorization')
 verify_official_canary(args.official_resource_canary,auth,repo_root=root); validate_node_resource_lease_receipt(args.node_resource_lease_receipt.resolve(),expected_role='SEARCH',expected_cpu_threads=24)
 from scripts.run_cn_program_primitive_main_production_v1 import run
 result=run(args,admission=admission,authorization=auth); print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0
if __name__=='__main__': raise SystemExit(main())
