from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Any, Mapping, Sequence
from our_system_phase2.runtime import cn_program_primitive_main_production_v1 as source_runtime
from our_system_phase2.services.node_resource_governor import validate_node_resource_lease_receipt
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,ProjectControlDenied,consume_active_admission,sha256_file,verify_campaign_authorization_binding,verify_consumed_admission_target
from our_system_phase2.services.unified_capability_registry import stable_hash
ROUTE_ID='cn-program-primitive-market-successor-v3'
CAMPAIGN_ID='CN_PROGRAM_PRIMITIVE_MARKET_SUCCESSOR_V3'
CAMPAIGN_PROFILE='cn_program_primitive_market_successor_v3'
AUTHORIZATION_SCHEMA='cn_program_primitive_market_successor_v3_authorization_v1'
AUTHORIZATION_RELATIVE_PATH=Path('runtime/run_plans/cn_program_primitive_market_successor_v3_authorization_v1.json')
PLAN_RELATIVE_PATH=Path('runtime/run_plans/cn_program_primitive_market_successor_v3_plan.json')

def _read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding='utf-8-sig'))
def _self_bound(root:Path,b:Mapping[str,Any],field:str,label:str)->dict[str,Any]:
 p=(root/Path(str(b['relative_path']))).resolve()
 if not p.is_relative_to(root) or not p.is_file() or sha256_file(p)!=str(b['file_sha256']): raise ValueError(f'{label} file drift')
 x=_read(p);body=dict(x);claim=str(body.pop(field,''))
 if claim!=str(b['payload_sha256']) or stable_hash(body)!=claim: raise ValueError(f'{label} payload drift')
 return x

def verify_authorization(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
 root=Path(repo_root or Path(__file__).resolve().parents[3]).resolve();p=_read(path.resolve());body=dict(p);claim=str(body.pop('authorization_payload_sha256',''))
 if not claim or stable_hash(body)!=claim: raise ValueError('market successor authorization self-hash drift')
 if p.get('schema_version')!=AUTHORIZATION_SCHEMA or p.get('status')!='PRIMITIVE_MARKET_SUCCESSOR_V3_AUTHORIZED_NOT_RUN' or p.get('execution_authorized') is not True or p.get('campaign_id')!=CAMPAIGN_ID or p.get('campaign_profile')!=CAMPAIGN_PROFILE or p.get('project_control_route_id')!=ROUTE_ID or list(p.get('permitted_project_control_actions') or ())!=[ACTION_LAUNCH,ACTION_RETRY] or p.get('evaluation_data_role')!='DEVELOPMENT_ONLY' or p.get('validation_feedback_used') is not False or p.get('oos_authority')!='NONE' or p.get('promotion_authorized') is not False or p.get('automatic_successor_authorized') is not False: raise ValueError('market successor authorization contract drift')
 sb=dict(p['source_production_authorization']);sp=(root/Path(sb['relative_path'])).resolve()
 if sha256_file(sp)!=str(sb['file_sha256']): raise ValueError('source production authorization file drift')
 source=source_runtime.verify_authorization(sp,repo_root=root)
 if source['authorization_payload_sha256']!=sb['payload_sha256']: raise ValueError('source production authorization payload drift')
 plan=_self_bound(root,p['successor_plan'],'plan_payload_sha256','successor plan')
 if int(plan['budget']['hard_cap_logical_records'])!=360 or plan['search_authority']['production_feedback_imported_into_primitive_stats'] is not False or plan['search_authority']['v1_successor_feedback_imported_into_primitive_stats'] is not False or plan['search_authority']['v2_successor_feedback_imported_into_primitive_stats'] is not False or int(plan['fresh_supply']['effective_spent_exact_count'])!=8294 or int(plan['fresh_supply']['fresh_unique_count'])!=3302: raise ValueError('successor v3 plan contract drift')
 v2audit=_self_bound(root,p['source_v2_terminal_audit'],'audit_payload_sha256','v2 terminal audit')
 if plan['source_v2_terminal_audit']['payload_sha256']!=v2audit['audit_payload_sha256'] or v2audit.get('status')!='PASS_INDEPENDENT_TERMINAL_AUDIT' or v2audit.get('gate_status')!='PRIMITIVE_MARKET_SUCCESSOR_V2_TRANSFER_FAIL' or list(v2audit.get('failed_gate_checks') or [])!=['each_core_template_uplift_stable']: raise ValueError('successor v3 source audit drift')
 v2outcome=_self_bound(root,p['source_v2_postrun_outcome'],'outcome_payload_sha256','v2 postrun outcome')
 if plan['source_v2_postrun_outcome']['payload_sha256']!=v2outcome['outcome_payload_sha256'] or v2outcome.get('post_batch_recommendation')!='REDIRECT': raise ValueError('successor v3 source outcome drift')
 focus=_self_bound(root,p['source_v2_focus_redirect_evidence'],'evidence_payload_sha256','v2 focus redirect evidence')
 if plan['source_v2_focus_redirect_evidence']['payload_sha256']!=focus['evidence_payload_sha256'] or focus.get('removed_focus_template')!='BASE_MARKET' or list(focus.get('redirect_focus_templates') or [])!=['BASE_EVENT','BASE_TEMPORAL_MARKET_EVENT','BASE_MARKET_EVENT']: raise ValueError('successor v3 focus redirect drift')
 audit=_self_bound(root,p['acceleration_accuracy_audit'],'audit_payload_sha256','acceleration accuracy audit')
 if audit.get('status')!='PASS_SUCCESSOR_REAUTH_ELIGIBLE' or plan['acceleration_accuracy_audit']['payload_sha256']!=audit['audit_payload_sha256']: raise ValueError('successor acceleration accuracy audit drift')
 if p.get('production_feedback_imported_into_primitive_stats') is not False or p.get('v1_successor_feedback_imported_into_primitive_stats') is not False or p.get('v2_successor_feedback_imported_into_primitive_stats') is not False or p.get('diversity_contract_changed') is not False: raise ValueError('successor v3 authorization search-authority drift')
 rc=dict(p['resource_contract']); cb=dict(p['official_resource_canary'])
 if rc.get('profile')!='SEARCH_DUAL_24' or int(rc.get('cpu_threads') or 0)!=24 or int(rc.get('primary_executor_workers') or 0)!=24 or int(rc.get('fallback_executor_workers') or 0)!=16 or int(rc.get('hard_cap_logical_records') or 0)!=360 or int(rc.get('field_column_count') or 0)<=0 or int(rc.get('field_column_count') or 0)!=int(cb.get('field_column_count') or 0) or rc.get('field_columns_sha256')!=cb.get('field_columns_sha256') or rc.get('evaluator_pool_lifetime')!='PERSISTENT_RUN_SCOPE' or float(rc.get('minimum_records_per_hour_after_first_checkpoint') or 0)<650.0 or rc.get('persistent_pool_record_hash_parity_required') is not True: raise ValueError('market successor resource contract drift')
 if any(int(v)!=0 for v in dict(p['restricted_reads']).values()): raise ValueError('market successor restricted-read drift')
 runner=root/'scripts/run_cn_program_primitive_market_successor_v3.py'
 if sha256_file(runner)!=str(p['implementation']['runner_source_file_sha256']): raise ValueError('market successor runner drift')
 return p

def verify_official_canary(path:Path,authorization:Mapping[str,Any],*,repo_root:Path)->dict[str,Any]:
 raw=path.resolve().read_bytes();b=dict(authorization['official_resource_canary'])
 if hashlib.sha256(raw).hexdigest()!=str(b['file_sha256']): raise ProjectControlDenied('market successor canary file drift')
 p=json.loads(raw.decode('utf-8-sig'));body=dict(p);claim=str(body.pop('official_canary_payload_sha256',''))
 if claim!=str(b['payload_sha256']) or stable_hash(body)!=claim or p.get('status')!='PASS' or p.get('candidate_evaluation_executed') is not False or int(p.get('preview_selected_count') or 0)!=360 or int(p.get('field_column_count') or 0)<=0 or int(p.get('field_column_count') or 0)!=int(b.get('field_column_count') or 0) or p.get('field_columns_sha256')!=b['field_columns_sha256'] or p.get('acceleration_accuracy_audit_payload_sha256')!=b['acceleration_accuracy_audit_payload_sha256'] or p.get('source_v2_terminal_audit_payload_sha256')!=b['source_v2_terminal_audit_payload_sha256'] or p.get('source_v2_postrun_outcome_payload_sha256')!=b['source_v2_postrun_outcome_payload_sha256'] or p.get('source_v2_focus_redirect_evidence_payload_sha256')!=b['source_v2_focus_redirect_evidence_payload_sha256'] or p.get('evaluator_pool_lifetime')!='PERSISTENT_RUN_SCOPE': raise ProjectControlDenied('market successor canary payload drift')
 if sha256_file(repo_root/'scripts/run_cn_program_primitive_market_successor_v3.py')!=str(b['runner_source_file_sha256']): raise ProjectControlDenied('market successor runner changed after canary')
 return p

def main(argv:Sequence[str]|None=None)->int:
 admission=consume_active_admission(ROUTE_ID,{ACTION_LAUNCH,ACTION_RETRY});q=argparse.ArgumentParser();q.add_argument('--campaign-authorization',type=Path,required=True);q.add_argument('--successor-plan',type=Path,required=True);q.add_argument('--official-resource-canary',type=Path,required=True);q.add_argument('--source-freeze-root',type=Path,required=True);q.add_argument('--prior-exact-freeze',type=Path,required=True);q.add_argument('--execution-contract',type=Path,required=True);q.add_argument('--train-field-root',type=Path,required=True);q.add_argument('--train-price-root',type=Path,required=True);q.add_argument('--registry',type=Path,required=True);q.add_argument('--node-resource-capacity',type=Path,required=True);q.add_argument('--node-resource-lease-receipt',type=Path,required=True);q.add_argument('--output-root',type=Path,required=True);a=q.parse_args(argv);a.executor_workers=24
 verify_consumed_admission_target(admission,output_root=a.output_root);verified=verify_campaign_authorization_binding(admission,a.campaign_authorization);auth=verify_authorization(verified.path)
 if dict(verified.payload)!=auth: raise ProjectControlDenied('market successor authorization payload drift')
 root=Path(__file__).resolve().parents[3]
 if a.successor_plan.resolve()!=(root/PLAN_RELATIVE_PATH).resolve(): raise ProjectControlDenied('market successor plan path outside authorization')
 verify_official_canary(a.official_resource_canary,auth,repo_root=root);validate_node_resource_lease_receipt(a.node_resource_lease_receipt.resolve(),expected_role='SEARCH',expected_cpu_threads=24)
 from scripts.run_cn_program_primitive_market_successor_v3 import run
 result=run(a,admission=admission,authorization=auth);print(json.dumps(result,ensure_ascii=False,sort_keys=True));return 0
if __name__=='__main__':raise SystemExit(main())
