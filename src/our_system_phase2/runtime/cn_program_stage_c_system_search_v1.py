"""Project-Control route for the 2016-exact Stage-C system-search benchmark."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.runtime import cn_program_disclosure_timing_mechanism_successor_v1 as source_runtime
from our_system_phase2.services.node_resource_governor import validate_node_resource_lease_receipt
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,ProjectControlDenied,consume_active_admission,sha256_file,verify_campaign_authorization_binding,verify_consumed_admission_target
from our_system_phase2.services.unified_capability_registry import stable_hash

ROUTE_ID="cn-program-stage-c-system-search-v1"
CAMPAIGN_ID="CN_PROGRAM_STAGE_C_SYSTEM_SEARCH_V1"
CAMPAIGN_PROFILE="cn_program_stage_c_system_search_v1"
AUTHORIZATION_SCHEMA="cn_program_stage_c_system_search_authorization_v1"
AUTHORIZATION_RELATIVE_PATH=Path("runtime/run_plans/cn_program_stage_c_system_search_authorization_v1.json")
PRIMARY_EXECUTOR_WORKERS=24
CHECKPOINT_SIZE=48

def _read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding="utf-8-sig"))
def _bound(root:Path,binding:Mapping[str,Any],*,payload_field:str,label:str)->dict[str,Any]:
    path=(root/Path(str(binding["relative_path"]))).resolve()
    if not path.is_relative_to(root) or not path.is_file() or sha256_file(path)!=str(binding["file_sha256"]): raise ValueError(f"{label} file drift")
    p=_read(path); body=dict(p); claimed=str(body.pop(payload_field,""))
    if claimed!=str(binding["payload_sha256"]) or stable_hash(body)!=claimed: raise ValueError(f"{label} payload drift")
    return p

def verify_authorization(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
    root=Path(repo_root or Path(__file__).resolve().parents[3]).resolve(); p=_read(path.resolve()); body=dict(p); claimed=str(body.pop("authorization_payload_sha256",""))
    if not claimed or stable_hash(body)!=claimed: raise ValueError("Stage C authorization self-hash drift")
    if p.get("schema_version")!=AUTHORIZATION_SCHEMA or p.get("status")!="STAGE_C_SYSTEM_SEARCH_AUTHORIZED_NOT_RUN" or p.get("campaign_id")!=CAMPAIGN_ID or p.get("campaign_profile")!=CAMPAIGN_PROFILE or p.get("project_control_route_id")!=ROUTE_ID or list(p.get("permitted_project_control_actions") or ())!=[ACTION_LAUNCH,ACTION_RETRY] or p.get("evaluation_data_role")!="DEVELOPMENT_ONLY" or p.get("execution_authorized") is not True or p.get("validation_feedback_used") is not False or p.get("promotion_authorized") is not False or p.get("oos_authority")!="NONE" or p.get("automatic_successor_authorized") is not False: raise ValueError("Stage C authorization contract drift")
    source=dict(p["source_mechanism_authorization"]); source_path=(root/Path(str(source["relative_path"]))).resolve()
    if sha256_file(source_path)!=str(source["file_sha256"]): raise ValueError("Stage C source authorization file drift")
    source_auth=source_runtime.verify_authorization(source_path,repo_root=root)
    if source_auth.get("authorization_payload_sha256")!=source["payload_sha256"]: raise ValueError("Stage C source authorization payload drift")
    pre=_bound(root,p["stage_c_prefreeze"],payload_field="prefreeze_payload_sha256",label="Stage C prefreeze")
    if pre.get("status")!="STAGE_C_SYSTEM_SEARCH_PREFROZEN_BEFORE_FINANCIAL_READ" or int(dict(pre["cohort"])["total_records"] or 0)!=2016 or pre.get("stage_c_financial_labels_read_during_freeze") is not False: raise ValueError("Stage C prefreeze binding drift")
    stageb=_bound(root,p["stage_b_transfer_audit"],payload_field="audit_payload_sha256",label="Stage B audit")
    if stageb.get("status")!="PASS_INDEPENDENT_TERMINAL_AUDIT" or stageb.get("search_transfer_status")!="PRIMITIVE_LOCAL_SEARCH_TRANSFER_PASS_STAGE_C_LARGE_SCALE_AUTHORIZATION_ELIGIBLE": raise ValueError("Stage B transfer evidence drift")
    training=dict(p["training_evidence"])
    dataset=_bound(root,training["dataset"],payload_field="dataset_payload_sha256",label="Stage C trained dataset")
    replay=_bound(root,training["search_core_replay"],payload_field="replay_payload_sha256",label="Stage C search-core replay")
    primitive=_bound(root,training["primitive_stats"],payload_field="stats_payload_sha256",label="Stage C primitive stats")
    if len(dataset.get("rows") or ())!=1310 or replay.get("qualified_trained_policy_leader")!="TRAINED_MULTIHEAD_V2" or primitive.get("status")!="SPENT_DEVELOPMENT_PRIMITIVE_CREDIT_STATS_FROZEN" or int(primitive.get("unique_program_exact_count") or 0)!=1392:
        raise ValueError("Stage C training evidence drift")
    resource=dict(p["resource_contract"])
    if resource.get("profile")!="SEARCH_DUAL_24" or int(resource.get("cpu_threads") or 0)!=24 or int(resource.get("executor_workers") or 0)!=24 or int(resource.get("checkpoint_size") or 0)!=48 or resource.get("candidate_evaluation_during_canary") is not False: raise ValueError("Stage C resource contract drift")
    if any(int(v)!=0 for v in dict(p["restricted_reads"]).values()): raise ValueError("Stage C restricted-read authorization drift")
    return p

def verify_official_canary(path:Path,authorization:Mapping[str,Any],*,repo_root:Path)->dict[str,Any]:
    raw=path.resolve().read_bytes(); observed=hashlib.sha256(raw).hexdigest(); binding=dict(authorization["official_resource_canary"])
    if observed!=str(binding["file_sha256"]): raise ProjectControlDenied("Stage C official canary file drift")
    p=json.loads(raw.decode("utf-8-sig")); body=dict(p); claimed=str(body.pop("official_canary_payload_sha256",""))
    if claimed!=str(binding["payload_sha256"]) or stable_hash(body)!=claimed or p.get("status")!="PASS" or p.get("candidate_evaluation_executed") is not False or int(p.get("field_column_count") or 0)!=int(binding["field_column_count"]) or p.get("field_columns_sha256")!=binding["field_columns_sha256"]: raise ProjectControlDenied("Stage C official canary payload drift")
    runner=repo_root/"scripts/run_cn_program_stage_c_system_search_v1.py"
    if sha256_file(runner)!=str(binding["runner_source_file_sha256"]): raise ProjectControlDenied("Stage C runner changed after official canary")
    return p

def main(argv:Sequence[str]|None=None)->int:
    admission=consume_active_admission(ROUTE_ID,{ACTION_LAUNCH,ACTION_RETRY}); parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization",type=Path,required=True); parser.add_argument("--stage-c-prefreeze",type=Path,required=True); parser.add_argument("--official-resource-canary",type=Path,required=True); parser.add_argument("--source-freeze-root",type=Path,required=True); parser.add_argument("--prior-exact-freeze",type=Path,required=True); parser.add_argument("--execution-contract",type=Path,required=True); parser.add_argument("--train-field-root",type=Path,required=True); parser.add_argument("--train-price-root",type=Path,required=True); parser.add_argument("--registry",type=Path,required=True); parser.add_argument("--node-resource-capacity",type=Path,required=True); parser.add_argument("--node-resource-lease-receipt",type=Path,required=True); parser.add_argument("--output-root",type=Path,required=True); args=parser.parse_args(argv); args.executor_workers=24
    verify_consumed_admission_target(admission,output_root=args.output_root); verified=verify_campaign_authorization_binding(admission,args.campaign_authorization); auth=verify_authorization(verified.path)
    if dict(verified.payload)!=auth: raise ProjectControlDenied("Stage C authorization payload drift")
    root=Path(__file__).resolve().parents[3]; expected=(root/Path(str(auth["stage_c_prefreeze"]["relative_path"]))).resolve()
    if args.stage_c_prefreeze.resolve()!=expected: raise ProjectControlDenied("Stage C prefreeze path outside authorization")
    verify_official_canary(args.official_resource_canary,auth,repo_root=root); validate_node_resource_lease_receipt(args.node_resource_lease_receipt.resolve(),expected_role="SEARCH",expected_cpu_threads=24)
    from scripts.run_cn_program_stage_c_system_search_v1 import run
    result=run(args,admission=admission,authorization=auth); print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
