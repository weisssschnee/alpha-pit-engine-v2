"""Build Search Core V2 Stage-1.5 authorization after zero-financial gates."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Any, Mapping
from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as source_runtime
from our_system_phase2.runtime import cn_search_core_v2_stage15_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash
from scripts import run_cn_search_core_v2_stage15_v1 as stage15

def _read(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding="utf-8-sig"))
def _self(path:Path,field:str,label:str)->tuple[dict[str,Any],str]:
    p=_read(path); body=dict(p); claimed=str(body.pop(field,""))
    if not claimed or stable_hash(body)!=claimed: raise ValueError(f"{label} self-hash drift")
    return p,claimed

def build(repo:Path,*,canary:Path,supply_audit:Path)->dict[str,Any]:
    repo=repo.resolve(); source_path=repo/source_runtime.AUTHORIZATION_RELATIVE_PATH; source=source_runtime.verify_authorization(source_path,repo_root=repo)
    pre_path=repo/runtime.PREFREEZE_RELATIVE_PATH; pre=stage15.verify_prefreeze(pre_path); pre_hash=str(pre["prefreeze_payload_sha256"])
    supply_path=supply_audit.resolve(); supply,supply_hash=_self(supply_path,"audit_payload_sha256","Stage-1.5 mature supply")
    if (
        supply.get("status")!="PASS_ZERO_FINANCIAL_MATURE_STATE_JUMP_SUPPLY_AUDIT" or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("financial_sidecar_read")) or int(supply.get("generated_total") or 0)!=runtime.TOTAL_PER_ARM
        or str(supply.get("prefreeze_payload_sha256") or "")!=pre_hash
    ): raise ValueError("Stage-1.5 mature supply not PASS")
    raw=canary.resolve().read_bytes(); canary_file=hashlib.sha256(raw).hexdigest(); c=json.loads(raw.decode("utf-8-sig")); body=dict(c); canary_hash=str(body.pop("official_canary_payload_sha256",""))
    if not canary_hash or stable_hash(body)!=canary_hash or c.get("status")!="PASS" or c.get("candidate_evaluation_executed") is not False:
        raise ValueError("Stage-1.5 canary not PASS")
    runner=repo/"scripts/run_cn_search_core_v2_stage15_v1.py"; runner_sha=sha256_file(runner)
    if c.get("runner_source_file_sha256")!=runner_sha or c.get("prefreeze_payload_sha256")!=pre_hash or c.get("mature_state_supply_audit_payload_sha256")!=supply_hash or int(c.get("requested_workers") or 0)!=runtime.PRIMARY_EXECUTOR_WORKERS:
        raise ValueError("Stage-1.5 canary binding drift")
    payload={
        "schema_version":runtime.AUTHORIZATION_SCHEMA,"status":"SEARCH_CORE_V2_STAGE15_AUTHORIZED_NOT_RUN","execution_authorized":True,
        "campaign_id":runtime.CAMPAIGN_ID,"campaign_profile":runtime.CAMPAIGN_PROFILE,"project_control_route_id":runtime.ROUTE_ID,
        "permitted_project_control_actions":[ACTION_LAUNCH,ACTION_RETRY],"evaluation_data_role":"DEVELOPMENT_ONLY",
        "source_stage_d_authorization":{"relative_path":str(source_runtime.AUTHORIZATION_RELATIVE_PATH).replace("\\","/"),"file_sha256":sha256_file(source_path),"payload_sha256":str(source["authorization_payload_sha256"])},
        "source_prior_exact":dict(source["source_prior_exact"]),"source_evaluator_authority":dict(source["source_evaluator_authority"]),
        "stage15_prefreeze":{"relative_path":str(runtime.PREFREEZE_RELATIVE_PATH).replace("\\","/"),"file_sha256":sha256_file(pre_path),"payload_sha256":pre_hash,"arm_a_selected_exact_identities_sha256":str(pre["arm_a_primitive"]["selected_exact_identities_sha256"]),"source_mature_snapshot_payload_sha256":str(pre["arm_b_state_jump"]["source_snapshot_payload_sha256"]),"budget_per_arm":runtime.TOTAL_PER_ARM,"total_financial_evaluations":runtime.TOTAL_EVALUATIONS},
        "mature_state_supply_audit":{"relative_path":str(supply_path.relative_to(repo)).replace("\\","/"),"file_sha256":sha256_file(supply_path),"payload_sha256":supply_hash,"generated_total":int(supply["generated_total"]),"field_column_count":int(supply["resource_field_surface"]["field_column_count"]),"field_columns_sha256":str(supply["resource_field_surface"]["field_columns_sha256"])},
        "official_resource_canary":{"relative_path":str(canary.resolve().relative_to(repo)).replace("\\","/"),"file_sha256":canary_file,"payload_sha256":canary_hash,"runner_source_file_sha256":runner_sha,"field_column_count":int(c["field_column_count"]),"field_columns_sha256":str(c["field_columns_sha256"])},
        "resource_contract":{"profile":"SEARCH_DUAL_24","cpu_threads":24,"executor_workers":runtime.PRIMARY_EXECUTOR_WORKERS,"checkpoint_size":runtime.CHECKPOINT_SIZE,"candidate_evaluation_during_canary":False,"field_column_count":int(c["field_column_count"]),"field_columns_sha256":str(c["field_columns_sha256"])},
        "stage15_contract":dict(pre["stage15"]),"stage15_decision_contract":dict(pre["stage15_decision_contract"]),
        "restricted_reads":{"validation":0,"holdout":0,"historical_2023":0,"forward_b":0,"forward_2026":0},
        "validation_feedback_used":False,"promotion_authorized":False,"oos_authority":"NONE","automatic_stage2_authorized":False,
    }
    payload["authorization_payload_sha256"]=stable_hash(payload); return payload

def main(argv:list[str]|None=None)->int:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--repo-root",type=Path,default=Path(__file__).resolve().parents[1]); p.add_argument("--official-resource-canary",type=Path,required=True); p.add_argument("--mature-state-supply-audit",type=Path,required=True); p.add_argument("--output",type=Path,default=runtime.AUTHORIZATION_RELATIVE_PATH)
    args=p.parse_args(argv); payload=build(args.repo_root,canary=args.official_resource_canary,supply_audit=args.mature_state_supply_audit); out=args.output if args.output.is_absolute() else args.repo_root/args.output; out.parent.mkdir(parents=True,exist_ok=True); out.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8"); print(json.dumps({"status":payload["status"],"authorization_payload_sha256":payload["authorization_payload_sha256"],"output":str(out.resolve())},sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
