"""Build final Stage-C authorization after an exact-runner official resource canary exists."""
from __future__ import annotations
import argparse, json, hashlib
from pathlib import Path
from typing import Any

from our_system_phase2.runtime import cn_program_disclosure_timing_mechanism_successor_v1 as source_runtime
from our_system_phase2.runtime import cn_program_stage_c_system_search_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY,sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash

def read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding="utf-8-sig"))
def selfhash(p:Path,field:str)->tuple[dict[str,Any],str]:
    row=read(p); body=dict(row); claimed=str(body.pop(field,""))
    if not claimed or stable_hash(body)!=claimed: raise ValueError(f"self-hash drift: {p}")
    return row,claimed

def build(repo:Path,canary:Path)->dict[str,Any]:
    repo=repo.resolve(); source_path=repo/source_runtime.AUTHORIZATION_RELATIVE_PATH; source=source_runtime.verify_authorization(source_path,repo_root=repo)
    pre_path=repo/"runtime/run_plans/cn_program_stage_c_system_search_prefreeze_v1.json"; pre,pre_hash=selfhash(pre_path,"prefreeze_payload_sha256")
    stageb_path=repo/"runtime/run_plans/cn_program_primitive_local_stage_b_d0160c6_independent_audit_20260820.json"; stageb,stageb_hash=selfhash(stageb_path,"audit_payload_sha256")
    dataset_path=repo/"runtime/run_plans/cn_program_spent_development_dataset_v2_1310_20260820.json"; dataset,dataset_hash=selfhash(dataset_path,"dataset_payload_sha256")
    replay_path=repo/"runtime/run_plans/cn_program_search_core_replay_v2_stagec_evidence_20260820.json"; replay,replay_hash=selfhash(replay_path,"replay_payload_sha256")
    stats_path=repo/"runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json"; stats,stats_hash=selfhash(stats_path,"stats_payload_sha256")
    raw=canary.resolve().read_bytes(); canary_file=hashlib.sha256(raw).hexdigest(); c=json.loads(raw.decode("utf-8-sig")); body=dict(c); c_hash=str(body.pop("official_canary_payload_sha256",""))
    if not c_hash or stable_hash(body)!=c_hash or c.get("status")!="PASS" or c.get("candidate_evaluation_executed") is not False: raise ValueError("official Stage C canary not PASS")
    runner_path=repo/"scripts/run_cn_program_stage_c_system_search_v1.py"; runner_sha=sha256_file(runner_path)
    if c.get("runner_source_file_sha256")!=runner_sha or c.get("prefreeze_payload_sha256")!=pre_hash: raise ValueError("official canary implementation binding drift")
    payload={"schema_version":runtime.AUTHORIZATION_SCHEMA,"status":"STAGE_C_SYSTEM_SEARCH_AUTHORIZED_NOT_RUN","execution_authorized":True,"campaign_id":runtime.CAMPAIGN_ID,"campaign_profile":runtime.CAMPAIGN_PROFILE,"project_control_route_id":runtime.ROUTE_ID,"permitted_project_control_actions":[ACTION_LAUNCH,ACTION_RETRY],"evaluation_data_role":"DEVELOPMENT_ONLY","source_mechanism_authorization":{"relative_path":str(source_runtime.AUTHORIZATION_RELATIVE_PATH).replace('\\','/'),"file_sha256":sha256_file(source_path),"payload_sha256":source["authorization_payload_sha256"]},"source_prior_exact":dict(source["source_prior_exact"]),"source_evaluator_authority":dict(source["source_evaluator_authority"]),"stage_c_prefreeze":{"relative_path":"runtime/run_plans/cn_program_stage_c_system_search_prefreeze_v1.json","file_sha256":sha256_file(pre_path),"payload_sha256":pre_hash,"cohort_exact_identities_sha256":pre["cohort"]["exact_identities_sha256"],"total_records":2016},"stage_b_transfer_audit":{"relative_path":"runtime/run_plans/cn_program_primitive_local_stage_b_d0160c6_independent_audit_20260820.json","file_sha256":sha256_file(stageb_path),"payload_sha256":stageb_hash,"search_transfer_status":stageb["search_transfer_status"]},"training_evidence":{"dataset":{"relative_path":"runtime/run_plans/cn_program_spent_development_dataset_v2_1310_20260820.json","file_sha256":sha256_file(dataset_path),"payload_sha256":dataset_hash,"rows":1310},"search_core_replay":{"relative_path":"runtime/run_plans/cn_program_search_core_replay_v2_stagec_evidence_20260820.json","file_sha256":sha256_file(replay_path),"payload_sha256":replay_hash,"qualified_leader":replay["qualified_trained_policy_leader"]},"primitive_stats":{"relative_path":"runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json","file_sha256":sha256_file(stats_path),"payload_sha256":stats_hash,"rows":1392}},"resource_contract":{"profile":"SEARCH_DUAL_24","cpu_threads":24,"executor_workers":24,"checkpoint_size":48,"candidate_evaluation_during_canary":False,"field_column_count":int(c["field_column_count"]),"field_columns_sha256":str(c["field_columns_sha256"])},"official_resource_canary":{"file_sha256":canary_file,"payload_sha256":c_hash,"field_column_count":int(c["field_column_count"]),"field_columns_sha256":str(c["field_columns_sha256"]),"runner_source_file_sha256":runner_sha},"system_search_contract":dict(pre["evaluation"]),"restricted_reads":{"validation":0,"holdout":0,"historical_2023":0,"forward_b":0,"forward_2026":0},"validation_feedback_used":False,"promotion_authorized":False,"oos_authority":"NONE","automatic_successor_authorized":False}
    payload["authorization_payload_sha256"]=stable_hash(payload); return payload

def main(argv=None)->int:
    p=argparse.ArgumentParser(); p.add_argument("--repo-root",type=Path,default=Path(__file__).resolve().parents[1]); p.add_argument("--official-resource-canary",type=Path,required=True); p.add_argument("--output",type=Path,default=runtime.AUTHORIZATION_RELATIVE_PATH); a=p.parse_args(argv); payload=build(a.repo_root,a.official_resource_canary); out=a.output if a.output.is_absolute() else a.repo_root/a.output; out.parent.mkdir(parents=True,exist_ok=True); out.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8"); print(json.dumps({"status":payload["status"],"authorization_payload_sha256":payload["authorization_payload_sha256"],"output":str(out.resolve())},sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
