"""Independently audit the terminal D1 Transfer Filter V2 C validation root."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT=Path(__file__).resolve().parents[1]
for path in (PROJECT_ROOT,PROJECT_ROOT/"src"):
    if str(path) not in sys.path: sys.path.insert(0,str(path))

from our_system_phase2.services.unified_capability_registry import stable_hash

ROUTE_ID="cn-program-optimizer-d1-transfer-prospective-validation-v1"
CAMPAIGN_ID="CN_PROGRAM_OPTIMIZER_D1_TRANSFER_FILTER_V2_C_PROSPECTIVE_VALIDATION"
CAMPAIGN_PROFILE="cn_program_optimizer_d1_transfer_filter_v2_c_prospective_validation"
CLOSURE_NAME="CN_PROGRAM_OPTIMIZER_D1_TRANSFER_FILTER_V2_C_PROSPECTIVE_VALIDATION_COMPLETE.json"
EXPECTED_CANDIDATE_COUNT=67
EXPECTED_SELECTED_COUNT=27
EXPECTED_UNSELECTED_COUNT=40
EXPECTED_CANDIDATE_SET_SHA256="80baef6280f1ae0c10644e10a1391679d7ed5b8aa56ad540cfaf0ba811d6ae18"
EXPECTED_SELECTED_SET_SHA256="53b7f2aab3dbeec8964525e6a46ce20509b4081044836b8baef068e5740d619b"
EXPECTED_FREEZE_PAYLOAD_SHA256="389cf2e86db80104d58110a3a477b8b42b6259c049939c640d41634c8283cb6f"
EXPECTED_FILTER_PAYLOAD_SHA256="512c81728e3fca363ffcb3a148b3fd09f088e1f56d9f3f330b9a9f1170466081"
EXPECTED_OUTCOME_PAYLOAD_SHA256="4e75e06d20834999d72408e014bd0cd58e7a22d74a7805db1d08d0a74df8e2f1"
EXPECTED_REPRO_AUDIT_PAYLOAD_SHA256="093e18fac0b778fd1653a6e8b989588c37f649bc080c7c85adb10cbe0cc54031"
EXPECTED_CONTRACT={
    "all_conditions_required":True,
    "filtered_minus_unfiltered_precision_minimum":0.1,
    "filtered_precision_minimum":0.35,
    "filtered_recall_minimum":0.6,
    "minimum_development_productive_population":30,
    "rule_change_after_C_development_or_validation":"FORBIDDEN",
}


def _read_json(path:Path)->dict[str,Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path:Path)->list[dict[str,Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha256(path:Path)->str:
    h=hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda:handle.read(4*1024*1024),b""): h.update(block)
    return h.hexdigest()


def _verify_hash(payload:Mapping[str,Any],field:str,label:str)->str:
    body=dict(payload); claimed=str(body.pop(field,""))
    if not claimed or stable_hash(body)!=claimed: raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _productive(result:Mapping[str,Any])->bool:
    admission=dict(result.get("admission") or {}); uplift=result.get("uplift")
    if not bool(admission.get("admitted")) or not isinstance(uplift,Mapping): return False
    credit=dict(uplift.get("program_credit") or {})
    return float(credit.get("matched_cumulative_net_return_increment") or 0.0)>0.0 and float(credit.get("matched_net_reward_increment") or 0.0)>0.0


def _metric(rows:Sequence[Mapping[str,Any]])->dict[str,Any]:
    n=len(rows); admitted=sum(bool(dict(row["admission"])["admitted"]) for row in rows); productive=sum(bool(row["validation_productive"]) for row in rows)
    return {"evaluated":n,"admitted":admitted,"productive":productive,"admission_rate":admitted/n if n else 0.0,"productive_transfer_rate":productive/n if n else 0.0}


def _verdict(*,all_rows:Sequence[Mapping[str,Any]],selected_rows:Sequence[Mapping[str,Any]])->dict[str,Any]:
    all_metric=_metric(all_rows); selected_metric=_metric(selected_rows)
    total_positive=int(all_metric["productive"]); selected_positive=int(selected_metric["productive"])
    recall=selected_positive/total_positive if total_positive else 0.0
    lift=float(selected_metric["productive_transfer_rate"])-float(all_metric["productive_transfer_rate"])
    checks={
        "minimum_development_productive_population":len(all_rows)>=int(EXPECTED_CONTRACT["minimum_development_productive_population"]),
        "filtered_precision_minimum":float(selected_metric["productive_transfer_rate"])>=float(EXPECTED_CONTRACT["filtered_precision_minimum"]),
        "filtered_minus_unfiltered_precision_minimum":lift>=float(EXPECTED_CONTRACT["filtered_minus_unfiltered_precision_minimum"]),
        "filtered_recall_minimum":recall>=float(EXPECTED_CONTRACT["filtered_recall_minimum"]),
    }
    return {
        "status":"PASS_PROSPECTIVE_FILTER_V2" if all(checks.values()) else "FAIL_PROSPECTIVE_FILTER_V2",
        "checks":checks,
        "all_development_positive_precision":float(all_metric["productive_transfer_rate"]),
        "filtered_precision":float(selected_metric["productive_transfer_rate"]),
        "filtered_minus_unfiltered_precision":lift,
        "filtered_recall":recall,
        "all_development_positive_count":len(all_rows),
        "filtered_count":len(selected_rows),
        "validation_productive_total":total_positive,
        "validation_productive_filtered":selected_positive,
        "contract":EXPECTED_CONTRACT,
    }


def audit(*,repo_root:Path,run_root:Path,authorization_path:Path,admission_path:Path,evidence_repo_sha:str,output:Path)->dict[str,Any]:
    repo=repo_root.resolve(); root=run_root.resolve(); authorization_path=authorization_path.resolve(); admission_path=admission_path.resolve()
    authorization=_read_json(authorization_path); authorization_payload_sha=_verify_hash(authorization,"authorization_payload_sha256","C validation authorization")
    if authorization.get("campaign_id")!=CAMPAIGN_ID or authorization.get("campaign_profile")!=CAMPAIGN_PROFILE or authorization.get("project_control_route_id")!=ROUTE_ID or authorization.get("source_cohort")!="D1_CONTINUATION_C" or list(authorization.get("permitted_project_control_actions") or ())!=["LAUNCH_HIGH_COST_CAMPAIGN"]: raise RuntimeError("C validation authorization target drift")
    if int(authorization.get("validation_reads_before_admission") or -1)!=0 or any(int(authorization.get(key) or 0)!=0 for key in ("holdout_reads","historical_challenge_reads","forward_b_reads","forward_2026_reads")): raise RuntimeError("C validation authorization restricted-read drift")
    if any(authorization.get(key)!="FORBIDDEN" for key in ("optimizer_feedback_write","scheduler_write","archive_write","search_memory_write")) or bool(authorization.get("promotion_authorized")) or bool(authorization.get("automatic_successor_authorized")): raise RuntimeError("C validation authorization write/continuation drift")
    if dict(authorization.get("transfer_filter",{})).get("payload_sha256")!=EXPECTED_FILTER_PAYLOAD_SHA256 or dict(authorization.get("development_outcome",{})).get("payload_sha256")!=EXPECTED_OUTCOME_PAYLOAD_SHA256 or dict(authorization.get("reproducibility_audit",{})).get("payload_sha256")!=EXPECTED_REPRO_AUDIT_PAYLOAD_SHA256: raise RuntimeError("C validation authorization evidence drift")
    candidate_binding=dict(authorization.get("candidate_freeze") or {})
    if int(candidate_binding.get("candidate_count") or 0)!=EXPECTED_CANDIDATE_COUNT or candidate_binding.get("candidate_exact_identities_sha256")!=EXPECTED_CANDIDATE_SET_SHA256 or int(candidate_binding.get("transfer_filter_selected_count") or 0)!=EXPECTED_SELECTED_COUNT or candidate_binding.get("transfer_filter_selected_exact_identities_sha256")!=EXPECTED_SELECTED_SET_SHA256 or candidate_binding.get("payload_sha256")!=EXPECTED_FREEZE_PAYLOAD_SHA256: raise RuntimeError("C validation authorization candidate drift")

    admission=_read_json(admission_path); admission_payload_sha=_verify_hash(admission,"admission_payload_sha256","C validation Project Control admission"); admission_file_sha=_sha256(admission_path)
    if admission.get("requested_action")!="LAUNCH_HIGH_COST_CAMPAIGN" or admission.get("target_campaign_id")!=ROUTE_ID or admission.get("target_run_id")!=root.name or Path(str(admission.get("target_output_root") or "")).resolve()!=root or admission.get("repo_sha")!=evidence_repo_sha: raise RuntimeError("C validation Project Control target drift")
    preflight=dict(admission.get("project_control_preflight") or {}).get("request") or {}
    if preflight.get("target_campaign_instance_id")!=CAMPAIGN_ID or preflight.get("target_campaign_profile")!=CAMPAIGN_PROFILE or Path(str(preflight.get("campaign_authorization_path") or "")).resolve()!=authorization_path or preflight.get("campaign_authorization_file_sha256")!=_sha256(authorization_path): raise RuntimeError("C validation Project Control authorization binding drift")
    control=root/".project_control_execution"; identity=_read_json(control/"execution_identity.json")
    if identity!={"schema_version":"cn_project_control_execution_identity_v1","project_id":admission["project_id"],"repo_sha":evidence_repo_sha,"target_campaign_id":ROUTE_ID,"target_run_id":root.name,"target_output_root":str(root),"root_action":"LAUNCH_HIGH_COST_CAMPAIGN","root_admission_file_sha256":admission_file_sha,"campaign_authorization_file_sha256":_sha256(authorization_path),"target_campaign_instance_id":CAMPAIGN_ID,"target_campaign_profile":CAMPAIGN_PROFILE}: raise RuntimeError("C validation execution identity drift")
    consumptions=list((control/"consumptions").glob("*.json"))
    if len(consumptions)!=1 or consumptions[0].stem!=admission_payload_sha: raise RuntimeError("C validation Project Control exactly-once drift")
    consumption=_read_json(consumptions[0])
    for key,expected in (("admission_file_sha256",admission_file_sha),("admission_payload_sha256",admission_payload_sha),("requested_action","LAUNCH_HIGH_COST_CAMPAIGN"),("target_campaign_id",ROUTE_ID),("target_run_id",root.name),("target_output_root",str(root)),("repo_sha",evidence_repo_sha),("campaign_authorization_file_sha256",_sha256(authorization_path)),("target_campaign_instance_id",CAMPAIGN_ID),("target_campaign_profile",CAMPAIGN_PROFILE)):
        if consumption.get(key)!=expected: raise RuntimeError(f"C validation consumption {key} drift")

    prefinancial=root/"prefinancial"; prepared_path=prefinancial/"D1_TRANSFER_PROSPECTIVE_VALIDATION_PREFINANCIAL_READY.json"; prepared=_read_json(prepared_path); prepared_payload_sha=_verify_hash(prepared,"prepared_binding_payload_sha256","C validation prepared binding")
    preflight_binding=Path(str(prepared["zero_read_preflight_path"])).resolve(); zero_read=_read_json(preflight_binding); zero_read_payload_sha=_verify_hash(zero_read,"zero_read_preflight_payload_sha256","C validation zero-read preflight")
    if prepared.get("zero_read_preflight_payload_sha256")!=zero_read_payload_sha or zero_read.get("implementation_repo_sha")!=evidence_repo_sha or int(zero_read.get("validation_reads_by_this_preflight") or -1)!=0 or bool(zero_read.get("candidate_evaluation_executed")): raise RuntimeError("C validation zero-read boundary drift")
    if int(prepared.get("candidate_count") or 0)!=EXPECTED_CANDIDATE_COUNT or prepared.get("candidate_exact_identities_sha256")!=EXPECTED_CANDIDATE_SET_SHA256 or int(prepared.get("transfer_filter_selected_count") or 0)!=EXPECTED_SELECTED_COUNT or prepared.get("transfer_filter_selected_exact_identities_sha256")!=EXPECTED_SELECTED_SET_SHA256 or int(prepared.get("validation_reads_before_project_control_admission") or -1)!=0: raise RuntimeError("C validation prepared membership drift")

    members_path=(repo/str(candidate_binding["members_relative_path"])).resolve(); members=_read_jsonl(members_path); member_by={str(row["exact_identity"]):row for row in members}; ids=sorted(member_by); selected_ids=sorted(exact for exact,row in member_by.items() if bool(row["transfer_filter_selected"]))
    if len(member_by)!=EXPECTED_CANDIDATE_COUNT or stable_hash(ids)!=EXPECTED_CANDIDATE_SET_SHA256 or len(selected_ids)!=EXPECTED_SELECTED_COUNT or stable_hash(selected_ids)!=EXPECTED_SELECTED_SET_SHA256: raise RuntimeError("C validation frozen membership drift")

    input_binding=_read_json(root/"input_binding.json"); input_binding_sha=_verify_hash(input_binding,"input_binding_sha256","C validation input binding")
    if input_binding.get("candidate_exact_identities_sha256")!=EXPECTED_CANDIDATE_SET_SHA256 or int(input_binding.get("candidate_count") or 0)!=EXPECTED_CANDIDATE_COUNT or input_binding.get("prepared_binding_payload_sha256")!=prepared_payload_sha or input_binding.get("zero_read_preflight_payload_sha256")!=zero_read_payload_sha or int(input_binding.get("validation_reads_before_project_control_admission") or -1)!=0: raise RuntimeError("C validation input binding drift")

    record_files=sorted((root/"records").glob("candidate_*.json"))
    if len(record_files)!=EXPECTED_CANDIDATE_COUNT: raise RuntimeError("C validation record cardinality drift")
    results=[]; observed=set()
    for path in record_files:
        wrapper=_read_json(path); pair=dict(wrapper.get("pair_record") or {}); result=dict(wrapper.get("validation_result") or {})
        pair_sha=_verify_hash(pair,"record_payload_sha256",f"C validation pair {path.name}"); _verify_hash(result,"result_payload_sha256",f"C validation result {path.name}")
        exact=str(result.get("exact_identity") or ""); member=member_by.get(exact)
        if member is None or exact in observed: raise RuntimeError("C validation result membership drift")
        observed.add(exact)
        if pair.get("exact_identity")!=exact or result.get("pair_record_sha256")!=pair_sha or pair.get("source_cohort")!="D1_CONTINUATION_C" or result.get("source_cohort")!="D1_CONTINUATION_C" or int(result.get("source_wave") if result.get("source_wave") is not None else -1)!=int(member["source_wave"]) or result.get("selection_kind")!=member["selection_kind"] or pair.get("source_schedule_record_sha256")!=member["schedule_record_sha256"] or result.get("development_productive") is not True: raise RuntimeError(f"C validation result lineage drift: {exact}")
        if bool(result.get("validation_productive"))!=_productive(result): raise RuntimeError(f"C validation productive recomputation drift: {exact}")
        if int(result.get("holdout_reads") or 0)!=0 or int(result.get("forward_2026_reads") or 0)!=0 or result.get("optimizer_feedback_write")!="FORBIDDEN" or bool(result.get("promotion_authorized")): raise RuntimeError(f"C validation result restricted boundary drift: {exact}")
        result["transfer_filter_selected"]=bool(member["transfer_filter_selected"]); results.append(result)
    if observed!=set(ids): raise RuntimeError("C validation result exact coverage drift")
    results.sort(key=lambda row:int(row["validation_record_ordinal"]))
    if [int(row["validation_record_ordinal"]) for row in results]!=list(range(EXPECTED_CANDIDATE_COUNT)): raise RuntimeError("C validation result ordinal drift")
    selected=[row for row in results if bool(row["transfer_filter_selected"])]; unselected=[row for row in results if not bool(row["transfer_filter_selected"])]
    if len(selected)!=EXPECTED_SELECTED_COUNT or len(unselected)!=EXPECTED_UNSELECTED_COUNT: raise RuntimeError("C validation selected/unselected count drift")
    verdict=_verdict(all_rows=results,selected_rows=selected)

    metrics=_read_json(root/"validation_metrics.json"); closure=_read_json(root/CLOSURE_NAME); closure_payload_sha=_verify_hash(closure,"closure_payload_sha256","C validation closure")
    if closure.get("execution_status")!="COMPLETE" or closure.get("status")!=verdict["status"] or closure.get("input_binding_sha256")!=input_binding_sha or int(closure.get("candidate_count") or 0)!=EXPECTED_CANDIDATE_COUNT or int(closure.get("transfer_filter_selected_count") or 0)!=EXPECTED_SELECTED_COUNT or int(closure.get("transfer_filter_unselected_count") or 0)!=EXPECTED_UNSELECTED_COUNT or dict(closure.get("metrics") or {})!=metrics or dict(metrics.get("prospective_transfer_filter_acceptance") or {})!=verdict: raise RuntimeError("C validation producer closure drift")
    if dict(metrics.get("all_67") or {}).get("evaluated")!=EXPECTED_CANDIDATE_COUNT or dict(metrics.get("selected_27") or {}).get("evaluated")!=EXPECTED_SELECTED_COUNT or dict(metrics.get("unselected_40") or {}).get("evaluated")!=EXPECTED_UNSELECTED_COUNT: raise RuntimeError("C validation aggregate cardinality drift")
    if any(int(closure.get(key) or 0)!=0 for key in ("validation_reads_before_project_control_admission","holdout_reads","historical_challenge_reads","forward_b_reads","forward_2026_reads")) or any(closure.get(key)!="FORBIDDEN" for key in ("optimizer_feedback_write","scheduler_write","archive_write","search_memory_write")) or bool(closure.get("candidate_promotion")) or bool(closure.get("promotion_authorized")) or bool(closure.get("automatic_successor_authorized")): raise RuntimeError("C validation terminal boundary drift")

    audit_payload={
        "schema_version":"cn_program_optimizer_d1_transfer_filter_v2_C_prospective_validation_independent_audit_v1",
        "status":"PASS_INDEPENDENT_C_TRANSFER_FILTER_V2_VALIDATION_AUDIT",
        "economic_verdict":verdict["status"],
        "execution_repo_sha":evidence_repo_sha,
        "run_root":str(root),
        "authorization_file_sha256":_sha256(authorization_path),
        "authorization_payload_sha256":authorization_payload_sha,
        "project_control_admission_file_sha256":admission_file_sha,
        "project_control_admission_payload_sha256":admission_payload_sha,
        "project_control_consumption_count":1,
        "project_control_exactly_once":True,
        "candidate_freeze_payload_sha256":EXPECTED_FREEZE_PAYLOAD_SHA256,
        "candidate_exact_identities_sha256":EXPECTED_CANDIDATE_SET_SHA256,
        "candidate_count":EXPECTED_CANDIDATE_COUNT,
        "selected_exact_identities_sha256":EXPECTED_SELECTED_SET_SHA256,
        "selected_count":EXPECTED_SELECTED_COUNT,
        "unselected_count":EXPECTED_UNSELECTED_COUNT,
        "zero_read_preflight_payload_sha256":zero_read_payload_sha,
        "prepared_binding_payload_sha256":prepared_payload_sha,
        "input_binding_payload_sha256":input_binding_sha,
        "producer_closure_payload_sha256":closure_payload_sha,
        "metrics":{"all_67":_metric(results),"selected_27":_metric(selected),"unselected_40":_metric(unselected),"acceptance":verdict},
        "restricted_reads":{"validation_before_admission":0,"holdout":0,"historical_spent_or_challenge":0,"forward_b":0,"forward_2026":0},
        "feedback_writes":{"optimizer":0,"scheduler":0,"archive":0,"search_memory":0},
        "candidate_promotion":False,
        "automatic_successor":False,
    }
    audit_payload["audit_payload_sha256"]=stable_hash(audit_payload)
    output=output.resolve(); output.parent.mkdir(parents=True,exist_ok=True); output.write_text(json.dumps(audit_payload,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    return audit_payload


def main(argv:Sequence[str]|None=None)->int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root",type=Path,required=True); parser.add_argument("--run-root",type=Path,required=True); parser.add_argument("--campaign-authorization",type=Path,required=True); parser.add_argument("--project-control-admission",type=Path,required=True); parser.add_argument("--evidence-repo-sha",required=True); parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args(argv); result=audit(repo_root=args.repo_root,run_root=args.run_root,authorization_path=args.campaign_authorization,admission_path=args.project_control_admission,evidence_repo_sha=args.evidence_repo_sha,output=args.output); print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0


if __name__=="__main__": raise SystemExit(main())
