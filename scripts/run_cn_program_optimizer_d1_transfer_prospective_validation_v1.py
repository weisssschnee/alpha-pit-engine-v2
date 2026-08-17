"""Prospective report-only validation for a fresh D1 cohort and frozen transfer filter.

Every development-productive Program in the frozen fresh cohort is evaluated.
Transfer-filter membership was frozen before validation access and is used only
to compute prospective precision/lift/recall.  No validation result is written
back to the optimizer, scheduler, or archive.
"""
from __future__ import annotations

import argparse, json, math, time, sys
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT=Path(__file__).resolve().parents[1]
for p in (PROJECT_ROOT,PROJECT_ROOT/"src"):
    if str(p) not in sys.path: sys.path.insert(0,str(p))

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_d1_report_only_validation_v1 as base
from scripts.prepare_cn_program_optimizer_d1_transfer_prospective_validation_v1 import VALIDATION_WINDOWS, _load_freeze, _read_json, _read_jsonl, _sha256, _verify_self_hash
from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA_VERSION="cn_program_optimizer_d1_transfer_filter_v2_C_prospective_validation_v1"
CLOSURE_NAME="CN_PROGRAM_OPTIMIZER_D1_TRANSFER_FILTER_V2_C_PROSPECTIVE_VALIDATION_COMPLETE.json"
PASS_STATUS="PASS_PROSPECTIVE_FILTER_V2"
FAIL_STATUS="FAIL_PROSPECTIVE_FILTER_V2"
EXPECTED_SOURCE_COHORT="D1_CONTINUATION_C"
EXPECTED_CANDIDATE_COUNT=67
EXPECTED_SELECTED_COUNT=27
EXPECTED_UNSELECTED_COUNT=40
EXPECTED_CANDIDATE_SET_SHA256="80baef6280f1ae0c10644e10a1391679d7ed5b8aa56ad540cfaf0ba811d6ae18"
EXPECTED_SELECTED_SET_SHA256="53b7f2aab3dbeec8964525e6a46ce20509b4081044836b8baef068e5740d619b"
EXPECTED_FREEZE_PAYLOAD_SHA256="389cf2e86db80104d58110a3a477b8b42b6259c049939c640d41634c8283cb6f"
EXPECTED_FILTER_PAYLOAD_SHA256="512c81728e3fca363ffcb3a148b3fd09f088e1f56d9f3f330b9a9f1170466081"
EXPECTED_ACCEPTANCE_CONTRACT={
    "all_conditions_required":True,
    "filtered_minus_unfiltered_precision_minimum":0.1,
    "filtered_precision_minimum":0.35,
    "filtered_recall_minimum":0.6,
    "minimum_development_productive_population":30,
    "rule_change_after_C_development_or_validation":"FORBIDDEN",
}


def _write_json(path:Path,payload:Mapping[str,Any])->Path:
    path.parent.mkdir(parents=True,exist_ok=True); path.write_text(json.dumps(dict(payload),ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8"); return path

def _wilson(successes:int,total:int)->dict[str,float]:
    if total<=0: return {"lower":0.0,"upper":1.0}
    z=1.959963984540054; rate=successes/total; denom=1+z*z/total; center=rate+z*z/(2*total); radius=z*math.sqrt(rate*(1-rate)/total+z*z/(4*total*total)); return {"lower":(center-radius)/denom,"upper":(center+radius)/denom}

def _metric(rows:Sequence[Mapping[str,Any]])->dict[str,Any]:
    n=len(rows); admitted=sum(bool(dict(x["admission"])["admitted"]) for x in rows); productive=sum(bool(x["validation_productive"]) for x in rows)
    return {"evaluated":n,"admitted":admitted,"productive":productive,"admission_rate":admitted/n if n else 0.0,"productive_transfer_rate":productive/n if n else 0.0,"productive_wilson_95":_wilson(productive,n)}
def _grouped(rows:Sequence[Mapping[str,Any]],field:str)->dict[str,Any]:
    return {v:_metric([x for x in rows if str(x[field])==v]) for v in sorted(set(str(x[field]) for x in rows))}

def _filter_acceptance_contract(filter_payload:Mapping[str,Any])->dict[str,Any]:
    if "prospective_B_validation_acceptance" in filter_payload: raise RuntimeError("stale B acceptance contract is not valid for C validation")
    contract=dict(filter_payload.get("prospective_C_validation_acceptance") or {})
    if contract!=EXPECTED_ACCEPTANCE_CONTRACT: raise RuntimeError("prospective C transfer acceptance contract drift")
    return contract


def _acceptance(*, all_rows:Sequence[Mapping[str,Any]], selected_rows:Sequence[Mapping[str,Any]], contract:Mapping[str,Any])->dict[str,Any]:
    all_metric=_metric(all_rows); sel_metric=_metric(selected_rows); total_positive=int(all_metric["productive"]); selected_positive=int(sel_metric["productive"])
    recall=(selected_positive/total_positive) if total_positive else 0.0
    lift=float(sel_metric["productive_transfer_rate"])-float(all_metric["productive_transfer_rate"])
    checks={
        "minimum_development_productive_population": len(all_rows)>=int(contract["minimum_development_productive_population"]),
        "filtered_precision_minimum": float(sel_metric["productive_transfer_rate"])>=float(contract["filtered_precision_minimum"]),
        "filtered_minus_unfiltered_precision_minimum": lift>=float(contract["filtered_minus_unfiltered_precision_minimum"]),
        "filtered_recall_minimum": recall>=float(contract["filtered_recall_minimum"]),
    }
    return {"status":PASS_STATUS if all(checks.values()) else FAIL_STATUS,"checks":checks,"all_development_positive_precision":float(all_metric["productive_transfer_rate"]),"filtered_precision":float(sel_metric["productive_transfer_rate"]),"filtered_minus_unfiltered_precision":lift,"filtered_recall":recall,"all_development_positive_count":len(all_rows),"filtered_count":len(selected_rows),"validation_productive_total":total_positive,"validation_productive_filtered":selected_positive,"contract":dict(contract)}


def _verify_admitted_materialized_output_root(output_root:Path)->Path:
    root=Path(output_root).resolve()
    if not root.is_dir() or not (root/".project_control_execution").is_dir() or not (root/"prefinancial").is_dir(): raise RuntimeError("D1_TRANSFER_C_VALIDATION_ADMITTED_OUTPUT_ROOT_MISSING")
    unexpected={path.name for path in root.iterdir() if path.name not in {".project_control_execution","prefinancial"}}
    if unexpected: raise RuntimeError(f"D1_TRANSFER_C_VALIDATION_ADMITTED_OUTPUT_ROOT_NOT_CLEAN: {sorted(unexpected)}")
    return root

def run(args:argparse.Namespace)->dict[str,Any]:
    repo=args.repo_root.resolve(); prepared=_read_json(args.prepared_binding.resolve()); _verify_self_hash(prepared,"prepared_binding_payload_sha256","prospective validation prepared binding")
    if prepared.get("status")!="D1_TRANSFER_PROSPECTIVE_VALIDATION_PREFINANCIAL_READY" or prepared.get("source_cohort")!=EXPECTED_SOURCE_COHORT: raise RuntimeError("prospective validation prepared binding not ready")
    freeze_root=Path(str(prepared["candidate_freeze_path"])).resolve().parent; freeze,members=_load_freeze(freeze_root)
    if str(prepared["candidate_freeze_payload_sha256"])!=EXPECTED_FREEZE_PAYLOAD_SHA256 or str(prepared["candidate_freeze_payload_sha256"])!=str(freeze["freeze_payload_sha256"]) or int(prepared["candidate_count"])!=EXPECTED_CANDIDATE_COUNT or int(prepared["candidate_count"])!=len(members) or str(prepared["candidate_exact_identities_sha256"])!=EXPECTED_CANDIDATE_SET_SHA256 or str(prepared["candidate_exact_identities_sha256"])!=str(freeze["candidate_exact_identities_sha256"]): raise RuntimeError("prospective prepared/freeze drift")
    if str(prepared["transfer_filter_id"])!=str(freeze["transfer_filter_id"]) or int(prepared["transfer_filter_selected_count"])!=EXPECTED_SELECTED_COUNT or int(prepared["transfer_filter_selected_count"])!=int(freeze["transfer_filter_selected_count"]) or str(prepared["transfer_filter_selected_exact_identities_sha256"])!=EXPECTED_SELECTED_SET_SHA256: raise RuntimeError("prospective prepared/filter drift")
    schedules=_read_jsonl(Path(str(prepared["resolved_schedule_path"])))
    if _sha256(Path(str(prepared["resolved_schedule_path"])))!=str(prepared["resolved_schedule_file_sha256"]) or len(schedules)!=len(members): raise RuntimeError("prospective schedule file drift")
    exacts=[str(x.get("d1_exact_identity") or x.get("successor_exact_identity") or "") for x in schedules]; ids=sorted(str(x["exact_identity"]) for x in members)
    if len(set(exacts))!=len(members) or sorted(exacts)!=ids: raise RuntimeError("prospective schedule exact set drift")
    member_by={str(x["exact_identity"]):dict(x) for x in members}
    for schedule,exact in zip(schedules,exacts,strict=True):
        m=member_by[exact]; body={k:v for k,v in schedule.items() if k!="schedule_record_sha256"}
        if stable_hash(body)!=str(schedule.get("schedule_record_sha256") or "") or str(schedule["schedule_record_sha256"])!=str(m["schedule_record_sha256"]): raise RuntimeError("prospective schedule hash/member drift")
    root=_verify_admitted_materialized_output_root(args.output_root)
    record_root=root/"records"; record_root.mkdir()
    input_binding={"schema_version":"cn_program_optimizer_d1_transfer_filter_v2_C_prospective_validation_input_binding_v1","candidate_freeze_payload_sha256":freeze["freeze_payload_sha256"],"candidate_exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"candidate_count":len(members),"source_cohort":EXPECTED_SOURCE_COHORT,"transfer_filter_id":freeze["transfer_filter_id"],"transfer_filter_application_file_sha256":freeze["transfer_filter_application_file_sha256"],"transfer_filter_selected_count":freeze["transfer_filter_selected_count"],"transfer_filter_selected_exact_identities_sha256":freeze["transfer_filter_selected_exact_identities_sha256"],"prepared_binding_payload_sha256":prepared["prepared_binding_payload_sha256"],"prepared_binding_file_sha256":_sha256(args.prepared_binding.resolve()),"zero_read_preflight_payload_sha256":prepared["zero_read_preflight_payload_sha256"],"source_contract_path":str(args.source_contract.resolve()),"source_contract_sha256":_sha256(args.source_contract.resolve()),"validation_field_manifest_sha256":prepared["validation_field_manifest_sha256"],"validation_label_manifest_sha256":prepared["validation_label_manifest_sha256"],"validation_session_authority_manifest_sha256":prepared["validation_session_authority_manifest_sha256"],"validation_windows":list(VALIDATION_WINDOWS),"evaluation_role":"validation","usage":"REPORT_ONLY_PROSPECTIVE_TRANSFER_FILTER_V2_C_TEST","validation_reads_before_project_control_admission":0,"optimizer_feedback_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","archive_write":"FORBIDDEN","search_memory_write":"FORBIDDEN","holdout_reads":0,"historical_challenge_reads":0,"forward_b_reads":0,"forward_2026_reads":0,"promotion_authorized":False}
    input_binding["input_binding_sha256"]=stable_hash(input_binding); _write_json(root/"input_binding.json",input_binding)
    started=time.perf_counter(); results=[]
    with ProcessPoolExecutor(max_workers=int(args.workers),initializer=base._initialize_worker,initargs=(str(args.source_contract.resolve()),str(Path(str(prepared["validation_field_root"]))),str(Path(str(prepared["validation_label_root"]))),str(Path(str(prepared["validation_session_authority_root"]))),str(args.registry.resolve()),str(input_binding["input_binding_sha256"]),str(record_root),tuple(members))) as executor:
        futures={executor.submit(base._evaluate_one,s,ordinal=i):i for i,s in enumerate(schedules)}; pending=set(futures)
        while pending:
            done,pending=wait(pending,timeout=2.0,return_when=FIRST_COMPLETED)
            for f in done: results.append(f.result())
            engine._require_runtime_resource_safety(engine._runtime_resource_snapshot())
    results.sort(key=lambda x:int(x["validation_record_ordinal"]))
    if len(results)!=EXPECTED_CANDIDATE_COUNT or len(results)!=len(members): raise RuntimeError("prospective validation result coverage drift")
    for r in results:
        m=member_by[str(r["exact_identity"])]; r["transfer_filter_selected"]=bool(m["transfer_filter_selected"]); r["transfer_linear_score"]=float(m["transfer_linear_score"]); r["transfer_probability_score"]=float(m["transfer_probability_score"])
    selected=[r for r in results if bool(r["transfer_filter_selected"])]
    unselected=[r for r in results if not bool(r["transfer_filter_selected"])]
    selected_ids=sorted(str(r["exact_identity"]) for r in selected)
    if len(selected)!=EXPECTED_SELECTED_COUNT or len(unselected)!=EXPECTED_UNSELECTED_COUNT or stable_hash(selected_ids)!=EXPECTED_SELECTED_SET_SHA256: raise RuntimeError("prospective validation selected membership drift")
    filter_payload=_read_json(args.transfer_filter.resolve()); _verify_self_hash(filter_payload,"filter_payload_sha256","transfer filter")
    if filter_payload.get("filter_payload_sha256")!=EXPECTED_FILTER_PAYLOAD_SHA256 or filter_payload.get("filter_id")!=freeze["transfer_filter_id"]: raise RuntimeError("prospective transfer filter identity drift")
    acceptance=_acceptance(all_rows=results,selected_rows=selected,contract=_filter_acceptance_contract(filter_payload))
    metrics={"all_67":_metric(results),"selected_27":_metric(selected),"unselected_40":_metric(unselected),"per_template":_grouped(results,"template_id"),"per_selector":_grouped(results,"selection_kind"),"prospective_transfer_filter_acceptance":acceptance}
    _write_json(root/"validation_metrics.json",metrics)
    closure={"schema_version":SCHEMA_VERSION,"status":acceptance["status"],"execution_status":"COMPLETE","candidate_count":len(members),"candidate_exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"source_cohort":EXPECTED_SOURCE_COHORT,"transfer_filter_id":freeze["transfer_filter_id"],"transfer_filter_payload_sha256":filter_payload["filter_payload_sha256"],"transfer_filter_selected_count":len(selected),"transfer_filter_unselected_count":len(unselected),"transfer_filter_selected_exact_identities_sha256":freeze["transfer_filter_selected_exact_identities_sha256"],"input_binding_sha256":input_binding["input_binding_sha256"],"metrics":metrics,"validation_windows":list(VALIDATION_WINDOWS),"wall_seconds":float(time.perf_counter()-started),"validation_reads_before_project_control_admission":0,"validation_reads_per_worker_context":int(results[0]["validation_reads"]) if results else 0,"holdout_reads":0,"historical_challenge_reads":0,"forward_b_reads":0,"forward_2026_reads":0,"optimizer_feedback_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","archive_write":"FORBIDDEN","search_memory_write":"FORBIDDEN","candidate_promotion":False,"promotion_authorized":False,"automatic_successor_authorized":False,"oos_authority":"VALIDATION_REPORT_ONLY_EVIDENCE_ONLY","holdout_authority":"NONE"}
    closure["closure_payload_sha256"]=stable_hash(closure); _write_json(root/CLOSURE_NAME,closure); return closure

def parser()->argparse.ArgumentParser:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--repo-root",type=Path,required=True); p.add_argument("--prepared-binding",type=Path,required=True); p.add_argument("--transfer-filter",type=Path,required=True); p.add_argument("--source-contract",type=Path,required=True); p.add_argument("--registry",type=Path,required=True); p.add_argument("--output-root",type=Path,required=True); p.add_argument("--workers",type=int,default=8); return p

def main(argv:Sequence[str]|None=None)->int:
    result=run(parser().parse_args(argv)); print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
