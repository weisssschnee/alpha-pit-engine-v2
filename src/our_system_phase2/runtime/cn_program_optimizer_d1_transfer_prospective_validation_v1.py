"""Project-Control entry for prospective D1 transfer-filter validation.

The fresh D1 development cohort, all development-positive members, and the
filter top-40% membership must be frozen before this route can access validation.
This route has no optimizer-feedback, holdout, Forward-2026, or promotion authority.
"""
from __future__ import annotations

import argparse, json
from pathlib import Path
from typing import Any, Sequence

from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    ProjectControlDenied,
    consume_active_admission,
    sha256_file,
    verify_campaign_authorization_binding,
    verify_consumed_admission_target,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

ROUTE_ID="cn-program-optimizer-d1-transfer-prospective-validation-v1"
CAMPAIGN_ID="CN_PROGRAM_OPTIMIZER_D1_TRANSFER_PROSPECTIVE_VALIDATION_V1"
CAMPAIGN_PROFILE="cn_program_optimizer_d1_transfer_prospective_validation_v1"
AUTHORIZATION_RELATIVE_PATH=Path("runtime/run_plans/cn_program_optimizer_d1_transfer_prospective_validation_v1.json")
VALIDATION_WINDOWS=(
    {"window_id":"validation_1","start_date":"2025-07-08","end_date":"2025-08-08","session_count":24},
    {"window_id":"validation_2","start_date":"2025-08-11","end_date":"2025-09-11","session_count":24},
    {"window_id":"validation_3","start_date":"2025-09-12","end_date":"2025-10-24","session_count":25},
)

def _read_self_hashed(path:Path,field:str,label:str)->dict[str,Any]:
    payload=json.loads(path.resolve().read_text(encoding="utf-8-sig")); body=dict(payload); claimed=str(body.pop(field,""))
    if not claimed or stable_hash(body)!=claimed: raise ValueError(f"{label} self-hash drift")
    return payload

def _relative(root:Path,raw:Any)->Path:
    p=(root/Path(str(raw))).resolve()
    if not p.is_relative_to(root.resolve()): raise ValueError("prospective validation binding escapes repo")
    return p

def verify_authorization(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
    root=Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload=_read_self_hashed(path,"authorization_payload_sha256","prospective transfer validation authorization")
    if (
        payload.get("schema_version")!="cn_program_optimizer_d1_transfer_prospective_validation_authorization_v1"
        or payload.get("status")!="D1_TRANSFER_PROSPECTIVE_VALIDATION_FROZEN_READY"
        or payload.get("campaign_id")!=CAMPAIGN_ID
        or payload.get("campaign_profile")!=CAMPAIGN_PROFILE
        or payload.get("project_control_route_id")!=ROUTE_ID
        or not bool(payload.get("execution_authorized"))
        or list(payload.get("permitted_project_control_actions") or ())!=[ACTION_LAUNCH,ACTION_RETRY]
        or payload.get("evaluation_role")!="validation"
        or payload.get("usage")!="REPORT_ONLY_PROSPECTIVE_TRANSFER_TEST"
        or payload.get("optimizer_feedback_write")!="FORBIDDEN"
        or payload.get("scheduler_write")!="FORBIDDEN"
        or payload.get("archive_write")!="FORBIDDEN"
        or bool(payload.get("promotion_authorized"))
        or int(payload.get("holdout_reads") or 0)!=0
        or int(payload.get("forward_2026_reads") or 0)!=0
        or list(payload.get("validation_windows") or ())!=list(VALIDATION_WINDOWS)
    ): raise ValueError("prospective transfer validation authorization contract drift")
    cand=dict(payload.get("candidate_freeze") or {}); freeze_path=_relative(root,cand.get("relative_path")); members_path=_relative(root,cand.get("members_relative_path"))
    if sha256_file(freeze_path)!=str(cand.get("file_sha256") or "") or sha256_file(members_path)!=str(cand.get("members_file_sha256") or ""): raise ValueError("prospective candidate freeze file drift")
    freeze=_read_self_hashed(freeze_path,"freeze_payload_sha256","prospective candidate freeze")
    if (
        freeze.get("status")!="FROZEN_BEFORE_VALIDATION_ACCESS"
        or freeze.get("freeze_payload_sha256")!=cand.get("payload_sha256")
        or int(freeze.get("candidate_count") or 0)!=int(cand.get("candidate_count") or -1)
        or freeze.get("candidate_exact_identities_sha256")!=cand.get("candidate_exact_identities_sha256")
        or int(freeze.get("transfer_filter_selected_count") or 0)!=int(cand.get("transfer_filter_selected_count") or -1)
        or freeze.get("transfer_filter_selected_exact_identities_sha256")!=cand.get("transfer_filter_selected_exact_identities_sha256")
    ): raise ValueError("prospective candidate freeze semantic drift")
    flt=dict(payload.get("transfer_filter") or {}); filter_path=_relative(root,flt.get("relative_path"))
    if sha256_file(filter_path)!=str(flt.get("file_sha256") or ""): raise ValueError("prospective transfer filter file drift")
    filter_payload=_read_self_hashed(filter_path,"filter_payload_sha256","prospective transfer filter")
    if filter_payload.get("filter_payload_sha256")!=flt.get("payload_sha256") or filter_payload.get("filter_id")!=flt.get("filter_id") or dict(filter_payload.get("prospective_B_validation_acceptance") or {})!=dict(flt.get("acceptance_contract") or {}): raise ValueError("prospective transfer filter semantic drift")
    prep=dict(payload.get("prepared_binding") or {}); prep_path=_relative(root,prep.get("relative_path"))
    if sha256_file(prep_path)!=str(prep.get("file_sha256") or ""): raise ValueError("prospective prepared binding file drift")
    prepared=_read_self_hashed(prep_path,"prepared_binding_payload_sha256","prospective validation prepared binding")
    if (
        prepared.get("prepared_binding_payload_sha256")!=prep.get("payload_sha256")
        or prepared.get("status")!="D1_TRANSFER_PROSPECTIVE_VALIDATION_PREFINANCIAL_READY"
        or int(prepared.get("candidate_count") or 0)!=int(cand.get("candidate_count") or -1)
        or prepared.get("candidate_exact_identities_sha256")!=cand.get("candidate_exact_identities_sha256")
        or int(prepared.get("transfer_filter_selected_count") or 0)!=int(cand.get("transfer_filter_selected_count") or -1)
        or bool(prepared.get("candidate_evaluation_executed"))
        or int(prepared.get("holdout_reads") or 0)!=0
        or int(prepared.get("forward_2026_reads") or 0)!=0
    ): raise ValueError("prospective prepared binding semantic drift")
    return payload

def main(argv:Sequence[str]|None=None)->int:
    admission=consume_active_admission("cn-program-optimizer-d1-transfer-prospective-validation-v1",{ACTION_LAUNCH,ACTION_RETRY})
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--campaign-authorization",type=Path,required=True); p.add_argument("--source-contract",type=Path,required=True); p.add_argument("--registry",type=Path,required=True); p.add_argument("--output-root",type=Path,required=True); p.add_argument("--workers",type=int,default=4); args=p.parse_args(argv)
    verify_consumed_admission_target(admission,output_root=args.output_root); verified=verify_campaign_authorization_binding(admission,args.campaign_authorization)
    repo_root=Path(__file__).resolve().parents[3]; authorization=verify_authorization(verified.path,repo_root=repo_root)
    if dict(verified.payload)!=authorization: raise ProjectControlDenied("prospective validation campaign authorization payload drift")
    if str(admission.get("requested_action") or "") not in {ACTION_LAUNCH,ACTION_RETRY}: raise ProjectControlDenied("prospective validation requested action drift")
    source=dict(authorization.get("source_binding") or {})
    if sha256_file(args.source_contract.resolve())!=str(source.get("source_contract_sha256") or ""): raise ProjectControlDenied("prospective validation source contract drift")
    if sha256_file(args.registry.resolve())!=str(source.get("registry_sha256") or ""): raise ProjectControlDenied("prospective validation registry drift")
    from scripts.run_cn_program_optimizer_d1_transfer_prospective_validation_v1 import run
    prep_path=_relative(repo_root,authorization["prepared_binding"]["relative_path"]); filter_path=_relative(repo_root,authorization["transfer_filter"]["relative_path"])
    result=run(argparse.Namespace(repo_root=repo_root,prepared_binding=prep_path,transfer_filter=filter_path,source_contract=args.source_contract,registry=args.registry,output_root=args.output_root,workers=args.workers))
    print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
