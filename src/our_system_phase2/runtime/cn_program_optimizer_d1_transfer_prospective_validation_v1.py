"""Project-Control entry for prospective D1 transfer-filter validation.

The fresh D1 development cohort, all development-positive members, and the
filter top-40% membership must be frozen before this route can access validation.
This route has no optimizer-feedback, holdout, Forward-2026, or promotion authority.
"""
from __future__ import annotations

import argparse, json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ProjectControlDenied,
    consume_active_admission,
    sha256_file,
    verify_campaign_authorization_binding,
    verify_consumed_admission_target,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

ROUTE_ID="cn-program-optimizer-d1-transfer-prospective-validation-v1"
CAMPAIGN_ID="CN_PROGRAM_OPTIMIZER_D1_TRANSFER_FILTER_V2_C_PROSPECTIVE_VALIDATION"
CAMPAIGN_PROFILE="cn_program_optimizer_d1_transfer_filter_v2_c_prospective_validation"
AUTHORIZATION_RELATIVE_PATH=Path("runtime/run_plans/cn_program_optimizer_d1_transfer_filter_v2_C_prospective_validation_20260817.json")
EXPECTED_SOURCE_COHORT="D1_CONTINUATION_C"
EXPECTED_CANDIDATE_COUNT=67
EXPECTED_SELECTED_COUNT=27
EXPECTED_CANDIDATE_SET_SHA256="80baef6280f1ae0c10644e10a1391679d7ed5b8aa56ad540cfaf0ba811d6ae18"
EXPECTED_SELECTED_SET_SHA256="53b7f2aab3dbeec8964525e6a46ce20509b4081044836b8baef068e5740d619b"
EXPECTED_FREEZE_PAYLOAD_SHA256="389cf2e86db80104d58110a3a477b8b42b6259c049939c640d41634c8283cb6f"
EXPECTED_OUTCOME_PAYLOAD_SHA256="4e75e06d20834999d72408e014bd0cd58e7a22d74a7805db1d08d0a74df8e2f1"
EXPECTED_REPRO_AUDIT_PAYLOAD_SHA256="093e18fac0b778fd1653a6e8b989588c37f649bc080c7c85adb10cbe0cc54031"
EXPECTED_FILTER_PAYLOAD_SHA256="512c81728e3fca363ffcb3a148b3fd09f088e1f56d9f3f330b9a9f1170466081"
EXPECTED_ACCEPTANCE_CONTRACT={
    "all_conditions_required":True,
    "filtered_minus_unfiltered_precision_minimum":0.1,
    "filtered_precision_minimum":0.35,
    "filtered_recall_minimum":0.6,
    "minimum_development_productive_population":30,
    "rule_change_after_C_development_or_validation":"FORBIDDEN",
}
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

def _filter_acceptance_contract(filter_payload:Mapping[str,Any])->dict[str,Any]:
    if "prospective_B_validation_acceptance" in filter_payload: raise ValueError("stale B acceptance contract is not valid for C validation")
    contract=dict(filter_payload.get("prospective_C_validation_acceptance") or {})
    if contract!=EXPECTED_ACCEPTANCE_CONTRACT: raise ValueError("prospective C transfer acceptance contract drift")
    return contract

def verify_authorization(path:Path,*,repo_root:Path|None=None)->dict[str,Any]:
    root=Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload=_read_self_hashed(path,"authorization_payload_sha256","prospective transfer validation authorization")
    if (
        payload.get("schema_version")!="cn_program_optimizer_d1_transfer_prospective_validation_authorization_v2"
        or payload.get("status")!="D1_TRANSFER_FILTER_V2_C_PROSPECTIVE_VALIDATION_FROZEN_READY"
        or payload.get("campaign_id")!=CAMPAIGN_ID
        or payload.get("campaign_profile")!=CAMPAIGN_PROFILE
        or payload.get("project_control_route_id")!=ROUTE_ID
        or not bool(payload.get("execution_authorized"))
        or payload.get("source_cohort")!=EXPECTED_SOURCE_COHORT
        or list(payload.get("permitted_project_control_actions") or ())!=[ACTION_LAUNCH]
        or payload.get("evaluation_role")!="validation"
        or payload.get("usage")!="REPORT_ONLY_PROSPECTIVE_TRANSFER_FILTER_V2_C_TEST"
        or payload.get("optimizer_feedback_write")!="FORBIDDEN"
        or payload.get("scheduler_write")!="FORBIDDEN"
        or payload.get("archive_write")!="FORBIDDEN"
        or payload.get("search_memory_write")!="FORBIDDEN"
        or bool(payload.get("promotion_authorized"))
        or int(payload.get("validation_reads_before_admission") or -1)!=0
        or int(payload.get("holdout_reads") or 0)!=0
        or int(payload.get("historical_challenge_reads") or 0)!=0
        or int(payload.get("forward_b_reads") or 0)!=0
        or int(payload.get("forward_2026_reads") or 0)!=0
        or list(payload.get("validation_windows") or ())!=list(VALIDATION_WINDOWS)
    ): raise ValueError("prospective transfer validation authorization contract drift")
    cand=dict(payload.get("candidate_freeze") or {}); freeze_path=_relative(root,cand.get("relative_path")); members_path=_relative(root,cand.get("members_relative_path"))
    if sha256_file(freeze_path)!=str(cand.get("file_sha256") or ""): raise ValueError("prospective candidate freeze file drift")
    members=[json.loads(line) for line in members_path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    if stable_hash(members)!=str(cand.get("members_payload_sha256") or ""): raise ValueError("prospective candidate members semantic drift")
    freeze=_read_self_hashed(freeze_path,"freeze_payload_sha256","prospective candidate freeze")
    if (
        freeze.get("status")!="FROZEN_BEFORE_VALIDATION_ACCESS"
        or freeze.get("source_cohort")!=EXPECTED_SOURCE_COHORT
        or freeze.get("freeze_payload_sha256")!=cand.get("payload_sha256")
        or freeze.get("freeze_payload_sha256")!=EXPECTED_FREEZE_PAYLOAD_SHA256
        or int(freeze.get("candidate_count") or 0)!=int(cand.get("candidate_count") or -1)
        or int(freeze.get("candidate_count") or 0)!=EXPECTED_CANDIDATE_COUNT
        or freeze.get("candidate_exact_identities_sha256")!=cand.get("candidate_exact_identities_sha256")
        or freeze.get("candidate_exact_identities_sha256")!=EXPECTED_CANDIDATE_SET_SHA256
        or freeze.get("candidate_members_payload_sha256")!=cand.get("members_payload_sha256")
        or int(freeze.get("transfer_filter_selected_count") or 0)!=int(cand.get("transfer_filter_selected_count") or -1)
        or int(freeze.get("transfer_filter_selected_count") or 0)!=EXPECTED_SELECTED_COUNT
        or freeze.get("transfer_filter_selected_exact_identities_sha256")!=cand.get("transfer_filter_selected_exact_identities_sha256")
        or freeze.get("transfer_filter_selected_exact_identities_sha256")!=EXPECTED_SELECTED_SET_SHA256
    ): raise ValueError("prospective candidate freeze semantic drift")
    flt=dict(payload.get("transfer_filter") or {}); filter_path=_relative(root,flt.get("relative_path"))
    if sha256_file(filter_path)!=str(flt.get("file_sha256") or ""): raise ValueError("prospective transfer filter file drift")
    filter_payload=_read_self_hashed(filter_path,"filter_payload_sha256","prospective transfer filter")
    if filter_payload.get("filter_payload_sha256")!=EXPECTED_FILTER_PAYLOAD_SHA256 or filter_payload.get("filter_payload_sha256")!=flt.get("payload_sha256") or filter_payload.get("filter_id")!=flt.get("filter_id") or _filter_acceptance_contract(filter_payload)!=dict(flt.get("acceptance_contract") or {}): raise ValueError("prospective transfer filter semantic drift")
    prep=dict(payload.get("zero_read_preflight") or {}); prep_path=Path(str(prep.get("path") or "")).resolve()
    if not prep_path.is_file(): raise ValueError("prospective zero-read preflight missing")
    if sha256_file(prep_path)!=str(prep.get("file_sha256") or ""): raise ValueError("prospective zero-read preflight file drift")
    prepared=_read_self_hashed(prep_path,"zero_read_preflight_payload_sha256","prospective validation zero-read preflight")
    if (
        prepared.get("zero_read_preflight_payload_sha256")!=prep.get("payload_sha256")
        or prepared.get("status")!="D1_TRANSFER_PROSPECTIVE_VALIDATION_ZERO_READ_PREFLIGHT_READY"
        or prepared.get("source_cohort")!=EXPECTED_SOURCE_COHORT
        or int(prepared.get("candidate_count") or 0)!=int(cand.get("candidate_count") or -1)
        or int(prepared.get("candidate_count") or 0)!=EXPECTED_CANDIDATE_COUNT
        or prepared.get("candidate_exact_identities_sha256")!=cand.get("candidate_exact_identities_sha256")
        or prepared.get("candidate_exact_identities_sha256")!=EXPECTED_CANDIDATE_SET_SHA256
        or int(prepared.get("transfer_filter_selected_count") or 0)!=int(cand.get("transfer_filter_selected_count") or -1)
        or int(prepared.get("transfer_filter_selected_count") or 0)!=EXPECTED_SELECTED_COUNT
        or prepared.get("transfer_filter_selected_exact_identities_sha256")!=EXPECTED_SELECTED_SET_SHA256
        or bool(prepared.get("candidate_evaluation_executed"))
        or int(prepared.get("validation_reads_by_this_preflight") or -1)!=0
        or int(prepared.get("holdout_reads") or 0)!=0
        or int(prepared.get("forward_2026_reads") or 0)!=0
    ): raise ValueError("prospective prepared binding semantic drift")
    for key,field,expected,status in (
        ("development_outcome","outcome_payload_sha256",EXPECTED_OUTCOME_PAYLOAD_SHA256,"C_DEVELOPMENT_COMPLETE_AND_V2_MEMBERSHIP_FROZEN_BEFORE_VALIDATION"),
        ("reproducibility_audit","audit_payload_sha256",EXPECTED_REPRO_AUDIT_PAYLOAD_SHA256,"PASS_V2_REPRODUCIBILITY_RESTORED_FROM_DURABLE_EVIDENCE"),
    ):
        binding=dict(payload.get(key) or {}); evidence_path=_relative(root,binding.get("relative_path"))
        if sha256_file(evidence_path)!=str(binding.get("file_sha256") or ""): raise ValueError(f"prospective {key} file drift")
        evidence=_read_self_hashed(evidence_path,field,key)
        if evidence.get(field)!=expected or binding.get("payload_sha256")!=expected or evidence.get("status")!=status: raise ValueError(f"prospective {key} semantic drift")
    return payload

def main(argv:Sequence[str]|None=None)->int:
    admission=consume_active_admission("cn-program-optimizer-d1-transfer-prospective-validation-v1",{ACTION_LAUNCH})
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--campaign-authorization",type=Path,required=True); p.add_argument("--source-contract",type=Path,required=True); p.add_argument("--registry",type=Path,required=True); p.add_argument("--output-root",type=Path,required=True); p.add_argument("--workers",type=int,default=8); args=p.parse_args(argv)
    verify_consumed_admission_target(admission,output_root=args.output_root); verified=verify_campaign_authorization_binding(admission,args.campaign_authorization)
    repo_root=Path(__file__).resolve().parents[3]; authorization=verify_authorization(verified.path,repo_root=repo_root)
    if dict(verified.payload)!=authorization: raise ProjectControlDenied("prospective validation campaign authorization payload drift")
    if str(admission.get("requested_action") or "")!=ACTION_LAUNCH: raise ProjectControlDenied("prospective validation requested action drift")
    if int(args.workers)!=8: raise ProjectControlDenied("prospective validation requires VALIDATION_DUAL_8 workers")
    preflight_path=Path(str(authorization["zero_read_preflight"]["path"])).resolve()
    preflight=_read_self_hashed(preflight_path,"zero_read_preflight_payload_sha256","prospective validation zero-read preflight")
    if str(preflight.get("implementation_repo_sha") or "")!=str(admission.get("repo_sha") or ""): raise ProjectControlDenied("prospective validation execution SHA/preflight drift")
    source=dict(authorization.get("source_binding") or {})
    if str(args.source_contract.resolve())!=str(Path(str(source.get("source_contract_path") or "")).resolve()) or sha256_file(args.source_contract.resolve())!=str(source.get("source_contract_sha256") or ""): raise ProjectControlDenied("prospective validation source contract drift")
    if str(args.registry.resolve())!=str(Path(str(source.get("registry_path") or "")).resolve()) or sha256_file(args.registry.resolve())!=str(source.get("registry_sha256") or ""): raise ProjectControlDenied("prospective validation registry drift")
    from scripts.prepare_cn_program_optimizer_d1_transfer_prospective_validation_v1 import materialize_authorized
    from scripts.run_cn_program_optimizer_d1_transfer_prospective_validation_v1 import run
    filter_path=_relative(repo_root,authorization["transfer_filter"]["relative_path"])
    prepared_path=materialize_authorized(repo_root=repo_root,preflight_binding=preflight_path,output_root=args.output_root.resolve()/"prefinancial")
    result=run(argparse.Namespace(repo_root=repo_root,prepared_binding=prepared_path,transfer_filter=filter_path,source_contract=args.source_contract,registry=args.registry,output_root=args.output_root,workers=args.workers))
    print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
