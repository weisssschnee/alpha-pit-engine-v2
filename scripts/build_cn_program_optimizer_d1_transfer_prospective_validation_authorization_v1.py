"""Build the byte-bound authorization for prospective D1 transfer validation."""
from __future__ import annotations

import argparse, hashlib, json, sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT=Path(__file__).resolve().parents[1]
for path in (PROJECT_ROOT,PROJECT_ROOT/"src"):
 if str(path) not in sys.path: sys.path.insert(0,str(path))
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH
from our_system_phase2.services.unified_capability_registry import stable_hash
from our_system_phase2.runtime.cn_program_optimizer_d1_transfer_prospective_validation_v1 import CAMPAIGN_ID,CAMPAIGN_PROFILE,ROUTE_ID,VALIDATION_WINDOWS
from scripts.prepare_cn_program_optimizer_d1_transfer_prospective_validation_v1 import ZERO_READ_STATUS

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

def _acceptance_contract(flt:Mapping[str,Any])->dict[str,Any]:
 if "prospective_B_validation_acceptance" in flt: raise RuntimeError("stale B acceptance contract is not valid for C validation")
 contract=dict(flt.get("prospective_C_validation_acceptance") or {})
 if contract!=EXPECTED_ACCEPTANCE_CONTRACT: raise RuntimeError("prospective C transfer acceptance contract drift")
 return contract

def _sha256(path:Path)->str:
 h=hashlib.sha256()
 with path.open("rb") as f:
  for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
 return h.hexdigest()
def _read(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding="utf-8-sig"))
def _verify(payload:Mapping[str,Any],field:str,label:str)->None:
 body=dict(payload); c=str(body.pop(field,""))
 if not c or stable_hash(body)!=c: raise RuntimeError(f"{label} self-hash drift")
def _rel(root:Path,path:Path)->str:
 p=path.resolve(); r=root.resolve()
 if not p.is_relative_to(r): raise RuntimeError(f"authorization input outside repo: {p}")
 return p.relative_to(r).as_posix()

def build(*,repo_root:Path,freeze_path:Path,preflight_path:Path,source_contract:Path,registry:Path,output:Path,transfer_filter_path:Path,development_outcome_path:Path,reproducibility_audit_path:Path)->dict[str,Any]:
 root=repo_root.resolve(); freeze=_read(freeze_path); _verify(freeze,"freeze_payload_sha256","prospective candidate freeze")
 preflight=_read(preflight_path); _verify(preflight,"zero_read_preflight_payload_sha256","prospective zero-read preflight")
 filter_path=transfer_filter_path.resolve(); flt=_read(filter_path); _verify(flt,"filter_payload_sha256","transfer filter")
 outcome=_read(development_outcome_path); _verify(outcome,"outcome_payload_sha256","C development outcome")
 repro=_read(reproducibility_audit_path); _verify(repro,"audit_payload_sha256","V2 reproducibility audit")
 if freeze.get("status")!="FROZEN_BEFORE_VALIDATION_ACCESS" or preflight.get("status")!=ZERO_READ_STATUS or flt.get("status")!="FROZEN_BEFORE_C_DEVELOPMENT_AND_VALIDATION": raise RuntimeError("prospective validation input status drift")
 if freeze.get("source_cohort")!=EXPECTED_SOURCE_COHORT or preflight.get("source_cohort")!=EXPECTED_SOURCE_COHORT: raise RuntimeError("prospective C cohort identity drift")
 if int(freeze["candidate_count"])!=EXPECTED_CANDIDATE_COUNT or int(preflight["candidate_count"])!=EXPECTED_CANDIDATE_COUNT or freeze["candidate_exact_identities_sha256"]!=EXPECTED_CANDIDATE_SET_SHA256 or preflight["candidate_exact_identities_sha256"]!=EXPECTED_CANDIDATE_SET_SHA256: raise RuntimeError("prospective C candidate set drift")
 if int(freeze["transfer_filter_selected_count"])!=EXPECTED_SELECTED_COUNT or int(preflight["transfer_filter_selected_count"])!=EXPECTED_SELECTED_COUNT or freeze["transfer_filter_selected_exact_identities_sha256"]!=EXPECTED_SELECTED_SET_SHA256 or preflight["transfer_filter_selected_exact_identities_sha256"]!=EXPECTED_SELECTED_SET_SHA256: raise RuntimeError("prospective C selected set drift")
 if freeze["freeze_payload_sha256"]!=EXPECTED_FREEZE_PAYLOAD_SHA256 or preflight["candidate_freeze_payload_sha256"]!=EXPECTED_FREEZE_PAYLOAD_SHA256: raise RuntimeError("prospective C freeze payload drift")
 if int(preflight.get("validation_reads_by_this_preflight") or -1)!=0 or bool(preflight.get("candidate_evaluation_executed")): raise RuntimeError("prospective preflight crossed validation boundary")
 if flt.get("filter_payload_sha256")!=EXPECTED_FILTER_PAYLOAD_SHA256 or freeze["transfer_filter_id"]!=flt["filter_id"] or preflight["transfer_filter_id"]!=flt["filter_id"]: raise RuntimeError("prospective filter identity drift")
 if outcome.get("outcome_payload_sha256")!=EXPECTED_OUTCOME_PAYLOAD_SHA256 or outcome.get("status")!="C_DEVELOPMENT_COMPLETE_AND_V2_MEMBERSHIP_FROZEN_BEFORE_VALIDATION" or bool(outcome.get("validation_accessed")) or int(outcome.get("candidate_count") or 0)!=140 or int(outcome.get("prior_overlap_count") or -1)!=0 or int(outcome.get("development_admitted_count") or 0)!=93 or int(outcome.get("development_productive_count") or 0)!=67 or int(outcome.get("transfer_filter_candidate_count") or 0)!=67 or int(outcome.get("transfer_filter_selected_count") or 0)!=27 or outcome.get("transfer_filter_selected_exact_identities_sha256")!=EXPECTED_SELECTED_SET_SHA256: raise RuntimeError("prospective C outcome drift")
 if repro.get("audit_payload_sha256")!=EXPECTED_REPRO_AUDIT_PAYLOAD_SHA256 or repro.get("status")!="PASS_V2_REPRODUCIBILITY_RESTORED_FROM_DURABLE_EVIDENCE" or int(repro.get("holdout_reads") or 0)!=0 or int(repro.get("forward_2026_reads") or 0)!=0 or bool(repro.get("C_validation_execution_authorized_by_this_audit")): raise RuntimeError("prospective V2 reproducibility drift")
 members_path=(root/str(preflight["candidate_members_relative_path"])).resolve()
 members=[json.loads(line) for line in members_path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
 if stable_hash(members)!=str(freeze["candidate_members_payload_sha256"]): raise RuntimeError("prospective members semantic drift")
 if str(preflight.get("candidate_members_payload_sha256") or "")!=str(freeze["candidate_members_payload_sha256"]): raise RuntimeError("prospective preflight members semantic drift")
 payload={
  "schema_version":"cn_program_optimizer_d1_transfer_prospective_validation_authorization_v2","status":"D1_TRANSFER_FILTER_V2_C_PROSPECTIVE_VALIDATION_FROZEN_READY",
  "campaign_id":CAMPAIGN_ID,"campaign_profile":CAMPAIGN_PROFILE,"project_control_route_id":ROUTE_ID,"source_cohort":EXPECTED_SOURCE_COHORT,"execution_authorized":True,"permitted_project_control_actions":[ACTION_LAUNCH],
  "candidate_freeze":{"relative_path":_rel(root,freeze_path),"file_sha256":_sha256(freeze_path),"payload_sha256":freeze["freeze_payload_sha256"],"members_relative_path":_rel(root,members_path),"members_payload_sha256":freeze["candidate_members_payload_sha256"],"candidate_count":int(freeze["candidate_count"]),"candidate_exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"transfer_filter_selected_count":int(freeze["transfer_filter_selected_count"]),"transfer_filter_selected_exact_identities_sha256":freeze["transfer_filter_selected_exact_identities_sha256"]},
  "transfer_filter":{"relative_path":_rel(root,filter_path),"file_sha256":_sha256(filter_path),"payload_sha256":flt["filter_payload_sha256"],"filter_id":flt["filter_id"],"acceptance_contract":_acceptance_contract(flt)},
  "zero_read_preflight":{"path":str(preflight_path.resolve()),"file_sha256":_sha256(preflight_path),"payload_sha256":preflight["zero_read_preflight_payload_sha256"],"required_physical_leaf_count":int(preflight["required_physical_leaf_count"]),"missing_required_physical_leaf_count":int(preflight["missing_required_physical_leaf_count"]),"base_validation_field_manifest_sha256":preflight["base_validation_field_manifest_sha256"],"validation_label_manifest_sha256":preflight["validation_label_manifest_sha256"],"source_validation_session_authority_manifest_sha256":preflight["source_validation_session_authority_manifest_sha256"],"source_validation_session_authority_audit_sha256":preflight["source_validation_session_authority_audit_sha256"],"validation_reads":0},
  "development_outcome":{"relative_path":_rel(root,development_outcome_path),"file_sha256":_sha256(development_outcome_path),"payload_sha256":outcome["outcome_payload_sha256"]},
  "reproducibility_audit":{"relative_path":_rel(root,reproducibility_audit_path),"file_sha256":_sha256(reproducibility_audit_path),"payload_sha256":repro["audit_payload_sha256"]},
  "source_binding":{"source_contract_path":str(source_contract.resolve()),"source_contract_sha256":_sha256(source_contract.resolve()),"registry_path":str(registry.resolve()),"registry_sha256":_sha256(registry.resolve())},
  "evaluation_role":"validation","usage":"REPORT_ONLY_PROSPECTIVE_TRANSFER_FILTER_V2_C_TEST","validation_windows":list(VALIDATION_WINDOWS),"optimizer_feedback_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","archive_write":"FORBIDDEN","search_memory_write":"FORBIDDEN","validation_reads_before_admission":0,"holdout_reads":0,"historical_challenge_reads":0,"forward_b_reads":0,"forward_2026_reads":0,"holdout_authority":"NONE","promotion_authorized":False,"automatic_successor_authorized":False,
 }
 payload["authorization_payload_sha256"]=stable_hash(payload); output.parent.mkdir(parents=True,exist_ok=True); output.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8"); return payload

def main(argv:Sequence[str]|None=None)->int:
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--repo-root",type=Path,required=True); p.add_argument("--freeze",type=Path,required=True); p.add_argument("--zero-read-preflight",type=Path,required=True); p.add_argument("--transfer-filter",type=Path,required=True); p.add_argument("--development-outcome",type=Path,required=True); p.add_argument("--reproducibility-audit",type=Path,required=True); p.add_argument("--source-contract",type=Path,required=True); p.add_argument("--registry",type=Path,required=True); p.add_argument("--output",type=Path,required=True); a=p.parse_args(argv)
 result=build(repo_root=a.repo_root,freeze_path=a.freeze,preflight_path=a.zero_read_preflight,source_contract=a.source_contract,registry=a.registry,output=a.output,transfer_filter_path=a.transfer_filter,development_outcome_path=a.development_outcome,reproducibility_audit_path=a.reproducibility_audit); print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
