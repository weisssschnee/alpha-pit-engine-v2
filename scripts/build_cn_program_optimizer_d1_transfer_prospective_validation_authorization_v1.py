"""Build the byte-bound authorization for prospective D1 transfer validation."""
from __future__ import annotations

import argparse, hashlib, json, sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT=Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT/"src") not in sys.path: sys.path.insert(0,str(PROJECT_ROOT/"src"))
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH,ACTION_RETRY
from our_system_phase2.services.unified_capability_registry import stable_hash
from our_system_phase2.runtime.cn_program_optimizer_d1_transfer_prospective_validation_v1 import CAMPAIGN_ID,CAMPAIGN_PROFILE,ROUTE_ID,VALIDATION_WINDOWS

FILTER_RELATIVE_PATH=Path("runtime/run_plans/cn_program_optimizer_d1_transfer_filter_v1_20260817.json")

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

def build(*,repo_root:Path,freeze_path:Path,prepared_path:Path,source_contract:Path,registry:Path,output:Path)->dict[str,Any]:
 root=repo_root.resolve(); freeze=_read(freeze_path); _verify(freeze,"freeze_payload_sha256","prospective candidate freeze")
 prepared=_read(prepared_path); _verify(prepared,"prepared_binding_payload_sha256","prospective prepared binding")
 filter_path=(root/FILTER_RELATIVE_PATH).resolve(); flt=_read(filter_path); _verify(flt,"filter_payload_sha256","transfer filter")
 if freeze.get("status")!="FROZEN_BEFORE_VALIDATION_ACCESS" or prepared.get("status")!="D1_TRANSFER_PROSPECTIVE_VALIDATION_PREFINANCIAL_READY" or flt.get("status")!="FROZEN_BEFORE_NEXT_DEVELOPMENT_COHORT_VALIDATION": raise RuntimeError("prospective validation input status drift")
 if int(freeze["candidate_count"])!=int(prepared["candidate_count"]) or freeze["candidate_exact_identities_sha256"]!=prepared["candidate_exact_identities_sha256"] or int(freeze["transfer_filter_selected_count"])!=int(prepared["transfer_filter_selected_count"]): raise RuntimeError("prospective freeze/prepared candidate drift")
 if freeze["transfer_filter_id"]!=flt["filter_id"] or prepared["transfer_filter_id"]!=flt["filter_id"]: raise RuntimeError("prospective filter identity drift")
 members_path=Path(str(prepared["candidate_members_path"])).resolve()
 if _sha256(members_path)!=str(freeze["candidate_members_file_sha256"]): raise RuntimeError("prospective members drift")
 payload={
  "schema_version":"cn_program_optimizer_d1_transfer_prospective_validation_authorization_v1","status":"D1_TRANSFER_PROSPECTIVE_VALIDATION_FROZEN_READY",
  "campaign_id":CAMPAIGN_ID,"campaign_profile":CAMPAIGN_PROFILE,"project_control_route_id":ROUTE_ID,"execution_authorized":True,"permitted_project_control_actions":[ACTION_LAUNCH,ACTION_RETRY],
  "candidate_freeze":{"relative_path":_rel(root,freeze_path),"file_sha256":_sha256(freeze_path),"payload_sha256":freeze["freeze_payload_sha256"],"members_relative_path":_rel(root,members_path),"members_file_sha256":_sha256(members_path),"candidate_count":int(freeze["candidate_count"]),"candidate_exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"transfer_filter_selected_count":int(freeze["transfer_filter_selected_count"]),"transfer_filter_selected_exact_identities_sha256":freeze["transfer_filter_selected_exact_identities_sha256"]},
  "transfer_filter":{"relative_path":FILTER_RELATIVE_PATH.as_posix(),"file_sha256":_sha256(filter_path),"payload_sha256":flt["filter_payload_sha256"],"filter_id":flt["filter_id"],"acceptance_contract":flt["prospective_B_validation_acceptance"]},
  "prepared_binding":{"relative_path":_rel(root,prepared_path),"file_sha256":_sha256(prepared_path),"payload_sha256":prepared["prepared_binding_payload_sha256"],"required_physical_leaf_count":int(prepared["required_physical_leaf_count"]),"validation_field_manifest_sha256":prepared["validation_field_manifest_sha256"],"validation_session_authority_manifest_sha256":prepared["validation_session_authority_manifest_sha256"],"validation_session_authority_audit_sha256":prepared["validation_session_authority_audit_sha256"]},
  "source_binding":{"source_contract_path":str(source_contract.resolve()),"source_contract_sha256":_sha256(source_contract.resolve()),"registry_path":str(registry.resolve()),"registry_sha256":_sha256(registry.resolve())},
  "evaluation_role":"validation","usage":"REPORT_ONLY_PROSPECTIVE_TRANSFER_TEST","validation_windows":list(VALIDATION_WINDOWS),"optimizer_feedback_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","archive_write":"FORBIDDEN","holdout_reads":0,"forward_2026_reads":0,"holdout_authority":"NONE","promotion_authorized":False,"automatic_successor_authorized":False,
 }
 payload["authorization_payload_sha256"]=stable_hash(payload); output.parent.mkdir(parents=True,exist_ok=True); output.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8"); return payload

def main(argv:Sequence[str]|None=None)->int:
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--repo-root",type=Path,required=True); p.add_argument("--freeze",type=Path,required=True); p.add_argument("--prepared",type=Path,required=True); p.add_argument("--source-contract",type=Path,required=True); p.add_argument("--registry",type=Path,required=True); p.add_argument("--output",type=Path,required=True); a=p.parse_args(argv)
 result=build(repo_root=a.repo_root,freeze_path=a.freeze,prepared_path=a.prepared,source_contract=a.source_contract,registry=a.registry,output=a.output); print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
