"""Authorize Wave1 validation infrastructure preparation after shortlist freeze."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Any, Mapping
from our_system_phase2.services.unified_capability_registry import stable_hash

FREEZE=Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_shortlist_freeze_20260825.json")
MEMBERS=Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_shortlist_members_20260825.jsonl")
RESOLUTION=Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_schedule_resolution_20260825.json")
SCHEDULES=Path("runtime/run_plans/cn_search_core_v2_production_wave1_validation_resolved_schedules_20260825.jsonl")
SOURCE_PLAN=Path("runtime/run_plans/cn_program_base_event_tier_a_report_only_validation_plan.json")
STATUS="SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_PREP_AUTHORIZED_NOT_RUN"

def _read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding="utf-8-sig"))
def _sha(p:Path)->str: return hashlib.sha256(p.read_bytes()).hexdigest()
def _source_sha(p:Path)->str: return hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
def _verify(x:Mapping[str,Any],field:str,label:str)->str:
 b=dict(x); c=str(b.pop(field,""));
 if not c or stable_hash(b)!=c: raise RuntimeError(f"{label} self-hash drift")
 return c

def build(repo:Path)->dict[str,Any]:
 repo=repo.resolve(); freeze=_read(repo/FREEZE); fh=_verify(freeze,"freeze_payload_sha256","shortlist freeze")
 resolution=_read(repo/RESOLUTION); rh=_verify(resolution,"resolution_payload_sha256","schedule resolution")
 plan=_read(repo/SOURCE_PLAN); ph=_verify(plan,"plan_payload_sha256","validation source plan")
 if freeze.get("status")!="SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_SHORTLIST_FROZEN_BEFORE_VALIDATION" or int(freeze["candidate_count"])!=42 or bool(freeze["validation_read"]): raise RuntimeError("shortlist freeze authority drift")
 if resolution.get("status")!="SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_SCHEDULES_RESOLVED_BEFORE_VALIDATION" or int(resolution["candidate_count"])!=42 or int(resolution["required_physical_leaf_count"])!=47 or bool(resolution["validation_read"]): raise RuntimeError("schedule resolution authority drift")
 if _sha(repo/SCHEDULES)!=str(resolution["resolved_schedule_file_sha256"]): raise RuntimeError("resolved schedules file drift")
 source=dict(plan["source_data"])
 impl_names=(
  "prepare_cn_search_core_v2_production_wave1_validation_v1.py",
  "build_cn_core_pack_validation_session_sidecar.py",
  "build_cn_validation_session_authority.py",
  "verify_cn_validation_session_authority.py",
  "run_cn_portfolio_decoder_v2_oos.py",
 )
 payload={
  "schema_version":"cn_search_core_v2_production_wave1_validation_prep_authorization_v1",
  "status":STATUS,
  "authorized_host":"DESKTOP-77OPJ6F",
  "evaluation_role":"validation",
  "usage":"VALIDATION_INFRASTRUCTURE_PREP_ONLY",
  "shortlist_freeze":{"relative_path":FREEZE.as_posix(),"file_sha256":_sha(repo/FREEZE),"payload_sha256":fh,"candidate_count":42,"exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"members_relative_path":MEMBERS.as_posix(),"members_file_sha256":_sha(repo/MEMBERS)},
  "schedule_resolution":{"relative_path":RESOLUTION.as_posix(),"file_sha256":_sha(repo/RESOLUTION),"payload_sha256":rh,"schedules_relative_path":SCHEDULES.as_posix(),"schedules_file_sha256":_sha(repo/SCHEDULES),"required_physical_leaf_count":47,"required_physical_leaf_ids_sha256":resolution["required_physical_leaf_ids_sha256"]},
  "source_validation_authority_plan":{"relative_path":SOURCE_PLAN.as_posix(),"file_sha256":_sha(repo/SOURCE_PLAN),"payload_sha256":ph,"source_data":source},
  "implementation_canonical_lf_sha256":{n:_source_sha(repo/"scripts"/n) for n in impl_names},
  "validation_windows":list(plan["validation_windows"]),
  "validation_context_materialization_authorized":True,
  "candidate_evaluation_authorized":False,
  "candidate_generation_authorized":False,
  "same_slice_reselection_allowed":False,
  "threshold_tuning_allowed":False,
  "optimizer_feedback_write":"FORBIDDEN",
  "policy_memory_write":"FORBIDDEN",
  "scheduler_write":"FORBIDDEN",
  "promotion":"FORBIDDEN",
  "holdout_read_authorized":False,
  "forward_read_authorized":False,
  "oos_authority":"VALIDATION_PREP_CONTEXT_ONLY",
 }
 payload["authorization_payload_sha256"]=stable_hash(payload); return payload

def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__);p.add_argument("--repo-root",type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument("--output",type=Path,required=True);a=p.parse_args(argv);x=build(a.repo_root);o=a.output if a.output.is_absolute() else a.repo_root.resolve()/a.output;o.parent.mkdir(parents=True,exist_ok=True);o.write_text(json.dumps(x,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8");print(json.dumps({"status":x["status"],"payload":x["authorization_payload_sha256"],"candidates":42,"fields":47,"output":str(o.resolve())},sort_keys=True));return 0
if __name__=="__main__": raise SystemExit(main())