"""Prepare immutable validation infrastructure for the frozen Wave1 shortlist.

Candidate membership and executable schedules are already frozen. This step may
materialize validation fields and load the validation context, but never evaluates
a candidate or writes optimizer/policy feedback.
"""
from __future__ import annotations
import argparse, csv, hashlib, json, shutil, subprocess, sys
from pathlib import Path
from typing import Any, Mapping
PROJECT_ROOT=Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0,str(PROJECT_ROOT))
from scripts import run_cn_portfolio_decoder_v2_oos as oos
from our_system_phase2.services.unified_capability_registry import stable_hash
STATUS="SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_PREFINANCIAL_READY"

def _read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding="utf-8-sig"))
def _rows(p:Path)->list[dict[str,Any]]: return [json.loads(x) for x in p.read_text(encoding="utf-8-sig").splitlines() if x.strip()]
def _sha(p:Path)->str: return hashlib.sha256(p.read_bytes()).hexdigest()
def _source_sha(p:Path)->str: return hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
def _verify(x:Mapping[str,Any],field:str,label:str)->str:
 b=dict(x);c=str(b.pop(field,""));
 if not c or stable_hash(b)!=c: raise RuntimeError(f"{label} self-hash drift")
 return c
def _run(cmd:list[str])->None: subprocess.run(cmd,check=True)
def _requirements(path:Path,fields:list[str])->None:
 with path.open("w",encoding="utf-8-sig",newline="") as h:
  w=csv.DictWriter(h,fieldnames=["candidate_id","expression"]);w.writeheader()
  for f in fields:w.writerow({"candidate_id":f"required::{f}","expression":f"${f}"})
def _source_path(source:dict[str,Any],key:str)->Path:
 v=source[key]; return Path(str(v["path"] if isinstance(v,dict) and "path" in v else v)).resolve()
def _verify_source_file(source:dict[str,Any],key:str)->Path:
 p=_source_path(source,key); expected=str(source[key]["sha256"]); 
 if not p.is_file() or _sha(p)!=expected: raise RuntimeError(f"validation source file drift: {key}")
 return p

def prepare(args:argparse.Namespace)->dict[str,Any]:
 repo=args.repo_root.resolve(); out=args.output_root.resolve()
 if out.exists(): raise FileExistsError(out)
 auth=_read(args.authorization.resolve()); ah=_verify(auth,"authorization_payload_sha256","Wave1 validation prep authorization")
 if auth.get("status")!="SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_PREP_AUTHORIZED_NOT_RUN" or auth.get("authorized_host")!="DESKTOP-77OPJ6F" or not bool(auth.get("validation_context_materialization_authorized")) or bool(auth.get("candidate_evaluation_authorized")): raise RuntimeError("Wave1 validation prep authorization drift")
 if str(args.repo_sha).lower()!=str(args.repo_sha) or len(str(args.repo_sha))!=40: raise RuntimeError("repo SHA format drift")
 for n,expected in dict(auth["implementation_canonical_lf_sha256"]).items():
  p=repo/"scripts"/n
  if not p.is_file() or _source_sha(p)!=str(expected): raise RuntimeError(f"validation prep implementation drift: {n}")
 freeze_path=repo/Path(auth["shortlist_freeze"]["relative_path"]); members_path=repo/Path(auth["shortlist_freeze"]["members_relative_path"]); resolution_path=repo/Path(auth["schedule_resolution"]["relative_path"]); schedules_path=repo/Path(auth["schedule_resolution"]["schedules_relative_path"])
 freeze=_read(freeze_path); fh=_verify(freeze,"freeze_payload_sha256","shortlist freeze"); resolution=_read(resolution_path); rh=_verify(resolution,"resolution_payload_sha256","schedule resolution")
 if _sha(freeze_path)!=auth["shortlist_freeze"]["file_sha256"] or fh!=auth["shortlist_freeze"]["payload_sha256"] or _sha(members_path)!=auth["shortlist_freeze"]["members_file_sha256"] or _sha(resolution_path)!=auth["schedule_resolution"]["file_sha256"] or rh!=auth["schedule_resolution"]["payload_sha256"] or _sha(schedules_path)!=auth["schedule_resolution"]["schedules_file_sha256"]: raise RuntimeError("Wave1 validation frozen input drift")
 schedules=_rows(schedules_path); fields=sorted({str(f) for row in schedules for k in ("primary_compiled","control_compiled") for f in dict(row[k]).get("physical_leaf_ids") or ()})
 if len(schedules)!=42 or len(fields)!=47 or stable_hash(fields)!=auth["schedule_resolution"]["required_physical_leaf_ids_sha256"]: raise RuntimeError("Wave1 validation schedule/field surface drift")
 source=dict(auth["source_validation_authority_plan"]["source_data"])
 registry=_verify_source_file(source,"registry"); split=_verify_source_file(source,"split_manifest"); public_manifest=_verify_source_file(source,"public_source_manifest"); daily_st=_verify_source_file(source,"daily_st_source"); contract=_verify_source_file(source,"source_contract"); validation_label_manifest=_verify_source_file(source,"validation_label_manifest"); chip_manifest=_verify_source_file(source,"chip_manifest")
 minute=Path(str(source["minute_source_root"])).resolve(); fundamental=Path(str(source["fundamental_root"])).resolve(); chip=Path(str(source["chip_root"])).resolve(); public_root=Path(str(source["public_source_root"])).resolve(); labels=Path(str(source["validation_label_root"])).resolve()
 for p in (minute,fundamental,chip,public_root,labels):
  if not p.exists(): raise FileNotFoundError(p)
 out.mkdir(parents=True); copied=out/"resolved_program_schedules.jsonl"; shutil.copyfile(schedules_path,copied); req=out/"validation_program_required_fields.csv"; _requirements(req,fields)
 field_root=out/"program_validation_session_fields"
 _run([sys.executable,str(repo/"scripts/build_cn_core_pack_validation_session_sidecar.py"),"--source-root",str(minute),"--evaluation-role","validation","--output-root",str(field_root),"--candidate-table",str(req),"--registry",str(registry),"--split-manifest",str(split),"--split-manifest-hash",str(source["split_manifest"]["sha256"]),"--fundamental-root",str(fundamental),"--chip-root",str(chip),"--max-shards","16"])
 fm=field_root/"CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"; fmsha=_sha(fm)
 authority=out/"validation_session_authority"
 _run([sys.executable,str(repo/"scripts/build_cn_validation_session_authority.py"),"--field-manifest",str(fm),"--public-source-root",str(public_root),"--output-root",str(authority),"--expected-field-manifest-sha256",fmsha,"--expected-source-manifest-sha256",str(source["public_source_manifest"]["sha256"]),"--historical-daily-st-source",str(daily_st),"--expected-daily-st-source-sha256",str(source["daily_st_source"]["sha256"]),"--builder-commit-sha",str(args.repo_sha),"--evaluation-role","validation"])
 audit_root=out/"validation_session_authority_audit"; _run([sys.executable,str(repo/"scripts/verify_cn_validation_session_authority.py"),"--authority-root",str(authority),"--output-root",str(audit_root)])
 authority_audit=_read(audit_root/"audit.json")
 if authority_audit.get("status")!="PASS_INDEPENDENT_VALIDATION_SESSION_AUTHORITY_VERIFICATION": raise RuntimeError("Wave1 validation session authority audit failed")
 context=oos._load_validation_context(source_contract_path=contract,validation_field_root=field_root,validation_label_root=labels,validation_session_authority_root=authority)
 missing=sorted(set(fields)-set(context["field_frame"].columns))
 if missing or not len(set(context["dates"])): raise RuntimeError(f"Wave1 validation prepared context drift: missing={missing}")
 payload={"schema_version":"cn_search_core_v2_production_wave1_validation_prepared_binding_v1","status":STATUS,"repo_sha":str(args.repo_sha),"prep_authorization_file_sha256":_sha(args.authorization.resolve()),"prep_authorization_payload_sha256":ah,"shortlist_freeze_payload_sha256":fh,"schedule_resolution_payload_sha256":rh,"candidate_count":42,"candidate_exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"resolved_schedule_path":str(copied),"resolved_schedule_file_sha256":_sha(copied),"required_physical_leaf_count":len(fields),"required_physical_leaf_ids":fields,"required_physical_leaf_ids_sha256":stable_hash(fields),"validation_field_root":str(field_root),"validation_field_manifest_sha256":fmsha,"validation_label_root":str(labels),"validation_label_manifest_sha256":_sha(validation_label_manifest),"validation_session_authority_root":str(authority),"validation_session_authority_manifest_sha256":_sha(authority/"validation_session_authority_manifest.json"),"validation_session_authority_audit_sha256":_sha(audit_root/"audit.json"),"validation_windows":list(auth["validation_windows"]),"validation_reads_during_context_smoke":int(context["validation_reads"]),"validation_access_started":True,"candidate_evaluation_executed":False,"candidate_results_generated":False,"optimizer_feedback_write":"FORBIDDEN","policy_memory_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","promotion":"FORBIDDEN","holdout_reads":0,"forward_reads":0,"oos_authority":"VALIDATION_PREP_CONTEXT_ONLY"}; payload["prepared_binding_payload_sha256"]=stable_hash(payload); p=out/"WAVE1_VALIDATION_PREFINANCIAL_READY.json";p.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8");return payload

def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__);p.add_argument("--repo-root",type=Path,required=True);p.add_argument("--repo-sha",required=True);p.add_argument("--authorization",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args(argv);x=prepare(a);print(json.dumps({"status":x["status"],"payload":x["prepared_binding_payload_sha256"],"candidates":x["candidate_count"],"fields":x["required_physical_leaf_count"],"validation_reads":x["validation_reads_during_context_smoke"],"candidate_evaluation_executed":x["candidate_evaluation_executed"]},sort_keys=True));return 0
if __name__=="__main__": raise SystemExit(main())