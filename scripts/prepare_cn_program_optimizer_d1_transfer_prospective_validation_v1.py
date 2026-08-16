"""Prepare a frozen fresh-D1 cohort for prospective transfer-filter validation.

The candidate/filter membership freeze must exist before this command starts.
This command resolves immutable development schedules, builds the exact
Program-complete validation field sidecar, rebuilds the validation session
authority, independently verifies it, and smoke-loads the replay context.
It never evaluates a candidate and never writes optimizer feedback.
"""
from __future__ import annotations

import argparse, csv, hashlib, json, subprocess, sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for p in (PROJECT_ROOT, PROJECT_ROOT / "src"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from scripts import run_cn_portfolio_decoder_v2_oos as oos
from our_system_phase2.services.unified_capability_registry import stable_hash

EXPECTED_SPLIT_SHA256 = "fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241"
EXPECTED_PUBLIC_SOURCE_MANIFEST_SHA256 = "0c06baa7c24109ea8a1064a291565ce6352ab2a0ca32f20f6cc257261e25514d"
EXPECTED_DAILY_ST_SOURCE_SHA256 = "7060dd78cde6b826157f10c03541a4c302d11fe3213517236dc893393c071e68"
VALIDATION_WINDOWS = (
    {"window_id": "validation_1", "start_date": "2025-07-08", "end_date": "2025-08-08", "session_count": 24},
    {"window_id": "validation_2", "start_date": "2025-08-11", "end_date": "2025-09-11", "session_count": 24},
    {"window_id": "validation_3", "start_date": "2025-09-12", "end_date": "2025-10-24", "session_count": 25},
)


def _sha256(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda:f.read(4*1024*1024),b""): h.update(b)
    return h.hexdigest()

def _read_json(path: Path)->dict[str,Any]: return json.loads(path.read_text(encoding="utf-8-sig"))
def _read_jsonl(path: Path)->list[dict[str,Any]]: return [json.loads(x) for x in path.read_text(encoding="utf-8-sig").splitlines() if x.strip()]
def _write_json(path: Path,payload: Mapping[str,Any])->Path:
    path.parent.mkdir(parents=True,exist_ok=True); path.write_text(json.dumps(dict(payload),ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8"); return path
def _write_jsonl(path: Path,rows: Sequence[Mapping[str,Any]])->Path:
    path.parent.mkdir(parents=True,exist_ok=True); path.write_text("".join(json.dumps(dict(x),ensure_ascii=False,sort_keys=True)+"\n" for x in rows),encoding="utf-8"); return path

def _verify_self_hash(payload: Mapping[str,Any],field:str,label:str)->None:
    body=dict(payload); claimed=str(body.pop(field,""))
    if not claimed or stable_hash(body)!=claimed: raise RuntimeError(f"{label} self-hash drift")

def _load_freeze(freeze_root: Path)->tuple[dict[str,Any],list[dict[str,Any]]]:
    freeze_path=freeze_root/"validation_candidate_freeze.json"; members_path=freeze_root/"validation_candidate_members.jsonl"
    freeze=_read_json(freeze_path); _verify_self_hash(freeze,"freeze_payload_sha256","prospective validation freeze")
    if freeze.get("status")!="FROZEN_BEFORE_VALIDATION_ACCESS": raise RuntimeError("prospective validation freeze not ready")
    members=_read_jsonl(members_path); ids=sorted(str(x["exact_identity"]) for x in members)
    n=int(freeze["candidate_count"])
    if n < int(freeze.get("transfer_filter_selected_count") or 0) or n < 1: raise RuntimeError("prospective validation freeze count invalid")
    if len(members)!=n or len(set(ids))!=n or stable_hash(ids)!=str(freeze["candidate_exact_identities_sha256"]): raise RuntimeError("prospective validation member identity drift")
    if stable_hash(members)!=str(freeze["candidate_members_payload_sha256"]): raise RuntimeError("prospective validation members semantic drift")
    selected=sorted(str(x["exact_identity"]) for x in members if bool(x["transfer_filter_selected"]))
    if len(selected)!=int(freeze["transfer_filter_selected_count"]) or stable_hash(selected)!=str(freeze["transfer_filter_selected_exact_identities_sha256"]): raise RuntimeError("prospective validation filter membership drift")
    if freeze["validation_contract"] != {"archive_write":"FORBIDDEN","evaluation_role":"validation","forward_2026_reads":0,"holdout_reads":0,"optimizer_feedback_write":"FORBIDDEN","promotion":"FORBIDDEN","scheduler_write":"FORBIDDEN","usage":"REPORT_ONLY_PROSPECTIVE_TRANSFER_TEST"}: raise RuntimeError("prospective validation contract drift")
    return freeze,members

def _schedule_index(source_root: Path)->dict[tuple[str,str],list[dict[str,Any]]]:
    out={}
    for w in range(20):
        p=source_root/f"wave_{w:03d}"/"physical_schedules.jsonl"
        if not p.is_file(): raise FileNotFoundError(p)
        for row in _read_jsonl(p):
            exact=str(row.get("d1_exact_identity") or row.get("successor_exact_identity") or ""); sh=str(row.get("schedule_record_sha256") or "")
            if not exact or not sh: raise RuntimeError("prospective schedule lacks exact/hash")
            out.setdefault((exact,sh),[]).append(row)
    return out

def _resolve_schedules(members:Sequence[Mapping[str,Any]])->list[dict[str,Any]]:
    indexes={}; resolved=[]
    for member in members:
        root=Path(str(member["source_root"])).resolve(); key=str(root)
        wave=int(member["source_wave"]); manifest=root/f"wave_{wave:03d}"/"wave_manifest.json"
        if _sha256(manifest)!=str(member["source_wave_manifest_sha256"]): raise RuntimeError("prospective logical wave manifest drift")
        if key not in indexes: indexes[key]=_schedule_index(root)
        exact=str(member["exact_identity"]); sh=str(member["schedule_record_sha256"]); matches=indexes[key].get((exact,sh),[])
        if len(matches)!=1: raise RuntimeError(f"prospective physical schedule cardinality drift: {exact}")
        schedule=dict(matches[0]); body={k:v for k,v in schedule.items() if k!="schedule_record_sha256"}
        if stable_hash(body)!=sh: raise RuntimeError("prospective schedule self-hash drift")
        if str(schedule["pair_id"])!=str(member["pair_id"]) or str(schedule["primary_program"]["program_id"])!=str(member["program_id"]) or str(schedule["control_program"]["program_id"])!=str(member["control_program_id"]): raise RuntimeError("prospective schedule/member economic identity drift")
        resolved.append(schedule)
    return resolved

def _required_fields(schedules:Sequence[Mapping[str,Any]])->tuple[str,...]:
    fields=set()
    for row in schedules:
        for key in ("primary_compiled","control_compiled"): fields.update(map(str,dict(row[key]).get("physical_leaf_ids") or ()))
    return tuple(sorted(fields))
def _requirements_table(path:Path,fields:Sequence[str])->Path:
    with path.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=["candidate_id","expression"]); w.writeheader()
        for field in fields: w.writerow({"candidate_id":f"required::{field}","expression":f"${field}"})
    return path

def _run(cmd:Sequence[str])->None: subprocess.run(list(cmd),check=True)

def prepare(args:argparse.Namespace)->dict[str,Any]:
    repo=args.repo_root.resolve(); freeze_root=args.freeze_root.resolve(); out=args.output_root.resolve()
    if out.exists(): raise FileExistsError(out)
    out.mkdir(parents=True)
    freeze,members=_load_freeze(freeze_root); schedules=_resolve_schedules(members); required=_required_fields(schedules)
    if not required: raise RuntimeError("prospective validation has no required fields")
    schedules_path=_write_jsonl(out/"resolved_program_schedules.jsonl",schedules)
    req_path=_requirements_table(out/"validation_program_required_fields.csv",required)
    selection={"schema_version":"cn_program_optimizer_d1_transfer_prospective_selection_binding_v1","status":"FROZEN_BEFORE_VALIDATION_RESULT_ACCESS","candidate_freeze_payload_sha256":freeze["freeze_payload_sha256"],"candidate_exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"candidate_count":len(members),"transfer_filter_id":freeze["transfer_filter_id"],"transfer_filter_selected_count":freeze["transfer_filter_selected_count"],"transfer_filter_selected_exact_identities_sha256":freeze["transfer_filter_selected_exact_identities_sha256"],"resolved_schedule_file_sha256":_sha256(schedules_path),"required_physical_leaf_count":len(required),"required_physical_leaf_ids":list(required),"requirements_table_file_sha256":_sha256(req_path),"validation_windows":list(VALIDATION_WINDOWS),"validation_reads":0,"candidate_evaluation_executed":False}
    selection["binding_payload_sha256"]=stable_hash(selection); _write_json(out/"selection_binding.json",selection)
    field_root=out/"program_validation_session_fields"
    _run([sys.executable,str(repo/"scripts/build_cn_core_pack_validation_session_sidecar.py"),"--source-root",str(args.minute_source_root.resolve()),"--evaluation-role","validation","--output-root",str(field_root),"--candidate-table",str(req_path),"--registry",str(args.registry.resolve()),"--split-manifest",str(args.split_manifest.resolve()),"--split-manifest-hash",EXPECTED_SPLIT_SHA256,"--fundamental-root",str(args.fundamental_root.resolve()),"--chip-root",str(args.chip_root.resolve()),"--max-shards","16"])
    field_manifest=field_root/"CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"; field_sha=_sha256(field_manifest)
    authority=out/"validation_session_authority"
    _run([sys.executable,str(repo/"scripts/build_cn_validation_session_authority.py"),"--field-manifest",str(field_manifest),"--public-source-root",str(args.public_source_root.resolve()),"--output-root",str(authority),"--expected-field-manifest-sha256",field_sha,"--expected-source-manifest-sha256",EXPECTED_PUBLIC_SOURCE_MANIFEST_SHA256,"--historical-daily-st-source",str(args.daily_st_source.resolve()),"--expected-daily-st-source-sha256",EXPECTED_DAILY_ST_SOURCE_SHA256,"--builder-commit-sha",str(args.repo_sha),"--evaluation-role","validation"])
    audit=out/"validation_session_authority_audit"
    _run([sys.executable,str(repo/"scripts/verify_cn_validation_session_authority.py"),"--authority-root",str(authority),"--output-root",str(audit)])
    audit_payload=_read_json(audit/"audit.json")
    if audit_payload.get("status")!="PASS_INDEPENDENT_VALIDATION_SESSION_AUTHORITY_VERIFICATION": raise RuntimeError("prospective validation authority audit failed")
    context=oos._load_validation_context(source_contract_path=args.source_contract.resolve(),validation_field_root=field_root,validation_label_root=args.validation_label_root.resolve(),validation_session_authority_root=authority)
    missing=sorted(set(required)-set(context["field_frame"].columns))
    if missing: raise RuntimeError(f"prospective validation prepared context misses leaves: {missing}")
    unique_dates=len(set(context["dates"]))
    if unique_dates!=73: raise RuntimeError(f"prospective validation calendar drift: {unique_dates}")
    prepared={"schema_version":"cn_program_optimizer_d1_transfer_prospective_validation_prepared_binding_v1","status":"D1_TRANSFER_PROSPECTIVE_VALIDATION_PREFINANCIAL_READY","repo_sha":str(args.repo_sha),"candidate_freeze_path":str((freeze_root/"validation_candidate_freeze.json").resolve()),"candidate_freeze_file_sha256":_sha256(freeze_root/"validation_candidate_freeze.json"),"candidate_freeze_payload_sha256":freeze["freeze_payload_sha256"],"candidate_members_path":str((freeze_root/"validation_candidate_members.jsonl").resolve()),"candidate_members_payload_sha256":freeze["candidate_members_payload_sha256"],"candidate_count":len(members),"candidate_exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"transfer_filter_id":freeze["transfer_filter_id"],"transfer_filter_application_file_sha256":freeze["transfer_filter_application_file_sha256"],"transfer_filter_selected_count":freeze["transfer_filter_selected_count"],"transfer_filter_selected_exact_identities_sha256":freeze["transfer_filter_selected_exact_identities_sha256"],"resolved_schedule_path":str(schedules_path),"resolved_schedule_file_sha256":_sha256(schedules_path),"required_physical_leaf_count":len(required),"required_physical_leaf_ids":list(required),"validation_field_root":str(field_root),"validation_field_manifest_sha256":field_sha,"validation_label_root":str(args.validation_label_root.resolve()),"validation_session_authority_root":str(authority),"validation_session_authority_manifest_sha256":_sha256(authority/"validation_session_authority_manifest.json"),"validation_session_authority_audit_sha256":_sha256(audit/"audit.json"),"validation_windows":list(VALIDATION_WINDOWS),"validation_reads_during_context_smoke":int(context["validation_reads"]),"holdout_reads":0,"forward_2026_reads":0,"candidate_evaluation_executed":False,"optimizer_feedback_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","archive_write":"FORBIDDEN","promotion":"FORBIDDEN"}
    prepared["prepared_binding_payload_sha256"]=stable_hash(prepared); p=_write_json(out/"D1_TRANSFER_PROSPECTIVE_VALIDATION_PREFINANCIAL_READY.json",prepared)
    return {**prepared,"prepared_binding_file_sha256":_sha256(p)}

def parser()->argparse.ArgumentParser:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--repo-root",type=Path,required=True); p.add_argument("--repo-sha",required=True); p.add_argument("--freeze-root",type=Path,required=True); p.add_argument("--output-root",type=Path,required=True); p.add_argument("--minute-source-root",type=Path,required=True); p.add_argument("--fundamental-root",type=Path,required=True); p.add_argument("--chip-root",type=Path,required=True); p.add_argument("--registry",type=Path,required=True); p.add_argument("--split-manifest",type=Path,required=True); p.add_argument("--public-source-root",type=Path,required=True); p.add_argument("--daily-st-source",type=Path,required=True); p.add_argument("--source-contract",type=Path,required=True); p.add_argument("--validation-label-root",type=Path,required=True); return p

def main(argv:Sequence[str]|None=None)->int:
    result=prepare(parser().parse_args(argv)); print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
