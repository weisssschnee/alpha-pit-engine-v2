"""Prepare a frozen fresh-D1 cohort for prospective transfer-filter validation.

The candidate/filter membership freeze must exist before this command starts.
The CLI is deliberately zero-validation-read.  It resolves immutable
development schedules, verifies frozen development
provenance plus immutable validation manifests and writes a materialization
plan.  Actual validation sidecar materialization and context loading happen
only through :func:`materialize_authorized` after Project Control admission has
been consumed.  Neither path evaluates a candidate or writes optimizer
feedback.
"""
from __future__ import annotations

import argparse, csv, hashlib, json, subprocess, sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for p in (PROJECT_ROOT, PROJECT_ROOT / "src"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from our_system_phase2.services.unified_capability_registry import stable_hash

EXPECTED_SPLIT_SHA256 = "fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241"
EXPECTED_PUBLIC_SOURCE_MANIFEST_SHA256 = "0c06baa7c24109ea8a1064a291565ce6352ab2a0ca32f20f6cc257261e25514d"
EXPECTED_DAILY_ST_SOURCE_SHA256 = "7060dd78cde6b826157f10c03541a4c302d11fe3213517236dc893393c071e68"
ZERO_READ_STATUS = "D1_TRANSFER_PROSPECTIVE_VALIDATION_ZERO_READ_PREFLIGHT_READY"
READY_STATUS = "D1_TRANSFER_PROSPECTIVE_VALIDATION_PREFINANCIAL_READY"
FIELD_MANIFEST_NAME = "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
LABEL_MANIFEST_NAME = "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json"
AUTHORITY_MANIFEST_NAME = "validation_session_authority_manifest.json"
EXPECTED_SOURCE_COHORT = "D1_CONTINUATION_C"
EXPECTED_CANDIDATE_COUNT = 67
EXPECTED_SELECTED_COUNT = 27
EXPECTED_CANDIDATE_SET_SHA256 = "80baef6280f1ae0c10644e10a1391679d7ed5b8aa56ad540cfaf0ba811d6ae18"
EXPECTED_SELECTED_SET_SHA256 = "53b7f2aab3dbeec8964525e6a46ce20509b4081044836b8baef068e5740d619b"
EXPECTED_FREEZE_PAYLOAD_SHA256 = "389cf2e86db80104d58110a3a477b8b42b6259c049939c640d41634c8283cb6f"
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
    if (
        freeze.get("status") != "FROZEN_BEFORE_VALIDATION_ACCESS"
        or freeze.get("source_cohort") != EXPECTED_SOURCE_COHORT
        or freeze.get("freeze_payload_sha256") != EXPECTED_FREEZE_PAYLOAD_SHA256
        or int(freeze.get("candidate_count") or 0) != EXPECTED_CANDIDATE_COUNT
        or int(freeze.get("transfer_filter_selected_count") or 0) != EXPECTED_SELECTED_COUNT
        or freeze.get("candidate_exact_identities_sha256") != EXPECTED_CANDIDATE_SET_SHA256
        or freeze.get("transfer_filter_selected_exact_identities_sha256") != EXPECTED_SELECTED_SET_SHA256
    ):
        raise RuntimeError("prospective C validation freeze not ready")
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
        if (
            str(schedule.get("d1_exact_identity") or "")!=exact
            or int(schedule.get("d1_wave_index") if schedule.get("d1_wave_index") is not None else -1)!=wave
            or str(schedule.get("d1_logical_proposal_id") or "")!=str(member.get("logical_proposal_id") or "")
            or str(schedule.get("d1_selection_kind") or "")!=str(member.get("selection_kind") or "")
        ): raise RuntimeError("prospective schedule/member logical lineage drift")
        resolved.append(schedule)
    return resolved

def _result_path(run_root:Path,wave:int)->Path:
    candidates=(run_root/f"wave_{wave:03d}"/"physical_results.jsonl",run_root/f"wave_{wave:03d}"/"wave_physical_results.jsonl")
    existing=[path for path in candidates if path.is_file()]
    if len(existing)!=1: raise RuntimeError(f"prospective physical result artifact cardinality drift: wave={wave} paths={existing}")
    return existing[0]

def _verify_source_results(members:Sequence[Mapping[str,Any]])->list[dict[str,Any]]:
    grouped:dict[tuple[str,int],list[Mapping[str,Any]]]={}
    for member in members:
        grouped.setdefault((str(Path(str(member["source_root"])).resolve()),int(member["source_wave"])),[]).append(member)
    artifacts=[]
    for (root_text,wave),group in sorted(grouped.items()):
        path=_result_path(Path(root_text),wave); rows=_read_jsonl(path)
        by_exact={str(row.get("exact_identity") or ""):row for row in rows}
        if len(by_exact)!=len(rows): raise RuntimeError(f"prospective duplicate physical result identity: {path}")
        for member in group:
            exact=str(member["exact_identity"]); result=by_exact.get(exact)
            if result is None: raise RuntimeError(f"prospective physical result missing: {exact}")
            if int(result.get("wave_index") if result.get("wave_index") is not None else -1)!=wave: raise RuntimeError(f"prospective physical result wave drift: {exact}")
            if str(result.get("source_record_sha256") or "")!=str(member.get("source_record_sha256") or ""): raise RuntimeError(f"prospective source record drift: {exact}")
            if str(result.get("physical_result_hash") or "")!=str(member.get("physical_result_hash") or ""): raise RuntimeError(f"prospective physical result hash drift: {exact}")
            body={"schema_version":"cn_program_successor_physical_result_v1","exact_identity":exact,"admission":result.get("admission"),"uplift":result.get("uplift")}
            if stable_hash(body)!=str(result["physical_result_hash"]): raise RuntimeError(f"prospective physical result self-hash drift: {exact}")
            if str(dict(result.get("admission") or {}).get("record_payload_sha256") or "")!=str(result["source_record_sha256"]): raise RuntimeError(f"prospective admission source record drift: {exact}")
            if not bool(dict(result.get("admission") or {}).get("admitted")) or result.get("uplift") is None: raise RuntimeError(f"prospective frozen member is not development productive: {exact}")
        artifacts.append({"path":str(path.resolve()),"file_sha256":_sha256(path),"wave":wave})
    return artifacts

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

def _verify_shards(manifest:Mapping[str,Any],*,label:str)->None:
    shards=list(manifest.get("shards") or ())
    if len(shards)!=16: raise RuntimeError(f"{label} shard cardinality drift")
    for shard in shards:
        path=Path(str(shard["output_path"])).resolve()
        if not path.is_file() or path.stat().st_size!=int(shard["output_bytes"]) or _sha256(path)!=str(shard["output_sha256"]): raise RuntimeError(f"{label} shard drift: {path}")

def _verify_base_field_root(root:Path,required:Sequence[str])->tuple[dict[str,Any],Path,tuple[str,...]]:
    path=root.resolve()/FIELD_MANIFEST_NAME; payload=_read_json(path)
    if payload.get("status")!="TIME_MAJOR_LAYOUT_PARITY_PASS" or payload.get("evaluation_role")!="validation" or payload.get("data_role")!="validation_report_only" or payload.get("split_manifest_hash")!=EXPECTED_SPLIT_SHA256 or int(payload.get("holdout_reads") or 0)!=0 or int(payload.get("forward_2026_reads") or 0)!=0: raise RuntimeError("prospective base validation field manifest drift")
    _verify_shards(payload,label="prospective base validation field")
    fields=set(map(str,payload.get("fields") or ())); missing=tuple(sorted(set(required)-fields))
    return payload,path,missing

def _verify_label_root(root:Path)->tuple[dict[str,Any],Path]:
    path=root.resolve()/LABEL_MANIFEST_NAME; payload=_read_json(path)
    if payload.get("status")!="GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY" or payload.get("evaluation_role")!="validation" or payload.get("data_role")!="validation_report_only" or payload.get("split_manifest_hash")!=EXPECTED_SPLIT_SHA256 or int(payload.get("holdout_reads") or 0)!=0 or int(payload.get("forward_2026_reads") or 0)!=0: raise RuntimeError("prospective validation label manifest drift")
    _verify_shards(payload,label="prospective validation label")
    return payload,path

def _verify_source_authority(*,root:Path,audit_path:Path,field_manifest_sha256:str)->tuple[dict[str,Any],Path,dict[str,Any]]:
    manifest_path=root.resolve()/AUTHORITY_MANIFEST_NAME; manifest=_read_json(manifest_path); _verify_self_hash(manifest,"manifest_payload_sha256","prospective source validation authority")
    if manifest.get("status")!="VALIDATION_SESSION_AUTHORITY_CLOSED_IMMUTABLE" or manifest.get("evaluation_role")!="validation" or manifest.get("data_role")!="validation_report_only" or manifest.get("field_manifest_sha256")!=field_manifest_sha256 or int(manifest.get("holdout_reads") or 0)!=0 or int(manifest.get("forward_2026_reads") or 0)!=0: raise RuntimeError("prospective source validation authority drift")
    for artifact in manifest.get("artifacts") or ():
        path=root.resolve()/str(artifact["path"])
        if not path.is_file() or path.stat().st_size!=int(artifact["bytes"]) or _sha256(path)!=str(artifact["sha256"]): raise RuntimeError(f"prospective source validation authority artifact drift: {path}")
    audit=_read_json(audit_path.resolve()); _verify_self_hash(audit,"receipt_payload_sha256","prospective source validation authority audit")
    if audit.get("status")!="PASS_INDEPENDENT_VALIDATION_SESSION_AUTHORITY_VERIFICATION" or audit.get("authority_manifest_sha256")!=_sha256(manifest_path) or int(audit.get("holdout_reads") or 0)!=0 or int(audit.get("forward_2026_reads") or 0)!=0: raise RuntimeError("prospective source validation authority audit drift")
    return manifest,manifest_path,audit

def prepare_zero_read(args:argparse.Namespace)->dict[str,Any]:
    repo=args.repo_root.resolve(); freeze_root=args.freeze_root.resolve(); out=args.output_root.resolve()
    if out.exists(): raise FileExistsError(out)
    out.mkdir(parents=True)
    freeze,members=_load_freeze(freeze_root); source_results=_verify_source_results(members); schedules=_resolve_schedules(members); required=_required_fields(schedules)
    if not required: raise RuntimeError("prospective validation has no required fields")
    schedules_path=_write_jsonl(out/"resolved_program_schedules.jsonl",schedules)
    req_path=_requirements_table(out/"validation_program_required_fields.csv",required)
    selection={"schema_version":"cn_program_optimizer_d1_transfer_prospective_selection_binding_v1","status":"FROZEN_BEFORE_VALIDATION_RESULT_ACCESS","candidate_freeze_payload_sha256":freeze["freeze_payload_sha256"],"candidate_exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"candidate_count":len(members),"transfer_filter_id":freeze["transfer_filter_id"],"transfer_filter_selected_count":freeze["transfer_filter_selected_count"],"transfer_filter_selected_exact_identities_sha256":freeze["transfer_filter_selected_exact_identities_sha256"],"resolved_schedule_file_sha256":_sha256(schedules_path),"required_physical_leaf_count":len(required),"required_physical_leaf_ids":list(required),"requirements_table_file_sha256":_sha256(req_path),"validation_windows":list(VALIDATION_WINDOWS),"validation_reads":0,"candidate_evaluation_executed":False}
    selection["binding_payload_sha256"]=stable_hash(selection); _write_json(out/"selection_binding.json",selection)
    base_field_root=args.base_validation_field_root.resolve(); _,base_manifest_path,missing_required=_verify_base_field_root(base_field_root,required)
    _,label_manifest_path=_verify_label_root(args.validation_label_root)
    _,source_authority_manifest_path,_=_verify_source_authority(root=args.source_validation_session_authority_root,audit_path=args.source_validation_session_authority_audit,field_manifest_sha256=_sha256(base_manifest_path))
    plan={"schema_version":"cn_program_optimizer_d1_transfer_prospective_validation_zero_read_preflight_v1","status":ZERO_READ_STATUS,"implementation_repo_sha":str(args.repo_sha),"candidate_freeze_relative_path":str((freeze_root/"validation_candidate_freeze.json").resolve().relative_to(repo)).replace("\\","/"),"candidate_freeze_file_sha256":_sha256(freeze_root/"validation_candidate_freeze.json"),"candidate_freeze_payload_sha256":freeze["freeze_payload_sha256"],"candidate_members_relative_path":str((freeze_root/"validation_candidate_members.jsonl").resolve().relative_to(repo)).replace("\\","/"),"candidate_members_payload_sha256":freeze["candidate_members_payload_sha256"],"candidate_count":len(members),"candidate_exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"source_cohort":freeze["source_cohort"],"transfer_filter_id":freeze["transfer_filter_id"],"transfer_filter_application_file_sha256":freeze["transfer_filter_application_file_sha256"],"transfer_filter_selected_count":freeze["transfer_filter_selected_count"],"transfer_filter_selected_exact_identities_sha256":freeze["transfer_filter_selected_exact_identities_sha256"],"resolved_schedule_path":str(schedules_path),"resolved_schedule_file_sha256":_sha256(schedules_path),"source_physical_result_files":source_results,"required_physical_leaf_count":len(required),"required_physical_leaf_ids":list(required),"requirements_table_path":str(req_path),"requirements_table_file_sha256":_sha256(req_path),"base_validation_field_root":str(base_field_root),"base_validation_field_manifest_sha256":_sha256(base_manifest_path),"missing_required_physical_leaf_count":len(missing_required),"missing_required_physical_leaf_ids":list(missing_required),"minute_source_root":str(args.minute_source_root.resolve()),"fundamental_root":str(args.fundamental_root.resolve()),"chip_root":str(args.chip_root.resolve()),"registry_path":str(args.registry.resolve()),"registry_sha256":_sha256(args.registry.resolve()),"split_manifest_path":str(args.split_manifest.resolve()),"split_manifest_sha256":_sha256(args.split_manifest.resolve()),"public_source_root":str(args.public_source_root.resolve()),"public_source_manifest_sha256":_sha256(args.public_source_root.resolve()/"source_snapshot_manifest.json"),"daily_st_source":str(args.daily_st_source.resolve()),"daily_st_source_sha256":_sha256(args.daily_st_source.resolve()),"source_contract_path":str(args.source_contract.resolve()),"source_contract_sha256":_sha256(args.source_contract.resolve()),"validation_label_root":str(args.validation_label_root.resolve()),"validation_label_manifest_sha256":_sha256(label_manifest_path),"source_validation_session_authority_root":str(args.source_validation_session_authority_root.resolve()),"source_validation_session_authority_manifest_sha256":_sha256(source_authority_manifest_path),"source_validation_session_authority_audit_path":str(args.source_validation_session_authority_audit.resolve()),"source_validation_session_authority_audit_sha256":_sha256(args.source_validation_session_authority_audit.resolve()),"validation_windows":list(VALIDATION_WINDOWS),"validation_reads_by_this_preflight":0,"candidate_evaluation_executed":False,"optimizer_feedback_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","archive_write":"FORBIDDEN","promotion":"FORBIDDEN","holdout_reads":0,"forward_2026_reads":0}
    plan["zero_read_preflight_payload_sha256"]=stable_hash(plan); p=_write_json(out/"D1_TRANSFER_PROSPECTIVE_VALIDATION_ZERO_READ_PREFLIGHT_READY.json",plan)
    return {**plan,"zero_read_preflight_file_sha256":_sha256(p)}

def materialize_authorized(*,repo_root:Path,preflight_binding:Path,output_root:Path)->Path:
    repo=repo_root.resolve(); plan=_read_json(preflight_binding.resolve()); _verify_self_hash(plan,"zero_read_preflight_payload_sha256","prospective validation zero-read preflight")
    if plan.get("status")!=ZERO_READ_STATUS or int(plan.get("validation_reads_by_this_preflight") or -1)!=0 or bool(plan.get("candidate_evaluation_executed")): raise RuntimeError("prospective zero-read preflight drift")
    out=output_root.resolve()
    if out.exists(): raise FileExistsError(out)
    out.mkdir(parents=True)
    freeze_root=(repo/str(plan["candidate_freeze_relative_path"])).resolve().parent; freeze,members=_load_freeze(freeze_root)
    source_results=_verify_source_results(members)
    if source_results!=list(plan["source_physical_result_files"]): raise RuntimeError("prospective source physical result artifact drift")
    if freeze["freeze_payload_sha256"]!=plan["candidate_freeze_payload_sha256"] or freeze["candidate_exact_identities_sha256"]!=plan["candidate_exact_identities_sha256"] or len(members)!=int(plan["candidate_count"]): raise RuntimeError("prospective zero-read freeze drift")
    schedules_path=Path(str(plan["resolved_schedule_path"])).resolve(); schedules=_read_jsonl(schedules_path)
    if _sha256(schedules_path)!=str(plan["resolved_schedule_file_sha256"]) or len(schedules)!=len(members): raise RuntimeError("prospective zero-read schedule drift")
    required=_required_fields(schedules)
    if list(required)!=list(plan["required_physical_leaf_ids"]): raise RuntimeError("prospective zero-read required leaf drift")
    base_field_root=Path(str(plan["base_validation_field_root"])).resolve(); _,base_manifest_path,missing_required=_verify_base_field_root(base_field_root,required)
    if _sha256(base_manifest_path)!=str(plan["base_validation_field_manifest_sha256"]) or list(missing_required)!=list(plan["missing_required_physical_leaf_ids"]): raise RuntimeError("prospective zero-read materialization plan drift")
    label_root=Path(str(plan["validation_label_root"])).resolve(); _,label_manifest_path=_verify_label_root(label_root)
    if _sha256(label_manifest_path)!=str(plan["validation_label_manifest_sha256"]): raise RuntimeError("prospective validation label binding drift")
    source_authority_root=Path(str(plan["source_validation_session_authority_root"])).resolve(); source_audit_path=Path(str(plan["source_validation_session_authority_audit_path"])).resolve()
    _,source_authority_manifest_path,_=_verify_source_authority(root=source_authority_root,audit_path=source_audit_path,field_manifest_sha256=_sha256(base_manifest_path))
    if _sha256(source_authority_manifest_path)!=str(plan["source_validation_session_authority_manifest_sha256"]) or _sha256(source_audit_path)!=str(plan["source_validation_session_authority_audit_sha256"]): raise RuntimeError("prospective validation source authority binding drift")
    for key,path_field,hash_field in (
        ("registry","registry_path","registry_sha256"),
        ("split_manifest","split_manifest_path","split_manifest_sha256"),
        ("daily_st_source","daily_st_source","daily_st_source_sha256"),
        ("source_contract","source_contract_path","source_contract_sha256"),
    ):
        path=Path(str(plan[path_field])).resolve()
        if _sha256(path)!=str(plan[hash_field]): raise RuntimeError(f"prospective {key} binding drift")
    public_root=Path(str(plan["public_source_root"])).resolve()
    if _sha256(public_root/"source_snapshot_manifest.json")!=str(plan["public_source_manifest_sha256"]): raise RuntimeError("prospective public source binding drift")
    field_root=out/"program_validation_session_fields"
    if not missing_required:
        field_root=base_field_root
    else:
        missing_req_path=_requirements_table(out/"validation_program_missing_fields.csv",missing_required); incremental_root=out/"incremental_validation_session_fields"
        _run([sys.executable,str(repo/"scripts/build_cn_core_pack_validation_session_sidecar.py"),"--source-root",str(Path(str(plan["minute_source_root"])).resolve()),"--evaluation-role","validation","--output-root",str(incremental_root),"--candidate-table",str(missing_req_path),"--registry",str(Path(str(plan["registry_path"])).resolve()),"--split-manifest",str(Path(str(plan["split_manifest_path"])).resolve()),"--split-manifest-hash",EXPECTED_SPLIT_SHA256,"--fundamental-root",str(Path(str(plan["fundamental_root"])).resolve()),"--chip-root",str(Path(str(plan["chip_root"])).resolve()),"--max-shards","16"])
        incremental_manifest=_read_json(incremental_root/FIELD_MANIFEST_NAME)
        incremental_fields=set(map(str,incremental_manifest.get("fields") or ()))
        if not set(missing_required).issubset(incremental_fields): raise RuntimeError("prospective incremental sidecar misses planned fields")
        required_path=out/"required_physical_leaf_ids.json"; required_path.write_text(json.dumps(list(required),ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        _run([sys.executable,str(repo/"scripts/fuse_cn_program_validation_session_sidecar_v1.py"),"--base-root",str(base_field_root),"--incremental-root",str(incremental_root),"--output-root",str(field_root),"--required-fields-json",str(required_path)])
    field_manifest=field_root/FIELD_MANIFEST_NAME; field_sha=_sha256(field_manifest)
    materialized_manifest=_read_json(field_manifest); _verify_shards(materialized_manifest,label="prospective materialized validation field")
    if not set(required).issubset(set(map(str,materialized_manifest.get("fields") or ()))): raise RuntimeError("prospective materialized validation fields incomplete")
    authority=out/"validation_session_authority"
    _run([sys.executable,str(repo/"scripts/build_cn_validation_session_authority.py"),"--field-manifest",str(field_manifest),"--public-source-root",str(public_root),"--output-root",str(authority),"--expected-field-manifest-sha256",field_sha,"--expected-source-manifest-sha256",EXPECTED_PUBLIC_SOURCE_MANIFEST_SHA256,"--historical-daily-st-source",str(Path(str(plan["daily_st_source"])).resolve()),"--expected-daily-st-source-sha256",EXPECTED_DAILY_ST_SOURCE_SHA256,"--builder-commit-sha",str(plan["implementation_repo_sha"]),"--evaluation-role","validation"])
    audit=out/"validation_session_authority_audit"
    _run([sys.executable,str(repo/"scripts/verify_cn_validation_session_authority.py"),"--authority-root",str(authority),"--output-root",str(audit)])
    audit_payload=_read_json(audit/"audit.json")
    if audit_payload.get("status")!="PASS_INDEPENDENT_VALIDATION_SESSION_AUTHORITY_VERIFICATION": raise RuntimeError("prospective validation authority audit failed")
    from scripts import run_cn_portfolio_decoder_v2_oos as oos
    context=oos._load_validation_context(source_contract_path=Path(str(plan["source_contract_path"])).resolve(),validation_field_root=field_root,validation_label_root=label_root,validation_session_authority_root=authority)
    missing=sorted(set(required)-set(context["field_frame"].columns))
    if missing: raise RuntimeError(f"prospective validation prepared context misses leaves: {missing}")
    unique_dates=len(set(context["dates"]))
    if unique_dates!=73: raise RuntimeError(f"prospective validation calendar drift: {unique_dates}")
    prepared={"schema_version":"cn_program_optimizer_d1_transfer_prospective_validation_prepared_binding_v1","status":READY_STATUS,"zero_read_preflight_path":str(preflight_binding.resolve()),"zero_read_preflight_file_sha256":_sha256(preflight_binding.resolve()),"zero_read_preflight_payload_sha256":plan["zero_read_preflight_payload_sha256"],"implementation_repo_sha":str(plan["implementation_repo_sha"]),"candidate_freeze_path":str((freeze_root/"validation_candidate_freeze.json").resolve()),"candidate_freeze_file_sha256":_sha256(freeze_root/"validation_candidate_freeze.json"),"candidate_freeze_payload_sha256":freeze["freeze_payload_sha256"],"candidate_members_path":str((freeze_root/"validation_candidate_members.jsonl").resolve()),"candidate_members_payload_sha256":freeze["candidate_members_payload_sha256"],"candidate_count":len(members),"candidate_exact_identities_sha256":freeze["candidate_exact_identities_sha256"],"source_cohort":freeze["source_cohort"],"transfer_filter_id":freeze["transfer_filter_id"],"transfer_filter_application_file_sha256":freeze["transfer_filter_application_file_sha256"],"transfer_filter_selected_count":freeze["transfer_filter_selected_count"],"transfer_filter_selected_exact_identities_sha256":freeze["transfer_filter_selected_exact_identities_sha256"],"resolved_schedule_path":str(schedules_path),"resolved_schedule_file_sha256":_sha256(schedules_path),"required_physical_leaf_count":len(required),"required_physical_leaf_ids":list(required),"validation_field_root":str(field_root),"validation_field_manifest_sha256":field_sha,"validation_label_root":str(label_root),"validation_label_manifest_sha256":_sha256(label_manifest_path),"validation_session_authority_root":str(authority),"validation_session_authority_manifest_sha256":_sha256(authority/AUTHORITY_MANIFEST_NAME),"validation_session_authority_audit_sha256":_sha256(audit/"audit.json"),"validation_windows":list(VALIDATION_WINDOWS),"validation_reads_before_project_control_admission":0,"validation_reads_during_authorized_context_smoke":int(context["validation_reads"]),"holdout_reads":0,"forward_2026_reads":0,"candidate_evaluation_executed":False,"optimizer_feedback_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","archive_write":"FORBIDDEN","promotion":"FORBIDDEN"}
    prepared["prepared_binding_payload_sha256"]=stable_hash(prepared); p=_write_json(out/"D1_TRANSFER_PROSPECTIVE_VALIDATION_PREFINANCIAL_READY.json",prepared)
    return p

def parser()->argparse.ArgumentParser:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--repo-root",type=Path,required=True); p.add_argument("--repo-sha",required=True); p.add_argument("--freeze-root",type=Path,required=True); p.add_argument("--output-root",type=Path,required=True); p.add_argument("--minute-source-root",type=Path,required=True); p.add_argument("--fundamental-root",type=Path,required=True); p.add_argument("--chip-root",type=Path,required=True); p.add_argument("--registry",type=Path,required=True); p.add_argument("--split-manifest",type=Path,required=True); p.add_argument("--public-source-root",type=Path,required=True); p.add_argument("--daily-st-source",type=Path,required=True); p.add_argument("--source-contract",type=Path,required=True); p.add_argument("--validation-label-root",type=Path,required=True); p.add_argument("--base-validation-field-root",type=Path,required=True); p.add_argument("--source-validation-session-authority-root",type=Path,required=True); p.add_argument("--source-validation-session-authority-audit",type=Path,required=True); return p

def main(argv:Sequence[str]|None=None)->int:
    result=prepare_zero_read(parser().parse_args(argv)); print(json.dumps(result,ensure_ascii=False,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
