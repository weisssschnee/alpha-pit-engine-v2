"""Official zero-financial resource canary for Search Core V2 Stage-2."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT=Path(__file__).resolve().parents[1]
from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_large_fresh_v1 as large_fresh
from scripts import run_cn_search_core_v2_stage1_v1 as stage1
from scripts import run_cn_search_core_v2_stage2_v1 as stage2
from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as stage_d_runtime
from our_system_phase2.runtime import cn_search_core_v2_stage2_v1 as runtime
from our_system_phase2.services.project_control_admission import sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash

PROBE_SECONDS=30.0

def _read(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding="utf-8-sig"))
def _verify(payload:Mapping[str,Any],field:str,label:str)->str:
    body=dict(payload); claimed=str(body.pop(field,""))
    if not claimed or stable_hash(body)!=claimed: raise RuntimeError(f"{label} self-hash drift")
    return claimed

def run(args:argparse.Namespace)->dict[str,Any]:
    repo=PROJECT_ROOT.resolve()
    source_auth=stage_d_runtime.verify_authorization(args.source_stage_d_authorization.resolve(),repo_root=repo)
    pre=stage2.verify_prefreeze(args.stage2_prefreeze.resolve())
    supply=_read(args.mature_state_supply_audit.resolve()); supply_hash=_verify(supply,"audit_payload_sha256","Stage-2 mature supply")
    if (
        supply.get("status")!="PASS_ZERO_FINANCIAL_MATURE_STATE_JUMP_STAGE2_CHECKPOINT_SUPPLY_AUDIT"
        or bool(supply.get("candidate_evaluation_executed")) or bool(supply.get("financial_sidecar_read"))
        or int(supply.get("generated_total") or 0)!=runtime.SUPPLY_PROBE_TOTAL
        or int(supply.get("unique_exact_count") or 0)!=runtime.SUPPLY_PROBE_TOTAL
        or int(supply.get("arm_a_overlap_count",-1))!=0
        or dict(supply.get("template_batch_size") or {})!=runtime.EXPECTED_TEMPLATE_BATCH_SIZE
        or int(supply.get("base_event_microbatch_robustness_state_count") or 0)!=8
        or int(supply.get("base_event_microbatch_robustness_batch_size") or 0)!=24
        or supply.get("synthetic_tell_used") is not False
        or supply.get("future_checkpoint_supply_fail_closed") is not True
        or str(supply.get("prefreeze_payload_sha256") or "")!=str(pre["prefreeze_payload_sha256"])
    ): raise RuntimeError("STAGE2_CANARY_SUPPLY_NOT_PASS")
    provisional={
        "authorization_payload_sha256":stable_hash({"role":"SEARCH_CORE_V2_STAGE2_ZERO_FINANCIAL_CANARY","prefreeze":pre["prefreeze_payload_sha256"],"supply":supply_hash}),
        "campaign_id":runtime.CAMPAIGN_ID,"campaign_profile":runtime.CAMPAIGN_PROFILE,
        "source_prior_exact":dict(source_auth["source_prior_exact"]),
    }
    args.executor_workers=runtime.PRIMARY_EXECUTOR_WORKERS
    authority=stage1._load_authority(args,authorization=provisional,repo_sha=str(args.repo_sha))
    stage_d_supply=stage1._stage_d_supply(repo,pre)
    by_exact={str(r["exact_identity"]):dict(r) for r in stage_d_supply["fresh_entries"]}
    schedules=[]; global_ordinal=0
    for checkpoint_ordinal,template in enumerate(stage1.TEMPLATES):
        exacts=list(map(str,pre["arm_a_primitive"]["selected_by_template"][template]))
        if len(exacts)!=72: raise RuntimeError("STAGE2_CANARY_ARM_A_UNDERFILL")
        for template_ordinal,exact in enumerate(exacts):
            schedules.append(stage2._static_schedule(by_exact[exact],authority=authority,global_ordinal=global_ordinal,checkpoint_ordinal=checkpoint_ordinal,template_ordinal=template_ordinal)); global_ordinal+=1
    if len(schedules)!=504: raise RuntimeError("STAGE2_CANARY_ARM_A_COUNT_DRIFT")
    arm_a_fields=set(map(str,engine._checkpoint_field_columns(schedules)))
    arm_b_fields=set(map(str,supply["resource_field_surface"]["field_columns"]))
    fields=tuple(sorted(arm_a_fields|arm_b_fields))
    if not fields: raise RuntimeError("STAGE2_CANARY_FIELD_SURFACE_EMPTY")
    old=large_fresh.RESOURCE_CANARY_PROBE_SECONDS
    try:
        large_fresh.RESOURCE_CANARY_PROBE_SECONDS=PROBE_SECONDS
        canary=large_fresh._resource_canary(authority,str(authority["input_binding"]["input_binding_sha256"]),runtime.PRIMARY_EXECUTOR_WORKERS,fields)
    finally: large_fresh.RESOURCE_CANARY_PROBE_SECONDS=old
    if (
        canary.get("status")!="PASS_ZERO_CANDIDATE_EVALUATION_RESOURCE_CANARY"
        or int(canary.get("requested_workers") or 0)!=runtime.PRIMARY_EXECUTOR_WORKERS
        or int(canary.get("pagefile_pages_in_delta_bytes",-1))!=0 or int(canary.get("pagefile_pages_out_delta_bytes",-1))!=0
        or canary.get("candidate_evaluation_executed") is not False
    ): raise RuntimeError("STAGE2_OFFICIAL_RESOURCE_CANARY_FAIL")
    runner=repo/"scripts/run_cn_search_core_v2_stage2_v1.py"
    payload={
        "schema_version":"cn_search_core_v2_stage2_official_resource_canary_v1","status":"PASS","repo_sha":str(args.repo_sha),
        "runner_source_file_sha256":sha256_file(runner),"prefreeze_payload_sha256":str(pre["prefreeze_payload_sha256"]),
        "mature_state_supply_audit_payload_sha256":supply_hash,"source_stage_d_authorization_payload_sha256":str(source_auth["authorization_payload_sha256"]),
        "resource_profile":"SEARCH_DUAL_24","requested_workers":runtime.PRIMARY_EXECUTOR_WORKERS,
        "template_batch_size":dict(pre["stage2"]["template_batch_size"]),
        "field_column_count":len(fields),"field_columns":list(fields),"field_columns_sha256":stable_hash(list(fields)),
        "arm_a_field_column_count":len(arm_a_fields),"arm_a_field_columns_sha256":stable_hash(sorted(arm_a_fields)),
        "arm_b_field_column_count":len(arm_b_fields),"arm_b_field_columns_sha256":stable_hash(sorted(arm_b_fields)),
        "resource_probe":dict(canary),"candidate_evaluation_executed":False,
        "validation_reads":0,"holdout_reads":0,"historical_2023_reads":0,"forward_b_reads":0,"forward_2026_reads":0,
    }
    payload["official_canary_payload_sha256"]=stable_hash(payload); return payload

def main(argv:list[str]|None=None)->int:
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source-stage-d-authorization",type=Path,required=True); p.add_argument("--stage2-prefreeze",type=Path,required=True)
    p.add_argument("--mature-state-supply-audit",type=Path,required=True); p.add_argument("--source-freeze-root",type=Path,required=True)
    p.add_argument("--prior-exact-freeze",type=Path,required=True); p.add_argument("--execution-contract",type=Path,required=True)
    p.add_argument("--train-field-root",type=Path,required=True); p.add_argument("--train-price-root",type=Path,required=True)
    p.add_argument("--registry",type=Path,required=True); p.add_argument("--node-resource-capacity",type=Path,required=True)
    p.add_argument("--repo-sha",required=True); p.add_argument("--output",type=Path,required=True)
    args=p.parse_args(argv); payload=run(args); args.output.parent.mkdir(parents=True,exist_ok=True); engine._write_json(args.output,payload)
    print(json.dumps({"status":payload["status"],"field_column_count":payload["field_column_count"],"payload":payload["official_canary_payload_sha256"],"output":str(args.output.resolve())},sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
