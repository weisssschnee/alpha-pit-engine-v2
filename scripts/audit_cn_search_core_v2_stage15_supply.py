"""Zero-financial supply audit for mature Search Core V2 Stage-1.5 continuation."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_search_core_v2_stage1_v1 as stage1
from scripts import run_cn_search_core_v2_stage15_v1 as stage15
from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as stage_d_runtime
from our_system_phase2.runtime import cn_search_core_v2_stage15_v1 as runtime
from our_system_phase2.services.program_search_state_jump_adapter_v2 import StateJumpProgramSearchAdapterV2
from our_system_phase2.services.unified_capability_registry import stable_hash

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo = PROJECT_ROOT.resolve()
    source_auth = stage_d_runtime.verify_authorization(args.source_stage_d_authorization.resolve(), repo_root=repo)
    pre = stage15.verify_prefreeze(args.stage15_prefreeze.resolve())
    provisional = {
        "authorization_payload_sha256": stable_hash({"role":"SEARCH_CORE_V2_STAGE15_ZERO_FINANCIAL_SUPPLY","prefreeze":pre["prefreeze_payload_sha256"]}),
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "source_prior_exact": dict(source_auth["source_prior_exact"]),
    }
    args.executor_workers = runtime.PRIMARY_EXECUTOR_WORKERS
    authority = stage1._load_authority(args, authorization=provisional, repo_sha=str(args.repo_sha))
    snapshot_path = repo / Path(str(pre["arm_b_state_jump"]["source_snapshot_relative_path"]))
    if engine._sha256(snapshot_path) != str(pre["arm_b_state_jump"]["source_snapshot_file_sha256"]):
        raise RuntimeError("STAGE15_SUPPLY_SNAPSHOT_FILE_DRIFT")
    snapshot = _read(snapshot_path)
    source_snapshot_hash = str(snapshot["snapshot_hash"])
    schedules=[]; operations=Counter(); by_template={}; global_ordinal=0
    # Zero-financial audit must not fabricate tell() feedback.  Each template is
    # therefore probed from the exact same mature frozen snapshot.  The live
    # runner, by contrast, evaluates then tells after every template batch.
    for checkpoint_ordinal, template in enumerate(stage1.TEMPLATES):
        state = StateJumpProgramSearchAdapterV2.restore(
            snapshot=snapshot, adapter=authority["adapter"], compiler=authority["compiler"],
            components_by_role=stage1._components_by_role(authority), seed=0,
        )
        rows, asks = stage1._generated_schedules(
            state, authority=authority, template_id=template,
            checkpoint_id=f"SEARCH_CORE_V2_STAGE15_SUPPLY_{template}",
            checkpoint_ordinal=checkpoint_ordinal, global_start=global_ordinal, template_start=0,
        )
        global_ordinal += len(rows); schedules.extend(rows); by_template[template]=len(rows)
        operations.update(str(dict(dict(a.get("acquisition") or {}).get("generator_summary") or {}).get("operation") or "UNKNOWN") for a in asks)
    exacts=[str(s["search_core_exact_identity"]) for s in schedules]
    a_exacts=set(map(str, pre["arm_a_primitive"]["selected_exact_identities"]))
    if len(exacts) != 168 or len(set(exacts)) != 168:
        raise RuntimeError("STAGE15_SUPPLY_GENERATED_EXACT_DRIFT")
    if set(exacts).intersection(a_exacts):
        raise RuntimeError("STAGE15_SUPPLY_ARM_OVERLAP")
    if any(v != 24 for v in by_template.values()):
        raise RuntimeError("STAGE15_SUPPLY_TEMPLATE_UNDERFILL")
    fields=tuple(sorted(map(str, engine._checkpoint_field_columns(schedules))))
    if not fields:
        raise RuntimeError("STAGE15_SUPPLY_FIELD_SURFACE_EMPTY")
    payload={
        "schema_version":"cn_search_core_v2_stage15_mature_supply_audit_v1",
        "status":"PASS_ZERO_FINANCIAL_MATURE_STATE_JUMP_SUPPLY_AUDIT",
        "repo_sha":str(args.repo_sha),
        "prefreeze_payload_sha256":str(pre["prefreeze_payload_sha256"]),
        "source_snapshot_payload_sha256":source_snapshot_hash,
        "generated_total":len(exacts),
        "unique_exact_count":len(set(exacts)),
        "generated_exact_identities_sha256":stable_hash(sorted(exacts)),
        "arm_a_overlap_count":0,
        "per_template_generated":by_template,
        "operation_counts":dict(sorted(operations.items())),
        "resource_field_surface":{"field_column_count":len(fields),"field_columns":list(fields),"field_columns_sha256":stable_hash(list(fields))},
        "candidate_evaluation_executed":False,
        "financial_sidecar_read":False,
        "restricted_reads":{"validation":0,"holdout":0,"historical_2023":0,"forward_b":0,"forward_2026":0},
    }
    payload["audit_payload_sha256"]=stable_hash(payload)
    return payload


def main(argv: list[str] | None=None) -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-stage-d-authorization",type=Path,required=True)
    parser.add_argument("--stage15-prefreeze",type=Path,required=True)
    parser.add_argument("--source-freeze-root",type=Path,required=True)
    parser.add_argument("--prior-exact-freeze",type=Path,required=True)
    parser.add_argument("--execution-contract",type=Path,required=True)
    parser.add_argument("--train-field-root",type=Path,required=True)
    parser.add_argument("--train-price-root",type=Path,required=True)
    parser.add_argument("--registry",type=Path,required=True)
    parser.add_argument("--node-resource-capacity",type=Path,required=True)
    parser.add_argument("--repo-sha",required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args(argv); payload=run(args)
    args.output.parent.mkdir(parents=True,exist_ok=True); engine._write_json(args.output,payload)
    print(json.dumps({"status":payload["status"],"generated_total":payload["generated_total"],"payload":payload["audit_payload_sha256"],"output":str(args.output.resolve())},sort_keys=True)); return 0

if __name__=="__main__": raise SystemExit(main())
