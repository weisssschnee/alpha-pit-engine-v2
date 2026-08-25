"""Zero-financial first-checkpoint supply audit for Production Wave 4."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_search_core_v2_stage1_v1 as stage1
from scripts import run_cn_search_core_v2_stage2_v1 as stage2
from scripts import run_cn_search_core_v2_production_wave4_v1 as wave
from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as stage_d_runtime
from our_system_phase2.runtime import cn_search_core_v2_production_wave4_v1 as runtime
from our_system_phase2.services.program_search_state_jump_adapter_v2 import StateJumpProgramSearchAdapterV2
from our_system_phase2.services.unified_capability_registry import stable_hash

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo = PROJECT_ROOT.resolve()
    source_auth = stage_d_runtime.verify_authorization(
        args.source_stage_d_authorization.resolve(), repo_root=repo
    )
    prefreeze = wave.verify_prefreeze(args.production_wave4_prefreeze.resolve())
    provisional = {
        "authorization_payload_sha256": stable_hash(
            {"role": "PRODUCTION_WAVE4_ZERO_FINANCIAL_SUPPLY", "prefreeze": prefreeze["prefreeze_payload_sha256"]}
        ),
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "source_prior_exact": dict(source_auth["source_prior_exact"]),
    }
    args.executor_workers = runtime.PRIMARY_EXECUTOR_WORKERS
    authority = stage1._load_authority(args, authorization=provisional, repo_sha=str(args.repo_sha))
    state_path = repo / Path(str(prefreeze["mature_state"]["relative_path"]))
    if engine._sha256(state_path) != str(prefreeze["mature_state"]["file_sha256"]):
        raise RuntimeError("PRODUCTION_WAVE4_SUPPLY_SOURCE_FILE_DRIFT")
    snapshot = _read(state_path)
    source_hash = _verify(snapshot, "snapshot_hash", "Production Wave 4 source state")
    if source_hash != str(prefreeze["mature_state"]["payload_sha256"]):
        raise RuntimeError("PRODUCTION_WAVE4_SUPPLY_SOURCE_HASH_DRIFT")

    schedules: list[dict[str, Any]] = []
    operations: Counter[str] = Counter()
    per_template: dict[str, int] = {}
    global_start = 0
    for ordinal, template in enumerate(wave.TEMPLATES):
        state = StateJumpProgramSearchAdapterV2.restore(
            snapshot=snapshot,
            adapter=authority["adapter"],
            compiler=authority["compiler"],
            components_by_role=stage1._components_by_role(authority),
            seed=0,
        )
        rows = stage2._generated_schedules_batch(
            state,
            authority=authority,
            template_id=template,
            checkpoint_id=f"PRODUCTION_WAVE4_SUPPLY_{template}",
            checkpoint_ordinal=ordinal,
            global_start=global_start,
            template_start=0,
            batch_size=24,
        )
        global_start += len(rows)
        schedules.extend(rows)
        per_template[template] = len(rows)
        operations.update(str(dict(row["generator_summary"])["operation"]) for row in rows)

    exacts = [str(row["search_core_exact_identity"]) for row in schedules]
    prior = set(map(str, snapshot["generated_exact_identities"])) | set(map(str, snapshot["seen_exact_identities"]))
    overlap = len(set(exacts) & prior)
    if (
        len(exacts) != runtime.SUPPLY_PROBE_TOTAL
        or len(set(exacts)) != runtime.SUPPLY_PROBE_TOTAL
        or overlap != 0
        or any(count != 24 for count in per_template.values())
        or "UNKNOWN" in operations
    ):
        raise RuntimeError("PRODUCTION_WAVE4_SUPPLY_EXACT_DRIFT")
    fields = tuple(sorted(map(str, engine._checkpoint_field_columns(schedules))))
    if not fields:
        raise RuntimeError("PRODUCTION_WAVE4_SUPPLY_FIELD_SURFACE_EMPTY")
    payload = {
        "schema_version": "cn_search_core_v2_production_wave4_supply_audit_v1",
        "status": "PASS_ZERO_FINANCIAL_PRODUCTION_WAVE4_CHECKPOINT_SUPPLY_AUDIT",
        "repo_sha": str(args.repo_sha),
        "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
        "policy_review_payload_sha256": str(prefreeze["source_policy_review"]["payload_sha256"]),
        "source_snapshot_payload_sha256": source_hash,
        "generated_total": len(exacts),
        "unique_exact_count": len(set(exacts)),
        "prior_overlap_count": overlap,
        "per_template_generated": dict(sorted(per_template.items())),
        "operation_counts": dict(sorted(operations.items())),
        "synthetic_tell_used": False,
        "future_checkpoint_supply_fail_closed": True,
        "source_mature_history_count": int(prefreeze["mature_state"]["source_history_count"]),
        "resource_field_surface": {
            "field_column_count": len(fields),
            "field_columns": list(fields),
            "field_columns_sha256": stable_hash(list(fields)),
        },
        "candidate_evaluation_executed": False,
        "financial_sidecar_read": False,
        "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-stage-d-authorization", type=Path, required=True)
    parser.add_argument("--production-wave4-prefreeze", type=Path, required=True)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = run(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    engine._write_json(args.output, payload)
    print(json.dumps({"status": payload["status"], "generated_total": payload["generated_total"], "payload": payload["audit_payload_sha256"], "output": str(args.output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
