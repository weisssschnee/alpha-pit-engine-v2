"""Official zero-candidate resource canary for Production Wave 3."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_large_fresh_v1 as large_fresh
from scripts import run_cn_search_core_v2_stage1_v1 as stage1
from scripts import run_cn_search_core_v2_production_wave3_v1 as wave
from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as stage_d_runtime
from our_system_phase2.runtime import cn_search_core_v2_production_wave3_v1 as runtime
from our_system_phase2.services.project_control_admission import sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash

PROBE_SECONDS = 30.0


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
    prefreeze = wave.verify_prefreeze(args.production_wave3_prefreeze.resolve())
    supply = _read(args.production_wave3_supply_audit.resolve())
    supply_hash = _verify(supply, "audit_payload_sha256", "Production Wave 3 supply")
    if (
        supply.get("status") != "PASS_ZERO_FINANCIAL_PRODUCTION_WAVE3_CHECKPOINT_SUPPLY_AUDIT"
        or int(supply.get("generated_total") or 0) != runtime.SUPPLY_PROBE_TOTAL
        or int(supply.get("unique_exact_count") or 0) != runtime.SUPPLY_PROBE_TOTAL
        or int(supply.get("prior_overlap_count", -1)) != 0
        or supply.get("synthetic_tell_used") is not False
        or supply.get("future_checkpoint_supply_fail_closed") is not True
        or bool(supply.get("candidate_evaluation_executed"))
        or bool(supply.get("financial_sidecar_read"))
        or str(supply.get("prefreeze_payload_sha256") or "") != str(prefreeze["prefreeze_payload_sha256"])
    ):
        raise RuntimeError("PRODUCTION_WAVE3_CANARY_SUPPLY_NOT_PASS")
    provisional = {
        "authorization_payload_sha256": stable_hash(
            {
                "role": "PRODUCTION_WAVE3_ZERO_FINANCIAL_CANARY",
                "prefreeze": prefreeze["prefreeze_payload_sha256"],
                "supply": supply_hash,
            }
        ),
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "source_prior_exact": dict(source_auth["source_prior_exact"]),
    }
    args.executor_workers = runtime.PRIMARY_EXECUTOR_WORKERS
    authority = stage1._load_authority(args, authorization=provisional, repo_sha=str(args.repo_sha))
    fields = tuple(map(str, supply["resource_field_surface"]["field_columns"]))
    if not fields:
        raise RuntimeError("PRODUCTION_WAVE3_CANARY_FIELDS_EMPTY")
    old_probe = large_fresh.RESOURCE_CANARY_PROBE_SECONDS
    try:
        large_fresh.RESOURCE_CANARY_PROBE_SECONDS = PROBE_SECONDS
        canary = large_fresh._resource_canary(
            authority,
            str(authority["input_binding"]["input_binding_sha256"]),
            runtime.PRIMARY_EXECUTOR_WORKERS,
            fields,
        )
    finally:
        large_fresh.RESOURCE_CANARY_PROBE_SECONDS = old_probe
    if (
        canary.get("status") != "PASS_ZERO_CANDIDATE_EVALUATION_RESOURCE_CANARY"
        or int(canary.get("requested_workers") or 0) != 24
        or int(canary.get("pagefile_pages_in_delta_bytes", -1)) != 0
        or int(canary.get("pagefile_pages_out_delta_bytes", -1)) != 0
        or canary.get("candidate_evaluation_executed") is not False
    ):
        raise RuntimeError("PRODUCTION_WAVE3_CANARY_FAIL")
    runner = repo / "scripts/run_cn_search_core_v2_production_wave3_v1.py"
    payload = {
        "schema_version": "cn_search_core_v2_production_wave3_resource_canary_v1",
        "status": "PASS",
        "repo_sha": str(args.repo_sha),
        "runner_source_file_sha256": sha256_file(runner),
        "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
        "supply_audit_payload_sha256": supply_hash,
        "source_stage_d_authorization_payload_sha256": str(source_auth["authorization_payload_sha256"]),
        "resource_profile": "SEARCH_DUAL_24",
        "requested_workers": 24,
        "field_column_count": len(fields),
        "field_columns": list(fields),
        "field_columns_sha256": stable_hash(list(fields)),
        "resource_probe": dict(canary),
        "candidate_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["official_canary_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-stage-d-authorization", type=Path, required=True)
    parser.add_argument("--production-wave3-prefreeze", type=Path, required=True)
    parser.add_argument("--production-wave3-supply-audit", type=Path, required=True)
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
    print(json.dumps({"status": payload["status"], "fields": payload["field_column_count"], "payload": payload["official_canary_payload_sha256"], "output": str(args.output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
