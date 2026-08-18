"""Build Large Fresh V2 authorization from frozen V1 supply + optimizer evidence."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import (
    verify_authorization as verify_v1_authorization,
)
from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v2 import (
    AUTHORIZATION_SCHEMA,
    CAMPAIGN_ID,
    CAMPAIGN_PROFILE,
    FORMAL_SEARCH_AUTHORITY,
    ROUTE_ID,
    PRIMARY_EXECUTOR_WORKERS,
    RESOURCE_CANARY_FIELD_COLUMNS_SHA256,
    RESOURCE_FALLBACK_EXECUTOR_WORKERS,
    RESOURCE_STRESS_RELATIVE_PATH,
    RESOURCE_STRESS_FILE_SHA256,
    RESOURCE_STRESS_PAYLOAD_SHA256,
)
from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build(args: argparse.Namespace) -> dict[str, Any]:
    repo = args.repo_root.resolve()
    base = verify_v1_authorization(args.baseline_authorization, repo_root=repo)
    evidence = _read(args.optimizer_evidence)
    evidence_body = dict(evidence)
    evidence_claimed = str(evidence_body.pop("evidence_payload_sha256", ""))
    if (
        not evidence_claimed
        or stable_hash(evidence_body) != evidence_claimed
        or evidence.get("status") != "PASS_REPORT_ONLY_OPTIMIZER_POLICY_EVIDENCE"
        or evidence.get("decision") != "EVOLUTION_PRIMARY_UNIFORM_RESERVE"
        or evidence.get("implementation_repo_sha") != "c8ff15049d29da00085cc7ce1e7b76b09ad58a37"
        or int(evidence.get("paired_seed_count") or 0) != 4
        or float(evidence.get("evolution_mean_productive_delta_at_168") or 0.0) != 3.0
        or int(evidence.get("evolution_positive_seed_count_at_168") or 0) != 4
        or float(evidence.get("evolution_mean_productive_delta_at_840") or 0.0) != 23.5
        or int(evidence.get("evolution_positive_seed_count_at_840") or 0) != 4
    ):
        raise ValueError("Large Fresh V2 optimizer evidence drift")

    resource_path = (repo / RESOURCE_STRESS_RELATIVE_PATH).resolve()
    resource = _read(resource_path)
    resource_body = dict(resource)
    resource_claimed = str(resource_body.pop("resource_evidence_payload_sha256", ""))
    if (
        _sha256(resource_path) != RESOURCE_STRESS_FILE_SHA256
        or resource_claimed != RESOURCE_STRESS_PAYLOAD_SHA256
        or stable_hash(resource_body) != resource_claimed
        or resource.get("status") != "PASS_RESOURCE_ONLY_REALISTIC_24_WORKER_STRESS"
        or int(resource.get("requested_workers") or 0) != PRIMARY_EXECUTOR_WORKERS
        or int(resource.get("distinct_worker_count") or 0) != PRIMARY_EXECUTOR_WORKERS
        or int(resource.get("field_column_count") or 0) != 47
        or resource.get("field_columns_sha256") != RESOURCE_CANARY_FIELD_COLUMNS_SHA256
        or int(resource.get("minimum_commit_headroom_bytes") or 0) < 24 * 1024**3
        or int(resource.get("pagefile_pages_in_delta_bytes", -1)) != 0
        or int(resource.get("pagefile_pages_out_delta_bytes", -1)) != 0
        or list(resource.get("orphan_worker_pids") or ())
        or resource.get("financial_candidate_evaluation_performed") is not False
    ):
        raise ValueError("Large Fresh V2 realistic resource stress drift")

    authorization = copy.deepcopy(base)
    baseline_payload = str(authorization.pop("authorization_payload_sha256"))
    authorization.update(
        {
            "schema_version": AUTHORIZATION_SCHEMA,
            "campaign_id": CAMPAIGN_ID,
            "campaign_profile": CAMPAIGN_PROFILE,
            "project_control_route_id": ROUTE_ID,
            "formal_search_authority": FORMAL_SEARCH_AUTHORITY,
            "baseline_v1_authorization_payload_sha256": baseline_payload,
        }
    )
    design = dict(authorization["search_design"])
    design.update(
        {
            "formal_optimizer_arm": CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
            "formal_optimizer_policy": {
                "algorithm_origin": "CRYPTO_TYPED_EVOLUTION_V2_PORT",
                "warmup": 32,
                "tournament_size": 4,
                "population_limit": 256,
                "template_cell_limit": 64,
                "gene_mutation_probability": 0.55,
                "compatible_skeleton_mutation_probability": 0.25,
                "homologous_crossover_probability": 0.20,
                "minimum_mutated_factors": 1,
                "maximum_mutated_factors": 3,
                "duplicate_resample_limit": 64,
            },
            "formal_optimizer_checkpoints_per_macro": 6,
            "uniform_reserve_checkpoints_per_macro": 1,
            "tpe_selection_authorized": False,
            "tpe_feedback_authorized": False,
            "cem_selection_authorized": False,
            "cem_feedback_authorized": False,
        }
    )
    authorization["search_design"] = design
    resource_contract = dict(authorization["resource_contract"])
    resource_contract.update(
        {
            "primary_executor_workers": PRIMARY_EXECUTOR_WORKERS,
            "pre_evaluation_fallback_executor_workers": RESOURCE_FALLBACK_EXECUTOR_WORKERS,
        }
    )
    authorization["resource_contract"] = resource_contract
    authorization["resource_evidence"] = {
        "relative_path": str(RESOURCE_STRESS_RELATIVE_PATH).replace("\\", "/"),
        "file_sha256": RESOURCE_STRESS_FILE_SHA256,
        "payload_sha256": RESOURCE_STRESS_PAYLOAD_SHA256,
        "requested_workers": PRIMARY_EXECUTOR_WORKERS,
        "distinct_worker_count": int(resource["distinct_worker_count"]),
        "field_column_count": int(resource["field_column_count"]),
        "field_columns_sha256": str(resource["field_columns_sha256"]),
        "minimum_available_physical_bytes": int(resource["minimum_available_physical_bytes"]),
        "minimum_commit_headroom_bytes": int(resource["minimum_commit_headroom_bytes"]),
        "maximum_committed_bytes": int(resource["maximum_committed_bytes"]),
        "maximum_process_tree_rss_bytes": int(resource["maximum_process_tree_rss_bytes"]),
        "maximum_pagefile_used_bytes": int(resource["maximum_pagefile_used_bytes"]),
        "pagefile_pages_in_delta_bytes": int(resource["pagefile_pages_in_delta_bytes"]),
        "pagefile_pages_out_delta_bytes": int(resource["pagefile_pages_out_delta_bytes"]),
        "usage": "RESOURCE_ONLY_NO_ECONOMIC_REUSE",
    }
    authorization["optimizer_evidence"] = {
        "relative_path": str(args.optimizer_evidence.resolve().relative_to(repo)).replace("\\", "/"),
        "file_sha256": _sha256(args.optimizer_evidence),
        "payload_sha256": evidence_claimed,
        "decision": "EVOLUTION_PRIMARY_UNIFORM_RESERVE",
        "implementation_repo_sha": evidence["implementation_repo_sha"],
        "paired_seed_count": 4,
        "evolution_mean_productive_delta_at_168": 3.0,
        "evolution_positive_seed_count_at_168": 4,
        "evolution_mean_productive_delta_at_840": 23.5,
        "evolution_positive_seed_count_at_840": 4,
        "evidence_role": evidence["evidence_role"],
    }
    authorization["authorization_payload_sha256"] = stable_hash(authorization)
    _write(args.authorization_output, authorization)
    return {
        "status": authorization["status"],
        "campaign_id": CAMPAIGN_ID,
        "formal_optimizer_arm": CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
        "logical_record_cap": int(authorization["search_design"]["logical_record_cap"]),
        "optimizer_evidence_payload_sha256": evidence_claimed,
        "authorization_payload_sha256": authorization["authorization_payload_sha256"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--baseline-authorization", type=Path, required=True)
    parser.add_argument("--optimizer-evidence", type=Path, required=True)
    parser.add_argument("--authorization-output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
