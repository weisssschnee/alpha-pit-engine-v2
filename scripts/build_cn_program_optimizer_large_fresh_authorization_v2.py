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
        or int(evidence.get("paired_seed_count") or 0) != 4
        or float(evidence.get("evolution_mean_productive_delta_at_840") or 0.0) != 24.25
        or int(evidence.get("evolution_positive_seed_count_at_840") or 0) != 4
    ):
        raise ValueError("Large Fresh V2 optimizer evidence drift")

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
    authorization["optimizer_evidence"] = {
        "relative_path": str(args.optimizer_evidence.resolve().relative_to(repo)).replace("\\", "/"),
        "file_sha256": _sha256(args.optimizer_evidence),
        "payload_sha256": evidence_claimed,
        "decision": "EVOLUTION_PRIMARY_UNIFORM_RESERVE",
        "paired_seed_count": 4,
        "evolution_mean_productive_delta_at_840": 24.25,
        "evolution_positive_seed_count_at_840": 4,
        "tpe_completed_paired_seed_count": len(evidence.get("tpe_completed_paired_168") or ()),
        "tpe_slow_path_seed_offsets": list(evidence.get("tpe_slow_path_seed_offsets") or ()),
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
