"""Project-Control entry for Large Fresh V2 (Typed Evolution + Uniform)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import (
    RESOURCE_CPU_THREADS,
    verify_authorization as verify_large_fresh_authorization,
)
from our_system_phase2.services.node_resource_governor import (
    validate_node_resource_lease_receipt,
)
from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
)
from our_system_phase2.services.unified_capability_registry import stable_hash
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    ProjectControlDenied,
    consume_active_admission,
    sha256_file,
    verify_campaign_authorization_binding,
    verify_consumed_admission_target,
)

ROUTE_ID = "cn-program-optimizer-large-fresh-development-v2"
CAMPAIGN_ID = "CN_PROGRAM_OPTIMIZER_LARGE_FRESH_DEVELOPMENT_V2"
CAMPAIGN_PROFILE = "cn_program_optimizer_large_fresh_development_v2"
AUTHORIZATION_SCHEMA = "cn_program_optimizer_large_fresh_development_authorization_v2"
FORMAL_SEARCH_AUTHORITY = "CATALOG_TYPED_EVOLUTION_AVAILABILITY_V2"
AUTHORIZATION_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_large_fresh_development_v2.json"
)


def verify_authorization(path: Path, *, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = verify_large_fresh_authorization(
        path,
        repo_root=repo_root,
        expected_schema_version=AUTHORIZATION_SCHEMA,
        expected_campaign_id=CAMPAIGN_ID,
        expected_campaign_profile=CAMPAIGN_PROFILE,
        expected_route_id=ROUTE_ID,
        expected_formal_search_authority=FORMAL_SEARCH_AUTHORITY,
        expected_formal_optimizer_arm=CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
    )
    design = dict(payload.get("search_design") or {})
    if (
        design.get("tpe_selection_authorized") is not False
        or design.get("tpe_feedback_authorized") is not False
        or design.get("cem_selection_authorized") is not False
        or design.get("cem_feedback_authorized") is not False
    ):
        raise ValueError("large fresh V2 challenger authorization drift")
    evidence = dict(payload.get("optimizer_evidence") or {})
    evidence_path = (root / Path(str(evidence.get("relative_path") or ""))).resolve()
    if (
        not evidence_path.is_relative_to(root)
        or not evidence_path.is_file()
        or sha256_file(evidence_path) != str(evidence.get("file_sha256") or "")
        or evidence.get("decision") != "EVOLUTION_PRIMARY_UNIFORM_RESERVE"
        or int(evidence.get("paired_seed_count") or 0) != 4
        or float(evidence.get("evolution_mean_productive_delta_at_840") or 0.0)
        != 24.25
        or int(evidence.get("evolution_positive_seed_count_at_840") or 0) != 4
    ):
        raise ValueError("large fresh V2 optimizer evidence binding drift")
    evidence_payload = json.loads(evidence_path.read_text(encoding="utf-8-sig"))
    evidence_body = dict(evidence_payload)
    claimed = str(evidence_body.pop("evidence_payload_sha256", ""))
    if (
        not claimed
        or stable_hash(evidence_body) != claimed
        or claimed != str(evidence.get("payload_sha256") or "")
    ):
        raise ValueError("large fresh V2 optimizer evidence payload drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(
        "cn-program-optimizer-large-fresh-development-v2",
        {ACTION_LAUNCH, ACTION_RETRY},
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--node-resource-lease-receipt", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)

    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(
        admission, args.campaign_authorization
    )
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("large fresh V2 authorization payload drift")
    validate_node_resource_lease_receipt(
        args.node_resource_lease_receipt.resolve(),
        expected_role="SEARCH",
        expected_cpu_threads=RESOURCE_CPU_THREADS,
    )

    from scripts.run_cn_program_optimizer_large_fresh_v2 import run

    result = run(args, admission=admission, authorization=authorization)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
