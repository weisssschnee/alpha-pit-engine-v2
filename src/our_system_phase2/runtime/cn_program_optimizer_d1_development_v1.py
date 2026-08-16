"""Prospective development-only D1 Program optimizer cohort.

The route executes the already-frozen TPE-bootstrap -> full Surrogate policy on
fresh development Programs only.  It has no validation/OOS/promotion authority.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from our_system_phase2.runtime.cn_program_optimizer_successor_benchmark_v1 import (
    AUTHORIZED_HOST,
    FROZEN_BASE_PROGRAM_COUNT,
    FROZEN_ENHANCED_PROGRAM_COUNT,
    FROZEN_PROGRAM_SPACE_COUNT,
    FROZEN_PROGRAM_SPACE_SHA256,
    SEEDS,
    SOURCE_COMPONENT_POOL_SHA256,
    SOURCE_EXECUTION_CONTRACT_PATH,
    SOURCE_EXECUTION_CONTRACT_SHA256,
    SOURCE_FREEZE_CLOSURE,
    SOURCE_FREEZE_CLOSURE_FILE_SHA256,
    SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256,
    SOURCE_FREEZE_ROOT,
    SOURCE_NODE_CAPACITY_PATH,
    SOURCE_NODE_CAPACITY_SHA256,
    SOURCE_RAW_RESERVOIR_SHA256,
    SOURCE_REGISTRY_PATH,
    SOURCE_REGISTRY_SHA256,
    SOURCE_RUN_CONTRACT_FILE_SHA256,
    SOURCE_RUN_CONTRACT_PAYLOAD_SHA256,
    SOURCE_TRAIN_FIELD_ROOT,
    SOURCE_TRAIN_PRICE_ROOT,
    SURROGATE_CONFIG,
    TPE_CONFIG,
)
from our_system_phase2.services.program_optimizer_d1_cohort_v1 import (
    D1_LOGICAL_RECORDS,
    D1_POLICY_ID,
    D1_SELECTOR_COUNTS,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    ProjectControlDenied,
    consume_active_admission,
    verify_campaign_authorization_binding,
    verify_consumed_admission_target,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


ROUTE_ID = "cn-program-optimizer-d1-development-v1"
CAMPAIGN_ID = "CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_V1"
CAMPAIGN_PROFILE = "cn_program_optimizer_d1_development_v1"
CAMPAIGN_ID_CONTINUATION = (
    "CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_CONTINUATION_V1"
)
AUTHORIZATION_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_d1_development_cohort_v1.json"
)
AUTHORIZATION_RELATIVE_PATH_CONTINUATION = Path(
    "runtime/run_plans/cn_program_optimizer_d1_development_continuation_cohort_v1.json"
)
POLICY_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_d1_development_policy_v1.json"
)
PRIOR_FREEZE_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_d1_next_prior_exact_freeze_20260816.json"
)
PRIOR_FREEZE_RELATIVE_PATH_CONTINUATION = Path(
    "runtime/run_plans/cn_program_optimizer_d1_postrun_prior_exact_freeze_20260816.json"
)
POLICY_PAYLOAD_SHA256 = (
    "3f2360953ae460d2c5078b049cbfb46b9da02ae0252aaa26bffee541b6bb107b"
)
PRIOR_EXACT_COUNT = 890
PRIOR_EXACT_IDENTITIES_SHA256 = (
    "f636b2991ede4eedb7ba0a0f75fdb502d516609c2636858ca6ff5aee820186cc"
)
PRIOR_FREEZE_PAYLOAD_SHA256 = (
    "2366701df20d2620371f85fe509418c7da53e5578a302c1f84879ddfaf591097"
)
REMAINING_PROSPECTIVE_ENHANCED = 2694
CONTINUATION_CAMPAIGN_PROFILE = (
    "cn_program_optimizer_d1_development_continuation_v1"
)
CONTINUATION_PRIOR_EXACT_COUNT = 1030
CONTINUATION_PRIOR_EXACT_IDENTITIES_SHA256 = (
    "1adc01934b8c9b11459cca4b40c2bb6926d7a35b2fe90bccf35a7a39f78cfec2"
)
CONTINUATION_PRIOR_FREEZE_PAYLOAD_SHA256 = (
    "a5431c84da70ae6151b70c3f4796a033fa798a5180856ed6efc886180329268d"
)
CONTINUATION_REMAINING_PROSPECTIVE_ENHANCED = 2554


@dataclass(frozen=True)
class _D1CampaignBinding:
    campaign_id: str
    campaign_profile: str
    authorization_relative_path: Path
    policy_relative_path: Path
    policy_payload_sha256: str
    prior_exact_count: int
    prior_exact_identities_sha256: str
    prior_freeze_relative_path: Path
    prior_freeze_payload_sha256: str
    remaining_prospective_enhanced_exact_count: int
    enforce_policy_prior_counts: bool = False


_CAMPAIGN_BINDINGS = {
    CAMPAIGN_ID: _D1CampaignBinding(
        campaign_id=CAMPAIGN_ID,
        campaign_profile=CAMPAIGN_PROFILE,
        authorization_relative_path=AUTHORIZATION_RELATIVE_PATH,
        policy_relative_path=POLICY_RELATIVE_PATH,
        policy_payload_sha256=POLICY_PAYLOAD_SHA256,
        prior_exact_count=PRIOR_EXACT_COUNT,
        prior_exact_identities_sha256=PRIOR_EXACT_IDENTITIES_SHA256,
        prior_freeze_relative_path=PRIOR_FREEZE_RELATIVE_PATH,
        prior_freeze_payload_sha256=PRIOR_FREEZE_PAYLOAD_SHA256,
        remaining_prospective_enhanced_exact_count=REMAINING_PROSPECTIVE_ENHANCED,
        enforce_policy_prior_counts=True,
    ),
    CAMPAIGN_ID_CONTINUATION: _D1CampaignBinding(
        campaign_id=CAMPAIGN_ID_CONTINUATION,
        campaign_profile=CONTINUATION_CAMPAIGN_PROFILE,
        authorization_relative_path=AUTHORIZATION_RELATIVE_PATH_CONTINUATION,
        policy_relative_path=POLICY_RELATIVE_PATH,
        policy_payload_sha256=POLICY_PAYLOAD_SHA256,
        prior_exact_count=CONTINUATION_PRIOR_EXACT_COUNT,
        prior_exact_identities_sha256=CONTINUATION_PRIOR_EXACT_IDENTITIES_SHA256,
        prior_freeze_relative_path=PRIOR_FREEZE_RELATIVE_PATH_CONTINUATION,
        prior_freeze_payload_sha256=CONTINUATION_PRIOR_FREEZE_PAYLOAD_SHA256,
        remaining_prospective_enhanced_exact_count=CONTINUATION_REMAINING_PROSPECTIVE_ENHANCED,
    ),
}


def _resolve_campaign_binding(campaign_id: str | None = None) -> _D1CampaignBinding:
    target = str(campaign_id or CAMPAIGN_ID)
    try:
        return _CAMPAIGN_BINDINGS[target]
    except KeyError as exc:
        raise ValueError("D1 authorization campaign not configured") from exc


def _read_self_hashed(path: Path, field: str, expected: str, label: str) -> dict[str, Any]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if claimed != stable_hash(body) or claimed != str(expected):
        raise ValueError(f"{label} hash drift")
    return payload


def _load_policy(repo_root: Path, *, binding: _D1CampaignBinding) -> dict[str, Any]:
    policy = _read_self_hashed(
        repo_root / binding.policy_relative_path,
        "policy_payload_sha256",
        binding.policy_payload_sha256,
        "D1 policy",
    )
    design = dict(policy.get("fixed_cohort_design") or {})
    selector_counts = dict(design.get("selector_counts") or {})
    universe = dict(policy.get("program_universe") or {})
    if (
        str(policy.get("policy_id") or "") != D1_POLICY_ID
        or str(policy.get("decision") or "")
        != "READY_FOR_SEPARATELY_AUTHORIZED_FRESH_DEVELOPMENT_COHORT"
        or str(policy.get("role") or "") != "PREFERRED_DEVELOPMENT_SEARCH_POLICY_ONLY"
        or str(policy.get("status") or "") != "FROZEN_NOT_RUN"
        or int(design.get("logical_records") or 0) != D1_LOGICAL_RECORDS
        or selector_counts != D1_SELECTOR_COUNTS
        or bool(design.get("template_routing"))
        or bool(design.get("dynamic_handoff"))
        or bool(design.get("dynamic_budget_reallocation"))
        or int(design.get("waves") or 0) != 20
        or str(policy.get("oos_authority") or "") != "NONE"
        or bool(policy.get("promotion_authorized"))
    ):
        raise ValueError("D1 policy contract drift")
    if binding.enforce_policy_prior_counts:
        if (
            int(universe.get("prior_exclusion_count") or 0)
            != binding.prior_exact_count
            or str(universe.get("prior_exclusion_exact_identities_sha256") or "")
            != binding.prior_exact_identities_sha256
            or int(universe.get("remaining_prospective_enhanced_exact_count") or 0)
            != binding.remaining_prospective_enhanced_exact_count
        ):
            raise ValueError("D1 policy contract drift")
    return policy


def _load_prior(
    repo_root: Path, *, binding: _D1CampaignBinding
) -> dict[str, Any]:
    prior = _read_self_hashed(
        repo_root / binding.prior_freeze_relative_path,
        "freeze_payload_sha256",
        binding.prior_freeze_payload_sha256,
        "D1 prior freeze",
    )
    ids = tuple(map(str, prior.get("combined_prior_exact_identities") or ()))
    if (
        len(ids) != binding.prior_exact_count
        or len(set(ids)) != binding.prior_exact_count
        or stable_hash(list(ids)) != binding.prior_exact_identities_sha256
        or int(prior.get("remaining_enhanced_program_count") or 0)
        != binding.remaining_prospective_enhanced_exact_count
    ):
        raise ValueError("D1 prior exact contract drift")
    # The generic development authority loader expects the canonical key name.
    prior["prior_exact_identities"] = list(ids)
    return prior


def authorization_payload_v1(
    repo_root: Path | None = None, *, campaign_id: str | None = None
) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    binding = _resolve_campaign_binding(campaign_id)
    policy = _load_policy(root, binding=binding)
    _load_prior(root, binding=binding)
    payload = {
        "schema_version": "cn_program_optimizer_d1_development_authorization_v1",
        "status": "CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_FROZEN_NOT_RUN",
        "campaign_id": binding.campaign_id,
        "campaign_profile": binding.campaign_profile,
        "project_control_route_id": ROUTE_ID,
        "execution_authorized": True,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "authorized_host": AUTHORIZED_HOST,
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "policy_binding": {
            "policy_id": D1_POLICY_ID,
            "policy_relative_path": str(POLICY_RELATIVE_PATH).replace("\\", "/"),
            "policy_payload_sha256": binding.policy_payload_sha256,
            "evidence_grade": str(
                dict(policy.get("evidence_binding") or {}).get("evidence_grade") or ""
            ),
            "selector_counts": dict(D1_SELECTOR_COUNTS),
            "logical_records": D1_LOGICAL_RECORDS,
            "wave_semantics": str(
                dict(policy["fixed_cohort_design"])["wave_semantics"]
            ),
            "template_routing": False,
            "dynamic_handoff": False,
            "dynamic_budget_reallocation": False,
        },
        "program_space": {
            "full_count": FROZEN_PROGRAM_SPACE_COUNT,
            "base_count": FROZEN_BASE_PROGRAM_COUNT,
            "enhanced_count": FROZEN_ENHANCED_PROGRAM_COUNT,
            "full_sha256": FROZEN_PROGRAM_SPACE_SHA256,
            "prior_exact_count": binding.prior_exact_count,
            "prior_exact_identities_sha256": binding.prior_exact_identities_sha256,
            "prior_freeze_relative_path": str(
                binding.prior_freeze_relative_path
            ).replace("\\", "/"),
            "prior_freeze_payload_sha256": binding.prior_freeze_payload_sha256,
            "remaining_prospective_enhanced_exact_count": binding.remaining_prospective_enhanced_exact_count,
            "prior_results_imported_as_optimizer_feedback": False,
        },
        "seeds": {
            "UNIFORM": int(SEEDS["UNIFORM"]),
            "TPE_BOOTSTRAP": int(SEEDS["TPE_CONTROL"]),
            "SURROGATE": int(SEEDS["TPE_TO_SURROGATE"]),
        },
        "tpe_config": dict(TPE_CONFIG),
        "surrogate_config": dict(SURROGATE_CONFIG),
        "source_evaluator_authority": {
            "source_freeze_root": SOURCE_FREEZE_ROOT,
            "source_freeze_closure": SOURCE_FREEZE_CLOSURE,
            "source_freeze_closure_file_sha256": SOURCE_FREEZE_CLOSURE_FILE_SHA256,
            "source_freeze_closure_payload_sha256": SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256,
            "source_run_contract_file_sha256": SOURCE_RUN_CONTRACT_FILE_SHA256,
            "source_run_contract_payload_sha256": SOURCE_RUN_CONTRACT_PAYLOAD_SHA256,
            "raw_reservoir_file_sha256": SOURCE_RAW_RESERVOIR_SHA256,
            "component_pool_file_sha256": SOURCE_COMPONENT_POOL_SHA256,
            "registry_path": SOURCE_REGISTRY_PATH,
            "registry_sha256": SOURCE_REGISTRY_SHA256,
            "node_capacity_path": SOURCE_NODE_CAPACITY_PATH,
            "node_capacity_sha256": SOURCE_NODE_CAPACITY_SHA256,
            "execution_contract_path": SOURCE_EXECUTION_CONTRACT_PATH,
            "execution_contract_sha256": SOURCE_EXECUTION_CONTRACT_SHA256,
            "train_field_root": SOURCE_TRAIN_FIELD_ROOT,
            "train_price_root": SOURCE_TRAIN_PRICE_ROOT,
        },
        "base_diversity": {
            "minimum_distinct_base_component_ids_per_template": 16,
            "maximum_variants_per_base_component_id_per_template": 4,
        },
        "ask_level_evidence_required": True,
        "restricted_reads": {
            "validation": 0,
            "holdout": 0,
            "historical_2023": 0,
            "forward_b": 0,
            "forward_2026": 0,
        },
        "oos_authority": "NONE",
        "promotion_authorized": False,
        "automatic_successor_authorized": False,
    }
    payload["authorization_payload_sha256"] = stable_hash(payload)
    return payload


def verify_authorization(path: Path) -> dict[str, Any]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop("authorization_payload_sha256", ""))
    if claimed != stable_hash(body):
        raise ValueError("D1 development authorization self-hash drift")
    campaign_id = str(payload.get("campaign_id") or "")
    if payload != authorization_payload_v1(campaign_id=campaign_id):
        raise ValueError("D1 development authorization contract drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(
        "cn-program-optimizer-d1-development-v1", {ACTION_LAUNCH, ACTION_RETRY}
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
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--executor-workers", type=int, default=8)
    parser.add_argument("--prefinancial-only", action="store_true")
    args = parser.parse_args(argv)

    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(admission, args.campaign_authorization)
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("D1 development authorization payload drift")
    if str(admission.get("requested_action") or "") not in {ACTION_LAUNCH, ACTION_RETRY}:
        raise ProjectControlDenied("D1 development action drift")

    from scripts.run_cn_program_optimizer_d1_development_v1 import (
        run_authorized_d1_cohort,
    )

    result = run_authorized_d1_cohort(
        args, admission=admission, authorization=authorization
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
