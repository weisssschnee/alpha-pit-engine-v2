"""Prospective development-only Program optimizer successor benchmark.

This route binds the fair four-policy benchmark to the already-closed V1
Program space and development evaluator authorities.  It has no OOS or
promotion authority.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.program_optimizer_successor_benchmark_v1 import (
    BOOTSTRAP_WAVES,
    MAX_GATE_RAW_ATTEMPTS_PER_ECONOMIC_ASK,
    POLICIES,
    POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY_DENOMINATOR,
    POST_BOOTSTRAP_EFFICIENCY_DENOMINATOR,
    TOTAL_POLICY_EFFICIENCY_DENOMINATOR,
    TOTAL_WAVES,
    UNIFORM_FLOOR_WAVES,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    ProjectControlDenied,
    consume_active_admission,
    sha256_file,
    verify_campaign_authorization_binding,
    verify_consumed_admission_target,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


ROUTE_ID = "cn-program-optimizer-successor-benchmark-v1"
CAMPAIGN_ID = "CN_PROGRAM_OPTIMIZER_SUCCESSOR_BENCHMARK_V1"
CAMPAIGN_PROFILE = "cn_program_optimizer_successor_benchmark_v1"
AUTHORIZATION_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_successor_benchmark_v1.json"
)
PRIOR_FREEZE_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_successor_v1_prior_exact_freeze_20260816.json"
)
AUTHORIZED_HOST = "DESKTOP-77OPJ6F"

FROZEN_PROGRAM_SPACE_COUNT = 3616
FROZEN_BASE_PROGRAM_COUNT = 32
FROZEN_ENHANCED_PROGRAM_COUNT = 3584
FROZEN_PROGRAM_SPACE_SHA256 = (
    "86d9bce8f7bdc75e55c791b6e101ec4beefe093e346abfc39c76ee1c30f354e0"
)
PRIOR_EXACT_COUNT = 504
PRIOR_EXACT_IDENTITIES_SHA256 = (
    "b85b216dce6476cb1601ab37b5e2503847450b8f7f18ed285eb217262b642424"
)
PRIOR_FREEZE_PAYLOAD_SHA256 = (
    "55814a1d20de121f6548f458f1c80c9e667b2df4df1ddca715f88f750c8ce406"
)
SOURCE_FREEZE_ROOT = (
    r"D:\ChengboRemote\runtime\cn_program_optimizer_tournament_batch_feasibility_retry_20260815_11c5d08\prefinancial_freeze_stage01"
)
SOURCE_FREEZE_CLOSURE = "PROGRAM_OPTIMIZER_TOURNAMENT_PREFINANCIAL_FREEZE_COMPLETE.json"
SOURCE_FREEZE_CLOSURE_FILE_SHA256 = (
    "c8a621f2e9344cbfb7ff5942351f7c15a88dca811a71cbc5ae210469db2fcad0"
)
SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256 = (
    "54ec9a239dac99cf779b187c113dba672b2ba2cc0865c56724f9e762d2110a18"
)
SOURCE_RUN_CONTRACT_FILE_SHA256 = (
    "b0af1e2c08f6e877a9f5ad4c35d1192756375f5b2a5bcfed8c2ae89df9d65452"
)
SOURCE_RUN_CONTRACT_PAYLOAD_SHA256 = (
    "6ed3da590c83f90f84d5488e74a713bdcc14480b07e14b8ac44e1788e230d282"
)
SOURCE_RAW_RESERVOIR_SHA256 = (
    "c284a095171b3634083484a0e810d8a8bbc6876328adf1972dc473e4ae7f5608"
)
SOURCE_COMPONENT_POOL_SHA256 = (
    "64ff0e47dab2d92a2bfc53049bf2fc9d42ae2bc757c1dc5e81139e059ee9d12e"
)
SOURCE_REGISTRY_PATH = (
    r"D:\ChengboRemote\workspace\alpha_pit_search_v2_1b91c88_20260812\runtime\field_registry\cn_unified_capability_registry_v3_20260717\unified_capability_registry.json"
)
SOURCE_REGISTRY_SHA256 = (
    "449fea36daaba8e501bd03d052497b881ac03c601cee701f3ebfe069c7ae61d7"
)
SOURCE_NODE_CAPACITY_PATH = (
    r"D:\ChengboRemote\workspace\alpha_pit_search_v2_1b91c88_20260812\runtime\run_plans\cn_alpha_node_resource_profiles_v1.json"
)
SOURCE_NODE_CAPACITY_SHA256 = (
    "48abcebe9324fdfdb4923b5de5b1e85fff28ca310e80ff90f22b0c403bb308a5"
)
SOURCE_EXECUTION_CONTRACT_PATH = (
    r"D:\ChengboRemote\runtime\cn_finalist_strict_train_replay_continuous_dual_32_20260802_2340_023363c\prepared\replay_then_oos_execution_contract.json"
)
SOURCE_EXECUTION_CONTRACT_SHA256 = (
    "23d756eeda5341c9a87091c3b6a420d6314acdb6399c4bd64ee046df96552ec9"
)
SOURCE_TRAIN_FIELD_ROOT = (
    r"D:\ChengboRemote\runtime\cn_search_engine_v2_canary_replacement_20260812_1b91c88\prefinancial_freeze\materialized_session_sidecar"
)
SOURCE_TRAIN_PRICE_ROOT = (
    r"D:\ChengboRemote\runtime\cn_finalist_strict_train_replay_continuous_dual_32_20260802_2340_023363c\sidecars\train_session_fields"
)

SEEDS = {
    "UNIFORM": 826001,
    "TPE_CONTROL": 826013,
    "TPE_TO_SURROGATE": 826017,
    "FEASIBILITY_GATED_TPE": 826019,
}
TPE_CONFIG = {"n_startup_trials": 24, "n_ei_candidates": 64}
SURROGATE_CONFIG = {
    "cold_start_asks": 24,
    "candidate_pool_size": 256,
    "n_estimators": 256,
    "min_samples_leaf": 2,
    "exploration_beta": 1.0,
}


def _load_prior_freeze(repo_root: Path) -> dict[str, Any]:
    path = (repo_root / PRIOR_FREEZE_RELATIVE_PATH).resolve()
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop("freeze_payload_sha256", ""))
    if claimed != stable_hash(body) or claimed != PRIOR_FREEZE_PAYLOAD_SHA256:
        raise ValueError("successor prior-exact freeze hash drift")
    if (
        int(payload.get("unique_prior_exact_identity_count") or 0) != PRIOR_EXACT_COUNT
        or str(payload.get("prior_exact_identities_sha256") or "")
        != PRIOR_EXACT_IDENTITIES_SHA256
    ):
        raise ValueError("successor prior-exact freeze contract drift")
    return payload


def authorization_payload_v1(repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    prior = _load_prior_freeze(root)
    payload = {
        "schema_version": "cn_program_optimizer_successor_benchmark_authorization_v1",
        "status": "PROGRAM_OPTIMIZER_SUCCESSOR_BENCHMARK_FROZEN_NOT_RUN",
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "project_control_route_id": ROUTE_ID,
        "execution_authorized": True,
        "permitted_project_control_actions": [ACTION_LAUNCH, ACTION_RETRY],
        "authorized_host": AUTHORIZED_HOST,
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "program_space": {
            "full_count": FROZEN_PROGRAM_SPACE_COUNT,
            "base_count": FROZEN_BASE_PROGRAM_COUNT,
            "enhanced_count": FROZEN_ENHANCED_PROGRAM_COUNT,
            "full_sha256": FROZEN_PROGRAM_SPACE_SHA256,
            "prior_exact_count": PRIOR_EXACT_COUNT,
            "prior_exact_identities_sha256": PRIOR_EXACT_IDENTITIES_SHA256,
            "prior_freeze_relative_path": str(PRIOR_FREEZE_RELATIVE_PATH).replace("\\", "/"),
            "prior_freeze_payload_sha256": PRIOR_FREEZE_PAYLOAD_SHA256,
            "prospective_enhanced_available_count": (
                FROZEN_ENHANCED_PROGRAM_COUNT - PRIOR_EXACT_COUNT
            ),
            "prior_results_imported_as_optimizer_feedback": False,
        },
        "policies": list(POLICIES),
        "logical_design": {
            "waves": TOTAL_WAVES,
            "templates_per_wave": 7,
            "policies_per_wave": 4,
            "logical_records_per_policy": TOTAL_POLICY_EFFICIENCY_DENOMINATOR,
            "total_logical_records": TOTAL_POLICY_EFFICIENCY_DENOMINATOR * 4,
            "bootstrap_waves": BOOTSTRAP_WAVES,
            "uniform_floor_waves": list(UNIFORM_FLOOR_WAVES),
            "post_bootstrap_denominator": POST_BOOTSTRAP_EFFICIENCY_DENOMINATOR,
            "policy_specific_optimized_denominator": (
                POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY_DENOMINATOR
            ),
            "wave_semantics": "BLIND_SELECT_ALL_POLICIES_AND_TEMPLATES_THEN_PHYSICAL_UNION_THEN_TELL",
            "template_serialization": "DETERMINISTIC_ONE_POSITION_ROTATION_PER_WAVE",
            "cross_policy_virtual_depletion": False,
            "physical_exact_deduplication": True,
            "result_visibility_before_logical_selection_freeze": False,
            "adaptive_budget_reallocation": False,
            "racing": False,
        },
        "d1": {
            "bootstrap_observations": 28,
            "handoff_before_observation": 29,
            "bootstrap_selector": "COMMON_TPE_PLUS_COMMON_UNIFORM_FLOOR",
            "post_handoff_selector": "STRUCTURED_SURROGATE_FULL_ACQUISITION",
        },
        "d2": {
            "bootstrap_observations": 28,
            "feasibility_head_only": True,
            "gate_fraction": 0.5,
            "gate_ranking": "P_ADMISSION_DESC_EXACT_IDENTITY_ASC",
            "gate_outside_action": "NON_ECONOMIC_TPE_FAIL_AND_REASK",
            "max_gate_raw_attempts_per_economic_ask": (
                MAX_GATE_RAW_ATTEMPTS_PER_ECONOMIC_ASK
            ),
            "ordinary_projection_after_gate": True,
            "global_fallback": False,
        },
        "seeds": dict(SEEDS),
        "tpe_config": dict(TPE_CONFIG),
        "surrogate_config": dict(SURROGATE_CONFIG),
        "base_diversity": {
            "minimum_distinct_base_component_ids_per_template": 16,
            "maximum_variants_per_base_component_id_per_template": 4,
        },
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
        "prior_freeze_summary": {
            "per_arm": prior["per_arm"],
            "per_template": prior["per_template"],
        },
    }
    payload["authorization_payload_sha256"] = stable_hash(payload)
    return payload


def verify_authorization(path: Path) -> dict[str, Any]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop("authorization_payload_sha256", ""))
    if claimed != stable_hash(body):
        raise ValueError("successor benchmark authorization self-hash drift")
    if payload != authorization_payload_v1():
        raise ValueError("successor benchmark authorization contract drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(
        "cn-program-optimizer-successor-benchmark-v1", {ACTION_LAUNCH, ACTION_RETRY}
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
    verified = verify_campaign_authorization_binding(
        admission, args.campaign_authorization
    )
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("successor benchmark authorization payload drift")
    if str(admission.get("requested_action") or "") not in {ACTION_LAUNCH, ACTION_RETRY}:
        raise ProjectControlDenied("successor benchmark action drift")

    from scripts.run_cn_program_optimizer_successor_benchmark_v1 import (
        run_authorized_successor_benchmark,
    )

    result = run_authorized_successor_benchmark(
        args, admission=admission, authorization=authorization
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
