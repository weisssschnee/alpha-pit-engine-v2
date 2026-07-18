"""Freeze the supplemental CN development-discovery campaign.

The campaign reuses the completed 77o generation/sketch/admission baseline and
only generates deltas for newly connected roots or independently implemented
challengers.  It deliberately does not provide a full-pack replay path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.compositional_generation_epoch import (  # noqa: E402
    SEARCHABLE_ROUTES,
)
from our_system_phase2.services.unified_capability_registry import (  # noqa: E402
    UnifiedCapabilityRegistry,
    stable_hash,
)


BASELINE_REPO_SHA = "ac81237a8c0d0759c965cb3eddbc3645ce2c336f"
BASELINE_ROOT_SCOPE_HASH = "5ff4e84f137ccffee31944f12be0045ae653573186915922c9583854600ae1a2"
BASELINE_ROOT_SCOPE_BLOB_SHA = "f73a40297a3927387d490bf9d2490f4693edfcb4"
BASELINE_ROOT_SCOPE_PATH = "runtime/run_plans/cn_core_pack_development_discovery_v1.json"
BASELINE_RUNTIME_ROOT = (
    "D:/ChengboRemote/runtime/"
    "cn_core_pack_aggressive_discovery_20260718_595c5fc"
)

DEFAULT_SCOPE = REPO / BASELINE_ROOT_SCOPE_PATH
DEFAULT_REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
DEFAULT_BASE_PLAN = REPO / "runtime/run_plans/cn_compositional_nline_bounded_search_epoch1_v1.json"
DEFAULT_LANES = REPO / "runtime/run_plans/nextgen_dark_hypothesis_lane_registry_v1.json"
DEFAULT_BENCHMARKS = REPO / "runtime/run_plans/nextgen_dark_benchmark_registry_v1.json"
DEFAULT_GAP_AUDIT = REPO / "runtime/cn_core_pack_generator_capacity_v1_20260718/field_root_review.json"
DEFAULT_SUPPLEMENTAL_SCOPE = REPO / "runtime/run_plans/cn_core_pack_supplemental_root_scope_v1.json"
DEFAULT_OUTPUT = REPO / "runtime/run_plans/cn_core_pack_delta_discovery_campaign_v1.json"

SUPPLEMENTAL_ROOTS = {
    "ctx_hfq_is_st": "MARKET_REGIME_CONDITION",
    "ctx_hfq_prev_is_limit_up": "MARKET_REGIME_CONDITION",
    "fund_disclosure_balance_age_sessions": "SLOW_CROSS_SECTIONAL_LEVEL",
    "fund_disclosure_cashflow_age_sessions": "SLOW_CROSS_SECTIONAL_LEVEL",
    "fund_disclosure_holder_age_sessions": "SLOW_CROSS_SECTIONAL_LEVEL",
    "fund_disclosure_profit_age_sessions": "SLOW_CROSS_SECTIONAL_LEVEL",
    "state_close_range_location_sign": "INTRADAY_STATE_TRANSITION",
}
RUNTIME_ACTIVATION_REQUIREMENTS = {
    **{
        field_id: {
            "receipt_type": "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT",
            "full_development_assembly": "FULL_DEVELOPMENT_SESSION_PANEL",
        }
        for field_id in SUPPLEMENTAL_ROOTS
        if field_id.startswith("fund_disclosure_")
    },
    "state_close_range_location_sign": {
        "receipt_type": "FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
        "full_development_assembly": "FULL_DEVELOPMENT_ACTIVE_PANEL",
    },
}

CHALLENGERS = (
    "typed_ast",
    "cem",
    "rx_ucb",
    "uct_mcts",
    "evolutionary",
    "surrogate",
    "llm_proposal_repair",
    "benchmark_competitor",
)
ADAPTIVE_CHALLENGERS = {
    "cem",
    "rx_ucb",
    "uct_mcts",
    "evolutionary",
    "surrogate",
}
ROUTE_PROPOSALS_PER_CHALLENGER = 1_024
ROUTE_ADMISSIONS_PER_CHALLENGER = 32

BASELINE_ARTIFACTS = (
    (
        "generation_manifest",
        "CN_GENERATION_ARTIFACT_MANIFEST.json",
        "5d03322b2ef71a0035faa4b4143e6c2b738101f8144c47687cdc38fd511f16f5",
    ),
    (
        "generation_summary",
        "CN_GENERATION_EPOCH_SUMMARY.json",
        "cefb5cc60abe03e1772cc3f4945390f3f43d81fb374a883a70b40e90c7a1684f",
    ),
    (
        "pair_receipts",
        "CN_PAIR_RECEIPTS.jsonl",
        "d332d501176258488ee26845a911a37f3dc7451f9e6c0b6bc23b504c572e10e9",
    ),
    (
        "signal_sketch_diagnostic",
        "CN_SIGNAL_SKETCH_DIAGNOSTIC.json",
        "8d40318d6820a467d14df7768c6a371d9f0326b20f9a536d727d1aea6ad7a3d7",
    ),
    (
        "signal_cluster_registry",
        "CN_SIGNAL_CLUSTER_REGISTRY.csv",
        "159b216da13b073db55153337a98f9d9eb0680bab5366e462f9639a07f73b83d",
    ),
    (
        "behavior_cluster_summary",
        "CN_BEHAVIOR_CLUSTERS.json",
        "74019646e5490350f4d70388f15452292727e402b7448f6174a5733a7e57b75e",
    ),
    (
        "diversity_admission",
        "CN_DIVERSITY_ADMISSION.csv",
        "1af56e467005e7bfe87a13d9c32d90b1ebeac31a73a264643c212df8eafb5bd0",
    ),
    (
        "strict_64_execution_receipt",
        "strict_wave_00064_evaluation_r5_asymmetric/CN_PHASE_E_COMBINED_EXECUTION_RECEIPT.json",
        "f8b467cc7f1296a35303a9d91da754fffb27b91606342d8a70318f8d34c61b60",
    ),
    (
        "strict_64_analysis",
        "strict_wave_00064_evaluation_r5_asymmetric/CN_STRICT_WAVE_00064_ANALYSIS.json",
        "decfb125f3acf0bd4997fee2c04c46776e127414f04dd16db9cb1600b5c1213e",
    ),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _git(repo: Path, *args: str, binary: bool = False) -> Any:
    return subprocess.check_output(
        ["git", *args],
        cwd=repo,
        text=not binary,
        encoding=None if binary else "utf-8",
    )


def _historical_json(repo: Path, repo_sha: str, relative_path: str) -> dict[str, Any]:
    raw = _git(repo, "show", f"{repo_sha}:{relative_path}", binary=True)
    return json.loads(raw.decode("utf-8"))


def _searchable_allowlists(scope: Mapping[str, Any]) -> dict[str, list[str]]:
    source = dict(scope["route_root_allowlists"])
    return {route: sorted(map(str, source[route])) for route in SEARCHABLE_ROUTES}


def _challenger_specs() -> list[dict[str, Any]]:
    route_proposals = {route: ROUTE_PROPOSALS_PER_CHALLENGER for route in SEARCHABLE_ROUTES}
    route_admissions = {route: ROUTE_ADMISSIONS_PER_CHALLENGER for route in SEARCHABLE_ROUTES}
    rows: list[dict[str, Any]] = []
    for ordinal, challenger_id in enumerate(CHALLENGERS):
        adaptive = challenger_id in ADAPTIVE_CHALLENGERS
        rows.append(
            {
                "challenger_id": challenger_id,
                "adaptive": adaptive,
                "activation_state": "PENDING_REAL_UNIFIED_ADAPTER_RECEIPT",
                "proposal_budget_by_route": dict(route_proposals),
                "admission_budget_by_route": dict(route_admissions),
                "strict_pair_budget_after_256": 16,
                "archive_namespace": f"cn_delta/challenger/{challenger_id}",
                "lineage_namespace": f"cn_delta.{challenger_id}",
                "seed_base": 2_000_003 + ordinal * 104_729,
                "matched_nonadaptive_control": {
                    "policy_id": "stratified_typed_random",
                    "proposal_budget_by_route": dict(route_proposals),
                    "admission_budget_by_route": dict(route_admissions),
                    "strict_pair_budget_after_256": 16,
                    "archive_namespace": f"cn_delta/control/{challenger_id}",
                    "lineage_namespace": f"cn_delta.control.{challenger_id}",
                    "seed_base": 20_000_003 + ordinal * 104_729,
                },
                "activation_requirements": [
                    "UNIFIED_REGISTRY_AND_TYPED_GRAMMAR_ADAPTER",
                    "SOURCE_PATH_AND_SHA256_BOUND",
                    "DETERMINISTIC_PROPOSAL_TRANSCRIPT_REPLAY",
                    "PROPOSAL_DISTRIBUTION_DIFFERS_FROM_MATCHED_CONTROL",
                    "INDEPENDENT_CANDIDATE_SUBMISSION_VALIDATION",
                    "NO_FAKE_POLICY_LABELS",
                ],
            }
        )
    return rows


def _route_delta_budget(new_roots: Mapping[str, list[str]]) -> dict[str, dict[str, int]]:
    output: dict[str, dict[str, int]] = {}
    for route in SEARCHABLE_ROUTES:
        root_count = len(new_roots[route])
        proposal = min(32_768, max(8_192, root_count * 4_096)) if root_count else 0
        output[route] = {
            "new_root_count": root_count,
            "proposal_budget": proposal,
            "admission_budget": min(512, max(64, root_count * 32)) if root_count else 0,
        }
    return output


def build_supplemental_scope(
    *,
    repo: Path = REPO,
    registry_path: Path = DEFAULT_REGISTRY,
    gap_audit_path: Path = DEFAULT_GAP_AUDIT,
) -> dict[str, Any]:
    registry = UnifiedCapabilityRegistry.read(registry_path)
    audit_rows = _json(gap_audit_path)
    if not isinstance(audit_rows, list):
        raise ValueError("generator gap audit must be a row list")
    by_id = {str(row.get("field_id")): row for row in audit_rows}
    roots: list[dict[str, Any]] = []
    for field_id, route_id in sorted(SUPPLEMENTAL_ROOTS.items()):
        row = by_id.get(field_id)
        if row is None:
            raise ValueError(f"supplemental root absent from gap audit: {field_id}")
        if not bool(row.get("registry_present")) or not bool(row.get("registry_search_eligible")):
            raise ValueError(f"supplemental root is not registry eligible: {field_id}")
        field_role = str(row.get("field_role") or "")
        if not bool(row.get("information_qualified")) and field_role not in {
            "condition-only",
            "state-only",
        }:
            raise ValueError(f"supplemental payload root lacks non-performance qualification: {field_id}")
        if route_id not in set(map(str, row.get("eligible_routes") or [])):
            raise ValueError(f"supplemental route mismatch: {field_id}")
        if row.get("generator_observed_routes"):
            raise ValueError(f"historical gap audit unexpectedly observed root: {field_id}")
        if str(row.get("pit_status") or "") == "PIT_CONTRACT_UNRESOLVED":
            raise ValueError(f"supplemental root has unresolved PIT contract: {field_id}")
        registry.resolve(field_id)
        roots.append(
            {
                "field_id": field_id,
                "route_id": route_id,
                "field_role": field_role,
                "entity_scope": str(row.get("entity_scope") or ""),
                "pit_status": str(row.get("pit_status") or ""),
                "gap_reason": str(row.get("review_reason") or ""),
                "information_qualified": bool(row.get("information_qualified")),
                "generator_exposure_status": "PENDING_CURRENT_SOURCE_COMPILE_PROOF",
                **(
                    {
                        "materialization_status": "NOT_MATERIALIZED",
                        "signal_sketch_allowed": False,
                        "strict_evaluation_allowed": False,
                        "runtime_ready": False,
                        "runtime_activation": (
                            "CANONICAL_FULL_DEVELOPMENT_RECEIPT_REQUIRED_PER_ROOT"
                        ),
                        "required_receipt": (
                            "FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT"
                            if field_id == "state_close_range_location_sign"
                            else "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT"
                        ),
                    }
                    if field_id == "state_close_range_location_sign"
                    or field_id.startswith("fund_disclosure_")
                    else {}
                ),
            }
        )
    scope: dict[str, Any] = {
        "contract_version": "cn_core_pack_supplemental_root_scope_v1",
        "status": "AUTHORIZED_APPEND_ONLY_SUPPLEMENTAL_SCOPE",
        "baseline_scope": {
            "repo_sha": BASELINE_REPO_SHA,
            "path": BASELINE_ROOT_SCOPE_PATH,
            "root_scope_hash": BASELINE_ROOT_SCOPE_HASH,
            "git_blob_sha": BASELINE_ROOT_SCOPE_BLOB_SHA,
            "mutation": "FORBIDDEN",
        },
        "source_gap_audit": {
            "path": str(gap_audit_path.relative_to(repo)).replace("\\", "/"),
            "sha256": _sha256(gap_audit_path),
            "claim": "NON_PERFORMANCE_GENERATOR_EXPOSURE_GAPS_ONLY",
        },
        "registry_hash": registry.registry_hash,
        "root_count": len(roots),
        "route_count": len({row["route_id"] for row in roots}),
        "roots": roots,
        "runtime_activation_contract": {
            "default": "FROZEN",
            "scope_mutation_allowed": False,
            "activation_unit": "ONE_ROOT_ONE_VERIFIED_RECEIPT",
            "candidate_receipt_hash_injection_required": True,
            "requirements_by_field": RUNTIME_ACTIVATION_REQUIREMENTS,
        },
        "plate_industry_roots": [],
        "reward_or_performance_used": False,
        "append_only": True,
        "contract_hash": "",
    }
    scope["contract_hash"] = stable_hash(
        {key: value for key, value in scope.items() if key != "contract_hash"}
    )
    return scope


def build_contract(
    *,
    repo: Path = REPO,
    scope_path: Path = DEFAULT_SCOPE,
    registry_path: Path = DEFAULT_REGISTRY,
    base_plan_path: Path = DEFAULT_BASE_PLAN,
    lane_registry_path: Path = DEFAULT_LANES,
    benchmark_registry_path: Path = DEFAULT_BENCHMARKS,
    supplemental_scope_path: Path = DEFAULT_SUPPLEMENTAL_SCOPE,
) -> dict[str, Any]:
    scope = _json(scope_path)
    baseline_scope = _historical_json(repo, BASELINE_REPO_SHA, BASELINE_ROOT_SCOPE_PATH)
    supplemental_scope = _json(supplemental_scope_path)
    registry = UnifiedCapabilityRegistry.read(registry_path)
    base_plan = _json(base_plan_path)
    lane_registry = _json(lane_registry_path)
    benchmark_registry = _json(benchmark_registry_path)

    if bool(scope.get("execution_authorized")):
        raise ValueError("source scope must remain non-executable")
    if any(
        int(scope.get(key) or 0) != 0
        for key in (
            "active_proposal_budget",
            "active_admission_budget",
            "active_strict_evaluation_budget",
        )
    ):
        raise ValueError("source scope budgets must remain zero")
    if str(scope["registry_hash"]) != registry.registry_hash:
        raise ValueError("source scope and unified registry hashes differ")

    baseline_allowlists = _searchable_allowlists(baseline_scope)
    observed_baseline_blob = str(
        _git(
            repo,
            "rev-parse",
            f"{BASELINE_REPO_SHA}:{BASELINE_ROOT_SCOPE_PATH}",
        )
    ).strip()
    if observed_baseline_blob != BASELINE_ROOT_SCOPE_BLOB_SHA:
        raise ValueError("historical baseline scope Git blob drift")
    if stable_hash(baseline_allowlists) != BASELINE_ROOT_SCOPE_HASH:
        raise ValueError("historical baseline root scope hash drift")
    if (
        str(scope.get("contract_hash") or "")
        != str(baseline_scope.get("contract_hash") or "")
        or stable_hash(_searchable_allowlists(scope)) != BASELINE_ROOT_SCOPE_HASH
    ):
        raise ValueError("local historical v1 scope was rewritten instead of supplemented")
    unsigned_supplemental = {
        key: value for key, value in supplemental_scope.items() if key != "contract_hash"
    }
    if supplemental_scope.get("contract_hash") != stable_hash(unsigned_supplemental):
        raise ValueError("supplemental scope self-hash drift")
    if supplemental_scope.get("registry_hash") != registry.registry_hash:
        raise ValueError("supplemental scope registry hash drift")
    if supplemental_scope.get("baseline_scope", {}).get("root_scope_hash") != BASELINE_ROOT_SCOPE_HASH:
        raise ValueError("supplemental scope baseline hash drift")
    if supplemental_scope.get("baseline_scope", {}).get("git_blob_sha") != BASELINE_ROOT_SCOPE_BLOB_SHA:
        raise ValueError("supplemental scope baseline Git blob drift")
    supplemental_rows = list(supplemental_scope.get("roots") or [])
    if {str(row.get("field_id")) for row in supplemental_rows} != set(SUPPLEMENTAL_ROOTS):
        raise ValueError("supplemental root identity set drift")
    activation = dict(supplemental_scope.get("runtime_activation_contract") or {})
    if (
        activation.get("default") != "FROZEN"
        or activation.get("scope_mutation_allowed") is not False
        or activation.get("activation_unit") != "ONE_ROOT_ONE_VERIFIED_RECEIPT"
        or activation.get("candidate_receipt_hash_injection_required") is not True
        or activation.get("requirements_by_field")
        != RUNTIME_ACTIVATION_REQUIREMENTS
    ):
        raise ValueError("supplemental runtime activation contract drift")
    new_roots = {route: [] for route in SEARCHABLE_ROUTES}
    for row in supplemental_rows:
        field_id = str(row["field_id"])
        route_id = str(row["route_id"])
        if route_id != SUPPLEMENTAL_ROOTS[field_id]:
            raise ValueError(f"supplemental root route drift: {field_id}")
        if field_id in baseline_allowlists[route_id]:
            raise ValueError(f"supplemental root already exists in baseline scope: {field_id}")
        if field_id in RUNTIME_ACTIVATION_REQUIREMENTS and (
            row.get("materialization_status") != "NOT_MATERIALIZED"
            or row.get("runtime_ready") is not False
            or row.get("signal_sketch_allowed") is not False
            or row.get("strict_evaluation_allowed") is not False
            or row.get("runtime_activation")
            != "CANONICAL_FULL_DEVELOPMENT_RECEIPT_REQUIRED_PER_ROOT"
            or row.get("required_receipt")
            != RUNTIME_ACTIVATION_REQUIREMENTS[field_id]["receipt_type"]
        ):
            raise ValueError(f"supplemental runtime default escaped frozen scope: {field_id}")
        new_roots[route_id].append(field_id)
    new_roots = {route: sorted(values) for route, values in new_roots.items()}
    if any("plate" in value.lower() or "industry" in value.lower() for values in new_roots.values() for value in values):
        raise ValueError("plate/industry roots cannot enter this campaign")
    if "plate_industry_linkage" not in set(benchmark_registry.get("disabled_benchmark_ids") or []):
        raise ValueError("plate benchmark must remain disabled")

    baseline_artifacts = [
        {
            "role": role,
            "path": f"{BASELINE_RUNTIME_ROOT}/{relative}",
            "sha256": sha256,
            "mode": "READ_ONLY_HASH_REQUIRED",
        }
        for role, relative, sha256 in BASELINE_ARTIFACTS
    ]
    challenger_specs = _challenger_specs()
    contract: dict[str, Any] = {
        "contract_version": "cn_core_pack_delta_discovery_campaign_v1",
        "experiment_id": "20260718_cn_core_pack_delta_discovery_001",
        "status": "AUTHORIZED_SUPPLEMENTAL_ONLY_NOT_STARTED",
        "authorization": "CURRENT_USER_RESEARCH_DISCOVERY_PRIMARY_20260718",
        "execution_authorized": True,
        "formal_search_unfrozen": False,
        "objective": "reuse the completed Core Pack baseline and discover only new-root or real-challenger semantic deltas on development data",
        "authority_note": "campaign contract is stable; every executable epoch separately freezes a clean source closure and all input hashes",
        "source_inputs": {
            "baseline_scope_local_copy": {"path": str(scope_path.relative_to(repo)).replace("\\", "/"), "sha256": _sha256(scope_path), "contract_hash": scope["contract_hash"], "mutation": "FORBIDDEN"},
            "supplemental_root_scope": {"path": str(supplemental_scope_path.relative_to(repo)).replace("\\", "/"), "sha256": _sha256(supplemental_scope_path), "contract_hash": supplemental_scope["contract_hash"]},
            "unified_registry": {"path": str(registry_path.relative_to(repo)).replace("\\", "/"), "sha256": _sha256(registry_path), "registry_hash": registry.registry_hash},
            "strict_base_plan": {"path": str(base_plan_path.relative_to(repo)).replace("\\", "/"), "sha256": _sha256(base_plan_path)},
            "lane_registry_template": {"path": str(lane_registry_path.relative_to(repo)).replace("\\", "/"), "sha256": _sha256(lane_registry_path), "registry_hash": lane_registry["registry_hash"]},
            "benchmark_registry_template": {"path": str(benchmark_registry_path.relative_to(repo)).replace("\\", "/"), "sha256": _sha256(benchmark_registry_path), "plan_hash": benchmark_registry["plan_hash"]},
        },
        "baseline_contract": {
            "reuse_mode": "READ_ONLY_NO_REWRITE",
            "full_pack_regeneration": "FORBIDDEN",
            "repo_sha": BASELINE_REPO_SHA,
            "root_scope_path": BASELINE_ROOT_SCOPE_PATH,
            "root_scope_hash": BASELINE_ROOT_SCOPE_HASH,
            "root_scope_blob_sha": BASELINE_ROOT_SCOPE_BLOB_SHA,
            "proposal_attempts": 1_900_544,
            "legal_exact_unique_primaries": 69_897,
            "coverage_qualified_signals": 47_836,
            "signal_clusters": 28_325,
            "strict_pairs_completed": 64,
            "artifacts": baseline_artifacts,
        },
        "root_delta_contract": {
            "union_root_scope_hash": stable_hash(
                {
                    route: sorted(set(baseline_allowlists[route]) | set(new_roots[route]))
                    for route in SEARCHABLE_ROUTES
                }
            ),
            "newly_connected_roots_by_route": new_roots,
            "retired_or_blocked_baseline_roots_by_route": {
                route: [] for route in SEARCHABLE_ROUTES
            },
            "supplemental_scope_path": str(supplemental_scope_path.relative_to(repo)).replace("\\", "/"),
            "budgets_by_route": _route_delta_budget(new_roots),
            "allowed_generation_reasons": [
                "NEWLY_CONNECTED_CURRENT_QUALIFIED_ROOT",
                "REAL_CHALLENGER_NOT_REPRESENTED_IN_BASELINE",
            ],
            "baseline_root_replay_for_yield": "FORBIDDEN",
            "retired_root_policy": "PRESERVE_HISTORICAL_RECEIPT_BUT_EXCLUDE_FROM_NEW_ADMISSION",
        },
        "challenger_contract": {
            "typed_random_baseline_role": "DEDICATED_NONADAPTIVE_MATCHED_CONTROL_ONLY",
            "challengers": challenger_specs,
            "all_reserved_budgets_nonzero": True,
            "pending_adapter_budget_execution": "FAIL_CLOSED",
            "candidate_level_matched_unconditional_control_required": True,
            "algorithm_level_matched_nonadaptive_control_required": True,
            "generator_may_self_gate": False,
            "proposal_generator_authority": "PROPOSAL_TRANSCRIPT_ONLY",
            "admission_and_continuation_authority": "INDEPENDENT_CAMPAIGN_CONTROLLER",
        },
        "epoch_freeze_contract": {
            "required_before_each_epoch": [
                "repo_sha",
                "tree_sha",
                "data_release_hash",
                "split_manifest_hash",
                "field_registry_hash",
                "unified_registry_hash",
                "grammar_hash",
                "field_roles_hash",
                "reward_contract_hash",
                "route_budgets",
                "lane_budgets",
                "seeds",
                "admission_contract_hash",
                "strict_contract_hash",
                "active_adapter_source_hashes",
                "supplemental_root_compile_and_exposure_receipt_hash",
            ],
            "immutable_within_epoch": [
                "grammar",
                "field_roles",
                "reward",
                "route_budgets",
                "lane_budgets",
                "seeds",
                "admission_thresholds",
            ],
            "adaptive_state_lifetime": "WITHIN_SINGLE_EPOCH_EPHEMERAL_ONLY",
            "cross_epoch_memory": "FORBIDDEN",
            "cross_sprint_memory": "FORBIDDEN",
            "budget_reallocation_from_winners": "FORBIDDEN",
        },
        "union_and_dedup_contract": {
            "order": [
                "VERIFY_BASELINE_ARTIFACT_HASHES",
                "VERIFY_SUPPLEMENTAL_EPOCH_RECEIPTS",
                "GLOBAL_EXACT_IDENTITY_DEDUP",
                "SKETCH_ONLY_NEW_EXACT_IDENTITIES_ON_FROZEN_A_B_COORDINATES",
                "MERGE_SIGNAL_CLUSTERS_WITH_BASELINE_REGISTRY",
                "INDEPENDENT_STRATIFIED_ADMISSION",
            ],
            "one_exact_identity_one_vote": True,
            "baseline_identity_owner_precedence": True,
            "new_signal_sketch_coordinates_must_match_baseline": True,
            "performance_used_for_exact_or_cluster_identity": False,
        },
        "semantic_saturation_contract": {
            "authority": "INDEPENDENT_CAMPAIGN_CONTROLLER",
            "generator_declared_saturation_accepted": False,
            "minimum_completed_epochs_per_activated_challenger": 2,
            "consecutive_below_threshold_epochs_required": 3,
            "metrics": {
                "new_global_signal_cluster_rate_lt": 0.005,
                "new_semantic_cell_rate_lt": 0.0025,
                "new_field_family_count_eq": 0,
                "new_primitive_family_count_eq": 0,
                "new_root_first_consumption_count_eq": 0,
            },
            "performance_or_reward_metrics_allowed": False,
            "resource_gate_can_stop_run_but_declare_saturation": False,
            "new_real_adapter_or_root_reopens_supplemental_epochs": True,
            "terminal_total_proposal_cap": None,
            "epoch_budget_policy": "FIXED_RECOVERABLE_DELTA_WAVES_ONLY",
        },
        "strict_continuation_contract": {
            "baseline_completed_pairs": 64,
            "next_cumulative_gate": 256,
            "later_cumulative_gates": [1_024, 3_072, 8_192],
            "pre_256_engineering_gate": {
                "required_status": "R6_LIVENESS_PARITY_PASS",
                "epoch_freeze_artifact_sha256_required": True,
                "r5_r6_pair_results_exact": True,
                "released_bytes_min_exclusive": 0,
                "dag_cache_peak_lte_frozen_cap": True,
                "unknown_artifact_hash_policy": "FAIL_CLOSED_DO_NOT_START_256",
            },
            "frozen_256_selection_uses_baseline_admission_only": True,
            "supplemental_candidates_enter_strict_after_256_only": True,
            "adaptive_challenger_minimum_completed_strict_pairs": 256,
            "wave64_reward_may_change_grammar_fields_or_route_budgets": False,
            "selection_authority": "INDEPENDENT_GLOBAL_EXACT_AND_CLUSTER_ADMISSION",
            "strict_stage_a": "NOT_AUTHORIZED",
            "development_objective": base_plan["strict_evaluation_contract"]["optimizer_reward_metric"],
        },
        "data_access_contract": {
            "development_2024_2025": "ALLOWED",
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "forward_2026": "SEALED",
            "plate_industry": "DISABLED_PENDING_REAL_PIT_MINUTE_MEMBERSHIP",
        },
        "decision_boundaries": {
            "candidate_promotion": "FORBIDDEN",
            "positive_or_negative_permanent_memory": "FORBIDDEN",
            "cross_epoch_adaptive_memory": "FORBIDDEN",
            "stage_a": "NOT_AUTHORIZED",
            "failed_or_underfilled_epochs": "RETAIN_WITH_REASON",
        },
        "output_contract": {
            "append_only_supplemental_epoch_manifests": True,
            "baseline_artifacts_mutable": False,
            "global_exact_union_registry_required": True,
            "global_signal_cluster_union_required": True,
            "access_ledger_required": True,
            "candidate_pack_role": "DEVELOPMENT_DISCOVERY_ONLY_NO_PROMOTION",
        },
        "contract_hash": "",
    }
    contract["contract_hash"] = stable_hash(
        {key: value for key, value in contract.items() if key != "contract_hash"}
    )
    validate_contract(contract)
    return contract


def validate_contract(contract: Mapping[str, Any]) -> None:
    errors: list[str] = []

    unsigned = {key: value for key, value in contract.items() if key != "contract_hash"}
    if str(contract.get("contract_hash")) != stable_hash(unsigned):
        errors.append("contract self-hash mismatch")
    data = contract.get("data_access_contract") or {}
    if any(int(data.get(key, -1)) != 0 for key in ("validation_reads", "holdout_reads", "forward_2026_reads")):
        errors.append("validation/holdout/forward read budgets must be zero")
    if data.get("forward_2026") != "SEALED":
        errors.append("forward 2026 must remain sealed")
    if not str(data.get("plate_industry") or "").startswith("DISABLED"):
        errors.append("plate/industry must remain disabled")

    boundaries = contract.get("decision_boundaries") or {}
    for key in ("candidate_promotion", "positive_or_negative_permanent_memory", "cross_epoch_adaptive_memory"):
        if boundaries.get(key) != "FORBIDDEN":
            errors.append(f"{key} must remain forbidden")
    if boundaries.get("stage_a") != "NOT_AUTHORIZED":
        errors.append("Stage A must remain unauthorized")

    baseline = contract.get("baseline_contract") or {}
    if baseline.get("full_pack_regeneration") != "FORBIDDEN":
        errors.append("full-pack regeneration must be forbidden")
    if int(baseline.get("proposal_attempts") or 0) != 1_900_544 or int(baseline.get("legal_exact_unique_primaries") or 0) != 69_897:
        errors.append("baseline generation counts drift")
    if baseline.get("repo_sha") != BASELINE_REPO_SHA:
        errors.append("baseline repo SHA drift")
    if baseline.get("root_scope_path") != BASELINE_ROOT_SCOPE_PATH:
        errors.append("baseline root-scope path drift")
    if baseline.get("root_scope_hash") != BASELINE_ROOT_SCOPE_HASH:
        errors.append("baseline root-scope content hash drift")
    if baseline.get("root_scope_blob_sha") != BASELINE_ROOT_SCOPE_BLOB_SHA:
        errors.append("baseline root-scope historical blob SHA drift")
    artifacts = list(baseline.get("artifacts") or [])
    observed_artifacts = {
        (
            str(row.get("role") or ""),
            str(row.get("path") or ""),
            str(row.get("sha256") or ""),
            str(row.get("mode") or ""),
        )
        for row in artifacts
    }
    expected_artifacts = {
        (
            role,
            f"{BASELINE_RUNTIME_ROOT}/{relative}",
            sha256,
            "READ_ONLY_HASH_REQUIRED",
        )
        for role, relative, sha256 in BASELINE_ARTIFACTS
    }
    if observed_artifacts != expected_artifacts or len(artifacts) != len(expected_artifacts):
        errors.append("baseline artifact role/path/hash/mode tuple drift")

    delta = contract.get("root_delta_contract") or {}
    if delta.get("baseline_root_replay_for_yield") != "FORBIDDEN":
        errors.append("baseline root yield replay must be forbidden")
    allowed_reasons = set(delta.get("allowed_generation_reasons") or [])
    if allowed_reasons != {"NEWLY_CONNECTED_CURRENT_QUALIFIED_ROOT", "REAL_CHALLENGER_NOT_REPRESENTED_IN_BASELINE"}:
        errors.append("supplemental generation reasons drift")
    observed_supplemental = {
        str(field_id): str(route)
        for route, values in (delta.get("newly_connected_roots_by_route") or {}).items()
        for field_id in values
    }
    if observed_supplemental != SUPPLEMENTAL_ROOTS:
        errors.append("supplemental seven-root/three-route scope drift")
    for route, budget in (delta.get("budgets_by_route") or {}).items():
        roots = list((delta.get("newly_connected_roots_by_route") or {}).get(route) or [])
        proposal = int(budget.get("proposal_budget") or 0)
        admission = int(budget.get("admission_budget") or 0)
        if bool(roots) != bool(proposal and admission):
            errors.append(f"root delta budget mismatch: {route}")

    challenger_contract = contract.get("challenger_contract") or {}
    if challenger_contract.get("generator_may_self_gate") is not False:
        errors.append("proposal generator cannot gate itself")
    if challenger_contract.get("admission_and_continuation_authority") != "INDEPENDENT_CAMPAIGN_CONTROLLER":
        errors.append("independent campaign controller must own gates")
    challengers = list(challenger_contract.get("challengers") or [])
    if {row.get("challenger_id") for row in challengers} != set(CHALLENGERS):
        errors.append("challenger set drift")
    namespaces: set[str] = set()
    for row in challengers:
        control = row.get("matched_nonadaptive_control") or {}
        if row.get("activation_state") != "PENDING_REAL_UNIFIED_ADAPTER_RECEIPT":
            errors.append(f"unproved challenger activated: {row.get('challenger_id')}")
        if control.get("policy_id") != "stratified_typed_random":
            errors.append(f"nonadaptive control drift: {row.get('challenger_id')}")
        for key in ("proposal_budget_by_route", "admission_budget_by_route"):
            if row.get(key) != control.get(key) or not row.get(key) or any(int(value) <= 0 for value in row[key].values()):
                errors.append(f"matched budget mismatch: {row.get('challenger_id')}:{key}")
        if int(row.get("strict_pair_budget_after_256") or 0) <= 0 or row.get("strict_pair_budget_after_256") != control.get("strict_pair_budget_after_256"):
            errors.append(f"matched strict budget mismatch: {row.get('challenger_id')}")
        for namespace_key in ("archive_namespace", "lineage_namespace"):
            for source in (row, control):
                namespace = str(source.get(namespace_key) or "")
                if not namespace or namespace in namespaces:
                    errors.append(f"challenger namespace collision: {namespace}")
                namespaces.add(namespace)
        requirements = set(row.get("activation_requirements") or [])
        if not {"SOURCE_PATH_AND_SHA256_BOUND", "PROPOSAL_DISTRIBUTION_DIFFERS_FROM_MATCHED_CONTROL", "NO_FAKE_POLICY_LABELS"} <= requirements:
            errors.append(f"real adapter proof incomplete: {row.get('challenger_id')}")

    freeze = contract.get("epoch_freeze_contract") or {}
    immutable = set(freeze.get("immutable_within_epoch") or [])
    if not {"grammar", "field_roles", "reward", "route_budgets", "lane_budgets", "seeds"} <= immutable:
        errors.append("within-epoch freeze is incomplete")
    if freeze.get("cross_epoch_memory") != "FORBIDDEN" or freeze.get("cross_sprint_memory") != "FORBIDDEN":
        errors.append("cross-epoch/sprint memory must be forbidden")

    saturation = contract.get("semantic_saturation_contract") or {}
    if saturation.get("authority") != "INDEPENDENT_CAMPAIGN_CONTROLLER" or saturation.get("generator_declared_saturation_accepted") is not False:
        errors.append("semantic saturation authority drift")
    if saturation.get("performance_or_reward_metrics_allowed") is not False:
        errors.append("performance/reward cannot gate semantic saturation")
    if int(saturation.get("consecutive_below_threshold_epochs_required") or 0) < 2:
        errors.append("semantic saturation requires repeated epochs")

    strict = contract.get("strict_continuation_contract") or {}
    if int(strict.get("baseline_completed_pairs") or 0) != 64 or int(strict.get("next_cumulative_gate") or 0) != 256:
        errors.append("strict continuation must resume from 64 to 256")
    engineering = strict.get("pre_256_engineering_gate") or {}
    if (
        engineering.get("required_status") != "R6_LIVENESS_PARITY_PASS"
        or engineering.get("epoch_freeze_artifact_sha256_required") is not True
        or engineering.get("r5_r6_pair_results_exact") is not True
        or engineering.get("released_bytes_min_exclusive") != 0
        or engineering.get("dag_cache_peak_lte_frozen_cap") is not True
        or engineering.get("unknown_artifact_hash_policy") != "FAIL_CLOSED_DO_NOT_START_256"
    ):
        errors.append("R6 liveness parity evidence must gate 256")
    if int(strict.get("adaptive_challenger_minimum_completed_strict_pairs") or 0) < 256:
        errors.append("adaptive challengers cannot start before 256 strict pairs")
    if strict.get("wave64_reward_may_change_grammar_fields_or_route_budgets") is not False:
        errors.append("wave64 reward cannot reshape the hypothesis space")
    if strict.get("strict_stage_a") != "NOT_AUTHORIZED":
        errors.append("strict Stage A must remain unauthorized")

    if errors:
        raise ValueError("invalid delta discovery campaign: " + "; ".join(errors))


def freeze(
    *,
    output_path: Path = DEFAULT_OUTPUT,
    supplemental_scope_path: Path = DEFAULT_SUPPLEMENTAL_SCOPE,
    **kwargs: Any,
) -> dict[str, Any]:
    supplemental_scope = build_supplemental_scope(**{
        key: value for key, value in kwargs.items() if key in {"repo", "registry_path", "gap_audit_path"}
    })
    supplemental_scope_path.parent.mkdir(parents=True, exist_ok=True)
    supplemental_scope_path.write_text(
        json.dumps(supplemental_scope, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    contract = build_contract(
        supplemental_scope_path=supplemental_scope_path,
        **{key: value for key, value in kwargs.items() if key != "gap_audit_path"},
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(contract, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return contract


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--supplemental-scope", type=Path, default=DEFAULT_SUPPLEMENTAL_SCOPE)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    if args.validate_only:
        contract = _json(args.output)
        validate_contract(contract)
    else:
        contract = freeze(
            output_path=args.output.resolve(),
            supplemental_scope_path=args.supplemental_scope.resolve(),
        )
    print(
        json.dumps(
            {
                "status": contract["status"],
                "contract_hash": contract["contract_hash"],
                "baseline_exact": contract["baseline_contract"]["legal_exact_unique_primaries"],
                "new_root_count": sum(
                    len(values)
                    for values in contract["root_delta_contract"]["newly_connected_roots_by_route"].values()
                ),
                "challenger_count": len(contract["challenger_contract"]["challengers"]),
                "next_strict_gate": contract["strict_continuation_contract"]["next_cumulative_gate"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
