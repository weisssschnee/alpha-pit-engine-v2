"""Freeze an aggressive, development-only Core Pack discovery contract.

The contract makes each execution epoch finite and recoverable, but it does
not impose an arbitrary terminal research budget.  Continuation is governed by
pre-registered semantic novelty, support, stability, and resource gates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


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


DEFAULT_SCOPE = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"
DEFAULT_REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
DEFAULT_BASE_PLAN = REPO / "runtime/run_plans/cn_compositional_nline_bounded_search_epoch1_v1.json"
DEFAULT_SPLIT = REPO / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
DEFAULT_OUTPUT = REPO / "runtime/run_plans/cn_core_pack_aggressive_development_discovery_v2.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", *args],
        cwd=repo,
        text=True,
        encoding="utf-8",
    ).strip()


def freeze(
    *,
    repo: Path,
    scope_path: Path,
    registry_path: Path,
    base_plan_path: Path,
    split_manifest_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    if _git(repo, "status", "--porcelain", "--untracked-files=no"):
        raise RuntimeError("contract freeze requires a clean tracked worktree")

    scope = _json(scope_path)
    base_plan = _json(base_plan_path)
    registry = UnifiedCapabilityRegistry.read(registry_path)
    if str(scope["registry_hash"]) != registry.registry_hash:
        raise RuntimeError("Core Pack scope and unified registry content hash differ")
    if bool(scope.get("execution_authorized")):
        raise RuntimeError("source scope must remain the non-executable v1 contract")

    all_allowlists = dict(scope["route_root_allowlists"])
    route_allowlists = {
        route_id: list(all_allowlists[route_id])
        for route_id in SEARCHABLE_ROUTES
    }
    if any(not values for values in route_allowlists.values()):
        raise RuntimeError("every searchable route requires a non-empty root allowlist")

    proposal_partitions = [f"typed_random_partition_{index:02d}" for index in range(8)]
    seeds = [1729, 2718, 31415, 65537, 104729, 130363, 155921, 196613]
    cells = len(proposal_partitions) * len(seeds)
    route_attempt_quotas = {
        "MINUTE_STATIC": 131_072,
        "FIRSTN_PATH": 262_144,
        "SLOW_CROSS_SECTIONAL_LEVEL": 262_144,
        "SLOW_TEMPORAL_CHANGE": 524_288,
        "DISCLOSURE_EVENT": 262_144,
        "MARKET_REGIME_CONDITION": 262_144,
        "INTRADAY_STATE_TRANSITION": 196_608,
    }
    if set(route_attempt_quotas) != set(SEARCHABLE_ROUTES):
        raise RuntimeError("aggressive proposal quotas do not cover the searchable routes")
    if any(value % cells for value in route_attempt_quotas.values()):
        raise RuntimeError("route proposal quotas must balance every partition/seed cell")

    repo_sha = _git(repo, "rev-parse", "HEAD")
    tree_sha = _git(repo, "rev-parse", "HEAD^{tree}")
    contract: dict[str, Any] = {
        "contract_version": "cn_core_pack_aggressive_development_discovery_v2",
        "experiment_id": "20260718_cn_core_pack_aggressive_discovery_001",
        "status": "AUTHORIZED_NOT_STARTED",
        "authorization": "CURRENT_USER_AGGRESSIVE_DEVELOPMENT_DISCOVERY_AUTHORIZATION_20260718",
        "execution_authorized": True,
        "objective": "discover reproducible development-only matched mechanisms across the current typed Core Pack without an arbitrary terminal research budget",
        "repo_sha": repo_sha,
        "tree_sha": tree_sha,
        "unified_registry_path": str(registry_path.relative_to(repo)).replace("\\", "/"),
        "registry_hash": registry.registry_hash,
        "source_scope_contract": {
            "path": str(scope_path.relative_to(repo)).replace("\\", "/"),
            "sha256": _sha256(scope_path),
            "contract_hash": scope["contract_hash"],
        },
        "input_hashes": {
            "registry_file_sha256": _sha256(registry_path),
            "base_plan_sha256": _sha256(base_plan_path),
            "split_manifest_sha256": _sha256(split_manifest_path),
        },
        "route_root_allowlists": route_allowlists,
        "held_or_blocked_roots": {
            "globally_blocked": scope["globally_blocked_core_roots"],
            "held_by_route": scope["held_core_roots"],
            "plate": "DISABLED_UNTIL_REAL_PIT_MINUTE_MEMBERSHIP_AND_MATERIALIZATION_QUALIFY",
        },
        "policies": proposal_partitions,
        "seeds": seeds,
        "generation_contract": {
            "proposal_origin": "TYPED_RANDOM_POOL_ONLY",
            "proposal_partition_semantics": "PARTITION_LABEL_ONLY_NO_ADAPTIVE_POLICY_CLAIM",
            "adaptive_policy_labels_forbidden": True,
            "route_attempt_quotas": route_attempt_quotas,
            "initial_proposal_attempts": sum(route_attempt_quotas.values()),
            "structural_preadmission_maximum_pairs": 80_000,
            "legal_exact_unique_target": 60_000,
            "continuation_wave": "REPEAT_WITH_PRECOMMITTED_SEED_OFFSETS_WHILE_ROUTE_MARGINAL_EXACT_YIELD_GTE_0_0025",
            "terminal_total_proposal_cap": None,
            "per_wave_recoverable": True,
        },
        "admission_contract": {
            "maximum_pairs": 32_000,
            "one_exact_identity_one_vote": True,
            "selection": "DETERMINISTIC_SIGNAL_CLUSTER_AND_SEMANTIC_VOLUME_STRATIFIED",
            "performance_selection": "FORBIDDEN_BEFORE_STRICT_WAVE_0",
            "parent_descendant_cap": 8,
            "fresh_budget_floor": 0.25,
            "no_memory_lane_floor": 0.20,
        },
        "progressive_strict_contract": {
            "cumulative_pair_targets": [64, 256, 1_024, 3_072, 8_192],
            "minimum_evidence_target": 1_024,
            "minimum_target_cannot_stop_for_early_negative_reward": True,
            "route_floor_by_1024": 128,
            "after_8192": "CONTINUE_IN_RECOVERABLE_2048_PAIR_EPOCHS_WHILE_GATES_PASS",
            "terminal_total_pair_cap": None,
            "continuation_scope": "SAME_EXPERIMENT_ONLY_EPHEMERAL_STATE",
            "continue_gates": [
                "NO_ACCESS_OR_PIT_VIOLATION",
                "NO_INFRASTRUCTURE_FAILED_PAIRS",
                "RESOURCE_GATES_PASS",
                "NEW_SIGNAL_CLUSTER_RATE_GTE_0_005_OR_NEW_MATCHED_MECHANISM_OBSERVED",
                "NO_SINGLE_ROUTE_OR_LINEAGE_DOMINATES_ABOVE_FROZEN_CAP",
            ],
            "stop_gates": [
                "ACCESS_OR_PIT_CONTRACT_FAILURE",
                "RESOURCE_HARD_GATE",
                "THREE_CONSECUTIVE_EPOCHS_BELOW_CLUSTER_NOVELTY_GATE_WITH_NO_NEW_MATCHED_MECHANISM",
                "REPEATED_RESUME_OR_SEMANTIC_PARITY_FAILURE",
            ],
        },
        "adaptive_selector_contract": {
            "enabled_after_strict_pairs": 256,
            "allowed": ["CEM", "RX_UCB", "UCT_MCTS", "EVOLUTIONARY"],
            "must_change_proposal_or_selection_distribution": True,
            "matched_nonadaptive_control_required": True,
            "per_policy_archive_namespace_required": True,
            "within_experiment_ephemeral_state_only": True,
            "cross_sprint_memory": "FORBIDDEN",
            "fake_policy_labels": "FORBIDDEN",
        },
        "strict_evaluation_contract": base_plan["strict_evaluation_contract"],
        "compute_contract": {
            "heavy_host": "77o",
            "heavy_processes_max": 2,
            "global_native_compute_threads_max": 24,
            "nested_parallelism": "FORBIDDEN",
            "pair_checkpoint_cadence": 64,
            "thread_probe": {
                "candidate_threads": [3, 6, 8, 12],
                "select_highest_parity_safe_throughput": True,
                "parallelism_gate": "effective_cores_gte_half_allocated_threads",
            },
            "hot_path_priority": "BATCHED_PORTFOLIO_KERNEL",
            "local_heavy_python": "FORBIDDEN",
        },
        "data_access_contract": {
            "development_2024_2025": "ALLOWED",
            "validation": "FORBIDDEN",
            "holdout": "FORBIDDEN",
            "forward_2026": "SEALED",
            "candidate_promotion": "FORBIDDEN",
            "positive_or_negative_permanent_memory": "FORBIDDEN",
        },
        "output_contract": {
            "candidate_pack_role": "FROZEN_DEVELOPMENT_DISCOVERY_ONLY_NO_PROMOTION",
            "candidate_pack_immutable_after_close": True,
            "access_ledger_required": True,
            "artifact_index_required": True,
            "failure_attempts_retained": True,
        },
        "contract_hash": "",
    }
    contract["contract_hash"] = stable_hash(
        {key: value for key, value in contract.items() if key != "contract_hash"}
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(contract, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return contract


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=REPO)
    parser.add_argument("--scope", type=Path, default=DEFAULT_SCOPE)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--base-plan", type=Path, default=DEFAULT_BASE_PLAN)
    parser.add_argument("--split-manifest", type=Path, default=DEFAULT_SPLIT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    contract = freeze(
        repo=args.repo.resolve(),
        scope_path=args.scope.resolve(),
        registry_path=args.registry.resolve(),
        base_plan_path=args.base_plan.resolve(),
        split_manifest_path=args.split_manifest.resolve(),
        output_path=args.output.resolve(),
    )
    print(json.dumps({
        "status": contract["status"],
        "contract_hash": contract["contract_hash"],
        "initial_proposal_attempts": contract["generation_contract"]["initial_proposal_attempts"],
        "minimum_strict_pairs": contract["progressive_strict_contract"]["minimum_evidence_target"],
        "terminal_total_pair_cap": contract["progressive_strict_contract"]["terminal_total_pair_cap"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
