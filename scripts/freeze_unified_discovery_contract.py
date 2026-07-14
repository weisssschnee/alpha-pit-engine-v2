"""Freeze the fixed-budget CN unified capability CANARY/discovery contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

from our_system_phase2.services.unified_capability_registry import ROUTE_IDS, UnifiedCapabilityRegistry, stable_hash


EXPERIMENT_ID = "cn_unified_capability_integration_discovery_v1"


def _sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=repo, text=True).strip()


def _mode_contract(route_budgets: dict[str, dict[str, int]], seed_sets: dict[str, dict[str, int]]) -> dict[str, Any]:
    return {
        "route_budgets": route_budgets,
        "seed_sets": seed_sets,
        "exact_dedup_before_budget": True,
        "selector": {
            "version": "cn_unified_development_selector_v1_frozen",
            "objective": "development_rank_ic_cost_stability_with_route_family_caps",
            "frozen_before_performance": True,
            "online_refit": False,
        },
        "admission": {
            "one_exact_identity_one_vote": True,
            "one_support_unit_one_vote": True,
            "route_caps": True,
            "family_caps": True,
            "matched_controls_required": True,
        },
    }


def freeze(
    *,
    repo: Path,
    registry_path: Path,
    release_manifest_path: Path,
    split_manifest_path: Path,
    broad_event_pack_path: Path,
    fundamental_manifest_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    if _git(repo, "status", "--porcelain=v1"):
        raise RuntimeError("contract freeze requires a clean worktree")
    repo_sha = _git(repo, "rev-parse", "HEAD")
    registry = UnifiedCapabilityRegistry.read(registry_path)
    release = json.loads(release_manifest_path.read_text(encoding="utf-8"))
    release_hash = str(release.get("release_hash") or release.get("manifest_hash") or "")
    if not release_hash:
        raise ValueError("development release manifest lacks release_hash")
    broad_event = json.loads(broad_event_pack_path.read_text(encoding="utf-8"))
    mechanisms = broad_event.get("mechanisms") or broad_event.get("entries") or []
    if len(mechanisms) != 11:
        raise ValueError(f"frozen Broad Event entry pack must contain 11 mechanisms, observed {len(mechanisms)}")

    seed_sets = {
        "seed_a": {
            "MINUTE_STATIC": 1729,
            "FIRSTN_PATH": 1733,
            "SLOW_CROSS_SECTIONAL_LEVEL": 1741,
            "SLOW_TEMPORAL_CHANGE": 1747,
            "DISCLOSURE_EVENT": 1753,
            "MARKET_REGIME_CONDITION": 1759,
            "INTRADAY_STATE_TRANSITION": 1777,
            "BROAD_EVENT_FROZEN_ENTRY": 1783,
        },
        "seed_b": {
            "MINUTE_STATIC": 2718,
            "FIRSTN_PATH": 2729,
            "SLOW_CROSS_SECTIONAL_LEVEL": 2731,
            "SLOW_TEMPORAL_CHANGE": 2741,
            "DISCLOSURE_EVENT": 2749,
            "MARKET_REGIME_CONDITION": 2753,
            "INTRADAY_STATE_TRANSITION": 2767,
            "BROAD_EVENT_FROZEN_ENTRY": 2777,
        },
    }
    canary_budgets = {
        "MINUTE_STATIC": {"proposal": 8, "admission": 4, "strict": 2},
        "FIRSTN_PATH": {"proposal": 8, "admission": 4, "strict": 2},
        "SLOW_CROSS_SECTIONAL_LEVEL": {"proposal": 8, "admission": 4, "strict": 2},
        "SLOW_TEMPORAL_CHANGE": {"proposal": 8, "admission": 4, "strict": 2},
        "DISCLOSURE_EVENT": {"proposal": 8, "admission": 8, "strict": 4},
        "MARKET_REGIME_CONDITION": {"proposal": 8, "admission": 4, "strict": 2},
        "INTRADAY_STATE_TRANSITION": {"proposal": 6, "admission": 4, "strict": 2},
        "BROAD_EVENT_FROZEN_ENTRY": {"proposal": 22, "admission": 22, "strict": 22},
    }
    discovery_budgets = {
        "MINUTE_STATIC": {"proposal": 16, "admission": 8, "strict": 4},
        "FIRSTN_PATH": {"proposal": 24, "admission": 12, "strict": 6},
        "SLOW_CROSS_SECTIONAL_LEVEL": {"proposal": 32, "admission": 16, "strict": 8},
        "SLOW_TEMPORAL_CHANGE": {"proposal": 32, "admission": 16, "strict": 8},
        "DISCLOSURE_EVENT": {"proposal": 8, "admission": 8, "strict": 4},
        "MARKET_REGIME_CONDITION": {"proposal": 16, "admission": 8, "strict": 4},
        "INTRADAY_STATE_TRANSITION": {"proposal": 18, "admission": 10, "strict": 6},
        "BROAD_EVENT_FROZEN_ENTRY": {"proposal": 22, "admission": 22, "strict": 22},
    }
    for budgets in (canary_budgets, discovery_budgets):
        if set(budgets) != set(ROUTE_IDS):
            raise ValueError("route budget coverage mismatch")
        for route_id, values in budgets.items():
            if values["proposal"] <= 0 or values["admission"] <= 0 or values["strict"] <= 0:
                raise ValueError(f"zero budget on {route_id}")
            if values["strict"] > values["admission"] or values["admission"] > values["proposal"]:
                raise ValueError(f"invalid funnel budget on {route_id}")
            if values["proposal"] % 2:
                raise ValueError(f"proposal budget must include complete candidate/control pairs on {route_id}")

    code_paths = {
        "compiler": repo / "src/our_system_phase2/services/typed_route_compiler.py",
        "generator": repo / "src/our_system_phase2/services/unified_discovery_generators.py",
        "ledger": repo / "src/our_system_phase2/services/search_exposure_ledger.py",
        "runner": repo / "src/our_system_phase2/runtime/cn_unified_capability_discovery.py",
        "preflight": repo / "src/our_system_phase2/runtime/cn_unified_capability_preflight.py",
        "fundamental_materializer": repo / "src/our_system_phase2/services/fundamental_representations.py",
    }
    missing = [str(path) for path in code_paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError("contract code paths missing: " + ",".join(missing))
    contract = {
        "contract_version": "cn_unified_capability_discovery_contract_v1",
        "experiment_id": EXPERIMENT_ID,
        "execution_state": "FROZEN_DEVELOPMENT_ONLY_AUTOMATIC_PREFLIGHT_CANARY_DISCOVERY",
        "repo_sha": repo_sha,
        "registry_hash": registry.registry_hash,
        "maximum_observable_time": "2025-07-07T15:00:00",
        "seed_sets": seed_sets,
        "capability_canary": _mode_contract(canary_budgets, seed_sets),
        "unified_discovery": {
            **_mode_contract(discovery_budgets, seed_sets),
            "adaptive_challenger": {
                "algorithm": "RX_UCB",
                "primary_adaptive_challenger": True,
                "proposal_budget_per_seed": 32,
                "admission_budget_per_seed": 8,
                "strict_budget_per_seed": 4,
                "matched_nonadaptive_control_budget_per_seed": 32,
                "state_persistence": "WITHIN_RUN_ONLY",
                "cross_sprint_memory": False,
            },
        },
        "data_release": {
            "release_id": "cn_true1min_development_only_release_v1_20260712_77o",
            "release_hash": release_hash,
            "manifest_file_sha256": _sha256(release_manifest_path),
            "split_manifest_sha256": _sha256(split_manifest_path),
            "required_shard_count": 16,
            "row_group_scope": "ALL_ROW_GROUPS",
            "development_session_scope": "ALL_TRAIN_SESSIONS",
            "allowed_years": [2024, 2025],
        },
        "preflight_smoke": {"trade_date": "2025-04-01", "row_group_index": 0},
        "capability_canary_scope": {
            "shards": list(range(16)),
            "fixed_row_group_indices": [0, 9, 18, 27],
            "fixed_development_dates": ["2024-04-29", "2024-12-03", "2025-03-04", "2025-06-16"],
            "selection_basis": "calendar blocks and physical coordinates fixed without labels or performance",
        },
        "unified_discovery_scope": {
            "shards": list(range(16)),
            "row_groups": "ALL",
            "development_dates": "ALL_TRAIN",
            "support_units": "route_specific",
        },
        "broad_event": {
            "entry_pack_sha256": _sha256(broad_event_pack_path),
            "mechanism_count": 11,
            "tier_c_descendants_allowed": False,
            "entry_pack_mutation_allowed": False,
        },
        "fundamental": {
            "manifest_sha256": _sha256(fundamental_manifest_path),
            "canonical_root_count": sum(row.source_family.startswith("canonical_fundamental_") for row in registry.fields),
            "raw_source_root_exposure_allowed": False,
            "zygc_status": "PIT_CONTRACT_UNRESOLVED",
        },
        "input_hashes": {
            "registry_file_sha256": _sha256(registry_path),
            "release_manifest_sha256": _sha256(release_manifest_path),
            "split_manifest_sha256": _sha256(split_manifest_path),
            "broad_event_pack_sha256": _sha256(broad_event_pack_path),
            "fundamental_manifest_sha256": _sha256(fundamental_manifest_path),
        },
        "code_hashes": {name: _sha256(path) for name, path in code_paths.items()},
        "boundaries": {
            "development_reward": True,
            "validation": False,
            "holdout": False,
            "challenge": False,
            "forward_2026": False,
            "candidate_promotion": False,
            "persistent_positive_memory": False,
            "persistent_negative_memory": False,
            "cross_sprint_adaptive_memory": False,
            "plate_industry": False,
        },
        "completion_boundary": "DEVELOPMENT_DISCOVERY_PACK_FROZEN",
        "challenge_application_requires_independent_authorization": True,
    }
    contract["contract_hash"] = stable_hash(contract)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(contract, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return contract


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--release-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--broad-event-pack", type=Path, required=True)
    parser.add_argument("--fundamental-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    contract = freeze(
        repo=args.repo,
        registry_path=args.registry,
        release_manifest_path=args.release_manifest,
        split_manifest_path=args.split_manifest,
        broad_event_pack_path=args.broad_event_pack,
        fundamental_manifest_path=args.fundamental_manifest,
        output_path=args.output,
    )
    print(json.dumps({"status": "FROZEN", "repo_sha": contract["repo_sha"], "contract_hash": contract["contract_hash"], "output": str(args.output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
