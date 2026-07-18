"""Generate the append-only Core Pack root delta without replaying the base pack.

This runner is structural only: it opens the unified registry and frozen root
scope, compiles deterministic matched pairs, and optionally removes exact
identities already owned by a baseline receipt table.  It never opens market
data, returns, labels, validation, holdout, or forward data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.compositional_grammar import (  # noqa: E402
    CompositionalGrammarV2,
    SUPPLEMENTAL_GRAMMAR_VERSION,
    supplemental_skeleton_registry,
)
from our_system_phase2.services.materialization_support_receipt import (  # noqa: E402
    load_verified_full_development_receipts,
)
from our_system_phase2.services.unified_capability_registry import (  # noqa: E402
    UnifiedCapabilityRegistry,
    stable_hash,
)


DEFAULT_REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
DEFAULT_SUPPLEMENTAL_SCOPE = (
    REPO / "runtime/run_plans/cn_core_pack_supplemental_root_scope_v1.json"
)
DEFAULT_CAMPAIGN = REPO / "runtime/run_plans/cn_core_pack_delta_discovery_campaign_v1.json"
DEFAULT_OUTPUT = REPO / "runtime/cn_core_pack_supplemental_generation_v1"
SCOPE_VERSION = "cn_core_pack_supplemental_root_scope_v1"
SEEDS = (20260718, 20260719)
ATTEMPTS_PER_ROUTE = 64
CANONICAL_BASELINE_RECEIPTS_BINDING = {
    "path": (
        "D:/ChengboRemote/runtime/cn_core_pack_aggressive_discovery_20260718_595c5fc/"
        "CN_PAIR_RECEIPTS.jsonl"
    ),
    "sha256": "d332d501176258488ee26845a911a37f3dc7451f9e6c0b6bc23b504c572e10e9",
}
TARGET_ROOTS_BY_ROUTE = {
    "SLOW_CROSS_SECTIONAL_LEVEL": (
        "fund_disclosure_balance_age_sessions",
        "fund_disclosure_profit_age_sessions",
        "fund_disclosure_cashflow_age_sessions",
        "fund_disclosure_holder_age_sessions",
    ),
    "MARKET_REGIME_CONDITION": (
        "ctx_hfq_is_st",
        "ctx_hfq_prev_is_limit_up",
    ),
    "INTRADAY_STATE_TRANSITION": ("state_close_range_location_sign",),
}
MATERIALIZATION_BLOCKED_ROOT_IDS = frozenset(
    {
        "fund_disclosure_balance_age_sessions",
        "fund_disclosure_profit_age_sessions",
        "fund_disclosure_cashflow_age_sessions",
        "fund_disclosure_holder_age_sessions",
        "state_close_range_location_sign",
    }
)
RUNTIME_RECEIPT_REQUIREMENTS = {
    **{
        field_id: {
            "receipt_type": "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT",
            "full_development_assembly": "FULL_DEVELOPMENT_SESSION_PANEL",
        }
        for field_id in MATERIALIZATION_BLOCKED_ROOT_IDS
        if field_id.startswith("fund_disclosure_")
    },
    "state_close_range_location_sign": {
        "receipt_type": "FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
        "full_development_assembly": "FULL_DEVELOPMENT_ACTIVE_PANEL",
    },
}
SOURCE_PATHS = {
    "grammar": REPO / "src/our_system_phase2/services/compositional_grammar.py",
    "compiler": REPO / "src/our_system_phase2/services/typed_route_compiler.py",
    "runner": Path(__file__).resolve(),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_baseline_exact(path: Path | None) -> set[str]:
    if path is None:
        return set()
    exact: set[str] = set()
    with Path(path).open("r", encoding="utf-8-sig") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            for key in ("exact_identity", "control_exact_identity"):
                if row.get(key):
                    exact.add(str(row[key]))
    return exact


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _compact_receipt(
    pair: Any,
    *,
    route_attempt_index: int,
    runtime_receipts: Mapping[str, Mapping[str, str]],
) -> dict[str, Any]:
    primary, control = pair.primary, pair.control
    declared = {str(value) for value in primary["declared_field_ids"]}
    gated = declared & set(MATERIALIZATION_BLOCKED_ROOT_IDS)
    bindings = {
        field_id: str(runtime_receipts[field_id]["receipt_hash"])
        for field_id in sorted(gated & set(runtime_receipts))
    }
    runtime_ready = bool(gated) and set(bindings) == gated
    if not gated:
        runtime_ready = True
    return {
        "receipt_schema": "cn_core_pack_supplemental_pair_receipt_v1",
        "route_id": primary["route_id"],
        "route_attempt_index": int(route_attempt_index),
        "seed": int(primary["seed"]),
        "candidate_id": primary["candidate_id"],
        "control_candidate_id": control["candidate_id"],
        "pair_id": primary["pair_id"],
        "skeleton_id": primary["skeleton_id"],
        "policy_id": "supplemental_typed_root_delta_smoke",
        "generator_version": primary["generator_version"],
        "supplemental_gap_id": primary["supplemental_gap_id"],
        "supplemental_authority_id": primary["supplemental_authority_id"],
        "exact_identity": primary["exact_identity"],
        "control_exact_identity": control["exact_identity"],
        "canonical_identity": primary["canonical_identity"],
        "canonical_expression": primary["canonical_expression"],
        "control_canonical_expression": control["canonical_expression"],
        "declared_field_ids": list(primary["declared_field_ids"]),
        "condition_field_ids": list(primary.get("condition_field_ids", ())),
        "market_condition_field_ids": list(
            primary.get("market_condition_field_ids", ())
        ),
        "stock_context_field_ids": list(primary.get("stock_context_field_ids", ())),
        "source_field_ids": list(primary["source_field_ids"]),
        "representation_ids": list(primary["representation_ids"]),
        "state_materialization_required": bool(
            primary.get("state_materialization_required", False)
        ),
        "state_materialization_expression": str(
            primary.get("state_materialization_expression") or ""
        ),
        "state_source_field_ids": list(primary.get("state_source_field_ids", ())),
        "runtime_ready": runtime_ready,
        "materialization_status": (
            "MATERIALIZED_DEVELOPMENT_ONLY"
            if gated and runtime_ready
            else str(
                primary.get("materialization_status")
                or "MATERIALIZATION_NOT_REQUIRED"
            )
        ),
        "signal_sketch_allowed": (
            runtime_ready
            if gated
            else bool(primary.get("signal_sketch_allowed", True))
        ),
        "strict_evaluation_allowed": (
            runtime_ready
            if gated
            else bool(primary.get("strict_evaluation_allowed", True))
        ),
        "materialization_support_receipt_hashes": bindings,
        "required_materialization_receipt": str(
            primary.get("required_materialization_receipt") or ""
        ),
        "required_support_receipt": str(
            primary.get("required_support_receipt") or ""
        ),
        "access_roles": ["development"],
        "promotion_allowed": False,
    }


def load_runtime_receipt_bindings(
    receipt_paths: Iterable[Path],
    *,
    registry: UnifiedCapabilityRegistry,
) -> dict[str, dict[str, str]]:
    """Verify actual full-development artifacts and return a canonical root catalog."""

    return load_verified_full_development_receipts(
        tuple(Path(value) for value in receipt_paths), registry=registry
    )


def validate_supplemental_scope(
    *,
    scope: Mapping[str, Any],
    base_scope: Mapping[str, Any],
    registry: UnifiedCapabilityRegistry,
) -> dict[str, list[str]]:
    """Validate the committed scope authority and return an in-memory overlay."""

    unsigned = {key: value for key, value in scope.items() if key != "contract_hash"}
    if str(scope.get("contract_hash") or "") != stable_hash(unsigned):
        raise ValueError("supplemental scope self-hash mismatch")
    if scope.get("contract_version") != SCOPE_VERSION or scope.get("append_only") is not True:
        raise ValueError("unsupported or non-append-only supplemental scope")
    if scope.get("status") != "AUTHORIZED_APPEND_ONLY_SUPPLEMENTAL_SCOPE":
        raise ValueError("supplemental scope is not authorized")
    if str(scope.get("registry_hash")) != registry.registry_hash:
        raise ValueError("supplemental scope and registry hash mismatch")
    baseline = dict(scope.get("baseline_scope") or {})
    if baseline.get("mutation") != "FORBIDDEN":
        raise ValueError("historical base scope mutation must remain forbidden")
    base_allowlists = {
        str(route_id): sorted(str(value) for value in values)
        for route_id, values in dict(base_scope["route_root_allowlists"]).items()
    }
    searchable = {
        route_id: values
        for route_id, values in base_allowlists.items()
        if route_id != "BROAD_EVENT_FROZEN_ENTRY"
    }
    if stable_hash(searchable) != str(baseline.get("root_scope_hash") or ""):
        raise ValueError("historical base root-scope hash mismatch")
    declared = {
        (str(row.get("route_id")), str(row.get("field_id")))
        for row in scope.get("roots", ())
    }
    expected = {
        (route_id, field_id)
        for route_id, field_ids in TARGET_ROOTS_BY_ROUTE.items()
        for field_id in field_ids
    }
    if declared != expected or int(scope.get("root_count") or 0) != len(expected):
        raise ValueError("supplemental root authority differs from implemented target set")
    rows = {
        (str(row.get("route_id")), str(row.get("field_id"))): row
        for row in scope.get("roots", ())
    }
    activation = dict(scope.get("runtime_activation_contract") or {})
    if (
        activation.get("default") != "FROZEN"
        or activation.get("scope_mutation_allowed") is not False
        or activation.get("activation_unit") != "ONE_ROOT_ONE_VERIFIED_RECEIPT"
        or activation.get("candidate_receipt_hash_injection_required") is not True
        or activation.get("requirements_by_field") != RUNTIME_RECEIPT_REQUIREMENTS
    ):
        raise ValueError("supplemental runtime activation contract drift")
    for route_id, field_id in expected:
        field = registry.resolve(field_id)
        row = rows[(route_id, field_id)]
        if (
            row.get("entity_scope") != field.entity_scope
            or row.get("field_role") != field.field_role
            or row.get("pit_status") != field.pit_status
        ):
            raise ValueError(f"supplemental root semantics drift: {route_id}:{field_id}")
        if field_id in MATERIALIZATION_BLOCKED_ROOT_IDS and (
            row.get("materialization_status") != "NOT_MATERIALIZED"
            or row.get("runtime_ready") is not False
            or row.get("signal_sketch_allowed") is not False
            or row.get("strict_evaluation_allowed") is not False
            or row.get("runtime_activation")
            != "CANONICAL_FULL_DEVELOPMENT_RECEIPT_REQUIRED_PER_ROOT"
            or row.get("required_receipt")
            != RUNTIME_RECEIPT_REQUIREMENTS[field_id]["receipt_type"]
        ):
            raise ValueError(
                f"unmaterialized supplemental root escaped downstream block: {field_id}"
            )
    if scope.get("plate_industry_roots") or scope.get("reward_or_performance_used") is not False:
        raise ValueError("plate roots and performance-driven scope are forbidden")
    gap_audit = dict(scope.get("source_gap_audit") or {})
    gap_path = (REPO / str(gap_audit.get("path") or "")).resolve()
    if not gap_path.is_file() or _sha256(gap_path) != str(gap_audit.get("sha256") or ""):
        raise ValueError("supplemental source-gap audit evidence hash mismatch")
    effective = dict(base_allowlists)
    for route_id, field_ids in TARGET_ROOTS_BY_ROUTE.items():
        for field_id in field_ids:
            field = registry.resolve(field_id)
            if not field.search_eligible or route_id not in field.allowed_routes:
                raise ValueError(
                    f"supplemental root is not registry-qualified on {route_id}: {field_id}"
                )
        effective[route_id] = sorted(set(effective[route_id]) | set(field_ids))
    return effective


def _source_identity(
    *,
    repo_sha: str,
    tree_sha: str,
) -> tuple[str, str]:
    try:
        inside = subprocess.check_output(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=REPO,
            text=True,
            encoding="utf-8",
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        inside = "false"
    if inside == "true":
        tracked_status = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=REPO,
            text=True,
            encoding="utf-8",
        ).strip()
        if tracked_status:
            raise ValueError("tracked working tree must be clean for supplemental generation")
        observed_repo = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO, text=True, encoding="utf-8"
        ).strip()
        observed_tree = subprocess.check_output(
            ["git", "rev-parse", "HEAD^{tree}"],
            cwd=REPO,
            text=True,
            encoding="utf-8",
        ).strip()
        if repo_sha and repo_sha != observed_repo:
            raise ValueError("provided repo SHA differs from clean Git source")
        if tree_sha and tree_sha != observed_tree:
            raise ValueError("provided tree SHA differs from clean Git source")
        return observed_repo, observed_tree
    if len(repo_sha) != 40 or len(tree_sha) != 40:
        raise ValueError("no-git source closure requires explicit 40-char repo/tree SHA")
    return repo_sha, tree_sha


def _expected_baseline_receipt_binding(campaign: Mapping[str, Any]) -> dict[str, str]:
    unsigned = {key: value for key, value in campaign.items() if key != "contract_hash"}
    if str(campaign.get("contract_hash") or "") != stable_hash(unsigned):
        raise ValueError("delta campaign self-hash mismatch")
    matches = [
        row
        for row in campaign.get("baseline_contract", {}).get("artifacts", ())
        if row.get("role") == "pair_receipts"
    ]
    if len(matches) != 1 or len(str(matches[0].get("sha256") or "")) != 64:
        raise ValueError("delta campaign lacks one baseline pair-receipt hash")
    if matches[0].get("mode") != "READ_ONLY_HASH_REQUIRED":
        raise ValueError("baseline pair receipts are not immutable")
    binding = {
        "path": str(matches[0]["path"]),
        "sha256": str(matches[0]["sha256"]),
    }
    if binding != CANONICAL_BASELINE_RECEIPTS_BINDING:
        raise ValueError(
            "delta campaign baseline pair receipts differ from canonical immutable binding"
        )
    return binding


def generate(
    *,
    registry: UnifiedCapabilityRegistry,
    route_root_allowlists: Mapping[str, Iterable[str]],
    baseline_exact_ids: set[str] | None = None,
    runtime_receipts: Mapping[str, Mapping[str, str]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    baseline = set(baseline_exact_ids or ())
    verified_runtime_receipts = dict(runtime_receipts or {})
    if set(verified_runtime_receipts) - set(MATERIALIZATION_BLOCKED_ROOT_IDS):
        raise ValueError("runtime receipt binding contains a non-gated root")
    for route_id, targets in TARGET_ROOTS_BY_ROUTE.items():
        missing = set(targets) - set(route_root_allowlists.get(route_id, ()))
        if missing:
            raise ValueError(
                f"supplemental roots are absent from frozen scope on {route_id}: {sorted(missing)}"
            )
    grammar = CompositionalGrammarV2(
        registry,
        route_root_allowlist=route_root_allowlists,
    )
    receipts: list[dict[str, Any]] = []
    seen = set(baseline)
    observed = {route_id: set() for route_id in TARGET_ROOTS_BY_ROUTE}
    generated_pairs = baseline_collision_pairs = duplicate_pairs = 0
    for route_id in supplemental_skeleton_registry():
        targets = set(TARGET_ROOTS_BY_ROUTE[route_id])
        for seed in SEEDS:
            for attempt_index in range(ATTEMPTS_PER_ROUTE):
                generated_pairs += 1
                pair = grammar.propose_supplemental(
                    route_id,
                    attempt_index=attempt_index,
                    seed=seed,
                )
                if not pair.primary["legal"] or not pair.control["legal"]:
                    raise ValueError(
                        f"illegal supplemental pair: {route_id}:{seed}:{attempt_index}"
                    )
                pair_exact = {
                    str(pair.primary["exact_identity"]),
                    str(pair.control["exact_identity"]),
                }
                if pair_exact & baseline:
                    baseline_collision_pairs += 1
                    continue
                if pair_exact & seen:
                    duplicate_pairs += 1
                    continue
                seen.update(pair_exact)
                receipt = _compact_receipt(
                    pair,
                    route_attempt_index=attempt_index,
                    runtime_receipts=verified_runtime_receipts,
                )
                receipts.append(receipt)
                observed[route_id].update(
                    set(receipt["declared_field_ids"]) & targets
                )
    missing_coverage = {
        route_id: sorted(set(TARGET_ROOTS_BY_ROUTE[route_id]) - values)
        for route_id, values in observed.items()
        if set(TARGET_ROOTS_BY_ROUTE[route_id]) - values
    }
    if missing_coverage:
        raise ValueError(f"supplemental target coverage incomplete: {missing_coverage}")
    coverage = {
        "status": "SUPPLEMENTAL_STRUCTURAL_SMOKE_NOT_CAMPAIGN_BUDGET_CONSUMPTION",
        "generator_version": SUPPLEMENTAL_GRAMMAR_VERSION,
        "routes": {
            route_id: {
                "target_root_ids": list(TARGET_ROOTS_BY_ROUTE[route_id]),
                "observed_root_ids": sorted(values),
                "all_target_roots_observed": True,
            }
            for route_id, values in observed.items()
        },
        "generated_pair_attempts": generated_pairs,
        "retained_exact_unique_pairs": len(receipts),
        "internal_duplicate_pairs": duplicate_pairs,
        "baseline_collision_pairs": baseline_collision_pairs,
        "baseline_exact_identity_count": len(baseline),
        "baseline_global_exact_dedup_applied": bool(baseline),
        "runtime_receipt_bound_root_ids": sorted(verified_runtime_receipts),
        "runtime_receipt_bound_root_count": len(verified_runtime_receipts),
        "runtime_ready_pair_count": sum(
            row.get("runtime_ready") is True for row in receipts
        ),
        "runtime_frozen_pair_count": sum(
            row.get("runtime_ready") is not True for row in receipts
        ),
        "runtime_ready_gated_pair_count": sum(
            row.get("runtime_ready") is True
            and bool(
                set(row.get("declared_field_ids") or ())
                & set(MATERIALIZATION_BLOCKED_ROOT_IDS)
            )
            for row in receipts
        ),
        "runtime_frozen_gated_pair_count": sum(
            row.get("runtime_ready") is not True
            and bool(
                set(row.get("declared_field_ids") or ())
                & set(MATERIALIZATION_BLOCKED_ROOT_IDS)
            )
            for row in receipts
        ),
        "campaign_budget_consumed": 0,
        "existing_pack_rewritten": False,
        "performance_or_reward_used": False,
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
    }
    return receipts, coverage


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument(
        "--supplemental-scope", type=Path, default=DEFAULT_SUPPLEMENTAL_SCOPE
    )
    parser.add_argument("--campaign-contract", type=Path, default=DEFAULT_CAMPAIGN)
    parser.add_argument("--baseline-receipts", type=Path, required=True)
    parser.add_argument(
        "--materialization-support-receipt",
        type=Path,
        action="append",
        default=[],
        help=(
            "canonical full-development receipt; repeat once per gated root "
            "that may become runtime-ready"
        ),
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--repo-sha", default="")
    parser.add_argument("--tree-sha", default="")
    args = parser.parse_args()
    registry_path = args.registry.resolve()
    supplemental_scope_path = args.supplemental_scope.resolve()
    campaign_path = args.campaign_contract.resolve()
    registry = UnifiedCapabilityRegistry.read(registry_path)
    supplemental_scope = json.loads(
        supplemental_scope_path.read_text(encoding="utf-8")
    )
    base_scope_path = (
        REPO / str(supplemental_scope["baseline_scope"]["path"])
    ).resolve()
    base_scope = json.loads(base_scope_path.read_text(encoding="utf-8"))
    effective_allowlists = validate_supplemental_scope(
        scope=supplemental_scope,
        base_scope=base_scope,
        registry=registry,
    )
    campaign = json.loads(campaign_path.read_text(encoding="utf-8"))
    expected_baseline = _expected_baseline_receipt_binding(campaign)
    repo_sha, tree_sha = _source_identity(
        repo_sha=str(args.repo_sha),
        tree_sha=str(args.tree_sha),
    )
    baseline_path = args.baseline_receipts.resolve()
    normalized_actual = str(baseline_path).replace("\\", "/").casefold()
    normalized_expected = str(expected_baseline["path"]).replace("\\", "/").casefold()
    if normalized_actual != normalized_expected:
        raise ValueError("baseline pair receipts path differs from canonical campaign binding")
    if _sha256(baseline_path) != expected_baseline["sha256"]:
        raise ValueError("baseline pair receipts do not match canonical campaign hash")
    runtime_receipts = load_runtime_receipt_bindings(
        args.materialization_support_receipt,
        registry=registry,
    )
    receipts, coverage = generate(
        registry=registry,
        route_root_allowlists=effective_allowlists,
        baseline_exact_ids=_read_baseline_exact(baseline_path),
        runtime_receipts=runtime_receipts,
    )
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    receipts_path = output / "CN_SUPPLEMENTAL_PAIR_RECEIPTS.jsonl"
    coverage_path = output / "CN_SUPPLEMENTAL_ROOT_COVERAGE.json"
    manifest_path = output / "CN_SUPPLEMENTAL_GENERATION_MANIFEST.json"
    _write_jsonl(receipts_path, receipts)
    _write_json(coverage_path, coverage)
    source_hashes = {
        role: {"path": str(path), "sha256": _sha256(path)}
        for role, path in SOURCE_PATHS.items()
    }
    manifest = {
        "status": coverage["status"],
        "repo_sha": repo_sha,
        "tree_sha": tree_sha,
        "generator_version": SUPPLEMENTAL_GRAMMAR_VERSION,
        "run_role": "STRUCTURAL_SMOKE_NOT_CAMPAIGN_BUDGET_CONSUMPTION",
        "campaign_budget_consumed": 0,
        "fixed_routes": list(supplemental_skeleton_registry()),
        "fixed_seeds": list(SEEDS),
        "fixed_attempts_per_route": ATTEMPTS_PER_ROUTE,
        "registry_path": str(registry_path),
        "registry_sha256": _sha256(registry_path),
        "registry_content_hash": registry.registry_hash,
        "supplemental_scope_path": str(supplemental_scope_path),
        "supplemental_scope_sha256": _sha256(supplemental_scope_path),
        "supplemental_scope_contract_hash": supplemental_scope["contract_hash"],
        "campaign_contract_path": str(campaign_path),
        "campaign_contract_sha256": _sha256(campaign_path),
        "campaign_contract_hash": campaign["contract_hash"],
        "historical_base_scope_path": str(base_scope_path),
        "historical_base_scope_sha256": _sha256(base_scope_path),
        "historical_base_contract_hash": str(base_scope.get("contract_hash") or ""),
        "baseline_receipts_path": str(baseline_path),
        "baseline_receipts_sha256": _sha256(baseline_path),
        "expected_baseline_receipts_path": expected_baseline["path"],
        "expected_baseline_receipts_sha256": expected_baseline["sha256"],
        "global_baseline_exact_dedup_status": "APPLIED",
        "runtime_receipt_bindings": runtime_receipts,
        "source_hashes": source_hashes,
        "artifacts": {
            receipts_path.name: _sha256(receipts_path),
            coverage_path.name: _sha256(coverage_path),
        },
        "artifact_set_hash": stable_hash(
            {
                receipts_path.name: _sha256(receipts_path),
                coverage_path.name: _sha256(coverage_path),
            }
        ),
        "performance_or_reward_used": False,
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
        "candidate_promotion": False,
    }
    _write_json(manifest_path, manifest)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
