"""Fixed V0 sampling across the eight unified CN candidate templates."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Sequence

from our_system_phase2.services.candidate_representation_v0 import (
    BROAD_EVENT_TEMPLATE_ID,
    TEMPLATE_CONTRACTS_V0,
    CandidateSpecV0,
)
from our_system_phase2.services.compositional_grammar import (
    CompositionalGrammarV2,
)
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    stable_hash,
)


FIXED_STRATIFIED_PLAN_VERSION = "cn_fixed_stratified_sampling_plan_v0"
FIXED_STRATIFIED_POLICY_ID = (
    "TEMPLATE_STRATIFIED_FIXED_NO_CROSS_TEMPLATE_CREDIT_V0"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class FixedStratifiedEpochResultV0:
    plan: dict[str, Any]
    summary: dict[str, Any]
    ledger: list[dict[str, Any]]
    candidate_specs: list[dict[str, Any]]
    candidate_rows: list[dict[str, Any]]
    waterfall: list[dict[str, Any]]


def _require_sha256(value: str, *, field: str) -> str:
    normalized = str(value).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{field} must be a SHA-256 digest")
    return normalized


def _plan_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in plan.items() if key != "plan_hash"}


def build_fixed_stratified_plan_v0(
    *,
    template_attempt_quotas: Mapping[str, int],
    seeds: Sequence[int],
    registry_hash: str,
    root_scope_hash: str,
    frozen_inventory_hashes: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Freeze per-template quotas without credit, spillover, or reallocation."""

    if set(template_attempt_quotas) != set(ROUTE_IDS):
        raise ValueError("V0 quotas must cover exactly all eight templates")
    normalized_seeds = tuple(sorted({int(seed) for seed in seeds}))
    if not normalized_seeds:
        raise ValueError("V0 fixed sampling requires at least one seed")
    quotas = {
        template_id: int(template_attempt_quotas[template_id])
        for template_id in ROUTE_IDS
    }
    if any(value < 0 for value in quotas.values()):
        raise ValueError("V0 template quotas cannot be negative")
    if any(value % len(normalized_seeds) for value in quotas.values()):
        raise ValueError("V0 quotas must be balanced across frozen seeds")

    inventories = {
        str(template_id): _require_sha256(value, field="frozen inventory")
        for template_id, value in dict(frozen_inventory_hashes or {}).items()
    }
    unknown_inventory = set(inventories) - {BROAD_EVENT_TEMPLATE_ID}
    if unknown_inventory:
        raise ValueError(
            f"frozen inventory supplied for non-replay templates: "
            f"{sorted(unknown_inventory)}"
        )
    if quotas[BROAD_EVENT_TEMPLATE_ID] > 0 and not inventories.get(
        BROAD_EVENT_TEMPLATE_ID
    ):
        raise ValueError(
            "positive Broad Event quota requires a frozen inventory binding"
        )

    plan: dict[str, Any] = {
        "schema_version": FIXED_STRATIFIED_PLAN_VERSION,
        "policy_id": FIXED_STRATIFIED_POLICY_ID,
        "top_level_scheduling_key": "route_id",
        "template_id_contract": "IDENTICAL_TO_ROUTE_ID_NO_SECOND_AUTHORITY",
        "template_attempt_quotas": quotas,
        "seeds": list(normalized_seeds),
        "registry_hash": _require_sha256(
            registry_hash, field="registry_hash"
        ),
        "root_scope_hash": _require_sha256(
            root_scope_hash, field="root_scope_hash"
        ),
        "frozen_inventory_hashes": inventories,
        "template_contracts": {
            template_id: {
                "template_id": contract.template_id,
                "route_id": contract.route_id,
                "template_version": contract.template_version,
                "sampling_kind": contract.sampling_kind,
                "new_generation_allowed": contract.new_generation_allowed,
            }
            for template_id, contract in TEMPLATE_CONTRACTS_V0.items()
        },
        "shared_tpe_study": False,
        "shared_tpe_credit": False,
        "optimizer_feedback_accessed": False,
        "dynamic_budget_reallocation_allowed": False,
        "underfill_spillover_allowed": False,
        "performance_data_accessed": False,
        "sealed_data_accessed": False,
    }
    plan["plan_hash"] = stable_hash(plan)
    return plan


def _verify_plan(plan: Mapping[str, Any]) -> None:
    if str(plan.get("schema_version") or "") != FIXED_STRATIFIED_PLAN_VERSION:
        raise ValueError("fixed stratified plan version mismatch")
    if str(plan.get("policy_id") or "") != FIXED_STRATIFIED_POLICY_ID:
        raise ValueError("fixed stratified policy mismatch")
    if str(plan.get("plan_hash") or "") != stable_hash(_plan_payload(plan)):
        raise ValueError("fixed stratified plan hash mismatch")
    if set(dict(plan.get("template_attempt_quotas") or {})) != set(ROUTE_IDS):
        raise ValueError("fixed stratified plan template coverage drift")
    forbidden_true = (
        "shared_tpe_study",
        "shared_tpe_credit",
        "optimizer_feedback_accessed",
        "dynamic_budget_reallocation_allowed",
        "underfill_spillover_allowed",
        "performance_data_accessed",
        "sealed_data_accessed",
    )
    if any(bool(plan.get(key)) for key in forbidden_true):
        raise ValueError("fixed stratified plan enables forbidden adaptation")


def iter_fixed_stratified_attempts_v0(
    plan: Mapping[str, Any],
) -> Iterator[dict[str, Any]]:
    _verify_plan(plan)
    seeds = tuple(int(value) for value in plan["seeds"])
    quotas = {
        str(key): int(value)
        for key, value in dict(plan["template_attempt_quotas"]).items()
    }
    global_ordinal = 0
    for template_id in ROUTE_IDS:
        per_seed = quotas[template_id] // len(seeds)
        contract = TEMPLATE_CONTRACTS_V0[template_id]
        for seed_slot, seed in enumerate(seeds):
            for local_index in range(per_seed):
                route_attempt_index = seed_slot * per_seed + local_index
                attempt_id = (
                    f"cn.v0.fixed.{template_id.lower()}.{seed}."
                    f"{local_index:06d}"
                )
                yield {
                    "attempt_id": attempt_id,
                    "global_ordinal": global_ordinal,
                    "template_id": template_id,
                    "route_id": template_id,
                    "template_version": contract.template_version,
                    "sampling_kind": contract.sampling_kind,
                    "new_generation_allowed": contract.new_generation_allowed,
                    "seed": seed,
                    "route_attempt_index": route_attempt_index,
                    "sampler_id": FIXED_STRATIFIED_POLICY_ID,
                    "optimizer_feedback_eligible": False,
                    "budget_reallocation_eligible": False,
                }
                global_ordinal += 1


def generate_fixed_stratified_epoch_v0(
    registry: UnifiedCapabilityRegistry,
    *,
    plan: Mapping[str, Any],
    route_root_allowlist: Mapping[str, Sequence[str]] | None = None,
) -> FixedStratifiedEpochResultV0:
    """Materialize a zero-financial eight-template production-supply epoch."""

    _verify_plan(plan)
    if str(plan.get("registry_hash") or "") != registry.registry_hash:
        raise ValueError("fixed stratified plan registry binding mismatch")
    grammar = CompositionalGrammarV2(
        registry,
        route_root_allowlist=route_root_allowlist,
    )
    counters = {template_id: Counter() for template_id in ROUTE_IDS}
    ledger: list[dict[str, Any]] = []
    candidate_specs: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    seen_exact: set[str] = set()
    seen_pairs: set[str] = set()

    for attempt in iter_fixed_stratified_attempts_v0(plan):
        template_id = str(attempt["template_id"])
        counter = counters[template_id]
        counter["scheduled"] += 1
        counter["attempted"] += 1
        base_ledger = {
            **attempt,
            "shared_tpe_credit": False,
            "optimizer_feedback_accessed": False,
            "dynamic_budget_reallocation_allowed": False,
            "underfill_spillover_allowed": False,
        }
        try:
            pair = grammar.propose(
                template_id,
                attempt_index=int(attempt["route_attempt_index"]),
                seed=int(attempt["seed"]),
            )
        except Exception as exc:
            counter["generation_failed"] += 1
            ledger.append(
                {
                    **base_ledger,
                    "outcome": "GENERATION_FAILED",
                    "failure_type": type(exc).__name__,
                    "failure_reason": str(exc),
                }
            )
            continue

        primary = pair.primary
        control = pair.control
        primary_legal = bool(primary.get("legal"))
        control_legal = bool(control.get("legal"))
        if primary_legal:
            counter["primary_legal"] += 1
        if control_legal:
            counter["control_valid"] += 1
        if not primary_legal or not control_legal:
            counter["legal_or_control_failed"] += 1
            ledger.append(
                {
                    **base_ledger,
                    "candidate_id": str(primary.get("candidate_id") or ""),
                    "pair_id": str(primary.get("pair_id") or ""),
                    "outcome": "LEGAL_OR_CONTROL_FAILED",
                    "primary_legal": primary_legal,
                    "control_valid": control_legal,
                }
            )
            continue

        exact_identity = str(primary.get("exact_identity") or "")
        pair_id = str(primary.get("pair_id") or "")
        duplicate_exact = exact_identity in seen_exact
        duplicate_pair = pair_id in seen_pairs
        if duplicate_exact or duplicate_pair:
            counter["duplicate"] += 1
            ledger.append(
                {
                    **base_ledger,
                    "candidate_id": str(primary.get("candidate_id") or ""),
                    "pair_id": pair_id,
                    "exact_identity": exact_identity,
                    "outcome": "DUPLICATE_IDENTITY",
                    "duplicate_exact_identity": duplicate_exact,
                    "duplicate_pair_id": duplicate_pair,
                }
            )
            continue

        seen_exact.add(exact_identity)
        seen_pairs.add(pair_id)
        counter["primary_exact_unique"] += 1
        counter["pair_unique"] += 1
        for member in (primary, control):
            spec = CandidateSpecV0.from_candidate_row(
                member,
                attempt_id=str(attempt["attempt_id"]),
                route_attempt_index=int(attempt["route_attempt_index"]),
                sampler_id=FIXED_STRATIFIED_POLICY_ID,
            )
            spec_record = spec.to_record()
            candidate_specs.append(spec_record)
            candidate_rows.append(
                {
                    **dict(member),
                    "template_id": spec.template_id,
                    "template_version": spec.template_version,
                    "sampling_kind": spec.sampling_kind,
                    "candidate_spec_v0_hash": spec.spec_hash,
                    "candidate_proposal_v0_hash": spec.proposal_hash,
                    "candidate_representation_version": (
                        spec.schema_version
                    ),
                    "fixed_sampling_policy_id": (
                        FIXED_STRATIFIED_POLICY_ID
                    ),
                    "optimizer_feedback_eligible": False,
                    "dynamic_budget_reallocation_eligible": False,
                }
            )
        ledger.append(
            {
                **base_ledger,
                "candidate_id": str(primary.get("candidate_id") or ""),
                "control_candidate_id": str(
                    control.get("candidate_id") or ""
                ),
                "pair_id": pair_id,
                "exact_identity": exact_identity,
                "outcome": "ACCEPTED_EXACT_UNIQUE_PAIR",
                "primary_legal": True,
                "control_valid": True,
            }
        )

    waterfall: list[dict[str, Any]] = []
    for template_id in ROUTE_IDS:
        counter = counters[template_id]
        contract = TEMPLATE_CONTRACTS_V0[template_id]
        scheduled = int(counter["scheduled"])
        unique = int(counter["primary_exact_unique"])
        waterfall.append(
            {
                "template_id": template_id,
                "route_id": template_id,
                "template_version": contract.template_version,
                "sampling_kind": contract.sampling_kind,
                "new_generation_allowed": contract.new_generation_allowed,
                "scheduled": scheduled,
                "attempted": int(counter["attempted"]),
                "primary_legal": int(counter["primary_legal"]),
                "control_valid": int(counter["control_valid"]),
                "primary_exact_unique": unique,
                "pair_unique": int(counter["pair_unique"]),
                "candidate_specs": unique * 2,
                "duplicate": int(counter["duplicate"]),
                "generation_failed": int(counter["generation_failed"]),
                "legal_or_control_failed": int(
                    counter["legal_or_control_failed"]
                ),
                "underfill": scheduled - unique,
                "pair_evaluated": 0,
                "development_productive": 0,
                "standalone_positive": 0,
                "matched_positive": 0,
                "behavior_family_unique": None,
                "candidate_per_wall_hour": None,
                "productive_per_core_hour": None,
            }
        )

    summary = {
        "schema_version": "cn_fixed_stratified_generation_epoch_v0",
        "plan_hash": str(plan["plan_hash"]),
        "scheduled_attempts": sum(row["scheduled"] for row in waterfall),
        "attempted": sum(row["attempted"] for row in waterfall),
        "legal_control_valid_attempts": sum(
            min(row["primary_legal"], row["control_valid"])
            for row in waterfall
        ),
        "primary_exact_unique": sum(
            row["primary_exact_unique"] for row in waterfall
        ),
        "candidate_spec_count": len(candidate_specs),
        "compatible_candidate_row_count": len(candidate_rows),
        "economic_evaluator_accessed": False,
        "optimizer_feedback_accessed": False,
        "shared_tpe_credit": False,
        "dynamic_budget_reallocation_allowed": False,
        "underfill_spillover_allowed": False,
        "sealed_data_read_count": 0,
        "validation_read_count": 0,
        "holdout_read_count": 0,
        "forward_2026_read_count": 0,
        "promotion_authorized": False,
    }
    return FixedStratifiedEpochResultV0(
        plan=dict(plan),
        summary=summary,
        ledger=ledger,
        candidate_specs=candidate_specs,
        candidate_rows=candidate_rows,
        waterfall=waterfall,
    )
