"""Feedback-dark generation and structural pre-admission for CN epoch 1."""

from __future__ import annotations

from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.expression_semantics import ExpressionNode, parse_expression
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


GENERATION_EPOCH_VERSION = "cn_compositional_generation_epoch_v1"
SEARCHABLE_ROUTES = (
    "MINUTE_STATIC",
    "FIRSTN_PATH",
    "SLOW_CROSS_SECTIONAL_LEVEL",
    "SLOW_TEMPORAL_CHANGE",
    "DISCLOSURE_EVENT",
    "MARKET_REGIME_CONDITION",
    "INTRADAY_STATE_TRANSITION",
)


@dataclass(frozen=True, slots=True)
class GenerationEpochResult:
    summary: dict[str, Any]
    ledger: list[dict[str, Any]]
    unique_pairs: list[dict[str, Any]]
    waterfall: list[dict[str, Any]]


def _depth(node: ExpressionNode) -> int:
    if not node.args:
        return 0
    return 1 + max(_depth(child) for child in node.args)


def _policy_state_hash(policy_id: str, seed: int, route_id: str) -> str:
    return stable_hash(
        {
            "policy_id": policy_id,
            "seed": int(seed),
            "route_id": route_id,
            "state": "FROZEN_UNIFORM_PRIOR_NO_FEEDBACK_ACCESSED",
        }
    )


def _failure_from_exception(message: str) -> str:
    if "FIELD_COVERAGE_BOTTLENECK" in message or "empty field pool" in message:
        return "FIELD_COVERAGE_BOTTLENECK"
    if "UNIT" in message:
        return "UNIT_CONTRACT_REJECTION"
    if "PIT" in message or "CLOCK" in message:
        return "PIT_OR_CLOCK_REJECTION"
    if "CONTROL" in message:
        return "CONTROL_CONSTRUCTION_FAILURE"
    return "SEMANTIC_ALIAS_COLLAPSE"


def _pair_record(
    registry: UnifiedCapabilityRegistry,
    *,
    primary: Mapping[str, Any],
    control: Mapping[str, Any],
    policy_id: str,
    seed: int,
    attempt_id: str,
    parent_candidate_id: str,
    mutation_receipt: str,
) -> dict[str, Any]:
    declared = tuple(sorted(str(value) for value in primary.get("declared_field_ids", ())))
    source_families = tuple(sorted({registry.resolve(field_id).source_family for field_id in declared}))
    canonical = str(primary["canonical_expression"])
    depth = _depth(parse_expression(canonical))
    operator_path_hash = stable_hash(list(primary.get("operator_paths", ())))
    return {
        "attempt_id": attempt_id,
        "candidate_id": str(primary["candidate_id"]),
        "control_candidate_id": str(control["candidate_id"]),
        "pair_id": str(primary["pair_id"]),
        "route_id": str(primary["route_id"]),
        "skeleton_id": str(primary["skeleton_id"]),
        "policy_id": policy_id,
        "seed": int(seed),
        "exact_identity": str(primary["exact_identity"]),
        "canonical_identity": str(primary["canonical_identity"]),
        "canonical_expression": canonical,
        "declared_field_ids": list(declared),
        "source_families": list(source_families),
        "expression_depth": depth,
        "operator_path_hash": operator_path_hash,
        "parent_candidate_id": parent_candidate_id,
        "mutation_receipt": mutation_receipt,
        "behavior_sketch_status": "PENDING_DEVELOPMENT_SIGNAL_SKETCH",
        "admission_reward_accessed": False,
        "primary": dict(primary),
        "control": dict(control),
    }


def build_compositional_generation_epoch(
    registry: UnifiedCapabilityRegistry,
    *,
    route_attempt_quotas: Mapping[str, int],
    policies: Sequence[str],
    seeds: Sequence[int],
) -> GenerationEpochResult:
    if set(route_attempt_quotas) != set(SEARCHABLE_ROUTES):
        raise ValueError("generation quotas must cover exactly the seven searchable routes")
    if not policies or not seeds:
        raise ValueError("policies and seeds are required")
    cells = len(policies) * len(seeds)
    if any(int(quota) <= 0 or int(quota) % cells for quota in route_attempt_quotas.values()):
        raise ValueError("every route quota must be positive and policy-seed balanced")

    grammar = CompositionalGrammarV2(registry)
    ledger: list[dict[str, Any]] = []
    unique_pairs: list[dict[str, Any]] = []
    exact_exposure: Counter[str] = Counter()
    seen_exact: set[str] = set()
    failure_counts: Counter[str] = Counter()
    route_counters: dict[str, Counter[str]] = {
        route_id: Counter() for route_id in SEARCHABLE_ROUTES
    }
    previous_candidate_by_cell: dict[tuple[str, str, int], str] = {}

    for route_id in SEARCHABLE_ROUTES:
        per_cell = int(route_attempt_quotas[route_id]) // cells
        for policy_index, policy_id in enumerate(policies):
            for seed_index, seed in enumerate(seeds):
                cell = (route_id, str(policy_id), int(seed))
                state_hash = _policy_state_hash(str(policy_id), int(seed), route_id)
                cell_offset = (policy_index * len(seeds) + seed_index) * per_cell
                for local_index in range(per_cell):
                    route_attempt_index = cell_offset + local_index
                    attempt_id = (
                        f"cn.comp.epoch1.{route_id.lower()}.{policy_id}."
                        f"{int(seed)}.{local_index:06d}"
                    )
                    route_counters[route_id]["proposal_attempts"] += 1
                    try:
                        pair = grammar.propose(
                            route_id,
                            attempt_index=route_attempt_index,
                            seed=int(seed),
                        )
                        primary, control = pair.primary, pair.control
                    except Exception as exc:
                        category = _failure_from_exception(str(exc))
                        failure_counts[category] += 1
                        route_counters[route_id][category] += 1
                        ledger.append(
                            {
                                "attempt_id": attempt_id,
                                "route_id": route_id,
                                "policy_id": str(policy_id),
                                "seed": int(seed),
                                "skeleton_id": "",
                                "candidate_id": "",
                                "exact_identity": "",
                                "legal": False,
                                "control_valid": False,
                                "first_visit": False,
                                "cache_hit": False,
                                "strict_call": False,
                                "cumulative_exposure_count": 0,
                                "policy_state_hash": state_hash,
                                "parent_candidate_id": previous_candidate_by_cell.get(cell, ""),
                                "mutation_receipt": "",
                                "outcome": category,
                            }
                        )
                        continue

                    primary_legal = bool(primary.get("legal"))
                    control_legal = bool(control.get("legal"))
                    if not primary_legal or not control_legal:
                        category = (
                            "CONTROL_CONSTRUCTION_FAILURE"
                            if primary_legal and not control_legal
                            else "SEMANTIC_ALIAS_COLLAPSE"
                        )
                        failure_counts[category] += 1
                        route_counters[route_id][category] += 1
                        exact_identity = str(primary.get("exact_identity") or "")
                        ledger.append(
                            {
                                "attempt_id": attempt_id,
                                "route_id": route_id,
                                "policy_id": str(policy_id),
                                "seed": int(seed),
                                "skeleton_id": str(primary.get("skeleton_id") or ""),
                                "candidate_id": str(primary.get("candidate_id") or ""),
                                "exact_identity": exact_identity,
                                "legal": primary_legal,
                                "control_valid": control_legal,
                                "first_visit": False,
                                "cache_hit": False,
                                "strict_call": False,
                                "cumulative_exposure_count": 0,
                                "policy_state_hash": state_hash,
                                "parent_candidate_id": previous_candidate_by_cell.get(cell, ""),
                                "mutation_receipt": "",
                                "outcome": category,
                            }
                        )
                        continue

                    route_counters[route_id]["parse_pass"] += 1
                    route_counters[route_id]["route_type_pass"] += 1
                    route_counters[route_id]["pit_pass"] += 1
                    route_counters[route_id]["unit_pass"] += 1
                    route_counters[route_id]["control_valid"] += 1
                    exact_identity = str(primary["exact_identity"])
                    exact_exposure[exact_identity] += 1
                    first_visit = exact_identity not in seen_exact
                    parent_id = (
                        previous_candidate_by_cell.get(cell, "")
                        if policy_id == "evolutionary_mutation"
                        else ""
                    )
                    mutation_receipt = (
                        "TYPED_FIELD_WINDOW_AND_SKELETON_MUTATION"
                        if parent_id
                        else "INITIAL_POPULATION_MEMBER"
                        if policy_id == "evolutionary_mutation"
                        else ""
                    )
                    record = _pair_record(
                        registry,
                        primary=primary,
                        control=control,
                        policy_id=str(policy_id),
                        seed=int(seed),
                        attempt_id=attempt_id,
                        parent_candidate_id=parent_id,
                        mutation_receipt=mutation_receipt,
                    )
                    if first_visit:
                        seen_exact.add(exact_identity)
                        unique_pairs.append(record)
                        route_counters[route_id]["exact_unique"] += 1
                        outcome = "ACCEPTED_EXACT_UNIQUE"
                    else:
                        failure_counts["SEMANTIC_ALIAS_COLLAPSE"] += 1
                        route_counters[route_id]["SEMANTIC_ALIAS_COLLAPSE"] += 1
                        outcome = "DUPLICATE_EXACT_IDENTITY"
                    previous_candidate_by_cell[cell] = str(primary["candidate_id"])
                    ledger.append(
                        {
                            "attempt_id": attempt_id,
                            "route_id": route_id,
                            "policy_id": str(policy_id),
                            "seed": int(seed),
                            "skeleton_id": str(primary["skeleton_id"]),
                            "candidate_id": str(primary["candidate_id"]),
                            "exact_identity": exact_identity,
                            "legal": True,
                            "control_valid": True,
                            "first_visit": first_visit,
                            "cache_hit": not first_visit,
                            "strict_call": False,
                            "cumulative_exposure_count": exact_exposure[exact_identity],
                            "policy_state_hash": state_hash,
                            "parent_candidate_id": parent_id,
                            "mutation_receipt": mutation_receipt,
                            "outcome": outcome,
                        }
                    )

    waterfall: list[dict[str, Any]] = []
    for route_id in SEARCHABLE_ROUTES:
        counter = route_counters[route_id]
        waterfall.append(
            {
                "route_id": route_id,
                "proposal_attempts": int(counter["proposal_attempts"]),
                "parse_pass": int(counter["parse_pass"]),
                "route_type_pass": int(counter["route_type_pass"]),
                "pit_pass": int(counter["pit_pass"]),
                "unit_pass": int(counter["unit_pass"]),
                "control_valid": int(counter["control_valid"]),
                "exact_unique": int(counter["exact_unique"]),
                "behavior_unique": None,
                "materialization_pass": None,
                "support_pass": None,
                "pair_evaluated": 0,
                "gross_positive": 0,
                "net_positive": 0,
                "matched_positive": 0,
                "pair_native_ready": 0,
                "independent_positive_cluster": 0,
                "cross_seed_reproduced_cluster": 0,
            }
        )
    summary = {
        "generation_epoch_version": GENERATION_EPOCH_VERSION,
        "proposal_attempts": len(ledger),
        "legal_control_valid_attempts": sum(row["control_valid"] for row in waterfall),
        "legal_exact_unique_primaries": len(unique_pairs),
        "economic_evaluator_accessed": False,
        "access_roles": [],
        "broad_event_new_search_attempts": 0,
        "failure_counts": dict(sorted(failure_counts.items())),
        "formal_search_unfrozen": False,
        "promotion_allowed": False,
        "forward_2026_accessed": False,
    }
    return GenerationEpochResult(summary, ledger, unique_pairs, waterfall)


def select_structural_preadmission(
    unique_pairs: Sequence[Mapping[str, Any]],
    *,
    maximum_pairs: int,
) -> list[dict[str, Any]]:
    if maximum_pairs <= 0:
        raise ValueError("maximum_pairs must be positive")
    buckets: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for raw in unique_pairs:
        row = dict(raw)
        key = (
            str(row["route_id"]),
            str(row["skeleton_id"]),
            tuple(row["source_families"]),
            str(row["operator_path_hash"]),
            int(row["expression_depth"]),
            int(row["seed"]),
            str(row["policy_id"]),
        )
        buckets[key].append(row)
    ordered_keys = sorted(buckets, key=lambda key: stable_hash(list(key)))
    queues: dict[tuple[Any, ...], deque[dict[str, Any]]] = {}
    for key in ordered_keys:
        rows = sorted(
            buckets[key],
            key=lambda row: stable_hash(
                {"exact_identity": row["exact_identity"], "admission": "STRUCTURAL_PREADMISSION"}
            ),
        )
        queues[key] = deque(rows)
    output: list[dict[str, Any]] = []
    active = list(ordered_keys)
    while active and len(output) < maximum_pairs:
        next_active: list[tuple[Any, ...]] = []
        for key in active:
            queue = queues[key]
            if queue and len(output) < maximum_pairs:
                selected = queue.popleft()
                selected["admission_reward_accessed"] = False
                selected["structural_preadmission"] = True
                selected["behavior_sketch_status"] = "PENDING_DEVELOPMENT_SIGNAL_SKETCH"
                output.append(selected)
            if queue:
                next_active.append(key)
        active = next_active
    return output
