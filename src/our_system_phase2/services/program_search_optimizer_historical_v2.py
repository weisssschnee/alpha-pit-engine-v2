"""Experimental Program-space challengers ported from proven Crypto search policies.

The formal Program optimizer authority is unchanged.  Both adapters operate only
on the frozen legal AvailabilityEntry catalog and consume development feedback
through ProgramOptimizerObservationV1.
"""
from __future__ import annotations

import copy
import math
import random
from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from our_system_phase2.services.candidate_program_proposal_v0 import (
    PROGRAM_TEMPLATE_COMPONENTS,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    PROGRAM_ROUTE_ID,
    ProgramOptimizerObservationV1,
    _AvailabilityProgramOptimizer,
    _batch_feasible_entries,
    _batch_group_counts,
    _record_batch_group_selection,
)
from our_system_phase2.services.route_local_availability import (
    AvailabilityEntry,
    RouteLocalAvailabilityController,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


HIERARCHICAL_CEM_PROGRAM_V2 = "HIERARCHICAL_CEM_PROGRAM_V2"
CATALOG_TYPED_EVOLUTION_PROGRAM_V2 = "CATALOG_TYPED_EVOLUTION_PROGRAM_V2"

DERIVED_PROGRAM_SLOTS = frozenset(
    {
        "skeleton_id",
        "gene_surface_id",
        "program_template_id",
        "active_component_roles",
        "composition_topology",
        "joint_clock_class",
        "lag_class",
        "structural_complexity_class",
        "raw_field_count",
        "rolling_node_count",
        "interaction_topology",
    }
)
COMBINATION_SLOT_BY_ROLE = {
    "temporal": ("combination_temporal",),
    "market": ("combination_market",),
    "event": ("combination_event_episode", "combination_event_application"),
}
EVOLUTION_OPERATIONS = (
    "EFFECTIVE_FACTOR_MUTATION_1_TO_3",
    "COMPATIBLE_COMPONENT_SKELETON_MUTATION",
    "ONE_POINT_HOMOLOGOUS_FACTOR_BUNDLE_CROSSOVER",
)


def _tupleize(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_tupleize(item) for item in value)
    return value


def _listify(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_listify(item) for item in value]
    if isinstance(value, list):
        return [_listify(item) for item in value]
    return value


def _template_entries(
    entries: Sequence[AvailabilityEntry], template_id: str
) -> tuple[AvailabilityEntry, ...]:
    rows = tuple(
        entry
        for entry in entries
        if str(entry.genes["program_template_id"]) == str(template_id)
    )
    if not rows:
        raise ValueError(f"PROGRAM_HISTORICAL_TEMPLATE_EMPTY:{template_id}")
    return rows


def program_factor_plan_v2(
    entries: Sequence[AvailabilityEntry], template_id: str
) -> tuple[str, ...]:
    """Return only active, controllable, varying Program factors in hierarchy order."""

    rows = _template_entries(entries, template_id)
    roles = PROGRAM_TEMPLATE_COMPONENTS[str(template_id)]
    plan: list[str] = []
    all_slots = tuple(dict(rows[0].genes))
    for role in roles:
        prefix = f"{role}__"
        preferred = (prefix + "route_id", prefix + "skeleton_id")
        remainder = tuple(
            sorted(
                slot
                for slot in all_slots
                if slot.startswith(prefix) and slot not in preferred
            )
        )
        for slot in (*preferred, *remainder):
            if slot not in rows[0].genes:
                continue
            if len({str(row.genes[slot]) for row in rows}) > 1:
                plan.append(slot)
    for role in roles:
        for slot in COMBINATION_SLOT_BY_ROLE.get(role, ()):
            if slot in rows[0].genes and len({str(row.genes[slot]) for row in rows}) > 1:
                plan.append(slot)
    if not plan:
        raise ValueError(f"PROGRAM_HISTORICAL_NO_VARIABLE_FACTORS:{template_id}")
    if any(slot in DERIVED_PROGRAM_SLOTS for slot in plan):
        raise RuntimeError("PROGRAM_HISTORICAL_DERIVED_FACTOR_DRIFT")
    return tuple(plan)


def _component_role(slot: str) -> str | None:
    return str(slot).split("__", 1)[0] if "__" in str(slot) else None


def _sampling_contexts(
    *, template_id: str, slot: str, selected: Mapping[str, str]
) -> tuple[str, ...]:
    role = _component_role(slot)
    if role is not None:
        route = selected.get(f"{role}__route_id")
        skeleton = selected.get(f"{role}__skeleton_id")
        output = []
        if route is not None and skeleton is not None:
            output.append(f"E|{template_id}|{role}|{route}|{skeleton}|{slot}")
        if route is not None:
            output.append(f"R|{template_id}|{role}|{route}|{slot}")
        output.append(f"T|{template_id}|{slot}")
        return tuple(output)
    routes = ";".join(
        f"{role}={selected.get(f'{role}__route_id', '')}"
        for role in PROGRAM_TEMPLATE_COMPONENTS[str(template_id)]
    )
    return (f"E|{template_id}|{routes}|{slot}", f"T|{template_id}|{slot}")


def _entry_contexts(
    *, template_id: str, slot: str, entry: AvailabilityEntry
) -> tuple[str, ...]:
    return _sampling_contexts(
        template_id=template_id,
        slot=slot,
        selected={str(key): str(value) for key, value in entry.genes.items()},
    )


def _productive_objective(
    observation: ProgramOptimizerObservationV1,
) -> tuple[bool, float | None]:
    if not observation.admission.admitted or observation.uplift is None:
        return False, None
    reward = float(
        observation.uplift.program_credit.get("matched_net_reward_increment", 0.0)
    )
    objective = observation.conditional_objective
    if reward <= 0.0 or objective is None or not math.isfinite(float(objective)):
        return False, None
    return True, float(objective)


class HierarchicalProgramCEMV2(_AvailabilityProgramOptimizer):
    """Hierarchical categorical CEM adapted from Crypto HierarchicalTypedCEMV2."""

    def __init__(
        self,
        *,
        elite_fraction: float = 0.25,
        smoothing: float = 0.35,
        minimum_probability: float = 0.002,
        entropy_floor_ratio: float = 0.60,
        minimum_observation_count: int = 4,
        count_pseudocount: float = 0.50,
        **kwargs: Any,
    ) -> None:
        super().__init__(arm=HIERARCHICAL_CEM_PROGRAM_V2, **kwargs)
        self.elite_fraction = float(elite_fraction)
        self.smoothing = float(smoothing)
        self.minimum_probability = float(minimum_probability)
        self.entropy_floor_ratio = float(entropy_floor_ratio)
        self.minimum_observation_count = int(minimum_observation_count)
        self.count_pseudocount = float(count_pseudocount)
        if not 0.0 < self.elite_fraction <= 0.5:
            raise ValueError("PROGRAM_CEM_ELITE_FRACTION_INVALID")
        if not 0.0 < self.smoothing <= 1.0:
            raise ValueError("PROGRAM_CEM_SMOOTHING_INVALID")
        if not 0.0 <= self.entropy_floor_ratio <= 1.0:
            raise ValueError("PROGRAM_CEM_ENTROPY_FLOOR_INVALID")
        if self.minimum_observation_count < 1 or self.count_pseudocount < 0.0:
            raise ValueError("PROGRAM_CEM_OBSERVATION_CONTRACT_INVALID")
        self.rng = random.Random(self.controller.emitter_seed)
        templates = sorted({str(entry.genes["program_template_id"]) for entry in self.entries})
        self.factor_plans = {
            template: program_factor_plan_v2(self.entries, template)
            for template in templates
        }
        self.tables: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self.update_count = 0

    @staticmethod
    def _entropy(probabilities: Sequence[float]) -> float:
        return float(-sum(p * math.log(p) for p in probabilities if p > 0.0))

    def _regularized(
        self, values: Sequence[str], weights: Mapping[str, float]
    ) -> dict[str, float]:
        ordered = tuple(sorted(map(str, values)))
        if not ordered:
            raise ValueError("PROGRAM_CEM_EMPTY_LEGAL_DOMAIN")
        if len(ordered) == 1:
            return {ordered[0]: 1.0}
        floor = self.minimum_probability
        if floor * len(ordered) >= 1.0:
            floor = min(floor, 0.5 / len(ordered))
        raw = [max(0.0, float(weights.get(value, 0.0))) for value in ordered]
        if not any(value > 0.0 for value in raw):
            raw = [1.0] * len(ordered)
        total = sum(raw)
        probs = [floor + (1.0 - floor * len(ordered)) * value / total for value in raw]
        entropy_floor = self.entropy_floor_ratio * math.log(len(ordered))
        if self._entropy(probs) < entropy_floor:
            uniform = 1.0 / len(ordered)
            for index in range(1, 51):
                mixture = index / 50.0
                candidate = [
                    (1.0 - mixture) * value + mixture * uniform for value in probs
                ]
                if self._entropy(candidate) >= entropy_floor:
                    probs = candidate
                    break
        return dict(zip(ordered, map(float, probs), strict=True))

    def _choice(
        self,
        *,
        slot: str,
        legal_values: Sequence[str],
        contexts: Sequence[str],
    ) -> str:
        ordered = tuple(sorted(set(map(str, legal_values))))
        probabilities = None
        for context in contexts:
            table = self.tables.get(slot, {}).get(context)
            if table is None or int(table.get("observations", 0)) < self.minimum_observation_count:
                continue
            probabilities = self._regularized(
                ordered, dict(table.get("probabilities") or {})
            )
            break
        if probabilities is None:
            probabilities = self._regularized(
                ordered, {value: 1.0 for value in ordered}
            )
        return str(
            self.rng.choices(
                ordered, weights=[probabilities[value] for value in ordered], k=1
            )[0]
        )

    def _sample_exact(
        self, *, template_id: str, remaining: Sequence[AvailabilityEntry]
    ) -> AvailabilityEntry:
        pool = list(remaining)
        selected: dict[str, str] = {}
        for slot in self.factor_plans[template_id]:
            legal_values = tuple(sorted({str(entry.genes[slot]) for entry in pool}))
            value = self._choice(
                slot=slot,
                legal_values=legal_values,
                contexts=_sampling_contexts(
                    template_id=template_id, slot=slot, selected=selected
                ),
            )
            selected[slot] = value
            pool = [entry for entry in pool if str(entry.genes[slot]) == value]
            if not pool:
                raise RuntimeError("PROGRAM_CEM_LEGAL_PREFIX_COLLAPSE")
        return self.rng.choice(pool)

    def ask(
        self,
        *,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str | None = None,
        eligible_exact_identities: Sequence[str] | None = None,
        batch_group_constraint: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if self._pending:
            raise RuntimeError("PROGRAM_OPTIMIZER_PENDING_NOT_TOLD")
        if required_program_template_id is None:
            raise ValueError("PROGRAM_CEM_REQUIRES_TEMPLATE_LANE")
        template_id = str(required_program_template_id)
        constraint, group_counts = _batch_group_counts(batch_group_constraint)
        asked = []
        for ordinal in range(int(count)):
            remaining = _batch_feasible_entries(
                self._remaining_entries(template_id, eligible_exact_identities),
                constraint=constraint,
                group_counts=group_counts,
            )
            if not remaining:
                break
            entry = self._sample_exact(template_id=template_id, remaining=remaining)
            emission = self.controller.reserve_exact(
                route_id=PROGRAM_ROUTE_ID,
                exact_identity=entry.exact_identity,
                emission_mode="PROGRAM_HIERARCHICAL_CEM_V2",
                source_exact_identity=stable_hash(
                    {
                        "template": template_id,
                        "factor_plan": self.factor_plans[template_id],
                        "step": self.observation_count + ordinal,
                    }
                ),
            )
            if emission is None:
                raise RuntimeError("PROGRAM_CEM_RESERVATION_FAILED")
            feasibility = _record_batch_group_selection(
                emission.exact_identity,
                constraint=constraint,
                group_counts=group_counts,
            )
            feasibility["batch_feasible_count_at_selection"] = len(remaining)
            proposal_id = stable_hash(
                {
                    "arm": self.arm,
                    "checkpoint_id": checkpoint_id,
                    "ask_ordinal": ordinal,
                    "exact_identity": emission.exact_identity,
                    "update_count": self.update_count,
                }
            )[:24]
            row = self._ask_row(
                emission=emission,
                checkpoint_id=checkpoint_id,
                ask_ordinal=ordinal,
                proposal_id=proposal_id,
                trial_number=None,
                optimizer_ask_identity=proposal_id,
                acquisition={
                    "source": "HIERARCHICAL_TYPED_CEM_PORT",
                    "update_count": self.update_count,
                    "factor_plan": list(self.factor_plans[template_id]),
                },
                batch_group_feasibility=feasibility,
            )
            asked.append(row)
            self._pending[proposal_id] = row
        return asked

    def _update_tables(self, elite_entries: Sequence[AvailabilityEntry]) -> None:
        accumulator: dict[str, dict[str, Counter[str]]] = defaultdict(
            lambda: defaultdict(Counter)
        )
        for entry in elite_entries:
            template = str(entry.genes["program_template_id"])
            for slot in self.factor_plans[template]:
                value = str(entry.genes[slot])
                for context in _entry_contexts(
                    template_id=template, slot=slot, entry=entry
                ):
                    accumulator[slot][context][value] += 1
        for slot, contexts in accumulator.items():
            for context, additions in contexts.items():
                table = self.tables[slot].setdefault(
                    context, {"observations": 0, "counts": {}, "probabilities": {}}
                )
                diagnostic = Counter(
                    {str(k): int(v) for k, v in dict(table["counts"]).items()}
                )
                diagnostic.update(additions)
                table["counts"] = dict(sorted(diagnostic.items()))
                table["observations"] = int(sum(additions.values()))
                prior = dict(table.get("probabilities") or {})
                values = tuple(sorted(set(prior) | set(additions)))
                total = int(sum(additions.values()))
                empirical = {
                    value: (additions[value] + self.count_pseudocount)
                    / (total + self.count_pseudocount * len(values))
                    for value in values
                }
                blended = {
                    value: (1.0 - self.smoothing)
                    * float(prior.get(value, 1.0 / len(values)))
                    + self.smoothing * empirical[value]
                    for value in values
                }
                table["probabilities"] = self._regularized(values, blended)
        self.update_count += 1

    def entropy_summary(self) -> dict[str, float]:
        return {
            slot: float(
                sum(
                    self._entropy(list(dict(table.get("probabilities") or {}).values()))
                    for table in contexts.values()
                    if table.get("probabilities")
                )
                / max(1, sum(bool(table.get("probabilities")) for table in contexts.values()))
            )
            for slot, contexts in sorted(self.tables.items())
        }

    def tell(
        self, observations: Sequence[ProgramOptimizerObservationV1]
    ) -> dict[str, Any]:
        by_id = {row.proposal_id: row for row in observations}
        if set(by_id) != set(self._pending):
            raise RuntimeError("PROGRAM_OPTIMIZER_ASK_TELL_COVERAGE_DRIFT")
        ranked = []
        entry_by_exact = {entry.exact_identity: entry for entry in self.entries}
        for proposal_id, ask in self._pending.items():
            feasible, objective = _productive_objective(by_id[proposal_id])
            if feasible:
                ranked.append((float(objective), str(ask["exact_identity"])))
        elite_entries = []
        elite_target_count = max(
            1, int(math.ceil(len(by_id) * self.elite_fraction))
        )
        update_applied = False
        if ranked:
            ranked.sort(key=lambda row: (row[0], row[1]), reverse=True)
            elite_entries = [
                entry_by_exact[exact]
                for _, exact in ranked[:elite_target_count]
            ]
            if len(elite_entries) >= self.minimum_observation_count:
                self._update_tables(elite_entries)
                update_applied = True
        receipt = {
            "schema_version": "cn_hierarchical_program_cem_tell_v2",
            "optimizer_arm": self.arm,
            "asked_count": len(by_id),
            "productive_feasible_count": len(ranked),
            "elite_target_count": elite_target_count,
            "elite_count": len(elite_entries),
            "update_applied": update_applied,
            "update_count": self.update_count,
            "entropy_summary": self.entropy_summary(),
            "objective": "MAX_MATCHED_CUMULATIVE_RETURN_INCREMENT",
            "feasibility": "ABSOLUTE_ADMISSION_AND_MATCHED_NET_REWARD_POSITIVE",
        }
        self._history.append(receipt)
        self._pending.clear()
        return receipt

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            "optimizer_arm": self.arm,
            "algorithm_origin": "CRYPTO_HIERARCHICAL_TYPED_CEM_V2_PORT",
            "factor_plans": {k: list(v) for k, v in sorted(self.factor_plans.items())},
            "elite_fraction": self.elite_fraction,
            "smoothing": self.smoothing,
            "minimum_probability": self.minimum_probability,
            "entropy_floor_ratio": self.entropy_floor_ratio,
            "minimum_observation_count": self.minimum_observation_count,
            "count_pseudocount": self.count_pseudocount,
            "update_count": self.update_count,
        }

    def snapshot(self) -> dict[str, Any]:
        payload = super().snapshot()
        payload.pop("snapshot_hash")
        payload["tables"] = copy.deepcopy(dict(self.tables))
        payload["update_count"] = self.update_count
        payload["rng_state"] = _listify(self.rng.getstate())
        payload["snapshot_hash"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(cls, **kwargs: Any) -> "HierarchicalProgramCEMV2":
        snapshot = dict(kwargs.pop("snapshot"))
        adapter = cls(**kwargs)
        adapter.controller = RouteLocalAvailabilityController.restore(
            entries=adapter.entries,
            seen_exact_identities=adapter.seen_exact_identities,
            state=dict(snapshot["availability"]),
            input_hashes=adapter.controller.input_hashes,
        )
        adapter._history = copy.deepcopy(list(snapshot["history"]))
        adapter.tables = defaultdict(
            dict,
            {
                str(slot): {str(ctx): dict(table) for ctx, table in contexts.items()}
                for slot, contexts in dict(snapshot["tables"]).items()
            },
        )
        adapter.update_count = int(snapshot["update_count"])
        adapter.rng.setstate(_tupleize(snapshot["rng_state"]))
        return adapter


class CatalogTypedEvolutionProgramV2(_AvailabilityProgramOptimizer):
    """Typed evolution over frozen legal Program entries, adapted from Crypto V2."""

    def __init__(
        self,
        *,
        warmup: int = 32,
        tournament_size: int = 4,
        population_limit: int = 256,
        gene_mutation_probability: float = 0.55,
        skeleton_mutation_probability: float = 0.25,
        crossover_probability: float = 0.20,
        minimum_mutated_factors: int = 1,
        maximum_mutated_factors: int = 3,
        duplicate_resample_limit: int = 64,
        **kwargs: Any,
    ) -> None:
        super().__init__(arm=CATALOG_TYPED_EVOLUTION_PROGRAM_V2, **kwargs)
        self.warmup = int(warmup)
        self.tournament_size = int(tournament_size)
        self.population_limit = int(population_limit)
        self.minimum_mutated_factors = int(minimum_mutated_factors)
        self.maximum_mutated_factors = int(maximum_mutated_factors)
        self.duplicate_resample_limit = int(duplicate_resample_limit)
        weights = {
            EVOLUTION_OPERATIONS[0]: float(gene_mutation_probability),
            EVOLUTION_OPERATIONS[1]: float(skeleton_mutation_probability),
            EVOLUTION_OPERATIONS[2]: float(crossover_probability),
        }
        if any(value < 0.0 for value in weights.values()) or not math.isclose(
            sum(weights.values()), 1.0, rel_tol=0.0, abs_tol=1e-12
        ):
            raise ValueError("PROGRAM_EVOLUTION_OPERATION_PROBABILITY_INVALID")
        self.operation_probabilities = weights
        self.rng = random.Random(self.controller.emitter_seed)
        templates = sorted({str(entry.genes["program_template_id"]) for entry in self.entries})
        self.factor_plans = {
            template: program_factor_plan_v2(self.entries, template)
            for template in templates
        }
        self.entry_by_exact = {entry.exact_identity: entry for entry in self.entries}
        self.population: dict[str, dict[str, Any]] = {}
        self.operation_productivity = {
            operation: {"trials": 0, "productive": 0}
            for operation in EVOLUTION_OPERATIONS
        }

    def _parent(self, template_id: str) -> AvailabilityEntry:
        eligible = [
            (exact, row)
            for exact, row in self.population.items()
            if bool(row["feasible"])
            and str(self.entry_by_exact[exact].genes["program_template_id"]) == template_id
        ]
        if not eligible:
            raise RuntimeError("PROGRAM_EVOLUTION_PARENT_POOL_EMPTY")
        size = min(self.tournament_size, len(eligible))
        sampled = self.rng.sample(eligible, size)
        exact, _ = max(sampled, key=lambda item: (float(item[1]["score"]), item[0]))
        return self.entry_by_exact[exact]

    def _factor_distance(
        self, left: AvailabilityEntry, right: AvailabilityEntry, template_id: str
    ) -> int:
        return sum(
            str(left.genes[slot]) != str(right.genes[slot])
            for slot in self.factor_plans[template_id]
        )

    def _gene_mutation(
        self, parent: AvailabilityEntry, remaining: Sequence[AvailabilityEntry]
    ) -> tuple[AvailabilityEntry, dict[str, Any]] | None:
        template = str(parent.genes["program_template_id"])
        target = self.rng.randint(
            self.minimum_mutated_factors,
            min(self.maximum_mutated_factors, len(self.factor_plans[template])),
        )
        candidates = [
            entry
            for entry in remaining
            if self._factor_distance(parent, entry, template) == target
        ]
        if not candidates:
            return None
        child = self.rng.choice(candidates)
        changed = [
            slot
            for slot in self.factor_plans[template]
            if str(parent.genes[slot]) != str(child.genes[slot])
        ]
        return child, {
            "operation": EVOLUTION_OPERATIONS[0],
            "parent_ids": [parent.exact_identity],
            "changed_factors": changed,
        }

    def _skeleton_mutation(
        self, parent: AvailabilityEntry, remaining: Sequence[AvailabilityEntry]
    ) -> tuple[AvailabilityEntry, dict[str, Any]] | None:
        template = str(parent.genes["program_template_id"])
        roles = PROGRAM_TEMPLATE_COMPONENTS[template]
        skeleton_slots = [
            f"{role}__skeleton_id"
            for role in roles
            if f"{role}__skeleton_id" in parent.genes
        ]
        route_slots = [
            f"{role}__route_id" for role in roles if f"{role}__route_id" in parent.genes
        ]
        candidates = []
        for entry in remaining:
            if any(str(entry.genes[slot]) != str(parent.genes[slot]) for slot in route_slots):
                continue
            changed_skeletons = sum(
                str(entry.genes[slot]) != str(parent.genes[slot]) for slot in skeleton_slots
            )
            if changed_skeletons != 1:
                continue
            candidates.append(entry)
        if not candidates:
            return None
        child = min(
            candidates,
            key=lambda entry: (
                self._factor_distance(parent, entry, template),
                stable_hash({"parent": parent.exact_identity, "child": entry.exact_identity}),
            ),
        )
        return child, {
            "operation": EVOLUTION_OPERATIONS[1],
            "parent_ids": [parent.exact_identity],
            "changed_skeleton_slots": [
                slot
                for slot in skeleton_slots
                if str(parent.genes[slot]) != str(child.genes[slot])
            ],
        }

    def _crossover(
        self,
        first: AvailabilityEntry,
        remaining: Sequence[AvailabilityEntry],
    ) -> tuple[AvailabilityEntry, dict[str, Any]] | None:
        template = str(first.genes["program_template_id"])
        peers = [
            self.entry_by_exact[exact]
            for exact, row in self.population.items()
            if bool(row["feasible"])
            and exact != first.exact_identity
            and str(self.entry_by_exact[exact].genes["program_template_id"]) == template
        ]
        self.rng.shuffle(peers)
        plan = self.factor_plans[template]
        if len(plan) < 2:
            return None
        remaining_by_signature = {
            tuple(str(entry.genes[slot]) for slot in plan): entry for entry in remaining
        }
        for second in peers:
            points = list(range(1, len(plan)))
            self.rng.shuffle(points)
            for point in points:
                signature = tuple(
                    str((first if index < point else second).genes[slot])
                    for index, slot in enumerate(plan)
                )
                child = remaining_by_signature.get(signature)
                if child is None:
                    continue
                return child, {
                    "operation": EVOLUTION_OPERATIONS[2],
                    "parent_ids": [first.exact_identity, second.exact_identity],
                    "crossover_point": point,
                    "factor_order": list(plan),
                }
        return None

    def _propose_child(
        self, *, template_id: str, remaining: Sequence[AvailabilityEntry]
    ) -> tuple[AvailabilityEntry, dict[str, Any]]:
        operations = tuple(self.operation_probabilities)
        weights = [self.operation_probabilities[operation] for operation in operations]
        for _ in range(self.duplicate_resample_limit):
            parent = self._parent(template_id)
            operation = self.rng.choices(operations, weights=weights, k=1)[0]
            result = (
                self._gene_mutation(parent, remaining)
                if operation == EVOLUTION_OPERATIONS[0]
                else self._skeleton_mutation(parent, remaining)
                if operation == EVOLUTION_OPERATIONS[1]
                else self._crossover(parent, remaining)
            )
            if result is not None:
                return result
        raise RuntimeError("PROGRAM_EVOLUTION_NO_LEGAL_CHILD_WITHIN_ATTEMPT_LIMIT")

    def ask(
        self,
        *,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str | None = None,
        eligible_exact_identities: Sequence[str] | None = None,
        batch_group_constraint: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if self._pending:
            raise RuntimeError("PROGRAM_OPTIMIZER_PENDING_NOT_TOLD")
        if required_program_template_id is None:
            raise ValueError("PROGRAM_EVOLUTION_REQUIRES_TEMPLATE_LANE")
        template_id = str(required_program_template_id)
        constraint, group_counts = _batch_group_counts(batch_group_constraint)
        asked = []
        for ordinal in range(int(count)):
            remaining = _batch_feasible_entries(
                self._remaining_entries(template_id, eligible_exact_identities),
                constraint=constraint,
                group_counts=group_counts,
            )
            if not remaining:
                break
            feasible_parents = sum(
                bool(row["feasible"])
                and str(self.entry_by_exact[exact].genes["program_template_id"]) == template_id
                for exact, row in self.population.items()
            )
            if len(self.population) < self.warmup or feasible_parents == 0:
                entry = self.rng.choice(list(remaining))
                receipt = {
                    "operation": "UNIFORM_WARMUP",
                    "parent_ids": [],
                }
            else:
                entry, receipt = self._propose_child(
                    template_id=template_id, remaining=remaining
                )
            emission = self.controller.reserve_exact(
                route_id=PROGRAM_ROUTE_ID,
                exact_identity=entry.exact_identity,
                emission_mode="PROGRAM_CATALOG_TYPED_EVOLUTION_V2",
                source_exact_identity=stable_hash(receipt),
            )
            if emission is None:
                raise RuntimeError("PROGRAM_EVOLUTION_RESERVATION_FAILED")
            feasibility = _record_batch_group_selection(
                emission.exact_identity,
                constraint=constraint,
                group_counts=group_counts,
            )
            feasibility["batch_feasible_count_at_selection"] = len(remaining)
            proposal_id = stable_hash(
                {
                    "arm": self.arm,
                    "checkpoint_id": checkpoint_id,
                    "ask_ordinal": ordinal,
                    "exact_identity": emission.exact_identity,
                    "operation_receipt": receipt,
                }
            )[:24]
            row = self._ask_row(
                emission=emission,
                checkpoint_id=checkpoint_id,
                ask_ordinal=ordinal,
                proposal_id=proposal_id,
                trial_number=None,
                optimizer_ask_identity=proposal_id,
                acquisition={"source": "CRYPTO_TYPED_EVOLUTION_V2_PORT", **receipt},
                batch_group_feasibility=feasibility,
            )
            asked.append(row)
            self._pending[proposal_id] = row
        return asked

    def tell(
        self, observations: Sequence[ProgramOptimizerObservationV1]
    ) -> dict[str, Any]:
        by_id = {row.proposal_id: row for row in observations}
        if set(by_id) != set(self._pending):
            raise RuntimeError("PROGRAM_OPTIMIZER_ASK_TELL_COVERAGE_DRIFT")
        productive = 0
        operation_counts = Counter()
        for proposal_id, ask in self._pending.items():
            feasible, objective = _productive_objective(by_id[proposal_id])
            exact = str(ask["exact_identity"])
            operation = str(dict(ask.get("acquisition") or {}).get("operation") or "")
            self.population[exact] = {
                "feasible": feasible,
                "score": float(objective) if objective is not None else None,
                "operation": operation,
            }
            if operation in self.operation_productivity:
                self.operation_productivity[operation]["trials"] += 1
                self.operation_productivity[operation]["productive"] += int(feasible)
                operation_counts[operation] += 1
            productive += int(feasible)
        if len(self.population) > self.population_limit:
            ordered = sorted(
                self.population.items(),
                key=lambda item: (
                    bool(item[1]["feasible"]),
                    float(item[1]["score"]) if item[1]["score"] is not None else float("-inf"),
                    item[0],
                ),
                reverse=True,
            )[: self.population_limit]
            self.population = dict(ordered)
        receipt = {
            "schema_version": "cn_catalog_typed_evolution_program_tell_v2",
            "optimizer_arm": self.arm,
            "asked_count": len(by_id),
            "productive_feasible_count": productive,
            "population_size": len(self.population),
            "operation_counts": dict(operation_counts),
            "operation_productivity": copy.deepcopy(self.operation_productivity),
            "objective": "MAX_MATCHED_CUMULATIVE_RETURN_INCREMENT",
            "feasibility": "ABSOLUTE_ADMISSION_AND_MATCHED_NET_REWARD_POSITIVE",
        }
        self._history.append(receipt)
        self._pending.clear()
        return receipt

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            "optimizer_arm": self.arm,
            "algorithm_origin": "CRYPTO_TYPED_EVOLUTION_V2_PORT",
            "factor_plans": {k: list(v) for k, v in sorted(self.factor_plans.items())},
            "warmup": self.warmup,
            "tournament_size": self.tournament_size,
            "population_limit": self.population_limit,
            "operation_probabilities": dict(self.operation_probabilities),
            "operation_productivity": copy.deepcopy(self.operation_productivity),
        }

    def snapshot(self) -> dict[str, Any]:
        payload = super().snapshot()
        payload.pop("snapshot_hash")
        payload["population"] = copy.deepcopy(self.population)
        payload["operation_productivity"] = copy.deepcopy(self.operation_productivity)
        payload["rng_state"] = _listify(self.rng.getstate())
        payload["snapshot_hash"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(cls, **kwargs: Any) -> "CatalogTypedEvolutionProgramV2":
        snapshot = dict(kwargs.pop("snapshot"))
        adapter = cls(**kwargs)
        adapter.controller = RouteLocalAvailabilityController.restore(
            entries=adapter.entries,
            seen_exact_identities=adapter.seen_exact_identities,
            state=dict(snapshot["availability"]),
            input_hashes=adapter.controller.input_hashes,
        )
        adapter._history = copy.deepcopy(list(snapshot["history"]))
        adapter.population = copy.deepcopy(dict(snapshot["population"]))
        adapter.operation_productivity = copy.deepcopy(
            dict(snapshot["operation_productivity"])
        )
        adapter.rng.setstate(_tupleize(snapshot["rng_state"]))
        return adapter
