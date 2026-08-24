"""Semantic state-jump generator for CN typed Candidate Programs.

This module deliberately changes the *proposal-generation* surface rather than
reranking a frozen Program catalog.  It recomposes already-authorized component
pairs through ``CandidateProgramProposalAdapterV0`` so every proposal still
passes the existing typed Program, receipt, compiler, matched-control and
execution authorities.

Only development feedback may be supplied to the search memory.  The module has
no validation/holdout/forward readers and no financial evaluator dependency.
"""
from __future__ import annotations

import copy
import math
import random
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from our_system_phase2.services.candidate_program_proposal_v0 import (
    DEFAULT_COMBINATION_POLICY,
    EVENT_APPLICATION_POLICIES,
    MARKET_COMBINATION_POLICIES,
    PROGRAM_TEMPLATE_COMPONENTS,
    TEMPORAL_COMBINATION_POLICIES,
    CandidateProgramProposalAdapterV0,
    ProgramProposalReceiptV0,
    ProgramSourceComponentV0,
)
from our_system_phase2.services.candidate_program_v1 import CandidateProgramSpecV1
from our_system_phase2.services.unified_capability_registry import stable_hash


SEMANTIC_STATE_JUMP_GENERATOR_V2 = "SEMANTIC_STATE_JUMP_GENERATOR_V2"
STATE_JUMP_MEMORY_SCHEMA = "cn_program_state_jump_search_memory_v2"
STATE_JUMP_GENERATOR_SCHEMA = "cn_program_state_jump_generator_v2"
STATE_JUMP_GENERATOR_SNAPSHOT_SCHEMA = "cn_program_state_jump_generator_snapshot_v2"

STATE_JUMP_OPERATIONS = (
    "FRESH_RECOMPOSE",
    "ROLE_REPLACE",
    "MULTI_ROLE_JUMP",
    "POLICY_JUMP",
    "HOMOLOGOUS_RECOMBINE",
)
DEFAULT_OPERATION_PRIORS = {
    "FRESH_RECOMPOSE": 0.20,
    "ROLE_REPLACE": 0.20,
    "MULTI_ROLE_JUMP": 0.25,
    "POLICY_JUMP": 0.15,
    "HOMOLOGOUS_RECOMBINE": 0.20,
}


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    return parsed if math.isfinite(parsed) else float(default)


def _listify(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_listify(item) for item in value]
    if isinstance(value, list):
        return [_listify(item) for item in value]
    return value


def _tupleify(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_tupleify(item) for item in value)
    return value


def _component_family(component: ProgramSourceComponentV0) -> str:
    primary = dict(component.primary)
    family = (
        primary.get("skeleton_id")
        or primary.get("formula_family")
        or primary.get("mechanism_family")
        or primary.get("representation")
        or primary.get("frozen_mechanism_id")
        or component.route_id
    )
    return str(family)


def component_memory_key(component: ProgramSourceComponentV0) -> str:
    return f"{component.role}|{component.route_id}|{component.component_id}"


def program_region_key(
    *,
    template_id: str,
    components: Mapping[str, ProgramSourceComponentV0],
    combination_policy: Mapping[str, str],
) -> str:
    payload = {
        "template_id": str(template_id),
        "component_families": {
            role: {
                "route_id": component.route_id,
                "family": _component_family(component),
            }
            for role, component in sorted(components.items())
        },
        "combination_policy": dict(sorted((str(k), str(v)) for k, v in combination_policy.items())),
    }
    return stable_hash(payload)


@dataclass(slots=True)
class _OutcomeStats:
    observations: int = 0
    admitted: int = 0
    productive: int = 0
    blocked: int = 0
    redundant: int = 0
    matched_return_sum: float = 0.0
    matched_reward_sum: float = 0.0

    def observe(self, outcome: Mapping[str, Any]) -> None:
        self.observations += 1
        self.admitted += int(bool(outcome.get("admitted")))
        self.productive += int(bool(outcome.get("productive")))
        self.blocked += int(bool(outcome.get("blocked")))
        self.redundant += int(bool(outcome.get("redundant")))
        self.matched_return_sum += _finite(outcome.get("matched_return_increment"))
        self.matched_reward_sum += _finite(outcome.get("matched_reward_increment"))

    @property
    def smoothed_productive_rate(self) -> float:
        return (self.productive + 1.0) / (self.observations + 2.0)

    @property
    def smoothed_admission_rate(self) -> float:
        return (self.admitted + 1.0) / (self.observations + 2.0)

    @property
    def failure_rate(self) -> float:
        if not self.observations:
            return 0.0
        return (self.blocked + self.redundant) / self.observations

    def score(self) -> float:
        support = self.observations / (self.observations + 6.0)
        economics = 0.5 * (
            math.tanh(self.matched_return_sum / max(1, self.observations))
            + math.tanh(self.matched_reward_sum / max(1, self.observations))
        )
        return (
            0.70 * self.smoothed_productive_rate
            + 0.15 * self.smoothed_admission_rate
            + 0.15 * economics
        ) * (0.35 + 0.65 * support) - 0.30 * self.failure_rate

    def dead(self, *, minimum_support: int = 4) -> bool:
        return (
            self.observations >= int(minimum_support)
            and self.productive == 0
            and (self.failure_rate >= 0.50 or self.admitted == 0)
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "observations": self.observations,
            "admitted": self.admitted,
            "productive": self.productive,
            "blocked": self.blocked,
            "redundant": self.redundant,
            "matched_return_sum": self.matched_return_sum,
            "matched_reward_sum": self.matched_reward_sum,
        }

    @classmethod
    def from_record(cls, row: Mapping[str, Any]) -> "_OutcomeStats":
        return cls(
            observations=int(row.get("observations") or 0),
            admitted=int(row.get("admitted") or 0),
            productive=int(row.get("productive") or 0),
            blocked=int(row.get("blocked") or 0),
            redundant=int(row.get("redundant") or 0),
            matched_return_sum=_finite(row.get("matched_return_sum")),
            matched_reward_sum=_finite(row.get("matched_reward_sum")),
        )


@dataclass(slots=True)
class ProgramStateJumpSearchMemoryV2:
    """Development-only success/dead-region memory for Generator V2."""

    region_stats: dict[str, _OutcomeStats] = field(default_factory=dict)
    component_stats: dict[str, _OutcomeStats] = field(default_factory=dict)
    operation_stats: dict[str, _OutcomeStats] = field(default_factory=dict)
    seen_semantic_hashes: set[str] = field(default_factory=set)
    behavior_counts: Counter[str] = field(default_factory=Counter)
    observations: int = 0

    def observe(
        self,
        generated: "GeneratedProgramV2",
        *,
        admitted: bool,
        productive: bool,
        matched_return_increment: float = 0.0,
        matched_reward_increment: float = 0.0,
        blocked: bool = False,
        redundant: bool = False,
        behavior_identity: str = "",
    ) -> None:
        outcome = {
            "admitted": bool(admitted),
            "productive": bool(productive),
            "matched_return_increment": _finite(matched_return_increment),
            "matched_reward_increment": _finite(matched_reward_increment),
            "blocked": bool(blocked),
            "redundant": bool(redundant),
        }
        self.region_stats.setdefault(generated.region_key, _OutcomeStats()).observe(outcome)
        for component in generated.components.values():
            self.component_stats.setdefault(component_memory_key(component), _OutcomeStats()).observe(outcome)
        self.operation_stats.setdefault(generated.operation, _OutcomeStats()).observe(outcome)
        self.seen_semantic_hashes.add(generated.program.semantic_program_hash)
        if behavior_identity:
            self.behavior_counts[str(behavior_identity)] += 1
        self.observations += 1

    def component_score(self, component: ProgramSourceComponentV0) -> float:
        stats = self.component_stats.get(component_memory_key(component))
        if stats is None:
            return 0.50
        novelty_bonus = 0.12 / math.sqrt(stats.observations + 1.0)
        return stats.score() + novelty_bonus

    def operation_score(self, operation: str) -> float:
        stats = self.operation_stats.get(str(operation))
        if stats is None:
            return 0.50
        exploration = 0.18 * math.sqrt(
            math.log(self.observations + 2.0) / (stats.observations + 1.0)
        )
        return stats.score() + exploration

    def region_dead(self, region_key: str) -> bool:
        stats = self.region_stats.get(str(region_key))
        return bool(stats and stats.dead())

    def snapshot(self) -> dict[str, Any]:
        payload = {
            "schema_version": STATE_JUMP_MEMORY_SCHEMA,
            "observations": self.observations,
            "region_stats": {k: v.to_record() for k, v in sorted(self.region_stats.items())},
            "component_stats": {k: v.to_record() for k, v in sorted(self.component_stats.items())},
            "operation_stats": {k: v.to_record() for k, v in sorted(self.operation_stats.items())},
            "seen_semantic_hashes": sorted(self.seen_semantic_hashes),
            "behavior_counts": dict(sorted(self.behavior_counts.items())),
            "validation_feedback_allowed": False,
            "holdout_feedback_allowed": False,
            "forward_feedback_allowed": False,
        }
        payload["memory_payload_sha256"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(cls, payload: Mapping[str, Any]) -> "ProgramStateJumpSearchMemoryV2":
        row = copy.deepcopy(dict(payload))
        body = dict(row)
        claimed = str(body.pop("memory_payload_sha256", ""))
        if not claimed or stable_hash(body) != claimed:
            raise ValueError("STATE_JUMP_MEMORY_SELF_HASH_DRIFT")
        if row.get("schema_version") != STATE_JUMP_MEMORY_SCHEMA or any(
            bool(row.get(key))
            for key in ("validation_feedback_allowed", "holdout_feedback_allowed", "forward_feedback_allowed")
        ):
            raise ValueError("STATE_JUMP_MEMORY_AUTHORITY_DRIFT")
        return cls(
            region_stats={k: _OutcomeStats.from_record(v) for k, v in dict(row["region_stats"]).items()},
            component_stats={k: _OutcomeStats.from_record(v) for k, v in dict(row["component_stats"]).items()},
            operation_stats={k: _OutcomeStats.from_record(v) for k, v in dict(row["operation_stats"]).items()},
            seen_semantic_hashes=set(map(str, row["seen_semantic_hashes"])),
            behavior_counts=Counter({str(k): int(v) for k, v in dict(row["behavior_counts"]).items()}),
            observations=int(row["observations"]),
        )


@dataclass(frozen=True, slots=True)
class GeneratedProgramV2:
    program: CandidateProgramSpecV1
    receipt: ProgramProposalReceiptV0
    template_id: str
    components: Mapping[str, ProgramSourceComponentV0]
    combination_policy: Mapping[str, str]
    operation: str
    parent_semantic_hashes: tuple[str, ...]
    changed_slots: tuple[str, ...]
    region_key: str

    def summary(self) -> dict[str, Any]:
        return {
            "schema_version": STATE_JUMP_GENERATOR_SCHEMA,
            "semantic_program_hash": self.program.semantic_program_hash,
            "program_id": self.program.program_id,
            "template_id": self.template_id,
            "component_ids": {role: component.component_id for role, component in sorted(self.components.items())},
            "combination_policy": dict(self.combination_policy),
            "operation": self.operation,
            "parent_semantic_hashes": list(self.parent_semantic_hashes),
            "changed_slots": list(self.changed_slots),
            "region_key": self.region_key,
            "receipt_hash": stable_hash(self.receipt.to_record()),
        }


@dataclass(slots=True)
class _EliteSpec:
    template_id: str
    component_ids: dict[str, str]
    combination_policy: dict[str, str]
    semantic_program_hash: str
    quality: float


class SemanticStateJumpProgramGeneratorV2:
    """Generate novel legal Programs by semantic recomposition and state jumps."""

    def __init__(
        self,
        *,
        adapter: CandidateProgramProposalAdapterV0,
        components_by_role: Mapping[str, Sequence[ProgramSourceComponentV0]],
        memory: ProgramStateJumpSearchMemoryV2 | None = None,
        seed: int = 20260824,
        operation_priors: Mapping[str, float] | None = None,
        maximum_attempts: int = 96,
    ) -> None:
        self.adapter = adapter
        self.memory = memory or ProgramStateJumpSearchMemoryV2()
        self.rng = random.Random(int(seed))
        self.maximum_attempts = int(maximum_attempts)
        if self.maximum_attempts < 1:
            raise ValueError("STATE_JUMP_MAXIMUM_ATTEMPTS_INVALID")
        self.components_by_role = {
            str(role): tuple(sorted(components, key=lambda component: component.component_id))
            for role, components in components_by_role.items()
        }
        required_roles = {role for roles in PROGRAM_TEMPLATE_COMPONENTS.values() for role in roles}
        if not required_roles.issubset(self.components_by_role) or any(
            not self.components_by_role[role] for role in required_roles
        ):
            raise ValueError("STATE_JUMP_COMPONENT_POOL_UNDERFILLED")
        self.component_by_id = {
            component.component_id: component
            for components in self.components_by_role.values()
            for component in components
        }
        priors = dict(DEFAULT_OPERATION_PRIORS if operation_priors is None else operation_priors)
        if set(priors) != set(STATE_JUMP_OPERATIONS) or any(float(v) < 0.0 for v in priors.values()):
            raise ValueError("STATE_JUMP_OPERATION_PRIOR_INVALID")
        total = sum(float(v) for v in priors.values())
        if total <= 0.0:
            raise ValueError("STATE_JUMP_OPERATION_PRIOR_EMPTY")
        self.operation_priors = {key: float(value) / total for key, value in priors.items()}
        self._elites: list[_EliteSpec] = []
        self._generated_semantic_hashes: set[str] = set()

    @staticmethod
    def _policy_for_template(template_id: str, source: Mapping[str, str] | None = None) -> dict[str, str]:
        policy = {**DEFAULT_COMBINATION_POLICY, **dict(source or {})}
        roles = set(PROGRAM_TEMPLATE_COMPONENTS[str(template_id)])
        if "temporal" not in roles:
            policy["temporal"] = DEFAULT_COMBINATION_POLICY["temporal"]
        if "market" not in roles:
            policy["market"] = DEFAULT_COMBINATION_POLICY["market"]
        if "event" not in roles:
            policy["event_application"] = DEFAULT_COMBINATION_POLICY["event_application"]
        return policy

    def _choose_operation(self, template_id: str) -> str:
        same_template_elites = [elite for elite in self._elites if elite.template_id == template_id]
        if not same_template_elites:
            return "FRESH_RECOMPOSE"
        scores = []
        for operation in STATE_JUMP_OPERATIONS:
            prior = self.operation_priors[operation]
            score = prior * (0.5 + max(0.0, self.memory.operation_score(operation)))
            if operation == "HOMOLOGOUS_RECOMBINE" and len(same_template_elites) < 2:
                score = 0.0
            scores.append(score)
        total = sum(scores)
        if total <= 0.0:
            return "FRESH_RECOMPOSE"
        return self.rng.choices(STATE_JUMP_OPERATIONS, weights=scores, k=1)[0]

    def _choose_component(self, role: str, *, exclude: set[str] | None = None, exploration: bool = False) -> ProgramSourceComponentV0:
        excluded = exclude or set()
        candidates = [component for component in self.components_by_role[role] if component.component_id not in excluded]
        if not candidates:
            raise RuntimeError(f"STATE_JUMP_NO_COMPONENT_ALTERNATIVE:{role}")
        if exploration:
            minimum_observations = min(
                self.memory.component_stats.get(
                    component_memory_key(component), _OutcomeStats()
                ).observations
                for component in candidates
            )
            least_observed = [
                component
                for component in candidates
                if self.memory.component_stats.get(
                    component_memory_key(component), _OutcomeStats()
                ).observations
                == minimum_observations
            ]
            return self.rng.choice(least_observed)
        best_score = max(self.memory.component_score(component) for component in candidates)
        shortlist = [component for component in candidates if self.memory.component_score(component) >= best_score - 0.05]
        return self.rng.choice(shortlist)

    def _elite(self, template_id: str) -> _EliteSpec:
        candidates = [elite for elite in self._elites if elite.template_id == template_id]
        if not candidates:
            raise RuntimeError("STATE_JUMP_ELITE_POOL_EMPTY")
        size = min(4, len(candidates))
        sampled = self.rng.sample(candidates, size)
        return max(sampled, key=lambda elite: (elite.quality, elite.semantic_program_hash))

    def _components_from_elite(self, elite: _EliteSpec) -> dict[str, ProgramSourceComponentV0]:
        return {role: self.component_by_id[component_id] for role, component_id in elite.component_ids.items()}

    def _fresh(self, template_id: str) -> tuple[dict[str, ProgramSourceComponentV0], dict[str, str], tuple[str, ...], tuple[str, ...]]:
        roles = PROGRAM_TEMPLATE_COMPONENTS[template_id]
        components = {
            role: self._choose_component(role, exploration=(self.rng.random() < 0.35))
            for role in roles
        }
        return components, self._policy_for_template(template_id), (), tuple(f"component:{role}" for role in roles)

    def _jump(self, template_id: str, operation: str) -> tuple[dict[str, ProgramSourceComponentV0], dict[str, str], tuple[str, ...], tuple[str, ...]]:
        if operation == "FRESH_RECOMPOSE":
            return self._fresh(template_id)
        first = self._elite(template_id)
        components = self._components_from_elite(first)
        policy = self._policy_for_template(template_id, first.combination_policy)
        parents = [first.semantic_program_hash]
        changed: list[str] = []
        roles = list(PROGRAM_TEMPLATE_COMPONENTS[template_id])
        if operation == "ROLE_REPLACE":
            role = self.rng.choice(roles)
            components[role] = self._choose_component(role, exclude={components[role].component_id})
            changed.append(f"component:{role}")
        elif operation == "MULTI_ROLE_JUMP":
            count = min(len(roles), 2)
            for role in self.rng.sample(roles, count):
                components[role] = self._choose_component(
                    role, exclude={components[role].component_id}, exploration=True
                )
                changed.append(f"component:{role}")
        elif operation == "POLICY_JUMP":
            slots: list[tuple[str, Sequence[str]]] = []
            if "temporal" in roles:
                slots.append(("temporal", sorted(TEMPORAL_COMBINATION_POLICIES)))
            if "market" in roles:
                slots.append(("market", sorted(MARKET_COMBINATION_POLICIES)))
            if "event" in roles:
                slots.append(("event_application", sorted(EVENT_APPLICATION_POLICIES)))
            if not slots:
                return self._fresh(template_id)
            slot, values = self.rng.choice(slots)
            alternatives = [value for value in values if value != policy[slot]]
            policy[slot] = self.rng.choice(alternatives)
            changed.append(f"policy:{slot}")
        elif operation == "HOMOLOGOUS_RECOMBINE":
            peers = [elite for elite in self._elites if elite.template_id == template_id and elite.semantic_program_hash != first.semantic_program_hash]
            if not peers:
                return self._fresh(template_id)
            second = max(self.rng.sample(peers, min(4, len(peers))), key=lambda elite: (elite.quality, elite.semantic_program_hash))
            second_components = self._components_from_elite(second)
            parents.append(second.semantic_program_hash)
            take_second = set(self.rng.sample(roles, max(1, len(roles) // 2)))
            for role in take_second:
                if second_components[role].component_id != components[role].component_id:
                    components[role] = second_components[role]
                    changed.append(f"component:{role}")
            second_policy = self._policy_for_template(template_id, second.combination_policy)
            for slot in ("temporal", "market", "event_application"):
                if self.rng.random() < 0.5 and second_policy[slot] != policy[slot]:
                    policy[slot] = second_policy[slot]
                    changed.append(f"policy:{slot}")
            if not changed:
                role = self.rng.choice(roles)
                components[role] = self._choose_component(role, exclude={components[role].component_id})
                changed.append(f"component:{role}")
        else:
            raise ValueError(f"STATE_JUMP_UNKNOWN_OPERATION:{operation}")
        return components, policy, tuple(parents), tuple(sorted(set(changed)))

    def propose(
        self,
        *,
        batch_id: str,
        ask_ordinal: int,
        template_id: str | None = None,
    ) -> GeneratedProgramV2:
        templates = [template for template in PROGRAM_TEMPLATE_COMPONENTS if template != "BASE"]
        chosen_template = str(template_id or self.rng.choice(templates))
        if chosen_template not in templates:
            raise ValueError("STATE_JUMP_TEMPLATE_NOT_ENHANCED")
        for _ in range(self.maximum_attempts):
            operation = self._choose_operation(chosen_template)
            components, policy, parents, changed = self._jump(chosen_template, operation)
            region = program_region_key(
                template_id=chosen_template,
                components=components,
                combination_policy=policy,
            )
            if self.memory.region_dead(region):
                continue
            kwargs = {
                f"{role}_component": components[role]
                for role in PROGRAM_TEMPLATE_COMPONENTS[chosen_template]
                if role != "base"
            }
            program = self.adapter.compose(
                chosen_template,
                components["base"],
                combination_policy=policy,
                **kwargs,
            )
            semantic_hash = program.semantic_program_hash
            if semantic_hash in self.memory.seen_semantic_hashes or semantic_hash in self._generated_semantic_hashes:
                continue
            ordered_components = [components[role] for role in PROGRAM_TEMPLATE_COMPONENTS[chosen_template]]
            receipt = self.adapter.build_receipt(
                program_template_id=chosen_template,
                program=program,
                components=ordered_components,
                combination_policy=policy,
                batch_id=str(batch_id),
                ask_ordinal=int(ask_ordinal),
                generation_arm=SEMANTIC_STATE_JUMP_GENERATOR_V2,
            )
            generated = GeneratedProgramV2(
                program=program,
                receipt=receipt,
                template_id=chosen_template,
                components=dict(components),
                combination_policy=dict(policy),
                operation=operation,
                parent_semantic_hashes=parents,
                changed_slots=changed,
                region_key=region,
            )
            self._generated_semantic_hashes.add(semantic_hash)
            return generated
        raise RuntimeError("STATE_JUMP_GENERATOR_EXHAUSTED_ATTEMPTS")

    def observe(
        self,
        generated: GeneratedProgramV2,
        *,
        admitted: bool,
        productive: bool,
        matched_return_increment: float = 0.0,
        matched_reward_increment: float = 0.0,
        blocked: bool = False,
        redundant: bool = False,
        behavior_identity: str = "",
    ) -> None:
        self.memory.observe(
            generated,
            admitted=admitted,
            productive=productive,
            matched_return_increment=matched_return_increment,
            matched_reward_increment=matched_reward_increment,
            blocked=blocked,
            redundant=redundant,
            behavior_identity=behavior_identity,
        )
        if admitted and productive:
            quality = (
                math.tanh(_finite(matched_return_increment))
                + math.tanh(_finite(matched_reward_increment))
            ) / 2.0
            self._elites.append(
                _EliteSpec(
                    template_id=generated.template_id,
                    component_ids={role: component.component_id for role, component in generated.components.items()},
                    combination_policy=dict(generated.combination_policy),
                    semantic_program_hash=generated.program.semantic_program_hash,
                    quality=float(quality),
                )
            )
            self._elites = sorted(
                self._elites,
                key=lambda elite: (elite.template_id, -elite.quality, elite.semantic_program_hash),
            )[:512]

    def diagnostics(self) -> dict[str, Any]:
        return {
            "schema_version": STATE_JUMP_GENERATOR_SCHEMA,
            "generated_unique_count": len(self._generated_semantic_hashes),
            "elite_count": len(self._elites),
            "memory_observations": self.memory.observations,
            "dead_region_count": sum(stats.dead() for stats in self.memory.region_stats.values()),
            "operation_scores": {
                operation: self.memory.operation_score(operation)
                for operation in STATE_JUMP_OPERATIONS
            },
        }

    def snapshot(self) -> dict[str, Any]:
        component_pool = {
            role: [component.component_id for component in components]
            for role, components in sorted(self.components_by_role.items())
        }
        payload = {
            "schema_version": STATE_JUMP_GENERATOR_SNAPSHOT_SCHEMA,
            "maximum_attempts": self.maximum_attempts,
            "operation_priors": dict(sorted(self.operation_priors.items())),
            "component_pool": component_pool,
            "component_pool_sha256": stable_hash(component_pool),
            "memory": self.memory.snapshot(),
            "rng_state": _listify(self.rng.getstate()),
            "generated_semantic_hashes": sorted(self._generated_semantic_hashes),
            "elites": [
                {
                    "template_id": elite.template_id,
                    "component_ids": dict(sorted(elite.component_ids.items())),
                    "combination_policy": dict(sorted(elite.combination_policy.items())),
                    "semantic_program_hash": elite.semantic_program_hash,
                    "quality": elite.quality,
                }
                for elite in self._elites
            ],
            "sealed_feedback_allowed": False,
        }
        payload["snapshot_sha256"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(
        cls,
        *,
        adapter: CandidateProgramProposalAdapterV0,
        components_by_role: Mapping[str, Sequence[ProgramSourceComponentV0]],
        snapshot: Mapping[str, Any],
    ) -> "SemanticStateJumpProgramGeneratorV2":
        payload = copy.deepcopy(dict(snapshot))
        body = dict(payload)
        claimed = str(body.pop("snapshot_sha256", ""))
        if not claimed or stable_hash(body) != claimed:
            raise ValueError("STATE_JUMP_GENERATOR_SNAPSHOT_SELF_HASH_DRIFT")
        if (
            payload.get("schema_version") != STATE_JUMP_GENERATOR_SNAPSHOT_SCHEMA
            or bool(payload.get("sealed_feedback_allowed"))
        ):
            raise ValueError("STATE_JUMP_GENERATOR_SNAPSHOT_AUTHORITY_DRIFT")
        state = cls(
            adapter=adapter,
            components_by_role=components_by_role,
            memory=ProgramStateJumpSearchMemoryV2.restore(dict(payload["memory"])),
            seed=0,
            operation_priors=dict(payload["operation_priors"]),
            maximum_attempts=int(payload["maximum_attempts"]),
        )
        observed_pool = {
            role: [component.component_id for component in components]
            for role, components in sorted(state.components_by_role.items())
        }
        if (
            stable_hash(observed_pool) != str(payload["component_pool_sha256"])
            or observed_pool != dict(payload["component_pool"])
        ):
            raise ValueError("STATE_JUMP_GENERATOR_COMPONENT_POOL_DRIFT")
        state.rng.setstate(_tupleify(payload["rng_state"]))
        state._generated_semantic_hashes = set(map(str, payload["generated_semantic_hashes"]))
        state._elites = [
            _EliteSpec(
                template_id=str(row["template_id"]),
                component_ids={str(k): str(v) for k, v in dict(row["component_ids"]).items()},
                combination_policy={str(k): str(v) for k, v in dict(row["combination_policy"]).items()},
                semantic_program_hash=str(row["semantic_program_hash"]),
                quality=float(row["quality"]),
            )
            for row in payload["elites"]
        ]
        if state.snapshot() != dict(snapshot):
            raise ValueError("STATE_JUMP_GENERATOR_SNAPSHOT_REPLAY_DRIFT")
        return state


__all__ = [
    "SEMANTIC_STATE_JUMP_GENERATOR_V2",
    "GeneratedProgramV2",
    "ProgramStateJumpSearchMemoryV2",
    "SemanticStateJumpProgramGeneratorV2",
    "component_memory_key",
    "program_region_key",
]
