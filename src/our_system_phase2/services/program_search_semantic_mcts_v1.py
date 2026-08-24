"""Persistent semantic MCTS over the frozen legal Candidate Program catalog.

The tree searches only low-cardinality Program meaning: template, active-role
route/skeleton neighborhoods, combination application, and composition
topology.  It never searches derived lag/clock/complexity hashes or the full
gene vector.  A leaf is completed by choosing one already-legal
``AvailabilityEntry`` and reserving it through the existing route-local
availability controller.
"""
from __future__ import annotations

import copy
import json
import math
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Sequence

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
from our_system_phase2.services.program_search_primitive_credit_v1 import (
    HierarchicalPrimitiveCreditScorerV1,
)
from our_system_phase2.services.route_local_availability import (
    AvailabilityEntry,
    RouteLocalAvailabilityController,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


SEMANTIC_MCTS_PROGRAM_V1 = "SEMANTIC_MCTS_PROGRAM_V1"
SEMANTIC_MCTS_POLICY_ID = "CN_SEMANTIC_PROGRAM_MCTS_V1"
SEMANTIC_MCTS_FEEDBACK_MODE = "CAUSAL_DEVELOPMENT_OBSERVATION_ONLY"
SEMANTIC_MCTS_STATE_SCHEMA = "cn_semantic_program_mcts_state_v1"
SEMANTIC_MCTS_TREE_SCHEMA = "cn_semantic_program_mcts_tree_v1"

TEMPLATE_SLOT = "program_template_id"
COMPONENT_NEIGHBORHOOD_PREFIX = "component_neighborhood::"
COMBINATION_SLOT_BY_ROLE = {
    "temporal": ("combination_temporal",),
    "market": ("combination_market",),
    "event": (
        "combination_event_episode",
        "combination_event_application",
    ),
}


@dataclass(frozen=True, slots=True)
class SemanticProgramStateV1:
    """Order-independent canonical semantic decision state."""

    decisions: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        normalized = tuple(sorted((str(slot), str(value)) for slot, value in self.decisions))
        if len({slot for slot, _ in normalized}) != len(normalized):
            raise ValueError("SEMANTIC_MCTS_DUPLICATE_STATE_SLOT")
        object.__setattr__(self, "decisions", normalized)

    @property
    def key(self) -> str:
        return stable_hash(self.to_dict())

    def get(self, slot: str) -> str | None:
        wanted = str(slot)
        return next((value for key, value in self.decisions if key == wanted), None)

    def apply(self, slot: str, value: str) -> "SemanticProgramStateV1":
        wanted = str(slot)
        existing = self.get(wanted)
        if existing is not None:
            if existing != str(value):
                raise ValueError(f"SEMANTIC_MCTS_STATE_CONFLICT:{wanted}")
            return self
        return SemanticProgramStateV1((*self.decisions, (wanted, str(value))))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SEMANTIC_MCTS_STATE_SCHEMA,
            "decisions": [
                {"slot": slot, "value": value} for slot, value in self.decisions
            ],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SemanticProgramStateV1":
        if str(payload.get("schema_version") or "") != SEMANTIC_MCTS_STATE_SCHEMA:
            raise ValueError("SEMANTIC_MCTS_STATE_SCHEMA_DRIFT")
        return cls(
            tuple(
                (str(row["slot"]), str(row["value"]))
                for row in payload.get("decisions") or ()
            )
        )


def _neighborhood_value(route_id: str, skeleton_id: str) -> str:
    return json.dumps(
        {"route_id": str(route_id), "skeleton_id": str(skeleton_id)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def semantic_program_facts_v1(entry: AvailabilityEntry) -> dict[str, str]:
    """Project one legal Program onto the intentionally small MCTS surface."""

    genes = {str(key): str(value) for key, value in entry.genes.items()}
    template_id = str(genes.get(TEMPLATE_SLOT) or "")
    roles = PROGRAM_TEMPLATE_COMPONENTS.get(template_id)
    if roles is None:
        raise ValueError(f"SEMANTIC_MCTS_UNKNOWN_TEMPLATE:{template_id}")
    facts = {TEMPLATE_SLOT: template_id}
    for role in roles:
        route_id = str(genes.get(f"{role}__route_id") or "")
        skeleton_id = str(genes.get(f"{role}__skeleton_id") or "")
        if not route_id or not skeleton_id or route_id == "__INACTIVE__":
            raise ValueError(f"SEMANTIC_MCTS_ACTIVE_COMPONENT_MISSING:{role}")
        facts[f"{COMPONENT_NEIGHBORHOOD_PREFIX}{role}"] = _neighborhood_value(
            route_id, skeleton_id
        )
    for role in roles:
        for slot in COMBINATION_SLOT_BY_ROLE.get(role, ()):
            value = str(genes.get(slot) or "")
            if not value:
                raise ValueError(f"SEMANTIC_MCTS_APPLICATION_MISSING:{slot}")
            facts[slot] = value
    topology = str(genes.get("composition_topology") or "")
    if topology:
        facts["composition_topology"] = topology
    return facts


def semantic_decision_plan_v1(template_id: str) -> tuple[str, ...]:
    roles = PROGRAM_TEMPLATE_COMPONENTS.get(str(template_id))
    if roles is None:
        raise ValueError(f"SEMANTIC_MCTS_UNKNOWN_TEMPLATE:{template_id}")
    slots = [TEMPLATE_SLOT]
    slots.extend(f"{COMPONENT_NEIGHBORHOOD_PREFIX}{role}" for role in roles)
    for role in roles:
        slots.extend(COMBINATION_SLOT_BY_ROLE.get(role, ()))
    slots.append("composition_topology")
    return tuple(slots)


class SemanticProgramPriorValueV1(Protocol):
    """Value surface used by selection, expansion, and legal completion."""

    def action_prior(
        self,
        *,
        state: SemanticProgramStateV1,
        slot: str,
        value: str,
        candidates: Sequence[AvailabilityEntry],
    ) -> float: ...

    def state_value(
        self,
        *,
        state: SemanticProgramStateV1,
        candidates: Sequence[AvailabilityEntry],
    ) -> float: ...

    def completion_key(self, entry: AvailabilityEntry) -> tuple[Any, ...]: ...

    def completion_details(self, entry: AvailabilityEntry) -> Mapping[str, Any]: ...


class PrimitiveHierarchicalPriorValueV1:
    """Primitive V1 hierarchical score/backoff exposed as an MCTS interface."""

    def __init__(
        self,
        *,
        metadata_by_exact_identity: Mapping[str, Mapping[str, Any]],
        primitive_stats: Mapping[str, Any],
        primitive_stats_payload_sha256: str | None = None,
    ) -> None:
        self.metadata_by_exact_identity = {
            str(key): copy.deepcopy(dict(value))
            for key, value in metadata_by_exact_identity.items()
        }
        self.scorer = HierarchicalPrimitiveCreditScorerV1(
            primitive_stats=primitive_stats,
            primitive_stats_payload_sha256=primitive_stats_payload_sha256,
        )
        self.score_by_exact = {
            exact: self.scorer.score(metadata)
            for exact, metadata in self.metadata_by_exact_identity.items()
        }
        self.metadata_payload_sha256 = stable_hash(
            dict(sorted(self.metadata_by_exact_identity.items()))
        )

    def _score(self, entry: AvailabilityEntry) -> float:
        try:
            return float(self.score_by_exact[str(entry.exact_identity)]["primitive_score"])
        except KeyError as exc:
            raise ValueError(
                f"SEMANTIC_MCTS_PRIMITIVE_METADATA_MISSING:{entry.exact_identity}"
            ) from exc

    def action_prior(
        self,
        *,
        state: SemanticProgramStateV1,
        slot: str,
        value: str,
        candidates: Sequence[AvailabilityEntry],
    ) -> float:
        del state, slot, value
        if not candidates:
            raise ValueError("SEMANTIC_MCTS_EMPTY_ACTION_DOMAIN")
        return max(self._score(entry) for entry in candidates)

    def state_value(
        self,
        *,
        state: SemanticProgramStateV1,
        candidates: Sequence[AvailabilityEntry],
    ) -> float:
        del state
        if not candidates:
            raise ValueError("SEMANTIC_MCTS_EMPTY_VALUE_DOMAIN")
        return max(self._score(entry) for entry in candidates)

    def completion_key(self, entry: AvailabilityEntry) -> tuple[Any, ...]:
        metadata = self.metadata_by_exact_identity[str(entry.exact_identity)]
        return (
            -self._score(entry),
            str(metadata.get("tie_break_identity") or entry.exact_identity),
            str(entry.exact_identity),
        )

    def completion_details(self, entry: AvailabilityEntry) -> Mapping[str, Any]:
        return copy.deepcopy(self.score_by_exact[str(entry.exact_identity)])


@dataclass(slots=True)
class SemanticMCTSEdgeV1:
    slot: str
    value: str
    child_key: str
    prior: float
    visits: int = 0
    value_sum: float = 0.0

    @property
    def mean_value(self) -> float:
        return self.value_sum / self.visits if self.visits else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "slot": self.slot,
            "value": self.value,
            "child_key": self.child_key,
            "prior": float(self.prior),
            "visits": int(self.visits),
            "value_sum": float(self.value_sum),
        }


@dataclass(slots=True)
class SemanticMCTSNodeV1:
    state: SemanticProgramStateV1
    visits: int = 0
    value_sum: float = 0.0
    edges: dict[str, SemanticMCTSEdgeV1] = field(default_factory=dict)

    @property
    def mean_value(self) -> float:
        return self.value_sum / self.visits if self.visits else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.to_dict(),
            "visits": int(self.visits),
            "value_sum": float(self.value_sum),
            "edges": {
                key: self.edges[key].to_dict() for key in sorted(self.edges)
            },
        }


@dataclass(frozen=True, slots=True)
class _LegalAction:
    action_key: str
    slot: str
    value: str
    candidates: tuple[AvailabilityEntry, ...]
    prior: float


class SemanticProgramMCTSTreeV1:
    """Deterministic PUCT tree with progressive widening and transpositions."""

    def __init__(
        self,
        *,
        facts_by_exact_identity: Mapping[str, Mapping[str, str]],
        prior_value: SemanticProgramPriorValueV1,
        exploration_coefficient: float = 1.25,
        progressive_widening_coefficient: float = 1.0,
        progressive_widening_alpha: float = 0.5,
    ) -> None:
        self.facts_by_exact_identity = {
            str(key): {str(slot): str(value) for slot, value in dict(row).items()}
            for key, row in facts_by_exact_identity.items()
        }
        self.prior_value = prior_value
        self.exploration_coefficient = float(exploration_coefficient)
        self.progressive_widening_coefficient = float(
            progressive_widening_coefficient
        )
        self.progressive_widening_alpha = float(progressive_widening_alpha)
        if self.exploration_coefficient < 0.0:
            raise ValueError("SEMANTIC_MCTS_EXPLORATION_INVALID")
        if self.progressive_widening_coefficient <= 0.0:
            raise ValueError("SEMANTIC_MCTS_WIDENING_COEFFICIENT_INVALID")
        if not 0.0 < self.progressive_widening_alpha <= 1.0:
            raise ValueError("SEMANTIC_MCTS_WIDENING_ALPHA_INVALID")
        root = SemanticProgramStateV1()
        self.root_key = root.key
        self.nodes = {self.root_key: SemanticMCTSNodeV1(root)}
        self.simulation_count = 0
        self.expansion_count = 0
        self.selection_count = 0
        self.transposition_hits = 0
        self.completion_count = 0
        self.candidate_value_queries = 0
        self.actual_backpropagations = 0

    def intern_state(self, state: SemanticProgramStateV1) -> SemanticMCTSNodeV1:
        existing = self.nodes.get(state.key)
        if existing is not None:
            if existing.state != state:
                raise RuntimeError("SEMANTIC_MCTS_STATE_HASH_COLLISION")
            self.transposition_hits += 1
            return existing
        node = SemanticMCTSNodeV1(state)
        self.nodes[state.key] = node
        return node

    def _next_slot(
        self,
        state: SemanticProgramStateV1,
        candidates: Sequence[AvailabilityEntry],
    ) -> str | None:
        if not candidates:
            raise ValueError("SEMANTIC_MCTS_EMPTY_NODE_DOMAIN")
        template_id = state.get(TEMPLATE_SLOT)
        if template_id is None:
            return TEMPLATE_SLOT
        for slot in semantic_decision_plan_v1(template_id)[1:]:
            if state.get(slot) is not None:
                continue
            values = {
                self.facts_by_exact_identity[str(entry.exact_identity)].get(slot)
                for entry in candidates
            }
            values.discard(None)
            if not values:
                continue
            if len(values) > 1:
                return slot
        return None

    def _legal_actions(
        self,
        state: SemanticProgramStateV1,
        candidates: Sequence[AvailabilityEntry],
    ) -> tuple[_LegalAction, ...]:
        slot = self._next_slot(state, candidates)
        if slot is None:
            return ()
        grouped: dict[str, list[AvailabilityEntry]] = {}
        for entry in candidates:
            facts = self.facts_by_exact_identity.get(str(entry.exact_identity))
            if facts is None:
                raise ValueError(
                    f"SEMANTIC_MCTS_FACTS_MISSING:{entry.exact_identity}"
                )
            value = facts.get(slot)
            if value is None:
                raise ValueError(f"SEMANTIC_MCTS_FACT_MISSING:{slot}")
            grouped.setdefault(value, []).append(entry)
        actions = []
        for value, rows in sorted(grouped.items()):
            ordered = tuple(sorted(rows, key=lambda entry: entry.exact_identity))
            self.candidate_value_queries += len(ordered)
            prior = float(
                self.prior_value.action_prior(
                    state=state,
                    slot=slot,
                    value=value,
                    candidates=ordered,
                )
            )
            if not math.isfinite(prior) or prior < 0.0:
                raise ValueError("SEMANTIC_MCTS_ACTION_PRIOR_INVALID")
            action_key = stable_hash({"slot": slot, "value": value})
            actions.append(_LegalAction(action_key, slot, value, ordered, prior))
        return tuple(actions)

    def _widening_limit(self, node: SemanticMCTSNodeV1, legal_count: int) -> int:
        allowed = int(
            math.floor(
                self.progressive_widening_coefficient
                * float(node.visits + 1) ** self.progressive_widening_alpha
            )
        )
        return min(int(legal_count), max(1, allowed))

    @staticmethod
    def _normalized_priors(actions: Sequence[_LegalAction]) -> dict[str, float]:
        total = sum(action.prior for action in actions)
        if total <= 0.0:
            return {action.action_key: 1.0 / len(actions) for action in actions}
        return {action.action_key: action.prior / total for action in actions}

    def _backpropagate(
        self,
        node_keys: Sequence[str],
        edge_path: Sequence[tuple[str, str]],
        value: float,
    ) -> None:
        observed = float(value)
        if not math.isfinite(observed):
            raise ValueError("SEMANTIC_MCTS_BACKPROP_VALUE_INVALID")
        for key in node_keys:
            node = self.nodes[str(key)]
            node.visits += 1
            node.value_sum += observed
        for parent_key, action_key in edge_path:
            edge = self.nodes[str(parent_key)].edges[str(action_key)]
            edge.visits += 1
            edge.value_sum += observed

    def simulate(self, candidates: Sequence[AvailabilityEntry]) -> None:
        pool = tuple(sorted(candidates, key=lambda entry: entry.exact_identity))
        if not pool:
            raise ValueError("SEMANTIC_MCTS_SIMULATION_DOMAIN_EMPTY")
        node = self.nodes[self.root_key]
        node_keys = [self.root_key]
        edge_path: list[tuple[str, str]] = []
        while True:
            actions = self._legal_actions(node.state, pool)
            if not actions:
                self.candidate_value_queries += len(pool)
                value = self.prior_value.state_value(
                    state=node.state, candidates=pool
                )
                break
            by_key = {action.action_key: action for action in actions}
            legal_edges = {
                key: edge for key, edge in node.edges.items() if key in by_key
            }
            limit = self._widening_limit(node, len(actions))
            unexpanded = [
                action for action in actions if action.action_key not in legal_edges
            ]
            if unexpanded and len(legal_edges) < limit:
                action = min(
                    unexpanded,
                    key=lambda row: (-row.prior, row.action_key),
                )
                child_state = node.state.apply(action.slot, action.value)
                prior_node_count = len(self.nodes)
                child = self.intern_state(child_state)
                if len(self.nodes) > prior_node_count:
                    # New states are not transposition hits.
                    pass
                node.edges[action.action_key] = SemanticMCTSEdgeV1(
                    slot=action.slot,
                    value=action.value,
                    child_key=child.state.key,
                    prior=action.prior,
                )
                self.expansion_count += 1
                edge_path.append((node.state.key, action.action_key))
                node = child
                node_keys.append(node.state.key)
                pool = action.candidates
                self.candidate_value_queries += len(pool)
                value = self.prior_value.state_value(
                    state=node.state, candidates=pool
                )
                break
            priors = self._normalized_priors(actions)
            parent_visits = max(1, node.visits)
            action = min(
                (by_key[key] for key in legal_edges),
                key=lambda row: (
                    -(
                        legal_edges[row.action_key].mean_value
                        + self.exploration_coefficient
                        * priors[row.action_key]
                        * math.sqrt(parent_visits)
                        / (1 + legal_edges[row.action_key].visits)
                    ),
                    row.action_key,
                ),
            )
            edge_path.append((node.state.key, action.action_key))
            node = self.nodes[legal_edges[action.action_key].child_key]
            node_keys.append(node.state.key)
            pool = action.candidates
            self.selection_count += 1
        self._backpropagate(node_keys, edge_path, float(value))
        self.simulation_count += 1

    def run_simulations(
        self, candidates: Sequence[AvailabilityEntry], *, count: int
    ) -> None:
        for _ in range(int(count)):
            self.simulate(candidates)

    def principal_variation(
        self, candidates: Sequence[AvailabilityEntry]
    ) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...], tuple[AvailabilityEntry, ...]]:
        pool = tuple(sorted(candidates, key=lambda entry: entry.exact_identity))
        node = self.nodes[self.root_key]
        node_keys = [self.root_key]
        edge_path: list[tuple[str, str]] = []
        while True:
            actions = self._legal_actions(node.state, pool)
            if not actions:
                break
            by_key = {action.action_key: action for action in actions}
            legal_edges = {
                key: edge for key, edge in node.edges.items() if key in by_key
            }
            if not legal_edges:
                break
            action = min(
                (by_key[key] for key in legal_edges),
                key=lambda row: (
                    -legal_edges[row.action_key].visits,
                    -legal_edges[row.action_key].mean_value,
                    -row.prior,
                    row.action_key,
                ),
            )
            edge_path.append((node.state.key, action.action_key))
            node = self.nodes[legal_edges[action.action_key].child_key]
            node_keys.append(node.state.key)
            pool = action.candidates
        return tuple(node_keys), tuple(edge_path), pool

    def complete(self, candidates: Sequence[AvailabilityEntry]) -> AvailabilityEntry:
        if not candidates:
            raise ValueError("SEMANTIC_MCTS_COMPLETION_DOMAIN_EMPTY")
        self.completion_count += 1
        self.candidate_value_queries += len(candidates)
        return min(candidates, key=self.prior_value.completion_key)

    def backpropagate_observation(
        self,
        *,
        node_keys: Sequence[str],
        edge_path: Sequence[tuple[str, str]],
        value: float,
    ) -> None:
        if not node_keys or str(node_keys[0]) != self.root_key:
            raise ValueError("SEMANTIC_MCTS_OBSERVATION_PATH_INVALID")
        if len(edge_path) + 1 != len(node_keys):
            raise ValueError("SEMANTIC_MCTS_OBSERVATION_PATH_LENGTH_DRIFT")
        for offset, (parent_key, action_key) in enumerate(edge_path):
            edge = self.nodes[str(parent_key)].edges[str(action_key)]
            if parent_key != node_keys[offset] or edge.child_key != node_keys[offset + 1]:
                raise ValueError("SEMANTIC_MCTS_OBSERVATION_EDGE_DRIFT")
        self._backpropagate(node_keys, edge_path, float(value))
        self.actual_backpropagations += 1

    def diagnostics(self) -> dict[str, Any]:
        return {
            "node_count": len(self.nodes),
            "edge_count": sum(len(node.edges) for node in self.nodes.values()),
            "max_semantic_depth": max(
                len(node.state.decisions) for node in self.nodes.values()
            ),
            "simulation_count": self.simulation_count,
            "expansion_count": self.expansion_count,
            "selection_count": self.selection_count,
            "transposition_hits": self.transposition_hits,
            "completion_count": self.completion_count,
            "candidate_value_queries": self.candidate_value_queries,
            "actual_backpropagations": self.actual_backpropagations,
        }

    def snapshot(self) -> dict[str, Any]:
        payload = {
            "schema_version": SEMANTIC_MCTS_TREE_SCHEMA,
            "root_key": self.root_key,
            "config": {
                "exploration_coefficient": self.exploration_coefficient,
                "progressive_widening_coefficient": self.progressive_widening_coefficient,
                "progressive_widening_alpha": self.progressive_widening_alpha,
            },
            "facts_payload_sha256": stable_hash(
                dict(sorted(self.facts_by_exact_identity.items()))
            ),
            "nodes": {key: self.nodes[key].to_dict() for key in sorted(self.nodes)},
            "diagnostics": self.diagnostics(),
        }
        payload["tree_payload_sha256"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(
        cls,
        *,
        snapshot: Mapping[str, Any],
        facts_by_exact_identity: Mapping[str, Mapping[str, str]],
        prior_value: SemanticProgramPriorValueV1,
    ) -> "SemanticProgramMCTSTreeV1":
        payload = copy.deepcopy(dict(snapshot))
        body = dict(payload)
        claimed = str(body.pop("tree_payload_sha256", ""))
        if not claimed or stable_hash(body) != claimed:
            raise ValueError("SEMANTIC_MCTS_TREE_SELF_HASH_DRIFT")
        if payload.get("schema_version") != SEMANTIC_MCTS_TREE_SCHEMA:
            raise ValueError("SEMANTIC_MCTS_TREE_SCHEMA_DRIFT")
        config = dict(payload["config"])
        tree = cls(
            facts_by_exact_identity=facts_by_exact_identity,
            prior_value=prior_value,
            exploration_coefficient=float(config["exploration_coefficient"]),
            progressive_widening_coefficient=float(
                config["progressive_widening_coefficient"]
            ),
            progressive_widening_alpha=float(config["progressive_widening_alpha"]),
        )
        if payload.get("facts_payload_sha256") != stable_hash(
            dict(sorted(tree.facts_by_exact_identity.items()))
        ):
            raise ValueError("SEMANTIC_MCTS_TREE_FACTS_DRIFT")
        nodes: dict[str, SemanticMCTSNodeV1] = {}
        for key, raw_node in dict(payload["nodes"]).items():
            row = dict(raw_node)
            state = SemanticProgramStateV1.from_dict(dict(row["state"]))
            if state.key != str(key):
                raise ValueError("SEMANTIC_MCTS_TREE_NODE_KEY_DRIFT")
            edges = {
                str(action_key): SemanticMCTSEdgeV1(
                    slot=str(edge["slot"]),
                    value=str(edge["value"]),
                    child_key=str(edge["child_key"]),
                    prior=float(edge["prior"]),
                    visits=int(edge["visits"]),
                    value_sum=float(edge["value_sum"]),
                )
                for action_key, edge in dict(row["edges"]).items()
            }
            nodes[str(key)] = SemanticMCTSNodeV1(
                state=state,
                visits=int(row["visits"]),
                value_sum=float(row["value_sum"]),
                edges=edges,
            )
        if str(payload["root_key"]) not in nodes:
            raise ValueError("SEMANTIC_MCTS_TREE_ROOT_MISSING")
        for node in nodes.values():
            if any(edge.child_key not in nodes for edge in node.edges.values()):
                raise ValueError("SEMANTIC_MCTS_TREE_CHILD_MISSING")
        tree.nodes = nodes
        tree.root_key = str(payload["root_key"])
        diagnostics = dict(payload["diagnostics"])
        tree.simulation_count = int(diagnostics["simulation_count"])
        tree.expansion_count = int(diagnostics["expansion_count"])
        tree.selection_count = int(diagnostics["selection_count"])
        tree.transposition_hits = int(diagnostics["transposition_hits"])
        tree.completion_count = int(diagnostics["completion_count"])
        tree.candidate_value_queries = int(diagnostics["candidate_value_queries"])
        tree.actual_backpropagations = int(diagnostics["actual_backpropagations"])
        if tree.snapshot() != payload:
            raise ValueError("SEMANTIC_MCTS_TREE_RESTORE_DRIFT")
        return tree


class SemanticMCTSProgramSearchAdapterV1(_AvailabilityProgramOptimizer):
    """Program optimizer adapter backed by the persistent semantic MCTS tree."""

    def __init__(
        self,
        *,
        metadata_by_exact_identity: Mapping[str, Mapping[str, Any]],
        primitive_stats: Mapping[str, Any],
        primitive_stats_payload_sha256: str | None = None,
        simulations_per_ask: int = 32,
        exploration_coefficient: float = 1.25,
        progressive_widening_coefficient: float = 1.0,
        progressive_widening_alpha: float = 0.5,
        **kwargs: Any,
    ) -> None:
        super().__init__(arm=SEMANTIC_MCTS_PROGRAM_V1, **kwargs)
        self.simulations_per_ask = int(simulations_per_ask)
        if self.simulations_per_ask < 1:
            raise ValueError("SEMANTIC_MCTS_SIMULATION_COUNT_INVALID")
        self.prior_value = PrimitiveHierarchicalPriorValueV1(
            metadata_by_exact_identity=metadata_by_exact_identity,
            primitive_stats=primitive_stats,
            primitive_stats_payload_sha256=primitive_stats_payload_sha256,
        )
        expected = {str(entry.exact_identity) for entry in self.entries}
        if set(self.prior_value.metadata_by_exact_identity) != expected:
            raise ValueError("SEMANTIC_MCTS_PRIMITIVE_METADATA_COVERAGE_DRIFT")
        self.facts_by_exact_identity = {
            str(entry.exact_identity): semantic_program_facts_v1(entry)
            for entry in self.entries
        }
        self.tree = SemanticProgramMCTSTreeV1(
            facts_by_exact_identity=self.facts_by_exact_identity,
            prior_value=self.prior_value,
            exploration_coefficient=exploration_coefficient,
            progressive_widening_coefficient=progressive_widening_coefficient,
            progressive_widening_alpha=progressive_widening_alpha,
        )
        self._pending_paths: dict[
            str, tuple[tuple[str, ...], tuple[tuple[str, str], ...]]
        ] = {}

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
        constraint, group_counts = _batch_group_counts(batch_group_constraint)
        asked: list[dict[str, Any]] = []
        for ordinal in range(int(count)):
            remaining = _batch_feasible_entries(
                self._remaining_entries(
                    required_program_template_id, eligible_exact_identities
                ),
                constraint=constraint,
                group_counts=group_counts,
            )
            if not remaining:
                break
            self.tree.run_simulations(remaining, count=self.simulations_per_ask)
            node_keys, edge_path, leaf_pool = self.tree.principal_variation(remaining)
            entry = self.tree.complete(leaf_pool)
            emission = self.controller.reserve_exact(
                route_id=PROGRAM_ROUTE_ID,
                exact_identity=entry.exact_identity,
                emission_mode="PROGRAM_SEMANTIC_MCTS_V1",
                source_exact_identity=node_keys[-1],
            )
            if emission is None:
                raise RuntimeError("SEMANTIC_MCTS_PROGRAM_RESERVATION_FAILED")
            feasibility = _record_batch_group_selection(
                emission.exact_identity,
                constraint=constraint,
                group_counts=group_counts,
            )
            feasibility["batch_feasible_count_at_selection"] = len(remaining)
            proposal_id = stable_hash(
                {
                    "arm": self.arm,
                    "checkpoint_id": str(checkpoint_id),
                    "ask_ordinal": int(ordinal),
                    "exact_identity": emission.exact_identity,
                    "simulation_count": self.tree.simulation_count,
                    "semantic_leaf_key": node_keys[-1],
                }
            )[:24]
            details = dict(self.prior_value.completion_details(entry))
            row = self._ask_row(
                emission=emission,
                checkpoint_id=str(checkpoint_id),
                ask_ordinal=int(ordinal),
                proposal_id=proposal_id,
                trial_number=None,
                optimizer_ask_identity=proposal_id,
                acquisition={
                    "source": SEMANTIC_MCTS_POLICY_ID,
                    "semantic_leaf_key": node_keys[-1],
                    "semantic_depth": len(node_keys) - 1,
                    "semantic_decisions": [
                        {"slot": slot, "value": value}
                        for slot, value in self.tree.nodes[node_keys[-1]].state.decisions
                    ],
                    "simulations_per_ask": self.simulations_per_ask,
                    "primitive_score": float(details["primitive_score"]),
                    "primitive_template_backoff": float(details["template_backoff"]),
                    "primitive_stats_payload_sha256": str(
                        details["primitive_stats_payload_sha256"]
                    ),
                    "leaf_completion": "EXISTING_LEGAL_PRIMITIVE_TYPED_AVAILABILITY",
                    "tree_diagnostics": self.tree.diagnostics(),
                },
                batch_group_feasibility=feasibility,
            )
            asked.append(row)
            self._pending[proposal_id] = row
            self._pending_paths[proposal_id] = (node_keys, edge_path)
        return asked

    @staticmethod
    def _observation_value(observation: ProgramOptimizerObservationV1) -> float:
        if not observation.admission.admitted or observation.uplift is None:
            return 0.0
        credit = dict(observation.uplift.program_credit)
        reward = float(credit.get("matched_net_reward_increment") or 0.0)
        objective = float(
            credit.get("matched_cumulative_net_return_increment") or 0.0
        )
        return float(reward > 0.0 and objective > 0.0)

    def tell(
        self, observations: Sequence[ProgramOptimizerObservationV1]
    ) -> dict[str, Any]:
        by_id = {row.proposal_id: row for row in observations}
        if set(by_id) != set(self._pending):
            raise RuntimeError("PROGRAM_OPTIMIZER_ASK_TELL_COVERAGE_DRIFT")
        productive = 0
        admitted = 0
        for proposal_id in self._pending:
            observation = by_id[proposal_id]
            asked_exact = str(self._pending[proposal_id]["exact_identity"])
            if str(observation.exact_identity) != asked_exact:
                raise ValueError("SEMANTIC_MCTS_OBSERVATION_EXACT_DRIFT")
            value = self._observation_value(observation)
            productive += int(value)
            admitted += int(bool(observation.admission.admitted))
            node_keys, edge_path = self._pending_paths[proposal_id]
            self.tree.backpropagate_observation(
                node_keys=node_keys, edge_path=edge_path, value=value
            )
        receipt = {
            "schema_version": "cn_semantic_mcts_program_tell_v1",
            "optimizer_arm": self.arm,
            "asked_count": len(by_id),
            "admitted_count": admitted,
            "productive_count": productive,
            "optimizer_feedback_applied": True,
            "online_feedback_mode": SEMANTIC_MCTS_FEEDBACK_MODE,
            "primitive_prior_mutated": False,
            "tree_diagnostics": self.tree.diagnostics(),
        }
        self._history.append(receipt)
        self._pending.clear()
        self._pending_paths.clear()
        return receipt

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            "optimizer_arm": self.arm,
            "policy_id": SEMANTIC_MCTS_POLICY_ID,
            "learning_mode": SEMANTIC_MCTS_FEEDBACK_MODE,
            "simulations_per_ask": self.simulations_per_ask,
            "semantic_decision_surface": {
                "template": True,
                "component_route_skeleton_neighborhood": True,
                "combination_application": True,
                "composition_topology": True,
                "derived_lag_class": False,
                "full_gene_vector": False,
                "exact_identity_as_tree_action": False,
            },
            "leaf_completion": "EXISTING_LEGAL_PRIMITIVE_TYPED_AVAILABILITY",
            "primitive_stats_payload_sha256": self.prior_value.scorer.stats_payload_sha256,
            "primitive_metadata_payload_sha256": self.prior_value.metadata_payload_sha256,
            "tree_config": {
                "exploration_coefficient": self.tree.exploration_coefficient,
                "progressive_widening_coefficient": self.tree.progressive_widening_coefficient,
                "progressive_widening_alpha": self.tree.progressive_widening_alpha,
            },
        }

    def snapshot(self) -> dict[str, Any]:
        payload = super().snapshot()
        payload.pop("snapshot_hash")
        payload["semantic_tree"] = self.tree.snapshot()
        payload["snapshot_hash"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(cls, **kwargs: Any) -> "SemanticMCTSProgramSearchAdapterV1":
        snapshot = copy.deepcopy(dict(kwargs.pop("snapshot")))
        body = dict(snapshot)
        claimed = str(body.pop("snapshot_hash", ""))
        if not claimed or stable_hash(body) != claimed:
            raise ValueError("SEMANTIC_MCTS_PROGRAM_SNAPSHOT_SELF_HASH_DRIFT")
        adapter = cls(**kwargs)
        adapter.controller = RouteLocalAvailabilityController.restore(
            entries=adapter.entries,
            seen_exact_identities=adapter.seen_exact_identities,
            state=dict(snapshot["availability"]),
            input_hashes=adapter.controller.input_hashes,
        )
        adapter._history = copy.deepcopy(list(snapshot["history"]))
        adapter.tree = SemanticProgramMCTSTreeV1.restore(
            snapshot=dict(snapshot["semantic_tree"]),
            facts_by_exact_identity=adapter.facts_by_exact_identity,
            prior_value=adapter.prior_value,
        )
        if adapter.snapshot() != snapshot:
            raise ValueError("SEMANTIC_MCTS_PROGRAM_SNAPSHOT_REPLAY_DRIFT")
        return adapter


__all__ = [
    "SEMANTIC_MCTS_FEEDBACK_MODE",
    "SEMANTIC_MCTS_POLICY_ID",
    "SEMANTIC_MCTS_PROGRAM_V1",
    "PrimitiveHierarchicalPriorValueV1",
    "SemanticMCTSProgramSearchAdapterV1",
    "SemanticProgramMCTSTreeV1",
    "SemanticProgramStateV1",
    "semantic_decision_plan_v1",
    "semantic_program_facts_v1",
]
