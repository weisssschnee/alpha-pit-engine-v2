"""Typed, multi-output candidate-program envelope for CN true1min research.

This module is deliberately a thin orchestration layer.  It does not replace
the unified capability registry, ``TypedRouteCompiler``, expression parser or
Phase3CM shared DAG.  Legacy expressions are delegated to those authorities;
this layer adds heterogeneous program nodes, four explicit outputs, joint
clock/control contracts and reward-free whole-program identity.
"""

from __future__ import annotations

import heapq
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Sequence

from our_system_phase2.services.phase3cm_streaming_dag import (
    SharedMultiCandidateDAGPlan,
)
from our_system_phase2.services.typed_route_compiler import TypedRouteCompiler
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    stable_hash,
)


PROGRAM_SCHEMA_VERSION = "cn_typed_candidate_program_v1"
PROGRAM_COMPILER_SEMANTICS_VERSION = "cn_program_compiler_adapter_v1"
PROGRAM_PORTFOLIO_AUTHORITY = "TOPK_10_EQUAL"

LEAF_NODE_TYPES = frozenset(
    {
        "STOCK_FIELD",
        "MARKET_FIELD",
        "INDUSTRY_FIELD",
        "PLATE_FIELD",
        "EVENT_EPISODE",
        "STATE_REPRESENTATION",
        "LEGACY_CANDIDATE_COMPONENT",
        "FROZEN_BROAD_EVENT_REF",
        "CONSTANT",
    }
)
TEMPORAL_NODE_TYPES = frozenset(
    {
        "LAG",
        "DELTA",
        "SLOPE",
        "ACCELERATION",
        "ROLLING_MEAN",
        "ROLLING_STD",
        "SELF_ZSCORE",
        "SELF_QUANTILE",
        "PERSISTENCE",
        "STATE_AGE",
        "TIME_SINCE",
        "SHORT_LONG_SPREAD",
    }
)
CROSS_SECTIONAL_NODE_TYPES = frozenset(
    {
        "CSRANK",
        "CS_ZSCORE",
        "WINSORIZE",
        "INDUSTRY_NEUTRALIZE",
        "SIZE_NEUTRALIZE",
        "RESIDUALIZE",
        "WITHIN_GROUP_RANK",
        "TOP_QUANTILE_MASK",
    }
)
EVENT_NODE_TYPES = frozenset(
    {
        "FIRST_HIT",
        "EVENT_AGE",
        "EVENT_COUNT",
        "EVENT_DIRECTION",
        "EVENT_WINDOW",
        "REPEATED_EVENT_SUPPRESSION",
        "PRE_EVENT_PATH",
        "POST_EVENT_STATE",
        "EPISODE_INTERSECT",
        "EPISODE_UNION",
    }
)
STATE_NODE_TYPES = frozenset(
    {
        "STATE",
        "TRANSITION",
        "STATE_PERSISTENCE",
        "BREADTH_RATIO",
        "LIMIT_DENSITY",
        "LIQUIDITY_STATE",
        "VOLATILITY_STATE",
    }
)
COMBINATION_NODE_TYPES = frozenset(
    {
        "ADD",
        "SUBTRACT",
        "MULTIPLY",
        "SAFE_DIVIDE",
        "MIN",
        "MAX",
        "GATE",
        "FILTER",
        "VETO",
        "MODULATE",
        "INTERSECT",
        "UNION",
        "CONDITIONAL_SWITCH",
    }
)
NODE_TYPES = (
    LEAF_NODE_TYPES
    | TEMPORAL_NODE_TYPES
    | CROSS_SECTIONAL_NODE_TYPES
    | EVENT_NODE_TYPES
    | STATE_NODE_TYPES
    | COMBINATION_NODE_TYPES
)

SEMANTIC_TYPES = frozenset(
    {
        "STOCK_VALUE",
        "STOCK_SCORE",
        "STOCK_MASK",
        "STOCK_MULTIPLIER",
        "MARKET_VALUE",
        "MARKET_MASK",
        "GROUP_VALUE",
        "GROUP_MASK",
        "EVENT_EPISODE",
        "STATE_VALUE",
        "SCALAR",
    }
)

ENTITY_SCOPES = frozenset(
    {"STOCK", "MARKET", "INDUSTRY", "PLATE", "EVENT", "CONSTANT"}
)

CONTROL_OPERATIONS = frozenset(
    {
        "ABLATE_NODE",
        "ABLATE_SUBGRAPH",
        "REMOVE_GATE",
        "REMOVE_VETO",
        "REMOVE_EVENT_TRIGGER",
        "REPLACE_WITH_LEVEL",
        "REPLACE_WITH_PLACEBO",
        "REPLACE_WITH_WRONG_LAG_CONTROL",
        "BASE_PAYLOAD_ONLY",
    }
)

_NODE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]*$")
_NON_SEMANTIC_EXACT_KEYS = frozenset(
    {
        "seed",
        "attempt",
        "attempt_id",
        "route_attempt_index",
        "sampler",
        "sampler_id",
        "proposal_parent",
        "generation_source",
        "runtime",
        "runtime_seconds",
        "scheduler_credit",
        "optimizer_reward",
        "portfolio_behavior_family_id",
        "portfolio_behavior_signature_id",
    }
)
_NON_SEMANTIC_FRAGMENTS = (
    "reward",
    "return",
    "turnover",
    "blocker",
    "behavior_family",
    "evaluation_result",
)


def _canonicalize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _canonicalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_canonicalize(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _strip_nonsemantic_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        output = {}
        for key, item in value.items():
            normalized = str(key).lower()
            if normalized in _NON_SEMANTIC_EXACT_KEYS or any(
                fragment in normalized for fragment in _NON_SEMANTIC_FRAGMENTS
            ):
                continue
            output[str(key)] = _strip_nonsemantic_metadata(item)
        return _canonicalize(output)
    if isinstance(value, (list, tuple)):
        return [_strip_nonsemantic_metadata(item) for item in value]
    return _canonicalize(value)


@dataclass(frozen=True, slots=True)
class ComplexityBudgetV1:
    maximum_effective_nodes: int = 48
    maximum_program_depth: int = 12
    maximum_stock_field_leaves: int = 12
    maximum_context_field_leaves: int = 6
    maximum_event_sources: int = 3
    maximum_window_kinds: int = 6
    maximum_gate_veto_depth: int = 3
    maximum_residualize_count: int = 2
    maximum_conditional_switch_count: int = 2
    maximum_event_join_cost: int = 8
    maximum_estimated_cost: int = 128
    policy_id: str = "CN_TYPED_PROGRAM_COMPLEXITY_CONSERVATIVE_V1"

    def __post_init__(self) -> None:
        numeric = [
            value
            for key, value in asdict(self).items()
            if key != "policy_id"
        ]
        if not self.policy_id or any(int(value) <= 0 for value in numeric):
            raise ValueError("program complexity budget must be positive and named")

    @property
    def policy_hash(self) -> str:
        return stable_hash(asdict(self))


@dataclass(frozen=True, slots=True)
class TypedNodeSpec:
    node_id: str
    node_type: str
    input_node_ids: tuple[str, ...]
    parameters: Mapping[str, Any]
    output_semantic_type: str
    entity_scope: str
    temporal_semantics: Mapping[str, Any]
    observable_clock: str
    maturity: str
    unit_signature: str
    support_unit: str
    source_lineage: tuple[str, ...] = ()
    component_route_provenance: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not _NODE_ID_RE.fullmatch(self.node_id):
            raise ValueError(f"invalid program node id: {self.node_id}")
        if self.node_type not in NODE_TYPES:
            raise ValueError(f"unsupported program node type: {self.node_type}")
        if self.output_semantic_type not in SEMANTIC_TYPES:
            raise ValueError(
                f"unsupported program semantic type: {self.output_semantic_type}"
            )
        if self.entity_scope not in ENTITY_SCOPES:
            raise ValueError(f"unsupported program entity scope: {self.entity_scope}")
        if any(route not in ROUTE_IDS for route in self.component_route_provenance):
            raise ValueError("program node has unknown component route provenance")
        required = {
            "observable_clock": self.observable_clock,
            "maturity": self.maturity,
            "unit_signature": self.unit_signature,
            "support_unit": self.support_unit,
        }
        missing = sorted(key for key, value in required.items() if not value)
        if missing:
            raise ValueError(f"program node lacks typed contracts: {missing}")

    def semantic_payload(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "input_node_ids": list(self.input_node_ids),
            "parameters": _strip_nonsemantic_metadata(self.parameters),
            "output_semantic_type": self.output_semantic_type,
            "entity_scope": self.entity_scope,
            "temporal_semantics": _canonicalize(self.temporal_semantics),
            "observable_clock": self.observable_clock,
            "maturity": self.maturity,
            "unit_signature": self.unit_signature,
            "support_unit": self.support_unit,
            "source_lineage": list(self.source_lineage),
            "component_route_provenance": list(
                self.component_route_provenance
            ),
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> "TypedNodeSpec":
        return cls(
            node_id=str(record.get("node_id") or ""),
            node_type=str(record.get("node_type") or ""),
            input_node_ids=tuple(map(str, record.get("input_node_ids") or ())),
            parameters=dict(record.get("parameters") or {}),
            output_semantic_type=str(record.get("output_semantic_type") or ""),
            entity_scope=str(record.get("entity_scope") or ""),
            temporal_semantics=dict(record.get("temporal_semantics") or {}),
            observable_clock=str(record.get("observable_clock") or ""),
            maturity=str(record.get("maturity") or ""),
            unit_signature=str(record.get("unit_signature") or ""),
            support_unit=str(record.get("support_unit") or ""),
            source_lineage=tuple(map(str, record.get("source_lineage") or ())),
            component_route_provenance=tuple(
                map(str, record.get("component_route_provenance") or ())
            ),
        )


@dataclass(frozen=True, slots=True)
class ProgramOutputSpec:
    stock_score_node_id: str
    eligibility_mask_node_id: str
    exposure_multiplier_node_id: str
    veto_mask_node_id: str

    def __post_init__(self) -> None:
        if any(not value for value in asdict(self).values()):
            raise ValueError("all four program outputs must be explicit")


@dataclass(frozen=True, slots=True)
class JointClockContractV1:
    component_clock_node_ids: tuple[str, ...]
    missing_component_policy: str
    action_session_policy: str
    pit_guard_version: str
    joint_eligible_from_policy: str = "ROW_WISE_MAX_REQUIRED_COMPONENT_MATURITY"
    joint_support_policy: str = "REQUIRED_COMPONENT_ELIGIBILITY_INTERSECTION"

    def __post_init__(self) -> None:
        if not self.component_clock_node_ids:
            raise ValueError("joint clock requires at least one component")
        if self.missing_component_policy not in {
            "FAIL_CLOSED_REQUIRED_COMPONENT",
            "IDENTITY_VALUE_OPTIONAL_COMPONENT",
        }:
            raise ValueError("unknown joint clock missing-component policy")
        if not self.action_session_policy or not self.pit_guard_version:
            raise ValueError("joint clock action session and PIT guard are required")


@dataclass(frozen=True, slots=True)
class MatchedControlOperationV1:
    operation: str
    target_node_ids: tuple[str, ...]
    replacement: Mapping[str, Any] = field(default_factory=dict)
    diagnostic_only: bool = False

    def __post_init__(self) -> None:
        if self.operation not in CONTROL_OPERATIONS:
            raise ValueError(f"unsupported matched-control operation: {self.operation}")
        if not self.target_node_ids and self.operation != "BASE_PAYLOAD_ONLY":
            raise ValueError("matched-control operation requires a target")
        if self.operation == "REPLACE_WITH_WRONG_LAG_CONTROL" and not self.diagnostic_only:
            raise ValueError("wrong-lag control must remain diagnostic-only")


@dataclass(frozen=True, slots=True)
class MatchedControlPlanV1:
    control_constructor_id: str
    operations: tuple[MatchedControlOperationV1, ...]
    pair_support_policy: str
    pair_maturity_policy: str
    portfolio_contract_unchanged: bool = True

    def __post_init__(self) -> None:
        if not self.control_constructor_id:
            raise ValueError("matched-control plan requires a constructor id")
        if not self.portfolio_contract_unchanged:
            raise ValueError("matched control may not change the portfolio contract")


@dataclass(frozen=True, slots=True)
class ProposalLineageV1:
    template_stratum_id: str
    seed: int
    attempt: int
    sampler: str
    proposal_parent: str = ""
    generation_source: str = ""

    def __post_init__(self) -> None:
        if self.template_stratum_id not in set(ROUTE_IDS) | {"MANUAL_FIXTURE"}:
            raise ValueError("proposal lineage has unknown fixed template stratum")
        if not self.sampler:
            raise ValueError("proposal lineage requires a sampler")

    def to_record(self, *, semantic_program_hash: str) -> dict[str, Any]:
        payload = {
            "semantic_program_hash": semantic_program_hash,
            **asdict(self),
        }
        payload["proposal_instance_hash"] = stable_hash(payload)
        return payload


@dataclass(frozen=True, slots=True)
class CandidateProgramSpecV1:
    schema_version: str
    nodes: tuple[TypedNodeSpec, ...]
    outputs: ProgramOutputSpec
    portfolio_contract: Mapping[str, Any]
    joint_clock_contract: JointClockContractV1
    complexity_budget: ComplexityBudgetV1
    matched_control_plan: MatchedControlPlanV1
    legacy_component_provenance: tuple[str, ...] = ()
    frozen_component_references: tuple[str, ...] = ()
    compiler_semantics_version: str = PROGRAM_COMPILER_SEMANTICS_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROGRAM_SCHEMA_VERSION:
            raise ValueError("candidate program schema version mismatch")
        if self.compiler_semantics_version != PROGRAM_COMPILER_SEMANTICS_VERSION:
            raise ValueError("candidate program compiler semantics mismatch")
        if not self.nodes:
            raise ValueError("candidate program requires nodes")
        ids = [node.node_id for node in self.nodes]
        if len(ids) != len(set(ids)):
            raise ValueError("candidate program node ids must be unique")
        if str(self.portfolio_contract.get("decoder_id") or "") != PROGRAM_PORTFOLIO_AUTHORITY:
            raise ValueError("candidate program must reuse TOPK_10_EQUAL")

    def semantic_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "nodes": [
                node.semantic_payload()
                for node in sorted(self.nodes, key=lambda item: item.node_id)
            ],
            "outputs": asdict(self.outputs),
            "portfolio_contract": _canonicalize(self.portfolio_contract),
            "joint_clock_contract": _canonicalize(
                asdict(self.joint_clock_contract)
            ),
            "complexity_budget": asdict(self.complexity_budget),
            "complexity_policy_hash": self.complexity_budget.policy_hash,
            "matched_control_plan": _canonicalize(
                asdict(self.matched_control_plan)
            ),
            "legacy_component_provenance": sorted(
                self.legacy_component_provenance
            ),
            "frozen_component_references": sorted(
                self.frozen_component_references
            ),
            "compiler_semantics_version": self.compiler_semantics_version,
        }

    @property
    def semantic_program_hash(self) -> str:
        return stable_hash(self.semantic_payload())

    @property
    def program_id(self) -> str:
        return "cn.program." + self.semantic_program_hash[:32]

    def to_record(self) -> dict[str, Any]:
        return {
            **self.semantic_payload(),
            "program_id": self.program_id,
            "semantic_program_hash": self.semantic_program_hash,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> "CandidateProgramSpecV1":
        budget = ComplexityBudgetV1(**dict(record.get("complexity_budget") or {}))
        clock = JointClockContractV1(
            **dict(record.get("joint_clock_contract") or {})
        )
        control_record = dict(record.get("matched_control_plan") or {})
        operations = tuple(
            MatchedControlOperationV1(**dict(item))
            for item in control_record.pop("operations", ())
        )
        control = MatchedControlPlanV1(operations=operations, **control_record)
        spec = cls(
            schema_version=str(record.get("schema_version") or ""),
            nodes=tuple(
                TypedNodeSpec.from_record(item)
                for item in record.get("nodes") or ()
            ),
            outputs=ProgramOutputSpec(**dict(record.get("outputs") or {})),
            portfolio_contract=dict(record.get("portfolio_contract") or {}),
            joint_clock_contract=clock,
            complexity_budget=budget,
            matched_control_plan=control,
            legacy_component_provenance=tuple(
                map(str, record.get("legacy_component_provenance") or ())
            ),
            frozen_component_references=tuple(
                map(str, record.get("frozen_component_references") or ())
            ),
            compiler_semantics_version=str(
                record.get("compiler_semantics_version") or ""
            ),
        )
        if str(record.get("semantic_program_hash") or "") != spec.semantic_program_hash:
            raise ValueError("candidate program semantic hash mismatch")
        if str(record.get("program_id") or "") != spec.program_id:
            raise ValueError("candidate program id mismatch")
        return spec


@dataclass(frozen=True, slots=True)
class CompiledCandidateProgramV1:
    program_id: str
    semantic_program_hash: str
    ordered_node_ids: tuple[str, ...]
    output_node_ids: Mapping[str, str]
    legacy_component_verdicts: tuple[Mapping[str, Any], ...]
    shared_dag_plan_hash: str
    compiler_semantics_version: str

    def to_record(self) -> dict[str, Any]:
        payload = _canonicalize(asdict(self))
        payload["compiled_program_hash"] = stable_hash(payload)
        return payload


def _topological_order(nodes: Sequence[TypedNodeSpec]) -> tuple[str, ...]:
    by_id = {node.node_id: node for node in nodes}
    missing = sorted(
        {
            dependency
            for node in nodes
            for dependency in node.input_node_ids
            if dependency not in by_id
        }
    )
    if missing:
        raise ValueError(f"program graph references missing nodes: {missing}")
    indegree = {node.node_id: len(set(node.input_node_ids)) for node in nodes}
    children = {node.node_id: set() for node in nodes}
    for node in nodes:
        for dependency in node.input_node_ids:
            children[dependency].add(node.node_id)
    ready = [node_id for node_id, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        node_id = heapq.heappop(ready)
        order.append(node_id)
        for child in sorted(children[node_id]):
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, child)
    if len(order) != len(nodes):
        raise ValueError("candidate program graph contains a cycle")
    return tuple(order)


def _program_depth(nodes: Sequence[TypedNodeSpec], order: Sequence[str]) -> int:
    by_id = {node.node_id: node for node in nodes}
    depth: dict[str, int] = {}
    for node_id in order:
        node = by_id[node_id]
        depth[node_id] = 1 + max(
            (depth[parent] for parent in node.input_node_ids), default=0
        )
    return max(depth.values(), default=0)


def _constant_node(
    node_id: str,
    *,
    value: bool | float,
    semantic_type: str,
    clock: str,
    maturity: str,
    support_unit: str,
) -> TypedNodeSpec:
    return TypedNodeSpec(
        node_id=node_id,
        node_type="CONSTANT",
        input_node_ids=(),
        parameters={"value": value},
        output_semantic_type=semantic_type,
        entity_scope="CONSTANT",
        temporal_semantics={"kind": "IDENTITY_OUTPUT"},
        observable_clock=clock,
        maturity=maturity,
        unit_signature="boolean" if isinstance(value, bool) else "dimensionless",
        support_unit=support_unit,
    )


def legacy_candidate_program_v1(
    candidate: Mapping[str, Any],
    *,
    portfolio_contract: Mapping[str, Any],
    complexity_budget: ComplexityBudgetV1 | None = None,
) -> CandidateProgramSpecV1:
    """Wrap one old candidate without changing its identity or expression."""

    component = _strip_nonsemantic_metadata(candidate)
    candidate_id = str(component.get("candidate_id") or "")
    route_id = str(component.get("route_id") or "")
    if not candidate_id or route_id not in ROUTE_IDS:
        raise ValueError("legacy component requires a registered candidate and route")
    clock = str(component.get("clock_contract") or component.get("maturity_rule") or "UNSPECIFIED")
    maturity = str(component.get("maturity_contract") or component.get("maturity_rule") or clock)
    support = str(component.get("support_unit") or "UNSPECIFIED")
    score = TypedNodeSpec(
        node_id="legacy_score",
        node_type="LEGACY_CANDIDATE_COMPONENT",
        input_node_ids=(),
        parameters={"candidate": component},
        output_semantic_type="STOCK_SCORE",
        entity_scope="STOCK",
        temporal_semantics={"kind": "LEGACY_TYPED_ROUTE_EXPRESSION"},
        observable_clock=clock,
        maturity=maturity,
        unit_signature=str(component.get("unit_signature") or "dimensionless"),
        support_unit=support,
        source_lineage=tuple(map(str, component.get("source_field_ids") or ())),
        component_route_provenance=(route_id,),
    )
    nodes = (
        score,
        _constant_node(
            "eligibility_identity",
            value=True,
            semantic_type="STOCK_MASK",
            clock=clock,
            maturity=maturity,
            support_unit=support,
        ),
        _constant_node(
            "exposure_identity",
            value=1.0,
            semantic_type="STOCK_MULTIPLIER",
            clock=clock,
            maturity=maturity,
            support_unit=support,
        ),
        _constant_node(
            "veto_identity",
            value=False,
            semantic_type="STOCK_MASK",
            clock=clock,
            maturity=maturity,
            support_unit=support,
        ),
    )
    return CandidateProgramSpecV1(
        schema_version=PROGRAM_SCHEMA_VERSION,
        nodes=nodes,
        outputs=ProgramOutputSpec(
            stock_score_node_id="legacy_score",
            eligibility_mask_node_id="eligibility_identity",
            exposure_multiplier_node_id="exposure_identity",
            veto_mask_node_id="veto_identity",
        ),
        portfolio_contract=dict(portfolio_contract),
        joint_clock_contract=JointClockContractV1(
            component_clock_node_ids=("legacy_score",),
            missing_component_policy="FAIL_CLOSED_REQUIRED_COMPONENT",
            action_session_policy="EXISTING_CANDIDATE_ACTION_SESSION",
            pit_guard_version=str(component.get("pit_guard_version") or "LEGACY_ROUTE_GUARD"),
        ),
        complexity_budget=complexity_budget or ComplexityBudgetV1(),
        matched_control_plan=MatchedControlPlanV1(
            control_constructor_id=str(component.get("control_constructor_id") or "LEGACY_REGISTERED_CONTROL"),
            operations=(),
            pair_support_policy=str(
                component.get("pair_support_alignment_policy")
                or "PRIMARY_CONTROL_FINITE_INTERSECTION_AT_SHARED_COORDINATE"
            ),
            pair_maturity_policy=str(
                component.get("pair_maturity_alignment_policy")
                or "MAX_PRIMARY_CONTROL_MATURITY_BEFORE_SHARED_SUPPORT"
            ),
        ),
        legacy_component_provenance=(candidate_id,),
    )


class ProgramCompilerV1:
    """Thin graph compiler delegating legacy semantics to existing authority."""

    def __init__(self, registry: UnifiedCapabilityRegistry) -> None:
        self.registry = registry
        self.typed_route_compiler = TypedRouteCompiler(registry)

    def compile(self, spec: CandidateProgramSpecV1) -> CompiledCandidateProgramV1:
        order = _topological_order(spec.nodes)
        if len(spec.nodes) > spec.complexity_budget.maximum_effective_nodes:
            raise ValueError("candidate program exceeds node-count budget")
        if _program_depth(spec.nodes, order) > spec.complexity_budget.maximum_program_depth:
            raise ValueError("candidate program exceeds depth budget")
        by_id = {node.node_id: node for node in spec.nodes}
        output_types = {
            "stock_score_node_id": {"STOCK_SCORE", "STOCK_VALUE"},
            "eligibility_mask_node_id": {"STOCK_MASK"},
            "exposure_multiplier_node_id": {"STOCK_MULTIPLIER", "STOCK_VALUE"},
            "veto_mask_node_id": {"STOCK_MASK"},
        }
        for output_name, allowed in output_types.items():
            node_id = str(getattr(spec.outputs, output_name))
            if node_id not in by_id:
                raise ValueError(f"program output references missing node: {node_id}")
            if by_id[node_id].output_semantic_type not in allowed:
                raise ValueError(
                    f"program output {output_name} has incompatible semantic type"
                )
        unknown_clock_nodes = sorted(
            set(spec.joint_clock_contract.component_clock_node_ids) - set(by_id)
        )
        if unknown_clock_nodes:
            raise ValueError(
                f"joint clock references missing nodes: {unknown_clock_nodes}"
            )

        verdicts: list[dict[str, Any]] = []
        legacy_candidates: list[dict[str, Any]] = []
        for node in spec.nodes:
            if node.node_type != "LEGACY_CANDIDATE_COMPONENT":
                continue
            candidate = dict(node.parameters.get("candidate") or {})
            verdict = self.typed_route_compiler.compile(candidate)
            if not verdict.legal:
                raise ValueError(
                    "legacy component rejected by TypedRouteCompiler: "
                    f"{verdict.rejection_code}:{verdict.reason}"
                )
            expected_canonical = str(
                candidate.get("canonical_expression")
                or candidate.get("expression")
                or ""
            )
            if verdict.canonical_expression != expected_canonical:
                raise ValueError("legacy component canonical expression drift")
            expected_exact = str(candidate.get("exact_identity") or "")
            if expected_exact and verdict.exact_identity != expected_exact:
                raise ValueError("legacy component exact identity drift")
            expected_canonical_id = str(candidate.get("canonical_identity") or "")
            if expected_canonical_id and verdict.canonical_identity != expected_canonical_id:
                raise ValueError("legacy component canonical identity drift")
            row = dict(candidate)
            row.update(
                {
                    "canonical_expression": verdict.canonical_expression,
                    "exact_identity": verdict.exact_identity,
                    "canonical_identity": verdict.canonical_identity,
                    "field_ids": list(verdict.field_ids),
                    "source_field_ids": list(verdict.source_field_ids),
                    "representation_ids": list(verdict.representation_ids),
                    "support_unit": verdict.support_unit,
                    "maturity_rule": verdict.maturity_rule,
                }
            )
            legacy_candidates.append(row)
            verdicts.append(
                {
                    "node_id": node.node_id,
                    "candidate_id": str(candidate.get("candidate_id") or ""),
                    "route_id": verdict.route_id,
                    "canonical_expression": verdict.canonical_expression,
                    "canonical_identity": verdict.canonical_identity,
                    "exact_identity": verdict.exact_identity,
                    "decision": verdict.decision,
                }
            )
        shared_plan_hash = ""
        if legacy_candidates:
            shared_plan_hash = SharedMultiCandidateDAGPlan.build(
                legacy_candidates
            ).plan_hash
        return CompiledCandidateProgramV1(
            program_id=spec.program_id,
            semantic_program_hash=spec.semantic_program_hash,
            ordered_node_ids=order,
            output_node_ids=asdict(spec.outputs),
            legacy_component_verdicts=tuple(verdicts),
            shared_dag_plan_hash=shared_plan_hash,
            compiler_semantics_version=PROGRAM_COMPILER_SEMANTICS_VERSION,
        )
