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

from our_system_phase2.services.candidate_materialization_requirements import (
    PhysicalLeafResolution,
    resolve_required_physical_leaves,
)
from our_system_phase2.services.phase3cm_streaming_dag import (
    SharedMultiCandidateDAGPlan,
)
from our_system_phase2.services.typed_route_compiler import (
    CompileVerdict,
    TypedRouteCompiler,
)
from our_system_phase2.services.typed_primitive_gate import expression_fields
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

NODE_EXECUTION_PHASE = {
    **{node_type: 1 for node_type in LEAF_NODE_TYPES},
    **{node_type: 3 for node_type in TEMPORAL_NODE_TYPES},
    **{node_type: 4 for node_type in EVENT_NODE_TYPES},
    **{node_type: 5 for node_type in STATE_NODE_TYPES},
    "VETO": 6,
    "FILTER": 7,
    "INTERSECT": 7,
    "UNION": 7,
    "GATE": 7,
    "MODULATE": 8,
    "MULTIPLY": 8,
    "ADD": 9,
    "SUBTRACT": 9,
    "SAFE_DIVIDE": 9,
    "MIN": 9,
    "MAX": 9,
    "CONDITIONAL_SWITCH": 9,
    **{node_type: 10 for node_type in CROSS_SECTIONAL_NODE_TYPES},
}
NODE_EXECUTION_PHASE["LEGACY_CANDIDATE_COMPONENT"] = 10

NUMERIC_SEMANTIC_TYPES = frozenset(
    {
        "STOCK_VALUE",
        "STOCK_SCORE",
        "STOCK_MULTIPLIER",
        "MARKET_VALUE",
        "GROUP_VALUE",
        "STATE_VALUE",
        "SCALAR",
    }
)
MASK_SEMANTIC_TYPES = frozenset(
    {"STOCK_MASK", "MARKET_MASK", "GROUP_MASK"}
)


def _support_unit_matches_entity(entity_scope: str, support_unit: str) -> bool:
    normalized = str(support_unit).lower()
    required_fragments = {
        "STOCK": ("stock",),
        "MARKET": ("market",),
        "INDUSTRY": ("industry", "group"),
        "PLATE": ("plate", "group"),
        "EVENT": ("event", "episode", "disclosure"),
        "CONSTANT": ("constant",),
    }.get(entity_scope, ())
    return bool(required_fragments) and any(
        fragment in normalized for fragment in required_fragments
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
            MatchedControlOperationV1(
                operation=str(item.get("operation") or ""),
                target_node_ids=tuple(map(str, item.get("target_node_ids") or ())),
                replacement=dict(item.get("replacement") or {}),
                diagnostic_only=bool(item.get("diagnostic_only", False)),
            )
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
    output_expressions: Mapping[str, str]
    joint_clock_contract: Mapping[str, Any]
    component_clock_requirements: Mapping[str, Mapping[str, Any]]
    field_lags: Mapping[str, int]
    node_execution_plan: tuple[Mapping[str, Any], ...]
    physical_leaf_ids: tuple[str, ...]
    external_adapter_requirements: tuple[str, ...]
    complexity_report: Mapping[str, Any]
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


def _node_parameter_int(node: TypedNodeSpec, key: str) -> int:
    value = int(node.parameters.get(key) or 0)
    if value <= 0:
        raise ValueError(f"program node {node.node_id} requires positive {key}")
    return value


def _node_expression(
    node: TypedNodeSpec,
    parent_expressions: Sequence[str],
) -> str:
    """Compile one typed node to the existing panel-expression DSL."""

    if node.node_type in {
        "STOCK_FIELD",
        "MARKET_FIELD",
        "INDUSTRY_FIELD",
        "PLATE_FIELD",
        "EVENT_EPISODE",
    }:
        field_id = str(node.parameters.get("field_id") or "")
        if not field_id:
            raise ValueError(f"program field node {node.node_id} lacks field_id")
        return f"${field_id}"
    if node.node_type == "STATE_REPRESENTATION":
        expression = str(node.parameters.get("state_source_expression") or "")
        if not expression:
            raise ValueError("state representation requires a source expression")
        return expression
    if node.node_type == "LEGACY_CANDIDATE_COMPONENT":
        candidate = dict(node.parameters.get("candidate") or {})
        expression = str(
            candidate.get("canonical_expression")
            or candidate.get("expression")
            or ""
        )
        if not expression:
            raise ValueError("legacy component lacks its canonical expression")
        return expression
    if node.node_type == "FROZEN_BROAD_EVENT_REF":
        field_id = str(node.parameters.get("field_id") or "")
        operator = (
            "MatchedControlReplay"
            if bool(node.parameters.get("is_matched_control"))
            else "FrozenMechanismReplay"
        )
        if not field_id:
            raise ValueError("frozen Broad Event node lacks field_id")
        return f"{operator}(${field_id})"
    if node.node_type == "CONSTANT":
        value = node.parameters.get("value")
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        raise ValueError("program constant must be boolean or numeric")

    unary_windows = {
        "LAG": "Delay",
        "DELTA": "Delta",
        "SLOPE": "Slope",
        "ACCELERATION": "Acceleration",
        "ROLLING_MEAN": "Mean",
        "ROLLING_STD": "Std",
        "SELF_QUANTILE": "SelfQuantile",
        "PERSISTENCE": "Persistence",
        "FIRST_HIT": "FirstHit",
        "EVENT_COUNT": "EventCount",
    }
    if node.node_type in unary_windows:
        return (
            f"{unary_windows[node.node_type]}({parent_expressions[0]},"
            f"{_node_parameter_int(node, 'window')})"
        )
    if node.node_type == "SELF_ZSCORE":
        window = _node_parameter_int(node, "window")
        minimum_ratio = float(node.parameters.get("minimum_valid_ratio") or 0.6)
        return f"MaskedZScore({parent_expressions[0]},{window},{minimum_ratio})"
    if node.node_type in {"STATE_AGE", "TIME_SINCE", "EVENT_AGE"}:
        operator = "StateAge" if node.node_type == "STATE_AGE" else "EventAge"
        return f"{operator}({parent_expressions[0]})"
    if node.node_type == "SHORT_LONG_SPREAD":
        short = _node_parameter_int(node, "short_window")
        long = _node_parameter_int(node, "long_window")
        if short >= long:
            raise ValueError("short-long spread requires short_window < long_window")
        return (
            f"Sub(Slope({parent_expressions[0]},{short}),"
            f"Slope({parent_expressions[0]},{long}))"
        )

    unary = {
        "CSRANK": "CSRank",
        "CS_ZSCORE": "ZScore",
        "WINSORIZE": "Winsorize",
        "TOP_QUANTILE_MASK": "TopQuantileMask",
        "EVENT_DIRECTION": "Sign",
        "REPEATED_EVENT_SUPPRESSION": "RepeatedEventSuppression",
        "STATE": "Positive",
        "STATE_PERSISTENCE": "StateAge",
        "BREADTH_RATIO": "Positive",
        "LIMIT_DENSITY": "Positive",
        "LIQUIDITY_STATE": "Positive",
        "VOLATILITY_STATE": "Positive",
        "VETO": "Positive",
    }
    comparison_state_types = {
        "STATE",
        "BREADTH_RATIO",
        "LIMIT_DENSITY",
        "LIQUIDITY_STATE",
        "VOLATILITY_STATE",
    }
    if node.node_type in unary and not (
        node.node_type in comparison_state_types and len(parent_expressions) == 2
    ):
        if node.node_type == "TOP_QUANTILE_MASK":
            quantile = float(node.parameters.get("top_fraction") or 0.1)
            if not 0.0 < quantile <= 1.0:
                raise ValueError("top quantile mask requires top_fraction in (0,1]")
            return f"TopQuantileMask({parent_expressions[0]},{quantile})"
        return f"{unary[node.node_type]}({parent_expressions[0]})"
    if node.node_type in comparison_state_types and len(parent_expressions) == 2:
        comparison = str(node.parameters.get("comparison") or "LEFT_GT_RIGHT")
        if comparison == "LEFT_GT_RIGHT":
            return f"Positive(Sub({parent_expressions[0]},{parent_expressions[1]}))"
        if comparison == "LEFT_LT_RIGHT":
            return f"Positive(Sub({parent_expressions[1]},{parent_expressions[0]}))"
        raise ValueError(f"unsupported state comparison: {comparison}")
    if node.node_type == "TRANSITION":
        return (
            f"Transition({parent_expressions[0]},"
            f"{node.parameters.get('from_state')},{node.parameters.get('to_state')})"
        )
    if node.node_type in {"RESIDUALIZE", "SIZE_NEUTRALIZE"}:
        expression = parent_expressions[0]
        for control in parent_expressions[1:]:
            expression = f"CSResidual({expression},{control})"
        return expression
    if node.node_type == "INDUSTRY_NEUTRALIZE":
        return f"WithinGroupDemean({parent_expressions[0]},{parent_expressions[1]})"
    if node.node_type == "WITHIN_GROUP_RANK":
        return f"WithinGroupRank({parent_expressions[0]},{parent_expressions[1]})"
    binary = {
        "ADD": "Add",
        "SUBTRACT": "Sub",
        "MULTIPLY": "Mul",
        "MIN": "PointwiseMin",
        "MAX": "PointwiseMax",
        "EPISODE_INTERSECT": "MaskIntersect",
        "EPISODE_UNION": "MaskUnion",
        "INTERSECT": "MaskIntersect",
        "UNION": "MaskUnion",
    }
    if node.node_type in binary:
        expression = parent_expressions[0]
        for parent in parent_expressions[1:]:
            expression = f"{binary[node.node_type]}({expression},{parent})"
        return expression
    if node.node_type == "SAFE_DIVIDE":
        floor = float(node.parameters.get("floor") or 1e-6)
        return f"SafeDiv({parent_expressions[0]},{parent_expressions[1]},{floor})"
    if node.node_type in {"GATE", "MODULATE"}:
        return f"Mul({parent_expressions[0]},{parent_expressions[1]})"
    if node.node_type == "FILTER":
        expression = parent_expressions[0]
        for parent in parent_expressions[1:]:
            expression = f"MaskIntersect({expression},{parent})"
        return expression
    if node.node_type == "CONDITIONAL_SWITCH":
        return (
            f"ConditionalSwitch({parent_expressions[0]},"
            f"{parent_expressions[1]},{parent_expressions[2]})"
        )
    if node.node_type in {
        "EVENT_WINDOW",
        "PRE_EVENT_PATH",
        "POST_EVENT_STATE",
    }:
        pre = int(node.parameters.get("pre") or 0)
        post = int(node.parameters.get("post") or 0)
        return (
            f"EventWindow({parent_expressions[0]},"
            f"{parent_expressions[1]},{pre},{post})"
        )
    raise ValueError(
        f"program node {node.node_id} has no existing-evaluator lowering: "
        f"{node.node_type}"
    )


def typed_node_execution_signature_v1(node: TypedNodeSpec) -> str:
    """Hash only the semantics consumed by the existing expression evaluator."""

    parent_expressions = tuple(
        f"@parent:{parent_id}" for parent_id in node.input_node_ids
    )
    return stable_hash(
        {
            "compiled_expression": _node_expression(node, parent_expressions),
            "output_semantic_type": node.output_semantic_type,
            "entity_scope": node.entity_scope,
            "unit_signature": node.unit_signature,
        }
    )


def _validate_node_semantics(
    node: TypedNodeSpec,
    parents: Sequence[TypedNodeSpec],
    registry: UnifiedCapabilityRegistry,
) -> None:
    if bool(node.temporal_semantics.get("uses_future_revision")) or bool(
        node.parameters.get("uses_future_revision")
    ):
        raise ValueError("program node attempts to use a future revision")
    if bool(node.parameters.get("fabricates_intrabar_order")):
        raise ValueError("program node attempts to fabricate intrabar order")
    if bool(node.parameters.get("online_adaptive_parameter")):
        raise ValueError("program node contains online adaptive parameters")
    if node.node_type in LEAF_NODE_TYPES and node.input_node_ids:
        raise ValueError(f"leaf node {node.node_id} may not have inputs")
    if node.node_type not in LEAF_NODE_TYPES and not parents:
        raise ValueError(f"operator node {node.node_id} requires inputs")
    if any(
        NODE_EXECUTION_PHASE[parent.node_type] > NODE_EXECUTION_PHASE[node.node_type]
        for parent in parents
    ):
        raise ValueError("program dependency violates fixed semantic phase order")
    if parents:
        expected_source_lineage = tuple(
            sorted(
                {
                    identity
                    for parent in parents
                    for identity in parent.source_lineage
                }
            )
        )
        expected_route_provenance = tuple(
            sorted(
                {
                    route
                    for parent in parents
                    for route in parent.component_route_provenance
                }
            )
        )
        if tuple(sorted(node.source_lineage)) != expected_source_lineage:
            raise ValueError(
                "operator source lineage must equal the parent lineage union"
            )
        if tuple(sorted(node.component_route_provenance)) != expected_route_provenance:
            raise ValueError(
                "operator route provenance must equal the parent route union"
            )

    if node.node_type in {
        "STOCK_FIELD",
        "MARKET_FIELD",
        "INDUSTRY_FIELD",
        "PLATE_FIELD",
        "EVENT_EPISODE",
    }:
        field_id = str(node.parameters.get("field_id") or "")
        try:
            field = registry.resolve(field_id)
        except KeyError as exc:
            raise ValueError(f"program field is not registered: {field_id}") from exc
        expected_scope = {
            "STOCK_FIELD": "STOCK",
            "MARKET_FIELD": "MARKET",
            "INDUSTRY_FIELD": "INDUSTRY",
            "PLATE_FIELD": "PLATE",
        }.get(node.node_type)
        if expected_scope and field.entity_scope.upper() != expected_scope:
            raise ValueError(
                f"program field {field_id} has wrong entity scope for {node.node_type}"
            )
        if not field.search_eligible and node.node_type != "EVENT_EPISODE":
            raise ValueError(f"program field is not generator-eligible: {field_id}")
        if not node.component_route_provenance or not set(
            node.component_route_provenance
        ).issubset(field.allowed_routes):
            raise ValueError("program field route provenance is not registry-authorized")
        if tuple(node.source_lineage) != (
            field.source_field_id,
            field.representation_id,
        ):
            raise ValueError("program field source lineage is not registry-authorized")
        if node.node_type in {"INDUSTRY_FIELD", "PLATE_FIELD"} and str(
            node.parameters.get("capability_status") or ""
        ) != "PIT_MATERIALIZATION_AUTHORIZED":
            raise ValueError(
                f"{node.node_type} remains fail-closed without PIT materialization authority"
            )
        authoritative_contract = {
            "observable_clock": field.observable_clock,
            "maturity": field.maturity_rule,
            "source_lag": field.source_lag,
            "source_lag_unit": field.source_lag_unit,
            "revision_policy": field.revision_policy,
            "pit_status": field.pit_status,
            "support_unit": field.support_unit,
            "source_field_id": field.source_field_id,
            "representation_id": field.representation_id,
            "temporal_kind": field.temporal_semantics,
        }
        declared_contract = {
            "observable_clock": node.observable_clock,
            "maturity": node.maturity,
            "source_lag": node.parameters.get("source_lag"),
            "source_lag_unit": node.parameters.get("source_lag_unit"),
            "revision_policy": node.parameters.get("revision_policy"),
            "pit_status": node.parameters.get("pit_status"),
            "support_unit": node.support_unit,
            "source_field_id": node.parameters.get("source_field_id"),
            "representation_id": node.parameters.get("representation_id"),
            "temporal_kind": node.temporal_semantics.get("kind"),
        }
        drift = sorted(
            key
            for key, value in authoritative_contract.items()
            if str(declared_contract.get(key)) != str(value)
        )
        temporal_lag_drift = sorted(
            key
            for key, value in {
                "source_lag": field.source_lag,
                "source_lag_unit": field.source_lag_unit,
                "revision_policy": field.revision_policy,
            }.items()
            if str(node.temporal_semantics.get(key)) != str(value)
        )
        if drift or temporal_lag_drift:
            raise ValueError(
                "program field contract drifts from registry authority: "
                f"declared={drift}, temporal={temporal_lag_drift}"
            )
    if node.node_type == "EVENT_EPISODE":
        if str(node.parameters.get("vote_policy") or "") != "ONE_EPISODE_ONE_VOTE":
            raise ValueError("event episode requires one-episode-one-vote")
        if not node.parameters.get("observable_cutoff") or not node.parameters.get(
            "registered_action_delay"
        ):
            raise ValueError("event episode requires cutoff and action delay")
        if bool(node.parameters.get("latched_observation_as_lifecycle")):
            raise ValueError("latched observation may not masquerade as lifecycle state")
    if node.node_type == "FROZEN_BROAD_EVENT_REF":
        required = (
            "field_id",
            "frozen_mechanism_id",
            "frozen_behavior_cluster_id",
            "frozen_inventory_hash",
        )
        if any(not node.parameters.get(key) for key in required):
            raise ValueError("frozen Broad Event node lacks inventory binding")
        if bool(node.parameters.get("discovery_budget_eligible")) or bool(
            node.parameters.get("dynamic_credit_eligible")
        ):
            raise ValueError("frozen Broad Event may not receive discovery credit")
        field = registry.resolve(str(node.parameters["field_id"]))
        if "BROAD_EVENT_FROZEN_ENTRY" not in field.allowed_routes:
            raise ValueError("frozen Broad Event field lacks registered replay route")
        if tuple(node.component_route_provenance) != (
            "BROAD_EVENT_FROZEN_ENTRY",
        ) or tuple(node.source_lineage) != (
            field.source_field_id,
            field.representation_id,
        ):
            raise ValueError(
                "frozen Broad Event provenance is not registry-authorized"
            )
        mechanism = dict((field.metadata or {}).get("frozen_mechanism") or {})
        if str(node.parameters["frozen_mechanism_id"]) != str(
            mechanism.get("mechanism_id") or ""
        ):
            raise ValueError("frozen Broad Event mechanism is not registry-authorized")
        if str(node.parameters["frozen_behavior_cluster_id"]) != str(
            mechanism.get("behavior_cluster_id") or ""
        ):
            raise ValueError("frozen Broad Event behavior cluster is not registry-authorized")
        registered_inventory_hash = stable_hash(
            [row.to_dict() for row in registry.fields_for_route("BROAD_EVENT_FROZEN_ENTRY")]
        )
        if str(node.parameters["frozen_inventory_hash"]) != registered_inventory_hash:
            raise ValueError("frozen Broad Event inventory hash is not registry-authorized")
        frozen_contract = {
            "observable_clock": field.observable_clock,
            "maturity": field.maturity_rule,
            "source_lag": field.source_lag,
            "source_lag_unit": field.source_lag_unit,
            "revision_policy": "FROZEN_INVENTORY_NO_FUTURE_REVISION",
            "pit_status": field.pit_status,
            "support_unit": field.support_unit,
            "source_field_id": field.source_field_id,
            "representation_id": field.representation_id,
            "temporal_kind": "FROZEN_EPISODE_REPLAY",
            "temporal_source_lag": field.source_lag,
            "temporal_source_lag_unit": field.source_lag_unit,
            "temporal_revision_policy": "FROZEN_INVENTORY_NO_FUTURE_REVISION",
        }
        frozen_declared = {
            "observable_clock": node.observable_clock,
            "maturity": node.maturity,
            "source_lag": node.parameters.get("source_lag"),
            "source_lag_unit": node.parameters.get("source_lag_unit"),
            "revision_policy": node.parameters.get("revision_policy"),
            "pit_status": node.parameters.get("pit_status"),
            "support_unit": node.support_unit,
            "source_field_id": node.parameters.get("source_field_id"),
            "representation_id": node.parameters.get("representation_id"),
            "temporal_kind": node.temporal_semantics.get("kind"),
            "temporal_source_lag": node.temporal_semantics.get("source_lag"),
            "temporal_source_lag_unit": node.temporal_semantics.get(
                "source_lag_unit"
            ),
            "temporal_revision_policy": node.temporal_semantics.get(
                "revision_policy"
            ),
        }
        frozen_drift = sorted(
            key
            for key, value in frozen_contract.items()
            if str(frozen_declared.get(key)) != str(value)
        )
        if frozen_drift:
            raise ValueError(
                "frozen Broad Event contract drifts from registry authority: "
                f"{frozen_drift}"
            )

    if node.node_type in {"CSRANK", "CS_ZSCORE", "WINSORIZE", "TOP_QUANTILE_MASK"}:
        if (
            len(parents) != 1
            or parents[0].entity_scope != "STOCK"
            or parents[0].output_semantic_type not in NUMERIC_SEMANTIC_TYPES
        ):
            raise ValueError("market/group payload may not enter direct stock cross-sectional mapping")
        expected_output_types = (
            {"STOCK_MASK"}
            if node.node_type == "TOP_QUANTILE_MASK"
            else {"STOCK_VALUE", "STOCK_SCORE"}
        )
        if node.entity_scope != "STOCK" or node.output_semantic_type not in expected_output_types:
            raise ValueError("stock cross-sectional operator has incompatible output type")
    if node.node_type in {
        "LAG",
        "DELTA",
        "SLOPE",
        "ACCELERATION",
        "ROLLING_MEAN",
        "ROLLING_STD",
        "SELF_ZSCORE",
        "SELF_QUANTILE",
        "SHORT_LONG_SPREAD",
    }:
        if len(parents) != 1 or parents[0].output_semantic_type not in NUMERIC_SEMANTIC_TYPES:
            raise ValueError("temporal operators require exactly one numeric input")
        parent = parents[0]
        expected_unit = (
            "dimensionless"
            if node.node_type in {"SELF_ZSCORE", "SELF_QUANTILE"}
            else parent.unit_signature
        )
        if (
            node.output_semantic_type != parent.output_semantic_type
            or node.entity_scope != parent.entity_scope
            or node.unit_signature != expected_unit
            or node.support_unit != parent.support_unit
        ):
            raise ValueError(
                "temporal operator output must preserve typed coordinate contracts"
            )
    if node.node_type == "PERSISTENCE":
        if len(parents) != 1 or parents[0].output_semantic_type not in (
            MASK_SEMANTIC_TYPES | {"EVENT_EPISODE", "STATE_VALUE"}
        ):
            raise ValueError("persistence requires one typed state/event input")
        if (
            node.output_semantic_type != "STATE_VALUE"
            or node.entity_scope
            not in {parents[0].entity_scope, "STOCK" if parents[0].entity_scope == "EVENT" else parents[0].entity_scope}
            or node.unit_signature != "dimensionless"
            or node.support_unit != parents[0].support_unit
        ):
            raise ValueError("persistence output contracts are incompatible")
    if node.node_type in {"STATE_AGE", "TIME_SINCE"}:
        if len(parents) != 1 or parents[0].output_semantic_type not in (
            MASK_SEMANTIC_TYPES | {"EVENT_EPISODE", "STATE_VALUE"}
        ):
            raise ValueError("age/time-since requires one typed state/event input")
        if (
            node.output_semantic_type != "STATE_VALUE"
            or node.entity_scope
            not in {parents[0].entity_scope, "STOCK" if parents[0].entity_scope == "EVENT" else parents[0].entity_scope}
            or node.unit_signature != "sessions"
            or node.support_unit != parents[0].support_unit
        ):
            raise ValueError("age/time-since output contracts are incompatible")
    if node.node_type in {"FIRST_HIT", "EVENT_AGE", "EVENT_COUNT", "EVENT_DIRECTION", "REPEATED_EVENT_SUPPRESSION"}:
        if len(parents) != 1:
            raise ValueError("unary event operator requires exactly one input")
        if node.node_type != "EVENT_DIRECTION" and parents[0].output_semantic_type not in (
            MASK_SEMANTIC_TYPES | {"EVENT_EPISODE", "STATE_VALUE"}
        ):
            raise ValueError("event operator requires typed episode/state input")
        parent = parents[0]
        compatible_entities = {
            parent.entity_scope,
            "STOCK" if parent.entity_scope == "EVENT" else parent.entity_scope,
        }
        if node.node_type in {"FIRST_HIT", "REPEATED_EVENT_SUPPRESSION"}:
            expected_mask = {
                "STOCK": "STOCK_MASK",
                "MARKET": "MARKET_MASK",
                "INDUSTRY": "GROUP_MASK",
                "PLATE": "GROUP_MASK",
            }.get(node.entity_scope)
            valid_output = (
                node.output_semantic_type == expected_mask
                and node.unit_signature == "boolean"
            )
        elif node.node_type == "EVENT_AGE":
            valid_output = (
                node.output_semantic_type == "STATE_VALUE"
                and node.unit_signature == "sessions"
            )
        elif node.node_type == "EVENT_COUNT":
            valid_output = (
                node.output_semantic_type == "STATE_VALUE"
                and node.unit_signature == "count"
            )
        else:
            valid_output = (
                node.output_semantic_type in NUMERIC_SEMANTIC_TYPES
                and node.unit_signature == "dimensionless"
            )
        if (
            not valid_output
            or node.entity_scope not in compatible_entities
            or not _support_unit_matches_entity(
                node.entity_scope, node.support_unit
            )
        ):
            raise ValueError("unary event output contracts are incompatible")
    if node.node_type in {"EVENT_WINDOW", "PRE_EVENT_PATH", "POST_EVENT_STATE"}:
        if len(parents) != 2:
            raise ValueError("event-window operator requires payload and episode inputs")
        if (
            parents[0].output_semantic_type not in NUMERIC_SEMANTIC_TYPES
            or parents[1].output_semantic_type not in (
                MASK_SEMANTIC_TYPES | {"EVENT_EPISODE", "STATE_VALUE"}
            )
        ):
            raise ValueError("event-window inputs have incompatible semantic types")
        payload = parents[0]
        if (
            node.output_semantic_type != payload.output_semantic_type
            or node.entity_scope != payload.entity_scope
            or node.unit_signature != payload.unit_signature
            or node.support_unit != payload.support_unit
        ):
            raise ValueError("event-window output must preserve payload contracts")
    if node.node_type in {"EPISODE_INTERSECT", "EPISODE_UNION"}:
        if len(parents) < 2 or not all(
            parent.output_semantic_type in (
                MASK_SEMANTIC_TYPES | {"EVENT_EPISODE"}
            )
            for parent in parents
        ):
            raise ValueError("episode set operator requires at least two inputs")
        if (
            len({parent.output_semantic_type for parent in parents}) != 1
            or len({parent.entity_scope for parent in parents}) != 1
            or len({parent.unit_signature for parent in parents}) != 1
            or len({parent.support_unit for parent in parents}) != 1
            or node.output_semantic_type != parents[0].output_semantic_type
            or node.entity_scope != parents[0].entity_scope
            or node.unit_signature != parents[0].unit_signature
            or node.support_unit != parents[0].support_unit
        ):
            raise ValueError("episode set output contracts are incompatible")
    if node.node_type in {
        "STATE",
        "BREADTH_RATIO",
        "LIMIT_DENSITY",
        "LIQUIDITY_STATE",
        "VOLATILITY_STATE",
    }:
        if len(parents) not in {1, 2} or not all(
            parent.output_semantic_type in NUMERIC_SEMANTIC_TYPES for parent in parents
        ):
            raise ValueError("state operator requires one numeric input or a numeric comparison")
        expected_mask = {
            "STOCK": "STOCK_MASK",
            "MARKET": "MARKET_MASK",
            "INDUSTRY": "GROUP_MASK",
            "PLATE": "GROUP_MASK",
        }.get(node.entity_scope)
        if (
            node.output_semantic_type != expected_mask
            or node.entity_scope != parents[0].entity_scope
            or node.unit_signature != "boolean"
        ):
            raise ValueError("state operator output contracts are incompatible")
    if node.node_type in {"TRANSITION", "STATE_PERSISTENCE"}:
        if len(parents) != 1:
            raise ValueError("transition/state-persistence requires exactly one input")
        parent = parents[0]
        if node.node_type == "TRANSITION":
            expected_mask = {
                "STOCK": "STOCK_MASK",
                "MARKET": "MARKET_MASK",
                "INDUSTRY": "GROUP_MASK",
                "PLATE": "GROUP_MASK",
            }.get(node.entity_scope)
            valid_output = (
                node.output_semantic_type == expected_mask
                and node.unit_signature == "boolean"
            )
        else:
            valid_output = (
                node.output_semantic_type == "STATE_VALUE"
                and node.unit_signature == "sessions"
            )
        if (
            not valid_output
            or node.entity_scope != parent.entity_scope
            or node.support_unit != parent.support_unit
        ):
            raise ValueError("transition/state-persistence output contracts are incompatible")
    if node.node_type in {"ADD", "SUBTRACT", "MIN", "MAX"}:
        if (
            len(parents) < 2
            or not all(
                parent.output_semantic_type in NUMERIC_SEMANTIC_TYPES
                for parent in parents
            )
            or node.output_semantic_type not in NUMERIC_SEMANTIC_TYPES
            or len({parent.unit_signature for parent in parents}) != 1
        ):
            raise ValueError("add/subtract/min/max require identical units")
        if node.unit_signature != parents[0].unit_signature:
            raise ValueError("arithmetic output unit drift")
    if node.node_type == "MULTIPLY":
        if (
            len(parents) < 2
            or not all(
                parent.output_semantic_type in NUMERIC_SEMANTIC_TYPES
                for parent in parents
            )
            or node.output_semantic_type not in NUMERIC_SEMANTIC_TYPES
            or node.entity_scope != parents[0].entity_scope
        ):
            raise ValueError("multiply requires numeric inputs")
        substantive = [
            parent.unit_signature
            for parent in parents
            if parent.unit_signature not in {"dimensionless", "boolean"}
        ]
        if substantive and node.unit_signature != "*".join(substantive):
            raise ValueError("multiply output unit drift")
    if node.node_type == "SAFE_DIVIDE":
        if (
            len(parents) != 2
            or not all(
                parent.output_semantic_type in NUMERIC_SEMANTIC_TYPES
                for parent in parents
            )
            or node.output_semantic_type not in NUMERIC_SEMANTIC_TYPES
            or node.entity_scope != parents[0].entity_scope
        ):
            raise ValueError("safe divide requires two numeric inputs")
        expected = (
            "dimensionless"
            if parents[0].unit_signature == parents[1].unit_signature
            else parents[0].unit_signature
            if parents[1].unit_signature == "dimensionless"
            else f"{parents[0].unit_signature}/{parents[1].unit_signature}"
        )
        if node.unit_signature != expected:
            raise ValueError("safe divide output unit drift")
    if node.node_type in {"GATE", "MODULATE"}:
        if (
            len(parents) != 2
            or parents[1].output_semantic_type
            not in (MASK_SEMANTIC_TYPES | {"STOCK_MULTIPLIER", "SCALAR"})
            or node.output_semantic_type != parents[0].output_semantic_type
            or node.entity_scope != parents[0].entity_scope
        ):
            raise ValueError("gate/modulate requires a typed mask or multiplier")
        if node.unit_signature != parents[0].unit_signature:
            raise ValueError("gate/modulate must preserve payload units")
    if node.node_type == "VETO" and len(parents) != 1:
        raise ValueError("veto requires exactly one mask input")
    if node.node_type in {"FILTER", "INTERSECT", "UNION"} and len(parents) < 2:
        raise ValueError("filter/set operators require at least two mask inputs")
    if node.node_type in {"FILTER", "VETO", "INTERSECT", "UNION"}:
        expected_mask_type = {
            "STOCK": "STOCK_MASK",
            "MARKET": "MARKET_MASK",
            "INDUSTRY": "GROUP_MASK",
            "PLATE": "GROUP_MASK",
        }.get(node.entity_scope)
        allowed_parent_entities = {
            "STOCK": ENTITY_SCOPES,
            "MARKET": {"MARKET", "CONSTANT"},
            "INDUSTRY": {"INDUSTRY", "MARKET", "CONSTANT"},
            "PLATE": {"PLATE", "MARKET", "CONSTANT"},
        }.get(node.entity_scope, set())
        if (
            not all(
                parent.output_semantic_type in MASK_SEMANTIC_TYPES
                for parent in parents
            )
            or node.output_semantic_type != expected_mask_type
            or not all(
                parent.entity_scope in allowed_parent_entities for parent in parents
            )
        ):
            raise ValueError("filter/veto/set operators require mask inputs")
    if node.node_type == "WITHIN_GROUP_RANK":
        if (
            len(parents) != 2
            or parents[0].entity_scope != "STOCK"
            or parents[0].output_semantic_type not in NUMERIC_SEMANTIC_TYPES
            or parents[1].entity_scope not in {"INDUSTRY", "PLATE"}
            or node.entity_scope != "STOCK"
            or node.output_semantic_type not in {"STOCK_VALUE", "STOCK_SCORE"}
        ):
            raise ValueError("within-group rank requires stock values and a PIT group key")
    if node.node_type == "CONDITIONAL_SWITCH":
        if len(parents) != 3 or parents[0].output_semantic_type not in MASK_SEMANTIC_TYPES:
            raise ValueError("conditional switch requires mask, true and false inputs")
        if (
            parents[1].output_semantic_type != parents[2].output_semantic_type
            or node.output_semantic_type != parents[1].output_semantic_type
            or node.entity_scope != parents[1].entity_scope
            or parents[1].unit_signature != parents[2].unit_signature
            or node.unit_signature != parents[1].unit_signature
        ):
            raise ValueError("conditional switch branches must preserve type and unit")
    if node.node_type in {"RESIDUALIZE", "SIZE_NEUTRALIZE"}:
        if (
            len(parents) < 2
            or parents[0].entity_scope != "STOCK"
            or parents[0].output_semantic_type not in NUMERIC_SEMANTIC_TYPES
            or node.entity_scope != "STOCK"
            or node.output_semantic_type not in {"STOCK_VALUE", "STOCK_SCORE"}
        ):
            raise ValueError("residualization requires stock payload plus controls")
        if node.unit_signature != parents[0].unit_signature:
            raise ValueError("residualization must preserve payload units")
    if node.node_type == "INDUSTRY_NEUTRALIZE":
        if (
            len(parents) != 2
            or parents[0].entity_scope != "STOCK"
            or parents[0].output_semantic_type not in NUMERIC_SEMANTIC_TYPES
            or parents[1].entity_scope != "INDUSTRY"
            or parents[1].output_semantic_type != "GROUP_VALUE"
            or node.entity_scope != "STOCK"
            or node.output_semantic_type not in {"STOCK_VALUE", "STOCK_SCORE"}
            or node.unit_signature != parents[0].unit_signature
        ):
            raise ValueError(
                "industry neutralization requires stock values and a PIT industry key"
            )


def _complexity_report(
    spec: CandidateProgramSpecV1,
    order: Sequence[str],
) -> dict[str, Any]:
    by_id = {node.node_id: node for node in spec.nodes}
    windows = {
        int(value)
        for node in spec.nodes
        for key, value in node.parameters.items()
        if "window" in str(key).lower()
        and isinstance(value, (int, float))
        and int(value) > 0
    }
    gate_depth: dict[str, int] = {}
    for node_id in order:
        node = by_id[node_id]
        gate_depth[node_id] = (
            (1 if node.node_type in {"GATE", "VETO"} else 0)
            + max((gate_depth[parent] for parent in node.input_node_ids), default=0)
        )
    report = {
        "effective_nodes": len(spec.nodes),
        "program_depth": _program_depth(spec.nodes, order),
        "stock_field_leaves": sum(node.node_type == "STOCK_FIELD" for node in spec.nodes),
        "context_field_leaves": sum(
            node.node_type in {"MARKET_FIELD", "INDUSTRY_FIELD", "PLATE_FIELD"}
            for node in spec.nodes
        ),
        "event_sources": sum(
            node.node_type in {"EVENT_EPISODE", "FROZEN_BROAD_EVENT_REF"}
            for node in spec.nodes
        ),
        "window_kinds": len(windows),
        "gate_veto_depth": max(gate_depth.values(), default=0),
        "residualize_count": sum(node.node_type == "RESIDUALIZE" for node in spec.nodes),
        "conditional_switch_count": sum(
            node.node_type == "CONDITIONAL_SWITCH" for node in spec.nodes
        ),
        "event_join_cost": sum(
            int(node.parameters.get("estimated_join_cost") or 1)
            for node in spec.nodes
            if node.node_type in EVENT_NODE_TYPES
        ),
        "estimated_cost": sum(
            int(node.parameters.get("estimated_cost") or 1) for node in spec.nodes
        ),
        "complexity_policy_hash": spec.complexity_budget.policy_hash,
    }
    limits = {
        "effective_nodes": spec.complexity_budget.maximum_effective_nodes,
        "program_depth": spec.complexity_budget.maximum_program_depth,
        "stock_field_leaves": spec.complexity_budget.maximum_stock_field_leaves,
        "context_field_leaves": spec.complexity_budget.maximum_context_field_leaves,
        "event_sources": spec.complexity_budget.maximum_event_sources,
        "window_kinds": spec.complexity_budget.maximum_window_kinds,
        "gate_veto_depth": spec.complexity_budget.maximum_gate_veto_depth,
        "residualize_count": spec.complexity_budget.maximum_residualize_count,
        "conditional_switch_count": spec.complexity_budget.maximum_conditional_switch_count,
        "event_join_cost": spec.complexity_budget.maximum_event_join_cost,
        "estimated_cost": spec.complexity_budget.maximum_estimated_cost,
    }
    exceeded = sorted(key for key, limit in limits.items() if int(report[key]) > int(limit))
    if exceeded:
        raise ValueError(f"candidate program exceeds complexity budget: {exceeded}")
    return report


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
    maturity = str(component.get("maturity_rule") or component.get("maturity_contract") or clock)
    support = str(component.get("support_unit") or "UNSPECIFIED")
    resolution = resolve_required_physical_leaves(component)
    score = TypedNodeSpec(
        node_id="legacy_score",
        node_type="LEGACY_CANDIDATE_COMPONENT",
        input_node_ids=(),
        parameters={
            "candidate": component,
            "physical_leaf_ids": list(resolution.physical_leaf_ids),
        },
        output_semantic_type="STOCK_SCORE",
        entity_scope="STOCK",
        temporal_semantics={"kind": "LEGACY_TYPED_ROUTE_EXPRESSION"},
        observable_clock=clock,
        maturity=maturity,
        unit_signature=str(component.get("unit_signature") or "dimensionless"),
        support_unit=support,
        source_lineage=tuple(resolution.logical_identity_ids),
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

    def _compile_route_bound_leaf(
        self,
        node: TypedNodeSpec,
        *,
        expected_route_id: str | None = None,
    ) -> tuple[dict[str, Any], CompileVerdict, PhysicalLeafResolution]:
        """Revalidate an adapted leaf against the existing route authority."""

        candidate = dict(node.parameters.get("candidate") or {})
        if not candidate:
            raise ValueError(f"{node.node_type} lacks its authoritative candidate binding")
        verdict = self.typed_route_compiler.compile(candidate)
        if not verdict.legal:
            raise ValueError(
                f"{node.node_type} rejected by TypedRouteCompiler: "
                f"{verdict.rejection_code}:{verdict.reason}"
            )
        if expected_route_id and verdict.route_id != expected_route_id:
            raise ValueError(
                f"{node.node_type} must bind route {expected_route_id}, got {verdict.route_id}"
            )
        expected_canonical = str(
            candidate.get("canonical_expression")
            or candidate.get("expression")
            or ""
        )
        if verdict.canonical_expression != expected_canonical:
            raise ValueError(f"{node.node_type} canonical expression drift")
        expected_exact = str(candidate.get("exact_identity") or "")
        if expected_exact and verdict.exact_identity != expected_exact:
            raise ValueError(f"{node.node_type} exact identity drift")
        expected_canonical_id = str(candidate.get("canonical_identity") or "")
        if expected_canonical_id and verdict.canonical_identity != expected_canonical_id:
            raise ValueError(f"{node.node_type} canonical identity drift")
        for key, authoritative in (
            ("field_ids", verdict.field_ids),
            ("source_field_ids", verdict.source_field_ids),
            ("representation_ids", verdict.representation_ids),
        ):
            declared = candidate.get(key)
            if declared is not None and tuple(sorted(map(str, declared))) != tuple(
                sorted(map(str, authoritative))
            ):
                raise ValueError(f"{node.node_type} {key} drift")
        resolution = resolve_required_physical_leaves(candidate)
        if tuple(sorted(map(str, node.parameters.get("physical_leaf_ids") or ()))) != tuple(
            resolution.physical_leaf_ids
        ):
            raise ValueError(f"{node.node_type} physical leaf binding drift")
        if tuple(sorted(node.source_lineage)) != tuple(resolution.logical_identity_ids):
            raise ValueError(f"{node.node_type} source lineage drift")
        if tuple(node.component_route_provenance) != (verdict.route_id,):
            raise ValueError(f"{node.node_type} route provenance drift")
        if node.support_unit != verdict.support_unit:
            raise ValueError(f"{node.node_type} support-unit drift")
        if node.maturity != verdict.maturity_rule:
            raise ValueError(f"{node.node_type} maturity drift")
        expected_clock = str(
            candidate.get("clock_contract")
            or candidate.get("maturity_contract")
            or verdict.maturity_rule
        )
        if node.observable_clock != expected_clock:
            raise ValueError(f"{node.node_type} observable-clock drift")
        return candidate, verdict, resolution

    def compile(self, spec: CandidateProgramSpecV1) -> CompiledCandidateProgramV1:
        order = _topological_order(spec.nodes)
        by_id = {node.node_id: node for node in spec.nodes}
        complexity = _complexity_report(spec, order)
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
        component_clock_requirements: dict[str, dict[str, Any]] = {}
        for node_id in spec.joint_clock_contract.component_clock_node_ids:
            clock_node = by_id[node_id]
            component_clock_requirements[node_id] = {
                "observable_clock_contract": clock_node.observable_clock,
                "maturity_contract": clock_node.maturity,
                "source_lag": clock_node.parameters.get("source_lag", 0),
                "source_lag_unit": str(
                    clock_node.parameters.get("source_lag_unit") or "sessions"
                ),
                "revision_policy": str(
                    clock_node.parameters.get("revision_policy")
                    or clock_node.temporal_semantics.get("revision_policy")
                    or "NO_FUTURE_REVISION"
                ),
            }
        field_lags: dict[str, int] = {}
        for node in spec.nodes:
            field_id = str(node.parameters.get("field_id") or "")
            source_lag_unit = str(
                node.parameters.get("source_lag_unit") or ""
            ).lower()
            source_lag = node.parameters.get("source_lag")
            if not field_id or source_lag is None or source_lag_unit not in {
                "session",
                "sessions",
            }:
                continue
            lag = int(source_lag)
            if lag < 0:
                raise ValueError("compiled field lag may not be negative")
            if field_id in field_lags and field_lags[field_id] != lag:
                raise ValueError("program field has conflicting lag contracts")
            field_lags[field_id] = lag
        frozen_nodes = [
            node for node in spec.nodes if node.node_type == "FROZEN_BROAD_EVENT_REF"
        ]
        expected_frozen_references = sorted(
            value
            for node in frozen_nodes
            for value in (
                str(node.parameters.get("field_id") or ""),
                str(node.parameters.get("frozen_inventory_hash") or ""),
            )
        )
        if sorted(spec.frozen_component_references) != expected_frozen_references:
            raise ValueError(
                "candidate frozen-component references do not exactly bind replay inventory"
            )

        verdicts: list[dict[str, Any]] = []
        legacy_candidates: list[dict[str, Any]] = []
        expressions: dict[str, str] = {}
        execution_plan: list[dict[str, Any]] = []
        physical_leaves: set[str] = set()
        external_requirements: set[str] = set()
        for node_id in order:
            node = by_id[node_id]
            parents = [by_id[parent] for parent in node.input_node_ids]
            _validate_node_semantics(node, parents, self.registry)
            parent_expressions = [expressions[parent] for parent in node.input_node_ids]
            expression = _node_expression(node, parent_expressions)
            route_leaf_binding = None
            if node.node_type == "STATE_REPRESENTATION":
                route_leaf_binding = self._compile_route_bound_leaf(
                    node,
                    expected_route_id="INTRADAY_STATE_TRANSITION",
                )
                candidate, _, _ = route_leaf_binding
                if expression != str(candidate.get("state_source_expression") or ""):
                    raise ValueError("STATE_REPRESENTATION source expression drift")
            elif node.node_type == "LEGACY_CANDIDATE_COMPONENT":
                route_leaf_binding = self._compile_route_bound_leaf(node)
            expressions[node_id] = expression
            execution_plan.append(
                {
                    "node_id": node.node_id,
                    "node_type": node.node_type,
                    "execution_phase": int(NODE_EXECUTION_PHASE[node.node_type]),
                    "input_node_ids": list(node.input_node_ids),
                    "compiled_expression": expression,
                    "output_semantic_type": node.output_semantic_type,
                    "entity_scope": node.entity_scope,
                    "observable_clock": node.observable_clock,
                    "maturity": node.maturity,
                }
            )
            if node.node_type in {
                "STOCK_FIELD",
                "MARKET_FIELD",
                "INDUSTRY_FIELD",
                "PLATE_FIELD",
                "EVENT_EPISODE",
            }:
                physical_leaves.add(str(node.parameters["field_id"]))
            elif node.node_type == "STATE_REPRESENTATION":
                assert route_leaf_binding is not None
                physical_leaves.update(route_leaf_binding[2].physical_leaf_ids)
            elif node.node_type == "FROZEN_BROAD_EVENT_REF":
                resolution = resolve_required_physical_leaves(
                    {
                        "route_id": "BROAD_EVENT_FROZEN_ENTRY",
                        "canonical_expression": expression,
                        "frozen_mechanism_id": node.parameters.get(
                            "frozen_mechanism_id"
                        ),
                        "frozen_behavior_cluster_id": node.parameters.get(
                            "frozen_behavior_cluster_id"
                        ),
                    }
                )
                external_requirements.update(
                    resolution.external_adapter_requirements
                )
            elif node.node_type == "LEGACY_CANDIDATE_COMPONENT":
                assert route_leaf_binding is not None
                resolution = route_leaf_binding[2]
                physical_leaves.update(resolution.physical_leaf_ids)
                external_requirements.update(
                    resolution.external_adapter_requirements
                )
            if node.node_type != "LEGACY_CANDIDATE_COMPONENT":
                continue
            assert route_leaf_binding is not None
            candidate, verdict, _ = route_leaf_binding
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
            output_expressions={
                key: expressions[node_id]
                for key, node_id in asdict(spec.outputs).items()
            },
            joint_clock_contract=_canonicalize(
                asdict(spec.joint_clock_contract)
            ),
            component_clock_requirements=_canonicalize(
                component_clock_requirements
            ),
            field_lags=dict(sorted(field_lags.items())),
            node_execution_plan=tuple(execution_plan),
            physical_leaf_ids=tuple(sorted(physical_leaves)),
            external_adapter_requirements=tuple(
                sorted(external_requirements)
            ),
            complexity_report=complexity,
            legacy_component_verdicts=tuple(verdicts),
            shared_dag_plan_hash=shared_plan_hash,
            compiler_semantics_version=PROGRAM_COMPILER_SEMANTICS_VERSION,
        )
