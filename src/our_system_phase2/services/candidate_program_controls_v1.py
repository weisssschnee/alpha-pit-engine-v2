"""Finite, type-preserving matched-control construction for program V1."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping

from our_system_phase2.services.candidate_program_v1 import (
    CandidateProgramSpecV1,
    EVENT_NODE_TYPES,
    MatchedControlOperationV1,
    NUMERIC_SEMANTIC_TYPES,
    TypedNodeSpec,
    typed_node_execution_signature_v1,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


@dataclass(frozen=True, slots=True)
class ProgramMatchedPairV1:
    primary_program_id: str
    control_program_id: str
    pair_id: str
    primary: CandidateProgramSpecV1
    control: CandidateProgramSpecV1
    diagnostic_only: bool


def _replacement_for_operation(
    operation: MatchedControlOperationV1,
    target_node_id: str,
) -> TypedNodeSpec:
    replacement = dict(operation.replacement or {})
    records = replacement.get("nodes")
    if isinstance(records, Mapping):
        record = records.get(target_node_id)
    else:
        record = replacement.get("node")
    if not isinstance(record, Mapping):
        raise ValueError(
            f"control operation {operation.operation} lacks an explicit typed replacement"
        )
    node = TypedNodeSpec.from_record(record)
    if node.node_id != target_node_id:
        raise ValueError("control replacement must preserve target node_id")
    return node


def _assert_type_preserving(
    operation: MatchedControlOperationV1,
    primary: TypedNodeSpec,
    control: TypedNodeSpec,
) -> None:
    fields = [
        "output_semantic_type",
        "entity_scope",
        "observable_clock",
        "maturity",
        "unit_signature",
        "support_unit",
    ]
    if operation.operation != "BASE_PAYLOAD_ONLY":
        fields.extend(("source_lineage", "component_route_provenance"))
    drift = [
        field_name
        for field_name in fields
        if getattr(primary, field_name) != getattr(control, field_name)
    ]
    if drift:
        raise ValueError(f"matched control changes protected contracts: {drift}")


def _operation_target_ids(
    operation: MatchedControlOperationV1,
) -> tuple[str, ...]:
    if operation.target_node_ids:
        return operation.target_node_ids
    records = dict(operation.replacement or {}).get("nodes")
    if operation.operation == "BASE_PAYLOAD_ONLY" and isinstance(records, Mapping):
        targets = tuple(sorted(map(str, records)))
        if targets:
            return targets
    raise ValueError(f"control operation {operation.operation} has no concrete targets")


def _ancestor_ids(
    nodes: Mapping[str, TypedNodeSpec], target_node_id: str
) -> set[str]:
    ancestors: set[str] = set()
    stack = list(nodes[target_node_id].input_node_ids)
    while stack:
        node_id = stack.pop()
        if node_id in ancestors:
            continue
        ancestors.add(node_id)
        stack.extend(nodes[node_id].input_node_ids)
    return ancestors


def _assert_operation_semantics(
    operation: MatchedControlOperationV1,
    primary: TypedNodeSpec,
    control: TypedNodeSpec,
    nodes: Mapping[str, TypedNodeSpec],
) -> None:
    if operation.operation == "REMOVE_GATE" and primary.node_type != "GATE":
        raise ValueError("REMOVE_GATE must target a GATE node")
    if operation.operation == "REMOVE_VETO" and primary.node_type != "VETO":
        raise ValueError("REMOVE_VETO must target a VETO node")
    if operation.operation == "REMOVE_EVENT_TRIGGER" and primary.node_type not in (
        EVENT_NODE_TYPES | {"EVENT_EPISODE", "FROZEN_BROAD_EVENT_REF"}
    ):
        raise ValueError("REMOVE_EVENT_TRIGGER must target an event node")
    if operation.operation == "REPLACE_WITH_LEVEL" and (
        primary.output_semantic_type not in NUMERIC_SEMANTIC_TYPES
        or control.output_semantic_type not in NUMERIC_SEMANTIC_TYPES
    ):
        raise ValueError("REPLACE_WITH_LEVEL requires a numeric target")
    if operation.operation == "ABLATE_SUBGRAPH":
        forbidden = _ancestor_ids(nodes, primary.node_id) | {primary.node_id}
        if set(control.input_node_ids) & forbidden:
            raise ValueError(
                "ABLATE_SUBGRAPH replacement may not retain the ablated subgraph"
            )
        if (
            control.node_type != "CONSTANT"
            or control.input_node_ids
            or not bool(control.parameters.get("ablation_identity"))
        ):
            raise ValueError(
                "ABLATE_SUBGRAPH requires an explicit typed identity constant"
            )
    if operation.operation == "ABLATE_NODE" and (
        control.node_type != "CONSTANT"
        or control.input_node_ids
        or not bool(control.parameters.get("ablation_identity"))
    ):
        raise ValueError("ABLATE_NODE requires an explicit typed identity constant")
    if operation.operation == "REPLACE_WITH_PLACEBO" and (
        not str(control.parameters.get("placebo_authority") or "")
        or not str(control.parameters.get("placebo_id") or "")
        or not bool(control.parameters.get("deterministic_placebo"))
    ):
        raise ValueError(
            "REPLACE_WITH_PLACEBO requires a deterministic placebo authority"
        )
    if operation.operation == "REPLACE_WITH_PLACEBO" and (
        typed_node_execution_signature_v1(primary)
        == typed_node_execution_signature_v1(control)
    ):
        raise ValueError(
            "REPLACE_WITH_PLACEBO must change compiled execution semantics"
        )
    if operation.operation == "REPLACE_WITH_WRONG_LAG_CONTROL":
        wrong_lag = control.parameters.get("wrong_lag_sessions")
        if (
            not operation.diagnostic_only
            or not isinstance(wrong_lag, int)
            or isinstance(wrong_lag, bool)
            or wrong_lag <= 0
            or control.node_type != "LAG"
            or int(control.parameters.get("window") or 0) != wrong_lag
        ):
            raise ValueError(
                "wrong-lag control requires an explicit positive diagnostic LAG"
            )
        if (
            typed_node_execution_signature_v1(primary)
            == typed_node_execution_signature_v1(control)
        ):
            raise ValueError(
                "wrong-lag control must change compiled execution semantics"
            )
    if operation.operation == "BASE_PAYLOAD_ONLY" and not bool(
        operation.replacement.get("base_payload_authority")
    ):
        raise ValueError("BASE_PAYLOAD_ONLY requires an explicit base payload authority")


def _reachable_node_ids(
    nodes: Mapping[str, TypedNodeSpec], primary: CandidateProgramSpecV1
) -> set[str]:
    reachable: set[str] = set()
    stack = list(asdict(primary.outputs).values()) + list(
        primary.joint_clock_contract.component_clock_node_ids
    )
    while stack:
        node_id = str(stack.pop())
        if node_id in reachable:
            continue
        if node_id not in nodes:
            raise ValueError(f"matched control removed required output node: {node_id}")
        reachable.add(node_id)
        stack.extend(nodes[node_id].input_node_ids)
    return reachable


def construct_matched_control_program_v1(
    primary: CandidateProgramSpecV1,
) -> ProgramMatchedPairV1:
    """Apply only preregistered, explicit, type-preserving replacements."""

    nodes = {node.node_id: node for node in primary.nodes}
    diagnostic_only = False
    prune_unreachable = False
    for operation in primary.matched_control_plan.operations:
        diagnostic_only = diagnostic_only or operation.diagnostic_only
        prune_unreachable = prune_unreachable or operation.operation in {
            "ABLATE_SUBGRAPH",
            "BASE_PAYLOAD_ONLY",
        }
        for target_node_id in _operation_target_ids(operation):
            if target_node_id not in nodes:
                raise ValueError(f"matched-control target is absent: {target_node_id}")
            replacement = _replacement_for_operation(operation, target_node_id)
            _assert_type_preserving(
                operation, nodes[target_node_id], replacement
            )
            _assert_operation_semantics(
                operation, nodes[target_node_id], replacement, nodes
            )
            nodes[target_node_id] = replacement
    selected_node_ids = (
        _reachable_node_ids(nodes, primary) if prune_unreachable else set(nodes)
    )
    control = replace(
        primary,
        nodes=tuple(nodes[node_id] for node_id in sorted(selected_node_ids)),
    )
    if control.semantic_program_hash == primary.semantic_program_hash:
        raise ValueError("matched-control construction did not change program semantics")
    pair_id = "cn.program_pair." + stable_hash(
        {
            "primary_program_hash": primary.semantic_program_hash,
            "control_program_hash": control.semantic_program_hash,
            "control_constructor_id": primary.matched_control_plan.control_constructor_id,
            "pair_support_policy": primary.matched_control_plan.pair_support_policy,
            "pair_maturity_policy": primary.matched_control_plan.pair_maturity_policy,
            "diagnostic_only": diagnostic_only,
        }
    )[:32]
    return ProgramMatchedPairV1(
        primary_program_id=primary.program_id,
        control_program_id=control.program_id,
        pair_id=pair_id,
        primary=primary,
        control=control,
        diagnostic_only=diagnostic_only,
    )
