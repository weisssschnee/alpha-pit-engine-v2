"""Finite, type-preserving matched-control construction for program V1."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping

from our_system_phase2.services.candidate_program_v1 import (
    CandidateProgramSpecV1,
    MatchedControlOperationV1,
    TypedNodeSpec,
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


def _assert_type_preserving(primary: TypedNodeSpec, control: TypedNodeSpec) -> None:
    fields = (
        "output_semantic_type",
        "entity_scope",
        "observable_clock",
        "maturity",
        "unit_signature",
        "support_unit",
        "source_lineage",
        "component_route_provenance",
    )
    drift = [
        field_name
        for field_name in fields
        if getattr(primary, field_name) != getattr(control, field_name)
    ]
    if drift:
        raise ValueError(f"matched control changes protected contracts: {drift}")


def construct_matched_control_program_v1(
    primary: CandidateProgramSpecV1,
) -> ProgramMatchedPairV1:
    """Apply only preregistered, explicit, type-preserving replacements."""

    nodes = {node.node_id: node for node in primary.nodes}
    diagnostic_only = False
    for operation in primary.matched_control_plan.operations:
        diagnostic_only = diagnostic_only or operation.diagnostic_only
        for target_node_id in operation.target_node_ids:
            if target_node_id not in nodes:
                raise ValueError(f"matched-control target is absent: {target_node_id}")
            replacement = _replacement_for_operation(operation, target_node_id)
            _assert_type_preserving(nodes[target_node_id], replacement)
            nodes[target_node_id] = replacement
    control = replace(
        primary,
        nodes=tuple(nodes[node_id] for node_id in sorted(nodes)),
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
