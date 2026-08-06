"""Adapters from accepted CN component authorities into program V1 nodes."""

from __future__ import annotations

from typing import Any, Mapping

from our_system_phase2.services.candidate_materialization_requirements import (
    resolve_required_physical_leaves,
)
from our_system_phase2.services.candidate_program_v1 import TypedNodeSpec
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


def _registered_field_node(
    registry: UnifiedCapabilityRegistry,
    *,
    node_id: str,
    field_id: str,
    node_type: str,
    route_id: str,
    output_semantic_type: str,
    entity_scope: str,
    unit_signature: str,
    extra_parameters: Mapping[str, Any] | None = None,
) -> TypedNodeSpec:
    field = registry.resolve(field_id)
    if field.pit_status == "PIT_CONTRACT_UNRESOLVED":
        raise ValueError(f"field has unresolved PIT contract: {field_id}")
    if route_id not in field.allowed_routes:
        raise ValueError(f"field is not authorized for route {route_id}: {field_id}")
    parameters = {
        "field_id": field.field_id,
        "representation_id": field.representation_id,
        "source_field_id": field.source_field_id,
        "source_lag": field.source_lag,
        "source_lag_unit": field.source_lag_unit,
        "pit_status": field.pit_status,
        **dict(extra_parameters or {}),
    }
    return TypedNodeSpec(
        node_id=node_id,
        node_type=node_type,
        input_node_ids=(),
        parameters=parameters,
        output_semantic_type=output_semantic_type,
        entity_scope=entity_scope,
        temporal_semantics={
            "kind": field.temporal_semantics,
            "source_lag": field.source_lag,
            "source_lag_unit": field.source_lag_unit,
            "uses_future_revision": False,
        },
        observable_clock=field.observable_clock,
        maturity=field.maturity_rule,
        unit_signature=unit_signature,
        support_unit=field.support_unit,
        source_lineage=(field.source_field_id, field.representation_id),
        component_route_provenance=(route_id,),
    )


class FundamentalRepresentationAdapter:
    def __init__(self, registry: UnifiedCapabilityRegistry) -> None:
        self.registry = registry

    def adapt(
        self,
        *,
        node_id: str,
        field_id: str,
        route_id: str = "SLOW_CROSS_SECTIONAL_LEVEL",
        unit_signature: str = "dimensionless",
    ) -> TypedNodeSpec:
        field = self.registry.resolve(field_id)
        if field.entity_scope.upper() != "STOCK":
            raise ValueError("fundamental representation must be stock-scoped")
        adapter_id = (
            "PIT_FUNDAMENTAL_FABRIC"
            if "fund" in field.source_family.lower()
            else "UNIFIED_REGISTERED_STOCK_CONTEXT"
        )
        return _registered_field_node(
            self.registry,
            node_id=node_id,
            field_id=field_id,
            node_type="STOCK_FIELD",
            route_id=route_id,
            output_semantic_type="STOCK_VALUE",
            entity_scope="STOCK",
            unit_signature=unit_signature,
            extra_parameters={"execution_adapter_id": adapter_id},
        )


class MarketConditionAdapter:
    def __init__(self, registry: UnifiedCapabilityRegistry) -> None:
        self.registry = registry

    def adapt(
        self,
        *,
        node_id: str,
        field_id: str,
        unit_signature: str = "count",
    ) -> TypedNodeSpec:
        field = self.registry.resolve(field_id)
        if field.entity_scope.upper() != "MARKET":
            raise ValueError("market condition must be market-scoped")
        return _registered_field_node(
            self.registry,
            node_id=node_id,
            field_id=field_id,
            node_type="MARKET_FIELD",
            route_id="MARKET_REGIME_CONDITION",
            output_semantic_type="MARKET_VALUE",
            entity_scope="MARKET",
            unit_signature=unit_signature,
            extra_parameters={
                "execution_adapter_id": "EXISTING_MARKET_CONDITION_BROADCAST"
            },
        )


class DisclosureEpisodeAdapter:
    def __init__(self, registry: UnifiedCapabilityRegistry) -> None:
        self.registry = registry

    def adapt(
        self,
        *,
        node_id: str,
        field_id: str,
        observable_cutoff: str,
        registered_action_delay: str,
    ) -> TypedNodeSpec:
        return _registered_field_node(
            self.registry,
            node_id=node_id,
            field_id=field_id,
            node_type="EVENT_EPISODE",
            route_id="DISCLOSURE_EVENT",
            output_semantic_type="EVENT_EPISODE",
            entity_scope="EVENT",
            unit_signature="boolean",
            extra_parameters={
                "execution_adapter_id": "EXISTING_DISCLOSURE_EPISODE",
                "vote_policy": "ONE_EPISODE_ONE_VOTE",
                "observable_cutoff": observable_cutoff,
                "registered_action_delay": registered_action_delay,
                "latched_observation_as_lifecycle": False,
            },
        )


class IntradayStateComponentAdapter:
    def adapt(self, *, node_id: str, candidate: Mapping[str, Any]) -> TypedNodeSpec:
        resolution = resolve_required_physical_leaves(candidate)
        if resolution.route_id != "INTRADAY_STATE_TRANSITION":
            raise ValueError("intraday state adapter requires the registered route")
        expression = str(
            candidate.get("state_source_expression")
            or dict(candidate.get("template_payload") or {}).get(
                "state_source_expression"
            )
            or ""
        )
        if not expression:
            raise ValueError("intraday state component lacks real source expression")
        return TypedNodeSpec(
            node_id=node_id,
            node_type="STATE_REPRESENTATION",
            input_node_ids=(),
            parameters={
                "state_source_expression": expression,
                "physical_leaf_ids": list(resolution.physical_leaf_ids),
                "execution_adapter_id": "EXISTING_INTRADAY_STATE_EXPRESSION",
            },
            output_semantic_type="STATE_VALUE",
            entity_scope="STOCK",
            temporal_semantics={"kind": "EXISTING_INTRADAY_STATE", "uses_future_revision": False},
            observable_clock=str(candidate.get("clock_contract") or "bar_close"),
            maturity=str(candidate.get("maturity_rule") or "bar_close"),
            unit_signature="state",
            support_unit=str(candidate.get("support_unit") or "stock-minute"),
            source_lineage=tuple(resolution.logical_identity_ids),
            component_route_provenance=("INTRADAY_STATE_TRANSITION",),
        )


class FrozenBroadEventComponentAdapter:
    def __init__(self, registry: UnifiedCapabilityRegistry) -> None:
        self.registry = registry

    def adapt(
        self,
        *,
        node_id: str,
        field_id: str,
        frozen_mechanism_id: str,
        frozen_behavior_cluster_id: str,
        frozen_inventory_hash: str,
        is_matched_control: bool = False,
        output_semantic_type: str = "STOCK_MULTIPLIER",
    ) -> TypedNodeSpec:
        field = self.registry.resolve(field_id)
        if "BROAD_EVENT_FROZEN_ENTRY" not in field.allowed_routes:
            raise ValueError("field is not a frozen Broad Event reference")
        mechanism = dict((field.metadata or {}).get("frozen_mechanism") or {})
        if frozen_mechanism_id != str(mechanism.get("mechanism_id") or ""):
            raise ValueError("frozen Broad Event mechanism is not registry-authorized")
        if frozen_behavior_cluster_id != str(
            mechanism.get("behavior_cluster_id") or ""
        ):
            raise ValueError("frozen Broad Event behavior cluster is not registry-authorized")
        registered_inventory_hash = stable_hash(
            [
                row.to_dict()
                for row in self.registry.fields_for_route("BROAD_EVENT_FROZEN_ENTRY")
            ]
        )
        if frozen_inventory_hash != registered_inventory_hash:
            raise ValueError("frozen Broad Event inventory hash is not registry-authorized")
        if output_semantic_type not in {
            "STOCK_VALUE",
            "STOCK_MASK",
            "STOCK_MULTIPLIER",
        }:
            raise ValueError("frozen Broad Event output type is not an authorized stock role")
        return TypedNodeSpec(
            node_id=node_id,
            node_type="FROZEN_BROAD_EVENT_REF",
            input_node_ids=(),
            parameters={
                "field_id": field_id,
                "frozen_mechanism_id": frozen_mechanism_id,
                "frozen_behavior_cluster_id": frozen_behavior_cluster_id,
                "frozen_inventory_hash": frozen_inventory_hash,
                "is_matched_control": bool(is_matched_control),
                "discovery_budget_eligible": False,
                "dynamic_credit_eligible": False,
                "execution_adapter_id": "EXISTING_FROZEN_BROAD_EVENT_REPLAY",
            },
            output_semantic_type=output_semantic_type,
            entity_scope="STOCK",
            temporal_semantics={"kind": "FROZEN_EPISODE_REPLAY", "uses_future_revision": False},
            observable_clock=field.observable_clock,
            maturity=field.maturity_rule,
            unit_signature="dimensionless",
            support_unit=field.support_unit,
            source_lineage=(field.source_field_id, field.representation_id),
            component_route_provenance=("BROAD_EVENT_FROZEN_ENTRY",),
        )


class LegacyRouteComponentAdapter:
    def adapt(self, *, node_id: str, candidate: Mapping[str, Any]) -> TypedNodeSpec:
        route_id = str(candidate.get("route_id") or "")
        if not route_id:
            raise ValueError("legacy route component lacks route_id")
        return TypedNodeSpec(
            node_id=node_id,
            node_type="LEGACY_CANDIDATE_COMPONENT",
            input_node_ids=(),
            parameters={"candidate": dict(candidate)},
            output_semantic_type="STOCK_SCORE",
            entity_scope="STOCK",
            temporal_semantics={"kind": "LEGACY_TYPED_ROUTE_EXPRESSION", "uses_future_revision": False},
            observable_clock=str(candidate.get("clock_contract") or "UNSPECIFIED"),
            maturity=str(candidate.get("maturity_rule") or "UNSPECIFIED"),
            unit_signature=str(candidate.get("unit_signature") or "dimensionless"),
            support_unit=str(candidate.get("support_unit") or "UNSPECIFIED"),
            source_lineage=tuple(map(str, candidate.get("source_field_ids") or ())),
            component_route_provenance=(route_id,),
        )
