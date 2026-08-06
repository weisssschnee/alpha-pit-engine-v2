"""Resolve physical materialization leaves without confusing typed identity.

Candidate records deliberately carry several namespaces: user-facing fields,
canonical representations, source-lineage identities and (for some routes)
logical state or frozen-mechanism handles.  Only the leaves consumed by the
existing execution adapter may be compared with a Parquet schema.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from our_system_phase2.services.real_market_validation import (
    frozen_replay_channels,
)
from our_system_phase2.services.typed_primitive_gate import expression_fields
from our_system_phase2.services.unified_capability_registry import stable_hash


INTRADAY_STATE_ROUTE = "INTRADAY_STATE_TRANSITION"
BROAD_EVENT_ROUTE = "BROAD_EVENT_FROZEN_ENTRY"


def _strings(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in (value or ()) if str(item))


def _candidate_value(candidate: Mapping[str, Any], key: str) -> Any:
    value = candidate.get(key)
    if value is not None and value != "":
        return value
    payload = candidate.get("template_payload")
    if isinstance(payload, Mapping):
        return payload.get(key)
    return None


@dataclass(frozen=True, slots=True)
class PhysicalLeafResolution:
    """Auditable separation of schema leaves from non-physical identities."""

    route_id: str
    resolution_strategy: str
    physical_leaf_ids: tuple[str, ...]
    logical_identity_ids: tuple[str, ...]
    external_adapter_requirements: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["physical_leaf_ids"] = list(self.physical_leaf_ids)
        payload["logical_identity_ids"] = list(self.logical_identity_ids)
        payload["external_adapter_requirements"] = list(
            self.external_adapter_requirements
        )
        payload["resolution_hash"] = stable_hash(payload)
        return payload


def resolve_required_physical_leaves(
    candidate: Mapping[str, Any],
) -> PhysicalLeafResolution:
    """Resolve only fields that the existing route adapter reads physically.

    The function is intentionally reward-free and data-free.  It parses typed
    candidate metadata only; it never opens a sidecar, label or price asset.
    """

    route_id = str(candidate.get("route_id") or "")
    expression = str(
        candidate.get("canonical_expression")
        or candidate.get("expression")
        or ""
    )
    expression_leaf_ids = set(expression_fields(expression)) if expression else set()
    if not expression_leaf_ids:
        expression_leaf_ids.update(
            _strings(candidate.get("field_ids") or candidate.get("declared_field_ids"))
        )

    logical_ids = set(_strings(candidate.get("source_field_ids")))
    logical_ids.update(_strings(candidate.get("representation_ids")))
    external_requirements: set[str] = set()
    strategy = "CANONICAL_EXPRESSION_PHYSICAL_LEAVES"

    if route_id == INTRADAY_STATE_ROUTE:
        claimed_state = str(_candidate_value(candidate, "claimed_state_field_id") or "")
        state_expression = str(_candidate_value(candidate, "state_source_expression") or "")
        if claimed_state:
            logical_ids.add(claimed_state)
            expression_leaf_ids.discard(claimed_state)
            if not state_expression:
                raise ValueError(
                    "intraday state identity requires state_source_expression"
                )
        if state_expression:
            expression_leaf_ids.update(expression_fields(state_expression))
        strategy = "INTRADAY_STATE_SOURCE_EXPRESSION_PHYSICAL_LEAVES"

    if route_id == BROAD_EVENT_ROUTE:
        mechanism_id = str(_candidate_value(candidate, "frozen_mechanism_id") or "")
        behavior_cluster_id = str(
            _candidate_value(candidate, "frozen_behavior_cluster_id") or ""
        )
        if not mechanism_id or not behavior_cluster_id:
            raise ValueError(
                "Broad Event candidate requires frozen mechanism and behavior cluster identities"
            )
        expected_handle = f"broad_event_{mechanism_id}"
        channels = tuple(frozen_replay_channels(expression))
        if expression_leaf_ids != {expected_handle} or len(channels) != 1:
            raise ValueError(
                "Broad Event candidate must reference exactly its frozen replay handle"
            )
        logical_ids.update((expected_handle, mechanism_id, behavior_cluster_id))
        expression_leaf_ids.clear()
        external_requirements.update(
            ("FROZEN_BROAD_EVENT_INVENTORY_BINDING", channels[0])
        )
        strategy = "FROZEN_BROAD_EVENT_EXECUTION_ADAPTER"

    return PhysicalLeafResolution(
        route_id=route_id,
        resolution_strategy=strategy,
        physical_leaf_ids=tuple(sorted(expression_leaf_ids)),
        logical_identity_ids=tuple(sorted(logical_ids)),
        external_adapter_requirements=tuple(sorted(external_requirements)),
    )
