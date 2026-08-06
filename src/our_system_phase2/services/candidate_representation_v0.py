"""Reward-free V0 candidate representation for all unified CN templates.

The representation deliberately separates an immutable candidate definition
from proposal lineage and from every evaluation observation.  ``template_id``
is an alias of the existing unified-registry ``route_id``; it is not a second
scheduler authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    stable_hash,
)


CANDIDATE_REPRESENTATION_VERSION = "cn_candidate_representation_v0"
TEMPLATE_CONTRACT_VERSION = "cn_candidate_template_contract_v0"
BROAD_EVENT_TEMPLATE_ID = "BROAD_EVENT_FROZEN_ENTRY"


@dataclass(frozen=True, slots=True)
class TemplateContractV0:
    template_id: str
    route_id: str
    template_version: str
    sampling_kind: str
    new_generation_allowed: bool

    def __post_init__(self) -> None:
        if self.template_id != self.route_id:
            raise ValueError("template_id must remain identical to route_id")
        if self.route_id not in ROUTE_IDS:
            raise ValueError(f"unknown unified template route: {self.route_id}")


TEMPLATE_CONTRACTS_V0: dict[str, TemplateContractV0] = {
    route_id: TemplateContractV0(
        template_id=route_id,
        route_id=route_id,
        template_version=(
            f"{TEMPLATE_CONTRACT_VERSION}.{route_id.lower()}"
        ),
        sampling_kind=(
            "FROZEN_REPLAY_INVENTORY"
            if route_id == BROAD_EVENT_TEMPLATE_ID
            else "DETERMINISTIC_UNIFORM_ATTEMPT_STREAM"
        ),
        new_generation_allowed=route_id != BROAD_EVENT_TEMPLATE_ID,
    )
    for route_id in ROUTE_IDS
}


_TEMPLATE_PAYLOAD_FIELDS: dict[str, tuple[str, ...]] = {
    "MINUTE_STATIC": (),
    "FIRSTN_PATH": ("subsequent_state_transform_id",),
    "SLOW_CROSS_SECTIONAL_LEVEL": (),
    "SLOW_TEMPORAL_CHANGE": (),
    "DISCLOSURE_EVENT": ("episode_policy",),
    "MARKET_REGIME_CONDITION": (
        "market_vote_policy",
        "entity_scope",
        "cross_sectional_rank_allowed_for_condition",
    ),
    "INTRADAY_STATE_TRANSITION": (
        "claimed_state_field_id",
        "state_source_expression",
        "state_support_unit",
    ),
    BROAD_EVENT_TEMPLATE_ID: (
        "frozen_mechanism_id",
        "frozen_behavior_cluster_id",
    ),
}


_CONTRACT_FIELDS = (
    "clock_contract",
    "maturity_contract",
    "maturity_rule",
    "pair_mapping_portfolio_contract",
    "pair_support_alignment_policy",
    "pair_maturity_alignment_policy",
    "control_ablation_rule",
    "allow_behavior_equivalence",
    "exposure_ledger_required",
    "requires_intrabar_order",
    "uses_future_revision",
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


def _strings(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in (value or ()))


@dataclass(frozen=True, slots=True)
class CandidateSpecV0:
    """Immutable candidate definition plus non-semantic proposal lineage."""

    schema_version: str
    candidate_id: str
    pair_id: str
    pair_member_role: str
    matched_control_id: str
    template_id: str
    route_id: str
    template_version: str
    sampling_kind: str
    skeleton_id: str
    canonical_expression: str
    exact_identity: str
    canonical_identity: str
    is_matched_control: bool
    declared_field_ids: tuple[str, ...]
    condition_field_ids: tuple[str, ...]
    source_field_ids: tuple[str, ...]
    representation_ids: tuple[str, ...]
    operator_paths: tuple[str, ...]
    operator_family: str
    unit_signature: Any
    support_unit: str
    input_roles: Any
    contracts: Any
    template_payload: Any
    control_constructor_id: str
    generator_version: str
    attempt_id: str
    route_attempt_index: int
    seed: int
    sampler_id: str

    def __post_init__(self) -> None:
        if self.schema_version != CANDIDATE_REPRESENTATION_VERSION:
            raise ValueError("candidate representation version mismatch")
        if self.template_id != self.route_id:
            raise ValueError("candidate template_id must equal route_id")
        contract = TEMPLATE_CONTRACTS_V0.get(self.template_id)
        if contract is None:
            raise ValueError(f"unknown V0 template: {self.template_id}")
        if self.template_version != contract.template_version:
            raise ValueError("candidate template version mismatch")
        if self.sampling_kind != contract.sampling_kind:
            raise ValueError("candidate sampling kind mismatch")
        required = {
            "candidate_id": self.candidate_id,
            "pair_id": self.pair_id,
            "canonical_expression": self.canonical_expression,
            "exact_identity": self.exact_identity,
        }
        missing = sorted(key for key, value in required.items() if not value)
        if missing:
            raise ValueError(
                f"candidate representation lacks required fields: {missing}"
            )

    @classmethod
    def from_candidate_row(
        cls,
        row: Mapping[str, Any],
        *,
        attempt_id: str,
        route_attempt_index: int,
        sampler_id: str,
    ) -> "CandidateSpecV0":
        route_id = str(row.get("route_id") or "")
        contract = TEMPLATE_CONTRACTS_V0.get(route_id)
        if contract is None:
            raise ValueError(f"unknown V0 candidate route: {route_id}")
        template_payload = {
            key: _canonicalize(row.get(key))
            for key in _TEMPLATE_PAYLOAD_FIELDS[route_id]
            if row.get(key) is not None
        }
        contracts = {
            key: _canonicalize(row.get(key))
            for key in _CONTRACT_FIELDS
            if row.get(key) is not None
        }
        return cls(
            schema_version=CANDIDATE_REPRESENTATION_VERSION,
            candidate_id=str(row.get("candidate_id") or ""),
            pair_id=str(row.get("pair_id") or ""),
            pair_member_role=str(row.get("pair_member_role") or ""),
            matched_control_id=str(row.get("matched_control_id") or ""),
            template_id=route_id,
            route_id=route_id,
            template_version=contract.template_version,
            sampling_kind=contract.sampling_kind,
            skeleton_id=str(row.get("skeleton_id") or ""),
            canonical_expression=str(
                row.get("canonical_expression")
                or row.get("expression")
                or ""
            ),
            exact_identity=str(row.get("exact_identity") or ""),
            canonical_identity=str(row.get("canonical_identity") or ""),
            is_matched_control=bool(row.get("is_matched_control")),
            declared_field_ids=_strings(row.get("declared_field_ids")),
            condition_field_ids=_strings(row.get("condition_field_ids")),
            source_field_ids=_strings(row.get("source_field_ids")),
            representation_ids=_strings(row.get("representation_ids")),
            operator_paths=_strings(row.get("operator_paths")),
            operator_family=str(row.get("operator_family") or ""),
            unit_signature=_canonicalize(row.get("unit_signature")),
            support_unit=str(row.get("support_unit") or ""),
            input_roles=_canonicalize(row.get("input_roles")),
            contracts=contracts,
            template_payload=template_payload,
            control_constructor_id=str(
                row.get("control_constructor_id") or ""
            ),
            generator_version=str(row.get("generator_version") or ""),
            attempt_id=str(attempt_id),
            route_attempt_index=int(route_attempt_index),
            seed=int(row.get("seed") or 0),
            sampler_id=str(sampler_id),
        )

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> "CandidateSpecV0":
        spec = cls(
            schema_version=str(record.get("schema_version") or ""),
            candidate_id=str(record.get("candidate_id") or ""),
            pair_id=str(record.get("pair_id") or ""),
            pair_member_role=str(record.get("pair_member_role") or ""),
            matched_control_id=str(record.get("matched_control_id") or ""),
            template_id=str(record.get("template_id") or ""),
            route_id=str(record.get("route_id") or ""),
            template_version=str(record.get("template_version") or ""),
            sampling_kind=str(record.get("sampling_kind") or ""),
            skeleton_id=str(record.get("skeleton_id") or ""),
            canonical_expression=str(
                record.get("canonical_expression") or ""
            ),
            exact_identity=str(record.get("exact_identity") or ""),
            canonical_identity=str(record.get("canonical_identity") or ""),
            is_matched_control=bool(record.get("is_matched_control")),
            declared_field_ids=_strings(record.get("declared_field_ids")),
            condition_field_ids=_strings(record.get("condition_field_ids")),
            source_field_ids=_strings(record.get("source_field_ids")),
            representation_ids=_strings(record.get("representation_ids")),
            operator_paths=_strings(record.get("operator_paths")),
            operator_family=str(record.get("operator_family") or ""),
            unit_signature=_canonicalize(record.get("unit_signature")),
            support_unit=str(record.get("support_unit") or ""),
            input_roles=_canonicalize(record.get("input_roles")),
            contracts=_canonicalize(record.get("contracts") or {}),
            template_payload=_canonicalize(
                record.get("template_payload") or {}
            ),
            control_constructor_id=str(
                record.get("control_constructor_id") or ""
            ),
            generator_version=str(record.get("generator_version") or ""),
            attempt_id=str(record.get("attempt_id") or ""),
            route_attempt_index=int(record.get("route_attempt_index") or 0),
            seed=int(record.get("seed") or 0),
            sampler_id=str(record.get("sampler_id") or ""),
        )
        if str(record.get("spec_hash") or "") != spec.spec_hash:
            raise ValueError("candidate V0 spec hash mismatch")
        if str(record.get("proposal_hash") or "") != spec.proposal_hash:
            raise ValueError("candidate V0 proposal hash mismatch")
        return spec

    def semantic_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "template_id": self.template_id,
            "route_id": self.route_id,
            "template_version": self.template_version,
            "sampling_kind": self.sampling_kind,
            "pair_member_role": self.pair_member_role,
            "skeleton_id": self.skeleton_id,
            "canonical_expression": self.canonical_expression,
            "exact_identity": self.exact_identity,
            "canonical_identity": self.canonical_identity,
            "is_matched_control": self.is_matched_control,
            "declared_field_ids": list(self.declared_field_ids),
            "condition_field_ids": list(self.condition_field_ids),
            "source_field_ids": list(self.source_field_ids),
            "representation_ids": list(self.representation_ids),
            "operator_paths": list(self.operator_paths),
            "operator_family": self.operator_family,
            "unit_signature": _canonicalize(self.unit_signature),
            "support_unit": self.support_unit,
            "input_roles": _canonicalize(self.input_roles),
            "contracts": _canonicalize(self.contracts),
            "template_payload": _canonicalize(self.template_payload),
            "control_constructor_id": self.control_constructor_id,
            "generator_version": self.generator_version,
        }

    @property
    def spec_hash(self) -> str:
        return stable_hash(self.semantic_payload())

    def lineage_payload(self) -> dict[str, Any]:
        return {
            "spec_hash": self.spec_hash,
            "attempt_id": self.attempt_id,
            "route_attempt_index": self.route_attempt_index,
            "seed": self.seed,
            "sampler_id": self.sampler_id,
        }

    @property
    def proposal_hash(self) -> str:
        return stable_hash(self.lineage_payload())

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "pair_id": self.pair_id,
            "pair_member_role": self.pair_member_role,
            "matched_control_id": self.matched_control_id,
            **self.semantic_payload(),
            "attempt_id": self.attempt_id,
            "route_attempt_index": self.route_attempt_index,
            "seed": self.seed,
            "sampler_id": self.sampler_id,
            "spec_hash": self.spec_hash,
            "proposal_hash": self.proposal_hash,
        }
