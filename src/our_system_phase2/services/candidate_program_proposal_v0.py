"""Deterministic joint-program composition over existing CN route proposals.

This module is deliberately not a sampler and not a route authority.  It
validates already-produced route-local primary/control pairs, composes them
into Candidate Program V1, and emits a separate reward-free proposal receipt.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Sequence

from our_system_phase2.services.candidate_program_adapters_v1 import (
    LegacyRouteComponentAdapter,
)
from our_system_phase2.services.candidate_program_fixtures_v1 import (
    PORTFOLIO_CONTRACT_V1,
)
from our_system_phase2.services.candidate_program_v1 import (
    PROGRAM_SCHEMA_VERSION,
    CandidateProgramSpecV1,
    ComplexityBudgetV1,
    JointClockContractV1,
    MatchedControlOperationV1,
    MatchedControlPlanV1,
    ProgramOutputSpec,
    TypedNodeSpec,
    legacy_candidate_program_v1,
    route_generation_receipt_v1,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)
from our_system_phase2.services.source_route_sampling_phase_v0 import (
    SOURCE_SAMPLING_PHASES,
    source_sampling_phase_v0,
)


PROGRAM_PROPOSAL_ADAPTER_VERSION = "cn_candidate_program_proposal_adapter_v0"
PROGRAM_PROPOSAL_RECEIPT_VERSION = "cn_program_proposal_receipt_v0"
PROGRAM_JOIN_POLICY_ID = "CN_CROSS_SECTIONAL_BASE_WITH_TYPED_ENHANCERS_V0"

PROGRAM_TEMPLATE_COMPONENTS: dict[str, tuple[str, ...]] = {
    "BASE": ("base",),
    "BASE_TEMPORAL": ("base", "temporal"),
    "BASE_MARKET": ("base", "market"),
    "BASE_EVENT": ("base", "event"),
    "BASE_TEMPORAL_MARKET": ("base", "temporal", "market"),
    "BASE_TEMPORAL_EVENT": ("base", "temporal", "event"),
    "BASE_MARKET_EVENT": ("base", "market", "event"),
    "BASE_TEMPORAL_MARKET_EVENT": (
        "base",
        "temporal",
        "market",
        "event",
    ),
}

COMPONENT_ROLE_ROUTES: dict[str, frozenset[str]] = {
    "base": frozenset(
        {
            "MINUTE_STATIC",
            "FIRSTN_PATH",
            "SLOW_CROSS_SECTIONAL_LEVEL",
            "INTRADAY_STATE_TRANSITION",
        }
    ),
    "temporal": frozenset({"SLOW_TEMPORAL_CHANGE"}),
    "market": frozenset({"MARKET_REGIME_CONDITION"}),
    "event": frozenset({"DISCLOSURE_EVENT", "BROAD_EVENT_FROZEN_ENTRY"}),
}

GENERATION_ARMS = frozenset(
    {
        "UNIFORM_FRESH",
        "FACTORIZED_EXPLOIT",
        "REVISED_EXPLOIT",
        "CONDITIONAL_UPLIFT_EXPLOIT",
        "NOVELTY_RESERVE",
        "UNIFORM_CONTROL",
        "HYBRID_TPE_PROGRAM",
        "CATALOG_TYPED_EVOLUTION_PROGRAM_V2",
        "PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1",
        "SEMANTIC_STATE_JUMP_GENERATOR_V2",
        "STRUCTURED_SURROGATE_PROGRAM",
        "SUCCESSOR_PHYSICAL_DEDUP",
    }
)
TEMPORAL_COMBINATION_POLICIES = frozenset({"ADD", "SUBTRACT", "MIN", "MAX"})
MARKET_COMBINATION_POLICIES = frozenset({"GATE", "FILTER", "VETO", "MODULATE"})
EVENT_EPISODE_POLICIES = frozenset({"SOURCE_ROUTE_EPISODE"})
EVENT_APPLICATION_POLICIES = frozenset({"GATE", "FILTER"})

DEFAULT_COMBINATION_POLICY = {
    "temporal": "ADD",
    "market": "FILTER",
    "event_episode": "SOURCE_ROUTE_EPISODE",
    "event_application": "FILTER",
}


@dataclass(frozen=True, slots=True)
class ProgramSourceComponentV0:
    """One immutable source route pair plus non-semantic ask provenance."""

    role: str
    primary: Mapping[str, Any]
    control: Mapping[str, Any]
    proposal_id: str
    trial_number: int | None
    sampling_phase: str

    def __post_init__(self) -> None:
        if self.role not in COMPONENT_ROLE_ROUTES:
            raise ValueError(f"unknown joint-program component role: {self.role}")
        primary = dict(self.primary)
        control = dict(self.control)
        object.__setattr__(self, "primary", primary)
        object.__setattr__(self, "control", control)
        route_id = str(primary.get("route_id") or "")
        if route_id not in COMPONENT_ROLE_ROUTES[self.role]:
            raise ValueError(
                f"component route is not legal for {self.role}: {route_id}"
            )
        if str(control.get("route_id") or "") != route_id:
            raise ValueError("component primary/control route mismatch")
        if bool(primary.get("is_matched_control")):
            raise ValueError("component primary is marked as a matched control")
        if not bool(control.get("is_matched_control")):
            raise ValueError("component control lacks matched-control identity")
        pair_id = str(primary.get("pair_id") or "")
        if not pair_id or str(control.get("pair_id") or "") != pair_id:
            raise ValueError("component primary/control pair mismatch")
        if str(primary.get("matched_control_id") or "") != str(
            control.get("candidate_id") or ""
        ):
            raise ValueError("component matched-control identity mismatch")
        route_generation_receipt_v1(primary)
        route_generation_receipt_v1(control)
        if not self.proposal_id:
            raise ValueError("component requires its source proposal_id")
        if self.sampling_phase not in SOURCE_SAMPLING_PHASES:
            raise ValueError("component has an unknown source sampling phase")
        if self.sampling_phase != "AVAILABILITY_FIXED" and self.trial_number is None:
            raise ValueError("non-fixed component requires its source trial number")
        if self.role == "event" and route_id == "DISCLOSURE_EVENT":
            if str(primary.get("episode_policy") or "") != (
                "UNIQUE_DISCLOSURE_EPISODE"
            ):
                raise ValueError("disclosure component lacks unique-episode semantics")

    @property
    def route_id(self) -> str:
        return str(self.primary["route_id"])

    @property
    def component_id(self) -> str:
        return str(
            self.primary.get("exact_identity")
            or self.primary.get("candidate_id")
            or ""
        )

    @property
    def generation_receipt_hash(self) -> str:
        return stable_hash(
            {
                "primary": route_generation_receipt_v1(self.primary),
                "control": route_generation_receipt_v1(self.control),
            }
        )

    @property
    def credit_eligible(self) -> bool:
        return self.route_id != "BROAD_EVENT_FROZEN_ENTRY"


@dataclass(frozen=True, slots=True)
class ProgramProposalReceiptV0:
    program_template_id: str
    adapter_version: str
    join_policy_id: str
    batch_id: str
    ask_ordinal: int
    generation_arm: str
    component_candidate_ids: Mapping[str, str]
    component_control_ids: Mapping[str, str]
    component_pair_ids: Mapping[str, str]
    component_route_ids: Mapping[str, str]
    component_proposal_ids: Mapping[str, str]
    component_trial_numbers: Mapping[str, int | None]
    component_sampling_phases: Mapping[str, str]
    component_generation_receipt_hashes: Mapping[str, str]
    component_credit_eligible: Mapping[str, bool]
    combination_policy: Mapping[str, str]
    semantic_program_hash: str
    program_id: str
    schema_version: str = PROGRAM_PROPOSAL_RECEIPT_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROGRAM_PROPOSAL_RECEIPT_VERSION:
            raise ValueError("program proposal receipt version mismatch")
        roles = PROGRAM_TEMPLATE_COMPONENTS.get(self.program_template_id)
        if roles is None:
            raise ValueError("program proposal receipt has unknown template")
        if self.adapter_version != PROGRAM_PROPOSAL_ADAPTER_VERSION:
            raise ValueError("program proposal receipt adapter mismatch")
        if self.join_policy_id != PROGRAM_JOIN_POLICY_ID:
            raise ValueError("program proposal receipt join policy mismatch")
        if not self.batch_id or int(self.ask_ordinal) < 0:
            raise ValueError("program proposal receipt lacks batch/ask identity")
        if self.generation_arm not in GENERATION_ARMS:
            raise ValueError("program proposal receipt has unknown generation arm")
        component_maps = (
            self.component_candidate_ids,
            self.component_control_ids,
            self.component_pair_ids,
            self.component_route_ids,
            self.component_proposal_ids,
            self.component_trial_numbers,
            self.component_sampling_phases,
            self.component_generation_receipt_hashes,
            self.component_credit_eligible,
        )
        if any(set(mapping) != set(roles) for mapping in component_maps):
            raise ValueError("program proposal receipt component coverage drift")
        if any(
            phase not in SOURCE_SAMPLING_PHASES
            for phase in self.component_sampling_phases.values()
        ):
            raise ValueError("program proposal receipt sampling phase drift")
        if not self.semantic_program_hash or not self.program_id:
            raise ValueError("program proposal receipt lacks semantic identity")

    def to_record(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["proposal_receipt_sha256"] = stable_hash(payload)
        return payload

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> "ProgramProposalReceiptV0":
        payload = dict(record)
        expected = str(payload.pop("proposal_receipt_sha256", ""))
        if expected != stable_hash(payload):
            raise ValueError("program proposal receipt self-hash mismatch")
        receipt = cls(**payload)
        if receipt.to_record()["proposal_receipt_sha256"] != expected:
            raise ValueError("program proposal receipt canonical replay drift")
        return receipt


def _constant_node(
    node_id: str,
    *,
    value: bool | float,
    semantic_type: str,
    support_unit: str,
    entity_scope: str = "CONSTANT",
    source_lineage: Sequence[str] = (),
    provenance: Sequence[str] = (),
) -> TypedNodeSpec:
    return TypedNodeSpec(
        node_id=node_id,
        node_type="CONSTANT",
        input_node_ids=(),
        parameters={"value": value, "ablation_identity": True},
        output_semantic_type=semantic_type,
        entity_scope=entity_scope,
        temporal_semantics={"kind": "IDENTITY_OUTPUT", "uses_future_revision": False},
        observable_clock="prior_close",
        maturity="prior_close",
        unit_signature="boolean" if isinstance(value, bool) else "dimensionless",
        support_unit=support_unit,
        source_lineage=tuple(source_lineage),
        component_route_provenance=tuple(provenance),
    )


def _operator_node(
    node_id: str,
    node_type: str,
    parents: Sequence[TypedNodeSpec],
    *,
    output_semantic_type: str,
    entity_scope: str,
    unit_signature: str,
    support_unit: str,
    parameters: Mapping[str, Any] | None = None,
) -> TypedNodeSpec:
    return TypedNodeSpec(
        node_id=node_id,
        node_type=node_type,
        input_node_ids=tuple(parent.node_id for parent in parents),
        parameters=dict(parameters or {}),
        output_semantic_type=output_semantic_type,
        entity_scope=entity_scope,
        temporal_semantics={
            "kind": "DETERMINISTIC_JOINT_PROGRAM_OPERATOR_V0",
            "uses_future_revision": False,
        },
        observable_clock="prior_close",
        maturity="prior_close",
        unit_signature=unit_signature,
        support_unit=support_unit,
        source_lineage=tuple(
            sorted(
                {
                    identity
                    for parent in parents
                    for identity in parent.source_lineage
                }
            )
        ),
        component_route_provenance=tuple(
            sorted(
                {
                    route
                    for parent in parents
                    for route in parent.component_route_provenance
                }
            )
        ),
    )


def _replacement_node(
    target: TypedNodeSpec,
    *,
    node_type: str,
    input_node_ids: Sequence[str],
    value: bool | float | None = None,
    parameters: Mapping[str, Any] | None = None,
    source_lineage: Sequence[str] | None = None,
    provenance: Sequence[str] | None = None,
) -> TypedNodeSpec:
    replacement_parameters: dict[str, Any] = {
        "base_payload_replacement": True,
        **dict(parameters or {}),
    }
    if value is not None:
        replacement_parameters.update(
            {"value": value, "ablation_identity": True}
        )
    return TypedNodeSpec(
        node_id=target.node_id,
        node_type=node_type,
        input_node_ids=tuple(input_node_ids),
        parameters=replacement_parameters,
        output_semantic_type=target.output_semantic_type,
        entity_scope=target.entity_scope,
        temporal_semantics=dict(target.temporal_semantics),
        observable_clock=target.observable_clock,
        maturity=target.maturity,
        unit_signature=target.unit_signature,
        support_unit=target.support_unit,
        source_lineage=tuple(
            target.source_lineage if source_lineage is None else source_lineage
        ),
        component_route_provenance=tuple(
            target.component_route_provenance
            if provenance is None
            else provenance
        ),
    )


def _base_score_replacement(
    target: TypedNodeSpec,
    *,
    base: TypedNodeSpec,
    exposure_identity: TypedNodeSpec,
) -> TypedNodeSpec:
    if target.unit_signature == base.unit_signature:
        return _replacement_node(
            target,
            node_type="PROGRAM_SCORE_GATE",
            input_node_ids=(base.node_id, exposure_identity.node_id),
            source_lineage=base.source_lineage,
            provenance=base.component_route_provenance,
        )
    return _replacement_node(
        target,
        node_type="PROGRAM_SCORE_COMBINE",
        input_node_ids=(base.node_id, base.node_id),
        parameters={"operation": "MAX", "base_rank_identity": True},
        source_lineage=base.source_lineage,
        provenance=base.component_route_provenance,
    )


class CandidateProgramProposalAdapterV0:
    """Compose accepted route pairs; never generate components or rewards."""

    def __init__(
        self,
        registry: UnifiedCapabilityRegistry,
        *,
        portfolio_contract: Mapping[str, Any] | None = None,
    ) -> None:
        self.registry = registry
        self.portfolio_contract = dict(
            portfolio_contract or PORTFOLIO_CONTRACT_V1
        )

    @staticmethod
    def _components(
        *,
        base_component: ProgramSourceComponentV0,
        temporal_component: ProgramSourceComponentV0 | None,
        market_component: ProgramSourceComponentV0 | None,
        event_component: ProgramSourceComponentV0 | None,
    ) -> dict[str, ProgramSourceComponentV0]:
        components = {
            "base": base_component,
            **(
                {"temporal": temporal_component}
                if temporal_component is not None
                else {}
            ),
            **({"market": market_component} if market_component is not None else {}),
            **({"event": event_component} if event_component is not None else {}),
        }
        if any(component.role != role for role, component in components.items()):
            raise ValueError("program component role binding mismatch")
        return components

    @staticmethod
    def _normalize_policy(
        policy: Mapping[str, str] | None,
    ) -> dict[str, str]:
        normalized = {**DEFAULT_COMBINATION_POLICY, **dict(policy or {})}
        if normalized["temporal"] not in TEMPORAL_COMBINATION_POLICIES:
            raise ValueError("unsupported temporal combination policy")
        if normalized["market"] not in MARKET_COMBINATION_POLICIES:
            raise ValueError("unsupported market combination policy")
        if normalized["event_episode"] not in EVENT_EPISODE_POLICIES:
            raise ValueError("unsupported event episode policy")
        if normalized["event_application"] not in EVENT_APPLICATION_POLICIES:
            raise ValueError("unsupported event application policy")
        return normalized

    def compose(
        self,
        program_template_id: str,
        base_component: ProgramSourceComponentV0,
        temporal_component: ProgramSourceComponentV0 | None = None,
        market_component: ProgramSourceComponentV0 | None = None,
        event_component: ProgramSourceComponentV0 | None = None,
        combination_policy: Mapping[str, str] | None = None,
    ) -> CandidateProgramSpecV1:
        roles = PROGRAM_TEMPLATE_COMPONENTS.get(program_template_id)
        if roles is None:
            raise ValueError(f"unknown joint-program template: {program_template_id}")
        components = self._components(
            base_component=base_component,
            temporal_component=temporal_component,
            market_component=market_component,
            event_component=event_component,
        )
        if set(components) != set(roles):
            raise ValueError("joint-program template/component coverage mismatch")
        policy = self._normalize_policy(combination_policy)

        if program_template_id == "BASE":
            return legacy_candidate_program_v1(
                base_component.primary,
                portfolio_contract=self.portfolio_contract,
            )

        adapter = LegacyRouteComponentAdapter()
        component_nodes = {
            role: adapter.adapt(
                node_id=f"component_{role}", candidate=component.primary
            )
            for role, component in components.items()
        }
        base = component_nodes["base"]
        nodes: list[TypedNodeSpec] = list(component_nodes.values())
        identity_mask = _constant_node(
            "eligibility_identity",
            value=True,
            semantic_type="STOCK_MASK",
            support_unit=base.support_unit,
        )
        exposure_identity = _constant_node(
            "exposure_identity",
            value=1.0,
            semantic_type="STOCK_MULTIPLIER",
            support_unit=base.support_unit,
        )
        veto_identity = _constant_node(
            "veto_identity",
            value=False,
            semantic_type="STOCK_MASK",
            support_unit=base.support_unit,
        )
        nodes.extend((identity_mask, exposure_identity, veto_identity))

        score = base
        eligibility = identity_mask
        exposure = exposure_identity
        veto = veto_identity
        replacements: dict[str, Mapping[str, Any]] = {}

        if "temporal" in components:
            temporal = component_nodes["temporal"]
            score = _operator_node(
                "joint_score_temporal",
                "PROGRAM_SCORE_COMBINE",
                (base, temporal),
                output_semantic_type="STOCK_SCORE",
                entity_scope="STOCK",
                unit_signature="dimensionless",
                support_unit=base.support_unit,
                parameters={
                    "operation": policy["temporal"],
                    "combination_policy": policy["temporal"],
                    "rank_normalization": "CSRank_each_component",
                },
            )
            nodes.append(score)
            replacements[score.node_id] = _base_score_replacement(
                score, base=base, exposure_identity=exposure_identity
            ).semantic_payload()

        market_mask: TypedNodeSpec | None = None
        if "market" in components:
            market = component_nodes["market"]
            market_mask = _operator_node(
                "market_condition_mask",
                "PROGRAM_SCORE_TO_MASK",
                (market,),
                output_semantic_type="STOCK_MASK",
                entity_scope="STOCK",
                unit_signature="boolean",
                support_unit=base.support_unit,
                parameters={"mode": "TOP_QUANTILE", "top_fraction": 0.5},
            )
            nodes.append(market_mask)
            if policy["market"] in {"GATE", "MODULATE"}:
                prior_score = score
                score = _operator_node(
                    "joint_score_market",
                    "PROGRAM_SCORE_GATE",
                    (prior_score, market_mask),
                    output_semantic_type="STOCK_SCORE",
                    entity_scope="STOCK",
                    unit_signature=prior_score.unit_signature,
                    support_unit=base.support_unit,
                    parameters={"combination_policy": policy["market"]},
                )
                nodes.append(score)
                replacements[score.node_id] = _base_score_replacement(
                    score, base=base, exposure_identity=exposure_identity
                ).semantic_payload()
            elif policy["market"] == "FILTER":
                eligibility = _operator_node(
                    "joint_eligibility_market",
                    "PROGRAM_MASK_FILTER",
                    (identity_mask, market_mask),
                    output_semantic_type="STOCK_MASK",
                    entity_scope="STOCK",
                    unit_signature="boolean",
                    support_unit=base.support_unit,
                    parameters={"combination_policy": "FILTER"},
                )
                nodes.append(eligibility)
                replacements[eligibility.node_id] = _replacement_node(
                    eligibility,
                    node_type="CONSTANT",
                    input_node_ids=(),
                    value=True,
                    source_lineage=(),
                    provenance=(),
                ).semantic_payload()
            else:
                veto = market_mask
                replacements[veto.node_id] = _replacement_node(
                    veto,
                    node_type="CONSTANT",
                    input_node_ids=(),
                    value=False,
                    source_lineage=(),
                    provenance=(),
                ).semantic_payload()

        if "event" in components:
            event = component_nodes["event"]
            event_mask = _operator_node(
                "event_episode_mask",
                "PROGRAM_SCORE_TO_MASK",
                (event,),
                output_semantic_type="STOCK_MASK",
                entity_scope="STOCK",
                unit_signature="boolean",
                support_unit=base.support_unit,
                parameters={
                    "mode": "POSITIVE",
                    "source_episode_policy": str(
                        event_component.primary.get("episode_policy")
                        or event_component.primary.get("frozen_mechanism_id")
                        or "FROZEN_EPISODE_REPLAY"
                    ),
                    "episode_view": "SOURCE_ROUTE_EPISODE",
                    "one_episode_one_vote": True,
                },
            )
            nodes.append(event_mask)

            if policy["event_application"] == "GATE":
                prior_score = score
                score = _operator_node(
                    "joint_score_event",
                    "PROGRAM_SCORE_GATE",
                    (prior_score, event_mask),
                    output_semantic_type="STOCK_SCORE",
                    entity_scope="STOCK",
                    unit_signature=prior_score.unit_signature,
                    support_unit=base.support_unit,
                    parameters={"combination_policy": "EVENT_GATE"},
                )
                nodes.append(score)
                replacements[score.node_id] = _base_score_replacement(
                    score, base=base, exposure_identity=exposure_identity
                ).semantic_payload()
            else:
                parents = (
                    (eligibility, event_mask)
                    if eligibility is not identity_mask
                    else (identity_mask, event_mask)
                )
                eligibility = _operator_node(
                    "joint_eligibility_event",
                    "PROGRAM_MASK_FILTER",
                    parents,
                    output_semantic_type="STOCK_MASK",
                    entity_scope="STOCK",
                    unit_signature="boolean",
                    support_unit=base.support_unit,
                    parameters={"combination_policy": "EVENT_FILTER"},
                )
                nodes.append(eligibility)
                replacements[eligibility.node_id] = _replacement_node(
                    eligibility,
                    node_type="CONSTANT",
                    input_node_ids=(),
                    value=True,
                    source_lineage=(),
                    provenance=(),
                ).semantic_payload()

        if not replacements:
            raise ValueError("enhanced program lacks a concrete Base ablation")
        score = _operator_node(
            "joint_support_bound_score",
            "PROGRAM_SCORE_WITH_SUPPORT",
            (
                score,
                *(component_nodes[role] for role in roles),
            ),
            output_semantic_type="STOCK_SCORE",
            entity_scope="STOCK",
            unit_signature=score.unit_signature,
            support_unit=base.support_unit,
            parameters={
                "support_binding": "ALL_REQUIRED_COMPONENTS_NO_VALUE_EFFECT",
            },
        )
        nodes.append(score)
        operation = MatchedControlOperationV1(
            operation="BASE_PAYLOAD_ONLY",
            target_node_ids=(),
            replacement={
                "nodes": replacements,
                "base_payload_authority": base_component.component_id,
            },
        )
        legacy_provenance = tuple(
            sorted(
                str(component.primary.get("exact_identity") or "")
                for component in components.values()
            )
        )
        frozen_references = tuple(
            sorted(
                component.component_id
                for component in components.values()
                if component.route_id == "BROAD_EVENT_FROZEN_ENTRY"
            )
        )
        return CandidateProgramSpecV1(
            schema_version=PROGRAM_SCHEMA_VERSION,
            nodes=tuple(nodes),
            outputs=ProgramOutputSpec(
                stock_score_node_id=score.node_id,
                eligibility_mask_node_id=eligibility.node_id,
                exposure_multiplier_node_id=exposure.node_id,
                veto_mask_node_id=veto.node_id,
            ),
            portfolio_contract=self.portfolio_contract,
            joint_clock_contract=JointClockContractV1(
                component_clock_node_ids=tuple(
                    component_nodes[role].node_id for role in roles
                ),
                missing_component_policy="FAIL_CLOSED_REQUIRED_COMPONENT",
                action_session_policy="PRIOR_CLOSE_SIGNAL_NEXT_OPEN_ACTION",
                pit_guard_version="CN_TYPED_PROGRAM_JOINT_CLOCK_V1",
            ),
            complexity_budget=ComplexityBudgetV1(),
            matched_control_plan=MatchedControlPlanV1(
                control_constructor_id="CN_FULL_PROGRAM_VS_BASE_CONTROL_V0",
                operations=(operation,),
                pair_support_policy=(
                    "PRIMARY_CONTROL_EXACT_COORDINATE_INTERSECTION"
                ),
                pair_maturity_policy=(
                    "MAX_PRIMARY_CONTROL_MATURITY_BEFORE_SHARED_SUPPORT"
                ),
            ),
            legacy_component_provenance=legacy_provenance,
            frozen_component_references=frozen_references,
        )

    def build_receipt(
        self,
        *,
        program_template_id: str,
        program: CandidateProgramSpecV1,
        components: Sequence[ProgramSourceComponentV0],
        combination_policy: Mapping[str, str] | None,
        batch_id: str,
        ask_ordinal: int,
        generation_arm: str,
    ) -> ProgramProposalReceiptV0:
        by_role = {component.role: component for component in components}
        if len(by_role) != len(components):
            raise ValueError("duplicate component role in proposal receipt")
        expected_roles = PROGRAM_TEMPLATE_COMPONENTS.get(program_template_id)
        if expected_roles is None or set(by_role) != set(expected_roles):
            raise ValueError("proposal receipt template/component mismatch")
        policy = self._normalize_policy(combination_policy)
        receipt = ProgramProposalReceiptV0(
            program_template_id=program_template_id,
            adapter_version=PROGRAM_PROPOSAL_ADAPTER_VERSION,
            join_policy_id=PROGRAM_JOIN_POLICY_ID,
            batch_id=str(batch_id),
            ask_ordinal=int(ask_ordinal),
            generation_arm=str(generation_arm),
            component_candidate_ids={
                role: str(by_role[role].primary.get("candidate_id") or "")
                for role in expected_roles
            },
            component_control_ids={
                role: str(by_role[role].control.get("candidate_id") or "")
                for role in expected_roles
            },
            component_pair_ids={
                role: str(by_role[role].primary.get("pair_id") or "")
                for role in expected_roles
            },
            component_route_ids={
                role: by_role[role].route_id for role in expected_roles
            },
            component_proposal_ids={
                role: by_role[role].proposal_id for role in expected_roles
            },
            component_trial_numbers={
                role: by_role[role].trial_number for role in expected_roles
            },
            component_sampling_phases={
                role: by_role[role].sampling_phase for role in expected_roles
            },
            component_generation_receipt_hashes={
                role: by_role[role].generation_receipt_hash
                for role in expected_roles
            },
            component_credit_eligible={
                role: by_role[role].credit_eligible for role in expected_roles
            },
            combination_policy=policy,
            semantic_program_hash=program.semantic_program_hash,
            program_id=program.program_id,
        )
        return ProgramProposalReceiptV0.from_record(receipt.to_record())
