"""Thin policy projection for one registry-authorized Grammar lane.

This module owns no formula, field, compiler, or matched-control semantics.
It projects the existing ``RegistryDrivenGenerator`` categorical lane into
stable decisions, records selections, and sends the selected genes back to the
same authoritative constructor.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence

import numpy as np

from our_system_phase2.services.matched_control_pairs import (
    COMPOSITIONAL_CONTROL_CONSTRUCTOR_MATRIX,
)
from our_system_phase2.services.unified_discovery_generators import (
    GeneratedPair,
    RegistryDrivenGenerator,
)


OLD_FORMULA_SPACE_ID = "OLD_PRODUCTION_SPACE"
EXPANDED_FORMULA_SPACE_ID = "EXPANDED_PRODUCTION_SPACE"
PRODUCTION_EXTENSION_ID = "PRODUCTION"
PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID = (
    "DISCLOSURE_PRE_EVENT_PAYLOAD_SIGN_V1"
)
_FORMULA_SPACES = {
    OLD_FORMULA_SPACE_ID,
    EXPANDED_FORMULA_SPACE_ID,
}


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class SearchChoice:
    token_id: str
    gene_value: str
    semantic_value: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "token_id": self.token_id,
            "gene_value": self.gene_value,
            "semantic_value": copy.deepcopy(dict(self.semantic_value)),
        }


@dataclass(frozen=True, slots=True)
class DecisionSpec:
    decision_id: str
    context_id: str
    decision_type: str
    gene_slot: str
    ordered_choices: tuple[SearchChoice, ...]

    def __post_init__(self) -> None:
        if not self.ordered_choices:
            raise ValueError(f"empty decision domain: {self.decision_id}")
        token_ids = [row.token_id for row in self.ordered_choices]
        if len(set(token_ids)) != len(token_ids):
            raise ValueError(f"duplicate decision tokens: {self.decision_id}")
        gene_values = [row.gene_value for row in self.ordered_choices]
        if len(set(gene_values)) != len(gene_values):
            raise ValueError(f"duplicate decision genes: {self.decision_id}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "context_id": self.context_id,
            "decision_type": self.decision_type,
            "gene_slot": self.gene_slot,
            "ordered_choices": [row.to_dict() for row in self.ordered_choices],
        }


@dataclass(frozen=True, slots=True)
class DecisionRecord:
    decision_id: str
    context_id: str
    decision_type: str
    selected_token_id: str

    def to_dict(self) -> dict[str, str]:
        return {
            "decision_id": self.decision_id,
            "context_id": self.context_id,
            "decision_type": self.decision_type,
            "selected_token_id": self.selected_token_id,
        }


class ChoicePolicy(Protocol):
    policy_id: str

    def choose(
        self,
        decision: DecisionSpec,
        *,
        rng: np.random.Generator,
    ) -> str:
        ...


@dataclass(frozen=True, slots=True)
class UniformPolicy:
    policy_id: str = "uniform_choice_v1"

    def choose(
        self,
        decision: DecisionSpec,
        *,
        rng: np.random.Generator,
    ) -> str:
        index = int(rng.integers(0, len(decision.ordered_choices)))
        return decision.ordered_choices[index].token_id


class LegacyParityPolicy:
    policy_id = "legacy_parity_replay_v1"

    def __init__(self, selected_tokens: Mapping[str, str]) -> None:
        self._selected = {
            str(decision_id): str(token_id)
            for decision_id, token_id in selected_tokens.items()
        }

    def choose(
        self,
        decision: DecisionSpec,
        *,
        rng: np.random.Generator,
    ) -> str:
        del rng
        try:
            return self._selected[decision.decision_id]
        except KeyError as exc:
            raise RuntimeError(
                f"LEGACY_SELECTION_MISSING:{decision.decision_id}"
            ) from exc


class TraceReplayPolicy:
    policy_id = "decision_trace_replay_v1"

    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._records = tuple(
            DecisionRecord(
                decision_id=str(row["decision_id"]),
                context_id=str(row["context_id"]),
                decision_type=str(row["decision_type"]),
                selected_token_id=str(row["selected_token_id"]),
            )
            for row in rows
        )
        self._cursor = 0

    @property
    def complete(self) -> bool:
        return self._cursor == len(self._records)

    def choose(
        self,
        decision: DecisionSpec,
        *,
        rng: np.random.Generator,
    ) -> str:
        del rng
        if self._cursor >= len(self._records):
            raise RuntimeError("DECISION_TRACE_EXHAUSTED")
        row = self._records[self._cursor]
        self._cursor += 1
        if (
            row.decision_id != decision.decision_id
            or row.context_id != decision.context_id
            or row.decision_type != decision.decision_type
        ):
            raise RuntimeError(
                "DECISION_TRACE_CONTEXT_DRIFT:"
                f"{row.decision_id}|{row.context_id}|{row.decision_type}"
            )
        return row.selected_token_id


class TargetedFormulaProjection:
    """Project exactly one existing skeleton lane plus one frozen extension."""

    def __init__(
        self,
        *,
        generator: RegistryDrivenGenerator,
        route_id: str,
        skeleton_id: str,
        extension_id: str,
    ) -> None:
        if extension_id != PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID:
            raise ValueError(f"unsupported targeted extension: {extension_id}")
        if route_id != "DISCLOSURE_EVENT" or not skeleton_id.endswith(
            ".pre_event_path"
        ):
            raise ValueError(
                "targeted projection is frozen to "
                "DISCLOSURE_EVENT/pre_event_path"
            )
        self.generator = generator
        self.route_id = str(route_id)
        self.skeleton_id = str(skeleton_id)
        self.extension_id = str(extension_id)
        self._lane = copy.deepcopy(
            generator.categorical_gene_space(
                self.route_id,
                skeleton_id=self.skeleton_id,
            )
        )
        categories = dict(self._lane["ordered_categories_by_slot"])
        if set(categories) != {
            "skeleton_id",
            "gene_surface_id",
            "event_field_id",
            "payload_field_id",
        }:
            raise RuntimeError(
                "TARGETED_LANE_DECISION_SURFACE_DRIFT:"
                + ",".join(categories)
            )
        if (
            categories["skeleton_id"] != [self.skeleton_id]
            or len(categories["gene_surface_id"]) != 1
        ):
            raise RuntimeError("TARGETED_LANE_FIXED_SLOT_DRIFT")

    def _field_choice(
        self,
        field_id: str,
        *,
        field_role: str,
    ) -> SearchChoice:
        field = self.generator.registry.resolve(str(field_id))
        return SearchChoice(
            token_id=field.representation_id,
            gene_value=field.field_id,
            semantic_value={
                "representation_id": field.representation_id,
                "field_id": field.field_id,
                "source_field_id": field.source_field_id,
                "field_role": field.field_role,
                "search_role": field_role,
                "entity_scope": field.entity_scope,
                "temporal_semantics": field.temporal_semantics,
                "observable_clock": field.observable_clock,
                "maturity_rule": field.maturity_rule,
            },
        )

    def decision_specs(
        self,
        formula_space_id: str,
    ) -> tuple[DecisionSpec, ...]:
        if formula_space_id not in _FORMULA_SPACES:
            raise ValueError(f"unknown formula space: {formula_space_id}")
        categories = dict(self._lane["ordered_categories_by_slot"])
        prefix = "disclosure_event.pre_event_path"
        base_context = (
            f"route={self.route_id}|skeleton={self.skeleton_id}|"
            f"formula_space={formula_space_id}"
        )
        rows = [
            DecisionSpec(
                decision_id=f"{prefix}.event_field",
                context_id=f"{base_context}|decision=event_field",
                decision_type="EVENT_FIELD",
                gene_slot="event_field_id",
                ordered_choices=tuple(
                    self._field_choice(field_id, field_role="event")
                    for field_id in categories["event_field_id"]
                ),
            ),
            DecisionSpec(
                decision_id=f"{prefix}.payload_field",
                context_id=f"{base_context}|decision=payload_field",
                decision_type="PAYLOAD_FIELD",
                gene_slot="payload_field_id",
                ordered_choices=tuple(
                    self._field_choice(field_id, field_role="payload")
                    for field_id in categories["payload_field_id"]
                ),
            ),
        ]
        if formula_space_id == EXPANDED_FORMULA_SPACE_ID:
            rows.append(
                DecisionSpec(
                    decision_id=f"{prefix}.extension",
                    context_id=f"{base_context}|decision=extension",
                    decision_type="EXTENSION",
                    gene_slot="extension_id",
                    ordered_choices=(
                        SearchChoice(
                            token_id="extension.PRODUCTION",
                            gene_value=PRODUCTION_EXTENSION_ID,
                            semantic_value={
                                "extension_id": PRODUCTION_EXTENSION_ID,
                                "structure": "CURRENT_CONSTRUCTOR",
                            },
                        ),
                        SearchChoice(
                            token_id=f"extension.{self.extension_id}",
                            gene_value=self.extension_id,
                            semantic_value={
                                "extension_id": self.extension_id,
                                "structure": (
                                    "ONE_LEVEL_NORMALIZER_PLACEMENT:"
                                    "Sign(payload)_inside_EventWindow"
                                ),
                            },
                        ),
                    ),
                )
            )
        return tuple(rows)

    def decision_catalog(self, formula_space_id: str) -> dict[str, Any]:
        decisions = self.decision_specs(formula_space_id)
        payload = {
            "schema_version": "cn_targeted_decision_catalog_v1",
            "authority_mode": "READ_ONLY_PROJECTION",
            "route_id": self.route_id,
            "skeleton_id": self.skeleton_id,
            "formula_space_id": formula_space_id,
            "field_authority": "UnifiedCapabilityRegistry",
            "constructor_authority": "CompositionalGrammarV2",
            "compiler_authority": "TypedRouteCompiler",
            "generator_authority": "RegistryDrivenGenerator",
            "registry_hash": self.generator.registry.registry_hash,
            "gene_surface_version": str(
                self._lane.get("surface_version") or ""
            ),
            "control_constructor_id": COMPOSITIONAL_CONTROL_CONSTRUCTOR_MATRIX[
                self.route_id
            ]["control_constructor_id"],
            "source_authority_paths": [
                "src/our_system_phase2/services/unified_capability_registry.py",
                "src/our_system_phase2/services/compositional_grammar.py",
                "src/our_system_phase2/services/typed_route_compiler.py",
                "src/our_system_phase2/services/matched_control_pairs.py",
            ],
            "extension_ids": (
                [PRODUCTION_EXTENSION_ID]
                if formula_space_id == OLD_FORMULA_SPACE_ID
                else [PRODUCTION_EXTENSION_ID, self.extension_id]
            ),
            "decisions": [row.to_dict() for row in decisions],
        }
        payload["decision_catalog_hash"] = stable_hash(payload)
        return payload

    def decision_catalog_hash(self, formula_space_id: str) -> str:
        return str(
            self.decision_catalog(formula_space_id)["decision_catalog_hash"]
        )

    def legacy_selection_from_pair(
        self,
        pair: GeneratedPair,
    ) -> dict[str, str]:
        primary = pair.candidate
        if (
            str(primary.get("route_id") or "") != self.route_id
            or str(primary.get("skeleton_id") or "") != self.skeleton_id
        ):
            raise ValueError("legacy pair is outside the frozen target lane")
        condition_ids = list(primary.get("condition_field_ids") or ())
        declared = list(primary.get("declared_field_ids") or ())
        if len(condition_ids) != 1:
            raise RuntimeError("LEGACY_EVENT_FIELD_BINDING_UNRESOLVED")
        event_id = str(condition_ids[0])
        payload_ids = [
            str(field_id)
            for field_id in declared
            if str(field_id) != event_id
        ]
        if len(payload_ids) != 1:
            raise RuntimeError("LEGACY_PAYLOAD_FIELD_BINDING_UNRESOLVED")
        event = self.generator.registry.resolve(event_id)
        payload = self.generator.registry.resolve(payload_ids[0])
        prefix = "disclosure_event.pre_event_path"
        return {
            f"{prefix}.event_field": event.representation_id,
            f"{prefix}.payload_field": payload.representation_id,
            f"{prefix}.extension": "extension.PRODUCTION",
        }

    def generate(
        self,
        *,
        formula_space_id: str,
        policy: ChoicePolicy,
        rng: np.random.Generator,
    ) -> GeneratedPair:
        decisions = self.decision_specs(formula_space_id)
        genes = {
            slot: str(values[0])
            for slot, values in dict(
                self._lane["ordered_categories_by_slot"]
            ).items()
            if slot in {"skeleton_id", "gene_surface_id"}
        }
        trace: list[DecisionRecord] = []
        extension_id = PRODUCTION_EXTENSION_ID
        for decision in decisions:
            token_id = str(policy.choose(decision, rng=rng))
            by_token = {
                row.token_id: row for row in decision.ordered_choices
            }
            if token_id not in by_token:
                raise RuntimeError(
                    "CHOICE_POLICY_RETURNED_UNKNOWN_TOKEN:"
                    f"{decision.decision_id}:{token_id}"
                )
            choice = by_token[token_id]
            if decision.gene_slot == "extension_id":
                extension_id = choice.gene_value
            else:
                genes[decision.gene_slot] = choice.gene_value
            trace.append(
                DecisionRecord(
                    decision_id=decision.decision_id,
                    context_id=decision.context_id,
                    decision_type=decision.decision_type,
                    selected_token_id=choice.token_id,
                )
            )
        if isinstance(policy, TraceReplayPolicy) and not policy.complete:
            raise RuntimeError("DECISION_TRACE_NOT_FULLY_CONSUMED")
        pair = self.generator.propose_categorical_genes(
            self.route_id,
            genes=genes,
            formula_extension_id=extension_id,
        )
        trace_rows = [row.to_dict() for row in trace]
        catalog_hash = self.decision_catalog_hash(formula_space_id)
        shared = {
            "decision_trace": trace_rows,
            "decision_catalog_hash": catalog_hash,
            "formula_space_id": formula_space_id,
            "generator_policy": str(
                getattr(policy, "policy_id", type(policy).__name__)
            ),
            "extension_id": extension_id,
            "target_family_id": self.skeleton_id,
            "decision_trace_hash": stable_hash(trace_rows),
        }
        return GeneratedPair(
            candidate={**pair.candidate, **shared},
            control={**pair.control, **shared},
        )
