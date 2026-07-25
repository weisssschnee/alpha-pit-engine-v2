"""Registry-driven proposal generator for the unified CN capability system."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.typed_route_compiler import TypedRouteCompiler
from our_system_phase2.services.matched_control_pairs import attach_pair_contract
from our_system_phase2.services.phase3cm_streaming_expression import (
    unsupported_streaming_operators,
)
from our_system_phase2.services.typed_primitive_gate import expression_fields
from our_system_phase2.services.unified_capability_registry import (
    CapabilityField,
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    stable_hash,
)


GENERATOR_VERSION = "cn_unified_registry_driven_generator_v1"
COMPOSITIONAL_GENERATOR_VERSION = "cn_unified_registry_driven_compositional_v2"
LEGACY_V1_PROFILE = "legacy_registry_v1"
COMPOSITIONAL_V2_PROFILE = "registry_compositional_v2"
CONSTRUCTOR_PROFILES = (LEGACY_V1_PROFILE, COMPOSITIONAL_V2_PROFILE)
DEVELOPMENT_DISCOVERY_CONTRACT_VERSION = "cn_core_pack_development_discovery_contract_v1"


@dataclass(frozen=True, slots=True)
class GeneratedPair:
    candidate: dict[str, Any]
    control: dict[str, Any]


def _pick(rows: Sequence[CapabilityField], index: int, seed: int, salt: str) -> CapabilityField:
    if not rows:
        raise ValueError(f"empty registry pool for {salt}")
    offset = int(stable_hash({"seed": int(seed), "salt": salt})[:12], 16)
    return rows[(offset + int(index) * 1009) % len(rows)]


def load_development_discovery_root_authority(
    path: Path,
    *,
    registry: UnifiedCapabilityRegistry,
) -> dict[str, Any]:
    """Load the frozen ontology root scope; it grants no execution budget."""

    source = Path(path).resolve()
    payload = json.loads(source.read_text(encoding="utf-8"))
    unsigned = {key: value for key, value in payload.items() if key != "contract_hash"}
    if payload.get("contract_hash") != stable_hash(unsigned):
        raise RuntimeError("DEVELOPMENT_DISCOVERY_CONTRACT_HASH_MISMATCH")
    if payload.get("contract_version") != DEVELOPMENT_DISCOVERY_CONTRACT_VERSION:
        raise RuntimeError("DEVELOPMENT_DISCOVERY_CONTRACT_VERSION_MISMATCH")
    if str(payload.get("registry_hash") or "") != registry.registry_hash:
        raise RuntimeError("DEVELOPMENT_DISCOVERY_REGISTRY_HASH_MISMATCH")
    allowlists = {
        str(route_id): tuple(map(str, field_ids))
        for route_id, field_ids in dict(payload.get("route_root_allowlists") or {}).items()
    }
    if set(allowlists) != set(ROUTE_IDS):
        raise RuntimeError("DEVELOPMENT_DISCOVERY_ROUTE_SCOPE_INCOMPLETE")
    # CompositionalGrammarV2 performs the definitive route/field eligibility
    # validation. Constructing it here makes malformed ontology contracts fail
    # before a campaign can consume a budget.
    CompositionalGrammarV2(
        registry,
        route_root_allowlist=allowlists,
        enforce_route_compatibility=True,
    )
    return {
        "path": str(source),
        "contract_hash": str(payload["contract_hash"]),
        "status": str(payload.get("status") or ""),
        "execution_authorized": bool(payload.get("execution_authorized")),
        "route_root_allowlists": allowlists,
        "claim_ceiling": str(payload.get("claim_ceiling") or ""),
    }


class RegistryDrivenGenerator:
    def __init__(
        self,
        registry: UnifiedCapabilityRegistry,
        *,
        constructor_profile: str = LEGACY_V1_PROFILE,
        enforce_route_compatibility: bool = True,
        route_root_allowlist: Mapping[str, Iterable[str]] | None = None,
    ) -> None:
        if constructor_profile not in CONSTRUCTOR_PROFILES:
            raise ValueError(f"unknown registry constructor profile: {constructor_profile}")
        self.registry = registry
        self.compiler = TypedRouteCompiler(registry)
        self.constructor_profile = str(constructor_profile)
        self.enforce_route_compatibility = bool(enforce_route_compatibility)
        self.route_root_allowlist = (
            None
            if route_root_allowlist is None
            else {
                str(route_id): tuple(map(str, field_ids))
                for route_id, field_ids in route_root_allowlist.items()
            }
        )
        self.generator_version = (
            COMPOSITIONAL_GENERATOR_VERSION
            if self.constructor_profile == COMPOSITIONAL_V2_PROFILE
            else GENERATOR_VERSION
        )
        self._compositional = (
            CompositionalGrammarV2(
                registry,
                route_root_allowlist=self.route_root_allowlist,
                enforce_route_compatibility=self.enforce_route_compatibility,
            )
            if self.constructor_profile == COMPOSITIONAL_V2_PROFILE
            else None
        )

    def categorical_gene_space(
        self,
        route_id: str,
        *,
        skeleton_id: str | None = None,
        formula_extension_id: str = "PRODUCTION",
    ) -> dict[str, Any]:
        if self._compositional is None:
            raise RuntimeError(
                "categorical genes require the compositional-v2 profile"
            )
        return self._compositional.categorical_gene_space(
            route_id,
            skeleton_id=skeleton_id,
            formula_extension_id=formula_extension_id,
        )

    def categorical_gene_lanes(self, route_id: str) -> dict[str, Any]:
        if self._compositional is None:
            raise RuntimeError(
                "categorical genes require the compositional-v2 profile"
            )
        return self._compositional.categorical_gene_lanes(route_id)

    def propose_categorical_genes(
        self,
        route_id: str,
        *,
        genes: Mapping[str, str],
        formula_extension_id: str = "PRODUCTION",
    ) -> GeneratedPair:
        if self._compositional is None:
            raise RuntimeError(
                "categorical genes require the compositional-v2 profile"
            )
        pair = self._compositional.propose_from_categorical_genes(
            route_id,
            genes=genes,
            formula_extension_id=formula_extension_id,
        )
        shared = {
            "generator_authority": "RegistryDrivenGenerator",
            "generator_version": self.generator_version,
            "constructor_profile": self.constructor_profile,
        }
        return GeneratedPair(
            {**pair.primary, **shared},
            {**pair.control, **shared},
        )

    def propose_attempt(
        self,
        route_id: str,
        *,
        attempt_index: int,
        seed: int,
    ) -> GeneratedPair:
        """Expose one scheduled attempt without searching past invalid supply."""

        return self._pair(route_id, int(attempt_index), int(seed))

    def _pool(self, route_id: str, predicate: Any | None = None) -> tuple[CapabilityField, ...]:
        rows = self.registry.fields_for_route(route_id)
        if predicate is not None:
            rows = tuple(row for row in rows if predicate(row))
        if not rows:
            raise ValueError(f"route has no qualified registry fields: {route_id}")
        return rows

    def _base(
        self,
        *,
        candidate_id: str,
        route_id: str,
        expression: str,
        operator_family: str,
        seed: int,
        origin: str,
        matched_control_id: str,
        declared_field_ids: Sequence[str] = (),
        condition_field_ids: Sequence[str] = (),
        is_control: bool = False,
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        row = {
            "candidate_id": candidate_id,
            "route_id": route_id,
            "expression": expression,
            "operator_family": operator_family,
            "seed": int(seed),
            "proposal_origin": origin,
            "matched_control_id": matched_control_id,
            "declared_field_ids": list(declared_field_ids),
            "condition_field_ids": list(condition_field_ids),
            "is_matched_control": bool(is_control),
            "vote_policy": "CONTROL_NO_SEPARATE_VOTE" if is_control else "ONE_SUPPORT_UNIT_ONE_VOTE",
            "maturity_contract_registered": route_id in {"DISCLOSURE_EVENT", "BROAD_EVENT_FROZEN_ENTRY"},
            "exposure_ledger_required": True,
            "access_roles": ["development"],
            "uses_future_revision": False,
            "requires_intrabar_order": False,
            "generator_version": GENERATOR_VERSION,
        }
        row.update(dict(extra or {}))
        return row

    def _pair(
        self,
        route_id: str,
        index: int,
        seed: int,
        *,
        compositional: CompositionalGrammarV2 | None = None,
    ) -> GeneratedPair:
        grammar = compositional or self._compositional
        if grammar is not None:
            pair = grammar.propose(
                route_id,
                attempt_index=int(index),
                seed=int(seed),
            )
            primary = {
                **pair.primary,
                "generator_authority": "RegistryDrivenGenerator",
                "generator_version": self.generator_version,
                "constructor_profile": self.constructor_profile,
            }
            control = {
                **pair.control,
                "generator_authority": "RegistryDrivenGenerator",
                "generator_version": self.generator_version,
                "constructor_profile": self.constructor_profile,
            }
            return GeneratedPair(primary, control)

        tag = route_id.lower()
        candidate_id = f"uc_{tag}_{seed}_{index:05d}"
        control_id = candidate_id + "_control"
        raw = self._pool(
            route_id,
            lambda row: row.source_family == "raw_1min" and row.entity_scope == "STOCK",
        ) if route_id in {"MINUTE_STATIC", "FIRSTN_PATH", "MARKET_REGIME_CONDITION", "INTRADAY_STATE_TRANSITION"} else ()

        if route_id == "MINUTE_STATIC":
            field = _pick(raw, index, seed, route_id)
            interaction = _pick(raw, index, seed, "minute_static_interaction")
            if interaction.field_id == field.field_id:
                interaction = _pick(raw, index + 1, seed, "minute_static_interaction_fallback")
            candidate_expression = (
                f"CSRank(Add(ZScore(${field.field_id}),ZScore(${interaction.field_id})))"
            )
            control_expression = f"CSRank(Sign(${field.field_id}))"
            operator = "Arithmetic"
            declared = [field.field_id, interaction.field_id]
            extra: dict[str, Any] = {}
            conditions: list[str] = []
            control_declared = [field.field_id]
            control_conditions: list[str] = []
        elif route_id == "FIRSTN_PATH":
            firstn = _pick(
                self._pool(route_id, lambda row: row.source_family == "firstN"), index, seed, "firstn"
            )
            raw_field = _pick(raw, index, seed, "firstn_raw")
            candidate_expression = f"CSRank(Add(${firstn.field_id},Sign(Delta(${raw_field.field_id},5))))"
            control_expression = (
                f"CSRank(Add(${firstn.field_id},Mul(0,Delta(${raw_field.field_id},5))))"
            )
            operator = "SignedPath"
            declared = [firstn.field_id, raw_field.field_id]
            extra = {}
            conditions = []
            control_declared = [firstn.field_id, raw_field.field_id]
            control_conditions = []
        elif route_id == "SLOW_CROSS_SECTIONAL_LEVEL":
            pool = self._pool(route_id, lambda row: row.entity_scope == "STOCK")
            field = _pick(pool, index, seed, route_id)
            candidate_expression = f"CSRank(${field.field_id})"
            control_expression = f"CSRank(Sign(${field.field_id}))"
            operator = "CSRank"
            declared = [field.field_id]
            extra = {}
            conditions = []
            control_declared = list(declared)
            control_conditions = []
        elif route_id == "SLOW_TEMPORAL_CHANGE":
            pool = self._pool(route_id, lambda row: row.entity_scope == "STOCK")
            field = _pick(pool, index, seed, route_id)
            rep_type = str((field.metadata or {}).get("canonical_representation", {}).get("representation_type", ""))
            if "slope" in rep_type:
                operator = "Slope"
            elif "acceleration" in rep_type:
                operator = "Acceleration"
            elif "persistence" in rep_type:
                operator = "Persistence"
            elif "yoy" in rep_type:
                operator = "YoY"
            else:
                operator = "Delta"
            candidate_expression = f"CSRank(${field.field_id})"
            control_expression = f"CSRank(Sign(${field.field_id}))"
            declared = [field.field_id]
            extra = {}
            conditions = []
            control_declared = list(declared)
            control_conditions = []
        elif route_id == "DISCLOSURE_EVENT":
            pool = self._pool(route_id, lambda row: row.temporal_semantics in {"EVENT_PULSE", "DISCLOSURE_PULSE"})
            field = _pick(pool, index, seed, route_id)
            candidate_expression = f"EventCount(${field.field_id},5)"
            control_expression = f"TimeSince(${field.field_id})"
            operator = "EventCount"
            declared = [field.field_id]
            extra = {"episode_policy": "UNIQUE_DISCLOSURE_EPISODE"}
            conditions = []
            control_declared = list(declared)
            control_conditions = []
        elif route_id == "MARKET_REGIME_CONDITION":
            regime = _pick(
                self._pool(route_id, lambda row: row.entity_scope == "MARKET"), index, seed, route_id
            )
            raw_field = _pick(raw, index, seed, "regime_payload")
            candidate_expression = f"CSRank(Mul(Sign(${regime.field_id}),${raw_field.field_id}))"
            control_expression = f"CSRank(${raw_field.field_id})"
            operator = "ConditionGate"
            declared = [regime.field_id, raw_field.field_id]
            extra = {"market_vote_policy": "ONE_MARKET_BLOCK_ONE_VOTE"}
            conditions = [regime.field_id]
            control_declared = [regime.field_id, raw_field.field_id]
            control_conditions = [regime.field_id]
        elif route_id == "INTRADAY_STATE_TRANSITION":
            state = _pick(
                self._pool(route_id, lambda row: row.temporal_semantics == "INTRADAY_DERIVED_STATE"),
                index,
                seed,
                route_id,
            )
            raw_field = _pick(raw, index, seed, "state_payload")
            state_expression = str((state.metadata or {})["materialization_expression"])
            candidate_expression = f"CSRank(Mul(Transition({state_expression},-1,1),Delta(${raw_field.field_id},5)))"
            control_expression = f"CSRank(Delta(${raw_field.field_id},5))"
            operator = "Transition"
            declared = [state.field_id, raw_field.field_id, *(state.metadata or {}).get("source_fields", [])]
            extra = {
                "claimed_state_field_id": state.field_id,
                "state_source_expression": state_expression,
                "state_support_unit": "symbol-state episode",
            }
            conditions = []
            control_declared = [raw_field.field_id]
            control_conditions = []
        elif route_id == "BROAD_EVENT_FROZEN_ENTRY":
            field = _pick(
                self._pool(route_id, lambda row: row.source_family == "broad_event_frozen_entry"),
                index,
                seed,
                route_id,
            )
            candidate_expression = f"FrozenMechanismReplay(${field.field_id})"
            control_expression = f"MatchedControlReplay(${field.field_id})"
            operator = "FrozenMechanismReplay"
            declared = [field.field_id]
            mechanism = dict((field.metadata or {}).get("frozen_mechanism") or {})
            extra = {
                "frozen_mechanism_id": mechanism.get("mechanism_id"),
                "event_tier": mechanism.get("priority_tier"),
                "adaptive_descendant_cap": int(mechanism.get("adaptive_descendant_cap") or 0),
                "tier_c_descendants_allowed": False,
            }
            conditions = [field.field_id] if field.entity_scope == "MARKET" else []
            control_declared = list(declared)
            control_conditions = list(conditions)
        else:
            raise KeyError(f"unsupported route: {route_id}")

        candidate = self._base(
            candidate_id=candidate_id,
            route_id=route_id,
            expression=candidate_expression,
            operator_family=operator,
            seed=seed,
            origin="registry_root",
            matched_control_id=control_id,
            declared_field_ids=declared,
            condition_field_ids=conditions,
            extra=extra,
        )
        control_operator = operator
        if route_id == "DISCLOSURE_EVENT":
            control_operator = "TimeSince"
        elif route_id == "BROAD_EVENT_FROZEN_ENTRY":
            control_operator = "MatchedControlReplay"
        control = self._base(
            candidate_id=control_id,
            route_id=route_id,
            expression=control_expression,
            operator_family=control_operator,
            seed=seed,
            origin="matched_control",
            matched_control_id=candidate_id,
            declared_field_ids=control_declared,
            condition_field_ids=control_conditions,
            is_control=True,
            extra=extra,
        )
        candidate, control = attach_pair_contract(candidate, control)
        return GeneratedPair(candidate, control)

    def generate_route_attempts(
        self,
        route_id: str,
        *,
        scheduled_pairs: int,
        seed: int,
        attempt_start: int = 0,
        attempt_limit: int | None = None,
        existing_exact_identities: set[str] | None = None,
        available_field_ids: set[str] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Consume a deterministic registry-route attempt stream.

        The attempt index, not an adaptive arm, defines the stream.  A
        feedback-on/off comparison can therefore share this exact output and
        differ only in which route budgets consume it.
        """

        if route_id not in ROUTE_IDS:
            raise KeyError(f"unknown route: {route_id}")
        if scheduled_pairs <= 0:
            raise ValueError("scheduled_pairs must be positive")
        output: list[dict[str, Any]] = []
        exact_seen: set[str] = {
            value for value in set(existing_exact_identities or ()) if not value.startswith("pair:")
        }
        pair_seen: set[str] = {
            value for value in set(existing_exact_identities or ()) if value.startswith("pair:")
        }
        start = max(0, int(attempt_start))
        index = start
        max_attempts = int(attempt_limit) if attempt_limit is not None else max(1000, scheduled_pairs * 100)
        legal_pairs = 0
        exact_unique_pairs = 0
        illegal_pairs = 0
        exact_duplicate_pairs = 0
        materialization_unsupported_pairs = 0
        materialization_missing_field_pairs = 0
        available_fields = (
            None
            if available_field_ids is None
            else set(map(str, available_field_ids))
        )
        schema_first_usable: tuple[str, ...] = ()
        schema_first_rejected = 0
        compositional = self._compositional
        route_fields = tuple(self.registry.fields_for_route(route_id))
        authorized_fields = (
            tuple(sorted(self.route_root_allowlist.get(route_id, ())))
            if self.route_root_allowlist is not None
            else tuple(field.field_id for field in route_fields)
        )
        if available_fields is not None and self.constructor_profile == COMPOSITIONAL_V2_PROFILE:
            authorized_set = set(authorized_fields)
            usable: list[str] = []
            for field in route_fields:
                if field.field_id not in authorized_set:
                    continue
                materialization_expression = str(
                    field.metadata.get("materialization_expression") or ""
                )
                physical_leaves = (
                    expression_fields(materialization_expression)
                    if materialization_expression
                    else {field.field_id}
                )
                if set(map(str, physical_leaves)).issubset(available_fields):
                    usable.append(field.field_id)
            schema_first_usable = tuple(sorted(set(usable)))
            schema_first_rejected = len(authorized_fields) - len(schema_first_usable)
            if schema_first_usable and self.route_root_allowlist is None:
                compositional = CompositionalGrammarV2(
                    self.registry,
                    route_root_allowlist={route_id: schema_first_usable},
                    enforce_route_compatibility=self.enforce_route_compatibility,
                )
            elif not schema_first_usable:
                max_attempts = 0
        skeleton_compatibility_rejects = 0
        while len(output) // 2 < scheduled_pairs and index < start + max_attempts:
            try:
                pair = self._pair(
                    route_id,
                    index,
                    seed,
                    compositional=compositional,
                )
            except ValueError as exc:
                message = str(exc)
                if (
                    "ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED" in message
                    or "FIELD_COVERAGE_BOTTLENECK" in message
                ):
                    skeleton_compatibility_rejects += 1
                    index += 1
                    continue
                raise
            if self.constructor_profile == COMPOSITIONAL_V2_PROFILE:
                unsupported = unsupported_streaming_operators(
                    (pair.candidate["expression"], pair.control["expression"])
                )
                if unsupported:
                    materialization_unsupported_pairs += 1
                    index += 1
                    continue
            if available_fields is not None:
                required_materialized_fields = {
                    str(field)
                    for member in (pair.candidate, pair.control)
                    for field in expression_fields(str(member.get("expression") or ""))
                }
                if not required_materialized_fields.issubset(available_fields):
                    materialization_missing_field_pairs += 1
                    index += 1
                    continue
            compiled_rows: list[dict[str, Any]] = []
            pair_ids: list[str] = []
            for row in (pair.candidate, pair.control):
                verdict = self.compiler.compile(row)
                enriched = {**row, **verdict.to_dict()}
                compiled_rows.append(enriched)
                if verdict.legal:
                    pair_ids.append(verdict.exact_identity)
            legal = all(bool(row["legal"]) for row in compiled_rows) and len(set(pair_ids)) == 2
            if legal:
                legal_pairs += 1
            else:
                illegal_pairs += 1
            pair_token = "pair:" + stable_hash(
                {
                    "route_id": route_id,
                    "primary_exact_identity": pair_ids[0] if pair_ids else "",
                    "control_exact_identity": pair_ids[1] if len(pair_ids) > 1 else "",
                }
            )
            # A control may intentionally be reused by distinct primaries. The
            # primary and the ordered pair are the exact-search identities;
            # all member identities are still retained so a later primary
            # cannot masquerade as a previously observed control.
            unique = legal and pair_ids[0] not in exact_seen and pair_token not in pair_seen
            if unique:
                exact_unique_pairs += 1
                for row in compiled_rows:
                    row["generation_attempt_index"] = int(index)
                    row["generation_stream_id"] = stable_hash(
                        {
                            "generator_version": self.generator_version,
                            "constructor_profile": self.constructor_profile,
                            "route_id": route_id,
                            "seed": int(seed),
                        }
                    )
                output.extend(compiled_rows)
                exact_seen.update(pair_ids)
                pair_seen.add(pair_token)
            elif legal:
                exact_duplicate_pairs += 1
            index += 1
        generated_pairs = len(output) // 2
        funnel = {
            "route_id": route_id,
            "scheduled_pairs": int(scheduled_pairs),
            "generation_attempts": int(index - start),
            "legal_pairs": int(legal_pairs),
            "exact_unique_pairs": int(exact_unique_pairs),
            "behavior_unique_pairs": 0,
            "admitted_pairs": 0,
            "illegal_pairs": int(illegal_pairs),
            "exact_duplicate_pairs": int(exact_duplicate_pairs),
            "materialization_unsupported_pairs": int(materialization_unsupported_pairs),
            "materialization_missing_field_pairs": int(
                materialization_missing_field_pairs
            ),
            "schema_first_usable_field_count": int(len(schema_first_usable)),
            "schema_first_rejected_field_count": int(schema_first_rejected),
            "route_authorized_field_count": int(len(authorized_fields)),
            "execution_compatible_authorized_field_count": int(len(schema_first_usable)),
            "root_scope_authority": (
                "FROZEN_DEVELOPMENT_DISCOVERY_CONTRACT"
                if self.route_root_allowlist is not None
                else "UNIFIED_REGISTRY_ROUTE_DEFAULT"
            ),
            "skeleton_compatibility_rejects": int(skeleton_compatibility_rejects),
            "attempt_start": int(start),
            "attempt_stop": int(index),
            "seed": int(seed),
            "top_level_scheduling_key": "unified_registry_route_id",
            "generator_authority": "RegistryDrivenGenerator",
            "constructor_profile": self.constructor_profile,
            "generator_version": self.generator_version,
            "route_compatibility_enforced": self.enforce_route_compatibility,
            "underfill_reason": "" if generated_pairs == scheduled_pairs else (
                "ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED"
                if skeleton_compatibility_rejects
                or (available_fields is not None and not schema_first_usable)
                else "GENERATION_ATTEMPT_LIMIT"
            ),
            "spillover_reason": "",
        }
        return output, funnel

    def generate_route(self, route_id: str, *, proposal_budget: int, seed: int) -> list[dict[str, Any]]:
        if proposal_budget <= 0 or proposal_budget % 2:
            raise ValueError("route proposal budget must be a positive even number including controls")
        output, funnel = self.generate_route_attempts(
            route_id,
            scheduled_pairs=proposal_budget // 2,
            seed=seed,
        )
        if len(output) != proposal_budget:
            raise RuntimeError(
                f"natural underfill after exact pre-budget dedup on {route_id}: {len(output)} != {proposal_budget}; "
                f"reason={funnel['underfill_reason']}"
            )
        return output

    def dry_generate(self, contract: Mapping[str, Any], *, seed_name: str) -> list[dict[str, Any]]:
        seeds = contract["seed_sets"][seed_name]
        budgets = contract["route_budgets"]
        rows: list[dict[str, Any]] = []
        global_exact: set[str] = set()
        for route_id in ROUTE_IDS:
            budget = int(budgets[route_id]["proposal"])
            generated = self.generate_route(route_id, proposal_budget=budget, seed=int(seeds[route_id]))
            for row in generated:
                if row["exact_identity"] in global_exact:
                    raise RuntimeError("global exact identity reached budget accounting twice")
                global_exact.add(row["exact_identity"])
                row["seed_set"] = seed_name
                rows.append(row)
        return rows
