"""Typed, route-aware compositional grammar for bounded CN research.

The grammar is a proposal authority only.  Registry, compiler, receipt and
pair-native evaluator authorities remain separate and fail closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from our_system_phase2.services.matched_control_pairs import (
    COMPOSITIONAL_CONTROL_CONSTRUCTOR_MATRIX,
    attach_pair_contract,
)
from our_system_phase2.services.typed_route_compiler import (
    SUPPLEMENTAL_ROOT_SCOPE_ID,
    TypedRouteCompiler,
)
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    CapabilityField,
    UnifiedCapabilityRegistry,
    stable_hash,
)


GRAMMAR_VERSION = "cn_typed_compositional_grammar_v2"
SUPPLEMENTAL_GRAMMAR_VERSION = "cn_typed_compositional_supplemental_v1"


@dataclass(frozen=True, slots=True)
class SkeletonSpec:
    skeleton_id: str
    route_id: str
    financial_hypothesis: str
    input_roles: tuple[str, ...]
    unit_signature: str
    clock_contract: str
    maturity_contract: str
    control_ablation_rule: str
    maximum_depth: int
    allowed_routes: tuple[str, ...]
    search_role: str = "PRIMARY_SEARCH"


@dataclass(frozen=True, slots=True)
class GeneratedCompositionalPair:
    skeleton: SkeletonSpec
    primary: dict[str, Any]
    control: dict[str, Any]


_ROUTE_CLOCKS = {
    "MINUTE_STATIC": ("bar_close", "bar_close"),
    "FIRSTN_PATH": ("bar_close_and_firstN_clock", "firstN end timestamp"),
    "SLOW_CROSS_SECTIONAL_LEVEL": (
        "latest_PIT_observable_version_before_session",
        "session_open_after_observable_version",
    ),
    "SLOW_TEMPORAL_CHANGE": (
        "all_source_versions_observable_before_action",
        "latest_required_version maturity",
    ),
    "DISCLOSURE_EVENT": (
        "episode_observable_time",
        "episode maturity plus registered action delay",
    ),
    "MARKET_REGIME_CONDITION": (
        "market_state_observable_at_or_before_payload",
        "joint payload and regime maturity",
    ),
    "INTRADAY_STATE_TRANSITION": (
        "conservative_bar_observable_transition",
        "transition bar close",
    ),
    "BROAD_EVENT_FROZEN_ENTRY": (
        "frozen_mechanism_observable_clock",
        "registered frozen episode maturity",
    ),
}


_DECLARATIONS: dict[str, tuple[tuple[str, str, tuple[str, ...], str, str, int], ...]] = {
    "MINUTE_STATIC": (
        ("normalized_level", "cross-sectional normalization exposes relative minute state", ("PRIMARY",), "dimensionless", "magnitude_to_sign", 3),
        ("field_spread", "normalized field spread captures relative dislocation", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-minus-dimensionless", "remove_secondary_spread", 4),
        ("normalized_ratio", "relative normalized intensity captures imbalance", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-ratio", "remove_ratio_denominator", 4),
        ("price_volume_interaction", "price and volume state interact nonlinearly", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-product", "remove_interaction", 4),
        ("liquidity_volatility_interaction", "liquidity conditions modulate volatility state", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-product", "remove_interaction", 4),
        ("cross_sectional_residual", "one minute field has residual information beyond another", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-residual", "remove_residualizer", 4),
        ("absolute_state_interaction", "magnitude and signed state jointly matter", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-product", "remove_magnitude_leg", 4),
        ("dispersion_interaction", "cross-sectional dispersion changes the meaning of level", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-product", "remove_dispersion_leg", 4),
    ),
    "FIRSTN_PATH": (
        ("firstn_path_state", "opening path and subsequent state interact", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-product", "remove_subsequent_path", 4),
        ("opening_path_liquidity", "opening path depends on intraday liquidity", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-product", "remove_liquidity_leg", 4),
        ("opening_imbalance_persistence", "opening imbalance persists through the early session", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-product", "remove_persistence_leg", 4),
        ("relative_strength_reversal", "firstN relative strength predicts post-open reversal", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-spread", "remove_reversal_leg", 4),
        ("opening_path_acceleration", "opening path acceleration contains information beyond level", ("PRIMARY",), "dimensionless", "replace_acceleration_with_level", 4),
        ("opening_multiscale_relation", "short and long opening paths disagree", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-relation", "remove_multiscale_leg", 4),
        ("opening_state_residual", "opening state has residual information beyond raw path", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-residual", "remove_residualizer", 4),
        ("conditioned_opening_path", "opening path sign changes subsequent path interpretation", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-gated", "remove_path_sign_gate", 4),
    ),
    "SLOW_CROSS_SECTIONAL_LEVEL": (
        ("fundamental_level", "PIT fundamental level differentiates firms", ("PRIMARY",), "nominal-source-unit", "magnitude_to_sign", 3),
        ("fundamental_ratio", "two normalized fundamental levels form a relative ratio", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-ratio", "remove_ratio_denominator", 4),
        ("fundamental_cap_condition", "size changes the interpretation of a fundamental level", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-gated", "remove_size_interaction", 4),
        ("fundamental_residual", "fundamental information remains after cross-sectional residualization", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-residual", "remove_residualizer", 4),
        ("cross_family_interaction", "independent fundamental families interact", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-product", "remove_interaction", 4),
        ("winsorized_level", "robust level information survives tail control", ("PRIMARY",), "nominal-source-unit", "winsorized_to_sign", 3),
        ("masked_normalized_level", "coverage-qualified normalized level carries information", ("PRIMARY",), "dimensionless", "masked_level_to_sign", 3),
        ("size_residual_level", "fundamental level has information orthogonal to size", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-residual", "remove_size_residualizer", 4),
    ),
    "SLOW_TEMPORAL_CHANGE": (
        ("delta", "newly observable level change carries information", ("PRIMARY",), "source-unit-change", "change_to_level", 3),
        ("slope", "persistent direction of disclosed change matters", ("PRIMARY",), "source-unit-per-session", "slope_to_level", 3),
        ("acceleration", "acceleration of disclosed change carries incremental information", ("PRIMARY",), "source-unit-per-session2", "acceleration_to_slope", 4),
        ("reported_change", "reported YoY QoQ or TTM change is informative", ("PRIMARY",), "reported-change-unit", "change_to_sign", 3),
        ("change_level_interaction", "change has different meaning conditional on level", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-product", "remove_level_leg", 4),
        ("short_long_change", "short and long change horizons diverge", ("PRIMARY",), "dimensionless-spread", "remove_short_long_spread", 4),
        ("change_persistence", "persistent change differs from a one-off move", ("PRIMARY",), "source-unit", "persistence_to_level", 3),
        ("cross_change_interaction", "changes from independent families interact", ("PRIMARY", "INTERACTION_ONLY"), "dimensionless-product", "remove_interaction", 4),
    ),
    "DISCLOSURE_EVENT": (
        ("event_pulse", "new disclosure pulse has immediate information", ("PRIMARY",), "event-indicator", "pulse_to_recency", 2),
        ("event_age", "information decays with disclosure age", ("PRIMARY",), "episode-age", "age_to_pulse", 2),
        ("pre_event_path", "pre-event path changes disclosure interpretation", ("PRIMARY",), "dimensionless-path", "remove_pre_event_path", 3),
        ("post_maturity_state", "post-maturity state distinguishes continuation", ("PRIMARY",), "dimensionless-state", "state_to_recency", 3),
        ("event_prior_condition", "prior PIT condition changes event impact", ("PRIMARY", "CONDITION_ONLY"), "dimensionless-gated", "remove_condition_gate", 4),
        ("repeated_event_suppression", "repeated events contain less incremental information", ("PRIMARY",), "event-count", "count_to_first_event", 3),
        ("event_window", "mature event window summarizes episode response", ("PRIMARY",), "dimensionless-window", "window_to_recency", 3),
        ("event_first_hit", "first event timing differs from repeated occurrence", ("PRIMARY",), "episode-position", "first_hit_to_recency", 3),
    ),
    "MARKET_REGIME_CONDITION": (
        ("payload_regime_interaction", "stock payload behaves differently by market regime", ("PRIMARY", "CONDITION_ONLY"), "dimensionless-gated", "remove_regime_gate", 4),
        ("payload_regime_residual", "stock payload residual differs under regime", ("PRIMARY", "CONDITION_ONLY"), "dimensionless-residual", "remove_regime_residualizer", 4),
        ("regime_transition_response", "stock response changes at regime transition", ("PRIMARY", "CONDITION_ONLY"), "dimensionless-gated", "remove_transition_gate", 4),
        ("regime_maturity_payload", "mature regime state modulates stock payload", ("PRIMARY", "CONDITION_ONLY"), "dimensionless-gated", "remove_maturity_gate", 4),
        ("regime_age_response", "stock payload varies with regime age", ("PRIMARY", "CONDITION_ONLY"), "dimensionless-product", "remove_regime_age", 4),
        ("regime_multiscale_response", "short and long payload relation changes by regime", ("PRIMARY", "CONDITION_ONLY"), "dimensionless-relation", "remove_multiscale_leg", 4),
        ("regime_conditioned_path", "regime conditions the recent stock path", ("PRIMARY", "CONDITION_ONLY"), "dimensionless-gated", "remove_condition_gate", 4),
        ("regime_persistence_gate", "persistent regime differs from transient state", ("PRIMARY", "CONDITION_ONLY"), "dimensionless-gated", "remove_persistence_gate", 4),
    ),
    "INTRADAY_STATE_TRANSITION": (
        ("transition_payload", "state transition changes payload direction", ("PRIMARY", "STATE_ONLY"), "dimensionless-gated", "remove_transition_gate", 4),
        ("transition_persistence", "persistent transition differs from a transient one", ("PRIMARY", "STATE_ONLY"), "dimensionless-gated", "remove_persistence_leg", 4),
        ("transition_state_age", "state age modulates transition response", ("PRIMARY", "STATE_ONLY"), "dimensionless-product", "remove_state_age", 4),
        ("transition_liquidity_condition", "liquidity conditions transition response", ("PRIMARY", "STATE_ONLY", "CONDITION_ONLY"), "dimensionless-gated", "remove_liquidity_gate", 4),
        ("state_residual", "payload retains information orthogonal to state", ("PRIMARY", "STATE_ONLY"), "dimensionless-residual", "remove_state_residualizer", 4),
        ("conditioned_state_path", "state conditions the subsequent intraday path", ("PRIMARY", "STATE_ONLY"), "dimensionless-gated", "remove_state_gate", 4),
        ("duration_payload", "state duration changes payload interpretation", ("PRIMARY", "STATE_ONLY"), "dimensionless-product", "remove_duration_leg", 4),
        ("multiscale_state_response", "short and long state responses diverge", ("PRIMARY", "STATE_ONLY"), "dimensionless-relation", "remove_multiscale_leg", 4),
    ),
    "BROAD_EVENT_FROZEN_ENTRY": (
        ("frozen_mechanism_reference", "frozen Broad Event mechanism is replayed without structural expansion", ("CONDITION_ONLY",), "registered-frozen-signal", "registered_matched_control_replay", 2),
    ),
}


def skeleton_registry() -> dict[str, tuple[SkeletonSpec, ...]]:
    output: dict[str, tuple[SkeletonSpec, ...]] = {}
    for route_id in ROUTE_IDS:
        clock, maturity = _ROUTE_CLOCKS[route_id]
        search_role = "FROZEN_REFERENCE_ONLY" if route_id == "BROAD_EVENT_FROZEN_ENTRY" else "PRIMARY_SEARCH"
        output[route_id] = tuple(
            SkeletonSpec(
                skeleton_id=f"cn.comp.v2.{route_id.lower()}.{name}",
                route_id=route_id,
                financial_hypothesis=hypothesis,
                input_roles=roles,
                unit_signature=unit_signature,
                clock_contract=clock,
                maturity_contract=maturity,
                control_ablation_rule=ablation,
                maximum_depth=maximum_depth,
                allowed_routes=(route_id,),
                search_role=search_role,
            )
            for name, hypothesis, roles, unit_signature, ablation, maximum_depth in _DECLARATIONS[route_id]
        )
    return output


def supplemental_skeleton_registry() -> dict[str, tuple[SkeletonSpec, ...]]:
    """Return append-only constructors for registry gaps found after the base pack.

    These skeletons are deliberately outside :func:`skeleton_registry` so adding
    them cannot change the attempt-to-skeleton mapping or identities of an
    already frozen base proposal pack.  They are generated only through
    ``propose_supplemental`` and must pass global exact dedup before admission.
    """

    declarations = {
        "SLOW_CROSS_SECTIONAL_LEVEL": (
            (
                "disclosure_age_condition",
                "PIT disclosure staleness conditions the comparable fundamental level",
                ("PRIMARY", "CONDITION_ONLY"),
                "dimensionless-gated",
                "remove_disclosure_age_keep_level",
                4,
            ),
        ),
        "MARKET_REGIME_CONDITION": (
            (
                "stock_context_regime_interaction",
                "previous-session stock context changes response within the same market regime",
                ("PRIMARY", "CONDITION_ONLY"),
                "dimensionless-gated",
                "remove_stock_context_keep_market_regime",
                4,
            ),
        ),
        "INTRADAY_STATE_TRANSITION": (
            (
                "materialized_compound_state_transition",
                "a canonical materialized compound state changes the subsequent intraday response",
                ("PRIMARY", "STATE_ONLY"),
                "dimensionless-gated",
                "remove_compound_state_keep_payload",
                4,
            ),
        ),
    }
    output: dict[str, tuple[SkeletonSpec, ...]] = {}
    for route_id, rows in declarations.items():
        clock, maturity = _ROUTE_CLOCKS[route_id]
        output[route_id] = tuple(
            SkeletonSpec(
                skeleton_id=f"cn.comp.supp.v1.{route_id.lower()}.{name}",
                route_id=route_id,
                financial_hypothesis=hypothesis,
                input_roles=roles,
                unit_signature=unit_signature,
                clock_contract=clock,
                maturity_contract=maturity,
                control_ablation_rule=ablation,
                maximum_depth=maximum_depth,
                allowed_routes=(route_id,),
            )
            for name, hypothesis, roles, unit_signature, ablation, maximum_depth in rows
        )
    return output


def _pick(rows: Sequence[CapabilityField], index: int, seed: int, salt: str) -> CapabilityField:
    if not rows:
        raise ValueError(f"CONTROL_CONSTRUCTION_UNRESOLVED: empty field pool for {salt}")
    choice = int(
        stable_hash({"seed": int(seed), "salt": salt, "attempt": int(index)})[:16],
        16,
    )
    return rows[choice % len(rows)]


def _pick_value(values: Sequence[int], index: int, seed: int, salt: str) -> int:
    if not values:
        raise ValueError(f"empty numeric choice pool for {salt}")
    choice = int(
        stable_hash({"seed": int(seed), "salt": salt, "attempt": int(index)})[:16],
        16,
    )
    return int(values[choice % len(values)])


def _pick_excluding(
    rows: Sequence[CapabilityField],
    excluded_field_ids: Sequence[str],
    index: int,
    seed: int,
    salt: str,
) -> CapabilityField:
    excluded = set(excluded_field_ids)
    eligible = tuple(row for row in rows if row.field_id not in excluded)
    if not eligible:
        raise ValueError(
            f"FIELD_COVERAGE_BOTTLENECK: no distinct field remains for {salt}"
        )
    return _pick(eligible, index, seed, salt)


def _pick_compatible_pair(
    rows: Sequence[CapabilityField],
    predicate: Any,
    index: int,
    seed: int,
    salt: str,
) -> tuple[CapabilityField, CapabilityField]:
    compatible = tuple(
        (left, right)
        for left in rows
        for right in rows
        if left.field_id != right.field_id and predicate(left, right)
    )
    if not compatible:
        raise ValueError(
            f"ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED: no compatible field pair for {salt}"
        )
    return _pick(compatible, index, seed, salt)


def _canonical_representation(field: CapabilityField) -> Mapping[str, Any]:
    value = field.metadata.get("canonical_representation") or {}
    return value if isinstance(value, Mapping) else {}


def _representation_family(field: CapabilityField) -> str:
    representation = _canonical_representation(field)
    semantic = str(representation.get("semantic_family") or "")
    kind = str(representation.get("representation_type") or "")
    return "|".join(value for value in (field.source_family, semantic, kind) if value)


def _minute_leg_role(field: CapabilityField) -> str:
    source = str(field.source_field or "").lower()
    if any(token in source for token in ("amount", "volume", "liquidity")):
        return "volume_amount_or_liquidity"
    if any(token in source for token in ("return", "ret", "pct_chg", "range")):
        return "volatility_range_or_return"
    if source in {"open", "high", "low", "close", "vwap"}:
        return "price_or_return"
    return "unresolved"


def _is_price_or_return(field: CapabilityField) -> bool:
    return _minute_leg_role(field) in {
        "price_or_return",
        "volatility_range_or_return",
    }


def _is_volatility_range_or_return(field: CapabilityField) -> bool:
    role = _minute_leg_role(field)
    source = str(field.source_field or "").lower()
    return role == "volatility_range_or_return" or source in {"high", "low"}


def _declared_size(field: CapabilityField) -> bool:
    representation = _canonical_representation(field)
    metadata_tokens = "|".join(
        (
            str(field.source_field or ""),
            str(field.source_family or ""),
            str(representation.get("semantic_family") or ""),
            str(representation.get("representation_type") or ""),
        )
    ).lower()
    return "market_cap" in metadata_tokens or "market-cap" in metadata_tokens or "size" in metadata_tokens


def _unit_comparable_or_normalized(
    left: CapabilityField,
    right: CapabilityField,
) -> bool:
    left_rep = _canonical_representation(left)
    right_rep = _canonical_representation(right)
    left_kind = str(left_rep.get("representation_type") or "").lower()
    right_kind = str(right_rep.get("representation_type") or "").lower()
    normalized = ("ratio" in left_kind or "normalized" in left_kind) and (
        "ratio" in right_kind or "normalized" in right_kind
    )
    same_registered_family = (
        bool(left.source_family)
        and left.source_family == right.source_family
        and left.unit_status == right.unit_status
    )
    return normalized or same_registered_family


def _declared_temporal_evolution(field: CapabilityField) -> bool:
    representation = _canonical_representation(field)
    kind = str(representation.get("representation_type") or "").lower()
    return field.temporal_semantics == "SLOW_CHANGE" and any(
        token in kind
        for token in ("change", "delta", "slope", "acceleration", "persistence", "yoy", "qoq", "ttm")
    )


def _declared_reported_change(field: CapabilityField) -> bool:
    representation = _canonical_representation(field)
    kind = str(representation.get("representation_type") or "").lower()
    return field.temporal_semantics == "SLOW_CHANGE" and any(
        token in kind for token in ("reported", "yoy", "qoq", "ttm")
    )


class CompositionalGrammarV2:
    """Deterministic compositional proposal interface.

    Each proposal is compiled immediately.  A matched control is part of the
    returned object and has no independent proposal or vote.
    """

    def __init__(
        self,
        registry: UnifiedCapabilityRegistry,
        *,
        route_root_allowlist: Mapping[str, Iterable[str]] | None = None,
        enforce_route_compatibility: bool = False,
    ) -> None:
        self.registry = registry
        self.compiler = TypedRouteCompiler(registry)
        self._skeletons = skeleton_registry()
        self._route_root_allowlist = self._validate_route_root_allowlist(
            route_root_allowlist
        )
        self._enforce_route_compatibility = bool(enforce_route_compatibility)

    def _validate_route_root_allowlist(
        self,
        value: Mapping[str, Iterable[str]] | None,
    ) -> dict[str, frozenset[str]]:
        if value is None:
            return {}
        unknown_routes = set(value) - set(ROUTE_IDS)
        if unknown_routes:
            raise ValueError(
                f"unknown routes in proposal root allowlist: {sorted(unknown_routes)}"
            )
        output: dict[str, frozenset[str]] = {}
        for route_id, field_ids in value.items():
            allowed: set[str] = set()
            for field_id in field_ids:
                field = self.registry.resolve(str(field_id))
                if not field.search_eligible or route_id not in field.allowed_routes:
                    raise ValueError(
                        "proposal root is not eligible on route "
                        f"{route_id}: {field.field_id}"
                    )
                allowed.add(field.field_id)
            if not allowed:
                raise ValueError(f"empty proposal root allowlist on {route_id}")
            output[route_id] = frozenset(allowed)
        return output

    def _filter_proposal_roots(
        self,
        route_id: str,
        rows: Sequence[CapabilityField],
    ) -> tuple[CapabilityField, ...]:
        allowed = self._route_root_allowlist.get(route_id)
        if allowed is None:
            return tuple(rows)
        return tuple(row for row in rows if row.field_id in allowed)

    def _payload_pool(self, route_id: str) -> tuple[CapabilityField, ...]:
        rows = self.registry.fields_for_route(
            route_id,
            entity_scopes=("STOCK",),
            field_roles=("primary", "interaction-only"),
        )
        rows = self._filter_proposal_roots(route_id, rows)
        if not rows:
            raise ValueError(f"FIELD_COVERAGE_BOTTLENECK: no payload fields on {route_id}")
        return rows

    def _route_pool(
        self,
        route_id: str,
        *,
        source_family: str | None = None,
        temporal_semantics: str | None = None,
        entity_scope: str | None = None,
        field_roles: Sequence[str] = (),
    ) -> tuple[CapabilityField, ...]:
        rows = self.registry.fields_for_route(
            route_id,
            entity_scopes=(entity_scope,) if entity_scope else None,
            field_roles=field_roles or None,
        )
        rows = self._filter_proposal_roots(route_id, rows)
        if source_family is not None:
            rows = tuple(row for row in rows if row.source_family == source_family)
        if temporal_semantics is not None:
            rows = tuple(row for row in rows if row.temporal_semantics == temporal_semantics)
        if not rows:
            raise ValueError(
                f"FIELD_COVERAGE_BOTTLENECK: empty pool route={route_id} "
                f"family={source_family} semantics={temporal_semantics}"
            )
        return rows

    def _base(
        self,
        *,
        candidate_id: str,
        matched_control_id: str,
        route_id: str,
        expression: str,
        operator_family: str,
        skeleton: SkeletonSpec,
        fields: Sequence[CapabilityField],
        condition_fields: Sequence[CapabilityField] = (),
        seed: int,
        is_control: bool,
        generator_version: str = GRAMMAR_VERSION,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        route = self.registry.route_contracts[route_id]
        row = {
            "candidate_id": candidate_id,
            "route_id": route_id,
            "expression": expression,
            "operator_family": operator_family,
            "seed": int(seed),
            "proposal_origin": "typed_compositional_grammar_v2",
            "matched_control_id": matched_control_id,
            "declared_field_ids": [field.field_id for field in fields],
            "condition_field_ids": [field.field_id for field in condition_fields],
            "is_matched_control": bool(is_control),
            "vote_policy": "CONTROL_NO_SEPARATE_VOTE" if is_control else "ONE_SUPPORT_UNIT_ONE_VOTE",
            "maturity_contract_registered": route_id in {"DISCLOSURE_EVENT", "BROAD_EVENT_FROZEN_ENTRY"},
            "exposure_ledger_required": True,
            "access_roles": ["development"],
            "uses_future_revision": False,
            "requires_intrabar_order": False,
            "generator_version": str(generator_version),
            "skeleton_id": skeleton.skeleton_id,
            "financial_hypothesis": skeleton.financial_hypothesis,
            "input_roles": list(skeleton.input_roles),
            "unit_signature": skeleton.unit_signature,
            "clock_contract": skeleton.clock_contract,
            "maturity_contract": skeleton.maturity_contract,
            "control_ablation_rule": skeleton.control_ablation_rule,
            "maximum_depth": skeleton.maximum_depth,
            "support_unit": str(route["support_unit"]),
            "outer_mapping": "cross_sectional",
            "search_role": skeleton.search_role,
        }
        row.update(dict(extra or {}))
        return row

    def _make_pair(
        self,
        *,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
        primary_expression: str,
        control_expression: str,
        operator_family: str,
        control_operator_family: str | None = None,
        fields: Sequence[CapabilityField],
        condition_fields: Sequence[CapabilityField] = (),
        generator_version: str = GRAMMAR_VERSION,
        extra: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        unique_fields = tuple({field.field_id: field for field in fields}.values())
        digest = stable_hash(
            {
                "grammar": str(generator_version),
                "route": skeleton.route_id,
                "skeleton": skeleton.skeleton_id,
                "seed": int(seed),
                "attempt": int(attempt_index),
                "fields": [field.field_id for field in unique_fields],
            }
        )[:20]
        primary_id = f"cn.comp.{digest}"
        control_id = primary_id + ".control"
        primary = self._base(
            candidate_id=primary_id,
            matched_control_id=control_id,
            route_id=skeleton.route_id,
            expression=primary_expression,
            operator_family=operator_family,
            skeleton=skeleton,
            fields=unique_fields,
            condition_fields=condition_fields,
            seed=seed,
            is_control=False,
            generator_version=generator_version,
            extra=extra,
        )
        control = self._base(
            candidate_id=control_id,
            matched_control_id=primary_id,
            route_id=skeleton.route_id,
            expression=control_expression,
            operator_family=control_operator_family or operator_family,
            skeleton=skeleton,
            fields=unique_fields,
            condition_fields=condition_fields,
            seed=seed,
            is_control=True,
            generator_version=generator_version,
            extra=extra,
        )
        constructor_id = COMPOSITIONAL_CONTROL_CONSTRUCTOR_MATRIX[skeleton.route_id][
            "control_constructor_id"
        ]
        return attach_pair_contract(
            primary,
            control,
            control_constructor_id=str(constructor_id),
        )

    def _minute_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        pool = self._payload_pool("MINUTE_STATIC")
        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        extra: dict[str, Any] = {}
        if self._enforce_route_compatibility and name == "price_volume_interaction":
            left, right = _pick_compatible_pair(
                pool,
                lambda price, volume: _is_price_or_return(price)
                and _minute_leg_role(volume) == "volume_amount_or_liquidity",
                attempt_index,
                seed,
                skeleton.skeleton_id + ":compatible",
            )
            extra["compatibility_leg_roles"] = [
                "price_or_return",
                "volume_amount_or_liquidity",
            ]
        elif self._enforce_route_compatibility and name == "liquidity_volatility_interaction":
            left, right = _pick_compatible_pair(
                pool,
                lambda liquidity, volatility: _minute_leg_role(liquidity)
                == "volume_amount_or_liquidity"
                and _is_volatility_range_or_return(volatility),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":compatible",
            )
            extra["compatibility_leg_roles"] = [
                "volume_amount_or_liquidity",
                "volatility_range_or_return",
            ]
        elif self._enforce_route_compatibility and name == "cross_sectional_residual":
            left, right = _pick_compatible_pair(
                pool,
                lambda first, second: _representation_family(first)
                != _representation_family(second),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":compatible",
            )
            extra["compatibility_leg_roles"] = [
                "distinct_representation_family",
                "distinct_representation_family",
            ]
        else:
            left = _pick(pool, attempt_index, seed, skeleton.skeleton_id + ":left")
            right = _pick_excluding(
                pool,
                (left.field_id,),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":right",
            )
        left_ref = f"${left.field_id}"
        right_ref = f"${right.field_id}"
        fields: tuple[CapabilityField, ...]
        if name == "normalized_level":
            primary_expression = f"CSRank(ZScore({left_ref}))"
            control_expression = f"CSRank(Sign({left_ref}))"
            operator_family = "ZScore"
            fields = (left,)
        else:
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,ZScore({right_ref}))))"
            operator_family = "Arithmetic"
            fields = (left, right)
            if name == "field_spread":
                primary_expression = f"CSRank(Sub(ZScore({left_ref}),ZScore({right_ref})))"
            elif name == "normalized_ratio":
                primary_expression = f"CSRank(SafeDiv(ZScore({left_ref}),ZScore({right_ref}),0.05))"
            elif name == "price_volume_interaction":
                primary_expression = f"CSRank(Mul(ZScore({left_ref}),ZScore({right_ref})))"
            elif name == "liquidity_volatility_interaction":
                primary_expression = f"CSRank(Mul(ZScore({left_ref}),Abs(ZScore({right_ref}))))"
            elif name == "cross_sectional_residual":
                primary_expression = f"CSResidual(ZScore({left_ref}),ZScore({right_ref}))"
                operator_family = "CrossSectionalResidual"
            elif name == "absolute_state_interaction":
                primary_expression = f"CSRank(Mul(Sign({left_ref}),Abs(ZScore({right_ref}))))"
            elif name == "dispersion_interaction":
                primary_expression = f"CSRank(Sub(Abs(ZScore({left_ref})),Abs(ZScore({right_ref}))))"
            else:  # pragma: no cover - declarations and constructors are kept exhaustive.
                raise KeyError(f"unhandled minute skeleton: {name}")

        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=primary_expression,
            control_expression=control_expression,
            operator_family=operator_family,
            fields=fields,
            extra=extra,
        )

    def _firstn_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        firstn_pool = self._route_pool("FIRSTN_PATH", source_family="firstN")
        raw_pool = self._route_pool("FIRSTN_PATH", source_family="raw_1min")
        firstn = _pick(firstn_pool, attempt_index, seed, skeleton.skeleton_id + ":firstn")
        raw = _pick(raw_pool, attempt_index, seed, skeleton.skeleton_id + ":raw")
        firstn_ref, raw_ref = f"${firstn.field_id}", f"${raw.field_id}"
        window = _pick_value((3, 5, 10, 20), attempt_index, seed, skeleton.skeleton_id + ":window")
        short, long = (3, 10) if window <= 5 else (5, 20)
        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        control_expression = (
            f"CSRank(Add(ZScore({firstn_ref}),Mul(0,Delta({raw_ref},{window}))))"
        )
        if name == "firstn_path_state":
            primary_expression = f"CSRank(Mul(ZScore({firstn_ref}),ZScore(Delta({raw_ref},{window}))))"
            family = "SignedPath"
        elif name == "opening_path_liquidity":
            primary_expression = f"CSRank(Mul(ZScore(PathShape({firstn_ref},{window})),ZScore({raw_ref})))"
            family = "PathShape"
        elif name == "opening_imbalance_persistence":
            primary_expression = f"Mul(ZScore({firstn_ref}),Persistence(Positive({raw_ref}),{window}))"
            family = "SignedPath"
        elif name == "relative_strength_reversal":
            primary_expression = f"CSRank(Sub(ZScore({firstn_ref}),ZScore(Delta({raw_ref},{window}))))"
            family = "MeanReversion"
        elif name == "opening_path_acceleration":
            primary_expression = f"CSRank(Mul(ZScore({firstn_ref}),ZScore(Acceleration({raw_ref},{window}))))"
            family = "PathShape"
        elif name == "opening_multiscale_relation":
            primary_expression = f"CSRank(MultiScaleRelation({firstn_ref},{raw_ref},{short},{long}))"
            family = "PathShape"
        elif name == "opening_state_residual":
            primary_expression = f"CSResidual(ZScore({firstn_ref}),ZScore(Delta({raw_ref},{window})))"
            family = "MeanReversion"
        elif name == "conditioned_opening_path":
            primary_expression = f"CSRank(Mul(ZScore(PathShape({firstn_ref},{window})),Sign(Delta({raw_ref},{window}))))"
            family = "SignedPath"
        else:  # pragma: no cover
            raise KeyError(f"unhandled FirstN skeleton: {name}")
        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=primary_expression,
            control_expression=control_expression,
            operator_family=family,
            fields=(firstn, raw),
        )

    def _slow_level_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        pool = self._payload_pool("SLOW_CROSS_SECTIONAL_LEVEL")
        size_pool = tuple(
            row
            for row in pool
            if (
                _declared_size(row)
                if self._enforce_route_compatibility
                else "market_cap" in row.field_id.lower()
            )
        )
        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        extra: dict[str, Any] = {}
        if self._enforce_route_compatibility and name == "fundamental_ratio":
            left, right = _pick_compatible_pair(
                pool,
                _unit_comparable_or_normalized,
                attempt_index,
                seed,
                skeleton.skeleton_id + ":comparable_units",
            )
            extra["compatibility_leg_roles"] = ["unit_comparable", "unit_comparable"]
        elif self._enforce_route_compatibility and name == "cross_family_interaction":
            left, right = _pick_compatible_pair(
                pool,
                lambda first, second: _representation_family(first)
                != _representation_family(second),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":cross_family",
            )
            extra["compatibility_leg_roles"] = [
                "distinct_source_or_representation_family",
                "distinct_source_or_representation_family",
            ]
        else:
            left = _pick(pool, attempt_index, seed, skeleton.skeleton_id + ":left")
            right = _pick_excluding(
                pool,
                (left.field_id,),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":right",
            )
        if self._enforce_route_compatibility and name in {
            "fundamental_cap_condition",
            "size_residual_level",
        }:
            non_size = tuple(row for row in pool if not _declared_size(row))
            if not size_pool or not non_size:
                raise ValueError(
                    f"ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED: explicit size leg unavailable for {skeleton.skeleton_id}"
                )
            left = _pick(non_size, attempt_index, seed, skeleton.skeleton_id + ":non_size_left")
            extra["compatibility_leg_roles"] = ["non_size_payload", "declared_size"]
        elif (
            not self._enforce_route_compatibility
            and name in {"fundamental_cap_condition", "size_residual_level"}
            and any(row.field_id == left.field_id for row in size_pool)
        ):
            left = _pick_excluding(
                pool,
                tuple(row.field_id for row in size_pool),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":non_size_left",
            )
        size = _pick(size_pool, attempt_index, seed, skeleton.skeleton_id + ":size") if size_pool else left
        left_ref, right_ref, size_ref = f"${left.field_id}", f"${right.field_id}", f"${size.field_id}"
        fields: tuple[CapabilityField, ...]
        if name == "fundamental_level":
            primary_expression = f"CSRank({left_ref})"
            control_expression = f"CSRank(Sign({left_ref}))"
            family = "CSRank"
            fields = (left,)
        elif name == "fundamental_ratio":
            primary_expression = f"CSRank(SafeDiv(ZScore({left_ref}),ZScore({right_ref}),0.05))"
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,ZScore({right_ref}))))"
            family = "CSRank"
            fields = (left, right)
        elif name == "fundamental_cap_condition":
            primary_expression = f"CSRank(Mul(ZScore({left_ref}),Sign({size_ref})))"
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,Sign({size_ref}))))"
            family = "SizeNeutralize"
            fields = (left, size)
        elif name == "fundamental_residual":
            primary_expression = f"CSResidual(ZScore({left_ref}),ZScore({right_ref}))"
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,ZScore({right_ref}))))"
            family = "SizeNeutralize"
            fields = (left, right)
        elif name == "cross_family_interaction":
            primary_expression = f"CSRank(Mul(ZScore({left_ref}),ZScore({right_ref})))"
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,ZScore({right_ref}))))"
            family = "CSRank"
            fields = (left, right)
        elif name == "winsorized_level":
            primary_expression = f"CSRank(Winsorize({left_ref}))"
            control_expression = f"CSRank(Sign({left_ref}))"
            family = "Winsorize"
            fields = (left,)
        elif name == "masked_normalized_level":
            primary_expression = f"MaskedZScore({left_ref},20,0.8)"
            control_expression = f"CSRank(Sign({left_ref}))"
            family = "MaskedZScore"
            fields = (left,)
        elif name == "size_residual_level":
            primary_expression = f"CSRank(Sub(ZScore({left_ref}),ZScore({size_ref})))"
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,ZScore({size_ref}))))"
            family = "SizeNeutralize"
            fields = (left, size)
        else:  # pragma: no cover
            raise KeyError(f"unhandled slow-level skeleton: {name}")
        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=primary_expression,
            control_expression=control_expression,
            operator_family=family,
            fields=fields,
            extra=extra,
        )

    def _slow_change_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        pool = self._payload_pool("SLOW_TEMPORAL_CHANGE")
        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        extra: dict[str, Any] = {}
        if self._enforce_route_compatibility and name in {
            "slope",
            "acceleration",
            "change_persistence",
        }:
            evolving = tuple(row for row in pool if _declared_temporal_evolution(row))
            if not evolving:
                raise ValueError(
                    f"ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED: temporal-evolution metadata unavailable for {skeleton.skeleton_id}"
                )
            left = _pick(evolving, attempt_index, seed, skeleton.skeleton_id + ":evolving")
            right = _pick_excluding(pool, (left.field_id,), attempt_index, seed, skeleton.skeleton_id + ":right")
            extra["compatibility_leg_roles"] = ["declared_temporal_evolution"]
        elif self._enforce_route_compatibility and name == "reported_change":
            reported = tuple(row for row in pool if _declared_reported_change(row))
            if not reported:
                raise ValueError(
                    f"ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED: reported-change metadata unavailable for {skeleton.skeleton_id}"
                )
            left = _pick(reported, attempt_index, seed, skeleton.skeleton_id + ":reported")
            right = _pick_excluding(pool, (left.field_id,), attempt_index, seed, skeleton.skeleton_id + ":right")
            extra["compatibility_leg_roles"] = ["declared_reported_change"]
        elif self._enforce_route_compatibility and name == "cross_change_interaction":
            evolving = tuple(row for row in pool if _declared_temporal_evolution(row))
            left, right = _pick_compatible_pair(
                evolving,
                lambda first, second: _representation_family(first)
                != _representation_family(second),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":cross_change_family",
            )
            extra["compatibility_leg_roles"] = [
                "distinct_change_representation_family",
                "distinct_change_representation_family",
            ]
        else:
            left = _pick(pool, attempt_index, seed, skeleton.skeleton_id + ":left")
            right = _pick_excluding(
                pool,
                (left.field_id,),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":right",
            )
        left_ref, right_ref = f"${left.field_id}", f"${right.field_id}"
        window = _pick_value((2, 3, 5, 10), attempt_index, seed, skeleton.skeleton_id + ":window")
        fields: tuple[CapabilityField, ...] = (left,)
        if name == "delta":
            primary_expression = f"CSRank(Delta({left_ref},{window}))"
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,Delta({left_ref},{window}))))"
            family = "Delta"
        elif name == "slope":
            primary_expression = f"CSRank(Slope({left_ref},{window}))"
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,Slope({left_ref},{window}))))"
            family = "Slope"
        elif name == "acceleration":
            primary_expression = f"CSRank(Acceleration({left_ref},{window}))"
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,Acceleration({left_ref},{window}))))"
            family = "Acceleration"
        elif name == "reported_change":
            primary_expression = f"CSRank({left_ref})"
            control_expression = f"CSRank(Sign({left_ref}))"
            family = "YoY"
        elif name == "change_level_interaction":
            primary_expression = f"CSRank(Mul(ZScore({left_ref}),ZScore({right_ref})))"
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,ZScore({right_ref}))))"
            family = "Delta"
            fields = (left, right)
        elif name == "short_long_change":
            primary_expression = f"CSRank(Sub(ZScore(Delta({left_ref},2)),ZScore(Delta({left_ref},10))))"
            control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,Delta({left_ref},10))))"
            family = "MultiScaleRelation"
        elif name == "change_persistence":
            primary_expression = f"CSRank(Persistence(Positive({left_ref}),{window}))"
            control_expression = f"Add(ZScore({left_ref}),Mul(0,Persistence(Positive({left_ref}),{window})))"
            family = "Persistence"
        elif name == "cross_change_interaction":
            primary_expression = f"CSRank(Mul(ZScore(Delta({left_ref},{window})),ZScore(Delta({right_ref},{window}))))"
            control_expression = f"CSRank(Add(Delta({left_ref},{window}),Mul(0,Delta({right_ref},{window}))))"
            family = "MultiScaleRelation"
            fields = (left, right)
        else:  # pragma: no cover
            raise KeyError(f"unhandled slow-change skeleton: {name}")
        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=primary_expression,
            control_expression=control_expression,
            operator_family=family,
            fields=fields,
            extra=extra,
        )

    def _disclosure_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        event_pool = self._route_pool(
            "DISCLOSURE_EVENT",
            temporal_semantics="DISCLOSURE_PULSE",
            entity_scope="STOCK",
            field_roles=("condition-only",),
        )
        payload_pool = tuple(
            row
            for row in self._payload_pool("DISCLOSURE_EVENT")
            if row.temporal_semantics
            in {"DISCLOSURE_LEVEL_PAYLOAD", "DISCLOSURE_CHANGE_PAYLOAD"}
        )
        if not payload_pool:
            raise ValueError("FIELD_COVERAGE_BOTTLENECK: no disclosure payload fields")
        event = _pick(event_pool, attempt_index, seed, skeleton.skeleton_id + ":event")
        payload = _pick(
            payload_pool,
            attempt_index,
            seed,
            skeleton.skeleton_id + ":payload",
        )
        event_ref, payload_ref = f"${event.field_id}", f"${payload.field_id}"
        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        fields: tuple[CapabilityField, ...] = (event,)
        control_expression = f"TimeSince({event_ref})"
        if name == "event_pulse":
            primary_expression = f"EventCount({event_ref},1)"
            family = "EventCount"
        elif name == "event_age":
            primary_expression = f"SafeDiv(1,Add(TimeSince({event_ref}),1),1)"
            family = "TimeSince"
        elif name == "pre_event_path":
            primary_expression = f"EventWindow({payload_ref},{event_ref},5,0)"
            control_expression = f"Add({payload_ref},Mul(0,TimeSince({event_ref})))"
            family = "PreEventPath"
            fields = (event, payload)
        elif name == "post_maturity_state":
            primary_expression = f"EventWindow({payload_ref},{event_ref},0,5)"
            control_expression = f"Add({payload_ref},Mul(0,TimeSince({event_ref})))"
            family = "PostMaturityOutcome"
            fields = (event, payload)
        elif name == "event_prior_condition":
            primary_expression = f"Mul(EventCount({event_ref},1),Sign({payload_ref}))"
            control_expression = f"Add({payload_ref},Mul(0,TimeSince({event_ref})))"
            family = "EventWindow"
            fields = (event, payload)
        elif name == "repeated_event_suppression":
            primary_expression = f"SafeDiv(EventCount({event_ref},5),Add(EventCount({event_ref},20),1),1)"
            family = "EventCount"
        elif name == "event_window":
            primary_expression = f"EventWindow({payload_ref},{event_ref},5,5)"
            control_expression = f"Add({payload_ref},Mul(0,TimeSince({event_ref})))"
            family = "EventWindow"
            fields = (event, payload)
        elif name == "event_first_hit":
            primary_expression = f"FirstHit({event_ref},20)"
            family = "FirstHit"
        else:  # pragma: no cover
            raise KeyError(f"unhandled disclosure skeleton: {name}")
        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=primary_expression,
            control_expression=control_expression,
            operator_family=family,
            fields=fields,
            condition_fields=(event,),
            extra={"episode_policy": "UNIQUE_DISCLOSURE_EPISODE"},
        )

    def _market_regime_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        payload_pool = self._payload_pool("MARKET_REGIME_CONDITION")
        condition_pool = self._route_pool(
            "MARKET_REGIME_CONDITION",
            entity_scope="MARKET",
            field_roles=("state-only", "condition-only", "primary"),
        )
        payload = _pick(payload_pool, attempt_index, seed, skeleton.skeleton_id + ":payload")
        condition = _pick(condition_pool, attempt_index, seed, skeleton.skeleton_id + ":condition")
        payload_ref, condition_ref = f"${payload.field_id}", f"${condition.field_id}"
        window = _pick_value((3, 5, 10, 20), attempt_index, seed, skeleton.skeleton_id + ":window")
        short, long = (3, 10) if window <= 5 else (5, 20)
        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        control_expression = (
            f"CSRank(Add(ZScore({payload_ref}),Mul(0,{condition_ref})))"
        )
        family = "RegimeInteraction"
        if name == "payload_regime_interaction":
            primary_expression = f"CSRank(Mul(ZScore({payload_ref}),Sign({condition_ref})))"
        elif name == "payload_regime_residual":
            primary_expression = (
                f"CSRank(Sub(ZScore({payload_ref}),Mul(Sign({payload_ref}),Sign({condition_ref}))))"
            )
        elif name == "regime_transition_response":
            primary_expression = (
                f"CSRank(Mul(ZScore(Delta({payload_ref},{window})),Transition({condition_ref},0,1)))"
            )
            family = "Transition"
        elif name == "regime_maturity_payload":
            primary_expression = (
                f"CSRank(Mul(Sign({payload_ref}),Persistence(Positive({condition_ref}),{window})))"
            )
            family = "StateAge"
        elif name == "regime_age_response":
            primary_expression = (
                f"CSRank(Mul(ZScore({payload_ref}),StateAge(Sign({condition_ref}))))"
            )
            family = "StateAge"
        elif name == "regime_multiscale_response":
            primary_expression = (
                f"CSRank(Mul(MultiScaleRelation({payload_ref},{condition_ref},{short},{long}),Sign({condition_ref})))"
            )
        elif name == "regime_conditioned_path":
            primary_expression = (
                f"CSRank(Mul(PathShape({payload_ref},{window}),Sign({condition_ref})))"
            )
            family = "ConditionGate"
        elif name == "regime_persistence_gate":
            primary_expression = (
                f"CSRank(Add(ZScore({payload_ref}),Mul(PathShape({payload_ref},{window}),Sign({condition_ref}))))"
            )
            family = "ConditionGate"
        else:  # pragma: no cover
            raise KeyError(f"unhandled market-regime skeleton: {name}")
        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=primary_expression,
            control_expression=control_expression,
            operator_family=family,
            fields=(payload, condition),
            condition_fields=(condition,),
            extra={
                "entity_scope": "MARKET_CONDITIONED_STOCK_PAYLOAD",
                "cross_sectional_rank_allowed_for_condition": False,
                "market_vote_policy": "ONE_MARKET_TIME_BLOCK_ONE_VOTE",
            },
        )

    def _slow_disclosure_age_supplemental_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        age_pool = self._route_pool(
            "SLOW_CROSS_SECTIONAL_LEVEL",
            source_family="canonical_fundamental_disclosure_timing_staleness",
            temporal_semantics="ASOF_LEVEL",
            entity_scope="STOCK",
            field_roles=("condition-only",),
        )
        age = _pick(age_pool, attempt_index, seed, skeleton.skeleton_id + ":age")
        linked_payloads = tuple(
            row
            for row in self._payload_pool("SLOW_CROSS_SECTIONAL_LEVEL")
            if row.source_table == age.source_table
        )
        if not linked_payloads:
            raise ValueError(
                "FIELD_COVERAGE_BOTTLENECK: disclosure-age condition has no "
                f"same-source payload: {age.field_id}"
            )
        payload = _pick(
            linked_payloads,
            attempt_index,
            seed,
            skeleton.skeleton_id + ":payload",
        )
        payload_ref, age_ref = f"${payload.field_id}", f"${age.field_id}"
        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=(
                f"CSRank(Mul(ZScore({payload_ref}),SafeDiv(1,Add({age_ref},1),1)))"
            ),
            control_expression=(
                f"CSRank(Add(ZScore({payload_ref}),Mul(0,{age_ref})))"
            ),
            operator_family="CSRank",
            fields=(payload, age),
            condition_fields=(age,),
            generator_version=SUPPLEMENTAL_GRAMMAR_VERSION,
            extra={
                "proposal_origin": "typed_compositional_supplemental_v1",
                "supplemental_authority_id": SUPPLEMENTAL_ROOT_SCOPE_ID,
                "supplemental_gap_id": "SLOW_DISCLOSURE_AGE_CONDITIONS",
                "supplemental_delta_only": True,
                "existing_pack_rewrite_allowed": False,
                "materialization_status": "NOT_MATERIALIZED",
                "signal_sketch_allowed": False,
                "strict_evaluation_allowed": False,
                "required_materialization_receipt": "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT",
                "required_support_receipt": "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT",
            },
        )

    def _market_stock_context_supplemental_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        payload = _pick(
            self._payload_pool("MARKET_REGIME_CONDITION"),
            attempt_index,
            seed,
            skeleton.skeleton_id + ":payload",
        )
        market_condition = _pick(
            self._route_pool(
                "MARKET_REGIME_CONDITION",
                entity_scope="MARKET",
                field_roles=("state-only", "condition-only", "primary"),
            ),
            attempt_index,
            seed,
            skeleton.skeleton_id + ":market_condition",
        )
        stock_context = _pick(
            self._route_pool(
                "MARKET_REGIME_CONDITION",
                temporal_semantics="PREVIOUS_SESSION_STOCK_CONTEXT",
                entity_scope="STOCK",
                field_roles=("state-only", "condition-only"),
            ),
            attempt_index,
            seed,
            skeleton.skeleton_id + ":stock_context",
        )
        payload_ref = f"${payload.field_id}"
        market_ref = f"${market_condition.field_id}"
        context_ref = f"${stock_context.field_id}"
        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=(
                f"CSRank(Mul(ZScore({payload_ref}),Mul(Sign({market_ref}),Sign({context_ref}))))"
            ),
            control_expression=(
                f"CSRank(Add(Mul(ZScore({payload_ref}),Sign({market_ref})),Mul(0,{context_ref})))"
            ),
            operator_family="RegimeInteraction",
            fields=(payload, market_condition, stock_context),
            condition_fields=(market_condition, stock_context),
            generator_version=SUPPLEMENTAL_GRAMMAR_VERSION,
            extra={
                "proposal_origin": "typed_compositional_supplemental_v1",
                "supplemental_authority_id": SUPPLEMENTAL_ROOT_SCOPE_ID,
                "supplemental_gap_id": "PREVIOUS_SESSION_STOCK_REGIME_CONTEXTS",
                "supplemental_delta_only": True,
                "existing_pack_rewrite_allowed": False,
                "market_condition_field_ids": [market_condition.field_id],
                "stock_context_field_ids": [stock_context.field_id],
                "entity_scope": "MARKET_AND_STOCK_CONTEXT_CONDITIONED_STOCK_PAYLOAD",
                "cross_sectional_rank_allowed_for_condition": False,
                "market_vote_policy": "ONE_MARKET_TIME_BLOCK_ONE_VOTE",
            },
        )

    def _compound_state_supplemental_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        compound_states = tuple(
            row
            for row in self._route_pool(
                "INTRADAY_STATE_TRANSITION",
                temporal_semantics="INTRADAY_DERIVED_STATE",
                entity_scope="STOCK",
                field_roles=("state-only",),
            )
            if str(row.metadata.get("materialization_expression") or "").count("(") > 1
        )
        state = _pick(
            compound_states,
            attempt_index,
            seed,
            skeleton.skeleton_id + ":compound_state",
        )
        source_ids = tuple(
            str(value) for value in state.metadata.get("source_fields", ())
        )
        if not source_ids:
            raise ValueError(
                f"FIELD_COVERAGE_BOTTLENECK: compound state has no lineage: {state.field_id}"
            )
        payload = _pick_excluding(
            self._route_pool(
                "INTRADAY_STATE_TRANSITION",
                source_family="raw_1min",
                entity_scope="STOCK",
                field_roles=("primary", "interaction-only"),
            ),
            source_ids,
            attempt_index,
            seed,
            skeleton.skeleton_id + ":payload",
        )
        state_ref, payload_ref = f"${state.field_id}", f"${payload.field_id}"
        materialization_expression = str(
            state.metadata.get("materialization_expression") or ""
        )
        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=(
                f"CSRank(Mul(Transition({state_ref},-1,1),Delta({payload_ref},5)))"
            ),
            control_expression=(
                f"CSRank(Add(ZScore(Delta({payload_ref},5)),Mul(0,{state_ref})))"
            ),
            operator_family="Transition",
            fields=(state, payload),
            generator_version=SUPPLEMENTAL_GRAMMAR_VERSION,
            extra={
                "proposal_origin": "typed_compositional_supplemental_v1",
                "supplemental_authority_id": SUPPLEMENTAL_ROOT_SCOPE_ID,
                "supplemental_gap_id": "COMPOUND_INTRADAY_STATE_ROOT",
                "supplemental_delta_only": True,
                "existing_pack_rewrite_allowed": False,
                "claimed_state_field_id": state.field_id,
                "state_source_expression": state_ref,
                "state_materialization_required": True,
                "state_materialization_expression": materialization_expression,
                "state_source_field_ids": list(source_ids),
                "state_materialization_authority": "feature_state_fabric",
                "state_leaf_contract": "CANONICAL_MATERIALIZED_REPRESENTATION_ONE_LEAF",
                "state_support_unit": "stock-state episode",
                "materialization_status": "NOT_MATERIALIZED",
                "signal_sketch_allowed": False,
                "strict_evaluation_allowed": False,
                "required_materialization_receipt": "FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
                "required_support_receipt": "FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
            },
        )

    def _intraday_state_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        state_pool = self._route_pool(
            "INTRADAY_STATE_TRANSITION",
            temporal_semantics="INTRADAY_DERIVED_STATE",
            entity_scope="STOCK",
            field_roles=("state-only",),
        )
        # Deep compound state definitions would consume the entire depth budget
        # before the tested mechanism is applied.  Keep those registered, but
        # fail closed for this bounded depth-4 grammar.
        state_pool = tuple(
            row
            for row in state_pool
            if str(row.metadata.get("materialization_expression") or "").count("(") <= 1
        )
        payload_pool = self._route_pool(
            "INTRADAY_STATE_TRANSITION",
            source_family="raw_1min",
            entity_scope="STOCK",
            field_roles=("primary", "interaction-only"),
        )
        state = _pick(state_pool, attempt_index, seed, skeleton.skeleton_id + ":state")
        state_expression = str(state.metadata.get("materialization_expression") or "")
        source_ids = tuple(str(value) for value in state.metadata.get("source_fields", ()))
        source_fields = tuple(self.registry.resolve(field_id) for field_id in source_ids)
        payload = _pick_excluding(
            payload_pool,
            source_ids,
            attempt_index,
            seed,
            skeleton.skeleton_id + ":payload",
        )
        payload_ref = f"${payload.field_id}"
        window = _pick_value((3, 5, 10, 20), attempt_index, seed, skeleton.skeleton_id + ":window")
        short, long = (3, 10) if window <= 5 else (5, 20)
        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        control_expression = (
            f"CSRank(Add(ZScore(Delta({payload_ref},{window})),Mul(0,{state_expression})))"
        )
        family = "Transition"
        if name == "transition_payload":
            primary_expression = (
                f"CSRank(Mul(Transition({state_expression},-1,1),Delta({payload_ref},{window})))"
            )
        elif name == "transition_persistence":
            primary_expression = (
                f"Mul(Persistence(Positive({state_expression}),{window}),Sign({payload_ref}))"
            )
        elif name == "transition_state_age":
            primary_expression = (
                f"CSRank(Mul(StateAge({state_expression}),Delta({payload_ref},{window})))"
            )
            family = "StateAge"
        elif name == "transition_liquidity_condition":
            primary_expression = (
                f"CSRank(Mul(Transition({state_expression},0,1),ZScore({payload_ref})))"
            )
            family = "ConditionedPath"
        elif name == "state_residual":
            primary_expression = f"CSResidual(ZScore(Delta({payload_ref},{window})),{state_expression})"
            family = "StateResidual"
        elif name == "conditioned_state_path":
            primary_expression = (
                f"CSRank(Mul({state_expression},PathShape({payload_ref},{window})))"
            )
            family = "ConditionedPath"
        elif name == "duration_payload":
            primary_expression = (
                f"CSRank(Mul(Duration({state_expression}),Delta({payload_ref},{window})))"
            )
            family = "Duration"
        elif name == "multiscale_state_response":
            primary_expression = (
                f"CSRank(MultiScaleRelation({state_expression},{payload_ref},{short},{long}))"
            )
            family = "StateResidual"
        else:  # pragma: no cover
            raise KeyError(f"unhandled intraday-state skeleton: {name}")
        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=primary_expression,
            control_expression=control_expression,
            operator_family=family,
            fields=(state, payload, *source_fields),
            extra={
                "claimed_state_field_id": state.field_id,
                "state_source_expression": state_expression,
                "state_support_unit": "stock-state episode",
            },
        )

    def _broad_event_reference_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        pool = self._route_pool(
            "BROAD_EVENT_FROZEN_ENTRY",
            source_family="broad_event_frozen_entry",
            field_roles=("condition-only",),
        )
        mechanism = _pick(pool, attempt_index, seed, skeleton.skeleton_id + ":frozen")
        mechanism_ref = f"${mechanism.field_id}"
        frozen = dict(mechanism.metadata.get("frozen_mechanism") or {})
        return self._make_pair(
            skeleton=skeleton,
            attempt_index=attempt_index,
            seed=seed,
            primary_expression=f"FrozenMechanismReplay({mechanism_ref})",
            control_expression=f"MatchedControlReplay({mechanism_ref})",
            operator_family="FrozenMechanismReplay",
            control_operator_family="MatchedControlReplay",
            fields=(mechanism,),
            condition_fields=(mechanism,),
            extra={
                "frozen_mechanism_id": str(frozen.get("mechanism_id") or mechanism.field_id),
                "frozen_behavior_cluster_id": str(frozen.get("behavior_cluster_id") or ""),
                "candidate_promotion": False,
                "cross_sprint_memory": False,
            },
        )

    def propose(
        self,
        route_id: str,
        *,
        attempt_index: int,
        seed: int,
    ) -> GeneratedCompositionalPair:
        if route_id not in self._skeletons:
            raise KeyError(f"unknown compositional route: {route_id}")
        skeletons = self._skeletons[route_id]
        skeleton = skeletons[int(attempt_index) % len(skeletons)]
        if route_id == "MINUTE_STATIC":
            primary, control = self._minute_pair(skeleton, int(attempt_index), int(seed))
        elif route_id == "FIRSTN_PATH":
            primary, control = self._firstn_pair(skeleton, int(attempt_index), int(seed))
        elif route_id == "SLOW_CROSS_SECTIONAL_LEVEL":
            primary, control = self._slow_level_pair(skeleton, int(attempt_index), int(seed))
        elif route_id == "SLOW_TEMPORAL_CHANGE":
            primary, control = self._slow_change_pair(skeleton, int(attempt_index), int(seed))
        elif route_id == "DISCLOSURE_EVENT":
            primary, control = self._disclosure_pair(skeleton, int(attempt_index), int(seed))
        elif route_id == "MARKET_REGIME_CONDITION":
            primary, control = self._market_regime_pair(skeleton, int(attempt_index), int(seed))
        elif route_id == "INTRADAY_STATE_TRANSITION":
            primary, control = self._intraday_state_pair(skeleton, int(attempt_index), int(seed))
        elif route_id == "BROAD_EVENT_FROZEN_ENTRY":
            primary, control = self._broad_event_reference_pair(skeleton, int(attempt_index), int(seed))
        else:
            raise NotImplementedError(f"route constructor is not implemented yet: {route_id}")
        compiled_primary = {**primary, **self.compiler.compile(primary).to_dict()}
        compiled_control = {**control, **self.compiler.compile(control).to_dict()}
        return GeneratedCompositionalPair(
            skeleton=skeleton,
            primary=compiled_primary,
            control=compiled_control,
        )

    def propose_supplemental(
        self,
        route_id: str,
        *,
        attempt_index: int,
        seed: int,
    ) -> GeneratedCompositionalPair:
        """Generate only append-only gap candidates without remapping base attempts."""

        supplemental = supplemental_skeleton_registry()
        if route_id not in supplemental:
            raise KeyError(f"route has no supplemental constructor: {route_id}")
        skeletons = supplemental[route_id]
        skeleton = skeletons[int(attempt_index) % len(skeletons)]
        if route_id == "SLOW_CROSS_SECTIONAL_LEVEL":
            primary, control = self._slow_disclosure_age_supplemental_pair(
                skeleton,
                int(attempt_index),
                int(seed),
            )
        elif route_id == "MARKET_REGIME_CONDITION":
            primary, control = self._market_stock_context_supplemental_pair(
                skeleton,
                int(attempt_index),
                int(seed),
            )
        elif route_id == "INTRADAY_STATE_TRANSITION":
            primary, control = self._compound_state_supplemental_pair(
                skeleton,
                int(attempt_index),
                int(seed),
            )
        else:  # pragma: no cover - registry and constructors are kept exhaustive.
            raise NotImplementedError(
                f"supplemental route constructor is not implemented: {route_id}"
            )
        compiled_primary = {**primary, **self.compiler.compile(primary).to_dict()}
        compiled_control = {**control, **self.compiler.compile(control).to_dict()}
        return GeneratedCompositionalPair(
            skeleton=skeleton,
            primary=compiled_primary,
            control=compiled_control,
        )
