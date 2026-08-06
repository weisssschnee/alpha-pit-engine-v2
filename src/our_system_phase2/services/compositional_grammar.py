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
OPTIMIZER_GENE_SURFACE_VERSION = "cn_optimizer_skeleton_lane_gene_surface_v2"
PRODUCTION_EXTENSION_ID = "PRODUCTION"
MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID = (
    "MINUTE_STATIC_TYPED_TRANSFORMS_V4"
)
MINUTE_STATIC_TYPED_TRANSFORM_GENE_SURFACE_VERSION = (
    "cn_minute_static_typed_transform_gene_surface_v4"
)
MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID = (
    "MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_V5"
)
MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_GENE_SURFACE_VERSION = (
    "cn_minute_static_online_typed_grammar_gene_surface_v5"
)
MINUTE_STATIC_TYPED_TRANSFORM_IDS = (
    "ZSCORE",
    "ABS_ZSCORE",
    "SIGN",
)
FIRSTN_SUBSEQUENT_STATE_TRANSFORM_IDS = (
    "ZSCORE",
    "SIGN",
    "ABS",
)
MINUTE_STATIC_ONLINE_BINARY_OPERATOR_IDS = (
    "SUB",
    "SAFE_DIV",
    "MUL",
)
MINUTE_STATIC_ONLINE_OPERATOR_SKELETON_NAMES = {
    "SUB": "field_spread",
    "SAFE_DIV": "normalized_ratio",
    "MUL": "absolute_state_interaction",
}
MINUTE_STATIC_ONLINE_TRANSFORM_PAIRS = {
    "SUB": tuple(
        (left, right)
        for left in MINUTE_STATIC_TYPED_TRANSFORM_IDS
        for right in MINUTE_STATIC_TYPED_TRANSFORM_IDS
    ),
    "SAFE_DIV": tuple(
        (left, right)
        for left in MINUTE_STATIC_TYPED_TRANSFORM_IDS
        for right in MINUTE_STATIC_TYPED_TRANSFORM_IDS
    ),
    # The existing absolute-state skeleton owns a magnitude-times-sign
    # hypothesis. Canonical leg order removes its commutative mirror.
    "MUL": (("ABS_ZSCORE", "SIGN"),),
}
MINUTE_STATIC_TYPED_TRANSFORM_PAIRS = {
    "field_spread": (
        ("ZSCORE", "ZSCORE"),
        ("ZSCORE", "SIGN"),
        ("SIGN", "ZSCORE"),
        ("SIGN", "SIGN"),
    ),
    "normalized_ratio": tuple(
        (left, right)
        for left in MINUTE_STATIC_TYPED_TRANSFORM_IDS
        for right in MINUTE_STATIC_TYPED_TRANSFORM_IDS
    ),
    "absolute_state_interaction": (
        ("SIGN", "ABS_ZSCORE"),
        ("ABS_ZSCORE", "SIGN"),
    ),
    "dispersion_interaction": (
        ("ABS_ZSCORE", "ABS_ZSCORE"),
        ("ABS_ZSCORE", "ZSCORE"),
        ("ZSCORE", "ABS_ZSCORE"),
    ),
}
PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID = (
    "DISCLOSURE_PRE_EVENT_PAYLOAD_SIGN_V1"
)
PRE_EVENT_PAYLOAD_CSRANK_EXTENSION_ID = (
    "DISCLOSURE_PRE_EVENT_PAYLOAD_CSRANK_V1"
)
PRE_EVENT_PAYLOAD_ABS_EXTENSION_ID = (
    "DISCLOSURE_PRE_EVENT_PAYLOAD_ABS_V1"
)
_TARGETED_PRE_EVENT_EXTENSION_IDS = {
    PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID,
    PRE_EVENT_PAYLOAD_CSRANK_EXTENSION_ID,
    PRE_EVENT_PAYLOAD_ABS_EXTENSION_ID,
}
LEGACY_ROUTE_WIDE_GENE_ROUTES = (
    "INTRADAY_STATE_TRANSITION",
    "DISCLOSURE_EVENT",
    "SLOW_TEMPORAL_CHANGE",
)
OPTIMIZER_GENE_ROUTES = tuple(
    route_id for route_id in ROUTE_IDS if route_id != "BROAD_EVENT_FROZEN_ENTRY"
)
_FIELD_PAIR_SEPARATOR = "::"
_DISCLOSURE_PAYLOAD_SKELETON_NAMES = (
    "pre_event_path",
    "pre_event_signed_path",
    "pre_event_ranked_path",
    "pre_event_absolute_path",
    "post_maturity_state",
    "event_prior_condition",
    "event_window",
)


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


def route_clock_contract(route_id: str) -> tuple[str, str]:
    """Return the frozen route-skeleton observable and maturity clocks."""

    try:
        return _ROUTE_CLOCKS[str(route_id)]
    except KeyError as exc:
        raise ValueError(f"unknown typed route clock contract: {route_id}") from exc


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


def optimizer_typed_supply_extension_registry(
) -> dict[str, tuple[SkeletonSpec, ...]]:
    """Append-only economic hypotheses used only by optimizer gene lanes.

    Legacy attempt-index proposal mapping remains owned by
    :func:`skeleton_registry`; these extensions cannot remap historical asks.
    """

    declarations = {
        "DISCLOSURE_EVENT": (
            (
                "pre_event_signed_path",
                "the direction of the PIT payload before disclosure changes event interpretation",
                ("PRIMARY",),
                "dimensionless-path",
                "remove_pre_event_path",
                4,
            ),
            (
                "pre_event_ranked_path",
                "the cross-sectional standing of the PIT payload before disclosure changes event interpretation",
                ("PRIMARY",),
                "dimensionless-path",
                "remove_pre_event_path",
                4,
            ),
            (
                "pre_event_absolute_path",
                "the magnitude of the PIT payload before disclosure changes event interpretation",
                ("PRIMARY",),
                "dimensionless-path",
                "remove_pre_event_path",
                4,
            ),
        ),
        "MARKET_REGIME_CONDITION": (
            (
                "regime_magnitude_persistence",
                "payload magnitude behaves differently when a market regime persists",
                ("PRIMARY", "CONDITION_ONLY"),
                "dimensionless-gated",
                "remove_regime_persistence_gate",
                4,
            ),
            (
                "regime_direction_persistence",
                "payload direction behaves differently when a market regime persists",
                ("PRIMARY", "CONDITION_ONLY"),
                "dimensionless-gated",
                "remove_regime_persistence_gate",
                4,
            ),
        ),
    }
    output: dict[str, tuple[SkeletonSpec, ...]] = {}
    for route_id, rows in declarations.items():
        clock, maturity = _ROUTE_CLOCKS[route_id]
        output[route_id] = tuple(
            SkeletonSpec(
                skeleton_id=(
                    f"cn.comp.supply.v1.{route_id.lower()}.{name}"
                ),
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
            for name, hypothesis, roles, unit_signature, ablation, maximum_depth
            in rows
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
    if semantic or kind:
        return "canonical|" + "|".join(value for value in (semantic, kind) if value)
    return f"source_family|{field.source_family}"


def _declared_size(field: CapabilityField) -> bool:
    representation = _canonical_representation(field)
    return (
        str(representation.get("semantic_family") or "").lower() == "size"
        or str(field.source_family).lower() == "canonical_fundamental_size"
    )


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
    return normalized


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
        supply_extensions = optimizer_typed_supply_extension_registry()
        self._optimizer_skeletons = {
            route_id: (
                self._skeletons[route_id]
                + supply_extensions.get(route_id, ())
            )
            for route_id in self._skeletons
        }
        self._route_root_allowlist = self._validate_route_root_allowlist(
            route_root_allowlist
        )
        self._enforce_route_compatibility = bool(enforce_route_compatibility)
        self._pool_cache: dict[tuple[Any, ...], tuple[CapabilityField, ...]] = {}
        self._field_lookup_cache: dict[
            tuple[str, ...], dict[str, CapabilityField]
        ] = {}
        self._gene_space_cache: dict[
            tuple[str, str, str], dict[str, Any]
        ] = {}

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
        cache_key = ("payload", str(route_id))
        cached = self._pool_cache.get(cache_key)
        if cached is not None:
            return cached
        rows = self.registry.fields_for_route(
            route_id,
            entity_scopes=("STOCK",),
            field_roles=("primary", "interaction-only"),
        )
        rows = self._filter_proposal_roots(route_id, rows)
        if not rows:
            raise ValueError(f"FIELD_COVERAGE_BOTTLENECK: no payload fields on {route_id}")
        self._pool_cache[cache_key] = tuple(rows)
        return self._pool_cache[cache_key]

    def _route_pool(
        self,
        route_id: str,
        *,
        source_family: str | None = None,
        temporal_semantics: str | None = None,
        entity_scope: str | None = None,
        field_roles: Sequence[str] = (),
    ) -> tuple[CapabilityField, ...]:
        cache_key = (
            "route",
            str(route_id),
            str(source_family or ""),
            str(temporal_semantics or ""),
            str(entity_scope or ""),
            *tuple(map(str, field_roles)),
        )
        cached = self._pool_cache.get(cache_key)
        if cached is not None:
            return cached
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
        self._pool_cache[cache_key] = tuple(rows)
        return self._pool_cache[cache_key]

    def _gene_field(
        self,
        genes: Mapping[str, str],
        slot_name: str,
        pool: Sequence[CapabilityField],
    ) -> CapabilityField:
        selected = str(genes.get(slot_name) or "")
        pool_key = tuple(row.field_id for row in pool)
        by_id = self._field_lookup_cache.get(pool_key)
        if by_id is None:
            by_id = {row.field_id: row for row in pool}
            self._field_lookup_cache[pool_key] = by_id
        if selected not in by_id:
            raise ValueError(
                f"INVALID_CATEGORICAL_GENE:{slot_name}:{selected}"
            )
        return by_id[selected]

    @staticmethod
    def _field_pair_id(
        left: CapabilityField,
        right: CapabilityField,
    ) -> str:
        return (
            f"{left.field_id}{_FIELD_PAIR_SEPARATOR}{right.field_id}"
        )

    def _compatible_field_pair_ids(
        self,
        left_pool: Sequence[CapabilityField],
        right_pool: Sequence[CapabilityField] | None = None,
        *,
        predicate: Any | None = None,
    ) -> list[str]:
        right_rows = tuple(right_pool if right_pool is not None else left_pool)
        output = []
        for left in left_pool:
            for right in right_rows:
                if left.field_id == right.field_id:
                    continue
                if predicate is not None and not bool(predicate(left, right)):
                    continue
                output.append(self._field_pair_id(left, right))
        return output

    def _gene_field_pair(
        self,
        genes: Mapping[str, str],
        slot_name: str,
        left_pool: Sequence[CapabilityField],
        right_pool: Sequence[CapabilityField] | None = None,
        *,
        predicate: Any | None = None,
    ) -> tuple[CapabilityField, CapabilityField]:
        selected = str(genes.get(slot_name) or "")
        try:
            left_id, right_id = selected.split(_FIELD_PAIR_SEPARATOR, 1)
        except ValueError as exc:  # pragma: no cover - allowed IDs are internal.
            raise ValueError(
                f"INVALID_CATEGORICAL_GENE:{slot_name}:{selected}"
            ) from exc
        left = self._gene_field(
            {slot_name: left_id},
            slot_name,
            left_pool,
        )
        right = self._gene_field(
            {slot_name: right_id},
            slot_name,
            tuple(right_pool if right_pool is not None else left_pool),
        )
        if left.field_id == right.field_id or (
            predicate is not None and not bool(predicate(left, right))
        ):
            raise ValueError(
                f"INVALID_CATEGORICAL_GENE:{slot_name}:{selected}"
            )
        return left, right

    @staticmethod
    def _gene_value(
        genes: Mapping[str, str],
        slot_name: str,
        allowed: Sequence[str],
    ) -> str:
        selected = str(genes.get(slot_name) or "")
        if selected not in set(map(str, allowed)):
            raise ValueError(
                f"INVALID_CATEGORICAL_GENE:{slot_name}:{selected}"
            )
        return selected

    def _skeleton_lane_gene_space(
        self,
        route_id: str,
        skeleton: SkeletonSpec,
        *,
        formula_extension_id: str = PRODUCTION_EXTENSION_ID,
    ) -> dict[str, Any]:
        """Expose only active, compatibility-qualified genes for one skeleton.

        The registry route remains the scheduling authority.  ``skeleton_id`` is
        a fixed route-local generation mode so a categorical optimizer never
        spends population slots on inactive conditional dimensions.
        """

        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        transform_extension = formula_extension_id in {
            MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID,
            MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID,
        }
        online_grammar_extension = (
            formula_extension_id
            == MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID
        )
        surface_version = (
            MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_GENE_SURFACE_VERSION
            if online_grammar_extension
            else MINUTE_STATIC_TYPED_TRANSFORM_GENE_SURFACE_VERSION
            if transform_extension
            else OPTIMIZER_GENE_SURFACE_VERSION
        )
        categories: dict[str, list[str]] = {
            "skeleton_id": [skeleton.skeleton_id],
            "gene_surface_id": [surface_version],
        }
        constraint = "REGISTRY_ROUTE_AND_TYPED_COMPILER"
        allowed_transform_pairs: tuple[tuple[str, str], ...] = ()
        if route_id == "MINUTE_STATIC":
            pool = self._payload_pool(route_id)
            if name == "normalized_level":
                categories["primary_field_id"] = [
                    row.field_id for row in pool
                ]
            elif name in {
                "field_spread",
                "normalized_ratio",
                "absolute_state_interaction",
                "dispersion_interaction",
            }:
                categories["field_pair_id"] = (
                    self._compatible_field_pair_ids(pool)
                )
                if transform_extension:
                    if online_grammar_extension:
                        operator_by_skeleton = {
                            skeleton_name: operator_id
                            for operator_id, skeleton_name
                            in (
                                MINUTE_STATIC_ONLINE_OPERATOR_SKELETON_NAMES
                                .items()
                            )
                        }
                        if name not in operator_by_skeleton:
                            raise ValueError(
                                "OPTIMIZER_SKELETON_UNAVAILABLE:"
                                f"{skeleton.skeleton_id}:"
                                "MINUTE_ONLINE_GRAMMAR_ROOT_RULE_UNDECLARED"
                            )
                        categories["binary_operator_id"] = [
                            operator_by_skeleton[name]
                        ]
                        allowed_transform_pairs = (
                            MINUTE_STATIC_ONLINE_TRANSFORM_PAIRS[
                                operator_by_skeleton[name]
                            ]
                        )
                    else:
                        allowed_transform_pairs = (
                            MINUTE_STATIC_TYPED_TRANSFORM_PAIRS[name]
                        )
                    categories["left_transform_id"] = list(
                        dict.fromkeys(
                            left
                            for left, _ in allowed_transform_pairs
                        )
                    )
                    categories["right_transform_id"] = list(
                        dict.fromkeys(
                            right
                            for _, right in allowed_transform_pairs
                        )
                    )
                    constraint = (
                        "REGISTRY_ROUTE_TYPED_COMPILER_STREAMING_DEPTH4_"
                        + (
                            "ONLINE_ROOT_RULE_AND_TYPED_CHILDREN"
                            if online_grammar_extension
                            else "AND_PRODUCTION_TRANSFORM_PAIR_MASK"
                        )
                    )
            elif name == "cross_sectional_residual":
                pair_ids = self._compatible_field_pair_ids(
                    pool,
                    predicate=lambda left, right: (
                        _representation_family(left)
                        != _representation_family(right)
                    ),
                )
                if not pair_ids:
                    raise ValueError(
                        "OPTIMIZER_SKELETON_UNAVAILABLE:"
                        f"{skeleton.skeleton_id}:"
                        "REPRESENTATION_FAMILY_METADATA_UNRESOLVED"
                    )
                categories["field_pair_id"] = pair_ids
                constraint = "DISTINCT_REPRESENTATION_FAMILY"
            else:
                raise ValueError(
                    "OPTIMIZER_SKELETON_UNAVAILABLE:"
                    f"{skeleton.skeleton_id}:"
                    "MINUTE_LEG_CLASS_METADATA_UNRESOLVED"
                )
        elif route_id == "FIRSTN_PATH":
            firstn_pool = self._route_pool(
                route_id,
                source_family="firstN",
            )
            raw_pool = self._route_pool(
                route_id,
                source_family="raw_1min",
            )
            categories.update(
                {
                    "firstn_field_id": [
                        row.field_id for row in firstn_pool
                    ],
                    "raw_field_id": [row.field_id for row in raw_pool],
                    "window_id": (
                        ["3", "10"]
                        if name == "opening_multiscale_relation"
                        else ["3", "5", "10", "20"]
                    ),
                }
            )
            if name == "firstn_path_state":
                categories["subsequent_state_transform_id"] = list(
                    FIRSTN_SUBSEQUENT_STATE_TRANSFORM_IDS
                )
                constraint = (
                    "FIRSTN_AND_RAW_ROUTE_ROOTS_"
                    "DEPTH4_SUBSEQUENT_STATE_TRANSFORM"
                )
        elif route_id == "SLOW_CROSS_SECTIONAL_LEVEL":
            pool = self._payload_pool(route_id)
            if name in {
                "fundamental_level",
                "winsorized_level",
                "masked_normalized_level",
            }:
                categories["primary_field_id"] = [
                    row.field_id for row in pool
                ]
            elif name == "fundamental_ratio":
                categories["field_pair_id"] = (
                    self._compatible_field_pair_ids(
                        pool,
                        predicate=_unit_comparable_or_normalized,
                    )
                )
                constraint = "UNIT_COMPARABLE_OR_NORMALIZED"
            elif name == "cross_family_interaction":
                categories["field_pair_id"] = (
                    self._compatible_field_pair_ids(
                        pool,
                        predicate=lambda left, right: (
                            _representation_family(left)
                            != _representation_family(right)
                        ),
                    )
                )
                constraint = "DISTINCT_REPRESENTATION_FAMILY"
            elif name == "fundamental_residual":
                categories["field_pair_id"] = (
                    self._compatible_field_pair_ids(pool)
                )
            else:
                raise ValueError(
                    "OPTIMIZER_SKELETON_UNAVAILABLE:"
                    f"{skeleton.skeleton_id}:"
                    "DECLARED_SIZE_ROOT_UNAVAILABLE"
                )
        elif route_id == "SLOW_TEMPORAL_CHANGE":
            pool = self._payload_pool(route_id)
            if name in {"slope", "acceleration", "change_persistence"}:
                primary_pool = tuple(
                    row
                    for row in pool
                    if _declared_temporal_evolution(row)
                )
                categories["primary_field_id"] = [
                    row.field_id for row in primary_pool
                ]
                categories["window_id"] = ["2", "3", "5", "10"]
                constraint = "DECLARED_TEMPORAL_EVOLUTION"
            elif name == "reported_change":
                primary_pool = tuple(
                    row for row in pool if _declared_reported_change(row)
                )
                categories["primary_field_id"] = [
                    row.field_id for row in primary_pool
                ]
                constraint = "DECLARED_REPORTED_CHANGE"
            elif name == "change_level_interaction":
                categories["field_pair_id"] = (
                    self._compatible_field_pair_ids(pool)
                )
            elif name == "cross_change_interaction":
                evolving = tuple(
                    row
                    for row in pool
                    if _declared_temporal_evolution(row)
                )
                categories["field_pair_id"] = (
                    self._compatible_field_pair_ids(
                        evolving,
                        predicate=lambda left, right: (
                            _representation_family(left)
                            != _representation_family(right)
                        ),
                    )
                )
                categories["window_id"] = ["2", "3", "5", "10"]
                constraint = (
                    "DISTINCT_DECLARED_CHANGE_REPRESENTATION_FAMILY"
                )
            else:
                categories["primary_field_id"] = [
                    row.field_id for row in pool
                ]
                if name == "delta":
                    categories["window_id"] = ["2", "3", "5", "10"]
                elif name != "short_long_change":
                    raise ValueError(
                        "OPTIMIZER_SKELETON_UNAVAILABLE:"
                        f"{skeleton.skeleton_id}:UNHANDLED_ACTIVE_SLOTS"
                    )
        elif route_id == "DISCLOSURE_EVENT":
            event_pool = self._route_pool(
                route_id,
                temporal_semantics="DISCLOSURE_PULSE",
                entity_scope="STOCK",
                field_roles=("condition-only",),
            )
            categories["event_field_id"] = [
                row.field_id for row in event_pool
            ]
            if name in _DISCLOSURE_PAYLOAD_SKELETON_NAMES:
                payload_pool = tuple(
                    row
                    for row in self._payload_pool(route_id)
                    if row.temporal_semantics
                    in {
                        "DISCLOSURE_LEVEL_PAYLOAD",
                        "DISCLOSURE_CHANGE_PAYLOAD",
                    }
                )
                categories["payload_field_id"] = [
                    row.field_id for row in payload_pool
                ]
                constraint = (
                    "DISCLOSURE_PULSE_EVENT_PLUS_DISCLOSURE_PAYLOAD"
                )
            else:
                constraint = "DISCLOSURE_PULSE_EVENT_ONLY"
        elif route_id == "MARKET_REGIME_CONDITION":
            payload_pool = self._payload_pool(route_id)
            condition_pool = self._route_pool(
                route_id,
                entity_scope="MARKET",
                field_roles=("state-only", "condition-only", "primary"),
            )
            categories.update(
                {
                    "payload_field_id": [
                        row.field_id for row in payload_pool
                    ],
                    "condition_field_id": [
                        row.field_id for row in condition_pool
                    ],
                }
            )
            if name == "regime_multiscale_response":
                categories["window_id"] = ["3", "10"]
            elif name in {
                "regime_transition_response",
                "regime_maturity_payload",
                "regime_conditioned_path",
                "regime_persistence_gate",
                "regime_magnitude_persistence",
                "regime_direction_persistence",
            }:
                categories["window_id"] = ["3", "5", "10", "20"]
            constraint = "STOCK_PAYLOAD_PLUS_MARKET_CONDITION"
        elif route_id == "INTRADAY_STATE_TRANSITION":
            state_pool = tuple(
                row
                for row in self._route_pool(
                    route_id,
                    temporal_semantics="INTRADAY_DERIVED_STATE",
                    entity_scope="STOCK",
                    field_roles=("state-only",),
                )
                if str(
                    row.metadata.get("materialization_expression") or ""
                ).count("(")
                <= 1
            )
            payload_pool = self._route_pool(
                route_id,
                source_family="raw_1min",
                entity_scope="STOCK",
                field_roles=("primary", "interaction-only"),
            )
            categories["field_pair_id"] = (
                self._compatible_field_pair_ids(
                    state_pool,
                    payload_pool,
                    predicate=lambda state, payload: (
                        payload.field_id
                        not in {
                            str(value)
                            for value in state.metadata.get(
                                "source_fields", ()
                            )
                        }
                    ),
                )
            )
            if name == "multiscale_state_response":
                categories["window_id"] = ["3", "10"]
            elif name != "transition_liquidity_condition":
                categories["window_id"] = ["3", "5", "10", "20"]
            constraint = (
                "PAYLOAD_MUST_NOT_BE_A_PHYSICAL_SOURCE_OF_SELECTED_STATE"
            )
        else:  # pragma: no cover - caller rejects frozen/unknown routes.
            raise ValueError(f"OPTIMIZER_GENE_ROUTE_NOT_AUTHORIZED:{route_id}")

        empty = [slot for slot, values in categories.items() if not values]
        if empty:
            raise ValueError(
                "OPTIMIZER_SKELETON_UNAVAILABLE:"
                f"{skeleton.skeleton_id}:EMPTY_SLOTS={empty}"
            )
        none_semantics = {
            slot: (
                "FIXED_ROUTE_LOCAL_GENERATION_LANE"
                if slot == "skeleton_id"
                else (
                    "FIXED_GENE_SURFACE_VERSION"
                    if slot == "gene_surface_id"
                    else "NONE_NOT_PRESENT_ACTIVE_SLOT"
                )
            )
            for slot in categories
        }
        result = {
            "route_id": route_id,
            "surface_version": surface_version,
            "surface_mode": "SKELETON_LANE",
            "generation_mode": skeleton.skeleton_id,
            "ordered_categories_by_slot": categories,
            "none_semantics": none_semantics,
            "skeleton_compatibility": {
                skeleton.skeleton_id: {
                    "required_slots": list(categories),
                    "inactive_slots": [],
                    "constraint": constraint,
                }
            },
        }
        if allowed_transform_pairs:
            result["allowed_transform_pairs"] = [
                {
                    "left_transform_id": left,
                    "right_transform_id": right,
                }
                for left, right in allowed_transform_pairs
            ]
        return result

    def categorical_gene_lanes(self, route_id: str) -> dict[str, Any]:
        """Return all usable skeleton lanes without changing route authority."""

        if route_id not in OPTIMIZER_GENE_ROUTES:
            raise ValueError(
                f"OPTIMIZER_GENE_ROUTE_NOT_AUTHORIZED:{route_id}"
            )
        lanes: dict[str, dict[str, Any]] = {}
        blocked: dict[str, str] = {}
        for skeleton in self._optimizer_skeletons[route_id]:
            try:
                lanes[skeleton.skeleton_id] = (
                    self.categorical_gene_space(
                        route_id,
                        skeleton_id=skeleton.skeleton_id,
                    )
                )
            except ValueError as exc:
                if "OPTIMIZER_SKELETON_UNAVAILABLE:" not in str(exc):
                    raise
                blocked[skeleton.skeleton_id] = str(exc)
        if not lanes:
            raise ValueError(f"OPTIMIZER_ROUTE_HAS_NO_USABLE_LANES:{route_id}")
        return {
            "route_id": route_id,
            "surface_version": OPTIMIZER_GENE_SURFACE_VERSION,
            "top_level_scheduling_key": "unified_registry_route_id",
            "route_local_generation_mode": "skeleton_id",
            "lanes": lanes,
            "blocked_skeletons": blocked,
        }

    def categorical_gene_space(
        self,
        route_id: str,
        *,
        skeleton_id: str | None = None,
        formula_extension_id: str = PRODUCTION_EXTENSION_ID,
    ) -> dict[str, Any]:
        """Expose exact categorical slots backed by existing constructors.

        New large-search callers must request a skeleton lane.  The historical
        route-wide surfaces remain readable only for replaying the completed
        qualification and are not the recommended scale path.
        """

        if route_id not in OPTIMIZER_GENE_ROUTES:
            raise ValueError(
                f"OPTIMIZER_GENE_ROUTE_NOT_AUTHORIZED:{route_id}"
            )
        extension_id = str(formula_extension_id)
        if extension_id not in {
            PRODUCTION_EXTENSION_ID,
            MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID,
            MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID,
        }:
            raise ValueError(
                "TARGETED_FORMULA_EXTENSION_NOT_AUTHORIZED:"
                f"{route_id}:{extension_id}"
            )
        if (
            extension_id
            in {
                MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID,
                MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID,
            }
            and route_id != "MINUTE_STATIC"
        ):
            raise ValueError(
                "TARGETED_FORMULA_EXTENSION_NOT_AUTHORIZED:"
                f"{route_id}:{extension_id}"
            )
        cache_key = (
            str(route_id),
            str(skeleton_id or "__LEGACY_ROUTE_WIDE__"),
            extension_id,
        )
        cached = self._gene_space_cache.get(cache_key)
        if cached is not None:
            return cached
        if skeleton_id is not None:
            try:
                skeleton = next(
                    row
                    for row in self._optimizer_skeletons[route_id]
                    if row.skeleton_id == str(skeleton_id)
                )
            except StopIteration as exc:
                raise ValueError(
                    f"INVALID_CATEGORICAL_GENE:skeleton_id:{skeleton_id}"
                ) from exc
            if (
                extension_id
                in {
                    MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID,
                    MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID,
                }
                and skeleton.skeleton_id.rsplit(".", 1)[-1]
                not in (
                    MINUTE_STATIC_ONLINE_OPERATOR_SKELETON_NAMES.values()
                    if extension_id
                    == MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID
                    else MINUTE_STATIC_TYPED_TRANSFORM_PAIRS
                )
            ):
                raise ValueError(
                    "OPTIMIZER_SKELETON_UNAVAILABLE:"
                    f"{skeleton.skeleton_id}:"
                    "MINUTE_TYPED_TRANSFORM_DOMAIN_NOT_DECLARED"
                )
            result = self._skeleton_lane_gene_space(
                route_id,
                skeleton,
                formula_extension_id=extension_id,
            )
            self._gene_space_cache[cache_key] = result
            return result
        if extension_id != PRODUCTION_EXTENSION_ID:
            raise ValueError(
                "SKELETON_LANE_REQUIRED_FOR_TYPED_FORMULA_EXTENSION:"
                f"{route_id}:{extension_id}"
            )
        if route_id not in LEGACY_ROUTE_WIDE_GENE_ROUTES:
            raise ValueError(
                f"SKELETON_LANE_REQUIRED_FOR_OPTIMIZER_GENE_SPACE:{route_id}"
            )
        skeletons = self._skeletons[route_id]
        if route_id == "DISCLOSURE_EVENT":
            skeletons = tuple(
                row
                for row in skeletons
                if row.skeleton_id.rsplit(".", 1)[-1]
                in _DISCLOSURE_PAYLOAD_SKELETON_NAMES
            )
            event_pool = self._route_pool(
                route_id,
                temporal_semantics="DISCLOSURE_PULSE",
                entity_scope="STOCK",
                field_roles=("condition-only",),
            )
            payload_pool = tuple(
                row
                for row in self._payload_pool(route_id)
                if row.temporal_semantics
                in {"DISCLOSURE_LEVEL_PAYLOAD", "DISCLOSURE_CHANGE_PAYLOAD"}
            )
            categories = {
                "skeleton_id": [row.skeleton_id for row in skeletons],
                "event_field_id": [row.field_id for row in event_pool],
                "payload_field_id": [row.field_id for row in payload_pool],
            }
            none_semantics = {
                slot: "NONE_NOT_PRESENT_SLOT_REQUIRED_FOR_EVERY_AUTHORIZED_SKELETON"
                for slot in categories
            }
            compatibility = {
                row.skeleton_id: {
                    "required_slots": list(categories),
                    "inactive_slots": [],
                    "constraint": "DISCLOSURE_PULSE_EVENT_PLUS_DISCLOSURE_PAYLOAD",
                }
                for row in skeletons
            }
        elif route_id == "INTRADAY_STATE_TRANSITION":
            state_pool = tuple(
                row
                for row in self._route_pool(
                    route_id,
                    temporal_semantics="INTRADAY_DERIVED_STATE",
                    entity_scope="STOCK",
                    field_roles=("state-only",),
                )
                if str(
                    row.metadata.get("materialization_expression") or ""
                ).count("(")
                <= 1
            )
            payload_pool = self._route_pool(
                route_id,
                source_family="raw_1min",
                entity_scope="STOCK",
                field_roles=("primary", "interaction-only"),
            )
            categories = {
                "skeleton_id": [row.skeleton_id for row in skeletons],
                "state_field_id": [row.field_id for row in state_pool],
                "payload_field_id": [row.field_id for row in payload_pool],
                "window_id": ["3", "5", "10", "20"],
            }
            none_semantics = {
                slot: "NONE_NOT_PRESENT_SLOT_REQUIRED_FOR_EVERY_AUTHORIZED_SKELETON"
                for slot in categories
            }
            compatibility = {
                row.skeleton_id: {
                    "required_slots": list(categories),
                    "inactive_slots": [],
                    "constraint": (
                        "PAYLOAD_MUST_NOT_BE_A_PHYSICAL_SOURCE_OF_SELECTED_STATE"
                    ),
                }
                for row in skeletons
            }
        else:
            pool = self._payload_pool(route_id)
            categories = {
                "skeleton_id": [row.skeleton_id for row in skeletons],
                "primary_field_id": [row.field_id for row in pool],
                "secondary_offset_id": [str(value) for value in range(1, 8)],
                "window_id": ["2", "3", "5", "10"],
            }
            none_semantics = {
                "skeleton_id": (
                    "NONE_NOT_PRESENT_SLOT_REQUIRED_FOR_EVERY_AUTHORIZED_SKELETON"
                ),
                "primary_field_id": (
                    "NONE_NOT_PRESENT_SLOT_REQUIRED_FOR_EVERY_AUTHORIZED_SKELETON"
                ),
                "secondary_offset_id": (
                    "NO_NONE_CATEGORY;DETERMINISTIC_OFFSET_IS_INACTIVE_FOR_SINGLE_LEG_SKELETONS"
                ),
                "window_id": (
                    "NO_NONE_CATEGORY;WINDOW_IS_INACTIVE_FOR_REPORTED_CHANGE_AND_SHORT_LONG_CHANGE"
                ),
            }
            compatibility = {}
            for skeleton in skeletons:
                name = skeleton.skeleton_id.rsplit(".", 1)[-1]
                inactive = []
                if name not in {
                    "change_level_interaction",
                    "cross_change_interaction",
                }:
                    inactive.append("secondary_offset_id")
                if name in {"reported_change", "short_long_change"}:
                    inactive.append("window_id")
                compatibility[skeleton.skeleton_id] = {
                    "required_slots": [
                        slot for slot in categories if slot not in inactive
                    ],
                    "inactive_slots": inactive,
                    "constraint": (
                        "SLOPE_ACCELERATION_PERSISTENCE_REQUIRE_DECLARED_EVOLUTION;"
                        "REPORTED_CHANGE_REQUIRES_REPORTED_CHANGE_METADATA;"
                        "CROSS_CHANGE_REQUIRES_DISTINCT_EVOLVING_REPRESENTATION_FAMILIES"
                    ),
                }
        if any(len(values) < 2 for values in categories.values()):
            sizes = {slot: len(values) for slot, values in categories.items()}
            raise ValueError(
                f"CATCMA_GENE_SPACE_UNDERFILLED:{route_id}:{sizes}"
            )
        result = {
            "route_id": route_id,
            "surface_version": "cn_optimizer_legacy_route_wide_gene_surface_v1",
            "surface_mode": "LEGACY_ROUTE_WIDE_COMPATIBILITY_ONLY",
            "ordered_categories_by_slot": categories,
            "none_semantics": none_semantics,
            "skeleton_compatibility": compatibility,
        }
        self._gene_space_cache[cache_key] = result
        return result

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
        categorical_genes: Mapping[str, str] | None = None,
        formula_extension_id: str = PRODUCTION_EXTENSION_ID,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        pool = self._payload_pool("MINUTE_STATIC")
        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        extra: dict[str, Any] = {}
        if self._enforce_route_compatibility and name in {
            "price_volume_interaction",
            "liquidity_volatility_interaction",
        }:
            raise ValueError(
                "ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED: registered minute metadata "
                f"does not distinguish required legs for {skeleton.skeleton_id}"
            )
        elif categorical_genes is not None and "field_pair_id" in categorical_genes:
            predicate = (
                (
                    lambda first, second: (
                        _representation_family(first)
                        != _representation_family(second)
                    )
                )
                if name == "cross_sectional_residual"
                else None
            )
            left, right = self._gene_field_pair(
                categorical_genes,
                "field_pair_id",
                pool,
                predicate=predicate,
            )
            if predicate is not None:
                extra["compatibility_leg_roles"] = [
                    "distinct_representation_family",
                    "distinct_representation_family",
                ]
        elif categorical_genes is not None:
            left = self._gene_field(
                categorical_genes,
                "primary_field_id",
                pool,
            )
            right = left
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
            if (
                formula_extension_id
                in {
                    MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID,
                    MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID,
                }
            ):
                raise ValueError(
                    "MINUTE_TYPED_TRANSFORM_DOMAIN_NOT_DECLARED:"
                    + name
                )
            primary_expression = f"CSRank(ZScore({left_ref}))"
            control_expression = f"CSRank(Sign({left_ref}))"
            operator_family = "ZScore"
            fields = (left,)
        else:
            operator_family = "Arithmetic"
            fields = (left, right)
            if (
                formula_extension_id
                in {
                    MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID,
                    MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID,
                }
            ):
                if categorical_genes is None:
                    raise ValueError(
                        "MINUTE_TYPED_TRANSFORM_GENES_REQUIRED"
                    )
                left_transform = self._gene_value(
                    categorical_genes,
                    "left_transform_id",
                    MINUTE_STATIC_TYPED_TRANSFORM_IDS,
                )
                right_transform = self._gene_value(
                    categorical_genes,
                    "right_transform_id",
                    MINUTE_STATIC_TYPED_TRANSFORM_IDS,
                )
                transform_pair = (
                    left_transform,
                    right_transform,
                )
                online_grammar_extension = (
                    formula_extension_id
                    == MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID
                )
                binary_operator_id = ""
                if online_grammar_extension:
                    binary_operator_id = self._gene_value(
                        categorical_genes,
                        "binary_operator_id",
                        MINUTE_STATIC_ONLINE_BINARY_OPERATOR_IDS,
                    )
                    expected_name = (
                        MINUTE_STATIC_ONLINE_OPERATOR_SKELETON_NAMES[
                            binary_operator_id
                        ]
                    )
                    if name != expected_name:
                        raise ValueError(
                            "MINUTE_ONLINE_GRAMMAR_ROOT_RULE_MISMATCH:"
                            f"{name}:{binary_operator_id}"
                        )
                allowed_transform_pairs = (
                    MINUTE_STATIC_ONLINE_TRANSFORM_PAIRS[
                        binary_operator_id
                    ]
                    if online_grammar_extension
                    else MINUTE_STATIC_TYPED_TRANSFORM_PAIRS.get(name, ())
                )
                if transform_pair not in allowed_transform_pairs:
                    raise ValueError(
                        "MINUTE_TYPED_TRANSFORM_PAIR_FORBIDDEN:"
                        f"{name}:{left_transform}:{right_transform}"
                    )

                def render_leg(
                    transform_id: str,
                    field_ref: str,
                ) -> str:
                    if transform_id == "ZSCORE":
                        return f"ZScore({field_ref})"
                    if transform_id == "ABS_ZSCORE":
                        return f"Abs(ZScore({field_ref}))"
                    if transform_id == "SIGN":
                        return f"Sign({field_ref})"
                    raise ValueError(
                        "MINUTE_TYPED_TRANSFORM_UNKNOWN:"
                        + transform_id
                    )

                left_leg = render_leg(left_transform, left_ref)
                right_leg = render_leg(right_transform, right_ref)
                # The right source remains in the control validity mask, while
                # its transform and the production interaction are ablated.
                # This keeps the control within the declared depth-4 surface.
                control_expression = (
                    f"CSRank(Add({left_leg},Mul(0,{right_ref})))"
                )
                if online_grammar_extension:
                    if binary_operator_id == "MUL":
                        left_key = (
                            MINUTE_STATIC_TYPED_TRANSFORM_IDS.index(
                                left_transform
                            ),
                            left.field_id,
                        )
                        right_key = (
                            MINUTE_STATIC_TYPED_TRANSFORM_IDS.index(
                                right_transform
                            ),
                            right.field_id,
                        )
                        if left_key >= right_key:
                            raise ValueError(
                                "MINUTE_ONLINE_GRAMMAR_COMMUTATIVE_"
                                "NONCANONICAL:"
                                f"{left_transform}:{left.field_id}:"
                                f"{right_transform}:{right.field_id}"
                            )
                if (
                    binary_operator_id == "SUB"
                    or not online_grammar_extension
                    and name == "field_spread"
                ):
                    primary_expression = (
                        f"CSRank(Sub({left_leg},{right_leg}))"
                    )
                elif (
                    binary_operator_id == "SAFE_DIV"
                    or not online_grammar_extension
                    and name == "normalized_ratio"
                ):
                    primary_expression = (
                        f"CSRank(SafeDiv({left_leg},{right_leg},0.05))"
                    )
                elif (
                    binary_operator_id == "MUL"
                    or not online_grammar_extension
                    and name == "absolute_state_interaction"
                ):
                    primary_expression = (
                        f"CSRank(Mul({left_leg},{right_leg}))"
                    )
                elif not online_grammar_extension and (
                    name == "dispersion_interaction"
                ):
                    primary_expression = (
                        f"CSRank(Sub({left_leg},{right_leg}))"
                    )
                else:
                    raise ValueError(
                        "MINUTE_TYPED_TRANSFORM_DOMAIN_NOT_DECLARED:"
                        + name
                    )
                extra.update(
                    {
                        "left_transform_id": left_transform,
                        "right_transform_id": right_transform,
                        "typed_transform_authority": (
                            "CompositionalGrammarV2."
                            + (
                                "MINUTE_STATIC_ONLINE_TRANSFORM_PAIRS"
                                if online_grammar_extension
                                else "MINUTE_STATIC_TYPED_TRANSFORM_PAIRS"
                            )
                        ),
                    }
                )
                if online_grammar_extension:
                    extra.update(
                        {
                            "binary_operator_id": binary_operator_id,
                            "online_grammar_rule_authority": (
                                "CompositionalGrammarV2."
                                "MINUTE_STATIC_ONLINE_BINARY_OPERATOR_IDS"
                            ),
                        }
                    )
            else:
                control_expression = f"CSRank(Add(ZScore({left_ref}),Mul(0,ZScore({right_ref}))))"
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
        categorical_genes: Mapping[str, str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        firstn_pool = self._route_pool("FIRSTN_PATH", source_family="firstN")
        raw_pool = self._route_pool("FIRSTN_PATH", source_family="raw_1min")
        if categorical_genes is not None:
            firstn = self._gene_field(
                categorical_genes,
                "firstn_field_id",
                firstn_pool,
            )
            raw = self._gene_field(
                categorical_genes,
                "raw_field_id",
                raw_pool,
            )
        else:
            firstn = _pick(
                firstn_pool,
                attempt_index,
                seed,
                skeleton.skeleton_id + ":firstn",
            )
            raw = _pick(
                raw_pool,
                attempt_index,
                seed,
                skeleton.skeleton_id + ":raw",
            )
        firstn_ref, raw_ref = f"${firstn.field_id}", f"${raw.field_id}"
        if categorical_genes is not None:
            window = (
                int(
                    self._gene_value(
                        categorical_genes,
                        "window_id",
                        ("3", "5", "10", "20"),
                    )
                )
                if "window_id" in categorical_genes
                else 3
            )
        else:
            window = _pick_value(
                (3, 5, 10, 20),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":window",
            )
        short, long = (3, 10) if window <= 5 else (5, 20)
        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        subsequent_state_transform = "ZSCORE"
        if (
            name == "firstn_path_state"
            and categorical_genes is not None
        ):
            subsequent_state_transform = self._gene_value(
                categorical_genes,
                "subsequent_state_transform_id",
                FIRSTN_SUBSEQUENT_STATE_TRANSFORM_IDS,
            )
        control_expression = (
            f"CSRank(Add(ZScore({firstn_ref}),Mul(0,Delta({raw_ref},{window}))))"
        )
        if name == "firstn_path_state":
            subsequent_state = {
                "ZSCORE": f"ZScore(Delta({raw_ref},{window}))",
                "SIGN": f"Sign(Delta({raw_ref},{window}))",
                "ABS": f"Abs(Delta({raw_ref},{window}))",
            }[subsequent_state_transform]
            primary_expression = (
                f"CSRank(Mul(ZScore({firstn_ref}),"
                f"{subsequent_state}))"
            )
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
            extra=(
                {
                    "subsequent_state_transform_id": (
                        subsequent_state_transform
                    )
                }
                if name == "firstn_path_state"
                else None
            ),
        )

    def _slow_level_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
        categorical_genes: Mapping[str, str] | None = None,
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
        if categorical_genes is not None and "field_pair_id" in categorical_genes:
            if name == "fundamental_ratio":
                predicate = _unit_comparable_or_normalized
                extra["compatibility_leg_roles"] = [
                    "unit_comparable",
                    "unit_comparable",
                ]
            elif name == "cross_family_interaction":
                predicate = lambda first, second: (
                    _representation_family(first)
                    != _representation_family(second)
                )
                extra["compatibility_leg_roles"] = [
                    "distinct_source_or_representation_family",
                    "distinct_source_or_representation_family",
                ]
            else:
                predicate = None
            left, right = self._gene_field_pair(
                categorical_genes,
                "field_pair_id",
                pool,
                predicate=predicate,
            )
        elif categorical_genes is not None:
            left = self._gene_field(
                categorical_genes,
                "primary_field_id",
                pool,
            )
            right = left
        elif self._enforce_route_compatibility and name == "fundamental_ratio":
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
        categorical_genes: Mapping[str, str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        pool = self._payload_pool("SLOW_TEMPORAL_CHANGE")
        name = skeleton.skeleton_id.rsplit(".", 1)[-1]
        extra: dict[str, Any] = {}
        if (
            categorical_genes is not None
            and "field_pair_id" in categorical_genes
        ):
            if name == "cross_change_interaction":
                evolving = tuple(
                    row
                    for row in pool
                    if _declared_temporal_evolution(row)
                )
                left, right = self._gene_field_pair(
                    categorical_genes,
                    "field_pair_id",
                    evolving,
                    predicate=lambda first, second: (
                        _representation_family(first)
                        != _representation_family(second)
                    ),
                )
                extra["compatibility_leg_roles"] = [
                    "distinct_change_representation_family",
                    "distinct_change_representation_family",
                ]
            else:
                left, right = self._gene_field_pair(
                    categorical_genes,
                    "field_pair_id",
                    pool,
                )
        elif categorical_genes is not None:
            left = self._gene_field(
                categorical_genes, "primary_field_id", pool
            )
            if "secondary_offset_id" in categorical_genes:
                offset = int(
                    self._gene_value(
                        categorical_genes,
                        "secondary_offset_id",
                        tuple(str(value) for value in range(1, 8)),
                    )
                )
            else:
                offset = 1
            if self._enforce_route_compatibility and name in {
                "slope",
                "acceleration",
                "change_persistence",
            }:
                if not _declared_temporal_evolution(left):
                    raise ValueError(
                        "ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED: selected "
                        f"field lacks temporal-evolution metadata for {skeleton.skeleton_id}"
                    )
                extra["compatibility_leg_roles"] = [
                    "declared_temporal_evolution"
                ]
            elif self._enforce_route_compatibility and name == "reported_change":
                if not _declared_reported_change(left):
                    raise ValueError(
                        "ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED: selected "
                        f"field lacks reported-change metadata for {skeleton.skeleton_id}"
                    )
                extra["compatibility_leg_roles"] = [
                    "declared_reported_change"
                ]
            if (
                self._enforce_route_compatibility
                and name == "cross_change_interaction"
            ):
                if not _declared_temporal_evolution(left):
                    raise ValueError(
                        "ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED: cross-change "
                        "primary lacks temporal-evolution metadata"
                    )
                right_pool = tuple(
                    row
                    for row in pool
                    if row.field_id != left.field_id
                    and _declared_temporal_evolution(row)
                    and _representation_family(row)
                    != _representation_family(left)
                )
                extra["compatibility_leg_roles"] = [
                    "distinct_change_representation_family",
                    "distinct_change_representation_family",
                ]
            else:
                right_pool = tuple(
                    row for row in pool if row.field_id != left.field_id
                )
            if not right_pool:
                raise ValueError(
                    "ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED: no compatible "
                    f"secondary field for {skeleton.skeleton_id}"
                )
            right = right_pool[(offset - 1) % len(right_pool)]
        elif self._enforce_route_compatibility and name in {
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
        if categorical_genes is not None:
            window = (
                int(
                    self._gene_value(
                        categorical_genes,
                        "window_id",
                        ("2", "3", "5", "10"),
                    )
                )
                if "window_id" in categorical_genes
                else 2
            )
        else:
            window = _pick_value(
                (2, 3, 5, 10),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":window",
            )
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
        categorical_genes: Mapping[str, str] | None = None,
        formula_extension_id: str = PRODUCTION_EXTENSION_ID,
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
        if categorical_genes is not None:
            event = self._gene_field(
                categorical_genes, "event_field_id", event_pool
            )
            payload = (
                self._gene_field(
                    categorical_genes,
                    "payload_field_id",
                    payload_pool,
                )
                if "payload_field_id" in categorical_genes
                else None
            )
        else:
            event = _pick(
                event_pool, attempt_index, seed, skeleton.skeleton_id + ":event"
            )
            payload = _pick(
                payload_pool,
                attempt_index,
                seed,
                skeleton.skeleton_id + ":payload",
            )
        event_ref = f"${event.field_id}"
        payload_ref = f"${payload.field_id}" if payload is not None else ""
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
            if payload is None:  # pragma: no cover - lane schema owns this.
                raise ValueError("DISCLOSURE_PAYLOAD_GENE_REQUIRED")
            if formula_extension_id == PRODUCTION_EXTENSION_ID:
                primary_expression = (
                    f"EventWindow({payload_ref},{event_ref},5,0)"
                )
                control_expression = (
                    f"Add({payload_ref},Mul(0,TimeSince({event_ref})))"
                )
            elif (
                formula_extension_id
                == PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID
            ):
                primary_expression = (
                    f"EventWindow(Sign({payload_ref}),"
                    f"{event_ref},5,0)"
                )
                control_expression = (
                    f"Add(Sign({payload_ref}),"
                    f"Mul(0,TimeSince({event_ref})))"
                )
            elif (
                formula_extension_id
                == PRE_EVENT_PAYLOAD_CSRANK_EXTENSION_ID
            ):
                primary_expression = (
                    f"EventWindow(CSRank({payload_ref}),"
                    f"{event_ref},5,0)"
                )
                control_expression = (
                    f"Add(CSRank({payload_ref}),"
                    f"Mul(0,TimeSince({event_ref})))"
                )
            elif (
                formula_extension_id
                == PRE_EVENT_PAYLOAD_ABS_EXTENSION_ID
            ):
                primary_expression = (
                    f"EventWindow(Abs({payload_ref}),"
                    f"{event_ref},5,0)"
                )
                control_expression = (
                    f"Add(Abs({payload_ref}),"
                    f"Mul(0,TimeSince({event_ref})))"
                )
            else:
                raise ValueError(
                    "TARGETED_FORMULA_EXTENSION_NOT_AUTHORIZED:"
                    f"{formula_extension_id}"
                )
            family = "PreEventPath"
            fields = (event, payload)
        elif name in {
            "pre_event_signed_path",
            "pre_event_ranked_path",
            "pre_event_absolute_path",
        }:
            if payload is None:  # pragma: no cover - lane schema owns this.
                raise ValueError("DISCLOSURE_PAYLOAD_GENE_REQUIRED")
            payload_expression = {
                "pre_event_signed_path": f"Sign({payload_ref})",
                "pre_event_ranked_path": f"CSRank({payload_ref})",
                "pre_event_absolute_path": f"Abs({payload_ref})",
            }[name]
            primary_expression = (
                f"EventWindow({payload_expression},{event_ref},5,0)"
            )
            control_expression = (
                f"Add({payload_expression},"
                f"Mul(0,TimeSince({event_ref})))"
            )
            family = "PreEventPath"
            fields = (event, payload)
        elif name == "post_maturity_state":
            if payload is None:  # pragma: no cover - lane schema owns this.
                raise ValueError("DISCLOSURE_PAYLOAD_GENE_REQUIRED")
            primary_expression = f"EventWindow({payload_ref},{event_ref},0,5)"
            control_expression = f"Add({payload_ref},Mul(0,TimeSince({event_ref})))"
            family = "PostMaturityOutcome"
            fields = (event, payload)
        elif name == "event_prior_condition":
            if payload is None:  # pragma: no cover - lane schema owns this.
                raise ValueError("DISCLOSURE_PAYLOAD_GENE_REQUIRED")
            primary_expression = f"Mul(EventCount({event_ref},1),Sign({payload_ref}))"
            control_expression = f"Add({payload_ref},Mul(0,TimeSince({event_ref})))"
            family = "EventWindow"
            fields = (event, payload)
        elif name == "repeated_event_suppression":
            primary_expression = f"SafeDiv(EventCount({event_ref},5),Add(EventCount({event_ref},20),1),1)"
            family = "EventCount"
        elif name == "event_window":
            if payload is None:  # pragma: no cover - lane schema owns this.
                raise ValueError("DISCLOSURE_PAYLOAD_GENE_REQUIRED")
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
            extra={
                "episode_policy": "UNIQUE_DISCLOSURE_EPISODE",
                **(
                    {"extension_id": formula_extension_id}
                    if name == "pre_event_path"
                    else {}
                ),
            },
        )

    def _market_regime_pair(
        self,
        skeleton: SkeletonSpec,
        attempt_index: int,
        seed: int,
        categorical_genes: Mapping[str, str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        payload_pool = self._payload_pool("MARKET_REGIME_CONDITION")
        condition_pool = self._route_pool(
            "MARKET_REGIME_CONDITION",
            entity_scope="MARKET",
            field_roles=("state-only", "condition-only", "primary"),
        )
        if categorical_genes is not None:
            payload = self._gene_field(
                categorical_genes,
                "payload_field_id",
                payload_pool,
            )
            condition = self._gene_field(
                categorical_genes,
                "condition_field_id",
                condition_pool,
            )
        else:
            payload = _pick(
                payload_pool,
                attempt_index,
                seed,
                skeleton.skeleton_id + ":payload",
            )
            condition = _pick(
                condition_pool,
                attempt_index,
                seed,
                skeleton.skeleton_id + ":condition",
            )
        payload_ref, condition_ref = f"${payload.field_id}", f"${condition.field_id}"
        if categorical_genes is not None:
            window = (
                int(
                    self._gene_value(
                        categorical_genes,
                        "window_id",
                        ("3", "5", "10", "20"),
                    )
                )
                if "window_id" in categorical_genes
                else 3
            )
        else:
            window = _pick_value(
                (3, 5, 10, 20),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":window",
            )
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
        elif name == "regime_magnitude_persistence":
            primary_expression = (
                f"CSRank(Mul(Abs(ZScore({payload_ref})),"
                f"Persistence(Positive({condition_ref}),{window})))"
            )
            control_expression = (
                f"CSRank(Add(Abs(ZScore({payload_ref})),"
                f"Mul(0,{condition_ref})))"
            )
            family = "RegimeInteraction"
        elif name == "regime_direction_persistence":
            primary_expression = (
                f"CSRank(Mul(Sign({payload_ref}),"
                f"Persistence(Positive({condition_ref}),{window})))"
            )
            control_expression = (
                f"CSRank(Add(Sign({payload_ref}),"
                f"Mul(0,{condition_ref})))"
            )
            family = "RegimeInteraction"
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
        categorical_genes: Mapping[str, str] | None = None,
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
        if (
            categorical_genes is not None
            and "field_pair_id" in categorical_genes
        ):
            state, payload = self._gene_field_pair(
                categorical_genes,
                "field_pair_id",
                state_pool,
                payload_pool,
                predicate=lambda selected_state, selected_payload: (
                    selected_payload.field_id
                    not in {
                        str(value)
                        for value in selected_state.metadata.get(
                            "source_fields", ()
                        )
                    }
                ),
            )
        else:
            state = (
                self._gene_field(
                    categorical_genes,
                    "state_field_id",
                    state_pool,
                )
                if categorical_genes is not None
                else _pick(
                    state_pool,
                    attempt_index,
                    seed,
                    skeleton.skeleton_id + ":state",
                )
            )
            source_ids = tuple(
                str(value)
                for value in state.metadata.get("source_fields", ())
            )
            if categorical_genes is not None:
                payload = self._gene_field(
                    categorical_genes,
                    "payload_field_id",
                    payload_pool,
                )
                if payload.field_id in source_ids:
                    raise ValueError(
                        "ROUTE_LOCAL_COMPATIBILITY_UNRESOLVED: selected "
                        "payload is a physical source of the selected state"
                    )
            else:
                payload = _pick_excluding(
                    payload_pool,
                    source_ids,
                    attempt_index,
                    seed,
                    skeleton.skeleton_id + ":payload",
                )
        state_expression = str(
            state.metadata.get("materialization_expression") or ""
        )
        source_ids = tuple(
            str(value) for value in state.metadata.get("source_fields", ())
        )
        source_fields = tuple(
            self.registry.resolve(field_id) for field_id in source_ids
        )
        payload_ref = f"${payload.field_id}"
        if categorical_genes is not None:
            window = (
                int(
                    self._gene_value(
                        categorical_genes,
                        "window_id",
                        ("3", "5", "10", "20"),
                    )
                )
                if "window_id" in categorical_genes
                else 3
            )
        else:
            window = _pick_value(
                (3, 5, 10, 20),
                attempt_index,
                seed,
                skeleton.skeleton_id + ":window",
            )
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

    def propose_from_categorical_genes(
        self,
        route_id: str,
        *,
        genes: Mapping[str, str],
        formula_extension_id: str = PRODUCTION_EXTENSION_ID,
    ) -> GeneratedCompositionalPair:
        """Construct one pair through the existing Grammar from exact genes."""

        extension_id = str(formula_extension_id)
        if extension_id != PRODUCTION_EXTENSION_ID and not (
            (
                route_id == "DISCLOSURE_EVENT"
                and str(genes.get("skeleton_id") or "").endswith(
                    ".pre_event_path"
                )
                and extension_id in _TARGETED_PRE_EVENT_EXTENSION_IDS
            )
            or (
                route_id == "MINUTE_STATIC"
                and extension_id
                in {
                    MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID,
                    MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID,
                }
            )
        ):
            raise ValueError(
                "TARGETED_FORMULA_EXTENSION_NOT_AUTHORIZED:"
                f"{route_id}:{extension_id}"
            )

        supplied_slots = tuple(map(str, genes))
        gene_space: dict[str, Any] | None = None
        if route_id in LEGACY_ROUTE_WIDE_GENE_ROUTES:
            legacy = self.categorical_gene_space(route_id)
            if set(supplied_slots) == set(
                legacy["ordered_categories_by_slot"]
            ):
                gene_space = legacy
        if gene_space is None:
            selected_skeleton = str(genes.get("skeleton_id") or "")
            if not selected_skeleton:
                raise ValueError(
                    "CATEGORICAL_GENE_SLOT_MISMATCH:"
                    "skeleton_id is required for a route-local gene lane"
                )
            gene_space = self.categorical_gene_space(
                route_id,
                skeleton_id=selected_skeleton,
                formula_extension_id=(
                    extension_id
                    if extension_id in {
                        MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID,
                        MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID,
                    }
                    else PRODUCTION_EXTENSION_ID
                ),
            )
        categories = dict(gene_space["ordered_categories_by_slot"])
        expected_slots = tuple(categories)
        if set(supplied_slots) != set(expected_slots):
            raise ValueError(
                "CATEGORICAL_GENE_SLOT_MISMATCH:"
                f"expected={expected_slots}:actual={supplied_slots}"
            )
        normalized = {
            slot: self._gene_value(genes, slot, categories[slot])
            for slot in expected_slots
        }
        skeleton_id = normalized["skeleton_id"]
        compatible = set(
            map(str, gene_space["skeleton_compatibility"])
        )
        if skeleton_id not in compatible:
            raise ValueError(
                f"INVALID_CATEGORICAL_GENE:skeleton_id:{skeleton_id}"
            )
        skeleton = next(
            row
            for row in self._optimizer_skeletons[route_id]
            if row.skeleton_id == skeleton_id
        )
        is_skeleton_lane = (
            gene_space.get("surface_mode") == "SKELETON_LANE"
        )
        gene_hash_payload = {
            "grammar": GRAMMAR_VERSION,
            "route_id": route_id,
            "genes": normalized,
            "formula_extension_id": extension_id,
        }
        if is_skeleton_lane:
            gene_hash_payload["gene_surface"] = str(
                gene_space.get("surface_version")
                or OPTIMIZER_GENE_SURFACE_VERSION
            )
        gene_hash = stable_hash(gene_hash_payload)
        attempt_index = int(gene_hash[:12], 16)
        seed = int(gene_hash[12:24], 16)
        if route_id == "SLOW_TEMPORAL_CHANGE":
            primary, control = self._slow_change_pair(
                skeleton,
                attempt_index,
                seed,
                categorical_genes=normalized,
            )
        elif route_id == "DISCLOSURE_EVENT":
            primary, control = self._disclosure_pair(
                skeleton,
                attempt_index,
                seed,
                categorical_genes=normalized,
                formula_extension_id=extension_id,
            )
        elif route_id == "INTRADAY_STATE_TRANSITION":
            primary, control = self._intraday_state_pair(
                skeleton,
                attempt_index,
                seed,
                categorical_genes=normalized,
            )
        elif route_id == "MINUTE_STATIC":
            primary, control = self._minute_pair(
                skeleton,
                attempt_index,
                seed,
                categorical_genes=normalized,
                formula_extension_id=extension_id,
            )
        elif route_id == "FIRSTN_PATH":
            primary, control = self._firstn_pair(
                skeleton,
                attempt_index,
                seed,
                categorical_genes=normalized,
            )
        elif route_id == "SLOW_CROSS_SECTIONAL_LEVEL":
            primary, control = self._slow_level_pair(
                skeleton,
                attempt_index,
                seed,
                categorical_genes=normalized,
            )
        elif route_id == "MARKET_REGIME_CONDITION":
            primary, control = self._market_regime_pair(
                skeleton,
                attempt_index,
                seed,
                categorical_genes=normalized,
            )
        else:  # guarded by categorical_gene_space
            raise ValueError(
                f"OPTIMIZER_GENE_ROUTE_NOT_AUTHORIZED:{route_id}"
            )
        gene_binding = {
            "categorical_gene_construction": (
                "AUTHORITATIVE_GRAMMAR_V2_SKELETON_LANE"
                if is_skeleton_lane
                else "AUTHORITATIVE_GRAMMAR_V2"
            ),
            "categorical_genes": normalized,
            "categorical_gene_hash": gene_hash,
            "extension_id": extension_id,
        }
        if is_skeleton_lane:
            gene_binding.update(
                {
                    "categorical_gene_surface_version": str(
                        gene_space.get("surface_version") or ""
                    ),
                    "generation_mode": skeleton.skeleton_id,
                }
            )
        primary = {**primary, **gene_binding}
        control = {**control, **gene_binding}
        compiled_primary = {
            **primary,
            **self.compiler.compile(primary).to_dict(),
        }
        compiled_control = {
            **control,
            **self.compiler.compile(control).to_dict(),
        }
        return GeneratedCompositionalPair(
            skeleton=skeleton,
            primary=compiled_primary,
            control=compiled_control,
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
