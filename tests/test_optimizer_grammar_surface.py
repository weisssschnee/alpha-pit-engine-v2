from __future__ import annotations

from functools import lru_cache
from itertools import product
from math import prod
from pathlib import Path

from our_system_phase2.services.catcma_search_adapter import ExactGeneSemantics
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    RegistryDrivenGenerator,
    load_development_discovery_root_authority,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
DISCOVERY_CONTRACT = (
    REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"
)
PRIMARY_ROUTES = (
    "MINUTE_STATIC",
    "FIRSTN_PATH",
    "SLOW_CROSS_SECTIONAL_LEVEL",
    "SLOW_TEMPORAL_CHANGE",
    "DISCLOSURE_EVENT",
    "MARKET_REGIME_CONDITION",
    "INTRADAY_STATE_TRANSITION",
)
EXPECTED_LANE_COUNTS = {
    "MINUTE_STATIC": (5, 3),
    "FIRSTN_PATH": (8, 0),
    "SLOW_CROSS_SECTIONAL_LEVEL": (6, 2),
    "SLOW_TEMPORAL_CHANGE": (8, 0),
    "DISCLOSURE_EVENT": (11, 0),
    "MARKET_REGIME_CONDITION": (10, 0),
    "INTRADAY_STATE_TRANSITION": (8, 0),
}
EXPECTED_COMPATIBLE_PHENOTYPE_CEILINGS = {
    "MINUTE_STATIC": 126,
    "FIRSTN_PATH": 4788,
    "SLOW_CROSS_SECTIONAL_LEVEL": 2545,
    "SLOW_TEMPORAL_CHANGE": 62354,
    "DISCLOSURE_EVENT": 2116,
    "MARKET_REGIME_CONDITION": 2784,
    "INTRADAY_STATE_TRANSITION": 270,
}


@lru_cache(maxsize=1)
def _generator() -> tuple[
    RegistryDrivenGenerator,
    UnifiedCapabilityRegistry,
    dict[str, object],
]:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    authority = load_development_discovery_root_authority(
        DISCOVERY_CONTRACT,
        registry=registry,
    )
    return (
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            enforce_route_compatibility=True,
            route_root_allowlist=authority["route_root_allowlists"],
        ),
        registry,
        authority,
    )


def _lane_genes(space: dict[str, object], *, last: bool) -> dict[str, str]:
    categories = dict(space["ordered_categories_by_slot"])
    return {
        slot: str(values[-1] if last else values[0])
        for slot, values in categories.items()
    }


def _enumerate_genes(space: dict[str, object]):
    categories = dict(space["ordered_categories_by_slot"])
    slots = tuple(categories)
    for values in product(*(categories[slot] for slot in slots)):
        yield dict(zip(slots, map(str, values)))


def test_skeleton_lane_surface_covers_every_primary_route_without_widening_roots() -> None:
    generator, registry, authority = _generator()
    total_ceiling = 0

    for route_id in PRIMARY_ROUTES:
        manifest = generator.categorical_gene_lanes(route_id)
        lanes = dict(manifest["lanes"])
        blocked = dict(manifest["blocked_skeletons"])
        assert (len(lanes), len(blocked)) == EXPECTED_LANE_COUNTS[route_id]
        assert manifest["top_level_scheduling_key"] == (
            "unified_registry_route_id"
        )
        route_ceiling = 0
        for skeleton_id, space in lanes.items():
            categories = dict(space["ordered_categories_by_slot"])
            assert categories["skeleton_id"] == [skeleton_id]
            assert "secondary_offset_id" not in categories
            assert not space["skeleton_compatibility"][skeleton_id][
                "inactive_slots"
            ]
            route_ceiling += prod(len(values) for values in categories.values())
            for slot, values in categories.items():
                if slot == "field_pair_id":
                    field_ids = {
                        field_id
                        for value in values
                        for field_id in str(value).split("::")
                    }
                elif slot.endswith("_field_id"):
                    field_ids = set(map(str, values))
                else:
                    continue
                assert field_ids.issubset(
                    set(authority["route_root_allowlists"][route_id])
                )
                assert all(
                    registry.resolve(field_id).search_eligible
                    and route_id
                    in registry.resolve(field_id).allowed_routes
                    for field_id in field_ids
                )
        assert (
            route_ceiling
            == EXPECTED_COMPATIBLE_PHENOTYPE_CEILINGS[route_id]
        )
        total_ceiling += route_ceiling

    assert total_ceiling == 74983


def test_every_open_lane_endpoint_compiles_as_one_legal_matched_pair() -> None:
    generator, registry, authority = _generator()

    for route_id in PRIMARY_ROUTES:
        manifest = generator.categorical_gene_lanes(route_id)
        for space in manifest["lanes"].values():
            for last in (False, True):
                pair = generator.propose_categorical_genes(
                    route_id,
                    genes=_lane_genes(space, last=last),
                )
                assert pair.candidate["legal"] is True
                assert pair.control["legal"] is True
                assert set(pair.candidate["declared_field_ids"]).issubset(
                    set(authority["route_root_allowlists"][route_id])
                )
                assert pair.candidate["generation_mode"] == (
                    pair.candidate["skeleton_id"]
                )
                assert pair.candidate["categorical_gene_construction"] == (
                    "AUTHORITATIVE_GRAMMAR_V2_SKELETON_LANE"
                )
                for field_id in pair.candidate["declared_field_ids"]:
                    assert registry.resolve(str(field_id)).search_eligible


def test_state_lanes_remove_source_conflicts_and_exact_aliases_before_search() -> None:
    generator, _, _ = _generator()
    manifest = generator.categorical_gene_lanes(
        "INTRADAY_STATE_TRANSITION"
    )
    exact_identities = set()
    scheduled = 0

    for space in manifest["lanes"].values():
        for genes in _enumerate_genes(space):
            scheduled += 1
            pair = generator.propose_categorical_genes(
                "INTRADAY_STATE_TRANSITION",
                genes=genes,
            )
            assert pair.candidate["legal"] is True
            assert pair.control["legal"] is True
            exact_identities.add(str(pair.candidate["exact_identity"]))

    assert scheduled == 270
    assert len(exact_identities) == 270


def test_disclosure_lanes_restore_event_only_skeletons_without_payload_aliases() -> None:
    generator, _, _ = _generator()
    manifest = generator.categorical_gene_lanes("DISCLOSURE_EVENT")
    exact_identities = set()
    scheduled = 0

    for space in manifest["lanes"].values():
        for genes in _enumerate_genes(space):
            scheduled += 1
            pair = generator.propose_categorical_genes(
                "DISCLOSURE_EVENT",
                genes=genes,
            )
            assert pair.candidate["legal"] is True
            exact_identities.add(str(pair.candidate["exact_identity"]))

    assert scheduled == 2116
    assert len(exact_identities) == 2116


def test_lane_semantics_freezes_fixed_skeleton_and_active_slots() -> None:
    generator, registry, authority = _generator()
    space = next(
        iter(
            generator.categorical_gene_lanes("MARKET_REGIME_CONDITION")[
                "lanes"
            ].values()
        )
    )
    semantics = ExactGeneSemantics.freeze(
        route_id="MARKET_REGIME_CONDITION",
        ordered_categories_by_slot=space["ordered_categories_by_slot"],
        none_semantics=space["none_semantics"],
        skeleton_compatibility=space["skeleton_compatibility"],
        registry_hash=registry.registry_hash,
        root_contract_hash=str(authority["contract_hash"]),
        grammar_hash="test-grammar-hash",
    )

    assert len(semantics.categories_for_slot("skeleton_id")) == 1
    assert any(
        len(categories) >= 2
        for categories in semantics.ordered_category_ids_by_slot
    )
