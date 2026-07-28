from __future__ import annotations

from dataclasses import dataclass

import pytest

from our_system_phase2.services.route_local_availability import (
    AvailabilityEntry,
    RouteLocalAvailabilityController,
    enumerate_authoritative_entries,
    structural_bucket_key,
)


ROUTE = "TEST_ROUTE"


def _genes(field: str, *, window: str = "5") -> dict[str, str]:
    return {
        "skeleton_id": "test.single",
        "gene_surface_id": "surface-v1",
        "primary_field_id": field,
        "window_id": window,
    }


def _entry(field: str, *, window: str = "5") -> AvailabilityEntry:
    genes = _genes(field, window=window)
    return AvailabilityEntry(
        route_id=ROUTE,
        bucket_key=structural_bucket_key(ROUTE, genes),
        exact_identity=f"exact-{field}-{window}",
        control_exact_identity=f"control-{field}-{window}",
        genes=genes,
    )


def _controller(
    *,
    seen: set[str] | None = None,
) -> RouteLocalAvailabilityController:
    return RouteLocalAvailabilityController(
        entries=[
            _entry("a"),
            _entry("b"),
            _entry("c", window="20"),
        ],
        seen_exact_identities=seen or set(),
        emitter_seed=17,
        input_hashes={"registry": "r", "grammar": "g", "compiler": "c"},
    )


def test_historical_duplicate_gets_same_bucket_fresh_replacement() -> None:
    controller = _controller(seen={"exact-a-5"})
    controller.record_optimizer_draw(ROUTE)

    assert (
        controller.accept_direct(
            route_id=ROUTE,
            genes=_genes("a"),
            exact_identity="exact-a-5",
        )
        is None
    )
    replacement = controller.emit_same_bucket(
        route_id=ROUTE,
        genes=_genes("a"),
        source_exact_identity="exact-a-5",
    )

    assert replacement is not None
    assert replacement.emission_mode == "TPE_BUCKET_REPLACEMENT"
    assert replacement.exact_identity == "exact-b-5"
    assert replacement.exact_identity not in {"exact-a-5"}
    assert controller.snapshot()["formal_fresh_exact_asks_by_route"][
        ROUTE
    ] == 1


def test_bucket_exhaustion_retires_without_consuming_formal_ask() -> None:
    controller = _controller(seen={"exact-a-5", "exact-b-5"})
    before = controller.snapshot()["formal_fresh_exact_asks_by_route"][
        ROUTE
    ]

    emission = controller.emit_same_bucket(
        route_id=ROUTE,
        genes=_genes("a"),
        source_exact_identity="exact-a-5",
    )

    assert emission is None
    snapshot = controller.snapshot()
    assert (
        structural_bucket_key(ROUTE, _genes("a"))
        in snapshot["retired_buckets"]
    )
    assert snapshot["formal_fresh_exact_asks_by_route"][ROUTE] == before


def test_global_fallback_is_deterministic_and_without_replacement() -> None:
    first = _controller(seen={"exact-a-5", "exact-b-5"})
    second = _controller(seen={"exact-a-5", "exact-b-5"})

    first_emission = first.emit_global_fallback(route_id=ROUTE)
    second_emission = second.emit_global_fallback(route_id=ROUTE)

    assert first_emission is not None
    assert second_emission is not None
    assert first_emission.to_dict() == second_emission.to_dict()
    assert first_emission.emission_mode == "GLOBAL_AVAILABILITY_FALLBACK"
    assert first.emit_global_fallback(route_id=ROUTE) is None


def test_checkpoint_restore_preserves_next_exact_sequence_and_hash() -> None:
    controller = _controller(seen={"exact-a-5"})
    first = controller.emit_same_bucket(
        route_id=ROUTE,
        genes=_genes("a"),
        source_exact_identity="exact-a-5",
    )
    assert first is not None
    state = controller.snapshot()

    restored = RouteLocalAvailabilityController.restore(
        entries=[
            _entry("a"),
            _entry("b"),
            _entry("c", window="20"),
        ],
        seen_exact_identities={"exact-a-5"},
        state=state,
        input_hashes={"registry": "r", "grammar": "g", "compiler": "c"},
    )

    assert restored.snapshot()["controller_state_hash"] == state[
        "controller_state_hash"
    ]
    assert (
        restored.emit_global_fallback(route_id=ROUTE).exact_identity
        == controller.emit_global_fallback(route_id=ROUTE).exact_identity
    )


def test_state_hash_or_authority_drift_fails_closed() -> None:
    controller = _controller()
    state = controller.snapshot()
    state["remaining_exact_by_route"][ROUTE] -= 1

    with pytest.raises(
        RuntimeError, match="AVAILABILITY_CONTROLLER_STATE_HASH_DRIFT"
    ):
        RouteLocalAvailabilityController.restore(
            entries=[_entry("a"), _entry("b"), _entry("c", window="20")],
            seen_exact_identities=set(),
            state=state,
            input_hashes={"registry": "r", "grammar": "g", "compiler": "c"},
        )


@dataclass
class _Pair:
    candidate: dict[str, object]
    control: dict[str, object]


class _Generator:
    def propose_categorical_genes(
        self,
        route_id: str,
        *,
        genes: dict[str, str],
    ) -> _Pair:
        field = genes["primary_field_id"]
        # "alias" deliberately compiles to the same exact formula as "a".
        exact = "exact-a" if field in {"a", "alias"} else f"exact-{field}"
        return _Pair(
            candidate={"legal": True, "exact_identity": exact},
            control={"legal": True, "exact_identity": f"control-{exact}"},
        )


def test_authoritative_enumeration_canonicalizes_semantic_aliases() -> None:
    lanes = {
        ROUTE: {
            "test.single": {
                "ordered_categories_by_slot": {
                    "skeleton_id": ["test.single"],
                    "gene_surface_id": ["surface-v1"],
                    "primary_field_id": ["a", "alias", "b"],
                    "window_id": ["5"],
                }
            }
        }
    }

    entries, report = enumerate_authoritative_entries(
        generator=_Generator(),
        lanes_by_route=lanes,
        routes=[ROUTE],
    )

    assert {entry.exact_identity for entry in entries} == {
        "exact-a",
        "exact-b",
    }
    assert report["routes"][ROUTE]["semantic_alias_points"] == 1
    assert report["entry_count"] == 2

