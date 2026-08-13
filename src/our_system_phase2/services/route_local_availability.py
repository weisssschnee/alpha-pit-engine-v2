"""Deterministic route-local exact-availability control for typed Grammar lanes.

This module is deliberately narrow.  It does not select routes, construct
formula semantics, evaluate candidates, or learn financial quality.  It turns
the authoritative categorical Grammar lanes into a without-replacement exact
index which can sit between a route-local optimizer and the existing behavior
admission path.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from itertools import product
from typing import Any, Iterable, Mapping, Sequence


AVAILABILITY_SCHEMA_VERSION = "cn_route_local_exact_availability_v1"
EMITTER_STATE_SCHEMA_VERSION = "cn_route_local_exact_emitter_state_v1"


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def is_concrete_field_slot(slot: str) -> bool:
    """Derive concrete field slots from the lane declaration itself."""

    normalized = str(slot)
    return (
        normalized.endswith("field_pair_id")
        or normalized.endswith("_field_id")
        or normalized.endswith("_field_ids")
    )


def structural_bucket_payload(
    route_id: str,
    genes: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = {str(key): str(value) for key, value in genes.items()}
    skeleton_id = str(normalized.get("skeleton_id") or "")
    if not skeleton_id:
        raise ValueError("AVAILABILITY_BUCKET_REQUIRES_SKELETON_ID")
    structural = {
        slot: value
        for slot, value in normalized.items()
        if slot not in {"skeleton_id", "gene_surface_id"}
        and not is_concrete_field_slot(slot)
    }
    return {
        "route_id": str(route_id),
        "skeleton_id": skeleton_id,
        "structural_slots": structural,
    }


def structural_bucket_key(
    route_id: str,
    genes: Mapping[str, Any],
) -> str:
    return _stable_hash(structural_bucket_payload(route_id, genes))


@dataclass(frozen=True, slots=True)
class AvailabilityEntry:
    route_id: str
    bucket_key: str
    exact_identity: str
    control_exact_identity: str
    genes: Mapping[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "route_id": self.route_id,
            "bucket_key": self.bucket_key,
            "exact_identity": self.exact_identity,
            "control_exact_identity": self.control_exact_identity,
            "genes": dict(self.genes),
        }


@dataclass(frozen=True, slots=True)
class AvailabilityEmission:
    route_id: str
    bucket_key: str
    exact_identity: str
    control_exact_identity: str
    genes: Mapping[str, str]
    emission_mode: str
    formal_ask_ordinal: int
    source_exact_identity: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "route_id": self.route_id,
            "bucket_key": self.bucket_key,
            "exact_identity": self.exact_identity,
            "control_exact_identity": self.control_exact_identity,
            "genes": dict(self.genes),
            "emission_mode": self.emission_mode,
            "formal_ask_ordinal": self.formal_ask_ordinal,
            "source_exact_identity": self.source_exact_identity,
        }


def enumerate_authoritative_entries(
    *,
    generator: Any,
    lanes_by_route: Mapping[str, Mapping[str, Mapping[str, Any]]],
    routes: Sequence[str],
) -> tuple[list[AvailabilityEntry], dict[str, Any]]:
    """Enumerate legal exact identities through the existing Grammar/compiler.

    Multiple categorical points can be semantic aliases.  Each exact identity
    is therefore assigned to one deterministic canonical bucket/genes row so
    the remaining index cannot emit it twice through different aliases.
    """

    canonical: dict[str, tuple[str, AvailabilityEntry]] = {}
    categorical_points = Counter()
    legal_points = Counter()
    invalid_points = Counter()
    alias_points = Counter()
    for route_id in map(str, routes):
        for space in lanes_by_route[route_id].values():
            categories = {
                str(slot): tuple(map(str, values))
                for slot, values in dict(
                    space["ordered_categories_by_slot"]
                ).items()
            }
            slots = tuple(categories)
            for values in product(*(categories[slot] for slot in slots)):
                categorical_points[route_id] += 1
                genes = dict(zip(slots, map(str, values)))
                try:
                    pair = generator.propose_categorical_genes(
                        route_id,
                        genes=genes,
                    )
                    primary = dict(pair.candidate)
                    control = dict(pair.control)
                    exact_identity = str(
                        primary.get("exact_identity") or ""
                    )
                    control_exact = str(
                        control.get("exact_identity") or ""
                    )
                    if (
                        not bool(primary.get("legal"))
                        or not bool(control.get("legal"))
                        or not exact_identity
                        or not control_exact
                        or exact_identity == control_exact
                    ):
                        raise ValueError("TYPED_COMPILER_REJECTED_PAIR")
                except (KeyError, ValueError):
                    invalid_points[route_id] += 1
                    continue
                legal_points[route_id] += 1
                bucket_key = structural_bucket_key(route_id, genes)
                entry = AvailabilityEntry(
                    route_id=route_id,
                    bucket_key=bucket_key,
                    exact_identity=exact_identity,
                    control_exact_identity=control_exact,
                    genes=copy.deepcopy(genes),
                )
                rank = _stable_hash(
                    {
                        "route_id": route_id,
                        "exact_identity": exact_identity,
                        "bucket_key": bucket_key,
                        "genes": genes,
                    }
                )
                prior = canonical.get(exact_identity)
                if prior is None or rank < prior[0]:
                    if prior is not None:
                        alias_points[route_id] += 1
                    canonical[exact_identity] = (rank, entry)
                else:
                    alias_points[route_id] += 1

    entries = sorted(
        (row[1] for row in canonical.values()),
        key=lambda entry: (
            entry.route_id,
            entry.bucket_key,
            entry.exact_identity,
        ),
    )
    route_exact = Counter(entry.route_id for entry in entries)
    report = {
        "schema_version": AVAILABILITY_SCHEMA_VERSION,
        "routes": {
            route_id: {
                "categorical_points": categorical_points[route_id],
                "legal_categorical_points": legal_points[route_id],
                "deterministic_invalid_points": invalid_points[route_id],
                "semantic_alias_points": alias_points[route_id],
                "canonical_exact_identities": route_exact[route_id],
            }
            for route_id in map(str, routes)
        },
        "entry_count": len(entries),
        "entry_digest": _stable_hash(
            [entry.to_dict() for entry in entries]
        ),
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    return entries, report


class RouteLocalAvailabilityController:
    """Without-replacement exact emitter with deterministic bucket fallback."""

    def __init__(
        self,
        *,
        entries: Sequence[AvailabilityEntry | Mapping[str, Any]],
        seen_exact_identities: Iterable[str],
        emitter_seed: int,
        input_hashes: Mapping[str, str],
    ) -> None:
        self.emitter_seed = int(emitter_seed)
        self.input_hashes = {
            str(key): str(value) for key, value in input_hashes.items()
        }
        frozen_entries = [
            entry
            if isinstance(entry, AvailabilityEntry)
            else AvailabilityEntry(
                route_id=str(entry["route_id"]),
                bucket_key=str(entry["bucket_key"]),
                exact_identity=str(entry["exact_identity"]),
                control_exact_identity=str(
                    entry.get("control_exact_identity") or ""
                ),
                genes={
                    str(key): str(value)
                    for key, value in dict(entry["genes"]).items()
                },
            )
            for entry in entries
        ]
        if not frozen_entries:
            raise ValueError("AVAILABILITY_INDEX_EMPTY")
        if len({entry.exact_identity for entry in frozen_entries}) != len(
            frozen_entries
        ):
            raise ValueError("AVAILABILITY_INDEX_EXACT_DUPLICATE")
        self._entry_by_exact = {
            entry.exact_identity: entry for entry in frozen_entries
        }
        self._bucket_entries: dict[str, list[AvailabilityEntry]] = defaultdict(
            list
        )
        self._bucket_route: dict[str, str] = {}
        for entry in frozen_entries:
            self._bucket_entries[entry.bucket_key].append(entry)
            prior_route = self._bucket_route.setdefault(
                entry.bucket_key, entry.route_id
            )
            if prior_route != entry.route_id:
                raise ValueError("AVAILABILITY_BUCKET_ROUTE_COLLISION")
        for bucket_key, rows in self._bucket_entries.items():
            rows.sort(
                key=lambda entry: _stable_hash(
                    {
                        "emitter_seed": self.emitter_seed,
                        "route_id": entry.route_id,
                        "bucket_key": bucket_key,
                        "exact_identity": entry.exact_identity,
                    }
                )
            )
        self._bucket_queues = {
            bucket_key: deque(rows)
            for bucket_key, rows in self._bucket_entries.items()
        }
        self._uniform_queues: dict[str, deque[AvailabilityEntry]] = {}
        for route_id in sorted({entry.route_id for entry in frozen_entries}):
            rows = [
                entry for entry in frozen_entries if entry.route_id == route_id
            ]
            rows.sort(
                key=lambda entry: _stable_hash(
                    {
                        "emitter_seed": self.emitter_seed,
                        "emitter": "AVAILABILITY_AWARE_UNIFORM_V1",
                        "route_id": route_id,
                        "exact_identity": entry.exact_identity,
                    }
                )
            )
            self._uniform_queues[route_id] = deque(rows)
        historical = {
            str(identity)
            for identity in seen_exact_identities
            if str(identity)
        }
        self._historical_seen = historical
        self._emitted_order: list[str] = []
        self._seen = set(historical)
        self._remaining_by_bucket = {
            bucket_key: {
                entry.exact_identity
                for entry in rows
                if entry.exact_identity not in historical
            }
            for bucket_key, rows in self._bucket_entries.items()
        }
        self._retired = {
            bucket_key
            for bucket_key, remaining in self._remaining_by_bucket.items()
            if not remaining
        }
        self._global_rotation = Counter()
        self._optimizer_draw_attempts = Counter()
        self._formal_asks = Counter()
        self._formal_by_bucket = Counter()
        self._behavior_admitted = Counter()
        self._behavior_blocked = Counter()
        self._evaluated = Counter()
        self._emission_modes = Counter()
        self._bucket_definitions_hash = _stable_hash(
            [
                {
                    "bucket_key": bucket_key,
                    "route_id": self._bucket_route[bucket_key],
                    "exact_identities": sorted(
                        entry.exact_identity
                        for entry in self._bucket_entries[bucket_key]
                    ),
                }
                for bucket_key in sorted(self._bucket_entries)
            ]
        )

    @property
    def bucket_definitions_hash(self) -> str:
        return self._bucket_definitions_hash

    @property
    def seen_exact_identities(self) -> frozenset[str]:
        return frozenset(self._seen)

    def record_optimizer_draw(self, route_id: str, count: int = 1) -> None:
        if count < 0:
            raise ValueError("optimizer draw count cannot be negative")
        self._optimizer_draw_attempts[str(route_id)] += int(count)

    def remaining_count(
        self,
        route_id: str | None = None,
        bucket_key: str | None = None,
    ) -> int:
        if bucket_key is not None:
            return len(self._remaining_by_bucket.get(str(bucket_key), set()))
        if route_id is None:
            return sum(map(len, self._remaining_by_bucket.values()))
        route = str(route_id)
        return sum(
            len(remaining)
            for key, remaining in self._remaining_by_bucket.items()
            if self._bucket_route[key] == route
        )

    def remaining_entries(
        self,
        *,
        route_id: str | None = None,
    ) -> tuple[AvailabilityEntry, ...]:
        route = None if route_id is None else str(route_id)
        return tuple(
            copy.deepcopy(self._entry_by_exact[identity])
            for identity in sorted(
                set().union(*self._remaining_by_bucket.values()),
                key=lambda identity: _stable_hash(
                    {
                        "emitter_seed": self.emitter_seed,
                        "route_id": self._entry_by_exact[identity].route_id,
                        "exact_identity": identity,
                    }
                ),
            )
            if route is None
            or self._entry_by_exact[identity].route_id == route
        )

    def reserve_exact(
        self,
        *,
        route_id: str,
        exact_identity: str,
        emission_mode: str,
        source_exact_identity: str = "",
    ) -> AvailabilityEmission | None:
        identity = str(exact_identity)
        if identity in self._seen:
            return None
        entry = self._entry_by_exact.get(identity)
        if entry is None or entry.route_id != str(route_id):
            raise RuntimeError("AVAILABILITY_EXACT_OUTSIDE_INDEX")
        return self._reserve(
            entry,
            emission_mode=str(emission_mode),
            source_exact_identity=str(source_exact_identity),
        )

    def bucket_key_for_genes(
        self,
        route_id: str,
        genes: Mapping[str, Any],
    ) -> str:
        return structural_bucket_key(route_id, genes)

    def _next_from_bucket(self, bucket_key: str) -> AvailabilityEntry | None:
        key = str(bucket_key)
        remaining = self._remaining_by_bucket.get(key)
        if not remaining:
            self._retired.add(key)
            return None
        queue = self._bucket_queues[key]
        while queue and queue[0].exact_identity not in remaining:
            queue.popleft()
        if queue:
            return queue[0]
        raise RuntimeError("AVAILABILITY_REMAINING_QUEUE_CORRUPT")

    def _reserve(
        self,
        entry: AvailabilityEntry,
        *,
        emission_mode: str,
        source_exact_identity: str,
    ) -> AvailabilityEmission:
        if entry.exact_identity in self._seen:
            raise RuntimeError("AVAILABILITY_EXACT_ALREADY_SEEN")
        remaining = self._remaining_by_bucket[entry.bucket_key]
        if entry.exact_identity not in remaining:
            raise RuntimeError("AVAILABILITY_EXACT_NOT_REMAINING")
        remaining.remove(entry.exact_identity)
        self._seen.add(entry.exact_identity)
        self._emitted_order.append(entry.exact_identity)
        if not remaining:
            self._retired.add(entry.bucket_key)
        # Preserve the existing campaign exact-memory contract: after a pair
        # is emitted, both primary and matched-control candidate identities
        # become unavailable as future primaries.
        control_entry = self._entry_by_exact.get(
            entry.control_exact_identity
        )
        if (
            control_entry is not None
            and control_entry.exact_identity not in self._seen
        ):
            self._seen.add(control_entry.exact_identity)
            control_remaining = self._remaining_by_bucket[
                control_entry.bucket_key
            ]
            control_remaining.discard(control_entry.exact_identity)
            if not control_remaining:
                self._retired.add(control_entry.bucket_key)
        self._formal_asks[entry.route_id] += 1
        self._formal_by_bucket[(entry.route_id, entry.bucket_key)] += 1
        self._emission_modes[(entry.route_id, str(emission_mode))] += 1
        return AvailabilityEmission(
            route_id=entry.route_id,
            bucket_key=entry.bucket_key,
            exact_identity=entry.exact_identity,
            control_exact_identity=entry.control_exact_identity,
            genes=copy.deepcopy(dict(entry.genes)),
            emission_mode=str(emission_mode),
            formal_ask_ordinal=sum(self._formal_asks.values()) - 1,
            source_exact_identity=str(source_exact_identity),
        )

    def accept_direct(
        self,
        *,
        route_id: str,
        genes: Mapping[str, Any],
        exact_identity: str,
    ) -> AvailabilityEmission | None:
        identity = str(exact_identity)
        if identity in self._seen:
            return None
        entry = self._entry_by_exact.get(identity)
        if entry is None or entry.route_id != str(route_id):
            raise RuntimeError("AVAILABILITY_DIRECT_EXACT_OUTSIDE_INDEX")
        proposed_bucket = structural_bucket_key(route_id, genes)
        if proposed_bucket != entry.bucket_key:
            # A semantic alias can map the same exact formula to a different
            # categorical point.  Exact ownership is canonical and unique.
            source = f"ALIAS_BUCKET:{proposed_bucket}:{identity}"
        else:
            source = identity
        return self._reserve(
            entry,
            emission_mode="TPE_DIRECT_FRESH",
            source_exact_identity=source,
        )

    def emit_same_bucket(
        self,
        *,
        route_id: str,
        genes: Mapping[str, Any],
        source_exact_identity: str,
    ) -> AvailabilityEmission | None:
        bucket_key = structural_bucket_key(route_id, genes)
        if self._bucket_route.get(bucket_key) not in {None, str(route_id)}:
            raise RuntimeError("AVAILABILITY_BUCKET_ROUTE_DRIFT")
        entry = self._next_from_bucket(bucket_key)
        if entry is None:
            return None
        return self._reserve(
            entry,
            emission_mode="TPE_BUCKET_REPLACEMENT",
            source_exact_identity=source_exact_identity,
        )

    def emit_global_fallback(
        self,
        *,
        route_id: str,
        source_exact_identity: str = "",
    ) -> AvailabilityEmission | None:
        route = str(route_id)
        active = [
            bucket_key
            for bucket_key, remaining in self._remaining_by_bucket.items()
            if self._bucket_route[bucket_key] == route and remaining
        ]
        if not active:
            return None
        rotation = self._global_rotation[route]
        bucket_key = min(
            active,
            key=lambda key: _stable_hash(
                {
                    "emitter_seed": self.emitter_seed,
                    "route_id": route,
                    "rotation": rotation,
                    "bucket_key": key,
                    "remaining_count": len(self._remaining_by_bucket[key]),
                }
            ),
        )
        self._global_rotation[route] += 1
        entry = self._next_from_bucket(bucket_key)
        if entry is None:  # pragma: no cover - active list proves otherwise.
            raise RuntimeError("AVAILABILITY_ACTIVE_BUCKET_EMPTY")
        return self._reserve(
            entry,
            emission_mode="GLOBAL_AVAILABILITY_FALLBACK",
            source_exact_identity=source_exact_identity,
        )

    def emit_uniform(
        self,
        *,
        route_id: str,
    ) -> AvailabilityEmission | None:
        """Emit one deterministic route-local uniform exact without replacement.

        The queue is a seed-bound hash permutation of the same authoritative
        typed exact index used by the TPE availability path.  It has no
        optimizer trial and therefore cannot receive optimizer feedback.
        """

        route = str(route_id)
        queue = self._uniform_queues.get(route)
        if queue is None:
            raise RuntimeError("AVAILABILITY_UNIFORM_ROUTE_MISSING")
        while queue and queue[0].exact_identity in self._seen:
            queue.popleft()
        if not queue:
            return None
        return self._reserve(
            queue[0],
            emission_mode="AVAILABILITY_AWARE_UNIFORM",
            source_exact_identity="UNIFORM_HASH_PERMUTATION",
        )

    def record_behavior(
        self,
        *,
        route_id: str,
        admitted: bool,
        bucket_key: str | None = None,
    ) -> None:
        route = str(route_id)
        counter = (
            self._behavior_admitted
            if admitted
            else self._behavior_blocked
        )
        counter[(route, str(bucket_key or ""))] += 1

    def record_evaluated(
        self,
        *,
        route_id: str,
        bucket_key: str | None = None,
    ) -> None:
        self._evaluated[(str(route_id), str(bucket_key or ""))] += 1

    def snapshot(self) -> dict[str, Any]:
        route_ids = sorted(set(self._bucket_route.values()))
        bucket_rows = []
        for bucket_key in sorted(self._bucket_entries):
            route_id = self._bucket_route[bucket_key]
            bucket_rows.append(
                {
                    "route_id": route_id,
                    "bucket_key": bucket_key,
                    "remaining_count": len(
                        self._remaining_by_bucket[bucket_key]
                    ),
                    "retired": bucket_key in self._retired,
                    "formal_fresh_exact_asks": self._formal_by_bucket[
                        (route_id, bucket_key)
                    ],
                    "behavior_admitted": self._behavior_admitted[
                        (route_id, bucket_key)
                    ],
                    "behavior_blocked": self._behavior_blocked[
                        (route_id, bucket_key)
                    ],
                    "evaluated": self._evaluated[
                        (route_id, bucket_key)
                    ],
                }
            )
        payload = {
            "schema_version": EMITTER_STATE_SCHEMA_VERSION,
            "availability_schema_version": AVAILABILITY_SCHEMA_VERSION,
            "input_hashes": dict(self.input_hashes),
            "bucket_definitions_hash": self._bucket_definitions_hash,
            "emitter_seed": self.emitter_seed,
            "historical_seen_count": len(self._historical_seen),
            "historical_seen_digest": _stable_hash(
                sorted(self._historical_seen)
            ),
            "emitted_exact_identities": list(self._emitted_order),
            "retired_buckets": sorted(self._retired),
            "global_rotation_by_route": {
                route: self._global_rotation[route] for route in route_ids
            },
            "optimizer_draw_attempts_by_route": {
                route: self._optimizer_draw_attempts[route]
                for route in route_ids
            },
            "formal_fresh_exact_asks_by_route": {
                route: self._formal_asks[route] for route in route_ids
            },
            "emission_modes_by_route": {
                route: {
                    mode: self._emission_modes[(route, mode)]
                    for mode in (
                        (
                            "TPE_DIRECT_FRESH",
                            "TPE_BUCKET_REPLACEMENT",
                            "GLOBAL_AVAILABILITY_FALLBACK",
                            "AVAILABILITY_AWARE_UNIFORM",
                        )
                        if self._emission_modes[
                            (route, "AVAILABILITY_AWARE_UNIFORM")
                        ]
                        else (
                            "TPE_DIRECT_FRESH",
                            "TPE_BUCKET_REPLACEMENT",
                            "GLOBAL_AVAILABILITY_FALLBACK",
                        )
                    )
                }
                for route in route_ids
            },
            "remaining_exact_by_route": {
                route: self.remaining_count(route_id=route)
                for route in route_ids
            },
            "remaining_count_by_bucket": {
                row["bucket_key"]: row["remaining_count"]
                for row in bucket_rows
            },
            "bucket_rows": bucket_rows,
            "financial_reads": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }
        payload["controller_state_hash"] = _stable_hash(payload)
        return payload

    @classmethod
    def restore(
        cls,
        *,
        entries: Sequence[AvailabilityEntry | Mapping[str, Any]],
        seen_exact_identities: Iterable[str],
        state: Mapping[str, Any],
        input_hashes: Mapping[str, str],
    ) -> "RouteLocalAvailabilityController":
        expected_state_hash = str(state.get("controller_state_hash") or "")
        unhashed = {
            key: copy.deepcopy(value)
            for key, value in state.items()
            if key != "controller_state_hash"
        }
        if not expected_state_hash or _stable_hash(unhashed) != expected_state_hash:
            raise RuntimeError("AVAILABILITY_CONTROLLER_STATE_HASH_DRIFT")
        controller = cls(
            entries=entries,
            seen_exact_identities=seen_exact_identities,
            emitter_seed=int(state["emitter_seed"]),
            input_hashes=input_hashes,
        )
        if (
            str(state.get("schema_version") or "")
            != EMITTER_STATE_SCHEMA_VERSION
            or dict(state.get("input_hashes") or {})
            != controller.input_hashes
            or str(state.get("bucket_definitions_hash") or "")
            != controller.bucket_definitions_hash
        ):
            raise RuntimeError("AVAILABILITY_CONTROLLER_INPUT_DRIFT")
        for exact_identity in state.get("emitted_exact_identities") or ():
            entry = controller._entry_by_exact.get(str(exact_identity))
            if entry is None:
                raise RuntimeError("AVAILABILITY_RESTORE_EXACT_MISSING")
            controller._reserve(
                entry,
                emission_mode="RESTORED_EMISSION",
                source_exact_identity="RESTORE",
            )
        controller._global_rotation.update(
            {
                str(key): int(value)
                for key, value in dict(
                    state.get("global_rotation_by_route") or {}
                ).items()
            }
        )
        controller._optimizer_draw_attempts.update(
            {
                str(key): int(value)
                for key, value in dict(
                    state.get("optimizer_draw_attempts_by_route") or {}
                ).items()
            }
        )
        # Restore the counters exactly; _reserve above is used only to rebuild
        # remaining queues and exact memory.
        controller._formal_asks.clear()
        controller._formal_by_bucket.clear()
        controller._emission_modes.clear()
        controller._formal_asks.update(
            {
                str(key): int(value)
                for key, value in dict(
                    state.get("formal_fresh_exact_asks_by_route") or {}
                ).items()
            }
        )
        for route, modes in dict(
            state.get("emission_modes_by_route") or {}
        ).items():
            for mode, count in dict(modes).items():
                controller._emission_modes[(str(route), str(mode))] = int(
                    count
                )
        for row in state.get("bucket_rows") or ():
            controller._formal_by_bucket[
                (str(row["route_id"]), str(row["bucket_key"]))
            ] = int(row.get("formal_fresh_exact_asks") or 0)
            controller._behavior_admitted[
                (str(row["route_id"]), str(row["bucket_key"]))
            ] = int(row.get("behavior_admitted") or 0)
            controller._behavior_blocked[
                (str(row["route_id"]), str(row["bucket_key"]))
            ] = int(row.get("behavior_blocked") or 0)
            controller._evaluated[
                (str(row["route_id"]), str(row["bucket_key"]))
            ] = int(row.get("evaluated") or 0)
        if (
            controller.snapshot()["controller_state_hash"]
            != expected_state_hash
        ):
            raise RuntimeError("AVAILABILITY_CONTROLLER_RESTORE_DRIFT")
        return controller
