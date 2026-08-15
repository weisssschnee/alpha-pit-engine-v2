"""Prefinancial foundation for the Program optimizer successor benchmark.

The benchmark keeps one virtual search state per policy, prepares complete
seven-template waves before any result is visible, and exposes only a physical
result deduplication contract.  It deliberately contains no evaluator or
Project Control entry point.
"""

from __future__ import annotations

import copy
import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    PROGRAM_ROUTE_ID,
    STRUCTURED_SURROGATE_PROGRAM,
    HybridTPEProgramSearchAdapter,
    ProgramOptimizerObservationV1,
    StructuredSurrogateProgramSearchAdapter,
    program_tpe_lane_id_v1,
    program_tpe_trial_genes_v1,
)
from our_system_phase2.services.route_local_availability import AvailabilityEntry
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import ProgramUpliftCredit
from our_system_phase2.services.unified_capability_registry import stable_hash


POLICY_UNIFORM = "UNIFORM"
POLICY_TPE = "TPE_CONTROL"
POLICY_D1 = "TPE_TO_SURROGATE"
POLICY_D2 = "FEASIBILITY_GATED_TPE"
POLICIES = (POLICY_UNIFORM, POLICY_TPE, POLICY_D1, POLICY_D2)

TOTAL_WAVES = 20
BOOTSTRAP_WAVES = 4
# wave 0 is the common bootstrap Uniform wave; the other four floor waves stay
# inside the first sixteen diversity-expansion waves so their base groups can be
# prospectively reserved without changing the frozen 16/4 feasibility rule.
UNIFORM_FLOOR_WAVES = (0, 4, 7, 10, 13)
MAX_GATE_RAW_ATTEMPTS_PER_ECONOMIC_ASK = 32

TOTAL_POLICY_EFFICIENCY_DENOMINATOR = 140
POST_BOOTSTRAP_EFFICIENCY_DENOMINATOR = 112
POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY_DENOMINATOR = 84


@dataclass(frozen=True, slots=True)
class PhysicalProgramResultV1:
    exact_identity: str
    admission: AbsoluteEconomicAdmission
    uplift: ProgramUpliftCredit | None
    physical_result_hash: str

    @classmethod
    def create(
        cls,
        *,
        exact_identity: str,
        admission: AbsoluteEconomicAdmission,
        uplift: ProgramUpliftCredit | None,
    ) -> "PhysicalProgramResultV1":
        if bool(admission.admitted) != (uplift is not None):
            raise ValueError("SUCCESSOR_PHYSICAL_RESULT_DUAL_HEAD_DOMAIN_DRIFT")
        body = {
            "schema_version": "cn_program_successor_physical_result_v1",
            "exact_identity": str(exact_identity),
            "admission": admission.to_record(),
            "uplift": None if uplift is None else uplift.to_record(),
        }
        return cls(
            exact_identity=str(exact_identity),
            admission=admission,
            uplift=uplift,
            physical_result_hash=stable_hash(body),
        )

    def verify(self) -> None:
        if bool(self.admission.admitted) != (self.uplift is not None):
            raise ValueError("SUCCESSOR_PHYSICAL_RESULT_DUAL_HEAD_DOMAIN_DRIFT")
        body = {
            "schema_version": "cn_program_successor_physical_result_v1",
            "exact_identity": self.exact_identity,
            "admission": self.admission.to_record(),
            "uplift": None if self.uplift is None else self.uplift.to_record(),
        }
        if stable_hash(body) != self.physical_result_hash:
            raise ValueError("SUCCESSOR_PHYSICAL_RESULT_HASH_DRIFT")

    def optimizer_observation(self, proposal_id: str) -> ProgramOptimizerObservationV1:
        return ProgramOptimizerObservationV1(
            proposal_id=str(proposal_id),
            exact_identity=self.exact_identity,
            admission=self.admission,
            uplift=self.uplift,
        )

    def surrogate_row(self, proposal_id: str) -> dict[str, Any]:
        observation = self.optimizer_observation(proposal_id)
        return {
            "proposal_id": str(proposal_id),
            "exact_identity": self.exact_identity,
            "admitted": bool(self.admission.admitted),
            "uplift": observation.conditional_objective,
            "admission_record": self.admission.to_record(),
            "uplift_record": None if self.uplift is None else self.uplift.to_record(),
        }


@dataclass(frozen=True, slots=True)
class LogicalProgramAskV1:
    policy: str
    wave_index: int
    template_id: str
    exact_identity: str
    logical_proposal_id: str
    optimizer_proposal_id: str
    selection_kind: str
    optimizer_ask: Mapping[str, Any] | None


@dataclass(slots=True)
class PreparedSuccessorWaveV1:
    wave_index: int
    asks: tuple[LogicalProgramAskV1, ...]
    working: dict[str, Any]
    gate_statistics: dict[str, int]

    @property
    def physical_exact_identities(self) -> tuple[str, ...]:
        return tuple(sorted({row.exact_identity for row in self.asks}))

    def overlap_map(self) -> dict[str, list[str]]:
        output: dict[str, list[str]] = {}
        for row in self.asks:
            output.setdefault(row.exact_identity, []).append(row.policy)
        return {key: sorted(value) for key, value in sorted(output.items())}


class ProgramOptimizerSuccessorBenchmarkV1:
    """Logical successor benchmark state with no financial execution surface."""

    def __init__(
        self,
        *,
        entries: Sequence[AvailabilityEntry],
        template_ids: Sequence[str],
        group_by_exact_identity: Mapping[str, str],
        uniform_seed: int,
        tpe_seed: int,
        d1_surrogate_seed: int,
        d2_surrogate_seed: int,
        tpe_config: Mapping[str, Any],
        surrogate_config: Mapping[str, Any],
        prior_exact_identities: Sequence[str] = (),
        minimum_distinct_groups: int = 16,
        maximum_per_group: int = 4,
    ) -> None:
        self.entries = tuple(copy.deepcopy(tuple(entries)))
        self.template_ids = tuple(map(str, template_ids))
        if len(self.template_ids) != 7 or len(set(self.template_ids)) != 7:
            raise ValueError("SUCCESSOR_BENCHMARK_REQUIRES_SEVEN_TEMPLATES")
        self.group_by_exact_identity = {
            str(key): str(value) for key, value in group_by_exact_identity.items()
        }
        all_exact_identities = {entry.exact_identity for entry in self.entries}
        if set(self.group_by_exact_identity) != all_exact_identities:
            raise ValueError("SUCCESSOR_BENCHMARK_GROUP_MAP_COVERAGE_DRIFT")
        self.prior_exact_identities = tuple(
            sorted(set(map(str, prior_exact_identities)))
        )
        if not set(self.prior_exact_identities).issubset(all_exact_identities):
            raise ValueError("SUCCESSOR_BENCHMARK_PRIOR_EXACT_OUTSIDE_SPACE")
        self.uniform_seed = int(uniform_seed)
        self.tpe_seed = int(tpe_seed)
        self.d1_surrogate_seed = int(d1_surrogate_seed)
        self.d2_surrogate_seed = int(d2_surrogate_seed)
        self.tpe_config = dict(tpe_config)
        self.surrogate_config = dict(surrogate_config)
        self.minimum_distinct_groups = int(minimum_distinct_groups)
        self.maximum_per_group = int(maximum_per_group)
        if self.minimum_distinct_groups < 0 or self.maximum_per_group < 1:
            raise ValueError("SUCCESSOR_BENCHMARK_GROUP_CONTRACT_INVALID")

        self._entry_by_exact = {
            entry.exact_identity: entry for entry in self.entries
        }
        self._entries_by_template = {
            template_id: tuple(
                entry
                for entry in self.entries
                if str(entry.genes["program_template_id"]) == template_id
            )
            for template_id in self.template_ids
        }
        if any(not rows for rows in self._entries_by_template.values()):
            raise ValueError("SUCCESSOR_BENCHMARK_TEMPLATE_SUPPLY_EMPTY")

        self._uniform_schedule = self._build_uniform_schedule()
        self._floor_exact = {
            (wave, template): self._uniform_schedule[wave][template]
            for wave in UNIFORM_FLOOR_WAVES
            for template in self.template_ids
        }
        self.wave_index = 0
        self._used = {policy: set() for policy in POLICIES}
        self._group_counts = {
            policy: {template: {} for template in self.template_ids}
            for policy in POLICIES
        }
        self._completed_rows = {policy: [] for policy in POLICIES}

        initial_tpe = self._new_tpe(seen=self.prior_exact_identities)
        self._tpe_snapshots = {
            POLICY_TPE: initial_tpe.snapshot(),
            POLICY_D2: initial_tpe.snapshot(),
        }
        self._d1_surrogate_snapshot = self._new_surrogate(
            seed=self.d1_surrogate_seed, seen=self.prior_exact_identities
        ).snapshot()
        self._d2_surrogate_snapshot = self._new_surrogate(
            seed=self.d2_surrogate_seed, seen=self.prior_exact_identities
        ).snapshot()

    def _new_tpe(self, *, seen: Sequence[str]) -> HybridTPEProgramSearchAdapter:
        return HybridTPEProgramSearchAdapter(
            entries=self.entries,
            seen_exact_identities=tuple(seen),
            seed=self.tpe_seed,
            **self.tpe_config,
        )

    def _restore_tpe(
        self, snapshot: Mapping[str, Any]
    ) -> HybridTPEProgramSearchAdapter:
        return HybridTPEProgramSearchAdapter.restore(
            snapshot=dict(snapshot),
            entries=self.entries,
            seen_exact_identities=self.prior_exact_identities,
            seed=self.tpe_seed,
            **self.tpe_config,
        )

    def _new_surrogate(
        self, *, seed: int, seen: Sequence[str]
    ) -> StructuredSurrogateProgramSearchAdapter:
        return StructuredSurrogateProgramSearchAdapter(
            entries=self.entries,
            seen_exact_identities=tuple(seen),
            seed=int(seed),
            **self.surrogate_config,
        )

    def _restore_surrogate(
        self,
        *,
        snapshot: Mapping[str, Any],
        seed: int,
        policy: str | None = None,
    ) -> StructuredSurrogateProgramSearchAdapter:
        bootstrap_seen = (
            ()
            if policy is None or self.wave_index < BOOTSTRAP_WAVES
            else tuple(
                str(row["exact_identity"])
                for row in self._completed_rows[policy][: 7 * BOOTSTRAP_WAVES]
            )
        )
        historical_seen = tuple(
            dict.fromkeys((*self.prior_exact_identities, *bootstrap_seen))
        )
        return StructuredSurrogateProgramSearchAdapter.restore(
            snapshot=dict(snapshot),
            entries=self.entries,
            seen_exact_identities=historical_seen,
            seed=int(seed),
            **self.surrogate_config,
        )

    def _build_uniform_schedule(self) -> tuple[dict[str, str], ...]:
        used: dict[str, set[str]] = {
            template: {
                exact
                for exact in self.prior_exact_identities
                if str(self._entry_by_exact[exact].genes["program_template_id"])
                == template
            }
            for template in self.template_ids
        }
        counts: dict[str, dict[str, int]] = {
            template: {} for template in self.template_ids
        }
        waves = []
        for wave in range(TOTAL_WAVES):
            selected: dict[str, str] = {}
            for template in self.template_ids:
                candidates = self._feasible_entries(
                    template_id=template,
                    used=used[template],
                    group_counts=counts[template],
                    reserved_groups=frozenset(),
                )
                if not candidates:
                    raise RuntimeError("SUCCESSOR_UNIFORM_SCHEDULE_SUPPLY_EXHAUSTED")
                entry = min(
                    candidates,
                    key=lambda row: stable_hash(
                        {
                            "seed": self.uniform_seed,
                            "emitter": "PROGRAM_UNIFORM_HASH_PERMUTATION_V1",
                            "exact_identity": row.exact_identity,
                        }
                    ),
                )
                selected[template] = entry.exact_identity
                used[template].add(entry.exact_identity)
                group = self.group_by_exact_identity[entry.exact_identity]
                counts[template][group] = counts[template].get(group, 0) + 1
            waves.append(selected)
        return tuple(waves)

    def _wave_template_order(self, wave_index: int) -> tuple[str, ...]:
        offset = int(wave_index) % len(self.template_ids)
        return self.template_ids[offset:] + self.template_ids[:offset]

    def _future_floor_groups(self, *, wave_index: int, template_id: str) -> frozenset[str]:
        return frozenset(
            self.group_by_exact_identity[self._floor_exact[(wave, template_id)]]
            for wave in UNIFORM_FLOOR_WAVES
            if wave > wave_index
        )

    def _feasible_entries(
        self,
        *,
        template_id: str,
        used: set[str],
        group_counts: Mapping[str, int],
        reserved_groups: frozenset[str],
        source_entries: Sequence[AvailabilityEntry] | None = None,
    ) -> tuple[AvailabilityEntry, ...]:
        distinct = sum(int(value) > 0 for value in group_counts.values())
        source = self._entries_by_template[template_id] if source_entries is None else source_entries
        output = []
        for entry in source:
            if entry.exact_identity in used:
                continue
            group = self.group_by_exact_identity[entry.exact_identity]
            count = int(group_counts.get(group, 0))
            if group in reserved_groups or count >= self.maximum_per_group:
                continue
            if distinct < self.minimum_distinct_groups and count > 0:
                continue
            output.append(entry)
        return tuple(output)

    def _available_for_policy(
        self,
        *,
        policy: str,
        template_id: str,
        wave_index: int,
        adapter: Any,
        enforce_groups: bool,
    ) -> tuple[AvailabilityEntry, ...]:
        remaining = tuple(
            entry
            for entry in adapter.controller.remaining_entries(route_id=PROGRAM_ROUTE_ID)
            if str(entry.genes["program_template_id"]) == template_id
        )
        reserved = self._future_floor_groups(
            wave_index=wave_index, template_id=template_id
        )
        if not enforce_groups:
            return tuple(
                entry
                for entry in remaining
                if self.group_by_exact_identity[entry.exact_identity] not in reserved
            )
        return self._feasible_entries(
            template_id=template_id,
            used=set(self._used[policy]),
            group_counts=self._group_counts[policy][template_id],
            reserved_groups=reserved,
            source_entries=remaining,
        )

    @staticmethod
    def _logical_ask(
        *,
        policy: str,
        wave_index: int,
        template_id: str,
        exact_identity: str,
        selection_kind: str,
        optimizer_ask: Mapping[str, Any] | None = None,
    ) -> LogicalProgramAskV1:
        optimizer_proposal_id = str(
            (optimizer_ask or {}).get("proposal_id")
            or stable_hash(
                {
                    "policy": policy,
                    "wave": wave_index,
                    "template": template_id,
                    "exact": exact_identity,
                    "kind": selection_kind,
                }
            )[:24]
        )
        logical_proposal_id = stable_hash(
            {
                "policy": policy,
                "wave": wave_index,
                "template": template_id,
                "exact": exact_identity,
                "optimizer_proposal_id": optimizer_proposal_id,
            }
        )[:24]
        return LogicalProgramAskV1(
            policy=policy,
            wave_index=wave_index,
            template_id=template_id,
            exact_identity=exact_identity,
            logical_proposal_id=logical_proposal_id,
            optimizer_proposal_id=optimizer_proposal_id,
            selection_kind=selection_kind,
            optimizer_ask=None if optimizer_ask is None else copy.deepcopy(dict(optimizer_ask)),
        )

    def _reserve_external(
        self, adapter: Any, *, exact_identity: str, source: str
    ) -> None:
        emission = adapter.controller.reserve_exact(
            route_id=PROGRAM_ROUTE_ID,
            exact_identity=str(exact_identity),
            emission_mode="AVAILABILITY_AWARE_UNIFORM",
            source_exact_identity=str(source),
        )
        if emission is None:
            raise RuntimeError("SUCCESSOR_COMMON_SELECTION_NOT_AVAILABLE")

    def _prepare_tpe_floor_observation(
        self,
        adapter: HybridTPEProgramSearchAdapter,
        *,
        checkpoint_id: str,
        ask_ordinal: int,
        exact_identity: str,
    ) -> dict[str, Any]:
        entry = adapter._entry_by_exact.get(str(exact_identity))
        if entry is None:
            raise RuntimeError("SUCCESSOR_FLOOR_EXACT_OUTSIDE_TPE_SPACE")
        trial = adapter.tpe.enqueue_fixed_trial(
            checkpoint_id=checkpoint_id,
            ask_ordinal=int(ask_ordinal),
            genes=program_tpe_trial_genes_v1(entry),
            metadata={
                "program_level_trial": True,
                "successor_common_uniform_floor": True,
            },
        )
        adapter.controller.record_optimizer_draw(PROGRAM_ROUTE_ID)
        emission = adapter.controller.reserve_exact(
            route_id=PROGRAM_ROUTE_ID,
            exact_identity=entry.exact_identity,
            emission_mode="AVAILABILITY_AWARE_UNIFORM",
            source_exact_identity="SUCCESSOR_COMMON_UNIFORM_FLOOR",
        )
        if emission is None:
            raise RuntimeError("SUCCESSOR_COMMON_SELECTION_NOT_AVAILABLE")
        row = adapter._ask_row(
            emission=emission,
            checkpoint_id=checkpoint_id,
            ask_ordinal=int(ask_ordinal),
            proposal_id=str(trial["proposal_id"]),
            trial_number=int(trial["trial_number"]),
            optimizer_ask_identity=str(trial["proposal_id"]),
            acquisition={
                "source": "PROGRAM_UNIFORM_FLOOR_FIXED_TPE_OBSERVATION",
                "global_fallback": False,
            },
        )
        adapter.tpe.annotate_pending_trial(
            row["proposal_id"],
            {
                "program_exact_identity": emission.exact_identity,
                "program_gene_hash": row["program_gene_hash"],
            },
        )
        adapter._pending[row["proposal_id"]] = row
        return row

    def _ask_surrogate_wave(
        self,
        *,
        adapter: StructuredSurrogateProgramSearchAdapter,
        policy: str,
        wave_index: int,
    ) -> list[LogicalProgramAskV1]:
        asks = []
        checkpoint_id = f"successor_wave_{wave_index:03d}"
        for ordinal, template in enumerate(self._wave_template_order(wave_index)):
            remaining = self._available_for_policy(
                policy=policy,
                template_id=template,
                wave_index=wave_index,
                adapter=adapter,
                enforce_groups=True,
            )
            if not remaining:
                raise RuntimeError("SUCCESSOR_SURROGATE_TEMPLATE_SUPPLY_EXHAUSTED")
            scored = adapter._acquisition_rows(remaining)
            for score in scored:
                score["eligible_compared_count"] = len(remaining)
                score["inference_batch_size"] = adapter.candidate_pool_size
            scored.sort(
                key=lambda row: (
                    -float(row["acquisition"] or 0.0),
                    stable_hash(row["entry"].to_dict()),
                )
            )
            score = scored[0]
            entry = score["entry"]
            emission = adapter.controller.reserve_exact(
                route_id=PROGRAM_ROUTE_ID,
                exact_identity=entry.exact_identity,
                emission_mode="STRUCTURED_SURROGATE_ACQUISITION",
                source_exact_identity=stable_hash(
                    {key: value for key, value in score.items() if key != "entry"}
                ),
            )
            if emission is None:
                raise RuntimeError("SUCCESSOR_SURROGATE_RESERVATION_FAILED")
            proposal_id = stable_hash(
                {
                    "arm": adapter.arm,
                    "checkpoint_id": checkpoint_id,
                    "ask_ordinal": ordinal,
                    "exact_identity": emission.exact_identity,
                    "observation_count": len(adapter._observations),
                }
            )[:24]
            row = adapter._ask_row(
                emission=emission,
                checkpoint_id=checkpoint_id,
                ask_ordinal=ordinal,
                proposal_id=proposal_id,
                trial_number=None,
                optimizer_ask_identity=proposal_id,
                acquisition={key: value for key, value in score.items() if key != "entry"},
            )
            adapter._pending[proposal_id] = row
            asks.append(
                self._logical_ask(
                    policy=policy,
                    wave_index=wave_index,
                    template_id=template,
                    exact_identity=entry.exact_identity,
                    selection_kind="SURROGATE_OPTIMIZED",
                    optimizer_ask=row,
                )
            )
        return asks

    def _ask_tpe_wave(
        self,
        *,
        adapter: HybridTPEProgramSearchAdapter,
        policy: str,
        wave_index: int,
        feasibility_model: StructuredSurrogateProgramSearchAdapter | None,
        gate_statistics: dict[str, int],
    ) -> list[LogicalProgramAskV1]:
        asks = []
        checkpoint_id = f"successor_wave_{wave_index:03d}"
        raw_ordinal = 0
        for template_ordinal, template in enumerate(
            self._wave_template_order(wave_index)
        ):
            gate_set: frozenset[str] | None = None
            if feasibility_model is not None:
                gate_candidates = self._available_for_policy(
                    policy=policy,
                    template_id=template,
                    wave_index=wave_index,
                    adapter=feasibility_model,
                    enforce_groups=False,
                )
                if not gate_candidates:
                    raise RuntimeError("SUCCESSOR_D2_GATE_SUPPLY_EXHAUSTED")
                predictions = feasibility_model.predict_feasibility(
                    [entry.genes for entry in gate_candidates]
                )
                ranked = sorted(
                    zip(gate_candidates, predictions, strict=True),
                    key=lambda item: (
                        -float(item[1]["feasibility_probability"]),
                        item[0].exact_identity,
                    ),
                )
                gate_set = frozenset(
                    row[0].exact_identity for row in ranked[: math.ceil(len(ranked) / 2)]
                )

            attempts = 0
            while True:
                attempts += 1
                if attempts > MAX_GATE_RAW_ATTEMPTS_PER_ECONOMIC_ASK:
                    raise RuntimeError("SUCCESSOR_D2_GATE_RAW_ATTEMPT_EXHAUSTED")
                native = adapter.tpe.ask_trial(
                    checkpoint_id=checkpoint_id,
                    ask_ordinal=raw_ordinal,
                    fixed_skeleton_id=program_tpe_lane_id_v1(template),
                )
                raw_ordinal += 1
                raw_native = dict(native)
                adapter.controller.record_optimizer_draw(PROGRAM_ROUTE_ID)
                adapter._projection_stats["tpe_raw_ask_count"] += 1
                adapter._projection_stats["raw_legal_exact_count"] += 1
                if feasibility_model is not None:
                    gate_statistics["gate_raw_ask_count"] += 1
                raw_exact = str(dict(native["genes"])["program_exact_identity"])
                source_entry = adapter._entry_by_exact.get(raw_exact)
                if source_entry is None:
                    raise RuntimeError("SUCCESSOR_TPE_RAW_EXACT_OUTSIDE_SPACE")
                if gate_set is not None and raw_exact not in gate_set:
                    gate_statistics["gate_rejection_count"] += 1
                    adapter._tpe_internal_observations[str(native["proposal_id"])] = {
                        "proposal_id": str(native["proposal_id"]),
                        "outcome_class": "SURROGATE_FEASIBILITY_REJECTED",
                        "optimizer_reward": None,
                        "outcome_reason": "SURROGATE_FEASIBILITY_REJECTED",
                    }
                    continue
                break

            allowed = self._available_for_policy(
                policy=policy,
                template_id=template,
                wave_index=wave_index,
                adapter=adapter,
                enforce_groups=True,
            )
            if gate_set is not None:
                allowed = tuple(row for row in allowed if row.exact_identity in gate_set)
            if not allowed:
                raise RuntimeError("SUCCESSOR_TPE_LEGAL_SET_EMPTY")
            allowed_by_identity = {row.exact_identity for row in allowed}
            emission = None
            projection = {
                "mode": "DIRECT_LEGAL_EXACT",
                "raw_optimizer_ask_identity": str(raw_native["proposal_id"]),
                "raw_trial_number": int(raw_native["trial_number"]),
                "raw_exact_identity": raw_exact,
                "availability_replacement_applied": False,
                "availability_replacement_reason": None,
                "actual_exact_identity": raw_exact,
                "structural_distance": 0.0,
                "global_fallback": False,
            }
            if raw_exact in allowed_by_identity:
                emission = adapter.controller.reserve_exact(
                    route_id=PROGRAM_ROUTE_ID,
                    exact_identity=raw_exact,
                    emission_mode="PROGRAM_TPE_DIRECT_FRESH",
                    source_exact_identity=raw_exact,
                )
                if emission is not None:
                    adapter._projection_stats["direct_exact_hit_count"] += 1
            if emission is None:
                replacement, distance = adapter._nearest_legal_projection(
                    source_entry, allowed
                )
                adapter._tpe_internal_observations[str(native["proposal_id"])] = {
                    "proposal_id": str(native["proposal_id"]),
                    "outcome_class": "AVAILABILITY_REPLACED",
                    "optimizer_reward": None,
                    "outcome_reason": "SUCCESSOR_CURRENT_FEASIBILITY_REPLACEMENT",
                }
                native = adapter.tpe.enqueue_fixed_trial(
                    checkpoint_id=checkpoint_id,
                    genes=program_tpe_trial_genes_v1(replacement),
                    ask_ordinal=raw_ordinal,
                    metadata={
                        "source_optimizer_trial_number": int(raw_native["trial_number"]),
                        "program_level_trial": True,
                        "projection_strategy": "FULL_LEGAL_SET_MINIMUM_STRUCTURAL_DISTANCE_V1",
                    },
                )
                raw_ordinal += 1
                adapter.controller.record_optimizer_draw(PROGRAM_ROUTE_ID)
                emission = adapter.controller.reserve_exact(
                    route_id=PROGRAM_ROUTE_ID,
                    exact_identity=replacement.exact_identity,
                    emission_mode="PROGRAM_TPE_LEGAL_STRUCTURAL_PROJECTION",
                    source_exact_identity=raw_exact,
                )
                if emission is None:
                    raise RuntimeError("SUCCESSOR_TPE_PROJECTION_RESERVATION_FAILED")
                adapter._projection_stats["legal_projection_count"] += 1
                adapter._projection_stats["availability_replacement_count"] += 1
                adapter._projection_stats["eligibility_projection_count"] += 1
                same_bucket = source_entry.bucket_key == replacement.bucket_key
                adapter._projection_stats[
                    "same_bucket_projection_count"
                    if same_bucket
                    else "cross_bucket_projection_count"
                ] += 1
                if feasibility_model is not None:
                    gate_statistics["ordinary_projection_count"] += 1
                projection = {
                    "mode": "FULL_LEGAL_SET_MINIMUM_STRUCTURAL_DISTANCE_V1",
                    "raw_optimizer_ask_identity": str(raw_native["proposal_id"]),
                    "raw_trial_number": int(raw_native["trial_number"]),
                    "raw_exact_identity": raw_exact,
                    "availability_replacement_applied": True,
                    "availability_replacement_reason": "SUCCESSOR_CURRENT_FEASIBILITY_REPLACEMENT",
                    "actual_exact_identity": replacement.exact_identity,
                    "structural_distance": float(distance),
                    "global_fallback": False,
                }
            adapter._projection_stats["intent_preserved_count"] += 1
            adapter._projection_stats["actual_evaluated_ask_count"] += 1
            if feasibility_model is not None:
                gate_statistics["economic_ask_count"] += 1
            row = adapter._ask_row(
                emission=emission,
                checkpoint_id=checkpoint_id,
                ask_ordinal=template_ordinal,
                proposal_id=str(native["proposal_id"]),
                trial_number=int(native["trial_number"]),
                optimizer_ask_identity=str(native["proposal_id"]),
                acquisition={
                    "source": "official_optuna.samplers.TPESampler",
                    "projection": projection,
                    "successor_feasibility_gate_applied": gate_set is not None,
                },
            )
            adapter.tpe.annotate_pending_trial(
                row["proposal_id"],
                {
                    "program_exact_identity": emission.exact_identity,
                    "program_gene_hash": row["program_gene_hash"],
                },
            )
            adapter._pending[row["proposal_id"]] = row
            if feasibility_model is not None:
                mirrored = feasibility_model.controller.reserve_exact(
                    route_id=PROGRAM_ROUTE_ID,
                    exact_identity=emission.exact_identity,
                    emission_mode="TPE_DIRECT_FRESH",
                    source_exact_identity=row["proposal_id"],
                )
                if mirrored is None:
                    raise RuntimeError("SUCCESSOR_D2_FEASIBILITY_MIRROR_DRIFT")
            asks.append(
                self._logical_ask(
                    policy=policy,
                    wave_index=wave_index,
                    template_id=template,
                    exact_identity=emission.exact_identity,
                    selection_kind=(
                        "D2_FEASIBILITY_GATED_TPE"
                        if feasibility_model is not None
                        else "TPE_OPTIMIZED"
                    ),
                    optimizer_ask=row,
                )
            )
        return asks

    def prepare_wave(self, wave_index: int | None = None) -> PreparedSuccessorWaveV1:
        wave = self.wave_index if wave_index is None else int(wave_index)
        if wave != self.wave_index or not 0 <= wave < TOTAL_WAVES:
            raise ValueError("SUCCESSOR_WAVE_INDEX_DRIFT")
        tpe = self._restore_tpe(self._tpe_snapshots[POLICY_TPE])
        d2_tpe = self._restore_tpe(self._tpe_snapshots[POLICY_D2])
        d1_surrogate = self._restore_surrogate(
            snapshot=self._d1_surrogate_snapshot,
            seed=self.d1_surrogate_seed,
            policy=POLICY_D1,
        )
        d2_surrogate = self._restore_surrogate(
            snapshot=self._d2_surrogate_snapshot,
            seed=self.d2_surrogate_seed,
            policy=POLICY_D2,
        )
        working = {
            POLICY_TPE: tpe,
            POLICY_D2: d2_tpe,
            "D1_SURROGATE": d1_surrogate,
            "D2_SURROGATE": d2_surrogate,
        }
        gate_stats = {
            "gate_raw_ask_count": 0,
            "gate_rejection_count": 0,
            "economic_ask_count": 0,
            "ordinary_projection_count": 0,
        }
        asks: list[LogicalProgramAskV1] = []
        for template in self.template_ids:
            exact = self._uniform_schedule[wave][template]
            asks.append(
                self._logical_ask(
                    policy=POLICY_UNIFORM,
                    wave_index=wave,
                    template_id=template,
                    exact_identity=exact,
                    selection_kind="PURE_UNIFORM",
                )
            )

        if wave in UNIFORM_FLOOR_WAVES:
            checkpoint_id = f"successor_wave_{wave:03d}"
            for ordinal, template in enumerate(self._wave_template_order(wave)):
                exact = self._floor_exact[(wave, template)]
                tpe_floor = self._prepare_tpe_floor_observation(
                    tpe,
                    checkpoint_id=checkpoint_id,
                    ask_ordinal=ordinal,
                    exact_identity=exact,
                )
                d2_floor = self._prepare_tpe_floor_observation(
                    d2_tpe,
                    checkpoint_id=checkpoint_id,
                    ask_ordinal=ordinal,
                    exact_identity=exact,
                )
                if wave >= BOOTSTRAP_WAVES:
                    self._reserve_external(
                        d1_surrogate, exact_identity=exact, source="COMMON_UNIFORM_FLOOR"
                    )
                    self._reserve_external(
                        d2_surrogate, exact_identity=exact, source="COMMON_UNIFORM_FLOOR"
                    )
                asks.append(
                    self._logical_ask(
                        policy=POLICY_TPE,
                        wave_index=wave,
                        template_id=template,
                        exact_identity=exact,
                        selection_kind="COMMON_UNIFORM_FLOOR",
                        optimizer_ask=tpe_floor,
                    )
                )
                asks.append(
                    self._logical_ask(
                        policy=POLICY_D1,
                        wave_index=wave,
                        template_id=template,
                        exact_identity=exact,
                        selection_kind="COMMON_UNIFORM_FLOOR",
                    )
                )
                asks.append(
                    self._logical_ask(
                        policy=POLICY_D2,
                        wave_index=wave,
                        template_id=template,
                        exact_identity=exact,
                        selection_kind="COMMON_UNIFORM_FLOOR",
                        optimizer_ask=d2_floor,
                    )
                )
        elif wave < BOOTSTRAP_WAVES:
            common = self._ask_tpe_wave(
                adapter=tpe,
                policy=POLICY_TPE,
                wave_index=wave,
                feasibility_model=None,
                gate_statistics=gate_stats,
            )
            asks.extend(common)
            for source in common:
                for policy in (POLICY_D1, POLICY_D2):
                    asks.append(
                        self._logical_ask(
                            policy=policy,
                            wave_index=wave,
                            template_id=source.template_id,
                            exact_identity=source.exact_identity,
                            selection_kind="COMMON_TPE_BOOTSTRAP",
                            optimizer_ask=source.optimizer_ask,
                        )
                    )
        else:
            asks.extend(
                self._ask_tpe_wave(
                    adapter=tpe,
                    policy=POLICY_TPE,
                    wave_index=wave,
                    feasibility_model=None,
                    gate_statistics=gate_stats,
                )
            )
            asks.extend(
                self._ask_surrogate_wave(
                    adapter=d1_surrogate,
                    policy=POLICY_D1,
                    wave_index=wave,
                )
            )
            asks.extend(
                self._ask_tpe_wave(
                    adapter=d2_tpe,
                    policy=POLICY_D2,
                    wave_index=wave,
                    feasibility_model=d2_surrogate,
                    gate_statistics=gate_stats,
                )
            )
        if len(asks) != len(POLICIES) * len(self.template_ids):
            raise RuntimeError("SUCCESSOR_WAVE_LOGICAL_CARDINALITY_DRIFT")
        return PreparedSuccessorWaveV1(
            wave_index=wave,
            asks=tuple(asks),
            working=working,
            gate_statistics=gate_stats,
        )

    def commit_wave(
        self,
        prepared: PreparedSuccessorWaveV1,
        physical_results: Mapping[str, PhysicalProgramResultV1],
    ) -> dict[str, Any]:
        if prepared.wave_index != self.wave_index:
            raise ValueError("SUCCESSOR_WAVE_COMMIT_INDEX_DRIFT")
        expected = set(prepared.physical_exact_identities)
        if set(map(str, physical_results)) != expected:
            raise ValueError("SUCCESSOR_PHYSICAL_RESULT_COVERAGE_DRIFT")
        for exact, result in physical_results.items():
            if str(exact) != result.exact_identity:
                raise ValueError("SUCCESSOR_PHYSICAL_RESULT_IDENTITY_DRIFT")
            result.verify()

        asks_by_policy = {
            policy: [row for row in prepared.asks if row.policy == policy]
            for policy in POLICIES
        }
        completed = copy.deepcopy(self._completed_rows)
        logical_receipts = []
        for policy, asks in asks_by_policy.items():
            for ask in asks:
                result = physical_results[ask.exact_identity]
                row = result.surrogate_row(ask.logical_proposal_id)
                row.update(
                    {
                        "policy": policy,
                        "wave_index": self.wave_index,
                        "template_id": ask.template_id,
                        "selection_kind": ask.selection_kind,
                        "physical_result_hash": result.physical_result_hash,
                    }
                )
                completed[policy].append(row)
                logical_receipts.append(
                    {
                        "policy": policy,
                        "wave_index": self.wave_index,
                        "template_id": ask.template_id,
                        "exact_identity": ask.exact_identity,
                        "logical_proposal_id": ask.logical_proposal_id,
                        "physical_result_hash": result.physical_result_hash,
                    }
                )

        tpe = prepared.working[POLICY_TPE]
        d2_tpe = prepared.working[POLICY_D2]
        d1_surrogate = prepared.working["D1_SURROGATE"]
        d2_surrogate = prepared.working["D2_SURROGATE"]
        wave = self.wave_index
        t_rows = asks_by_policy[POLICY_TPE]
        tpe.tell(
            [
                physical_results[row.exact_identity].optimizer_observation(
                    row.optimizer_proposal_id
                )
                for row in t_rows
            ]
        )
        if wave < BOOTSTRAP_WAVES:
            d2_tpe_snapshot = tpe.snapshot()
        else:
            d2_tpe_snapshot = None

        if wave == BOOTSTRAP_WAVES - 1:
            d1_surrogate = self._new_surrogate(
                seed=self.d1_surrogate_seed,
                seen=tuple(
                    dict.fromkeys(
                        (
                            *self.prior_exact_identities,
                            *(row["exact_identity"] for row in completed[POLICY_D1]),
                        )
                    )
                ),
            )
            d2_surrogate = self._new_surrogate(
                seed=self.d2_surrogate_seed,
                seen=tuple(
                    dict.fromkeys(
                        (
                            *self.prior_exact_identities,
                            *(row["exact_identity"] for row in completed[POLICY_D2]),
                        )
                    )
                ),
            )
            d1_surrogate.ingest_completed_observations(completed[POLICY_D1])
            d2_surrogate.ingest_completed_observations(completed[POLICY_D2])
        elif wave >= BOOTSTRAP_WAVES:
            d1_rows = asks_by_policy[POLICY_D1]
            if wave in UNIFORM_FLOOR_WAVES:
                d1_surrogate.ingest_completed_observations(
                    [
                        physical_results[row.exact_identity].surrogate_row(
                            row.logical_proposal_id
                        )
                        for row in d1_rows
                    ]
                )
            else:
                d1_surrogate.tell(
                    [
                        physical_results[row.exact_identity].optimizer_observation(
                            row.optimizer_proposal_id
                        )
                        for row in d1_rows
                    ]
                )
            d2_surrogate.ingest_completed_observations(
                [
                    physical_results[row.exact_identity].surrogate_row(
                        row.logical_proposal_id
                    )
                    for row in asks_by_policy[POLICY_D2]
                ]
            )
            d2_tpe.tell(
                [
                    physical_results[row.exact_identity].optimizer_observation(
                        row.optimizer_proposal_id
                    )
                    for row in asks_by_policy[POLICY_D2]
                ]
            )

        used = copy.deepcopy(self._used)
        group_counts = copy.deepcopy(self._group_counts)
        for policy, asks in asks_by_policy.items():
            for ask in asks:
                if ask.exact_identity in used[policy]:
                    raise RuntimeError("SUCCESSOR_POLICY_DUPLICATE_EXACT_SELECTION")
                used[policy].add(ask.exact_identity)
                group = self.group_by_exact_identity[ask.exact_identity]
                counts = group_counts[policy][ask.template_id]
                counts[group] = counts.get(group, 0) + 1
                if counts[group] > self.maximum_per_group:
                    raise RuntimeError("SUCCESSOR_GROUP_CAPACITY_DRIFT")

        self._used = used
        self._group_counts = group_counts
        self._completed_rows = completed
        self._tpe_snapshots[POLICY_TPE] = tpe.snapshot()
        self._tpe_snapshots[POLICY_D2] = (
            d2_tpe_snapshot if d2_tpe_snapshot is not None else d2_tpe.snapshot()
        )
        self._d1_surrogate_snapshot = d1_surrogate.snapshot()
        self._d2_surrogate_snapshot = d2_surrogate.snapshot()
        self.wave_index += 1
        gate_raw = int(prepared.gate_statistics["gate_raw_ask_count"])
        gate_rejections = int(prepared.gate_statistics["gate_rejection_count"])
        return {
            "schema_version": "cn_program_optimizer_successor_wave_commit_v1",
            "wave_index": wave,
            "logical_receipts": logical_receipts,
            "logical_ask_count": len(prepared.asks),
            "physical_unique_count": len(expected),
            "overlap_map": prepared.overlap_map(),
            "gate_statistics": {
                **prepared.gate_statistics,
                "gate_acceptance_rate": (
                    (gate_raw - gate_rejections) / gate_raw if gate_raw else 1.0
                ),
            },
        }

    def snapshot(self) -> dict[str, Any]:
        payload = {
            "schema_version": "cn_program_optimizer_successor_benchmark_state_v1",
            "wave_index": self.wave_index,
            "template_ids": list(self.template_ids),
            "program_space_hash": stable_hash(
                [entry.to_dict() for entry in sorted(self.entries, key=lambda row: row.exact_identity)]
            ),
            "configuration": {
                "uniform_seed": self.uniform_seed,
                "tpe_seed": self.tpe_seed,
                "d1_surrogate_seed": self.d1_surrogate_seed,
                "d2_surrogate_seed": self.d2_surrogate_seed,
                "tpe_config": copy.deepcopy(self.tpe_config),
                "surrogate_config": copy.deepcopy(self.surrogate_config),
                "prior_exact_identity_count": len(self.prior_exact_identities),
                "prior_exact_identities_hash": stable_hash(
                    list(self.prior_exact_identities)
                ),
                "minimum_distinct_groups": self.minimum_distinct_groups,
                "maximum_per_group": self.maximum_per_group,
                "group_map_hash": stable_hash(self.group_by_exact_identity),
                "uniform_schedule_hash": stable_hash(self._uniform_schedule),
            },
            "used": {policy: sorted(values) for policy, values in self._used.items()},
            "group_counts": copy.deepcopy(self._group_counts),
            "completed_rows": copy.deepcopy(self._completed_rows),
            "tpe_snapshots": copy.deepcopy(self._tpe_snapshots),
            "d1_surrogate_snapshot": copy.deepcopy(self._d1_surrogate_snapshot),
            "d2_surrogate_snapshot": copy.deepcopy(self._d2_surrogate_snapshot),
            "uniform_floor_waves": list(UNIFORM_FLOOR_WAVES),
            "metric_denominators": {
                "TOTAL_POLICY_EFFICIENCY": TOTAL_POLICY_EFFICIENCY_DENOMINATOR,
                "POST_BOOTSTRAP_EFFICIENCY": POST_BOOTSTRAP_EFFICIENCY_DENOMINATOR,
                "POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY": (
                    POLICY_SPECIFIC_OPTIMIZED_EFFICIENCY_DENOMINATOR
                ),
            },
            "financial_evaluation_executed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        }
        payload["state_hash"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(
        cls,
        snapshot: Mapping[str, Any],
        *,
        entries: Sequence[AvailabilityEntry],
        template_ids: Sequence[str],
        group_by_exact_identity: Mapping[str, str],
        uniform_seed: int,
        tpe_seed: int,
        d1_surrogate_seed: int,
        d2_surrogate_seed: int,
        tpe_config: Mapping[str, Any],
        surrogate_config: Mapping[str, Any],
        prior_exact_identities: Sequence[str] = (),
        minimum_distinct_groups: int = 16,
        maximum_per_group: int = 4,
    ) -> "ProgramOptimizerSuccessorBenchmarkV1":
        source = copy.deepcopy(dict(snapshot))
        claimed = str(source.pop("state_hash", ""))
        if not claimed or stable_hash(source) != claimed:
            raise ValueError("SUCCESSOR_BENCHMARK_STATE_SELF_HASH_DRIFT")
        instance = cls(
            entries=entries,
            template_ids=template_ids,
            group_by_exact_identity=group_by_exact_identity,
            uniform_seed=uniform_seed,
            tpe_seed=tpe_seed,
            d1_surrogate_seed=d1_surrogate_seed,
            d2_surrogate_seed=d2_surrogate_seed,
            tpe_config=tpe_config,
            surrogate_config=surrogate_config,
            prior_exact_identities=prior_exact_identities,
            minimum_distinct_groups=minimum_distinct_groups,
            maximum_per_group=maximum_per_group,
        )
        config = dict(source.get("configuration") or {})
        expected_config = dict(instance.snapshot()["configuration"])
        if config != expected_config:
            raise ValueError("SUCCESSOR_BENCHMARK_CONFIGURATION_DRIFT")
        if list(source.get("template_ids") or ()) != list(instance.template_ids):
            raise ValueError("SUCCESSOR_BENCHMARK_TEMPLATE_ID_DRIFT")
        if str(source.get("program_space_hash") or "") != str(
            instance.snapshot()["program_space_hash"]
        ):
            raise ValueError("SUCCESSOR_BENCHMARK_PROGRAM_SPACE_DRIFT")
        instance.wave_index = int(source["wave_index"])
        instance._used = {
            str(policy): set(map(str, values))
            for policy, values in dict(source["used"]).items()
        }
        instance._group_counts = copy.deepcopy(dict(source["group_counts"]))
        instance._completed_rows = copy.deepcopy(dict(source["completed_rows"]))
        instance._tpe_snapshots = copy.deepcopy(dict(source["tpe_snapshots"]))
        instance._d1_surrogate_snapshot = copy.deepcopy(
            dict(source["d1_surrogate_snapshot"])
        )
        instance._d2_surrogate_snapshot = copy.deepcopy(
            dict(source["d2_surrogate_snapshot"])
        )
        if instance.snapshot() != dict(snapshot):
            raise RuntimeError("SUCCESSOR_BENCHMARK_STATE_RESTORE_DRIFT")
        return instance
