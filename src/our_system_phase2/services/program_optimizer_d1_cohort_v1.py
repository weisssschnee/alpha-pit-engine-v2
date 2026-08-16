"""Single-policy D1 development cohort over the accepted successor foundation.

This module deliberately reuses the exact Uniform/TPE/Surrogate implementation
from ``ProgramOptimizerSuccessorBenchmarkV1``.  Only the prospectively tested
D1 path is active:

- wave 0 and waves 4/7/10/13: deterministic Uniform exploration,
- waves 1..3: Hybrid TPE bootstrap,
- after 28 completed observations: full Structured Surrogate acquisition.

No evaluator or Project Control surface lives here.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, replace
from typing import Any, Mapping, Sequence

from our_system_phase2.services.program_optimizer_successor_benchmark_v1 import (
    BOOTSTRAP_WAVES,
    POLICY_D1,
    POLICY_D2,
    POLICY_TPE,
    POLICY_UNIFORM,
    TOTAL_WAVES,
    UNIFORM_FLOOR_WAVES,
    LogicalProgramAskV1,
    PhysicalProgramResultV1,
    ProgramOptimizerSuccessorBenchmarkV1,
)
from our_system_phase2.services.route_local_availability import AvailabilityEntry
from our_system_phase2.services.unified_capability_registry import stable_hash


D1_POLICY_ID = "CN_PROGRAM_OPTIMIZER_TPE_BOOTSTRAP_TO_SURROGATE_D1_V1"
D1_SELECTION_UNIFORM = "UNIFORM"
D1_SELECTION_TPE = "TPE_BOOTSTRAP"
D1_SELECTION_SURROGATE = "SURROGATE_FULL_ACQUISITION"
D1_SELECTOR_COUNTS = {
    D1_SELECTION_UNIFORM: 35,
    D1_SELECTION_TPE: 21,
    D1_SELECTION_SURROGATE: 84,
}
D1_LOGICAL_RECORDS = 140


@dataclass(slots=True)
class PreparedD1WaveV1:
    wave_index: int
    asks: tuple[LogicalProgramAskV1, ...]
    working: dict[str, Any]

    @property
    def physical_exact_identities(self) -> tuple[str, ...]:
        return tuple(sorted(row.exact_identity for row in self.asks))


class ProgramOptimizerD1CohortV1:
    """Thin D1-only state machine backed by the accepted successor state."""

    def __init__(
        self,
        *,
        entries: Sequence[AvailabilityEntry],
        template_ids: Sequence[str],
        group_by_exact_identity: Mapping[str, str],
        uniform_seed: int,
        tpe_seed: int,
        surrogate_seed: int,
        tpe_config: Mapping[str, Any],
        surrogate_config: Mapping[str, Any],
        prior_exact_identities: Sequence[str] = (),
        minimum_distinct_groups: int = 16,
        maximum_per_group: int = 4,
    ) -> None:
        self.uniform_seed = int(uniform_seed)
        self.tpe_seed = int(tpe_seed)
        self.surrogate_seed = int(surrogate_seed)
        self.tpe_config = dict(tpe_config)
        self.surrogate_config = dict(surrogate_config)
        self.minimum_distinct_groups = int(minimum_distinct_groups)
        self.maximum_per_group = int(maximum_per_group)
        self._inner = ProgramOptimizerSuccessorBenchmarkV1(
            entries=entries,
            template_ids=template_ids,
            group_by_exact_identity=group_by_exact_identity,
            uniform_seed=self.uniform_seed,
            tpe_seed=self.tpe_seed,
            d1_surrogate_seed=self.surrogate_seed,
            # D2 is never selected or told by this wrapper.  Binding it to the
            # same seed avoids inventing an otherwise irrelevant parameter.
            d2_surrogate_seed=self.surrogate_seed,
            tpe_config=self.tpe_config,
            surrogate_config=self.surrogate_config,
            prior_exact_identities=prior_exact_identities,
            minimum_distinct_groups=self.minimum_distinct_groups,
            maximum_per_group=self.maximum_per_group,
        )

    @property
    def wave_index(self) -> int:
        return int(self._inner.wave_index)

    @property
    def template_ids(self) -> tuple[str, ...]:
        return self._inner.template_ids

    @property
    def prior_exact_identities(self) -> tuple[str, ...]:
        return self._inner.prior_exact_identities

    def completed_rows(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self._inner._completed_rows[POLICY_D1])

    def _assert_other_policies_unused(self) -> None:
        for policy in (POLICY_UNIFORM, POLICY_TPE, POLICY_D2):
            if (
                self._inner._used[policy]
                or self._inner._completed_rows[policy]
                or any(self._inner._group_counts[policy][template] for template in self.template_ids)
            ):
                raise RuntimeError("D1_COHORT_CROSS_POLICY_STATE_DRIFT")

    def prepare_wave(self, wave_index: int | None = None) -> PreparedD1WaveV1:
        self._assert_other_policies_unused()
        wave = self.wave_index if wave_index is None else int(wave_index)
        if wave != self.wave_index or not 0 <= wave < TOTAL_WAVES:
            raise ValueError("D1_COHORT_WAVE_INDEX_DRIFT")

        tpe = self._inner._restore_tpe(self._inner._tpe_snapshots[POLICY_TPE])
        surrogate = self._inner._restore_surrogate(
            snapshot=self._inner._d1_surrogate_snapshot,
            seed=self.surrogate_seed,
            policy=POLICY_D1,
        )
        asks: list[LogicalProgramAskV1] = []
        working = {"TPE": tpe, "SURROGATE": surrogate}

        if wave in UNIFORM_FLOOR_WAVES:
            checkpoint_id = f"d1_wave_{wave:03d}"
            for ordinal, template in enumerate(self._inner._wave_template_order(wave)):
                exact = self._inner._floor_exact[(wave, template)]
                optimizer_ask = None
                if wave == 0:
                    # This is essential to reproduce the tested D1: waves 1..3
                    # copied the common TPE control, whose TPE state had already
                    # learned the wave-0 Uniform result through a fixed trial.
                    optimizer_ask = self._inner._prepare_tpe_floor_observation(
                        tpe,
                        checkpoint_id=checkpoint_id,
                        ask_ordinal=ordinal,
                        exact_identity=exact,
                    )
                else:
                    self._inner._reserve_external(
                        surrogate,
                        exact_identity=exact,
                        source="D1_UNIFORM_FLOOR",
                    )
                asks.append(
                    self._inner._logical_ask(
                        policy=POLICY_D1,
                        wave_index=wave,
                        template_id=template,
                        exact_identity=exact,
                        selection_kind=D1_SELECTION_UNIFORM,
                        optimizer_ask=optimizer_ask,
                    )
                )
        elif wave < BOOTSTRAP_WAVES:
            selected = self._inner._ask_tpe_wave(
                adapter=tpe,
                policy=POLICY_D1,
                wave_index=wave,
                feasibility_model=None,
                gate_statistics={
                    "gate_raw_ask_count": 0,
                    "gate_rejection_count": 0,
                    "economic_ask_count": 0,
                    "ordinary_projection_count": 0,
                },
                gate_rejections=[],
            )
            asks.extend(replace(row, selection_kind=D1_SELECTION_TPE) for row in selected)
        else:
            selected = self._inner._ask_surrogate_wave(
                adapter=surrogate,
                policy=POLICY_D1,
                wave_index=wave,
            )
            asks.extend(
                replace(row, selection_kind=D1_SELECTION_SURROGATE)
                for row in selected
            )

        if len(asks) != len(self.template_ids):
            raise RuntimeError("D1_COHORT_WAVE_CARDINALITY_DRIFT")
        if {row.template_id for row in asks} != set(self.template_ids):
            raise RuntimeError("D1_COHORT_WAVE_TEMPLATE_COVERAGE_DRIFT")
        exacts = [row.exact_identity for row in asks]
        if len(exacts) != len(set(exacts)):
            raise RuntimeError("D1_COHORT_WAVE_DUPLICATE_EXACT")
        if any(exact in set(self.prior_exact_identities) for exact in exacts):
            raise RuntimeError("D1_COHORT_PRIOR_EXACT_REUSE")
        return PreparedD1WaveV1(
            wave_index=wave,
            asks=tuple(asks),
            working=working,
        )

    def commit_wave(
        self,
        prepared: PreparedD1WaveV1,
        physical_results: Mapping[str, PhysicalProgramResultV1],
    ) -> dict[str, Any]:
        self._assert_other_policies_unused()
        if prepared.wave_index != self.wave_index:
            raise ValueError("D1_COHORT_COMMIT_WAVE_INDEX_DRIFT")
        expected = set(prepared.physical_exact_identities)
        if set(map(str, physical_results)) != expected:
            raise ValueError("D1_COHORT_PHYSICAL_RESULT_COVERAGE_DRIFT")
        for exact, result in physical_results.items():
            if str(exact) != result.exact_identity:
                raise ValueError("D1_COHORT_PHYSICAL_RESULT_IDENTITY_DRIFT")
            result.verify()

        wave = self.wave_index
        completed = copy.deepcopy(self._inner._completed_rows)
        logical_receipts: list[dict[str, Any]] = []
        for ask in prepared.asks:
            result = physical_results[ask.exact_identity]
            row = result.surrogate_row(ask.logical_proposal_id)
            row.update(
                {
                    "policy": POLICY_D1,
                    "policy_id": D1_POLICY_ID,
                    "wave_index": wave,
                    "template_id": ask.template_id,
                    "selection_kind": ask.selection_kind,
                    "physical_result_hash": result.physical_result_hash,
                }
            )
            completed[POLICY_D1].append(row)
            logical_receipts.append(
                {
                    "policy": POLICY_D1,
                    "policy_id": D1_POLICY_ID,
                    "wave_index": wave,
                    "template_id": ask.template_id,
                    "selection_kind": ask.selection_kind,
                    "exact_identity": ask.exact_identity,
                    "logical_proposal_id": ask.logical_proposal_id,
                    "physical_result_hash": result.physical_result_hash,
                }
            )

        tpe = prepared.working["TPE"]
        surrogate = prepared.working["SURROGATE"]
        if wave == 0 or 1 <= wave < BOOTSTRAP_WAVES:
            tpe.tell(
                [
                    physical_results[row.exact_identity].optimizer_observation(
                        row.optimizer_proposal_id
                    )
                    for row in prepared.asks
                ]
            )
            self._inner._tpe_snapshots[POLICY_TPE] = tpe.snapshot()

        if wave == BOOTSTRAP_WAVES - 1:
            surrogate = self._inner._new_surrogate(
                seed=self.surrogate_seed,
                seen=tuple(
                    dict.fromkeys(
                        row["exact_identity"] for row in completed[POLICY_D1]
                    )
                ),
            )
            surrogate.ingest_completed_observations(completed[POLICY_D1])
        elif wave >= BOOTSTRAP_WAVES:
            if wave in UNIFORM_FLOOR_WAVES:
                surrogate.ingest_completed_observations(
                    [
                        physical_results[row.exact_identity].surrogate_row(
                            row.logical_proposal_id
                        )
                        for row in prepared.asks
                    ]
                )
            else:
                surrogate.tell(
                    [
                        physical_results[row.exact_identity].optimizer_observation(
                            row.optimizer_proposal_id
                        )
                        for row in prepared.asks
                    ]
                )
        self._inner._d1_surrogate_snapshot = surrogate.snapshot()

        used = copy.deepcopy(self._inner._used)
        group_counts = copy.deepcopy(self._inner._group_counts)
        for ask in prepared.asks:
            if ask.exact_identity in used[POLICY_D1]:
                raise RuntimeError("D1_COHORT_DUPLICATE_EXACT_SELECTION")
            used[POLICY_D1].add(ask.exact_identity)
            group = self._inner.group_by_exact_identity[ask.exact_identity]
            counts = group_counts[POLICY_D1][ask.template_id]
            counts[group] = counts.get(group, 0) + 1
            if counts[group] > self.maximum_per_group:
                raise RuntimeError("D1_COHORT_GROUP_CAPACITY_DRIFT")

        self._inner._used = used
        self._inner._group_counts = group_counts
        self._inner._completed_rows = completed
        self._inner.wave_index += 1
        self._assert_other_policies_unused()
        return {
            "schema_version": "cn_program_optimizer_d1_wave_commit_v1",
            "policy_id": D1_POLICY_ID,
            "wave_index": wave,
            "logical_ask_count": len(prepared.asks),
            "physical_unique_count": len(expected),
            "logical_receipts": logical_receipts,
        }

    def selector_counts(self) -> dict[str, int]:
        counts = {
            D1_SELECTION_UNIFORM: 0,
            D1_SELECTION_TPE: 0,
            D1_SELECTION_SURROGATE: 0,
        }
        for row in self._inner._completed_rows[POLICY_D1]:
            kind = str(row["selection_kind"])
            if kind not in counts:
                raise RuntimeError("D1_COHORT_UNKNOWN_SELECTION_KIND")
            counts[kind] += 1
        return counts

    def snapshot(self) -> dict[str, Any]:
        self._assert_other_policies_unused()
        payload = {
            "schema_version": "cn_program_optimizer_d1_cohort_state_v1",
            "policy_id": D1_POLICY_ID,
            "wave_index": self.wave_index,
            "selector_counts": self.selector_counts(),
            "inner_successor_state": self._inner.snapshot(),
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
        surrogate_seed: int,
        tpe_config: Mapping[str, Any],
        surrogate_config: Mapping[str, Any],
        prior_exact_identities: Sequence[str] = (),
        minimum_distinct_groups: int = 16,
        maximum_per_group: int = 4,
    ) -> "ProgramOptimizerD1CohortV1":
        source = copy.deepcopy(dict(snapshot))
        claimed = str(source.pop("state_hash", ""))
        if not claimed or stable_hash(source) != claimed:
            raise ValueError("D1_COHORT_STATE_SELF_HASH_DRIFT")
        if (
            str(source.get("schema_version") or "")
            != "cn_program_optimizer_d1_cohort_state_v1"
            or str(source.get("policy_id") or "") != D1_POLICY_ID
        ):
            raise ValueError("D1_COHORT_STATE_SCHEMA_DRIFT")
        instance = cls(
            entries=entries,
            template_ids=template_ids,
            group_by_exact_identity=group_by_exact_identity,
            uniform_seed=uniform_seed,
            tpe_seed=tpe_seed,
            surrogate_seed=surrogate_seed,
            tpe_config=tpe_config,
            surrogate_config=surrogate_config,
            prior_exact_identities=prior_exact_identities,
            minimum_distinct_groups=minimum_distinct_groups,
            maximum_per_group=maximum_per_group,
        )
        instance._inner = ProgramOptimizerSuccessorBenchmarkV1.restore(
            source["inner_successor_state"],
            entries=entries,
            template_ids=template_ids,
            group_by_exact_identity=group_by_exact_identity,
            uniform_seed=uniform_seed,
            tpe_seed=tpe_seed,
            d1_surrogate_seed=surrogate_seed,
            d2_surrogate_seed=surrogate_seed,
            tpe_config=tpe_config,
            surrogate_config=surrogate_config,
            prior_exact_identities=prior_exact_identities,
            minimum_distinct_groups=minimum_distinct_groups,
            maximum_per_group=maximum_per_group,
        )
        if instance.snapshot() != dict(snapshot):
            raise RuntimeError("D1_COHORT_STATE_RESTORE_DRIFT")
        return instance

    def verify_terminal_shape(self) -> None:
        if self.wave_index != TOTAL_WAVES:
            raise RuntimeError("D1_COHORT_NOT_TERMINAL")
        if len(self._inner._completed_rows[POLICY_D1]) != D1_LOGICAL_RECORDS:
            raise RuntimeError("D1_COHORT_TERMINAL_CARDINALITY_DRIFT")
        if self.selector_counts() != D1_SELECTOR_COUNTS:
            raise RuntimeError("D1_COHORT_SELECTOR_COUNT_DRIFT")
        self._assert_other_policies_unused()
