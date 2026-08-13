"""Shared typed Program optimizer contract for the prospective VNext tournament.

The adapters in this module choose whole Programs only.  Candidate Program
composition, compilation, materialization, evaluation, admission, and uplift
remain the existing authorities outside this module.
"""

from __future__ import annotations

import copy
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from our_system_phase2.services.candidate_program_proposal_v0 import (
    DEFAULT_COMBINATION_POLICY,
    PROGRAM_TEMPLATE_COMPONENTS,
    ProgramSourceComponentV0,
)
from our_system_phase2.services.candidate_program_v1 import (
    CandidateProgramSpecV1,
    route_generation_receipt_v1,
)
from our_system_phase2.services.optuna_tpe_search_adapter import (
    EVALUATED,
    RouteConditionalTPESearchAdapter,
)
from our_system_phase2.services.route_local_availability import (
    AvailabilityEmission,
    AvailabilityEntry,
    RouteLocalAvailabilityController,
    structural_bucket_key,
)
from our_system_phase2.services.search_v2_admission import (
    AbsoluteEconomicAdmission,
)
from our_system_phase2.services.search_v2_conditional_uplift import (
    ProgramUpliftCredit,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


PROGRAM_SPACE_ID = "CN_TYPED_PROGRAM_GENE_SPACE_V1"
PROGRAM_ROUTE_ID = "CN_CANDIDATE_PROGRAM_V1"
INACTIVE_PROGRAM_GENE = "__INACTIVE__"


def _inactive_program_gene(slot: str) -> str:
    return (
        f"{INACTIVE_PROGRAM_GENE}::{INACTIVE_PROGRAM_GENE}"
        if str(slot).endswith("field_pair_id")
        else INACTIVE_PROGRAM_GENE
    )
UNIFORM_CONTROL = "UNIFORM_CONTROL"
HYBRID_TPE_PROGRAM = "HYBRID_TPE_PROGRAM"
STRUCTURED_SURROGATE_PROGRAM = "STRUCTURED_SURROGATE_PROGRAM"
PROGRAM_OPTIMIZER_ARMS = (
    UNIFORM_CONTROL,
    HYBRID_TPE_PROGRAM,
    STRUCTURED_SURROGATE_PROGRAM,
)
PROGRAM_OBJECTIVE_KEY = "matched_cumulative_return_increment"
PROGRAM_CLOCK_KEYS = (
    "joint_eligible_from_policy",
    "joint_support_policy",
    "action_session_policy",
    "pit_guard_version",
)


def _normalized(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _normalized(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_normalized(item) for item in value]
    return value


def program_structural_genes_v1(
    *,
    program_template_id: str,
    components: Mapping[str, ProgramSourceComponentV0],
    combination_policy: Mapping[str, str] | None,
    program: CandidateProgramSpecV1,
    compiled: Any,
) -> dict[str, str]:
    """Compose route receipts and compiled Program structure into typed genes."""

    roles = PROGRAM_TEMPLATE_COMPONENTS.get(str(program_template_id))
    if roles is None or set(components) != set(roles):
        raise ValueError("PROGRAM_GENE_TEMPLATE_COMPONENT_COVERAGE_DRIFT")
    policy = {**DEFAULT_COMBINATION_POLICY, **dict(combination_policy or {})}
    record = program.to_record()
    nodes = tuple(record["nodes"])
    genes: dict[str, str] = {
        "skeleton_id": PROGRAM_SPACE_ID,
        "gene_surface_id": PROGRAM_SPACE_ID,
        "program_template_id": str(program_template_id),
        "active_component_roles": "+".join(roles),
        "composition_topology": ">".join(roles),
        "combination_temporal": str(policy["temporal"]),
        "combination_market": str(policy["market"]),
        "combination_event_episode": str(policy["event_episode"]),
        "combination_event_application": str(policy["event_application"]),
        "joint_clock_class": stable_hash(
            {
                key: getattr(program.joint_clock_contract, key)
                for key in PROGRAM_CLOCK_KEYS
            }
        )[:24],
        "lag_class": stable_hash(dict(compiled.field_lags))[:24],
        "structural_complexity_class": stable_hash(
            dict(compiled.complexity_report)
        )[:24],
        "raw_field_count": str(len(compiled.physical_leaf_ids)),
        "rolling_node_count": str(
            sum(
                "ROLL" in str(node["node_type"])
                or "WINDOW" in str(node["node_type"])
                for node in nodes
            )
        ),
        "interaction_topology": stable_hash(
            [
                {
                    "node_type": node["node_type"],
                    "inputs": node["input_node_ids"],
                }
                for node in nodes
                if len(node["input_node_ids"]) > 1
            ]
        )[:24],
    }
    for role in roles:
        component = components[role]
        receipt = route_generation_receipt_v1(component.primary)
        prefix = f"{role}__"
        genes[prefix + "route_id"] = component.route_id
        genes[prefix + "skeleton_id"] = str(receipt["skeleton_id"])
        categorical = dict(receipt.get("categorical_genes") or {})
        for slot, value in sorted(categorical.items()):
            genes[prefix + str(slot)] = str(value)
    return genes


def program_optimizer_lane_v1(
    entries: Sequence[AvailabilityEntry],
) -> dict[str, dict[str, Any]]:
    if not entries:
        raise ValueError("PROGRAM_OPTIMIZER_SPACE_EMPTY")
    slots = tuple(dict(entries[0].genes))
    for entry in entries:
        if tuple(dict(entry.genes)) != slots:
            raise ValueError("PROGRAM_OPTIMIZER_GENE_SLOT_DRIFT")
    return {
        PROGRAM_SPACE_ID: {
            "ordered_categories_by_slot": {
                **{
                    slot: list(
                        dict.fromkeys(
                            str(entry.genes[slot]) for entry in entries
                        )
                    )
                    for slot in slots
                },
            }
        }
    }


@dataclass(frozen=True, slots=True)
class ProgramOptimizerObservationV1:
    proposal_id: str
    exact_identity: str
    admission: AbsoluteEconomicAdmission
    uplift: ProgramUpliftCredit | None

    def __post_init__(self) -> None:
        if self.admission.admitted != (self.uplift is not None):
            raise ValueError("PROGRAM_OPTIMIZER_DUAL_HEAD_DOMAIN_DRIFT")

    @property
    def conditional_objective(self) -> float | None:
        if self.uplift is None:
            return None
        value = self.uplift.program_credit.get(PROGRAM_OBJECTIVE_KEY)
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("PROGRAM_OPTIMIZER_UPLIFT_OBJECTIVE_INVALID")
        return parsed


class ProgramSearchOptimizerAdapter(ABC):
    @abstractmethod
    def ask(
        self,
        *,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str | None = None,
        eligible_exact_identities: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def tell(
        self, observations: Sequence[ProgramOptimizerObservationV1]
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def snapshot(self) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def restore(cls, **kwargs: Any) -> "ProgramSearchOptimizerAdapter":
        raise NotImplementedError

    @abstractmethod
    def proposal_identity(self, ask: Mapping[str, Any]) -> str:
        raise NotImplementedError

    @abstractmethod
    def optimizer_metadata(self) -> dict[str, Any]:
        raise NotImplementedError


class _AvailabilityProgramOptimizer(ProgramSearchOptimizerAdapter):
    def __init__(
        self,
        *,
        entries: Sequence[AvailabilityEntry],
        seen_exact_identities: Sequence[str],
        seed: int,
        arm: str,
    ) -> None:
        self.arm = str(arm)
        self.entries = tuple(copy.deepcopy(tuple(entries)))
        self.seen_exact_identities = tuple(map(str, seen_exact_identities))
        self.controller = RouteLocalAvailabilityController(
            entries=self.entries,
            seen_exact_identities=self.seen_exact_identities,
            emitter_seed=int(seed),
            input_hashes={
                "program_space": stable_hash(
                    [entry.to_dict() for entry in self.entries]
                )
            },
        )
        self._pending: dict[str, dict[str, Any]] = {}
        self._history: list[dict[str, Any]] = []

    @property
    def observation_count(self) -> int:
        return int(sum(int(row.get("asked_count", 0)) for row in self._history))

    def proposal_identity(self, ask: Mapping[str, Any]) -> str:
        identity = str(ask.get("proposal_id") or "")
        if not identity:
            raise ValueError("PROGRAM_OPTIMIZER_PROPOSAL_ID_MISSING")
        return identity

    def _remaining_entries(
        self,
        required_program_template_id: str | None,
        eligible_exact_identities: Sequence[str] | None = None,
    ) -> tuple[AvailabilityEntry, ...]:
        remaining = self.controller.remaining_entries(route_id=PROGRAM_ROUTE_ID)
        allowed = (
            None
            if eligible_exact_identities is None
            else frozenset(map(str, eligible_exact_identities))
        )
        template_id = (
            None
            if required_program_template_id is None
            else str(required_program_template_id)
        )
        return tuple(
            entry
            for entry in remaining
            if (template_id is None or str(entry.genes["program_template_id"]) == template_id)
            and (allowed is None or entry.exact_identity in allowed)
        )

    def _ask_row(
        self,
        *,
        emission: AvailabilityEmission,
        checkpoint_id: str,
        ask_ordinal: int,
        proposal_id: str,
        trial_number: int | None,
        optimizer_ask_identity: str,
        acquisition: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "schema_version": "cn_program_optimizer_ask_v1",
            "optimizer_arm": self.arm,
            "proposal_id": str(proposal_id),
            "optimizer_ask_identity": str(optimizer_ask_identity),
            "trial_number": trial_number,
            "checkpoint_id": str(checkpoint_id),
            "ask_ordinal": int(ask_ordinal),
            "exact_identity": emission.exact_identity,
            "control_exact_identity": emission.control_exact_identity,
            "program_genes": dict(emission.genes),
            "program_gene_hash": stable_hash(dict(emission.genes)),
            "program_space_id": PROGRAM_SPACE_ID,
            "availability": emission.to_dict(),
            "acquisition": dict(acquisition or {}),
        }

    def snapshot(self) -> dict[str, Any]:
        if self._pending:
            raise RuntimeError("PROGRAM_OPTIMIZER_SNAPSHOT_HAS_PENDING")
        payload = {
            "schema_version": "cn_program_optimizer_snapshot_v1",
            "optimizer_arm": self.arm,
            "availability": self.controller.snapshot(),
            "history": copy.deepcopy(self._history),
            "optimizer_metadata": self.optimizer_metadata(),
        }
        payload["snapshot_hash"] = stable_hash(payload)
        return payload


class UniformProgramSearchAdapter(_AvailabilityProgramOptimizer):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(arm=UNIFORM_CONTROL, **kwargs)

    @property
    def observation_count(self) -> int:
        return 0

    def ask(
        self,
        *,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str | None = None,
        eligible_exact_identities: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        if self._pending:
            raise RuntimeError("PROGRAM_OPTIMIZER_PENDING_NOT_TOLD")
        asked = []
        for ordinal in range(int(count)):
            remaining = self._remaining_entries(
                required_program_template_id, eligible_exact_identities
            )
            if not remaining:
                break
            entry = min(
                remaining,
                key=lambda row: stable_hash(
                    {
                        "seed": self.controller.emitter_seed,
                        "emitter": "PROGRAM_UNIFORM_HASH_PERMUTATION_V1",
                        "exact_identity": row.exact_identity,
                    }
                ),
            )
            emission = self.controller.reserve_exact(
                route_id=PROGRAM_ROUTE_ID,
                exact_identity=entry.exact_identity,
                emission_mode="PROGRAM_AVAILABILITY_AWARE_UNIFORM",
                source_exact_identity="PROGRAM_UNIFORM_HASH_PERMUTATION",
            )
            if emission is None:
                break
            proposal_id = stable_hash(
                {
                    "arm": self.arm,
                    "checkpoint_id": checkpoint_id,
                    "ask_ordinal": ordinal,
                    "exact_identity": emission.exact_identity,
                }
            )[:24]
            row = self._ask_row(
                emission=emission,
                checkpoint_id=checkpoint_id,
                ask_ordinal=ordinal,
                proposal_id=proposal_id,
                trial_number=None,
                optimizer_ask_identity=proposal_id,
            )
            asked.append(row)
            self._pending[proposal_id] = row
        return asked

    def tell(
        self, observations: Sequence[ProgramOptimizerObservationV1]
    ) -> dict[str, Any]:
        by_id = {row.proposal_id: row for row in observations}
        if set(by_id) != set(self._pending):
            raise RuntimeError("PROGRAM_OPTIMIZER_ASK_TELL_COVERAGE_DRIFT")
        receipt = {
            "schema_version": "cn_uniform_program_tell_receipt_v1",
            "optimizer_arm": self.arm,
            "asked_count": len(by_id),
            "optimizer_feedback_applied": False,
            "economic_observations_retained": 0,
            "program_level_credit_only": True,
            "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
        }
        self._history.append(receipt)
        self._pending.clear()
        return receipt

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            "optimizer_arm": self.arm,
            "policy_id": "PROGRAM_AVAILABILITY_AWARE_UNIFORM_V1",
            "program_space_id": PROGRAM_SPACE_ID,
            "learning": False,
        }

    @classmethod
    def restore(cls, **kwargs: Any) -> "UniformProgramSearchAdapter":
        snapshot = dict(kwargs.pop("snapshot"))
        adapter = cls(**kwargs)
        adapter.controller = RouteLocalAvailabilityController.restore(
            entries=adapter.entries,
            seen_exact_identities=adapter.seen_exact_identities,
            state=dict(snapshot["availability"]),
            input_hashes=adapter.controller.input_hashes,
        )
        adapter._history = copy.deepcopy(list(snapshot["history"]))
        return adapter


class HybridTPEProgramSearchAdapter(_AvailabilityProgramOptimizer):
    def __init__(
        self,
        *,
        n_startup_trials: int = 24,
        n_ei_candidates: int = 64,
        **kwargs: Any,
    ) -> None:
        super().__init__(arm=HYBRID_TPE_PROGRAM, **kwargs)
        self.tpe = RouteConditionalTPESearchAdapter(
            route_id=PROGRAM_ROUTE_ID,
            lane_spaces=program_optimizer_lane_v1(self.entries),
            seed=self.controller.emitter_seed,
            n_startup_trials=int(n_startup_trials),
            n_ei_candidates=int(n_ei_candidates),
            multivariate=True,
            group=True,
            constant_liar=True,
            constraints_enabled=True,
        )
        self._tpe_internal_observations: dict[str, dict[str, Any]] = {}

    def ask(
        self,
        *,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str | None = None,
        eligible_exact_identities: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        if self._pending:
            raise RuntimeError("PROGRAM_OPTIMIZER_PENDING_NOT_TOLD")
        asked: list[dict[str, Any]] = []
        for ordinal in range(int(count)):
            native = self.tpe.ask_trial(
                checkpoint_id=checkpoint_id,
                ask_ordinal=ordinal,
            )
            self.controller.record_optimizer_draw(PROGRAM_ROUTE_ID)
            genes = dict(native["genes"])
            exact_identity = stable_hash(genes)
            remaining_by_identity = {
                entry.exact_identity
                for entry in self._remaining_entries(
                    required_program_template_id, eligible_exact_identities
                )
            }
            emission = (
                self.controller.reserve_exact(
                    route_id=PROGRAM_ROUTE_ID,
                    exact_identity=exact_identity,
                    emission_mode="PROGRAM_TPE_DIRECT_FRESH",
                    source_exact_identity=exact_identity,
                )
                if exact_identity in remaining_by_identity
                else None
            )
            if emission is None:
                bucket = structural_bucket_key(PROGRAM_ROUTE_ID, genes)
                candidates = [
                    entry
                    for entry in self._remaining_entries(
                        required_program_template_id, eligible_exact_identities
                    )
                    if entry.bucket_key == bucket
                ]
                replacement = candidates[0] if candidates else None
                if replacement is None:
                    remaining = self._remaining_entries(
                        required_program_template_id, eligible_exact_identities
                    )
                    replacement = remaining[0] if remaining else None
                if replacement is None:
                    self._tpe_internal_observations[str(native["proposal_id"])] = {
                        "proposal_id": str(native["proposal_id"]),
                        "outcome_class": "AVAILABILITY_EXHAUSTED",
                        "optimizer_reward": None,
                        "outcome_reason": "PROGRAM_EXACT_SUPPLY_EXHAUSTED",
                    }
                    break
                self._tpe_internal_observations[str(native["proposal_id"])] = {
                    "proposal_id": str(native["proposal_id"]),
                    "outcome_class": "AVAILABILITY_REPLACED",
                    "optimizer_reward": None,
                    "outcome_reason": "PROGRAM_EXACT_ALREADY_SEEN",
                }
                native = self.tpe.enqueue_fixed_trial(
                    checkpoint_id=checkpoint_id,
                    genes=replacement.genes,
                    metadata={
                        "source_optimizer_trial_number": int(
                            native["trial_number"]
                        ),
                        "program_level_trial": True,
                    },
                )
                self.controller.record_optimizer_draw(PROGRAM_ROUTE_ID)
                emission = self.controller.reserve_exact(
                    route_id=PROGRAM_ROUTE_ID,
                    exact_identity=replacement.exact_identity,
                    emission_mode="PROGRAM_TPE_AVAILABILITY_REPLACEMENT",
                    source_exact_identity=exact_identity,
                )
            if emission is None:  # pragma: no cover - reserved above.
                raise RuntimeError("PROGRAM_TPE_AVAILABILITY_RESERVATION_FAILED")
            row = self._ask_row(
                emission=emission,
                checkpoint_id=checkpoint_id,
                ask_ordinal=ordinal,
                proposal_id=str(native["proposal_id"]),
                trial_number=int(native["trial_number"]),
                optimizer_ask_identity=str(native["proposal_id"]),
                acquisition={"source": "official_optuna.samplers.TPESampler"},
            )
            self.tpe.annotate_pending_trial(
                row["proposal_id"],
                {
                    "program_exact_identity": emission.exact_identity,
                    "program_gene_hash": row["program_gene_hash"],
                },
            )
            asked.append(row)
            self._pending[row["proposal_id"]] = row
        return asked

    def tell(
        self, observations: Sequence[ProgramOptimizerObservationV1]
    ) -> dict[str, Any]:
        by_id = {row.proposal_id: row for row in observations}
        if set(by_id) != set(self._pending):
            raise RuntimeError("PROGRAM_OPTIMIZER_ASK_TELL_COVERAGE_DRIFT")
        tpe_observations = list(self._tpe_internal_observations.values())
        for proposal_id in self._pending:
            row = by_id[proposal_id]
            tpe_observations.append(
                {
                    "proposal_id": proposal_id,
                    "outcome_class": EVALUATED,
                    "optimizer_reward": row.conditional_objective,
                    "objective_domain_eligible": row.admission.admitted,
                    "admission_constraint_violation": (
                        0.0 if row.admission.admitted else 1.0
                    ),
                    "outcome_reason": (
                        "ADMITTED_CONDITIONAL_UPLIFT"
                        if row.admission.admitted
                        else "ABSOLUTE_ADMISSION_FAILED"
                    ),
                }
            )
        tpe_receipt = self.tpe.tell_population(tpe_observations)
        receipt = {
            "schema_version": "cn_hybrid_tpe_program_tell_receipt_v1",
            "optimizer_arm": self.arm,
            "tpe_receipt": tpe_receipt,
            "program_level_credit_only": True,
            "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
            "absolute_admission_scalarized_with_uplift": False,
            "asked_count": len(by_id),
        }
        self._history.append(receipt)
        self._pending.clear()
        self._tpe_internal_observations.clear()
        return receipt

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            "optimizer_arm": self.arm,
            "program_space_id": PROGRAM_SPACE_ID,
            **self.tpe.environment_receipt(),
        }

    @classmethod
    def restore(cls, **kwargs: Any) -> "HybridTPEProgramSearchAdapter":
        snapshot = dict(kwargs.pop("snapshot"))
        adapter = cls(**kwargs)
        adapter.controller = RouteLocalAvailabilityController.restore(
            entries=adapter.entries,
            seen_exact_identities=adapter.seen_exact_identities,
            state=dict(snapshot["availability"]),
            input_hashes=adapter.controller.input_hashes,
        )
        adapter.tpe = RouteConditionalTPESearchAdapter.restore_trials(
            route_id=PROGRAM_ROUTE_ID,
            lane_spaces=program_optimizer_lane_v1(adapter.entries),
            seed=adapter.controller.emitter_seed,
            transcripts=list(snapshot["tpe_history"]),
            n_startup_trials=adapter.tpe.n_startup_trials,
            n_ei_candidates=adapter.tpe.n_ei_candidates,
            multivariate=True,
            group=True,
            constant_liar=True,
            constraints_enabled=True,
        )
        adapter.tpe.restore_mode = str(
            dict(snapshot["optimizer_metadata"])["restore_mode"]
        )
        adapter._history = copy.deepcopy(list(snapshot["history"]))
        adapter._tpe_internal_observations.clear()
        return adapter

    def snapshot(self) -> dict[str, Any]:
        payload = super().snapshot()
        payload.pop("snapshot_hash")
        payload["tpe_history"] = self.tpe.history
        payload["snapshot_hash"] = stable_hash(payload)
        return payload


class StructuredSurrogateProgramSearchAdapter(_AvailabilityProgramOptimizer):
    def __init__(
        self,
        *,
        cold_start_asks: int = 24,
        candidate_pool_size: int = 256,
        n_estimators: int = 256,
        min_samples_leaf: int = 2,
        exploration_beta: float = 1.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(arm=STRUCTURED_SURROGATE_PROGRAM, **kwargs)
        self.cold_start_asks = int(cold_start_asks)
        self.candidate_pool_size = int(candidate_pool_size)
        self.n_estimators = int(n_estimators)
        self.min_samples_leaf = int(min_samples_leaf)
        self.exploration_beta = float(exploration_beta)
        if min(
            self.cold_start_asks,
            self.candidate_pool_size,
            self.n_estimators,
            self.min_samples_leaf,
        ) < 1:
            raise ValueError("PROGRAM_SURROGATE_CONFIG_INVALID")
        self._observations: list[dict[str, Any]] = []
        self._categories = self._freeze_categories()
        self._feature_names = tuple(
            f"{slot}=={value}"
            for slot, values in self._categories.items()
            for value in values
        )

    def _freeze_categories(self) -> dict[str, tuple[str, ...]]:
        slots = tuple(dict(self.entries[0].genes))
        return {
            slot: tuple(
                dict.fromkeys(str(entry.genes[slot]) for entry in self.entries)
            )
            for slot in slots
        }

    def _encode(self, genes: Mapping[str, Any]) -> np.ndarray:
        values = {str(key): str(value) for key, value in genes.items()}
        return np.asarray(
            [
                1.0 if values.get(slot) == category else 0.0
                for slot, categories in self._categories.items()
                for category in categories
            ],
            dtype=float,
        )

    @staticmethod
    def _tree_distribution(model: Any, rows: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        predictions = np.asarray(
            [tree.predict(rows) for tree in model.estimators_], dtype=float
        )
        return predictions.mean(axis=0), predictions.std(axis=0)

    def _fit_models(self) -> tuple[Any, Any | None]:
        from sklearn.ensemble import ExtraTreesClassifier, ExtraTreesRegressor

        rows = np.vstack([self._encode(row["genes"]) for row in self._observations])
        admission = np.asarray(
            [bool(row["admitted"]) for row in self._observations], dtype=int
        )
        admission_model = ExtraTreesClassifier(
            n_estimators=self.n_estimators,
            min_samples_leaf=self.min_samples_leaf,
            max_features="sqrt",
            class_weight="balanced",
            random_state=self.controller.emitter_seed,
            n_jobs=1,
        ).fit(rows, admission)
        eligible = [
            index
            for index, row in enumerate(self._observations)
            if bool(row["admitted"])
        ]
        uplift_model = None
        if len(eligible) >= max(4, self.min_samples_leaf * 2):
            uplift_model = ExtraTreesRegressor(
                n_estimators=self.n_estimators,
                min_samples_leaf=self.min_samples_leaf,
                max_features="sqrt",
                random_state=self.controller.emitter_seed + 1,
                n_jobs=1,
            ).fit(
                rows[eligible],
                np.asarray(
                    [self._observations[index]["uplift"] for index in eligible],
                    dtype=float,
                ),
            )
        return admission_model, uplift_model

    def _acquisition_rows(
        self, entries: Sequence[AvailabilityEntry]
    ) -> list[dict[str, Any]]:
        if len(self._observations) < self.cold_start_asks:
            return [
                {
                    "entry": entry,
                    "cold_start": True,
                    "acquisition": None,
                }
                for entry in entries
            ]
        admission_model, uplift_model = self._fit_models()
        matrix = np.vstack([self._encode(entry.genes) for entry in entries])
        classes = tuple(int(value) for value in admission_model.classes_)
        probabilities = admission_model.predict_proba(matrix)
        feasible_probability = (
            probabilities[:, classes.index(1)]
            if 1 in classes
            else np.ones(len(entries), dtype=float)
            if classes == (1,)
            else np.zeros(len(entries), dtype=float)
        )
        if uplift_model is None:
            uplift_mean = np.zeros(len(entries), dtype=float)
            uplift_std = np.ones(len(entries), dtype=float)
            cold_head_b = True
        else:
            uplift_mean, uplift_std = self._tree_distribution(
                uplift_model, matrix
            )
            cold_head_b = False
        positive_uplift_acquisition = np.maximum(
            0.0, uplift_mean + self.exploration_beta * uplift_std
        )
        acquisition = feasible_probability * positive_uplift_acquisition
        return [
            {
                "entry": entry,
                "cold_start": False,
                "head_b_cold_start": cold_head_b,
                "feasibility_probability": float(feasible_probability[index]),
                "uplift_mean": float(uplift_mean[index]),
                "uplift_ensemble_dispersion": float(uplift_std[index]),
                "positive_uplift_acquisition": float(
                    positive_uplift_acquisition[index]
                ),
                "acquisition": float(acquisition[index]),
            }
            for index, entry in enumerate(entries)
        ]

    def predict_acquisition(
        self, genes_rows: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        entries = [
            AvailabilityEntry(
                route_id=PROGRAM_ROUTE_ID,
                bucket_key=structural_bucket_key(PROGRAM_ROUTE_ID, genes),
                exact_identity=stable_hash(dict(genes)),
                control_exact_identity="",
                genes=dict(genes),
            )
            for genes in genes_rows
        ]
        return [
            {key: value for key, value in row.items() if key != "entry"}
            for row in self._acquisition_rows(entries)
        ]

    def ask(
        self,
        *,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str | None = None,
        eligible_exact_identities: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        if self._pending:
            raise RuntimeError("PROGRAM_OPTIMIZER_PENDING_NOT_TOLD")
        remaining = self._remaining_entries(
            required_program_template_id, eligible_exact_identities
        )
        pool = remaining[: self.candidate_pool_size]
        scored = self._acquisition_rows(pool)
        scored.sort(
            key=lambda row: (
                -float(row["acquisition"] or 0.0),
                stable_hash(row["entry"].to_dict()),
            )
        )
        asked: list[dict[str, Any]] = []
        for ordinal, score in enumerate(scored[: int(count)]):
            entry = score["entry"]
            emission = self.controller.reserve_exact(
                route_id=PROGRAM_ROUTE_ID,
                exact_identity=entry.exact_identity,
                emission_mode="STRUCTURED_SURROGATE_ACQUISITION",
                source_exact_identity=stable_hash(
                    {key: value for key, value in score.items() if key != "entry"}
                ),
            )
            if emission is None:
                continue
            proposal_id = stable_hash(
                {
                    "arm": self.arm,
                    "checkpoint_id": checkpoint_id,
                    "ask_ordinal": ordinal,
                    "exact_identity": emission.exact_identity,
                    "observation_count": len(self._observations),
                }
            )[:24]
            row = self._ask_row(
                emission=emission,
                checkpoint_id=checkpoint_id,
                ask_ordinal=ordinal,
                proposal_id=proposal_id,
                trial_number=None,
                optimizer_ask_identity=proposal_id,
                acquisition={
                    key: value for key, value in score.items() if key != "entry"
                },
            )
            asked.append(row)
            self._pending[proposal_id] = row
        return asked

    def tell(
        self, observations: Sequence[ProgramOptimizerObservationV1]
    ) -> dict[str, Any]:
        by_id = {row.proposal_id: row for row in observations}
        if set(by_id) != set(self._pending):
            raise RuntimeError("PROGRAM_OPTIMIZER_ASK_TELL_COVERAGE_DRIFT")
        records = []
        for proposal_id, ask in self._pending.items():
            observation = by_id[proposal_id]
            row = {
                "proposal_id": proposal_id,
                "exact_identity": observation.exact_identity,
                "genes": dict(ask["program_genes"]),
                "admitted": observation.admission.admitted,
                "uplift": observation.conditional_objective,
                "admission_record": observation.admission.to_record(),
                "uplift_record": (
                    observation.uplift.to_record()
                    if observation.uplift is not None
                    else None
                ),
                "program_level_credit_only": True,
                "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
            }
            records.append(row)
            self._observations.append(row)
        receipt = {
            "schema_version": "cn_structured_surrogate_program_tell_receipt_v1",
            "optimizer_arm": self.arm,
            "asked_count": len(records),
            "admission_head_observation_count": len(records),
            "conditional_head_observation_count": sum(
                row["uplift"] is not None for row in records
            ),
            "observation_digest": stable_hash(records),
            "program_level_credit_only": True,
            "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
        }
        self._history.append(receipt)
        self._pending.clear()
        return receipt

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            "optimizer_arm": self.arm,
            "policy_id": "SKLEARN_EXTRA_TREES_CONSTRAINED_PROGRAM_BO_V1",
            "implementation": "sklearn.ensemble.ExtraTreesClassifier+ExtraTreesRegressor",
            "program_space_id": PROGRAM_SPACE_ID,
            "conditional_hierarchical_encoding": "FROZEN_TYPED_ONE_HOT_WITH_INACTIVE_SENTINELS",
            "feasibility_head": "ExtraTreesClassifier",
            "conditional_uplift_head": "ExtraTreesRegressor_ADMITTED_ONLY",
            "uncertainty": "PER_TREE_ENSEMBLE_DISPERSION",
            "acquisition": "P_ADMISSION_TIMES_POSITIVE_UPLIFT_UCB",
            "cold_start_asks": self.cold_start_asks,
            "candidate_pool_size": self.candidate_pool_size,
            "n_estimators": self.n_estimators,
            "min_samples_leaf": self.min_samples_leaf,
            "exploration_beta": self.exploration_beta,
            "seed": self.controller.emitter_seed,
            "feature_count": len(self._feature_names),
            "feature_schema_hash": stable_hash(self._feature_names),
        }

    def snapshot(self) -> dict[str, Any]:
        payload = super().snapshot()
        payload.pop("snapshot_hash")
        payload["observations"] = copy.deepcopy(self._observations)
        payload["categories"] = copy.deepcopy(self._categories)
        payload["snapshot_hash"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(
        cls, **kwargs: Any
    ) -> "StructuredSurrogateProgramSearchAdapter":
        snapshot = dict(kwargs.pop("snapshot"))
        adapter = cls(**kwargs)
        adapter.controller = RouteLocalAvailabilityController.restore(
            entries=adapter.entries,
            seen_exact_identities=adapter.seen_exact_identities,
            state=dict(snapshot["availability"]),
            input_hashes=adapter.controller.input_hashes,
        )
        adapter._history = copy.deepcopy(list(snapshot["history"]))
        adapter._observations = copy.deepcopy(list(snapshot["observations"]))
        if _normalized(adapter._categories) != _normalized(snapshot["categories"]):
            raise RuntimeError("PROGRAM_SURROGATE_FEATURE_SCHEMA_DRIFT")
        return adapter


def program_availability_entries_v1(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[AvailabilityEntry, ...]:
    if not rows:
        raise ValueError("PROGRAM_AVAILABILITY_SPACE_EMPTY")
    gene_rows = [
        {str(key): str(value) for key, value in dict(row["genes"]).items()}
        for row in rows
    ]
    slots = tuple(sorted({slot for genes in gene_rows for slot in genes}))
    entries = []
    for row, source_genes in zip(rows, gene_rows, strict=True):
        genes = {
            slot: source_genes.get(slot, _inactive_program_gene(slot))
            for slot in slots
        }
        identity = stable_hash(genes)
        entries.append(
            AvailabilityEntry(
                route_id=PROGRAM_ROUTE_ID,
                bucket_key=structural_bucket_key(PROGRAM_ROUTE_ID, genes),
                exact_identity=identity,
                control_exact_identity=str(
                    row.get("control_exact_identity") or ""
                ),
                genes=genes,
            )
        )
    if len({entry.exact_identity for entry in entries}) != len(entries):
        raise ValueError("PROGRAM_AVAILABILITY_EXACT_DUPLICATE")
    return tuple(entries)


def normalized_program_gene_identity_v1(
    genes: Mapping[str, Any], *, ordered_slots: Sequence[str]
) -> str:
    source = {str(key): str(value) for key, value in genes.items()}
    slots = tuple(map(str, ordered_slots))
    if any(slot not in slots for slot in source):
        raise ValueError("PROGRAM_GENE_OUTSIDE_FROZEN_SLOT_SCHEMA")
    return stable_hash(
        {slot: source.get(slot, _inactive_program_gene(slot)) for slot in slots}
    )
