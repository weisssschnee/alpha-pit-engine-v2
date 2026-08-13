"""Thin official Optuna TPE adapter over route-local typed Grammar lanes.

The adapter chooses categorical genes only.  Registry route authority, formula
construction, compilation, exact/behavior admission, evaluation, and access
control remain outside this module.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from our_system_phase2.services.source_route_sampling_phase_v0 import (
    source_sampling_phase_v0,
)


EXPECTED_OPTUNA_VERSION = "4.8.0"
EVALUATED = "EVALUATED"
PRUNED = "LIVE_RUNNER_PRUNED"
ADMISSION_CONSTRAINT_USER_ATTR = "admission_constraint_violation"


def _admission_constraints(trial: Any) -> tuple[float]:
    value = trial.user_attrs.get(ADMISSION_CONSTRAINT_USER_ATTR)
    if value is None:
        raise RuntimeError("OPTUNA_ADMISSION_CONSTRAINT_MISSING")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0.0:
        raise RuntimeError("OPTUNA_ADMISSION_CONSTRAINT_INVALID")
    return (parsed,)


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _load_optuna() -> tuple[Any, str, str]:
    try:
        import optuna
    except ImportError as exc:  # pragma: no cover - launch-host guard.
        raise RuntimeError("OFFICIAL_OPTUNA_PACKAGE_MISSING") from exc
    version = str(getattr(optuna, "__version__", ""))
    if version != EXPECTED_OPTUNA_VERSION:
        raise RuntimeError(
            "OPTUNA_VERSION_MISMATCH:"
            f"expected={EXPECTED_OPTUNA_VERSION}:actual={version}"
        )
    return optuna, version, str(getattr(optuna, "__file__", ""))


@dataclass(frozen=True, slots=True)
class ConditionalLane:
    skeleton_id: str
    ordered_categories_by_slot: Mapping[str, tuple[str, ...]]
    allowed_field_pairs: frozenset[str]

    @classmethod
    def freeze(
        cls,
        skeleton_id: str,
        space: Mapping[str, Any],
    ) -> "ConditionalLane":
        categories = {
            str(slot): tuple(map(str, values))
            for slot, values in dict(
                space["ordered_categories_by_slot"]
            ).items()
        }
        if categories.get("skeleton_id") != (str(skeleton_id),):
            raise ValueError(
                f"route-local lane skeleton drift: {skeleton_id}"
            )
        if any(not values for values in categories.values()):
            raise ValueError(f"empty categorical lane: {skeleton_id}")
        pair_values: list[str] = []
        for slot, values in categories.items():
            if not slot.endswith("field_pair_id"):
                continue
            for value in values:
                if value.count("::") != 1:
                    raise ValueError(
                        f"invalid typed field pair token: {skeleton_id}:{value}"
                    )
                pair_values.append(value)
        return cls(
            skeleton_id=str(skeleton_id),
            ordered_categories_by_slot=categories,
            allowed_field_pairs=frozenset(pair_values),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "skeleton_id": self.skeleton_id,
            "ordered_categories_by_slot": {
                slot: list(values)
                for slot, values in self.ordered_categories_by_slot.items()
            },
            "allowed_field_pair_count": len(self.allowed_field_pairs),
        }


class RouteConditionalTPESearchAdapter:
    """Official Optuna TPE over one registry route's conditional Grammar.

    A field-pair category is factorized into its typed left/right field IDs so
    the optimizer can learn field preferences instead of treating thousands of
    complete pairs as unrelated opaque categories.  The authoritative lane's
    exact allowed-pair set remains the final compatibility mask.
    """

    policy_id = "official_optuna_tpe_conditional_typed_grammar_v1"

    def __getstate__(self) -> dict[str, Any]:
        state = dict(self.__dict__)
        state["_optuna"] = None
        return state

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        self.__dict__.update(state)
        optuna, version, package_path = _load_optuna()
        if version != self.package_version:
            raise RuntimeError(
                "OPTUNA_PICKLE_VERSION_MISMATCH:"
                f"expected={self.package_version}:actual={version}"
            )
        self._optuna = optuna
        self.package_path = package_path

    def __init__(
        self,
        *,
        route_id: str,
        lane_spaces: Mapping[str, Mapping[str, Any]],
        seed: int,
        n_startup_trials: int = 512,
        n_ei_candidates: int = 64,
        multivariate: bool = True,
        group: bool = True,
        constant_liar: bool = True,
        constraints_enabled: bool = False,
        categorical_distance_func: Mapping[
            str, Callable[[Any, Any], float]
        ] | None = None,
    ) -> None:
        if not lane_spaces:
            raise ValueError(f"route has no optimizer lanes: {route_id}")
        if n_startup_trials < 1:
            raise ValueError("n_startup_trials must be positive")
        if n_ei_candidates < 1:
            raise ValueError("n_ei_candidates must be positive")
        if group and not multivariate:
            raise ValueError("group requires multivariate TPE")
        optuna, version, package_path = _load_optuna()
        self._optuna = optuna
        self.route_id = str(route_id)
        self.seed = int(seed)
        self.n_startup_trials = int(n_startup_trials)
        self.n_ei_candidates = int(n_ei_candidates)
        self.multivariate = bool(multivariate)
        self.group = bool(group)
        self.constant_liar = bool(constant_liar)
        self.constraints_enabled = bool(constraints_enabled)
        self.categorical_distance_func = dict(
            categorical_distance_func or {}
        )
        self.policy_id = (
            "official_optuna_tpe_conditional_typed_grammar_v1"
            if self.multivariate and self.group
            else "official_optuna_tpe_conditional_typed_grammar_v2_univariate"
        )
        self.package_version = version
        self.package_path = package_path
        self.restore_mode = "GENESIS_EMPTY_STUDY"
        self.lanes = {
            str(skeleton_id): ConditionalLane.freeze(skeleton_id, space)
            for skeleton_id, space in sorted(lane_spaces.items())
        }
        self.lane_hash = _stable_hash(
            [lane.to_dict() for lane in self.lanes.values()]
        )
        sampler = optuna.samplers.TPESampler(
            seed=self.seed,
            n_startup_trials=self.n_startup_trials,
            n_ei_candidates=self.n_ei_candidates,
            multivariate=self.multivariate,
            group=self.group,
            warn_independent_sampling=False,
            constant_liar=self.constant_liar,
            constraints_func=(
                _admission_constraints if self.constraints_enabled else None
            ),
            categorical_distance_func=(
                self.categorical_distance_func or None
            ),
        )
        self._study = optuna.create_study(
            direction="maximize",
            sampler=sampler,
        )
        self._pending: dict[str, dict[str, Any]] = {}
        self._pending_order: list[str] = []
        self._history: list[dict[str, Any]] = []

    @property
    def has_pending_population(self) -> bool:
        return bool(self._pending)

    @property
    def history(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self._history)

    def environment_receipt(self) -> dict[str, Any]:
        return {
            "optimizer": "official_optuna.samplers.TPESampler",
            "policy_id": self.policy_id,
            "package_version": self.package_version,
            "package_path": self.package_path,
            "route_id": self.route_id,
            "seed": self.seed,
            "n_startup_trials": self.n_startup_trials,
            "n_ei_candidates": self.n_ei_candidates,
            "multivariate": self.multivariate,
            "group": self.group,
            "constant_liar": self.constant_liar,
            "constraints_enabled": self.constraints_enabled,
            "categorical_distance_parameter_names": sorted(
                self.categorical_distance_func
            ),
            "constraint_semantics": (
                "ABSOLUTE_ADMISSION_FEASIBILITY_SEPARATE_FROM_CONDITIONAL_OBJECTIVE"
                if self.constraints_enabled
                else None
            ),
            "persistent_database": False,
            "restore_authority": (
                "HASH_BOUND_OPTUNA_STATE_SNAPSHOT_PLUS_IMMUTABLE_TRANSCRIPTS"
            ),
            "restore_mode": self.restore_mode,
            "lane_hash": self.lane_hash,
            "lane_count": len(self.lanes),
        }

    @staticmethod
    def _parameter_name(skeleton_id: str, slot: str) -> str:
        return f"{skeleton_id}|{slot}"

    @staticmethod
    def _pair_parameter_slot(slot: str, suffix: str) -> str:
        if slot == "field_pair_id":
            return suffix
        return f"{slot}|{suffix}"

    def _sample_genes(self, trial: Any) -> tuple[dict[str, str], bool]:
        skeleton_id = str(
            trial.suggest_categorical(
                "skeleton_id",
                tuple(self.lanes),
            )
        )
        lane = self.lanes[skeleton_id]
        genes: dict[str, str] = {}
        pair_compatible = True
        for slot, values in lane.ordered_categories_by_slot.items():
            if slot in {"skeleton_id", "gene_surface_id"}:
                genes[slot] = str(values[0])
                continue
            if slot.endswith("field_pair_id"):
                right_values_by_left: dict[str, list[str]] = {}
                for value in values:
                    left_value, right_value = value.split("::", 1)
                    right_values_by_left.setdefault(
                        left_value, []
                    ).append(right_value)
                left_values = tuple(right_values_by_left)
                left = str(
                    trial.suggest_categorical(
                        self._parameter_name(
                            skeleton_id,
                            self._pair_parameter_slot(slot, "left_field_id"),
                        ),
                        left_values,
                    )
                )
                right = str(
                    trial.suggest_categorical(
                        self._parameter_name(
                            skeleton_id,
                            self._pair_parameter_slot(
                                slot, f"right_field_id|left={left}"
                            ),
                        ),
                        tuple(
                            dict.fromkeys(
                                right_values_by_left[left]
                            )
                        ),
                    )
                )
                pair_id = f"{left}::{right}"
                genes[slot] = pair_id
                pair_compatible = (
                    pair_compatible and pair_id in lane.allowed_field_pairs
                )
                continue
            genes[slot] = str(
                trial.suggest_categorical(
                    self._parameter_name(skeleton_id, slot),
                    values,
                )
            )
        return genes, pair_compatible

    def _register_asked_trial(
        self,
        *,
        trial: Any,
        checkpoint_id: str,
        ask_ordinal: int,
        genes: Mapping[str, str],
        pair_compatible: bool,
        ask_kind: str | None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._pending_order:
            pending_checkpoint = str(
                self._pending[self._pending_order[0]]["row"][
                    "checkpoint_id"
                ]
            )
            if pending_checkpoint != str(checkpoint_id):
                raise RuntimeError("OPTUNA_PENDING_CHECKPOINT_DRIFT")
        normalized_genes = {
            str(key): str(value) for key, value in genes.items()
        }
        proposal_id = _stable_hash(
            {
                "policy_id": self.policy_id,
                "route_id": self.route_id,
                "seed": self.seed,
                "trial_number": trial.number,
                "checkpoint_id": str(checkpoint_id),
                "ask_ordinal": int(ask_ordinal),
                "genes": normalized_genes,
            }
        )[:24]
        row = {
            "proposal_id": proposal_id,
            "trial_number": int(trial.number),
            "checkpoint_id": str(checkpoint_id),
            "ask_ordinal": int(ask_ordinal),
            "route_id": self.route_id,
            "generation": len(self._history),
            "genes": normalized_genes,
            "category_id": _stable_hash(
                {
                    "route_id": self.route_id,
                    "genes": normalized_genes,
                }
            ),
            "typed_pair_compatible": bool(pair_compatible),
            "optimizer_policy_id": self.policy_id,
        }
        if ask_kind:
            row["optimizer_ask_kind"] = str(ask_kind)
        row["source_sampling_phase"] = source_sampling_phase_v0(
            optimizer_ask_kind=str(ask_kind or ""),
            trial_number=int(trial.number),
            n_startup_trials=self.n_startup_trials,
        )
        if metadata:
            overlap = set(row) & set(metadata)
            if overlap:
                raise ValueError(
                    "OPTUNA_ASK_METADATA_RESERVED_KEYS:"
                    + ",".join(sorted(overlap))
                )
            row.update(copy.deepcopy(dict(metadata)))
        self._pending[proposal_id] = {
            "trial": trial,
            "row": copy.deepcopy(row),
        }
        self._pending_order.append(proposal_id)
        return row

    def ask_trial(
        self,
        *,
        checkpoint_id: str,
        ask_ordinal: int | None = None,
        expected_genes: Mapping[str, str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        record_ask_kind: bool = True,
        fixed_skeleton_id: str | None = None,
    ) -> dict[str, Any]:
        """Ask one native TPE trial while retaining batch ask/tell coverage."""

        ordinal = (
            len(self._pending_order)
            if ask_ordinal is None
            else int(ask_ordinal)
        )
        if fixed_skeleton_id is not None:
            skeleton_id = str(fixed_skeleton_id)
            if skeleton_id not in self.lanes:
                raise ValueError("OPTUNA_FIXED_SKELETON_OUTSIDE_LANES")
            self._study.enqueue_trial(
                {"skeleton_id": skeleton_id},
                user_attrs={"conditional_lane_fixed": True},
            )
        trial = self._study.ask()
        genes, pair_compatible = self._sample_genes(trial)
        if expected_genes is not None:
            expected = {
                str(key): str(value)
                for key, value in expected_genes.items()
            }
            if genes != expected:
                raise RuntimeError(
                    "OPTUNA_TRANSCRIPT_REPLAY_DRIFT:"
                    f"{self.route_id}:{checkpoint_id}:{ordinal}"
                )
        return self._register_asked_trial(
            trial=trial,
            checkpoint_id=checkpoint_id,
            ask_ordinal=ordinal,
            genes=genes,
            pair_compatible=pair_compatible,
            ask_kind=(
                "TPE_NATIVE_CONDITIONAL_LANE_DRAW"
                if record_ask_kind and fixed_skeleton_id is not None
                else "TPE_NATIVE_DRAW"
                if record_ask_kind
                else None
            ),
            metadata=metadata,
        )

    def enqueue_fixed_trial(
        self,
        *,
        checkpoint_id: str,
        genes: Mapping[str, Any],
        ask_ordinal: int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a fixed-parameter official Optuna trial for an exact emitter.

        The fixed row still consumes a real ``study.ask()`` trial number and is
        included in ordinary ask/tell coverage.  It receives no synthetic
        reward and is not a second optimizer.
        """

        normalized = {
            str(key): str(value) for key, value in genes.items()
        }
        params, _ = self._frozen_trial_params(normalized)
        self._study.enqueue_trial(
            params,
            user_attrs={
                "availability_fixed_trial": True,
                "availability_fixed_genes_hash": _stable_hash(normalized),
            },
        )
        trial = self._study.ask()
        actual, pair_compatible = self._sample_genes(trial)
        if actual != normalized:
            raise RuntimeError("OPTUNA_FIXED_TRIAL_GENE_DRIFT")
        ordinal = (
            len(self._pending_order)
            if ask_ordinal is None
            else int(ask_ordinal)
        )
        return self._register_asked_trial(
            trial=trial,
            checkpoint_id=checkpoint_id,
            ask_ordinal=ordinal,
            genes=actual,
            pair_compatible=pair_compatible,
            ask_kind="AVAILABILITY_FIXED_ENQUEUED",
            metadata=metadata,
        )

    def annotate_pending_trial(
        self,
        proposal_id: str,
        metadata: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Append immutable ask metadata after exact materialization.

        The trial and sampled parameters are unchanged.  This exists only
        because exact identity and availability mode are known after the
        Grammar/compiler path materializes the sampled genes.
        """

        identity = str(proposal_id)
        pending = self._pending.get(identity)
        if pending is None:
            raise RuntimeError("OPTUNA_PENDING_PROPOSAL_MISSING")
        row = pending["row"]
        overlap = set(row) & set(metadata)
        if overlap:
            raise ValueError(
                "OPTUNA_ASK_METADATA_RESERVED_KEYS:"
                + ",".join(sorted(overlap))
            )
        row.update(copy.deepcopy(dict(metadata)))
        return copy.deepcopy(row)

    def ask_population(
        self,
        *,
        checkpoint_id: str,
        count: int,
        expected_genes: Sequence[Mapping[str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        if self.has_pending_population:
            raise RuntimeError("OPTUNA_PENDING_POPULATION_NOT_TOLD")
        if count < 1:
            raise ValueError("ask count must be positive")
        if expected_genes is not None and len(expected_genes) != count:
            raise ValueError("expected gene count drift")
        asked: list[dict[str, Any]] = []
        for ordinal in range(int(count)):
            asked.append(
                self.ask_trial(
                    checkpoint_id=checkpoint_id,
                    ask_ordinal=ordinal,
                    expected_genes=(
                        expected_genes[ordinal]
                        if expected_genes is not None
                        else None
                    ),
                    record_ask_kind=False,
                )
            )
        return asked

    def tell_population(
        self,
        observations: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        if not self.has_pending_population:
            raise RuntimeError("OPTUNA_TELL_WITHOUT_PENDING_POPULATION")
        by_id = {
            str(row["proposal_id"]): dict(row) for row in observations
        }
        if set(by_id) != set(self._pending):
            raise RuntimeError("OPTUNA_ASK_TELL_OBSERVATION_COVERAGE_MISMATCH")
        told: list[dict[str, Any]] = []
        completed = 0
        pruned = 0
        failed = 0
        for proposal_id in self._pending_order:
            pending = self._pending[proposal_id]
            trial = pending["trial"]
            source = by_id[proposal_id]
            outcome_class = str(source.get("outcome_class") or "")
            reward = source.get("optimizer_reward")
            if outcome_class == EVALUATED:
                objective_domain_eligible = bool(
                    source.get("objective_domain_eligible", True)
                )
                constraint_violation = source.get(
                    "admission_constraint_violation"
                )
                if self.constraints_enabled:
                    if (
                        constraint_violation is None
                        or not math.isfinite(float(constraint_violation))
                        or float(constraint_violation) < 0.0
                    ):
                        raise RuntimeError(
                            "OPTUNA_COMPLETE_REQUIRES_ADMISSION_CONSTRAINT:"
                            f"{proposal_id}"
                        )
                    constraint_violation = float(constraint_violation)
                    trial.set_user_attr(
                        ADMISSION_CONSTRAINT_USER_ATTR,
                        constraint_violation,
                    )
                    if objective_domain_eligible:
                        if reward is None or not math.isfinite(float(reward)):
                            raise RuntimeError(
                                "OPTUNA_FEASIBLE_COMPLETE_REQUIRES_FINITE_OBJECTIVE:"
                                f"{proposal_id}"
                            )
                        study_value = float(reward)
                    else:
                        if reward is not None or constraint_violation <= 0.0:
                            raise RuntimeError(
                                "OPTUNA_INFEASIBLE_COMPLETE_DOMAIN_DRIFT:"
                                f"{proposal_id}"
                            )
                        # Optuna requires a finite value for COMPLETE trials.
                        # Constrained TPE excludes this neutral placeholder from
                        # the feasible objective density via constraints_func.
                        study_value = 0.0
                    value = float(reward) if reward is not None else None
                else:
                    if reward is None or not math.isfinite(float(reward)):
                        raise RuntimeError(
                            "OPTUNA_COMPLETE_REQUIRES_FINITE_REAL_REWARD:"
                            f"{proposal_id}"
                        )
                    value = float(reward)
                    study_value = value
                    objective_domain_eligible = True
                    constraint_violation = None
                self._study.tell(trial, study_value)
                state = "COMPLETE"
                completed += 1
                intermediate_value = None
                intermediate_step = None
                pruning_authority = ""
            elif outcome_class == PRUNED:
                if reward is not None:
                    raise RuntimeError(
                        "OPTUNA_PRUNED_FORBIDS_OPTIMIZER_REWARD:"
                        f"{proposal_id}"
                    )
                intermediate_value = source.get(
                    "optimizer_intermediate_value"
                )
                intermediate_step = source.get(
                    "optimizer_intermediate_step"
                )
                pruning_authority = str(
                    source.get("pruning_authority") or ""
                )
                if (
                    intermediate_value is None
                    or not math.isfinite(float(intermediate_value))
                    or isinstance(intermediate_step, bool)
                    or not isinstance(intermediate_step, int)
                    or int(intermediate_step) < 0
                    or not pruning_authority
                ):
                    raise RuntimeError(
                        "OPTUNA_PRUNED_REQUIRES_REAL_INTERMEDIATE_RECEIPT:"
                        f"{proposal_id}"
                    )
                intermediate_value = float(intermediate_value)
                intermediate_step = int(intermediate_step)
                trial.report(intermediate_value, intermediate_step)
                self._study.tell(
                    trial,
                    state=self._optuna.trial.TrialState.PRUNED,
                )
                value = None
                state = "PRUNED"
                pruned += 1
            else:
                value = None
                intermediate_value = None
                intermediate_step = None
                pruning_authority = ""
                self._study.tell(
                    trial,
                    state=self._optuna.trial.TrialState.FAIL,
                )
                state = "FAIL"
                failed += 1
            told.append(
                {
                    "proposal_id": proposal_id,
                    "trial_number": int(trial.number),
                    "state": state,
                    "optimizer_reward": value,
                    "outcome_class": outcome_class,
                    "outcome_reason": str(
                        source.get("outcome_reason") or ""
                    ),
                    "optimizer_intermediate_value": intermediate_value,
                    "optimizer_intermediate_step": intermediate_step,
                    "pruning_authority": pruning_authority,
                    **(
                        {
                            "study_value": study_value,
                            "objective_domain_eligible": (
                                objective_domain_eligible
                            ),
                            "admission_constraint_violation": (
                                constraint_violation
                            ),
                            "infeasible_objective_placeholder": (
                                not objective_domain_eligible
                            ),
                        }
                        if self.constraints_enabled and state == "COMPLETE"
                        else {}
                    ),
                }
            )
        transcript = {
            "checkpoint_id": str(
                self._pending[
                    self._pending_order[0]
                ]["row"]["checkpoint_id"]
            ),
            "asked": [
                copy.deepcopy(self._pending[proposal_id]["row"])
                for proposal_id in self._pending_order
            ],
            "observations": told,
        }
        receipt = {
            "schema_version": "cn_optuna_tpe_tell_receipt_v1",
            "route_id": self.route_id,
            "checkpoint_id": transcript["checkpoint_id"],
            "asked_count": len(told),
            "completed_count": completed,
            "pruned_count": pruned,
            "failed_count": failed,
            "study_trial_count": len(self._study.trials),
            "complete_study_trial_count": sum(
                trial.state
                == self._optuna.trial.TrialState.COMPLETE
                for trial in self._study.trials
            ),
            "pruned_study_trial_count": sum(
                trial.state
                == self._optuna.trial.TrialState.PRUNED
                for trial in self._study.trials
            ),
            "transcript_hash": _stable_hash(transcript),
            "constraints_enabled": self.constraints_enabled,
        }
        transcript["receipt"] = receipt
        self._history.append(transcript)
        self._pending.clear()
        self._pending_order.clear()
        return copy.deepcopy(receipt)

    def history_receipt(self) -> dict[str, Any]:
        return {
            "schema_version": "cn_optuna_tpe_genesis_replay_v1",
            "environment": self.environment_receipt(),
            "checkpoint_count": len(self._history),
            "transcripts": copy.deepcopy(self._history),
            "history_hash": _stable_hash(self._history),
        }

    def _frozen_trial_params(
        self,
        genes: Mapping[str, Any],
    ) -> tuple[dict[str, str], dict[str, Any]]:
        skeleton_id = str(genes["skeleton_id"])
        lane = self.lanes[skeleton_id]
        categorical = self._optuna.distributions.CategoricalDistribution
        params: dict[str, str] = {"skeleton_id": skeleton_id}
        distributions: dict[str, Any] = {
            "skeleton_id": categorical(tuple(self.lanes))
        }
        for slot, values in lane.ordered_categories_by_slot.items():
            if slot in {"skeleton_id", "gene_surface_id"}:
                continue
            if slot.endswith("field_pair_id"):
                left_values: list[str] = []
                right_values_by_left: dict[str, list[str]] = {}
                for value in values:
                    left, right = value.split("::", 1)
                    if left not in right_values_by_left:
                        left_values.append(left)
                        right_values_by_left[left] = []
                    if right not in right_values_by_left[left]:
                        right_values_by_left[left].append(right)
                chosen_left, chosen_right = str(genes[slot]).split(
                    "::", 1
                )
                left_name = self._parameter_name(
                    skeleton_id,
                    self._pair_parameter_slot(slot, "left_field_id"),
                )
                right_name = self._parameter_name(
                    skeleton_id,
                    self._pair_parameter_slot(
                        slot, f"right_field_id|left={chosen_left}"
                    ),
                )
                params[left_name] = chosen_left
                params[right_name] = chosen_right
                distributions[left_name] = categorical(tuple(left_values))
                distributions[right_name] = categorical(
                    tuple(right_values_by_left[chosen_left])
                )
                continue
            name = self._parameter_name(skeleton_id, slot)
            params[name] = str(genes[slot])
            distributions[name] = categorical(tuple(values))
        return params, distributions

    @classmethod
    def restore_trials(
        cls,
        *,
        route_id: str,
        lane_spaces: Mapping[str, Mapping[str, Any]],
        seed: int,
        transcripts: Sequence[Mapping[str, Any]],
        n_startup_trials: int = 512,
        n_ei_candidates: int = 24,
        multivariate: bool = True,
        group: bool = True,
        constant_liar: bool = True,
        constraints_enabled: bool = False,
        categorical_distance_func: Mapping[
            str, Callable[[Any, Any], float]
        ] | None = None,
    ) -> "RouteConditionalTPESearchAdapter":
        adapter = cls(
            route_id=route_id,
            lane_spaces=lane_spaces,
            seed=seed,
            n_startup_trials=n_startup_trials,
            n_ei_candidates=n_ei_candidates,
            multivariate=multivariate,
            group=group,
            constant_liar=constant_liar,
            constraints_enabled=constraints_enabled,
            categorical_distance_func=categorical_distance_func,
        )
        complete = adapter._optuna.trial.TrialState.COMPLETE
        pruned = adapter._optuna.trial.TrialState.PRUNED
        failed = adapter._optuna.trial.TrialState.FAIL
        expected_number = 0
        for transcript in transcripts:
            asked = list(transcript.get("asked") or ())
            observations = list(transcript.get("observations") or ())
            observation_by_id = {
                str(row["proposal_id"]): row for row in observations
            }
            if len(observation_by_id) != len(asked):
                raise RuntimeError(
                    "OPTUNA_TRIAL_IMPORT_OBSERVATION_COVERAGE_DRIFT"
                )
            for row in asked:
                if int(row["trial_number"]) != expected_number:
                    raise RuntimeError(
                        "OPTUNA_TRIAL_IMPORT_NUMBER_DRIFT:"
                        f"{route_id}:{expected_number}"
                    )
                proposal_id = str(row["proposal_id"])
                observation = observation_by_id.get(proposal_id)
                if observation is None:
                    raise RuntimeError(
                        "OPTUNA_TRIAL_IMPORT_PROPOSAL_COVERAGE_DRIFT"
                    )
                params, distributions = adapter._frozen_trial_params(
                    dict(row["genes"])
                )
                observation_state = str(
                    observation.get("state") or ""
                )
                if observation_state == "COMPLETE":
                    study_value = observation.get(
                        "study_value", observation.get("optimizer_reward")
                    )
                    if study_value is None:
                        raise RuntimeError(
                            "OPTUNA_TRIAL_IMPORT_COMPLETE_REWARD_MISSING"
                        )
                    system_attrs = None
                    user_attrs = None
                    if constraints_enabled:
                        violation = observation.get(
                            "admission_constraint_violation"
                        )
                        if violation is None:
                            raise RuntimeError(
                                "OPTUNA_TRIAL_IMPORT_CONSTRAINT_MISSING"
                            )
                        parsed_violation = float(violation)
                        system_attrs = {
                            "constraints": (parsed_violation,)
                        }
                        user_attrs = {
                            ADMISSION_CONSTRAINT_USER_ATTR: parsed_violation
                        }
                    trial = adapter._optuna.trial.create_trial(
                        params=params,
                        distributions=distributions,
                        value=float(study_value),
                        state=complete,
                        system_attrs=system_attrs,
                        user_attrs=user_attrs,
                    )
                elif observation_state == "PRUNED":
                    intermediate_value = observation.get(
                        "optimizer_intermediate_value"
                    )
                    intermediate_step = observation.get(
                        "optimizer_intermediate_step"
                    )
                    pruning_authority = str(
                        observation.get("pruning_authority") or ""
                    )
                    if (
                        intermediate_value is None
                        or not math.isfinite(float(intermediate_value))
                        or isinstance(intermediate_step, bool)
                        or not isinstance(intermediate_step, int)
                        or int(intermediate_step) < 0
                        or not pruning_authority
                    ):
                        raise RuntimeError(
                            "OPTUNA_TRIAL_IMPORT_PRUNED_RECEIPT_MISSING"
                        )
                    trial = adapter._optuna.trial.create_trial(
                        params=params,
                        distributions=distributions,
                        state=pruned,
                        intermediate_values={
                            int(intermediate_step): float(
                                intermediate_value
                            )
                        },
                    )
                else:
                    trial = adapter._optuna.trial.create_trial(
                        params=params,
                        distributions=distributions,
                        state=failed,
                    )
                adapter._study.add_trial(trial)
                expected_number += 1
        adapter._history = copy.deepcopy(list(transcripts))
        adapter.restore_mode = (
            "IMMUTABLE_TRIAL_IMPORT_FRESH_DETERMINISTIC_SAMPLER_RNG"
        )
        return adapter

    @classmethod
    def replay(
        cls,
        *,
        route_id: str,
        lane_spaces: Mapping[str, Mapping[str, Any]],
        seed: int,
        transcripts: Sequence[Mapping[str, Any]],
        n_startup_trials: int = 512,
        n_ei_candidates: int = 64,
        multivariate: bool = True,
        group: bool = True,
        constant_liar: bool = True,
        constraints_enabled: bool = False,
        categorical_distance_func: Mapping[
            str, Callable[[Any, Any], float]
        ] | None = None,
    ) -> "RouteConditionalTPESearchAdapter":
        adapter = cls(
            route_id=route_id,
            lane_spaces=lane_spaces,
            seed=seed,
            n_startup_trials=n_startup_trials,
            n_ei_candidates=n_ei_candidates,
            multivariate=multivariate,
            group=group,
            constant_liar=constant_liar,
            constraints_enabled=constraints_enabled,
            categorical_distance_func=categorical_distance_func,
        )
        for transcript in transcripts:
            asked = list(transcript.get("asked") or ())
            observations = list(transcript.get("observations") or ())
            if adapter.has_pending_population:
                raise RuntimeError("OPTUNA_REPLAY_PENDING_POPULATION_DRIFT")
            actual = []
            core_keys = {
                "proposal_id",
                "trial_number",
                "checkpoint_id",
                "ask_ordinal",
                "route_id",
                "generation",
                "genes",
                "category_id",
                "typed_pair_compatible",
                "optimizer_policy_id",
                "optimizer_ask_kind",
                "source_sampling_phase",
            }
            for ordinal, source in enumerate(asked):
                ask_kind = str(
                    source.get("optimizer_ask_kind") or ""
                )
                metadata = {
                    key: copy.deepcopy(value)
                    for key, value in source.items()
                    if key not in core_keys
                }
                if ask_kind == "AVAILABILITY_FIXED_ENQUEUED":
                    actual.append(
                        adapter.enqueue_fixed_trial(
                            checkpoint_id=str(
                                transcript["checkpoint_id"]
                            ),
                            genes=dict(source["genes"]),
                            ask_ordinal=ordinal,
                            metadata=metadata,
                        )
                    )
                else:
                    fixed_skeleton_id = (
                        str(dict(source["genes"])["skeleton_id"])
                        if ask_kind
                        == "TPE_NATIVE_CONDITIONAL_LANE_DRAW"
                        else None
                    )
                    actual.append(
                        adapter.ask_trial(
                            checkpoint_id=str(
                                transcript["checkpoint_id"]
                            ),
                            ask_ordinal=ordinal,
                            expected_genes=dict(source["genes"]),
                            metadata=metadata,
                            record_ask_kind=bool(ask_kind),
                            fixed_skeleton_id=fixed_skeleton_id,
                        )
                    )
            expected_ids = [
                str(row["proposal_id"]) for row in asked
            ]
            if [row["proposal_id"] for row in actual] != expected_ids:
                raise RuntimeError("OPTUNA_PROPOSAL_ID_REPLAY_DRIFT")
            replay_observations = []
            for row in observations:
                replay_observations.append(
                    {
                        "proposal_id": str(row["proposal_id"]),
                        "outcome_class": (
                            EVALUATED
                            if str(row.get("state") or "") == "COMPLETE"
                            else (
                                PRUNED
                                if str(row.get("state") or "") == "PRUNED"
                                else str(
                                    row.get("outcome_class") or "FAILED"
                                )
                            )
                        ),
                        "optimizer_reward": row.get("optimizer_reward"),
                        "outcome_reason": str(
                            row.get("outcome_reason") or ""
                        ),
                        "optimizer_intermediate_value": row.get(
                            "optimizer_intermediate_value"
                        ),
                        "optimizer_intermediate_step": row.get(
                            "optimizer_intermediate_step"
                        ),
                        "pruning_authority": str(
                            row.get("pruning_authority") or ""
                        ),
                        **(
                            {
                                "objective_domain_eligible": bool(
                                    row.get("objective_domain_eligible")
                                ),
                                "admission_constraint_violation": row.get(
                                    "admission_constraint_violation"
                                ),
                            }
                            if constraints_enabled
                            and str(row.get("state") or "") == "COMPLETE"
                            else {}
                        ),
                    }
                )
            receipt = adapter.tell_population(replay_observations)
            expected_hash = str(
                (transcript.get("receipt") or {}).get(
                    "transcript_hash"
                )
                or ""
            )
            if expected_hash and receipt["transcript_hash"] != expected_hash:
                raise RuntimeError("OPTUNA_TELL_TRANSCRIPT_REPLAY_DRIFT")
        adapter.restore_mode = "EXACT_TRANSCRIPT_REPLAY"
        return adapter
