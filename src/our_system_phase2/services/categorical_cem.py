"""Support-gated categorical CEM over frozen decision token domains."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from our_system_phase2.services.search_choice_policy import DecisionSpec


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class CEMParameters:
    elite_fraction: float = 0.20
    minimum_elites: int = 6
    alpha: float = 0.25
    uniform_mix: float = 0.05
    maximum_category_probability: float = 0.80
    minimum_active_observations_per_context: int = 8
    minimum_elite_support_per_context: int = 4

    def __post_init__(self) -> None:
        if not 0.0 < self.elite_fraction <= 1.0:
            raise ValueError("elite_fraction must be in (0, 1]")
        if self.minimum_elites <= 0:
            raise ValueError("minimum_elites must be positive")
        if not 0.0 < self.alpha <= 1.0:
            raise ValueError("alpha must be in (0, 1]")
        if not 0.0 <= self.uniform_mix < 1.0:
            raise ValueError("uniform_mix must be in [0, 1)")
        if not 0.0 < self.maximum_category_probability <= 1.0:
            raise ValueError(
                "maximum_category_probability must be in (0, 1]"
            )


def _cap_simplex(values: np.ndarray, cap: float) -> np.ndarray:
    output = np.asarray(values, dtype=float).copy()
    output /= float(output.sum())
    if len(output) * cap < 1.0 - 1e-12:
        raise ValueError("category probability cap is infeasible")
    for _ in range(len(output) + 2):
        above = output > cap
        if not bool(np.any(above)):
            break
        excess = float(np.sum(output[above] - cap))
        output[above] = cap
        below = ~above
        if not bool(np.any(below)):
            break
        weights = output[below]
        if float(weights.sum()) <= 0.0:
            output[below] += excess / int(np.sum(below))
        else:
            output[below] += excess * weights / float(weights.sum())
    output /= float(output.sum())
    return output


class CategoricalCEMPolicy:
    """One family-level CEM; formula construction stays in the Grammar."""

    policy_id = "categorical_cem_v1"

    def __init__(
        self,
        *,
        decisions: Sequence[DecisionSpec],
        decision_catalog_hash: str,
        formula_space_id: str,
        probability_tables: Mapping[str, Sequence[float]],
        parameters: CEMParameters,
        generation: int = 0,
        reward_observation_count: int = 0,
        last_support_diagnostics: Sequence[Mapping[str, Any]] = (),
    ) -> None:
        self.decisions = tuple(decisions)
        self._by_context = {
            row.context_id: row for row in self.decisions
        }
        if len(self._by_context) != len(self.decisions):
            raise ValueError("decision context IDs must be unique")
        self.decision_catalog_hash = str(decision_catalog_hash)
        self.formula_space_id = str(formula_space_id)
        self.parameters = parameters
        self.generation = int(generation)
        self.reward_observation_count = int(reward_observation_count)
        self._probability_tables: dict[str, np.ndarray] = {}
        for decision in self.decisions:
            values = np.asarray(
                probability_tables[decision.context_id],
                dtype=float,
            )
            if len(values) != len(decision.ordered_choices):
                raise ValueError(
                    f"probability domain drift: {decision.context_id}"
                )
            if (
                not bool(np.all(np.isfinite(values)))
                or bool(np.any(values < 0.0))
                or float(values.sum()) <= 0.0
            ):
                raise ValueError(
                    f"invalid probability table: {decision.context_id}"
                )
            self._probability_tables[decision.context_id] = (
                values / float(values.sum())
            )
        self.last_support_diagnostics = [
            copy.deepcopy(dict(row))
            for row in last_support_diagnostics
        ]

    @classmethod
    def fresh(
        cls,
        *,
        decisions: Sequence[DecisionSpec],
        decision_catalog_hash: str,
        formula_space_id: str,
        parameters: CEMParameters | None = None,
    ) -> "CategoricalCEMPolicy":
        rows = tuple(decisions)
        return cls(
            decisions=rows,
            decision_catalog_hash=decision_catalog_hash,
            formula_space_id=formula_space_id,
            probability_tables={
                row.context_id: [
                    1.0 / len(row.ordered_choices)
                ] * len(row.ordered_choices)
                for row in rows
            },
            parameters=parameters or CEMParameters(),
        )

    @property
    def probability_tables(self) -> dict[str, list[float]]:
        return {
            context: [float(value) for value in values]
            for context, values in self._probability_tables.items()
        }

    def choose(
        self,
        decision: DecisionSpec,
        *,
        rng: np.random.Generator,
    ) -> str:
        expected = self._by_context.get(decision.context_id)
        if expected is None or expected.to_dict() != decision.to_dict():
            raise RuntimeError(
                f"CEM_DECISION_CONTEXT_DRIFT:{decision.context_id}"
            )
        probabilities = self._probability_tables[decision.context_id]
        index = int(
            rng.choice(len(decision.ordered_choices), p=probabilities)
        )
        return decision.ordered_choices[index].token_id

    @staticmethod
    def _rank_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        outcome = str(row.get("outcome_class") or "")
        identity = str(row.get("exact_identity") or "")
        if outcome == "EVALUATED":
            increment = float(row["signed_matched_increment"])
            if not math.isfinite(increment):
                raise ValueError(
                    "evaluated observation increment must be finite"
                )
            return (0, -increment, identity)
        if outcome in {
            "SUPPORT_BLOCKED",
            "CONTROL_BLOCKED",
            "BEHAVIOR_BLOCKED",
            "SUPPORT_OR_CONTROL_BLOCKED",
        }:
            return (1, 0.0, identity)
        if outcome in {
            "DETERMINISTIC_INVALID",
            "PIT_OR_SEMANTIC_BLOCKED",
        }:
            return (2, 0.0, identity)
        raise ValueError(f"unknown CEM outcome: {outcome}")

    def tell(
        self,
        observations: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        rows = [copy.deepcopy(dict(row)) for row in observations]
        if any(
            str(row.get("outcome_class") or "")
            == "INFRASTRUCTURE_FAILURE"
            for row in rows
        ):
            raise RuntimeError(
                "INFRASTRUCTURE_FAILURE_IS_RUN_HEALTH_NOT_CEM_OBSERVATION"
            )
        if not rows:
            raise ValueError("CEM tell requires observations")
        ranked = sorted(rows, key=self._rank_key)
        elite_count = min(
            len(ranked),
            max(
                self.parameters.minimum_elites,
                int(math.ceil(
                    len(ranked) * self.parameters.elite_fraction
                )),
            ),
        )
        elites = ranked[:elite_count]
        diagnostics: list[dict[str, Any]] = []
        updated_context_count = 0
        before_hash = _stable_hash(self.probability_tables)

        for decision in self.decisions:
            token_index = {
                row.token_id: index
                for index, row in enumerate(decision.ordered_choices)
            }

            def selected_tokens(
                source: Sequence[Mapping[str, Any]],
            ) -> list[str]:
                selected = []
                for observation in source:
                    matches = [
                        str(row.get("selected_token_id") or "")
                        for row in observation.get("decision_trace") or ()
                        if str(row.get("context_id") or "")
                        == decision.context_id
                    ]
                    if len(matches) > 1:
                        raise RuntimeError(
                            "CEM_DUPLICATE_CONTEXT_IN_DECISION_TRACE:"
                            f"{decision.context_id}"
                        )
                    if matches:
                        if matches[0] not in token_index:
                            raise RuntimeError(
                                "CEM_TRACE_TOKEN_DRIFT:"
                                f"{decision.context_id}:{matches[0]}"
                            )
                        selected.append(matches[0])
                return selected

            active = selected_tokens(ranked)
            elite = selected_tokens(elites)
            support_ok = (
                len(active)
                >= self.parameters.minimum_active_observations_per_context
                and len(elite)
                >= self.parameters.minimum_elite_support_per_context
            )
            changed = False
            if support_ok:
                counts = np.zeros(len(decision.ordered_choices), dtype=float)
                for token_id in elite:
                    counts[token_index[token_id]] += 1.0
                p_hat = counts / float(counts.sum())
                p_old = self._probability_tables[decision.context_id]
                p_smooth = (
                    (1.0 - self.parameters.alpha) * p_old
                    + self.parameters.alpha * p_hat
                )
                p_new = (
                    (1.0 - self.parameters.uniform_mix) * p_smooth
                    + self.parameters.uniform_mix / len(p_smooth)
                )
                p_new = _cap_simplex(
                    p_new,
                    self.parameters.maximum_category_probability,
                )
                changed = not bool(
                    np.allclose(p_old, p_new, rtol=0.0, atol=1e-15)
                )
                self._probability_tables[decision.context_id] = p_new
                updated_context_count += int(changed)
            diagnostics.append(
                {
                    "decision_id": decision.decision_id,
                    "context_id": decision.context_id,
                    "active_observation_count": len(active),
                    "elite_support_count": len(elite),
                    "support_status": (
                        "UPDATED" if changed else (
                            "SUPPORTED_NO_NUMERIC_CHANGE"
                            if support_ok
                            else "INSUFFICIENT_ACTIVE_ELITE_SUPPORT"
                        )
                    ),
                    "probability_changed": changed,
                }
            )

        evaluated = [
            row
            for row in rows
            if str(row.get("outcome_class") or "") == "EVALUATED"
        ]
        negative_count = sum(
            float(row["signed_matched_increment"]) < 0.0
            for row in evaluated
        )
        self.reward_observation_count += len(evaluated)
        receipt = {
            "generation": self.generation,
            "observation_count": len(rows),
            "evaluated_observation_count": len(evaluated),
            "negative_evaluable_observation_count": negative_count,
            "elite_count": elite_count,
            "updated_context_count": updated_context_count,
            "probability_hash_before": before_hash,
            "probability_hash_after": _stable_hash(
                self.probability_tables
            ),
            "support_diagnostics": diagnostics,
            "observation_digest": _stable_hash(rows),
        }
        self.last_support_diagnostics = copy.deepcopy(diagnostics)
        self.generation += 1
        return receipt

    def state_dict(
        self,
        *,
        rng: np.random.Generator,
    ) -> dict[str, Any]:
        return {
            "schema_version": "cn_categorical_cem_state_v1",
            "state_origin": (
                "fresh_uniform_from_frozen_decision_catalog"
            ),
            "source_campaign": "none",
            "qualification_evidence_only": True,
            "forbidden_as_large_search_initialization": True,
            "formula_space_id": self.formula_space_id,
            "decision_catalog_hash": self.decision_catalog_hash,
            "decision_specs_hash": _stable_hash(
                [row.to_dict() for row in self.decisions]
            ),
            "generation": self.generation,
            "reward_observation_count": self.reward_observation_count,
            "parameters": asdict(self.parameters),
            "probability_tables": self.probability_tables,
            "rng_state": copy.deepcopy(rng.bit_generator.state),
            "support_diagnostics": copy.deepcopy(
                self.last_support_diagnostics
            ),
        }

    @classmethod
    def restore(
        cls,
        state: Mapping[str, Any],
        *,
        decisions: Sequence[DecisionSpec],
        decision_catalog_hash: str,
        formula_space_id: str,
        rng: np.random.Generator,
    ) -> "CategoricalCEMPolicy":
        if (
            str(state.get("decision_catalog_hash") or "")
            != str(decision_catalog_hash)
        ):
            raise RuntimeError("DECISION_CATALOG_HASH_DRIFT")
        if str(state.get("formula_space_id") or "") != str(
            formula_space_id
        ):
            raise RuntimeError("FORMULA_SPACE_ID_DRIFT")
        if str(state.get("source_campaign") or "") != "none":
            raise RuntimeError("CROSS_SPRINT_ADAPTIVE_MEMORY_FORBIDDEN")
        expected_specs = _stable_hash(
            [row.to_dict() for row in decisions]
        )
        if str(state.get("decision_specs_hash") or "") != expected_specs:
            raise RuntimeError("DECISION_SPECS_HASH_DRIFT")
        rng.bit_generator.state = copy.deepcopy(state["rng_state"])
        return cls(
            decisions=decisions,
            decision_catalog_hash=decision_catalog_hash,
            formula_space_id=formula_space_id,
            probability_tables=dict(state["probability_tables"]),
            parameters=CEMParameters(**dict(state["parameters"])),
            generation=int(state.get("generation") or 0),
            reward_observation_count=int(
                state.get("reward_observation_count") or 0
            ),
            last_support_diagnostics=list(
                state.get("support_diagnostics") or ()
            ),
        )
