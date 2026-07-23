"""Minimal official CatCMAwM adapter over frozen categorical gene semantics.

The optimizer selects route-local semantic category IDs only. It never
constructs expressions, changes route authority, or owns evaluation. The
authoritative Grammar, compiler, matched-control evaluator, behavior admission,
and development-only access gates remain outside this module.
"""

from __future__ import annotations

import copy
import hashlib
import json
import pickle
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np


EXPECTED_CMAES_VERSION = "0.13.0"
EVALUATED = "EVALUATED"
SUPPORT_BLOCKED = "SUPPORT_BLOCKED"
CONTROL_BLOCKED = "CONTROL_BLOCKED"
BEHAVIOR_BLOCKED = "BEHAVIOR_BLOCKED"
DETERMINISTIC_INVALID = "DETERMINISTIC_INVALID"
INFRASTRUCTURE_FAILURE = "INFRASTRUCTURE_FAILURE"

_BLOCKER_OUTCOMES = {SUPPORT_BLOCKED, CONTROL_BLOCKED, BEHAVIOR_BLOCKED}
_VALID_OUTCOMES = {
    EVALUATED,
    *_BLOCKER_OUTCOMES,
    DETERMINISTIC_INVALID,
    INFRASTRUCTURE_FAILURE,
}


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _optimizer_state_hash(optimizer: Any) -> str:
    """Diagnostic state hash only; replay history remains restore authority."""

    return hashlib.sha256(pickle.dumps(optimizer)).hexdigest()


def _load_catcmawm() -> tuple[type[Any], str, str]:
    try:
        import cmaes
        from cmaes import CatCMAwM
    except ImportError as exc:  # pragma: no cover - exercised on launch hosts.
        raise RuntimeError("OFFICIAL_CMAES_PACKAGE_MISSING") from exc
    version = str(getattr(cmaes, "__version__", ""))
    if version != EXPECTED_CMAES_VERSION:
        raise RuntimeError(
            f"CMAES_VERSION_MISMATCH: expected={EXPECTED_CMAES_VERSION} actual={version}"
        )
    return CatCMAwM, version, str(getattr(cmaes, "__file__", ""))


@dataclass(frozen=True, slots=True)
class ExactGeneSemantics:
    """Immutable meaning and category order of every CatCMA categorical row."""

    route_id: str
    ordered_gene_slot_names: tuple[str, ...]
    ordered_category_ids_by_slot: tuple[tuple[str, ...], ...]
    none_semantics: Mapping[str, str]
    skeleton_compatibility: Mapping[str, Any]
    registry_hash: str
    root_contract_hash: str
    grammar_hash: str
    exact_gene_semantics_hash: str

    @classmethod
    def freeze(
        cls,
        *,
        route_id: str,
        ordered_categories_by_slot: Mapping[str, Sequence[str]],
        none_semantics: Mapping[str, str],
        skeleton_compatibility: Mapping[str, Any],
        registry_hash: str,
        root_contract_hash: str,
        grammar_hash: str,
    ) -> "ExactGeneSemantics":
        slot_names = tuple(map(str, ordered_categories_by_slot))
        if not slot_names:
            raise ValueError("CatCMA requires at least one categorical gene slot")
        if len(set(slot_names)) != len(slot_names):
            raise ValueError("gene slot names must be unique and ordered")
        categories = tuple(
            tuple(map(str, ordered_categories_by_slot[slot]))
            for slot in slot_names
        )
        for slot, category_ids in zip(slot_names, categories):
            if len(category_ids) < 2:
                raise ValueError(
                    f"CatCMA gene slot requires at least two categories: {slot}"
                )
            if len(set(category_ids)) != len(category_ids):
                raise ValueError(
                    f"category IDs must be unique and ordered within slot: {slot}"
                )
        if set(map(str, none_semantics)) != set(slot_names):
            raise ValueError("NONE semantics must cover every gene slot exactly")
        payload = {
            "route_id": str(route_id),
            "ordered_gene_slot_names": list(slot_names),
            "ordered_category_ids_by_slot": {
                slot: list(category_ids)
                for slot, category_ids in zip(slot_names, categories)
            },
            "none_semantics": {
                slot: str(none_semantics[slot]) for slot in slot_names
            },
            "skeleton_compatibility": dict(skeleton_compatibility),
            "registry_hash": str(registry_hash),
            "root_contract_hash": str(root_contract_hash),
            "grammar_hash": str(grammar_hash),
        }
        return cls(
            route_id=str(route_id),
            ordered_gene_slot_names=slot_names,
            ordered_category_ids_by_slot=categories,
            none_semantics={
                slot: str(none_semantics[slot]) for slot in slot_names
            },
            skeleton_compatibility=copy.deepcopy(
                dict(skeleton_compatibility)
            ),
            registry_hash=str(registry_hash),
            root_contract_hash=str(root_contract_hash),
            grammar_hash=str(grammar_hash),
            exact_gene_semantics_hash=_stable_hash(payload),
        )

    def categories_for_slot(self, slot_name: str) -> tuple[str, ...]:
        try:
            index = self.ordered_gene_slot_names.index(str(slot_name))
        except ValueError as exc:
            raise KeyError(f"unknown gene slot: {slot_name}") from exc
        return self.ordered_category_ids_by_slot[index]

    def to_dict(self) -> dict[str, Any]:
        return {
            "route_id": self.route_id,
            "ordered_gene_slot_names": list(self.ordered_gene_slot_names),
            "ordered_category_ids_by_slot": {
                slot: list(category_ids)
                for slot, category_ids in zip(
                    self.ordered_gene_slot_names,
                    self.ordered_category_ids_by_slot,
                )
            },
            "none_semantics": dict(self.none_semantics),
            "skeleton_compatibility": copy.deepcopy(
                dict(self.skeleton_compatibility)
            ),
            "registry_hash": self.registry_hash,
            "root_contract_hash": self.root_contract_hash,
            "grammar_hash": self.grammar_hash,
            "exact_gene_semantics_hash": self.exact_gene_semantics_hash,
        }


def rank_population_observations(
    observations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return deterministic minimization losses without novelty reward shaping."""

    normalized: list[dict[str, Any]] = []
    for source in observations:
        row = dict(source)
        outcome = str(row.get("outcome_class") or "")
        if outcome not in _VALID_OUTCOMES:
            raise ValueError(f"unknown CatCMA observation outcome: {outcome}")
        if outcome == INFRASTRUCTURE_FAILURE:
            raise RuntimeError("INFRASTRUCTURE_FAILURE_INVALIDATES_WHOLE_POPULATION")
        if outcome == EVALUATED:
            try:
                increment = float(row["signed_matched_increment"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    "evaluated observation requires signed_matched_increment"
                ) from exc
            if not np.isfinite(increment):
                raise ValueError("signed_matched_increment must be finite")
            tier = 0
            objective_key = -increment
        elif outcome in _BLOCKER_OUTCOMES:
            tier = 1
            objective_key = 0.0
        else:
            tier = 2
            objective_key = 0.0
        exact_identity = str(
            row.get("exact_identity") or row.get("proposal_id") or ""
        )
        normalized.append(
            {
                **row,
                "outcome_class": outcome,
                "signed_matched_increment": (
                    float(row["signed_matched_increment"])
                    if outcome == EVALUATED
                    else None
                ),
                "_rank_key": (tier, objective_key, exact_identity),
            }
        )
    ranked = sorted(normalized, key=lambda row: row["_rank_key"])
    loss_by_id = {
        str(row["proposal_id"]): float(rank)
        for rank, row in enumerate(ranked)
    }
    return [
        {
            **{key: value for key, value in row.items() if key != "_rank_key"},
            "loss": loss_by_id[str(row["proposal_id"])],
        }
        for row in normalized
    ]


class CatCMASearchAdapter:
    """One route-local official CatCMA optimizer over exact gene semantics."""

    def __init__(
        self,
        *,
        semantics: ExactGeneSemantics,
        seed: int,
        population_size: int,
    ) -> None:
        if population_size < 2:
            raise ValueError("population_size must be at least two")
        CatCMAwM, version, package_path = _load_catcmawm()
        self.semantics = semantics
        self.seed = int(seed)
        self.population_size = int(population_size)
        self.package_version = version
        self.package_path = package_path
        self._optimizer = CatCMAwM(
            c_space=[
                len(category_ids)
                for category_ids in semantics.ordered_category_ids_by_slot
            ],
            population_size=self.population_size,
            seed=self.seed,
        )
        if int(self._optimizer.population_size) != self.population_size:
            raise RuntimeError("CatCMA population size drift")
        self.generation = 0
        self._pending: dict[str, dict[str, Any]] = {}
        self._history: list[dict[str, Any]] = []

    def environment_receipt(self) -> dict[str, Any]:
        return {
            "optimizer": "official_cmaes.CatCMAwM",
            "package_version": self.package_version,
            "package_path": self.package_path,
            "seed": self.seed,
            "population_size": self.population_size,
            "categorical_gene_slot_count": len(
                self.semantics.ordered_gene_slot_names
            ),
            "restore_authority": "GENESIS_PLUS_ASK_TRANSCRIPT_PLUS_TELL_LOSSES",
            "pickle_usage": "DIAGNOSTIC_STATE_HASH_ONLY",
        }

    @property
    def history(self) -> tuple[dict[str, Any], ...]:
        return tuple(copy.deepcopy(self._history))

    @property
    def has_pending_population(self) -> bool:
        return bool(self._pending)

    def ask_population(
        self,
        *,
        checkpoint_id: str,
        expected_genes: Sequence[Mapping[str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        if self._pending:
            raise RuntimeError("ASK_BEFORE_PENDING_POPULATION_TELL")
        state_hash_before = _optimizer_state_hash(self._optimizer)
        rows: list[dict[str, Any]] = []
        seen_gene_signatures: set[str] = set()
        for ordinal in range(self.population_size):
            solution = self._optimizer.ask()
            categorical = np.asarray(solution.c)
            slot_count = len(self.semantics.ordered_gene_slot_names)
            if categorical.ndim != 2 or categorical.shape[0] != slot_count:
                raise RuntimeError(
                    f"unexpected CatCMA categorical shape: {categorical.shape}"
                )
            genes: dict[str, str] = {}
            category_indices: dict[str, int] = {}
            for slot_index, (slot_name, category_ids) in enumerate(
                zip(
                    self.semantics.ordered_gene_slot_names,
                    self.semantics.ordered_category_ids_by_slot,
                )
            ):
                category_index = int(
                    np.argmax(categorical[slot_index, : len(category_ids)])
                )
                category_indices[slot_name] = category_index
                genes[slot_name] = category_ids[category_index]
            gene_signature = _stable_hash(
                {
                    "semantics": self.semantics.exact_gene_semantics_hash,
                    "genes": genes,
                }
            )
            proposal_id = _stable_hash(
                {
                    "semantics": self.semantics.exact_gene_semantics_hash,
                    "seed": self.seed,
                    "generation": self.generation,
                    "ask_ordinal": ordinal,
                    "genes": genes,
                }
            )[:24]
            row = {
                "proposal_id": proposal_id,
                "checkpoint_id": str(checkpoint_id),
                "generation": self.generation,
                "ask_ordinal": ordinal,
                "route_id": self.semantics.route_id,
                "genes": genes,
                "gene_category_indices": category_indices,
                "gene_signature": gene_signature,
                # Compatibility alias for common concentration accounting.
                "category_id": gene_signature,
                "exact_gene_semantics_hash": (
                    self.semantics.exact_gene_semantics_hash
                ),
                "duplicate_in_population": (
                    gene_signature in seen_gene_signatures
                ),
                "optimizer_state_hash_before_ask": state_hash_before,
            }
            seen_gene_signatures.add(gene_signature)
            rows.append(row)
            self._pending[proposal_id] = {
                "solution": solution,
                "transcript": copy.deepcopy(row),
            }
        actual = [dict(row["genes"]) for row in rows]
        if expected_genes is not None and actual != [
            dict(row) for row in expected_genes
        ]:
            self._pending.clear()
            raise RuntimeError("CHECKPOINT_REPLAY_NEXT_ASK_DIVERGED")
        return rows

    def tell_population(
        self,
        observations: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        if len(self._pending) != self.population_size:
            raise RuntimeError("TELL_WITHOUT_COMPLETE_PENDING_POPULATION")
        if len(observations) != self.population_size:
            raise ValueError("tell requires exactly one observation per asked solution")
        observation_ids = [
            str(row.get("proposal_id") or "") for row in observations
        ]
        if len(set(observation_ids)) != len(observation_ids):
            raise ValueError("tell observations contain duplicate proposal IDs")
        if set(observation_ids) != set(self._pending):
            raise ValueError("tell accepts only the complete currently asked population")
        ranked = rank_population_observations(observations)
        by_id = {str(row["proposal_id"]): row for row in ranked}
        solutions = [
            (
                self._pending[proposal_id]["solution"],
                float(by_id[proposal_id]["loss"]),
            )
            for proposal_id in self._pending
        ]
        state_hash_before = _optimizer_state_hash(self._optimizer)
        self._optimizer.tell(solutions)
        state_hash_after = _optimizer_state_hash(self._optimizer)
        transcript = [
            copy.deepcopy(self._pending[proposal_id]["transcript"])
            for proposal_id in self._pending
        ]
        ordered_observations = [
            copy.deepcopy(by_id[str(row["proposal_id"])]) for row in transcript
        ]
        receipt = {
            "generation": self.generation,
            "proposal_transcript": transcript,
            "observations": ordered_observations,
            "optimizer_state_hash_before_tell": state_hash_before,
            "optimizer_state_hash_after_tell": state_hash_after,
            "optimizer_state_changed": state_hash_before != state_hash_after,
        }
        self._history.append(receipt)
        self._pending.clear()
        self.generation += 1
        return copy.deepcopy(receipt)

    def history_receipt(self) -> dict[str, Any]:
        return {
            "schema_version": "catcma_genesis_replay_v2",
            "semantics": self.semantics.to_dict(),
            "environment": self.environment_receipt(),
            "generation_count": len(self._history),
            "generations": copy.deepcopy(self._history),
            "history_hash": _stable_hash(self._history),
        }

    @classmethod
    def replay(
        cls,
        *,
        semantics: ExactGeneSemantics,
        seed: int,
        population_size: int,
        generations: Sequence[Mapping[str, Any]],
    ) -> "CatCMASearchAdapter":
        adapter = cls(
            semantics=semantics,
            seed=seed,
            population_size=population_size,
        )
        for expected in generations:
            transcript = list(expected.get("proposal_transcript") or ())
            if len(transcript) != population_size:
                raise RuntimeError("CHECKPOINT_REPLAY_TRANSCRIPT_CARDINALITY_DRIFT")
            asked = adapter.ask_population(
                checkpoint_id=str(transcript[0]["checkpoint_id"]),
                expected_genes=[
                    dict(row.get("genes") or {}) for row in transcript
                ],
            )
            if [str(row["proposal_id"]) for row in asked] != [
                str(row["proposal_id"]) for row in transcript
            ]:
                raise RuntimeError("CHECKPOINT_REPLAY_PROPOSAL_ID_DIVERGED")
            receipt = adapter.tell_population(
                list(expected.get("observations") or ())
            )
            expected_losses = [
                float(row["loss"])
                for row in expected.get("observations") or ()
            ]
            actual_losses = [
                float(row["loss"]) for row in receipt["observations"]
            ]
            if actual_losses != expected_losses:
                raise RuntimeError("CHECKPOINT_REPLAY_LOSS_DIVERGED")
        return adapter

    def next_ask_preview(
        self, *, checkpoint_id: str
    ) -> list[dict[str, str]]:
        replayed = self.replay(
            semantics=self.semantics,
            seed=self.seed,
            population_size=self.population_size,
            generations=self._history,
        )
        asked = replayed.ask_population(checkpoint_id=checkpoint_id)
        return [dict(row["genes"]) for row in asked]
