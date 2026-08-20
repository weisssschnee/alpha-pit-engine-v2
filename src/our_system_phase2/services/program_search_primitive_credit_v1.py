"""Confirmed hierarchical primitive-credit Program search.

This module productionizes the exact primitive-local scoring rule that was
prospectively confirmed in Stage D.  The scorer is deliberately frozen-prior:
financial feedback observed during a campaign is recorded for audit but does
not mutate ranking state.  A future prior refresh must be a separately frozen
artifact/campaign generation.
"""
from __future__ import annotations

import copy
import math
from typing import Any, Mapping, Sequence

from our_system_phase2.services.program_search_optimizer_v1 import (
    PROGRAM_ROUTE_ID,
    ProgramOptimizerObservationV1,
    _AvailabilityProgramOptimizer,
    _batch_feasible_entries,
    _batch_group_counts,
    _record_batch_group_selection,
)
from our_system_phase2.services.route_local_availability import (
    AvailabilityEntry,
    RouteLocalAvailabilityController,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1 = "PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1"
PRIMITIVE_SCORER_POLICY_ID = "CN_HIERARCHICAL_PRIMITIVE_CREDIT_SCORER_V1"
PRIMITIVE_FEEDBACK_MODE = "FROZEN_PRIOR_AUDIT_ONLY"


def _shrink(stat: Mapping[str, Any] | None, parent: float, strength: float) -> float:
    row = dict(stat or {})
    n = float(row.get("n") or 0.0)
    productive = float(row.get("productive") or 0.0)
    return (productive + float(strength) * float(parent)) / (n + float(strength))


def verify_primitive_stats_v1(
    payload: Mapping[str, Any], *, expected_payload_sha256: str | None = None
) -> dict[str, Any]:
    row = copy.deepcopy(dict(payload))
    body = dict(row)
    claimed = str(body.pop("stats_payload_sha256", ""))
    if not claimed or stable_hash(body) != claimed:
        raise ValueError("PRIMITIVE_CREDIT_STATS_SELF_HASH_DRIFT")
    if expected_payload_sha256 is not None and claimed != str(expected_payload_sha256):
        raise ValueError("PRIMITIVE_CREDIT_STATS_AUTHORITY_DRIFT")
    if (
        row.get("status") != "SPENT_DEVELOPMENT_PRIMITIVE_CREDIT_STATS_FROZEN"
        or bool(row.get("financial_evaluation_performed_by_builder"))
        or int(row.get("validation_reads") or 0) != 0
        or int(row.get("holdout_reads") or 0) != 0
        or int(row.get("forward_2026_reads") or 0) != 0
    ):
        raise ValueError("PRIMITIVE_CREDIT_STATS_CONTRACT_DRIFT")
    for key in (
        "global",
        "template",
        "role",
        "component_route",
        "component_skeleton",
        "component_exact",
    ):
        if not isinstance(row.get(key), Mapping):
            raise ValueError(f"PRIMITIVE_CREDIT_STATS_SECTION_MISSING:{key}")
    return row


def primitive_program_metadata_v1(
    *,
    entries: Sequence[AvailabilityEntry],
    catalog_by_exact: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Bind normalized Program exacts to the component identities needed by the scorer."""
    output: dict[str, dict[str, Any]] = {}
    for entry in entries:
        exact = str(entry.exact_identity)
        source = dict(catalog_by_exact.get(exact) or {})
        reservoir = dict(source.get("reservoir") or {})
        components = dict(reservoir.get("components") or {})
        template_id = str(entry.genes.get("program_template_id") or "")
        if not source or str(source.get("template_id") or "") != template_id:
            raise ValueError(f"PRIMITIVE_PROGRAM_METADATA_SOURCE_DRIFT:{exact}")
        if not components:
            raise ValueError(f"PRIMITIVE_PROGRAM_METADATA_COMPONENTS_MISSING:{exact}")
        bound: dict[str, dict[str, str]] = {}
        for role, raw in sorted(components.items()):
            binding = dict(raw)
            component_id = str(binding.get("component_id") or "")
            route_id = str(binding.get("route_id") or "")
            skeleton_id = str(entry.genes.get(f"{role}__skeleton_id") or "")
            if not component_id or not route_id or not skeleton_id:
                raise ValueError(f"PRIMITIVE_PROGRAM_METADATA_BINDING_DRIFT:{exact}:{role}")
            bound[str(role)] = {
                "component_id": component_id,
                "route_id": route_id,
                "skeleton_id": skeleton_id,
            }
        output[exact] = {
            "template_id": template_id,
            "components": bound,
            # Stage C/D ranked ties by the raw structural-gene hash before the
            # catalog normalized inactive slots.  Preserve that exact ordering
            # while keeping normalized Program exact as availability identity.
            "tie_break_identity": stable_hash(dict(source["program_genes"])),
        }
    if set(output) != {str(entry.exact_identity) for entry in entries}:
        raise ValueError("PRIMITIVE_PROGRAM_METADATA_COVERAGE_DRIFT")
    return output


class HierarchicalPrimitiveCreditScorerV1:
    """Byte-stable production form of the Stage-C/Stage-D primitive scorer."""

    def __init__(
        self,
        *,
        primitive_stats: Mapping[str, Any],
        primitive_stats_payload_sha256: str | None = None,
    ) -> None:
        self.stats = verify_primitive_stats_v1(
            primitive_stats, expected_payload_sha256=primitive_stats_payload_sha256
        )
        self.stats_payload_sha256 = str(self.stats["stats_payload_sha256"])
        global_row = dict(self.stats["global"])
        self.global_prior = (float(global_row["productive"]) + 1.0) / (
            float(global_row["n"]) + 2.0
        )

    def score(self, metadata: Mapping[str, Any]) -> dict[str, Any]:
        row = dict(metadata)
        template_id = str(row.get("template_id") or "")
        components = dict(row.get("components") or {})
        if not template_id or not components:
            raise ValueError("PRIMITIVE_SCORER_METADATA_DRIFT")
        template_p = _shrink(
            dict(self.stats["template"]).get(template_id), self.global_prior, 32.0
        )
        role_posteriors: list[float] = []
        exposures: list[int] = []
        component_scores: dict[str, dict[str, Any]] = {}
        for role, raw in sorted(components.items()):
            binding = dict(raw)
            component_id = str(binding["component_id"])
            route_id = str(binding["route_id"])
            skeleton_id = str(binding["skeleton_id"])
            role_p = _shrink(
                dict(self.stats["role"]).get(str(role)), self.global_prior, 32.0
            )
            route_p = _shrink(
                dict(self.stats["component_route"]).get(f"{role}|{route_id}"),
                role_p,
                24.0,
            )
            skeleton_p = _shrink(
                dict(self.stats["component_skeleton"]).get(
                    f"{role}|{skeleton_id}"
                ),
                route_p,
                12.0,
            )
            exact_stat = dict(self.stats["component_exact"]).get(
                f"{role}|{component_id}"
            )
            exact_p = _shrink(exact_stat, skeleton_p, 6.0)
            exposure = int(dict(exact_stat or {}).get("n") or 0)
            exposures.append(exposure)
            role_posteriors.append(exact_p)
            component_scores[str(role)] = {
                "component_id": component_id,
                "route_id": route_id,
                "skeleton_id": skeleton_id,
                "exposure": exposure,
                "posterior": exact_p,
                "skeleton_backoff": skeleton_p,
                "route_backoff": route_p,
            }
        geometric = math.exp(
            sum(math.log(max(1e-12, value)) for value in role_posteriors)
            / len(role_posteriors)
        )
        primitive_score = geometric * (0.75 + 0.25 * template_p)
        novelty_score = sum(1.0 / math.sqrt(n + 1.0) for n in exposures) / len(
            exposures
        )
        return {
            "policy_id": PRIMITIVE_SCORER_POLICY_ID,
            "primitive_score": float(primitive_score),
            "novelty_score": float(novelty_score),
            "global_prior": float(self.global_prior),
            "template_backoff": float(template_p),
            "components": component_scores,
            "primitive_stats_payload_sha256": self.stats_payload_sha256,
        }


class PrimitiveLocalProgramSearchAdapterV1(_AvailabilityProgramOptimizer):
    """Main Program search adapter backed by the confirmed frozen primitive prior."""

    def __init__(
        self,
        *,
        metadata_by_exact_identity: Mapping[str, Mapping[str, Any]],
        primitive_stats: Mapping[str, Any],
        primitive_stats_payload_sha256: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(arm=PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1, **kwargs)
        self.metadata_by_exact_identity = {
            str(key): copy.deepcopy(dict(value))
            for key, value in metadata_by_exact_identity.items()
        }
        expected = {str(entry.exact_identity) for entry in self.entries}
        if set(self.metadata_by_exact_identity) != expected:
            raise ValueError("PRIMITIVE_PROGRAM_METADATA_EXACT_COVERAGE_DRIFT")
        self.metadata_payload_sha256 = stable_hash(
            dict(sorted(self.metadata_by_exact_identity.items()))
        )
        self.scorer = HierarchicalPrimitiveCreditScorerV1(
            primitive_stats=primitive_stats,
            primitive_stats_payload_sha256=primitive_stats_payload_sha256,
        )
        self.score_by_exact = {
            exact: self.scorer.score(metadata)
            for exact, metadata in self.metadata_by_exact_identity.items()
        }

    def _ranked_remaining(
        self,
        *,
        required_program_template_id: str | None,
        eligible_exact_identities: Sequence[str] | None,
        batch_group_constraint: Mapping[str, Any] | None,
    ) -> tuple[AvailabilityEntry, ...]:
        constraint, group_counts = _batch_group_counts(batch_group_constraint)
        remaining = _batch_feasible_entries(
            self._remaining_entries(
                required_program_template_id, eligible_exact_identities
            ),
            constraint=constraint,
            group_counts=group_counts,
        )
        return tuple(
            sorted(
                remaining,
                key=lambda entry: (
                    -float(self.score_by_exact[entry.exact_identity]["primitive_score"]),
                    str(
                        self.metadata_by_exact_identity[entry.exact_identity].get(
                            "tie_break_identity"
                        )
                        or entry.exact_identity
                    ),
                ),
            )
        )

    def ask(
        self,
        *,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str | None = None,
        eligible_exact_identities: Sequence[str] | None = None,
        batch_group_constraint: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if self._pending:
            raise RuntimeError("PROGRAM_OPTIMIZER_PENDING_NOT_TOLD")
        constraint, group_counts = _batch_group_counts(batch_group_constraint)
        asked: list[dict[str, Any]] = []
        for ordinal in range(int(count)):
            remaining = _batch_feasible_entries(
                self._remaining_entries(
                    required_program_template_id, eligible_exact_identities
                ),
                constraint=constraint,
                group_counts=group_counts,
            )
            if not remaining:
                break
            entry = min(
                remaining,
                key=lambda candidate: (
                    -float(
                        self.score_by_exact[candidate.exact_identity][
                            "primitive_score"
                        ]
                    ),
                    str(
                        self.metadata_by_exact_identity[candidate.exact_identity].get(
                            "tie_break_identity"
                        )
                        or candidate.exact_identity
                    ),
                ),
            )
            score = self.score_by_exact[entry.exact_identity]
            emission = self.controller.reserve_exact(
                route_id=PROGRAM_ROUTE_ID,
                exact_identity=entry.exact_identity,
                emission_mode="PROGRAM_PRIMITIVE_LOCAL_HIERARCHICAL_V1",
                source_exact_identity=self.scorer.stats_payload_sha256,
            )
            if emission is None:
                raise RuntimeError("PRIMITIVE_PROGRAM_RESERVATION_FAILED")
            feasibility = _record_batch_group_selection(
                emission.exact_identity,
                constraint=constraint,
                group_counts=group_counts,
            )
            feasibility["batch_feasible_count_at_selection"] = len(remaining)
            proposal_id = stable_hash(
                {
                    "arm": self.arm,
                    "checkpoint_id": str(checkpoint_id),
                    "ask_ordinal": int(ordinal),
                    "exact_identity": emission.exact_identity,
                    "primitive_stats_payload_sha256": self.scorer.stats_payload_sha256,
                }
            )[:24]
            row = self._ask_row(
                emission=emission,
                checkpoint_id=str(checkpoint_id),
                ask_ordinal=int(ordinal),
                proposal_id=proposal_id,
                trial_number=None,
                optimizer_ask_identity=proposal_id,
                acquisition={
                    "source": PRIMITIVE_SCORER_POLICY_ID,
                    "primitive_score": float(score["primitive_score"]),
                    "novelty_score": float(score["novelty_score"]),
                    "primitive_stats_payload_sha256": self.scorer.stats_payload_sha256,
                    "online_feedback_mode": PRIMITIVE_FEEDBACK_MODE,
                },
                batch_group_feasibility=feasibility,
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
        productive = 0
        admitted = 0
        for proposal_id in self._pending:
            observation = by_id[proposal_id]
            admitted += int(bool(observation.admission.admitted))
            if observation.admission.admitted and observation.uplift is not None:
                credit = dict(observation.uplift.program_credit)
                productive += int(
                    float(credit.get("matched_cumulative_net_return_increment") or 0.0)
                    > 0.0
                    and float(credit.get("matched_net_reward_increment") or 0.0) > 0.0
                )
        receipt = {
            "schema_version": "cn_primitive_local_program_tell_v1",
            "optimizer_arm": self.arm,
            "asked_count": len(by_id),
            "admitted_count": admitted,
            "productive_count": productive,
            "optimizer_feedback_applied": False,
            "online_feedback_mode": PRIMITIVE_FEEDBACK_MODE,
            "prior_stats_mutated": False,
            "primitive_stats_payload_sha256": self.scorer.stats_payload_sha256,
        }
        self._history.append(receipt)
        self._pending.clear()
        return receipt

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            "optimizer_arm": self.arm,
            "policy_id": PRIMITIVE_SCORER_POLICY_ID,
            "learning_mode": PRIMITIVE_FEEDBACK_MODE,
            "primitive_stats_payload_sha256": self.scorer.stats_payload_sha256,
            "metadata_payload_sha256": self.metadata_payload_sha256,
            "online_feedback_applied": False,
            "stage_c_or_stage_d_feedback_imported": False,
        }

    @classmethod
    def restore(cls, **kwargs: Any) -> "PrimitiveLocalProgramSearchAdapterV1":
        snapshot = dict(kwargs.pop("snapshot"))
        body = dict(snapshot)
        claimed = str(body.pop("snapshot_hash", ""))
        if not claimed or stable_hash(body) != claimed:
            raise ValueError("PRIMITIVE_PROGRAM_SNAPSHOT_SELF_HASH_DRIFT")
        adapter = cls(**kwargs)
        adapter.controller = RouteLocalAvailabilityController.restore(
            entries=adapter.entries,
            seen_exact_identities=adapter.seen_exact_identities,
            state=dict(snapshot["availability"]),
            input_hashes=adapter.controller.input_hashes,
        )
        adapter._history = copy.deepcopy(list(snapshot["history"]))
        metadata = dict(snapshot.get("optimizer_metadata") or {})
        if (
            metadata.get("primitive_stats_payload_sha256")
            != adapter.scorer.stats_payload_sha256
            or metadata.get("metadata_payload_sha256")
            != adapter.metadata_payload_sha256
        ):
            raise ValueError("PRIMITIVE_PROGRAM_SNAPSHOT_AUTHORITY_DRIFT")
        if adapter.snapshot() != snapshot:
            raise ValueError("PRIMITIVE_PROGRAM_SNAPSHOT_REPLAY_DRIFT")
        return adapter


__all__ = [
    "PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1",
    "PRIMITIVE_SCORER_POLICY_ID",
    "PRIMITIVE_FEEDBACK_MODE",
    "HierarchicalPrimitiveCreditScorerV1",
    "PrimitiveLocalProgramSearchAdapterV1",
    "primitive_program_metadata_v1",
    "verify_primitive_stats_v1",
]
