"""Optimizer-style adapter for the semantic state-jump Program generator.

Unlike availability-based optimizers, this adapter creates new typed Programs by
recomposing authorized component pairs.  It normalizes every generated Program
onto the frozen Program gene schema, rejects identities already present in the
frozen/prior space, and exposes the same ask/tell lifecycle used by the existing
Program tournament.
"""
from __future__ import annotations

import copy
from typing import Any, Mapping, Sequence

from our_system_phase2.services.candidate_program_controls_v1 import (
    construct_matched_control_program_v1,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    CandidateProgramProposalAdapterV0,
    ProgramSourceComponentV0,
)
from our_system_phase2.services.candidate_program_v1 import ProgramCompilerV1
from our_system_phase2.services.program_search_optimizer_v1 import (
    PROGRAM_SPACE_ID,
    ProgramOptimizerObservationV1,
    ProgramSearchOptimizerAdapter,
    normalized_program_gene_identity_v1,
    program_structural_genes_v1,
)
from our_system_phase2.services.program_search_state_jump_generator_v2 import (
    SEMANTIC_STATE_JUMP_GENERATOR_V2,
    GeneratedProgramV2,
    ProgramStateJumpSearchMemoryV2,
    SemanticStateJumpProgramGeneratorV2,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


STATE_JUMP_ADAPTER_SCHEMA = "cn_program_state_jump_optimizer_adapter_v2"
STATE_JUMP_ASK_SCHEMA = "cn_program_state_jump_optimizer_ask_v2"
STATE_JUMP_TELL_SCHEMA = "cn_program_state_jump_optimizer_tell_v2"


class StateJumpProgramSearchAdapterV2(ProgramSearchOptimizerAdapter):
    """Replayable ask/tell adapter whose proposal space is not a frozen catalog."""

    def __init__(
        self,
        *,
        adapter: CandidateProgramProposalAdapterV0,
        compiler: ProgramCompilerV1,
        components_by_role: Mapping[str, Sequence[ProgramSourceComponentV0]],
        ordered_gene_slots: Sequence[str],
        seen_exact_identities: Sequence[str],
        seed: int,
        memory: ProgramStateJumpSearchMemoryV2 | None = None,
        generator_snapshot: Mapping[str, Any] | None = None,
        operation_priors: Mapping[str, float] | None = None,
        maximum_attempts: int = 96,
    ) -> None:
        self.arm = SEMANTIC_STATE_JUMP_GENERATOR_V2
        self.adapter = adapter
        self.compiler = compiler
        self.components_by_role = {
            str(role): tuple(components)
            for role, components in components_by_role.items()
        }
        self.ordered_gene_slots = tuple(map(str, ordered_gene_slots))
        if not self.ordered_gene_slots:
            raise ValueError("STATE_JUMP_ADAPTER_GENE_SCHEMA_EMPTY")
        self.seen_exact_identities = frozenset(map(str, seen_exact_identities))
        if generator_snapshot is None:
            self.generator = SemanticStateJumpProgramGeneratorV2(
                adapter=adapter,
                components_by_role=self.components_by_role,
                memory=memory,
                seed=int(seed),
                operation_priors=operation_priors,
                maximum_attempts=int(maximum_attempts),
            )
        else:
            self.generator = SemanticStateJumpProgramGeneratorV2.restore(
                adapter=adapter,
                components_by_role=self.components_by_role,
                snapshot=generator_snapshot,
            )
        self._pending: dict[str, dict[str, Any]] = {}
        self._pending_generated: dict[str, GeneratedProgramV2] = {}
        self._history: list[dict[str, Any]] = []
        self._generated_exact_identities: set[str] = set()

    @property
    def observation_count(self) -> int:
        return int(sum(int(row.get("asked_count", 0)) for row in self._history))

    def proposal_identity(self, ask: Mapping[str, Any]) -> str:
        identity = str(ask.get("proposal_id") or "")
        if not identity:
            raise ValueError("STATE_JUMP_PROPOSAL_ID_MISSING")
        return identity

    @staticmethod
    def _constraint_state(
        batch_group_constraint: Mapping[str, Any] | None,
    ) -> tuple[int, int, dict[str, int]]:
        if not batch_group_constraint:
            return 0, 2**31 - 1, {}
        minimum = int(batch_group_constraint.get("minimum_distinct_groups") or 0)
        maximum = int(batch_group_constraint.get("maximum_per_group") or 2**31 - 1)
        counts = {
            str(key): int(value)
            for key, value in dict(
                batch_group_constraint.get("current_group_counts") or {}
            ).items()
            if int(value) > 0
        }
        if minimum < 0 or maximum < 1 or any(value > maximum for value in counts.values()):
            raise ValueError("STATE_JUMP_BATCH_GROUP_CONSTRAINT_INVALID")
        return minimum, maximum, counts

    @staticmethod
    def _base_group_feasible(
        base_group: str,
        *,
        minimum_distinct: int,
        maximum_per_group: int,
        counts: Mapping[str, int],
    ) -> bool:
        count = int(counts.get(str(base_group), 0))
        if count >= int(maximum_per_group):
            return False
        distinct = sum(int(value) > 0 for value in counts.values())
        return not (distinct < int(minimum_distinct) and count > 0)

    def _generated_identity(
        self, generated: GeneratedProgramV2
    ) -> tuple[str, dict[str, str], dict[str, Any]]:
        compiled = self.compiler.compile(generated.program)
        genes = program_structural_genes_v1(
            program_template_id=generated.template_id,
            components=generated.components,
            combination_policy=generated.combination_policy,
            program=generated.program,
            compiled=compiled,
        )
        exact = normalized_program_gene_identity_v1(
            genes,
            ordered_slots=self.ordered_gene_slots,
        )
        return exact, genes, compiled.to_record()

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
        if required_program_template_id is None:
            raise ValueError("STATE_JUMP_REQUIRES_TEMPLATE_LANE")
        if eligible_exact_identities:
            # A generated Program does not exist in a pre-enumerated exact list.
            # Supply exclusion is instead enforced against seen/frozen exacts.
            raise ValueError("STATE_JUMP_PREENUMERATED_ELIGIBLE_SET_FORBIDDEN")
        minimum, maximum, group_counts = self._constraint_state(
            batch_group_constraint
        )
        asked: list[dict[str, Any]] = []
        for ordinal in range(int(count)):
            accepted: tuple[GeneratedProgramV2, str, dict[str, str], dict[str, Any]] | None = None
            for _ in range(self.generator.maximum_attempts):
                generated = self.generator.propose(
                    batch_id=str(checkpoint_id),
                    ask_ordinal=int(ordinal),
                    template_id=str(required_program_template_id),
                )
                exact, genes, compiled = self._generated_identity(generated)
                if exact in self.seen_exact_identities or exact in self._generated_exact_identities:
                    continue
                base_group = generated.components["base"].component_id
                if not self._base_group_feasible(
                    base_group,
                    minimum_distinct=minimum,
                    maximum_per_group=maximum,
                    counts=group_counts,
                ):
                    continue
                accepted = (generated, exact, genes, compiled)
                break
            if accepted is None:
                raise RuntimeError("STATE_JUMP_ADAPTER_NO_FRESH_FEASIBLE_PROGRAM")
            generated, exact, genes, compiled = accepted
            base_group = generated.components["base"].component_id
            count_before = int(group_counts.get(base_group, 0))
            distinct_before = sum(int(value) > 0 for value in group_counts.values())
            group_counts[base_group] = count_before + 1
            proposal_id = stable_hash(
                {
                    "arm": self.arm,
                    "checkpoint_id": str(checkpoint_id),
                    "ask_ordinal": int(ordinal),
                    "exact_identity": exact,
                    "semantic_program_hash": generated.program.semantic_program_hash,
                    "generator_summary": generated.summary(),
                }
            )[:24]
            matched = construct_matched_control_program_v1(generated.program)
            row = {
                "schema_version": STATE_JUMP_ASK_SCHEMA,
                "optimizer_arm": self.arm,
                "proposal_id": proposal_id,
                "optimizer_ask_identity": proposal_id,
                "trial_number": None,
                "checkpoint_id": str(checkpoint_id),
                "ask_ordinal": int(ordinal),
                "exact_identity": exact,
                "control_exact_identity": matched.control.semantic_program_hash,
                "program_genes": dict(genes),
                "program_gene_hash": stable_hash(dict(genes)),
                "program_space_id": PROGRAM_SPACE_ID,
                "availability": {
                    "mode": "GENERATED_TYPED_PROGRAM_NOT_PREENUMERATED",
                    "semantic_program_hash": generated.program.semantic_program_hash,
                    "base_group_identity": base_group,
                },
                "acquisition": {
                    "source": SEMANTIC_STATE_JUMP_GENERATOR_V2,
                    "generator_summary": generated.summary(),
                    "compiled_program_hash": str(compiled["compiled_program_hash"]),
                    "sealed_feedback_used": False,
                },
                "batch_group_feasibility": (
                    {}
                    if not batch_group_constraint
                    else {
                        "group_identity": base_group,
                        "group_count_before": count_before,
                        "group_count_after": count_before + 1,
                        "distinct_group_count_before": distinct_before,
                        "distinct_group_count_after": sum(
                            int(value) > 0 for value in group_counts.values()
                        ),
                    }
                ),
            }
            asked.append(row)
            self._pending[proposal_id] = copy.deepcopy(row)
            self._pending_generated[proposal_id] = generated
            self._generated_exact_identities.add(exact)
        return asked

    def generated_for_proposal(self, proposal_id: str) -> GeneratedProgramV2:
        try:
            return self._pending_generated[str(proposal_id)]
        except KeyError as exc:
            raise KeyError(f"STATE_JUMP_PENDING_PROPOSAL_UNKNOWN:{proposal_id}") from exc

    def tell(
        self, observations: Sequence[ProgramOptimizerObservationV1]
    ) -> dict[str, Any]:
        by_id = {row.proposal_id: row for row in observations}
        if set(by_id) != set(self._pending):
            raise RuntimeError("PROGRAM_OPTIMIZER_ASK_TELL_COVERAGE_DRIFT")
        admitted = 0
        productive = 0
        operation_counts: dict[str, int] = {}
        for proposal_id, ask in self._pending.items():
            observation = by_id[proposal_id]
            if str(observation.exact_identity) != str(ask["exact_identity"]):
                raise ValueError("STATE_JUMP_OBSERVATION_EXACT_DRIFT")
            generated = self._pending_generated[proposal_id]
            is_admitted = bool(observation.admission.admitted)
            credit = (
                dict(observation.uplift.program_credit)
                if is_admitted and observation.uplift is not None
                else {}
            )
            matched_return = float(
                credit.get("matched_cumulative_net_return_increment") or 0.0
            )
            matched_reward = float(credit.get("matched_net_reward_increment") or 0.0)
            is_productive = is_admitted and matched_return > 0.0 and matched_reward > 0.0
            admitted += int(is_admitted)
            productive += int(is_productive)
            operation_counts[generated.operation] = operation_counts.get(generated.operation, 0) + 1
            self.generator.observe(
                generated,
                admitted=is_admitted,
                productive=is_productive,
                matched_return_increment=matched_return,
                matched_reward_increment=matched_reward,
            )
        receipt = {
            "schema_version": STATE_JUMP_TELL_SCHEMA,
            "optimizer_arm": self.arm,
            "asked_count": len(by_id),
            "admitted_count": admitted,
            "productive_count": productive,
            "operation_counts": dict(sorted(operation_counts.items())),
            "optimizer_feedback_applied": True,
            "feedback_domain": "DEVELOPMENT_ONLY",
            "sealed_feedback_used": False,
            "generator_diagnostics": self.generator.diagnostics(),
        }
        self._history.append(receipt)
        self._pending.clear()
        self._pending_generated.clear()
        return receipt

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            "optimizer_arm": self.arm,
            "algorithm": "SEMANTIC_STATE_JUMP_RECOMPOSITION_WITH_DEVELOPMENT_MEMORY",
            "candidate_space": "NEW_TYPED_PROGRAMS_FROM_AUTHORIZED_COMPONENT_PAIRS",
            "frozen_catalog_required": False,
            "seen_exact_count": len(self.seen_exact_identities),
            "gene_slot_schema_sha256": stable_hash(list(self.ordered_gene_slots)),
            "generator_diagnostics": self.generator.diagnostics(),
            "validation_feedback_allowed": False,
            "holdout_feedback_allowed": False,
            "forward_feedback_allowed": False,
        }

    def snapshot(self) -> dict[str, Any]:
        if self._pending:
            raise RuntimeError("PROGRAM_OPTIMIZER_SNAPSHOT_HAS_PENDING")
        payload = {
            "schema_version": STATE_JUMP_ADAPTER_SCHEMA,
            "optimizer_arm": self.arm,
            "ordered_gene_slots": list(self.ordered_gene_slots),
            "seen_exact_identities": sorted(self.seen_exact_identities),
            "generated_exact_identities": sorted(self._generated_exact_identities),
            "history": copy.deepcopy(self._history),
            "generator": self.generator.snapshot(),
        }
        payload["snapshot_hash"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(
        cls,
        *,
        snapshot: Mapping[str, Any],
        adapter: CandidateProgramProposalAdapterV0,
        compiler: ProgramCompilerV1,
        components_by_role: Mapping[str, Sequence[ProgramSourceComponentV0]],
        seed: int = 0,
    ) -> "StateJumpProgramSearchAdapterV2":
        payload = copy.deepcopy(dict(snapshot))
        body = dict(payload)
        claimed = str(body.pop("snapshot_hash", ""))
        if not claimed or stable_hash(body) != claimed:
            raise ValueError("STATE_JUMP_ADAPTER_SNAPSHOT_SELF_HASH_DRIFT")
        if payload.get("schema_version") != STATE_JUMP_ADAPTER_SCHEMA:
            raise ValueError("STATE_JUMP_ADAPTER_SNAPSHOT_SCHEMA_DRIFT")
        state = cls(
            adapter=adapter,
            compiler=compiler,
            components_by_role=components_by_role,
            ordered_gene_slots=payload["ordered_gene_slots"],
            seen_exact_identities=payload["seen_exact_identities"],
            seed=int(seed),
            generator_snapshot=dict(payload["generator"]),
        )
        state._generated_exact_identities = set(
            map(str, payload["generated_exact_identities"])
        )
        state._history = copy.deepcopy(list(payload["history"]))
        if state.snapshot() != dict(snapshot):
            raise ValueError("STATE_JUMP_ADAPTER_SNAPSHOT_REPLAY_DRIFT")
        return state


__all__ = [
    "STATE_JUMP_ADAPTER_SCHEMA",
    "StateJumpProgramSearchAdapterV2",
]
