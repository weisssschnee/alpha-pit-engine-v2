"""Unified three-arm state for the CN Search Core V2 tournament."""
from __future__ import annotations

import copy
from typing import Any, Mapping, Sequence

from our_system_phase2.services.candidate_program_proposal_v0 import (
    CandidateProgramProposalAdapterV0,
    ProgramSourceComponentV0,
)
from our_system_phase2.services.candidate_program_v1 import ProgramCompilerV1
from our_system_phase2.services.program_search_optimizer_v1 import (
    ProgramOptimizerObservationV1,
)
from our_system_phase2.services.program_search_primitive_credit_v1 import (
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
    PrimitiveLocalProgramSearchAdapterV1,
)
from our_system_phase2.services.program_search_semantic_mcts_v1 import (
    SEMANTIC_MCTS_PROGRAM_V1,
    SemanticMCTSProgramSearchAdapterV1,
)
from our_system_phase2.services.program_search_state_jump_adapter_v2 import (
    StateJumpProgramSearchAdapterV2,
)
from our_system_phase2.services.program_search_state_jump_generator_v2 import (
    SEMANTIC_STATE_JUMP_GENERATOR_V2,
)
from our_system_phase2.services.route_local_availability import AvailabilityEntry
from our_system_phase2.services.unified_capability_registry import stable_hash


SEARCH_CORE_V2_STATE_SCHEMA = "cn_program_search_core_v2_tournament_state_v1"
SEARCH_CORE_V2_ARMS = (
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
    SEMANTIC_STATE_JUMP_GENERATOR_V2,
    SEMANTIC_MCTS_PROGRAM_V1,
)


class SearchCoreV2TournamentState:
    """Replayable shared interface for baseline, new generator, and MCTS challenger."""

    def __init__(
        self,
        *,
        campaign_id: str,
        entries: Sequence[AvailabilityEntry],
        all_frozen_exact_identities: Sequence[str],
        primitive_config: Mapping[str, Any],
        composer: CandidateProgramProposalAdapterV0,
        compiler: ProgramCompilerV1,
        components_by_role: Mapping[str, Sequence[ProgramSourceComponentV0]],
        ordered_gene_slots: Sequence[str],
        seeds: Mapping[str, int],
        state_jump_config: Mapping[str, Any] | None = None,
        mcts_config: Mapping[str, Any] | None = None,
    ) -> None:
        if set(seeds) != set(SEARCH_CORE_V2_ARMS):
            raise ValueError("SEARCH_CORE_V2_SEED_COVERAGE_DRIFT")
        self.campaign_id = str(campaign_id)
        self.entries = tuple(entries)
        self.all_frozen_exact_identities = tuple(sorted(map(str, all_frozen_exact_identities)))
        self.primitive_config = copy.deepcopy(dict(primitive_config))
        self.composer = composer
        self.compiler = compiler
        self.components_by_role = {
            str(role): tuple(components)
            for role, components in components_by_role.items()
        }
        self.ordered_gene_slots = tuple(map(str, ordered_gene_slots))
        self.seeds = {str(arm): int(seed) for arm, seed in seeds.items()}
        self.state_jump_config = dict(state_jump_config or {})
        self.mcts_config = dict(mcts_config or {})
        self.program_space_hash = stable_hash(
            [entry.to_dict() for entry in sorted(self.entries, key=lambda row: row.exact_identity)]
        )
        component_pool = {
            role: [component.component_id for component in components]
            for role, components in sorted(self.components_by_role.items())
        }
        self.component_pool_hash = stable_hash(component_pool)
        metadata = dict(self.primitive_config["metadata_by_exact_identity"])
        self.adapters = {
            PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1: PrimitiveLocalProgramSearchAdapterV1(
                entries=self.entries,
                seen_exact_identities=(),
                seed=self.seeds[PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1],
                **self.primitive_config,
            ),
            SEMANTIC_STATE_JUMP_GENERATOR_V2: StateJumpProgramSearchAdapterV2(
                adapter=self.composer,
                compiler=self.compiler,
                components_by_role=self.components_by_role,
                ordered_gene_slots=self.ordered_gene_slots,
                seen_exact_identities=self.all_frozen_exact_identities,
                seed=self.seeds[SEMANTIC_STATE_JUMP_GENERATOR_V2],
                **self.state_jump_config,
            ),
            SEMANTIC_MCTS_PROGRAM_V1: SemanticMCTSProgramSearchAdapterV1(
                entries=self.entries,
                seen_exact_identities=(),
                seed=self.seeds[SEMANTIC_MCTS_PROGRAM_V1],
                metadata_by_exact_identity=metadata,
                primitive_stats=self.primitive_config["primitive_stats"],
                primitive_stats_payload_sha256=self.primitive_config[
                    "primitive_stats_payload_sha256"
                ],
                **self.mcts_config,
            ),
        }

    @property
    def observations(self) -> int:
        return sum(adapter.observation_count for adapter in self.adapters.values())

    def _ask_live(
        self,
        *,
        arm: str,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str,
        eligible_exact_identities: Sequence[str] | None = None,
        batch_group_constraint: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        selected = self.adapters.get(str(arm))
        if selected is None:
            raise ValueError(f"SEARCH_CORE_V2_ARM_UNKNOWN:{arm}")
        if str(arm) == SEMANTIC_STATE_JUMP_GENERATOR_V2:
            eligible_exact_identities = None
        return selected.ask(
            checkpoint_id=str(checkpoint_id),
            count=int(count),
            required_program_template_id=str(required_program_template_id),
            eligible_exact_identities=eligible_exact_identities,
            batch_group_constraint=batch_group_constraint,
        )

    def ask(self, **kwargs: Any) -> list[dict[str, Any]]:
        preview = type(self).restore(
            snapshot=self.snapshot(),
            entries=self.entries,
            all_frozen_exact_identities=self.all_frozen_exact_identities,
            primitive_config=self.primitive_config,
            composer=self.composer,
            compiler=self.compiler,
            components_by_role=self.components_by_role,
            ordered_gene_slots=self.ordered_gene_slots,
            expected_campaign_id=self.campaign_id,
        )
        return preview._ask_live(**kwargs)

    def commit_ask(
        self,
        *,
        expected_asks: Sequence[Mapping[str, Any]],
        **kwargs: Any,
    ) -> None:
        committed = self._ask_live(**kwargs)
        if committed != [dict(row) for row in expected_asks]:
            raise RuntimeError("SEARCH_CORE_V2_PREVIEW_COMMIT_DRIFT")

    def tell(
        self,
        *,
        arm: str,
        observations: Sequence[ProgramOptimizerObservationV1],
    ) -> dict[str, Any]:
        selected = self.adapters.get(str(arm))
        if selected is None:
            raise ValueError(f"SEARCH_CORE_V2_ARM_UNKNOWN:{arm}")
        return selected.tell(observations)

    def generated_for_proposal(self, proposal_id: str):
        adapter = self.adapters[SEMANTIC_STATE_JUMP_GENERATOR_V2]
        return adapter.generated_for_proposal(proposal_id)

    def snapshot(self) -> dict[str, Any]:
        payload = {
            "schema_version": SEARCH_CORE_V2_STATE_SCHEMA,
            "campaign_id": self.campaign_id,
            "program_space_hash": self.program_space_hash,
            "component_pool_hash": self.component_pool_hash,
            "all_frozen_exact_identities_sha256": stable_hash(
                list(self.all_frozen_exact_identities)
            ),
            "ordered_gene_slots": list(self.ordered_gene_slots),
            "seeds": dict(self.seeds),
            "state_jump_config": copy.deepcopy(self.state_jump_config),
            "mcts_config": copy.deepcopy(self.mcts_config),
            "arms": {
                arm: self.adapters[arm].snapshot() for arm in SEARCH_CORE_V2_ARMS
            },
            "observations": self.observations,
            "validation_feedback_allowed": False,
            "holdout_feedback_allowed": False,
            "forward_feedback_allowed": False,
        }
        payload["state_sha256"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(
        cls,
        *,
        snapshot: Mapping[str, Any],
        entries: Sequence[AvailabilityEntry],
        all_frozen_exact_identities: Sequence[str],
        primitive_config: Mapping[str, Any],
        composer: CandidateProgramProposalAdapterV0,
        compiler: ProgramCompilerV1,
        components_by_role: Mapping[str, Sequence[ProgramSourceComponentV0]],
        ordered_gene_slots: Sequence[str],
        expected_campaign_id: str | None = None,
    ) -> "SearchCoreV2TournamentState":
        payload = copy.deepcopy(dict(snapshot))
        body = dict(payload)
        claimed = str(body.pop("state_sha256", ""))
        if not claimed or stable_hash(body) != claimed:
            raise ValueError("SEARCH_CORE_V2_STATE_SELF_HASH_DRIFT")
        if payload.get("schema_version") != SEARCH_CORE_V2_STATE_SCHEMA or any(
            bool(payload.get(key))
            for key in (
                "validation_feedback_allowed",
                "holdout_feedback_allowed",
                "forward_feedback_allowed",
            )
        ):
            raise ValueError("SEARCH_CORE_V2_STATE_AUTHORITY_DRIFT")
        if expected_campaign_id is not None and str(payload["campaign_id"]) != str(
            expected_campaign_id
        ):
            raise ValueError("SEARCH_CORE_V2_CAMPAIGN_DRIFT")
        state = cls(
            campaign_id=str(payload["campaign_id"]),
            entries=entries,
            all_frozen_exact_identities=all_frozen_exact_identities,
            primitive_config=primitive_config,
            composer=composer,
            compiler=compiler,
            components_by_role=components_by_role,
            ordered_gene_slots=ordered_gene_slots,
            seeds=dict(payload["seeds"]),
            state_jump_config=dict(payload["state_jump_config"]),
            mcts_config=dict(payload["mcts_config"]),
        )
        if state.program_space_hash != str(payload["program_space_hash"]):
            raise ValueError("SEARCH_CORE_V2_PROGRAM_SPACE_DRIFT")
        if state.component_pool_hash != str(payload["component_pool_hash"]):
            raise ValueError("SEARCH_CORE_V2_COMPONENT_POOL_DRIFT")
        if stable_hash(list(state.all_frozen_exact_identities)) != str(
            payload["all_frozen_exact_identities_sha256"]
        ):
            raise ValueError("SEARCH_CORE_V2_FROZEN_EXACT_DRIFT")
        state.adapters = {
            PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1: PrimitiveLocalProgramSearchAdapterV1.restore(
                snapshot=dict(payload["arms"][PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1]),
                entries=state.entries,
                seen_exact_identities=(),
                seed=state.seeds[PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1],
                **state.primitive_config,
            ),
            SEMANTIC_STATE_JUMP_GENERATOR_V2: StateJumpProgramSearchAdapterV2.restore(
                snapshot=dict(payload["arms"][SEMANTIC_STATE_JUMP_GENERATOR_V2]),
                adapter=state.composer,
                compiler=state.compiler,
                components_by_role=state.components_by_role,
            ),
            SEMANTIC_MCTS_PROGRAM_V1: SemanticMCTSProgramSearchAdapterV1.restore(
                snapshot=dict(payload["arms"][SEMANTIC_MCTS_PROGRAM_V1]),
                entries=state.entries,
                seen_exact_identities=(),
                seed=state.seeds[SEMANTIC_MCTS_PROGRAM_V1],
                metadata_by_exact_identity=state.primitive_config[
                    "metadata_by_exact_identity"
                ],
                primitive_stats=state.primitive_config["primitive_stats"],
                primitive_stats_payload_sha256=state.primitive_config[
                    "primitive_stats_payload_sha256"
                ],
                **state.mcts_config,
            ),
        }
        if state.snapshot() != dict(snapshot):
            raise ValueError("SEARCH_CORE_V2_STATE_REPLAY_DRIFT")
        return state

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            arm: self.adapters[arm].optimizer_metadata()
            for arm in SEARCH_CORE_V2_ARMS
        }


__all__ = [
    "SEARCH_CORE_V2_ARMS",
    "SEARCH_CORE_V2_STATE_SCHEMA",
    "SearchCoreV2TournamentState",
]
