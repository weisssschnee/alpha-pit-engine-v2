"""Common orchestration seam for the prospective Program optimizer tournament.

This is not another optimizer.  It owns one instance of each implementation of
``ProgramSearchOptimizerAdapter`` and presents the checkpoint state shape that
the accepted Joint Program execution engine already persists and replays.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    STRUCTURED_SURROGATE_PROGRAM,
    UNIFORM_CONTROL,
    HybridTPEProgramSearchAdapter,
    ProgramOptimizerObservationV1,
    StructuredSurrogateProgramSearchAdapter,
    UniformProgramSearchAdapter,
)
from our_system_phase2.services.route_local_availability import AvailabilityEntry
from our_system_phase2.services.unified_capability_registry import stable_hash


TOURNAMENT_STATE_SCHEMA = "cn_program_optimizer_tournament_state_v1"
ARM_TYPES = {
    UNIFORM_CONTROL: UniformProgramSearchAdapter,
    HYBRID_TPE_PROGRAM: HybridTPEProgramSearchAdapter,
    STRUCTURED_SURROGATE_PROGRAM: StructuredSurrogateProgramSearchAdapter,
}


class ProgramOptimizerTournamentV1:
    """Three shared-boundary Program optimizers with immutable state replay."""

    def __init__(
        self,
        *,
        campaign_id: str,
        entries_by_arm: Mapping[str, Sequence[AvailabilityEntry]],
        seeds: Mapping[str, int],
        tpe_config: Mapping[str, Any],
        surrogate_config: Mapping[str, Any],
    ) -> None:
        if set(entries_by_arm) != set(ARM_TYPES) or set(seeds) != set(ARM_TYPES):
            raise ValueError("PROGRAM_TOURNAMENT_ARM_COVERAGE_DRIFT")
        self.campaign_id = str(campaign_id)
        self.entries_by_arm = {
            arm: tuple(entries_by_arm[arm]) for arm in ARM_TYPES
        }
        space_hashes = {
            stable_hash([entry.to_dict() for entry in entries])
            for entries in self.entries_by_arm.values()
        }
        if len(space_hashes) != 1:
            raise ValueError("PROGRAM_TOURNAMENT_COMMON_SPACE_DRIFT")
        self.program_space_hash = next(iter(space_hashes))
        self.seeds = {arm: int(seeds[arm]) for arm in ARM_TYPES}
        self.tpe_config = dict(tpe_config)
        self.surrogate_config = dict(surrogate_config)
        common = {
            arm: {
                "entries": self.entries_by_arm[arm],
                "seen_exact_identities": (),
                "seed": self.seeds[arm],
            }
            for arm in ARM_TYPES
        }
        self.adapters = {
            UNIFORM_CONTROL: UniformProgramSearchAdapter(
                **common[UNIFORM_CONTROL]
            ),
            HYBRID_TPE_PROGRAM: HybridTPEProgramSearchAdapter(
                **common[HYBRID_TPE_PROGRAM], **self.tpe_config
            ),
            STRUCTURED_SURROGATE_PROGRAM: StructuredSurrogateProgramSearchAdapter(
                **common[STRUCTURED_SURROGATE_PROGRAM], **self.surrogate_config
            ),
        }

    @classmethod
    def fresh(
        cls,
        *,
        campaign_id: str,
        entries_by_arm: Mapping[str, Sequence[AvailabilityEntry]],
        seeds: Mapping[str, int],
        tpe_config: Mapping[str, Any],
        surrogate_config: Mapping[str, Any],
    ) -> "ProgramOptimizerTournamentV1":
        return cls(
            campaign_id=campaign_id,
            entries_by_arm=entries_by_arm,
            seeds=seeds,
            tpe_config=tpe_config,
            surrogate_config=surrogate_config,
        )

    def ask(
        self,
        *,
        arm: str,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str,
        eligible_exact_identities: Sequence[str],
    ) -> list[dict[str, Any]]:
        preview = type(self).restore(
            self.snapshot(),
            entries_by_arm=self.entries_by_arm,
            expected_campaign_id=self.campaign_id,
        )
        return preview._ask_live(
            arm=arm,
            checkpoint_id=checkpoint_id,
            count=count,
            required_program_template_id=required_program_template_id,
            eligible_exact_identities=eligible_exact_identities,
        )

    def _ask_live(
        self,
        *,
        arm: str,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str,
        eligible_exact_identities: Sequence[str],
    ) -> list[dict[str, Any]]:
        adapter = self.adapters.get(str(arm))
        if adapter is None:
            raise ValueError(f"PROGRAM_TOURNAMENT_ARM_UNKNOWN:{arm}")
        return adapter.ask(
            checkpoint_id=checkpoint_id,
            count=int(count),
            required_program_template_id=required_program_template_id,
            eligible_exact_identities=eligible_exact_identities,
        )

    def commit_ask(
        self,
        *,
        arm: str,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str,
        eligible_exact_identities: Sequence[str],
        expected_asks: Sequence[Mapping[str, Any]],
    ) -> None:
        committed = self._ask_live(
            arm=arm,
            checkpoint_id=checkpoint_id,
            count=count,
            required_program_template_id=required_program_template_id,
            eligible_exact_identities=eligible_exact_identities,
        )
        if committed != [dict(row) for row in expected_asks]:
            raise RuntimeError("PROGRAM_TOURNAMENT_PREVIEW_COMMIT_DRIFT")

    def tell(
        self,
        *,
        arm: str,
        observations: Sequence[ProgramOptimizerObservationV1],
    ) -> dict[str, Any]:
        adapter = self.adapters.get(str(arm))
        if adapter is None:
            raise ValueError(f"PROGRAM_TOURNAMENT_ARM_UNKNOWN:{arm}")
        return adapter.tell(observations)

    def discard_nonlearning_pending(
        self, *, arm: str, expected_asks: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any]:
        adapter = self.adapters.get(str(arm))
        if adapter is None:
            raise ValueError(f"PROGRAM_TOURNAMENT_ARM_UNKNOWN:{arm}")
        if list(adapter._pending.values()) != [dict(row) for row in expected_asks]:
            raise RuntimeError("PROGRAM_TOURNAMENT_BASE_PENDING_DRIFT")
        adapter._pending.clear()
        return {
            "schema_version": "cn_program_tournament_nonlearning_discard_v1",
            "optimizer_arm": str(arm),
            "discarded_count": len(expected_asks),
            "reason": "BASE_PARITY_NOT_PROGRAM_OPTIMIZER_ELIGIBLE",
            "optimizer_feedback_applied": False,
        }

    @property
    def observations(self) -> int:
        return sum(adapter.observation_count for adapter in self.adapters.values())

    def snapshot(self) -> dict[str, Any]:
        payload = {
            "schema_version": TOURNAMENT_STATE_SCHEMA,
            "campaign_id": self.campaign_id,
            "program_space_hash": self.program_space_hash,
            "seeds": dict(self.seeds),
            "tpe_config": dict(self.tpe_config),
            "surrogate_config": dict(self.surrogate_config),
            "arms": {
                arm: adapter.snapshot()
                for arm, adapter in self.adapters.items()
            },
            "observations": self.observations,
            "development_financial_observations_imported": False,
            "serialized_optimizer_state_imported": False,
            "candidate_results_imported": False,
        }
        payload["bandit_state_sha256"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(
        cls,
        snapshot: Mapping[str, Any],
        *,
        entries_by_arm: Mapping[str, Sequence[AvailabilityEntry]],
        expected_campaign_id: str | None = None,
    ) -> "ProgramOptimizerTournamentV1":
        payload = dict(snapshot)
        expected = str(payload.pop("bandit_state_sha256", ""))
        if not expected or stable_hash(payload) != expected:
            raise ValueError("PROGRAM_TOURNAMENT_STATE_SELF_HASH_DRIFT")
        if payload.get("schema_version") != TOURNAMENT_STATE_SCHEMA:
            raise ValueError("PROGRAM_TOURNAMENT_STATE_SCHEMA_DRIFT")
        if expected_campaign_id is not None and str(payload["campaign_id"]) != str(
            expected_campaign_id
        ):
            raise ValueError("PROGRAM_TOURNAMENT_CAMPAIGN_IDENTITY_DRIFT")
        tournament = cls(
            campaign_id=str(payload["campaign_id"]),
            entries_by_arm=entries_by_arm,
            seeds=dict(payload["seeds"]),
            tpe_config=dict(payload["tpe_config"]),
            surrogate_config=dict(payload["surrogate_config"]),
        )
        if tournament.program_space_hash != str(payload["program_space_hash"]):
            raise ValueError("PROGRAM_TOURNAMENT_COMMON_SPACE_DRIFT")
        tournament.adapters = {
            arm: ARM_TYPES[arm].restore(
                snapshot=dict(payload["arms"][arm]),
                entries=tournament.entries_by_arm[arm],
                seen_exact_identities=(),
                seed=tournament.seeds[arm],
                **(
                    tournament.tpe_config
                    if arm == HYBRID_TPE_PROGRAM
                    else tournament.surrogate_config
                    if arm == STRUCTURED_SURROGATE_PROGRAM
                    else {}
                ),
            )
            for arm in ARM_TYPES
        }
        if tournament.snapshot() != dict(snapshot):
            raise ValueError("PROGRAM_TOURNAMENT_STATE_REPLAY_DRIFT")
        return tournament

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            arm: adapter.optimizer_metadata()
            for arm, adapter in self.adapters.items()
        }
