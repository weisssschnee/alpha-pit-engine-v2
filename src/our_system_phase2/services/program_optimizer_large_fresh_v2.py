"""Two-arm optimizer state for Large Fresh V2.

This is deliberately separate from ProgramOptimizerTournamentV1 so the old
three-arm tournament snapshot contract remains unchanged.  V2 owns exactly
one non-learning Uniform reserve arm and one Catalog Typed Evolution arm.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
    CatalogTypedEvolutionProgramV2,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    UNIFORM_CONTROL,
    ProgramOptimizerObservationV1,
    UniformProgramSearchAdapter,
)
from our_system_phase2.services.route_local_availability import AvailabilityEntry
from our_system_phase2.services.unified_capability_registry import stable_hash

STATE_SCHEMA = "cn_program_optimizer_large_fresh_bandit_v2"
ARMS = (UNIFORM_CONTROL, CATALOG_TYPED_EVOLUTION_PROGRAM_V2)


class LargeFreshProgramBanditV2:
    """Replayable two-arm state used only by Large Fresh V2."""

    def __init__(
        self,
        *,
        campaign_id: str,
        entries_by_arm: Mapping[str, Sequence[AvailabilityEntry]],
        seeds: Mapping[str, int],
        evolution_config: Mapping[str, Any],
    ) -> None:
        if set(entries_by_arm) != set(ARMS) or set(seeds) != set(ARMS):
            raise ValueError("LARGE_FRESH_V2_ARM_COVERAGE_DRIFT")
        self.campaign_id = str(campaign_id)
        self.entries_by_arm = {arm: tuple(entries_by_arm[arm]) for arm in ARMS}
        hashes = {
            stable_hash([entry.to_dict() for entry in rows])
            for rows in self.entries_by_arm.values()
        }
        if len(hashes) != 1:
            raise ValueError("LARGE_FRESH_V2_COMMON_SPACE_DRIFT")
        self.program_space_hash = next(iter(hashes))
        self.seeds = {arm: int(seeds[arm]) for arm in ARMS}
        self.evolution_config = dict(evolution_config)
        self.adapters = {
            UNIFORM_CONTROL: UniformProgramSearchAdapter(
                entries=self.entries_by_arm[UNIFORM_CONTROL],
                seen_exact_identities=(),
                seed=self.seeds[UNIFORM_CONTROL],
            ),
            CATALOG_TYPED_EVOLUTION_PROGRAM_V2: CatalogTypedEvolutionProgramV2(
                entries=self.entries_by_arm[CATALOG_TYPED_EVOLUTION_PROGRAM_V2],
                seen_exact_identities=(),
                seed=self.seeds[CATALOG_TYPED_EVOLUTION_PROGRAM_V2],
                **self.evolution_config,
            ),
        }

    def _ask_live(
        self,
        *,
        arm: str,
        checkpoint_id: str,
        count: int,
        required_program_template_id: str,
        eligible_exact_identities: Sequence[str],
        batch_group_constraint: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        adapter = self.adapters.get(str(arm))
        if adapter is None:
            raise ValueError(f"LARGE_FRESH_V2_ARM_UNKNOWN:{arm}")
        return adapter.ask(
            checkpoint_id=str(checkpoint_id),
            count=int(count),
            required_program_template_id=str(required_program_template_id),
            eligible_exact_identities=tuple(map(str, eligible_exact_identities)),
            batch_group_constraint=batch_group_constraint,
        )

    def ask(self, **kwargs: Any) -> list[dict[str, Any]]:
        preview = type(self).restore(
            self.snapshot(),
            entries_by_arm=self.entries_by_arm,
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
            raise RuntimeError("LARGE_FRESH_V2_PREVIEW_COMMIT_DRIFT")

    def tell(
        self,
        *,
        arm: str,
        observations: Sequence[ProgramOptimizerObservationV1],
    ) -> dict[str, Any]:
        adapter = self.adapters.get(str(arm))
        if adapter is None:
            raise ValueError(f"LARGE_FRESH_V2_ARM_UNKNOWN:{arm}")
        return adapter.tell(observations)

    def discard_nonlearning_pending(
        self, *, arm: str, expected_asks: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any]:
        adapter = self.adapters.get(str(arm))
        if adapter is None:
            raise ValueError(f"LARGE_FRESH_V2_ARM_UNKNOWN:{arm}")
        if list(adapter._pending.values()) != [dict(row) for row in expected_asks]:
            raise RuntimeError("LARGE_FRESH_V2_PENDING_DRIFT")
        adapter._pending.clear()
        return {
            "schema_version": "cn_program_large_fresh_v2_nonlearning_discard_v1",
            "optimizer_arm": str(arm),
            "discarded_count": len(expected_asks),
            "optimizer_feedback_applied": False,
        }

    @property
    def observations(self) -> int:
        return sum(adapter.observation_count for adapter in self.adapters.values())

    def snapshot(self) -> dict[str, Any]:
        payload = {
            "schema_version": STATE_SCHEMA,
            "campaign_id": self.campaign_id,
            "program_space_hash": self.program_space_hash,
            "seeds": dict(self.seeds),
            "evolution_config": dict(self.evolution_config),
            "arms": {arm: self.adapters[arm].snapshot() for arm in ARMS},
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
    ) -> "LargeFreshProgramBanditV2":
        payload = dict(snapshot)
        claimed = str(payload.pop("bandit_state_sha256", ""))
        if not claimed or stable_hash(payload) != claimed:
            raise ValueError("LARGE_FRESH_V2_STATE_SELF_HASH_DRIFT")
        if payload.get("schema_version") != STATE_SCHEMA:
            raise ValueError("LARGE_FRESH_V2_STATE_SCHEMA_DRIFT")
        if expected_campaign_id is not None and str(payload["campaign_id"]) != str(
            expected_campaign_id
        ):
            raise ValueError("LARGE_FRESH_V2_CAMPAIGN_DRIFT")
        state = cls(
            campaign_id=str(payload["campaign_id"]),
            entries_by_arm=entries_by_arm,
            seeds=dict(payload["seeds"]),
            evolution_config=dict(payload["evolution_config"]),
        )
        if state.program_space_hash != str(payload["program_space_hash"]):
            raise ValueError("LARGE_FRESH_V2_COMMON_SPACE_DRIFT")
        state.adapters = {
            UNIFORM_CONTROL: UniformProgramSearchAdapter.restore(
                snapshot=dict(payload["arms"][UNIFORM_CONTROL]),
                entries=state.entries_by_arm[UNIFORM_CONTROL],
                seen_exact_identities=(),
                seed=state.seeds[UNIFORM_CONTROL],
            ),
            CATALOG_TYPED_EVOLUTION_PROGRAM_V2: CatalogTypedEvolutionProgramV2.restore(
                snapshot=dict(payload["arms"][CATALOG_TYPED_EVOLUTION_PROGRAM_V2]),
                entries=state.entries_by_arm[CATALOG_TYPED_EVOLUTION_PROGRAM_V2],
                seen_exact_identities=(),
                seed=state.seeds[CATALOG_TYPED_EVOLUTION_PROGRAM_V2],
                **state.evolution_config,
            ),
        }
        if state.snapshot() != dict(snapshot):
            raise ValueError("LARGE_FRESH_V2_STATE_REPLAY_DRIFT")
        return state

    def optimizer_metadata(self) -> dict[str, Any]:
        return {
            arm: self.adapters[arm].optimizer_metadata()
            for arm in ARMS
        }
