from __future__ import annotations

import json
import math
from pathlib import Path

from scripts.build_cn_program_stage_c_system_search_prefreeze_v1 import primitive_score
from our_system_phase2.services.program_search_optimizer_v1 import (
    ProgramOptimizerObservationV1,
    program_availability_entries_v1,
)
from our_system_phase2.services.program_search_primitive_credit_v1 import (
    PRIMITIVE_FEEDBACK_MODE,
    HierarchicalPrimitiveCreditScorerV1,
    PrimitiveLocalProgramSearchAdapterV1,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import ProgramUpliftCredit


REPO = Path(__file__).resolve().parents[1]
STATS = REPO / "runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json"
STAGE_D = REPO / "runtime/run_plans/cn_program_stage_d_primitive_confirmation_prefreeze_v1.json"


def _read(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _metadata(candidate: dict, entry) -> dict:
    components = {}
    for role, raw in sorted(dict(candidate["components"]).items()):
        binding = dict(raw)
        components[role] = {
            "component_id": str(binding["component_id"]),
            "route_id": str(binding["route_id"]),
            "skeleton_id": str(entry.genes[f"{role}__skeleton_id"]),
        }
    return {
        "template_id": str(candidate["template_id"]),
        "components": components,
        "tie_break_identity": str(candidate["exact_identity"]),
    }


def _positive_observation(ask: dict) -> ProgramOptimizerObservationV1:
    exact = str(ask["exact_identity"])
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=f"record::{exact}",
        pair_id=f"pair::{exact}",
        program_id=f"program::{exact}",
        control_program_id=f"control::{exact}",
        admitted=True,
        failure_reasons=(),
        metrics={},
    )
    uplift = ProgramUpliftCredit(
        record_payload_sha256=f"record::{exact}",
        pair_id=f"pair::{exact}",
        program_id=f"program::{exact}",
        control_program_id=f"control::{exact}",
        program_credit={
            "matched_cumulative_net_return_increment": 1.0,
            "matched_net_reward_increment": 1.0,
        },
    )
    return ProgramOptimizerObservationV1(
        proposal_id=str(ask["proposal_id"]),
        exact_identity=exact,
        admission=admission,
        uplift=uplift,
    )


def test_production_scorer_is_exactly_stage_d_scorer() -> None:
    stats = _read(STATS)
    stage_d = _read(STAGE_D)
    candidates = list(stage_d["cohort"]["candidates"])
    entries = program_availability_entries_v1(
        [{"genes": dict(row["program_genes"])} for row in candidates]
    )
    scorer = HierarchicalPrimitiveCreditScorerV1(
        primitive_stats=stats,
        primitive_stats_payload_sha256=stats["stats_payload_sha256"],
    )
    for candidate, entry in zip(candidates, entries, strict=True):
        old_score, old_novelty, _ = primitive_score(candidate, stats)
        observed = scorer.score(_metadata(candidate, entry))
        assert math.isclose(observed["primitive_score"], old_score, rel_tol=0.0, abs_tol=1e-15)
        assert math.isclose(observed["novelty_score"], old_novelty, rel_tol=0.0, abs_tol=1e-15)


def test_adapter_replays_all_stage_d_template_orders_exactly() -> None:
    stats = _read(STATS)
    stage_d = _read(STAGE_D)
    candidates = list(stage_d["cohort"]["candidates"])
    entries = program_availability_entries_v1(
        [{"genes": dict(row["program_genes"])} for row in candidates]
    )
    physical_by_normalized = {
        entry.exact_identity: str(candidate["exact_identity"])
        for candidate, entry in zip(candidates, entries, strict=True)
    }
    metadata = {
        entry.exact_identity: _metadata(candidate, entry)
        for candidate, entry in zip(candidates, entries, strict=True)
    }
    expected = dict(stage_d["policy"]["primary_policy_order_by_template"])
    for template_id, expected_order in expected.items():
        template_entries = tuple(
            entry
            for entry in entries
            if str(entry.genes["program_template_id"]) == str(template_id)
        )
        template_metadata = {
            entry.exact_identity: metadata[entry.exact_identity]
            for entry in template_entries
        }
        adapter = PrimitiveLocalProgramSearchAdapterV1(
            entries=template_entries,
            seen_exact_identities=(),
            seed=991,
            metadata_by_exact_identity=template_metadata,
            primitive_stats=stats,
            primitive_stats_payload_sha256=stats["stats_payload_sha256"],
        )
        asks = adapter.ask(
            checkpoint_id=f"REPLAY::{template_id}",
            count=len(template_entries),
            required_program_template_id=template_id,
        )
        observed = [physical_by_normalized[str(row["exact_identity"])] for row in asks]
        assert observed == list(map(str, expected_order))


def test_adapter_tell_is_audit_only_and_snapshot_restore_is_exact() -> None:
    stats = _read(STATS)
    stage_d = _read(STAGE_D)
    template_id = "BASE_EVENT"
    candidates = [
        row
        for row in stage_d["cohort"]["candidates"]
        if str(row["template_id"]) == template_id
    ]
    entries = program_availability_entries_v1(
        [{"genes": dict(row["program_genes"])} for row in candidates]
    )
    metadata = {
        entry.exact_identity: _metadata(candidate, entry)
        for candidate, entry in zip(candidates, entries, strict=True)
    }
    kwargs = dict(
        entries=entries,
        seen_exact_identities=(),
        seed=77,
        metadata_by_exact_identity=metadata,
        primitive_stats=stats,
        primitive_stats_payload_sha256=stats["stats_payload_sha256"],
    )
    adapter = PrimitiveLocalProgramSearchAdapterV1(**kwargs)
    first = adapter.ask(
        checkpoint_id="C1", count=24, required_program_template_id=template_id
    )
    before_scores = dict(adapter.score_by_exact)
    receipt = adapter.tell([_positive_observation(row) for row in first])
    assert receipt["optimizer_feedback_applied"] is False
    assert receipt["online_feedback_mode"] == PRIMITIVE_FEEDBACK_MODE
    assert receipt["prior_stats_mutated"] is False
    assert receipt["productive_count"] == 24
    assert adapter.score_by_exact == before_scores
    snapshot = adapter.snapshot()
    restored = PrimitiveLocalProgramSearchAdapterV1.restore(snapshot=snapshot, **kwargs)
    assert restored.snapshot() == snapshot
    next_left = adapter.ask(
        checkpoint_id="C2", count=24, required_program_template_id=template_id
    )
    next_right = restored.ask(
        checkpoint_id="C2", count=24, required_program_template_id=template_id
    )
    assert next_left == next_right
