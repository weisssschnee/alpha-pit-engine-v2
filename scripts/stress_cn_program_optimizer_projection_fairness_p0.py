"""Zero-financial stress for Program optimizer projection fairness P0."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from our_system_phase2.runtime.cn_program_optimizer_tournament_v1 import (
    FROZEN_PROGRAM_SPACE_ENTRY_COUNT,
    FROZEN_PROGRAM_SPACE_SHA256,
    FROZEN_PROGRAM_SPACE_SOURCE_SHA256,
    SURROGATE_CONFIG,
    TPE_CONFIG,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    PROGRAM_ROUTE_ID,
    STRUCTURED_SURROGATE_PROGRAM,
    UNIFORM_CONTROL,
    HybridTPEProgramSearchAdapter,
    ProgramOptimizerObservationV1,
    StructuredSurrogateProgramSearchAdapter,
    UniformProgramSearchAdapter,
    program_optimizer_lane_v1,
)
from our_system_phase2.services.program_tournament_freeze_v1 import (
    _program_entries,
)
from our_system_phase2.services.search_v2_admission import (
    AbsoluteEconomicAdmission,
)
from our_system_phase2.services.search_v2_conditional_uplift import (
    ProgramUpliftCredit,
)
from our_system_phase2.services.unified_capability_registry import stable_hash
from scripts import run_cn_joint_program_phase_c_v0 as phase_c


def _observation(
    ask: Mapping[str, Any], *, sequence: int
) -> ProgramOptimizerObservationV1:
    exact_identity = str(ask["exact_identity"])
    admitted = int(exact_identity[:8], 16) % 4 != 0
    record_hash = stable_hash(
        {
            "synthetic_projection_fairness_stress": True,
            "exact_identity": exact_identity,
            "sequence": int(sequence),
        }
    )
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=record_hash,
        pair_id=f"synthetic-pair-{sequence}",
        program_id=f"synthetic-program-{exact_identity[:16]}",
        control_program_id=f"synthetic-control-{exact_identity[:16]}",
        admitted=admitted,
        failure_reasons=() if admitted else ("SYNTHETIC_ADMISSION_FALSE",),
        metrics={"synthetic_zero_financial": True},
    )
    uplift = (
        ProgramUpliftCredit(
            record_payload_sha256=record_hash,
            pair_id=admission.pair_id,
            program_id=admission.program_id,
            control_program_id=admission.control_program_id,
            program_credit={
                "matched_cumulative_net_return_increment": (
                    (int(exact_identity[8:16], 16) % 2001 - 1000) / 10000.0
                ),
                "matched_net_reward_increment": (
                    (int(exact_identity[16:24], 16) % 2001 - 1000) / 10000.0
                ),
            },
        )
        if admitted
        else None
    )
    return ProgramOptimizerObservationV1(
        proposal_id=str(ask["proposal_id"]),
        exact_identity=exact_identity,
        admission=admission,
        uplift=uplift,
    )


def _tell_synthetic(adapter: Any, asked: Sequence[Mapping[str, Any]], start: int) -> None:
    adapter.tell(
        [
            _observation(ask, sequence=start + ordinal)
            for ordinal, ask in enumerate(asked)
        ]
    )


def _verify_sources(source_root: Path, registry_path: Path) -> None:
    actual = {
        "raw_program_reservoir": phase_c._sha256(
            source_root / "phase_c_raw_program_reservoir.jsonl"
        ),
        "session_executable_component_pool": phase_c._sha256(
            source_root / "phase_c_session_executable_component_pool.jsonl"
        ),
        "unified_capability_registry": phase_c._sha256(registry_path),
    }
    if actual != FROZEN_PROGRAM_SPACE_SOURCE_SHA256:
        raise RuntimeError("PROGRAM_PROJECTION_STRESS_SOURCE_HASH_DRIFT")


def run_stress(
    *, source_root: Path, registry_path: Path, tpe_ask_count: int
) -> dict[str, Any]:
    _verify_sources(source_root, registry_path)
    entries = _program_entries(
        reservoir_path=source_root / "phase_c_raw_program_reservoir.jsonl",
        component_path=(
            source_root / "phase_c_session_executable_component_pool.jsonl"
        ),
        registry_path=registry_path,
    )
    program_space_hash = stable_hash([entry.to_dict() for entry in entries])
    if (
        len(entries) != FROZEN_PROGRAM_SPACE_ENTRY_COUNT
        or program_space_hash != FROZEN_PROGRAM_SPACE_SHA256
    ):
        raise RuntimeError("PROGRAM_PROJECTION_STRESS_FROZEN_SPACE_DRIFT")
    exact_space = {entry.exact_identity for entry in entries}
    templates = sorted(
        {str(entry.genes["program_template_id"]) for entry in entries}
    )
    lanes = program_optimizer_lane_v1(entries)
    lane_exact = {
        exact_identity
        for lane in lanes.values()
        for exact_identity in lane["ordered_categories_by_slot"][
            "program_exact_identity"
        ]
    }
    if lane_exact != exact_space:
        raise RuntimeError("PROGRAM_TPE_LEGAL_LANE_COVERAGE_DRIFT")

    uniform = UniformProgramSearchAdapter(
        entries=entries, seen_exact_identities=(), seed=721003
    )
    hybrid = HybridTPEProgramSearchAdapter(
        entries=entries,
        seen_exact_identities=(),
        seed=721013,
        n_startup_trials=int(TPE_CONFIG["n_startup_trials"]),
        n_ei_candidates=int(TPE_CONFIG["n_ei_candidates"]),
    )
    surrogate = StructuredSurrogateProgramSearchAdapter(
        entries=entries,
        seen_exact_identities=(),
        seed=721019,
        cold_start_asks=int(SURROGATE_CONFIG["cold_start_asks"]),
        candidate_pool_size=int(SURROGATE_CONFIG["candidate_pool_size"]),
        n_estimators=64,
        min_samples_leaf=int(SURROGATE_CONFIG["min_samples_leaf"]),
        exploration_beta=float(SURROGATE_CONFIG["exploration_beta"]),
    )
    shared_hashes = {
        adapter.controller.input_hashes["program_space"]
        for adapter in (uniform, hybrid, surrogate)
    }
    if len(shared_hashes) != 1:
        raise RuntimeError("PROGRAM_OPTIMIZER_ARM_SPACE_DRIFT")

    actual_asks: list[dict[str, Any]] = []
    sequence = 0
    checkpoint = 0
    while len(actual_asks) < int(tpe_ask_count):
        template_id = templates[checkpoint % len(templates)]
        batch_size = min(8, int(tpe_ask_count) - len(actual_asks))
        asked = hybrid.ask(
            checkpoint_id=f"stress_tpe_{checkpoint:04d}",
            count=batch_size,
            required_program_template_id=template_id,
        )
        if len(asked) != batch_size:
            raise RuntimeError("PROGRAM_TPE_STRESS_SUPPLY_EXHAUSTED_EARLY")
        for ask in asked:
            projection = dict(ask["acquisition"]["projection"])
            if (
                str(ask["exact_identity"]) not in exact_space
                or str(projection["raw_exact_identity"]) not in exact_space
                or str(projection["actual_exact_identity"])
                != str(ask["exact_identity"])
                or not bool(projection["intent_preserved"])
                or bool(projection["global_fallback"])
                or str(projection["raw_legality"]) != "LEGAL_FROZEN_EXACT"
                or bool(projection["legality_projection_applied"])
            ):
                raise RuntimeError("PROGRAM_TPE_STRESS_BINDING_DRIFT")
        _tell_synthetic(hybrid, asked, sequence)
        sequence += len(asked)
        actual_asks.extend(dict(row) for row in asked)
        checkpoint += 1

    hybrid_snapshot = hybrid.snapshot()
    restored_hybrid = HybridTPEProgramSearchAdapter.restore(
        snapshot=hybrid_snapshot,
        entries=entries,
        seen_exact_identities=(),
        seed=721013,
        n_startup_trials=int(TPE_CONFIG["n_startup_trials"]),
        n_ei_candidates=int(TPE_CONFIG["n_ei_candidates"]),
    )
    if restored_hybrid.snapshot() != hybrid_snapshot:
        raise RuntimeError("PROGRAM_TPE_STRESS_RESTORE_DRIFT")
    restored_hybrid_peer = HybridTPEProgramSearchAdapter.restore(
        snapshot=hybrid_snapshot,
        entries=entries,
        seen_exact_identities=(),
        seed=721013,
        n_startup_trials=int(TPE_CONFIG["n_startup_trials"]),
        n_ei_candidates=int(TPE_CONFIG["n_ei_candidates"]),
    )
    restore_template = templates[checkpoint % len(templates)]
    restored_next = restored_hybrid.ask(
        checkpoint_id="stress_restore_next", count=8,
        required_program_template_id=restore_template,
    )
    peer_next = restored_hybrid_peer.ask(
        checkpoint_id="stress_restore_next", count=8,
        required_program_template_id=restore_template,
    )
    if restored_next != peer_next:
        raise RuntimeError("PROGRAM_TPE_STRESS_RESTORED_NEXT_ASK_DRIFT")

    depletion_template = templates[0]
    depletion_exact = [
        entry.exact_identity
        for entry in entries
        if str(entry.genes["program_template_id"]) == depletion_template
    ][:16]
    depletion = HybridTPEProgramSearchAdapter(
        entries=entries,
        seen_exact_identities=(),
        seed=721113,
        n_startup_trials=2,
        n_ei_candidates=8,
    )
    depletion_asks: list[dict[str, Any]] = []
    for round_id in range(2):
        asked = depletion.ask(
            checkpoint_id=f"depletion_{round_id}",
            count=8,
            required_program_template_id=depletion_template,
            eligible_exact_identities=depletion_exact,
        )
        _tell_synthetic(depletion, asked, sequence)
        sequence += len(asked)
        depletion_asks.extend(dict(row) for row in asked)
    exhausted = depletion.ask(
        checkpoint_id="depletion_exhausted",
        count=1,
        required_program_template_id=depletion_template,
        eligible_exact_identities=depletion_exact,
    )
    if len(depletion_asks) != 16 or exhausted:
        raise RuntimeError("PROGRAM_TPE_STRESS_DEPLETION_DRIFT")

    surrogate_reverse = StructuredSurrogateProgramSearchAdapter(
        entries=tuple(reversed(entries)),
        seen_exact_identities=(),
        seed=721019,
        cold_start_asks=int(SURROGATE_CONFIG["cold_start_asks"]),
        candidate_pool_size=37,
        n_estimators=64,
        min_samples_leaf=int(SURROGATE_CONFIG["min_samples_leaf"]),
        exploration_beta=float(SURROGATE_CONFIG["exploration_beta"]),
    )
    surrogate_forward = StructuredSurrogateProgramSearchAdapter(
        entries=entries,
        seen_exact_identities=(),
        seed=721019,
        cold_start_asks=int(SURROGATE_CONFIG["cold_start_asks"]),
        candidate_pool_size=37,
        n_estimators=64,
        min_samples_leaf=int(SURROGATE_CONFIG["min_samples_leaf"]),
        exploration_beta=float(SURROGATE_CONFIG["exploration_beta"]),
    )
    for round_id in range(3):
        template_id = templates[round_id % len(templates)]
        forward = surrogate_forward.ask(
            checkpoint_id=f"surrogate_train_{round_id}",
            count=8,
            required_program_template_id=template_id,
        )
        reverse = surrogate_reverse.ask(
            checkpoint_id=f"surrogate_train_{round_id}",
            count=8,
            required_program_template_id=template_id,
        )
        if [row["exact_identity"] for row in forward] != [
            row["exact_identity"] for row in reverse
        ]:
            raise RuntimeError("PROGRAM_SURROGATE_ORDER_PERMUTATION_DRIFT")
        _tell_synthetic(surrogate_forward, forward, sequence)
        _tell_synthetic(surrogate_reverse, reverse, sequence)
        sequence += len(forward)

    surrogate_snapshot = surrogate_forward.snapshot()
    compared_count = 0
    selected_by_template: dict[str, list[str]] = {}
    for template_id in templates:
        probe = StructuredSurrogateProgramSearchAdapter.restore(
            snapshot=surrogate_snapshot,
            entries=entries,
            seen_exact_identities=(),
            seed=721019,
            cold_start_asks=int(SURROGATE_CONFIG["cold_start_asks"]),
            candidate_pool_size=37,
            n_estimators=64,
            min_samples_leaf=int(SURROGATE_CONFIG["min_samples_leaf"]),
            exploration_beta=float(SURROGATE_CONFIG["exploration_beta"]),
        )
        expected_eligible = len(
            probe._remaining_entries(template_id, None)
        )
        asked = probe.ask(
            checkpoint_id=f"surrogate_probe_{template_id}",
            count=8,
            required_program_template_id=template_id,
        )
        if not asked or any(
            int(row["acquisition"]["eligible_compared_count"])
            != expected_eligible
            for row in asked
        ):
            raise RuntimeError("PROGRAM_SURROGATE_FULL_ELIGIBLE_SCORING_DRIFT")
        compared_count += expected_eligible
        selected_by_template[template_id] = [
            str(row["exact_identity"]) for row in asked
        ]

    remaining = surrogate_forward.controller.remaining_entries(
        route_id=PROGRAM_ROUTE_ID
    )
    predictions = surrogate_forward.predict_acquisition(
        [remaining[0].genes, remaining[-1].genes]
    )
    unseen_differentiated = predictions[0] != predictions[1]
    restored_surrogate = StructuredSurrogateProgramSearchAdapter.restore(
        snapshot=surrogate_snapshot,
        entries=entries,
        seen_exact_identities=(),
        seed=721019,
        cold_start_asks=int(SURROGATE_CONFIG["cold_start_asks"]),
        candidate_pool_size=37,
        n_estimators=64,
        min_samples_leaf=int(SURROGATE_CONFIG["min_samples_leaf"]),
        exploration_beta=float(SURROGATE_CONFIG["exploration_beta"]),
    )
    if (
        restored_surrogate.snapshot() != surrogate_snapshot
        or not unseen_differentiated
    ):
        raise RuntimeError("PROGRAM_SURROGATE_STRESS_RESTORE_OR_MODEL_DRIFT")
    restored_surrogate_peer = StructuredSurrogateProgramSearchAdapter.restore(
        snapshot=surrogate_snapshot,
        entries=tuple(reversed(entries)),
        seen_exact_identities=(),
        seed=721019,
        cold_start_asks=int(SURROGATE_CONFIG["cold_start_asks"]),
        candidate_pool_size=37,
        n_estimators=64,
        min_samples_leaf=int(SURROGATE_CONFIG["min_samples_leaf"]),
        exploration_beta=float(SURROGATE_CONFIG["exploration_beta"]),
    )
    restored_surrogate_next = restored_surrogate.ask(
        checkpoint_id="surrogate_restore_next",
        count=8,
        required_program_template_id=templates[0],
    )
    restored_surrogate_peer_next = restored_surrogate_peer.ask(
        checkpoint_id="surrogate_restore_next",
        count=8,
        required_program_template_id=templates[0],
    )
    if restored_surrogate_next != restored_surrogate_peer_next:
        raise RuntimeError("PROGRAM_SURROGATE_STRESS_RESTORED_NEXT_ASK_DRIFT")

    stats = hybrid.projection_statistics()
    depletion_stats = depletion.projection_statistics()
    combined_global_fallbacks = (
        int(stats["global_fallback_count"])
        + int(depletion_stats["global_fallback_count"])
    )
    if combined_global_fallbacks != 0:
        raise RuntimeError("PROGRAM_TPE_GLOBAL_FALLBACK_NONZERO")
    if int(stats["intent_preserved_count"]) != len(actual_asks):
        raise RuntimeError("PROGRAM_TPE_INTENT_PRESERVATION_DRIFT")

    report = {
        "schema_version": "cn_program_optimizer_projection_fairness_p0_stress_v1",
        "status": "PASS",
        "program_space_entry_count": len(entries),
        "program_space_sha256": program_space_hash,
        "legal_tpe_lane_exact_coverage": len(lane_exact),
        "template_count": len(templates),
        "same_exact_universe_all_three_arms": True,
        "tpe_actual_ask_count": len(actual_asks),
        "tpe_projection_statistics": stats,
        "depletion_actual_ask_count": len(depletion_asks),
        "depletion_projection_statistics": depletion_stats,
        "global_fallback_count": combined_global_fallbacks,
        "surrogate_full_eligible_compared_count": compared_count,
        "surrogate_selected_by_template": selected_by_template,
        "surrogate_input_order_permutation_robust": True,
        "surrogate_unseen_programs_differentiated": unseen_differentiated,
        "tpe_snapshot_restore": "PASS",
        "tpe_restored_next_ask_deterministic": True,
        "surrogate_snapshot_restore": "PASS",
        "surrogate_restored_next_ask_deterministic": True,
        "synthetic_admission_and_uplift_only": True,
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
        "project_control_admission": "NOT_REQUESTED",
        "tournament": "NOT_RUN",
    }
    report["report_sha256"] = stable_hash(report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--tpe-ask-count", type=int, default=224)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    report = run_stress(
        source_root=args.source_root.resolve(),
        registry_path=args.registry.resolve(),
        tpe_ask_count=int(args.tpe_ask_count),
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
