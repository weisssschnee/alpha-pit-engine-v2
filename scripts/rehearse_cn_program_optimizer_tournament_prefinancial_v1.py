"""Build and verify Stage 0/1 without app.py or Project Control admission."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from our_system_phase2.services.program_tournament_freeze_v1 import (
    build_stage01_freeze_v1,
    verify_phase_freeze_v1,
    verify_source_binding_v1,
)
from our_system_phase2.runtime.cn_program_optimizer_tournament_v1 import (
    build_maximum_ask_plan_v1,
    authorization_payload_v1,
)
from our_system_phase2.services.program_optimizer_tournament_v1 import (
    ProgramOptimizerTournamentV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    PROGRAM_OPTIMIZER_ARMS,
    STRUCTURED_SURROGATE_PROGRAM,
    UNIFORM_CONTROL,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)
from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_tournament_v1 as tournament_runner


EXPECTED_INITIAL_STATE_SHA256 = (
    "2ed3b5971622949d5f90633b20b65db5aebacc24f580a0081b15fd93ae0ac1b3"
)
EXPECTED_AUTHORIZATION_SHA256 = (
    "99430779f9b80651da1928a90e3cfc0df2357a750fb8837da5c5cd0f195685fd"
)
EXPECTED_MAXIMUM_ASK_PLAN_SHA256 = (
    "eb66b7b374f7e2550db5bf5d7cee98fd66a8526dabadfbda14af1ecd6cf3dc4b"
)
EXPECTED_PROGRAM_SPACE_SHA256 = (
    "86d9bce8f7bdc75e55c791b6e101ec4beefe093e346abfc39c76ee1c30f354e0"
)
EXPECTED_SOURCE_BINDING_SHA256 = (
    "d9cdcc4459590dc769aa9b1f724530cb0d1d18f2ba999e3d790a4707b07d6d94"
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _first_enhanced_checkpoint_by_arm(
    asks: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    for arm in PROGRAM_OPTIMIZER_ARMS:
        first = next(
            int(row["checkpoint_ordinal"])
            for row in asks
            if str(row["generation_arm"]) == arm
            and str(row["template_id"]) != "BASE"
        )
        output[arm] = [
            dict(row)
            for row in asks
            if int(row["checkpoint_ordinal"]) == first
        ]
    return output


def _arm_rehearsal(
    *,
    arm: str,
    asks: Sequence[Mapping[str, Any]],
    catalog: Mapping[str, Sequence[Mapping[str, Any]]],
    bandit: ProgramOptimizerTournamentV1,
    components_by_id: Mapping[str, Any],
    adapter: Any,
    compiler: Any,
) -> dict[str, Any]:
    schedules, decisions = tournament_runner._select_checkpoint(
        asks,
        catalog=catalog,
        bandit=bandit,
        state=engine._selection_state(),
        components_by_id=components_by_id,
        adapter=adapter,
        compiler=compiler,
    )
    if len(schedules) != len(asks) or len(decisions) != len(asks):
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_CARDINALITY_DRIFT")
    if [int(row["template_record_ordinal"]) for row in schedules] != [
        int(row["template_record_ordinal"]) for row in asks
    ] or any(
        row["schedule_record_sha256"]
        != stable_hash(
            {
                key: value
                for key, value in row.items()
                if key != "schedule_record_sha256"
            }
        )
        for row in schedules
    ):
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_SCHEDULE_DRIFT")
    optimizer_asks = [dict(row["optimizer_ask"]) for row in schedules]
    exact_identities = [str(row["exact_identity"]) for row in optimizer_asks]
    legal = {entry.exact_identity for entry in bandit.entries_by_arm[arm]}
    if len(set(exact_identities)) != len(exact_identities) or not set(
        exact_identities
    ).issubset(legal):
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_LEGALITY_DRIFT")
    result = {
        "optimizer_arm": arm,
        "checkpoint_ordinal": int(asks[0]["checkpoint_ordinal"]),
        "checkpoint_id": str(optimizer_asks[0]["checkpoint_id"]),
        "template_id": str(asks[0]["template_id"]),
        "ask_count": len(optimizer_asks),
        "schedule_count": len(schedules),
        "decision_count": len(decisions),
        "exact_identity_count": len(set(exact_identities)),
        "all_exact_identities_legal": True,
        "template_record_ordinals": [
            int(row["template_record_ordinal"]) for row in schedules
        ],
    }
    if arm == HYBRID_TPE_PROGRAM:
        projections = [
            dict(dict(row["acquisition"])["projection"])
            for row in optimizer_asks
        ]
        projection_statistics = bandit.adapters[arm].projection_statistics()
        result.update(
            {
                "official_optuna_ask": all(
                    dict(row["acquisition"])["source"]
                    == "official_optuna.samplers.TPESampler"
                    for row in optimizer_asks
                ),
                "trial_numbers": [int(row["trial_number"]) for row in optimizer_asks],
                "raw_exact_identities": [
                    str(row["raw_exact_identity"]) for row in projections
                ],
                "actual_exact_identities": [
                    str(row["actual_exact_identity"]) for row in projections
                ],
                "projection_modes": [str(row["mode"]) for row in projections],
                "projection_statistics": projection_statistics,
                "global_fallback_count": sum(
                    bool(row["global_fallback"]) for row in projections
                ),
            }
        )
    elif arm == STRUCTURED_SURROGATE_PROGRAM:
        compared = [
            int(dict(row["acquisition"])["eligible_compared_count"])
            for row in optimizer_asks
        ]
        eligible_count = len(schedules[0]["optimizer_eligible_exact_identities"])
        result.update(
            {
                "eligible_compared_counts": compared,
                "eligible_exact_identity_count": eligible_count,
                "full_eligible_set_scored": len(set(compared)) == 1
                and compared[0] == eligible_count,
                "cold_start": all(
                    bool(dict(row["acquisition"])["cold_start"])
                    for row in optimizer_asks
                ),
            }
        )
    elif arm == UNIFORM_CONTROL:
        result["economic_feedback_used"] = False
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-binding", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--failed-initial-state", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--git-executable", default="git")
    args = parser.parse_args()
    if args.output_root.exists():
        raise FileExistsError(args.output_root)
    actual_repo_sha = subprocess.check_output(
        [args.git_executable, "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    if actual_repo_sha != args.repo_sha:
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_REPO_SHA_DRIFT")

    evaluator_guard_attempts = 0

    def _forbid_evaluator(*_args: Any, **_kwargs: Any) -> None:
        nonlocal evaluator_guard_attempts
        evaluator_guard_attempts += 1
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_EVALUATOR_FORBIDDEN")

    engine._initialize_worker = _forbid_evaluator
    engine._evaluate_record = _forbid_evaluator
    engine.phase_b._load_context = _forbid_evaluator
    engine.phase_b._initialize_worker = _forbid_evaluator
    engine.phase_b._evaluate_record = _forbid_evaluator

    source = verify_source_binding_v1(
        args.source_binding, repository_root=ROOT
    )
    build_stage01_freeze_v1(
        output_root=args.output_root,
        source_binding_path=args.source_binding,
        repo_sha=args.repo_sha,
    )
    closure = verify_phase_freeze_v1(args.output_root)
    contract = json.loads(
        (args.output_root / "phase_c_run_contract.json").read_text(
            encoding="utf-8-sig"
        )
    )
    entries = tuple(source["program_entries"])
    entries_by_arm = {arm: entries for arm in PROGRAM_OPTIMIZER_ARMS}
    initial_state_path = args.output_root / "initial_bandit_state.json"
    initial_state = _read_json(initial_state_path)
    restored = ProgramOptimizerTournamentV1.restore(
        initial_state,
        entries_by_arm=entries_by_arm,
        expected_campaign_id=str(initial_state["campaign_id"]),
    )
    restored_state = restored.snapshot()
    if initial_state != restored_state:
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_STATE_REPLAY_DRIFT")

    failed_state = _read_json(args.failed_initial_state.resolve())
    failed_restored = ProgramOptimizerTournamentV1.restore(
        failed_state,
        entries_by_arm=entries_by_arm,
        expected_campaign_id=str(failed_state["campaign_id"]),
    )
    failed_restored_state = failed_restored.snapshot()
    state_hashes = {
        "new_freeze_original": str(initial_state["bandit_state_sha256"]),
        "new_freeze_restored": str(restored_state["bandit_state_sha256"]),
        "failed_freeze_original": str(failed_state["bandit_state_sha256"]),
        "failed_freeze_restored": str(
            failed_restored_state["bandit_state_sha256"]
        ),
    }
    if set(state_hashes.values()) != {EXPECTED_INITIAL_STATE_SHA256}:
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_STATE_HASH_DRIFT")

    reservoir = engine._read_jsonl(
        args.output_root / "phase_c_raw_program_reservoir.jsonl"
    )
    component_rows = engine._read_jsonl(
        args.output_root / "phase_c_session_executable_component_pool.jsonl"
    )
    registry = UnifiedCapabilityRegistry.read(Path(source["registry_path"]))
    catalog, catalog_report = engine._build_catalog(
        reservoir=reservoir,
        component_rows=component_rows,
        registry=registry,
    )
    components_by_id = {
        component.component_id: component
        for component in (
            engine._component_from_row(row) for row in component_rows
        )
    }
    adapter = engine.CandidateProgramProposalAdapterV0(registry)
    compiler = engine.ProgramCompilerV1(registry)
    asks = engine._read_jsonl(args.output_root / "phase_c_ask_plan.jsonl")
    checkpoint_zero_asks = [
        dict(row) for row in asks if int(row["checkpoint_ordinal"]) == 0
    ]
    checkpoint_zero_bandit = ProgramOptimizerTournamentV1.restore(
        initial_state,
        entries_by_arm=entries_by_arm,
        expected_campaign_id=str(initial_state["campaign_id"]),
    )
    checkpoint_zero = _arm_rehearsal(
        arm=UNIFORM_CONTROL,
        asks=checkpoint_zero_asks,
        catalog=catalog,
        bandit=checkpoint_zero_bandit,
        components_by_id=components_by_id,
        adapter=adapter,
        compiler=compiler,
    )
    arm_asks = _first_enhanced_checkpoint_by_arm(asks)
    arm_rehearsals = {
        arm: _arm_rehearsal(
            arm=arm,
            asks=arm_asks[arm],
            catalog=catalog,
            bandit=ProgramOptimizerTournamentV1.restore(
                initial_state,
                entries_by_arm=entries_by_arm,
                expected_campaign_id=str(initial_state["campaign_id"]),
            ),
            components_by_id=components_by_id,
            adapter=adapter,
            compiler=compiler,
        )
        for arm in PROGRAM_OPTIMIZER_ARMS
    }
    if (
        checkpoint_zero["checkpoint_ordinal"] != 0
        or checkpoint_zero["template_id"] != "BASE"
        or checkpoint_zero["schedule_count"] != 8
    ):
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_FIRST_CHECKPOINT_DRIFT")
    if any(
        row["template_id"] == "BASE" or row["schedule_count"] != 8
        for row in arm_rehearsals.values()
    ):
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_ENHANCED_ARM_DRIFT")
    if arm_rehearsals[HYBRID_TPE_PROGRAM]["global_fallback_count"] != 0:
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_GLOBAL_FALLBACK")
    authorization = authorization_payload_v1()
    maximum_ask_plan_sha256 = stable_hash(list(build_maximum_ask_plan_v1()))
    source_payload = dict(source["payload"])
    if (
        str(authorization["authorization_payload_sha256"])
        != EXPECTED_AUTHORIZATION_SHA256
        or maximum_ask_plan_sha256 != EXPECTED_MAXIMUM_ASK_PLAN_SHA256
        or len(source["program_entries"]) != 3616
        or str(source["program_space_sha256"]) != EXPECTED_PROGRAM_SPACE_SHA256
        or str(source_payload["source_binding_payload_sha256"])
        != EXPECTED_SOURCE_BINDING_SHA256
    ):
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_FROZEN_IDENTITY_DRIFT")
    optimizer_economic_observations = int(contract["development_observation_count"])
    restricted_reads = {
        "validation": int(closure["validation_reads"]),
        "holdout": int(closure["holdout_reads"]),
        "historical_2023": int(closure["historical_2023_reads"]),
        "forward_b": int(closure["forward_b_reads"]),
        "forward_2026": int(closure["forward_2026_reads"]),
    }
    if (
        evaluator_guard_attempts
        or optimizer_economic_observations
        or bool(closure["financial_evaluation_executed"])
        or any(restricted_reads.values())
    ):
        raise RuntimeError("PROGRAM_TOURNAMENT_REHEARSAL_READ_BOUNDARY_DRIFT")
    result = {
        "status": "EXECUTION_BOUNDARY_REHEARSAL_COMPLETE",
        "repo_sha": actual_repo_sha,
        "search_v2_source_freeze_verify": "PASS",
        "tournament_stage01_freeze_verify": "PASS",
        "initial_state_disk_load": "PASS",
        "initial_state_replay_exact": True,
        "failed_initial_state_replay_exact": failed_restored_state == failed_state,
        "state_hashes": state_hashes,
        "failed_initial_state_path": str(args.failed_initial_state.resolve()),
        "failed_initial_state_file_sha256": engine._sha256(
            args.failed_initial_state.resolve()
        ),
        "output_root": str(args.output_root.resolve()),
        "program_space_count": len(source["program_entries"]),
        "program_space_sha256": source["program_space_sha256"],
        "maximum_ask_plan_sha256": maximum_ask_plan_sha256,
        "tournament_authorization_payload_sha256": str(
            authorization["authorization_payload_sha256"]
        ),
        "source_binding_payload_sha256": str(
            source_payload["source_binding_payload_sha256"]
        ),
        "source_required_physical_leaves": int(
            source_payload["source_search_v2_prefinancial_freeze"][
                "required_physical_leaf_count"
            ]
        ),
        "source_available_after_materialization": int(
            source_payload["source_search_v2_prefinancial_freeze"][
                "available_after_materialization_count"
            ]
        ),
        "source_unresolved_required_fields": int(
            source_payload["source_search_v2_prefinancial_freeze"][
                "unresolved_required_field_count"
            ]
        ),
        "stage01_ask_count": int(closure["main_record_count"]),
        "catalog_preflight_sha256": str(catalog_report["catalog_preflight_sha256"]),
        "checkpoint_zero_rehearsal": checkpoint_zero,
        "arm_rehearsals": arm_rehearsals,
        "optimizer_economic_observations": optimizer_economic_observations,
        "financial_evaluation_executed": bool(
            closure["financial_evaluation_executed"]
        ),
        "evaluator_guard": "INSTALLED_AND_NOT_TRIGGERED",
        "evaluator_guard_attempts": evaluator_guard_attempts,
        "market_price_rows_read": 0,
        "restricted_reads": restricted_reads,
        "project_control": "NOT_REQUESTED",
        "project_control_admission": "NOT_REQUESTED",
        "app_high_cost_route": "NOT_CALLED",
        "evaluator": "NOT_CALLED",
        "retry": "NOT_RUN",
        "tournament": "NOT_RUN",
        "promotion": False,
        "automatic_successor": False,
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
