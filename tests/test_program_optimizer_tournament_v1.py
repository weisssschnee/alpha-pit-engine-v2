from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

from our_system_phase2.runtime.cn_program_optimizer_tournament_v1 import (
    AUTHORIZATION_STATUS,
    MAXIMUM_TOTAL_RECORDS,
    RACING_RULES,
    authorization_payload_v1,
    build_maximum_ask_plan_v1,
    freeze_racing_decision_v1,
    verify_racing_decision_v1,
)
from our_system_phase2.services.program_tournament_freeze_v1 import (
    _phase_freeze,
    stage01_asks_v1,
    stage2_asks_v1,
)
from our_system_phase2.services.program_optimizer_tournament_v1 import (
    ProgramOptimizerTournamentV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    PROGRAM_OPTIMIZER_ARMS,
    STRUCTURED_SURROGATE_PROGRAM,
    UNIFORM_CONTROL,
    program_availability_entries_v1,
)


ROOT = Path(__file__).resolve().parents[1]


def _import_smoke(statement: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "src")
    return subprocess.run(
        [sys.executable, "-c", statement],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_execution_runner_import_smoke() -> None:
    result = _import_smoke(
        "import scripts.run_cn_program_optimizer_tournament_v1"
    )
    assert result.returncode == 0, result.stderr


def test_project_control_tournament_entry_import_smoke() -> None:
    result = _import_smoke(
        "import app; "
        "import our_system_phase2.runtime.cn_program_optimizer_tournament_v1; "
        "import scripts.run_cn_program_optimizer_tournament_v1"
    )
    assert result.returncode == 0, result.stderr


def _entries():
    return program_availability_entries_v1(
        [
            {
                "genes": {
                    "skeleton_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "gene_surface_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "program_template_id": "BASE_TEMPORAL",
                    "active_component_roles": "base+temporal",
                    "base__route_id": f"base_{index % 4}",
                    "temporal__primitive": f"temporal_{index % 6}",
                    "raw_field_count": str(2 + index % 3),
                    "program_variant": str(index),
                }
            }
            for index in range(48)
        ]
    )


def _tournament() -> ProgramOptimizerTournamentV1:
    entries = _entries()
    return ProgramOptimizerTournamentV1.fresh(
        campaign_id="test-tournament",
        entries_by_arm={arm: entries for arm in PROGRAM_OPTIMIZER_ARMS},
        seeds={
            UNIFORM_CONTROL: 1,
            HYBRID_TPE_PROGRAM: 2,
            STRUCTURED_SURROGATE_PROGRAM: 3,
        },
        tpe_config={"n_startup_trials": 2, "n_ei_candidates": 8},
        surrogate_config={
            "cold_start_asks": 4,
            "candidate_pool_size": 32,
            "n_estimators": 16,
            "min_samples_leaf": 1,
        },
    )


def test_frozen_plan_has_three_arms_uniform_support_and_no_runtime_authority() -> None:
    asks = build_maximum_ask_plan_v1()
    authorization = authorization_payload_v1()
    assert len(asks) == MAXIMUM_TOTAL_RECORDS
    assert authorization["status"] == AUTHORIZATION_STATUS
    assert authorization["execution_authorized"] is True
    assert authorization["tournament_status"] == "FROZEN_NOT_RUN"
    assert set(authorization["optimizer_arms"]) == set(PROGRAM_OPTIMIZER_ARMS)
    assert authorization["development_feedback_provenance"][
        "development_observation_count"
    ] == 0
    assert authorization["financial_data_reads"] == 0
    assert RACING_RULES["winner_takes_all_allowed"] is False
    assert RACING_RULES["uniform_control_stage_2_floor_per_template"] > 0


def test_tournament_shared_space_snapshot_restore_and_real_tpe_trial() -> None:
    tournament = _tournament()
    snapshot = tournament.snapshot()
    restored = ProgramOptimizerTournamentV1.restore(
        snapshot,
        entries_by_arm=tournament.entries_by_arm,
        expected_campaign_id="test-tournament",
    )
    assert restored.snapshot() == snapshot
    spaces = {
        arm: adapter.controller.input_hashes["program_space"]
        for arm, adapter in restored.adapters.items()
    }
    assert len(set(spaces.values())) == 1
    eligible = [entry.exact_identity for entry in _entries()]
    asked = restored.ask(
        arm=HYBRID_TPE_PROGRAM,
        checkpoint_id="checkpoint_001",
        count=4,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=eligible,
    )
    assert len(asked) == 4
    assert all(row["trial_number"] is not None for row in asked)
    assert all(row["optimizer_ask_identity"] for row in asked)


def test_staged_racing_plans_are_checkpoint_stratified_and_keep_uniform() -> None:
    stage01 = stage01_asks_v1()
    stage2 = stage2_asks_v1(
        (UNIFORM_CONTROL, STRUCTURED_SURROGATE_PROGRAM)
    )
    assert len(stage01) == 368
    assert len(stage2) == 112
    for rows in (stage01, stage2):
        assert len(rows) % 8 == 0
        for start in range(0, len(rows), 8):
            checkpoint = rows[start : start + 8]
            assert len({row["optimizer_arm"] for row in checkpoint}) == 1
            assert len({row["template_id"] for row in checkpoint}) == 1
    assert {row["optimizer_arm"] for row in stage2} == {
        UNIFORM_CONTROL,
        STRUCTURED_SURROGATE_PROGRAM,
    }


def _feedback(arm: str, *, admitted: int, productive: int) -> list[dict]:
    rows = []
    for index in range(112):
        is_admitted = index < admitted
        is_productive = index < productive
        rows.append(
            {
                "template_id": (
                    "BASE_TEMPORAL",
                    "BASE_MARKET",
                    "BASE_EVENT",
                    "BASE_TEMPORAL_MARKET",
                    "BASE_TEMPORAL_EVENT",
                    "BASE_MARKET_EVENT",
                    "BASE_TEMPORAL_MARKET_EVENT",
                )[index % 7],
                "generation_arm": arm,
                "absolute_admission": {"admitted": is_admitted},
                "enhancer_credit": (
                    {
                        "program_credit": {
                            "matched_net_reward_increment": 0.1 if is_productive else -0.1,
                            "matched_cumulative_net_return_increment": (
                                0.1 if is_productive else -0.1
                            ),
                        }
                    }
                    if is_admitted
                    else None
                ),
            }
        )
    return rows


def test_frozen_racing_keeps_uniform_and_stops_only_supported_futile_arm() -> None:
    rows = [
        *_feedback(UNIFORM_CONTROL, admitted=98, productive=91),
        *_feedback(HYBRID_TPE_PROGRAM, admitted=49, productive=4),
        *_feedback(STRUCTURED_SURROGATE_PROGRAM, admitted=20, productive=1),
    ]
    decision = freeze_racing_decision_v1(rows)
    assert verify_racing_decision_v1(decision) == decision
    assert decision["stage2_active_arms"] == [
        UNIFORM_CONTROL,
        STRUCTURED_SURROGATE_PROGRAM,
    ]
    assert decision["arm_decisions"][HYBRID_TPE_PROGRAM]["support_met"] is True
    assert decision["arm_decisions"][HYBRID_TPE_PROGRAM]["stage2_active"] is False
    assert decision["arm_decisions"][STRUCTURED_SURROGATE_PROGRAM][
        "support_met"
    ] is False


def test_stage2_freeze_carries_same_campaign_state_without_import(tmp_path) -> None:
    source = tmp_path / "stage01_freeze"
    source.mkdir()
    (source / "phase_c_raw_program_reservoir.jsonl").write_text(
        "{}\n", encoding="utf-8"
    )
    (source / "phase_c_session_executable_component_pool.jsonl").write_text(
        "{}\n", encoding="utf-8"
    )
    contract = {
        "schema_version": "source",
        "accepted_field_manifest_path": str(tmp_path / "manifest.json"),
        "run_contract_sha256": "source-hash",
    }
    import json

    (source / "phase_c_run_contract.json").write_text(
        json.dumps(contract), encoding="utf-8"
    )
    state = _tournament().snapshot()
    target = tmp_path / "stage02_freeze"
    _phase_freeze(
        output_root=target,
        source_freeze_root=source,
        asks=stage2_asks_v1((UNIFORM_CONTROL,)),
        initial_state=state,
        program_space_hash=str(state["program_space_hash"]),
        tournament_phase="STAGE_2",
        active_arms=(UNIFORM_CONTROL,),
        repo_sha="a" * 40,
    )
    frozen = json.loads((target / "phase_c_run_contract.json").read_text())
    assert frozen["main_record_count"] == 56
    assert frozen["minimum_base_identities_per_template"] == 8
    assert frozen["same_campaign_optimizer_state_carried_forward"] is True
    assert frozen["development_financial_observations_imported"] is False
    assert frozen["prior_program_exact_identity_count"] == 0
