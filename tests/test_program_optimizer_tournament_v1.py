from __future__ import annotations

import os
import json
from pathlib import Path
import subprocess
import sys

import pytest

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
    build_stage01_freeze_v1,
    stage01_asks_v1,
    stage2_asks_v1,
    verify_source_binding_v1,
)
from our_system_phase2.services import program_tournament_freeze_v1 as freeze
from our_system_phase2.services.program_optimizer_tournament_v1 import (
    ProgramOptimizerTournamentV1,
    _first_replay_difference,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    PROGRAM_OPTIMIZER_ARMS,
    STRUCTURED_SURROGATE_PROGRAM,
    UNIFORM_CONTROL,
    ProgramOptimizerObservationV1,
    program_availability_entries_v1,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import ProgramUpliftCredit


ROOT = Path(__file__).resolve().parents[1]


class _SyntheticEntry:
    def __init__(self, value: int) -> None:
        self.value = value

    def to_dict(self) -> dict[str, int]:
        return {"value": self.value}


def _synthetic_source_binding(tmp_path, monkeypatch) -> tuple[Path, dict[str, Path]]:
    source = tmp_path / "source_freeze"
    source.mkdir()
    phase_b_freeze = tmp_path / "phase_b_freeze"
    phase_b_freeze.mkdir()
    phase_b_result = tmp_path / "phase_b_result"
    phase_b_result.mkdir()
    repo = tmp_path / "repo"
    registry_relative = Path("runtime/registry.json")
    registry = repo / registry_relative
    registry.parent.mkdir(parents=True)
    capacity = tmp_path / "capacity.json"
    accepted = tmp_path / "accepted.json"
    closure = source / freeze.SOURCE_FREEZE_CLOSURE_NAME
    manifest = source / "ARTIFACT_MANIFEST.json"
    reservoir = source / "phase_c_raw_program_reservoir.jsonl"
    components = source / "phase_c_session_executable_component_pool.jsonl"
    phase_b_closure = phase_b_result / freeze.SOURCE_PHASE_B_CLOSURE_NAME
    for path, content in (
        (registry, "registry"),
        (capacity, "capacity"),
        (accepted, "accepted"),
        (closure, "closure"),
        (manifest, "manifest"),
        (reservoir, "reservoir"),
        (components, "components"),
    ):
        path.write_text(content, encoding="utf-8")
    phase_b_closure.write_text(
        json.dumps({"closure_payload_sha256": "phase-b-payload"}),
        encoding="utf-8",
    )
    sha = freeze.phase_c._sha256
    entries = (_SyntheticEntry(1), _SyntheticEntry(2))
    space_sha = freeze.stable_hash([entry.to_dict() for entry in entries])
    source_hashes = {
        "raw_program_reservoir": sha(reservoir),
        "session_executable_component_pool": sha(components),
        "unified_capability_registry": sha(registry),
    }
    contract = {
        "registry_path": str(registry),
        "registry_file_sha256": sha(registry),
        "node_resource_capacity_path": str(capacity),
        "node_resource_capacity_file_sha256": sha(capacity),
        "source_accepted_field_manifest_path": str(accepted),
        "source_accepted_field_manifest_file_sha256": sha(accepted),
        "phase_b_freeze_root": str(phase_b_freeze),
        "phase_b_result_root": str(phase_b_result),
    }
    (source / "phase_c_run_contract.json").write_text(
        json.dumps(contract), encoding="utf-8"
    )
    monkeypatch.setattr(freeze, "SOURCE_FREEZE_ROOT", source)
    monkeypatch.setattr(freeze, "SOURCE_REGISTRY_PATH", registry)
    monkeypatch.setattr(
        freeze, "SOURCE_REGISTRY_REPOSITORY_RELATIVE_PATH", registry_relative
    )
    monkeypatch.setattr(freeze, "SOURCE_NODE_CAPACITY_PATH", capacity)
    monkeypatch.setattr(freeze, "SOURCE_ACCEPTED_FIELD_MANIFEST_PATH", accepted)
    monkeypatch.setattr(freeze, "SOURCE_PHASE_B_FREEZE_ROOT", phase_b_freeze)
    monkeypatch.setattr(freeze, "SOURCE_PHASE_B_RESULT_ROOT", phase_b_result)
    monkeypatch.setattr(freeze, "SOURCE_FREEZE_CLOSURE_FILE_SHA256", sha(closure))
    monkeypatch.setattr(freeze, "SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256", "closure-payload")
    monkeypatch.setattr(freeze, "SOURCE_FREEZE_MANIFEST_FILE_SHA256", sha(manifest))
    monkeypatch.setattr(freeze, "SOURCE_FREEZE_MANIFEST_PAYLOAD_SHA256", "manifest-payload")
    monkeypatch.setattr(freeze, "SOURCE_REGISTRY_SHA256", sha(registry))
    monkeypatch.setattr(freeze, "SOURCE_NODE_CAPACITY_SHA256", sha(capacity))
    monkeypatch.setattr(
        freeze, "SOURCE_ACCEPTED_FIELD_MANIFEST_SHA256", sha(accepted)
    )
    monkeypatch.setattr(
        freeze, "SOURCE_PHASE_B_CLOSURE_FILE_SHA256", sha(phase_b_closure)
    )
    monkeypatch.setattr(
        freeze, "SOURCE_PHASE_B_CLOSURE_PAYLOAD_SHA256", "phase-b-payload"
    )
    monkeypatch.setattr(freeze, "FROZEN_PROGRAM_SPACE_ENTRY_COUNT", len(entries))
    monkeypatch.setattr(freeze, "FROZEN_PROGRAM_SPACE_SHA256", space_sha)
    monkeypatch.setattr(freeze, "FROZEN_PROGRAM_SPACE_SOURCE_SHA256", source_hashes)
    monkeypatch.setattr(freeze, "_program_entries", lambda **_: entries)
    monkeypatch.setattr(
        freeze,
        "verify_search_v2_freeze",
        lambda _: {
            "closure_sha256": "closure-payload",
            "required_physical_leaf_count": 84,
            "available_after_materialization_count": 84,
            "unresolved_required_field_count": 0,
        },
    )
    body = {
        "schema_version": freeze.SOURCE_BINDING_SCHEMA,
        "source_search_v2_prefinancial_freeze": {
            "root": str(source),
            "closure": {
                "relative_path": freeze.SOURCE_FREEZE_CLOSURE_NAME,
                "file_sha256": sha(closure),
                "payload_sha256": "closure-payload",
            },
            "artifact_manifest": {
                "file_sha256": sha(manifest),
                "payload_sha256": "manifest-payload",
            },
            "required_physical_leaf_count": 84,
            "available_after_materialization_count": 84,
            "unresolved_required_field_count": 0,
            "financial_evaluation_executed": False,
        },
        "registry_authority": {
            "source_path": str(registry),
            "repository_relative_path": str(registry_relative),
            "sha256": sha(registry),
        },
        "node_resource_capacity_authority": {
            "source_path": str(capacity),
            "sha256": sha(capacity),
        },
        "accepted_field_manifest": {
            "path": str(accepted),
            "sha256": sha(accepted),
        },
        "phase_b_binding": {
            "freeze_root": str(phase_b_freeze),
            "result_root": str(phase_b_result),
            "result_closure": {
                "relative_path": freeze.SOURCE_PHASE_B_CLOSURE_NAME,
                "file_sha256": sha(phase_b_closure),
                "payload_sha256": "phase-b-payload",
            },
        },
        "program_space": {
            "entry_count": len(entries),
            "sha256": space_sha,
            "source_sha256": source_hashes,
        },
        "maximum_ask_plan_sha256": freeze.stable_hash(
            list(freeze.build_maximum_ask_plan_v1())
        ),
        "tournament_authorization_payload_sha256": freeze.authorization_payload_v1()[
            "authorization_payload_sha256"
        ],
        "restricted_reads": {
            "validation": 0,
            "holdout": 0,
            "historical_2023": 0,
            "forward_b": 0,
            "forward_2026": 0,
        },
    }
    payload = dict(body)
    payload["source_binding_payload_sha256"] = freeze.stable_hash(body)
    binding = tmp_path / "binding.json"
    binding.write_text(json.dumps(payload), encoding="utf-8")
    return binding, {
        "repo": repo,
        "registry": registry,
        "capacity": capacity,
        "accepted": accepted,
        "closure": closure,
    }


def test_existing_source_freeze_is_verified_and_consumed_without_rebuild(
    tmp_path, monkeypatch
) -> None:
    binding, paths = _synthetic_source_binding(tmp_path, monkeypatch)
    verified = verify_source_binding_v1(binding, repository_root=paths["repo"])
    assert len(verified["program_entries"]) == 2
    monkeypatch.setattr(
        freeze,
        "verify_source_binding_v1",
        lambda _: {
            "source_freeze_root": tmp_path / "closed-source",
            "program_entries": (_SyntheticEntry(1),),
        },
    )
    monkeypatch.setattr(
        freeze.ProgramOptimizerTournamentV1,
        "fresh",
        lambda **_: type("Tournament", (), {"snapshot": lambda self: {"program_space_hash": freeze.FROZEN_PROGRAM_SPACE_SHA256}})(),
    )
    captured = {}
    monkeypatch.setattr(
        freeze,
        "_phase_freeze",
        lambda **kwargs: captured.update(kwargs) or {"status": "complete"},
    )
    result = build_stage01_freeze_v1(
        output_root=tmp_path / "target",
        source_binding_path=binding,
        repo_sha="a" * 40,
    )
    assert result["status"] == "complete"
    assert captured["source_freeze_root"] == tmp_path / "closed-source"


@pytest.mark.parametrize("field", ["registry", "capacity", "accepted", "closure"])
def test_source_binding_asset_drift_fails_closed(tmp_path, monkeypatch, field) -> None:
    binding, paths = _synthetic_source_binding(tmp_path, monkeypatch)
    paths[field].write_text("drift", encoding="utf-8")
    with pytest.raises(ValueError, match="source binding"):
        verify_source_binding_v1(binding, repository_root=paths["repo"])


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


def _persisted(payload: dict) -> dict:
    return json.loads(json.dumps(payload, sort_keys=True))


def _observation(ask: dict, *, admitted: bool) -> ProgramOptimizerObservationV1:
    identity = str(ask["exact_identity"])
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=f"record-{identity}",
        pair_id=f"pair-{identity}",
        program_id=f"program-{identity}",
        control_program_id=f"control-{identity}",
        admitted=admitted,
        failure_reasons=() if admitted else ("PRIMARY_NET_REWARD_NOT_POSITIVE",),
        metrics={"synthetic": True},
    )
    uplift = (
        ProgramUpliftCredit(
            record_payload_sha256=admission.record_payload_sha256,
            pair_id=admission.pair_id,
            program_id=admission.program_id,
            control_program_id=admission.control_program_id,
            program_credit={
                "matched_cumulative_net_return_increment": 0.1,
                "matched_net_reward_increment": 0.2,
            },
        )
        if admitted
        else None
    )
    return ProgramOptimizerObservationV1(
        proposal_id=str(ask["proposal_id"]),
        exact_identity=identity,
        admission=admission,
        uplift=uplift,
    )


def _consume_arm(tournament: ProgramOptimizerTournamentV1, arm: str) -> None:
    eligible = [entry.exact_identity for entry in tournament.entries_by_arm[arm]]
    asked = tournament.ask(
        arm=arm,
        checkpoint_id=f"checkpoint-{arm}",
        count=4,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=eligible,
    )
    tournament.commit_ask(
        arm=arm,
        checkpoint_id=f"checkpoint-{arm}",
        count=4,
        required_program_template_id="BASE_TEMPORAL",
        eligible_exact_identities=eligible,
        expected_asks=asked,
    )
    tournament.tell(
        arm=arm,
        observations=[
            _observation(row, admitted=index % 2 == 0)
            for index, row in enumerate(asked)
        ],
    )


def test_tournament_genesis_json_persisted_roundtrip_exact() -> None:
    tournament = _tournament()
    persisted = _persisted(tournament.snapshot())
    restored = ProgramOptimizerTournamentV1.restore(
        persisted,
        entries_by_arm=tournament.entries_by_arm,
        expected_campaign_id="test-tournament",
    )
    assert restored.snapshot() == persisted


def test_tournament_nonempty_mixed_arm_json_persisted_roundtrip_exact() -> None:
    tournament = _tournament()
    for arm in PROGRAM_OPTIMIZER_ARMS:
        _consume_arm(tournament, arm)
    persisted = _persisted(tournament.snapshot())
    restored = ProgramOptimizerTournamentV1.restore(
        persisted,
        entries_by_arm=tournament.entries_by_arm,
        expected_campaign_id="test-tournament",
    )
    assert persisted["observations"] > 0
    assert restored.snapshot() == persisted


def test_replay_diagnostic_reports_first_json_path_and_types() -> None:
    original = {
        "arms": {
            STRUCTURED_SURROGATE_PROGRAM: {
                "categories": {"program_template_id": ["BASE_TEMPORAL"]}
            }
        }
    }
    restored = {
        "arms": {
            STRUCTURED_SURROGATE_PROGRAM: {
                "categories": {"program_template_id": ("BASE_TEMPORAL",)}
            }
        }
    }
    assert _first_replay_difference(original, restored) == {
        "path": (
            "/arms/STRUCTURED_SURROGATE_PROGRAM/categories/program_template_id"
        ),
        "original_type": "list",
        "restored_type": "tuple",
        "original_value": "['BASE_TEMPORAL']",
        "restored_value": "('BASE_TEMPORAL',)",
    }


def test_frozen_plan_has_three_arms_uniform_support_and_no_runtime_authority() -> None:
    asks = build_maximum_ask_plan_v1()
    authorization = authorization_payload_v1()
    assert len(asks) == MAXIMUM_TOTAL_RECORDS
    assert freeze.stable_hash(list(asks)) == (
        "eb66b7b374f7e2550db5bf5d7cee98fd66a8526dabadfbda14af1ecd6cf3dc4b"
    )
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
    snapshot = _persisted(tournament.snapshot())
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


def test_stage01_execution_asks_derive_phase_local_template_ordinals() -> None:
    stage01 = stage01_asks_v1()
    assert len(stage01) == 368
    assert len(stage01) // 8 == 46
    frozen_source = [
        row for row in build_maximum_ask_plan_v1() if int(row["stage"]) <= 1
    ]
    assert [row["template_stage_ordinal"] for row in stage01] == [
        row["template_stage_ordinal"] for row in frozen_source
    ]
    assert all(
        row["ask_record_sha256"]
        == freeze.stable_hash(
            {key: value for key, value in row.items() if key != "ask_record_sha256"}
        )
        for row in stage01
    )
    for template_id in {str(row["template_id"]) for row in stage01}:
        ordinals = [
            int(row["template_record_ordinal"])
            for row in stage01
            if str(row["template_id"]) == template_id
        ]
        expected_count = 32 if template_id == "BASE" else 48
        assert ordinals == list(range(expected_count))
    for start in range(0, len(stage01), 8):
        checkpoint = stage01[start : start + 8]
        assert len({row["optimizer_arm"] for row in checkpoint}) == 1
        assert len({row["template_id"] for row in checkpoint}) == 1


@pytest.mark.parametrize(
    "active_arms",
    (
        (UNIFORM_CONTROL,),
        (UNIFORM_CONTROL, HYBRID_TPE_PROGRAM),
        (UNIFORM_CONTROL, STRUCTURED_SURROGATE_PROGRAM),
        PROGRAM_OPTIMIZER_ARMS,
    ),
)
def test_stage2_execution_asks_derive_contiguous_phase_local_template_ordinals(
    active_arms,
) -> None:
    stage2 = stage2_asks_v1(active_arms)
    frozen_source = [
        row
        for row in build_maximum_ask_plan_v1()
        if int(row["stage"]) == 2 and str(row["optimizer_arm"]) in active_arms
    ]
    assert [row["template_stage_ordinal"] for row in stage2] == [
        row["template_stage_ordinal"] for row in frozen_source
    ]
    assert all("template_record_ordinal" in row for row in stage2)
    assert all(
        row["ask_record_sha256"]
        == freeze.stable_hash(
            {key: value for key, value in row.items() if key != "ask_record_sha256"}
        )
        for row in stage2
    )
    for template_id in {str(row["template_id"]) for row in stage2}:
        ordinals = [
            int(row["template_record_ordinal"])
            for row in stage2
            if str(row["template_id"]) == template_id
        ]
        assert ordinals == list(range(len(ordinals)))
    for start in range(0, len(stage2), 8):
        checkpoint = stage2[start : start + 8]
        assert len({row["optimizer_arm"] for row in checkpoint}) == 1
        assert len({row["template_id"] for row in checkpoint}) == 1


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
    from our_system_phase2.services.program_tournament_freeze_v1 import (
        verify_phase_freeze_v1,
    )

    assert verify_phase_freeze_v1(target)["status"].endswith("PHASE_FROZEN")
    assert frozen["main_record_count"] == 56
    assert frozen["minimum_base_identities_per_template"] == 8
    assert frozen["same_campaign_optimizer_state_carried_forward"] is True
    assert frozen["development_financial_observations_imported"] is False
    assert frozen["prior_program_exact_identity_count"] == 0
