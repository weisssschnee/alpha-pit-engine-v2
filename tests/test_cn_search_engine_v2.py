from __future__ import annotations

from collections import Counter
import json
from pathlib import Path

import pytest

from our_system_phase2.runtime.cn_joint_program_search_v2_canary import (
    ARM_CONDITIONAL_UPLIFT,
    ARM_NOVELTY,
    ARM_UNIFORM,
    CANARY_AUTHORIZATION_RELATIVE_PATH,
    CANARY_PROFILE,
    EXPECTED_RECORDS,
    PROSPECTIVE_SUCCESS_GATES,
    build_ask_plan_v1,
    canary_provenance_v1,
    evaluate_prospective_canary_v1,
    verify_canary_authorization,
)
from our_system_phase2.runtime import (
    cn_joint_program_search_v2_canary as search_v2_route,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    ProgramProposalReceiptV0,
)
from our_system_phase2.services.evaluation_asset_authority import (
    EvaluationAssetDenied,
    verify_historical_challenge_destructive_use,
    verify_search_feedback_boundary,
)
from our_system_phase2.services.search_v2_admission import (
    AbsoluteEconomicAdmission,
)
from our_system_phase2.services.search_v2_conditional_uplift import (
    COMPONENT_ATTRIBUTION_UNIDENTIFIED,
    MATCHED_CONTROL_CONTRACT_ID,
    conditional_uplift_credit,
)
from our_system_phase2.services.search_v2_scheduler import SearchV2SchedulerV1
from our_system_phase2.services.project_control_admission import (
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
    ProjectControlDenied,
    clear_active_admission,
)
from our_system_phase2.services.unified_capability_registry import stable_hash
from our_system_phase2.services import search_v2_canary_freeze as search_v2_freeze
from scripts import run_cn_joint_program_search_v2_canary as search_v2_runner


REPO = Path(__file__).resolve().parents[1]


def _receipt(index: int = 0) -> ProgramProposalReceiptV0:
    return ProgramProposalReceiptV0(
        adapter_version="cn_candidate_program_proposal_adapter_v0",
        join_policy_id="CN_CROSS_SECTIONAL_BASE_WITH_TYPED_ENHANCERS_V0",
        batch_id="SEARCH_V2_SYNTHETIC",
        ask_ordinal=index,
        generation_arm=ARM_CONDITIONAL_UPLIFT,
        program_template_id="BASE_TEMPORAL",
        component_candidate_ids={"base": "base-1", "temporal": "temporal-1"},
        component_control_ids={"base": "base-c", "temporal": "temporal-c"},
        component_pair_ids={"base": "pair-b", "temporal": "pair-t"},
        component_route_ids={"base": "MINUTE_STATIC", "temporal": "SLOW_TEMPORAL_CHANGE"},
        component_proposal_ids={"base": "p-base", "temporal": "p-temporal"},
        component_trial_numbers={"base": None, "temporal": 1},
        component_sampling_phases={"base": "AVAILABILITY_FIXED", "temporal": "TPE_GUIDED"},
        component_generation_receipt_hashes={"base": "a" * 64, "temporal": "b" * 64},
        component_credit_eligible={"base": True, "temporal": True},
        combination_policy={
            "temporal": "ADD",
            "market": "FILTER",
            "event_episode": "SOURCE_ROUTE_EPISODE",
            "event_application": "FILTER",
        },
        semantic_program_hash=f"semantic-{index}",
        program_id=f"program-{index}",
    )


def _record(
    *,
    reward: float = 0.25,
    cumulative_return: float = 0.12,
    matched_reward: float = 0.04,
    matched_return: float = 0.03,
    blocked: bool = False,
) -> dict:
    primary_windows = [0.05, 0.04, -0.01]
    control_windows = [0.03, 0.03, -0.005]
    record = {
        "schema_version": "cn_joint_program_phase_c_record_v0",
        "status": "JOINT_PROGRAM_PHASE_C_RECORD_CLOSED_IMMUTABLE",
        "main_record_ordinal": 0,
        "template_id": "BASE_TEMPORAL",
        "generation_arm": ARM_CONDITIONAL_UPLIFT,
        "record_kind": "ENHANCED_FULL_BASE_PAIR",
        "program_id": "program-0",
        "control_program_id": "program-0::BASE_CONTROL",
        "pair_id": "pair-0",
        "control_contract_valid": True,
        "compile_status": "PASS",
        "physical_ready": True,
        "dag_ready": True,
        "semantic_noop": False,
        "replay_status": "PAIR_REPLAY_BLOCKED" if blocked else "PAIR_REPLAY_COMPLETE",
        "blockers": ["SYNTHETIC_BLOCKER"] if blocked else [],
        "primary": None,
        "base_control": None,
        "matched_net_reward_increment": None if blocked else matched_reward,
        "matched_cumulative_return_increment": None if blocked else matched_return,
        "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    if not blocked:
        record["primary"] = {
            "continuous_book_net_reward": reward,
            "cumulative_net_return": cumulative_return,
            "net_return_per_turnover": 0.4,
            "mean_one_way_turnover": 0.30,
            "fill_count": 10,
            "development_subwindows": [
                {"window_id": f"w{i + 1}", "cumulative_net_return": value}
                for i, value in enumerate(primary_windows)
            ],
        }
        record["base_control"] = {
            "continuous_book_net_reward": reward - matched_reward,
            "cumulative_net_return": cumulative_return - matched_return,
            "net_return_per_turnover": 0.3,
            "mean_one_way_turnover": 0.20,
            "fill_count": 10,
            "development_subwindows": [
                {"window_id": f"w{i + 1}", "cumulative_net_return": value}
                for i, value in enumerate(control_windows)
            ],
        }
    record["record_payload_sha256"] = stable_hash(record)
    return record


def _admit(record: dict) -> AbsoluteEconomicAdmission:
    return AbsoluteEconomicAdmission.evaluate(
        record,
        expected_pair_id="pair-0",
        expected_program_id="program-0",
        expected_control_program_id="program-0::BASE_CONTROL",
    )


def test_absolute_admission_reuses_authoritative_fields_as_a_gate() -> None:
    admission = _admit(_record())
    assert admission.admitted is True
    assert admission.failure_reasons == ()
    assert admission.metrics["primary_continuous_book_net_reward"] == 0.25
    assert admission.metrics["positive_development_window_count"] == 2
    assert "matched_net_reward_increment" not in admission.metrics


@pytest.mark.parametrize(
    "mutator,reason",
    [
        (lambda row: row.update(replay_status="PAIR_REPLAY_BLOCKED"), "REPLAY_NOT_COMPLETE"),
        (lambda row: row.update(blockers=["SYNTHETIC_BLOCKER"]), "CANDIDATE_LOCAL_BLOCKER"),
        (lambda row: row["primary"].update(continuous_book_net_reward=-1.0), "PRIMARY_NET_REWARD_NOT_POSITIVE"),
        (lambda row: row["primary"].update(cumulative_net_return=-1.0), "PRIMARY_CUMULATIVE_RETURN_NOT_POSITIVE"),
        (lambda row: row["primary"].update(net_return_per_turnover=float("nan")), "TURNOVER_EFFICIENCY_INVALID"),
        (lambda row: row["primary"].pop("fill_count"), "SUPPORT_OR_COVERAGE_INVALID"),
    ],
)
def test_absolute_admission_missing_or_failed_economics_fail_closed(mutator, reason) -> None:
    row = _record()
    row.pop("record_payload_sha256")
    mutator(row)
    row["record_payload_sha256"] = stable_hash(row)
    admission = _admit(row)
    assert admission.admitted is False
    assert reason in admission.failure_reasons


def test_bad_standalone_with_strong_relative_uplift_gets_no_enhancer_credit() -> None:
    row = _record(reward=-0.5, cumulative_return=-0.2, matched_reward=10.0, matched_return=9.0)
    admission = _admit(row)
    assert admission.admitted is False
    assert conditional_uplift_credit(row, admission) is None


def test_strong_base_with_useless_enhancer_passes_admission_without_rewarding_enhancer() -> None:
    row = _record(reward=1.0, cumulative_return=0.5, matched_reward=-0.02, matched_return=-0.01)
    admission = _admit(row)
    credit = conditional_uplift_credit(row, admission)
    assert admission.admitted is True
    assert credit is not None
    assert credit.program_credit["matched_net_reward_increment"] == -0.02
    assert credit.program_credit["matched_cumulative_net_return_increment"] == -0.01
    assert credit.component_attribution_status == COMPONENT_ATTRIBUTION_UNIDENTIFIED
    assert credit.component_credits is None

    scheduler = SearchV2SchedulerV1.fresh("search-v2-test", canary_provenance_v1())
    for _ in range(4):
        scheduler.observe(_receipt(0), admission, credit)
    score = scheduler.score_receipt(_receipt(0))
    assert score["admission_head"]["probability"] > 0.5
    assert score["conditional_uplift_head"]["positive_uplift"] is False
    assert score["conditional_uplift_exploit_eligible"] is False


def test_conditional_credit_rejects_identity_and_matched_control_drift() -> None:
    row = _record()
    admission = _admit(row)
    drifted = dict(row)
    drifted["pair_id"] = "other-pair"
    with pytest.raises(ValueError, match="identity drift"):
        conditional_uplift_credit(drifted, admission)
    drifted = dict(row)
    drifted["matched_control_contract_id"] = "other-contract"
    with pytest.raises(ValueError, match="matched-control semantics drift"):
        conditional_uplift_credit(drifted, admission)


def test_scheduler_is_fresh_campaign_local_and_exactly_restorable() -> None:
    provenance = canary_provenance_v1()
    scheduler = SearchV2SchedulerV1.fresh("campaign-a", provenance)
    assert scheduler.observations == 0
    assert scheduler.conditional_uplift_observations == 0
    snapshot = scheduler.snapshot()
    assert snapshot["serialized_optimizer_state_imported"] is False
    assert snapshot["development_financial_observations_imported"] is False
    assert snapshot["cross_campaign_state_import_allowed"] is False
    assert SearchV2SchedulerV1.restore(snapshot, expected_campaign_id="campaign-a").snapshot() == snapshot
    with pytest.raises(ValueError, match="campaign identity drift"):
        SearchV2SchedulerV1.restore(snapshot, expected_campaign_id="campaign-b")

    admission = _admit(_record())
    credit = conditional_uplift_credit(_record(), admission)
    scheduler.observe(_receipt(0), admission, credit)
    observed_snapshot = scheduler.snapshot()
    assert SearchV2SchedulerV1.restore(
        observed_snapshot, expected_campaign_id="campaign-a"
    ).snapshot() == observed_snapshot


def test_runner_observes_failed_admission_without_creating_enhancer_credit() -> None:
    record = _record(
        reward=-0.5,
        cumulative_return=-0.2,
        matched_reward=10.0,
        matched_return=9.0,
    )
    schedule = {
        "main_record_ordinal": 0,
        "pair_id": "pair-0",
        "primary_program": {"program_id": "program-0"},
        "control_program": {"program_id": "program-0::BASE_CONTROL"},
        "proposal_receipt": _receipt(0).to_record(),
    }
    scheduler = SearchV2SchedulerV1.fresh("runner-test", canary_provenance_v1())
    ledger = search_v2_runner._feedback_update(
        [record], [schedule], bandit=scheduler, behavior_counts=Counter()
    )
    assert scheduler.observations == 1
    assert scheduler.conditional_uplift_observations == 0
    assert ledger[0]["bandit_update_applied"] is True
    assert ledger[0]["absolute_admission"]["admitted"] is False
    assert ledger[0]["enhancer_credit"] is None
    assert ledger[0]["failed_candidate_negative_enhancer_reward"] is False


def test_shared_evaluator_adapter_preserves_frozen_matched_control_contract() -> None:
    shared = (
        REPO / "scripts" / "run_cn_joint_program_phase_c_v0.py"
    ).read_text(encoding="utf-8")
    assert '"matched_control_contract_id"' in shared
    assert search_v2_runner.ENGINE_CLOSURE_NAME != search_v2_runner.CLOSURE_NAME
    with pytest.raises(PermissionError, match="Direct Search V2 runner invocation"):
        search_v2_runner.main([])


def test_gate_reads_one_manifest_bound_feedback_byte_buffer(tmp_path: Path) -> None:
    feedback_path = tmp_path / "phase_c_bandit_feedback_ledger.jsonl"
    feedback_path.write_text('{"synthetic":true}\n', encoding="utf-8")
    feedback_binding = {
        "path": feedback_path.name,
        "bytes": feedback_path.stat().st_size,
        "sha256": search_v2_freeze.phase_c._sha256(feedback_path),
    }
    manifest = {
        "schema_version": "synthetic_manifest_v1",
        "artifacts": [feedback_binding],
    }
    manifest["artifact_manifest_sha256"] = stable_hash(manifest)
    manifest_path = tmp_path / "ARTIFACT_MANIFEST.json"
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8"
    )
    closure = {
        "artifact_manifest": {
            "path": manifest_path.name,
            "bytes": manifest_path.stat().st_size,
            "sha256": search_v2_freeze.phase_c._sha256(manifest_path),
        }
    }
    assert search_v2_runner._verified_feedback_rows(tmp_path, closure) == [
        {"synthetic": True}
    ]
    feedback_path.write_text('{"synthetic":false}\n', encoding="utf-8")
    with pytest.raises(RuntimeError, match="feedback ledger binding drift"):
        search_v2_runner._verified_feedback_rows(tmp_path, closure)


def test_prefinancial_source_binding_reads_metadata_not_parent_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    freeze_root = tmp_path / "phase-b-freeze"
    result_root = tmp_path / "phase-b-result"
    freeze_root.mkdir()
    result_root.mkdir()
    freeze_closure_path = freeze_root / search_v2_freeze.PHASE_B_FREEZE_CLOSURE_NAME
    freeze_closure_path.write_text('{"zero_financial":true}\n', encoding="utf-8")
    freeze_payload_sha = "f" * 64
    monkeypatch.setattr(
        search_v2_freeze,
        "verify_phase_b_prefinancial_freeze_v0",
        lambda _root: {"closure_sha256": freeze_payload_sha},
    )

    result_closure = {"status": "CN_JOINT_PROGRAM_PHASE_B_COMPLETE"}
    result_closure["closure_payload_sha256"] = stable_hash(result_closure)
    result_closure_path = result_root / search_v2_freeze.PHASE_B_RESULT_CLOSURE_NAME
    result_closure_path.write_text(
        json.dumps(result_closure) + "\n", encoding="utf-8"
    )
    result_file_sha = search_v2_freeze.phase_c._sha256(result_closure_path)
    input_binding = {
        "phase_b_prefinancial_closure_file_sha256": (
            search_v2_freeze.phase_c._sha256(freeze_closure_path)
        ),
        "phase_b_prefinancial_closure_payload_sha256": freeze_payload_sha,
    }
    input_binding["input_binding_sha256"] = stable_hash(input_binding)
    (result_root / "input_binding.json").write_text(
        json.dumps(input_binding) + "\n", encoding="utf-8"
    )
    outcome = {
        "status": "CN_JOINT_PROGRAM_ROLLING_SEARCH_V0_PHASE_B_ACCEPTED",
        "phase_b_root": str(result_root),
        "closure": {
            "file_sha256": result_file_sha,
            "payload_sha256": result_closure["closure_payload_sha256"],
        },
    }
    outcome["receipt_payload_sha256"] = stable_hash(outcome)
    outcome_path = tmp_path / "outcome.json"
    outcome_path.write_text(json.dumps(outcome) + "\n", encoding="utf-8")
    authorization = {
        "source_phase_b_closure_file_sha256": result_file_sha,
        "source_phase_b_closure_payload_sha256": result_closure[
            "closure_payload_sha256"
        ],
    }

    verified_freeze, verified_outcome, verified_input = (
        search_v2_freeze._verify_phase_b_source_authority(
            phase_b_freeze_root=freeze_root,
            phase_b_result_root=result_root,
            phase_b_outcome_path=outcome_path,
            authorization=authorization,
        )
    )
    assert verified_freeze["closure_sha256"] == freeze_payload_sha
    assert verified_outcome["phase_b_root"] == str(result_root)
    assert verified_input["input_binding_sha256"] == input_binding[
        "input_binding_sha256"
    ]
    assert not (result_root / "phase_b_record_results.jsonl").exists()


def test_prefinancial_field_manifest_is_bound_to_accepted_phase_b_input(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "accepted_field_manifest.json"
    manifest_path.write_text('{"fields":["synthetic"]}\n', encoding="utf-8")
    field_manifest = {"manifest_hash": "a" * 64}
    phase_b_input = {
        "train_field_manifest_sha256": search_v2_freeze.phase_c._sha256(
            manifest_path
        ),
        "train_field_manifest_payload_sha256": "a" * 64,
    }
    search_v2_freeze._verify_phase_b_field_manifest_binding(
        accepted_field_manifest_path=manifest_path,
        field_manifest=field_manifest,
        phase_b_input=phase_b_input,
    )
    manifest_path.write_text('{"fields":["swapped"]}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="accepted Phase B input"):
        search_v2_freeze._verify_phase_b_field_manifest_binding(
            accepted_field_manifest_path=manifest_path,
            field_manifest=field_manifest,
            phase_b_input=phase_b_input,
        )


def test_prospective_schedule_is_exact_512_and_frozen_before_observations() -> None:
    rows = build_ask_plan_v1()
    assert len(rows) == EXPECTED_RECORDS == 512
    assert [row["main_record_ordinal"] for row in rows] == list(range(512))
    assert Counter(row["template_id"] for row in rows) == Counter(
        {
            "BASE": 64,
            "BASE_TEMPORAL": 64,
            "BASE_MARKET": 64,
            "BASE_EVENT": 64,
            "BASE_TEMPORAL_MARKET": 64,
            "BASE_TEMPORAL_EVENT": 64,
            "BASE_MARKET_EVENT": 64,
            "BASE_TEMPORAL_MARKET_EVENT": 64,
        }
    )
    assert Counter(row["generation_arm"] for row in rows) == Counter(
        {ARM_UNIFORM: 288, ARM_CONDITIONAL_UPLIFT: 168, ARM_NOVELTY: 56}
    )
    assert all(
        row["generation_arm"] == ARM_UNIFORM
        for row in rows
        if row["template_id"] == "BASE"
    )
    for template_id in {
        row["template_id"] for row in rows if row["template_id"] != "BASE"
    }:
        assert Counter(
            row["generation_arm"]
            for row in rows
            if row["template_id"] == template_id
        ) == Counter(
            {ARM_UNIFORM: 32, ARM_CONDITIONAL_UPLIFT: 24, ARM_NOVELTY: 8}
        )
    for checkpoint_ordinal in range(64):
        checkpoint = [
            row for row in rows if row["checkpoint_ordinal"] == checkpoint_ordinal
        ]
        assert len(checkpoint) == 8
        assert len({row["template_id"] for row in checkpoint}) == 1
        expected = (
            Counter({ARM_UNIFORM: 8})
            if checkpoint[0]["template_id"] == "BASE"
            else Counter(
                {ARM_UNIFORM: 4, ARM_CONDITIONAL_UPLIFT: 3, ARM_NOVELTY: 1}
            )
        )
        assert Counter(row["generation_arm"] for row in checkpoint) == expected
    assert not any(row["adaptive_budget_reallocation_allowed"] for row in rows)
    provenance = canary_provenance_v1()
    assert provenance["development_observation_count"] == 0
    assert provenance["serialized_optimizer_state_imported"] is False
    assert provenance["development_financial_observations_imported"] is False
    assert provenance["objective_designed_after_parent_results"] is True
    assert provenance["manual_diagnosis_imported"] is True
    assert provenance["cross_campaign_development_feedback"] is True


def test_success_gate_is_dual_and_cannot_trade_absolute_against_uplift() -> None:
    assert PROSPECTIVE_SUCCESS_GATES["decision_rule"] == (
        "NONINFERIOR_ABSOLUTE_AND_SUPERIOR_CONDITIONAL_UPLIFT"
    )
    assert PROSPECTIVE_SUCCESS_GATES["tradeoff_between_heads_allowed"] is False
    assert PROSPECTIVE_SUCCESS_GATES["absolute_noninferiority"]["margin"] == 0.0
    assert PROSPECTIVE_SUCCESS_GATES["conditional_superiority"]["minimum_improved_templates"] == 4


def _synthetic_gate_feedback(*, exploit_turnover_efficiency: float) -> list[dict]:
    rows: list[dict] = []
    for template_id in (
        "BASE_TEMPORAL",
        "BASE_MARKET",
        "BASE_EVENT",
        "BASE_TEMPORAL_MARKET",
        "BASE_TEMPORAL_EVENT",
        "BASE_MARKET_EVENT",
        "BASE_TEMPORAL_MARKET_EVENT",
    ):
        for arm, quota, uplift, turnover in (
            (ARM_UNIFORM, 32, 0.01, 0.4),
            (
                ARM_CONDITIONAL_UPLIFT,
                24,
                0.02,
                exploit_turnover_efficiency,
            ),
        ):
            for _ in range(quota):
                rows.append(
                    {
                        "template_id": template_id,
                        "generation_arm": arm,
                        "absolute_admission": {
                            "admitted": True,
                            "failure_reasons": [],
                            "metrics": {
                                "primary_continuous_book_net_reward": 0.5,
                                "primary_cumulative_net_return": 0.2,
                                "primary_net_return_per_turnover": turnover,
                                "positive_development_window_count": 3,
                            },
                        },
                        "enhancer_credit": {
                            "program_credit": {
                                "matched_net_reward_increment": uplift,
                                "matched_cumulative_net_return_increment": uplift,
                                "cross_window_matched_consistency": (
                                    0.9
                                    if arm == ARM_CONDITIONAL_UPLIFT
                                    else 0.8
                                ),
                            }
                        },
                    }
                )
    return rows


def test_frozen_gate_is_machine_applied_and_disallows_head_tradeoff() -> None:
    passed = evaluate_prospective_canary_v1(
        _synthetic_gate_feedback(exploit_turnover_efficiency=0.4)
    )
    assert passed["decision"] == "PASS"
    assert all(passed["absolute_checks"].values())
    assert all(passed["conditional_checks"].values())

    failed = evaluate_prospective_canary_v1(
        _synthetic_gate_feedback(exploit_turnover_efficiency=0.2)
    )
    assert failed["conditional_checks"]["strict_global_superiority"] is True
    assert failed["absolute_checks"]["turnover_efficiency_noninferior"] is False
    assert failed["decision"] == "FAIL"


def test_committed_authorization_is_frozen_not_run_and_self_hashed() -> None:
    authorization = verify_canary_authorization(REPO / CANARY_AUTHORIZATION_RELATIVE_PATH)
    assert authorization["campaign_profile"] == CANARY_PROFILE
    assert authorization["canary_status"] == "FROZEN_NOT_RUN"
    assert authorization["execution_authorized"] is True
    assert authorization["ask_plan_sha256"] == stable_hash(list(build_ask_plan_v1()))
    assert authorization["financial_data_reads"] == 0


def test_wrong_canary_profile_and_missing_project_control_fail_closed(
    tmp_path: Path,
) -> None:
    authorization = search_v2_route.authorization_payload_v1()
    authorization["campaign_profile"] = "wrong-profile"
    authorization.pop("authorization_payload_sha256")
    authorization["authorization_payload_sha256"] = stable_hash(authorization)
    wrong = tmp_path / "wrong-profile.json"
    wrong.write_text(json.dumps(authorization) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="authorization contract drift"):
        verify_canary_authorization(wrong)

    clear_active_admission()
    with pytest.raises(
        ProjectControlDenied, match="DIRECT_HIGH_COST_MODULE_EXECUTION_FORBIDDEN"
    ):
        search_v2_route.main([])


def test_project_control_and_asset_boundaries_cover_search_v2() -> None:
    app_source = (REPO / "app.py").read_text(encoding="utf-8")
    control_source = (
        REPO / "src" / "our_system_phase2" / "services" / "project_control_admission.py"
    ).read_text(encoding="utf-8")
    module_source = (
        REPO / "src" / "our_system_phase2" / "runtime" / "cn_joint_program_search_v2_canary.py"
    ).read_text(encoding="utf-8")
    launcher = (
        REPO / "scripts" / "run_cn_joint_program_search_v2_77o.ps1"
    ).read_text(encoding="utf-8")
    assert '"cn-joint-program-search-v2-canary"' in app_source
    assert '"cn-joint-program-search-v2-canary"' in control_source
    assert "cn-joint-program-search-v2-canary" in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES
    assert "consume_active_admission(" in module_source
    assert "verify_campaign_authorization_binding(" in module_source
    assert "verify_consumed_admission_target(" in module_source
    assert "--project-control-admission" in launcher
    assert "--project-control-admission-sha256" in launcher
    assert "--campaign-authorization" in launcher

    with pytest.raises(EvaluationAssetDenied):
        verify_historical_challenge_destructive_use(
            role_registry_path=REPO / "runtime" / "run_plans" / "evaluation_data_roles_v1.json",
            access_started_path=REPO / "runtime" / "run_plans" / "cn_historical_challenge_2023_access_started.json",
            outcome_path=REPO / "runtime" / "run_plans" / "cn_fixed10_historical_challenge_2023_outcome_20260806.json",
        )
    boundary = verify_search_feedback_boundary(
        role_registry_path=REPO / "runtime" / "run_plans" / "evaluation_data_roles_v1.json",
        access_started_path=REPO / "runtime" / "run_plans" / "cn_historical_challenge_2023_access_started.json",
        outcome_path=REPO / "runtime" / "run_plans" / "cn_fixed10_historical_challenge_2023_outcome_20260806.json",
    )
    assert boundary["historical_2023_state"] == "SPENT"
    assert boundary["historical_2023_result"] == "NEGATIVE"
    assert boundary["historical_2023_retry"] == "FORBIDDEN"
    assert boundary["forward_b_state"] == "SEALED"
    assert boundary["financial_data_reads"] == 0
