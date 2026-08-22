from __future__ import annotations

import json
from pathlib import Path

import app
from scripts import build_cn_program_base_event_replication_study_authorization_v1 as auth_builder
from scripts import run_cn_program_base_event_replication_study_v1 as r
from our_system_phase2.runtime import cn_program_base_event_replication_study_v1 as runtime
from our_system_phase2.services.project_control_admission import ACTION_LAUNCH, ACTION_RETRY, CAMPAIGN_AUTHORIZATION_BOUND_ROUTES, sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "runtime/run_plans/cn_program_base_event_replication_study_plan.json"
POOL = ROOT / "runtime/run_plans/cn_program_base_event_replication_pools_review_20260822.json"
REVIEW = ROOT / "runtime/run_plans/cn_program_base_event_replication_study_review_20260822.json"


def test_replication_route_registered_high_cost_and_authorization_bound() -> None:
    route = "cn-program-base-event-replication-study-v1"
    assert app.ROUTES[route] == "our_system_phase2.runtime.cn_program_base_event_replication_study_v1"
    assert app.HIGH_COST_ROUTE_ACTIONS[route] == {ACTION_LAUNCH, ACTION_RETRY}
    assert route in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_replication_plan_freezes_two_same_depth_disjoint_confirmations() -> None:
    plan = r.verify_plan(PLAN)
    assert plan["focus_template"] == "BASE_EVENT"
    assert plan["anchor_v3_used_in_prospective_gate"] is False
    assert plan["budget"] == {
        "checkpoint_count": 10,
        "checkpoint_size": 24,
        "hard_cap_logical_records": 240,
        "primitive_records": 144,
        "records_per_replicate": 120,
        "replicate_count": 2,
        "typed_evolution_records": 48,
        "uniform_records": 48,
    }
    reps = {row["replicate_id"]: row for row in plan["replicates"]}
    assert reps["CONFIRM_A"]["pool_id"] == "REPL_1"
    assert reps["CONFIRM_A"]["raw_offset"] == 3584
    assert reps["CONFIRM_A"]["arm_order"] == [r.U, r.P, r.P, r.E, r.P]
    assert reps["CONFIRM_B"]["pool_id"] == "REPL_2"
    assert reps["CONFIRM_B"]["raw_offset"] == 5120
    assert reps["CONFIRM_B"]["arm_order"] == [r.E, r.P, r.P, r.P, r.U]
    assert [row["arm"] for row in plan["schedule"][:5]] == reps["CONFIRM_A"]["arm_order"]
    assert [row["arm"] for row in plan["schedule"][5:]] == reps["CONFIRM_B"]["arm_order"]


def test_replication_pool_review_is_zero_financial_disjoint_and_capacity_bound() -> None:
    pool = json.loads(POOL.read_text(encoding="utf-8-sig"))
    body = dict(pool)
    claim = body.pop("review_payload_sha256")
    assert stable_hash(body) == claim
    assert pool["status"] == "ZERO_FINANCIAL_BASE_EVENT_REPLICATION_POOLS_INSUFFICIENT_UNDER_FULL_POOL_RESERVATION"
    assert pool["candidate_evaluation_executed"] is False
    assert pool["financial_labels_read"] is False
    assert pool["effective_spent_exact_count"] == 8654
    assert pool["reserved_exact_count"] == 589
    assert all(pool[key] == 0 for key in ("validation_reads", "holdout_reads", "historical_2023_reads", "forward_b_reads", "forward_2026_reads"))
    reps = {row["replicate_id"]: row for row in pool["replicate_pools"]}
    assert set(reps) == {"REPL_1", "REPL_2"}
    assert len(reps["REPL_1"]["entries"]) == 433
    assert len(reps["REPL_2"]["entries"]) == 156
    assert reps["REPL_1"]["max4_capacity"] == 128
    assert reps["REPL_2"]["max4_capacity"] == 123
    raw_a = {row["exact_identity"] for row in reps["REPL_1"]["entries"]}
    raw_b = {row["exact_identity"] for row in reps["REPL_2"]["entries"]}
    norm_a = {row["normalized_exact_identity"] for row in reps["REPL_1"]["entries"]}
    norm_b = {row["normalized_exact_identity"] for row in reps["REPL_2"]["entries"]}
    assert not (raw_a & raw_b)
    assert not (norm_a & norm_b)


def test_replication_study_review_rejects_fake_scale_and_pool_cherry_picking() -> None:
    review = json.loads(REVIEW.read_text(encoding="utf-8-sig"))
    body = dict(review)
    claim = body.pop("review_payload_sha256")
    assert stable_hash(body) == claim
    assert review["status"] == "BASE_EVENT_REPLICATION_STUDY_REVIEW_FROZEN"
    assert review["decision"] == "REJECT_SINGLE_480_AND_THREE_NEW_POOL_SCALE; USE_V3_AS_RETROSPECTIVE_ANCHOR_PLUS_TWO_NEW_PROSPECTIVE_ORDER_ROTATED_CONFIRMATIONS"
    scan = review["pool_scan_decision"]
    assert scan["pool_score_used_to_choose_offset"] is False
    assert scan["three_new_pool_design_feasible"] is False
    assert scan["two_new_pool_confirmation_feasible"] is True
    assert [(row["replicate_id"], row["raw_offset"], row["max4_capacity"]) for row in scan["accepted_new_pools"]] == [
        ("REPL_1", 3584, 128),
        ("REPL_2", 5120, 123),
    ]
    assert review["v3_anchor"]["used_in_new_prospective_gate"] is False
    assert review["new_candidate_evaluation_executed"] is False
    assert all(review[key] == 0 for key in ("validation_reads", "holdout_reads", "historical_2023_reads", "forward_b_reads", "forward_2026_reads"))


def test_replication_gate_and_resource_contract_are_frozen() -> None:
    plan = r.verify_plan(PLAN)
    assert plan["prospective_gate"] == {
        "aggregate_primitive_minus_best_control_productive_rate_min": 0.08,
        "aggregate_primitive_minus_best_control_uplift_stable_rate_min": 0.10,
        "aggregate_primitive_productive_rate_min": 0.45,
        "aggregate_primitive_to_best_control_productive_ratio_min": 1.20,
        "aggregate_primitive_to_best_control_uplift_stable_ratio_min": 1.40,
        "aggregate_primitive_uplift_stable_2of3_rate_min": 0.35,
        "behavior_pair_rate_min": 0.75,
        "each_replicate_primitive_productive_rate_min": 0.30,
        "each_replicate_primitive_uplift_stable_2of3_rate_min": 0.25,
        "effective_spent_overlap_count_required": 0,
        "replicate_directional_policy_win_count_min": 2,
        "restricted_reads_required_zero": True,
    }
    rc = plan["resource_contract"]
    assert rc["evaluator_pool_lifetime"] == "PERSISTENT_RUN_SCOPE"
    assert rc["primary_executor_workers"] == 24
    assert rc["fallback_executor_workers"] == 16
    assert rc["minimum_records_per_hour_after_first_checkpoint"] == 650.0
    assert rc["throughput_enforcement_after_warm_checkpoints"] == 2
    assert rc["wall_clock_budget_minutes"] == 40
    assert rc["minimum_free_memory_bytes"] == 24 * 1024**3


def _feedback(arm: str, ordinal: int, *, productive: bool, stable: bool) -> dict:
    return {
        "generation_arm": arm,
        "main_record_ordinal": ordinal,
        "absolute_admission": {"admitted": productive},
        "enhancer_credit": {
            "program_credit": {
                "matched_cumulative_net_return_increment": 1.0 if productive else -1.0,
                "matched_net_reward_increment": 1.0 if productive else -1.0,
                "cross_window_positive_increment_count": 2 if stable else (1 if productive else 0),
            }
        },
    }


def _synthetic_inputs(*, strong_best_control: bool = False, weak_second_replicate: bool = False) -> tuple[list[dict], list[dict]]:
    plan = r.verify_plan(PLAN)
    feedback: list[dict] = []
    records: list[dict] = []
    for block in plan["schedule"]:
        rid = block["replicate_id"]
        arm = block["arm"]
        start = block["start_ordinal"]
        if arm == r.P:
            # 40/72 productive and 32/72 stable by default; a weak second replicate
            # intentionally falls below the frozen per-replicate absolute gates.
            replicate_local_start = sum(
                24
                for prior in plan["schedule"]
                if prior["replicate_id"] == rid and prior["arm"] == r.P and prior["checkpoint"] < block["checkpoint"]
            )
            prod_limit = 20 if (weak_second_replicate and rid == "CONFIRM_B") else 40
            stable_limit = 16 if (weak_second_replicate and rid == "CONFIRM_B") else 32
            for i in range(24):
                j = replicate_local_start + i
                prod = j < prod_limit
                stab = prod and j < stable_limit
                feedback.append(_feedback(arm, start + i, productive=prod, stable=stab))
                records.append({"primary": {"behavior_identity": f"p-{start+i}"}, "base_control": {"behavior_identity": f"c-{start+i}"}})
        else:
            prod_limit = 13 if (strong_best_control and arm == r.E) else (8 if arm == r.U else 6)
            stable_limit = 8 if (strong_best_control and arm == r.E) else (4 if arm == r.U else 3)
            for i in range(24):
                prod = i < prod_limit
                stab = prod and i < stable_limit
                feedback.append(_feedback(arm, start + i, productive=prod, stable=stab))
                records.append({"primary": {"behavior_identity": f"p-{start+i}"}, "base_control": {"behavior_identity": f"c-{start+i}"}})
    feedback.sort(key=lambda x: x["main_record_ordinal"])
    records.sort(key=lambda x: int(x["primary"]["behavior_identity"].split("-")[1]))
    return feedback, records


def test_replication_gate_passes_two_directional_confirmations() -> None:
    plan = r.verify_plan(PLAN)
    feedback, records = _synthetic_inputs()
    result = r._gate(plan, feedback, records)
    assert result["status"] == r.PASS_STATUS
    assert all(result["checks"].values())
    assert result["replicate_directional_policy_win_count"] == 2
    assert result["anchor_v3_used_in_prospective_gate"] is False
    assert result["aggregate"]["best_control_productive_rate"] == result["aggregate"]["uniform"]["productive_efficiency"]


def test_replication_gate_uses_best_control_not_combined_control() -> None:
    plan = r.verify_plan(PLAN)
    feedback, records = _synthetic_inputs(strong_best_control=True)
    result = r._gate(plan, feedback, records)
    assert result["aggregate"]["best_control_productive_rate"] == result["aggregate"]["typed_evolution"]["productive_efficiency"]
    assert result["checks"]["aggregate_primitive_to_best_control_productive_ratio"] is False
    assert result["checks"]["aggregate_primitive_minus_best_control_productive_rate"] is False
    assert result["status"] == r.FAIL_STATUS


def test_replication_gate_requires_both_replicates_to_hold_absolute_yield() -> None:
    plan = r.verify_plan(PLAN)
    feedback, records = _synthetic_inputs(weak_second_replicate=True)
    result = r._gate(plan, feedback, records)
    assert result["checks"]["each_replicate_primitive_productive_rate"] is False
    assert result["checks"]["each_replicate_primitive_uplift_stable"] is False
    assert result["replicate_directional_policy_win_count"] < 2
    assert result["status"] == r.FAIL_STATUS


def test_replication_runtime_verifier_binds_runner_plan_and_v3_redirect() -> None:
    source = (ROOT / "src/our_system_phase2/runtime/cn_program_base_event_replication_study_v1.py").read_text(encoding="utf-8-sig")
    assert 'ROUTE_ID = "cn-program-base-event-replication-study-v1"' in source
    assert 'runner = root / "scripts/run_cn_program_base_event_replication_study_v1.py"' in source
    assert "source_v3_terminal_audit" in source
    assert "source_v3_postrun_outcome" in source
    assert "BASE_EVENT_ONLY_SYSTEM_SCALE_DEVELOPMENT_SEARCH_REVIEW" in source
    assert "consume_active_admission(ROUTE_ID, {ACTION_LAUNCH, ACTION_RETRY})" in source


def test_replication_authorization_binds_dynamic_prefinancial_field_projection(tmp_path: Path) -> None:
    plan = r.verify_plan(PLAN)
    canary = {
        "schema_version": "synthetic_base_event_replication_canary",
        "status": "PASS",
        "candidate_evaluation_executed": False,
        "preview_selected_count": 240,
        "field_column_count": 41,
        "field_columns_sha256": "synthetic-41-field-hash",
        "evaluator_pool_lifetime": "PERSISTENT_RUN_SCOPE",
        "replication_plan_payload_sha256": plan["plan_payload_sha256"],
        "pool_review_payload_sha256": plan["pool_review"]["payload_sha256"],
        "study_review_payload_sha256": plan["study_review"]["payload_sha256"],
        "source_v3_terminal_audit_payload_sha256": plan["source_v3_terminal_audit"]["payload_sha256"],
        "source_v3_postrun_outcome_payload_sha256": plan["source_v3_postrun_outcome"]["payload_sha256"],
        "acceleration_accuracy_audit_payload_sha256": plan["acceleration_accuracy_audit"]["payload_sha256"],
        "runner_source_file_sha256": sha256_file(ROOT / "scripts/run_cn_program_base_event_replication_study_v1.py"),
    }
    canary["official_canary_payload_sha256"] = stable_hash(canary)
    canary_path = tmp_path / "canary.json"
    canary_path.write_text(json.dumps(canary, sort_keys=True), encoding="utf-8")
    authorization = auth_builder.build(ROOT, canary_path)
    assert authorization["resource_contract"]["field_column_count"] == 41
    assert authorization["resource_contract"]["field_columns_sha256"] == "synthetic-41-field-hash"
    assert authorization["official_resource_canary"]["field_column_count"] == 41
    auth_path = tmp_path / "authorization.json"
    auth_path.write_text(json.dumps(authorization, sort_keys=True), encoding="utf-8")
    verified = runtime.verify_authorization(auth_path, repo_root=ROOT)
    checked = runtime.verify_official_canary(canary_path, verified, repo_root=ROOT)
    assert checked["field_column_count"] == 41
