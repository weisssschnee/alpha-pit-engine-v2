from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import app
from scripts import build_cn_program_primitive_market_successor_v3_authorization_v1 as auth_builder
from scripts import run_cn_program_primitive_market_successor_v3 as r
from our_system_phase2.runtime import cn_program_primitive_market_successor_v3 as runtime
from our_system_phase2.services.project_control_admission import CAMPAIGN_AUTHORIZATION_BOUND_ROUTES, sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "runtime/run_plans/cn_program_primitive_market_successor_v3_plan.json"
FOCUS = ("BASE_EVENT", "BASE_TEMPORAL_MARKET_EVENT", "BASE_MARKET_EVENT")


def test_v3_route_registered_high_cost_and_authorization_bound() -> None:
    route = "cn-program-primitive-market-successor-v3"
    assert app.ROUTES[route] == "our_system_phase2.runtime.cn_program_primitive_market_successor_v3"
    assert route in app.HIGH_COST_ROUTE_ACTIONS
    assert route in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_v3_plan_fresh_spent_and_frozen_stats_contract() -> None:
    plan = r.verify_plan(PLAN)
    assert plan["budget"]["hard_cap_logical_records"] == 360
    assert plan["fresh_supply"]["raw_ordinal_offset"] == 3072
    assert plan["fresh_supply"]["effective_spent_exact_count"] == 8294
    assert plan["fresh_supply"]["fresh_unique_count"] == 3302
    assert all(plan["fresh_supply"]["per_template_fresh"][t] >= 120 for t in FOCUS)
    authority = plan["search_authority"]
    assert authority["production_results_in_stats"] == 0
    assert authority["v1_successor_results_in_stats"] == 0
    assert authority["v2_successor_results_in_stats"] == 0
    assert authority["production_feedback_imported_into_primitive_stats"] is False
    assert authority["v1_successor_feedback_imported_into_primitive_stats"] is False
    assert authority["v2_successor_feedback_imported_into_primitive_stats"] is False
    assert authority["diversity_contract_changed"] is False


def test_v3_binds_v2_redirect_not_v1_parity_lane() -> None:
    plan = r.verify_plan(PLAN)
    assert plan["source_v2_terminal_audit"]["gate_status"] == "PRIMITIVE_MARKET_SUCCESSOR_V2_TRANSFER_FAIL"
    assert plan["source_v2_terminal_audit"]["failed_gate_checks"] == ["each_core_template_uplift_stable"]
    assert plan["source_v2_postrun_outcome"]["recommendation"] == "REDIRECT"
    assert plan["source_v2_focus_redirect_evidence"]["redirect_focus_templates"] == list(FOCUS)
    assert plan["source_v2_focus_redirect_evidence"]["removed_focus_template"] == "BASE_MARKET"
    assert "source_v1_acceleration_full_parity" not in plan


def test_v3_controlled_frontier_geometry() -> None:
    plan = r.verify_plan(PLAN)
    schedule = plan["schedule"]

    def n(template: str, arm: str) -> int:
        return sum(
            int(row["checkpoint_size"])
            for row in schedule
            if row["template"] == template and row["arm"] == arm
        )

    assert set(row["template"] for row in schedule) == set(FOCUS)
    assert all(n(t, r.P) == 72 for t in FOCUS)
    assert all(n(t, r.U) == 24 for t in FOCUS)
    assert all(n(t, r.E) == 24 for t in FOCUS)
    assert sum(int(row["checkpoint_size"]) for row in schedule) == 360
    assert plan["budget"] == {
        "checkpoint_count": 15,
        "checkpoint_size": 24,
        "focus_records": 360,
        "hard_cap_logical_records": 360,
        "per_focus_template_records": 120,
        "primitive_records": 216,
        "typed_evolution_records": 72,
        "uniform_records": 72,
    }


def test_v3_template_ordinals_are_campaign_global() -> None:
    plan = r.verify_plan(PLAN)
    seen: dict[str, int] = {}
    for row in plan["schedule"]:
        assert row["template_start_ordinal"] == seen.get(row["template"], 0)
        asks = r._ask_rows(row)
        assert asks[0]["template_record_ordinal"] == row["template_start_ordinal"]
        assert asks[-1]["template_record_ordinal"] == row["template_start_ordinal"] + 23
        seen[row["template"]] = row["template_start_ordinal"] + 24


def test_v3_gate_and_throughput_contract_are_frozen() -> None:
    plan = r.verify_plan(PLAN)
    gate = plan["prospective_gate"]
    assert gate["focus_primitive_productive_rate_min"] == 0.40
    assert gate["focus_primitive_uplift_stable_2of3_rate_min"] == 0.30
    assert gate["each_focus_template_primitive_productive_rate_min"] == 0.30
    assert gate["each_focus_template_primitive_uplift_stable_2of3_rate_min"] == 0.25
    assert gate["each_focus_template_primitive_to_controls_productive_ratio_min"] == 1.20
    assert gate["each_focus_template_primitive_to_controls_uplift_stable_ratio_min"] == 1.20
    assert gate["total_productive_count_min"] == 100
    assert gate["behavior_pair_rate_min"] == 0.70
    resource = plan["resource_contract"]
    assert resource["evaluator_pool_lifetime"] == "PERSISTENT_RUN_SCOPE"
    assert resource["primary_executor_workers"] == 24
    assert resource["fallback_executor_workers"] == 16
    assert resource["minimum_records_per_hour_after_first_checkpoint"] == 650.0
    assert resource["throughput_enforcement_after_warm_checkpoints"] == 2
    assert resource["minimum_free_memory_bytes"] == 24 * 1024**3


def _feedback(template: str, arm: str, ordinal: int, *, productive: bool, stable: bool) -> dict:
    return {
        "template_id": template,
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


def _synthetic_gate_inputs(*, weak_event_stable: bool = False) -> tuple[list[dict], list[dict]]:
    feedback: list[dict] = []
    records: list[dict] = []
    ordinal = 0
    for template in FOCUS:
        for i in range(72):
            is_productive = i < 36
            stable_cutoff = 10 if (template == "BASE_EVENT" and weak_event_stable) else 30
            feedback.append(_feedback(template, r.P, ordinal, productive=is_productive, stable=is_productive and i < stable_cutoff))
            records.append({"primary": {"behavior_identity": f"p-{ordinal}"}, "base_control": {"behavior_identity": f"c-{ordinal}"}})
            ordinal += 1
        for arm in (r.U, r.E):
            for i in range(24):
                is_productive = i < 5
                feedback.append(_feedback(template, arm, ordinal, productive=is_productive, stable=is_productive and i < 4))
                records.append({"primary": {"behavior_identity": f"p-{ordinal}"}, "base_control": {"behavior_identity": f"c-{ordinal}"}})
                ordinal += 1
    assert ordinal == 360
    return feedback, records


def test_v3_controlled_gate_passes_when_each_frontier_beats_controls() -> None:
    plan = r.verify_plan(PLAN)
    feedback, records = _synthetic_gate_inputs()
    result = r._gate(plan, feedback, records)
    assert result["status"] == "PRIMITIVE_FRONTIER_CONTROLLED_V3_PASS"
    assert all(result["checks"].values())
    assert set(result["per_focus_template"]) == set(FOCUS)


def test_v3_controlled_gate_fails_single_template_stable_transfer() -> None:
    plan = r.verify_plan(PLAN)
    feedback, records = _synthetic_gate_inputs(weak_event_stable=True)
    result = r._gate(plan, feedback, records)
    assert result["status"] == "PRIMITIVE_FRONTIER_CONTROLLED_V3_FAIL"
    assert result["checks"]["each_focus_template_primitive_uplift_stable"] is False
    assert result["checks"]["each_focus_template_productive_advantage"] is True


def test_v3_persistent_executor_uses_full_frozen_union() -> None:
    authority = {
        "execution_contract_path": "contract",
        "train_field_root": "fields",
        "train_price_root": "prices",
        "price_manifest": {"x": 1},
        "price_manifest_path": "price_manifest",
        "registry_path": "registry",
        "windows": ({"window_id": "w"},),
        "field_manifest_file_sha": "f",
        "field_manifest_payload_sha": "p",
    }
    fields = ("trade_time", "code", "close", "open")
    opts = r._persistent_executor_options(authority, "inputhash", 24, fields)
    assert opts["max_workers"] == 24
    assert opts["initializer"] is r.engine._initialize_worker
    assert opts["initargs"][-1] == fields
    assert opts["initargs"][6] == "inputhash"


def test_v3_runtime_verifier_hashes_v3_runner_and_binds_v2_redirect() -> None:
    source = (ROOT / "src/our_system_phase2/runtime/cn_program_primitive_market_successor_v3.py").read_text(encoding="utf-8-sig")
    assert "ROUTE_ID='cn-program-primitive-market-successor-v3'" in source
    assert "runner=root/'scripts/run_cn_program_primitive_market_successor_v3.py'" in source
    assert "repo_root/'scripts/run_cn_program_primitive_market_successor_v3.py'" in source
    assert "source_v2_terminal_audit" in source
    assert "source_v2_postrun_outcome" in source
    assert "source_v2_focus_redirect_evidence" in source
    assert "effective_spent_exact_count'])!=8294" in source


def test_v3_authorization_binds_dynamic_prefinancial_field_projection(tmp_path: Path) -> None:
    plan = r.verify_plan(PLAN)
    canary = {
        "schema_version": "synthetic_v3_canary_dynamic_field_projection",
        "status": "PASS",
        "candidate_evaluation_executed": False,
        "preview_selected_count": 360,
        "field_column_count": 47,
        "field_columns_sha256": "synthetic-47-field-hash",
        "evaluator_pool_lifetime": "PERSISTENT_RUN_SCOPE",
        "successor_plan_payload_sha256": plan["plan_payload_sha256"],
        "fresh_supply_payload_sha256": plan["fresh_supply"]["payload_sha256"],
        "source_v2_terminal_audit_payload_sha256": plan["source_v2_terminal_audit"]["payload_sha256"],
        "source_v2_postrun_outcome_payload_sha256": plan["source_v2_postrun_outcome"]["payload_sha256"],
        "source_v2_focus_redirect_evidence_payload_sha256": plan["source_v2_focus_redirect_evidence"]["payload_sha256"],
        "acceleration_accuracy_audit_payload_sha256": plan["acceleration_accuracy_audit"]["payload_sha256"],
        "runner_source_file_sha256": sha256_file(ROOT / "scripts/run_cn_program_primitive_market_successor_v3.py"),
    }
    canary["official_canary_payload_sha256"] = stable_hash(canary)
    canary_path = tmp_path / "canary.json"
    canary_path.write_text(json.dumps(canary, sort_keys=True), encoding="utf-8")

    authorization = auth_builder.build(ROOT, canary_path)
    assert authorization["resource_contract"]["field_column_count"] == 47
    assert authorization["resource_contract"]["field_columns_sha256"] == "synthetic-47-field-hash"
    assert authorization["official_resource_canary"]["field_column_count"] == 47

    auth_path = tmp_path / "authorization.json"
    auth_path.write_text(json.dumps(authorization, sort_keys=True), encoding="utf-8")
    verified = runtime.verify_authorization(auth_path, repo_root=ROOT)
    checked = runtime.verify_official_canary(canary_path, verified, repo_root=ROOT)
    assert checked["field_column_count"] == 47
