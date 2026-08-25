from pathlib import Path

from app import HIGH_COST_ROUTE_ACTIONS, ROUTES
from scripts import build_cn_search_core_v2_production_wave1_report_only_validation_authorization_v1 as auth_builder
from scripts import run_cn_search_core_v2_production_wave1_report_only_validation_v1 as runner
from our_system_phase2.runtime import cn_search_core_v2_production_wave1_report_only_validation_v1 as runtime

REPO = Path(__file__).resolve().parents[1]


def test_wave1_report_only_validation_authorization_is_frozen_and_no_feedback():
    payload = auth_builder.build(REPO, source_repo_sha="a" * 40)
    assert payload["status"] == "SEARCH_CORE_V2_PRODUCTION_WAVE1_REPORT_ONLY_VALIDATION_AUTHORIZED_NOT_RUN"
    assert payload["candidate_count"] == 42
    assert payload["resource_contract"] == {
        "profile": "VALIDATION_DUAL_8",
        "cpu_threads": 8,
        "evaluator_workers": 4,
        "candidate_count": 42,
    }
    assert payload["candidate_generation_authorized"] is False
    assert payload["threshold_tuning_allowed"] is False
    assert payload["same_slice_reselection_allowed"] is False
    assert payload["optimizer_feedback_write"] == "FORBIDDEN"
    assert payload["policy_memory_write"] == "FORBIDDEN"
    assert payload["scheduler_write"] == "FORBIDDEN"
    assert payload["archive_write"] == "FORBIDDEN"
    assert payload["promotion_authorized"] is False
    assert payload["automatic_followon_authorized"] is False
    assert payload["oos_authority"] == "VALIDATION_REPORT_ONLY_EVIDENCE_ONLY"


def test_wave1_report_only_validation_route_is_project_controlled():
    assert ROUTES[runtime.ROUTE_ID] == "our_system_phase2.runtime.cn_search_core_v2_production_wave1_report_only_validation_v1"
    assert runtime.ROUTE_ID in HIGH_COST_ROUTE_ACTIONS


def test_wave1_validation_metric_tracks_productive_and_cross_window_stability():
    rows = [
        {
            "admission": {"admitted": True},
            "validation_productive": True,
            "validation_stable_2of3": True,
            "validation_stable_3of3": True,
        },
        {
            "admission": {"admitted": True},
            "validation_productive": True,
            "validation_stable_2of3": True,
            "validation_stable_3of3": False,
        },
        {
            "admission": {"admitted": False},
            "validation_productive": False,
            "validation_stable_2of3": False,
            "validation_stable_3of3": False,
        },
    ]
    metric = runner._metric(rows)
    assert metric["evaluated"] == 3
    assert metric["admitted"] == 2
    assert metric["productive"] == 2
    assert metric["stable_2of3"] == 2
    assert metric["stable_3of3"] == 1
