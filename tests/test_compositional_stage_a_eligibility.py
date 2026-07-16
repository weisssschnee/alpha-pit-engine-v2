from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.freeze_cn_compositional_stage_a_eligibility import (
    FORMALLY_EVALUABLE,
    NATURAL_UNDERFILL,
    PARTIAL,
    StageAEligibilityError,
    build_stage_a_eligibility,
    freeze_stage_a_eligibility,
)


REPO = Path(__file__).resolve().parents[1]
PLAN = REPO / "runtime/run_plans/cn_compositional_nline_bounded_search_epoch1_v1.json"
RUNTIME = REPO / "runtime/cn_compositional_nline_large_search_20260715"


def test_current_evidence_supports_scoped_stage_a_pack(tmp_path: Path) -> None:
    manifest = tmp_path / "eligibility.json"
    pack = tmp_path / "pack.csv"

    decision = freeze_stage_a_eligibility(
        repo_root=REPO,
        plan_path=PLAN,
        runtime_root=RUNTIME,
        output_manifest=manifest,
        output_pack=pack,
    )

    assert decision["frozen_stage_a_pair_budget"] == 3_484
    assert decision["unused_pair_budget_not_reallocated"] == 612
    assert decision["excluded_routes"] == ["DISCLOSURE_EVENT"]
    assert decision["route_decisions"]["DISCLOSURE_EVENT"]["status"] == PARTIAL
    assert decision["route_decisions"]["DISCLOSURE_EVENT"]["eligible_pair_quota"] == 0
    assert (
        decision["route_decisions"]["INTRADAY_STATE_TRANSITION"]["status"]
        == NATURAL_UNDERFILL
    )
    assert (
        decision["route_decisions"]["INTRADAY_STATE_TRANSITION"]["eligible_pair_quota"]
        == 540
    )
    assert decision["route_decisions"]["MINUTE_STATIC"]["status"] == FORMALLY_EVALUABLE
    assert decision["strict_stage_a"] == "NOT_AUTHORIZED"
    assert pack.is_file()
    assert json.loads(manifest.read_text(encoding="utf-8"))["stage_a_pack"]["pair_count"] == 3_484


def test_performance_selected_admission_fails_closed() -> None:
    plan = json.loads(PLAN.read_text(encoding="utf-8"))
    waterfall = [
        {
            "route_id": route,
            "exact_unique": "40" if route == "DISCLOSURE_EVENT" else str(quota),
            "behavior_unique": "1",
            "materialization_pass": str(quota),
            "support_pass": str(quota),
            "diversity_admitted": "1" if route == "DISCLOSURE_EVENT" else str(quota),
        }
        for route, quota in plan["stage_a"]["route_pair_quotas"].items()
    ]
    admissions = []
    for route, quota in plan["stage_a"]["route_pair_quotas"].items():
        count = 1 if route == "DISCLOSURE_EVENT" else quota
        for index in range(count):
            admissions.append(
                {
                    "route_id": route,
                    "pair_id": f"{route}.pair.{index}",
                    "exact_identity": f"{route}.exact.{index}",
                    "exact_behavior_identity": f"{route}.behavior.{index}",
                    "behavior_cluster_id": f"{route}.cluster.{index}",
                    "skeleton_id": f"{route}.skeleton.{index % 8}",
                    "policy_id": plan["policies"][index % len(plan["policies"])],
                    "seed": str(plan["seeds"][index % len(plan["seeds"])]),
                    "admission_rank": str(index + 1),
                    "performance_accessed_for_admission": "False",
                }
            )
    admissions[0]["performance_accessed_for_admission"] = "True"

    with pytest.raises(StageAEligibilityError, match="performance-selected"):
        build_stage_a_eligibility(
            plan=plan,
            waterfall_rows=waterfall,
            admission_rows=admissions,
            expressivity={
                "generators": {
                    "compositional_v2": {
                        "routes": {"DISCLOSURE_EVENT": {"exact_identity_count": 40}}
                    }
                }
            },
        )
