from __future__ import annotations

import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
PLAN = REPO / "runtime/run_plans/cn_compositional_nline_bounded_search_epoch1_v1.json"


def test_bounded_search_contract_freezes_balanced_budget_and_sealed_roles() -> None:
    plan = json.loads(PLAN.read_text(encoding="utf-8"))

    assert plan["authorization"] == "EXPERIMENTAL_BOUNDED_DEVELOPMENT_SEARCH"
    assert plan["access_contract"]["validation"] == "FORBIDDEN"
    assert plan["access_contract"]["holdout"] == "FORBIDDEN"
    assert plan["access_contract"]["forward_2026"] == "SEALED"
    assert plan["access_contract"]["promotion"] == "FORBIDDEN"
    assert plan["access_contract"]["cross_sprint_memory"] == "FORBIDDEN"
    assert sum(plan["generation_contract"]["route_attempt_quotas"].values()) == 200_000
    assert sum(plan["stage_a"]["route_pair_quotas"].values()) == 4_096
    assert sum(plan["stage_b"]["route_pair_quotas"].values()) == 4_096
    assert plan["strict_hard_cap"] == {"evaluator_calls": 16_384, "pairs": 8_192}
    assert len(plan["policies"]) == 4
    assert len(plan["seeds"]) == 4
    assert plan["route_search_roles"]["BROAD_EVENT_FROZEN_ENTRY"] == "FROZEN_REFERENCE_ONLY"
    assert plan["grammar_contract"]["plate_industry_neutralization"].startswith("DISABLED_")


def test_stage_route_quotas_are_policy_seed_balanced() -> None:
    plan = json.loads(PLAN.read_text(encoding="utf-8"))
    cells = len(plan["policies"]) * len(plan["seeds"])

    for stage in ("stage_a", "stage_b"):
        assert all(
            quota % cells == 0
            for quota in plan[stage]["route_pair_quotas"].values()
        )
