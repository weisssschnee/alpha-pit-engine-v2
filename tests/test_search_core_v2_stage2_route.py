from app import HIGH_COST_ROUTE_ACTIONS, ROUTES
from our_system_phase2.services.project_control_admission import CAMPAIGN_AUTHORIZATION_BOUND_ROUTES, ACTION_LAUNCH, ACTION_RETRY
from our_system_phase2.runtime import cn_search_core_v2_stage2_v1 as runtime


def test_stage2_route_is_high_cost_and_authorization_bound():
    assert ROUTES[runtime.ROUTE_ID] == "our_system_phase2.runtime.cn_search_core_v2_stage2_v1"
    assert HIGH_COST_ROUTE_ACTIONS[runtime.ROUTE_ID] == frozenset({ACTION_LAUNCH, ACTION_RETRY})
    assert runtime.ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES
    assert runtime.TOTAL_EVALUATIONS == 1008
    assert runtime.TOTAL_PER_ARM == 504