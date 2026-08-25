from app import HIGH_COST_ROUTE_ACTIONS, ROUTES
from our_system_phase2.runtime import cn_search_core_v2_production_wave2_v1 as runtime
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
)


def test_production_wave2_route_is_high_cost_and_authorization_bound():
    assert ROUTES[runtime.ROUTE_ID] == "our_system_phase2.runtime.cn_search_core_v2_production_wave2_v1"
    assert HIGH_COST_ROUTE_ACTIONS[runtime.ROUTE_ID] == {ACTION_LAUNCH, ACTION_RETRY}
    assert runtime.ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES
