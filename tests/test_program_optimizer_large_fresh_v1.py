from __future__ import annotations

import app

from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import (
    ENHANCED_TEMPLATES,
    MINIMUM_MACROS_BEFORE_VALUE_COLLAPSE_TEST,
    NEW_BEHAVIOR_PAIR_RATE_COLLAPSE_MAX,
    PRODUCTIVE_EFFICIENCY_COLLAPSE_MAX,
    RESOURCE_CPU_THREADS,
    RESOURCE_PROFILE,
    ROUTE_ID,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    UNIFORM_CONTROL,
)
from scripts.run_cn_program_optimizer_large_fresh_v1 import (
    _behavior_pair_identity,
    _checkpoint_arm,
    _productive_feedback,
    _value_collapse,
)


def test_large_fresh_route_is_high_cost_and_authorization_bound() -> None:
    assert app.ROUTES[ROUTE_ID] == (
        "our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1"
    )
    assert app.HIGH_COST_ROUTE_ACTIONS[ROUTE_ID] == {ACTION_LAUNCH, ACTION_RETRY}
    assert ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES
    assert RESOURCE_PROFILE == "SEARCH_DUAL_24"
    assert RESOURCE_CPU_THREADS == 24


def test_uniform_reserve_rotates_one_template_checkpoint_per_macro() -> None:
    template_count = len(ENHANCED_TEMPLATES)
    for macro_index in range(template_count * 2):
        arms = [_checkpoint_arm(macro_index, index) for index in range(template_count)]
        assert arms.count(UNIFORM_CONTROL) == 1
        assert arms.count(HYBRID_TPE_PROGRAM) == template_count - 1
        assert arms[macro_index % template_count] == UNIFORM_CONTROL


def test_value_collapse_requires_two_recent_bad_macros_after_minimum_history() -> None:
    good = {
        "productive_efficiency": PRODUCTIVE_EFFICIENCY_COLLAPSE_MAX + 0.01,
        "new_behavior_pair_rate": NEW_BEHAVIOR_PAIR_RATE_COLLAPSE_MAX + 0.01,
    }
    bad = {
        "productive_efficiency": PRODUCTIVE_EFFICIENCY_COLLAPSE_MAX,
        "new_behavior_pair_rate": NEW_BEHAVIOR_PAIR_RATE_COLLAPSE_MAX,
    }
    assert not _value_collapse([bad] * (MINIMUM_MACROS_BEFORE_VALUE_COLLAPSE_TEST - 1))
    assert _value_collapse(
        [good] * (MINIMUM_MACROS_BEFORE_VALUE_COLLAPSE_TEST - 2) + [bad, bad]
    )
    assert not _value_collapse(
        [good] * (MINIMUM_MACROS_BEFORE_VALUE_COLLAPSE_TEST - 2) + [bad, good]
    )


def test_productive_feedback_requires_admission_and_both_positive_uplifts() -> None:
    row = {
        "absolute_admission": {"admitted": True},
        "enhancer_credit": {
            "program_credit": {
                "matched_cumulative_net_return_increment": 0.1,
                "matched_net_reward_increment": 0.2,
            }
        },
    }
    assert _productive_feedback(row)
    row["enhancer_credit"]["program_credit"]["matched_net_reward_increment"] = 0.0
    assert not _productive_feedback(row)


def test_behavior_pair_identity_is_label_free_and_rejects_base_equivalence() -> None:
    record = {
        "primary": {"behavior_identity": "primary-behavior"},
        "base_control": {"behavior_identity": "control-behavior"},
    }
    identity = _behavior_pair_identity(record)
    assert isinstance(identity, str) and len(identity) == 64
    record["base_control"]["behavior_identity"] = "primary-behavior"
    assert _behavior_pair_identity(record) is None
