from scripts.run_cn_search_core_v2_stage2_v1 import _decision
from our_system_phase2.services.program_search_primitive_credit_v1 import PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1 as A
from our_system_phase2.services.program_search_state_jump_generator_v2 import SEMANTIC_STATE_JUMP_GENERATOR_V2 as B

NAMES=("BASE_EVENT","BASE_MARKET","BASE_MARKET_EVENT","BASE_TEMPORAL","BASE_TEMPORAL_EVENT","BASE_TEMPORAL_MARKET","BASE_TEMPORAL_MARKET_EVENT")


def _m(productive, stable, behaviors):
    return {"productive": productive, "stable": stable, "distinct_behavior_pair_count": behaviors}


def _per(a_vals, b_vals):
    return {A:{name:{"productive":value} for name,value in zip(NAMES,a_vals)}, B:{name:{"productive":value} for name,value in zip(NAMES,b_vals)}}


def test_stage2_scale_confirmation_requires_broad_nonconcentrated_win():
    d=_decision(_m(196,180,420),_m(222,190,410),_per([28]*7,[34,34,34,34,30,28,28]))
    assert d["status"]=="MATURE_GENERATOR_STAGE2_SCALE_CONFIRMATION_PASS_SEARCH_CORE_POLICY_REVIEW_ELIGIBLE"
    assert d["productive_template_win_count_b"]==5
    assert d["maximum_positive_productive_gain_template_fraction"] < 0.40
    assert d["automatic_policy_change_authorized"] is False


def test_stage2_concentrated_gain_does_not_pass():
    d=_decision(_m(196,180,420),_m(215,185,410),_per([28]*7,[47,29,28,28,28,28,28]))
    assert d["status"]=="AMBIGUOUS_STAGE2_SCALE_CONFIRMATION_NO_POLICY_CHANGE"


def test_stage2_clear_loss_stops():
    d=_decision(_m(196,180,420),_m(180,175,400),_per([28]*7,[25]*7))
    assert d["status"]=="MATURE_GENERATOR_STAGE2_SCALE_CLEAR_LOSS_STOP"