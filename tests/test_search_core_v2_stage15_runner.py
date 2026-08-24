from scripts.run_cn_search_core_v2_stage15_v1 import _decision
from our_system_phase2.services.program_search_primitive_credit_v1 import PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1 as A
from our_system_phase2.services.program_search_state_jump_generator_v2 import SEMANTIC_STATE_JUMP_GENERATOR_V2 as B


def _m(productive, stable, behaviors):
    return {"productive": productive, "stable": stable, "distinct_behavior_pair_count": behaviors}


def _per(a_vals, b_vals):
    names=("BASE_EVENT","BASE_MARKET","BASE_MARKET_EVENT","BASE_TEMPORAL","BASE_TEMPORAL_EVENT","BASE_TEMPORAL_MARKET","BASE_TEMPORAL_MARKET_EVENT")
    return {A:{n:{"productive":v} for n,v in zip(names,a_vals)}, B:{n:{"productive":v} for n,v in zip(names,b_vals)}}


def test_stage15_confirmation_requires_broad_mature_win():
    d=_decision(_m(80,75,150),_m(90,76,150),_per([10]*7,[12,12,12,12,12,10,10]))
    assert d["status"]=="MATURE_GENERATOR_STAGE15_CONFIRMATION_PASS_STAGE2_REVIEW_ELIGIBLE"
    assert d["productive_template_win_count_b"]==5
    assert d["automatic_stage2_authorized"] is False


def test_stage15_concentrated_gain_does_not_pass():
    d=_decision(_m(80,75,150),_m(90,76,150),_per([10]*7,[20,10,10,10,10,10,10]))
    assert d["status"]=="AMBIGUOUS_MATURE_STATE_CONFIRMATION_NO_STAGE2"


def test_stage15_clear_loss_stops():
    d=_decision(_m(80,75,150),_m(70,70,140),_per([10]*7,[9]*7))
    assert d["status"]=="MATURE_GENERATOR_STAGE15_CLEAR_LOSS_STOP"
