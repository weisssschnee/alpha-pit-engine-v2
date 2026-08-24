from scripts.run_cn_search_core_v2_stage2_v1 import _decision, _execution_plan
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

def test_stage2_execution_plan_uses_base_event_microbatches_without_budget_drift():
    prefreeze={"stage2":{"template_batch_size":{
        "BASE_EVENT":12,"BASE_MARKET":24,"BASE_MARKET_EVENT":24,"BASE_TEMPORAL":24,
        "BASE_TEMPORAL_EVENT":24,"BASE_TEMPORAL_MARKET":24,"BASE_TEMPORAL_MARKET_EVENT":24,
    }}}
    plan=_execution_plan(prefreeze)
    assert len(plan)==24
    assert sum(row["batch_size"] for row in plan)==504
    base=[row for row in plan if row["template_id"]=="BASE_EVENT"]
    assert len(base)==6
    assert all(row["batch_size"]==12 for row in base)
    assert [(row["stage2_round_index"],row["microbatch_index"],row["slice_start"],row["slice_end"]) for row in base]==[
        (0,0,0,12),(0,1,12,24),(1,0,24,36),(1,1,36,48),(2,0,48,60),(2,1,60,72)
    ]
    other=[row for row in plan if row["template_id"]!="BASE_EVENT"]
    assert len(other)==18
    assert all(row["batch_size"]==24 for row in other)
