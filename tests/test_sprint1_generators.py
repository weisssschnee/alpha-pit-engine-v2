from __future__ import annotations

from our_system_phase2.services.sprint1_generators import (
    MECHANISM_LANES,
    RAW_FIELDS,
    generate_mechanism_pool,
    generate_program,
    mutate_program,
)
from our_system_phase2.services.typed_primitive_gate import validate_expression


def test_mechanism_generators_are_distinct_legal_and_not_lane_labels_on_one_grammar() -> None:
    expressions_by_lane: dict[str, set[str]] = {}
    primitives_by_lane: dict[str, set[str]] = {}
    for lane in MECHANISM_LANES:
        programs = [generate_program(lane, index, seed=17) for index in range(256)]
        expressions = {program.expression for program in programs}
        assert len(expressions) >= 180
        assert all(program.hypothesis_arm == lane for program in programs)
        assert all(
            validate_expression(
                program.expression,
                entry_lineage=f"test/{lane}",
                materialization_stage="sprint1_generator_test",
                candidate_role="research_canary",
            ).typed_gate_decision == "allow"
            for program in programs
        )
        expressions_by_lane[lane] = expressions
        primitives_by_lane[lane] = {program.primitive_family for program in programs}

    assert len({frozenset(values) for values in expressions_by_lane.values()}) == len(MECHANISM_LANES)
    assert primitives_by_lane["event_conditioned"] >= {
        "seal_entry", "entry_intensity", "entry_count", "event_age",
        "first_hit", "last_hit", "pre_event_path", "post_continuation",
        "post_reversal", "firstn_confirmation",
    }
    assert primitives_by_lane["state_transition"] >= {
        "level", "enter", "exit", "duration", "transition",
        "conditioned_temporal_path", "conditioned_residual",
    }


def test_mechanism_pool_and_mutation_preserve_hypothesis_arm_and_lineage_operator() -> None:
    parent = generate_mechanism_pool(31, seed=9)
    child = mutate_program(parent.metadata(), child_index=3, seed=9)
    assert child.hypothesis_arm == parent.hypothesis_arm
    assert child.mutation_operator == "condition_insert"
    assert child.expression != parent.expression


def test_raw_generator_fields_match_materialized_true1min_contract() -> None:
    assert set(RAW_FIELDS) == {
        "close", "vwap", "ret_1m", "intraday_ret_from_open",
        "amount_yuan", "volume", "high", "low",
    }
