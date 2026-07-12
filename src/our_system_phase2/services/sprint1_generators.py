"""Distinct mechanism generators for CN Generator Research Sprint-1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence


GENERATOR_VERSION = "cn_generator_research_sprint1_v1"
EPS = "0.000001"
WINDOWS = (2, 3, 5, 8, 15, 30)
RAW_FIELDS = (
    "close", "vwap", "ret_1m", "intraday_ret_from_open",
    "amount_yuan", "volume", "high", "low",
)
CONTEXT_FIELDS = (
    "ctx_hfq_turnover_ratio", "ctx_hfq_volume_ratio", "ctx_hfq_market_cap_yuan",
    "ctx_hfq_float_market_cap_yuan", "ctx_hfq_pb", "ctx_hfq_ps_ttm",
    "ctx_ths_hot_rank", "ctx_ths_hot_rank_diff", "ctx_rzrq_rzye",
    "ctx_rzrq_rzjme", "ctx_holder_holder_num_ratio", "ctx_billboard_deal_net_ratio",
)
FIRSTN_FIELDS = (
    "m1_first5_last_return_vs_open", "m1_first5_vwap_return_vs_open", "m1_first5_range",
    "m1_first15_last_return_vs_open", "m1_first15_vwap_return_vs_open", "m1_first15_range",
    "m1_first30_last_return_vs_open", "m1_first30_vwap_return_vs_open", "m1_first30_range",
)
EVENT_INTENSITY_FIELDS = (
    "evt_uplimit_age_min", "evt_uplimit_amount", "evt_uplimit_auction_buy",
    "evt_uplimit_auction_money", "evt_uplimit_auction_turnover",
    "evt_uplimit_fd_close", "evt_uplimit_fd_max", "evt_uplimit_up_limit_keep_times",
)
INTRADAY_STATE_FIELDS = ("evt_uplimit_active", "evt_uplimit_type_code")
CONTEXT_STATE_FIELDS = (
    "ctx_hfq_prev_is_limit_up", "ctx_sent_zb_num", "ctx_sent_lb_2_num", "ctx_zls_strong",
)
STATE_FIELDS = INTRADAY_STATE_FIELDS + CONTEXT_STATE_FIELDS
MECHANISM_LANES = (
    "static_cross_sectional", "firstn_intraday_path", "temporal_program",
    "event_conditioned", "state_transition", "orthogonal_exile",
)


@dataclass(frozen=True, slots=True)
class ProgramSpec:
    expression: str
    motif: str
    hypothesis_arm: str
    primitive_family: str
    complexity: int
    generator_index: int
    parameters: Mapping[str, Any]
    mutation_operator: str = "root_sample"

    def metadata(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("expression")
        payload.pop("motif")
        payload["program_parameters"] = payload.pop("parameters")
        return payload


def _pick(values: Sequence[Any], index: int, stride: int = 1) -> Any:
    return values[(index // stride) % len(values)]


def _static(index: int) -> ProgramSpec:
    primitive = _pick(("rank_level", "rank_change", "interaction", "simple_nonlinear"), index)
    a = _pick(RAW_FIELDS + CONTEXT_FIELDS, index, 4)
    b = _pick(RAW_FIELDS, index, 4 * len(RAW_FIELDS + CONTEXT_FIELDS))
    window = _pick(WINDOWS, index, 4 * len(RAW_FIELDS + CONTEXT_FIELDS) * len(RAW_FIELDS))
    coefficient = _pick((0.05, 0.1, 0.2, 0.35), index, 17)
    if primitive == "rank_level":
        expression = f"CSRank(${a})"
    elif primitive == "rank_change":
        expression = f"CSRank(Delta(${a},{window}))"
    elif primitive == "interaction":
        normalized = (
            f"MaskedZScore(${a},60,0.6)"
            if str(a).startswith("ctx_") or a in {"price_spread", "trade_count"}
            else f"ZScore(${a})"
        )
        expression = f"CSRank(Add({normalized},Mul(Sign(${b}),{coefficient})))"
    else:
        expression = f"CSRank(Div(Sub(${a},${b}),Add(Abs(${a}),{EPS})))"
    return ProgramSpec(expression, primitive, "static_cross_sectional", primitive, 2 + expression.count("("), index,
                       {"a": a, "b": b, "window": window, "coefficient": coefficient})


def _firstn(index: int) -> ProgramSpec:
    primitive = _pick(("level", "signed_path", "opening_gap", "path_shape", "mean_reversion"), index)
    firstn = _pick(FIRSTN_FIELDS, index, 5)
    raw = _pick(RAW_FIELDS, index, 5 * len(FIRSTN_FIELDS))
    window = _pick(WINDOWS, index, 5 * len(FIRSTN_FIELDS) * len(RAW_FIELDS))
    if primitive == "level":
        expression = f"CSRank(${firstn})"
    elif primitive == "signed_path":
        expression = f"CSRank(Mul(${firstn},Sign(Delta(${raw},{window}))))"
    elif primitive == "opening_gap":
        expression = f"CSRank(Sub(${firstn},Mean(${raw},{window})))"
    elif primitive == "path_shape":
        expression = f"CSRank(Add(${firstn},PathShape(${raw},{window})))"
    else:
        expression = f"CSRank(Sub(${firstn},Delta(${raw},{window})))"
    return ProgramSpec(expression, f"firstn_{primitive}", "firstn_intraday_path", primitive,
                       2 + expression.count("("), index, {"firstn": firstn, "raw": raw, "window": window})


def _temporal(index: int) -> ProgramSpec:
    primitives = ("Delta", "Slope", "Acceleration", "Persistence", "Duration", "StateAge",
                  "TimeSince", "PathShape", "DrawdownRecovery", "MultiScaleRelation")
    primitive = primitives[index % len(primitives)]
    occurrence = index // len(primitives)
    field = RAW_FIELDS[occurrence % len(RAW_FIELDS)]
    other = RAW_FIELDS[(occurrence * 5 + 3) % len(RAW_FIELDS)]
    window = WINDOWS[(occurrence // len(RAW_FIELDS)) % len(WINDOWS)]
    state = INTRADAY_STATE_FIELDS[(occurrence * 5 + 1) % len(INTRADAY_STATE_FIELDS)]
    if primitive in {"Delta", "Slope", "Acceleration", "PathShape"}:
        expression = f"CSRank({primitive}(${field},{window}))"
    elif primitive == "Persistence":
        expression = f"CSRank(Mul(Persistence($evt_uplimit_active,{window}),Sign(Delta(${field},{window}))))"
    elif primitive in {"Duration", "StateAge"}:
        expression = f"CSRank(Mul({primitive}(${state}),Sign(Delta(${field},{window}))))"
    elif primitive == "TimeSince":
        expression = f"CSRank(Mul(TimeSince($evt_uplimit_active),Sign(Delta(${field},{window}))))"
    elif primitive == "DrawdownRecovery":
        expression = f"CSRank(Sub(RecoveryPath(${field},{window}),DrawdownPath(${other},{window})))"
    else:
        short = max(2, min(window, 5))
        long = max(short + 1, min(30, short * 3))
        expression = f"CSRank(MultiScaleRelation(${field},${other},{short},{long}))"
    return ProgramSpec(expression, primitive.lower(), "temporal_program", primitive.lower(),
                       2 + expression.count("("), index,
                       {"field": field, "other": other, "window": window, "state": state})


def _event(index: int) -> ProgramSpec:
    primitives = ("seal", "break_board", "reseal", "event_age", "continuation", "reversal",
                  "intensity", "firstn_confirmation")
    primitive = primitives[index % len(primitives)]
    occurrence = index // len(primitives)
    raw = RAW_FIELDS[occurrence % len(RAW_FIELDS)]
    intensity = EVENT_INTENSITY_FIELDS[(occurrence * 5 + 1) % len(EVENT_INTENSITY_FIELDS)]
    window = WINDOWS[(occurrence * 7 + 2) % len(WINDOWS)]
    firstn = FIRSTN_FIELDS[(occurrence * 11 + 3) % len(FIRSTN_FIELDS)]
    if primitive == "seal":
        expression = f"CSRank(Mul(Transition($evt_uplimit_active,0,1),Mul(Sign(${intensity}),Sign(Delta(${raw},{window})))))"
    elif primitive == "break_board":
        expression = f"CSRank(Mul(Transition($evt_uplimit_active,1,0),Neg(Delta(${raw},{window}))))"
    elif primitive == "reseal":
        expression = f"CSRank(Mul(Transition($evt_uplimit_active,0,1),Mul(EventCount($evt_uplimit_active,{window}),Sign(${intensity}))))"
    elif primitive == "event_age":
        expression = f"CSRank(Mul(EventAge($evt_uplimit_active),Mul(Sign(${intensity}),Sign(Delta(${raw},{window})))))"
    elif primitive == "continuation":
        expression = f"CSRank(Mul(EventWindow(${raw},$evt_uplimit_active,0,{min(window, 8)}),Sign(${intensity})))"
    elif primitive == "reversal":
        expression = f"CSRank(Mul(Neg(EventWindow(${raw},$evt_uplimit_active,0,{min(window, 8)})),Sign(${intensity})))"
    elif primitive == "intensity":
        expression = f"CSRank(Mul(EventWindow(${intensity},$evt_uplimit_active,0,{min(window, 8)}),Sign(${raw})))"
    else:
        expression = f"CSRank(Mul(${firstn},Mul(Add(EventCount($evt_uplimit_active,{window}),1),Sign(${intensity}))))"
    return ProgramSpec(expression, f"event_{primitive}", "event_conditioned", primitive,
                       2 + expression.count("("), index,
                       {"raw": raw, "intensity": intensity, "window": window, "firstn": firstn})


def _state(index: int) -> ProgramSpec:
    primitives = ("enter", "leave", "duration", "switch", "conditioned_transition", "multi_state")
    primitive = primitives[index % len(primitives)]
    occurrence = index // len(primitives)
    # Decode each primitive's occurrence as a mixed-radix coordinate.  Every
    # state expression below consumes the raw/window coordinate as well as its
    # state coordinate; otherwise switch/conditioned templates collapse to a
    # handful of identities even though their proposal indices are distinct.
    state = INTRADAY_STATE_FIELDS[occurrence % len(INTRADAY_STATE_FIELDS)]
    raw = RAW_FIELDS[(occurrence // len(INTRADAY_STATE_FIELDS)) % len(RAW_FIELDS)]
    window = WINDOWS[
        (occurrence // (len(INTRADAY_STATE_FIELDS) * len(RAW_FIELDS))) % len(WINDOWS)
    ]
    other_state = INTRADAY_STATE_FIELDS[(occurrence // 2 + 1) % len(INTRADAY_STATE_FIELDS)]
    context_state = CONTEXT_STATE_FIELDS[(occurrence // 4) % len(CONTEXT_STATE_FIELDS)]
    context = CONTEXT_FIELDS[(occurrence // 8) % len(CONTEXT_FIELDS)]
    if primitive == "enter":
        expression = f"CSRank(Mul(Transition(${state},0,1),Sign(Delta(${raw},{window}))))"
    elif primitive == "leave":
        expression = f"CSRank(Mul(Transition(${state},1,0),Neg(Delta(${raw},{window}))))"
    elif primitive == "duration":
        expression = f"CSRank(Mul(StateAge(${state}),Sign(Delta(${raw},{window}))))"
    elif primitive == "switch":
        expression = (
            f"CSRank(Mul(Sub(Transition(${state},0,1),Transition(${state},1,0)),"
            f"Sign(Delta(${raw},{window}))))"
        )
    elif primitive == "conditioned_transition":
        expression = (
            f"CSRank(Mul(Transition(${state},0,1),"
            f"Mul(Sign(${context_state}),Sign(Delta(${raw},{window})))))"
        )
    else:
        expression = (
            f"CSRank(Mul(StateAge(${state}),Mul(Sign(${context_state}),"
            f"Mul(Sign(${other_state}),Sign(Delta(${raw},{window}))))))"
        )
    return ProgramSpec(expression, f"state_{primitive}", "state_transition", primitive,
                       2 + expression.count("("), index,
                       {"state": state, "other_state": other_state, "context_state": context_state, "raw": raw,
                        "context": context, "window": window})


def _orthogonal(index: int) -> ProgramSpec:
    primitives = ("level_residual", "change_residual", "path_residual", "exile_divergence")
    primitive = primitives[index % len(primitives)]
    occurrence = index // len(primitives)
    # Mixed-radix coordinates keep each residual family broad on its own.
    # The previous unrelated strides aliased after canonicalization and left
    # 105 of 256 proposals as duplicate expressions.
    raw = RAW_FIELDS[occurrence % len(RAW_FIELDS)]
    other = RAW_FIELDS[(occurrence // len(RAW_FIELDS)) % len(RAW_FIELDS)]
    context = CONTEXT_FIELDS[(occurrence // len(RAW_FIELDS)) % len(CONTEXT_FIELDS)]
    window = WINDOWS[(occurrence // (len(RAW_FIELDS) * 2)) % len(WINDOWS)]
    if primitive == "level_residual":
        expression = f"CSRank(SafeCSResidual(${raw},${context},20,5,0.6))"
    elif primitive == "change_residual":
        expression = f"CSRank(SafeCSResidual(Delta(${raw},{window}),${context},20,5,0.6))"
    elif primitive == "path_residual":
        expression = f"CSRank(CSResidual(PathShape(${raw},{window}),Delta(${other},{window})))"
    else:
        expression = f"CSRank(Sub(Delta(${raw},{window}),MaskedZScore(${context},60,0.6)))"
    return ProgramSpec(expression, primitive, "orthogonal_exile", primitive,
                       2 + expression.count("("), index,
                       {"raw": raw, "other": other, "context": context, "window": window})


_BUILDERS = {
    "static_cross_sectional": _static,
    "firstn_intraday_path": _firstn,
    "temporal_program": _temporal,
    "event_conditioned": _event,
    "state_transition": _state,
    "orthogonal_exile": _orthogonal,
}


def generate_program(hypothesis_arm: str, index: int, seed: int = 0) -> ProgramSpec:
    try:
        builder = _BUILDERS[hypothesis_arm]
    except KeyError as exc:
        raise KeyError(f"unknown Sprint-1 hypothesis arm: {hypothesis_arm}") from exc
    return builder(int(index) + int(seed) * 1009)


def generate_mechanism_pool(index: int, seed: int = 0) -> ProgramSpec:
    arm = MECHANISM_LANES[(int(index) + int(seed)) % len(MECHANISM_LANES)]
    local_index = int(index) // len(MECHANISM_LANES)
    return generate_program(arm, local_index, seed)


def mutate_program(parent: Mapping[str, Any], child_index: int, seed: int) -> ProgramSpec:
    arm = str(parent["hypothesis_arm"])
    parent_index = int(parent.get("generator_index", 0))
    operators = ("field_swap", "window_shift", "primitive_swap", "condition_insert")
    operator = operators[int(child_index) % len(operators)]
    offsets = {"field_swap": 97, "window_shift": 509, "primitive_swap": 1, "condition_insert": 1301}
    base = generate_program(arm, parent_index + offsets[operator] + int(child_index) * 17, seed)
    expression = base.expression
    complexity = base.complexity
    if operator == "condition_insert" and arm not in {"event_conditioned", "state_transition"}:
        context = CONTEXT_FIELDS[(int(child_index) + int(seed)) % len(CONTEXT_FIELDS)]
        expression = f"CSRank(Mul({expression},Sign(${context})))"
        complexity += 3
    return ProgramSpec(expression, base.motif, arm, base.primitive_family, complexity,
                       base.generator_index, base.parameters, operator)
