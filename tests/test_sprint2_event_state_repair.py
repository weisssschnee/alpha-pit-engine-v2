from __future__ import annotations

import numpy as np
import pandas as pd

from our_system_phase2.services.real_market_validation import evaluate_panel_expression
from our_system_phase2.services.sprint1_generators import generate_program
from our_system_phase2.services.typed_primitive_gate import expression_fields, validate_expression


def _planted_panel() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for code_index in range(32):
        event_start = 6 + code_index % 4
        for minute in range(28):
            trade_time = pd.Timestamp("2025-04-01 09:31") + pd.Timedelta(minutes=minute)
            active = int(
                event_start <= minute <= event_start + 2
                or minute == event_start + 5
            )
            trend = 0.018 * minute + 0.11 * np.sin((minute + code_index) / 3.0)
            close = 10.0 + code_index * 0.07 + trend
            rows.append(
                {
                    "code": f"{code_index:06d}",
                    "trade_time": trade_time,
                    "evt_uplimit_active": float(active),
                    "evt_uplimit_type_code": float((code_index + minute // 8) % 3),
                    "evt_uplimit_amount": float((1.0 + code_index % 7) * (1.0 + active)),
                    "intraday_ret_from_open": float(
                        -1 if ((minute + code_index % 5) // 4) % 2 == 0 else 1
                    ),
                    "ret_1m": float(-1 if ((minute + code_index % 7) // 3) % 2 == 0 else 1),
                    "close": close,
                    "high": close + 0.08 + 0.001 * code_index,
                    "low": close - 0.06 - 0.001 * (code_index % 5),
                    "m1_first15_last_return_vs_open": 0.001 * (code_index - 16),
                    "ctx_hfq_turnover_ratio": 0.25 + 0.03 * (code_index % 11),
                }
            )
    return pd.DataFrame(rows)


def _evaluate(frame: pd.DataFrame, expression: str) -> pd.Series:
    return evaluate_panel_expression(
        frame,
        expression,
        cache={},
        data_role="development",
    )


def test_event_repair_templates_materialize_and_retain_the_planted_trigger() -> None:
    frame = _planted_panel()
    required_parameters = {
        "event_trigger", "pre_event_path", "event_age_window",
        "post_event_action", "direction", "matched_control_expression",
    }

    for index in range(10):
        program = generate_program("event_conditioned", index, seed=0)
        control_expression = str(program.parameters["matched_control_expression"])
        verdict = validate_expression(
            program.expression,
            entry_lineage="test/sprint2/event",
            materialization_stage="planted_capability",
            candidate_role="research_canary",
        )
        signal = _evaluate(frame, program.expression)
        control = _evaluate(frame, control_expression)

        assert required_parameters <= set(program.parameters)
        assert program.parameters["unsupported_event_capabilities"] == ["break_board", "reseal"]
        assert verdict.typed_gate_decision == "allow"
        assert "$evt_uplimit_active" in program.expression
        assert "$evt_uplimit_active" not in control_expression
        assert signal.notna().sum() > 0
        assert control.notna().sum() > 0
        assert not signal.fillna(-999.0).equals(control.fillna(-999.0))


def test_state_repair_templates_materialize_distinct_from_unconditional_controls() -> None:
    frame = _planted_panel()

    for index in range(7):
        program = generate_program("state_transition", index, seed=0)
        control_expression = str(program.parameters["matched_control_expression"])
        verdict = validate_expression(
            program.expression,
            entry_lineage="test/sprint2/state",
            materialization_stage="planted_capability",
            candidate_role="research_canary",
        )
        signal = _evaluate(frame, program.expression)
        control = _evaluate(frame, control_expression)

        assert verdict.typed_gate_decision == "allow"
        assert program.parameters["state_source_expression"] in program.expression
        assert set(expression_fields(control_expression)).isdisjoint(
            set(expression_fields(str(program.parameters["state_source_expression"])))
        )
        assert signal.notna().sum() > 0
        assert signal.nunique(dropna=True) > 1, (index, program.motif, program.expression)
        assert not signal.fillna(-999.0).equals(control.fillna(-999.0))
