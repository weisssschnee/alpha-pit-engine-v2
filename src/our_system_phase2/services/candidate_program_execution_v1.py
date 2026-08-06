"""Thin execution bridge from compiled program outputs to existing replay.

The bridge evaluates the four compiled expressions with the existing panel
expression evaluator, then writes the already-supported ``signal`` and
``universe_eligible`` columns.  Selection, T+1, fees, tradability and the
continuous-book ledger remain owned by the existing replay kernel.
"""

from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd

from our_system_phase2.services.candidate_program_v1 import (
    CompiledCandidateProgramV1,
)
from our_system_phase2.services.real_market_validation import (
    evaluate_panel_expression,
)


def apply_compiled_candidate_program_v1(
    frame: pd.DataFrame,
    compiled: CompiledCandidateProgramV1,
    *,
    data_role: str,
    field_lags: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """Materialize program outputs without creating a second evaluator."""

    output = frame.copy()
    cache: dict[str, pd.Series] = {}
    expressions = dict(compiled.output_expressions)
    required = {
        "stock_score_node_id",
        "eligibility_mask_node_id",
        "exposure_multiplier_node_id",
        "veto_mask_node_id",
    }
    if set(expressions) != required:
        raise ValueError("compiled program lacks the exact four output expressions")

    score = pd.to_numeric(
        evaluate_panel_expression(
            output,
            expressions["stock_score_node_id"],
            cache=cache,
            field_lags=dict(field_lags or {}),
            data_role=data_role,
        ),
        errors="coerce",
    )
    eligibility = pd.to_numeric(
        evaluate_panel_expression(
            output,
            expressions["eligibility_mask_node_id"],
            cache=cache,
            field_lags=dict(field_lags or {}),
            data_role=data_role,
        ),
        errors="coerce",
    )
    exposure = pd.to_numeric(
        evaluate_panel_expression(
            output,
            expressions["exposure_multiplier_node_id"],
            cache=cache,
            field_lags=dict(field_lags or {}),
            data_role=data_role,
        ),
        errors="coerce",
    )
    veto = pd.to_numeric(
        evaluate_panel_expression(
            output,
            expressions["veto_mask_node_id"],
            cache=cache,
            field_lags=dict(field_lags or {}),
            data_role=data_role,
        ),
        errors="coerce",
    )
    eligible = (
        score.notna()
        & eligibility.gt(0.0)
        & exposure.notna()
        & exposure.ge(0.0)
        & veto.notna()
        & veto.le(0.0)
    )
    prior_universe = (
        output["universe_eligible"].fillna(False).astype(bool)
        if "universe_eligible" in output.columns
        else pd.Series(True, index=output.index)
    )
    final_eligible = eligible & prior_universe
    final_signal = (score * exposure).where(final_eligible)
    output["program_stock_score"] = score
    output["program_eligibility_mask"] = eligibility
    output["program_exposure_multiplier"] = exposure
    output["program_veto_mask"] = veto
    output["program_eligible"] = final_eligible
    output["signal"] = final_signal.replace([np.inf, -np.inf], np.nan)
    output["universe_eligible"] = final_eligible
    return output
