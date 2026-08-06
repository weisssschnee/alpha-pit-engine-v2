"""Thin execution bridge from compiled program outputs to existing replay.

The bridge evaluates the four compiled expressions with the existing panel
expression evaluator, then writes the already-supported ``signal`` and
``universe_eligible`` columns.  Selection, T+1, fees, tradability and the
continuous-book ledger remain owned by the existing replay kernel.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from our_system_phase2.services.candidate_program_v1 import (
    CompiledCandidateProgramV1,
    JointClockContractV1,
)
from our_system_phase2.services.candidate_program_clock_v1 import (
    resolve_joint_clock_coordinates_v1,
)
from our_system_phase2.services.real_market_validation import (
    evaluate_panel_expression,
)


def apply_compiled_candidate_program_v1(
    frame: pd.DataFrame,
    compiled: CompiledCandidateProgramV1,
    *,
    data_role: str,
    joint_clock_component_rows: Mapping[
        str, Sequence[Mapping[str, Any]]
    ] | None = None,
    coordinate_id_column: str = "program_coordinate_id",
    materialized_sidecar_clock_column: str | None = None,
    materialized_sidecar_authority: str | None = None,
) -> pd.DataFrame:
    """Materialize program outputs without creating a second evaluator."""

    output = frame.copy(deep=False)
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
    contract = JointClockContractV1(**dict(compiled.joint_clock_contract))
    expected_clock_nodes = set(contract.component_clock_node_ids)
    if joint_clock_component_rows is not None:
        if materialized_sidecar_clock_column is not None:
            raise ValueError("execution may bind only one joint-clock authority")
        if set(joint_clock_component_rows) != expected_clock_nodes:
            raise ValueError(
                "execution requires exact joint-clock rows for every declared component"
            )
        if coordinate_id_column not in output.columns:
            raise ValueError("execution frame lacks the bound program coordinate id")
        joint_clock = resolve_joint_clock_coordinates_v1(
            contract,
            joint_clock_component_rows,
            component_requirements=compiled.component_clock_requirements,
        )
        clock_by_coordinate = {
            str(row["coordinate_id"]): row for row in joint_clock
        }
        frame_coordinates = output[coordinate_id_column].astype(str)
        unbound_coordinates = sorted(
            set(frame_coordinates) - set(clock_by_coordinate)
        )
        if unbound_coordinates:
            raise ValueError(
                "execution frame contains coordinates absent from joint-clock authority"
            )
        joint_eligible = frame_coordinates.map(
            lambda value: bool(clock_by_coordinate[value]["eligible"])
        )
        joint_eligible_from = frame_coordinates.map(
            lambda value: clock_by_coordinate[value]["joint_eligible_from"]
        )
    else:
        if materialized_sidecar_clock_column is None:
            raise ValueError("execution requires an explicit joint-clock authority")
        if materialized_sidecar_authority != "PIT_MATERIALIZED_FIELD_SIDECAR":
            raise ValueError("materialized sidecar clock authority is not accepted")
        if materialized_sidecar_clock_column not in output.columns:
            raise ValueError("materialized sidecar lacks its bound clock column")
        materialized_clock = pd.to_datetime(
            output[materialized_sidecar_clock_column], errors="coerce"
        )
        joint_eligible = materialized_clock.notna()
        for node_id in contract.component_clock_node_ids:
            requirement = dict(compiled.component_clock_requirements[node_id])
            revision_policy = str(requirement.get("revision_policy") or "")
            if (
                not revision_policy
                or "FUTURE_REVISION" in revision_policy.upper()
                and "NO_FUTURE_REVISION" not in revision_policy.upper()
            ):
                raise ValueError("materialized sidecar component permits future revision")
            field_requirements = dict(requirement.get("field_requirements") or {})
            missing = sorted(set(field_requirements) - set(output.columns))
            if missing:
                raise ValueError(
                    f"materialized sidecar lacks component fields for {node_id}: {missing}"
                )
            if field_requirements:
                joint_eligible &= output[list(field_requirements)].notna().all(axis=1)
        joint_eligible_from = materialized_clock.where(joint_eligible).map(
            lambda value: value.isoformat() if pd.notna(value) else None
        )

    score = pd.to_numeric(
        evaluate_panel_expression(
            output,
            expressions["stock_score_node_id"],
            cache=cache,
            field_lags=dict(compiled.field_lags),
            data_role=data_role,
        ),
        errors="coerce",
    )
    eligibility = pd.to_numeric(
        evaluate_panel_expression(
            output,
            expressions["eligibility_mask_node_id"],
            cache=cache,
            field_lags=dict(compiled.field_lags),
            data_role=data_role,
        ),
        errors="coerce",
    )
    exposure = pd.to_numeric(
        evaluate_panel_expression(
            output,
            expressions["exposure_multiplier_node_id"],
            cache=cache,
            field_lags=dict(compiled.field_lags),
            data_role=data_role,
        ),
        errors="coerce",
    )
    veto = pd.to_numeric(
        evaluate_panel_expression(
            output,
            expressions["veto_mask_node_id"],
            cache=cache,
            field_lags=dict(compiled.field_lags),
            data_role=data_role,
        ),
        errors="coerce",
    )
    eligible = (
        joint_eligible
        & score.notna()
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
    output["program_joint_eligible"] = joint_eligible
    output["program_joint_eligible_from"] = joint_eligible_from
    output["program_eligible"] = final_eligible
    output["signal"] = final_signal.replace([np.inf, -np.inf], np.nan)
    output["universe_eligible"] = final_eligible
    return output
