from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.services.a_share_executable_replay import (
    AShareCorporateActionPolicy,
    AShareExecutionPolicy,
    AShareFeeSchedule,
    ASharePortfolioDecoderPolicy,
    AShareUniversePolicy,
    ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    run_a_share_long_only_replay,
)
from our_system_phase2.services.candidate_program_adapters_v1 import (
    FrozenBroadEventComponentAdapter,
    IntradayStateComponentAdapter,
)
from our_system_phase2.services.candidate_program_clock_v1 import (
    resolve_joint_clock_coordinates_v1,
)
from our_system_phase2.services.candidate_program_controls_v1 import (
    construct_matched_control_program_v1,
)
from our_system_phase2.services.candidate_program_execution_v1 import (
    apply_compiled_candidate_program_v1,
)
from our_system_phase2.services.candidate_program_fixtures_v1 import (
    build_golden_program_fixtures_v1,
)
from our_system_phase2.services.candidate_program_v1 import (
    MatchedControlOperationV1,
    ProgramCompilerV1,
    ProgramOutputSpec,
    TypedNodeSpec,
)
from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.real_market_validation import evaluate_panel_expression
from our_system_phase2.services.real_market_validation import frozen_replay_channel
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY_PATH = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"


def _bind_synthetic_joint_clock(frame, compiled):
    bound = frame.copy()
    bound["program_coordinate_id"] = [f"row-{index}" for index in range(len(bound))]
    rows = {}
    for node_id in compiled.joint_clock_contract["component_clock_node_ids"]:
        component = []
        for index, source in bound.iterrows():
            observed = pd.Timestamp(source["date"]) + pd.Timedelta(hours=15)
            action = pd.Timestamp(source["date"]) + pd.Timedelta(days=1, hours=9, minutes=30)
            component.append(
                {
                    "coordinate_id": f"row-{index}",
                    "observable_at": observed.isoformat(),
                    "mature_at": observed.isoformat(),
                    "action_session": action.isoformat(),
                    "source_lag": 0,
                    "revision_policy": "NO_FUTURE_REVISION",
                    "support_present": True,
                    "eligible": True,
                }
            )
        rows[node_id] = component
    return bound, rows


@pytest.fixture(scope="module")
def program_context():
    registry = UnifiedCapabilityRegistry.read(REGISTRY_PATH)
    allowlists = json.loads(ROOT_CONTRACT.read_text(encoding="utf-8"))[
        "route_root_allowlists"
    ]
    grammar = CompositionalGrammarV2(registry, route_root_allowlist=allowlists)
    legacy = dict(grammar.propose("MINUTE_STATIC", attempt_index=0, seed=1729).primary)
    fixtures = {
        row.fixture_id: row
        for row in build_golden_program_fixtures_v1(
            registry, legacy_candidate=legacy
        )
    }
    return registry, grammar, legacy, fixtures


def test_all_golden_fixtures_are_explicit_and_authorized_ones_compile(program_context) -> None:
    registry, _, _, fixtures = program_context
    assert tuple(fixtures) == (
        "A_LEGACY_PARITY",
        "B_MULTI_TIMESCALE_FINANCING",
        "C_MARKET_CONDITIONED",
        "D_BILLBOARD_EVENT",
        "E_FULL_MULTI_COMPONENT",
        "F_FROZEN_BROAD_EVENT_GATE",
    )
    assert fixtures["D_BILLBOARD_EVENT"].status == "BLOCKED_FAIL_CLOSED"
    assert fixtures["D_BILLBOARD_EVENT"].blocker_code == (
        "BILLBOARD_EPISODE_AUTHORITY_NOT_REGISTERED"
    )
    for key, fixture in fixtures.items():
        if key == "D_BILLBOARD_EVENT":
            assert fixture.program is None
            continue
        compiled = ProgramCompilerV1(registry).compile(fixture.program)
        assert compiled.program_id == fixture.program.program_id
        assert set(compiled.output_expressions) == {
            "stock_score_node_id",
            "eligibility_mask_node_id",
            "exposure_multiplier_node_id",
            "veto_mask_node_id",
        }
    frozen = ProgramCompilerV1(registry).compile(
        fixtures["F_FROZEN_BROAD_EVENT_GATE"].program
    )
    assert "FROZEN_BROAD_EVENT_INVENTORY_BINDING" in (
        frozen.external_adapter_requirements
    )
    assert not any(value.startswith("broad_event_") for value in frozen.physical_leaf_ids)


def test_multifield_windows_and_market_condition_do_not_rank_market_payload(program_context) -> None:
    registry, _, _, fixtures = program_context
    b = fixtures["B_MULTI_TIMESCALE_FINANCING"].program
    compiled = ProgramCompilerV1(registry).compile(b)
    plan = {row["node_id"]: row for row in compiled.node_execution_plan}
    assert plan["net_buy_slope_5"]["compiled_expression"].endswith(",5)")
    assert plan["net_buy_slope_20"]["compiled_expression"].endswith(",20)")
    assert "CSResidual" in compiled.output_expressions["stock_score_node_id"]

    c = fixtures["C_MARKET_CONDITIONED"].program
    market_node = next(node for node in c.nodes if node.node_id == "market_up_count")
    direct_rank = TypedNodeSpec(
        node_id="illegal_market_rank",
        node_type="CSRANK",
        input_node_ids=(market_node.node_id,),
        parameters={},
        output_semantic_type="STOCK_SCORE",
        entity_scope="STOCK",
        temporal_semantics={"kind": "ILLEGAL_DIRECT_MAPPING"},
        observable_clock="prior_close",
        maturity="prior_close",
        unit_signature="dimensionless",
        support_unit="stock-session",
    )
    invalid = replace(
        c,
        nodes=c.nodes + (direct_rank,),
        outputs=replace(c.outputs, stock_score_node_id=direct_rank.node_id),
    )
    with pytest.raises(ValueError, match="may not enter direct stock"):
        ProgramCompilerV1(registry).compile(invalid)


def test_type_unit_future_and_unregistered_plate_fail_closed(program_context) -> None:
    registry, _, _, fixtures = program_context
    b = fixtures["B_MULTI_TIMESCALE_FINANCING"].program
    mismatch = replace(
        b,
        nodes=tuple(
            replace(node, input_node_ids=("scaled_short_long", "float_market_cap"))
            if node.node_id == "decongested_feature"
            else node
            for node in b.nodes
        ),
    )
    with pytest.raises(ValueError, match="identical units"):
        ProgramCompilerV1(registry).compile(mismatch)

    future = replace(
        b,
        nodes=tuple(
            replace(node, temporal_semantics={"uses_future_revision": True})
            if node.node_id == "financing_net_buy"
            else node
            for node in b.nodes
        ),
    )
    with pytest.raises(ValueError, match="future revision"):
        ProgramCompilerV1(registry).compile(future)

    plate = TypedNodeSpec(
        node_id="unauthorized_plate",
        node_type="PLATE_FIELD",
        input_node_ids=(),
        parameters={"field_id": "plate_membership_unregistered"},
        output_semantic_type="GROUP_VALUE",
        entity_scope="PLATE",
        temporal_semantics={"kind": "PIT"},
        observable_clock="prior_close",
        maturity="prior_close",
        unit_signature="group_id",
        support_unit="stock-session",
    )
    with pytest.raises(ValueError, match="not registered"):
        ProgramCompilerV1(registry).compile(replace(b, nodes=b.nodes + (plate,)))


def test_joint_clock_uses_coordinate_max_and_fails_closed_when_missing_or_late(program_context) -> None:
    _, _, _, fixtures = program_context
    contract = fixtures["B_MULTI_TIMESCALE_FINANCING"].program.joint_clock_contract
    contract = replace(contract, component_clock_node_ids=("a", "b"))
    rows = resolve_joint_clock_coordinates_v1(
        contract,
        {
            "a": [
                {
                    "coordinate_id": "x",
                    "observable_at": "2024-01-02 15:00",
                    "mature_at": "2024-01-02 15:00",
                    "action_session": "2024-01-03 09:30",
                    "source_lag": 1,
                    "revision_policy": "NO_FUTURE_REVISION",
                    "support_present": True,
                    "eligible": True,
                },
                {
                    "coordinate_id": "y",
                    "observable_at": "2024-01-02 15:00",
                    "mature_at": "2024-01-02 15:00",
                    "action_session": "2024-01-03 09:30",
                    "source_lag": 1,
                    "revision_policy": "NO_FUTURE_REVISION",
                    "support_present": True,
                    "eligible": True,
                },
            ],
            "b": [
                {
                    "coordinate_id": "x",
                    "observable_at": "2024-01-03 08:00",
                    "mature_at": "2024-01-03 08:30",
                    "action_session": "2024-01-03 09:30",
                    "source_lag": 0,
                    "revision_policy": "REGISTERED_EPISODE",
                    "support_present": True,
                    "eligible": True,
                },
                {
                    "coordinate_id": "late",
                    "observable_at": "2024-01-03 10:00",
                    "mature_at": "2024-01-03 10:00",
                    "action_session": "2024-01-03 09:30",
                    "source_lag": 0,
                    "revision_policy": "REGISTERED_EPISODE",
                    "support_present": True,
                    "eligible": True,
                },
            ],
        },
    )
    by_id = {row["coordinate_id"]: row for row in rows}
    assert by_id["x"]["eligible"] is True
    assert by_id["x"]["joint_eligible_from"] == "2024-01-03T08:30:00"
    assert len(by_id["x"]["component_clocks"]) == 2
    assert by_id["y"]["eligible"] is False
    assert by_id["y"]["missing_component_node_ids"] == ["b"]
    assert by_id["late"]["eligible"] is False


def test_controls_are_deterministic_type_preserving_and_wrong_lag_is_diagnostic(program_context) -> None:
    registry, _, _, fixtures = program_context
    primary = fixtures["B_MULTI_TIMESCALE_FINANCING"].program
    first = construct_matched_control_program_v1(primary)
    second = construct_matched_control_program_v1(primary)
    assert first.pair_id == second.pair_id
    assert first.primary.semantic_program_hash == primary.semantic_program_hash
    assert first.control.semantic_program_hash != primary.semantic_program_hash
    assert first.control.portfolio_contract == primary.portfolio_contract
    ProgramCompilerV1(registry).compile(first.control)

    operation = primary.matched_control_plan.operations[0]
    replacement = dict(operation.replacement["node"])
    replacement["maturity"] = "future_session"
    drifted = replace(
        primary,
        matched_control_plan=replace(
            primary.matched_control_plan,
            operations=(replace(operation, replacement={"node": replacement}),),
        ),
    )
    with pytest.raises(ValueError, match="protected contracts"):
        construct_matched_control_program_v1(drifted)
    with pytest.raises(ValueError, match="diagnostic-only"):
        MatchedControlOperationV1(
            operation="REPLACE_WITH_WRONG_LAG_CONTROL",
            target_node_ids=("net_buy_short_long",),
            replacement=operation.replacement,
            diagnostic_only=False,
        )


def test_control_operation_semantics_are_enforced_and_base_payload_is_real(program_context) -> None:
    _, _, _, fixtures = program_context
    primary = fixtures["B_MULTI_TIMESCALE_FINANCING"].program
    level_operation = primary.matched_control_plan.operations[0]
    target = level_operation.target_node_ids[0]
    base_only = replace(
        primary,
        matched_control_plan=replace(
            primary.matched_control_plan,
            operations=(
                MatchedControlOperationV1(
                    operation="BASE_PAYLOAD_ONLY",
                    target_node_ids=(),
                    replacement={
                        "nodes": {target: level_operation.replacement["node"]},
                        "base_payload_authority": "financing_net_buy",
                    },
                ),
            ),
        ),
    )
    pair = construct_matched_control_program_v1(base_only)
    assert len(pair.control.nodes) < len(primary.nodes)
    assert pair.control.semantic_program_hash != primary.semantic_program_hash

    mislabeled = replace(
        primary,
        matched_control_plan=replace(
            primary.matched_control_plan,
            operations=(replace(level_operation, operation="REMOVE_GATE"),),
        ),
    )
    with pytest.raises(ValueError, match="must target a GATE"):
        construct_matched_control_program_v1(mislabeled)

    retained_subgraph = replace(
        primary,
        matched_control_plan=replace(
            primary.matched_control_plan,
            operations=(replace(level_operation, operation="ABLATE_SUBGRAPH"),),
        ),
    )
    with pytest.raises(ValueError, match="may not retain the ablated subgraph"):
        construct_matched_control_program_v1(retained_subgraph)


def test_semantic_type_checks_reject_mask_arithmetic_and_bad_cs_output(program_context) -> None:
    registry, _, _, fixtures = program_context
    primary = fixtures["B_MULTI_TIMESCALE_FINANCING"].program
    identity = next(node for node in primary.nodes if node.node_id == "eligibility_identity")
    bad_arithmetic = TypedNodeSpec(
        node_id="bad_mask_add",
        node_type="ADD",
        input_node_ids=(identity.node_id, identity.node_id),
        parameters={},
        output_semantic_type="STOCK_VALUE",
        entity_scope="STOCK",
        temporal_semantics={"kind": "FIXED_PROGRAM_OPERATOR"},
        observable_clock=identity.observable_clock,
        maturity=identity.maturity,
        unit_signature="boolean",
        support_unit=identity.support_unit,
    )
    with pytest.raises(ValueError, match="require identical units"):
        ProgramCompilerV1(registry).compile(
            replace(primary, nodes=primary.nodes + (bad_arithmetic,))
        )

    score = next(node for node in primary.nodes if node.node_id == "final_score")
    bad_cross_section = replace(
        score, node_id="bad_cs_output", output_semantic_type="MARKET_VALUE"
    )
    with pytest.raises(ValueError, match="incompatible output type"):
        ProgramCompilerV1(registry).compile(
            replace(primary, nodes=primary.nodes + (bad_cross_section,))
        )


def test_frozen_broad_event_binding_must_match_registry_inventory(program_context) -> None:
    registry, _, _, fixtures = program_context
    primary = fixtures["F_FROZEN_BROAD_EVENT_GATE"].program
    frozen = next(
        node for node in primary.nodes if node.node_type == "FROZEN_BROAD_EVENT_REF"
    )
    spoofed = replace(
        frozen,
        parameters={**dict(frozen.parameters), "frozen_mechanism_id": "spoofed"},
    )
    with pytest.raises(ValueError, match="mechanism is not registry-authorized"):
        ProgramCompilerV1(registry).compile(
            replace(
                primary,
                nodes=tuple(spoofed if node.node_id == frozen.node_id else node for node in primary.nodes),
            )
        )
    with pytest.raises(ValueError, match="inventory hash is not registry-authorized"):
        FrozenBroadEventComponentAdapter(registry).adapt(
            node_id="bad_frozen",
            field_id=str(frozen.parameters["field_id"]),
            frozen_mechanism_id=str(frozen.parameters["frozen_mechanism_id"]),
            frozen_behavior_cluster_id=str(
                frozen.parameters["frozen_behavior_cluster_id"]
            ),
            frozen_inventory_hash="0" * 64,
        )


def test_intraday_adapter_uses_real_expression_leaves(program_context) -> None:
    _, grammar, _, _ = program_context
    candidate = dict(
        grammar.propose(
            "INTRADAY_STATE_TRANSITION", attempt_index=0, seed=1729
        ).primary
    )
    node = IntradayStateComponentAdapter().adapt(
        node_id="intraday_state", candidate=candidate
    )
    assert node.parameters["state_source_expression"]
    assert node.parameters["physical_leaf_ids"]
    assert not any(value.startswith("state_") for value in node.parameters["physical_leaf_ids"])


def test_authorized_joint_fixtures_execute_on_typed_synthetic_panel(program_context) -> None:
    registry, _, _, fixtures = program_context
    dates = pd.date_range("2024-01-02", periods=35, freq="B")
    rows = []
    frozen_program = fixtures["F_FROZEN_BROAD_EVENT_GATE"].program
    frozen_node = next(
        node for node in frozen_program.nodes if node.node_id == "frozen_broad_event_gate"
    )
    frozen_channel = frozen_replay_channel(
        frozen_node.parameters["field_id"], is_control=False
    )
    for day_index, date in enumerate(dates):
        for code_index in range(15):
            rows.append(
                {
                    "date": date,
                    "trade_time": date + pd.Timedelta(hours=15),
                    "code": f"{code_index:06d}",
                    "amount": float((day_index + 1) * (code_index + 2) ** 2),
                    "ctx_rzrq_rzjme": float(
                        (day_index + 2) ** 2 * (code_index + 1)
                    ),
                    "ctx_rzrq_rzyezb": 0.01 * (code_index + 1),
                    "ctx_hfq_float_market_cap_yuan": float(
                        1_000_000 + code_index * 100_000 + day_index * 100
                    ),
                    "ctx_hfq_turnover_ratio": 0.001 * (
                        1 + ((day_index + code_index) % 7)
                    ),
                    "ctx_holder_holder_num_change": 0.002 * (
                        code_index - 7
                    ),
                    "ctx_sent_up_num": 300.0,
                    "ctx_sent_down_num": 200.0,
                    "ctx_sent_zb_num": 10.0,
                    "fund_disclosure_holder_pulse": float(day_index in {21, 28}),
                    frozen_channel: float(code_index % 2 == 0),
                    "universe_eligible": True,
                }
            )
    frame = pd.DataFrame(rows)
    for fixture_id in (
        "B_MULTI_TIMESCALE_FINANCING",
        "C_MARKET_CONDITIONED",
        "E_FULL_MULTI_COMPONENT",
        "F_FROZEN_BROAD_EVENT_GATE",
    ):
        compiled = ProgramCompilerV1(registry).compile(fixtures[fixture_id].program)
        bound_frame, clock_rows = _bind_synthetic_joint_clock(frame, compiled)
        first = apply_compiled_candidate_program_v1(
            bound_frame,
            compiled,
            data_role="development",
            field_lags={},
            joint_clock_component_rows=clock_rows,
        )
        second = apply_compiled_candidate_program_v1(
            bound_frame,
            compiled,
            data_role="development",
            field_lags={},
            joint_clock_component_rows=clock_rows,
        )
        pd.testing.assert_series_equal(first["signal"], second["signal"])
        assert np.isfinite(first["signal"].dropna()).all()
        assert first["program_eligible"].any()


def _synthetic_replay_frame(expression: str) -> tuple[pd.DataFrame, pd.Series]:
    dates = pd.date_range("2024-01-02", periods=45, freq="B")
    rows = []
    for day_index, date in enumerate(dates):
        for code_index in range(12):
            base = 10.0 + code_index
            rows.append(
                {
                    "date": date,
                    "code": f"{code_index:06d}",
                    "amount": float((day_index + 1) * (code_index + 2) * 1000),
                    "open": base * (1.0 + 0.0005 * day_index),
                    "close": base * (1.0 + 0.0005 * day_index + 0.0001 * code_index),
                    "security_type": "A_SHARE",
                    "exchange": "SSE",
                    "universe_eligible": True,
                    "listing_age_sessions": 500,
                    "is_st": False,
                    "is_delisting": False,
                    "suspended": False,
                    "up_limit_price": base * 2.0,
                    "down_limit_price": base * 0.5,
                    "corporate_action_cash_per_share": 0.0,
                    "corporate_action_share_multiplier": 1.0,
                    "is_terminal_session": False,
                    "terminal_liquidation_price": float("nan"),
                    "lot_size": 100,
                }
            )
    frame = pd.DataFrame(rows)
    signal = evaluate_panel_expression(frame, expression, data_role="development")
    return frame, signal


def _fees() -> AShareFeeSchedule:
    return AShareFeeSchedule(
        commission_bps=2.5,
        minimum_commission_cny=5.0,
        exchange_handling_bps=0.341,
        transfer_fee_bps=0.1,
        sell_stamp_duty_bps=5.0,
        effective_start="2023-08-28",
        effective_end="2025-12-31",
        source_reference="synthetic_candidate_program_parity",
    )


def test_execution_enforces_compiled_joint_clock_before_signal(program_context) -> None:
    registry, _, legacy, fixtures = program_context
    compiled = ProgramCompilerV1(registry).compile(fixtures["A_LEGACY_PARITY"].program)
    frame, _ = _synthetic_replay_frame(legacy["canonical_expression"])
    bound, clock_rows = _bind_synthetic_joint_clock(frame, compiled)
    component_id = compiled.joint_clock_contract["component_clock_node_ids"][0]
    late_row = dict(clock_rows[component_id][0])
    late_row["mature_at"] = "2099-01-01T00:00:00"
    clock_rows[component_id][0] = late_row
    output = apply_compiled_candidate_program_v1(
        bound,
        compiled,
        data_role="development",
        field_lags={},
        joint_clock_component_rows=clock_rows,
    )
    assert output.loc[0, "program_joint_eligible"] == np.bool_(False)
    assert pd.isna(output.loc[0, "signal"])
    assert output.loc[1:, "program_joint_eligible"].all()

    with pytest.raises(ValueError, match="exact joint-clock rows"):
        apply_compiled_candidate_program_v1(
            bound,
            compiled,
            data_role="development",
            field_lags={},
            joint_clock_component_rows={},
        )


def test_legacy_program_signal_rank_and_replay_are_exactly_identical(program_context) -> None:
    registry, _, legacy, fixtures = program_context
    compiled = ProgramCompilerV1(registry).compile(fixtures["A_LEGACY_PARITY"].program)
    frame, legacy_signal = _synthetic_replay_frame(legacy["canonical_expression"])
    legacy_frame = frame.assign(signal=legacy_signal)
    bound_frame, clock_rows = _bind_synthetic_joint_clock(frame, compiled)
    wrapped = apply_compiled_candidate_program_v1(
        bound_frame,
        compiled,
        data_role="development",
        field_lags={},
        joint_clock_component_rows=clock_rows,
    )
    pd.testing.assert_series_equal(
        legacy_signal, wrapped["signal"], check_names=False, check_exact=True
    )
    pd.testing.assert_series_equal(
        legacy_signal.groupby(frame["date"]).rank(method="average"),
        wrapped["signal"].groupby(frame["date"]).rank(method="average"),
        check_names=False,
        check_exact=True,
    )
    assert wrapped["program_eligible"].equals(legacy_signal.notna())

    common = {
        "fee_schedule": _fees(),
        "universe_policy": AShareUniversePolicy(
            minimum_listing_sessions=60,
            source_reference="synthetic_candidate_program_parity",
        ),
        "execution_policy": AShareExecutionPolicy(),
        "corporate_action_policy": AShareCorporateActionPolicy(
            source_reference="synthetic_candidate_program_parity"
        ),
        "portfolio_decoder_policy": ASharePortfolioDecoderPolicy(
            decoder_id="TOPK_10_EQUAL",
            selection="TOP_K",
            top_k=10,
            weighting="EQUAL",
        ),
        "ending_book_policy": ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    }
    before = run_a_share_long_only_replay(legacy_frame, **common)
    after = run_a_share_long_only_replay(wrapped, **common)
    pd.testing.assert_frame_equal(before["fills"], after["fills"])
    pd.testing.assert_frame_equal(before["daily"], after["daily"])
    assert before["ending_nav_cny"] == after["ending_nav_cny"]
    assert before["total_fees_cny"] == after["total_fees_cny"]
    assert before["a_share_executable_net_reward"] == after[
        "a_share_executable_net_reward"
    ]
    assert before["accounting_invariants"] == after["accounting_invariants"]
