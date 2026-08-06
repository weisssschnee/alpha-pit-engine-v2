"""Hand-built, performance-dark golden fixtures for candidate-program V1."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping

from our_system_phase2.services.candidate_program_adapters_v1 import (
    DisclosureEpisodeAdapter,
    FrozenBroadEventComponentAdapter,
    FundamentalRepresentationAdapter,
    LegacyRouteComponentAdapter,
    MarketConditionAdapter,
)
from our_system_phase2.services.candidate_program_v1 import (
    PROGRAM_SCHEMA_VERSION,
    CandidateProgramSpecV1,
    ComplexityBudgetV1,
    JointClockContractV1,
    MatchedControlOperationV1,
    MatchedControlPlanV1,
    ProgramOutputSpec,
    TypedNodeSpec,
    legacy_candidate_program_v1,
)
from our_system_phase2.services.fixed_stratified_candidate_sampling import (
    _broad_event_inventory_hash,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


PORTFOLIO_CONTRACT_V1 = {
    "decoder_id": "TOPK_10_EQUAL",
    "execution": "PRIOR_CLOSE_SIGNAL_NEXT_OPEN_T_PLUS_1",
    "fees": "EXISTING_FROZEN_A_SHARE_FEES",
    "tradability": "EXISTING_A_SHARE_TRADABILITY",
    "ledger": "EXISTING_CONTINUOUS_BOOK_FINAL_CLOSE_MTM",
}


@dataclass(frozen=True, slots=True)
class GoldenProgramFixtureV1:
    fixture_id: str
    explanation_zh: str
    status: str
    program: CandidateProgramSpecV1 | None
    blocker_code: str = ""

    def to_record(self) -> dict[str, Any]:
        return {
            "fixture_id": self.fixture_id,
            "explanation_zh": self.explanation_zh,
            "status": self.status,
            "blocker_code": self.blocker_code,
            "program": self.program.to_record() if self.program is not None else None,
        }


def _constant(
    node_id: str,
    *,
    value: bool | float,
    output_type: str,
) -> TypedNodeSpec:
    return TypedNodeSpec(
        node_id=node_id,
        node_type="CONSTANT",
        input_node_ids=(),
        parameters={"value": value},
        output_semantic_type=output_type,
        entity_scope="CONSTANT",
        temporal_semantics={"kind": "IDENTITY_OUTPUT", "uses_future_revision": False},
        observable_clock="prior_close",
        maturity="prior_close",
        unit_signature="boolean" if isinstance(value, bool) else "dimensionless",
        support_unit="stock-session",
    )


def _operator(
    node_id: str,
    node_type: str,
    inputs: tuple[str, ...],
    *,
    output_type: str = "STOCK_VALUE",
    entity_scope: str = "STOCK",
    unit: str = "dimensionless",
    parameters: Mapping[str, Any] | None = None,
    source_lineage: tuple[str, ...] = (),
    provenance: tuple[str, ...] = (),
    support_unit: str = "stock-session",
) -> TypedNodeSpec:
    return TypedNodeSpec(
        node_id=node_id,
        node_type=node_type,
        input_node_ids=inputs,
        parameters=dict(parameters or {}),
        output_semantic_type=output_type,
        entity_scope=entity_scope,
        temporal_semantics={"kind": "FIXED_PROGRAM_OPERATOR", "uses_future_revision": False},
        observable_clock="prior_close",
        maturity="prior_close",
        unit_signature=unit,
        support_unit=support_unit,
        source_lineage=source_lineage,
        component_route_provenance=provenance,
    )


def _bind_operator_provenance(
    nodes: tuple[TypedNodeSpec, ...],
) -> tuple[TypedNodeSpec, ...]:
    """Derive every operator's lineage and stratum provenance from its parents."""

    bound: dict[str, TypedNodeSpec] = {}
    output: list[TypedNodeSpec] = []
    for node in nodes:
        if not node.input_node_ids:
            updated = node
        else:
            parents = [bound[parent_id] for parent_id in node.input_node_ids]
            updated = replace(
                node,
                source_lineage=tuple(
                    sorted(
                        {
                            identity
                            for parent in parents
                            for identity in parent.source_lineage
                        }
                    )
                ),
                component_route_provenance=tuple(
                    sorted(
                        {
                            route
                            for parent in parents
                            for route in parent.component_route_provenance
                        }
                    )
                ),
            )
        bound[updated.node_id] = updated
        output.append(updated)
    return tuple(output)


def _control_plan(short_node: TypedNodeSpec) -> MatchedControlPlanV1:
    replacement = _operator(
        short_node.node_id,
        "ROLLING_MEAN",
        ("financing_net_buy",),
        output_type=short_node.output_semantic_type,
        unit=short_node.unit_signature,
        parameters={"window": 5},
        source_lineage=short_node.source_lineage,
        provenance=short_node.component_route_provenance,
        support_unit=short_node.support_unit,
    )
    return MatchedControlPlanV1(
        control_constructor_id="CN_TYPED_PROGRAM_LEVEL_REPLACEMENT_V1",
        operations=(
            MatchedControlOperationV1(
                operation="REPLACE_WITH_LEVEL",
                target_node_ids=(short_node.node_id,),
                replacement={"node": replacement.semantic_payload()},
            ),
        ),
        pair_support_policy="PRIMARY_CONTROL_EXACT_COORDINATE_INTERSECTION",
        pair_maturity_policy="MAX_PRIMARY_CONTROL_MATURITY_BEFORE_SHARED_SUPPORT",
    )


def _program(
    nodes: tuple[TypedNodeSpec, ...],
    *,
    score: str,
    eligibility: str,
    exposure: str,
    veto: str,
    clock_nodes: tuple[str, ...],
    control_plan: MatchedControlPlanV1,
    legacy: tuple[str, ...] = (),
    frozen: tuple[str, ...] = (),
) -> CandidateProgramSpecV1:
    nodes = _bind_operator_provenance(nodes)
    return CandidateProgramSpecV1(
        schema_version=PROGRAM_SCHEMA_VERSION,
        nodes=nodes,
        outputs=ProgramOutputSpec(
            stock_score_node_id=score,
            eligibility_mask_node_id=eligibility,
            exposure_multiplier_node_id=exposure,
            veto_mask_node_id=veto,
        ),
        portfolio_contract=PORTFOLIO_CONTRACT_V1,
        joint_clock_contract=JointClockContractV1(
            component_clock_node_ids=clock_nodes,
            missing_component_policy="FAIL_CLOSED_REQUIRED_COMPONENT",
            action_session_policy="PRIOR_CLOSE_SIGNAL_NEXT_OPEN_ACTION",
            pit_guard_version="CN_TYPED_PROGRAM_JOINT_CLOCK_V1",
        ),
        complexity_budget=ComplexityBudgetV1(),
        matched_control_plan=control_plan,
        legacy_component_provenance=legacy,
        frozen_component_references=frozen,
    )


def _fixture_b_nodes(
    registry: UnifiedCapabilityRegistry,
) -> tuple[tuple[TypedNodeSpec, ...], TypedNodeSpec]:
    adapter = FundamentalRepresentationAdapter(registry)
    net_buy = adapter.adapt(
        node_id="financing_net_buy",
        field_id="ctx_rzrq_rzjme",
        route_id="SLOW_TEMPORAL_CHANGE",
        unit_signature="yuan",
    )
    balance_ratio = adapter.adapt(
        node_id="financing_balance_ratio",
        field_id="ctx_rzrq_rzyezb",
        unit_signature="dimensionless",
    )
    float_cap = adapter.adapt(
        node_id="float_market_cap",
        field_id="ctx_hfq_float_market_cap_yuan",
        unit_signature="yuan",
    )
    turnover = adapter.adapt(
        node_id="turnover_ratio",
        field_id="ctx_hfq_turnover_ratio",
        unit_signature="dimensionless",
    )
    lineage = net_buy.source_lineage
    provenance = ("SLOW_TEMPORAL_CHANGE",)
    slope_5 = _operator(
        "net_buy_slope_5", "SLOPE", (net_buy.node_id,), unit="yuan",
        parameters={"window": 5}, source_lineage=lineage, provenance=provenance,
        support_unit=net_buy.support_unit,
    )
    slope_20 = _operator(
        "net_buy_slope_20", "SLOPE", (net_buy.node_id,), unit="yuan",
        parameters={"window": 20}, source_lineage=lineage, provenance=provenance,
        support_unit=net_buy.support_unit,
    )
    short_long = _operator(
        "net_buy_short_long", "SHORT_LONG_SPREAD", (net_buy.node_id,), unit="yuan",
        parameters={"short_window": 5, "long_window": 20},
        source_lineage=lineage, provenance=provenance,
        support_unit=net_buy.support_unit,
    )
    scaled = _operator(
        "scaled_short_long", "SAFE_DIVIDE", (short_long.node_id, float_cap.node_id),
        unit="dimensionless", parameters={"floor": 1.0},
    )
    decongested = _operator(
        "decongested_feature", "SUBTRACT", (scaled.node_id, balance_ratio.node_id),
    )
    residual = _operator(
        "residual_feature", "RESIDUALIZE",
        (decongested.node_id, float_cap.node_id, turnover.node_id),
    )
    score = _operator(
        "final_score", "CSRANK", (residual.node_id,), output_type="STOCK_SCORE"
    )
    bound_nodes = _bind_operator_provenance(
        (
            net_buy,
            balance_ratio,
            float_cap,
            turnover,
            slope_5,
            slope_20,
            short_long,
            scaled,
            decongested,
            residual,
            score,
        )
    )
    return (
        bound_nodes,
        next(node for node in bound_nodes if node.node_id == short_long.node_id),
    )


def build_golden_program_fixtures_v1(
    registry: UnifiedCapabilityRegistry,
    *,
    legacy_candidate: Mapping[str, Any],
) -> tuple[GoldenProgramFixtureV1, ...]:
    """Return frozen A-F fixtures without reading any financial data."""

    identity_mask = _constant("eligibility_identity", value=True, output_type="STOCK_MASK")
    exposure = _constant("exposure_identity", value=1.0, output_type="STOCK_MULTIPLIER")
    veto_identity = _constant("veto_identity", value=False, output_type="STOCK_MASK")

    fixture_a = GoldenProgramFixtureV1(
        fixture_id="A_LEGACY_PARITY",
        explanation_zh="单一既有 route 候选；程序层不得改变其身份或执行语义。",
        status="COMPILE_READY",
        program=legacy_candidate_program_v1(
            legacy_candidate, portfolio_contract=PORTFOLIO_CONTRACT_V1
        ),
    )

    b_nodes, short_node = _fixture_b_nodes(registry)
    b_program = _program(
        b_nodes + (identity_mask, exposure, veto_identity),
        score="final_score",
        eligibility=identity_mask.node_id,
        exposure=exposure.node_id,
        veto=veto_identity.node_id,
        clock_nodes=(
            "financing_net_buy",
            "financing_balance_ratio",
            "float_market_cap",
            "turnover_ratio",
        ),
        control_plan=_control_plan(short_node),
    )
    fixture_b = GoldenProgramFixtureV1(
        fixture_id="B_MULTI_TIMESCALE_FINANCING",
        explanation_zh="融资净买额的 5/20 日尺度差，经流通市值缩放、融资拥挤扣除及市值/换手残差化后横截面排序。",
        status="COMPILE_READY",
        program=b_program,
    )

    market = MarketConditionAdapter(registry)
    up = market.adapt(node_id="market_up_count", field_id="ctx_sent_up_num")
    down = market.adapt(node_id="market_down_count", field_id="ctx_sent_down_num")
    board = market.adapt(node_id="market_board_count", field_id="ctx_sent_zb_num")
    breadth = _operator(
        "market_breadth_gate", "STATE", (up.node_id, down.node_id),
        output_type="MARKET_MASK", entity_scope="MARKET", unit="boolean",
        parameters={"comparison": "LEFT_GT_RIGHT"},
        provenance=("MARKET_REGIME_CONDITION",),
    )
    low_board = _operator(
        "low_board_density", "LIMIT_DENSITY", (board.node_id, _constant(
            "board_threshold", value=30.0, output_type="SCALAR"
        ).node_id),
        output_type="MARKET_MASK", entity_scope="MARKET", unit="boolean",
        parameters={"comparison": "LEFT_LT_RIGHT"},
        provenance=("MARKET_REGIME_CONDITION",),
    )
    board_threshold = _constant("board_threshold", value=30.0, output_type="SCALAR")
    market_intersection = _operator(
        "market_condition", "INTERSECT", (breadth.node_id, low_board.node_id),
        output_type="MARKET_MASK", entity_scope="MARKET", unit="boolean",
    )
    eligible_market = _operator(
        "market_eligibility", "FILTER", (identity_mask.node_id, market_intersection.node_id),
        output_type="STOCK_MASK", unit="boolean",
    )
    c_nodes = b_nodes + (
        identity_mask,
        exposure,
        veto_identity,
        up,
        down,
        board,
        board_threshold,
        breadth,
        low_board,
        market_intersection,
        eligible_market,
    )
    fixture_c = GoldenProgramFixtureV1(
        fixture_id="C_MARKET_CONDITIONED",
        explanation_zh="Fixture B 仅在上涨家数超过下跌家数且炸板密度低时进入组合；市场字段不直接参与股票横截面排序。",
        status="COMPILE_READY",
        program=_program(
            c_nodes,
            score="final_score",
            eligibility=eligible_market.node_id,
            exposure=exposure.node_id,
            veto=veto_identity.node_id,
            clock_nodes=(
                "financing_net_buy", "financing_balance_ratio", "float_market_cap",
                "turnover_ratio", up.node_id, down.node_id, board.node_id,
            ),
            control_plan=_control_plan(short_node),
        ),
    )

    fixture_d = GoldenProgramFixtureV1(
        fixture_id="D_BILLBOARD_EVENT",
        explanation_zh="龙虎榜 FirstHit/EventAge/去重事件设想被冻结，但当前 registry 没有龙虎榜 episode authority，不能把每日状态列冒充事件生命周期。",
        status="BLOCKED_FAIL_CLOSED",
        blocker_code="BILLBOARD_EPISODE_AUTHORITY_NOT_REGISTERED",
        program=None,
    )

    holder = FundamentalRepresentationAdapter(registry).adapt(
        node_id="holder_count_change",
        field_id="ctx_holder_holder_num_change",
        route_id="SLOW_TEMPORAL_CHANGE",
        unit_signature="dimensionless",
    )
    combined = _operator(
        "holder_financing_combined", "ADD", ("decongested_feature", holder.node_id)
    )
    full_residual = _operator(
        "full_residual_feature", "RESIDUALIZE",
        (combined.node_id, "float_market_cap", "turnover_ratio"),
    )
    full_score = _operator(
        "full_final_score", "CSRANK", (full_residual.node_id,), output_type="STOCK_SCORE"
    )
    episode = DisclosureEpisodeAdapter(registry).adapt(
        node_id="holder_disclosure_episode",
        field_id="fund_disclosure_holder_pulse",
        observable_cutoff="SOURCE_DISCLOSURE_CLOSE",
        registered_action_delay="NEXT_SESSION_OPEN",
    )
    first_vote = _operator(
        "holder_episode_first_vote", "REPEATED_EVENT_SUPPRESSION", (episode.node_id,),
        output_type="STOCK_MASK", unit="boolean",
        provenance=("DISCLOSURE_EVENT",),
    )
    full_eligibility = _operator(
        "full_eligibility", "FILTER",
        (identity_mask.node_id, market_intersection.node_id, first_vote.node_id),
        output_type="STOCK_MASK", unit="boolean",
    )
    high_board = _operator(
        "high_board_density", "LIMIT_DENSITY", (board.node_id, board_threshold.node_id),
        output_type="MARKET_MASK", entity_scope="MARKET", unit="boolean",
        parameters={"comparison": "LEFT_GT_RIGHT"},
        provenance=("MARKET_REGIME_CONDITION",),
    )
    high_board_veto = _operator(
        "high_board_veto", "VETO", (high_board.node_id,),
        output_type="STOCK_MASK", unit="boolean",
    )
    e_nodes = b_nodes + (
        holder,
        combined,
        full_residual,
        full_score,
        identity_mask,
        exposure,
        up,
        down,
        board,
        board_threshold,
        breadth,
        low_board,
        market_intersection,
        episode,
        first_vote,
        full_eligibility,
        high_board,
        high_board_veto,
    )
    fixture_e = GoldenProgramFixtureV1(
        fixture_id="E_FULL_MULTI_COMPONENT",
        explanation_zh="股东户数变化与融资短长差联合，加入市值/换手残差、市场宽度、炸板 veto 和一次一票的股东披露 episode。",
        status="COMPILE_READY",
        program=_program(
            e_nodes,
            score=full_score.node_id,
            eligibility=full_eligibility.node_id,
            exposure=exposure.node_id,
            veto=high_board_veto.node_id,
            clock_nodes=(
                "financing_net_buy", "financing_balance_ratio", "float_market_cap",
                "turnover_ratio", holder.node_id, up.node_id, down.node_id,
                board.node_id, episode.node_id,
            ),
            control_plan=_control_plan(short_node),
        ),
    )

    frozen_fields = registry.fields_for_route(
        "BROAD_EVENT_FROZEN_ENTRY", entity_scopes=("STOCK",)
    )
    if not frozen_fields:
        raise ValueError("frozen Broad Event registry is empty")
    frozen_field = frozen_fields[0]
    mechanism = dict((frozen_field.metadata or {}).get("frozen_mechanism") or {})
    frozen_inventory_hash = _broad_event_inventory_hash(registry)
    broad_gate = FrozenBroadEventComponentAdapter(registry).adapt(
        node_id="frozen_broad_event_gate",
        field_id=frozen_field.field_id,
        frozen_mechanism_id=str(mechanism.get("mechanism_id") or ""),
        frozen_behavior_cluster_id=str(mechanism.get("behavior_cluster_id") or ""),
        frozen_inventory_hash=frozen_inventory_hash,
        output_semantic_type="STOCK_MASK",
    )
    legacy_score = LegacyRouteComponentAdapter().adapt(
        node_id="legacy_base_score", candidate=legacy_candidate
    )
    frozen_eligibility = _operator(
        "frozen_event_eligibility", "FILTER",
        (identity_mask.node_id, broad_gate.node_id),
        output_type="STOCK_MASK", unit="boolean",
    )
    fixture_f = GoldenProgramFixtureV1(
        fixture_id="F_FROZEN_BROAD_EVENT_GATE",
        explanation_zh="既有冻结 Broad Event 仅作为旧横截面 score 的 eligibility gate；不生成新机制，也不获得 discovery credit。",
        status="COMPILE_READY",
        program=_program(
            (legacy_score, broad_gate, identity_mask, exposure, veto_identity, frozen_eligibility),
            score=legacy_score.node_id,
            eligibility=frozen_eligibility.node_id,
            exposure=exposure.node_id,
            veto=veto_identity.node_id,
            clock_nodes=(legacy_score.node_id, broad_gate.node_id),
            control_plan=MatchedControlPlanV1(
                control_constructor_id="LEGACY_REGISTERED_CONTROL_PLUS_FROZEN_GATE",
                operations=(),
                pair_support_policy="PRIMARY_CONTROL_EXACT_COORDINATE_INTERSECTION",
                pair_maturity_policy="MAX_PRIMARY_CONTROL_MATURITY_BEFORE_SHARED_SUPPORT",
            ),
            legacy=(str(legacy_candidate.get("candidate_id") or ""),),
            frozen=(frozen_field.field_id, frozen_inventory_hash),
        ),
    )
    return fixture_a, fixture_b, fixture_c, fixture_d, fixture_e, fixture_f
