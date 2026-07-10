"""Curated semantic lanes for the repaired CN true-1min search.

These lanes widen around audited mechanisms without changing the optimizer
reward. They emit expressions only; Phase3CM remains the sole reward source.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from typing import Any

from our_system_phase2.services.typed_primitive_gate import validate_expression


FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


def _digest(expression: str) -> str:
    return hashlib.sha256(expression.encode("utf-8")).hexdigest()[:24]


def _field_set(expression: str) -> set[str]:
    return {field.lower() for field in FIELD_RE.findall(expression)}


def _eq_expressions() -> Iterable[tuple[str, str]]:
    relations = (
        ("ctx_sent_ditian_num", "vwap", "ditian_vwap"),
        ("ctx_sent_damian_num", "amount", "damian_amount"),
        ("ctx_sent_lb_h_num", "vwap", "high_board_vwap"),
        ("ctx_rzrq_rzjme", "amount", "rzjme_amount"),
    )
    windows = (10, 15, 20, 30, 40, 60, 90, 120)
    ratios = (0.4, 0.5, 0.6, 0.8)
    for context, target, motif in relations:
        for window in windows:
            for ratio in ratios:
                base = f"CSRank(MaskedCorr(${context},${target},{window},{ratio}))"
                yield base, motif
                yield f"Neg({base})", f"{motif}_inverse"
                for smooth in (5, 10, 20):
                    yield f"CSRank(Mean({base},{smooth}))", f"{motif}_smoothed"

    event_fields = (
        "evt_uplimit_up_limit_keep_times",
        "evt_uplimit_auction_turnover",
        "evt_uplimit_fd_close",
    )
    context_relations = (
        ("ctx_sent_ditian_num", "vwap"),
        ("ctx_sent_lb_h_num", "vwap"),
        ("ctx_rzrq_rqyl", "amount"),
    )
    for event in event_fields:
        for event_window in (5, 10, 20, 40):
            state = f"CSRank(WindowStateCount(${event},{event_window}))"
            for context, target in context_relations:
                for window in (10, 20, 40, 60):
                    for ratio in (0.4, 0.6, 0.8):
                        relation = f"CSRank(MaskedCorr(${context},${target},{window},{ratio}))"
                        yield f"CSRank(Mul({state},{relation}))", "event_relation_product"
                        yield f"CSRank(Add({state},{relation}))", "event_relation_add"
                        yield f"CSRank(Sub({state},{relation}))", "event_relation_spread"


def _x0_expressions() -> Iterable[tuple[str, str]]:
    windows = (5, 8, 10, 13, 20, 21, 30, 34, 40, 60, 90, 120)
    for window in windows:
        for lag in (1, 2, 3, 5):
            yield f"CSRank(ZScore(Mean(Abs(Delta($vwap,{lag})),{window})))", "x0_001_vwap_variation"
            yield f"CSRank(ZScore(Mean(Abs(Delta($close,{lag})),{window})))", "x0_009_close_variation"
        yield f"CSRank(Mul(CSRank($open),CSRank(Mean($amount,{window}))))", "x0_002_open_amount"
        yield f"CSRank(Mul(CSRank($m1_first30_vwap),CSRank(Mean($amount,{window}))))", "x0_002_opening_vwap_amount"
        for other in windows:
            yield (
                f"CSRank(Mul(CSRank(Std($open,{window})),ZScore(Mean(Abs(Delta($vwap,1)),{other}))))",
                "x0_005_open_volatility_vwap_variation",
            )
            yield (
                f"CSRank(Mul(ZScore(Mean(Abs($close),{window})),ZScore(Mean(Abs($amount),{other}))))",
                "x0_006_price_amount_level",
            )
        for ratio in (0.5, 0.6, 0.8):
            size = f"MaskedZScore($ctx_hfq_float_market_cap_yuan,{window},{ratio})"
            yield f"CSRank(Mul(ZScore(Mean($close,{window})),{size}))", "x0_004_price_size"
            yield (
                f"CSRank(Mul(CSRank(SafeCSResidual(CSRank($close),CSRank(ValidRatioGate($ctx_hfq_market_cap_yuan,{window},{ratio})))),ZScore(Mean(Abs(Delta($close,1)),{window}))))",
                "x0_009_size_residual_close_variation",
            )


def _industry_expressions() -> Iterable[tuple[str, str]]:
    fields = (
        "ctx_industry_member_count",
        "ctx_industry_pct_chg_mean",
        "ctx_industry_pct_chg_median",
        "ctx_industry_pct_chg_std",
        "ctx_industry_up_ratio",
        "ctx_industry_turnover_ratio_mean",
        "ctx_industry_volume_ratio_mean",
        "ctx_industry_pct_chg_rank",
        "ctx_industry_up_ratio_rank",
        "ctx_industry_stock_pct_chg_residual",
        "ctx_industry_amount_share",
    )
    targets = ("vwap", "amount", "volume", "m1_first30_vwap")
    windows = (5, 10, 15, 20, 30, 40, 60, 90, 120)
    ratios = (0.5, 0.6, 0.8)
    for field in fields:
        for window in windows:
            for ratio in ratios:
                guarded = f"ValidRatioGate(${field},{window},{ratio})"
                ranked = f"CSRank({guarded})"
                yield ranked, "industry_level"
                yield f"Neg({ranked})", "industry_level_inverse"
                yield f"CSRank(MaskedZScore(${field},{window},{ratio}))", "industry_standardized"
                for target in targets:
                    relation = f"CSRank(MaskedCorr(${field},${target},{window},{ratio}))"
                    yield relation, "industry_price_relation"
                    yield f"Neg({relation})", "industry_price_relation_inverse"
        for other in fields:
            if other <= field:
                continue
            for window in (10, 20, 40, 60):
                left = f"CSRank(ValidRatioGate(${field},{window},0.6))"
                right = f"CSRank(ValidRatioGate(${other},{window},0.6))"
                yield f"CSRank(Mul({left},{right}))", "industry_internal_interaction"
                yield f"CSRank(Sub({left},{right}))", "industry_internal_spread"


def generate_curated_lane_candidates(
    arm_id: str,
    *,
    budget: int,
    blocked: set[str],
    available_fields: Iterable[str] | None,
) -> list[dict[str, Any]]:
    generators = {
        "eq_mechanism_deepen": _eq_expressions,
        "x0_true1min_reexpression": _x0_expressions,
        "industry_context_canary": _industry_expressions,
    }
    generator = generators.get(arm_id)
    if generator is None or budget <= 0:
        return []

    available = {str(field).lower() for field in (available_fields or [])}
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for expression, motif in generator():
        digest = _digest(expression)
        if digest in seen or digest in blocked or f"phase3bp:{digest}" in blocked:
            continue
        if available and not _field_set(expression).issubset(available):
            continue
        verdict = validate_expression(
            expression,
            entry_lineage=arm_id,
            materialization_stage="phase3cp_curated_semantic_lane",
            candidate_role="research_canary" if arm_id == "industry_context_canary" else "search_candidate",
        )
        if verdict.typed_gate_decision != "allow":
            continue
        seen.add(digest)
        rows.append(
            {
                "expression": expression,
                "expression_hash": digest,
                "candidate_hash": digest,
                "policy_score": 0.35,
                "factor_lane": arm_id,
                "field_family": arm_id,
                "primitive_family": motif,
                "family_id": hashlib.sha1(f"{arm_id}|{motif}".encode("utf-8")).hexdigest()[:18],
                "motif_id": motif,
                "candidate_role": "research_canary" if arm_id == "industry_context_canary" else "search_candidate",
                "promotion_boundary": (
                    "research_canary_only_static_industry_taxonomy_not_full_pit"
                    if arm_id == "industry_context_canary"
                    else "phase3cm_reward_required"
                ),
            }
        )
        if len(rows) >= int(budget):
            break
    return rows
