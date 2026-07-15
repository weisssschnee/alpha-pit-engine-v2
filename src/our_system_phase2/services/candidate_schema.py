"""Shared candidate schema helpers for true1min reward feedback.

The search chain has several generator families (BS/CEM/UCB, BT/AST, BU
variants, future scheduler arms). This module provides a small, deterministic
schema layer so downstream CA/CM/CN routes can compare rows without relying on
route-specific column names.
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import Any


FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")

OPTIMIZER_REWARD_METRIC = "train_portfolio_sortino_rankic_regime_composite_reward"


CANONICAL_CANDIDATE_FIELDS = [
    "candidate_id",
    "pair_id",
    "pair_member_role",
    "primary_candidate_id",
    "control_candidate_id",
    "primary_receipt_hash",
    "control_receipt_hash",
    "pair_receipt_hash",
    "control_constructor_id",
    "pair_evaluation_status",
    "pair_evaluation_blockers",
    "pair_train_reward",
    "pair_train_reward_decision",
    "pair_train_reward_blockers",
    "pair_turnover_metric",
    "pair_support_metric",
    "pair_rank_ic_metric",
    "expression_hash",
    "expression",
    "generator_arm",
    "generator_route",
    "seed",
    "round_id",
    "parent_id",
    "mutation_type",
    "field_family",
    "primitive_family",
    "event_state_family",
    "horizon_bucket",
    "turnover_bucket",
    "family_id",
    "motif_id",
    "subtree_hashes",
    "proxy_quality",
    "aligned_ic_mean",
    "spread_hit_rate",
    "mean_one_way_turnover",
    "blocker_flags",
    "phase3ca_proxy_quality",
    "train_reward",
    "train_reward_decision",
    "train_reward_blockers",
    "optimizer_reward",
    "optimizer_reward_source",
    "optimizer_reward_metric",
    "optimizer_reward_split",
    "primary_train_reward",
    "control_train_reward",
    "matched_train_increment",
    "primary_turnover",
    "control_turnover",
    "matched_turnover_increment",
    "primary_rank_ic",
    "control_rank_ic",
    "matched_rank_ic_increment",
    "primary_behavior_identity",
    "control_behavior_identity",
    "primary_signal_value_identity",
    "control_signal_value_identity",
    "pair_support_overlap",
    "primary_evaluator_invocation_count",
    "control_evaluator_invocation_count",
    "primary_standalone_train_reward_decision",
    "primary_standalone_train_reward_blockers",
    "primary_standalone_train_mean_one_way_turnover",
    "train_rank_ic_mean",
    "train_rank_ic_hit_rate",
    "train_rank_ic_loss",
    "train_rank_ic_reward_component",
    "train_rank_ic_obs",
    "train_regime_stability_score",
    "train_regime_reward_component",
    "train_regime_worst_day_sortino",
    "train_regime_median_day_sortino",
    "train_regime_positive_share",
    "train_regime_count",
    "train_regime_method",
    "train_regime_rows",
    "validation_usage",
    "holdout_usage",
    "validation_day_sortino",
    "validation_mcmc_prob_gt_0",
    "validation_rank_ic_mean",
    "validation_rank_ic_loss",
    "holdout_day_sortino",
    "holdout_mcmc_prob_gt_0",
    "holdout_rank_ic_mean",
    "holdout_rank_ic_loss",
]

TRAIN_ONLY_FEEDBACK_FIELDS = [
    field
    for field in CANONICAL_CANDIDATE_FIELDS
    if not field.startswith(("validation_", "holdout_", "challenge_", "sealed_", "forward_", "oos_"))
] + ["feedback_data_role", "evaluation_access_guard"]


def stable_hash(text: str, length: int = 24) -> str:
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()[:length]


def safe_float(value: Any, default: float = float("nan")) -> float:
    try:
        if value in (None, ""):
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def fields_from_expression(expression: str) -> list[str]:
    return sorted(set(FIELD_RE.findall(expression or "")))


def primitive_names(expression: str) -> list[str]:
    return [name.lower() for name in CALL_RE.findall(expression or "")]


def _first_existing(row: dict[str, Any], names: list[str], default: str = "") -> str:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return str(value)
    return default


def infer_generator_arm(row: dict[str, Any]) -> str:
    text = " ".join(
        str(row.get(name) or "")
        for name in ("generator_arm", "source_generator", "source_lane", "factor_lane", "run", "round_id", "source_round")
    ).lower()
    if "cem" in text:
        return "cem_exploit"
    if "ast" in text or "bt" in text:
        return "typed_ast_fresh"
    if "rx" in text or "ucb" in text:
        return "rx_ucb_fresh"
    if "event" in text or "limit" in text or "board" in text:
        return "event_state"
    if "random" in text or "orthogonal" in text:
        return "random_orthogonal"
    if "mcts" in text or "repair" in text:
        return "challenger_repair"
    return "unknown_arm"


def infer_generator_route(row: dict[str, Any]) -> str:
    source = _first_existing(row, ["generator_route", "source_file", "run"], "")
    if "phase3bs" in source.lower():
        return "phase3bs-adaptive-ucb-cem-practice"
    if "phase3bt" in source.lower():
        return "phase3bt-ast-algorithm-bakeoff"
    if "phase3bu" in source.lower():
        return "phase3bu-ast-fresh-winner-variants"
    if "phase3cm" in source.lower():
        return "phase3cm-train-portfolio-sortino-reward-audit"
    return source


def infer_field_family(expression: str, row: dict[str, Any] | None = None) -> str:
    field_text = "|".join(fields_from_expression(expression)).lower()
    lane = str((row or {}).get("factor_lane") or "").lower()
    text = f"{field_text}|{lane}"
    families: list[str] = []
    if "intraday_ret_from_open" in text or "ret_1m" in text:
        families.append("intraday_return")
    if "range_location" in text or "range" in text or "high" in text or "low" in text:
        families.append("range_location")
    if "m1_first" in text or "opening" in text or "auction" in text:
        families.append("opening_state")
    if "amount" in text or "volume" in text or "vol" in text:
        families.append("flow_amount_volume")
    if "limit" in text or "board" in text or "up_limit" in text or "lb_" in text:
        families.append("limit_event_state")
    if "rzrq" in text or "billboard" in text or "holder" in text:
        families.append("lagged_context")
    return "+".join(sorted(set(families))) or "generic_price_volume"


def infer_primitive_family(expression: str) -> str:
    names = primitive_names(expression)
    if not names:
        return "raw_field"
    buckets: list[str] = []
    if any(name in names for name in ("csrank", "rank", "zscore", "csresidual")):
        buckets.append("cross_sectional")
    if any(name in names for name in ("delta", "delay", "mom", "mean", "std", "wma", "med")):
        buckets.append("time_series")
    if any(name in names for name in ("corr", "cov")):
        buckets.append("relation")
    if any(name in names for name in ("mul", "div", "add", "sub")):
        buckets.append("arithmetic")
    if any(name in names for name in ("sign", "abs", "neg")):
        buckets.append("state_transform")
    return "+".join(buckets) or "+".join(sorted(set(names[:4])))


def infer_event_state_family(expression: str, row: dict[str, Any] | None = None) -> str:
    text = (expression + "|" + str((row or {}).get("fields") or "") + "|" + str((row or {}).get("factor_lane") or "")).lower()
    tags: list[str] = []
    if "up_limit" in text or "limit_up" in text:
        tags.append("limit_up")
    if "open_board" in text or "touch" in text or "board" in text:
        tags.append("board_transition")
    if "lb_" in text or "high_board" in text:
        tags.append("high_board")
    if "auction" in text or "m1_first" in text:
        tags.append("opening_state")
    return "+".join(sorted(set(tags))) or "none"


def infer_horizon_bucket(row: dict[str, Any], expression: str = "") -> str:
    horizon = safe_float(_first_existing(row, ["horizon_min", "primary_horizon_min", "best_horizon_min"], ""), float("nan"))
    if math.isfinite(horizon):
        if horizon <= 5:
            return "h1_5"
        if horizon <= 15:
            return "h5_15"
        if horizon <= 30:
            return "h15_30"
        return "h30_plus"
    text = expression.lower()
    if "1,5,15,30" in text:
        return "multi_1_5_15_30"
    return "multi_or_unknown"


def infer_turnover_bucket(row: dict[str, Any]) -> str:
    turnover = safe_float(
        _first_existing(row, ["train_mean_one_way_turnover", "mean_one_way_turnover"], ""),
        float("nan"),
    )
    if not math.isfinite(turnover):
        return "turnover_unknown"
    if turnover < 0.35:
        return "low_turnover"
    if turnover < 0.65:
        return "medium_turnover"
    if turnover < 0.85:
        return "high_turnover"
    return "extreme_turnover"


def normalize_candidate_schema(row: dict[str, Any]) -> dict[str, Any]:
    expression = str(row.get("expression") or "")
    expression_hash = str(row.get("expression_hash") or "").strip() or stable_hash(expression)
    field_family = str(row.get("field_family") or "") or infer_field_family(expression, row)
    primitive_family = str(row.get("primitive_family") or "") or infer_primitive_family(expression)
    event_state_family = str(row.get("event_state_family") or "") or infer_event_state_family(expression, row)
    horizon_bucket = str(row.get("horizon_bucket") or "") or infer_horizon_bucket(row, expression)
    turnover_bucket = str(row.get("turnover_bucket") or "") or infer_turnover_bucket(row)
    major_subtree = stable_hash("|".join(primitive_names(expression)[:6]) + "|" + "|".join(fields_from_expression(expression)), 12)
    family_source = "|".join([field_family, primitive_family, event_state_family, horizon_bucket, turnover_bucket, major_subtree])
    family_id = str(row.get("family_id") or "") or stable_hash(family_source, 18)
    motif_source = "|".join([field_family, primitive_family, event_state_family, major_subtree])
    motif_id = str(row.get("motif_id") or "") or stable_hash(motif_source, 18)
    proxy_quality = _first_existing(row, ["proxy_quality", "phase3ca_proxy_quality", "proxy_sortino", "aligned_ic_mean"], "")
    validation_prob = _first_existing(row, ["validation_mcmc_prob_gt_0", "validation_day_mcmc_prob_gt_0"], "")
    holdout_prob = _first_existing(row, ["holdout_mcmc_prob_gt_0", "holdout_day_mcmc_prob_gt_0"], "")
    blocker_flags = _first_existing(row, ["blocker_flags", "phase3bp_blocker_flags", "inherited_blockers"], "")
    train_reward = _first_existing(row, ["train_reward"], "")
    optimizer_reward = _first_existing(row, ["optimizer_reward"], train_reward)
    return {
        "candidate_id": _first_existing(row, ["candidate_id"], expression_hash[:12]),
        "pair_id": _first_existing(row, ["pair_id"], ""),
        "pair_member_role": _first_existing(row, ["pair_member_role"], ""),
        "primary_candidate_id": _first_existing(row, ["primary_candidate_id"], ""),
        "control_candidate_id": _first_existing(row, ["control_candidate_id"], ""),
        "primary_receipt_hash": _first_existing(row, ["primary_receipt_hash"], ""),
        "control_receipt_hash": _first_existing(row, ["control_receipt_hash"], ""),
        "pair_receipt_hash": _first_existing(row, ["pair_receipt_hash"], ""),
        "control_constructor_id": _first_existing(row, ["control_constructor_id"], ""),
        "pair_evaluation_status": _first_existing(row, ["pair_evaluation_status"], ""),
        "pair_evaluation_blockers": _first_existing(row, ["pair_evaluation_blockers"], ""),
        "pair_train_reward": _first_existing(row, ["pair_train_reward"], ""),
        "pair_train_reward_decision": _first_existing(row, ["pair_train_reward_decision"], ""),
        "pair_train_reward_blockers": _first_existing(row, ["pair_train_reward_blockers"], ""),
        "pair_turnover_metric": _first_existing(row, ["pair_turnover_metric"], ""),
        "pair_support_metric": _first_existing(row, ["pair_support_metric"], ""),
        "pair_rank_ic_metric": _first_existing(row, ["pair_rank_ic_metric"], ""),
        "expression_hash": expression_hash,
        "expression": expression,
        "generator_arm": _first_existing(row, ["generator_arm"], infer_generator_arm(row)),
        "generator_route": _first_existing(row, ["generator_route"], infer_generator_route(row)),
        "seed": _first_existing(row, ["seed"], ""),
        "round_id": _first_existing(row, ["round_id", "source_round"], ""),
        "parent_id": _first_existing(row, ["parent_id"], ""),
        "mutation_type": _first_existing(row, ["mutation_type"], ""),
        "field_family": field_family,
        "primitive_family": primitive_family,
        "event_state_family": event_state_family,
        "horizon_bucket": horizon_bucket,
        "turnover_bucket": turnover_bucket,
        "family_id": family_id,
        "motif_id": motif_id,
        "subtree_hashes": _first_existing(row, ["subtree_hashes"], major_subtree),
        "proxy_quality": proxy_quality,
        "aligned_ic_mean": _first_existing(row, ["aligned_ic_mean", "abs_aligned_ic_mean"], ""),
        "spread_hit_rate": _first_existing(row, ["spread_hit_rate"], ""),
        "mean_one_way_turnover": _first_existing(row, ["mean_one_way_turnover", "train_mean_one_way_turnover"], ""),
        "blocker_flags": blocker_flags,
        "phase3ca_proxy_quality": _first_existing(row, ["phase3ca_proxy_quality"], ""),
        "train_reward": train_reward,
        "train_reward_decision": _first_existing(row, ["train_reward_decision"], ""),
        "train_reward_blockers": _first_existing(row, ["train_reward_blockers"], ""),
        "optimizer_reward": optimizer_reward,
        "optimizer_reward_source": _first_existing(row, ["optimizer_reward_source"], "train_only_phase3cm" if optimizer_reward else ""),
        "optimizer_reward_metric": _first_existing(row, ["optimizer_reward_metric"], OPTIMIZER_REWARD_METRIC if optimizer_reward else ""),
        "optimizer_reward_split": _first_existing(row, ["optimizer_reward_split"], "train" if optimizer_reward else ""),
        "primary_train_reward": _first_existing(row, ["primary_train_reward"], ""),
        "control_train_reward": _first_existing(row, ["control_train_reward"], ""),
        "matched_train_increment": _first_existing(row, ["matched_train_increment"], ""),
        "primary_turnover": _first_existing(row, ["primary_turnover"], ""),
        "control_turnover": _first_existing(row, ["control_turnover"], ""),
        "matched_turnover_increment": _first_existing(row, ["matched_turnover_increment"], ""),
        "primary_rank_ic": _first_existing(row, ["primary_rank_ic"], ""),
        "control_rank_ic": _first_existing(row, ["control_rank_ic"], ""),
        "matched_rank_ic_increment": _first_existing(row, ["matched_rank_ic_increment"], ""),
        "primary_behavior_identity": _first_existing(row, ["primary_behavior_identity"], ""),
        "control_behavior_identity": _first_existing(row, ["control_behavior_identity"], ""),
        "primary_signal_value_identity": _first_existing(row, ["primary_signal_value_identity"], ""),
        "control_signal_value_identity": _first_existing(row, ["control_signal_value_identity"], ""),
        "pair_support_overlap": _first_existing(row, ["pair_support_overlap"], ""),
        "primary_evaluator_invocation_count": _first_existing(row, ["primary_evaluator_invocation_count"], ""),
        "control_evaluator_invocation_count": _first_existing(row, ["control_evaluator_invocation_count"], ""),
        "primary_standalone_train_reward_decision": _first_existing(
            row, ["primary_standalone_train_reward_decision"], ""
        ),
        "primary_standalone_train_reward_blockers": _first_existing(
            row, ["primary_standalone_train_reward_blockers"], ""
        ),
        "primary_standalone_train_mean_one_way_turnover": _first_existing(
            row, ["primary_standalone_train_mean_one_way_turnover"], ""
        ),
        "train_rank_ic_mean": _first_existing(row, ["train_rank_ic_mean"], ""),
        "train_rank_ic_hit_rate": _first_existing(row, ["train_rank_ic_hit_rate"], ""),
        "train_rank_ic_loss": _first_existing(row, ["train_rank_ic_loss"], ""),
        "train_rank_ic_reward_component": _first_existing(row, ["train_rank_ic_reward_component"], ""),
        "train_rank_ic_obs": _first_existing(row, ["train_rank_ic_obs"], ""),
        "train_regime_stability_score": _first_existing(row, ["train_regime_stability_score"], ""),
        "train_regime_reward_component": _first_existing(row, ["train_regime_reward_component"], ""),
        "train_regime_worst_day_sortino": _first_existing(row, ["train_regime_worst_day_sortino"], ""),
        "train_regime_median_day_sortino": _first_existing(row, ["train_regime_median_day_sortino"], ""),
        "train_regime_positive_share": _first_existing(row, ["train_regime_positive_share"], ""),
        "train_regime_count": _first_existing(row, ["train_regime_count"], ""),
        "train_regime_method": _first_existing(row, ["train_regime_method"], ""),
        "train_regime_rows": _first_existing(row, ["train_regime_rows"], ""),
        "validation_usage": _first_existing(row, ["validation_usage"], "report_only"),
        "holdout_usage": _first_existing(row, ["holdout_usage"], "report_only"),
        "validation_day_sortino": _first_existing(row, ["validation_day_sortino"], ""),
        "validation_mcmc_prob_gt_0": validation_prob,
        "validation_rank_ic_mean": _first_existing(row, ["validation_rank_ic_mean"], ""),
        "validation_rank_ic_loss": _first_existing(row, ["validation_rank_ic_loss"], ""),
        "holdout_day_sortino": _first_existing(row, ["holdout_day_sortino"], ""),
        "holdout_mcmc_prob_gt_0": holdout_prob,
        "holdout_rank_ic_mean": _first_existing(row, ["holdout_rank_ic_mean"], ""),
        "holdout_rank_ic_loss": _first_existing(row, ["holdout_rank_ic_loss"], ""),
    }
