"""Build Phase3DV budget-pool self-deepen candidates.

Phase3DV changes the DS/DT/DU posture from "find perfect followups" to
"allocate bounded budgets to viable clusters".  Validation and holdout columns
are copied for audit only; seed scoring and optimizer feedback use train-side
evidence only.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any

from our_system_phase2.services.candidate_schema import CANONICAL_CANDIDATE_FIELDS, normalize_candidate_schema
from our_system_phase2.services.typed_primitive_gate import REGISTRY_VERSION, validate_expression


DEFAULT_SOURCE_REWARD = Path("reports/phase3dt_survivor_expansion_cm_reward_20260630/phase3cm_train_reward.csv")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3dv_budget_pool_self_deepen_pack_20260630")
DEFAULT_REPORT_ROOT = Path("reports/phase3dv_budget_pool_self_deepen_pack_20260630")


EVENT_FIELDS = [
    "evt_uplimit_fd_max",
    "evt_uplimit_fd_close",
    "evt_uplimit_up_limit_keep_times",
    "evt_uplimit_amount",
    "evt_uplimit_auction_buy",
    "evt_uplimit_auction_money",
    "evt_uplimit_auction_offer",
    "evt_uplimit_auction_pre1max_ratio",
    "evt_uplimit_auction_turnover",
    "evt_uplimit_active",
    "evt_uplimit_age_min",
]

VALUE_FIELDS = [
    "ctx_hfq_pb",
    "ctx_hfq_pe_ttm",
    "ctx_hfq_ps_ttm",
    "ctx_hfq_float_market_cap_yuan",
    "ctx_hfq_market_cap_yuan",
]

SENTIMENT_FIELDS = [
    "ctx_sent_uplimit_num",
    "ctx_sent_downlimit_num",
    "ctx_sent_lb_2_num",
    "ctx_sent_lb_3_num",
    "ctx_sent_max_lb_num",
    "ctx_sent_gt5_num",
    "ctx_sent_zb_num",
]

LIQUIDITY_FIELDS = [
    "ctx_hfq_volume_ratio",
    "ctx_hfq_turnover_ratio",
    "ctx_rzrq_rzyezb",
    "ctx_rzrq_rzye",
    "ctx_rzrq_rqye",
    "ctx_rzrq_rzrqye",
    "ctx_rzrq_rzmre",
    "ctx_rzrq_rzche",
    "ctx_rzrq_rzjme",
    "ctx_rzrq_rqyl",
    "ctx_billboard_billboard_net_amt",
    "ctx_billboard_billboard_deal_amt",
    "ctx_billboard_billboard_buy_amt",
    "ctx_billboard_billboard_sell_amt",
    "ctx_billboard_deal_amount_ratio",
    "ctx_billboard_deal_net_ratio",
    "ctx_billboard_accum_amount",
    "ctx_billboard_turnoverrate",
    "ctx_billboard_buy_ratio",
    "ctx_billboard_sell_ratio",
    "ctx_billboard_sum_buy_amt",
    "ctx_billboard_sum_sell_amt",
    "ctx_billboard_net_bs_amt",
]

UNDERUSED_CONTEXT_FIELDS = [
    "ctx_holder_avg_market_cap",
    "ctx_holder_holder_num",
    "ctx_holder_holder_num_change",
    "ctx_holder_holder_num_ratio",
    "ctx_holder_avg_hold_num",
]

INTRADAY_FIELDS = [
    "amount",
    "vol",
    "vwap",
    "open",
    "high",
    "low",
    "close",
    "m1_first5_amount",
    "m1_first5_vol",
    "m1_first5_high",
    "m1_first5_low",
    "m1_first15_amount",
    "m1_first15_vol",
    "m1_first15_high",
    "m1_first15_low",
    "m1_first30_amount",
    "m1_first30_vol",
    "m1_first30_high",
    "m1_first30_low",
    "m1_first5_last_close",
    "m1_first15_last_close",
    "m1_first30_last_close",
    "m1_first5_vwap",
    "m1_first15_vwap",
    "m1_first30_vwap",
    "m1_first5_vwap_return_vs_open",
    "m1_first15_vwap_return_vs_open",
    "m1_first30_vwap_return_vs_open",
    "m1_first5_last_return_vs_open",
    "m1_first15_last_return_vs_open",
    "m1_first30_last_return_vs_open",
    "m1_first5_range",
    "m1_first15_range",
    "m1_first30_range",
]

EVENT_WINDOWS = [3, 5, 10, 15, 20, 40]
CONTEXT_WINDOWS = [10, 20, 40, 60, 120]
INTRADAY_WINDOWS = [3, 5, 10, 20, 30, 40, 60]
VALID_RATIOS = [0.5, 0.6, 0.8]

HARD_BLOCKER_TOKENS = (
    "wrong_lag",
    "future_signal",
    "blocked_or_future",
    "candidate_uses_blocked_or_future_fields",
    "missing_schema",
    "unsafe_known_structure",
    "label",
)


def _valid_ratios_for_context(field: str) -> list[float]:
    name = str(field or "").lower()
    if any(token in name for token in ("billboard", "holder", "dividend", "share_change", "shareholder", "zls")):
        return [0.02, 0.05, 0.10]
    if "rzrq" in name:
        return [0.40, 0.60]
    return VALID_RATIOS


def _allow_masked_relation(field: str) -> bool:
    name = str(field or "").lower()
    return not any(token in name for token in ("billboard", "holder", "dividend", "share_change", "shareholder", "zls"))


def _hash(text: str, length: int = 24) -> str:
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()[:length]


def _f(value: Any, default: float = float("nan")) -> float:
    try:
        if value in (None, ""):
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _round(value: Any, ndigits: int = 8) -> float | str:
    val = _f(value)
    if not math.isfinite(val):
        return ""
    return round(val, ndigits)


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(CANONICAL_CANDIDATE_FIELDS)
    for row in rows:
        for name in row:
            if name not in fieldnames:
                fieldnames.append(name)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _available_fields_from_shard_root(shard_root: Path | None) -> set[str] | None:
    if shard_root is None:
        return None
    root = shard_root.resolve()
    if not root.exists():
        return None
    try:
        import pyarrow.parquet as pq
    except Exception:
        return None
    for path in root.rglob("*.parquet"):
        try:
            return set(pq.read_schema(path).names)
        except Exception:
            continue
    return None


def _filter_available(fields: list[str], available: set[str] | None) -> list[str]:
    if available is None:
        return list(fields)
    return [field for field in fields if field in available]


def _has_hard_blocker(row: dict[str, Any]) -> bool:
    text = "|".join(
        str(row.get(name) or "")
        for name in (
            "train_reward_blockers",
            "blocker_flags",
            "phase3bp_blocker_flags",
            "phase3ca_blocker_flags",
            "inherited_blockers",
        )
    ).lower()
    return any(token in text for token in HARD_BLOCKER_TOKENS)


def _family_key(expression: str) -> str:
    text = expression.lower()
    if "fd_max" in text:
        event = "fd_max"
    elif "fd_close" in text:
        event = "fd_close"
    elif "up_limit_keep" in text:
        event = "up_limit_keep"
    elif "uplimit_amount" in text or "evt_uplimit_amount" in text:
        event = "uplimit_amount"
    else:
        event = "other_event"

    if "ctx_hfq_pb" in text:
        ctx = "pb"
    elif "market_cap" in text:
        ctx = "market_cap"
    elif "sent_" in text:
        ctx = "sentiment"
    elif "rzrq" in text or "billboard" in text:
        ctx = "flow_context"
    elif "volume_ratio" in text or "turnover" in text or "amount" in text or "vol" in text:
        ctx = "liquidity"
    elif "m1_first" in text:
        ctx = "opening"
    else:
        ctx = "other_ctx"

    if any(token in text for token in ("eventcount", "statedwell", "windowstatecount")):
        state = "event_state"
    elif any(token in text for token in ("maskedcorr", "safecsresidual")):
        state = "relation"
    elif any(token in text for token in ("validratiogate", "maskedzscore")):
        state = "coverage"
    else:
        state = "age"
    return f"{event}|{ctx}|{state}"


def _train_signal_status(row: dict[str, Any]) -> tuple[str, float, str]:
    if _has_hard_blocker(row):
        return "reject_no_budget", -999.0, "hard_input_or_typed_blocker"

    train_reward = _f(row.get("train_reward"), -999.0)
    train_day = _f(row.get("train_day_sortino"), train_reward)
    train_worst = _f(row.get("train_worst_horizon_day_sortino"), train_day)
    train_median = _f(row.get("train_median_horizon_day_sortino"), train_day)
    train_mcmc = _f(row.get("train_day_mcmc_prob_gt_0"), _f(row.get("train_mcmc_prob_gt_0"), 0.50))
    rank_ic = _f(row.get("train_rank_ic_mean"), 0.0)
    rank_ic_hit = _f(row.get("train_rank_ic_hit_rate"), 0.0)
    regime_score = _f(row.get("train_regime_stability_score"), 0.0)
    regime_positive_share = _f(row.get("train_regime_positive_share"), 0.0)
    turnover = _f(row.get("train_mean_one_way_turnover"), _f(row.get("mean_one_way_turnover"), 0.0))
    blockers = str(row.get("train_reward_blockers") or "")

    base = (
        0.45 * max(train_reward, -0.50)
        + 0.22 * max(train_day, -0.50)
        + 0.16 * max(train_median, -0.50)
        + 0.08 * min(1.0, max(0.0, train_mcmc))
        + 1.80 * max(-0.02, min(0.04, rank_ic))
        + 0.08 * max(0.0, regime_score)
        + 0.04 * max(0.0, regime_positive_share)
    )
    if train_worst < 0:
        base -= min(0.12, abs(train_worst) * 0.05)
    if turnover > 0.75:
        base -= 0.30

    strong_train = train_reward > 0 and train_day > 0 and train_median > 0 and train_mcmc >= 0.55
    regime_viable = (
        train_reward > -0.08
        and (
            regime_score > 0.02
            or regime_positive_share >= 0.50
            or train_median > 0.05
            or (rank_ic > 0.006 and rank_ic_hit >= 0.52)
            or ("non_positive_worst_horizon_train_sortino" in blockers and train_day > 0)
        )
    )
    weak_viable = train_reward > -0.20 and (
        train_day > 0 or train_median > 0 or rank_ic > 0.003 or regime_positive_share >= 0.40
    )

    if strong_train:
        return "global_survivor", base, "clean_train_reward_median_horizon_mcmc"
    if regime_viable:
        return "regime_specialist", base, "train_regime_or_median_horizon_viable"
    if weak_viable:
        return "probation", base, "weak_train_signal_budget_probe"
    return "reject_no_budget", base, "insufficient_train_evidence"


def _cluster_rows(source_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    seed_rows: list[dict[str, Any]] = []
    clusters: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in source_rows:
        row = dict(raw)
        expression = str(row.get("expression") or "")
        status, score, reason = _train_signal_status(row)
        family = _family_key(expression)
        row.update(
            {
                "budget_status": status,
                "budget_score": _round(score),
                "budget_reason": reason,
                "family_key": family,
                "expression": expression,
            }
        )
        clusters[family].append(row)

    cluster_rows: list[dict[str, Any]] = []
    for family, items in clusters.items():
        ranked = sorted(items, key=lambda item: _f(item.get("budget_score"), -999.0), reverse=True)
        status_counts = Counter(str(item.get("budget_status") or "unknown") for item in items)
        train_rewards = [_f(item.get("train_reward")) for item in items if math.isfinite(_f(item.get("train_reward")))]
        rank_ics = [_f(item.get("train_rank_ic_mean")) for item in items if math.isfinite(_f(item.get("train_rank_ic_mean")))]
        top = ranked[0]
        top_status = str(top.get("budget_status") or "reject_no_budget")
        cluster_rows.append(
            {
                "family_key": family,
                "cluster_size": len(items),
                "top_budget_status": top_status,
                "top_budget_score": top.get("budget_score", ""),
                "status_counts": json.dumps(dict(status_counts), sort_keys=True),
                "median_train_reward": _round(_median(train_rewards)),
                "best_train_reward": _round(max(train_rewards) if train_rewards else float("nan")),
                "median_train_rank_ic": _round(_median(rank_ics)),
                "top_candidate_id": top.get("candidate_id", ""),
                "top_expression_hash": top.get("expression_hash", ""),
                "top_expression": top.get("expression", ""),
                "validation_usage": "audit_only_not_budgeted",
                "holdout_usage": "audit_only_not_budgeted",
            }
        )
    seed_rows = [item for values in clusters.values() for item in values]
    return seed_rows, cluster_rows


def _median(values: list[float]) -> float:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return float("nan")
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return 0.5 * (clean[mid - 1] + clean[mid])


def _status_priority(status: str) -> int:
    return {"global_survivor": 4, "regime_specialist": 3, "probation": 2, "reject_no_budget": 1}.get(status, 0)


def _allocate_seed_budgets(
    seed_rows: list[dict[str, Any]],
    *,
    max_seed_rows: int,
    per_family_seed_cap: int,
) -> list[dict[str, Any]]:
    by_status: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in seed_rows:
        by_status[str(row.get("budget_status") or "reject_no_budget")].append(row)
    for rows in by_status.values():
        rows.sort(key=lambda item: _f(item.get("budget_score"), -999.0), reverse=True)

    quotas = {
        "global_survivor": int(round(max_seed_rows * 0.24)),
        "regime_specialist": int(round(max_seed_rows * 0.38)),
        "probation": int(round(max_seed_rows * 0.28)),
    }
    quotas["global_survivor"] = max(8, quotas["global_survivor"])
    quotas["regime_specialist"] = max(16, quotas["regime_specialist"])
    quotas["probation"] = max(12, quotas["probation"])

    selected: list[dict[str, Any]] = []
    family_counts: Counter[str] = Counter()
    for status in ("global_survivor", "regime_specialist", "probation"):
        cap = quotas[status]
        used = 0
        for row in by_status.get(status, []):
            if used >= cap or len(selected) >= max_seed_rows:
                break
            family = str(row.get("family_key") or "unknown")
            family_bucket = f"{status}|{family}"
            if family_counts[family_bucket] >= per_family_seed_cap:
                continue
            item = dict(row)
            item["seed_quota_status"] = status
            selected.append(item)
            family_counts[family_bucket] += 1
            used += 1

    if len(selected) < max_seed_rows:
        already = {str(row.get("expression_hash") or _hash(row.get("expression", ""))) for row in selected}
        leftovers = [
            row
            for row in seed_rows
            if str(row.get("budget_status") or "") != "reject_no_budget"
            and str(row.get("expression_hash") or _hash(row.get("expression", ""))) not in already
        ]
        leftovers.sort(
            key=lambda item: (_status_priority(str(item.get("budget_status") or "")), _f(item.get("budget_score"), -999.0)),
            reverse=True,
        )
        for row in leftovers:
            if len(selected) >= max_seed_rows:
                break
            family = str(row.get("family_key") or "unknown")
            status = str(row.get("budget_status") or "")
            family_bucket = f"{status}|{family}"
            if family_counts[family_bucket] >= per_family_seed_cap:
                continue
            item = dict(row)
            item["seed_quota_status"] = status
            selected.append(item)
            family_counts[family_bucket] += 1

    return selected[:max_seed_rows]


def _ranked_event_atoms(available: set[str] | None = None) -> list[tuple[str, str, str]]:
    atoms: list[tuple[str, str, str]] = []
    unusable_event_fields = {"evt_uplimit_type_code"}
    dense_event_age_fields = {"evt_uplimit_active"}
    for field in _filter_available(EVENT_FIELDS, available):
        if field in unusable_event_fields:
            continue
        if not field.startswith("evt_") or field in dense_event_age_fields:
            for op in ("EventAge", "SinceLastEvent"):
                expr = f"CSRank({op}(${field}))"
                atoms.extend(
                    [
                        ("event_age", expr, field),
                        ("event_age_neg", f"Neg({expr})", field),
                        ("event_age_sign", f"Sign(Sub({expr},0.5))", field),
                    ]
                )
        state_ops = ("EventCount", "WindowStateCount") if field.startswith("evt_") else ("EventCount", "StateDwell", "WindowStateCount")
        for op in state_ops:
            for window in EVENT_WINDOWS:
                expr = f"CSRank({op}(${field},{window}))"
                atoms.extend(
                    [
                        ("event_state", expr, field),
                        ("event_state_neg", f"Neg({expr})", field),
                        ("event_state_sign", f"Sign(Sub({expr},0.5))", field),
                    ]
                )
    return atoms


def _ranked_context_atoms(available: set[str] | None = None) -> dict[str, list[tuple[str, str, str]]]:
    groups: dict[str, list[tuple[str, str, str]]] = defaultdict(list)

    def add_coverage(field: str, group: str, windows: list[int]) -> None:
        for window in windows:
            for ratio in _valid_ratios_for_context(field):
                z = f"CSRank(MaskedZScore(${field},{window},{ratio}))"
                gate = f"CSRank(ValidRatioGate(${field},{window},{ratio}))"
                groups[group].extend(
                    [
                        (f"{group}_masked_z", z, field),
                        (f"{group}_masked_z_neg", f"Neg({z})", field),
                        (f"{group}_valid_ratio", gate, field),
                    ]
                )
                for control in ("amount", "vol", "vwap"):
                    if not _allow_masked_relation(field):
                        continue
                    corr = f"CSRank(MaskedCorr(${field},${control},{window},{ratio}))"
                    groups[f"{group}_relation"].extend(
                        [
                            (f"{group}_masked_corr", corr, field),
                            (f"{group}_masked_corr_neg", f"Neg({corr})", field),
                        ]
                    )

    for field in _filter_available(VALUE_FIELDS, available):
        add_coverage(field, "value", CONTEXT_WINDOWS)
    for field in _filter_available(SENTIMENT_FIELDS, available):
        add_coverage(field, "sentiment", CONTEXT_WINDOWS)
    for field in _filter_available(LIQUIDITY_FIELDS, available):
        add_coverage(field, "liquidity", CONTEXT_WINDOWS)
    for field in _filter_available(UNDERUSED_CONTEXT_FIELDS, available):
        add_coverage(field, "underused_context", CONTEXT_WINDOWS)
    for field in _filter_available(INTRADAY_FIELDS, available):
        # Keep event-combined intraday atoms raw/derived for now. The current
        # construction-time typed gate is intentionally conservative and will
        # reject event branches combined with ordinary TS primitives such as
        # Delta/Mean/Std even when they are on a separate continuous branch.
        expr = f"CSRank(${field})"
        groups["intraday"].extend(
            [
                ("intraday_raw_rank", expr, field),
                ("intraday_raw_rank_neg", f"Neg({expr})", field),
                ("intraday_raw_rank_sign", f"Sign(Sub({expr},0.5))", field),
            ]
        )
    return groups


def _forms(event: str, a: str, b: str | None, c: str | None) -> list[tuple[str, str]]:
    out = [
        ("add_event_context", f"CSRank(Add({event},{a}))"),
        ("sub_context_event", f"CSRank(Sub({a},{event}))"),
        ("sub_event_context", f"CSRank(Sub({event},{a}))"),
        ("mul_event_context", f"CSRank(Mul({event},{a}))"),
        ("neg_mul_event_context", f"Neg(CSRank(Mul({event},{a})))"),
    ]
    if b is not None:
        out.extend(
            [
                ("add_event_two_context", f"CSRank(Add(Add({event},{a}),{b}))"),
                ("spread_context_event", f"CSRank(Add(Sub({a},{b}),{event}))"),
                ("mul_event_context_spread", f"CSRank(Mul({event},CSRank(Sub({a},{b}))))"),
            ]
        )
    if b is not None and c is not None:
        out.extend(
            [
                ("three_context_interaction", f"CSRank(Add(Mul({event},CSRank(Add({a},{b}))),{c}))"),
                ("three_context_spread", f"CSRank(Add(Mul({event},CSRank(Sub({a},{b}))),{c}))"),
                ("three_context_contrarian", f"Neg(CSRank(Add(Mul({event},CSRank(Sub({a},{b}))),{c})))"),
            ]
        )
    return out


def _choose_context_groups(status: str, lane: str) -> list[str]:
    if status == "global_survivor":
        return ["value", "value_relation", "liquidity", "sentiment", "intraday"]
    if status == "regime_specialist":
        return [
            "value",
            "sentiment",
            "sentiment_relation",
            "liquidity",
            "liquidity_relation",
            "intraday",
            "underused_context",
        ]
    if status == "probation":
        return ["value", "sentiment", "liquidity", "intraday", "underused_context", "underused_context_relation"]
    if "fresh" in lane:
        return [
            "value",
            "value_relation",
            "sentiment",
            "sentiment_relation",
            "liquidity",
            "liquidity_relation",
            "intraday",
            "underused_context",
            "underused_context_relation",
        ]
    return ["value", "sentiment", "liquidity", "intraday"]


def _fields_in_text(text: str, universe: list[str]) -> list[str]:
    lower = (text or "").lower()
    return [field for field in universe if f"${field.lower()}" in lower or field.lower() in lower]


def _core_event_fields() -> set[str]:
    return {"evt_uplimit_fd_max", "evt_uplimit_fd_close", "evt_uplimit_up_limit_keep_times", "evt_uplimit_active"}


def _add_candidate(
    rows: list[dict[str, Any]],
    seen: set[str],
    reject_counter: Counter[str],
    *,
    expression: str,
    lane: str,
    seed_status: str,
    parent_id: str,
    mutation_type: str,
    budget_score: Any = "",
) -> bool:
    digest = _hash(expression)
    if digest in seen:
        reject_counter["duplicate_expression"] += 1
        return False
    verdict = validate_expression(
        expression,
        entry_lineage="phase3dv_budget_pool_self_deepen",
        materialization_stage="candidate_materialization",
        candidate_role=lane,
    )
    if verdict.typed_gate_decision != "allow":
        reject_counter[f"typed_gate_{verdict.typed_gate_decision}"] += 1
        return False
    seen.add(digest)
    idx = len(rows) + 1
    row: dict[str, Any] = {
        "candidate_id": f"phase3dv_{idx:05d}",
        "expression_hash": digest,
        "expression": expression,
        "generator_arm": "phase3dv_budget_pool_self_deepen",
        "generator_route": "phase3dv-budget-pool-self-deepen-pack",
        "seed": "20260630",
        "round_id": lane,
        "parent_id": parent_id,
        "mutation_type": mutation_type,
        "mean_one_way_turnover": "0.14",
        "phase3ca_proxy_quality": "0",
        "proxy_quality": "0",
        "validation_usage": "audit_only_not_budgeted",
        "holdout_usage": "audit_only_not_budgeted",
        "phase3dv_lane": lane,
        "phase3dv_seed_status": seed_status,
        "phase3dv_seed_budget_score": budget_score,
        "typed_gate_decision": verdict.typed_gate_decision,
        "typed_gate_reason": verdict.typed_gate_reason,
        "typed_gate_registry_version": REGISTRY_VERSION,
        "metric_boundary": "budget-pool pack only; Phase3CM train reward is optimizer feedback; candidate-level non-development fields forbidden",
    }
    row.update(normalize_candidate_schema(row))
    rows.append(row)
    return True


def _build_from_seed(
    rows: list[dict[str, Any]],
    seen: set[str],
    reject_counter: Counter[str],
    *,
    seed: dict[str, Any],
    lane: str,
    target_count: int,
    event_atoms: list[tuple[str, str, str]],
    context_atoms: dict[str, list[tuple[str, str, str]]],
) -> None:
    status = str(seed.get("budget_status") or "probation")
    seed_expr = str(seed.get("expression") or "")
    rng = random.Random(_hash(str(seed.get("expression_hash") or seed.get("expression") or lane), 16))
    context_groups = _choose_context_groups(status, lane)
    event_pool = list(event_atoms)
    if status == "global_survivor":
        allowed = _core_event_fields() | set(_fields_in_text(seed_expr, EVENT_FIELDS))
        event_pool = [item for item in event_pool if item[2] in allowed and item[0] in {"event_age", "event_age_neg", "event_state"}] or event_pool
    elif status in {"regime_specialist", "probation"}:
        preferred = _core_event_fields() | set(_fields_in_text(seed_expr, EVENT_FIELDS))
        preferred_pool = [item for item in event_pool if item[2] in preferred]
        other_pool = [item for item in event_pool if item[2] not in preferred]
        rng.shuffle(preferred_pool)
        rng.shuffle(other_pool)
        event_pool = preferred_pool + other_pool
    else:
        rng.shuffle(event_pool)
    context_pool: list[tuple[str, str, str]] = []
    for group in context_groups:
        context_pool.extend(context_atoms.get(group, []))
    preferred_context_fields = set(
        _fields_in_text(
            seed_expr,
            VALUE_FIELDS + SENTIMENT_FIELDS + LIQUIDITY_FIELDS + UNDERUSED_CONTEXT_FIELDS + INTRADAY_FIELDS,
        )
    )
    if preferred_context_fields:
        preferred_context = [item for item in context_pool if item[2] in preferred_context_fields]
        other_context = [item for item in context_pool if item[2] not in preferred_context_fields]
        rng.shuffle(preferred_context)
        rng.shuffle(other_context)
        context_pool = preferred_context + other_context
    else:
        rng.shuffle(context_pool)
    secondary_pool = list(context_atoms.get("sentiment", [])) + list(context_atoms.get("liquidity", [])) + list(context_atoms.get("intraday", []))
    tertiary_pool = list(context_atoms.get("intraday", [])) + list(context_atoms.get("value", [])) + list(context_atoms.get("underused_context", []))
    rng.shuffle(secondary_pool)
    rng.shuffle(tertiary_pool)

    attempts = 0
    max_attempts = max(400, target_count * 80)
    while sum(1 for row in rows if row.get("parent_id") == str(seed.get("candidate_id") or "")) < target_count and attempts < max_attempts:
        attempts += 1
        if not event_pool or not context_pool:
            break
        event = event_pool[attempts % len(event_pool)][1]
        a = context_pool[(attempts * 7 + rng.randrange(len(context_pool))) % len(context_pool)][1]
        b = secondary_pool[(attempts * 11 + rng.randrange(len(secondary_pool))) % len(secondary_pool)][1] if secondary_pool else None
        c = tertiary_pool[(attempts * 13 + rng.randrange(len(tertiary_pool))) % len(tertiary_pool)][1] if tertiary_pool else None
        form_bank = _forms(event, a, b, c)
        mutation_type, expression = form_bank[(attempts + rng.randrange(len(form_bank))) % len(form_bank)]
        _add_candidate(
            rows,
            seen,
            reject_counter,
            expression=expression,
            lane=lane,
            seed_status=status,
            parent_id=str(seed.get("candidate_id") or status),
            mutation_type=mutation_type,
            budget_score=seed.get("budget_score", ""),
        )


def _build_candidate_rows(
    selected_seeds: list[dict[str, Any]],
    *,
    max_candidates: int,
    fresh_share: float,
    per_seed_base_budget: int,
    available_fields: set[str] | None,
) -> tuple[list[dict[str, Any]], Counter[str]]:
    event_atoms = _ranked_event_atoms(available_fields)
    context_atoms = _ranked_context_atoms(available_fields)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    reject_counter: Counter[str] = Counter()

    if selected_seeds:
        non_fresh_budget = max(0, int(round(max_candidates * (1.0 - fresh_share))))
        fresh_budget = max_candidates - non_fresh_budget
    else:
        non_fresh_budget = 0
        fresh_budget = max_candidates
    seeds_by_status: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for seed in selected_seeds:
        seeds_by_status[str(seed.get("budget_status") or "probation")].append(seed)

    lane_specs = [
        ("phase3dv_lane_a_elite_budget_deepen", "global_survivor", 0.24),
        ("phase3dv_lane_b_regime_specialist_budget", "regime_specialist", 0.42),
        ("phase3dv_lane_c_probation_budget_probe", "probation", 0.24),
    ]
    for lane, status, share in lane_specs:
        seeds = seeds_by_status.get(status, [])
        if not seeds:
            continue
        lane_budget = max(len(seeds), int(round(non_fresh_budget * share)))
        per_seed = max(1, min(max(1, per_seed_base_budget), int(math.ceil(lane_budget / max(1, len(seeds))))))
        for seed in seeds:
            seed_expr = str(seed.get("expression") or "").strip()
            if seed_expr:
                _add_candidate(
                    rows,
                    seen,
                    reject_counter,
                    expression=seed_expr,
                    lane=lane,
                    seed_status=status,
                    parent_id=str(seed.get("candidate_id") or status),
                    mutation_type="seed_identity_control",
                    budget_score=seed.get("budget_score", ""),
                )
            _build_from_seed(
                rows,
                seen,
                reject_counter,
                seed=seed,
                lane=lane,
                target_count=per_seed,
                event_atoms=event_atoms,
                context_atoms=context_atoms,
            )

    fresh_seed_count = max(8, min(96, int(math.ceil(fresh_budget / max(1, per_seed_base_budget)))))
    fresh_seeds = [
        {
            "candidate_id": f"fresh_self_{idx:03d}",
            "budget_status": "fresh_self_deepen",
            "budget_score": 0.0,
            "expression_hash": _hash(f"fresh_self_{idx}"),
        }
        for idx in range(fresh_seed_count)
    ]
    for seed in fresh_seeds:
        _build_from_seed(
            rows,
            seen,
            reject_counter,
            seed=seed,
            lane="phase3dv_lane_d_fresh_self_deepen",
            target_count=max(1, fresh_budget // fresh_seed_count),
            event_atoms=event_atoms,
            context_atoms=context_atoms,
        )

    fill_round = 0
    while len(rows) < max_candidates and fill_round < 6:
        fill_round += 1
        missing = max_candidates - len(rows)
        refill_seed_count = max(4, min(96, int(math.ceil(missing / max(1, per_seed_base_budget)))))
        for idx in range(refill_seed_count):
            if len(rows) >= max_candidates:
                break
            seed = {
                "candidate_id": f"fresh_refill_{fill_round:02d}_{idx:03d}",
                "budget_status": "fresh_self_deepen",
                "budget_score": 0.0,
                "expression_hash": _hash(f"fresh_refill_{fill_round}_{idx}_{len(rows)}"),
            }
            _build_from_seed(
                rows,
                seen,
                reject_counter,
                seed=seed,
                lane="phase3dv_lane_d_fresh_self_deepen",
                target_count=max(1, int(math.ceil(missing / refill_seed_count))),
                event_atoms=event_atoms,
                context_atoms=context_atoms,
            )
        if missing == max_candidates - len(rows):
            break

    return _balanced_limit(rows, max_candidates), reject_counter


def _balanced_limit(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0 or len(rows) <= limit:
        for idx, row in enumerate(rows, 1):
            row["candidate_id"] = f"phase3dv_{idx:05d}"
            row.update(normalize_candidate_schema(row))
        return rows
    queues: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    for row in rows:
        queues[str(row.get("phase3dv_lane") or "unknown")].append(row)
    selected: list[dict[str, Any]] = []
    lane_order = sorted(queues)
    while len(selected) < limit and any(queues.values()):
        for lane in lane_order:
            if queues[lane]:
                selected.append(queues[lane].popleft())
                if len(selected) >= limit:
                    break
    for idx, row in enumerate(selected, 1):
        row["candidate_id"] = f"phase3dv_{idx:05d}"
        row.update(normalize_candidate_schema(row))
    return selected


def _render_md(summary: dict[str, Any]) -> str:
    lines = [
        "# Phase3DV Budget Pool Self-Deepen Pack",
        "",
        f"candidate_count: `{summary['candidate_count']}`",
        f"seed_count: `{summary['seed_count']}`",
        f"cluster_count: `{summary['cluster_count']}`",
        f"seed_status_counts: `{summary['seed_status_counts']}`",
        f"lane_counts: `{summary['lane_counts']}`",
        f"reject_counts: `{summary['reject_counts']}`",
        "",
        "## Boundary",
        "",
        "- Seed budget is allocated by train-side evidence, not validation or holdout.",
        "- `non_positive_worst_horizon_train_sortino` is a soft budget-risk flag, not a hard discard.",
        "- Global survivor, regime specialist, probation, and fresh self-deepen lanes all retain bounded budgets.",
        "- Wrong-lag/future/unsafe typed structures remain hard blocked by construction-time typed gate.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-reward", type=Path, default=DEFAULT_SOURCE_REWARD)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-seed-rows", type=int, default=256)
    parser.add_argument("--max-candidates", type=int, default=12288)
    parser.add_argument("--per-family-seed-cap", type=int, default=16)
    parser.add_argument("--per-seed-base-budget", type=int, default=48)
    parser.add_argument("--fresh-share", type=float, default=0.18)
    parser.add_argument("--shard-root", type=Path, default=None)
    args = parser.parse_args(argv)

    source_path = args.source_reward.resolve()
    source_rows = _read_csv(source_path)
    available_fields = _available_fields_from_shard_root(args.shard_root)
    seed_rows, cluster_rows = _cluster_rows(source_rows)
    selected_seeds = _allocate_seed_budgets(
        seed_rows,
        max_seed_rows=max(1, int(args.max_seed_rows)),
        per_family_seed_cap=max(1, int(args.per_family_seed_cap)),
    )
    rows, reject_counter = _build_candidate_rows(
        selected_seeds,
        max_candidates=max(1, int(args.max_candidates)),
        fresh_share=max(0.0, min(0.75, float(args.fresh_share))),
        per_seed_base_budget=max(1, int(args.per_seed_base_budget)),
        available_fields=available_fields,
    )

    seed_status_counts = Counter(str(row.get("budget_status") or "unknown") for row in selected_seeds)
    lane_counts = Counter(str(row.get("phase3dv_lane") or "unknown") for row in rows)
    cluster_status_counts = Counter(str(row.get("top_budget_status") or "unknown") for row in cluster_rows)

    output_root = args.output_root.resolve()
    report_root = args.report_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    summary = {
        "phase": "Phase3DV",
        "source_reward": str(source_path),
        "source_rows": len(source_rows),
        "seed_count": len(selected_seeds),
        "cluster_count": len(cluster_rows),
        "candidate_count": len(rows),
        "seed_status_counts": dict(seed_status_counts),
        "cluster_status_counts": dict(cluster_status_counts),
        "lane_counts": dict(lane_counts),
        "reject_counts": dict(reject_counter),
        "decision": "PACK_READY_FOR_PHASE3CM_TRAIN_REWARD_AUDIT",
        "optimizer_metric": "train_portfolio_sortino_rankic_regime_composite_reward",
        "budget_boundary": "train-only budget score; validation/holdout audit-only",
        "typed_gate_registry_version": REGISTRY_VERSION,
        "available_field_filter_enabled": available_fields is not None,
        "available_field_count": len(available_fields or []),
        "configured_missing_fields": sorted(
            set(EVENT_FIELDS + VALUE_FIELDS + SENTIMENT_FIELDS + LIQUIDITY_FIELDS + UNDERUSED_CONTEXT_FIELDS + INTRADAY_FIELDS)
            - set(available_fields or [])
        )
        if available_fields is not None
        else [],
    }

    for root in (output_root, report_root):
        _write_csv(root / "phase3dv_seed_budget_table.csv", selected_seeds)
        _write_csv(root / "phase3dv_cluster_budget_table.csv", cluster_rows)
        _write_csv(root / "phase3dv_budget_pool_candidate_audit.csv", rows)
        _write_json(root / "phase3dv_budget_pool_pack_summary.json", summary)
        (root / "PHASE3DV_BUDGET_POOL_SELF_DEEPEN_PACK_20260630.md").write_text(
            _render_md(summary), encoding="utf-8"
        )

    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
