"""Build Phase3DU adaptive regime-budget free-deepen candidates.

DU changes the search posture after DS/DT:

* `HOLD_TRAIN_REWARD` is not automatically a discard.  Rows can become
  regime-specialist or probation seeds when validation/holdout/regime evidence
  is non-trivial.
* Candidate generation is no longer a fixed local template around one formula.
  It uses a bounded typed grammar over event, value, sentiment, liquidity, and
  intraday primitives.
* Complexity is controlled by typed grammar, max budget, and deduplication, not
  by a hard reward penalty.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any

from our_system_phase2.services.candidate_schema import CANONICAL_CANDIDATE_FIELDS, normalize_candidate_schema


DEFAULT_SOURCE_REWARD = Path("reports/phase3dt_survivor_expansion_cm_reward_20260630/phase3cm_train_reward.csv")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3du_adaptive_regime_free_deepen_pack_20260630")
DEFAULT_REPORT_ROOT = Path("reports/phase3du_adaptive_regime_free_deepen_pack_20260630")


EVENT_FIELDS = [
    "evt_uplimit_fd_max",
    "evt_uplimit_fd_close",
    "evt_uplimit_up_limit_keep_times",
    "evt_uplimit_amount",
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
    "ctx_billboard_billboard_net_amt",
    "ctx_billboard_deal_amount_ratio",
]
INTRADAY_FIELDS = [
    "amount",
    "vol",
    "vwap",
    "m1_first5_amount",
    "m1_first15_amount",
    "m1_first30_amount",
    "m1_first15_vwap_return_vs_open",
    "m1_first30_vwap_return_vs_open",
]

WINDOWS_SHORT = [5, 10, 15, 20, 30, 40]
WINDOWS_MEDIUM = [20, 40, 60, 120]
VALID_RATIOS = [0.5, 0.6, 0.8]


def _valid_ratios_for_context(field: str) -> list[float]:
    name = str(field or "").lower()
    if any(token in name for token in ("billboard", "holder", "dividend", "share_change", "shareholder", "zls")):
        return [0.02, 0.05, 0.10]
    if "rzrq" in name:
        return [0.40, 0.60]
    return VALID_RATIOS


def _hash(text: str, length: int = 24) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:length]


def _f(value: Any, default: float = float("nan")) -> float:
    try:
        if value in (None, ""):
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


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


def _is_followup(row: dict[str, Any]) -> bool:
    return str(row.get("train_reward_decision") or "") == "TRAIN_REWARD_FOLLOWUP_READY"


def _val_hold_pos(row: dict[str, Any]) -> bool:
    return _f(row.get("validation_day_sortino")) > 0.0 and _f(row.get("holdout_day_sortino")) > 0.0


def _strict_survivor(row: dict[str, Any]) -> bool:
    return (
        _is_followup(row)
        and _val_hold_pos(row)
        and _f(row.get("validation_day_mcmc_prob_gt_0")) >= 0.5
        and _f(row.get("holdout_day_mcmc_prob_gt_0")) >= 0.5
    )


def _has_regime_signal(row: dict[str, Any]) -> bool:
    train_regime = _f(row.get("train_regime_reward_component"), 0.0)
    val = _f(row.get("validation_day_sortino"), -999.0)
    hold = _f(row.get("holdout_day_sortino"), -999.0)
    rank_ic = _f(row.get("train_rank_ic_mean"), 0.0)
    blockers = str(row.get("train_reward_blockers") or "")
    return (
        (val > 0 and hold > 0)
        or (val > 0.10 and hold > -0.10)
        or (hold > 0.10 and val > -0.10)
        or (train_regime > 0.03 and rank_ic > 0.005)
        or ("non_positive_worst_horizon_train_sortino" in blockers and (val > 0 or hold > 0))
    )


def _classify_seed(row: dict[str, Any]) -> tuple[str, float, str]:
    train = _f(row.get("train_reward"), -999.0)
    val = _f(row.get("validation_day_sortino"), -999.0)
    hold = _f(row.get("holdout_day_sortino"), -999.0)
    val_mcmc = _f(row.get("validation_day_mcmc_prob_gt_0"), 0.0)
    hold_mcmc = _f(row.get("holdout_day_mcmc_prob_gt_0"), 0.0)
    rank_ic = _f(row.get("train_rank_ic_mean"), 0.0)
    turnover = _f(row.get("train_mean_one_way_turnover"), 0.0)
    blockers = str(row.get("train_reward_blockers") or "")

    if _strict_survivor(row):
        score = train + 0.45 * val + 0.45 * hold + 0.12 * rank_ic + 0.10 * (val_mcmc + hold_mcmc)
        return "global_survivor", score, "train_followup_val_holdout_mcmc"
    if _has_regime_signal(row):
        score = max(train, 0.0) * 0.35 + max(val, 0.0) * 0.45 + max(hold, 0.0) * 0.45 + 0.08 * rank_ic
        if "extreme_turnover" in blockers or turnover > 0.75:
            score -= 0.35
        return "regime_specialist", score, "partial_oos_or_regime_signal"
    if train > 0 or val > 0 or hold > 0 or rank_ic > 0.01:
        score = max(train, 0.0) * 0.25 + max(val, 0.0) * 0.25 + max(hold, 0.0) * 0.25 + 0.05 * rank_ic
        return "probation", score, "weak_but_not_empty"
    return "reject_no_budget", -999.0, "no_positive_signal"


def _seed_budget_rows(source_rows: list[dict[str, Any]], *, max_seed_rows: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in source_rows:
        status, score, reason = _classify_seed(row)
        expression = str(row.get("expression") or "")
        family_key = _family_key(expression)
        rows.append(
            {
                "candidate_id": row.get("candidate_id", ""),
                "expression_hash": row.get("expression_hash", ""),
                "budget_status": status,
                "budget_score": round(score, 8) if math.isfinite(score) else score,
                "budget_reason": reason,
                "family_key": family_key,
                "train_reward": row.get("train_reward", ""),
                "validation_day_sortino": row.get("validation_day_sortino", ""),
                "holdout_day_sortino": row.get("holdout_day_sortino", ""),
                "train_reward_blockers": row.get("train_reward_blockers", ""),
                "expression": expression,
            }
        )
    rows.sort(key=lambda item: (_status_priority(str(item["budget_status"])), _f(item["budget_score"], -999.0)), reverse=True)

    kept: list[dict[str, Any]] = []
    family_counter: Counter[str] = Counter()
    status_caps = {
        "global_survivor": max_seed_rows,
        "regime_specialist": max(24, int(max_seed_rows * 0.55)),
        "probation": max(12, int(max_seed_rows * 0.20)),
        "reject_no_budget": 0,
    }
    status_counter: Counter[str] = Counter()
    for row in rows:
        status = str(row["budget_status"])
        if status_counter[status] >= status_caps.get(status, 0):
            continue
        family = str(row["family_key"])
        family_cap = 48 if status == "global_survivor" else 24
        if family_counter[family] >= family_cap:
            continue
        kept.append(row)
        status_counter[status] += 1
        family_counter[family] += 1
        if len(kept) >= max_seed_rows:
            break
    return kept


def _status_priority(status: str) -> int:
    return {"global_survivor": 4, "regime_specialist": 3, "probation": 2, "reject_no_budget": 1}.get(status, 0)


def _family_key(expression: str) -> str:
    text = expression.lower()
    event = "fd_max" if "fd_max" in text else "up_limit_keep" if "up_limit_keep" in text else "other_event"
    if "ctx_hfq_pb" in text:
        ctx = "pb"
    elif "market_cap" in text:
        ctx = "market_cap"
    elif "sent_" in text:
        ctx = "sentiment"
    elif "volume_ratio" in text or "turnover" in text:
        ctx = "liquidity"
    else:
        ctx = "other_ctx"
    shape = "state" if any(token in expression for token in ("StateDwell", "EventCount", "WindowStateCount")) else "age"
    return f"{event}|{ctx}|{shape}"


def _event_components() -> dict[str, list[str]]:
    components: dict[str, list[str]] = defaultdict(list)
    unusable_event_fields = {"evt_uplimit_type_code"}
    dense_event_age_fields = {"evt_uplimit_active"}
    for field in EVENT_FIELDS:
        if field in unusable_event_fields:
            continue
        if not field.startswith("evt_") or field in dense_event_age_fields:
            for op in ["EventAge", "SinceLastEvent"]:
                base = f"CSRank({op}(${field}))"
                components["event_age"].extend([base, f"Neg({base})", f"Sign(Sub({base},0.5))"])
        state_ops = ["EventCount", "WindowStateCount"] if field.startswith("evt_") else ["EventCount", "StateDwell", "WindowStateCount"]
        for op in state_ops:
            for window in [5, 10, 20, 40]:
                base = f"CSRank({op}(${field},{window}))"
                components["event_state"].extend([base, f"Neg({base})", f"Sign(Sub({base},0.5))"])
    return components


def _context_components() -> dict[str, list[str]]:
    components: dict[str, list[str]] = defaultdict(list)
    def add(field: str, family: str, windows: list[int]) -> None:
        for op in ["MaskedZScore", "ValidRatioGate"]:
            for window in windows:
                for ratio in _valid_ratios_for_context(field):
                    base = f"CSRank({op}(${field},{window},{ratio}))"
                    components[family].extend([base, f"Neg({base})"])

    for field in VALUE_FIELDS:
        add(field, "value", WINDOWS_SHORT)
    for field in SENTIMENT_FIELDS:
        add(field, "sentiment", WINDOWS_MEDIUM)
    for field in LIQUIDITY_FIELDS:
        add(field, "liquidity", WINDOWS_MEDIUM)
    for field in INTRADAY_FIELDS:
        for op in ["Delta", "Mean", "Mom"]:
            for window in [5, 10, 20, 40]:
                base = f"CSRank({op}(${field},{window}))"
                components["intraday"].extend([base, f"Neg({base})"])
    return components


def _compose_forms(event: str, context_a: str, context_b: str | None, context_c: str | None) -> list[tuple[str, str]]:
    forms = [
        ("du_add_event_context", f"CSRank(Add({event},{context_a}))"),
        ("du_sub_context_event", f"CSRank(Sub({context_a},{event}))"),
        ("du_mul_event_context", f"CSRank(Mul({event},{context_a}))"),
        ("du_neg_mul_event_context", f"Neg(CSRank(Mul({event},{context_a})))"),
    ]
    if context_b is not None:
        forms.extend(
            [
                ("du_add_event_two_context", f"CSRank(Add(Add({event},{context_a}),{context_b}))"),
                ("du_mul_event_two_context", f"CSRank(Mul({event},CSRank(Add({context_a},{context_b}))))"),
                ("du_context_spread_event", f"CSRank(Add(Sub({context_a},{context_b}),{event}))"),
            ]
        )
    if context_b is not None and context_c is not None:
        forms.extend(
            [
                (
                    "du_three_context_interaction",
                    f"CSRank(Add(Mul({event},CSRank(Add({context_a},{context_b}))),{context_c}))",
                ),
                (
                    "du_three_context_contrarian",
                    f"Neg(CSRank(Add(Mul({event},CSRank(Sub({context_a},{context_b}))),{context_c})))",
                ),
            ]
        )
    return forms


def _add_candidate(
    rows: list[dict[str, Any]],
    seen: set[str],
    *,
    expression: str,
    lane: str,
    seed_status: str,
    parent_id: str,
    mutation_type: str,
) -> None:
    digest = _hash(expression)
    if digest in seen:
        return
    seen.add(digest)
    idx = len(rows) + 1
    row: dict[str, Any] = {
        "candidate_id": f"phase3du_{idx:05d}",
        "expression_hash": digest,
        "expression": expression,
        "generator_arm": "phase3du_adaptive_regime_free_deepen",
        "generator_route": "phase3du-adaptive-regime-free-deepen-pack",
        "seed": "20260630",
        "round_id": lane,
        "parent_id": parent_id,
        "mutation_type": mutation_type,
        "mean_one_way_turnover": "0.12",
        "phase3ca_proxy_quality": "0",
        "proxy_quality": "0",
        "validation_usage": "report_only",
        "holdout_usage": "report_only",
        "phase3du_lane": lane,
        "phase3du_seed_status": seed_status,
        "metric_boundary": "adaptive free-deepen pack only; Phase3CM train reward is optimizer feedback",
    }
    row.update(normalize_candidate_schema(row))
    rows.append(row)


def _sample(items: list[str], offset: int, count: int, stride: int = 1) -> list[str]:
    if not items:
        return []
    out: list[str] = []
    idx = offset % len(items)
    while len(out) < count:
        out.append(items[idx % len(items)])
        idx += max(1, stride)
    return out


def _build_free_deepen_rows(seed_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    event_components = _event_components()
    context_components = _context_components()
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    lane_specs = [
        ("phase3du_lane_a_global_survivor_deepen", "global_survivor", ["value", "sentiment", "liquidity"], 56),
        ("phase3du_lane_b_regime_specialist_budget", "regime_specialist", ["value", "sentiment", "liquidity", "intraday"], 42),
        ("phase3du_lane_c_probation_explore", "probation", ["value", "sentiment", "intraday"], 24),
        ("phase3du_lane_d_fresh_typed_complex", "fresh_complex", ["value", "sentiment", "liquidity", "intraday"], 96),
    ]

    seeds_by_status: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in seed_rows:
        seeds_by_status[str(row["budget_status"])].append(row)

    for lane, status, families, per_seed_budget in lane_specs:
        lane_seeds = seeds_by_status.get(status, []) if status != "fresh_complex" else [{"candidate_id": "fresh", "budget_status": "fresh_complex"}]
        if not lane_seeds:
            continue
        for seed_idx, seed in enumerate(lane_seeds):
            event_pool = event_components["event_age"] if status != "fresh_complex" else event_components["event_age"] + event_components["event_state"]
            if "regime" in lane or status == "fresh_complex":
                event_pool = event_components["event_age"] + event_components["event_state"]
            event_sample = _sample(event_pool, seed_idx * 7, min(16, len(event_pool)), stride=3)
            context_a_pool: list[str] = []
            for family in families:
                context_a_pool.extend(context_components[family])
            context_b_pool = context_components["sentiment"] + context_components["liquidity"]
            context_c_pool = context_components["intraday"] + context_components["value"]
            generated_for_seed = 0
            for e_idx, event in enumerate(event_sample):
                a_items = _sample(context_a_pool, seed_idx * 11 + e_idx * 5, 8, stride=5)
                b_items = _sample(context_b_pool, seed_idx * 13 + e_idx * 3, 5, stride=7)
                c_items = _sample(context_c_pool, seed_idx * 17 + e_idx * 2, 3, stride=11)
                for a_idx, context_a in enumerate(a_items):
                    context_b = b_items[a_idx % len(b_items)] if b_items else None
                    context_c = c_items[a_idx % len(c_items)] if c_items else None
                    for mutation_type, expression in _compose_forms(event, context_a, context_b, context_c):
                        _add_candidate(
                            rows,
                            seen,
                            expression=expression,
                            lane=lane,
                            seed_status=status,
                            parent_id=str(seed.get("candidate_id") or status),
                            mutation_type=mutation_type,
                        )
                        generated_for_seed += 1
                        if generated_for_seed >= per_seed_budget:
                            break
                    if generated_for_seed >= per_seed_budget:
                        break
                if generated_for_seed >= per_seed_budget:
                    break
    return rows


def _balanced_limit(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0 or len(rows) <= limit:
        return rows
    queues: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    for row in rows:
        queues[str(row.get("phase3du_lane") or "unknown")].append(row)
    selected: list[dict[str, Any]] = []
    lane_order = sorted(queues)
    while len(selected) < limit and any(queues.values()):
        for lane in lane_order:
            if queues[lane]:
                selected.append(queues[lane].popleft())
                if len(selected) >= limit:
                    break
    for idx, row in enumerate(selected, 1):
        row["candidate_id"] = f"phase3du_{idx:05d}"
        row.update(normalize_candidate_schema(row))
    return selected


def _render_md(summary: dict[str, Any]) -> str:
    lines = [
        "# Phase3DU Adaptive Regime Free-Deepen Pack",
        "",
        f"candidate_count: `{summary['candidate_count']}`",
        f"raw_candidate_count: `{summary['raw_candidate_count']}`",
        f"seed_count: `{summary['seed_count']}`",
        f"seed_status_counts: `{summary['seed_status_counts']}`",
        f"lane_counts: `{summary['lane_counts']}`",
        "",
        "## Boundary",
        "",
        "- HOLD is not treated as discard; regime specialist and probation seeds receive bounded budgets.",
        "- Generator uses typed grammar with event/value/sentiment/liquidity/intraday components.",
        "- Complexity is bounded by grammar and candidate budget, not by direct complexity reward penalty.",
        "- Validation and holdout are report-only and must not feed optimizer reward.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-reward", type=Path, default=DEFAULT_SOURCE_REWARD)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-seed-rows", type=int, default=192)
    parser.add_argument("--max-candidates", type=int, default=8192)
    args = parser.parse_args(argv)

    source_rows = _read_csv(args.source_reward.resolve())
    seed_rows = _seed_budget_rows(source_rows, max_seed_rows=int(args.max_seed_rows))
    raw_rows = _build_free_deepen_rows(seed_rows)
    rows = _balanced_limit(raw_rows, int(args.max_candidates))

    seed_status_counts = Counter(str(row["budget_status"]) for row in seed_rows)
    lane_counts = Counter(str(row.get("phase3du_lane") or "unknown") for row in rows)

    output_root = args.output_root.resolve()
    report_root = args.report_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    summary = {
        "phase": "Phase3DU",
        "source_reward": str(args.source_reward.resolve()),
        "source_rows": len(source_rows),
        "seed_count": len(seed_rows),
        "seed_status_counts": dict(seed_status_counts),
        "raw_candidate_count": len(raw_rows),
        "candidate_count": len(rows),
        "lane_counts": dict(lane_counts),
        "decision": "PACK_READY_FOR_PHASE3CM_REWARD_AUDIT",
        "optimizer_metric": "train_portfolio_sortino_rankic_regime_composite_reward",
    }

    for root in (output_root, report_root):
        _write_csv(root / "phase3du_seed_budget_table.csv", seed_rows)
        _write_csv(root / "phase3du_adaptive_regime_candidate_audit.csv", rows)
        _write_json(root / "phase3du_adaptive_regime_pack_summary.json", summary)
        (root / "PHASE3DU_ADAPTIVE_REGIME_FREE_DEEPEN_PACK_20260630.md").write_text(
            _render_md(summary), encoding="utf-8"
        )

    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
