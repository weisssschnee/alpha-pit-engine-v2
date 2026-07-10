"""Build Phase3DT survivor expansion and horizon-fragility repair candidates.

Phase3DS found a small strict-survivor pocket around event-age of limit seal
strength (`evt_uplimit_fd_max`) combined with PB/value context.  It also showed
market-cap variants with validation/holdout strength but train horizon
fragility.  This pack expands those two pockets with known-safe typed
primitives and keeps lanes balanced before Phase3CM reward evaluation.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

from our_system_phase2.services.candidate_schema import CANONICAL_CANDIDATE_FIELDS, normalize_candidate_schema


DEFAULT_OUTPUT_ROOT = Path("runtime/phase3dt_survivor_expansion_repair_pack_20260630")
DEFAULT_REPORT_ROOT = Path("reports/phase3dt_survivor_expansion_repair_pack_20260630")


VALUE_CONTEXT_FIELDS = [
    "ctx_hfq_pb",
    "ctx_hfq_pe_ttm",
    "ctx_hfq_ps_ttm",
    "ctx_hfq_float_market_cap_yuan",
    "ctx_hfq_market_cap_yuan",
]
ACTIVITY_CONTEXT_FIELDS = [
    "ctx_hfq_turnover_ratio",
    "ctx_hfq_volume_ratio",
    "ctx_rzrq_rzyezb",
    "ctx_billboard_billboard_net_amt",
]
CONTEXT_OPS = ["MaskedZScore", "ValidRatioGate"]
EVENT_OPS = ["EventAge", "SinceLastEvent", "EventCount", "WindowStateCount"]
WINDOWS_FAST = [5, 10, 15, 20, 30, 40, 60]
WINDOWS_SLOW = [20, 40, 60, 120]
VALID_RATIOS = [0.5, 0.6, 0.8]
EVENT_STATE_WINDOWS = [10, 20, 40]


def _valid_ratios_for_context(field: str) -> list[float]:
    name = str(field or "").lower()
    if any(token in name for token in ("billboard", "holder", "dividend", "share_change", "shareholder", "zls")):
        return [0.02, 0.05, 0.10]
    if "rzrq" in name:
        return [0.40, 0.60]
    return VALID_RATIOS


def _hash(text: str, length: int = 24) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:length]


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


def _ctx(op: str, field: str, window: int, valid_ratio: float) -> str:
    return f"{op}(${field},{window},{valid_ratio})"


def _event_exprs(field: str) -> list[tuple[str, str]]:
    if field == "evt_uplimit_type_code":
        return []
    expressions: list[tuple[str, str]] = []
    if not field.startswith("evt_") or field == "evt_uplimit_active":
        for op in ("EventAge", "SinceLastEvent"):
            expressions.append((op, f"{op}(${field})"))
    for op in ("EventCount", "WindowStateCount"):
        for window in EVENT_STATE_WINDOWS:
            expressions.append((f"{op}_{window}", f"{op}(${field},{window})"))
    return expressions


def _add_row(
    rows: list[dict[str, Any]],
    seen: set[str],
    *,
    expression: str,
    lane: str,
    parent_id: str,
    event_field: str,
    event_op: str,
    context_field: str,
    context_op: str,
    window: int,
    valid_ratio: float,
    mutation_type: str,
) -> None:
    digest = _hash(expression)
    if digest in seen:
        return
    seen.add(digest)
    idx = len(rows) + 1
    row: dict[str, Any] = {
        "candidate_id": f"phase3dt_{idx:05d}",
        "expression_hash": digest,
        "expression": expression,
        "generator_arm": "phase3dt_survivor_expansion_repair",
        "generator_route": "phase3dt-survivor-expansion-repair-pack",
        "seed": "20260630",
        "round_id": lane,
        "parent_id": parent_id,
        "mutation_type": mutation_type,
        "mean_one_way_turnover": "0.08",
        "phase3ca_proxy_quality": "0",
        "proxy_quality": "0",
        "validation_usage": "report_only",
        "holdout_usage": "report_only",
        "phase3dt_lane": lane,
        "phase3dt_event_field": event_field,
        "phase3dt_event_op": event_op,
        "phase3dt_context_field": context_field,
        "phase3dt_context_op": context_op,
        "phase3dt_window": window,
        "phase3dt_valid_ratio": valid_ratio,
        "metric_boundary": "Phase3DT pack only; Phase3CM train reward is first reward gate",
    }
    row.update(normalize_candidate_schema(row))
    rows.append(row)


def _rank_forms(event_rank: str, event_sign: str, context_rank: str) -> list[tuple[str, str]]:
    return [
        ("add_recent_event_high_context", f"CSRank(Add(Neg({event_rank}),{context_rank}))"),
        ("add_recent_event_low_context", f"CSRank(Add(Neg({event_rank}),Neg({context_rank})))"),
        ("neg_add_old_event_high_context", f"Neg(CSRank(Add({event_rank},{context_rank})))"),
        ("neg_add_old_event_low_context", f"Neg(CSRank(Add({event_rank},Neg({context_rank}))))"),
        ("mul_signed_event_high_context", f"Neg(CSRank(Mul({event_sign},{context_rank})))"),
        ("mul_signed_event_low_context", f"CSRank(Mul({event_sign},{context_rank}))"),
    ]


def _build_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    # Lane A: strict survivor pocket.  Phase3DS strict survivors were this
    # family, especially fd_max event age with PB at short windows.
    for event_field in ["evt_uplimit_fd_max", "evt_uplimit_fd_close"]:
        for event_op, event_raw in _event_exprs(event_field):
            event_rank = f"CSRank({event_raw})"
            event_sign = f"Sign(Sub({event_rank},0.5))"
            for context_field in VALUE_CONTEXT_FIELDS:
                for context_op in CONTEXT_OPS:
                    for window in WINDOWS_FAST:
                        for valid_ratio in _valid_ratios_for_context(context_field):
                            context_rank = f"CSRank({_ctx(context_op, context_field, window, valid_ratio)})"
                            for mutation_type, expression in _rank_forms(event_rank, event_sign, context_rank):
                                _add_row(
                                    rows,
                                    seen,
                                    expression=expression,
                                    lane="phase3dt_lane_a_fdmax_value_survivor_expansion",
                                    parent_id="phase3ds_01320",
                                    event_field=event_field,
                                    event_op=event_op,
                                    context_field=context_field,
                                    context_op=context_op,
                                    window=window,
                                    valid_ratio=valid_ratio,
                                    mutation_type=mutation_type,
                                )

    # Lane B: horizon-fragility repair.  These forms had validation/holdout
    # strength in DS but failed worst-horizon train checks.
    for event_field in ["evt_uplimit_up_limit_keep_times", "evt_uplimit_fd_max"]:
        for event_op, event_raw in _event_exprs(event_field):
            event_rank = f"CSRank({event_raw})"
            event_sign = f"Sign(Sub({event_rank},0.5))"
            for context_field in ["ctx_hfq_market_cap_yuan", "ctx_hfq_float_market_cap_yuan", "ctx_hfq_pb"]:
                for context_op in CONTEXT_OPS:
                    for window in WINDOWS_FAST:
                        for valid_ratio in _valid_ratios_for_context(context_field):
                            context_rank = f"CSRank({_ctx(context_op, context_field, window, valid_ratio)})"
                            for mutation_type, expression in _rank_forms(event_rank, event_sign, context_rank):
                                _add_row(
                                    rows,
                                    seen,
                                    expression=expression,
                                    lane="phase3dt_lane_b_horizon_fragility_repair",
                                    parent_id="phase3ds_market_cap_val_holdout_positive",
                                    event_field=event_field,
                                    event_op=event_op,
                                    context_field=context_field,
                                    context_op=context_op,
                                    window=window,
                                    valid_ratio=valid_ratio,
                                    mutation_type=mutation_type,
                                )

    # Lane C: small typed event-state expansion using supported event count and
    # dwell primitives.  This is deliberately bounded and separate from A/B.
    for event_field in ["evt_uplimit_fd_max", "evt_uplimit_up_limit_keep_times"]:
        state_ops = ["EventCount", "WindowStateCount"] if event_field.startswith("evt_") else ["EventCount", "StateDwell", "WindowStateCount"]
        for state_op in state_ops:
            for state_window in [10, 20, 40]:
                event_raw = f"{state_op}(${event_field},{state_window})"
                event_rank = f"CSRank({event_raw})"
                event_sign = f"Sign(Sub({event_rank},0.5))"
                for context_field in ["ctx_hfq_pb", "ctx_hfq_market_cap_yuan", *ACTIVITY_CONTEXT_FIELDS]:
                    for context_op in CONTEXT_OPS:
                        for window in [20, 40, 60]:
                            for valid_ratio in [0.6, 0.8]:
                                context_rank = f"CSRank({_ctx(context_op, context_field, window, valid_ratio)})"
                                for mutation_type, expression in _rank_forms(event_rank, event_sign, context_rank)[:4]:
                                    _add_row(
                                        rows,
                                        seen,
                                        expression=expression,
                                        lane="phase3dt_lane_c_typed_event_state_probe",
                                        parent_id="phase3ds_event_state_typed_probe",
                                        event_field=event_field,
                                        event_op=state_op,
                                        context_field=context_field,
                                        context_op=context_op,
                                        window=window,
                                        valid_ratio=valid_ratio,
                                        mutation_type=f"{mutation_type}_state_window_{state_window}",
                                    )

    return rows


def _balanced_limit(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0 or len(rows) <= limit:
        return rows
    queues: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    for row in rows:
        queues[str(row.get("phase3dt_lane") or "unknown")].append(row)
    selected: list[dict[str, Any]] = []
    lane_order = sorted(queues)
    while len(selected) < limit and any(queues.values()):
        for lane in lane_order:
            if queues[lane]:
                selected.append(queues[lane].popleft())
                if len(selected) >= limit:
                    break
    for idx, row in enumerate(selected, 1):
        row["candidate_id"] = f"phase3dt_{idx:05d}"
        row.update(normalize_candidate_schema(row))
    return selected


def _render_md(summary: dict[str, Any]) -> str:
    lines = [
        "# Phase3DT Survivor Expansion Repair Pack",
        "",
        f"candidate_count: `{summary['candidate_count']}`",
        f"raw_candidate_count: `{summary['raw_candidate_count']}`",
        f"lane_counts: `{summary['lane_counts']}`",
        "",
        "## Boundary",
        "",
        "- This expands the Phase3DS strict-survivor pocket and market-cap horizon-fragility pocket.",
        "- It uses only existing evaluator primitives that have already run in DS or are implemented in real_market_validation.",
        "- Validation and holdout remain report-only; this pack cannot promote candidates.",
        "- High-turnover event-state multiplication is not expanded.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-candidates", type=int, default=4096)
    args = parser.parse_args(argv)

    raw_rows = _build_rows()
    rows = _balanced_limit(raw_rows, int(args.max_candidates))
    lane_counts: dict[str, int] = {}
    for row in rows:
        lane = str(row.get("phase3dt_lane") or "unknown")
        lane_counts[lane] = lane_counts.get(lane, 0) + 1

    output_root = args.output_root.resolve()
    report_root = args.report_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    summary = {
        "phase": "Phase3DT",
        "raw_candidate_count": len(raw_rows),
        "candidate_count": len(rows),
        "lane_counts": lane_counts,
        "decision": "PACK_READY_FOR_PHASE3CM_REWARD_AUDIT",
        "parent_phase": "Phase3DS",
        "optimizer_metric": "train_portfolio_sortino_rankic_regime_composite_reward",
    }

    for root in (output_root, report_root):
        _write_csv(root / "phase3dt_survivor_expansion_candidate_audit.csv", rows)
        _write_json(root / "phase3dt_survivor_expansion_pack_summary.json", summary)
        (root / "PHASE3DT_SURVIVOR_EXPANSION_REPAIR_PACK_20260630.md").write_text(
            _render_md(summary), encoding="utf-8"
        )

    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
