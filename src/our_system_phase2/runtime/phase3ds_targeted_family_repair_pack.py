"""Build a targeted Phase3DS family repair candidate pack.

This route does not search the full grammar.  It materializes bounded variants
around a train-reward followup family so Phase3CM can test whether the local
mechanism survives stricter horizon, turnover, validation, and holdout checks.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from our_system_phase2.services.candidate_schema import CANONICAL_CANDIDATE_FIELDS, normalize_candidate_schema


DEFAULT_OUTPUT_ROOT = Path("runtime/phase3ds_targeted_family_repair_pack_20260629")
DEFAULT_REPORT_ROOT = Path("reports/phase3ds_targeted_family_repair_pack_20260629")


EVENT_FIELDS = [
    "evt_uplimit_up_limit_keep_times",
    "evt_uplimit_fd_max",
    "evt_uplimit_amount",
    "evt_uplimit_age_min",
]

CONTEXT_FIELDS = [
    "ctx_hfq_pb",
    "ctx_hfq_pe_ttm",
    "ctx_hfq_ps_ttm",
    "ctx_hfq_float_market_cap_yuan",
    "ctx_hfq_market_cap_yuan",
    "ctx_hfq_turnover_ratio",
    "ctx_hfq_volume_ratio",
    "ctx_rzrq_rzyezb",
    "ctx_billboard_billboard_net_amt",
    "ctx_billboard_deal_amount_ratio",
]

EVENT_OPS = ["EventAge", "SinceLastEvent", "EventCount", "WindowStateCount"]
CONTEXT_OPS = ["MaskedZScore", "ValidRatioGate"]
WINDOWS = [20, 40, 60, 120]
VALID_RATIOS = [0.6, 0.8]
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
    fieldnames: list[str] = []
    for name in CANONICAL_CANDIDATE_FIELDS:
        if name not in fieldnames:
            fieldnames.append(name)
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


def _context_expr(op: str, field: str, window: int, valid_ratio: float) -> str:
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


def _variant_expressions() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    idx = 0
    for event_field in EVENT_FIELDS:
        for event_op, event_raw in _event_exprs(event_field):
            event_rank = f"CSRank({event_raw})"
            event_sign = f"Sign({event_rank})"
            for context_field in CONTEXT_FIELDS:
                for context_op in CONTEXT_OPS:
                    for window in WINDOWS:
                        for valid_ratio in _valid_ratios_for_context(context_field):
                            context_raw = _context_expr(context_op, context_field, window, valid_ratio)
                            context_rank = f"CSRank({context_raw})"
                            forms = [
                                f"Neg(CSRank(Mul({event_sign},{context_rank})))",
                                f"CSRank(Mul({event_sign},{context_rank}))",
                                f"Neg(CSRank(Add({event_rank},{context_rank})))",
                                f"CSRank(Add(Neg({event_rank}),{context_rank}))",
                            ]
                            for expression in forms:
                                digest = _hash(expression)
                                if digest in seen:
                                    continue
                                seen.add(digest)
                                idx += 1
                                row: dict[str, Any] = {
                                    "candidate_id": f"phase3ds_{idx:05d}",
                                    "expression_hash": digest,
                                    "expression": expression,
                                    "generator_arm": "phase3ds_targeted_family_repair",
                                    "generator_route": "phase3ds-targeted-family-repair-pack",
                                    "seed": "20260629",
                                    "round_id": "phase3ds_event_age_value_repair",
                                    "parent_id": "phase3cp_21555",
                                    "mutation_type": "targeted_event_value_family_repair",
                                    "mean_one_way_turnover": "0.05",
                                    "phase3ca_proxy_quality": "0",
                                    "proxy_quality": "0",
                                    "validation_usage": "report_only",
                                    "holdout_usage": "report_only",
                                    "phase3ds_event_field": event_field,
                                    "phase3ds_event_op": event_op,
                                    "phase3ds_context_field": context_field,
                                    "phase3ds_context_op": context_op,
                                    "phase3ds_window": window,
                                    "phase3ds_valid_ratio": valid_ratio,
                                    "metric_boundary": "targeted pack only; Phase3CM train reward is the first reward gate",
                                }
                                row.update(normalize_candidate_schema(row))
                                rows.append(row)
    return rows


def _render_md(summary: dict[str, Any]) -> str:
    lines = [
        "# Phase3DS Targeted Family Repair Pack",
        "",
        f"candidate_count: `{summary['candidate_count']}`",
        f"parent_candidate: `{summary['parent_candidate']}`",
        f"event_fields: `{summary['event_fields']}`",
        f"context_fields: `{summary['context_fields']}`",
        "",
        "## Boundary",
        "",
        "- This is not full fresh search.",
        "- It only tests whether the clean Phase3DR family can be repaired into validation/holdout stability.",
        "- Event fields enter only through typed event/state primitives.",
        "- Validation and holdout remain report-only; no promotion decision is made here.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-candidates", type=int, default=0)
    args = parser.parse_args(argv)

    rows = _variant_expressions()
    if args.max_candidates and args.max_candidates > 0:
        rows = rows[: int(args.max_candidates)]

    output_root = args.output_root.resolve()
    report_root = args.report_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    summary = {
        "phase": "Phase3DS",
        "parent_candidate": "phase3cp_21555",
        "candidate_count": len(rows),
        "event_fields": EVENT_FIELDS,
        "context_fields": CONTEXT_FIELDS,
        "event_ops": EVENT_OPS,
        "context_ops": CONTEXT_OPS,
        "windows": WINDOWS,
        "valid_ratios": VALID_RATIOS,
        "decision": "PACK_READY_FOR_PHASE3CM_REWARD_AUDIT",
    }

    for root in (output_root, report_root):
        _write_csv(root / "phase3ds_targeted_family_candidate_audit.csv", rows)
        _write_json(root / "phase3ds_targeted_family_pack_summary.json", summary)
        (root / "PHASE3DS_TARGETED_FAMILY_REPAIR_PACK_20260629.md").write_text(_render_md(summary), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
