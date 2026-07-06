"""Phase3CP real-CM small closed-loop search.

This route is the first Phase3CP escalation that replaces the controlled CM
fixture with the real true-1min train portfolio Sortino reward audit.

It remains diagnostic-only:

CO/CP budget -> bounded generation -> CA bridge -> field-availability gate ->
real CM train reward -> CN feedback memory -> CO reschedule.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _discover_panels,
    _fields,
    _write_csv,
    _write_json,
)
from our_system_phase2.runtime.phase3ca_build_bz_candidate_audit import build_candidate_table
from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    _candidate_summary_from_reward_atoms,
    main as phase3cm_main,
)
from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import build_feedback_memory
from our_system_phase2.runtime.phase3cp_reward_gated_medium_search_smoke import (
    _copy_report_files,
    _decisionize,
    _generate_for_arm,
    _scale_budgets,
    _write_arm_outputs,
)
from our_system_phase2.runtime.phase3bp_true1min_search_algorithm_smoke import (
    begin_generation_accounting,
    build_checked_seed_policy,
    end_generation_accounting,
)
from our_system_phase2.services.candidate_schema import OPTIMIZER_REWARD_METRIC, normalize_candidate_schema, safe_float
from our_system_phase2.services.multi_arm_scheduler import build_arm_schedule, read_csv_rows


REPO = Path(__file__).resolve().parents[3]
DEFAULT_CO_ROOT = Path("reports/phase3cp_reward_gated_medium_search_smoke_20260623")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cp_real_cm_small_loop_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3cp_real_cm_small_loop_20260623")
DEFAULT_MEMORY_GLOBS = [
    "**/*.csv",
    "**/*search_memory*.json",
    "**/candidate_ledger.json",
    "**/*search_memory_ledger.csv",
    "**/*top_decisions.csv",
    "**/*candidate_audit.csv",
    "**/*generated_candidates.csv",
    "**/*train_reward.csv",
]
FIELD_RE = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*")
NUM_RE = re.compile(r"(?<![A-Za-z0-9_])(?:\d+\.\d+|\d+)(?![A-Za-z0-9_])")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _stable_expression_hash(expression: str) -> str:
    return hashlib.sha256(expression.encode("utf-8")).hexdigest()[:24]


def _canonical_expression_key(expression: str) -> str:
    canonical = re.sub(r"\s+", "", expression.strip())
    return f"v2cand-{hashlib.sha1(canonical.encode('utf-8')).hexdigest()[:12]}"


def _skeleton_key(expression: str) -> str:
    canonical = re.sub(r"\s+", "", expression.strip())
    skeleton = FIELD_RE.sub("FIELD", canonical)
    skeleton = NUM_RE.sub("WINDOW", skeleton)
    return f"skeleton-{hashlib.sha1(skeleton.encode('utf-8')).hexdigest()[:16]}"


def _add_memory_key(hashes: set[str], value: Any) -> None:
    text = str(value or "").strip()
    if 8 <= len(text) <= 160:
        hashes.add(text)


def _add_expression_memory_keys(hashes: set[str], expression: Any) -> None:
    expr = str(expression or "").strip()
    if not expr:
        return
    digest = _stable_expression_hash(expr)
    hashes.add(digest)
    hashes.add(f"phase3bp:{digest}")
    hashes.add(_canonical_expression_key(expr))


def _is_structural_block_record(row: dict[str, Any]) -> bool:
    text = " ".join(
        str(row.get(key) or "")
        for key in (
            "memory_block_policy",
            "typed_gate_decision",
            "typed_gate_reason",
            "blocker_flags",
            "phase3bp_blocker_flags",
            "phase3ca_blocker_flags",
            "memory_block_reason",
            "pre_cm_semantic_decision",
            "pre_cm_semantic_reasons",
            "reason",
            "decision",
        )
    ).lower()
    return any(token in text for token in ("unsafe", "blocked", "block", "quarantine", "typed_gate"))


def _add_structural_memory_keys(hashes: set[str], row: dict[str, Any]) -> None:
    if not _is_structural_block_record(row):
        return
    _add_memory_key(hashes, row.get("skeleton_key"))
    expression = row.get("expression")
    if expression:
        hashes.add(_skeleton_key(str(expression)))


def _collect_json_memory_keys(payload: Any, hashes: set[str], *, depth: int = 0) -> None:
    if depth > 8:
        return
    if isinstance(payload, list):
        for item in payload:
            _collect_json_memory_keys(item, hashes, depth=depth + 1)
        return
    if not isinstance(payload, dict):
        return

    for key in ("expression_hash", "candidate_hash", "search_memory_key", "expression_key"):
        _add_memory_key(hashes, payload.get(key))
    _add_expression_memory_keys(hashes, payload.get("expression"))
    _add_structural_memory_keys(hashes, payload)

    for key in ("expression_keys", "search_memory_keys"):
        values = payload.get(key)
        if isinstance(values, dict):
            values = list(values.keys())
        if isinstance(values, list):
            for value in values:
                _add_memory_key(hashes, value)

    for key in (
        "memory_entries",
        "records",
        "candidates",
        "candidate_ledger",
        "rows",
        "entries",
        "duplicate_skip_events",
        "blocked_keys",
    ):
        if key in payload:
            _collect_json_memory_keys(payload.get(key), hashes, depth=depth + 1)


def _round(value: Any, ndigits: int = 8) -> float | None:
    out = safe_float(value)
    if not math.isfinite(out):
        return None
    return round(out, ndigits)


def _budget_table_path(co_root: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        return _resolve(explicit)
    candidates = [
        co_root / "phase3cp_next_arm_budget_table.csv",
        co_root / "phase3co_arm_budget_table.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"no CP/CO arm budget table under {co_root}")


def _load_memory_hashes(memory_roots: list[Path], memory_globs: list[str]) -> tuple[set[str], list[dict[str, Any]]]:
    hashes: set[str] = set()
    files_seen: set[Path] = set()
    rows: list[dict[str, Any]] = []
    for raw_root in memory_roots:
        root = _resolve(raw_root)
        if root.is_file():
            files = [root]
        elif root.is_dir():
            files = []
            for pattern in memory_globs:
                files.extend(root.glob(pattern))
        else:
            rows.append({"memory_root": str(root), "exists": False, "file_count": 0, "hash_count": 0})
            continue

        before = len(hashes)
        file_count = 0
        parse_error_count = 0
        for file_path in files:
            file_path = file_path.resolve()
            suffix = file_path.suffix.lower()
            if file_path in files_seen or suffix not in {".csv", ".json"}:
                continue
            files_seen.add(file_path)
            file_count += 1
            try:
                if suffix == ".csv":
                    for row in _read_csv(file_path):
                        for key in ("expression_hash", "candidate_hash", "search_memory_key", "expression_key"):
                            _add_memory_key(hashes, row.get(key))
                        _add_expression_memory_keys(hashes, row.get("expression"))
                        _add_structural_memory_keys(hashes, row)
                else:
                    payload = json.loads(file_path.read_text(encoding="utf-8-sig"))
                    _collect_json_memory_keys(payload, hashes)
            except Exception:
                parse_error_count += 1
        rows.append(
            {
                "memory_root": str(root),
                "exists": True,
                "file_count": file_count,
                "hash_count": len(hashes) - before,
                "parse_error_count": parse_error_count,
            }
        )
    return hashes, rows


def _write_semantic_block_memory(memory_root: Path, rejected: list[dict[str, Any]], run_label: str) -> Path | None:
    if not rejected:
        return None
    memory_root = _resolve(memory_root)
    memory_root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for row in rejected:
        expression = str(row.get("expression") or "").strip()
        if not expression:
            continue
        expression_hash = str(row.get("expression_hash") or _stable_expression_hash(expression))
        expression_key = str(row.get("expression_key") or _canonical_expression_key(expression))
        skeleton_key = str(row.get("skeleton_key") or _skeleton_key(expression))
        rows.append(
            {
                "run_label": run_label,
                "candidate_id": row.get("candidate_id"),
                "generator_arm": row.get("generator_arm"),
                "expression": expression,
                "expression_hash": expression_hash,
                "candidate_hash": row.get("candidate_hash") or expression_hash,
                "expression_key": expression_key,
                "search_memory_key": expression_key,
                "skeleton_key": skeleton_key,
                "memory_block_policy": "semantic_viability_block",
                "memory_block_reason": row.get("memory_block_reason") or "blocked_weak_semantic_viability",
                "pre_cm_semantic_decision": row.get("pre_cm_semantic_decision"),
                "pre_cm_semantic_reasons": row.get("pre_cm_semantic_reasons"),
                "semantic_total_rows": row.get("semantic_total_rows"),
                "semantic_nonzero_shards": row.get("semantic_nonzero_shards"),
                "semantic_checked_shards": row.get("semantic_checked_shards"),
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
    if not rows:
        return None
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_label).strip("_") or "semantic_block"
    path = memory_root / f"{safe_label}_semantic_block_memory.csv"
    _write_csv(path, rows)
    return path


def _generate_candidates(
    *,
    budget_rows: list[dict[str, Any]],
    total_budget: int,
    initial_blocked: set[str],
    available_fields: set[str],
    output_root: Path,
    report_root: Path,
    shortfall_fill_rounds: int = 4,
    shortfall_oversample_multiplier: float = 1.5,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    scaled_plan = _scale_budgets(budget_rows, total_budget)
    policy, _, _ = build_checked_seed_policy(exploration=0.94)
    generated: list[dict[str, Any]] = []
    blocked: set[str] = set(initial_blocked)

    generation_attempt_rows: list[dict[str, Any]] = []

    def run_arm(arm: dict[str, Any], *, requested_budget: int, phase: str, round_index: int) -> list[dict[str, Any]]:
        before = len(generated)
        previous_accounting, _ = begin_generation_accounting()
        try:
            rows = _generate_for_arm(
                arm,
                budget=max(0, int(requested_budget)),
                blocked=blocked,
                policy=policy,
                start_idx=len(generated) + 1,
                available_fields=available_fields,
            )
        finally:
            accounting_row = end_generation_accounting(previous_accounting)
        accounting_row["emitted_produced"] = len(rows)
        known_drop = sum(
            int(accounting_row.get(key) or 0)
            for key in ("dropped_by_emit_cap", "dropped_by_diversity_cap", "dropped_by_global_pool")
        )
        accounting_row["accepted_to_emitted_unexplained"] = max(
            0,
            int(accounting_row.get("accepted_unique") or 0) - len(rows) - known_drop,
        )
        for row in rows:
            digest = str(row.get("expression_hash") or "")
            if digest:
                blocked.add(digest)
                blocked.add(f"phase3bp:{digest}")
            _add_expression_memory_keys(blocked, row.get("expression"))
        generated.extend(rows)
        generation_attempt_rows.append(
            {
                "phase": phase,
                "round_index": round_index,
                "arm_id": arm.get("arm_id", ""),
                "route_hint": arm.get("route_hint", ""),
                "requested_budget": int(requested_budget),
                "produced_count": len(rows),
                "generated_before": before,
                "generated_after": len(generated),
                "remaining_after": max(0, int(total_budget) - len(generated)),
                "per_arm_attempt_accept": (
                    f"{arm.get('arm_id', '')}:"
                    f"{accounting_row.get('raw_attempts', 0)}/"
                    f"{accounting_row.get('accepted_unique', 0)}"
                ),
                **accounting_row,
            }
        )
        return rows

    for arm in scaled_plan:
        budget = int(arm.get("cp_smoke_candidate_budget") or 0)
        run_arm(arm, requested_budget=budget, phase="scheduled", round_index=0)

    shortfall_rows: list[dict[str, Any]] = []
    fill_arms = [
        {
            "arm_id": "turnover_aware_fresh",
            "route_hint": "phase3bp-true1min-turnover-aware",
            "category": "fresh",
        },
        {
            "arm_id": "typed_ast_fresh",
            "route_hint": "phase3bt-ast-algorithm-bakeoff",
            "category": "fresh",
        },
        {
            "arm_id": "rx_ucb_fresh",
            "route_hint": "phase3bs-adaptive-ucb-cem-practice",
            "category": "fresh",
        },
        {
            "arm_id": "challenger_repair",
            "route_hint": "future-phase3cr-repair",
            "category": "fresh",
        },
        {
            "arm_id": "event_state",
            "route_hint": "future-event-state-generator",
            "category": "event",
        },
        {
            "arm_id": "random_orthogonal",
            "route_hint": "control-random-orthogonal",
            "category": "control",
        },
    ]
    previous_total = -1
    for round_index in range(1, max(0, int(shortfall_fill_rounds)) + 1):
        if len(generated) >= total_budget:
            break
        if len(generated) == previous_total:
            break
        previous_total = len(generated)
        missing_at_round_start = total_budget - len(generated)
        per_arm_budget = max(
            256,
            int(math.ceil((missing_at_round_start * max(1.0, float(shortfall_oversample_multiplier))) / len(fill_arms))),
        )
        for fill_arm in fill_arms:
            if len(generated) >= total_budget:
                break
            rows = run_arm(fill_arm, requested_budget=per_arm_budget, phase="shortfall_fill", round_index=round_index)
            shortfall_rows.extend(rows)
        if len(generated) == previous_total:
            break

    decisions = [_decisionize(row, idx) for idx, row in enumerate(generated[:total_budget], 1)]
    for row in decisions:
        row["phase3cp_real_cm_loop"] = "true"
        row["metric_boundary"] = "Phase3CP real-CM loop generation metrics are CA ranking only; CM train_reward is the feedback source"
        row.update(normalize_candidate_schema(row))

    search_root = output_root / "search_outputs"
    report_search_root = report_root / "search_outputs"
    _write_arm_outputs(decisions, search_root)
    _write_arm_outputs(decisions, report_search_root)
    _write_csv(output_root / "phase3cp_real_cm_arm_execution_plan.csv", scaled_plan)
    _write_csv(report_root / "phase3cp_real_cm_arm_execution_plan.csv", scaled_plan)
    _write_csv(output_root / "phase3cp_real_cm_generation_attempts.csv", generation_attempt_rows)
    _write_csv(report_root / "phase3cp_real_cm_generation_attempts.csv", generation_attempt_rows)
    _write_csv(output_root / "phase3cp_real_cm_all_generated_top_decisions.csv", decisions)
    _write_csv(report_root / "phase3cp_real_cm_all_generated_top_decisions.csv", decisions)
    if shortfall_rows:
        _write_csv(output_root / "phase3cp_real_cm_generation_shortfall_fill.csv", shortfall_rows)
        _write_csv(report_root / "phase3cp_real_cm_generation_shortfall_fill.csv", shortfall_rows)
    return decisions, scaled_plan


def _available_fields(shard_root: Path, max_shards: int) -> tuple[set[str], list[dict[str, Any]]]:
    panels = _discover_panels(shard_root, max_shards)
    metas: list[dict[str, Any]] = []
    intersection: set[str] | None = None
    for panel in panels:
        fields = set(pq.ParquetFile(panel).schema_arrow.names)
        intersection = fields if intersection is None else intersection & fields
        metas.append(
            {
                "panel": str(panel),
                "column_count": len(fields),
                "has_trade_time": "trade_time" in fields,
                "has_close": "close" in fields,
                "has_volume": "volume" in fields,
                "has_amount": "amount" in fields,
            }
        )
    return intersection or set(), metas


def _filter_cm_feasible_candidates(
    *,
    ca_table: Path,
    shard_root: Path,
    max_shards: int,
    limit: int,
    selection_mode: str,
    pre_cm_turnover_proxy_max: float,
    output_root: Path,
    report_root: Path,
) -> tuple[Path, dict[str, Any]]:
    available, panel_meta = _available_fields(shard_root, max_shards)
    passed: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in _read_csv(ca_table):
        expression = str(row.get("expression") or "")
        needed = set(_fields(expression))
        missing = sorted(needed - available)
        item = dict(row)
        item["cm_required_fields"] = "|".join(sorted(needed))
        if missing:
            item["cm_field_gate_decision"] = "REJECT_MISSING_FIELDS"
            item["cm_missing_fields"] = "|".join(missing)
            rejected.append(item)
            continue
        turnover_proxy = safe_float(item.get("mean_one_way_turnover"), float("nan"))
        turnover_cap = safe_float(pre_cm_turnover_proxy_max, float("nan"))
        if math.isfinite(turnover_cap) and math.isfinite(turnover_proxy) and turnover_proxy > turnover_cap:
            item["cm_field_gate_decision"] = "REJECT_TURNOVER_PROXY"
            item["cm_missing_fields"] = ""
            item["cm_turnover_proxy"] = turnover_proxy
            item["cm_turnover_proxy_max"] = turnover_cap
            rejected.append(item)
            continue
        item["cm_field_gate_decision"] = "PASS"
        item["cm_missing_fields"] = ""
        item["cm_turnover_proxy"] = turnover_proxy if math.isfinite(turnover_proxy) else ""
        item["cm_turnover_proxy_max"] = turnover_cap if math.isfinite(turnover_cap) else ""
        passed.append(item)

    if selection_mode == "arm_balanced":
        by_arm: dict[str, list[dict[str, Any]]] = {}
        for row in passed:
            by_arm.setdefault(str(row.get("generator_arm") or "unknown_arm"), []).append(row)
        arm_order = [
            "turnover_aware_fresh",
            "low_turnover_repair",
            "rx_ucb_fresh",
            "typed_ast_fresh",
            "challenger_repair",
            "event_state",
            "cem_exploit",
            "random_orthogonal",
            "unknown_arm",
        ]
        kept = []
        while len(kept) < limit:
            changed = False
            for arm in arm_order:
                bucket = by_arm.get(arm) or []
                if bucket and len(kept) < limit:
                    kept.append(bucket.pop(0))
                    changed = True
            if not changed:
                break
    else:
        kept = passed[:limit]
    pass_over_limit = max(0, len(passed) - len(kept))

    table = output_root / "phase3cp_real_cm_candidate_audit.csv"
    _write_csv(table, kept)
    _write_csv(output_root / "phase3cp_real_cm_field_gate_rejects.csv", rejected)
    _write_csv(report_root / "phase3cp_real_cm_candidate_audit.csv", kept)
    _write_csv(report_root / "phase3cp_real_cm_field_gate_rejects.csv", rejected)
    summary = {
        "candidate_count": len(kept),
        "rejected_missing_field_count": len(rejected),
        "rejected_turnover_proxy_count": sum(1 for row in rejected if row.get("cm_field_gate_decision") == "REJECT_TURNOVER_PROXY"),
        "passed_total_count": len(passed),
        "passed_over_limit_count": pass_over_limit,
        "selection_mode": selection_mode,
        "pre_cm_turnover_proxy_max": pre_cm_turnover_proxy_max,
        "available_field_count": len(available),
        "shard_root": str(shard_root),
        "max_shards_schema_checked": max_shards,
        "panel_meta": panel_meta,
        "missing_field_examples": [
            {
                "candidate_id": row.get("candidate_id"),
                "expression_hash": row.get("expression_hash"),
                "cm_missing_fields": row.get("cm_missing_fields"),
            }
            for row in rejected[:10]
        ],
    }
    _write_json(output_root / "phase3cp_real_cm_field_gate_summary.json", summary)
    _write_json(report_root / "phase3cp_real_cm_field_gate_summary.json", summary)
    if not kept:
        raise RuntimeError("no CA candidates passed the true1min CM field-availability gate")
    return table, summary


def _run_pre_cm_semantic_viability_gate(
    *,
    args: argparse.Namespace,
    candidate_table: Path,
    output_root: Path,
    report_root: Path,
    final_limit: int,
) -> tuple[Path, dict[str, Any]]:
    candidates = _read_csv(candidate_table)
    gate_output_root = output_root / "phase3cp_pre_cm_semantic_viability_gate"
    gate_report_root = report_root / "phase3cp_pre_cm_semantic_viability_gate"
    gate_output_root.mkdir(parents=True, exist_ok=True)
    gate_report_root.mkdir(parents=True, exist_ok=True)

    if not bool(getattr(args, "pre_cm_semantic_gate", True)):
        table = output_root / "phase3cp_real_cm_candidate_audit_semantic.csv"
        _write_csv(table, candidates[:final_limit])
        _write_csv(report_root / "phase3cp_real_cm_candidate_audit_semantic.csv", candidates[:final_limit])
        summary = {
            "enabled": False,
            "input_candidate_count": len(candidates),
            "passed_candidate_count": min(len(candidates), final_limit),
            "rejected_candidate_count": 0,
            "final_limit": int(final_limit),
            "decision": "PRE_CM_SEMANTIC_GATE_DISABLED",
        }
        _write_json(output_root / "phase3cp_pre_cm_semantic_viability_summary.json", summary)
        _write_json(report_root / "phase3cp_pre_cm_semantic_viability_summary.json", summary)
        return table, summary

    if not candidates:
        raise RuntimeError("pre-CM semantic viability gate received no candidates")

    gate_input = gate_output_root / "phase3cp_pre_cm_semantic_viability_input.csv"
    _write_csv(gate_input, candidates)
    max_shards = max(1, min(int(args.cm_max_shards), int(args.pre_cm_semantic_max_shards)))
    sample_times = max(1, int(args.pre_cm_semantic_sample_trade_times_per_shard))
    event_sample_times = int(args.pre_cm_semantic_event_sample_trade_times_per_shard)
    if event_sample_times <= 0:
        event_sample_times = sample_times

    argv = [
        "--candidate-audit",
        str(gate_input),
        "--shard-root",
        str(_resolve(args.shard_root)),
        "--output-root",
        str(gate_output_root),
        "--report-root",
        str(gate_report_root),
        "--candidate-limit",
        str(len(candidates)),
        "--max-shards",
        str(max_shards),
        "--sample-trade-times-per-shard",
        str(sample_times),
        "--event-aware-sample-times" if bool(args.cm_event_aware_sample_times) else "--no-event-aware-sample-times",
        "--event-sample-trade-times-per-shard",
        str(event_sample_times),
        "--horizons",
        str(args.pre_cm_semantic_horizons or args.cm_horizons),
        "--train-fraction",
        str(args.cm_train_fraction),
        "--validation-fraction",
        str(args.cm_validation_fraction),
        "--min-obs-per-time",
        str(args.cm_min_obs_per_time),
        "--cost-bps",
        str(args.cm_cost_bps),
        "--top-quantile",
        str(args.cm_top_quantile),
        "--rank-ic-loss-weight",
        str(args.cm_rank_ic_loss_weight),
        "--rank-ic-component-cap",
        str(args.cm_rank_ic_component_cap),
        "--regime-stability-weight",
        "0.0",
        "--regime-component-cap",
        str(args.cm_regime_component_cap),
        "--operator-cache-max-entries",
        str(min(int(args.cm_operator_cache_max_entries), int(args.pre_cm_semantic_operator_cache_max_entries))),
        "--feature-matrix-cache-max-windows",
        str(min(int(args.cm_feature_matrix_cache_max_windows), int(args.pre_cm_semantic_feature_matrix_cache_max_windows))),
        "--numexpr-threads",
        str(args.numexpr_threads),
        "--fast-mode",
    ]
    result = phase3cm_main(argv)
    if int(result or 0) != 0:
        raise RuntimeError(f"pre-CM semantic viability gate failed with exit code {result}")

    progress_rows = _read_csv(gate_output_root / "phase3cm_candidate_progress.csv")
    progress_by_id: dict[str, dict[str, Any]] = {}
    for row in progress_rows:
        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id:
            continue
        item = progress_by_id.setdefault(
            candidate_id,
            {
                "semantic_total_rows": 0,
                "semantic_nonzero_shards": 0,
                "semantic_checked_shards": 0,
            },
        )
        rows_added = int(float(row.get("rows_added") or 0))
        item["semantic_total_rows"] += rows_added
        item["semantic_checked_shards"] += 1
        if rows_added > 0:
            item["semantic_nonzero_shards"] += 1

    min_rows = max(0, int(args.pre_cm_semantic_min_rows_total))
    min_nonzero_shards = max(0, int(args.pre_cm_semantic_min_nonzero_shards))
    passed: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    viability_rows: list[dict[str, Any]] = []

    for row in candidates:
        candidate_id = str(row.get("candidate_id") or "")
        stats = progress_by_id.get(
            candidate_id,
            {"semantic_total_rows": 0, "semantic_nonzero_shards": 0, "semantic_checked_shards": 0},
        )
        item = dict(row)
        item.update(stats)
        reasons: list[str] = []
        if int(stats["semantic_total_rows"]) < min_rows:
            reasons.append("semantic_total_rows_below_min")
        if int(stats["semantic_nonzero_shards"]) < min_nonzero_shards:
            reasons.append("semantic_nonzero_shards_below_min")
        item["pre_cm_semantic_decision"] = "REJECT_WEAK_SEMANTIC_VIABILITY" if reasons else "PASS"
        item["pre_cm_semantic_reasons"] = "|".join(reasons)
        item["pre_cm_semantic_gate"] = "true"
        viability_rows.append(item)
        if reasons:
            item["memory_block_reason"] = "blocked_weak_semantic_viability"
            rejected.append(item)
        else:
            passed.append(item)

    kept = passed[: int(final_limit)]
    final_table = output_root / "phase3cp_real_cm_candidate_audit_semantic.csv"
    _write_csv(final_table, kept)
    _write_csv(output_root / "phase3cp_pre_cm_semantic_viability.csv", viability_rows)
    _write_csv(output_root / "phase3cp_semantic_blocked_candidate_audit.csv", rejected)
    _write_csv(report_root / "phase3cp_real_cm_candidate_audit_semantic.csv", kept)
    _write_csv(report_root / "phase3cp_pre_cm_semantic_viability.csv", viability_rows)
    _write_csv(report_root / "phase3cp_semantic_blocked_candidate_audit.csv", rejected)
    semantic_block_memory_path = None
    if bool(getattr(args, "write_semantic_block_memory", True)):
        semantic_block_memory_path = _write_semantic_block_memory(
            args.semantic_block_memory_root,
            rejected,
            run_label=output_root.name,
        )
    _copy_report_files(gate_output_root, gate_report_root)

    by_arm: dict[str, dict[str, int]] = {}
    for item in viability_rows:
        arm = str(item.get("generator_arm") or "unknown_arm")
        arm_row = by_arm.setdefault(arm, {"input": 0, "passed": 0, "rejected": 0})
        arm_row["input"] += 1
        if item["pre_cm_semantic_decision"] == "PASS":
            arm_row["passed"] += 1
        else:
            arm_row["rejected"] += 1
    by_arm_rows = [{"generator_arm": arm, **values} for arm, values in sorted(by_arm.items())]
    _write_csv(output_root / "phase3cp_pre_cm_semantic_viability_by_arm.csv", by_arm_rows)
    _write_csv(report_root / "phase3cp_pre_cm_semantic_viability_by_arm.csv", by_arm_rows)

    gate_summary_path = gate_output_root / "phase3cm_train_reward_audit_summary.json"
    gate_summary = json.loads(gate_summary_path.read_text(encoding="utf-8")) if gate_summary_path.exists() else {}
    summary = {
        "enabled": True,
        "decision": "PRE_CM_SEMANTIC_GATE_READY",
        "input_candidate_count": len(candidates),
        "passed_candidate_count": len(passed),
        "rejected_candidate_count": len(rejected),
        "kept_candidate_count": len(kept),
        "final_limit": int(final_limit),
        "min_rows_total": min_rows,
        "min_nonzero_shards": min_nonzero_shards,
        "max_shards": max_shards,
        "sample_trade_times_per_shard": sample_times,
        "event_sample_trade_times_per_shard": event_sample_times,
        "horizons": str(args.pre_cm_semantic_horizons or args.cm_horizons),
        "by_arm": by_arm_rows,
        "gate_cm_summary": {
            "candidate_count": gate_summary.get("candidate_count"),
            "reward_atom_rows_merged": gate_summary.get("reward_atom_rows_merged"),
            "parallel_axis": gate_summary.get("parallel_axis"),
            "fast_mode": gate_summary.get("fast_mode"),
        },
        "semantic_block_memory_path": str(semantic_block_memory_path) if semantic_block_memory_path else None,
        "metric_boundary": "pre-CM semantic viability uses real CM evaluator rows only; it must not use reward to optimize or promote candidates",
    }
    _write_json(output_root / "phase3cp_pre_cm_semantic_viability_summary.json", summary)
    _write_json(report_root / "phase3cp_pre_cm_semantic_viability_summary.json", summary)
    if not kept:
        raise RuntimeError("pre-CM semantic viability gate rejected all candidates")
    return final_table, summary


def _audit_cm_lineage_consistency(
    *,
    candidate_table: Path,
    cm_table: Path,
    output_root: Path,
    report_root: Path,
) -> dict[str, Any]:
    input_by_hash = {str(row.get("expression_hash") or ""): row for row in _read_csv(candidate_table)}
    rows: list[dict[str, Any]] = []
    mismatch_count = 0
    for row in _read_csv(cm_table):
        digest = str(row.get("expression_hash") or "")
        source = input_by_hash.get(digest, {})
        expected_arm = str(source.get("generator_arm") or "")
        actual_arm = str(row.get("generator_arm") or "")
        expected_candidate_id = str(source.get("candidate_id") or "")
        actual_candidate_id = str(row.get("candidate_id") or "")
        ok = bool(expected_arm and expected_arm == actual_arm and expected_candidate_id == actual_candidate_id)
        if not ok:
            mismatch_count += 1
        rows.append(
            {
                "expression_hash": digest,
                "expected_candidate_id": expected_candidate_id,
                "actual_candidate_id": actual_candidate_id,
                "expected_generator_arm": expected_arm,
                "actual_generator_arm": actual_arm,
                "lineage_consistent": str(ok).lower(),
            }
        )
    _write_csv(output_root / "phase3cp_real_cm_lineage_consistency.csv", rows)
    _write_csv(report_root / "phase3cp_real_cm_lineage_consistency.csv", rows)
    summary = {
        "checked_count": len(rows),
        "mismatch_count": mismatch_count,
        "lineage_consistent": mismatch_count == 0 and bool(rows),
    }
    _write_json(output_root / "phase3cp_real_cm_lineage_consistency_summary.json", summary)
    _write_json(report_root / "phase3cp_real_cm_lineage_consistency_summary.json", summary)
    return summary


def _write_minimal_parallel_cm_md(summary: dict[str, Any], reward_rows: list[dict[str, Any]], report_root: Path) -> None:
    ranked = sorted(reward_rows, key=lambda row: safe_float(row.get("train_reward"), -999.0), reverse=True)
    lines = [
        "# Phase3CM Parallel Train Portfolio Sortino Reward Audit",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Summary",
        "",
        f"- candidates: `{summary['candidate_count']}`",
        f"- followup-ready: `{summary['followup_count']}`",
        f"- parallel workers: `{summary['acceleration_contract']['parallel_workers']}`",
        f"- chunk count: `{summary['parallel_chunk_count']}`",
        "",
        "## Top Rows",
        "",
        "| rank | candidate | reward | train day sortino | train rank IC loss | rank IC component | validation day sortino | turnover | decision | blockers |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for idx, row in enumerate(ranked[:30], 1):
        lines.append(
            f"| {idx} | `{row.get('candidate_id')}` | {row.get('train_reward')} | {row.get('train_day_sortino')} | "
            f"{row.get('train_rank_ic_loss')} | {row.get('train_rank_ic_reward_component')} | "
            f"{row.get('validation_day_sortino')} | {row.get('train_mean_one_way_turnover')} | "
            f"`{row.get('train_reward_decision')}` | `{row.get('train_reward_blockers')}` |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            f"- This is the same Phase3CM train composite reward audit executed by `{summary.get('parallel_axis', 'candidate')}` chunks.",
            "- `rank_ic_loss` is included only through the bounded train-side reward component.",
            "- Holdout remains report-only and must not feed search.",
            "- X0/R3 remain read-only.",
        ]
    )
    (report_root / "PHASE3CM_PARALLEL_TRAIN_PORTFOLIO_SORTINO_REWARD_AUDIT.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def _append_cm_persistent_cache_args(argv: list[str], args: argparse.Namespace) -> None:
    root = getattr(args, "cm_persistent_cache_root", None)
    if root is None:
        return
    argv.extend(
        [
            "--persistent-cache-root",
            str(root),
            "--persistent-cache-mode",
            str(getattr(args, "cm_persistent_cache_mode", "readwrite")),
        ]
    )
    if bool(getattr(args, "cm_disable_persistent_expression_cache", False)):
        argv.append("--disable-persistent-expression-cache")
    if bool(getattr(args, "cm_disable_persistent_operator_cache", False)):
        argv.append("--disable-persistent-operator-cache")
    if bool(getattr(args, "cm_disable_persistent_feature_matrix_cache", False)):
        argv.append("--disable-persistent-feature-matrix-cache")


def _run_real_cm_chunk_subprocess(
    *,
    args: argparse.Namespace,
    chunk_table: Path,
    chunk_output_root: Path,
    chunk_report_root: Path,
    chunk_limit: int,
) -> dict[str, Any]:
    argv = [
        sys.executable,
        "-m",
        "our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit",
        "--candidate-audit",
        str(chunk_table),
        "--shard-root",
        str(_resolve(args.shard_root)),
        "--output-root",
        str(chunk_output_root),
        "--report-root",
        str(chunk_report_root),
        "--candidate-limit",
        str(chunk_limit),
        "--max-shards",
        str(args.cm_max_shards),
        "--sample-trade-times-per-shard",
        str(args.cm_sample_trade_times_per_shard),
        "--event-aware-sample-times" if bool(args.cm_event_aware_sample_times) else "--no-event-aware-sample-times",
        "--event-sample-trade-times-per-shard",
        str(args.cm_event_sample_trade_times_per_shard),
        "--horizons",
        str(args.cm_horizons),
        "--train-fraction",
        str(args.cm_train_fraction),
        "--validation-fraction",
        str(args.cm_validation_fraction),
        "--min-obs-per-time",
        str(args.cm_min_obs_per_time),
        "--cost-bps",
        str(args.cm_cost_bps),
        "--top-quantile",
        str(args.cm_top_quantile),
        "--rank-ic-loss-weight",
        str(args.cm_rank_ic_loss_weight),
        "--rank-ic-component-cap",
        str(args.cm_rank_ic_component_cap),
        "--regime-stability-weight",
        str(args.cm_regime_stability_weight),
        "--regime-component-cap",
        str(args.cm_regime_component_cap),
        "--operator-cache-max-entries",
        str(args.cm_operator_cache_max_entries),
        "--feature-matrix-cache-max-windows",
        str(args.cm_feature_matrix_cache_max_windows),
        "--numexpr-threads",
        str(args.numexpr_threads),
        "--fast-mode",
    ]
    _append_cm_persistent_cache_args(argv, args)
    env = os.environ.copy()
    env.setdefault("PYTHONPATH", "src")
    proc = subprocess.run(
        argv,
        cwd=str(REPO),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    (chunk_output_root / "phase3cm_subprocess_stdout.log").write_text(proc.stdout or "", encoding="utf-8")
    (chunk_output_root / "phase3cm_subprocess_stderr.log").write_text(proc.stderr or "", encoding="utf-8")
    if proc.returncode != 0:
        partial_path = chunk_output_root / "phase3cm_train_reward_partial.csv"
        partial_rows = _read_csv(partial_path)
        if partial_rows:
            return {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "experiment_id": "20260623_phase3cm_train_portfolio_sortino_reward_audit",
                "decision": "PHASE3CM_CHUNK_PARTIAL_RECOVERED_AFTER_FAILURE",
                "candidate_count": len(partial_rows),
                "followup_count": sum(
                    1
                    for row in partial_rows
                    if row.get("train_reward_decision") == "TRAIN_REWARD_FOLLOWUP_READY"
                ),
                "partial": True,
                "subprocess_returncode": proc.returncode,
                "chunk_table": str(chunk_table),
                "error_log": str(chunk_output_root / "phase3cm_subprocess_stderr.log"),
                "metric_boundary": "partial chunk recovered; rerun missing candidates before promotion decisions",
            }
        raise RuntimeError(f"Phase3CM chunk failed rc={proc.returncode}: {chunk_table}")
    summary_path = chunk_output_root / "phase3cm_train_reward_audit_summary.json"
    return json.loads(summary_path.read_text(encoding="utf-8"))


def _run_real_cm_shard_subprocess(
    *,
    args: argparse.Namespace,
    candidate_table: Path,
    shard_indices: list[int],
    shard_output_root: Path,
    shard_report_root: Path,
    candidate_limit: int,
) -> dict[str, Any]:
    argv = [
        sys.executable,
        "-m",
        "our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit",
        "--candidate-audit",
        str(candidate_table),
        "--shard-root",
        str(_resolve(args.shard_root)),
        "--output-root",
        str(shard_output_root),
        "--report-root",
        str(shard_report_root),
        "--candidate-limit",
        str(candidate_limit),
        "--max-shards",
        str(args.cm_max_shards),
        "--shard-indices",
        ",".join(str(item) for item in shard_indices),
        "--sample-trade-times-per-shard",
        str(args.cm_sample_trade_times_per_shard),
        "--event-aware-sample-times" if bool(args.cm_event_aware_sample_times) else "--no-event-aware-sample-times",
        "--event-sample-trade-times-per-shard",
        str(args.cm_event_sample_trade_times_per_shard),
        "--horizons",
        str(args.cm_horizons),
        "--train-fraction",
        str(args.cm_train_fraction),
        "--validation-fraction",
        str(args.cm_validation_fraction),
        "--min-obs-per-time",
        str(args.cm_min_obs_per_time),
        "--cost-bps",
        str(args.cm_cost_bps),
        "--top-quantile",
        str(args.cm_top_quantile),
        "--rank-ic-loss-weight",
        str(args.cm_rank_ic_loss_weight),
        "--rank-ic-component-cap",
        str(args.cm_rank_ic_component_cap),
        "--regime-stability-weight",
        str(args.cm_regime_stability_weight),
        "--regime-component-cap",
        str(args.cm_regime_component_cap),
        "--operator-cache-max-entries",
        str(args.cm_operator_cache_max_entries),
        "--feature-matrix-cache-max-windows",
        str(args.cm_feature_matrix_cache_max_windows),
        "--numexpr-threads",
        str(args.numexpr_threads),
        "--fast-mode",
        "--write-reward-atoms",
        "--disable-schema-gate",
    ]
    _append_cm_persistent_cache_args(argv, args)
    env = os.environ.copy()
    env.setdefault("PYTHONPATH", "src")
    proc = subprocess.run(
        argv,
        cwd=str(REPO),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    (shard_output_root / "phase3cm_subprocess_stdout.log").write_text(proc.stdout or "", encoding="utf-8")
    (shard_output_root / "phase3cm_subprocess_stderr.log").write_text(proc.stderr or "", encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(f"Phase3CM shard chunk failed rc={proc.returncode}: shard_indices={shard_indices}")
    summary_path = shard_output_root / "phase3cm_train_reward_audit_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["parallel_shard_indices"] = ",".join(str(item) for item in shard_indices)
    return summary


def _run_real_cm_retry_table(
    *,
    args: argparse.Namespace,
    retry_table: Path,
    retry_output_root: Path,
    retry_report_root: Path,
    retry_limit: int,
) -> dict[str, Any]:
    retry_output_root.mkdir(parents=True, exist_ok=True)
    retry_report_root.mkdir(parents=True, exist_ok=True)
    argv = [
        "--candidate-audit",
        str(retry_table),
        "--shard-root",
        str(_resolve(args.shard_root)),
        "--output-root",
        str(retry_output_root),
        "--report-root",
        str(retry_report_root),
        "--candidate-limit",
        str(retry_limit),
        "--max-shards",
        str(args.cm_max_shards),
        "--sample-trade-times-per-shard",
        str(args.cm_sample_trade_times_per_shard),
        "--event-aware-sample-times" if bool(args.cm_event_aware_sample_times) else "--no-event-aware-sample-times",
        "--event-sample-trade-times-per-shard",
        str(args.cm_event_sample_trade_times_per_shard),
        "--horizons",
        str(args.cm_horizons),
        "--train-fraction",
        str(args.cm_train_fraction),
        "--validation-fraction",
        str(args.cm_validation_fraction),
        "--min-obs-per-time",
        str(args.cm_min_obs_per_time),
        "--cost-bps",
        str(args.cm_cost_bps),
        "--top-quantile",
        str(args.cm_top_quantile),
        "--rank-ic-loss-weight",
        str(args.cm_rank_ic_loss_weight),
        "--rank-ic-component-cap",
        str(args.cm_rank_ic_component_cap),
        "--regime-stability-weight",
        str(args.cm_regime_stability_weight),
        "--regime-component-cap",
        str(args.cm_regime_component_cap),
        "--operator-cache-max-entries",
        str(args.cm_operator_cache_max_entries),
        "--feature-matrix-cache-max-windows",
        str(args.cm_feature_matrix_cache_max_windows),
        "--numexpr-threads",
        str(args.numexpr_threads),
        "--fast-mode",
    ]
    _append_cm_persistent_cache_args(argv, args)
    result = phase3cm_main(argv)
    if int(result or 0) != 0:
        raise RuntimeError(f"Phase3CM retry failed with exit code {result}")
    return json.loads((retry_output_root / "phase3cm_train_reward_audit_summary.json").read_text(encoding="utf-8"))


def _run_real_cm_parallel(args: argparse.Namespace, candidate_table: Path, output_root: Path, report_root: Path) -> dict[str, Any]:
    cm_output_root = output_root / "phase3cm_train_reward"
    cm_report_root = report_root / "phase3cm_train_reward"
    cm_output_root.mkdir(parents=True, exist_ok=True)
    cm_report_root.mkdir(parents=True, exist_ok=True)

    candidates = _read_csv(candidate_table)
    workers = max(1, min(int(args.cm_workers), len(candidates)))
    if workers <= 1:
        return _run_real_cm_serial(args, candidate_table, output_root, report_root)

    chunk_root = output_root / "phase3cm_train_reward_chunks"
    chunk_report_root = report_root / "phase3cm_train_reward_chunks"
    chunk_root.mkdir(parents=True, exist_ok=True)
    chunk_report_root.mkdir(parents=True, exist_ok=True)

    chunks: list[tuple[int, Path, Path, Path, int]] = []
    for worker_idx in range(workers):
        rows = candidates[worker_idx::workers]
        if not rows:
            continue
        chunk_table = chunk_root / f"candidate_chunk_{worker_idx + 1:02d}.csv"
        chunk_out = chunk_root / f"chunk_{worker_idx + 1:02d}"
        chunk_rep = chunk_report_root / f"chunk_{worker_idx + 1:02d}"
        chunk_out.mkdir(parents=True, exist_ok=True)
        chunk_rep.mkdir(parents=True, exist_ok=True)
        _write_csv(chunk_table, rows)
        chunks.append((worker_idx + 1, chunk_table, chunk_out, chunk_rep, len(rows)))

    chunk_summaries: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        futures = {
            executor.submit(
                _run_real_cm_chunk_subprocess,
                args=args,
                chunk_table=chunk_table,
                chunk_output_root=chunk_out,
                chunk_report_root=chunk_rep,
                chunk_limit=count,
            ): (chunk_id, chunk_table, chunk_out, count)
            for chunk_id, chunk_table, chunk_out, chunk_rep, count in chunks
        }
        for future in as_completed(futures):
            chunk_id, chunk_table, chunk_out, count = futures[future]
            summary = future.result()
            summary["parallel_chunk_id"] = chunk_id
            summary["parallel_chunk_candidate_count"] = count
            summary["parallel_chunk_table"] = str(chunk_table)
            summary["parallel_chunk_output_root"] = str(chunk_out)
            chunk_summaries.append(summary)

    reward_rows: list[dict[str, Any]] = []
    split_horizon_rows: list[dict[str, Any]] = []
    shard_meta_rows: list[dict[str, Any]] = []
    progress_rows: list[dict[str, Any]] = []
    partial_chunk_ids = {
        int(row.get("parallel_chunk_id"))
        for row in chunk_summaries
        if row.get("parallel_chunk_id") and bool(row.get("partial"))
    }
    for chunk_id, _, chunk_out, _, _ in chunks:
        if int(chunk_id) in partial_chunk_ids:
            continue
        chunk_reward_path = chunk_out / "phase3cm_train_reward.csv"
        if not chunk_reward_path.exists():
            chunk_reward_path = chunk_out / "phase3cm_train_reward_partial.csv"
        for row in _read_csv(chunk_reward_path):
            row["parallel_chunk_id"] = chunk_id
            reward_rows.append(row)
        for row in _read_csv(chunk_out / "phase3cm_candidate_split_horizon_summary.csv"):
            row["parallel_chunk_id"] = chunk_id
            split_horizon_rows.append(row)
        for row in _read_csv(chunk_out / "phase3cm_shard_meta.csv"):
            row["parallel_chunk_id"] = chunk_id
            shard_meta_rows.append(row)
        for row in _read_csv(chunk_out / "phase3cm_candidate_progress.csv"):
            row["parallel_chunk_id"] = chunk_id
            progress_rows.append(row)

    expected_by_id = {str(row.get("candidate_id") or ""): row for row in candidates if str(row.get("candidate_id") or "")}
    recovered_ids = {str(row.get("candidate_id") or "") for row in reward_rows if str(row.get("candidate_id") or "")}
    missing_rows = [row for cid, row in expected_by_id.items() if cid not in recovered_ids]
    retry_summary: dict[str, Any] = {
        "missing_candidate_count_before_retry": len(missing_rows),
        "retry_candidate_count": 0,
        "retry_success_count": 0,
        "missing_candidate_count_after_retry": len(missing_rows),
    }
    if missing_rows:
        retry_root = output_root / "phase3cm_train_reward_retry_missing"
        retry_report_root = report_root / "phase3cm_train_reward_retry_missing"
        retry_table = retry_root / "candidate_retry_missing.csv"
        _write_csv(retry_table, missing_rows)
        retry_summary_raw = _run_real_cm_retry_table(
            args=args,
            retry_table=retry_table,
            retry_output_root=retry_root,
            retry_report_root=retry_report_root,
            retry_limit=len(missing_rows),
        )
        retry_reward_rows = _read_csv(retry_root / "phase3cm_train_reward.csv")
        for row in retry_reward_rows:
            row["parallel_chunk_id"] = "retry_missing"
            reward_rows.append(row)
        for row in _read_csv(retry_root / "phase3cm_candidate_split_horizon_summary.csv"):
            row["parallel_chunk_id"] = "retry_missing"
            split_horizon_rows.append(row)
        for row in _read_csv(retry_root / "phase3cm_shard_meta.csv"):
            row["parallel_chunk_id"] = "retry_missing"
            shard_meta_rows.append(row)
        for row in _read_csv(retry_root / "phase3cm_candidate_progress.csv"):
            row["parallel_chunk_id"] = "retry_missing"
            progress_rows.append(row)
        recovered_ids = {str(row.get("candidate_id") or "") for row in reward_rows if str(row.get("candidate_id") or "")}
        retry_summary = {
            "missing_candidate_count_before_retry": len(missing_rows),
            "retry_candidate_count": int(retry_summary_raw.get("candidate_count") or len(retry_reward_rows)),
            "retry_success_count": len(retry_reward_rows),
            "missing_candidate_count_after_retry": sum(1 for cid in expected_by_id if cid not in recovered_ids),
            "retry_summary": retry_summary_raw,
        }

    reward_rows.sort(key=lambda row: safe_float(row.get("train_reward"), -999.0), reverse=True)
    followup_count = sum(1 for row in reward_rows if row.get("train_reward_decision") == "TRAIN_REWARD_FOLLOWUP_READY")
    partial_chunk_count = sum(1 for row in chunk_summaries if bool(row.get("partial")))
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cm_train_portfolio_sortino_reward_audit",
        "decision": "PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY",
        "candidate_count": len(reward_rows),
        "followup_count": followup_count,
        "input_candidate_audit": str(candidate_table),
        "shard_root": str(_resolve(args.shard_root)),
        "max_shards": args.cm_max_shards,
        "sample_trade_times_per_shard": args.cm_sample_trade_times_per_shard,
        "horizons": [int(item.strip()) for item in str(args.cm_horizons).split(",") if item.strip()],
        "train_fraction": args.cm_train_fraction,
        "validation_fraction": args.cm_validation_fraction,
        "holdout_fraction": round(1.0 - args.cm_train_fraction - args.cm_validation_fraction, 8),
        "cost_bps": args.cm_cost_bps,
        "top_quantile": args.cm_top_quantile,
        "rank_ic_loss_weight": args.cm_rank_ic_loss_weight,
        "rank_ic_component_cap": args.cm_rank_ic_component_cap,
        "regime_stability_weight": args.cm_regime_stability_weight,
        "regime_component_cap": args.cm_regime_component_cap,
        "optimizer_reward_metric": OPTIMIZER_REWARD_METRIC,
        "portfolio_pnl_rows_written": 0,
        "metric_boundary": "parallel train portfolio Sortino + rankIC loss reward audit; not production proof; validation/holdout must not feed search",
        "fast_mode": True,
        "numexpr_threads": int(args.numexpr_threads),
        "incremental_checkpoints_enabled": True,
        "checkpoint_every_candidates": 8,
        "drop_hard_blocked_input": False,
        "python_executable": sys.executable,
        "package_versions": chunk_summaries[0].get("package_versions", {}) if chunk_summaries else {},
        "parallel_chunk_count": len(chunks),
        "partial_chunk_count": partial_chunk_count,
        "parallel_chunk_summaries": sorted(chunk_summaries, key=lambda row: int(row.get("parallel_chunk_id") or 0)),
        "missing_retry_summary": retry_summary,
        "acceleration_contract": {
            "batched_shard_read": True,
            "column_pruned_pyarrow_read": True,
            "expression_cache_scope": "per_worker_per_shard",
            "factor_expression_cache": True,
            "feature_matrix_cache": True,
            "operator_subtree_cache": True,
            "operator_cache_max_entries": int(args.cm_operator_cache_max_entries),
            "feature_matrix_cache_max_windows": int(args.cm_feature_matrix_cache_max_windows),
            "fast_group_rank": True,
            "omp_threads": os.environ.get("OMP_NUM_THREADS"),
            "mkl_threads": os.environ.get("MKL_NUM_THREADS"),
            "numexpr_max_threads": os.environ.get("NUMEXPR_MAX_THREADS"),
            "parallel_workers": len(chunks),
            "global_worker_limit": len(chunks),
        },
    }
    for root in (cm_output_root, cm_report_root):
        _write_csv(root / "phase3cm_candidate_train_reward_summary.csv", reward_rows)
        _write_csv(root / "phase3cm_train_reward.csv", reward_rows)
        _write_csv(root / "phase3cm_candidate_split_horizon_summary.csv", split_horizon_rows)
        _write_csv(root / "phase3cm_shard_meta.csv", shard_meta_rows)
        _write_csv(root / "phase3cm_candidate_progress.csv", progress_rows)
        _write_csv(root / "phase3cm_train_reward_partial.csv", reward_rows)
        _write_json(root / "phase3cm_train_reward_audit_summary.json", summary)
        _write_json(
            root / "phase3cm_incremental_checkpoint_summary.json",
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "experiment_id": "20260623_phase3cm_incremental_checkpoint",
                "partial": False,
                "final": True,
                "candidate_count": len(reward_rows),
                "shard_count": int(args.cm_max_shards),
                "processed_candidate_shards": len(progress_rows),
                "partial_reward_count": len(reward_rows),
                "progress_row_count": len(progress_rows),
                "completed_shards": int(args.cm_max_shards),
                "parallel_workers": len(chunks),
            },
        )
    _write_minimal_parallel_cm_md(summary, reward_rows, cm_report_root)
    return summary


def _run_real_cm_parallel_by_shard(args: argparse.Namespace, candidate_table: Path, output_root: Path, report_root: Path) -> dict[str, Any]:
    cm_output_root = output_root / "phase3cm_train_reward"
    cm_report_root = report_root / "phase3cm_train_reward"
    cm_output_root.mkdir(parents=True, exist_ok=True)
    cm_report_root.mkdir(parents=True, exist_ok=True)

    candidates = _read_csv(candidate_table)
    if int(args.cm_candidate_limit) > 0:
        candidates = candidates[: int(args.cm_candidate_limit)]
    panels = _discover_panels(_resolve(args.shard_root), args.cm_max_shards)
    shard_indices = list(range(len(panels)))
    workers = max(1, min(int(args.cm_workers), len(shard_indices)))
    if workers <= 1:
        return _run_real_cm_serial(args, candidate_table, output_root, report_root)

    chunk_root = output_root / "phase3cm_train_reward_shard_chunks"
    chunk_report_root = report_root / "phase3cm_train_reward_shard_chunks"
    chunk_root.mkdir(parents=True, exist_ok=True)
    chunk_report_root.mkdir(parents=True, exist_ok=True)

    chunks: list[tuple[int, list[int], Path, Path]] = []
    for worker_idx in range(workers):
        indices = shard_indices[worker_idx::workers]
        if not indices:
            continue
        chunk_out = chunk_root / f"shard_chunk_{worker_idx + 1:02d}"
        chunk_rep = chunk_report_root / f"shard_chunk_{worker_idx + 1:02d}"
        chunk_out.mkdir(parents=True, exist_ok=True)
        chunk_rep.mkdir(parents=True, exist_ok=True)
        chunks.append((worker_idx + 1, indices, chunk_out, chunk_rep))

    chunk_summaries: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        futures = {
            executor.submit(
                _run_real_cm_shard_subprocess,
                args=args,
                candidate_table=candidate_table,
                shard_indices=indices,
                shard_output_root=chunk_out,
                shard_report_root=chunk_rep,
                candidate_limit=len(candidates),
            ): (chunk_id, indices, chunk_out)
            for chunk_id, indices, chunk_out, chunk_rep in chunks
        }
        for future in as_completed(futures):
            chunk_id, indices, chunk_out = futures[future]
            summary = future.result()
            summary["parallel_chunk_id"] = chunk_id
            summary["parallel_shard_indices"] = ",".join(str(item) for item in indices)
            summary["parallel_chunk_output_root"] = str(chunk_out)
            chunk_summaries.append(summary)

    atom_rows: list[dict[str, Any]] = []
    shard_meta_rows: list[dict[str, Any]] = []
    progress_rows: list[dict[str, Any]] = []
    for chunk_id, indices, chunk_out, _ in chunks:
        for row in _read_csv(chunk_out / "phase3cm_reward_atoms.csv"):
            row["parallel_chunk_id"] = chunk_id
            row["parallel_shard_indices"] = ",".join(str(item) for item in indices)
            atom_rows.append(row)
        for row in _read_csv(chunk_out / "phase3cm_shard_meta.csv"):
            row["parallel_chunk_id"] = chunk_id
            row["parallel_shard_indices"] = ",".join(str(item) for item in indices)
            shard_meta_rows.append(row)
        for row in _read_csv(chunk_out / "phase3cm_candidate_progress.csv"):
            row["parallel_chunk_id"] = chunk_id
            row["parallel_shard_indices"] = ",".join(str(item) for item in indices)
            progress_rows.append(row)

    atom_rows_by_hash: dict[str, list[dict[str, Any]]] = {}
    for row in atom_rows:
        digest = str(row.get("expression_hash") or "")
        if digest:
            atom_rows_by_hash.setdefault(digest, []).append(row)

    horizons = tuple(int(item.strip()) for item in str(args.cm_horizons).split(",") if item.strip())
    reward_rows: list[dict[str, Any]] = []
    split_horizon_rows: list[dict[str, Any]] = []
    for idx, candidate in enumerate(candidates, 1):
        digest = str(candidate.get("expression_hash") or "")
        per_split, reward_row = _candidate_summary_from_reward_atoms(
            candidate,
            atom_rows_by_hash.get(digest, []),
            horizons,
            seed=20260623 + idx,
            rank_ic_loss_weight=args.cm_rank_ic_loss_weight,
            rank_ic_component_cap=args.cm_rank_ic_component_cap,
            regime_stability_weight=args.cm_regime_stability_weight,
            regime_component_cap=args.cm_regime_component_cap,
        )
        for row in per_split:
            split_horizon_rows.append(
                {
                    "candidate_id": candidate.get("candidate_id"),
                    "expression_hash": candidate.get("expression_hash"),
                    "generator_arm": candidate.get("generator_arm"),
                    "factor_lane": candidate.get("factor_lane"),
                    "expression": candidate.get("expression"),
                    **row,
                }
            )
        reward_rows.append(reward_row)

    reward_rows.sort(key=lambda row: safe_float(row.get("train_reward"), -999.0), reverse=True)
    followup_count = sum(1 for row in reward_rows if row.get("train_reward_decision") == "TRAIN_REWARD_FOLLOWUP_READY")
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cm_train_portfolio_sortino_reward_audit",
        "decision": "PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY",
        "candidate_count": len(reward_rows),
        "followup_count": followup_count,
        "input_candidate_audit": str(candidate_table),
        "shard_root": str(_resolve(args.shard_root)),
        "max_shards": args.cm_max_shards,
        "selected_shard_count": len(shard_indices),
        "sample_trade_times_per_shard": args.cm_sample_trade_times_per_shard,
        "horizons": [int(item) for item in horizons],
        "train_fraction": args.cm_train_fraction,
        "validation_fraction": args.cm_validation_fraction,
        "holdout_fraction": round(1.0 - args.cm_train_fraction - args.cm_validation_fraction, 8),
        "cost_bps": args.cm_cost_bps,
        "top_quantile": args.cm_top_quantile,
        "rank_ic_loss_weight": args.cm_rank_ic_loss_weight,
        "rank_ic_component_cap": args.cm_rank_ic_component_cap,
        "regime_stability_weight": args.cm_regime_stability_weight,
        "regime_component_cap": args.cm_regime_component_cap,
        "optimizer_reward_metric": OPTIMIZER_REWARD_METRIC,
        "portfolio_pnl_rows_written": 0,
        "reward_atom_rows_merged": len(atom_rows),
        "metric_boundary": "parallel shard-axis train portfolio Sortino + rankIC loss reward audit; not production proof; validation/holdout must not feed search",
        "fast_mode": True,
        "numexpr_threads": int(args.numexpr_threads),
        "incremental_checkpoints_enabled": True,
        "checkpoint_every_candidates": 8,
        "drop_hard_blocked_input": False,
        "python_executable": sys.executable,
        "package_versions": chunk_summaries[0].get("package_versions", {}) if chunk_summaries else {},
        "parallel_axis": "shard",
        "parallel_chunk_count": len(chunks),
        "partial_chunk_count": 0,
        "parallel_chunk_summaries": sorted(chunk_summaries, key=lambda row: int(row.get("parallel_chunk_id") or 0)),
        "missing_retry_summary": {
            "missing_candidate_count_before_retry": 0,
            "retry_candidate_count": 0,
            "retry_success_count": 0,
            "missing_candidate_count_after_retry": 0,
        },
        "acceleration_contract": {
            "batched_shard_read": True,
            "column_pruned_pyarrow_read": True,
            "expression_cache_scope": "per_worker_distinct_shards",
            "factor_expression_cache": True,
            "feature_matrix_cache": True,
            "operator_subtree_cache": True,
            "operator_cache_max_entries": int(args.cm_operator_cache_max_entries),
            "feature_matrix_cache_max_windows": int(args.cm_feature_matrix_cache_max_windows),
            "fast_group_rank": True,
            "omp_threads": os.environ.get("OMP_NUM_THREADS"),
            "mkl_threads": os.environ.get("MKL_NUM_THREADS"),
            "numexpr_max_threads": os.environ.get("NUMEXPR_MAX_THREADS"),
            "parallel_workers": len(chunks),
            "global_worker_limit": len(chunks),
            "parallel_axis": "shard",
            "duplicate_shard_reads_per_full_pass": 1,
            "event_aware_sample_times": bool(args.cm_event_aware_sample_times),
            "event_sample_trade_times_per_shard": int(args.cm_event_sample_trade_times_per_shard),
        },
    }
    for root in (cm_output_root, cm_report_root):
        _write_csv(root / "phase3cm_candidate_train_reward_summary.csv", reward_rows)
        _write_csv(root / "phase3cm_train_reward.csv", reward_rows)
        _write_csv(root / "phase3cm_candidate_split_horizon_summary.csv", split_horizon_rows)
        _write_csv(root / "phase3cm_shard_meta.csv", shard_meta_rows)
        _write_csv(root / "phase3cm_candidate_progress.csv", progress_rows)
        _write_csv(root / "phase3cm_reward_atoms.csv", atom_rows)
        _write_csv(root / "phase3cm_train_reward_partial.csv", reward_rows)
        _write_json(root / "phase3cm_train_reward_audit_summary.json", summary)
        _write_json(
            root / "phase3cm_incremental_checkpoint_summary.json",
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "experiment_id": "20260623_phase3cm_incremental_checkpoint",
                "partial": False,
                "final": True,
                "candidate_count": len(reward_rows),
                "shard_count": len(shard_indices),
                "processed_candidate_shards": len(progress_rows),
                "partial_reward_count": len(reward_rows),
                "progress_row_count": len(progress_rows),
                "completed_shards": len(shard_indices),
                "parallel_workers": len(chunks),
                "parallel_axis": "shard",
            },
        )
    _write_minimal_parallel_cm_md(summary, reward_rows, cm_report_root)
    return summary


def _run_real_cm_serial(args: argparse.Namespace, candidate_table: Path, output_root: Path, report_root: Path) -> dict[str, Any]:
    cm_output_root = output_root / "phase3cm_train_reward"
    cm_report_root = report_root / "phase3cm_train_reward"
    argv = [
        "--candidate-audit",
        str(candidate_table),
        "--shard-root",
        str(_resolve(args.shard_root)),
        "--output-root",
        str(cm_output_root),
        "--report-root",
        str(cm_report_root),
        "--candidate-limit",
        str(args.cm_candidate_limit),
        "--max-shards",
        str(args.cm_max_shards),
        "--sample-trade-times-per-shard",
        str(args.cm_sample_trade_times_per_shard),
        "--event-aware-sample-times" if bool(args.cm_event_aware_sample_times) else "--no-event-aware-sample-times",
        "--event-sample-trade-times-per-shard",
        str(args.cm_event_sample_trade_times_per_shard),
        "--horizons",
        str(args.cm_horizons),
        "--train-fraction",
        str(args.cm_train_fraction),
        "--validation-fraction",
        str(args.cm_validation_fraction),
        "--min-obs-per-time",
        str(args.cm_min_obs_per_time),
        "--cost-bps",
        str(args.cm_cost_bps),
        "--top-quantile",
        str(args.cm_top_quantile),
        "--rank-ic-loss-weight",
        str(args.cm_rank_ic_loss_weight),
        "--rank-ic-component-cap",
        str(args.cm_rank_ic_component_cap),
        "--regime-stability-weight",
        str(args.cm_regime_stability_weight),
        "--regime-component-cap",
        str(args.cm_regime_component_cap),
        "--operator-cache-max-entries",
        str(args.cm_operator_cache_max_entries),
        "--feature-matrix-cache-max-windows",
        str(args.cm_feature_matrix_cache_max_windows),
        "--numexpr-threads",
        str(args.numexpr_threads),
        "--fast-mode",
    ]
    _append_cm_persistent_cache_args(argv, args)
    result = phase3cm_main(argv)
    if int(result or 0) != 0:
        raise RuntimeError(f"Phase3CM audit failed with exit code {result}")
    return json.loads((cm_output_root / "phase3cm_train_reward_audit_summary.json").read_text(encoding="utf-8"))


def _run_real_cm(args: argparse.Namespace, candidate_table: Path, output_root: Path, report_root: Path) -> dict[str, Any]:
    if int(getattr(args, "cm_workers", 1) or 1) > 1:
        if str(getattr(args, "cm_parallel_axis", "candidate")) == "shard":
            return _run_real_cm_parallel_by_shard(args, candidate_table, output_root, report_root)
        return _run_real_cm_parallel(args, candidate_table, output_root, report_root)
    return _run_real_cm_serial(args, candidate_table, output_root, report_root)


def _render_md(summary: dict[str, Any]) -> str:
    checks = summary["checks"]
    cm = summary["cm_summary"]
    cn = summary["cn_summary"]
    semantic = summary.get("semantic_gate_summary", {})
    lines = [
        "# Phase3CP Real CM Small Loop 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Result",
        "",
        "```text",
        f"generated_candidates: {summary['generated_candidates']}",
        f"ca_candidate_count: {summary['ca_summary']['candidate_count']}",
        f"cm_field_gate_passed: {summary['field_gate_summary']['candidate_count']}",
        f"cm_field_gate_rejected_missing: {summary['field_gate_summary']['rejected_missing_field_count']}",
        f"cm_field_gate_rejected_turnover_proxy: {summary['field_gate_summary'].get('rejected_turnover_proxy_count', 0)}",
        f"cm_field_gate_passed_over_limit: {summary['field_gate_summary']['passed_over_limit_count']}",
        f"cm_selection_mode: {summary['field_gate_summary']['selection_mode']}",
        f"pre_cm_turnover_proxy_max: {summary['field_gate_summary'].get('pre_cm_turnover_proxy_max')}",
        f"pre_cm_semantic_gate_enabled: {semantic.get('enabled')}",
        f"pre_cm_semantic_passed: {semantic.get('passed_candidate_count')}",
        f"pre_cm_semantic_rejected: {semantic.get('rejected_candidate_count')}",
        f"pre_cm_semantic_kept: {semantic.get('kept_candidate_count')}",
        f"cm_lineage_consistent: {summary['lineage_consistency_summary']['lineage_consistent']}",
        f"cm_candidate_count: {cm['candidate_count']}",
        f"cm_followup_count: {cm['followup_count']}",
        f"cn_candidate_count: {cn['candidate_count']}",
        f"next_allocated_budget: {summary['reschedule_summary']['allocated_budget']}",
        f"next_fresh_share: {summary['reschedule_summary']['fresh_share']}",
        "```",
        "",
        "## Checks",
        "",
        "```text",
    ]
    for key, value in checks.items():
        lines.append(f"{key}: {value}")
    lines.extend(
        [
            "```",
            "",
            "## Boundary",
            "",
            "- This route runs real `phase3cm-train-portfolio-sortino-reward-audit`.",
            "- It still uses a bounded small candidate/sample budget.",
            "- CA metrics remain ranking-only.",
            "- Holdout is report-only and not scheduler feedback.",
            "- X0/R3 remain read-only.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--co-root", type=Path, default=DEFAULT_CO_ROOT)
    parser.add_argument("--arm-budget-table", type=Path)
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--generation-budget", type=int, default=32)
    parser.add_argument("--shortfall-fill-rounds", type=int, default=4)
    parser.add_argument("--shortfall-oversample-multiplier", type=float, default=1.5)
    parser.add_argument("--ca-top-n", type=int, default=24)
    parser.add_argument("--cm-candidate-limit", type=int, default=8)
    parser.add_argument("--cm-selection-mode", choices=["ca_ranked", "arm_balanced"], default="ca_ranked")
    parser.add_argument("--cm-max-shards", type=int, default=1)
    parser.add_argument("--cm-sample-trade-times-per-shard", type=int, default=32)
    parser.add_argument("--cm-event-aware-sample-times", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--cm-event-sample-trade-times-per-shard", type=int, default=0)
    parser.add_argument("--cm-horizons", default="1,5,15")
    parser.add_argument("--cm-train-fraction", type=float, default=0.60)
    parser.add_argument("--cm-validation-fraction", type=float, default=0.20)
    parser.add_argument("--cm-min-obs-per-time", type=int, default=20)
    parser.add_argument("--cm-cost-bps", type=float, default=5.0)
    parser.add_argument("--cm-top-quantile", type=float, default=0.2)
    parser.add_argument("--cm-rank-ic-loss-weight", type=float, default=6.0)
    parser.add_argument("--cm-rank-ic-component-cap", type=float, default=0.35)
    parser.add_argument("--cm-regime-stability-weight", type=float, default=0.08)
    parser.add_argument("--cm-regime-component-cap", type=float, default=0.10)
    parser.add_argument("--cm-operator-cache-max-entries", type=int, default=512)
    parser.add_argument("--cm-feature-matrix-cache-max-windows", type=int, default=6)
    parser.add_argument("--cm-persistent-cache-root", type=Path, default=None)
    parser.add_argument("--cm-persistent-cache-mode", choices=("off", "read", "write", "readwrite"), default="readwrite")
    parser.add_argument("--cm-disable-persistent-expression-cache", action="store_true")
    parser.add_argument("--cm-disable-persistent-operator-cache", action="store_true")
    parser.add_argument("--cm-disable-persistent-feature-matrix-cache", action="store_true")
    parser.add_argument("--pre-cm-turnover-proxy-max", type=float, default=float("nan"))
    parser.add_argument("--pre-cm-semantic-gate", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--pre-cm-semantic-oversample-multiplier", type=float, default=2.0)
    parser.add_argument("--pre-cm-semantic-max-shards", type=int, default=1)
    parser.add_argument("--pre-cm-semantic-sample-trade-times-per-shard", type=int, default=32)
    parser.add_argument("--pre-cm-semantic-event-sample-trade-times-per-shard", type=int, default=96)
    parser.add_argument("--pre-cm-semantic-horizons", default="1,5")
    parser.add_argument("--pre-cm-semantic-min-rows-total", type=int, default=1)
    parser.add_argument("--pre-cm-semantic-min-nonzero-shards", type=int, default=1)
    parser.add_argument("--pre-cm-semantic-operator-cache-max-entries", type=int, default=128)
    parser.add_argument("--pre-cm-semantic-feature-matrix-cache-max-windows", type=int, default=2)
    parser.add_argument("--write-semantic-block-memory", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--semantic-block-memory-root", type=Path, default=Path("runtime/search_memory/phase3cp_semantic_blocks"))
    parser.add_argument("--cm-workers", type=int, default=1)
    parser.add_argument("--cm-parallel-axis", choices=("candidate", "shard"), default="candidate")
    parser.add_argument("--numexpr-threads", type=int, default=4)
    parser.add_argument("--min-clean-feedback", type=int, default=2)
    parser.add_argument("--reschedule-total-budget", type=int, default=512)
    parser.add_argument("--memory-root", type=Path, action="append", default=[])
    parser.add_argument("--memory-glob", action="append", default=DEFAULT_MEMORY_GLOBS)
    args = parser.parse_args(argv)

    co_root = _resolve(args.co_root)
    shard_root = _resolve(args.shard_root)
    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    if not shard_root.exists():
        raise FileNotFoundError(f"true1min shard root does not exist: {shard_root}")
    shard_root_text = str(shard_root).lower()
    if "tdxofficial" in shard_root_text or "\\1d" in shard_root_text or "/1d" in shard_root_text:
        raise RuntimeError(f"refusing suspicious non-true1min shard root: {shard_root}")

    arm_budget_path = _budget_table_path(co_root, args.arm_budget_table)
    arm_budget_rows = read_csv_rows(arm_budget_path)
    available_fields, generation_panel_meta = _available_fields(shard_root, max(1, int(args.cm_max_shards)))
    memory_hashes, memory_rows = _load_memory_hashes(args.memory_root, args.memory_glob)
    _write_csv(output_root / "phase3cp_real_cm_memory_roots.csv", memory_rows)
    _write_csv(report_root / "phase3cp_real_cm_memory_roots.csv", memory_rows)
    _write_csv(output_root / "phase3cp_real_cm_generation_panel_schema.csv", generation_panel_meta)
    _write_csv(report_root / "phase3cp_real_cm_generation_panel_schema.csv", generation_panel_meta)
    decisions, scaled_plan = _generate_candidates(
        budget_rows=arm_budget_rows,
        total_budget=args.generation_budget,
        initial_blocked=memory_hashes,
        available_fields=available_fields,
        output_root=output_root,
        report_root=report_root,
        shortfall_fill_rounds=args.shortfall_fill_rounds,
        shortfall_oversample_multiplier=args.shortfall_oversample_multiplier,
    )

    search_root = output_root / "search_outputs"
    ca_root = output_root / "phase3ca_bridge"
    report_ca_root = report_root / "phase3ca_bridge"
    ca_selection_mode = "arm_balanced" if args.cm_selection_mode == "arm_balanced" else "ranked"
    ca_summary = build_candidate_table(
        [search_root],
        ca_root,
        top_n=args.ca_top_n,
        allow_high_corr=False,
        selection_mode=ca_selection_mode,
    )
    _copy_report_files(ca_root, report_ca_root)
    ca_table = ca_root / "phase3ca_bz_candidate_audit.csv"
    field_gate_limit = int(args.cm_candidate_limit)
    if bool(args.pre_cm_semantic_gate):
        field_gate_limit = max(
            int(args.cm_candidate_limit),
            int(math.ceil(int(args.cm_candidate_limit) * max(1.0, float(args.pre_cm_semantic_oversample_multiplier)))),
        )
    cm_candidate_table_raw, field_gate_summary = _filter_cm_feasible_candidates(
        ca_table=ca_table,
        shard_root=shard_root,
        max_shards=args.cm_max_shards,
        limit=field_gate_limit,
        selection_mode=args.cm_selection_mode,
        pre_cm_turnover_proxy_max=args.pre_cm_turnover_proxy_max,
        output_root=output_root,
        report_root=report_root,
    )
    cm_candidate_table, semantic_gate_summary = _run_pre_cm_semantic_viability_gate(
        args=args,
        candidate_table=cm_candidate_table_raw,
        output_root=output_root,
        report_root=report_root,
        final_limit=int(args.cm_candidate_limit),
    )
    cm_summary = _run_real_cm(args, cm_candidate_table, output_root, report_root)

    cm_table = output_root / "phase3cm_train_reward" / "phase3cm_train_reward.csv"
    lineage_consistency_summary = _audit_cm_lineage_consistency(
        candidate_table=cm_candidate_table,
        cm_table=cm_table,
        output_root=output_root,
        report_root=report_root,
    )
    cn_output_root = output_root / "phase3cn_feedback_memory"
    cn_report_root = report_root / "phase3cn_feedback_memory"
    cn_summary = build_feedback_memory(
        cm_tables=[cm_table],
        cm_roots=[],
        output_root=cn_output_root,
        report_root=cn_report_root,
        train_threshold=0.0,
        validation_floor=0.0,
        max_turnover=0.75,
        max_family_share=0.25,
        min_clean_feedback=args.min_clean_feedback,
    )

    next_arm_rows = read_csv_rows(cn_output_root / "phase3cn_arm_score_table.csv")
    next_family_rows = read_csv_rows(cn_output_root / "phase3cn_family_score_table.csv")
    next_blocked_rows = read_csv_rows(cn_output_root / "phase3cn_blocked_family_table.csv")
    next_exploit_rows = read_csv_rows(cn_output_root / "phase3cn_exploit_allowed_family_table.csv")
    reschedule_rows, reschedule_family_actions, reschedule_summary = build_arm_schedule(
        next_arm_rows,
        next_family_rows,
        next_blocked_rows,
        next_exploit_rows,
        total_budget=args.reschedule_total_budget,
        fresh_floor_share=0.45,
        cem_probe_cap_share=0.06,
        max_family_share=0.25,
    )
    _write_csv(output_root / "phase3cp_real_cm_next_arm_budget_table.csv", reschedule_rows)
    _write_csv(output_root / "phase3cp_real_cm_next_family_action_table.csv", reschedule_family_actions)
    _write_csv(report_root / "phase3cp_real_cm_next_arm_budget_table.csv", reschedule_rows)
    _write_csv(report_root / "phase3cp_real_cm_next_family_action_table.csv", reschedule_family_actions)

    checks = {
        "budget_table_used": bool(arm_budget_rows),
        "generated_budget_ok": len(decisions) == int(args.generation_budget),
        "ca_has_candidates": int(ca_summary["candidate_count"]) > 0,
        "field_gate_has_cm_candidates": int(field_gate_summary["candidate_count"]) > 0,
        "semantic_gate_has_cm_candidates": int(semantic_gate_summary.get("kept_candidate_count") or semantic_gate_summary.get("passed_candidate_count") or 0) > 0,
        "real_cm_eval_used": str(cm_summary.get("experiment_id")) == "20260623_phase3cm_train_portfolio_sortino_reward_audit",
        "true1min_shard_root_exists": shard_root.exists(),
        "suspicious_1d_path_blocked": "tdxofficial" not in shard_root_text and "\\1d" not in shard_root_text and "/1d" not in shard_root_text,
        "cm_fast_mode": bool(cm_summary.get("fast_mode")),
        "cm_candidate_count_ok": int(cm_summary["candidate_count"]) == min(
            int(args.cm_candidate_limit),
            int(semantic_gate_summary.get("kept_candidate_count") or semantic_gate_summary.get("passed_candidate_count") or 0),
        ),
        "cm_lineage_consistent": bool(lineage_consistency_summary["lineage_consistent"]),
        "cn_memory_matches_cm": int(cn_summary["candidate_count"]) == int(cm_summary["candidate_count"]),
        "reschedule_total_ok": int(reschedule_summary["allocated_budget"]) == int(args.reschedule_total_budget),
        "holdout_not_optimizer_input": True,
        "memory_blocklist_loaded": len(memory_hashes) > 0,
    }
    passed = all(bool(value) for value in checks.values())
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cp_real_cm_small_loop",
        "decision": "PHASE3CP_REAL_CM_SMALL_LOOP_PASS_DIAGNOSTIC_ONLY" if passed else "PHASE3CP_REAL_CM_SMALL_LOOP_FAIL",
        "input_co_root": str(co_root),
        "input_arm_budget_table": str(arm_budget_path),
        "shard_root": str(shard_root),
        "generated_candidates": len(decisions),
        "generation_budget": int(args.generation_budget),
        "shortfall_fill_rounds": int(args.shortfall_fill_rounds),
        "shortfall_oversample_multiplier": float(args.shortfall_oversample_multiplier),
        "memory_hash_count": len(memory_hashes),
        "memory_roots": memory_rows,
        "search_generation": True,
        "true1min_portfolio_eval": True,
        "cm_reward_source": "phase3cm_train_portfolio_sortino_reward_audit",
        "cm_parallel_axis": str(args.cm_parallel_axis),
        "cm_event_aware_sample_times": bool(args.cm_event_aware_sample_times),
        "cm_event_sample_trade_times_per_shard": int(args.cm_event_sample_trade_times_per_shard),
        "cm_persistent_cache_root": str(args.cm_persistent_cache_root or ""),
        "cm_persistent_cache_mode": str(args.cm_persistent_cache_mode),
        "cm_persistent_expression_cache": not bool(args.cm_disable_persistent_expression_cache),
        "cm_persistent_operator_cache": not bool(args.cm_disable_persistent_operator_cache),
        "cm_persistent_feature_matrix_cache": not bool(args.cm_disable_persistent_feature_matrix_cache),
        "pre_cm_semantic_gate": bool(args.pre_cm_semantic_gate),
        "checks": checks,
        "initial_arm_plan": scaled_plan,
        "ca_summary": ca_summary,
        "field_gate_summary": field_gate_summary,
        "semantic_gate_summary": semantic_gate_summary,
        "cm_summary": cm_summary,
        "lineage_consistency_summary": lineage_consistency_summary,
        "cn_summary": cn_summary,
        "reschedule_summary": reschedule_summary,
        "metric_boundary": "small real-CM diagnostic loop; not alpha proof and not production promotion",
    }
    _write_json(output_root / "phase3cp_real_cm_small_loop_summary.json", summary)
    _write_json(report_root / "phase3cp_real_cm_small_loop_summary.json", summary)
    _write_csv(report_root / "phase3cp_real_cm_small_loop_checks.csv", [checks])
    (report_root / "PHASE3CP_REAL_CM_SMALL_LOOP_20260623.md").write_text(_render_md(summary), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
