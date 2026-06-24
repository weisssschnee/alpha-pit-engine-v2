"""True-1min search algorithm smoke test.

Phase3BP compares a conservative BO-style template reference against broader
true-1min native generator arms. This is still a smoke test: it measures whether
the generator core deserves a larger run, not whether any candidate is
production-ready.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _discover_panels,
    _f,
    _fields,
    _fmt,
    _max_expression_window,
    _run_materialization,
    _write_csv,
    _write_json,
)
from our_system_phase2.runtime.phase3bn_open_diversified_true1min_canary import (
    DEFAULT_MEMORY_ROOT,
    _load_memory_hashes,
    _prior_hashes,
)
from our_system_phase2.services.atom_lane_manifest import EPS, build_search_atoms
from our_system_phase2.services.typed_primitive_gate import validate_expression


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3bp_true1min_search_algorithm_smoke_20260615")
DEFAULT_REPORT_ROOT = Path("reports/phase3bp_true1min_search_algorithm_smoke_20260615")
OP_RE = re.compile(r"\b([A-Za-z][A-Za-z0-9_]*)\s*\(")
WIN_RE = re.compile(r"(?<![A-Za-z0-9_])([1-9][0-9]{0,2})(?![A-Za-z0-9_])")
FIELD_RE = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*")
NUM_RE = re.compile(r"(?<![A-Za-z0-9_])(?:\d+\.\d+|\d+)(?![A-Za-z0-9_])")


PRIOR_DECISION_FILES = [
    Path("reports/phase3bn_open_diversified_true1min_canary_20260615/phase3bn_top_decisions.csv"),
    Path("reports/phase3bo_mature_cem_bridge_true1min_pack_20260615/phase3bo_top_decisions.csv"),
    Path("reports/phase3bm_bl_pass_focused_replay_20260615/phase3bm_candidate_decisions.csv"),
]
PRIOR_HASH_FILES = [
    Path("reports/phase3bk_bj_top64_strict_audit_20260615/phase3bk_bj_top64_candidate_audit.csv"),
    Path("reports/phase3bl_bk_priority_signal_materialization_20260615/phase3bl_candidate_horizon_aggregate.csv"),
    Path("reports/phase3bm_bl_pass_focused_replay_20260615/phase3bm_candidate_decisions.csv"),
    Path("reports/phase3bn_open_diversified_true1min_canary_20260615/phase3bn_top_decisions.csv"),
    Path("reports/phase3bo_mature_cem_bridge_true1min_pack_20260615/phase3bo_top_decisions.csv"),
]
OPERATORS = {
    "Abs",
    "Add",
    "CSRank",
    "CSResidual",
    "Delay",
    "Div",
    "Mean",
    "Mom",
    "Mul",
    "Neg",
    "Sign",
    "Std",
    "Sub",
    "ZScore",
    "Delta",
    "EventAge",
    "SinceLastEvent",
    "EventCount",
    "StateAge",
    "StateDwell",
    "WindowStateCount",
    "ValidRatioGate",
    "MaskedZScore",
    "MaskedCorr",
    "SafeCSResidual",
}


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _hash(text: str, length: int = 24) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:length]


def _read_csv(path: Path) -> list[dict[str, str]]:
    path = _resolve(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _operators(expression: str) -> set[str]:
    return {match.group(1) for match in OP_RE.finditer(expression or "") if match.group(1) in OPERATORS}


def _windows(expression: str) -> set[str]:
    return {match.group(1) for match in WIN_RE.finditer(expression or "") if 1 <= int(match.group(1)) <= 252}


def _bin_int(value: int, edges: tuple[int, ...], prefix: str) -> str:
    for edge in edges:
        if value <= edge:
            return f"{prefix}_le_{edge}"
    return f"{prefix}_gt_{edges[-1]}"


def _ast_variables(expression: str) -> dict[str, Any]:
    expression = expression or ""
    ops_in_order = [match.group(1) for match in OP_RE.finditer(expression) if match.group(1) in OPERATORS]
    fields = _fields(expression)
    wins = sorted({int(win) for win in _windows(expression)})
    depth = 0
    max_depth = 0
    for char in expression:
        if char == "(":
            depth += 1
            max_depth = max(max_depth, depth)
        elif char == ")":
            depth = max(0, depth - 1)
    skeleton = FIELD_RE.sub("$F", expression)
    skeleton = NUM_RE.sub("N", skeleton)
    skeleton = re.sub(r"\s+", "", skeleton)
    op_sequence = ">".join(ops_in_order[:10]) or "none"
    op_multiset = "|".join(f"{op}:{count}" for op, count in sorted(Counter(ops_in_order).items())) or "none"
    field_count = len(fields)
    op_count = len(ops_in_order)
    window_count = len(wins)
    max_window = max(wins, default=0)
    complexity = op_count + field_count + window_count
    return {
        "ast_skeleton": skeleton[:240],
        "ast_operator_sequence": op_sequence,
        "ast_operator_multiset": op_multiset,
        "ast_root_operator": ops_in_order[0] if ops_in_order else "none",
        "ast_depth": max_depth,
        "ast_depth_bin": _bin_int(max_depth, (2, 4, 6, 8), "depth"),
        "ast_operator_count": op_count,
        "ast_operator_count_bin": _bin_int(op_count, (3, 6, 9, 12), "opcount"),
        "ast_field_count": field_count,
        "ast_field_count_bin": _bin_int(field_count, (1, 2, 4, 6), "fieldcount"),
        "ast_window_count": window_count,
        "ast_window_count_bin": _bin_int(window_count, (0, 1, 2, 4), "wincount"),
        "ast_max_window": max_window,
        "ast_max_window_bin": _bin_int(max_window, (0, 5, 15, 30, 60), "maxwin"),
        "ast_complexity": complexity,
        "ast_complexity_bin": _bin_int(complexity, (6, 10, 14, 18), "complexity"),
    }


def _prior_reward(row: dict[str, Any]) -> float:
    abs_ic = abs(
        _f(
            row.get("abs_aligned_ic_mean")
            or row.get("aligned_ic_mean")
            or row.get("ic_mean")
            or row.get("mean_window_rank_ic"),
            0.0,
        )
    )
    decision = " ".join(str(row.get(key) or "") for key in ("phase3bo_decision", "phase3bn_decision", "phase3bm_decision", "phase3bl_decision"))
    blockers = " ".join(str(row.get(key) or "") for key in ("phase3bo_blocker_flags", "phase3bn_blocker_flags", "blocker_flags"))
    reward = abs_ic
    if "followup_priority" in decision:
        reward += 0.035
    if "pass" in decision:
        reward += 0.015
    if "future_signal_wrong_lag_too_strong" in blockers:
        reward -= 0.18
    if "signal_corr_abs" in blockers:
        reward -= 0.06
    if "weak_dense" in blockers:
        reward -= 0.04
    if "too_few_positive_horizons" in blockers:
        reward -= 0.03
    return float(max(-0.25, min(0.25, reward)))


def _ucb(values: list[float], total: int, *, exploration: float) -> float:
    if not values:
        return 0.40 * exploration
    arr = np.asarray(values, dtype=float)
    mean = float(arr.mean())
    uncertainty = math.sqrt(max(0.0, math.log(max(2, total + 1))) / max(1, len(values)))
    return mean + (exploration * uncertainty)


def _build_policy(prior_files: list[Path], *, exploration: float) -> dict[str, Any]:
    buckets: dict[str, dict[str, list[float]]] = {
        "field": defaultdict(list),
        "operator": defaultdict(list),
        "window": defaultdict(list),
        "lane": defaultdict(list),
        "fieldset": defaultdict(list),
        "ast_skeleton": defaultdict(list),
        "ast_operator_sequence": defaultdict(list),
        "ast_operator_multiset": defaultdict(list),
        "ast_root_operator": defaultdict(list),
        "ast_depth_bin": defaultdict(list),
        "ast_operator_count_bin": defaultdict(list),
        "ast_field_count_bin": defaultdict(list),
        "ast_window_count_bin": defaultdict(list),
        "ast_max_window_bin": defaultdict(list),
        "ast_complexity_bin": defaultdict(list),
    }
    examples: list[dict[str, Any]] = []
    total = 0
    for path in prior_files:
        for row in _read_csv(path):
            expression = str(row.get("expression") or "")
            if not expression:
                continue
            reward = _prior_reward(row)
            total += 1
            lane = str(row.get("factor_lane") or row.get("primitive_family") or "unknown")
            fields = _fields(expression)
            fieldset = "|".join(fields)
            ast = _ast_variables(expression)
            buckets["lane"][lane].append(reward)
            buckets["fieldset"][fieldset].append(reward)
            for key in (
                "ast_skeleton",
                "ast_operator_sequence",
                "ast_operator_multiset",
                "ast_root_operator",
                "ast_depth_bin",
                "ast_operator_count_bin",
                "ast_field_count_bin",
                "ast_window_count_bin",
                "ast_max_window_bin",
                "ast_complexity_bin",
            ):
                buckets[key][str(ast[key])].append(reward)
            for field in fields:
                buckets["field"][field].append(reward)
            for operator in _operators(expression):
                buckets["operator"][operator].append(reward)
            for window in _windows(expression):
                buckets["window"][window].append(reward)
            if len(examples) < 20:
                examples.append({"source": str(path), "lane": lane, "reward": round(reward, 6), "expression": expression})
    scores = {
        kind: {key: round(_ucb(values, total, exploration=exploration), 6) for key, values in values_by_key.items()}
        for kind, values_by_key in buckets.items()
    }
    return {
        "policy_version": "phase3bp_true1min_ucb_smoke_v1",
        "scope": "true1min_prior_routing_not_production_reward",
        "total_observation_count": total,
        "exploration": float(exploration),
        "scores": scores,
        "examples": examples,
        "top_keys": {
            kind: sorted(values.items(), key=lambda item: item[1], reverse=True)[:12]
            for kind, values in scores.items()
        },
    }


def _policy_score(expression: str, lane: str, policy: dict[str, Any]) -> float:
    scores = policy.get("scores") or {}
    fields = _fields(expression)
    ops = _operators(expression)
    wins = _windows(expression)
    fieldset = "|".join(fields)
    lane_score = float((scores.get("lane") or {}).get(lane, 0.0))
    field_score = np.mean([float((scores.get("field") or {}).get(field, 0.0)) for field in fields]) if fields else 0.0
    op_score = np.mean([float((scores.get("operator") or {}).get(op, 0.0)) for op in ops]) if ops else 0.0
    win_score = np.mean([float((scores.get("window") or {}).get(win, 0.0)) for win in wins]) if wins else 0.0
    fieldset_score = float((scores.get("fieldset") or {}).get(fieldset, 0.0))
    ast = _ast_variables(expression)
    ast_score_keys = (
        "ast_skeleton",
        "ast_operator_sequence",
        "ast_operator_multiset",
        "ast_root_operator",
        "ast_depth_bin",
        "ast_operator_count_bin",
        "ast_field_count_bin",
        "ast_window_count_bin",
        "ast_max_window_bin",
        "ast_complexity_bin",
    )
    ast_scores = [float((scores.get(key) or {}).get(str(ast[key]), 0.0)) for key in ast_score_keys]
    ast_score = float(np.mean(ast_scores)) if ast_scores else 0.0
    novelty_bonus = 0.01 * sum(1 for field in fields if field not in (scores.get("field") or {}))
    ast_novelty_bonus = 0.006 * sum(1 for key in ast_score_keys if str(ast[key]) not in (scores.get(key) or {}))
    return float(
        (0.18 * lane_score)
        + (0.21 * field_score)
        + (0.12 * op_score)
        + (0.08 * win_score)
        + (0.15 * fieldset_score)
        + (0.20 * ast_score)
        + novelty_bonus
        + ast_novelty_bonus
    )


def _add_candidate(
    rows: list[dict[str, Any]],
    seen: set[str],
    blocked: set[str],
    expression: str,
    *,
    lane: str,
    source_generator: str,
    note: str,
    policy: dict[str, Any],
) -> None:
    expression = expression.strip()
    verdict = validate_expression(
        expression,
        entry_lineage="phase3bp_generator",
        materialization_stage="candidate_construction",
        candidate_role="true1min_search_candidate",
    )
    if verdict.typed_gate_decision != "allow":
        return
    digest = _hash(expression)
    if digest in seen or digest in blocked:
        return
    memory_key = f"phase3bp:{digest}"
    if memory_key in blocked:
        return
    seen.add(digest)
    fields = _fields(expression)
    ast = _ast_variables(expression)
    rows.append(
        {
            "candidate_id": f"phase3bp_{len(rows) + 1:05d}",
            "expression_hash": digest,
            "expression": expression,
            "factor_lane": lane,
            "source_lane": source_generator,
            "source_generator": source_generator,
            "fields": "|".join(fields),
            "fields_list": fields,
            "max_window": _max_expression_window(expression),
            **ast,
            "search_memory_key": memory_key,
            "policy_score": round(_policy_score(expression, lane, policy), 8),
            "expected_direction": 1,
            "x0_r3_role": "read_only_research_candidate",
            "note": note,
            "typed_gate_decision": verdict.typed_gate_decision,
            "typed_gate_reason": verdict.typed_gate_reason,
            "registry_version": verdict.registry_version,
        }
    )


def _panel_schema_fields(panels: list[Path]) -> list[str]:
    """Return fields present in every selected panel.

    Candidate generation is schema-bound: a field listed in the global atom
    manifest is not enough. It must exist in every panel used by the run.
    """

    field_sets: list[set[str]] = []
    for panel in panels:
        field_sets.append(set(pq.ParquetFile(panel).schema_arrow.names))
    if not field_sets:
        return []
    common = set.intersection(*field_sets)
    return sorted(common)


def _raw_atoms(available_fields: list[str] | set[str] | None = None) -> list[dict[str, Any]]:
    return build_search_atoms(available_fields)


def _atom_rank_expr(atom: dict[str, Any]) -> str:
    expr = str(atom["expr"])
    if str(atom.get("transform_mode") or "") == "typed_rank":
        return f"CSRank({expr})"
    return f"CSRank(ZScore({expr}))"


def _atom_inverted_expr(atom: dict[str, Any]) -> str:
    return f"Neg({_atom_rank_expr(atom)})"


def _atom_normalized_expr(atom: dict[str, Any]) -> str:
    expr = str(atom["expr"])
    if str(atom.get("transform_mode") or "") == "typed_rank":
        return f"CSRank({expr})"
    return f"ZScore({expr})"


def _rank_atoms_for_interaction(atoms: list[dict[str, Any]], policy: dict[str, Any], max_count: int) -> list[dict[str, Any]]:
    ranked = sorted(
        atoms,
        key=lambda atom: (
            _policy_score(str(atom["expr"]), str(atom.get("lane") or ""), policy),
            str(atom.get("field_class") or ""),
            str(atom.get("name") or ""),
        ),
        reverse=True,
    )
    return ranked[: max(1, min(len(ranked), int(max_count)))]


def _generate_rx_ucb_candidates(
    max_candidates: int,
    blocked: set[str],
    policy: dict[str, Any],
    *,
    include_residual: bool,
    available_fields: list[str] | set[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    atoms = _raw_atoms(available_fields)
    for atom in atoms:
        for transform, expression in {
            "rank": _atom_rank_expr(atom),
            "inverted": _atom_inverted_expr(atom),
        }.items():
            _add_candidate(
                rows,
                seen,
                blocked,
                expression,
                lane=f"{atom['lane']}::{transform}",
                source_generator="phase3bp_true1min_rx_ucb_native",
                note=f"rx atom transform {atom['name']}",
                policy=policy,
            )
    event_atoms = [atom for atom in atoms if atom["side"] == "event"]
    state_atoms = [atom for atom in atoms if atom["side"] == "state"]
    event_atoms = _rank_atoms_for_interaction(event_atoms, policy, max(32, max_candidates))
    state_atoms = _rank_atoms_for_interaction(state_atoms, policy, max(32, max_candidates))
    for left in event_atoms:
        for right in state_atoms:
            if left["name"].split("_")[0] == right["name"].split("_")[0]:
                continue
            lane = f"rx_interaction::{left['lane']}::{right['lane']}"
            left_norm = _atom_normalized_expr(left)
            right_norm = _atom_normalized_expr(right)
            variants = {
                "product": f"CSRank(Mul({left_norm},{right_norm}))",
                "spread": f"CSRank(Sub({left_norm},{right_norm}))",
            }
            if include_residual:
                variants["residual"] = f"CSRank(CSResidual(CSRank({left['expr']}),CSRank({right['expr']})))"
            for kind, expression in variants.items():
                _add_candidate(
                    rows,
                    seen,
                    blocked,
                    expression,
                    lane=f"{lane}::{kind}",
                    source_generator="phase3bp_true1min_rx_ucb_native",
                    note=f"rx interaction {left['name']} x {right['name']} {kind}",
                    policy=policy,
                )
    rows.sort(key=lambda row: (float(row.get("policy_score") or 0.0), -int(row.get("max_window") or 0), row["expression_hash"]), reverse=True)
    selected: list[dict[str, Any]] = []
    lane_counts: Counter[str] = Counter()
    fieldset_counts: Counter[str] = Counter()
    lane_cap = max(3, int(math.ceil(max_candidates * 0.10)))
    fieldset_cap = 4
    for row in rows:
        lane = str(row.get("factor_lane"))
        fieldset = str(row.get("fields"))
        if lane_counts[lane] >= lane_cap:
            continue
        if fieldset_counts[fieldset] >= fieldset_cap:
            continue
        selected.append(row)
        lane_counts[lane] += 1
        fieldset_counts[fieldset] += 1
        if len(selected) >= max_candidates:
            break
    return selected


def _generate_event_state_candidates(
    max_candidates: int,
    blocked: set[str],
    policy: dict[str, Any],
    *,
    include_interactions: bool = True,
    available_fields: list[str] | set[str] | None = None,
) -> list[dict[str, Any]]:
    event_atom_rows: list[dict[str, Any]] = []
    context_atom_rows: list[dict[str, Any]] = []
    interaction_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    atoms = _raw_atoms(available_fields)
    event_atoms = [atom for atom in atoms if str(atom.get("role") or "") == "event_state_search"]
    context_atoms = [atom for atom in atoms if str(atom.get("role") or "") == "lagged_context_search"]
    state_atoms = [
        atom
        for atom in atoms
        if atom["side"] == "state" and str(atom.get("field_class") or "") not in {"event_state"}
    ]
    event_atoms = _rank_atoms_for_interaction(event_atoms, policy, max(64, max_candidates * 2))
    context_atoms = _rank_atoms_for_interaction(context_atoms, policy, max(64, max_candidates * 2))
    state_atoms = _rank_atoms_for_interaction(state_atoms, policy, max(32, max_candidates))

    for atom in event_atoms:
        for transform, expression in {
            "event_rank": _atom_rank_expr(atom),
            "event_inverted": _atom_inverted_expr(atom),
        }.items():
            _add_candidate(
                event_atom_rows,
                seen,
                blocked,
                expression,
                lane=f"typed_event_state::{atom['lane']}::{transform}",
                source_generator="phase3bp_true1min_typed_event_state",
                note=f"typed event atom {atom['name']} role={atom.get('role')} class={atom.get('field_class')}",
                policy=policy,
            )
            if len(event_atom_rows) >= max_candidates and not include_interactions:
                return event_atom_rows[:max_candidates]

    for atom in context_atoms:
        for transform, expression in {
            "context_rank": _atom_rank_expr(atom),
            "context_inverted": _atom_inverted_expr(atom),
        }.items():
            _add_candidate(
                context_atom_rows,
                seen,
                blocked,
                expression,
                lane=f"typed_lagged_context::{atom['lane']}::{transform}",
                source_generator="phase3bp_true1min_typed_event_state",
                note=f"typed lagged context atom {atom['name']} role={atom.get('role')} class={atom.get('field_class')}",
                policy=policy,
            )

    if include_interactions:
        for left in event_atoms:
            for right in state_atoms:
                if len(interaction_rows) >= max_candidates * 4:
                    break
                left_norm = _atom_normalized_expr(left)
                right_norm = _atom_normalized_expr(right)
                for kind, expression in {
                    "event_x_state": f"CSRank(Mul({left_norm},{right_norm}))",
                    "event_minus_state": f"CSRank(Sub({left_norm},{right_norm}))",
                }.items():
                    _add_candidate(
                        interaction_rows,
                        seen,
                        blocked,
                        expression,
                        lane=f"typed_event_interaction::{left['lane']}::{right['lane']}::{kind}",
                        source_generator="phase3bp_true1min_typed_event_state",
                        note=f"typed event interaction {left['name']} x {right['name']} {kind}",
                        policy=policy,
                    )
                    if len(interaction_rows) >= max_candidates * 4:
                        break

    event_atom_rows.sort(key=lambda row: (float(row.get("policy_score") or 0.0), row["expression_hash"]), reverse=True)
    context_atom_rows.sort(key=lambda row: (float(row.get("policy_score") or 0.0), row["expression_hash"]), reverse=True)
    interaction_rows.sort(key=lambda row: (float(row.get("policy_score") or 0.0), row["expression_hash"]), reverse=True)

    def select_from(rows: list[dict[str, Any]], limit: int, *, selected: list[dict[str, Any]], lane_counts: Counter[str], fieldset_counts: Counter[str]) -> None:
        lane_cap = max(8, int(math.ceil(max_candidates * 0.35)))
        fieldset_cap = 4
        for row in rows:
            if len(selected) >= limit:
                break
            lane = str(row.get("factor_lane"))
            fieldset = str(row.get("fields"))
            if lane_counts[lane] >= lane_cap:
                continue
            if fieldset_counts[fieldset] >= fieldset_cap:
                continue
            selected.append(row)
            lane_counts[lane] += 1
            fieldset_counts[fieldset] += 1

    selected: list[dict[str, Any]] = []
    lane_counts: Counter[str] = Counter()
    fieldset_counts: Counter[str] = Counter()
    event_atom_quota = min(max_candidates, max(16, int(math.ceil(max_candidates * 0.25))))
    context_atom_quota = min(max_candidates, max(16, int(math.ceil(max_candidates * 0.25))))
    select_from(event_atom_rows, event_atom_quota, selected=selected, lane_counts=lane_counts, fieldset_counts=fieldset_counts)
    select_from(context_atom_rows, min(max_candidates, len(selected) + context_atom_quota), selected=selected, lane_counts=lane_counts, fieldset_counts=fieldset_counts)
    select_from(interaction_rows, max_candidates, selected=selected, lane_counts=lane_counts, fieldset_counts=fieldset_counts)
    if len(selected) < max_candidates:
        select_from(event_atom_rows, max_candidates, selected=selected, lane_counts=lane_counts, fieldset_counts=fieldset_counts)
    if len(selected) < max_candidates:
        select_from(context_atom_rows, max_candidates, selected=selected, lane_counts=lane_counts, fieldset_counts=fieldset_counts)
    for idx, row in enumerate(selected, 1):
        row["candidate_id"] = f"phase3bp_event_{idx:05d}"
    return selected


def _proposal_score(row: dict[str, Any], policy: dict[str, Any]) -> float:
    base = float(row.get("policy_score") or 0.0)
    fields = set(str(row.get("fields") or "").split("|")) - {""}
    ops = _operators(str(row.get("expression") or ""))
    complexity_penalty = 0.006 * max(0, len(ops) - 5)
    novelty = 0.004 * len(fields)
    jitter = (int(str(row.get("expression_hash") or "0")[:6], 16) % 1000) / 1000_000.0
    return float(base + novelty + jitter - complexity_penalty)


def _generate_cem_elite_candidates(
    max_candidates: int,
    blocked: set[str],
    policy: dict[str, Any],
    *,
    include_residual: bool,
    population_size: int,
    elite_frac: float,
    rounds: int,
    available_fields: list[str] | set[str] | None = None,
) -> list[dict[str, Any]]:
    """Prior-guided CEM-style elite resampling over true-1min expression atoms.

    This is not the old daily CEM chain. It is a true-1min native arm that uses
    cross-entropy-style elite selection to bias formula generation before the
    strict minute materialization pass.
    """

    atoms = _raw_atoms(available_fields)
    event_atoms = [atom for atom in atoms if atom["side"] == "event"]
    state_atoms = [atom for atom in atoms if atom["side"] == "state"]
    event_atoms = _rank_atoms_for_interaction(event_atoms, policy, max(48, max_candidates))
    state_atoms = _rank_atoms_for_interaction(state_atoms, policy, max(48, max_candidates))

    def add_pool(pool: list[dict[str, Any]], seen: set[str], expression: str, lane: str, note: str) -> None:
        before = len(pool)
        _add_candidate(
            pool,
            seen,
            blocked,
            expression,
            lane=lane,
            source_generator="phase3bp_true1min_cem_elite",
            note=note,
            policy=policy,
        )
        if len(pool) > before:
            pool[-1]["cem_prior_score"] = round(_proposal_score(pool[-1], policy), 8)

    pool: list[dict[str, Any]] = []
    seen: set[str] = set()
    for atom in atoms:
        for transform, expression in {
            "rank": _atom_rank_expr(atom),
            "inverted": _atom_inverted_expr(atom),
        }.items():
            add_pool(pool, seen, expression, f"cem_atom::{atom['lane']}::{transform}", f"cem seed atom {atom['name']}")
    for left in event_atoms:
        for right in state_atoms:
            if left["name"].split("_")[0] == right["name"].split("_")[0]:
                continue
            variants = {
                "product": f"CSRank(Mul({_atom_normalized_expr(left)},{_atom_normalized_expr(right)}))",
                "spread": f"CSRank(Sub({_atom_normalized_expr(left)},{_atom_normalized_expr(right)}))",
                "signed_state": f"CSRank(Mul(Sign({_atom_normalized_expr(left)}),{_atom_normalized_expr(right)}))",
            }
            if include_residual:
                variants["residual"] = f"CSRank(CSResidual(CSRank({left['expr']}),CSRank({right['expr']})))"
            for kind, expression in variants.items():
                add_pool(
                    pool,
                    seen,
                    expression,
                    f"cem_interaction::{left['lane']}::{right['lane']}::{kind}",
                    f"cem seed interaction {left['name']} x {right['name']} {kind}",
                )

    population_size = max(max_candidates, int(population_size))
    elite_frac = min(0.50, max(0.05, float(elite_frac)))
    for round_idx in range(max(1, int(rounds))):
        pool.sort(key=lambda row: (_proposal_score(row, policy), row["expression_hash"]), reverse=True)
        population = pool[:population_size]
        elite_count = max(4, int(math.ceil(len(population) * elite_frac)))
        elites = population[:elite_count]
        field_credit: Counter[str] = Counter()
        lane_credit: Counter[str] = Counter()
        for row in elites:
            field_credit.update(str(row.get("fields") or "").split("|"))
            lane_credit.update([str(row.get("factor_lane") or "")])
        event_ranked = sorted(
            event_atoms,
            key=lambda atom: (
                field_credit.get("|".join(_fields(atom["expr"])), 0),
                _policy_score(atom["expr"], atom["lane"], policy),
                atom["name"],
            ),
            reverse=True,
        )
        state_ranked = sorted(
            state_atoms,
            key=lambda atom: (
                field_credit.get("|".join(_fields(atom["expr"])), 0),
                _policy_score(atom["expr"], atom["lane"], policy),
                atom["name"],
            ),
            reverse=True,
        )
        for left in event_ranked[: max(6, max_candidates // 8)]:
            for right in state_ranked[: max(6, max_candidates // 8)]:
                if left["name"].split("_")[0] == right["name"].split("_")[0]:
                    continue
                if (round_idx + int(_hash(left["name"] + right["name"], 8), 16)) % 3 == 0:
                    expression = f"CSRank(Sub({_atom_normalized_expr(left)},Mean({_atom_normalized_expr(right)},3)))"
                    kind = "elite_spread_mean3"
                elif (round_idx + int(_hash(right["name"] + left["name"], 8), 16)) % 3 == 1:
                    expression = f"CSRank(Mul(Delta({_atom_normalized_expr(left)},2),{_atom_normalized_expr(right)}))"
                    kind = "elite_delta_product"
                else:
                    expression = f"Neg(CSRank(Mul({_atom_normalized_expr(left)},{_atom_normalized_expr(right)})))"
                    kind = "elite_inverted_product"
                add_pool(
                    pool,
                    seen,
                    expression,
                    f"cem_resample::{left['lane']}::{right['lane']}::{kind}",
                    f"cem round {round_idx + 1} elite resample {left['name']} x {right['name']}",
                )

    pool.sort(key=lambda row: (_proposal_score(row, policy), row["expression_hash"]), reverse=True)
    selected: list[dict[str, Any]] = []
    lane_counts: Counter[str] = Counter()
    fieldset_counts: Counter[str] = Counter()
    lane_cap = max(4, int(math.ceil(max_candidates * 0.12)))
    fieldset_cap = 5
    for row in pool:
        lane = str(row.get("factor_lane"))
        fieldset = str(row.get("fields"))
        if lane_counts[lane] >= lane_cap:
            continue
        if fieldset_counts[fieldset] >= fieldset_cap:
            continue
        selected.append(row)
        lane_counts[lane] += 1
        fieldset_counts[fieldset] += 1
        if len(selected) >= max_candidates:
            break
    for idx, row in enumerate(selected, 1):
        row["candidate_id"] = f"phase3bp_{idx:05d}"
    return selected


def _generate_hybrid_candidates(
    max_candidates: int,
    blocked: set[str],
    policy: dict[str, Any],
    *,
    include_residual: bool,
    population_size: int,
    elite_frac: float,
    rounds: int,
    available_fields: list[str] | set[str] | None = None,
) -> list[dict[str, Any]]:
    rx_budget = max(1, int(math.ceil(max_candidates * 0.45)))
    cem_budget = max_candidates - rx_budget
    rx_rows = _generate_rx_ucb_candidates(rx_budget, blocked, policy, include_residual=False, available_fields=available_fields)
    cem_rows = _generate_cem_elite_candidates(
        cem_budget + max(8, cem_budget // 4),
        blocked | {str(row.get("expression_hash")) for row in rx_rows},
        policy,
        include_residual=include_residual,
        population_size=max(population_size, max_candidates * 3),
        elite_frac=elite_frac,
        rounds=rounds,
        available_fields=available_fields,
    )
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in [*rx_rows, *cem_rows]:
        digest = str(row.get("expression_hash"))
        if digest in seen:
            continue
        seen.add(digest)
        item = dict(row)
        item["source_generator"] = "phase3bp_true1min_hybrid_rx_cem"
        item["source_lane"] = "phase3bp_true1min_hybrid_rx_cem"
        item["note"] = f"hybrid arm from {row.get('source_generator')}: {row.get('note')}"
        rows.append(item)
        if len(rows) >= max_candidates:
            break
    for idx, row in enumerate(rows, 1):
        row["candidate_id"] = f"phase3bp_{idx:05d}"
    return rows


def _generate_candidates(
    mode: str,
    max_candidates: int,
    blocked: set[str],
    policy: dict[str, Any],
    *,
    include_residual: bool,
    population_size: int,
    elite_frac: float,
    rounds: int,
    available_fields: list[str] | set[str] | None = None,
) -> list[dict[str, Any]]:
    if mode == "rx_ucb":
        return _generate_rx_ucb_candidates(max_candidates, blocked, policy, include_residual=include_residual, available_fields=available_fields)
    if mode == "cem_elite":
        return _generate_cem_elite_candidates(
            max_candidates,
            blocked,
            policy,
            include_residual=include_residual,
            population_size=population_size,
            elite_frac=elite_frac,
            rounds=rounds,
            available_fields=available_fields,
        )
    if mode == "hybrid_rx_cem":
        return _generate_hybrid_candidates(
            max_candidates,
            blocked,
            policy,
            include_residual=include_residual,
            population_size=population_size,
            elite_frac=elite_frac,
            rounds=rounds,
            available_fields=available_fields,
        )
    if mode == "event_state":
        return _generate_event_state_candidates(
            max_candidates,
            blocked,
            policy,
            include_interactions=True,
            available_fields=available_fields,
        )
    raise ValueError(f"unknown algorithm mode: {mode}")


def _aggregate_decisions(
    aggregate_rows: list[dict[str, Any]],
    pairwise_rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    top_n: int,
) -> list[dict[str, Any]]:
    candidate_meta = {str(row.get("expression_hash")): dict(row) for row in candidates}
    by_hash: dict[str, list[dict[str, Any]]] = {}
    for row in aggregate_rows:
        by_hash.setdefault(str(row.get("expression_hash")), []).append(row)
    crowded: set[str] = set()
    for row in pairwise_rows:
        if abs(_f(row.get("signal_rank_corr"), 0.0)) >= 0.75:
            crowded.add(str(row.get("left_expression_hash")))
            crowded.add(str(row.get("right_expression_hash")))
    decisions: list[dict[str, Any]] = []
    for expr_hash, rows in by_hash.items():
        rows = sorted(rows, key=lambda item: abs(_f(item.get("aligned_ic_mean"), 0.0)), reverse=True)
        best = dict(rows[0])
        meta = candidate_meta.get(expr_hash, {})
        for key in ("candidate_id", "source_generator", "source_lane", "policy_score", "note"):
            if key in meta:
                best[key] = meta[key]
        stable = sum(1 for row in rows if abs(_f(row.get("aligned_ic_mean"), 0.0)) > 0.02)
        inherited = {item.strip() for item in str(best.get("blocker_flags") or "").split("|") if item.strip()}
        blockers: list[str] = []
        if expr_hash in crowded:
            blockers.append("signal_corr_abs_ge_0.75")
        if "future_signal_wrong_lag_too_strong" in inherited:
            blockers.append("future_signal_wrong_lag_too_strong")
        if stable < 2:
            blockers.append("too_few_positive_horizons")
        if _f(best.get("mean_one_way_turnover"), 0.0) > 0.95:
            blockers.append("extreme_turnover")
        aligned_ic = _f(best.get("aligned_ic_mean"), float("nan"))
        abs_ic = abs(aligned_ic) if math.isfinite(aligned_ic) else float("nan")
        if not math.isfinite(abs_ic) or abs_ic <= 0.03:
            blockers.append("weak_dense_primary_abs_ic")
        best["open_direction"] = "long_top" if aligned_ic >= 0 else "short_top"
        best["abs_aligned_ic_mean"] = abs_ic
        best["positive_horizon_count"] = stable
        best["phase3bp_blocker_flags"] = "|".join(blockers)
        best["phase3bp_decision"] = "bp_followup_priority" if not blockers and abs_ic > 0.035 else "bp_watch_or_reject"
        decisions.append(best)

    def priority(item: dict[str, Any]) -> tuple[Any, ...]:
        flags = str(item.get("phase3bp_blocker_flags") or "")
        has_future = "future_signal_wrong_lag_too_strong" in flags
        has_crowding = "signal_corr_abs" in flags
        has_blockers = bool(flags)
        return (
            item.get("phase3bp_decision") == "bp_followup_priority",
            not has_future,
            not has_crowding,
            not has_blockers,
            _f(item.get("abs_aligned_ic_mean"), -999.0),
            int(item.get("positive_horizon_count") or 0),
            -_f(item.get("mean_one_way_turnover"), 999.0),
        )

    decisions.sort(key=priority, reverse=True)
    return decisions[:top_n]


def _summarize_by(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key) or "unknown")].append(row)
    out: list[dict[str, Any]] = []
    for value, items in grouped.items():
        out.append(
            {
                key: value,
                "count": len(items),
                "best_abs_aligned_ic": max((abs(_f(row.get("aligned_ic_mean"), 0.0)) for row in items), default=None),
                "followup_count": sum(1 for row in items if row.get("phase3bp_decision") == "bp_followup_priority"),
                "future_wrong_lag_count": sum(1 for row in items if "future_signal_wrong_lag_too_strong" in str(row.get("phase3bp_blocker_flags") or "")),
            }
        )
    out.sort(key=lambda row: (_f(row.get("best_abs_aligned_ic"), -999.0), int(row.get("followup_count") or 0)), reverse=True)
    return out


def _render_md(summary: dict[str, Any], generator_rows: list[dict[str, Any]], lane_rows: list[dict[str, Any]], decisions: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3BP True-1min Search Algorithm Smoke 2026-06-15",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Scope",
        "",
        f"- generator mode: `{summary['generator_mode']}`",
        f"- candidates generated: `{summary['candidate_count']}`",
        f"- true-1min shard panels: `{summary['panel_count']}`",
        f"- sampled signal trade_times per shard: `{summary['sample_trade_times_per_shard']}`",
        f"- total eval rows: `{summary['total_eval_rows']}`",
        f"- followup priority: `{summary['followup_priority_count']}`",
        "",
        "## Generator Comparison",
        "",
        "| generator | count | best abs aligned IC | followup | future-wrong-lag |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in generator_rows:
        lines.append(
            f"| `{row['source_generator']}` | {row['count']} | {_fmt(row.get('best_abs_aligned_ic'))} | {row['followup_count']} | {row['future_wrong_lag_count']} |"
        )
    lines.extend(["", "## Lane Summary", "", "| lane | count | best abs aligned IC | followup | future-wrong-lag |", "|---|---:|---:|---:|---:|"])
    for row in lane_rows[:20]:
        lines.append(
            f"| `{row['factor_lane']}` | {row['count']} | {_fmt(row.get('best_abs_aligned_ic'))} | {row['followup_count']} | {row['future_wrong_lag_count']} |"
        )
    lines.extend(["", "## Top Decisions", "", "| rank | generator | lane | h | fields | abs IC | direction | turnover | decision | blockers |", "|---:|---|---|---:|---|---:|---|---:|---|---|"])
    for idx, row in enumerate(decisions[:25], 1):
        lines.append(
            f"| {idx} | `{row.get('source_generator')}` | `{row.get('factor_lane')}` | {row.get('horizon_min')} | `{row.get('fields')}` | "
            f"{_fmt(row.get('abs_aligned_ic_mean'))} | `{row.get('open_direction')}` | {_fmt(row.get('mean_one_way_turnover'))} | "
            f"`{row.get('phase3bp_decision')}` | `{row.get('phase3bp_blocker_flags') or ''}` |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- This tests the search algorithm, not production alpha.",
            "- `future_signal_wrong_lag_too_strong` is treated as a hard smoke blocker.",
            "- True `trade_time` 1min shards only; no old 1D stock-PIT default panel.",
            "- X0/R3 remains read-only.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--memory-root", type=Path, default=DEFAULT_MEMORY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-candidates", type=int, default=160)
    parser.add_argument("--top-decisions", type=int, default=48)
    parser.add_argument("--max-shards", type=int, default=8)
    parser.add_argument("--sample-trade-times-per-shard", type=int, default=60)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs-per-time", type=int, default=20)
    parser.add_argument("--policy-exploration", type=float, default=0.45)
    parser.add_argument("--include-residual", action="store_true")
    parser.add_argument("--algorithm-mode", choices=["rx_ucb", "cem_elite", "hybrid_rx_cem", "event_state"], default="rx_ucb")
    parser.add_argument("--cem-population-size", type=int, default=384)
    parser.add_argument("--cem-elite-frac", type=float, default=0.18)
    parser.add_argument("--cem-rounds", type=int, default=2)
    args = parser.parse_args(argv)

    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    panels = _discover_panels(_resolve(args.shard_root), args.max_shards)
    available_fields = _panel_schema_fields(panels)
    policy = _build_policy(PRIOR_DECISION_FILES, exploration=args.policy_exploration)
    blocked = _load_memory_hashes(args.memory_root) | _prior_hashes(PRIOR_HASH_FILES)
    candidates = _generate_candidates(
        args.algorithm_mode,
        args.max_candidates,
        blocked,
        policy,
        include_residual=bool(args.include_residual),
        population_size=args.cem_population_size,
        elite_frac=args.cem_elite_frac,
        rounds=args.cem_rounds,
        available_fields=available_fields,
    )
    horizons = tuple(int(item.strip()) for item in str(args.horizons).split(",") if item.strip())
    metric_rows, aggregate_rows, meta = _run_materialization(
        candidates=candidates,
        panels=panels,
        horizons=horizons,
        sample_trade_times_per_shard=args.sample_trade_times_per_shard,
        min_obs_per_time=args.min_obs_per_time,
    )
    pairwise_rows = meta.pop("pairwise_rows")
    decisions = _aggregate_decisions(aggregate_rows, pairwise_rows, candidates, args.top_decisions)
    generator_rows = _summarize_by(decisions, "source_generator")
    lane_rows = _summarize_by(decisions, "factor_lane")
    total_eval_rows = sum(int(shard.get("eval_rows") or 0) for shard in meta["shards"])
    followup_count = sum(1 for row in decisions if row.get("phase3bp_decision") == "bp_followup_priority")
    best_followup = next((row for row in decisions if row.get("phase3bp_decision") == "bp_followup_priority"), None)
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3BP_TRUE1MIN_SEARCH_ALGORITHM_SMOKE_COMPLETE_DIAGNOSTIC_ONLY",
        "generator_mode": f"true1min_{args.algorithm_mode}_smoke",
        "algorithm_mode": args.algorithm_mode,
        "include_residual": bool(args.include_residual),
        "cem_population_size": args.cem_population_size,
        "cem_elite_frac": args.cem_elite_frac,
        "cem_rounds": args.cem_rounds,
        "candidate_count": len(candidates),
        "blocked_hash_count": len(blocked),
        "panel_count": len(panels),
        "schema_bound_generation": True,
        "available_field_count": len(available_fields),
        "available_fields": available_fields,
        "sample_trade_times_per_shard": args.sample_trade_times_per_shard,
        "horizons_min": list(horizons),
        "total_eval_rows": total_eval_rows,
        "followup_priority_count": followup_count,
        "best_followup": best_followup,
        "policy": policy,
        "output_root": str(output_root),
        "report_root": str(report_root),
        "hard_boundary": [
            "true trade_time minute panels only",
            "old daily stock-PIT default dataset not used",
            "search algorithm smoke only, not production proof",
            "future wrong-lag is a hard blocker",
            "X0/R3 read-only",
        ],
        **meta,
    }
    _write_csv(output_root / "phase3bp_candidate_pack.csv", candidates)
    _write_csv(output_root / "phase3bp_candidate_horizon_shard_metrics.csv", metric_rows)
    _write_csv(output_root / "phase3bp_candidate_horizon_aggregate.csv", aggregate_rows)
    _write_csv(output_root / "phase3bp_pairwise_signal_rank_corr.csv", pairwise_rows)
    _write_csv(output_root / "phase3bp_top_decisions.csv", decisions)
    _write_json(output_root / "phase3bp_true1min_search_algorithm_summary.json", summary)
    _write_csv(report_root / "phase3bp_candidate_pack.csv", candidates)
    _write_csv(report_root / "phase3bp_top_decisions.csv", decisions)
    _write_csv(report_root / "phase3bp_generator_summary.csv", generator_rows)
    _write_csv(report_root / "phase3bp_lane_summary.csv", lane_rows)
    _write_json(report_root / "phase3bp_true1min_search_algorithm_summary.json", {**summary, "top_decisions": decisions[:12]})
    (report_root / "PHASE3BP_TRUE1MIN_SEARCH_ALGORITHM_SMOKE_20260615.md").write_text(
        _render_md(summary, generator_rows, lane_rows, decisions),
        encoding="utf-8",
    )
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
