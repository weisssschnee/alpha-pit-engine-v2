"""Run the fixed-budget CN B1S development-only CANARY."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
import time
import traceback
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from our_system_phase2.runtime.nextgen_dark_development_canary import (
    CONTEXT_FIELDS,
    EPS,
    EVENT_FIELDS,
    FIRSTN_FIELDS,
    RAW_FIELDS,
    STATE_FIELDS,
    WINDOWS,
    _atomic_csv,
    _event_expression,
    _frame_hash,
    _git,
    _orthogonal_expression,
    _runtime_environment,
    _sha256,
    _state_expression,
    _static_expression,
    _temporal_expression,
)
from our_system_phase2.services import development_only_data_access
from our_system_phase2.services import feature_state_fabric as feature_state_fabric_module
from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    _fields,
    _future_returns,
)
from our_system_phase2.services.atomic_checkpoint import atomic_write_json, durable_flush
from our_system_phase2.services.deterministic_signal_sketch import (
    build_signal_sketch,
    cluster_distribution,
    cluster_sketches,
    projection_matrix,
)
from our_system_phase2.services.expression_semantics import analyze_expression
from our_system_phase2.services.feature_state_fabric import FeatureStateFabric, FieldRegistry
from our_system_phase2.services.development_only_data_access import (
    cache_provenance,
    initialize_cache_root,
    read_development_panel,
    validate_development_release,
)
from our_system_phase2.services.real_market_validation import (
    UnsupportedExpressionError,
    _ExpressionEvaluationContext,
    _cached_group_layout,
    fast_rank_pct_by_group,
    evaluate_panel_expression,
)
from our_system_phase2.services.typed_primitive_gate import validate_expression


EXPERIMENT_ID = "cn_b1s_development_canary"
AUTHORIZATION = "USER_AUTHORIZED_CN_B1S_DEVELOPMENT_ONLY_CANARY_20260712"
ADAPTIVE_LANES = ("cem", "rx_ucb", "uct_mcts", "evolutionary", "surrogate")
PLATE_TOKENS = ("plate", "industry", "membership", "sector")
MOTIFS = (
    "delta",
    "reversal",
    "volatility",
    "path",
    "relative",
    "multiscale",
    "acceleration",
    "signed_context",
)


def _hash_text(value: str, length: int = 64) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def _canonical(expression: str) -> tuple[str, str]:
    value = analyze_expression(expression).canonical_expression
    return value, _hash_text(value)


def _atomic_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temporary = Path(name)
    try:
        temporary.write_text(text, encoding="utf-8")
        with temporary.open("r+b") as handle:
            durable_flush(handle)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _json_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def _materialization_input_columns(registry: FieldRegistry, fields: Iterable[str]) -> list[str]:
    """Include registry-declared PIT clock evidence without exposing it to candidate generation."""

    requested = set(fields)
    for field in tuple(requested):
        spec = registry.get(field)
        requested.update(spec.source_fields)
        if spec.observable_time_field:
            requested.add(spec.observable_time_field)
        if spec.source_session_field:
            requested.add(spec.source_session_field)
    return sorted(requested)


def _candidate(
    lane_id: str,
    index: int,
    expression: str,
    *,
    origin: str,
    stage: str,
    motif: str,
    seed: int,
    parent_id: str = "",
    benchmark_id: str = "",
    llm_action: str = "",
) -> dict[str, Any]:
    canonical_expression, canonical_identity = _canonical(expression)
    fields = sorted(_fields(expression))
    lowered = expression.lower()
    if any(token in lowered for token in ("validation", "holdout", "forward", "2026", "label", "winner")):
        raise ValueError(f"forbidden expression token: {expression}")
    if any(any(token in field.lower() for token in PLATE_TOKENS) for field in fields):
        raise ValueError(f"plate/industry field entered B1S: {expression}")
    family = "mixed"
    if fields and all(field.startswith("m1_first") for field in fields):
        family = "firstN"
    elif fields and all(field.startswith("evt_") for field in fields):
        family = "event_state"
    elif fields and all(field.startswith("ctx_") for field in fields):
        family = "lagged_context"
    elif fields and all(not field.startswith(("ctx_", "evt_", "m1_first")) for field in fields):
        family = "raw_1min"
    return {
        "candidate_id": f"b1s_{lane_id}_{index + 1:04d}",
        "lane_id": lane_id,
        "expression": expression,
        "canonical_expression": canonical_expression,
        "exact_identity": _hash_text(expression),
        "canonical_identity": canonical_identity,
        "lineage_key": f"b1s/{lane_id}:{stage}:{index + 1:04d}",
        "parent_id": parent_id,
        "proposal_stage": stage,
        "proposal_origin": origin,
        "motif": motif,
        "field_family": family,
        "family_id": f"{lane_id}:{motif}",
        "semantic_bucket": f"{lane_id}:{motif}:{family}",
        "fields_list": fields,
        "lane_seed": int(seed),
        "benchmark_id": benchmark_id,
        "llm_action": llm_action,
        "data_role": "development",
    }


def _firstn_expression(index: int) -> tuple[str, str]:
    firstn = FIRSTN_FIELDS[index % len(FIRSTN_FIELDS)]
    raw = RAW_FIELDS[(index // len(FIRSTN_FIELDS)) % len(RAW_FIELDS)]
    window = WINDOWS[(index // (len(FIRSTN_FIELDS) * len(RAW_FIELDS))) % len(WINDOWS)]
    variants = (
        (f"CSRank(${firstn})", "firstn_level"),
        (f"CSRank(Mul(${firstn},Sign(${raw})))", "firstn_signed_path"),
        (f"CSRank(Sub(${firstn},Mean(${raw},{window})))", "firstn_intraday_gap"),
        (f"CSRank(Add(${firstn},PathShape(${raw},{window})))", "firstn_path_shape"),
    )
    return variants[(index // 7) % len(variants)]


def _structural_spec(index: int, salt: int) -> dict[str, Any]:
    a_index = (index * 5 + salt) % len(RAW_FIELDS)
    b_index = (index * 7 + salt // 3 + 1) % len(RAW_FIELDS)
    context_index = (index * 11 + salt) % len(CONTEXT_FIELDS)
    window_index = (index * 3 + salt) % len(WINDOWS)
    motif_index = (index + salt) % len(MOTIFS)
    mix_index = (index * 13 + salt) % 17
    return {
        "a": RAW_FIELDS[a_index],
        "b": RAW_FIELDS[b_index],
        "context": CONTEXT_FIELDS[context_index],
        "window": WINDOWS[window_index],
        "motif": MOTIFS[motif_index],
        "a_index": a_index,
        "b_index": b_index,
        "context_index": context_index,
        "window_index": window_index,
        "motif_index": motif_index,
        "mix_index": mix_index,
        "mix": round((mix_index + 1) / 100.0, 4),
    }


def _structural_expression(spec: Mapping[str, Any]) -> str:
    a, b, context = spec["a"], spec["b"], spec["context"]
    window = int(spec["window"])
    motif = str(spec["motif"])
    mix = float(spec["mix"])
    if motif == "delta":
        return f"CSRank(Add(Delta(${a},{window}),Mul(${b},{mix})))"
    if motif == "reversal":
        return f"CSRank(Sub(Neg(Delta(${a},{window})),Mul(${b},{mix})))"
    if motif == "volatility":
        return f"CSRank(Sub(ZScore(Std(${a},{window})),Mul(ZScore(Mean(${b},{window})),{mix})))"
    if motif == "path":
        return f"CSRank(Add(PathShape(${a},{window}),Mul(DrawdownPath(${b},{window}),{1.0 + mix})))"
    if motif == "relative":
        return f"CSRank(Div(Delta(${a},{window}),Add(Abs(Std(${b},{window})),{mix + float(EPS)})))"
    if motif == "multiscale":
        short = max(2, min(window, 5))
        long = max(short + 1, min(30, short * 3))
        return f"CSRank(Add(MultiScaleRelation(${a},${b},{short},{long}),Mul(Delta(${b},{short}),{mix})))"
    if motif == "acceleration":
        return f"CSRank(Sub(Acceleration(${a},{window}),Mul(Slope(${b},{window}),{mix})))"
    return f"CSRank(Add(Mul(Sign(${context}),Delta(${a},{window})),Mul(${b},{mix})))"


def _typed_random_expression(index: int, seed: int) -> tuple[str, str]:
    rng = np.random.default_rng(seed + index)
    spec = _structural_spec(int(rng.integers(0, 100000)), seed)
    return _structural_expression(spec), f"typed_random_{spec['motif']}"


def _typed_ast_expression(index: int, seed: int) -> tuple[str, str]:
    left = _structural_spec(index, seed)
    right = _structural_spec(index + 131, seed + 17)
    left_expr = _structural_expression(left)
    right_expr = _structural_expression(right)
    variants = (
        f"CSRank(Sub({left_expr},{right_expr}))",
        f"CSRank(Add({left_expr},Mul(Sign(${left['context']}),{right_expr})))",
        f"CSRank(Div({left_expr},Add(Abs({right_expr}),{EPS})))",
    )
    return variants[index % len(variants)], f"typed_ast_{left['motif']}_{right['motif']}"


def _llm_expression(index: int, repair: bool) -> tuple[str, str, str]:
    a = RAW_FIELDS[(index * 3 + 1) % len(RAW_FIELDS)]
    b = RAW_FIELDS[(index * 5 + 2) % len(RAW_FIELDS)]
    context = CONTEXT_FIELDS[(index * 7 + 3) % len(CONTEXT_FIELDS)]
    window = WINDOWS[(index * 2 + 1) % len(WINDOWS)]
    if repair:
        expression = f"CSRank(Div(Sub(PathShape(${a},{window}),Slope(${b},{window})),Add(Abs(${context}),{EPS})))"
        return expression, "llm_repaired_safe_division", "repair"
    expression = f"CSRank(Add(RecoveryPath(${a},{window}),Mul(Sign(${context}),Delta(${b},{max(2, window // 2)}))))"
    return expression, "llm_frozen_path_context", "proposal"


def _benchmark_expression(index: int) -> tuple[str, str, str]:
    benchmark_ids = (
        "x0_r3",
        "simple_momentum",
        "simple_reversal",
        "volatility",
        "liquidity",
        "firstN",
        "event_state",
        "external_competitor_reproduction",
    )
    benchmark = benchmark_ids[index % len(benchmark_ids)]
    window = WINDOWS[(index // len(benchmark_ids)) % len(WINDOWS)]
    if benchmark == "x0_r3":
        expr = f"CSRank(ZScore(Mean(Abs(Delta($vwap,1)),{window})))"
    elif benchmark == "simple_momentum":
        expr = f"CSRank(Delta($close,{window}))"
    elif benchmark == "simple_reversal":
        expr = f"CSRank(Neg(Delta($close,{window})))"
    elif benchmark == "volatility":
        expr = f"CSRank(Std($ret_1m,{window}))"
    elif benchmark == "liquidity":
        expr = f"CSRank(Mean($amount_yuan,{window}))"
    elif benchmark == "firstN":
        field = FIRSTN_FIELDS[(index // len(benchmark_ids)) % len(FIRSTN_FIELDS)]
        expr = f"CSRank(${field})"
    elif benchmark == "event_state":
        expr = f"CSRank(EventCount($evt_uplimit_active,{window}))"
    else:
        expr = f"CSRank(Sub(ZScore(Delta($vwap,{window})),ZScore(Std($ret_1m,{window}))))"
    return expr, f"benchmark_{benchmark}", benchmark


def generate_initial_proposals(contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    specs = contract["lane_specs"]
    seeds = contract["seeds"]
    rows: list[dict[str, Any]] = []
    fixed_lanes = (
        "static_cross_sectional",
        "firstn_intraday_path",
        "temporal_program",
        "event_conditioned",
        "state_transition",
        "orthogonal_exile",
        "typed_random",
        "typed_ast",
        "llm_proposal_repair",
        "benchmark_competitor",
    )
    for lane in fixed_lanes:
        quota = int(specs[lane]["proposal"])
        seed = int(seeds[lane])
        for index in range(quota):
            benchmark_id = ""
            llm_action = ""
            if lane == "static_cross_sectional":
                expression, motif, _ = _static_expression("raw" if index % 2 == 0 else "context", index)
            elif lane == "firstn_intraday_path":
                expression, motif = _firstn_expression(index)
            elif lane == "temporal_program":
                expression, motif, _ = _temporal_expression("single_scale" if index % 2 == 0 else "multi_scale", index)
            elif lane == "event_conditioned":
                expression, motif, _ = _event_expression(("limit", "firstN", "context")[index % 3], index)
            elif lane == "state_transition":
                expression, motif, _ = _state_expression(("duration", "transition", "confirmation")[index % 3], index)
            elif lane == "orthogonal_exile":
                expression, motif, _ = _orthogonal_expression("orthogonal" if index % 2 == 0 else "exile", index)
            elif lane == "typed_random":
                expression, motif = _typed_random_expression(index, seed)
            elif lane == "typed_ast":
                expression, motif = _typed_ast_expression(index, seed)
            elif lane == "llm_proposal_repair":
                expression, motif, llm_action = _llm_expression(index % 24, repair=index >= 24)
            else:
                expression, motif, benchmark_id = _benchmark_expression(index)
            rows.append(
                _candidate(
                    lane,
                    index,
                    expression,
                    origin=("codex_gpt5_frozen_pre_reward" if lane == "llm_proposal_repair" else "frozen_nonadaptive"),
                    stage=(llm_action or "fixed"),
                    motif=motif,
                    seed=seed,
                    benchmark_id=benchmark_id,
                    llm_action=llm_action,
                )
            )
    for lane in ADAPTIVE_LANES:
        seed = int(seeds[lane])
        control = int(specs[lane]["control"])
        for index in range(control):
            spec = _structural_spec(index, seed)
            rows.append(
                _candidate(
                    lane,
                    index,
                    _structural_expression(spec),
                    origin="matched_nonadaptive_control",
                    stage="control",
                    motif=str(spec["motif"]),
                    seed=seed,
                )
            )
    return rows


def _structural_vector(spec: Mapping[str, Any]) -> list[float]:
    return [
        float(spec["a_index"]),
        float(spec["b_index"]),
        float(spec["context_index"]),
        float(spec["window_index"]),
        float(spec["motif_index"]),
        float(spec["mix_index"]),
    ]


def _control_spec(row: Mapping[str, Any], seed: int, index: int) -> dict[str, Any]:
    spec = _structural_spec(index, seed)
    spec["reward"] = float(row.get("proxy_reward") or 0.0)
    spec["candidate_id"] = str(row["candidate_id"])
    return spec


def generate_adaptive_proposals(
    contract: Mapping[str, Any],
    initial_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    specs = contract["lane_specs"]
    seeds = contract["seeds"]
    output: list[dict[str, Any]] = []
    for lane in ADAPTIVE_LANES:
        seed = int(seeds[lane])
        adaptive_quota = int(specs[lane]["adaptive"])
        controls = [row for row in initial_rows if row["lane_id"] == lane]
        control_specs = [_control_spec(row, seed, index) for index, row in enumerate(controls)]
        pool = [_structural_spec(1000 + index, seed + 31) for index in range(max(256, adaptive_quota * 6))]
        rewards = np.asarray([float(spec["reward"]) for spec in control_specs], dtype=float)
        elite_count = max(4, int(math.ceil(len(control_specs) * 0.20)))
        elite_order = np.argsort(-rewards, kind="stable")[:elite_count]
        elites = [control_specs[int(index)] for index in elite_order]

        if lane == "cem":
            motif_counts = Counter(spec["motif"] for spec in elites)
            window_counts = Counter(int(spec["window_index"]) for spec in elites)
            ranked = sorted(
                pool,
                key=lambda spec: (
                    -(motif_counts[str(spec["motif"])] + 1) * (window_counts[int(spec["window_index"])] + 1),
                    _hash_text(json.dumps(spec, sort_keys=True)),
                ),
            )
        elif lane == "rx_ucb":
            by_arm: dict[str, list[float]] = defaultdict(list)
            for spec in control_specs:
                by_arm[str(spec["motif"])].append(float(spec["reward"]))
            total = max(1, len(control_specs))
            ucb = {
                arm: float(np.mean(values)) + math.sqrt(2.0 * math.log(total + 1) / len(values))
                for arm, values in by_arm.items()
            }
            ranked = sorted(pool, key=lambda spec: (-ucb.get(str(spec["motif"]), 0.0), _hash_text(json.dumps(spec, sort_keys=True))))
        elif lane == "uct_mcts":
            branch_values: dict[tuple[str, int], list[float]] = defaultdict(list)
            for spec in control_specs:
                branch_values[(str(spec["motif"]), int(spec["window_index"]) // 2)].append(float(spec["reward"]))
            total = max(1, len(control_specs))
            uct = {
                branch: float(np.mean(values)) + math.sqrt(2.0 * math.log(total + 1) / len(values))
                for branch, values in branch_values.items()
            }
            ranked = sorted(
                pool,
                key=lambda spec: (
                    -uct.get((str(spec["motif"]), int(spec["window_index"]) // 2), 0.0),
                    _hash_text(json.dumps(spec, sort_keys=True)),
                ),
            )
        elif lane == "evolutionary":
            def similarity(spec: Mapping[str, Any]) -> tuple[float, str]:
                score = max(
                    (
                        3.0 * float(spec["motif"] == elite["motif"])
                        + 2.0 * float(spec["a"] == elite["a"])
                        + 1.0 * float(abs(int(spec["window_index"]) - int(elite["window_index"])) <= 1)
                        + float(elite["reward"])
                    )
                    for elite in elites
                )
                return -score, _hash_text(json.dumps(spec, sort_keys=True))
            ranked = sorted(pool, key=similarity)
        else:
            model = RandomForestRegressor(
                n_estimators=64,
                max_depth=4,
                random_state=seed,
                n_jobs=1,
            )
            model.fit(np.asarray([_structural_vector(spec) for spec in control_specs]), rewards)
            predictions = model.predict(np.asarray([_structural_vector(spec) for spec in pool]))
            ranked = [pool[int(index)] for index in np.argsort(-predictions, kind="stable")]

        seen = {str(row["canonical_identity"]) for row in initial_rows + output}
        selected: list[dict[str, Any]] = []
        for spec in ranked:
            expression = _structural_expression(spec)
            _, identity = _canonical(expression)
            if identity in seen:
                continue
            parent = str(elites[len(selected) % len(elites)]["candidate_id"])
            row = _candidate(
                lane,
                int(specs[lane]["control"]) + len(selected),
                expression,
                origin=f"ephemeral_{lane}_adaptive",
                stage="adaptive",
                motif=str(spec["motif"]),
                seed=seed,
                parent_id=parent,
            )
            selected.append(row)
            seen.add(identity)
            if len(selected) >= adaptive_quota:
                break
        if len(selected) != adaptive_quota:
            raise RuntimeError(f"adaptive proposal underfill before evaluation: {lane}")
        output.extend(selected)
    return output


def apply_typed_gate(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        verdict = validate_expression(
            str(row["expression"]),
            entry_lineage=str(row["lineage_key"]),
            materialization_stage="cn_b1s_canary_generation",
            candidate_role="research_canary",
        )
        row["typed_gate_decision"] = verdict.typed_gate_decision
        row["typed_gate_reason"] = verdict.typed_gate_reason
        row["legal"] = verdict.typed_gate_decision == "allow"


def fast_group_ic(
    signal_rank: np.ndarray,
    label_rank: np.ndarray,
    group_codes: np.ndarray,
    *,
    min_obs: int,
) -> dict[str, Any]:
    x = np.asarray(signal_rank, dtype=float)
    y = np.asarray(label_rank, dtype=float)
    g = np.asarray(group_codes, dtype=np.int64)
    valid = np.isfinite(x) & np.isfinite(y) & (g >= 0)
    if not bool(valid.any()):
        return {"ic_mean": None, "ic_abs_mean": None, "ic_count": 0, "ic_hit_rate": None}
    xv, yv, gv = x[valid], y[valid], g[valid]
    size = int(gv.max()) + 1
    n = np.bincount(gv, minlength=size).astype(float)
    sx = np.bincount(gv, weights=xv, minlength=size)
    sy = np.bincount(gv, weights=yv, minlength=size)
    sxx = np.bincount(gv, weights=xv * xv, minlength=size)
    syy = np.bincount(gv, weights=yv * yv, minlength=size)
    sxy = np.bincount(gv, weights=xv * yv, minlength=size)
    cov = sxy - sx * sy / np.maximum(n, 1.0)
    vx = sxx - sx * sx / np.maximum(n, 1.0)
    vy = syy - sy * sy / np.maximum(n, 1.0)
    eligible = (n >= min_obs) & (vx > 1e-15) & (vy > 1e-15)
    corr = cov[eligible] / np.sqrt(vx[eligible] * vy[eligible])
    corr = corr[np.isfinite(corr)]
    if not len(corr):
        return {"ic_mean": None, "ic_abs_mean": None, "ic_count": 0, "ic_hit_rate": None}
    return {
        "ic_mean": float(corr.mean()),
        "ic_abs_mean": float(np.abs(corr).mean()),
        "ic_count": int(len(corr)),
        "ic_hit_rate": float(np.mean(corr > 0.0)),
    }


def fast_group_spread(
    signal_rank: np.ndarray,
    label: np.ndarray,
    group_codes: np.ndarray,
    *,
    min_obs: int,
) -> dict[str, Any]:
    rank = np.asarray(signal_rank, dtype=float)
    y = np.asarray(label, dtype=float)
    g = np.asarray(group_codes, dtype=np.int64)
    valid = np.isfinite(rank) & np.isfinite(y) & (g >= 0)
    if not bool(valid.any()):
        return {"spread_mean": None, "spread_abs_mean": None, "spread_count": 0, "spread_hit_rate": None}
    size = int(g[valid].max()) + 1
    total = np.bincount(g[valid], minlength=size)
    top = valid & (rank >= 0.8)
    bottom = valid & (rank <= 0.2)
    nt = np.bincount(g[top], minlength=size)
    nb = np.bincount(g[bottom], minlength=size)
    st = np.bincount(g[top], weights=y[top], minlength=size)
    sb = np.bincount(g[bottom], weights=y[bottom], minlength=size)
    eligible = (total >= min_obs) & (nt > 0) & (nb > 0)
    spread = st[eligible] / nt[eligible] - sb[eligible] / nb[eligible]
    spread = spread[np.isfinite(spread)]
    if not len(spread):
        return {"spread_mean": None, "spread_abs_mean": None, "spread_count": 0, "spread_hit_rate": None}
    return {
        "spread_mean": float(spread.mean()),
        "spread_abs_mean": float(np.abs(spread).mean()),
        "spread_count": int(len(spread)),
        "spread_hit_rate": float(np.mean(spread > 0.0)),
    }


def fast_turnover(signal_rank: np.ndarray, frame: pd.DataFrame) -> dict[str, Any]:
    work = pd.DataFrame(
        {
            "code": frame["code"].astype(str).to_numpy(),
            "trade_time": frame["trade_time"].to_numpy(),
            "rank": np.asarray(signal_rank, dtype=float),
        }
    ).dropna(subset=["rank"])
    if work.empty:
        return {"mean_one_way_turnover": None, "turnover_count": 0}
    codes = sorted(work["code"].unique())
    top = work.assign(value=(work["rank"] >= 0.8)).pivot_table(index="trade_time", columns="code", values="value", aggfunc="max", fill_value=False).reindex(columns=codes, fill_value=False).to_numpy(bool)
    bottom = work.assign(value=(work["rank"] <= 0.2)).pivot_table(index="trade_time", columns="code", values="value", aggfunc="max", fill_value=False).reindex(columns=codes, fill_value=False).to_numpy(bool)
    if len(top) < 2:
        return {"mean_one_way_turnover": None, "turnover_count": 0}
    top_n = top[1:].sum(axis=1)
    bottom_n = bottom[1:].sum(axis=1)
    eligible = (top_n > 0) & (bottom_n > 0)
    if not bool(eligible.any()):
        return {"mean_one_way_turnover": None, "turnover_count": 0}
    top_overlap = (top[1:] & top[:-1]).sum(axis=1)
    bottom_overlap = (bottom[1:] & bottom[:-1]).sum(axis=1)
    values = 0.5 * (1.0 - top_overlap[eligible] / top_n[eligible] + 1.0 - bottom_overlap[eligible] / bottom_n[eligible])
    return {"mean_one_way_turnover": float(values.mean()), "turnover_count": int(len(values))}


def _coordinate_rows(frame: pd.DataFrame, indices: np.ndarray) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for position in indices:
        timestamp = pd.Timestamp(frame.iloc[int(position)]["trade_time"])
        minute = timestamp.hour * 60 + timestamp.minute
        if minute < 600:
            period = "open"
        elif minute < 780:
            period = "midday"
        else:
            period = "close"
        code = str(frame.iloc[int(position)]["code"])
        rows.append(
            {
                "coordinate_id": f"{timestamp.isoformat()}|{code}",
                "trade_month": timestamp.strftime("%Y-%m"),
                "intraday_period": period,
                "stock_coverage_interval": str(int(hashlib.sha1(code.encode()).hexdigest()[:4], 16) % 8),
                "listing_age_bucket": "not_used_for_selection",
                "activation_density_bucket": "post_materialization_profile_only",
            }
        )
    return rows


def _proxy_mask(frame: pd.DataFrame, stride: int) -> np.ndarray:
    times = pd.Index(sorted(pd.to_datetime(frame["trade_time"]).unique()))
    selected = set(times[:: max(1, int(stride))])
    return pd.to_datetime(frame["trade_time"]).isin(selected).to_numpy()


def _sketch_indices(frame: pd.DataFrame, mask: np.ndarray, count: int) -> np.ndarray:
    available = np.flatnonzero(mask)
    if not len(available):
        raise ValueError("proxy coordinate mask is empty")
    if len(available) <= count:
        return available
    positions = np.linspace(0, len(available) - 1, count, dtype=int)
    return available[positions]


def evaluate_proxy_rows(
    rows: list[dict[str, Any]],
    frame: pd.DataFrame,
    label_rank: np.ndarray,
    *,
    proxy_mask: np.ndarray,
    coordinate_indices: np.ndarray,
    coordinate_rows: list[dict[str, str]],
    projection: np.ndarray,
    min_obs: int,
    survivor_contract: Mapping[str, Any],
    signal_store: dict[str, np.ndarray],
    metric_store: dict[str, dict[str, Any]],
    sketch_store: dict[str, dict[str, Any]],
    evaluation_context: _ExpressionEvaluationContext,
    cross_layout: Any,
) -> None:
    cross_key = frame["trade_time"]
    proxy_groups, proxy_uniques = pd.factorize(cross_key[proxy_mask], sort=False)
    expected_groups = len(proxy_uniques)
    canonical_owner: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not bool(row.get("legal")):
            row.update(
                {
                    "materialized": False,
                    "survivor": False,
                    "evaluation_error": "typed_gate_rejected",
                    "proxy_reward": None,
                }
            )
            continue
        identity = str(row["canonical_identity"])
        if identity in metric_store:
            row.update(metric_store[identity])
            canonical_owner.setdefault(identity, row)
            continue
        try:
            signal = pd.to_numeric(
                evaluate_panel_expression(
                    frame,
                    str(row["expression"]),
                    cache={},
                    data_role="development",
                    _evaluation_context=evaluation_context,
                ),
                errors="coerce",
            )
            rank = fast_rank_pct_by_group(signal, cross_key, layout=cross_layout)
            signal_values = signal.to_numpy(dtype=np.float32, copy=True)
            rank_values = rank.to_numpy(dtype=np.float32, copy=False)
            proxy_ic = fast_group_ic(
                rank_values[proxy_mask],
                label_rank[proxy_mask],
                proxy_groups,
                min_obs=min_obs,
            )
            finite = np.isfinite(signal_values)
            finite_ratio = float(finite.mean())
            signal_unique = int(np.unique(signal_values[finite]).size) if bool(finite.any()) else 0
            uniqueness_factor = min(1.0, math.log1p(signal_unique) / math.log(65.0))
            ic_count = int(proxy_ic["ic_count"])
            ic_value = proxy_ic["ic_mean"]
            proxy_reward = (
                abs(float(ic_value))
                * math.sqrt(ic_count / max(1, expected_groups))
                * finite_ratio
                * uniqueness_factor
                if ic_value is not None
                else 0.0
            )
            survivor = (
                finite_ratio >= float(survivor_contract["minimum_finite_ratio"])
                and signal_unique >= int(survivor_contract["minimum_signal_unique"])
                and ic_count >= int(survivor_contract["minimum_eligible_cross_sections"])
                and proxy_reward >= float(survivor_contract["minimum_proxy_reward"])
            )
            sketch = {
                "candidate_id": str(row["candidate_id"]),
                **build_signal_sketch(
                    signal_values[coordinate_indices].astype(float),
                    rank_values[coordinate_indices].astype(float),
                    coordinate_rows,
                    coordinate_set="B1S_PROXY",
                    projection=projection,
                ),
            }
            metrics = {
                "materialized": True,
                "evaluation_error": "",
                "proxy_ic_mean": ic_value,
                "proxy_ic_abs_mean": proxy_ic["ic_abs_mean"],
                "proxy_ic_count": ic_count,
                "proxy_ic_hit_rate": proxy_ic["ic_hit_rate"],
                "proxy_finite_ratio": finite_ratio,
                "proxy_signal_unique": signal_unique,
                "proxy_reward": float(proxy_reward),
                "survivor": bool(survivor),
            }
            row.update(metrics)
            signal_store[identity] = signal_values
            metric_store[identity] = metrics
            sketch_store[identity] = sketch
            canonical_owner[identity] = row
        except (UnsupportedExpressionError, ValueError, TypeError, FloatingPointError) as exc:
            metrics = {
                "materialized": False,
                "evaluation_error": f"{type(exc).__name__}:{exc}",
                "proxy_ic_mean": None,
                "proxy_ic_abs_mean": None,
                "proxy_ic_count": 0,
                "proxy_ic_hit_rate": None,
                "proxy_finite_ratio": 0.0,
                "proxy_signal_unique": 0,
                "proxy_reward": 0.0,
                "survivor": False,
            }
            row.update(metrics)
            metric_store[identity] = metrics

    # Duplicates reuse their canonical owner's signal behavior and sketch.
    for row in rows:
        identity = str(row["canonical_identity"])
        if identity in metric_store:
            row.update(metric_store[identity])


def assign_signal_clusters(
    rows: list[dict[str, Any]],
    sketch_store: Mapping[str, dict[str, Any]],
    contract: Mapping[str, Any],
) -> dict[str, int]:
    representatives: list[dict[str, Any]] = []
    owner_by_identity: dict[str, str] = {}
    for row in sorted(rows, key=lambda item: str(item["candidate_id"])):
        identity = str(row["canonical_identity"])
        sketch = sketch_store.get(identity)
        if sketch is None or identity in owner_by_identity:
            continue
        payload = dict(sketch)
        payload["candidate_id"] = str(row["candidate_id"])
        representatives.append(payload)
        owner_by_identity[identity] = str(row["candidate_id"])
    cluster_contract = contract["signal_cluster_contract"]
    labels = cluster_sketches(
        representatives,
        rank_threshold=float(cluster_contract["rank_threshold"]),
        value_threshold=float(cluster_contract["value_threshold"]),
        mask_threshold=float(cluster_contract["mask_threshold"]),
        activation_threshold=float(cluster_contract["activation_threshold"]),
    )
    by_identity = {
        identity: labels[candidate_id]
        for identity, candidate_id in owner_by_identity.items()
    }
    for row in rows:
        row["signal_cluster_id"] = int(by_identity.get(str(row["canonical_identity"]), 0))
    return labels


def _eligible_representatives(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    owners: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not bool(row.get("survivor")) or int(row.get("signal_cluster_id") or 0) <= 0:
            continue
        identity = str(row["canonical_identity"])
        current = owners.get(identity)
        if current is None or float(row.get("proxy_reward") or 0.0) > float(current.get("proxy_reward") or 0.0):
            owners[identity] = row
    return list(owners.values())


def _select_with_constraints(
    ordered: Iterable[dict[str, Any]],
    *,
    cap: int,
    existing: Iterable[dict[str, Any]] = (),
    family_cap: int,
    bucket_cap: int,
    parent_cap: int,
) -> list[dict[str, Any]]:
    selected = list(existing)
    ids = {str(row["candidate_id"]) for row in selected}
    clusters = {int(row["signal_cluster_id"]) for row in selected}
    family = Counter(str(row["family_id"]) for row in selected)
    bucket = Counter(str(row["semantic_bucket"]) for row in selected)
    parents = Counter(str(row.get("parent_id") or "") for row in selected if str(row.get("parent_id") or ""))
    for row in ordered:
        candidate_id = str(row["candidate_id"])
        cluster = int(row["signal_cluster_id"])
        family_id = str(row["family_id"])
        semantic_bucket = str(row["semantic_bucket"])
        parent = str(row.get("parent_id") or "")
        if len(selected) >= cap:
            break
        if candidate_id in ids or cluster in clusters:
            continue
        if family[family_id] >= family_cap or bucket[semantic_bucket] >= bucket_cap:
            continue
        if parent and parents[parent] >= parent_cap:
            continue
        selected.append(row)
        ids.add(candidate_id)
        clusters.add(cluster)
        family[family_id] += 1
        bucket[semantic_bucket] += 1
        if parent:
            parents[parent] += 1
    return selected


def build_admissions(rows: list[dict[str, Any]], contract: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    candidates = _eligible_representatives(rows)
    ordered = sorted(candidates, key=lambda row: (-float(row["proxy_reward"]), str(row["candidate_id"])))
    admission = contract["admission_contract"]
    total = int(contract["budgets"]["admission_total"])
    kwargs = {
        "family_cap": int(admission["family_cap"]),
        "bucket_cap": int(admission["semantic_bucket_cap"]),
        "parent_cap": int(admission["parent_descendant_cap"]),
    }

    stratified: list[dict[str, Any]] = []
    for lane, spec in contract["lane_specs"].items():
        lane_rows = [row for row in ordered if row["lane_id"] == lane]
        quota = int(spec["admission"])
        target = len(stratified) + quota
        if lane in ADAPTIVE_LANES:
            half = quota // 2
            control = [row for row in lane_rows if row["proposal_stage"] == "control"]
            adaptive = [row for row in lane_rows if row["proposal_stage"] == "adaptive"]
            stratified = _select_with_constraints(
                control, cap=len(stratified) + half, existing=stratified, **kwargs
            )
            stratified = _select_with_constraints(
                adaptive, cap=target, existing=stratified, **kwargs
            )
            stratified = _select_with_constraints(
                lane_rows, cap=target, existing=stratified, **kwargs
            )
        else:
            stratified = _select_with_constraints(
                lane_rows, cap=target, existing=stratified, **kwargs
            )

    global_top_k = _select_with_constraints(ordered, cap=total, **kwargs)

    hybrid_seed: list[dict[str, Any]] = []
    for lane, spec in contract["lane_specs"].items():
        lane_rows = [row for row in ordered if row["lane_id"] == lane]
        quota = int(spec["admission"]) // 2
        target = len(hybrid_seed) + quota
        if lane in ADAPTIVE_LANES:
            control = [row for row in lane_rows if row["proposal_stage"] == "control"]
            adaptive = [row for row in lane_rows if row["proposal_stage"] == "adaptive"]
            hybrid_seed = _select_with_constraints(
                control, cap=len(hybrid_seed) + quota // 2, existing=hybrid_seed, **kwargs
            )
            hybrid_seed = _select_with_constraints(
                adaptive, cap=target, existing=hybrid_seed, **kwargs
            )
            hybrid_seed = _select_with_constraints(
                lane_rows, cap=target, existing=hybrid_seed, **kwargs
            )
        else:
            hybrid_seed = _select_with_constraints(
                lane_rows, cap=target, existing=hybrid_seed, **kwargs
            )
    hybrid = _select_with_constraints(ordered, cap=total, existing=hybrid_seed, **kwargs)
    return {"stratified": stratified, "global_top_k": global_top_k, "hybrid": hybrid}


def build_strict_pack(admitted: list[dict[str, Any]], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    strict: list[dict[str, Any]] = []
    for lane, spec in contract["lane_specs"].items():
        quota = int(spec["strict"])
        lane_rows = sorted(
            [row for row in admitted if row["lane_id"] == lane],
            key=lambda row: (-float(row["proxy_reward"]), str(row["candidate_id"])),
        )
        selected: list[dict[str, Any]] = []
        if lane in ADAPTIVE_LANES:
            control = [row for row in lane_rows if row["proposal_stage"] == "control"]
            adaptive = [row for row in lane_rows if row["proposal_stage"] == "adaptive"]
            selected.extend(control[: quota // 2])
            selected.extend(adaptive[: quota - len(selected)])
        ids = {row["candidate_id"] for row in selected}
        selected.extend(row for row in lane_rows if row["candidate_id"] not in ids)
        strict.extend(selected[:quota])
    return strict


def _stage_distribution(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(rows)
    labels = [int(row.get("signal_cluster_id") or 0) for row in rows if int(row.get("signal_cluster_id") or 0) > 0]
    metrics = cluster_distribution(labels)
    metrics.update(
        {
            "count": len(rows),
            "lane_count": len({str(row["lane_id"]) for row in rows}),
            "mean_proxy_reward": float(np.mean([float(row.get("proxy_reward") or 0.0) for row in rows])) if rows else None,
            "median_proxy_reward": float(np.median([float(row.get("proxy_reward") or 0.0) for row in rows])) if rows else None,
        }
    )
    return metrics


def lane_funnel(rows: list[dict[str, Any]], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    output = []
    for lane in contract["lane_specs"]:
        lane_rows = [row for row in rows if row["lane_id"] == lane]
        legal = [row for row in lane_rows if bool(row.get("legal"))]
        survivors = [row for row in lane_rows if bool(row.get("survivor"))]
        cluster_metrics = _stage_distribution(survivors)
        output.append(
            {
                "lane_id": lane,
                "proposal_count": len(lane_rows),
                "legal_count": len(legal),
                "canonical_count": len({str(row["canonical_identity"]) for row in legal}),
                "exact_count": len({str(row["exact_identity"]) for row in legal}),
                "materialized_count": sum(bool(row.get("materialized")) for row in legal),
                "survivor_count": len(survivors),
                "legal_conversion": len(legal) / max(1, len(lane_rows)),
                "survivor_conversion": len(survivors) / max(1, len(lane_rows)),
                "signal_cluster_count": cluster_metrics["cluster_count"],
                "n_eff": cluster_metrics["n_eff"],
                "top1_cluster_share": cluster_metrics["top1_share"],
                "top3_cluster_share": cluster_metrics["top3_share"],
            }
        )
    return output


def temporal_cluster_increment(rows: list[dict[str, Any]]) -> dict[str, Any]:
    static = {
        int(row["signal_cluster_id"])
        for row in rows
        if row["lane_id"] == "static_cross_sectional" and bool(row.get("survivor")) and int(row.get("signal_cluster_id") or 0) > 0
    }
    output: dict[str, Any] = {"static_cluster_count": len(static)}
    union: set[int] = set()
    for lane in ("temporal_program", "event_conditioned", "state_transition"):
        clusters = {
            int(row["signal_cluster_id"])
            for row in rows
            if row["lane_id"] == lane and bool(row.get("survivor")) and int(row.get("signal_cluster_id") or 0) > 0
        }
        new = clusters - static
        output[lane] = {
            "cluster_count": len(clusters),
            "new_vs_static_count": len(new),
            "new_vs_static_share": len(new) / max(1, len(clusters)),
        }
        union.update(new)
    output["combined_new_vs_static_count"] = len(union)
    return output


def adaptive_comparison(rows: list[dict[str, Any]], admissions: Mapping[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    output = []
    hybrid_ids = {str(row["candidate_id"]) for row in admissions["hybrid"]}
    for lane in ADAPTIVE_LANES:
        lane_rows = [row for row in rows if row["lane_id"] == lane]
        control = [row for row in lane_rows if row["proposal_stage"] == "control"]
        adaptive = [row for row in lane_rows if row["proposal_stage"] == "adaptive"]

        def stats(source: list[dict[str, Any]]) -> dict[str, Any]:
            rewards = [float(row.get("proxy_reward") or 0.0) for row in source if bool(row.get("materialized"))]
            return {
                "proposal_count": len(source),
                "legal_count": sum(bool(row.get("legal")) for row in source),
                "survivor_count": sum(bool(row.get("survivor")) for row in source),
                "cluster_count": len({int(row.get("signal_cluster_id") or 0) for row in source if bool(row.get("survivor"))}),
                "hybrid_admission_count": sum(str(row["candidate_id"]) in hybrid_ids for row in source),
                "mean_reward": float(np.mean(rewards)) if rewards else None,
                "median_reward": float(np.median(rewards)) if rewards else None,
                "best_reward": float(np.max(rewards)) if rewards else None,
            }

        control_stats = stats(control)
        adaptive_stats = stats(adaptive)
        control_median = float(control_stats["median_reward"] or 0.0)
        adaptive_median = float(adaptive_stats["median_reward"] or 0.0)
        output.append(
            {
                "lane_id": lane,
                "control": control_stats,
                "adaptive": adaptive_stats,
                "median_reward_delta": adaptive_median - control_median,
                "adaptive_outperformed_matched_control": adaptive_median > control_median,
                "ephemeral_state_persisted": False,
            }
        )
    return output


def strict_metrics(
    strict: list[dict[str, Any]],
    frame: pd.DataFrame,
    signals: Mapping[str, np.ndarray],
    horizons: Iterable[int],
    *,
    min_obs: int,
    cross_layout: Any,
) -> pd.DataFrame:
    cross_key = frame["trade_time"]
    group_codes, _ = pd.factorize(cross_key, sort=False)
    labels = _future_returns(frame, tuple(horizons))
    label_ranks = {
        horizon: fast_rank_pct_by_group(labels[f"fwd_ret_{horizon}m"], cross_key, layout=cross_layout).to_numpy(dtype=float)
        for horizon in horizons
    }
    rows: list[dict[str, Any]] = []
    for candidate in strict:
        signal = signals[str(candidate["canonical_identity"])].astype(float)
        rank = fast_rank_pct_by_group(pd.Series(signal, index=frame.index), cross_key, layout=cross_layout).to_numpy(dtype=float)
        turnover = fast_turnover(rank, frame)
        for horizon in horizons:
            row = {
                "candidate_id": candidate["candidate_id"],
                "lane_id": candidate["lane_id"],
                "proposal_stage": candidate["proposal_stage"],
                "exact_identity": candidate["exact_identity"],
                "canonical_identity": candidate["canonical_identity"],
                "signal_cluster_id": candidate["signal_cluster_id"],
                "expression": candidate["expression"],
                "horizon_bars": int(horizon),
                "proxy_reward": candidate["proxy_reward"],
                "signal_nonnull": int(np.isfinite(signal).sum()),
                "signal_unique": int(np.unique(signal[np.isfinite(signal)]).size) if bool(np.isfinite(signal).any()) else 0,
            }
            row.update(fast_group_ic(rank, label_ranks[int(horizon)], group_codes, min_obs=min_obs))
            row.update(
                fast_group_spread(
                    rank,
                    labels[f"fwd_ret_{horizon}m"].to_numpy(dtype=float),
                    group_codes,
                    min_obs=min_obs,
                )
            )
            row.update(turnover)
            rows.append(row)
    return pd.DataFrame(rows)


def _admission_comparison(admissions: Mapping[str, list[dict[str, Any]]]) -> dict[str, Any]:
    return {name: _stage_distribution(rows) for name, rows in admissions.items()}


def _benchmark_increment(rows: list[dict[str, Any]]) -> dict[str, Any]:
    benchmark = [row for row in rows if row["lane_id"] == "benchmark_competitor" and bool(row.get("survivor"))]
    benchmark_rewards = [float(row["proxy_reward"]) for row in benchmark]
    baseline = float(np.median(benchmark_rewards)) if benchmark_rewards else None
    lanes = {}
    for lane in ("typed_ast", "cem", "rx_ucb", "uct_mcts", "evolutionary", "surrogate", "llm_proposal_repair"):
        source = [float(row["proxy_reward"]) for row in rows if row["lane_id"] == lane and bool(row.get("survivor"))]
        median = float(np.median(source)) if source else None
        lanes[lane] = {
            "median_reward": median,
            "increment_vs_benchmark_median": (median - baseline) if median is not None and baseline is not None else None,
        }
    return {"benchmark_survivor_count": len(benchmark), "benchmark_median_reward": baseline, "complex_lanes": lanes}


def _bottleneck(
    funnel: list[dict[str, Any]],
    admissions: Mapping[str, list[dict[str, Any]]],
    strict: pd.DataFrame,
) -> dict[str, Any]:
    proposals = sum(int(row["proposal_count"]) for row in funnel)
    legal = sum(int(row["legal_count"]) for row in funnel)
    survivors = sum(int(row["survivor_count"]) for row in funnel)
    clusters = len({int(row["signal_cluster_id"]) for row in admissions["hybrid"]})
    strict_h5 = (
        strict[strict["horizon_bars"].eq(5)].dropna(subset=["ic_mean", "proxy_reward"])
        if {"horizon_bars", "ic_mean", "proxy_reward"}.issubset(strict.columns)
        else pd.DataFrame()
    )
    reward_corr = (
        float(strict_h5["proxy_reward"].corr(strict_h5["ic_mean"].abs()))
        if len(strict_h5) >= 5
        else None
    )
    legal_rate = legal / max(1, proposals)
    survivor_rate = survivors / max(1, legal)
    cluster_retention = clusters / max(1, survivors)
    if legal_rate < 0.80:
        primary = "generator"
    elif survivor_rate < 0.50:
        primary = "hypothesis"
    elif cluster_retention < 0.35:
        primary = "generator"
    elif reward_corr is None or reward_corr < 0.40:
        primary = "reward"
    elif len(admissions["hybrid"]) < 0.75 * 168:
        primary = "admission"
    else:
        primary = "cost_stability"
    return {
        "primary_bottleneck": primary,
        "legal_rate": legal_rate,
        "survivor_rate_of_legal": survivor_rate,
        "signal_cluster_to_survivor_ratio": cluster_retention,
        "proxy_to_strict_abs_ic_correlation": reward_corr,
        "cost_model_available": False,
        "stability_window_count": 1,
    }


def _record_output(record: dict[str, Any], path: Path, purpose: str, stage: str) -> None:
    record["outputs"].append(
        {
            "path": str(path),
            "purpose": purpose,
            "stage": stage,
            "size": path.stat().st_size,
            "sha256": _sha256(path),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )


def _persist_attempt(attempt: Path, latest: Path, record: dict[str, Any]) -> None:
    atomic_write_json(attempt, record)
    atomic_write_json(
        latest,
        {
            "experiment_id": EXPERIMENT_ID,
            "latest_attempt": str(attempt),
            "status": record["status"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def _development_role(split_manifest: Path, trade_date: pd.Timestamp) -> str:
    split = pd.read_csv(split_manifest, dtype=str)
    rows = split[pd.to_datetime(split["trade_date"], errors="coerce").dt.normalize().eq(trade_date)]
    if len(rows) != 1:
        raise ValueError("B1S date must resolve exactly once in split manifest")
    role = str(rows.iloc[0]["split"]).strip().lower()
    if role not in {"train", "development"}:
        raise PermissionError(f"B1S requires train/development role, observed {role}")
    return role


def _validate_contract(contract: Mapping[str, Any]) -> None:
    if contract["execution_state"] != "FROZEN_REQUIRES_EXACT_SHA_AND_USER_AUTHORIZATION":
        raise ValueError("B1S contract is not frozen")
    if contract["authorization_token"] != AUTHORIZATION:
        raise ValueError("B1S contract authorization token drift")
    specs = contract["lane_specs"]
    if sum(int(spec["proposal"]) for spec in specs.values()) != int(contract["budgets"]["proposal_total"]):
        raise ValueError("B1S proposal quota sum mismatch")
    if sum(int(spec["admission"]) for spec in specs.values()) != int(contract["budgets"]["admission_total"]):
        raise ValueError("B1S admission quota sum mismatch")
    if sum(int(spec["strict"]) for spec in specs.values()) != int(contract["budgets"]["strict_eval_total"]):
        raise ValueError("B1S strict quota sum mismatch")
    capabilities = contract["capability_matrix"]
    if any(
        capabilities[key] != expected
        for key, expected in {
            "plate_industry_membership": "DISABLED_MISSING_HISTORICAL_PIT_RELEASE",
            "plate_industry_index": "DISABLED_MISSING_HISTORICAL_PIT_RELEASE",
            "plate_zero_placeholder": "FORBIDDEN",
            "current_snapshot_membership": "FORBIDDEN",
        }.items()
    ):
        raise ValueError("B1S plate/industry boundary drift")
    if contract["data_boundary"]["forward_2026_allowed"]:
        raise ValueError("B1S cannot open 2026")
    if contract["adaptation_contract"]["cross_epoch_memory_allowed"]:
        raise ValueError("B1S cannot enable cross-epoch memory")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--panel-root", type=Path, required=True)
    parser.add_argument("--release-manifest", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--field-registry", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--benchmark-registry", type=Path, required=True)
    parser.add_argument("--augmentation-summary", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--authorization", required=True)
    parser.add_argument("--frozen-sha", required=True)
    args = parser.parse_args(argv)

    repo = args.repo.resolve()
    if args.authorization != AUTHORIZATION:
        raise PermissionError("CN B1S CANARY requires the exact user authorization token")
    head = _git(repo, "rev-parse", "HEAD")
    if head != args.frozen_sha:
        raise RuntimeError(f"frozen SHA mismatch: HEAD={head}, authorized={args.frozen_sha}")
    if _git(repo, "status", "--porcelain=v1"):
        raise RuntimeError("CN B1S CANARY requires a clean frozen worktree")

    contract = json.loads(args.contract.read_text(encoding="utf-8"))
    benchmark = json.loads(args.benchmark_registry.read_text(encoding="utf-8"))
    augmentation = json.loads(args.augmentation_summary.read_text(encoding="utf-8"))
    _validate_contract(contract)
    if args.authorization != contract["authorization_token"]:
        raise PermissionError("authorization does not match frozen B1S contract")
    if benchmark.get("disabled_benchmark_ids") != ["plate_industry_linkage"]:
        raise ValueError("plate/industry benchmark must remain disabled")
    hard_rules = set(augmentation.get("hard_rules", []))
    required_rules = {
        "ctx_* sidecars are previous-available only via source_date < exec_date",
        "evt_uplimit_* sidecars are same-day but hidden until trade_time >= cutoff minute",
    }
    if not required_rules.issubset(hard_rules):
        raise ValueError("augmentation manifest lacks B1S PIT hard rules")

    dates = [pd.Timestamp(value).normalize() for value in contract["data_boundary"]["trade_dates"]]
    if not dates or any(date.year not in {2024, 2025} for date in dates):
        raise ValueError("B1S dates must remain inside 2024-2025")
    roles = {str(date.date()): _development_role(args.split_manifest, date) for date in dates}
    if any(role not in {"train", "development"} for role in roles.values()):
        raise PermissionError("B1S split role escaped train/development")
    if len(dates) != 1:
        raise ValueError("B1S read-ledger contract currently freezes exactly one development date")

    data_access_contract = contract.get("data_access_contract")
    if not isinstance(data_access_contract, Mapping):
        raise ValueError("B1S contract lacks the fail-closed data-access contract")
    loader_path = Path(development_only_data_access.__file__).resolve()
    materializer_path = Path(feature_state_fabric_module.__file__).resolve()
    loader_sha = _sha256(loader_path)
    materializer_sha = _sha256(materializer_path)
    if loader_sha != data_access_contract.get("loader_code_hash"):
        raise ValueError("loader code hash does not match the frozen CANARY contract")
    if materializer_sha != data_access_contract.get("materializer_hash"):
        raise ValueError("materializer hash does not match the frozen CANARY contract")
    if _sha256(args.release_manifest) != data_access_contract.get("release_manifest_sha256"):
        raise ValueError("release manifest file hash does not match the frozen CANARY contract")
    release = validate_development_release(
        args.panel_root,
        args.release_manifest,
        args.split_manifest,
        expected_release_hash=str(data_access_contract["development_only_release_hash"]),
    )
    expected_cache_provenance = cache_provenance(
        release_hash_value=release.release_hash,
        split_manifest_sha256=release.split_manifest_sha256,
        loader_code_hash=loader_sha,
        field_registry_hash=_sha256(args.field_registry),
        data_role="development",
        materializer_hash=materializer_sha,
    )
    if expected_cache_provenance != data_access_contract.get("cache_provenance"):
        raise ValueError("cache provenance contract mismatch")
    initialize_cache_root(
        args.cache_root,
        expected_cache_provenance,
        require_fresh=bool(data_access_contract.get("fresh_cache_required", True)),
    )

    started_at = datetime.now(timezone.utc)
    attempt_id = started_at.strftime("%Y%m%dT%H%M%S%fZ")
    attempt_path = args.output_root / "run_records" / "attempts" / f"{EXPERIMENT_ID}_{attempt_id}.json"
    latest_path = args.output_root / "run_records" / f"{EXPERIMENT_ID}.latest.json"
    artifact_root = args.output_root / "artifacts" / attempt_id
    paths = {
        "frozen_contract": artifact_root / "frozen_contract.json",
        "proposals": artifact_root / "candidate_proposals.csv",
        "lane_funnel": artifact_root / "lane_funnel.csv",
        "cluster_registry": artifact_root / "signal_cluster_registry.csv",
        "stratified": artifact_root / "admission_stratified.csv",
        "global_top_k": artifact_root / "admission_global_top_k.csv",
        "hybrid": artifact_root / "admission_hybrid.csv",
        "strict_pack": artifact_root / "strict_candidate_pack.pre_metrics.csv",
        "strict_metrics": artifact_root / "strict_metrics.csv",
        "adaptive": artifact_root / "adaptive_vs_control.json",
        "admission_comparison": artifact_root / "admission_comparison.json",
        "new_clusters": artifact_root / "temporal_event_state_new_clusters.json",
        "benchmark_increment": artifact_root / "benchmark_increment.json",
        "bottleneck": artifact_root / "bottleneck_diagnosis.json",
        "candidate_pack": artifact_root / "frozen_candidate_pack.csv",
        "summary": artifact_root / "summary.json",
        "read_ledger": artifact_root / "development_only_read_ledger.json",
    }
    input_paths = {
        "contract": args.contract,
        "split_manifest": args.split_manifest,
        "field_registry": args.field_registry,
        "benchmark_registry": args.benchmark_registry,
        "augmentation_summary": args.augmentation_summary,
        "release_manifest": args.release_manifest,
    }
    record: dict[str, Any] = {
        "experiment_id": EXPERIMENT_ID,
        "attempt_id": attempt_id,
        "objective": "compare frozen CN B1S hypothesis, search, admission and benchmark lanes on development only",
        "status": "RUNNING",
        "mode": "fixed_budget_development_only_canary",
        "started_at": started_at.isoformat(),
        "authorized_sha": head,
        "authorization": args.authorization,
        "runtime_environment": _runtime_environment(),
        "commands": [
            "$env:PYTHONPATH='src'; python app.py cn-b1s-development-canary -- "
            f'--panel-root "{args.panel_root}" --split-manifest "{args.split_manifest}" '
            f'--release-manifest "{args.release_manifest}" --cache-root "{args.cache_root}" '
            f'--field-registry "{args.field_registry}" --contract "{args.contract}" '
            f'--benchmark-registry "{args.benchmark_registry}" '
            f'--augmentation-summary "{args.augmentation_summary}" '
            f'--output-root "{args.output_root}" --authorization {args.authorization} '
            f"--frozen-sha {args.frozen_sha}"
        ],
        "parameters": {
            "trade_dates": [str(date.date()) for date in dates],
            "split_roles": roles,
            "row_group_index": int(contract["data_boundary"]["row_group_index"]),
            "proposal_budget": int(contract["budgets"]["proposal_total"]),
            "admission_budget": int(contract["budgets"]["admission_total"]),
            "strict_eval_budget": int(contract["budgets"]["strict_eval_total"]),
            "forward_2026_allowed": False,
            "plate_industry_enabled": False,
            "cross_epoch_memory_allowed": False,
            "candidate_promotion_allowed": False,
            "development_only_release_hash": release.release_hash,
            "cache_namespace": expected_cache_provenance["cache_namespace"],
        },
        "inputs": {
            name: {"path": str(path), "size": path.stat().st_size, "sha256": _sha256(path)}
            for name, path in input_paths.items()
        },
        "estimated_runtime": "45-120 minutes on one deterministic development session",
        "outputs": [],
        "reproducibility": "PENDING",
        "failure": None,
        "continuation": "do not open validation, holdout or 2026; do not persist adaptation or promotion",
    }
    _persist_attempt(attempt_path, latest_path, record)
    started = time.perf_counter()

    try:
        frozen_contract = {
            "repo_sha": head,
            "baseline_tag": contract["baseline_tag"],
            "data_release": {
                "panel_root": str(args.panel_root),
                "release_manifest": str(args.release_manifest),
                "release_manifest_sha256": _sha256(args.release_manifest),
                "development_only_release_hash": release.release_hash,
                "schema_sha256": release.manifest["schema_sha256"],
                "file_count": len(release.files),
                "augmentation_summary_sha256": _sha256(args.augmentation_summary),
            },
            "split_manifest_sha256": _sha256(args.split_manifest),
            "field_registry_sha256": _sha256(args.field_registry),
            "benchmark_registry_sha256": _sha256(args.benchmark_registry),
            "contract_sha256": _sha256(args.contract),
            "capability_matrix_hash": _json_hash(contract["capability_matrix"]),
            "lane_specs_hash": _json_hash(contract["lane_specs"]),
            "seeds_hash": _json_hash(contract["seeds"]),
            "budgets_hash": _json_hash(contract["budgets"]),
            "development_objective_hash": _json_hash(contract["development_objective"]),
            "survivor_contract_hash": _json_hash(contract["survivor_contract"]),
            "admission_contract_hash": _json_hash(contract["admission_contract"]),
            "benchmark_contract_hash": _json_hash(contract["benchmark_contract"]),
            "candidate_contract_hash": _json_hash(contract["candidate_contract"]),
            "strict_contract_hash": _json_hash(contract["strict_contract"]),
            "budgets": contract["budgets"],
            "forward_2026_sealed": True,
            "plate_industry_disabled": True,
            "loader_code_hash": loader_sha,
            "materializer_hash": materializer_sha,
            "cache_provenance": expected_cache_provenance,
            "read_ledger_contract": data_access_contract["read_ledger_contract"],
        }
        atomic_write_json(paths["frozen_contract"], frozen_contract)
        _record_output(record, paths["frozen_contract"], "frozen_contract", "pre_evaluation_final")
        record["frozen_contract_sha256"] = _sha256(paths["frozen_contract"])
        record["status"] = "CONTRACT_FROZEN_DATA_READ_RUNNING"
        _persist_attempt(attempt_path, latest_path, record)

        fields = sorted(set(RAW_FIELDS + CONTEXT_FIELDS + FIRSTN_FIELDS + EVENT_FIELDS + STATE_FIELDS + ("close",)))
        registry = FieldRegistry.read(args.field_registry)
        read_columns = _materialization_input_columns(registry, fields)
        raw_frame, panel_inputs = read_development_panel(
            release,
            trade_date=dates[0],
            row_group_index=int(contract["data_boundary"]["row_group_index"]),
            columns=read_columns,
            read_ledger_path=paths["read_ledger"],
            loader_sha=loader_sha,
        )
        _record_output(record, paths["read_ledger"], "development_only_read_ledger", "pre_generation_final")
        if raw_frame["trade_time"].ge(pd.Timestamp("2026-01-01")).any():
            raise ValueError("B1S accessed 2026 rows")
        materialized, fabric_manifest = FeatureStateFabric(
            registry, cache_namespace=expected_cache_provenance["cache_namespace"]
        ).materialize(raw_frame, fields)
        record["status"] = "CONTRACT_FROZEN_GENERATION_RUNNING"
        _persist_attempt(attempt_path, latest_path, record)
        initial = generate_initial_proposals(contract)
        expected_initial = int(contract["budgets"]["proposal_total"]) - sum(
            int(contract["lane_specs"][lane]["adaptive"]) for lane in ADAPTIVE_LANES
        )
        if len(initial) != expected_initial:
            raise AssertionError(f"initial proposal count mismatch: {len(initial)} != {expected_initial}")
        apply_typed_gate(initial)
        materialized["date"] = materialized["trade_time"]
        cross_key = materialized["trade_time"]
        evaluation_context = _ExpressionEvaluationContext.for_frame(materialized)
        cross_layout = _cached_group_layout(materialized, "cross_section", cross_key, context=evaluation_context)
        labels = _future_returns(materialized, (5,))
        label_rank = fast_rank_pct_by_group(labels["fwd_ret_5m"], cross_key, layout=cross_layout).to_numpy(dtype=float)
        proxy_mask = _proxy_mask(materialized, int(contract["development_objective"]["coordinate_minute_stride"]))
        coordinate_indices = _sketch_indices(materialized, proxy_mask, int(contract["signal_cluster_contract"]["coordinate_count"]))
        coordinates = _coordinate_rows(materialized, coordinate_indices)
        projection = projection_matrix([row["coordinate_id"] for row in coordinates], seed=int(contract["seeds"]["signal_sketch"]))
        signals: dict[str, np.ndarray] = {}
        metric_store: dict[str, dict[str, Any]] = {}
        sketches: dict[str, dict[str, Any]] = {}
        evaluate_proxy_rows(
            initial,
            materialized,
            label_rank,
            proxy_mask=proxy_mask,
            coordinate_indices=coordinate_indices,
            coordinate_rows=coordinates,
            projection=projection,
            min_obs=int(contract["strict_contract"]["minimum_observations_per_cross_section"]),
            survivor_contract=contract["survivor_contract"],
            signal_store=signals,
            metric_store=metric_store,
            sketch_store=sketches,
            evaluation_context=evaluation_context,
            cross_layout=cross_layout,
        )

        adaptive = generate_adaptive_proposals(contract, initial)
        apply_typed_gate(adaptive)
        evaluate_proxy_rows(
            adaptive,
            materialized,
            label_rank,
            proxy_mask=proxy_mask,
            coordinate_indices=coordinate_indices,
            coordinate_rows=coordinates,
            projection=projection,
            min_obs=int(contract["strict_contract"]["minimum_observations_per_cross_section"]),
            survivor_contract=contract["survivor_contract"],
            signal_store=signals,
            metric_store=metric_store,
            sketch_store=sketches,
            evaluation_context=evaluation_context,
            cross_layout=cross_layout,
        )
        proposals = initial + adaptive
        if len(proposals) != int(contract["budgets"]["proposal_total"]):
            raise AssertionError("final proposal budget mismatch")
        assign_signal_clusters(proposals, sketches, contract)
        admissions = build_admissions(proposals, contract)
        strict = build_strict_pack(admissions["hybrid"], contract)
        funnel = lane_funnel(proposals, contract)

        _atomic_csv(pd.DataFrame(proposals), paths["proposals"])
        _atomic_csv(pd.DataFrame(funnel), paths["lane_funnel"])
        _atomic_csv(
            pd.DataFrame(
                [
                    {
                        "candidate_id": row["candidate_id"],
                        "lane_id": row["lane_id"],
                        "canonical_identity": row["canonical_identity"],
                        "signal_cluster_id": row["signal_cluster_id"],
                        "survivor": row.get("survivor", False),
                    }
                    for row in proposals
                ]
            ),
            paths["cluster_registry"],
        )
        for name in ("stratified", "global_top_k", "hybrid"):
            _atomic_csv(pd.DataFrame(admissions[name]), paths[name])
        _atomic_csv(pd.DataFrame(strict), paths["strict_pack"])
        for name in ("proposals", "lane_funnel", "cluster_registry", "stratified", "global_top_k", "hybrid", "strict_pack"):
            _record_output(record, paths[name], name, "pre_strict_metrics_final")
        record.update(
            {
                "status": "STRICT_PACK_FROZEN_METRICS_RUNNING",
                "strict_pack_sha256": _sha256(paths["strict_pack"]),
                "strict_pack_frozen_at": datetime.now(timezone.utc).isoformat(),
                "sampled_panel_inputs": panel_inputs,
            }
        )
        _persist_attempt(attempt_path, latest_path, record)

        metrics = strict_metrics(
            strict,
            materialized,
            signals,
            contract["strict_contract"]["horizons_bars"],
            min_obs=int(contract["strict_contract"]["minimum_observations_per_cross_section"]),
            cross_layout=cross_layout,
        )
        adaptive_result = adaptive_comparison(proposals, admissions)
        admission_result = _admission_comparison(admissions)
        new_cluster_result = temporal_cluster_increment(proposals)
        benchmark_result = _benchmark_increment(proposals)
        bottleneck_result = _bottleneck(funnel, admissions, metrics)
        candidate_pack = pd.DataFrame(strict).copy()
        candidate_pack["pack_role"] = "FROZEN_RESEARCH_ONLY_NO_PROMOTION"
        candidate_pack["forward_2026_allowed"] = False
        candidate_pack["persistent_memory_write_allowed"] = False

        _atomic_csv(metrics, paths["strict_metrics"])
        atomic_write_json(paths["adaptive"], adaptive_result)
        atomic_write_json(paths["admission_comparison"], admission_result)
        atomic_write_json(paths["new_clusters"], new_cluster_result)
        atomic_write_json(paths["benchmark_increment"], benchmark_result)
        atomic_write_json(paths["bottleneck"], bottleneck_result)
        _atomic_csv(candidate_pack, paths["candidate_pack"])

        natural_underfill = (
            len(admissions["stratified"]) < int(contract["budgets"]["admission_total"])
            or len(admissions["global_top_k"]) < int(contract["budgets"]["global_topk_total"])
            or len(admissions["hybrid"]) < int(contract["budgets"]["admission_total"])
            or len(strict) < int(contract["budgets"]["strict_eval_total"])
        )
        final_status = "CN_B1S_CANARY_COMPLETED_WITH_NATURAL_UNDERFILL" if natural_underfill else "CN_B1S_CANARY_COMPLETED"
        recommendation = (
            "REVISE_HYPOTHESIS_SPACE_AND_REPEAT_CANARY"
            if bottleneck_result["primary_bottleneck"] == "hypothesis"
            else "REVISE_SEARCH_ENGINE_AND_REPEAT_CANARY"
            if bottleneck_result["primary_bottleneck"] in {"generator", "admission", "reward"}
            else "PREPARE_FROZEN_CANDIDATE_PACK"
        )
        summary = {
            "status": final_status,
            "recommendation": recommendation,
            "experiment_id": EXPERIMENT_ID,
            "attempt_id": attempt_id,
            "authorized_sha": head,
            "proposal_count": len(proposals),
            "legal_count": sum(bool(row.get("legal")) for row in proposals),
            "canonical_count": len({str(row["canonical_identity"]) for row in proposals if bool(row.get("legal"))}),
            "exact_count": len({str(row["exact_identity"]) for row in proposals if bool(row.get("legal"))}),
            "survivor_count": sum(bool(row.get("survivor")) for row in proposals),
            "admission_counts": {name: len(rows) for name, rows in admissions.items()},
            "strict_count": len(strict),
            "strict_metric_rows": len(metrics),
            "lane_funnel": funnel,
            "temporal_event_state_new_clusters": new_cluster_result,
            "adaptive_vs_control": adaptive_result,
            "admission_comparison": admission_result,
            "benchmark_increment": benchmark_result,
            "bottleneck": bottleneck_result,
            "fabric_manifest": fabric_manifest,
            "forward_2026_sealed": True,
            "validation_accessed": False,
            "holdout_accessed": False,
            "plate_industry_enabled": False,
            "candidate_promotion_made": False,
            "cross_epoch_memory_updated": False,
            "persistent_positive_memory_updated": False,
            "persistent_negative_memory_updated": False,
            "ephemeral_adaptation_persisted": False,
            "development_only_release_hash": release.release_hash,
            "cache_provenance": expected_cache_provenance,
            "read_ledger": json.loads(paths["read_ledger"].read_text(encoding="utf-8")),
        }
        atomic_write_json(paths["summary"], summary)
        for name in ("strict_metrics", "adaptive", "admission_comparison", "new_clusters", "benchmark_increment", "bottleneck", "candidate_pack", "summary"):
            _record_output(record, paths[name], name, "final")
        record.update(
            {
                "status": final_status,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_runtime_seconds": round(time.perf_counter() - started, 3),
                "reproducibility": "YES_FOR_FROZEN_INPUTS_PENDING_INDEPENDENT_REPRODUCTION",
                "decision": "HOLD_RESEARCH_NO_PROMOTION",
                "recommendation": recommendation,
            }
        )
        _persist_attempt(attempt_path, latest_path, record)
        print(json.dumps({"status": final_status, "recommendation": recommendation, "attempt_manifest": str(attempt_path), "summary": str(paths["summary"])}, indent=2))
        return 0
    except Exception as exc:
        record.update(
            {
                "status": "CN_B1S_CANARY_FAILED",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_runtime_seconds": round(time.perf_counter() - started, 3),
                "reproducibility": "PARTIAL",
                "failure": {"type": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()},
                "decision": "FAIL",
            }
        )
        _persist_attempt(attempt_path, latest_path, record)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
