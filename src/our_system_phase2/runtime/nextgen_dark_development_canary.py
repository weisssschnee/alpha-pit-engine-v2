"""Run the fixed-budget NEXTGEN-DARK development-only CANARY."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time
import traceback
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    _fields,
    _future_returns,
    _mean_ic,
    _rank_by_group,
    _spread,
    _turnover,
)
from our_system_phase2.services.admission_diversity import AdmissionConfig, admit_candidates
from our_system_phase2.services.atomic_checkpoint import atomic_write_json, durable_flush
from our_system_phase2.services.coverage_metrics import compute_coverage_metrics
from our_system_phase2.services.feature_state_fabric import FeatureStateFabric, FieldRegistry
from our_system_phase2.services.hypothesis_lanes import (
    HypothesisLaneRegistry,
    default_nextgen_lane_registry,
)
from our_system_phase2.services.real_market_validation import evaluate_panel_expression


EXPERIMENT_ID = "nextgen_dark_development_canary"
AUTHORIZATION = "USER_AUTHORIZED_20260712_DEVELOPMENT_ONLY_CANARY"
PANEL_RELATIVE_PATH = Path(
    "phase3aq_wide_true1min/canary/phase3aq_true_1min_formula_canary.parquet"
)
EPS = "0.000001"
WINDOWS = (2, 3, 5, 8, 10, 15, 20, 30)
RAW_FIELDS = (
    "close",
    "vwap",
    "ret_1m",
    "intraday_ret_from_open",
    "amount_yuan",
    "volume",
    "high",
    "low",
)
CONTEXT_FIELDS = (
    "ctx_hfq_turnover_ratio",
    "ctx_hfq_volume_ratio",
    "ctx_hfq_market_cap_yuan",
    "ctx_hfq_float_market_cap_yuan",
    "ctx_hfq_pb",
    "ctx_hfq_ps_ttm",
    "ctx_ths_hot_rank",
    "ctx_ths_hot_rank_diff",
    "ctx_rzrq_rzye",
    "ctx_rzrq_rzjme",
    "ctx_holder_holder_num_ratio",
    "ctx_billboard_deal_net_ratio",
)
FIRSTN_FIELDS = (
    "m1_first5_last_return_vs_open",
    "m1_first5_vwap_return_vs_open",
    "m1_first5_range",
    "m1_first15_last_return_vs_open",
    "m1_first15_vwap_return_vs_open",
    "m1_first15_range",
    "m1_first30_last_return_vs_open",
    "m1_first30_vwap_return_vs_open",
    "m1_first30_range",
)
EVENT_FIELDS = (
    "evt_uplimit_active",
    "evt_uplimit_age_min",
    "evt_uplimit_amount",
    "evt_uplimit_auction_buy",
    "evt_uplimit_auction_money",
    "evt_uplimit_auction_turnover",
    "evt_uplimit_fd_close",
    "evt_uplimit_fd_max",
    "evt_uplimit_up_limit_keep_times",
)
STATE_FIELDS = (
    "evt_uplimit_type_code",
    "evt_uplimit_active",
    "ctx_hfq_prev_is_limit_up",
    "ctx_sent_zb_num",
    "ctx_sent_lb_2_num",
    "ctx_zls_strong",
)
TEMPORAL_PRIMITIVES = (
    "Delta",
    "Slope",
    "Acceleration",
    "Persistence",
    "Duration",
    "StateAge",
    "TimeSince",
    "Transition",
    "FirstHit",
    "LastHit",
    "PathShape",
    "DrawdownPath",
    "RecoveryPath",
    "EventWindow",
    "MultiScaleRelation",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _text_hash(value: str, *, length: int = 24) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:length]


def _frame_hash(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update("|".join(map(str, frame.columns)).encode())
    digest.update(pd.util.hash_pandas_object(frame, index=False).to_numpy().tobytes())
    return digest.hexdigest()


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temporary = Path(name)
    try:
        frame.to_csv(temporary, index=False, encoding="utf-8-sig")
        with temporary.open("r+b") as handle:
            durable_flush(handle)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=repo, text=True).strip()


def _allocate_quota(weights: Iterable[tuple[str, float]], total: int) -> dict[str, int]:
    rows = [(name, float(weight)) for name, weight in weights]
    raw = [(name, weight * total) for name, weight in rows]
    allocated = {name: int(math.floor(value)) for name, value in raw}
    remaining = total - sum(allocated.values())
    order = sorted(raw, key=lambda item: (-(item[1] - math.floor(item[1])), item[0]))
    for name, _ in order[:remaining]:
        allocated[name] += 1
    return allocated


def _static_expression(root: str, index: int) -> tuple[str, str, int]:
    pool = RAW_FIELDS if root == "raw" else CONTEXT_FIELDS
    a = pool[index % len(pool)]
    b = RAW_FIELDS[(index // len(pool)) % len(RAW_FIELDS)]
    variant = (index // (len(pool) * len(RAW_FIELDS))) % 5
    if variant == 0:
        expr = f"CSRank(Div(${a},Add(Abs(${b}),{EPS})))"
        primitive = "ratio_rank"
    elif variant == 1:
        expr = f"CSRank(Sub(ZScore(${a}),ZScore(${b})))"
        primitive = "zscore_spread"
    elif variant == 2:
        expr = f"CSRank(Mul(Sign(${b}),${a}))"
        primitive = "signed_level"
    elif variant == 3:
        expr = f"CSRank(Add(${a},Mul(${b},0.1)))"
        primitive = "linear_interaction"
    else:
        expr = f"CSRank(Div(Sub(${a},${b}),Add(Abs(${a}),{EPS})))"
        primitive = "relative_difference"
    return expr, primitive, 0


def _temporal_expression(root: str, index: int) -> tuple[str, str, int]:
    primitive = TEMPORAL_PRIMITIVES[index % len(TEMPORAL_PRIMITIVES)]
    field = RAW_FIELDS[(index // len(TEMPORAL_PRIMITIVES)) % len(RAW_FIELDS)]
    windows = WINDOWS[:4] if root == "single_scale" else WINDOWS[4:]
    window = windows[(index // (len(TEMPORAL_PRIMITIVES) * len(RAW_FIELDS))) % len(windows)]
    if primitive in {"Delta", "Slope", "Acceleration", "PathShape", "DrawdownPath", "RecoveryPath"}:
        expr = f"CSRank({primitive}(${field},{window}))"
    elif primitive == "Persistence":
        expr = f"CSRank(Persistence($evt_uplimit_active,{window}))"
    elif primitive in {"Duration", "StateAge"}:
        state = STATE_FIELDS[(index // len(TEMPORAL_PRIMITIVES)) % len(STATE_FIELDS)]
        expr = f"CSRank({primitive}(${state}))"
    elif primitive == "TimeSince":
        event = ("evt_uplimit_active", "ctx_hfq_prev_is_limit_up")[index % 2]
        expr = f"CSRank(TimeSince(${event}))"
    elif primitive == "Transition":
        state = STATE_FIELDS[(index // len(TEMPORAL_PRIMITIVES)) % len(STATE_FIELDS)]
        expr = f"CSRank(Transition(${state},0,1))"
    elif primitive in {"FirstHit", "LastHit"}:
        expr = f"CSRank({primitive}($evt_uplimit_active,{window}))"
    elif primitive == "EventWindow":
        expr = f"CSRank(EventWindow(${field},$evt_uplimit_active,0,{min(window, 8)}))"
    else:
        other = RAW_FIELDS[(RAW_FIELDS.index(field) + 1) % len(RAW_FIELDS)]
        short = max(2, min(window, 5))
        long = max(short + 1, min(30, short * 3))
        expr = f"CSRank(MultiScaleRelation(${field},${other},{short},{long}))"
    return expr, primitive.lower(), window


def _event_expression(root: str, index: int) -> tuple[str, str, int]:
    variant = index % 4
    window = WINDOWS[(index // 4) % len(WINDOWS)]
    event = EVENT_FIELDS[(index // (4 * len(WINDOWS))) % len(EVENT_FIELDS)]
    if root == "limit":
        variants = (
            f"CSRank(Mul(EventCount($evt_uplimit_active,{window}),Sign(${event})))",
            f"CSRank(Mul(EventAge($evt_uplimit_active),Sign(${event})))",
            f"CSRank(Mul(EventWindow($close,$evt_uplimit_active,0,{min(window, 8)}),Sign(${event})))",
            f"CSRank(Mul(${event},Sign($ret_1m)))",
        )
        return variants[variant], "limit_event", window
    if root == "firstN":
        window = WINDOWS[index % len(WINDOWS)]
        firstn = FIRSTN_FIELDS[(index // len(WINDOWS)) % len(FIRSTN_FIELDS)]
        return f"CSRank(Mul(${firstn},Add(EventCount($evt_uplimit_active,{window}),1)))", "firstn_event", window
    window = WINDOWS[index % len(WINDOWS)]
    context = CONTEXT_FIELDS[(index // len(WINDOWS)) % len(CONTEXT_FIELDS)]
    return f"CSRank(Mul(${context},Add(EventCount($evt_uplimit_active,{window}),1)))", "context_event", window


def _state_expression(root: str, index: int) -> tuple[str, str, int]:
    window = WINDOWS[index % len(WINDOWS)]
    state = STATE_FIELDS[(index // len(WINDOWS)) % len(STATE_FIELDS)]
    raw = RAW_FIELDS[(index // (len(WINDOWS) * len(STATE_FIELDS))) % len(RAW_FIELDS)]
    if root == "duration":
        return f"CSRank(Mul(StateAge(${state}),Sign(Delta(${raw},{window}))))", "state_age", window
    if root == "transition":
        return f"CSRank(Mul(Transition(${state},0,1),Sign(Delta(${raw},{window}))))", "state_transition", window
    context = CONTEXT_FIELDS[(index // (len(STATE_FIELDS) * len(WINDOWS))) % len(CONTEXT_FIELDS)]
    return f"CSRank(Mul(StateAge(${state}),Sign(Delta(${context},{window}))))", "non_plate_confirmation", window


def _orthogonal_expression(root: str, index: int) -> tuple[str, str, int]:
    left = RAW_FIELDS[index % len(RAW_FIELDS)]
    right = CONTEXT_FIELDS[(index // len(RAW_FIELDS)) % len(CONTEXT_FIELDS)]
    window = WINDOWS[(index // (len(RAW_FIELDS) * len(CONTEXT_FIELDS))) % len(WINDOWS)]
    if root == "orthogonal":
        return f"CSRank(CSResidual(${left},${right}))", "cs_residual", window
    return f"CSRank(Sub(Delta(${left},{window}),ZScore(${right})))", "exile_divergence", window


def _competitor_expression(root: str, index: int) -> tuple[str, str, int]:
    window = WINDOWS[index % len(WINDOWS)]
    field = RAW_FIELDS[(index // len(WINDOWS)) % len(RAW_FIELDS)]
    if root == "strategy":
        firstn = FIRSTN_FIELDS[(index // (len(WINDOWS) * len(RAW_FIELDS))) % len(FIRSTN_FIELDS)]
        templates = (
            f"CSRank(Delta(${field},{window}))",
            f"CSRank(Neg(Delta(${field},{window})))",
            f"CSRank(Std(${field},{window}))",
            f"CSRank(Mean(${field},{window}))",
            f"CSRank(Mul(${firstn},Sign(${field})))",
            f"CSRank(Mul(EventCount($evt_uplimit_active,{window}),Sign(${field})))",
        )
        return templates[(index // (len(WINDOWS) * len(RAW_FIELDS))) % len(templates)], "strategy_reproduction", window
    return f"CSRank(Sub(ZScore(Delta(${field},{window})),ZScore(Std($ret_1m,{window}))))", "external_contract_reproduction", window


def _challenger_expression(root: str, index: int) -> tuple[str, str, int]:
    a = RAW_FIELDS[index % len(RAW_FIELDS)]
    b = RAW_FIELDS[(index // len(RAW_FIELDS)) % len(RAW_FIELDS)]
    c = CONTEXT_FIELDS[(index // (len(RAW_FIELDS) ** 2)) % len(CONTEXT_FIELDS)]
    window = WINDOWS[(index // (len(RAW_FIELDS) ** 2 * len(CONTEXT_FIELDS))) % len(WINDOWS)]
    if root == "mcts":
        expr = f"CSRank(Sub(ZScore(Delta(${a},{window})),ZScore(Div(${b},Add(Abs(${c}),{EPS})))))"
        return expr, "frozen_mcts_tree", window
    expr = f"CSRank(Add(PathShape(${a},{max(2, window)}),Mul(Sign(${c}),Delta(${b},{max(1, window // 2)}))))"
    return expr, "frozen_evolutionary_tree", window


EXPRESSION_BUILDERS = {
    "static_cross_sectional": _static_expression,
    "temporal_program": _temporal_expression,
    "event_conditioned": _event_expression,
    "state_transition": _state_expression,
    "orthogonal_exile": _orthogonal_expression,
    "competitor_reproduction": _competitor_expression,
    "mcts_evolutionary_challenger": _challenger_expression,
}


def generate_proposals(lanes: HypothesisLaneRegistry) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    exact_seen: set[str] = set()
    for lane in lanes.specs:
        root_quota = _allocate_quota(lane.root_distribution, lane.proposal_quota)
        lane_rows: list[dict[str, Any]] = []
        for root, quota in root_quota.items():
            accepted = 0
            sequence = 0
            while accepted < quota:
                if sequence > max(300, quota * 3):
                    raise RuntimeError(
                        f"proposal template capacity exhausted: {lane.lane_id}/{root} "
                        f"accepted={accepted}, required={quota}"
                    )
                expression, primitive, window = EXPRESSION_BUILDERS[lane.lane_id](root, sequence)
                exact = _text_hash(expression, length=64)
                sequence += 1
                if exact in exact_seen:
                    continue
                exact_seen.add(exact)
                fields = _fields(expression)
                if any("plate" in field.lower() or "industry" in field.lower() for field in fields):
                    raise ValueError("user-deferred plate/industry field entered a proposal")
                local_index = len(lane_rows)
                parent_id = "" if local_index % 3 == 0 else f"{lane.lineage_namespace}:parent:{local_index // 2:04d}"
                field_family = "mixed" if len(fields) > 1 else (
                    "firstN" if fields and fields[0].startswith("m1_first") else
                    "event_state" if fields and fields[0].startswith("evt_") else
                    "lagged_daily_context" if fields and fields[0].startswith("ctx_") else
                    "raw_1min"
                )
                semantic_key = _text_hash(
                    f"{lane.lane_id}|{root}|{primitive}|{field_family}|{window}", length=32
                )
                row = {
                    "candidate_id": f"ngd_{lane.lane_id}_{local_index + 1:04d}",
                    "lane_id": lane.lane_id,
                    "exact_identity": exact,
                    "semantic_key": semantic_key,
                    "lineage_key": f"{lane.lineage_namespace}:candidate:{local_index + 1:04d}",
                    "expression": expression,
                    "data_role": "development",
                    "root_cell": root,
                    "field_family": field_family,
                    "primitive_family": primitive,
                    "temporal_cell": f"w{window}" if window else "instant",
                    "event_state_family": primitive if lane.lane_id in {"event_conditioned", "state_transition"} else "none",
                    "plate_industry_family": "disabled_user_deferred",
                    "economic_hypothesis": f"{lane.lane_id}:{root}:{primitive}",
                    "grammar_cell": f"{lane.lane_id}:{root}:{primitive}",
                    "family_id": f"{lane.lane_id}:{root}",
                    "semantic_bucket": f"{lane.lane_id}:{root}:{primitive}:w{window}",
                    "parent_id": parent_id,
                    "fresh": not bool(parent_id),
                    "fields_list": fields,
                    "max_window": int(window),
                    "proposal_origin": "frozen_template_no_reward",
                }
                lane_rows.append(lanes.validate_submission(row))
                accepted += 1
        if len(lane_rows) != lane.proposal_quota:
            raise AssertionError(f"proposal quota mismatch: {lane.lane_id}")
        proposals.extend(lane_rows)
    return proposals


def select_canary_candidates(
    proposals: list[dict[str, Any]],
    lanes: HypothesisLaneRegistry,
    plan: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    selection = plan["selection"]
    admission = admit_candidates(
        proposals,
        lanes,
        AdmissionConfig(
            total_quota=int(plan["admission_budget_total"]),
            family_cap=32,
            bucket_cap=8,
            parent_descendant_cap=int(selection["parent_descendant_cap"]),
            fresh_budget_floor=int(selection["fresh_budget_floor"]),
            exile_quota=int(selection["exile_quota"]),
            seed=int(selection["deterministic_seed"]),
            global_topk_baseline_quota=int(plan["global_topk_baseline_quota"]),
        ),
    )
    strict: list[dict[str, Any]] = []
    selected = list(admission["selected"])
    for lane_id, quota in plan["strict_eval_allocation"].items():
        rows = [row for row in selected if row["lane_id"] == lane_id]
        if len(rows) < int(quota):
            raise ValueError(
                f"strict allocation cannot be met for {lane_id}: selected={len(rows)}, required={quota}"
            )
        strict.extend(rows[: int(quota)])
    if len(strict) != int(plan["strict_eval_budget_total"]):
        raise AssertionError("strict evaluation budget mismatch")
    return admission, strict


def _development_gate(split_manifest: Path, trade_date: pd.Timestamp) -> str:
    split = pd.read_csv(split_manifest, dtype=str)
    rows = split[pd.to_datetime(split["trade_date"], errors="coerce").dt.normalize().eq(trade_date)]
    if len(rows) != 1:
        raise ValueError("CANARY date must resolve exactly once in split manifest")
    role = str(rows.iloc[0]["split"]).strip().lower()
    if role not in {"train", "development"}:
        raise PermissionError(f"CANARY requires development/train date, observed {role}")
    return role


def read_sampled_development_panel(
    panel_root: Path,
    *,
    trade_date: pd.Timestamp,
    row_group_index: int,
    columns: Iterable[str],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    paths = sorted(panel_root.glob(f"shard_*/{PANEL_RELATIVE_PATH.as_posix()}"))
    if len(paths) != 16:
        raise ValueError(f"CANARY requires all 16 canonical shards, observed {len(paths)}")
    required = sorted(set(columns) | {"code", "trade_time", "date", "close", "signal_time"})
    frames: list[pd.DataFrame] = []
    inputs: list[dict[str, Any]] = []
    for path in paths:
        parquet = pq.ParquetFile(path)
        if not 0 <= row_group_index < parquet.metadata.num_row_groups:
            raise IndexError(f"row group {row_group_index} unavailable: {path}")
        missing = sorted(set(required) - set(parquet.schema_arrow.names))
        if missing:
            raise ValueError(f"CANARY panel missing fields {missing}: {path}")
        table = parquet.read_row_group(row_group_index, columns=required)
        times = pd.to_datetime(
            table["trade_time"].combine_chunks().to_pandas(), errors="coerce", format="mixed"
        )
        mask = times.normalize().eq(trade_date).to_numpy()
        selected = table.filter(pa.array(mask)).to_pandas()
        selected["trade_time"] = pd.to_datetime(
            selected["trade_time"], errors="coerce", format="mixed"
        )
        if not selected.empty:
            frames.append(selected)
        inputs.append(
            {
                "path": str(path),
                "size": path.stat().st_size,
                "mtime_ns": path.stat().st_mtime_ns,
                "row_group_index": row_group_index,
                "selected_rows": len(selected),
                "selected_rows_sha256": _frame_hash(selected[required]),
            }
        )
    if not frames:
        raise ValueError(f"CANARY sampled panels contain no rows for {trade_date.date()}")
    out = pd.concat(frames, ignore_index=True)
    if out["trade_time"].ge(pd.Timestamp("2026-01-01")).any():
        raise ValueError("CANARY cannot access 2026 forward rows")
    out["ctx_source_session_upper_bound"] = out["trade_time"].dt.normalize() - pd.Timedelta(
        days=1
    )
    return out, inputs


def _runtime_environment() -> dict[str, Any]:
    return {
        "python": sys.version,
        "executable": sys.executable,
        "platform": platform.platform(),
        "packages": {
            name: importlib.metadata.version(name)
            for name in ("numpy", "pandas", "pyarrow")
        },
    }


def _panel_inventory(panel_root: Path, row_group_index: int) -> list[dict[str, Any]]:
    paths = sorted(panel_root.glob(f"shard_*/{PANEL_RELATIVE_PATH.as_posix()}"))
    if len(paths) != 16:
        raise ValueError(f"CANARY requires all 16 canonical shards, observed {len(paths)}")
    inventory = []
    for path in paths:
        parquet = pq.ParquetFile(path)
        if not 0 <= row_group_index < parquet.metadata.num_row_groups:
            raise IndexError(f"row group {row_group_index} unavailable: {path}")
        row_group = parquet.metadata.row_group(row_group_index)
        inventory.append(
            {
                "path": str(path),
                "size": path.stat().st_size,
                "mtime_ns": path.stat().st_mtime_ns,
                "row_group_index": row_group_index,
                "row_group_rows": row_group.num_rows,
                "row_group_total_byte_size": row_group.total_byte_size,
                "schema_sha256": hashlib.sha256(
                    str(parquet.schema_arrow).encode("utf-8")
                ).hexdigest(),
            }
        )
    return inventory


def evaluate_strict_pack(
    frame: pd.DataFrame,
    strict: list[dict[str, Any]],
    registry: FieldRegistry,
    *,
    horizons: tuple[int, ...],
    min_obs_per_time: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    fields = sorted({field for row in strict for field in row["fields_list"]} | {"close"})
    fabric = FeatureStateFabric(registry)
    materialized, fabric_manifest = fabric.materialize(frame, fields)
    eval_frame = materialized.copy()
    eval_frame["date"] = eval_frame["trade_time"]
    labels = _future_returns(eval_frame, horizons)
    label_ranks = {
        horizon: _rank_by_group(labels[f"fwd_ret_{horizon}m"], eval_frame["trade_time"])
        for horizon in horizons
    }
    rows: list[dict[str, Any]] = []
    signal_fingerprints: dict[str, str] = {}
    expression_cache: dict[str, pd.Series] = {}
    for candidate in strict:
        signal_all = pd.to_numeric(
            evaluate_panel_expression(
                eval_frame,
                str(candidate["expression"]),
                cache=expression_cache,
                data_role="development",
            ),
            errors="coerce",
        )
        signal_rank = _rank_by_group(signal_all, eval_frame["trade_time"])
        signal_future = signal_all.groupby(eval_frame["code"], sort=False).shift(-1)
        future_rank = _rank_by_group(signal_future, eval_frame["trade_time"])
        signal_fingerprints[str(candidate["candidate_id"])] = _frame_hash(
            pd.DataFrame({"code": eval_frame["code"], "trade_time": eval_frame["trade_time"], "signal": signal_all})
        )
        for horizon in horizons:
            row = {
                "candidate_id": candidate["candidate_id"],
                "lane_id": candidate["lane_id"],
                "exact_identity": candidate["exact_identity"],
                "semantic_key": candidate["semantic_key"],
                "expression": candidate["expression"],
                "horizon_bars": horizon,
                "signal_nonnull": int(signal_all.notna().sum()),
                "signal_unique": int(signal_all.nunique(dropna=True)),
                "eval_rows": len(eval_frame),
                "eval_trade_times": int(eval_frame["trade_time"].nunique()),
            }
            row.update(
                _mean_ic(
                    signal_rank,
                    label_ranks[horizon],
                    eval_frame["trade_time"],
                    min_obs_per_time,
                )
            )
            row.update(
                _spread(
                    signal_rank,
                    labels[f"fwd_ret_{horizon}m"],
                    eval_frame["trade_time"],
                    min_obs_per_time,
                )
            )
            wrong_lag = _mean_ic(
                future_rank,
                label_ranks[horizon],
                eval_frame["trade_time"],
                min_obs_per_time,
            )
            row["wrong_lag_future_ic_mean"] = wrong_lag.get("ic_mean")
            row.update(_turnover(signal_rank, eval_frame))
            rows.append(row)
    diagnostics = {
        "data_role": "development",
        "strict_candidate_count": len(strict),
        "horizons_bars": list(horizons),
        "eval_rows": len(eval_frame),
        "eval_code_count": int(eval_frame["code"].nunique()),
        "eval_trade_time_count": int(eval_frame["trade_time"].nunique()),
        "fabric_manifest": fabric_manifest,
        "signal_fingerprints": signal_fingerprints,
        "portfolio_return_computed": False,
        "transaction_cost_model": "NOT_APPLICABLE_NO_PORTFOLIO_RETURN",
        "oos_evidence": "NONE_DEVELOPMENT_ONLY_CANARY",
        "formal_promotion_allowed": False,
        "forward_2026_accessed": False,
        "adaptive_reward_updated": False,
    }
    return pd.DataFrame(rows), diagnostics


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


def _record_output(record: dict[str, Any], path: Path, purpose: str, stage: str) -> None:
    record["outputs"].append(
        {
            "path": str(path),
            "producer": "nextgen_dark_development_canary",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "purpose": purpose,
            "stage": stage,
            "size": path.stat().st_size,
            "sha256": _sha256(path),
        }
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--panel-root", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--field-registry", type=Path, required=True)
    parser.add_argument("--lane-registry", type=Path, required=True)
    parser.add_argument("--canary-plan", type=Path, required=True)
    parser.add_argument("--benchmark-registry", type=Path, required=True)
    parser.add_argument("--augmentation-summary", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--trade-date", default="2025-04-01")
    parser.add_argument("--row-group-index", type=int, default=0)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs-per-time", type=int, default=20)
    parser.add_argument("--authorization", required=True)
    parser.add_argument("--frozen-sha", required=True)
    args = parser.parse_args(argv)

    repo = args.repo.resolve()
    if args.authorization != AUTHORIZATION:
        raise PermissionError("NEXTGEN CANARY requires the exact independent authorization token")
    head = _git(repo, "rev-parse", "HEAD")
    if head != args.frozen_sha:
        raise RuntimeError(f"frozen SHA mismatch: HEAD={head}, authorized={args.frozen_sha}")
    if _git(repo, "status", "--porcelain=v1"):
        raise RuntimeError("NEXTGEN CANARY requires a clean frozen worktree")
    trade_date = pd.Timestamp(args.trade_date).normalize()
    if trade_date >= pd.Timestamp("2026-01-01"):
        raise ValueError("NEXTGEN CANARY cannot use 2026 forward")
    split_role = _development_gate(args.split_manifest, trade_date)
    plan = json.loads(args.canary_plan.read_text(encoding="utf-8"))
    benchmark = json.loads(args.benchmark_registry.read_text(encoding="utf-8"))
    augmentation = json.loads(args.augmentation_summary.read_text(encoding="utf-8"))
    lane_contract = json.loads(args.lane_registry.read_text(encoding="utf-8"))
    lanes = default_nextgen_lane_registry()
    if lane_contract != lanes.contract():
        raise ValueError("lane registry file does not match the frozen code contract")
    if plan["execution_state"] != "PREPARED_NOT_STARTED_REQUIRES_INDEPENDENT_AUTHORIZATION":
        raise ValueError("unexpected CANARY execution state")
    if plan["allowed_data_roles"] != ["development"] or plan["forward_2026_allowed"]:
        raise ValueError("CANARY data boundary is not development-only")
    if plan["online_policy_update_allowed"] or plan["adaptive_reward_allowed"]:
        raise ValueError("CANARY cannot enable adaptive policy or reward")
    if plan["plate_industry_linkage"]["enabled"]:
        raise ValueError("user-deferred plate/industry linkage cannot enter CANARY")
    if benchmark.get("disabled_benchmark_ids") != ["plate_industry_linkage"]:
        raise ValueError("plate/industry benchmark must remain disabled")
    lag_rule = "ctx_* sidecars are previous-available only via source_date < exec_date"
    if lag_rule not in augmentation.get("hard_rules", []):
        raise ValueError("augmentation manifest lacks the previous-session context rule")
    event_rule = (
        "evt_uplimit_* sidecars are same-day but hidden until trade_time >= cutoff minute"
    )
    if event_rule not in augmentation.get("hard_rules", []):
        raise ValueError("augmentation manifest lacks the event observability rule")

    horizons = tuple(int(value) for value in args.horizons.split(",") if value.strip())
    if not horizons or any(value <= 0 for value in horizons):
        raise ValueError("CANARY horizons must be positive")
    started_at = datetime.now(timezone.utc)
    attempt_id = started_at.strftime("%Y%m%dT%H%M%S%fZ")
    attempt_path = args.output_root / "run_records" / "attempts" / f"{EXPERIMENT_ID}_{attempt_id}.json"
    latest_path = args.output_root / "run_records" / f"{EXPERIMENT_ID}.latest.json"
    artifact_root = args.output_root / "artifacts" / attempt_id
    command = (
        "$env:PYTHONPATH='src'; python app.py nextgen-dark-development-canary -- "
        f'--panel-root "{args.panel_root}" --split-manifest "{args.split_manifest}" '
        f'--field-registry "{args.field_registry}" --lane-registry "{args.lane_registry}" '
        f'--canary-plan "{args.canary_plan}" --benchmark-registry "{args.benchmark_registry}" '
        f'--augmentation-summary "{args.augmentation_summary}" '
        f'--output-root "{args.output_root}" --trade-date {trade_date.date()} '
        f"--row-group-index {args.row_group_index} --horizons {args.horizons} "
        f"--min-obs-per-time {args.min_obs_per_time} --authorization {args.authorization} "
        f"--frozen-sha {args.frozen_sha}"
    )
    input_paths = {
        "split_manifest": args.split_manifest,
        "field_registry": args.field_registry,
        "lane_registry": args.lane_registry,
        "canary_plan": args.canary_plan,
        "benchmark_registry": args.benchmark_registry,
        "augmentation_summary": args.augmentation_summary,
    }
    record: dict[str, Any] = {
        "experiment_id": EXPERIMENT_ID,
        "attempt_id": attempt_id,
        "objective": "exercise fixed-budget NEXTGEN proposal, admission and strict evaluation plumbing",
        "status": "RUNNING",
        "mode": "bounded_development_canary",
        "started_at": started_at.isoformat(),
        "authorized_sha": args.frozen_sha,
        "authorization": args.authorization,
        "inputs": {
            name: {"path": str(path), "size": path.stat().st_size, "sha256": _sha256(path)}
            for name, path in input_paths.items()
        },
        "parameters": {
            "trade_date": str(trade_date.date()),
            "split_role": split_role,
            "effective_data_role": "development",
            "row_group_index": args.row_group_index,
            "horizons_bars": list(horizons),
            "proposal_budget_total": plan["proposal_budget_total"],
            "admission_budget_total": plan["admission_budget_total"],
            "strict_eval_budget_total": plan["strict_eval_budget_total"],
            "seed": plan["selection"]["deterministic_seed"],
            "plate_industry_enabled": False,
            "forward_2026_allowed": False,
            "online_policy_update_allowed": False,
            "adaptive_reward_allowed": False,
        },
        "commands": [command],
        "runtime_environment": _runtime_environment(),
        "estimated_runtime": "5-30 minutes on one deterministic row group per 16 shards",
        "outputs": [],
        "reproducibility": "PENDING",
        "decision": "HOLD_RESEARCH_UNTIL_INDEPENDENT_REVIEW",
        "continuation": "do not open 2026; complete reviewer, reproducer, bias audit and gatekeeper roles",
        "failure": None,
    }
    _persist_attempt(attempt_path, latest_path, record)
    started = time.perf_counter()
    paths = {
        "proposals": artifact_root / "candidate_proposals.csv",
        "admitted": artifact_root / "admitted_candidates.csv",
        "strict_pack": artifact_root / "strict_pack.pre_eval.csv",
        "global_topk_baseline": artifact_root / "global_topk_baseline.pre_eval.csv",
        "strict_metrics": artifact_root / "strict_metrics.csv",
        "admission": artifact_root / "admission_diagnostics.json",
        "coverage": artifact_root / "coverage_metrics.json",
        "evaluation": artifact_root / "evaluation_diagnostics.json",
        "summary": artifact_root / "summary.json",
    }
    try:
        proposals = generate_proposals(lanes)
        if len(proposals) != int(plan["proposal_budget_total"]):
            raise AssertionError("proposal budget mismatch")
        admission, strict = select_canary_candidates(proposals, lanes, plan)
        coverage = compute_coverage_metrics(proposals)
        artifact_root.mkdir(parents=True, exist_ok=True)
        _atomic_csv(pd.DataFrame(proposals), paths["proposals"])
        _atomic_csv(pd.DataFrame(admission["selected"]), paths["admitted"])
        _atomic_csv(pd.DataFrame(strict), paths["strict_pack"])
        _atomic_csv(
            pd.DataFrame(admission["global_topk_baseline"]),
            paths["global_topk_baseline"],
        )
        atomic_write_json(
            paths["admission"],
            {
                key: value
                for key, value in admission.items()
                if key not in {"selected", "global_topk_baseline"}
            },
        )
        atomic_write_json(paths["coverage"], coverage)
        for name in (
            "proposals",
            "admitted",
            "strict_pack",
            "global_topk_baseline",
            "admission",
            "coverage",
        ):
            _record_output(
                record,
                paths[name],
                name,
                "final_pre_eval" if name == "strict_pack" else "diagnostic_pre_eval",
            )
        record["strict_pack_frozen_at"] = datetime.now(timezone.utc).isoformat()
        record["strict_pack_sha256"] = _sha256(paths["strict_pack"])
        record["panel_inventory"] = _panel_inventory(
            args.panel_root, args.row_group_index
        )
        record["status"] = "STRICT_PACK_FROZEN_EVALUATION_RUNNING"
        _persist_attempt(attempt_path, latest_path, record)
        fields = sorted({field for row in strict for field in row["fields_list"]} | {"close"})
        raw_frame, panel_inputs = read_sampled_development_panel(
            args.panel_root,
            trade_date=trade_date,
            row_group_index=args.row_group_index,
            columns=fields,
        )
        record["sampled_panel_inputs"] = panel_inputs
        record["status"] = "SAMPLED_INPUTS_HASHED_EVALUATION_RUNNING"
        _persist_attempt(attempt_path, latest_path, record)
        registry = FieldRegistry.read(args.field_registry)
        metrics, diagnostics = evaluate_strict_pack(
            raw_frame,
            strict,
            registry,
            horizons=horizons,
            min_obs_per_time=args.min_obs_per_time,
        )
        diagnostics.update(
            {
                "attempt_id": attempt_id,
                "proposal_count": len(proposals),
                "admission_count": admission["selected_count"],
                "strict_count": len(strict),
                "panel_inputs": panel_inputs,
                "split_manifest_role": split_role,
                "strict_pack_frozen_before_metrics": True,
                "strict_pack_sha256": record["strict_pack_sha256"],
            }
        )
        summary = {
            "status": "COMPLETED_AWAITING_INDEPENDENT_REVIEW",
            "experiment_id": EXPERIMENT_ID,
            "attempt_id": attempt_id,
            "authorized_sha": args.frozen_sha,
            "proposal_count": len(proposals),
            "admission_count": admission["selected_count"],
            "admission_budget_cap": plan["admission_budget_total"],
            "admission_budget_unused": admission["admission_budget_unused"],
            "global_topk_baseline_count": admission["global_topk_baseline_count"],
            "strict_count": len(strict),
            "metric_row_count": len(metrics),
            "per_lane_proposals": dict(Counter(row["lane_id"] for row in proposals)),
            "per_lane_admissions": admission["per_lane_admission_distribution"],
            "per_lane_strict": dict(Counter(row["lane_id"] for row in strict)),
            "plate_industry_enabled": False,
            "forward_2026_accessed": False,
            "adaptive_reward_updated": False,
            "online_policy_updated": False,
            "candidate_promotion_made": False,
            "decision": "HOLD_RESEARCH_UNTIL_INDEPENDENT_REVIEW",
        }
        _atomic_csv(metrics, paths["strict_metrics"])
        atomic_write_json(paths["evaluation"], diagnostics)
        atomic_write_json(paths["summary"], summary)
        for name in ("strict_metrics", "evaluation", "summary"):
            _record_output(
                record,
                paths[name],
                name,
                "final" if name == "summary" else "diagnostic_post_eval",
            )
        record.update(
            {
                "status": "COMPLETED_AWAITING_INDEPENDENT_REVIEW",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_runtime_seconds": round(time.perf_counter() - started, 3),
                "reproducibility": "YES_FOR_FROZEN_SAMPLED_INPUTS_WITH_CONTENT_HASHES",
            }
        )
        _persist_attempt(attempt_path, latest_path, record)
        print(json.dumps({"status": record["status"], "run_manifest": str(attempt_path), "summary": str(paths["summary"])}, indent=2))
        return 0
    except Exception as exc:
        record.update(
            {
                "status": "FAILED",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_runtime_seconds": round(time.perf_counter() - started, 3),
                "reproducibility": "PARTIAL",
                "failure": {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                },
            }
        )
        _persist_attempt(attempt_path, latest_path, record)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
