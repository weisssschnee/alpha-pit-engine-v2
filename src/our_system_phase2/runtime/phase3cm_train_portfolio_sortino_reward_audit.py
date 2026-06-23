"""Train-set portfolio Sortino reward audit for true-1min candidates.

Phase3BZ fragment replay is useful as a diagnostic slice replay, but it is not
the right optimization target for CEM/UCB. This module builds a continuous
minute portfolio PnL curve on true trade_time shards and reports train /
validation / holdout Sortino-style reward fields for later search feedback.

This route is diagnostic-only. It does not generate candidates, launch search,
or modify X0/R3.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.metadata
import json
import math
import os
import random
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _discover_panels,
    _fields,
    _future_returns,
    _max_expression_window,
    _rank_by_group,
    _read_windowed_panel,
    _write_csv,
    _write_json,
)
from our_system_phase2.services.candidate_schema import normalize_candidate_schema
from our_system_phase2.services.real_market_validation import evaluate_panel_expression


REPO = Path(__file__).resolve().parents[3]
DEFAULT_CANDIDATE_AUDIT = Path("reports/phase3cl_bz_candidate_audit_20260622/phase3ca_bz_candidate_audit.csv")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cm_train_portfolio_sortino_reward_audit_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3cm_train_portfolio_sortino_reward_audit_20260623")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _f(value: Any, default: float = float("nan")) -> float:
    try:
        if value in (None, ""):
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _round(value: Any, ndigits: int = 8) -> float | None:
    val = _f(value)
    if not math.isfinite(val):
        return None
    return round(val, ndigits)


def _sortino(values: list[float], annualizer: float = 1.0) -> float | None:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if not clean:
        return None
    mean = statistics.fmean(clean)
    downside = [min(0.0, value) for value in clean]
    downside_var = statistics.fmean([value * value for value in downside])
    if downside_var <= 1e-18:
        return None
    return mean / math.sqrt(downside_var) * math.sqrt(annualizer)


def _max_drawdown(values: list[float]) -> float | None:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if not clean:
        return None
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for value in clean:
        equity *= 1.0 + value
        peak = max(peak, equity)
        if peak > 0:
            max_dd = min(max_dd, equity / peak - 1.0)
    return max_dd


def _quantile(values: list[float], q: float) -> float | None:
    clean = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    pos = (len(clean) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return clean[lo]
    return clean[lo] * (hi - pos) + clean[hi] * (pos - lo)


def _safe_stdev(values: list[float]) -> float | None:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if len(clean) < 2:
        return None
    return float(statistics.stdev(clean))


def _package_versions() -> dict[str, str]:
    packages = ["numpy", "pandas", "pyarrow", "numba", "bottleneck", "numexpr", "polars", "joblib", "scikit-learn"]
    versions: dict[str, str] = {}
    for package in packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "missing"
    return versions


def _load_candidates(path: Path, limit: int) -> list[dict[str, Any]]:
    rows = _read_csv(path)
    rows.sort(
        key=lambda row: (
            _f(row.get("phase3ca_proxy_quality"), -999.0),
            _f(row.get("aligned_ic_mean") or row.get("abs_aligned_ic_mean"), -999.0),
        ),
        reverse=True,
    )
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        expression = str(row.get("expression") or "").strip()
        digest = str(row.get("expression_hash") or "").strip()
        if not expression:
            continue
        if not digest:
            digest = hashlib.sha256(expression.encode("utf-8")).hexdigest()[:24]
        if digest in seen:
            continue
        seen.add(digest)
        item = dict(row)
        item["expression_hash"] = digest
        item["candidate_id"] = row.get("candidate_id") or digest[:12]
        item["fields_list"] = _fields(expression)
        item["max_window"] = _max_expression_window(expression)
        selected.append(item)
        if len(selected) >= limit:
            break
    if not selected:
        raise RuntimeError(f"no candidates selected from {path}")
    return selected


def _split_map(signal_times: set[pd.Timestamp], train_fraction: float, validation_fraction: float) -> dict[pd.Timestamp, str]:
    times = sorted(pd.to_datetime(list(signal_times)))
    if not times:
        return {}
    n = len(times)
    train_end = max(1, min(n, int(round(n * train_fraction))))
    validation_end = max(train_end, min(n, train_end + int(round(n * validation_fraction))))
    out: dict[pd.Timestamp, str] = {}
    for idx, trade_time in enumerate(times):
        if idx < train_end:
            split = "train"
        elif idx < validation_end:
            split = "validation"
        else:
            split = "holdout"
        out[pd.Timestamp(trade_time)] = split
    return out


def _read_train_shard(
    *,
    candidates: list[dict[str, Any]],
    panel_path: Path,
    horizons: tuple[int, ...],
    sample_trade_times: int | None,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.DataFrame, set[pd.Timestamp], dict[str, Any]]:
    max_window = max((int(candidate.get("max_window") or 0) for candidate in candidates), default=0)
    max_horizon = max(horizons)
    fields = sorted({field for candidate in candidates for field in candidate["fields_list"]})
    required = {
        "code",
        "trade_time",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vol",
        "amount",
        "amount_yuan",
        "vwap",
        *fields,
    }

    schema = set(pq.ParquetFile(panel_path).schema_arrow.names)
    columns = [column for column in sorted(required) if column in schema]
    missing = sorted({"code", "trade_time", "date", "close"} - set(columns))
    if missing:
        raise RuntimeError(f"{panel_path} missing required columns {missing}")
    missing_fields = sorted(set(fields) - set(columns))
    if missing_fields:
        raise RuntimeError(f"{panel_path} missing expression fields {missing_fields}")

    frame, signal_times, signal_time_count, read_time_count = _read_windowed_panel(
        panel_path,
        columns=columns,
        signal_time_count=sample_trade_times,
        lookback=max_window,
        max_horizon=max_horizon,
    )
    frame["code"] = frame["code"].astype(str)
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["code", "trade_time", "close"]).sort_values(["code", "trade_time"]).reset_index(drop=True)
    eval_mask = frame["trade_time"].isin(signal_times)
    eval_frame = frame.loc[eval_mask].copy().reset_index(drop=True)
    labels = _future_returns(frame, horizons).loc[eval_mask].reset_index(drop=True)
    meta = {
        "panel": str(panel_path),
        "read_rows": int(len(frame)),
        "eval_rows": int(len(eval_frame)),
        "signal_trade_times": int(signal_time_count),
        "read_trade_times": int(read_time_count),
        "read_column_count": len(columns),
        "candidate_count_in_batch": len(candidates),
    }
    return frame, eval_mask, eval_frame, labels, signal_times, meta


def _candidate_portfolio_rows_from_frame(
    *,
    candidate: dict[str, Any],
    frame: pd.DataFrame,
    eval_mask: pd.Series,
    eval_frame: pd.DataFrame,
    labels: pd.DataFrame,
    split_by_time: dict[pd.Timestamp, str],
    shard_index: int,
    horizons: tuple[int, ...],
    min_obs: int,
    cost_bps: float,
    top_quantile: float,
    expression_cache: dict[str, pd.Series],
) -> list[dict[str, Any]]:
    expression = str(candidate["expression"])
    if expression in expression_cache:
        signal_all = expression_cache[expression]
    else:
        signal_all = pd.to_numeric(evaluate_panel_expression(frame, expression, cache={}), errors="coerce")
        expression_cache[expression] = signal_all
    signal = pd.Series(signal_all.loc[eval_mask].to_numpy(dtype=float))
    signal_rank = _rank_by_group(signal, eval_frame["trade_time"])
    direction = 1.0 if str(candidate.get("open_direction") or "long_top") == "long_top" else -1.0
    one_way_cost = float(cost_bps) / 10000.0
    q_low = float(top_quantile)
    q_high = 1.0 - float(top_quantile)
    rows: list[dict[str, Any]] = []

    for horizon in horizons:
        label = pd.to_numeric(labels[f"fwd_ret_{horizon}m"], errors="coerce")
        work = pd.DataFrame(
            {
                "code": eval_frame["code"].astype(str),
                "trade_time": eval_frame["trade_time"],
                "rank": signal_rank,
                "ret": label,
                "signal": signal,
            }
        ).dropna(subset=["rank", "ret"])
        previous_top: set[str] | None = None
        previous_bottom: set[str] | None = None
        for trade_time, block in work.groupby("trade_time", sort=True):
            if len(block) < min_obs:
                continue
            top_block = block.loc[block["rank"] >= q_high]
            bottom_block = block.loc[block["rank"] <= q_low]
            if top_block.empty or bottom_block.empty:
                continue
            top_codes = set(top_block["code"].astype(str))
            bottom_codes = set(bottom_block["code"].astype(str))
            if previous_top is None or previous_bottom is None:
                one_way_turnover = 1.0
            else:
                top_turn = 1.0 - (len(top_codes & previous_top) / max(1, len(top_codes)))
                bottom_turn = 1.0 - (len(bottom_codes & previous_bottom) / max(1, len(bottom_codes)))
                one_way_turnover = (top_turn + bottom_turn) / 2.0
            previous_top = top_codes
            previous_bottom = bottom_codes

            raw_return = float(top_block["ret"].mean() - bottom_block["ret"].mean()) * direction
            trading_cost = 2.0 * one_way_cost * one_way_turnover
            net_return = raw_return - trading_cost
            ts = pd.Timestamp(trade_time)
            rows.append(
                {
                    "candidate_id": candidate.get("candidate_id"),
                    "expression_hash": candidate.get("expression_hash"),
                    "run": candidate.get("run"),
                    "source_round": candidate.get("round_id"),
                    "factor_lane": candidate.get("factor_lane"),
                    "shard_index": shard_index,
                    "split": split_by_time.get(ts, "unassigned"),
                    "trade_time": ts.isoformat(),
                    "trade_date": ts.date().isoformat(),
                    "horizon_min": horizon,
                    "long_count": int(len(top_block) if direction > 0 else len(bottom_block)),
                    "short_count": int(len(bottom_block) if direction > 0 else len(top_block)),
                    "raw_return": raw_return,
                    "trading_cost": trading_cost,
                    "net_return": net_return,
                    "one_way_turnover": one_way_turnover,
                    "top_mean_return": float(top_block["ret"].mean()),
                    "bottom_mean_return": float(bottom_block["ret"].mean()),
                    "top_signal_mean": float(top_block["signal"].mean()),
                    "bottom_signal_mean": float(bottom_block["signal"].mean()),
                    "cost_bps": cost_bps,
                }
            )
    return rows


def _curve_rows(rows: list[dict[str, Any]], *, split: str, horizon: int | None = None) -> list[dict[str, Any]]:
    filtered = [
        row
        for row in rows
        if str(row.get("split")) == split and (horizon is None or int(row.get("horizon_min") or -1) == horizon)
    ]
    if not filtered:
        return []
    frame = pd.DataFrame(filtered)
    frame["net_return"] = pd.to_numeric(frame["net_return"], errors="coerce")
    frame["raw_return"] = pd.to_numeric(frame["raw_return"], errors="coerce")
    frame["one_way_turnover"] = pd.to_numeric(frame["one_way_turnover"], errors="coerce")
    grouped = (
        frame.groupby(["trade_time", "trade_date"], sort=True)
        .agg(
            net_return=("net_return", "mean"),
            raw_return=("raw_return", "mean"),
            one_way_turnover=("one_way_turnover", "mean"),
            sleeve_count=("net_return", "count"),
        )
        .reset_index()
    )
    return grouped.to_dict("records")


def _daily_returns(curve_rows: list[dict[str, Any]]) -> list[float]:
    if not curve_rows:
        return []
    frame = pd.DataFrame(curve_rows)
    frame["net_return"] = pd.to_numeric(frame["net_return"], errors="coerce")
    daily = frame.groupby("trade_date", sort=True)["net_return"].sum()
    return [float(value) for value in daily.to_numpy(dtype=float) if math.isfinite(float(value))]


def _bootstrap_days(day_values: list[float], *, iterations: int, seed: int) -> dict[str, Any]:
    clean = [float(value) for value in day_values if math.isfinite(float(value))]
    if not clean:
        return {"iterations": 0, "day_count": 0}
    rng = random.Random(seed)
    draws: list[float] = []
    positives = 0
    for _ in range(iterations):
        sample = [clean[rng.randrange(len(clean))] for _ in range(len(clean))]
        value = _sortino(sample)
        if value is None:
            continue
        draws.append(value)
        positives += int(value > 0)
    return {
        "iterations": len(draws),
        "day_count": len(clean),
        "p05": _round(_quantile(draws, 0.05)),
        "p25": _round(_quantile(draws, 0.25)),
        "median": _round(_quantile(draws, 0.50)),
        "p95": _round(_quantile(draws, 0.95)),
        "prob_gt_0": _round(positives / len(draws) if draws else None),
    }


def _summarize_curve(curve_rows: list[dict[str, Any]], *, split: str, horizon: int | str, seed: int) -> dict[str, Any]:
    values = [_f(row.get("net_return")) for row in curve_rows]
    values = [value for value in values if math.isfinite(value)]
    raw = [_f(row.get("raw_return")) for row in curve_rows]
    raw = [value for value in raw if math.isfinite(value)]
    turnover = [_f(row.get("one_way_turnover")) for row in curve_rows]
    turnover = [value for value in turnover if math.isfinite(value)]
    days = sorted({str(row.get("trade_date")) for row in curve_rows if row.get("trade_date")})
    day_values = _daily_returns(curve_rows)
    boot = _bootstrap_days(day_values, iterations=600, seed=seed)
    return {
        "split": split,
        "horizon_min": horizon,
        "curve_count": len(values),
        "day_count": len(days),
        "net_mean_return": _round(statistics.fmean(values) if values else None, 10),
        "raw_mean_return": _round(statistics.fmean(raw) if raw else None, 10),
        "net_hit_rate": _round(sum(1 for value in values if value > 0) / len(values) if values else None),
        "minute_sortino": _round(_sortino(values)),
        "day_sortino": _round(_sortino(day_values)),
        "day_mcmc_sortino_p25": boot.get("p25"),
        "day_mcmc_sortino_median": boot.get("median"),
        "day_mcmc_prob_sortino_gt_0": boot.get("prob_gt_0"),
        "max_drawdown": _round(_max_drawdown(values)),
        "mean_one_way_turnover": _round(statistics.fmean(turnover) if turnover else None),
    }


def _candidate_summary(candidate: dict[str, Any], rows: list[dict[str, Any]], horizons: tuple[int, ...], *, seed: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    split_rows: list[dict[str, Any]] = []
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for split in ("train", "validation", "holdout"):
        all_curve = _curve_rows(rows, split=split)
        summary = _summarize_curve(all_curve, split=split, horizon="equal_weight_horizon_sleeves", seed=seed + len(split))
        split_rows.append(summary)
        by_key[(split, "all")] = summary
        for horizon in horizons:
            curve = _curve_rows(rows, split=split, horizon=horizon)
            horizon_summary = _summarize_curve(curve, split=split, horizon=horizon, seed=seed + horizon)
            split_rows.append(horizon_summary)
            by_key[(split, str(horizon))] = horizon_summary

    train_all = by_key.get(("train", "all"), {})
    validation_all = by_key.get(("validation", "all"), {})
    holdout_all = by_key.get(("holdout", "all"), {})
    train_horizon_sortinos = [
        _f(by_key.get(("train", str(horizon)), {}).get("day_sortino"))
        for horizon in horizons
    ]
    train_horizon_sortinos = [value for value in train_horizon_sortinos if math.isfinite(value)]
    train_worst = min(train_horizon_sortinos) if train_horizon_sortinos else float("nan")
    train_median = float(np.median(train_horizon_sortinos)) if train_horizon_sortinos else float("nan")
    instability = _safe_stdev(train_horizon_sortinos)
    train_turnover = _f(train_all.get("mean_one_way_turnover"), 0.0)
    train_day_sortino = _f(train_all.get("day_sortino"))
    train_day_mcmc_p25 = _f(train_all.get("day_mcmc_sortino_p25"))
    turnover_penalty = max(0.0, train_turnover - 0.55) * 0.75
    instability_penalty = max(0.0, _f(instability, 0.0) - 0.50) * 0.25
    inherited_blocker_penalty = 0.15 if str(candidate.get("phase3bp_blocker_flags") or candidate.get("blocker_flags") or "") else 0.0
    reward = (
        0.55 * _f(train_day_sortino, -2.0)
        + 0.25 * _f(train_worst, -2.0)
        + 0.20 * _f(train_day_mcmc_p25, -2.0)
        - turnover_penalty
        - instability_penalty
        - inherited_blocker_penalty
    )
    blockers: list[str] = []
    if not math.isfinite(train_day_sortino) or train_day_sortino <= 0.0:
        blockers.append("non_positive_train_day_sortino")
    if not math.isfinite(train_worst) or train_worst <= 0.0:
        blockers.append("non_positive_worst_horizon_train_sortino")
    if _f(train_all.get("day_mcmc_prob_sortino_gt_0"), 0.0) < 0.60:
        blockers.append("weak_train_day_mcmc")
    if train_turnover > 0.75:
        blockers.append("extreme_turnover")
    if str(candidate.get("phase3bp_blocker_flags") or candidate.get("blocker_flags") or ""):
        blockers.append("inherited_search_blocker")
    decision = "TRAIN_REWARD_FOLLOWUP_READY" if not blockers else "HOLD_TRAIN_REWARD"
    reward_row = {
        "candidate_id": candidate.get("candidate_id"),
        "expression_hash": candidate.get("expression_hash"),
        "run": candidate.get("run"),
        "source_round": candidate.get("round_id"),
        "generator_arm": candidate.get("generator_arm"),
        "generator_route": candidate.get("generator_route"),
        "source_generator": candidate.get("source_generator"),
        "source_lane": candidate.get("source_lane"),
        "factor_lane": candidate.get("factor_lane"),
        "field_family": candidate.get("field_family"),
        "primitive_family": candidate.get("primitive_family"),
        "event_state_family": candidate.get("event_state_family"),
        "horizon_bucket": candidate.get("horizon_bucket"),
        "turnover_bucket": candidate.get("turnover_bucket"),
        "family_id": candidate.get("family_id"),
        "motif_id": candidate.get("motif_id"),
        "subtree_hashes": candidate.get("subtree_hashes"),
        "phase3ca_proxy_quality": candidate.get("phase3ca_proxy_quality"),
        "proxy_quality": candidate.get("proxy_quality"),
        "aligned_ic_mean": candidate.get("aligned_ic_mean"),
        "spread_hit_rate": candidate.get("spread_hit_rate"),
        "mean_one_way_turnover": candidate.get("mean_one_way_turnover"),
        "fields": candidate.get("fields"),
        "expression": candidate.get("expression"),
        "train_reward": _round(reward),
        "train_day_sortino": train_all.get("day_sortino"),
        "train_minute_sortino": train_all.get("minute_sortino"),
        "train_worst_horizon_day_sortino": _round(train_worst),
        "train_median_horizon_day_sortino": _round(train_median),
        "train_horizon_sortino_stdev": _round(instability),
        "train_day_mcmc_p25": train_all.get("day_mcmc_sortino_p25"),
        "train_day_mcmc_prob_gt_0": train_all.get("day_mcmc_prob_sortino_gt_0"),
        "train_mean_one_way_turnover": train_all.get("mean_one_way_turnover"),
        "validation_day_sortino": validation_all.get("day_sortino"),
        "validation_day_mcmc_prob_gt_0": validation_all.get("day_mcmc_prob_sortino_gt_0"),
        "holdout_day_sortino": holdout_all.get("day_sortino"),
        "holdout_day_mcmc_prob_gt_0": holdout_all.get("day_mcmc_prob_sortino_gt_0"),
        "inherited_blockers": candidate.get("phase3bp_blocker_flags") or candidate.get("blocker_flags"),
        "train_reward_blockers": "|".join(blockers),
        "train_reward_decision": decision,
    }
    reward_row.update(normalize_candidate_schema(reward_row))
    return split_rows, reward_row


def _render_md(summary: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    ranked = sorted(rows, key=lambda row: _f(row.get("train_reward"), -999.0), reverse=True)
    lines = [
        "# Phase3CM Train Portfolio Sortino Reward Audit 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Scope",
        "",
        "Computes true1min train / validation / holdout portfolio reward curves for already-generated candidates.",
        "This replaces fragment Sortino as the intended search feedback target. It does not launch search and does not promote candidates.",
        "",
        "## Summary",
        "",
        f"- candidates: `{summary['candidate_count']}`",
        f"- portfolio pnl rows written: `{summary['portfolio_pnl_rows_written']}`",
        f"- followup-ready by train reward only: `{summary['followup_count']}`",
        f"- horizons: `{summary['horizons']}`",
        f"- train/validation/holdout fractions: `{summary['train_fraction']}` / `{summary['validation_fraction']}` / `{summary['holdout_fraction']}`",
        "",
        "## Top Train Reward Rows",
        "",
        "| rank | candidate | reward | train day sortino | worst h sortino | val sortino | holdout sortino | turnover | decision | blockers | expression |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for idx, row in enumerate(ranked[:30], 1):
        expr = str(row.get("expression") or "").replace("|", "/")[:120]
        lines.append(
            f"| {idx} | `{row.get('candidate_id')}` | {row.get('train_reward')} | {row.get('train_day_sortino')} | "
            f"{row.get('train_worst_horizon_day_sortino')} | {row.get('validation_day_sortino')} | {row.get('holdout_day_sortino')} | "
            f"{row.get('train_mean_one_way_turnover')} | `{row.get('train_reward_decision')}` | `{row.get('train_reward_blockers')}` | `{expr}` |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This is train-set reward evidence, not final alpha proof.",
            "- Validation and holdout columns are reported for leakage control; searchers must not optimize holdout.",
            "- Horizon sleeves are equal-weighted at each trade_time before portfolio Sortino is computed.",
            "- Costs use turnover-adjusted long-short one-way turnover; this is still not a full fill simulator.",
            "- Phase3BZ fragment replay remains available only as diagnostic slice replay.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-audit", type=Path, default=DEFAULT_CANDIDATE_AUDIT)
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--candidate-limit", type=int, default=64)
    parser.add_argument("--max-shards", type=int, default=8)
    parser.add_argument("--sample-trade-times-per-shard", type=int, default=240)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--min-obs-per-time", type=int, default=20)
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--top-quantile", type=float, default=0.2)
    parser.add_argument("--write-pnl-rows", action="store_true")
    parser.add_argument("--fast-mode", action="store_true")
    parser.add_argument("--numexpr-threads", type=int, default=4)
    args = parser.parse_args(argv)

    if args.fast_mode:
        os.environ.setdefault("NUMEXPR_MAX_THREADS", str(args.numexpr_threads))
        os.environ.setdefault("OMP_NUM_THREADS", "1")
        os.environ.setdefault("MKL_NUM_THREADS", "1")

    if args.train_fraction <= 0 or args.validation_fraction < 0 or args.train_fraction + args.validation_fraction >= 1:
        raise ValueError("train_fraction must be >0 and train_fraction + validation_fraction must be < 1")

    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    horizons = tuple(int(item.strip()) for item in str(args.horizons).split(",") if item.strip())
    candidates = _load_candidates(_resolve(args.candidate_audit), args.candidate_limit)
    panels = _discover_panels(_resolve(args.shard_root), args.max_shards)

    rows_by_hash: dict[str, list[dict[str, Any]]] = {str(candidate["expression_hash"]): [] for candidate in candidates}
    pnl_rows: list[dict[str, Any]] = []
    shard_meta: list[dict[str, Any]] = []
    for shard_index, panel in enumerate(panels):
        frame, eval_mask, eval_frame, labels, signal_times, meta = _read_train_shard(
            candidates=candidates,
            panel_path=panel,
            horizons=horizons,
            sample_trade_times=args.sample_trade_times_per_shard,
        )
        meta["shard_index"] = shard_index
        split_by_time = _split_map(signal_times, args.train_fraction, args.validation_fraction)
        expression_cache: dict[str, pd.Series] = {}
        shard_rows = 0
        for candidate in candidates:
            rows = _candidate_portfolio_rows_from_frame(
                candidate=candidate,
                frame=frame,
                eval_mask=eval_mask,
                eval_frame=eval_frame,
                labels=labels,
                split_by_time=split_by_time,
                shard_index=shard_index,
                horizons=horizons,
                min_obs=args.min_obs_per_time,
                cost_bps=args.cost_bps,
                top_quantile=args.top_quantile,
                expression_cache=expression_cache,
            )
            rows_by_hash[str(candidate["expression_hash"])].extend(rows)
            shard_rows += len(rows)
            if args.write_pnl_rows:
                pnl_rows.extend(rows)
        meta["portfolio_pnl_rows"] = shard_rows
        meta["expression_cache_size"] = len(expression_cache)
        shard_meta.append(meta)
        del frame, eval_mask, eval_frame, labels, expression_cache

    split_horizon_rows: list[dict[str, Any]] = []
    reward_rows: list[dict[str, Any]] = []
    for idx, candidate in enumerate(candidates, 1):
        rows = rows_by_hash[str(candidate["expression_hash"])]
        per_split, reward_row = _candidate_summary(candidate, rows, horizons, seed=20260623 + idx)
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

    reward_rows.sort(key=lambda row: _f(row.get("train_reward"), -999.0), reverse=True)
    followup_count = sum(1 for row in reward_rows if row.get("train_reward_decision") == "TRAIN_REWARD_FOLLOWUP_READY")
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cm_train_portfolio_sortino_reward_audit",
        "decision": "PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY",
        "candidate_count": len(candidates),
        "followup_count": followup_count,
        "input_candidate_audit": str(_resolve(args.candidate_audit)),
        "shard_root": str(_resolve(args.shard_root)),
        "max_shards": args.max_shards,
        "sample_trade_times_per_shard": args.sample_trade_times_per_shard,
        "horizons": list(horizons),
        "train_fraction": args.train_fraction,
        "validation_fraction": args.validation_fraction,
        "holdout_fraction": round(1.0 - args.train_fraction - args.validation_fraction, 8),
        "cost_bps": args.cost_bps,
        "top_quantile": args.top_quantile,
        "portfolio_pnl_rows_written": len(pnl_rows) if args.write_pnl_rows else 0,
        "metric_boundary": "train portfolio Sortino reward audit; not production proof; holdout must not feed search",
        "fast_mode": bool(args.fast_mode),
        "numexpr_threads": int(args.numexpr_threads),
        "python_executable": os.sys.executable,
        "package_versions": _package_versions(),
        "acceleration_contract": {
            "batched_shard_read": True,
            "column_pruned_pyarrow_read": True,
            "expression_cache_scope": "per_shard",
            "fast_group_rank": True,
            "omp_threads": os.environ.get("OMP_NUM_THREADS"),
            "mkl_threads": os.environ.get("MKL_NUM_THREADS"),
            "numexpr_max_threads": os.environ.get("NUMEXPR_MAX_THREADS"),
            "parallel_workers": 1,
            "global_worker_limit": 1,
        },
    }
    if args.write_pnl_rows:
        _write_csv(output_root / "phase3cm_portfolio_pnl_rows.csv", pnl_rows)
    _write_csv(output_root / "phase3cm_candidate_split_horizon_summary.csv", split_horizon_rows)
    _write_csv(output_root / "phase3cm_candidate_train_reward_summary.csv", reward_rows)
    _write_csv(output_root / "phase3cm_train_reward.csv", reward_rows)
    _write_csv(output_root / "phase3cm_shard_meta.csv", shard_meta)
    _write_json(output_root / "phase3cm_train_reward_audit_summary.json", summary)
    _write_csv(report_root / "phase3cm_candidate_train_reward_summary.csv", reward_rows)
    _write_csv(report_root / "phase3cm_train_reward.csv", reward_rows)
    _write_csv(report_root / "phase3cm_candidate_split_horizon_summary.csv", split_horizon_rows)
    _write_json(report_root / "phase3cm_train_reward_audit_summary.json", summary)
    (report_root / "PHASE3CM_TRAIN_PORTFOLIO_SORTINO_REWARD_AUDIT_20260623.md").write_text(
        _render_md(summary, reward_rows),
        encoding="utf-8",
    )
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
