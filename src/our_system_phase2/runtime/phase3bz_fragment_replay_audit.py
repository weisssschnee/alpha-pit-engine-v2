"""Fragment replay audit for Phase3BV/BX true-1min candidates.

This expands proxy BV candidates back into tradable minute fragments:
candidate x shard x trade_time x horizon. It is a diagnostic replay bridge,
not a production fill simulator.
"""

from __future__ import annotations

import argparse
import csv
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
from our_system_phase2.services.real_market_validation import evaluate_panel_expression


REPO = Path(__file__).resolve().parents[3]
DEFAULT_BX_AUDIT = Path("reports/phase3bx_bv_sortino_mcmc_audit_20260616/phase3bx_bv_sortino_mcmc_audit.csv")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3bz_fragment_replay_audit_20260616")
DEFAULT_REPORT_ROOT = Path("reports/phase3bz_fragment_replay_audit_20260616")


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


def _package_versions() -> dict[str, str]:
    packages = ["numpy", "pandas", "pyarrow", "numba", "bottleneck", "numexpr", "polars", "joblib", "scikit-learn"]
    versions: dict[str, str] = {}
    for package in packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "missing"
    return versions


def _hot_path_scan() -> dict[str, Any]:
    text = Path(__file__).read_text(encoding="utf-8", errors="ignore")
    patterns = [
        "_read_windowed_panel",
        "evaluate_panel_expression",
        "groupby(",
        "sort=False",
        "sort=True",
        "pyarrow",
        "numba",
        "polars",
        "numexpr",
    ]
    return {
        "module": "phase3bz_fragment_replay_audit",
        "hits": [pattern for pattern in patterns if pattern in text],
        "interpretation": "batched shard read; pandas/numpy expression and groupby path; pyarrow column/time filtering is active via _read_windowed_panel; numba is not treated as active unless evaluator calls it",
    }


def _sortino(values: list[float], annualizer: float = 1.0) -> float | None:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if not clean:
        return None
    mean = statistics.fmean(clean)
    downside = [min(0.0, v) for v in clean]
    downside_var = statistics.fmean([v * v for v in downside])
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
    clean = sorted(float(v) for v in values if math.isfinite(float(v)))
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


def _bootstrap(values: list[float], *, iterations: int, seed: int) -> dict[str, Any]:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if not clean:
        return {"iterations": 0}
    rng = random.Random(seed)
    draws: list[float] = []
    positives = 0
    for _ in range(iterations):
        sample = [clean[rng.randrange(len(clean))] for _ in range(len(clean))]
        val = _sortino(sample)
        if val is None:
            continue
        draws.append(val)
        positives += int(val > 0)
    return {
        "iterations": len(draws),
        "p05": _round(_quantile(draws, 0.05)),
        "median": _round(_quantile(draws, 0.50)),
        "p95": _round(_quantile(draws, 0.95)),
        "prob_gt_0": _round(positives / len(draws) if draws else None),
    }


def _block_bootstrap_by_day(fragment_rows: list[dict[str, Any]], *, iterations: int, seed: int) -> dict[str, Any]:
    by_day: dict[str, list[float]] = {}
    for row in fragment_rows:
        day = str(row.get("trade_date") or "")
        value = _f(row.get("net_spread_return"))
        if day and math.isfinite(value):
            by_day.setdefault(day, []).append(value)
    days = sorted(by_day)
    if not days:
        return {"iterations": 0, "day_count": 0}
    rng = random.Random(seed)
    draws: list[float] = []
    positives = 0
    for _ in range(iterations):
        sample_values: list[float] = []
        for _ in range(len(days)):
            sample_values.extend(by_day[rng.choice(days)])
        val = _sortino(sample_values)
        if val is None:
            continue
        draws.append(val)
        positives += int(val > 0)
    return {
        "iterations": len(draws),
        "day_count": len(days),
        "p05": _round(_quantile(draws, 0.05)),
        "median": _round(_quantile(draws, 0.50)),
        "p95": _round(_quantile(draws, 0.95)),
        "prob_gt_0": _round(positives / len(draws) if draws else None),
    }


def _load_candidates(path: Path, limit: int) -> list[dict[str, Any]]:
    rows = _read_csv(path)
    rows.sort(
        key=lambda row: (
            _f(row.get("mcmc_sortino_median"), -999.0),
            _f(row.get("proxy_sortino"), -999.0),
        ),
        reverse=True,
    )
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        digest = str(row.get("expression_hash") or "")
        if not digest or digest in seen:
            continue
        expr = str(row.get("expression") or "")
        if not expr:
            continue
        seen.add(digest)
        item = dict(row)
        item["fields_list"] = _fields(expr)
        item["max_window"] = _max_expression_window(expr)
        selected.append(item)
        if len(selected) >= limit:
            break
    if not selected:
        raise RuntimeError(f"no candidates selected from {path}")
    return selected


def _read_fragment_shard(
    *,
    candidates: list[dict[str, Any]],
    panel_path: Path,
    horizons: tuple[int, ...],
    sample_trade_times: int,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
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
    import pyarrow.parquet as pq

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
    return frame, eval_mask, eval_frame, labels, meta


def _trade_fragments_from_frame(
    *,
    candidate: dict[str, Any],
    frame: pd.DataFrame,
    eval_mask: pd.Series,
    eval_frame: pd.DataFrame,
    labels: pd.DataFrame,
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
    cost = float(cost_bps) / 10000.0
    fragments: list[dict[str, Any]] = []
    q_low = float(top_quantile)
    q_high = 1.0 - float(top_quantile)
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
        for trade_time, block in work.groupby("trade_time", sort=False):
            if len(block) < min_obs:
                continue
            top = block.loc[block["rank"] >= q_high]
            bottom = block.loc[block["rank"] <= q_low]
            if top.empty or bottom.empty:
                continue
            raw_spread = float(top["ret"].mean() - bottom["ret"].mean()) * direction
            net_spread = raw_spread - (2.0 * cost)
            fragments.append(
                {
                    "candidate_id": candidate.get("candidate_id"),
                    "expression_hash": candidate.get("expression_hash"),
                    "run": candidate.get("run"),
                    "source_round": candidate.get("round_id"),
                    "shard_index": shard_index,
                    "trade_time": pd.Timestamp(trade_time).isoformat(),
                    "trade_date": pd.Timestamp(trade_time).date().isoformat(),
                    "horizon_min": horizon,
                    "long_count": int(len(top) if direction > 0 else len(bottom)),
                    "short_count": int(len(bottom) if direction > 0 else len(top)),
                    "raw_spread_return": raw_spread,
                    "net_spread_return": net_spread,
                    "top_mean_return": float(top["ret"].mean()),
                    "bottom_mean_return": float(bottom["ret"].mean()),
                    "top_signal_mean": float(top["signal"].mean()),
                    "bottom_signal_mean": float(bottom["signal"].mean()),
                    "cost_bps": cost_bps,
                }
            )
    return fragments


def _trade_fragments(
    *,
    candidate: dict[str, Any],
    panel_path: Path,
    shard_index: int,
    horizons: tuple[int, ...],
    sample_trade_times: int,
    min_obs: int,
    cost_bps: float,
    top_quantile: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    expression = str(candidate["expression"])
    max_window = int(candidate.get("max_window") or 0)
    max_horizon = max(horizons)
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
        *candidate["fields_list"],
    }
    import pyarrow.parquet as pq

    schema = set(pq.ParquetFile(panel_path).schema_arrow.names)
    columns = [column for column in sorted(required) if column in schema]
    missing = sorted({"code", "trade_time", "date", "close"} - set(columns))
    if missing:
        raise RuntimeError(f"{panel_path} missing required columns {missing}")
    missing_fields = sorted(set(candidate["fields_list"]) - set(columns))
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
    if frame.empty:
        return [], {"eval_rows": 0, "signal_trade_times": 0}
    eval_mask = frame["trade_time"].isin(signal_times)
    eval_frame = frame.loc[eval_mask].copy().reset_index(drop=True)
    labels_all = _future_returns(frame, horizons)
    labels = labels_all.loc[eval_mask].reset_index(drop=True)
    signal_all = pd.to_numeric(evaluate_panel_expression(frame, expression, cache={}), errors="coerce")
    signal = pd.Series(signal_all.loc[eval_mask].to_numpy(dtype=float))
    signal_rank = _rank_by_group(signal, eval_frame["trade_time"])
    direction = 1.0 if str(candidate.get("open_direction") or "long_top") == "long_top" else -1.0
    cost = float(cost_bps) / 10000.0
    fragments: list[dict[str, Any]] = []
    q_low = float(top_quantile)
    q_high = 1.0 - float(top_quantile)
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
        for trade_time, block in work.groupby("trade_time", sort=True):
            if len(block) < min_obs:
                continue
            top = block.loc[block["rank"] >= q_high]
            bottom = block.loc[block["rank"] <= q_low]
            if top.empty or bottom.empty:
                continue
            raw_spread = float(top["ret"].mean() - bottom["ret"].mean()) * direction
            # Approximate round-trip long-short fragment cost. This is a
            # diagnostic proxy; strict execution replay must replace it.
            net_spread = raw_spread - (2.0 * cost)
            fragments.append(
                {
                    "candidate_id": candidate.get("candidate_id"),
                    "expression_hash": candidate.get("expression_hash"),
                    "run": candidate.get("run"),
                    "source_round": candidate.get("round_id"),
                    "shard_index": shard_index,
                    "trade_time": pd.Timestamp(trade_time).isoformat(),
                    "trade_date": pd.Timestamp(trade_time).date().isoformat(),
                    "horizon_min": horizon,
                    "long_count": int(len(top) if direction > 0 else len(bottom)),
                    "short_count": int(len(bottom) if direction > 0 else len(top)),
                    "raw_spread_return": raw_spread,
                    "net_spread_return": net_spread,
                    "top_mean_return": float(top["ret"].mean()),
                    "bottom_mean_return": float(bottom["ret"].mean()),
                    "top_signal_mean": float(top["signal"].mean()),
                    "bottom_signal_mean": float(bottom["signal"].mean()),
                    "cost_bps": cost_bps,
                }
            )
    meta = {
        "panel": str(panel_path),
        "shard_index": shard_index,
        "read_rows": int(len(frame)),
        "eval_rows": int(len(eval_frame)),
        "signal_trade_times": int(signal_time_count),
        "read_trade_times": int(read_time_count),
        "fragments": len(fragments),
    }
    return fragments, meta


def _summarize_candidate(candidate: dict[str, Any], rows: list[dict[str, Any]], *, seed: int) -> dict[str, Any]:
    values = [_f(row.get("net_spread_return")) for row in rows]
    values = [v for v in values if math.isfinite(v)]
    raw = [_f(row.get("raw_spread_return")) for row in rows]
    raw = [v for v in raw if math.isfinite(v)]
    horizons = sorted({str(row.get("horizon_min")) for row in rows})
    shards = sorted({str(row.get("shard_index")) for row in rows})
    days = sorted({str(row.get("trade_date")) for row in rows})
    boot = _bootstrap(values, iterations=1000, seed=seed)
    block_boot = _block_bootstrap_by_day(rows, iterations=1000, seed=seed + 17)
    turnover = _f(candidate.get("mean_one_way_turnover"))
    blockers: list[str] = []
    if not values:
        blockers.append("no_fragments")
    if boot.get("prob_gt_0") is not None and float(boot["prob_gt_0"]) < 0.80:
        blockers.append("weak_fragment_mcmc")
    if block_boot.get("prob_gt_0") is not None and float(block_boot["prob_gt_0"]) < 0.75:
        blockers.append("weak_day_block_mcmc")
    if math.isfinite(turnover) and turnover > 0.75:
        blockers.append("extreme_turnover")
    if str(candidate.get("phase3bp_blocker_flags") or ""):
        blockers.append("inherited_bp_blocker")
    decision = "FRAGMENT_REPLAY_FOLLOWUP" if not blockers else "HOLD_FRAGMENT_REPLAY"
    return {
        "candidate_id": candidate.get("candidate_id"),
        "expression_hash": candidate.get("expression_hash"),
        "run": candidate.get("run"),
        "source_round": candidate.get("round_id"),
        "factor_lane": candidate.get("factor_lane"),
        "fields": candidate.get("fields"),
        "expression": candidate.get("expression"),
        "fragment_count": len(values),
        "day_count": len(days),
        "shard_count": len(shards),
        "horizon_count": len(horizons),
        "raw_mean_return": _round(statistics.fmean(raw) if raw else None, 10),
        "net_mean_return": _round(statistics.fmean(values) if values else None, 10),
        "net_hit_rate": _round(sum(1 for v in values if v > 0) / len(values) if values else None),
        "fragment_sortino": _round(_sortino(values)),
        "fragment_max_drawdown": _round(_max_drawdown(values)),
        "mcmc_sortino_p05": boot.get("p05"),
        "mcmc_sortino_median": boot.get("median"),
        "mcmc_sortino_p95": boot.get("p95"),
        "mcmc_prob_sortino_gt_0": boot.get("prob_gt_0"),
        "day_block_sortino_p05": block_boot.get("p05"),
        "day_block_sortino_median": block_boot.get("median"),
        "day_block_prob_sortino_gt_0": block_boot.get("prob_gt_0"),
        "mean_one_way_turnover": _round(turnover),
        "inherited_bp_blockers": candidate.get("phase3bp_blocker_flags"),
        "fragment_blockers": "|".join(blockers),
        "fragment_decision": decision,
    }


def _render_md(summary: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3BZ Fragment Replay Audit 2026-06-16",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Scope",
        "",
        "Replays selected BV/BX formulas on true `trade_time` 1min shards and expands proxy spread into trade-time fragments.",
        "",
        "## Summary",
        "",
        f"- candidates: `{summary['candidate_count']}`",
        f"- fragments: `{summary['fragment_count']}`",
        f"- followup: `{summary['followup_count']}`",
        f"- cost bps: `{summary['cost_bps']}`",
        f"- max shards: `{summary['max_shards']}`",
        f"- sampled trade times per shard: `{summary['sample_trade_times_per_shard']}`",
        "",
        "## Top Candidates",
        "",
        "| rank | candidate | fragments | days | sortino | mcmc median | day-block prob>0 | turnover | decision | blockers | expression |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    ranked = sorted(
        rows,
        key=lambda row: (
            str(row.get("fragment_decision") == "FRAGMENT_REPLAY_FOLLOWUP"),
            _f(row.get("day_block_prob_sortino_gt_0"), -1.0),
            _f(row.get("mcmc_sortino_median"), -999.0),
            _f(row.get("fragment_sortino"), -999.0),
        ),
        reverse=True,
    )
    for idx, row in enumerate(ranked[:30], 1):
        expr = str(row.get("expression") or "").replace("|", "/")[:120]
        lines.append(
            f"| {idx} | `{row.get('candidate_id')}` | {row.get('fragment_count')} | {row.get('day_count')} | "
            f"{row.get('fragment_sortino')} | {row.get('mcmc_sortino_median')} | {row.get('day_block_prob_sortino_gt_0')} | "
            f"{row.get('mean_one_way_turnover')} | `{row.get('fragment_decision')}` | `{row.get('fragment_blockers')}` | `{expr}` |"
        )
    lines.extend(
        [
            "",
            "## Bias Audit Boundary",
            "",
            "- Signal is computed at sampled true `trade_time`; labels use future minute close returns.",
            "- Cost model is a simple long-short fragment cost proxy, not real fill simulation.",
            "- Day-block bootstrap is used to reduce minute-fragment overconfidence.",
            "- Inherited BP blockers are retained; this audit does not override wrong-lag/crowding flags.",
            "- Decision is HOLD unless fragment evidence survives cost, day-block stability, and blocker checks.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bx-audit", type=Path, default=DEFAULT_BX_AUDIT)
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--candidate-limit", type=int, default=12)
    parser.add_argument("--max-shards", type=int, default=4)
    parser.add_argument("--sample-trade-times-per-shard", type=int, default=120)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs-per-time", type=int, default=20)
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--top-quantile", type=float, default=0.2)
    parser.add_argument("--fast-mode", action="store_true")
    parser.add_argument("--numexpr-threads", type=int, default=4)
    args = parser.parse_args(argv)

    if args.fast_mode:
        os.environ.setdefault("NUMEXPR_MAX_THREADS", str(args.numexpr_threads))
        os.environ.setdefault("OMP_NUM_THREADS", "1")
        os.environ.setdefault("MKL_NUM_THREADS", "1")

    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    horizons = tuple(int(item.strip()) for item in str(args.horizons).split(",") if item.strip())
    candidates = _load_candidates(_resolve(args.bx_audit), args.candidate_limit)
    panels = _discover_panels(_resolve(args.shard_root), args.max_shards)

    fragment_rows: list[dict[str, Any]] = []
    meta_rows: list[dict[str, Any]] = []
    fragments_by_hash: dict[str, list[dict[str, Any]]] = {str(candidate["expression_hash"]): [] for candidate in candidates}
    for shard_index, panel in enumerate(panels):
        frame, eval_mask, eval_frame, labels, meta = _read_fragment_shard(
            candidates=candidates,
            panel_path=panel,
            horizons=horizons,
            sample_trade_times=args.sample_trade_times_per_shard,
        )
        meta["shard_index"] = shard_index
        expression_cache: dict[str, pd.Series] = {}
        shard_fragment_count = 0
        for candidate in candidates:
            fragments = _trade_fragments_from_frame(
                candidate=candidate,
                frame=frame,
                eval_mask=eval_mask,
                eval_frame=eval_frame,
                labels=labels,
                shard_index=shard_index,
                horizons=horizons,
                min_obs=args.min_obs_per_time,
                cost_bps=args.cost_bps,
                top_quantile=args.top_quantile,
                expression_cache=expression_cache,
            )
            shard_fragment_count += len(fragments)
            fragments_by_hash[str(candidate["expression_hash"])].extend(fragments)
            fragment_rows.extend(fragments)
        meta["fragments"] = shard_fragment_count
        meta["expression_cache_size"] = len(expression_cache)
        meta_rows.append(meta)
        del frame, eval_mask, eval_frame, labels, expression_cache

    summary_rows: list[dict[str, Any]] = []
    for cand_idx, candidate in enumerate(candidates, 1):
        summary_rows.append(_summarize_candidate(candidate, fragments_by_hash[str(candidate["expression_hash"])], seed=20260616 + cand_idx))

    followup_count = sum(1 for row in summary_rows if row.get("fragment_decision") == "FRAGMENT_REPLAY_FOLLOWUP")
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260616_phase3bz_fragment_replay_audit",
        "decision": "HOLD_RESEARCH_FRAGMENT_REPLAY_AUDIT_COMPLETE",
        "candidate_count": len(candidates),
        "fragment_count": len(fragment_rows),
        "followup_count": followup_count,
        "cost_bps": args.cost_bps,
        "top_quantile": args.top_quantile,
        "max_shards": args.max_shards,
        "sample_trade_times_per_shard": args.sample_trade_times_per_shard,
        "horizons": list(horizons),
        "input_bx_audit": str(_resolve(args.bx_audit)),
        "shard_root": str(_resolve(args.shard_root)),
        "metric_boundary": "fragment replay from sampled true-1min panels; not full exchange fill simulation",
        "fast_mode": bool(args.fast_mode),
        "numexpr_threads": int(args.numexpr_threads),
        "python_executable": os.sys.executable,
        "package_versions": _package_versions(),
        "hot_path_scan": _hot_path_scan(),
        "acceleration_contract": {
            "batched_shard_read": True,
            "column_pruned_pyarrow_read": True,
            "expression_cache_scope": "per_shard",
            "omp_threads": os.environ.get("OMP_NUM_THREADS"),
            "mkl_threads": os.environ.get("MKL_NUM_THREADS"),
            "numexpr_max_threads": os.environ.get("NUMEXPR_MAX_THREADS"),
            "parallel_workers": 1,
            "global_worker_limit": 1,
        },
    }
    _write_csv(output_root / "phase3bz_fragment_rows.csv", fragment_rows)
    _write_csv(output_root / "phase3bz_candidate_fragment_summary.csv", summary_rows)
    _write_csv(output_root / "phase3bz_shard_meta.csv", meta_rows)
    _write_json(output_root / "phase3bz_fragment_replay_summary.json", summary)
    _write_csv(report_root / "phase3bz_candidate_fragment_summary.csv", summary_rows)
    _write_json(report_root / "phase3bz_fragment_replay_summary.json", summary)
    (report_root / "PHASE3BZ_FRAGMENT_REPLAY_AUDIT_20260616.md").write_text(_render_md(summary, summary_rows), encoding="utf-8")
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
