from __future__ import annotations

import argparse
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import asdict
from datetime import datetime, timezone
import gc
import json
import math
import os
from pathlib import Path
import platform
import sys
import time
from typing import Any, Mapping

import numpy as np
import pandas as pd
import psutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import build_cn_portfolio_decoder_autopsy_v1 as v1
from scripts import run_cn_finalist_mark_to_market_replay as mtm
from scripts import run_cn_finalist_replay_then_oos as base
from our_system_phase2.services.a_share_executable_replay import (
    AShareCorporateActionPolicy,
    AShareExecutionPolicy,
    AShareFeeSchedule,
    ASharePortfolioDecoderPolicy,
    AShareUniversePolicy,
    ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    _prepare_sessions,
    run_a_share_long_only_replay,
)


SCHEMA_VERSION = "cn_portfolio_decoder_v2"
EXPECTED_PAIR_COUNT = 32
EXPECTED_MEMBER_COUNT = 64
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
DECODER_POLICIES = (
    ASharePortfolioDecoderPolicy(
        decoder_id="CURRENT_TOP20PCT_EQUAL",
        selection="TOP_FRACTION",
        top_fraction=0.20,
        weighting="EQUAL",
    ),
    ASharePortfolioDecoderPolicy(
        decoder_id="TOPK_10_EQUAL",
        selection="TOP_K",
        top_k=10,
        weighting="EQUAL",
    ),
    ASharePortfolioDecoderPolicy(
        decoder_id="TOPK_10_RANK",
        selection="TOP_K",
        top_k=10,
        weighting="LINEAR_DESCENDING_RANK",
    ),
)

EXECUTION_BACKENDS = ("THREAD_POOL", "PROCESS_POOL")
_PROCESS_WORKER_CONTEXT: dict[str, Any] | None = None
_PROCESS_WORKER_CANDIDATE_ROOT: Path | None = None
_PROCESS_WORKER_INPUT_DATA_SHA256: str | None = None


def _attach_execution_prices(
    field_frame: pd.DataFrame,
    price_manifest: Mapping[str, Any],
) -> pd.DataFrame:
    price_frames = [
        pd.read_parquet(
            Path(str(row["output_path"])).resolve(),
            columns=["trade_time", "code", "open", "close"],
        )
        for row in price_manifest.get("shards") or ()
    ]
    if not price_frames or len(price_frames) != int(
        price_manifest.get("source_shard_count") or -1
    ):
        raise RuntimeError("execution price shard cardinality drift")
    price_frame = pd.concat(price_frames, ignore_index=True, copy=False)
    price_frame["code"] = price_frame["code"].map(base._normalize_code)
    price_frame["trade_time"] = pd.to_datetime(
        price_frame["trade_time"], errors="raise"
    )
    price_frame["date"] = price_frame["trade_time"].dt.normalize()
    price_frame = price_frame.sort_values(
        ["code", "trade_time"], kind="mergesort"
    ).reset_index(drop=True)
    if price_frame.duplicated(["date", "code"]).any():
        raise RuntimeError("execution price sidecar has duplicate coordinates")
    field_index = pd.MultiIndex.from_frame(field_frame[["date", "code"]])
    price_index = pd.MultiIndex.from_frame(price_frame[["date", "code"]])
    if not field_index.equals(price_index):
        raise RuntimeError("execution price/feature coordinate drift")
    feature_close = pd.to_numeric(
        field_frame["close"], errors="coerce"
    ).to_numpy()
    price_close = pd.to_numeric(
        price_frame["close"], errors="coerce"
    ).to_numpy()
    if not np.allclose(
        feature_close,
        price_close,
        rtol=0.0,
        atol=0.0,
        equal_nan=True,
    ):
        raise RuntimeError("execution price/feature close parity drift")
    field_frame["open"] = pd.to_numeric(
        price_frame["open"], errors="coerce"
    ).to_numpy()
    return field_frame


def _finite(value: Any) -> float | None:
    try:
        rendered = float(value)
    except (TypeError, ValueError):
        return None
    return rendered if math.isfinite(rendered) else None


def _hash_frame(frame: pd.DataFrame) -> str:
    records = frame.astype(object).where(frame.notna(), None).to_dict(
        orient="records"
    )
    return v1._stable_hash(records)


def _top_set(frame: pd.DataFrame, column: str, count: int) -> set[str]:
    ranked = frame.dropna(subset=[column]).sort_values(
        [column, "candidate_id"],
        ascending=[False, True],
        kind="mergesort",
    )
    return set(ranked.head(count)["candidate_id"].astype(str))


def _require_persisted_accounting_ledgers(
    ledger_closure: Mapping[str, Any],
) -> None:
    if not bool(ledger_closure.get("accounting_ledgers_persisted")):
        raise RuntimeError("decoder V2 requires persisted accounting ledgers")


def _validate_executor_contract(
    *,
    execution_backend: str,
    entitlement_worker_count: int,
    executor_worker_count: int,
) -> str:
    backend = str(execution_backend).upper()
    if backend not in EXECUTION_BACKENDS:
        raise ValueError(
            f"execution_backend must be one of {EXECUTION_BACKENDS}"
        )
    if int(entitlement_worker_count) < 1 or int(entitlement_worker_count) > 32:
        raise ValueError("worker_count must be in [1, 32]")
    if int(executor_worker_count) < 1:
        raise ValueError("executor_worker_count must be positive")
    if int(executor_worker_count) > int(entitlement_worker_count):
        raise ValueError(
            "executor_worker_count cannot exceed the admitted CPU entitlement"
        )
    return backend


def _load_evaluation_context(
    *,
    contract_path: Path,
    train_field_root: Path,
    qualification_mode: bool,
    validated_field_manifest: Mapping[str, Any] | None = None,
    validated_field_manifest_path: Path | None = None,
    execution_price_root: Path | None = None,
    validated_execution_price_manifest: Mapping[str, Any] | None = None,
    validated_execution_price_manifest_path: Path | None = None,
) -> dict[str, Any]:
    contract = v1._read_json(contract_path)
    base._verify_payload_hash(
        contract,
        field="contract_payload_sha256",
        label="decoder V2 execution contract",
    )
    if (validated_field_manifest is None) != (
        validated_field_manifest_path is None
    ):
        raise RuntimeError("validated field manifest override is incomplete")
    if validated_field_manifest is None:
        field_manifest, field_manifest_path = base._validate_sidecar(
            train_field_root,
            evaluation_role="train",
            split_hash=str(contract["split_manifest_sha256"]),
        )
    else:
        field_manifest = dict(validated_field_manifest)
        field_manifest_path = Path(validated_field_manifest_path).resolve()
        expected_manifest_path = (
            Path(train_field_root).resolve()
            / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
        )
        if field_manifest_path != expected_manifest_path:
            raise RuntimeError("validated field manifest path/root drift")
    field_frame = base._load_field_frame(train_field_root, field_manifest)
    price_override_values = (
        execution_price_root,
        validated_execution_price_manifest,
        validated_execution_price_manifest_path,
    )
    if any(value is not None for value in price_override_values):
        if not all(value is not None for value in price_override_values):
            raise RuntimeError("execution price sidecar override is incomplete")
        price_root = Path(execution_price_root).resolve()
        price_manifest = dict(validated_execution_price_manifest)
        price_manifest_path = Path(
            validated_execution_price_manifest_path
        ).resolve()
        expected_price_manifest_path = (
            price_root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
        )
        if price_manifest_path != expected_price_manifest_path:
            raise RuntimeError("execution price manifest path/root drift")
        field_frame = _attach_execution_prices(field_frame, price_manifest)
    if not field_frame["trade_time"].dt.strftime("%H:%M:%S").eq(
        "15:00:00"
    ).all():
        raise RuntimeError("decoder V2 sidecar contains intraday clocks")
    session_manifest_path = Path(
        str(contract["session_authority_manifest"])
    ).resolve()
    session_path = Path(str(contract["session_authority_path"])).resolve()
    if v1._sha256(session_manifest_path) != str(
        contract["session_authority_manifest_sha256"]
    ):
        raise RuntimeError("decoder V2 session manifest drift")
    session_authority = pd.read_parquet(session_path)
    master, observed_index, authority_index = base._materialize_replay_master(
        field_frame, session_authority
    )
    universe_raw = dict(contract["universe_policy"])
    universe_raw["allowed_exchanges"] = tuple(
        universe_raw["allowed_exchanges"]
    )
    universe = AShareUniversePolicy(**universe_raw)
    fee = AShareFeeSchedule(**dict(contract["fee_schedule"]))
    execution = AShareExecutionPolicy(**dict(contract["execution_policy"]))
    corporate = AShareCorporateActionPolicy(
        **dict(contract["corporate_action_policy"])
    )
    prepared_master = _prepare_sessions(
        master.assign(signal=0.0), universe_policy=universe
    ).drop(columns=["signal"])
    prepared_index = pd.MultiIndex.from_frame(
        prepared_master[["date", "code"]]
    )
    if set(prepared_index) != set(authority_index):
        raise RuntimeError("decoder V2 session coordinate drift")
    future_returns = v1.forward_open_to_close_returns(
        prepared_master, horizons=(1,)
    )
    codes = prepared_master["code"].astype(str).to_numpy()
    dates = pd.to_datetime(prepared_master["date"]).to_numpy()
    eligible = prepared_master[
        "promotion_universe_eligible"
    ].astype(bool).to_numpy()
    date_order = np.argsort(dates, kind="mergesort")
    sorted_dates = dates[date_order]
    boundaries = np.flatnonzero(
        np.r_[True, sorted_dates[1:] != sorted_dates[:-1], True]
    )
    date_groups = [
        date_order[start:end]
        for start, end in zip(boundaries[:-1], boundaries[1:])
    ]
    evaluation_policies = (
        (DECODER_POLICIES[0],)
        if qualification_mode
        else DECODER_POLICIES
    )
    return {
        "contract": contract,
        "field_manifest_path": field_manifest_path,
        "session_manifest_path": session_manifest_path,
        "session_path": session_path,
        "field_frame": field_frame,
        "master": master,
        "observed_index": observed_index,
        "authority_index": authority_index,
        "prepared_index": prepared_index,
        "future_returns": future_returns,
        "codes": codes,
        "dates": dates,
        "eligible": eligible,
        "date_groups": date_groups,
        "universe": universe,
        "fee": fee,
        "execution": execution,
        "corporate": corporate,
        "evaluation_policies": evaluation_policies,
    }


def _candidate_metric(
    *,
    candidate: Mapping[str, Any],
    decoder: ASharePortfolioDecoderPolicy,
    result: Mapping[str, Any],
    signal_rank_ic_mean: float | None,
    theoretical_gross_mean: float | None,
) -> dict[str, Any]:
    initial_cash = float(result["execution_policy"]["initial_cash_cny"])
    cumulative_return = float(result["ending_nav_cny"]) / initial_cash - 1.0
    turnover = _finite(result["a_share_mean_one_way_turnover"])
    return {
        "pair_id": str(candidate["pair_id"]),
        "candidate_id": str(candidate["candidate_id"]),
        "pair_member_role": str(candidate["pair_member_role"]),
        "route_id": str(candidate["route_id"]),
        "exact_identity": str(candidate["exact_identity"]),
        "decoder_id": decoder.decoder_id,
        "decoder_policy_sha256": result[
            "portfolio_decoder_policy_sha256"
        ],
        "signal_rank_ic_mean": signal_rank_ic_mean,
        "theoretical_one_session_gross_return_mean": theoretical_gross_mean,
        "continuous_book_net_reward": float(
            result["a_share_executable_net_reward"]
        ),
        "ending_nav_cny": float(result["ending_nav_cny"]),
        "cumulative_net_return": cumulative_return,
        "cumulative_net_pnl_cny": float(result["cumulative_net_pnl_cny"]),
        "cumulative_realized_trade_pnl_cny": float(
            result["cumulative_realized_trade_pnl_cny"]
        ),
        "cumulative_corporate_action_cash_pnl_cny": float(
            result["cumulative_corporate_action_cash_pnl_cny"]
        ),
        "ending_unrealized_pnl_cny": float(
            result["ending_unrealized_pnl_cny"]
        ),
        "total_fees_cny": float(result["total_fees_cny"]),
        "mean_one_way_turnover": turnover,
        "net_return_per_turnover": (
            cumulative_return / turnover
            if turnover is not None and turnover > 0
            else None
        ),
        "ending_holdings_weight": _finite(
            result["ending_holdings_weight"]
        ),
        "share_weighted_average_position_age_sessions": _finite(
            result["share_weighted_average_position_age_sessions"]
        ),
        "maximum_position_age_sessions": int(
            result["maximum_position_age_sessions"]
        ),
        "fill_count": int(result["fill_count"]),
        "trade_count": int(result["trade_count"]),
        "blocked_buy_count": int(result["blocked_buy_count"]),
        "blocked_sell_count": int(result["blocked_sell_count"]),
        "ending_holding_count": int(result["ending_holding_count"]),
        "accounting_invariants_status": str(
            result["accounting_invariants"]["status"]
        ),
        "maximum_cash_identity_error_cny": float(
            result["accounting_invariants"][
                "maximum_cash_identity_error_cny"
            ]
        ),
        "maximum_nav_identity_error_cny": float(
            result["accounting_invariants"][
                "maximum_nav_identity_error_cny"
            ]
        ),
        "maximum_pnl_identity_error_cny": float(
            result["accounting_invariants"][
                "maximum_pnl_identity_error_cny"
            ]
        ),
        "maximum_lot_quantity_error": int(
            result["accounting_invariants"]["maximum_lot_quantity_error"]
        ),
        "daily_sha256": _hash_frame(result["daily"]),
        "fills_sha256": _hash_frame(result["fills"]),
        "daily_accounting_ledger_sha256": _hash_frame(
            result["daily_accounting_ledger"]
        ),
        "lot_ledger_sha256": _hash_frame(result["lot_ledger"]),
        "lot_consumption_ledger_sha256": _hash_frame(
            result["lot_consumption_ledger"]
        ),
        "signal_clock": "SESSION_CLOSE_T",
        "execution_clock": "NEXT_SESSION_OPEN_T_PLUS_1",
        "target_refresh_clock": decoder.target_refresh_clock,
        "session_end_policy": decoder.session_end_policy,
        "fixed_decoder_horizon_sessions": 1,
        "horizon_interpretation": (
            "DAILY_TARGET_REFRESH_WITH_FINAL_CLOSE_MARK_AND_NO_FORCED_SALE"
        ),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }


def _evaluate_candidate_with_context(
    candidate: Mapping[str, Any],
    *,
    context: Mapping[str, Any],
    candidate_root: Path,
    input_data_sha256: str,
) -> list[dict[str, Any]]:
    candidate_id = str(candidate["candidate_id"])
    target = candidate_root / f"{candidate_id}.json"
    signal_field = pd.to_numeric(
        base.evaluate_panel_expression(
            context["field_frame"],
            str(candidate["expression"]),
            cache={},
            data_role="development",
        ),
        errors="coerce",
    )
    signal_series = pd.Series(
        signal_field.to_numpy(), index=context["observed_index"]
    )
    aligned_signal = signal_series.reindex(
        context["prepared_index"]
    ).to_numpy(dtype=float)
    theoretical = v1.evaluate_candidate_decoder_matrix(
        candidate=candidate,
        signal=aligned_signal,
        codes=context["codes"],
        dates=context["dates"],
        eligible=context["eligible"],
        future_returns=context["future_returns"],
        date_groups=context["date_groups"],
        decoders=tuple(
            {
                "decoder_id": policy.decoder_id,
                "selection": policy.selection,
                "weighting": policy.weighting,
                **(
                    {"top_fraction": policy.top_fraction}
                    if policy.top_fraction is not None
                    else {"top_k": policy.top_k}
                ),
            }
            for policy in context["evaluation_policies"]
        ),
    )
    theoretical_by_decoder = {
        str(row["decoder_id"]): row for row in theoretical
    }
    replay_signal = signal_series.reindex(
        context["authority_index"]
    ).to_numpy()
    replay_frame = context["master"].copy()
    replay_frame["signal"] = replay_signal
    metrics: list[dict[str, Any]] = []
    for decoder in context["evaluation_policies"]:
        result = run_a_share_long_only_replay(
            replay_frame,
            fee_schedule=context["fee"],
            universe_policy=context["universe"],
            execution_policy=context["execution"],
            corporate_action_policy=context["corporate"],
            portfolio_decoder_policy=decoder,
            ending_book_policy=ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
        )
        theory = theoretical_by_decoder[decoder.decoder_id]
        metrics.append(
            _candidate_metric(
                candidate=candidate,
                decoder=decoder,
                result=result,
                signal_rank_ic_mean=_finite(theory["signal_rank_ic_mean"]),
                theoretical_gross_mean=_finite(
                    theory["gross_forward_return_mean"]
                ),
            )
        )
    payload = {
        "schema_version": "cn_portfolio_decoder_v2_candidate_v1",
        "status": "DECODER_V2_CANDIDATE_CLOSED_IMMUTABLE",
        "candidate_id": candidate_id,
        "input_data_sha256": input_data_sha256,
        "metrics": metrics,
    }
    payload["payload_sha256"] = v1._stable_hash(payload)
    v1._write_json(target, payload)
    return metrics


def _initialize_process_worker(
    contract_path: str,
    train_field_root: str,
    qualification_mode: bool,
    candidate_root: str,
    input_data_sha256: str,
) -> None:
    global _PROCESS_WORKER_CONTEXT
    global _PROCESS_WORKER_CANDIDATE_ROOT
    global _PROCESS_WORKER_INPUT_DATA_SHA256
    _PROCESS_WORKER_CONTEXT = _load_evaluation_context(
        contract_path=Path(contract_path),
        train_field_root=Path(train_field_root),
        qualification_mode=bool(qualification_mode),
    )
    _PROCESS_WORKER_CANDIDATE_ROOT = Path(candidate_root)
    _PROCESS_WORKER_INPUT_DATA_SHA256 = str(input_data_sha256)


def _evaluate_candidate_in_process(
    candidate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if (
        _PROCESS_WORKER_CONTEXT is None
        or _PROCESS_WORKER_CANDIDATE_ROOT is None
        or _PROCESS_WORKER_INPUT_DATA_SHA256 is None
    ):
        raise RuntimeError("decoder V2 process worker was not initialized")
    return _evaluate_candidate_with_context(
        candidate,
        context=_PROCESS_WORKER_CONTEXT,
        candidate_root=_PROCESS_WORKER_CANDIDATE_ROOT,
        input_data_sha256=_PROCESS_WORKER_INPUT_DATA_SHA256,
    )


def _resource_snapshot() -> tuple[int, int, int]:
    process = psutil.Process()
    parent_rss = int(process.memory_info().rss)
    tree_rss = parent_rss
    for child in process.children(recursive=True):
        try:
            tree_rss += int(child.memory_info().rss)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return (
        int(psutil.virtual_memory().available),
        parent_rss,
        tree_rss,
    )


def _baseline_parity(
    metrics: pd.DataFrame,
    baseline: pd.DataFrame,
    *,
    expected_member_count: int = EXPECTED_MEMBER_COUNT,
) -> pd.DataFrame:
    current = metrics[
        metrics["decoder_id"].eq("CURRENT_TOP20PCT_EQUAL")
    ].copy()
    expected_columns = {
        "candidate_id",
        "ending_nav_cny",
        "mark_to_market_net_reward",
        "cumulative_net_return",
        "total_fees_cny",
        "fill_count",
        "blocked_buy_count",
        "blocked_sell_count",
        "cumulative_realized_trade_pnl_cny",
        "ending_unrealized_pnl_cny",
    }
    missing = sorted(expected_columns - set(baseline.columns))
    if missing:
        raise RuntimeError(f"ledger baseline fields missing: {missing}")
    ledger = baseline[list(expected_columns)].rename(
        columns={
            "mark_to_market_net_reward": "continuous_book_net_reward"
        }
    )
    merged = current.merge(
        ledger,
        on="candidate_id",
        how="inner",
        validate="one_to_one",
        suffixes=("_v2", "_ledger"),
    )
    if len(merged) != int(expected_member_count):
        raise RuntimeError("decoder V2 baseline candidate parity cardinality drift")
    comparisons = {
        "ending_nav_cny": 1e-6,
        "cumulative_net_return": 1e-12,
        "total_fees_cny": 1e-6,
        "fill_count": 0.0,
        "blocked_buy_count": 0.0,
        "blocked_sell_count": 0.0,
        "cumulative_realized_trade_pnl_cny": 1e-6,
        "ending_unrealized_pnl_cny": 1e-6,
    }
    comparisons["continuous_book_net_reward"] = 1e-12
    mismatch_columns: list[str] = []
    for name, tolerance in comparisons.items():
        left = pd.to_numeric(merged[f"{name}_v2"], errors="coerce")
        right = pd.to_numeric(merged[f"{name}_ledger"], errors="coerce")
        delta = left - right
        merged[f"{name}_delta"] = delta
        if delta.abs().gt(tolerance).any() or left.isna().ne(right.isna()).any():
            mismatch_columns.append(name)
    merged["parity_status"] = (
        "PASS" if not mismatch_columns else "FAIL"
    )
    if mismatch_columns:
        raise RuntimeError(
            f"decoder V2 baseline parity mismatch: {mismatch_columns}"
        )
    return merged


def _pair_metrics(candidate_metrics: pd.DataFrame) -> pd.DataFrame:
    primary = candidate_metrics[
        candidate_metrics["pair_member_role"].eq("PRIMARY")
    ].copy()
    control = candidate_metrics[
        candidate_metrics["pair_member_role"].eq("CONTROL")
    ].copy()
    keys = ["pair_id", "decoder_id"]
    value_columns = [
        "candidate_id",
        "continuous_book_net_reward",
        "cumulative_net_return",
        "cumulative_realized_trade_pnl_cny",
        "ending_unrealized_pnl_cny",
        "total_fees_cny",
        "mean_one_way_turnover",
        "net_return_per_turnover",
    ]
    primary = primary[keys + ["route_id", *value_columns]].rename(
        columns={name: f"primary_{name}" for name in value_columns}
    )
    control = control[keys + value_columns].rename(
        columns={name: f"control_{name}" for name in value_columns}
    )
    merged = primary.merge(
        control,
        on=keys,
        how="inner",
        validate="one_to_one",
    )
    for name in (
        "continuous_book_net_reward",
        "cumulative_net_return",
        "cumulative_realized_trade_pnl_cny",
        "ending_unrealized_pnl_cny",
    ):
        merged[f"matched_{name}_increment"] = (
            pd.to_numeric(merged[f"primary_{name}"], errors="coerce")
            - pd.to_numeric(merged[f"control_{name}"], errors="coerce")
        )
    merged["primary_absolute_positive"] = (
        merged["primary_cumulative_net_return"] > 0
    )
    merged["matched_increment_positive"] = (
        merged["matched_cumulative_net_return_increment"] > 0
    )
    merged["both_economic_gates_positive"] = (
        merged["primary_absolute_positive"]
        & merged["matched_increment_positive"]
    )
    merged["promotion_authorized"] = False
    return merged


def _rank_preservation(candidate_metrics: pd.DataFrame) -> pd.DataFrame:
    primary = candidate_metrics[
        candidate_metrics["pair_member_role"].eq("PRIMARY")
    ].copy()
    rows: list[dict[str, Any]] = []
    for decoder_id, group in primary.groupby("decoder_id", sort=True):
        n = len(group)
        top_decile_count = max(1, int(math.ceil(n * 0.10)))
        signal_top_decile = _top_set(
            group, "signal_rank_ic_mean", top_decile_count
        )
        net_top_decile = _top_set(
            group, "cumulative_net_return", top_decile_count
        )
        signal_top5 = _top_set(group, "signal_rank_ic_mean", min(5, n))
        net_top5 = _top_set(group, "cumulative_net_return", min(5, n))
        positive = pd.to_numeric(
            group["cumulative_net_return"], errors="coerce"
        ).clip(lower=0.0)
        positive_total = float(positive.sum())
        ordered_positive = positive.sort_values(ascending=False)
        rows.append(
            {
                "decoder_id": str(decoder_id),
                "primary_candidate_count": int(n),
                "signal_to_net_mtm_spearman": v1._rank_correlation(
                    pd.to_numeric(
                        group["signal_rank_ic_mean"], errors="coerce"
                    ).to_numpy(dtype=float),
                    pd.to_numeric(
                        group["cumulative_net_return"], errors="coerce"
                    ).to_numpy(dtype=float),
                ),
                "top_decile_retention": (
                    len(signal_top_decile & net_top_decile)
                    / max(1, top_decile_count)
                ),
                "top5_retention": (
                    len(signal_top5 & net_top5) / max(1, len(signal_top5))
                ),
                "positive_primary_count": int(positive.gt(0).sum()),
                "positive_return_top1_contribution": (
                    float(ordered_positive.head(1).sum() / positive_total)
                    if positive_total > 0
                    else None
                ),
                "positive_return_top5_contribution": (
                    float(ordered_positive.head(5).sum() / positive_total)
                    if positive_total > 0
                    else None
                ),
                "median_net_return_per_turnover": _finite(
                    pd.to_numeric(
                        group["net_return_per_turnover"], errors="coerce"
                    ).median()
                ),
                "evidence_scope": "DEVELOPMENT_TRAIN_ONLY",
            }
        )
    result = pd.DataFrame(rows)
    baseline = result[
        result["decoder_id"].eq("CURRENT_TOP20PCT_EQUAL")
    ].iloc[0]
    result["top5_retention_at_least_baseline"] = (
        result["top5_retention"] >= float(baseline["top5_retention"])
    )
    baseline_spearman = _finite(baseline["signal_to_net_mtm_spearman"])
    result["signal_to_net_spearman_above_baseline"] = (
        pd.to_numeric(
            result["signal_to_net_mtm_spearman"], errors="coerce"
        ).gt(baseline_spearman)
        if baseline_spearman is not None
        else False
    )
    return result


def _transition_matrix(candidate_metrics: pd.DataFrame) -> pd.DataFrame:
    primary = candidate_metrics[
        candidate_metrics["pair_member_role"].eq("PRIMARY")
    ][
        [
            "candidate_id",
            "pair_id",
            "route_id",
            "signal_rank_ic_mean",
            "decoder_id",
            "cumulative_net_return",
            "continuous_book_net_reward",
            "mean_one_way_turnover",
            "cumulative_realized_trade_pnl_cny",
            "ending_unrealized_pnl_cny",
        ]
    ].copy()
    wide = primary.pivot(
        index=["candidate_id", "pair_id", "route_id", "signal_rank_ic_mean"],
        columns="decoder_id",
        values=[
            "cumulative_net_return",
            "continuous_book_net_reward",
            "mean_one_way_turnover",
            "cumulative_realized_trade_pnl_cny",
            "ending_unrealized_pnl_cny",
        ],
    )
    wide.columns = [f"{metric}__{decoder}" for metric, decoder in wide.columns]
    wide = wide.reset_index()
    baseline = "cumulative_net_return__CURRENT_TOP20PCT_EQUAL"
    for decoder in ("TOPK_10_EQUAL", "TOPK_10_RANK"):
        wide[f"cumulative_net_return_delta__{decoder}"] = (
            wide[f"cumulative_net_return__{decoder}"] - wide[baseline]
        )
    return wide


def _report(
    *,
    selection_sha256: str,
    rank: pd.DataFrame,
    pairs: pd.DataFrame,
) -> str:
    lines = [
        "# CN_PORTFOLIO_DECODER_V2",
        "",
        f"- Frozen selection: `{selection_sha256}` (32 pairs / 64 members).",
        "- Scope: development train only; no search, reward change, validation, holdout or 2026 reads.",
        "- All three treatments use the same close-T signal, next-session-open execution, T+1 inventory, fees and continuous-book final-close MTM.",
        "- One-session means daily target refresh from the latest available prior-close signal. It does not fabricate a same-session sale; unchanged holdings may remain as older lots and their ages are reported.",
        "",
        "| decoder | signal-net Spearman | top-decile retention | top-5 retention | positive primary | both economic gates | top1 positive contribution | top5 positive contribution |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rank.sort_values("decoder_id").to_dict(orient="records"):
        both = int(
            pairs[
                pairs["decoder_id"].eq(str(row["decoder_id"]))
            ]["both_economic_gates_positive"].sum()
        )
        def rendered(value: Any) -> str:
            number = _finite(value)
            return "NA" if number is None else f"{number:.6f}"
        lines.append(
            "| {decoder} | {spearman} | {decile:.3f} | {top5:.3f} | {positive} | {both} | {top1} | {top5c} |".format(
                decoder=row["decoder_id"],
                spearman=rendered(row["signal_to_net_mtm_spearman"]),
                decile=float(row["top_decile_retention"]),
                top5=float(row["top5_retention"]),
                positive=int(row["positive_primary_count"]),
                both=both,
                top1=rendered(row["positive_return_top1_contribution"]),
                top5c=rendered(row["positive_return_top5_contribution"]),
            )
        )
    lines.extend(
        [
            "",
            "No decoder is automatically promoted. A decoder can advance only if standalone and matched net MTM are positive, information retention is not worse than baseline, and gains are not a one-candidate concentration artifact.",
        ]
    )
    return "\n".join(lines) + "\n"


def run_decoder_v2(
    *,
    replay_oos_root: Path,
    ledger_replay_root: Path,
    output_root: Path,
    builder_commit_sha: str,
    expected_selection_payload_sha256: str,
    worker_count: int = 8,
    execution_backend: str = "THREAD_POOL",
    executor_worker_count: int | None = None,
    qualification_candidate_count: int | None = None,
    qualification_baseline_candidates_per_hour: float | None = None,
    qualification_minimum_speedup_ratio: float = 1.5,
) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(f"decoder V2 must run on {AUTHORIZED_HOST}")
    if psutil.virtual_memory().available < MINIMUM_FREE_MEMORY_BYTES:
        raise RuntimeError("decoder V2 minimum-free-memory gate failed")
    if executor_worker_count is None:
        executor_worker_count = int(worker_count)
    execution_backend = _validate_executor_contract(
        execution_backend=execution_backend,
        entitlement_worker_count=int(worker_count),
        executor_worker_count=int(executor_worker_count),
    )
    if os.environ.get("CN_NODE_RESOURCE_LEASE_REQUIRED") == "1":
        entitlement = int(os.environ.get("CN_NODE_CPU_ENTITLEMENT") or 0)
        if entitlement != int(worker_count):
            raise RuntimeError("decoder V2 lease entitlement drift")
        if int(worker_count) > 8:
            nested = {
                name: int(os.environ.get(name) or 0)
                for name in (
                    "NUMBA_NUM_THREADS",
                    "POLARS_MAX_THREADS",
                    "ARROW_NUM_THREADS",
                    "OMP_NUM_THREADS",
                    "MKL_NUM_THREADS",
                    "OPENBLAS_NUM_THREADS",
                    "NUMEXPR_NUM_THREADS",
                )
            }
            if any(value != 1 for value in nested.values()):
                raise RuntimeError(
                    f"decoder V2 nested parallelism drift: {nested}"
                )
    qualification_mode = qualification_candidate_count is not None
    if qualification_mode and not (
        1 <= int(qualification_candidate_count) <= EXPECTED_MEMBER_COUNT
    ):
        raise ValueError(
            "qualification_candidate_count must be in [1, 64]"
        )
    if qualification_mode:
        if (
            qualification_baseline_candidates_per_hour is None
            or float(qualification_baseline_candidates_per_hour) <= 0
        ):
            raise ValueError(
                "qualification baseline candidates/hour must be positive"
            )
        if float(qualification_minimum_speedup_ratio) <= 1.0:
            raise ValueError(
                "qualification minimum speedup ratio must exceed 1.0"
            )
    if len(builder_commit_sha) != 40:
        raise ValueError("builder_commit_sha must be a full Git SHA")

    replay_oos_root = Path(replay_oos_root).resolve()
    ledger_replay_root = Path(ledger_replay_root).resolve()
    output_root = Path(output_root).resolve()
    freeze_path = replay_oos_root / "prepared" / "finalist_replay_then_oos_freeze.json"
    train_field_root = replay_oos_root / "sidecars" / "train_session_fields"
    freeze = mtm._load_freeze_for_mark_to_market(freeze_path)
    selection_sha256 = str(freeze["selection_payload_sha256"])
    if selection_sha256 != expected_selection_payload_sha256:
        raise RuntimeError("decoder V2 selection payload drift")
    if (
        int(freeze["pair_count"]) != EXPECTED_PAIR_COUNT
        or int(freeze["candidate_member_count"]) != EXPECTED_MEMBER_COUNT
    ):
        raise RuntimeError("decoder V2 frozen cohort cardinality drift")

    ledger_closure_path = ledger_replay_root / "MARK_TO_MARKET_REPLAY_COMPLETE.json"
    ledger_closure = v1._verify_manifest_artifacts(
        ledger_closure_path,
        allowed_statuses=(
            "FINAL_CLOSE_MARK_TO_MARKET_REPLAY_CLOSED_IMMUTABLE_DIAGNOSTIC_ONLY",
            "FINAL_CLOSE_MARK_TO_MARKET_REPLAY_CLOSED_IMMUTABLE_TRAIN_ECONOMIC_EVIDENCE",
        ),
    )
    if str(ledger_closure["selection_payload_sha256"]) != selection_sha256:
        raise RuntimeError("decoder V2 ledger selection drift")
    _require_persisted_accounting_ledgers(ledger_closure)

    frozen_root = freeze_path.parents[1]
    candidate_path = base._resolved_artifact(
        frozen_root, freeze["candidate_artifact"]
    )
    contract_path = base._resolved_artifact(
        frozen_root, freeze["execution_contract_artifact"]
    )
    candidates = pd.read_parquet(candidate_path).where(pd.notna, None)
    if candidates["candidate_id"].astype(str).tolist() != list(
        freeze["candidate_ids"]
    ):
        raise RuntimeError("decoder V2 candidate identity/order drift")
    baseline_path = ledger_replay_root / "candidate_mark_to_market_results.parquet"
    baseline = pd.read_parquet(baseline_path)
    if baseline["candidate_id"].astype(str).tolist() != candidates[
        "candidate_id"
    ].astype(str).tolist():
        raise RuntimeError("decoder V2 ledger candidate order drift")
    evaluation_candidates = (
        candidates.head(int(qualification_candidate_count)).copy()
        if qualification_mode
        else candidates
    )
    evaluation_context = _load_evaluation_context(
        contract_path=contract_path,
        train_field_root=train_field_root,
        qualification_mode=qualification_mode,
    )
    evaluation_policies = evaluation_context["evaluation_policies"]
    contract = evaluation_context["contract"]
    field_manifest_path = evaluation_context["field_manifest_path"]
    session_manifest_path = evaluation_context["session_manifest_path"]
    session_path = evaluation_context["session_path"]

    decoder_contract = [
        {
            **asdict(policy),
            "payload_sha256": policy.payload_sha256,
        }
        for policy in evaluation_policies
    ]
    input_binding = {
        "schema_version": "cn_portfolio_decoder_v2_input_binding_v1",
        "selection_payload_sha256": selection_sha256,
        "freeze_sha256": v1._sha256(freeze_path),
        "candidate_sha256": v1._sha256(candidate_path),
        "contract_sha256": v1._sha256(contract_path),
        "train_field_manifest_sha256": v1._sha256(field_manifest_path),
        "session_authority_manifest_sha256": v1._sha256(
            session_manifest_path
        ),
        "session_authority_sha256": v1._sha256(session_path),
        "ledger_closure_sha256": v1._sha256(ledger_closure_path),
        "ledger_candidate_results_sha256": v1._sha256(baseline_path),
        "builder_commit_sha": builder_commit_sha,
        "builder_source_sha256": v1._sha256(Path(__file__).resolve()),
        "decoder_contract": decoder_contract,
        "decoder_contract_sha256": v1._stable_hash(decoder_contract),
        "worker_count": int(worker_count),
        "execution_backend": execution_backend,
        "executor_worker_count": int(executor_worker_count),
        "native_threads_per_executor_worker": 1,
        "qualification_baseline_candidates_per_hour": (
            float(qualification_baseline_candidates_per_hour)
            if qualification_mode
            else None
        ),
        "qualification_minimum_speedup_ratio": (
            float(qualification_minimum_speedup_ratio)
            if qualification_mode
            else None
        ),
        "run_mode": (
            "ACCELERATION_QUALIFICATION"
            if qualification_mode
            else "DECODER_V2_FULL"
        ),
        "evaluation_candidate_ids": evaluation_candidates[
            "candidate_id"
        ].astype(str).tolist(),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "search_run": False,
        "reward_changed": False,
        "evaluator_changed": False,
        "promotion_authorized": False,
    }
    input_data_sha256 = v1._stable_hash(input_binding)
    output_root.mkdir(parents=True, exist_ok=True)
    binding_path = v1._write_json(output_root / "input_binding.json", input_binding)
    candidate_root = output_root / "candidates"
    candidate_root.mkdir(exist_ok=True)

    metric_rows: list[dict[str, Any]] = []
    candidate_records = evaluation_candidates.to_dict(orient="records")
    if execution_backend == "PROCESS_POOL":
        del evaluation_context
        gc.collect()
    started = time.perf_counter()
    (
        minimum_free_memory_bytes,
        maximum_process_rss_bytes,
        maximum_process_tree_rss_bytes,
    ) = _resource_snapshot()
    host_cpu_samples: list[float] = []
    psutil.cpu_percent(interval=None)
    if execution_backend == "PROCESS_POOL":
        executor = ProcessPoolExecutor(
            max_workers=int(executor_worker_count),
            initializer=_initialize_process_worker,
            initargs=(
                str(contract_path),
                str(train_field_root),
                bool(qualification_mode),
                str(candidate_root),
                input_data_sha256,
            ),
        )
        submit = lambda candidate: executor.submit(  # noqa: E731
            _evaluate_candidate_in_process, candidate
        )
    else:
        executor = ThreadPoolExecutor(max_workers=int(executor_worker_count))
        submit = lambda candidate: executor.submit(  # noqa: E731
            _evaluate_candidate_with_context,
            candidate,
            context=evaluation_context,
            candidate_root=candidate_root,
            input_data_sha256=input_data_sha256,
        )
    with executor:
        futures = {
            submit(candidate): str(candidate["candidate_id"])
            for candidate in candidate_records
        }
        pending = set(futures)
        while pending:
            completed, pending = wait(
                pending,
                timeout=2.0,
                return_when=FIRST_COMPLETED,
            )
            for future in completed:
                metric_rows.extend(future.result())
            available, parent_rss, tree_rss = _resource_snapshot()
            minimum_free_memory_bytes = min(
                minimum_free_memory_bytes, available
            )
            maximum_process_rss_bytes = max(
                maximum_process_rss_bytes,
                parent_rss,
            )
            maximum_process_tree_rss_bytes = max(
                maximum_process_tree_rss_bytes,
                tree_rss,
            )
            host_cpu_samples.append(float(psutil.cpu_percent(interval=None)))
            if available < MINIMUM_FREE_MEMORY_BYTES:
                for future in pending:
                    future.cancel()
                raise RuntimeError("decoder V2 runtime memory gate failed")
    elapsed_seconds = float(time.perf_counter() - started)
    logical_cpu_count = int(psutil.cpu_count(logical=True) or 1)
    host_cpu_mean_percent = (
        float(np.mean(host_cpu_samples)) if host_cpu_samples else 0.0
    )
    effective_cores = host_cpu_mean_percent * logical_cpu_count / 100.0
    saturation_threshold_percent = (
        70.0 * int(executor_worker_count) / logical_cpu_count
    )
    saturated_compute_sample_fraction = (
        float(
            np.mean(
                np.asarray(host_cpu_samples, dtype=float)
                >= saturation_threshold_percent
            )
        )
        if host_cpu_samples
        else 0.0
    )
    candidate_metrics = pd.DataFrame(metric_rows).sort_values(
        ["pair_id", "pair_member_role", "decoder_id"],
        kind="mergesort",
    ).reset_index(drop=True)
    if len(candidate_metrics) != len(evaluation_candidates) * len(
        evaluation_policies
    ):
        raise RuntimeError("decoder V2 metric cardinality drift")
    if not candidate_metrics["accounting_invariants_status"].eq("PASS").all():
        raise RuntimeError("decoder V2 accounting invariant failure")
    parity = _baseline_parity(
        candidate_metrics,
        baseline,
        expected_member_count=len(evaluation_candidates),
    )
    if qualification_mode:
        if not parity["parity_status"].eq("PASS").all():
            raise RuntimeError("decoder V2 qualification parity failed")
        candidate_metrics_path = (
            output_root / "qualification_candidate_metrics.parquet"
        )
        parity_path = output_root / "qualification_baseline_parity.parquet"
        candidate_metrics.to_parquet(candidate_metrics_path, index=False)
        parity.to_parquet(parity_path, index=False)
        candidate_files = sorted(candidate_root.glob("*.json"))
        artifacts = [
            v1._artifact(path, root=output_root)
            for path in (
                binding_path,
                candidate_metrics_path,
                parity_path,
                *candidate_files,
            )
        ]
        candidates_per_hour = (
            float(len(evaluation_candidates)) * 3600.0 / elapsed_seconds
        )
        speedup_ratio = candidates_per_hour / float(
            qualification_baseline_candidates_per_hour
        )
        closure = {
            "schema_version": (
                "cn_portfolio_decoder_v2_acceleration_qualification_v1"
            ),
            "status": "DECODER_V2_ACCELERATION_QUALIFICATION_COMPLETE",
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "selection_payload_sha256": selection_sha256,
            "input_data_sha256": input_data_sha256,
            "candidate_member_count": int(len(evaluation_candidates)),
            "decoder_ids": [
                policy.decoder_id for policy in evaluation_policies
            ],
            "worker_count": int(worker_count),
            "execution_backend": execution_backend,
            "executor_worker_count": int(executor_worker_count),
            "native_threads_per_executor_worker": 1,
            "elapsed_seconds": elapsed_seconds,
            "candidates_per_hour": candidates_per_hour,
            "qualification_baseline_candidates_per_hour": float(
                qualification_baseline_candidates_per_hour
            ),
            "qualification_minimum_speedup_ratio": float(
                qualification_minimum_speedup_ratio
            ),
            "qualification_observed_speedup_ratio": speedup_ratio,
            "throughput_qualification_status": (
                "PASS"
                if speedup_ratio
                >= float(qualification_minimum_speedup_ratio)
                else "FAIL"
            ),
            "minimum_free_memory_bytes": minimum_free_memory_bytes,
            "maximum_process_rss_bytes": maximum_process_rss_bytes,
            "maximum_process_tree_rss_bytes": (
                maximum_process_tree_rss_bytes
            ),
            "logical_cpu_count": logical_cpu_count,
            "host_cpu_mean_percent": host_cpu_mean_percent,
            "effective_cores": effective_cores,
            "saturated_compute_sample_fraction": (
                saturated_compute_sample_fraction
            ),
            "baseline_parity_count": int(len(parity)),
            "baseline_parity_status": "PASS",
            "accounting_invariants_status": "PASS",
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "search_run": False,
            "promotion_authorized": False,
            "artifacts": artifacts,
        }
        closure["manifest_body_sha256"] = v1._stable_hash(closure)
        return v1._write_json(
            output_root
            / "DECODER_V2_ACCELERATION_QUALIFICATION_COMPLETE.json",
            closure,
        )
    pairs = _pair_metrics(candidate_metrics)
    rank = _rank_preservation(candidate_metrics)
    transition = _transition_matrix(candidate_metrics)

    candidate_metrics_path = output_root / "decoder_candidate_metrics.parquet"
    pair_metrics_path = output_root / "decoder_pair_metrics.parquet"
    rank_path = output_root / "decoder_rank_preservation.parquet"
    transition_path = output_root / "candidate_transition_matrix.parquet"
    parity_path = output_root / "baseline_ledger_parity.parquet"
    candidate_metrics.to_parquet(candidate_metrics_path, index=False)
    pairs.to_parquet(pair_metrics_path, index=False)
    rank.to_parquet(rank_path, index=False)
    transition.to_parquet(transition_path, index=False)
    parity.to_parquet(parity_path, index=False)
    report_path = output_root / "CN_PORTFOLIO_DECODER_V2.md"
    report_path.write_text(
        _report(selection_sha256=selection_sha256, rank=rank, pairs=pairs),
        encoding="utf-8",
    )

    candidate_files = sorted(candidate_root.glob("*.json"))
    artifacts = [
        v1._artifact(path, root=output_root)
        for path in (
            binding_path,
            candidate_metrics_path,
            pair_metrics_path,
            rank_path,
            transition_path,
            parity_path,
            report_path,
            *candidate_files,
        )
    ]
    closure = {
        "schema_version": SCHEMA_VERSION,
        "status": "CN_PORTFOLIO_DECODER_V2_CLOSED_IMMUTABLE_DIAGNOSTIC_ONLY",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection_payload_sha256": selection_sha256,
        "input_data_sha256": input_data_sha256,
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "decoder_count": len(DECODER_POLICIES),
        "candidate_metric_count": int(len(candidate_metrics)),
        "pair_metric_count": int(len(pairs)),
        "baseline_parity_count": int(len(parity)),
        "baseline_parity_status": "PASS",
        "accounting_invariants_status": "PASS",
        "worker_count": int(worker_count),
        "execution_backend": execution_backend,
        "executor_worker_count": int(executor_worker_count),
        "native_threads_per_executor_worker": 1,
        "elapsed_seconds": elapsed_seconds,
        "candidates_per_hour": (
            float(len(evaluation_candidates)) * 3600.0 / elapsed_seconds
        ),
        "minimum_free_memory_bytes": minimum_free_memory_bytes,
        "maximum_process_rss_bytes": maximum_process_rss_bytes,
        "maximum_process_tree_rss_bytes": maximum_process_tree_rss_bytes,
        "logical_cpu_count": logical_cpu_count,
        "host_cpu_mean_percent": host_cpu_mean_percent,
        "effective_cores": effective_cores,
        "saturated_compute_sample_fraction": (
            saturated_compute_sample_fraction
        ),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "search_run": False,
        "reward_changed": False,
        "evaluator_changed": False,
        "promotion_authorized": False,
        "artifacts": artifacts,
    }
    closure["manifest_body_sha256"] = v1._stable_hash(closure)
    return v1._write_json(
        output_root / "DECODER_V2_COMPLETE.json", closure
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-oos-root", required=True, type=Path)
    parser.add_argument("--ledger-replay-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--builder-commit-sha", required=True)
    parser.add_argument("--expected-selection-payload-sha256", required=True)
    parser.add_argument("--worker-count", type=int, default=8)
    parser.add_argument(
        "--execution-backend",
        choices=EXECUTION_BACKENDS,
        default="THREAD_POOL",
    )
    parser.add_argument("--executor-worker-count", type=int)
    parser.add_argument("--qualification-candidate-count", type=int)
    parser.add_argument(
        "--qualification-baseline-candidates-per-hour", type=float
    )
    parser.add_argument(
        "--qualification-minimum-speedup-ratio", type=float, default=1.5
    )
    args = parser.parse_args()
    closure = run_decoder_v2(
        replay_oos_root=args.replay_oos_root,
        ledger_replay_root=args.ledger_replay_root,
        output_root=args.output_root,
        builder_commit_sha=args.builder_commit_sha,
        expected_selection_payload_sha256=(
            args.expected_selection_payload_sha256
        ),
        worker_count=args.worker_count,
        execution_backend=args.execution_backend,
        executor_worker_count=args.executor_worker_count,
        qualification_candidate_count=args.qualification_candidate_count,
        qualification_baseline_candidates_per_hour=(
            args.qualification_baseline_candidates_per_hour
        ),
        qualification_minimum_speedup_ratio=(
            args.qualification_minimum_speedup_ratio
        ),
    )
    print(closure)


if __name__ == "__main__":
    main()
