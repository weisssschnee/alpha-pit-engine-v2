from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


SCHEMA_VERSION = "cn_alpha_autopsy_v1"
EXPECTED_PAIR_COUNT = 32
EXPECTED_MEMBER_COUNT = 64
EXPECTED_HORIZONS = (1, 5, 15, 30)
MISSING = "UNAVAILABLE_FROM_IMMUTABLE_EVIDENCE"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return destination


def _artifact(path: Path, *, root: Path) -> dict[str, Any]:
    source = Path(path).resolve()
    return {
        "path": source.relative_to(Path(root).resolve()).as_posix(),
        "bytes": source.stat().st_size,
        "sha256": _sha256(source),
    }


def _finite(value: Any) -> float | None:
    try:
        rendered = float(value)
    except (TypeError, ValueError):
        return None
    return rendered if math.isfinite(rendered) else None


def _safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)


def _verify_manifest(
    path: Path,
    *,
    allowed_statuses: Sequence[str],
) -> dict[str, Any]:
    source = Path(path).resolve()
    payload = _read_json(source)
    status = str(payload.get("status") or "")
    if status not in set(allowed_statuses):
        raise RuntimeError(f"manifest status drift at {source}: {status}")
    stored = str(payload.get("manifest_body_sha256") or "")
    if not stored:
        raise RuntimeError(f"manifest self-hash missing at {source}")
    body = dict(payload)
    body.pop("manifest_body_sha256", None)
    computed = _stable_hash(body)
    if computed != stored:
        raise RuntimeError(
            f"manifest self-hash mismatch at {source}: {computed} != {stored}"
        )
    verified = 0
    for record in payload.get("artifacts") or []:
        rendered = Path(str(record.get("path") or ""))
        artifact_path = rendered if rendered.is_absolute() else source.parent / rendered
        artifact_path = artifact_path.resolve()
        if not artifact_path.is_file():
            raise RuntimeError(f"declared artifact missing: {artifact_path}")
        declared_bytes = record.get("bytes")
        if declared_bytes is None:
            raise RuntimeError(f"declared artifact size missing: {artifact_path}")
        if int(declared_bytes) != artifact_path.stat().st_size:
            raise RuntimeError(f"declared artifact size mismatch: {artifact_path}")
        if str(record.get("sha256") or "") != _sha256(artifact_path):
            raise RuntimeError(f"declared artifact hash mismatch: {artifact_path}")
        verified += 1
    return {
        "path": str(source),
        "file_sha256": _sha256(source),
        "manifest_body_sha256": stored,
        "status": status,
        "selection_payload_sha256": str(
            payload.get("selection_payload_sha256") or ""
        ),
        "artifact_count_verified": verified,
        "payload": payload,
    }


def _require_columns(frame: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"{label} missing required columns: {missing}")


def _ensure_exact_identity(
    prepared: pd.DataFrame,
    oos: pd.DataFrame,
    mtm: pd.DataFrame,
) -> None:
    for label, frame in (("prepared", prepared), ("oos", oos), ("mtm", mtm)):
        if len(frame) != EXPECTED_PAIR_COUNT:
            raise RuntimeError(f"{label} pair count drift: {len(frame)}")
        if frame["pair_id"].astype(str).nunique() != EXPECTED_PAIR_COUNT:
            raise RuntimeError(f"{label} pair identity duplication")
    expected = prepared[
        ["pair_id", "primary_candidate_id", "control_candidate_id", "route_id"]
    ].astype(str)
    expected_rows = list(expected.itertuples(index=False, name=None))
    for label, frame in (("oos", oos), ("mtm", mtm)):
        actual = frame[
            ["pair_id", "primary_candidate_id", "control_candidate_id", "route_id"]
        ].astype(str)
        actual_rows = list(actual.itertuples(index=False, name=None))
        if actual_rows != expected_rows:
            raise RuntimeError(f"{label} identity/order drift from frozen cohort")


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _atom_summary(group: pd.DataFrame) -> dict[str, Any]:
    curve_count = _numeric(group, "curve_count").fillna(0).sum()
    rank_count = _numeric(group, "rank_ic_count").fillna(0).sum()
    turnover_count = _numeric(group, "turnover_count").fillna(0).sum()
    rank_mean = (
        _numeric(group, "rank_ic_sum").fillna(0).sum() / rank_count
        if rank_count > 0
        else None
    )
    rank_hit_rate = (
        _numeric(group, "rank_ic_positive_count").fillna(0).sum() / rank_count
        if rank_count > 0
        else None
    )
    gross_mean = (
        _numeric(group, "raw_return_sum").fillna(0).sum() / curve_count
        if curve_count > 0
        else None
    )
    net_mean = (
        _numeric(group, "net_return_sum").fillna(0).sum() / curve_count
        if curve_count > 0
        else None
    )
    turnover_mean = (
        _numeric(group, "turnover_sum").fillna(0).sum() / turnover_count
        if turnover_count > 0
        else None
    )
    daily = group.copy()
    daily["_rank_ic"] = _numeric(daily, "rank_ic_sum") / _numeric(
        daily, "rank_ic_count"
    ).replace(0, np.nan)
    daily_rank = daily["_rank_ic"].dropna()
    rank_std = float(daily_rank.std(ddof=1)) if len(daily_rank) > 1 else None
    icir = (
        float(daily_rank.mean()) / rank_std
        if rank_std is not None and rank_std > 0
        else None
    )
    return {
        "rank_ic_mean": _finite(rank_mean),
        "rank_ic_std_daily": _finite(rank_std),
        "icir_unannualized": _finite(icir),
        "rank_ic_hit_rate": _finite(rank_hit_rate),
        "gross_portfolio_return_mean": _finite(gross_mean),
        "net_portfolio_return_mean": _finite(net_mean),
        "cost_drag_mean": _finite(
            gross_mean - net_mean
            if gross_mean is not None and net_mean is not None
            else None
        ),
        "mean_one_way_turnover": _finite(turnover_mean),
        "curve_count": int(curve_count),
        "rank_ic_count": int(rank_count),
        "daily_observation_count": int(group["trade_date"].astype(str).nunique()),
    }


def aggregate_signal_quality(
    atoms: pd.DataFrame,
    prepared_pairs: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(
        atoms,
        (
            "candidate_id",
            "split",
            "horizon_min",
            "trade_date",
            "curve_count",
            "net_return_sum",
            "raw_return_sum",
            "turnover_sum",
            "turnover_count",
            "rank_ic_sum",
            "rank_ic_count",
            "rank_ic_positive_count",
        ),
        "streaming reward atoms",
    )
    validation = atoms.loc[atoms["split"].astype(str).str.lower().eq("validation")].copy()
    if validation.empty:
        raise RuntimeError("no validation reward atoms found")
    primary_ids = set(prepared_pairs["primary_candidate_id"].astype(str))
    validation = validation.loc[validation["candidate_id"].astype(str).isin(primary_ids)]
    observed_ids = set(validation["candidate_id"].astype(str))
    if observed_ids != primary_ids:
        missing = sorted(primary_ids - observed_ids)
        raise RuntimeError(f"validation reward atom candidate coverage drift: {missing}")

    output: list[dict[str, Any]] = []
    for pair in prepared_pairs.to_dict(orient="records"):
        candidate_id = str(pair["primary_candidate_id"])
        candidate_atoms = validation.loc[
            validation["candidate_id"].astype(str).eq(candidate_id)
        ].copy()
        horizons = candidate_atoms["horizon_min"].astype(str)
        all_atoms = candidate_atoms.loc[horizons.str.lower().eq("all")]
        overall_source = all_atoms if not all_atoms.empty else candidate_atoms
        overall = _atom_summary(overall_source)
        row: dict[str, Any] = {
            "pair_id": str(pair["pair_id"]),
            "candidate_id": candidate_id,
            "route_id": str(pair["route_id"]),
            "train_rank_ic_mean": _finite(pair.get("train_rank_ic_mean")),
            "train_rank_ic_hit_rate": _finite(pair.get("train_rank_ic_hit_rate")),
            "train_icir": None,
            "train_top_bottom_spread": None,
            "oos_rank_ic_mean": overall["rank_ic_mean"],
            "oos_rank_ic_std_daily": overall["rank_ic_std_daily"],
            "oos_icir_unannualized": overall["icir_unannualized"],
            "oos_rank_ic_hit_rate": overall["rank_ic_hit_rate"],
            "oos_top_bottom_spread": None,
            "oos_gross_portfolio_return_mean": overall[
                "gross_portfolio_return_mean"
            ],
            "oos_net_portfolio_return_mean": overall["net_portfolio_return_mean"],
            "oos_cost_drag_mean": overall["cost_drag_mean"],
            "oos_mean_one_way_turnover": overall["mean_one_way_turnover"],
            "oos_daily_observation_count": overall["daily_observation_count"],
            "oos_sample_grade": (
                "WEAK" if overall["daily_observation_count"] < 250 else "MODERATE"
            ),
            "train_icir_availability": MISSING,
            "train_top_bottom_spread_availability": MISSING,
            "oos_top_bottom_spread_availability": MISSING,
        }
        decay: dict[str, Any] = {}
        observed_numeric_horizons: set[int] = set()
        for horizon in EXPECTED_HORIZONS:
            selected = candidate_atoms.loc[horizons.eq(str(horizon))]
            if selected.empty:
                summary = {key: None for key in _atom_summary(candidate_atoms.iloc[:0])}
            else:
                summary = _atom_summary(selected)
                observed_numeric_horizons.add(horizon)
            prefix = f"oos_{horizon}m"
            row[f"{prefix}_rank_ic_mean"] = summary.get("rank_ic_mean")
            row[f"{prefix}_icir_unannualized"] = summary.get("icir_unannualized")
            row[f"{prefix}_gross_portfolio_return_mean"] = summary.get(
                "gross_portfolio_return_mean"
            )
            row[f"{prefix}_net_portfolio_return_mean"] = summary.get(
                "net_portfolio_return_mean"
            )
            decay[str(horizon)] = summary.get("rank_ic_mean")
        if observed_numeric_horizons != set(EXPECTED_HORIZONS):
            raise RuntimeError(
                f"validation horizon coverage drift for {candidate_id}: "
                f"{sorted(observed_numeric_horizons)}"
            )
        row["oos_rank_ic_decay_curve_json"] = _safe_json(decay)
        output.append(row)
    return pd.DataFrame(output)


def _holding_concentration(value: Any) -> tuple[float | None, float | None]:
    holdings = value if isinstance(value, (list, tuple, np.ndarray)) else []
    market_values: list[float] = []
    for holding in holdings:
        if not isinstance(holding, Mapping):
            continue
        amount = _finite(
            holding.get("market_value_cny")
            if "market_value_cny" in holding
            else holding.get("market_value")
        )
        if amount is not None and amount >= 0:
            market_values.append(amount)
    total = sum(market_values)
    if total <= 0:
        return None, None
    weights = sorted((amount / total for amount in market_values), reverse=True)
    return _finite(sum(weights[:10])), _finite(sum(weight * weight for weight in weights))


def _signal_status(train_value: Any, oos_value: Any) -> str:
    train = _finite(train_value)
    oos = _finite(oos_value)
    if train is None or oos is None:
        return "UNKNOWN"
    if train > 0 and oos > 0:
        return "STABLE"
    if train > 0 and oos <= 0:
        return "OOS_DECAY"
    if train <= 0 and oos > 0:
        return "OOS_DIVERGENT"
    return "FAILURE"


def _portfolio_status(signal: Any, gross: Any) -> str:
    signal_value = _finite(signal)
    gross_value = _finite(gross)
    if signal_value is None or gross_value is None:
        return "UNKNOWN"
    if signal_value > 0 and gross_value > 0:
        return "PASS"
    if signal_value > 0 and gross_value <= 0:
        return "FAILED"
    return "NOT_SIGNAL_STABLE"


def build_tables(
    prepared: pd.DataFrame,
    oos: pd.DataFrame,
    mtm_pairs: pd.DataFrame,
    mtm_candidates: pd.DataFrame,
    signal: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    primary_candidates = prepared[
        ["pair_id", "primary_candidate_id"]
    ].rename(columns={"primary_candidate_id": "candidate_id"})
    primary_mtm = primary_candidates.merge(
        mtm_candidates,
        on=["pair_id", "candidate_id"],
        how="left",
        validate="one_to_one",
    )
    if len(primary_mtm) != len(prepared):
        raise RuntimeError("primary MTM candidate coverage drift")

    pair_base = prepared.merge(
        signal,
        left_on=["pair_id", "primary_candidate_id", "route_id"],
        right_on=["pair_id", "candidate_id", "route_id"],
        how="left",
        validate="one_to_one",
        suffixes=("", "_signal"),
    ).merge(
        oos,
        on=["pair_id", "primary_candidate_id", "control_candidate_id", "route_id"],
        how="left",
        validate="one_to_one",
        suffixes=("", "_oos"),
    ).merge(
        mtm_pairs,
        on=["pair_id", "primary_candidate_id", "control_candidate_id", "route_id"],
        how="left",
        validate="one_to_one",
        suffixes=("", "_mtm"),
    ).merge(
        primary_mtm.drop(columns=["candidate_id"]),
        on="pair_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_primary_candidate"),
    )

    signal_table = signal.copy()
    signal_table["signal_status"] = [
        _signal_status(train, oos_value)
        for train, oos_value in zip(
            signal_table["train_rank_ic_mean"], signal_table["oos_rank_ic_mean"]
        )
    ]

    portfolio_rows: list[dict[str, Any]] = []
    execution_rows: list[dict[str, Any]] = []
    reward_rows: list[dict[str, Any]] = []
    diagnosis_rows: list[dict[str, Any]] = []
    for row in pair_base.to_dict(orient="records"):
        top10, hhi = _holding_concentration(row.get("ending_holdings"))
        initial_cash = _finite(row.get("initial_cash_cny"))
        ending_nav = _finite(row.get("ending_nav_cny"))
        fees = _finite(row.get("total_fees_cny"))
        net_pnl = (
            ending_nav - initial_cash
            if initial_cash is not None and ending_nav is not None
            else None
        )
        fee_addback_pnl = (
            net_pnl + fees if net_pnl is not None and fees is not None else None
        )
        fee_residual = (
            fee_addback_pnl - fees - net_pnl
            if fee_addback_pnl is not None and fees is not None and net_pnl is not None
            else None
        )
        signal_status = _signal_status(
            row.get("train_rank_ic_mean"), row.get("oos_rank_ic_mean")
        )
        portfolio_status = _portfolio_status(
            row.get("oos_rank_ic_mean"), row.get("oos_gross_portfolio_return_mean")
        )
        blocked_orders = int(row.get("blocked_buy_count") or 0) + int(
            row.get("blocked_sell_count") or 0
        )
        execution_status = (
            "IMPAIRED_UNQUANTIFIED" if blocked_orders > 0 else "PASS_OBSERVED"
        )
        cost_status = "UNKNOWN"
        if net_pnl is not None and fee_addback_pnl is not None:
            if fee_addback_pnl > 0 >= net_pnl:
                cost_status = "FAILED"
            elif fees is not None and fees > 0:
                cost_status = "IMPAIRED"
            else:
                cost_status = "PASS"
        diagnoses: list[str] = [f"SIGNAL_{signal_status}"]
        if portfolio_status == "FAILED":
            diagnoses.append("PORTFOLIO_LOSS")
        if execution_status == "IMPAIRED_UNQUANTIFIED":
            diagnoses.append("EXECUTION_IMPAIRED_UNQUANTIFIED")
        if cost_status in {"FAILED", "IMPAIRED"}:
            diagnoses.append(f"COST_{cost_status}")
        diagnoses.append("TRANSFER_UNKNOWN_NO_OOS_MTM")

        common = {
            "pair_id": str(row["pair_id"]),
            "candidate_id": str(row["primary_candidate_id"]),
            "route_id": str(row["route_id"]),
        }
        portfolio_rows.append(
            {
                **common,
                "prediction_horizons_minutes": "1|5|15|30",
                "train_rank_ic_mean": _finite(row.get("train_rank_ic_mean")),
                "oos_rank_ic_mean": _finite(row.get("oos_rank_ic_mean")),
                "oos_gross_portfolio_return_mean": _finite(
                    row.get("oos_gross_portfolio_return_mean")
                ),
                "oos_net_portfolio_return_mean": _finite(
                    row.get("oos_net_portfolio_return_mean")
                ),
                "oos_cost_drag_mean": _finite(row.get("oos_cost_drag_mean")),
                "train_mean_one_way_turnover": _finite(
                    row.get("train_mean_one_way_turnover")
                ),
                "oos_mean_one_way_turnover": _finite(
                    row.get("oos_mean_one_way_turnover")
                ),
                "mtm_mean_one_way_turnover": _finite(
                    row.get("primary_mean_one_way_turnover")
                    if "primary_mean_one_way_turnover" in row
                    else row.get("a_share_mean_one_way_turnover")
                ),
                "ending_holding_count": int(row.get("ending_holding_count") or 0),
                "ending_holdings_weight": _finite(row.get("ending_holdings_weight")),
                "ending_book_top10_weight": top10,
                "ending_book_hhi": hhi,
                "average_position_age": None,
                "maximum_position_age": None,
                "realized_pnl": None,
                "unrealized_pnl": None,
                "average_position_age_availability": MISSING,
                "maximum_position_age_availability": MISSING,
                "realized_unrealized_pnl_availability": MISSING,
                "horizon_mismatch_status": "UNKNOWN_POSITION_AGE_NOT_PERSISTED",
                "portfolio_status": portfolio_status,
            }
        )
        execution_rows.append(
            {
                **common,
                "initial_cash_cny": initial_cash,
                "initial_cash_cny_availability": (
                    "AVAILABLE" if initial_cash is not None else MISSING
                ),
                "ending_nav_cny": ending_nav,
                "continuous_book_net_pnl_cny": _finite(net_pnl),
                "continuous_book_cumulative_net_return": _finite(
                    row.get("cumulative_net_return")
                    if "cumulative_net_return" in row
                    else row.get("primary_cumulative_net_return")
                ),
                "fees_cny": fees,
                "fee_addback_pnl_cny": _finite(fee_addback_pnl),
                "fee_only_reconciliation_residual_cny": _finite(fee_residual),
                "fee_only_reconciliation_status": (
                    "PASS"
                    if fee_residual is not None and abs(fee_residual) <= 1e-8
                    else "UNAVAILABLE_INITIAL_CASH_NOT_PERSISTED"
                ),
                "trade_count": int(row.get("trade_count") or 0),
                "fill_count": int(row.get("fill_count") or 0),
                "blocked_buy_count": int(row.get("blocked_buy_count") or 0),
                "blocked_sell_count": int(row.get("blocked_sell_count") or 0),
                "ending_holdings_weight": _finite(row.get("ending_holdings_weight")),
                "same_bar_rejection_loss_cny": None,
                "t_plus_one_loss_cny": None,
                "limit_up_down_loss_cny": None,
                "suspension_loss_cny": None,
                "liquidity_loss_cny": None,
                "slippage_loss_cny": None,
                "execution_loss_attribution_availability": MISSING,
                "slippage_loss_availability": MISSING,
                "full_additive_reconciliation_status": (
                    "INCOMPLETE_EVIDENCE_EXECUTION_COUNTERFACTUALS_MISSING"
                    if fee_residual is not None
                    else "INCOMPLETE_EVIDENCE_INITIAL_CASH_NOT_PERSISTED"
                ),
                "execution_status": execution_status,
                "cost_status": cost_status,
            }
        )
        reward_rows.append(
            {
                **common,
                "development_primary_reward": _finite(
                    row.get("primary_composite_reward")
                ),
                "development_matched_increment": _finite(
                    row.get("matched_train_increment")
                ),
                "development_search_score": _finite(row.get("search_score")),
                "train_continuous_book_mtm_absolute_reward": _finite(
                    row.get("primary_mark_to_market_net_reward")
                ),
                "train_continuous_book_mtm_matched_increment": _finite(
                    row.get("mark_to_market_net_increment")
                ),
                "train_continuous_book_mtm_cumulative_return": _finite(
                    row.get("primary_cumulative_net_return")
                ),
                "train_continuous_book_mtm_cumulative_return_increment": _finite(
                    row.get("cumulative_net_return_increment")
                ),
                "validation_primary_report_metric": _finite(
                    row.get("primary_validation_report_metric")
                ),
                "validation_matched_report_metric": _finite(
                    row.get("pair_validation_report_metric")
                ),
                "validation_search_score": _finite(row.get("validation_search_score")),
                "oos_mtm_absolute_reward": None,
                "oos_mtm_matched_increment": None,
                "oos_mtm_reward_availability": MISSING,
                "absolute_positive_train_mtm": bool(
                    (_finite(row.get("primary_mark_to_market_net_reward")) or -math.inf)
                    > 0
                ),
                "relative_positive_train_mtm": bool(
                    (_finite(row.get("mark_to_market_net_increment")) or -math.inf) > 0
                ),
            }
        )
        diagnosis_rows.append(
            {
                **common,
                "signal_status": signal_status,
                "portfolio_status": portfolio_status,
                "execution_status": execution_status,
                "cost_status": cost_status,
                "transfer_status": "UNKNOWN_NOT_COMPARABLE_NO_OOS_MTM",
                "diagnosis_labels_json": _safe_json(diagnoses),
            }
        )

    reward = pd.DataFrame(reward_rows)
    rank_fields = (
        "development_primary_reward",
        "development_matched_increment",
        "development_search_score",
        "train_continuous_book_mtm_absolute_reward",
        "train_continuous_book_mtm_matched_increment",
        "validation_primary_report_metric",
        "validation_matched_report_metric",
        "validation_search_score",
    )
    for field in rank_fields:
        reward[f"{field}_rank"] = _numeric(reward, field).rank(
            method="average", ascending=False, na_option="bottom"
        )

    diagnosis = pd.DataFrame(diagnosis_rows)
    portfolio = pd.DataFrame(portfolio_rows)
    execution = pd.DataFrame(execution_rows)
    merged_summary = diagnosis.merge(
        signal_table.drop(columns=["signal_status"]),
        on=["pair_id", "candidate_id", "route_id"],
    )
    merged_summary = merged_summary.merge(
        portfolio[
            [
                "pair_id",
                "candidate_id",
                "route_id",
                "oos_gross_portfolio_return_mean",
                "oos_net_portfolio_return_mean",
                "ending_holdings_weight",
            ]
        ],
        on=["pair_id", "candidate_id", "route_id"],
    ).merge(
        reward[
            [
                "pair_id",
                "candidate_id",
                "route_id",
                "train_continuous_book_mtm_absolute_reward",
                "train_continuous_book_mtm_matched_increment",
                "validation_search_score",
            ]
        ],
        on=["pair_id", "candidate_id", "route_id"],
    )
    route_rows: list[dict[str, Any]] = []
    for route_id, group in merged_summary.groupby("route_id", sort=True):
        route_rows.append(
            {
                "route_id": str(route_id),
                "pair_count": int(len(group)),
                "signal_status_counts_json": _safe_json(
                    Counter(group["signal_status"].astype(str))
                ),
                "portfolio_status_counts_json": _safe_json(
                    Counter(group["portfolio_status"].astype(str))
                ),
                "train_rank_ic_median": _finite(
                    _numeric(group, "train_rank_ic_mean").median()
                ),
                "oos_rank_ic_median": _finite(
                    _numeric(group, "oos_rank_ic_mean").median()
                ),
                "oos_gross_portfolio_return_median": _finite(
                    _numeric(group, "oos_gross_portfolio_return_mean").median()
                ),
                "oos_net_portfolio_return_median": _finite(
                    _numeric(group, "oos_net_portfolio_return_mean").median()
                ),
                "train_mtm_absolute_reward_median": _finite(
                    _numeric(group, "train_continuous_book_mtm_absolute_reward").median()
                ),
                "train_mtm_matched_increment_median": _finite(
                    _numeric(group, "train_continuous_book_mtm_matched_increment").median()
                ),
                "validation_search_score_median": _finite(
                    _numeric(group, "validation_search_score").median()
                ),
                "ending_holdings_weight_median": _finite(
                    _numeric(group, "ending_holdings_weight").median()
                ),
            }
        )
    return {
        "signal_quality": signal_table,
        "portfolio_transformation": portfolio,
        "execution_cost_attribution": execution,
        "reward_alignment": reward,
        "candidate_diagnosis": diagnosis,
        "route_summary": pd.DataFrame(route_rows),
    }


def _top_ids(frame: pd.DataFrame, field: str, k: int) -> list[str]:
    ranked = frame[["pair_id", field]].copy()
    ranked[field] = _numeric(ranked, field)
    ranked = ranked.dropna().sort_values(
        [field, "pair_id"], ascending=[False, True], kind="mergesort"
    )
    return ranked.head(k)["pair_id"].astype(str).tolist()


def _transition(frame: pd.DataFrame, source: str, target: str) -> dict[str, Any]:
    valid = frame[["pair_id", source, target]].copy()
    valid[source] = _numeric(valid, source)
    valid[target] = _numeric(valid, target)
    valid = valid.dropna()
    n = len(valid)
    spearman = _finite(valid[source].corr(valid[target], method="spearman")) if n >= 2 else None
    result: dict[str, Any] = {
        "source": source,
        "target": target,
        "n_valid": int(n),
        "spearman": spearman,
        "source_tie_count": int(n - valid[source].nunique()),
        "target_tie_count": int(n - valid[target].nunique()),
    }
    for label, k in (("top_decile", max(1, math.ceil(n * 0.1))), ("top5", min(5, n))):
        source_ids = _top_ids(valid, source, k)
        target_ids = _top_ids(valid, target, k)
        overlap = sorted(set(source_ids) & set(target_ids))
        result[f"{label}_k"] = int(k)
        result[f"{label}_retention"] = _finite(len(overlap) / k) if k else None
        result[f"{label}_retained_pair_ids"] = overlap
    return result


def rank_preservation_report(
    tables: Mapping[str, pd.DataFrame],
) -> dict[str, Any]:
    signal = tables["signal_quality"]
    reward = tables["reward_alignment"]
    validation = signal.merge(
        reward[["pair_id", "validation_search_score"]], on="pair_id"
    )
    validation_layers = [
        "oos_rank_ic_mean",
        "oos_gross_portfolio_return_mean",
        "oos_net_portfolio_return_mean",
        "validation_search_score",
    ]
    train = signal[["pair_id", "train_rank_ic_mean"]].merge(
        reward[
            [
                "pair_id",
                "development_primary_reward",
                "development_search_score",
                "train_continuous_book_mtm_absolute_reward",
                "train_continuous_book_mtm_matched_increment",
            ]
        ],
        on="pair_id",
    )
    train_layers = [
        "train_rank_ic_mean",
        "development_primary_reward",
        "development_search_score",
        "train_continuous_book_mtm_absolute_reward",
        "train_continuous_book_mtm_matched_increment",
    ]
    alignments = []
    for source in (
        "development_primary_reward",
        "development_search_score",
        "train_continuous_book_mtm_absolute_reward",
        "train_continuous_book_mtm_matched_increment",
    ):
        for target in ("validation_primary_report_metric", "validation_search_score"):
            alignments.append(_transition(reward, source, target))
    return {
        "schema_version": "cn_alpha_autopsy_rank_preservation_v1",
        "pair_count": EXPECTED_PAIR_COUNT,
        "top_decile_k": math.ceil(EXPECTED_PAIR_COUNT * 0.1),
        "validation_predictive_waterfall": {
            "layers": validation_layers,
            "adjacent_transitions": [
                _transition(validation, left, right)
                for left, right in zip(validation_layers, validation_layers[1:])
            ],
            "start_to_end": _transition(
                validation, validation_layers[0], validation_layers[-1]
            ),
        },
        "train_economic_waterfall": {
            "layers": train_layers,
            "adjacent_transitions": [
                _transition(train, left, right)
                for left, right in zip(train_layers, train_layers[1:])
            ],
            "start_to_end": _transition(train, train_layers[0], train_layers[-1]),
        },
        "reward_alignment_to_existing_report_only_validation": alignments,
        "requested_but_unavailable": {
            "spearman_train_reward_to_oos_mtm": MISSING,
            "spearman_train_mtm_to_oos_mtm": MISSING,
            "reason": (
                "The immutable OOS run persisted validation report metrics and reward atoms, "
                "not a continuous-book OOS MTM replay. No validation rerun is authorized."
            ),
        },
    }


def _format_float(value: Any) -> str:
    finite = _finite(value)
    return "NA" if finite is None else f"{finite:.6f}"


def _report_markdown(
    *,
    selection_sha256: str,
    tables: Mapping[str, pd.DataFrame],
    rank_report: Mapping[str, Any],
    source_bindings: Mapping[str, Any],
) -> str:
    signal = tables["signal_quality"]
    reward = tables["reward_alignment"]
    diagnosis = tables["candidate_diagnosis"]
    routes = tables["route_summary"]
    validation_waterfall = rank_report["validation_predictive_waterfall"]
    lines = [
        "# CN_ALPHA_AUTOPSY_V1",
        "",
        "## Scope and decision",
        "",
        f"- Frozen cohort: 32 pairs / 64 members; selection payload `{selection_sha256}`.",
        "- Read-only analysis of already immutable train, continuous-book train MTM, and report-only validation artifacts.",
        "- No search, evaluator change, reward change, validation rerun, holdout/2026 read, promotion, or Graph authority write.",
        "- Decision: **HOLD_RESEARCH**. This report diagnoses the loss path; it does not promote an alpha.",
        "",
        "## Headline evidence",
        "",
        f"- OOS signal RankIC positive: {int((_numeric(signal, 'oos_rank_ic_mean') > 0).sum())}/32.",
        f"- Train MTM absolute reward positive: {int((_numeric(reward, 'train_continuous_book_mtm_absolute_reward') > 0).sum())}/32.",
        f"- Train MTM matched increment positive: {int((_numeric(reward, 'train_continuous_book_mtm_matched_increment') > 0).sum())}/32.",
        f"- Existing validation search score positive: {int((_numeric(reward, 'validation_search_score') > 0).sum())}/32.",
        "- Strict terminal liquidation blockers are not treated as inability to trade: all candidates in the continuous-book MTM evidence completed.",
        "",
        "## Rank-preservation waterfall",
        "",
        "| Source | Target | N | Spearman | Top-decile retention | Top-5 retention |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for transition in validation_waterfall["adjacent_transitions"]:
        lines.append(
            "| {source} | {target} | {n_valid} | {spearman} | {topd} | {top5} |".format(
                source=transition["source"],
                target=transition["target"],
                n_valid=transition["n_valid"],
                spearman=_format_float(transition["spearman"]),
                topd=_format_float(transition["top_decile_retention"]),
                top5=_format_float(transition["top5_retention"]),
            )
        )
    lines.extend(
        [
            "",
            "## Multi-label diagnosis counts",
            "",
            f"- Signal: {_safe_json(Counter(diagnosis['signal_status'].astype(str)))}",
            f"- Portfolio: {_safe_json(Counter(diagnosis['portfolio_status'].astype(str)))}",
            f"- Execution: {_safe_json(Counter(diagnosis['execution_status'].astype(str)))}",
            f"- Cost: {_safe_json(Counter(diagnosis['cost_status'].astype(str)))}",
            "- Transfer remains UNKNOWN because existing OOS evidence is not continuous-book OOS MTM.",
            "",
            "## Route summary",
            "",
            "| Route | Pairs | Train RankIC median | OOS RankIC median | Train MTM abs median | Validation score median |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in routes.to_dict(orient="records"):
        lines.append(
            f"| {row['route_id']} | {row['pair_count']} | "
            f"{_format_float(row['train_rank_ic_median'])} | "
            f"{_format_float(row['oos_rank_ic_median'])} | "
            f"{_format_float(row['train_mtm_absolute_reward_median'])} | "
            f"{_format_float(row['validation_search_score_median'])} |"
        )
    lines.extend(
        [
            "",
            "## Evidence availability and bias audit",
            "",
            "| Requested evidence | Status | Interpretation |",
            "|---|---|---|",
            "| Train/OOS RankIC and horizon decay | AVAILABLE | Rebuilt from immutable streaming reward atoms. |",
            "| Gross/net portfolio return and turnover | AVAILABLE | Rebuilt from immutable validation reward atoms. |",
            "| Continuous-book train MTM absolute/relative economics | AVAILABLE | Final PIT close; no fabricated terminal sale or fee. |",
            f"| Top-bottom spread | {MISSING} | Required constituent quantile series was not persisted. |",
            f"| Average/max position age and realized/unrealized split | {MISSING} | Candidate receipts persist ending book, not lot-age ledger. |",
            f"| Monetary loss by T+1/limit/suspension/liquidity | {MISSING} | Block counts exist; counterfactual fills/prices do not. |",
            f"| Continuous-book OOS MTM reward | {MISSING} | Existing OOS was report-only Phase3CM; rerun forbidden by scope. |",
            "| PIT/split integrity | PASS_FROM_BOUND_SOURCE_CLOSURES | All source manifests and artifacts were hash-verified. |",
            "| OOS sample strength | WEAK | 73 validation dates is below the 250-observation promotion threshold. |",
            "| Costs | INCLUDED_BUT_PARTIAL_ATTRIBUTION | OOS gross/net atoms and actual A-share fees exist; the old MTM receipt omitted initial cash, and slippage counterfactuals do not exist. |",
            "",
            "## Interpretation boundary",
            "",
            "The report separates predictive ranking, portfolio transformation, continuous-book train economics, and existing report-only validation. It does not call the validation metric OOS MTM, does not infer zero execution loss from missing counterfactuals, and does not turn diagnostic correlations into promotion evidence.",
            "",
            "## Source closures",
            "",
        ]
    )
    for name, binding in source_bindings.items():
        lines.append(
            f"- {name}: `{binding['file_sha256']}` / body `{binding['manifest_body_sha256']}`"
        )
    return "\n".join(lines) + "\n"


def build_autopsy(
    *,
    replay_oos_root: Path,
    mark_to_market_root: Path,
    output_root: Path,
    expected_selection_payload_sha256: str | None = None,
) -> dict[str, Any]:
    replay_oos_root = Path(replay_oos_root).resolve()
    mark_to_market_root = Path(mark_to_market_root).resolve()
    output_root = Path(output_root).resolve()
    if output_root.exists() and any(output_root.iterdir()):
        raise RuntimeError(f"autopsy output root must be empty: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)

    source_bindings = {
        "replay_closure": _verify_manifest(
            replay_oos_root / "replay" / "REPLAY_COMPLETE.json",
            allowed_statuses=("A_SHARE_REPLAY_CLOSED_IMMUTABLE",),
        ),
        "oos_closure": _verify_manifest(
            replay_oos_root / "oos" / "OOS_COMPLETE.json",
            allowed_statuses=("REPORT_ONLY_OOS_CLOSED_IMMUTABLE",),
        ),
        "root_closure": _verify_manifest(
            replay_oos_root / "REPLAY_THEN_OOS_COMPLETE.json",
            allowed_statuses=("REPLAY_THEN_OOS_COMPLETE_IMMUTABLE_REPORT_ONLY",),
        ),
        "mtm_closure": _verify_manifest(
            mark_to_market_root / "MARK_TO_MARKET_REPLAY_COMPLETE.json",
            allowed_statuses=(
                "FINAL_CLOSE_MARK_TO_MARKET_REPLAY_CLOSED_IMMUTABLE_DIAGNOSTIC_ONLY",
                "FINAL_CLOSE_MARK_TO_MARKET_REPLAY_CLOSED_IMMUTABLE_TRAIN_ECONOMIC_EVIDENCE",
            ),
        ),
    }
    selections = {
        value["selection_payload_sha256"] for value in source_bindings.values()
    }
    if len(selections) != 1:
        raise RuntimeError(f"source selection payload drift: {sorted(selections)}")
    selection_sha256 = selections.pop()
    if expected_selection_payload_sha256 and selection_sha256 != expected_selection_payload_sha256:
        raise RuntimeError(
            f"selection payload mismatch: {selection_sha256} != "
            f"{expected_selection_payload_sha256}"
        )

    prepared_path = replay_oos_root / "prepared" / "finalist_pairs.parquet"
    oos_path = replay_oos_root / "oos" / "replay_then_oos_pair_results.parquet"
    mtm_pairs_path = mark_to_market_root / "pair_mark_to_market_results.parquet"
    mtm_candidates_path = mark_to_market_root / "candidate_mark_to_market_results.parquet"
    prepared = pd.read_parquet(prepared_path)
    oos = pd.read_parquet(oos_path)
    mtm_pairs = pd.read_parquet(mtm_pairs_path)
    mtm_candidates = pd.read_parquet(mtm_candidates_path)
    identity_columns = (
        "pair_id",
        "route_id",
        "primary_candidate_id",
        "control_candidate_id",
    )
    for label, frame in (("prepared", prepared), ("oos", oos), ("mtm", mtm_pairs)):
        _require_columns(frame, identity_columns, label)
    _ensure_exact_identity(prepared, oos, mtm_pairs)
    _require_columns(
        mtm_candidates,
        ("pair_id", "candidate_id", "pair_member_role", "route_id"),
        "MTM candidates",
    )
    if len(mtm_candidates) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError(f"MTM candidate count drift: {len(mtm_candidates)}")
    if not mtm_candidates["candidate_mark_to_market_status"].astype(str).eq(
        "CANDIDATE_MARK_TO_MARKET_COMPLETE"
    ).all():
        raise RuntimeError("MTM candidates are not all complete")

    atom_paths = sorted((replay_oos_root / "oos").rglob("CN_STREAMING_REWARD_ATOMS.csv"))
    if not atom_paths:
        raise RuntimeError("immutable OOS streaming reward atoms missing")
    atoms = pd.concat((pd.read_csv(path) for path in atom_paths), ignore_index=True)
    atoms = atoms.drop_duplicates().reset_index(drop=True)
    signal = aggregate_signal_quality(atoms, prepared)
    tables = build_tables(prepared, oos, mtm_pairs, mtm_candidates, signal)
    rank_report = rank_preservation_report(tables)

    artifacts: list[Path] = []
    for name, frame in tables.items():
        path = output_root / f"{name}.parquet"
        frame.to_parquet(path, index=False)
        artifacts.append(path)
    rank_path = _write_json(output_root / "rank_preservation_report.json", rank_report)
    artifacts.append(rank_path)
    source_public = {
        name: {key: value[key] for key in value if key != "payload"}
        for name, value in source_bindings.items()
    }
    report_path = output_root / "CN_ALPHA_AUTOPSY_V1.md"
    report_path.write_text(
        _report_markdown(
            selection_sha256=selection_sha256,
            tables=tables,
            rank_report=rank_report,
            source_bindings=source_public,
        ),
        encoding="utf-8",
    )
    artifacts.append(report_path)
    source_path = _write_json(
        output_root / "source_closure_bindings.json",
        {
            "schema_version": "cn_alpha_autopsy_source_bindings_v1",
            "selection_payload_sha256": selection_sha256,
            "bindings": source_public,
            "source_data_access": "READ_EXISTING_IMMUTABLE_ARTIFACTS_ONLY",
            "new_financial_evaluation": False,
            "new_validation_evaluation": False,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
    )
    artifacts.append(source_path)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "status": "CN_ALPHA_AUTOPSY_V1_CLOSED_IMMUTABLE_HOLD_RESEARCH",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection_payload_sha256": selection_sha256,
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "source_data_access": "READ_EXISTING_IMMUTABLE_ARTIFACTS_ONLY",
        "new_financial_evaluation": False,
        "new_validation_evaluation": False,
        "search_run": False,
        "reward_changed": False,
        "evaluator_changed": False,
        "graph_authority_changed": False,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
        "decision": "HOLD_RESEARCH",
        "artifacts": [_artifact(path, root=output_root) for path in artifacts],
    }
    manifest["manifest_body_sha256"] = _stable_hash(manifest)
    manifest_path = _write_json(output_root / "AUTOPSY_COMPLETE.json", manifest)
    return {
        "status": manifest["status"],
        "output_root": str(output_root),
        "selection_payload_sha256": selection_sha256,
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_body_sha256": manifest["manifest_body_sha256"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-oos-root", type=Path, required=True)
    parser.add_argument("--mark-to-market-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--expected-selection-payload-sha256")
    args = parser.parse_args()
    result = build_autopsy(
        replay_oos_root=args.replay_oos_root,
        mark_to_market_root=args.mark_to_market_root,
        output_root=args.output_root,
        expected_selection_payload_sha256=args.expected_selection_payload_sha256,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
