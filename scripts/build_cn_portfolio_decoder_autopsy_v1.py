from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import sys
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import psutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_finalist_mark_to_market_replay as mtm
from scripts import run_cn_finalist_replay_then_oos as base
from our_system_phase2.services.a_share_executable_replay import (
    AShareCorporateActionPolicy,
    AShareExecutionPolicy,
    AShareFeeSchedule,
    AShareUniversePolicy,
    ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    _prepare_sessions,
    run_a_share_long_only_replay,
)


SCHEMA_VERSION = "cn_portfolio_decoder_autopsy_v1"
EXPECTED_PAIR_COUNT = 32
EXPECTED_MEMBER_COUNT = 64
FORWARD_HOLDING_SESSIONS = (1, 5, 15, 30)
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
AUTHORIZED_HOST = "DESKTOP-77OPJ6F"

DECODERS: tuple[dict[str, Any], ...] = (
    {
        "decoder_id": "CURRENT_TOP20PCT_EQUAL",
        "selection": "TOP_FRACTION",
        "top_fraction": 0.20,
        "weighting": "EQUAL",
    },
    *(
        {
            "decoder_id": f"TOPK_{top_k}_EQUAL",
            "selection": "TOP_K",
            "top_k": top_k,
            "weighting": "EQUAL",
        }
        for top_k in (10, 20, 50)
    ),
    *(
        {
            "decoder_id": f"TOPK_{top_k}_RANK",
            "selection": "TOP_K",
            "top_k": top_k,
            "weighting": "LINEAR_DESCENDING_RANK",
        }
        for top_k in (10, 20, 50)
    ),
    *(
        {
            "decoder_id": f"SOFTMAX_TOP50_TAU_{temperature:.2f}",
            "selection": "TOP_K",
            "top_k": 50,
            "weighting": "SOFTMAX_PERCENTILE_RANK",
            "temperature": temperature,
        }
        for temperature in (0.10, 0.25, 0.50)
    ),
)


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
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
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
    os.replace(temporary, destination)
    return destination


def _finite(value: Any) -> float | None:
    try:
        rendered = float(value)
    except (TypeError, ValueError):
        return None
    return rendered if math.isfinite(rendered) else None


def _artifact(path: Path, *, root: Path) -> dict[str, Any]:
    source = Path(path).resolve()
    return {
        "path": source.relative_to(Path(root).resolve()).as_posix(),
        "bytes": source.stat().st_size,
        "sha256": _sha256(source),
    }


def _verify_manifest_artifacts(
    path: Path,
    *,
    status_field: str = "status",
    allowed_statuses: Iterable[str],
) -> dict[str, Any]:
    source = Path(path).resolve()
    payload = _read_json(source)
    if str(payload.get(status_field) or "") not in set(allowed_statuses):
        raise RuntimeError(f"manifest status drift: {source}")
    body_hash = str(payload.get("manifest_body_sha256") or "")
    if not body_hash:
        raise RuntimeError(f"manifest self-hash missing: {source}")
    body = dict(payload)
    body.pop("manifest_body_sha256", None)
    if _stable_hash(body) != body_hash:
        raise RuntimeError(f"manifest self-hash mismatch: {source}")
    for record in payload.get("artifacts") or ():
        rendered = Path(str(record["path"]))
        artifact_path = (
            rendered if rendered.is_absolute() else source.parent / rendered
        ).resolve()
        if not artifact_path.is_file():
            raise RuntimeError(f"declared artifact missing: {artifact_path}")
        if int(record["bytes"]) != artifact_path.stat().st_size:
            raise RuntimeError(f"declared artifact size drift: {artifact_path}")
        if str(record["sha256"]) != _sha256(artifact_path):
            raise RuntimeError(f"declared artifact hash drift: {artifact_path}")
    return payload


def decoder_weights(
    signal: np.ndarray,
    codes: np.ndarray,
    decoder: Mapping[str, Any],
) -> tuple[np.ndarray, np.ndarray]:
    """Return deterministic selected positions and normalized long-only weights."""

    values = np.asarray(signal, dtype=float)
    rendered_codes = np.asarray(codes, dtype=str)
    if len(values) != len(rendered_codes):
        raise ValueError("signal/code length mismatch")
    valid = np.isfinite(values)
    if not bool(valid.any()):
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=float)
    positions = np.flatnonzero(valid)
    order = np.lexsort((rendered_codes[positions], -values[positions]))
    ordered = positions[order]
    if str(decoder["selection"]) == "TOP_FRACTION":
        count = max(1, int(math.ceil(len(ordered) * float(decoder["top_fraction"]))))
    elif str(decoder["selection"]) == "TOP_K":
        count = min(len(ordered), int(decoder["top_k"]))
    else:
        raise ValueError(f"unknown decoder selection: {decoder['selection']}")
    selected = ordered[:count]
    weighting = str(decoder["weighting"])
    if weighting == "EQUAL":
        weights = np.full(count, 1.0 / count, dtype=float)
    elif weighting == "LINEAR_DESCENDING_RANK":
        raw = np.arange(count, 0, -1, dtype=float)
        weights = raw / float(raw.sum())
    elif weighting == "SOFTMAX_PERCENTILE_RANK":
        temperature = float(decoder["temperature"])
        if temperature <= 0:
            raise ValueError("softmax temperature must be positive")
        # Magnitude-free percentile ranks prevent formula scale from silently
        # becoming a second, unregistered decoder parameter.
        percentile = np.arange(count, 0, -1, dtype=float) / count
        logits = percentile / temperature
        raw = np.exp(logits - float(logits.max()))
        weights = raw / float(raw.sum())
    else:
        raise ValueError(f"unknown decoder weighting: {weighting}")
    if not math.isclose(float(weights.sum()), 1.0, abs_tol=1e-12):
        raise RuntimeError("decoder weights do not sum to one")
    return selected, weights


def forward_open_to_close_returns(
    frame: pd.DataFrame,
    horizons: Sequence[int] = FORWARD_HOLDING_SESSIONS,
) -> dict[int, np.ndarray]:
    """Build train-only next-open to H-th-session-close gross returns.

    H=1 means signal at close T, buy at open T+1, mark at close T+1.  These
    horizons are stock sessions.  They must never be labelled as minutes.
    """

    required = {"date", "code", "open", "close"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"session frame missing columns: {missing}")
    work = frame[["date", "code", "open", "close"]].copy()
    work["date"] = pd.to_datetime(work["date"], errors="raise")
    if not work.groupby("code", sort=False)["date"].apply(
        lambda values: values.is_monotonic_increasing
    ).all():
        raise ValueError("session frame must be chronological within each code")
    grouped_open = pd.to_numeric(work["open"], errors="coerce").groupby(
        work["code"], sort=False
    )
    grouped_close = pd.to_numeric(work["close"], errors="coerce").groupby(
        work["code"], sort=False
    )
    entry = grouped_open.shift(-1).to_numpy(dtype=float)
    results: dict[int, np.ndarray] = {}
    for horizon in horizons:
        if int(horizon) <= 0:
            raise ValueError("holding horizons must be positive sessions")
        exit_close = grouped_close.shift(-int(horizon)).to_numpy(dtype=float)
        with np.errstate(divide="ignore", invalid="ignore"):
            value = exit_close / entry - 1.0
        value[~np.isfinite(value)] = np.nan
        results[int(horizon)] = value
    return results


def _rank_correlation(left: np.ndarray, right: np.ndarray) -> float | None:
    frame = pd.DataFrame({"left": left, "right": right}).dropna()
    if len(frame) < 3 or frame["left"].nunique() < 2 or frame["right"].nunique() < 2:
        return None
    return _finite(
        frame["left"].rank(method="average").corr(
            frame["right"].rank(method="average")
        )
    )


def _target_turnover(
    previous: Mapping[str, float] | None,
    current: Mapping[str, float],
) -> float:
    if previous is None:
        return 1.0 if current else 0.0
    keys = set(previous) | set(current)
    return 0.5 * sum(abs(float(current.get(key, 0.0)) - float(previous.get(key, 0.0))) for key in keys)


def evaluate_candidate_decoder_matrix(
    *,
    candidate: Mapping[str, Any],
    signal: np.ndarray,
    codes: np.ndarray,
    dates: np.ndarray,
    eligible: np.ndarray,
    future_returns: Mapping[int, np.ndarray],
    date_groups: Sequence[np.ndarray],
    decoders: Sequence[Mapping[str, Any]] = DECODERS,
) -> list[dict[str, Any]]:
    signal_values = np.asarray(signal, dtype=float)
    code_values = np.asarray(codes, dtype=str)
    date_values = np.asarray(dates)
    eligible_values = np.asarray(eligible, dtype=bool)
    if not (
        len(signal_values)
        == len(code_values)
        == len(date_values)
        == len(eligible_values)
    ):
        raise ValueError("candidate matrix input length mismatch")
    states: dict[tuple[str, int], dict[str, Any]] = {}
    for decoder in decoders:
        for horizon in future_returns:
            states[(str(decoder["decoder_id"]), int(horizon))] = {
                "gross": [],
                "turnover": [],
                "hhi": [],
                "top10": [],
                "selected_count": [],
                "rank_ic": [],
                "previous": None,
            }

    for group in date_groups:
        group_signal = signal_values[group]
        group_codes = code_values[group]
        for horizon, all_returns in future_returns.items():
            group_return = np.asarray(all_returns, dtype=float)[group]
            valid = (
                eligible_values[group]
                & np.isfinite(group_signal)
                & np.isfinite(group_return)
            )
            if int(valid.sum()) < 3:
                continue
            local_signal = group_signal[valid]
            local_codes = group_codes[valid]
            local_return = group_return[valid]
            rank_ic = _rank_correlation(local_signal, local_return)
            for decoder in decoders:
                decoder_id = str(decoder["decoder_id"])
                selected, weights = decoder_weights(
                    local_signal,
                    local_codes,
                    decoder,
                )
                if len(selected) == 0:
                    continue
                gross = float(np.dot(weights, local_return[selected]))
                target = {
                    str(code): float(weight)
                    for code, weight in zip(local_codes[selected], weights, strict=True)
                }
                state = states[(decoder_id, int(horizon))]
                state["gross"].append(gross)
                state["turnover"].append(
                    _target_turnover(state["previous"], target)
                )
                state["previous"] = target
                state["hhi"].append(float(np.square(weights).sum()))
                state["top10"].append(float(np.sort(weights)[::-1][:10].sum()))
                state["selected_count"].append(len(selected))
                if rank_ic is not None:
                    state["rank_ic"].append(rank_ic)

    rows: list[dict[str, Any]] = []
    for decoder in decoders:
        decoder_id = str(decoder["decoder_id"])
        for horizon in sorted(future_returns):
            state = states[(decoder_id, int(horizon))]
            gross = np.asarray(state["gross"], dtype=float)
            rank_ic = np.asarray(state["rank_ic"], dtype=float)
            rows.append(
                {
                    "pair_id": str(candidate["pair_id"]),
                    "candidate_id": str(candidate["candidate_id"]),
                    "pair_member_role": str(candidate["pair_member_role"]),
                    "route_id": str(candidate["route_id"]),
                    "exact_identity": str(candidate["exact_identity"]),
                    "decoder_id": decoder_id,
                    "decoder_contract_json": json.dumps(
                        dict(decoder), sort_keys=True, separators=(",", ":")
                    ),
                    "signal_clock": "SESSION_CLOSE_T",
                    "entry_clock": "NEXT_SESSION_OPEN_T_PLUS_1",
                    "forward_holding_sessions": int(horizon),
                    "historical_phase3cm_horizon_field": f"horizon_min={horizon}",
                    "horizon_semantics": "STOCK_SESSION_SHIFT_NOT_MINUTE",
                    "gross_forward_return_mean": _finite(gross.mean()) if len(gross) else None,
                    "gross_forward_return_median": _finite(np.median(gross)) if len(gross) else None,
                    "gross_forward_return_hit_rate": _finite((gross > 0).mean()) if len(gross) else None,
                    "signal_rank_ic_mean": _finite(rank_ic.mean()) if len(rank_ic) else None,
                    "signal_rank_ic_hit_rate": _finite((rank_ic > 0).mean()) if len(rank_ic) else None,
                    "target_one_way_turnover_mean": _finite(np.mean(state["turnover"])) if state["turnover"] else None,
                    "target_hhi_mean": _finite(np.mean(state["hhi"])) if state["hhi"] else None,
                    "target_top10_weight_mean": _finite(np.mean(state["top10"])) if state["top10"] else None,
                    "selected_count_mean": _finite(np.mean(state["selected_count"])) if state["selected_count"] else None,
                    "observation_session_count": int(len(gross)),
                    "financial_claim_authorized": False,
                    "promotion_authorized": False,
                }
            )
    return rows


def _top_set(frame: pd.DataFrame, column: str, count: int) -> set[str]:
    ranked = frame.dropna(subset=[column]).sort_values(
        [column, "candidate_id"], ascending=[False, True], kind="mergesort"
    )
    return set(ranked.head(count)["candidate_id"].astype(str))


def build_rank_preservation(
    candidate_metrics: pd.DataFrame,
    existing_mtm: pd.DataFrame,
) -> pd.DataFrame:
    primary = candidate_metrics[
        candidate_metrics["pair_member_role"].astype(str).eq("PRIMARY")
    ].copy()
    mtm_columns = [
        "candidate_id",
        "mark_to_market_net_reward",
        "ending_nav_cny",
    ]
    mtm_primary = existing_mtm[
        existing_mtm["pair_member_role"].astype(str).eq("PRIMARY")
    ][mtm_columns].copy()
    rows: list[dict[str, Any]] = []
    for (decoder_id, horizon), group in primary.groupby(
        ["decoder_id", "forward_holding_sessions"], sort=True
    ):
        merged = group.merge(mtm_primary, on="candidate_id", how="left", validate="one_to_one")
        n = len(merged)
        top_decile_count = max(1, int(math.ceil(n * 0.10)))
        signal_top_decile = _top_set(merged, "signal_rank_ic_mean", top_decile_count)
        gross_top_decile = _top_set(merged, "gross_forward_return_mean", top_decile_count)
        signal_top5 = _top_set(merged, "signal_rank_ic_mean", min(5, n))
        gross_top5 = _top_set(merged, "gross_forward_return_mean", min(5, n))
        rows.append(
            {
                "decoder_id": str(decoder_id),
                "forward_holding_sessions": int(horizon),
                "primary_candidate_count": n,
                "signal_to_gross_spearman": _rank_correlation(
                    pd.to_numeric(merged["signal_rank_ic_mean"], errors="coerce").to_numpy(dtype=float),
                    pd.to_numeric(merged["gross_forward_return_mean"], errors="coerce").to_numpy(dtype=float),
                ),
                "signal_top_decile_count": top_decile_count,
                "top_decile_retention": len(signal_top_decile & gross_top_decile) / max(1, top_decile_count),
                "top5_retention": len(signal_top5 & gross_top5) / max(1, len(signal_top5)),
                "gross_to_existing_mtm_reward_spearman": _rank_correlation(
                    pd.to_numeric(merged["gross_forward_return_mean"], errors="coerce").to_numpy(dtype=float),
                    pd.to_numeric(merged["mark_to_market_net_reward"], errors="coerce").to_numpy(dtype=float),
                ),
                "gross_to_existing_mtm_ending_nav_spearman": _rank_correlation(
                    pd.to_numeric(merged["gross_forward_return_mean"], errors="coerce").to_numpy(dtype=float),
                    pd.to_numeric(merged["ending_nav_cny"], errors="coerce").to_numpy(dtype=float),
                ),
                "evidence_scope": "DEVELOPMENT_TRAIN_ONLY",
            }
        )
    return pd.DataFrame(rows)


def build_pair_metrics(candidate_metrics: pd.DataFrame) -> pd.DataFrame:
    primary = candidate_metrics[
        candidate_metrics["pair_member_role"].astype(str).eq("PRIMARY")
    ].copy()
    control = candidate_metrics[
        candidate_metrics["pair_member_role"].astype(str).eq("CONTROL")
    ].copy()
    keys = ["pair_id", "decoder_id", "forward_holding_sessions"]
    primary = primary.rename(
        columns={
            "candidate_id": "primary_candidate_id",
            "gross_forward_return_mean": "primary_gross_forward_return_mean",
            "signal_rank_ic_mean": "primary_signal_rank_ic_mean",
            "target_one_way_turnover_mean": "primary_target_one_way_turnover_mean",
        }
    )
    control = control.rename(
        columns={
            "candidate_id": "control_candidate_id",
            "gross_forward_return_mean": "control_gross_forward_return_mean",
            "signal_rank_ic_mean": "control_signal_rank_ic_mean",
            "target_one_way_turnover_mean": "control_target_one_way_turnover_mean",
        }
    )
    keep_primary = keys + [
        "route_id",
        "primary_candidate_id",
        "primary_gross_forward_return_mean",
        "primary_signal_rank_ic_mean",
        "primary_target_one_way_turnover_mean",
    ]
    keep_control = keys + [
        "control_candidate_id",
        "control_gross_forward_return_mean",
        "control_signal_rank_ic_mean",
        "control_target_one_way_turnover_mean",
    ]
    merged = primary[keep_primary].merge(
        control[keep_control], on=keys, how="inner", validate="one_to_one"
    )
    merged["matched_gross_forward_return_increment"] = (
        merged["primary_gross_forward_return_mean"]
        - merged["control_gross_forward_return_mean"]
    )
    merged["matched_signal_rank_ic_increment"] = (
        merged["primary_signal_rank_ic_mean"]
        - merged["control_signal_rank_ic_mean"]
    )
    merged["primary_absolute_positive"] = (
        merged["primary_gross_forward_return_mean"] > 0
    )
    merged["matched_increment_positive"] = (
        merged["matched_gross_forward_return_increment"] > 0
    )
    merged["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    merged["promotion_authorized"] = False
    return merged


def _baseline_canary(
    *,
    candidate: Mapping[str, Any],
    signal: np.ndarray,
    master: pd.DataFrame,
    fee: AShareFeeSchedule,
    universe: AShareUniversePolicy,
    execution: AShareExecutionPolicy,
    corporate: AShareCorporateActionPolicy,
    existing_mtm: pd.DataFrame,
) -> dict[str, Any]:
    frame = master.copy()
    frame["signal"] = np.asarray(signal, dtype=float)
    result = run_a_share_long_only_replay(
        frame,
        fee_schedule=fee,
        universe_policy=universe,
        execution_policy=execution,
        corporate_action_policy=corporate,
        ending_book_policy=ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    )
    expected = existing_mtm[
        existing_mtm["candidate_id"].astype(str).eq(str(candidate["candidate_id"]))
    ]
    if len(expected) != 1:
        raise RuntimeError("baseline canary existing MTM row drift")
    row = expected.iloc[0]
    comparisons = {
        "ending_nav_cny": (float(result["ending_nav_cny"]), float(row["ending_nav_cny"]), 1e-6),
        "total_fees_cny": (float(result["total_fees_cny"]), float(row["total_fees_cny"]), 1e-6),
        "trade_count": (int(result["trade_count"]), int(row["trade_count"]), 0),
        "fill_count": (int(result["fill_count"]), int(row["fill_count"]), 0),
        "blocked_buy_count": (int(result["blocked_buy_count"]), int(row["blocked_buy_count"]), 0),
        "blocked_sell_count": (int(result["blocked_sell_count"]), int(row["blocked_sell_count"]), 0),
    }
    mismatches = []
    rendered: dict[str, Any] = {}
    for key, (actual, historical, tolerance) in comparisons.items():
        delta = float(actual) - float(historical)
        rendered[key] = {
            "actual": actual,
            "historical": historical,
            "delta": delta,
            "tolerance": tolerance,
        }
        if abs(delta) > float(tolerance):
            mismatches.append(key)
    if mismatches:
        raise RuntimeError(f"current decoder baseline canary mismatch: {mismatches}")
    return {
        "schema_version": "cn_portfolio_decoder_current_baseline_canary_v1",
        "status": "CURRENT_DECODER_EXACT_REPLAY_PARITY_PASS",
        "candidate_id": str(candidate["candidate_id"]),
        "decoder_id": "CURRENT_TOP20PCT_EQUAL",
        "ending_book_policy": ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
        "comparisons": rendered,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def _report(
    *,
    selection_sha256: str,
    rank: pd.DataFrame,
    pairs: pd.DataFrame,
    baseline: Mapping[str, Any],
) -> str:
    ordered = rank.sort_values(
        ["signal_to_gross_spearman", "decoder_id", "forward_holding_sessions"],
        ascending=[False, True, True],
        na_position="last",
    )
    best = ordered.iloc[0] if len(ordered) else None
    lines = [
        "# CN_PORTFOLIO_DECODER_AUTOPSY_V1",
        "",
        f"- Frozen selection: `{selection_sha256}` (32 pairs / 64 members).",
        "- Scope: development train only; no search, validation, holdout or 2026 reads.",
        "- Current executable decoder canary: " + str(baseline["status"]),
        "- Critical semantic finding: the stock-session backend stores one 15:00 row per security/session. Historical `horizon_min=1/5/15/30` names therefore behave as row/session shifts here, not true minute holding clocks.",
        "- Decoder gross metrics use signal at close T, theoretical entry at open T+1, and mark at the close of the H-th following stock session. They are mapping diagnostics, not executable PnL or promotion evidence.",
        "",
        "## Rank preservation",
        "",
        "| decoder | hold sessions | signal→gross Spearman | top-decile retention | top-5 retention | gross→existing MTM reward Spearman |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in ordered.to_dict(orient="records"):
        lines.append(
            "| {decoder_id} | {forward_holding_sessions} | {signal_to_gross_spearman} | {top_decile_retention:.3f} | {top5_retention:.3f} | {gross_to_existing_mtm_reward_spearman} |".format(
                decoder_id=row["decoder_id"],
                forward_holding_sessions=row["forward_holding_sessions"],
                signal_to_gross_spearman=(
                    "NA" if row["signal_to_gross_spearman"] is None or pd.isna(row["signal_to_gross_spearman"]) else f"{float(row['signal_to_gross_spearman']):.6f}"
                ),
                top_decile_retention=float(row["top_decile_retention"]),
                top5_retention=float(row["top5_retention"]),
                gross_to_existing_mtm_reward_spearman=(
                    "NA" if row["gross_to_existing_mtm_reward_spearman"] is None or pd.isna(row["gross_to_existing_mtm_reward_spearman"]) else f"{float(row['gross_to_existing_mtm_reward_spearman']):.6f}"
                ),
            )
        )
    lines.extend(["", "## Diagnostic boundary", ""])
    if best is not None:
        lines.append(
            "- Highest observed train rank preservation (diagnostic only): "
            f"`{best['decoder_id']}` at {int(best['forward_holding_sessions'])} sessions, "
            f"Spearman={float(best['signal_to_gross_spearman']):.6f}."
        )
    both_positive = int(
        (
            pairs["primary_absolute_positive"].astype(bool)
            & pairs["matched_increment_positive"].astype(bool)
        ).sum()
    )
    lines.extend(
        [
            f"- Positive absolute and matched train cells: {both_positive}/{len(pairs)} pair-decoder-horizon cells.",
            "- No decoder is promoted. The report only identifies where signal ordering survives or breaks under frozen mappings.",
            "- A later reward or portfolio authority change requires a separate decision and unchanged report-only OOS; it is not performed here.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_decoder_autopsy(
    *,
    replay_oos_root: Path,
    mark_to_market_root: Path,
    output_root: Path,
    builder_commit_sha: str,
    worker_count: int = 8,
    expected_selection_payload_sha256: str | None = None,
) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(f"decoder autopsy must run on {AUTHORIZED_HOST}")
    if psutil.virtual_memory().available < MINIMUM_FREE_MEMORY_BYTES:
        raise RuntimeError("decoder autopsy minimum-free-memory gate failed")
    if int(worker_count) < 1 or int(worker_count) > 8:
        raise ValueError("worker_count must be in [1, 8]")
    if len(builder_commit_sha) != 40 or any(ch not in "0123456789abcdef" for ch in builder_commit_sha):
        raise ValueError("builder_commit_sha must be a lowercase Git SHA")

    replay_oos_root = Path(replay_oos_root).resolve()
    mark_to_market_root = Path(mark_to_market_root).resolve()
    output_root = Path(output_root).resolve()
    freeze_path = replay_oos_root / "prepared" / "finalist_replay_then_oos_freeze.json"
    train_field_root = replay_oos_root / "sidecars" / "train_session_fields"
    freeze = mtm._load_freeze_for_mark_to_market(freeze_path)
    selection_sha256 = str(freeze["selection_payload_sha256"])
    if expected_selection_payload_sha256 and selection_sha256 != expected_selection_payload_sha256:
        raise RuntimeError("decoder autopsy selection payload drift")
    if int(freeze["pair_count"]) != EXPECTED_PAIR_COUNT or int(freeze["candidate_member_count"]) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError("decoder autopsy frozen cohort cardinality drift")

    mtm_closure_path = mark_to_market_root / "MARK_TO_MARKET_REPLAY_COMPLETE.json"
    mtm_closure = _verify_manifest_artifacts(
        mtm_closure_path,
        allowed_statuses=(
            "FINAL_CLOSE_MARK_TO_MARKET_REPLAY_CLOSED_IMMUTABLE_DIAGNOSTIC_ONLY",
            "FINAL_CLOSE_MARK_TO_MARKET_REPLAY_CLOSED_IMMUTABLE_TRAIN_ECONOMIC_EVIDENCE",
        ),
    )
    if str(mtm_closure["selection_payload_sha256"]) != selection_sha256:
        raise RuntimeError("decoder autopsy MTM selection drift")

    frozen_root = freeze_path.parents[1]
    candidate_path = base._resolved_artifact(frozen_root, freeze["candidate_artifact"])
    contract_path = base._resolved_artifact(frozen_root, freeze["execution_contract_artifact"])
    candidates = pd.read_parquet(candidate_path).where(pd.notna, None)
    if candidates["candidate_id"].astype(str).tolist() != list(freeze["candidate_ids"]):
        raise RuntimeError("decoder autopsy candidate identity/order drift")
    existing_mtm_path = mark_to_market_root / "candidate_mark_to_market_results.parquet"
    existing_mtm = pd.read_parquet(existing_mtm_path)
    if set(existing_mtm["candidate_id"].astype(str)) != set(candidates["candidate_id"].astype(str)):
        raise RuntimeError("decoder autopsy historical MTM candidate drift")

    contract = _read_json(contract_path)
    base._verify_payload_hash(
        contract,
        field="contract_payload_sha256",
        label="decoder autopsy execution contract",
    )
    if dict(contract["execution_policy"]) != {
        "default_lot_size": 100,
        "execution_clock": "NEXT_SESSION_OPEN_T_PLUS_1",
        "initial_cash_cny": 1000000.0,
        "long_only": True,
        "price_tick": 0.01,
        "rebalance_frequency": "EACH_SESSION",
        "signal_clock": "SESSION_CLOSE_T",
        "top_quantile": 0.2,
    }:
        raise RuntimeError("current decoder execution contract drift")
    field_manifest, field_manifest_path = base._validate_sidecar(
        train_field_root,
        evaluation_role="train",
        split_hash=str(contract["split_manifest_sha256"]),
    )
    field_frame = base._load_field_frame(train_field_root, field_manifest)
    if not field_frame["trade_time"].dt.strftime("%H:%M:%S").eq("15:00:00").all():
        raise RuntimeError("stock-session sidecar contains non-session-close rows")
    if field_frame.groupby(field_frame["trade_time"].dt.normalize())["trade_time"].nunique().max() != 1:
        raise RuntimeError("stock-session sidecar contains intraday clocks")

    session_manifest_path = Path(str(contract["session_authority_manifest"])).resolve()
    session_path = Path(str(contract["session_authority_path"])).resolve()
    if _sha256(session_manifest_path) != str(contract["session_authority_manifest_sha256"]):
        raise RuntimeError("decoder autopsy session authority manifest drift")
    session_authority = pd.read_parquet(session_path)
    master, observed_index, authority_index = base._materialize_replay_master(
        field_frame, session_authority
    )
    universe_raw = dict(contract["universe_policy"])
    universe_raw["allowed_exchanges"] = tuple(universe_raw["allowed_exchanges"])
    universe = AShareUniversePolicy(**universe_raw)
    fee = AShareFeeSchedule(**dict(contract["fee_schedule"]))
    execution = AShareExecutionPolicy(**dict(contract["execution_policy"]))
    corporate = AShareCorporateActionPolicy(**dict(contract["corporate_action_policy"]))
    prepared_master = _prepare_sessions(
        master.assign(signal=0.0), universe_policy=universe
    ).drop(columns=["signal"])
    prepared_index = pd.MultiIndex.from_frame(prepared_master[["date", "code"]])
    if set(prepared_index) != set(authority_index):
        raise RuntimeError("decoder autopsy session coordinate drift")
    future_returns = forward_open_to_close_returns(prepared_master)
    codes = prepared_master["code"].astype(str).to_numpy()
    dates = pd.to_datetime(prepared_master["date"]).to_numpy()
    eligible = prepared_master["promotion_universe_eligible"].astype(bool).to_numpy()
    date_order = np.argsort(dates, kind="mergesort")
    sorted_dates = dates[date_order]
    boundaries = np.flatnonzero(np.r_[True, sorted_dates[1:] != sorted_dates[:-1], True])
    date_groups = [date_order[start:end] for start, end in zip(boundaries[:-1], boundaries[1:])]

    input_binding = {
        "schema_version": "cn_portfolio_decoder_autopsy_input_binding_v1",
        "selection_payload_sha256": selection_sha256,
        "freeze_sha256": _sha256(freeze_path),
        "candidate_sha256": _sha256(candidate_path),
        "contract_sha256": _sha256(contract_path),
        "train_field_manifest_sha256": _sha256(field_manifest_path),
        "session_authority_manifest_sha256": _sha256(session_manifest_path),
        "session_authority_sha256": _sha256(session_path),
        "mark_to_market_closure_sha256": _sha256(mtm_closure_path),
        "mark_to_market_candidate_results_sha256": _sha256(existing_mtm_path),
        "builder_commit_sha": builder_commit_sha,
        "builder_source_sha256": _sha256(Path(__file__).resolve()),
        "decoder_contract_sha256": _stable_hash(list(DECODERS)),
        "forward_holding_sessions": list(FORWARD_HOLDING_SESSIONS),
        "horizon_semantics": "STOCK_SESSION_SHIFT_NOT_MINUTE",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "search_run": False,
        "reward_changed": False,
        "evaluator_changed": False,
    }
    input_data_sha256 = _stable_hash(input_binding)
    output_root.mkdir(parents=True, exist_ok=True)
    binding_path = output_root / "input_binding.json"
    if binding_path.is_file():
        if _read_json(binding_path) != input_binding:
            raise RuntimeError("decoder autopsy resume input binding drift")
    else:
        _write_json(binding_path, input_binding)

    candidate_root = output_root / "candidates"
    candidate_root.mkdir(parents=True, exist_ok=True)
    first_primary = next(
        row
        for row in candidates.to_dict(orient="records")
        if str(row["pair_member_role"]) == "PRIMARY"
    )
    canary_signal_field = pd.to_numeric(
        base.evaluate_panel_expression(
            field_frame,
            str(first_primary["expression"]),
            cache={},
            data_role="development",
        ),
        errors="coerce",
    )
    canary_signal_series = pd.Series(canary_signal_field.to_numpy(), index=observed_index)
    canary_signal = canary_signal_series.reindex(authority_index).to_numpy(dtype=float)
    baseline = _baseline_canary(
        candidate=first_primary,
        signal=canary_signal,
        master=master,
        fee=fee,
        universe=universe,
        execution=execution,
        corporate=corporate,
        existing_mtm=existing_mtm,
    )
    baseline_path = _write_json(output_root / "current_decoder_baseline_canary.json", baseline)

    def evaluate_one(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
        candidate_id = str(candidate["candidate_id"])
        target = candidate_root / f"{candidate_id}.json"
        if target.is_file():
            payload = _read_json(target)
            if (
                str(payload.get("status")) != "DECODER_CANDIDATE_CLOSED_IMMUTABLE"
                or str(payload.get("candidate_id")) != candidate_id
                or str(payload.get("input_data_sha256")) != input_data_sha256
            ):
                raise RuntimeError(f"decoder candidate resume drift: {candidate_id}")
            return list(payload["metrics"])
        if candidate_id == str(first_primary["candidate_id"]):
            aligned_signal = canary_signal_series.reindex(prepared_index).to_numpy(
                dtype=float
            )
        else:
            signal_field = pd.to_numeric(
                base.evaluate_panel_expression(
                    field_frame,
                    str(candidate["expression"]),
                    cache={},
                    data_role="development",
                ),
                errors="coerce",
            )
            signal_series = pd.Series(signal_field.to_numpy(), index=observed_index)
            aligned_signal = signal_series.reindex(prepared_index).to_numpy(dtype=float)
        metrics = evaluate_candidate_decoder_matrix(
            candidate=candidate,
            signal=aligned_signal,
            codes=codes,
            dates=dates,
            eligible=eligible,
            future_returns=future_returns,
            date_groups=date_groups,
        )
        payload = {
            "schema_version": "cn_portfolio_decoder_candidate_result_v1",
            "status": "DECODER_CANDIDATE_CLOSED_IMMUTABLE",
            "candidate_id": candidate_id,
            "input_data_sha256": input_data_sha256,
            "metrics": metrics,
        }
        payload["payload_sha256"] = _stable_hash(payload)
        _write_json(target, payload)
        return metrics

    metric_rows: list[dict[str, Any]] = []
    candidate_records = candidates.to_dict(orient="records")
    with ThreadPoolExecutor(max_workers=int(worker_count)) as executor:
        futures = {executor.submit(evaluate_one, row): str(row["candidate_id"]) for row in candidate_records}
        for future in as_completed(futures):
            metric_rows.extend(future.result())
    candidate_metrics = pd.DataFrame(metric_rows).sort_values(
        ["pair_id", "pair_member_role", "decoder_id", "forward_holding_sessions"],
        kind="mergesort",
    ).reset_index(drop=True)
    expected_rows = EXPECTED_MEMBER_COUNT * len(DECODERS) * len(FORWARD_HOLDING_SESSIONS)
    if len(candidate_metrics) != expected_rows:
        raise RuntimeError(f"decoder candidate metric cardinality drift: {len(candidate_metrics)}")
    pair_metrics = build_pair_metrics(candidate_metrics)
    rank = build_rank_preservation(candidate_metrics, existing_mtm)

    candidate_metrics_path = output_root / "decoder_candidate_session_metrics.parquet"
    pair_metrics_path = output_root / "decoder_pair_session_metrics.parquet"
    rank_path = output_root / "decoder_rank_preservation.parquet"
    candidate_metrics.to_parquet(candidate_metrics_path, index=False)
    pair_metrics.to_parquet(pair_metrics_path, index=False)
    rank.to_parquet(rank_path, index=False)
    report_path = output_root / "CN_PORTFOLIO_DECODER_AUTOPSY_V1.md"
    report_path.write_text(
        _report(
            selection_sha256=selection_sha256,
            rank=rank,
            pairs=pair_metrics,
            baseline=baseline,
        ),
        encoding="utf-8",
    )
    summary = {
        "schema_version": "cn_portfolio_decoder_autopsy_summary_v1",
        "status": "PORTFOLIO_DECODER_AUTOPSY_COMPLETE_DIAGNOSTIC_ONLY",
        "selection_payload_sha256": selection_sha256,
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "decoder_count": len(DECODERS),
        "holding_session_count": len(FORWARD_HOLDING_SESSIONS),
        "candidate_metric_count": len(candidate_metrics),
        "pair_metric_count": len(pair_metrics),
        "rank_preservation_row_count": len(rank),
        "current_decoder_exact_replay_parity": baseline["status"],
        "horizon_semantics": "STOCK_SESSION_SHIFT_NOT_MINUTE",
        "best_train_rank_preservation": (
            rank.sort_values(
                ["signal_to_gross_spearman", "decoder_id", "forward_holding_sessions"],
                ascending=[False, True, True],
                na_position="last",
            ).head(1).to_dict(orient="records")[0]
            if len(rank)
            else None
        ),
        "search_run": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion_authorized": False,
    }
    summary_path = _write_json(output_root / "decoder_summary.json", summary)
    artifacts = [
        binding_path,
        baseline_path,
        candidate_metrics_path,
        pair_metrics_path,
        rank_path,
        report_path,
        summary_path,
        *sorted(candidate_root.glob("*.json")),
    ]
    closure = {
        "schema_version": SCHEMA_VERSION,
        "status": "CN_PORTFOLIO_DECODER_AUTOPSY_V1_CLOSED_IMMUTABLE_DIAGNOSTIC_ONLY",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection_payload_sha256": selection_sha256,
        "builder_commit_sha": builder_commit_sha,
        "builder_source_sha256": _sha256(Path(__file__).resolve()),
        "input_data_sha256": input_data_sha256,
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "decoder_count": len(DECODERS),
        "holding_sessions": list(FORWARD_HOLDING_SESSIONS),
        "horizon_semantics": "STOCK_SESSION_SHIFT_NOT_MINUTE",
        "current_decoder_exact_replay_parity": baseline["status"],
        "search_run": False,
        "reward_changed": False,
        "evaluator_changed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
        "graph_authority_changed": False,
        "artifacts": [_artifact(path, root=output_root) for path in artifacts],
    }
    closure["manifest_body_sha256"] = _stable_hash(closure)
    closure_path = _write_json(output_root / "DECODER_AUTOPSY_COMPLETE.json", closure)
    return {
        "status": closure["status"],
        "output_root": str(output_root),
        "manifest_path": str(closure_path),
        "manifest_file_sha256": _sha256(closure_path),
        "manifest_body_sha256": closure["manifest_body_sha256"],
        "summary": summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-oos-root", type=Path, required=True)
    parser.add_argument("--mark-to-market-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--builder-commit-sha", required=True)
    parser.add_argument("--worker-count", type=int, default=8)
    parser.add_argument("--expected-selection-payload-sha256")
    args = parser.parse_args()
    result = build_decoder_autopsy(
        replay_oos_root=args.replay_oos_root,
        mark_to_market_root=args.mark_to_market_root,
        output_root=args.output_root,
        builder_commit_sha=args.builder_commit_sha,
        worker_count=args.worker_count,
        expected_selection_payload_sha256=args.expected_selection_payload_sha256,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
