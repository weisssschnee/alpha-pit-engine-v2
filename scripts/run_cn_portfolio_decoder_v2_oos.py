"""Run unchanged-cohort report-only OOS for frozen Decoder V2 finalists.

This entry point deliberately does not share the train-only Decoder V2 run
contract.  It reuses only the exact executable replay kernel and decoder
policy, evaluates every frozen pair intention-to-treat on the validation
sidecar, and emits report-only evidence with no optimizer or promotion write.
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
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
from scripts import freeze_cn_decoder_v2_finalists as freeze
from scripts import run_cn_finalist_replay_then_oos as base
from scripts import run_cn_portfolio_decoder_v2 as train_v2
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


SCHEMA_VERSION = "cn_portfolio_decoder_v2_report_only_oos_v1"
EXPECTED_PAIR_COUNT = 22
EXPECTED_MEMBER_COUNT = 44
DECODER_ID = "TOPK_10_EQUAL"
AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
EVIDENCE_SCOPE = "ADAPTIVE_REPORT_ONLY_VALIDATION_OOS"
POLICY = ASharePortfolioDecoderPolicy(
    decoder_id=DECODER_ID,
    selection="TOP_K",
    top_k=10,
    weighting="EQUAL",
)

_PROCESS_CONTEXT: dict[str, Any] | None = None
_PROCESS_CANDIDATE_ROOT: Path | None = None
_PROCESS_INPUT_HASH: str | None = None


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _artifact_path(root: Path, artifacts: list[Mapping[str, Any]], name: str) -> Path:
    matches = [row for row in artifacts if Path(str(row["path"])).name == name]
    if len(matches) != 1:
        raise RuntimeError(f"frozen artifact cardinality drift for {name}")
    path = (root / str(matches[0]["path"])).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.stat().st_size != int(matches[0]["bytes"]):
        raise RuntimeError(f"frozen artifact size drift: {path}")
    if v1._sha256(path) != str(matches[0]["sha256"]):
        raise RuntimeError(f"frozen artifact hash drift: {path}")
    return path


def _load_finalists(
    *,
    finalist_root: Path,
    expected_selection_payload_sha256: str,
    expected_decoder_policy_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any], pd.DataFrame, pd.DataFrame]:
    finalist_root = Path(finalist_root).resolve()
    manifest_path = finalist_root / "finalist_manifest.json"
    manifest = freeze._read_json(manifest_path)
    freeze._verify_self_hash(
        manifest,
        field="manifest_payload_sha256",
        label="Decoder V2 finalist manifest",
    )
    freeze._verify_artifacts(
        manifest,
        root=finalist_root,
        label="Decoder V2 finalist manifest",
    )
    required = {
        "status": "DECODER_V2_TRAIN_ONLY_FINALIST_FREEZE_CLOSED_IMMUTABLE",
        "decoder_id": DECODER_ID,
        "selection_payload_sha256": expected_selection_payload_sha256,
        "selected_pairs": EXPECTED_PAIR_COUNT,
        "selected_candidate_members": EXPECTED_MEMBER_COUNT,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    drift = [key for key, value in required.items() if manifest.get(key) != value]
    if drift:
        raise RuntimeError("finalist manifest drift: " + ",".join(drift))
    if str(manifest["decoder_contract_payload_sha256"]) != (
        expected_decoder_policy_sha256
    ):
        raise RuntimeError("finalist decoder policy hash drift")

    artifacts = list(manifest.get("artifacts") or ())
    contract_path = _artifact_path(finalist_root, artifacts, "finalist_contract.json")
    pair_path = _artifact_path(finalist_root, artifacts, "finalist_pairs.parquet")
    candidate_path = _artifact_path(
        finalist_root, artifacts, "finalist_candidates.parquet"
    )
    contract = freeze._read_json(contract_path)
    freeze._verify_self_hash(
        contract,
        field="contract_payload_sha256",
        label="Decoder V2 finalist contract",
    )
    if str(contract.get("decoder_id") or "") != DECODER_ID:
        raise RuntimeError("finalist contract decoder drift")
    if str(contract.get("decoder_contract_payload_sha256") or "") != (
        expected_decoder_policy_sha256
    ):
        raise RuntimeError("finalist contract policy hash drift")
    boundary = dict(contract.get("authority_boundary") or {})
    if any(
        int(boundary.get(key) or 0) != 0
        for key in ("validation_reads", "holdout_reads", "forward_2026_reads")
    ):
        raise RuntimeError("finalist freeze crossed sealed boundary")

    pairs = pd.read_parquet(pair_path).where(pd.notna, None)
    candidates = pd.read_parquet(candidate_path).where(pd.notna, None)
    if len(pairs) != EXPECTED_PAIR_COUNT or len(candidates) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError("frozen finalist cardinality drift")
    if pairs["finalist_order"].astype(int).tolist() != list(
        range(1, EXPECTED_PAIR_COUNT + 1)
    ):
        raise RuntimeError("frozen finalist pair order drift")
    expected_candidate_ids: list[str] = []
    for row in pairs.to_dict(orient="records"):
        expected_candidate_ids.extend(
            [str(row["primary_candidate_id"]), str(row["control_candidate_id"])]
        )
    if candidates["candidate_id"].astype(str).tolist() != expected_candidate_ids:
        raise RuntimeError("frozen finalist member order drift")
    if candidates["pair_id"].astype(str).duplicated().sum() != EXPECTED_PAIR_COUNT:
        raise RuntimeError("frozen finalist pair membership drift")
    return manifest, contract, pairs, candidates


def _load_validation_context(
    *,
    source_contract_path: Path,
    validation_field_root: Path,
    validation_label_root: Path,
    validation_session_authority_root: Path,
) -> dict[str, Any]:
    contract = v1._read_json(source_contract_path)
    base._verify_payload_hash(
        contract,
        field="contract_payload_sha256",
        label="source replay/OOS execution contract",
    )
    split_hash = str(contract["split_manifest_sha256"])
    field_manifest, field_manifest_path = base._validate_sidecar(
        validation_field_root,
        evaluation_role="validation",
        split_hash=split_hash,
    )
    label_manifest, label_manifest_path = base._validate_label_sidecar(
        validation_label_root,
        split_hash=split_hash,
    )
    field_frame = base._load_field_frame(validation_field_root, field_manifest)
    if not field_frame["trade_time"].dt.strftime("%H:%M:%S").eq(
        "15:00:00"
    ).all():
        raise RuntimeError("validation sidecar contains intraday clocks")
    validation_dates = set(pd.to_datetime(field_frame["date"]).dt.normalize())
    if len(validation_dates) != int(field_manifest["eligible_validation_date_count"]):
        raise RuntimeError("validation sidecar calendar count drift")

    session_manifest_path = (
        validation_session_authority_root.resolve()
        / "validation_session_authority_manifest.json"
    )
    session_manifest = v1._read_json(session_manifest_path)
    session_manifest_body = dict(session_manifest)
    declared_session_hash = str(
        session_manifest_body.pop("manifest_payload_sha256", "")
    )
    if (
        not declared_session_hash
        or v1._stable_hash(session_manifest_body) != declared_session_hash
    ):
        raise RuntimeError("validation session authority self-hash drift")
    required_session = {
        "status": "VALIDATION_SESSION_AUTHORITY_CLOSED_IMMUTABLE",
        "evaluation_role": "validation",
        "data_role": "validation_report_only",
        "field_manifest_sha256": v1._sha256(field_manifest_path),
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    session_drift = [
        key
        for key, value in required_session.items()
        if session_manifest.get(key) != value
    ]
    if session_drift:
        raise RuntimeError(
            "validation session authority drift: " + ",".join(session_drift)
        )
    for artifact in session_manifest.get("artifacts") or ():
        artifact_path = validation_session_authority_root / str(artifact["path"])
        if (
            not artifact_path.is_file()
            or artifact_path.stat().st_size != int(artifact["bytes"])
            or v1._sha256(artifact_path) != str(artifact["sha256"])
        ):
            raise RuntimeError(
                f"validation session authority artifact drift: {artifact_path}"
            )
    session_path = (
        validation_session_authority_root.resolve()
        / "validation_session_authority.parquet"
    )
    session_authority = pd.read_parquet(session_path)
    session_dates = pd.to_datetime(session_authority["date"], errors="raise").dt.normalize()
    session_authority = session_authority[session_dates.isin(validation_dates)].copy()
    if set(pd.to_datetime(session_authority["date"]).dt.normalize()) != validation_dates:
        raise RuntimeError("validation session authority calendar drift")
    master, observed_index, authority_index = base._materialize_replay_master(
        field_frame, session_authority
    )

    universe_raw = dict(contract["universe_policy"])
    universe_raw["allowed_exchanges"] = tuple(universe_raw["allowed_exchanges"])
    universe = AShareUniversePolicy(**universe_raw)
    fee = AShareFeeSchedule(**dict(contract["fee_schedule"]))
    execution = AShareExecutionPolicy(**dict(contract["execution_policy"]))
    corporate = AShareCorporateActionPolicy(
        **dict(contract["corporate_action_policy"])
    )
    prepared_master = _prepare_sessions(
        master.assign(signal=0.0), universe_policy=universe
    ).drop(columns=["signal"])
    prepared_index = pd.MultiIndex.from_frame(prepared_master[["date", "code"]])
    if set(prepared_index) != set(authority_index):
        raise RuntimeError("validation prepared/session coordinate drift")
    future_returns = v1.forward_open_to_close_returns(prepared_master, horizons=(1,))
    codes = prepared_master["code"].astype(str).to_numpy()
    dates = pd.to_datetime(prepared_master["date"]).to_numpy()
    eligible = prepared_master["promotion_universe_eligible"].astype(bool).to_numpy()
    date_order = np.argsort(dates, kind="mergesort")
    sorted_dates = dates[date_order]
    boundaries = np.flatnonzero(
        np.r_[True, sorted_dates[1:] != sorted_dates[:-1], True]
    )
    date_groups = [
        date_order[start:end]
        for start, end in zip(boundaries[:-1], boundaries[1:])
    ]
    validation_field_reads = int(field_manifest["validation_reads"])
    validation_authority_reads = int(
        session_manifest["authority_session_row_count"]
    )
    return {
        "contract": contract,
        "field_manifest": field_manifest,
        "label_manifest": label_manifest,
        "field_manifest_path": field_manifest_path,
        "label_manifest_path": label_manifest_path,
        "session_manifest_path": session_manifest_path,
        "session_manifest": session_manifest,
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
        "validation_field_reads": validation_field_reads,
        "validation_authority_reads": validation_authority_reads,
        "validation_reads": validation_field_reads + validation_authority_reads,
    }


def _daily_distribution(result: Mapping[str, Any]) -> dict[str, Any]:
    daily = result["daily"].copy()
    returns = pd.to_numeric(daily["daily_net_return"], errors="coerce").dropna()
    dated = pd.DataFrame(
        {
            "date": pd.to_datetime(daily["date"], errors="raise"),
            "return": pd.to_numeric(daily["daily_net_return"], errors="coerce"),
        }
    ).dropna()
    regimes = (
        dated.assign(regime=dated["date"].dt.to_period("Q").astype(str))
        .groupby("regime", sort=True)["return"]
        .apply(lambda values: float((1.0 + values).prod() - 1.0))
    )
    return {
        "daily_net_return_median": _finite(returns.median()),
        "daily_net_return_p10": _finite(returns.quantile(0.10)),
        "daily_net_return_worst": _finite(returns.min()),
        "daily_net_return_positive_share": _finite(returns.gt(0).mean()),
        "quarterly_regime_count": int(len(regimes)),
        "quarterly_regime_positive_share": _finite(regimes.gt(0).mean()),
        "quarterly_regime_worst_return": _finite(regimes.min()),
    }


def _candidate_metric(
    *,
    candidate: Mapping[str, Any],
    result: Mapping[str, Any],
    signal_rank_ic_mean: float | None,
    theoretical_gross_mean: float | None,
) -> dict[str, Any]:
    metric = train_v2._candidate_metric(
        candidate=candidate,
        decoder=POLICY,
        result=result,
        signal_rank_ic_mean=signal_rank_ic_mean,
        theoretical_gross_mean=theoretical_gross_mean,
    )
    metric.update(_daily_distribution(result))
    metric.update(
        {
            "evaluation_role": "validation",
            "data_role": "validation_report_only",
            "evidence_scope": EVIDENCE_SCOPE,
            "validation_reads": int(result["validation_reads"]),
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "optimizer_feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "promotion_authorized": False,
        }
    )
    return metric


def _evaluate_candidate(
    candidate: Mapping[str, Any],
    *,
    context: Mapping[str, Any],
    candidate_root: Path,
    input_data_sha256: str,
) -> dict[str, Any]:
    candidate_id = str(candidate["candidate_id"])
    signal_field = pd.to_numeric(
        base.evaluate_panel_expression(
            context["field_frame"],
            str(candidate["expression"]),
            cache={},
            data_role="validation",
        ),
        errors="coerce",
    )
    signal_series = pd.Series(
        signal_field.to_numpy(), index=context["observed_index"]
    )
    aligned_signal = signal_series.reindex(context["prepared_index"]).to_numpy(
        dtype=float
    )
    theoretical = v1.evaluate_candidate_decoder_matrix(
        candidate=candidate,
        signal=aligned_signal,
        codes=context["codes"],
        dates=context["dates"],
        eligible=context["eligible"],
        future_returns=context["future_returns"],
        date_groups=context["date_groups"],
        decoders=(
            {
                "decoder_id": POLICY.decoder_id,
                "selection": POLICY.selection,
                "weighting": POLICY.weighting,
                "top_k": POLICY.top_k,
            },
        ),
    )[0]
    replay_frame = context["master"].copy()
    replay_frame["signal"] = signal_series.reindex(
        context["authority_index"]
    ).to_numpy()
    result = run_a_share_long_only_replay(
        replay_frame,
        fee_schedule=context["fee"],
        universe_policy=context["universe"],
        execution_policy=context["execution"],
        corporate_action_policy=context["corporate"],
        portfolio_decoder_policy=POLICY,
        ending_book_policy=ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    )
    if str(result["portfolio_decoder_policy_sha256"]) != POLICY.payload_sha256:
        raise RuntimeError("OOS decoder policy hash drift")
    result = dict(result)
    result["validation_reads"] = int(context["validation_reads"])
    metric = _candidate_metric(
        candidate=candidate,
        result=result,
        signal_rank_ic_mean=_finite(theoretical["signal_rank_ic_mean"]),
        theoretical_gross_mean=_finite(theoretical["gross_forward_return_mean"]),
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": "DECODER_V2_REPORT_ONLY_OOS_CANDIDATE_CLOSED_IMMUTABLE",
        "candidate_id": candidate_id,
        "input_data_sha256": input_data_sha256,
        "metric": metric,
    }
    payload["payload_sha256"] = v1._stable_hash(payload)
    v1._write_json(candidate_root / f"{candidate_id}.json", payload)
    return metric


def _initialize_worker(
    source_contract_path: str,
    validation_field_root: str,
    validation_label_root: str,
    validation_session_authority_root: str,
    candidate_root: str,
    input_data_sha256: str,
) -> None:
    global _PROCESS_CONTEXT, _PROCESS_CANDIDATE_ROOT, _PROCESS_INPUT_HASH
    _PROCESS_CONTEXT = _load_validation_context(
        source_contract_path=Path(source_contract_path),
        validation_field_root=Path(validation_field_root),
        validation_label_root=Path(validation_label_root),
        validation_session_authority_root=Path(
            validation_session_authority_root
        ),
    )
    _PROCESS_CANDIDATE_ROOT = Path(candidate_root)
    _PROCESS_INPUT_HASH = str(input_data_sha256)


def _evaluate_in_worker(candidate: Mapping[str, Any]) -> dict[str, Any]:
    if (
        _PROCESS_CONTEXT is None
        or _PROCESS_CANDIDATE_ROOT is None
        or _PROCESS_INPUT_HASH is None
    ):
        raise RuntimeError("OOS worker is not initialized")
    return _evaluate_candidate(
        candidate,
        context=_PROCESS_CONTEXT,
        candidate_root=_PROCESS_CANDIDATE_ROOT,
        input_data_sha256=_PROCESS_INPUT_HASH,
    )


def _pair_metrics(
    *, pairs: pd.DataFrame, candidate_metrics: pd.DataFrame
) -> pd.DataFrame:
    by_candidate = {
        str(row["candidate_id"]): row
        for row in candidate_metrics.to_dict(orient="records")
    }
    rows: list[dict[str, Any]] = []
    for source in pairs.to_dict(orient="records"):
        primary = by_candidate[str(source["primary_candidate_id"])]
        control = by_candidate[str(source["control_candidate_id"])]
        primary_reward = float(primary["continuous_book_net_reward"])
        control_reward = float(control["continuous_book_net_reward"])
        primary_return = float(primary["cumulative_net_return"])
        control_return = float(control["cumulative_net_return"])
        reward_increment = primary_reward - control_reward
        return_increment = primary_return - control_return
        rows.append(
            {
                "finalist_order": int(source["finalist_order"]),
                "pair_id": str(source["pair_id"]),
                "primary_candidate_id": str(source["primary_candidate_id"]),
                "control_candidate_id": str(source["control_candidate_id"]),
                "decoder_id": DECODER_ID,
                "decoder_policy_sha256": POLICY.payload_sha256,
                "train_search_score": source.get("search_score"),
                "train_rank_ic_mean": source.get("train_rank_ic_mean"),
                "train_decoder_primary_cumulative_net_return": source.get(
                    "primary_cumulative_net_return"
                ),
                "train_decoder_matched_cumulative_net_return_increment": (
                    source.get("matched_cumulative_net_return_increment")
                ),
                "primary_continuous_book_net_reward": primary_reward,
                "control_continuous_book_net_reward": control_reward,
                "matched_continuous_book_net_reward_increment": reward_increment,
                "primary_cumulative_net_return": primary_return,
                "control_cumulative_net_return": control_return,
                "matched_cumulative_net_return_increment": return_increment,
                "primary_mean_one_way_turnover": primary["mean_one_way_turnover"],
                "primary_net_return_per_turnover": primary[
                    "net_return_per_turnover"
                ],
                "primary_total_fees_cny": primary["total_fees_cny"],
                "primary_cumulative_realized_trade_pnl_cny": primary[
                    "cumulative_realized_trade_pnl_cny"
                ],
                "primary_ending_unrealized_pnl_cny": primary[
                    "ending_unrealized_pnl_cny"
                ],
                "primary_ending_holdings_weight": primary[
                    "ending_holdings_weight"
                ],
                "primary_daily_net_return_p10": primary[
                    "daily_net_return_p10"
                ],
                "primary_daily_net_return_worst": primary[
                    "daily_net_return_worst"
                ],
                "primary_quarterly_regime_positive_share": primary[
                    "quarterly_regime_positive_share"
                ],
                "primary_quarterly_regime_worst_return": primary[
                    "quarterly_regime_worst_return"
                ],
                "primary_signal_rank_ic_mean": primary["signal_rank_ic_mean"],
                "absolute_reward_positive": primary_reward > 0.0,
                "matched_reward_increment_positive": reward_increment > 0.0,
                "absolute_return_positive": primary_return > 0.0,
                "matched_return_increment_positive": return_increment > 0.0,
                "all_four_economic_gates_positive": (
                    primary_reward > 0.0
                    and reward_increment > 0.0
                    and primary_return > 0.0
                    and return_increment > 0.0
                ),
                "interstage_filter_applied": False,
                "evidence_scope": EVIDENCE_SCOPE,
                "promotion_authorized": False,
            }
        )
    return pd.DataFrame(rows)


def _spearman(frame: pd.DataFrame, left: str, right: str) -> float | None:
    pair = frame[[left, right]].apply(pd.to_numeric, errors="coerce").dropna()
    if len(pair) < 2:
        return None
    return _finite(pair[left].corr(pair[right], method="spearman"))


def _top_retention(
    frame: pd.DataFrame, left: str, right: str, *, count: int = 5
) -> int:
    usable = frame[["pair_id", left, right]].copy()
    left_top = set(
        usable.dropna(subset=[left])
        .sort_values([left, "pair_id"], ascending=[False, True], kind="mergesort")
        .head(count)["pair_id"]
        .astype(str)
    )
    right_top = set(
        usable.dropna(subset=[right])
        .sort_values([right, "pair_id"], ascending=[False, True], kind="mergesort")
        .head(count)["pair_id"]
        .astype(str)
    )
    return len(left_top.intersection(right_top))


def run_oos(
    *,
    finalist_root: Path,
    source_replay_root: Path,
    validation_session_authority_root: Path,
    output_root: Path,
    builder_commit_sha: str,
    expected_selection_payload_sha256: str,
    expected_decoder_policy_sha256: str,
    worker_count: int,
    executor_worker_count: int,
) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(f"Decoder V2 OOS must run on {AUTHORIZED_HOST}")
    initial_free = int(psutil.virtual_memory().available)
    if initial_free < MINIMUM_FREE_MEMORY_BYTES:
        raise RuntimeError("Decoder V2 OOS minimum-free-memory gate failed")
    train_v2._validate_executor_contract(
        execution_backend="PROCESS_POOL",
        entitlement_worker_count=int(worker_count),
        executor_worker_count=int(executor_worker_count),
    )
    if os.environ.get("CN_NODE_RESOURCE_LEASE_REQUIRED") == "1":
        if int(os.environ.get("CN_NODE_CPU_ENTITLEMENT") or 0) != int(worker_count):
            raise RuntimeError("Decoder V2 OOS lease entitlement drift")
        nested = {
            key: int(os.environ.get(key) or 0)
            for key in (
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
            raise RuntimeError(f"Decoder V2 OOS nested parallelism drift: {nested}")
    if len(builder_commit_sha) != 40:
        raise ValueError("builder_commit_sha must be a full Git SHA")

    finalist_root = Path(finalist_root).resolve()
    source_replay_root = Path(source_replay_root).resolve()
    validation_session_authority_root = Path(
        validation_session_authority_root
    ).resolve()
    output_root = Path(output_root).resolve()
    manifest, finalist_contract, pairs, candidates = _load_finalists(
        finalist_root=finalist_root,
        expected_selection_payload_sha256=expected_selection_payload_sha256,
        expected_decoder_policy_sha256=expected_decoder_policy_sha256,
    )
    if POLICY.payload_sha256 != expected_decoder_policy_sha256:
        raise RuntimeError("runtime Decoder V2 policy hash drift")
    source_contract_path = (
        source_replay_root / "prepared" / "replay_then_oos_execution_contract.json"
    )
    validation_field_root = source_replay_root / "sidecars" / "validation_session_fields"
    validation_label_root = source_replay_root / "sidecars" / "validation_session_labels"
    context = _load_validation_context(
        source_contract_path=source_contract_path,
        validation_field_root=validation_field_root,
        validation_label_root=validation_label_root,
        validation_session_authority_root=validation_session_authority_root,
    )
    field_manifest_path = context["field_manifest_path"]
    label_manifest_path = context["label_manifest_path"]
    session_manifest_path = context["session_manifest_path"]
    session_path = context["session_path"]
    validation_field_reads = int(context["validation_field_reads"])
    validation_authority_reads = int(context["validation_authority_reads"])
    validation_reads = int(context["validation_reads"])
    if validation_reads <= 0:
        raise RuntimeError("Decoder V2 OOS has no validation reads")

    input_binding = {
        "schema_version": SCHEMA_VERSION,
        "selection_payload_sha256": expected_selection_payload_sha256,
        "finalist_manifest_sha256": v1._sha256(
            finalist_root / "finalist_manifest.json"
        ),
        "finalist_contract_payload_sha256": finalist_contract[
            "contract_payload_sha256"
        ],
        "source_execution_contract_sha256": v1._sha256(source_contract_path),
        "validation_field_manifest_sha256": v1._sha256(field_manifest_path),
        "validation_label_manifest_sha256": v1._sha256(label_manifest_path),
        "session_authority_manifest_sha256": v1._sha256(session_manifest_path),
        "session_authority_sha256": v1._sha256(session_path),
        "validation_session_authority_root": str(
            validation_session_authority_root
        ),
        "decoder_contract": {
            **asdict(POLICY),
            "payload_sha256": POLICY.payload_sha256,
        },
        "pair_ids": pairs["pair_id"].astype(str).tolist(),
        "candidate_ids": candidates["candidate_id"].astype(str).tolist(),
        "evaluation_role": "validation",
        "data_role": "validation_report_only",
        "evidence_scope": EVIDENCE_SCOPE,
        "interstage_filter_applied": False,
        "train_recomputed": False,
        "worker_count": int(worker_count),
        "execution_backend": "PROCESS_POOL",
        "executor_worker_count": int(executor_worker_count),
        "native_threads_per_executor_worker": 1,
        "builder_commit_sha": builder_commit_sha,
        "builder_source_sha256": v1._sha256(Path(__file__).resolve()),
        "validation_reads": validation_reads,
        "validation_field_reads": validation_field_reads,
        "validation_authority_reads": validation_authority_reads,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    input_data_sha256 = v1._stable_hash(input_binding)
    output_root.mkdir(parents=True, exist_ok=True)
    binding_path = v1._write_json(output_root / "input_binding.json", input_binding)
    candidate_root = output_root / "candidates"
    candidate_root.mkdir(exist_ok=True)

    del context
    gc.collect()
    records = candidates.to_dict(orient="records")
    metrics: list[dict[str, Any]] = []
    minimum_free = initial_free
    maximum_tree_rss = 0
    cpu_samples: list[float] = []
    started = time.perf_counter()
    with ProcessPoolExecutor(
        max_workers=int(executor_worker_count),
        initializer=_initialize_worker,
        initargs=(
            str(source_contract_path),
            str(validation_field_root),
            str(validation_label_root),
            str(validation_session_authority_root),
            str(candidate_root),
            input_data_sha256,
        ),
    ) as executor:
        pending = {executor.submit(_evaluate_in_worker, row) for row in records}
        while pending:
            completed, pending = wait(
                pending, timeout=5.0, return_when=FIRST_COMPLETED
            )
            cpu_samples.append(float(psutil.cpu_percent(interval=None)))
            available, _, tree_rss = train_v2._resource_snapshot()
            minimum_free = min(minimum_free, int(available))
            maximum_tree_rss = max(maximum_tree_rss, int(tree_rss))
            if minimum_free < MINIMUM_FREE_MEMORY_BYTES:
                for future in pending:
                    future.cancel()
                raise RuntimeError("Decoder V2 OOS runtime memory gate failed")
            for future in completed:
                metrics.append(future.result())
    elapsed = float(time.perf_counter() - started)
    candidate_metrics = pd.DataFrame(metrics)
    order = {
        candidate_id: index
        for index, candidate_id in enumerate(
            candidates["candidate_id"].astype(str).tolist()
        )
    }
    candidate_metrics["_order"] = candidate_metrics["candidate_id"].map(order)
    candidate_metrics = candidate_metrics.sort_values(
        "_order", kind="mergesort"
    ).drop(columns="_order").reset_index(drop=True)
    if candidate_metrics["candidate_id"].astype(str).tolist() != candidates[
        "candidate_id"
    ].astype(str).tolist():
        raise RuntimeError("OOS candidate metric order drift")
    if len(candidate_metrics) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError("OOS candidate metric count drift")
    if not candidate_metrics["accounting_invariants_status"].eq("PASS").all():
        raise RuntimeError("OOS accounting invariants failed")
    pair_metrics = _pair_metrics(pairs=pairs, candidate_metrics=candidate_metrics)
    if pair_metrics["pair_id"].astype(str).tolist() != pairs["pair_id"].astype(
        str
    ).tolist():
        raise RuntimeError("OOS pair metric order drift")

    candidate_path = output_root / "oos_candidate_metrics.parquet"
    pair_path = output_root / "oos_pair_metrics.parquet"
    candidate_metrics.to_parquet(candidate_path, index=False)
    pair_metrics.to_parquet(pair_path, index=False)
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "DECODER_V2_TOPK10_EQUAL_REPORT_ONLY_OOS_COMPLETE",
        "evidence_scope": EVIDENCE_SCOPE,
        "selection_payload_sha256": expected_selection_payload_sha256,
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "decoder_id": DECODER_ID,
        "decoder_policy_sha256": POLICY.payload_sha256,
        "interstage_filter_applied": False,
        "train_recomputed": False,
        "primary_reward_positive_count": int(
            pair_metrics["absolute_reward_positive"].sum()
        ),
        "matched_reward_increment_positive_count": int(
            pair_metrics["matched_reward_increment_positive"].sum()
        ),
        "primary_return_positive_count": int(
            pair_metrics["absolute_return_positive"].sum()
        ),
        "matched_return_increment_positive_count": int(
            pair_metrics["matched_return_increment_positive"].sum()
        ),
        "all_four_economic_gates_positive_count": int(
            pair_metrics["all_four_economic_gates_positive"].sum()
        ),
        "primary_cumulative_return_median": _finite(
            pair_metrics["primary_cumulative_net_return"].median()
        ),
        "primary_cumulative_return_p10": _finite(
            pair_metrics["primary_cumulative_net_return"].quantile(0.10)
        ),
        "matched_cumulative_return_increment_median": _finite(
            pair_metrics["matched_cumulative_net_return_increment"].median()
        ),
        "matched_cumulative_return_increment_p10": _finite(
            pair_metrics["matched_cumulative_net_return_increment"].quantile(
                0.10
            )
        ),
        "primary_reward_median": _finite(
            pair_metrics["primary_continuous_book_net_reward"].median()
        ),
        "primary_reward_p10": _finite(
            pair_metrics["primary_continuous_book_net_reward"].quantile(0.10)
        ),
        "matched_reward_increment_median": _finite(
            pair_metrics[
                "matched_continuous_book_net_reward_increment"
            ].median()
        ),
        "matched_reward_increment_p10": _finite(
            pair_metrics[
                "matched_continuous_book_net_reward_increment"
            ].quantile(0.10)
        ),
        "primary_turnover_median": _finite(
            pair_metrics["primary_mean_one_way_turnover"].median()
        ),
        "primary_net_return_per_turnover_median": _finite(
            pair_metrics["primary_net_return_per_turnover"].median()
        ),
        "primary_daily_left_tail_p10_median": _finite(
            pair_metrics["primary_daily_net_return_p10"].median()
        ),
        "primary_worst_daily_return": _finite(
            pair_metrics["primary_daily_net_return_worst"].min()
        ),
        "primary_quarterly_regime_positive_share_median": _finite(
            pair_metrics[
                "primary_quarterly_regime_positive_share"
            ].median()
        ),
        "signal_to_oos_net_return_spearman": _spearman(
            pair_metrics,
            "primary_signal_rank_ic_mean",
            "primary_cumulative_net_return",
        ),
        "train_signal_to_oos_net_return_spearman": _spearman(
            pair_metrics,
            "train_rank_ic_mean",
            "primary_cumulative_net_return",
        ),
        "train_search_score_to_oos_net_return_spearman": _spearman(
            pair_metrics,
            "train_search_score",
            "primary_cumulative_net_return",
        ),
        "train_decoder_to_oos_net_return_spearman": _spearman(
            pair_metrics,
            "train_decoder_primary_cumulative_net_return",
            "primary_cumulative_net_return",
        ),
        "train_signal_to_oos_top5_retention": _top_retention(
            pair_metrics,
            "train_rank_ic_mean",
            "primary_cumulative_net_return",
        ),
        "train_decoder_to_oos_top5_retention": _top_retention(
            pair_metrics,
            "train_decoder_primary_cumulative_net_return",
            "primary_cumulative_net_return",
        ),
        "elapsed_seconds": elapsed,
        "pairs_per_hour": float(EXPECTED_PAIR_COUNT * 3600.0 / elapsed),
        "worker_count": int(worker_count),
        "execution_backend": "PROCESS_POOL",
        "executor_worker_count": int(executor_worker_count),
        "minimum_free_memory_bytes": int(minimum_free),
        "maximum_process_tree_rss_bytes": int(maximum_tree_rss),
        "host_cpu_mean_percent": _finite(np.mean(cpu_samples)),
        "validation_reads": validation_reads,
        "validation_field_reads": validation_field_reads,
        "validation_authority_reads": validation_authority_reads,
        "validation_session_authority_manifest_sha256": v1._sha256(
            session_manifest_path
        ),
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "promotion_authorized": False,
    }
    report_path = v1._write_json(output_root / "oos_report.json", report)
    candidate_files = sorted(candidate_root.glob("*.json"))
    artifacts = [
        v1._artifact(path, root=output_root)
        for path in (
            binding_path,
            candidate_path,
            pair_path,
            report_path,
            *candidate_files,
        )
    ]
    closure = {
        **report,
        "status": "DECODER_V2_TOPK10_EQUAL_REPORT_ONLY_OOS_CLOSED_IMMUTABLE",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "accounting_invariants_status": "PASS",
        "artifacts": artifacts,
    }
    closure["manifest_body_sha256"] = v1._stable_hash(closure)
    closure_path = v1._write_json(output_root / "OOS_COMPLETE.json", closure)
    return {
        **report,
        "closure_path": str(closure_path),
        "closure_sha256": v1._sha256(closure_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--finalist-root", type=Path, required=True)
    parser.add_argument("--source-replay-root", type=Path, required=True)
    parser.add_argument(
        "--validation-session-authority-root", type=Path, required=True
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--builder-commit-sha", required=True)
    parser.add_argument("--expected-selection-payload-sha256", required=True)
    parser.add_argument("--expected-decoder-policy-sha256", required=True)
    parser.add_argument("--worker-count", type=int, default=32)
    parser.add_argument("--executor-worker-count", type=int, default=12)
    args = parser.parse_args()
    result = run_oos(
        finalist_root=args.finalist_root,
        source_replay_root=args.source_replay_root,
        validation_session_authority_root=(
            args.validation_session_authority_root
        ),
        output_root=args.output_root,
        builder_commit_sha=args.builder_commit_sha,
        expected_selection_payload_sha256=(
            args.expected_selection_payload_sha256
        ),
        expected_decoder_policy_sha256=args.expected_decoder_policy_sha256,
        worker_count=args.worker_count,
        executor_worker_count=args.executor_worker_count,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
