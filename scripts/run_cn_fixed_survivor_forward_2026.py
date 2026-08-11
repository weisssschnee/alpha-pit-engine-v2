"""One-shot 2026 forward confirmation for the immutable fixed ten-pair cohort."""

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

from scripts import build_cn_forward_2026_session_authority as forward_authority
from scripts import build_cn_portfolio_decoder_autopsy_v1 as v1
from scripts import freeze_cn_decoder_v2_finalists as freeze
from scripts import run_cn_finalist_replay_then_oos as base
from scripts import run_cn_portfolio_decoder_v2 as train_v2
from scripts import run_cn_portfolio_decoder_v2_oos as oos
from our_system_phase2.services.a_share_executable_replay import (
    AShareCorporateActionPolicy,
    AShareExecutionPolicy,
    AShareFeeSchedule,
    AShareUniversePolicy,
    ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    _prepare_sessions,
    run_a_share_long_only_replay,
)
from our_system_phase2.services.evaluation_asset_authority import (
    verify_historical_challenge_destructive_use,
)


EXPECTED_PAIR_COUNT = 10
EXPECTED_MEMBER_COUNT = 20
AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
RUN_MODE = os.environ.get("CN_FIXED10_RUN_MODE", "forward_2026")
if RUN_MODE == "historical_challenge":
    SCHEMA_VERSION = "cn_fixed10_historical_challenge_2023_v1"
    STATUS = "FIXED10_HISTORICAL_CHALLENGE_2023_CLOSED_IMMUTABLE"
    REPORT_STATUS = "FIXED10_HISTORICAL_CHALLENGE_2023_COMPLETE"
    CANDIDATE_STATUS = "HISTORICAL_CHALLENGE_CANDIDATE_CLOSED_IMMUTABLE"
    EXPECTED_DATE_COUNT: int | None = None
    EVALUATION_ROLE = "historical_challenge"
    DATA_ROLE = "historical_challenge_report_only"
    EVIDENCE_SCOPE = "ONE_SHOT_BACKWARD_OOT_2023_FIXED10_REPORT_ONLY"
    SESSION_SCHEMA_VERSION = "cn_historical_challenge_session_authority_v1"
    SESSION_STATUS = "HISTORICAL_CHALLENGE_SESSION_AUTHORITY_COMPLETE"
    READS_FIELD = "historical_challenge_reads"
    DATE_COUNT_FIELD = "historical_challenge_date_count"
    ARTIFACT_PREFIX = "historical_challenge_2023"
    COMPLETE_FILENAME = "HISTORICAL_CHALLENGE_2023_COMPLETE.json"
    ACCESS_STATE = "SPENT_BY_THIS_ONE_SHOT_HISTORICAL_CHALLENGE"
    PROJECT_STATE_AFTER_RUN = "HOLD_RESEARCH_WEAK_BACKWARD_OOT_EVIDENCE"
elif RUN_MODE == "forward_2026":
    SCHEMA_VERSION = "cn_fixed10_forward_2026_confirmation_v1"
    STATUS = "FIXED10_FORWARD_2026_CONFIRMATION_CLOSED_IMMUTABLE"
    REPORT_STATUS = "FIXED10_FORWARD_2026_CONFIRMATION_COMPLETE"
    CANDIDATE_STATUS = "FORWARD_2026_CANDIDATE_CLOSED_IMMUTABLE"
    EXPECTED_DATE_COUNT = 63
    EVALUATION_ROLE = "forward_2026"
    DATA_ROLE = "forward_2026_report_only"
    EVIDENCE_SCOPE = "ONE_SHOT_FORWARD_2026_CONFIRMATION_REPORT_ONLY"
    SESSION_SCHEMA_VERSION = forward_authority.SCHEMA_VERSION
    SESSION_STATUS = forward_authority.STATUS
    READS_FIELD = "forward_2026_reads"
    DATE_COUNT_FIELD = "forward_2026_date_count"
    ARTIFACT_PREFIX = "forward_2026"
    COMPLETE_FILENAME = "FORWARD_2026_COMPLETE.json"
    ACCESS_STATE = "SPENT_BY_THIS_ONE_SHOT_RUN"
    PROJECT_STATE_AFTER_RUN = "HOLD_PROMOTION_PENDING_EXPLICIT_DECISION"
else:
    raise RuntimeError(f"unsupported fixed-ten run mode: {RUN_MODE}")
POLICY = oos.POLICY
HISTORICAL_STAMP_DUTY_SOURCE_BEFORE_REDUCTION = (
    "https://tianjin.chinatax.gov.cn/11200000000/0300/030005/"
    "p20220725150235434.shtml"
)
HISTORICAL_STAMP_DUTY_SOURCE_REDUCTION = (
    "https://fgk.chinatax.gov.cn/zcfgk/c102416/c5211343/content.html"
)

_PROCESS_CONTEXT: dict[str, Any] | None = None
_PROCESS_CANDIDATE_ROOT: Path | None = None
_PROCESS_INPUT_HASH: str | None = None


def _fee_schedule_for_run(
    raw_schedule: Mapping[str, Any],
    *,
    evaluation_dates: set[pd.Timestamp],
) -> tuple[AShareFeeSchedule, str]:
    raw = dict(raw_schedule)
    if RUN_MODE != "historical_challenge":
        return AShareFeeSchedule(**raw), "FROZEN_SINGLE_PERIOD"

    if not evaluation_dates:
        raise RuntimeError("historical challenge fee authority has no dates")
    date_min = min(evaluation_dates)
    date_max = max(evaluation_dates)
    if date_min.year != 2023 or date_max.year != 2023:
        raise RuntimeError("historical challenge fee authority year drift")
    if (
        str(raw.get("effective_start")) != "2023-08-28"
        or float(raw.get("sell_stamp_duty_bps", math.nan)) != 5.0
        or pd.Timestamp(raw.get("effective_end")).normalize() < date_max
        or raw.get("sell_stamp_duty_periods")
    ):
        raise RuntimeError("historical challenge base fee contract drift")

    raw["effective_start"] = "2023-01-01"
    raw["sell_stamp_duty_periods"] = (
        {
            "effective_start": "2023-01-01",
            "effective_end": "2023-08-27",
            "sell_stamp_duty_bps": 10.0,
            "source_reference": HISTORICAL_STAMP_DUTY_SOURCE_BEFORE_REDUCTION,
        },
        {
            "effective_start": "2023-08-28",
            "effective_end": str(raw["effective_end"]),
            "sell_stamp_duty_bps": 5.0,
            "source_reference": HISTORICAL_STAMP_DUTY_SOURCE_REDUCTION,
        },
    )
    raw["source_reference"] = "; ".join(
        (
            str(raw["source_reference"]),
            HISTORICAL_STAMP_DUTY_SOURCE_BEFORE_REDUCTION,
            HISTORICAL_STAMP_DUTY_SOURCE_REDUCTION,
        )
    )
    schedule = AShareFeeSchedule(**raw)
    schedule.validate()
    return schedule, "HISTORICAL_DATED_STAMP_DUTY"


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    declared = str(body.pop(field, ""))
    if not declared or v1._stable_hash(body) != declared:
        raise RuntimeError(f"{label} self-hash drift")


def _artifact_path(root: Path, artifacts: list[Mapping[str, Any]], name: str) -> Path:
    matches = [row for row in artifacts if Path(str(row["path"])).name == name]
    if len(matches) != 1:
        raise RuntimeError(f"confirmation artifact cardinality drift: {name}")
    path = (root / str(matches[0]["path"])).resolve()
    if (
        not path.is_file()
        or path.stat().st_size != int(matches[0]["bytes"])
        or v1._sha256(path) != str(matches[0]["sha256"])
    ):
        raise RuntimeError(f"confirmation artifact drift: {path}")
    return path


def _load_confirmation_cohort(
    root: Path,
    *,
    expected_selection_payload_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any], pd.DataFrame, pd.DataFrame]:
    root = root.resolve()
    manifest_path = root / "CONFIRMATION_COHORT_FROZEN.json"
    manifest = v1._read_json(manifest_path)
    _verify_self_hash(manifest, "manifest_payload_sha256", "confirmation cohort")
    required = {
        "status": "FIXED_SURVIVOR_CONFIRMATORY_FREEZE_CLOSED_IMMUTABLE",
        "selected_pairs": EXPECTED_PAIR_COUNT,
        "selected_candidate_members": EXPECTED_MEMBER_COUNT,
        "selection_payload_sha256": expected_selection_payload_sha256,
        "holdout_market_or_label_rows_read": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    drift = [key for key, value in required.items() if manifest.get(key) != value]
    if drift:
        raise RuntimeError("confirmation cohort drift: " + ",".join(drift))
    artifacts = list(manifest.get("artifacts") or ())
    for artifact in artifacts:
        _artifact_path(root, artifacts, Path(str(artifact["path"])).name)
    contract_path = _artifact_path(root, artifacts, "confirmation_contract.json")
    pair_path = _artifact_path(root, artifacts, "confirmation_pairs.parquet")
    candidate_path = _artifact_path(root, artifacts, "confirmation_candidates.parquet")
    contract = v1._read_json(contract_path)
    _verify_self_hash(contract, "contract_payload_sha256", "confirmation contract")
    if (
        contract.get("replacement") != "FORBIDDEN"
        or contract.get("backfill") != "FORBIDDEN"
        or contract.get("post_freeze_filtering") != "FORBIDDEN"
        or contract.get("automatic_promotion") != "FORBIDDEN"
    ):
        raise RuntimeError("confirmation contract mutation boundary drift")
    pairs = pd.read_parquet(pair_path).where(pd.notna, None)
    candidates = pd.read_parquet(candidate_path).where(pd.notna, None)
    if len(pairs) != EXPECTED_PAIR_COUNT or len(candidates) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError("confirmation cohort cardinality drift")
    if pairs["confirmation_order"].astype(int).tolist() != list(
        range(1, EXPECTED_PAIR_COUNT + 1)
    ):
        raise RuntimeError("confirmation pair order drift")
    expected_ids: list[str] = []
    for row in pairs.to_dict(orient="records"):
        expected_ids.extend(
            [str(row["primary_candidate_id"]), str(row["control_candidate_id"])]
        )
    if candidates["candidate_id"].astype(str).tolist() != expected_ids:
        raise RuntimeError("confirmation candidate order drift")
    if not pairs["confirmation_cohort_outcome"].eq(
        "FROZEN_ADAPTIVE_VALIDATION_SURVIVOR_INTENTION_TO_TREAT"
    ).all():
        raise RuntimeError("confirmation intention-to-treat identity drift")
    return manifest, contract, pairs, candidates


def _verify_authorization(
    path: Path,
    *,
    expected_sha256: str,
    expected_selection_payload_sha256: str,
    expected_forward_split_sha256: str,
    role_registry_path: Path | None = None,
    access_started_path: Path | None = None,
    outcome_path: Path | None = None,
) -> dict[str, Any]:
    if RUN_MODE == "historical_challenge":
        run_plans = PROJECT_ROOT / "runtime" / "run_plans"
        verify_historical_challenge_destructive_use(
            role_registry_path=(
                role_registry_path
                or run_plans / "evaluation_data_roles_v1.json"
            ),
            access_started_path=(
                access_started_path
                or run_plans / "cn_historical_challenge_2023_access_started.json"
            ),
            outcome_path=(
                outcome_path
                or run_plans
                / "cn_fixed10_historical_challenge_2023_outcome_20260806.json"
            ),
        )
    path = path.resolve()
    if v1._sha256(path) != expected_sha256:
        raise RuntimeError(f"{RUN_MODE} authorization file hash drift")
    authorization = v1._read_json(path)
    if RUN_MODE == "historical_challenge":
        _verify_self_hash(
            authorization,
            "contract_payload_sha256",
            "historical challenge authorization",
        )
        required = {
            "schema_version": "cn_historical_challenge_authorization_v1",
            "status": "HISTORICAL_CHALLENGE_2023_FIXED_TEN_AUTHORIZED_UNOPENED",
            "asset_id": "historical_challenge_2023_b05e2ca0",
            "source_archive_sha256": (
                "b05e2ca0b732821edf48a065c88b402d5c173b6a6b1266e0407bef8d9a546923"
            ),
            "fixed_cohort_selection_payload_sha256": (
                expected_selection_payload_sha256
            ),
            "fixed_pairs": EXPECTED_PAIR_COUNT,
            "fixed_members": EXPECTED_MEMBER_COUNT,
            "performance_rows_read_before_freeze": 0,
            "access_count": "exactly_one_report_only_challenge",
            "replacement": "FORBIDDEN",
            "backfill": "FORBIDDEN",
            "post_read_filtering": "FORBIDDEN",
            "optimizer_feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "automatic_promotion": "FORBIDDEN",
            "validation_holdout_forward_b_and_2026_reads": "FORBIDDEN",
        }
        drift = [
            key for key, expected in required.items()
            if authorization.get(key) != expected
        ]
        if drift:
            raise RuntimeError(
                "historical challenge authorization drift: " + ",".join(drift)
            )
        return authorization
    if authorization.get("status") != "AUTHORIZED_ZERO_FINANCIAL_PREFLIGHT":
        raise RuntimeError("forward authorization status drift")
    if authorization.get("confirmation_access_count") != "EXACTLY_ONE":
        raise RuntimeError("forward authorization access-count drift")
    if str(authorization["cohort"]["selection_payload_sha256"]) != (
        expected_selection_payload_sha256
    ):
        raise RuntimeError("forward authorization cohort drift")
    if str(authorization["forward_asset"]["split_manifest_sha256"]) != (
        expected_forward_split_sha256
    ):
        raise RuntimeError("forward authorization split drift")
    if int(authorization["forward_asset"]["preflight_forward_financial_rows_read"]) != 0:
        raise RuntimeError("forward authorization preflight was not zero-read")
    if not all(bool(value) for value in authorization["forbidden"].values()):
        raise RuntimeError("forward authorization forbidden boundary drift")
    return authorization


def _load_forward_context(
    *,
    execution_contract_path: Path,
    expected_execution_contract_sha256: str,
    field_root: Path,
    label_root: Path,
    session_authority_root: Path,
    split_hash: str,
) -> dict[str, Any]:
    execution_contract_path = execution_contract_path.resolve()
    if v1._sha256(execution_contract_path) != expected_execution_contract_sha256:
        raise RuntimeError("execution contract hash drift")
    contract = v1._read_json(execution_contract_path)
    base._verify_payload_hash(
        contract, field="contract_payload_sha256", label="execution contract"
    )
    field_manifest, field_manifest_path = base._validate_sidecar(
        field_root, evaluation_role=EVALUATION_ROLE, split_hash=split_hash
    )
    label_manifest, label_manifest_path = base._validate_label_sidecar(
        label_root, split_hash=split_hash, evaluation_role=EVALUATION_ROLE
    )
    field_frame = base._load_field_frame(field_root, field_manifest)
    if not field_frame["trade_time"].dt.strftime("%H:%M:%S").eq("15:00:00").all():
        raise RuntimeError(f"{RUN_MODE} sidecar contains intraday clocks")
    evaluation_dates = set(pd.to_datetime(field_frame["date"]).dt.normalize())
    if len(evaluation_dates) != int(field_manifest[f"eligible_{EVALUATION_ROLE}_date_count"]):
        raise RuntimeError(f"{RUN_MODE} sidecar calendar count drift")
    if EXPECTED_DATE_COUNT is not None and len(evaluation_dates) != EXPECTED_DATE_COUNT:
        raise RuntimeError(f"{RUN_MODE} sidecar expected-date count drift")
    if RUN_MODE == "historical_challenge" and not 200 <= len(evaluation_dates) < 250:
        raise RuntimeError("historical challenge evidence ceiling/calendar drift")

    session_authority_root = session_authority_root.resolve()
    session_manifest_path = (
        session_authority_root
        / f"{EVALUATION_ROLE}_session_authority_manifest.json"
    )
    session_manifest = v1._read_json(session_manifest_path)
    _verify_self_hash(
        session_manifest, "manifest_payload_sha256", "forward session authority"
    )
    required_session = {
        "schema_version": SESSION_SCHEMA_VERSION,
        "status": SESSION_STATUS,
        "evaluation_role": EVALUATION_ROLE,
        "data_role": DATA_ROLE,
        "field_manifest_sha256": v1._sha256(field_manifest_path),
        "validation_reads": 0,
        "holdout_reads": 0,
        "promotion": "FORBIDDEN",
    }
    drift = [
        key for key, value in required_session.items()
        if session_manifest.get(key) != value
    ]
    if drift:
        raise RuntimeError(f"{RUN_MODE} session authority drift: " + ",".join(drift))
    if int(session_manifest.get(READS_FIELD) or 0) <= 0:
        raise RuntimeError(f"{RUN_MODE} session authority has no role reads")
    if int(session_manifest.get("missing_exact_st_fail_closed_session_count", -1)) != 0:
        raise RuntimeError("forward session authority has missing ST states")
    for artifact in session_manifest.get("artifacts") or ():
        artifact_path = session_authority_root / str(artifact["path"])
        if (
            not artifact_path.is_file()
            or artifact_path.stat().st_size != int(artifact["bytes"])
            or v1._sha256(artifact_path) != str(artifact["sha256"])
        ):
            raise RuntimeError(f"forward authority artifact drift: {artifact_path}")
    session_path = (
        session_authority_root / f"{EVALUATION_ROLE}_session_authority.parquet"
    )
    session_authority = pd.read_parquet(session_path)
    session_dates = pd.to_datetime(session_authority["date"], errors="raise").dt.normalize()
    session_authority = session_authority[session_dates.isin(evaluation_dates)].copy()
    if set(pd.to_datetime(session_authority["date"]).dt.normalize()) != evaluation_dates:
        raise RuntimeError(f"{RUN_MODE} session authority calendar drift")
    master, observed_index, authority_index = base._materialize_replay_master(
        field_frame, session_authority
    )

    universe_raw = dict(contract["universe_policy"])
    universe_raw["allowed_exchanges"] = tuple(universe_raw["allowed_exchanges"])
    universe = AShareUniversePolicy(**universe_raw)
    fee, fee_schedule_mode = _fee_schedule_for_run(
        dict(contract["fee_schedule"]),
        evaluation_dates=evaluation_dates,
    )
    execution = AShareExecutionPolicy(**dict(contract["execution_policy"]))
    corporate = AShareCorporateActionPolicy(**dict(contract["corporate_action_policy"]))
    prepared_master = _prepare_sessions(
        master.assign(signal=0.0), universe_policy=universe
    ).drop(columns=["signal"])
    prepared_index = pd.MultiIndex.from_frame(prepared_master[["date", "code"]])
    if set(prepared_index) != set(authority_index):
        raise RuntimeError("forward prepared/session coordinate drift")
    future_returns = v1.forward_open_to_close_returns(prepared_master, horizons=(1,))
    codes = prepared_master["code"].astype(str).to_numpy()
    dates = pd.to_datetime(prepared_master["date"]).to_numpy()
    eligible = prepared_master["promotion_universe_eligible"].astype(bool).to_numpy()
    if not eligible.any():
        raise RuntimeError("forward prepared universe has no eligible sessions")
    order = np.argsort(dates, kind="mergesort")
    sorted_dates = dates[order]
    boundaries = np.flatnonzero(
        np.r_[True, sorted_dates[1:] != sorted_dates[:-1], True]
    )
    date_groups = [
        order[start:end] for start, end in zip(boundaries[:-1], boundaries[1:])
    ]
    field_reads = int(field_manifest[READS_FIELD])
    authority_reads = int(session_manifest["authority_session_row_count"])
    return {
        "contract": contract,
        "field_manifest": field_manifest,
        "label_manifest": label_manifest,
        "field_manifest_path": field_manifest_path,
        "label_manifest_path": label_manifest_path,
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
        "fee_schedule_mode": fee_schedule_mode,
        "execution": execution,
        "corporate": corporate,
        "field_reads": field_reads,
        "authority_reads": authority_reads,
        "evaluation_date_count": len(evaluation_dates),
        READS_FIELD: field_reads + authority_reads,
    }


def _candidate_metric(
    candidate: Mapping[str, Any],
    result: Mapping[str, Any],
    theoretical: Mapping[str, Any],
) -> dict[str, Any]:
    metric = train_v2._candidate_metric(
        candidate=candidate,
        decoder=POLICY,
        result=result,
        signal_rank_ic_mean=oos._finite(theoretical["signal_rank_ic_mean"]),
        theoretical_gross_mean=oos._finite(theoretical["gross_forward_return_mean"]),
    )
    metric.update(oos._daily_distribution(result))
    metric.update(
        {
            "evaluation_role": EVALUATION_ROLE,
            "data_role": DATA_ROLE,
            "evidence_scope": EVIDENCE_SCOPE,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": (
                int(result[READS_FIELD]) if READS_FIELD == "forward_2026_reads" else 0
            ),
            "historical_challenge_reads": (
                int(result[READS_FIELD])
                if READS_FIELD == "historical_challenge_reads"
                else 0
            ),
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
    signal = pd.to_numeric(
        base.evaluate_panel_expression(
            context["field_frame"],
            str(candidate["expression"]),
            cache={},
            data_role=DATA_ROLE,
        ),
        errors="coerce",
    )
    signal_series = pd.Series(signal.to_numpy(), index=context["observed_index"])
    aligned = signal_series.reindex(context["prepared_index"]).to_numpy(dtype=float)
    theoretical = v1.evaluate_candidate_decoder_matrix(
        candidate=candidate,
        signal=aligned,
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
    replay_frame["signal"] = signal_series.reindex(context["authority_index"]).to_numpy()
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
        raise RuntimeError(f"{RUN_MODE} decoder policy hash drift")
    result = dict(result)
    result[READS_FIELD] = int(context[READS_FIELD])
    metric = _candidate_metric(candidate, result, theoretical)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": CANDIDATE_STATUS,
        "candidate_id": candidate_id,
        "input_data_sha256": input_data_sha256,
        "metric": metric,
    }
    payload["payload_sha256"] = v1._stable_hash(payload)
    v1._write_json(candidate_root / f"{candidate_id}.json", payload)
    return metric


def _initialize_worker(
    execution_contract_path: str,
    expected_execution_contract_sha256: str,
    field_root: str,
    label_root: str,
    session_authority_root: str,
    split_hash: str,
    candidate_root: str,
    input_data_sha256: str,
) -> None:
    global _PROCESS_CONTEXT, _PROCESS_CANDIDATE_ROOT, _PROCESS_INPUT_HASH
    _PROCESS_CONTEXT = _load_forward_context(
        execution_contract_path=Path(execution_contract_path),
        expected_execution_contract_sha256=expected_execution_contract_sha256,
        field_root=Path(field_root),
        label_root=Path(label_root),
        session_authority_root=Path(session_authority_root),
        split_hash=split_hash,
    )
    _PROCESS_CANDIDATE_ROOT = Path(candidate_root)
    _PROCESS_INPUT_HASH = input_data_sha256


def _evaluate_in_worker(candidate: Mapping[str, Any]) -> dict[str, Any]:
    if _PROCESS_CONTEXT is None or _PROCESS_CANDIDATE_ROOT is None or _PROCESS_INPUT_HASH is None:
        raise RuntimeError(f"{RUN_MODE} worker is not initialized")
    return _evaluate_candidate(
        candidate,
        context=_PROCESS_CONTEXT,
        candidate_root=_PROCESS_CANDIDATE_ROOT,
        input_data_sha256=_PROCESS_INPUT_HASH,
    )


def _pair_metrics(pairs: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    by_id = {
        str(row["candidate_id"]): row for row in candidates.to_dict(orient="records")
    }
    rows: list[dict[str, Any]] = []
    for source in pairs.to_dict(orient="records"):
        primary = by_id[str(source["primary_candidate_id"])]
        control = by_id[str(source["control_candidate_id"])]
        primary_reward = float(primary["continuous_book_net_reward"])
        control_reward = float(control["continuous_book_net_reward"])
        primary_return = float(primary["cumulative_net_return"])
        control_return = float(control["cumulative_net_return"])
        reward_increment = primary_reward - control_reward
        return_increment = primary_return - control_return
        rows.append(
            {
                "confirmation_order": int(source["confirmation_order"]),
                "source_finalist_order": int(source["finalist_order"]),
                "pair_id": str(source["pair_id"]),
                "primary_candidate_id": str(source["primary_candidate_id"]),
                "control_candidate_id": str(source["control_candidate_id"]),
                "decoder_id": POLICY.decoder_id,
                "decoder_policy_sha256": POLICY.payload_sha256,
                "train_search_score": source.get("search_score"),
                "train_rank_ic_mean": source.get("train_rank_ic_mean"),
                "train_decoder_primary_cumulative_net_return": source.get(
                    "primary_cumulative_net_return"
                ),
                "primary_continuous_book_net_reward": primary_reward,
                "control_continuous_book_net_reward": control_reward,
                "matched_continuous_book_net_reward_increment": reward_increment,
                "primary_cumulative_net_return": primary_return,
                "control_cumulative_net_return": control_return,
                "matched_cumulative_net_return_increment": return_increment,
                "primary_mean_one_way_turnover": primary["mean_one_way_turnover"],
                "primary_net_return_per_turnover": primary["net_return_per_turnover"],
                "primary_total_fees_cny": primary["total_fees_cny"],
                "primary_cumulative_realized_trade_pnl_cny": primary[
                    "cumulative_realized_trade_pnl_cny"
                ],
                "primary_ending_unrealized_pnl_cny": primary[
                    "ending_unrealized_pnl_cny"
                ],
                "primary_ending_holdings_weight": primary["ending_holdings_weight"],
                "primary_daily_net_return_p10": primary["daily_net_return_p10"],
                "primary_daily_net_return_worst": primary["daily_net_return_worst"],
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


def _wilson(successes: int, total: int) -> tuple[float, float]:
    if total <= 0:
        return math.nan, math.nan
    z = 1.959963984540054
    p = successes / total
    denominator = 1.0 + z * z / total
    center = (p + z * z / (2.0 * total)) / denominator
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total) / denominator
    return center - margin, center + margin


def run_forward_confirmation(
    *,
    confirmation_root: Path,
    authorization_path: Path,
    expected_authorization_sha256: str,
    execution_contract_path: Path,
    expected_execution_contract_sha256: str,
    field_root: Path,
    label_root: Path,
    session_authority_root: Path,
    output_root: Path,
    builder_commit_sha: str,
    expected_selection_payload_sha256: str,
    expected_decoder_policy_sha256: str,
    expected_forward_split_sha256: str,
    worker_count: int,
    executor_worker_count: int,
) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(f"{RUN_MODE} must run on {AUTHORIZED_HOST}")
    initial_free = int(psutil.virtual_memory().available)
    if initial_free < MINIMUM_FREE_MEMORY_BYTES:
        raise RuntimeError(f"{RUN_MODE} minimum-free-memory gate failed")
    train_v2._validate_executor_contract(
        execution_backend="PROCESS_POOL",
        entitlement_worker_count=int(worker_count),
        executor_worker_count=int(executor_worker_count),
    )
    if os.environ.get("CN_NODE_RESOURCE_LEASE_REQUIRED") == "1":
        if int(os.environ.get("CN_NODE_CPU_ENTITLEMENT") or 0) != int(worker_count):
            raise RuntimeError(f"{RUN_MODE} lease entitlement drift")
        nested = {
            key: int(os.environ.get(key) or 0)
            for key in (
                "NUMBA_NUM_THREADS", "POLARS_MAX_THREADS", "ARROW_NUM_THREADS",
                "OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
                "NUMEXPR_NUM_THREADS",
            )
        }
        if any(value != 1 for value in nested.values()):
            raise RuntimeError(f"{RUN_MODE} nested parallelism drift: {nested}")
    if len(builder_commit_sha) != 40:
        raise ValueError("builder_commit_sha must be a full Git SHA")
    if POLICY.payload_sha256 != expected_decoder_policy_sha256:
        raise RuntimeError("runtime decoder policy hash drift")

    authorization = _verify_authorization(
        authorization_path,
        expected_sha256=expected_authorization_sha256,
        expected_selection_payload_sha256=expected_selection_payload_sha256,
        expected_forward_split_sha256=expected_forward_split_sha256,
    )
    cohort_manifest, cohort_contract, pairs, candidate_table = _load_confirmation_cohort(
        confirmation_root,
        expected_selection_payload_sha256=expected_selection_payload_sha256,
    )
    context = _load_forward_context(
        execution_contract_path=execution_contract_path,
        expected_execution_contract_sha256=expected_execution_contract_sha256,
        field_root=field_root,
        label_root=label_root,
        session_authority_root=session_authority_root,
        split_hash=expected_forward_split_sha256,
    )
    if int(context[READS_FIELD]) <= 0:
        raise RuntimeError(f"{RUN_MODE} has no role reads")
    if int(context["field_manifest"].get("validation_reads") or 0) != 0:
        raise RuntimeError(f"{RUN_MODE} read validation")
    if int(context["field_manifest"].get("holdout_reads") or 0) != 0:
        raise RuntimeError(f"{RUN_MODE} read holdout")
    if RUN_MODE == "historical_challenge":
        if int(context["field_manifest"].get("forward_2026_reads") or 0) != 0:
            raise RuntimeError("historical challenge read forward_2026")
    context_date_count = int(context["evaluation_date_count"])

    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=False)
    input_binding = {
        "schema_version": SCHEMA_VERSION,
        "authorization_file_sha256": expected_authorization_sha256,
        "selection_payload_sha256": expected_selection_payload_sha256,
        "confirmation_cohort_manifest_sha256": v1._sha256(
            confirmation_root / "CONFIRMATION_COHORT_FROZEN.json"
        ),
        "confirmation_cohort_manifest_payload_sha256": cohort_manifest[
            "manifest_payload_sha256"
        ],
        "confirmation_contract_payload_sha256": cohort_contract[
            "contract_payload_sha256"
        ],
        "execution_contract_sha256": expected_execution_contract_sha256,
        "fee_schedule_mode": str(context["fee_schedule_mode"]),
        "fee_schedule_payload_sha256": context["fee"].payload_sha256,
        "fee_schedule": asdict(context["fee"]),
        "split_manifest_sha256": expected_forward_split_sha256,
        "field_manifest_sha256": v1._sha256(context["field_manifest_path"]),
        "label_manifest_sha256": v1._sha256(context["label_manifest_path"]),
        "session_authority_manifest_sha256": v1._sha256(
            context["session_manifest_path"]
        ),
        "session_authority_sha256": v1._sha256(context["session_path"]),
        "decoder_contract": {**asdict(POLICY), "payload_sha256": POLICY.payload_sha256},
        "pair_ids": pairs["pair_id"].astype(str).tolist(),
        "candidate_ids": candidate_table["candidate_id"].astype(str).tolist(),
        "evaluation_role": EVALUATION_ROLE,
        "data_role": DATA_ROLE,
        "evidence_scope": EVIDENCE_SCOPE,
        "interstage_filter_applied": False,
        "cohort_mutated": False,
        "worker_count": int(worker_count),
        "execution_backend": "PROCESS_POOL",
        "executor_worker_count": int(executor_worker_count),
        "native_threads_per_executor_worker": 1,
        "builder_commit_sha": builder_commit_sha,
        "builder_source_sha256": v1._sha256(Path(__file__).resolve()),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": (
            int(context[READS_FIELD]) if READS_FIELD == "forward_2026_reads" else 0
        ),
        "historical_challenge_reads": (
            int(context[READS_FIELD])
            if READS_FIELD == "historical_challenge_reads"
            else 0
        ),
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    input_hash = v1._stable_hash(input_binding)
    binding_path = v1._write_json(output_root / "input_binding.json", input_binding)
    candidate_root = output_root / "candidates"
    candidate_root.mkdir()

    del context
    gc.collect()
    metrics: list[dict[str, Any]] = []
    minimum_free = initial_free
    maximum_tree_rss = 0
    cpu_samples: list[float] = []
    started = time.perf_counter()
    with ProcessPoolExecutor(
        max_workers=int(executor_worker_count),
        initializer=_initialize_worker,
        initargs=(
            str(execution_contract_path), expected_execution_contract_sha256,
            str(field_root), str(label_root), str(session_authority_root),
            expected_forward_split_sha256, str(candidate_root), input_hash,
        ),
    ) as executor:
        pending = {
            executor.submit(_evaluate_in_worker, row)
            for row in candidate_table.to_dict(orient="records")
        }
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
                raise RuntimeError(f"{RUN_MODE} runtime memory gate failed")
            for future in completed:
                metrics.append(future.result())
    elapsed = float(time.perf_counter() - started)
    candidate_metrics = pd.DataFrame(metrics)
    expected_ids = candidate_table["candidate_id"].astype(str).tolist()
    order = {candidate_id: index for index, candidate_id in enumerate(expected_ids)}
    candidate_metrics["_order"] = candidate_metrics["candidate_id"].map(order)
    candidate_metrics = candidate_metrics.sort_values("_order", kind="mergesort").drop(
        columns="_order"
    ).reset_index(drop=True)
    if candidate_metrics["candidate_id"].astype(str).tolist() != expected_ids:
        raise RuntimeError(f"{RUN_MODE} candidate order drift")
    if len(candidate_metrics) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError(f"{RUN_MODE} candidate metric count drift")
    if not candidate_metrics["accounting_invariants_status"].eq("PASS").all():
        raise RuntimeError(f"{RUN_MODE} accounting invariants failed")
    pair_metrics = _pair_metrics(pairs, candidate_metrics)
    if pair_metrics["pair_id"].astype(str).tolist() != pairs["pair_id"].astype(str).tolist():
        raise RuntimeError(f"{RUN_MODE} pair order drift")

    candidate_path = output_root / f"{ARTIFACT_PREFIX}_candidate_metrics.parquet"
    pair_path = output_root / f"{ARTIFACT_PREFIX}_pair_metrics.parquet"
    candidate_metrics.to_parquet(candidate_path, index=False)
    pair_metrics.to_parquet(pair_path, index=False)
    all_four = int(pair_metrics["all_four_economic_gates_positive"].sum())
    wilson_low, wilson_high = _wilson(all_four, EXPECTED_PAIR_COUNT)
    positive_returns = pair_metrics["primary_cumulative_net_return"].clip(lower=0.0)
    positive_sum = float(positive_returns.sum())
    sorted_positive = positive_returns.sort_values(ascending=False)
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": REPORT_STATUS,
        "evidence_scope": EVIDENCE_SCOPE,
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        DATE_COUNT_FIELD: int(context_date_count),
        "decoder_id": POLICY.decoder_id,
        "decoder_policy_sha256": POLICY.payload_sha256,
        "intention_to_treat": True,
        "replacement": "FORBIDDEN",
        "backfill": "FORBIDDEN",
        "interstage_filter_applied": False,
        "primary_reward_positive_count": int(pair_metrics["absolute_reward_positive"].sum()),
        "matched_reward_increment_positive_count": int(
            pair_metrics["matched_reward_increment_positive"].sum()
        ),
        "primary_return_positive_count": int(pair_metrics["absolute_return_positive"].sum()),
        "matched_return_increment_positive_count": int(
            pair_metrics["matched_return_increment_positive"].sum()
        ),
        "all_four_economic_gates_positive_count": all_four,
        "all_four_success_rate": all_four / EXPECTED_PAIR_COUNT,
        "all_four_success_rate_wilson95_low": wilson_low,
        "all_four_success_rate_wilson95_high": wilson_high,
        "primary_cumulative_return_median": oos._finite(
            pair_metrics["primary_cumulative_net_return"].median()
        ),
        "primary_cumulative_return_p10": oos._finite(
            pair_metrics["primary_cumulative_net_return"].quantile(0.10)
        ),
        "matched_cumulative_return_increment_median": oos._finite(
            pair_metrics["matched_cumulative_net_return_increment"].median()
        ),
        "matched_cumulative_return_increment_p10": oos._finite(
            pair_metrics["matched_cumulative_net_return_increment"].quantile(0.10)
        ),
        "primary_turnover_median": oos._finite(
            pair_metrics["primary_mean_one_way_turnover"].median()
        ),
        "primary_net_return_per_turnover_median": oos._finite(
            pair_metrics["primary_net_return_per_turnover"].median()
        ),
        "primary_daily_left_tail_p10_median": oos._finite(
            pair_metrics["primary_daily_net_return_p10"].median()
        ),
        "primary_worst_daily_return": oos._finite(
            pair_metrics["primary_daily_net_return_worst"].min()
        ),
        "primary_quarterly_regime_positive_share_median": oos._finite(
            pair_metrics["primary_quarterly_regime_positive_share"].median()
        ),
        "positive_return_top1_contribution_share": (
            float(sorted_positive.head(1).sum() / positive_sum) if positive_sum > 0 else None
        ),
        "positive_return_top5_contribution_share": (
            float(sorted_positive.head(5).sum() / positive_sum) if positive_sum > 0 else None
        ),
        f"train_search_score_to_{ARTIFACT_PREFIX}_return_spearman": oos._spearman(
            pair_metrics, "train_search_score", "primary_cumulative_net_return"
        ),
        f"train_decoder_to_{ARTIFACT_PREFIX}_return_spearman": oos._spearman(
            pair_metrics,
            "train_decoder_primary_cumulative_net_return",
            "primary_cumulative_net_return",
        ),
        "elapsed_seconds": elapsed,
        "pairs_per_hour": EXPECTED_PAIR_COUNT * 3600.0 / elapsed,
        "worker_count": int(worker_count),
        "execution_backend": "PROCESS_POOL",
        "executor_worker_count": int(executor_worker_count),
        "minimum_free_memory_bytes": int(minimum_free),
        "maximum_process_tree_rss_bytes": int(maximum_tree_rss),
        "host_cpu_mean_percent": oos._finite(np.mean(cpu_samples)),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": int(input_binding["forward_2026_reads"]),
        "historical_challenge_reads": int(
            input_binding["historical_challenge_reads"]
        ),
        "evaluation_asset_access_state": ACCESS_STATE,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "promotion_authorized": False,
        "project_state_after_run": PROJECT_STATE_AFTER_RUN,
    }
    report_path = v1._write_json(
        output_root / f"{ARTIFACT_PREFIX}_report.json", report
    )
    artifacts = [
        v1._artifact(path, root=output_root)
        for path in (
            binding_path, candidate_path, pair_path, report_path,
            *sorted(candidate_root.glob("*.json")),
        )
    ]
    closure = {
        **report,
        "status": STATUS,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "accounting_invariants_status": "PASS",
        "artifacts": artifacts,
    }
    closure["manifest_body_sha256"] = v1._stable_hash(closure)
    closure_path = v1._write_json(output_root / COMPLETE_FILENAME, closure)
    return {
        **report,
        "closure_path": str(closure_path),
        "closure_sha256": v1._sha256(closure_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirmation-root", type=Path, required=True)
    parser.add_argument("--authorization-path", type=Path, required=True)
    parser.add_argument("--expected-authorization-sha256", required=True)
    parser.add_argument("--execution-contract-path", type=Path, required=True)
    parser.add_argument("--expected-execution-contract-sha256", required=True)
    parser.add_argument("--field-root", type=Path, required=True)
    parser.add_argument("--label-root", type=Path, required=True)
    parser.add_argument("--session-authority-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--builder-commit-sha", required=True)
    parser.add_argument("--expected-selection-payload-sha256", required=True)
    parser.add_argument("--expected-decoder-policy-sha256", required=True)
    parser.add_argument("--expected-forward-split-sha256", required=True)
    parser.add_argument("--worker-count", type=int, default=32)
    parser.add_argument("--executor-worker-count", type=int, default=12)
    args = parser.parse_args()
    result = run_forward_confirmation(
        confirmation_root=args.confirmation_root,
        authorization_path=args.authorization_path,
        expected_authorization_sha256=args.expected_authorization_sha256,
        execution_contract_path=args.execution_contract_path,
        expected_execution_contract_sha256=args.expected_execution_contract_sha256,
        field_root=args.field_root,
        label_root=args.label_root,
        session_authority_root=args.session_authority_root,
        output_root=args.output_root,
        builder_commit_sha=args.builder_commit_sha,
        expected_selection_payload_sha256=args.expected_selection_payload_sha256,
        expected_decoder_policy_sha256=args.expected_decoder_policy_sha256,
        expected_forward_split_sha256=args.expected_forward_split_sha256,
        worker_count=args.worker_count,
        executor_worker_count=args.executor_worker_count,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
