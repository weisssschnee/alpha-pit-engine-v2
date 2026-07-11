"""Sparse, PIT-safe true1min plate aggregation with leave-one-out peers."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import Any, Mapping

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix, csr_matrix

from our_system_phase2.services.feature_state_fabric import (
    FieldRole,
    FieldSpec,
    MissingPolicy,
    ObservableClock,
)
from our_system_phase2.services.pit_group_release import FORWARD_SEALED_FROM
from our_system_phase2.services.pit_group_sidecar import (
    PITGroupContract,
    membership_content_sha256,
)


TRUE1MIN_PLATE_AGGREGATION_VERSION = "nextgen_dark_true1min_plate_sparse_v1"


@dataclass(frozen=True, slots=True)
class True1MinPlateAggregationResult:
    group_panel: pd.DataFrame
    stock_context: pd.DataFrame
    diagnostics: dict[str, Any]


def _normalize_code(value: Any) -> str:
    matches = re.findall(r"\d{6}", str(value))
    if len(matches) != 1:
        raise ValueError(f"invalid CN security code: {value!r}")
    return matches[0]


def _active_membership(
    membership: pd.DataFrame,
    *,
    observed_at: pd.Timestamp,
) -> pd.DataFrame:
    required = {
        "code",
        "group_id",
        "effective_from",
        "effective_to",
        "source_observed_at",
    }
    missing = sorted(required - set(membership.columns))
    if missing:
        raise ValueError(f"plate membership missing columns: {missing}")
    frame = membership.copy()
    frame["code"] = frame["code"].map(_normalize_code)
    frame["group_id"] = frame["group_id"].astype(str).str.strip()
    if "source_observed_to" not in frame.columns:
        frame["source_observed_to"] = pd.NaT
    for column in (
        "effective_from",
        "effective_to",
        "source_observed_at",
        "source_observed_to",
    ):
        frame[column] = pd.to_datetime(frame[column], errors="coerce", format="mixed").astype(
            "datetime64[ns]"
        )
    if frame[["effective_from", "source_observed_at"]].isna().any().any():
        raise ValueError("plate membership has invalid entry clocks")
    observable_from = frame[["effective_from", "source_observed_at"]].max(axis=1)
    observable_to = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
    has_exit_observation = frame["source_observed_to"].notna()
    observable_to.loc[has_exit_observation] = frame.loc[
        has_exit_observation, ["effective_to", "source_observed_to"]
    ].max(axis=1)
    active = observable_from.le(observed_at) & (
        observable_to.isna() | observable_to.gt(observed_at)
    )
    out = frame.loc[active, ["code", "group_id"]].drop_duplicates()
    if out["group_id"].eq("").any():
        raise ValueError("empty plate group_id")
    return out.sort_values(["code", "group_id"], kind="mergesort").reset_index(drop=True)


def _validate_membership_release(
    membership: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    execution_session: pd.Timestamp,
) -> None:
    required = {
        "source_name",
        "source_version",
        "group_type",
        "membership_policy",
        "row_count",
        "membership_sha256",
        "survivorship_guard",
        "reward_or_performance_used",
        "coverage_gate",
        "required_start",
        "required_end",
    }
    missing = sorted(required - set(manifest))
    if missing:
        raise ValueError(f"membership release manifest missing keys: {missing}")
    if manifest["survivorship_guard"] is not True:
        raise ValueError("membership release lacks survivorship guard")
    if manifest["reward_or_performance_used"] is not False:
        raise ValueError("performance-derived membership release is forbidden")
    if manifest["coverage_gate"] != "PASS":
        raise ValueError("membership release coverage gate did not pass")
    if int(manifest["row_count"]) != len(membership):
        raise ValueError("membership release row count mismatch")
    required_start = pd.Timestamp(manifest["required_start"]).normalize()
    required_end = pd.Timestamp(manifest["required_end"]).normalize()
    if not (required_start <= execution_session <= required_end):
        raise ValueError("execution session is outside membership release coverage")
    contract = PITGroupContract(
        source_name=str(manifest["source_name"]),
        source_version=str(manifest["source_version"]),
        group_type=str(manifest["group_type"]),
        membership_policy=str(manifest["membership_policy"]),
    )
    actual_hash = membership_content_sha256(membership, contract)
    if actual_hash != str(manifest["membership_sha256"]):
        raise ValueError("membership release content hash mismatch")


def _reject_intraday_membership_transitions(
    membership: pd.DataFrame,
    *,
    first_bar: pd.Timestamp,
    last_bar: pd.Timestamp,
) -> None:
    frame = membership.copy()
    if "source_observed_to" not in frame.columns:
        frame["source_observed_to"] = pd.NaT
    for column in ("effective_from", "effective_to", "source_observed_at", "source_observed_to"):
        frame[column] = pd.to_datetime(frame[column], errors="coerce", format="mixed")
    observable_from = frame[["effective_from", "source_observed_at"]].max(axis=1)
    observable_to = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
    observed_exit = frame["source_observed_to"].notna()
    observable_to.loc[observed_exit] = frame.loc[
        observed_exit, ["effective_to", "source_observed_to"]
    ].max(axis=1)
    entry_inside = observable_from.gt(first_bar) & observable_from.le(last_bar)
    exit_inside = observable_to.gt(first_bar) & observable_to.le(last_bar)
    if entry_inside.any() or exit_inside.any():
        raise ValueError(
            "intraday membership transition is unsupported; split the session at transition clocks"
        )


def _fingerprint(*frames: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    for frame in frames:
        digest.update(pd.util.hash_pandas_object(frame, index=False).to_numpy().tobytes())
    return digest.hexdigest()


def aggregate_true1min_plate(
    bars: pd.DataFrame,
    membership: pd.DataFrame,
    *,
    membership_manifest: Mapping[str, Any],
    data_role: str,
    return_field: str = "ret_1m",
    amount_field: str = "amount_yuan",
    volume_field: str = "volume",
    minimum_active_peers: int = 2,
) -> True1MinPlateAggregationResult:
    if data_role != "development":
        raise PermissionError("true1min plate aggregation is development-only")
    if minimum_active_peers < 1:
        raise ValueError("minimum_active_peers must be positive")
    required = {"code", "trade_time", return_field, amount_field, volume_field}
    missing = sorted(required - set(bars.columns))
    if missing:
        raise ValueError(f"true1min bars missing columns: {missing}")
    frame = bars[["code", "trade_time", return_field, amount_field, volume_field]].copy()
    frame["code"] = frame["code"].map(_normalize_code)
    frame["trade_time"] = pd.to_datetime(
        frame["trade_time"], errors="coerce", format="mixed"
    ).astype("datetime64[ns]")
    if frame["trade_time"].isna().any():
        raise ValueError("invalid true1min trade_time")
    if frame["trade_time"].ge(FORWARD_SEALED_FROM).any():
        raise ValueError("true1min plate aggregation cannot access sealed 2026")
    sessions = frame["trade_time"].dt.normalize().unique()
    if len(sessions) != 1:
        raise ValueError("true1min plate aggregation requires one execution session")
    if frame.duplicated(["code", "trade_time"]).any():
        raise ValueError("duplicate true1min code/trade_time")
    for column in (return_field, amount_field, volume_field):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.sort_values(["trade_time", "code"], kind="mergesort").reset_index(drop=True)
    execution_session = pd.Timestamp(sessions[0])
    _validate_membership_release(
        membership,
        membership_manifest,
        execution_session=execution_session,
    )
    _reject_intraday_membership_transitions(
        membership,
        first_bar=frame["trade_time"].min(),
        last_bar=frame["trade_time"].max(),
    )
    active_membership = _active_membership(
        membership,
        observed_at=frame["trade_time"].min(),
    )
    bar_codes = set(frame["code"])
    codes = sorted(bar_codes | set(active_membership["code"]))
    groups = sorted(active_membership["group_id"].unique())
    if not groups:
        raise ValueError("no observable plate membership for true1min bars")
    code_index = {code: index for index, code in enumerate(codes)}
    group_index = {group: index for index, group in enumerate(groups)}
    edge_codes = active_membership["code"].map(code_index).to_numpy(dtype=np.int64)
    edge_groups = active_membership["group_id"].map(group_index).to_numpy(dtype=np.int64)
    membership_matrix = csr_matrix(
        (np.ones(len(active_membership), dtype=np.float64), (edge_codes, edge_groups)),
        shape=(len(codes), len(groups)),
    )
    times = pd.Index(sorted(frame["trade_time"].unique()))
    time_index = {time: index for index, time in enumerate(times)}
    row_times = frame["trade_time"].map(time_index).to_numpy(dtype=np.int64)
    row_codes = frame["code"].map(code_index).to_numpy(dtype=np.int64)
    returns = frame[return_field].to_numpy(dtype=np.float64)
    amounts = frame[amount_field].to_numpy(dtype=np.float64)
    volumes = frame[volume_field].to_numpy(dtype=np.float64)
    shape = (len(times), len(codes))

    active_values = np.isfinite(returns).astype(np.float64)
    active_matrix = coo_matrix((active_values, (row_times, row_codes)), shape=shape).tocsr()
    return_matrix = coo_matrix(
        (np.nan_to_num(returns, nan=0.0), (row_times, row_codes)), shape=shape
    ).tocsr()
    up_matrix = coo_matrix(
        ((np.isfinite(returns) & (returns > 0)).astype(np.float64), (row_times, row_codes)),
        shape=shape,
    ).tocsr()
    amount_matrix = coo_matrix(
        (np.nan_to_num(amounts, nan=0.0), (row_times, row_codes)), shape=shape
    ).tocsr()
    amount_active_matrix = coo_matrix(
        (np.isfinite(amounts).astype(np.float64), (row_times, row_codes)), shape=shape
    ).tocsr()
    volume_matrix = coo_matrix(
        (np.nan_to_num(volumes, nan=0.0), (row_times, row_codes)), shape=shape
    ).tocsr()

    group_active = (active_matrix @ membership_matrix).toarray()
    group_return_sum = (return_matrix @ membership_matrix).toarray()
    group_up = (up_matrix @ membership_matrix).toarray()
    group_amount = (amount_matrix @ membership_matrix).toarray()
    group_amount_active = (amount_active_matrix @ membership_matrix).toarray()
    group_volume = (volume_matrix @ membership_matrix).toarray()
    member_count = np.asarray(membership_matrix.sum(axis=0)).ravel()
    with np.errstate(divide="ignore", invalid="ignore"):
        group_return = np.where(group_active > 0, group_return_sum / group_active, np.nan)
        group_up_ratio = np.where(group_active > 0, group_up / group_active, np.nan)
        group_coverage = np.where(member_count > 0, group_active / member_count, np.nan)

    time_values = np.repeat(times.to_numpy(), len(groups))
    group_values = np.tile(np.asarray(groups, dtype=object), len(times))
    group_panel = pd.DataFrame(
        {
            "trade_time": time_values,
            "group_id": group_values,
            "plate_member_count": np.tile(member_count, len(times)),
            "plate_active_count": group_active.ravel(),
            "plate_coverage": group_coverage.ravel(),
            "plate_return_equal": group_return.ravel(),
            "plate_up_ratio": group_up_ratio.ravel(),
            "plate_amount_sum": group_amount.ravel(),
            "plate_volume_sum": group_volume.ravel(),
        }
    ).sort_values(["trade_time", "group_id"], kind="mergesort", ignore_index=True)

    peer_return_mean = np.full(shape, np.nan)
    peer_return_max = np.full(shape, np.nan)
    peer_up_mean = np.full(shape, np.nan)
    peer_coverage_mean = np.full(shape, np.nan)
    peer_amount_mean = np.full(shape, np.nan)
    peer_group_count = np.zeros(shape, dtype=np.int64)
    for time_pos in range(len(times)):
        own_return = return_matrix.getrow(time_pos).toarray().ravel()
        own_amount = amount_matrix.getrow(time_pos).toarray().ravel()
        own_active = active_matrix.getrow(time_pos).toarray().ravel()
        own_up = up_matrix.getrow(time_pos).toarray().ravel()
        denominator = group_active[time_pos, edge_groups] - own_active[edge_codes]
        valid = denominator >= minimum_active_peers
        if not valid.any():
            continue
        valid_codes = edge_codes[valid]
        valid_groups = edge_groups[valid]
        denom = denominator[valid]
        peer_return = (
            group_return_sum[time_pos, valid_groups] - own_return[valid_codes]
        ) / denom
        peer_up = (group_up[time_pos, valid_groups] - own_up[valid_codes]) / denom
        possible_peers = member_count[valid_groups] - 1.0
        peer_coverage = np.where(possible_peers > 0, denom / possible_peers, np.nan)
        counts = np.bincount(valid_codes, minlength=len(codes))
        nonzero = counts > 0
        peer_group_count[time_pos] = counts
        peer_return_mean[time_pos, nonzero] = (
            np.bincount(valid_codes, weights=peer_return, minlength=len(codes))[nonzero]
            / counts[nonzero]
        )
        maximum = np.full(len(codes), -np.inf)
        np.maximum.at(maximum, valid_codes, peer_return)
        peer_return_max[time_pos, nonzero] = maximum[nonzero]
        peer_up_mean[time_pos, nonzero] = (
            np.bincount(valid_codes, weights=peer_up, minlength=len(codes))[nonzero]
            / counts[nonzero]
        )
        peer_coverage_mean[time_pos, nonzero] = (
            np.bincount(valid_codes, weights=peer_coverage, minlength=len(codes))[nonzero]
            / counts[nonzero]
        )
        own_amount_active = amount_active_matrix.getrow(time_pos).toarray().ravel()
        amount_denominator = (
            group_amount_active[time_pos, edge_groups] - own_amount_active[edge_codes]
        )
        amount_valid = amount_denominator >= minimum_active_peers
        if amount_valid.any():
            amount_codes = edge_codes[amount_valid]
            amount_groups = edge_groups[amount_valid]
            peer_amount = (
                group_amount[time_pos, amount_groups] - own_amount[amount_codes]
            ) / amount_denominator[amount_valid]
            amount_counts = np.bincount(amount_codes, minlength=len(codes))
            amount_nonzero = amount_counts > 0
            peer_amount_mean[time_pos, amount_nonzero] = (
                np.bincount(amount_codes, weights=peer_amount, minlength=len(codes))[
                    amount_nonzero
                ]
                / amount_counts[amount_nonzero]
            )

    stock_context = pd.DataFrame(
        {
            "code": frame["code"],
            "trade_time": frame["trade_time"],
            "plate_peer_return_mean": peer_return_mean[row_times, row_codes],
            "plate_peer_return_max": peer_return_max[row_times, row_codes],
            "plate_peer_up_ratio_mean": peer_up_mean[row_times, row_codes],
            "plate_peer_coverage_mean": peer_coverage_mean[row_times, row_codes],
            "plate_peer_amount_mean": peer_amount_mean[row_times, row_codes],
            "plate_peer_group_count": peer_group_count[row_times, row_codes],
        }
    ).sort_values(["trade_time", "code"], kind="mergesort", ignore_index=True)
    diagnostics = {
        "aggregation_version": TRUE1MIN_PLATE_AGGREGATION_VERSION,
        "data_role": data_role,
        "execution_session": execution_session.isoformat(),
        "bar_row_count": len(frame),
        "bar_code_count": len(bar_codes),
        "membership_code_count": int(active_membership["code"].nunique()),
        "matrix_code_count": len(codes),
        "minute_count": len(times),
        "active_membership_edge_count": len(active_membership),
        "group_count": len(groups),
        "minimum_active_peers": minimum_active_peers,
        "leave_one_out": True,
        "formal_performance_search_allowed": False,
        "reward_or_performance_used": False,
        "forward_2026_performance_accessed": False,
        "output_fingerprint": _fingerprint(group_panel, stock_context),
    }
    return True1MinPlateAggregationResult(group_panel, stock_context, diagnostics)


def true1min_plate_field_specs() -> tuple[FieldSpec, ...]:
    roles = {
        "plate_member_count": FieldRole.CONDITION_ONLY,
        "plate_active_count": FieldRole.CONDITION_ONLY,
        "plate_coverage": FieldRole.CONDITION_ONLY,
        "plate_return_equal": FieldRole.BENCHMARK_ONLY,
        "plate_up_ratio": FieldRole.CONDITION_ONLY,
        "plate_amount_sum": FieldRole.BENCHMARK_ONLY,
        "plate_volume_sum": FieldRole.BENCHMARK_ONLY,
        "plate_peer_return_mean": FieldRole.INTERACTION_ONLY,
        "plate_peer_return_max": FieldRole.INTERACTION_ONLY,
        "plate_peer_up_ratio_mean": FieldRole.CONDITION_ONLY,
        "plate_peer_coverage_mean": FieldRole.CONDITION_ONLY,
        "plate_peer_amount_mean": FieldRole.INTERACTION_ONLY,
        "plate_peer_group_count": FieldRole.CONDITION_ONLY,
    }
    return tuple(
        FieldSpec(
            name=name,
            dtype="float64",
            family="true1min_plate_sparse",
            role=role,
            observable_clock=ObservableClock.BAR_CLOSE,
            maturity=0,
            maturity_unit="bars",
            missing_policy=MissingPolicy.PROPAGATE,
            source_fields=(name,),
            transform="pit_membership_sparse_leave_one_out",
            observable_time_field="trade_time",
        )
        for name, role in roles.items()
    )
