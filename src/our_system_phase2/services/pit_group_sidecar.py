"""Point-in-time plate/industry membership and cross-sectional sidecars."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


PIT_GROUP_VERSION = "nextgen_dark_pit_group_sidecar_v2"
PLACEHOLDER_GROUPS = {"", "0", "00", "000000", "none", "null", "unknown", "placeholder"}


@dataclass(frozen=True, slots=True)
class PITGroupContract:
    source_name: str
    source_version: str
    group_type: str
    membership_policy: str

    def validate(self) -> None:
        if self.group_type not in {"plate", "industry"}:
            raise ValueError(f"unsupported group type: {self.group_type}")
        if self.membership_policy not in {"single", "multi"}:
            raise ValueError(f"unsupported membership policy: {self.membership_policy}")
        if self.group_type == "industry" and self.membership_policy != "single":
            raise ValueError("industry membership must be single-valued at a point in time")


def _canonical_membership(membership: pd.DataFrame, contract: PITGroupContract) -> pd.DataFrame:
    contract.validate()
    required = {
        "code", "group_id", "effective_from", "effective_to", "source_observed_at",
    }
    missing = sorted(required - set(membership.columns))
    if missing:
        raise ValueError(f"membership missing columns: {missing}")
    out = membership.copy()
    out["code"] = out["code"].astype(str)
    out["group_id"] = out["group_id"].astype(str).str.strip()
    bad = out["group_id"].str.lower().isin(PLACEHOLDER_GROUPS)
    if bad.any():
        raise ValueError(f"placeholder group IDs are forbidden: {sorted(out.loc[bad, 'group_id'].unique())}")
    if "source_observed_to" not in out.columns:
        out["source_observed_to"] = pd.NaT
    for column in ("effective_from", "effective_to", "source_observed_at", "source_observed_to"):
        out[column] = pd.to_datetime(out[column], errors="coerce", format="mixed")
    if out[["effective_from", "source_observed_at"]].isna().any().any():
        raise ValueError("effective_from/source_observed_at must be valid timestamps")
    if (out["source_observed_at"] > out["effective_from"]).any():
        # A source may arrive after the economic effective date, but it cannot
        # be used until observed.  Keep both clocks and let the join enforce max.
        pass
    invalid_end = out["effective_to"].notna() & (out["effective_to"] <= out["effective_from"])
    if invalid_end.any():
        raise ValueError("effective_to must be after effective_from")
    invalid_exit_observation = out["source_observed_to"].notna() & out["effective_to"].isna()
    if invalid_exit_observation.any():
        raise ValueError("source_observed_to requires effective_to")
    out["group_type"] = contract.group_type
    out["source_name"] = contract.source_name
    out["source_version"] = contract.source_version
    out = out.sort_values(["code", "effective_from", "group_id"], kind="mergesort").reset_index(drop=True)
    if contract.membership_policy == "single":
        for _, rows in out.groupby("code", sort=False):
            previous_end = pd.Timestamp.min
            for row in rows.itertuples(index=False):
                start = max(row.effective_from, row.source_observed_at)
                if start < previous_end:
                    raise ValueError(f"overlapping single-valued membership for {row.code}")
                if pd.isna(row.effective_to):
                    previous_end = pd.Timestamp.max
                elif pd.notna(row.source_observed_to):
                    previous_end = max(row.effective_to, row.source_observed_to)
                else:
                    previous_end = row.effective_to
    return out


def canonical_membership(membership: pd.DataFrame, contract: PITGroupContract) -> pd.DataFrame:
    """Return the stable normal form used by joins, releases, and content hashes."""

    return _canonical_membership(membership, contract)


def membership_content_sha256(membership: pd.DataFrame, contract: PITGroupContract) -> str:
    canonical = _canonical_membership(membership, contract)
    payload = canonical.to_json(orient="records", date_format="iso", date_unit="ns")
    return hashlib.sha256(payload.encode()).hexdigest()


def membership_manifest(membership: pd.DataFrame, contract: PITGroupContract) -> dict[str, Any]:
    canonical = _canonical_membership(membership, contract)
    return {
        "sidecar_version": PIT_GROUP_VERSION,
        "source_name": contract.source_name,
        "source_version": contract.source_version,
        "group_type": contract.group_type,
        "membership_policy": contract.membership_policy,
        "row_count": len(canonical),
        "membership_sha256": membership_content_sha256(canonical, contract),
        "survivorship_guard": True,
        "reward_or_performance_used": False,
    }


def point_in_time_membership(
    bars: pd.DataFrame,
    membership: pd.DataFrame,
    contract: PITGroupContract,
) -> pd.DataFrame:
    canonical = _canonical_membership(membership, contract)
    required = {"code", "trade_time"}
    if not required <= set(bars.columns):
        raise ValueError(f"bars need columns: {sorted(required)}")
    base = bars.copy()
    base["code"] = base["code"].astype(str)
    base["trade_time"] = pd.to_datetime(base["trade_time"], errors="coerce", format="mixed")
    base["_bar_row_id"] = np.arange(len(base))
    joined = base.merge(canonical, on="code", how="left", validate="many_to_many")
    observable_from = joined[["effective_from", "source_observed_at"]].max(axis=1)
    observable_to = joined["effective_to"].copy()
    has_exit_observation = joined["source_observed_to"].notna()
    observable_to.loc[has_exit_observation] = joined.loc[
        has_exit_observation, ["effective_to", "source_observed_to"]
    ].max(axis=1)
    active = (
        joined["group_id"].notna()
        & joined["trade_time"].ge(observable_from)
        & (observable_to.isna() | joined["trade_time"].lt(observable_to))
    )
    out = joined.loc[active].copy()
    if contract.membership_policy == "single" and out.duplicated("_bar_row_id").any():
        raise ValueError("single-valued PIT join produced multiple memberships")
    return out.drop(columns=["_bar_row_id"]).sort_values(
        ["trade_time", "code", "group_id"], kind="mergesort"
    ).reset_index(drop=True)


def cross_sectional_group_confirmation(
    bars: pd.DataFrame,
    membership: pd.DataFrame,
    contract: PITGroupContract,
    *,
    value_field: str,
) -> pd.DataFrame:
    if value_field not in bars.columns:
        raise KeyError(value_field)
    joined = point_in_time_membership(bars, membership, contract)
    values = pd.to_numeric(joined[value_field], errors="coerce")
    keys = ["trade_time", "group_type", "group_id"]
    joined["_value"] = values
    group = joined.groupby(keys, sort=False)["_value"]
    joined["group_member_count"] = group.transform("count")
    joined["group_value_mean"] = group.transform("mean")
    joined["group_up_ratio"] = joined.assign(_up=(values > 0).astype(float)).groupby(keys, sort=False)["_up"].transform("mean")
    total = group.transform("sum")
    count = group.transform("count")
    joined["peer_value_mean"] = (total - values) / (count - 1).replace(0, np.nan)
    columns = [
        "code", "trade_time", "group_type", "group_id", "source_name", "source_version",
        "group_member_count", "group_value_mean", "peer_value_mean", "group_up_ratio",
    ]
    return joined[columns].sort_values(["trade_time", "group_id", "code"], kind="mergesort").reset_index(drop=True)
