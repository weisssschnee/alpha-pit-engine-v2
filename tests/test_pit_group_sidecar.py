from __future__ import annotations

import pandas as pd
import pytest

from our_system_phase2.services.pit_group_sidecar import (
    PITGroupContract,
    cross_sectional_group_confirmation,
    membership_manifest,
    point_in_time_membership,
)


def _membership() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": ["A", "A", "B"],
            "group_id": ["I1", "I2", "I1"],
            "effective_from": pd.to_datetime(["2024-01-01", "2024-01-03", "2024-01-01"]),
            "effective_to": pd.to_datetime(["2024-01-03", None, None]),
            "source_observed_at": pd.to_datetime(
                ["2024-01-01", "2024-01-03 12:00", "2024-01-01"], format="mixed"
            ),
        }
    )


def _bars() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": ["A", "B", "A", "B"],
            "trade_time": pd.to_datetime(["2024-01-02 10:00", "2024-01-02 10:00", "2024-01-03 10:00", "2024-01-03 10:00"]),
            "ret": [0.1, -0.1, 0.2, 0.0],
        }
    )


def test_pit_membership_uses_observed_and_effective_time() -> None:
    contract = PITGroupContract("vendor", "v1", "industry", "single")
    joined = point_in_time_membership(_bars(), _membership(), contract)

    assert set(joined.loc[joined["trade_time"] < pd.Timestamp("2024-01-03"), "group_id"]) == {"I1"}
    assert not ((joined["code"] == "A") & (joined["trade_time"] == pd.Timestamp("2024-01-03 10:00"))).any()
    assert membership_manifest(_membership(), contract)["survivorship_guard"] is True


def test_cross_sectional_confirmation_is_point_in_time() -> None:
    contract = PITGroupContract("vendor", "v1", "industry", "single")
    result = cross_sectional_group_confirmation(_bars(), _membership(), contract, value_field="ret")
    first_a = result[(result["code"] == "A") & (result["trade_time"] == pd.Timestamp("2024-01-02 10:00"))].iloc[0]

    assert first_a["group_member_count"] == 2
    assert first_a["peer_value_mean"] == pytest.approx(-0.1)


def test_placeholder_plate_or_industry_ids_are_rejected() -> None:
    bad = _membership()
    bad.loc[0, "group_id"] = "0"
    with pytest.raises(ValueError, match="placeholder"):
        membership_manifest(bad, PITGroupContract("vendor", "v1", "industry", "single"))


def test_snapshot_exit_is_not_applied_before_it_is_observed() -> None:
    membership = pd.DataFrame(
        {
            "code": ["A", "A"],
            "group_id": ["I1", "I2"],
            "effective_from": pd.to_datetime(["2024-01-01", "2024-01-03"]),
            "effective_to": pd.to_datetime(["2024-01-03", None]),
            "source_observed_at": pd.to_datetime(["2024-01-01", "2024-01-03 12:00"], format="mixed"),
            "source_observed_to": pd.to_datetime(["2024-01-03 12:00", None], format="mixed"),
        }
    )
    bars = pd.DataFrame(
        {
            "code": ["A", "A"],
            "trade_time": pd.to_datetime(["2024-01-03 10:00", "2024-01-03 13:00"]),
        }
    )

    joined = point_in_time_membership(
        bars,
        membership,
        PITGroupContract("vendor", "v1", "industry", "single"),
    )

    assert joined["group_id"].tolist() == ["I1", "I2"]
