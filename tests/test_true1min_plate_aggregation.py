from __future__ import annotations

import pandas as pd
import pytest

from our_system_phase2.services.feature_state_fabric import FieldRole
from our_system_phase2.services.pit_group_sidecar import (
    PITGroupContract,
    membership_content_sha256,
)
from our_system_phase2.services.true1min_plate_aggregation import (
    aggregate_true1min_plate,
    true1min_plate_field_specs,
)


def _membership(*, late_b: bool = False) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": ["000001", "000002", "000001", "000003"],
            "group_id": ["G1", "G1", "G2", "G2"],
            "effective_from": pd.to_datetime(["2025-01-02"] * 4),
            "effective_to": [pd.NaT] * 4,
            "source_observed_at": pd.to_datetime(
                ["2025-01-03", "2025-01-06" if late_b else "2025-01-03", "2025-01-03", "2025-01-03"]
            ),
            "source_observed_to": [pd.NaT] * 4,
        }
    )


def _bars() -> pd.DataFrame:
    rows = []
    for minute, returns in (("09:31", [0.1, 0.2, -0.1]), ("09:32", [0.0, 0.1, 0.2])):
        for index, value in enumerate(returns, start=1):
            rows.append(
                {
                    "code": f"{index:06d}.SZ",
                    "trade_time": pd.Timestamp(f"2025-01-03 {minute}"),
                    "ret_1m": value,
                    "amount_yuan": float(index * 100),
                    "volume": float(index * 10),
                }
            )
    return pd.DataFrame(rows)


def _manifest(membership: pd.DataFrame) -> dict[str, object]:
    contract = PITGroupContract("test", "v1", "plate", "multi")
    return {
        "source_name": "test",
        "source_version": "v1",
        "group_type": "plate",
        "membership_policy": "multi",
        "row_count": len(membership),
        "membership_sha256": membership_content_sha256(membership, contract),
        "survivorship_guard": True,
        "reward_or_performance_used": False,
        "coverage_gate": "PASS",
        "required_start": "2025-01-01",
        "required_end": "2025-12-31",
    }


def _aggregate(
    bars: pd.DataFrame,
    membership: pd.DataFrame,
    *,
    minimum_active_peers: int = 1,
):
    return aggregate_true1min_plate(
        bars,
        membership,
        membership_manifest=_manifest(membership),
        data_role="development",
        minimum_active_peers=minimum_active_peers,
    )


def test_sparse_group_aggregation_and_leave_one_out_are_exact() -> None:
    result = _aggregate(_bars(), _membership())
    first = result.group_panel[result.group_panel["trade_time"].eq(pd.Timestamp("2025-01-03 09:31"))]
    g1 = first[first["group_id"].eq("G1")].iloc[0]
    g2 = first[first["group_id"].eq("G2")].iloc[0]

    assert g1["plate_member_count"] == 2
    assert g1["plate_active_count"] == 2
    assert g1["plate_return_equal"] == pytest.approx(0.15)
    assert g1["plate_up_ratio"] == 1.0
    assert g2["plate_return_equal"] == pytest.approx(0.0)
    assert g2["plate_up_ratio"] == 0.5

    stock = result.stock_context[result.stock_context["trade_time"].eq(pd.Timestamp("2025-01-03 09:31"))]
    by_code = stock.set_index("code")
    assert by_code.loc["000001", "plate_peer_return_mean"] == pytest.approx(0.05)
    assert by_code.loc["000001", "plate_peer_return_max"] == pytest.approx(0.2)
    assert by_code.loc["000001", "plate_peer_group_count"] == 2
    assert by_code.loc["000002", "plate_peer_return_mean"] == pytest.approx(0.1)
    assert by_code.loc["000003", "plate_peer_return_mean"] == pytest.approx(0.1)


def test_membership_observable_clock_hides_late_constituent() -> None:
    result = _aggregate(_bars(), _membership(late_b=True))
    g1 = result.group_panel[
        result.group_panel["group_id"].eq("G1")
        & result.group_panel["trade_time"].eq(pd.Timestamp("2025-01-03 09:31"))
    ].iloc[0]
    b = result.stock_context[
        result.stock_context["code"].eq("000002")
        & result.stock_context["trade_time"].eq(pd.Timestamp("2025-01-03 09:31"))
    ].iloc[0]

    assert g1["plate_member_count"] == 1
    assert pd.isna(b["plate_peer_return_mean"])
    assert result.diagnostics["active_membership_edge_count"] == 3


def test_unobserved_exit_does_not_leak_future_membership_change() -> None:
    membership = _membership()
    membership.loc[membership["code"].eq("000002"), "effective_to"] = pd.Timestamp(
        "2025-01-03 09:00"
    )

    result = _aggregate(_bars(), membership)
    g1 = result.group_panel[
        result.group_panel["group_id"].eq("G1")
        & result.group_panel["trade_time"].eq(pd.Timestamp("2025-01-03 09:31"))
    ].iloc[0]

    assert g1["plate_member_count"] == 2
    assert g1["plate_active_count"] == 2


def test_intraday_membership_transition_fails_closed() -> None:
    membership = _membership()
    membership.loc[membership["code"].eq("000002"), "source_observed_at"] = pd.Timestamp(
        "2025-01-03 09:32"
    )

    with pytest.raises(ValueError, match="intraday membership transition"):
        _aggregate(_bars(), membership)


def test_survivorship_manifest_is_required() -> None:
    membership = _membership()
    manifest = _manifest(membership)
    manifest["survivorship_guard"] = False

    with pytest.raises(ValueError, match="survivorship guard"):
        aggregate_true1min_plate(
            _bars(),
            membership,
            membership_manifest=manifest,
            data_role="development",
        )


def test_materialization_is_input_order_invariant() -> None:
    one = _aggregate(_bars(), _membership())
    shuffled_membership = _membership().sample(frac=1, random_state=11)
    two = _aggregate(_bars().sample(frac=1, random_state=7), shuffled_membership)

    pd.testing.assert_frame_equal(one.group_panel, two.group_panel)
    pd.testing.assert_frame_equal(one.stock_context, two.stock_context)
    assert one.diagnostics["output_fingerprint"] == two.diagnostics["output_fingerprint"]


def test_sealed_or_multi_session_input_fails_closed() -> None:
    sealed = _bars()
    sealed["trade_time"] = sealed["trade_time"] + pd.DateOffset(years=1)
    with pytest.raises(ValueError, match="sealed 2026"):
        _aggregate(sealed, _membership())

    multi = pd.concat([_bars(), _bars().assign(trade_time=pd.Timestamp("2025-01-06 09:31"))])
    with pytest.raises(ValueError, match="one execution session"):
        _aggregate(multi, _membership())


def test_true1min_plate_fields_are_not_primary_search_fields() -> None:
    specs = {spec.name: spec for spec in true1min_plate_field_specs()}

    assert specs["plate_return_equal"].role is FieldRole.BENCHMARK_ONLY
    assert specs["plate_peer_return_mean"].role is FieldRole.INTERACTION_ONLY
    assert specs["plate_peer_up_ratio_mean"].role is FieldRole.CONDITION_ONLY
    assert all(spec.role is not FieldRole.PRIMARY for spec in specs.values())


def test_coverage_denominator_includes_members_without_bars() -> None:
    membership = pd.concat(
        [
            _membership(),
            pd.DataFrame(
                {
                    "code": ["000004"],
                    "group_id": ["G1"],
                    "effective_from": pd.to_datetime(["2025-01-02"]),
                    "effective_to": [pd.NaT],
                    "source_observed_at": pd.to_datetime(["2025-01-03"]),
                    "source_observed_to": [pd.NaT],
                }
            ),
        ],
        ignore_index=True,
    )

    result = _aggregate(_bars(), membership)
    g1 = result.group_panel[
        result.group_panel["group_id"].eq("G1")
        & result.group_panel["trade_time"].eq(pd.Timestamp("2025-01-03 09:31"))
    ].iloc[0]

    assert g1["plate_member_count"] == 3
    assert g1["plate_active_count"] == 2
    assert g1["plate_coverage"] == pytest.approx(2 / 3)
    assert result.diagnostics["bar_code_count"] == 3
    assert result.diagnostics["membership_code_count"] == 4


def test_peer_amount_mean_uses_amount_observation_count() -> None:
    membership = _membership()
    membership.loc[membership["code"].eq("000003"), "group_id"] = "G1"
    bars = _bars()
    row = bars["code"].isin(["000002.SZ", "000003.SZ"]) & bars["trade_time"].eq(
        pd.Timestamp("2025-01-03 09:31")
    )
    bars.loc[row, "ret_1m"] = float("nan")
    bars.loc[
        bars["code"].eq("000003.SZ")
        & bars["trade_time"].eq(pd.Timestamp("2025-01-03 09:31")),
        "amount_yuan",
    ] = 900.0

    result = _aggregate(bars, membership)
    stock = result.stock_context[
        result.stock_context["code"].eq("000001")
        & result.stock_context["trade_time"].eq(pd.Timestamp("2025-01-03 09:31"))
    ].iloc[0]

    assert stock["plate_peer_amount_mean"] == pytest.approx(550.0)
