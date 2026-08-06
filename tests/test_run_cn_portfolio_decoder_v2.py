from __future__ import annotations

import pandas as pd
import pytest

from scripts import run_cn_portfolio_decoder_v2 as subject


def test_decoder_v2_contract_is_exact_three_by_one() -> None:
    assert [policy.decoder_id for policy in subject.DECODER_POLICIES] == [
        "CURRENT_TOP20PCT_EQUAL",
        "TOPK_10_EQUAL",
        "TOPK_10_RANK",
    ]
    assert all(
        policy.target_refresh_clock
        == "EACH_SESSION_OPEN_FROM_PRIOR_CLOSE_SIGNAL"
        for policy in subject.DECODER_POLICIES
    )
    assert all(
        policy.session_end_policy == "FINAL_CLOSE_MARK_NO_FORCED_SALE"
        for policy in subject.DECODER_POLICIES
    )


def test_decoder_v2_uses_closed_ledger_manifest_field() -> None:
    subject._require_persisted_accounting_ledgers(
        {"accounting_ledgers_persisted": True}
    )
    with pytest.raises(RuntimeError, match="persisted accounting ledgers"):
        subject._require_persisted_accounting_ledgers(
            {"persist_accounting_ledgers": True}
        )


def test_process_executor_contract_is_bounded_by_cpu_entitlement() -> None:
    assert (
        subject._validate_executor_contract(
            execution_backend="PROCESS_POOL",
            entitlement_worker_count=32,
            executor_worker_count=12,
        )
        == "PROCESS_POOL"
    )
    with pytest.raises(ValueError, match="cannot exceed"):
        subject._validate_executor_contract(
            execution_backend="PROCESS_POOL",
            entitlement_worker_count=8,
            executor_worker_count=12,
        )
    with pytest.raises(ValueError, match="execution_backend"):
        subject._validate_executor_contract(
            execution_backend="UNBOUNDED",
            entitlement_worker_count=32,
            executor_worker_count=12,
        )


def test_uninitialized_process_worker_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="was not initialized"):
        subject._evaluate_candidate_in_process({"candidate_id": "candidate-1"})


def test_execution_price_sidecar_attaches_only_on_exact_coordinate_and_close_parity(
    tmp_path,
) -> None:
    feature = pd.DataFrame(
        {
            "trade_time": pd.to_datetime(
                ["2025-01-02 15:00", "2025-01-03 15:00"]
            ),
            "date": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "code": ["000001", "000001"],
            "close": [10.5, 11.5],
        }
    )
    price_path = tmp_path / "price.parquet"
    pd.DataFrame(
        {
            "trade_time": pd.to_datetime(
                ["2025-01-02 15:00", "2025-01-03 15:00"]
            ),
            "code": ["000001", "000001"],
            "open": [10.0, 11.0],
            "close": [10.5, 11.5],
        }
    ).to_parquet(price_path, index=False)
    manifest = {
        "source_shard_count": 1,
        "shards": [{"output_path": str(price_path)}],
    }

    observed = subject._attach_execution_prices(feature, manifest)

    assert observed["open"].tolist() == [10.0, 11.0]

    drifted = pd.read_parquet(price_path)
    drifted.loc[1, "close"] = 11.4
    drifted.to_parquet(price_path, index=False)
    with pytest.raises(RuntimeError, match="close parity drift"):
        subject._attach_execution_prices(
            feature.drop(columns=["open"]),
            manifest,
        )


def test_baseline_parity_accepts_exact_64_member_ledger() -> None:
    metrics = []
    baseline = []
    for index in range(subject.EXPECTED_MEMBER_COUNT):
        candidate_id = f"candidate-{index:03d}"
        row = {
            "candidate_id": candidate_id,
            "decoder_id": "CURRENT_TOP20PCT_EQUAL",
            "ending_nav_cny": 1_000_000.0 + index,
            "continuous_book_net_reward": 0.1 + index / 1000,
            "cumulative_net_return": index / 1_000_000,
            "total_fees_cny": 10.0,
            "fill_count": 2,
            "blocked_buy_count": 0,
            "blocked_sell_count": 0,
            "cumulative_realized_trade_pnl_cny": float(index),
            "ending_unrealized_pnl_cny": 0.0,
        }
        metrics.append(row)
        baseline.append(
            {
                **{
                    key: value
                    for key, value in row.items()
                    if key
                    not in {"decoder_id", "continuous_book_net_reward"}
                },
                "mark_to_market_net_reward": row[
                    "continuous_book_net_reward"
                ],
            }
        )

    parity = subject._baseline_parity(
        pd.DataFrame(metrics), pd.DataFrame(baseline)
    )

    assert len(parity) == subject.EXPECTED_MEMBER_COUNT
    assert parity["parity_status"].eq("PASS").all()
    assert parity.filter(like="_delta").abs().sum().sum() == 0.0


def test_baseline_parity_accepts_bounded_qualification_subset() -> None:
    metrics = pd.DataFrame(
        [
            {
                "candidate_id": "candidate-001",
                "decoder_id": "CURRENT_TOP20PCT_EQUAL",
                "ending_nav_cny": 1_000_001.0,
                "continuous_book_net_reward": 0.1,
                "cumulative_net_return": 0.000001,
                "total_fees_cny": 10.0,
                "fill_count": 2,
                "blocked_buy_count": 0,
                "blocked_sell_count": 0,
                "cumulative_realized_trade_pnl_cny": 1.0,
                "ending_unrealized_pnl_cny": 0.0,
            }
        ]
    )
    baseline = metrics.drop(columns=["decoder_id"]).rename(
        columns={
            "continuous_book_net_reward": "mark_to_market_net_reward"
        }
    )

    parity = subject._baseline_parity(
        metrics, baseline, expected_member_count=1
    )

    assert len(parity) == 1
    assert parity["parity_status"].eq("PASS").all()


def test_pair_metrics_keep_absolute_and_matched_axes_separate() -> None:
    frame = pd.DataFrame(
        [
            {
                "pair_id": "pair-1",
                "candidate_id": "primary",
                "pair_member_role": "PRIMARY",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "decoder_id": "TOPK_10_EQUAL",
                "continuous_book_net_reward": 0.4,
                "cumulative_net_return": 0.03,
                "cumulative_realized_trade_pnl_cny": 20_000.0,
                "ending_unrealized_pnl_cny": 10_000.0,
                "total_fees_cny": 100.0,
                "mean_one_way_turnover": 0.2,
                "net_return_per_turnover": 0.15,
            },
            {
                "pair_id": "pair-1",
                "candidate_id": "control",
                "pair_member_role": "CONTROL",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "decoder_id": "TOPK_10_EQUAL",
                "continuous_book_net_reward": 0.1,
                "cumulative_net_return": 0.01,
                "cumulative_realized_trade_pnl_cny": 8_000.0,
                "ending_unrealized_pnl_cny": 2_000.0,
                "total_fees_cny": 80.0,
                "mean_one_way_turnover": 0.15,
                "net_return_per_turnover": 0.067,
            },
        ]
    )

    pair = subject._pair_metrics(frame).iloc[0]

    assert pair["primary_absolute_positive"]
    assert pair["matched_increment_positive"]
    assert pair["both_economic_gates_positive"]
    assert pair["matched_cumulative_net_return_increment"] == pytest.approx(
        0.02
    )
    assert not bool(pair["promotion_authorized"])
