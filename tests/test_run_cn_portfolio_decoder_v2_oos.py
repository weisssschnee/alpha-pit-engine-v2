from pathlib import Path

import pandas as pd

from scripts import run_cn_portfolio_decoder_v2_oos as oos


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _metric(candidate_id: str, pair_id: str, role: str, value: float) -> dict:
    return {
        "pair_id": pair_id,
        "candidate_id": candidate_id,
        "pair_member_role": role,
        "continuous_book_net_reward": value,
        "cumulative_net_return": value / 10.0,
        "mean_one_way_turnover": 0.25,
        "net_return_per_turnover": value / 2.5,
        "total_fees_cny": 100.0,
        "cumulative_realized_trade_pnl_cny": value * 1_000.0,
        "ending_unrealized_pnl_cny": value * 100.0,
        "ending_holdings_weight": 0.2,
        "daily_net_return_p10": -0.01,
        "daily_net_return_worst": -0.02,
        "quarterly_regime_positive_share": 0.75,
        "quarterly_regime_worst_return": -0.03,
        "signal_rank_ic_mean": value / 100.0,
    }


def test_pair_metrics_preserve_order_and_apply_all_four_gates() -> None:
    pairs = pd.DataFrame(
        [
            {
                "finalist_order": 1,
                "pair_id": "p1",
                "primary_candidate_id": "a",
                "control_candidate_id": "b",
            },
            {
                "finalist_order": 2,
                "pair_id": "p2",
                "primary_candidate_id": "c",
                "control_candidate_id": "d",
            },
        ]
    )
    metrics = pd.DataFrame(
        [
            _metric("d", "p2", "CONTROL", 0.5),
            _metric("a", "p1", "PRIMARY", 2.0),
            _metric("c", "p2", "PRIMARY", 0.2),
            _metric("b", "p1", "CONTROL", 1.0),
        ]
    )
    result = oos._pair_metrics(pairs=pairs, candidate_metrics=metrics)
    assert result["pair_id"].tolist() == ["p1", "p2"]
    assert result["all_four_economic_gates_positive"].tolist() == [True, False]
    assert result["matched_continuous_book_net_reward_increment"].tolist() == [
        1.0,
        -0.3,
    ]
    assert result["interstage_filter_applied"].eq(False).all()
    assert result["promotion_authorized"].eq(False).all()


def test_daily_distribution_reports_left_tail_and_quarterly_regime() -> None:
    result = oos._daily_distribution(
        {
            "daily": pd.DataFrame(
                {
                    "date": [
                        "2025-01-02",
                        "2025-01-03",
                        "2025-04-01",
                        "2025-04-02",
                    ],
                    "daily_net_return": [0.02, -0.01, 0.03, 0.01],
                }
            )
        }
    )
    assert result["daily_net_return_worst"] == -0.01
    assert result["daily_net_return_positive_share"] == 0.75
    assert result["quarterly_regime_count"] == 2
    assert result["quarterly_regime_positive_share"] == 1.0


def test_remote_launcher_binds_qualified_process_profile() -> None:
    script = (
        PROJECT_ROOT / "scripts" / "run_cn_portfolio_decoder_v2_oos_77o.ps1"
    ).read_text(encoding="utf-8")
    assert "VALIDATION_EXCLUSIVE_32" in script
    assert "[int]$WorkerCount = 32" in script
    assert "[int]$ExecutorWorkerCount = 12" in script
    assert "$env:OMP_NUM_THREADS = '1'" in script
    assert "minimum_free_memory_bytes" in script
    assert "interstage_filter_applied = $false" in script
    assert "promotion = 'FORBIDDEN'" in script
    assert "--expected-selection-payload-sha256" in script
    assert "--expected-decoder-policy-sha256" in script


def test_oos_contract_is_one_decoder_and_report_only() -> None:
    assert oos.DECODER_ID == "TOPK_10_EQUAL"
    assert oos.POLICY.selection == "TOP_K"
    assert oos.POLICY.top_k == 10
    assert oos.POLICY.weighting == "EQUAL"
    assert oos.EVIDENCE_SCOPE == "ADAPTIVE_REPORT_ONLY_VALIDATION_OOS"
