from pathlib import Path

import pandas as pd
import pytest

from scripts import build_cn_validation_session_authority as session
from scripts import run_cn_fixed_survivor_forward_2026 as forward
from our_system_phase2.services.typed_temporal_program import (
    TemporalInput,
    evaluate_typed_temporal_primitive,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_forward_role_is_explicit_but_holdout_remains_rejected() -> None:
    frame = pd.DataFrame(
        {
            "code": ["000001", "000001"],
            "trade_time": pd.to_datetime(["2026-01-05", "2026-01-06"]),
        }
    )
    typed = TemporalInput(
        values=pd.Series([1.0, 2.0]),
        value_type="numeric",
        observable_at=frame["trade_time"],
    )
    result = evaluate_typed_temporal_primitive(
        frame, "Delta", [typed], [1], data_role="forward_2026_report_only"
    )
    assert result.values.isna().sum() == 1
    with pytest.raises(PermissionError):
        evaluate_typed_temporal_primitive(
            frame, "Delta", [typed], [1], data_role="holdout_report_only"
        )


def test_forward_field_manifest_role_and_calendar_are_fail_closed(tmp_path: Path) -> None:
    shard = tmp_path / "shard_00.parquet"
    pd.DataFrame(
        {
            "trade_time": pd.to_datetime(["2026-01-05 15:00", "2026-01-06 15:00"]),
            "code": ["000001", "000001"],
            "open": [10.0, 10.1],
            "close": [10.1, 10.2],
            "source_shard": [0, 0],
        }
    ).to_parquet(shard, index=False)
    manifest_path = tmp_path / "manifest.json"
    manifest = {
        "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
        "evaluation_role": "forward_2026",
        "data_role": "forward_2026_report_only",
        "eligible_forward_2026_date_count": 2,
        "source_shard_count": 1,
        "sidecar_rows": 2,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 2,
        "shards": [
            {
                "output_path": str(shard),
                "output_sha256": session.v1._sha256(shard),
                "rows": 2,
            }
        ],
    }
    session.v1._write_json(manifest_path, manifest)
    observed, loaded = session._load_field_sessions(
        field_manifest_path=manifest_path,
        expected_sha256=session.v1._sha256(manifest_path),
        evaluation_role="forward_2026",
        date_min="2026-01-05",
        date_max="2026-01-06",
    )
    assert len(observed) == 2
    assert loaded["forward_2026_reads"] == 2
    manifest["validation_reads"] = 1
    session.v1._write_json(manifest_path, manifest)
    with pytest.raises(RuntimeError, match="crossed validation_reads"):
        session._load_field_sessions(
            field_manifest_path=manifest_path,
            expected_sha256=session.v1._sha256(manifest_path),
            evaluation_role="forward_2026",
            date_min="2026-01-05",
            date_max="2026-01-06",
        )


def _metric(candidate_id: str, value: float) -> dict:
    return {
        "candidate_id": candidate_id,
        "continuous_book_net_reward": value,
        "cumulative_net_return": value / 10.0,
        "mean_one_way_turnover": 0.2,
        "net_return_per_turnover": value / 2.0,
        "total_fees_cny": 10.0,
        "cumulative_realized_trade_pnl_cny": value * 100.0,
        "ending_unrealized_pnl_cny": value * 10.0,
        "ending_holdings_weight": 0.1,
        "daily_net_return_p10": -0.01,
        "daily_net_return_worst": -0.02,
        "quarterly_regime_positive_share": 1.0,
        "quarterly_regime_worst_return": 0.01,
        "signal_rank_ic_mean": value / 100.0,
    }


def test_forward_pair_metrics_preserve_confirmation_order() -> None:
    pairs = pd.DataFrame(
        [
            {
                "confirmation_order": 1,
                "finalist_order": 2,
                "pair_id": "p1",
                "primary_candidate_id": "a",
                "control_candidate_id": "b",
                "search_score": 0.1,
                "train_rank_ic_mean": 0.01,
                "primary_cumulative_net_return": 0.2,
            }
        ]
    )
    result = forward._pair_metrics(
        pairs, pd.DataFrame([_metric("a", 2.0), _metric("b", 1.0)])
    )
    assert result["confirmation_order"].tolist() == [1]
    assert result["source_finalist_order"].tolist() == [2]
    assert result["all_four_economic_gates_positive"].tolist() == [True]
    assert result["promotion_authorized"].eq(False).all()


def test_forward_runner_and_launcher_bind_one_shot_boundaries() -> None:
    runner = (PROJECT_ROOT / "scripts" / "run_cn_fixed_survivor_forward_2026.py").read_text(
        encoding="utf-8"
    )
    launcher = (PROJECT_ROOT / "scripts" / "run_cn_fixed_survivor_forward_2026_77o.ps1").read_text(
        encoding="utf-8"
    )
    assert 'EVALUATION_ROLE = "forward_2026"' in runner
    assert 'DATA_ROLE = "forward_2026_report_only"' in runner
    assert 'EXPECTED_PAIR_COUNT = 10' in runner
    assert 'EXPECTED_MEMBER_COUNT = 20' in runner
    assert '"optimizer_feedback_write": "FORBIDDEN"' in runner
    assert '"promotion": "FORBIDDEN"' in runner
    assert "VALIDATION_EXCLUSIVE_32" in launcher
    assert "[int]$ExecutorWorkerCount = 12" in launcher
    assert "forward_access_started.json" in launcher
    assert "--evaluation-role forward_2026" in launcher
    assert "--max-shards 63" in launcher
    assert "preflight_forward_financial_rows_read = 0" in launcher
    assert "$dateDifferences = @(Compare-Object $sourceDates $splitDates)" in launcher
    assert "$dateDifferences.Count -ne 0" in launcher
