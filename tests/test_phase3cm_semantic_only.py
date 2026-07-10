from __future__ import annotations

import pandas as pd

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    _build_eval_time_index,
    _candidate_portfolio_rows_from_frame,
)


def test_semantic_only_candidate_evaluation_skips_portfolio_rows() -> None:
    trade_times = pd.to_datetime(
        [
            "2026-01-05 09:30:00",
            "2026-01-05 09:30:00",
            "2026-01-05 09:31:00",
            "2026-01-05 09:31:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "code": ["A", "B", "A", "B"],
            "trade_time": trade_times,
            "date": pd.to_datetime(["2026-01-05"] * 4),
            "close": [10.0, 20.0, 10.1, 19.9],
            "x": [1.0, 3.0, 2.0, 4.0],
        }
    )
    eval_mask = pd.Series([True] * len(frame))
    eval_frame = frame.copy()
    diagnostics: dict[str, object] = {}

    rows = _candidate_portfolio_rows_from_frame(
        candidate={
            "candidate_id": "candidate-a",
            "expression": "CSRank($x)",
            "expression_hash": "hash-a",
            "max_window": 0,
            "open_direction": "long_top",
        },
        frame=frame,
        eval_mask=eval_mask,
        eval_frame=eval_frame,
        labels=pd.DataFrame(index=frame.index),
        eval_time_index=_build_eval_time_index(eval_frame),
        split_by_time={pd.Timestamp(value): "train" for value in trade_times},
        context_times_by_window={0: set(pd.to_datetime(trade_times))},
        shard_index=0,
        horizons=(1,),
        min_obs=2,
        cost_bps=5.0,
        top_quantile=0.2,
        portfolio_mode="long_only_top",
        expression_cache={},
        feature_matrix_cache={},
        operator_cache_by_window={},
        cache_stats={},
        operator_cache_max_entries=32,
        feature_matrix_cache_max_windows=2,
        persistent_cache_root=None,
        persistent_cache_mode="off",
        persistent_expression_cache=False,
        persistent_operator_cache=False,
        persistent_feature_matrix_cache=False,
        persistent_cache_min_free_gb=2.0,
        persistent_cache_max_gb=1.0,
        persistent_cache_ttl_days=1.0,
        persistent_cache_scope={},
        semantic_only=True,
        semantic_diagnostics=diagnostics,
        semantic_sketch_size=32,
    )

    assert rows == []
    assert diagnostics["signal_finite_count"] == 4
    assert diagnostics["signal_is_constant"] is False
    assert diagnostics["signal_rank_hash"]
    assert diagnostics["signal_rank_sketch"]
