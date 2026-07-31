from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from our_system_phase2.services.candidate_result_schema import (
    CANDIDATE_RESULT_COLUMNS,
    candidate_result_summary_frame,
)


def test_mixed_candidate_outcomes_have_one_stable_parquet_schema(
    tmp_path: Path,
) -> None:
    rows = [
        {
            "candidate_id": "complete",
            "pair_id": "pair-1",
            "pair_member_role": "PRIMARY",
            "route_id": "SLOW_TEMPORAL_CHANGE",
            "exact_identity": "exact-1",
            "candidate_replay_status": "CANDIDATE_REPLAY_COMPLETE",
            "a_share_executable_net_reward": 0.12,
            "train_read_count": 100,
            "trade_count": 4,
            "fill_count": 4,
            "corporate_action_policy": {"fractional_shares": "FAIL_CLOSED"},
        },
        {
            "candidate_id": "terminal",
            "pair_id": "pair-1",
            "pair_member_role": "CONTROL",
            "route_id": "SLOW_TEMPORAL_CHANGE",
            "exact_identity": "exact-2",
            "candidate_replay_status": "CANDIDATE_REPLAY_BLOCKED",
            "blocker_code": "FINAL_SESSION_UNLIQUIDATED_HOLDINGS",
            "remaining_holdings": [{"code": "000001", "shares": 100}],
            "train_read_count": 100,
        },
        {
            "candidate_id": "fractional",
            "pair_id": "pair-2",
            "pair_member_role": "PRIMARY",
            "route_id": "SLOW_CROSS_SECTIONAL_LEVEL",
            "exact_identity": "exact-3",
            "candidate_replay_status": "CANDIDATE_REPLAY_BLOCKED",
            "blocker_code": "CORPORATE_ACTION_FRACTIONAL_SHARES",
            "corporate_action_fractional_share_policy": (
                "FAIL_CLOSED_NON_INTEGER"
            ),
            "train_read_count": 100,
        },
    ]
    frame = candidate_result_summary_frame(rows)
    assert tuple(frame.columns) == CANDIDATE_RESULT_COLUMNS
    path = tmp_path / "mixed.parquet"
    frame.to_parquet(path, index=False)
    observed = pd.read_parquet(path)
    assert len(observed) == 3
    payload = json.loads(
        observed.loc[
            observed["candidate_id"] == "complete", "result_payload_json"
        ].iloc[0]
    )
    assert payload["corporate_action_policy"]["fractional_shares"] == (
        "FAIL_CLOSED"
    )

