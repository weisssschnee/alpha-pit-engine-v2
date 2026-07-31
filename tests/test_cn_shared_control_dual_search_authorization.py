from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.freeze_cn_shared_control_dual_search_authorization import (
    freeze_authorization,
)


def _write(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _hash(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def test_freeze_binds_budget_history_and_dual_profile(tmp_path: Path) -> None:
    source = _write(
        tmp_path / "source.json",
        {
            "execution_authorized": True,
            "campaign_profile": "cn_winner_guided_continuation_search_v1",
            "active_threads": 32,
            "session_threads": 32,
        },
    )
    contract = _write(
        tmp_path / "contract.json",
        {
            "status": "FROZEN_USER_AUTHORIZED_QUALIFICATION_REQUIRED_BEFORE_LAUNCH",
            "authority_source": "USER",
            "shared_resource_authority": {
                "search_profile": "SEARCH_DUAL_24",
                "total_cpu_threads": 32,
            },
            "search_lane": {
                "campaign_id": "campaign",
                "campaign_profile": "cn_winner_guided_continuation_search_v1",
                "checkpoint_count": 2,
                "formal_fresh_exact_asks_per_checkpoint": 12,
                "maximum_formal_fresh_exact_asks": 24,
                "seed_base": 7,
                "fixed_route_formal_asks_per_checkpoint": {"ROUTE": 12},
                "final_route_formal_ask_allocation": {"ROUTE": 24},
                "throughput_contract": {
                    "maximum_wall_seconds": 100,
                    "first_checkpoint_minimum_pair_evaluated_per_hour": 3,
                    "first_checkpoint_minimum_entitlement_occupancy": 0.7,
                },
            },
        },
    )
    capacity_body = {
        "profiles": {
            "SEARCH_DUAL_24": {"role": "SEARCH", "cpu_threads": 24}
        }
    }
    capacity = _write(
        tmp_path / "capacity.json",
        {
            **capacity_body,
            "capacity_manifest_sha256": _hash(capacity_body),
        },
    )
    candidate = tmp_path / "candidate.parquet"
    behavior = tmp_path / "behavior.parquet"
    candidate.write_bytes(b"candidate")
    behavior.write_bytes(b"behavior")
    history = _write(
        tmp_path / "history.json",
        {
            "status": "PASS",
            "merge_policy": "IDENTITY_ONLY_NO_REWARD_NO_SCHEDULER_STATE",
            "candidate_exact_archive": {
                "path": str(candidate),
                "sha256": hashlib.sha256(b"candidate").hexdigest(),
            },
            "behavior_archive": {
                "path": str(behavior),
                "sha256": hashlib.sha256(b"behavior").hexdigest(),
            },
            "exact_identity_count": 20,
            "behavior_identity_row_count": 10,
            "reward_columns_imported": [],
            "scheduler_state_imported": False,
        },
    )

    frozen = freeze_authorization(
        source_authorization=source,
        dual_lane_contract=contract,
        capacity_manifest=capacity,
        candidate_archive=candidate,
        behavior_archive=behavior,
        history_manifest=history,
    )

    assert frozen["maximum_raw_asks"] == 24
    assert frozen["node_resource_profiles_allowed"] == ["SEARCH_DUAL_24"]
    assert "active_threads" not in frozen
    assert frozen["historical_snapshot"]["reward_rows_imported"] == 0
