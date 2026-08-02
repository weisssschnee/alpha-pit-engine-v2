from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.freeze_cn_shared_control_dual_search_authorization import (
    freeze_authorization,
)
from scripts.materialize_cn_search_preflight_authorization import (
    materialize_authorization,
)
from our_system_phase2.runtime.cn_large_tpe_search_campaign import (
    CONTINUOUS_SHARED_CONTROL_WINNER_GUIDED_SEARCH_PROFILE,
    SHARED_CONTROL_WINNER_GUIDED_SEARCH_PROFILE,
    _authorization_binding,
    _campaign_runtime_spec,
)


def test_continuous_shared_profile_is_one_full_dual_lane_checkpoint() -> None:
    spec = _campaign_runtime_spec(
        CONTINUOUS_SHARED_CONTROL_WINNER_GUIDED_SEARCH_PROFILE
    )
    assert spec["maximum_checkpoints"] == 1
    assert spec["asks_per_checkpoint"] == 1_152
    assert spec["maximum_raw_asks"] == 1_152
    assert spec["fixed_route_mix"] == {
        "SLOW_TEMPORAL_CHANGE": 1_120,
        "FIRSTN_PATH": 32,
        "SLOW_CROSS_SECTIONAL_LEVEL": 0,
        "MARKET_REGIME_CONDITION": 0,
        "DISCLOSURE_EVENT": 0,
    }


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
                "campaign_profile": "cn_shared_control_winner_guided_search_v1",
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
    winner = tmp_path / "winner.json"
    candidate.write_bytes(b"candidate")
    behavior.write_bytes(b"behavior")
    winner.write_text("{}", encoding="utf-8")
    source_payload = json.loads(source.read_text(encoding="utf-8"))
    source_payload["winner_structural_guide_sha256"] = hashlib.sha256(
        winner.read_bytes()
    ).hexdigest()
    source_payload["resource_topology_authorization_sha256"] = (
        "inherited-source-self-hash"
    )
    _write(source, source_payload)
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
            "winner_structural_guide": {
                "path": str(winner),
                "sha256": hashlib.sha256(winner.read_bytes()).hexdigest(),
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
    materialized = materialize_authorization(
        _write(tmp_path / "materializable.json", frozen)
    )
    assert materialized["status"] == "ZERO_FINANCIAL_PREFLIGHT_AUTHORIZED"


def test_frozen_shared_authority_matches_runtime_and_preflight_hash(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_payload = json.loads(
        (
            repo_root
            / "runtime"
            / "run_plans"
            / "cn_winner_guided_continuation_search_v1_authorization.json"
        ).read_text(encoding="utf-8-sig")
    )
    winner = _write(
        tmp_path / "winner.json",
        {
            "status": "FROZEN_DEVELOPMENT_WINNER_STRUCTURES",
            "source_selection_payload_sha256": source_payload[
                "winner_source_selection_payload_sha256"
            ],
            "allowed_skeletons_by_route": source_payload[
                "winner_structural_skeletons"
            ],
        },
    )
    source_payload["winner_structural_guide_sha256"] = hashlib.sha256(
        winner.read_bytes()
    ).hexdigest()
    source = _write(tmp_path / "source.json", source_payload)
    contract = _write(
        tmp_path / "contract.json",
        {
            "status": (
                "FROZEN_USER_AUTHORIZED_QUALIFICATION_REQUIRED_BEFORE_LAUNCH"
            ),
            "authority_source": "USER",
            "shared_resource_authority": {
                "search_profile": "SEARCH_DUAL_24",
                "total_cpu_threads": 32,
            },
            "search_lane": {
                "campaign_id": "shared-search",
                "campaign_profile": SHARED_CONTROL_WINNER_GUIDED_SEARCH_PROFILE,
                "checkpoint_count": 8,
                "formal_fresh_exact_asks_per_checkpoint": 1_152,
                "maximum_formal_fresh_exact_asks": 9_216,
                "seed_base": 2026073102,
                "fixed_route_formal_asks_per_checkpoint": {
                    "SLOW_TEMPORAL_CHANGE": 1_120,
                    "FIRSTN_PATH": 32,
                    "SLOW_CROSS_SECTIONAL_LEVEL": 0,
                    "MARKET_REGIME_CONDITION": 0,
                    "DISCLOSURE_EVENT": 0,
                },
                "final_route_formal_ask_allocation": {
                    "SLOW_TEMPORAL_CHANGE": 8_960,
                    "FIRSTN_PATH": 256,
                    "SLOW_CROSS_SECTIONAL_LEVEL": 0,
                    "MARKET_REGIME_CONDITION": 0,
                    "DISCLOSURE_EVENT": 0,
                },
                "throughput_contract": {
                    "maximum_wall_seconds": 129_600,
                    "first_checkpoint_minimum_pair_evaluated_per_hour": 300,
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
    capacity_hash = _hash(capacity_body)
    capacity = _write(
        tmp_path / "capacity.json",
        {
            **capacity_body,
            "capacity_manifest_sha256": capacity_hash,
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
                "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
            },
            "behavior_archive": {
                "path": str(behavior),
                "sha256": hashlib.sha256(behavior.read_bytes()).hexdigest(),
            },
            "winner_structural_guide": {
                "path": str(winner),
                "sha256": hashlib.sha256(winner.read_bytes()).hexdigest()
            },
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
    frozen_path = _write(tmp_path / "frozen.json", frozen)
    preflight = materialize_authorization(frozen_path)
    preflight_path = _write(tmp_path / "preflight.json", preflight)
    binding = _authorization_binding(
        path=preflight_path,
        candidate_archive=candidate,
        behavior_archive=behavior,
        history_manifest=history,
        winner_structural_guide=winner,
        seed_base=2026073102,
        active_threads=24,
        session_threads=24,
        node_resource_profile="SEARCH_DUAL_24",
        node_resource_capacity_sha256=capacity_hash,
        preflight_only=True,
    )

    spec = _campaign_runtime_spec(SHARED_CONTROL_WINNER_GUIDED_SEARCH_PROFILE)
    assert spec["asks_per_checkpoint"] == 1_152
    assert spec["maximum_raw_asks"] == 9_216
    assert binding["frozen"]["fixed_route_formal_asks_per_checkpoint"] == {
        "SLOW_TEMPORAL_CHANGE": 1_120,
        "FIRSTN_PATH": 32,
        "SLOW_CROSS_SECTIONAL_LEVEL": 0,
        "MARKET_REGIME_CONDITION": 0,
        "DISCLOSURE_EVENT": 0,
    }
