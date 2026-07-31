from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.freeze_cn_search_resource_topology_authorization import (
    _stable_hash,
    freeze_authorization,
)
from our_system_phase2.runtime.cn_large_tpe_search_campaign import (
    _authorization_binding,
    _compute_threads_by_backend,
)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_phase3cm_threads_follow_dual_lane_runtime_entitlement() -> None:
    assert _compute_threads_by_backend(
        active_threads=24,
        session_threads=24,
    ) == {
        "active_bar": 24,
        "stock_session": 24,
    }


def test_topology_authorization_separates_resources_from_search_semantics(
    tmp_path: Path,
) -> None:
    source = _write_json(
        tmp_path / "source.json",
        {
            "schema_version": "source_v1",
            "campaign_profile": "cn_winner_guided_continuation_search_v1",
            "execution_authorized": True,
            "financial_campaign_authorized": True,
            "active_threads": 32,
            "session_threads": 32,
            "seed_base": 1,
            "optimizer_search_score_policy": "FROZEN",
        },
    )
    capacity_body: dict[str, object] = {
        "schema_version": "cn_alpha_node_resource_profiles_v1",
        "authorized_host": "DESKTOP-77OPJ6F",
        "logical_cpu_threads": 32,
        "memory_capacity_bytes": 96 * 1024**3,
        "minimum_free_memory_bytes": 24 * 1024**3,
        "profiles": {
            "SEARCH_EXCLUSIVE_32": {"role": "SEARCH", "cpu_threads": 32},
            "SEARCH_DUAL_24": {"role": "SEARCH", "cpu_threads": 24},
        },
    }
    capacity_body["capacity_manifest_sha256"] = _stable_hash(capacity_body)
    capacity = _write_json(tmp_path / "capacity.json", capacity_body)

    frozen = freeze_authorization(
        source_authorization=source,
        capacity_manifest=capacity,
    )

    assert "active_threads" not in frozen
    assert "session_threads" not in frozen
    assert frozen["node_resource_profiles_allowed"] == [
        "SEARCH_EXCLUSIVE_32",
        "SEARCH_DUAL_24",
    ]
    assert frozen["resource_profile_switch_boundary"] == (
        "BATCH_CLOSED_IMMUTABLE_ONLY"
    )
    claimed = frozen.pop("resource_topology_authorization_sha256")
    assert claimed == _stable_hash(frozen)


def test_runtime_accepts_dual_profile_without_mutating_search_contract(
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
    candidate = (tmp_path / "candidate.parquet")
    behavior = (tmp_path / "behavior.parquet")
    history = (tmp_path / "history.json")
    winner = (tmp_path / "winner.json")
    for path, content in (
        (candidate, b"candidate"),
        (behavior, b"behavior"),
    ):
        path.write_bytes(content)
    winner.write_text(
        json.dumps(
            {
                "status": "FROZEN_DEVELOPMENT_WINNER_STRUCTURES",
                "source_selection_payload_sha256": source_payload[
                    "winner_source_selection_payload_sha256"
                ],
                "allowed_skeletons_by_route": source_payload[
                    "winner_structural_skeletons"
                ]
            }
        ),
        encoding="utf-8",
    )
    history.write_text(
        json.dumps(
            {
                "status": "PASS",
                "candidate_exact_archive": {
                    "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest()
                },
                "behavior_archive": {
                    "sha256": hashlib.sha256(behavior.read_bytes()).hexdigest()
                },
                "winner_structural_guide": {
                    "sha256": hashlib.sha256(winner.read_bytes()).hexdigest()
                },
            }
        ),
        encoding="utf-8",
    )
    source_payload["historical_candidate_archive_sha256"] = hashlib.sha256(
        candidate.read_bytes()
    ).hexdigest()
    source_payload["historical_behavior_archive_sha256"] = hashlib.sha256(
        behavior.read_bytes()
    ).hexdigest()
    source_payload["historical_manifest_sha256"] = hashlib.sha256(
        history.read_bytes()
    ).hexdigest()
    source_payload["winner_structural_guide_sha256"] = hashlib.sha256(
        winner.read_bytes()
    ).hexdigest()
    source = _write_json(tmp_path / "source.json", source_payload)
    capacity_body: dict[str, object] = {
        "schema_version": "cn_alpha_node_resource_profiles_v1",
        "authorized_host": "DESKTOP-77OPJ6F",
        "logical_cpu_threads": 32,
        "memory_capacity_bytes": 96 * 1024**3,
        "minimum_free_memory_bytes": 24 * 1024**3,
        "profiles": {
            "SEARCH_EXCLUSIVE_32": {"role": "SEARCH", "cpu_threads": 32},
            "SEARCH_DUAL_24": {"role": "SEARCH", "cpu_threads": 24},
        },
    }
    capacity_hash = _stable_hash(capacity_body)
    capacity_body["capacity_manifest_sha256"] = capacity_hash
    capacity = _write_json(tmp_path / "capacity.json", capacity_body)
    flexible = freeze_authorization(
        source_authorization=source,
        capacity_manifest=capacity,
    )
    flexible_path = _write_json(tmp_path / "flexible.json", flexible)

    binding = _authorization_binding(
        path=flexible_path,
        candidate_archive=candidate,
        behavior_archive=behavior,
        history_manifest=history,
        winner_structural_guide=winner,
        seed_base=int(source_payload["seed_base"]),
        active_threads=24,
        session_threads=24,
        node_resource_profile="SEARCH_DUAL_24",
        node_resource_capacity_sha256=capacity_hash,
    )

    assert binding["frozen"]["node_resource_profiles_allowed"] == [
        "SEARCH_EXCLUSIVE_32",
        "SEARCH_DUAL_24",
    ]
    assert "active_threads" not in binding["frozen"]
