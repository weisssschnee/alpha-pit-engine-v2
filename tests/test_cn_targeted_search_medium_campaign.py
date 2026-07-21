import json
from pathlib import Path

import pytest

from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    CHECKPOINT_BASE_TARGETS,
    CHECKPOINT_COUNT,
    MAX_RAW_ATTEMPTS,
    SEARCH_ROUTES,
    TOTAL_SCHEDULED_MATCHED_PAIR_BUDGET,
    _load_historical_dedupe,
    _block_compute_rows,
    _git_sha,
    _registry_binding,
    build_seed_attempt_manifest,
)
from our_system_phase2.services.portfolio_behavior_archive import (
    PortfolioBehaviorArchive,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = REPO / "runtime/field_registry/cn_unified_capability_registry_v3_20260717/unified_capability_registry.json"


def test_checkpoint_base_targets_close_exact_campaign_schedule() -> None:
    assert len(CHECKPOINT_BASE_TARGETS) == CHECKPOINT_COUNT == 6
    assert all(sum(row.values()) == 256 for row in CHECKPOINT_BASE_TARGETS)
    totals = {
        route: sum(row[route] for row in CHECKPOINT_BASE_TARGETS)
        for route in SEARCH_ROUTES
    }
    assert totals == {
        "MINUTE_STATIC": 224,
        "FIRSTN_PATH": 192,
        "SLOW_CROSS_SECTIONAL_LEVEL": 256,
        "SLOW_TEMPORAL_CHANGE": 224,
        "DISCLOSURE_EVENT": 160,
        "MARKET_REGIME_CONDITION": 192,
        "INTRADAY_STATE_TRANSITION": 288,
    }
    assert sum(totals.values()) == TOTAL_SCHEDULED_MATCHED_PAIR_BUDGET


def test_seed_attempt_ranges_are_disjoint_and_bounded() -> None:
    manifest = build_seed_attempt_manifest(
        registry_hash="r" * 64,
        schema_hash_by_backend={"active_bar": "a" * 64, "stock_session": "s" * 64},
        grammar_hash="g" * 64,
        seed_base=1729,
    )

    assert manifest["frozen_route_attempt_capacity"] <= MAX_RAW_ATTEMPTS
    for route in SEARCH_ROUTES:
        ranges = [
            (int(row["attempt_start"]), int(row["attempt_stop"]))
            for row in manifest["rows"]
            if row["route_id"] == route
        ]
        assert ranges == [(index * 2300, (index + 1) * 2300) for index in range(6)]


def test_registry_binding_accepts_only_frozen_v3_authority(tmp_path: Path) -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    assert _registry_binding(REGISTRY, registry)["status"] == "CURRENT_V3_REGISTRY_AUTHORITY_BOUND"
    copied = tmp_path / "unified_capability_registry.json"
    copied.write_bytes(REGISTRY.read_bytes())
    with pytest.raises(RuntimeError, match="CURRENT_REGISTRY_AUTHORITY_MISMATCH"):
        _registry_binding(copied, UnifiedCapabilityRegistry.read(copied))


def test_historical_dedupe_imports_identities_without_reward_or_scheduler(
    tmp_path: Path,
) -> None:
    candidate_path = tmp_path / "candidate_receipts.jsonl"
    candidate_path.write_text(
        json.dumps({"exact_identity": "exact.primary", "optimizer_reward": 99.0})
        + "\n"
        + json.dumps({"exact_identity": "exact.control", "scheduler_state": "old"})
        + "\n",
        encoding="utf-8",
    )
    behavior_path = tmp_path / "behavior.parquet"
    PortfolioBehaviorArchive(
        [
            {
                "pair_id": "pair.1",
                "behavior_probe_id": "probe.1",
                "portfolio_behavior_signature_id": "signature.1",
                "portfolio_behavior_family_id": "family.1",
                "optimizer_reward": 42.0,
                "scheduler_state": "old",
            }
        ]
    ).write_parquet(behavior_path)

    exact, behavior, snapshot = _load_historical_dedupe(
        candidate_archive_path=candidate_path,
        behavior_archive_path=behavior_path,
    )

    assert exact == {"exact.primary", "exact.control"}
    assert behavior.contains_probe("probe.1")
    assert "optimizer_reward" not in behavior.rows[0]
    assert "scheduler_state" not in behavior.rows[0]
    assert snapshot["reward_columns_imported"] == []
    assert snapshot["scheduler_state_imported"] is False


def test_runtime_gate_counts_only_completed_compute_blocks() -> None:
    events = []
    for block in range(4):
        events.extend(
            [
                {"phase": "global_trade_time_barrier", "blocks_processed": 1},
                {
                    "phase": "expression_value_dag",
                    "wall_seconds": 2.0,
                    "cpu_seconds": 12.0,
                },
                {
                    "phase": "turnover_and_cost",
                    "wall_seconds": 1.0,
                    "cpu_seconds": 6.0,
                },
            ]
        )
        if block < 3:
            events.append({"phase": "checkpoint", "blocks_processed": 1})

    rows = _block_compute_rows(events, allocated_threads=8)

    assert len(rows) == 3
    assert all(row["normalized_cpu_utilization"] == pytest.approx(0.75) for row in rows)


def test_deployment_commit_sha_supports_gitless_77o_workspace(monkeypatch) -> None:
    expected = "7" * 40
    monkeypatch.setenv("CN_CAMPAIGN_REPO_SHA", expected)
    assert _git_sha() == expected
