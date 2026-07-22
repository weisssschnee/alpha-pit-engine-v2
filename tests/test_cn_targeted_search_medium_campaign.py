import json
from pathlib import Path

import pytest

from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    CHECKPOINT_BASE_TARGETS,
    CHECKPOINT_COUNT,
    CHECKPOINT_SCHEDULED_PAIRS,
    MAX_COMPLETED_DEVELOPMENT_MATCHED_PAIRS,
    MAX_RAW_ATTEMPTS,
    MAX_WALL_SECONDS,
    PAIR_BATCH_SIZE_BY_BACKEND,
    SEARCH_ROUTES,
    TOTAL_SCHEDULED_MATCHED_PAIR_BUDGET,
    _campaign_authorization_binding,
    _load_historical_dedupe,
    _add_resolved_behavior_rows,
    _block_compute_rows,
    _bounded_runtime_adjustment,
    _git_sha,
    _registry_binding,
    _runtime_gate,
    build_seed_attempt_manifest,
)
import our_system_phase2.runtime.cn_targeted_search_medium_campaign as campaign_module
from scripts.build_cn_campaign_history_snapshot import build_snapshot
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


def test_unresolved_probe_rows_never_enter_durable_behavior_archive() -> None:
    archive = PortfolioBehaviorArchive()
    added = _add_resolved_behavior_rows(
        archive,
        [
            {"pair_id": "resolved", "behavior_status": "RESOLVED", "behavior_probe_id": "probe.ok"},
            {"pair_id": "unresolved", "behavior_status": "BEHAVIOR_UNRESOLVED", "behavior_probe_id": "probe.bad"},
        ],
    )

    assert added == 1
    assert archive.contains_probe("probe.ok")
    assert not archive.contains_probe("probe.bad")


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


@pytest.mark.parametrize(
    ("cpu_seconds", "expected_status", "expected_bottleneck"),
    [
        (17.0, "FAIL", "HOST_COMPUTE_UNDERALLOCATED"),
        (18.0, "PASS", "FULL_HOST_NATIVE_KERNEL_SMT_CEILING_PROVEN"),
        (26.0, "PASS", "CPU_COMPUTE_SATURATED"),
    ],
)
def test_runtime_gate_requires_primary_host_occupancy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cpu_seconds: float,
    expected_status: str,
    expected_bottleneck: str,
) -> None:
    backend_root = tmp_path / "phase3cm" / "active_bar"
    backend_root.mkdir(parents=True)
    result = {
        "parallelism_status": "PARALLELISM_ENGAGED",
        "wall_seconds": 4.0,
        "rows_processed": 100,
        "pair_count": 1,
        "peak_rss_bytes": 1024,
        "phase_totals": {"checkpoint": {"wall_seconds": 0.1}},
        "expression_audits": [{"cache_hits": 1}],
        "pair_results": [{"pair_id": "pair.1"}],
    }
    (backend_root / "CN_STREAMING_BACKEND_RESULT.json").write_text(
        json.dumps(result), encoding="utf-8"
    )
    events = []
    for _ in range(3):
        events.extend(
            [
                {"phase": "global_trade_time_barrier", "blocks_processed": 1},
                {
                    "phase": "expression_value_dag",
                    "wall_seconds": 1.0,
                    "cpu_seconds": cpu_seconds,
                },
                {"phase": "checkpoint", "blocks_processed": 1},
            ]
        )
    (backend_root / "CN_PHASE3CM_PHASE_TIMING.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in events), encoding="utf-8"
    )
    (backend_root / "runtime_samples.json").write_text(
        json.dumps(
            [
                {
                    "elapsed_seconds": 4.0,
                    "available_memory_bytes": 64 * 1024**3,
                    "system_read_bytes": 0,
                    "system_write_bytes": 0,
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(campaign_module, "_physical_cpu_count", lambda: 16)
    monkeypatch.setattr(campaign_module, "_logical_cpu_count", lambda: 32)

    gate = _runtime_gate(tmp_path, {"active_bar": 30, "stock_session": 2})
    active = gate["backends"]["active_bar"]

    assert active["status"] == expected_status
    assert active["hot_path_bottleneck"] == expected_bottleneck
    assert active["host_logical_cpu_occupancy"] == pytest.approx(cpu_seconds / 32.0)


def test_runtime_gate_keeps_memory_headroom_failure_out_of_route_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for backend, threads in (("active_bar", 30), ("stock_session", 2)):
        backend_root = tmp_path / "phase3cm" / backend
        backend_root.mkdir(parents=True)
        required = {
            phase: {"parallelism_status": "PARALLELISM_ENGAGED"}
            for phase in (
                "expression_value_dag",
                "cross_sectional_rank_mapping",
                "label_free_behavior",
                "turnover_and_cost",
            )
        }
        required["portfolio_label_order_prepare"] = {
            "parallelism_status": "PARALLELISM_NOT_ENGAGED"
        }
        result = {
            "parallelism_status": "PARALLELISM_NOT_ENGAGED",
            "compute_phase_parallelism": required,
            "wall_seconds": 4.0,
            "rows_processed": 100,
            "pair_count": 1,
            "peak_rss_bytes": 1024,
            "phase_totals": {"checkpoint": {"wall_seconds": 0.1}},
            "expression_audits": [{"cache_hits": 1}],
            "pair_results": [{"pair_id": f"pair.{backend}"}],
        }
        (backend_root / "CN_STREAMING_BACKEND_RESULT.json").write_text(
            json.dumps(result), encoding="utf-8"
        )
        cpu_seconds = 18.0 if backend == "active_bar" else 1.5
        events = []
        for _ in range(3):
            events.extend(
                [
                    {"phase": "global_trade_time_barrier", "blocks_processed": 1},
                    {
                        "phase": "expression_value_dag",
                        "wall_seconds": 1.0,
                        "cpu_seconds": cpu_seconds,
                    },
                    {"phase": "checkpoint", "blocks_processed": 1},
                ]
            )
        (backend_root / "CN_PHASE3CM_PHASE_TIMING.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in events), encoding="utf-8"
        )
        (backend_root / "runtime_samples.json").write_text(
            json.dumps(
                [
                    {
                        "elapsed_seconds": 4.0,
                        "available_memory_bytes": 8 * 1024**3,
                        "system_read_bytes": 0,
                        "system_write_bytes": 0,
                    }
                ]
            ),
            encoding="utf-8",
        )
    monkeypatch.setattr(campaign_module, "_physical_cpu_count", lambda: 16)
    monkeypatch.setattr(campaign_module, "_logical_cpu_count", lambda: 32)

    gate = _runtime_gate(tmp_path, {"active_bar": 30, "stock_session": 2})

    assert gate["status"] == "PASS_WITH_RUN_HEALTH_FAILURE"
    assert all(row["status"] == "PASS" for row in gate["backends"].values())
    assert all(
        row["run_health_status"] == "MEMORY_HEADROOM_GATE_FAILED"
        for row in gate["backends"].values()
    )
    assert gate["run_health_policy"] == "INFRASTRUCTURE_ONLY_DOES_NOT_MUTATE_ROUTE_HEALTH"


def test_bounded_adjustment_never_repeats_an_unchanged_full_host_pool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(campaign_module, "_logical_cpu_count", lambda: 32)
    monkeypatch.setattr(
        campaign_module,
        "_run_phase3cm_monitored",
        lambda **_: pytest.fail("unchanged thread allocation must not relaunch Phase3CM"),
    )
    gate, receipts = _bounded_runtime_adjustment(
        initial_gate={
            "status": "RUNTIME_ACCELERATION_GATE_FAILED",
            "backends": {
                "active_bar": {
                    "status": "FAIL",
                    "hot_path_bottleneck": "HOST_COMPUTE_UNDERALLOCATED",
                }
            },
        },
        checkpoint_id="checkpoint_001",
        checkpoint_root=tmp_path,
        binding_path=tmp_path / "binding.json",
        table_paths={},
        split_manifest=tmp_path / "split.csv",
        field_roots={},
        label_roots={},
        purity_path=tmp_path / "purity.json",
        compute_threads={"active_bar": 30, "stock_session": 2},
        deadline_epoch=0.0,
    )
    assert receipts == []
    assert gate["status"] == "RUN_INVALID_NO_ACTIONABLE_CONCURRENCY_ADJUSTMENT"
    assert gate["bounded_concurrency_adjustment_count"] == 0


def test_deployment_commit_sha_supports_gitless_77o_workspace(monkeypatch) -> None:
    expected = "7" * 40
    monkeypatch.setenv("CN_CAMPAIGN_REPO_SHA", expected)
    assert _git_sha() == expected


def test_large_campaign_history_and_authorization_are_identity_only(
    tmp_path: Path,
) -> None:
    candidate_one = tmp_path / "candidate_one.jsonl"
    candidate_one.write_text(
        json.dumps({"exact_identity": "exact.1", "optimizer_reward": 9.0}) + "\n",
        encoding="utf-8",
    )
    candidate_two = tmp_path / "candidate_two.parquet"
    pd_rows = [
        {"exact_identity": "exact.1", "scheduler_state": "old"},
        {"exact_identity": "exact.2", "scheduler_state": "old"},
    ]
    import pandas as pd

    pd.DataFrame(pd_rows).to_parquet(candidate_two, index=False)
    behavior_one = tmp_path / "behavior_one.parquet"
    behavior_two = tmp_path / "behavior_two.parquet"
    PortfolioBehaviorArchive(
        [
            {
                "pair_id": "pair.1",
                "behavior_status": "RESOLVED",
                "behavior_probe_id": "probe.1",
                "optimizer_reward": 99.0,
            }
        ]
    ).write_parquet(behavior_one)
    PortfolioBehaviorArchive(
        [
            {
                "pair_id": "pair.2",
                "behavior_status": "RESOLVED",
                "portfolio_behavior_signature_id": "signature.2",
                "scheduler_state": "old",
            }
        ]
    ).write_parquet(behavior_two)
    candidate_output = tmp_path / "candidate_exact.parquet"
    behavior_output = tmp_path / "behavior.parquet"
    manifest_output = tmp_path / "manifest.json"
    manifest = build_snapshot(
        candidate_sources=[candidate_one, candidate_two],
        behavior_sources=[behavior_one, behavior_two],
        candidate_output=candidate_output,
        behavior_output=behavior_output,
        manifest_output=manifest_output,
    )

    assert manifest["exact_identity_count"] == 2
    assert set(pd.read_parquet(candidate_output)["exact_identity"]) == {
        "exact.1",
        "exact.2",
    }
    rows = PortfolioBehaviorArchive.read_parquet(behavior_output).rows
    assert len(rows) == 2
    assert all("optimizer_reward" not in row for row in rows)
    assert all("scheduler_state" not in row for row in rows)

    authorization_path = tmp_path / "authorization.json"
    authorization = {
        "campaign_id": "TEST_FRESH_CAMPAIGN",
        "execution_authorized": True,
        "checkpoint_count": CHECKPOINT_COUNT,
        "checkpoint_scheduled_pairs": CHECKPOINT_SCHEDULED_PAIRS,
        "total_scheduled_matched_pair_budget": TOTAL_SCHEDULED_MATCHED_PAIR_BUDGET,
        "maximum_completed_development_matched_pairs": MAX_COMPLETED_DEVELOPMENT_MATCHED_PAIRS,
        "maximum_raw_attempts": MAX_RAW_ATTEMPTS,
        "maximum_wall_seconds": MAX_WALL_SECONDS,
        "seed_base": 1729,
        "active_threads": 11,
        "session_threads": 2,
        "active_pair_batch_size": PAIR_BATCH_SIZE_BY_BACKEND["active_bar"],
        "session_pair_batch_size": PAIR_BATCH_SIZE_BY_BACKEND["stock_session"],
        "global_worker_limit": 24,
        "constructor_profile": "registry_compositional_v2",
        "scheduler_authority": "UNIFIED_REGISTRY_ROUTE_ID",
        "required_parallelism_status": "PARALLELISM_ENGAGED",
        "peak_rss_limit_bytes": 48 * 1024**3,
        "checkpoint_recovery": "EXISTING_PHASE3CM_ONLY",
        "validation_mode": "AUTOMATIC_POST_TRAIN_REPORT_ONLY",
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "holdout": "SEALED",
        "forward_2026": "SEALED",
        "historical_source_sha256": [
            row["sha256"] for row in manifest["sources"]
        ],
    }
    authorization_path.write_text(
        json.dumps(authorization), encoding="utf-8"
    )
    binding = _campaign_authorization_binding(
        authorization_path=authorization_path,
        history_manifest_path=manifest_output,
        candidate_archive_path=candidate_output,
        behavior_archive_path=behavior_output,
        seed_base=1729,
        active_threads=11,
        session_threads=2,
    )
    assert binding["status"] == "CAMPAIGN_EXECUTION_AUTHORIZED"
    with pytest.raises(RuntimeError, match="seed_base"):
        _campaign_authorization_binding(
            authorization_path=authorization_path,
            history_manifest_path=manifest_output,
            candidate_archive_path=candidate_output,
            behavior_archive_path=behavior_output,
            seed_base=1730,
            active_threads=11,
            session_threads=2,
        )
