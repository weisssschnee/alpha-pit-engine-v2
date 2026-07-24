from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.run_minute_static_production_cem_v3 import (
    MINIMUM_BEHAVIOR_SUPPLY,
    MINIMUM_EXACT_SUPPLY,
    MinuteStaticProductionProjection,
    _failure_decision,
    _load_production_contract,
    _production_parity,
    _session_sample_contract,
    run,
)
from our_system_phase2.services.fixed_split_authority import (
    FixedSplitAuthority,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    RegistryDrivenGenerator,
)
from scripts.run_cn_phase3cm_streaming_qualification import (
    _sha256 as _streaming_sha256,
    _session_sample_calendar,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime"
    / "field_registry"
    / "cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
PRODUCTION_CONTRACT = (
    REPO
    / "runtime"
    / "run_plans"
    / "cn_minute_static_production_roots_v1.json"
)
SPLIT = (
    REPO
    / "runtime"
    / "run_plans"
    / "phase3ga_true1min_2024_2025_global_split_manifest.csv"
)


def test_supply_capacity_hard_blocker_stops_all_downstream_work() -> None:
    final = _failure_decision(
        "OLD_POST_ARCHIVE_EXACT_SUPPLY_30_BELOW_72"
    )
    assert final == {
        "SAMPLED_PHASE3CM_AUTHORITY": (
            "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER"
        ),
        "FORMULA_SPACE_INCREMENT": (
            "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER"
        ),
        "CEM_SEARCH_INCREMENT": "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER",
        "PERFORMANCE_CONTRACT": "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER",
        "TARGET_FAMILY_LARGE_SEARCH_READINESS": "SUPPLY_BLOCKED",
        "READINESS_BLOCKERS": [
            "OLD_POST_ARCHIVE_EXACT_SUPPLY_30_BELOW_72"
        ],
    }


def test_real_production_roots_form_110_old_pairs_and_pass_exact_gate(
    tmp_path: Path,
) -> None:
    contract = json.loads(
        PRODUCTION_CONTRACT.read_text(encoding="utf-8")
    )
    route_roots = list(map(str, contract["route_roots"]))
    assert len(route_roots) == 11
    assert MINIMUM_EXACT_SUPPLY == 72
    assert MINIMUM_BEHAVIOR_SUPPLY == 48

    layout = tmp_path / "active_layout.json"
    layout.write_text(
        json.dumps({"fields": route_roots}),
        encoding="utf-8",
    )
    archive = tmp_path / "archive.parquet"
    ledger = tmp_path / "ledger.parquet"
    pq.write_table(
        pa.table({"exact_identity": ["historical-a"]}),
        archive,
    )
    pq.write_table(
        pa.table({"exact_identity": ["historical-b"]}),
        ledger,
    )
    output_root = tmp_path / "result"
    result = run(
        argparse.Namespace(
            registry=REGISTRY,
            production_root_contract=PRODUCTION_CONTRACT,
            active_layout=layout,
            historical_exact_archive=archive,
            source_candidate_ledger=ledger,
            historical_behavior_archive=None,
            split_manifest=None,
            compute_threads=2,
            output_root=output_root,
            repo_sha="test-sha",
            task_id="test-task",
            allow_noncanonical_host=True,
            static_only=True,
        )
    )

    gate = result["old_supply_gate"]
    assert gate["production_root_count"] == 11
    assert gate["atomic_ordered_field_pair_count"] == 110
    assert gate["legal_pairs"] == 110
    assert gate["exact_unique_pairs"] == 110
    assert gate["post_archive_exact_supply"] == 110
    assert gate["exact_gate"] == "PASS"
    assert gate["behavior_gate"] == "PENDING"
    assert result["status"] == (
        "EXACT_SUPPLY_QUALIFIED_BEHAVIOR_PENDING_STATIC_ONLY"
    )

    candidates = pq.read_table(
        output_root / "old_post_archive_candidates.parquet"
    )
    assert candidates.num_rows == 220
    manifest = json.loads(
        (output_root / "artifact_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["behavior_probe_count"] == 0
    assert manifest["phase3cm_pair_count"] == 0
    assert manifest["campaign_arm_count"] == 0
    assert manifest["validation_reads"] == 0
    assert manifest["holdout_reads"] == 0
    assert manifest["forward_2026_reads"] == 0
    assert {path.name for path in output_root.iterdir()} == {
        "artifact_manifest.json",
        "disclosure_v2_dispositions.json",
        "old_post_archive_candidates.parquet",
        "old_supply_gate.json",
        "supply_decision.json",
    }


def test_minute_production_projection_replays_existing_grammar() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    _, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )
    parity = _production_parity(projection)
    assert parity["status"] == "PASS"
    assert parity["checked_projection_rows"] == 330
    assert parity["failure_count"] == 0


def test_session_sample_is_frozen_month_stratified_quarter() -> None:
    split = FixedSplitAuthority.read(SPLIT)
    left = _session_sample_contract(split, seed=2026072500)
    right = _session_sample_contract(split, seed=2026072500)
    assert left == right
    assert left["selected_session_count"] == len(
        left["selected_sessions"]
    )
    assert left["selected_session_count"] == 91
    assert left["validation_reads"] == 0
    assert left["holdout_reads"] == 0
    assert left["forward_2026_reads"] == 0
    assert all(
        row["selected_session_count"]
        == max(1, round(row["full_session_count"] * 0.25))
        for row in left["month_receipts"]
    )


def test_streaming_evaluator_binds_session_sample_without_split_rewrite(
    tmp_path: Path,
) -> None:
    split = FixedSplitAuthority.read(SPLIT)
    contract = _session_sample_contract(split, seed=2026072501)
    path = tmp_path / "session_sample.json"
    path.write_text(
        json.dumps(contract, sort_keys=True),
        encoding="utf-8",
    )
    full_train = tuple(
        row["trade_date"]
        for row in split.rows
        if row["split"] == "train"
    )
    selected, receipt = _session_sample_calendar(
        path,
        binding={
            "split_manifest_hash": split.manifest_hash,
            "evaluation_scope": "development_session_sample",
            "session_sample": {
                "sha256": _streaming_sha256(path)
            },
        },
        evaluation_role="train",
        full_calendar=full_train,
    )
    assert selected == tuple(contract["selected_sessions"])
    assert receipt["selected_session_count"] == 91
    assert receipt["full_session_count"] == 364
