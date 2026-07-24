from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.run_minute_static_production_cem_v3 import (
    ROUTE_ID,
    _final_decision,
    run,
)
from our_system_phase2.services.typed_primitive_gate import (
    expression_fields,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (
    load_development_discovery_root_authority,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime"
    / "field_registry"
    / "cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
DISCOVERY = (
    REPO
    / "runtime"
    / "run_plans"
    / "cn_core_pack_development_discovery_v1.json"
)


def test_supply_capacity_hard_blocker_stops_all_downstream_work() -> None:
    final = _final_decision(30)
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
            "OLD_AUTHORIZED_CATALOG_CAPACITY_30_BELOW_144"
        ],
    }


def test_real_registry_discovery_lane_has_only_30_authorized_pairs(
    tmp_path: Path,
) -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    discovery = load_development_discovery_root_authority(
        DISCOVERY,
        registry=registry,
    )
    active_fields: set[str] = set()
    for field_id in discovery["route_root_allowlists"][ROUTE_ID]:
        field = registry.resolve(str(field_id))
        materialization = str(
            field.metadata.get("materialization_expression") or ""
        )
        active_fields.update(
            expression_fields(materialization)
            if materialization
            else {field.field_id}
        )
    layout = tmp_path / "active_layout.json"
    layout.write_text(
        json.dumps({"fields": sorted(active_fields)}),
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
            discovery_contract=DISCOVERY,
            active_layout=layout,
            historical_exact_archive=archive,
            source_candidate_ledger=ledger,
            output_root=output_root,
            repo_sha="test-sha",
            task_id="test-task",
            allow_noncanonical_host=True,
        )
    )

    assert result["old_supply_gate"][
        "discovery_authorized_root_count"
    ] == 6
    assert result["old_supply_gate"][
        "old_catalog_exact_capacity"
    ] == 30
    manifest = json.loads(
        (output_root / "artifact_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["behavior_probe_count"] == 0
    assert manifest["phase3cm_pair_count"] == 0
    assert manifest["campaign_arm_count"] == 0
    assert {path.name for path in output_root.iterdir()} == {
        "artifact_manifest.json",
        "disclosure_v2_dispositions.json",
        "final_decision.json",
        "old_supply_gate.json",
    }
