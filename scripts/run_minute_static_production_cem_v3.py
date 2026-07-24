"""Close the mandatory MINUTE_STATIC V3 OLD-supply preflight.

The frozen contract requires OLD post-archive exact supply >= 144 before any
behavior probe, sampled evaluator work, or financial arm can run.  This script
binds the current Registry, discovery authority, active sidecar schema, and
search memory, then fails fast when the authorized catalog itself is smaller
than that threshold.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
from pathlib import Path
from typing import Any, Mapping

import pyarrow.parquet as pq

from our_system_phase2.services.search_choice_policy import (
    DISCLOSURE_V2_EXTENSION_DISPOSITIONS,
)
from our_system_phase2.services.typed_primitive_gate import (
    expression_fields,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    RegistryDrivenGenerator,
    load_development_discovery_root_authority,
)


AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
AUTHORIZATION_ID = "MINUTE_STATIC_PRODUCTION_LANE_CEM_V3"
ROUTE_ID = "MINUTE_STATIC"
OLD_SKELETON_ID = "cn.comp.v2.minute_static.field_spread"
MINIMUM_EXACT_SUPPLY = 144
MINIMUM_BEHAVIOR_SUPPLY = 72


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            dict(payload),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _historical_exact_count(*paths: Path) -> int:
    values: set[str] = set()
    for path in paths:
        values.update(
            str(value)
            for value in pq.read_table(
                path,
                columns=["exact_identity"],
            ).column("exact_identity").to_pylist()
            if str(value or "")
        )
    if not values:
        raise RuntimeError("HISTORICAL_EXACT_MEMORY_EMPTY")
    return len(values)


def _materializable_authorized_roots(
    *,
    registry: UnifiedCapabilityRegistry,
    discovery_roots: list[str],
    active_fields: set[str],
) -> tuple[str, ...]:
    usable = []
    for field_id in discovery_roots:
        field = registry.resolve(str(field_id))
        materialization = str(
            field.metadata.get("materialization_expression") or ""
        )
        leaves = (
            expression_fields(materialization)
            if materialization
            else {field.field_id}
        )
        if set(map(str, leaves)).issubset(active_fields):
            usable.append(field.field_id)
    return tuple(sorted(set(usable)))


def _final_decision(capacity: int) -> dict[str, Any]:
    blocker = (
        f"OLD_AUTHORIZED_CATALOG_CAPACITY_{capacity}_BELOW_"
        f"{MINIMUM_EXACT_SUPPLY}"
    )
    return {
        "SAMPLED_PHASE3CM_AUTHORITY": (
            "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER"
        ),
        "FORMULA_SPACE_INCREMENT": (
            "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER"
        ),
        "CEM_SEARCH_INCREMENT": "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER",
        "PERFORMANCE_CONTRACT": "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER",
        "TARGET_FAMILY_LARGE_SEARCH_READINESS": "SUPPLY_BLOCKED",
        "READINESS_BLOCKERS": [blocker],
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    if (
        not args.allow_noncanonical_host
        and platform.node().upper() != AUTHORIZED_HOST
    ):
        raise RuntimeError(f"official V3 evidence must run on {AUTHORIZED_HOST}")

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    discovery = load_development_discovery_root_authority(
        args.discovery_contract.resolve(),
        registry=registry,
    )
    layout = json.loads(
        args.active_layout.resolve().read_text(encoding="utf-8-sig")
    )
    active_fields = set(map(str, layout.get("fields") or ()))
    discovery_roots = list(
        map(
            str,
            discovery["route_root_allowlists"][ROUTE_ID],
        )
    )
    materializable = _materializable_authorized_roots(
        registry=registry,
        discovery_roots=discovery_roots,
        active_fields=active_fields,
    )
    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist={
            **discovery["route_root_allowlists"],
            ROUTE_ID: materializable,
        },
    )
    old_lane = generator.categorical_gene_space(
        ROUTE_ID,
        skeleton_id=OLD_SKELETON_ID,
    )
    categories = dict(old_lane["ordered_categories_by_slot"])
    if set(categories) != {
        "skeleton_id",
        "gene_surface_id",
        "field_pair_id",
    }:
        raise RuntimeError("MINUTE_STATIC_OLD_LANE_SURFACE_DRIFT")
    capacity = len(categories["field_pair_id"])
    if capacity >= MINIMUM_EXACT_SUPPLY:
        raise RuntimeError(
            "OLD_CAPACITY_GATE_PASSED_CONTINUOUS_V3_PATH_REQUIRED"
        )

    historical_count = _historical_exact_count(
        args.historical_exact_archive.resolve(),
        args.source_candidate_ledger.resolve(),
    )
    supply = {
        "schema_version": "cn_minute_static_old_supply_gate_v1",
        "status": "FAIL_STOP_BEFORE_BEHAVIOR_AND_PHASE3CM",
        "hard_blocker": (
            "AUTHORIZED_CATALOG_CAPACITY_BELOW_REQUIRED_EXACT_SUPPLY"
        ),
        "discovery_authorized_root_count": len(discovery_roots),
        "materializable_authorized_roots": list(materializable),
        "atomic_ordered_field_pair_count": capacity,
        "old_catalog_exact_capacity": capacity,
        "post_archive_exact_supply_upper_bound": capacity,
        "required_post_archive_exact_supply": MINIMUM_EXACT_SUPPLY,
        "required_behavior_supply": MINIMUM_BEHAVIOR_SUPPLY,
        "historical_exact_identity_count": historical_count,
        "generation": "NOT_RUN_STRUCTURAL_CAPACITY_HARD_STOP",
        "behavior_probe": "NOT_RUN_EXACT_GATE_FAILED",
        "sampled_phase3cm": "NOT_RUN_EXACT_GATE_FAILED",
        "full_phase3cm": "NOT_RUN_EXACT_GATE_FAILED",
    }
    supply_path = _write_json(
        output_root / "old_supply_gate.json",
        supply,
    )
    dispositions_path = _write_json(
        output_root / "disclosure_v2_dispositions.json",
        DISCLOSURE_V2_EXTENSION_DISPOSITIONS,
    )
    final_path = _write_json(
        output_root / "final_decision.json",
        _final_decision(capacity),
    )
    input_paths = {
        "registry": args.registry.resolve(),
        "discovery_contract": args.discovery_contract.resolve(),
        "active_layout": args.active_layout.resolve(),
        "historical_exact_archive": (
            args.historical_exact_archive.resolve()
        ),
        "source_candidate_ledger": (
            args.source_candidate_ledger.resolve()
        ),
    }
    manifest = {
        "schema_version": (
            "cn_minute_static_production_lane_cem_v3_manifest_v1"
        ),
        "status": "QUALIFICATION_CLOSED_SUPPLY_BLOCKED",
        "authorization_id": AUTHORIZATION_ID,
        "repo_sha": str(args.repo_sha),
        "host": platform.node(),
        "task_id": str(args.task_id),
        "inputs": {
            name: {
                "path": str(path),
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
            for name, path in input_paths.items()
        },
        "artifacts": [
            {
                "path": path.name,
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
            for path in (
                supply_path,
                dispositions_path,
                final_path,
            )
        ],
        "behavior_probe_count": 0,
        "phase3cm_pair_count": 0,
        "campaign_arm_count": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    manifest["manifest_payload_hash"] = _stable_hash(manifest)
    manifest_path = _write_json(
        output_root / "artifact_manifest.json",
        manifest,
    )
    return {
        "status": manifest["status"],
        "old_supply_gate": supply,
        "final_decision": json.loads(
            final_path.read_text(encoding="utf-8")
        ),
        "manifest": str(manifest_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--discovery-contract", type=Path, required=True)
    parser.add_argument("--active-layout", type=Path, required=True)
    parser.add_argument(
        "--historical-exact-archive",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--source-candidate-ledger",
        type=Path,
        required=True,
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--task-id", default="")
    parser.add_argument("--allow-noncanonical-host", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
