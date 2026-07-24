"""Run the repaired MINUTE_STATIC V3 production OLD-supply gate.

The original V3 preflight incorrectly treated the Core-Pack information-core
discovery projection as the complete production field surface.  This runner
binds a campaign-local production-root contract to the existing Registry,
Grammar, compiler, real development sidecar, and historical exact/behavior
archives.  It performs no label, validation, holdout, or 2026 reads.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
from pathlib import Path
from typing import Any, Mapping, Sequence

import pyarrow.parquet as pq

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _write_parquet,
)
from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    _admit_behavior_unique,
)
from our_system_phase2.services.fixed_split_authority import (
    FixedSplitAuthority,
)
from our_system_phase2.services.portfolio_behavior_archive import (
    PortfolioBehaviorArchive,
    bounded_label_free_behavior_probe,
)
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
)


AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
AUTHORIZATION_ID = "MINUTE_STATIC_PRODUCTION_LANE_CEM_V3"
PRODUCTION_CONTRACT_VERSION = (
    "cn_minute_static_production_root_contract_v1"
)
ROUTE_ID = "MINUTE_STATIC"
OLD_SKELETON_ID = "cn.comp.v2.minute_static.field_spread"
MINIMUM_EXACT_SUPPLY = 72
MINIMUM_BEHAVIOR_SUPPLY = 48
BEHAVIOR_PROBE_CAP = 256
FROZEN_SUPPLY_SEED = 2026072419


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


def _historical_exact(*paths: Path) -> set[str]:
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
    return values


def _load_production_contract(
    path: Path,
    *,
    registry: UnifiedCapabilityRegistry,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    unsigned = {
        key: value
        for key, value in payload.items()
        if key != "contract_hash"
    }
    if payload.get("contract_hash") != _stable_hash(unsigned):
        raise RuntimeError("PRODUCTION_ROOT_CONTRACT_HASH_MISMATCH")
    expected = {
        "authorization_id": AUTHORIZATION_ID,
        "contract_version": PRODUCTION_CONTRACT_VERSION,
        "route_id": ROUTE_ID,
        "registry_hash": registry.registry_hash,
    }
    drift = [
        key
        for key, value in expected.items()
        if payload.get(key) != value
    ]
    gate = dict(payload.get("supply_gate") or {})
    if int(gate.get("minimum_post_archive_exact_unique") or -1) != (
        MINIMUM_EXACT_SUPPLY
    ):
        drift.append("minimum_post_archive_exact_unique")
    if int(gate.get("minimum_behavior_unique") or -1) != (
        MINIMUM_BEHAVIOR_SUPPLY
    ):
        drift.append("minimum_behavior_unique")
    if drift:
        raise RuntimeError(
            "PRODUCTION_ROOT_CONTRACT_BINDING_DRIFT:"
            + ",".join(sorted(drift))
        )
    roots = tuple(map(str, payload.get("route_roots") or ()))
    if len(roots) != len(set(roots)) or not roots:
        raise RuntimeError("PRODUCTION_ROOT_CONTRACT_ROOTS_INVALID")
    for field_id in roots:
        field = registry.resolve(field_id)
        if (
            not field.search_eligible
            or ROUTE_ID not in field.allowed_routes
            or field.entity_scope != "STOCK"
            or field.field_role not in {"primary", "interaction-only"}
        ):
            raise RuntimeError(
                "PRODUCTION_ROOT_NOT_REGISTRY_AUTHORIZED:" + field_id
            )
        if field.source_family in set(
            map(
                str,
                dict(
                    payload.get("excluded_source_families") or {}
                ),
            )
        ):
            raise RuntimeError(
                "PRODUCTION_ROOT_USES_EXCLUDED_SOURCE_FAMILY:"
                + field_id
            )
    return payload, roots


def _materializable_roots(
    *,
    registry: UnifiedCapabilityRegistry,
    route_roots: Sequence[str],
    active_fields: set[str],
) -> tuple[str, ...]:
    usable = []
    for field_id in route_roots:
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
    return tuple(usable)


def _enumerate_old_space(
    generator: RegistryDrivenGenerator,
) -> list[dict[str, Any]]:
    lane = generator.categorical_gene_space(
        ROUTE_ID,
        skeleton_id=OLD_SKELETON_ID,
    )
    categories = dict(lane["ordered_categories_by_slot"])
    if set(categories) != {
        "skeleton_id",
        "gene_surface_id",
        "field_pair_id",
    }:
        raise RuntimeError("MINUTE_STATIC_OLD_LANE_SURFACE_DRIFT")
    rows = []
    for ordinal, field_pair_id in enumerate(
        categories["field_pair_id"]
    ):
        pair = generator.propose_categorical_genes(
            ROUTE_ID,
            genes={
                "skeleton_id": categories["skeleton_id"][0],
                "gene_surface_id": categories["gene_surface_id"][0],
                "field_pair_id": field_pair_id,
            },
        )
        primary = dict(pair.candidate)
        control = dict(pair.control)
        rows.append(
            {
                "ordinal": ordinal,
                "field_pair_id": str(field_pair_id),
                "pair_id": str(primary.get("pair_id") or ""),
                "exact_identity": str(
                    primary.get("exact_identity") or ""
                ),
                "canonical_identity": str(
                    primary.get("canonical_identity") or ""
                ),
                "legal": bool(primary.get("legal"))
                and bool(control.get("legal")),
                "primary": primary,
                "control": control,
            }
        )
    return rows


def _post_archive_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    historical_exact: set[str],
) -> list[dict[str, Any]]:
    exact_seen: set[str] = set()
    canonical_seen: set[str] = set()
    selected = []
    ordered = sorted(
        (dict(row) for row in rows),
        key=lambda row: _stable_hash(
            {
                "seed": FROZEN_SUPPLY_SEED,
                "exact_identity": str(
                    row.get("exact_identity") or ""
                ),
            }
        ),
    )
    for row in ordered:
        exact = str(row.get("exact_identity") or "")
        canonical = str(row.get("canonical_identity") or "")
        if (
            not bool(row.get("legal"))
            or not exact
            or not canonical
            or exact in historical_exact
            or exact in exact_seen
            or canonical in canonical_seen
        ):
            continue
        exact_seen.add(exact)
        canonical_seen.add(canonical)
        selected.append(row)
    return selected


def _train_dates(split: FixedSplitAuthority) -> tuple[str, ...]:
    return tuple(
        row["trade_date"]
        for row in split.rows
        if row["split"] == "train"
    )


def _field_sidecars(
    layout: Mapping[str, Any],
    *,
    required_roots: Sequence[str],
) -> tuple[Path, ...]:
    if str(layout.get("data_role") or "") != "development_train_only":
        raise PermissionError("PRODUCTION_SIDECAR_NOT_TRAIN_ONLY")
    if any(
        int(layout.get(key) or 0)
        for key in (
            "validation_reads",
            "holdout_reads",
            "forward_2026_reads",
        )
    ):
        raise PermissionError("PRODUCTION_SIDECAR_FORBIDDEN_ACCESS")
    available = set(map(str, layout.get("fields") or ()))
    missing = sorted(set(required_roots) - available)
    if missing:
        raise RuntimeError(
            "PRODUCTION_SIDECAR_MISSING_ROOTS:" + ",".join(missing)
        )
    paths = tuple(
        Path(str(row["output_path"])).resolve()
        for row in layout.get("shards") or ()
    )
    if len(paths) != 16 or any(not path.is_file() for path in paths):
        raise RuntimeError("PRODUCTION_SIDECAR_SHARD_SET_INVALID")
    return paths


def _failure_decision(blocker: str) -> dict[str, Any]:
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
        "READINESS_BLOCKERS": [str(blocker)],
    }


def _input_artifact(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": _sha256(path),
        "bytes": path.stat().st_size,
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
    contract, route_roots = _load_production_contract(
        args.production_root_contract.resolve(),
        registry=registry,
    )
    layout = json.loads(
        args.active_layout.resolve().read_text(encoding="utf-8-sig")
    )
    active_fields = set(map(str, layout.get("fields") or ()))
    materializable = _materializable_roots(
        registry=registry,
        route_roots=route_roots,
        active_fields=active_fields,
    )
    if materializable != route_roots:
        missing = sorted(set(route_roots) - set(materializable))
        raise RuntimeError(
            "PRODUCTION_INPUT_AUTHORITY_MISMATCH:"
            + ",".join(missing)
        )
    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist={ROUTE_ID: materializable},
    )
    old_rows = _enumerate_old_space(generator)
    historical_exact = _historical_exact(
        args.historical_exact_archive.resolve(),
        args.source_candidate_ledger.resolve(),
    )
    post_archive = _post_archive_rows(
        old_rows,
        historical_exact=historical_exact,
    )
    exact_gate = len(post_archive) >= MINIMUM_EXACT_SUPPLY
    supply = {
        "schema_version": "cn_minute_static_old_supply_gate_v2",
        "status": (
            "EXACT_SUPPLY_PASS_BEHAVIOR_PENDING"
            if exact_gate
            else "FAIL_STOP_BEFORE_BEHAVIOR_AND_PHASE3CM"
        ),
        "input_repair": (
            "FULL_PRODUCTION_ROOT_PROJECTION_REPLACES_"
            "CORE_PACK_INFORMATION_CORE"
        ),
        "production_root_contract_hash": contract["contract_hash"],
        "production_root_count": len(route_roots),
        "materializable_production_roots": list(materializable),
        "atomic_ordered_field_pair_count": len(old_rows),
        "legal_pairs": sum(bool(row["legal"]) for row in old_rows),
        "exact_unique_pairs": len(
            {
                str(row["exact_identity"])
                for row in old_rows
                if bool(row["legal"])
            }
        ),
        "historical_exact_identity_count": len(historical_exact),
        "historical_overlap": len(old_rows) - len(post_archive),
        "post_archive_exact_supply": len(post_archive),
        "required_post_archive_exact_supply": MINIMUM_EXACT_SUPPLY,
        "required_behavior_supply": MINIMUM_BEHAVIOR_SUPPLY,
        "frozen_seed": FROZEN_SUPPLY_SEED,
        "ordering": "sha256(json(seed,exact_identity));ascending",
        "exact_gate": "PASS" if exact_gate else "FAIL",
        "behavior_gate": "PENDING" if exact_gate else "NOT_RUN",
        "sampled_phase3cm": "NOT_RUN_SUPPLY_GATE_INCOMPLETE",
        "full_phase3cm": "NOT_RUN_SUPPLY_GATE_INCOMPLETE",
    }
    supply_path = _write_json(
        output_root / "old_supply_gate.json",
        supply,
    )
    dispositions_path = _write_json(
        output_root / "disclosure_v2_dispositions.json",
        DISCLOSURE_V2_EXTENSION_DISPOSITIONS,
    )
    member_rows = [
        dict(member)
        for row in post_archive
        for member in (row["primary"], row["control"])
    ]
    candidate_path = _write_parquet(
        output_root / "old_post_archive_candidates.parquet",
        member_rows,
    )
    artifact_paths: list[Path] = [
        supply_path,
        dispositions_path,
        candidate_path,
    ]
    behavior_probe_count = 0
    behavior_unique = 0
    status = "QUALIFICATION_CLOSED_SUPPLY_BLOCKED"
    final_decision: dict[str, Any]

    if not exact_gate:
        blocker = (
            "OLD_POST_ARCHIVE_EXACT_SUPPLY_"
            f"{len(post_archive)}_BELOW_{MINIMUM_EXACT_SUPPLY}"
        )
        final_decision = _failure_decision(blocker)
    elif bool(getattr(args, "static_only", False)):
        status = "EXACT_SUPPLY_QUALIFIED_BEHAVIOR_PENDING_STATIC_ONLY"
        final_decision = {
            "NEXT_ACTION": "RUN_LABEL_FREE_BEHAVIOR_GATE",
            "TARGET_FAMILY_LARGE_SEARCH_READINESS": "PENDING_BEHAVIOR_GATE",
            "READINESS_BLOCKERS": [],
        }
    else:
        split = FixedSplitAuthority.read(args.split_manifest.resolve())
        if (
            str(layout.get("split_manifest_hash") or "")
            != split.manifest_hash
        ):
            raise RuntimeError("PRODUCTION_SIDECAR_SPLIT_HASH_DRIFT")
        field_paths = _field_sidecars(
            layout,
            required_roots=route_roots,
        )
        selected = post_archive[:BEHAVIOR_PROBE_CAP]
        selected_members = [
            dict(member)
            for row in selected
            for member in (row["primary"], row["control"])
        ]
        probe, audit = bounded_label_free_behavior_probe(
            candidates=selected_members,
            field_sidecars=field_paths,
            eligible_trade_dates=_train_dates(split),
            coordinate_binding=_stable_hash(
                {
                    "authorization_id": AUTHORIZATION_ID,
                    "contract_hash": contract["contract_hash"],
                    "split_hash": split.manifest_hash,
                    "seed": FROZEN_SUPPLY_SEED,
                }
            ),
            batch_id="minute_static_v3.old_supply",
            compute_threads=int(args.compute_threads),
            max_trade_times=30,
            max_trade_dates=1,
            date_selection="calendar_stratified",
            pair_batch_size=4,
        )
        behavior_probe_count = len(selected)
        behavior_archive = PortfolioBehaviorArchive.read_parquet(
            args.historical_behavior_archive.resolve()
        )
        admitted, decisions = _admit_behavior_unique(
            candidate_rows=selected_members,
            probe_rows=probe,
            historical_archive=behavior_archive,
        )
        behavior_unique = sum(
            str(row.get("pair_member_role") or "") == "PRIMARY"
            for row in admitted
        )
        probe_path = _write_parquet(
            output_root / "behavior_probe.parquet",
            probe,
        )
        audit_path = _write_json(
            output_root / "behavior_probe_audit.json",
            audit,
        )
        decisions_path = _write_parquet(
            output_root / "behavior_admission_decisions.parquet",
            decisions,
        )
        artifact_paths.extend(
            (probe_path, audit_path, decisions_path)
        )
        behavior_gate = behavior_unique >= MINIMUM_BEHAVIOR_SUPPLY
        supply.update(
            {
                "status": (
                    "OLD_SUPPLY_QUALIFIED"
                    if behavior_gate
                    else "FAIL_STOP_BEFORE_PHASE3CM"
                ),
                "behavior_probe_candidates": behavior_probe_count,
                "behavior_unique_pairs": behavior_unique,
                "behavior_gate": "PASS" if behavior_gate else "FAIL",
            }
        )
        _write_json(supply_path, supply)
        if behavior_gate:
            status = "OLD_SUPPLY_QUALIFIED_READY_FOR_PARITY"
            final_decision = {
                "NEXT_ACTION": (
                    "CONTINUE_TO_PRODUCTION_PARITY_AND_"
                    "SAMPLED_PHASE3CM_QUALIFICATION"
                ),
                "TARGET_FAMILY_LARGE_SEARCH_READINESS": (
                    "PENDING_DOWNSTREAM_QUALIFICATION"
                ),
                "READINESS_BLOCKERS": [],
            }
        else:
            blocker = (
                "OLD_BEHAVIOR_UNIQUE_SUPPLY_"
                f"{behavior_unique}_BELOW_{MINIMUM_BEHAVIOR_SUPPLY}"
            )
            final_decision = _failure_decision(blocker)

    final_path = _write_json(
        output_root / "supply_decision.json",
        final_decision,
    )
    artifact_paths.append(final_path)
    input_paths = {
        "registry": args.registry.resolve(),
        "production_root_contract": (
            args.production_root_contract.resolve()
        ),
        "active_layout": args.active_layout.resolve(),
        "historical_exact_archive": (
            args.historical_exact_archive.resolve()
        ),
        "source_candidate_ledger": (
            args.source_candidate_ledger.resolve()
        ),
    }
    if not bool(getattr(args, "static_only", False)) and exact_gate:
        input_paths.update(
            {
                "historical_behavior_archive": (
                    args.historical_behavior_archive.resolve()
                ),
                "split_manifest": args.split_manifest.resolve(),
            }
        )
    manifest = {
        "schema_version": (
            "cn_minute_static_production_lane_cem_v3_"
            "supply_repair_manifest_v2"
        ),
        "status": status,
        "authorization_id": AUTHORIZATION_ID,
        "repo_sha": str(args.repo_sha),
        "host": platform.node(),
        "task_id": str(args.task_id),
        "inputs": {
            name: _input_artifact(path)
            for name, path in input_paths.items()
        },
        "artifacts": [
            {
                "path": path.name,
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
            for path in artifact_paths
        ],
        "behavior_probe_count": behavior_probe_count,
        "behavior_unique_pairs": behavior_unique,
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
        "status": status,
        "old_supply_gate": supply,
        "supply_decision": final_decision,
        "manifest": str(manifest_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument(
        "--production-root-contract",
        type=Path,
        required=True,
    )
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
    parser.add_argument(
        "--historical-behavior-archive",
        type=Path,
    )
    parser.add_argument("--split-manifest", type=Path)
    parser.add_argument("--compute-threads", type=int, default=30)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--task-id", default="")
    parser.add_argument("--allow-noncanonical-host", action="store_true")
    parser.add_argument("--static-only", action="store_true")
    args = parser.parse_args()
    if not args.static_only and (
        args.historical_behavior_archive is None
        or args.split_manifest is None
    ):
        parser.error(
            "--historical-behavior-archive and --split-manifest "
            "are required unless --static-only"
        )
    print(json.dumps(run(args), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
