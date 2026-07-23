"""Bounded CatCMA search-policy qualification on the existing CN search fabric.

This launcher compares scheduled attempts from the deterministic
RegistryDrivenGenerator stream with official CatCMAwM selection over frozen,
registry-authorized Grammar gene slots. It owns no field, formula constructor,
route, compiler, admission, evaluator, or promotion authority.
"""

from __future__ import annotations

import argparse
import copy
import json
import platform
import statistics
import time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _artifact,
    _clock_for_route,
    _context_and_binding,
    _join_full_behavior_identities,
    _outcome_rows,
    _probe_pack,
    _sha256,
    _stable_hash,
    _train_dates,
    _write_json,
    _write_parquet,
)
from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    _add_resolved_behavior_rows,
    _admit_behavior_unique,
    _bind_purity,
    _registry_binding,
    _run_phase3cm_monitored,
    _runtime_envelope,
    _runtime_gate,
    materialized_schema_binding,
)
from our_system_phase2.services.catcma_search_adapter import (
    BEHAVIOR_BLOCKED,
    CONTROL_BLOCKED,
    DETERMINISTIC_INVALID,
    EVALUATED,
    SUPPORT_BLOCKED,
    CatCMASearchAdapter,
    ExactGeneSemantics,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.portfolio_behavior_archive import (
    PortfolioBehaviorArchive,
)
from our_system_phase2.services.split_boundary_label_purity import (
    audit_split_boundary_label_purity,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.phase3cm_streaming_expression import (
    unsupported_streaming_operators,
)
from our_system_phase2.services.typed_primitive_gate import expression_fields
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    RegistryDrivenGenerator,
    load_development_discovery_root_authority,
)


REPO = Path(__file__).resolve().parents[3]
AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
ROUTES = (
    "INTRADAY_STATE_TRANSITION",
    "DISCLOSURE_EVENT",
    "SLOW_TEMPORAL_CHANGE",
)
ARMS = ("registry_baseline", "official_catcma")
CHECKPOINT_COUNT = 4
POPULATION_SIZE = 16
TOTAL_SCHEDULED_PAIR_BUDGET = (
    len(ARMS) * len(ROUTES) * CHECKPOINT_COUNT * POPULATION_SIZE
)
PAIR_BATCH_SIZES = {"active_bar": 4, "stock_session": 4}
CATCMA_WHEEL_SHA256 = (
    "ccf61c73d5792cf44b50672b63f28c590082d1c1f5ab5155e2dd6e5305427cad"
)
REFERENCE_EFFECTIVE_CORES = 19.280918559822002
REFERENCE_HOST_LOGICAL_OCCUPANCY = 0.6025287049944376
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
MAXIMUM_CACHE_BYTES = 8 * 1024**3
MAX_CONCENTRATION_HHI_DETERIORATION = 0.10
MAX_CATEGORY_ASK_SHARE = 0.25


def _read_rows(path: Path) -> list[dict[str, Any]]:
    source = Path(path)
    if source.suffix.lower() == ".parquet":
        return pd.read_parquet(source).fillna("").to_dict(orient="records")
    if source.suffix.lower() == ".json":
        payload = json.loads(source.read_text(encoding="utf-8-sig"))
        return [dict(row) for row in payload]
    raise ValueError(f"unsupported table: {source}")


def _copy_behavior_archive(
    archive: PortfolioBehaviorArchive,
) -> PortfolioBehaviorArchive:
    return PortfolioBehaviorArchive([dict(row) for row in archive.rows])


def _verify_artifacts(root: Path, manifest: Mapping[str, Any]) -> None:
    claimed_payload_hash = str(manifest.get("manifest_payload_hash") or "")
    if claimed_payload_hash:
        unsigned = dict(manifest)
        unsigned.pop("manifest_payload_hash", None)
        if _stable_hash(unsigned) != claimed_payload_hash:
            raise RuntimeError(f"IMMUTABLE_MANIFEST_PAYLOAD_DRIFT:{root}")
    for artifact in manifest.get("artifacts") or ():
        path = root / str(artifact["path"])
        if (
            not path.is_file()
            or _sha256(path) != str(artifact["sha256"])
            or path.stat().st_size != int(artifact["bytes"])
        ):
            raise RuntimeError(f"IMMUTABLE_ARTIFACT_DRIFT:{path}")


def _qualification_authority(
    *,
    path: Path,
    source_receipt_path: Path,
    cmaes_wheel_path: Path,
    seed_base: int,
    active_threads: int,
    session_threads: int,
) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    expected = {
        "execution_authorized": True,
        "authorized_host": AUTHORIZED_HOST,
        "routes": list(ROUTES),
        "arms": list(ARMS),
        "checkpoint_count": CHECKPOINT_COUNT,
        "population_size": POPULATION_SIZE,
        "total_scheduled_matched_pair_budget": TOTAL_SCHEDULED_PAIR_BUDGET,
        "optimizer": "official_cmaes.CatCMAwM",
        "optimizer_package_version": "0.13.0",
        "optimizer_wheel_sha256": CATCMA_WHEEL_SHA256,
        "active_threads": int(active_threads),
        "session_threads": int(session_threads),
        "active_pair_batch_size": 4,
        "session_pair_batch_size": 4,
        "cache_cap_bytes": MAXIMUM_CACHE_BYTES,
        "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "seed_base": int(seed_base),
        "validation": "FORBIDDEN",
        "holdout": "SEALED",
        "forward_2026": "SEALED",
        "promotion": "FORBIDDEN",
    }
    drift = [
        key for key, value in expected.items() if payload.get(key) != value
    ]
    if _sha256(cmaes_wheel_path).lower() != CATCMA_WHEEL_SHA256:
        drift.append("optimizer_wheel_file_sha256")
    source_receipt = json.loads(
        source_receipt_path.read_text(encoding="utf-8-sig")
    )
    if str(payload.get("source_receipt_payload_hash") or "") != _stable_hash(
        source_receipt
    ):
        drift.append("source_receipt_payload_hash")
    if drift:
        raise RuntimeError(
            "SEARCH_POLICY_QUALIFICATION_AUTHORITY_MISMATCH:"
            + ",".join(sorted(set(drift)))
        )
    return {
        "schema_version": "cn_search_policy_qualification_authority_binding_v1",
        "status": "BOUNDED_QUALIFICATION_AUTHORIZED",
        "authorization": _artifact(path),
        "source_receipt": _artifact(source_receipt_path),
        "cmaes_wheel": _artifact(cmaes_wheel_path),
        "frozen": expected,
    }


def _source_campaign_binding(
    *,
    source_root: Path,
    source_receipt_path: Path,
    historical_candidate_archive: Path,
    historical_archive_manifest: Path,
) -> tuple[set[str], PortfolioBehaviorArchive, dict[str, Any]]:
    receipt = json.loads(source_receipt_path.read_text(encoding="utf-8-sig"))
    expected = dict(
        (receipt.get("immutable_train_verification") or {}).get(
            "root_train_artifact_hashes"
        )
        or {}
    )
    artifacts = {
        "candidate_ledger.parquet": source_root / "candidate_ledger.parquet",
        "behavior_archive.parquet": source_root / "behavior_archive.parquet",
    }
    train_manifest = source_root / "train_complete_manifest.json"
    drift = [
        name
        for name, path in artifacts.items()
        if not path.is_file()
        or _sha256(path).lower() != str(expected.get(name) or "").lower()
    ]
    expected_train = str(
        (receipt.get("immutable_train_verification") or {}).get(
            "train_manifest_sha256"
        )
        or ""
    )
    if (
        not train_manifest.is_file()
        or _sha256(train_manifest).lower() != expected_train.lower()
    ):
        drift.append("train_complete_manifest.json")
    history = json.loads(
        historical_archive_manifest.read_text(encoding="utf-8-sig")
    )
    historical_candidate_receipt = dict(
        history.get("candidate_exact_archive") or {}
    )
    if (
        str(history.get("status") or "") != "PASS"
        or not historical_candidate_archive.is_file()
        or _sha256(historical_candidate_archive).lower()
        != str(historical_candidate_receipt.get("sha256") or "").lower()
    ):
        drift.append("historical_candidate_exact_archive")
    if drift:
        raise RuntimeError("SOURCE_CAMPAIGN_IMMUTABILITY_DRIFT:" + ",".join(drift))

    exact: set[str] = set()
    for path in (historical_candidate_archive, artifacts["candidate_ledger.parquet"]):
        for row in _read_rows(path):
            identity = str(row.get("exact_identity") or "")
            if identity:
                exact.add(identity)
    if not exact:
        raise RuntimeError("SOURCE_SEARCH_MEMORY_EMPTY")
    behavior_archive = PortfolioBehaviorArchive.read_parquet(
        artifacts["behavior_archive.parquet"]
    )
    return exact, behavior_archive, {
        "schema_version": "cn_search_policy_source_campaign_binding_v1",
        "status": "IMMUTABLE_COMPLETED_SEARCH_MEMORY_BOUND",
        "campaign_id": str(receipt.get("campaign_id") or ""),
        "train_manifest": _artifact(train_manifest),
        "candidate_ledger": _artifact(artifacts["candidate_ledger.parquet"]),
        "behavior_archive": _artifact(artifacts["behavior_archive.parquet"]),
        "historical_candidate_archive": _artifact(historical_candidate_archive),
        "historical_archive_manifest": _artifact(historical_archive_manifest),
        "exact_identity_count": len(exact),
        "exact_identity_digest": _stable_hash(sorted(exact)),
        "behavior_row_count": len(behavior_archive.rows),
        "behavior_digest": _stable_hash(list(behavior_archive.rows)),
        "reward_memory_imported": False,
        "scheduler_memory_imported": False,
    }


def _materialized_route_root_allowlists(
    *,
    registry: UnifiedCapabilityRegistry,
    discovery_allowlists: Mapping[str, Sequence[str]],
    schema_by_backend: Mapping[str, set[str]],
) -> dict[str, tuple[str, ...]]:
    output: dict[str, tuple[str, ...]] = {}
    for route_id in ROUTES:
        available = schema_by_backend[_clock_for_route(route_id)]
        usable = []
        for field_id in discovery_allowlists[route_id]:
            field = registry.resolve(str(field_id))
            materialization = str(
                field.metadata.get("materialization_expression") or ""
            )
            physical_leaves = (
                expression_fields(materialization)
                if materialization
                else {field.field_id}
            )
            if set(map(str, physical_leaves)).issubset(available):
                usable.append(field.field_id)
        if not usable:
            raise RuntimeError(
                f"MATERIALIZED_ROUTE_ROOTS_EMPTY:{route_id}"
            )
        output[route_id] = tuple(sorted(set(usable)))
    return output


def _freeze_gene_spaces(
    *,
    output_root: Path,
    generator: RegistryDrivenGenerator,
    registry_hash: str,
    root_contract_hash: str,
    grammar_hash: str,
    input_hashes: Mapping[str, str],
) -> tuple[
    dict[str, ExactGeneSemantics],
    Path,
    Path,
]:
    space_path = output_root / "categorical_gene_spaces.json"
    semantics_path = output_root / "exact_gene_semantics.json"
    manifest_path = output_root / "categorical_gene_spaces_manifest.json"
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        if dict(manifest.get("input_hashes") or {}) != dict(input_hashes):
            raise RuntimeError("CATEGORICAL_GENE_SPACE_INPUT_DRIFT")
        _verify_artifacts(output_root, manifest)
        payload = json.loads(space_path.read_text(encoding="utf-8-sig"))
    else:
        payload = {
            "schema_version": "cn_search_policy_categorical_gene_spaces_v1",
            "routes": {
                route_id: generator.categorical_gene_space(route_id)
                for route_id in ROUTES
            },
        }
        _write_json(space_path, payload)
    semantics = {
        route_id: ExactGeneSemantics.freeze(
            route_id=route_id,
            ordered_categories_by_slot=dict(
                payload["routes"][route_id]["ordered_categories_by_slot"]
            ),
            none_semantics=dict(
                payload["routes"][route_id]["none_semantics"]
            ),
            skeleton_compatibility=dict(
                payload["routes"][route_id]["skeleton_compatibility"]
            ),
            registry_hash=registry_hash,
            root_contract_hash=root_contract_hash,
            grammar_hash=grammar_hash,
        )
        for route_id in ROUTES
    }
    _write_json(
        semantics_path,
        {route_id: value.to_dict() for route_id, value in semantics.items()},
    )
    if not manifest_path.is_file():
        manifest = {
            "schema_version": "cn_search_policy_gene_space_manifest_v1",
            "status": "FROZEN_IMMUTABLE",
            "input_hashes": dict(input_hashes),
            "artifacts": [
                _artifact(space_path, root=output_root),
                _artifact(semantics_path, root=output_root),
            ],
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }
        manifest["manifest_payload_hash"] = _stable_hash(manifest)
        _write_json(manifest_path, manifest)
    return semantics, semantics_path, manifest_path


def build_baseline_population(
    *,
    route_id: str,
    checkpoint_index: int,
    seed: int,
) -> list[dict[str, Any]]:
    """Schedule attempts without searching past duplicate or invalid supply."""

    start = checkpoint_index * POPULATION_SIZE
    return [
        {
            "proposal_id": _stable_hash(
                {
                    "arm": "registry_baseline",
                    "route_id": route_id,
                    "checkpoint_index": checkpoint_index,
                    "ask_ordinal": ordinal,
                    "attempt_index": start + ordinal,
                    "seed": int(seed),
                }
            )[:24],
            "checkpoint_id": f"checkpoint_{checkpoint_index + 1:03d}",
            "generation": checkpoint_index,
            "ask_ordinal": ordinal,
            "route_id": route_id,
            "generation_attempt_index": start + ordinal,
            "generation_seed": int(seed),
            "category_id": f"baseline_attempt_{start + ordinal:05d}",
            "duplicate_in_population": False,
        }
        for ordinal in range(POPULATION_SIZE)
    ]


def _materialize_population(
    *,
    asked: Sequence[Mapping[str, Any]],
    generator: RegistryDrivenGenerator,
    schema_by_backend: Mapping[str, set[str]],
) -> list[dict[str, Any]]:
    output = []
    for source in asked:
        row = copy.deepcopy(dict(source))
        route_id = str(row["route_id"])
        try:
            if "genes" in row:
                pair = generator.propose_categorical_genes(
                    route_id, genes=dict(row["genes"])
                )
            else:
                pair = generator.propose_attempt(
                    route_id,
                    attempt_index=int(row["generation_attempt_index"]),
                    seed=int(row["generation_seed"]),
                )
            primary, control = dict(pair.candidate), dict(pair.control)
            unsupported = unsupported_streaming_operators(
                (primary["expression"], control["expression"])
            )
            if unsupported:
                raise ValueError(
                    "MATERIALIZATION_UNSUPPORTED_OPERATOR:"
                    + ",".join(unsupported)
                )
            available = schema_by_backend[_clock_for_route(route_id)]
            required = {
                str(field)
                for member in (primary, control)
                for field in expression_fields(str(member["expression"]))
            }
            if not required.issubset(available):
                raise ValueError(
                    "MATERIALIZATION_MISSING_FIELDS:"
                    + ",".join(sorted(required - available))
                )
            identities = (
                str(primary.get("exact_identity") or ""),
                str(control.get("exact_identity") or ""),
            )
            if (
                not bool(primary.get("legal"))
                or not bool(control.get("legal"))
                or not all(identities)
                or len(set(identities)) != 2
            ):
                raise ValueError("TYPED_COMPILER_REJECTED_PAIR")
            row.update(
                {
                    "construction_status": "LEGAL",
                    "pair_id": str(primary["pair_id"]),
                    "exact_identity": identities[0],
                    "control_exact_identity": identities[1],
                    "skeleton_id": str(primary.get("skeleton_id") or ""),
                    "primary": primary,
                    "control": control,
                }
            )
        except (KeyError, ValueError) as exc:
            invalid_identity = "invalid:" + _stable_hash(
                {
                    "route_id": route_id,
                    "category_id": row["category_id"],
                    "genes": row.get("genes"),
                    "attempt_index": row.get("generation_attempt_index"),
                    "error": str(exc),
                }
            )
            row.update(
                {
                    "construction_status": DETERMINISTIC_INVALID,
                    "construction_error": str(exc),
                    "pair_id": invalid_identity,
                    "exact_identity": invalid_identity,
                    "control_exact_identity": "",
                    "skeleton_id": str(
                        (row.get("genes") or {}).get("skeleton_id") or ""
                    ),
                }
            )
        output.append(row)
    return output


def _phase3cm_wall_seconds(checkpoint_root: Path) -> float:
    total = 0.0
    for backend in ("active_bar", "stock_session"):
        path = (
            checkpoint_root
            / "phase3cm"
            / backend
            / "CN_STREAMING_BACKEND_RESULT.json"
        )
        if path.is_file():
            total += float(
                json.loads(path.read_text(encoding="utf-8-sig")).get(
                    "wall_seconds"
                )
                or 0.0
            )
    return total


def _outcome_class(row: Mapping[str, Any]) -> str:
    if str(row.get("pair_evaluation_status") or "") == "PAIR_EVALUATED":
        return EVALUATED
    blockers = str(row.get("pair_evaluation_blockers") or "")
    if "primary_signal" in blockers or "empty_pair_support" in blockers:
        return SUPPORT_BLOCKED
    if "control_signal" in blockers:
        return CONTROL_BLOCKED
    return SUPPORT_BLOCKED


def _minimum_free_memory(checkpoint_root: Path) -> int | None:
    values = []
    for path in (checkpoint_root / "phase3cm").glob(
        "*/runtime_samples.json"
    ):
        for row in json.loads(path.read_text(encoding="utf-8-sig")):
            value = int(row.get("available_memory_bytes") or 0)
            if value:
                values.append(value)
    return min(values) if values else None


def _checkpoint_manifest(
    *,
    checkpoint_root: Path,
    arm: str,
    checkpoint_id: str,
    paths: Sequence[Path],
    access_receipts: Sequence[Mapping[str, Any]],
    input_hashes: Mapping[str, str],
) -> Path:
    artifacts = [
        _artifact(path, root=checkpoint_root)
        for path in paths
        if path.is_file()
    ]
    manifest = {
        "schema_version": "cn_search_policy_arm_checkpoint_manifest_v1",
        "status": "BATCH_CLOSED_IMMUTABLE",
        "arm": arm,
        "checkpoint_id": checkpoint_id,
        "input_hashes": dict(input_hashes),
        "artifacts": artifacts,
        "access_receipts": [dict(row) for row in access_receipts],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "evaluation_name": (
            "full-coordinate development Phase3CM pair evaluation"
        ),
    }
    manifest["manifest_payload_hash"] = _stable_hash(manifest)
    return _write_json(checkpoint_root / "batch_manifest.json", manifest)


def _load_closed_arm_state(
    *,
    output_root: Path,
    arm: str,
    initial_exact: set[str],
    initial_behavior: PortfolioBehaviorArchive,
) -> tuple[
    set[str],
    PortfolioBehaviorArchive,
    dict[str, list[dict[str, Any]]],
    int,
]:
    exact = set(initial_exact)
    behavior = _copy_behavior_archive(initial_behavior)
    histories: dict[str, list[dict[str, Any]]] = {
        route_id: [] for route_id in ROUTES
    }
    closed_count = 0
    gap_seen = False
    for checkpoint_index in range(CHECKPOINT_COUNT):
        root = (
            output_root
            / "arms"
            / arm
            / f"checkpoint_{checkpoint_index + 1:03d}"
        )
        manifest_path = root / "batch_manifest.json"
        if not manifest_path.is_file():
            gap_seen = True
            continue
        if gap_seen:
            raise RuntimeError(f"NONCONTIGUOUS_CLOSED_CHECKPOINTS:{arm}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        if str(manifest.get("status") or "") != "BATCH_CLOSED_IMMUTABLE":
            raise RuntimeError(f"INVALID_CLOSED_CHECKPOINT:{root}")
        _verify_artifacts(root, manifest)
        asks = json.loads(
            (root / "asked_population.json").read_text(encoding="utf-8-sig")
        )
        exact.update(str(row["exact_identity"]) for row in asks)
        for name in ("behavior_probe.parquet", "full_behavior.parquet"):
            path = root / name
            if path.is_file():
                _add_resolved_behavior_rows(behavior, _read_rows(path))
        if arm == "official_catcma":
            tell_path = root / "optimizer_tell_receipts.json"
            for row in json.loads(tell_path.read_text(encoding="utf-8-sig")):
                histories[str(row["route_id"])].append(dict(row["receipt"]))
        closed_count += 1
    return exact, behavior, histories, closed_count


def _execute_arm_checkpoint(
    *,
    arm: str,
    checkpoint_index: int,
    output_root: Path,
    adapters: Mapping[str, CatCMASearchAdapter],
    generator: RegistryDrivenGenerator,
    schema_by_backend: Mapping[str, set[str]],
    seed_base: int,
    exact_seen: set[str],
    behavior_archive: PortfolioBehaviorArchive,
    registry: UnifiedCapabilityRegistry,
    split: FixedSplitAuthority,
    field_roots: Mapping[str, Path],
    label_roots: Mapping[str, Path],
    purity_path: Path,
    sidecar_closure: Path,
    compute_threads: Mapping[str, int],
    deadline_epoch: float,
    frozen_contract_path: Path,
    gene_space_manifest_path: Path,
) -> dict[str, Any]:
    checkpoint_id = f"checkpoint_{checkpoint_index + 1:03d}"
    checkpoint_root = output_root / "arms" / arm / checkpoint_id
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    if (checkpoint_root / "batch_manifest.json").is_file():
        return json.loads(
            (checkpoint_root / "checkpoint_summary.json").read_text(
                encoding="utf-8-sig"
            )
        )

    ask_path = checkpoint_root / "asked_population.json"
    asked: list[dict[str, Any]] = []
    ask_started = time.perf_counter()
    if arm == "registry_baseline":
        for route_index, route_id in enumerate(ROUTES):
            asked.extend(
                build_baseline_population(
                    route_id=route_id,
                    checkpoint_index=checkpoint_index,
                    seed=seed_base + route_index * 1009,
                )
            )
    else:
        expected_by_route: dict[str, list[dict[str, str]]] = {}
        if ask_path.is_file():
            previous = json.loads(ask_path.read_text(encoding="utf-8-sig"))
            for route_id in ROUTES:
                expected_by_route[route_id] = [
                    dict(row["genes"])
                    for row in previous
                    if str(row["route_id"]) == route_id
                ]
        for route_id in ROUTES:
            asked.extend(
                adapters[route_id].ask_population(
                    checkpoint_id=checkpoint_id,
                    expected_genes=expected_by_route.get(route_id),
                )
            )
    ask_seconds = time.perf_counter() - ask_started
    asked = _materialize_population(
        asked=asked,
        generator=generator,
        schema_by_backend=schema_by_backend,
    )
    _write_json(ask_path, asked)
    if len(asked) != len(ROUTES) * POPULATION_SIZE:
        raise RuntimeError("INCOMPLETE_ARM_POPULATION")

    observations: dict[str, dict[str, Any]] = {}
    unique_asked: list[dict[str, Any]] = []
    generation_exact: set[str] = set()
    for row in asked:
        identity = str(row["exact_identity"])
        if str(row.get("construction_status") or "") != "LEGAL":
            observations[str(row["proposal_id"])] = {
                "proposal_id": str(row["proposal_id"]),
                "route_id": str(row["route_id"]),
                "pair_id": str(row["pair_id"]),
                "category_id": str(row["category_id"]),
                "exact_identity": identity,
                "skeleton_id": str(row.get("skeleton_id") or ""),
                "outcome_class": DETERMINISTIC_INVALID,
                "outcome_reason": str(
                    row.get("construction_error")
                    or "DETERMINISTIC_CONSTRUCTION_INVALID"
                ),
            }
        elif identity in exact_seen or identity in generation_exact:
            observations[str(row["proposal_id"])] = {
                "proposal_id": str(row["proposal_id"]),
                "route_id": str(row["route_id"]),
                "pair_id": str(row["pair_id"]),
                "category_id": str(row["category_id"]),
                "exact_identity": identity,
                "skeleton_id": str(row.get("skeleton_id") or ""),
                "outcome_class": DETERMINISTIC_INVALID,
                "outcome_reason": "EXACT_SEARCH_MEMORY_DUPLICATE",
            }
        else:
            generation_exact.add(identity)
            unique_asked.append(row)
    candidate_rows = [
        dict(member)
        for row in unique_asked
        for member in (row["primary"], row["control"])
    ]
    candidate_path = _write_parquet(
        checkpoint_root / "candidate_attempts.parquet", candidate_rows
    )

    probe_started = time.perf_counter()
    probe_path = checkpoint_root / "behavior_probe.parquet"
    probe_audit_path = checkpoint_root / "behavior_probe_audit.json"
    preadmission_path = checkpoint_root / "pre_admission_complete.json"
    if preadmission_path.is_file():
        preadmission = json.loads(
            preadmission_path.read_text(encoding="utf-8-sig")
        )
        if (
            str(preadmission.get("candidate_attempts_sha256") or "")
            != _sha256(candidate_path)
            or str(preadmission.get("behavior_probe_sha256") or "")
            != _sha256(probe_path)
        ):
            raise RuntimeError("PRE_ADMISSION_ARTIFACT_DRIFT")
        probe_rows = _read_rows(probe_path)
        probe_audit = json.loads(
            probe_audit_path.read_text(encoding="utf-8-sig")
        )
        probe_seconds = float(preadmission["probe_wall_seconds"])
    else:
        probe_rows, probe_audit = _probe_pack(
            candidate_rows=candidate_rows,
            field_roots=field_roots,
            train_dates=_train_dates(split),
            coordinate_binding=_stable_hash(
                {
                    "arm": arm,
                    "checkpoint": checkpoint_id,
                    "frozen_contract": _sha256(frozen_contract_path),
                    "gene_space": _sha256(gene_space_manifest_path),
                    "split": split.manifest_hash,
                }
            ),
            batch_id=f"{arm}.{checkpoint_id}",
            compute_threads=compute_threads,
        )
        probe_seconds = time.perf_counter() - probe_started
        _write_parquet(probe_path, probe_rows)
        _write_json(probe_audit_path, probe_audit)
        _write_json(
            preadmission_path,
            {
                "status": "PRE_ADMISSION_COMPLETE",
                "candidate_attempts_sha256": _sha256(candidate_path),
                "behavior_probe_sha256": _sha256(probe_path),
                "behavior_probe_audit_sha256": _sha256(probe_audit_path),
                "probe_wall_seconds": probe_seconds,
            },
        )
    admitted, decisions = _admit_behavior_unique(
        candidate_rows=candidate_rows,
        probe_rows=probe_rows,
        historical_archive=_copy_behavior_archive(behavior_archive),
    )
    decisions_path = _write_parquet(
        checkpoint_root / "admission_decisions.parquet", decisions
    )
    decision_by_pair = {
        str(row["pair_id"]): row for row in decisions
    }
    first_ask_by_pair = {
        str(row["pair_id"]): row for row in unique_asked
    }
    for pair_id, ask in first_ask_by_pair.items():
        decision = decision_by_pair[pair_id]
        if str(decision.get("admission_decision") or "") != "ADMIT":
            observations[str(ask["proposal_id"])] = {
                "proposal_id": str(ask["proposal_id"]),
                "route_id": str(ask["route_id"]),
                "pair_id": pair_id,
                "category_id": str(ask["category_id"]),
                "exact_identity": str(ask["exact_identity"]),
                "skeleton_id": str(ask.get("skeleton_id") or ""),
                "outcome_class": BEHAVIOR_BLOCKED,
                "outcome_reason": str(
                    decision.get("admission_reason") or "BEHAVIOR_BLOCKED"
                ),
            }

    access_receipts: list[dict[str, Any]] = []
    binding_path: Path | None = None
    table_paths: dict[str, Path] = {}
    if admitted:
        binding_path, table_paths = _context_and_binding(
            batch_root=checkpoint_root,
            candidates=admitted,
            registry=registry,
            split=split,
            data_release_hash=_sha256(sidecar_closure),
        )
        _bind_purity(binding_path, purity_path)
        try:
            access_receipts = _run_phase3cm_monitored(
                checkpoint_id=f"{arm}.{checkpoint_id}",
                checkpoint_root=checkpoint_root,
                binding_path=binding_path,
                table_paths=table_paths,
                split_manifest=split.manifest_path,
                field_roots=field_roots,
                label_roots=label_roots,
                purity_path=purity_path,
                compute_threads=compute_threads,
                deadline_epoch=deadline_epoch,
                pair_batch_sizes=PAIR_BATCH_SIZES,
            )
        except Exception as exc:
            _write_json(
                checkpoint_root / "infrastructure_incident.json",
                {
                    "status": "GENERATION_INVALID_NO_TELL",
                    "arm": arm,
                    "checkpoint": checkpoint_id,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "route_health_mutation": "FORBIDDEN",
                },
            )
            raise

    outcomes, full_behavior = _outcome_rows(checkpoint_root)
    if admitted:
        full_behavior = _join_full_behavior_identities(
            full_behavior, probe_rows
        )
    outcome_by_pair = {str(row["pair_id"]): row for row in outcomes}
    admitted_pair_ids = {
        str(row["pair_id"])
        for row in admitted
        if str(row.get("pair_member_role") or "") == "PRIMARY"
    }
    if admitted_pair_ids != set(outcome_by_pair):
        raise RuntimeError("COMPLETED_EVALUATOR_OUTCOME_COVERAGE_MISMATCH")
    for pair_id in admitted_pair_ids:
        ask = first_ask_by_pair[pair_id]
        outcome = outcome_by_pair[pair_id]
        outcome_class = _outcome_class(outcome)
        row = {
            "proposal_id": str(ask["proposal_id"]),
            "route_id": str(ask["route_id"]),
            "pair_id": pair_id,
            "category_id": str(ask["category_id"]),
            "exact_identity": str(ask["exact_identity"]),
            "skeleton_id": str(ask.get("skeleton_id") or ""),
            "outcome_class": outcome_class,
            "outcome_reason": str(
                outcome.get("pair_evaluation_blockers") or "PAIR_EVALUATED"
            ),
        }
        if outcome_class == EVALUATED:
            row["signed_matched_increment"] = float(
                outcome["matched_net_increment"]
            )
        observations[str(ask["proposal_id"])] = row
    if set(observations) != {
        str(row["proposal_id"]) for row in asked
    }:
        raise RuntimeError("ASK_TELL_OBSERVATION_COVERAGE_MISMATCH")
    ordered_observations = [
        observations[str(row["proposal_id"])] for row in asked
    ]

    tell_receipts: list[dict[str, Any]] = []
    if arm == "official_catcma":
        for route_id in ROUTES:
            route_observations = [
                row
                for row in ordered_observations
                if str(row["route_id"]) == route_id
            ]
            tell_receipts.append(
                {
                    "route_id": route_id,
                    "receipt": adapters[route_id].tell_population(
                        route_observations
                    ),
                }
            )
    tell_path = _write_json(
        checkpoint_root / "optimizer_tell_receipts.json", tell_receipts
    )
    observation_path = _write_parquet(
        checkpoint_root / "optimizer_observations.parquet",
        ordered_observations,
    )
    full_behavior_path = _write_parquet(
        checkpoint_root / "full_behavior.parquet", full_behavior
    )
    _add_resolved_behavior_rows(behavior_archive, probe_rows)
    _add_resolved_behavior_rows(behavior_archive, full_behavior)
    exact_seen.update(str(row["exact_identity"]) for row in asked)

    gate = (
        _runtime_gate(checkpoint_root, compute_threads)
        if admitted
        else {
            "schema_version": "cn_medium_campaign_runtime_utilization_gate_v3",
            "status": "NOT_EVALUATED_NO_ADMITTED_PAIRS",
            "backends": {},
            "bounded_concurrency_adjustment_count": 0,
        }
    )
    gate_path = _write_json(
        checkpoint_root / "runtime_utilization_gate.json", gate
    )
    evaluated = [
        row
        for row in ordered_observations
        if row["outcome_class"] == EVALUATED
    ]
    phase3cm_wall = _phase3cm_wall_seconds(checkpoint_root)
    summary = {
        "schema_version": "cn_search_policy_arm_checkpoint_summary_v1",
        "arm": arm,
        "checkpoint": checkpoint_id,
        "scheduled_pairs": len(asked),
        "exact_unique_asked_pairs": len(unique_asked),
        "behavior_admitted_pairs": len(admitted_pair_ids),
        "evaluated_pairs": len(evaluated),
        "positive_matched_pairs": sum(
            float(row["signed_matched_increment"]) > 0.0
            for row in evaluated
        ),
        "ask_wall_seconds": ask_seconds,
        "probe_wall_seconds": probe_seconds,
        "phase3cm_wall_seconds": phase3cm_wall,
        "active_wall_seconds": ask_seconds + probe_seconds + phase3cm_wall,
        "minimum_free_memory_bytes": _minimum_free_memory(checkpoint_root),
        "runtime_gate_status": str(gate.get("status") or ""),
        "population_contract": "FULL_ASK_EVALUATE_OR_PENALIZE_THEN_TELL",
        "tell_observation_count": len(ordered_observations),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    summary_path = _write_json(
        checkpoint_root / "checkpoint_summary.json", summary
    )
    artifact_paths = [
        ask_path,
        candidate_path,
        probe_path,
        probe_audit_path,
        preadmission_path,
        decisions_path,
        observation_path,
        full_behavior_path,
        tell_path,
        gate_path,
        summary_path,
        *table_paths.values(),
    ]
    if binding_path is not None:
        artifact_paths.append(binding_path)
    artifact_paths.extend(
        Path(str(row["result_path"]))
        for row in access_receipts
        if Path(str(row["result_path"])).is_file()
    )
    _checkpoint_manifest(
        checkpoint_root=checkpoint_root,
        arm=arm,
        checkpoint_id=checkpoint_id,
        paths=artifact_paths,
        access_receipts=access_receipts,
        input_hashes={
            "frozen_contract": _sha256(frozen_contract_path),
            "categorical_gene_spaces_manifest": _sha256(
                gene_space_manifest_path
            ),
            "prior_arm_checkpoint": (
                _sha256(
                    checkpoint_root.parent
                    / f"checkpoint_{checkpoint_index:03d}"
                    / "batch_manifest.json"
                )
                if checkpoint_index
                else "GENESIS"
            ),
        },
    )
    return summary


def _hhi(values: Sequence[str]) -> float:
    if not values:
        return 1.0
    counts = Counter(values)
    total = len(values)
    return sum((count / total) ** 2 for count in counts.values())


def _arm_metrics(
    output_root: Path,
    arm: str,
    *,
    initial_behavior_families: set[str] | None = None,
) -> dict[str, Any]:
    observations: list[dict[str, Any]] = []
    asks: list[dict[str, Any]] = []
    behavior: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    runtime_rows: list[dict[str, Any]] = []
    cache_peaks: list[int] = []
    for checkpoint_index in range(CHECKPOINT_COUNT):
        root = (
            output_root
            / "arms"
            / arm
            / f"checkpoint_{checkpoint_index + 1:03d}"
        )
        manifest = json.loads(
            (root / "batch_manifest.json").read_text(encoding="utf-8-sig")
        )
        _verify_artifacts(root, manifest)
        observations.extend(_read_rows(root / "optimizer_observations.parquet"))
        asks.extend(
            json.loads(
                (root / "asked_population.json").read_text(
                    encoding="utf-8-sig"
                )
            )
        )
        behavior.extend(_read_rows(root / "full_behavior.parquet"))
        summaries.append(
            json.loads(
                (root / "checkpoint_summary.json").read_text(
                    encoding="utf-8-sig"
                )
            )
        )
        gate = json.loads(
            (root / "runtime_utilization_gate.json").read_text(
                encoding="utf-8-sig"
            )
        )
        if "active_bar" in (gate.get("backends") or {}):
            runtime_rows.append(dict(gate["backends"]["active_bar"]))
        for result_path in (root / "phase3cm").glob(
            "*/CN_STREAMING_BACKEND_RESULT.json"
        ):
            result = json.loads(
                result_path.read_text(encoding="utf-8-sig")
            )
            cache_peaks.extend(
                int(row.get("cache_peak_bytes") or 0)
                for row in result.get("expression_audits") or ()
            )
    evaluated = [
        row for row in observations if row["outcome_class"] == EVALUATED
    ]
    increments = [
        float(row["signed_matched_increment"]) for row in evaluated
    ]
    wall_seconds = sum(
        float(row.get("active_wall_seconds") or 0.0) for row in summaries
    )
    category_counts = Counter(str(row["category_id"]) for row in asks)
    behavior_families = {
        str(row.get("portfolio_behavior_family_id") or "")
        for row in behavior
        if str(row.get("portfolio_behavior_family_id") or "")
    } - set(initial_behavior_families or ())
    return {
        "arm": arm,
        "scheduled_pairs": len(asks),
        "tell_observations": len(observations),
        "tell_observations_by_route": dict(
            Counter(str(row["route_id"]) for row in observations)
        ),
        "evaluated_pairs_by_route": dict(
            Counter(str(row["route_id"]) for row in evaluated)
        ),
        "deterministic_invalid_pairs_by_route": dict(
            Counter(
                str(row["route_id"])
                for row in observations
                if row["outcome_class"] == DETERMINISTIC_INVALID
            )
        ),
        "evaluated_pairs": len(evaluated),
        "positive_matched_pairs": sum(value > 0.0 for value in increments),
        "active_wall_seconds": wall_seconds,
        "positive_matched_pairs_per_wall_hour": (
            sum(value > 0.0 for value in increments)
            * 3600
            / max(1.0, wall_seconds)
        ),
        "evaluated_pairs_per_wall_hour": (
            len(evaluated) * 3600 / max(1.0, wall_seconds)
        ),
        "median_signed_matched_increment": (
            statistics.median(increments) if increments else None
        ),
        "behavior_family_count": len(behavior_families),
        "behavior_discovery_per_evaluated_pair": (
            len(behavior_families) / max(1, len(evaluated))
        ),
        "evaluated_route_hhi": _hhi(
            [str(row["route_id"]) for row in evaluated]
        ),
        "asked_skeleton_hhi": _hhi(
            [str(row.get("skeleton_id") or "") for row in asks]
        ),
        "maximum_category_ask_share": (
            max(category_counts.values(), default=0) / max(1, len(asks))
        ),
        "active_effective_cores_median": (
            statistics.median(
                float(row.get("effective_compute_cores") or 0.0)
                for row in runtime_rows
            )
            if runtime_rows
            else None
        ),
        "active_host_logical_occupancy_median": (
            statistics.median(
                float(row.get("host_logical_cpu_occupancy") or 0.0)
                for row in runtime_rows
            )
            if runtime_rows
            else None
        ),
        "active_system_cpu_percent_mean": (
            statistics.mean(
                float(row.get("system_cpu_percent_mean") or 0.0)
                for row in runtime_rows
            )
            if runtime_rows
            else None
        ),
        "peak_rss_bytes": max(
            (int(row.get("peak_rss_bytes") or 0) for row in runtime_rows),
            default=0,
        ),
        "minimum_free_memory_bytes": min(
            (
                int(row["minimum_free_memory_bytes"])
                for row in summaries
                if row.get("minimum_free_memory_bytes") is not None
            ),
            default=0,
        ),
        "maximum_observed_cache_bytes": max(cache_peaks, default=0),
    }


def build_final_verdict(
    baseline: Mapping[str, Any], catcma: Mapping[str, Any]
) -> dict[str, Any]:
    quality_checks = {
        "positive_matched_pairs_per_wall_hour_improved": float(
            catcma["positive_matched_pairs_per_wall_hour"]
        )
        > float(baseline["positive_matched_pairs_per_wall_hour"]),
        "median_signed_matched_increment_not_worse": (
            catcma["median_signed_matched_increment"] is not None
            and baseline["median_signed_matched_increment"] is not None
            and float(catcma["median_signed_matched_increment"])
            >= float(baseline["median_signed_matched_increment"])
        ),
        "behavior_discovery_at_least_90_percent_of_baseline": float(
            catcma["behavior_discovery_per_evaluated_pair"]
        )
        + 1e-12
        >= 0.90 * float(baseline["behavior_discovery_per_evaluated_pair"]),
        "route_concentration_not_materially_worse": float(
            catcma["evaluated_route_hhi"]
        )
        <= float(baseline["evaluated_route_hhi"])
        + MAX_CONCENTRATION_HHI_DETERIORATION,
        "skeleton_concentration_not_materially_worse": float(
            catcma["asked_skeleton_hhi"]
        )
        <= float(baseline["asked_skeleton_hhi"])
        + MAX_CONCENTRATION_HHI_DETERIORATION,
        "no_single_category_collapse": float(
            catcma["maximum_category_ask_share"]
        )
        <= MAX_CATEGORY_ASK_SHARE,
    }
    cpu_a = float(
        catcma.get("active_host_logical_occupancy_median") or 0.0
    ) >= 0.75
    cpu_b = (
        float(catcma.get("active_effective_cores_median") or 0.0)
        >= REFERENCE_EFFECTIVE_CORES
        and float(catcma["evaluated_pairs_per_wall_hour"])
        >= float(baseline["evaluated_pairs_per_wall_hour"])
    )
    scale_checks = {
        "search_policy_quality_pass": all(quality_checks.values()),
        "three_route_groups_each_have_at_least_32_tell_observations": all(
            int((catcma.get("tell_observations_by_route") or {}).get(route_id, 0))
            >= 32
            for route_id in ROUTES
        ),
        "cpu_gate_a_or_b": cpu_a or cpu_b,
        "minimum_free_memory_at_least_24_gib": int(
            catcma.get("minimum_free_memory_bytes") or 0
        )
        >= MINIMUM_FREE_MEMORY_BYTES,
        "cache_cap_respected": (
            0
            < int(catcma.get("maximum_observed_cache_bytes") or 0)
            <= MAXIMUM_CACHE_BYTES
        ),
        "pair_batch_size_at_most_4": max(PAIR_BATCH_SIZES.values()) <= 4,
        "active_and_session_thread_contract": True,
        "bounded_tuning_rounds_at_most_one": True,
        "semantic_parity_required_if_tuned": True,
    }
    return {
        "schema_version": "cn_search_policy_qualification_verdict_v1",
        "SEARCH_POLICY_QUALITY": (
            "PASS" if all(quality_checks.values()) else "FAIL"
        ),
        "LARGE_SEARCH_SCALE_READINESS": (
            "PASS" if all(scale_checks.values()) else "FAIL"
        ),
        "quality_checks": quality_checks,
        "scale_checks": scale_checks,
        "cpu_gate": {
            "path_a_logical_cpu_at_least_75_percent": cpu_a,
            "path_b_smt_ceiling_and_throughput": cpu_b,
            "reference_effective_cores": REFERENCE_EFFECTIVE_CORES,
            "reference_host_logical_occupancy": (
                REFERENCE_HOST_LOGICAL_OCCUPANCY
            ),
            "baseline_pairs_per_hour_source": (
                "THIS_QUALIFICATION_REGISTRY_BASELINE_IMMUTABLE_CHECKPOINTS"
            ),
        },
        "claim_boundary": (
            "Development-only optimizer qualification; no validation, holdout, "
            "2026, Alpha, promotion, or active-authority claim."
        ),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"REAL_QUALIFICATION_AUTHORIZED_ONLY_ON_77O:{platform.node()}"
        )
    if int(args.active_threads) != 30 or int(args.session_threads) != 2:
        raise RuntimeError("FROZEN_THREAD_CONTRACT_MISMATCH")
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    authority = _qualification_authority(
        path=args.qualification_authorization.resolve(),
        source_receipt_path=args.source_receipt.resolve(),
        cmaes_wheel_path=args.cmaes_wheel.resolve(),
        seed_base=args.seed_base,
        active_threads=args.active_threads,
        session_threads=args.session_threads,
    )
    authority_path = _write_json(
        output_root / "qualification_authority_binding.json", authority
    )
    initial_exact, initial_behavior, source_binding = _source_campaign_binding(
        source_root=args.source_campaign_root.resolve(),
        source_receipt_path=args.source_receipt.resolve(),
        historical_candidate_archive=args.historical_candidate_archive.resolve(),
        historical_archive_manifest=args.historical_archive_manifest.resolve(),
    )
    source_binding_path = _write_json(
        output_root / "source_campaign_binding.json", source_binding
    )
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    registry_binding = _registry_binding(args.registry.resolve(), registry)
    registry_binding_path = _write_json(
        output_root / "registry_binding.json", registry_binding
    )
    discovery = load_development_discovery_root_authority(
        args.discovery_contract.resolve(), registry=registry
    )
    discovery_auth = json.loads(
        args.discovery_authorization.resolve().read_text(encoding="utf-8-sig")
    )
    if (
        not bool(discovery_auth.get("execution_authorized"))
        or str(discovery_auth.get("root_contract_hash") or "")
        != str(discovery["contract_hash"])
    ):
        raise RuntimeError("DISCOVERY_AUTHORITY_MISMATCH")
    field_roots = {
        "active_bar": args.active_field_root.resolve(),
        "stock_session": args.session_field_root.resolve(),
    }
    label_roots = {
        "active_bar": args.active_label_root.resolve(),
        "stock_session": args.session_label_root.resolve(),
    }
    compute_threads = {
        "active_bar": int(args.active_threads),
        "stock_session": int(args.session_threads),
    }
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    schema_binding, schema_by_backend = materialized_schema_binding(
        field_roots=field_roots, registry=registry
    )
    schema_path = _write_json(
        output_root / "materialized_schema_binding.json", schema_binding
    )
    purity = audit_split_boundary_label_purity(
        split=split, registry=registry, label_roots=label_roots
    )
    purity_path = _write_json(
        output_root / "split_boundary_purity.json", purity
    )
    if str(purity.get("status") or "") != "PASS":
        raise RuntimeError("SPLIT_BOUNDARY_LABEL_PURITY_FAILED")
    observed_runtime = _runtime_envelope(
        compute_threads["active_bar"], compute_threads["stock_session"]
    )
    if str(observed_runtime.get("status") or "") != "PASS":
        raise RuntimeError(
            str(observed_runtime.get("status") or "RUNTIME_PREFLIGHT_FAILED")
        )
    runtime_path = output_root / "runtime_envelope.json"
    if not runtime_path.is_file():
        _write_json(runtime_path, observed_runtime)
    materialized_root_allowlists = _materialized_route_root_allowlists(
        registry=registry,
        discovery_allowlists=discovery["route_root_allowlists"],
        schema_by_backend=schema_by_backend,
    )

    contract = {
        "schema_version": "cn_search_policy_qualification_contract_v1",
        "status": "FROZEN_EXECUTABLE",
        "routes": list(ROUTES),
        "excluded_route": "FIRSTN_PATH",
        "arms": list(ARMS),
        "checkpoint_count": CHECKPOINT_COUNT,
        "population_size": POPULATION_SIZE,
        "total_scheduled_matched_pair_budget": TOTAL_SCHEDULED_PAIR_BUDGET,
        "arm_order_by_checkpoint": {
            "checkpoint_001": list(ARMS),
            "checkpoint_002": list(reversed(ARMS)),
            "checkpoint_003": list(ARMS),
            "checkpoint_004": list(reversed(ARMS)),
        },
        "top_level_scheduling_key": "UNIFIED_REGISTRY_ROUTE_ID",
        "baseline": (
            "RegistryDrivenGenerator deterministic scheduled attempt stream; "
            "no search-past-invalid or search-past-exact-duplicate"
        ),
        "optimizer": "official_cmaes.CatCMAwM",
        "optimizer_role": (
            "ROUTE_LOCAL_CATEGORICAL_GENE_SELECTION_POLICY_ONLY"
        ),
        "formula_constructor_authority": "CompositionalGrammarV2",
        "materialized_route_root_allowlists": {
            route_id: list(field_ids)
            for route_id, field_ids in materialized_root_allowlists.items()
        },
        "ask_tell": "FULL_POPULATION_ONLY",
        "restore": "GENESIS_PLUS_ASK_TRANSCRIPT_PLUS_TELL_LOSSES",
        "reward_order": (
            "EVALUATED_SIGNED_MATCHED_INCREMENT_DESC_THEN_BLOCKER_THEN_INVALID"
        ),
        "behavior_novelty_in_loss": False,
        "private_arm_post_genesis_exact_and_behavior_memory": True,
        "shared_historical_archive": True,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "active_threads": 30,
        "session_threads": 2,
        "pair_batch_sizes": PAIR_BATCH_SIZES,
        "cache_cap_bytes": MAXIMUM_CACHE_BYTES,
        "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "input_bindings": {
            "authority": _artifact(authority_path, root=output_root),
            "source_campaign": _artifact(
                source_binding_path, root=output_root
            ),
            "registry": _artifact(registry_binding_path, root=output_root),
            "schema": _artifact(schema_path, root=output_root),
            "split_purity": _artifact(purity_path, root=output_root),
            "runtime": _artifact(runtime_path, root=output_root),
        },
    }
    frozen_contract_path = _write_json(
        output_root / "frozen_contract.json", contract
    )
    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist=materialized_root_allowlists,
    )
    gene_input_hashes = {
        "frozen_contract": _sha256(frozen_contract_path),
        "source_campaign_binding": _sha256(source_binding_path),
        "registry_binding": _sha256(registry_binding_path),
        "materialized_schema_binding": _sha256(schema_path),
    }
    semantics, semantics_path, gene_space_manifest_path = _freeze_gene_spaces(
        output_root=output_root,
        generator=generator,
        registry_hash=registry.registry_hash,
        root_contract_hash=str(discovery["contract_hash"]),
        grammar_hash=str(registry_binding["grammar_hash"]),
        input_hashes=gene_input_hashes,
    )
    deadline_epoch = time.time() + int(args.maximum_wall_seconds)

    arm_state: dict[str, dict[str, Any]] = {}
    for arm in ARMS:
        exact, archive, histories, closed_count = _load_closed_arm_state(
            output_root=output_root,
            arm=arm,
            initial_exact=initial_exact,
            initial_behavior=initial_behavior,
        )
        arm_state[arm] = {
            "exact": exact,
            "behavior": archive,
            "histories": histories,
            "closed_count": closed_count,
        }
    adapters = {
        route_id: CatCMASearchAdapter.replay(
            semantics=semantics[route_id],
            seed=args.seed_base + 50_000 + route_index * 1009,
            population_size=POPULATION_SIZE,
            generations=arm_state["official_catcma"]["histories"][route_id],
        )
        for route_index, route_id in enumerate(ROUTES)
    }
    environment_path = _write_json(
        output_root / "optimizer_environment.json",
        {
            route_id: adapter.environment_receipt()
            for route_id, adapter in adapters.items()
        },
    )
    if bool(args.preflight_only):
        route_rows = []
        for route_index, route_id in enumerate(ROUTES):
            baseline = _materialize_population(
                asked=build_baseline_population(
                    route_id=route_id,
                    checkpoint_index=0,
                    seed=args.seed_base + route_index * 1009,
                ),
                generator=generator,
                schema_by_backend=schema_by_backend,
            )
            catcma = _materialize_population(
                asked=adapters[route_id].ask_population(
                    checkpoint_id="preflight_checkpoint_001"
                ),
                generator=generator,
                schema_by_backend=schema_by_backend,
            )
            for arm, rows in (
                ("registry_baseline", baseline),
                ("official_catcma", catcma),
            ):
                legal = [
                    row
                    for row in rows
                    if str(row.get("construction_status") or "") == "LEGAL"
                ]
                novel = [
                    row
                    for row in legal
                    if str(row["exact_identity"]) not in initial_exact
                ]
                route_rows.append(
                    {
                        "arm": arm,
                        "route_id": route_id,
                        "scheduled_pairs": len(rows),
                        "legal_pairs": len(legal),
                        "historically_exact_novel_pairs": len(novel),
                        "deterministic_invalid_pairs": len(rows) - len(legal),
                        "distinct_exact_identities": len(
                            {str(row["exact_identity"]) for row in legal}
                        ),
                    }
                )
        catcma_rows = [
            row
            for row in route_rows
            if row["arm"] == "official_catcma"
        ]
        failed_routes = [
            str(row["route_id"])
            for row in catcma_rows
            if int(row["legal_pairs"]) == 0
        ]
        supply_exhausted_routes = [
            str(row["route_id"])
            for row in catcma_rows
            if int(row["legal_pairs"]) > 0
            and int(row["historically_exact_novel_pairs"]) == 0
        ]
        receipt = {
            "schema_version": "cn_search_policy_preflight_materialization_v1",
            "status": (
                "FAIL"
                if failed_routes
                else (
                    "PASS_WITH_EXACT_SUPPLY_EXHAUSTION"
                    if supply_exhausted_routes
                    else "PASS"
                )
            ),
            "budget_consumed": 0,
            "phase3cm_started": False,
            "failed_routes": failed_routes,
            "exact_supply_exhausted_routes": supply_exhausted_routes,
            "routes": route_rows,
            "gene_space_manifest": _artifact(
                gene_space_manifest_path, root=output_root
            ),
            "optimizer_environment": _artifact(
                environment_path, root=output_root
            ),
        }
        _write_json(
            output_root / "preflight_materialization.json", receipt
        )
        if failed_routes:
            raise RuntimeError(
                "CATCMA_FIRST_POPULATION_HAS_NO_LEGAL_SUPPLY:"
                + ",".join(failed_routes)
            )
        return receipt

    summaries = []
    for checkpoint_index in range(CHECKPOINT_COUNT):
        checkpoint_id = f"checkpoint_{checkpoint_index + 1:03d}"
        order = list(ARMS) if checkpoint_index % 2 == 0 else list(reversed(ARMS))
        for arm in order:
            if checkpoint_index < int(arm_state[arm]["closed_count"]):
                root = output_root / "arms" / arm / checkpoint_id
                summaries.append(
                    json.loads(
                        (root / "checkpoint_summary.json").read_text(
                            encoding="utf-8-sig"
                        )
                    )
                )
                continue
            summaries.append(
                _execute_arm_checkpoint(
                    arm=arm,
                    checkpoint_index=checkpoint_index,
                    output_root=output_root,
                    adapters=adapters,
                    generator=generator,
                    schema_by_backend=schema_by_backend,
                    seed_base=args.seed_base,
                    exact_seen=arm_state[arm]["exact"],
                    behavior_archive=arm_state[arm]["behavior"],
                    registry=registry,
                    split=split,
                    field_roots=field_roots,
                    label_roots=label_roots,
                    purity_path=purity_path,
                    sidecar_closure=args.sidecar_closure.resolve(),
                    compute_threads=compute_threads,
                    deadline_epoch=deadline_epoch,
                    frozen_contract_path=frozen_contract_path,
                    gene_space_manifest_path=gene_space_manifest_path,
                )
            )

    initial_behavior_families = {
        str(row.get("portfolio_behavior_family_id") or "")
        for row in initial_behavior.rows
        if str(row.get("portfolio_behavior_family_id") or "")
    }
    baseline_metrics = _arm_metrics(
        output_root,
        "registry_baseline",
        initial_behavior_families=initial_behavior_families,
    )
    catcma_metrics = _arm_metrics(
        output_root,
        "official_catcma",
        initial_behavior_families=initial_behavior_families,
    )
    metrics_path = _write_json(
        output_root / "qualification_metrics.json",
        {
            "registry_baseline": baseline_metrics,
            "official_catcma": catcma_metrics,
        },
    )
    verdict = build_final_verdict(baseline_metrics, catcma_metrics)
    verdict_path = _write_json(
        output_root / "qualification_verdict.json", verdict
    )
    run_manifest = {
        "schema_version": "cn_search_policy_qualification_run_manifest_v1",
        "status": "QUALIFICATION_COMPLETE",
        "SEARCH_POLICY_QUALITY": verdict["SEARCH_POLICY_QUALITY"],
        "LARGE_SEARCH_SCALE_READINESS": verdict[
            "LARGE_SEARCH_SCALE_READINESS"
        ],
        "artifacts": [
            _artifact(path, root=output_root)
            for path in (
                authority_path,
                source_binding_path,
                registry_binding_path,
                schema_path,
                purity_path,
                runtime_path,
                frozen_contract_path,
                gene_space_manifest_path,
                semantics_path,
                environment_path,
                metrics_path,
                verdict_path,
                *(
                    (output_root / "preflight_materialization.json",)
                    if (
                        output_root / "preflight_materialization.json"
                    ).is_file()
                    else ()
                ),
                *(
                    output_root
                    / "arms"
                    / arm
                    / f"checkpoint_{checkpoint_index + 1:03d}"
                    / "batch_manifest.json"
                    for checkpoint_index in range(CHECKPOINT_COUNT)
                    for arm in ARMS
                ),
            )
        ],
        "checkpoint_summaries": summaries,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    run_manifest["manifest_payload_hash"] = _stable_hash(run_manifest)
    _write_json(output_root / "run_manifest.json", run_manifest)
    return verdict


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qualification-authorization", type=Path, required=True)
    parser.add_argument("--source-campaign-root", type=Path, required=True)
    parser.add_argument("--source-receipt", type=Path, required=True)
    parser.add_argument("--historical-candidate-archive", type=Path, required=True)
    parser.add_argument("--historical-archive-manifest", type=Path, required=True)
    parser.add_argument("--cmaes-wheel", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--discovery-contract", type=Path, required=True)
    parser.add_argument("--discovery-authorization", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--sidecar-closure", type=Path, required=True)
    parser.add_argument("--active-field-root", type=Path, required=True)
    parser.add_argument("--active-label-root", type=Path, required=True)
    parser.add_argument("--session-field-root", type=Path, required=True)
    parser.add_argument("--session-label-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--seed-base", type=int, default=2026072401)
    parser.add_argument("--active-threads", type=int, default=30)
    parser.add_argument("--session-threads", type=int, default=2)
    parser.add_argument("--maximum-wall-seconds", type=int, default=64_800)
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args(argv)
    verdict = run(args)
    print(json.dumps(verdict, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
