"""Six-checkpoint, development-only CN targeted search campaign.

This extends the existing iterative-search capability with a bounded campaign;
it does not introduce another registry, compiler, evaluator, or search platform.
"""

from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    AUTHORIZED_HOST,
    REPO,
    _artifact,
    _batch_manifest,
    _clock_for_route,
    _context_and_binding,
    _join_full_behavior_identities,
    _outcome_rows,
    _probe_pack,
    _route_health,
    _run_automatic_validation_after_train,
    _sha256,
    _stable_hash,
    _train_dates,
    _write_csv,
    _write_json,
    _write_parquet,
)
from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import (
    build_iterative_feedback_views,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.multi_arm_scheduler import (
    build_medium_campaign_schedule,
)
from our_system_phase2.services.portfolio_behavior_archive import (
    PortfolioBehaviorArchive,
)
from our_system_phase2.services.split_boundary_label_purity import (
    audit_split_boundary_label_purity,
)
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    RegistryDrivenGenerator,
    load_development_discovery_root_authority,
)


CAMPAIGN_ID = "CN_TARGETED_FIX_AND_MEDIUM_DEVELOPMENT_CAMPAIGN"
CHECKPOINT_COUNT = 6
CHECKPOINT_SCHEDULED_PAIRS = 256
TOTAL_SCHEDULED_MATCHED_PAIR_BUDGET = 1536
MAX_COMPLETED_DEVELOPMENT_MATCHED_PAIRS = 1536
MAX_RAW_ATTEMPTS = 100_000
MAX_WALL_SECONDS = 12 * 60 * 60
ROUTE_ATTEMPT_CAP = 2_300
GLOBAL_WORKER_LIMIT = 24
MIN_PRIMARY_HOST_LOGICAL_OCCUPANCY = 0.75
PEAK_RSS_LIMIT_BYTES = 48 * 1024**3
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
PARALLELISM_PHASE_MINIMUM_WALL_SECONDS = 1.0
PARALLELISM_PHASE_MINIMUM_WALL_FRACTION = 0.01
PAIR_BATCH_SIZE_BY_BACKEND = {"active_bar": 4, "stock_session": 8}
EXPECTED_REGISTRY_RELATIVE_PATH = Path(
    "runtime/field_registry/cn_unified_capability_registry_v3_20260717/"
    "unified_capability_registry.json"
)
EXPECTED_REGISTRY_FILE_SHA256 = "449fea36daaba8e501bd03d052497b881ac03c601cee701f3ebfe069c7ae61d7"
EXPECTED_REGISTRY_INTERNAL_HASH = "7aecfd9423cd47684460ad6f485a82fb5c29ea02c36a7e488134b33ecb98dae3"
EXPECTED_REGISTRY_FIELD_COUNT = 450
SEARCH_ROUTES = tuple(
    route for route in ROUTE_IDS if route != "BROAD_EVENT_FROZEN_ENTRY"
)
MODIFIED_COMPATIBILITY_ROUTES = (
    "MINUTE_STATIC",
    "SLOW_CROSS_SECTIONAL_LEVEL",
    "SLOW_TEMPORAL_CHANGE",
    "DISCLOSURE_EVENT",
)
LEGACY_CAMPAIGN_PROFILE = "legacy_medium"
SLOW_CROSS_SECTIONAL_384_PROFILE = "slow_cross_sectional_evaluated384"
SLOW_CROSS_SECTIONAL_TARGET_ROUTE = "SLOW_CROSS_SECTIONAL_LEVEL"
SLOW_CROSS_SECTIONAL_EVALUATED_TARGET = 384
SLOW_CROSS_SECTIONAL_MAX_CHECKPOINTS = 12
SLOW_CROSS_SECTIONAL_MAX_ADMITTED_PER_CHECKPOINT = 56
SLOW_CROSS_SECTIONAL_ROUTE_ATTEMPT_CAP = 10_000
SLOW_CROSS_SECTIONAL_MAX_RAW_ATTEMPTS = (
    SLOW_CROSS_SECTIONAL_MAX_CHECKPOINTS
    * SLOW_CROSS_SECTIONAL_ROUTE_ATTEMPT_CAP
)


CHECKPOINT_BASE_TARGETS = (
    {"INTRADAY_STATE_TRANSITION": 48, "SLOW_CROSS_SECTIONAL_LEVEL": 43, "MINUTE_STATIC": 37, "SLOW_TEMPORAL_CHANGE": 37, "FIRSTN_PATH": 32, "MARKET_REGIME_CONDITION": 32, "DISCLOSURE_EVENT": 27},
    {"INTRADAY_STATE_TRANSITION": 48, "SLOW_CROSS_SECTIONAL_LEVEL": 43, "MINUTE_STATIC": 38, "SLOW_TEMPORAL_CHANGE": 37, "FIRSTN_PATH": 32, "MARKET_REGIME_CONDITION": 32, "DISCLOSURE_EVENT": 26},
    {"INTRADAY_STATE_TRANSITION": 48, "SLOW_CROSS_SECTIONAL_LEVEL": 43, "MINUTE_STATIC": 37, "SLOW_TEMPORAL_CHANGE": 38, "FIRSTN_PATH": 32, "MARKET_REGIME_CONDITION": 32, "DISCLOSURE_EVENT": 26},
    {"INTRADAY_STATE_TRANSITION": 48, "SLOW_CROSS_SECTIONAL_LEVEL": 43, "MINUTE_STATIC": 37, "SLOW_TEMPORAL_CHANGE": 37, "FIRSTN_PATH": 32, "MARKET_REGIME_CONDITION": 32, "DISCLOSURE_EVENT": 27},
    {"INTRADAY_STATE_TRANSITION": 48, "SLOW_CROSS_SECTIONAL_LEVEL": 42, "MINUTE_STATIC": 38, "SLOW_TEMPORAL_CHANGE": 37, "FIRSTN_PATH": 32, "MARKET_REGIME_CONDITION": 32, "DISCLOSURE_EVENT": 27},
    {"INTRADAY_STATE_TRANSITION": 48, "SLOW_CROSS_SECTIONAL_LEVEL": 42, "MINUTE_STATIC": 37, "SLOW_TEMPORAL_CHANGE": 38, "FIRSTN_PATH": 32, "MARKET_REGIME_CONDITION": 32, "DISCLOSURE_EVENT": 27},
)


def _campaign_profile(name: str) -> dict[str, Any]:
    if name == LEGACY_CAMPAIGN_PROFILE:
        return {
            "name": name,
            "checkpoint_count": CHECKPOINT_COUNT,
            "checkpoint_scheduled_pairs": CHECKPOINT_SCHEDULED_PAIRS,
            "total_scheduled_matched_pair_budget": TOTAL_SCHEDULED_MATCHED_PAIR_BUDGET,
            "maximum_completed_development_matched_pairs": MAX_COMPLETED_DEVELOPMENT_MATCHED_PAIRS,
            "minimum_actual_evaluated_pairs": 0,
            "maximum_raw_attempts": MAX_RAW_ATTEMPTS,
            "route_attempt_cap": ROUTE_ATTEMPT_CAP,
            "search_routes": SEARCH_ROUTES,
            "base_targets": CHECKPOINT_BASE_TARGETS,
            "evaluated_fill_required": False,
            "target_route": "",
        }
    if name == SLOW_CROSS_SECTIONAL_384_PROFILE:
        base_targets = tuple(
            {
                SLOW_CROSS_SECTIONAL_TARGET_ROUTE:
                    SLOW_CROSS_SECTIONAL_MAX_ADMITTED_PER_CHECKPOINT
            }
            for _ in range(SLOW_CROSS_SECTIONAL_MAX_CHECKPOINTS)
        )
        return {
            "name": name,
            "checkpoint_count": SLOW_CROSS_SECTIONAL_MAX_CHECKPOINTS,
            "checkpoint_scheduled_pairs": SLOW_CROSS_SECTIONAL_MAX_ADMITTED_PER_CHECKPOINT,
            "total_scheduled_matched_pair_budget": (
                SLOW_CROSS_SECTIONAL_MAX_CHECKPOINTS
                * SLOW_CROSS_SECTIONAL_MAX_ADMITTED_PER_CHECKPOINT
            ),
            "maximum_completed_development_matched_pairs": (
                SLOW_CROSS_SECTIONAL_MAX_CHECKPOINTS
                * SLOW_CROSS_SECTIONAL_MAX_ADMITTED_PER_CHECKPOINT
            ),
            "minimum_actual_evaluated_pairs": SLOW_CROSS_SECTIONAL_EVALUATED_TARGET,
            "maximum_raw_attempts": SLOW_CROSS_SECTIONAL_MAX_RAW_ATTEMPTS,
            "route_attempt_cap": SLOW_CROSS_SECTIONAL_ROUTE_ATTEMPT_CAP,
            "search_routes": (SLOW_CROSS_SECTIONAL_TARGET_ROUTE,),
            "base_targets": base_targets,
            "evaluated_fill_required": True,
            "target_route": SLOW_CROSS_SECTIONAL_TARGET_ROUTE,
        }
    raise ValueError(f"unknown campaign profile: {name}")


def _source_hash(path: Path) -> str:
    if not path.is_file():
        raise FileNotFoundError(path)
    return _sha256(path)


def _git_sha() -> str:
    deployment_sha = str(os.environ.get("CN_CAMPAIGN_REPO_SHA") or "").lower()
    if len(deployment_sha) == 40 and all(
        character in "0123456789abcdef" for character in deployment_sha
    ):
        return deployment_sha
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO,
        text=True,
        capture_output=True,
        check=True,
    )
    return completed.stdout.strip()


def _campaign_authorization_binding(
    *,
    authorization_path: Path,
    history_manifest_path: Path,
    candidate_archive_path: Path,
    behavior_archive_path: Path,
    seed_base: int,
    active_threads: int,
    session_threads: int,
    profile: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    authorization_path = Path(authorization_path).resolve()
    history_manifest_path = Path(history_manifest_path).resolve()
    candidate_archive_path = Path(candidate_archive_path).resolve()
    behavior_archive_path = Path(behavior_archive_path).resolve()
    authorization = json.loads(authorization_path.read_text(encoding="utf-8"))
    history = json.loads(history_manifest_path.read_text(encoding="utf-8"))
    selected_profile = dict(profile or _campaign_profile(LEGACY_CAMPAIGN_PROFILE))
    expected = {
        "execution_authorized": True,
        "checkpoint_count": int(selected_profile["checkpoint_count"]),
        "checkpoint_scheduled_pairs": int(
            selected_profile["checkpoint_scheduled_pairs"]
        ),
        "total_scheduled_matched_pair_budget": int(
            selected_profile["total_scheduled_matched_pair_budget"]
        ),
        "maximum_completed_development_matched_pairs": int(
            selected_profile["maximum_completed_development_matched_pairs"]
        ),
        "maximum_raw_attempts": int(selected_profile["maximum_raw_attempts"]),
        "maximum_wall_seconds": MAX_WALL_SECONDS,
        "seed_base": int(seed_base),
        "active_threads": int(active_threads),
        "session_threads": int(session_threads),
        "active_pair_batch_size": PAIR_BATCH_SIZE_BY_BACKEND["active_bar"],
        "session_pair_batch_size": PAIR_BATCH_SIZE_BY_BACKEND["stock_session"],
        "global_worker_limit": GLOBAL_WORKER_LIMIT,
        "constructor_profile": COMPOSITIONAL_V2_PROFILE,
        "scheduler_authority": "UNIFIED_REGISTRY_ROUTE_ID",
        "required_parallelism_status": "PARALLELISM_ENGAGED",
        "peak_rss_limit_bytes": PEAK_RSS_LIMIT_BYTES,
        "checkpoint_recovery": "EXISTING_PHASE3CM_ONLY",
        "validation_mode": "AUTOMATIC_POST_TRAIN_REPORT_ONLY",
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "holdout": "SEALED",
        "forward_2026": "SEALED",
    }
    if str(selected_profile["name"]) != LEGACY_CAMPAIGN_PROFILE:
        expected.update(
            {
                "campaign_profile": str(selected_profile["name"]),
                "minimum_actual_evaluated_pairs": int(
                    selected_profile["minimum_actual_evaluated_pairs"]
                ),
            }
        )
    drift = [
        key
        for key, value in expected.items()
        if authorization.get(key) != value
    ]
    campaign_id = str(authorization.get("campaign_id") or "")
    if not campaign_id:
        drift.append("campaign_id")
    if str(history.get("status") or "") != "PASS":
        drift.append("history_manifest_status")
    candidate_artifact = dict(history.get("candidate_exact_archive") or {})
    behavior_artifact = dict(history.get("behavior_archive") or {})
    if str(candidate_artifact.get("sha256") or "").lower() != _sha256(
        candidate_archive_path
    ).lower():
        drift.append("candidate_exact_archive_sha256")
    if str(behavior_artifact.get("sha256") or "").lower() != _sha256(
        behavior_archive_path
    ).lower():
        drift.append("behavior_archive_sha256")
    actual_sources = sorted(
        str(row.get("sha256") or "").lower()
        for row in list(history.get("sources") or [])
    )
    expected_sources = sorted(
        str(value).lower()
        for value in list(authorization.get("historical_source_sha256") or [])
    )
    if actual_sources != expected_sources:
        drift.append("historical_source_sha256")
    if drift:
        raise RuntimeError(
            "CAMPAIGN_AUTHORIZATION_MISMATCH:" + ",".join(sorted(set(drift)))
        )
    return {
        "schema_version": "cn_campaign_authorization_binding_v1",
        "status": "CAMPAIGN_EXECUTION_AUTHORIZED",
        "campaign_id": campaign_id,
        "authorization": _artifact(authorization_path),
        "historical_archive_manifest": _artifact(history_manifest_path),
        "historical_candidate_exact_archive": _artifact(candidate_archive_path),
        "historical_behavior_archive": _artifact(behavior_archive_path),
        "frozen_parameters": expected,
        "historical_source_sha256": expected_sources,
    }


def build_seed_attempt_manifest(
    *,
    registry_hash: str,
    schema_hash_by_backend: Mapping[str, str],
    grammar_hash: str,
    seed_base: int,
    checkpoint_count: int = CHECKPOINT_COUNT,
    search_routes: Sequence[str] = SEARCH_ROUTES,
    route_attempt_cap: int = ROUTE_ATTEMPT_CAP,
    maximum_raw_attempts: int = MAX_RAW_ATTEMPTS,
    base_targets: Sequence[Mapping[str, int]] = CHECKPOINT_BASE_TARGETS,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    if len(base_targets) != checkpoint_count:
        raise ValueError("base target count must match checkpoint count")
    for checkpoint_index in range(checkpoint_count):
        for route_index, route_id in enumerate(search_routes):
            attempt_start = checkpoint_index * route_attempt_cap
            rows.append(
                {
                    "checkpoint": f"checkpoint_{checkpoint_index + 1:03d}",
                    "route_id": route_id,
                    "seed": int(seed_base + checkpoint_index * 100_003 + route_index * 1_009),
                    "attempt_start": attempt_start,
                    "attempt_stop": attempt_start + route_attempt_cap,
                    "raw_attempt_cap": route_attempt_cap,
                    "base_scheduled_matched_pair_target": int(
                        base_targets[checkpoint_index][route_id]
                    ),
                    "constructor_profile": COMPOSITIONAL_V2_PROFILE,
                    "registry_hash": registry_hash,
                    "materialized_schema_hash": schema_hash_by_backend[
                        _clock_for_route(route_id)
                    ],
                    "grammar_hash": grammar_hash,
                }
            )
    total_cap = sum(int(row["raw_attempt_cap"]) for row in rows)
    if total_cap > maximum_raw_attempts:
        raise RuntimeError("frozen attempt ranges exceed the campaign raw-attempt cap")
    return {
        "schema_version": "cn_medium_campaign_seed_attempt_manifest_v1",
        "attempt_stream_policy": "DISJOINT_ROUTE_LOCAL_RANGES_NO_EXTENSION",
        "maximum_raw_attempts": maximum_raw_attempts,
        "frozen_route_attempt_capacity": total_cap,
        "rows": rows,
    }


def _schema_names(root: Path) -> tuple[str, ...]:
    paths = tuple(sorted(Path(root).glob("shard_*.parquet")))
    if not paths:
        raise FileNotFoundError(f"no materialized field shards: {root}")
    intersection: set[str] | None = None
    for path in paths:
        names = set(pq.ParquetFile(path).schema_arrow.names)
        intersection = names if intersection is None else intersection & names
    return tuple(sorted(intersection or ()))


def _column_has_positive_activation(paths: Sequence[Path], field_id: str) -> bool:
    for path in paths:
        parquet = pq.ParquetFile(path)
        names = parquet.schema_arrow.names
        if field_id not in names:
            continue
        column_index = names.index(field_id)
        metadata_proved = False
        for group_index in range(parquet.metadata.num_row_groups):
            statistics = parquet.metadata.row_group(group_index).column(column_index).statistics
            if statistics is not None and statistics.has_min_max:
                metadata_proved = True
                maximum = statistics.max
                if maximum is not None and float(maximum) > 0.0:
                    return True
        if not metadata_proved:
            series = pq.read_table(path, columns=[field_id]).column(0).to_pandas()
            if bool((pd.to_numeric(series, errors="coerce") > 0).any()):
                return True
    return False


def materialized_schema_binding(
    *,
    field_roots: Mapping[str, Path],
    registry: UnifiedCapabilityRegistry,
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    schemas: dict[str, set[str]] = {}
    records: dict[str, Any] = {}
    disclosure_conditions = {
        field.field_id
        for field in registry.fields_for_route(
            "DISCLOSURE_EVENT", field_roles=("condition-only",)
        )
    }
    for backend in ("active_bar", "stock_session"):
        root = Path(field_roots[backend]).resolve()
        names = _schema_names(root)
        schemas[backend] = set(names)
        paths = tuple(sorted(root.glob("shard_*.parquet")))
        active_conditions = sorted(
            field_id
            for field_id in disclosure_conditions & set(names)
            if _column_has_positive_activation(paths, field_id)
        ) if backend == "stock_session" else []
        if backend == "stock_session":
            schemas[backend] -= disclosure_conditions - set(active_conditions)
        records[backend] = {
            "root": str(root),
            "materialized_field_ids": sorted(schemas[backend]),
            "materialized_field_count": len(schemas[backend]),
            "materialized_schema_hash": _stable_hash(sorted(schemas[backend])),
            "train_condition_activation_field_ids": active_conditions,
            "shard_count": len(paths),
        }
    return {
        "schema_version": "cn_medium_campaign_materialized_schema_binding_v1",
        "status": "SCHEMA_FIRST_COMPATIBLE_POOLS_BOUND",
        "backends": records,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }, schemas


def _registry_binding(registry_path: Path, registry: UnifiedCapabilityRegistry) -> dict[str, Any]:
    expected_path = (REPO / EXPECTED_REGISTRY_RELATIVE_PATH).resolve()
    if (
        registry_path.resolve() != expected_path
        or _sha256(registry_path) != EXPECTED_REGISTRY_FILE_SHA256
        or registry.registry_hash != EXPECTED_REGISTRY_INTERNAL_HASH
        or len(registry.fields) != EXPECTED_REGISTRY_FIELD_COUNT
    ):
        raise RuntimeError("CURRENT_REGISTRY_AUTHORITY_MISMATCH")
    sources = {
        "generator": REPO / "src/our_system_phase2/services/unified_discovery_generators.py",
        "grammar": REPO / "src/our_system_phase2/services/compositional_grammar.py",
        "compiler": REPO / "src/our_system_phase2/services/typed_route_compiler.py",
    }
    canonical_fundamental_roots = {
        field.field_id
        for field in registry.fields
        if field.source_family.startswith("canonical_fundamental_")
    }
    return {
        "schema_version": "cn_medium_campaign_registry_binding_v1",
        "status": "CURRENT_V3_REGISTRY_AUTHORITY_BOUND",
        "registry_path": str(registry_path),
        "registry_file_sha256": _sha256(registry_path),
        "internal_registry_hash": registry.registry_hash,
        "field_count": len(registry.fields),
        "canonical_fundamental_root_count": len(canonical_fundamental_roots),
        "generator_hash": _source_hash(sources["generator"]),
        "grammar_hash": _source_hash(sources["grammar"]),
        "compiler_hash": _source_hash(sources["compiler"]),
        "repo_sha": _git_sha(),
    }


def _read_table_rows(path: Path) -> list[dict[str, Any]]:
    source = Path(path).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    if source.suffix.lower() == ".parquet":
        return pd.read_parquet(source).fillna("").to_dict(orient="records")
    if source.suffix.lower() == ".csv":
        return pd.read_csv(source).fillna("").to_dict(orient="records")
    if source.suffix.lower() == ".jsonl":
        return [
            json.loads(line)
            for line in source.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    raise ValueError(f"historical archive must be parquet, csv, or jsonl: {source}")


def _load_historical_dedupe(
    *,
    candidate_archive_path: Path,
    behavior_archive_path: Path,
) -> tuple[set[str], PortfolioBehaviorArchive, dict[str, Any]]:
    candidate_rows = _read_table_rows(candidate_archive_path)
    exact_identities = {
        str(row.get("exact_identity") or "")
        for row in candidate_rows
        if str(row.get("exact_identity") or "")
    }
    if not exact_identities:
        raise RuntimeError("historical candidate archive contains no exact identities")
    source_behavior = PortfolioBehaviorArchive.read_parquet(behavior_archive_path)
    allowed_fields = {
        "pair_id", "route_id", "structural_family_id", "signal_cluster_id",
        "behavior_probe_id", "primary_behavior_probe_id", "control_behavior_probe_id",
        "portfolio_behavior_signature_id", "portfolio_behavior_family_id",
        "behavior_status", "primary_support_rate", "control_support_rate",
        "primary_mean_turnover", "control_mean_turnover", "coordinate_binding",
    }
    behavior_rows = [
        {key: value for key, value in row.items() if key in allowed_fields}
        for row in source_behavior.rows
        if str(row.get("behavior_status") or "RESOLVED") == "RESOLVED"
    ]
    behavior_archive = PortfolioBehaviorArchive(behavior_rows)
    if not any(
        str(row.get("behavior_probe_id") or row.get("portfolio_behavior_signature_id") or "")
        for row in behavior_rows
    ):
        raise RuntimeError("historical behavior archive contains no dedupe identities")
    snapshot = {
        "schema_version": "cn_medium_campaign_historical_dedupe_snapshot_v1",
        "candidate_archive": _artifact(candidate_archive_path),
        "behavior_archive": _artifact(behavior_archive_path),
        "exact_identity_count": len(exact_identities),
        "exact_identity_digest": _stable_hash(sorted(exact_identities)),
        "behavior_identity_row_count": len(behavior_rows),
        "behavior_identity_digest": _stable_hash(behavior_rows),
        "reward_columns_imported": [],
        "scheduler_state_imported": False,
        "status": "FROZEN_HISTORICAL_EXACT_AND_BEHAVIOR_DEDUPE_BOUND",
    }
    return exact_identities, behavior_archive, snapshot


def _add_resolved_behavior_rows(
    archive: PortfolioBehaviorArchive,
    rows: Sequence[Mapping[str, Any]],
) -> int:
    """Persist only resolved identities; unresolved probes remain run-local evidence."""

    added = 0
    for raw in rows:
        row = dict(raw)
        if str(row.get("behavior_status") or "") != "RESOLVED":
            continue
        archive.add(row)
        added += 1
    return added


def _structural_comparison(
    *,
    registry: UnifiedCapabilityRegistry,
    schema_by_backend: Mapping[str, set[str]],
    route_root_allowlists: Mapping[str, Sequence[str]],
    seed: int,
) -> dict[str, Any]:
    before = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=False,
        route_root_allowlist=route_root_allowlists,
    )
    after = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist=route_root_allowlists,
    )
    rows = []
    for ordinal, route_id in enumerate(MODIFIED_COMPATIBILITY_ROUTES):
        route_seed = seed + ordinal * 1_009
        _, before_funnel = before.generate_route_attempts(
            route_id,
            scheduled_pairs=32,
            seed=route_seed,
            attempt_limit=256,
        )
        _, after_funnel = after.generate_route_attempts(
            route_id,
            scheduled_pairs=32,
            seed=route_seed,
            attempt_limit=256,
            available_field_ids=schema_by_backend[_clock_for_route(route_id)],
        )
        rows.append(
            {
                "route_id": route_id,
                "seed": route_seed,
                "attempt_cap": 256,
                "before": before_funnel,
                "after": after_funnel,
                "phase3cm_evaluation": "NOT_RUN",
            }
        )
    return {
        "schema_version": "cn_medium_campaign_structural_generation_comparison_v1",
        "routes": rows,
        "comparison_scope": "MODIFIED_ROUTES_ONE_SEED_GENERATION_ONLY",
    }


def _package_matrix() -> dict[str, str]:
    packages = (
        "numpy", "pandas", "pyarrow", "numba", "bottleneck",
        "numexpr", "polars", "joblib", "sklearn", "psutil",
    )
    versions = {}
    for name in packages:
        try:
            module = importlib.import_module(name)
            versions[name] = str(getattr(module, "__version__", "UNKNOWN"))
        except Exception as exc:
            versions[name] = f"UNAVAILABLE:{type(exc).__name__}"
    return versions


def _runtime_envelope(active_threads: int, session_threads: int) -> dict[str, Any]:
    try:
        import psutil

        memory = psutil.virtual_memory()
        heavy = []
        for process in psutil.process_iter(["pid", "name", "cmdline"]):
            command = " ".join(process.info.get("cmdline") or ())
            if "run_cn_phase3cm_streaming_qualification.py" in command:
                heavy.append({"pid": process.info["pid"], "command": command})
        host = {
            "logical_cpu_count": psutil.cpu_count(logical=True),
            "physical_cpu_count": psutil.cpu_count(logical=False),
            "total_memory_bytes": int(memory.total),
            "available_memory_bytes": int(memory.available),
            "system_cpu_percent": float(psutil.cpu_percent(interval=1.0)),
        }
    except Exception as exc:
        raise RuntimeError("psutil is required for the real runtime utilization gate") from exc
    return {
        "schema_version": "cn_medium_campaign_runtime_envelope_v1",
        "host": platform.node(),
        "python_executable": sys.executable,
        "python_version": platform.python_version(),
        "package_matrix": _package_matrix(),
        "actual_hot_path": "Phase3CM TimeMajorBlockReader -> SharedMultiCandidateDAG -> Numba portfolio kernel -> streaming reducer",
        "acceleration_status": {
            "numba": "ENABLED_AND_REQUIRED_BY_PORTFOLIO_HOT_PATH",
            "vectorized_numpy": "ENABLED_IN_EXPRESSION_AND_PORTFOLIO_HOT_PATH",
            "pyarrow": "ENABLED_FOR_PARQUET_SIDECAR_IO",
            "polars": "ENABLED_FOR_SIDECAR_MATERIALIZATION_NOT_EVALUATOR_INNER_LOOP",
            "evaluation_cache": "ENABLED_SHARED_DAG_BLOCK_CACHE",
            "evaluation_cache_key": "execution_plan_hash+dag_plan_hash+block_boundary+candidate_value_cohort",
            "use_fast_context": "NOT_APPLICABLE_NO_SEPARATE_FAST_CONTEXT_SWITCH",
            "successive_halving": "DISABLED_BY_CAMPAIGN_CONTRACT",
        },
        "thread_contract": {
            "active_bar": active_threads,
            "stock_session": session_threads,
            "global_worker_limit": GLOBAL_WORKER_LIMIT,
            "heavy_processes": 1,
        },
        "host_state": host,
        "existing_heavy_workers": heavy,
        "checkpoint_recovery": "EXISTING_PHASE3CM_CHECKPOINT_ONLY",
        "status": "PASS" if not heavy else "FAIL_ORPHAN_HEAVY_WORKER",
    }


def _physical_cpu_count() -> int:
    try:
        import psutil

        return int(psutil.cpu_count(logical=False) or 1)
    except Exception as exc:
        raise RuntimeError("psutil is required for host-level compute allocation") from exc


def _logical_cpu_count() -> int:
    try:
        import psutil

        return int(psutil.cpu_count(logical=True) or 1)
    except Exception as exc:
        raise RuntimeError("psutil is required for host-level compute allocation") from exc


def _admit_behavior_unique(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    probe_rows: Sequence[Mapping[str, Any]],
    historical_archive: PortfolioBehaviorArchive,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    seen_probe_ids: set[str] = set()
    admitted_ids: set[str] = set()
    decisions = []
    for source in probe_rows:
        row = dict(source)
        probe_id = str(row.get("behavior_probe_id") or "")
        member_duplicate = bool(row.get("primary_behavior_probe_id")) and (
            row.get("primary_behavior_probe_id") == row.get("control_behavior_probe_id")
        )
        if str(row.get("behavior_status") or "") != "RESOLVED":
            decision, reason = "REJECT", "BEHAVIOR_UNRESOLVED"
        elif member_duplicate or historical_archive.contains_probe(probe_id) or probe_id in seen_probe_ids:
            decision, reason = "REJECT", "EXACT_BEHAVIOR_DUPLICATE"
        else:
            decision, reason = "ADMIT", "LABEL_FREE_BEHAVIOR_UNIQUE"
            seen_probe_ids.add(probe_id)
            admitted_ids.add(str(row["pair_id"]))
        decisions.append({**row, "admission_decision": decision, "admission_reason": reason})
    admitted = [
        dict(row)
        for row in candidate_rows
        if str(row.get("pair_id") or "") in admitted_ids
    ]
    return admitted, decisions


def _next_target_admission_count(
    *,
    evaluated_target: int,
    completed_evaluated: int,
    completed_admitted: int,
    maximum_per_checkpoint: int,
) -> int:
    remaining = max(0, int(evaluated_target) - int(completed_evaluated))
    if remaining == 0:
        return 0
    observed_rate = (
        float(completed_evaluated) / float(completed_admitted)
        if completed_admitted > 0
        else 0.75
    )
    bounded_rate = min(0.95, max(0.50, observed_rate))
    predicted = int(math.ceil(remaining / bounded_rate))
    return min(int(maximum_per_checkpoint), max(16, predicted))


def _generate_behavior_unique_target_pack(
    *,
    generator: RegistryDrivenGenerator,
    registry: UnifiedCapabilityRegistry,
    route_id: str,
    admitted_pair_target: int,
    seed: int,
    attempt_start: int,
    attempt_limit: int,
    historical_exact: set[str],
    historical_behavior_archive: PortfolioBehaviorArchive,
    available_field_ids: set[str],
    field_roots: Mapping[str, Path],
    train_dates: Sequence[str],
    coordinate_binding: str,
    checkpoint_id: str,
    compute_threads: Mapping[str, int],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
]:
    """Fill one immutable checkpoint to a behavior-unique admission target.

    Generation and label-free probing may use multiple bounded rounds.  Full
    Phase3CM materialization still runs once, after the candidate pack closes.
    """

    if admitted_pair_target <= 0:
        raise ValueError("admitted_pair_target must be positive")
    stop = int(attempt_start) + int(attempt_limit)
    cursor = int(attempt_start)
    generated: list[dict[str, Any]] = []
    probe_rows: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    admitted: list[dict[str, Any]] = []
    funnels: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    probe_archive = PortfolioBehaviorArchive(historical_behavior_archive.rows)
    round_index = 0

    while len(admitted) // 2 < admitted_pair_target and cursor < stop:
        round_index += 1
        remaining = admitted_pair_target - len(admitted) // 2
        requested = min(128, max(64, remaining * 2))
        round_rows, funnel = generator.generate_route_attempts(
            route_id,
            scheduled_pairs=requested,
            seed=seed,
            attempt_start=cursor,
            attempt_limit=stop - cursor,
            existing_exact_identities=historical_exact,
            available_field_ids=available_field_ids,
        )
        next_cursor = int(funnel["attempt_stop"])
        funnel = {
            **funnel,
            "generation_round": round_index,
            "generation_requested_pairs": requested,
        }
        funnels.append(funnel)
        if round_rows:
            round_rows = _annotate_generation_metadata(round_rows, registry)
            generated.extend(round_rows)
            historical_exact.update(
                str(row["exact_identity"])
                for row in round_rows
                if str(row.get("exact_identity") or "")
            )
            round_probe, round_audit = _probe_pack(
                candidate_rows=round_rows,
                field_roots=field_roots,
                train_dates=train_dates,
                coordinate_binding=coordinate_binding,
                batch_id=checkpoint_id,
                compute_threads=compute_threads,
            )
            round_admitted, round_decisions = _admit_behavior_unique(
                candidate_rows=round_rows,
                probe_rows=round_probe,
                historical_archive=probe_archive,
            )
            selected_pair_ids: list[str] = []
            for index in range(0, len(round_admitted), 2):
                pair_id = str(round_admitted[index].get("pair_id") or "")
                if pair_id and pair_id not in selected_pair_ids:
                    selected_pair_ids.append(pair_id)
                if len(selected_pair_ids) >= remaining:
                    break
            selected = set(selected_pair_ids)
            for row in round_decisions:
                if (
                    str(row.get("admission_decision") or "") == "ADMIT"
                    and str(row.get("pair_id") or "") not in selected
                ):
                    row["admission_decision"] = "DEFER"
                    row["admission_reason"] = "EVALUATED_TARGET_FILLED"
            admitted.extend(
                row
                for row in round_admitted
                if str(row.get("pair_id") or "") in selected
            )
            probe_rows.extend(round_probe)
            decisions.extend(round_decisions)
            audits.extend(
                {"generation_round": round_index, **row}
                for row in round_audit
            )
            _add_resolved_behavior_rows(probe_archive, round_probe)
        if next_cursor <= cursor:
            break
        cursor = next_cursor

    numeric_sums = (
        "generation_attempts",
        "legal_pairs",
        "exact_unique_pairs",
        "illegal_pairs",
        "exact_duplicate_pairs",
        "materialization_unsupported_pairs",
        "materialization_missing_field_pairs",
        "skeleton_compatibility_rejects",
        "schema_first_rejected_field_count",
        "generation_requested_pairs",
    )
    aggregate = dict(funnels[0]) if funnels else {
        "route_id": route_id,
        "constructor_profile": COMPOSITIONAL_V2_PROFILE,
        "attempt_start": attempt_start,
        "attempt_stop": attempt_start,
    }
    for key in numeric_sums:
        aggregate[key] = sum(int(row.get(key) or 0) for row in funnels)
    aggregate.update(
        {
            "scheduled_pairs": int(admitted_pair_target),
            "attempt_start": int(attempt_start),
            "attempt_stop": int(cursor),
            "generation_round_count": int(round_index),
            "behavior_unique_pairs": sum(
                str(row.get("admission_reason") or "")
                in {"LABEL_FREE_BEHAVIOR_UNIQUE", "EVALUATED_TARGET_FILLED"}
                for row in decisions
            ),
            "admitted_pairs": len(admitted) // 2,
            "evaluated_pair_target": int(admitted_pair_target),
            "underfill_reason": (
                ""
                if len(admitted) // 2 >= admitted_pair_target
                else "BEHAVIOR_UNIQUE_SUPPLY_EXHAUSTED"
            ),
        }
    )
    return generated, probe_rows, decisions, admitted, {
        "funnel": aggregate,
        "probe_audits": audits,
    }


def _bind_purity(binding_path: Path, purity_path: Path) -> None:
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    binding.pop("binding_hash", None)
    binding["split_boundary_purity"] = _artifact(purity_path)
    binding["retained_label_crossing_count"] = 0
    binding["label_purge_enforcement"] = (
        "FINITE_SIGNAL_AND_LABEL_INTERSECTION_IN_BATCHED_PORTFOLIO_KERNEL"
    )
    binding["binding_hash"] = _stable_hash(binding)
    _write_json(binding_path, binding)


def _monitor_process(process: subprocess.Popen[str], deadline_epoch: float) -> list[dict[str, Any]]:
    import psutil

    samples: list[dict[str, Any]] = []
    psutil.cpu_percent(interval=None)
    disk_before = psutil.disk_io_counters()
    started = time.time()
    while process.poll() is None:
        if time.time() >= deadline_epoch:
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
            raise RuntimeError("CAMPAIGN_WALL_TIME_CAP_REACHED")
        root = psutil.Process(process.pid)
        descendants = [root, *root.children(recursive=True)]
        rss = 0
        threads = 0
        alive = 0
        for child in descendants:
            try:
                rss += int(child.memory_info().rss)
                threads += int(child.num_threads())
                alive += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        memory = psutil.virtual_memory()
        disk = psutil.disk_io_counters()
        samples.append(
            {
                "elapsed_seconds": time.time() - started,
                "system_cpu_percent": float(psutil.cpu_percent(interval=2.0)),
                "process_tree_count": alive,
                "process_tree_threads": threads,
                "process_tree_rss_bytes": rss,
                "available_memory_bytes": int(memory.available),
                "system_read_bytes": int(disk.read_bytes - disk_before.read_bytes),
                "system_write_bytes": int(disk.write_bytes - disk_before.write_bytes),
            }
        )
    return samples


def _run_phase3cm_monitored(
    *,
    checkpoint_id: str,
    checkpoint_root: Path,
    binding_path: Path,
    table_paths: Mapping[str, Path],
    split_manifest: Path,
    field_roots: Mapping[str, Path],
    label_roots: Mapping[str, Path],
    purity_path: Path,
    compute_threads: Mapping[str, int],
    deadline_epoch: float,
    output_namespace: str = "phase3cm",
    selected_backends: Sequence[str] = ("active_bar", "stock_session"),
    pair_batch_sizes: Mapping[str, int] | None = None,
    session_sample_manifest: Path | None = None,
) -> list[dict[str, Any]]:
    receipts = []
    effective_pair_batch_sizes = dict(PAIR_BATCH_SIZE_BY_BACKEND)
    if pair_batch_sizes is not None:
        effective_pair_batch_sizes.update(
            {str(key): int(value) for key, value in pair_batch_sizes.items()}
        )
    for backend in selected_backends:
        candidate_table = table_paths.get(backend)
        if candidate_table is None:
            continue
        pair_count = pd.read_csv(candidate_table)["pair_id"].nunique()
        output_root = checkpoint_root / output_namespace / backend
        output_root.mkdir(parents=True, exist_ok=True)
        result_path = output_root / "CN_STREAMING_BACKEND_RESULT.json"
        if result_path.is_file():
            result = json.loads(result_path.read_text(encoding="utf-8"))
            binding = json.loads(binding_path.read_text(encoding="utf-8"))
            expected_sample_hash = (
                _sha256(session_sample_manifest)
                if session_sample_manifest is not None
                else ""
            )
            observed_sample_hash = str(
                ((result.get("session_sample") or {}).get("sha256"))
                or ""
            )
            if (
                result.get("status") != "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED"
                or result.get("input_binding_hash") != binding.get("binding_hash")
                or int(result.get("pair_count") or 0) != int(pair_count)
                or observed_sample_hash != expected_sample_hash
            ):
                raise RuntimeError(f"completed Phase3CM result drift on {backend}")
            receipts.append({"backend": backend, "status": "COMPLETED_REUSED", "result_path": str(result_path), "result_sha256": _sha256(result_path)})
            continue
        command = [
            sys.executable,
            str(REPO / "scripts/run_cn_phase3cm_streaming_qualification.py"),
            "--backend", backend,
            "--phase", "D",
            "--pair-count", str(pair_count),
            "--candidate-table", str(candidate_table),
            "--binding", str(binding_path),
            "--split-boundary-purity", str(purity_path),
            "--split-manifest", str(split_manifest),
            "--artifact-root", str(checkpoint_root),
            "--field-sidecar-root", str(field_roots[backend]),
            "--label-sidecar-root", str(label_roots[backend]),
            "--output-root", str(output_root),
            "--block-sessions", "10",
            "--pair-batch-size", str(effective_pair_batch_sizes[backend]),
            "--compute-threads", str(compute_threads[backend]),
            "--iterative-batch-id", checkpoint_id,
        ]
        if session_sample_manifest is not None:
            command.extend(
                [
                    "--session-sample-manifest",
                    str(session_sample_manifest),
                ]
            )
        checkpoint_path = output_root / "CN_STREAMING_CHECKPOINT.json"
        if checkpoint_path.is_file():
            command.append("--resume")
        environment = dict(os.environ)
        environment.update(
            {
                "PYTHONPATH": str(REPO / "src"),
                "NUMBA_NUM_THREADS": str(compute_threads[backend]),
                "ARROW_NUM_THREADS": "1",
                "OMP_NUM_THREADS": "1",
                "MKL_NUM_THREADS": "1",
                "OPENBLAS_NUM_THREADS": "1",
                "NUMEXPR_MAX_THREADS": "1",
                "POLARS_MAX_THREADS": "1",
            }
        )
        started = pd.Timestamp.now("UTC")
        process = subprocess.Popen(
            command,
            cwd=REPO,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        samples = _monitor_process(process, deadline_epoch)
        stdout, stderr = process.communicate()
        (output_root / "stdout.log").write_text(stdout or "", encoding="utf-8")
        (output_root / "stderr.log").write_text(stderr or "", encoding="utf-8")
        _write_json(output_root / "runtime_samples.json", samples)
        receipt = {
            "backend": backend,
            "command": command,
            "returncode": process.returncode,
            "started_at": started.isoformat(),
            "completed_at": pd.Timestamp.now("UTC").isoformat(),
            "result_path": str(result_path),
            "result_sha256": _sha256(result_path) if result_path.is_file() else "",
            "runtime_samples_path": str(output_root / "runtime_samples.json"),
            "status": "COMPLETED" if process.returncode == 0 and result_path.is_file() else "INFRASTRUCTURE_FAILURE",
        }
        receipts.append(receipt)
        if receipt["status"] != "COMPLETED":
            raise RuntimeError(f"Phase3CM infrastructure failure on {backend}: {output_root}")
    return receipts


def _block_compute_rows(
    events: Sequence[Mapping[str, Any]], allocated_threads: int
) -> list[dict[str, Any]]:
    compute_phases = {
        "expression_value_dag",
        "cross_sectional_rank_mapping",
        "turnover_and_cost",
    }
    blocks: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for event in events:
        phase = str(event.get("phase") or "")
        if phase == "global_trade_time_barrier" and int(event.get("blocks_processed") or 0) == 1:
            if current is not None:
                blocks.append(current)
            current = {
                "block_ordinal": len(blocks),
                "compute_wall_seconds": 0.0,
                "compute_cpu_seconds": 0.0,
                "complete": False,
            }
        elif current is not None and phase in compute_phases:
            current["compute_wall_seconds"] += float(event.get("wall_seconds") or 0.0)
            current["compute_cpu_seconds"] += float(event.get("cpu_seconds") or 0.0)
        elif current is not None and phase == "checkpoint" and int(event.get("blocks_processed") or 0) == 1:
            current["complete"] = True
    if current is not None:
        blocks.append(current)
    for row in blocks:
        row["normalized_cpu_utilization"] = float(row["compute_cpu_seconds"]) / max(
            1e-12, float(row["compute_wall_seconds"]) * int(allocated_threads)
        )
    return [row for row in blocks if row["complete"]]


def _longest_consecutive(values: Sequence[bool]) -> int:
    longest = current = 0
    for value in values:
        current = current + 1 if value else 0
        longest = max(longest, current)
    return longest


def _phase3cm_semantic_digest(result_path: Path) -> str:
    result = json.loads(result_path.read_text(encoding="utf-8"))
    return _stable_hash(
        {
            "backend": result.get("backend"),
            "pair_count": result.get("pair_count"),
            "candidate_rewards": result.get("candidate_rewards"),
            "pair_results": result.get("pair_results"),
            "support_identities": result.get("support_identities"),
        }
    )


def _runtime_gate(
    checkpoint_root: Path,
    compute_threads: Mapping[str, int],
    *,
    output_namespace: str = "phase3cm",
    expected_backends: Sequence[str] = ("active_bar", "stock_session"),
) -> dict[str, Any]:
    expected = tuple(dict.fromkeys(str(backend) for backend in expected_backends))
    backends = {}
    overall_execution = True
    overall_run_health = True
    for backend in expected:
        backend_root = checkpoint_root / output_namespace / backend
        result_path = backend_root / "CN_STREAMING_BACKEND_RESULT.json"
        if not result_path.is_file():
            continue
        result = json.loads(result_path.read_text(encoding="utf-8"))
        timing_path = backend_root / "CN_PHASE3CM_PHASE_TIMING.jsonl"
        events = [json.loads(line) for line in timing_path.read_text(encoding="utf-8").splitlines() if line]
        compute = [
            row for row in events
            if row.get("phase") in {"expression_value_dag", "cross_sectional_rank_mapping", "turnover_and_cost"}
        ]
        compute_wall = sum(float(row.get("wall_seconds") or 0.0) for row in compute)
        compute_cpu = sum(float(row.get("cpu_seconds") or 0.0) for row in compute)
        normalized = compute_cpu / max(1e-12, compute_wall * compute_threads[backend])
        effective_cores = compute_cpu / max(1e-12, compute_wall)
        physical_cpu_count = _physical_cpu_count()
        logical_cpu_count = _logical_cpu_count()
        host_logical_occupancy = effective_cores / max(1, logical_cpu_count)
        primary_host_occupancy_pass = (
            backend != "active_bar"
            or host_logical_occupancy >= MIN_PRIMARY_HOST_LOGICAL_OCCUPANCY
        )
        blocks = _block_compute_rows(events, compute_threads[backend])
        threshold = 0.55 if backend == "active_bar" else 0.50
        sustained_blocks = _longest_consecutive(
            [float(row["normalized_cpu_utilization"]) >= threshold for row in blocks]
        )
        sample_path = backend_root / "runtime_samples.json"
        samples = json.loads(sample_path.read_text(encoding="utf-8")) if sample_path.is_file() else []
        duration = max((float(row.get("elapsed_seconds") or 0.0) for row in samples), default=float(result.get("wall_seconds") or 0.0))
        last = samples[-1] if samples else {}
        first = samples[0] if samples else {}
        read_bytes = max(0, int(last.get("system_read_bytes") or 0) - int(first.get("system_read_bytes") or 0))
        write_bytes = max(0, int(last.get("system_write_bytes") or 0) - int(first.get("system_write_bytes") or 0))
        compute_phase_parallelism = dict(result.get("compute_phase_parallelism") or {})
        required_parallel_phases = {
            "expression_value_dag",
            "cross_sectional_rank_mapping",
            "label_free_behavior",
            "turnover_and_cost",
        }
        present_parallel_phases = required_parallel_phases & set(compute_phase_parallelism)
        phase_totals = dict(result.get("phase_totals") or {})
        present_parallel_wall_seconds = sum(
            float((phase_totals.get(phase) or {}).get("wall_seconds") or 0.0)
            for phase in present_parallel_phases
        )
        parallelism_phase_minimum_wall_seconds = max(
            PARALLELISM_PHASE_MINIMUM_WALL_SECONDS,
            PARALLELISM_PHASE_MINIMUM_WALL_FRACTION
            * present_parallel_wall_seconds,
        )
        timed_parallel_phases = {
            phase
            for phase in present_parallel_phases
            if float((phase_totals.get(phase) or {}).get("wall_seconds") or 0.0)
            >= parallelism_phase_minimum_wall_seconds
        }
        parallelism_required_phases = (
            timed_parallel_phases
            if timed_parallel_phases
            else present_parallel_phases
        )
        parallel = (
            all(
                str(compute_phase_parallelism[phase].get("parallelism_status") or "")
                == "PARALLELISM_ENGAGED"
                for phase in parallelism_required_phases
            )
            if present_parallel_phases == required_parallel_phases
            else result.get("parallelism_status") == "PARALLELISM_ENGAGED"
        )
        io_wall = float((phase_totals.get("global_trade_time_barrier") or {}).get("wall_seconds") or 0.0)
        total_wall = max(float(result.get("wall_seconds") or 0.0), 1e-12)
        read_throughput = read_bytes / max(duration, 1.0)
        minimum_free = min(
            (int(row.get("available_memory_bytes") or 0) for row in samples),
            default=0,
        )
        peak_rss = int(result.get("peak_rss_bytes") or 0)
        full_host_native_ceiling = (
            backend == "active_bar"
            and int(compute_threads[backend]) >= max(1, logical_cpu_count - 2)
            and normalized >= threshold
            and sustained_blocks >= 3
            and effective_cores >= 1.10 * physical_cpu_count
        )
        if full_host_native_ceiling and not primary_host_occupancy_pass:
            bottleneck_class = "FULL_HOST_NATIVE_KERNEL_SMT_CEILING_PROVEN"
            alternative_pass = True
        elif (
            normalized >= threshold
            and sustained_blocks >= 3
            and not primary_host_occupancy_pass
        ):
            bottleneck_class = "HOST_COMPUTE_UNDERALLOCATED"
            alternative_pass = False
        elif normalized >= threshold and sustained_blocks >= 3:
            bottleneck_class = "CPU_COMPUTE_SATURATED"
            alternative_pass = False
        elif io_wall / total_wall >= 0.40 and read_throughput >= 100 * 1024**2:
            bottleneck_class = "IO_BOUND_PROVEN"
            alternative_pass = True
        elif peak_rss >= 40 * 1024**3 or (0 < minimum_free <= 16 * 1024**3):
            bottleneck_class = "MEMORY_BOUND_PROVEN"
            alternative_pass = True
        elif max((int(row.get("process_tree_count") or 0) for row in samples), default=0) > 1:
            bottleneck_class = "SCHEDULER_FRAGMENTATION"
            alternative_pass = False
        else:
            bottleneck_class = "LOW_UTILIZATION_UNEXPLAINED"
            alternative_pass = False
        allocated_pool_compute_pass = normalized >= threshold and sustained_blocks >= 3
        utilization_pass = len(blocks) >= 3 and (
            (allocated_pool_compute_pass and primary_host_occupancy_pass)
            or alternative_pass
        )
        resource_pass = (
            peak_rss <= PEAK_RSS_LIMIT_BYTES
            and minimum_free >= MINIMUM_FREE_MEMORY_BYTES
        )
        expression_audits = list(result.get("expression_audits") or [])
        cache_hits = sum(int(row.get("cache_hits") or 0) for row in expression_audits)
        cache_misses = sum(
            int(row.get("value_node_evaluations") or 0)
            + int(row.get("mapping_node_evaluations") or 0)
            for row in expression_audits
        )
        pair_results = list(result.get("pair_results") or [])
        pair_ids = [str(row.get("pair_id") or "") for row in pair_results]
        duplicate_pair_evaluations = len(pair_ids) - len(set(pair_ids))
        cache_pass = cache_hits + cache_misses > 0
        exact_once_pass = duplicate_pair_evaluations == 0
        backend_pass = parallel and utilization_pass and cache_pass and exact_once_pass
        overall_execution = overall_execution and backend_pass
        overall_run_health = overall_run_health and resource_pass
        backends[backend] = {
            "allocated_compute_threads": compute_threads[backend],
            "process_cpu_seconds": compute_cpu,
            "compute_wall_seconds": compute_wall,
            "effective_compute_cores": effective_cores,
            "host_physical_cpu_count": physical_cpu_count,
            "host_logical_cpu_count": logical_cpu_count,
            "host_logical_cpu_occupancy": host_logical_occupancy,
            "required_primary_host_logical_cpu_occupancy": (
                MIN_PRIMARY_HOST_LOGICAL_OCCUPANCY if backend == "active_bar" else None
            ),
            "primary_host_occupancy_pass": primary_host_occupancy_pass,
            "normalized_cpu_utilization": normalized,
            "required_normalized_cpu_utilization": threshold,
            "complete_compute_block_count": len(blocks),
            "sustained_blocks_meeting_threshold": sustained_blocks,
            "per_block_compute": blocks,
            "parallelism_engaged": parallel,
            "parallelism_required_phases": sorted(parallelism_required_phases),
            "parallelism_below_measurement_resolution_phases": sorted(
                present_parallel_phases - parallelism_required_phases
            ),
            "parallelism_phase_minimum_wall_seconds": (
                parallelism_phase_minimum_wall_seconds
            ),
            "parallelism_phase_minimum_wall_fraction": (
                PARALLELISM_PHASE_MINIMUM_WALL_FRACTION
            ),
            "system_cpu_percent_mean": statistics.mean(
                [float(row.get("system_cpu_percent") or 0.0) for row in samples]
            ) if samples else None,
            "active_threads_max": max((int(row.get("process_tree_threads") or 0) for row in samples), default=0),
            "heavy_worker_count_max": max((int(row.get("process_tree_count") or 0) for row in samples), default=0),
            "peak_rss_bytes": peak_rss,
            "minimum_free_memory_bytes": minimum_free,
            "minimum_required_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
            "run_health_status": "PASS" if resource_pass else "MEMORY_HEADROOM_GATE_FAILED",
            "read_throughput_bytes_per_second": read_throughput,
            "write_throughput_bytes_per_second": write_bytes / max(duration, 1.0),
            "rows_per_second": int(result.get("rows_processed") or 0) / max(float(result.get("wall_seconds") or 0.0), 1.0),
            "matched_pairs_per_hour": int(result.get("pair_count") or 0) * 3600 / max(float(result.get("wall_seconds") or 0.0), 1.0),
            "matched_pairs_per_cpu_hour": int(result.get("pair_count") or 0) * 3600 / max(compute_cpu, 1.0),
            "phase_wall_time_breakdown": phase_totals,
            "hot_path_bottleneck": bottleneck_class,
            "evaluator_reported_bottleneck": result.get("hot_path_bottleneck"),
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "cache_hit_rate": cache_hits / max(1, cache_hits + cache_misses),
            "evaluation_cache_key_status": "BOUND_IN_FROZEN_EXECUTION_AND_DAG_PLAN",
            "duplicate_exact_pair_evaluations": duplicate_pair_evaluations,
            "exact_pair_evaluated_once": exact_once_pass,
            "checkpoint_status": "PASS" if "checkpoint" in (result.get("phase_totals") or {}) else "FAIL",
            "status": "PASS" if backend_pass else "FAIL",
        }
    expected_outputs_complete = set(backends) == set(expected) and bool(expected)
    return {
        "schema_version": "cn_medium_campaign_runtime_utilization_gate_v4",
        "status": (
            "PASS"
            if overall_execution and overall_run_health and expected_outputs_complete
            else "PASS_WITH_RUN_HEALTH_FAILURE"
            if overall_execution and expected_outputs_complete
            else "RUNTIME_ACCELERATION_GATE_FAILED"
        ),
        "expected_backends": list(expected),
        "observed_backends": list(backends),
        "backends": backends,
        "bounded_concurrency_adjustment_count": 0,
        "second_failure_policy": "RUN_INVALID",
        "run_health_policy": "INFRASTRUCTURE_ONLY_DOES_NOT_MUTATE_ROUTE_HEALTH",
    }


def _bounded_runtime_adjustment(
    *,
    initial_gate: Mapping[str, Any],
    checkpoint_id: str,
    checkpoint_root: Path,
    binding_path: Path,
    table_paths: Mapping[str, Path],
    split_manifest: Path,
    field_roots: Mapping[str, Path],
    label_roots: Mapping[str, Path],
    purity_path: Path,
    compute_threads: Mapping[str, int],
    deadline_epoch: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    failed = [
        backend
        for backend, row in dict(initial_gate.get("backends") or {}).items()
        if row.get("status") != "PASS"
        and row.get("hot_path_bottleneck")
        in {"LOW_UTILIZATION_UNEXPLAINED", "HOST_COMPUTE_UNDERALLOCATED"}
    ]
    if not failed:
        return dict(initial_gate), []
    adjusted_threads = dict(compute_threads)
    if "active_bar" in failed:
        logical_cpu_count = _logical_cpu_count()
        adjusted_threads["active_bar"] = min(
            max(1, logical_cpu_count - 2),
            max(12, int(compute_threads["active_bar"]) + 4),
        )
    if "stock_session" in failed:
        adjusted_threads["stock_session"] = min(4, max(3, int(compute_threads["stock_session"]) + 1))
    if all(adjusted_threads[backend] == int(compute_threads[backend]) for backend in failed):
        unchanged = json.loads(json.dumps(initial_gate))
        unchanged["bounded_concurrency_adjustment_count"] = 0
        unchanged["bounded_concurrency_adjustment"] = {
            "reason": "NO_ACTIONABLE_THREAD_INCREASE_AVAILABLE",
            "backends": failed,
            "initial_threads": dict(compute_threads),
            "adjusted_threads": adjusted_threads,
            "sweep_performed": False,
        }
        unchanged["status"] = "RUN_INVALID_NO_ACTIONABLE_CONCURRENCY_ADJUSTMENT"
        return unchanged, []
    receipts = _run_phase3cm_monitored(
        checkpoint_id=checkpoint_id,
        checkpoint_root=checkpoint_root,
        binding_path=binding_path,
        table_paths=table_paths,
        split_manifest=split_manifest,
        field_roots=field_roots,
        label_roots=label_roots,
        purity_path=purity_path,
        compute_threads=adjusted_threads,
        deadline_epoch=deadline_epoch,
        output_namespace="phase3cm_adjustment_1",
        selected_backends=failed,
    )
    adjusted_gate = _runtime_gate(
        checkpoint_root,
        adjusted_threads,
        output_namespace="phase3cm_adjustment_1",
        expected_backends=failed,
    )
    combined = json.loads(json.dumps(initial_gate))
    parity: dict[str, bool] = {}
    for backend in failed:
        original_path = checkpoint_root / "phase3cm" / backend / "CN_STREAMING_BACKEND_RESULT.json"
        adjusted_path = (
            checkpoint_root
            / "phase3cm_adjustment_1"
            / backend
            / "CN_STREAMING_BACKEND_RESULT.json"
        )
        parity[backend] = (
            _phase3cm_semantic_digest(original_path)
            == _phase3cm_semantic_digest(adjusted_path)
        )
        adjusted_row = dict((adjusted_gate.get("backends") or {}).get(backend) or {})
        adjusted_row["semantic_parity_with_initial_run"] = parity[backend]
        combined["backends"][backend] = adjusted_row
    combined["bounded_concurrency_adjustment_count"] = 1
    combined["bounded_concurrency_adjustment"] = {
        "reason": "HOST_OR_ALLOCATED_POOL_UNDERUTILIZATION",
        "backends": failed,
        "initial_threads": dict(compute_threads),
        "adjusted_threads": adjusted_threads,
        "semantic_parity": parity,
        "sweep_performed": False,
    }
    combined["status"] = (
        "PASS"
        if all(row.get("status") == "PASS" for row in combined["backends"].values())
        and all(parity.values())
        else "RUN_INVALID_AFTER_SINGLE_BOUNDED_ADJUSTMENT"
    )
    return combined, receipts


def _initial_feedback() -> list[dict[str, Any]]:
    return [{"route_id": route, "actionable_support": 0} for route in ROUTE_IDS]


def _annotate_generation_metadata(
    rows: Sequence[Mapping[str, Any]], registry: UnifiedCapabilityRegistry
) -> list[dict[str, Any]]:
    output = []
    for source in rows:
        row = dict(source)
        fields = [
            registry.resolve(str(field_id))
            for field_id in row.get("declared_field_ids") or ()
            if str(field_id) in {field.field_id for field in registry.fields}
        ]
        row["campaign_source_family"] = "|".join(sorted({field.source_family for field in fields}))
        row["campaign_representation_family"] = "|".join(
            sorted(
                {
                    str((field.metadata.get("canonical_representation") or {}).get("semantic_family") or field.source_family)
                    for field in fields
                }
            )
        )
        row["campaign_field_pair_family"] = _stable_hash(
            sorted(field.representation_id for field in fields)
        )[:20]
        output.append(row)
    return output


def _close_route_funnels(
    *,
    funnels: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    closed: list[dict[str, Any]] = []
    for source in funnels:
        row = dict(source)
        route_id = str(row["route_id"])
        route_decisions = [
            item for item in decisions if str(item.get("route_id") or "") == route_id
        ]
        route_outcomes = [
            item for item in outcomes if str(item.get("route_id") or "") == route_id
        ]
        row.update(
            {
                "skeleton_compatible_pairs": max(
                    0,
                    int(row.get("generation_attempts") or 0)
                    - int(row.get("skeleton_compatibility_rejects") or 0),
                ),
                "behavior_resolved_pairs": sum(
                    str(item.get("behavior_status") or "") == "RESOLVED"
                    for item in route_decisions
                ),
                "behavior_unique_pairs": sum(
                    str(item.get("admission_reason") or "")
                    == "LABEL_FREE_BEHAVIOR_UNIQUE"
                    for item in route_decisions
                ),
                "admitted_pairs": sum(
                    str(item.get("admission_decision") or "") == "ADMIT"
                    for item in route_decisions
                ),
                "full_coordinate_development_matched_evaluated_pairs": len(
                    route_outcomes
                ),
                "positive_matched_increment_pairs": sum(
                    float(item.get("matched_net_increment") or 0.0) > 0.0
                    for item in route_outcomes
                ),
                "cycle_exhausted": str(row.get("underfill_reason") or "")
                == "GENERATION_ATTEMPT_LIMIT",
            }
        )
        closed.append(row)
    return closed


def _verify_closed_checkpoint_manifest(
    *,
    checkpoint_root: Path,
    checkpoint_id: str,
    previous_manifest: Path | None,
) -> tuple[Path, dict[str, Any]] | None:
    """Verify a closed checkpoint before any campaign code may write into it."""

    manifest_path = checkpoint_root / "batch_manifest.json"
    if not manifest_path.is_file():
        return None
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    if str(manifest.get("schema_version") or "") != "cn_iterative_search_v1_batch_manifest_v1":
        raise RuntimeError(f"{checkpoint_id}: unsupported closed checkpoint manifest schema")
    if str(manifest.get("batch_id") or "") != checkpoint_id:
        raise RuntimeError(f"{checkpoint_id}: closed checkpoint manifest batch drift")
    if str(manifest.get("status") or "") != "BATCH_CLOSED_IMMUTABLE":
        raise RuntimeError(f"{checkpoint_id}: checkpoint manifest is not immutable-closed")
    payload = dict(manifest)
    recorded_payload_hash = str(payload.pop("manifest_payload_hash", ""))
    if not recorded_payload_hash or _stable_hash(payload) != recorded_payload_hash:
        raise RuntimeError(f"{checkpoint_id}: immutable manifest self-hash mismatch")
    if any(int(manifest.get(key) or 0) != 0 for key in ("validation_reads", "holdout_reads", "forward_2026_reads")):
        raise RuntimeError(f"{checkpoint_id}: closed development checkpoint contains sealed reads")
    if str(manifest.get("promotion") or "") != "FORBIDDEN":
        raise RuntimeError(f"{checkpoint_id}: closed development checkpoint promotion drift")

    expected_prior = _sha256(previous_manifest) if previous_manifest else "GENESIS"
    input_hashes = dict(manifest.get("input_hashes") or {})
    if str(input_hashes.get("prior_checkpoint_manifest") or "") != expected_prior:
        raise RuntimeError(f"{checkpoint_id}: prior checkpoint manifest chain mismatch")
    expected_schedule_source = (
        _sha256(previous_manifest) if previous_manifest else "FROZEN_INITIAL_PRIOR"
    )
    if str(input_hashes.get("adaptive_schedule_source") or "") != expected_schedule_source:
        raise RuntimeError(f"{checkpoint_id}: adaptive schedule source chain mismatch")

    root = checkpoint_root.resolve()
    artifact_paths: set[Path] = set()
    for artifact in manifest.get("artifacts") or ():
        relative = Path(str(artifact.get("path") or ""))
        path = (root / relative).resolve()
        if not path.is_relative_to(root):
            raise RuntimeError(f"{checkpoint_id}: manifest artifact escapes checkpoint root")
        if not path.is_file():
            raise RuntimeError(f"{checkpoint_id}: immutable artifact missing: {relative}")
        if path.stat().st_size != int(artifact.get("bytes") or -1):
            raise RuntimeError(f"{checkpoint_id}: immutable artifact byte drift: {relative}")
        if _sha256(path) != str(artifact.get("sha256") or ""):
            raise RuntimeError(f"{checkpoint_id}: immutable artifact hash drift: {relative}")
        artifact_paths.add(path)

    for receipt in manifest.get("access_receipts") or ():
        result_path = Path(str(receipt.get("result_path") or "")).resolve()
        if result_path not in artifact_paths:
            raise RuntimeError(f"{checkpoint_id}: result receipt is not manifest-bound")
        if _sha256(result_path) != str(receipt.get("result_sha256") or ""):
            raise RuntimeError(f"{checkpoint_id}: result receipt hash drift")

    binding_path = checkpoint_root / "phase3cm_input_binding.json"
    binding = json.loads(binding_path.read_text(encoding="utf-8-sig"))
    result_pair_count = 0
    result_binding_hashes: set[str] = set()
    for backend in ("active_bar", "stock_session"):
        result_path = checkpoint_root / "phase3cm" / backend / "CN_STREAMING_BACKEND_RESULT.json"
        if not result_path.is_file():
            continue
        result = json.loads(result_path.read_text(encoding="utf-8-sig"))
        if str(result.get("status") or "") != "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED":
            raise RuntimeError(f"{checkpoint_id}: incomplete closed Phase3CM result on {backend}")
        result_pair_count += int(result.get("pair_count") or 0)
        result_binding_hashes.add(str(result.get("input_binding_hash") or ""))
    if result_binding_hashes != {str(binding.get("binding_hash") or "")}:
        raise RuntimeError(f"{checkpoint_id}: closed Phase3CM binding identity drift")
    if result_pair_count != int(binding.get("pair_count") or 0):
        raise RuntimeError(f"{checkpoint_id}: closed Phase3CM pair-count drift")
    return manifest_path, manifest


def _rehydrate_closed_checkpoint(
    *, checkpoint_root: Path, checkpoint_id: str, manifest_path: Path
) -> dict[str, Any]:
    """Load campaign state from immutable artifacts without writing the checkpoint."""

    schedule = pd.read_parquet(checkpoint_root / "schedule.parquet").fillna("").to_dict(orient="records")
    generated = pd.read_parquet(checkpoint_root / "candidate_attempts.parquet").fillna("").to_dict(orient="records")
    funnels = pd.read_parquet(checkpoint_root / "route_funnel.parquet").fillna("").to_dict(orient="records")
    probe_rows = pd.read_parquet(checkpoint_root / "behavior_probe.parquet").fillna("").to_dict(orient="records")
    decisions = pd.read_parquet(checkpoint_root / "admission_decisions.parquet").fillna("").to_dict(orient="records")
    health = pd.read_parquet(checkpoint_root / "route_health.parquet").fillna("").to_dict(orient="records")
    outcomes = pd.read_parquet(checkpoint_root / "observation_ledger.parquet").fillna("").to_dict(orient="records")
    full_behavior = pd.read_parquet(checkpoint_root / "full_behavior.parquet").fillna("").to_dict(orient="records")
    metrics = pd.read_parquet(checkpoint_root / "campaign_metrics.parquet").fillna("").to_dict(orient="records")
    admitted_ids = {
        str(row.get("pair_id") or "")
        for row in decisions
        if str(row.get("admission_decision") or "") == "ADMIT"
    }
    admitted = [
        row for row in generated if str(row.get("pair_id") or "") in admitted_ids
    ]
    ledger, positive, negative, run_health = build_iterative_feedback_views(outcomes)
    manifest_sha256 = _sha256(manifest_path)
    return {
        "schedule": schedule,
        "generated": generated,
        "funnels": funnels,
        "probe_rows": probe_rows,
        "decisions": decisions,
        "health": health,
        "outcomes": outcomes,
        "full_behavior": full_behavior,
        "metrics": metrics,
        "admitted": admitted,
        "ledger": ledger,
        "positive": positive,
        "negative": negative,
        "run_health": run_health,
        "behavior_archive": PortfolioBehaviorArchive.read_parquet(
            checkpoint_root / "behavior_archive.parquet"
        ),
        "raw_attempts": sum(int(row.get("generation_attempts") or 0) for row in funnels),
        "summary": {
            "checkpoint": checkpoint_id,
            "scheduled_pairs": sum(int(row.get("scheduled_pairs") or 0) for row in schedule),
            "generated_pairs": len(generated) // 2,
            "admitted_pairs": len(admitted) // 2,
            "pair_result_count": len(outcomes),
            "evaluated_pairs": sum(
                str(row.get("pair_evaluation_status") or "") == "PAIR_EVALUATED"
                for row in outcomes
            ),
            "raw_attempts": sum(int(row.get("generation_attempts") or 0) for row in funnels),
            "positive_matched_increments": sum(
                float(row.get("matched_net_increment") or 0.0) > 0.0
                for row in outcomes
            ),
            "manifest_sha256": manifest_sha256,
            "resume_action": "REUSED_VERIFIED_CLOSED_CHECKPOINT",
        },
    }


def _backend_cpu_cost(checkpoint_root: Path) -> dict[str, dict[str, float]]:
    output: dict[str, dict[str, float]] = {}
    compute_phases = {
        "expression_value_dag",
        "cross_sectional_rank_mapping",
        "turnover_and_cost",
    }
    for backend in ("active_bar", "stock_session"):
        root = checkpoint_root / "phase3cm" / backend
        result_path = root / "CN_STREAMING_BACKEND_RESULT.json"
        timing_path = root / "CN_PHASE3CM_PHASE_TIMING.jsonl"
        if not result_path.is_file() or not timing_path.is_file():
            continue
        result = json.loads(result_path.read_text(encoding="utf-8"))
        events = [
            json.loads(line)
            for line in timing_path.read_text(encoding="utf-8").splitlines()
            if line
        ]
        output[backend] = {
            "pair_count": float(result.get("pair_count") or 0),
            "cpu_seconds": sum(
                float(row.get("cpu_seconds") or 0.0)
                for row in events
                if row.get("phase") in compute_phases
            ),
        }
    return output


def _metrics_rows(
    *,
    checkpoint_id: str,
    schedule: Sequence[Mapping[str, Any]],
    funnel: Sequence[Mapping[str, Any]],
    admitted: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    full_behavior: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    negative: Sequence[Mapping[str, Any]],
    checkpoint_root: Path,
    known_behavior_families: set[str],
) -> list[dict[str, Any]]:
    outcome_by_pair = {str(row["pair_id"]): row for row in outcomes}
    behavior_by_pair = {str(row["pair_id"]): row for row in full_behavior}
    primary = [row for row in admitted if str(row.get("pair_member_role")) == "PRIMARY"]
    schedule_by_route = {str(row["route_id"]): row for row in schedule}
    funnel_by_route = {str(row["route_id"]): row for row in funnel}
    backend_cost = _backend_cpu_cost(checkpoint_root)
    negative_by_pair = {
        str(row.get("pair_id") or ""): set(row.get("negative_labels") or ())
        for row in negative
    }
    decision_by_pair = {
        str(row.get("pair_id") or ""): row for row in decisions
    }
    rows = []
    dimensions = {
        "checkpoint": lambda row: checkpoint_id,
        "route": lambda row: str(row.get("route_id") or ""),
        "skeleton": lambda row: str(row.get("skeleton_id") or ""),
        "source_family": lambda row: str(row.get("campaign_source_family") or ""),
        "representation_family": lambda row: str(row.get("campaign_representation_family") or ""),
        "operator_family": lambda row: str(row.get("operator_family") or ""),
        "field_pair_family": lambda row: str(row.get("campaign_field_pair_family") or ""),
    }
    for level, key_fn in dimensions.items():
        grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in primary:
            grouped[key_fn(row)].append(row)
        for key, members in grouped.items():
            pair_ids = [str(row["pair_id"]) for row in members]
            outcome_rows = [outcome_by_pair[pair_id] for pair_id in pair_ids if pair_id in outcome_by_pair]
            rewards = [float(row.get("matched_net_increment") or 0.0) for row in outcome_rows]
            families = {
                str(behavior_by_pair[pair_id].get("portfolio_behavior_family_id") or "")
                for pair_id in pair_ids if pair_id in behavior_by_pair
            } - {""}
            new_families = families - known_behavior_families
            route_id = key if level == "route" else ""
            if level == "checkpoint":
                funnel_row = {
                    name: sum(int(row.get(name) or 0) for row in funnel)
                    for name in (
                        "generation_attempts",
                        "exact_unique_pairs",
                        "materialization_missing_field_pairs",
                        "materialization_unsupported_pairs",
                        "behavior_resolved_pairs",
                        "behavior_unique_pairs",
                        "admitted_pairs",
                        "skeleton_compatibility_rejects",
                    )
                }
            else:
                funnel_row = funnel_by_route.get(route_id, {})
            attempts = int(funnel_row.get("generation_attempts") or 0)
            if level == "route":
                member_decisions = [
                    row
                    for row in decisions
                    if str(row.get("route_id") or "") == route_id
                ]
            elif level == "checkpoint":
                member_decisions = list(decisions)
            else:
                member_decisions = [
                    decision_by_pair[pair_id]
                    for pair_id in pair_ids
                    if pair_id in decision_by_pair
                ]
            labels = [
                label
                for pair_id in pair_ids
                for label in negative_by_pair.get(pair_id, set())
            ]
            estimated_cpu_seconds = 0.0
            for backend, cost in backend_cost.items():
                backend_pairs = sum(
                    _clock_for_route(str(row.get("route_id") or "")) == backend
                    for row in members
                )
                estimated_cpu_seconds += backend_pairs * float(cost["cpu_seconds"]) / max(
                    1.0, float(cost["pair_count"])
                )
            rows.append(
                {
                    "checkpoint": checkpoint_id,
                    "aggregation_level": level,
                    "aggregation_key": key,
                    "scheduled_pairs": (
                        sum(int(row.get("scheduled_pairs") or 0) for row in schedule)
                        if level == "checkpoint"
                        else int(schedule_by_route.get(route_id, {}).get("scheduled_pairs") or 0)
                    ),
                    "raw_attempts": attempts,
                    "exact_unique_per_1000_attempts": 1000 * int(funnel_row.get("exact_unique_pairs") or 0) / max(1, attempts),
                    "materialization_valid_per_1000_attempts": 1000 * (attempts - int(funnel_row.get("materialization_missing_field_pairs") or 0)) / max(1, attempts),
                    "behavior_resolved_per_1000_attempts": 1000 * int(funnel_row.get("behavior_resolved_pairs") or 0) / max(1, attempts),
                    "behavior_unique_per_1000_attempts": 1000 * int(funnel_row.get("behavior_unique_pairs") or 0) / max(1, attempts),
                    "admitted_pairs": len(pair_ids),
                    "full_coordinate_development_matched_pairs": len(outcome_rows),
                    "pairs_per_cpu_hour": len(outcome_rows) * 3600 / max(1.0, estimated_cpu_seconds),
                    "new_behavior_families": len(new_families),
                    "new_behavior_families_per_100_evaluated": 100 * len(new_families) / max(1, len(outcome_rows)),
                    "positive_matched_increments": sum(value > 0 for value in rewards),
                    "positive_matched_increments_per_100_evaluated": 100 * sum(value > 0 for value in rewards) / max(1, len(outcome_rows)),
                    "mean_matched_net_increment": statistics.mean(rewards) if rewards else None,
                    "median_matched_net_increment": statistics.median(rewards) if rewards else None,
                    "top_decile_matched_net_increment": float(pd.Series(rewards).quantile(0.9)) if rewards else None,
                    "unsupported_operator_rate": int(funnel_row.get("materialization_unsupported_pairs") or 0) / max(1, attempts),
                    "materialization_missing_rate": int(funnel_row.get("materialization_missing_field_pairs") or 0) / max(1, attempts),
                    "cost_killed_rate": labels.count("COST_KILLED") / max(1, len(outcome_rows)),
                    "turnover_killed_rate": labels.count("TURNOVER_KILLED") / max(1, len(outcome_rows)),
                    "behavior_duplicate_rate": sum(
                        str(row.get("admission_reason") or "") == "EXACT_BEHAVIOR_DUPLICATE"
                        for row in member_decisions
                    ) / max(1, len(member_decisions)),
                    "cycle_exhaustion_rate": (
                        sum(bool(row.get("cycle_exhausted")) for row in funnel)
                        / max(1, len(funnel))
                        if level == "checkpoint"
                        else float(bool(funnel_row.get("cycle_exhausted")))
                    ),
                    "marginal_discovery_rate": len(new_families) / max(1, len(outcome_rows)),
                }
            )
    return rows


def _productivity_status(metrics: Sequence[Mapping[str, Any]], routes: set[str]) -> str:
    rows = [
        row for row in metrics
        if row.get("aggregation_level") == "route" and row.get("aggregation_key") in routes
    ]
    support = sum(int(row.get("full_coordinate_development_matched_pairs") or 0) for row in rows)
    productive_checkpoints = len(
        {
            str(row["checkpoint"])
            for row in rows
            if (row.get("median_matched_net_increment") is not None and float(row["median_matched_net_increment"]) > 0)
        }
    )
    medians = [float(row["median_matched_net_increment"]) for row in rows if row.get("median_matched_net_increment") is not None]
    if support >= 64 and productive_checkpoints >= 2 and medians and statistics.median(medians) > 0:
        return "QUALIFIED"
    if support >= 32:
        return "MIXED"
    return "NOT_QUALIFIED"


def _checkpoint_elites(
    *,
    candidates: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    limit_per_checkpoint: int = 8,
) -> list[dict[str, Any]]:
    primary_by_pair = {
        (str(row.get("checkpoint") or ""), str(row.get("pair_id") or "")): row
        for row in candidates
        if str(row.get("pair_member_role") or "") == "PRIMARY"
    }
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source in outcomes:
        row = dict(source)
        if str(row.get("pair_evaluation_status") or "") != "PAIR_EVALUATED":
            continue
        value = row.get("matched_train_increment")
        try:
            reward = float(value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(reward):
            continue
        row["_elite_reward"] = reward
        grouped[str(row.get("checkpoint") or "")].append(row)
    elites: list[dict[str, Any]] = []
    for checkpoint in sorted(grouped):
        ranked = sorted(
            grouped[checkpoint],
            key=lambda row: (
                -float(row["_elite_reward"]),
                str(row.get("pair_id") or ""),
            ),
        )[:limit_per_checkpoint]
        for rank, outcome in enumerate(ranked, start=1):
            candidate = primary_by_pair.get(
                (checkpoint, str(outcome.get("pair_id") or "")),
                {},
            )
            elites.append(
                {
                    "checkpoint": checkpoint,
                    "rank": rank,
                    "pair_id": str(outcome.get("pair_id") or ""),
                    "route_id": str(outcome.get("route_id") or ""),
                    "candidate_id": str(candidate.get("candidate_id") or ""),
                    "expression": str(candidate.get("expression") or ""),
                    "exact_identity": str(
                        candidate.get("exact_identity") or ""
                    ),
                    "structural_family_id": str(
                        candidate.get("structural_family_id") or ""
                    ),
                    "matched_train_increment": float(
                        outcome["_elite_reward"]
                    ),
                    "matched_net_increment": outcome.get(
                        "matched_net_increment"
                    ),
                    "pair_turnover_metric": outcome.get(
                        "pair_turnover_metric"
                    ),
                    "selection_scope": "TRAIN_ONLY_CHECKPOINT_ELITE",
                    "validation_feedback": "FORBIDDEN",
                    "promotion": "FORBIDDEN",
                }
            )
    return elites


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"full materialization and Phase3CM are authorized only on 77o ({AUTHORIZED_HOST})"
        )
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    profile = _campaign_profile(
        str(getattr(args, "campaign_profile", LEGACY_CAMPAIGN_PROFILE))
    )
    registry_path = args.registry.resolve()
    registry = UnifiedCapabilityRegistry.read(registry_path)
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    field_roots = {"active_bar": args.active_field_root.resolve(), "stock_session": args.session_field_root.resolve()}
    label_roots = {"active_bar": args.active_label_root.resolve(), "stock_session": args.session_label_root.resolve()}
    compute_threads = {"active_bar": int(args.active_threads), "stock_session": int(args.session_threads)}
    campaign_authorization = _campaign_authorization_binding(
        authorization_path=args.campaign_authorization,
        history_manifest_path=args.historical_archive_manifest,
        candidate_archive_path=args.historical_candidate_archive,
        behavior_archive_path=args.historical_behavior_archive,
        seed_base=args.seed_base,
        active_threads=compute_threads["active_bar"],
        session_threads=compute_threads["stock_session"],
        profile=profile,
    )
    campaign_id = str(campaign_authorization["campaign_id"])
    campaign_authorization_path = _write_json(
        output_root / "campaign_authorization_binding.json",
        campaign_authorization,
    )
    historical_exact, behavior_archive, archive_snapshot = _load_historical_dedupe(
        candidate_archive_path=args.historical_candidate_archive.resolve(),
        behavior_archive_path=args.historical_behavior_archive.resolve(),
    )
    registry_binding = _registry_binding(registry_path, registry)
    registry_binding_path = _write_json(output_root / "registry_binding.json", registry_binding)
    discovery_authority = load_development_discovery_root_authority(
        args.discovery_contract.resolve(),
        registry=registry,
    )
    authorization = json.loads(
        args.discovery_authorization.resolve().read_text(encoding="utf-8")
    )
    if not bool(authorization.get("execution_authorized")):
        raise RuntimeError("DEVELOPMENT_DISCOVERY_EXECUTION_NOT_AUTHORIZED")
    if str(authorization.get("root_contract_hash") or "") != discovery_authority["contract_hash"]:
        raise RuntimeError("DEVELOPMENT_DISCOVERY_AUTHORIZATION_HASH_MISMATCH")
    if str(authorization.get("validation_mode") or "") != "AUTOMATIC_POST_TRAIN_REPORT_ONLY":
        raise RuntimeError("POST_TRAIN_VALIDATION_AUTHORIZATION_MISMATCH")
    discovery_authority_path = _write_json(
        output_root / "development_discovery_authority_binding.json",
        {
            **{
                key: value
                for key, value in discovery_authority.items()
                if key != "route_root_allowlists"
            },
            "route_root_counts": {
                route_id: len(values)
                for route_id, values in discovery_authority["route_root_allowlists"].items()
            },
            "authorization_path": str(args.discovery_authorization.resolve()),
            "authorization_id": str(authorization.get("authorization_id") or ""),
            "execution_authorized": True,
            "validation_mode": "AUTOMATIC_POST_TRAIN_REPORT_ONLY",
            "status": "ONTOLOGY_ROOT_AUTHORITY_BOUND",
        },
    )
    schema_binding, schema_by_backend = materialized_schema_binding(field_roots=field_roots, registry=registry)
    schema_binding_path = _write_json(output_root / "materialized_schema_binding.json", schema_binding)
    purity = audit_split_boundary_label_purity(split=split, registry=registry, label_roots=label_roots)
    purity_path = _write_json(output_root / "split_boundary_purity.json", purity)
    if purity["status"] != "PASS":
        raise RuntimeError("SPLIT_BOUNDARY_LABEL_PURITY_FAILED")
    comparison_path = _write_json(
        output_root / "prelaunch_structural_comparison.json",
        _structural_comparison(
            registry=registry,
            schema_by_backend=schema_by_backend,
            route_root_allowlists=discovery_authority["route_root_allowlists"],
            seed=args.seed_base,
        ),
    )
    runtime_envelope = _runtime_envelope(compute_threads["active_bar"], compute_threads["stock_session"])
    runtime_envelope_path = _write_json(output_root / "runtime_envelope.json", runtime_envelope)
    if runtime_envelope["status"] != "PASS":
        raise RuntimeError(runtime_envelope["status"])

    seed_manifest = build_seed_attempt_manifest(
        registry_hash=registry.registry_hash,
        schema_hash_by_backend={
            backend: str(schema_binding["backends"][backend]["materialized_schema_hash"])
            for backend in ("active_bar", "stock_session")
        },
        grammar_hash=registry_binding["grammar_hash"],
        seed_base=args.seed_base,
        checkpoint_count=int(profile["checkpoint_count"]),
        search_routes=tuple(profile["search_routes"]),
        route_attempt_cap=int(profile["route_attempt_cap"]),
        maximum_raw_attempts=int(profile["maximum_raw_attempts"]),
        base_targets=tuple(profile["base_targets"]),
    )
    seed_manifest_path = _write_json(output_root / "seed_attempt_manifest.json", seed_manifest)
    started_epoch = time.time()
    contract = {
        "schema_version": "cn_targeted_search_medium_campaign_contract_v1",
        "campaign_id": campaign_id,
        "started_epoch": started_epoch,
        "campaign_profile": str(profile["name"]),
        "checkpoint_count": int(profile["checkpoint_count"]),
        "total_scheduled_matched_pair_budget": int(
            profile["total_scheduled_matched_pair_budget"]
        ),
        "maximum_completed_development_matched_pairs": int(
            profile["maximum_completed_development_matched_pairs"]
        ),
        "minimum_actual_evaluated_pairs": int(
            profile["minimum_actual_evaluated_pairs"]
        ),
        "scheduled_target_is_fill_requirement": bool(
            profile["evaluated_fill_required"]
        ),
        "budget_counting_unit": (
            "PAIR_EVALUATED"
            if profile["evaluated_fill_required"]
            else "FULL_COORDINATE_PAIR_RESULT"
        ),
        "maximum_raw_attempts": int(profile["maximum_raw_attempts"]),
        "maximum_wall_seconds": MAX_WALL_SECONDS,
        "constructor_profile": COMPOSITIONAL_V2_PROFILE,
        "broad_event": {"status": "FROZEN_REFERENCE_ONLY", "search_budget": 0},
        "numeric_definitions": {
            "actionable_support_pairs_per_route": 2,
            "sustained_checkpoint_count": 2,
            "qualified_group_min_evaluated_pairs": 64,
            "mixed_group_min_evaluated_pairs": 32,
            "runtime_low_utilization_consecutive_blocks": 3,
        },
        "natural_underfill_retained": not bool(profile["evaluated_fill_required"]),
        "underfill_continuation": (
            "NEXT_IMMUTABLE_CHECKPOINT_UNTIL_ACTUAL_EVALUATED_TARGET"
            if profile["evaluated_fill_required"]
            else "NONE"
        ),
        "cross_route_spillover": "FORBIDDEN",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "evaluation_name": "full-coordinate development Phase3CM pair evaluation",
        "discovery_root_contract_hash": discovery_authority["contract_hash"],
        "root_scope_authority": "FROZEN_DEVELOPMENT_DISCOVERY_CONTRACT",
        "validation_mode": "AUTOMATIC_POST_TRAIN_REPORT_ONLY",
        "input_bindings": {
            "campaign_authorization": _artifact(
                campaign_authorization_path, root=output_root
            ),
            "registry": _artifact(registry_binding_path, root=output_root),
            "development_discovery_authority": _artifact(
                discovery_authority_path, root=output_root
            ),
            "schema": _artifact(schema_binding_path, root=output_root),
            "split_purity": _artifact(purity_path, root=output_root),
            "seed_attempt": _artifact(seed_manifest_path, root=output_root),
            "historical_candidate_exact_archive": _artifact(
                args.historical_candidate_archive.resolve()
            ),
            "historical_behavior_archive": _artifact(
                args.historical_behavior_archive.resolve()
            ),
            "historical_dedupe_snapshot": {
                "exact_identity_count": archive_snapshot["exact_identity_count"],
                "exact_identity_digest": archive_snapshot["exact_identity_digest"],
                "behavior_identity_row_count": archive_snapshot[
                    "behavior_identity_row_count"
                ],
                "behavior_identity_digest": archive_snapshot[
                    "behavior_identity_digest"
                ],
                "reward_columns_imported": [],
                "scheduler_state_imported": False,
            },
        },
    }
    frozen_contract_path = _write_json(output_root / "frozen_contract.json", contract)
    archive_snapshot_path = _write_json(output_root / "archive_snapshot.json", archive_snapshot)

    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist=discovery_authority["route_root_allowlists"],
    )
    target_supply_preflight_path: Path | None = None
    if profile["evaluated_fill_required"]:
        target_route = str(profile["target_route"])
        preflight_rows, preflight_funnel = generator.generate_route_attempts(
            target_route,
            scheduled_pairs=768,
            seed=int(args.seed_base),
            attempt_start=0,
            attempt_limit=int(profile["route_attempt_cap"]),
            existing_exact_identities=set(historical_exact),
            available_field_ids=schema_by_backend[
                _clock_for_route(target_route)
            ],
        )
        fresh_exact_pairs = len(preflight_rows) // 2
        target_supply_preflight_path = _write_json(
            output_root / "target_route_supply_preflight.json",
            {
                "schema_version": "cn_target_route_supply_preflight_v1",
                "campaign_profile": str(profile["name"]),
                "route_id": target_route,
                "financial_reads": 0,
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
                "fresh_exact_pair_count": fresh_exact_pairs,
                "minimum_required_fresh_exact_pairs": 512,
                "candidate_pack_digest": _stable_hash(
                    [
                        str(row.get("exact_identity") or "")
                        for row in preflight_rows
                    ]
                ),
                "sampled_primary_expressions": [
                    {
                        "expression": str(row.get("expression") or ""),
                        "declared_field_ids": list(
                            row.get("declared_field_ids") or ()
                        ),
                        "exact_identity": str(
                            row.get("exact_identity") or ""
                        ),
                    }
                    for row in preflight_rows
                    if str(row.get("pair_member_role") or "") == "PRIMARY"
                ][:16],
                "funnel": preflight_funnel,
                "status": (
                    "PASS"
                    if fresh_exact_pairs >= 512
                    else "TARGET_ROUTE_FRESH_EXACT_SUPPLY_BELOW_512"
                ),
            },
        )
        if fresh_exact_pairs < 512:
            raise RuntimeError("TARGET_ROUTE_FRESH_EXACT_SUPPLY_BELOW_512")
    previous_feedback = _initial_feedback()
    previous_manifest: Path | None = None
    cumulative_candidates: list[dict[str, Any]] = []
    cumulative_outcomes: list[dict[str, Any]] = []
    cumulative_metrics: list[dict[str, Any]] = []
    cumulative_ledger: list[dict[str, Any]] = []
    cumulative_positive: list[dict[str, Any]] = []
    cumulative_negative: list[dict[str, Any]] = []
    cumulative_run_health: list[dict[str, Any]] = []
    completed_pairs = 0
    completed_admitted_pairs = 0
    raw_attempts = 0
    checkpoint_summaries = []
    runtime_gate_path: Path | None = None
    runtime_gate_status = "NOT_EVALUATED"
    seed_rows = {(row["checkpoint"], row["route_id"]): row for row in seed_manifest["rows"]}
    deadline_epoch = started_epoch + MAX_WALL_SECONDS

    for checkpoint_index in range(int(profile["checkpoint_count"])):
        checkpoint_id = f"checkpoint_{checkpoint_index + 1:03d}"
        checkpoint_root = output_root / "checkpoints" / checkpoint_id
        closed_checkpoint = _verify_closed_checkpoint_manifest(
            checkpoint_root=checkpoint_root,
            checkpoint_id=checkpoint_id,
            previous_manifest=previous_manifest,
        )
        if closed_checkpoint is not None:
            manifest_path, _ = closed_checkpoint
            restored = _rehydrate_closed_checkpoint(
                checkpoint_root=checkpoint_root,
                checkpoint_id=checkpoint_id,
                manifest_path=manifest_path,
            )
            previous_manifest = manifest_path
            previous_feedback = restored["health"]
            behavior_archive = restored["behavior_archive"]
            for row in restored["generated"]:
                if row.get("exact_identity"):
                    historical_exact.add(str(row["exact_identity"]))
            restored_evaluated_pairs = sum(
                str(row.get("pair_evaluation_status") or "") == "PAIR_EVALUATED"
                for row in restored["outcomes"]
            )
            completed_pairs += (
                restored_evaluated_pairs
                if profile["evaluated_fill_required"]
                else len(restored["outcomes"])
            )
            completed_admitted_pairs += len(restored["admitted"]) // 2
            raw_attempts += int(restored["raw_attempts"])
            cumulative_candidates.extend(
                {"checkpoint": checkpoint_id, **row} for row in restored["admitted"]
            )
            cumulative_outcomes.extend(
                {"checkpoint": checkpoint_id, **row} for row in restored["outcomes"]
            )
            cumulative_metrics.extend(restored["metrics"])
            cumulative_ledger.extend(
                {"checkpoint": checkpoint_id, **row} for row in restored["ledger"]
            )
            cumulative_positive.extend(
                {"checkpoint": checkpoint_id, **row} for row in restored["positive"]
            )
            cumulative_negative.extend(
                {"checkpoint": checkpoint_id, **row} for row in restored["negative"]
            )
            cumulative_run_health.extend(
                {"checkpoint": checkpoint_id, **row}
                for row in restored["run_health"]
            )
            checkpoint_summaries.append(restored["summary"])
            if checkpoint_index == 0:
                runtime_gate_path = checkpoint_root / "runtime_utilization_gate.json"
                gate = json.loads(runtime_gate_path.read_text(encoding="utf-8-sig"))
                runtime_gate_status = str(gate.get("status") or "NOT_EVALUATED")
            continue
        if (
            (
                bool(profile["evaluated_fill_required"])
                and completed_pairs
                >= int(profile["minimum_actual_evaluated_pairs"])
            )
            or (
                not bool(profile["evaluated_fill_required"])
                and completed_pairs
                >= int(profile["maximum_completed_development_matched_pairs"])
            )
            or raw_attempts >= int(profile["maximum_raw_attempts"])
            or time.time() >= deadline_epoch
        ):
            break
        checkpoint_root.mkdir(parents=True, exist_ok=True)
        checkpoint_runtime_gate_path: Path | None = None
        if profile["evaluated_fill_required"]:
            admitted_pair_target = _next_target_admission_count(
                evaluated_target=int(profile["minimum_actual_evaluated_pairs"]),
                completed_evaluated=completed_pairs,
                completed_admitted=completed_admitted_pairs,
                maximum_per_checkpoint=int(
                    profile["checkpoint_scheduled_pairs"]
                ),
            )
            schedule = [
                {
                    "route_id": str(profile["target_route"]),
                    "scheduled_pairs": admitted_pair_target,
                    "generation_mode": "fresh",
                    "top_level_scheduling_key": "unified_registry_route_id",
                }
            ]
            schedule_summary = {
                "campaign_profile": str(profile["name"]),
                "schedule_authority": "UNIFIED_REGISTRY_ROUTE_ID",
                "scheduled_admission_target": admitted_pair_target,
                "cumulative_pair_evaluated_before_checkpoint": completed_pairs,
                "campaign_pair_evaluated_target": int(
                    profile["minimum_actual_evaluated_pairs"]
                ),
            }
        else:
            schedule, schedule_summary = build_medium_campaign_schedule(
                previous_feedback,
                base_targets=profile["base_targets"][checkpoint_index],
                total_pairs=int(profile["checkpoint_scheduled_pairs"]),
            )
        for row in schedule:
            row["checkpoint"] = checkpoint_id
        schedule_path = _write_parquet(checkpoint_root / "schedule.parquet", schedule)
        schedule_summary_path = _write_json(checkpoint_root / "schedule_summary.json", schedule_summary)
        generated: list[dict[str, Any]] = []
        probe_rows: list[dict[str, Any]] = []
        decisions: list[dict[str, Any]] = []
        admitted: list[dict[str, Any]] = []
        funnels = []
        probe_audits: list[dict[str, Any]] = []
        if profile["evaluated_fill_required"]:
            schedule_row = schedule[0]
            route_id = str(schedule_row["route_id"])
            spec = seed_rows[(checkpoint_id, route_id)]
            probe_binding = _stable_hash(
                {
                    "checkpoint": checkpoint_id,
                    "seed_manifest": _sha256(seed_manifest_path),
                    "split": split.manifest_hash,
                    "schema": _sha256(schema_binding_path),
                    "probe_trade_times": 30,
                }
            )
            generated, probe_rows, decisions, admitted, target_pack = (
                _generate_behavior_unique_target_pack(
                    generator=generator,
                    registry=registry,
                    route_id=route_id,
                    admitted_pair_target=int(schedule_row["scheduled_pairs"]),
                    seed=int(spec["seed"]),
                    attempt_start=int(spec["attempt_start"]),
                    attempt_limit=int(spec["raw_attempt_cap"]),
                    historical_exact=historical_exact,
                    historical_behavior_archive=behavior_archive,
                    available_field_ids=schema_by_backend[
                        _clock_for_route(route_id)
                    ],
                    field_roots=field_roots,
                    train_dates=_train_dates(split),
                    coordinate_binding=probe_binding,
                    checkpoint_id=checkpoint_id,
                    compute_threads=compute_threads,
                )
            )
            funnels.append(
                {"checkpoint": checkpoint_id, **target_pack["funnel"]}
            )
            probe_audits.extend(target_pack["probe_audits"])
            raw_attempts += int(target_pack["funnel"]["generation_attempts"])
        else:
            for schedule_row in schedule:
                route_id = str(schedule_row["route_id"])
                spec = seed_rows[(checkpoint_id, route_id)]
                rows, funnel = generator.generate_route_attempts(
                    route_id,
                    scheduled_pairs=int(schedule_row["scheduled_pairs"]),
                    seed=int(spec["seed"]),
                    attempt_start=int(spec["attempt_start"]),
                    attempt_limit=int(spec["raw_attempt_cap"]),
                    existing_exact_identities=set(historical_exact),
                    available_field_ids=schema_by_backend[
                        _clock_for_route(route_id)
                    ],
                )
                generated.extend(rows)
                funnels.append({"checkpoint": checkpoint_id, **funnel})
                raw_attempts += int(funnel["generation_attempts"])
            generated = _annotate_generation_metadata(generated, registry)
        candidate_attempt_path = _write_parquet(checkpoint_root / "candidate_attempts.parquet", generated)
        funnel_path = _write_parquet(checkpoint_root / "route_funnel.parquet", funnels)
        probe_audit_path: Path | None = None
        if profile["evaluated_fill_required"]:
            probe_audit_path = _write_json(
                checkpoint_root / "behavior_probe_audit.json",
                {
                    "schema_version": "cn_behavior_probe_refill_audit_v1",
                    "campaign_profile": str(profile["name"]),
                    "rows": probe_audits,
                },
            )
        else:
            probe_binding = _stable_hash({"checkpoint": checkpoint_id, "seed_manifest": _sha256(seed_manifest_path), "split": split.manifest_hash, "schema": _sha256(schema_binding_path), "probe_trade_times": 30})
            probe_rows, probe_audits = _probe_pack(
                candidate_rows=generated,
                field_roots=field_roots,
                train_dates=_train_dates(split),
                coordinate_binding=probe_binding,
                batch_id=checkpoint_id,
                compute_threads=compute_threads,
            )
            admitted, decisions = _admit_behavior_unique(candidate_rows=generated, probe_rows=probe_rows, historical_archive=PortfolioBehaviorArchive(behavior_archive.rows))
        probe_path = _write_parquet(checkpoint_root / "behavior_probe.parquet", probe_rows)
        decisions_path = _write_parquet(checkpoint_root / "admission_decisions.parquet", decisions)
        if not admitted:
            raise RuntimeError(f"{checkpoint_id}: no behavior-unique matched pairs")
        binding_path, table_paths = _context_and_binding(
            batch_root=checkpoint_root,
            candidates=admitted,
            registry=registry,
            split=split,
            data_release_hash=_sha256(args.sidecar_closure.resolve()),
        )
        _bind_purity(binding_path, purity_path)
        access_receipts = _run_phase3cm_monitored(
            checkpoint_id=checkpoint_id,
            checkpoint_root=checkpoint_root,
            binding_path=binding_path,
            table_paths=table_paths,
            split_manifest=args.split_manifest.resolve(),
            field_roots=field_roots,
            label_roots=label_roots,
            purity_path=purity_path,
            compute_threads=compute_threads,
            deadline_epoch=deadline_epoch,
        )
        if checkpoint_index == 0:
            expected_backends = tuple(
                backend
                for backend in ("active_bar", "stock_session")
                if backend in table_paths
            )
            gate = _runtime_gate(
                checkpoint_root,
                compute_threads,
                expected_backends=expected_backends,
            )
            if gate["status"] == "RUNTIME_ACCELERATION_GATE_FAILED":
                gate, adjustment_receipts = _bounded_runtime_adjustment(
                    initial_gate=gate,
                    checkpoint_id=checkpoint_id,
                    checkpoint_root=checkpoint_root,
                    binding_path=binding_path,
                    table_paths=table_paths,
                    split_manifest=args.split_manifest.resolve(),
                    field_roots=field_roots,
                    label_roots=label_roots,
                    purity_path=purity_path,
                    compute_threads=compute_threads,
                    deadline_epoch=deadline_epoch,
                )
                access_receipts.extend(adjustment_receipts)
            runtime_gate_path = _write_json(output_root / "runtime_utilization_gate.json", gate)
            checkpoint_runtime_gate_path = _write_json(
                checkpoint_root / "runtime_utilization_gate.json", gate
            )
            runtime_gate_status = str(gate["status"])
            if gate["status"] not in {"PASS", "PASS_WITH_RUN_HEALTH_FAILURE"}:
                raise RuntimeError(str(gate["status"]))
        outcomes, full_behavior = _outcome_rows(checkpoint_root)
        full_behavior = _join_full_behavior_identities(full_behavior, probe_rows)
        known_behavior_families = {
            str(row.get("portfolio_behavior_family_id") or "")
            for row in behavior_archive.rows
            if str(row.get("portfolio_behavior_family_id") or "")
        }
        ledger, positive, negative, run_health = build_iterative_feedback_views(outcomes)
        funnels = _close_route_funnels(
            funnels=funnels,
            decisions=decisions,
            outcomes=outcomes,
        )
        funnel_path = _write_parquet(checkpoint_root / "route_funnel.parquet", funnels)
        health = _route_health(outcomes=outcomes, ledger=ledger, positive=positive, negative=negative, admission_rows=decisions, full_behavior_rows=full_behavior)
        previous_feedback = health
        for row in generated:
            if row.get("exact_identity"):
                historical_exact.add(str(row["exact_identity"]))
        _add_resolved_behavior_rows(behavior_archive, probe_rows)
        _add_resolved_behavior_rows(behavior_archive, full_behavior)
        evaluated_pairs = sum(
            str(row.get("pair_evaluation_status") or "") == "PAIR_EVALUATED"
            for row in outcomes
        )
        completed_pairs += (
            evaluated_pairs
            if profile["evaluated_fill_required"]
            else len(outcomes)
        )
        completed_admitted_pairs += len(admitted) // 2
        cumulative_candidates.extend({"checkpoint": checkpoint_id, **row} for row in admitted)
        cumulative_outcomes.extend({"checkpoint": checkpoint_id, **row} for row in outcomes)
        cumulative_ledger.extend({"checkpoint": checkpoint_id, **row} for row in ledger)
        cumulative_positive.extend({"checkpoint": checkpoint_id, **row} for row in positive)
        cumulative_negative.extend({"checkpoint": checkpoint_id, **row} for row in negative)
        cumulative_run_health.extend({"checkpoint": checkpoint_id, **row} for row in run_health)
        metric_rows = _metrics_rows(
            checkpoint_id=checkpoint_id,
            schedule=schedule,
            funnel=funnels,
            admitted=admitted,
            outcomes=outcomes,
            full_behavior=full_behavior,
            decisions=decisions,
            negative=negative,
            checkpoint_root=checkpoint_root,
            known_behavior_families=known_behavior_families,
        )
        cumulative_metrics.extend(metric_rows)
        health_path = _write_parquet(checkpoint_root / "route_health.parquet", health)
        outcome_path = _write_parquet(checkpoint_root / "observation_ledger.parquet", outcomes)
        full_behavior_path = _write_parquet(checkpoint_root / "full_behavior.parquet", full_behavior)
        archive_path = checkpoint_root / "behavior_archive.parquet"
        behavior_archive.write_parquet(archive_path)
        metrics_path = _write_parquet(checkpoint_root / "campaign_metrics.parquet", metric_rows)
        input_hashes = {
            "frozen_contract": _sha256(frozen_contract_path),
            "seed_attempt_manifest": _sha256(seed_manifest_path),
            "prior_checkpoint_manifest": _sha256(previous_manifest) if previous_manifest else "GENESIS",
            "adaptive_schedule_source": _sha256(previous_manifest) if previous_manifest else "FROZEN_INITIAL_PRIOR",
        }
        previous_manifest = _batch_manifest(
            batch_root=checkpoint_root,
            batch_id=checkpoint_id,
            input_hashes=input_hashes,
            paths=[schedule_path, schedule_summary_path, candidate_attempt_path, funnel_path, probe_path, decisions_path, binding_path, health_path, outcome_path, full_behavior_path, archive_path, metrics_path, *([probe_audit_path] if probe_audit_path else []), *([checkpoint_runtime_gate_path] if checkpoint_runtime_gate_path else []), *[Path(str(row["result_path"])) for row in access_receipts]],
            access_receipts=access_receipts,
        )
        checkpoint_summaries.append({"checkpoint": checkpoint_id, "scheduled_pairs": sum(int(row["scheduled_pairs"]) for row in schedule), "generated_pairs": len(generated) // 2, "admitted_pairs": len(admitted) // 2, "pair_result_count": len(outcomes), "evaluated_pairs": evaluated_pairs, "cumulative_evaluated_pairs": completed_pairs, "raw_attempts": sum(int(row["generation_attempts"]) for row in funnels), "positive_matched_increments": sum(float(row.get("matched_net_increment") or 0.0) > 0 for row in outcomes), "manifest_sha256": _sha256(previous_manifest)})

    candidate_ledger_path = _write_parquet(output_root / "candidate_ledger.parquet", cumulative_candidates)
    observation_ledger_path = _write_parquet(output_root / "observation_ledger.parquet", cumulative_outcomes)
    behavior_archive_path = output_root / "behavior_archive.parquet"
    behavior_archive.write_parquet(behavior_archive_path)
    metrics_path = _write_parquet(output_root / "campaign_metrics.parquet", cumulative_metrics)
    checkpoint_elites_path = _write_parquet(
        output_root / "checkpoint_elites.parquet",
        _checkpoint_elites(
            candidates=cumulative_candidates,
            outcomes=cumulative_outcomes,
        ),
    )
    _write_parquet(output_root / "positive_policy_view.parquet", cumulative_positive)
    _write_parquet(output_root / "negative_scheduler_view.parquet", cumulative_negative)
    _write_parquet(output_root / "run_health.parquet", cumulative_run_health)
    temporal_status = _productivity_status(cumulative_metrics, {"FIRSTN_PATH", "SLOW_TEMPORAL_CHANGE", "DISCLOSURE_EVENT", "MARKET_REGIME_CONDITION", "INTRADAY_STATE_TRANSITION"})
    cross_status = _productivity_status(cumulative_metrics, {"MINUTE_STATIC", "SLOW_CROSS_SECTIONAL_LEVEL"})
    materialization_missing = sum(int(row.get("materialization_missing_field_pairs") or 0) for path in (output_root / "checkpoints").glob("checkpoint_*/route_funnel.parquet") for row in pd.read_parquet(path).to_dict(orient="records"))
    field_status = "PASS" if materialization_missing == 0 else "PASS_WITH_LOCAL_BOTTLENECKS"
    evaluated_target_met = (
        completed_pairs >= int(profile["minimum_actual_evaluated_pairs"])
        if profile["evaluated_fill_required"]
        else True
    )
    qualified = (
        bool(checkpoint_summaries)
        and purity["status"] == "PASS"
        and runtime_gate_path is not None
        and evaluated_target_met
    )
    if not qualified:
        next_step = "INVALID"
    elif temporal_status == cross_status == "NOT_QUALIFIED":
        next_step = "PIVOT_INFORMATION_OR_MECHANISM"
    elif "NOT_QUALIFIED" in {temporal_status, cross_status}:
        next_step = "TARGETED_ROUTE_REPAIR_THEN_REPEAT"
    else:
        next_step = "KEEP_MEDIUM_SCALE_AND_ADD_SKELETON_POLICY"
    train_manifest_path = _write_json(
        output_root / "train_complete_manifest.json",
        {
            "schema_version": "cn_targeted_search_train_complete_v1",
            "status": "TRAIN_COMPLETE" if qualified else "TRAIN_INVALID",
            "campaign_id": campaign_id,
            "campaign_profile": str(profile["name"]),
            "completed_development_matched_pairs": completed_pairs,
            "budget_counting_unit": (
                "PAIR_EVALUATED"
                if profile["evaluated_fill_required"]
                else "FULL_COORDINATE_PAIR_RESULT"
            ),
            "minimum_actual_evaluated_pairs": int(
                profile["minimum_actual_evaluated_pairs"]
            ),
            "candidate_ledger": _artifact(candidate_ledger_path, root=output_root),
            "observation_ledger": _artifact(observation_ledger_path, root=output_root),
            "behavior_archive": _artifact(behavior_archive_path, root=output_root),
            "campaign_metrics": _artifact(metrics_path, root=output_root),
            "checkpoint_elites": _artifact(
                checkpoint_elites_path, root=output_root
            ),
            "validation_trigger": "AUTOMATIC_AFTER_IMMUTABLE_TRAIN_COMPLETE",
            "runtime_gate_status": runtime_gate_status,
            "run_health_status": (
                "PASS"
                if runtime_gate_status == "PASS"
                else "INFRASTRUCTURE_FAILURE_PRESERVED"
            ),
            "promotion": "FORBIDDEN",
        },
    )
    validation_receipt = None
    if qualified:
        validation_receipt = _run_automatic_validation_after_train(
            train_manifest_path=train_manifest_path,
            protected_train_artifacts=(
                candidate_ledger_path,
                observation_ledger_path,
                behavior_archive_path,
                metrics_path,
                checkpoint_elites_path,
            ),
            validation_root=output_root / "post_train_validation",
            candidates=cumulative_candidates,
            registry=registry,
            split=split,
            validation_data_release_hash=_sha256(
                args.validation_sidecar_closure.resolve()
            ),
            split_manifest=args.split_manifest.resolve(),
            validation_field_roots={
                "active_bar": args.validation_active_field_root.resolve(),
                "stock_session": args.validation_session_field_root.resolve(),
            },
            validation_label_roots={
                "active_bar": args.validation_active_label_root.resolve(),
                "stock_session": args.validation_session_label_root.resolve(),
            },
            compute_threads=compute_threads,
        )
    validation_reads = int(
        ((validation_receipt or {}).get("validation_result") or {}).get(
            "validation_reads", 0
        )
    )
    decision = {
        "status": (
            "CAMPAIGN_CLOSED"
            if qualified and runtime_gate_status == "PASS"
            else "CAMPAIGN_CLOSED_WITH_RUN_HEALTH_FAILURE"
            if qualified
            else "RUN_INVALID"
        ),
        "campaign_profile": str(profile["name"]),
        "stop_reason": (
            "ACTUAL_EVALUATED_TARGET_REACHED"
            if profile["evaluated_fill_required"] and evaluated_target_met
            else "CHECKPOINT_LIMIT"
            if len(checkpoint_summaries) == int(profile["checkpoint_count"])
            else "HARD_CAP_OR_GATE"
        ),
        "total_scheduled_matched_pair_budget": int(
            profile["total_scheduled_matched_pair_budget"]
        ),
        "maximum_completed_development_matched_pairs": int(
            profile["maximum_completed_development_matched_pairs"]
        ),
        "minimum_actual_evaluated_pairs": int(
            profile["minimum_actual_evaluated_pairs"]
        ),
        "actual_evaluated_target_met": evaluated_target_met,
        "budget_counting_unit": (
            "PAIR_EVALUATED"
            if profile["evaluated_fill_required"]
            else "FULL_COORDINATE_PAIR_RESULT"
        ),
        "completed_development_matched_pairs": completed_pairs,
        "raw_generation_attempts": raw_attempts,
        "checkpoint_summaries": checkpoint_summaries,
        "FIELD_MATERIALIZATION_FUNNEL": field_status,
        "SPLIT_BOUNDARY_LABEL_PURITY": purity["status"],
        "CAMPAIGN_LOCAL_TEMPORAL_EVENT_ROUTE_PRODUCTIVITY": temporal_status,
        "CAMPAIGN_LOCAL_CROSS_SECTIONAL_ROUTE_PRODUCTIVITY": cross_status,
        "REGISTRY_COMPOSITIONAL_V2": "CAMPAIGN_QUALIFIED" if qualified else "CAMPAIGN_NOT_QUALIFIED",
        "CN_SEARCH_NEXT_STEP": next_step,
        "all_route_conclusions": "CAMPAIGN_LOCAL",
        "validation_reads": validation_reads,
        "validation_status": (
            "AUTOMATIC_POST_TRAIN_VALIDATION_COMPLETE"
            if validation_receipt
            else "NOT_RUN_TRAIN_INVALID"
        ),
        "validation_usage": "report_only",
        "validation_feedback": "FORBIDDEN",
        "runtime_gate_status": runtime_gate_status,
        "run_health_scope": "INFRASTRUCTURE_ONLY_NOT_ROUTE_HEALTH",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    decision_path = _write_json(output_root / "final_decision.json", decision)
    report_path = (
        args.report_path.resolve()
        if args.report_path is not None
        else output_root / "campaign_report.md"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        f"# {campaign_id}\n\n"
        f"- Status: `{decision['status']}`\n"
        f"- Full-coordinate development matched pairs: `{completed_pairs}` / cap `{int(profile['maximum_completed_development_matched_pairs'])}`\n"
        f"- Raw attempts: `{raw_attempts}` / cap `{int(profile['maximum_raw_attempts'])}`\n"
        f"- Field materialization funnel: `{field_status}`\n"
        f"- Split-boundary label purity: `{purity['status']}`\n"
        f"- Temporal/event productivity (CAMPAIGN_LOCAL): `{temporal_status}`\n"
        f"- Cross-sectional productivity (CAMPAIGN_LOCAL): `{cross_status}`\n"
        f"- Next step: `{next_step}`\n\n"
        f"Post-train validation: `{decision['validation_status']}` (report-only; no feedback). "
        "Holdout, forward-2026, promotion, and Formal Strict Stage A remained forbidden and unread.\n",
        encoding="utf-8",
    )
    run_manifest = {
        "status": decision["status"],
        "campaign_id": campaign_id,
        "host": platform.node(),
        "python": sys.executable,
        "artifacts": [_artifact(path, root=output_root) for path in (campaign_authorization_path, frozen_contract_path, registry_binding_path, discovery_authority_path, schema_binding_path, purity_path, seed_manifest_path, runtime_envelope_path, comparison_path, archive_snapshot_path, *([target_supply_preflight_path] if target_supply_preflight_path else []), candidate_ledger_path, observation_ledger_path, behavior_archive_path, metrics_path, checkpoint_elites_path, train_manifest_path, decision_path, output_root / "post_train_validation/automatic_post_train_validation_receipt.json") if path.is_file()],
        "report": _artifact(report_path),
        "validation_reads": validation_reads,
        "validation_usage": "report_only",
        "validation_feedback": "FORBIDDEN",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    _write_json(output_root / "run_manifest.json", run_manifest)
    return decision


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign-profile",
        choices=(LEGACY_CAMPAIGN_PROFILE, SLOW_CROSS_SECTIONAL_384_PROFILE),
        default=LEGACY_CAMPAIGN_PROFILE,
    )
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--discovery-contract", type=Path, required=True)
    parser.add_argument("--discovery-authorization", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--sidecar-closure", type=Path, required=True)
    parser.add_argument("--active-field-root", type=Path, required=True)
    parser.add_argument("--active-label-root", type=Path, required=True)
    parser.add_argument("--session-field-root", type=Path, required=True)
    parser.add_argument("--session-label-root", type=Path, required=True)
    parser.add_argument("--validation-sidecar-closure", type=Path, required=True)
    parser.add_argument("--validation-active-field-root", type=Path, required=True)
    parser.add_argument("--validation-active-label-root", type=Path, required=True)
    parser.add_argument("--validation-session-field-root", type=Path, required=True)
    parser.add_argument("--validation-session-label-root", type=Path, required=True)
    parser.add_argument("--historical-candidate-archive", type=Path, required=True)
    parser.add_argument("--historical-behavior-archive", type=Path, required=True)
    parser.add_argument("--historical-archive-manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--report-path", type=Path)
    parser.add_argument("--seed-base", type=int, default=2026072101)
    parser.add_argument("--active-threads", type=int, default=30)
    parser.add_argument("--session-threads", type=int, default=2)
    args = parser.parse_args(argv)
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if str(result["status"]).startswith("CAMPAIGN_CLOSED") else 1


if __name__ == "__main__":
    raise SystemExit(main())
