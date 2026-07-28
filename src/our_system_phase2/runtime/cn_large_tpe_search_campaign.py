"""Train-only five-digit CN search using official Optuna TPE.

This is one campaign runner over existing Registry, typed Grammar, compiler,
matched-control, behavior admission, Phase3CM, archive, and checkpoint
authorities.  It is not a new search platform or promotion path.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import multiprocessing
import pickle
import platform
import statistics
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from itertools import product
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _artifact,
    _batch_manifest,
    _clock_for_route,
    _context_and_binding,
    _join_full_behavior_identities,
    _outcome_rows,
    _probe_pack,
    _run_automatic_validation_after_train,
    _sha256,
    _stable_hash,
    _train_dates,
    _write_json,
    _write_parquet,
)
from our_system_phase2.runtime.cn_search_policy_qualification import (
    _materialize_population,
    _minimum_free_memory,
    _outcome_class,
    _verify_artifacts,
)
from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    _add_resolved_behavior_rows,
    _admit_behavior_unique,
    _bind_purity,
    _load_historical_dedupe,
    _registry_binding,
    _run_phase3cm_monitored,
    _runtime_envelope,
    _runtime_gate,
    materialized_schema_binding,
)
from our_system_phase2.services.compositional_grammar import (
    OPTIMIZER_GENE_SURFACE_VERSION,
)
from our_system_phase2.services.fixed_split_authority import (
    FixedSplitAuthority,
)
from our_system_phase2.services.optuna_tpe_search_adapter import (
    EVALUATED,
    RouteConditionalTPESearchAdapter,
)
from our_system_phase2.services.phase3cm_streaming_expression import (
    unsupported_streaming_operators,
)
from our_system_phase2.services.portfolio_behavior_archive import (
    PortfolioBehaviorArchive,
)
from our_system_phase2.services.route_local_availability import (
    AvailabilityEntry,
    RouteLocalAvailabilityController,
    enumerate_authoritative_entries,
)
from our_system_phase2.services.split_boundary_label_purity import (
    audit_split_boundary_label_purity,
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
CAMPAIGN_PROFILE = "cn_large_optuna_tpe_availability_v3"
ROUTE_EVALUATED_TARGETS = {
    "SLOW_TEMPORAL_CHANGE": 14_000,
    "FIRSTN_PATH": 1_300,
    "SLOW_CROSS_SECTIONAL_LEVEL": 900,
    "MARKET_REGIME_CONDITION": 380,
    "DISCLOSURE_EVENT": 300,
}
ROUTES = tuple(ROUTE_EVALUATED_TARGETS)
MINIMUM_ACTUAL_EVALUATED_PAIRS = sum(ROUTE_EVALUATED_TARGETS.values())
MAXIMUM_CHECKPOINTS = 96
ASKS_PER_CHECKPOINT = 384
MAXIMUM_RAW_ASKS = 36_864
MAXIMUM_INTERNAL_NATIVE_DRAWS_PER_FORMAL = 8
MAXIMUM_WALL_SECONDS = 7 * 24 * 60 * 60
VALIDATION_FINALIST_PAIRS = 256
PAIR_BATCH_SIZES = {"active_bar": 12, "stock_session": 12}
MAXIMUM_CACHE_BYTES = 8 * 1024**3
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
FRESH_EXACT_MARGIN = 1.20
OPTUNA_ROUTE_WORKERS = len(ROUTES)
STARTUP_TRIALS_BY_ROUTE = {
    "SLOW_TEMPORAL_CHANGE": 512,
    "FIRSTN_PATH": 128,
    "SLOW_CROSS_SECTIONAL_LEVEL": 128,
    "MARKET_REGIME_CONDITION": 64,
    "DISCLOSURE_EVENT": 64,
}
N_EI_CANDIDATES = 24
TPE_MULTIVARIATE = False
TPE_GROUP = False
TPE_CONSTANT_LIAR = True
TPE_SAMPLER_MODE = "OFFICIAL_DEFAULT_UNIVARIATE_CONSTANT_LIAR"
SEARCH_SCORE_POLICY = "MIN_PRIMARY_COMPOSITE_AND_MATCHED_INCREMENT_V1"
VALIDATION_PRIMARY_DECISION = "TRAIN_REWARD_FOLLOWUP_READY"


def _finite_float(value: Any) -> float | None:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return None
    return output if math.isfinite(output) else None


def _conservative_search_score(
    outcome: Mapping[str, Any],
) -> float | None:
    if str(outcome.get("pair_evaluation_status") or "") != "PAIR_EVALUATED":
        return None
    primary = _finite_float(outcome.get("primary_composite_reward"))
    if primary is None:
        primary = _finite_float(outcome.get("primary_train_reward"))
    matched = _finite_float(outcome.get("matched_train_increment"))
    if matched is None:
        matched = _finite_float(outcome.get("pair_train_reward"))
    if primary is None or matched is None:
        return None
    return min(primary, matched)


def _validation_eligible(outcome: Mapping[str, Any]) -> bool:
    score = _conservative_search_score(outcome)
    matched = _finite_float(outcome.get("matched_train_increment"))
    if matched is None:
        matched = _finite_float(outcome.get("pair_train_reward"))
    return (
        score is not None
        and matched is not None
        and matched > 0.0
        and str(
            outcome.get("primary_standalone_train_reward_decision") or ""
        )
        == VALIDATION_PRIMARY_DECISION
    )


def _restore_route_adapter_worker(
    route_id: str,
    lane_spaces: Mapping[str, Mapping[str, Any]],
    seed: int,
    transcripts: Sequence[Mapping[str, Any]],
    n_startup_trials: int,
    n_ei_candidates: int,
    multivariate: bool,
    group: bool,
    constant_liar: bool,
) -> RouteConditionalTPESearchAdapter:
    return RouteConditionalTPESearchAdapter.restore_trials(
        route_id=route_id,
        lane_spaces=lane_spaces,
        seed=seed,
        transcripts=transcripts,
        n_startup_trials=n_startup_trials,
        n_ei_candidates=n_ei_candidates,
        multivariate=multivariate,
        group=group,
        constant_liar=constant_liar,
    )


def _restore_route_adapters(
    *,
    lanes_by_route: Mapping[str, Mapping[str, Any]],
    seed_base: int,
    transcripts: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, RouteConditionalTPESearchAdapter]:
    with ProcessPoolExecutor(
        max_workers=min(OPTUNA_ROUTE_WORKERS, len(ROUTES)),
        mp_context=multiprocessing.get_context("spawn"),
    ) as executor:
        futures = {
            route_id: executor.submit(
                _restore_route_adapter_worker,
                route_id,
                lanes_by_route[route_id],
                int(seed_base) + index * 1009,
                transcripts[route_id],
                STARTUP_TRIALS_BY_ROUTE[route_id],
                N_EI_CANDIDATES,
                TPE_MULTIVARIATE,
                TPE_GROUP,
                TPE_CONSTANT_LIAR,
            )
            for index, route_id in enumerate(ROUTES)
        }
        return {
            route_id: futures[route_id].result()
            for route_id in ROUTES
        }


def _ask_route_population_worker(
    adapter: RouteConditionalTPESearchAdapter,
    checkpoint_id: str,
    count: int,
    expected_genes: Sequence[Mapping[str, str]] | None,
) -> tuple[RouteConditionalTPESearchAdapter, list[dict[str, Any]], float]:
    started = time.perf_counter()
    rows = adapter.ask_population(
        checkpoint_id=checkpoint_id,
        count=count,
        expected_genes=expected_genes,
    )
    return adapter, rows, time.perf_counter() - started


def _ask_route_populations(
    *,
    schedule: Sequence[Mapping[str, Any]],
    adapters: dict[str, RouteConditionalTPESearchAdapter],
    checkpoint_id: str,
    expected_by_route: Mapping[str, Sequence[Mapping[str, str]]],
    replay_existing: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    route_specs = [
        (str(route["route_id"]), int(route["asked_pairs"]))
        for route in schedule
    ]
    if not route_specs:
        raise RuntimeError("LARGE_TPE_EMPTY_ROUTE_SCHEDULE")

    started = time.perf_counter()
    with ProcessPoolExecutor(
        max_workers=min(OPTUNA_ROUTE_WORKERS, len(route_specs)),
        mp_context=multiprocessing.get_context("spawn"),
    ) as executor:
        futures = {
            route_id: executor.submit(
                _ask_route_population_worker,
                adapters[route_id],
                checkpoint_id,
                count,
                (
                    expected_by_route.get(route_id)
                    if replay_existing
                    else None
                ),
            )
            for route_id, count in route_specs
        }
        results = {
            route_id: futures[route_id].result()
            for route_id, _ in route_specs
        }
    for route_id, _ in route_specs:
        adapters[route_id] = results[route_id][0]
    wall_seconds = time.perf_counter() - started
    asked = [
        row
        for route_id, _ in route_specs
        for row in results[route_id][1]
    ]
    route_timings = {
        route_id: {
            "asked_pairs": len(results[route_id][1]),
            "wall_seconds": results[route_id][2],
            "asks_per_second": (
                len(results[route_id][1])
                / max(results[route_id][2], 1e-12)
            ),
        }
        for route_id, _ in route_specs
    }
    return asked, {
        "schema_version": "cn_large_tpe_optimizer_ask_runtime_v1",
        "checkpoint": checkpoint_id,
        "execution": "PROCESS_PARALLEL_ROUTE_LOCAL_OPTUNA_STUDIES",
        "route_worker_count": min(
            OPTUNA_ROUTE_WORKERS, len(route_specs)
        ),
        "asked_pairs": len(asked),
        "wall_seconds": wall_seconds,
        "asks_per_second": len(asked) / max(wall_seconds, 1e-12),
        "routes": route_timings,
        "replay_existing": bool(replay_existing),
    }


def _availability_metadata(
    emission: Any,
    *,
    formal_ask_ordinal: int,
    source_optimizer_trial_number: int | None = None,
) -> dict[str, Any]:
    metadata = {
        "formal_fresh_exact_ask": True,
        "formal_ask_ordinal": int(formal_ask_ordinal),
        "availability_emission_mode": str(emission.emission_mode),
        "availability_bucket_key": str(emission.bucket_key),
        "availability_exact_identity": str(emission.exact_identity),
        "availability_source_exact_identity": str(
            emission.source_exact_identity
        ),
    }
    if source_optimizer_trial_number is not None:
        metadata["source_optimizer_trial_number"] = int(
            source_optimizer_trial_number
        )
    return metadata


def _ask_availability_aware_populations(
    *,
    schedule: Sequence[Mapping[str, Any]],
    adapters: dict[str, RouteConditionalTPESearchAdapter],
    controller: RouteLocalAvailabilityController,
    generator: RegistryDrivenGenerator,
    schema_by_backend: Mapping[str, set[str]],
    checkpoint_id: str,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, Any],
]:
    """Emit formal fresh exact asks while retaining every official trial.

    Native draws that hit invalid, seen, or exhausted exact space remain real
    Optuna trials and are told FAIL.  They do not consume the formal ask
    budget.  Fixed replacements are ordinary queued Optuna trials and receive
    financial reward only if the existing evaluator later completes them.
    """

    started = time.perf_counter()
    all_asked: list[dict[str, Any]] = []
    formal_asked: list[dict[str, Any]] = []
    internal_observations: dict[str, dict[str, Any]] = {}
    route_metrics: dict[str, dict[str, Any]] = {}
    global_formal_ordinal = 0
    for route_spec in schedule:
        route_id = str(route_spec["route_id"])
        requested = int(route_spec["asked_pairs"])
        route_started = time.perf_counter()
        native_draws = 0
        fixed_draws = 0
        direct = 0
        replacement = 0
        fallback_count = 0
        unfulfilled = 0
        for route_formal_ordinal in range(requested):
            emitted = False
            last_identity = ""
            for internal_draw_ordinal in range(
                MAXIMUM_INTERNAL_NATIVE_DRAWS_PER_FORMAL
            ):
                native = adapters[route_id].ask_trial(
                    checkpoint_id=checkpoint_id,
                    metadata={
                        "formal_ask_target_ordinal": int(
                            route_formal_ordinal
                        ),
                        "internal_draw_ordinal": int(
                            internal_draw_ordinal
                        ),
                        "availability_trial_role": "NATIVE_DRAW",
                    },
                )
                controller.record_optimizer_draw(route_id)
                native_draws += 1
                materialized = _materialize_population(
                    asked=[native],
                    generator=generator,
                    schema_by_backend=schema_by_backend,
                )[0]
                proposal_id = str(native["proposal_id"])
                legal = (
                    str(materialized.get("construction_status") or "")
                    == "LEGAL"
                )
                last_identity = str(
                    materialized.get("exact_identity") or ""
                )
                if not legal:
                    internal_observations[proposal_id] = {
                        "proposal_id": proposal_id,
                        "outcome_class": "DETERMINISTIC_INVALID",
                        "optimizer_reward": None,
                        "outcome_reason": str(
                            materialized.get("construction_error")
                            or "DETERMINISTIC_INVALID"
                        ),
                    }
                    all_asked.append(native)
                    continue
                emission = controller.accept_direct(
                    route_id=route_id,
                    genes=dict(native["genes"]),
                    exact_identity=last_identity,
                )
                if emission is not None:
                    annotated = adapters[
                        route_id
                    ].annotate_pending_trial(
                        proposal_id,
                        _availability_metadata(
                            emission,
                            formal_ask_ordinal=global_formal_ordinal,
                        ),
                    )
                    all_asked.append(annotated)
                    formal_asked.append(annotated)
                    direct += 1
                    emitted = True
                    break
                replacement_emission = controller.emit_same_bucket(
                    route_id=route_id,
                    genes=dict(native["genes"]),
                    source_exact_identity=last_identity,
                )
                internal_observations[proposal_id] = {
                    "proposal_id": proposal_id,
                    "outcome_class": (
                        "AVAILABILITY_REPLACED"
                        if replacement_emission is not None
                        else "AVAILABILITY_BUCKET_EXHAUSTED"
                    ),
                    "optimizer_reward": None,
                    "outcome_reason": (
                        "EXACT_ALREADY_SEEN"
                        if replacement_emission is not None
                        else "STRUCTURAL_BUCKET_EXHAUSTED"
                    ),
                }
                all_asked.append(native)
                if replacement_emission is None:
                    continue
                fixed = adapters[route_id].enqueue_fixed_trial(
                    checkpoint_id=checkpoint_id,
                    genes=replacement_emission.genes,
                    metadata={
                        **_availability_metadata(
                            replacement_emission,
                            formal_ask_ordinal=global_formal_ordinal,
                            source_optimizer_trial_number=int(
                                native["trial_number"]
                            ),
                        ),
                        "availability_trial_role": "FIXED_REPLACEMENT",
                    },
                )
                controller.record_optimizer_draw(route_id)
                fixed_draws += 1
                all_asked.append(fixed)
                formal_asked.append(fixed)
                replacement += 1
                emitted = True
                break
            if not emitted:
                fallback = controller.emit_global_fallback(
                    route_id=route_id,
                    source_exact_identity=last_identity,
                )
                if fallback is None:
                    unfulfilled += 1
                    break
                fixed = adapters[route_id].enqueue_fixed_trial(
                    checkpoint_id=checkpoint_id,
                    genes=fallback.genes,
                    metadata={
                        **_availability_metadata(
                            fallback,
                            formal_ask_ordinal=global_formal_ordinal,
                        ),
                        "availability_trial_role": "FIXED_FALLBACK",
                    },
                )
                controller.record_optimizer_draw(route_id)
                fixed_draws += 1
                all_asked.append(fixed)
                formal_asked.append(fixed)
                fallback_count += 1
                emitted = True
            if emitted:
                global_formal_ordinal += 1
        route_wall = time.perf_counter() - route_started
        route_metrics[route_id] = {
            "requested_formal_fresh_exact_asks": requested,
            "emitted_formal_fresh_exact_asks": (
                direct + replacement + fallback_count
            ),
            "native_optimizer_draw_attempts": native_draws,
            "fixed_optimizer_draw_attempts": fixed_draws,
            "optimizer_draw_attempts": native_draws + fixed_draws,
            "tpe_direct_fresh": direct,
            "tpe_bucket_replacement": replacement,
            "global_availability_fallback": fallback_count,
            "unfulfilled_formal_requests": unfulfilled,
            "remaining_exact": controller.remaining_count(
                route_id=route_id
            ),
            "wall_seconds": route_wall,
        }
    wall_seconds = time.perf_counter() - started
    runtime = {
        "schema_version": "cn_large_tpe_availability_ask_runtime_v1",
        "checkpoint": checkpoint_id,
        "execution": "ROUTE_LOCAL_AVAILABILITY_AWARE_OFFICIAL_OPTUNA",
        "maximum_internal_native_draws_per_formal": (
            MAXIMUM_INTERNAL_NATIVE_DRAWS_PER_FORMAL
        ),
        "formal_fresh_exact_asks": len(formal_asked),
        "optimizer_draw_attempts": len(all_asked),
        "internal_failed_trials": len(internal_observations),
        "wall_seconds": wall_seconds,
        "routes": route_metrics,
    }
    return all_asked, formal_asked, internal_observations, runtime


def _write_optimizer_snapshot(
    *,
    snapshot_path: Path,
    receipt_path: Path,
    adapters: Mapping[str, RouteConditionalTPESearchAdapter],
    boundary_checkpoint: str,
    prior_manifest_sha256: str,
) -> tuple[Path, Path]:
    if set(adapters) != set(ROUTES):
        raise RuntimeError("LARGE_TPE_OPTIMIZER_SNAPSHOT_ROUTE_DRIFT")
    if any(adapter.has_pending_population for adapter in adapters.values()):
        raise RuntimeError("LARGE_TPE_OPTIMIZER_SNAPSHOT_HAS_PENDING")
    payload = {
        "schema_version": "cn_large_tpe_optimizer_snapshot_v1",
        "boundary_checkpoint": boundary_checkpoint,
        "routes": list(ROUTES),
        "n_ei_candidates": N_EI_CANDIDATES,
        "sampler_mode": TPE_SAMPLER_MODE,
        "adapters": dict(adapters),
    }
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = snapshot_path.with_suffix(snapshot_path.suffix + ".tmp")
    temporary.write_bytes(
        pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    )
    temporary.replace(snapshot_path)
    receipt_path = _write_json(
        receipt_path,
        {
            "schema_version": "cn_large_tpe_optimizer_snapshot_receipt_v1",
            "boundary_checkpoint": boundary_checkpoint,
            "prior_manifest_sha256": prior_manifest_sha256,
            "snapshot_path": str(snapshot_path),
            "snapshot_sha256": _sha256(snapshot_path),
            "routes": list(ROUTES),
            "n_ei_candidates": N_EI_CANDIDATES,
            "sampler_mode": TPE_SAMPLER_MODE,
            "restore_authority": (
                "HASH_BOUND_OPTUNA_STATE_SNAPSHOT_PLUS_IMMUTABLE_TRANSCRIPTS"
            ),
        },
    )
    return snapshot_path, receipt_path


def _load_optimizer_snapshot(
    *,
    snapshot_path: Path,
    receipt_path: Path,
    boundary_checkpoint: str,
    prior_manifest_sha256: str,
) -> dict[str, RouteConditionalTPESearchAdapter] | None:
    if not snapshot_path.is_file() and not receipt_path.is_file():
        return None
    if not snapshot_path.is_file() or not receipt_path.is_file():
        raise RuntimeError("LARGE_TPE_OPTIMIZER_SNAPSHOT_PARTIAL")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8-sig"))
    expected = {
        "boundary_checkpoint": boundary_checkpoint,
        "prior_manifest_sha256": prior_manifest_sha256,
        "snapshot_sha256": _sha256(snapshot_path),
        "routes": list(ROUTES),
        "n_ei_candidates": N_EI_CANDIDATES,
    }
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise RuntimeError("LARGE_TPE_OPTIMIZER_SNAPSHOT_RECEIPT_DRIFT")
    if str(receipt.get("sampler_mode") or "") != TPE_SAMPLER_MODE:
        return None
    payload = pickle.loads(snapshot_path.read_bytes())
    if (
        str(payload.get("schema_version") or "")
        != "cn_large_tpe_optimizer_snapshot_v1"
        or str(payload.get("boundary_checkpoint") or "")
        != boundary_checkpoint
        or list(payload.get("routes") or ()) != list(ROUTES)
        or int(payload.get("n_ei_candidates") or 0)
        != N_EI_CANDIDATES
        or str(payload.get("sampler_mode") or "") != TPE_SAMPLER_MODE
    ):
        raise RuntimeError("LARGE_TPE_OPTIMIZER_SNAPSHOT_PAYLOAD_DRIFT")
    adapters = dict(payload.get("adapters") or {})
    if set(adapters) != set(ROUTES):
        raise RuntimeError("LARGE_TPE_OPTIMIZER_SNAPSHOT_ROUTE_DRIFT")
    for route_id, adapter in adapters.items():
        environment = adapter.environment_receipt()
        if (
            str(environment.get("route_id") or "") != route_id
            or int(environment.get("n_ei_candidates") or 0)
            != N_EI_CANDIDATES
            or bool(environment.get("multivariate"))
            != TPE_MULTIVARIATE
            or bool(environment.get("group")) != TPE_GROUP
            or bool(environment.get("constant_liar"))
            != TPE_CONSTANT_LIAR
            or adapter.has_pending_population
        ):
            raise RuntimeError(
                "LARGE_TPE_OPTIMIZER_SNAPSHOT_ADAPTER_DRIFT"
            )
    return adapters


def _restore_or_import_adapters(
    *,
    output_root: Path,
    lanes_by_route: Mapping[str, Mapping[str, Any]],
    seed_base: int,
    transcripts: Mapping[str, Sequence[Mapping[str, Any]]],
    checkpoint_count: int,
    prior_manifest: Path | None,
) -> dict[str, RouteConditionalTPESearchAdapter]:
    boundary = (
        f"checkpoint_{checkpoint_count:03d}"
        if checkpoint_count
        else "GENESIS"
    )
    current_manifest_hash = (
        _sha256(prior_manifest) if prior_manifest else "GENESIS"
    )
    if checkpoint_count:
        manifest = json.loads(
            prior_manifest.read_text(encoding="utf-8-sig")
        )
        checkpoint_prior_hash = str(
            (manifest.get("input_hashes") or {}).get(
                "prior_checkpoint_manifest"
            )
            or ""
        )
        checkpoint_root = (
            output_root / "checkpoints" / f"checkpoint_{checkpoint_count:03d}"
        )
        adapters = _load_optimizer_snapshot(
            snapshot_path=checkpoint_root / "optimizer_state.pkl",
            receipt_path=checkpoint_root / "optimizer_state_receipt.json",
            boundary_checkpoint=boundary,
            prior_manifest_sha256=checkpoint_prior_hash,
        )
        if adapters is not None:
            return adapters
    recovery_root = output_root / "optimizer_recovery"
    mode_slug = TPE_SAMPLER_MODE.lower()
    recovery_snapshot = recovery_root / (
        f"{boundary}_{mode_slug}_post_tell.pkl"
    )
    recovery_receipt = recovery_root / (
        f"{boundary}_{mode_slug}_post_tell_receipt.json"
    )
    adapters = _load_optimizer_snapshot(
        snapshot_path=recovery_snapshot,
        receipt_path=recovery_receipt,
        boundary_checkpoint=boundary,
        prior_manifest_sha256=current_manifest_hash,
    )
    if adapters is not None:
        return adapters
    adapters = _restore_route_adapters(
        lanes_by_route=lanes_by_route,
        seed_base=seed_base,
        transcripts=transcripts,
    )
    _write_optimizer_snapshot(
        snapshot_path=recovery_snapshot,
        receipt_path=recovery_receipt,
        adapters=adapters,
        boundary_checkpoint=boundary,
        prior_manifest_sha256=current_manifest_hash,
    )
    return adapters


def _copy_behavior_archive(
    archive: PortfolioBehaviorArchive,
) -> PortfolioBehaviorArchive:
    return PortfolioBehaviorArchive([dict(row) for row in archive.rows])


def _authorization_binding(
    *,
    path: Path,
    candidate_archive: Path,
    behavior_archive: Path,
    history_manifest: Path,
    seed_base: int,
    active_threads: int,
    session_threads: int,
) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    expected = {
        "execution_authorized": True,
        "authorized_host": AUTHORIZED_HOST,
        "campaign_profile": CAMPAIGN_PROFILE,
        "optimizer": (
            "official_optuna.samplers.TPESampler_conditional_typed_grammar"
        ),
        "optimizer_package_version": "4.8.0",
        "routes": list(ROUTES),
        "route_actual_evaluated_targets": ROUTE_EVALUATED_TARGETS,
        "minimum_actual_evaluated_pairs": MINIMUM_ACTUAL_EVALUATED_PAIRS,
        "maximum_checkpoints": MAXIMUM_CHECKPOINTS,
        "asks_per_checkpoint": ASKS_PER_CHECKPOINT,
        "maximum_raw_asks": MAXIMUM_RAW_ASKS,
        "budget_counting_unit": "FORMAL_FRESH_EXACT_ASK",
        "maximum_internal_native_draws_per_formal": (
            MAXIMUM_INTERNAL_NATIVE_DRAWS_PER_FORMAL
        ),
        "maximum_wall_seconds": MAXIMUM_WALL_SECONDS,
        "validation_finalist_pairs": VALIDATION_FINALIST_PAIRS,
        "active_threads": int(active_threads),
        "session_threads": int(session_threads),
        "active_pair_batch_size": PAIR_BATCH_SIZES["active_bar"],
        "session_pair_batch_size": PAIR_BATCH_SIZES["stock_session"],
        "optuna_route_workers": OPTUNA_ROUTE_WORKERS,
        "optimizer_ask_execution": (
            "ROUTE_LOCAL_AVAILABILITY_AWARE_OFFICIAL_OPTUNA"
        ),
        "optimizer_n_ei_candidates": N_EI_CANDIDATES,
        "optimizer_sampler_mode": TPE_SAMPLER_MODE,
        "optimizer_multivariate": TPE_MULTIVARIATE,
        "optimizer_group": TPE_GROUP,
        "optimizer_constant_liar": TPE_CONSTANT_LIAR,
        "optimizer_restore_authority": (
            "HASH_BOUND_OPTUNA_STATE_SNAPSHOT_PLUS_IMMUTABLE_TRANSCRIPTS"
        ),
        "optimizer_search_score_policy": SEARCH_SCORE_POLICY,
        "optimizer_gene_surface_version": OPTIMIZER_GENE_SURFACE_VERSION,
        "validation_primary_decision": VALIDATION_PRIMARY_DECISION,
        "cache_cap_bytes": MAXIMUM_CACHE_BYTES,
        "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "seed_base": int(seed_base),
        "portfolio_mode": "LONG_ONLY_TOP",
        "shorting": "FORBIDDEN",
        "one_way_cost_bps": 5,
        "horizons_minutes": [1, 5, 15, 30],
        "validation": "AUTOMATIC_POST_TRAIN_REPORT_ONLY",
        "holdout": "SEALED",
        "forward_2026": "SEALED",
        "promotion": "FORBIDDEN",
    }
    drift = [
        key for key, value in expected.items() if payload.get(key) != value
    ]
    manifest = json.loads(history_manifest.read_text(encoding="utf-8-sig"))
    if (
        str(manifest.get("status") or "") != "PASS"
        or str(
            (manifest.get("candidate_exact_archive") or {}).get("sha256")
            or ""
        ).lower()
        != _sha256(candidate_archive).lower()
        or str(
            (manifest.get("behavior_archive") or {}).get("sha256")
            or ""
        ).lower()
        != _sha256(behavior_archive).lower()
    ):
        drift.append("historical_identity_snapshot")
    if (
        str(payload.get("historical_candidate_archive_sha256") or "").lower()
        != _sha256(candidate_archive).lower()
        or str(payload.get("historical_behavior_archive_sha256") or "").lower()
        != _sha256(behavior_archive).lower()
        or str(payload.get("historical_manifest_sha256") or "").lower()
        != _sha256(history_manifest).lower()
    ):
        drift.append("historical_snapshot_authorization_hashes")
    if drift:
        raise RuntimeError(
            "LARGE_TPE_CAMPAIGN_AUTHORITY_MISMATCH:"
            + ",".join(sorted(set(drift)))
        )
    return {
        "schema_version": "cn_large_tpe_campaign_authority_binding_v3",
        "status": "FIVE_DIGIT_TRAIN_SEARCH_AUTHORIZED",
        "authorization": _artifact(path),
        "historical_candidate_archive": _artifact(candidate_archive),
        "historical_behavior_archive": _artifact(behavior_archive),
        "historical_archive_manifest": _artifact(history_manifest),
        "frozen": expected,
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
            leaves = (
                expression_fields(materialization)
                if materialization
                else {field.field_id}
            )
            if set(map(str, leaves)).issubset(available):
                usable.append(field.field_id)
        if not usable:
            raise RuntimeError(
                f"MATERIALIZED_ROUTE_ROOTS_EMPTY:{route_id}"
            )
        output[route_id] = tuple(sorted(set(usable)))
    return output


def _freeze_gene_lanes(
    *,
    output_root: Path,
    generator: RegistryDrivenGenerator,
    input_hashes: Mapping[str, str],
) -> tuple[dict[str, dict[str, Any]], Path]:
    payload_path = output_root / "categorical_gene_lanes.json"
    manifest_path = output_root / "categorical_gene_lanes_manifest.json"
    if manifest_path.is_file():
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8-sig")
        )
        _verify_artifacts(output_root, manifest)
        payload = json.loads(
            payload_path.read_text(encoding="utf-8-sig")
        )
        projected_payload = {
            "schema_version": "cn_large_tpe_gene_lanes_v1",
            "routes": {
                route_id: generator.categorical_gene_lanes(route_id)
                for route_id in ROUTES
            },
        }
        if projected_payload != payload:
            raise RuntimeError("LARGE_TPE_GENE_LANE_INPUT_DRIFT")
        # The frozen JSON is the immutable content authority, but _write_json
        # sorts mapping keys.  Optuna consumes categorical slots in mapping
        # order, so resume with the structurally equal Registry projection to
        # preserve the original generator order used before the first write.
        payload = projected_payload
    else:
        payload = {
            "schema_version": "cn_large_tpe_gene_lanes_v1",
            "routes": {
                route_id: generator.categorical_gene_lanes(route_id)
                for route_id in ROUTES
            },
        }
        _write_json(payload_path, payload)
        manifest = {
            "schema_version": "cn_large_tpe_gene_lane_manifest_v1",
            "status": "FROZEN_IMMUTABLE",
            "input_hashes": dict(input_hashes),
            "artifacts": [_artifact(payload_path, root=output_root)],
            "financial_reads": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }
        manifest["manifest_payload_hash"] = _stable_hash(manifest)
        _write_json(manifest_path, manifest)
    lanes = {
        route_id: dict(payload["routes"][route_id]["lanes"])
        for route_id in ROUTES
    }
    if any(not values for values in lanes.values()):
        raise RuntimeError("LARGE_TPE_ROUTE_HAS_NO_GENE_LANES")
    return lanes, manifest_path


def _fresh_exact_supply(
    *,
    generator: RegistryDrivenGenerator,
    lanes_by_route: Mapping[str, Mapping[str, Any]],
    historical_exact: set[str],
) -> dict[str, Any]:
    route_rows = {}
    for route_id in ROUTES:
        scheduled = 0
        exact: set[str] = set()
        for space in lanes_by_route[route_id].values():
            categories = dict(space["ordered_categories_by_slot"])
            slots = tuple(categories)
            for values in product(
                *(categories[slot] for slot in slots)
            ):
                scheduled += 1
                pair = generator.propose_categorical_genes(
                    route_id,
                    genes=dict(zip(slots, map(str, values))),
                )
                if (
                    pair.candidate.get("legal")
                    and pair.control.get("legal")
                ):
                    exact.add(str(pair.candidate["exact_identity"]))
        fresh = exact - historical_exact
        required = int(
            math.ceil(
                ROUTE_EVALUATED_TARGETS[route_id]
                * FRESH_EXACT_MARGIN
            )
        )
        route_rows[route_id] = {
            "categorical_points": scheduled,
            "exact_unique": len(exact),
            "historical_overlap": len(exact & historical_exact),
            "fresh_exact": len(fresh),
            "minimum_required_fresh_exact": required,
            "fresh_exact_digest": _stable_hash(sorted(fresh)),
            "status": "PASS" if len(fresh) >= required else "FAIL",
        }
    return {
        "schema_version": "cn_large_tpe_fresh_exact_supply_preflight_v1",
        "route_rows": route_rows,
        "total_fresh_exact": sum(
            int(row["fresh_exact"]) for row in route_rows.values()
        ),
        "minimum_actual_evaluated_pairs": MINIMUM_ACTUAL_EVALUATED_PAIRS,
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "status": (
            "PASS"
            if all(row["status"] == "PASS" for row in route_rows.values())
            else "FAIL"
        ),
    }


def _freeze_availability_index(
    *,
    output_root: Path,
    generator: RegistryDrivenGenerator,
    lanes_by_route: Mapping[str, Mapping[str, Any]],
    input_hashes: Mapping[str, str],
) -> tuple[list[AvailabilityEntry], Path]:
    path = output_root / "route_local_availability_index.json"
    normalized_hashes = {
        str(key): str(value) for key, value in input_hashes.items()
    }
    if path.is_file():
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        unhashed = {
            key: copy.deepcopy(value)
            for key, value in payload.items()
            if key != "payload_hash"
        }
        if (
            str(payload.get("schema_version") or "")
            != "cn_route_local_availability_index_binding_v1"
            or dict(payload.get("input_hashes") or {})
            != normalized_hashes
            or _stable_hash(unhashed)
            != str(payload.get("payload_hash") or "")
        ):
            raise RuntimeError("LARGE_TPE_AVAILABILITY_INDEX_DRIFT")
        entries = [
            AvailabilityEntry(
                route_id=str(row["route_id"]),
                bucket_key=str(row["bucket_key"]),
                exact_identity=str(row["exact_identity"]),
                control_exact_identity=str(
                    row["control_exact_identity"]
                ),
                genes={
                    str(key): str(value)
                    for key, value in dict(row["genes"]).items()
                },
            )
            for row in payload.get("entries") or ()
        ]
    else:
        entries, enumeration = enumerate_authoritative_entries(
            generator=generator,
            lanes_by_route=lanes_by_route,
            routes=ROUTES,
        )
        payload = {
            "schema_version": (
                "cn_route_local_availability_index_binding_v1"
            ),
            "input_hashes": normalized_hashes,
            "enumeration": enumeration,
            "entries": [entry.to_dict() for entry in entries],
            "financial_reads": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }
        payload["payload_hash"] = _stable_hash(payload)
        _write_json(path, payload)
    if not entries:
        raise RuntimeError("LARGE_TPE_AVAILABILITY_INDEX_EMPTY")
    return entries, path


def _restore_or_initialize_availability_controller(
    *,
    output_root: Path,
    entries: Sequence[AvailabilityEntry],
    historical_exact: set[str],
    checkpoint_count: int,
    emitter_seed: int,
    input_hashes: Mapping[str, str],
) -> RouteLocalAvailabilityController:
    if checkpoint_count == 0:
        return RouteLocalAvailabilityController(
            entries=entries,
            seen_exact_identities=historical_exact,
            emitter_seed=emitter_seed,
            input_hashes=input_hashes,
        )
    state_path = (
        output_root
        / "checkpoints"
        / f"checkpoint_{checkpoint_count:03d}"
        / "availability_controller_state.json"
    )
    if not state_path.is_file():
        raise RuntimeError(
            "LARGE_TPE_AVAILABILITY_CONTROLLER_STATE_MISSING"
        )
    return RouteLocalAvailabilityController.restore(
        entries=entries,
        seen_exact_identities=historical_exact,
        state=json.loads(state_path.read_text(encoding="utf-8-sig")),
        input_hashes=input_hashes,
    )


def _allocate_checkpoint_asks(
    *,
    evaluated_by_route: Mapping[str, int],
    asked_by_route: Mapping[str, int],
    infeasible_routes: Sequence[str] = (),
    remaining_exact_by_route: Mapping[str, int] | None = None,
) -> dict[str, int]:
    infeasible = set(map(str, infeasible_routes))
    active = [
        route_id
        for route_id in ROUTES
        if int(evaluated_by_route.get(route_id, 0))
        < ROUTE_EVALUATED_TARGETS[route_id]
        and route_id not in infeasible
        and (
            remaining_exact_by_route is None
            or int(remaining_exact_by_route.get(route_id, 0)) > 0
        )
    ]
    if not active:
        return {}
    weights = {}
    for route_id in active:
        completed = int(evaluated_by_route.get(route_id, 0))
        asked = int(asked_by_route.get(route_id, 0))
        remaining = ROUTE_EVALUATED_TARGETS[route_id] - completed
        if asked:
            observed_yield = completed / asked
            # A zero-yield route receives a finite evidence-sized weight; no
            # invented 20% floor and no infinite weight.
            allocation_yield = (
                observed_yield if observed_yield > 0.0 else 1.0 / (asked + 1)
            )
            weights[route_id] = remaining / allocation_yield
        else:
            # No historical yield exists.  Preserve target proportionality
            # rather than claiming an arbitrary preflight conversion rate.
            weights[route_id] = float(remaining)
    minimum = 8
    allocation = {route_id: minimum for route_id in active}
    residual = ASKS_PER_CHECKPOINT - minimum * len(active)
    total_weight = sum(weights.values())
    fractional = []
    for route_id in active:
        raw = residual * weights[route_id] / total_weight
        whole = int(math.floor(raw))
        allocation[route_id] += whole
        fractional.append((raw - whole, route_id))
    missing = ASKS_PER_CHECKPOINT - sum(allocation.values())
    for _, route_id in sorted(fractional, reverse=True)[:missing]:
        allocation[route_id] += 1
    return allocation


def _route_budget_feasibility(
    *,
    remaining_evaluated_target: int,
    remaining_formal_ask_budget: int,
    remaining_exact_count: int,
    recent_checkpoints: Sequence[Mapping[str, int]],
) -> dict[str, Any]:
    """Classify absolute infeasibility separately from point-estimate risk."""

    remaining_target = max(0, int(remaining_evaluated_target))
    remaining_budget = max(0, int(remaining_formal_ask_budget))
    remaining_exact = max(0, int(remaining_exact_count))
    required_future_yield = (
        remaining_target / remaining_budget
        if remaining_budget
        else (0.0 if remaining_target == 0 else None)
    )
    checkpoint_yields = []
    for row in recent_checkpoints[-2:]:
        formal = max(0, int(row.get("formal_fresh_exact_asks", 0)))
        evaluated = max(0, int(row.get("pair_evaluated", 0)))
        checkpoint_yields.append(
            evaluated / formal if formal else None
        )
    two_checkpoint_shortfall = (
        required_future_yield is not None
        and len(checkpoint_yields) == 2
        and all(
            value is not None and value < required_future_yield
            for value in checkpoint_yields
        )
    )
    absolute_ceiling = min(remaining_budget, remaining_exact)
    if remaining_target > absolute_ceiling:
        status = "INFEASIBLE_ABSOLUTE_CEILING"
    elif two_checkpoint_shortfall:
        status = "AT_RISK_RECENT_YIELD_SHORTFALL_TWO_CHECKPOINTS"
    else:
        status = "FEASIBLE_NOT_PROVEN"
    return {
        "status": status,
        "remaining_evaluated_target": remaining_target,
        "remaining_formal_ask_budget": remaining_budget,
        "remaining_exact_count": remaining_exact,
        "absolute_maximum_future_evaluations": absolute_ceiling,
        "required_future_yield": required_future_yield,
        "recent_checkpoint_yields": checkpoint_yields,
        "two_checkpoint_point_estimate_shortfall": two_checkpoint_shortfall,
    }


def _load_closed_state(
    *,
    output_root: Path,
    initial_exact: set[str],
    initial_behavior: PortfolioBehaviorArchive,
) -> dict[str, Any]:
    exact = set(initial_exact)
    behavior = _copy_behavior_archive(initial_behavior)
    candidates: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    full_behavior: list[dict[str, Any]] = []
    transcripts = {route_id: [] for route_id in ROUTES}
    evaluated = Counter()
    asked_counts = Counter()
    summaries = []
    prior_manifest: Path | None = None
    for checkpoint_index in range(MAXIMUM_CHECKPOINTS):
        root = (
            output_root
            / "checkpoints"
            / f"checkpoint_{checkpoint_index + 1:03d}"
        )
        manifest_path = root / "batch_manifest.json"
        if not manifest_path.is_file():
            if any(
                (
                    output_root
                    / "checkpoints"
                    / f"checkpoint_{later + 1:03d}"
                    / "batch_manifest.json"
                ).is_file()
                for later in range(
                    checkpoint_index + 1, MAXIMUM_CHECKPOINTS
                )
            ):
                raise RuntimeError("NONCONTIGUOUS_LARGE_TPE_CHECKPOINTS")
            break
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8-sig")
        )
        _verify_artifacts(root, manifest)
        expected_prior = (
            _sha256(prior_manifest) if prior_manifest else "GENESIS"
        )
        if str(
            (manifest.get("input_hashes") or {}).get(
                "prior_checkpoint_manifest"
            )
            or ""
        ) != expected_prior:
            raise RuntimeError("LARGE_TPE_CHECKPOINT_CHAIN_DRIFT")
        asked = json.loads(
            (root / "asked_population.json").read_text(
                encoding="utf-8-sig"
            )
        )
        observations = (
            pd.read_parquet(root / "optimizer_observations.parquet")
            .where(pd.notna, None)
            .to_dict(orient="records")
        )
        asked_by_id = {
            str(row["proposal_id"]): row for row in asked
        }
        for route_id in ROUTES:
            route_asked = [
                row for row in asked if str(row["route_id"]) == route_id
            ]
            route_observations = [
                row
                for row in observations
                if str(asked_by_id[str(row["proposal_id"])]["route_id"])
                == route_id
            ]
            if route_asked:
                transcripts[route_id].append(
                    {
                        "checkpoint_id": (
                            f"checkpoint_{checkpoint_index + 1:03d}"
                        ),
                        "asked": route_asked,
                        "observations": [
                            {
                                "proposal_id": str(row["proposal_id"]),
                                "trial_number": int(
                                    asked_by_id[
                                        str(row["proposal_id"])
                                    ]["trial_number"]
                                ),
                                "state": (
                                    "COMPLETE"
                                    if str(row.get("outcome_class") or "")
                                    == EVALUATED
                                    else (
                                        "PRUNED"
                                        if str(
                                            row.get("outcome_class") or ""
                                        )
                                        == "LIVE_RUNNER_PRUNED"
                                        else "FAIL"
                                    )
                                ),
                                "optimizer_reward": row.get(
                                    "optimizer_reward"
                                ),
                                "outcome_class": str(
                                    row.get("outcome_class") or ""
                                ),
                                "outcome_reason": str(
                                    row.get("outcome_reason") or ""
                                ),
                                "optimizer_intermediate_value": row.get(
                                    "optimizer_intermediate_value"
                                ),
                                "optimizer_intermediate_step": row.get(
                                    "optimizer_intermediate_step"
                                ),
                                "pruning_authority": str(
                                    row.get("pruning_authority") or ""
                                ),
                            }
                            for row in route_observations
                        ],
                    }
                )
            asked_counts[route_id] += sum(
                bool(row.get("formal_fresh_exact_ask", True))
                for row in route_asked
            )
        candidate_rows = (
            pd.read_parquet(root / "candidate_attempts.parquet")
            .where(pd.notna, None)
            .to_dict(orient="records")
        )
        admitted_rows = (
            pd.read_parquet(root / "admitted_candidates.parquet")
            .where(pd.notna, None)
            .to_dict(orient="records")
        )
        outcome_rows = (
            pd.read_parquet(root / "pair_outcomes.parquet")
            .where(pd.notna, None)
            .to_dict(orient="records")
        )
        probe_rows = (
            pd.read_parquet(root / "behavior_probe.parquet")
            .where(pd.notna, None)
            .to_dict(orient="records")
        )
        full_rows = (
            pd.read_parquet(root / "full_behavior.parquet")
            .where(pd.notna, None)
            .to_dict(orient="records")
        )
        for row in candidate_rows:
            identity = str(row.get("exact_identity") or "")
            if identity:
                exact.add(identity)
        _add_resolved_behavior_rows(behavior, probe_rows)
        _add_resolved_behavior_rows(behavior, full_rows)
        candidates.extend(admitted_rows)
        outcomes.extend(outcome_rows)
        full_behavior.extend(full_rows)
        for row in outcome_rows:
            if str(row.get("pair_evaluation_status") or "") == (
                "PAIR_EVALUATED"
            ):
                evaluated[str(row["route_id"])] += 1
        summaries.append(
            json.loads(
                (root / "checkpoint_summary.json").read_text(
                    encoding="utf-8-sig"
                )
            )
        )
        prior_manifest = manifest_path
    return {
        "exact": exact,
        "behavior": behavior,
        "candidates": candidates,
        "outcomes": outcomes,
        "full_behavior": full_behavior,
        "transcripts": transcripts,
        "evaluated": evaluated,
        "asked_counts": asked_counts,
        "summaries": summaries,
        "prior_manifest": prior_manifest,
    }


def _select_validation_finalists(
    *,
    candidates: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    behavior_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    candidate_by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        candidate_by_pair[str(row.get("pair_id") or "")].append(dict(row))
    family_by_pair = {
        str(row.get("pair_id") or ""): str(
            row.get("portfolio_behavior_family_id")
            or row.get("portfolio_behavior_signature_id")
            or ""
        )
        for row in behavior_rows
    }
    evaluated = [dict(row) for row in outcomes if _validation_eligible(row)]
    for row in evaluated:
        row["search_score"] = _conservative_search_score(row)
        primary = _finite_float(row.get("primary_composite_reward"))
        if primary is None:
            primary = _finite_float(row.get("primary_train_reward"))
        matched = _finite_float(row.get("matched_train_increment"))
        if matched is None:
            matched = _finite_float(row.get("pair_train_reward"))
        row["primary_composite_reward"] = primary
        row["matched_train_increment"] = matched
    evaluated.sort(
        key=lambda row: (
            -float(row["search_score"]),
            -float(row["primary_composite_reward"]),
            -float(row["matched_train_increment"]),
            str(row.get("pair_id") or ""),
        )
    )
    selected_pairs: list[str] = []
    selected_families: set[str] = set()
    per_route = Counter()
    route_floor = 16
    for row in evaluated:
        route_id = str(row["route_id"])
        pair_id = str(row["pair_id"])
        family_id = family_by_pair.get(pair_id, "")
        if per_route[route_id] >= route_floor:
            continue
        if family_id and family_id in selected_families:
            continue
        selected_pairs.append(pair_id)
        per_route[route_id] += 1
        if family_id:
            selected_families.add(family_id)
    for row in evaluated:
        if len(selected_pairs) >= VALIDATION_FINALIST_PAIRS:
            break
        pair_id = str(row["pair_id"])
        if pair_id in selected_pairs:
            continue
        family_id = family_by_pair.get(pair_id, "")
        if family_id and family_id in selected_families:
            continue
        selected_pairs.append(pair_id)
        if family_id:
            selected_families.add(family_id)
    if len(selected_pairs) < VALIDATION_FINALIST_PAIRS:
        for row in evaluated:
            if len(selected_pairs) >= VALIDATION_FINALIST_PAIRS:
                break
            pair_id = str(row["pair_id"])
            if pair_id not in selected_pairs:
                selected_pairs.append(pair_id)
    if len(selected_pairs) < VALIDATION_FINALIST_PAIRS:
        raise RuntimeError("TRAIN_FINALIST_SUPPLY_BELOW_256")
    return [
        row
        for pair_id in selected_pairs[:VALIDATION_FINALIST_PAIRS]
        for row in candidate_by_pair[pair_id]
    ]


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"LARGE_TPE_AUTHORIZED_ONLY_ON_77O:{platform.node()}"
        )
    if int(args.active_threads) != 32 or int(args.session_threads) != 32:
        raise RuntimeError("LARGE_TPE_THREAD_CONTRACT_MISMATCH")
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    authority = _authorization_binding(
        path=args.campaign_authorization.resolve(),
        candidate_archive=args.historical_candidate_archive.resolve(),
        behavior_archive=args.historical_behavior_archive.resolve(),
        history_manifest=args.historical_archive_manifest.resolve(),
        seed_base=args.seed_base,
        active_threads=args.active_threads,
        session_threads=args.session_threads,
    )
    authority_path = _write_json(
        output_root / "campaign_authority_binding.json", authority
    )
    historical_exact, initial_behavior, archive_snapshot = (
        _load_historical_dedupe(
            candidate_archive_path=args.historical_candidate_archive.resolve(),
            behavior_archive_path=args.historical_behavior_archive.resolve(),
        )
    )
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    registry_path = _write_json(
        output_root / "registry_binding.json",
        _registry_binding(args.registry.resolve(), registry),
    )
    discovery = load_development_discovery_root_authority(
        args.discovery_contract.resolve(), registry=registry
    )
    discovery_auth = json.loads(
        args.discovery_authorization.resolve().read_text(
            encoding="utf-8-sig"
        )
    )
    if (
        not bool(discovery_auth.get("execution_authorized"))
        or str(discovery_auth.get("root_contract_hash") or "")
        != str(discovery["contract_hash"])
    ):
        raise RuntimeError("DISCOVERY_AUTHORITY_MISMATCH")
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    field_roots = {
        "active_bar": args.active_field_root.resolve(),
        "stock_session": args.session_field_root.resolve(),
    }
    label_roots = {
        "active_bar": args.active_label_root.resolve(),
        "stock_session": args.session_label_root.resolve(),
    }
    validation_field_roots = {
        "active_bar": args.validation_active_field_root.resolve(),
        "stock_session": args.validation_session_field_root.resolve(),
    }
    validation_label_roots = {
        "active_bar": args.validation_active_label_root.resolve(),
        "stock_session": args.validation_session_label_root.resolve(),
    }
    compute_threads = {"active_bar": 32, "stock_session": 32}
    schema, schema_by_backend = materialized_schema_binding(
        field_roots=field_roots, registry=registry
    )
    schema_path = _write_json(
        output_root / "materialized_schema_binding.json", schema
    )
    purity = audit_split_boundary_label_purity(
        split=split, registry=registry, label_roots=label_roots
    )
    purity_path = _write_json(
        output_root / "split_boundary_purity.json", purity
    )
    if str(purity.get("status") or "") != "PASS":
        raise RuntimeError("SPLIT_BOUNDARY_LABEL_PURITY_FAILED")
    runtime = _runtime_envelope(32, 32)
    runtime_path = _write_json(
        output_root / "runtime_envelope.json", runtime
    )
    if str(runtime.get("status") or "") != "PASS":
        raise RuntimeError(str(runtime.get("status") or "RUNTIME_FAIL"))
    materialized_allowlists = _materialized_route_root_allowlists(
        registry=registry,
        discovery_allowlists=discovery["route_root_allowlists"],
        schema_by_backend=schema_by_backend,
    )
    contract = {
        "schema_version": "cn_large_tpe_search_contract_v2",
        "status": "FROZEN_EXECUTABLE",
        "campaign_profile": CAMPAIGN_PROFILE,
        "minimum_actual_evaluated_pairs": MINIMUM_ACTUAL_EVALUATED_PAIRS,
        "route_actual_evaluated_targets": ROUTE_EVALUATED_TARGETS,
        "maximum_checkpoints": MAXIMUM_CHECKPOINTS,
        "asks_per_checkpoint": ASKS_PER_CHECKPOINT,
        "maximum_raw_asks": MAXIMUM_RAW_ASKS,
        "maximum_wall_seconds": MAXIMUM_WALL_SECONDS,
        "top_level_scheduling_key": "UNIFIED_REGISTRY_ROUTE_ID",
        "route_local_generation_mode": "TYPED_GRAMMAR_SKELETON_ID",
        "optimizer": (
            "official_optuna.samplers.TPESampler_conditional_typed_grammar"
        ),
        "optimizer_role": "ROUTE_LOCAL_GENE_SELECTION_ONLY",
        "optimizer_initialization": "FRESH_NO_CROSS_CAMPAIGN_REWARD_STATE",
        "startup_trials_by_route": STARTUP_TRIALS_BY_ROUTE,
        "n_ei_candidates": N_EI_CANDIDATES,
        "sampler_mode": TPE_SAMPLER_MODE,
        "multivariate": TPE_MULTIVARIATE,
        "group": TPE_GROUP,
        "constant_liar": TPE_CONSTANT_LIAR,
        "formula_constructor_authority": "CompositionalGrammarV2",
        "compiler_authority": "TypedRouteCompiler",
        "matched_control_authority": "MATCHED_CONTROL_PAIR_AUTHORITY",
        "exact_memory": "CUMULATIVE_IDENTITY_ONLY",
        "behavior_memory": "CUMULATIVE_LABEL_FREE_AND_FULL_COORDINATE",
        "optimizer_feedback": "FULL_COORDINATE_TRAIN_ONLY",
        "optimizer_reward": "conservative_primary_and_increment_search_score",
        "optimizer_search_score_policy": SEARCH_SCORE_POLICY,
        "optimizer_search_score_formula": (
            "min(primary_composite_reward,matched_train_increment)"
        ),
        "validation_primary_decision": VALIDATION_PRIMARY_DECISION,
        "optimizer_restore_authority": (
            "HASH_BOUND_OPTUNA_STATE_SNAPSHOT_PLUS_IMMUTABLE_TRANSCRIPTS"
        ),
        "validation": "AUTOMATIC_REPORT_ONLY_ON_256_TRAIN_FINALISTS",
        "validation_feedback": "FORBIDDEN",
        "validation_scheduler_write": "FORBIDDEN",
        "validation_archive_write": "FORBIDDEN",
        "portfolio_mode": "LONG_ONLY_TOP",
        "shorting": "FORBIDDEN",
        "one_way_cost_bps": 5,
        "horizons_minutes": [1, 5, 15, 30],
        "pair_batch_sizes": PAIR_BATCH_SIZES,
        "compute_threads": compute_threads,
        "cache_cap_bytes": MAXIMUM_CACHE_BYTES,
        "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "materialized_route_root_allowlists": {
            route_id: list(values)
            for route_id, values in materialized_allowlists.items()
        },
        "archive_snapshot": archive_snapshot,
        "input_bindings": {
            "authority": _artifact(authority_path, root=output_root),
            "registry": _artifact(registry_path, root=output_root),
            "schema": _artifact(schema_path, root=output_root),
            "purity": _artifact(purity_path, root=output_root),
            "runtime": _artifact(runtime_path, root=output_root),
        },
    }
    contract_path = _write_json(
        output_root / "frozen_contract.json", contract
    )
    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist=materialized_allowlists,
    )
    lanes_by_route, lane_manifest_path = _freeze_gene_lanes(
        output_root=output_root,
        generator=generator,
        input_hashes={
            "contract": _sha256(contract_path),
            "registry": _sha256(registry_path),
            "schema": _sha256(schema_path),
        },
    )
    supply_path = output_root / "fresh_exact_supply_preflight.json"
    if supply_path.is_file():
        supply = json.loads(
            supply_path.read_text(encoding="utf-8-sig")
        )
    else:
        supply = _fresh_exact_supply(
            generator=generator,
            lanes_by_route=lanes_by_route,
            historical_exact=historical_exact,
        )
        _write_json(supply_path, supply)
    if str(supply.get("status") or "") != "PASS":
        raise RuntimeError("LARGE_TPE_FRESH_EXACT_SUPPLY_PREFLIGHT_FAILED")
    if args.preflight_only:
        return {
            "status": "PREFLIGHT_PASS",
            "supply": supply,
            "financial_reads": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }

    state = _load_closed_state(
        output_root=output_root,
        initial_exact=historical_exact,
        initial_behavior=initial_behavior,
    )
    adapters = _restore_or_import_adapters(
        output_root=output_root,
        lanes_by_route=lanes_by_route,
        seed_base=int(args.seed_base),
        transcripts=state["transcripts"],
        checkpoint_count=len(state["summaries"]),
        prior_manifest=state["prior_manifest"],
    )
    availability_input_hashes = {
        "registry": _sha256(registry_path),
        "grammar_lanes": _sha256(lane_manifest_path),
        "compiler_contract": _sha256(contract_path),
        "historical_exact_archive": str(
            archive_snapshot["candidate_archive"]["sha256"]
        ),
    }
    availability_entries, availability_index_path = (
        _freeze_availability_index(
            output_root=output_root,
            generator=generator,
            lanes_by_route=lanes_by_route,
            input_hashes=availability_input_hashes,
        )
    )
    availability_controller = (
        _restore_or_initialize_availability_controller(
            output_root=output_root,
            entries=availability_entries,
            historical_exact=historical_exact,
            checkpoint_count=len(state["summaries"]),
            emitter_seed=int(args.seed_base) + 97_003,
            input_hashes=availability_input_hashes,
        )
    )
    deadline = time.time() + int(args.maximum_wall_seconds)
    checkpoint_index = len(state["summaries"])
    while (
        checkpoint_index < MAXIMUM_CHECKPOINTS
        and sum(state["evaluated"].values())
        < MINIMUM_ACTUAL_EVALUATED_PAIRS
        and sum(state["asked_counts"].values()) < MAXIMUM_RAW_ASKS
        and time.time() < deadline
    ):
        checkpoint_id = f"checkpoint_{checkpoint_index + 1:03d}"
        root = output_root / "checkpoints" / checkpoint_id
        root.mkdir(parents=True, exist_ok=True)
        schedule_path = root / "route_schedule.json"
        if schedule_path.is_file():
            schedule = json.loads(
                schedule_path.read_text(encoding="utf-8-sig")
            )
        else:
            allocation = _allocate_checkpoint_asks(
                evaluated_by_route=state["evaluated"],
                asked_by_route=state["asked_counts"],
                remaining_exact_by_route={
                    route_id: availability_controller.remaining_count(
                        route_id=route_id
                    )
                    for route_id in ROUTES
                },
            )
            schedule = {
                "checkpoint": checkpoint_id,
                "top_level_scheduling_key": "UNIFIED_REGISTRY_ROUTE_ID",
                "routes": [
                    {
                        "route_id": route_id,
                        "asked_pairs": count,
                        "generation_mode": (
                            "OPTUNA_TPE_ROUTE_LOCAL_AVAILABILITY"
                        ),
                    }
                    for route_id, count in allocation.items()
                ],
            }
            _write_json(schedule_path, schedule)
        ask_path = root / "asked_population.json"
        previous_asked: list[dict[str, Any]] | None = None
        if ask_path.is_file():
            previous_asked = json.loads(
                ask_path.read_text(encoding="utf-8-sig")
            )
        (
            asked,
            formal_asked,
            internal_observations,
            ask_runtime,
        ) = _ask_availability_aware_populations(
            schedule=schedule["routes"],
            adapters=adapters,
            controller=availability_controller,
            generator=generator,
            schema_by_backend=schema_by_backend,
            checkpoint_id=checkpoint_id,
        )
        if previous_asked is not None and _stable_hash(
            previous_asked
        ) != _stable_hash(asked):
            raise RuntimeError(
                "LARGE_TPE_AVAILABILITY_ASK_REPLAY_DRIFT"
            )
        _write_json(ask_path, asked)
        ask_runtime_path = _write_json(
            root / "optimizer_ask_runtime.json", ask_runtime
        )
        materialized = _materialize_population(
            asked=formal_asked,
            generator=generator,
            schema_by_backend=schema_by_backend,
        )
        observations: dict[str, dict[str, Any]] = dict(
            internal_observations
        )
        unique_asked = []
        generation_exact: set[str] = set()
        for row in materialized:
            proposal_id = str(row["proposal_id"])
            identity = str(row.get("exact_identity") or "")
            if (
                str(row.get("construction_status") or "") != "LEGAL"
                or not identity
                or identity in state["exact"]
                or identity in generation_exact
                or identity
                != str(row.get("availability_exact_identity") or "")
            ):
                raise RuntimeError(
                    "LARGE_TPE_FORMAL_AVAILABILITY_GUARANTEE_DRIFT:"
                    f"{proposal_id}"
                )
            generation_exact.add(identity)
            unique_asked.append(row)
        candidate_rows = [
            dict(member)
            for row in unique_asked
            for member in (row["primary"], row["control"])
        ]
        candidate_path = _write_parquet(
            root / "candidate_attempts.parquet", candidate_rows
        )
        probe_path = root / "behavior_probe.parquet"
        probe_audit_path = root / "behavior_probe_audit.json"
        preadmission_path = root / "pre_admission_complete.json"
        if preadmission_path.is_file():
            preadmission = json.loads(
                preadmission_path.read_text(encoding="utf-8-sig")
            )
            if (
                str(preadmission["candidate_attempts_sha256"])
                != _sha256(candidate_path)
                or str(preadmission["behavior_probe_sha256"])
                != _sha256(probe_path)
            ):
                raise RuntimeError("LARGE_TPE_PREADMISSION_DRIFT")
            probe_rows = (
                pd.read_parquet(probe_path)
                .where(pd.notna, None)
                .to_dict(orient="records")
            )
        else:
            probe_rows, probe_audit = _probe_pack(
                candidate_rows=candidate_rows,
                field_roots=field_roots,
                train_dates=_train_dates(split),
                coordinate_binding=_stable_hash(
                    {
                        "campaign": CAMPAIGN_PROFILE,
                        "checkpoint": checkpoint_id,
                        "contract": _sha256(contract_path),
                        "gene_lanes": _sha256(lane_manifest_path),
                        "split": split.manifest_hash,
                    }
                ),
                batch_id=checkpoint_id,
                compute_threads=compute_threads,
            )
            _write_parquet(probe_path, probe_rows)
            _write_json(probe_audit_path, probe_audit)
            _write_json(
                preadmission_path,
                {
                    "status": "PRE_ADMISSION_COMPLETE",
                    "candidate_attempts_sha256": _sha256(candidate_path),
                    "behavior_probe_sha256": _sha256(probe_path),
                    "behavior_probe_audit_sha256": _sha256(
                        probe_audit_path
                    ),
                },
            )
        admitted, decisions = _admit_behavior_unique(
            candidate_rows=candidate_rows,
            probe_rows=probe_rows,
            historical_archive=_copy_behavior_archive(state["behavior"]),
        )
        admitted_path = _write_parquet(
            root / "admitted_candidates.parquet", admitted
        )
        decisions_path = _write_parquet(
            root / "admission_decisions.parquet", decisions
        )
        decision_by_pair = {
            str(row["pair_id"]): row for row in decisions
        }
        ask_by_pair = {
            str(row["pair_id"]): row for row in unique_asked
        }
        for pair_id, ask in ask_by_pair.items():
            decision = decision_by_pair[pair_id]
            admitted_decision = (
                str(decision.get("admission_decision") or "") == "ADMIT"
            )
            availability_controller.record_behavior(
                route_id=str(ask["route_id"]),
                admitted=admitted_decision,
                bucket_key=str(
                    ask.get("availability_bucket_key") or ""
                ),
            )
            if not admitted_decision:
                observations[str(ask["proposal_id"])] = {
                    "proposal_id": str(ask["proposal_id"]),
                    "outcome_class": "BEHAVIOR_BLOCKED",
                    "optimizer_reward": None,
                    "outcome_reason": str(
                        decision.get("admission_reason")
                        or "BEHAVIOR_BLOCKED"
                    ),
                }
        access_receipts = []
        binding_path: Path | None = None
        table_paths: dict[str, Path] = {}
        if admitted:
            binding_path, table_paths = _context_and_binding(
                batch_root=root,
                candidates=admitted,
                registry=registry,
                split=split,
                data_release_hash=_sha256(args.sidecar_closure.resolve()),
            )
            _bind_purity(binding_path, purity_path)
            access_receipts = _run_phase3cm_monitored(
                checkpoint_id=checkpoint_id,
                checkpoint_root=root,
                binding_path=binding_path,
                table_paths=table_paths,
                split_manifest=split.manifest_path,
                field_roots=field_roots,
                label_roots=label_roots,
                purity_path=purity_path,
                compute_threads=compute_threads,
                deadline_epoch=deadline,
                pair_batch_sizes=PAIR_BATCH_SIZES,
            )
        outcomes, full_behavior = _outcome_rows(root)
        if admitted:
            full_behavior = _join_full_behavior_identities(
                full_behavior, probe_rows
            )
        for outcome in outcomes:
            outcome["primary_composite_reward"] = outcome.get(
                "primary_composite_reward",
                outcome.get("primary_train_reward"),
            )
            outcome["control_composite_reward"] = outcome.get(
                "control_composite_reward",
                outcome.get("control_train_reward"),
            )
            outcome["matched_train_increment"] = outcome.get(
                "matched_train_increment",
                outcome.get("pair_train_reward"),
            )
            outcome["search_score"] = _conservative_search_score(outcome)
            outcome["search_score_policy"] = SEARCH_SCORE_POLICY
        outcome_path = _write_parquet(
            root / "pair_outcomes.parquet", outcomes
        )
        full_behavior_path = _write_parquet(
            root / "full_behavior.parquet", full_behavior
        )
        outcome_by_pair = {
            str(row["pair_id"]): row for row in outcomes
        }
        admitted_pair_ids = {
            str(row["pair_id"])
            for row in admitted
            if str(row.get("pair_member_role") or "") == "PRIMARY"
        }
        if admitted_pair_ids != set(outcome_by_pair):
            raise RuntimeError("LARGE_TPE_EVALUATOR_OUTCOME_COVERAGE_DRIFT")
        for pair_id in admitted_pair_ids:
            ask = ask_by_pair[pair_id]
            outcome = outcome_by_pair[pair_id]
            outcome_class = _outcome_class(outcome)
            reward = (
                _conservative_search_score(outcome)
                if outcome_class == EVALUATED
                else None
            )
            observations[str(ask["proposal_id"])] = {
                "proposal_id": str(ask["proposal_id"]),
                "outcome_class": outcome_class,
                "optimizer_reward": reward,
                "search_score": reward,
                "search_score_policy": SEARCH_SCORE_POLICY,
                "primary_composite_reward": outcome.get(
                    "primary_composite_reward"
                ),
                "control_composite_reward": outcome.get(
                    "control_composite_reward"
                ),
                "matched_train_increment": outcome.get(
                    "matched_train_increment"
                ),
                "outcome_reason": str(
                    outcome.get("pair_evaluation_blockers")
                    or "PAIR_EVALUATED"
                ),
            }
            if outcome_class == EVALUATED:
                availability_controller.record_evaluated(
                    route_id=str(ask["route_id"]),
                    bucket_key=str(
                        ask.get("availability_bucket_key") or ""
                    ),
                )
        if set(observations) != {
            str(row["proposal_id"]) for row in asked
        }:
            raise RuntimeError("LARGE_TPE_ASK_TELL_COVERAGE_DRIFT")
        ordered_observations = [
            observations[str(row["proposal_id"])] for row in asked
        ]
        observation_path = _write_parquet(
            root / "optimizer_observations.parquet",
            ordered_observations,
        )
        tell_receipts = []
        transcripts = {}
        asked_by_id = {
            str(row["proposal_id"]): row for row in asked
        }
        for route in schedule["routes"]:
            route_id = str(route["route_id"])
            route_observations = [
                row
                for row in ordered_observations
                if str(
                    asked_by_id[str(row["proposal_id"])]["route_id"]
                )
                == route_id
            ]
            tell_receipts.append(
                {
                    "route_id": route_id,
                    "receipt": adapters[route_id].tell_population(
                        route_observations
                    ),
                }
            )
            transcripts[route_id] = adapters[route_id].history[-1]
        tell_path = _write_json(
            root / "optimizer_tell_receipts.json", tell_receipts
        )
        transcript_path = _write_json(
            root / "optimizer_transcripts.json", transcripts
        )
        optimizer_snapshot_path, optimizer_snapshot_receipt_path = (
            _write_optimizer_snapshot(
                snapshot_path=root / "optimizer_state.pkl",
                receipt_path=root / "optimizer_state_receipt.json",
                adapters=adapters,
                boundary_checkpoint=checkpoint_id,
                prior_manifest_sha256=(
                    _sha256(state["prior_manifest"])
                    if state["prior_manifest"]
                    else "GENESIS"
                ),
            )
        )
        _add_resolved_behavior_rows(state["behavior"], probe_rows)
        _add_resolved_behavior_rows(state["behavior"], full_behavior)
        for row in candidate_rows:
            identity = str(row.get("exact_identity") or "")
            if identity:
                state["exact"].add(identity)
        state["candidates"].extend(admitted)
        state["outcomes"].extend(outcomes)
        state["full_behavior"].extend(full_behavior)
        for row in outcomes:
            if str(row.get("pair_evaluation_status") or "") == (
                "PAIR_EVALUATED"
            ):
                state["evaluated"][str(row["route_id"])] += 1
        for row in formal_asked:
            state["asked_counts"][str(row["route_id"])] += 1
        expected_backends = tuple(
            backend
            for backend in ("active_bar", "stock_session")
            if backend in table_paths
        )
        gate = (
            _runtime_gate(
                root,
                compute_threads,
                expected_backends=expected_backends,
            )
            if expected_backends
            else {
                "status": "NOT_APPLICABLE_NO_PHASE3CM",
                "expected_backends": [],
                "observed_backends": [],
            }
        )
        gate_path = _write_json(
            root / "runtime_utilization_gate.json", gate
        )
        if expected_backends and str(gate.get("status") or "") != "PASS":
            raise RuntimeError(
                "LARGE_TPE_RUNTIME_ACCELERATION_GATE_FAILED"
            )
        availability_state_path = _write_json(
            root / "availability_controller_state.json",
            availability_controller.snapshot(),
        )
        summary = {
            "checkpoint": checkpoint_id,
            "asked_pairs": len(formal_asked),
            "formal_fresh_exact_asks": len(formal_asked),
            "optimizer_draw_attempts": len(asked),
            "internal_failed_trials": len(internal_observations),
            "exact_unique_pairs": len(unique_asked),
            "behavior_admitted_pairs": len(admitted) // 2,
            "actual_evaluated_pairs": sum(
                str(row.get("pair_evaluation_status") or "")
                == "PAIR_EVALUATED"
                for row in outcomes
            ),
            "cumulative_evaluated_pairs": sum(
                state["evaluated"].values()
            ),
            "evaluated_by_route": dict(state["evaluated"]),
            "asked_by_route": dict(state["asked_counts"]),
            "minimum_free_memory_bytes": _minimum_free_memory(root),
            "runtime_gate_status": str(gate.get("status") or ""),
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }
        summary_path = _write_json(
            root / "checkpoint_summary.json", summary
        )
        manifest_paths = [
            schedule_path,
            ask_path,
            ask_runtime_path,
            candidate_path,
            probe_path,
            probe_audit_path,
            preadmission_path,
            admitted_path,
            decisions_path,
            outcome_path,
            full_behavior_path,
            observation_path,
            tell_path,
            transcript_path,
            optimizer_snapshot_path,
            optimizer_snapshot_receipt_path,
            availability_state_path,
            gate_path,
            summary_path,
        ]
        if binding_path is not None:
            manifest_paths.append(binding_path)
            manifest_paths.extend(table_paths.values())
        manifest_paths.extend(
            Path(str(row["result_path"]))
            for row in access_receipts
            if str(row.get("result_path") or "")
        )
        previous = state["prior_manifest"]
        state["prior_manifest"] = _batch_manifest(
            batch_root=root,
            batch_id=checkpoint_id,
            input_hashes={
                "campaign_authority": _sha256(authority_path),
                "frozen_contract": _sha256(contract_path),
                "gene_lane_manifest": _sha256(lane_manifest_path),
                "availability_index": _sha256(
                    availability_index_path
                ),
                "prior_checkpoint_manifest": (
                    _sha256(previous) if previous else "GENESIS"
                ),
            },
            paths=manifest_paths,
            access_receipts=access_receipts,
        )
        state["summaries"].append(summary)
        checkpoint_index += 1

    evaluated_total = sum(state["evaluated"].values())
    route_targets_met = all(
        int(state["evaluated"].get(route_id, 0)) >= target
        for route_id, target in ROUTE_EVALUATED_TARGETS.items()
    )
    qualified = (
        evaluated_total >= MINIMUM_ACTUAL_EVALUATED_PAIRS
        and route_targets_met
    )
    candidate_ledger_path = _write_parquet(
        output_root / "candidate_ledger.parquet", state["candidates"]
    )
    observation_ledger_path = _write_parquet(
        output_root / "observation_ledger.parquet", state["outcomes"]
    )
    behavior_path = output_root / "behavior_archive.parquet"
    state["behavior"].write_parquet(behavior_path)
    optimizer_path = _write_json(
        output_root / "optimizer_history.json",
        {
            route_id: adapters[route_id].history_receipt()
            for route_id in ROUTES
        },
    )
    finalists = (
        _select_validation_finalists(
            candidates=state["candidates"],
            outcomes=state["outcomes"],
            behavior_rows=state["full_behavior"],
        )
        if qualified
        else []
    )
    finalists_path = _write_parquet(
        output_root / "train_finalists.parquet", finalists
    )
    train_manifest_path = _write_json(
        output_root / "train_complete_manifest.json",
        {
            "schema_version": "cn_large_tpe_train_complete_v1",
            "status": "TRAIN_COMPLETE" if qualified else "TRAIN_INCOMPLETE",
            "actual_evaluated_pairs": evaluated_total,
            "minimum_actual_evaluated_pairs": MINIMUM_ACTUAL_EVALUATED_PAIRS,
            "route_actual_evaluated_targets": ROUTE_EVALUATED_TARGETS,
            "actual_evaluated_by_route": dict(state["evaluated"]),
            "checkpoint_count": len(state["summaries"]),
            "raw_asks": sum(state["asked_counts"].values()),
            "candidate_ledger": _artifact(
                candidate_ledger_path, root=output_root
            ),
            "observation_ledger": _artifact(
                observation_ledger_path, root=output_root
            ),
            "behavior_archive": _artifact(behavior_path, root=output_root),
            "optimizer_history": _artifact(
                optimizer_path, root=output_root
            ),
            "train_finalists": _artifact(
                finalists_path, root=output_root
            ),
            "validation_trigger": (
                "AUTOMATIC_AFTER_IMMUTABLE_TRAIN_COMPLETE"
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
                behavior_path,
                optimizer_path,
                finalists_path,
            ),
            validation_root=output_root / "post_train_validation",
            candidates=finalists,
            registry=registry,
            split=split,
            validation_data_release_hash=_sha256(
                args.validation_sidecar_closure.resolve()
            ),
            split_manifest=args.split_manifest.resolve(),
            validation_field_roots=validation_field_roots,
            validation_label_roots=validation_label_roots,
            compute_threads=compute_threads,
        )
    decision = {
        "status": "CAMPAIGN_CLOSED" if qualified else "CAMPAIGN_INCOMPLETE",
        "actual_evaluated_pairs": evaluated_total,
        "minimum_actual_evaluated_pairs": MINIMUM_ACTUAL_EVALUATED_PAIRS,
        "actual_evaluated_by_route": dict(state["evaluated"]),
        "route_targets_met": route_targets_met,
        "checkpoint_count": len(state["summaries"]),
        "raw_asks": sum(state["asked_counts"].values()),
        "optimizer": (
            "official_optuna.samplers.TPESampler_conditional_typed_grammar"
        ),
        "validation_status": (
            "AUTOMATIC_POST_TRAIN_VALIDATION_COMPLETE"
            if validation_receipt
            else "NOT_RUN_TRAIN_INCOMPLETE"
        ),
        "validation_feedback": "FORBIDDEN",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    decision_path = _write_json(
        output_root / "final_decision.json", decision
    )
    _write_json(
        output_root / "run_manifest.json",
        {
            "status": decision["status"],
            "artifacts": [
                _artifact(path, root=output_root)
                for path in (
                    authority_path,
                    contract_path,
                    registry_path,
                    schema_path,
                    purity_path,
                    runtime_path,
                    lane_manifest_path,
                    availability_index_path,
                    supply_path,
                    candidate_ledger_path,
                    observation_ledger_path,
                    behavior_path,
                    optimizer_path,
                    finalists_path,
                    train_manifest_path,
                    decision_path,
                )
            ],
            "batch_manifest_count": len(state["summaries"]),
            "validation_feedback": "FORBIDDEN",
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
        },
    )
    return decision


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign-authorization", type=Path, required=True
    )
    parser.add_argument(
        "--historical-candidate-archive", type=Path, required=True
    )
    parser.add_argument(
        "--historical-behavior-archive", type=Path, required=True
    )
    parser.add_argument(
        "--historical-archive-manifest", type=Path, required=True
    )
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--discovery-contract", type=Path, required=True)
    parser.add_argument(
        "--discovery-authorization", type=Path, required=True
    )
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--sidecar-closure", type=Path, required=True)
    parser.add_argument("--active-field-root", type=Path, required=True)
    parser.add_argument("--active-label-root", type=Path, required=True)
    parser.add_argument("--session-field-root", type=Path, required=True)
    parser.add_argument("--session-label-root", type=Path, required=True)
    parser.add_argument(
        "--validation-sidecar-closure", type=Path, required=True
    )
    parser.add_argument(
        "--validation-active-field-root", type=Path, required=True
    )
    parser.add_argument(
        "--validation-active-label-root", type=Path, required=True
    )
    parser.add_argument(
        "--validation-session-field-root", type=Path, required=True
    )
    parser.add_argument(
        "--validation-session-label-root", type=Path, required=True
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--seed-base", type=int, default=2026072602)
    parser.add_argument("--active-threads", type=int, default=32)
    parser.add_argument("--session-threads", type=int, default=32)
    parser.add_argument(
        "--maximum-wall-seconds",
        type=int,
        default=MAXIMUM_WALL_SECONDS,
    )
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args(argv)
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
