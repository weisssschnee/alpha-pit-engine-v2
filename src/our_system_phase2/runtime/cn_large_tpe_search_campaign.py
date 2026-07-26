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
import platform
import statistics
import time
from collections import Counter, defaultdict
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
CAMPAIGN_PROFILE = "cn_large_optuna_tpe_actual20000_v1"
ROUTE_EVALUATED_TARGETS = {
    "SLOW_TEMPORAL_CHANGE": 15_500,
    "FIRSTN_PATH": 2_000,
    "SLOW_CROSS_SECTIONAL_LEVEL": 1_200,
    "MARKET_REGIME_CONDITION": 800,
    "DISCLOSURE_EVENT": 500,
}
ROUTES = tuple(ROUTE_EVALUATED_TARGETS)
MINIMUM_ACTUAL_EVALUATED_PAIRS = sum(ROUTE_EVALUATED_TARGETS.values())
MAXIMUM_CHECKPOINTS = 96
ASKS_PER_CHECKPOINT = 768
MAXIMUM_RAW_ASKS = MAXIMUM_CHECKPOINTS * ASKS_PER_CHECKPOINT
MAXIMUM_WALL_SECONDS = 7 * 24 * 60 * 60
VALIDATION_FINALIST_PAIRS = 256
PAIR_BATCH_SIZES = {"active_bar": 4, "stock_session": 8}
MAXIMUM_CACHE_BYTES = 8 * 1024**3
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
FRESH_EXACT_MARGIN = 1.20
STARTUP_TRIALS_BY_ROUTE = {
    "SLOW_TEMPORAL_CHANGE": 512,
    "FIRSTN_PATH": 128,
    "SLOW_CROSS_SECTIONAL_LEVEL": 128,
    "MARKET_REGIME_CONDITION": 64,
    "DISCLOSURE_EVENT": 64,
}
N_EI_CANDIDATES = 64


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
        "maximum_wall_seconds": MAXIMUM_WALL_SECONDS,
        "validation_finalist_pairs": VALIDATION_FINALIST_PAIRS,
        "active_threads": int(active_threads),
        "session_threads": int(session_threads),
        "active_pair_batch_size": PAIR_BATCH_SIZES["active_bar"],
        "session_pair_batch_size": PAIR_BATCH_SIZES["stock_session"],
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
        "schema_version": "cn_large_tpe_campaign_authority_binding_v1",
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
        if dict(manifest.get("input_hashes") or {}) != dict(input_hashes):
            raise RuntimeError("LARGE_TPE_GENE_LANE_INPUT_DRIFT")
        _verify_artifacts(output_root, manifest)
        payload = json.loads(
            payload_path.read_text(encoding="utf-8-sig")
        )
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


def _allocate_checkpoint_asks(
    *,
    evaluated_by_route: Mapping[str, int],
    asked_by_route: Mapping[str, int],
) -> dict[str, int]:
    active = [
        route_id
        for route_id in ROUTES
        if int(evaluated_by_route.get(route_id, 0))
        < ROUTE_EVALUATED_TARGETS[route_id]
    ]
    if not active:
        return {}
    weights = {}
    for route_id in active:
        completed = int(evaluated_by_route.get(route_id, 0))
        asked = int(asked_by_route.get(route_id, 0))
        remaining = ROUTE_EVALUATED_TARGETS[route_id] - completed
        observed_yield = completed / asked if asked else 0.50
        bounded_yield = min(0.90, max(0.20, observed_yield))
        weights[route_id] = remaining / bounded_yield
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
                                    else "FAIL"
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
                            }
                            for row in route_observations
                        ],
                    }
                )
            asked_counts[route_id] += len(route_asked)
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
    evaluated = [
        dict(row)
        for row in outcomes
        if str(row.get("pair_evaluation_status") or "") == "PAIR_EVALUATED"
        and row.get("optimizer_reward") is not None
        and math.isfinite(float(row["optimizer_reward"]))
    ]
    evaluated.sort(
        key=lambda row: (
            -float(row["optimizer_reward"]),
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
    if int(args.active_threads) != 30 or int(args.session_threads) != 30:
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
    compute_threads = {"active_bar": 30, "stock_session": 30}
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
    runtime = _runtime_envelope(30, 30)
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
        "schema_version": "cn_large_tpe_search_contract_v1",
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
        "formula_constructor_authority": "CompositionalGrammarV2",
        "compiler_authority": "TypedRouteCompiler",
        "matched_control_authority": "MATCHED_CONTROL_PAIR_AUTHORITY",
        "exact_memory": "CUMULATIVE_IDENTITY_ONLY",
        "behavior_memory": "CUMULATIVE_LABEL_FREE_AND_FULL_COORDINATE",
        "optimizer_feedback": "FULL_COORDINATE_TRAIN_ONLY",
        "optimizer_reward": "pair_optimizer_reward",
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
    adapters = {
        route_id: RouteConditionalTPESearchAdapter.replay(
            route_id=route_id,
            lane_spaces=lanes_by_route[route_id],
            seed=int(args.seed_base) + index * 1009,
            transcripts=state["transcripts"][route_id],
            n_startup_trials=STARTUP_TRIALS_BY_ROUTE[route_id],
            n_ei_candidates=N_EI_CANDIDATES,
        )
        for index, route_id in enumerate(ROUTES)
    }
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
            )
            schedule = {
                "checkpoint": checkpoint_id,
                "top_level_scheduling_key": "UNIFIED_REGISTRY_ROUTE_ID",
                "routes": [
                    {
                        "route_id": route_id,
                        "asked_pairs": count,
                        "generation_mode": "OPTUNA_TPE_TYPED_GRAMMAR",
                    }
                    for route_id, count in allocation.items()
                ],
            }
            _write_json(schedule_path, schedule)
        ask_path = root / "asked_population.json"
        expected_by_route: dict[str, list[dict[str, str]]] = {}
        if ask_path.is_file():
            previous_asked = json.loads(
                ask_path.read_text(encoding="utf-8-sig")
            )
            for route_id in ROUTES:
                expected_by_route[route_id] = [
                    dict(row["genes"])
                    for row in previous_asked
                    if str(row["route_id"]) == route_id
                ]
        asked: list[dict[str, Any]] = []
        for route in schedule["routes"]:
            route_id = str(route["route_id"])
            count = int(route["asked_pairs"])
            asked.extend(
                adapters[route_id].ask_population(
                    checkpoint_id=checkpoint_id,
                    count=count,
                    expected_genes=(
                        expected_by_route.get(route_id)
                        if ask_path.is_file()
                        else None
                    ),
                )
            )
        _write_json(ask_path, asked)
        materialized = _materialize_population(
            asked=asked,
            generator=generator,
            schema_by_backend=schema_by_backend,
        )
        observations: dict[str, dict[str, Any]] = {}
        unique_asked = []
        generation_exact: set[str] = set()
        for row in materialized:
            proposal_id = str(row["proposal_id"])
            identity = str(row.get("exact_identity") or "")
            if str(row.get("construction_status") or "") != "LEGAL":
                observations[proposal_id] = {
                    "proposal_id": proposal_id,
                    "outcome_class": "DETERMINISTIC_INVALID",
                    "optimizer_reward": None,
                    "outcome_reason": str(
                        row.get("construction_error")
                        or "DETERMINISTIC_INVALID"
                    ),
                }
            elif identity in state["exact"] or identity in generation_exact:
                observations[proposal_id] = {
                    "proposal_id": proposal_id,
                    "outcome_class": "EXACT_BLOCKED",
                    "optimizer_reward": None,
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
            if str(decision.get("admission_decision") or "") != "ADMIT":
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
                float(outcome["optimizer_reward"])
                if outcome_class == EVALUATED
                and outcome.get("optimizer_reward") is not None
                else None
            )
            observations[str(ask["proposal_id"])] = {
                "proposal_id": str(ask["proposal_id"]),
                "outcome_class": outcome_class,
                "optimizer_reward": reward,
                "outcome_reason": str(
                    outcome.get("pair_evaluation_blockers")
                    or "PAIR_EVALUATED"
                ),
            }
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
        for row in asked:
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
        summary = {
            "checkpoint": checkpoint_id,
            "asked_pairs": len(asked),
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
    parser.add_argument("--active-threads", type=int, default=30)
    parser.add_argument("--session-threads", type=int, default=30)
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
