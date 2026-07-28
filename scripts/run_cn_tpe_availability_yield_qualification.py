"""Zero-financial qualification for the CN route-local exact controller.

Layer A remaps the frozen historical proposal stream.  It is intentionally not
presented as a causal replay of adaptive TPE because no old reward or search
score is read.  Layer B runs only the existing label-free behavior probe on
training-period field sidecars.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import platform
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _artifact,
    _clock_for_route,
    _sha256,
    _stable_hash,
    _train_dates,
    _write_json,
    _write_parquet,
)
from our_system_phase2.runtime.cn_large_tpe_search_campaign import (
    AUTHORIZED_HOST,
    MINIMUM_FREE_MEMORY_BYTES,
    ROUTES,
    _materialized_route_root_allowlists,
)
from our_system_phase2.runtime.cn_search_policy_qualification import (
    _materialize_population,
)
from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    _admit_behavior_unique,
    _load_historical_dedupe,
    _registry_binding,
    materialized_schema_binding,
)
from our_system_phase2.services.fixed_split_authority import (
    FixedSplitAuthority,
)
from our_system_phase2.services.portfolio_behavior_archive import (
    bounded_label_free_behavior_probe,
)
from our_system_phase2.services.route_local_availability import (
    AvailabilityEmission,
    RouteLocalAvailabilityController,
    enumerate_authoritative_entries,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    RegistryDrivenGenerator,
    load_development_discovery_root_authority,
)


QUALIFICATION_SCHEMA_VERSION = "cn_tpe_availability_yield_qualification_v1"
LAYER_A_MODE = "FROZEN_PROPOSAL_STREAM_AVAILABILITY_REMAP"
BEHAVIOR_SAMPLE_SIZE = 2_048
MINIMUM_ROUTE_BEHAVIOR_SAMPLE = 128
MAXIMUM_INTERNAL_NATIVE_DRAWS_PER_FORMAL = 8
LEGACY_CLOSED_RAW_ASKS = 73_728
MAXIMUM_OPTIMIZER_DRAW_ATTEMPTS = LEGACY_CLOSED_RAW_ASKS * (
    MAXIMUM_INTERNAL_NATIVE_DRAWS_PER_FORMAL + 1
)
BEHAVIOR_COMPUTE_THREADS = 30
BEHAVIOR_PAIR_BATCH_SIZE = 4
BEHAVIOR_CACHE_CAP_BYTES = 4 * 1024**3
LEGACY_CLOSED_ROUTE_TARGETS = {
    "SLOW_TEMPORAL_CHANGE": 15_500,
    "FIRSTN_PATH": 2_000,
    "SLOW_CROSS_SECTIONAL_LEVEL": 1_200,
    "MARKET_REGIME_CONDITION": 800,
    "DISCLOSURE_EVENT": 500,
}


def _canonical_manifest_hash(manifest: Mapping[str, Any]) -> str:
    return _stable_hash(
        {
            key: copy.deepcopy(value)
            for key, value in manifest.items()
            if key != "manifest_payload_hash"
        }
    )


def _closed_campaign_authority_binding(
    *,
    path: Path,
    candidate_archive: Path,
    behavior_archive: Path,
    history_manifest: Path,
    seed_base: int,
) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    manifest = json.loads(
        history_manifest.read_text(encoding="utf-8-sig")
    )
    expected = {
        "execution_authorized": True,
        "authorized_host": AUTHORIZED_HOST,
        "campaign_profile": "cn_large_optuna_tpe_actual20000_v2",
        "optimizer": (
            "official_optuna.samplers.TPESampler_conditional_typed_grammar"
        ),
        "optimizer_package_version": "4.8.0",
        "route_actual_evaluated_targets": LEGACY_CLOSED_ROUTE_TARGETS,
        "minimum_actual_evaluated_pairs": 20_000,
        "maximum_raw_asks": 73_728,
        "seed_base": int(seed_base),
        "optimizer_search_score_policy": (
            "MIN_PRIMARY_COMPOSITE_AND_MATCHED_INCREMENT_V1"
        ),
        "portfolio_mode": "LONG_ONLY_TOP",
        "shorting": "FORBIDDEN",
        "one_way_cost_bps": 5,
        "horizons_minutes": [1, 5, 15, 30],
        "holdout": "SEALED",
        "forward_2026": "SEALED",
        "promotion": "FORBIDDEN",
    }
    drift = [
        key for key, value in expected.items() if payload.get(key) != value
    ]
    if (
        str(payload.get("historical_candidate_archive_sha256") or "")
        .lower()
        != _sha256(candidate_archive).lower()
        or str(payload.get("historical_behavior_archive_sha256") or "")
        .lower()
        != _sha256(behavior_archive).lower()
        or str(payload.get("historical_manifest_sha256") or "").lower()
        != _sha256(history_manifest).lower()
        or str(manifest.get("status") or "") != "PASS"
    ):
        drift.append("historical_identity_snapshot")
    if drift:
        raise RuntimeError(
            "QUALIFICATION_CLOSED_CAMPAIGN_AUTHORITY_DRIFT:"
            + ",".join(sorted(set(drift)))
        )
    return {
        "schema_version": (
            "cn_tpe_qualification_closed_campaign_binding_v1"
        ),
        "status": "CLOSED_CAMPAIGN_IDENTITY_EVIDENCE_BOUND",
        "authorization": _artifact(path),
        "historical_candidate_archive": _artifact(candidate_archive),
        "historical_behavior_archive": _artifact(behavior_archive),
        "historical_archive_manifest": _artifact(history_manifest),
        "frozen": expected,
    }


def _artifact_entry(
    manifest: Mapping[str, Any],
    relative_path: str,
) -> Mapping[str, Any]:
    matches = [
        row
        for row in manifest.get("artifacts") or ()
        if str(row.get("relative_path") or row.get("path") or "").replace(
            "\\", "/"
        ).endswith(relative_path.replace("\\", "/"))
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"QUALIFICATION_CHECKPOINT_ARTIFACT_COVERAGE:{relative_path}"
        )
    return matches[0]


def _verify_allowed_artifact(
    checkpoint_root: Path,
    manifest: Mapping[str, Any],
    name: str,
) -> Path:
    entry = _artifact_entry(manifest, name)
    path = checkpoint_root / name
    if (
        not path.is_file()
        or int(entry.get("bytes") or -1) != path.stat().st_size
        or str(entry.get("sha256") or "").lower() != _sha256(path).lower()
    ):
        raise RuntimeError(f"QUALIFICATION_ARTIFACT_HASH_DRIFT:{path}")
    return path


def _read_checkpoint_inputs(
    campaign_root: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read only proposal, exact/admission identity, and count evidence."""

    raw_rows: list[dict[str, Any]] = []
    checkpoint_rows: list[dict[str, Any]] = []
    original_seen_by_checkpoint: list[set[str]] = []
    checkpoint_index = 1
    prior_manifest_hash = "GENESIS"
    cumulative_evaluated = Counter()
    while True:
        checkpoint_id = f"checkpoint_{checkpoint_index:03d}"
        root = campaign_root / "checkpoints" / checkpoint_id
        manifest_path = root / "batch_manifest.json"
        if not manifest_path.is_file():
            break
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8-sig")
        )
        if (
            str(manifest.get("status") or "")
            != "BATCH_CLOSED_IMMUTABLE"
            or str(manifest.get("manifest_payload_hash") or "")
            != _canonical_manifest_hash(manifest)
            or str(
                (manifest.get("input_hashes") or {}).get(
                    "prior_checkpoint_manifest"
                )
                or ""
            )
            != prior_manifest_hash
        ):
            raise RuntimeError(
                f"QUALIFICATION_CHECKPOINT_MANIFEST_DRIFT:{checkpoint_id}"
            )
        if any(
            int(manifest.get(key) or 0) != 0
            for key in (
                "validation_reads",
                "holdout_reads",
                "forward_2026_reads",
            )
        ):
            raise RuntimeError(
                f"QUALIFICATION_SEALED_ACCESS_DRIFT:{checkpoint_id}"
            )
        ask_path = _verify_allowed_artifact(
            root, manifest, "asked_population.json"
        )
        candidates_path = _verify_allowed_artifact(
            root, manifest, "candidate_attempts.parquet"
        )
        decisions_path = _verify_allowed_artifact(
            root, manifest, "admission_decisions.parquet"
        )
        summary_path = _verify_allowed_artifact(
            root, manifest, "checkpoint_summary.json"
        )
        asked = json.loads(ask_path.read_text(encoding="utf-8-sig"))
        candidates = pd.read_parquet(
            candidates_path,
            columns=[
                "pair_id",
                "route_id",
                "pair_member_role",
                "exact_identity",
            ],
        ).where(pd.notna, None)
        candidate_records = candidates.to_dict(orient="records")
        primary_exact = {
            str(row.get("exact_identity") or "")
            for row in candidate_records
            if str(row.get("pair_member_role") or "") == "PRIMARY"
        }
        all_exact = {
            str(row.get("exact_identity") or "")
            for row in candidate_records
            if str(row.get("exact_identity") or "")
        }
        decisions = pd.read_parquet(
            decisions_path,
            columns=[
                "pair_id",
                "route_id",
                "admission_decision",
                "admission_reason",
            ],
        ).where(pd.notna, None)
        decision_records = decisions.to_dict(orient="records")
        admitted_by_route = Counter(
            str(row["route_id"])
            for row in decision_records
            if str(row.get("admission_decision") or "") == "ADMIT"
        )
        summary = json.loads(
            summary_path.read_text(encoding="utf-8-sig")
        )
        if (
            int(summary.get("asked_pairs") or -1) != len(asked)
            or int(summary.get("exact_unique_pairs") or -1)
            != len(primary_exact)
            or int(summary.get("behavior_admitted_pairs") or -1)
            != sum(admitted_by_route.values())
        ):
            raise RuntimeError(
                f"QUALIFICATION_LEDGER_COUNT_DRIFT:{checkpoint_id}"
            )
        current_cumulative = Counter(
            {
                str(key): int(value)
                for key, value in dict(
                    summary.get("evaluated_by_route") or {}
                ).items()
            }
        )
        incremental_evaluated = Counter(
            {
                route_id: current_cumulative[route_id]
                - cumulative_evaluated[route_id]
                for route_id in ROUTES
            }
        )
        if any(value < 0 for value in incremental_evaluated.values()):
            raise RuntimeError(
                f"QUALIFICATION_EVALUATED_COUNT_REGRESSION:{checkpoint_id}"
            )
        cumulative_evaluated = current_cumulative
        for ordinal, row in enumerate(asked):
            raw_rows.append(
                {
                    **copy.deepcopy(dict(row)),
                    "original_checkpoint": checkpoint_id,
                    "original_checkpoint_index": checkpoint_index,
                    "original_checkpoint_ordinal": ordinal,
                }
            )
        checkpoint_rows.append(
            {
                "checkpoint": checkpoint_id,
                "asked_count": len(asked),
                "primary_exact_identities": primary_exact,
                "all_candidate_exact_identities": all_exact,
                "behavior_admitted_by_route": admitted_by_route,
                "evaluated_by_route": incremental_evaluated,
                "manifest_sha256": _sha256(manifest_path),
                "allowed_input_hashes": {
                    "asked_population": _sha256(ask_path),
                    "candidate_attempts": _sha256(candidates_path),
                    "admission_decisions": _sha256(decisions_path),
                    "checkpoint_summary": _sha256(summary_path),
                },
            }
        )
        original_seen_by_checkpoint.append(all_exact)
        prior_manifest_hash = _sha256(manifest_path)
        checkpoint_index += 1
    if not checkpoint_rows:
        raise RuntimeError("QUALIFICATION_NO_CLOSED_CHECKPOINTS")
    if len(raw_rows) != LEGACY_CLOSED_RAW_ASKS:
        raise RuntimeError(
            "QUALIFICATION_HISTORICAL_DRAW_COUNT_DRIFT:"
            f"{len(raw_rows)}!={LEGACY_CLOSED_RAW_ASKS}"
        )
    return raw_rows, {
        "checkpoints": checkpoint_rows,
        "checkpoint_count": len(checkpoint_rows),
        "raw_asked_count": len(raw_rows),
        "final_manifest_sha256": prior_manifest_hash,
        "cumulative_evaluated_by_route": dict(cumulative_evaluated),
        "allowed_columns_only": True,
        "forbidden_reward_columns_read": [],
    }


def _wilson_interval(
    successes: int,
    trials: int,
    z: float = 1.959963984540054,
) -> tuple[float, float]:
    if trials <= 0:
        return 0.0, 1.0
    count = int(trials)
    success = min(count, max(0, int(successes)))
    rate = success / count
    denominator = 1.0 + z * z / count
    centre = rate + z * z / (2.0 * count)
    margin = z * math.sqrt(
        (rate * (1.0 - rate) + z * z / (4.0 * count)) / count
    )
    return (
        max(0.0, (centre - margin) / denominator),
        min(1.0, (centre + margin) / denominator),
    )


def _replay_layer_a(
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    ledger: Mapping[str, Any],
    generator: RegistryDrivenGenerator,
    schema_by_backend: Mapping[str, set[str]],
    controller: RouteLocalAvailabilityController,
    historical_exact: set[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_checkpoint: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in raw_rows:
        by_checkpoint[str(row["original_checkpoint"])].append(dict(row))
    emissions: list[dict[str, Any]] = []
    checkpoint_metrics = []
    original_seen = set(historical_exact)
    retired_streak = Counter()
    unfulfilled = Counter()
    original_exact_unique_total = 0
    for checkpoint in ledger["checkpoints"]:
        checkpoint_id = str(checkpoint["checkpoint"])
        materialized = _materialize_population(
            asked=by_checkpoint[checkpoint_id],
            generator=generator,
            schema_by_backend=schema_by_backend,
        )
        original_generation_seen: set[str] = set()
        original_primary = set()
        metrics = Counter()
        for row in materialized:
            route_id = str(row["route_id"])
            controller.record_optimizer_draw(route_id)
            metrics["optimizer_native_draws"] += 1
            identity = str(row.get("exact_identity") or "")
            legal = str(row.get("construction_status") or "") == "LEGAL"
            if (
                legal
                and identity not in original_seen
                and identity not in original_generation_seen
            ):
                original_generation_seen.add(identity)
                original_primary.add(identity)
                original_exact_unique_total += 1
            emission: AvailabilityEmission | None = None
            if legal:
                emission = controller.accept_direct(
                    route_id=route_id,
                    genes=dict(row["genes"]),
                    exact_identity=identity,
                )
                if emission is None:
                    emission = controller.emit_same_bucket(
                        route_id=route_id,
                        genes=dict(row["genes"]),
                        source_exact_identity=identity,
                    )
                    if emission is not None:
                        controller.record_optimizer_draw(route_id)
                        metrics["fixed_enqueued_trials"] += 1
            if emission is not None:
                retired_streak[route_id] = 0
                metrics[emission.emission_mode] += 1
                emissions.append(
                    {
                        **emission.to_dict(),
                        "original_checkpoint": checkpoint_id,
                        "original_checkpoint_index": int(
                            row["original_checkpoint_index"]
                        ),
                        "original_checkpoint_ordinal": int(
                            row["original_checkpoint_ordinal"]
                        ),
                    }
                )
                continue
            retired_streak[route_id] += 1
            metrics[
                "deterministic_invalid_or_exhausted_native_draws"
            ] += 1
            if (
                retired_streak[route_id]
                >= MAXIMUM_INTERNAL_NATIVE_DRAWS_PER_FORMAL
            ):
                fallback = controller.emit_global_fallback(
                    route_id=route_id,
                    source_exact_identity=identity,
                )
                retired_streak[route_id] = 0
                if fallback is None:
                    unfulfilled[route_id] += 1
                    metrics["unfulfilled_formal_requests"] += 1
                else:
                    controller.record_optimizer_draw(route_id)
                    metrics["fixed_enqueued_trials"] += 1
                    metrics[fallback.emission_mode] += 1
                    emissions.append(
                        {
                            **fallback.to_dict(),
                            "original_checkpoint": checkpoint_id,
                            "original_checkpoint_index": int(
                                row["original_checkpoint_index"]
                            ),
                            "original_checkpoint_ordinal": int(
                                row["original_checkpoint_ordinal"]
                            ),
                        }
                    )
        expected_primary = set(checkpoint["primary_exact_identities"])
        if original_primary != expected_primary:
            raise RuntimeError(
                "QUALIFICATION_HISTORICAL_EXACT_LEDGER_REPLAY_DRIFT:"
                f"{checkpoint_id}"
            )
        original_seen.update(checkpoint["all_candidate_exact_identities"])
        emitted_here = sum(
            metrics[mode]
            for mode in (
                "TPE_DIRECT_FRESH",
                "TPE_BUCKET_REPLACEMENT",
                "GLOBAL_AVAILABILITY_FALLBACK",
            )
        )
        checkpoint_metrics.append(
            {
                "checkpoint": checkpoint_id,
                **dict(metrics),
                "formal_fresh_exact_emitted": emitted_here,
                "formal_fresh_exact_yield": (
                    1.0 if emitted_here else None
                ),
            }
        )
    for route_id, streak in retired_streak.items():
        if streak and controller.remaining_count(route_id=route_id) == 0:
            unfulfilled[route_id] += 1
    emitted_ids = [str(row["exact_identity"]) for row in emissions]
    if len(emitted_ids) != len(set(emitted_ids)):
        raise RuntimeError("QUALIFICATION_FORMAL_EXACT_DUPLICATE")
    snapshot = controller.snapshot()
    fixed_count = sum(
        int(row.get("fixed_enqueued_trials") or 0)
        for row in checkpoint_metrics
    )
    total_optimizer_asks = len(raw_rows) + fixed_count
    mode_counts = Counter(str(row["emission_mode"]) for row in emissions)
    last_two = checkpoint_metrics[-2:]
    layer = {
        "schema_version": "cn_tpe_availability_layer_a_v1",
        "mode": LAYER_A_MODE,
        "causal_tpe_counterfactual": False,
        "financial_reward_columns_read": [],
        "historical_optimizer_native_draws": len(raw_rows),
        "conceptual_fixed_enqueued_trials": fixed_count,
        "optimizer_draw_attempts_including_fixed": total_optimizer_asks,
        "maximum_optimizer_draw_attempts": MAXIMUM_OPTIMIZER_DRAW_ATTEMPTS,
        "optimizer_draw_budget_status": (
            "PASS"
            if total_optimizer_asks <= MAXIMUM_OPTIMIZER_DRAW_ATTEMPTS
            else "FAIL"
        ),
        "formal_fresh_exact_asks": len(emissions),
        "internal_draws_per_formal_ask": (
            total_optimizer_asks / len(emissions) if emissions else None
        ),
        "original_exact_unique_count": original_exact_unique_total,
        "original_exact_unique_yield": (
            original_exact_unique_total / len(raw_rows)
        ),
        "emission_mode_counts": dict(mode_counts),
        "global_fallback_share": (
            mode_counts["GLOBAL_AVAILABILITY_FALLBACK"]
            / len(emissions)
            if emissions
            else None
        ),
        "tpe_guided_share": (
            (
                mode_counts["TPE_DIRECT_FRESH"]
                + mode_counts["TPE_BUCKET_REPLACEMENT"]
            )
            / len(emissions)
            if emissions
            else None
        ),
        "unfulfilled_formal_requests_by_route": dict(unfulfilled),
        "retired_bucket_count": len(snapshot["retired_buckets"]),
        "remaining_exact_by_route": snapshot["remaining_exact_by_route"],
        "remaining_count_by_bucket": snapshot[
            "remaining_count_by_bucket"
        ],
        "checkpoint_metrics": checkpoint_metrics,
        "last_two_checkpoint_formal_fresh_exact_yield": [
            row["formal_fresh_exact_yield"] for row in last_two
        ],
        "formal_exact_duplicate_count": 0,
        "controller_state_hash": snapshot["controller_state_hash"],
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    return emissions, layer


def _target_weighted_quotas(
    sample_size: int,
    route_targets: Mapping[str, int],
) -> dict[str, int]:
    base = MINIMUM_ROUTE_BEHAVIOR_SAMPLE
    residual = int(sample_size) - base * len(ROUTES)
    if residual < 0:
        raise ValueError("behavior sample below route floors")
    total_target = sum(route_targets.values())
    quotas = {route_id: base for route_id in ROUTES}
    fractional = []
    for route_id in ROUTES:
        raw = residual * int(route_targets[route_id]) / total_target
        whole = int(math.floor(raw))
        quotas[route_id] += whole
        fractional.append((raw - whole, route_id))
    for _, route_id in sorted(
        fractional, key=lambda row: (-row[0], row[1])
    )[: sample_size - sum(quotas.values())]:
        quotas[route_id] += 1
    return quotas


def _select_behavior_sample(
    *,
    emissions: Sequence[Mapping[str, Any]],
    qualification_seed: int,
    sample_size: int,
    route_targets: Mapping[str, int],
) -> list[dict[str, Any]]:
    quotas = _target_weighted_quotas(sample_size, route_targets)
    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    max_checkpoint = max(
        int(row["original_checkpoint_index"]) for row in emissions
    )
    for source in emissions:
        row = dict(source)
        index = int(row["original_checkpoint_index"])
        row["replay_stage"] = (
            "EARLY"
            if index <= max_checkpoint / 3
            else ("MIDDLE" if index <= 2 * max_checkpoint / 3 else "LATE")
        )
        row["qualification_rank"] = _stable_hash(
            {
                "qualification_seed": int(qualification_seed),
                "route_id": row["route_id"],
                "original_checkpoint": row["original_checkpoint"],
                "bucket_key": row["bucket_key"],
                "exact_identity": row["exact_identity"],
            }
        )
        by_route[str(row["route_id"])].append(row)
    for route_id in ROUTES:
        rows = sorted(
            by_route[route_id],
            key=lambda row: str(row["qualification_rank"]),
        )
        if len(rows) < quotas[route_id]:
            raise RuntimeError(
                f"QUALIFICATION_ROUTE_SAMPLE_SHORTFALL:{route_id}:"
                f"{len(rows)}<{quotas[route_id]}"
            )
        coverage = {}
        for row in rows:
            key = (row["replay_stage"], row["emission_mode"])
            coverage.setdefault(key, row)
        route_selected = sorted(
            coverage.values(),
            key=lambda row: str(row["qualification_rank"]),
        )[: quotas[route_id]]
        route_ids = {str(row["exact_identity"]) for row in route_selected}
        for row in rows:
            if len(route_selected) >= quotas[route_id]:
                break
            if str(row["exact_identity"]) not in route_ids:
                route_selected.append(row)
                route_ids.add(str(row["exact_identity"]))
        selected.extend(route_selected)
        selected_ids.update(route_ids)
    if len(selected) != sample_size or len(selected_ids) != sample_size:
        raise RuntimeError("QUALIFICATION_SAMPLE_EXACT_DUPLICATE")
    return sorted(
        selected,
        key=lambda row: (
            ROUTES.index(str(row["route_id"])),
            str(row["qualification_rank"]),
        ),
    )


def _probe_behavior_sample(
    *,
    selected: Sequence[Mapping[str, Any]],
    generator: RegistryDrivenGenerator,
    schema_by_backend: Mapping[str, set[str]],
    field_roots: Mapping[str, Path],
    train_dates: Sequence[str],
    historical_behavior: Any,
    qualification_seed: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    asked = [
        {
            "proposal_id": _stable_hash(
                {
                    "qualification_seed": qualification_seed,
                    "exact_identity": row["exact_identity"],
                }
            )[:24],
            "route_id": str(row["route_id"]),
            "category_id": str(row["qualification_rank"]),
            "genes": dict(row["genes"]),
        }
        for row in selected
    ]
    materialized = _materialize_population(
        asked=asked,
        generator=generator,
        schema_by_backend=schema_by_backend,
    )
    if any(
        str(row.get("construction_status") or "") != "LEGAL"
        or str(row.get("exact_identity") or "")
        != str(selected[index]["exact_identity"])
        for index, row in enumerate(materialized)
    ):
        raise RuntimeError("QUALIFICATION_SELECTED_MATERIALIZATION_DRIFT")
    candidates = [
        dict(member)
        for row in materialized
        for member in (row["primary"], row["control"])
    ]
    probes: list[dict[str, Any]] = []
    audits = []
    for route_id in ROUTES:
        members = [
            dict(row)
            for row in candidates
            if str(row.get("route_id") or "") == route_id
        ]
        backend = _clock_for_route(route_id)
        if route_id == "DISCLOSURE_EVENT":
            max_trade_dates, max_trade_times = 12, 1
            date_selection, time_selection = (
                "condition_activation",
                "session_open_head",
            )
        elif route_id == "FIRSTN_PATH":
            max_trade_dates, max_trade_times = 4, 8
            date_selection, time_selection = (
                "calendar_stratified",
                "intraday_stratified",
            )
        else:
            max_trade_dates, max_trade_times = 4, 8
            date_selection, time_selection = (
                "calendar_stratified",
                "session_open_head",
            )
        rows, audit = bounded_label_free_behavior_probe(
            candidates=members,
            field_sidecars=tuple(
                sorted(field_roots[backend].glob("shard_*.parquet"))
            ),
            eligible_trade_dates=train_dates,
            coordinate_binding=_stable_hash(
                {
                    "qualification_seed": qualification_seed,
                    "route_id": route_id,
                    "backend": backend,
                    "scope": "ZERO_FINANCIAL_BEHAVIOR_QUALIFICATION",
                }
            ),
            batch_id=f"availability_behavior_{route_id.lower()}",
            compute_threads=BEHAVIOR_COMPUTE_THREADS,
            max_trade_dates=max_trade_dates,
            max_trade_times=max_trade_times,
            date_selection=date_selection,
            time_selection=time_selection,
            pair_batch_size=BEHAVIOR_PAIR_BATCH_SIZE,
        )
        probes.extend(rows)
        audits.append({"route_id": route_id, "backend": backend, **audit})
    order = {
        str(row["exact_identity"]): index
        for index, row in enumerate(selected)
    }
    pair_to_exact = {
        str(row["pair_id"]): str(row["exact_identity"])
        for row in materialized
    }
    probes.sort(key=lambda row: order[pair_to_exact[str(row["pair_id"])]])
    admitted, decisions = _admit_behavior_unique(
        candidate_rows=candidates,
        probe_rows=probes,
        historical_archive=historical_behavior,
    )
    return candidates, decisions, {
        "probe_rows": probes,
        "probe_audits": audits,
        "pair_to_exact": pair_to_exact,
        "admitted_candidate_rows": len(admitted),
    }


def _layer_b_and_feasibility(
    *,
    selected: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    probe_audits: Sequence[Mapping[str, Any]],
    ledger: Mapping[str, Any],
    initial_remaining_by_route: Mapping[str, int],
    route_targets: Mapping[str, int],
    formal_ask_budget: int,
) -> dict[str, Any]:
    selected_by_exact = {
        str(row["exact_identity"]): dict(row) for row in selected
    }
    exact_by_pair: dict[str, str] = {}
    # Pair IDs are not in the emission row; the caller binds this before use.
    for row in decisions:
        exact = str(row.get("qualification_exact_identity") or "")
        if exact:
            exact_by_pair[str(row["pair_id"])] = exact
    sample_by_route = Counter(str(row["route_id"]) for row in selected)
    admitted_by_route = Counter(
        str(row["route_id"])
        for row in decisions
        if str(row.get("admission_decision") or "") == "ADMIT"
    )
    historical_admitted = Counter()
    historical_evaluated = Counter()
    for checkpoint in ledger["checkpoints"]:
        historical_admitted.update(checkpoint["behavior_admitted_by_route"])
        historical_evaluated.update(checkpoint["evaluated_by_route"])
    route_rows = {}
    projected_total = 0
    evidence_sufficient = True
    supply_sufficient = True
    for route_id in ROUTES:
        sample = sample_by_route[route_id]
        admitted = admitted_by_route[route_id]
        behavior_lcb, behavior_ucb = _wilson_interval(admitted, sample)
        evaluated = historical_evaluated[route_id]
        historical_admission = historical_admitted[route_id]
        if evaluated > historical_admission:
            raise RuntimeError(
                f"QUALIFICATION_HISTORICAL_COMPLETION_COUNT_DRIFT:"
                f"{route_id}:{evaluated}>{historical_admission}"
            )
        completion_lcb, completion_ucb = _wilson_interval(
            evaluated, historical_admission
        )
        conservative_conversion = behavior_lcb * completion_lcb
        projected = (
            math.ceil(
                int(route_targets[route_id])
                / conservative_conversion
            )
            if conservative_conversion > 0.0
            else None
        )
        if projected is None:
            evidence_sufficient = False
        else:
            projected_total += projected
        required_supply = (
            math.ceil(projected * 1.10)
            if projected is not None
            else None
        )
        route_supply_ok = (
            required_supply is not None
            and int(initial_remaining_by_route[route_id])
            >= required_supply
        )
        supply_sufficient = supply_sufficient and route_supply_ok
        if sample < MINIMUM_ROUTE_BEHAVIOR_SAMPLE:
            evidence_sufficient = False
        route_rows[route_id] = {
            "target_evaluated": int(route_targets[route_id]),
            "sample_formal_fresh_exact_asks": sample,
            "behavior_admitted": admitted,
            "behavior_admission_rate": admitted / sample if sample else None,
            "behavior_admission_wilson_95_lcb": behavior_lcb,
            "behavior_admission_wilson_95_ucb": behavior_ucb,
            "historical_evaluator_admitted": historical_admission,
            "historical_pair_evaluated": evaluated,
            "historical_completion_rate": (
                evaluated / historical_admission
                if historical_admission
                else None
            ),
            "historical_completion_wilson_95_lcb": completion_lcb,
            "historical_completion_wilson_95_ucb": completion_ucb,
            "conservative_formal_to_evaluated_conversion": (
                conservative_conversion
            ),
            "projected_formal_asks_required": projected,
            "initial_formal_fresh_exact_capacity": int(
                initial_remaining_by_route[route_id]
            ),
            "required_supply_with_10pct_margin": required_supply,
            "route_supply_margin_pass": route_supply_ok,
        }
    feasibility = (
        "INSUFFICIENT_ZERO_FINANCIAL_EVIDENCE"
        if not evidence_sufficient
        else (
            "FEASIBLE_WITHIN_FROZEN_FORMAL_BUDGET"
            if projected_total <= int(formal_ask_budget)
            and supply_sufficient
            else "INFEASIBLE_UNDER_CURRENT_ROUTE_TARGETS"
        )
    )
    return {
        "schema_version": "cn_tpe_availability_layer_b_v1",
        "sample_size": len(selected),
        "minimum_route_sample": MINIMUM_ROUTE_BEHAVIOR_SAMPLE,
        "route_rows": route_rows,
        "projected_formal_asks_required_total": projected_total,
        "formal_ask_budget_reference": int(formal_ask_budget),
        "projected_target_ask_feasibility": feasibility,
        "probe_audits": list(probe_audits),
        "label_sidecar_paths_accepted": 0,
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def _memory_sample() -> dict[str, int]:
    import psutil

    memory = psutil.virtual_memory()
    return {
        "available_memory_bytes": int(memory.available),
        "process_rss_bytes": int(psutil.Process().memory_info().rss),
    }


def _load_successor_target_contract(
    path: Path,
) -> tuple[dict[str, int], int, dict[str, Any]]:
    source = path.resolve()
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    unsigned = {
        key: copy.deepcopy(value)
        for key, value in payload.items()
        if key != "contract_hash"
    }
    targets = {
        str(key): int(value)
        for key, value in dict(
            payload.get("route_actual_evaluated_targets") or {}
        ).items()
    }
    formal_budget = int(
        payload.get("maximum_formal_fresh_exact_asks") or 0
    )
    if (
        str(payload.get("schema_version") or "")
        != "cn_large_tpe_successor_route_targets_v3"
        or str(payload.get("status") or "")
        != "FROZEN_FOR_ZERO_FINANCIAL_QUALIFICATION_ONLY"
        or bool(payload.get("execution_authorized"))
        or bool(payload.get("financial_campaign_authorized"))
        or set(targets) != set(ROUTES)
        or sum(targets.values())
        != int(payload.get("minimum_actual_evaluated_pairs") or 0)
        or formal_budget < 1
        or _stable_hash(unsigned)
        != str(payload.get("contract_hash") or "")
    ):
        raise RuntimeError("QUALIFICATION_TARGET_CONTRACT_DRIFT")
    return targets, formal_budget, payload


def _drain_formal_capacity(
    controller: RouteLocalAvailabilityController,
) -> dict[str, int]:
    capacity = Counter()
    for route_id in ROUTES:
        while True:
            emission = controller.emit_global_fallback(route_id=route_id)
            if emission is None:
                break
            capacity[route_id] += 1
    return {route_id: capacity[route_id] for route_id in ROUTES}


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"AVAILABILITY_QUALIFICATION_AUTHORIZED_ONLY_ON_77O:"
            f"{platform.node()}"
        )
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    if any(output_root.iterdir()):
        raise RuntimeError("QUALIFICATION_OUTPUT_ROOT_NOT_EMPTY")
    campaign_root = args.closed_campaign_root.resolve()
    route_targets, formal_ask_budget, target_contract = (
        _load_successor_target_contract(args.target_contract)
    )
    authority = _closed_campaign_authority_binding(
        path=args.campaign_authorization.resolve(),
        candidate_archive=args.historical_candidate_archive.resolve(),
        behavior_archive=args.historical_behavior_archive.resolve(),
        history_manifest=args.historical_archive_manifest.resolve(),
        seed_base=int(args.campaign_seed_base),
    )
    historical_exact, historical_behavior, archive_snapshot = (
        _load_historical_dedupe(
            candidate_archive_path=args.historical_candidate_archive.resolve(),
            behavior_archive_path=args.historical_behavior_archive.resolve(),
        )
    )
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    registry_binding = _registry_binding(args.registry.resolve(), registry)
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
        raise RuntimeError("QUALIFICATION_DISCOVERY_AUTHORITY_DRIFT")
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    field_roots = {
        "active_bar": args.active_field_root.resolve(),
        "stock_session": args.session_field_root.resolve(),
    }
    schema, schema_by_backend = materialized_schema_binding(
        field_roots=field_roots,
        registry=registry,
    )
    allowlists = _materialized_route_root_allowlists(
        registry=registry,
        discovery_allowlists=discovery["route_root_allowlists"],
        schema_by_backend=schema_by_backend,
    )
    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist=allowlists,
    )
    lanes_by_route = {
        route_id: dict(
            generator.categorical_gene_lanes(route_id)["lanes"]
        )
        for route_id in ROUTES
    }
    entries, index_report = enumerate_authoritative_entries(
        generator=generator,
        lanes_by_route=lanes_by_route,
        routes=ROUTES,
    )
    controller_inputs = {
        "historical_exact_archive": _sha256(
            args.historical_candidate_archive.resolve()
        ),
        "historical_behavior_archive": _sha256(
            args.historical_behavior_archive.resolve()
        ),
        "historical_archive_manifest": _sha256(
            args.historical_archive_manifest.resolve()
        ),
        "registry_file": str(registry_binding["registry_file_sha256"]),
        "registry_internal": str(
            registry_binding["internal_registry_hash"]
        ),
        "grammar": str(registry_binding["grammar_hash"]),
        "compiler": str(registry_binding["compiler_hash"]),
        "generator": str(registry_binding["generator_hash"]),
        "categorical_gene_lanes": _stable_hash(lanes_by_route),
        "closed_campaign_final_manifest": "PENDING_LEDGER_LOAD",
    }
    raw_rows, ledger = _read_checkpoint_inputs(campaign_root)
    closed_campaign_exact = {
        str(identity)
        for checkpoint in ledger["checkpoints"]
        for identity in checkpoint["all_candidate_exact_identities"]
    }
    controller_inputs["closed_campaign_final_manifest"] = str(
        ledger["final_manifest_sha256"]
    )
    controller = RouteLocalAvailabilityController(
        entries=entries,
        seen_exact_identities=historical_exact,
        emitter_seed=int(args.qualification_seed),
        input_hashes=controller_inputs,
    )
    initial_snapshot = controller.snapshot()
    future_controller = RouteLocalAvailabilityController(
        entries=entries,
        seen_exact_identities=historical_exact | closed_campaign_exact,
        emitter_seed=int(args.qualification_seed),
        input_hashes=controller_inputs,
    )
    future_initial_snapshot = future_controller.snapshot()
    future_formal_capacity_by_route = _drain_formal_capacity(
        future_controller
    )
    memory_samples = [_memory_sample()]
    if (
        memory_samples[-1]["available_memory_bytes"]
        < MINIMUM_FREE_MEMORY_BYTES
    ):
        raise RuntimeError("QUALIFICATION_FREE_MEMORY_BELOW_24_GIB")
    emissions, layer_a = _replay_layer_a(
        raw_rows=raw_rows,
        ledger=ledger,
        generator=generator,
        schema_by_backend=schema_by_backend,
        controller=controller,
        historical_exact=historical_exact,
    )
    layer_a_path = _write_json(output_root / "layer_a_replay.json", layer_a)
    emissions_path = _write_parquet(
        output_root / "layer_a_formal_emissions.parquet", emissions
    )
    controller_state_path = _write_json(
        output_root / "availability_controller_state.json",
        controller.snapshot(),
    )
    future_probe_pool = [
        row
        for row in emissions
        if str(row["emission_mode"])
        in {
            "TPE_BUCKET_REPLACEMENT",
            "GLOBAL_AVAILABILITY_FALLBACK",
        }
        and str(row["exact_identity"]) not in closed_campaign_exact
    ]
    selected = _select_behavior_sample(
        emissions=future_probe_pool,
        qualification_seed=int(args.qualification_seed),
        sample_size=int(args.behavior_sample_size),
        route_targets=route_targets,
    )
    selected_path = _write_parquet(
        output_root / "layer_b_selected_emissions.parquet", selected
    )
    candidates, decisions, probe_bundle = _probe_behavior_sample(
        selected=selected,
        generator=generator,
        schema_by_backend=schema_by_backend,
        field_roots=field_roots,
        train_dates=_train_dates(split),
        historical_behavior=historical_behavior,
        qualification_seed=int(args.qualification_seed),
    )
    pair_to_exact = dict(probe_bundle["pair_to_exact"])
    selected_by_exact = {
        str(row["exact_identity"]): row for row in selected
    }
    bound_decisions = []
    for source in decisions:
        row = dict(source)
        exact = pair_to_exact[str(row["pair_id"])]
        emission = selected_by_exact[exact]
        row["qualification_exact_identity"] = exact
        row["qualification_bucket_key"] = str(emission["bucket_key"])
        row["qualification_emission_mode"] = str(
            emission["emission_mode"]
        )
        bound_decisions.append(row)
        controller.record_behavior(
            route_id=str(row["route_id"]),
            admitted=str(row.get("admission_decision") or "") == "ADMIT",
            bucket_key=str(emission["bucket_key"]),
        )
    _write_json(
        controller_state_path,
        controller.snapshot(),
    )
    candidate_path = _write_parquet(
        output_root / "layer_b_candidates.parquet", candidates
    )
    probe_path = _write_parquet(
        output_root / "layer_b_behavior_probe.parquet",
        probe_bundle["probe_rows"],
    )
    decisions_path = _write_parquet(
        output_root / "layer_b_admission_decisions.parquet",
        bound_decisions,
    )
    probe_audit_path = _write_json(
        output_root / "layer_b_behavior_probe_audit.json",
        probe_bundle["probe_audits"],
    )
    layer_b = _layer_b_and_feasibility(
        selected=selected,
        decisions=bound_decisions,
        probe_audits=probe_bundle["probe_audits"],
        ledger=ledger,
        initial_remaining_by_route=future_formal_capacity_by_route,
        route_targets=route_targets,
        formal_ask_budget=formal_ask_budget,
    )
    memory_samples.append(_memory_sample())
    layer_b["runtime"] = {
        "compute_threads": BEHAVIOR_COMPUTE_THREADS,
        "pair_batch_size": BEHAVIOR_PAIR_BATCH_SIZE,
        "source_cache_cap_bytes": BEHAVIOR_CACHE_CAP_BYTES,
        "memory_samples": memory_samples,
        "minimum_sampled_free_memory_bytes": min(
            row["available_memory_bytes"] for row in memory_samples
        ),
        "maximum_sampled_process_rss_bytes": max(
            row["process_rss_bytes"] for row in memory_samples
        ),
    }
    if (
        layer_b["runtime"]["minimum_sampled_free_memory_bytes"]
        < MINIMUM_FREE_MEMORY_BYTES
    ):
        raise RuntimeError("QUALIFICATION_FREE_MEMORY_BELOW_24_GIB")
    layer_b_path = _write_json(
        output_root / "layer_b_yield_and_feasibility.json", layer_b
    )
    unfulfilled = sum(
        int(value)
        for value in layer_a[
            "unfulfilled_formal_requests_by_route"
        ].values()
    )
    mechanical = (
        "PASS"
        if layer_a["formal_exact_duplicate_count"] == 0
        else "FAIL"
    )
    formal_yield = (
        "SUPPLY_EXHAUSTED" if unfulfilled else "PASS"
    )
    behavior_yield = (
        "PASS"
        if all(
            int(row["sample_formal_fresh_exact_asks"])
            >= MINIMUM_ROUTE_BEHAVIOR_SAMPLE
            and float(row["behavior_admission_wilson_95_lcb"]) > 0.0
            for row in layer_b["route_rows"].values()
        )
        else "ROUTE_PARTIAL"
    )
    fallback_share = float(layer_a["global_fallback_share"] or 0.0)
    feasibility = str(
        layer_b["projected_target_ask_feasibility"]
    )
    supply_margin_pass = all(
        bool(row["route_supply_margin_pass"])
        for row in layer_b["route_rows"].values()
    )
    if mechanical != "PASS":
        readiness = "AVAILABILITY_CONTROLLER_BLOCKED"
    elif not supply_margin_pass:
        readiness = "SUPPLY_BLOCKED"
    elif feasibility == "INFEASIBLE_UNDER_CURRENT_ROUTE_TARGETS":
        readiness = "ROUTE_TARGET_REDESIGN_REQUIRED"
    elif (
        feasibility == "FEASIBLE_WITHIN_FROZEN_FORMAL_BUDGET"
        and fallback_share
        <= float(args.maximum_global_fallback_share)
        and behavior_yield == "PASS"
    ):
        readiness = "READY_FOR_SEPARATE_FINANCIAL_AUTHORIZATION"
    else:
        readiness = "ROUTE_TARGET_REDESIGN_REQUIRED"
    verdicts = {
        "AVAILABILITY_CONTROLLER_MECHANICAL": mechanical,
        "HISTORICAL_TAIL_REPLAY_RECOVERY": "PASS",
        "FORMAL_FRESH_EXACT_YIELD": formal_yield,
        "BEHAVIOR_ADMISSION_YIELD": behavior_yield,
        "PROJECTED_FROZEN_TARGET_ASK_FEASIBILITY": feasibility,
        "NOVELTY_AWARE_TPE_LARGE_SEARCH_READINESS": readiness,
    }
    access_path = _write_json(
        output_root / "zero_financial_access_receipt.json",
        {
            "financial_reads": 0,
            "optimizer_reward_columns_read": [],
            "search_score_columns_read": [],
            "matched_increment_columns_read": [],
            "primary_reward_columns_read": [],
            "label_sidecar_paths_accepted": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "phase3cm_calls": 0,
            "qualification_field_roots": {
                key: str(value) for key, value in field_roots.items()
            },
        },
    )
    verdict_path = _write_json(
        output_root / "qualification_verdicts.json",
        {
            "schema_version": QUALIFICATION_SCHEMA_VERSION,
            **verdicts,
            "tpe_agency": {
                "tpe_guided_share": layer_a["tpe_guided_share"],
                "global_fallback_share": fallback_share,
                "maximum_global_fallback_share_for_readiness": float(
                    args.maximum_global_fallback_share
                ),
            },
            "promotion": "FORBIDDEN",
        },
    )
    input_receipt_path = _write_json(
        output_root / "qualification_input_receipt.json",
        {
            "schema_version": QUALIFICATION_SCHEMA_VERSION,
            "authority": authority,
            "successor_target_contract": {
                "artifact": _artifact(args.target_contract.resolve()),
                "contract_hash": str(target_contract["contract_hash"]),
                "route_actual_evaluated_targets": route_targets,
                "minimum_actual_evaluated_pairs": sum(
                    route_targets.values()
                ),
                "maximum_formal_fresh_exact_asks": formal_ask_budget,
                "execution_authorized": False,
            },
            "archive_snapshot": archive_snapshot,
            "registry_binding": registry_binding,
            "materialized_schema": schema,
            "controller_inputs": controller_inputs,
            "availability_index": index_report,
            "layer_a_genesis_remaining_exact_by_route": initial_snapshot[
                "remaining_exact_by_route"
            ],
            "future_after_closed_campaign_remaining_exact_by_route": (
                future_initial_snapshot["remaining_exact_by_route"]
            ),
            "future_after_closed_campaign_formal_capacity_by_route": (
                future_formal_capacity_by_route
            ),
            "closed_campaign_exact_identity_count": len(
                closed_campaign_exact
            ),
            "closed_campaign_exact_identity_digest": _stable_hash(
                sorted(closed_campaign_exact)
            ),
            "layer_b_future_fresh_replacement_fallback_pool_count": len(
                future_probe_pool
            ),
            "closed_campaign_ledger": {
                key: value
                for key, value in ledger.items()
                if key != "checkpoints"
            },
            "split_manifest_hash": split.manifest_hash,
            "sidecar_closure_sha256": _sha256(
                args.sidecar_closure.resolve()
            ),
        },
    )
    artifacts = [
        input_receipt_path,
        layer_a_path,
        emissions_path,
        controller_state_path,
        selected_path,
        candidate_path,
        probe_path,
        decisions_path,
        probe_audit_path,
        layer_b_path,
        access_path,
        verdict_path,
    ]
    manifest = {
        "schema_version": "cn_tpe_availability_qualification_manifest_v1",
        "status": "QUALIFICATION_CLOSED_IMMUTABLE",
        "artifacts": [
            _artifact(path, root=output_root) for path in artifacts
        ],
        "verdicts": verdicts,
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "phase3cm_calls": 0,
        "promotion": "FORBIDDEN",
    }
    manifest["manifest_payload_hash"] = _stable_hash(manifest)
    manifest_path = _write_json(
        output_root / "artifact_manifest.json", manifest
    )
    return {
        "status": "QUALIFICATION_COMPLETE",
        "verdicts": verdicts,
        "artifact_manifest": str(manifest_path),
        "artifact_manifest_sha256": _sha256(manifest_path),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--closed-campaign-root", type=Path, required=True)
    parser.add_argument("--target-contract", type=Path, required=True)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
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
    parser.add_argument("--session-field-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--campaign-seed-base", type=int, default=2026072602)
    parser.add_argument("--qualification-seed", type=int, default=2026072801)
    parser.add_argument(
        "--behavior-sample-size",
        type=int,
        default=BEHAVIOR_SAMPLE_SIZE,
    )
    parser.add_argument(
        "--maximum-global-fallback-share",
        type=float,
        default=0.20,
    )
    args = parser.parse_args(argv)
    if int(args.behavior_sample_size) != BEHAVIOR_SAMPLE_SIZE:
        raise RuntimeError("QUALIFICATION_BEHAVIOR_SAMPLE_MUST_BE_2048")
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
