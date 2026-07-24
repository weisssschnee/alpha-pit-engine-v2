"""Run the bounded one-family Formula-Space/CEM qualification.

The target is frozen to ``DISCLOSURE_EVENT/pre_event_path``.  This script
projects the current Registry/Grammar lane, opens one compiler-qualified
normalizer-placement extension, and compares:

* Arm A: uniform old space
* Arm B: uniform expanded space
* Arm C: categorical CEM over the exact same expanded space

It reuses the existing behavior probe, matched-control binding, Phase3CM
development evaluator, runtime telemetry, and immutable checkpoint format.
It never reads validation, holdout, or 2026 data and never promotes a
candidate.
"""

from __future__ import annotations

import argparse
import copy
import itertools
import json
import math
import os
import platform
import re
import statistics
import time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _artifact,
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
from our_system_phase2.runtime.cn_search_policy_qualification import (
    _copy_behavior_archive,
    _minimum_free_memory,
    _outcome_class,
    _phase3cm_wall_seconds,
    _read_rows,
    _source_campaign_binding,
    _verify_artifacts,
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
from our_system_phase2.services.categorical_cem import (
    CategoricalCEMPolicy,
)
from our_system_phase2.services.compositional_grammar import (
    skeleton_registry,
)
from our_system_phase2.services.fixed_split_authority import (
    FixedSplitAuthority,
)
from our_system_phase2.services.phase3cm_streaming_expression import (
    unsupported_streaming_operators,
)
from our_system_phase2.services.portfolio_behavior_archive import (
    BEHAVIOR_VERSION,
    PortfolioBehaviorArchive,
)
from our_system_phase2.services.search_choice_policy import (
    EXPANDED_FORMULA_SPACE_ID,
    HISTORICAL_REJECTED_EXTENSION_IDS,
    OLD_FORMULA_SPACE_ID,
    PRE_EVENT_PAYLOAD_ABS_EXTENSION_ID,
    PRE_EVENT_PAYLOAD_CSRANK_EXTENSION_ID,
    PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID,
    TARGETED_RETRY_EXTENSION_IDS,
    LegacyParityPolicy,
    TargetedFormulaProjection,
    UniformPolicy,
    stable_hash,
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
    GeneratedPair,
    RegistryDrivenGenerator,
    load_development_discovery_root_authority,
)


REPO = Path(__file__).resolve().parents[1]
AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
ROUTE_ID = "DISCLOSURE_EVENT"
SKELETON_ID = "cn.comp.v2.disclosure_event.pre_event_path"
ARMS = ("arm_a_uniform_old", "arm_b_uniform_expanded", "arm_c_cem_expanded")
CHECKPOINT_COUNT = 3
RAW_ATTEMPT_CAP = 2048
COMPILE_VALID_CAP = 512
BEHAVIOR_PROBE_CAP = 128
SAMPLED_SELECTION_CAP = 48
FULL_PAIR_CAP = 24
STATIC_ATTEMPT_CAP = 4096
STATIC_COMPARABLE_ATTEMPTS = 1000
STATIC_BEHAVIOR_PROBE_CAP = 256
MINIMUM_EXACT_SUPPLY = 144
MINIMUM_BEHAVIOR_SUPPLY = 72
MINIMUM_EVALUATED_PAIRS = 48
MINIMUM_ACTIVE_CHECKPOINTS = 2
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
MAXIMUM_CACHE_BYTES = 8 * 1024**3
PAIR_BATCH_SIZES = {"active_bar": 4, "stock_session": 4}
RETRY_AUTHORIZATION_ID = "DISCLOSURE_PRE_EVENT_EXTENSION_RETRY_V2"
REQUIRED_RETRY_BEHAVIOR_UNIQUE = 114


def _formula_space_for_arm(arm: str) -> str:
    return (
        OLD_FORMULA_SPACE_ID
        if arm == "arm_a_uniform_old"
        else EXPANDED_FORMULA_SPACE_ID
    )


def _policy_id_for_arm(arm: str) -> str:
    return (
        "categorical_cem_v1"
        if arm == "arm_c_cem_expanded"
        else "uniform_choice_v1"
    )


def _shape(expression: str) -> str:
    return re.sub(r"\$[A-Za-z0-9_.]+", "$FIELD", str(expression))


def _expression_depth(expression: str) -> int:
    depth = maximum = 0
    for character in str(expression):
        if character == "(":
            depth += 1
            maximum = max(maximum, depth)
        elif character == ")":
            depth -= 1
    return maximum


def _projection(
    *,
    registry: UnifiedCapabilityRegistry,
    route_root_allowlist: Mapping[str, Sequence[str]],
    extension_id: str,
) -> TargetedFormulaProjection:
    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist=route_root_allowlist,
    )
    return TargetedFormulaProjection(
        generator=generator,
        route_id=ROUTE_ID,
        skeleton_id=SKELETON_ID,
        extension_id=extension_id,
    )


def _selection_policy(
    decisions: Sequence[Any],
    selected_tokens: Sequence[str],
) -> LegacyParityPolicy:
    return LegacyParityPolicy(
        {
            decision.decision_id: token_id
            for decision, token_id in zip(decisions, selected_tokens)
        }
    )


def _enumerate_space(
    projection: TargetedFormulaProjection,
    formula_space_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    decisions = projection.decision_specs(formula_space_id)
    combinations = tuple(
        itertools.product(
            *(
                tuple(choice.token_id for choice in decision.ordered_choices)
                for decision in decisions
            )
        )
    )
    if not combinations:
        raise RuntimeError(f"EMPTY_FORMULA_SPACE:{formula_space_id}")
    attempt_count = min(STATIC_ATTEMPT_CAP, STATIC_COMPARABLE_ATTEMPTS)
    rows: list[dict[str, Any]] = []
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    for ordinal in range(attempt_count):
        selected = combinations[ordinal % len(combinations)]
        pair = projection.generate(
            formula_space_id=formula_space_id,
            policy=_selection_policy(decisions, selected),
            rng=np.random.default_rng(0),
        )
        primary = dict(pair.candidate)
        control = dict(pair.control)
        rows.append(
            {
                "ordinal": ordinal,
                "pair_id": str(primary.get("pair_id") or ""),
                "exact_identity": str(primary.get("exact_identity") or ""),
                "canonical_identity": str(
                    primary.get("canonical_identity") or ""
                ),
                "legal": bool(primary.get("legal"))
                and bool(control.get("legal")),
                "primary": primary,
                "control": control,
                "primary_shape": _shape(primary["expression"]),
                "control_shape": _shape(control["expression"]),
                "depth": max(
                    _expression_depth(primary["expression"]),
                    _expression_depth(control["expression"]),
                ),
                "decision_trace": copy.deepcopy(
                    primary["decision_trace"]
                ),
            }
        )
    wall = time.perf_counter() - started_wall
    cpu = time.process_time() - started_cpu
    legal = [row for row in rows if row["legal"]]
    exact = {
        str(row["exact_identity"])
        for row in legal
        if str(row["exact_identity"])
    }
    canonical = {
        str(row["canonical_identity"])
        for row in legal
        if str(row["canonical_identity"])
    }
    shapes = {
        (str(row["primary_shape"]), str(row["control_shape"]))
        for row in legal
    }
    selected_tokens = [
        str(trace["selected_token_id"])
        for row in rows
        for trace in row["decision_trace"]
    ]
    token_counts = Counter(selected_tokens)
    metrics = {
        "formula_space_id": formula_space_id,
        "enumeration_mode": "DETERMINISTIC_CYCLIC_FULL_STRATUM",
        "space_cardinality": len(combinations),
        "raw_attempts": len(rows),
        "compile_valid_pairs": len(legal),
        "control_valid_pairs": len(legal),
        "semantic_block_pairs": len(rows) - len(legal),
        "exact_unique_pairs": len(exact),
        "canonical_unique_pairs": len(canonical),
        "ast_shape_unique_pairs": len(shapes),
        "compile_valid_rate": len(legal) / max(1, len(rows)),
        "control_valid_rate": len(legal) / max(1, len(rows)),
        "canonical_unique_per_attempt": len(canonical) / max(1, len(rows)),
        "ast_shape_unique_per_attempt": len(shapes) / max(1, len(rows)),
        "duplicate_rate": 1.0 - len(exact) / max(1, len(legal)),
        "depth_distribution": dict(
            Counter(str(row["depth"]) for row in legal)
        ),
        "maximum_token_share": (
            max(token_counts.values(), default=0)
            / max(1, len(selected_tokens))
        ),
        "static_generation_wall_seconds": wall,
        "static_generation_cpu_seconds": cpu,
        "raw_attempts_per_second": len(rows) / max(wall, 1e-9),
        "compile_valid_per_second": len(legal) / max(wall, 1e-9),
    }
    return rows, metrics


def _post_archive_exact_unique_rows(
    rows: Sequence[Mapping[str, Any]],
    historical_exact: set[str],
) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    canonical_seen: set[str] = set()
    for raw in rows:
        row = dict(raw)
        exact_identity = str(row.get("exact_identity") or "")
        canonical_identity = str(row.get("canonical_identity") or "")
        if (
            not bool(row.get("legal"))
            or not exact_identity
            or not canonical_identity
            or exact_identity in historical_exact
            or canonical_identity in canonical_seen
        ):
            continue
        selected.setdefault(exact_identity, row)
        canonical_seen.add(canonical_identity)
    return list(selected.values())


def _acquire_campaign_writer(
    *,
    output_root: Path,
    task_id: str,
) -> tuple[Path, Path]:
    receipt_path = output_root / "campaign_launch_receipt.json"
    if not receipt_path.is_file():
        raise RuntimeError("CAMPAIGN_LAUNCH_RECEIPT_MISSING")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8-sig"))
    expected = {
        "task_id": task_id,
        "launcher_mode": "A_IMMEDIATE_START_NO_SCHEDULED_TRIGGER",
        "launch_event_count": 1,
    }
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise RuntimeError("CAMPAIGN_LAUNCH_RECEIPT_DRIFT")
    if (
        Path(str(receipt.get("campaign_root") or "")).resolve()
        != output_root
    ):
        raise RuntimeError("CAMPAIGN_LAUNCH_ROOT_DRIFT")
    if (output_root / "artifact_manifest.json").exists():
        raise RuntimeError("CAMPAIGN_ALREADY_CLOSED")
    lock_path = output_root / "campaign_writer.lock"
    lock = {
        "schema_version": "cn_single_campaign_writer_lock_v1",
        "status": "ACTIVE",
        "task_id": task_id,
        "pid": os.getpid(),
        "host": platform.node(),
        "campaign_root": output_root.as_posix(),
        "launch_receipt_sha256": _sha256(receipt_path),
    }
    try:
        with lock_path.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(lock, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    except FileExistsError as exc:
        raise RuntimeError(
            "DUPLICATE_CAMPAIGN_LAUNCH_OR_ACTIVE_WRITER"
        ) from exc
    return receipt_path, lock_path


def _close_campaign_writer(lock_path: Path, *, task_id: str) -> None:
    lock = json.loads(lock_path.read_text(encoding="utf-8-sig"))
    if (
        str(lock.get("status") or "") != "ACTIVE"
        or str(lock.get("task_id") or "") != task_id
        or int(lock.get("pid") or -1) != os.getpid()
    ):
        raise RuntimeError("CAMPAIGN_WRITER_LOCK_OWNERSHIP_DRIFT")
    _write_json(
        lock_path,
        {
            **lock,
            "status": "CLOSED",
            "closed_by_pid": os.getpid(),
        },
    )


def _resolve_sampled_evaluator_authority(
    output_root: Path,
) -> tuple[dict[str, Any], Path]:
    checked_roots = (
        REPO / "runtime" / "run_plans",
        REPO / "config",
    )
    checked_files = 0
    declared = []
    qualified = []
    for root in checked_roots:
        if not root.is_dir():
            continue
        for path in sorted(root.glob("*.json")):
            checked_files += 1
            try:
                payload = json.loads(
                    path.read_text(encoding="utf-8-sig")
                )
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if (
                str(payload.get("authority_role") or "")
                != "development_sampled_evaluator"
            ):
                continue
            row = {
                "path": path.relative_to(REPO).as_posix(),
                "sha256": _sha256(path),
                "lifecycle_state": str(
                    payload.get("lifecycle_state") or ""
                ),
                "evidence_state": str(
                    payload.get("evidence_state") or ""
                ),
                "evaluation_role": str(
                    payload.get("evaluation_role") or ""
                ),
            }
            declared.append(row)
            if (
                row["lifecycle_state"] in {"ACTIVE", "CURRENT"}
                and row["evidence_state"] == "QUALIFIED"
                and row["evaluation_role"] == "development"
            ):
                qualified.append(row)
    result = {
        "schema_version": (
            "cn_sampled_evaluator_authority_resolution_v1"
        ),
        "status": (
            "PASS_EXISTING_QUALIFIED_AUTHORITY"
            if len(qualified) == 1
            else "NOT_AVAILABLE_NO_EXISTING_AUTHORITY"
        ),
        "authority_role": "development_sampled_evaluator",
        "checked_roots": [
            root.relative_to(REPO).as_posix() for root in checked_roots
        ],
        "checked_json_file_count": checked_files,
        "declared_authorities": declared,
        "qualified_current_authorities": qualified,
        "surrogate_created": False,
    }
    if len(qualified) > 1:
        raise RuntimeError("MULTIPLE_SAMPLED_EVALUATOR_AUTHORITIES")
    path = _write_json(
        output_root / "sampled_evaluator_authority_resolution.json",
        result,
    )
    if qualified:
        raise RuntimeError(
            "SAMPLED_EVALUATOR_AUTHORITY_PRESENT_REQUIRES_PARITY_PATH"
        )
    return result, path


def _legacy_parity(
    projection: TargetedFormulaProjection,
    *,
    seed: int,
    sample_count: int = 128,
) -> list[dict[str, Any]]:
    rows = []
    route_skeletons = skeleton_registry()[ROUTE_ID]
    target_index = next(
        index
        for index, skeleton in enumerate(route_skeletons)
        if skeleton.skeleton_id == SKELETON_ID
    )
    for ordinal in range(sample_count):
        attempt_index = target_index + ordinal * len(route_skeletons)
        legacy = projection.generator.propose_attempt(
            ROUTE_ID,
            attempt_index=attempt_index,
            seed=seed,
        )
        selected = projection.legacy_selection_from_pair(legacy)
        replayed = projection.generate(
            formula_space_id=OLD_FORMULA_SPACE_ID,
            policy=LegacyParityPolicy(selected),
            rng=np.random.default_rng(seed + ordinal),
        )
        checks = {
            "primary_exact": (
                legacy.candidate["exact_identity"]
                == replayed.candidate["exact_identity"]
            ),
            "control_exact": (
                legacy.control["exact_identity"]
                == replayed.control["exact_identity"]
            ),
            "primary_canonical": (
                legacy.candidate["canonical_identity"]
                == replayed.candidate["canonical_identity"]
            ),
            "control_canonical": (
                legacy.control["canonical_identity"]
                == replayed.control["canonical_identity"]
            ),
            "declared_fields": (
                legacy.candidate["declared_field_ids"]
                == replayed.candidate["declared_field_ids"]
                and legacy.control["declared_field_ids"]
                == replayed.control["declared_field_ids"]
            ),
            "matched_control": (
                legacy.candidate["control_constructor_id"]
                == replayed.candidate["control_constructor_id"]
            ),
            "route_skeleton_clock_maturity": all(
                legacy.candidate[key] == replayed.candidate[key]
                for key in (
                    "route_id",
                    "skeleton_id",
                    "clock_contract",
                    "maturity_contract",
                )
            ),
        }
        rows.append(
            {
                "attempt_index": attempt_index,
                "seed": seed,
                "status": (
                    "EXACT_PARITY"
                    if all(checks.values())
                    else "FAIL"
                ),
                **checks,
                "legacy_primary_exact_identity": legacy.candidate[
                    "exact_identity"
                ],
                "replayed_primary_exact_identity": replayed.candidate[
                    "exact_identity"
                ],
            }
        )
    if not rows or any(row["status"] != "EXACT_PARITY" for row in rows):
        raise RuntimeError("LEGACY_PRODUCTION_PROJECTION_NOT_PROVEN")
    return rows


def _static_verdict(
    old: Mapping[str, Any],
    expanded: Mapping[str, Any],
    expanded_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    target = next(
        row
        for row in skeleton_registry()[ROUTE_ID]
        if row.skeleton_id == SKELETON_ID
    )
    legal_rows = [row for row in expanded_rows if bool(row.get("legal"))]
    checks = {
        "compile_valid_100_percent": (
            float(expanded["compile_valid_rate"]) == 1.0
        ),
        "control_valid_100_percent": (
            float(expanded["control_valid_rate"]) == 1.0
        ),
        "semantic_authority": bool(legal_rows)
        and len(legal_rows) == len(expanded_rows),
        "route_operator_authority": bool(legal_rows)
        and all(
            str(row["primary"].get("route_id") or "") == ROUTE_ID
            and str(row["control"].get("route_id") or "") == ROUTE_ID
            and not unsupported_streaming_operators(
                (
                    str(row["primary"].get("expression") or ""),
                    str(row["control"].get("expression") or ""),
                )
            )
            for row in legal_rows
        ),
        "primary_control_field_lineage": bool(legal_rows)
        and all(
            set(map(str, row["primary"].get("declared_field_ids") or ()))
            == set(map(str, row["control"].get("declared_field_ids") or ()))
            for row in legal_rows
        ),
        "maximum_depth": bool(legal_rows)
        and max(int(row["depth"]) for row in legal_rows)
        <= int(target.maximum_depth),
        "streaming_operator_support": bool(legal_rows)
        and all(
            not unsupported_streaming_operators(
                (
                    str(row["primary"].get("expression") or ""),
                    str(row["control"].get("expression") or ""),
                )
            )
            for row in legal_rows
        ),
        "exact_space_increment": int(expanded["exact_unique_pairs"])
        > int(old["exact_unique_pairs"]),
        "canonical_space_increment": int(
            expanded["canonical_unique_pairs"]
        )
        > int(old["canonical_unique_pairs"]),
        "ast_shape_increment": int(expanded["ast_shape_unique_pairs"])
        > int(old["ast_shape_unique_pairs"]),
        "validation_holdout_2026_reads_zero": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "maximum_allowed_depth": int(target.maximum_depth),
    }


def _qualification_gate_status(
    *,
    exact_gate: bool,
    behavior_gate: bool,
    formula_space_behavior_gate: bool,
) -> str:
    if not exact_gate or not behavior_gate:
        return "TARGET_FAMILY_SUPPLY_NOT_PROVEN"
    if not formula_space_behavior_gate:
        return "FORMULA_SPACE_INCREMENT_NOT_PROVEN"
    return "PASS"


def _bind_reused_old_authority(
    *,
    authorization: Mapping[str, Any],
    output_root: Path,
    projection: TargetedFormulaProjection,
    registry: UnifiedCapabilityRegistry,
    discovery_contract: Path,
    source_binding: Mapping[str, Any],
    base_seed: int,
) -> tuple[dict[str, Any], dict[str, Any], int, Path]:
    """Verify and reuse the frozen OLD result without re-running it."""

    authority = dict(authorization.get("old_authority") or {})
    evidence_root = Path(str(authority.get("evidence_root") or "")).resolve()
    artifact_hashes = {
        str(path): str(sha256)
        for path, sha256 in dict(
            authority.get("artifact_sha256") or {}
        ).items()
    }
    drift: list[str] = []
    for relative_path, expected_hash in artifact_hashes.items():
        path = evidence_root / relative_path
        if not path.is_file() or _sha256(path) != expected_hash:
            drift.append(f"artifact:{relative_path}")

    current_old_catalog = projection.decision_catalog(
        OLD_FORMULA_SPACE_ID
    )
    expected_bindings = {
        "formula_space_id": OLD_FORMULA_SPACE_ID,
        "decision_catalog_hash": str(
            current_old_catalog["decision_catalog_hash"]
        ),
        "registry_hash": registry.registry_hash,
        "development_discovery_contract_sha256": _sha256(
            discovery_contract
        ),
        "historical_exact_archive_sha256": str(
            (
                source_binding.get("historical_candidate_archive")
                or {}
            ).get("sha256")
            or ""
        ),
        "historical_behavior_archive_sha256": str(
            (source_binding.get("behavior_archive") or {}).get(
                "sha256"
            )
            or ""
        ),
        "behavior_identity_version": BEHAVIOR_VERSION,
        "old_probe_seed": int(base_seed) + 101,
        "old_probe_order_contract": (
            "sha256(json(seed,exact_identity));ascending"
        ),
    }
    for key, actual in expected_bindings.items():
        if authority.get(key) != actual:
            drift.append(key)

    if drift:
        raise RuntimeError(
            "OLD_AUTHORITY_BINDING_DRIFT:" + ",".join(sorted(drift))
        )

    old_static_rows = _read_rows(
        evidence_root / "static_generation_metrics.parquet"
    )
    old_static = next(
        dict(row)
        for row in old_static_rows
        if str(row.get("formula_space_id") or "")
        == OLD_FORMULA_SPACE_ID
    )
    old_behavior_rows = _read_rows(
        evidence_root / "behavior_probe_metrics.parquet"
    )
    old_behavior = next(
        dict(row)
        for row in old_behavior_rows
        if str(row.get("mode") or "") == "OLD_UNIFORM"
    )
    old_supply = json.loads(
        (evidence_root / "supply_and_behavior_gate.json").read_text(
            encoding="utf-8-sig"
        )
    )
    admission_rows = _read_rows(
        evidence_root
        / "static_behavior"
        / "old_uniform"
        / "admission_decisions.parquet"
    )
    coordinate_bindings = {
        str(row.get("coordinate_binding") or "")
        for row in admission_rows
    }
    behavior_versions = {
        str(row.get("behavior_identity_version") or "")
        for row in admission_rows
    }
    observed = {
        "post_archive_exact_supply": int(
            old_supply["post_archive_old_space_exact_supply"]
        ),
        "behavior_probe_candidates": int(
            old_behavior["behavior_probe_candidates"]
        ),
        "behavior_unique_pairs": int(
            old_behavior["behavior_unique_pairs"]
        ),
        "ast_shape_unique_pairs": int(
            old_static["ast_shape_unique_pairs"]
        ),
        "probe_coordinate_binding": (
            next(iter(coordinate_bindings))
            if len(coordinate_bindings) == 1
            else ""
        ),
        "behavior_identity_version": (
            next(iter(behavior_versions))
            if len(behavior_versions) == 1
            else ""
        ),
    }
    expected_observed = {
        key: authority.get(key)
        for key in observed
    }
    observed_drift = [
        key
        for key, value in observed.items()
        if expected_observed[key] != value
    ]
    if observed_drift:
        raise RuntimeError(
            "OLD_AUTHORITY_EVIDENCE_DRIFT:"
            + ",".join(sorted(observed_drift))
        )

    receipt = {
        "schema_version": "cn_old_formula_space_reuse_receipt_v1",
        "status": "VERIFIED_REUSED_NOT_RECOMPUTED",
        "authority": authority,
        "observed": observed,
        "current_semantic_bindings": expected_bindings,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    receipt["receipt_hash"] = _stable_hash(receipt)
    receipt_path = _write_json(
        output_root / "old_authority_reuse_receipt.json", receipt
    )
    return (
        old_static,
        old_behavior,
        int(observed["post_archive_exact_supply"]),
        receipt_path,
    )


def _pair_members(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        dict(member)
        for row in rows
        for member in (row["primary"], row["control"])
    ]


def _probe_rows(
    *,
    root: Path,
    batch_id: str,
    candidate_pairs: Sequence[Mapping[str, Any]],
    field_roots: Mapping[str, Path],
    split: FixedSplitAuthority,
    compute_threads: Mapping[str, int],
    coordinate_binding: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], float]:
    members = _pair_members(candidate_pairs)
    started = time.perf_counter()
    probe, audit = _probe_pack(
        candidate_rows=members,
        field_roots=field_roots,
        train_dates=_train_dates(split),
        coordinate_binding=coordinate_binding,
        batch_id=batch_id,
        compute_threads=compute_threads,
    )
    wall = time.perf_counter() - started
    _write_parquet(root / "behavior_probe.parquet", probe)
    _write_json(root / "behavior_probe_audit.json", audit)
    return probe, audit, wall


def _deterministic_pair_order(
    rows: Sequence[Mapping[str, Any]],
    *,
    seed: int,
) -> list[dict[str, Any]]:
    return sorted(
        (copy.deepcopy(dict(row)) for row in rows),
        key=lambda row: stable_hash(
            {
                "seed": int(seed),
                "exact_identity": str(row.get("exact_identity") or ""),
            }
        ),
    )


def _static_behavior_probe(
    *,
    output_root: Path,
    mode: str,
    rows: Sequence[Mapping[str, Any]],
    seed: int,
    historical: PortfolioBehaviorArchive,
    field_roots: Mapping[str, Path],
    split: FixedSplitAuthority,
    compute_threads: Mapping[str, int],
    coordinate_binding: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    root = output_root / "static_behavior" / mode.lower()
    root.mkdir(parents=True, exist_ok=True)
    selected = _deterministic_pair_order(
        [row for row in rows if bool(row.get("legal"))],
        seed=seed,
    )[:STATIC_BEHAVIOR_PROBE_CAP]
    probe, _, wall = _probe_rows(
        root=root,
        batch_id=f"static.{mode.lower()}",
        candidate_pairs=selected,
        field_roots=field_roots,
        split=split,
        compute_threads=compute_threads,
        coordinate_binding=coordinate_binding,
    )
    admitted, decisions = _admit_behavior_unique(
        candidate_rows=_pair_members(selected),
        probe_rows=probe,
        historical_archive=_copy_behavior_archive(historical),
    )
    _write_parquet(root / "admission_decisions.parquet", decisions)
    admitted_count = sum(
        str(row.get("pair_member_role") or "") == "PRIMARY"
        for row in admitted
    )
    metrics = {
        "mode": mode,
        "behavior_probe_candidates": len(selected),
        "behavior_unique_pairs": admitted_count,
        "behavior_unique_per_candidate": admitted_count
        / max(1, len(selected)),
        "behavior_duplicate_pairs": len(selected) - admitted_count,
        "behavior_probe_wall_seconds": wall,
        "behavior_probes_per_second": len(selected) / max(wall, 1e-9),
    }
    _write_json(root / "behavior_probe_metrics.json", metrics)
    return metrics, probe


def _materializable_allowlist(
    *,
    registry: UnifiedCapabilityRegistry,
    discovery_allowlist: Sequence[str],
    available_fields: set[str],
) -> tuple[str, ...]:
    usable = []
    for field_id in discovery_allowlist:
        field = registry.resolve(str(field_id))
        materialization = str(
            field.metadata.get("materialization_expression") or ""
        )
        leaves = (
            expression_fields(materialization)
            if materialization
            else {field.field_id}
        )
        if set(map(str, leaves)).issubset(available_fields):
            usable.append(field.field_id)
    if not usable:
        raise RuntimeError("MATERIALIZED_TARGET_ROOTS_EMPTY")
    return tuple(sorted(set(usable)))


def _write_batch_manifest(
    *,
    root: Path,
    arm: str,
    checkpoint_id: str,
    paths: Sequence[Path],
    access_receipts: Sequence[Mapping[str, Any]],
    input_hashes: Mapping[str, str],
) -> Path:
    manifest = {
        "schema_version": "cn_targeted_formula_cem_batch_manifest_v1",
        "status": "BATCH_CLOSED_IMMUTABLE",
        "arm": arm,
        "checkpoint": checkpoint_id,
        "input_hashes": dict(input_hashes),
        "artifacts": [
            _artifact(path, root=root)
            for path in paths
            if path.is_file()
        ],
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
    return _write_json(root / "batch_manifest.json", manifest)


def _selected_runtime_gate(
    checkpoint_root: Path,
    compute_threads: Mapping[str, int],
    *,
    selected_backends: Sequence[str],
) -> dict[str, Any]:
    """Project the existing gate onto the backends this route actually uses."""

    full = _runtime_gate(checkpoint_root, compute_threads)
    selected = {
        backend: dict((full.get("backends") or {}).get(backend) or {})
        for backend in selected_backends
    }
    execution_pass = bool(selected) and all(
        str(row.get("status") or "") == "PASS"
        for row in selected.values()
    )
    run_health_pass = bool(selected) and all(
        str(row.get("run_health_status") or "") == "PASS"
        for row in selected.values()
    )
    return {
        **dict(full),
        "status": (
            "PASS"
            if execution_pass and run_health_pass
            else (
                "PASS_WITH_RUN_HEALTH_FAILURE"
                if execution_pass
                else "RUNTIME_ACCELERATION_GATE_FAILED"
            )
        ),
        "backends": selected,
        "selected_backends": list(selected_backends),
        "projection_reason": "TARGET_ROUTE_BACKEND_ONLY",
    }


def _load_arm_state(
    *,
    output_root: Path,
    arm: str,
    initial_exact: set[str],
    initial_behavior: PortfolioBehaviorArchive,
) -> tuple[
    set[str],
    PortfolioBehaviorArchive,
    int,
    dict[str, Any] | None,
    dict[str, Any] | None,
]:
    exact = set(initial_exact)
    behavior = _copy_behavior_archive(initial_behavior)
    closed = 0
    optimizer_state = None
    rng_state = None
    gap = False
    for index in range(CHECKPOINT_COUNT):
        root = output_root / "arms" / arm / f"checkpoint_{index + 1:03d}"
        manifest_path = root / "batch_manifest.json"
        if not manifest_path.is_file():
            gap = True
            continue
        if gap:
            raise RuntimeError(f"NONCONTIGUOUS_CLOSED_CHECKPOINTS:{arm}")
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8-sig")
        )
        _verify_artifacts(root, manifest)
        attempts = _read_rows(root / "proposal_ledger.parquet")
        exact.update(
            str(row["exact_identity"])
            for row in attempts
            if str(row.get("exact_identity") or "")
        )
        for name in ("behavior_probe.parquet", "full_behavior.parquet"):
            path = root / name
            if path.is_file():
                _add_resolved_behavior_rows(behavior, _read_rows(path))
        state_path = root / "arm_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8-sig"))
        rng_state = copy.deepcopy(state["rng_state"])
        optimizer_state = (
            copy.deepcopy(state.get("optimizer_state"))
            if state.get("optimizer_state")
            else None
        )
        closed += 1
    return exact, behavior, closed, rng_state, optimizer_state


def _generate_checkpoint_pool(
    *,
    projection: TargetedFormulaProjection,
    arm: str,
    policy: Any,
    rng: np.random.Generator,
    exact_seen: set[str],
    checkpoint_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    formula_space_id = _formula_space_for_arm(arm)
    novel: list[dict[str, Any]] = []
    generation_exact: set[str] = set()
    raw = legal = duplicates = semantic_blocked = 0
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    while (
        raw < RAW_ATTEMPT_CAP
        and legal < COMPILE_VALID_CAP
        and len(novel) < BEHAVIOR_PROBE_CAP
    ):
        raw += 1
        pair = projection.generate(
            formula_space_id=formula_space_id,
            policy=policy,
            rng=rng,
        )
        primary, control = pair.candidate, pair.control
        unsupported = unsupported_streaming_operators(
            (primary["expression"], control["expression"])
        )
        if (
            unsupported
            or not bool(primary.get("legal"))
            or not bool(control.get("legal"))
        ):
            semantic_blocked += 1
            continue
        legal += 1
        exact_identity = str(primary["exact_identity"])
        if exact_identity in exact_seen or exact_identity in generation_exact:
            duplicates += 1
            continue
        generation_exact.add(exact_identity)
        novel.append(
            {
                "proposal_id": stable_hash(
                    {
                        "arm": arm,
                        "checkpoint": checkpoint_id,
                        "raw_attempt": raw,
                        "decision_trace_hash": primary[
                            "decision_trace_hash"
                        ],
                    }
                )[:24],
                "arm": arm,
                "checkpoint": checkpoint_id,
                "raw_attempt": raw,
                "route_id": ROUTE_ID,
                "skeleton_id": SKELETON_ID,
                "formula_space_id": formula_space_id,
                "generator_policy": _policy_id_for_arm(arm),
                "extension_id": str(primary["extension_id"]),
                "pair_id": str(primary["pair_id"]),
                "exact_identity": exact_identity,
                "canonical_identity": str(
                    primary["canonical_identity"]
                ),
                "decision_trace": copy.deepcopy(
                    primary["decision_trace"]
                ),
                "decision_trace_hash": str(
                    primary["decision_trace_hash"]
                ),
                "primary": dict(primary),
                "control": dict(control),
            }
        )
    exact_seen.update(generation_exact)
    wall = time.perf_counter() - started_wall
    cpu = time.process_time() - started_cpu
    funnel = {
        "raw_attempts": raw,
        "compile_valid_pairs": legal,
        "semantic_block_pairs": semantic_blocked,
        "exact_duplicate_pairs": duplicates,
        "exact_unique_pairs": len(novel),
        "behavior_probe_cap": BEHAVIOR_PROBE_CAP,
        "sampled_selection_cap": SAMPLED_SELECTION_CAP,
        "full_coordinate_pair_cap": FULL_PAIR_CAP,
        "generation_wall_seconds": wall,
        "generation_cpu_seconds": cpu,
        "raw_attempts_per_second": raw / max(wall, 1e-9),
        "compile_valid_per_second": legal / max(wall, 1e-9),
        "underfill_reason": (
            ""
            if len(novel) >= BEHAVIOR_PROBE_CAP
            else "EXACT_SUPPLY_OR_ATTEMPT_CAP"
        ),
    }
    return novel, funnel


def _execute_checkpoint(
    *,
    arm: str,
    checkpoint_index: int,
    output_root: Path,
    projection: TargetedFormulaProjection,
    policy: Any,
    rng: np.random.Generator,
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
    decision_catalog_path: Path,
) -> dict[str, Any]:
    checkpoint_id = f"checkpoint_{checkpoint_index + 1:03d}"
    root = output_root / "arms" / arm / checkpoint_id
    root.mkdir(parents=True, exist_ok=True)
    if (root / "batch_manifest.json").is_file():
        return json.loads(
            (root / "checkpoint_summary.json").read_text(
                encoding="utf-8-sig"
            )
        )

    proposals, funnel = _generate_checkpoint_pool(
        projection=projection,
        arm=arm,
        policy=policy,
        rng=rng,
        exact_seen=exact_seen,
        checkpoint_id=checkpoint_id,
    )
    proposal_rows = [
        {
            key: value
            for key, value in row.items()
            if key not in {"primary", "control", "decision_trace"}
        }
        | {
            "decision_trace_json": json.dumps(
                row["decision_trace"],
                ensure_ascii=False,
                sort_keys=True,
            )
        }
        for row in proposals
    ]
    proposal_path = _write_parquet(
        root / "proposal_ledger.parquet", proposal_rows
    )
    candidate_path = _write_parquet(
        root / "candidate_attempts.parquet", _pair_members(proposals)
    )
    probe, _, probe_wall = _probe_rows(
        root=root,
        batch_id=f"{arm}.{checkpoint_id}",
        candidate_pairs=proposals,
        field_roots=field_roots,
        split=split,
        compute_threads=compute_threads,
        coordinate_binding=stable_hash(
            {
                "frozen_contract": _sha256(frozen_contract_path),
                "decision_catalog": _sha256(decision_catalog_path),
                "arm": arm,
                "checkpoint": checkpoint_id,
                "split": split.manifest_hash,
            }
        ),
    )
    admitted, decisions = _admit_behavior_unique(
        candidate_rows=_pair_members(proposals),
        probe_rows=probe,
        historical_archive=_copy_behavior_archive(behavior_archive),
    )
    decisions_path = _write_parquet(
        root / "admission_decisions.parquet", decisions
    )
    primary_admitted = {
        str(row["pair_id"]): row
        for row in admitted
        if str(row.get("pair_member_role") or "") == "PRIMARY"
    }
    ordered_pair_ids = sorted(
        primary_admitted,
        key=lambda pair_id: stable_hash(
            {
                "arm": arm,
                "checkpoint": checkpoint_id,
                "pair_id": pair_id,
            }
        ),
    )
    sampled_pair_ids = ordered_pair_ids[:SAMPLED_SELECTION_CAP]
    full_pair_ids = set(sampled_pair_ids[:FULL_PAIR_CAP])
    full_candidates = [
        dict(row)
        for row in admitted
        if str(row["pair_id"]) in full_pair_ids
    ]
    proposal_by_pair = {
        str(row["pair_id"]): row for row in proposals
    }

    access_receipts: list[dict[str, Any]] = []
    table_paths: dict[str, Path] = {}
    binding_path: Path | None = None
    if full_candidates:
        binding_path, table_paths = _context_and_binding(
            batch_root=root,
            candidates=full_candidates,
            registry=registry,
            split=split,
            data_release_hash=_sha256(sidecar_closure),
        )
        _bind_purity(binding_path, purity_path)
        access_receipts = _run_phase3cm_monitored(
            checkpoint_id=f"{arm}.{checkpoint_id}",
            checkpoint_root=root,
            binding_path=binding_path,
            table_paths=table_paths,
            split_manifest=split.manifest_path,
            field_roots=field_roots,
            label_roots=label_roots,
            purity_path=purity_path,
            compute_threads=compute_threads,
            deadline_epoch=deadline_epoch,
            selected_backends=("stock_session",),
            pair_batch_sizes=PAIR_BATCH_SIZES,
        )

    outcomes, full_behavior = _outcome_rows(root)
    if full_candidates:
        full_behavior = _join_full_behavior_identities(
            full_behavior, probe
        )
    outcome_by_pair = {
        str(row["pair_id"]): row for row in outcomes
    }
    observations = []
    for pair_id in sorted(full_pair_ids):
        source = proposal_by_pair[pair_id]
        outcome = outcome_by_pair[pair_id]
        outcome_class = _outcome_class(outcome)
        row = {
            "proposal_id": str(source["proposal_id"]),
            "pair_id": pair_id,
            "exact_identity": str(source["exact_identity"]),
            "outcome_class": outcome_class,
            "outcome_reason": str(
                outcome.get("pair_evaluation_blockers")
                or "PAIR_EVALUATED"
            ),
            "decision_trace": copy.deepcopy(
                source["decision_trace"]
            ),
            "decision_trace_hash": str(
                source["decision_trace_hash"]
            ),
        }
        if outcome_class == "EVALUATED":
            row["signed_matched_increment"] = float(
                outcome["matched_net_increment"]
            )
        observations.append(row)

    tell_receipt: dict[str, Any] = {
        "status": "NOT_CEM_ARM",
        "updated_context_count": 0,
    }
    if arm == "arm_c_cem_expanded" and observations:
        tell_receipt = policy.tell(observations)
    tell_path = _write_json(
        root / "optimizer_tell_receipt.json", tell_receipt
    )
    observation_write_rows = [
        {
            key: value
            for key, value in row.items()
            if key != "decision_trace"
        }
        | {
            "decision_trace_json": json.dumps(
                row["decision_trace"],
                ensure_ascii=False,
                sort_keys=True,
            )
        }
        for row in observations
    ]
    observation_path = _write_parquet(
        root / "observation_ledger.parquet",
        observation_write_rows,
    )
    full_behavior_path = _write_parquet(
        root / "full_behavior.parquet", full_behavior
    )
    _add_resolved_behavior_rows(behavior_archive, probe)
    _add_resolved_behavior_rows(behavior_archive, full_behavior)

    gate = (
        _selected_runtime_gate(
            root,
            compute_threads,
            selected_backends=("stock_session",),
        )
        if full_candidates
        else {
            "status": "NOT_EVALUATED_NO_ADMITTED_PAIRS",
            "backends": {},
        }
    )
    gate_path = _write_json(root / "runtime_utilization_gate.json", gate)
    state_payload = {
        "schema_version": "cn_targeted_formula_arm_state_v1",
        "arm": arm,
        "checkpoint": checkpoint_id,
        "rng_state": copy.deepcopy(rng.bit_generator.state),
        "optimizer_state": (
            policy.state_dict(rng=rng)
            if arm == "arm_c_cem_expanded"
            else None
        ),
    }
    state_path = _write_json(root / "arm_state.json", state_payload)

    evaluated = [
        row
        for row in observations
        if row["outcome_class"] == "EVALUATED"
    ]
    funnel.update(
        {
            "behavior_probed_pairs": len(proposals),
            "behavior_unique_pairs": len(primary_admitted),
            "sampled_selected_pairs": len(sampled_pair_ids),
            "full_coordinate_pairs": len(full_pair_ids),
            "evaluated_pairs": len(evaluated),
            "probe_wall_seconds": probe_wall,
        }
    )
    funnel_path = _write_json(root / "funnel_metrics.json", funnel)
    summary = {
        "schema_version": (
            "cn_targeted_formula_cem_checkpoint_summary_v1"
        ),
        "arm": arm,
        "checkpoint": checkpoint_id,
        **funnel,
        "positive_matched_pairs": sum(
            float(row["signed_matched_increment"]) > 0.0
            for row in evaluated
        ),
        "median_signed_matched_increment": (
            statistics.median(
                float(row["signed_matched_increment"])
                for row in evaluated
            )
            if evaluated
            else None
        ),
        "phase3cm_wall_seconds": _phase3cm_wall_seconds(root),
        "minimum_free_memory_bytes": _minimum_free_memory(root),
        "runtime_gate_status": str(gate.get("status") or ""),
        "cem_updated_context_count": int(
            tell_receipt.get("updated_context_count") or 0
        ),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    summary_path = _write_json(
        root / "checkpoint_summary.json", summary
    )
    paths = [
        proposal_path,
        candidate_path,
        root / "behavior_probe.parquet",
        root / "behavior_probe_audit.json",
        decisions_path,
        observation_path,
        full_behavior_path,
        tell_path,
        gate_path,
        state_path,
        funnel_path,
        summary_path,
        *table_paths.values(),
    ]
    if binding_path is not None:
        paths.append(binding_path)
    paths.extend(
        Path(str(row["result_path"]))
        for row in access_receipts
        if Path(str(row["result_path"])).is_file()
    )
    _write_batch_manifest(
        root=root,
        arm=arm,
        checkpoint_id=checkpoint_id,
        paths=paths,
        access_receipts=access_receipts,
        input_hashes={
            "frozen_contract": _sha256(frozen_contract_path),
            "decision_catalog": _sha256(decision_catalog_path),
            "prior_checkpoint": (
                _sha256(
                    root.parent
                    / f"checkpoint_{checkpoint_index:03d}"
                    / "batch_manifest.json"
                )
                if checkpoint_index
                else "GENESIS"
            ),
        },
    )
    return summary


def _arm_metrics(
    output_root: Path,
    arm: str,
    initial_behavior_families: set[str],
) -> dict[str, Any]:
    summaries = []
    observations = []
    full_behavior = []
    tokens = []
    ast_shapes = set()
    cache_peaks = []
    runtime_rows = []
    cem_updates = 0
    for index in range(CHECKPOINT_COUNT):
        root = output_root / "arms" / arm / f"checkpoint_{index + 1:03d}"
        manifest = json.loads(
            (root / "batch_manifest.json").read_text(
                encoding="utf-8-sig"
            )
        )
        _verify_artifacts(root, manifest)
        summaries.append(
            json.loads(
                (root / "checkpoint_summary.json").read_text(
                    encoding="utf-8-sig"
                )
            )
        )
        observations.extend(_read_rows(root / "observation_ledger.parquet"))
        full_behavior.extend(_read_rows(root / "full_behavior.parquet"))
        proposals = _read_rows(root / "proposal_ledger.parquet")
        candidate_rows = _read_rows(root / "candidate_attempts.parquet")
        primaries = {
            str(row.get("pair_id") or ""): row
            for row in candidate_rows
            if str(row.get("pair_member_role") or "") == "PRIMARY"
        }
        controls = {
            str(row.get("pair_id") or ""): row
            for row in candidate_rows
            if str(row.get("pair_member_role") or "") == "CONTROL"
        }
        for pair_id in primaries.keys() & controls.keys():
            ast_shapes.add(
                (
                    _shape(str(primaries[pair_id].get("expression") or "")),
                    _shape(str(controls[pair_id].get("expression") or "")),
                )
            )
        for proposal in proposals:
            for row in json.loads(
                str(proposal.get("decision_trace_json") or "[]")
            ):
                tokens.append(str(row["selected_token_id"]))
        tell = json.loads(
            (root / "optimizer_tell_receipt.json").read_text(
                encoding="utf-8-sig"
            )
        )
        cem_updates += int(tell.get("updated_context_count") or 0)
        gate = json.loads(
            (root / "runtime_utilization_gate.json").read_text(
                encoding="utf-8-sig"
            )
        )
        if "stock_session" in (gate.get("backends") or {}):
            runtime_rows.append(dict(gate["backends"]["stock_session"]))
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
        row
        for row in observations
        if str(row.get("outcome_class") or "") == "EVALUATED"
    ]
    increments = [
        float(row["signed_matched_increment"]) for row in evaluated
    ]
    wall = sum(
        float(row.get("phase3cm_wall_seconds") or 0.0)
        + float(row.get("probe_wall_seconds") or 0.0)
        + float(row.get("generation_wall_seconds") or 0.0)
        for row in summaries
    )
    families = {
        str(row.get("portfolio_behavior_family_id") or "")
        for row in full_behavior
        if str(row.get("portfolio_behavior_family_id") or "")
    } - initial_behavior_families
    token_counts = Counter(tokens)
    effective_cores_median = (
        statistics.median(
            float(row.get("effective_compute_cores") or 0.0)
            for row in runtime_rows
        )
        if runtime_rows
        else None
    )
    effective_cpu_hours = (
        wall * float(effective_cores_median) / 3600.0
        if effective_cores_median is not None
        else 0.0
    )
    return {
        "arm": arm,
        "scheduled_full_coordinate_pairs": sum(
            int(row["full_coordinate_pairs"]) for row in summaries
        ),
        "evaluated_pairs": len(evaluated),
        "active_checkpoint_count": sum(
            int(row["evaluated_pairs"]) > 0 for row in summaries
        ),
        "positive_matched_pairs": sum(value > 0 for value in increments),
        "positive_matched_pairs_per_wall_hour": (
            sum(value > 0 for value in increments)
            * 3600
            / max(1.0, wall)
        ),
        "evaluated_pairs_per_wall_hour": len(evaluated)
        * 3600
        / max(1.0, wall),
        "full_coordinate_pairs_per_wall_hour": len(evaluated)
        * 3600
        / max(1.0, wall),
        "estimated_effective_cpu_hours": effective_cpu_hours,
        "full_coordinate_pairs_per_effective_cpu_hour": (
            len(evaluated) / max(1e-12, effective_cpu_hours)
            if effective_cpu_hours > 0.0
            else None
        ),
        "effective_cpu_hour_method": (
            "total_wall_seconds_x_"
            "stock_session_effective_cores_median"
        ),
        "median_signed_matched_increment": (
            statistics.median(increments) if increments else None
        ),
        "behavior_family_count": len(families),
        "behavior_discovery_per_evaluated_pair": len(families)
        / max(1, len(evaluated)),
        "ast_shape_count": len(ast_shapes),
        "exact_unique_by_checkpoint": [
            int(row["exact_unique_pairs"]) for row in summaries
        ],
        "behavior_unique_by_checkpoint": [
            int(row["behavior_unique_pairs"]) for row in summaries
        ],
        "maximum_token_share": max(
            token_counts.values(), default=0
        )
        / max(1, len(tokens)),
        "cem_updated_context_count": cem_updates,
        "minimum_free_memory_bytes": min(
            (
                int(row["minimum_free_memory_bytes"])
                for row in summaries
                if row.get("minimum_free_memory_bytes") is not None
            ),
            default=0,
        ),
        "maximum_observed_cache_bytes": max(cache_peaks, default=0),
        "stock_session_effective_cores_median": effective_cores_median,
        "stock_session_host_cpu_median": (
            statistics.median(
                float(row.get("host_logical_cpu_occupancy") or 0.0)
                for row in runtime_rows
            )
            if runtime_rows
            else None
        ),
        "peak_rss_bytes": max(
            (int(row.get("peak_rss_bytes") or 0) for row in runtime_rows),
            default=0,
        ),
        "checkpoint_summaries": summaries,
    }


def _comparison_verdict(
    *,
    arm_a: Mapping[str, Any],
    arm_b: Mapping[str, Any],
    arm_c: Mapping[str, Any],
    static_status: str,
    behavior_status: str,
    sampled_full_contract: str,
) -> dict[str, Any]:
    support = {
        arm["arm"]: (
            int(arm["evaluated_pairs"]) >= MINIMUM_EVALUATED_PAIRS
            and int(arm["active_checkpoint_count"])
            >= MINIMUM_ACTIVE_CHECKPOINTS
        )
        for arm in (arm_a, arm_b, arm_c)
    }
    formula_checks = {
        "positive_per_wall_hour": float(
            arm_b["positive_matched_pairs_per_wall_hour"]
        )
        + 1e-12
        >= float(arm_a["positive_matched_pairs_per_wall_hour"]),
        "median_increment": (
            arm_a["median_signed_matched_increment"] is not None
            and arm_b["median_signed_matched_increment"] is not None
            and float(arm_b["median_signed_matched_increment"])
            >= float(arm_a["median_signed_matched_increment"])
        ),
        "behavior_discovery": float(
            arm_b["behavior_discovery_per_evaluated_pair"]
        )
        + 1e-12
        >= 0.90
        * float(arm_a["behavior_discovery_per_evaluated_pair"]),
        "ast_shape_diversity": int(arm_b["ast_shape_count"])
        > int(arm_a["ast_shape_count"]),
    }
    cem_checks = {
        "positive_per_wall_hour_15pct": float(
            arm_c["positive_matched_pairs_per_wall_hour"]
        )
        + 1e-12
        >= 1.15 * float(arm_b["positive_matched_pairs_per_wall_hour"]),
        "median_increment": (
            arm_b["median_signed_matched_increment"] is not None
            and arm_c["median_signed_matched_increment"] is not None
            and float(arm_c["median_signed_matched_increment"])
            >= float(arm_b["median_signed_matched_increment"])
        ),
        "behavior_discovery": float(
            arm_c["behavior_discovery_per_evaluated_pair"]
        )
        + 1e-12
        >= 0.90
        * float(arm_b["behavior_discovery_per_evaluated_pair"]),
        "checkpoint_2_3_supply": all(
            int(value) > 0
            for value in arm_c["exact_unique_by_checkpoint"][1:]
        )
        and all(
            int(value) > 0
            for value in arm_c["behavior_unique_by_checkpoint"][1:]
        ),
        "cem_table_updated": int(
            arm_c["cem_updated_context_count"]
        )
        > 0,
        "no_category_collapse": float(
            arm_c["maximum_token_share"]
        )
        < 0.90,
    }
    if not all(support.values()):
        formula_verdict = "INSUFFICIENT_FINANCIAL_COMPARISON_SUPPORT"
        cem_verdict = "INSUFFICIENT_FINANCIAL_COMPARISON_SUPPORT"
    else:
        formula_verdict = (
            "QUALIFIED"
            if all(formula_checks.values())
            else "NOT_QUALIFIED"
        )
        cem_verdict = (
            "QUALIFIED"
            if all(cem_checks.values())
            else (
                "INSUFFICIENT_ACTIVE_ELITE_SUPPORT"
                if not cem_checks["cem_table_updated"]
                else "NOT_QUALIFIED"
            )
        )
    performance_safety_checks = {
        "stock_session_native_contract": all(
            str(summary.get("runtime_gate_status") or "") == "PASS"
            for arm in (arm_a, arm_b, arm_c)
            for summary in arm["checkpoint_summaries"]
            if int(summary.get("full_coordinate_pairs") or 0) > 0
        ),
        "minimum_free_memory_24_gib": min(
            int(arm.get("minimum_free_memory_bytes") or 0)
            for arm in (arm_a, arm_b, arm_c)
        )
        >= MINIMUM_FREE_MEMORY_BYTES,
        "cache_cap_8_gib": max(
            int(arm.get("maximum_observed_cache_bytes") or 0)
            for arm in (arm_a, arm_b, arm_c)
        )
        <= MAXIMUM_CACHE_BYTES,
        "pair_batch_at_most_4": max(PAIR_BATCH_SIZES.values()) <= 4,
    }
    performance_utilization_checks = {
        "all_arms_logical_cpu_occupancy_at_least_75_percent": all(
            float(arm.get("stock_session_host_cpu_median") or 0.0)
            >= 0.75
            for arm in (arm_a, arm_b, arm_c)
        ),
        "full_host_native_kernel_smt_ceiling_proven": False,
    }
    performance = (
        "PASS"
        if all(performance_safety_checks.values())
        and any(performance_utilization_checks.values())
        else (
            "PARTIAL"
            if all(performance_safety_checks.values())
            else "FAIL"
        )
    )
    readiness_checks = {
        "static_formula_space": static_status == "PASS",
        "behavior_space": behavior_status == "PASS",
        "financial_support": all(support.values()),
        "formula_increment": formula_verdict == "QUALIFIED",
        "cem_increment": cem_verdict == "QUALIFIED",
        "sampled_full_contract": sampled_full_contract == "PASS",
        "performance": performance == "PASS",
        "access_boundary": True,
    }
    readiness = (
        "READY"
        if all(readiness_checks.values())
        else (
            "SEMANTICS_BLOCKED"
            if not readiness_checks["sampled_full_contract"]
            else (
                "COMPARISON_SUPPORT_BLOCKED"
                if not readiness_checks["financial_support"]
                else (
                    "FORMULA_SPACE_BLOCKED"
                if not readiness_checks["formula_increment"]
                else (
                    "SEARCH_POLICY_BLOCKED"
                    if not readiness_checks["cem_increment"]
                        else "PERFORMANCE_BLOCKED"
                    )
                )
                )
            )
        )
    return {
        "FORMULA_SPACE_INCREMENT": formula_verdict,
        "CEM_SEARCH_INCREMENT": cem_verdict,
        "SAMPLED_FULL_CONTRACT_PARITY": sampled_full_contract,
        "PERFORMANCE_CONTRACT": performance,
        "TARGET_FAMILY_LARGE_SEARCH_READINESS": readiness,
        "minimum_support": support,
        "formula_checks": formula_checks,
        "cem_checks": cem_checks,
        "performance_safety_checks": performance_safety_checks,
        "performance_utilization_checks": (
            performance_utilization_checks
        ),
        "readiness_checks": readiness_checks,
    }


def _fresh_large_search_state(
    projection: TargetedFormulaProjection,
    *,
    seed: int,
) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    policy = CategoricalCEMPolicy.fresh(
        decisions=projection.decision_specs(
            EXPANDED_FORMULA_SPACE_ID
        ),
        decision_catalog_hash=projection.decision_catalog_hash(
            EXPANDED_FORMULA_SPACE_ID
        ),
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
    )
    state = policy.state_dict(rng=rng)
    state.update(
        {
            "optimizer_state_origin": state["state_origin"],
            "state_origin": "fresh_uniform_from_frozen_catalog",
            "qualification_evidence_only": False,
            "forbidden_as_large_search_initialization": False,
            "large_search_seed": seed,
        }
    )
    return state


def _fresh_large_search_state_parity(
    projection: TargetedFormulaProjection,
    *,
    seed: int,
) -> dict[str, Any]:
    left = _fresh_large_search_state(projection, seed=seed)
    right = _fresh_large_search_state(projection, seed=seed)
    checks = {
        "deterministic_replay": _stable_hash(left) == _stable_hash(right),
        "state_origin": (
            left.get("state_origin")
            == "fresh_uniform_from_frozen_catalog"
        ),
        "generation_zero": left.get("generation") == 0,
        "reward_observation_count_zero": (
            left.get("reward_observation_count") == 0
        ),
        "source_campaign_none": left.get("source_campaign") == "none",
        "catalog_hash_bound": (
            left.get("decision_catalog_hash")
            == projection.decision_catalog_hash(
                EXPANDED_FORMULA_SPACE_ID
            )
        ),
    }
    return {
        "schema_version": "cn_fresh_large_search_state_parity_v1",
        "status": "PASS" if all(checks.values()) else "FAIL",
        "seed": seed,
        "state_hash": _stable_hash(left),
        "checks": checks,
    }


def run_static(args: argparse.Namespace) -> dict[str, Any]:
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    discovery = load_development_discovery_root_authority(
        args.discovery_contract.resolve(),
        registry=registry,
    )
    projection = _projection(
        registry=registry,
        route_root_allowlist=discovery["route_root_allowlists"],
        extension_id=args.extension_id,
    )
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    old_catalog_path = _write_json(
        output_root / "decision_catalog_old.json",
        projection.decision_catalog(OLD_FORMULA_SPACE_ID),
    )
    expanded_catalog_path = _write_json(
        output_root / "decision_catalog.json",
        projection.decision_catalog(EXPANDED_FORMULA_SPACE_ID),
    )
    parity = _legacy_parity(projection, seed=args.seed)
    parity_path = _write_parquet(
        output_root / "legacy_parity.parquet", parity
    )
    old_rows, old_metrics = _enumerate_space(
        projection, OLD_FORMULA_SPACE_ID
    )
    expanded_rows, expanded_metrics = _enumerate_space(
        projection, EXPANDED_FORMULA_SPACE_ID
    )
    static_verdict = _static_verdict(
        old_metrics, expanded_metrics, expanded_rows
    )
    metrics_path = _write_parquet(
        output_root / "static_generation_metrics.parquet",
        [old_metrics, expanded_metrics],
    )
    verdict_path = _write_json(
        output_root / "static_generation_verdict.json", static_verdict
    )
    result = {
        "status": (
            "STATIC_QUALIFICATION_PASS"
            if static_verdict["status"] == "PASS"
            else "FORMULA_SPACE_INCREMENT_NOT_PROVEN"
        ),
        "route_id": ROUTE_ID,
        "skeleton_id": SKELETON_ID,
        "extension_id": args.extension_id,
        "old_space_exact_upper_bound": old_metrics[
            "exact_unique_pairs"
        ],
        "expanded_space_exact_upper_bound": expanded_metrics[
            "exact_unique_pairs"
        ],
        "legacy_parity_rows": len(parity),
        "artifacts": [
            _artifact(path, root=output_root)
            for path in (
                old_catalog_path,
                expanded_catalog_path,
                parity_path,
                metrics_path,
                verdict_path,
            )
        ],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    _write_json(output_root / "static_receipt.json", result)
    return result


def run_full(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"REAL_QUALIFICATION_AUTHORIZED_ONLY_ON_77O:{platform.node()}"
        )
    if int(args.active_threads) != 30 or int(args.session_threads) != 2:
        raise RuntimeError("FROZEN_THREAD_CONTRACT_MISMATCH")
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    launch_receipt_path, writer_lock_path = _acquire_campaign_writer(
        output_root=output_root,
        task_id=args.task_id,
    )
    authorization = json.loads(
        args.qualification_authorization.resolve().read_text(
            encoding="utf-8-sig"
        )
    )
    expected_authority = {
        "authorization_id": RETRY_AUTHORIZATION_ID,
        "status": "BOUNDED_CONTINUOUS_EXECUTION",
        "route_id": ROUTE_ID,
        "skeleton_id": SKELETON_ID,
    }
    authority_drift = [
        key
        for key, value in expected_authority.items()
        if str(authorization.get(key) or "") != value
    ]
    if authority_drift:
        raise RuntimeError(
            "QUALIFICATION_AUTHORIZATION_DRIFT:"
            + ",".join(authority_drift)
        )
    if (
        authorization.get("candidate_queue")
        != list(TARGETED_RETRY_EXTENSION_IDS)
        or dict(authorization.get("historical_rejected_extension") or {})
        != {
            "extension_id": PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID,
            **HISTORICAL_REJECTED_EXTENSION_IDS[
                PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID
            ],
        }
        or authorization.get("financial_arms") != list(ARMS)
        or int(authorization.get("base_seed") or -1) != int(args.seed)
        or int(authorization.get("checkpoint_count") or -1)
        != CHECKPOINT_COUNT
        or int(
            authorization.get(
                "full_coordinate_pair_cap_per_checkpoint"
            )
            or -1
        )
        != FULL_PAIR_CAP
        or int(
            authorization.get("required_behavior_unique_pairs") or -1
        )
        != REQUIRED_RETRY_BEHAVIOR_UNIQUE
        or authorization.get("launcher_mode")
        != "A_IMMEDIATE_START_NO_SCHEDULED_TRIGGER"
        or authorization.get("validation") != "FORBIDDEN"
        or authorization.get("holdout") != "SEALED"
        or authorization.get("forward_2026") != "SEALED"
        or authorization.get("promotion") != "FORBIDDEN"
    ):
        raise RuntimeError("QUALIFICATION_AUTHORIZATION_BOUNDARY_DRIFT")
    authorization_path = _write_json(
        output_root / "qualification_authorization_binding.json",
        {
            **authorization,
            "source": _artifact(
                args.qualification_authorization.resolve()
            ),
        },
    )
    sampled_authority, sampled_authority_path = (
        _resolve_sampled_evaluator_authority(output_root)
    )
    initial_exact, initial_behavior, source_binding = (
        _source_campaign_binding(
            source_root=args.source_campaign_root.resolve(),
            source_receipt_path=args.source_receipt.resolve(),
            historical_candidate_archive=(
                args.historical_candidate_archive.resolve()
            ),
            historical_archive_manifest=(
                args.historical_archive_manifest.resolve()
            ),
        )
    )
    source_path = _write_json(
        output_root / "source_campaign_binding.json", source_binding
    )
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    registry_path = _write_json(
        output_root / "registry_binding.json",
        _registry_binding(args.registry.resolve(), registry),
    )
    discovery = load_development_discovery_root_authority(
        args.discovery_contract.resolve(),
        registry=registry,
    )
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
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
    schema, schema_by_backend = materialized_schema_binding(
        field_roots=field_roots,
        registry=registry,
    )
    schema_path = _write_json(
        output_root / "materialized_schema_binding.json", schema
    )
    purity = audit_split_boundary_label_purity(
        split=split,
        registry=registry,
        label_roots=label_roots,
    )
    purity_path = _write_json(
        output_root / "split_boundary_purity.json", purity
    )
    if str(purity.get("status") or "") != "PASS":
        raise RuntimeError("SPLIT_BOUNDARY_LABEL_PURITY_FAILED")
    runtime = _runtime_envelope(
        compute_threads["active_bar"], compute_threads["stock_session"]
    )
    if str(runtime.get("status") or "") != "PASS":
        raise RuntimeError(str(runtime.get("status") or "RUNTIME_FAILED"))
    runtime_path = _write_json(
        output_root / "runtime_envelope.json", runtime
    )
    usable = _materializable_allowlist(
        registry=registry,
        discovery_allowlist=discovery["route_root_allowlists"][ROUTE_ID],
        available_fields=schema_by_backend["stock_session"],
    )
    old_projection = _projection(
        registry=registry,
        route_root_allowlist={ROUTE_ID: usable},
        extension_id=PRE_EVENT_PAYLOAD_CSRANK_EXTENSION_ID,
    )
    old_static, old_behavior, old_exact_supply, old_reuse_path = (
        _bind_reused_old_authority(
            authorization=authorization,
            output_root=output_root,
            projection=old_projection,
            registry=registry,
            discovery_contract=args.discovery_contract.resolve(),
            source_binding=source_binding,
            base_seed=args.seed,
        )
    )
    rejected_path = _write_json(
        output_root / "historical_rejected_extension_binding.json",
        {
            "schema_version": (
                "cn_historical_rejected_formula_extension_binding_v1"
            ),
            "status": "PRESERVED_UNCHANGED",
            **dict(authorization["historical_rejected_extension"]),
            "historical_authorization": dict(
                authorization["historical_sign_authorization"]
            ),
        },
    )
    parity_path = _write_json(
        output_root / "legacy_parity_reuse_receipt.json",
        {
            "status": "PASS_REUSED_NOT_RECOMPUTED",
            "source_artifact": (
                Path(str(authorization["old_authority"]["evidence_root"]))
                / "legacy_parity.parquet"
            ).as_posix(),
            "source_sha256": authorization["old_authority"][
                "artifact_sha256"
            ]["legacy_parity.parquet"],
        },
    )

    candidate_results = {
        extension_id: {
            "extension_id": extension_id,
            "static_gate": "NOT_RUN_PRIOR_CANDIDATE_ACCEPTED",
            "behavior_gate": "NOT_RUN_PRIOR_CANDIDATE_ACCEPTED",
        }
        for extension_id in TARGETED_RETRY_EXTENSION_IDS
    }
    candidate_artifacts: list[Path] = []
    behavior_metric_rows = [
        {
            **old_behavior,
            "authority_mode": "VERIFIED_REUSED_NOT_RECOMPUTED",
        }
    ]
    accepted_extension = ""
    projection: TargetedFormulaProjection | None = None
    expanded_rows: list[dict[str, Any]] = []
    expanded_static: dict[str, Any] = {}
    static_verdict: dict[str, Any] = {}
    expanded_behavior: dict[str, Any] = {}
    for extension_id in TARGETED_RETRY_EXTENSION_IDS:
        candidate_root = output_root / "extension_qualification" / extension_id
        candidate_root.mkdir(parents=True, exist_ok=True)
        current_projection = _projection(
            registry=registry,
            route_root_allowlist={ROUTE_ID: usable},
            extension_id=extension_id,
        )
        candidate_catalog_path = _write_json(
            candidate_root / "decision_catalog.json",
            current_projection.decision_catalog(
                EXPANDED_FORMULA_SPACE_ID
            ),
        )
        current_rows, current_static = _enumerate_space(
            current_projection, EXPANDED_FORMULA_SPACE_ID
        )
        current_static_verdict = _static_verdict(
            old_static, current_static, current_rows
        )
        current_static_path = _write_parquet(
            candidate_root / "static_generation_metrics.parquet",
            [old_static, current_static],
        )
        current_static_verdict_path = _write_json(
            candidate_root / "static_generation_verdict.json",
            current_static_verdict,
        )
        candidate_artifacts.extend(
            (
                candidate_catalog_path,
                current_static_path,
                current_static_verdict_path,
            )
        )
        candidate_results[extension_id]["static_gate"] = str(
            current_static_verdict["status"]
        )
        if current_static_verdict["status"] != "PASS":
            candidate_results[extension_id][
                "behavior_gate"
            ] = "NOT_RUN_STATIC_GATE_FAILED"
            candidate_artifacts.append(
                _write_json(
                    candidate_root / "extension_qualification.json",
                    candidate_results[extension_id],
                )
            )
            continue

        exact_novel_expanded = _post_archive_exact_unique_rows(
            current_rows, initial_exact
        )
        coordinate_binding = stable_hash(
            {
                "authorization": _sha256(authorization_path),
                "candidate_catalog": _sha256(candidate_catalog_path),
                "source_campaign": _sha256(source_path),
                "old_authority_receipt": _sha256(old_reuse_path),
                "expanded_probe_seed": int(args.seed) + 103,
                "order_contract": (
                    "sha256(json(seed,exact_identity));ascending"
                ),
            }
        )
        current_behavior, _ = _static_behavior_probe(
            output_root=candidate_root,
            mode="EXPANDED_UNIFORM",
            rows=exact_novel_expanded,
            seed=args.seed + 103,
            historical=initial_behavior,
            field_roots=field_roots,
            split=split,
            compute_threads=compute_threads,
            coordinate_binding=coordinate_binding,
        )
        behavior_pass = (
            int(current_behavior["behavior_unique_pairs"])
            >= REQUIRED_RETRY_BEHAVIOR_UNIQUE
        )
        candidate_results[extension_id].update(
            {
                "behavior_gate": "PASS" if behavior_pass else "FAIL",
                "behavior_unique_pairs": int(
                    current_behavior["behavior_unique_pairs"]
                ),
                "behavior_probe_candidates": int(
                    current_behavior["behavior_probe_candidates"]
                ),
                "required_behavior_unique_pairs": (
                    REQUIRED_RETRY_BEHAVIOR_UNIQUE
                ),
                "expanded_probe_seed": int(args.seed) + 103,
                "coordinate_binding": coordinate_binding,
            }
        )
        behavior_metric_rows.append(
            {
                **current_behavior,
                "extension_id": extension_id,
                "authority_mode": "NEW_RETRY_PROBE",
            }
        )
        candidate_result_path = _write_json(
            candidate_root / "extension_qualification.json",
            candidate_results[extension_id],
        )
        candidate_artifacts.append(candidate_result_path)
        if behavior_pass:
            accepted_extension = extension_id
            projection = current_projection
            expanded_rows = current_rows
            expanded_static = current_static
            static_verdict = current_static_verdict
            expanded_behavior = current_behavior
            break

    final_gate_fields = {
        "EXTENSION_CSRANK_STATIC_GATE": candidate_results[
            PRE_EVENT_PAYLOAD_CSRANK_EXTENSION_ID
        ]["static_gate"],
        "EXTENSION_CSRANK_BEHAVIOR_GATE": candidate_results[
            PRE_EVENT_PAYLOAD_CSRANK_EXTENSION_ID
        ]["behavior_gate"],
        "EXTENSION_ABS_STATIC_GATE": candidate_results[
            PRE_EVENT_PAYLOAD_ABS_EXTENSION_ID
        ]["static_gate"],
        "EXTENSION_ABS_BEHAVIOR_GATE": candidate_results[
            PRE_EVENT_PAYLOAD_ABS_EXTENSION_ID
        ]["behavior_gate"],
        "ACCEPTED_EXTENSION": accepted_extension or "NONE",
    }
    if projection is None:
        verdict = {
            **final_gate_fields,
            "SAMPLED_FULL_CONTRACT_PARITY": (
                "NOT_RUN_QUEUE_EXHAUSTED"
            ),
            "FORMULA_SPACE_INCREMENT": "NOT_RUN_QUEUE_EXHAUSTED",
            "CEM_SEARCH_INCREMENT": "NOT_RUN_QUEUE_EXHAUSTED",
            "PERFORMANCE_CONTRACT": "NOT_RUN_QUEUE_EXHAUSTED",
            "TARGET_FAMILY_LARGE_SEARCH_READINESS": (
                "FORMULA_EXTENSION_CANDIDATE_QUEUE_EXHAUSTED"
            ),
        }
        verdict_path = _write_json(
            output_root / "final_decision.json", verdict
        )
        metrics_path = _write_json(
            output_root / "qualification_metrics.json",
            {"extension_candidates": candidate_results},
        )
        manifest = {
            "schema_version": (
                "cn_targeted_formula_cem_artifact_manifest_v2"
            ),
            "status": "FORMULA_EXTENSION_CANDIDATE_QUEUE_EXHAUSTED",
            "verdict": verdict,
            "artifacts": [
                _artifact(path, root=output_root)
                for path in (
                    authorization_path,
                    launch_receipt_path,
                    sampled_authority_path,
                    source_path,
                    registry_path,
                    schema_path,
                    purity_path,
                    runtime_path,
                    old_reuse_path,
                    rejected_path,
                    parity_path,
                    metrics_path,
                    verdict_path,
                    *candidate_artifacts,
                )
                if path.is_file()
            ],
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
        }
        manifest["manifest_payload_hash"] = _stable_hash(manifest)
        _write_json(output_root / "artifact_manifest.json", manifest)
        _close_campaign_writer(
            writer_lock_path, task_id=args.task_id
        )
        return verdict

    contract = {
        "schema_version": "cn_targeted_formula_cem_contract_v2",
        "status": "FROZEN_EXECUTABLE",
        "route_id": ROUTE_ID,
        "skeleton_id": SKELETON_ID,
        "extension_id": accepted_extension,
        "extension_lifecycle": (
            "FROZEN_ACCEPTED_EXTENSION_FOR_FINANCIAL_QUALIFICATION"
        ),
        "candidate_queue": list(TARGETED_RETRY_EXTENSION_IDS),
        "arms": list(ARMS),
        "checkpoint_count": CHECKPOINT_COUNT,
        "funnel_caps_per_arm_checkpoint": {
            "raw_attempts": RAW_ATTEMPT_CAP,
            "compile_valid": COMPILE_VALID_CAP,
            "behavior_probe": BEHAVIOR_PROBE_CAP,
            "sampled_selection": SAMPLED_SELECTION_CAP,
            "full_coordinate_pairs": FULL_PAIR_CAP,
        },
        "sampled_evaluator_authority_status": sampled_authority[
            "status"
        ],
        "sampled_evaluator": (
            "NO_SURROGATE_CREATED;"
            + str(sampled_authority["status"])
        ),
        "full_coordinate_evaluator": (
            "existing development Phase3CM matched evaluator"
        ),
        "historical_exact_supply_minimum": MINIMUM_EXACT_SUPPLY,
        "historical_behavior_supply_minimum": MINIMUM_BEHAVIOR_SUPPLY,
        "minimum_evaluated_pairs_per_arm": MINIMUM_EVALUATED_PAIRS,
        "minimum_active_checkpoints_per_arm": MINIMUM_ACTIVE_CHECKPOINTS,
        "materialized_target_root_allowlist": list(usable),
        "active_threads": 30,
        "session_threads": 2,
        "pair_batch_sizes": PAIR_BATCH_SIZES,
        "cache_cap_bytes": MAXIMUM_CACHE_BYTES,
        "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "cross_sprint_adaptive_memory": "FORBIDDEN",
        "qualification_authorization_sha256": _sha256(
            authorization_path
        ),
    }
    frozen_path = _write_json(
        output_root / "frozen_contract.json", contract
    )
    catalog_path = _write_json(
        output_root / "decision_catalog.json",
        projection.decision_catalog(EXPANDED_FORMULA_SPACE_ID),
    )
    old_catalog_path = _write_json(
        output_root / "decision_catalog_old.json",
        projection.decision_catalog(OLD_FORMULA_SPACE_ID),
    )
    static_path = _write_parquet(
        output_root / "static_generation_metrics.parquet",
        [old_static, expanded_static],
    )
    static_verdict_path = _write_json(
        output_root / "static_generation_verdict.json",
        static_verdict,
    )
    behavior_metrics_path = _write_parquet(
        output_root / "behavior_probe_metrics.parquet",
        behavior_metric_rows,
    )
    behavior_gate = (
        int(expanded_behavior["behavior_unique_pairs"])
        >= REQUIRED_RETRY_BEHAVIOR_UNIQUE
    )
    supply = {
        "post_archive_old_space_exact_supply": old_exact_supply,
        "post_archive_old_space_behavior_supply": int(
            old_behavior["behavior_unique_pairs"]
        ),
        "old_authority_mode": "VERIFIED_REUSED_NOT_RECOMPUTED",
        "accepted_extension": accepted_extension,
        "expanded_behavior_unique_pairs": int(
            expanded_behavior["behavior_unique_pairs"]
        ),
        "required_expanded_behavior_unique_pairs": (
            REQUIRED_RETRY_BEHAVIOR_UNIQUE
        ),
        "exact_gate": old_exact_supply >= MINIMUM_EXACT_SUPPLY,
        "behavior_gate": int(old_behavior["behavior_unique_pairs"])
        >= MINIMUM_BEHAVIOR_SUPPLY,
        "formula_space_behavior_gate": behavior_gate,
    }
    supply["status"] = _qualification_gate_status(
        exact_gate=bool(supply["exact_gate"]),
        behavior_gate=bool(supply["behavior_gate"]),
        formula_space_behavior_gate=behavior_gate,
    )
    supply_path = _write_json(
        output_root / "supply_and_behavior_gate.json", supply
    )
    if supply["status"] != "PASS":
        raise RuntimeError(str(supply["status"]))
    fresh_state_parity = _fresh_large_search_state_parity(
        projection, seed=args.seed + 900_001
    )
    fresh_state_parity_path = _write_json(
        output_root / "fresh_large_search_state_parity.json",
        fresh_state_parity,
    )
    if fresh_state_parity["status"] != "PASS":
        raise RuntimeError("FRESH_LARGE_SEARCH_STATE_PARITY_FAILED")

    deadline_epoch = time.time() + int(args.maximum_wall_seconds)
    arm_state: dict[str, dict[str, Any]] = {}
    for arm_index, arm in enumerate(ARMS):
        exact, archive, closed, rng_state, optimizer_state = _load_arm_state(
            output_root=output_root,
            arm=arm,
            initial_exact=initial_exact,
            initial_behavior=initial_behavior,
        )
        rng = np.random.default_rng(args.seed + 10_000 * (arm_index + 1))
        if rng_state is not None:
            rng.bit_generator.state = copy.deepcopy(rng_state)
        if arm == "arm_c_cem_expanded":
            decisions = projection.decision_specs(
                EXPANDED_FORMULA_SPACE_ID
            )
            policy = (
                CategoricalCEMPolicy.restore(
                    optimizer_state,
                    decisions=decisions,
                    decision_catalog_hash=(
                        projection.decision_catalog_hash(
                            EXPANDED_FORMULA_SPACE_ID
                        )
                    ),
                    formula_space_id=EXPANDED_FORMULA_SPACE_ID,
                    rng=rng,
                )
                if optimizer_state is not None
                else CategoricalCEMPolicy.fresh(
                    decisions=decisions,
                    decision_catalog_hash=(
                        projection.decision_catalog_hash(
                            EXPANDED_FORMULA_SPACE_ID
                        )
                    ),
                    formula_space_id=EXPANDED_FORMULA_SPACE_ID,
                )
            )
        else:
            policy = UniformPolicy()
        arm_state[arm] = {
            "exact": exact,
            "behavior": archive,
            "closed": closed,
            "rng": rng,
            "policy": policy,
        }

    summaries = []
    for checkpoint_index in range(CHECKPOINT_COUNT):
        order = (
            list(ARMS)
            if checkpoint_index % 2 == 0
            else list(reversed(ARMS))
        )
        for arm in order:
            state = arm_state[arm]
            if checkpoint_index < int(state["closed"]):
                root = (
                    output_root
                    / "arms"
                    / arm
                    / f"checkpoint_{checkpoint_index + 1:03d}"
                )
                summaries.append(
                    json.loads(
                        (root / "checkpoint_summary.json").read_text(
                            encoding="utf-8-sig"
                        )
                    )
                )
                continue
            summaries.append(
                _execute_checkpoint(
                    arm=arm,
                    checkpoint_index=checkpoint_index,
                    output_root=output_root,
                    projection=projection,
                    policy=state["policy"],
                    rng=state["rng"],
                    exact_seen=state["exact"],
                    behavior_archive=state["behavior"],
                    registry=registry,
                    split=split,
                    field_roots=field_roots,
                    label_roots=label_roots,
                    purity_path=purity_path,
                    sidecar_closure=args.sidecar_closure.resolve(),
                    compute_threads=compute_threads,
                    deadline_epoch=deadline_epoch,
                    frozen_contract_path=frozen_path,
                    decision_catalog_path=catalog_path,
                )
            )

    initial_families = {
        str(row.get("portfolio_behavior_family_id") or "")
        for row in initial_behavior.rows
        if str(row.get("portfolio_behavior_family_id") or "")
    }
    arm_metrics = {
        arm: _arm_metrics(output_root, arm, initial_families)
        for arm in ARMS
    }
    comparison = _comparison_verdict(
        arm_a=arm_metrics["arm_a_uniform_old"],
        arm_b=arm_metrics["arm_b_uniform_expanded"],
        arm_c=arm_metrics["arm_c_cem_expanded"],
        static_status=str(static_verdict["status"]),
        behavior_status="PASS" if behavior_gate else "FAIL",
        sampled_full_contract=str(sampled_authority["status"]),
    )
    metrics_path = _write_json(
        output_root / "qualification_metrics.json",
        {
            "extension_candidates": candidate_results,
            "supply_gate": supply,
            "arms": arm_metrics,
            "comparison": comparison,
            "access_boundary": {
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
                "feedback_writes_from_validation": 0,
                "promotion": "FORBIDDEN",
            },
        },
    )
    verdict = {
        **final_gate_fields,
        "SAMPLED_FULL_CONTRACT_PARITY": comparison[
            "SAMPLED_FULL_CONTRACT_PARITY"
        ],
        "FORMULA_SPACE_INCREMENT": comparison[
            "FORMULA_SPACE_INCREMENT"
        ],
        "CEM_SEARCH_INCREMENT": comparison["CEM_SEARCH_INCREMENT"],
        "PERFORMANCE_CONTRACT": comparison["PERFORMANCE_CONTRACT"],
        "TARGET_FAMILY_LARGE_SEARCH_READINESS": comparison[
            "TARGET_FAMILY_LARGE_SEARCH_READINESS"
        ],
    }
    verdict_path = _write_json(
        output_root / "final_decision.json", verdict
    )
    large_contract_path = None
    fresh_state_path = None
    if verdict["TARGET_FAMILY_LARGE_SEARCH_READINESS"] == "READY":
        large_contract_path = _write_json(
            output_root / "target_family_large_search_contract.json",
            {
                "schema_version": (
                    "cn_target_family_large_search_contract_v1"
                ),
                "status": "READY_NOT_LAUNCHED",
                "route_id": ROUTE_ID,
                "skeleton_id": SKELETON_ID,
                "formula_space_id": EXPANDED_FORMULA_SPACE_ID,
                "extension_id": accepted_extension,
                "raw_attempt_cap": 20_000,
                "compile_valid_cap": 6_000,
                "behavior_admitted_cap": 1_500,
                "sampled_selection_cap": 384,
                "full_coordinate_pair_cap": 96,
                "checkpoint_count": 4,
                "raw_attempts_per_checkpoint": 5_000,
                "full_pairs_per_checkpoint_cap": 24,
                "wall_time_stop_hours": 12,
                "cem_share": 0.70,
                "uniform_reserve_share": 0.30,
                "automatic_launch": False,
                "validation": "FORBIDDEN",
                "holdout": "SEALED",
                "forward_2026": "SEALED",
                "promotion": "FORBIDDEN",
            },
        )
        fresh_state_path = _write_json(
            output_root / "optimizer_initial_state.json",
            _fresh_large_search_state(
                projection, seed=args.seed + 900_001
            ),
        )
    artifacts = [
        source_path,
        authorization_path,
        launch_receipt_path,
        sampled_authority_path,
        registry_path,
        schema_path,
        purity_path,
        runtime_path,
        old_reuse_path,
        rejected_path,
        frozen_path,
        catalog_path,
        old_catalog_path,
        parity_path,
        static_path,
        static_verdict_path,
        behavior_metrics_path,
        supply_path,
        fresh_state_parity_path,
        metrics_path,
        verdict_path,
        *candidate_artifacts,
        *(
            [large_contract_path, fresh_state_path]
            if large_contract_path is not None
            and fresh_state_path is not None
            else []
        ),
        *(
            output_root
            / "arms"
            / arm
            / f"checkpoint_{index + 1:03d}"
            / "batch_manifest.json"
            for arm in ARMS
            for index in range(CHECKPOINT_COUNT)
        ),
    ]
    manifest = {
        "schema_version": (
            "cn_targeted_formula_cem_artifact_manifest_v2"
        ),
        "status": "QUALIFICATION_COMPLETE",
        "verdict": verdict,
        "artifacts": [
            _artifact(path, root=output_root)
            for path in artifacts
            if path is not None and path.is_file()
        ],
        "checkpoint_summaries": summaries,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    manifest["manifest_payload_hash"] = _stable_hash(manifest)
    _write_json(output_root / "artifact_manifest.json", manifest)
    _close_campaign_writer(writer_lock_path, task_id=args.task_id)
    return verdict


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", choices=("static", "full"), default="static"
    )
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--discovery-contract", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=2026072417)
    parser.add_argument(
        "--extension-id",
        choices=TARGETED_RETRY_EXTENSION_IDS,
        default=PRE_EVENT_PAYLOAD_CSRANK_EXTENSION_ID,
    )
    parser.add_argument("--source-campaign-root", type=Path)
    parser.add_argument("--qualification-authorization", type=Path)
    parser.add_argument("--source-receipt", type=Path)
    parser.add_argument("--historical-candidate-archive", type=Path)
    parser.add_argument("--historical-archive-manifest", type=Path)
    parser.add_argument("--split-manifest", type=Path)
    parser.add_argument("--sidecar-closure", type=Path)
    parser.add_argument("--active-field-root", type=Path)
    parser.add_argument("--active-label-root", type=Path)
    parser.add_argument("--session-field-root", type=Path)
    parser.add_argument("--session-label-root", type=Path)
    parser.add_argument("--active-threads", type=int, default=30)
    parser.add_argument("--session-threads", type=int, default=2)
    parser.add_argument("--maximum-wall-seconds", type=int, default=43_200)
    parser.add_argument("--task-id")
    args = parser.parse_args(argv)
    if args.mode == "static":
        result = run_static(args)
    else:
        required = (
            "source_campaign_root",
            "qualification_authorization",
            "source_receipt",
            "historical_candidate_archive",
            "historical_archive_manifest",
            "split_manifest",
            "sidecar_closure",
            "active_field_root",
            "active_label_root",
            "session_field_root",
            "session_label_root",
            "task_id",
        )
        missing = [
            name for name in required if getattr(args, name) is None
        ]
        if missing:
            parser.error(
                "full mode missing required arguments: "
                + ",".join(missing)
            )
        result = run_full(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
