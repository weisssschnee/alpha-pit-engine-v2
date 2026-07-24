"""Run the repaired MINUTE_STATIC V3 production OLD-supply gate.

The original V3 preflight incorrectly treated the Core-Pack information-core
discovery projection as the complete production field surface.  This runner
binds a campaign-local production-root contract to the existing Registry,
Grammar, compiler, real development sidecar, and historical exact/behavior
archives.  It performs no label, validation, holdout, or 2026 reads.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import platform
import statistics
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
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
    EXPANDED_FORMULA_SPACE_ID,
    OLD_FORMULA_SPACE_ID,
    DecisionRecord,
    DecisionSpec,
    LegacyParityPolicy,
    SearchChoice,
    UniformPolicy,
)
from our_system_phase2.services.categorical_cem import (
    CategoricalCEMPolicy,
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
)
from scripts.run_targeted_formula_cem_qualification import (
    ARMS,
    CHECKPOINT_COUNT,
    FULL_PAIR_CAP,
    MINIMUM_ACTIVE_CHECKPOINTS,
    MINIMUM_EVALUATED_PAIRS,
    PAIR_BATCH_SIZES,
    _arm_metrics,
    _bind_purity,
    _comparison_verdict,
    _execute_checkpoint,
    _fresh_large_search_state,
    _fresh_large_search_state_parity,
    _load_arm_state,
)
from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _context_and_binding,
)
from our_system_phase2.runtime.cn_search_policy_qualification import (
    _read_rows,
    _verify_artifacts,
)
from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    _run_phase3cm_monitored,
)


AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
AUTHORIZATION_ID = "MINUTE_STATIC_PRODUCTION_LANE_CEM_V3"
PRODUCTION_CONTRACT_VERSION = (
    "cn_minute_static_production_root_contract_v1"
)
ROUTE_ID = "MINUTE_STATIC"
OLD_SKELETON_ID = "cn.comp.v2.minute_static.field_spread"
NORMALIZED_RATIO_SKELETON_ID = (
    "cn.comp.v2.minute_static.normalized_ratio"
)
SAMPLED_AUTHORITY_ID = "MINUTE_STATIC_PHASE3CM_SESSION_SAMPLE_V1"
MINIMUM_EXACT_SUPPLY = 72
MINIMUM_BEHAVIOR_SUPPLY = 48
BEHAVIOR_PROBE_CAP = 256
FROZEN_SUPPLY_SEED = 2026072419
FINANCIAL_SEED = 2026072423
FINANCIAL_BEHAVIOR_PROBE_TARGET = 32
SAMPLED_QUALIFICATION_PAIR_COUNT = 64
SAMPLED_SESSION_FRACTION = 0.25
SAMPLED_SPEARMAN_MINIMUM = 0.30
SAMPLED_SIGN_AGREEMENT_MINIMUM = 0.60
SAMPLED_TOP_RECALL_MINIMUM = 0.75
SAMPLED_SPEED_MULTIPLIER_MINIMUM = 3.0


class MinuteStaticProductionProjection:
    """Read-only optimizer projection over two existing Grammar productions."""

    _production_skeletons = {
        "field_spread": OLD_SKELETON_ID,
        "normalized_ratio": NORMALIZED_RATIO_SKELETON_ID,
    }

    def __init__(self, generator: RegistryDrivenGenerator) -> None:
        self.generator = generator
        lanes = {
            production_id: generator.categorical_gene_space(
                ROUTE_ID,
                skeleton_id=skeleton_id,
            )
            for production_id, skeleton_id
            in self._production_skeletons.items()
        }
        pair_domains = {
            production_id: tuple(
                lane["ordered_categories_by_slot"]["field_pair_id"]
            )
            for production_id, lane in lanes.items()
        }
        if len(set(pair_domains.values())) != 1:
            raise RuntimeError(
                "MINUTE_PRODUCTION_FIELD_PAIR_DOMAIN_DRIFT"
            )
        self.field_pair_ids = next(iter(pair_domains.values()))
        if not self.field_pair_ids:
            raise RuntimeError("MINUTE_PRODUCTION_FIELD_PAIR_DOMAIN_EMPTY")
        self.gene_surface_id = str(
            lanes["field_spread"]["ordered_categories_by_slot"][
                "gene_surface_id"
            ][0]
        )

    def decision_specs(
        self,
        formula_space_id: str,
    ) -> tuple[DecisionSpec, ...]:
        if formula_space_id == OLD_FORMULA_SPACE_ID:
            productions = ("field_spread",)
        elif formula_space_id == EXPANDED_FORMULA_SPACE_ID:
            productions = ("field_spread", "normalized_ratio")
        else:
            raise ValueError("unknown formula space: " + formula_space_id)
        base = (
            f"route={ROUTE_ID}|formula_space={formula_space_id}"
        )
        production = DecisionSpec(
            decision_id="minute_static.production_id",
            context_id=base + "|decision=production_id",
            decision_type="PRODUCTION",
            gene_slot="production_id",
            ordered_choices=tuple(
                SearchChoice(
                    token_id=production_id,
                    gene_value=self._production_skeletons[
                        production_id
                    ],
                    semantic_value={
                        "production_id": production_id,
                        "skeleton_id": self._production_skeletons[
                            production_id
                        ],
                        "authority": "CompositionalGrammarV2",
                    },
                )
                for production_id in productions
            ),
        )
        field_pair = DecisionSpec(
            decision_id="minute_static.field_pair_id",
            context_id=base + "|decision=field_pair_id",
            decision_type="FIELD_PAIR",
            gene_slot="field_pair_id",
            ordered_choices=tuple(
                SearchChoice(
                    token_id="field_pair:" + pair_id,
                    gene_value=pair_id,
                    semantic_value={
                        "field_pair_id": pair_id,
                        "left_field_id": pair_id.split("::", 1)[0],
                        "right_field_id": pair_id.split("::", 1)[1],
                        "compatibility_authority": (
                            "CompositionalGrammarV2"
                        ),
                    },
                )
                for pair_id in self.field_pair_ids
            ),
        )
        return production, field_pair

    def decision_catalog(self, formula_space_id: str) -> dict[str, Any]:
        rows = self.decision_specs(formula_space_id)
        payload = {
            "schema_version": (
                "cn_minute_static_production_decision_catalog_v1"
            ),
            "route_id": ROUTE_ID,
            "formula_space_id": formula_space_id,
            "fixed_gene_surface_id": self.gene_surface_id,
            "decisions": [row.to_dict() for row in rows],
            "learnable_contexts": [
                "production_id",
                "field_pair_id",
            ],
            "left_right_independent_probabilities": "FORBIDDEN",
        }
        payload["catalog_hash"] = _stable_hash(payload)
        return payload

    def decision_catalog_hash(self, formula_space_id: str) -> str:
        return str(self.decision_catalog(formula_space_id)["catalog_hash"])

    def generate(
        self,
        *,
        formula_space_id: str,
        policy: Any,
        rng: np.random.Generator,
    ) -> GeneratedPair:
        decisions = self.decision_specs(formula_space_id)
        records = []
        selected: dict[str, str] = {}
        for decision in decisions:
            token_id = str(policy.choose(decision, rng=rng))
            choice = next(
                row
                for row in decision.ordered_choices
                if row.token_id == token_id
            )
            selected[decision.gene_slot] = str(choice.gene_value)
            records.append(
                {
                    "decision_id": decision.decision_id,
                    "context_id": decision.context_id,
                    "decision_type": decision.decision_type,
                    "selected_token_id": token_id,
                }
            )
        pair = self.generator.propose_categorical_genes(
            ROUTE_ID,
            genes={
                "skeleton_id": selected["production_id"],
                "gene_surface_id": self.gene_surface_id,
                "field_pair_id": selected["field_pair_id"],
            },
        )
        trace_hash = _stable_hash(records)
        production_id = next(
            key for key, value in self._production_skeletons.items()
            if value == selected["production_id"]
        )
        shared = {
            "formula_space_id": formula_space_id,
            "production_id": production_id,
            "decision_trace": copy.deepcopy(records),
            "decision_trace_hash": trace_hash,
        }
        return GeneratedPair(
            {**dict(pair.candidate), **shared},
            {**dict(pair.control), **shared},
        )


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


def _verify_reused_supply(root: Path) -> dict[str, Any]:
    manifest_path = root / "artifact_manifest.json"
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8-sig")
    )
    _verify_artifacts(root, manifest)
    gate = json.loads(
        (root / "old_supply_gate.json").read_text(
            encoding="utf-8-sig"
        )
    )
    if (
        str(manifest.get("status") or "")
        != "OLD_SUPPLY_QUALIFIED_READY_FOR_PARITY"
        or gate.get("exact_gate") != "PASS"
        or gate.get("behavior_gate") != "PASS"
        or int(gate.get("post_archive_exact_supply") or 0)
        < MINIMUM_EXACT_SUPPLY
        or int(gate.get("behavior_unique_pairs") or 0)
        < MINIMUM_BEHAVIOR_SUPPLY
    ):
        raise RuntimeError("REUSED_OLD_SUPPLY_NOT_QUALIFIED")
    return {
        "status": "PASS_REUSED_HASH_VERIFIED",
        "manifest": _input_artifact(manifest_path),
        "exact_supply": int(gate["post_archive_exact_supply"]),
        "behavior_unique_pairs": int(gate["behavior_unique_pairs"]),
    }


def _production_parity(
    projection: MinuteStaticProductionProjection,
) -> dict[str, Any]:
    failures = []
    checked = 0
    for formula_space_id, productions in (
        (OLD_FORMULA_SPACE_ID, ("field_spread",)),
        (
            EXPANDED_FORMULA_SPACE_ID,
            ("field_spread", "normalized_ratio"),
        ),
    ):
        decisions = projection.decision_specs(formula_space_id)
        by_slot = {row.gene_slot: row for row in decisions}
        for production_id in productions:
            skeleton_id = projection._production_skeletons[
                production_id
            ]
            production_token = next(
                row.token_id
                for row in by_slot["production_id"].ordered_choices
                if row.gene_value == skeleton_id
            )
            for field_pair_id in projection.field_pair_ids:
                direct = projection.generator.propose_categorical_genes(
                    ROUTE_ID,
                    genes={
                        "skeleton_id": skeleton_id,
                        "gene_surface_id": projection.gene_surface_id,
                        "field_pair_id": field_pair_id,
                    },
                )
                replay = projection.generate(
                    formula_space_id=formula_space_id,
                    policy=LegacyParityPolicy(
                        {
                            by_slot["production_id"].decision_id: (
                                production_token
                            ),
                            by_slot["field_pair_id"].decision_id: (
                                "field_pair:" + field_pair_id
                            ),
                        }
                    ),
                    rng=np.random.default_rng(0),
                )
                checks = {
                    "primary_exact": (
                        direct.candidate["exact_identity"]
                        == replay.candidate["exact_identity"]
                    ),
                    "primary_canonical": (
                        direct.candidate["canonical_identity"]
                        == replay.candidate["canonical_identity"]
                    ),
                    "control_exact": (
                        direct.control["exact_identity"]
                        == replay.control["exact_identity"]
                    ),
                    "control_canonical": (
                        direct.control["canonical_identity"]
                        == replay.control["canonical_identity"]
                    ),
                    "pair_id": (
                        direct.candidate["pair_id"]
                        == replay.candidate["pair_id"]
                        == direct.control["pair_id"]
                        == replay.control["pair_id"]
                    ),
                    "field_pair": (
                        direct.candidate["categorical_genes"][
                            "field_pair_id"
                        ]
                        == field_pair_id
                        == replay.candidate["categorical_genes"][
                            "field_pair_id"
                        ]
                    ),
                    "matched_control_binding": (
                        direct.candidate[
                            "pair_mapping_portfolio_contract"
                        ]
                        == replay.candidate[
                            "pair_mapping_portfolio_contract"
                        ]
                    ),
                    "route_skeleton_clock_maturity": all(
                        direct.candidate.get(key)
                        == replay.candidate.get(key)
                        for key in (
                            "route_id",
                            "skeleton_id",
                            "clock_contract",
                            "maturity_contract",
                        )
                    ),
                    "normalized_ratio_existing_constructor": (
                        production_id != "normalized_ratio"
                        or (
                            "SafeDiv(" in direct.candidate["expression"]
                            and ",0.05)" in direct.candidate["expression"]
                        )
                    ),
                }
                checked += 1
                if not all(checks.values()):
                    failures.append(
                        {
                            "formula_space_id": formula_space_id,
                            "production_id": production_id,
                            "field_pair_id": field_pair_id,
                            "failed_checks": sorted(
                                key
                                for key, value in checks.items()
                                if not value
                            ),
                        }
                    )
    return {
        "schema_version": "cn_minute_production_parity_v1",
        "status": "PASS" if not failures else "FAIL",
        "checked_projection_rows": checked,
        "failure_count": len(failures),
        "failures": failures[:20],
        "old_production": "field_spread",
        "added_existing_production": "normalized_ratio",
        "grammar_authority": "CompositionalGrammarV2",
        "compiler_authority": "TypedRouteCompiler",
    }


def _session_sample_contract(
    split: FixedSplitAuthority,
    *,
    seed: int,
) -> dict[str, Any]:
    by_month: dict[str, list[str]] = defaultdict(list)
    for row in split.rows:
        if row["split"] == "train":
            by_month[row["trade_date"][:7]].append(row["trade_date"])
    selected = []
    month_receipts = []
    for month, dates in sorted(by_month.items()):
        target = max(1, int(round(len(dates) * SAMPLED_SESSION_FRACTION)))
        ordered = sorted(
            dates,
            key=lambda session_id: _stable_hash(
                {"sample_seed": seed, "session_id": session_id}
            ),
        )
        chosen = sorted(ordered[:target])
        selected.extend(chosen)
        month_receipts.append(
            {
                "calendar_month": month,
                "full_session_count": len(dates),
                "selected_session_count": len(chosen),
                "selected_sessions": chosen,
            }
        )
    payload = {
        "schema_version": "cn_phase3cm_session_sample_contract_v1",
        "authority_id": SAMPLED_AUTHORITY_ID,
        "status": "FROZEN_DETERMINISTIC_SESSION_SUBSET",
        "evaluation_role": "train",
        "full_split_manifest_hash": split.manifest_hash,
        "sample_seed": int(seed),
        "sample_fraction": SAMPLED_SESSION_FRACTION,
        "selection": "sha256(sample_seed,session_id)_calendar_month",
        "stratification": "calendar_month",
        "full_train_session_count": sum(
            row["split"] == "train" for row in split.rows
        ),
        "selected_session_count": len(selected),
        "selected_sessions": sorted(selected),
        "month_receipts": month_receipts,
        "uses_return_or_regime_labels": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _normalize_candidate_row(row: Mapping[str, Any]) -> dict[str, Any]:
    output = dict(row)
    for key, value in tuple(output.items()):
        if (
            isinstance(value, str)
            and value[:1] in {"[", "{"}
        ):
            try:
                output[key] = json.loads(value)
            except json.JSONDecodeError:
                pass
    return output


def _source_minute_corpus(
    *,
    candidate_ledger: Path,
    observation_ledger: Path,
    pair_count: int,
    seed: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = [
        _normalize_candidate_row(row)
        for row in _read_rows(candidate_ledger)
        if str(row.get("route_id") or "") == ROUTE_ID
    ]
    observations = [
        dict(row)
        for row in _read_rows(observation_ledger)
        if (
            str(row.get("route_id") or "") == ROUTE_ID
            and str(row.get("pair_evaluation_status") or "")
            == "PAIR_EVALUATED"
            and math.isfinite(
                float(row.get("matched_net_increment"))
            )
        )
    ]
    by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        by_pair[str(row.get("pair_id") or "")].append(row)
    eligible = [
        row for row in observations
        if len(by_pair.get(str(row.get("pair_id") or ""), ())) == 2
    ]
    selected_observations = sorted(
        eligible,
        key=lambda row: _stable_hash(
            {
                "seed": seed,
                "pair_id": str(row["pair_id"]),
            }
        ),
    )[:pair_count]
    if len(selected_observations) < pair_count:
        raise RuntimeError(
            "SAMPLED_AUTHORITY_COMPARABLE_CORPUS_BELOW_"
            + str(pair_count)
        )
    selected_ids = {
        str(row["pair_id"]) for row in selected_observations
    }
    selected_candidates = [
        row for row in candidates
        if str(row.get("pair_id") or "") in selected_ids
    ]
    if len(selected_candidates) != 2 * pair_count:
        raise RuntimeError("SAMPLED_AUTHORITY_CANDIDATE_MEMBER_DRIFT")
    return selected_candidates, selected_observations


def _source_full_pairs_per_hour(source_root: Path) -> float:
    pairs = 0
    wall = 0.0
    for path in source_root.glob(
        "checkpoints/checkpoint_*/phase3cm/"
        "active_bar/CN_STREAMING_BACKEND_RESULT.json"
    ):
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if (
            payload.get("status")
            == "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED"
        ):
            pairs += int(payload.get("pair_count") or 0)
            wall += float(payload.get("wall_seconds") or 0.0)
    if not pairs or wall <= 0.0:
        raise RuntimeError("SOURCE_FULL_PHASE3CM_SPEED_EVIDENCE_MISSING")
    return pairs * 3600.0 / wall


def _qualify_sampled_authority(
    *,
    root: Path,
    registry: UnifiedCapabilityRegistry,
    split: FixedSplitAuthority,
    session_sample_path: Path,
    source_root: Path,
    source_candidate_ledger: Path,
    source_observation_ledger: Path,
    field_root: Path,
    label_root: Path,
    purity_path: Path,
    sidecar_closure: Path,
    compute_threads: int,
    deadline_epoch: float,
) -> dict[str, Any]:
    candidates, full_observations = _source_minute_corpus(
        candidate_ledger=source_candidate_ledger,
        observation_ledger=source_observation_ledger,
        pair_count=SAMPLED_QUALIFICATION_PAIR_COUNT,
        seed=FINANCIAL_SEED + 101,
    )
    binding_path, tables = _context_and_binding(
        batch_root=root,
        candidates=candidates,
        registry=registry,
        split=split,
        data_release_hash=_sha256(sidecar_closure),
    )
    _bind_purity(binding_path, purity_path)
    receipts = _run_phase3cm_monitored(
        checkpoint_id="minute_static.sampled_authority",
        checkpoint_root=root,
        binding_path=binding_path,
        table_paths=tables,
        split_manifest=split.manifest_path,
        field_roots={"active_bar": field_root},
        label_roots={"active_bar": label_root},
        purity_path=purity_path,
        compute_threads={"active_bar": compute_threads},
        deadline_epoch=deadline_epoch,
        selected_backends=("active_bar",),
        pair_batch_sizes={"active_bar": 4},
        session_sample_manifest=session_sample_path,
    )
    result_path = (
        root
        / "phase3cm"
        / "active_bar"
        / "CN_STREAMING_BACKEND_RESULT.json"
    )
    result = json.loads(
        result_path.read_text(encoding="utf-8-sig")
    )
    sampled_by_pair = {
        str(row["pair_id"]): dict(row)
        for row in result.get("pair_results") or ()
        if str(row.get("pair_evaluation_status") or "")
        == "PAIR_EVALUATED"
    }
    full_by_pair = {
        str(row["pair_id"]): dict(row)
        for row in full_observations
    }
    comparable_ids = sorted(set(sampled_by_pair) & set(full_by_pair))
    if len(comparable_ids) != SAMPLED_QUALIFICATION_PAIR_COUNT:
        raise RuntimeError("SAMPLED_AUTHORITY_OUTCOME_SET_INCOMPLETE")
    sampled_values = np.asarray(
        [
            float(sampled_by_pair[pair_id]["matched_net_increment"])
            for pair_id in comparable_ids
        ],
        dtype=float,
    )
    full_values = np.asarray(
        [
            float(full_by_pair[pair_id]["matched_net_increment"])
            for pair_id in comparable_ids
        ],
        dtype=float,
    )
    sampled_rank = pd.Series(sampled_values).rank(method="average")
    full_rank = pd.Series(full_values).rank(method="average")
    spearman = float(sampled_rank.corr(full_rank))
    sign_agreement = float(
        np.mean(np.sign(sampled_values) == np.sign(full_values))
    )
    full_top_count = max(1, int(math.ceil(len(full_values) * 0.25)))
    sampled_half_count = max(
        1, int(math.ceil(len(sampled_values) * 0.50))
    )
    full_top = set(
        np.argsort(full_values)[-full_top_count:].tolist()
    )
    sampled_half = set(
        np.argsort(sampled_values)[-sampled_half_count:].tolist()
    )
    top_recall = len(full_top & sampled_half) / len(full_top)
    sampled_pairs_per_hour = (
        len(comparable_ids)
        * 3600.0
        / max(float(result.get("wall_seconds") or 0.0), 1e-9)
    )
    full_pairs_per_hour = _source_full_pairs_per_hour(source_root)
    speed_multiple = sampled_pairs_per_hour / full_pairs_per_hour

    members_by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        members_by_pair[str(row["pair_id"])].append(row)
    contract_pair_ids = sorted(
        comparable_ids,
        key=lambda pair_id: _stable_hash(
            {"contract_seed": FINANCIAL_SEED + 202, "pair_id": pair_id}
        ),
    )[:4]
    contract_rows = []
    for pair_id in contract_pair_ids:
        members = members_by_pair[pair_id]
        primary = next(
            row for row in members
            if str(row.get("pair_member_role") or "") == "PRIMARY"
        )
        control = next(
            row for row in members
            if str(row.get("pair_member_role") or "") == "CONTROL"
        )
        checks = {
            "candidate_identity": all(
                str(row.get("exact_identity") or "")
                and str(row.get("canonical_identity") or "")
                for row in members
            ),
            "primary_control_identity": (
                primary["candidate_id"] != control["candidate_id"]
                and primary["pair_id"] == control["pair_id"]
            ),
            "direction_and_long_only": (
                result.get("portfolio_mode") == "long_only_top"
            ),
            "cost": float(result.get("cost_bps") or -1.0) == 5.0,
            "horizons": result.get("horizons") == [1, 5, 15, 30],
            "field_lineage": (
                set(map(str, primary.get("field_ids") or ()))
                == set(map(str, control.get("field_ids") or ()))
            ),
            "matched_control_binding": (
                primary.get("pair_mapping_portfolio_contract")
                == control.get("pair_mapping_portfolio_contract")
            ),
            "development_only_access": (
                result.get("evaluation_role") == "train"
                and int(result.get("validation_reads") or 0) == 0
                and int(result.get("holdout_reads") or 0) == 0
                and int(result.get("forward_2026_reads") or 0) == 0
            ),
        }
        contract_rows.append(
            {
                "pair_id": pair_id,
                "checks": checks,
                "status": "PASS" if all(checks.values()) else "FAIL",
            }
        )
    checks = {
        "minimum_comparable_pairs_64": (
            len(comparable_ids) >= SAMPLED_QUALIFICATION_PAIR_COUNT
        ),
        "four_pair_contract": all(
            row["status"] == "PASS" for row in contract_rows
        ),
        "spearman_at_least_0_30": (
            spearman >= SAMPLED_SPEARMAN_MINIMUM
        ),
        "sign_agreement_at_least_0_60": (
            sign_agreement >= SAMPLED_SIGN_AGREEMENT_MINIMUM
        ),
        "top_quartile_recall_at_least_0_75": (
            top_recall >= SAMPLED_TOP_RECALL_MINIMUM
        ),
        "speed_at_least_3x": (
            speed_multiple >= SAMPLED_SPEED_MULTIPLIER_MINIMUM
        ),
    }
    qualified = all(checks.values())
    return {
        "schema_version": (
            "cn_minute_static_sampled_phase3cm_qualification_v1"
        ),
        "authority_id": SAMPLED_AUTHORITY_ID,
        "status": "QUALIFIED" if qualified else "NOT_QUALIFIED",
        "comparable_pair_count": len(comparable_ids),
        "spearman_rank_correlation": spearman,
        "sign_agreement": sign_agreement,
        "full_top_quartile_recall_in_sampled_top_half": top_recall,
        "sampled_pairs_per_hour": sampled_pairs_per_hour,
        "full_pairs_per_hour": full_pairs_per_hour,
        "speed_multiple": speed_multiple,
        "checks": checks,
        "contract_pairs": contract_rows,
        "session_sample_manifest": _input_artifact(
            session_sample_path
        ),
        "result": _input_artifact(result_path),
        "access_receipts": receipts,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "PENDING_REPOSITORY_AUTHORITY_UPDATE"
        if qualified
        else "FORBIDDEN",
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


def run_financial(args: argparse.Namespace) -> dict[str, Any]:
    if (
        not args.allow_noncanonical_host
        and platform.node().upper() != AUTHORIZED_HOST
    ):
        raise RuntimeError(
            f"official V3 evidence must run on {AUTHORIZED_HOST}"
        )
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    lock_path = output_root / "campaign_writer.json"
    lock = {
        "task_id": str(args.task_id),
        "pid": os.getpid(),
        "status": "ACTIVE",
    }
    if lock_path.exists():
        existing = json.loads(
            lock_path.read_text(encoding="utf-8-sig")
        )
        if (
            str(existing.get("task_id") or "") != str(args.task_id)
            or str(existing.get("status") or "") != "ACTIVE"
        ):
            raise RuntimeError("DUPLICATE_CAMPAIGN_WRITER")
        _write_json(lock_path, lock)
    else:
        try:
            with lock_path.open(
                "x", encoding="utf-8", newline="\n"
            ) as handle:
                json.dump(lock, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
        except FileExistsError as exc:
            raise RuntimeError("DUPLICATE_CAMPAIGN_WRITER") from exc

    supply_reuse = _verify_reused_supply(
        args.reused_supply_root.resolve()
    )
    supply_reuse_path = _write_json(
        output_root / "old_supply_reuse_receipt.json",
        supply_reuse,
    )
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    contract, route_roots = _load_production_contract(
        args.production_root_contract.resolve(),
        registry=registry,
    )
    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist={ROUTE_ID: route_roots},
    )
    projection = MinuteStaticProductionProjection(generator)
    parity = _production_parity(projection)
    parity_path = _write_json(
        output_root / "production_parity.json", parity
    )
    if parity["status"] != "PASS":
        raise RuntimeError("MINUTE_PRODUCTION_LANE_PARITY_NOT_PROVEN")
    old_catalog_path = _write_json(
        output_root / "decision_catalog_old.json",
        projection.decision_catalog(OLD_FORMULA_SPACE_ID),
    )
    expanded_catalog_path = _write_json(
        output_root / "decision_catalog_expanded.json",
        projection.decision_catalog(EXPANDED_FORMULA_SPACE_ID),
    )
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    session_sample = _session_sample_contract(
        split, seed=FINANCIAL_SEED + 77
    )
    session_sample_path = _write_json(
        output_root / "session_sample_contract.json",
        session_sample,
    )
    frozen_contract = {
        "schema_version": (
            "cn_minute_static_production_cem_v3_financial_contract_v1"
        ),
        "status": "FROZEN_EXECUTABLE",
        "authorization_id": AUTHORIZATION_ID,
        "route_id": ROUTE_ID,
        "old_production": "field_spread",
        "added_existing_production": "normalized_ratio",
        "arms": list(ARMS),
        "checkpoint_count": CHECKPOINT_COUNT,
        "full_coordinate_pair_cap_per_checkpoint": FULL_PAIR_CAP,
        "behavior_probe_cap": 128,
        "behavior_probe_target": FINANCIAL_BEHAVIOR_PROBE_TARGET,
        "sampled_selection_cap": 48,
        "minimum_evaluated_pairs_per_arm": (
            MINIMUM_EVALUATED_PAIRS
        ),
        "minimum_active_checkpoints_per_arm": (
            MINIMUM_ACTIVE_CHECKPOINTS
        ),
        "production_root_contract_hash": contract["contract_hash"],
        "session_sample_contract_hash": session_sample[
            "contract_hash"
        ],
        "decision_surface": [
            "production_id",
            "field_pair_id",
        ],
        "left_right_independent_probabilities": "FORBIDDEN",
        "portfolio_mode": "long_only_top",
        "cost_bps": 5.0,
        "horizons": [1, 5, 15, 30],
        "active_bar_threads": int(args.compute_threads),
        "pair_batch_size": 4,
        "cache_cap_bytes": 8 * 1024**3,
        "minimum_free_memory_bytes": 24 * 1024**3,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    frozen_contract["contract_hash"] = _stable_hash(frozen_contract)
    frozen_path = _write_json(
        output_root / "frozen_contract.json", frozen_contract
    )
    fresh_state_parity = _fresh_large_search_state_parity(
        projection, seed=FINANCIAL_SEED + 900_001
    )
    fresh_state_parity_path = _write_json(
        output_root / "fresh_large_search_state_parity.json",
        fresh_state_parity,
    )
    if fresh_state_parity["status"] != "PASS":
        raise RuntimeError("FRESH_LARGE_SEARCH_STATE_PARITY_FAILED")

    deadline_epoch = time.time() + int(args.maximum_wall_seconds)
    sampled_root = output_root / "sampled_authority"
    sampled_root.mkdir(parents=True, exist_ok=True)
    sampled_qualification = _qualify_sampled_authority(
        root=sampled_root,
        registry=registry,
        split=split,
        session_sample_path=session_sample_path,
        source_root=args.source_campaign_root.resolve(),
        source_candidate_ledger=(
            args.source_candidate_ledger.resolve()
        ),
        source_observation_ledger=(
            args.source_observation_ledger.resolve()
        ),
        field_root=args.active_field_root.resolve(),
        label_root=args.active_label_root.resolve(),
        purity_path=args.split_boundary_purity.resolve(),
        sidecar_closure=args.sidecar_closure.resolve(),
        compute_threads=int(args.compute_threads),
        deadline_epoch=deadline_epoch,
    )
    sampled_qualification_path = _write_json(
        output_root / "sampled_authority_qualification.json",
        sampled_qualification,
    )
    sampled_qualified = (
        sampled_qualification["status"] == "QUALIFIED"
    )

    initial_exact = _historical_exact(
        args.historical_exact_archive.resolve(),
        args.source_candidate_ledger.resolve(),
    )
    initial_behavior = PortfolioBehaviorArchive.read_parquet(
        args.historical_behavior_archive.resolve()
    )
    field_roots = {
        "active_bar": args.active_field_root.resolve()
    }
    label_roots = {
        "active_bar": args.active_label_root.resolve()
    }
    compute_threads = {
        "active_bar": int(args.compute_threads)
    }
    arm_state: dict[str, dict[str, Any]] = {}
    for arm_index, arm in enumerate(ARMS):
        exact, archive, closed, rng_state, optimizer_state = (
            _load_arm_state(
                output_root=output_root,
                arm=arm,
                initial_exact=initial_exact,
                initial_behavior=initial_behavior,
            )
        )
        rng = np.random.default_rng(
            FINANCIAL_SEED + 10_000 * (arm_index + 1)
        )
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

    for checkpoint_index in range(CHECKPOINT_COUNT):
        order = (
            list(ARMS)
            if checkpoint_index % 2 == 0
            else list(reversed(ARMS))
        )
        for arm in order:
            state = arm_state[arm]
            if checkpoint_index < int(state["closed"]):
                continue
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
                purity_path=args.split_boundary_purity.resolve(),
                sidecar_closure=args.sidecar_closure.resolve(),
                compute_threads=compute_threads,
                deadline_epoch=deadline_epoch,
                frozen_contract_path=frozen_path,
                decision_catalog_path=expanded_catalog_path,
                route_id=ROUTE_ID,
                skeleton_id=OLD_SKELETON_ID,
                selected_backends=("active_bar",),
                pair_batch_sizes={"active_bar": 4},
                session_sample_manifest=session_sample_path,
                sampled_authority_qualified=sampled_qualified,
                behavior_probe_target=(
                    FINANCIAL_BEHAVIOR_PROBE_TARGET
                ),
            )

    initial_families = {
        str(row.get("portfolio_behavior_family_id") or "")
        for row in initial_behavior.rows
        if str(row.get("portfolio_behavior_family_id") or "")
    }
    arm_metrics = {
        arm: _arm_metrics(
            output_root,
            arm,
            initial_families,
            backend_name="active_bar",
        )
        for arm in ARMS
    }
    comparison = _comparison_verdict(
        arm_a=arm_metrics["arm_a_uniform_old"],
        arm_b=arm_metrics["arm_b_uniform_expanded"],
        arm_c=arm_metrics["arm_c_cem_expanded"],
        static_status="PASS",
        behavior_status="PASS",
        sampled_full_contract=(
            "PASS" if sampled_qualified else "FAIL"
        ),
    )
    blockers = []
    if not sampled_qualified:
        blockers.append("SAMPLED_PHASE3CM_AUTHORITY_NOT_QUALIFIED")
    if not all(comparison["minimum_support"].values()):
        blockers.append("INSUFFICIENT_FINANCIAL_COMPARISON_SUPPORT")
    if comparison["FORMULA_SPACE_INCREMENT"] != "QUALIFIED":
        blockers.append("FORMULA_SPACE_INCREMENT_NOT_QUALIFIED")
    if comparison["CEM_SEARCH_INCREMENT"] != "QUALIFIED":
        blockers.append("CEM_SEARCH_INCREMENT_NOT_QUALIFIED")
    if comparison["PERFORMANCE_CONTRACT"] != "PASS":
        blockers.append("PERFORMANCE_CONTRACT_NOT_PASS")
    final = {
        "SAMPLED_PHASE3CM_AUTHORITY": (
            sampled_qualification["status"]
        ),
        "FORMULA_SPACE_INCREMENT": comparison[
            "FORMULA_SPACE_INCREMENT"
        ],
        "CEM_SEARCH_INCREMENT": comparison["CEM_SEARCH_INCREMENT"],
        "PERFORMANCE_CONTRACT": comparison["PERFORMANCE_CONTRACT"],
        "TARGET_FAMILY_LARGE_SEARCH_READINESS": comparison[
            "TARGET_FAMILY_LARGE_SEARCH_READINESS"
        ],
        "READINESS_BLOCKERS": blockers,
    }
    metrics_path = _write_json(
        output_root / "qualification_metrics.json",
        {
            "old_supply": supply_reuse,
            "production_parity": parity,
            "sampled_authority": sampled_qualification,
            "arms": arm_metrics,
            "comparison": comparison,
            "access_boundary": {
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
                "promotion": "FORBIDDEN",
            },
        },
    )
    final_path = _write_json(
        output_root / "final_decision.json", final
    )
    large_state_path = None
    if final["TARGET_FAMILY_LARGE_SEARCH_READINESS"] == "READY":
        large_state_path = _write_json(
            output_root / "optimizer_initial_state.json",
            _fresh_large_search_state(
                projection, seed=FINANCIAL_SEED + 900_001
            ),
        )
    artifact_paths = [
        supply_reuse_path,
        parity_path,
        old_catalog_path,
        expanded_catalog_path,
        session_sample_path,
        frozen_path,
        fresh_state_parity_path,
        sampled_qualification_path,
        metrics_path,
        final_path,
        *(
            [large_state_path]
            if large_state_path is not None
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
            "cn_minute_static_production_cem_v3_manifest_v2"
        ),
        "status": "CAMPAIGN_CLOSED",
        "authorization_id": AUTHORIZATION_ID,
        "repo_sha": str(args.repo_sha),
        "task_id": str(args.task_id),
        "verdict": final,
        "artifacts": [
            {
                "path": path.relative_to(output_root).as_posix(),
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
            for path in artifact_paths
            if path is not None and path.is_file()
        ],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    manifest["manifest_payload_hash"] = _stable_hash(manifest)
    manifest_path = _write_json(
        output_root / "artifact_manifest.json", manifest
    )
    _write_json(
        lock_path,
        {
            **lock,
            "status": "CLOSED",
            "closed_at": pd.Timestamp.now("UTC").isoformat(),
        },
    )
    return {
        "status": "CAMPAIGN_CLOSED",
        "final_decision": final,
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
    parser.add_argument("--continue-financial", action="store_true")
    parser.add_argument("--reused-supply-root", type=Path)
    parser.add_argument("--source-campaign-root", type=Path)
    parser.add_argument("--source-observation-ledger", type=Path)
    parser.add_argument("--active-field-root", type=Path)
    parser.add_argument("--active-label-root", type=Path)
    parser.add_argument("--split-boundary-purity", type=Path)
    parser.add_argument("--sidecar-closure", type=Path)
    parser.add_argument(
        "--maximum-wall-seconds",
        type=int,
        default=43_200,
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--task-id", default="")
    parser.add_argument("--allow-noncanonical-host", action="store_true")
    parser.add_argument("--static-only", action="store_true")
    args = parser.parse_args()
    if args.continue_financial:
        required = (
            "reused_supply_root",
            "source_campaign_root",
            "source_observation_ledger",
            "active_field_root",
            "active_label_root",
            "split_boundary_purity",
            "sidecar_closure",
            "historical_behavior_archive",
            "split_manifest",
        )
        missing = [
            name for name in required
            if getattr(args, name) is None
        ]
        if missing:
            parser.error(
                "--continue-financial missing: " + ",".join(missing)
            )
    elif not args.static_only and (
        args.historical_behavior_archive is None
        or args.split_manifest is None
    ):
        parser.error(
            "--historical-behavior-archive and --split-manifest "
            "are required unless --static-only"
        )
    result = (
        run_financial(args)
        if args.continue_financial
        else run(args)
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
