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
    AvailableUniformPolicy,
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
    RankWeightedCategoricalCEMPolicy,
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
    _bind_session_sample,
    _comparison_verdict,
    _execute_checkpoint,
    _fresh_large_search_state,
    _fresh_large_search_state_parity,
    _load_arm_state,
)
from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _context_and_binding,
    _outcome_rows,
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
PAIRED_STRUCTURAL_CEM_V2_ARMS = (
    "arm_b_uniform_expanded",
    "arm_c_structural_cem_v2",
)
PAIRED_STRUCTURAL_CEM_V2_CHECKPOINT_COUNT = 2
PAIRED_STRUCTURAL_CEM_V2_FULL_PAIR_CAP = 12


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
        self._available_candidate_cache: dict[
            str, tuple[dict[str, Any], ...]
        ] = {}

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

    def structural_decision_specs(
        self,
        formula_space_id: str,
    ) -> tuple[DecisionSpec, ...]:
        """Expose only reusable structural choices to adaptive policy."""

        production, _ = self.decision_specs(formula_space_id)
        return (production,)

    def structural_decision_catalog(
        self,
        formula_space_id: str,
    ) -> dict[str, Any]:
        rows = self.structural_decision_specs(formula_space_id)
        payload = {
            "schema_version": (
                "cn_minute_static_structural_decision_catalog_v2"
            ),
            "route_id": ROUTE_ID,
            "formula_space_id": formula_space_id,
            "fixed_gene_surface_id": self.gene_surface_id,
            "adaptive_decisions": [row.to_dict() for row in rows],
            "concrete_field_pair_selection": (
                "UNIFORM_WITHOUT_REPLACEMENT_FROM_EXACT_AVAILABLE_SET"
            ),
            "joint_exact_availability_key": (
                "production_id_x_field_pair_id"
            ),
            "field_family_authority": (
                "NOT_DECLARED_IN_CURRENT_REGISTRY_NO_INVENTED_GROUPS"
            ),
        }
        payload["catalog_hash"] = _stable_hash(payload)
        return payload

    def structural_decision_catalog_hash(
        self,
        formula_space_id: str,
    ) -> str:
        return str(
            self.structural_decision_catalog(formula_space_id)[
                "catalog_hash"
            ]
        )

    def _available_candidate_catalog(
        self,
        formula_space_id: str,
    ) -> tuple[dict[str, Any], ...]:
        cached = self._available_candidate_cache.get(formula_space_id)
        if cached is not None:
            return cached
        production = self.structural_decision_specs(
            formula_space_id
        )[0]
        rows = []
        for choice in production.ordered_choices:
            production_id = str(choice.token_id)
            skeleton_id = str(choice.gene_value)
            for pair_id in self.field_pair_ids:
                pair = self.generator.propose_categorical_genes(
                    ROUTE_ID,
                    genes={
                        "skeleton_id": skeleton_id,
                        "gene_surface_id": self.gene_surface_id,
                        "field_pair_id": pair_id,
                    },
                )
                rows.append(
                    {
                        "production_id": production_id,
                        "skeleton_id": skeleton_id,
                        "field_pair_id": pair_id,
                        "candidate": dict(pair.candidate),
                        "control": dict(pair.control),
                    }
                )
        result = tuple(
            sorted(
                rows,
                key=lambda row: (
                    row["production_id"],
                    row["field_pair_id"],
                ),
            )
        )
        self._available_candidate_cache[formula_space_id] = result
        return result

    def generate_available(
        self,
        *,
        formula_space_id: str,
        policy: Any,
        rng: np.random.Generator,
        exact_seen: set[str],
    ) -> GeneratedPair:
        """Draw a structural choice, then one unused concrete exact candidate."""

        available = [
            row
            for row in self._available_candidate_catalog(formula_space_id)
            if str(row["candidate"]["exact_identity"]) not in exact_seen
        ]
        if not available:
            raise RuntimeError("MINUTE_STRUCTURAL_EXACT_SUPPLY_EXHAUSTED")
        by_production: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in available:
            by_production[str(row["production_id"])].append(row)
        production = self.structural_decision_specs(
            formula_space_id
        )[0]
        choose_available = getattr(policy, "choose_available", None)
        if not callable(choose_available):
            raise TypeError(
                "structural V2 policy must implement choose_available"
            )
        selected_production = str(
            choose_available(
                production,
                allowed_token_ids=tuple(by_production),
                rng=rng,
            )
        )
        field_decision = DecisionSpec(
            decision_id="minute_static.field_pair_id",
            context_id=(
                f"route={ROUTE_ID}|formula_space={formula_space_id}|"
                f"production={selected_production}|"
                "decision=field_pair_id|adaptive=false"
            ),
            decision_type="FIELD_PAIR_NON_ADAPTIVE",
            gene_slot="field_pair_id",
            ordered_choices=tuple(
                SearchChoice(
                    token_id="field_pair:" + pair_id,
                    gene_value=pair_id,
                    semantic_value={
                        "field_pair_id": pair_id,
                        "selection": (
                            "UNIFORM_REMAINING_EXACT_AVAILABLE"
                        ),
                    },
                )
                for pair_id in self.field_pair_ids
            ),
        )
        concrete_uniform = AvailableUniformPolicy()
        selected_pair_token = concrete_uniform.choose_available(
            field_decision,
            allowed_token_ids=tuple(
                "field_pair:" + str(row["field_pair_id"])
                for row in by_production[selected_production]
            ),
            rng=rng,
        )
        selected_pair_id = selected_pair_token.split(":", 1)[1]
        selected = next(
            row
            for row in by_production[selected_production]
            if str(row["field_pair_id"]) == selected_pair_id
        )
        trace = [
            DecisionRecord(
                decision_id=production.decision_id,
                context_id=production.context_id,
                decision_type=production.decision_type,
                selected_token_id=selected_production,
            ).to_dict(),
            DecisionRecord(
                decision_id=field_decision.decision_id,
                context_id=field_decision.context_id,
                decision_type=field_decision.decision_type,
                selected_token_id=selected_pair_token,
            ).to_dict(),
        ]
        shared = {
            "formula_space_id": formula_space_id,
            "production_id": selected_production,
            "decision_trace": trace,
            "decision_trace_hash": _stable_hash(trace),
            "generator_policy": str(
                getattr(policy, "policy_id", type(policy).__name__)
            ),
            "exact_availability_mask": "APPLIED_BEFORE_DRAW",
        }
        return GeneratedPair(
            {**dict(selected["candidate"]), **shared},
            {**dict(selected["control"]), **shared},
        )

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


def _structural_v2_fresh_stream_parity(
    projection: MinuteStaticProductionProjection,
    *,
    seed: int,
    count: int = 32,
) -> dict[str, Any]:
    decisions = projection.structural_decision_specs(
        EXPANDED_FORMULA_SPACE_ID
    )
    catalog_hash = projection.structural_decision_catalog_hash(
        EXPANDED_FORMULA_SPACE_ID
    )
    uniform = AvailableUniformPolicy()
    cem = RankWeightedCategoricalCEMPolicy.fresh(
        decisions=decisions,
        decision_catalog_hash=catalog_hash,
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
    )
    uniform_rng = np.random.default_rng(int(seed))
    cem_rng = np.random.default_rng(int(seed))
    uniform_seen: set[str] = set()
    cem_seen: set[str] = set()
    uniform_rows = []
    cem_rows = []
    for _ in range(int(count)):
        left = projection.generate_available(
            formula_space_id=EXPANDED_FORMULA_SPACE_ID,
            policy=uniform,
            rng=uniform_rng,
            exact_seen=uniform_seen,
        )
        right = projection.generate_available(
            formula_space_id=EXPANDED_FORMULA_SPACE_ID,
            policy=cem,
            rng=cem_rng,
            exact_seen=cem_seen,
        )
        left_identity = str(left.candidate["exact_identity"])
        right_identity = str(right.candidate["exact_identity"])
        uniform_rows.append(left_identity)
        cem_rows.append(right_identity)
        uniform_seen.add(left_identity)
        cem_seen.add(right_identity)
    payload = {
        "schema_version": (
            "cn_minute_static_structural_cem_v2_fresh_parity_v1"
        ),
        "seed": int(seed),
        "candidate_count": int(count),
        "uniform_policy": uniform.policy_id,
        "cem_policy": cem.policy_id,
        "exact_candidate_stream_equal": uniform_rows == cem_rows,
        "uniform_exact_unique": len(set(uniform_rows)) == len(uniform_rows),
        "cem_exact_unique": len(set(cem_rows)) == len(cem_rows),
        "uniform_stream_hash": _stable_hash(uniform_rows),
        "cem_stream_hash": _stable_hash(cem_rows),
    }
    payload["status"] = (
        "PASS"
        if all(
            (
                payload["exact_candidate_stream_equal"],
                payload["uniform_exact_unique"],
                payload["cem_exact_unique"],
            )
        )
        else "FAIL"
    )
    return payload


def _verify_reused_sampled_authority(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    root = root.resolve()
    qualification_path = root / "sampled_authority_qualification.json"
    session_path = root / "session_sample_contract.json"
    manifest_path = root / "artifact_manifest.json"
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8-sig")
    )
    artifacts = {
        str(row["path"]): str(row["sha256"])
        for row in manifest.get("artifacts") or ()
    }
    expected = {
        qualification_path.name: _sha256(qualification_path),
        session_path.name: _sha256(session_path),
    }
    drift = [
        name
        for name, digest in expected.items()
        if artifacts.get(name) != digest
    ]
    if drift:
        raise RuntimeError(
            "REUSED_SAMPLED_AUTHORITY_HASH_DRIFT:" + ",".join(drift)
        )
    qualification = json.loads(
        qualification_path.read_text(encoding="utf-8-sig")
    )
    if not bool(
        qualification.get("campaign_local_selector_authorized")
    ):
        raise RuntimeError("REUSED_SAMPLED_AUTHORITY_NOT_QUALIFIED")
    session = json.loads(
        session_path.read_text(encoding="utf-8-sig")
    )
    receipt = {
        "schema_version": (
            "cn_minute_static_reused_sampled_authority_receipt_v1"
        ),
        "status": "REUSED_HASH_VERIFIED_NO_RECOMPUTATION",
        "source_root": str(root),
        "source_manifest_sha256": _sha256(manifest_path),
        "qualification_sha256": expected[qualification_path.name],
        "session_sample_sha256": expected[session_path.name],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    return qualification, session, receipt


def _paired_structural_canary_verdict(
    output_root: Path,
    arm_metrics: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    uniform_arm, cem_arm = PAIRED_STRUCTURAL_CEM_V2_ARMS

    def proposal_stream(arm: str, checkpoint: int) -> list[str]:
        rows = _read_rows(
            output_root
            / "arms"
            / arm
            / f"checkpoint_{checkpoint:03d}"
            / "proposal_ledger.parquet"
        )
        return [
            str(row["exact_identity"])
            for row in sorted(
                rows, key=lambda row: int(row["raw_attempt"])
            )
        ]

    first_uniform = proposal_stream(uniform_arm, 1)
    first_cem = proposal_stream(cem_arm, 1)

    def evaluated_exact_set(arm: str, checkpoint: int) -> list[str]:
        return sorted(
            str(row["exact_identity"])
            for row in _read_rows(
                output_root
                / "arms"
                / arm
                / f"checkpoint_{checkpoint:03d}"
                / "observation_ledger.parquet"
            )
            if str(row.get("outcome_class") or "") == "EVALUATED"
        )

    first_uniform_full = evaluated_exact_set(uniform_arm, 1)
    first_cem_full = evaluated_exact_set(cem_arm, 1)
    duplicate_counts = {}
    for arm in PAIRED_STRUCTURAL_CEM_V2_ARMS:
        duplicate_counts[arm] = []
        for checkpoint in range(
            1, PAIRED_STRUCTURAL_CEM_V2_CHECKPOINT_COUNT + 1
        ):
            summary = json.loads(
                (
                    output_root
                    / "arms"
                    / arm
                    / f"checkpoint_{checkpoint:03d}"
                    / "checkpoint_summary.json"
                ).read_text(encoding="utf-8-sig")
            )
            duplicate_counts[arm].append(
                int(summary["exact_duplicate_pairs"])
            )
    cem_state = json.loads(
        (
            output_root
            / "arms"
            / cem_arm
            / (
                "checkpoint_"
                f"{PAIRED_STRUCTURAL_CEM_V2_CHECKPOINT_COUNT:03d}"
            )
            / "arm_state.json"
        ).read_text(encoding="utf-8-sig")
    )["optimizer_state"]
    contracts = {
        "generation_one_exact_stream_parity": (
            bool(first_uniform)
            and first_uniform == first_cem
        ),
        "generation_one_full_evaluation_set_parity": (
            bool(first_uniform_full)
            and first_uniform_full == first_cem_full
        ),
        "availability_mask_zero_exact_duplicates": all(
            count == 0
            for values in duplicate_counts.values()
            for count in values
        ),
        "support_complete": all(
            int(arm_metrics[arm]["evaluated_pairs"])
            >= (
                PAIRED_STRUCTURAL_CEM_V2_CHECKPOINT_COUNT
                * PAIRED_STRUCTURAL_CEM_V2_FULL_PAIR_CAP
            )
            for arm in PAIRED_STRUCTURAL_CEM_V2_ARMS
        ),
        "state_adapted": (
            str(cem_state.get("current_state") or "") == "ADAPTED"
            and int(cem_state.get("generation") or 0)
            == PAIRED_STRUCTURAL_CEM_V2_CHECKPOINT_COUNT
        ),
    }
    uniform_metrics = arm_metrics[uniform_arm]
    cem_metrics = arm_metrics[cem_arm]
    return {
        "schema_version": (
            "cn_minute_static_structural_cem_v2_canary_verdict_v1"
        ),
        "mechanical_contracts": contracts,
        "exact_duplicate_counts": duplicate_counts,
        "generation_one_stream_hash": _stable_hash(first_uniform),
        "generation_one_full_evaluation_set_hash": _stable_hash(
            first_uniform_full
        ),
        "directional_observation": {
            "uniform_positive_pairs": int(
                uniform_metrics["positive_matched_pairs"]
            ),
            "cem_positive_pairs": int(
                cem_metrics["positive_matched_pairs"]
            ),
            "uniform_median_increment": uniform_metrics[
                "median_signed_matched_increment"
            ],
            "cem_median_increment": cem_metrics[
                "median_signed_matched_increment"
            ],
            "not_a_financial_qualification": True,
        },
        "status": (
            "MECHANICAL_PASS_RUN_MEDIUM"
            if all(contracts.values())
            else "MECHANICAL_FAIL_STOP"
        ),
        "large_search_authorized": False,
    }


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


def _verify_reused_supply(
    root: Path,
    *,
    expected_production_root_contract_hash: str,
) -> dict[str, Any]:
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
        or str(gate.get("production_root_contract_hash") or "")
        != str(expected_production_root_contract_hash)
    ):
        raise RuntimeError("REUSED_OLD_SUPPLY_NOT_QUALIFIED")
    return {
        "status": "PASS_REUSED_HASH_VERIFIED",
        "manifest": _input_artifact(manifest_path),
        "exact_supply": int(gate["post_archive_exact_supply"]),
        "behavior_unique_pairs": int(gate["behavior_unique_pairs"]),
        "production_root_contract_hash": str(
            gate["production_root_contract_hash"]
        ),
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
        "authority_lifecycle": "EXPERIMENTAL_CAMPAIGN_LOCAL",
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
    source_root: Path,
    candidate_ledger: Path,
    observation_ledger: Path,
    split_manifest_hash: str,
    data_release_hash: str,
    registry_hash: str,
    pair_count: int,
    seed: int,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
]:
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
    source_contract = _source_full_comparator_contract(
        source_root,
        split_manifest_hash=split_manifest_hash,
        data_release_hash=data_release_hash,
        registry_hash=registry_hash,
    )
    result_pairs = source_contract.pop("_pair_proofs")
    by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        by_pair[str(row.get("pair_id") or "")].append(row)
    eligible = [
        row for row in observations
        if (
            len(by_pair.get(str(row.get("pair_id") or ""), ())) == 2
            and str(row.get("pair_id") or "") in result_pairs
            and str(row.get("pair_receipt_hash") or "")
            == str(
                result_pairs[str(row.get("pair_id") or "")].get(
                    "pair_receipt_hash"
                )
                or ""
            )
        )
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
    selected_contract_checks = {
        "source_pair_receipts_bound": all(
            str(row.get("pair_receipt_hash") or "")
            == str(result_pairs[str(row["pair_id"])]["pair_receipt_hash"])
            for row in selected_observations
        ),
        "direction_long_top": all(
            str(row.get("open_direction") or "long_top").lower()
            == "long_top"
            for row in selected_candidates
        ),
        "full_cross_section": all(
            str(row.get("outer_mapping") or "") == "cross_sectional"
            and str(row.get("support_unit") or "")
            == "stock-minute cross-section"
            for row in selected_candidates
        ),
        "matched_reward_contract": all(
            str(row.get("pair_mapping_portfolio_contract") or "")
            == (
                "SAME_FULL_SHARD_UNIVERSE|SAME_TRADE_TIMES|"
                "SAME_SPLIT_ROLES|SAME_HORIZONS|"
                "SAME_SUPPORT_COORDINATES|SAME_PORTFOLIO_MODE|"
                "SAME_COST_ASSUMPTIONS"
            )
            for row in selected_candidates
        ),
    }
    if not all(selected_contract_checks.values()):
        raise RuntimeError("SOURCE_FULL_COMPARATOR_CONTRACT_DRIFT")
    source_contract["selected_pair_contract_checks"] = (
        selected_contract_checks
    )
    source_contract["selected_pair_count"] = len(selected_observations)
    return selected_candidates, selected_observations, source_contract


def _source_full_comparator_contract(
    source_root: Path,
    *,
    split_manifest_hash: str,
    data_release_hash: str,
    registry_hash: str,
) -> dict[str, Any]:
    pair_proofs: dict[str, dict[str, Any]] = {}
    pairs = 0
    wall = 0.0
    effective_cores = []
    result_artifacts = []
    for path in source_root.glob(
        "checkpoints/checkpoint_*/phase3cm/"
        "active_bar/CN_STREAMING_BACKEND_RESULT.json"
    ):
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        checkpoint_root = path.parents[2]
        binding_path = checkpoint_root / "phase3cm_input_binding.json"
        manifest_path = checkpoint_root / "batch_manifest.json"
        gate_path = checkpoint_root / "runtime_utilization_gate.json"
        binding = json.loads(
            binding_path.read_text(encoding="utf-8-sig")
        )
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8-sig")
        )
        gate = (
            json.loads(gate_path.read_text(encoding="utf-8-sig"))
            if gate_path.is_file()
            else {}
        )
        artifact_hashes = {
            str(row.get("path") or ""): str(row.get("sha256") or "")
            for row in manifest.get("artifacts") or ()
        }
        expected_result_rel = (
            "phase3cm/active_bar/CN_STREAMING_BACKEND_RESULT.json"
        )
        checks = {
            "immutable_manifest": (
                manifest.get("status") == "BATCH_CLOSED_IMMUTABLE"
            ),
            "result_hash": (
                artifact_hashes.get(expected_result_rel) == _sha256(path)
            ),
            "binding_hash": (
                artifact_hashes.get("phase3cm_input_binding.json")
                == _sha256(binding_path)
                and str(binding.get("binding_hash") or "")
                == str(payload.get("input_binding_hash") or "")
            ),
            "completed_train_full_coordinate": (
                payload.get("status")
                == "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED"
                and payload.get("evaluation_role") == "train"
                and binding.get("evaluation_role") == "train"
                and binding.get("data_role") == "development"
                and binding.get("evaluation_name")
                == "full-coordinate development Phase3CM pair evaluation"
            ),
            "split": (
                str(payload.get("split_manifest_hash") or "")
                == str(split_manifest_hash)
                and str(binding.get("split_manifest_hash") or "")
                == str(split_manifest_hash)
            ),
            "evaluator_inputs": (
                str(binding.get("development_release_hash") or "")
                == str(data_release_hash)
                and str(binding.get("source_closure_sha") or "")
                == str(data_release_hash)
                and str(binding.get("registry_hash") or "")
                == str(registry_hash)
            ),
            "access": (
                int(payload.get("validation_reads") or 0) == 0
                and int(payload.get("holdout_reads") or 0) == 0
                and int(payload.get("forward_2026_reads") or 0) == 0
                and all(
                    int((binding.get("sealed_reads") or {}).get(key) or 0)
                    == 0
                    for key in ("validation", "holdout", "forward_2026")
                )
            ),
            "promotion_forbidden": (
                payload.get("promotion") == "FORBIDDEN"
                and binding.get("promotion") == "FORBIDDEN"
            ),
        }
        if not all(checks.values()):
            raise RuntimeError(
                "SOURCE_FULL_COMPARATOR_ARTIFACT_DRIFT:"
                + checkpoint_root.name
            )
        bound_pairs = {
            str(row.get("pair_id") or ""): dict(row)
            for row in binding.get("pairs") or ()
            if str(row.get("route_id") or "") == ROUTE_ID
            and str(row.get("clock_namespace") or "") == "active_bar"
        }
        for row in payload.get("pair_results") or ():
            pair_id = str(row.get("pair_id") or "")
            if (
                str(row.get("route_id") or "") == ROUTE_ID
                and pair_id in bound_pairs
            ):
                pair_proofs[pair_id] = {
                    "pair_receipt_hash": str(
                        row.get("pair_receipt_hash") or ""
                    ),
                    "primary_candidate_id": str(
                        row.get("primary_candidate_id") or ""
                    ),
                    "control_candidate_id": str(
                        row.get("control_candidate_id") or ""
                    ),
                    "checkpoint": checkpoint_root.name,
                }
        backend_gate = dict((gate.get("backends") or {}).get("active_bar") or {})
        if backend_gate.get("effective_compute_cores") is not None:
            effective_cores.append(
                float(backend_gate["effective_compute_cores"])
            )
        pairs += int(payload.get("pair_count") or 0)
        wall += float(payload.get("wall_seconds") or 0.0)
        result_artifacts.append(
            {
                "checkpoint": checkpoint_root.name,
                "result": _input_artifact(path),
                "binding": _input_artifact(binding_path),
                "checks": checks,
            }
        )
    if (
        not pair_proofs
        or not pairs
        or wall <= 0.0
        or not effective_cores
    ):
        raise RuntimeError("SOURCE_FULL_PHASE3CM_EVIDENCE_MISSING")
    return {
        "status": "HASH_BOUND_COMPARABLE_FULL_EVIDENCE",
        "portfolio_mode": "long_only_top",
        "direction": "candidate_open_direction_default_long_top",
        "cost_bps": 5.0,
        "horizons": [1, 5, 15, 30],
        "reward": "matched_net_increment",
        "result_schema": "cn_phase3cm_streaming_backend_result_v1",
        "full_coordinate_pairs_per_hour": pairs * 3600.0 / wall,
        "effective_cores_median": statistics.median(effective_cores),
        "result_artifacts": result_artifacts,
        "_pair_proofs": pair_proofs,
    }


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
    data_release_hash = _sha256(sidecar_closure)
    candidates, full_observations, source_contract = _source_minute_corpus(
        source_root=source_root,
        candidate_ledger=source_candidate_ledger,
        observation_ledger=source_observation_ledger,
        split_manifest_hash=split.manifest_hash,
        data_release_hash=data_release_hash,
        registry_hash=registry.registry_hash,
        pair_count=SAMPLED_QUALIFICATION_PAIR_COUNT,
        seed=FINANCIAL_SEED + 101,
    )
    full_by_pair = {
        str(row["pair_id"]): dict(row)
        for row in full_observations
    }
    contract_pair_ids = sorted(
        full_by_pair,
        key=lambda pair_id: _stable_hash(
            {"contract_seed": FINANCIAL_SEED + 202, "pair_id": pair_id}
        ),
    )[:4]
    full_contract_candidates = [
        dict(row)
        for row in candidates
        if str(row["pair_id"]) in set(contract_pair_ids)
    ]
    full_contract_root = root / "full_contract_check"
    full_binding_path, full_tables = _context_and_binding(
        batch_root=full_contract_root,
        candidates=full_contract_candidates,
        registry=registry,
        split=split,
        data_release_hash=data_release_hash,
    )
    _bind_purity(full_binding_path, purity_path)
    full_contract_receipts = _run_phase3cm_monitored(
        checkpoint_id="minute_static.sampled_authority.full_contract",
        checkpoint_root=full_contract_root,
        binding_path=full_binding_path,
        table_paths=full_tables,
        split_manifest=split.manifest_path,
        field_roots={"active_bar": field_root},
        label_roots={"active_bar": label_root},
        purity_path=purity_path,
        compute_threads={"active_bar": compute_threads},
        deadline_epoch=deadline_epoch,
        selected_backends=("active_bar",),
        pair_batch_sizes={"active_bar": 4},
    )
    current_full_outcomes, _ = _outcome_rows(full_contract_root)
    current_full_by_pair = {
        str(row["pair_id"]): dict(row)
        for row in current_full_outcomes
    }
    if set(current_full_by_pair) != set(contract_pair_ids):
        raise RuntimeError("FULL_CONTRACT_CHECK_OUTCOME_SET_DRIFT")
    full_contract_result_path = (
        full_contract_root
        / "phase3cm"
        / "active_bar"
        / "CN_STREAMING_BACKEND_RESULT.json"
    )
    full_contract_result = json.loads(
        full_contract_result_path.read_text(encoding="utf-8-sig")
    )
    binding_path, tables = _context_and_binding(
        batch_root=root,
        candidates=candidates,
        registry=registry,
        split=split,
        data_release_hash=data_release_hash,
    )
    _bind_purity(binding_path, purity_path)
    _bind_session_sample(binding_path, session_sample_path)
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
    full_binding = json.loads(
        full_binding_path.read_text(encoding="utf-8-sig")
    )
    sampled_binding = json.loads(
        binding_path.read_text(encoding="utf-8-sig")
    )

    def normalized_binding(payload: Mapping[str, Any]) -> dict[str, Any]:
        contract_ids = set(contract_pair_ids)
        return {
            key: payload.get(key)
            for key in (
                "status",
                "data_role",
                "evaluation_name",
                "evaluation_role",
                "development_release_hash",
                "source_closure_sha",
                "split_manifest_hash",
                "registry_hash",
                "label_purge_enforcement",
                "sealed_reads",
                "strict_stage_a",
                "promotion",
            )
        } | {
            "candidate_members": sorted(
                (
                    dict(row)
                    for row in payload.get("candidate_members") or ()
                    if str(row.get("pair_id") or "") in contract_ids
                ),
                key=lambda row: str(row.get("candidate_id") or ""),
            ),
            "pairs": sorted(
                (
                    dict(row)
                    for row in payload.get("pairs") or ()
                    if str(row.get("pair_id") or "") in contract_ids
                ),
                key=lambda row: str(row.get("pair_id") or ""),
            ),
        }

    binding_parity = (
        normalized_binding(full_binding)
        == normalized_binding(sampled_binding)
    )
    sampled_outcomes, _ = _outcome_rows(root)
    sampled_by_pair = {
        str(row["pair_id"]): dict(row)
        for row in sampled_outcomes
        if str(row.get("pair_evaluation_status") or "")
        == "PAIR_EVALUATED"
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
    full_pairs_per_hour = float(
        source_contract["full_coordinate_pairs_per_hour"]
    )
    speed_multiple = sampled_pairs_per_hour / full_pairs_per_hour

    members_by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        members_by_pair[str(row["pair_id"])].append(row)
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
            "direction": all(
                str(row.get("open_direction") or "long_top").lower()
                == "long_top"
                for row in members
            ),
            "long_only": result.get("portfolio_mode") == "long_only_top",
            "cost": float(result.get("cost_bps") or -1.0) == 5.0,
            "horizons": result.get("horizons") == [1, 5, 15, 30],
            "full_sampled_evaluator_config_parity": (
                binding_parity
                and full_contract_result.get("portfolio_mode")
                == result.get("portfolio_mode")
                == "long_only_top"
                and float(full_contract_result.get("cost_bps") or -1.0)
                == float(result.get("cost_bps") or -1.0)
                == 5.0
                and full_contract_result.get("horizons")
                == result.get("horizons")
                == [1, 5, 15, 30]
            ),
            "current_full_outcome_matches_source": math.isclose(
                float(
                    current_full_by_pair[pair_id][
                        "matched_net_increment"
                    ]
                ),
                float(full_by_pair[pair_id]["matched_net_increment"]),
                rel_tol=1e-12,
                abs_tol=1e-12,
            ),
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
        "status": (
            "CAMPAIGN_LOCAL_EVIDENCE_QUALIFIED"
            if qualified
            else "CAMPAIGN_LOCAL_EVIDENCE_NOT_QUALIFIED"
        ),
        "evidence_status": "QUALIFIED" if qualified else "NOT_QUALIFIED",
        "formal_authority_status": "NOT_PROMOTED",
        "campaign_local_selector_authorized": bool(qualified),
        "authority_lifecycle": "EXPERIMENTAL_CAMPAIGN_LOCAL",
        "comparable_pair_count": len(comparable_ids),
        "spearman_rank_correlation": spearman,
        "sign_agreement": sign_agreement,
        "full_top_quartile_recall_in_sampled_top_half": top_recall,
        "sampled_pairs_per_hour": sampled_pairs_per_hour,
        "full_pairs_per_hour": full_pairs_per_hour,
        "speed_multiple": speed_multiple,
        "source_full_comparator_contract": source_contract,
        "checks": checks,
        "contract_pairs": contract_rows,
        "session_sample_manifest": _input_artifact(
            session_sample_path
        ),
        "result": _input_artifact(result_path),
        "full_contract_result": _input_artifact(
            full_contract_result_path
        ),
        "full_contract_binding": _input_artifact(full_binding_path),
        "sampled_binding": _input_artifact(binding_path),
        "access_receipts": [
            *full_contract_receipts,
            *receipts,
        ],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "PENDING_FORMAL_PROMOTION_AFTER_CAMPAIGN_CLOSURE"
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
    paired_structural_v2 = bool(
        getattr(args, "paired_structural_cem_v2_canary", False)
    )
    if (
        (paired_structural_v2 or not args.allow_noncanonical_host)
        and platform.node().upper() != AUTHORIZED_HOST
    ):
        raise RuntimeError(
            f"official V3 evidence must run on {AUTHORIZED_HOST}"
        )
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    campaign_arms = (
        PAIRED_STRUCTURAL_CEM_V2_ARMS
        if paired_structural_v2
        else ARMS
    )
    checkpoint_count = (
        PAIRED_STRUCTURAL_CEM_V2_CHECKPOINT_COUNT
        if paired_structural_v2
        else CHECKPOINT_COUNT
    )
    full_pair_cap = (
        PAIRED_STRUCTURAL_CEM_V2_FULL_PAIR_CAP
        if paired_structural_v2
        else FULL_PAIR_CAP
    )
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

    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    contract, route_roots = _load_production_contract(
        args.production_root_contract.resolve(),
        registry=registry,
    )
    supply_reuse = _verify_reused_supply(
        args.reused_supply_root.resolve(),
        expected_production_root_contract_hash=contract["contract_hash"],
    )
    supply_reuse_path = _write_json(
        output_root / "old_supply_reuse_receipt.json",
        supply_reuse,
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
        (
            projection.structural_decision_catalog(
                EXPANDED_FORMULA_SPACE_ID
            )
            if paired_structural_v2
            else projection.decision_catalog(
                EXPANDED_FORMULA_SPACE_ID
            )
        ),
    )
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    reused_sampled_receipt_path = None
    reused_sampled_qualification = None
    if paired_structural_v2:
        (
            reused_sampled_qualification,
            session_sample,
            reused_sampled_receipt,
        ) = _verify_reused_sampled_authority(
            args.reused_sampled_authority_root
        )
        reused_sampled_receipt_path = _write_json(
            output_root / "sampled_authority_reuse_receipt.json",
            reused_sampled_receipt,
        )
    else:
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
        "authorization_id": (
            "MINUTE_STATIC_STRUCTURAL_CEM_V2_PAIRED_CANARY"
            if paired_structural_v2
            else AUTHORIZATION_ID
        ),
        "route_id": ROUTE_ID,
        "old_production": "field_spread",
        "added_existing_production": "normalized_ratio",
        "arms": list(campaign_arms),
        "checkpoint_count": checkpoint_count,
        "full_coordinate_pair_cap_per_checkpoint": full_pair_cap,
        "behavior_probe_cap": 128,
        "behavior_probe_target": FINANCIAL_BEHAVIOR_PROBE_TARGET,
        "sampled_selection_cap": 48,
        "minimum_evaluated_pairs_per_arm": (
            checkpoint_count * full_pair_cap
            if paired_structural_v2
            else MINIMUM_EVALUATED_PAIRS
        ),
        "minimum_active_checkpoints_per_arm": checkpoint_count,
        "production_root_contract_hash": contract["contract_hash"],
        "session_sample_contract_hash": session_sample[
            "contract_hash"
        ],
        "decision_surface": (
            [
                "production_id_adaptive",
                "field_pair_id_uniform_remaining_exact",
            ]
            if paired_structural_v2
            else ["production_id", "field_pair_id"]
        ),
        "left_right_independent_probabilities": "FORBIDDEN",
        "paired_common_random_stream": paired_structural_v2,
        "sampled_feedback_writes": "FORBIDDEN",
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
    fresh_state_parity = (
        _structural_v2_fresh_stream_parity(
            projection, seed=FINANCIAL_SEED + 900_001
        )
        if paired_structural_v2
        else _fresh_large_search_state_parity(
            projection, seed=FINANCIAL_SEED + 900_001
        )
    )
    fresh_state_parity_path = _write_json(
        output_root / "fresh_large_search_state_parity.json",
        fresh_state_parity,
    )
    if fresh_state_parity["status"] != "PASS":
        raise RuntimeError("FRESH_LARGE_SEARCH_STATE_PARITY_FAILED")

    deadline_epoch = time.time() + int(args.maximum_wall_seconds)
    if paired_structural_v2:
        sampled_qualification = dict(reused_sampled_qualification or {})
    else:
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
            field_root=args.sampled_authority_field_root.resolve(),
            label_root=args.sampled_authority_label_root.resolve(),
            purity_path=args.split_boundary_purity.resolve(),
            sidecar_closure=(
                args.sampled_authority_sidecar_closure.resolve()
            ),
            compute_threads=int(args.compute_threads),
            deadline_epoch=deadline_epoch,
        )
    sampled_qualification_path = _write_json(
        output_root / "sampled_authority_qualification.json",
        sampled_qualification,
    )
    sampled_selector_evidence_qualified = bool(
        sampled_qualification["campaign_local_selector_authorized"]
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
    for arm_index, arm in enumerate(campaign_arms):
        exact, archive, closed, rng_state, optimizer_state = (
            _load_arm_state(
                output_root=output_root,
                arm=arm,
                initial_exact=initial_exact,
                initial_behavior=initial_behavior,
                checkpoint_count=checkpoint_count,
            )
        )
        rng = np.random.default_rng(
            (
                FINANCIAL_SEED + 20_000
                if paired_structural_v2
                else FINANCIAL_SEED + 10_000 * (arm_index + 1)
            )
        )
        if rng_state is not None:
            rng.bit_generator.state = copy.deepcopy(rng_state)
        if paired_structural_v2 and arm == "arm_c_structural_cem_v2":
            decisions = projection.structural_decision_specs(
                EXPANDED_FORMULA_SPACE_ID
            )
            policy = (
                RankWeightedCategoricalCEMPolicy.restore(
                    optimizer_state,
                    decisions=decisions,
                    decision_catalog_hash=(
                        projection.structural_decision_catalog_hash(
                            EXPANDED_FORMULA_SPACE_ID
                        )
                    ),
                    formula_space_id=EXPANDED_FORMULA_SPACE_ID,
                    rng=rng,
                )
                if optimizer_state is not None
                else RankWeightedCategoricalCEMPolicy.fresh(
                    decisions=decisions,
                    decision_catalog_hash=(
                        projection.structural_decision_catalog_hash(
                            EXPANDED_FORMULA_SPACE_ID
                        )
                    ),
                    formula_space_id=EXPANDED_FORMULA_SPACE_ID,
                )
            )
        elif paired_structural_v2:
            policy = AvailableUniformPolicy()
        elif arm == "arm_c_cem_expanded":
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

    for checkpoint_index in range(checkpoint_count):
        order = (
            list(campaign_arms)
            if checkpoint_index % 2 == 0
            else list(reversed(campaign_arms))
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
                sampled_selector_evidence_qualified=(
                    sampled_selector_evidence_qualified
                ),
                behavior_probe_target=(
                    FINANCIAL_BEHAVIOR_PROBE_TARGET
                ),
                availability_masked_sampling=paired_structural_v2,
                generator_policy_id=str(
                    getattr(
                        state["policy"],
                        "policy_id",
                        type(state["policy"]).__name__,
                    )
                ),
                optimizer_updates_enabled=(
                    paired_structural_v2
                    and arm == "arm_c_structural_cem_v2"
                )
                if paired_structural_v2
                else None,
                sampled_selection_cap=(
                    FINANCIAL_BEHAVIOR_PROBE_TARGET
                    if paired_structural_v2
                    else 48
                ),
                full_pair_cap=full_pair_cap,
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
            checkpoint_count=checkpoint_count,
        )
        for arm in campaign_arms
    }
    if paired_structural_v2:
        comparison = _paired_structural_canary_verdict(
            output_root, arm_metrics
        )
        final = {
            "SAMPLED_PHASE3CM_AUTHORITY": (
                "REUSED_ACTIVE_ROUTE_LOCAL_SELECTION_AUTHORITY"
            ),
            "PAIRED_STREAM_CONTRACT": (
                "PASS"
                if all(
                    comparison["mechanical_contracts"][name]
                    for name in (
                        "generation_one_exact_stream_parity",
                        "generation_one_full_evaluation_set_parity",
                    )
                )
                else "FAIL"
            ),
            "EXACT_AVAILABILITY_MASK": (
                "PASS"
                if comparison["mechanical_contracts"][
                    "availability_mask_zero_exact_duplicates"
                ]
                else "FAIL"
            ),
            "STRUCTURAL_CEM_V2_CANARY": comparison["status"],
            "CEM_SEARCH_INCREMENT": (
                "NOT_YET_FINANCIALLY_QUALIFIED_MECHANICAL_CANARY_ONLY"
            ),
            "TARGET_FAMILY_LARGE_SEARCH_READINESS": (
                "SEARCH_POLICY_MEDIUM_QUALIFICATION_REQUIRED"
                if comparison["status"]
                == "MECHANICAL_PASS_RUN_MEDIUM"
                else "SEARCH_POLICY_IMPLEMENTATION_BLOCKED"
            ),
            "READINESS_BLOCKERS": [
                "MEDIUM_PAIRED_SEARCH_POLICY_QUALIFICATION_REQUIRED"
            ]
            if comparison["status"] == "MECHANICAL_PASS_RUN_MEDIUM"
            else ["STRUCTURAL_CEM_V2_MECHANICAL_CONTRACT_FAILED"],
        }
    else:
        comparison = _comparison_verdict(
            arm_a=arm_metrics["arm_a_uniform_old"],
            arm_b=arm_metrics["arm_b_uniform_expanded"],
            arm_c=arm_metrics["arm_c_cem_expanded"],
            static_status="PASS",
            behavior_status="PASS",
            sampled_full_contract=(
                "PASS" if sampled_selector_evidence_qualified else "FAIL"
            ),
            performance_baseline=sampled_qualification[
                "source_full_comparator_contract"
            ],
            selected_backends=("active_bar",),
        )
        blockers = []
        if not sampled_selector_evidence_qualified:
            blockers.append("SAMPLED_PHASE3CM_EVIDENCE_NOT_QUALIFIED")
        else:
            blockers.append(
                "SAMPLED_PHASE3CM_AUTHORITY_PENDING_FORMAL_PROMOTION"
            )
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
                "PENDING_FORMAL_PROMOTION"
                if sampled_selector_evidence_qualified
                else "NOT_QUALIFIED"
            ),
            "SAMPLED_PHASE3CM_EVIDENCE": sampled_qualification[
                "evidence_status"
            ],
            "FORMULA_SPACE_INCREMENT": comparison[
                "FORMULA_SPACE_INCREMENT"
            ],
            "CEM_SEARCH_INCREMENT": comparison[
                "CEM_SEARCH_INCREMENT"
            ],
            "PERFORMANCE_CONTRACT": comparison[
                "PERFORMANCE_CONTRACT"
            ],
            "CAMPAIGN_LOCAL_LARGE_SEARCH_READINESS": comparison[
                "TARGET_FAMILY_LARGE_SEARCH_READINESS"
            ],
            "TARGET_FAMILY_LARGE_SEARCH_READINESS": (
                "SEMANTICS_BLOCKED_PENDING_SAMPLED_AUTHORITY_PROMOTION"
                if (
                    sampled_selector_evidence_qualified
                    and comparison[
                        "TARGET_FAMILY_LARGE_SEARCH_READINESS"
                    ]
                    == "READY"
                )
                else comparison["TARGET_FAMILY_LARGE_SEARCH_READINESS"]
            ),
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
        *(
            [reused_sampled_receipt_path]
            if reused_sampled_receipt_path is not None
            else []
        ),
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
            for arm in campaign_arms
            for index in range(checkpoint_count)
        ),
    ]
    manifest = {
        "schema_version": (
            "cn_minute_static_structural_cem_v2_canary_manifest_v1"
            if paired_structural_v2
            else "cn_minute_static_production_cem_v3_manifest_v2"
        ),
        "status": "CAMPAIGN_CLOSED",
        "authorization_id": frozen_contract["authorization_id"],
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
    parser.add_argument(
        "--paired-structural-cem-v2-canary",
        action="store_true",
    )
    parser.add_argument("--reused-sampled-authority-root", type=Path)
    parser.add_argument("--reused-supply-root", type=Path)
    parser.add_argument("--source-campaign-root", type=Path)
    parser.add_argument("--source-observation-ledger", type=Path)
    parser.add_argument("--active-field-root", type=Path)
    parser.add_argument("--active-label-root", type=Path)
    parser.add_argument("--sampled-authority-field-root", type=Path)
    parser.add_argument("--sampled-authority-label-root", type=Path)
    parser.add_argument(
        "--sampled-authority-sidecar-closure", type=Path
    )
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
            "sampled_authority_field_root",
            "sampled_authority_label_root",
            "sampled_authority_sidecar_closure",
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
        if (
            args.paired_structural_cem_v2_canary
            and args.reused_sampled_authority_root is None
        ):
            parser.error(
                "--paired-structural-cem-v2-canary requires "
                "--reused-sampled-authority-root"
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
