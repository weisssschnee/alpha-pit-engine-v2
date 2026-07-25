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
from our_system_phase2.services.compositional_grammar import (
    MINUTE_STATIC_ONLINE_BINARY_OPERATOR_IDS,
    MINUTE_STATIC_ONLINE_OPERATOR_SKELETON_NAMES,
    MINUTE_STATIC_ONLINE_TRANSFORM_PAIRS,
    MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID,
    MINUTE_STATIC_TYPED_TRANSFORM_IDS,
    MINUTE_STATIC_TYPED_TRANSFORM_PAIRS,
    MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID,
)
from our_system_phase2.services.phase3cm_streaming_expression import (
    unsupported_streaming_operators,
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
ABSOLUTE_STATE_INTERACTION_SKELETON_ID = (
    "cn.comp.v2.minute_static.absolute_state_interaction"
)
DISPERSION_INTERACTION_SKELETON_ID = (
    "cn.comp.v2.minute_static.dispersion_interaction"
)
STRUCTURAL_SUPPLY_FORMULA_SPACE_ID = (
    "MINUTE_STATIC_STRUCTURAL_SUPPLY_V1"
)
STRUCTURAL_TYPED_FORMULA_SPACE_ID = (
    "MINUTE_STATIC_STRUCTURAL_TYPED_V2"
)
STRUCTURAL_FORMULA_V4_SPACE_ID = (
    "MINUTE_STATIC_STRUCTURAL_FORMULA_V4"
)
STRUCTURAL_ONLINE_TYPED_GRAMMAR_SPACE_ID = (
    "MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_V5"
)
ONLINE_TYPED_GRAMMAR_GENERATION_ATTEMPT_CAP = 256
STRUCTURAL_SUPPLY_AUTHORIZATION_ID = (
    "MINUTE_STATIC_STRUCTURAL_FORMULA_SPACE_SUPPLY_V1"
)
STRUCTURAL_TYPED_AUTHORIZATION_ID = (
    "MINUTE_STATIC_STRUCTURAL_TYPED_SURFACE_AUDIT_V2"
)
STRUCTURAL_FORMULA_V4_AUTHORIZATION_ID = (
    "MINUTE_STATIC_STRUCTURAL_FORMULA_SUPPLY_V4"
)
STRUCTURAL_SUPPLY_PRODUCTION_IDS = (
    "field_spread",
    "normalized_ratio",
    "absolute_state_interaction",
    "dispersion_interaction",
)
STRUCTURAL_SUPPLY_REFERENCE_PAIR_BUDGET = 72
STRUCTURAL_SUPPLY_NONEXHAUSTIVE_HEADROOM_MULTIPLIER = 2
STRUCTURAL_FORMULA_V4_MINIMUM_FRESH_EXACT = 144
STRUCTURAL_FORMULA_V4_MINIMUM_BEHAVIOR_UNIQUE = 72
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
PAIRED_STRUCTURAL_CEM_V2_MEDIUM_CHECKPOINT_COUNT = 3
PAIRED_STRUCTURAL_CEM_V2_MEDIUM_FULL_PAIR_CAP = 24
PAIRED_STRUCTURAL_SUPPLY_MEDIUM_CHECKPOINT_COUNT = 4
PAIRED_STRUCTURAL_SUPPLY_MEDIUM_FULL_PAIR_CAP = 17
PAIRED_ONLINE_TYPED_GRAMMAR_CHECKPOINT_COUNT = 3
PAIRED_ONLINE_TYPED_GRAMMAR_FULL_PAIR_CAP = 24
PAIRED_STRUCTURAL_CEM_V2_CANARY_SEED_OFFSET = 20_000
PAIRED_STRUCTURAL_CEM_V2_MEDIUM_SEED_OFFSET = 30_000
PAIRED_STRUCTURAL_SUPPLY_MEDIUM_SEED_OFFSET = 40_000
PAIRED_ONLINE_TYPED_GRAMMAR_SEED_OFFSET = 50_000


class MinuteStaticProductionProjection:
    """Read-only optimizer projection over qualified existing Grammar lanes."""

    _production_skeletons = {
        "field_spread": OLD_SKELETON_ID,
        "normalized_ratio": NORMALIZED_RATIO_SKELETON_ID,
        "absolute_state_interaction": (
            ABSOLUTE_STATE_INTERACTION_SKELETON_ID
        ),
        "dispersion_interaction": DISPERSION_INTERACTION_SKELETON_ID,
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
        v4_lanes = {
            production_id: generator.categorical_gene_space(
                ROUTE_ID,
                skeleton_id=skeleton_id,
                formula_extension_id=(
                    MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID
                ),
            )
            for production_id, skeleton_id
            in self._production_skeletons.items()
        }
        v4_pair_domains = {
            production_id: tuple(
                lane["ordered_categories_by_slot"]["field_pair_id"]
            )
            for production_id, lane in v4_lanes.items()
        }
        if any(
            domain != self.field_pair_ids
            for domain in v4_pair_domains.values()
        ):
            raise RuntimeError(
                "MINUTE_FORMULA_V4_FIELD_PAIR_DOMAIN_DRIFT"
            )
        self.v4_gene_surface_id = str(
            v4_lanes["field_spread"][
                "ordered_categories_by_slot"
            ]["gene_surface_id"][0]
        )
        self.v4_transform_pairs = {
            production_id: tuple(
                (
                    str(row["left_transform_id"]),
                    str(row["right_transform_id"]),
                )
                for row in lane["allowed_transform_pairs"]
            )
            for production_id, lane in v4_lanes.items()
        }
        self.online_operator_skeletons = {
            operator_id: (
                "cn.comp.v2.minute_static." + skeleton_name
            )
            for operator_id, skeleton_name
            in MINUTE_STATIC_ONLINE_OPERATOR_SKELETON_NAMES.items()
        }
        self._online_gene_surface_id: str | None = None
        self._available_candidate_cache: dict[
            str, tuple[dict[str, Any], ...]
        ] = {}

    @property
    def online_gene_surface_id(self) -> str:
        """Resolve the V5 surface only when the online path is requested."""

        if self._online_gene_surface_id is not None:
            return self._online_gene_surface_id
        online_lanes = {
            operator_id: self.generator.categorical_gene_space(
                ROUTE_ID,
                skeleton_id=skeleton_id,
                formula_extension_id=(
                    MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID
                ),
            )
            for operator_id, skeleton_id
            in self.online_operator_skeletons.items()
        }
        if tuple(online_lanes) != MINUTE_STATIC_ONLINE_BINARY_OPERATOR_IDS:
            raise RuntimeError(
                "MINUTE_ONLINE_GRAMMAR_OPERATOR_AUTHORITY_DRIFT"
            )
        if any(
            tuple(
                lane["ordered_categories_by_slot"]["field_pair_id"]
            )
            != self.field_pair_ids
            for lane in online_lanes.values()
        ):
            raise RuntimeError(
                "MINUTE_ONLINE_GRAMMAR_FIELD_PAIR_DOMAIN_DRIFT"
            )
        online_gene_surface_id = str(
            online_lanes["SUB"]["ordered_categories_by_slot"][
                "gene_surface_id"
            ][0]
        )
        if any(
            str(
                lane["ordered_categories_by_slot"]["gene_surface_id"][
                    0
                ]
            )
            != online_gene_surface_id
            for lane in online_lanes.values()
        ):
            raise RuntimeError(
                "MINUTE_ONLINE_GRAMMAR_GENE_SURFACE_DRIFT"
            )
        self._online_gene_surface_id = online_gene_surface_id
        return online_gene_surface_id

    def decision_specs(
        self,
        formula_space_id: str,
    ) -> tuple[DecisionSpec, ...]:
        if formula_space_id == OLD_FORMULA_SPACE_ID:
            productions = ("field_spread",)
        elif formula_space_id == EXPANDED_FORMULA_SPACE_ID:
            productions = ("field_spread", "normalized_ratio")
        elif formula_space_id in {
            STRUCTURAL_SUPPLY_FORMULA_SPACE_ID,
            STRUCTURAL_TYPED_FORMULA_SPACE_ID,
            STRUCTURAL_FORMULA_V4_SPACE_ID,
        }:
            productions = STRUCTURAL_SUPPLY_PRODUCTION_IDS
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

    def structural_typed_decision_specs(
        self,
        formula_space_id: str,
    ) -> tuple[DecisionSpec, ...]:
        """Project the authoritative joint field-pair domain losslessly."""

        if formula_space_id not in {
            STRUCTURAL_TYPED_FORMULA_SPACE_ID,
            STRUCTURAL_FORMULA_V4_SPACE_ID,
        }:
            raise ValueError(
                "typed structural decisions require a typed formula space"
            )
        production, _ = self.decision_specs(formula_space_id)
        base = f"route={ROUTE_ID}|formula_space={formula_space_id}"
        ordered_fields = tuple(
            dict.fromkeys(
                field_id
                for pair_id in self.field_pair_ids
                for field_id in pair_id.split("::", 1)
            )
        )

        def field_decision(
            *,
            side: str,
        ) -> DecisionSpec:
            return DecisionSpec(
                decision_id=f"minute_static.{side}_field_id",
                context_id=base + f"|decision={side}_field_id",
                decision_type=f"{side.upper()}_FIELD",
                gene_slot=f"{side}_field_id",
                ordered_choices=tuple(
                    SearchChoice(
                        token_id="field:" + field_id,
                        gene_value=field_id,
                        semantic_value={
                            "field_id": field_id,
                            "side": side,
                            "projection_authority": (
                                "CompositionalGrammarV2."
                                "field_pair_id"
                            ),
                        },
                    )
                    for field_id in ordered_fields
                ),
            )

        fields = (
            field_decision(side="left"),
            field_decision(side="right"),
        )
        if formula_space_id != STRUCTURAL_FORMULA_V4_SPACE_ID:
            return production, *fields

        def transform_decision(*, side: str) -> DecisionSpec:
            return DecisionSpec(
                decision_id=(
                    f"minute_static.{side}_transform_id"
                ),
                context_id=(
                    base + f"|decision={side}_transform_id"
                ),
                decision_type=f"{side.upper()}_TRANSFORM",
                gene_slot=f"{side}_transform_id",
                ordered_choices=tuple(
                    SearchChoice(
                        token_id="transform:" + transform_id,
                        gene_value=transform_id,
                        semantic_value={
                            "transform_id": transform_id,
                            "side": side,
                            "operator_authority": (
                                "TypedRouteCompiler."
                                "ROUTE_PRIMITIVE_ALLOWLIST"
                            ),
                            "streaming_authority": (
                                "phase3cm_streaming_expression."
                                "STREAMING_OPERATOR_SURFACE"
                            ),
                        },
                    )
                    for transform_id
                    in MINUTE_STATIC_TYPED_TRANSFORM_IDS
                ),
            )

        return (
            production,
            transform_decision(side="left"),
            transform_decision(side="right"),
            *fields,
        )

    def structural_typed_decision_catalog(
        self,
        formula_space_id: str,
    ) -> dict[str, Any]:
        rows = self.structural_typed_decision_specs(formula_space_id)
        payload = {
            "schema_version": (
                "cn_minute_static_structural_typed_decision_catalog_v1"
            ),
            "route_id": ROUTE_ID,
            "formula_space_id": formula_space_id,
            "fixed_gene_surface_id": (
                self.v4_gene_surface_id
                if formula_space_id
                == STRUCTURAL_FORMULA_V4_SPACE_ID
                else self.gene_surface_id
            ),
            "adaptive_decisions": [row.to_dict() for row in rows],
            "authoritative_joint_source": (
                (
                    "CompositionalGrammarV2."
                    "MINUTE_STATIC_TYPED_TRANSFORM_PAIRS"
                    "+field_pair_id"
                )
                if formula_space_id
                == STRUCTURAL_FORMULA_V4_SPACE_ID
                else "CompositionalGrammarV2.field_pair_id"
            ),
            "projection_contract": (
                (
                    "LOSSLESS_PRODUCTION_TRANSFORM_"
                    "AND_LEFT_RIGHT_COORDINATES"
                )
                if formula_space_id
                == STRUCTURAL_FORMULA_V4_SPACE_ID
                else "LOSSLESS_PRODUCTION_LEFT_RIGHT_COORDINATES"
            ),
            "joint_exact_availability_mask": (
                (
                    "APPLIED_BEFORE_EACH_OF_FIVE_"
                    "HIERARCHICAL_DRAWS"
                )
                if formula_space_id
                == STRUCTURAL_FORMULA_V4_SPACE_ID
                else "APPLIED_BEFORE_EACH_HIERARCHICAL_DRAW"
            ),
            "field_family_authority": (
                "NOT_DECLARED_NO_INVENTED_GROUPS"
            ),
            "new_fields_operators_formulas": (
                {
                    "new_fields": 0,
                    "new_operators": 0,
                    "bounded_formula_variants": True,
                }
                if formula_space_id
                == STRUCTURAL_FORMULA_V4_SPACE_ID
                else 0
            ),
            "transform_pair_authority": (
                "CompositionalGrammarV2."
                "MINUTE_STATIC_TYPED_TRANSFORM_PAIRS"
                if formula_space_id
                == STRUCTURAL_FORMULA_V4_SPACE_ID
                else "NOT_APPLICABLE"
            ),
            "maximum_depth": 4,
        }
        payload["catalog_hash"] = _stable_hash(payload)
        return payload

    def structural_typed_decision_catalog_hash(
        self,
        formula_space_id: str,
    ) -> str:
        return str(
            self.structural_typed_decision_catalog(formula_space_id)[
                "catalog_hash"
            ]
        )

    def online_grammar_decision_specs(
        self,
        formula_space_id: str,
    ) -> tuple[DecisionSpec, ...]:
        """Expose grammar rules, not preconstructed formulas, to the policy."""

        if formula_space_id != STRUCTURAL_ONLINE_TYPED_GRAMMAR_SPACE_ID:
            raise ValueError(
                "online Grammar decisions require the V5 formula space"
            )
        base = f"route={ROUTE_ID}|formula_space={formula_space_id}"
        binary_operator = DecisionSpec(
            decision_id="minute_static.binary_operator_id",
            context_id=base + "|decision=binary_operator_id",
            decision_type="BINARY_OPERATOR_RULE",
            gene_slot="binary_operator_id",
            ordered_choices=tuple(
                SearchChoice(
                    token_id=operator_id,
                    gene_value=operator_id,
                    semantic_value={
                        "binary_operator_id": operator_id,
                        "skeleton_id": self.online_operator_skeletons[
                            operator_id
                        ],
                        "rule_authority": (
                            "CompositionalGrammarV2."
                            "MINUTE_STATIC_ONLINE_BINARY_OPERATOR_IDS"
                        ),
                    },
                )
                for operator_id
                in MINUTE_STATIC_ONLINE_BINARY_OPERATOR_IDS
            ),
        )
        ordered_fields = tuple(
            dict.fromkeys(
                field_id
                for pair_id in self.field_pair_ids
                for field_id in pair_id.split("::", 1)
            )
        )
        child_decisions: list[DecisionSpec] = []
        for operator_id in MINUTE_STATIC_ONLINE_BINARY_OPERATOR_IDS:
            context = base + f"|root_rule={operator_id}"
            for side in ("left", "right"):
                transform_index = 0 if side == "left" else 1
                allowed_transforms = tuple(
                    dict.fromkeys(
                        pair[transform_index]
                        for pair
                        in MINUTE_STATIC_ONLINE_TRANSFORM_PAIRS[
                            operator_id
                        ]
                    )
                )
                child_decisions.append(
                    DecisionSpec(
                        decision_id=(
                            f"minute_static.{operator_id}."
                            f"{side}_transform_id"
                        ),
                        context_id=(
                            context + f"|decision={side}_transform_id"
                        ),
                        decision_type=(
                            f"{side.upper()}_TYPED_CHILD_TRANSFORM"
                        ),
                        gene_slot=f"{side}_transform_id",
                        ordered_choices=tuple(
                            SearchChoice(
                                token_id="transform:" + transform_id,
                                gene_value=transform_id,
                                semantic_value={
                                    "root_rule": operator_id,
                                    "side": side,
                                    "transform_id": transform_id,
                                    "operator_authority": (
                                        "TypedRouteCompiler."
                                        "ROUTE_PRIMITIVE_ALLOWLIST"
                                    ),
                                },
                            )
                            for transform_id
                            in allowed_transforms
                        ),
                    )
                )
            for side in ("left", "right"):
                child_decisions.append(
                    DecisionSpec(
                        decision_id=(
                            f"minute_static.{operator_id}."
                            f"{side}_field_id"
                        ),
                        context_id=(
                            context + f"|decision={side}_field_id"
                        ),
                        decision_type=f"{side.upper()}_TYPED_FIELD",
                        gene_slot=f"{side}_field_id",
                        ordered_choices=tuple(
                            SearchChoice(
                                token_id="field:" + field_id,
                                gene_value=field_id,
                                semantic_value={
                                    "root_rule": operator_id,
                                    "side": side,
                                    "field_id": field_id,
                                    "field_authority": (
                                        "unified_capability_registry"
                                    ),
                                },
                            )
                            for field_id in ordered_fields
                        ),
                    )
                )
        return binary_operator, *child_decisions

    def online_grammar_decision_catalog(
        self,
        formula_space_id: str,
    ) -> dict[str, Any]:
        decisions = self.online_grammar_decision_specs(formula_space_id)
        directional_pair_count = len(self.field_pair_ids)
        root_rule_cardinality = {
            operator_id: (
                len(MINUTE_STATIC_ONLINE_TRANSFORM_PAIRS[operator_id])
                * directional_pair_count
            )
            for operator_id
            in MINUTE_STATIC_ONLINE_BINARY_OPERATOR_IDS
        }
        payload = {
            "schema_version": (
                "cn_minute_static_online_typed_grammar_catalog_v1"
            ),
            "route_id": ROUTE_ID,
            "formula_space_id": formula_space_id,
            "gene_surface_id": self.online_gene_surface_id,
            "formula_materialization": (
                "LAZY_ONLINE_FROM_SELECTED_GRAMMAR_RULES"
            ),
            "eager_formula_catalog": "FORBIDDEN_IN_ASK_PATH",
            "maximum_depth": 4,
            "root_mapping": "CSRank",
            "root_mapping_status": (
                "FROZEN_NOT_AN_ADAPTIVE_DECISION_IN_V5"
            ),
            "root_mapping_rationale": (
                "BOUNDED_FIRST_SLICE_REUSES_EXISTING_MATCHED_CONTROL_"
                "SAFE_MINUTE_STATIC_AUTHORITY"
            ),
            "binary_operator_rules": list(
                MINUTE_STATIC_ONLINE_BINARY_OPERATOR_IDS
            ),
            "typed_child_transforms": list(
                MINUTE_STATIC_TYPED_TRANSFORM_IDS
            ),
            "field_pair_authority": (
                "CompositionalGrammarV2.field_pair_id"
            ),
            "matched_control_rule": (
                "CSRank(Add(left_leg,Mul(0,right_raw_field)))"
            ),
            "commutative_semantic_dedupe": (
                "MUL_ORDERED_TYPED_LEG_KEYS"
            ),
            "root_rule_cardinality": root_rule_cardinality,
            "bounded_formula_cardinality": sum(
                root_rule_cardinality.values()
            ),
            "decisions": [row.to_dict() for row in decisions],
        }
        payload["catalog_hash"] = _stable_hash(payload)
        return payload

    def online_grammar_decision_catalog_hash(
        self,
        formula_space_id: str,
    ) -> str:
        return str(
            self.online_grammar_decision_catalog(formula_space_id)[
                "catalog_hash"
            ]
        )

    def adaptive_decision_specs(
        self,
        formula_space_id: str,
    ) -> tuple[DecisionSpec, ...]:
        if formula_space_id == STRUCTURAL_ONLINE_TYPED_GRAMMAR_SPACE_ID:
            return self.online_grammar_decision_specs(formula_space_id)
        return self.structural_decision_specs(formula_space_id)

    def adaptive_decision_catalog(
        self,
        formula_space_id: str,
    ) -> dict[str, Any]:
        if formula_space_id == STRUCTURAL_ONLINE_TYPED_GRAMMAR_SPACE_ID:
            return self.online_grammar_decision_catalog(formula_space_id)
        return self.structural_decision_catalog(formula_space_id)

    def adaptive_decision_catalog_hash(
        self,
        formula_space_id: str,
    ) -> str:
        return str(
            self.adaptive_decision_catalog(formula_space_id)[
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
            transform_pairs = (
                self.v4_transform_pairs[production_id]
                if formula_space_id
                == STRUCTURAL_FORMULA_V4_SPACE_ID
                else ((None, None),)
            )
            for left_transform, right_transform in transform_pairs:
                for pair_id in self.field_pair_ids:
                    genes = {
                        "skeleton_id": skeleton_id,
                        "gene_surface_id": (
                            self.v4_gene_surface_id
                            if formula_space_id
                            == STRUCTURAL_FORMULA_V4_SPACE_ID
                            else self.gene_surface_id
                        ),
                        "field_pair_id": pair_id,
                    }
                    if left_transform is not None:
                        genes.update(
                            {
                                "left_transform_id": left_transform,
                                "right_transform_id": right_transform,
                            }
                        )
                    pair = self.generator.propose_categorical_genes(
                        ROUTE_ID,
                        genes=genes,
                        formula_extension_id=(
                            MINUTE_STATIC_TYPED_TRANSFORMS_EXTENSION_ID
                            if formula_space_id
                            == STRUCTURAL_FORMULA_V4_SPACE_ID
                            else "PRODUCTION"
                        ),
                    )
                    rows.append(
                        {
                            "production_id": production_id,
                            "skeleton_id": skeleton_id,
                            "field_pair_id": pair_id,
                            "left_transform_id": left_transform,
                            "right_transform_id": right_transform,
                            "candidate": dict(pair.candidate),
                            "control": dict(pair.control),
                        }
                    )
        result = tuple(
            sorted(
                rows,
                key=lambda row: (
                    row["production_id"],
                    str(row.get("left_transform_id") or ""),
                    str(row.get("right_transform_id") or ""),
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

        if (
            formula_space_id
            == STRUCTURAL_ONLINE_TYPED_GRAMMAR_SPACE_ID
        ):
            return self._generate_available_online_grammar(
                formula_space_id=formula_space_id,
                policy=policy,
                rng=rng,
                exact_seen=exact_seen,
            )
        if formula_space_id == STRUCTURAL_FORMULA_V4_SPACE_ID:
            return self._generate_available_v4(
                formula_space_id=formula_space_id,
                policy=policy,
                rng=rng,
                exact_seen=exact_seen,
            )
        if formula_space_id == STRUCTURAL_TYPED_FORMULA_SPACE_ID:
            return self._generate_available_typed(
                formula_space_id=formula_space_id,
                policy=policy,
                rng=rng,
                exact_seen=exact_seen,
            )
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

    def _generate_available_online_grammar(
        self,
        *,
        formula_space_id: str,
        policy: Any,
        rng: np.random.Generator,
        exact_seen: set[str],
    ) -> GeneratedPair:
        """Construct one formula lazily from bounded typed Grammar rules."""

        choose_available = getattr(policy, "choose_available", None)
        if not callable(choose_available):
            raise TypeError(
                "online typed Grammar policy must implement choose_available"
            )
        decisions = self.online_grammar_decision_specs(
            formula_space_id
        )
        root_decision = decisions[0]
        by_context = {
            row.context_id: row
            for row in decisions[1:]
        }
        base = f"route={ROUTE_ID}|formula_space={formula_space_id}"
        rejected_exact: list[str] = []

        def choose(decision: DecisionSpec, allowed: tuple[str, ...]) -> str:
            return str(
                choose_available(
                    decision,
                    allowed_token_ids=allowed,
                    rng=rng,
                )
            )

        for attempt in range(
            1,
            ONLINE_TYPED_GRAMMAR_GENERATION_ATTEMPT_CAP + 1,
        ):
            operator_token = choose(
                root_decision,
                tuple(
                    row.token_id
                    for row in root_decision.ordered_choices
                ),
            )
            operator_id = operator_token
            context = base + f"|root_rule={operator_id}"

            selected_decisions = [root_decision]
            selected_tokens = [operator_token]
            selected_genes: dict[str, str] = {}
            for slot in (
                "left_transform_id",
                "right_transform_id",
                "left_field_id",
            ):
                decision = by_context[
                    context + f"|decision={slot}"
                ]
                allowed_choices = tuple(decision.ordered_choices)
                if (
                    slot == "right_transform_id"
                ):
                    allowed_choices = tuple(
                        row
                        for row in allowed_choices
                        if (
                            selected_genes["left_transform_id"],
                            str(row.gene_value),
                        )
                        in MINUTE_STATIC_ONLINE_TRANSFORM_PAIRS[
                            operator_id
                        ]
                    )
                if (
                    operator_id == "MUL"
                    and slot == "left_field_id"
                    and selected_genes["left_transform_id"]
                    == selected_genes["right_transform_id"]
                ):
                    maximum_field_id = max(
                        str(row.gene_value)
                        for row in allowed_choices
                    )
                    allowed_choices = tuple(
                        row
                        for row in allowed_choices
                        if str(row.gene_value) != maximum_field_id
                    )
                token_id = choose(
                    decision,
                    tuple(
                        row.token_id
                        for row in allowed_choices
                    ),
                )
                choice = next(
                    row
                    for row in decision.ordered_choices
                    if row.token_id == token_id
                )
                selected_genes[slot] = str(choice.gene_value)
                selected_decisions.append(decision)
                selected_tokens.append(token_id)

            right_decision = by_context[
                context + "|decision=right_field_id"
            ]
            left_field_id = selected_genes["left_field_id"]
            left_transform_id = selected_genes[
                "left_transform_id"
            ]
            right_transform_id = selected_genes[
                "right_transform_id"
            ]
            right_token = choose(
                right_decision,
                tuple(
                    row.token_id
                    for row in right_decision.ordered_choices
                    if (
                        str(row.gene_value) != left_field_id
                        and (
                            (
                                MINUTE_STATIC_TYPED_TRANSFORM_IDS.index(
                                    right_transform_id
                                ),
                                str(row.gene_value),
                            )
                            > (
                                MINUTE_STATIC_TYPED_TRANSFORM_IDS.index(
                                    left_transform_id
                                ),
                                left_field_id,
                            )
                            if operator_id == "MUL"
                            else True
                        )
                    )
                ),
            )
            right_choice = next(
                row
                for row in right_decision.ordered_choices
                if row.token_id == right_token
            )
            selected_genes["right_field_id"] = str(
                right_choice.gene_value
            )
            selected_decisions.append(right_decision)
            selected_tokens.append(right_token)

            pair = self.generator.propose_categorical_genes(
                ROUTE_ID,
                genes={
                    "skeleton_id": self.online_operator_skeletons[
                        operator_id
                    ],
                    "gene_surface_id": self.online_gene_surface_id,
                    "field_pair_id": (
                        selected_genes["left_field_id"]
                        + "::"
                        + selected_genes["right_field_id"]
                    ),
                    "binary_operator_id": operator_id,
                    "left_transform_id": selected_genes[
                        "left_transform_id"
                    ],
                    "right_transform_id": selected_genes[
                        "right_transform_id"
                    ],
                },
                formula_extension_id=(
                    MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID
                ),
            )
            exact_identity = str(
                pair.candidate["exact_identity"]
            )
            if exact_identity in exact_seen:
                rejected_exact.append(exact_identity)
                continue

            trace = [
                DecisionRecord(
                    decision_id=decision.decision_id,
                    context_id=decision.context_id,
                    decision_type=decision.decision_type,
                    selected_token_id=token_id,
                ).to_dict()
                for decision, token_id in zip(
                    selected_decisions,
                    selected_tokens,
                    strict=True,
                )
            ]
            shared = {
                "formula_space_id": formula_space_id,
                "formula_extension_id": (
                    MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_EXTENSION_ID
                ),
                "formula_construction": "ONLINE_TYPED_GRAMMAR",
                "binary_operator_id": operator_id,
                "left_transform_id": selected_genes[
                    "left_transform_id"
                ],
                "right_transform_id": selected_genes[
                    "right_transform_id"
                ],
                "left_field_id": selected_genes["left_field_id"],
                "right_field_id": selected_genes["right_field_id"],
                "decision_trace": trace,
                "decision_trace_hash": _stable_hash(trace),
                "generator_policy": str(
                    getattr(policy, "policy_id", type(policy).__name__)
                ),
                "exact_availability_mask": (
                    "POST_CONSTRUCTION_ARCHIVE_CHECK_WITH_BOUNDED_RESAMPLE"
                ),
                "canonical_dedupe_contract": (
                    "EXACT_IDENTITY_HASHES_COMPILER_CANONICAL_EXPRESSION"
                ),
                "online_generation_attempts": attempt,
                "online_exact_duplicate_rejections": len(
                    rejected_exact
                ),
                "online_rejected_exact_identities": rejected_exact,
                "eager_formula_catalog_materialized": False,
                "commutative_rule_canonicalization": (
                    "ORDERED_TYPED_LEG_KEYS"
                    if operator_id == "MUL"
                    else "NOT_APPLICABLE"
                ),
            }
            return GeneratedPair(
                {**dict(pair.candidate), **shared},
                {**dict(pair.control), **shared},
            )
        raise RuntimeError(
            "MINUTE_ONLINE_TYPED_GRAMMAR_RETRY_LIMIT_REACHED:"
            f"attempts={ONLINE_TYPED_GRAMMAR_GENERATION_ATTEMPT_CAP}:"
            f"exact_duplicate_rejections={len(rejected_exact)}"
        )

    def _generate_available_typed(
        self,
        *,
        formula_space_id: str,
        policy: Any,
        rng: np.random.Generator,
        exact_seen: set[str],
    ) -> GeneratedPair:
        """Draw lossless typed coordinates under the joint exact mask."""

        available = [
            row
            for row in self._available_candidate_catalog(formula_space_id)
            if str(row["candidate"]["exact_identity"]) not in exact_seen
        ]
        if not available:
            raise RuntimeError("MINUTE_STRUCTURAL_EXACT_SUPPLY_EXHAUSTED")
        choose_available = getattr(policy, "choose_available", None)
        if not callable(choose_available):
            raise TypeError(
                "typed structural policy must implement choose_available"
            )
        production, left, right = self.structural_typed_decision_specs(
            formula_space_id
        )

        allowed_productions = {
            str(row["production_id"]) for row in available
        }
        selected_production = str(
            choose_available(
                production,
                allowed_token_ids=tuple(
                    choice.token_id
                    for choice in production.ordered_choices
                    if choice.token_id in allowed_productions
                ),
                rng=rng,
            )
        )
        production_rows = [
            row
            for row in available
            if str(row["production_id"]) == selected_production
        ]
        allowed_left = {
            str(row["field_pair_id"]).split("::", 1)[0]
            for row in production_rows
        }
        selected_left_token = str(
            choose_available(
                left,
                allowed_token_ids=tuple(
                    choice.token_id
                    for choice in left.ordered_choices
                    if str(choice.gene_value) in allowed_left
                ),
                rng=rng,
            )
        )
        selected_left = selected_left_token.split(":", 1)[1]
        left_rows = [
            row
            for row in production_rows
            if str(row["field_pair_id"]).split("::", 1)[0]
            == selected_left
        ]
        allowed_right = {
            str(row["field_pair_id"]).split("::", 1)[1]
            for row in left_rows
        }
        selected_right_token = str(
            choose_available(
                right,
                allowed_token_ids=tuple(
                    choice.token_id
                    for choice in right.ordered_choices
                    if str(choice.gene_value) in allowed_right
                ),
                rng=rng,
            )
        )
        selected_right = selected_right_token.split(":", 1)[1]
        selected_pair_id = selected_left + "::" + selected_right
        selected = next(
            row
            for row in left_rows
            if str(row["field_pair_id"]) == selected_pair_id
        )
        trace = [
            DecisionRecord(
                decision_id=decision.decision_id,
                context_id=decision.context_id,
                decision_type=decision.decision_type,
                selected_token_id=token_id,
            ).to_dict()
            for decision, token_id in (
                (production, selected_production),
                (left, selected_left_token),
                (right, selected_right_token),
            )
        ]
        shared = {
            "formula_space_id": formula_space_id,
            "production_id": selected_production,
            "left_field_id": selected_left,
            "right_field_id": selected_right,
            "decision_trace": trace,
            "decision_trace_hash": _stable_hash(trace),
            "generator_policy": str(
                getattr(policy, "policy_id", type(policy).__name__)
            ),
            "exact_availability_mask": (
                "APPLIED_BEFORE_EACH_HIERARCHICAL_DRAW"
            ),
        }
        return GeneratedPair(
            {**dict(selected["candidate"]), **shared},
            {**dict(selected["control"]), **shared},
        )

    def _generate_available_v4(
        self,
        *,
        formula_space_id: str,
        policy: Any,
        rng: np.random.Generator,
        exact_seen: set[str],
    ) -> GeneratedPair:
        """Draw five bounded typed coordinates under the exact joint mask."""

        available = [
            row
            for row in self._available_candidate_catalog(
                formula_space_id
            )
            if str(row["candidate"]["exact_identity"])
            not in exact_seen
        ]
        if not available:
            raise RuntimeError(
                "MINUTE_FORMULA_V4_EXACT_SUPPLY_EXHAUSTED"
            )
        choose_available = getattr(policy, "choose_available", None)
        if not callable(choose_available):
            raise TypeError(
                "formula V4 policy must implement choose_available"
            )
        (
            production,
            left_transform,
            right_transform,
            left_field,
            right_field,
        ) = self.structural_typed_decision_specs(formula_space_id)

        def choose(
            decision: DecisionSpec,
            allowed_token_values: set[str],
        ) -> str:
            token_id = str(
                choose_available(
                    decision,
                    allowed_token_ids=tuple(
                        choice.token_id
                        for choice in decision.ordered_choices
                        if choice.token_id
                        in allowed_token_values
                    ),
                    rng=rng,
                )
            )
            return token_id

        selected_production = choose(
            production,
            {
                str(row["production_id"])
                for row in available
            },
        )
        production_rows = [
            row
            for row in available
            if str(row["production_id"]) == selected_production
        ]
        selected_left_transform_token = choose(
            left_transform,
            {
                "transform:" + str(row["left_transform_id"])
                for row in production_rows
            },
        )
        selected_left_transform = (
            selected_left_transform_token.split(":", 1)[1]
        )
        left_transform_rows = [
            row
            for row in production_rows
            if str(row["left_transform_id"])
            == selected_left_transform
        ]
        selected_right_transform_token = choose(
            right_transform,
            {
                "transform:" + str(row["right_transform_id"])
                for row in left_transform_rows
            },
        )
        selected_right_transform = (
            selected_right_transform_token.split(":", 1)[1]
        )
        transform_rows = [
            row
            for row in left_transform_rows
            if str(row["right_transform_id"])
            == selected_right_transform
        ]
        selected_left_field_token = choose(
            left_field,
            {
                "field:"
                + str(row["field_pair_id"]).split("::", 1)[0]
                for row in transform_rows
            },
        )
        selected_left_field = (
            selected_left_field_token.split(":", 1)[1]
        )
        left_field_rows = [
            row
            for row in transform_rows
            if str(row["field_pair_id"]).split("::", 1)[0]
            == selected_left_field
        ]
        selected_right_field_token = choose(
            right_field,
            {
                "field:"
                + str(row["field_pair_id"]).split("::", 1)[1]
                for row in left_field_rows
            },
        )
        selected_right_field = (
            selected_right_field_token.split(":", 1)[1]
        )
        selected_pair_id = (
            selected_left_field + "::" + selected_right_field
        )
        selected = next(
            row
            for row in left_field_rows
            if str(row["field_pair_id"]) == selected_pair_id
        )
        selected_tokens = (
            selected_production,
            selected_left_transform_token,
            selected_right_transform_token,
            selected_left_field_token,
            selected_right_field_token,
        )
        decisions = (
            production,
            left_transform,
            right_transform,
            left_field,
            right_field,
        )
        trace = [
            DecisionRecord(
                decision_id=decision.decision_id,
                context_id=decision.context_id,
                decision_type=decision.decision_type,
                selected_token_id=token_id,
            ).to_dict()
            for decision, token_id in zip(
                decisions,
                selected_tokens,
                strict=True,
            )
        ]
        shared = {
            "formula_space_id": formula_space_id,
            "production_id": selected_production,
            "left_transform_id": selected_left_transform,
            "right_transform_id": selected_right_transform,
            "left_field_id": selected_left_field,
            "right_field_id": selected_right_field,
            "decision_trace": trace,
            "decision_trace_hash": _stable_hash(trace),
            "generator_policy": str(
                getattr(policy, "policy_id", type(policy).__name__)
            ),
            "exact_availability_mask": (
                "APPLIED_BEFORE_EACH_OF_FIVE_HIERARCHICAL_DRAWS"
            ),
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


def _expression_depth(expression: str) -> int:
    depth = maximum = 0
    for character in str(expression):
        if character == "(":
            depth += 1
            maximum = max(maximum, depth)
        elif character == ")":
            depth -= 1
    if depth != 0:
        raise ValueError("UNBALANCED_EXPRESSION_DEPTH")
    return maximum


def _structural_formula_v4_mechanical_proof(
    projection: MinuteStaticProductionProjection,
    *,
    seed: int,
    generation_size: int = 32,
) -> dict[str, Any]:
    """Prove five-axis parity and adaptation without market or label reads."""

    decisions = projection.structural_typed_decision_specs(
        STRUCTURAL_FORMULA_V4_SPACE_ID
    )
    catalog_hash = projection.structural_typed_decision_catalog_hash(
        STRUCTURAL_FORMULA_V4_SPACE_ID
    )
    uniform = AvailableUniformPolicy()
    cem = RankWeightedCategoricalCEMPolicy.fresh(
        decisions=decisions,
        decision_catalog_hash=catalog_hash,
        formula_space_id=STRUCTURAL_FORMULA_V4_SPACE_ID,
    )
    uniform_rng = np.random.default_rng(seed)
    cem_rng = np.random.default_rng(seed)
    uniform_seen: set[str] = set()
    cem_seen: set[str] = set()
    uniform_first: list[str] = []
    cem_first: list[str] = []
    observations = []
    for _ in range(generation_size):
        uniform_pair = projection.generate_available(
            formula_space_id=STRUCTURAL_FORMULA_V4_SPACE_ID,
            policy=uniform,
            rng=uniform_rng,
            exact_seen=uniform_seen,
        )
        cem_pair = projection.generate_available(
            formula_space_id=STRUCTURAL_FORMULA_V4_SPACE_ID,
            policy=cem,
            rng=cem_rng,
            exact_seen=cem_seen,
        )
        uniform_exact = str(
            uniform_pair.candidate["exact_identity"]
        )
        cem_exact = str(cem_pair.candidate["exact_identity"])
        uniform_seen.add(uniform_exact)
        cem_seen.add(cem_exact)
        uniform_first.append(uniform_exact)
        cem_first.append(cem_exact)
        observations.append(
            {
                "exact_identity": cem_exact,
                "outcome_class": "EVALUATED",
                "signed_matched_increment": float(
                    int(
                        hashlib.sha256(
                            cem_exact.encode("utf-8")
                        ).hexdigest()[:16],
                        16,
                    )
                ),
                "decision_trace": cem_pair.candidate[
                    "decision_trace"
                ],
            }
        )
    tell_receipt = cem.tell(observations)
    uniform_second: list[str] = []
    cem_second: list[str] = []
    for _ in range(generation_size):
        uniform_pair = projection.generate_available(
            formula_space_id=STRUCTURAL_FORMULA_V4_SPACE_ID,
            policy=uniform,
            rng=uniform_rng,
            exact_seen=uniform_seen,
        )
        cem_pair = projection.generate_available(
            formula_space_id=STRUCTURAL_FORMULA_V4_SPACE_ID,
            policy=cem,
            rng=cem_rng,
            exact_seen=cem_seen,
        )
        uniform_exact = str(
            uniform_pair.candidate["exact_identity"]
        )
        cem_exact = str(cem_pair.candidate["exact_identity"])
        uniform_seen.add(uniform_exact)
        cem_seen.add(cem_exact)
        uniform_second.append(uniform_exact)
        cem_second.append(cem_exact)

    first_parity = uniform_first == cem_first
    duplicate_count = (
        len(cem_first + cem_second)
        - len(set(cem_first + cem_second))
    )
    updated_context_count = int(
        tell_receipt["updated_context_count"]
    )
    passed = (
        first_parity
        and duplicate_count == 0
        and uniform_second != cem_second
        and updated_context_count == len(decisions)
    )
    return {
        "schema_version": (
            "cn_minute_static_structural_formula_v4_"
            "mechanical_proof_v1"
        ),
        "status": "PASS" if passed else "FAIL",
        "proof_type": "SYNTHETIC_NON_FINANCIAL",
        "formula_space_id": STRUCTURAL_FORMULA_V4_SPACE_ID,
        "seed": int(seed),
        "generation_size": int(generation_size),
        "decision_slots": [
            decision.gene_slot for decision in decisions
        ],
        "first_generation_exact_stream_parity": first_parity,
        "cumulative_duplicate_count": duplicate_count,
        "second_generation_stream_changed": (
            uniform_second != cem_second
        ),
        "updated_context_count": updated_context_count,
        "support_diagnostics": tell_receipt[
            "support_diagnostics"
        ],
        "financial_reads": 0,
        "phase3cm_pair_count": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def _structural_typed_surface_mechanical_proof(
    projection: MinuteStaticProductionProjection,
    *,
    seed: int,
    generation_size: int = 32,
) -> dict[str, Any]:
    """Prove parity, masking, and adaptation without financial reads."""

    decisions = projection.structural_typed_decision_specs(
        STRUCTURAL_TYPED_FORMULA_SPACE_ID
    )
    catalog_hash = projection.structural_typed_decision_catalog_hash(
        STRUCTURAL_TYPED_FORMULA_SPACE_ID
    )
    uniform = AvailableUniformPolicy()
    cem = RankWeightedCategoricalCEMPolicy.fresh(
        decisions=decisions,
        decision_catalog_hash=catalog_hash,
        formula_space_id=STRUCTURAL_TYPED_FORMULA_SPACE_ID,
    )
    uniform_rng = np.random.default_rng(seed)
    cem_rng = np.random.default_rng(seed)
    uniform_seen: set[str] = set()
    cem_seen: set[str] = set()
    first_uniform: list[GeneratedPair] = []
    first_cem: list[GeneratedPair] = []
    for _ in range(generation_size):
        uniform_pair = projection.generate_available(
            formula_space_id=STRUCTURAL_TYPED_FORMULA_SPACE_ID,
            policy=uniform,
            rng=uniform_rng,
            exact_seen=uniform_seen,
        )
        cem_pair = projection.generate_available(
            formula_space_id=STRUCTURAL_TYPED_FORMULA_SPACE_ID,
            policy=cem,
            rng=cem_rng,
            exact_seen=cem_seen,
        )
        uniform_exact = str(
            uniform_pair.candidate["exact_identity"]
        )
        cem_exact = str(cem_pair.candidate["exact_identity"])
        uniform_seen.add(uniform_exact)
        cem_seen.add(cem_exact)
        first_uniform.append(uniform_pair)
        first_cem.append(cem_pair)

    first_uniform_exact = [
        str(row.candidate["exact_identity"]) for row in first_uniform
    ]
    first_cem_exact = [
        str(row.candidate["exact_identity"]) for row in first_cem
    ]
    observations = []
    for pair in first_cem:
        exact_identity = str(pair.candidate["exact_identity"])
        synthetic_rank = int(
            hashlib.sha256(exact_identity.encode("utf-8")).hexdigest()[:16],
            16,
        )
        observations.append(
            {
                "exact_identity": exact_identity,
                "outcome_class": "EVALUATED",
                "signed_matched_increment": float(synthetic_rank),
                "decision_trace": pair.candidate["decision_trace"],
            }
        )
    tell_receipt = cem.tell(observations)
    field_only_cem = RankWeightedCategoricalCEMPolicy.fresh(
        decisions=decisions[1:],
        decision_catalog_hash=catalog_hash,
        formula_space_id=STRUCTURAL_TYPED_FORMULA_SPACE_ID,
    )
    field_only_receipt = field_only_cem.tell(observations)

    class _FieldOnlyProofPolicy:
        policy_id = "synthetic_field_only_cem_proof"

        def __init__(
            self,
            adaptive: RankWeightedCategoricalCEMPolicy,
        ) -> None:
            self.adaptive = adaptive
            self.uniform = AvailableUniformPolicy()

        def choose_available(
            self,
            decision: DecisionSpec,
            *,
            allowed_token_ids: Sequence[str],
            rng: np.random.Generator,
        ) -> str:
            if decision.gene_slot == "production_id":
                return self.uniform.choose_available(
                    decision,
                    allowed_token_ids=allowed_token_ids,
                    rng=rng,
                )
            return self.adaptive.choose_available(
                decision,
                allowed_token_ids=allowed_token_ids,
                rng=rng,
            )

    second_uniform_exact = []
    second_cem_exact = []
    for _ in range(generation_size):
        uniform_pair = projection.generate_available(
            formula_space_id=STRUCTURAL_TYPED_FORMULA_SPACE_ID,
            policy=uniform,
            rng=uniform_rng,
            exact_seen=uniform_seen,
        )
        cem_pair = projection.generate_available(
            formula_space_id=STRUCTURAL_TYPED_FORMULA_SPACE_ID,
            policy=cem,
            rng=cem_rng,
            exact_seen=cem_seen,
        )
        uniform_exact = str(
            uniform_pair.candidate["exact_identity"]
        )
        cem_exact = str(cem_pair.candidate["exact_identity"])
        uniform_seen.add(uniform_exact)
        cem_seen.add(cem_exact)
        second_uniform_exact.append(uniform_exact)
        second_cem_exact.append(cem_exact)

    causal_uniform_rng = np.random.default_rng(seed + 1)
    causal_field_rng = np.random.default_rng(seed + 1)
    causal_uniform_seen = set(first_cem_exact)
    causal_field_seen = set(first_cem_exact)
    causal_uniform_exact = []
    causal_field_exact = []
    causal_uniform_productions = []
    causal_field_productions = []
    field_only_policy = _FieldOnlyProofPolicy(field_only_cem)
    for _ in range(generation_size):
        causal_uniform_pair = projection.generate_available(
            formula_space_id=STRUCTURAL_TYPED_FORMULA_SPACE_ID,
            policy=AvailableUniformPolicy(),
            rng=causal_uniform_rng,
            exact_seen=causal_uniform_seen,
        )
        causal_field_pair = projection.generate_available(
            formula_space_id=STRUCTURAL_TYPED_FORMULA_SPACE_ID,
            policy=field_only_policy,
            rng=causal_field_rng,
            exact_seen=causal_field_seen,
        )
        uniform_exact = str(
            causal_uniform_pair.candidate["exact_identity"]
        )
        field_exact = str(
            causal_field_pair.candidate["exact_identity"]
        )
        causal_uniform_seen.add(uniform_exact)
        causal_field_seen.add(field_exact)
        causal_uniform_exact.append(uniform_exact)
        causal_field_exact.append(field_exact)
        causal_uniform_productions.append(
            str(causal_uniform_pair.candidate["production_id"])
        )
        causal_field_productions.append(
            str(causal_field_pair.candidate["production_id"])
        )

    first_parity = first_uniform_exact == first_cem_exact
    first_duplicate_count = (
        len(first_cem_exact) - len(set(first_cem_exact))
    )
    second_duplicate_count = (
        len(first_cem_exact + second_cem_exact)
        - len(set(first_cem_exact + second_cem_exact))
    )
    stream_changed = second_uniform_exact != second_cem_exact
    causal_production_parity = (
        causal_uniform_productions == causal_field_productions
    )
    added_field_contexts_change_proposals = (
        causal_production_parity
        and causal_uniform_exact != causal_field_exact
        and int(field_only_receipt["updated_context_count"]) == 2
    )
    updated_context_count = int(
        tell_receipt["updated_context_count"]
    )
    passed = (
        first_parity
        and first_duplicate_count == 0
        and second_duplicate_count == 0
        and stream_changed
        and added_field_contexts_change_proposals
        and updated_context_count == len(decisions)
    )
    return {
        "schema_version": (
            "cn_minute_static_structural_typed_mechanical_proof_v1"
        ),
        "status": "PASS" if passed else "FAIL",
        "proof_type": "SYNTHETIC_NON_FINANCIAL",
        "formula_space_id": STRUCTURAL_TYPED_FORMULA_SPACE_ID,
        "seed": int(seed),
        "generation_size": int(generation_size),
        "first_generation_exact_stream_parity": first_parity,
        "first_generation_duplicate_count": first_duplicate_count,
        "second_generation_stream_changed": stream_changed,
        "second_generation_duplicate_count": second_duplicate_count,
        "field_only_causal_production_stream_parity": (
            causal_production_parity
        ),
        "added_field_contexts_change_proposals": (
            added_field_contexts_change_proposals
        ),
        "field_only_updated_context_count": int(
            field_only_receipt["updated_context_count"]
        ),
        "updated_context_count": updated_context_count,
        "support_diagnostics": tell_receipt["support_diagnostics"],
        "financial_reads": 0,
        "phase3cm_pair_count": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


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
    formula_space_id: str = EXPANDED_FORMULA_SPACE_ID,
    exact_seen: set[str] | None = None,
) -> dict[str, Any]:
    decisions = projection.adaptive_decision_specs(
        formula_space_id
    )
    catalog_hash = projection.adaptive_decision_catalog_hash(
        formula_space_id
    )
    uniform = AvailableUniformPolicy()
    cem = RankWeightedCategoricalCEMPolicy.fresh(
        decisions=decisions,
        decision_catalog_hash=catalog_hash,
        formula_space_id=formula_space_id,
    )
    uniform_rng = np.random.default_rng(int(seed))
    cem_rng = np.random.default_rng(int(seed))
    uniform_seen = set(exact_seen or ())
    cem_seen = set(exact_seen or ())
    uniform_rows = []
    cem_rows = []
    for _ in range(int(count)):
        left = projection.generate_available(
            formula_space_id=formula_space_id,
            policy=uniform,
            rng=uniform_rng,
            exact_seen=uniform_seen,
        )
        right = projection.generate_available(
            formula_space_id=formula_space_id,
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
        "formula_space_id": formula_space_id,
        "initial_exact_memory_count": len(exact_seen or ()),
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


def _verify_reused_structural_canary(
    root: Path,
) -> tuple[set[str], dict[str, Any]]:
    root = root.resolve()
    manifest_path = root / "artifact_manifest.json"
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8-sig")
    )
    _verify_artifacts(root, manifest)
    final = json.loads(
        (root / "final_decision.json").read_text(encoding="utf-8-sig")
    )
    if (
        str(manifest.get("status") or "") != "CAMPAIGN_CLOSED"
        or str(final.get("STRUCTURAL_CEM_V2_CANARY") or "")
        != "MECHANICAL_PASS_RUN_MEDIUM"
    ):
        raise RuntimeError("STRUCTURAL_CEM_V2_CANARY_NOT_QUALIFIED")
    exact: set[str] = set()
    batch_manifest_hashes = []
    for arm in PAIRED_STRUCTURAL_CEM_V2_ARMS:
        for checkpoint in range(
            1, PAIRED_STRUCTURAL_CEM_V2_CHECKPOINT_COUNT + 1
        ):
            batch_root = (
                root / "arms" / arm / f"checkpoint_{checkpoint:03d}"
            )
            batch_manifest_path = batch_root / "batch_manifest.json"
            batch_manifest = json.loads(
                batch_manifest_path.read_text(encoding="utf-8-sig")
            )
            if (
                str(batch_manifest.get("status") or "")
                != "BATCH_CLOSED_IMMUTABLE"
            ):
                raise RuntimeError(
                    "STRUCTURAL_CEM_V2_CANARY_BATCH_NOT_CLOSED"
                )
            _verify_artifacts(batch_root, batch_manifest)
            batch_manifest_hashes.append(_sha256(batch_manifest_path))
            exact.update(
                str(row["exact_identity"])
                for row in _read_rows(
                    batch_root / "proposal_ledger.parquet"
                )
            )
    return exact, {
        "schema_version": (
            "cn_minute_static_structural_cem_v2_canary_reuse_receipt_v1"
        ),
        "status": "REUSED_HASH_VERIFIED_EXACT_MEMORY_ONLY",
        "source_root": str(root),
        "source_manifest_sha256": _sha256(manifest_path),
        "batch_manifest_hashes": batch_manifest_hashes,
        "exact_identity_count": len(exact),
        "optimizer_probabilities_imported": False,
        "reward_observations_imported": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


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


def _paired_structural_medium_verdict(
    output_root: Path,
    arm_metrics: Mapping[str, Mapping[str, Any]],
    *,
    prior_canary_exact: set[str],
    checkpoint_count: int = (
        PAIRED_STRUCTURAL_CEM_V2_MEDIUM_CHECKPOINT_COUNT
    ),
    full_pair_cap: int = PAIRED_STRUCTURAL_CEM_V2_MEDIUM_FULL_PAIR_CAP,
    schema_version: str = (
        "cn_minute_static_structural_cem_v2_medium_verdict_v1"
    ),
) -> dict[str, Any]:
    uniform_arm, cem_arm = PAIRED_STRUCTURAL_CEM_V2_ARMS

    def proposal_stream(arm: str, checkpoint: int) -> list[str]:
        return [
            str(row["exact_identity"])
            for row in sorted(
                _read_rows(
                    output_root
                    / "arms"
                    / arm
                    / f"checkpoint_{checkpoint:03d}"
                    / "proposal_ledger.parquet"
                ),
                key=lambda row: int(row["raw_attempt"]),
            )
        ]

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

    def evaluated_increments(
        arm: str, checkpoints: Sequence[int]
    ) -> list[float]:
        return [
            float(row["signed_matched_increment"])
            for checkpoint in checkpoints
            for row in _read_rows(
                output_root
                / "arms"
                / arm
                / f"checkpoint_{checkpoint:03d}"
                / "observation_ledger.parquet"
            )
            if str(row.get("outcome_class") or "") == "EVALUATED"
        ]

    summaries = {
        (arm, checkpoint): json.loads(
            (
                output_root
                / "arms"
                / arm
                / f"checkpoint_{checkpoint:03d}"
                / "checkpoint_summary.json"
            ).read_text(encoding="utf-8-sig")
        )
        for arm in PAIRED_STRUCTURAL_CEM_V2_ARMS
        for checkpoint in range(1, int(checkpoint_count) + 1)
    }
    proposed_exact = {
        str(row["exact_identity"])
        for arm in PAIRED_STRUCTURAL_CEM_V2_ARMS
        for checkpoint in range(1, int(checkpoint_count) + 1)
        for row in _read_rows(
            output_root
            / "arms"
            / arm
            / f"checkpoint_{checkpoint:03d}"
            / "proposal_ledger.parquet"
        )
    }
    adaptive_checkpoints = tuple(range(2, int(checkpoint_count) + 1))
    uniform_adaptive = evaluated_increments(
        uniform_arm, adaptive_checkpoints
    )
    cem_adaptive = evaluated_increments(cem_arm, adaptive_checkpoints)
    uniform_metrics = arm_metrics[uniform_arm]
    cem_metrics = arm_metrics[cem_arm]
    cem_state = json.loads(
        (
            output_root
            / "arms"
            / cem_arm
            / f"checkpoint_{int(checkpoint_count):03d}"
            / "arm_state.json"
        ).read_text(encoding="utf-8-sig")
    )["optimizer_state"]
    first_uniform = proposal_stream(uniform_arm, 1)
    first_cem = proposal_stream(cem_arm, 1)
    first_uniform_full = evaluated_exact_set(uniform_arm, 1)
    first_cem_full = evaluated_exact_set(cem_arm, 1)
    contracts = {
        "generation_one_exact_stream_parity": (
            bool(first_uniform) and first_uniform == first_cem
        ),
        "generation_one_full_evaluation_set_parity": (
            bool(first_uniform_full)
            and first_uniform_full == first_cem_full
        ),
        "minimum_support": all(
            int(arm_metrics[arm]["evaluated_pairs"])
            >= int(checkpoint_count) * int(full_pair_cap)
            for arm in PAIRED_STRUCTURAL_CEM_V2_ARMS
        ),
        "zero_exact_duplicates": all(
            int(summary["exact_duplicate_pairs"]) == 0
            for summary in summaries.values()
        ),
        "no_canary_exact_replay": not bool(
            proposed_exact & prior_canary_exact
        ),
        "fresh_medium_state_adapted": (
            str(cem_state.get("initialization_origin") or "")
            == "fresh_uniform_from_frozen_decision_catalog"
            and str(cem_state.get("imported_source_campaign") or "")
            == "none"
            and str(cem_state.get("current_state") or "") == "ADAPTED"
            and int(cem_state.get("generation") or 0)
            == int(checkpoint_count)
            and int(cem_state.get("reward_observation_count") or 0)
            >= int(checkpoint_count) * int(full_pair_cap)
        ),
        "sealed_reads_zero": all(
            int(summary[name]) == 0
            for summary in summaries.values()
            for name in (
                "validation_reads",
                "holdout_reads",
                "forward_2026_reads",
            )
        ),
    }
    uniform_positive = sum(value > 0.0 for value in uniform_adaptive)
    cem_positive = sum(value > 0.0 for value in cem_adaptive)
    uniform_median = (
        statistics.median(uniform_adaptive) if uniform_adaptive else None
    )
    cem_median = statistics.median(cem_adaptive) if cem_adaptive else None
    financial_checks = {
        "adaptive_positive_count_noninferior": (
            cem_positive >= uniform_positive
        ),
        "adaptive_median_strictly_better": (
            cem_median is not None
            and uniform_median is not None
            and cem_median > uniform_median
        ),
        "behavior_discovery_noninferior": (
            float(cem_metrics["behavior_discovery_per_evaluated_pair"])
            >= float(
                uniform_metrics["behavior_discovery_per_evaluated_pair"]
            )
        ),
    }
    performance_checks = {
        "uniform_hot_path_host_occupancy_at_least_75pct": (
            float(uniform_metrics["selected_backend_host_cpu_median"] or 0.0)
            >= 0.75
        ),
        "cem_hot_path_host_occupancy_at_least_75pct": (
            float(cem_metrics["selected_backend_host_cpu_median"] or 0.0)
            >= 0.75
        ),
        "cache_at_most_8_gib": max(
            int(uniform_metrics["maximum_observed_cache_bytes"]),
            int(cem_metrics["maximum_observed_cache_bytes"]),
        )
        <= 8 * 1024**3,
        "free_memory_at_least_24_gib": min(
            int(uniform_metrics["minimum_free_memory_bytes"]),
            int(cem_metrics["minimum_free_memory_bytes"]),
        )
        >= 24 * 1024**3,
        "cem_full_pair_throughput_at_least_90pct_uniform": (
            float(cem_metrics["full_coordinate_pairs_per_wall_hour"])
            >= 0.90
            * float(
                uniform_metrics["full_coordinate_pairs_per_wall_hour"]
            )
        ),
    }
    qualified = (
        all(contracts.values())
        and all(financial_checks.values())
        and all(performance_checks.values())
    )
    return {
        "schema_version": str(schema_version),
        "status": (
            "STRUCTURAL_CEM_V2_FINANCIALLY_QUALIFIED"
            if qualified
            else "STRUCTURAL_CEM_V2_NOT_QUALIFIED"
        ),
        "mechanical_contracts": contracts,
        "financial_checks": financial_checks,
        "performance_checks": performance_checks,
        "adaptive_checkpoint_metrics": {
            "uniform_evaluated": len(uniform_adaptive),
            "cem_evaluated": len(cem_adaptive),
            "uniform_positive": uniform_positive,
            "cem_positive": cem_positive,
            "uniform_median": uniform_median,
            "cem_median": cem_median,
        },
        "large_search_authorized": False,
        "ready_for_separate_large_search_authorization": qualified,
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


def _nonexhaustive_supply_decision(
    *,
    post_archive_exact_supply: int,
    observed_behavior_unique_supply: int,
) -> dict[str, Any]:
    limiting_supply = min(
        int(post_archive_exact_supply),
        int(observed_behavior_unique_supply),
    )
    ceiling = (
        limiting_supply
        // STRUCTURAL_SUPPLY_NONEXHAUSTIVE_HEADROOM_MULTIPLIER
    )
    reference_ready = (
        ceiling >= STRUCTURAL_SUPPLY_REFERENCE_PAIR_BUDGET
    )
    return {
        "schema_version": (
            "cn_minute_static_structural_supply_decision_v1"
        ),
        "status": (
            "STRUCTURAL_FORMULA_SPACE_SUPPLY_QUALIFIED"
            if reference_ready
            else "STRUCTURAL_FORMULA_SPACE_SUPPLY_BLOCKED"
        ),
        "post_archive_exact_supply": int(
            post_archive_exact_supply
        ),
        "observed_behavior_unique_supply": int(
            observed_behavior_unique_supply
        ),
        "behavior_supply_claim": (
            "OBSERVED_LOWER_BOUND_WITHIN_FROZEN_PROBE_CAP"
        ),
        "nonexhaustive_headroom_multiplier": (
            STRUCTURAL_SUPPLY_NONEXHAUSTIVE_HEADROOM_MULTIPLIER
        ),
        "maximum_nonexhaustive_pair_budget_per_arm": ceiling,
        "reference_pair_budget_per_arm": (
            STRUCTURAL_SUPPLY_REFERENCE_PAIR_BUDGET
        ),
        "reference_paired_qualification_supply_ready": (
            reference_ready
        ),
        "large_search_authorized": False,
        "next_action": (
            "FREEZE_SEPARATE_PAIRED_SEARCH_POLICY_BUDGET"
            if reference_ready
            else "FREEZE_PAIRED_BUDGET_AT_OR_BELOW_SUPPLY_CEILING"
            if ceiling > 0
            else "EXPAND_LEGITIMATE_ROUTE_LOCAL_FORMULA_SPACE"
        ),
    }


def _formula_v4_supply_decision(
    *,
    post_archive_exact_supply: int,
    observed_behavior_unique_supply: int,
) -> dict[str, Any]:
    exact_ready = (
        int(post_archive_exact_supply)
        >= STRUCTURAL_FORMULA_V4_MINIMUM_FRESH_EXACT
    )
    behavior_ready = (
        int(observed_behavior_unique_supply)
        >= STRUCTURAL_FORMULA_V4_MINIMUM_BEHAVIOR_UNIQUE
    )
    maximum_budget = min(
        int(post_archive_exact_supply)
        // STRUCTURAL_SUPPLY_NONEXHAUSTIVE_HEADROOM_MULTIPLIER,
        int(observed_behavior_unique_supply),
    )
    qualified = exact_ready and behavior_ready
    return {
        "schema_version": (
            "cn_minute_static_structural_formula_v4_"
            "supply_decision_v1"
        ),
        "status": (
            "STRUCTURAL_FORMULA_V4_SUPPLY_QUALIFIED"
            if qualified
            else "STRUCTURAL_FORMULA_V4_SUPPLY_BLOCKED"
        ),
        "post_archive_exact_supply": int(
            post_archive_exact_supply
        ),
        "minimum_post_archive_exact_supply": (
            STRUCTURAL_FORMULA_V4_MINIMUM_FRESH_EXACT
        ),
        "exact_supply_gate": (
            "PASS" if exact_ready else "FAIL"
        ),
        "observed_behavior_unique_supply": int(
            observed_behavior_unique_supply
        ),
        "minimum_behavior_unique_supply": (
            STRUCTURAL_FORMULA_V4_MINIMUM_BEHAVIOR_UNIQUE
        ),
        "behavior_supply_gate": (
            "PASS" if behavior_ready else "FAIL"
        ),
        "behavior_supply_claim": (
            "OBSERVED_LOWER_BOUND_WITHIN_FROZEN_PROBE_CAP"
        ),
        "maximum_supported_paired_budget_per_arm": (
            maximum_budget
        ),
        "reference_pair_budget_per_arm": (
            STRUCTURAL_SUPPLY_REFERENCE_PAIR_BUDGET
        ),
        "reference_paired_qualification_supply_ready": (
            qualified
            and maximum_budget
            >= STRUCTURAL_SUPPLY_REFERENCE_PAIR_BUDGET
        ),
        "financial_qualification_authorized": False,
        "large_search_authorized": False,
        "next_action": (
            "FREEZE_SEPARATE_FRESH_UNIFORM_PAIRED_QUALIFICATION"
            if qualified
            and maximum_budget
            >= STRUCTURAL_SUPPLY_REFERENCE_PAIR_BUDGET
            else "REVISE_BOUNDED_FORMULA_CATALOG"
        ),
    }


def _combined_behavior_archive(
    *paths: Path,
) -> PortfolioBehaviorArchive:
    rows: list[dict[str, Any]] = []
    for path in paths:
        rows.extend(
            PortfolioBehaviorArchive.read_parquet(path).rows
        )
    return PortfolioBehaviorArchive(rows)


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


def _verify_reused_structural_supply(
    root: Path,
    *,
    required_pair_budget_per_arm: int,
) -> dict[str, Any]:
    root = root.resolve()
    manifest_path = root / "artifact_manifest.json"
    design_path = root / "structural_formula_space_supply.json"
    decision_path = root / "supply_decision.json"
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8-sig")
    )
    unsigned_manifest = {
        key: value
        for key, value in manifest.items()
        if key != "manifest_payload_hash"
    }
    if str(manifest.get("manifest_payload_hash") or "") != _stable_hash(
        unsigned_manifest
    ):
        raise RuntimeError("STRUCTURAL_SUPPLY_MANIFEST_PAYLOAD_HASH_DRIFT")
    _verify_artifacts(root, manifest)
    design = json.loads(design_path.read_text(encoding="utf-8-sig"))
    decision = json.loads(
        decision_path.read_text(encoding="utf-8-sig")
    )
    maximum_budget = int(
        decision.get("maximum_nonexhaustive_pair_budget_per_arm") or 0
    )
    access_counts = {
        name: int(manifest.get(name) or 0)
        for name in (
            "financial_reads",
            "phase3cm_pair_count",
            "validation_reads",
            "holdout_reads",
            "forward_2026_reads",
        )
    }
    checks = {
        "formula_space_exact": (
            str(design.get("formula_space_id") or "")
            == STRUCTURAL_SUPPLY_FORMULA_SPACE_ID
        ),
        "production_queue_exact": tuple(
            map(str, design.get("production_ids") or ())
        )
        == STRUCTURAL_SUPPLY_PRODUCTION_IDS,
        "raw_catalog_exact_unique": (
            int(design.get("raw_categorical_rows") or 0) == 440
            and int(design.get("raw_exact_unique") or 0) == 440
            and int(design.get("raw_canonical_unique") or 0) == 440
        ),
        "required_budget_within_observed_nonexhaustive_ceiling": (
            int(required_pair_budget_per_arm) <= maximum_budget
        ),
        "access_boundary_clean": all(
            value == 0 for value in access_counts.values()
        ),
        "large_search_not_authorized": not bool(
            decision.get("large_search_authorized")
        ),
    }
    if not all(checks.values()):
        raise RuntimeError(
            "REUSED_STRUCTURAL_SUPPLY_NOT_QUALIFIED:"
            + ",".join(
                name for name, passed in checks.items() if not passed
            )
        )

    inputs = dict(manifest.get("inputs") or {})

    def input_hashes(*prefixes: str) -> list[str]:
        return sorted(
            str(row.get("sha256") or "")
            for name, row in inputs.items()
            if any(
                str(name) == prefix
                or str(name).startswith(prefix + "_")
                for prefix in prefixes
            )
        )

    return {
        "schema_version": (
            "cn_minute_static_structural_supply_reuse_receipt_v1"
        ),
        "status": "REUSED_HASH_VERIFIED_FINANCIAL_READS_ZERO",
        "source_root": str(root),
        "source_manifest_sha256": _sha256(manifest_path),
        "formula_space_id": STRUCTURAL_SUPPLY_FORMULA_SPACE_ID,
        "production_ids": list(STRUCTURAL_SUPPLY_PRODUCTION_IDS),
        "post_archive_exact_supply": int(
            design.get("post_archive_exact_supply") or 0
        ),
        "post_archive_rows_by_production": dict(
            design.get("post_archive_rows_by_production") or {}
        ),
        "observed_behavior_unique_supply": int(
            design.get("observed_behavior_unique_supply") or 0
        ),
        "maximum_nonexhaustive_pair_budget_per_arm": maximum_budget,
        "required_pair_budget_per_arm": int(
            required_pair_budget_per_arm
        ),
        "exact_archive_sha256s": input_hashes(
            "historical_exact_archive",
            "source_candidate_ledger",
            "additional_exact_archive",
        ),
        "behavior_archive_sha256s": input_hashes(
            "historical_behavior_archive",
            "additional_behavior_archive",
        ),
        "checks": checks,
        **access_counts,
    }


def _structural_supply_generation_audit(
    projection: MinuteStaticProductionProjection,
    *,
    initial_exact: set[str],
    expected_post_archive_exact_supply: int,
    expected_post_archive_rows_by_production: Mapping[str, Any],
) -> dict[str, Any]:
    catalog = projection._available_candidate_catalog(
        STRUCTURAL_SUPPLY_FORMULA_SPACE_ID
    )
    raw_counts = defaultdict(int)
    available_counts = defaultdict(int)
    exact_ids = []
    canonical_ids = []
    lane_domain_checks = {}
    for production_id in STRUCTURAL_SUPPLY_PRODUCTION_IDS:
        skeleton_id = projection._production_skeletons[production_id]
        lane = projection.generator.categorical_gene_space(
            ROUTE_ID,
            skeleton_id=skeleton_id,
        )
        lane_domain_checks[production_id] = tuple(
            lane["ordered_categories_by_slot"]["field_pair_id"]
        ) == tuple(projection.field_pair_ids)
    for row in catalog:
        production_id = str(row["production_id"])
        exact_id = str(row["candidate"]["exact_identity"])
        canonical_id = str(row["candidate"]["canonical_identity"])
        raw_counts[production_id] += 1
        exact_ids.append(exact_id)
        canonical_ids.append(canonical_id)
        if exact_id not in initial_exact:
            available_counts[production_id] += 1
    expected_available = {
        production_id: int(
            expected_post_archive_rows_by_production.get(
                production_id, 0
            )
        )
        for production_id in STRUCTURAL_SUPPLY_PRODUCTION_IDS
    }
    observed_available = {
        production_id: int(available_counts[production_id])
        for production_id in STRUCTURAL_SUPPLY_PRODUCTION_IDS
    }
    checks = {
        "all_productions_use_authoritative_lane_domain": all(
            lane_domain_checks.values()
        ),
        "raw_rows_440": len(catalog) == 440,
        "raw_rows_110_per_production": all(
            int(raw_counts[production_id]) == 110
            for production_id in STRUCTURAL_SUPPLY_PRODUCTION_IDS
        ),
        "raw_exact_unique_440": len(set(exact_ids)) == 440,
        "raw_canonical_unique_440": len(set(canonical_ids)) == 440,
        "all_primary_control_legal": all(
            bool(row["candidate"].get("legal"))
            and bool(row["control"].get("legal"))
            for row in catalog
        ),
        "cumulative_memory_available_count_matches_supply_evidence": (
            sum(observed_available.values())
            == int(expected_post_archive_exact_supply)
        ),
        "cumulative_memory_available_production_counts_match": (
            observed_available == expected_available
        ),
    }
    return {
        "schema_version": (
            "cn_minute_static_structural_supply_generation_audit_v1"
        ),
        "status": "PASS" if all(checks.values()) else "FAIL",
        "formula_space_id": STRUCTURAL_SUPPLY_FORMULA_SPACE_ID,
        "production_ids": list(STRUCTURAL_SUPPLY_PRODUCTION_IDS),
        "authoritative_field_pair_domain_count": len(
            projection.field_pair_ids
        ),
        "raw_rows_by_production": {
            production_id: int(raw_counts[production_id])
            for production_id in STRUCTURAL_SUPPLY_PRODUCTION_IDS
        },
        "post_memory_exact_by_production": observed_available,
        "post_memory_exact_supply": sum(observed_available.values()),
        "initial_exact_memory_count": len(initial_exact),
        "lane_domain_checks": lane_domain_checks,
        "checks": checks,
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


def run_structural_supply_design(
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Qualify formula-space supply without labels or financial evaluation."""

    if (
        not args.allow_noncanonical_host
        and platform.node().upper() != AUTHORIZED_HOST
    ):
        raise RuntimeError(
            f"official structural supply evidence must run on {AUTHORIZED_HOST}"
        )

    typed_surface = bool(
        getattr(args, "structural_typed_surface_audit", False)
    )
    formula_v4 = bool(
        getattr(args, "structural_formula_v4_supply", False)
    )
    formula_space_id = (
        STRUCTURAL_FORMULA_V4_SPACE_ID
        if formula_v4
        else (
            STRUCTURAL_TYPED_FORMULA_SPACE_ID
            if typed_surface
            else STRUCTURAL_SUPPLY_FORMULA_SPACE_ID
        )
    )
    authorization_id = (
        STRUCTURAL_FORMULA_V4_AUTHORIZATION_ID
        if formula_v4
        else (
            STRUCTURAL_TYPED_AUTHORIZATION_ID
            if typed_surface
            else STRUCTURAL_SUPPLY_AUTHORIZATION_ID
        )
    )
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
            "STRUCTURAL_SUPPLY_INPUT_AUTHORITY_MISMATCH:"
            + ",".join(missing)
        )
    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist={ROUTE_ID: materializable},
    )
    projection = MinuteStaticProductionProjection(generator)
    catalog = projection._available_candidate_catalog(
        formula_space_id
    )
    catalog_rows = []
    for ordinal, source in enumerate(catalog):
        primary = dict(source["candidate"])
        control = dict(source["control"])
        catalog_rows.append(
            {
                "ordinal": ordinal,
                "production_id": str(source["production_id"]),
                "skeleton_id": str(source["skeleton_id"]),
                "field_pair_id": str(source["field_pair_id"]),
                "left_transform_id": str(
                    source.get("left_transform_id") or ""
                ),
                "right_transform_id": str(
                    source.get("right_transform_id") or ""
                ),
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

    additional_exact_paths = tuple(
        Path(path).resolve()
        for path in (
            getattr(args, "additional_exact_archive", ()) or ()
        )
    )
    historical_exact_paths = (
        args.historical_exact_archive.resolve(),
        args.source_candidate_ledger.resolve(),
        *additional_exact_paths,
    )
    historical_exact = _historical_exact(
        *historical_exact_paths
    )
    post_archive = _post_archive_rows(
        catalog_rows,
        historical_exact=historical_exact,
    )
    expected_productions = set(STRUCTURAL_SUPPLY_PRODUCTION_IDS)
    observed_productions = {
        str(row["production_id"]) for row in catalog_rows
    }
    expected_v4_rows = (
        len(projection.field_pair_ids)
        * sum(
            len(pairs)
            for pairs in projection.v4_transform_pairs.values()
        )
    )
    static_checks = {
        "production_queue_exact": (
            observed_productions == expected_productions
        ),
        "all_primary_control_legal": all(
            bool(row["legal"]) for row in catalog_rows
        ),
        "all_exact_identity_present": all(
            bool(row["exact_identity"]) for row in catalog_rows
        ),
        "all_canonical_identity_present": all(
            bool(row["canonical_identity"]) for row in catalog_rows
        ),
        "legacy_productions_preserved": {
            "field_spread",
            "normalized_ratio",
        }.issubset(observed_productions),
        "blocked_metadata_lanes_excluded": observed_productions.isdisjoint(
            {
                "price_volume_interaction",
                "liquidity_volatility_interaction",
                "cross_sectional_residual",
            }
        ),
        "formula_v4_raw_row_count_exact": (
            not formula_v4
            or len(catalog_rows) == expected_v4_rows
        ),
        "formula_v4_transform_pair_domain_exact": (
            not formula_v4
            or projection.v4_transform_pairs
            == {
                production_id: tuple(
                    MINUTE_STATIC_TYPED_TRANSFORM_PAIRS[
                        production_id
                    ]
                )
                for production_id
                in STRUCTURAL_SUPPLY_PRODUCTION_IDS
            }
        ),
        "formula_v4_depth_within_declared_maximum": (
            not formula_v4
            or all(
                _expression_depth(
                    str(member.get("expression") or "")
                )
                <= int(member.get("maximum_depth") or 0)
                for row in catalog_rows
                for member in (row["primary"], row["control"])
            )
        ),
        "formula_v4_streaming_operator_support": (
            not formula_v4
            or not unsupported_streaming_operators(
                str(member.get("expression") or "")
                for row in catalog_rows
                for member in (row["primary"], row["control"])
            )
        ),
        "formula_v4_primary_exact_unique": (
            not formula_v4
            or len(
                {
                    str(row["exact_identity"])
                    for row in catalog_rows
                }
            )
            == len(catalog_rows)
        ),
        "formula_v4_primary_canonical_unique": (
            not formula_v4
            or len(
                {
                    str(row["canonical_identity"])
                    for row in catalog_rows
                }
            )
            == len(catalog_rows)
        ),
    }
    if not all(static_checks.values()):
        raise RuntimeError(
            "STRUCTURAL_SUPPLY_STATIC_CONTRACT_FAILED"
        )

    def production_counts(
        rows: Sequence[Mapping[str, Any]],
    ) -> dict[str, int]:
        counts = defaultdict(int)
        for row in rows:
            counts[str(row["production_id"])] += 1
        return {
            production_id: int(counts[production_id])
            for production_id in STRUCTURAL_SUPPLY_PRODUCTION_IDS
        }

    design = {
        "schema_version": (
            "cn_minute_static_structural_formula_v4_supply_v1"
            if formula_v4
            else (
                "cn_minute_static_structural_typed_surface_supply_v1"
                if typed_surface
                else "cn_minute_static_structural_formula_space_supply_v1"
            )
        ),
        "status": "EXACT_SUPPLY_CLOSED_BEHAVIOR_PENDING",
        "authorization_id": authorization_id,
        "route_id": ROUTE_ID,
        "formula_space_id": formula_space_id,
        "production_ids": list(STRUCTURAL_SUPPLY_PRODUCTION_IDS),
        "production_skeletons": {
            production_id: projection._production_skeletons[
                production_id
            ]
            for production_id in STRUCTURAL_SUPPLY_PRODUCTION_IDS
        },
        "typed_transform_ids": (
            list(MINUTE_STATIC_TYPED_TRANSFORM_IDS)
            if formula_v4
            else []
        ),
        "typed_transform_pairs_by_production": (
            {
                production_id: [
                    {
                        "left_transform_id": left,
                        "right_transform_id": right,
                    }
                    for left, right
                    in projection.v4_transform_pairs[production_id]
                ]
                for production_id
                in STRUCTURAL_SUPPLY_PRODUCTION_IDS
            }
            if formula_v4
            else {}
        ),
        "excluded_transform_candidates": (
            {
                "IDENTITY": (
                    "EXCLUDED_CROSS_UNIT_DIMENSIONLESS_SIGNATURE"
                ),
                "DELTA_EXISTING_WINDOW": (
                    "EXCLUDED_MINUTE_STATIC_COMPILER_ALLOWLIST"
                ),
            }
            if formula_v4
            else {}
        ),
        "authority": {
            "route": "UnifiedCapabilityRegistry",
            "grammar": "CompositionalGrammarV2",
            "compiler": "TypedRouteCompiler",
            "matched_control": "existing_route_constructor",
        },
        "static_checks": static_checks,
        "production_root_contract_hash": contract["contract_hash"],
        "decision_catalog_hash": (
            projection.structural_typed_decision_catalog_hash(
                formula_space_id
            )
            if typed_surface or formula_v4
            else projection.structural_decision_catalog_hash(
                formula_space_id
            )
        ),
        "raw_categorical_rows": len(catalog_rows),
        "raw_rows_by_production": production_counts(catalog_rows),
        "raw_exact_unique": len(
            {row["exact_identity"] for row in catalog_rows}
        ),
        "raw_canonical_unique": len(
            {row["canonical_identity"] for row in catalog_rows}
        ),
        "historical_exact_identity_count": len(historical_exact),
        "post_archive_exact_supply": len(post_archive),
        "post_archive_rows_by_production": production_counts(
            post_archive
        ),
        "behavior_probe_cap": BEHAVIOR_PROBE_CAP,
        "behavior_probe_candidates": 0,
        "observed_behavior_unique_supply": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "financial_reads": 0,
        "phase3cm_pair_count": 0,
        "promotion": "FORBIDDEN",
        "large_search_authorized": False,
    }
    member_rows = [
        dict(member)
        for row in post_archive
        for member in (row["primary"], row["control"])
    ]
    candidate_path = _write_parquet(
        output_root / "structural_post_archive_candidates.parquet",
        member_rows,
    )
    design_path = _write_json(
        output_root / "structural_formula_space_supply.json",
        design,
    )
    artifact_paths = [design_path, candidate_path]

    if bool(getattr(args, "static_only", False)):
        if formula_v4:
            decision_catalog = (
                projection.structural_typed_decision_catalog(
                    formula_space_id
                )
            )
            mechanical_proof = (
                _structural_formula_v4_mechanical_proof(
                    projection,
                    seed=FROZEN_SUPPLY_SEED + 4,
                )
            )
            if mechanical_proof["status"] != "PASS":
                raise RuntimeError(
                    "STRUCTURAL_FORMULA_V4_MECHANICAL_PROOF_FAILED"
                )
            exact_ready = (
                len(post_archive)
                >= STRUCTURAL_FORMULA_V4_MINIMUM_FRESH_EXACT
            )
            status = (
                "STRUCTURAL_FORMULA_V4_EXACT_PASS_BEHAVIOR_PENDING"
                if exact_ready
                else "STRUCTURAL_FORMULA_V4_EXACT_SUPPLY_BLOCKED"
            )
            decision = {
                "schema_version": (
                    "cn_minute_static_structural_formula_v4_"
                    "static_decision_v1"
                ),
                "status": status,
                "post_archive_exact_supply": len(post_archive),
                "minimum_post_archive_exact_supply": (
                    STRUCTURAL_FORMULA_V4_MINIMUM_FRESH_EXACT
                ),
                "exact_supply_gate": (
                    "PASS" if exact_ready else "FAIL"
                ),
                "behavior_supply_gate": "PENDING",
                "financial_qualification_authorized": False,
                "large_search_authorized": False,
                "next_action": (
                    "RUN_LABEL_FREE_BEHAVIOR_SUPPLY_PROBE"
                    if exact_ready
                    else "REVISE_BOUNDED_FORMULA_CATALOG"
                ),
            }
            design.update(
                {
                    "status": status,
                    "formula_v4_mechanical_status": (
                        "FIVE_AXIS_MECHANICAL_PASS"
                    ),
                    "minimum_post_archive_exact_supply": (
                        STRUCTURAL_FORMULA_V4_MINIMUM_FRESH_EXACT
                    ),
                }
            )
            _write_json(design_path, design)
            catalog_path = _write_json(
                output_root
                / "structural_formula_v4_decision_catalog.json",
                decision_catalog,
            )
            proof_path = _write_json(
                output_root
                / "synthetic_v4_adaptation_proof.json",
                mechanical_proof,
            )
            artifact_paths.extend((catalog_path, proof_path))
        elif typed_surface:
            decision_catalog = (
                projection.structural_typed_decision_catalog(
                    formula_space_id
                )
            )
            mechanical_proof = (
                _structural_typed_surface_mechanical_proof(
                    projection,
                    seed=FROZEN_SUPPLY_SEED + 2,
                )
            )
            if mechanical_proof["status"] != "PASS":
                raise RuntimeError(
                    "STRUCTURAL_TYPED_SURFACE_MECHANICAL_PROOF_FAILED"
                )
            exact_minimum = (
                STRUCTURAL_SUPPLY_REFERENCE_PAIR_BUDGET
                * STRUCTURAL_SUPPLY_NONEXHAUSTIVE_HEADROOM_MULTIPLIER
            )
            exact_ready = len(post_archive) >= exact_minimum
            status = (
                "STRUCTURAL_TYPED_SURFACE_RAW_CATALOG_"
                "MECHANICAL_PASS_"
                + (
                    "BEHAVIOR_SUPPLY_PENDING"
                    if exact_ready
                    else "FINANCIAL_SUPPLY_BLOCKED"
                )
            )
            decision = {
                "schema_version": (
                    "cn_minute_static_structural_typed_"
                    "surface_decision_v1"
                ),
                "status": status,
                "post_archive_exact_supply": len(post_archive),
                "minimum_nonexhaustive_exact_supply": exact_minimum,
                "maximum_nonexhaustive_pair_budget_per_arm": (
                    len(post_archive)
                    // STRUCTURAL_SUPPLY_NONEXHAUSTIVE_HEADROOM_MULTIPLIER
                ),
                "financial_qualification_authorized": False,
                "reference_paired_qualification_supply_ready": (
                    exact_ready
                ),
                "large_search_authorized": False,
                "next_action": (
                    "RUN_LABEL_FREE_BEHAVIOR_SUPPLY_PROBE"
                    if exact_ready
                    else (
                        "EXPAND_AUTHORITATIVE_FORMULA_SUPPLY_"
                        "BEFORE_FINANCIAL_RETRY"
                    )
                ),
            }
            design.update(
                {
                    "status": status,
                    "typed_surface_mechanical_status": (
                        "RAW_CATALOG_MECHANICAL_PASS"
                    ),
                    "minimum_nonexhaustive_exact_supply": (
                        exact_minimum
                    ),
                    "reference_paired_qualification_supply_ready": (
                        exact_ready
                    ),
                }
            )
            _write_json(design_path, design)
            catalog_path = _write_json(
                output_root
                / "structural_typed_decision_catalog.json",
                decision_catalog,
            )
            proof_path = _write_json(
                output_root / "synthetic_adaptation_proof.json",
                mechanical_proof,
            )
            artifact_paths.extend((catalog_path, proof_path))
        else:
            decision = {
                "schema_version": (
                    "cn_minute_static_structural_supply_decision_v1"
                ),
                "status": "BEHAVIOR_SUPPLY_PENDING",
                "post_archive_exact_supply": len(post_archive),
                "maximum_nonexhaustive_pair_budget_per_arm": None,
                "reference_paired_qualification_supply_ready": False,
                "large_search_authorized": False,
                "next_action": "RUN_LABEL_FREE_BEHAVIOR_SUPPLY_PROBE",
            }
            status = "STRUCTURAL_EXACT_SUPPLY_CLOSED_BEHAVIOR_PENDING"
    else:
        split = FixedSplitAuthority.read(
            args.split_manifest.resolve()
        )
        if (
            str(layout.get("split_manifest_hash") or "")
            != split.manifest_hash
        ):
            raise RuntimeError(
                "STRUCTURAL_SUPPLY_SIDECAR_SPLIT_HASH_DRIFT"
            )
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
                    "authorization_id": (
                        authorization_id
                    ),
                    "contract_hash": contract["contract_hash"],
                    "decision_catalog_hash": design[
                        "decision_catalog_hash"
                    ],
                    "split_hash": split.manifest_hash,
                    "seed": FROZEN_SUPPLY_SEED,
                }
            ),
            batch_id=(
                "minute_static.structural_formula_v4"
                if formula_v4
                else "minute_static.structural_supply_v1"
            ),
            compute_threads=int(args.compute_threads),
            max_trade_times=30,
            max_trade_dates=1,
            date_selection="calendar_stratified",
            pair_batch_size=4,
        )
        additional_behavior_paths = tuple(
            Path(path).resolve()
            for path in (
                getattr(
                    args,
                    "additional_behavior_archive",
                    (),
                )
                or ()
            )
        )
        behavior_archive_paths = (
            args.historical_behavior_archive.resolve(),
            *additional_behavior_paths,
        )
        behavior_archive = _combined_behavior_archive(
            *behavior_archive_paths
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
        decision = (
            _formula_v4_supply_decision(
                post_archive_exact_supply=len(post_archive),
                observed_behavior_unique_supply=behavior_unique,
            )
            if formula_v4
            else _nonexhaustive_supply_decision(
                post_archive_exact_supply=len(post_archive),
                observed_behavior_unique_supply=behavior_unique,
            )
        )
        status = str(decision["status"])
        design.update(
            {
                "status": status,
                "behavior_probe_candidates": len(selected),
                "observed_behavior_unique_supply": behavior_unique,
                "historical_behavior_row_count": len(
                    behavior_archive.rows
                ),
                "maximum_nonexhaustive_pair_budget_per_arm": (
                    decision.get(
                        "maximum_nonexhaustive_pair_budget_per_arm",
                        decision.get(
                            "maximum_supported_paired_budget_per_arm"
                        ),
                    )
                ),
                "reference_paired_qualification_supply_ready": (
                    decision[
                        "reference_paired_qualification_supply_ready"
                    ]
                ),
            }
        )
        _write_json(design_path, design)
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

    decision_path = _write_json(
        output_root / "supply_decision.json",
        decision,
    )
    artifact_paths.append(decision_path)
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
        **{
            f"additional_exact_archive_{index:03d}": path
            for index, path in enumerate(
                additional_exact_paths,
                start=1,
            )
        },
    }
    if not bool(getattr(args, "static_only", False)):
        input_paths.update(
            {
                "historical_behavior_archive": (
                    args.historical_behavior_archive.resolve()
                ),
                "split_manifest": args.split_manifest.resolve(),
                **{
                    (
                        "additional_behavior_archive_"
                        f"{index:03d}"
                    ): path
                    for index, path in enumerate(
                        additional_behavior_paths,
                        start=1,
                    )
                },
            }
        )
    manifest = {
        "schema_version": (
            "cn_minute_static_structural_formula_v4_manifest_v1"
            if formula_v4
            else (
                "cn_minute_static_structural_typed_surface_manifest_v1"
                if typed_surface
                else "cn_minute_static_structural_supply_manifest_v1"
            )
        ),
        "status": status,
        "authorization_id": authorization_id,
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
        "phase3cm_pair_count": 0,
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "large_search_authorized": False,
    }
    manifest["manifest_payload_hash"] = _stable_hash(manifest)
    manifest_path = _write_json(
        output_root / "artifact_manifest.json",
        manifest,
    )
    return {
        "status": status,
        "formula_space_supply": design,
        "supply_decision": decision,
        "manifest": str(manifest_path),
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
    paired_structural_canary = bool(
        getattr(args, "paired_structural_cem_v2_canary", False)
    )
    paired_structural_medium = bool(
        getattr(args, "paired_structural_cem_v2_medium", False)
    )
    paired_structural_supply_medium = bool(
        getattr(
            args,
            "paired_structural_cem_v2_supply_medium",
            False,
        )
    )
    paired_online_typed_grammar = bool(
        getattr(
            args,
            "paired_online_typed_grammar_cem_v1",
            False,
        )
    )
    if sum(
        (
            paired_structural_canary,
            paired_structural_medium,
            paired_structural_supply_medium,
            paired_online_typed_grammar,
        )
    ) > 1:
        raise ValueError("STRUCTURAL_CEM_V2_MODE_AMBIGUOUS")
    paired_structural_v2 = (
        paired_structural_canary
        or paired_structural_medium
        or paired_structural_supply_medium
        or paired_online_typed_grammar
    )
    search_formula_space_id = (
        STRUCTURAL_ONLINE_TYPED_GRAMMAR_SPACE_ID
        if paired_online_typed_grammar
        else STRUCTURAL_SUPPLY_FORMULA_SPACE_ID
        if paired_structural_supply_medium
        else EXPANDED_FORMULA_SPACE_ID
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
        PAIRED_ONLINE_TYPED_GRAMMAR_CHECKPOINT_COUNT
        if paired_online_typed_grammar
        else PAIRED_STRUCTURAL_SUPPLY_MEDIUM_CHECKPOINT_COUNT
        if paired_structural_supply_medium
        else (
            PAIRED_STRUCTURAL_CEM_V2_MEDIUM_CHECKPOINT_COUNT
            if paired_structural_medium
            else (
                PAIRED_STRUCTURAL_CEM_V2_CHECKPOINT_COUNT
                if paired_structural_canary
                else CHECKPOINT_COUNT
            )
        )
    )
    full_pair_cap = (
        PAIRED_ONLINE_TYPED_GRAMMAR_FULL_PAIR_CAP
        if paired_online_typed_grammar
        else PAIRED_STRUCTURAL_SUPPLY_MEDIUM_FULL_PAIR_CAP
        if paired_structural_supply_medium
        else (
            PAIRED_STRUCTURAL_CEM_V2_MEDIUM_FULL_PAIR_CAP
            if paired_structural_medium
            else (
                PAIRED_STRUCTURAL_CEM_V2_FULL_PAIR_CAP
                if paired_structural_canary
                else FULL_PAIR_CAP
            )
        )
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
    structural_supply_reuse_path = None
    structural_supply_reuse: dict[str, Any] | None = None
    if paired_structural_supply_medium:
        structural_supply_reuse = _verify_reused_structural_supply(
            args.reused_structural_supply_root,
            required_pair_budget_per_arm=(
                checkpoint_count * full_pair_cap
            ),
        )
        structural_supply_reuse_path = _write_json(
            output_root / "structural_supply_reuse_receipt.json",
            structural_supply_reuse,
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
        output_root
        / (
            "decision_catalog_online_typed_grammar.json"
            if paired_online_typed_grammar
            else "decision_catalog_structural_supply.json"
            if paired_structural_supply_medium
            else "decision_catalog_expanded.json"
        ),
        (
            projection.adaptive_decision_catalog(
                search_formula_space_id
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
    reused_canary_receipt_path = None
    reused_canary_receipt: dict[str, Any] | None = None
    prior_canary_exact: set[str] = set()
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
        if paired_structural_medium:
            (
                prior_canary_exact,
                reused_canary_receipt,
            ) = _verify_reused_structural_canary(
                args.reused_structural_canary_root
            )
            reused_canary_receipt_path = _write_json(
                output_root / "structural_canary_reuse_receipt.json",
                reused_canary_receipt,
            )
    else:
        session_sample = _session_sample_contract(
            split, seed=FINANCIAL_SEED + 77
        )
    session_sample_path = _write_json(
        output_root / "session_sample_contract.json",
        session_sample,
    )
    additional_exact_paths = tuple(
        Path(path).resolve()
        for path in (
            getattr(args, "additional_exact_archive", ()) or ()
        )
    )
    exact_archive_paths = (
        args.historical_exact_archive.resolve(),
        args.source_candidate_ledger.resolve(),
        *additional_exact_paths,
    )
    initial_exact = _historical_exact(*exact_archive_paths)
    initial_exact.update(prior_canary_exact)
    additional_behavior_paths = tuple(
        Path(path).resolve()
        for path in (
            getattr(args, "additional_behavior_archive", ()) or ()
        )
    )
    behavior_archive_paths = (
        args.historical_behavior_archive.resolve(),
        *additional_behavior_paths,
    )
    initial_behavior = _combined_behavior_archive(
        *behavior_archive_paths
    )
    structural_supply_audit_path = None
    structural_supply_audit: dict[str, Any] | None = None
    if paired_structural_supply_medium:
        assert structural_supply_reuse is not None
        current_exact_hashes = sorted(
            _sha256(path) for path in exact_archive_paths
        )
        current_behavior_hashes = sorted(
            _sha256(path) for path in behavior_archive_paths
        )
        if (
            current_exact_hashes
            != structural_supply_reuse["exact_archive_sha256s"]
            or current_behavior_hashes
            != structural_supply_reuse["behavior_archive_sha256s"]
        ):
            raise RuntimeError(
                "STRUCTURAL_SUPPLY_CUMULATIVE_MEMORY_HASH_DRIFT"
            )
        structural_supply_audit = _structural_supply_generation_audit(
            projection,
            initial_exact=initial_exact,
            expected_post_archive_exact_supply=int(
                structural_supply_reuse["post_archive_exact_supply"]
            ),
            expected_post_archive_rows_by_production=dict(
                structural_supply_reuse[
                    "post_archive_rows_by_production"
                ]
            ),
        )
        structural_supply_audit_path = _write_json(
            output_root / "structural_supply_generation_audit.json",
            structural_supply_audit,
        )
        if structural_supply_audit["status"] != "PASS":
            raise RuntimeError(
                "STRUCTURAL_SUPPLY_GENERATION_AUDIT_FAILED"
            )
    frozen_contract = {
        "schema_version": (
            "cn_minute_static_production_cem_v3_financial_contract_v1"
        ),
        "status": "FROZEN_EXECUTABLE",
        "authorization_id": (
            "MINUTE_STATIC_ONLINE_TYPED_GRAMMAR_CEM_V1_PAIRED"
            if paired_online_typed_grammar
            else "MINUTE_STATIC_STRUCTURAL_SUPPLY_CEM_V2_PAIRED_MEDIUM"
            if paired_structural_supply_medium
            else "MINUTE_STATIC_STRUCTURAL_CEM_V2_PAIRED_MEDIUM"
            if paired_structural_medium
            else "MINUTE_STATIC_STRUCTURAL_CEM_V2_PAIRED_CANARY"
            if paired_structural_canary
            else AUTHORIZATION_ID
        ),
        "route_id": ROUTE_ID,
        "formula_space_id": search_formula_space_id,
        "production_ids": [
            choice.token_id
            for choice in projection.adaptive_decision_specs(
                search_formula_space_id
            )[0].ordered_choices
        ]
        if paired_structural_v2
        else ["field_spread", "normalized_ratio"],
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
            (
                [
                    row.gene_slot
                    for row in projection.adaptive_decision_specs(
                        search_formula_space_id
                    )
                ]
                if paired_online_typed_grammar
                else [
                    "production_id_adaptive",
                    "field_pair_id_uniform_remaining_exact",
                ]
            )
            if paired_structural_v2
            else ["production_id", "field_pair_id"]
        ),
        "root_mapping": (
            {
                "value": "CSRank",
                "status": "FROZEN_NOT_AN_ADAPTIVE_DECISION_IN_V5",
            }
            if paired_online_typed_grammar
            else None
        ),
        "left_right_independent_probabilities": "FORBIDDEN",
        "paired_common_random_stream": paired_structural_v2,
        "prior_canary_exact_memory_count": len(prior_canary_exact),
        "cumulative_exact_memory_count": len(initial_exact),
        "cumulative_behavior_memory_row_count": len(
            initial_behavior.rows
        ),
        "prior_canary_source_manifest_sha256": (
            reused_canary_receipt["source_manifest_sha256"]
            if reused_canary_receipt is not None
            else None
        ),
        "prior_optimizer_probabilities_imported": False,
        "prior_reward_observations_imported": False,
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
            projection,
            seed=FINANCIAL_SEED + 900_001,
            count=full_pair_cap,
            formula_space_id=search_formula_space_id,
            exact_seen=initial_exact,
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
                FINANCIAL_SEED
                + PAIRED_ONLINE_TYPED_GRAMMAR_SEED_OFFSET
                if paired_online_typed_grammar
                else FINANCIAL_SEED
                + PAIRED_STRUCTURAL_SUPPLY_MEDIUM_SEED_OFFSET
                if paired_structural_supply_medium
                else FINANCIAL_SEED
                + PAIRED_STRUCTURAL_CEM_V2_MEDIUM_SEED_OFFSET
                if paired_structural_medium
                else FINANCIAL_SEED
                + PAIRED_STRUCTURAL_CEM_V2_CANARY_SEED_OFFSET
                if paired_structural_canary
                else FINANCIAL_SEED + 10_000 * (arm_index + 1)
            )
        )
        if rng_state is not None:
            rng.bit_generator.state = copy.deepcopy(rng_state)
        if paired_structural_v2 and arm == "arm_c_structural_cem_v2":
            decisions = projection.adaptive_decision_specs(
                search_formula_space_id
            )
            policy = (
                RankWeightedCategoricalCEMPolicy.restore(
                    optimizer_state,
                    decisions=decisions,
                    decision_catalog_hash=(
                        projection.adaptive_decision_catalog_hash(
                            search_formula_space_id
                        )
                    ),
                    formula_space_id=search_formula_space_id,
                    rng=rng,
                )
                if optimizer_state is not None
                else RankWeightedCategoricalCEMPolicy.fresh(
                    decisions=decisions,
                    decision_catalog_hash=(
                        projection.adaptive_decision_catalog_hash(
                            search_formula_space_id
                        )
                    ),
                    formula_space_id=search_formula_space_id,
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
                formula_space_id=(
                    search_formula_space_id
                    if paired_structural_v2
                    else None
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
            checkpoint_count=checkpoint_count,
        )
        for arm in campaign_arms
    }
    if (
        paired_structural_medium
        or paired_structural_supply_medium
        or paired_online_typed_grammar
    ):
        comparison = _paired_structural_medium_verdict(
            output_root,
            arm_metrics,
            prior_canary_exact=(
                initial_exact
                if paired_structural_supply_medium
                else prior_canary_exact
            ),
            checkpoint_count=checkpoint_count,
            full_pair_cap=full_pair_cap,
            schema_version=(
                "cn_minute_static_online_typed_grammar_cem_v1_verdict"
                if paired_online_typed_grammar
                else "cn_minute_static_structural_supply_cem_v2_"
                "medium_verdict_v1"
                if paired_structural_supply_medium
                else "cn_minute_static_structural_cem_v2_"
                "medium_verdict_v1"
            ),
        )
        qualified = (
            comparison["status"]
            == "STRUCTURAL_CEM_V2_FINANCIALLY_QUALIFIED"
        )
        final = {
            "SAMPLED_PHASE3CM_AUTHORITY": (
                "REUSED_ACTIVE_ROUTE_LOCAL_SELECTION_AUTHORITY"
            ),
            (
                "ONLINE_TYPED_GRAMMAR_CEM_V1"
                if paired_online_typed_grammar
                else "STRUCTURAL_SUPPLY_CEM_V2_MEDIUM"
                if paired_structural_supply_medium
                else "STRUCTURAL_CEM_V2_MEDIUM"
            ): comparison["status"],
            "FORMULA_SPACE_ID": search_formula_space_id,
            "PAIR_BUDGET_PER_ARM": checkpoint_count * full_pair_cap,
            "CEM_SEARCH_INCREMENT": (
                "QUALIFIED" if qualified else "NOT_QUALIFIED"
            ),
            "PERFORMANCE_CONTRACT": (
                "PASS"
                if all(comparison["performance_checks"].values())
                else "FAIL"
            ),
            "TARGET_FAMILY_LARGE_SEARCH_READINESS": (
                "READY_FOR_SEPARATE_LARGE_SEARCH_AUTHORIZATION"
                if qualified
                else "SEARCH_POLICY_BLOCKED"
            ),
            "LARGE_SEARCH_INITIALIZATION": (
                "FRESH_UNIFORM_REQUIRED_AFTER_SEPARATE_AUTHORIZATION"
            ),
            "READINESS_BLOCKERS": (
                []
                if qualified
                else [
                    name
                    for group in (
                        "mechanical_contracts",
                        "financial_checks",
                        "performance_checks",
                    )
                    for name, passed in comparison[group].items()
                    if not passed
                ]
            ),
        }
    elif paired_structural_canary:
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
            "structural_supply": structural_supply_reuse,
            "structural_supply_generation_audit": (
                structural_supply_audit
            ),
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
        *(
            [reused_canary_receipt_path]
            if reused_canary_receipt_path is not None
            else []
        ),
        *(
            [structural_supply_reuse_path]
            if structural_supply_reuse_path is not None
            else []
        ),
        *(
            [structural_supply_audit_path]
            if structural_supply_audit_path is not None
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
            "cn_minute_static_online_typed_grammar_cem_v1_manifest_v1"
            if paired_online_typed_grammar
            else "cn_minute_static_structural_supply_cem_v2_medium_manifest_v1"
            if paired_structural_supply_medium
            else "cn_minute_static_structural_cem_v2_medium_manifest_v1"
            if paired_structural_medium
            else "cn_minute_static_structural_cem_v2_canary_manifest_v1"
            if paired_structural_canary
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
    parser.add_argument(
        "--structural-supply-design",
        action="store_true",
    )
    parser.add_argument(
        "--structural-typed-surface-audit",
        action="store_true",
    )
    parser.add_argument(
        "--structural-formula-v4-supply",
        action="store_true",
    )
    parser.add_argument(
        "--additional-exact-archive",
        type=Path,
        action="append",
        default=[],
    )
    parser.add_argument(
        "--additional-behavior-archive",
        type=Path,
        action="append",
        default=[],
    )
    parser.add_argument("--continue-financial", action="store_true")
    parser.add_argument(
        "--paired-structural-cem-v2-canary",
        action="store_true",
    )
    parser.add_argument(
        "--paired-structural-cem-v2-medium",
        action="store_true",
    )
    parser.add_argument(
        "--paired-structural-cem-v2-supply-medium",
        action="store_true",
    )
    parser.add_argument(
        "--paired-online-typed-grammar-cem-v1",
        action="store_true",
    )
    parser.add_argument("--reused-sampled-authority-root", type=Path)
    parser.add_argument("--reused-structural-canary-root", type=Path)
    parser.add_argument("--reused-structural-supply-root", type=Path)
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
    paired_modes = (
        args.paired_structural_cem_v2_canary,
        args.paired_structural_cem_v2_medium,
        args.paired_structural_cem_v2_supply_medium,
        args.paired_online_typed_grammar_cem_v1,
    )
    if sum(map(bool, paired_modes)) > 1:
        parser.error(
            "choose exactly one structural CEM V2 paired mode"
        )
    supply_design_modes = (
        args.structural_supply_design,
        args.structural_typed_surface_audit,
        args.structural_formula_v4_supply,
    )
    if sum(map(bool, supply_design_modes)) > 1:
        parser.error(
            "choose exactly one structural supply design mode"
        )
    if args.structural_supply_design and (
        args.structural_typed_surface_audit
        or args.structural_formula_v4_supply
        or args.continue_financial
        or args.paired_structural_cem_v2_canary
        or args.paired_structural_cem_v2_medium
        or args.paired_structural_cem_v2_supply_medium
        or args.paired_online_typed_grammar_cem_v1
    ):
        parser.error(
            "--structural-supply-design cannot run another mode"
        )
    if args.structural_typed_surface_audit and (
        args.structural_formula_v4_supply
        or args.continue_financial
        or args.paired_structural_cem_v2_canary
        or args.paired_structural_cem_v2_medium
        or args.paired_structural_cem_v2_supply_medium
        or args.paired_online_typed_grammar_cem_v1
    ):
        parser.error(
            "--structural-typed-surface-audit cannot run another mode"
        )
    if args.structural_formula_v4_supply and (
        args.continue_financial
        or args.paired_structural_cem_v2_canary
        or args.paired_structural_cem_v2_medium
        or args.paired_structural_cem_v2_supply_medium
        or args.paired_online_typed_grammar_cem_v1
    ):
        parser.error(
            "--structural-formula-v4-supply cannot run a financial mode"
        )
    if (
        args.structural_typed_surface_audit
        and not args.static_only
    ):
        parser.error(
            "--structural-typed-surface-audit requires --static-only"
        )
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
            (
                args.paired_structural_cem_v2_canary
                or args.paired_structural_cem_v2_medium
                or args.paired_structural_cem_v2_supply_medium
                or args.paired_online_typed_grammar_cem_v1
            )
            and args.reused_sampled_authority_root is None
        ):
            parser.error(
                "paired typed CEM requires "
                "--reused-sampled-authority-root"
            )
        if (
            args.paired_structural_cem_v2_medium
            and args.reused_structural_canary_root is None
        ):
            parser.error(
                "--paired-structural-cem-v2-medium requires "
                "--reused-structural-canary-root"
            )
        if args.paired_structural_cem_v2_supply_medium:
            if args.reused_structural_supply_root is None:
                parser.error(
                    "--paired-structural-cem-v2-supply-medium requires "
                    "--reused-structural-supply-root"
                )
            if (
                not args.additional_exact_archive
                or not args.additional_behavior_archive
            ):
                parser.error(
                    "--paired-structural-cem-v2-supply-medium requires "
                    "cumulative --additional-exact-archive and "
                    "--additional-behavior-archive inputs"
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
        run_structural_supply_design(args)
        if (
            args.structural_supply_design
            or args.structural_typed_surface_audit
            or args.structural_formula_v4_supply
        )
        else run_financial(args)
        if args.continue_financial
        else run(args)
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
