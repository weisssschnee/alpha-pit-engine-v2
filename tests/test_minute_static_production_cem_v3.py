from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.run_minute_static_production_cem_v3 import (
    MINIMUM_BEHAVIOR_SUPPLY,
    MINIMUM_EXACT_SUPPLY,
    PAIRED_STRUCTURAL_CEM_V2_ARMS,
    PAIRED_STRUCTURAL_CEM_V2_MEDIUM_CHECKPOINT_COUNT,
    PAIRED_STRUCTURAL_CEM_V2_MEDIUM_FULL_PAIR_CAP,
    PAIRED_STRUCTURAL_SUPPLY_MEDIUM_CHECKPOINT_COUNT,
    PAIRED_STRUCTURAL_SUPPLY_MEDIUM_FULL_PAIR_CAP,
    STRUCTURAL_FORMULA_V4_MINIMUM_FRESH_EXACT,
    STRUCTURAL_FORMULA_V4_SPACE_ID,
    STRUCTURAL_SUPPLY_FORMULA_SPACE_ID,
    STRUCTURAL_SUPPLY_PRODUCTION_IDS,
    STRUCTURAL_TYPED_FORMULA_SPACE_ID,
    MinuteStaticProductionProjection,
    _expression_depth,
    _failure_decision,
    _load_production_contract,
    _nonexhaustive_supply_decision,
    _paired_structural_canary_verdict,
    _paired_structural_medium_verdict,
    _production_parity,
    _session_sample_contract,
    _structural_supply_generation_audit,
    _structural_formula_v4_mechanical_proof,
    _structural_typed_surface_mechanical_proof,
    _structural_v2_fresh_stream_parity,
    run,
    run_financial,
    run_structural_supply_design,
)
from our_system_phase2.services.categorical_cem import (
    RankWeightedCategoricalCEMPolicy,
)
from our_system_phase2.services.fixed_split_authority import (
    FixedSplitAuthority,
)
from our_system_phase2.services.phase3cm_streaming_expression import (
    unsupported_streaming_operators,
)
from our_system_phase2.services.search_choice_policy import (
    AvailableUniformPolicy,
    EXPANDED_FORMULA_SPACE_ID,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    RegistryDrivenGenerator,
)
from scripts.run_cn_phase3cm_streaming_qualification import (
    _sha256 as _streaming_sha256,
    _session_sample_calendar,
)
from scripts.run_targeted_formula_cem_qualification import (
    _generate_checkpoint_pool,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime"
    / "field_registry"
    / "cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
PRODUCTION_CONTRACT = (
    REPO
    / "runtime"
    / "run_plans"
    / "cn_minute_static_production_roots_v1.json"
)
SPLIT = (
    REPO
    / "runtime"
    / "run_plans"
    / "phase3ga_true1min_2024_2025_global_split_manifest.csv"
)


def test_supply_capacity_hard_blocker_stops_all_downstream_work() -> None:
    final = _failure_decision(
        "OLD_POST_ARCHIVE_EXACT_SUPPLY_30_BELOW_72"
    )
    assert final == {
        "SAMPLED_PHASE3CM_AUTHORITY": (
            "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER"
        ),
        "FORMULA_SPACE_INCREMENT": (
            "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER"
        ),
        "CEM_SEARCH_INCREMENT": "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER",
        "PERFORMANCE_CONTRACT": "NOT_RUN_OLD_SUPPLY_HARD_BLOCKER",
        "TARGET_FAMILY_LARGE_SEARCH_READINESS": "SUPPLY_BLOCKED",
        "READINESS_BLOCKERS": [
            "OLD_POST_ARCHIVE_EXACT_SUPPLY_30_BELOW_72"
        ],
    }


def test_real_production_roots_form_110_old_pairs_and_pass_exact_gate(
    tmp_path: Path,
) -> None:
    contract = json.loads(
        PRODUCTION_CONTRACT.read_text(encoding="utf-8")
    )
    route_roots = list(map(str, contract["route_roots"]))
    assert len(route_roots) == 11
    assert MINIMUM_EXACT_SUPPLY == 72
    assert MINIMUM_BEHAVIOR_SUPPLY == 48

    layout = tmp_path / "active_layout.json"
    layout.write_text(
        json.dumps({"fields": route_roots}),
        encoding="utf-8",
    )
    archive = tmp_path / "archive.parquet"
    ledger = tmp_path / "ledger.parquet"
    pq.write_table(
        pa.table({"exact_identity": ["historical-a"]}),
        archive,
    )
    pq.write_table(
        pa.table({"exact_identity": ["historical-b"]}),
        ledger,
    )
    output_root = tmp_path / "result"
    result = run(
        argparse.Namespace(
            registry=REGISTRY,
            production_root_contract=PRODUCTION_CONTRACT,
            active_layout=layout,
            historical_exact_archive=archive,
            source_candidate_ledger=ledger,
            historical_behavior_archive=None,
            split_manifest=None,
            compute_threads=2,
            output_root=output_root,
            repo_sha="test-sha",
            task_id="test-task",
            allow_noncanonical_host=True,
            static_only=True,
        )
    )

    gate = result["old_supply_gate"]
    assert gate["production_root_count"] == 11
    assert gate["atomic_ordered_field_pair_count"] == 110
    assert gate["legal_pairs"] == 110
    assert gate["exact_unique_pairs"] == 110
    assert gate["post_archive_exact_supply"] == 110
    assert gate["exact_gate"] == "PASS"
    assert gate["behavior_gate"] == "PENDING"
    assert result["status"] == (
        "EXACT_SUPPLY_QUALIFIED_BEHAVIOR_PENDING_STATIC_ONLY"
    )

    candidates = pq.read_table(
        output_root / "old_post_archive_candidates.parquet"
    )
    assert candidates.num_rows == 220
    manifest = json.loads(
        (output_root / "artifact_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["behavior_probe_count"] == 0
    assert manifest["phase3cm_pair_count"] == 0
    assert manifest["campaign_arm_count"] == 0
    assert manifest["validation_reads"] == 0
    assert manifest["holdout_reads"] == 0
    assert manifest["forward_2026_reads"] == 0
    assert {path.name for path in output_root.iterdir()} == {
        "artifact_manifest.json",
        "disclosure_v2_dispositions.json",
        "old_post_archive_candidates.parquet",
        "old_supply_gate.json",
        "supply_decision.json",
    }


def test_minute_production_projection_replays_existing_grammar() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    _, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )
    parity = _production_parity(projection)
    assert parity["status"] == "PASS"
    assert parity["checked_projection_rows"] == 330
    assert parity["failure_count"] == 0


def test_structural_supply_space_reuses_only_qualified_pair_lanes() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    _, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            enforce_route_compatibility=True,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )

    decisions = projection.structural_decision_specs(
        STRUCTURAL_SUPPLY_FORMULA_SPACE_ID
    )
    assert [
        row.token_id for row in decisions[0].ordered_choices
    ] == list(STRUCTURAL_SUPPLY_PRODUCTION_IDS)
    catalog = projection._available_candidate_catalog(
        STRUCTURAL_SUPPLY_FORMULA_SPACE_ID
    )
    assert len(catalog) == 440
    assert {
        str(row["production_id"]) for row in catalog
    } == set(STRUCTURAL_SUPPLY_PRODUCTION_IDS)
    assert len(
        {
            str(row["candidate"]["exact_identity"])
            for row in catalog
        }
    ) == 440
    assert all(
        bool(row["candidate"]["legal"])
        and bool(row["control"]["legal"])
        for row in catalog
    )


def test_structural_typed_surface_is_lossless_authoritative_projection() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    _, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            enforce_route_compatibility=True,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )

    decisions = projection.structural_typed_decision_specs(
        STRUCTURAL_TYPED_FORMULA_SPACE_ID
    )
    assert [row.gene_slot for row in decisions] == [
        "production_id",
        "left_field_id",
        "right_field_id",
    ]
    assert [len(row.ordered_choices) for row in decisions] == [4, 11, 11]
    left_fields = {
        pair_id.split("::", 1)[0]
        for pair_id in projection.field_pair_ids
    }
    right_fields = {
        pair_id.split("::", 1)[1]
        for pair_id in projection.field_pair_ids
    }
    assert {
        str(choice.gene_value)
        for choice in decisions[1].ordered_choices
    } == left_fields
    assert {
        str(choice.gene_value)
        for choice in decisions[2].ordered_choices
    } == right_fields

    decision_catalog = projection.structural_typed_decision_catalog(
        STRUCTURAL_TYPED_FORMULA_SPACE_ID
    )
    assert decision_catalog["field_family_authority"] == (
        "NOT_DECLARED_NO_INVENTED_GROUPS"
    )
    assert decision_catalog["joint_exact_availability_mask"] == (
        "APPLIED_BEFORE_EACH_HIERARCHICAL_DRAW"
    )
    catalog = projection._available_candidate_catalog(
        STRUCTURAL_TYPED_FORMULA_SPACE_ID
    )
    assert len(catalog) == 440
    assert len(
        {
            str(row["candidate"]["exact_identity"])
            for row in catalog
        }
    ) == 440
    assert all(
        bool(row["candidate"]["legal"])
        and bool(row["control"]["legal"])
        for row in catalog
    )


def test_formula_v4_is_bounded_typed_unique_and_adaptive() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    _, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            enforce_route_compatibility=True,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )

    decisions = projection.structural_typed_decision_specs(
        STRUCTURAL_FORMULA_V4_SPACE_ID
    )
    assert [row.gene_slot for row in decisions] == [
        "production_id",
        "left_transform_id",
        "right_transform_id",
        "left_field_id",
        "right_field_id",
    ]
    assert [len(row.ordered_choices) for row in decisions] == [
        4,
        3,
        3,
        11,
        11,
    ]
    assert projection.v4_transform_pairs == {
        "field_spread": (
            ("ZSCORE", "ZSCORE"),
            ("ZSCORE", "SIGN"),
            ("SIGN", "ZSCORE"),
            ("SIGN", "SIGN"),
        ),
        "normalized_ratio": (
            ("ZSCORE", "ZSCORE"),
            ("ZSCORE", "ABS_ZSCORE"),
            ("ZSCORE", "SIGN"),
            ("ABS_ZSCORE", "ZSCORE"),
            ("ABS_ZSCORE", "ABS_ZSCORE"),
            ("ABS_ZSCORE", "SIGN"),
            ("SIGN", "ZSCORE"),
            ("SIGN", "ABS_ZSCORE"),
            ("SIGN", "SIGN"),
        ),
        "absolute_state_interaction": (
            ("SIGN", "ABS_ZSCORE"),
            ("ABS_ZSCORE", "SIGN"),
        ),
        "dispersion_interaction": (
            ("ABS_ZSCORE", "ABS_ZSCORE"),
            ("ABS_ZSCORE", "ZSCORE"),
            ("ZSCORE", "ABS_ZSCORE"),
        ),
    }
    catalog = projection._available_candidate_catalog(
        STRUCTURAL_FORMULA_V4_SPACE_ID
    )
    assert len(catalog) == 1_980
    assert {
        production_id: sum(
            str(row["production_id"]) == production_id
            for row in catalog
        )
        for production_id in STRUCTURAL_SUPPLY_PRODUCTION_IDS
    } == {
        "field_spread": 440,
        "normalized_ratio": 990,
        "absolute_state_interaction": 220,
        "dispersion_interaction": 330,
    }
    assert len(
        {
            str(row["candidate"]["exact_identity"])
            for row in catalog
        }
    ) == len(catalog)
    assert len(
        {
            str(row["candidate"]["canonical_identity"])
            for row in catalog
        }
    ) == len(catalog)
    assert all(
        bool(row["candidate"]["legal"])
        and bool(row["control"]["legal"])
        and set(row["candidate"]["field_ids"])
        == set(row["control"]["field_ids"])
        and _expression_depth(row["candidate"]["expression"])
        <= int(row["candidate"]["maximum_depth"])
        and _expression_depth(row["control"]["expression"])
        <= int(row["control"]["maximum_depth"])
        for row in catalog
    )
    assert not unsupported_streaming_operators(
        str(member["expression"])
        for row in catalog
        for member in (row["candidate"], row["control"])
    )
    example = next(
        row
        for row in catalog
        if row["production_id"] == "field_spread"
        and row["left_transform_id"] == "SIGN"
        and row["right_transform_id"] == "ZSCORE"
        and row["field_pair_id"] == "amount::volume"
    )
    assert example["candidate"]["expression"] == (
        "CSRank(Sub(Sign($amount),ZScore($volume)))"
    )
    assert example["control"]["expression"] == (
        "CSRank(Add(Sign($amount),Mul(0,$volume)))"
    )
    assert "Delta(" not in example["candidate"]["expression"]
    proof = _structural_formula_v4_mechanical_proof(
        projection,
        seed=2026072423,
    )
    assert proof["status"] == "PASS"
    assert proof["first_generation_exact_stream_parity"] is True
    assert proof["cumulative_duplicate_count"] == 0
    assert proof["second_generation_stream_changed"] is True
    assert proof["updated_context_count"] == 5


def test_minute_static_fields_and_formula_templates_match_authority() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    contract, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    assert contract["registry_hash"] == registry.registry_hash
    assert roots == (
        "amount",
        "amount_yuan",
        "close",
        "high",
        "intraday_ret_from_open",
        "low",
        "open",
        "pct_chg",
        "ret_1m",
        "volume",
        "vwap",
    )
    assert len(set(roots)) == 11
    assert set(roots).issubset(
        {row.field_id for row in registry.fields_for_route("MINUTE_STATIC")}
    )
    for field_id in roots:
        field = registry.resolve(field_id)
        assert field.source_family == "raw_1min"
        assert field.source_table == "true1min_augmented_panel"
        assert field.source_field == field_id
        assert field.entity_scope == "STOCK"
        assert field.temporal_semantics == "BAR_VALUE"
        assert field.observable_clock == "bar_close"
        assert field.maturity_rule == "0 bars"
        assert field.pit_status == "PIT_VERSIONED_HISTORY"
        assert field.search_eligible is True
        assert field.semantic_role == "primary"
        assert field.field_role == "primary"
        assert field.support_unit == "stock-minute cross-section"
        assert field.blocked_reason == ""
        assert field.source_lag == 0
        assert field.source_lag_unit == "bars"
        assert field.metadata["dtype"] == "float64"
        assert field.metadata["external_phase2_join"] is False
        assert field.metadata["transform"] == "identity"

    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            enforce_route_compatibility=True,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )
    expected_pairs = {
        f"{left}::{right}"
        for left in roots
        for right in roots
        if left != right
    }
    assert set(projection.field_pair_ids) == expected_pairs
    assert len(projection.field_pair_ids) == 110

    expression_templates = {
        "field_spread": (
            "CSRank(Sub(ZScore(${left}),ZScore(${right})))"
        ),
        "normalized_ratio": (
            "CSRank(SafeDiv(ZScore(${left}),ZScore(${right}),0.05))"
        ),
        "absolute_state_interaction": (
            "CSRank(Mul(Sign(${left}),Abs(ZScore(${right}))))"
        ),
        "dispersion_interaction": (
            "CSRank(Sub(Abs(ZScore(${left})),"
            "Abs(ZScore(${right}))))"
        ),
    }
    control_template = (
        "CSRank(Add(ZScore(${left}),Mul(0,ZScore(${right}))))"
    )
    catalog = projection._available_candidate_catalog(
        STRUCTURAL_TYPED_FORMULA_SPACE_ID
    )
    production_ids = [str(row["production_id"]) for row in catalog]
    assert set(production_ids) == set(expression_templates)
    assert {
        production_id: production_ids.count(production_id)
        for production_id in expression_templates
    } == {
        production_id: 110
        for production_id in expression_templates
    }
    primary_exact = set()
    control_exact = set()
    for row in catalog:
        left, right = str(row["field_pair_id"]).split("::", 1)
        candidate = row["candidate"]
        control = row["control"]
        expected_fields = [left, right]
        canonical_fields = sorted(expected_fields)
        expected_source_fields = [
            registry.resolve(field_id).source_field_id
            for field_id in canonical_fields
        ]
        expected_expression = expression_templates[
            str(row["production_id"])
        ].format(left=left, right=right)
        expected_control = control_template.format(
            left=left,
            right=right,
        )
        assert candidate["expression"] == expected_expression
        assert candidate["canonical_expression"] == expected_expression
        assert control["expression"] == expected_control
        assert control["canonical_expression"] == expected_control
        assert candidate["field_ids"] == canonical_fields
        assert control["field_ids"] == canonical_fields
        assert candidate["declared_field_ids"] == expected_fields
        assert control["declared_field_ids"] == expected_fields
        assert candidate["source_field_ids"] == expected_source_fields
        assert control["source_field_ids"] == expected_source_fields
        assert candidate["categorical_genes"]["field_pair_id"] == (
            row["field_pair_id"]
        )
        assert control["categorical_genes"] == (
            candidate["categorical_genes"]
        )
        assert candidate["pair_id"] == control["pair_id"]
        assert candidate["matched_control_id"] == control["candidate_id"]
        assert control["matched_control_id"] == candidate["candidate_id"]
        assert candidate["access_roles"] == ["development"]
        assert control["access_roles"] == ["development"]
        assert candidate["uses_future_revision"] is False
        assert control["uses_future_revision"] is False
        assert candidate["requires_intrabar_order"] is False
        assert control["requires_intrabar_order"] is False
        assert candidate["clock_contract"] == "bar_close"
        assert control["clock_contract"] == "bar_close"
        assert candidate["maturity_contract"] == "bar_close"
        assert control["maturity_contract"] == "bar_close"
        assert candidate["typed_route_decision"] == "ALLOW"
        assert control["typed_route_decision"] == "ALLOW"
        assert candidate["typed_route_rejection_code"] == ""
        assert control["typed_route_rejection_code"] == ""
        assert candidate["legal"] is True
        assert control["legal"] is True
        primary_exact.add(str(candidate["exact_identity"]))
        control_exact.add(str(control["exact_identity"]))

    assert len(catalog) == 440
    assert len(primary_exact) == 440
    # Each production ablates to the same pair-specific baseline by design.
    assert len(control_exact) == 110


def test_structural_typed_surface_parity_mask_and_synthetic_adaptation() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    _, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            enforce_route_compatibility=True,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )

    proof = _structural_typed_surface_mechanical_proof(
        projection,
        seed=2026072511,
        generation_size=32,
    )
    assert proof["status"] == "PASS"
    assert proof["first_generation_exact_stream_parity"] is True
    assert proof["first_generation_duplicate_count"] == 0
    assert proof["second_generation_stream_changed"] is True
    assert proof["second_generation_duplicate_count"] == 0
    assert proof["field_only_causal_production_stream_parity"] is True
    assert proof["added_field_contexts_change_proposals"] is True
    assert proof["field_only_updated_context_count"] == 2
    assert proof["updated_context_count"] == 3
    assert {
        row["decision_id"]
        for row in proof["support_diagnostics"]
        if row["support_status"] == "UPDATED"
    } == {
        "minute_static.production_id",
        "minute_static.left_field_id",
        "minute_static.right_field_id",
    }
    assert proof["financial_reads"] == 0
    assert proof["phase3cm_pair_count"] == 0
    assert proof["validation_reads"] == 0
    assert proof["holdout_reads"] == 0
    assert proof["forward_2026_reads"] == 0


def test_structural_supply_generation_audit_matches_cumulative_memory() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    _, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            enforce_route_compatibility=True,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )
    catalog = projection._available_candidate_catalog(
        STRUCTURAL_SUPPLY_FORMULA_SPACE_ID
    )
    by_production = {
        production_id: [
            row
            for row in catalog
            if row["production_id"] == production_id
        ]
        for production_id in STRUCTURAL_SUPPLY_PRODUCTION_IDS
    }
    initial_exact = {
        str(row["candidate"]["exact_identity"])
        for production_id in ("field_spread", "normalized_ratio")
        for row in by_production[production_id]
    }
    initial_exact.update(
        str(row["candidate"]["exact_identity"])
        for row in by_production["absolute_state_interaction"][:32]
    )
    initial_exact.update(
        str(row["candidate"]["exact_identity"])
        for row in by_production["dispersion_interaction"][:31]
    )
    expected = {
        "field_spread": 0,
        "normalized_ratio": 0,
        "absolute_state_interaction": 78,
        "dispersion_interaction": 79,
    }
    audit = _structural_supply_generation_audit(
        projection,
        initial_exact=initial_exact,
        expected_post_archive_exact_supply=157,
        expected_post_archive_rows_by_production=expected,
    )
    assert audit["status"] == "PASS"
    assert audit["authoritative_field_pair_domain_count"] == 110
    assert audit["post_memory_exact_by_production"] == expected
    assert PAIRED_STRUCTURAL_SUPPLY_MEDIUM_CHECKPOINT_COUNT == 4
    assert PAIRED_STRUCTURAL_SUPPLY_MEDIUM_FULL_PAIR_CAP == 17

    parity = _structural_v2_fresh_stream_parity(
        projection,
        seed=2026072504,
        count=PAIRED_STRUCTURAL_SUPPLY_MEDIUM_FULL_PAIR_CAP,
        formula_space_id=STRUCTURAL_SUPPLY_FORMULA_SPACE_ID,
        exact_seen=initial_exact,
    )
    assert parity["status"] == "PASS"
    assert parity["formula_space_id"] == (
        STRUCTURAL_SUPPLY_FORMULA_SPACE_ID
    )
    proposals, funnel = _generate_checkpoint_pool(
        projection=projection,
        arm="arm_b_uniform_expanded",
        policy=AvailableUniformPolicy(),
        rng=np.random.default_rng(2026072505),
        exact_seen=set(initial_exact),
        checkpoint_id="checkpoint_001",
        behavior_probe_target=(
            PAIRED_STRUCTURAL_SUPPLY_MEDIUM_FULL_PAIR_CAP
        ),
        availability_masked_sampling=True,
        full_pair_cap=PAIRED_STRUCTURAL_SUPPLY_MEDIUM_FULL_PAIR_CAP,
        formula_space_id=STRUCTURAL_SUPPLY_FORMULA_SPACE_ID,
    )
    assert len(proposals) == (
        PAIRED_STRUCTURAL_SUPPLY_MEDIUM_FULL_PAIR_CAP
    )
    assert funnel["exact_duplicate_pairs"] == 0
    assert {
        row["formula_space_id"] for row in proposals
    } == {STRUCTURAL_SUPPLY_FORMULA_SPACE_ID}
    assert {
        row["skeleton_id"].rsplit(".", 1)[-1]
        for row in proposals
    }.issubset(
        {"absolute_state_interaction", "dispersion_interaction"}
    )


def test_structural_supply_decision_reserves_two_x_headroom() -> None:
    qualified = _nonexhaustive_supply_decision(
        post_archive_exact_supply=220,
        observed_behavior_unique_supply=150,
    )
    assert qualified[
        "maximum_nonexhaustive_pair_budget_per_arm"
    ] == 75
    assert qualified[
        "reference_paired_qualification_supply_ready"
    ] is True
    assert qualified["large_search_authorized"] is False

    blocked = _nonexhaustive_supply_decision(
        post_archive_exact_supply=220,
        observed_behavior_unique_supply=143,
    )
    assert blocked[
        "maximum_nonexhaustive_pair_budget_per_arm"
    ] == 71
    assert blocked[
        "reference_paired_qualification_supply_ready"
    ] is False
    assert blocked["next_action"] == (
        "FREEZE_PAIRED_BUDGET_AT_OR_BELOW_SUPPLY_CEILING"
    )


def test_structural_supply_static_run_closes_without_financial_reads(
    tmp_path: Path,
) -> None:
    contract = json.loads(
        PRODUCTION_CONTRACT.read_text(encoding="utf-8")
    )
    layout = tmp_path / "active_layout.json"
    layout.write_text(
        json.dumps({"fields": contract["route_roots"]}),
        encoding="utf-8",
    )
    archive = tmp_path / "archive.parquet"
    ledger = tmp_path / "ledger.parquet"
    pq.write_table(
        pa.table({"exact_identity": ["historical-a"]}),
        archive,
    )
    pq.write_table(
        pa.table({"exact_identity": ["historical-b"]}),
        ledger,
    )
    output_root = tmp_path / "structural_supply"
    result = run_structural_supply_design(
        argparse.Namespace(
            registry=REGISTRY,
            production_root_contract=PRODUCTION_CONTRACT,
            active_layout=layout,
            historical_exact_archive=archive,
            source_candidate_ledger=ledger,
            historical_behavior_archive=None,
            additional_exact_archive=[],
            additional_behavior_archive=[],
            split_manifest=None,
            compute_threads=2,
            output_root=output_root,
            repo_sha="test-sha",
            task_id="test-task",
            allow_noncanonical_host=True,
            static_only=True,
        )
    )

    supply = result["formula_space_supply"]
    assert supply["raw_categorical_rows"] == 440
    assert supply["raw_exact_unique"] == 440
    assert supply["post_archive_exact_supply"] == 440
    assert supply["financial_reads"] == 0
    assert supply["phase3cm_pair_count"] == 0
    assert result["status"] == (
        "STRUCTURAL_EXACT_SUPPLY_CLOSED_BEHAVIOR_PENDING"
    )
    manifest = json.loads(
        (output_root / "artifact_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["financial_reads"] == 0
    assert manifest["large_search_authorized"] is False


def test_structural_typed_static_audit_blocks_financial_when_19_exact_remain(
    tmp_path: Path,
) -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    contract = json.loads(
        PRODUCTION_CONTRACT.read_text(encoding="utf-8")
    )
    roots = tuple(map(str, contract["route_roots"]))
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            enforce_route_compatibility=True,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )
    catalog = projection._available_candidate_catalog(
        STRUCTURAL_TYPED_FORMULA_SPACE_ID
    )
    by_production = {
        production_id: [
            row
            for row in catalog
            if row["production_id"] == production_id
        ]
        for production_id in STRUCTURAL_SUPPLY_PRODUCTION_IDS
    }
    seen = [
        str(row["candidate"]["exact_identity"])
        for production_id in ("field_spread", "normalized_ratio")
        for row in by_production[production_id]
    ]
    seen.extend(
        str(row["candidate"]["exact_identity"])
        for row in by_production["absolute_state_interaction"][:-1]
    )
    seen.extend(
        str(row["candidate"]["exact_identity"])
        for row in by_production["dispersion_interaction"][:-18]
    )
    layout = tmp_path / "active_layout.json"
    layout.write_text(
        json.dumps({"fields": list(roots)}),
        encoding="utf-8",
    )
    archive = tmp_path / "archive.parquet"
    ledger = tmp_path / "ledger.parquet"
    additional = tmp_path / "additional.parquet"
    pq.write_table(
        pa.table({"exact_identity": seen[:200]}),
        archive,
    )
    pq.write_table(
        pa.table({"exact_identity": seen[200:400]}),
        ledger,
    )
    pq.write_table(
        pa.table({"exact_identity": seen[400:]}),
        additional,
    )
    output_root = tmp_path / "typed_surface"
    result = run_structural_supply_design(
        argparse.Namespace(
            registry=REGISTRY,
            production_root_contract=PRODUCTION_CONTRACT,
            active_layout=layout,
            historical_exact_archive=archive,
            source_candidate_ledger=ledger,
            historical_behavior_archive=None,
            additional_exact_archive=[additional],
            additional_behavior_archive=[],
            split_manifest=None,
            compute_threads=2,
            output_root=output_root,
            repo_sha="test-sha",
            task_id="test-task",
            allow_noncanonical_host=True,
            static_only=True,
            structural_typed_surface_audit=True,
        )
    )

    supply = result["formula_space_supply"]
    assert result["status"] == (
        "STRUCTURAL_TYPED_SURFACE_RAW_CATALOG_MECHANICAL_PASS_"
        "FINANCIAL_SUPPLY_BLOCKED"
    )
    assert supply["formula_space_id"] == (
        STRUCTURAL_TYPED_FORMULA_SPACE_ID
    )
    assert supply["post_archive_exact_supply"] == 19
    assert supply["post_archive_rows_by_production"] == {
        "field_spread": 0,
        "normalized_ratio": 0,
        "absolute_state_interaction": 1,
        "dispersion_interaction": 18,
    }
    assert result["supply_decision"][
        "financial_qualification_authorized"
    ] is False
    assert result["supply_decision"]["next_action"] == (
        "EXPAND_AUTHORITATIVE_FORMULA_SUPPLY_BEFORE_FINANCIAL_RETRY"
    )
    assert (output_root / "structural_typed_decision_catalog.json").is_file()
    assert (output_root / "synthetic_adaptation_proof.json").is_file()
    manifest = json.loads(
        (output_root / "artifact_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["financial_reads"] == 0
    assert manifest["phase3cm_pair_count"] == 0


def test_formula_v4_static_supply_retires_old_440_and_keeps_1540_fresh(
    tmp_path: Path,
) -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    _, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            enforce_route_compatibility=True,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )
    old_exact = [
        str(row["candidate"]["exact_identity"])
        for row in projection._available_candidate_catalog(
            STRUCTURAL_TYPED_FORMULA_SPACE_ID
        )
    ]
    layout = tmp_path / "active_layout.json"
    layout.write_text(
        json.dumps({"fields": list(roots)}),
        encoding="utf-8",
    )
    archive = tmp_path / "archive.parquet"
    ledger = tmp_path / "ledger.parquet"
    pq.write_table(
        pa.table({"exact_identity": old_exact}),
        archive,
    )
    pq.write_table(
        pa.table(
            {
                "exact_identity": pa.array(
                    [],
                    type=pa.string(),
                )
            }
        ),
        ledger,
    )
    output_root = tmp_path / "formula_v4"
    result = run_structural_supply_design(
        argparse.Namespace(
            registry=REGISTRY,
            production_root_contract=PRODUCTION_CONTRACT,
            active_layout=layout,
            historical_exact_archive=archive,
            source_candidate_ledger=ledger,
            historical_behavior_archive=None,
            additional_exact_archive=[],
            additional_behavior_archive=[],
            split_manifest=None,
            compute_threads=2,
            output_root=output_root,
            repo_sha="test-sha",
            task_id="test-task",
            allow_noncanonical_host=True,
            static_only=True,
            structural_typed_surface_audit=False,
            structural_formula_v4_supply=True,
        )
    )

    supply = result["formula_space_supply"]
    decision = result["supply_decision"]
    assert result["status"] == (
        "STRUCTURAL_FORMULA_V4_EXACT_PASS_BEHAVIOR_PENDING"
    )
    assert supply["formula_space_id"] == (
        STRUCTURAL_FORMULA_V4_SPACE_ID
    )
    assert supply["raw_categorical_rows"] == 1_980
    assert supply["raw_exact_unique"] == 1_980
    assert supply["raw_canonical_unique"] == 1_980
    assert supply["post_archive_exact_supply"] == 1_540
    assert supply["post_archive_exact_supply"] >= (
        STRUCTURAL_FORMULA_V4_MINIMUM_FRESH_EXACT
    )
    assert decision["exact_supply_gate"] == "PASS"
    assert decision["behavior_supply_gate"] == "PENDING"
    assert decision["financial_qualification_authorized"] is False
    assert (
        output_root
        / "structural_formula_v4_decision_catalog.json"
    ).is_file()
    assert (
        output_root / "synthetic_v4_adaptation_proof.json"
    ).is_file()
    manifest = json.loads(
        (output_root / "artifact_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["financial_reads"] == 0
    assert manifest["phase3cm_pair_count"] == 0


def test_structural_v2_fresh_policy_matches_uniform_without_exact_replay() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    _, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )
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
    uniform_rng = np.random.default_rng(2026072501)
    cem_rng = np.random.default_rng(2026072501)
    uniform_seen: set[str] = set()
    cem_seen: set[str] = set()
    uniform_rows = []
    cem_rows = []
    for _ in range(40):
        uniform_pair = projection.generate_available(
            formula_space_id=EXPANDED_FORMULA_SPACE_ID,
            policy=uniform,
            rng=uniform_rng,
            exact_seen=uniform_seen,
        )
        cem_pair = projection.generate_available(
            formula_space_id=EXPANDED_FORMULA_SPACE_ID,
            policy=cem,
            rng=cem_rng,
            exact_seen=cem_seen,
        )
        uniform_rows.append(uniform_pair.candidate["exact_identity"])
        cem_rows.append(cem_pair.candidate["exact_identity"])
        uniform_seen.add(uniform_rows[-1])
        cem_seen.add(cem_rows[-1])

    assert uniform_rows == cem_rows
    assert len(set(cem_rows)) == 40
    assert len(
        projection.generate_available(
            formula_space_id=EXPANDED_FORMULA_SPACE_ID,
            policy=uniform,
            rng=uniform_rng,
            exact_seen=uniform_seen,
        ).candidate["decision_trace"]
    ) == 2


def test_structural_v2_exact_exhaustion_closes_as_bounded_underfill() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    _, roots = _load_production_contract(
        PRODUCTION_CONTRACT,
        registry=registry,
    )
    projection = MinuteStaticProductionProjection(
        RegistryDrivenGenerator(
            registry,
            constructor_profile=COMPOSITIONAL_V2_PROFILE,
            route_root_allowlist={"MINUTE_STATIC": roots},
        )
    )
    catalog = projection._available_candidate_catalog(
        EXPANDED_FORMULA_SPACE_ID
    )
    remaining = {
        str(row["candidate"]["exact_identity"]) for row in catalog[:2]
    }
    exact_seen = {
        str(row["candidate"]["exact_identity"])
        for row in catalog
        if str(row["candidate"]["exact_identity"]) not in remaining
    }
    proposals, funnel = _generate_checkpoint_pool(
        projection=projection,
        arm="arm_b_uniform_expanded",
        policy=AvailableUniformPolicy(),
        rng=np.random.default_rng(2026072503),
        exact_seen=exact_seen,
        checkpoint_id="checkpoint_003",
        route_id="MINUTE_STATIC",
        skeleton_id="cn.comp.v2.minute_static.field_spread",
        behavior_probe_target=32,
        availability_masked_sampling=True,
        generator_policy_id="available_uniform_v1",
        sampled_selection_cap=32,
        full_pair_cap=24,
    )
    assert len(proposals) == 2
    assert funnel["exact_unique_pairs"] == 2
    assert funnel["underfill_reason"] == "EXACT_SUPPLY_EXHAUSTED"


def test_structural_v2_canary_requires_common_first_full_evaluation_set(
    tmp_path: Path,
) -> None:
    uniform_arm, cem_arm = PAIRED_STRUCTURAL_CEM_V2_ARMS
    for arm in PAIRED_STRUCTURAL_CEM_V2_ARMS:
        for checkpoint in (1, 2):
            root = (
                tmp_path
                / "arms"
                / arm
                / f"checkpoint_{checkpoint:03d}"
            )
            root.mkdir(parents=True)
            exact_ids = [
                f"exact-{checkpoint}-{index}" for index in range(12)
            ]
            pq.write_table(
                pa.Table.from_pylist(
                    [
                        {
                            "exact_identity": exact_id,
                            "raw_attempt": index + 1,
                        }
                        for index, exact_id in enumerate(exact_ids)
                    ]
                ),
                root / "proposal_ledger.parquet",
            )
            pq.write_table(
                pa.Table.from_pylist(
                    [
                        {
                            "exact_identity": exact_id,
                            "outcome_class": "EVALUATED",
                        }
                        for exact_id in exact_ids
                    ]
                ),
                root / "observation_ledger.parquet",
            )
            (root / "checkpoint_summary.json").write_text(
                json.dumps({"exact_duplicate_pairs": 0}),
                encoding="utf-8",
            )
            (root / "arm_state.json").write_text(
                json.dumps(
                    {
                        "optimizer_state": (
                            {
                                "current_state": "ADAPTED",
                                "generation": 2,
                            }
                            if arm == cem_arm and checkpoint == 2
                            else None
                        )
                    }
                ),
                encoding="utf-8",
            )

    metrics = {
        arm: {
            "evaluated_pairs": 24,
            "positive_matched_pairs": 12,
            "median_signed_matched_increment": 1.0,
        }
        for arm in PAIRED_STRUCTURAL_CEM_V2_ARMS
    }
    passed = _paired_structural_canary_verdict(tmp_path, metrics)
    assert passed["status"] == "MECHANICAL_PASS_RUN_MEDIUM"

    cem_first = (
        tmp_path
        / "arms"
        / cem_arm
        / "checkpoint_001"
        / "observation_ledger.parquet"
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "exact_identity": f"different-{index}",
                    "outcome_class": "EVALUATED",
                }
                for index in range(12)
            ]
        ),
        cem_first,
    )
    failed = _paired_structural_canary_verdict(tmp_path, metrics)
    assert failed["status"] == "MECHANICAL_FAIL_STOP"
    assert not failed["mechanical_contracts"][
        "generation_one_full_evaluation_set_parity"
    ]


@pytest.mark.parametrize("mode", ("canary", "medium", "supply"))
def test_structural_v2_modes_cannot_override_77o_host(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mode: str,
) -> None:
    monkeypatch.setattr(
        "scripts.run_minute_static_production_cem_v3.platform.node",
        lambda: "NOT-77O",
    )
    with pytest.raises(
        RuntimeError, match="official V3 evidence must run on"
    ):
        run_financial(
            argparse.Namespace(
                paired_structural_cem_v2_canary=mode == "canary",
                paired_structural_cem_v2_medium=mode == "medium",
                paired_structural_cem_v2_supply_medium=mode == "supply",
                allow_noncanonical_host=True,
                output_root=tmp_path,
            )
        )


@pytest.mark.parametrize(
    ("checkpoint_count", "full_pair_cap"),
    (
        (
            PAIRED_STRUCTURAL_CEM_V2_MEDIUM_CHECKPOINT_COUNT,
            PAIRED_STRUCTURAL_CEM_V2_MEDIUM_FULL_PAIR_CAP,
        ),
        (
            PAIRED_STRUCTURAL_SUPPLY_MEDIUM_CHECKPOINT_COUNT,
            PAIRED_STRUCTURAL_SUPPLY_MEDIUM_FULL_PAIR_CAP,
        ),
    ),
)
def test_structural_v2_medium_requires_financial_and_runtime_increment(
    tmp_path: Path,
    checkpoint_count: int,
    full_pair_cap: int,
) -> None:
    uniform_arm, cem_arm = PAIRED_STRUCTURAL_CEM_V2_ARMS
    for arm in PAIRED_STRUCTURAL_CEM_V2_ARMS:
        for checkpoint in range(1, checkpoint_count + 1):
            root = (
                tmp_path
                / "arms"
                / arm
                / f"checkpoint_{checkpoint:03d}"
            )
            root.mkdir(parents=True)
            exact_ids = [
                f"medium-{checkpoint}-{index}"
                for index in range(full_pair_cap)
            ]
            pq.write_table(
                pa.Table.from_pylist(
                    [
                        {
                            "exact_identity": exact_id,
                            "raw_attempt": index + 1,
                        }
                        for index, exact_id in enumerate(exact_ids)
                    ]
                ),
                root / "proposal_ledger.parquet",
            )
            increment = (
                float(checkpoint)
                if arm == uniform_arm or checkpoint == 1
                else float(checkpoint + 2)
            )
            pq.write_table(
                pa.Table.from_pylist(
                    [
                        {
                            "exact_identity": exact_id,
                            "outcome_class": "EVALUATED",
                            "signed_matched_increment": increment,
                        }
                        for exact_id in exact_ids
                    ]
                ),
                root / "observation_ledger.parquet",
            )
            (root / "checkpoint_summary.json").write_text(
                json.dumps(
                    {
                        "exact_duplicate_pairs": 0,
                        "validation_reads": 0,
                        "holdout_reads": 0,
                        "forward_2026_reads": 0,
                    }
                ),
                encoding="utf-8",
            )
            (root / "arm_state.json").write_text(
                json.dumps(
                    {
                        "optimizer_state": (
                            {
                                "initialization_origin": (
                                    "fresh_uniform_from_frozen_decision_catalog"
                                ),
                                "imported_source_campaign": "none",
                                "current_state": "ADAPTED",
                                "generation": checkpoint_count,
                                "reward_observation_count": (
                                    checkpoint_count * full_pair_cap
                                ),
                            }
                            if (
                                arm == cem_arm
                                and checkpoint == checkpoint_count
                            )
                            else None
                        )
                    }
                ),
                encoding="utf-8",
            )

    metrics = {
        uniform_arm: {
            "evaluated_pairs": checkpoint_count * full_pair_cap,
            "behavior_discovery_per_evaluated_pair": 0.50,
            "selected_backend_host_cpu_median": 0.80,
            "maximum_observed_cache_bytes": 2 * 1024**3,
            "minimum_free_memory_bytes": 60 * 1024**3,
            "full_coordinate_pairs_per_wall_hour": 100.0,
        },
        cem_arm: {
            "evaluated_pairs": checkpoint_count * full_pair_cap,
            "behavior_discovery_per_evaluated_pair": 0.50,
            "selected_backend_host_cpu_median": 0.82,
            "maximum_observed_cache_bytes": 2 * 1024**3,
            "minimum_free_memory_bytes": 58 * 1024**3,
            "full_coordinate_pairs_per_wall_hour": 95.0,
        },
    }
    passed = _paired_structural_medium_verdict(
        tmp_path,
        metrics,
        prior_canary_exact={"canary-only"},
        checkpoint_count=checkpoint_count,
        full_pair_cap=full_pair_cap,
    )
    assert passed["status"] == (
        "STRUCTURAL_CEM_V2_FINANCIALLY_QUALIFIED"
    )
    assert not passed["large_search_authorized"]
    assert passed["ready_for_separate_large_search_authorization"]
    assert passed["mechanical_contracts"][
        "generation_one_exact_stream_parity"
    ]

    cem_first_proposals = (
        tmp_path / "arms" / cem_arm / "checkpoint_001"
        / "proposal_ledger.parquet"
    )
    rows = pq.read_table(cem_first_proposals).to_pylist()
    rows[0]["exact_identity"] = "different-first-proposal"
    pq.write_table(pa.Table.from_pylist(rows), cem_first_proposals)
    unpaired = _paired_structural_medium_verdict(
        tmp_path,
        metrics,
        prior_canary_exact={"canary-only"},
        checkpoint_count=checkpoint_count,
        full_pair_cap=full_pair_cap,
    )
    assert unpaired["status"] == "STRUCTURAL_CEM_V2_NOT_QUALIFIED"
    assert not unpaired["mechanical_contracts"][
        "generation_one_exact_stream_parity"
    ]

    cem_adaptive = (
        tmp_path / "arms" / cem_arm / "checkpoint_002"
        / "observation_ledger.parquet"
    )
    rows = pq.read_table(cem_adaptive).to_pylist()
    for row in rows:
        row["signed_matched_increment"] = -1.0
    pq.write_table(pa.Table.from_pylist(rows), cem_adaptive)
    failed = _paired_structural_medium_verdict(
        tmp_path,
        metrics,
        prior_canary_exact={"canary-only"},
        checkpoint_count=checkpoint_count,
        full_pair_cap=full_pair_cap,
    )
    assert failed["status"] == "STRUCTURAL_CEM_V2_NOT_QUALIFIED"
    assert not failed["financial_checks"][
        "adaptive_positive_count_noninferior"
    ]


def test_session_sample_is_frozen_month_stratified_quarter() -> None:
    split = FixedSplitAuthority.read(SPLIT)
    left = _session_sample_contract(split, seed=2026072500)
    right = _session_sample_contract(split, seed=2026072500)
    assert left == right
    assert left["selected_session_count"] == len(
        left["selected_sessions"]
    )
    assert left["selected_session_count"] == 91
    assert left["authority_lifecycle"] == (
        "EXPERIMENTAL_CAMPAIGN_LOCAL"
    )
    assert left["validation_reads"] == 0
    assert left["holdout_reads"] == 0
    assert left["forward_2026_reads"] == 0
    assert all(
        row["selected_session_count"]
        == max(1, round(row["full_session_count"] * 0.25))
        for row in left["month_receipts"]
    )


def test_streaming_evaluator_binds_session_sample_without_split_rewrite(
    tmp_path: Path,
) -> None:
    split = FixedSplitAuthority.read(SPLIT)
    contract = _session_sample_contract(split, seed=2026072501)
    path = tmp_path / "session_sample.json"
    path.write_text(
        json.dumps(contract, sort_keys=True),
        encoding="utf-8",
    )
    full_train = tuple(
        row["trade_date"]
        for row in split.rows
        if row["split"] == "train"
    )
    selected, receipt = _session_sample_calendar(
        path,
        binding={
            "split_manifest_hash": split.manifest_hash,
            "evaluation_scope": "development_session_sample",
            "session_sample": {
                "sha256": _streaming_sha256(path)
            },
        },
        evaluation_role="train",
        full_calendar=full_train,
    )
    assert selected == tuple(contract["selected_sessions"])
    assert receipt["selected_session_count"] == 91
    assert receipt["full_session_count"] == 364
