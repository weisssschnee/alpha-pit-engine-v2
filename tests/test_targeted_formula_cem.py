from __future__ import annotations

import copy
from pathlib import Path

import numpy as np
import pytest

from scripts.run_targeted_formula_cem_qualification import (
    _qualification_gate_status,
)
from our_system_phase2.services.categorical_cem import (
    CategoricalCEMPolicy,
)
from our_system_phase2.services.search_choice_policy import (
    EXPANDED_FORMULA_SPACE_ID,
    OLD_FORMULA_SPACE_ID,
    PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID,
    LegacyParityPolicy,
    TargetedFormulaProjection,
    TraceReplayPolicy,
    UniformPolicy,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    RegistryDrivenGenerator,
    load_development_discovery_root_authority,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime"
    / "field_registry"
    / "cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
DISCOVERY = (
    REPO
    / "runtime"
    / "run_plans"
    / "cn_core_pack_development_discovery_v1.json"
)
ROUTE_ID = "DISCLOSURE_EVENT"
SKELETON_ID = "cn.comp.v2.disclosure_event.pre_event_path"


@pytest.mark.parametrize(
    ("exact_gate", "behavior_gate", "formula_gate", "expected"),
    (
        (False, True, True, "TARGET_FAMILY_SUPPLY_NOT_PROVEN"),
        (True, False, True, "TARGET_FAMILY_SUPPLY_NOT_PROVEN"),
        (True, True, False, "FORMULA_SPACE_INCREMENT_NOT_PROVEN"),
        (True, True, True, "PASS"),
    ),
)
def test_qualification_gate_classifies_supply_and_formula_failures_separately(
    exact_gate: bool,
    behavior_gate: bool,
    formula_gate: bool,
    expected: str,
) -> None:
    assert (
        _qualification_gate_status(
            exact_gate=exact_gate,
            behavior_gate=behavior_gate,
            formula_space_behavior_gate=formula_gate,
        )
        == expected
    )


def _projection() -> TargetedFormulaProjection:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    discovery = load_development_discovery_root_authority(
        DISCOVERY,
        registry=registry,
    )
    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist=discovery["route_root_allowlists"],
    )
    return TargetedFormulaProjection(
        generator=generator,
        route_id=ROUTE_ID,
        skeleton_id=SKELETON_ID,
        extension_id=PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID,
    )


def test_catalog_is_read_only_projection_of_authority() -> None:
    projection = _projection()
    old = projection.decision_catalog(OLD_FORMULA_SPACE_ID)
    expanded = projection.decision_catalog(EXPANDED_FORMULA_SPACE_ID)

    assert old["route_id"] == ROUTE_ID
    assert old["skeleton_id"] == SKELETON_ID
    assert old["authority_mode"] == "READ_ONLY_PROJECTION"
    assert old["field_authority"] == "UnifiedCapabilityRegistry"
    assert old["constructor_authority"] == "CompositionalGrammarV2"
    assert [row["decision_type"] for row in old["decisions"]] == [
        "EVENT_FIELD",
        "PAYLOAD_FIELD",
    ]
    assert [row["decision_type"] for row in expanded["decisions"]] == [
        "EVENT_FIELD",
        "PAYLOAD_FIELD",
        "EXTENSION",
    ]
    field_tokens = [
        choice
        for row in old["decisions"]
        for choice in row["ordered_choices"]
    ]
    assert field_tokens
    assert all(
        str(row["token_id"]).startswith("cn.rep.")
        for row in field_tokens
    )
    assert expanded["extension_ids"] == [
        "PRODUCTION",
        PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID,
    ]


def test_legacy_projection_replays_exact_pair_and_control() -> None:
    projection = _projection()
    legacy = projection.generator.propose_attempt(
        ROUTE_ID,
        attempt_index=2,
        seed=2026072407,
    )
    selected = projection.legacy_selection_from_pair(legacy)
    projected = projection.generate(
        formula_space_id=OLD_FORMULA_SPACE_ID,
        policy=LegacyParityPolicy(selected),
        rng=np.random.default_rng(7),
    )

    for left, right in (
        (legacy.candidate, projected.candidate),
        (legacy.control, projected.control),
    ):
        assert left["exact_identity"] == right["exact_identity"]
        assert left["canonical_identity"] == right["canonical_identity"]
        assert left["declared_field_ids"] == right["declared_field_ids"]
        assert left["route_id"] == right["route_id"]
        assert left["skeleton_id"] == right["skeleton_id"]
        assert left["clock_contract"] == right["clock_contract"]
        assert left["maturity_contract"] == right["maturity_contract"]
        assert left["control_constructor_id"] == right["control_constructor_id"]


def test_trace_replay_and_single_extension_are_exact() -> None:
    projection = _projection()
    rng = np.random.default_rng(31)
    first = projection.generate(
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
        policy=UniformPolicy(),
        rng=rng,
    )
    replayed = projection.generate(
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
        policy=TraceReplayPolicy(first.candidate["decision_trace"]),
        rng=np.random.default_rng(999),
    )
    assert replayed.candidate["exact_identity"] == first.candidate["exact_identity"]
    assert replayed.control["exact_identity"] == first.control["exact_identity"]
    assert replayed.candidate["decision_trace"] == first.candidate["decision_trace"]

    production_trace = [
        {
            **row,
            "selected_token_id": (
                "extension.PRODUCTION"
                if row["decision_type"] == "EXTENSION"
                else row["selected_token_id"]
            ),
        }
        for row in first.candidate["decision_trace"]
    ]
    extension_trace = [
        {
            **row,
            "selected_token_id": (
                f"extension.{PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID}"
                if row["decision_type"] == "EXTENSION"
                else row["selected_token_id"]
            ),
        }
        for row in first.candidate["decision_trace"]
    ]
    production = projection.generate(
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
        policy=TraceReplayPolicy(production_trace),
        rng=np.random.default_rng(1),
    )
    extended = projection.generate(
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
        policy=TraceReplayPolicy(extension_trace),
        rng=np.random.default_rng(1),
    )
    assert production.candidate["declared_field_ids"] == extended.candidate[
        "declared_field_ids"
    ]
    assert production.control["declared_field_ids"] == extended.control[
        "declared_field_ids"
    ]
    assert production.candidate["expression"] != extended.candidate["expression"]
    assert production.control["expression"] != extended.control["expression"]
    assert extended.candidate["extension_id"] == PRE_EVENT_PAYLOAD_SIGN_EXTENSION_ID
    assert "Sign(" in extended.candidate["expression"]
    assert extended.candidate["legal"] is True
    assert extended.control["legal"] is True


def _evaluated_observations(
    projection: TargetedFormulaProjection,
    policy: CategoricalCEMPolicy,
    rng: np.random.Generator,
    count: int,
) -> list[dict[str, object]]:
    rows = []
    for index in range(count):
        pair = projection.generate(
            formula_space_id=EXPANDED_FORMULA_SPACE_ID,
            policy=policy,
            rng=rng,
        )
        rows.append(
            {
                "proposal_id": f"p{index:03d}",
                "outcome_class": "EVALUATED",
                "signed_matched_increment": float(index - count // 2),
                "exact_identity": pair.candidate["exact_identity"],
                "decision_trace": copy.deepcopy(
                    pair.candidate["decision_trace"]
                ),
            }
        )
    return rows


def test_cem_is_fresh_support_gated_and_exactly_restorable() -> None:
    projection = _projection()
    specs = projection.decision_specs(EXPANDED_FORMULA_SPACE_ID)
    policy = CategoricalCEMPolicy.fresh(
        decisions=specs,
        decision_catalog_hash=projection.decision_catalog_hash(
            EXPANDED_FORMULA_SPACE_ID
        ),
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
    )
    genesis = policy.state_dict(rng=np.random.default_rng(41))
    assert genesis["state_origin"] == "fresh_uniform_from_frozen_decision_catalog"
    assert genesis["generation"] == 0
    assert genesis["reward_observation_count"] == 0
    assert genesis["source_campaign"] == "none"

    small_rng = np.random.default_rng(43)
    small = _evaluated_observations(projection, policy, small_rng, 7)
    before = copy.deepcopy(policy.probability_tables)
    receipt = policy.tell(small)
    assert receipt["updated_context_count"] == 0
    assert policy.probability_tables == before

    rng = np.random.default_rng(47)
    observations = _evaluated_observations(projection, policy, rng, 24)
    receipt = policy.tell(observations)
    assert receipt["updated_context_count"] >= 1
    assert receipt["negative_evaluable_observation_count"] > 0

    state = policy.state_dict(rng=rng)
    restored_rng = np.random.default_rng(0)
    restored = CategoricalCEMPolicy.restore(
        state,
        decisions=specs,
        decision_catalog_hash=projection.decision_catalog_hash(
            EXPANDED_FORMULA_SPACE_ID
        ),
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
        rng=restored_rng,
    )
    next_original = projection.generate(
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
        policy=policy,
        rng=rng,
    )
    next_restored = projection.generate(
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
        policy=restored,
        rng=restored_rng,
    )
    assert next_original.candidate["decision_trace"] == next_restored.candidate[
        "decision_trace"
    ]
    assert next_original.candidate["exact_identity"] == next_restored.candidate[
        "exact_identity"
    ]


def test_cem_rejects_infrastructure_failure_and_catalog_drift() -> None:
    projection = _projection()
    specs = projection.decision_specs(EXPANDED_FORMULA_SPACE_ID)
    policy = CategoricalCEMPolicy.fresh(
        decisions=specs,
        decision_catalog_hash=projection.decision_catalog_hash(
            EXPANDED_FORMULA_SPACE_ID
        ),
        formula_space_id=EXPANDED_FORMULA_SPACE_ID,
    )
    with pytest.raises(RuntimeError, match="INFRASTRUCTURE_FAILURE"):
        policy.tell(
            [
                {
                    "proposal_id": "infra",
                    "outcome_class": "INFRASTRUCTURE_FAILURE",
                    "decision_trace": [],
                }
            ]
        )
    state = policy.state_dict(rng=np.random.default_rng(53))
    with pytest.raises(RuntimeError, match="DECISION_CATALOG_HASH_DRIFT"):
        CategoricalCEMPolicy.restore(
            state,
            decisions=specs,
            decision_catalog_hash="wrong",
            formula_space_id=EXPANDED_FORMULA_SPACE_ID,
            rng=np.random.default_rng(53),
        )
