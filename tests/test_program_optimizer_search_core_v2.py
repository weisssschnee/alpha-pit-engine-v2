from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from our_system_phase2.services.candidate_program_proposal_v0 import (
    CandidateProgramProposalAdapterV0,
    ProgramSourceComponentV0,
)
from our_system_phase2.services.candidate_program_v1 import ProgramCompilerV1
from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.program_optimizer_search_core_v2 import (
    SEARCH_CORE_V2_ARMS,
    SearchCoreV2TournamentState,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    program_availability_entries_v1,
    program_structural_genes_v1,
)
from our_system_phase2.services.program_search_primitive_credit_v1 import (
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
)
from our_system_phase2.services.program_search_semantic_mcts_v1 import (
    SEMANTIC_MCTS_PROGRAM_V1,
)
from our_system_phase2.services.program_search_state_jump_generator_v2 import (
    SEMANTIC_STATE_JUMP_GENERATOR_V2,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"


def _self_hashed_stats(body: dict) -> dict:
    payload = dict(body)
    payload["stats_payload_sha256"] = stable_hash(payload)
    return payload


@pytest.fixture(scope="module")
def search_core_context():
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    allowlists = json.loads(ROOT_CONTRACT.read_text(encoding="utf-8"))["route_root_allowlists"]
    grammar = CompositionalGrammarV2(registry, route_root_allowlist=allowlists)
    routes = {
        "base": "SLOW_CROSS_SECTIONAL_LEVEL",
        "temporal": "SLOW_TEMPORAL_CHANGE",
        "market": "MARKET_REGIME_CONDITION",
        "event": "DISCLOSURE_EVENT",
    }
    pools: dict[str, list[ProgramSourceComponentV0]] = {}
    for role, route_id in routes.items():
        rows = []
        seen = set()
        for attempt in range(120):
            pair = grammar.propose(route_id, attempt_index=attempt, seed=19000 + attempt * 29)
            component = ProgramSourceComponentV0(
                role=role,
                primary=dict(pair.primary),
                control=dict(pair.control),
                proposal_id=f"search-core-{role}-{attempt}",
                trial_number=attempt,
                sampling_phase="STARTUP_RANDOM",
            )
            if component.component_id in seen:
                continue
            seen.add(component.component_id)
            rows.append(component)
            if len(rows) == 8:
                break
        assert len(rows) >= 5, role
        pools[role] = rows

    composer = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    catalog_rows = []
    metadata_raw: list[tuple[dict[str, str], dict]] = []
    templates = (
        "BASE_EVENT",
        "BASE_TEMPORAL",
        "BASE_MARKET",
        "BASE_TEMPORAL_EVENT",
    )
    for template_index, template_id in enumerate(templates):
        roles = tuple(role for role in ("base", "temporal", "market", "event") if role in template_id.lower().split("_") or role == "base")
        # Use explicit role extraction because BASE_* template tokens are uppercase.
        roles = ("base",) + tuple(
            role for role in ("temporal", "market", "event") if role.upper() in template_id
        )
        for ordinal in range(12):
            components = {
                role: pools[role][(ordinal + template_index + offset) % len(pools[role])]
                for offset, role in enumerate(roles)
            }
            kwargs = {
                f"{role}_component": component
                for role, component in components.items()
                if role != "base"
            }
            policy = {
                "temporal": ("ADD", "MAX", "SUBTRACT")[ordinal % 3],
                "market": ("FILTER", "GATE", "MODULATE")[ordinal % 3],
                "event_episode": "SOURCE_ROUTE_EPISODE",
                "event_application": ("FILTER", "GATE")[ordinal % 2],
            }
            program = composer.compose(
                template_id,
                components["base"],
                combination_policy=policy,
                **kwargs,
            )
            compiled = compiler.compile(program)
            genes = program_structural_genes_v1(
                program_template_id=template_id,
                components=components,
                combination_policy=policy,
                program=program,
                compiled=compiled,
            )
            catalog_rows.append({"genes": genes})
            metadata_raw.append(
                (
                    genes,
                    {
                        "template_id": template_id,
                        "components": {
                            role: {
                                "component_id": component.component_id,
                                "route_id": component.route_id,
                                "skeleton_id": str(genes[f"{role}__skeleton_id"]),
                            }
                            for role, component in components.items()
                        },
                        "tie_break_identity": program.semantic_program_hash,
                    },
                )
            )
    entries = program_availability_entries_v1(catalog_rows)
    assert len(entries) == len(catalog_rows)
    metadata = {
        entry.exact_identity: raw_metadata
        for entry, (_, raw_metadata) in zip(entries, metadata_raw, strict=True)
    }
    all_roles = {role for row in metadata.values() for role in row["components"]}
    role_stats = {
        role: {"n": 40, "productive": 18} for role in sorted(all_roles)
    }
    route_stats = {}
    skeleton_stats = {}
    exact_stats = {}
    template_stats = {}
    for row in metadata.values():
        template_stats.setdefault(row["template_id"], {"n": 0, "productive": 0})
        template_stats[row["template_id"]]["n"] += 12
        template_stats[row["template_id"]]["productive"] += 6
        for role, binding in row["components"].items():
            route_stats[f"{role}|{binding['route_id']}"] = {"n": 40, "productive": 18}
            skeleton_stats[f"{role}|{binding['skeleton_id']}"] = {"n": 18, "productive": 9}
            exact_stats[f"{role}|{binding['component_id']}"] = {"n": 8, "productive": 4}
    primitive_stats = _self_hashed_stats(
        {
            "schema_version": "synthetic_search_core_v2_primitive_stats_v1",
            "status": "SPENT_DEVELOPMENT_PRIMITIVE_CREDIT_STATS_FROZEN",
            "financial_evaluation_performed_by_builder": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "global": {"n": 160, "productive": 72},
            "template": template_stats,
            "role": role_stats,
            "component_route": route_stats,
            "component_skeleton": skeleton_stats,
            "component_exact": exact_stats,
        }
    )
    primitive_config = {
        "metadata_by_exact_identity": metadata,
        "primitive_stats": primitive_stats,
        "primitive_stats_payload_sha256": primitive_stats["stats_payload_sha256"],
    }
    ordered_slots = tuple(entries[0].genes)
    seeds = {
        PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1: 4101,
        SEMANTIC_STATE_JUMP_GENERATOR_V2: 4103,
        SEMANTIC_MCTS_PROGRAM_V1: 4105,
    }
    state = SearchCoreV2TournamentState(
        campaign_id="SEARCH_CORE_V2_TEST",
        entries=entries,
        all_frozen_exact_identities=[entry.exact_identity for entry in entries],
        primitive_config=primitive_config,
        composer=composer,
        compiler=compiler,
        components_by_role=pools,
        ordered_gene_slots=ordered_slots,
        seeds=seeds,
        state_jump_config={"maximum_attempts": 128},
        mcts_config={"simulations_per_ask": 4},
    )
    return state, entries, primitive_config, composer, compiler, pools, ordered_slots


def test_search_core_v2_arm_coverage_and_metadata(search_core_context):
    state, *_ = search_core_context
    assert tuple(state.adapters) == SEARCH_CORE_V2_ARMS
    metadata = state.optimizer_metadata()
    assert set(metadata) == set(SEARCH_CORE_V2_ARMS)
    assert metadata[SEMANTIC_STATE_JUMP_GENERATOR_V2]["frozen_catalog_required"] is False
    assert metadata[SEMANTIC_MCTS_PROGRAM_V1]["semantic_decision_surface"]["derived_lag_class"] is False


def test_search_core_v2_snapshot_restore_exact(search_core_context):
    state, entries, primitive_config, composer, compiler, pools, ordered_slots = search_core_context
    snapshot = state.snapshot()
    restored = SearchCoreV2TournamentState.restore(
        snapshot=snapshot,
        entries=entries,
        all_frozen_exact_identities=[entry.exact_identity for entry in entries],
        primitive_config=primitive_config,
        composer=composer,
        compiler=compiler,
        components_by_role=pools,
        ordered_gene_slots=ordered_slots,
        expected_campaign_id="SEARCH_CORE_V2_TEST",
    )
    assert restored.snapshot() == snapshot


@pytest.mark.parametrize("arm", SEARCH_CORE_V2_ARMS)
def test_search_core_v2_preview_commit_is_exact(search_core_context, arm):
    state, entries, *_ = search_core_context
    eligible = [
        entry.exact_identity
        for entry in entries
        if entry.genes["program_template_id"] == "BASE_EVENT"
    ]
    kwargs = {
        "arm": arm,
        "checkpoint_id": f"preview-{arm}",
        "count": 3,
        "required_program_template_id": "BASE_EVENT",
        "eligible_exact_identities": eligible,
        "batch_group_constraint": None,
    }
    preview = state.ask(**kwargs)
    assert len(preview) == 3
    state.commit_ask(expected_asks=preview, **kwargs)
    # Drop pending without pretending these synthetic asks were financially evaluated.
    adapter = state.adapters[arm]
    adapter._pending.clear()
    if hasattr(adapter, "_pending_paths"):
        adapter._pending_paths.clear()
    if hasattr(adapter, "_pending_generated"):
        adapter._pending_generated.clear()


def test_search_core_v2_state_jump_expands_beyond_frozen_exact_space(search_core_context):
    state, entries, *_ = search_core_context
    frozen = {entry.exact_identity for entry in entries}
    asks = state._ask_live(
        arm=SEMANTIC_STATE_JUMP_GENERATOR_V2,
        checkpoint_id="expand",
        count=8,
        required_program_template_id="BASE_EVENT",
        eligible_exact_identities=None,
        batch_group_constraint=None,
    )
    assert len(asks) == 8
    assert not ({row["exact_identity"] for row in asks} & frozen)
