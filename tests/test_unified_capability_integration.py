from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scripts.build_unified_capability_registry import build
from our_system_phase2.runtime.cn_unified_capability_discovery import _apply_metrics
from our_system_phase2.services.search_exposure_ledger import SearchExposureLedger
from our_system_phase2.services.typed_route_compiler import TypedRouteCompiler
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    source_field_id,
)
from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator


REPO = Path(__file__).resolve().parents[1]
EXTERNAL_ROOT = REPO / "reports/cn_unified_capability_architecture_phase2_20260714/external_contracts"
FUNDAMENTAL_ROOT = REPO / "runtime/cn_pit_fundamental_fabric_v1"


@pytest.fixture(scope="module")
def built_registry(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict[str, object]]:
    output = tmp_path_factory.mktemp("unified_registry")
    summary = build(
        external_root=EXTERNAL_ROOT,
        fundamental_universe_path=(
            REPO
            / "reports/cn_field_universe_search_exposure_audit_20260714/fundamental_source_universe.csv"
        ),
        fundamental_registry_path=FUNDAMENTAL_ROOT / "fundamental_semantic_registry.json",
        fundamental_manifest_path=FUNDAMENTAL_ROOT / "pit_sidecar_manifest.json",
        active121_registry_path=REPO / "runtime/field_registry/nextgen_dark_field_registry_v2.json",
        broad_event_pack_path=REPO / "reports/cn_broad_event_recovery_20260713/DISCOVERY_ENTRY_PACK.json",
        output_root=output,
        repo_sha="test-repo-sha",
    )
    return output, summary


def test_source_identity_is_release_bound_and_deterministic() -> None:
    kwargs = {
        "provider": "AKSHARE_EASTMONEY",
        "source_table": "profit_sheet_report_em",
        "source_field": "OPERATE_INCOME",
    }
    first = source_field_id(source_release_version="release_a", **kwargs)
    assert first == source_field_id(source_release_version="release_a", **kwargs)
    assert first != source_field_id(source_release_version="release_b", **kwargs)
    assert first.startswith("cn.sf.")


def test_registry_merges_external_contracts_and_fails_closed(
    built_registry: tuple[Path, dict[str, object]],
) -> None:
    output, summary = built_registry
    registry = UnifiedCapabilityRegistry.read(output / "unified_capability_registry.json")
    assert set(registry.route_contracts) == set(ROUTE_IDS)
    assert summary["external_active121_unresolved_count"] == 0
    assert summary["external_active121_join_count"] == 59
    assert 0 < int(summary["canonical_fundamental_root_count"]) <= 384
    assert int(summary["fundamental_source_field_count"]) == 1227
    broad_fields = registry.fields_for_route("BROAD_EVENT_FROZEN_ENTRY")
    assert len(broad_fields) == 11
    assert {row.source_family for row in broad_fields} == {"broad_event_frozen_entry"}
    assert all(
        not row.search_eligible
        for row in registry.fields
        if row.source_table == "zygc_em"
    )
    assert all(
        row.unit_status != "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED"
        and row.pit_status != "PIT_CONTRACT_UNRESOLVED"
        for row in registry.fields
        if row.search_eligible
    )
    identity_rows = list(
        csv.DictReader(
            (output / "source_field_identity_registry.csv").open(
                "r", encoding="utf-8", newline=""
            )
        )
    )
    assert len(identity_rows) == 1227
    assert len({row["source_field_id"] for row in identity_rows}) == 1227


def test_all_typed_routes_generate_legal_control_complete_pairs(
    built_registry: tuple[Path, dict[str, object]],
) -> None:
    output, _ = built_registry
    registry = UnifiedCapabilityRegistry.read(output / "unified_capability_registry.json")
    generator = RegistryDrivenGenerator(registry)
    exact: set[str] = set()
    for index, route_id in enumerate(ROUTE_IDS):
        budget = 22 if route_id == "BROAD_EVENT_FROZEN_ENTRY" else 2
        rows = generator.generate_route(route_id, proposal_budget=budget, seed=1700 + index)
        assert len(rows) == budget
        assert all(row["legal"] for row in rows)
        assert all(row["matched_control_id"] for row in rows)
        route_exact = {row["exact_identity"] for row in rows}
        assert len(route_exact) == budget
        assert not route_exact.intersection(exact)
        exact.update(route_exact)

    state_rows = generator.generate_route(
        "INTRADAY_STATE_TRANSITION", proposal_budget=2, seed=1777
    )
    candidate = next(row for row in state_rows if not row["is_matched_control"])
    control = next(row for row in state_rows if row["is_matched_control"])
    assert candidate["claimed_state_field_id"] in candidate["field_ids"]
    assert candidate["state_source_expression"] in candidate["canonical_expression"]
    assert control["legal"]


def test_compiler_rejects_direct_market_rank_and_sealed_access(
    built_registry: tuple[Path, dict[str, object]],
) -> None:
    output, _ = built_registry
    registry = UnifiedCapabilityRegistry.read(output / "unified_capability_registry.json")
    generator = RegistryDrivenGenerator(registry)
    compiler = TypedRouteCompiler(registry)
    valid = generator.generate_route(
        "MARKET_REGIME_CONDITION", proposal_budget=2, seed=1759
    )[0]
    market_id = valid["condition_field_ids"][0]
    direct_rank = compiler.compile({**valid, "expression": f"CSRank(${market_id})"})
    assert not direct_rank.legal
    assert direct_rank.rejection_code == "MARKET_FIELD_DIRECT_CSRANK"
    sealed = compiler.compile({**valid, "access_roles": ["development", "forward"]})
    assert not sealed.legal
    assert sealed.rejection_code == "SEALED_DATA_ACCESS"


def test_exposure_ledger_proves_lineage_and_not_evaluated_ceiling(
    built_registry: tuple[Path, dict[str, object]], tmp_path: Path
) -> None:
    output, _ = built_registry
    registry = UnifiedCapabilityRegistry.read(output / "unified_capability_registry.json")
    rows = RegistryDrivenGenerator(registry).generate_route(
        "MINUTE_STATIC", proposal_budget=2, seed=1729
    )
    budgets = {
        route_id: {"proposal": 2, "admission": 1, "strict": 1}
        for route_id in ROUTE_IDS
    }
    ledger = SearchExposureLedger(
        registry=registry,
        run_id="unit-test",
        repo_sha="test-repo-sha",
        contract_hash="contract-hash",
        data_release_hash="development-release-hash",
        route_budgets=budgets,
    )
    ledger.ingest(rows)
    summary = ledger.write(tmp_path / "ledger")
    assert summary["candidate_count"] == 2
    assert summary["exact_count"] == 2
    assert summary["claim_ceiling"] == "NOT_EVALUATED"
    assert (tmp_path / "ledger/CANDIDATE_FIELD_LINEAGE.csv").exists()
    assert (tmp_path / "ledger/UNTESTED_INFORMATION_FAMILIES.csv").exists()


def test_app_exposes_only_explicit_unified_routes() -> None:
    app_source = (REPO / "app.py").read_text(encoding="utf-8")
    assert '"cn-unified-capability-preflight"' in app_source
    assert '"cn-broad-event-frozen-replay"' in app_source
    assert '"cn-unified-capability-discovery"' in app_source
    assert "2026" not in " ".join(
        line for line in app_source.splitlines() if "cn-unified-capability" in line
    )


def test_external_contract_files_are_immutable_inputs() -> None:
    expected = {
        "CN_BROAD_EVENT_ENTRY_POLICY_V2_20260714.json",
        "CN_SEARCH_EXPOSURE_LEDGER_SCHEMA_V1_20260714.json",
        "CN_TYPED_ROUTE_COMPILER_CONTRACT_V1_20260714.json",
        "CN_UNIFIED_CAPABILITY_REGISTRY_V0_2_20260714.json",
        "CN_UNIFIED_DISCOVERY_CAPABILITY_PREFLIGHT_V1_20260714.json",
    }
    assert expected.issubset({path.name for path in EXTERNAL_ROOT.iterdir()})
    assert all(json.loads((EXTERNAL_ROOT / name).read_text(encoding="utf-8")) for name in expected)


def test_zero_reward_is_valid_and_broad_event_uses_frozen_route_seeds() -> None:
    candidate = {
        "candidate_id": "candidate",
        "matched_control_id": "control",
        "is_matched_control": False,
    }
    control = {
        "candidate_id": "control",
        "matched_control_id": "candidate",
        "is_matched_control": True,
    }
    rows = [candidate, control]
    _apply_metrics(
        rows,
        {
            "candidate": {"reward": 0.0},
            "control": {"reward": -0.1},
        },
    )
    assert candidate["matched_increment"] == pytest.approx(0.1)
    assert candidate["development_increment_positive"]

    runner = (
        REPO / "src/our_system_phase2/runtime/cn_unified_capability_discovery.py"
    ).read_text(encoding="utf-8")
    assert "broad_seed_by_name" in runner
    assert "seeds=[1729, 2718]" not in runner
