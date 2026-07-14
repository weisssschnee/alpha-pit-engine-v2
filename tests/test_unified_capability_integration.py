from __future__ import annotations

import csv
import json
import warnings
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from scripts.build_unified_capability_registry import build
import our_system_phase2.runtime.cn_unified_capability_discovery as unified_runner
from our_system_phase2.runtime.cn_unified_capability_discovery import (
    _apply_metrics,
    _evaluate_fundamental_candidates,
    _independent_challenge_eligible,
    _raw_expression,
    _rx_ucb_expand,
    _route_summary,
    _shared_survivor_class,
)
from our_system_phase2.services.real_market_validation import evaluate_panel_expression
from our_system_phase2.services.fundamental_representations import (
    CanonicalFundamentalMaterializer,
)
from our_system_phase2.services.typed_primitive_gate import expression_fields
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


def test_frozen_broad_replays_cannot_unlock_independent_challenge() -> None:
    frozen = {
        "route_id": "BROAD_EVENT_FROZEN_ENTRY",
        "exact_identity": "old-frozen-exact",
    }
    new = {
        "route_id": "SLOW_TEMPORAL_CHANGE",
        "exact_identity": "new-cross-seed-exact",
    }
    assert _shared_survivor_class(frozen) == "OLD_FROZEN_MECHANISM_REPRODUCED"
    assert (
        _shared_survivor_class(new)
        == "NEW_CANONICAL_MECHANISM_CROSS_SEED_REPRODUCED"
    )
    assert not _independent_challenge_eligible(
        [frozen], route_exposure_ok=True, full_development_access=True
    )
    assert _independent_challenge_eligible(
        [frozen, new], route_exposure_ok=True, full_development_access=True
    )


def test_fundamental_evaluation_normalizes_exchange_suffixed_panel_codes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    field_id = "fund_test_level"

    class FakeMaterializer:
        def __init__(self, adapter: object) -> None:
            del adapter

        def materialize(
            self, spec: dict[str, object], coordinates: pd.DataFrame
        ) -> pd.DataFrame:
            assert spec["field_id"] == field_id
            assert coordinates["code"].str.fullmatch(r"\d{6}").all()
            output = coordinates.copy()
            output[field_id] = np.arange(len(output), dtype=float)
            return output

    class FakeRegistry:
        def resolve(self, requested: str) -> SimpleNamespace:
            assert requested == field_id
            return SimpleNamespace(
                field_id=field_id,
                source_family="canonical_fundamental_test",
                metadata={
                    "canonical_representation": {
                        "field_id": field_id,
                        "search_eligible": True,
                    }
                },
            )

    monkeypatch.setattr(
        unified_runner, "CanonicalFundamentalMaterializer", FakeMaterializer
    )
    session = pd.Timestamp("2024-04-29 15:00:00")
    target = pd.DataFrame(
        {
            "code": [f"{index:06d}.SZ" for index in range(1, 31)],
            "session_time": [session] * 30,
            "target": np.arange(30, dtype=float),
        }
    )
    candidate = {
        "candidate_id": "fundamental",
        "matched_control_id": "control",
        "is_matched_control": False,
        "route_id": "SLOW_CROSS_SECTIONAL_LEVEL",
        "field_ids": [field_id],
        "canonical_expression": f"CSRank(${field_id})",
    }
    metrics = _evaluate_fundamental_candidates(
        candidates=[candidate],
        registry=FakeRegistry(),
        adapter=object(),
        target_frame=target,
        cache_root=tmp_path / "cache",
    )
    assert metrics["fundamental"]["support"] == 30
    assert metrics["fundamental"]["rank_ic_mean"] == pytest.approx(1.0)


def test_route_summary_handles_all_nan_increments_without_runtime_warning() -> None:
    rows = [
        {
            "route_id": "DISCLOSURE_EVENT",
            "is_matched_control": False,
            "matched_control_id": "control",
            "matched_increment": float("nan"),
            "legal": True,
            "canonical_identity": "canonical",
            "exact_identity": "exact",
            "behavior_identity": "",
            "admission": True,
            "strict": True,
            "survivor": False,
        }
    ]
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        summary = _route_summary(rows)
    assert not captured
    assert summary["DISCLOSURE_EVENT"]["mean_matched_increment"] is None


def test_holder_source_level_forwards_registered_aggregation() -> None:
    class FakeAdapter:
        observed_transform = ""

        def materialize_level(
            self, request: object, coordinates: pd.DataFrame
        ) -> pd.DataFrame:
            self.observed_transform = str(request.transform)
            output = coordinates.copy()
            output[str(request.output_name)] = 1.0
            return output

    adapter = FakeAdapter()
    materializer = CanonicalFundamentalMaterializer(adapter)
    coordinates = pd.DataFrame(
        {"code": ["000001"], "session_time": [pd.Timestamp("2024-04-29 15:00:00")]}
    )
    output = materializer.materialize(
        {
            "field_id": "fund_holder_test_sum",
            "search_eligible": True,
            "operation": "source_level",
            "source_fields": [
                {
                    "source_table": "main_stock_holder_sina",
                    "source_field": "holder_amount",
                }
            ],
            "parameters": {"aggregation": "sum"},
        },
        coordinates,
    )
    assert adapter.observed_transform == "sum"
    assert output["fund_holder_test_sum"].tolist() == [1.0]


def test_rx_ucb_freezes_an_exact_exhausted_arm_and_reallocates(
    built_registry: tuple[Path, dict[str, object]],
) -> None:
    output, _ = built_registry
    registry = UnifiedCapabilityRegistry.read(output / "unified_capability_registry.json")
    generator = RegistryDrivenGenerator(registry)
    disclosure = generator.generate_route(
        "DISCLOSURE_EVENT", proposal_budget=8, seed=1753
    )
    minute = generator.generate_route("MINUTE_STATIC", proposal_budget=2, seed=1729)
    roots = disclosure + minute
    for row in roots:
        if row["is_matched_control"]:
            continue
        row["matched_increment"] = (
            float("nan") if row["route_id"] == "DISCLOSURE_EVENT" else -1.0
        )
    adaptive = _rx_ucb_expand(
        generator=generator,
        roots=roots,
        seed=51729,
        total_pairs=1,
    )
    assert len(adaptive) == 2
    assert {row["route_id"] for row in adaptive} == {"MINUTE_STATIC"}
    assert all(
        "DISCLOSURE_EVENT" in row["adaptive_exhausted_arms"] for row in adaptive
    )
    assert not {row["exact_identity"] for row in adaptive}.intersection(
        {row["exact_identity"] for row in roots}
    )


def test_adaptive_proxy_reload_includes_fields_introduced_after_roots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame(
        {
            "trade_time": [pd.Timestamp("2025-04-01 10:30:00")],
            "code": ["000001"],
            "close": [10.0],
            "root_field": [1.0],
        }
    )
    candidates = [
        {"canonical_expression": "Sign($root_field)"},
        {"canonical_expression": "Sign($adaptive_field)"},
    ]
    captured: dict[str, object] = {}

    def fake_read_proxy_frame(release, *, columns, row_group_indices, dates):
        captured["columns"] = list(columns)
        output = frame.copy()
        output["adaptive_field"] = 2.0
        return output, [{"path": "development-only"}]

    monkeypatch.setattr(unified_runner, "_read_proxy_frame", fake_read_proxy_frame)
    output, reads = unified_runner._ensure_proxy_candidate_columns(
        SimpleNamespace(),
        frame=frame,
        candidates=candidates,
        row_group_indices=[0],
        dates=["2025-04-01"],
    )
    assert captured["columns"] == [
        "adaptive_field", "close", "code", "root_field", "trade_time"
    ]
    assert output["adaptive_field"].tolist() == [2.0]
    assert reads == [{"path": "development-only"}]


def test_generated_intraday_expressions_execute_on_a_synthetic_panel(
    built_registry: tuple[Path, dict[str, object]],
) -> None:
    output, _ = built_registry
    registry = UnifiedCapabilityRegistry.read(output / "unified_capability_registry.json")
    generator = RegistryDrivenGenerator(registry)
    routes = (
        "MINUTE_STATIC",
        "FIRSTN_PATH",
        "MARKET_REGIME_CONDITION",
        "INTRADAY_STATE_TRANSITION",
    )
    generated = [
        row
        for index, route_id in enumerate(routes)
        for row in generator.generate_route(route_id, proposal_budget=2, seed=4000 + index)
    ]
    codes = [f"{index:06d}" for index in range(1, 31)]
    times = pd.date_range("2025-04-01 09:31:00", periods=12, freq="min")
    frame = pd.DataFrame(
        [(code, timestamp) for code in codes for timestamp in times],
        columns=["code", "trade_time"],
    ).sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True)
    rng = np.random.default_rng(20260714)
    for field_id in sorted(
        {
            field_id
            for row in generated
            for field_id in expression_fields(row["canonical_expression"])
        }
    ):
        frame[field_id] = rng.normal(size=len(frame))
    frame["date"] = frame["trade_time"]
    for row in generated:
        values = evaluate_panel_expression(
            frame,
            _raw_expression(row["canonical_expression"]),
            data_role="development",
        )
        assert len(values) == len(frame)
