from __future__ import annotations

import copy
from pathlib import Path
from types import SimpleNamespace

import pytest
import app

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import main as phase3cm_main
from our_system_phase2.runtime.phase3cp_real_cm_small_loop import (
    _authorize_proposal_decisions,
    main as phase3cp_main,
)
from our_system_phase2.services.candidate_submission_receipt import (
    CandidateReceiptError,
    CandidateSubmissionAuthority,
    LegacyCandidateSubmissionAdapter,
    ReceiptContext,
)
from our_system_phase2.services.fixed_split_authority import (
    ForbiddenSplitAccessError,
    FixedSplitAuthority,
    SplitAuthorityError,
)
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry
from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator


REPO = Path(__file__).resolve().parents[1]
SPLIT = REPO / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
REGISTRY = REPO / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry/unified_capability_registry.json"
EVALUATOR = REPO / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
DATA_RELEASE_HASH = "cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827"


def _authority(registry: UnifiedCapabilityRegistry, split: FixedSplitAuthority) -> CandidateSubmissionAuthority:
    context = ReceiptContext.build(
        registry=registry,
        split_authority=split,
        data_release_hash=DATA_RELEASE_HASH,
        evaluator_paths=[EVALUATOR],
    )
    return CandidateSubmissionAuthority(registry, context)


def test_fixed_485_day_manifest_is_the_only_role_authority() -> None:
    authority = FixedSplitAuthority.read(SPLIT)
    assert len(authority.rows) == 485
    assert authority.role_for("2024-01-02 09:31:00") == "train"
    assert authority.role_for("2025-12-31 15:00:00") == "holdout"
    with pytest.raises(SplitAuthorityError, match="not covered"):
        authority.role_for("2023-12-29")
    with pytest.raises(ForbiddenSplitAccessError, match="FORWARD_2026_SEALED"):
        authority.role_for("2026-01-05")
    with pytest.raises(ForbiddenSplitAccessError, match="report-only"):
        authority.assert_feedback_rows([{"split": "validation"}])
    with pytest.raises(ForbiddenSplitAccessError, match="report-only"):
        authority.assert_feedback_rows([{"optimizer_reward_split": "holdout"}])
    authority.assert_feedback_rows([{"optimizer_reward_split": "train"}])


def test_formal_worker_and_serial_entrypoints_fail_without_manifest_and_receipts() -> None:
    assert app.CURRENT_SEARCH_ROUTE == "phase3cp-real-cm-small-loop"
    assert "UNIFIED_RECEIPT_GATE" in app.CURRENT_SEARCH_ROUTE_CONTRACT
    assert app.CURRENT_SEARCH_STATUS.startswith("FORMAL_SEARCH_FROZEN")
    assert "phase3dv-budget-pool-self-deepen-pack" in app.RETIRED_ROUTES
    with pytest.raises(SystemExit):
        phase3cm_main([])
    with pytest.raises(SystemExit):
        phase3cp_main([])


def test_legal_registry_candidates_receive_and_validate_immutable_receipts() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    split = FixedSplitAuthority.read(SPLIT)
    candidates = RegistryDrivenGenerator(registry).generate_route("MINUTE_STATIC", proposal_budget=2, seed=71)
    authority = _authority(registry, split)
    receipts = authority.authorize_table(candidates)
    assert len(receipts) == 2
    assert all(row["authorization_status"] == "AUTHORIZED_FOR_FORMAL_EVALUATION" for row in receipts)
    assert authority.validate_table(candidates, receipts) == receipts

    missing = receipts[:1]
    with pytest.raises(CandidateReceiptError, match="matched control receipt|missing candidate"):
        authority.validate_table(candidates, missing)

    tampered = copy.deepcopy(receipts)
    tampered[0]["route_id"] = "FIRSTN_PATH"
    with pytest.raises(CandidateReceiptError, match="hash"):
        authority.validate_table(candidates, tampered)

    tampered_control = copy.deepcopy(receipts)
    tampered_control[1]["authorization_status"] = "REJECTED"
    with pytest.raises(CandidateReceiptError, match="hash|not authorized"):
        authority.validate_table(candidates[:1], tampered_control)


def test_receipt_rejects_metadata_wrong_lag_and_missing_event_control() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    split = FixedSplitAuthority.read(SPLIT)
    authority = _authority(registry, split)
    generator = RegistryDrivenGenerator(registry)

    static_pair = generator.generate_route("MINUTE_STATIC", proposal_budget=2, seed=73)
    metadata = copy.deepcopy(static_pair[0])
    metadata.update(
        {
            "candidate_id": "metadata_forbidden",
            "expression": "CSRank($code)",
            "declared_field_ids": ["code"],
            "matched_control_id": static_pair[1]["candidate_id"],
        }
    )
    with pytest.raises(CandidateReceiptError, match="PIT_UNQUALIFIED"):
        authority.authorize(metadata)

    lagged = next(
        field
        for field in registry.fields
        if field.search_eligible and field.source_lag > 0 and "SLOW_CROSS_SECTIONAL_LEVEL" in field.allowed_routes
    )
    slow_pair = generator.generate_route("SLOW_CROSS_SECTIONAL_LEVEL", proposal_budget=2, seed=79)
    wrong_lag = copy.deepcopy(slow_pair[0])
    wrong_lag["declared_source_lags"] = {lagged.field_id: f"0 {lagged.source_lag_unit}"}
    with pytest.raises(CandidateReceiptError, match="source lag drift"):
        authority.authorize(wrong_lag)

    event_pair = generator.generate_route("DISCLOSURE_EVENT", proposal_budget=2, seed=83)
    authority.authorize_table(event_pair)
    with pytest.raises(CandidateReceiptError, match="matched control candidate is absent"):
        authority.authorize_table(event_pair[:1])

    fundamental_pair = generator.generate_route("SLOW_CROSS_SECTIONAL_LEVEL", proposal_budget=2, seed=87)
    fundamental_field = next(
        field
        for field in registry.fields
        if field.search_eligible
        and field.source_family.startswith("canonical_fundamental_")
        and "SLOW_CROSS_SECTIONAL_LEVEL" in field.allowed_routes
    )
    fundamental_pair[0].update(
        {"expression": f"CSRank(${fundamental_field.field_id})", "declared_field_ids": [fundamental_field.field_id]}
    )
    fundamental_pair[1].update(
        {"expression": f"CSRank(Sign(${fundamental_field.field_id}))", "declared_field_ids": [fundamental_field.field_id]}
    )
    authority.authorize_table(fundamental_pair)

    blocked_event = copy.deepcopy(event_pair[0])
    blocked_event.update(
        {
            "candidate_id": "blocked-event-state",
            "expression": "EventCount($evt_uplimit_active,5)",
            "declared_field_ids": ["evt_uplimit_active"],
            "matched_control_id": event_pair[1]["candidate_id"],
        }
    )
    with pytest.raises(CandidateReceiptError, match="PIT_UNQUALIFIED"):
        authority.authorize(blocked_event)

    cutoff = copy.deepcopy(blocked_event)
    cutoff.update(
        {
            "candidate_id": "cutoff-minute-as-alpha",
            "expression": "EventCount($evt_uplimit_cutoff_minute,5)",
            "declared_field_ids": ["evt_uplimit_cutoff_minute"],
        }
    )
    with pytest.raises(CandidateReceiptError, match="PIT_UNQUALIFIED"):
        authority.authorize(cutoff)

    future_snapshot = copy.deepcopy(static_pair[0])
    future_snapshot["uses_future_revision"] = True
    with pytest.raises(CandidateReceiptError, match="PIT_UNQUALIFIED"):
        authority.authorize(future_snapshot)

    for candidate_id, expression in (
        ("direct-raw-fundamental", "CSRank($TOTAL_ASSETS)"),
        ("plate-placeholder", "CSRank($plate)"),
    ):
        raw = copy.deepcopy(static_pair[0])
        raw.update(
            {
                "candidate_id": candidate_id,
                "expression": expression,
                "declared_field_ids": [expression.split("$")[1].split(")")[0]],
                "matched_control_id": static_pair[1]["candidate_id"],
            }
        )
        with pytest.raises(CandidateReceiptError, match="UNKNOWN_FIELD_ID"):
            authority.authorize(raw)


def test_receipt_fails_closed_on_registry_or_split_hash_drift(tmp_path: Path) -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    split = FixedSplitAuthority.read(SPLIT)
    candidates = RegistryDrivenGenerator(registry).generate_route("FIRSTN_PATH", proposal_budget=2, seed=89)
    receipts = _authority(registry, split).authorize_table(candidates)

    changed_payload = copy.deepcopy(registry.payload)
    changed_payload["authority_scope"] = str(changed_payload.get("authority_scope") or "") + " audited"
    changed_registry = UnifiedCapabilityRegistry(changed_payload)
    with pytest.raises(CandidateReceiptError, match="context drift|authority drift"):
        _authority(changed_registry, split).validate_table(candidates, receipts)

    changed_split_path = tmp_path / "split.csv"
    changed_split_path.write_text(SPLIT.read_text(encoding="utf-8-sig").replace(
        "repaired_true1min_2024_2025_full_trade_calendar",
        "repaired_true1min_2024_2025_full_trade_calendar_receipt_drift_test",
    ), encoding="utf-8")
    changed_split = FixedSplitAuthority.read(changed_split_path)
    with pytest.raises(CandidateReceiptError, match="context drift|authority drift"):
        _authority(registry, changed_split).validate_table(candidates, receipts)


def test_legacy_proposals_are_proposal_only_until_conservative_typed_adaptation() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    authority = _authority(registry, FixedSplitAuthority.read(SPLIT))
    adapter = LegacyCandidateSubmissionAdapter(registry)

    raw, raw_control = adapter.adapt_pair(
        {"candidate_id": "legacy-raw", "expression": "CSRank(Add($close,$open))", "generator_arm": "legacy"}
    )
    assert raw["route_id"] == "MINUTE_STATIC"
    authority.authorize_table([raw, raw_control])

    firstn_field = next(field for field in registry.fields if field.search_eligible and field.source_family == "firstN")
    firstn, firstn_control = adapter.adapt_pair(
        {
            "candidate_id": "legacy-firstn",
            "expression": f"CSRank(Add(${firstn_field.field_id},$close))",
            "generator_arm": "legacy",
        }
    )
    assert firstn["route_id"] == "FIRSTN_PATH"
    authority.authorize_table([firstn, firstn_control])

    lagged_field = next(
        field
        for field in registry.fields
        if field.search_eligible
        and field.source_family == "lagged_daily_context"
        and "SLOW_CROSS_SECTIONAL_LEVEL" in field.allowed_routes
    )
    lagged, lagged_control = adapter.adapt_pair(
        {"candidate_id": "legacy-lagged", "expression": f"CSRank(${lagged_field.field_id})"}
    )
    assert lagged["route_id"] == "SLOW_CROSS_SECTIONAL_LEVEL"
    authority.authorize_table([lagged, lagged_control])

    with pytest.raises(CandidateReceiptError):
        adapter.adapt_pair({"candidate_id": "legacy-metadata", "expression": "CSRank($code)"})


def test_phase3cp_receipt_gate_runs_before_admission_and_rejects_schema_only_fields(tmp_path: Path) -> None:
    args = SimpleNamespace(
        cm_split_manifest=SPLIT,
        unified_registry=REGISTRY,
        data_release_hash=DATA_RELEASE_HASH,
    )
    accepted, receipts = _authorize_proposal_decisions(
        args,
        [
            {"candidate_id": "legal", "expression": "CSRank(Add($close,$open))", "generator_arm": "legacy"},
            {"candidate_id": "schema-only", "expression": "CSRank($code)", "generator_arm": "legacy"},
        ],
        output_root=tmp_path / "runtime",
        report_root=tmp_path / "reports",
    )
    assert [row["candidate_id"] for row in accepted] == ["legal"]
    assert receipts.is_file()
    rejection_text = (tmp_path / "runtime/candidate_submission_receipt_rejections.csv").read_text(encoding="utf-8")
    assert "schema-only" in rejection_text
    assert "REJECTED_BEFORE_ADMISSION" in rejection_text
