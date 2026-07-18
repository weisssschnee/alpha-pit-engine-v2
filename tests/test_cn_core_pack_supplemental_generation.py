from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from pathlib import Path

import pytest
import pandas as pd

from scripts.run_cn_core_pack_supplemental_generation import (
    DEFAULT_CAMPAIGN,
    DEFAULT_REGISTRY,
    DEFAULT_SUPPLEMENTAL_SCOPE,
    REPO,
    TARGET_ROOTS_BY_ROUTE,
    _expected_baseline_receipt_binding,
    _source_identity,
    generate,
    load_runtime_receipt_bindings,
    validate_supplemental_scope,
)
from our_system_phase2.services.materialization_support_receipt import (
    build_materialization_support_receipt,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


def test_supplemental_scope_and_generation_are_append_only_and_globally_dedupable() -> None:
    registry = UnifiedCapabilityRegistry.read(Path(DEFAULT_REGISTRY))
    campaign = json.loads(Path(DEFAULT_CAMPAIGN).read_text(encoding="utf-8"))
    assert _expected_baseline_receipt_binding(campaign) == {
        "path": (
            "D:/ChengboRemote/runtime/cn_core_pack_aggressive_discovery_20260718_595c5fc/"
            "CN_PAIR_RECEIPTS.jsonl"
        ),
        "sha256": "d332d501176258488ee26845a911a37f3dc7451f9e6c0b6bc23b504c572e10e9",
    }
    scope = json.loads(Path(DEFAULT_SUPPLEMENTAL_SCOPE).read_text(encoding="utf-8"))
    base_path = REPO / scope["baseline_scope"]["path"]
    base = json.loads(base_path.read_text(encoding="utf-8"))
    original = copy.deepcopy(base)
    effective = validate_supplemental_scope(
        scope=scope,
        base_scope=base,
        registry=registry,
    )

    assert base == original
    assert scope["append_only"] is True
    assert scope["baseline_scope"]["mutation"] == "FORBIDDEN"
    assert sum(len(values) for values in TARGET_ROOTS_BY_ROUTE.values()) == 7
    for route_id, roots in TARGET_ROOTS_BY_ROUTE.items():
        assert set(roots) <= set(effective[route_id])

    receipts, coverage = generate(
        registry=registry,
        route_root_allowlists=effective,
    )
    assert receipts
    assert coverage["existing_pack_rewritten"] is False
    assert coverage["performance_or_reward_used"] is False
    assert coverage["validation_accessed"] is False
    assert coverage["runtime_receipt_bound_root_count"] == 0
    assert coverage["runtime_ready_gated_pair_count"] == 0
    assert coverage["runtime_frozen_gated_pair_count"] > 0
    assert all(
        row["runtime_ready"] is False
        and row["signal_sketch_allowed"] is False
        and row["strict_evaluation_allowed"] is False
        and row["materialization_support_receipt_hashes"] == {}
        for row in receipts
        if set(row["declared_field_ids"]) & {
            "fund_disclosure_balance_age_sessions",
            "fund_disclosure_profit_age_sessions",
            "fund_disclosure_cashflow_age_sessions",
            "fund_disclosure_holder_age_sessions",
            "state_close_range_location_sign",
        }
    )
    assert all(
        row["all_target_roots_observed"] for row in coverage["routes"].values()
    )

    baseline_exact = {
        receipts[0]["exact_identity"],
        receipts[0]["control_exact_identity"],
    }
    deduped, dedup_coverage = generate(
        registry=registry,
        route_root_allowlists=effective,
        baseline_exact_ids=baseline_exact,
    )
    assert len(deduped) == len(receipts) - 1
    assert dedup_coverage["baseline_collision_pairs"] > 0
    assert dedup_coverage["baseline_global_exact_dedup_applied"] is True


def test_no_git_source_closure_requires_explicit_repo_and_tree_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_git(*args: object, **kwargs: object) -> str:
        raise subprocess.CalledProcessError(128, "git")

    monkeypatch.setattr(subprocess, "check_output", no_git)
    with pytest.raises(ValueError, match="no-git source closure requires"):
        _source_identity(repo_sha="", tree_sha="")
    assert _source_identity(repo_sha="a" * 40, tree_sha="b" * 40) == (
        "a" * 40,
        "b" * 40,
    )


@pytest.mark.parametrize("field", ["path", "sha256"])
def test_baseline_receipt_binding_cannot_be_forged_and_resigned(field: str) -> None:
    campaign = json.loads(Path(DEFAULT_CAMPAIGN).read_text(encoding="utf-8"))
    pair_receipt = next(
        row
        for row in campaign["baseline_contract"]["artifacts"]
        if row["role"] == "pair_receipts"
    )
    pair_receipt[field] = "f" * 64 if field == "sha256" else "forged"
    campaign["contract_hash"] = stable_hash(
        {key: value for key, value in campaign.items() if key != "contract_hash"}
    )

    with pytest.raises(ValueError, match="canonical immutable binding"):
        _expected_baseline_receipt_binding(campaign)


def _write_runtime_receipt(
    tmp_path: Path,
    *,
    field_id: str = "fund_disclosure_balance_age_sessions",
    assembly: str = "FULL_DEVELOPMENT_SESSION_PANEL",
    pit_status: str | None = None,
    values: list[float] | None = None,
    access_ledger: dict | None = None,
    include_full_evidence: bool = True,
) -> tuple[Path, dict, UnifiedCapabilityRegistry]:
    registry = UnifiedCapabilityRegistry.read(Path(DEFAULT_REGISTRY))
    capability = registry.resolve(field_id)
    frame = pd.DataFrame(
        {
            "code": ["000001.SZ", "000002.SZ"],
            "trade_time": pd.to_datetime(
                ["2025-06-03 09:31", "2025-06-03 09:31"]
            ),
            field_id: values if values is not None else [1.0, 2.0],
        }
    )
    split_path = tmp_path / "split.csv"
    pd.DataFrame({"trade_date": ["2025-06-03"], "split": ["train"]}).to_csv(
        split_path, index=False
    )
    release_path = tmp_path / "development_only_release_manifest.json"
    release_hash = "c" * 64
    release_path.write_text(
        json.dumps(
            {
                "release_hash": release_hash,
                "forbidden_roles_present": [],
                "forward_2026_present": False,
            }
        ),
        encoding="utf-8",
    )
    coordinate_path = tmp_path / f"{field_id}.coordinates.parquet"
    materialized_path = tmp_path / f"{field_id}.parquet"
    frame[["code", "trade_time"]].to_parquet(coordinate_path, index=False)
    frame.to_parquet(materialized_path, index=False)
    sha = lambda value: hashlib.sha256(Path(value).read_bytes()).hexdigest()
    evidence = (
        {
            "evidence_role": "FULL_DEVELOPMENT_ROOT_AUTHORITY",
            "development_release": {
                "path": str(release_path),
                "sha256": sha(release_path),
                "release_hash": release_hash,
            },
            "split_manifest": {"path": str(split_path), "sha256": sha(split_path)},
            "coordinate_authority": {
                "path": str(coordinate_path),
                "sha256": sha(coordinate_path),
            },
            "materialized_artifact": {
                "path": str(materialized_path),
                "sha256": sha(materialized_path),
            },
        }
        if include_full_evidence
        else {}
    )
    receipt = build_materialization_support_receipt(
        frame,
        field_id=field_id,
        representation_id=capability.representation_id,
        registry_hash=registry.registry_hash,
        receipt_type=(
            "FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT"
            if field_id == "state_close_range_location_sign"
            else "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT"
        ),
        materializer_authority=(
            "FeatureStateFabric"
            if field_id == "state_close_range_location_sign"
            else "CompositionalSessionSignalPanelAssembler"
        ),
        materializer_manifest={"fixture": "full-development"},
        support_unit=capability.support_unit,
        observable_time_contract=capability.observable_clock,
        maturity_contract=capability.maturity_rule,
        row_keys=("code", "trade_time"),
        partition_identity={"assembly": assembly},
        source_binding={
            "pit_status": pit_status or capability.pit_status,
            "development_release_hash": release_hash,
            "split_manifest_sha256": sha(split_path),
            "maximum_observable_time": "2025-06-03 15:00:00",
        },
        access_ledger=access_ledger,
        evidence_contract=evidence,
    )
    path = tmp_path / f"{field_id}.receipt.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")
    return path, receipt, registry


@pytest.mark.parametrize(
    ("field_id", "assembly"),
    [
        (
            "fund_disclosure_balance_age_sessions",
            "FULL_DEVELOPMENT_SESSION_PANEL",
        ),
        (
            "state_close_range_location_sign",
            "FULL_DEVELOPMENT_ACTIVE_PANEL",
        ),
    ],
)
def test_canonical_runtime_receipt_unlocks_only_its_covered_root(
    tmp_path: Path,
    field_id: str,
    assembly: str,
) -> None:
    path, receipt, registry = _write_runtime_receipt(
        tmp_path,
        field_id=field_id,
        assembly=assembly,
    )
    scope = json.loads(Path(DEFAULT_SUPPLEMENTAL_SCOPE).read_text(encoding="utf-8"))
    base = json.loads((REPO / scope["baseline_scope"]["path"]).read_text(encoding="utf-8"))
    effective = validate_supplemental_scope(scope=scope, base_scope=base, registry=registry)
    bindings = load_runtime_receipt_bindings([path], registry=registry)

    receipts, coverage = generate(
        registry=registry,
        route_root_allowlists=effective,
        runtime_receipts=bindings,
    )

    covered = [row for row in receipts if field_id in row["declared_field_ids"]]
    uncovered = [
        row
        for row in receipts
        if field_id not in row["declared_field_ids"]
        and set(row["declared_field_ids"])
        & {
            "fund_disclosure_balance_age_sessions",
            "fund_disclosure_profit_age_sessions",
            "fund_disclosure_cashflow_age_sessions",
            "fund_disclosure_holder_age_sessions",
            "state_close_range_location_sign",
        }
    ]
    assert covered
    assert all(row["runtime_ready"] is True for row in covered)
    assert all(row["signal_sketch_allowed"] is True for row in covered)
    assert all(row["strict_evaluation_allowed"] is True for row in covered)
    assert all(
        row["materialization_support_receipt_hashes"]
        == {field_id: receipt["receipt_hash"]}
        for row in covered
    )
    assert uncovered
    assert all(row["runtime_ready"] is False for row in uncovered)
    assert coverage["runtime_receipt_bound_root_ids"] == [field_id]


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ("partition", "frozen full-development assembly"),
        ("pit", "PIT/representation contract drift"),
        ("access", "restricted data"),
        ("support", "no real materialized support"),
        ("content", "self-hash"),
    ],
)
def test_runtime_receipt_qualification_fails_closed(
    tmp_path: Path,
    change: str,
    match: str,
) -> None:
    kwargs: dict = {}
    if change == "partition":
        kwargs["assembly"] = "PARTITION_0_ONLY"
    elif change == "pit":
        kwargs["pit_status"] = "FORGED_PIT"
    elif change == "access":
        kwargs["access_ledger"] = {"validation_rows_read": 1}
    elif change == "support":
        kwargs["values"] = [float("nan"), float("nan")]
    path, _receipt, registry = _write_runtime_receipt(tmp_path, **kwargs)
    if change == "content":
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["materialization"]["finite_count"] = 999
        path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises((ValueError, PermissionError), match=match):
        load_runtime_receipt_bindings([path], registry=registry)


def test_two_row_self_hashed_smoke_receipt_cannot_claim_full_development(
    tmp_path: Path,
) -> None:
    path, _receipt, registry = _write_runtime_receipt(
        tmp_path, include_full_evidence=False
    )

    with pytest.raises(ValueError, match="full-development authority evidence"):
        load_runtime_receipt_bindings([path], registry=registry)
