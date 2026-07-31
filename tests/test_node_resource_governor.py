from __future__ import annotations

import hashlib
import json
import os
import socket
from pathlib import Path

import pytest

from our_system_phase2.services.node_resource_governor import (
    NodeResourceAdmissionError,
    NodeResourceLeaseDriftError,
    acquire_node_resource_lease,
    inspect_node_resource_state,
    release_node_resource_lease,
    validate_node_resource_lease_receipt,
)
from our_system_phase2.services import node_resource_governor


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _capacity(path: Path) -> Path:
    body = {
        "schema_version": "cn_alpha_node_resource_profiles_v1",
        "authorized_host": socket.gethostname().upper(),
        "logical_cpu_threads": 32,
        "memory_capacity_bytes": 1024,
        "minimum_free_memory_bytes": 1,
        "profiles": {
            "SEARCH_EXCLUSIVE_32": {
                "role": "SEARCH",
                "cpu_threads": 32,
                "memory_claim_bytes": 1,
            },
            "SEARCH_DUAL_24": {
                "role": "SEARCH",
                "cpu_threads": 24,
                "memory_claim_bytes": 1,
            },
            "VALIDATION_DUAL_8": {
                "role": "VALIDATION",
                "cpu_threads": 8,
                "memory_claim_bytes": 1,
            },
        },
    }
    body["capacity_manifest_sha256"] = _hash(body)
    path.write_text(json.dumps(body), encoding="utf-8")
    return path


def test_dual_lane_profiles_share_exact_node_cpu_capacity(tmp_path: Path) -> None:
    manifest = _capacity(tmp_path / "capacity.json")
    state_root = tmp_path / "state"
    search = acquire_node_resource_lease(
        state_root=state_root,
        capacity_manifest_path=manifest,
        profile_id="SEARCH_DUAL_24",
        lease_id="search",
        owner_pid=os.getpid(),
        workload_id="campaign-a",
    )
    validation = acquire_node_resource_lease(
        state_root=state_root,
        capacity_manifest_path=manifest,
        profile_id="VALIDATION_DUAL_8",
        lease_id="validation",
        owner_pid=os.getpid(),
        workload_id="cohort-a",
    )
    assert search["active_cpu_threads_after_admission"] == 24
    assert validation["active_cpu_threads_after_admission"] == 32
    receipt_path = tmp_path / "validation-receipt.json"
    receipt_path.write_text(json.dumps(validation), encoding="utf-8")
    validated = validate_node_resource_lease_receipt(
        receipt_path,
        expected_role="VALIDATION",
        expected_cpu_threads=8,
    )
    assert validated["lease"]["profile_id"] == "VALIDATION_DUAL_8"


def test_exclusive_search_rejects_concurrent_validation(tmp_path: Path) -> None:
    manifest = _capacity(tmp_path / "capacity.json")
    state_root = tmp_path / "state"
    acquire_node_resource_lease(
        state_root=state_root,
        capacity_manifest_path=manifest,
        profile_id="SEARCH_EXCLUSIVE_32",
        lease_id="search",
        owner_pid=os.getpid(),
        workload_id="campaign-a",
    )
    with pytest.raises(NodeResourceAdmissionError, match="CPU entitlement exceeded"):
        acquire_node_resource_lease(
            state_root=state_root,
            capacity_manifest_path=manifest,
            profile_id="VALIDATION_DUAL_8",
            lease_id="validation",
            owner_pid=os.getpid(),
            workload_id="cohort-a",
        )


def test_release_returns_entitlement_to_node_pool(tmp_path: Path) -> None:
    manifest = _capacity(tmp_path / "capacity.json")
    state_root = tmp_path / "state"
    acquire_node_resource_lease(
        state_root=state_root,
        capacity_manifest_path=manifest,
        profile_id="SEARCH_EXCLUSIVE_32",
        lease_id="search",
        owner_pid=os.getpid(),
        workload_id="campaign-a",
    )
    released = release_node_resource_lease(
        state_root=state_root,
        lease_id="search",
        owner_pid=os.getpid(),
    )
    assert released["status"] == "NODE_RESOURCE_LEASE_RELEASED"
    admitted = acquire_node_resource_lease(
        state_root=state_root,
        capacity_manifest_path=manifest,
        profile_id="VALIDATION_DUAL_8",
        lease_id="validation",
        owner_pid=os.getpid(),
        workload_id="cohort-a",
    )
    assert admitted["active_cpu_threads_after_admission"] == 8


def test_released_receipt_cannot_authorize_a_live_runtime(tmp_path: Path) -> None:
    manifest = _capacity(tmp_path / "capacity.json")
    state_root = tmp_path / "state"
    receipt = acquire_node_resource_lease(
        state_root=state_root,
        capacity_manifest_path=manifest,
        profile_id="VALIDATION_DUAL_8",
        lease_id="validation",
        owner_pid=os.getpid(),
        workload_id="cohort-a",
    )
    receipt_path = tmp_path / "receipt.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    release_node_resource_lease(
        state_root=state_root,
        lease_id="validation",
        owner_pid=os.getpid(),
    )
    with pytest.raises(
        NodeResourceLeaseDriftError,
        match="active_state_membership",
    ):
        validate_node_resource_lease_receipt(
            receipt_path,
            expected_role="VALIDATION",
            expected_cpu_threads=8,
        )


def test_same_lease_id_cannot_change_profile_or_workload(tmp_path: Path) -> None:
    manifest = _capacity(tmp_path / "capacity.json")
    state_root = tmp_path / "state"
    acquire_node_resource_lease(
        state_root=state_root,
        capacity_manifest_path=manifest,
        profile_id="SEARCH_DUAL_24",
        lease_id="shared-id",
        owner_pid=os.getpid(),
        workload_id="campaign-a",
    )
    with pytest.raises(NodeResourceAdmissionError, match="already active"):
        acquire_node_resource_lease(
            state_root=state_root,
            capacity_manifest_path=manifest,
            profile_id="VALIDATION_DUAL_8",
            lease_id="shared-id",
            owner_pid=os.getpid(),
            workload_id="cohort-b",
        )


def test_live_orphan_workload_keeps_resource_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    lease = {"workload_id": "unique-heavy-campaign-root"}
    monkeypatch.setattr(node_resource_governor, "_owner_is_alive", lambda _: False)
    monkeypatch.setattr(
        node_resource_governor,
        "_workload_process_is_alive",
        lambda _: True,
    )
    assert node_resource_governor._lease_is_active(lease) is True


def test_status_reports_exact_remaining_entitlement(tmp_path: Path) -> None:
    manifest = _capacity(tmp_path / "capacity.json")
    state_root = tmp_path / "state"
    acquire_node_resource_lease(
        state_root=state_root,
        capacity_manifest_path=manifest,
        profile_id="SEARCH_DUAL_24",
        lease_id="search",
        owner_pid=os.getpid(),
        workload_id="campaign-a",
    )
    status = inspect_node_resource_state(
        state_root=state_root,
        capacity_manifest_path=manifest,
    )
    assert status["active_cpu_threads"] == 24
    assert status["available_cpu_threads"] == 8
    assert status["active_lease_count"] == 1
    assert status["stale_lease_count"] == 0


def test_releasing_empty_pool_does_not_poison_future_admission(
    tmp_path: Path,
) -> None:
    manifest = _capacity(tmp_path / "capacity.json")
    state_root = tmp_path / "state"
    released = release_node_resource_lease(
        state_root=state_root,
        lease_id="not-present",
        owner_pid=os.getpid(),
    )
    assert released["status"] == "LEASE_ALREADY_ABSENT"
    assert not (state_root / "active_node_resource_leases.json").exists()
    admitted = acquire_node_resource_lease(
        state_root=state_root,
        capacity_manifest_path=manifest,
        profile_id="VALIDATION_DUAL_8",
        lease_id="validation",
        owner_pid=os.getpid(),
        workload_id="cohort-a",
    )
    assert admitted["active_cpu_threads_after_admission"] == 8
