from __future__ import annotations

import copy
import json
from pathlib import Path

from scripts.validate_cn_phase3cm_146_replay_contract import contract_hash, validate_contract


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "runtime" / "run_plans" / "cn_phase3cm_current_kernel_146_parity_replay_v1.json"


def _payload() -> dict:
    return json.loads(CONTRACT.read_text(encoding="utf-8-sig"))


def test_frozen_146_replay_contract_is_valid() -> None:
    payload = _payload()
    assert payload["contract_hash"] == contract_hash(payload)
    assert validate_contract(payload) == []


def test_thread_oversubscription_fails_closed() -> None:
    payload = copy.deepcopy(_payload())
    payload["execution_contract"]["active_native_compute_threads_total"] = 25
    payload["contract_hash"] = contract_hash(payload)
    errors = validate_contract(payload)
    assert "global native thread budget exceeded" in errors


def test_partition_identity_drift_fails_closed() -> None:
    payload = copy.deepcopy(_payload())
    payload["partitions"][1]["expected_pair_ids"][0] = payload["partitions"][0]["expected_pair_ids"][0]
    payload["contract_hash"] = contract_hash(payload)
    errors = validate_contract(payload)
    assert "partition pair union differs from global pair IDs" in errors
    assert "partition pair IDs overlap" in errors

